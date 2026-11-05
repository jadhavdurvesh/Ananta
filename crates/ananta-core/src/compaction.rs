use crate::chronicle::sha256;
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

const EVENT_MAGIC: &[u8; 4] = b"CJE1";
const EVENT_VERSION: u16 = 1;
const EVENT_LEN: usize = 160;
const EVENT_CHECKSUM_OFFSET: usize = EVENT_LEN - 32;
const MAX_JOURNAL_BYTES: u64 = 64 * 1024 * 1024;
const MAX_JOURNAL_EVENTS: u64 = MAX_JOURNAL_BYTES / EVENT_LEN as u64;

/// Stable identity for one logical compaction job.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CompactionJobId([u8; 16]);

impl CompactionJobId {
    /// Construct a job identity. The all-zero identity is reserved.
    pub fn from_bytes(bytes: [u8; 16]) -> Result<Self, CompactionError> {
        if bytes.iter().all(|byte| *byte == 0) {
            return Err(CompactionError::InvalidIdentity);
        }
        Ok(Self(bytes))
    }

    pub fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }
}

/// Durable lifecycle states for a single compaction attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum CompactionJobState {
    Planned = 0,
    ValidatingInputs = 1,
    Running = 2,
    Verifying = 3,
    Publishing = 4,
    CancellationRequested = 5,
    Completed = 6,
    Cancelled = 7,
    Failed = 8,
    Interrupted = 9,
}

impl CompactionJobState {
    fn from_byte(value: u8) -> Result<Self, CompactionError> {
        match value {
            0 => Ok(Self::Planned),
            1 => Ok(Self::ValidatingInputs),
            2 => Ok(Self::Running),
            3 => Ok(Self::Verifying),
            4 => Ok(Self::Publishing),
            5 => Ok(Self::CancellationRequested),
            6 => Ok(Self::Completed),
            7 => Ok(Self::Cancelled),
            8 => Ok(Self::Failed),
            9 => Ok(Self::Interrupted),
            _ => Err(CompactionError::CorruptEvent("unknown job state")),
        }
    }

    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Cancelled | Self::Failed | Self::Interrupted)
    }

    fn can_transition_to(self, next: Self) -> bool {
        use CompactionJobState as S;
        match self {
            S::Planned => matches!(next, S::ValidatingInputs | S::Failed | S::Interrupted),
            S::ValidatingInputs => matches!(next, S::Running | S::CancellationRequested | S::Failed | S::Interrupted),
            S::Running => matches!(next, S::Verifying | S::CancellationRequested | S::Failed | S::Interrupted),
            S::Verifying => matches!(next, S::Publishing | S::CancellationRequested | S::Failed | S::Interrupted),
            S::Publishing => matches!(next, S::Completed | S::Failed | S::Interrupted),
            S::CancellationRequested => matches!(next, S::Cancelled | S::Failed | S::Interrupted),
            S::Completed | S::Cancelled | S::Failed | S::Interrupted => false,
        }
    }
}

/// Bounded counters recorded at a job event or work-unit boundary.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CompactionProgress {
    pub input_bytes: u64,
    pub output_bytes: u64,
    pub temporary_bytes: u64,
    pub working_memory_bytes: u64,
    pub rows_processed: u64,
    pub open_files: u64,
    pub current_work_unit_rows: u64,
}

/// Finite limits captured when a compaction attempt is planned.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompactionResourceLimits {
    pub max_input_bytes: u64,
    pub max_output_bytes: u64,
    pub max_temporary_bytes: u64,
    pub max_working_memory_bytes: u64,
    pub max_rows: u64,
    pub max_open_files: u64,
    pub max_work_unit_rows: u64,
}

impl CompactionResourceLimits {
    /// Reject unbounded/zero budgets and internally inconsistent work-unit limits.
    pub fn validate(&self) -> Result<(), CompactionError> {
        let limits = [
            self.max_input_bytes,
            self.max_output_bytes,
            self.max_temporary_bytes,
            self.max_working_memory_bytes,
            self.max_rows,
            self.max_open_files,
            self.max_work_unit_rows,
        ];
        if limits.iter().any(|limit| *limit == 0) || self.max_work_unit_rows > self.max_rows {
            return Err(CompactionError::InvalidLimits);
        }
        Ok(())
    }

    /// Check cumulative and instantaneous usage before the next bounded work unit.
    pub fn check(&self, progress: &CompactionProgress) -> Result<(), CompactionError> {
        self.validate()?;
        let checks = [
            ("input bytes", progress.input_bytes, self.max_input_bytes),
            ("output bytes", progress.output_bytes, self.max_output_bytes),
            ("temporary bytes", progress.temporary_bytes, self.max_temporary_bytes),
            ("working memory bytes", progress.working_memory_bytes, self.max_working_memory_bytes),
            ("rows", progress.rows_processed, self.max_rows),
            ("open files", progress.open_files, self.max_open_files),
            ("work-unit rows", progress.current_work_unit_rows, self.max_work_unit_rows),
        ];
        for (resource, actual, limit) in checks {
            if actual > limit {
                return Err(CompactionError::LimitExceeded { resource, actual, limit });
            }
        }
        Ok(())
    }
}

/// One fixed-width, checksummed event in the append-only compaction job journal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactionJobEvent {
    pub job_id: CompactionJobId,
    pub attempt: u64,
    pub event_sequence: u64,
    pub state: CompactionJobState,
    pub input_manifest_sha256: [u8; 32],
    pub progress: CompactionProgress,
}

impl CompactionJobEvent {
    fn validate_fields(&self) -> Result<(), CompactionError> {
        if self.attempt == 0
            || self.event_sequence == 0
            || self.input_manifest_sha256.iter().all(|byte| *byte == 0)
        {
            return Err(CompactionError::CorruptEvent("zero attempt, event sequence or manifest fingerprint"));
        }
        Ok(())
    }

    /// Encode a canonical 160-byte CJE1 journal event.
    pub fn encode(&self) -> Result<[u8; EVENT_LEN], CompactionError> {
        self.validate_fields()?;
        let mut bytes = [0_u8; EVENT_LEN];
        let mut cursor = 0;

        copy_into(&mut bytes, &mut cursor, EVENT_MAGIC);
        copy_into(&mut bytes, &mut cursor, &EVENT_VERSION.to_le_bytes());
        copy_into(&mut bytes, &mut cursor, &[self.state as u8]);
        copy_into(&mut bytes, &mut cursor, &[0_u8]);
        copy_into(&mut bytes, &mut cursor, self.job_id.as_bytes());
        copy_into(&mut bytes, &mut cursor, &self.attempt.to_le_bytes());
        copy_into(&mut bytes, &mut cursor, &self.event_sequence.to_le_bytes());
        copy_into(&mut bytes, &mut cursor, &self.input_manifest_sha256);

        for value in [
            self.progress.input_bytes,
            self.progress.output_bytes,
            self.progress.temporary_bytes,
            self.progress.working_memory_bytes,
            self.progress.rows_processed,
            self.progress.open_files,
            self.progress.current_work_unit_rows,
        ] {
            copy_into(&mut bytes, &mut cursor, &value.to_le_bytes());
        }

        if cursor != EVENT_CHECKSUM_OFFSET {
            return Err(CompactionError::CorruptEvent("internal event layout mismatch"));
        }
        let checksum = sha256(&bytes[..EVENT_CHECKSUM_OFFSET]);
        copy_into(&mut bytes, &mut cursor, &checksum);
        if cursor != EVENT_LEN {
            return Err(CompactionError::CorruptEvent("internal event length mismatch"));
        }
        Ok(bytes)
    }

    /// Decode and strictly validate one canonical CJE1 journal event.
    pub fn decode(bytes: &[u8]) -> Result<Self, CompactionError> {
        if bytes.len() != EVENT_LEN {
            return Err(CompactionError::CorruptEvent("event length mismatch"));
        }
        if &bytes[..4] != EVENT_MAGIC {
            return Err(CompactionError::CorruptEvent("invalid event magic"));
        }
        if read_u16(bytes, 4)? != EVENT_VERSION {
            return Err(CompactionError::CorruptEvent("unsupported event version"));
        }
        if bytes[7] != 0 {
            return Err(CompactionError::CorruptEvent("nonzero reserved event flags"));
        }
        let expected = sha256(&bytes[..EVENT_CHECKSUM_OFFSET]);
        if bytes[EVENT_CHECKSUM_OFFSET..] != expected[..] {
            return Err(CompactionError::CorruptEvent("event checksum mismatch"));
        }

        let mut job_bytes = [0_u8; 16];
        job_bytes.copy_from_slice(&bytes[8..24]);
        let mut manifest_hash = [0_u8; 32];
        manifest_hash.copy_from_slice(&bytes[40..72]);

        let cursor = 72;
        let progress = CompactionProgress {
            input_bytes: read_u64(bytes, cursor)?,
            output_bytes: read_u64(bytes, cursor + 8)?,
            temporary_bytes: read_u64(bytes, cursor + 16)?,
            working_memory_bytes: read_u64(bytes, cursor + 24)?,
            rows_processed: read_u64(bytes, cursor + 32)?,
            open_files: read_u64(bytes, cursor + 40)?,
            current_work_unit_rows: read_u64(bytes, cursor + 48)?,
        };
        if cursor + 56 != EVENT_CHECKSUM_OFFSET {
            return Err(CompactionError::CorruptEvent("internal event decode layout mismatch"));
        }

        let event = Self {
            job_id: CompactionJobId::from_bytes(job_bytes)?,
            attempt: read_u64(bytes, 24)?,
            event_sequence: read_u64(bytes, 32)?,
            state: CompactionJobState::from_byte(bytes[6])?,
            input_manifest_sha256: manifest_hash,
            progress,
        };
        event.validate_fields()?;
        Ok(event)
    }
}

/// In-memory state reducer for an append-only compaction event stream.
///
/// Persistence is deliberately separate: callers append each encoded event to a
/// durable journal file, then use replay to rebuild and validate the state.
#[derive(Debug, Clone)]
pub struct CompactionJobJournal {
    job_id: CompactionJobId,
    attempt: u64,
    input_manifest_sha256: [u8; 32],
    last_event_sequence: u64,
    last_state: Option<CompactionJobState>,
}

impl CompactionJobJournal {
    pub fn new(
        job_id: CompactionJobId,
        attempt: u64,
        input_manifest_sha256: [u8; 32],
    ) -> Result<Self, CompactionError> {
        if attempt == 0 || input_manifest_sha256.iter().all(|byte| *byte == 0) {
            return Err(CompactionError::InvalidIdentity);
        }
        Ok(Self {
            job_id,
            attempt,
            input_manifest_sha256,
            last_event_sequence: 0,
            last_state: None,
        })
    }

    /// Validate and reduce the next state transition, returning its immutable event.
    pub fn append(
        &mut self,
        state: CompactionJobState,
        progress: CompactionProgress,
    ) -> Result<CompactionJobEvent, CompactionError> {
        match self.last_state {
            None if state == CompactionJobState::Planned => {}
            Some(previous) if previous.can_transition_to(state) => {}
            Some(previous) => return Err(CompactionError::InvalidTransition { from: previous, to: state }),
            None => return Err(CompactionError::FirstStateMustBePlanned),
        }

        let event_sequence = self.last_event_sequence
            .checked_add(1)
            .ok_or(CompactionError::SequenceOverflow)?;
        let event = CompactionJobEvent {
            job_id: self.job_id,
            attempt: self.attempt,
            event_sequence,
            state,
            input_manifest_sha256: self.input_manifest_sha256,
            progress,
        };
        event.validate_fields()?;
        self.last_event_sequence = event_sequence;
        self.last_state = Some(state);
        Ok(event)
    }

    /// Rebuild state from ordered journal events, rejecting gaps, identity changes
    /// and illegal transitions rather than accepting a partially valid history.
    pub fn replay(events: &[CompactionJobEvent]) -> Result<Self, CompactionError> {
        let first = events.first().ok_or(CompactionError::EmptyJournal)?;
        let mut journal = Self::new(first.job_id, first.attempt, first.input_manifest_sha256)?;
        for event in events {
            event.validate_fields()?;
            if event.job_id != journal.job_id
                || event.attempt != journal.attempt
                || event.input_manifest_sha256 != journal.input_manifest_sha256
            {
                return Err(CompactionError::JournalIdentityMismatch);
            }
            let expected_sequence = journal.last_event_sequence
                .checked_add(1)
                .ok_or(CompactionError::SequenceOverflow)?;
            if event.event_sequence != expected_sequence {
                return Err(CompactionError::JournalSequenceMismatch);
            }
            match journal.last_state {
                None if event.state == CompactionJobState::Planned => {}
                Some(previous) if previous.can_transition_to(event.state) => {}
                Some(previous) => return Err(CompactionError::InvalidTransition { from: previous, to: event.state }),
                None => return Err(CompactionError::FirstStateMustBePlanned),
            }
            journal.last_event_sequence = event.event_sequence;
            journal.last_state = Some(event.state);
        }
        Ok(journal)
    }

    pub fn last_state(&self) -> Option<CompactionJobState> {
        self.last_state
    }

    pub fn last_event_sequence(&self) -> u64 {
        self.last_event_sequence
    }
}


/// Read and verify every fixed-width event in one attempt journal.
///
/// The file must be a regular file and must contain complete CJE1 records. A
/// partial tail is reported as corruption; it is never silently truncated.
pub fn read_compaction_journal(path: impl AsRef<Path>) -> Result<Vec<CompactionJobEvent>, CompactionError> {
    let path = path.as_ref();
    let path_metadata = fs::symlink_metadata(path).map_err(compaction_io_error)?;
    if !path_metadata.file_type().is_file() {
        return Err(CompactionError::JournalNotRegularFile);
    }
    let file_len = path_metadata.len();
    if file_len == 0 {
        return Err(CompactionError::EmptyJournal);
    }
    if file_len > MAX_JOURNAL_BYTES {
        return Err(CompactionError::JournalTooLarge { actual: file_len, limit: MAX_JOURNAL_BYTES });
    }
    if file_len % EVENT_LEN as u64 != 0 {
        return Err(CompactionError::JournalTruncated);
    }
    let event_count = file_len / EVENT_LEN as u64;
    if event_count > MAX_JOURNAL_EVENTS {
        return Err(CompactionError::JournalTooLarge { actual: file_len, limit: MAX_JOURNAL_BYTES });
    }

    let file = File::open(path).map_err(compaction_io_error)?;
    if file.metadata().map_err(compaction_io_error)?.len() != file_len {
        return Err(CompactionError::JournalChangedDuringRead);
    }
    let capacity = usize::try_from(file_len).map_err(|_| CompactionError::JournalTooLarge {
        actual: file_len,
        limit: MAX_JOURNAL_BYTES,
    })?;
    let mut bytes = Vec::with_capacity(capacity);
    file.take(MAX_JOURNAL_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(compaction_io_error)?;
    if bytes.len() as u64 != file_len {
        return Err(CompactionError::JournalChangedDuringRead);
    }

    let mut events = Vec::with_capacity(event_count as usize);
    for chunk in bytes.chunks_exact(EVENT_LEN) {
        events.push(CompactionJobEvent::decode(chunk)?);
    }
    CompactionJobJournal::replay(&events)?;
    Ok(events)
}

/// Append one event to an attempt journal and sync it to stable storage.
///
/// The caller must hold the database's exclusive writer ownership and derive
/// the path from validated job identities beneath the database's compaction area.
/// Existing journal bytes are replayed before append, so illegal transitions,
/// sequence gaps, checksum damage and partial tails are rejected without writes.
/// An I/O error after writing begins is reported as uncertain; reload the journal
/// before deciding whether to retry.
pub fn append_compaction_journal_event(
    path: impl AsRef<Path>,
    event: &CompactionJobEvent,
) -> Result<(), CompactionError> {
    let path = path.as_ref();
    let encoded = event.encode()?;
    let path_metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => Some(metadata),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => return Err(compaction_io_error(error)),
    };

    let previous_len = if let Some(metadata) = &path_metadata {
        if !metadata.file_type().is_file() {
            return Err(CompactionError::JournalNotRegularFile);
        }
        let events = read_compaction_journal(path)?;
        let mut journal = CompactionJobJournal::replay(&events)?;
        let expected = journal.append(event.state, event.progress)?;
        if expected != *event {
            return Err(CompactionError::JournalEventMismatch);
        }
        Some(metadata.len())
    } else {
        let mut journal = CompactionJobJournal::new(
            event.job_id,
            event.attempt,
            event.input_manifest_sha256,
        )?;
        let expected = journal.append(event.state, event.progress)?;
        if expected != *event {
            return Err(CompactionError::JournalEventMismatch);
        }
        None
    };

    let previous_len_value = previous_len.unwrap_or(0);
    let new_len = previous_len_value
        .checked_add(EVENT_LEN as u64)
        .ok_or(CompactionError::SequenceOverflow)?;
    if new_len > MAX_JOURNAL_BYTES {
        return Err(CompactionError::JournalTooLarge { actual: new_len, limit: MAX_JOURNAL_BYTES });
    }

    let mut file = if previous_len.is_some() {
        OpenOptions::new()
            .write(true)
            .append(true)
            .open(path)
            .map_err(compaction_io_error)?
    } else {
        OpenOptions::new()
            .write(true)
            .append(true)
            .create_new(true)
            .open(path)
            .map_err(compaction_io_error)?
    };

    let actual_len = file.metadata().map_err(compaction_io_error)?.len();
    if actual_len != previous_len_value {
        return Err(CompactionError::JournalChangedBeforeAppend);
    }

    if let Err(error) = file.write_all(&encoded) {
        return Err(CompactionError::JournalAppendUncertain {
            path: path.to_path_buf(),
            reason: error.to_string(),
        });
    }
    if let Err(error) = file.sync_all() {
        return Err(CompactionError::JournalAppendUncertain {
            path: path.to_path_buf(),
            reason: error.to_string(),
        });
    }
    drop(file);

    if previous_len.is_none() {
        let parent = journal_parent(path);
        if let Err(error) = sync_journal_directory(parent) {
            return Err(CompactionError::JournalAppendUncertain {
                path: path.to_path_buf(),
                reason: error.to_string(),
            });
        }
    }
    Ok(())
}

/// Recover an unfinished attempt by appending exactly one Interrupted event.
///
/// Completed/failed/cancelled/interrupted attempts are returned unchanged.
/// A journal with a partial or corrupt record is preserved and rejected; recovery
/// never truncates evidence or guesses which event was durable.
pub fn recover_compaction_journal(path: impl AsRef<Path>) -> Result<CompactionJobState, CompactionError> {
    let path = path.as_ref();
    let events = read_compaction_journal(path)?;
    let last = events.last().ok_or(CompactionError::EmptyJournal)?;
    if last.state.is_terminal() {
        return Ok(last.state);
    }

    let mut journal = CompactionJobJournal::replay(&events)?;
    let interrupted = journal.append(CompactionJobState::Interrupted, last.progress)?;
    append_compaction_journal_event(path, &interrupted)?;
    Ok(CompactionJobState::Interrupted)
}

fn journal_parent(path: &Path) -> &Path {
    path.parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
}

fn compaction_io_error(error: std::io::Error) -> CompactionError {
    CompactionError::Io(error.to_string())
}

#[cfg(unix)]
fn sync_journal_directory(path: &Path) -> std::io::Result<()> {
    File::open(path)?.sync_all()
}

#[cfg(not(unix))]
fn sync_journal_directory(_path: &Path) -> std::io::Result<()> {
    // Portable std does not expose directory-entry flushing on all supported targets.
    Ok(())
}

/// Errors from the M3.1 compaction state and resource primitives.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CompactionError {
    InvalidIdentity,
    InvalidLimits,
    LimitExceeded { resource: &'static str, actual: u64, limit: u64 },
    InvalidTransition { from: CompactionJobState, to: CompactionJobState },
    FirstStateMustBePlanned,
    EmptyJournal,
    JournalIdentityMismatch,
    JournalSequenceMismatch,
    SequenceOverflow,
    CorruptEvent(&'static str),
    Io(String),
    JournalNotRegularFile,
    JournalTooLarge { actual: u64, limit: u64 },
    JournalTruncated,
    JournalChangedDuringRead,
    JournalChangedBeforeAppend,
    JournalEventMismatch,
    JournalAppendUncertain { path: PathBuf, reason: String },
}

impl fmt::Display for CompactionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidIdentity => f.write_str("compaction job identity or manifest fingerprint is invalid"),
            Self::InvalidLimits => f.write_str("compaction resource limits must be finite, nonzero and internally consistent"),
            Self::LimitExceeded { resource, actual, limit } => {
                write!(f, "compaction {resource} budget exceeded: {actual} > {limit}")
            }
            Self::InvalidTransition { from, to } => write!(f, "invalid compaction job transition: {from:?} -> {to:?}"),
            Self::FirstStateMustBePlanned => f.write_str("the first compaction job event must be Planned"),
            Self::EmptyJournal => f.write_str("cannot replay an empty compaction job journal"),
            Self::JournalIdentityMismatch => f.write_str("compaction journal events do not share one job, attempt and manifest"),
            Self::JournalSequenceMismatch => f.write_str("compaction journal event sequence is not contiguous"),
            Self::SequenceOverflow => f.write_str("compaction journal event sequence overflow"),
            Self::CorruptEvent(message) => write!(f, "corrupt compaction journal event: {message}"),
            Self::Io(message) => write!(f, "compaction journal I/O error: {message}"),
            Self::JournalNotRegularFile => f.write_str("compaction journal path is not a regular file"),
            Self::JournalTooLarge { actual, limit } => write!(f, "compaction journal is too large: {actual} > {limit} bytes"),
            Self::JournalTruncated => f.write_str("compaction journal ends with a partial CJE1 event; preserve it and use a fresh attempt"),
            Self::JournalChangedDuringRead => f.write_str("compaction journal changed while being read"),
            Self::JournalChangedBeforeAppend => f.write_str("compaction journal changed after validation and before append"),
            Self::JournalEventMismatch => f.write_str("compaction journal event does not match the next valid event for this attempt"),
            Self::JournalAppendUncertain { path, reason } => write!(f, "compaction journal append durability is uncertain for {}: {reason}; reload before retrying", path.display()),
        }
    }
}

impl std::error::Error for CompactionError {}

fn copy_into<const N: usize>(target: &mut [u8; N], cursor: &mut usize, source: &[u8]) {
    let end = *cursor + source.len();
    target[*cursor..end].copy_from_slice(source);
    *cursor = end;
}

fn read_u16(bytes: &[u8], offset: usize) -> Result<u16, CompactionError> {
    let slice = bytes.get(offset..offset + 2)
        .ok_or(CompactionError::CorruptEvent("truncated integer"))?;
    Ok(u16::from_le_bytes([slice[0], slice[1]]))
}

fn read_u64(bytes: &[u8], offset: usize) -> Result<u64, CompactionError> {
    let slice = bytes.get(offset..offset + 8)
        .ok_or(CompactionError::CorruptEvent("truncated integer"))?;
    Ok(u64::from_le_bytes([
        slice[0], slice[1], slice[2], slice[3],
        slice[4], slice[5], slice[6], slice[7],
    ]))
}

#[cfg(test)]
mod cim_tests {
    use super::*;

    fn job_id() -> CompactionJobId {
        CompactionJobId::from_bytes([7_u8; 16]).unwrap()
    }

    fn manifest_hash() -> [u8; 32] {
        [9_u8; 32]
    }

    fn progress() -> CompactionProgress {
        CompactionProgress {
            input_bytes: 128,
            output_bytes: 64,
            temporary_bytes: 64,
            working_memory_bytes: 4096,
            rows_processed: 16,
            open_files: 2,
            current_work_unit_rows: 8,
        }
    }

    fn limits() -> CompactionResourceLimits {
        CompactionResourceLimits {
            max_input_bytes: 1024,
            max_output_bytes: 1024,
            max_temporary_bytes: 2048,
            max_working_memory_bytes: 8192,
            max_rows: 1000,
            max_open_files: 8,
            max_work_unit_rows: 64,
        }
    }

    #[test]
    fn records_round_trip_with_checksum() {
        let event = CompactionJobEvent {
            job_id: job_id(),
            attempt: 1,
            event_sequence: 3,
            state: CompactionJobState::Running,
            input_manifest_sha256: manifest_hash(),
            progress: progress(),
        };
        let encoded = event.encode().unwrap();
        assert_eq!(encoded.len(), EVENT_LEN);
        assert_eq!(CompactionJobEvent::decode(&encoded).unwrap(), event);

        let mut corrupted = encoded;
        corrupted[90] ^= 0x40;
        assert!(matches!(
            CompactionJobEvent::decode(&corrupted),
            Err(CompactionError::CorruptEvent("event checksum mismatch"))
        ));
    }

    #[test]
    fn accepts_only_the_documented_forward_lifecycle() {
        let mut journal = CompactionJobJournal::new(job_id(), 1, manifest_hash()).unwrap();
        let states = [
            CompactionJobState::Planned,
            CompactionJobState::ValidatingInputs,
            CompactionJobState::Running,
            CompactionJobState::Verifying,
            CompactionJobState::Publishing,
            CompactionJobState::Completed,
        ];
        let mut events = Vec::new();
        for state in states {
            events.push(journal.append(state, progress()).unwrap());
        }
        let replayed = CompactionJobJournal::replay(&events).unwrap();
        assert_eq!(replayed.last_state(), Some(CompactionJobState::Completed));
        assert_eq!(replayed.last_event_sequence(), 6);
    }

    #[test]
    fn cancellation_is_cooperative_and_forbidden_after_publishing_starts() {
        let mut journal = CompactionJobJournal::new(job_id(), 2, manifest_hash()).unwrap();
        journal.append(CompactionJobState::Planned, progress()).unwrap();
        journal.append(CompactionJobState::ValidatingInputs, progress()).unwrap();
        journal.append(CompactionJobState::Running, progress()).unwrap();
        journal.append(CompactionJobState::CancellationRequested, progress()).unwrap();
        journal.append(CompactionJobState::Cancelled, progress()).unwrap();
        assert_eq!(journal.last_state(), Some(CompactionJobState::Cancelled));

        let mut publishing = CompactionJobJournal::new(job_id(), 3, manifest_hash()).unwrap();
        publishing.append(CompactionJobState::Planned, progress()).unwrap();
        publishing.append(CompactionJobState::ValidatingInputs, progress()).unwrap();
        publishing.append(CompactionJobState::Running, progress()).unwrap();
        publishing.append(CompactionJobState::Verifying, progress()).unwrap();
        publishing.append(CompactionJobState::Publishing, progress()).unwrap();
        assert!(matches!(
            publishing.append(CompactionJobState::CancellationRequested, progress()),
            Err(CompactionError::InvalidTransition {
                from: CompactionJobState::Publishing,
                to: CompactionJobState::CancellationRequested
            })
        ));
    }

    #[test]
    fn rejects_terminal_state_changes_and_invalid_replay() {
        let mut journal = CompactionJobJournal::new(job_id(), 1, manifest_hash()).unwrap();
        let mut events = Vec::new();
        events.push(journal.append(CompactionJobState::Planned, progress()).unwrap());
        events.push(journal.append(CompactionJobState::ValidatingInputs, progress()).unwrap());
        events.push(journal.append(CompactionJobState::Failed, progress()).unwrap());
        assert!(matches!(
            journal.append(CompactionJobState::Running, progress()),
            Err(CompactionError::InvalidTransition { .. })
        ));

        let mut broken = events.clone();
        broken[2].event_sequence = 4;
        assert_eq!(
            CompactionJobJournal::replay(&broken).unwrap_err(),
            CompactionError::JournalSequenceMismatch
        );
    }

    #[test]
    fn enforces_every_finite_resource_budget() {
        let limits = limits();
        limits.validate().unwrap();
        limits.check(&progress()).unwrap();

        let cases = [
            (CompactionProgress { input_bytes: 1025, ..progress() }, "input bytes"),
            (CompactionProgress { output_bytes: 1025, ..progress() }, "output bytes"),
            (CompactionProgress { temporary_bytes: 2049, ..progress() }, "temporary bytes"),
            (CompactionProgress { working_memory_bytes: 8193, ..progress() }, "working memory bytes"),
            (CompactionProgress { rows_processed: 1001, ..progress() }, "rows"),
            (CompactionProgress { open_files: 9, ..progress() }, "open files"),
            (CompactionProgress { current_work_unit_rows: 65, ..progress() }, "work-unit rows"),
        ];
        for (usage, resource) in cases {
            assert!(matches!(
                limits.check(&usage),
                Err(CompactionError::LimitExceeded { resource: actual, .. }) if actual == resource
            ));
        }
    }

    #[test]
    fn rejects_zero_and_inconsistent_budgets() {
        let invalid = CompactionResourceLimits { max_rows: 0, ..limits() };
        assert_eq!(invalid.validate().unwrap_err(), CompactionError::InvalidLimits);
        let invalid_work_unit = CompactionResourceLimits { max_work_unit_rows: 1001, ..limits() };
        assert_eq!(invalid_work_unit.validate().unwrap_err(), CompactionError::InvalidLimits);
    }

    #[test]
    fn rejects_zero_identity_and_manifest_fingerprint() {
        assert_eq!(CompactionJobId::from_bytes([0; 16]).unwrap_err(), CompactionError::InvalidIdentity);
        assert_eq!(
            CompactionJobJournal::new(job_id(), 1, [0; 32]).unwrap_err(),
            CompactionError::InvalidIdentity
        );
    }

    fn temp_journal_path(label: &str) -> PathBuf {
        use std::time::{SystemTime, UNIX_EPOCH};
        let nonce = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
        let directory = std::env::temp_dir().join(format!("ananta-compaction-{label}-{nonce}"));
        fs::create_dir_all(&directory).unwrap();
        directory.join("job.cje")
    }

    #[test]
    fn persists_events_and_recovers_unfinished_attempt_once() {
        let path = temp_journal_path("recover");
        let mut journal = CompactionJobJournal::new(job_id(), 4, manifest_hash()).unwrap();
        for state in [
            CompactionJobState::Planned,
            CompactionJobState::ValidatingInputs,
            CompactionJobState::Running,
        ] {
            let event = journal.append(state, progress()).unwrap();
            append_compaction_journal_event(&path, &event).unwrap();
        }

        let stored = read_compaction_journal(&path).unwrap();
        assert_eq!(stored.len(), 3);
        assert_eq!(stored.last().unwrap().state, CompactionJobState::Running);

        assert_eq!(recover_compaction_journal(&path).unwrap(), CompactionJobState::Interrupted);
        let recovered = read_compaction_journal(&path).unwrap();
        assert_eq!(recovered.len(), 4);
        assert_eq!(recovered.last().unwrap().state, CompactionJobState::Interrupted);
        assert_eq!(recovered.last().unwrap().event_sequence, 4);

        let length = fs::metadata(&path).unwrap().len();
        assert_eq!(recover_compaction_journal(&path).unwrap(), CompactionJobState::Interrupted);
        assert_eq!(fs::metadata(&path).unwrap().len(), length);

        fs::remove_dir_all(path.parent().unwrap()).unwrap();
    }

    #[test]
    fn completed_journal_is_not_modified_by_recovery() {
        let path = temp_journal_path("completed");
        let mut journal = CompactionJobJournal::new(job_id(), 5, manifest_hash()).unwrap();
        for state in [
            CompactionJobState::Planned,
            CompactionJobState::ValidatingInputs,
            CompactionJobState::Running,
            CompactionJobState::Verifying,
            CompactionJobState::Publishing,
            CompactionJobState::Completed,
        ] {
            let event = journal.append(state, progress()).unwrap();
            append_compaction_journal_event(&path, &event).unwrap();
        }

        let length = fs::metadata(&path).unwrap().len();
        assert_eq!(recover_compaction_journal(&path).unwrap(), CompactionJobState::Completed);
        assert_eq!(fs::metadata(&path).unwrap().len(), length);

        fs::remove_dir_all(path.parent().unwrap()).unwrap();
    }

    #[test]
    fn refuses_to_append_to_a_partial_tail_without_truncating_it() {
        let path = temp_journal_path("partial");
        let mut journal = CompactionJobJournal::new(job_id(), 6, manifest_hash()).unwrap();
        let planned = journal.append(CompactionJobState::Planned, progress()).unwrap();
        append_compaction_journal_event(&path, &planned).unwrap();

        let mut file = OpenOptions::new().append(true).open(&path).unwrap();
        file.write_all(&[0x43, 0x4a, 0x45]).unwrap();
        file.sync_all().unwrap();
        drop(file);
        let damaged_len = fs::metadata(&path).unwrap().len();

        let next = journal.append(CompactionJobState::ValidatingInputs, progress()).unwrap();
        assert_eq!(append_compaction_journal_event(&path, &next).unwrap_err(), CompactionError::JournalTruncated);
        assert_eq!(recover_compaction_journal(&path).unwrap_err(), CompactionError::JournalTruncated);
        assert_eq!(fs::metadata(&path).unwrap().len(), damaged_len);

        fs::remove_dir_all(path.parent().unwrap()).unwrap();
    }

    #[test]
    fn rejects_mismatched_event_and_checksum_damage_on_disk() {
        let path = temp_journal_path("mismatch");
        let mut journal = CompactionJobJournal::new(job_id(), 7, manifest_hash()).unwrap();
        let planned = journal.append(CompactionJobState::Planned, progress()).unwrap();
        append_compaction_journal_event(&path, &planned).unwrap();

        let mut wrong = journal.append(CompactionJobState::ValidatingInputs, progress()).unwrap();
        wrong.event_sequence = 3;
        assert_eq!(append_compaction_journal_event(&path, &wrong).unwrap_err(), CompactionError::JournalEventMismatch);

        let mut bytes = fs::read(&path).unwrap();
        bytes[90] ^= 1;
        fs::write(&path, &bytes).unwrap();
        assert!(matches!(
            read_compaction_journal(&path),
            Err(CompactionError::CorruptEvent("event checksum mismatch"))
        ));

        fs::remove_dir_all(path.parent().unwrap()).unwrap();
    }

}
