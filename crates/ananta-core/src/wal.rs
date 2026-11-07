//! Append-only write-ahead log framing for ANANTA.
//!
//! M6.1 provides the versioned writer/reader primitives. The database mutation
//! paths are deliberately not routed through this module until M6.2 defines
//! replay and publication ordering; this module alone does not imply durability.

use crate::chronicle::sha256;
use std::collections::BTreeMap;
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

const FILE_MAGIC: &[u8; 4] = b"ANWL";
const FILE_VERSION: u16 = 1;
const FILE_HEADER_LEN: usize = 8;
const FRAME_PREFIX_LEN: usize = 4;
const FRAME_FIXED_BODY_LEN: usize = 8 + 8 + 1 + 32;
const MAX_WAL_RECORD_BYTES: usize = 64 * 1024 * 1024;

#[derive(Debug)]
pub enum WalError {
    Io(io::Error),
    InvalidHeader,
    UnsupportedVersion(u16),
    Corrupt(&'static str),
    InvalidRecord(String),
    RecordTooLarge(usize),
    SequenceOverflow,
    WriterPoisoned,
}

impl fmt::Display for WalError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "WAL I/O error: {error}"),
            Self::InvalidHeader => formatter.write_str("invalid ANANTA WAL file header"),
            Self::UnsupportedVersion(version) => {
                write!(formatter, "unsupported ANANTA WAL version {version}")
            }
            Self::Corrupt(message) => write!(formatter, "corrupt ANANTA WAL: {message}"),
            Self::InvalidRecord(message) => write!(formatter, "invalid WAL record: {message}"),
            Self::RecordTooLarge(size) => {
                write!(formatter, "WAL record size {size} exceeds the configured limit")
            }
            Self::SequenceOverflow => formatter.write_str("WAL sequence number overflow"),
            Self::WriterPoisoned => formatter.write_str(
                "WAL writer is poisoned after a partial write; reopen and inspect the log",
            ),
        }
    }
}

impl std::error::Error for WalError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            _ => None,
        }
    }
}

impl From<io::Error> for WalError {
    fn from(value: io::Error) -> Self {
        Self::Io(value)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum WalRecordKind {
    Begin = 1,
    Mutation = 2,
    Commit = 3,
    Abort = 4,
    Checkpoint = 5,
}

impl WalRecordKind {
    fn from_byte(value: u8) -> Result<Self, WalError> {
        match value {
            1 => Ok(Self::Begin),
            2 => Ok(Self::Mutation),
            3 => Ok(Self::Commit),
            4 => Ok(Self::Abort),
            5 => Ok(Self::Checkpoint),
            _ => Err(WalError::Corrupt("unknown record kind")),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalRecord {
    pub sequence: u64,
    /// Zero is reserved for checkpoint records. Transaction records require a
    /// non-zero ID so their begin/mutation/commit frames can be correlated.
    pub transaction_id: u64,
    pub kind: WalRecordKind,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalSyncPolicy {
    /// The caller must explicitly call sync(); no append implicitly flushes.
    Manual,
    /// Commit and checkpoint records sync all preceding WAL frames.
    OnCommit,
    /// Every appended frame is synchronised before the append returns.
    EveryRecord,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct TransactionState {
    mutation_count: u64,
    rolling_digest: [u8; 32],
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalReadReport {
    pub records: Vec<WalRecord>,
    pub last_sequence: u64,
    /// Byte offset through the last complete, verified frame.
    pub valid_bytes: u64,
    /// True when the final frame prefix or body ended early. A reader accepts
    /// this as an incomplete tail; it never treats a checksum mismatch as tail.
    pub truncated_tail: bool,
    /// Transactions with BEGIN but no COMMIT/ABORT marker at end-of-file.
    pub incomplete_transaction_ids: Vec<u64>,
    active_transactions: BTreeMap<u64, TransactionState>,
}

impl WalReadReport {
    pub fn is_clean(&self) -> bool {
        !self.truncated_tail && self.incomplete_transaction_ids.is_empty()
    }
}

pub struct WalWriter {
    path: PathBuf,
    file: File,
    next_sequence: u64,
    active_transactions: BTreeMap<u64, TransactionState>,
    sync_policy: WalSyncPolicy,
    poisoned: bool,
}

impl WalWriter {
    /// Open an append-only WAL. An incomplete final frame is truncated to the
    /// last valid boundary; a complete corrupt frame is returned as an error.
    pub fn open(
        path: impl AsRef<Path>,
        sync_policy: WalSyncPolicy,
    ) -> Result<Self, WalError> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent().filter(|parent| !parent.as_os_str().is_empty()) {
            fs::create_dir_all(parent)?;
        }

        let mut open_options = OpenOptions::new();
        open_options.create(true).read(true).write(true);
        #[cfg(windows)]
        {
            use std::os::windows::fs::OpenOptionsExt;
            const FILE_SHARE_READ: u32 = 0x0000_0001;
            const FILE_SHARE_WRITE: u32 = 0x0000_0002;
            const FILE_SHARE_DELETE: u32 = 0x0000_0004;
            // Windows cannot replace a file while any open handle denies
            // delete-sharing. Keep the writer handle compatible with atomic
            // MoveFileExW replacement throughout the WAL's lifetime.
            open_options.share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE);
        }
        let mut file = open_options.open(&path)?;

        let length = file.metadata()?.len();
        if length == 0 {
            file.write_all(FILE_MAGIC)?;
            file.write_all(&FILE_VERSION.to_le_bytes())?;
            file.write_all(&(FILE_HEADER_LEN as u16).to_le_bytes())?;
            file.sync_all()?;
            return Ok(Self {
                path,
                file,
                next_sequence: 1,
                active_transactions: BTreeMap::new(),
                sync_policy,
                poisoned: false,
            });
        }

        let report = read_wal(&path)?;
        if report.truncated_tail {
            file.set_len(report.valid_bytes)?;
            file.sync_all()?;
        }
        file.seek(SeekFrom::End(0))?;
        let next_sequence = report
            .last_sequence
            .checked_add(1)
            .ok_or(WalError::SequenceOverflow)?;

        Ok(Self {
            path,
            file,
            next_sequence,
            active_transactions: report.active_transactions,
            sync_policy,
            poisoned: false,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn next_sequence(&self) -> u64 {
        self.next_sequence
    }

    pub fn incomplete_transaction_ids(&self) -> Vec<u64> {
        self.active_transactions.keys().copied().collect()
    }

    pub fn begin_transaction(
        &mut self,
        transaction_id: u64,
        metadata: &[u8],
    ) -> Result<WalRecord, WalError> {
        self.append(WalRecordKind::Begin, transaction_id, metadata)
    }

    /// Append an opaque mutation payload. The payload format belongs to the
    /// operation layer (ANF insert, Chronicle ingest, catalog mutation, etc.).
    pub fn append_mutation(
        &mut self,
        transaction_id: u64,
        mutation_payload: &[u8],
    ) -> Result<WalRecord, WalError> {
        self.append(WalRecordKind::Mutation, transaction_id, mutation_payload)
    }

    /// Append a commit marker containing the mutation count and rolling digest.
    /// Under OnCommit/EveryRecord, the commit marker synchronises the log.
    pub fn commit_transaction(
        &mut self,
        transaction_id: u64,
    ) -> Result<WalRecord, WalError> {
        let state = self.active_transactions.get(&transaction_id)
            .ok_or_else(|| WalError::InvalidRecord(
                format!("transaction {transaction_id} has no BEGIN record"),
            ))?;
        let mut payload = Vec::with_capacity(40);
        payload.extend_from_slice(&state.mutation_count.to_le_bytes());
        payload.extend_from_slice(&state.rolling_digest);
        self.append(WalRecordKind::Commit, transaction_id, &payload)
    }

    pub fn abort_transaction(
        &mut self,
        transaction_id: u64,
        reason: &[u8],
    ) -> Result<WalRecord, WalError> {
        self.append(WalRecordKind::Abort, transaction_id, reason)
    }

    pub fn append_checkpoint(&mut self, payload: &[u8]) -> Result<WalRecord, WalError> {
        self.append(WalRecordKind::Checkpoint, 0, payload)
    }

    pub fn sync(&mut self) -> Result<(), WalError> {
        self.ensure_healthy()?;
        self.file.sync_all()?;
        Ok(())
    }

    /// Atomically replace the current WAL with a new log whose first frame
    /// anchors a separately synced, checksummed database checkpoint manifest.
    ///
    /// This operation is currently enabled on Unix, where rename atomically
    /// replaces the existing pathname while the old file handle remains open.
    /// Other targets fail closed until a platform-specific atomic replace is
    /// implemented; they never emulate replacement with delete-then-rename.
    pub(crate) fn replace_with_checkpoint(&mut self, base_sequence: u64, payload: &[u8]) -> Result<(), WalError> {
        self.ensure_healthy()?;
        if !self.active_transactions.is_empty() {
            return Err(WalError::InvalidRecord(
                "WAL rotation requires no open transactions".into(),
            ));
        }

        #[cfg(not(any(unix, windows)))]
        {
            let _ = payload;
            return Err(WalError::Io(io::Error::new(
                io::ErrorKind::Unsupported,
                "atomic WAL replacement is not implemented on this platform",
            )));
        }

        #[cfg(any(unix, windows))]
        {
            self.file.sync_all()?;
            let parent = self.path.parent()
                .filter(|path| !path.as_os_str().is_empty())
                .unwrap_or(Path::new("."));
            let filename = self.path.file_name()
                .ok_or(WalError::InvalidHeader)?
                .to_string_lossy();
            let temporary = parent.join(format!(".{filename}.checkpoint.tmp"));

            match fs::symlink_metadata(&temporary) {
                Ok(metadata) if metadata.file_type().is_file() => fs::remove_file(&temporary)?,
                Ok(_) => return Err(WalError::InvalidRecord(
                    "WAL checkpoint temporary path is not a regular file".into(),
                )),
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => return Err(WalError::Io(error)),
            }

            let checkpoint_sequence = base_sequence
                .checked_add(1)
                .ok_or(WalError::SequenceOverflow)?;
            if self.next_sequence != checkpoint_sequence {
                return Err(WalError::InvalidRecord(
                    "checkpoint base sequence differs from the WAL high-water mark".into(),
                ));
            }
            let checkpoint = WalRecord {
                sequence: checkpoint_sequence,
                transaction_id: 0,
                kind: WalRecordKind::Checkpoint,
                payload: payload.to_vec(),
            };
            apply_transaction_record(&mut BTreeMap::new(), &checkpoint)?;
            let encoded = encode_record(&checkpoint)?;
            let mut staged = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&temporary)?;
            staged.write_all(FILE_MAGIC)?;
            staged.write_all(&FILE_VERSION.to_le_bytes())?;
            staged.write_all(&(FILE_HEADER_LEN as u16).to_le_bytes())?;
            staged.write_all(&encoded)?;
            staged.flush()?;
            staged.sync_all()?;
            drop(staged);
            sync_wal_directory(parent)?;
            crate::wal_recovery::maybe_crash_after(
                crate::wal_recovery::WalRecoveryCrashPoint::AfterCheckpointTempSynced,
            );

            // Platform replacement atomically switches the WAL directory entry.
            // Until this point the old complete log remains authoritative.
            ananta_platform::atomic_replace(&temporary, &self.path)?;
            crate::wal_recovery::maybe_crash_after(
                crate::wal_recovery::WalRecoveryCrashPoint::AfterCheckpointRenamedBeforeDirectorySync,
            );
            sync_wal_directory(parent)?;
            crate::wal_recovery::maybe_crash_after(
                crate::wal_recovery::WalRecoveryCrashPoint::AfterCheckpointDirectorySync,
            );

            let mut replacement_options = OpenOptions::new();
            replacement_options.read(true).append(true);
            #[cfg(windows)]
            {
                use std::os::windows::fs::OpenOptionsExt;
                const FILE_SHARE_READ: u32 = 0x0000_0001;
                const FILE_SHARE_WRITE: u32 = 0x0000_0002;
                const FILE_SHARE_DELETE: u32 = 0x0000_0004;
                replacement_options.share_mode(
                    FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
                );
            }
            let replacement = replacement_options.open(&self.path)?;
            self.file = replacement;
            self.next_sequence = base_sequence.checked_add(2).ok_or(WalError::SequenceOverflow)?;
            self.active_transactions.clear();
            self.poisoned = false;
            Ok(())
        }
    }

    fn append(
        &mut self,
        kind: WalRecordKind,
        transaction_id: u64,
        payload: &[u8],
    ) -> Result<WalRecord, WalError> {
        self.ensure_healthy()?;
        let sequence = self.next_sequence;
        let following_sequence = sequence
            .checked_add(1)
            .ok_or(WalError::SequenceOverflow)?;
        let record = WalRecord {
            sequence,
            transaction_id,
            kind,
            payload: payload.to_vec(),
        };
        let mut next_transactions = self.active_transactions.clone();
        apply_transaction_record(&mut next_transactions, &record)?;
        let encoded = encode_record(&record)?;

        if let Err(error) = self.file.write_all(&encoded) {
            self.poisoned = true;
            return Err(WalError::Io(error));
        }

        self.active_transactions = next_transactions;
        self.next_sequence = following_sequence;

        let should_sync = self.sync_policy == WalSyncPolicy::EveryRecord
            || (self.sync_policy == WalSyncPolicy::OnCommit
                && matches!(kind, WalRecordKind::Commit | WalRecordKind::Checkpoint));
        if should_sync {
            if let Err(error) = self.file.sync_data() {
                self.poisoned = true;
                return Err(WalError::Io(error));
            }
        }
        Ok(record)
    }

    fn ensure_healthy(&self) -> Result<(), WalError> {
        if self.poisoned {
            Err(WalError::WriterPoisoned)
        } else {
            Ok(())
        }
    }
}

#[cfg(unix)]
fn sync_wal_directory(path: &Path) -> io::Result<()> {
    File::open(path)?.sync_all()
}

// Windows replacement uses MOVEFILE_WRITE_THROUGH, which flushes the move
// before returning. Rust does not expose a portable directory-handle flush.
#[cfg(windows)]
fn sync_wal_directory(_path: &Path) -> io::Result<()> {
    Ok(())
}

/// Read and verify a WAL. Complete frames are checksum/sequence/transaction
/// validated. Only an incomplete final length prefix or frame body is accepted
/// as a truncated tail; corruption in a complete frame is always an error.
pub fn read_wal(path: impl AsRef<Path>) -> Result<WalReadReport, WalError> {
    let mut file = File::open(path)?;
    let mut header = [0_u8; FILE_HEADER_LEN];
    file.read_exact(&mut header).map_err(|_| WalError::InvalidHeader)?;
    if &header[..4] != FILE_MAGIC {
        return Err(WalError::InvalidHeader);
    }
    let version = u16::from_le_bytes([header[4], header[5]]);
    if version != FILE_VERSION {
        return Err(WalError::UnsupportedVersion(version));
    }
    let header_len = u16::from_le_bytes([header[6], header[7]]) as usize;
    if header_len != FILE_HEADER_LEN {
        return Err(WalError::InvalidHeader);
    }

    let mut report = WalReadReport {
        records: Vec::new(),
        last_sequence: 0,
        valid_bytes: FILE_HEADER_LEN as u64,
        truncated_tail: false,
        incomplete_transaction_ids: Vec::new(),
        active_transactions: BTreeMap::new(),
    };

    loop {
        let frame_start = file.stream_position()?;
        let mut length_prefix = [0_u8; FRAME_PREFIX_LEN];
        let mut prefix_read = 0;
        while prefix_read < FRAME_PREFIX_LEN {
            match file.read(&mut length_prefix[prefix_read..]) {
                Ok(0) => {
                    if prefix_read > 0 {
                        report.truncated_tail = true;
                    }
                    break;
                }
                Ok(read) => prefix_read += read,
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                Err(error) => return Err(WalError::Io(error)),
            }
        }
        if prefix_read == 0 || prefix_read < FRAME_PREFIX_LEN {
            if prefix_read > 0 {
                report.truncated_tail = true;
            }
            break;
        }

        let body_len = u32::from_le_bytes(length_prefix) as usize;
        if body_len < FRAME_FIXED_BODY_LEN {
            return Err(WalError::Corrupt("frame is shorter than the fixed header"));
        }
        if body_len > MAX_WAL_RECORD_BYTES {
            return Err(WalError::RecordTooLarge(body_len));
        }

        let mut body = vec![0_u8; body_len];
        let mut body_read = 0;
        while body_read < body_len {
            match file.read(&mut body[body_read..]) {
                Ok(0) => {
                    report.truncated_tail = true;
                    break;
                }
                Ok(read) => body_read += read,
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                Err(error) => return Err(WalError::Io(error)),
            }
        }
        if body_read < body_len {
            break;
        }

        let content_len = body_len - 32;
        let content = &body[..content_len];
        let checksum = &body[content_len..];
        if sha256(content).as_slice() != checksum {
            return Err(WalError::Corrupt("frame checksum mismatch"));
        }

        let sequence = u64::from_le_bytes(
            content[0..8].try_into().map_err(|_| WalError::Corrupt("invalid sequence"))?,
        );
        let transaction_id = u64::from_le_bytes(
            content[8..16].try_into().map_err(|_| WalError::Corrupt("invalid transaction ID"))?,
        );
        let kind = WalRecordKind::from_byte(content[16])?;
        let expected_sequence = if report.records.is_empty()
            && kind == WalRecordKind::Checkpoint
            && transaction_id == 0
        {
            // A rotated WAL begins with its checkpoint at the previous
            // high-water mark plus one, preserving globally monotonic IDs.
            sequence
        } else {
            report.last_sequence.checked_add(1).ok_or(WalError::SequenceOverflow)?
        };
        if sequence == 0 || sequence != expected_sequence {
            return Err(WalError::Corrupt("sequence is not contiguous"));
        }
        let payload = content[17..].to_vec();
        let record = WalRecord {
            sequence,
            transaction_id,
            kind,
            payload,
        };
        apply_transaction_record(&mut report.active_transactions, &record)?;
        report.last_sequence = sequence;
        report.valid_bytes = frame_start
            .checked_add(FRAME_PREFIX_LEN as u64)
            .and_then(|value| value.checked_add(body_len as u64))
            .ok_or(WalError::Corrupt("file offset overflow"))?;
        report.records.push(record);
    }

    report.incomplete_transaction_ids = report.active_transactions.keys().copied().collect();
    Ok(report)
}

fn encode_record(record: &WalRecord) -> Result<Vec<u8>, WalError> {
    let mut content = Vec::with_capacity(17 + record.payload.len());
    content.extend_from_slice(&record.sequence.to_le_bytes());
    content.extend_from_slice(&record.transaction_id.to_le_bytes());
    content.push(record.kind as u8);
    content.extend_from_slice(&record.payload);
    let checksum = sha256(&content);
    let body_len = content.len()
        .checked_add(checksum.len())
        .ok_or(WalError::RecordTooLarge(usize::MAX))?;
    if body_len > MAX_WAL_RECORD_BYTES {
        return Err(WalError::RecordTooLarge(body_len));
    }
    let body_len_u32 = u32::try_from(body_len)
        .map_err(|_| WalError::RecordTooLarge(body_len))?;
    let mut encoded = Vec::with_capacity(FRAME_PREFIX_LEN + body_len);
    encoded.extend_from_slice(&body_len_u32.to_le_bytes());
    encoded.extend_from_slice(&content);
    encoded.extend_from_slice(&checksum);
    Ok(encoded)
}

fn apply_transaction_record(
    transactions: &mut BTreeMap<u64, TransactionState>,
    record: &WalRecord,
) -> Result<(), WalError> {
    match record.kind {
        WalRecordKind::Begin => {
            if record.transaction_id == 0 || transactions.contains_key(&record.transaction_id) {
                return Err(WalError::InvalidRecord(
                    "BEGIN requires a unique non-zero transaction ID".into(),
                ));
            }
            transactions.insert(record.transaction_id, TransactionState::default());
        }
        WalRecordKind::Mutation => {
            if record.transaction_id == 0 {
                return Err(WalError::InvalidRecord(
                    "MUTATION requires a non-zero transaction ID".into(),
                ));
            }
            let state = transactions.get_mut(&record.transaction_id).ok_or_else(|| {
                WalError::InvalidRecord("MUTATION has no open transaction".into())
            })?;
            let payload_len = u64::try_from(record.payload.len())
                .map_err(|_| WalError::RecordTooLarge(record.payload.len()))?;
            let mut digest_input = Vec::with_capacity(40 + record.payload.len());
            digest_input.extend_from_slice(&state.rolling_digest);
            digest_input.extend_from_slice(&payload_len.to_le_bytes());
            digest_input.extend_from_slice(&record.payload);
            state.rolling_digest = sha256(&digest_input);
            state.mutation_count = state.mutation_count
                .checked_add(1)
                .ok_or(WalError::SequenceOverflow)?;
        }
        WalRecordKind::Commit => {
            if record.transaction_id == 0 {
                return Err(WalError::InvalidRecord(
                    "COMMIT requires a non-zero transaction ID".into(),
                ));
            }
            let state = transactions.get(&record.transaction_id).ok_or_else(|| {
                WalError::InvalidRecord("COMMIT has no open transaction".into())
            })?;
            if record.payload.len() != 40 {
                return Err(WalError::Corrupt("COMMIT marker has an invalid payload length"));
            }
            let mutation_count = u64::from_le_bytes(
                record.payload[..8].try_into()
                    .map_err(|_| WalError::Corrupt("invalid COMMIT mutation count"))?,
            );
            if mutation_count != state.mutation_count
                || record.payload[8..] != state.rolling_digest[..]
            {
                return Err(WalError::Corrupt("COMMIT mutation digest/count mismatch"));
            }
            transactions.remove(&record.transaction_id);
        }
        WalRecordKind::Abort => {
            if record.transaction_id == 0 || transactions.remove(&record.transaction_id).is_none() {
                return Err(WalError::InvalidRecord(
                    "ABORT requires an open non-zero transaction".into(),
                ));
            }
        }
        WalRecordKind::Checkpoint => {
            if record.transaction_id != 0 || !transactions.is_empty() {
                return Err(WalError::InvalidRecord(
                    "CHECKPOINT requires transaction ID zero and no open transactions".into(),
                ));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn path(label: &str) -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("ananta-wal-{label}-{nonce}.wal"))
    }

    #[test]
    fn wal_round_trips_transaction_commit_and_checkpoint() {
        let path = path("roundtrip");
        let mut writer = WalWriter::open(&path, WalSyncPolicy::OnCommit).unwrap();
        writer.begin_transaction(7, b"insert-batch").unwrap();
        writer.append_mutation(7, b"table=3;segment=9;payload=abc").unwrap();
        writer.append_mutation(7, b"table=3;segment=10;payload=def").unwrap();
        writer.commit_transaction(7).unwrap();
        writer.append_checkpoint(&[1, 0, 0, 0, 0, 0, 0, 0]).unwrap();
        assert_eq!(writer.next_sequence(), 6);
        writer.sync().unwrap();
        drop(writer);

        let report = read_wal(&path).unwrap();
        assert!(report.is_clean());
        assert_eq!(report.records.len(), 5);
        assert_eq!(report.records[0].kind, WalRecordKind::Begin);
        assert_eq!(report.records[3].kind, WalRecordKind::Commit);
        assert_eq!(report.records[4].kind, WalRecordKind::Checkpoint);
        assert_eq!(report.last_sequence, 5);
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn wal_checkpoint_replacement_is_atomic_and_preserves_sequence() {
        let path = path("checkpoint-replace");
        let mut writer = WalWriter::open(&path, WalSyncPolicy::OnCommit).unwrap();
        writer.begin_transaction(41, b"before-checkpoint").unwrap();
        writer.commit_transaction(41).unwrap();
        let before = read_wal(&path).unwrap();
        let base_sequence = before.last_sequence;

        writer.replace_with_checkpoint(base_sequence, b"checkpoint-manifest").unwrap();
        assert_eq!(writer.next_sequence(), base_sequence + 2);
        let checkpoint = read_wal(&path).unwrap();
        assert_eq!(checkpoint.records.len(), 1);
        assert_eq!(checkpoint.records[0].kind, WalRecordKind::Checkpoint);
        assert_eq!(checkpoint.records[0].sequence, base_sequence + 1);
        assert_eq!(checkpoint.records[0].payload, b"checkpoint-manifest");

        let next_transaction = base_sequence + 2;
        writer.begin_transaction(next_transaction, b"after-checkpoint").unwrap();
        writer.commit_transaction(next_transaction).unwrap();
        drop(writer);
        let after = read_wal(&path).unwrap();
        assert!(after.is_clean());
        assert_eq!(after.records.len(), 3);
        assert_eq!(after.records[1].sequence, next_transaction);
        assert_eq!(after.records[2].sequence, next_transaction + 1);
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn wal_reader_reports_and_writer_truncates_an_incomplete_tail() {
        let path = path("truncated");
        let mut writer = WalWriter::open(&path, WalSyncPolicy::Manual).unwrap();
        writer.begin_transaction(11, b"metadata").unwrap();
        writer.append_mutation(11, b"mutation-one").unwrap();
        writer.sync().unwrap();
        drop(writer);

        {
            let mut file = OpenOptions::new().append(true).open(&path).unwrap();
            file.write_all(&[20, 0]).unwrap(); // incomplete four-byte frame length
            file.sync_all().unwrap();
        }
        let report = read_wal(&path).unwrap();
        assert!(report.truncated_tail);
        assert_eq!(report.incomplete_transaction_ids, vec![11]);

        let mut writer = WalWriter::open(&path, WalSyncPolicy::OnCommit).unwrap();
        assert_eq!(writer.incomplete_transaction_ids(), vec![11]);
        writer.abort_transaction(11, b"recovered as uncommitted").unwrap();
        writer.begin_transaction(12, b"next transaction").unwrap();
        writer.commit_transaction(12).unwrap();
        drop(writer);

        let recovered = read_wal(&path).unwrap();
        assert!(!recovered.truncated_tail);
        assert!(recovered.incomplete_transaction_ids.is_empty());
        assert_eq!(recovered.records.last().unwrap().kind, WalRecordKind::Commit);
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn wal_reader_rejects_a_complete_frame_with_a_bad_checksum() {
        let path = path("corrupt");
        let mut writer = WalWriter::open(&path, WalSyncPolicy::EveryRecord).unwrap();
        writer.begin_transaction(21, b"metadata").unwrap();
        writer.append_mutation(21, b"payload").unwrap();
        writer.commit_transaction(21).unwrap();
        drop(writer);

        let mut bytes = fs::read(&path).unwrap();
        let last = bytes.len() - 1;
        bytes[last] ^= 0x80;
        fs::write(&path, bytes).unwrap();

        assert!(matches!(
            read_wal(&path),
            Err(WalError::Corrupt("frame checksum mismatch"))
        ));
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn wal_sync_policy_manual_requires_an_explicit_sync() {
        let path = path("manual-sync");
        let mut writer = WalWriter::open(&path, WalSyncPolicy::Manual).unwrap();
        writer.begin_transaction(31, b"metadata").unwrap();
        writer.commit_transaction(31).unwrap();
        writer.sync().unwrap();
        drop(writer);
        assert!(read_wal(&path).unwrap().is_clean());
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn wal_reader_reports_a_truncated_frame_body_without_discarding_prior_records() {
        let path = path("truncated-body");
        let mut writer = WalWriter::open(&path, WalSyncPolicy::Manual).unwrap();
        writer.begin_transaction(41, b"metadata").unwrap();
        writer.sync().unwrap();
        drop(writer);

        let valid_record = WalRecord {
            sequence: 2,
            transaction_id: 41,
            kind: WalRecordKind::Mutation,
            payload: b"partially-written-body".to_vec(),
        };
        let encoded = encode_record(&valid_record).unwrap();
        let valid_end = fs::metadata(&path).unwrap().len();
        {
            let mut file = OpenOptions::new().append(true).open(&path).unwrap();
            file.write_all(&encoded[..encoded.len() - 7]).unwrap();
            file.sync_all().unwrap();
        }

        let report = read_wal(&path).unwrap();
        assert!(report.truncated_tail);
        assert_eq!(report.records.len(), 1);
        assert_eq!(report.last_sequence, 1);
        assert_eq!(report.valid_bytes, valid_end);
        assert_eq!(report.incomplete_transaction_ids, vec![41]);

        let repaired = WalWriter::open(&path, WalSyncPolicy::Manual).unwrap();
        drop(repaired);
        let after_open = read_wal(&path).unwrap();
        assert!(!after_open.truncated_tail);
        assert_eq!(after_open.records.len(), 1);
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn wal_reader_rejects_a_complete_frame_with_a_sequence_gap() {
        let path = path("sequence-gap");
        {
            let mut file = File::create(&path).unwrap();
            file.write_all(FILE_MAGIC).unwrap();
            file.write_all(&FILE_VERSION.to_le_bytes()).unwrap();
            file.write_all(&(FILE_HEADER_LEN as u16).to_le_bytes()).unwrap();
            let record = WalRecord {
                sequence: 2,
                transaction_id: 51,
                kind: WalRecordKind::Begin,
                payload: b"metadata".to_vec(),
            };
            file.write_all(&encode_record(&record).unwrap()).unwrap();
            file.sync_all().unwrap();
        }
        assert!(matches!(
            read_wal(&path),
            Err(WalError::Corrupt("sequence is not contiguous"))
        ));
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn wal_reader_rejects_a_commit_with_a_recomputed_bad_transaction_digest() {
        let path = path("commit-digest");
        {
            let mut file = File::create(&path).unwrap();
            file.write_all(FILE_MAGIC).unwrap();
            file.write_all(&FILE_VERSION.to_le_bytes()).unwrap();
            file.write_all(&(FILE_HEADER_LEN as u16).to_le_bytes()).unwrap();
            let begin = WalRecord {
                sequence: 1,
                transaction_id: 61,
                kind: WalRecordKind::Begin,
                payload: b"metadata".to_vec(),
            };
            let mutation = WalRecord {
                sequence: 2,
                transaction_id: 61,
                kind: WalRecordKind::Mutation,
                payload: b"real mutation".to_vec(),
            };
            let bad_commit = WalRecord {
                sequence: 3,
                transaction_id: 61,
                kind: WalRecordKind::Commit,
                payload: [99_u64.to_le_bytes().as_slice(), &[0_u8; 32]].concat(),
            };
            for record in [&begin, &mutation, &bad_commit] {
                file.write_all(&encode_record(record).unwrap()).unwrap();
            }
            file.sync_all().unwrap();
        }
        assert!(matches!(
            read_wal(&path),
            Err(WalError::Corrupt("COMMIT mutation digest/count mismatch"))
        ));
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn wal_rejects_invalid_transaction_ordering() {
        let path = path("ordering");
        let mut writer = WalWriter::open(&path, WalSyncPolicy::Manual).unwrap();
        assert!(matches!(
            writer.append_mutation(99, b"orphan"),
            Err(WalError::InvalidRecord(_))
        ));
        writer.begin_transaction(99, b"metadata").unwrap();
        assert!(matches!(
            writer.begin_transaction(99, b"duplicate"),
            Err(WalError::InvalidRecord(_))
        ));
        writer.abort_transaction(99, b"done").unwrap();
        drop(writer);
        fs::remove_file(path).unwrap();
    }
}
