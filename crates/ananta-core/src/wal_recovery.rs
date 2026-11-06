//! Typed mutation intents and recovery-plan construction for the ANWL log.
//!
//! This module does not publish files or mutate the database. It validates the
//! WAL payload contract and classifies committed versus incomplete transactions
//! so Database::open can apply the recovery protocol without reinterpreting
//! opaque payload bytes at every call site.

use crate::wal::{WalReadReport, WalRecordKind, WalWriter};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

const INTENT_MAGIC: &[u8; 4] = b"AMUT";
const INTENT_VERSION: u16 = 1;
const INTENT_LEN: usize = 4 + 2 + 1 + 8 + 8 + 8 + 8 + 32;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum WalArtifactKind {
    AnfSegment = 1,
    ChronicleRecord = 2,
    SchemaSnapshot = 3,
    CatalogSnapshot = 4,
}

impl WalArtifactKind {
    fn from_byte(value: u8) -> Result<Self, WalRecoveryError> {
        match value {
            1 => Ok(Self::AnfSegment),
            2 => Ok(Self::ChronicleRecord),
            3 => Ok(Self::SchemaSnapshot),
            4 => Ok(Self::CatalogSnapshot),
            _ => Err(WalRecoveryError::CorruptIntent("unknown artifact kind")),
        }
    }

    pub fn final_file_name(self, file_id: u64) -> String {
        match self {
            Self::AnfSegment => format!("segment-{file_id:020}.anf"),
            Self::ChronicleRecord => format!("segment-{file_id:020}.chr"),
            Self::SchemaSnapshot => format!("schema-{file_id:020}.ans"),
            Self::CatalogSnapshot => format!("catalog-{file_id:020}.anc"),
        }
    }

    pub fn staging_file_name(self, file_id: u64) -> String {
        match self {
            Self::AnfSegment => format!(".segment-{file_id:020}.tmp"),
            Self::ChronicleRecord => format!(".chronicle-{file_id:020}.tmp"),
            Self::SchemaSnapshot => format!(".schema-{file_id:020}.tmp"),
            Self::CatalogSnapshot => format!(".catalog-{file_id:020}.tmp"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalMutationIntent {
    pub artifact_kind: WalArtifactKind,
    pub table_id: u64,
    pub schema_revision: u64,
    pub file_id: u64,
    pub file_len: u64,
    pub sha256: [u8; 32],
}

impl WalMutationIntent {
    pub fn encode(&self) -> Result<Vec<u8>, WalRecoveryError> {
        self.validate()?;
        let mut bytes = Vec::with_capacity(INTENT_LEN);
        bytes.extend_from_slice(INTENT_MAGIC);
        bytes.extend_from_slice(&INTENT_VERSION.to_le_bytes());
        bytes.push(self.artifact_kind as u8);
        bytes.extend_from_slice(&self.table_id.to_le_bytes());
        bytes.extend_from_slice(&self.schema_revision.to_le_bytes());
        bytes.extend_from_slice(&self.file_id.to_le_bytes());
        bytes.extend_from_slice(&self.file_len.to_le_bytes());
        bytes.extend_from_slice(&self.sha256);
        Ok(bytes)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, WalRecoveryError> {
        if bytes.len() != INTENT_LEN || &bytes[..4] != INTENT_MAGIC {
            return Err(WalRecoveryError::CorruptIntent("invalid mutation intent header or length"));
        }
        let version = u16::from_le_bytes([bytes[4], bytes[5]]);
        if version != INTENT_VERSION {
            return Err(WalRecoveryError::UnsupportedIntentVersion(version));
        }
        let artifact_kind = WalArtifactKind::from_byte(bytes[6])?;
        let table_id = u64::from_le_bytes(bytes[7..15].try_into()
            .map_err(|_| WalRecoveryError::CorruptIntent("invalid TableId"))?);
        let schema_revision = u64::from_le_bytes(bytes[15..23].try_into()
            .map_err(|_| WalRecoveryError::CorruptIntent("invalid schema revision"))?);
        let file_id = u64::from_le_bytes(bytes[23..31].try_into()
            .map_err(|_| WalRecoveryError::CorruptIntent("invalid file ID"))?);
        let file_len = u64::from_le_bytes(bytes[31..39].try_into()
            .map_err(|_| WalRecoveryError::CorruptIntent("invalid file length"))?);
        let mut sha256 = [0_u8; 32];
        sha256.copy_from_slice(&bytes[39..71]);
        let intent = Self {
            artifact_kind,
            table_id,
            schema_revision,
            file_id,
            file_len,
            sha256,
        };
        intent.validate()?;
        Ok(intent)
    }

    fn validate(&self) -> Result<(), WalRecoveryError> {
        if self.table_id == 0 || self.schema_revision == 0 || self.file_id == 0 {
            return Err(WalRecoveryError::CorruptIntent(
                "TableId, schema revision and file ID must be non-zero",
            ));
        }
        if self.file_len == 0 {
            return Err(WalRecoveryError::CorruptIntent("immutable artifact cannot be empty"));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoveredWalTransaction {
    pub transaction_id: u64,
    pub intents: Vec<WalMutationIntent>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WalRecoveryPlan {
    /// Transactions with a valid COMMIT frame, in WAL order.
    pub committed: Vec<RecoveredWalTransaction>,
    /// Transactions with BEGIN but no COMMIT/ABORT marker at EOF.
    pub incomplete: Vec<RecoveredWalTransaction>,
}

impl WalRecoveryPlan {
    pub fn committed_intent_count(&self) -> usize {
        self.committed.iter().map(|transaction| transaction.intents.len()).sum()
    }

    pub fn incomplete_intent_count(&self) -> usize {
        self.incomplete.iter().map(|transaction| transaction.intents.len()).sum()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WalRecoveryError {
    CorruptIntent(&'static str),
    UnsupportedIntentVersion(u16),
    TransactionClassification(&'static str),
    Io(String),
    FileConflict(&'static str),
    ChronicleCorrupt(&'static str),
    MissingCommittedArtifact,
    MissingTableId(u64),
    Wal(String),
    SchemaSnapshot(String),
}

impl fmt::Display for WalRecoveryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CorruptIntent(message) => write!(formatter, "corrupt WAL mutation intent: {message}"),
            Self::UnsupportedIntentVersion(version) => {
                write!(formatter, "unsupported WAL mutation intent version {version}")
            }
            Self::TransactionClassification(message) => {
                write!(formatter, "invalid WAL recovery transaction classification: {message}")
            }
            Self::Io(message) => write!(formatter, "WAL recovery I/O error: {message}"),
            Self::FileConflict(message) => write!(formatter, "WAL recovery file conflict: {message}"),
            Self::ChronicleCorrupt(message) => write!(formatter, "corrupt committed Chronicle record: {message}"),
            Self::MissingCommittedArtifact => formatter.write_str(
                "committed WAL artifact has neither a valid staging file nor a valid final file",
            ),
            Self::MissingTableId(table_id) => {
                write!(formatter, "WAL recovery cannot resolve TableId {table_id}")
            }
            Self::Wal(message) => write!(formatter, "WAL recovery log error: {message}"),
            Self::SchemaSnapshot(message) => write!(formatter, "WAL recovery schema snapshot error: {message}"),
        }
    }
}

impl std::error::Error for WalRecoveryError {}

/// Classify a verified ANWL read report into committed redo work and incomplete
/// work whose staging artifacts may be discarded. The low-level WAL reader has
/// already validated frame checksum, sequence and transaction ordering.
pub fn build_recovery_plan(report: &WalReadReport) -> Result<WalRecoveryPlan, WalRecoveryError> {
    let incomplete_ids = report.incomplete_transaction_ids.iter().copied().collect::<BTreeSet<_>>();
    let mut transaction_intents: BTreeMap<u64, Vec<WalMutationIntent>> = BTreeMap::new();
    let mut transaction_order = Vec::new();
    let mut committed_ids = BTreeSet::new();
    let mut aborted_ids = BTreeSet::new();

    for record in &report.records {
        match record.kind {
            WalRecordKind::Begin => {
                transaction_order.push(record.transaction_id);
                transaction_intents.entry(record.transaction_id).or_default();
            }
            WalRecordKind::Mutation => {
                let intent = WalMutationIntent::decode(&record.payload)?;
                transaction_intents.get_mut(&record.transaction_id)
                    .ok_or(WalRecoveryError::TransactionClassification(
                        "MUTATION was not preceded by BEGIN",
                    ))?
                    .push(intent);
            }
            WalRecordKind::Commit => {
                committed_ids.insert(record.transaction_id);
            }
            WalRecordKind::Abort => {
                aborted_ids.insert(record.transaction_id);
            }
            WalRecordKind::Checkpoint => {}
        }
    }

    let mut plan = WalRecoveryPlan::default();
    for transaction_id in transaction_order {
        let intents = transaction_intents.remove(&transaction_id)
            .ok_or(WalRecoveryError::TransactionClassification(
                "BEGIN transaction lost its intent accumulator",
            ))?;
        if committed_ids.contains(&transaction_id) {
            plan.committed.push(RecoveredWalTransaction {
                transaction_id,
                intents,
            });
        } else if incomplete_ids.contains(&transaction_id) {
            plan.incomplete.push(RecoveredWalTransaction {
                transaction_id,
                intents,
            });
        } else if aborted_ids.contains(&transaction_id) {
            // Explicitly aborted transactions have no redo/discard work left
            // in this plan. Their staging files are handled by the abort path.
        } else {
            return Err(WalRecoveryError::TransactionClassification(
                "transaction is neither committed, aborted nor incomplete",
            ));
        }
    }

    if plan.incomplete.iter().any(|transaction| committed_ids.contains(&transaction.transaction_id)) {
        return Err(WalRecoveryError::TransactionClassification(
            "transaction is both committed and incomplete",
        ));
    }
    Ok(plan)
}

/// Deterministic interruption points used only by the recovery test suite.
/// These checkpoints deliberately sit after durable/visible filesystem steps.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WalRecoveryCrashPoint {
    AfterStagingFileCreated,
    AfterStagingBytesWritten,
    AfterStagingFlush,
    AfterStagingFileSync,
    AfterStagingDirectorySync,
    AfterWalBegin,
    AfterWalMutation,
    AfterWalCommit,
    AfterFinalNameLinked,
    AfterPublishDirectorySync,
    AfterStagingNameRemoved,
    AfterCleanupDirectorySync,
    AfterSchemaSnapshotPublished,
    AfterCatalogSnapshotPublished,
    AfterCanonicalCatalogPublished,
}

#[cfg(test)]
std::thread_local! {
    static WAL_RECOVERY_CRASH_POINT: std::cell::Cell<Option<WalRecoveryCrashPoint>> =
        std::cell::Cell::new(None);
}

#[cfg(test)]
pub(crate) fn arm_wal_recovery_crash_point(point: WalRecoveryCrashPoint) {
    WAL_RECOVERY_CRASH_POINT.with(|armed| armed.set(Some(point)));
}

#[inline]
pub(crate) fn maybe_crash_after(point: WalRecoveryCrashPoint) {
    #[cfg(test)]
    WAL_RECOVERY_CRASH_POINT.with(|armed| {
        if armed.get() == Some(point) {
            armed.set(None);
            if std::env::var_os("ANANTA_WAL_CRASH_EXIT").is_some() {
                // Exit without Rust destructors so the parent process can
                // observe a genuinely abandoned database lock and WAL state.
                std::process::exit(86);
            }
            panic!("simulated process interruption at {point:?}");
        }
    });
    #[cfg(not(test))]
    let _ = point;
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct WalRecoverySummary {
    pub committed_artifacts_published_or_verified: u64,
    pub incomplete_artifacts_discarded: u64,
    pub aborted_transactions: u64,
}

/// Apply the redo/cleanup plan. The caller resolves each stable TableId to its
/// real table directory using the loaded catalog and validates the referenced
/// schema snapshot before returning the directory. Incomplete transactions
/// receive durable ABORT markers after their staged files are discarded.
pub fn recover_staged_artifacts<F>(
    report: &WalReadReport,
    writer: &mut WalWriter,
    mut resolve_table_directory: F,
) -> Result<WalRecoverySummary, WalRecoveryError>
where
    F: FnMut(&WalMutationIntent) -> Result<PathBuf, WalRecoveryError>,
{
    let plan = build_recovery_plan(report)?;
    let mut summary = WalRecoverySummary::default();

    for transaction in &plan.committed {
        for intent in &transaction.intents {
            let directory = resolve_table_directory(intent)?;
            publish_staged_wal_artifact(&directory, intent)?;
            summary.committed_artifacts_published_or_verified =
                summary.committed_artifacts_published_or_verified
                    .checked_add(1)
                    .ok_or(WalRecoveryError::CorruptIntent("recovery artifact counter overflow"))?;
        }
    }

    for transaction in &plan.incomplete {
        for intent in &transaction.intents {
            let directory = resolve_table_directory(intent)?;
            discard_staged_wal_artifact(&directory, intent)?;
            summary.incomplete_artifacts_discarded =
                summary.incomplete_artifacts_discarded
                    .checked_add(1)
                    .ok_or(WalRecoveryError::CorruptIntent("recovery artifact counter overflow"))?;
        }
    }

    for transaction in &plan.incomplete {
        writer.abort_transaction(
            transaction.transaction_id,
            b"startup recovery discarded uncommitted staged artifacts",
        ).map_err(|error| WalRecoveryError::Wal(error.to_string()))?;
        summary.aborted_transactions = summary.aborted_transactions
            .checked_add(1)
            .ok_or(WalRecoveryError::CorruptIntent("recovery transaction counter overflow"))?;
    }
    if summary.aborted_transactions > 0 {
        writer.sync().map_err(|error| WalRecoveryError::Wal(error.to_string()))?;
    }
    Ok(summary)
}

/// Stage complete immutable bytes under the intent's canonical temporary
/// filename. The file and directory entry are synchronised before WAL COMMIT.
pub fn stage_wal_artifact(
    directory: impl AsRef<Path>,
    intent: &WalMutationIntent,
    bytes: &[u8],
) -> Result<PathBuf, WalRecoveryError> {
    intent.validate()?;
    let directory = directory.as_ref();
    fs::create_dir_all(directory).map_err(io_error)?;
    require_recovery_directory(directory)?;
    let expected_len = u64::try_from(bytes.len())
        .map_err(|_| WalRecoveryError::CorruptIntent("artifact length exceeds u64"))?;
    if expected_len != intent.file_len || crate::chronicle::sha256(bytes) != intent.sha256 {
        return Err(WalRecoveryError::FileConflict(
            "staged bytes do not match mutation intent length/SHA-256",
        ));
    }

    let staging_path = directory.join(intent.artifact_kind.staging_file_name(intent.file_id));
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&staging_path)
        .map_err(io_error)?;
    maybe_crash_after(WalRecoveryCrashPoint::AfterStagingFileCreated);
    let write_result = (|| {
        file.write_all(bytes)?;
        maybe_crash_after(WalRecoveryCrashPoint::AfterStagingBytesWritten);
        file.flush()?;
        maybe_crash_after(WalRecoveryCrashPoint::AfterStagingFlush);
        file.sync_all()?;
        maybe_crash_after(WalRecoveryCrashPoint::AfterStagingFileSync);
        sync_recovery_directory(directory)?;
        maybe_crash_after(WalRecoveryCrashPoint::AfterStagingDirectorySync);
        Ok::<(), std::io::Error>(())
    })();
    drop(file);
    if let Err(error) = write_result {
        let _ = fs::remove_file(&staging_path);
        return Err(io_error(error));
    }
    Ok(staging_path)
}

/// Idempotently publish a committed intent. Existing final files are accepted
/// only when they exactly match the recorded size and checksum. Publication
/// uses a no-replace hard link so recovery never overwrites conflicting data.
pub fn publish_staged_wal_artifact(
    directory: impl AsRef<Path>,
    intent: &WalMutationIntent,
) -> Result<PathBuf, WalRecoveryError> {
    intent.validate()?;
    let directory = directory.as_ref();
    require_recovery_directory(directory)?;
    let staging_path = directory.join(intent.artifact_kind.staging_file_name(intent.file_id));
    let final_path = directory.join(intent.artifact_kind.final_file_name(intent.file_id));

    if final_path.exists() {
        verify_intent_file(&final_path, intent)?;
        if staging_path.exists() {
            verify_intent_file(&staging_path, intent)?;
            fs::remove_file(&staging_path).map_err(io_error)?;
            maybe_crash_after(WalRecoveryCrashPoint::AfterStagingNameRemoved);
            sync_recovery_directory(directory).map_err(io_error)?;
            maybe_crash_after(WalRecoveryCrashPoint::AfterCleanupDirectorySync);
        }
        return Ok(final_path);
    }

    if !staging_path.exists() {
        return Err(WalRecoveryError::MissingCommittedArtifact);
    }
    verify_intent_file(&staging_path, intent)?;
    match fs::hard_link(&staging_path, &final_path) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            verify_intent_file(&final_path, intent)?;
        }
        Err(error) => return Err(io_error(error)),
    }
    maybe_crash_after(WalRecoveryCrashPoint::AfterFinalNameLinked);
    sync_recovery_directory(directory).map_err(io_error)?;
    maybe_crash_after(WalRecoveryCrashPoint::AfterPublishDirectorySync);
    verify_intent_file(&final_path, intent)?;
    if staging_path.exists() {
        fs::remove_file(&staging_path).map_err(io_error)?;
        maybe_crash_after(WalRecoveryCrashPoint::AfterStagingNameRemoved);
        sync_recovery_directory(directory).map_err(io_error)?;
        maybe_crash_after(WalRecoveryCrashPoint::AfterCleanupDirectorySync);
    }
    Ok(final_path)
}

/// Discard an incomplete transaction's staged artifact. A final artifact is
/// never removed by this operation.
pub fn discard_staged_wal_artifact(
    directory: impl AsRef<Path>,
    intent: &WalMutationIntent,
) -> Result<(), WalRecoveryError> {
    intent.validate()?;
    let directory = directory.as_ref();
    require_recovery_directory(directory)?;
    let staging_path = directory.join(intent.artifact_kind.staging_file_name(intent.file_id));
    match fs::symlink_metadata(&staging_path) {
        Ok(metadata) => {
            if !metadata.file_type().is_file() {
                return Err(WalRecoveryError::FileConflict(
                    "staging path is not a regular file",
                ));
            }
            fs::remove_file(&staging_path).map_err(io_error)?;
            sync_recovery_directory(directory).map_err(io_error)?;
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(io_error(error)),
    }
    Ok(())
}

fn verify_intent_file(
    path: &Path,
    intent: &WalMutationIntent,
) -> Result<(), WalRecoveryError> {
    let metadata = fs::symlink_metadata(path).map_err(io_error)?;
    let chronicle = intent.artifact_kind == WalArtifactKind::ChronicleRecord;
    if !metadata.file_type().is_file() {
        return Err(if chronicle {
            WalRecoveryError::ChronicleCorrupt("record path is not a regular file")
        } else {
            WalRecoveryError::FileConflict("artifact path is not a regular file")
        });
    }
    if metadata.len() != intent.file_len {
        return Err(if chronicle {
            WalRecoveryError::ChronicleCorrupt("record length mismatch")
        } else {
            WalRecoveryError::FileConflict("artifact length differs from committed WAL intent")
        });
    }
    let checksum = crate::chronicle::sha256_file(path).map_err(io_error)?;
    if checksum != intent.sha256 {
        return Err(if chronicle {
            WalRecoveryError::ChronicleCorrupt("record checksum mismatch")
        } else {
            WalRecoveryError::FileConflict("artifact SHA-256 differs from committed WAL intent")
        });
    }
    verify_artifact_identity(path, intent)?;
    Ok(())
}

fn verify_artifact_identity(
    path: &Path,
    intent: &WalMutationIntent,
) -> Result<(), WalRecoveryError> {
    match intent.artifact_kind {
        WalArtifactKind::AnfSegment => {
            let mut file = File::open(path).map_err(io_error)?;
            let mut header = [0_u8; 36];
            file.read_exact(&mut header)
                .map_err(|_| WalRecoveryError::FileConflict("ANF segment header is truncated"))?;
            let version = ananta_format::segment_version(&header)
                .map_err(|_| WalRecoveryError::FileConflict("ANF segment header is invalid"))?;
            if version != 2 || header[6] != 0 || header[7] != 0 {
                return Err(WalRecoveryError::FileConflict(
                    "WAL ANF intent requires an ANF v2 header with no mandatory feature flags",
                ));
            }
            let table_id = u64::from_le_bytes(header[20..28].try_into()
                .map_err(|_| WalRecoveryError::FileConflict("ANF table identity is truncated"))?);
            let schema_revision = u64::from_le_bytes(header[28..36].try_into()
                .map_err(|_| WalRecoveryError::FileConflict("ANF schema revision is truncated"))?);
            if table_id != intent.table_id {
                return Err(WalRecoveryError::FileConflict(
                    "ANF TableId differs from committed WAL intent",
                ));
            }
            if schema_revision != intent.schema_revision {
                return Err(WalRecoveryError::FileConflict(
                    "ANF schema revision differs from committed WAL intent",
                ));
            }
        }
        WalArtifactKind::ChronicleRecord => {
            let (metadata, _) = crate::chronicle::inspect_file(path, intent.table_id)
                .map_err(|error| match error {
                    crate::DbError::ChronicleCorrupt(message) => {
                        WalRecoveryError::ChronicleCorrupt(message)
                    }
                    crate::DbError::Io(error) => WalRecoveryError::Io(error.to_string()),
                    _ => WalRecoveryError::ChronicleCorrupt("invalid Chronicle record metadata"),
                })?;
            if metadata.schema_revision != intent.schema_revision {
                return Err(WalRecoveryError::ChronicleCorrupt(
                    "record schema revision differs from committed WAL intent",
                ));
            }
        }
        WalArtifactKind::SchemaSnapshot => {
            let bytes = fs::read(path).map_err(io_error)?;
            let snapshot = crate::schema_snapshot::SchemaSnapshot::decode(&bytes)
                .map_err(|_| WalRecoveryError::FileConflict("schema snapshot is invalid"))?;
            if snapshot.table_id().0 != intent.table_id
                || snapshot.revision() != intent.schema_revision
                || intent.file_id != intent.schema_revision
            {
                return Err(WalRecoveryError::FileConflict(
                    "schema snapshot identity differs from committed WAL intent",
                ));
            }
        }
        WalArtifactKind::CatalogSnapshot => {
            let mut bytes = Vec::new();
            File::open(path).map_err(io_error)?.read_to_end(&mut bytes).map_err(io_error)?;
            let catalog = crate::decode_catalog_v2(&bytes)
                .map_err(|_| WalRecoveryError::FileConflict("catalog snapshot is invalid"))?;
            let table_matches = catalog.tables.values().any(|table| {
                table.id.0 == intent.table_id && table.schema_revision == intent.schema_revision
            });
            if catalog.revision != intent.file_id || !table_matches {
                return Err(WalRecoveryError::FileConflict(
                    "catalog snapshot identity differs from committed WAL intent",
                ));
            }
        }
    }
    Ok(())
}

fn require_recovery_directory(path: &Path) -> Result<(), WalRecoveryError> {
    let metadata = fs::symlink_metadata(path).map_err(io_error)?;
    if !metadata.file_type().is_dir() {
        return Err(WalRecoveryError::FileConflict(
            "table directory is not a real directory",
        ));
    }
    Ok(())
}

fn sync_recovery_directory(path: &Path) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        File::open(path)?.sync_all()
    }
    #[cfg(not(unix))]
    {
        let _ = path;
        Ok(())
    }
}

fn io_error(error: std::io::Error) -> WalRecoveryError {
    WalRecoveryError::Io(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::{read_wal, WalSyncPolicy, WalWriter};
    use ananta_types::{Column, DataType, Schema, Value};
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn path(label: &str) -> PathBuf {
        let nonce = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
        std::env::temp_dir().join(format!("ananta-wal-plan-{label}-{nonce}.wal"))
    }

    fn valid_anf_bytes(table_id: u64, schema_revision: u64) -> Vec<u8> {
        let schema = Schema::new(vec![
            Column::new("value", DataType::Int64, false),
        ]).unwrap();
        ananta_format::encode_segment_v2(
            &schema,
            &[vec![Value::Int64(42)]],
            table_id,
            schema_revision,
            &[table_id + 100],
        ).unwrap()
    }

    fn intent(file_id: u64) -> WalMutationIntent {
        WalMutationIntent {
            artifact_kind: WalArtifactKind::AnfSegment,
            table_id: 4,
            schema_revision: 2,
            file_id,
            file_len: 128,
            sha256: [file_id as u8; 32],
        }
    }

    #[test]
    fn mutation_intent_round_trips_and_derives_only_canonical_file_names() {
        let original = intent(91);
        let bytes = original.encode().unwrap();
        assert_eq!(bytes.len(), INTENT_LEN);
        assert_eq!(WalMutationIntent::decode(&bytes).unwrap(), original);
        assert_eq!(original.artifact_kind.final_file_name(91), "segment-00000000000000000091.anf");
        assert_eq!(original.artifact_kind.staging_file_name(91), ".segment-00000000000000000091.tmp");

        let mut corrupt = bytes;
        corrupt[6] = 0xff;
        assert_eq!(
            WalMutationIntent::decode(&corrupt),
            Err(WalRecoveryError::CorruptIntent("unknown artifact kind")),
        );

        let mut zero_id = original;
        zero_id.file_id = 0;
        assert_eq!(
            zero_id.encode(),
            Err(WalRecoveryError::CorruptIntent("TableId, schema revision and file ID must be non-zero")),
        );
    }

    #[test]
    fn recovery_plan_separates_committed_and_incomplete_intents() {
        let path = path("classify");
        let mut writer = WalWriter::open(&path, WalSyncPolicy::Manual).unwrap();

        let committed = writer.next_sequence();
        writer.begin_transaction(committed, b"insert").unwrap();
        writer.append_mutation(committed, &intent(11).encode().unwrap()).unwrap();
        writer.commit_transaction(committed).unwrap();

        let incomplete = writer.next_sequence();
        writer.begin_transaction(incomplete, b"ingest").unwrap();
        writer.append_mutation(
            incomplete,
            &WalMutationIntent {
                artifact_kind: WalArtifactKind::ChronicleRecord,
                file_id: 12,
                ..intent(12)
            }.encode().unwrap(),
        ).unwrap();

        let aborted = writer.next_sequence();
        writer.begin_transaction(aborted, b"aborted").unwrap();
        writer.append_mutation(aborted, &intent(13).encode().unwrap()).unwrap();
        writer.abort_transaction(aborted, b"not committed").unwrap();
        drop(writer);

        let report = read_wal(&path).unwrap();
        let plan = build_recovery_plan(&report).unwrap();
        assert_eq!(plan.committed.len(), 1);
        assert_eq!(plan.committed[0].transaction_id, committed);
        assert_eq!(plan.committed_intent_count(), 1);
        assert_eq!(plan.incomplete.len(), 1);
        assert_eq!(plan.incomplete[0].transaction_id, incomplete);
        assert_eq!(plan.incomplete_intent_count(), 1);
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn staged_artifact_publication_is_idempotent_and_checksum_guarded() {
        let root = std::env::temp_dir().join(format!(
            "ananta-wal-publish-{}",
            SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos(),
        ));
        fs::create_dir_all(&root).unwrap();
        let bytes = valid_anf_bytes(4, 2);
        let intent = WalMutationIntent {
            artifact_kind: WalArtifactKind::AnfSegment,
            table_id: 4,
            schema_revision: 2,
            file_id: 91,
            file_len: bytes.len() as u64,
            sha256: crate::chronicle::sha256(&bytes),
        };
        let staged = stage_wal_artifact(&root, &intent, &bytes).unwrap();
        assert!(staged.is_file());
        let final_path = publish_staged_wal_artifact(&root, &intent).unwrap();
        assert_eq!(fs::read(&final_path).unwrap(), bytes);
        assert!(!staged.exists());
        // Replaying a committed intent after final publication is a no-op.
        assert_eq!(publish_staged_wal_artifact(&root, &intent).unwrap(), final_path);

        let conflicting = b"different immutable payload";
        fs::write(&final_path, conflicting).unwrap();
        assert!(matches!(
            publish_staged_wal_artifact(&root, &intent),
            Err(WalRecoveryError::FileConflict("artifact length differs from committed WAL intent"))
        ));
        assert_eq!(fs::read(&final_path).unwrap(), conflicting);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn recovery_replays_committed_and_aborts_incomplete_staged_artifacts_idempotently() {
        let wal_path = path("replay");
        let table_dir = wal_path.with_extension("table");
        fs::create_dir_all(&table_dir).unwrap();
        let committed_bytes = valid_anf_bytes(71, 1);
        let committed_intent = WalMutationIntent {
            artifact_kind: WalArtifactKind::AnfSegment,
            table_id: 71,
            schema_revision: 1,
            file_id: 101,
            file_len: committed_bytes.len() as u64,
            sha256: crate::chronicle::sha256(&committed_bytes),
        };
        let incomplete_bytes = b"incomplete payload";
        let incomplete_intent = WalMutationIntent {
            artifact_kind: WalArtifactKind::ChronicleRecord,
            table_id: 71,
            schema_revision: 1,
            file_id: 102,
            file_len: incomplete_bytes.len() as u64,
            sha256: crate::chronicle::sha256(incomplete_bytes),
        };
        stage_wal_artifact(&table_dir, &committed_intent, committed_bytes).unwrap();
        stage_wal_artifact(&table_dir, &incomplete_intent, incomplete_bytes).unwrap();

        let mut writer = WalWriter::open(&wal_path, WalSyncPolicy::OnCommit).unwrap();
        let committed_id = writer.next_sequence();
        writer.begin_transaction(committed_id, b"ANF insert").unwrap();
        writer.append_mutation(committed_id, &committed_intent.encode().unwrap()).unwrap();
        writer.commit_transaction(committed_id).unwrap();

        let incomplete_id = writer.next_sequence();
        writer.begin_transaction(incomplete_id, b"Chronicle ingest").unwrap();
        writer.append_mutation(incomplete_id, &incomplete_intent.encode().unwrap()).unwrap();
        drop(writer);

        let mut writer = WalWriter::open(&wal_path, WalSyncPolicy::OnCommit).unwrap();
        let report = read_wal(&wal_path).unwrap();
        let resolve = |intent: &WalMutationIntent| {
            if intent.table_id == 71 {
                Ok(table_dir.clone())
            } else {
                Err(WalRecoveryError::MissingTableId(intent.table_id))
            }
        };
        let summary = recover_staged_artifacts(&report, &mut writer, resolve).unwrap();
        assert_eq!(summary.committed_artifacts_published_or_verified, 1);
        assert_eq!(summary.incomplete_artifacts_discarded, 1);
        assert_eq!(summary.aborted_transactions, 1);
        assert!(table_dir.join(committed_intent.artifact_kind.final_file_name(101)).is_file());
        assert!(!table_dir.join(incomplete_intent.artifact_kind.staging_file_name(102)).exists());

        let second_report = read_wal(&wal_path).unwrap();
        assert!(second_report.is_clean());
        let second = recover_staged_artifacts(&second_report, &mut writer, |intent| {
            if intent.table_id == 71 { Ok(table_dir.clone()) }
            else { Err(WalRecoveryError::MissingTableId(intent.table_id)) }
        }).unwrap();
        assert_eq!(second.committed_artifacts_published_or_verified, 1);
        assert_eq!(second.aborted_transactions, 0);

        drop(writer);
        fs::remove_file(wal_path).unwrap();
        fs::remove_dir_all(table_dir).unwrap();
    }

    #[test]
    fn recovery_fails_closed_when_a_committed_artifact_is_missing() {
        let wal_path = path("missing-artifact");
        let table_dir = wal_path.with_extension("table");
        fs::create_dir_all(&table_dir).unwrap();
        let intent = WalMutationIntent {
            artifact_kind: WalArtifactKind::AnfSegment,
            table_id: 81,
            schema_revision: 1,
            file_id: 111,
            file_len: 3,
            sha256: crate::chronicle::sha256(b"abc"),
        };
        let mut writer = WalWriter::open(&wal_path, WalSyncPolicy::OnCommit).unwrap();
        let transaction = writer.next_sequence();
        writer.begin_transaction(transaction, b"insert").unwrap();
        writer.append_mutation(transaction, &intent.encode().unwrap()).unwrap();
        writer.commit_transaction(transaction).unwrap();
        drop(writer);

        let mut writer = WalWriter::open(&wal_path, WalSyncPolicy::OnCommit).unwrap();
        let report = read_wal(&wal_path).unwrap();
        assert_eq!(
            recover_staged_artifacts(&report, &mut writer, |_| Ok(table_dir.clone())),
            Err(WalRecoveryError::MissingCommittedArtifact),
        );
        drop(writer);
        fs::remove_file(wal_path).unwrap();
        fs::remove_dir_all(table_dir).unwrap();
    }

    #[test]
    fn publication_rejects_an_anf_schema_revision_that_disagrees_with_the_intent() {
        let root = std::env::temp_dir().join(format!(
            "ananta-wal-schema-mismatch-{}",
            SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos(),
        ));
        fs::create_dir_all(&root).unwrap();
        let bytes = valid_anf_bytes(91, 1);
        let intent = WalMutationIntent {
            artifact_kind: WalArtifactKind::AnfSegment,
            table_id: 91,
            schema_revision: 2,
            file_id: 92,
            file_len: bytes.len() as u64,
            sha256: crate::chronicle::sha256(&bytes),
        };
        stage_wal_artifact(&root, &intent, &bytes).unwrap();
        assert_eq!(
            publish_staged_wal_artifact(&root, &intent),
            Err(WalRecoveryError::FileConflict(
                "ANF schema revision differs from committed WAL intent"
            )),
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn recovery_plan_rejects_unknown_mutation_intent_versions() {
        let path = path("bad-version");
        let mut writer = WalWriter::open(&path, WalSyncPolicy::Manual).unwrap();
        let transaction = writer.next_sequence();
        writer.begin_transaction(transaction, b"future").unwrap();
        let mut bytes = intent(21).encode().unwrap();
        bytes[4..6].copy_from_slice(&99_u16.to_le_bytes());
        writer.append_mutation(transaction, &bytes).unwrap();
        writer.commit_transaction(transaction).unwrap();
        drop(writer);

        let report = read_wal(&path).unwrap();
        assert_eq!(
            build_recovery_plan(&report),
            Err(WalRecoveryError::UnsupportedIntentVersion(99)),
        );
        fs::remove_file(path).unwrap();
    }
}
