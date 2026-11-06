use ananta_format::{decode_projected_segment, decode_segment_v2, encode_segment_v2, inspect_segment_v2, inspect_segment_v2_details, segment_version, FormatError, SegmentBatchReader, MAX_BATCH_CELLS, MAX_BATCH_COLUMNS, MAX_BATCH_ROWS, MAX_SEGMENT_BYTES};
use ananta_types::{Column, DataType, Schema, TypeError, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{Cursor, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

pub mod schema_snapshot;
pub mod backup;
pub use backup::BackupReport;
pub mod wal_checkpoint;
pub use wal_checkpoint::CheckpointReport;
mod nullable_ddl;
mod column_rename_reorder;
mod segment_publication;
mod chronicle;
mod capsule;
mod navigation_map;
mod compaction_execution;
mod active_generation;
pub mod wal;
pub mod wal_recovery;
pub use wal::{read_wal, WalError, WalReadReport, WalRecord, WalRecordKind, WalSyncPolicy, WalWriter};
pub use wal_recovery::{
    build_recovery_plan, discard_staged_wal_artifact, publish_staged_wal_artifact,
    recover_staged_artifacts, stage_wal_artifact, RecoveredWalTransaction, WalArtifactKind,
    WalMutationIntent, WalRecoveryError, WalRecoveryPlan, WalRecoverySummary,
};
pub use active_generation::{ActiveWeaveGeneration, ActiveWeavePointer};
pub use compaction_execution::{CompactionExecutionReport, CompactionReadMeasurement, RowOrigin};
pub mod compaction;
pub mod compaction_manifest;
pub use compaction_manifest::{CapsuleSourceReference, ChronicleReceiptReference, CompactionInputManifest, ManifestError};
pub mod compaction_output_manifest;
pub use compaction_output_manifest::{CompactionOutputManifest, WeaveArtifactReference, WeaveLayoutProfile};
pub use compaction::{CompactionError, CompactionJobEvent, CompactionJobId, CompactionJobJournal, CompactionJobState, CompactionProgress, CompactionResourceLimits};
pub use chronicle::{BatchId, IngestOutcome, IngestResult, IngestionStats};
pub use capsule::{CapsuleColumnStats, CapsuleMetadata};
pub use navigation_map::{CapsuleScanDiagnostics, CapsuleScanRequest, CapsuleScanResult, ColumnRangePredicate};
use schema_snapshot::{SchemaSnapshot, SchemaSnapshotConsistencyReport};
use segment_publication::{cleanup_orphan_segment_temps, publish_segment, publish_chronicle, DatabaseLock};
use chronicle::{encode_record, extract_payload, inspect_file as inspect_chronicle_file, inspect_reader as inspect_chronicle_reader, scan_table_index, BatchReceipt, PayloadReader, TableChronicleIndex};
use capsule::{decode_capsule, derive_column_stats, derive_temporal_bounds, encode_capsule};
use navigation_map::{file_sha256, map_filename, range_may_match, value_matches, NavigationMap, NavigationMapEntry};

const CATALOG_MAGIC: &[u8; 4] = b"ANC1";
const CATALOG_END: &[u8; 4] = b"CEND";
const CATALOG_V1_VERSION: u16 = 1;
const CATALOG_VERSION: u16 = 2;
const CATALOG_V2_HEADER_LEN: usize = 28;
const CATALOG_TRAILER_LEN: usize = 12;
const MAX_CATALOG_ITEMS: usize = 1_000_000;
const MAX_NAME_BYTES: usize = 1024 * 1024;
const MAX_CATALOG_BYTES: u64 = 64 * 1024 * 1024;

/// A database-local stable identity for a table or column.
/// The value zero is reserved and never valid.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ObjectId(pub u64);

impl fmt::Display for ObjectId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug)]
pub enum DbError {
    Io(std::io::Error),
    Format(FormatError),
    Type(TypeError),
    InvalidName(String),
    TableExists(String),
    TableNotFound(String),
    ColumnNotFound(String),
    ColumnExists(String),
    InvalidColumnOrder(String),
    CorruptCatalog(&'static str),
    SchemaCompatibility(&'static str),
    UnsupportedCatalogVersion(u16),
    ChecksumMismatch,
    LengthOverflow,
    DatabaseLocked(PathBuf),
    SegmentPublicationUncertain { path: PathBuf, reason: String },
    InvalidBatchId,
    EmptyBatch,
    BatchIdConflict(BatchId),
    ChronicleCorrupt(&'static str),
    IngestRowValidation { row_index: usize, message: String },
    CapsuleCorrupt(&'static str),
    NavigationMapCorrupt(&'static str),
    NavigationMapStale(&'static str),
    InvalidRange(String),
    CompactionExecution(String),
    CompactionManifest(crate::compaction_manifest::ManifestError),
    CompactionPreflight(&'static str),
    Wal(String),
    RecoveryRequired(String),
    Backup(String),
}

impl fmt::Display for DbError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(e) => write!(f, "I/O error: {e}"),
            Self::Format(e) => write!(f, "format error: {e}"),
            Self::Type(e) => write!(f, "type error: {e}"),
            Self::InvalidName(s) => write!(f, "invalid database object name: {s}"),
            Self::TableExists(s) => write!(f, "table already exists: {s}"),
            Self::TableNotFound(s) => write!(f, "table not found: {s}"),
            Self::ColumnNotFound(s) => write!(f, "column not found: {s}"),
            Self::ColumnExists(s) => write!(f, "column already exists: {s}"),
            Self::InvalidColumnOrder(s) => write!(f, "invalid column order: {s}"),
            Self::CorruptCatalog(s) => write!(f, "corrupt ANANTA catalog: {s}"),
            Self::SchemaCompatibility(s) => write!(f, "ANANTA schema compatibility error: {s}"),
            Self::UnsupportedCatalogVersion(v) => write!(f, "unsupported catalog version: {v}"),
            Self::ChecksumMismatch => f.write_str("catalog checksum mismatch"),
            Self::LengthOverflow => f.write_str("catalog field exceeds supported limits"),
            Self::DatabaseLocked(path) => write!(f, "database is locked by another writer: {} (verify no process owns it before removing a stale lock)", path.display()),
            Self::SegmentPublicationUncertain { path, reason } => write!(f, "segment publication durability is uncertain for {}: {reason}; inspect the final path before retrying", path.display()),
            Self::InvalidBatchId => f.write_str("BatchId must be 16 nonzero bytes"),
            Self::EmptyBatch => f.write_str("an ingestion batch must contain at least one row"),
            Self::BatchIdConflict(id) => write!(f, "BatchId {id} was already committed with a different payload"),
            Self::ChronicleCorrupt(message) => write!(f, "corrupt Chronicle record: {message}"),
            Self::IngestRowValidation { row_index, message } => write!(f, "ingestion row {row_index} failed validation: {message}"),
            Self::CapsuleCorrupt(message) => write!(f, "corrupt Capsule: {message}"),
            Self::NavigationMapCorrupt(message) => write!(f, "corrupt Navigation Map: {message}"),
            Self::NavigationMapStale(message) => write!(f, "stale Navigation Map: {message}"),
            Self::InvalidRange(message) => write!(f, "invalid Capsule scan range: {message}"),
            Self::CompactionExecution(message) => write!(f, "compaction execution failed: {message}"),
            Self::CompactionManifest(error) => write!(f, "invalid compaction manifest: {error}"),
            Self::CompactionPreflight(message) => write!(f, "compaction input preflight failed: {message}"),
            Self::Wal(message) => write!(f, "ANANTA WAL error: {message}"),
            Self::RecoveryRequired(message) => write!(f, "database handle requires recovery before use: {message}"),
            Self::Backup(message) => write!(f, "ANANTA backup/restore error: {message}"),
        }
    }
}
impl std::error::Error for DbError {}
impl From<crate::compaction_manifest::ManifestError> for DbError {
    fn from(value: crate::compaction_manifest::ManifestError) -> Self { Self::CompactionManifest(value) }
}
impl From<std::io::Error> for DbError {
    fn from(value: std::io::Error) -> Self { Self::Io(value) }
}
impl From<FormatError> for DbError {
    fn from(value: FormatError) -> Self { Self::Format(value) }
}
impl From<TypeError> for DbError {
    fn from(value: TypeError) -> Self { Self::Type(value) }
}

#[derive(Debug, Clone)]
struct TableDefinition {
    id: ObjectId,
    schema_revision: u64,
    column_ids: Vec<ObjectId>,
    schema: Schema,
}


/// One typed output column in a RecordBatch. Duplicate names are allowed here because
/// a projection may explicitly request the same logical column more than once.
#[derive(Debug, Clone, PartialEq)]
pub struct BatchColumn {
    pub column: Column,
    pub values: Vec<Value>,
}

/// An owned, columnar batch. Field descriptors preserve caller order and may
/// repeat a logical column when the projection requests it more than once.
#[derive(Debug, Clone, PartialEq)]
pub struct RecordBatch {
    pub columns: Vec<BatchColumn>,
    pub row_count: usize,
}

impl RecordBatch {
    pub fn row_count(&self) -> usize { self.row_count }
    pub fn column(&self, index: usize) -> Option<&[Value]> {
        self.columns.get(index).map(|column| column.values.as_slice())
    }
    pub fn column_by_name(&self, name: &str) -> Option<&[Value]> {
        self.columns.iter().find(|column| column.column.name == name)
            .map(|column| column.values.as_slice())
    }
}

struct ActiveScanSegment {
    decoder: SegmentBatchReader<PayloadReader<File>>,
    output_slots: Vec<Option<usize>>,
}

/// Stateful scan iterator. It opens one segment at a time and never materialises
/// the complete table result.
pub struct ScanBatchIter {
    root: PathBuf,
    current: TableDefinition,
    projection: Vec<usize>,
    segments: Vec<PathBuf>,
    next_segment: usize,
    batch_rows: usize,
    remaining_limit: Option<usize>,
    active: Option<ActiveScanSegment>,
    terminal: bool,
}

impl ScanBatchIter {
    fn open_next_segment(&mut self) -> Result<bool, DbError> {
        if self.next_segment >= self.segments.len() {
            return Ok(false);
        }
        let path = self.segments[self.next_segment].clone();
        self.next_segment += 1;
        let mut file = File::open(&path)?;
        let file_len = file.metadata()?.len();
        file.seek(SeekFrom::Start(0))?;
        let mut magic = [0_u8; 4];
        file.read_exact(&mut magic)?;

        let (snapshot_revision, expected_v2, payload_reader) = if &magic == b"CHR1" {
            let (metadata, payload_offset) = chronicle::inspect_reader(&mut file, self.current.id.0)?;
            if metadata.schema_revision > self.current.schema_revision {
                return Err(DbError::SchemaCompatibility("Chronicle schema revision is newer than the current catalog schema"));
            }
            let reader = PayloadReader::new(file, payload_offset, metadata.payload_len);
            (
                metadata.schema_revision,
                Some((metadata.table_id, metadata.schema_revision)),
                reader,
            )
        } else if &magic == b"ANF1" {
            file.seek(SeekFrom::Start(0))?;
            let mut version_bytes = [0_u8; 6];
            file.read_exact(&mut version_bytes)?;
            let version = u16::from_le_bytes([version_bytes[4], version_bytes[5]]);
            let (revision, identity) = match version {
                1 => (1, None),
                2 => {
                    let mut header = [0_u8; 36];
                    file.seek(SeekFrom::Start(0))?;
                    file.read_exact(&mut header)?;
                    let table_id = u64::from_le_bytes(header[20..28].try_into().map_err(|_| DbError::LengthOverflow)?);
                    let revision = u64::from_le_bytes(header[28..36].try_into().map_err(|_| DbError::LengthOverflow)?);
                    if table_id != self.current.id.0 || table_id == 0 || revision == 0 {
                        return Err(DbError::SchemaCompatibility("segment table identity or schema revision is invalid"));
                    }
                    if revision > self.current.schema_revision {
                        return Err(DbError::SchemaCompatibility("segment schema revision is newer than the current catalog schema"));
                    }
                    (revision, Some((table_id, revision)))
                }
                other => return Err(DbError::Format(FormatError::UnsupportedVersion(other))),
            };
            (revision, identity, PayloadReader::new(file, 0, file_len))
        } else {
            return Err(DbError::Format(FormatError::InvalidMagic));
        };

        let snapshot = SchemaSnapshot::read_from(&self.root, self.current.id, snapshot_revision)
            .map_err(|_| DbError::SchemaCompatibility("segment schema snapshot is missing or invalid"))?;
        if snapshot.table_id() != self.current.id {
            return Err(DbError::SchemaCompatibility("schema snapshot table ID does not match current table"));
        }
        let physical_schema = snapshot.schema()?;
        let historical_ids = snapshot.column_ids().collect::<Vec<_>>();
        let mut physical_projection = Vec::new();
        let mut output_slots = Vec::with_capacity(self.projection.len());
        for current_index in &self.projection {
            let current_column = &self.current.schema.columns()[*current_index];
            let current_id = self.current.column_ids[*current_index];
            match historical_ids.iter().position(|id| *id == current_id) {
                Some(physical_index) => {
                    if physical_schema.columns()[physical_index].data_type != current_column.data_type {
                        return Err(DbError::SchemaCompatibility("column data type changed across schema revisions"));
                    }
                    output_slots.push(Some(physical_projection.len()));
                    physical_projection.push(physical_index);
                }
                None if current_column.nullable => output_slots.push(None),
                None => return Err(DbError::SchemaCompatibility("a non-nullable current column is absent from an older segment")),
            }
        }

        let expected_identity = match expected_v2 {
            Some((table_id, revision)) => {
                Some((table_id, revision, historical_ids.iter().map(|id| id.0).collect::<Vec<_>>()))
            }
            None => None,
        };
        let decoder = SegmentBatchReader::new(
            payload_reader,
            &physical_schema,
            &physical_projection,
            expected_identity,
        )?;
        self.active = Some(ActiveScanSegment { decoder, output_slots });
        Ok(true)
    }
}

impl Iterator for ScanBatchIter {
    type Item = Result<RecordBatch, DbError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.terminal || self.remaining_limit == Some(0) {
            return None;
        }
        loop {
            if self.active.is_none() {
                match self.open_next_segment() {
                    Ok(true) => {}
                    Ok(false) => return None,
                    Err(error) => {
                        self.terminal = true;
                        return Some(Err(error));
                    }
                }
            }

            let active = self.active.as_mut().expect("active segment was opened");
            let requested = self.remaining_limit.map_or(self.batch_rows, |limit| self.batch_rows.min(limit));
            let take = requested.min(active.decoder.remaining_rows());
            if take == 0 {
                self.active = None;
                continue;
            }
            let decoded = match active.decoder.next_batch(take) {
                Ok(Some(columns)) => columns,
                Ok(None) => {
                    self.active = None;
                    continue;
                }
                Err(error) => {
                    self.terminal = true;
                    self.active = None;
                    return Some(Err(DbError::Format(error)));
                }
            };
            let mut output_columns = Vec::with_capacity(active.output_slots.len());
            for (output_index, slot) in active.output_slots.iter().enumerate() {
                let values = match slot {
                    Some(index) => decoded[*index].clone(),
                    None => vec![Value::Null; take],
                };
                output_columns.push(BatchColumn {
                    column: self.current.schema.columns()[self.projection[output_index]].clone(),
                    values,
                });
            }
            if let Some(limit) = &mut self.remaining_limit {
                *limit -= take;
            }
            return Some(Ok(RecordBatch {
                columns: output_columns,
                row_count: take,
            }));
        }
    }
}

#[derive(Debug, Clone)]
struct Catalog {
    revision: u64,
    next_object_id: u64,
    tables: BTreeMap<String, TableDefinition>,
}

impl Catalog {
    fn empty() -> Self {
        Self { revision: 0, next_object_id: 1, tables: BTreeMap::new() }
    }
}

fn allocate_object_id(catalog: &mut Catalog) -> Result<ObjectId, DbError> {
    let value = catalog.next_object_id;
    if value == 0 {
        return Err(DbError::CorruptCatalog("next object ID is zero"));
    }
    catalog.next_object_id = value.checked_add(1).ok_or(DbError::LengthOverflow)?;
    Ok(ObjectId(value))
}

fn increment_catalog_revision(catalog: &mut Catalog) -> Result<(), DbError> {
    catalog.revision = catalog.revision.checked_add(1).ok_or(DbError::LengthOverflow)?;
    Ok(())
}


/// Result of independently validating the files pinned by a CIM1 input manifest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactionPreflightReport {
    pub table_id: u64,
    pub capsules_verified: u64,
    pub chronicle_records_verified: u64,
    pub total_source_bytes: u64,
    pub total_row_count: u64,
    pub input_manifest_sha256: [u8; 32],
}

/// One file in the database's on-disk inventory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageFileInfo {
    pub relative_path: String,
    pub byte_length: u64,
    pub artifact_kind: String,
}

/// On-disk inventory. The active lock file is intentionally excluded.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StorageInventory {
    pub files: Vec<StorageFileInfo>,
    pub total_bytes: u64,
    pub temporary_files: Vec<String>,
}

/// Results from a full bounded-read consistency pass.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ConsistencyReport {
    pub tables_checked: u64,
    pub rows_checked: u64,
    pub chronicle_records_checked: u64,
    pub capsules_checked: u64,
    pub navigation_maps_checked: u64,
    pub wal_records_checked: u64,
    pub inventory: StorageInventory,
}

/// Read-only inspection of a database's exclusive writer lock file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatabaseLockStatus {
    pub lock_path: PathBuf,
    pub is_locked: bool,
    pub owner_process_id: Option<u32>,
}

/// An embedded, single-writer ANANTA database handle. No external database is required.
pub struct Database {
    root: PathBuf,
    catalog: Catalog,
    chronicle_indices: BTreeMap<ObjectId, TableChronicleIndex>,
    wal: WalWriter,
    recovery_required: bool,
    _writer_lock: DatabaseLock,
}

impl Database {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, DbError> {
        let root = path.as_ref().to_path_buf();
        fs::create_dir_all(root.join("catalog"))?;
        fs::create_dir_all(root.join("data"))?;
        // A handle owns the exclusive local writer lock for its entire lifetime.
        let writer_lock = DatabaseLock::acquire(&root)?;
        cleanup_stale_catalog_replacement_temp(&root)?;
        cleanup_legacy_schema_snapshot_temps(&root)?;
        let catalog_path = root.join("catalog").join("catalog.anc");
        let mut catalog = match fs::metadata(&catalog_path) {
            Ok(_) => load_catalog(&catalog_path)?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Catalog::empty(),
            Err(error) => return Err(DbError::Io(error)),
        };
        ensure_initial_schema_snapshots(&root, &catalog)?;

        // Create and durably publish the WAL directory entry before the
        // first transaction can ever commit. The WAL file header itself is
        // synchronised by WalWriter::open; syncing wal_dir persists that name.
        let wal_dir = root.join("wal");
        fs::create_dir_all(&wal_dir)?;
        cleanup_stale_checkpoint_temp(&wal_dir)?;
        sync_recovery_directory(&root)?;
        sync_recovery_directory(&wal_dir)?;
        let wal_path = wal_dir.join("database.anwl");
        let mut wal = WalWriter::open(&wal_path, WalSyncPolicy::OnCommit)
            .map_err(|error| DbError::Wal(error.to_string()))?;
        sync_recovery_directory(&wal_dir)?;
        let wal_report = read_wal(&wal_path)
            .map_err(|error| DbError::Wal(error.to_string()))?;
        validate_checkpoint_records(&root, &wal_report, &catalog)?;
        let committed_catalog_revisions = build_recovery_plan(&wal_report)
            .map_err(|error| DbError::Wal(error.to_string()))?
            .committed
            .into_iter()
            .flat_map(|transaction| transaction.intents)
            .filter(|intent| intent.artifact_kind == WalArtifactKind::CatalogSnapshot)
            .map(|intent| intent.file_id)
            .collect::<BTreeSet<_>>();
        let committed_active_pointer_ids = build_recovery_plan(&wal_report)
            .map_err(|error| DbError::Wal(error.to_string()))?
            .committed
            .into_iter()
            .flat_map(|transaction| transaction.intents)
            .filter(|intent| intent.artifact_kind == WalArtifactKind::ActiveWeavePointer)
            .map(|intent| (intent.table_id, intent.file_id))
            .collect::<BTreeSet<_>>();

        recover_staged_artifacts(&wal_report, &mut wal, |intent| {
            match intent.artifact_kind {
                WalArtifactKind::SchemaSnapshot => {
                    let snapshot_path = SchemaSnapshot::path(
                        &root,
                        ObjectId(intent.table_id),
                        intent.file_id,
                    ).map_err(|error| WalRecoveryError::SchemaSnapshot(error.to_string()))?;
                    snapshot_path.parent()
                        .map(Path::to_path_buf)
                        .ok_or_else(|| WalRecoveryError::SchemaSnapshot(
                            "schema snapshot path has no parent directory".into(),
                        ))
                }
                WalArtifactKind::CatalogSnapshot => {
                    let directory = root.join("catalog");
                    if committed_catalog_revisions.contains(&intent.file_id) {
                        // Publish/apply the catalog snapshot here, in WAL order,
                        // so a later transaction in this same log can resolve a
                        // table introduced or revised by this DDL transaction.
                        let final_path = publish_staged_wal_artifact(&directory, intent)?;
                        apply_catalog_snapshot_artifact(
                            &root,
                            &mut catalog,
                            intent,
                            &final_path,
                        ).map_err(|error| WalRecoveryError::SchemaSnapshot(error.to_string()))?;
                    }
                    Ok(directory)
                }
                WalArtifactKind::ActiveWeavePointer => {
                    let Some((table_name, definition)) = catalog.tables.iter()
                        .find(|(_, definition)| definition.id.0 == intent.table_id)
                    else {
                        return Err(WalRecoveryError::MissingTableId(intent.table_id));
                    };
                    if definition.schema_revision != intent.schema_revision {
                        return Err(WalRecoveryError::FileConflict(
                            "active pointer schema revision differs from the catalog",
                        ));
                    }
                    let weave_root = root.join("data").join(table_name).join("weaves");
                    if committed_active_pointer_ids.contains(&(intent.table_id, intent.file_id)) {
                        let versioned = publish_staged_wal_artifact(&weave_root, intent)?;
                        let bytes = fs::read(&versioned).map_err(|error| {
                            WalRecoveryError::Io(error.to_string())
                        })?;
                        let pointer = ActiveWeavePointer::decode(&bytes)
                            .map_err(|_| WalRecoveryError::FileConflict("active pointer snapshot is invalid"))?;
                        crate::active_generation::install_recovered_pointer(&weave_root, &pointer)
                            .map_err(|_| WalRecoveryError::FileConflict("active pointer WAL replay failed"))?;
                    }
                    Ok(weave_root)
                }
                WalArtifactKind::Capsule => {
                    let Some((table_name, _definition)) = catalog.tables.iter()
                        .find(|(_, definition)| definition.id.0 == intent.table_id)
                    else {
                        return Err(WalRecoveryError::MissingTableId(intent.table_id));
                    };
                    Ok(root.join("data").join(table_name).join("capsules"))
                }
                WalArtifactKind::NavigationMap => {
                    let Some((table_name, definition)) = catalog.tables.iter()
                        .find(|(_, definition)| definition.id.0 == intent.table_id)
                    else {
                        return Err(WalRecoveryError::MissingTableId(intent.table_id));
                    };
                    if definition.schema_revision != intent.schema_revision {
                        return Err(WalRecoveryError::FileConflict(
                            "Navigation Map schema revision differs from the catalog",
                        ));
                    }
                    Ok(root.join("data").join(table_name).join("capsules"))
                }
                WalArtifactKind::CompactionOutputManifest => {
                    let Some((table_name, definition)) = catalog.tables.iter()
                        .find(|(_, definition)| definition.id.0 == intent.table_id)
                    else {
                        return Err(WalRecoveryError::MissingTableId(intent.table_id));
                    };
                    if definition.schema_revision != intent.schema_revision {
                        return Err(WalRecoveryError::FileConflict(
                            "compaction manifest schema revision differs from the catalog",
                        ));
                    }
                    Ok(root.join("data").join(table_name).join("weaves"))
                }
                WalArtifactKind::AnfSegment | WalArtifactKind::ChronicleRecord => {
                    let Some((table_name, definition)) = catalog.tables.iter()
                        .find(|(_, definition)| definition.id.0 == intent.table_id)
                    else {
                        return Err(WalRecoveryError::MissingTableId(intent.table_id));
                    };
                    SchemaSnapshot::read_from(
                        &root,
                        definition.id,
                        intent.schema_revision,
                    ).map_err(|error| WalRecoveryError::SchemaSnapshot(error.to_string()))?;
                    Ok(root.join("data").join(table_name))
                }
            }
        }).map_err(|error| match error {
            WalRecoveryError::ChronicleCorrupt(message) => DbError::ChronicleCorrupt(message),
            other => DbError::Wal(other.to_string()),
        })?;

        // Anything left here has no verified WAL ownership (for example a
        // process stopped while staging the second metadata artifact).
        cleanup_orphan_segment_temps(&root.join("data"))?;
        cleanup_orphan_catalog_staging_temps(&root)?;
        crate::active_generation::cleanup_orphan_active_pointer_staging(&root, &catalog.tables)?;
        let mut chronicle_indices = BTreeMap::new();
        for (name, definition) in &catalog.tables {
            let directory = root.join("data").join(name);
            chronicle_indices.insert(definition.id, scan_table_index(&directory, definition.id.0)?);
        }
        let mut database = Self {
            root,
            catalog,
            chronicle_indices,
            wal,
            recovery_required: false,
            _writer_lock: writer_lock,
        };
        database.recover_compaction_candidate_publications()?;
        database.recover_compaction_jobs()?;
        database.recover_active_weaves()?;
        Ok(database)
    }

    /// Inspect the lock marker without changing or removing it.
    ///
    /// A PID is informational only: callers must verify process ownership using
    /// operating-system tools before treating an existing lock as stale.
    pub fn inspect_lock(database_directory: impl AsRef<Path>) -> Result<DatabaseLockStatus, DbError> {
        let lock_path = database_directory.as_ref().join(".ananta.lock");
        match fs::read_to_string(&lock_path) {
            Ok(contents) => {
                let owner_process_id = contents.lines()
                    .find_map(|line| line.strip_prefix("pid="))
                    .and_then(|value| value.parse::<u32>().ok());
                Ok(DatabaseLockStatus {
                    lock_path,
                    is_locked: true,
                    owner_process_id,
                })
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(DatabaseLockStatus {
                lock_path,
                is_locked: false,
                owner_process_id: None,
            }),
            Err(error) => Err(DbError::Io(error)),
        }
    }

    /// Inventory persisted files and surface leftover temporary names without deleting them.
    pub fn storage_inventory(&self) -> Result<StorageInventory, DbError> {
        self.ensure_operational()?;
        let mut inventory = StorageInventory::default();
        collect_storage_inventory(&self.root, &self.root, &mut inventory)?;
        inventory.files.sort_by(|a, b| a.relative_path.cmp(&b.relative_path));
        inventory.temporary_files.sort();
        Ok(inventory)
    }

    /// Perform a bounded-memory integrity pass over WAL, table rows, Chronicle,
    /// Capsules and Navigation Maps. It fails closed on the first invalid artifact.
    pub fn check_consistency(&self, batch_rows: usize) -> Result<ConsistencyReport, DbError> {
        self.ensure_operational()?;
        if batch_rows == 0 {
            return Err(DbError::InvalidRange("consistency batch size must be nonzero".into()));
        }
        let wal_path = self.root.join("wal").join("database.anwl");
        let wal_report = read_wal(&wal_path)
            .map_err(|error| DbError::Wal(error.to_string()))?;
        validate_checkpoint_records(&self.root, &wal_report, &self.catalog)?;
        let recovery_plan = build_recovery_plan(&wal_report)
            .map_err(|error| DbError::Wal(error.to_string()))?;
        if !wal_report.is_clean() || !recovery_plan.incomplete.is_empty() {
            return Err(DbError::Wal("consistency check found an incomplete or truncated WAL tail".into()));
        }
        let mut report = ConsistencyReport {
            wal_records_checked: u64::try_from(wal_report.records.len()).map_err(|_| DbError::LengthOverflow)?,
            inventory: self.storage_inventory()?,
            ..ConsistencyReport::default()
        };
        for (table, definition) in &self.catalog.tables {
            let mut scan = self.scan_batches(table, batch_rows)?;
            while let Some(batch) = scan.next() {
                let batch = batch?;
                report.rows_checked = report.rows_checked
                    .checked_add(u64::try_from(batch.row_count).map_err(|_| DbError::LengthOverflow)?)
                    .ok_or(DbError::LengthOverflow)?;
            }
            let table_directory = self.root.join("data").join(table);
            let chronicle_index = scan_table_index(&table_directory, definition.id.0)?;
            report.chronicle_records_checked = report.chronicle_records_checked
                .checked_add(u64::try_from(chronicle_index.by_batch_id.len()).map_err(|_| DbError::LengthOverflow)?)
                .ok_or(DbError::LengthOverflow)?;

            let capsules_directory = table_directory.join("capsules");
            for (capsule_id, generation, _) in list_capsule_paths(&capsules_directory)? {
                self.validate_capsule(table, capsule_id, generation)?;
                report.capsules_checked = report.capsules_checked.checked_add(1).ok_or(DbError::LengthOverflow)?;
            }

            match fs::read_dir(&capsules_directory) {
                Ok(entries) => for entry in entries {
                    let entry = entry?;
                    if !entry.file_type()?.is_file() { continue; }
                    let path = entry.path();
                    if path.extension().and_then(|value| value.to_str()) != Some("nvm") { continue; }
                    let filename = path.file_name().and_then(|value| value.to_str())
                        .ok_or(DbError::NavigationMapCorrupt("Navigation Map filename is not UTF-8"))?;
                    let generation_text = filename.strip_prefix("navigation-")
                        .and_then(|value| value.strip_suffix(".nvm"))
                        .ok_or(DbError::NavigationMapCorrupt("Navigation Map filename is not canonical"))?;
                    if generation_text.len() != 20 || !generation_text.bytes().all(|byte| byte.is_ascii_digit()) {
                        return Err(DbError::NavigationMapCorrupt("Navigation Map filename is not canonical"));
                    }
                    let generation = generation_text.parse::<u64>()
                        .map_err(|_| DbError::NavigationMapCorrupt("Navigation Map generation is invalid"))?;
                    if generation == 0 || format!("navigation-{generation:020}.nvm") != filename {
                        return Err(DbError::NavigationMapCorrupt("Navigation Map filename is not canonical"));
                    }
                    let file_metadata = fs::symlink_metadata(&path)?;
                    if !file_metadata.file_type().is_file() {
                        return Err(DbError::NavigationMapCorrupt("Navigation Map is not a regular file"));
                    }
                    if file_metadata.len() > 256 * 1024 * 1024 {
                        return Err(DbError::NavigationMapCorrupt("Navigation Map exceeds the consistency-check size limit"));
                    }
                    let map_bytes = fs::read(&path)?;
                    let map = NavigationMap::decode(&map_bytes, definition.id.0, generation)?;
                    for map_entry in &map.entries {
                        let capsule_path = capsules_directory.join(format!(
                            "capsule-{:020}-g{:020}.cap",
                            map_entry.metadata.capsule_id,
                            map_entry.metadata.generation,
                        ));
                        let metadata = fs::symlink_metadata(&capsule_path)?;
                        if !metadata.file_type().is_file() || metadata.len() != map_entry.file_len {
                            return Err(DbError::NavigationMapCorrupt("map references a missing or length-mismatched Capsule"));
                        }
                        let bytes = fs::read(&capsule_path)?;
                        if file_sha256(&bytes) != map_entry.capsule_sha256 {
                            return Err(DbError::NavigationMapCorrupt("map Capsule fingerprint mismatch"));
                        }
                        let (capsule, _) = decode_capsule(&bytes)?;
                        if capsule != map_entry.metadata {
                            return Err(DbError::NavigationMapCorrupt("map Capsule metadata mismatch"));
                        }
                    }
                    report.navigation_maps_checked = report.navigation_maps_checked
                        .checked_add(1).ok_or(DbError::LengthOverflow)?;
                },
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {},
                Err(error) => return Err(DbError::Io(error)),
            }
            self.validate_active_weave_integrity(table)?;
            report.tables_checked = report.tables_checked.checked_add(1).ok_or(DbError::LengthOverflow)?;
        }
        Ok(report)
    }

    /// Install a durable checkpoint and atomically rotate the active ANWL log.
    ///
    /// The checkpoint manifest is synced first and fingerprints the immutable
    /// storage/schema artifacts that form the database state at this boundary.
    /// The replacement WAL begins with a CHECKPOINT frame at the previous
    /// high-water sequence plus one; future transactions retain monotonic IDs.
    /// Full replay is retained for every transaction written after the marker.
    pub fn create_checkpoint(&mut self) -> Result<CheckpointReport, DbError> {
        #[cfg(not(unix))]
        {
            return Err(DbError::Wal(
                "atomic WAL checkpoint rotation is currently supported only on Unix".into(),
            ));
        }
        self.ensure_operational()?;
        let consistency = self.check_consistency(1024)?;
        if !consistency.inventory.temporary_files.is_empty() {
            return Err(DbError::Wal(
                "checkpoint refused while staging/temp artifacts remain".into(),
            ));
        }
        let wal_path = self.root.join("wal").join("database.anwl");
        let current = read_wal(&wal_path)
            .map_err(|error| DbError::Wal(error.to_string()))?;
        if !current.is_clean() || !self.wal.incomplete_transaction_ids().is_empty() {
            return Err(DbError::Wal("checkpoint requires a clean WAL with no open transactions".into()));
        }
        let (reference, report) = crate::wal_checkpoint::create_checkpoint(
            &self.root,
            current.last_sequence,
            self.catalog.revision,
        ).map_err(|error| DbError::Wal(format!("checkpoint snapshot failed: {error}")))?;
        if let Err(error) = self.wal.replace_with_checkpoint(current.last_sequence, &reference.encode()) {
            if matches!(&error, WalError::Io(io_error) if io_error.kind() == std::io::ErrorKind::Unsupported) {
                return Err(DbError::Wal(error.to_string()));
            }
            self.recovery_required = true;
            return Err(DbError::RecoveryRequired(format!(
                "checkpoint WAL replacement was uncertain: {error}",
            )));
        }
        let rotated = read_wal(&wal_path)
            .map_err(|error| DbError::Wal(error.to_string()))?;
        validate_checkpoint_records(&self.root, &rotated, &self.catalog)?;
        if rotated.records.len() != 1
            || rotated.last_sequence != current.last_sequence.saturating_add(1)
            || !rotated.is_clean()
        {
            self.recovery_required = true;
            return Err(DbError::RecoveryRequired(
                "rotated WAL did not reopen as a clean checkpoint anchor".into(),
            ));
        }
        report.pruned_manifest_count = crate::wal_checkpoint::prune_superseded_checkpoint_manifests(
            &self.root,
            &report.manifest_path,
        ).map_err(|error| DbError::Wal(format!(
            "checkpoint is durable but superseded-manifest pruning failed: {error}",
        )))?;
        Ok(report)
    }

    /// Create a point-in-time, checksummed backup of the database directory.
    ///
    /// The current handle owns the exclusive writer lock, and each acknowledged
    /// mutation has a durable WAL commit. The backup excludes only the live lock
    /// file; restore always creates a fresh destination and validates it by opening
    /// the restored database through the normal recovery path.
    pub fn backup_to(&self, destination: impl AsRef<Path>) -> Result<BackupReport, DbError> {
        self.ensure_operational()?;
        let consistency = self.check_consistency(1024)?;
        if !consistency.inventory.temporary_files.is_empty() {
            return Err(DbError::Backup(
                "backup refused while staging/temp artifacts remain; inspect storage_inventory first".into(),
            ));
        }
        crate::backup::create_backup(&self.root, destination.as_ref())
            .map_err(|error| DbError::Backup(error.to_string()))
    }

    /// Verify and restore an ANBK v1 backup into a new directory.
    ///
    /// A successful return means both the backup manifest and the restored
    /// database's ordinary startup recovery checks have completed.
    pub fn restore_backup(
        backup_directory: impl AsRef<Path>,
        destination: impl AsRef<Path>,
    ) -> Result<BackupReport, DbError> {
        crate::backup::verify_backup(backup_directory.as_ref())
            .map_err(|error| DbError::Backup(error.to_string()))?;
        let report = crate::backup::restore_backup(
            backup_directory.as_ref(),
            destination.as_ref(),
        ).map_err(|error| DbError::Backup(error.to_string()))?;
        let restored = Self::open(&report.directory)?;
        restored.check_consistency(1024)?;
        drop(restored);
        Ok(report)
    }

    fn ensure_operational(&self) -> Result<(), DbError> {
        if self.recovery_required {
            Err(DbError::RecoveryRequired(
                "a committed WAL intent could not be published; close and reopen the database".into(),
            ))
        } else {
            Ok(())
        }
    }

    fn map_precommit_stage_error(error: WalRecoveryError) -> DbError {
        match error {
            WalRecoveryError::Io(message) => DbError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                message,
            )),
            other => DbError::Wal(other.to_string()),
        }
    }

    fn commit_wal_intent(
        &mut self,
        intent: &WalMutationIntent,
        operation: &[u8],
    ) -> Result<(), DbError> {
        self.commit_wal_intents(std::slice::from_ref(intent), operation)
    }

    fn commit_wal_intents(
        &mut self,
        intents: &[WalMutationIntent],
        operation: &[u8],
    ) -> Result<(), DbError> {
        if intents.is_empty() {
            return Err(DbError::Wal("cannot commit an empty mutation-intent list".into()));
        }
        let payloads = intents.iter()
            .map(|intent| intent.encode().map_err(|error| DbError::Wal(error.to_string())))
            .collect::<Result<Vec<_>, _>>()?;
        let transaction_id = self.wal.next_sequence();
        if let Err(error) = self.wal.begin_transaction(transaction_id, operation) {
            self.recovery_required = true;
            return Err(DbError::RecoveryRequired(format!(
                "could not append WAL BEGIN: {error}"
            )));
        }
        crate::wal_recovery::maybe_crash_after(
            crate::wal_recovery::WalRecoveryCrashPoint::AfterWalBegin,
        );
        for payload in &payloads {
            if let Err(error) = self.wal.append_mutation(transaction_id, payload) {
                self.recovery_required = true;
                return Err(DbError::RecoveryRequired(format!(
                    "could not append WAL mutation intent: {error}"
                )));
            }
            crate::wal_recovery::maybe_crash_after(
                crate::wal_recovery::WalRecoveryCrashPoint::AfterWalMutation,
            );
        }
        if let Err(error) = self.wal.commit_transaction(transaction_id) {
            self.recovery_required = true;
            return Err(DbError::RecoveryRequired(format!(
                "WAL COMMIT durability is uncertain: {error}"
            )));
        }
        crate::wal_recovery::maybe_crash_after(
            crate::wal_recovery::WalRecoveryCrashPoint::AfterWalCommit,
        );
        Ok(())
    }

    fn publish_wal_intent(
        &mut self,
        directory: &Path,
        intent: &WalMutationIntent,
    ) -> Result<PathBuf, DbError> {
        match publish_staged_wal_artifact(directory, intent) {
            Ok(path) => Ok(path),
            Err(error) => {
                self.recovery_required = true;
                Err(DbError::RecoveryRequired(error.to_string()))
            }
        }
    }

    fn commit_catalog_mutation(
        &mut self,
        candidate: Catalog,
        snapshot: SchemaSnapshot,
        operation: &[u8],
    ) -> Result<(), DbError> {
        self.ensure_operational()?;

        let table_id = snapshot.table_id();
        let schema_revision = snapshot.revision();
        let expected_catalog_revision = self.catalog.revision
            .checked_add(1)
            .ok_or(DbError::LengthOverflow)?;
        if candidate.revision != expected_catalog_revision {
            return Err(DbError::CorruptCatalog(
                "catalog mutation revision is not the next revision",
            ));
        }
        let definition = candidate.tables.values()
            .find(|definition| definition.id == table_id)
            .ok_or(DbError::CorruptCatalog("catalog mutation snapshot TableId is missing"))?;
        if definition.schema_revision != schema_revision
            || snapshot.schema()? != definition.schema
            || snapshot.column_ids().collect::<Vec<_>>() != definition.column_ids
        {
            return Err(DbError::SchemaCompatibility(
                "catalog mutation snapshot does not match candidate catalog",
            ));
        }

        let table_name = candidate.tables.iter()
            .find(|(_, definition)| definition.id == table_id)
            .map(|(name, _)| name.as_str())
            .ok_or(DbError::CorruptCatalog("catalog mutation table name is missing"))?;
        let data_root = self.root.join("data");
        fs::create_dir_all(&data_root)?;
        let table_directory = data_root.join(table_name);
        let table_previously_existed = self.catalog.tables.values()
            .any(|existing| existing.id == table_id);
        match fs::symlink_metadata(&table_directory) {
            Ok(metadata) if metadata.file_type().is_dir() => {
                if !table_previously_existed && fs::read_dir(&table_directory)?.next().is_some() {
                    return Err(DbError::CorruptCatalog(
                        "new catalog table directory is not empty",
                    ));
                }
            }
            Ok(_) => {
                return Err(DbError::CorruptCatalog(
                    "catalog table path is not a real directory",
                ));
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                if table_previously_existed {
                    return Err(DbError::CorruptCatalog(
                        "existing catalog table directory is missing",
                    ));
                }
                fs::create_dir(&table_directory)?;
                sync_recovery_directory(&data_root)?;
            }
            Err(error) => return Err(DbError::Io(error)),
        }
        sync_recovery_directory(&table_directory)?;

        let schema_path = SchemaSnapshot::path(&self.root, table_id, schema_revision)?;
        let schema_directory = schema_path.parent().ok_or(DbError::LengthOverflow)?.to_path_buf();
        let schemas_root = self.root.join("catalog").join("schemas");
        let catalog_directory = self.root.join("catalog");
        fs::create_dir_all(&schema_directory)?;
        // Persist each new directory entry before WAL COMMIT can depend on it.
        sync_recovery_directory(&catalog_directory)?;
        sync_recovery_directory(&schemas_root)?;
        sync_recovery_directory(&schema_directory)?;

        let snapshot_bytes = snapshot.encode()?;
        let catalog_bytes = encode_catalog_v2(&candidate)?;
        let catalog_replacement_temp = catalog_directory.join("catalog.anc.tmp");
        match fs::symlink_metadata(&catalog_replacement_temp) {
            Ok(_) => {
                return Err(DbError::Io(std::io::Error::new(
                    std::io::ErrorKind::AlreadyExists,
                    "catalog replacement temporary path already exists",
                )));
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(DbError::Io(error)),
        }
        verify_immutable_file_if_present(
            &schema_path,
            &snapshot_bytes,
            "immutable schema snapshot conflict",
        )?;
        let catalog_snapshot_path = catalog_directory.join(
            WalArtifactKind::CatalogSnapshot.final_file_name(candidate.revision),
        );
        verify_immutable_file_if_present(
            &catalog_snapshot_path,
            &catalog_bytes,
            "immutable catalog snapshot conflict",
        )?;
        let schema_intent = WalMutationIntent {
            artifact_kind: WalArtifactKind::SchemaSnapshot,
            table_id: table_id.0,
            schema_revision,
            file_id: schema_revision,
            file_len: u64::try_from(snapshot_bytes.len()).map_err(|_| DbError::LengthOverflow)?,
            sha256: chronicle::sha256(&snapshot_bytes),
        };
        let catalog_intent = WalMutationIntent {
            artifact_kind: WalArtifactKind::CatalogSnapshot,
            table_id: table_id.0,
            schema_revision,
            file_id: candidate.revision,
            file_len: u64::try_from(catalog_bytes.len()).map_err(|_| DbError::LengthOverflow)?,
            sha256: chronicle::sha256(&catalog_bytes),
        };

        stage_wal_artifact(&schema_directory, &schema_intent, &snapshot_bytes)
            .map_err(Self::map_precommit_stage_error)?;
        if let Err(error) = stage_wal_artifact(&catalog_directory, &catalog_intent, &catalog_bytes) {
            let _ = discard_staged_wal_artifact(&schema_directory, &schema_intent);
            return Err(Self::map_precommit_stage_error(error));
        }

        self.commit_wal_intents(&[schema_intent.clone(), catalog_intent.clone()], operation)?;
        self.publish_wal_intent(&schema_directory, &schema_intent)?;
        crate::wal_recovery::maybe_crash_after(
            crate::wal_recovery::WalRecoveryCrashPoint::AfterSchemaSnapshotPublished,
        );
        let catalog_path = self.publish_wal_intent(&catalog_directory, &catalog_intent)?;
        crate::wal_recovery::maybe_crash_after(
            crate::wal_recovery::WalRecoveryCrashPoint::AfterCatalogSnapshotPublished,
        );
        if let Err(error) = apply_catalog_snapshot_artifact(
            &self.root,
            &mut self.catalog,
            &catalog_intent,
            &catalog_path,
        ) {
            self.recovery_required = true;
            return Err(DbError::RecoveryRequired(format!(
                "committed catalog mutation requires recovery: {error}"
            )));
        }
        Ok(())
    }

    pub fn create_table(&mut self, name: impl Into<String>, schema: Schema) -> Result<(), DbError> {
        self.ensure_operational()?;
        let name = name.into();
        validate_name(&name)?;
        if self.catalog.tables.contains_key(&name) {
            return Err(DbError::TableExists(name));
        }

        let mut candidate = self.catalog.clone();
        let table_id = allocate_object_id(&mut candidate)?;
        let mut column_ids = Vec::with_capacity(schema.len());
        for _ in schema.columns() {
            column_ids.push(allocate_object_id(&mut candidate)?);
        }
        increment_catalog_revision(&mut candidate)?;

        candidate.tables.insert(name.clone(), TableDefinition {
            id: table_id,
            schema_revision: 1,
            column_ids,
            schema,
        });

        let definition = candidate.tables.get(&name).expect("new table inserted");
        let snapshot = SchemaSnapshot::new(
            definition.id,
            definition.schema_revision,
            &definition.schema,
            &definition.column_ids,
        )?;
        self.commit_catalog_mutation(candidate, snapshot, b"create_table")?;
        self.chronicle_indices.entry(table_id).or_default();
        Ok(())
    }

    pub fn schema(&self, table: &str) -> Result<&Schema, DbError> {
        self.ensure_operational()?;
        self.catalog.tables.get(table)
            .map(|definition| &definition.schema)
            .ok_or_else(|| DbError::TableNotFound(table.to_owned()))
    }

    pub fn table_names(&self) -> impl Iterator<Item = &str> {
        self.catalog.tables.keys().map(String::as_str)
    }

    /// Current database-wide catalog revision. Starts at zero for an empty database.
    pub fn catalog_revision(&self) -> u64 {
        self.catalog.revision
    }

    /// Stable database-local identity for a table.
    pub fn table_id(&self, table: &str) -> Result<ObjectId, DbError> {
        self.ensure_operational()?;
        self.catalog.tables.get(table)
            .map(|definition| definition.id)
            .ok_or_else(|| DbError::TableNotFound(table.to_owned()))
    }

    /// Canonical path for the disposable, checksummed analysis cache of a table.
    /// The cache is keyed by stable table identity and is never a source of row data.
    pub fn statistics_cache_path(&self, table: &str) -> Result<PathBuf, DbError> {
        self.ensure_operational()?;
        let table_id = self.table_id(table)?;
        Ok(self.root.join("catalog").join("statistics")
            .join(format!("table-{:020}.ast", table_id.0)))
    }

    /// Stable database-local identity for a column.
    pub fn column_id(&self, table: &str, column: &str) -> Result<ObjectId, DbError> {
        self.ensure_operational()?;
        let definition = self.catalog.tables.get(table)
            .ok_or_else(|| DbError::TableNotFound(table.to_owned()))?;
        let index = definition.schema.columns().iter()
            .position(|candidate| candidate.name == column)
            .ok_or_else(|| DbError::ColumnNotFound(column.to_owned()))?;
        Ok(definition.column_ids[index])
    }

    /// Logical schema revision for a table. A newly-created table starts at revision one.
    pub fn table_schema_revision(&self, table: &str) -> Result<u64, DbError> {
        self.ensure_operational()?;
        self.catalog.tables.get(table)
            .map(|definition| definition.schema_revision)
            .ok_or_else(|| DbError::TableNotFound(table.to_owned()))
    }

    /// Check catalog-to-snapshot references and report missing, corrupt or orphaned files.
    ///
    /// Valid historical revisions below the current catalog revision are retained;
    /// the checker cannot classify them as garbage until segment manifests carry
    /// schema revision references.
    pub fn check_schema_snapshot_consistency(
        &self,
    ) -> Result<SchemaSnapshotConsistencyReport, DbError> {
        self.ensure_operational()?;
        let mut report = SchemaSnapshotConsistencyReport::default();
        let current = self.catalog.tables.values()
            .map(|definition| (definition.id, definition.schema_revision))
            .collect::<BTreeMap<_, _>>();
        let mut found_current = BTreeSet::new();
        let schemas_root = self.root.join("catalog").join("schemas");

        match fs::read_dir(&schemas_root) {
            Ok(table_dirs) => {
                for table_entry in table_dirs {
                    let table_entry = table_entry?;
                    let table_dir = table_entry.path();
                    let file_type = table_entry.file_type()?;
                    let table_name = table_entry.file_name();
                    let table_name = table_name.to_string_lossy();

                    if !file_type.is_dir() {
                        report.invalid_files.push(table_dir);
                        continue;
                    }
                    let table_id = match table_name.parse::<u64>() {
                        Ok(id) if id != 0 && id.to_string() == table_name => ObjectId(id),
                        _ => {
                            report.invalid_files.push(table_dir);
                            continue;
                        }
                    };

                    for entry in fs::read_dir(&table_dir)? {
                        let entry = entry?;
                        let path = entry.path();
                        let filename = entry.file_name();
                        let filename = filename.to_string_lossy();
                        let file_type = entry.file_type()?;

                        if filename.starts_with("schema-") && path.extension().and_then(|s| s.to_str()) == Some("tmp") {
                            report.orphaned_files.push(path);
                            continue;
                        }
                        if path.extension().and_then(|s| s.to_str()) != Some("ans") {
                            continue;
                        }
                        if !file_type.is_file() {
                            report.invalid_files.push(path);
                            continue;
                        }

                        let revision = path.file_stem()
                            .and_then(|stem| stem.to_str())
                            .and_then(|stem| stem.strip_prefix("schema-"))
                            .and_then(|value| value.parse::<u64>().ok());
                        let Some(revision) = revision else {
                            report.invalid_files.push(path);
                            continue;
                        };
                        if filename != format!("schema-{revision:020}.ans") || revision == 0 {
                            report.invalid_files.push(path);
                            continue;
                        }

                        match SchemaSnapshot::read_from(&self.root, table_id, revision) {
                            Ok(_) => match current.get(&table_id) {
                                None => report.orphaned_files.push(path),
                                Some(current_revision) if revision > *current_revision => {
                                    report.orphaned_files.push(path)
                                }
                                Some(current_revision) if revision == *current_revision => {
                                    found_current.insert((table_id, *current_revision));
                                }
                                Some(_) => {}
                            },
                            Err(_) => report.invalid_files.push(path),
                        }
                    }
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(DbError::Io(error)),
        }

        for (table_id, revision) in current {
            if !found_current.contains(&(table_id, revision)) {
                report.missing_current.push((table_id, revision));
            }
        }
        report.missing_current.sort();
        report.invalid_files.sort();
        report.orphaned_files.sort();
        Ok(report)
    }

    /// Persist the current table schema as an immutable sidecar snapshot.
    ///
    /// This does not change the active catalog revision or enable schema DDL.
    pub fn write_schema_snapshot(&self, table: &str) -> Result<PathBuf, DbError> {
        self.ensure_operational()?;
        let definition = self.catalog.tables.get(table)
            .ok_or_else(|| DbError::TableNotFound(table.to_owned()))?;
        let snapshot = SchemaSnapshot::new(
            definition.id,
            definition.schema_revision,
            &definition.schema,
            &definition.column_ids,
        )?;
        snapshot.write_to(&self.root)
    }

    /// Load a historical schema snapshot for a table by revision.
    pub fn read_schema_snapshot(
        &self,
        table: &str,
        revision: u64,
    ) -> Result<SchemaSnapshot, DbError> {
        self.ensure_operational()?;
        let definition = self.catalog.tables.get(table)
            .ok_or_else(|| DbError::TableNotFound(table.to_owned()))?;
        SchemaSnapshot::read_from(&self.root, definition.id, revision)
    }

    pub fn insert_batch(&mut self, table: &str, rows: &[Vec<Value>]) -> Result<Option<PathBuf>, DbError> {
        self.ensure_operational()?;
        let definition = self.catalog.tables.get(table)
            .ok_or_else(|| DbError::TableNotFound(table.to_owned()))?;
        for row in rows { definition.schema.validate_row(row)?; }
        if rows.is_empty() { return Ok(None); }

        let table_id = definition.id.0;
        let schema_revision = definition.schema_revision;
        let column_ids = definition.column_ids.iter().map(|id| id.0).collect::<Vec<_>>();
        let bytes = encode_segment_v2(
            &definition.schema,
            rows,
            table_id,
            schema_revision,
            &column_ids,
        )?;

        let table_dir = self.root.join("data").join(table);
        fs::create_dir_all(&table_dir)?;
        let segment_id = next_segment_id(&table_dir)?;
        let intent = WalMutationIntent {
            artifact_kind: WalArtifactKind::AnfSegment,
            table_id,
            schema_revision,
            file_id: segment_id,
            file_len: u64::try_from(bytes.len()).map_err(|_| DbError::LengthOverflow)?,
            sha256: chronicle::sha256(&bytes),
        };
        stage_wal_artifact(&table_dir, &intent, &bytes)
            .map_err(Self::map_precommit_stage_error)?;
        self.commit_wal_intent(&intent, b"ANF insert_batch")?;
        let final_path = self.publish_wal_intent(&table_dir, &intent)?;
        Ok(Some(final_path))
    }

    /// Atomically ingest one batch using a caller-supplied idempotency identity.
    ///
    /// An identical retry returns the original receipt. A reused BatchId with a
    /// different historical payload is rejected. Data and receipt are published
    /// together in one immutable Chronicle file.
    pub fn ingest_batch(
        &mut self,
        table: &str,
        batch_id: BatchId,
        rows: &[Vec<Value>],
    ) -> Result<IngestResult, DbError> {
        self.ensure_operational()?;
        if rows.is_empty() {
            let table_id = self.table_id(table)?;
            if let Some(index) = self.chronicle_indices.get_mut(&table_id) {
                index.stats.rejected_requests = index.stats.rejected_requests
                    .checked_add(1).ok_or(DbError::LengthOverflow)?;
            }
            return Err(DbError::EmptyBatch);
        }

        let definition = self.catalog.tables.get(table)
            .ok_or_else(|| DbError::TableNotFound(table.to_owned()))?
            .clone();
        let table_id = definition.id;

        if let Some(existing) = self.chronicle_indices.get(&table_id)
            .and_then(|index| index.by_batch_id.get(&batch_id))
            .cloned()
        {
            let snapshot = SchemaSnapshot::read_from(
                &self.root,
                definition.id,
                existing.metadata.schema_revision,
            )?;
            let historical_ids = snapshot.column_ids().map(|id| id.0).collect::<Vec<_>>();
            let candidate = encode_segment_v2(
                &snapshot.schema()?,
                rows,
                definition.id.0,
                existing.metadata.schema_revision,
                &historical_ids,
            );
            let fingerprint_matches = candidate
                .as_ref()
                .map(|bytes| chronicle::sha256(bytes) == existing.metadata.fingerprint)
                .unwrap_or(false);
            if !fingerprint_matches {
                if let Some(index) = self.chronicle_indices.get_mut(&table_id) {
                    index.stats.rejected_requests = index.stats.rejected_requests
                        .checked_add(1).ok_or(DbError::LengthOverflow)?;
                }
                return Err(DbError::BatchIdConflict(batch_id));
            }
            if let Some(index) = self.chronicle_indices.get_mut(&table_id) {
                index.stats.duplicate_requests = index.stats.duplicate_requests
                    .checked_add(1).ok_or(DbError::LengthOverflow)?;
            }
            return Ok(IngestResult {
                outcome: IngestOutcome::Duplicate,
                batch_id,
                table_id: definition.id.0,
                ingestion_sequence: existing.metadata.sequence,
                row_count: existing.metadata.row_count,
                encoded_bytes: existing.metadata.payload_len,
                chronicle_path: existing.path,
            });
        }

        for (row_index, row) in rows.iter().enumerate() {
            if let Err(error) = definition.schema.validate_row(row) {
                if let Some(index) = self.chronicle_indices.get_mut(&table_id) {
                    index.stats.rejected_requests = index.stats.rejected_requests
                        .checked_add(1).ok_or(DbError::LengthOverflow)?;
                }
                return Err(DbError::IngestRowValidation {
                    row_index,
                    message: error.to_string(),
                });
            }
        }

        let column_ids = definition.column_ids.iter().map(|id| id.0).collect::<Vec<_>>();
        let payload = match encode_segment_v2(
            &definition.schema,
            rows,
            definition.id.0,
            definition.schema_revision,
            &column_ids,
        ) {
            Ok(payload) => payload,
            Err(error) => {
                if let Some(index) = self.chronicle_indices.get_mut(&table_id) {
                    index.stats.rejected_requests = index.stats.rejected_requests
                        .checked_add(1).ok_or(DbError::LengthOverflow)?;
                }
                return Err(DbError::Format(error));
            }
        };
        let sequence = self.chronicle_indices.get(&table_id)
            .ok_or(DbError::ChronicleCorrupt("table has no Chronicle receipt index"))?
            .next_sequence()?;
        let (record_bytes, metadata) = encode_record(
            definition.id.0,
            batch_id,
            sequence,
            definition.schema_revision,
            u64::try_from(rows.len()).map_err(|_| DbError::LengthOverflow)?,
            &payload,
        )?;

        let directory = self.root.join("data").join(table);
        fs::create_dir_all(&directory)?;
        let physical_file_id = next_segment_id(&directory)?;
        let intent = WalMutationIntent {
            artifact_kind: WalArtifactKind::ChronicleRecord,
            table_id: definition.id.0,
            schema_revision: definition.schema_revision,
            file_id: physical_file_id,
            file_len: u64::try_from(record_bytes.len()).map_err(|_| DbError::LengthOverflow)?,
            sha256: chronicle::sha256(&record_bytes),
        };
        if let Err(error) = stage_wal_artifact(&directory, &intent, &record_bytes) {
            let index = self.chronicle_indices.get_mut(&table_id)
                .ok_or(DbError::ChronicleCorrupt("table has no Chronicle receipt index"))?;
            index.stats.publication_failures = index.stats.publication_failures
                .checked_add(1).ok_or(DbError::LengthOverflow)?;
            return Err(Self::map_precommit_stage_error(error));
        }
        self.commit_wal_intent(&intent, b"Chronicle ingest_batch")?;

        let path = match self.publish_wal_intent(&directory, &intent) {
            Ok(path) => path,
            Err(error) => {
                if let Some(index) = self.chronicle_indices.get_mut(&table_id) {
                    index.stats.uncertain_publications = index.stats.uncertain_publications
                        .checked_add(1).ok_or(DbError::LengthOverflow)?;
                }
                return Err(error);
            }
        };
        let receipt = BatchReceipt {
            metadata: metadata.clone(),
            path: path.clone(),
        };
        if let Err(error) = self.chronicle_indices
            .get_mut(&table_id)
            .ok_or(DbError::ChronicleCorrupt("table has no Chronicle receipt index"))?
            .record(receipt)
        {
            self.recovery_required = true;
            return Err(DbError::RecoveryRequired(format!(
                "Chronicle file was committed but the in-memory receipt index could not advance: {error}"
            )));
        }

        Ok(IngestResult {
            outcome: IngestOutcome::Accepted,
            batch_id,
            table_id: definition.id.0,
            ingestion_sequence: sequence,
            row_count: metadata.row_count,
            encoded_bytes: metadata.payload_len,
            chronicle_path: path,
        })
    }

    /// Build one immutable Capsule from an inclusive contiguous range of committed Chronicle sequences.
    ///
    /// The current logical schema is used for the derived ANF payload. Historical source
    /// batches are resolved by stable ColumnId. No Chronicle records are removed or hidden.
    pub fn build_capsule(
        &mut self,
        table: &str,
        capsule_id: u64,
        generation: u64,
        first_sequence: u64,
        last_sequence: u64,
        event_time_column: Option<&str>,
    ) -> Result<PathBuf, DbError> {
        self.ensure_operational()?;
        if capsule_id == 0 || generation == 0 || first_sequence == 0 || last_sequence < first_sequence {
            return Err(DbError::CapsuleCorrupt("Capsule identity, generation or source sequence range is invalid"));
        }
        let definition = self.catalog.tables.get(table)
            .ok_or_else(|| DbError::TableNotFound(table.to_owned()))?
            .clone();
        let index = self.chronicle_indices.get(&definition.id)
            .ok_or(DbError::CapsuleCorrupt("table has no Chronicle receipt index"))?;
        let mut receipts = index.by_batch_id.values()
            .filter(|receipt| receipt.metadata.sequence >= first_sequence
                && receipt.metadata.sequence <= last_sequence)
            .cloned()
            .collect::<Vec<_>>();
        receipts.sort_by_key(|receipt| receipt.metadata.sequence);
        let expected_receipts = last_sequence.checked_sub(first_sequence)
            .and_then(|value| value.checked_add(1))
            .ok_or(DbError::LengthOverflow)?;
        if u64::try_from(receipts.len()).map_err(|_| DbError::LengthOverflow)? != expected_receipts {
            return Err(DbError::CapsuleCorrupt("source sequence range is not fully committed and contiguous"));
        }

        let mut rows = Vec::new();
        for (offset, receipt) in receipts.iter().enumerate() {
            let expected_sequence = first_sequence.checked_add(offset as u64).ok_or(DbError::LengthOverflow)?;
            if receipt.metadata.sequence != expected_sequence {
                return Err(DbError::CapsuleCorrupt("source Chronicle sequences contain a gap"));
            }
            let record_bytes = fs::read(&receipt.path)?;
            let (record_metadata, payload) = extract_payload(&record_bytes, definition.id.0)?;
            if record_metadata.batch_id != receipt.metadata.batch_id
                || record_metadata.sequence != receipt.metadata.sequence
                || record_metadata.fingerprint != receipt.metadata.fingerprint
            {
                return Err(DbError::ChronicleCorrupt("receipt index differs from source Chronicle record"));
            }
            let mut batch_rows = self.decode_and_resolve_segment(table, &definition, &payload)?;
            rows.append(&mut batch_rows);
            let cells = rows.len().checked_mul(definition.schema.len()).ok_or(DbError::LengthOverflow)?;
            if rows.len() > 1_000_000 || cells > 1_000_000 {
                return Err(DbError::CapsuleCorrupt("Capsule exceeds ANF row or decoded-cell limits"));
            }
        }
        if rows.is_empty() {
            return Err(DbError::CapsuleCorrupt("Capsule cannot be empty"));
        }

        let event_time_index = match event_time_column {
            Some(name) => {
                let index = definition.schema.columns().iter().position(|column| column.name == name)
                    .ok_or_else(|| DbError::ColumnNotFound(name.to_owned()))?;
                if definition.schema.columns()[index].data_type != DataType::TimestampMicros {
                    return Err(DbError::CapsuleCorrupt("event-time column must have type TIMESTAMP_MICROS"));
                }
                Some(index)
            }
            None => None,
        };
        let column_ids = definition.column_ids.iter().map(|id| id.0).collect::<Vec<_>>();
        let payload = encode_segment_v2(
            &definition.schema,
            &rows,
            definition.id.0,
            definition.schema_revision,
            &column_ids,
        )?;
        let inspection = inspect_segment_v2_details(&payload)?;
        let columns = derive_column_stats(&definition.schema, &column_ids, &rows, &inspection)?;
        let event_time_column_id = event_time_index.map(|index| column_ids[index]);
        let (temporal_min, temporal_max) = derive_temporal_bounds(&columns, event_time_column_id)?;
        let metadata = CapsuleMetadata {
            table_id: definition.id.0,
            capsule_id,
            generation,
            schema_revision: definition.schema_revision,
            row_count: u64::try_from(rows.len()).map_err(|_| DbError::LengthOverflow)?,
            first_ingestion_sequence: first_sequence,
            last_ingestion_sequence: last_sequence,
            event_time_column_id,
            temporal_min,
            temporal_max,
            payload_len: 0,
            payload_sha256: [0; 32],
            columns,
        };
        let capsule_bytes = encode_capsule(metadata, &payload)?;
        let capsule_directory = self.root.join("data").join(table).join("capsules");
        fs::create_dir_all(&capsule_directory)?;
        let final_path = capsule_directory.join(format!(
            "capsule-{capsule_id:020}-g{generation:020}.cap",
        ));
        if final_path.exists() {
            return Err(DbError::Io(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                "immutable Capsule generation already exists",
            )));
        }
        let intent = WalMutationIntent {
            artifact_kind: WalArtifactKind::Capsule,
            table_id: definition.id.0,
            // Capsule generations are independent from logical schema revisions.
            // AMUT v1 keeps this generation in the existing revision slot; the
            // Capsule header carries and validates its actual schema revision.
            schema_revision: generation,
            file_id: capsule_id,
            file_len: u64::try_from(capsule_bytes.len()).map_err(|_| DbError::LengthOverflow)?,
            sha256: chronicle::sha256(&capsule_bytes),
        };
        stage_wal_artifact(&capsule_directory, &intent, &capsule_bytes)
            .map_err(Self::map_precommit_stage_error)?;
        self.commit_wal_intent(&intent, b"Capsule build_capsule")?;
        self.publish_wal_intent(&capsule_directory, &intent)
    }

    /// Verify every Capsule and Chronicle file pinned by a canonical compaction input manifest.
    ///
    /// This performs real disk preflight, not just manifest self-consistency checks:
    /// it rejects symlink/non-regular paths, checks exact file lengths and SHA-256,
    /// verifies Chronicle framing and Capsule checksums, recomputes Capsule stats,
    /// and validates historical schema compatibility.
    pub fn preflight_compaction_inputs(
        &self,
        table: &str,
        manifest: &crate::compaction_manifest::CompactionInputManifest,
    ) -> Result<CompactionPreflightReport, DbError> {
        self.ensure_operational()?;
        manifest.validate()?;
        let definition = self.catalog.tables.get(table)
            .ok_or_else(|| DbError::TableNotFound(table.to_owned()))?;
        if manifest.table_id != definition.id.0 {
            return Err(DbError::CompactionPreflight("manifest TableId does not match the requested table"));
        }
        if manifest.target_schema_revision != definition.schema_revision {
            return Err(DbError::CompactionPreflight("manifest target schema revision is stale"));
        }

        let data_root = self.root.join("data");
        let table_dir = data_root.join(table);
        let capsule_dir = table_dir.join("capsules");
        require_real_directory(&data_root)?;
        require_real_directory(&table_dir)?;
        require_real_directory(&capsule_dir)?;

        let index = self.chronicle_indices.get(&definition.id)
            .ok_or(DbError::CompactionPreflight("table Chronicle receipt index is missing"))?;
        let mut total_source_bytes = 0_u64;

        for reference in &manifest.receipts {
            let batch_id = BatchId::from_bytes(reference.batch_id)?;
            let receipt = index.by_batch_id.get(&batch_id)
                .ok_or(DbError::CompactionPreflight("manifest Chronicle BatchId is not present in the table receipt index"))?;
            if receipt.metadata.table_id != definition.id.0
                || receipt.metadata.sequence != reference.sequence
                || receipt.metadata.row_count != reference.row_count
                || receipt.metadata.fingerprint != reference.payload_sha256
            {
                return Err(DbError::CompactionPreflight("Chronicle receipt index metadata differs from the manifest"));
            }
            if !is_canonical_chronicle_path(&receipt.path, &table_dir) {
                return Err(DbError::CompactionPreflight("Chronicle receipt path is not a canonical table-local record path"));
            }

            let bytes = read_verified_regular_file(
                &receipt.path,
                reference.record_file_len,
                reference.record_file_sha256,
            )?;
            let mut cursor = Cursor::new(bytes.as_slice());
            let (metadata, _) = inspect_chronicle_reader(&mut cursor, definition.id.0)?;
            if metadata != receipt.metadata
                || metadata.batch_id.as_bytes() != &reference.batch_id
                || metadata.sequence != reference.sequence
                || metadata.row_count != reference.row_count
                || metadata.fingerprint != reference.payload_sha256
            {
                return Err(DbError::CompactionPreflight("Chronicle record contents differ from the pinned receipt"));
            }
            total_source_bytes = total_source_bytes.checked_add(reference.record_file_len)
                .ok_or(DbError::LengthOverflow)?;
        }

        for reference in &manifest.capsules {
            let path = capsule_dir.join(format!(
                "capsule-{:020}-g{:020}.cap",
                reference.capsule_id,
                reference.generation
            ));
            let bytes = read_verified_regular_file(
                &path,
                reference.file_len,
                reference.file_sha256,
            )?;
            let (metadata, payload) = decode_capsule(&bytes)?;
            if metadata.table_id != definition.id.0
                || metadata.capsule_id != reference.capsule_id
                || metadata.generation != reference.generation
                || metadata.schema_revision != reference.schema_revision
                || metadata.first_ingestion_sequence != reference.first_sequence
                || metadata.last_ingestion_sequence != reference.last_sequence
                || metadata.row_count != reference.row_count
            {
                return Err(DbError::CompactionPreflight("Capsule metadata differs from its pinned manifest reference"));
            }
            if metadata.schema_revision > definition.schema_revision {
                return Err(DbError::CompactionPreflight("Capsule schema revision is newer than the target schema"));
            }

            let snapshot = SchemaSnapshot::read_from(
                &self.root,
                definition.id,
                metadata.schema_revision,
            ).map_err(|_| DbError::CapsuleCorrupt("Capsule schema snapshot is missing or corrupt"))?;
            let historical_schema = snapshot.schema()?;
            let historical_ids = snapshot.column_ids().map(|id| id.0).collect::<Vec<_>>();
            let rows = decode_segment_v2(
                &payload,
                &historical_schema,
                definition.id.0,
                metadata.schema_revision,
                &historical_ids,
            )?;
            if u64::try_from(rows.len()).map_err(|_| DbError::LengthOverflow)? != metadata.row_count {
                return Err(DbError::CapsuleCorrupt("Capsule row count differs from decoded payload"));
            }
            let inspection = inspect_segment_v2_details(&payload)?;
            let recomputed = derive_column_stats(&historical_schema, &historical_ids, &rows, &inspection)?;
            if !capsule_stats_equal(&metadata.columns, &recomputed) {
                return Err(DbError::CapsuleCorrupt("Capsule column statistics do not match decoded payload"));
            }
            let (temporal_min, temporal_max) = derive_temporal_bounds(&recomputed, metadata.event_time_column_id)?;
            if temporal_min != metadata.temporal_min || temporal_max != metadata.temporal_max {
                return Err(DbError::CapsuleCorrupt("Capsule temporal bounds do not match decoded payload"));
            }
            let resolved_rows = resolve_rows_by_column_id(&snapshot, definition, rows)?;

            // Counts and statistics cannot prove row lineage. Reconstruct each source
            // batch in sequence/row-ordinal order and compare canonical typed fingerprints.
            let mut capsule_row_offset = 0_usize;
            for receipt_reference in manifest.receipts.iter().filter(|receipt| {
                receipt.sequence >= reference.first_sequence
                    && receipt.sequence <= reference.last_sequence
            }) {
                let batch_id = BatchId::from_bytes(receipt_reference.batch_id)?;
                let receipt = index.by_batch_id.get(&batch_id)
                    .ok_or(DbError::CompactionPreflight("Capsule source receipt disappeared from the index"))?;
                let record_bytes = read_verified_regular_file(
                    &receipt.path,
                    receipt_reference.record_file_len,
                    receipt_reference.record_file_sha256,
                )?;
                let (_, source_payload) = extract_payload(&record_bytes, definition.id.0)?;
                let source_rows = self.decode_and_resolve_segment(table, definition, &source_payload)?;
                if u64::try_from(source_rows.len()).map_err(|_| DbError::LengthOverflow)?
                    != receipt_reference.row_count
                {
                    return Err(DbError::CompactionPreflight("decoded Chronicle source row count differs from its receipt"));
                }
                for source_row in &source_rows {
                    let capsule_row = resolved_rows.get(capsule_row_offset)
                        .ok_or(DbError::CompactionPreflight("Capsule ended before its Chronicle source rows"))?;
                    let source_fingerprint = canonical_typed_row_fingerprint(
                        &definition.schema,
                        definition.id.0,
                        definition.schema_revision,
                        &definition.column_ids.iter().map(|id| id.0).collect::<Vec<_>>(),
                        source_row,
                    )?;
                    let capsule_fingerprint = canonical_typed_row_fingerprint(
                        &definition.schema,
                        definition.id.0,
                        definition.schema_revision,
                        &definition.column_ids.iter().map(|id| id.0).collect::<Vec<_>>(),
                        capsule_row,
                    )?;
                    if source_fingerprint != capsule_fingerprint {
                        return Err(DbError::CompactionPreflight(
                            "Capsule rows differ from the Chronicle source rows at their declared origins"
                        ));
                    }
                    capsule_row_offset = capsule_row_offset.checked_add(1).ok_or(DbError::LengthOverflow)?;
                }
            }
            if capsule_row_offset != resolved_rows.len() {
                return Err(DbError::CompactionPreflight("Capsule contains rows outside its declared Chronicle source interval"));
            }
            total_source_bytes = total_source_bytes.checked_add(reference.file_len)
                .ok_or(DbError::LengthOverflow)?;
        }

        Ok(CompactionPreflightReport {
            table_id: manifest.table_id,
            capsules_verified: u64::try_from(manifest.capsules.len()).map_err(|_| DbError::LengthOverflow)?,
            chronicle_records_verified: u64::try_from(manifest.receipts.len()).map_err(|_| DbError::LengthOverflow)?,
            total_source_bytes,
            total_row_count: manifest.expected_row_count,
            input_manifest_sha256: manifest.fingerprint()?,
        })
    }

    /// Create a canonical CIM1 snapshot from verified Chronicle receipts and
    /// named immutable Capsule generations. Filesystem paths are derived by the engine.
    pub fn create_compaction_input_manifest(
        &self,
        table: &str,
        job_id: CompactionJobId,
        attempt: u64,
        first_sequence: u64,
        last_sequence: u64,
        capsule_generations: &[(u64, u64)],
    ) -> Result<CompactionInputManifest, DbError> {
        self.ensure_operational()?;
        let definition = self.catalog.tables.get(table)
            .ok_or_else(|| DbError::TableNotFound(table.to_owned()))?;
        let index = self.chronicle_indices.get(&definition.id)
            .ok_or(DbError::CompactionPreflight("table Chronicle receipt index is missing"))?;
        if first_sequence == 0 || last_sequence < first_sequence || last_sequence > index.last_sequence {
            return Err(DbError::CompactionPreflight("requested source sequence interval is invalid"));
        }

        let table_dir = self.root.join("data").join(table);
        let mut receipts = Vec::new();
        let mut expected_row_count = 0_u64;
        for sequence in first_sequence..=last_sequence {
            let receipt = index.by_batch_id.values()
                .find(|receipt| receipt.metadata.sequence == sequence)
                .ok_or(DbError::CompactionPreflight("requested Chronicle sequence is missing"))?;
            if !is_canonical_chronicle_path(&receipt.path, &table_dir) {
                return Err(DbError::CompactionPreflight("Chronicle receipt path is not canonical"));
            }
            let metadata = fs::symlink_metadata(&receipt.path)?;
            if !metadata.file_type().is_file() {
                return Err(DbError::CompactionPreflight("Chronicle receipt is not a regular non-symlink file"));
            }
            let bytes = fs::read(&receipt.path)?;
            let mut cursor = Cursor::new(bytes.as_slice());
            let (decoded, _) = inspect_chronicle_reader(&mut cursor, definition.id.0)?;
            if decoded != receipt.metadata {
                return Err(DbError::ChronicleCorrupt("Chronicle record differs from its receipt index"));
            }
            expected_row_count = expected_row_count.checked_add(decoded.row_count)
                .ok_or(DbError::LengthOverflow)?;
            receipts.push(ChronicleReceiptReference {
                sequence,
                batch_id: *decoded.batch_id.as_bytes(),
                payload_sha256: decoded.fingerprint,
                row_count: decoded.row_count,
                record_file_len: u64::try_from(bytes.len()).map_err(|_| DbError::LengthOverflow)?,
                record_file_sha256: chronicle::sha256(&bytes),
            });
        }

        let capsule_dir = table_dir.join("capsules");
        require_real_directory(&capsule_dir)?;
        let mut capsules = Vec::with_capacity(capsule_generations.len());
        for (capsule_id, generation) in capsule_generations {
            let path = capsule_dir.join(format!("capsule-{capsule_id:020}-g{generation:020}.cap"));
            let metadata = fs::symlink_metadata(&path)?;
            if !metadata.file_type().is_file() {
                return Err(DbError::CompactionPreflight("Capsule path is not a regular non-symlink file"));
            }
            let capsule = self.validate_capsule(table, *capsule_id, *generation)?;
            let bytes = fs::read(&path)?;
            capsules.push(CapsuleSourceReference {
                capsule_id: capsule.capsule_id,
                generation: capsule.generation,
                schema_revision: capsule.schema_revision,
                first_sequence: capsule.first_ingestion_sequence,
                last_sequence: capsule.last_ingestion_sequence,
                row_count: capsule.row_count,
                file_len: u64::try_from(bytes.len()).map_err(|_| DbError::LengthOverflow)?,
                file_sha256: chronicle::sha256(&bytes),
            });
        }

        let manifest = CompactionInputManifest {
            table_id: definition.id.0,
            job_id,
            attempt,
            target_schema_revision: definition.schema_revision,
            first_sequence,
            last_sequence,
            expected_row_count,
            capsules,
            receipts,
        };
        manifest.validate()?;
        Ok(manifest)
    }

    /// Read all rows from one independently validated Capsule.
    ///
    /// This convenience API materialises the selected Capsule's full row set. Use
    /// bounded Chronicle scan APIs for large logical tables; this is primarily useful
    /// for Capsule validation and offline benchmark tooling.
    pub fn scan_capsule(
        &self,
        table: &str,
        capsule_id: u64,
        generation: u64,
    ) -> Result<Vec<Vec<Value>>, DbError> {
        self.ensure_operational()?;
        self.validate_capsule(table, capsule_id, generation)?;
        let definition = self.catalog.tables.get(table)
            .ok_or_else(|| DbError::TableNotFound(table.to_owned()))?;
        let path = self.root.join("data").join(table).join("capsules")
            .join(format!("capsule-{capsule_id:020}-g{generation:020}.cap"));
        let bytes = fs::read(path)?;
        let (_, payload) = decode_capsule(&bytes)?;
        // Resolve the historical Capsule schema into the current logical schema,
        // synthesising NULL for nullable columns added after this Capsule was built.
        self.decode_and_resolve_segment(table, definition, &payload)
    }

    /// Build an immutable Navigation Map generation from all verified Capsules for a table.
    ///
    /// A new generation is published as a separate .nvm file and never overwrites a
    /// previous map generation. The map is an index only; Capsule files remain authoritative.
    pub fn build_navigation_map(
        &mut self,
        table: &str,
        map_generation: u64,
    ) -> Result<PathBuf, DbError> {
        self.ensure_operational()?;
        if map_generation == 0 {
            return Err(DbError::NavigationMapCorrupt("map generation must be nonzero"));
        }
        let definition = self.catalog.tables.get(table)
            .ok_or_else(|| DbError::TableNotFound(table.to_owned()))?;
        let capsule_directory = self.root.join("data").join(table).join("capsules");
        fs::create_dir_all(&capsule_directory)?;
        let capsule_paths = list_capsule_paths(&capsule_directory)?;
        let mut entries = Vec::with_capacity(capsule_paths.len());
        for (capsule_id, generation, path) in capsule_paths {
            let metadata = self.validate_capsule(table, capsule_id, generation)?;
            if metadata.table_id != definition.id.0 {
                return Err(DbError::CapsuleCorrupt("Capsule table identity mismatch while building map"));
            }
            let bytes = fs::read(&path)?;
            let file_len = u64::try_from(bytes.len()).map_err(|_| DbError::LengthOverflow)?;
            entries.push(NavigationMapEntry {
                metadata,
                file_len,
                capsule_sha256: file_sha256(&bytes),
            });
        }
        entries.sort_by_key(|entry| (entry.metadata.capsule_id, entry.metadata.generation));
        let map = NavigationMap {
            table_id: definition.id.0,
            generation: map_generation,
            entries,
        };
        let bytes = map.encode()?;
        let final_path = capsule_directory.join(map_filename(map_generation));
        if final_path.exists() {
            return Err(DbError::Io(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                "immutable Navigation Map generation already exists",
            )));
        }
        let intent = WalMutationIntent {
            artifact_kind: WalArtifactKind::NavigationMap,
            table_id: definition.id.0,
            schema_revision: definition.schema_revision,
            file_id: map_generation,
            file_len: u64::try_from(bytes.len()).map_err(|_| DbError::LengthOverflow)?,
            sha256: chronicle::sha256(&bytes),
        };
        stage_wal_artifact(&capsule_directory, &intent, &bytes)
            .map_err(Self::map_precommit_stage_error)?;
        self.commit_wal_intent(&intent, b"Navigation Map build_navigation_map")?;
        self.publish_wal_intent(&capsule_directory, &intent)
    }

    /// Scan derived Capsules with exact row predicates and optional safe Navigation Map pruning.
    ///
    /// Pass Some(map_generation) to use a previously built map or None for the unpruned
    /// reference path. Both paths use the same row predicates and logical projection.
    pub fn scan_capsules(
        &self,
        table: &str,
        request: &CapsuleScanRequest,
        map_generation: Option<u64>,
    ) -> Result<CapsuleScanResult, DbError> {
        self.ensure_operational()?;
        let definition = self.catalog.tables.get(table)
            .ok_or_else(|| DbError::TableNotFound(table.to_owned()))?;
        let mut resolved_ranges = Vec::with_capacity(request.ranges.len());
        for predicate in &request.ranges {
            let column_index = definition.schema.columns().iter()
                .position(|column| column.name == predicate.column)
                .ok_or_else(|| DbError::ColumnNotFound(predicate.column.clone()))?;
            let data_type = definition.schema.columns()[column_index].data_type;
            for bound in [predicate.lower.as_ref(), predicate.upper.as_ref()].into_iter().flatten() {
                if bound.data_type() != Some(data_type) {
                    return Err(DbError::InvalidRange(format!(
                        "column '{}' expects {}; range bound has a different type",
                        predicate.column, data_type
                    )));
                }
            }
            if predicate.lower.is_none() && predicate.upper.is_none() {
                return Err(DbError::InvalidRange(format!(
                    "column '{}' range requires at least one bound",
                    predicate.column
                )));
            }
            if let (Some(lower), Some(upper)) = (&predicate.lower, &predicate.upper) {
                if capsule::compare_values(lower, upper).is_gt() {
                    return Err(DbError::InvalidRange(format!(
                        "lower bound exceeds upper bound for column '{}'",
                        predicate.column
                    )));
                }
            }
            resolved_ranges.push((column_index, definition.column_ids[column_index].0, predicate));
        }

        let projection = match request.projection.as_ref() {
            Some(names) => {
                let mut indices = Vec::with_capacity(names.len());
                for name in names {
                    let index = definition.schema.columns().iter()
                        .position(|column| column.name == *name)
                        .ok_or_else(|| DbError::ColumnNotFound(name.clone()))?;
                    indices.push(index);
                }
                indices
            }
            None => (0..definition.schema.len()).collect(),
        };

        let capsule_directory = self.root.join("data").join(table).join("capsules");
        let paths = list_capsule_paths(&capsule_directory)?;
        let mut diagnostics = CapsuleScanDiagnostics::default();
        let work: Vec<(u64, u64, Option<NavigationMapEntry>)> = if let Some(map_generation) = map_generation {
            let map_path = capsule_directory.join(map_filename(map_generation));
            let map_bytes = fs::read(&map_path)?;
            let map = NavigationMap::decode(&map_bytes, definition.id.0, map_generation)?;
            let actual_identities = paths.iter().map(|(id, generation, _)| (*id, *generation)).collect::<BTreeSet<_>>();
            let map_identities = map.entries.iter().map(|entry| {
                (entry.metadata.capsule_id, entry.metadata.generation)
            }).collect::<BTreeSet<_>>();
            if actual_identities != map_identities {
                return Err(DbError::NavigationMapStale("Capsule set changed since this map generation was built"));
            }
            let path_lookup = paths.iter().map(|(id, generation, path)| ((*id, *generation), path.clone()))
                .collect::<BTreeMap<_, _>>();
            let mut selected = Vec::with_capacity(map.entries.len());
            for entry in map.entries {
                let identity = (entry.metadata.capsule_id, entry.metadata.generation);
                let path = path_lookup.get(&identity)
                    .ok_or(DbError::NavigationMapStale("mapped Capsule path is missing"))?;
                let bytes = fs::read(path)?;
                diagnostics.bytes_verified = diagnostics.bytes_verified
                    .checked_add(u64::try_from(bytes.len()).map_err(|_| DbError::LengthOverflow)?)
                    .ok_or(DbError::LengthOverflow)?;
                if bytes.len() as u64 != entry.file_len || file_sha256(&bytes) != entry.capsule_sha256 {
                    return Err(DbError::NavigationMapStale("Capsule bytes changed after map publication"));
                }
                selected.push((identity.0, identity.1, Some(entry)));
            }
            selected
        } else {
            paths.into_iter().map(|(id, generation, _)| (id, generation, None)).collect()
        };

        let mut rows = Vec::new();
        for (capsule_id, generation, map_entry) in work {
            diagnostics.capsules_considered = diagnostics.capsules_considered.checked_add(1).ok_or(DbError::LengthOverflow)?;
            if let Some(entry) = map_entry.as_ref() {
                let may_match = resolved_ranges.iter().all(|(_, column_id, predicate)| {
                    let Some(stats) = entry.metadata.columns.iter().find(|stats| stats.column_id == *column_id) else {
                        return true;
                    };
                    let (minimum, maximum) = if entry.metadata.event_time_column_id == Some(*column_id) {
                        match (entry.metadata.temporal_min, entry.metadata.temporal_max) {
                            (Some(min), Some(max)) => (Some(Value::TimestampMicros(min)), Some(Value::TimestampMicros(max))),
                            _ => (None, None),
                        }
                    } else {
                        (stats.minimum.clone(), stats.maximum.clone())
                    };
                    range_may_match(minimum.as_ref(), maximum.as_ref(), predicate)
                });
                if !may_match {
                    diagnostics.capsules_skipped = diagnostics.capsules_skipped.checked_add(1).ok_or(DbError::LengthOverflow)?;
                    continue;
                }
            }

            let capsule_rows = self.scan_capsule(table, capsule_id, generation)?;
            diagnostics.capsules_read = diagnostics.capsules_read.checked_add(1).ok_or(DbError::LengthOverflow)?;
            if let Some(entry) = map_entry.as_ref() {
                diagnostics.payload_bytes_decoded = diagnostics.payload_bytes_decoded
                    .checked_add(entry.metadata.payload_len).ok_or(DbError::LengthOverflow)?;
            } else {
                let metadata = self.validate_capsule(table, capsule_id, generation)?;
                diagnostics.payload_bytes_decoded = diagnostics.payload_bytes_decoded
                    .checked_add(metadata.payload_len).ok_or(DbError::LengthOverflow)?;
            }
            for row in capsule_rows {
                diagnostics.rows_examined = diagnostics.rows_examined.checked_add(1).ok_or(DbError::LengthOverflow)?;
                let matches = resolved_ranges.iter().all(|(index, _, predicate)| {
                    row.get(*index).is_some_and(|value| value_matches(value, predicate))
                });
                if matches {
                    diagnostics.rows_returned = diagnostics.rows_returned.checked_add(1).ok_or(DbError::LengthOverflow)?;
                    rows.push(projection.iter().map(|index| row[*index].clone()).collect());
                }
            }
        }

        Ok(CapsuleScanResult { rows, diagnostics })
    }

    /// Independently validate a Capsule file and recompute every statistic from its ANF payload.
    pub fn validate_capsule(
        &self,
        table: &str,
        capsule_id: u64,
        generation: u64,
    ) -> Result<CapsuleMetadata, DbError> {
        self.ensure_operational()?;
        let definition = self.catalog.tables.get(table)
            .ok_or_else(|| DbError::TableNotFound(table.to_owned()))?;
        if capsule_id == 0 || generation == 0 {
            return Err(DbError::CapsuleCorrupt("Capsule identity and generation must be nonzero"));
        }
        let path = self.root.join("data").join(table).join("capsules")
            .join(format!("capsule-{capsule_id:020}-g{generation:020}.cap"));
        let file_len = fs::metadata(&path)?.len();
        if file_len > capsule::MAX_CAPSULE_BYTES as u64 {
            return Err(DbError::CapsuleCorrupt("Capsule file exceeds size limit"));
        }
        let bytes = fs::read(path)?;
        let (metadata, payload) = decode_capsule(&bytes)?;
        if metadata.table_id != definition.id.0
            || metadata.capsule_id != capsule_id
            || metadata.generation != generation
        {
            return Err(DbError::CapsuleCorrupt("Capsule identity does not match requested table/path"));
        }
        if metadata.schema_revision > definition.schema_revision {
            return Err(DbError::CapsuleCorrupt("Capsule schema revision is newer than the current table"));
        }
        let snapshot = SchemaSnapshot::read_from(&self.root, definition.id, metadata.schema_revision)
            .map_err(|_| DbError::CapsuleCorrupt("Capsule schema snapshot is missing or corrupt"))?;
        let historical_schema = snapshot.schema()?;
        let historical_ids = snapshot.column_ids().map(|id| id.0).collect::<Vec<_>>();
        let rows = decode_segment_v2(
            &payload,
            &historical_schema,
            definition.id.0,
            metadata.schema_revision,
            &historical_ids,
        )?;
        if rows.len() as u64 != metadata.row_count {
            return Err(DbError::CapsuleCorrupt("Capsule row count differs from decoded payload"));
        }
        let inspection = inspect_segment_v2_details(&payload)?;
        let recomputed = derive_column_stats(&historical_schema, &historical_ids, &rows, &inspection)?;
        if !capsule_stats_equal(&metadata.columns, &recomputed) {
            return Err(DbError::CapsuleCorrupt("Capsule column statistics do not match decoded payload"));
        }
        let (temporal_min, temporal_max) = derive_temporal_bounds(&recomputed, metadata.event_time_column_id)?;
        if temporal_min != metadata.temporal_min || temporal_max != metadata.temporal_max {
            return Err(DbError::CapsuleCorrupt("Capsule temporal bounds do not match decoded payload"));
        }
        let index = self.chronicle_indices.get(&definition.id)
            .ok_or(DbError::CapsuleCorrupt("table Chronicle index is missing"))?;
        let available_sequences = index.by_batch_id.values()
            .map(|receipt| receipt.metadata.sequence)
            .collect::<BTreeSet<_>>();
        for sequence in metadata.first_ingestion_sequence..=metadata.last_ingestion_sequence {
            if !available_sequences.contains(&sequence) {
                return Err(DbError::CapsuleCorrupt("Capsule source Chronicle sequence is no longer available"));
            }
        }
        Ok(metadata)
    }

    /// Return independently validated metadata for every canonical Capsule generation.
    /// The returned entries are sorted by their source Chronicle sequence interval.
    pub fn verified_capsule_metadata(&self, table: &str) -> Result<Vec<CapsuleMetadata>, DbError> {
        self.ensure_operational()?;
        let directory = self.root.join("data").join(table).join("capsules");
        match fs::symlink_metadata(&directory) {
            Ok(metadata) if metadata.file_type().is_dir() => {}
            Ok(_) => return Err(DbError::CapsuleCorrupt("Capsule directory is not a real directory")),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(error) => return Err(DbError::Io(error)),
        }
        let paths = list_capsule_paths(&directory)?;
        let mut metadata = Vec::with_capacity(paths.len());
        for (capsule_id, generation, _) in paths {
            metadata.push(self.validate_capsule(table, capsule_id, generation)?);
        }
        metadata.sort_by_key(|item| (
            item.first_ingestion_sequence, item.last_ingestion_sequence,
            item.capsule_id, item.generation,
        ));
        Ok(metadata)
    }

    /// True only when validated, non-overlapping Capsules account for every
    /// accepted Chronicle sequence and every accepted row. This proof is required
    /// before an analytical query may replace the authoritative scan with a
    /// materialised Navigation Map Capsule path.
    pub fn capsules_cover_chronicle(&self, table: &str) -> Result<bool, DbError> {
        self.ensure_operational()?;
        let stats = self.ingestion_stats(table)?;
        if stats.accepted_batches == 0 {
            return Ok(false);
        }

        // Legacy ANF segments may coexist with Chronicle records. Capsule
        // coverage is only complete for a pure Chronicle-backed logical table;
        // never substitute a Capsule scan while standalone ANF rows remain.
        let table_directory = self.root.join("data").join(table);
        for entry in fs::read_dir(&table_directory)? {
            let entry = entry?;
            if !entry.file_type()?.is_file() {
                continue;
            }
            if entry.path().extension().and_then(|extension| extension.to_str()) == Some("anf") {
                return Ok(false);
            }
        }

        let capsules = self.verified_capsule_metadata(table)?;
        if capsules.is_empty() {
            return Ok(false);
        }
        let mut next_sequence = 1_u64;
        let mut total_rows = 0_u64;
        for capsule in capsules {
            if capsule.first_ingestion_sequence != next_sequence
                || capsule.last_ingestion_sequence < capsule.first_ingestion_sequence
            {
                return Ok(false);
            }
            total_rows = total_rows.checked_add(capsule.row_count).ok_or(DbError::LengthOverflow)?;
            next_sequence = capsule.last_ingestion_sequence.checked_add(1).ok_or(DbError::LengthOverflow)?;
        }
        Ok(next_sequence.checked_sub(1) == Some(stats.accepted_batches)
            && total_rows == stats.accepted_rows)
    }

    /// Durable accepted counters are reconstructed from Chronicle receipts on open.
    /// Request/failure counters are process-local and reset after restart.
    pub fn ingestion_stats(&self, table: &str) -> Result<IngestionStats, DbError> {
        self.ensure_operational()?;
        let definition = self.catalog.tables.get(table)
            .ok_or_else(|| DbError::TableNotFound(table.to_owned()))?;
        Ok(self.chronicle_indices.get(&definition.id)
            .map(|index| index.stats.clone())
            .unwrap_or_default())
    }

    pub fn scan(&self, table: &str) -> Result<Vec<Vec<Value>>, DbError> {
        self.ensure_operational()?;
        let schema = self.schema(table)?;
        let projection = (0..schema.len()).collect::<Vec<_>>();
        self.scan_projected_indices(table, &projection)
    }

    /// Return only the requested logical columns, preserving caller order.
    /// ANF v0.1 and v0.2 segments are resolved independently using their stored schema identity.
    pub fn scan_columns(&self, table: &str, columns: &[&str]) -> Result<Vec<Vec<Value>>, DbError> {
        self.ensure_operational()?;
        let schema = self.schema(table)?;
        let mut projection = Vec::with_capacity(columns.len());
        for name in columns {
            let index = schema.columns().iter().position(|column| column.name == *name)
                .ok_or_else(|| DbError::ColumnNotFound((*name).to_owned()))?;
            projection.push(index);
        }
        self.scan_projected_indices(table, &projection)
    }


    /// Scan all logical columns in bounded columnar batches.
    pub fn scan_batches(&self, table: &str, batch_rows: usize) -> Result<ScanBatchIter, DbError> {
        self.ensure_operational()?;
        let schema = self.schema(table)?;
        let projection = (0..schema.len()).collect::<Vec<_>>();
        self.scan_indices_batches(table, projection, batch_rows, None)
    }

    /// Scan selected logical columns in caller order, preserving duplicate projections.
    pub fn scan_columns_batches(
        &self,
        table: &str,
        columns: &[&str],
        batch_rows: usize,
    ) -> Result<ScanBatchIter, DbError> {
        self.ensure_operational()?;
        let schema = self.schema(table)?;
        let mut projection = Vec::with_capacity(columns.len());
        for name in columns {
            let index = schema.columns().iter().position(|column| column.name == *name)
                .ok_or_else(|| DbError::ColumnNotFound((*name).to_owned()))?;
            projection.push(index);
        }
        self.scan_indices_batches(table, projection, batch_rows, None)
    }

    /// Scan all columns with an optional global row limit.
    pub fn scan_batches_with_limit(
        &self,
        table: &str,
        batch_rows: usize,
        row_limit: usize,
    ) -> Result<ScanBatchIter, DbError> {
        self.ensure_operational()?;
        let schema = self.schema(table)?;
        let projection = (0..schema.len()).collect::<Vec<_>>();
        self.scan_indices_batches(table, projection, batch_rows, Some(row_limit))
    }

    /// Projected bounded scan with an optional global row limit.
    pub fn scan_columns_batches_with_limit(
        &self,
        table: &str,
        columns: &[&str],
        batch_rows: usize,
        row_limit: usize,
    ) -> Result<ScanBatchIter, DbError> {
        self.ensure_operational()?;
        let schema = self.schema(table)?;
        let mut projection = Vec::with_capacity(columns.len());
        for name in columns {
            let index = schema.columns().iter().position(|column| column.name == *name)
                .ok_or_else(|| DbError::ColumnNotFound((*name).to_owned()))?;
            projection.push(index);
        }
        self.scan_indices_batches(table, projection, batch_rows, Some(row_limit))
    }

    fn scan_indices_batches(
        &self,
        table: &str,
        projection: Vec<usize>,
        batch_rows: usize,
        row_limit: Option<usize>,
    ) -> Result<ScanBatchIter, DbError> {
        self.ensure_operational()?;
        if batch_rows == 0 || batch_rows > MAX_BATCH_ROWS {
            return Err(DbError::Format(FormatError::LimitExceeded(
                "batch row count must be between 1 and 65536",
            )));
        }
        let definition = self.catalog.tables.get(table)
            .ok_or_else(|| DbError::TableNotFound(table.to_owned()))?;
        if projection.is_empty() {
            return Err(DbError::Format(FormatError::InvalidValue("projection cannot be empty")));
        }
        if let Some(index) = projection.iter().find(|index| **index >= definition.schema.len()) {
            return Err(DbError::Format(FormatError::InvalidProjection(*index)));
        }
        if projection.len() > MAX_BATCH_COLUMNS {
            return Err(DbError::Format(FormatError::LimitExceeded("batch projection column count")));
        }

        let effective_batch_rows = batch_rows.min((MAX_BATCH_CELLS / projection.len()).max(1));
        let segments = if row_limit == Some(0) {
            Vec::new()
        } else {
            self.list_table_scan_paths(table, definition)?
        };
        Ok(ScanBatchIter {
            root: self.root.clone(),
            current: definition.clone(),
            projection,
            segments,
            next_segment: 0,
            batch_rows: effective_batch_rows,
            remaining_limit: row_limit,
            active: None,
            terminal: false,
        })
    }

    fn scan_projected_indices(
        &self,
        table: &str,
        projection: &[usize],
    ) -> Result<Vec<Vec<Value>>, DbError> {
        let definition = self.catalog.tables.get(table)
            .ok_or_else(|| DbError::TableNotFound(table.to_owned()))?;
        if let Some(index) = projection.iter().find(|index| **index >= definition.schema.len()) {
            return Err(DbError::Format(FormatError::InvalidProjection(*index)));
        }

        let segments = self.list_table_scan_paths(table, definition)?;

        let mut rows = Vec::new();
        for segment in segments {
            let mut file = File::open(&segment)?;
            let file_len = file.metadata()?.len();
            let is_chronicle = segment.extension().and_then(|value| value.to_str()) == Some("chr");
            let maximum_file_len = MAX_SEGMENT_BYTES as u64 + if is_chronicle { 108 } else { 0 };
            if file_len > maximum_file_len {
                return Err(DbError::Format(FormatError::LimitExceeded("segment bytes")));
            }
            let capacity = usize::try_from(file_len).map_err(|_| DbError::LengthOverflow)?;
            let mut bytes = Vec::with_capacity(capacity);
            file.read_to_end(&mut bytes)?;
            let segment_bytes = if bytes.starts_with(b"CHR1") {
                extract_payload(&bytes, definition.id.0)?.1
            } else {
                bytes
            };
            let logical_rows = self.decode_and_resolve_segment(table, definition, &segment_bytes)?;
            for row in logical_rows {
                rows.push(projection.iter().map(|index| row[*index].clone()).collect());
            }
        }
        Ok(rows)
    }

    fn decode_and_resolve_segment(
        &self,
        _table: &str,
        current: &TableDefinition,
        bytes: &[u8],
    ) -> Result<Vec<Vec<Value>>, DbError> {
        let version = segment_version(bytes)?;
        let snapshot = match version {
            1 => {
                // ANF v0.1 has no table or schema identity. It is interpreted
                // only against the table's immutable initial schema snapshot.
                SchemaSnapshot::read_from(&self.root, current.id, 1)
                    .map_err(|_| DbError::SchemaCompatibility("ANF v0.1 requires the table's revision-one schema snapshot"))?
            }
            2 => {
                let identity = inspect_segment_v2(bytes)?;
                if identity.table_id != current.id.0 {
                    return Err(DbError::SchemaCompatibility("segment table identity does not match its catalog table"));
                }
                if identity.schema_revision > current.schema_revision {
                    return Err(DbError::SchemaCompatibility("segment schema revision is newer than the current catalog schema"));
                }
                let snapshot = SchemaSnapshot::read_from(&self.root, current.id, identity.schema_revision)
                    .map_err(|_| DbError::SchemaCompatibility("segment schema snapshot is missing or invalid"))?;
                if snapshot.column_ids().map(|id| id.0).collect::<Vec<_>>() != identity.column_ids {
                    return Err(DbError::SchemaCompatibility("segment column identities do not match its schema snapshot"));
                }
                snapshot
            }
            other => return Err(DbError::Format(FormatError::UnsupportedVersion(other))),
        };

        let physical_schema = snapshot.schema()?;
        let physical_rows = match version {
            1 => {
                let projection = (0..physical_schema.len()).collect::<Vec<_>>();
                decode_projected_segment(bytes, &physical_schema, &projection)?
            }
            2 => {
                let identity = inspect_segment_v2(bytes)?;
                decode_segment_v2(
                    bytes,
                    &physical_schema,
                    current.id.0,
                    snapshot.revision(),
                    &identity.column_ids,
                )?
            }
            _ => unreachable!(),
        };

        resolve_rows_by_column_id(&snapshot, current, physical_rows)
    }

}

/// Ensure a revision-one snapshot exists for every table created by older
/// catalog-v2 builds before snapshot publication became part of table creation.
fn ensure_initial_schema_snapshots(root: &Path, catalog: &Catalog) -> Result<(), DbError> {
    for definition in catalog.tables.values() {
        if definition.schema_revision == 1 {
            let snapshot = SchemaSnapshot::new(
                definition.id,
                definition.schema_revision,
                &definition.schema,
                &definition.column_ids,
            )?;
            snapshot.write_to(root)?;
        }
    }
    Ok(())
}

/// Map historical physical rows into the current logical schema by stable ColumnId.
/// Missing historical columns are synthesised as NULL only when the current column is nullable.
fn resolve_rows_by_column_id(
    historical: &SchemaSnapshot,
    current: &TableDefinition,
    physical_rows: Vec<Vec<Value>>,
) -> Result<Vec<Vec<Value>>, DbError> {
    if historical.table_id() != current.id {
        return Err(DbError::SchemaCompatibility("schema snapshot table ID does not match current table"));
    }

    let historical_columns = historical.columns();
    let historical_ids = historical.column_ids().collect::<Vec<_>>();
    let current_columns = current.schema.columns();
    let mut physical_indexes = Vec::with_capacity(current.column_ids.len());

    for (current_id, current_column) in current.column_ids.iter().zip(current_columns) {
        match historical_ids.iter().position(|id| id == current_id) {
            Some(index) => {
                if historical_columns[index].data_type() != current_column.data_type {
                    return Err(DbError::SchemaCompatibility("column data type changed across schema revisions"));
                }
                physical_indexes.push(Some(index));
            }
            None if current_column.nullable => physical_indexes.push(None),
            None => return Err(DbError::SchemaCompatibility("a non-nullable current column is absent from an older segment")),
        }
    }

    let mut logical_rows = Vec::with_capacity(physical_rows.len());
    for physical_row in physical_rows {
        if physical_row.len() != historical_columns.len() {
            return Err(DbError::SchemaCompatibility("decoded row width does not match historical schema"));
        }
        let mut logical_row = Vec::with_capacity(current_columns.len());
        for (index, current_column) in physical_indexes.iter().zip(current_columns) {
            match index {
                Some(index) => logical_row.push(physical_row[*index].clone()),
                None if current_column.nullable => logical_row.push(Value::Null),
                None => return Err(DbError::SchemaCompatibility("cannot synthesise NULL for a non-nullable column")),
            }
        }
        current.schema.validate_row(&logical_row)
            .map_err(|_| DbError::SchemaCompatibility("resolved row violates the current logical schema"))?;
        current.schema.validate_row(&logical_row)
            .map_err(|_| DbError::SchemaCompatibility("resolved row violates the current logical schema"))?;
        logical_rows.push(logical_row);
    }
    Ok(logical_rows)
}




fn canonical_typed_row_fingerprint(
    schema: &Schema,
    table_id: u64,
    schema_revision: u64,
    column_ids: &[u64],
    row: &[Value],
) -> Result<[u8; 32], DbError> {
    schema.validate_row(row)?;
    let bytes = encode_segment_v2(schema, &[row.to_vec()], table_id, schema_revision, column_ids)?;
    Ok(chronicle::sha256(&bytes))
}

fn require_real_directory(path: &Path) -> Result<(), DbError> {
    let metadata = fs::symlink_metadata(path)?;
    if !metadata.file_type().is_dir() {
        return Err(DbError::CompactionPreflight("source directory is missing, not a directory, or is a symlink"));
    }
    Ok(())
}

fn read_verified_regular_file(
    path: &Path,
    expected_len: u64,
    expected_sha256: [u8; 32],
) -> Result<Vec<u8>, DbError> {
    let metadata = fs::symlink_metadata(path)?;
    if !metadata.file_type().is_file() {
        return Err(DbError::CompactionPreflight("source file is not a regular non-symlink file"));
    }
    if metadata.len() != expected_len {
        return Err(DbError::CompactionPreflight("source file length differs from the pinned manifest"));
    }
    let bytes = fs::read(path)?;
    if u64::try_from(bytes.len()).map_err(|_| DbError::LengthOverflow)? != expected_len {
        return Err(DbError::CompactionPreflight("source file length changed during preflight"));
    }
    if chronicle::sha256(&bytes) != expected_sha256 {
        return Err(DbError::CompactionPreflight("source file SHA-256 differs from the pinned manifest"));
    }
    Ok(bytes)
}

fn is_canonical_chronicle_path(path: &Path, table_dir: &Path) -> bool {
    if path.parent() != Some(table_dir) {
        return false;
    }
    let Some(filename) = path.file_name().and_then(|value| value.to_str()) else {
        return false;
    };
    let Some(identifier) = filename.strip_prefix("segment-").and_then(|value| value.strip_suffix(".chr")) else {
        return false;
    };
    if identifier.len() != 20 || !identifier.bytes().all(|byte| byte.is_ascii_digit()) {
        return false;
    }
    let Ok(file_id) = identifier.parse::<u64>() else {
        return false;
    };
    file_id != 0 && format!("segment-{file_id:020}.chr") == filename
}

fn list_capsule_paths(directory: &Path) -> Result<Vec<(u64, u64, PathBuf)>, DbError> {
    let entries = match fs::read_dir(directory) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(DbError::Io(error)),
    };
    let mut paths = Vec::new();
    for entry in entries {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }
        let path = entry.path();
        if path.extension().and_then(|value| value.to_str()) != Some("cap") {
            continue;
        }
        let filename = path.file_name().and_then(|value| value.to_str())
            .ok_or(DbError::CapsuleCorrupt("Capsule filename is not UTF-8"))?;
        let stem = filename.strip_prefix("capsule-").and_then(|value| value.strip_suffix(".cap"))
            .ok_or(DbError::CapsuleCorrupt("noncanonical Capsule filename"))?;
        let (id_text, generation_text) = stem.split_once("-g")
            .ok_or(DbError::CapsuleCorrupt("noncanonical Capsule filename"))?;
        let parse_component = |value: &str| -> Result<u64, DbError> {
            if value.len() != 20 || !value.bytes().all(|byte| byte.is_ascii_digit()) {
                return Err(DbError::CapsuleCorrupt("noncanonical Capsule filename"));
            }
            let parsed = value.parse::<u64>().map_err(|_| DbError::CapsuleCorrupt("invalid Capsule filename number"))?;
            if parsed == 0 || format!("{parsed:020}") != value {
                return Err(DbError::CapsuleCorrupt("noncanonical Capsule filename"));
            }
            Ok(parsed)
        };
        paths.push((parse_component(id_text)?, parse_component(generation_text)?, path));
    }
    paths.sort_by_key(|(id, generation, _)| (*id, *generation));
    if paths.windows(2).any(|pair| (pair[0].0, pair[0].1) == (pair[1].0, pair[1].1)) {
        return Err(DbError::CapsuleCorrupt("duplicate Capsule identity"));
    }
    Ok(paths)
}

fn capsule_values_equal(left: &Option<Value>, right: &Option<Value>) -> bool {
    match (left, right) {
        (None, None) => true,
        (Some(Value::Float64(left)), Some(Value::Float64(right))) => left.to_bits() == right.to_bits(),
        (Some(left), Some(right)) => left == right,
        _ => false,
    }
}

fn capsule_stats_equal(left: &[CapsuleColumnStats], right: &[CapsuleColumnStats]) -> bool {
    left.len() == right.len() && left.iter().zip(right).all(|(left, right)| {
        left.column_id == right.column_id
            && left.data_type == right.data_type
            && left.null_count == right.null_count
            && left.encoded_bytes == right.encoded_bytes
            && capsule_values_equal(&left.minimum, &right.minimum)
            && capsule_values_equal(&left.maximum, &right.maximum)
    })
}

fn validate_name(name: &str) -> Result<(), DbError> {
    if name.is_empty() || name.len() > 128 ||
       !name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-') {
        return Err(DbError::InvalidName(name.to_owned()));
    }
    Ok(())
}

fn sync_recovery_directory(path: &Path) -> Result<(), DbError> {
    #[cfg(unix)]
    {
        File::open(path)?.sync_all()?;
    }
    #[cfg(not(unix))]
    {
        let _ = path;
    }
    Ok(())
}

fn next_segment_id(directory: &Path) -> Result<u64, DbError> {
    let mut max_id = 0_u64;
    for entry in fs::read_dir(directory)? {
        let path = entry?.path();
        if !matches!(path.extension().and_then(|s| s.to_str()), Some("anf" | "chr")) { continue; }
        if let Some(stem) = path.file_stem().and_then(|s| s.to_str()) {
            if let Some(number) = stem.strip_prefix("segment-").and_then(|s| s.parse::<u64>().ok()) {
                max_id = max_id.max(number);
            }
        }
    }
    max_id.checked_add(1).ok_or(DbError::LengthOverflow)
}

fn validate_catalog(catalog: &Catalog) -> Result<(), DbError> {
    if catalog.next_object_id == 0 {
        return Err(DbError::CorruptCatalog("next object ID is zero"));
    }
    if catalog.tables.len() > MAX_CATALOG_ITEMS {
        return Err(DbError::CorruptCatalog("table count exceeds limit"));
    }

    let mut ids = BTreeSet::new();
    let mut max_id = 0_u64;
    let mut object_count = 0_usize;

    for (name, table) in &catalog.tables {
        validate_name(name).map_err(|_| DbError::CorruptCatalog("invalid table name"))?;
        if table.id.0 == 0 || !ids.insert(table.id) {
            return Err(DbError::CorruptCatalog("zero or duplicate object ID"));
        }
        max_id = max_id.max(table.id.0);
        object_count = object_count.checked_add(1).ok_or(DbError::LengthOverflow)?;

        if table.schema_revision == 0 {
            return Err(DbError::CorruptCatalog("schema revision is zero"));
        }
        if table.schema.is_empty() || table.column_ids.len() != table.schema.len() {
            return Err(DbError::CorruptCatalog("column identity count does not match schema"));
        }

        for (column_id, column) in table.column_ids.iter().zip(table.schema.columns()) {
            if column_id.0 == 0 || !ids.insert(*column_id) {
                return Err(DbError::CorruptCatalog("zero or duplicate object ID"));
            }
            if column.name.len() > MAX_NAME_BYTES {
                return Err(DbError::CorruptCatalog("column name exceeds limit"));
            }
            max_id = max_id.max(column_id.0);
            object_count = object_count.checked_add(1).ok_or(DbError::LengthOverflow)?;
        }
    }

    if object_count > MAX_CATALOG_ITEMS {
        return Err(DbError::CorruptCatalog("object count exceeds limit"));
    }
    if catalog.next_object_id <= max_id {
        return Err(DbError::CorruptCatalog("next object ID is not greater than allocated IDs"));
    }
    Ok(())
}

fn cleanup_stale_checkpoint_temp(wal_directory: &Path) -> Result<(), DbError> {
    let path = wal_directory.join(".database.anwl.checkpoint.tmp");
    match fs::symlink_metadata(&path) {
        Ok(metadata) if metadata.file_type().is_file() => {
            fs::remove_file(&path)?;
            sync_recovery_directory(wal_directory)?;
        }
        Ok(_) => return Err(DbError::Wal(
            "WAL checkpoint temporary path is not a regular file".into(),
        )),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(DbError::Io(error)),
    }
    let checkpoints = wal_directory.join("checkpoints");
    match fs::read_dir(&checkpoints) {
        Ok(entries) => {
            let mut removed = false;
            for entry in entries {
                let entry = entry?;
                let name = entry.file_name();
                let text = name.to_string_lossy();
                if !text.starts_with(".checkpoint-") || !text.ends_with(".tmp") {
                    continue;
                }
                let metadata = entry.file_type()?;
                if !metadata.is_file() {
                    return Err(DbError::Wal(
                        "checkpoint staging path is not a regular file".into(),
                    ));
                }
                fs::remove_file(entry.path())?;
                removed = true;
            }
            if removed {
                sync_recovery_directory(&checkpoints)?;
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(DbError::Io(error)),
    }
    Ok(())
}

fn validate_checkpoint_records(
    root: &Path,
    report: &WalReadReport,
    catalog: &Catalog,
) -> Result<(), DbError> {
    let checkpoints = report.records.iter()
        .filter(|record| record.kind == WalRecordKind::Checkpoint)
        .collect::<Vec<_>>();
    if checkpoints.is_empty() {
        return Ok(());
    }
    if checkpoints.len() != 1 || report.records.first().map_or(true, |record| {
        record.kind != WalRecordKind::Checkpoint
    }) || checkpoints[0].transaction_id != 0
    {
        return Err(DbError::Wal(
            "checkpoint must be the unique first frame in a rotated WAL".into(),
        ));
    }
    let plan = build_recovery_plan(report)
        .map_err(|error| DbError::Wal(error.to_string()))?;
    let mut permitted_changed_paths = BTreeSet::new();
    for transaction in &plan.committed {
        for intent in &transaction.intents {
            match intent.artifact_kind {
                WalArtifactKind::CatalogSnapshot => {
                    permitted_changed_paths.insert("catalog/catalog.anc".to_owned());
                }
                WalArtifactKind::ActiveWeavePointer => {
                    let Some((table_name, _)) = catalog.tables.iter()
                        .find(|(_, definition)| definition.id.0 == intent.table_id)
                    else {
                        return Err(DbError::Wal(format!(
                            "checkpoint post-commit pointer has unknown TableId {}",
                            intent.table_id,
                        )));
                    };
                    permitted_changed_paths.insert(format!(
                        "data/{table_name}/weaves/active.awp",
                    ));
                }
                _ => {}
            }
        }
    }
    let checkpoint = crate::wal_checkpoint::validate_checkpoint(
        root,
        &checkpoints[0].payload,
        &permitted_changed_paths,
    ).map_err(|error| DbError::Wal(format!("checkpoint validation failed: {error}")))?;
    if checkpoints[0].sequence != checkpoint.base_sequence.checked_add(1).ok_or(DbError::LengthOverflow)? {
        return Err(DbError::Wal(
            "checkpoint WAL sequence does not follow its declared base sequence".into(),
        ));
    }
    if checkpoint.catalog_revision > catalog.revision {
        return Err(DbError::Wal(
            "checkpoint catalog revision is newer than the loaded database catalog".into(),
        ));
    }
    Ok(())
}

fn collect_storage_inventory(
    root: &Path,
    directory: &Path,
    inventory: &mut StorageInventory,
) -> Result<(), DbError> {
    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        let name = entry.file_name();
        if directory == root && name == ".ananta.lock" {
            continue;
        }
        let path = entry.path();
        let file_type = entry.file_type()?;
        if file_type.is_symlink() {
            return Err(DbError::Wal(format!(
                "storage inventory refuses symbolic link {}",
                path.display(),
            )));
        }
        if file_type.is_dir() {
            if name.to_str().is_some_and(|value| {
                value.starts_with(".stage-") || value.starts_with(".candidate-") || value.ends_with(".tmp")
            }) {
                let relative = path.strip_prefix(root)
                    .map_err(|_| DbError::Wal("temporary directory escaped database root".into()))?
                    .to_str()
                    .ok_or_else(|| DbError::Wal("temporary directory path is not UTF-8".into()))?
                    .replace('\\', "/");
                inventory.temporary_files.push(relative);
            }
            collect_storage_inventory(root, &path, inventory)?;
            continue;
        }
        if !file_type.is_file() {
            return Err(DbError::Wal(format!(
                "storage inventory found unsupported filesystem entry {}",
                path.display(),
            )));
        }
        let relative_path = path.strip_prefix(root)
            .map_err(|_| DbError::Wal("inventory path escaped database root".into()))?
            .to_str()
            .ok_or_else(|| DbError::Wal("database inventory path is not UTF-8".into()))?
            .replace('\\', "/");
        let byte_length = entry.metadata()?.len();
        inventory.total_bytes = inventory.total_bytes.checked_add(byte_length).ok_or(DbError::LengthOverflow)?;
        let extension = path.extension().and_then(|value| value.to_str()).unwrap_or("");
        let artifact_kind = match extension {
            "anf" => "ANF segment",
            "chr" => "Chronicle record",
            "cap" => "Capsule",
            "nvm" => "Navigation Map",
            "anwl" => "ANWL WAL",
            "anc" => "Catalog",
            "ans" => "Schema snapshot",
            "awp" => "Active Weave pointer",
            "owm" => "Compaction output manifest",
            _ => "Other",
        }.to_owned();
        if path.file_name().and_then(|value| value.to_str())
            .is_some_and(|name| name.starts_with('.') && name.ends_with(".tmp"))
        {
            inventory.temporary_files.push(relative_path.clone());
        }
        inventory.files.push(StorageFileInfo {
            relative_path,
            byte_length,
            artifact_kind,
        });
    }
    Ok(())
}

fn encode_catalog_v2(catalog: &Catalog) -> Result<Vec<u8>, DbError> {
    validate_catalog(catalog)?;
    let mut bytes = Vec::new();
    bytes.extend_from_slice(CATALOG_MAGIC);
    bytes.extend_from_slice(&CATALOG_VERSION.to_le_bytes());
    bytes.extend_from_slice(&0_u16.to_le_bytes()); // reserved mandatory feature flags
    bytes.extend_from_slice(&catalog.revision.to_le_bytes());
    bytes.extend_from_slice(&catalog.next_object_id.to_le_bytes());
    bytes.extend_from_slice(&u32::try_from(catalog.tables.len()).map_err(|_| DbError::LengthOverflow)?.to_le_bytes());

    for (name, table) in &catalog.tables {
        bytes.extend_from_slice(&table.id.0.to_le_bytes());
        put_string(&mut bytes, name)?;
        bytes.extend_from_slice(&table.schema_revision.to_le_bytes());
        bytes.extend_from_slice(&u32::try_from(table.schema.len()).map_err(|_| DbError::LengthOverflow)?.to_le_bytes());

        for (column_id, column) in table.column_ids.iter().zip(table.schema.columns()) {
            bytes.extend_from_slice(&column_id.0.to_le_bytes());
            put_string(&mut bytes, &column.name)?;
            bytes.push(column.data_type as u8);
            bytes.push(u8::from(column.nullable));
            if bytes.len() > MAX_CATALOG_BYTES as usize {
                return Err(DbError::LengthOverflow);
            }
        }
    }

    if bytes.len().checked_add(CATALOG_TRAILER_LEN).ok_or(DbError::LengthOverflow)? > MAX_CATALOG_BYTES as usize {
        return Err(DbError::LengthOverflow);
    }
    let checksum = checksum64(&bytes);
    bytes.extend_from_slice(&checksum.to_le_bytes());
    bytes.extend_from_slice(CATALOG_END);

    let decoded = decode_catalog_v2(&bytes)?;
    validate_catalog(&decoded)?;
    Ok(bytes)
}

fn verify_immutable_file_if_present(
    path: &Path,
    expected: &[u8],
    conflict: &'static str,
) -> Result<(), DbError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_file() && metadata.len() == expected.len() as u64 => {
            if fs::read(path)? == expected {
                Ok(())
            } else {
                Err(DbError::CorruptCatalog(conflict))
            }
        }
        Ok(_) => Err(DbError::CorruptCatalog(conflict)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(DbError::Io(error)),
    }
}

fn apply_catalog_snapshot_artifact(
    root: &Path,
    current: &mut Catalog,
    intent: &WalMutationIntent,
    artifact_path: &Path,
) -> Result<(), DbError> {
    if intent.artifact_kind != WalArtifactKind::CatalogSnapshot {
        return Err(DbError::CorruptCatalog("non-catalog artifact passed to catalog recovery"));
    }
    let metadata = fs::metadata(artifact_path)?;
    if metadata.len() > MAX_CATALOG_BYTES {
        return Err(DbError::CorruptCatalog("catalog snapshot exceeds size limit"));
    }
    let capacity = usize::try_from(metadata.len()).map_err(|_| DbError::LengthOverflow)?;
    let mut bytes = Vec::with_capacity(capacity);
    File::open(artifact_path)?.read_to_end(&mut bytes)?;
    let candidate = decode_catalog_v2(&bytes)?;
    validate_catalog(&candidate)?;
    if candidate.revision != intent.file_id {
        return Err(DbError::CorruptCatalog(
            "catalog snapshot revision differs from committed WAL intent",
        ));
    }
    let (table_name, definition) = candidate.tables.iter()
        .find(|(_, definition)| definition.id.0 == intent.table_id)
        .ok_or(DbError::CorruptCatalog(
            "catalog snapshot does not contain the committed TableId",
        ))?;
    if definition.schema_revision != intent.schema_revision {
        return Err(DbError::CorruptCatalog(
            "catalog snapshot schema revision differs from committed WAL intent",
        ));
    }

    // The schema intent is ordered before the catalog intent in the same WAL
    // transaction. It must already have been published and identity-checked.
    let snapshot = SchemaSnapshot::read_from(
        root,
        definition.id,
        definition.schema_revision,
    )?;
    if snapshot.schema()? != definition.schema
        || snapshot.column_ids().collect::<Vec<_>>() != definition.column_ids
    {
        return Err(DbError::SchemaCompatibility(
            "published schema snapshot does not match committed catalog snapshot",
        ));
    }

    let data_root = root.join("data");
    fs::create_dir_all(&data_root)?;
    let table_directory = data_root.join(table_name);
    match fs::symlink_metadata(&table_directory) {
        Ok(metadata) if metadata.file_type().is_dir() => {}
        Ok(_) => {
            return Err(DbError::CorruptCatalog(
                "catalog table path is not a real directory",
            ));
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            fs::create_dir(&table_directory)?;
            sync_recovery_directory(&data_root)?;
        }
        Err(error) => return Err(DbError::Io(error)),
    }
    sync_recovery_directory(&table_directory)?;

    if candidate.revision > current.revision {
        let expected_revision = current.revision
            .checked_add(1)
            .ok_or(DbError::LengthOverflow)?;
        if candidate.revision != expected_revision {
            return Err(DbError::CorruptCatalog(
                "committed catalog revisions are not contiguous",
            ));
        }
        save_catalog(&root.join("catalog").join("catalog.anc"), &candidate)?;
        *current = candidate;
        crate::wal_recovery::maybe_crash_after(
            crate::wal_recovery::WalRecoveryCrashPoint::AfterCanonicalCatalogPublished,
        );
    } else if candidate.revision == current.revision {
        if encode_catalog_v2(current)? != bytes {
            return Err(DbError::CorruptCatalog(
                "canonical catalog conflicts with committed catalog snapshot",
            ));
        }
    } else {
        // Earlier committed snapshots remain in the WAL. A later catalog is
        // acceptable only if it still contains this table and has advanced its
        // schema past this historical revision.
        let current_definition = current.tables.values()
            .find(|definition| definition.id.0 == intent.table_id)
            .ok_or(DbError::CorruptCatalog(
                "current catalog lost a table from committed history",
            ))?;
        if current_definition.schema_revision < intent.schema_revision {
            return Err(DbError::CorruptCatalog(
                "current catalog is behind a committed schema snapshot",
            ));
        }
    }
    Ok(())
}

fn is_canonical_numbered_temp(filename: &str, prefix: &str, suffix: &str) -> bool {
    let Some(id) = filename.strip_prefix(prefix).and_then(|value| value.strip_suffix(suffix)) else {
        return false;
    };
    id.len() == 20
        && id.bytes().all(|byte| byte.is_ascii_digit())
        && id.parse::<u64>().is_ok_and(|value| value != 0 && format!("{value:020}") == id)
}

fn cleanup_stale_catalog_replacement_temp(root: &Path) -> Result<(), DbError> {
    let directory = root.join("catalog");
    let temp = directory.join("catalog.anc.tmp");
    match fs::symlink_metadata(&temp) {
        Ok(metadata) if metadata.file_type().is_file() => {
            fs::remove_file(&temp)?;
            sync_recovery_directory(&directory)?;
        }
        Ok(_) => {
            return Err(DbError::CorruptCatalog(
                "catalog replacement temporary path is not a regular file",
            ));
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(DbError::Io(error)),
    }
    Ok(())
}

fn cleanup_legacy_schema_snapshot_temps(root: &Path) -> Result<(), DbError> {
    let schemas_root = root.join("catalog").join("schemas");
    match fs::read_dir(&schemas_root) {
        Ok(entries) => {
            for entry in entries {
                let entry = entry?;
                if !entry.file_type()?.is_dir() {
                    continue;
                }
                let directory = entry.path();
                let mut changed = false;
                for schema_entry in fs::read_dir(&directory)? {
                    let schema_entry = schema_entry?;
                    if !schema_entry.file_type()?.is_file() {
                        continue;
                    }
                    let filename = schema_entry.file_name();
                    let filename = filename.to_string_lossy();
                    if is_canonical_numbered_temp(&filename, "schema-", ".ans.tmp") {
                        fs::remove_file(schema_entry.path())?;
                        changed = true;
                    }
                }
                if changed {
                    sync_recovery_directory(&directory)?;
                }
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(DbError::Io(error)),
    }
    Ok(())
}

fn cleanup_orphan_catalog_staging_temps(root: &Path) -> Result<(), DbError> {
    let catalog_directory = root.join("catalog");
    let mut catalog_changed = false;
    for entry in fs::read_dir(&catalog_directory)? {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }
        let filename = entry.file_name();
        let filename = filename.to_string_lossy();
        if is_canonical_numbered_temp(&filename, ".catalog-", ".tmp") {
            fs::remove_file(entry.path())?;
            catalog_changed = true;
        }
    }
    if catalog_changed {
        sync_recovery_directory(&catalog_directory)?;
    }

    let schemas_root = catalog_directory.join("schemas");
    match fs::read_dir(&schemas_root) {
        Ok(entries) => {
            for entry in entries {
                let entry = entry?;
                if !entry.file_type()?.is_dir() {
                    continue;
                }
                let schema_directory = entry.path();
                let mut changed = false;
                for schema_entry in fs::read_dir(&schema_directory)? {
                    let schema_entry = schema_entry?;
                    if !schema_entry.file_type()?.is_file() {
                        continue;
                    }
                    let filename = schema_entry.file_name();
                    let filename = filename.to_string_lossy();
                    if is_canonical_numbered_temp(&filename, ".schema-", ".tmp")
                        || is_canonical_numbered_temp(&filename, "schema-", ".ans.tmp")
                    {
                        fs::remove_file(schema_entry.path())?;
                        changed = true;
                    }
                }
                if changed {
                    sync_recovery_directory(&schema_directory)?;
                }
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(DbError::Io(error)),
    }
    Ok(())
}

fn save_catalog(path: &Path, catalog: &Catalog) -> Result<(), DbError> {
    let bytes = encode_catalog_v2(catalog)?;
    let temp = path.with_extension("anc.tmp");
    // Acquire the temporary path before entering the cleanup scope. If it
    // already exists, create_new fails and the pre-existing file is untouched.
    let mut file = OpenOptions::new().write(true).create_new(true).open(&temp)?;
    let write_result = (|| -> Result<(), DbError> {
        file.write_all(&bytes)?;
        file.sync_all()?;
        drop(file);
        crate::wal_recovery::maybe_crash_after(
            crate::wal_recovery::WalRecoveryCrashPoint::AfterCanonicalCatalogTempSynced,
        );
        fs::rename(&temp, path)?;
        crate::wal_recovery::maybe_crash_after(
            crate::wal_recovery::WalRecoveryCrashPoint::AfterCanonicalCatalogRenamedBeforeDirectorySync,
        );
        if let Some(parent) = path.parent() {
            sync_recovery_directory(parent)?;
        }
        Ok(())
    })();
    if write_result.is_err() {
        let _ = fs::remove_file(&temp);
    }
    write_result
}

fn load_catalog(path: &Path) -> Result<Catalog, DbError> {
    let metadata = fs::metadata(path)?;
    if metadata.len() > MAX_CATALOG_BYTES {
        return Err(DbError::CorruptCatalog("catalog exceeds size limit"));
    }
    let mut bytes = Vec::with_capacity(usize::try_from(metadata.len()).map_err(|_| DbError::LengthOverflow)?);
    File::open(path)?.read_to_end(&mut bytes)?;
    if bytes.len() < 6 { return Err(DbError::CorruptCatalog("file is truncated")); }
    if &bytes[..4] != CATALOG_MAGIC { return Err(DbError::CorruptCatalog("invalid magic")); }
    let version = u16::from_le_bytes([bytes[4], bytes[5]]);

    match version {
        CATALOG_V1_VERSION => {
            let legacy = decode_catalog_v1(&bytes)?;
            let migrated = migrate_v1_catalog(legacy)?;
            // v1 bytes remain at the canonical path until the fully encoded,
            // checked and synced v2 temp file is renamed over them.
            save_catalog(path, &migrated)?;
            Ok(migrated)
        }
        CATALOG_VERSION => decode_catalog_v2(&bytes),
        other => Err(DbError::UnsupportedCatalogVersion(other)),
    }
}

fn decode_catalog_v1(bytes: &[u8]) -> Result<BTreeMap<String, Schema>, DbError> {
    if bytes.len() < 22 { return Err(DbError::CorruptCatalog("file is truncated")); }
    let trailer = bytes.len() - CATALOG_TRAILER_LEN;
    validate_catalog_trailer(bytes, trailer)?;

    let mut cursor = 6_usize;
    let table_count = read_u32(bytes, &mut cursor, trailer)? as usize;
    if table_count > MAX_CATALOG_ITEMS || table_count > (trailer - cursor) / 9 {
        return Err(DbError::CorruptCatalog("table count exceeds catalog bounds"));
    }

    let mut tables = BTreeMap::new();
    for _ in 0..table_count {
        let name = read_string(bytes, &mut cursor, trailer)?;
        validate_name(&name).map_err(|_| DbError::CorruptCatalog("invalid table name"))?;
        let column_count = read_u32(bytes, &mut cursor, trailer)? as usize;
        if column_count == 0
            || column_count > MAX_CATALOG_ITEMS
            || column_count > (trailer - cursor) / 7
        {
            return Err(DbError::CorruptCatalog("column count exceeds catalog bounds"));
        }

        let mut columns = Vec::with_capacity(column_count);
        for _ in 0..column_count {
            let column_name = read_string(bytes, &mut cursor, trailer)?;
            let type_code = take(bytes, &mut cursor, trailer, 1)?[0];
            let nullable = decode_nullable(take(bytes, &mut cursor, trailer, 1)?[0])?;
            let data_type = DataType::from_code(type_code)
                .ok_or(DbError::CorruptCatalog("unknown data type"))?;
            columns.push(Column::new(column_name, data_type, nullable));
        }
        let schema = Schema::new(columns).map_err(DbError::Type)?;
        if tables.insert(name, schema).is_some() {
            return Err(DbError::CorruptCatalog("duplicate table name"));
        }
    }
    if cursor != trailer { return Err(DbError::CorruptCatalog("unexpected trailing catalog data")); }
    Ok(tables)
}

fn migrate_v1_catalog(legacy: BTreeMap<String, Schema>) -> Result<Catalog, DbError> {
    let mut catalog = Catalog::empty();
    for (name, schema) in legacy {
        let table_id = allocate_object_id(&mut catalog)?;
        let mut column_ids = Vec::with_capacity(schema.len());
        for _ in schema.columns() {
            column_ids.push(allocate_object_id(&mut catalog)?);
        }
        increment_catalog_revision(&mut catalog)?;
        catalog.tables.insert(name, TableDefinition {
            id: table_id,
            schema_revision: 1,
            column_ids,
            schema,
        });
    }
    validate_catalog(&catalog)?;
    Ok(catalog)
}

fn decode_catalog_v2(bytes: &[u8]) -> Result<Catalog, DbError> {
    if bytes.len() < CATALOG_V2_HEADER_LEN + CATALOG_TRAILER_LEN {
        return Err(DbError::CorruptCatalog("file is truncated"));
    }
    if &bytes[..4] != CATALOG_MAGIC { return Err(DbError::CorruptCatalog("invalid magic")); }
    let version = u16::from_le_bytes([bytes[4], bytes[5]]);
    if version != CATALOG_VERSION { return Err(DbError::UnsupportedCatalogVersion(version)); }
    let flags = u16::from_le_bytes([bytes[6], bytes[7]]);
    if flags != 0 { return Err(DbError::CorruptCatalog("unknown mandatory feature flags")); }

    let trailer = bytes.len() - CATALOG_TRAILER_LEN;
    validate_catalog_trailer(bytes, trailer)?;

    let mut cursor = 8_usize;
    let revision = read_u64(bytes, &mut cursor, trailer)?;
    let next_object_id = read_u64(bytes, &mut cursor, trailer)?;
    let table_count = read_u32(bytes, &mut cursor, trailer)? as usize;
    if table_count > MAX_CATALOG_ITEMS || table_count > (trailer - cursor) / 40 {
        return Err(DbError::CorruptCatalog("table count exceeds catalog bounds"));
    }

    let mut tables = BTreeMap::new();
    for _ in 0..table_count {
        let table_id = ObjectId(read_u64(bytes, &mut cursor, trailer)?);
        let name = read_string(bytes, &mut cursor, trailer)?;
        validate_name(&name).map_err(|_| DbError::CorruptCatalog("invalid table name"))?;
        let schema_revision = read_u64(bytes, &mut cursor, trailer)?;
        let column_count = read_u32(bytes, &mut cursor, trailer)? as usize;
        if column_count == 0
            || column_count > MAX_CATALOG_ITEMS
            || column_count > (trailer - cursor) / 15
        {
            return Err(DbError::CorruptCatalog("column count exceeds catalog bounds"));
        }

        let mut columns = Vec::with_capacity(column_count);
        let mut column_ids = Vec::with_capacity(column_count);
        for _ in 0..column_count {
            column_ids.push(ObjectId(read_u64(bytes, &mut cursor, trailer)?));
            let column_name = read_string(bytes, &mut cursor, trailer)?;
            let type_code = take(bytes, &mut cursor, trailer, 1)?[0];
            let nullable = decode_nullable(take(bytes, &mut cursor, trailer, 1)?[0])?;
            let data_type = DataType::from_code(type_code)
                .ok_or(DbError::CorruptCatalog("unknown data type"))?;
            columns.push(Column::new(column_name, data_type, nullable));
        }
        let schema = Schema::new(columns).map_err(DbError::Type)?;
        let definition = TableDefinition { id: table_id, schema_revision, column_ids, schema };
        if tables.insert(name, definition).is_some() {
            return Err(DbError::CorruptCatalog("duplicate table name"));
        }
    }
    if cursor != trailer { return Err(DbError::CorruptCatalog("unexpected trailing catalog data")); }

    let catalog = Catalog { revision, next_object_id, tables };
    validate_catalog(&catalog)?;
    Ok(catalog)
}

fn validate_catalog_trailer(bytes: &[u8], trailer: usize) -> Result<(), DbError> {
    if trailer + CATALOG_TRAILER_LEN != bytes.len() {
        return Err(DbError::CorruptCatalog("invalid catalog trailer"));
    }
    if &bytes[trailer + 8..] != CATALOG_END {
        return Err(DbError::CorruptCatalog("invalid end marker"));
    }
    if checksum64(&bytes[..trailer]) != read_u64_at(bytes, trailer)? {
        return Err(DbError::ChecksumMismatch);
    }
    Ok(())
}

fn decode_nullable(value: u8) -> Result<bool, DbError> {
    match value {
        0 => Ok(false),
        1 => Ok(true),
        _ => Err(DbError::CorruptCatalog("invalid nullability flag")),
    }
}

fn put_string(out: &mut Vec<u8>, value: &str) -> Result<(), DbError> {
    if value.len() > MAX_NAME_BYTES { return Err(DbError::LengthOverflow); }
    out.extend_from_slice(&u32::try_from(value.len()).map_err(|_| DbError::LengthOverflow)?.to_le_bytes());
    out.extend_from_slice(value.as_bytes());
    Ok(())
}

fn read_string(bytes: &[u8], cursor: &mut usize, end: usize) -> Result<String, DbError> {
    let length = read_u32(bytes, cursor, end)? as usize;
    if length > MAX_NAME_BYTES { return Err(DbError::CorruptCatalog("name length exceeds limit")); }
    let raw = take(bytes, cursor, end, length)?;
    String::from_utf8(raw.to_vec()).map_err(|_| DbError::CorruptCatalog("name is not valid UTF-8"))
}

fn read_u32(bytes: &[u8], cursor: &mut usize, end: usize) -> Result<u32, DbError> {
    let data = take(bytes, cursor, end, 4)?;
    Ok(u32::from_le_bytes(data.try_into().map_err(|_| DbError::CorruptCatalog("truncated integer"))?))
}

fn read_u64(bytes: &[u8], cursor: &mut usize, end: usize) -> Result<u64, DbError> {
    let data = take(bytes, cursor, end, 8)?;
    Ok(u64::from_le_bytes(data.try_into().map_err(|_| DbError::CorruptCatalog("truncated integer"))?))
}

fn read_u64_at(bytes: &[u8], offset: usize) -> Result<u64, DbError> {
    let end = offset.checked_add(8).ok_or(DbError::LengthOverflow)?;
    let slice = bytes.get(offset..end).ok_or(DbError::CorruptCatalog("truncated checksum"))?;
    Ok(u64::from_le_bytes(slice.try_into().map_err(|_| DbError::CorruptCatalog("invalid checksum"))?))
}

fn take<'a>(bytes: &'a [u8], cursor: &mut usize, end: usize, length: usize) -> Result<&'a [u8], DbError> {
    let next = cursor.checked_add(length).ok_or(DbError::LengthOverflow)?;
    if next > end || next > bytes.len() {
        return Err(DbError::CorruptCatalog("field exceeds catalog boundary"));
    }
    let value = &bytes[*cursor..next];
    *cursor = next;
    Ok(value)
}

fn checksum64(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf29ce484222325_u64;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

#[cfg(test)]
mod tests {
    use super::*;
    use ananta_format::encode_segment;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_path() -> PathBuf {
        let nonce = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
        std::env::temp_dir().join(format!("ananta-test-{nonce}"))
    }

    fn sample_schema() -> Schema {
        Schema::new(vec![
            Column::new("timestamp", DataType::TimestampMicros, false),
            Column::new("device", DataType::Utf8, false),
            Column::new("temperature", DataType::Float64, true),
        ]).unwrap()
    }

    #[test]
    fn database_backup_restores_into_a_new_directory_and_replays_wal() {
        let root = temp_path();
        let backup = temp_path();
        let restored = temp_path();
        let rows = vec![
            vec![Value::TimestampMicros(10), Value::Utf8("backup-a".into()), Value::Float64(1.5)],
            vec![Value::TimestampMicros(20), Value::Utf8("backup-b".into()), Value::Float64(2.5)],
        ];
        {
            let mut db = Database::open(&root).unwrap();
            db.create_table("telemetry", sample_schema()).unwrap();
            db.ingest_batch(
                "telemetry",
                BatchId::from_bytes([201; 16]).unwrap(),
                &rows,
            ).unwrap();
            db.build_capsule("telemetry", 3, 1, 1, 1, Some("timestamp")).unwrap();
            db.build_navigation_map("telemetry", 1).unwrap();
            let consistency = db.check_consistency(1).unwrap();
            assert_eq!(consistency.tables_checked, 1);
            assert_eq!(consistency.rows_checked, 2);
            assert_eq!(consistency.chronicle_records_checked, 1);
            assert_eq!(consistency.capsules_checked, 1);
            assert_eq!(consistency.navigation_maps_checked, 1);
            assert!(consistency.inventory.total_bytes > 0);
            let report = db.backup_to(&backup).unwrap();
            assert!(report.file_count >= 7);
            assert!(backup.join("backup.anbk").is_file());
        }

        let restored_report = Database::restore_backup(&backup, &restored).unwrap();
        assert_eq!(restored_report.file_count >= 4, true);
        let db = Database::open(&restored).unwrap();
        assert_eq!(db.scan("telemetry").unwrap(), rows);
        drop(db);

        fs::remove_dir_all(root).unwrap();
        fs::remove_dir_all(backup).unwrap();
        fs::remove_dir_all(restored).unwrap();
    }

    #[test]
    fn writer_lock_inspection_reports_owner_without_mutating_lock() {
        let path = temp_path();
        let db = Database::open(&path).unwrap();
        let status = Database::inspect_lock(&path).unwrap();
        assert!(status.is_locked);
        assert_eq!(status.owner_process_id, Some(std::process::id()));
        assert!(matches!(Database::open(&path), Err(DbError::DatabaseLocked(_))));
        drop(db);
        let unlocked = Database::inspect_lock(&path).unwrap();
        assert!(!unlocked.is_locked);
        assert_eq!(unlocked.owner_process_id, None);
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn one_database_handle_supports_concurrent_readers() {
        let path = temp_path();
        let mut db = Database::open(&path).unwrap();
        db.create_table("telemetry", sample_schema()).unwrap();
        let rows = vec![
            vec![Value::TimestampMicros(1), Value::Utf8("reader-a".into()), Value::Float64(1.0)],
            vec![Value::TimestampMicros(2), Value::Utf8("reader-b".into()), Value::Float64(2.0)],
        ];
        db.ingest_batch(
            "telemetry",
            BatchId::from_bytes([202; 16]).unwrap(),
            &rows,
        ).unwrap();
        std::thread::scope(|scope| {
            let readers = (0..4)
                .map(|_| scope.spawn(|| db.scan("telemetry").unwrap()))
                .collect::<Vec<_>>();
            for reader in readers {
                assert_eq!(reader.join().unwrap(), rows);
            }
        });
        drop(db);
        fs::remove_dir_all(path).unwrap();
    }

    fn legacy_v1_catalog(tables: &[(&str, &Schema)]) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(CATALOG_MAGIC);
        bytes.extend_from_slice(&CATALOG_V1_VERSION.to_le_bytes());
        bytes.extend_from_slice(&(tables.len() as u32).to_le_bytes());
        for (name, schema) in tables {
            put_string(&mut bytes, name).unwrap();
            bytes.extend_from_slice(&(schema.len() as u32).to_le_bytes());
            for column in schema.columns() {
                put_string(&mut bytes, &column.name).unwrap();
                bytes.push(column.data_type as u8);
                bytes.push(u8::from(column.nullable));
            }
        }
        let checksum = checksum64(&bytes);
        bytes.extend_from_slice(&checksum.to_le_bytes());
        bytes.extend_from_slice(CATALOG_END);
        bytes
    }

    fn read_catalog_v2_fixture() -> Vec<u8> {
        include_str!("../tests/fixtures/catalog-v2-single-table.hex")
            .split_whitespace()
            .map(|byte| u8::from_str_radix(byte, 16).expect("valid catalog hex fixture byte"))
            .collect()
    }

    fn refresh_catalog_checksum(bytes: &mut [u8]) {
        let trailer = bytes.len() - CATALOG_TRAILER_LEN;
        let checksum = checksum64(&bytes[..trailer]);
        bytes[trailer..trailer + 8].copy_from_slice(&checksum.to_le_bytes());
    }

    fn write_catalog(path: &Path, bytes: &[u8]) {
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, bytes).unwrap();
    }

    #[test]
    fn catalog_v2_golden_fixture_is_byte_for_byte_stable() {
        let schema = Schema::new(vec![
            Column::new("x", DataType::Int32, false),
        ]).unwrap();
        let mut tables = BTreeMap::new();
        tables.insert("t".to_owned(), TableDefinition {
            id: ObjectId(1),
            schema_revision: 1,
            column_ids: vec![ObjectId(2)],
            schema,
        });
        let catalog = Catalog { revision: 1, next_object_id: 3, tables };
        let fixture = read_catalog_v2_fixture();

        assert_eq!(encode_catalog_v2(&catalog).unwrap(), fixture);
        let decoded = decode_catalog_v2(&fixture).unwrap();
        assert_eq!(decoded.revision, 1);
        assert_eq!(decoded.next_object_id, 3);
        assert_eq!(decoded.tables["t"].id, ObjectId(1));
        assert_eq!(decoded.tables["t"].column_ids, vec![ObjectId(2)]);
        assert_eq!(decoded.tables["t"].schema_revision, 1);
    }

    #[test]
    fn catalog_v2_rejects_next_object_id_not_above_allocated_ids() {
        let fixture = read_catalog_v2_fixture();
        let mut bytes = fixture;
        bytes[16..24].copy_from_slice(&2_u64.to_le_bytes());
        refresh_catalog_checksum(&mut bytes);
        assert!(matches!(
            decode_catalog_v2(&bytes),
            Err(DbError::CorruptCatalog("next object ID is not greater than allocated IDs"))
        ));
    }

    #[test]
    fn catalog_v2_rejects_zero_schema_revision() {
        let mut bytes = read_catalog_v2_fixture();
        let table_name_len = u32::from_le_bytes(bytes[36..40].try_into().unwrap()) as usize;
        let schema_revision_offset = 40 + table_name_len;
        bytes[schema_revision_offset..schema_revision_offset + 8]
            .copy_from_slice(&0_u64.to_le_bytes());
        refresh_catalog_checksum(&mut bytes);
        assert!(matches!(
            decode_catalog_v2(&bytes),
            Err(DbError::CorruptCatalog("schema revision is zero"))
        ));
    }

    #[test]
    fn catalog_v2_rejects_unsupported_version() {
        let mut bytes = read_catalog_v2_fixture();
        bytes[4..6].copy_from_slice(&3_u16.to_le_bytes());
        assert!(matches!(
            decode_catalog_v2(&bytes),
            Err(DbError::UnsupportedCatalogVersion(3))
        ));
    }

    #[test]
    fn failed_table_creation_does_not_advance_in_memory_catalog() {
        let path = temp_path();
        let mut db = Database::open(&path).unwrap();
        fs::write(path.join("catalog").join("catalog.anc.tmp"), b"reserved").unwrap();

        assert!(matches!(
            db.create_table("telemetry", sample_schema()),
            Err(DbError::Io(_))
        ));
        assert_eq!(db.catalog_revision(), 0);
        assert_eq!(db.table_names().count(), 0);
        assert_eq!(fs::read(path.join("catalog").join("catalog.anc.tmp")).unwrap(), b"reserved");

        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn database_persists_immutable_schema_snapshot_without_catalog_mutation() {
        let path = temp_path();
        let mut db = Database::open(&path).unwrap();
        db.create_table("telemetry", sample_schema()).unwrap();
        let revision = db.catalog_revision();
        let table_id = db.table_id("telemetry").unwrap();
        let column_ids = ["timestamp", "device", "temperature"]
            .map(|name| db.column_id("telemetry", name).unwrap());

        let snapshot_path = db.write_schema_snapshot("telemetry").unwrap();
        assert!(snapshot_path.exists());
        assert_eq!(db.catalog_revision(), revision);

        let snapshot = db.read_schema_snapshot("telemetry", 1).unwrap();
        assert_eq!(snapshot.table_id(), table_id);
        assert_eq!(snapshot.revision(), 1);
        assert_eq!(snapshot.column_ids().collect::<Vec<_>>(), column_ids);
        assert_eq!(snapshot.schema().unwrap(), *db.schema("telemetry").unwrap());

        drop(db);
        let reopened = Database::open(&path).unwrap();
        assert_eq!(reopened.read_schema_snapshot("telemetry", 1).unwrap(), snapshot);
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn snapshot_consistency_reports_missing_current_snapshot() {
        let path = temp_path();
        let mut db = Database::open(&path).unwrap();
        db.create_table("telemetry", sample_schema()).unwrap();

        let snapshot_path = SchemaSnapshot::path(&path, db.table_id("telemetry").unwrap(), 1).unwrap();
        fs::remove_file(snapshot_path).unwrap();
        let report = db.check_schema_snapshot_consistency().unwrap();
        assert_eq!(report.missing_current, vec![(db.table_id("telemetry").unwrap(), 1)]);
        assert!(report.invalid_files.is_empty());
        assert!(report.orphaned_files.is_empty());
        assert!(!report.is_clean());

        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn snapshot_consistency_is_clean_after_current_snapshot_is_written() {
        let path = temp_path();
        let mut db = Database::open(&path).unwrap();
        db.create_table("telemetry", sample_schema()).unwrap();
        db.write_schema_snapshot("telemetry").unwrap();

        let report = db.check_schema_snapshot_consistency().unwrap();
        assert!(report.is_clean(), "{report:?}");

        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn snapshot_consistency_reports_orphaned_snapshot_and_temp_file() {
        let path = temp_path();
        let db = Database::open(&path).unwrap();
        let schema = Schema::new(vec![
            Column::new("value", DataType::Int32, false),
        ]).unwrap();
        let orphan = SchemaSnapshot::new(ObjectId(999), 1, &schema, &[ObjectId(1000)]).unwrap();
        let orphan_path = orphan.write_to(&path).unwrap();
        let temp_path = orphan_path.with_extension("ans.tmp");
        fs::write(&temp_path, b"unfinished snapshot").unwrap();

        let report = db.check_schema_snapshot_consistency().unwrap();
        assert!(report.orphaned_files.contains(&orphan_path));
        assert!(report.orphaned_files.contains(&temp_path));
        assert!(report.invalid_files.is_empty());

        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn snapshot_consistency_reports_corrupt_snapshot_file() {
        let path = temp_path();
        let mut db = Database::open(&path).unwrap();
        db.create_table("telemetry", sample_schema()).unwrap();
        let table_id = db.table_id("telemetry").unwrap();
        let snapshot_path = SchemaSnapshot::path(&path, table_id, 1).unwrap();
        fs::create_dir_all(snapshot_path.parent().unwrap()).unwrap();
        fs::write(&snapshot_path, b"broken snapshot").unwrap();

        let report = db.check_schema_snapshot_consistency().unwrap();
        assert!(report.invalid_files.contains(&snapshot_path));
        assert_eq!(report.missing_current, vec![(table_id, 1)]);

        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn new_inserts_use_anf_v0_2_and_existing_v0_1_segments_remain_readable() {
        let path = temp_path();
        let mut db = Database::open(&path).unwrap();
        let schema_v1 = sample_schema();
        db.create_table("telemetry", schema_v1.clone()).unwrap();

        let old_rows = vec![vec![
            Value::TimestampMicros(10),
            Value::Utf8("old-sensor".into()),
            Value::Float64(20.0),
        ]];
        let legacy_bytes = encode_segment(&schema_v1, &old_rows).unwrap();
        let table_dir = path.join("data").join("telemetry");
        fs::write(table_dir.join("segment-00000000000000000001.anf"), legacy_bytes).unwrap();

        let mut candidate = db.catalog.clone();
        // Allocate in the candidate, not in the live catalog, then publish
        // the revised snapshot before updating the catalog pointer.
        let new_column_id = allocate_object_id(&mut candidate).unwrap();
        let definition = candidate.tables.get_mut("telemetry").unwrap();
        let schema_v2 = Schema::new(vec![
            Column::new("timestamp", DataType::TimestampMicros, false),
            Column::new("device", DataType::Utf8, false),
            Column::new("temperature", DataType::Float64, true),
            Column::new("humidity", DataType::Float64, true),
        ]).unwrap();
        definition.schema = schema_v2;
        definition.column_ids.push(new_column_id);
        definition.schema_revision = 2;
        increment_catalog_revision(&mut candidate).unwrap();

        let revised = candidate.tables.get("telemetry").unwrap();
        SchemaSnapshot::new(
            revised.id,
            revised.schema_revision,
            &revised.schema,
            &revised.column_ids,
        ).unwrap().write_to(&path).unwrap();
        save_catalog(&path.join("catalog").join("catalog.anc"), &candidate).unwrap();
        db.catalog = candidate;

        let new_rows = vec![vec![
            Value::TimestampMicros(20),
            Value::Utf8("new-sensor".into()),
            Value::Float64(22.5),
            Value::Float64(55.0),
        ]];
        let new_segment = db.insert_batch("telemetry", &new_rows).unwrap().unwrap();
        let bytes = fs::read(&new_segment).unwrap();
        assert_eq!(segment_version(&bytes).unwrap(), 2);

        let expected = vec![
            vec![
                Value::TimestampMicros(10),
                Value::Utf8("old-sensor".into()),
                Value::Float64(20.0),
                Value::Null,
            ],
            new_rows[0].clone(),
        ];
        assert_eq!(db.scan("telemetry").unwrap(), expected);
        assert_eq!(
            db.scan_columns("telemetry", &["humidity", "device"]).unwrap(),
            vec![
                vec![Value::Null, Value::Utf8("old-sensor".into())],
                vec![Value::Float64(55.0), Value::Utf8("new-sensor".into())],
            ]
        );

        drop(db);
        let reopened = Database::open(&path).unwrap();
        assert_eq!(reopened.scan("telemetry").unwrap(), expected);
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn mixed_anf_v0_1_and_v0_2_segments_resolve_by_column_id() {
        let path = temp_path();
        let mut db = Database::open(&path).unwrap();
        let schema_v1 = sample_schema();
        db.create_table("telemetry", schema_v1.clone()).unwrap();

        let old_rows = vec![vec![
            Value::TimestampMicros(10),
            Value::Utf8("old-sensor".into()),
            Value::Float64(20.0),
        ]];
        let legacy_bytes = encode_segment(&schema_v1, &old_rows).unwrap();
        let table_dir = path.join("data").join("telemetry");
        fs::write(
            table_dir.join("segment-00000000000000000001.anf"),
            legacy_bytes,
        ).unwrap();

        let mut candidate = db.catalog.clone();
        let new_column_id = allocate_object_id(&mut candidate).unwrap();
        let schema_v2 = Schema::new(vec![
            Column::new("timestamp", DataType::TimestampMicros, false),
            Column::new("device", DataType::Utf8, false),
            Column::new("temperature", DataType::Float64, true),
            Column::new("humidity", DataType::Float64, true),
        ]).unwrap();
        let definition = candidate.tables.get_mut("telemetry").unwrap();
        definition.schema = schema_v2;
        definition.column_ids.push(new_column_id);
        definition.schema_revision = 2;
        increment_catalog_revision(&mut candidate).unwrap();

        let revised = candidate.tables.get("telemetry").unwrap();
        SchemaSnapshot::new(
            revised.id,
            revised.schema_revision,
            &revised.schema,
            &revised.column_ids,
        ).unwrap().write_to(&path).unwrap();
        save_catalog(&path.join("catalog").join("catalog.anc"), &candidate).unwrap();
        db.catalog = candidate;

        let new_rows = vec![vec![
            Value::TimestampMicros(20),
            Value::Utf8("new-sensor".into()),
            Value::Float64(22.5),
            Value::Float64(55.0),
        ]];
        let new_segment = db.insert_batch("telemetry", &new_rows).unwrap().unwrap();
        assert_eq!(segment_version(&fs::read(new_segment).unwrap()).unwrap(), 2);

        let expected = vec![
            vec![
                Value::TimestampMicros(10),
                Value::Utf8("old-sensor".into()),
                Value::Float64(20.0),
                Value::Null,
            ],
            new_rows[0].clone(),
        ];
        assert_eq!(db.scan("telemetry").unwrap(), expected);
        assert_eq!(
            db.scan_columns("telemetry", &["humidity", "device"]).unwrap(),
            vec![
                vec![Value::Null, Value::Utf8("old-sensor".into())],
                vec![Value::Float64(55.0), Value::Utf8("new-sensor".into())],
            ]
        );

        drop(db);
        let reopened = Database::open(&path).unwrap();
        assert_eq!(reopened.scan("telemetry").unwrap(), expected);
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn add_nullable_column_supports_mixed_segments_and_reopen() {
        let path = temp_path();
        let mut db = Database::open(&path).unwrap();
        let original_schema = sample_schema();
        db.create_table("telemetry", original_schema.clone()).unwrap();

        let table_id = db.table_id("telemetry").unwrap();
        let original_ids = ["timestamp", "device", "temperature"]
            .map(|name| db.column_id("telemetry", name).unwrap());

        // ANF v0.1 has no schema identity and must continue to use revision one.
        let v0_1_rows = vec![vec![
            Value::TimestampMicros(10),
            Value::Utf8("legacy-v0.1".into()),
            Value::Float64(20.0),
        ]];
        let table_dir = path.join("data").join("telemetry");
        fs::write(
            table_dir.join("segment-00000000000000000001.anf"),
            encode_segment(&original_schema, &v0_1_rows).unwrap(),
        ).unwrap();

        // This ANF v0.2 segment captures the old revision before DDL.
        let v0_2_old_rows = vec![vec![
            Value::TimestampMicros(20),
            Value::Utf8("legacy-v0.2".into()),
            Value::Float64(21.0),
        ]];
        db.insert_batch("telemetry", &v0_2_old_rows).unwrap();

        let new_column_id = db.add_nullable_column(
            "telemetry",
            "humidity",
            DataType::Float64,
        ).unwrap();
        assert!(new_column_id.0 > original_ids.iter().map(|id| id.0).max().unwrap());
        assert_eq!(db.table_id("telemetry").unwrap(), table_id);
        for (name, original_id) in ["timestamp", "device", "temperature"]
            .into_iter().zip(original_ids)
        {
            assert_eq!(db.column_id("telemetry", name).unwrap(), original_id);
        }
        assert_eq!(db.column_id("telemetry", "humidity").unwrap(), new_column_id);
        assert_eq!(db.table_schema_revision("telemetry").unwrap(), 2);
        assert_eq!(db.catalog_revision(), 2);
        assert_eq!(db.schema("telemetry").unwrap().len(), 4);
        assert!(db.schema("telemetry").unwrap().columns()[3].nullable);

        // New writes use revision two and store the new column identity.
        let v0_2_new_rows = vec![vec![
            Value::TimestampMicros(30),
            Value::Utf8("new-v0.2".into()),
            Value::Float64(22.0),
            Value::Float64(48.5),
        ]];
        let new_segment = db.insert_batch("telemetry", &v0_2_new_rows).unwrap().unwrap();
        let new_bytes = fs::read(&new_segment).unwrap();
        assert_eq!(segment_version(&new_bytes).unwrap(), 2);
        let identity = inspect_segment_v2(&new_bytes).unwrap();
        assert_eq!(identity.table_id, table_id.0);
        assert_eq!(identity.schema_revision, 2);
        assert_eq!(identity.column_ids.last(), Some(&new_column_id.0));

        let expected = vec![
            vec![
                Value::TimestampMicros(10),
                Value::Utf8("legacy-v0.1".into()),
                Value::Float64(20.0),
                Value::Null,
            ],
            vec![
                Value::TimestampMicros(20),
                Value::Utf8("legacy-v0.2".into()),
                Value::Float64(21.0),
                Value::Null,
            ],
            v0_2_new_rows[0].clone(),
        ];
        assert_eq!(db.scan("telemetry").unwrap(), expected);
        assert_eq!(
            db.scan_columns("telemetry", &["humidity", "device"]).unwrap(),
            vec![
                vec![Value::Null, Value::Utf8("legacy-v0.1".into())],
                vec![Value::Null, Value::Utf8("legacy-v0.2".into())],
                vec![Value::Float64(48.5), Value::Utf8("new-v0.2".into())],
            ],
        );
        assert!(db.check_schema_snapshot_consistency().unwrap().is_clean());

        drop(db);
        let reopened = Database::open(&path).unwrap();
        assert_eq!(reopened.scan("telemetry").unwrap(), expected);
        assert_eq!(reopened.table_schema_revision("telemetry").unwrap(), 2);
        assert_eq!(reopened.catalog_revision(), 2);
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn add_nullable_column_to_empty_table_and_rejects_invalid_or_duplicate_names() {
        let path = temp_path();
        let mut db = Database::open(&path).unwrap();
        db.create_table("empty_table", sample_schema()).unwrap();

        let id = db.add_nullable_column("empty_table", "humidity", DataType::Float64).unwrap();
        assert_eq!(db.column_id("empty_table", "humidity").unwrap(), id);
        assert_eq!(db.schema("empty_table").unwrap().len(), 4);
        assert!(db.scan("empty_table").unwrap().is_empty());

        assert!(matches!(
            db.add_nullable_column("empty_table", "HUMIDITY", DataType::Float64),
            Err(DbError::ColumnExists(name)) if name == "HUMIDITY"
        ));
        assert!(matches!(
            db.add_nullable_column("empty_table", "   ", DataType::Float64),
            Err(DbError::Type(TypeError::InvalidSchema(_)))
        ));
        assert!(matches!(
            db.add_nullable_column("missing_table", "humidity", DataType::Float64),
            Err(DbError::TableNotFound(_))
        ));
        assert_eq!(db.table_schema_revision("empty_table").unwrap(), 2);
        assert_eq!(db.catalog_revision(), 2);
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn nullable_column_rejects_missing_or_corrupt_current_snapshot() {
        for corrupt in [false, true] {
            let path = temp_path();
            let mut db = Database::open(&path).unwrap();
            db.create_table("telemetry", sample_schema()).unwrap();
            let table_id = db.table_id("telemetry").unwrap();
            let snapshot_path = SchemaSnapshot::path(&path, table_id, 1).unwrap();
            if corrupt {
                fs::write(&snapshot_path, b"corrupt snapshot").unwrap();
            } else {
                fs::remove_file(&snapshot_path).unwrap();
            }

            assert!(matches!(
                db.add_nullable_column("telemetry", "humidity", DataType::Float64),
                Err(DbError::SchemaCompatibility(_))
            ));
            assert_eq!(db.table_schema_revision("telemetry").unwrap(), 1);
            assert_eq!(db.catalog_revision(), 1);
            assert_eq!(db.schema("telemetry").unwrap().len(), 3);
            fs::remove_dir_all(path).unwrap();
        }
    }

    #[test]
    fn nullable_column_catalog_temp_collision_fails_before_commit_without_orphan() {
        let path = temp_path();
        let mut db = Database::open(&path).unwrap();
        db.create_table("telemetry", sample_schema()).unwrap();
        let table_id = db.table_id("telemetry").unwrap();
        let next_snapshot = SchemaSnapshot::path(&path, table_id, 2).unwrap();
        let catalog_temp = path.join("catalog").join("catalog.anc.tmp");
        fs::write(&catalog_temp, b"pre-existing temporary catalog").unwrap();

        assert!(matches!(
            db.add_nullable_column("telemetry", "humidity", DataType::Float64),
            Err(DbError::Io(_))
        ));
        assert_eq!(db.table_schema_revision("telemetry").unwrap(), 1);
        assert_eq!(db.catalog_revision(), 1);
        assert_eq!(db.schema("telemetry").unwrap().len(), 3);
        assert!(!next_snapshot.exists());
        let report = db.check_schema_snapshot_consistency().unwrap();
        assert!(report.is_clean());

        drop(db);
        fs::remove_file(catalog_temp).unwrap();
        let reopened = Database::open(&path).unwrap();
        assert_eq!(reopened.table_schema_revision("telemetry").unwrap(), 1);
        assert_eq!(reopened.catalog_revision(), 1);
        assert_eq!(reopened.schema("telemetry").unwrap().len(), 3);
        assert!(reopened.check_schema_snapshot_consistency().unwrap().is_clean());
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn nullable_column_retry_after_catalog_temp_collision() {
        let path = temp_path();
        let mut db = Database::open(&path).unwrap();
        db.create_table("telemetry", sample_schema()).unwrap();
        let temp = path.join("catalog").join("catalog.anc.tmp");
        fs::write(&temp, b"block publication").unwrap();

        assert!(db.add_nullable_column("telemetry", "humidity", DataType::Float64).is_err());
        assert_eq!(db.table_schema_revision("telemetry").unwrap(), 1);
        fs::remove_file(temp).unwrap();

        let id = db.add_nullable_column("telemetry", "humidity", DataType::Float64).unwrap();
        assert_eq!(db.column_id("telemetry", "humidity").unwrap(), id);
        assert_eq!(db.table_schema_revision("telemetry").unwrap(), 2);
        assert_eq!(db.catalog_revision(), 2);
        assert!(db.check_schema_snapshot_consistency().unwrap().is_clean());
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn catalog_wal_recovery_discards_uncommitted_ddl_staging_and_intents() {
        use crate::wal_recovery::WalRecoveryCrashPoint::*;

        for point in [
            AfterStagingFileCreated,
            AfterStagingBytesWritten,
            AfterStagingFlush,
            AfterStagingFileSync,
            AfterStagingDirectorySync,
            AfterWalBegin,
            AfterWalMutation,
        ] {
            let path = temp_path();
            let mut database = Database::open(&path).unwrap();
            crate::wal_recovery::arm_wal_recovery_crash_point(point);
            let interruption = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                database.create_table("telemetry", sample_schema())
            }));
            assert!(interruption.is_err(), "DDL pre-COMMIT checkpoint {point:?} was not reached");
            drop(database);

            let recovered = Database::open(&path).unwrap();
            assert_eq!(recovered.catalog_revision(), 0, "at {point:?}");
            assert!(matches!(
                recovered.table_id("telemetry"),
                Err(DbError::TableNotFound(_))
            ));
            drop(recovered);

            let catalog_directory = path.join("catalog");
            let mut stack = vec![catalog_directory.clone()];
            while let Some(directory) = stack.pop() {
                for entry in fs::read_dir(&directory).unwrap() {
                    let entry = entry.unwrap();
                    if entry.file_type().unwrap().is_dir() {
                        stack.push(entry.path());
                    } else {
                        assert!(
                            !entry.file_name().to_string_lossy().ends_with(".tmp"),
                            "orphan DDL staging file remained after {point:?}: {}",
                            entry.path().display(),
                        );
                    }
                }
            }

            let reopened = Database::open(&path).unwrap();
            assert_eq!(reopened.catalog_revision(), 0);
            assert!(matches!(
                reopened.table_id("telemetry"),
                Err(DbError::TableNotFound(_))
            ));
            drop(reopened);
            fs::remove_dir_all(path).unwrap();
        }
    }

    #[test]
    fn catalog_wal_recovery_replays_create_table_at_every_ddl_publication_boundary() {
        use crate::wal_recovery::WalRecoveryCrashPoint::*;

        for point in [
            AfterWalCommit,
            AfterSchemaSnapshotPublished,
            AfterCatalogSnapshotPublished,
            AfterCanonicalCatalogTempSynced,
            AfterCanonicalCatalogRenamedBeforeDirectorySync,
            AfterCanonicalCatalogPublished,
        ] {
            let path = temp_path();
            let mut database = Database::open(&path).unwrap();
            crate::wal_recovery::arm_wal_recovery_crash_point(point);
            let interruption = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                database.create_table("telemetry", sample_schema())
            }));
            assert!(interruption.is_err(), "DDL checkpoint {point:?} was not reached");
            drop(database);

            let expected_schema = sample_schema();
            let recovered = Database::open(&path).unwrap();
            assert_eq!(recovered.schema("telemetry").unwrap(), &expected_schema, "at {point:?}");
            assert_eq!(recovered.catalog_revision(), 1);
            assert!(recovered.table_id("telemetry").is_ok());
            assert!(recovered.scan("telemetry").unwrap().is_empty());
            assert!(SchemaSnapshot::path(
                &path,
                recovered.table_id("telemetry").unwrap(),
                1,
            ).unwrap().is_file());
            assert!(path.join("catalog").join("catalog-00000000000000000001.anc").is_file());
            assert!(recovered.check_schema_snapshot_consistency().unwrap().is_clean());
            drop(recovered);

            // Every historical catalog mutation must remain idempotent on a
            // second startup after the canonical catalog was recovered.
            let reopened = Database::open(&path).unwrap();
            assert_eq!(reopened.schema("telemetry").unwrap(), &expected_schema);
            assert_eq!(reopened.catalog_revision(), 1);
            assert!(reopened.check_schema_snapshot_consistency().unwrap().is_clean());
            drop(reopened);
            fs::remove_dir_all(path).unwrap();
        }
    }

    #[test]
    fn nullable_column_refuses_to_overwrite_conflicting_orphan_snapshot() {
        let path = temp_path();
        let mut db = Database::open(&path).unwrap();
        db.create_table("telemetry", sample_schema()).unwrap();
        let table_id = db.table_id("telemetry").unwrap();

        let mut conflicting_columns = sample_schema().columns().to_vec();
        conflicting_columns.push(Column::new("other", DataType::Utf8, true));
        let conflicting_schema = Schema::new(conflicting_columns).unwrap();
        let mut conflicting_ids = ["timestamp", "device", "temperature"]
            .map(|name| db.column_id("telemetry", name).unwrap())
            .to_vec();
        conflicting_ids.push(ObjectId(999));
        let conflicting_snapshot = SchemaSnapshot::new(
            table_id,
            2,
            &conflicting_schema,
            &conflicting_ids,
        ).unwrap();
        let conflicting_path = conflicting_snapshot.write_to(&path).unwrap();
        let original_bytes = fs::read(&conflicting_path).unwrap();

        assert!(matches!(
            db.add_nullable_column("telemetry", "humidity", DataType::Float64),
            Err(DbError::CorruptCatalog("immutable schema snapshot conflict"))
        ));
        assert_eq!(fs::read(&conflicting_path).unwrap(), original_bytes);
        assert_eq!(db.table_schema_revision("telemetry").unwrap(), 1);
        assert_eq!(db.catalog_revision(), 1);
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn nullable_column_rejects_snapshot_that_does_not_match_catalog() {
        let path = temp_path();
        let mut db = Database::open(&path).unwrap();
        db.create_table("telemetry", sample_schema()).unwrap();

        let table_id = db.table_id("telemetry").unwrap();
        let column_ids = ["timestamp", "device", "temperature"]
            .map(|name| db.column_id("telemetry", name).unwrap());
        let mismatched_schema = Schema::new(vec![
            Column::new("timestamp", DataType::TimestampMicros, false),
            Column::new("station", DataType::Utf8, false),
            Column::new("temperature", DataType::Float64, true),
        ]).unwrap();
        let mismatched_snapshot = SchemaSnapshot::new(
            table_id,
            1,
            &mismatched_schema,
            &column_ids,
        ).unwrap();
        let snapshot_path = SchemaSnapshot::path(&path, table_id, 1).unwrap();
        fs::remove_file(&snapshot_path).unwrap();
        mismatched_snapshot.write_to(&path).unwrap();

        assert!(matches!(
            db.add_nullable_column("telemetry", "humidity", DataType::Float64),
            Err(DbError::SchemaCompatibility(_))
        ));
        assert_eq!(db.table_schema_revision("telemetry").unwrap(), 1);
        assert_eq!(db.catalog_revision(), 1);
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn nullable_column_rejects_object_and_revision_counter_overflow() {
        // ObjectId allocator overflow.
        let path = temp_path();
        let mut db = Database::open(&path).unwrap();
        db.create_table("telemetry", sample_schema()).unwrap();
        db.catalog.next_object_id = u64::MAX;
        assert!(matches!(
            db.add_nullable_column("telemetry", "humidity", DataType::Float64),
            Err(DbError::LengthOverflow)
        ));
        assert_eq!(db.table_schema_revision("telemetry").unwrap(), 1);
        assert_eq!(db.catalog_revision(), 1);
        fs::remove_dir_all(path).unwrap();

        // Table schema revision overflow with a matching immutable snapshot.
        let path = temp_path();
        let mut db = Database::open(&path).unwrap();
        db.create_table("telemetry", sample_schema()).unwrap();
        let definition = db.catalog.tables.get("telemetry").unwrap().clone();
        SchemaSnapshot::new(
            definition.id,
            u64::MAX,
            &definition.schema,
            &definition.column_ids,
        ).unwrap().write_to(&path).unwrap();
        db.catalog.tables.get_mut("telemetry").unwrap().schema_revision = u64::MAX;
        assert!(matches!(
            db.add_nullable_column("telemetry", "humidity", DataType::Float64),
            Err(DbError::LengthOverflow)
        ));
        assert_eq!(db.table_schema_revision("telemetry").unwrap(), u64::MAX);
        assert_eq!(db.catalog_revision(), 1);
        fs::remove_dir_all(path).unwrap();

        // Database catalog revision overflow occurs before snapshot publication.
        let path = temp_path();
        let mut db = Database::open(&path).unwrap();
        db.create_table("telemetry", sample_schema()).unwrap();
        db.catalog.revision = u64::MAX;
        let table_id = db.table_id("telemetry").unwrap();
        let next_snapshot = SchemaSnapshot::path(&path, table_id, 2).unwrap();
        assert!(matches!(
            db.add_nullable_column("telemetry", "humidity", DataType::Float64),
            Err(DbError::LengthOverflow)
        ));
        assert!(!next_snapshot.exists());
        assert_eq!(db.table_schema_revision("telemetry").unwrap(), 1);
        assert_eq!(db.catalog_revision(), u64::MAX);
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn rename_and_reorder_preserve_ids_and_historical_segment_reads() {
        let path = temp_path();
        let mut db = Database::open(&path).unwrap();
        let original_schema = sample_schema();
        db.create_table("telemetry", original_schema.clone()).unwrap();

        let table_id = db.table_id("telemetry").unwrap();
        let timestamp_id = db.column_id("telemetry", "timestamp").unwrap();
        let device_id = db.column_id("telemetry", "device").unwrap();
        let temperature_id = db.column_id("telemetry", "temperature").unwrap();

        // Legacy v0.1 data resolves via the immutable revision-one snapshot.
        let legacy_v0_1 = vec![vec![
            Value::TimestampMicros(10),
            Value::Utf8("sensor-v0.1".into()),
            Value::Float64(18.5),
        ]];
        let table_dir = path.join("data").join("telemetry");
        fs::write(
            table_dir.join("segment-00000000000000000001.anf"),
            encode_segment(&original_schema, &legacy_v0_1).unwrap(),
        ).unwrap();

        // A v0.2 segment also records the pre-rename/pre-reorder identity.
        let legacy_v0_2 = vec![vec![
            Value::TimestampMicros(20),
            Value::Utf8("sensor-v0.2".into()),
            Value::Float64(19.5),
        ]];
        db.insert_batch("telemetry", &legacy_v0_2).unwrap();

        assert_eq!(
            db.rename_column("telemetry", "device", "sensor").unwrap(),
            device_id,
        );
        assert_eq!(db.column_id("telemetry", "sensor").unwrap(), device_id);
        assert!(matches!(
            db.column_id("telemetry", "device"),
            Err(DbError::ColumnNotFound(_))
        ));
        assert_eq!(db.table_schema_revision("telemetry").unwrap(), 2);
        assert_eq!(db.catalog_revision(), 2);

        db.reorder_columns("telemetry", &["temperature", "timestamp", "sensor"]).unwrap();
        assert_eq!(db.table_schema_revision("telemetry").unwrap(), 3);
        assert_eq!(db.catalog_revision(), 3);
        assert_eq!(
            db.schema("telemetry").unwrap().columns().iter()
                .map(|column| column.name.as_str()).collect::<Vec<_>>(),
            vec!["temperature", "timestamp", "sensor"],
        );
        assert_eq!(db.column_id("telemetry", "timestamp").unwrap(), timestamp_id);
        assert_eq!(db.column_id("telemetry", "sensor").unwrap(), device_id);
        assert_eq!(db.column_id("telemetry", "temperature").unwrap(), temperature_id);

        let new_rows = vec![vec![
            Value::Float64(23.5),
            Value::TimestampMicros(30),
            Value::Utf8("sensor-new".into()),
        ]];
        let new_segment = db.insert_batch("telemetry", &new_rows).unwrap().unwrap();
        let identity = inspect_segment_v2(&fs::read(new_segment).unwrap()).unwrap();
        assert_eq!(identity.table_id, table_id.0);
        assert_eq!(identity.schema_revision, 3);
        assert_eq!(
            identity.column_ids,
            vec![temperature_id.0, timestamp_id.0, device_id.0],
        );

        let expected = vec![
            vec![
                Value::Float64(18.5),
                Value::TimestampMicros(10),
                Value::Utf8("sensor-v0.1".into()),
            ],
            vec![
                Value::Float64(19.5),
                Value::TimestampMicros(20),
                Value::Utf8("sensor-v0.2".into()),
            ],
            new_rows[0].clone(),
        ];
        assert_eq!(db.scan("telemetry").unwrap(), expected);
        assert_eq!(
            db.scan_columns("telemetry", &["sensor", "temperature"]).unwrap(),
            vec![
                vec![Value::Utf8("sensor-v0.1".into()), Value::Float64(18.5)],
                vec![Value::Utf8("sensor-v0.2".into()), Value::Float64(19.5)],
                vec![Value::Utf8("sensor-new".into()), Value::Float64(23.5)],
            ],
        );
        assert!(db.check_schema_snapshot_consistency().unwrap().is_clean());

        drop(db);
        let reopened = Database::open(&path).unwrap();
        assert_eq!(reopened.scan("telemetry").unwrap(), expected);
        assert_eq!(reopened.table_schema_revision("telemetry").unwrap(), 3);
        assert_eq!(reopened.catalog_revision(), 3);
        assert_eq!(reopened.column_id("telemetry", "sensor").unwrap(), device_id);
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn rename_column_validates_names_and_exact_rename_is_a_noop() {
        let path = temp_path();
        let mut db = Database::open(&path).unwrap();
        db.create_table("telemetry", sample_schema()).unwrap();
        let device_id = db.column_id("telemetry", "device").unwrap();

        assert_eq!(db.rename_column("telemetry", "device", "device").unwrap(), device_id);
        assert_eq!(db.table_schema_revision("telemetry").unwrap(), 1);
        assert_eq!(db.catalog_revision(), 1);

        assert!(matches!(
            db.rename_column("telemetry", "device", "TEMPERATURE"),
            Err(DbError::ColumnExists(name)) if name == "TEMPERATURE"
        ));
        assert!(matches!(
            db.rename_column("telemetry", "device", "   "),
            Err(DbError::Type(TypeError::InvalidSchema(_)))
        ));
        assert!(matches!(
            db.rename_column("telemetry", "missing", "renamed"),
            Err(DbError::ColumnNotFound(_))
        ));
        assert!(matches!(
            db.rename_column("missing_table", "device", "renamed"),
            Err(DbError::TableNotFound(_))
        ));
        assert_eq!(db.table_schema_revision("telemetry").unwrap(), 1);
        assert_eq!(db.catalog_revision(), 1);

        // Case-only spelling changes are real renames, but retain the same ID.
        assert_eq!(db.rename_column("telemetry", "device", "Device").unwrap(), device_id);
        assert_eq!(db.column_id("telemetry", "Device").unwrap(), device_id);
        assert_eq!(db.table_schema_revision("telemetry").unwrap(), 2);
        assert_eq!(db.catalog_revision(), 2);
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn reorder_columns_requires_an_exact_complete_permutation() {
        let path = temp_path();
        let mut db = Database::open(&path).unwrap();
        db.create_table("telemetry", sample_schema()).unwrap();

        // The existing order is a no-op.
        db.reorder_columns("telemetry", &["timestamp", "device", "temperature"]).unwrap();
        assert_eq!(db.table_schema_revision("telemetry").unwrap(), 1);
        assert_eq!(db.catalog_revision(), 1);

        assert!(matches!(
            db.reorder_columns("telemetry", &["timestamp", "device"]),
            Err(DbError::InvalidColumnOrder(_))
        ));
        assert!(matches!(
            db.reorder_columns("telemetry", &["timestamp", "device", "device"]),
            Err(DbError::InvalidColumnOrder(_))
        ));
        assert!(matches!(
            db.reorder_columns("telemetry", &["timestamp", "device", "missing"]),
            Err(DbError::ColumnNotFound(_))
        ));
        assert!(matches!(
            db.reorder_columns("telemetry", &["timestamp", "device", "temperature", "extra"]),
            Err(DbError::InvalidColumnOrder(_))
        ));
        assert!(matches!(
            db.reorder_columns("missing_table", &["a"]),
            Err(DbError::TableNotFound(_))
        ));
        assert_eq!(db.table_schema_revision("telemetry").unwrap(), 1);
        assert_eq!(db.catalog_revision(), 1);

        db.reorder_columns("telemetry", &["temperature", "device", "timestamp"]).unwrap();
        assert_eq!(db.table_schema_revision("telemetry").unwrap(), 2);
        assert_eq!(db.catalog_revision(), 2);
        assert_eq!(
            db.schema("telemetry").unwrap().columns().iter()
                .map(|column| column.name.as_str()).collect::<Vec<_>>(),
            vec!["temperature", "device", "timestamp"],
        );
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn rename_and_reorder_reject_missing_corrupt_and_inconsistent_snapshots() {
        for operation in 0..2 {
            for snapshot_state in 0..3 {
                let path = temp_path();
                let mut db = Database::open(&path).unwrap();
                db.create_table("telemetry", sample_schema()).unwrap();
                let table_id = db.table_id("telemetry").unwrap();
                let snapshot_path = SchemaSnapshot::path(&path, table_id, 1).unwrap();

                match snapshot_state {
                    0 => fs::remove_file(&snapshot_path).unwrap(),
                    1 => fs::write(&snapshot_path, b"broken snapshot").unwrap(),
                    _ => {
                        let ids = ["timestamp", "device", "temperature"]
                            .map(|name| db.column_id("telemetry", name).unwrap()).to_vec();
                        let mismatch = Schema::new(vec![
                            Column::new("timestamp", DataType::TimestampMicros, false),
                            Column::new("sensor", DataType::Utf8, false),
                            Column::new("temperature", DataType::Float64, true),
                        ]).unwrap();
                        let bad = SchemaSnapshot::new(table_id, 1, &mismatch, &ids).unwrap();
                        fs::remove_file(&snapshot_path).unwrap();
                        bad.write_to(&path).unwrap();
                    }
                }

                let result = if operation == 0 {
                    db.rename_column("telemetry", "device", "sensor").map(|_| ())
                } else {
                    db.reorder_columns("telemetry", &["temperature", "device", "timestamp"])
                };
                assert!(matches!(result, Err(DbError::SchemaCompatibility(_))));
                assert_eq!(db.table_schema_revision("telemetry").unwrap(), 1);
                assert_eq!(db.catalog_revision(), 1);
                assert_eq!(db.schema("telemetry").unwrap().len(), 3);
                fs::remove_dir_all(path).unwrap();
            }
        }
    }

    #[test]
    fn rename_and_reorder_publication_failures_preserve_old_catalog_and_allow_retry() {
        let path = temp_path();
        let mut db = Database::open(&path).unwrap();
        db.create_table("telemetry", sample_schema()).unwrap();

        let table_id = db.table_id("telemetry").unwrap();
        let rename_orphan = SchemaSnapshot::path(&path, table_id, 2).unwrap();
        let catalog_temp = path.join("catalog").join("catalog.anc.tmp");
        fs::write(&catalog_temp, b"block catalog publish").unwrap();

        assert!(matches!(
            db.rename_column("telemetry", "device", "sensor"),
            Err(DbError::Io(_))
        ));
        assert_eq!(db.schema("telemetry").unwrap().columns()[1].name, "device");
        assert_eq!(db.table_schema_revision("telemetry").unwrap(), 1);
        assert_eq!(db.catalog_revision(), 1);
        assert!(db.check_schema_snapshot_consistency().unwrap().orphaned_files.contains(&rename_orphan));

        fs::remove_file(&catalog_temp).unwrap();
        db.rename_column("telemetry", "device", "sensor").unwrap();
        assert_eq!(db.table_schema_revision("telemetry").unwrap(), 2);
        assert!(db.check_schema_snapshot_consistency().unwrap().is_clean());

        let reorder_orphan = SchemaSnapshot::path(&path, table_id, 3).unwrap();
        fs::write(&catalog_temp, b"block catalog publish").unwrap();
        assert!(matches!(
            db.reorder_columns("telemetry", &["temperature", "timestamp", "sensor"]),
            Err(DbError::Io(_))
        ));
        assert_eq!(
            db.schema("telemetry").unwrap().columns().iter()
                .map(|column| column.name.as_str()).collect::<Vec<_>>(),
            vec!["timestamp", "sensor", "temperature"],
        );
        assert_eq!(db.table_schema_revision("telemetry").unwrap(), 2);
        assert_eq!(db.catalog_revision(), 2);
        assert!(db.check_schema_snapshot_consistency().unwrap().orphaned_files.contains(&reorder_orphan));

        fs::remove_file(&catalog_temp).unwrap();
        db.reorder_columns("telemetry", &["temperature", "timestamp", "sensor"]).unwrap();
        assert_eq!(db.table_schema_revision("telemetry").unwrap(), 3);
        assert_eq!(db.catalog_revision(), 3);
        assert!(db.check_schema_snapshot_consistency().unwrap().is_clean());

        drop(db);
        let reopened = Database::open(&path).unwrap();
        assert_eq!(
            reopened.schema("telemetry").unwrap().columns().iter()
                .map(|column| column.name.as_str()).collect::<Vec<_>>(),
            vec!["temperature", "timestamp", "sensor"],
        );
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn rename_column_refuses_to_overwrite_a_conflicting_orphan_snapshot() {
        let path = temp_path();
        let mut db = Database::open(&path).unwrap();
        db.create_table("telemetry", sample_schema()).unwrap();
        let table_id = db.table_id("telemetry").unwrap();
        let ids = ["timestamp", "device", "temperature"]
            .map(|name| db.column_id("telemetry", name).unwrap());
        let conflicting_schema = Schema::new(vec![
            Column::new("timestamp", DataType::TimestampMicros, false),
            Column::new("other", DataType::Utf8, false),
            Column::new("temperature", DataType::Float64, true),
        ]).unwrap();
        let conflicting = SchemaSnapshot::new(table_id, 2, &conflicting_schema, &ids)
            .unwrap();
        let conflicting_path = conflicting.write_to(&path).unwrap();
        let original_bytes = fs::read(&conflicting_path).unwrap();

        assert!(matches!(
            db.rename_column("telemetry", "device", "sensor"),
            Err(DbError::CorruptCatalog("immutable schema snapshot conflict"))
        ));
        assert_eq!(fs::read(&conflicting_path).unwrap(), original_bytes);
        assert_eq!(db.table_schema_revision("telemetry").unwrap(), 1);
        assert_eq!(db.catalog_revision(), 1);
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn rename_and_reorder_reject_revision_counter_overflow() {
        let path = temp_path();
        let mut db = Database::open(&path).unwrap();
        db.create_table("telemetry", sample_schema()).unwrap();
        let definition = db.catalog.tables.get("telemetry").unwrap().clone();
        SchemaSnapshot::new(
            definition.id,
            u64::MAX,
            &definition.schema,
            &definition.column_ids,
        ).unwrap().write_to(&path).unwrap();
        db.catalog.tables.get_mut("telemetry").unwrap().schema_revision = u64::MAX;
        assert!(matches!(
            db.rename_column("telemetry", "device", "sensor"),
            Err(DbError::LengthOverflow)
        ));
        assert_eq!(db.table_schema_revision("telemetry").unwrap(), u64::MAX);
        fs::remove_dir_all(path).unwrap();

        let path = temp_path();
        let mut db = Database::open(&path).unwrap();
        db.create_table("telemetry", sample_schema()).unwrap();
        db.catalog.revision = u64::MAX;
        let next_snapshot = SchemaSnapshot::path(
            &path,
            db.table_id("telemetry").unwrap(),
            2,
        ).unwrap();
        assert!(matches!(
            db.reorder_columns("telemetry", &["temperature", "device", "timestamp"]),
            Err(DbError::LengthOverflow)
        ));
        assert!(!next_snapshot.exists());
        assert_eq!(db.table_schema_revision("telemetry").unwrap(), 1);
        assert_eq!(db.catalog_revision(), u64::MAX);
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn database_survives_close_and_reopen_with_stable_object_ids() {
        let path = temp_path();
        let rows = vec![
            vec![Value::TimestampMicros(10), Value::Utf8("sensor-1".into()), Value::Float64(21.25)],
            vec![Value::TimestampMicros(20), Value::Utf8("sensor-2".into()), Value::Null],
        ];
        let table_id;
        let column_id;
        {
            let mut db = Database::open(&path).unwrap();
            db.create_table("telemetry", sample_schema()).unwrap();
            db.insert_batch("telemetry", &rows).unwrap();
            table_id = db.table_id("telemetry").unwrap();
            column_id = db.column_id("telemetry", "device").unwrap();
            assert_eq!(db.catalog_revision(), 1);
            assert_eq!(db.table_schema_revision("telemetry").unwrap(), 1);
        }
        {
            let db = Database::open(&path).unwrap();
            assert_eq!(db.scan("telemetry").unwrap(), rows);
            assert_eq!(db.table_id("telemetry").unwrap(), table_id);
            assert_eq!(db.column_id("telemetry", "device").unwrap(), column_id);
            assert_eq!(db.catalog_revision(), 1);
            assert_eq!(db.table_schema_revision("telemetry").unwrap(), 1);
        }
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn object_ids_are_unique_across_tables_and_columns() {
        let path = temp_path();
        let mut db = Database::open(&path).unwrap();
        db.create_table("alpha", sample_schema()).unwrap();
        db.create_table("beta", sample_schema()).unwrap();

        let mut ids = BTreeSet::new();
        for table in ["alpha", "beta"] {
            ids.insert(db.table_id(table).unwrap());
            for column in ["timestamp", "device", "temperature"] {
                ids.insert(db.column_id(table, column).unwrap());
            }
        }
        assert_eq!(ids.len(), 8);
        assert_eq!(db.catalog_revision(), 2);
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn migrates_v1_catalog_and_preserves_existing_data() {
        let path = temp_path();
        let catalog_path = path.join("catalog").join("catalog.anc");
        let schema = sample_schema();
        let legacy = legacy_v1_catalog(&[("telemetry", &schema)]);
        write_catalog(&catalog_path, &legacy);

        let table_dir = path.join("data").join("telemetry");
        fs::create_dir_all(&table_dir).unwrap();
        let rows = vec![vec![
            Value::TimestampMicros(42),
            Value::Utf8("sensor".into()),
            Value::Float64(19.5),
        ]];
        let segment = encode_segment(&schema, &rows).unwrap();
        fs::write(table_dir.join("segment-00000000000000000001.anf"), segment).unwrap();

        let db = Database::open(&path).unwrap();
        assert_eq!(db.scan("telemetry").unwrap(), rows);
        assert_eq!(db.table_id("telemetry").unwrap(), ObjectId(1));
        assert_eq!(db.column_id("telemetry", "timestamp").unwrap(), ObjectId(2));
        assert_eq!(db.column_id("telemetry", "device").unwrap(), ObjectId(3));
        assert_eq!(db.column_id("telemetry", "temperature").unwrap(), ObjectId(4));
        assert_eq!(db.catalog_revision(), 1);
        assert_eq!(db.table_schema_revision("telemetry").unwrap(), 1);

        let migrated = fs::read(&catalog_path).unwrap();
        assert_eq!(u16::from_le_bytes([migrated[4], migrated[5]]), CATALOG_VERSION);
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn v1_migration_failure_leaves_original_catalog_intact() {
        let path = temp_path();
        let catalog_path = path.join("catalog").join("catalog.anc");
        let schema = sample_schema();
        let legacy = legacy_v1_catalog(&[("telemetry", &schema)]);
        write_catalog(&catalog_path, &legacy);
        fs::write(path.join("catalog").join("catalog.anc.tmp"), b"pre-existing temp").unwrap();

        let result = Database::open(&path);
        assert!(matches!(result, Err(DbError::Io(_))));
        assert_eq!(fs::read(&catalog_path).unwrap(), legacy);
        assert_eq!(fs::read(path.join("catalog").join("catalog.anc.tmp")).unwrap(), b"pre-existing temp");
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn rejects_zero_table_object_id() {
        let path = temp_path();
        let mut db = Database::open(&path).unwrap();
        db.create_table("telemetry", sample_schema()).unwrap();
        drop(db);

        let catalog_path = path.join("catalog").join("catalog.anc");
        let mut bytes = fs::read(&catalog_path).unwrap();
        bytes[CATALOG_V2_HEADER_LEN..CATALOG_V2_HEADER_LEN + 8].copy_from_slice(&0_u64.to_le_bytes());
        refresh_catalog_checksum(&mut bytes);
        fs::write(&catalog_path, bytes).unwrap();

        assert!(matches!(
            Database::open(&path),
            Err(DbError::CorruptCatalog("zero or duplicate object ID"))
        ));
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn rejects_duplicate_table_and_column_object_ids() {
        let path = temp_path();
        let mut db = Database::open(&path).unwrap();
        db.create_table("telemetry", sample_schema()).unwrap();
        drop(db);

        let catalog_path = path.join("catalog").join("catalog.anc");
        let mut bytes = fs::read(&catalog_path).unwrap();
        let table_id = bytes[CATALOG_V2_HEADER_LEN..CATALOG_V2_HEADER_LEN + 8].to_vec();
        let table_name_len = u32::from_le_bytes(bytes[CATALOG_V2_HEADER_LEN + 8..CATALOG_V2_HEADER_LEN + 12].try_into().unwrap()) as usize;
        let first_column_id = CATALOG_V2_HEADER_LEN + 8 + 4 + table_name_len + 8 + 4;
        bytes[first_column_id..first_column_id + 8].copy_from_slice(&table_id);
        refresh_catalog_checksum(&mut bytes);
        fs::write(&catalog_path, bytes).unwrap();

        assert!(matches!(
            Database::open(&path),
            Err(DbError::CorruptCatalog("zero or duplicate object ID"))
        ));
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn rejects_catalog_v2_unknown_feature_flags() {
        let path = temp_path();
        let mut db = Database::open(&path).unwrap();
        db.create_table("telemetry", sample_schema()).unwrap();
        drop(db);

        let catalog_path = path.join("catalog").join("catalog.anc");
        let mut bytes = fs::read(&catalog_path).unwrap();
        bytes[6..8].copy_from_slice(&1_u16.to_le_bytes());
        refresh_catalog_checksum(&mut bytes);
        fs::write(&catalog_path, bytes).unwrap();

        assert!(matches!(
            Database::open(&path),
            Err(DbError::CorruptCatalog("unknown mandatory feature flags"))
        ));
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn catalog_counter_overflow_is_rejected() {
        let mut catalog = Catalog::empty();
        catalog.next_object_id = u64::MAX;
        assert!(matches!(allocate_object_id(&mut catalog), Err(DbError::LengthOverflow)));

        catalog.revision = u64::MAX;
        assert!(matches!(increment_catalog_revision(&mut catalog), Err(DbError::LengthOverflow)));
    }

    #[test]
    fn rejects_invalid_table_names_and_rows() {
        let path = temp_path();
        let mut db = Database::open(&path).unwrap();
        assert!(db.create_table("../outside", sample_schema()).is_err());
        db.create_table("telemetry", sample_schema()).unwrap();
        assert!(db.insert_batch("telemetry", &[vec![Value::Int64(1)]]).is_err());
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn rejects_truncated_catalog_before_parsing() {
        let path = temp_path();
        fs::create_dir_all(path.join("catalog")).unwrap();
        fs::create_dir_all(path.join("data")).unwrap();
        fs::write(path.join("catalog").join("catalog.anc"), &[b'A', b'N', b'C', b'1', 1, 0]).unwrap();

        let result = Database::open(&path);
        assert!(matches!(result, Err(DbError::CorruptCatalog("file is truncated"))));

        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn rejects_catalog_counts_that_cannot_fit_file() {
        let path = temp_path();
        fs::create_dir_all(path.join("catalog")).unwrap();
        fs::create_dir_all(path.join("data")).unwrap();

        let mut bytes = Vec::new();
        bytes.extend_from_slice(CATALOG_MAGIC);
        bytes.extend_from_slice(&CATALOG_V1_VERSION.to_le_bytes());
        bytes.extend_from_slice(&1_u32.to_le_bytes());
        bytes.extend_from_slice(&1_u32.to_le_bytes());
        bytes.extend_from_slice(b"t");
        bytes.extend_from_slice(&(MAX_CATALOG_ITEMS as u32).to_le_bytes());
        let checksum = checksum64(&bytes);
        bytes.extend_from_slice(&checksum.to_le_bytes());
        bytes.extend_from_slice(CATALOG_END);
        fs::write(path.join("catalog").join("catalog.anc"), bytes).unwrap();

        let result = Database::open(&path);
        assert!(matches!(
            result,
            Err(DbError::CorruptCatalog("column count exceeds catalog bounds"))
        ));

        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn empty_catalog_is_readable_after_reopen() {
        let path = temp_path();
        {
            let _db = Database::open(&path).unwrap();
        }
        {
            let db = Database::open(&path).unwrap();
            assert_eq!(db.table_names().count(), 0);
            assert_eq!(db.catalog_revision(), 0);
        }
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn scan_reports_missing_table_storage_directory() {
        let path = temp_path();
        let mut db = Database::open(&path).unwrap();
        db.create_table("telemetry", sample_schema()).unwrap();
        fs::remove_dir_all(path.join("data").join("telemetry")).unwrap();

        assert!(matches!(db.scan("telemetry"), Err(DbError::Io(_))));

        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn empty_batch_does_not_create_a_segment() {
        let path = temp_path();
        let mut db = Database::open(&path).unwrap();
        db.create_table("telemetry", sample_schema()).unwrap();

        assert!(db.insert_batch("telemetry", &[]).unwrap().is_none());
        assert!(db.scan("telemetry").unwrap().is_empty());

        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn oversized_segment_is_rejected_before_reading_into_memory() {
        let path = temp_path();
        let mut db = Database::open(&path).unwrap();
        db.create_table("telemetry", sample_schema()).unwrap();

        let segment_path = path
            .join("data")
            .join("telemetry")
            .join("segment-00000000000000000001.anf");
        let file = OpenOptions::new().write(true).create_new(true).open(&segment_path).unwrap();
        file.set_len((MAX_SEGMENT_BYTES as u64) + 1).unwrap();
        drop(file);

        assert!(matches!(
            db.scan("telemetry"),
            Err(DbError::Format(FormatError::LimitExceeded("segment bytes")))
        ));

        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn multiple_batches_are_read_in_segment_order() {
        let path = temp_path();
        let mut db = Database::open(&path).unwrap();
        db.create_table("telemetry", sample_schema()).unwrap();
        let a = vec![Value::TimestampMicros(1), Value::Utf8("a".into()), Value::Float64(1.0)];
        let b = vec![Value::TimestampMicros(2), Value::Utf8("b".into()), Value::Float64(2.0)];
        db.insert_batch("telemetry", &[a.clone()]).unwrap();
        db.insert_batch("telemetry", &[b.clone()]).unwrap();
        assert_eq!(db.scan("telemetry").unwrap(), vec![a.clone(), b.clone()]);
        assert_eq!(
            db.scan_columns("telemetry", &["device", "timestamp"]).unwrap(),
            vec![
                vec![Value::Utf8("a".into()), Value::TimestampMicros(1)],
                vec![Value::Utf8("b".into()), Value::TimestampMicros(2)],
            ]
        );
        assert!(db.scan_columns("telemetry", &["missing"]).is_err());
        fs::remove_dir_all(path).unwrap();
    }

    fn batches_to_rows(batches: Vec<RecordBatch>) -> Vec<Vec<Value>> {
        let mut rows = Vec::new();
        for batch in batches {
            for row_index in 0..batch.row_count {
                rows.push(batch.columns.iter().map(|column| column.values[row_index].clone()).collect());
            }
        }
        rows
    }

    #[test]
    fn bounded_scan_returns_projected_columnar_batches_and_honours_limit() {
        let path = temp_path();
        let mut db = Database::open(&path).unwrap();
        db.create_table("telemetry", sample_schema()).unwrap();
        let first_rows = vec![
            vec![Value::TimestampMicros(1), Value::Utf8("a".into()), Value::Float64(1.0)],
            vec![Value::TimestampMicros(2), Value::Utf8("b".into()), Value::Null],
            vec![Value::TimestampMicros(3), Value::Utf8("c".into()), Value::Float64(3.0)],
        ];
        let second_rows = vec![
            vec![Value::TimestampMicros(4), Value::Utf8("d".into()), Value::Float64(4.0)],
            vec![Value::TimestampMicros(5), Value::Utf8("e".into()), Value::Float64(5.0)],
        ];
        db.insert_batch("telemetry", &first_rows).unwrap();
        db.insert_batch("telemetry", &second_rows).unwrap();

        let batches = db.scan_batches("telemetry", 2).unwrap()
            .collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(batches.iter().map(|batch| batch.row_count).collect::<Vec<_>>(), vec![2, 1, 2]);
        assert!(batches.iter().all(|batch| batch.row_count <= 2));
        let mut expected = first_rows.clone();
        expected.extend(second_rows.clone());
        assert_eq!(batches_to_rows(batches), expected);

        let projected = db.scan_columns_batches("telemetry", &["device", "temperature", "device"], 2).unwrap()
            .collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(
            projected[0].columns.iter().map(|column| column.column.name.as_str()).collect::<Vec<_>>(),
            vec!["device", "temperature", "device"]
        );
        assert_eq!(
            batches_to_rows(projected),
            vec![
                vec![Value::Utf8("a".into()), Value::Float64(1.0), Value::Utf8("a".into())],
                vec![Value::Utf8("b".into()), Value::Null, Value::Utf8("b".into())],
                vec![Value::Utf8("c".into()), Value::Float64(3.0), Value::Utf8("c".into())],
                vec![Value::Utf8("d".into()), Value::Float64(4.0), Value::Utf8("d".into())],
                vec![Value::Utf8("e".into()), Value::Float64(5.0), Value::Utf8("e".into())],
            ]
        );

        db.create_table("exact", sample_schema()).unwrap();
        db.insert_batch("exact", &first_rows[..2]).unwrap();
        let exact = db.scan_batches("exact", 2).unwrap()
            .collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(exact.iter().map(|batch| batch.row_count).collect::<Vec<_>>(), vec![2]);

        let limited = db.scan_columns_batches_with_limit(
            "telemetry",
            &["timestamp", "device"],
            2,
            3,
        ).unwrap().collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(limited.iter().map(|batch| batch.row_count).collect::<Vec<_>>(), vec![2, 1]);
        assert_eq!(
            batches_to_rows(limited),
            vec![
                vec![Value::TimestampMicros(1), Value::Utf8("a".into())],
                vec![Value::TimestampMicros(2), Value::Utf8("b".into())],
                vec![Value::TimestampMicros(3), Value::Utf8("c".into())],
            ]
        );

        assert!(db.scan_batches_with_limit("telemetry", 2, 0).unwrap().next().is_none());
        assert!(db.scan_batches("telemetry", 0).is_err());
        assert!(db.scan_batches("telemetry", MAX_BATCH_ROWS + 1).is_err());
        assert!(db.scan_columns_batches("telemetry", &[], 2).is_err());
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn bounded_scan_handles_empty_tables_and_one_row_batches() {
        let path = temp_path();
        let mut db = Database::open(&path).unwrap();
        db.create_table("telemetry", sample_schema()).unwrap();
        assert!(db.scan_batches("telemetry", 1).unwrap().next().is_none());

        let rows = vec![
            vec![Value::TimestampMicros(1), Value::Utf8("a".into()), Value::Float64(1.0)],
            vec![Value::TimestampMicros(2), Value::Utf8("b".into()), Value::Float64(2.0)],
        ];
        db.insert_batch("telemetry", &rows).unwrap();
        let batches = db.scan_batches("telemetry", 1).unwrap()
            .collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(batches.iter().map(|batch| batch.row_count).collect::<Vec<_>>(), vec![1, 1]);
        assert_eq!(batches_to_rows(batches), rows);
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn bounded_scan_maps_legacy_columns_after_nullable_addition_and_reopen() {
        let path = temp_path();
        let mut db = Database::open(&path).unwrap();
        let original = sample_schema();
        db.create_table("telemetry", original.clone()).unwrap();
        let old_rows = vec![
            vec![Value::TimestampMicros(1), Value::Utf8("legacy-a".into()), Value::Float64(10.0)],
            vec![Value::TimestampMicros(2), Value::Utf8("legacy-b".into()), Value::Float64(20.0)],
        ];
        let table_dir = path.join("data").join("telemetry");
        fs::write(
            table_dir.join("segment-00000000000000000001.anf"),
            encode_segment(&original, &old_rows).unwrap(),
        ).unwrap();
        db.add_nullable_column("telemetry", "humidity", DataType::Float64).unwrap();
        let new_rows = vec![
            vec![Value::TimestampMicros(3), Value::Utf8("new-c".into()), Value::Float64(30.0), Value::Float64(40.0)],
        ];
        db.insert_batch("telemetry", &new_rows).unwrap();

        let batches = db.scan_columns_batches("telemetry", &["humidity", "device"], 1).unwrap()
            .collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(batches_to_rows(batches), vec![
            vec![Value::Null, Value::Utf8("legacy-a".into())],
            vec![Value::Null, Value::Utf8("legacy-b".into())],
            vec![Value::Float64(40.0), Value::Utf8("new-c".into())],
        ]);

        drop(db);
        let reopened = Database::open(&path).unwrap();
        let reopened_batches = reopened.scan_columns_batches("telemetry", &["humidity"], 2).unwrap()
            .collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(batches_to_rows(reopened_batches), vec![
            vec![Value::Null],
            vec![Value::Null],
            vec![Value::Float64(40.0)],
        ]);
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn bounded_scan_returns_corrupt_segment_error_once_then_stops() {
        let path = temp_path();
        let mut db = Database::open(&path).unwrap();
        db.create_table("telemetry", sample_schema()).unwrap();
        let rows = vec![vec![
            Value::TimestampMicros(1),
            Value::Utf8("sensor".into()),
            Value::Float64(12.5),
        ]];
        let segment = db.insert_batch("telemetry", &rows).unwrap().unwrap();
        let mut bytes = fs::read(&segment).unwrap();
        let last_payload = bytes.len() - 13;
        bytes[last_payload] ^= 1;
        fs::write(&segment, bytes).unwrap();

        let mut iter = db.scan_batches("telemetry", 1).unwrap();
        assert!(matches!(iter.next(), Some(Err(DbError::Format(FormatError::ChecksumMismatch)))));
        assert!(iter.next().is_none());
        fs::remove_dir_all(path).unwrap();
    }


    #[test]
    fn database_handle_enforces_exclusive_writer_lock_and_releases_it_on_drop() {
        let path = temp_path();
        let db = Database::open(&path).unwrap();
        assert!(path.join(".ananta.lock").is_file());
        assert!(matches!(
            Database::open(&path),
            Err(DbError::DatabaseLocked(_))
        ));

        drop(db);
        assert!(!path.join(".ananta.lock").exists());
        let reopened = Database::open(&path).unwrap();
        assert!(path.join(".ananta.lock").is_file());
        drop(reopened);
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn database_open_cleans_orphan_temp_segments_but_preserves_published_and_unknown_files() {
        let path = temp_path();
        let mut db = Database::open(&path).unwrap();
        db.create_table("telemetry", sample_schema()).unwrap();
        let rows = vec![vec![
            Value::TimestampMicros(1),
            Value::Utf8("published".into()),
            Value::Float64(12.5),
        ]];
        let published = db.insert_batch("telemetry", &rows).unwrap().unwrap();
        assert!(published.is_file());
        drop(db);

        let table_dir = path.join("data").join("telemetry");
        let orphan_temp = table_dir.join(".segment-00000000000000000099.tmp");
        let orphan_chronicle_temp = table_dir.join(".chronicle-00000000000000000100.tmp");
        let unrelated = table_dir.join(".segment-99.tmp");
        let unknown = table_dir.join("notes.txt");
        fs::write(&orphan_temp, b"interrupted segment write").unwrap();
        fs::write(&orphan_chronicle_temp, b"interrupted Chronicle write").unwrap();
        fs::write(&unrelated, b"not a canonical temp segment").unwrap();
        fs::write(&unknown, b"leave untouched").unwrap();

        let reopened = Database::open(&path).unwrap();
        assert!(!orphan_temp.exists());
        assert!(!orphan_chronicle_temp.exists());
        assert_eq!(fs::read(unrelated).unwrap(), b"not a canonical temp segment");
        assert_eq!(fs::read(unknown).unwrap(), b"leave untouched");
        assert_eq!(reopened.scan("telemetry").unwrap(), rows);
        assert!(published.exists());
        drop(reopened);
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn database_open_fails_closed_on_stale_writer_lock() {
        let path = temp_path();
        fs::create_dir_all(&path).unwrap();
        let lock_path = path.join(".ananta.lock");
        fs::write(&lock_path, b"pid=unverified\n").unwrap();

        assert!(matches!(
            Database::open(&path),
            Err(DbError::DatabaseLocked(found)) if found == lock_path
        ));
        assert_eq!(fs::read(&lock_path).unwrap(), b"pid=unverified\n");
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn insert_batch_publishes_complete_segment_without_leaving_temp_name() {
        let path = temp_path();
        let mut db = Database::open(&path).unwrap();
        db.create_table("telemetry", sample_schema()).unwrap();
        let rows = vec![vec![
            Value::TimestampMicros(7),
            Value::Utf8("sensor".into()),
            Value::Float64(21.0),
        ]];

        let final_path = db.insert_batch("telemetry", &rows).unwrap().unwrap();
        assert_eq!(
            final_path.file_name().and_then(|name| name.to_str()),
            Some("segment-00000000000000000001.anf")
        );
        assert!(final_path.is_file());
        assert!(!path.join("data").join("telemetry")
            .join(".segment-00000000000000000001.tmp").exists());
        assert_eq!(db.scan("telemetry").unwrap(), rows);
        drop(db);
        fs::remove_dir_all(path).unwrap();
    }


    #[test]
    fn chronicle_ingestion_accepts_once_and_resolves_duplicate_retry_after_restart() {
        let path = temp_path();
        let rows = vec![vec![
            Value::TimestampMicros(100),
            Value::Utf8("sensor-1".into()),
            Value::Float64(21.5),
        ]];
        let batch_id = BatchId::from_bytes([1; 16]).unwrap();
        let first_result;
        {
            let mut db = Database::open(&path).unwrap();
            db.create_table("telemetry", sample_schema()).unwrap();
            first_result = db.ingest_batch("telemetry", batch_id, &rows).unwrap();
            assert_eq!(first_result.outcome, IngestOutcome::Accepted);
            assert_eq!(first_result.ingestion_sequence, 1);
            assert_eq!(first_result.row_count, 1);
            assert!(first_result.chronicle_path.extension().is_some_and(|ext| ext == "chr"));

            let retry = db.ingest_batch("telemetry", batch_id, &rows).unwrap();
            assert_eq!(retry.outcome, IngestOutcome::Duplicate);
            assert_eq!(retry.ingestion_sequence, first_result.ingestion_sequence);
            assert_eq!(retry.chronicle_path, first_result.chronicle_path);
            assert_eq!(db.scan("telemetry").unwrap(), rows);
            let stats = db.ingestion_stats("telemetry").unwrap();
            assert_eq!(stats.accepted_batches, 1);
            assert_eq!(stats.accepted_rows, 1);
            assert_eq!(stats.duplicate_requests, 1);
        }

        {
            let mut db = Database::open(&path).unwrap();
            let retry = db.ingest_batch("telemetry", batch_id, &rows).unwrap();
            assert_eq!(retry.outcome, IngestOutcome::Duplicate);
            assert_eq!(retry.ingestion_sequence, 1);
            assert_eq!(db.scan("telemetry").unwrap(), rows);
            // Durable accepted counters are reconstructed; request counters are process-local.
            let stats = db.ingestion_stats("telemetry").unwrap();
            assert_eq!(stats.accepted_batches, 1);
            assert_eq!(stats.accepted_rows, 1);
            assert_eq!(stats.duplicate_requests, 1);
        }
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn chronicle_rejects_conflicting_batch_id_and_does_not_consume_sequence() {
        let path = temp_path();
        let mut db = Database::open(&path).unwrap();
        db.create_table("telemetry", sample_schema()).unwrap();
        let first = vec![vec![
            Value::TimestampMicros(10),
            Value::Utf8("sensor-a".into()),
            Value::Float64(10.0),
        ]];
        let different = vec![vec![
            Value::TimestampMicros(11),
            Value::Utf8("sensor-b".into()),
            Value::Float64(11.0),
        ]];
        let id = BatchId::from_bytes([2; 16]).unwrap();

        let accepted = db.ingest_batch("telemetry", id, &first).unwrap();
        assert_eq!(accepted.ingestion_sequence, 1);
        assert!(matches!(
            db.ingest_batch("telemetry", id, &different),
            Err(DbError::BatchIdConflict(found)) if found == id
        ));
        let next = db.ingest_batch(
            "telemetry",
            BatchId::from_bytes([3; 16]).unwrap(),
            &different,
        ).unwrap();
        assert_eq!(next.ingestion_sequence, 2);
        assert_eq!(db.scan("telemetry").unwrap(), vec![first[0].clone(), different[0].clone()]);
        let stats = db.ingestion_stats("telemetry").unwrap();
        assert_eq!(stats.accepted_batches, 2);
        assert_eq!(stats.rejected_requests, 1);
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn chronicle_batch_id_scope_uses_stable_table_identity() {
        let path = temp_path();
        let mut db = Database::open(&path).unwrap();
        db.create_table("alpha", sample_schema()).unwrap();
        db.create_table("beta", sample_schema()).unwrap();
        let rows = vec![vec![
            Value::TimestampMicros(1),
            Value::Utf8("same-id-different-table".into()),
            Value::Float64(1.0),
        ]];
        let id = BatchId::from_bytes([4; 16]).unwrap();

        let alpha = db.ingest_batch("alpha", id, &rows).unwrap();
        let beta = db.ingest_batch("beta", id, &rows).unwrap();
        assert_eq!(alpha.ingestion_sequence, 1);
        assert_eq!(beta.ingestion_sequence, 1);
        assert_ne!(alpha.table_id, beta.table_id);
        assert_eq!(db.ingestion_stats("alpha").unwrap().accepted_batches, 1);
        assert_eq!(db.ingestion_stats("beta").unwrap().accepted_batches, 1);
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn chronicle_precommit_publication_failure_consumes_no_sequence() {
        let path = temp_path();
        let mut db = Database::open(&path).unwrap();
        db.create_table("telemetry", sample_schema()).unwrap();
        let rows = vec![vec![
            Value::TimestampMicros(44),
            Value::Utf8("retry-after-write-failure".into()),
            Value::Float64(4.4),
        ]];
        let blocked_temp = path.join("data").join("telemetry")
            .join(".chronicle-00000000000000000001.tmp");
        fs::write(&blocked_temp, b"owned by another attempt").unwrap();
        let batch_id = BatchId::from_bytes([17; 16]).unwrap();

        assert!(matches!(
            db.ingest_batch("telemetry", batch_id, &rows),
            Err(DbError::Io(_))
        ));
        assert_eq!(db.ingestion_stats("telemetry").unwrap().accepted_batches, 0);
        assert_eq!(db.ingestion_stats("telemetry").unwrap().publication_failures, 1);
        assert_eq!(fs::read(&blocked_temp).unwrap(), b"owned by another attempt");

        fs::remove_file(blocked_temp).unwrap();
        let accepted = db.ingest_batch("telemetry", batch_id, &rows).unwrap();
        assert_eq!(accepted.ingestion_sequence, 1);
        assert_eq!(db.ingestion_stats("telemetry").unwrap().accepted_batches, 1);
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn chronicle_rejects_empty_and_invalid_batches_without_sequence_gaps() {
        let path = temp_path();
        let mut db = Database::open(&path).unwrap();
        db.create_table("telemetry", sample_schema()).unwrap();
        let id1 = BatchId::from_bytes([5; 16]).unwrap();
        assert!(matches!(db.ingest_batch("telemetry", id1, &[]), Err(DbError::EmptyBatch)));

        let invalid = vec![vec![
            Value::Utf8("not-a-timestamp".into()),
            Value::Utf8("sensor".into()),
            Value::Float64(1.0),
        ]];
        assert!(matches!(
            db.ingest_batch("telemetry", BatchId::from_bytes([6; 16]).unwrap(), &invalid),
            Err(DbError::IngestRowValidation { row_index: 0, .. })
        ));
        assert_eq!(db.scan("telemetry").unwrap(), Vec::<Vec<Value>>::new());
        assert_eq!(db.ingestion_stats("telemetry").unwrap().accepted_batches, 0);
        assert_eq!(db.ingestion_stats("telemetry").unwrap().rejected_requests, 2);

        let valid = vec![vec![
            Value::TimestampMicros(2),
            Value::Utf8("sensor".into()),
            Value::Float64(2.0),
        ]];
        let result = db.ingest_batch("telemetry", BatchId::from_bytes([7; 16]).unwrap(), &valid).unwrap();
        assert_eq!(result.ingestion_sequence, 1);
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn chronicle_keeps_commit_order_and_preserves_late_event_timestamps() {
        let path = temp_path();
        let mut db = Database::open(&path).unwrap();
        db.create_table("telemetry", sample_schema()).unwrap();
        let later_event = vec![vec![
            Value::TimestampMicros(900),
            Value::Utf8("later".into()),
            Value::Float64(9.0),
        ]];
        let late_event = vec![vec![
            Value::TimestampMicros(100),
            Value::Utf8("late".into()),
            Value::Float64(1.0),
        ]];
        let first = db.ingest_batch("telemetry", BatchId::from_bytes([8; 16]).unwrap(), &later_event).unwrap();
        let second = db.ingest_batch("telemetry", BatchId::from_bytes([9; 16]).unwrap(), &late_event).unwrap();
        assert_eq!((first.ingestion_sequence, second.ingestion_sequence), (1, 2));
        assert_eq!(db.scan("telemetry").unwrap(), vec![later_event[0].clone(), late_event[0].clone()]);
        let batches = db.scan_batches("telemetry", 1).unwrap()
            .collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(batches.iter().map(|batch| batch.row_count).collect::<Vec<_>>(), vec![1, 1]);
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn chronicle_coexists_with_legacy_segments_and_legacy_insert_keeps_physical_order() {
        let path = temp_path();
        let mut db = Database::open(&path).unwrap();
        db.create_table("telemetry", sample_schema()).unwrap();
        let legacy_before = vec![vec![
            Value::TimestampMicros(1),
            Value::Utf8("legacy-before".into()),
            Value::Float64(1.0),
        ]];
        db.insert_batch("telemetry", &legacy_before).unwrap();

        let chronicle_rows = vec![vec![
            Value::TimestampMicros(2),
            Value::Utf8("chronicle".into()),
            Value::Float64(2.0),
        ]];
        db.ingest_batch("telemetry", BatchId::from_bytes([10; 16]).unwrap(), &chronicle_rows).unwrap();

        let legacy_after = vec![vec![
            Value::TimestampMicros(3),
            Value::Utf8("legacy-after".into()),
            Value::Float64(3.0),
        ]];
        db.insert_batch("telemetry", &legacy_after).unwrap();
        let next_chronicle = vec![vec![
            Value::TimestampMicros(4),
            Value::Utf8("chronicle-next".into()),
            Value::Float64(4.0),
        ]];
        let second = db.ingest_batch("telemetry", BatchId::from_bytes([11; 16]).unwrap(), &next_chronicle).unwrap();
        assert_eq!(second.ingestion_sequence, 2);
        assert_eq!(
            db.scan("telemetry").unwrap(),
            vec![
                legacy_before[0].clone(),
                chronicle_rows[0].clone(),
                legacy_after[0].clone(),
                next_chronicle[0].clone(),
            ]
        );
        drop(db);
        let reopened = Database::open(&path).unwrap();
        assert_eq!(reopened.scan("telemetry").unwrap().len(), 4);
        assert_eq!(reopened.ingestion_stats("telemetry").unwrap().accepted_batches, 2);
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn chronicle_duplicate_retry_uses_the_original_schema_after_nullable_column_evolution() {
        let path = temp_path();
        let mut db = Database::open(&path).unwrap();
        db.create_table("telemetry", sample_schema()).unwrap();
        let old_rows = vec![vec![
            Value::TimestampMicros(100),
            Value::Utf8("schema-v1".into()),
            Value::Float64(10.0),
        ]];
        let id = BatchId::from_bytes([13; 16]).unwrap();
        let accepted = db.ingest_batch("telemetry", id, &old_rows).unwrap();
        assert_eq!(accepted.ingestion_sequence, 1);

        db.add_nullable_column("telemetry", "humidity", DataType::Float64).unwrap();
        let retry = db.ingest_batch("telemetry", id, &old_rows).unwrap();
        assert_eq!(retry.outcome, IngestOutcome::Duplicate);
        assert_eq!(retry.ingestion_sequence, 1);

        let new_rows = vec![vec![
            Value::TimestampMicros(200),
            Value::Utf8("schema-v2".into()),
            Value::Float64(20.0),
            Value::Float64(30.0),
        ]];
        let next = db.ingest_batch(
            "telemetry",
            BatchId::from_bytes([14; 16]).unwrap(),
            &new_rows,
        ).unwrap();
        assert_eq!(next.ingestion_sequence, 2);
        assert_eq!(db.scan("telemetry").unwrap(), vec![
            vec![
                Value::TimestampMicros(100),
                Value::Utf8("schema-v1".into()),
                Value::Float64(10.0),
                Value::Null,
            ],
            new_rows[0].clone(),
        ]);
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn chronicle_commit_file_recovers_an_uncertain_client_acknowledgement() {
        let path = temp_path();
        let mut db = Database::open(&path).unwrap();
        db.create_table("telemetry", sample_schema()).unwrap();
        let table_id = db.table_id("telemetry").unwrap().0;
        let schema_revision = db.table_schema_revision("telemetry").unwrap();
        let column_ids = ["timestamp", "device", "temperature"]
            .map(|name| db.column_id("telemetry", name).unwrap().0);
        let rows = vec![vec![
            Value::TimestampMicros(77),
            Value::Utf8("ack-lost".into()),
            Value::Float64(7.7),
        ]];
        let payload = encode_segment_v2(
            db.schema("telemetry").unwrap(),
            &rows,
            table_id,
            schema_revision,
            &column_ids,
        ).unwrap();
        let batch_id = BatchId::from_bytes([15; 16]).unwrap();
        let (record, _) = encode_record(table_id, batch_id, 1, schema_revision, 1, &payload).unwrap();
        let directory = path.join("data").join("telemetry");
        let final_path = publish_chronicle(&directory, 1, &record).unwrap();
        assert!(final_path.exists());
        drop(db);

        let mut reopened = Database::open(&path).unwrap();
        let retry = reopened.ingest_batch("telemetry", batch_id, &rows).unwrap();
        assert_eq!(retry.outcome, IngestOutcome::Duplicate);
        assert_eq!(retry.ingestion_sequence, 1);
        let next = reopened.ingest_batch(
            "telemetry",
            BatchId::from_bytes([16; 16]).unwrap(),
            &rows,
        ).unwrap();
        assert_eq!(next.ingestion_sequence, 2);
        assert_eq!(reopened.scan("telemetry").unwrap(), vec![rows[0].clone(), rows[0].clone()]);
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn chronicle_corruption_is_detected_during_database_open() {
        let path = temp_path();
        let mut db = Database::open(&path).unwrap();
        db.create_table("telemetry", sample_schema()).unwrap();
        let rows = vec![vec![
            Value::TimestampMicros(5),
            Value::Utf8("integrity".into()),
            Value::Float64(5.0),
        ]];
        let result = db.ingest_batch("telemetry", BatchId::from_bytes([12; 16]).unwrap(), &rows).unwrap();
        drop(db);

        let mut bytes = fs::read(&result.chronicle_path).unwrap();
        let position = 96 + 36 + 3;
        bytes[position] ^= 0x01;
        fs::write(&result.chronicle_path, bytes).unwrap();
        assert!(matches!(
            Database::open(&path),
            Err(DbError::ChronicleCorrupt("record checksum mismatch"))
        ));
        fs::remove_dir_all(path).unwrap();
    }


    #[test]
    fn capsule_and_navigation_map_wal_publications_replay_after_commit_interruption() {
        let path = temp_path();
        let mut db = Database::open(&path).unwrap();
        db.create_table("telemetry", sample_schema()).unwrap();
        for sequence in 1_u8..=2 {
            let rows = vec![vec![
                Value::TimestampMicros(i64::from(sequence) * 100),
                Value::Utf8(format!("capsule-{sequence}")),
                Value::Float64(f64::from(sequence)),
            ]];
            db.ingest_batch(
                "telemetry",
                BatchId::from_bytes([sequence + 100; 16]).unwrap(),
                &rows,
            ).unwrap();
        }

        crate::wal_recovery::arm_wal_recovery_crash_point(
            crate::wal_recovery::WalRecoveryCrashPoint::AfterWalCommit,
        );
        let interrupted = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            db.build_capsule("telemetry", 700, 1, 1, 2, Some("timestamp"))
        }));
        assert!(interrupted.is_err());
        drop(db);

        let mut db = Database::open(&path).unwrap();
        let capsule = db.validate_capsule("telemetry", 700, 1).unwrap();
        assert_eq!(capsule.row_count, 2);
        assert_eq!(db.scan_capsule("telemetry", 700, 1).unwrap().len(), 2);

        crate::wal_recovery::arm_wal_recovery_crash_point(
            crate::wal_recovery::WalRecoveryCrashPoint::AfterWalCommit,
        );
        let interrupted = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            db.build_navigation_map("telemetry", 701)
        }));
        assert!(interrupted.is_err());
        drop(db);

        let db = Database::open(&path).unwrap();
        let request = CapsuleScanRequest {
            ranges: vec![ColumnRangePredicate::new(
                "timestamp",
                Some(Value::TimestampMicros(0)),
                true,
                Some(Value::TimestampMicros(1_000)),
                true,
            )],
            projection: None,
        };
        let mapped = db.scan_capsules("telemetry", &request, Some(701)).unwrap();
        assert_eq!(mapped.rows.len(), 2);
        assert_eq!(mapped.diagnostics.capsules_read, 1);
        drop(db);
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn capsule_builds_from_contiguous_chronicle_range_and_validates_column_stats() {
        let path = temp_path();
        let mut db = Database::open(&path).unwrap();
        let schema = Schema::new(vec![
            Column::new("event_time", DataType::TimestampMicros, false),
            Column::new("temperature", DataType::Float64, true),
            Column::new("device", DataType::Utf8, false),
            Column::new("all_null", DataType::Int64, true),
        ]).unwrap();
        db.create_table("telemetry", schema).unwrap();
        let rows = vec![
            vec![Value::TimestampMicros(100), Value::Float64(21.5), Value::Utf8("a".into()), Value::Null],
            vec![Value::TimestampMicros(300), Value::Null, Value::Utf8("bb".into()), Value::Null],
            vec![Value::TimestampMicros(200), Value::Float64(18.5), Value::Utf8("c".into()), Value::Null],
        ];
        for (index, row) in rows.iter().enumerate() {
            db.ingest_batch(
                "telemetry",
                BatchId::from_bytes([20 + index as u8; 16]).unwrap(),
                std::slice::from_ref(row),
            ).unwrap();
        }

        let capsule_path = db.build_capsule("telemetry", 42, 1, 1, 3, Some("event_time")).unwrap();
        assert!(capsule_path.is_file());
        assert_eq!(
            capsule_path.file_name().and_then(|name| name.to_str()),
            Some("capsule-00000000000000000042-g00000000000000000001.cap")
        );
        let metadata = db.validate_capsule("telemetry", 42, 1).unwrap();
        assert_eq!(metadata.row_count, 3);
        assert_eq!((metadata.first_ingestion_sequence, metadata.last_ingestion_sequence), (1, 3));
        assert_eq!(metadata.event_time_column_id, Some(db.column_id("telemetry", "event_time").unwrap().0));
        assert_eq!((metadata.temporal_min, metadata.temporal_max), (Some(100), Some(300)));
        assert_eq!(metadata.columns[1].null_count, 1);
        assert_eq!(metadata.columns[1].minimum, Some(Value::Float64(18.5)));
        assert_eq!(metadata.columns[1].maximum, Some(Value::Float64(21.5)));
        assert_eq!(metadata.columns[2].minimum, None);
        assert_eq!(metadata.columns[3].null_count, 3);
        assert_eq!(metadata.columns[3].minimum, None);
        assert_eq!(db.scan("telemetry").unwrap(), rows);

        // Capsules are derived copies: all committed Chronicle records remain present.
        let chronicle_count = fs::read_dir(path.join("data").join("telemetry")).unwrap()
            .filter_map(Result::ok)
            .filter(|entry| entry.path().extension().and_then(|ext| ext.to_str()) == Some("chr"))
            .count();
        assert_eq!(chronicle_count, 3);

        // A new generation can coexist; the same identity/generation cannot overwrite.
        let second_generation = db.build_capsule("telemetry", 42, 2, 1, 3, None).unwrap();
        assert!(second_generation.is_file());
        let second_metadata = db.validate_capsule("telemetry", 42, 2).unwrap();
        assert_eq!(second_metadata.event_time_column_id, None);
        assert_eq!(second_metadata.temporal_min, None);
        assert!(matches!(
            db.build_capsule("telemetry", 42, 1, 1, 3, Some("event_time")),
            Err(DbError::Io(_))
        ));
        assert!(capsule_path.is_file());

        assert!(matches!(
            db.build_capsule("telemetry", 43, 1, 1, 4, Some("event_time")),
            Err(DbError::CapsuleCorrupt("source sequence range is not fully committed and contiguous"))
        ));
        assert!(matches!(
            db.build_capsule("telemetry", 44, 1, 1, 3, Some("device")),
            Err(DbError::CapsuleCorrupt("event-time column must have type TIMESTAMP_MICROS"))
        ));
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn capsule_coverage_requires_all_logical_rows_to_be_chronicle_backed() {
        let path = temp_path();
        let mut db = Database::open(&path).unwrap();
        db.create_table("telemetry", sample_schema()).unwrap();
        let chronicle_rows = vec![vec![
            Value::TimestampMicros(10),
            Value::Utf8("chronicle".into()),
            Value::Float64(1.0),
        ]];
        db.ingest_batch(
            "telemetry",
            BatchId::from_bytes([71; 16]).unwrap(),
            &chronicle_rows,
        ).unwrap();
        db.build_capsule("telemetry", 1, 1, 1, 1, Some("timestamp")).unwrap();
        assert!(db.capsules_cover_chronicle("telemetry").unwrap());

        let legacy_rows = vec![vec![
            Value::TimestampMicros(20),
            Value::Utf8("legacy-anf".into()),
            Value::Float64(2.0),
        ]];
        db.insert_batch("telemetry", &legacy_rows).unwrap();
        assert!(!db.capsules_cover_chronicle("telemetry").unwrap());
        assert_eq!(db.scan("telemetry").unwrap(), [chronicle_rows[0].clone(), legacy_rows[0].clone()]);
        drop(db);
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn capsule_build_resolves_historical_rows_into_current_schema_revision() {
        let path = temp_path();
        let mut db = Database::open(&path).unwrap();
        let original = sample_schema();
        db.create_table("telemetry", original).unwrap();
        let old_rows = vec![vec![
            Value::TimestampMicros(10),
            Value::Utf8("old".into()),
            Value::Float64(1.0),
        ]];
        db.ingest_batch(
            "telemetry",
            BatchId::from_bytes([30; 16]).unwrap(),
            &old_rows,
        ).unwrap();

        db.add_nullable_column("telemetry", "humidity", DataType::Float64).unwrap();
        let new_rows = vec![vec![
            Value::TimestampMicros(20),
            Value::Utf8("new".into()),
            Value::Float64(2.0),
            Value::Float64(60.0),
        ]];
        db.ingest_batch(
            "telemetry",
            BatchId::from_bytes([31; 16]).unwrap(),
            &new_rows,
        ).unwrap();

        let path_to_capsule = db.build_capsule("telemetry", 5, 1, 1, 2, Some("timestamp")).unwrap();
        let metadata = db.validate_capsule("telemetry", 5, 1).unwrap();
        assert_eq!(metadata.schema_revision, 2);
        assert_eq!(metadata.row_count, 2);
        assert_eq!(metadata.columns.len(), 4);
        assert_eq!(metadata.columns[3].null_count, 1);
        assert_eq!(metadata.columns[3].minimum, Some(Value::Float64(60.0)));
        assert_eq!(metadata.columns[3].maximum, Some(Value::Float64(60.0)));
        assert!(path_to_capsule.is_file());
        assert_eq!(db.scan("telemetry").unwrap(), vec![
            vec![
                Value::TimestampMicros(10),
                Value::Utf8("old".into()),
                Value::Float64(1.0),
                Value::Null,
            ],
            new_rows[0].clone(),
        ]);
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn capsule_temp_files_are_cleaned_and_corruption_is_detected_by_validator() {
        let path = temp_path();
        let mut db = Database::open(&path).unwrap();
        db.create_table("telemetry", sample_schema()).unwrap();
        let rows = vec![vec![
            Value::TimestampMicros(50),
            Value::Utf8("capsule".into()),
            Value::Float64(5.0),
        ]];
        db.ingest_batch("telemetry", BatchId::from_bytes([32; 16]).unwrap(), &rows).unwrap();
        let capsule_path = db.build_capsule("telemetry", 9, 1, 1, 1, Some("timestamp")).unwrap();
        drop(db);

        let capsule_dir = path.join("data").join("telemetry").join("capsules");
        let orphan_temp = capsule_dir.join(".capsule-00000000000000000010-g00000000000000000001.tmp");
        fs::write(&orphan_temp, b"interrupted Capsule publication").unwrap();
        let mut bytes = fs::read(&capsule_path).unwrap();
        let payload_start = 144 + 3 * 48;
        bytes[payload_start] ^= 1;
        fs::write(&capsule_path, bytes).unwrap();

        let reopened = Database::open(&path).unwrap();
        assert!(!orphan_temp.exists());
        assert!(matches!(
            reopened.validate_capsule("telemetry", 9, 1),
            Err(DbError::CapsuleCorrupt("Capsule checksum mismatch"))
        ));
        assert_eq!(reopened.scan("telemetry").unwrap(), rows);
        fs::remove_dir_all(path).unwrap();
    }


    #[test]
    fn navigation_map_prunes_without_changing_query_results_and_reports_diagnostics() {
        let path = temp_path();
        let mut db = Database::open(&path).unwrap();
        db.create_table("telemetry", sample_schema()).unwrap();

        for capsule_index in 0..4_i64 {
            let start = capsule_index * 10;
            let rows = (0..10_i64).map(|offset| {
                let timestamp = start + offset;
                vec![
                    Value::TimestampMicros(timestamp),
                    Value::Utf8(format!("device-{}", capsule_index)),
                    Value::Float64((timestamp + 10) as f64),
                ]
            }).collect::<Vec<_>>();
            let sequence = capsule_index as u8 + 1;
            db.ingest_batch(
                "telemetry",
                BatchId::from_bytes([sequence; 16]).unwrap(),
                &rows,
            ).unwrap();
            db.build_capsule(
                "telemetry",
                sequence as u64,
                1,
                sequence as u64,
                sequence as u64,
                Some("timestamp"),
            ).unwrap();
        }

        let map_path = db.build_navigation_map("telemetry", 1).unwrap();
        assert!(map_path.is_file());

        let request = CapsuleScanRequest {
            ranges: vec![
                ColumnRangePredicate::new(
                    "timestamp",
                    Some(Value::TimestampMicros(15)),
                    true,
                    Some(Value::TimestampMicros(25)),
                    true,
                ),
                ColumnRangePredicate::new(
                    "temperature",
                    Some(Value::Float64(25.0)),
                    true,
                    Some(Value::Float64(45.0)),
                    true,
                ),
            ],
            projection: Some(vec!["device".into(), "temperature".into()]),
        };
        let reference = db.scan_capsules("telemetry", &request, None).unwrap();
        let pruned = db.scan_capsules("telemetry", &request, Some(1)).unwrap();
        assert_eq!(pruned.rows, reference.rows);
        assert_eq!(pruned.rows.len(), 11);
        assert_eq!(reference.diagnostics.capsules_read, 4);
        assert_eq!(reference.diagnostics.rows_examined, 40);
        assert_eq!(pruned.diagnostics.capsules_considered, 4);
        assert_eq!(pruned.diagnostics.capsules_skipped, 2);
        assert_eq!(pruned.diagnostics.capsules_read, 2);
        assert_eq!(pruned.diagnostics.rows_examined, 20);
        assert_eq!(pruned.diagnostics.rows_returned, 11);
        assert!(pruned.diagnostics.bytes_verified > 0);
        assert!(pruned.diagnostics.payload_bytes_decoded < reference.diagnostics.payload_bytes_decoded);

        // Inclusive endpoints must retain Capsules whose min/max only touches a bound.
        let inclusive_edges = CapsuleScanRequest {
            ranges: vec![ColumnRangePredicate::new(
                "timestamp",
                Some(Value::TimestampMicros(19)),
                true,
                Some(Value::TimestampMicros(20)),
                true,
            )],
            projection: None,
        };
        let edge_reference = db.scan_capsules("telemetry", &inclusive_edges, None).unwrap();
        let edge_pruned = db.scan_capsules("telemetry", &inclusive_edges, Some(1)).unwrap();
        assert_eq!(edge_pruned.rows, edge_reference.rows);
        assert_eq!(edge_pruned.rows.len(), 2);
        assert_eq!(edge_pruned.diagnostics.capsules_skipped, 2);

        // Exclusive touching endpoints have no matches and may prune both touching Capsules.
        let exclusive_edges = CapsuleScanRequest {
            ranges: vec![ColumnRangePredicate::new(
                "timestamp",
                Some(Value::TimestampMicros(19)),
                false,
                Some(Value::TimestampMicros(20)),
                false,
            )],
            projection: None,
        };
        let exclusive_reference = db.scan_capsules("telemetry", &exclusive_edges, None).unwrap();
        let exclusive_pruned = db.scan_capsules("telemetry", &exclusive_edges, Some(1)).unwrap();
        assert_eq!(exclusive_pruned.rows, exclusive_reference.rows);
        assert!(exclusive_pruned.rows.is_empty());
        assert_eq!(exclusive_pruned.diagnostics.capsules_skipped, 4);
        assert_eq!(exclusive_pruned.diagnostics.capsules_read, 0);

        // UTF8 has no min/max in Capsule v0.1; missing stats conservatively keep all Capsules.
        let string_filter = CapsuleScanRequest {
            ranges: vec![ColumnRangePredicate::new(
                "device",
                Some(Value::Utf8("device-2".into())),
                true,
                Some(Value::Utf8("device-2".into())),
                true,
            )],
            projection: Some(vec!["timestamp".into()]),
        };
        let string_reference = db.scan_capsules("telemetry", &string_filter, None).unwrap();
        let string_pruned = db.scan_capsules("telemetry", &string_filter, Some(1)).unwrap();
        assert_eq!(string_pruned.rows, string_reference.rows);
        assert_eq!(string_pruned.rows.len(), 10);
        assert_eq!(string_pruned.diagnostics.capsules_skipped, 0);
        assert_eq!(string_pruned.diagnostics.capsules_read, 4);

        assert!(matches!(
            db.scan_capsules("telemetry", &CapsuleScanRequest {
                ranges: vec![ColumnRangePredicate::new("temperature", Some(Value::Int64(1)), true, None, true)],
                projection: None,
            }, Some(1)),
            Err(DbError::InvalidRange(_))
        ));
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn navigation_map_detects_capsule_set_and_file_changes() {
        let path = temp_path();
        let mut db = Database::open(&path).unwrap();
        db.create_table("telemetry", sample_schema()).unwrap();
        let make_rows = |time: i64| vec![vec![
            Value::TimestampMicros(time),
            Value::Utf8("sensor".into()),
            Value::Float64(time as f64),
        ]];
        db.ingest_batch("telemetry", BatchId::from_bytes([40; 16]).unwrap(), &make_rows(10)).unwrap();
        let first_capsule = db.build_capsule("telemetry", 1, 1, 1, 1, Some("timestamp")).unwrap();
        db.build_navigation_map("telemetry", 1).unwrap();

        db.ingest_batch("telemetry", BatchId::from_bytes([41; 16]).unwrap(), &make_rows(20)).unwrap();
        db.build_capsule("telemetry", 2, 1, 2, 2, Some("timestamp")).unwrap();
        let all_range = CapsuleScanRequest {
            ranges: vec![ColumnRangePredicate::new(
                "timestamp", Some(Value::TimestampMicros(0)), true,
                Some(Value::TimestampMicros(100)), true,
            )],
            projection: None,
        };
        assert!(matches!(
            db.scan_capsules("telemetry", &all_range, Some(1)),
            Err(DbError::NavigationMapStale("Capsule set changed since this map generation was built"))
        ));

        // A new map records both files; any subsequent byte mutation is rejected before pruning.
        db.build_navigation_map("telemetry", 2).unwrap();
        let mut bytes = fs::read(&first_capsule).unwrap();
        bytes[144] ^= 1;
        fs::write(&first_capsule, bytes).unwrap();
        assert!(matches!(
            db.scan_capsules("telemetry", &all_range, Some(2)),
            Err(DbError::NavigationMapStale("Capsule bytes changed after map publication"))
        ));
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn navigation_map_pruning_survives_nullable_schema_evolution() {
        let path = temp_path();
        let mut db = Database::open(&path).unwrap();
        db.create_table("telemetry", sample_schema()).unwrap();
        let first = vec![vec![
            Value::TimestampMicros(10),
            Value::Utf8("old".into()),
            Value::Float64(1.0),
        ]];
        db.ingest_batch("telemetry", BatchId::from_bytes([42; 16]).unwrap(), &first).unwrap();
        db.build_capsule("telemetry", 1, 1, 1, 1, Some("timestamp")).unwrap();

        db.add_nullable_column("telemetry", "humidity", DataType::Float64).unwrap();
        let second = vec![vec![
            Value::TimestampMicros(20),
            Value::Utf8("new".into()),
            Value::Float64(2.0),
            Value::Float64(50.0),
        ]];
        db.ingest_batch("telemetry", BatchId::from_bytes([43; 16]).unwrap(), &second).unwrap();
        db.build_capsule("telemetry", 2, 1, 2, 2, Some("timestamp")).unwrap();
        db.build_navigation_map("telemetry", 1).unwrap();

        let request = CapsuleScanRequest {
            ranges: vec![ColumnRangePredicate::new(
                "humidity", Some(Value::Float64(40.0)), true,
                Some(Value::Float64(60.0)), true,
            )],
            projection: None,
        };
        let reference = db.scan_capsules("telemetry", &request, None).unwrap();
        let pruned = db.scan_capsules("telemetry", &request, Some(1)).unwrap();
        assert_eq!(pruned.rows, reference.rows);
        assert_eq!(pruned.rows.len(), 1);
        // Older Capsule has no ColumnId/stat for humidity, so the map must retain it.
        assert_eq!(pruned.diagnostics.capsules_skipped, 0);
        assert_eq!(pruned.diagnostics.capsules_read, 2);
        assert_eq!(pruned.rows[0][3], Value::Float64(50.0));
        fs::remove_dir_all(path).unwrap();
    }


    #[test]
    fn compaction_preflight_verifies_real_capsule_and_chronicle_files() {
        let path = temp_path();
        let mut db = Database::open(&path).unwrap();
        db.create_table("telemetry", sample_schema()).unwrap();

        for sequence in 1_u8..=3 {
            let batch_id = BatchId::from_bytes([sequence; 16]).unwrap();
            let rows = vec![vec![
                Value::TimestampMicros(i64::from(sequence) * 1_000),
                Value::Utf8(format!("device-{sequence}")),
                Value::Float64(f64::from(sequence)),
            ]];
            db.ingest_batch("telemetry", batch_id, &rows).unwrap();
        }

        db.build_capsule("telemetry", 10, 1, 1, 2, None).unwrap();
        db.build_capsule("telemetry", 11, 1, 3, 3, None).unwrap();

        let table_id = db.table_id("telemetry").unwrap();
        let index = db.chronicle_indices.get(&table_id).unwrap();
        let mut receipts = Vec::new();
        for sequence in 1_u8..=3 {
            let batch_id = BatchId::from_bytes([sequence; 16]).unwrap();
            let receipt = index.by_batch_id.get(&batch_id).unwrap();
            let bytes = fs::read(&receipt.path).unwrap();
            receipts.push(crate::compaction_manifest::ChronicleReceiptReference {
                sequence: receipt.metadata.sequence,
                batch_id: *receipt.metadata.batch_id.as_bytes(),
                payload_sha256: receipt.metadata.fingerprint,
                row_count: receipt.metadata.row_count,
                record_file_len: bytes.len() as u64,
                record_file_sha256: chronicle::sha256(&bytes),
            });
        }

        let capsule_path = |id| path.join("data").join("telemetry").join("capsules")
            .join(format!("capsule-{id:020}-g{:020}.cap", 1));
        let mut capsules = Vec::new();
        for (id, first, last) in [(10_u64, 1_u64, 2_u64), (11_u64, 3_u64, 3_u64)] {
            let metadata = db.validate_capsule("telemetry", id, 1).unwrap();
            let bytes = fs::read(capsule_path(id)).unwrap();
            capsules.push(crate::compaction_manifest::CapsuleSourceReference {
                capsule_id: metadata.capsule_id,
                generation: metadata.generation,
                schema_revision: metadata.schema_revision,
                first_sequence: first,
                last_sequence: last,
                row_count: metadata.row_count,
                file_len: bytes.len() as u64,
                file_sha256: chronicle::sha256(&bytes),
            });
        }

        let manifest = crate::compaction_manifest::CompactionInputManifest {
            table_id: table_id.0,
            job_id: CompactionJobId::from_bytes([77; 16]).unwrap(),
            attempt: 1,
            target_schema_revision: 1,
            first_sequence: 1,
            last_sequence: 3,
            expected_row_count: 3,
            capsules,
            receipts,
        };
        let report = db.preflight_compaction_inputs("telemetry", &manifest).unwrap();
        assert_eq!(report.table_id, table_id.0);
        assert_eq!(report.capsules_verified, 2);
        assert_eq!(report.chronicle_records_verified, 3);
        assert_eq!(report.total_row_count, 3);
        assert!(report.total_source_bytes > 0);
        assert_eq!(report.input_manifest_sha256, manifest.fingerprint().unwrap());

        drop(db);
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn compaction_preflight_rejects_a_modified_capsule_file() {
        let path = temp_path();
        let mut db = Database::open(&path).unwrap();
        db.create_table("telemetry", sample_schema()).unwrap();
        let batch_id = BatchId::from_bytes([33; 16]).unwrap();
        let rows = vec![vec![
            Value::TimestampMicros(1_000),
            Value::Utf8("sensor-a".into()),
            Value::Float64(33.0),
        ]];
        db.ingest_batch("telemetry", batch_id, &rows).unwrap();
        db.build_capsule("telemetry", 21, 1, 1, 1, None).unwrap();

        let table_id = db.table_id("telemetry").unwrap();
        let receipt = db.chronicle_indices.get(&table_id).unwrap()
            .by_batch_id.get(&batch_id).unwrap();
        let receipt_bytes = fs::read(&receipt.path).unwrap();
        let metadata = db.validate_capsule("telemetry", 21, 1).unwrap();
        let capsule_path = path.join("data").join("telemetry").join("capsules")
            .join(format!("capsule-{:020}-g{:020}.cap", 21, 1));
        let capsule_bytes = fs::read(&capsule_path).unwrap();
        let manifest = crate::compaction_manifest::CompactionInputManifest {
            table_id: table_id.0,
            job_id: CompactionJobId::from_bytes([78; 16]).unwrap(),
            attempt: 1,
            target_schema_revision: 1,
            first_sequence: 1,
            last_sequence: 1,
            expected_row_count: 1,
            capsules: vec![crate::compaction_manifest::CapsuleSourceReference {
                capsule_id: 21,
                generation: 1,
                schema_revision: metadata.schema_revision,
                first_sequence: 1,
                last_sequence: 1,
                row_count: metadata.row_count,
                file_len: capsule_bytes.len() as u64,
                file_sha256: chronicle::sha256(&capsule_bytes),
            }],
            receipts: vec![crate::compaction_manifest::ChronicleReceiptReference {
                sequence: 1,
                batch_id: *receipt.metadata.batch_id.as_bytes(),
                payload_sha256: receipt.metadata.fingerprint,
                row_count: receipt.metadata.row_count,
                record_file_len: receipt_bytes.len() as u64,
                record_file_sha256: chronicle::sha256(&receipt_bytes),
            }],
        };

        fs::write(&capsule_path, b"tampered").unwrap();
        assert!(matches!(
            db.preflight_compaction_inputs("telemetry", &manifest),
            Err(DbError::CompactionPreflight("source file length differs from the pinned manifest"))
        ));

        drop(db);
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn compaction_preflight_rejects_capsule_rows_with_valid_stats_but_wrong_origins() {
        let path = temp_path();
        let mut db = Database::open(&path).unwrap();
        db.create_table("telemetry", sample_schema()).unwrap();
        let source_rows = vec![
            vec![Value::TimestampMicros(1_000), Value::Utf8("a".into()), Value::Float64(1.0)],
            vec![Value::TimestampMicros(2_000), Value::Utf8("b".into()), Value::Float64(2.0)],
        ];
        let batch_id = BatchId::from_bytes([91; 16]).unwrap();
        db.ingest_batch("telemetry", batch_id, &source_rows).unwrap();
        db.build_capsule("telemetry", 31, 1, 1, 1, None).unwrap();

        let table_id = db.table_id("telemetry").unwrap();
        let receipt = db.chronicle_indices.get(&table_id).unwrap()
            .by_batch_id.get(&batch_id).unwrap();
        let receipt_bytes = fs::read(&receipt.path).unwrap();
        let receipt_reference = crate::compaction_manifest::ChronicleReceiptReference {
            sequence: 1,
            batch_id: *receipt.metadata.batch_id.as_bytes(),
            payload_sha256: receipt.metadata.fingerprint,
            row_count: receipt.metadata.row_count,
            record_file_len: receipt_bytes.len() as u64,
            record_file_sha256: chronicle::sha256(&receipt_bytes),
        };

        let capsule_path = path.join("data").join("telemetry").join("capsules")
            .join(format!("capsule-{:020}-g{:020}.cap", 31, 1));
        let (mut metadata, _) = decode_capsule(&fs::read(&capsule_path).unwrap()).unwrap();
        let mut reversed = source_rows.clone();
        reversed.reverse();
        let definition = db.catalog.tables.get("telemetry").unwrap();
        let column_ids = definition.column_ids.iter().map(|id| id.0).collect::<Vec<_>>();
        let payload = encode_segment_v2(
            &definition.schema, &reversed, table_id.0, definition.schema_revision, &column_ids,
        ).unwrap();
        let inspection = inspect_segment_v2_details(&payload).unwrap();
        metadata.columns = derive_column_stats(&definition.schema, &column_ids, &reversed, &inspection).unwrap();
        let forged_capsule = encode_capsule(metadata.clone(), &payload).unwrap();
        fs::write(&capsule_path, &forged_capsule).unwrap();

        let manifest = crate::compaction_manifest::CompactionInputManifest {
            table_id: table_id.0,
            job_id: CompactionJobId::from_bytes([92; 16]).unwrap(),
            attempt: 1,
            target_schema_revision: 1,
            first_sequence: 1,
            last_sequence: 1,
            expected_row_count: 2,
            capsules: vec![crate::compaction_manifest::CapsuleSourceReference {
                capsule_id: 31,
                generation: 1,
                schema_revision: metadata.schema_revision,
                first_sequence: 1,
                last_sequence: 1,
                row_count: 2,
                file_len: forged_capsule.len() as u64,
                file_sha256: chronicle::sha256(&forged_capsule),
            }],
            receipts: vec![receipt_reference],
        };

        assert!(matches!(
            db.preflight_compaction_inputs("telemetry", &manifest),
            Err(DbError::CompactionPreflight(
                "Capsule rows differ from the Chronicle source rows at their declared origins"
            ))
        ));
        drop(db);
        fs::remove_dir_all(path).unwrap();
    }


    #[test]
    fn database_wal_persists_anf_and_chronicle_writes_across_reopen() {
        let path = temp_path();
        let batch_id = BatchId::from_bytes([0xA1; 16]).unwrap();
        let anf_row = vec![
            Value::TimestampMicros(1_000),
            Value::Utf8("anf-device".into()),
            Value::Float64(21.5),
        ];
        let chronicle_rows = vec![vec![
            Value::TimestampMicros(2_000),
            Value::Utf8("chronicle-device".into()),
            Value::Float64(22.5),
        ]];

        let mut database = Database::open(&path).unwrap();
        database.create_table("telemetry", sample_schema()).unwrap();
        let anf_path = database.insert_batch("telemetry", &[anf_row.clone()])
            .unwrap().unwrap();
        assert!(anf_path.is_file());
        let ingested = database.ingest_batch("telemetry", batch_id, &chronicle_rows).unwrap();
        assert_eq!(ingested.outcome, IngestOutcome::Accepted);
        assert!(ingested.chronicle_path.is_file());
        drop(database);

        let wal_path = path.join("wal").join("database.anwl");
        let report = read_wal(&wal_path).unwrap();
        assert_eq!(
            report.records.iter().filter(|record| record.kind == WalRecordKind::Commit).count(),
            2,
        );
        assert!(report.is_clean());

        let mut reopened = Database::open(&path).unwrap();
        let rows = reopened.scan("telemetry").unwrap();
        assert_eq!(rows.len(), 2);
        assert!(rows.iter().any(|row| row[1] == Value::Utf8("anf-device".into())));
        assert!(rows.iter().any(|row| row[1] == Value::Utf8("chronicle-device".into())));
        let stats = reopened.ingestion_stats("telemetry").unwrap();
        assert_eq!(stats.accepted_batches, 1);
        assert_eq!(stats.accepted_rows, 1);

        let duplicate = reopened.ingest_batch("telemetry", batch_id, &chronicle_rows).unwrap();
        assert_eq!(duplicate.outcome, IngestOutcome::Duplicate);
        assert_eq!(duplicate.chronicle_path, ingested.chronicle_path);

        drop(reopened);
        let reopened_again = Database::open(&path).unwrap();
        assert_eq!(reopened_again.scan("telemetry").unwrap(), vec![
            anf_row,
            chronicle_rows[0].clone(),
        ]);
        assert_eq!(reopened_again.ingestion_stats("telemetry").unwrap().accepted_batches, 1);
        drop(reopened_again);
        fs::remove_dir_all(path).unwrap();
    }


    #[test]
    fn wal_recovery_process_crash_worker() {
        let Some(operation) = std::env::var_os("ANANTA_WAL_CRASH_OPERATION") else {
            return;
        };
        let operation = operation.to_string_lossy().into_owned();
        let point_name = std::env::var("ANANTA_WAL_CRASH_POINT").unwrap();
        use crate::wal_recovery::WalRecoveryCrashPoint::*;
        let point = match point_name.as_str() {
            "AfterStagingFileCreated" => AfterStagingFileCreated,
            "AfterStagingBytesWritten" => AfterStagingBytesWritten,
            "AfterStagingFlush" => AfterStagingFlush,
            "AfterStagingFileSync" => AfterStagingFileSync,
            "AfterStagingDirectorySync" => AfterStagingDirectorySync,
            "AfterWalBegin" => AfterWalBegin,
            "AfterWalMutation" => AfterWalMutation,
            "AfterWalCommit" => AfterWalCommit,
            "AfterFinalNameLinked" => AfterFinalNameLinked,
            "AfterPublishDirectorySync" => AfterPublishDirectorySync,
            "AfterStagingNameRemoved" => AfterStagingNameRemoved,
            "AfterCleanupDirectorySync" => AfterCleanupDirectorySync,
            "AfterSchemaSnapshotPublished" => AfterSchemaSnapshotPublished,
            "AfterCatalogSnapshotPublished" => AfterCatalogSnapshotPublished,
            "AfterCanonicalCatalogPublished" => AfterCanonicalCatalogPublished,
            "AfterCanonicalCatalogTempSynced" => AfterCanonicalCatalogTempSynced,
            "AfterCanonicalCatalogRenamedBeforeDirectorySync" => AfterCanonicalCatalogRenamedBeforeDirectorySync,
            "AfterCheckpointManifestTempSynced" => AfterCheckpointManifestTempSynced,
            "AfterCheckpointManifestPublished" => AfterCheckpointManifestPublished,
            "AfterCheckpointTempSynced" => AfterCheckpointTempSynced,
            "AfterCheckpointRenamedBeforeDirectorySync" => AfterCheckpointRenamedBeforeDirectorySync,
            "AfterCheckpointDirectorySync" => AfterCheckpointDirectorySync,
            "AfterBackupFileSynced" => AfterBackupFileSynced,
            "AfterBackupManifestSynced" => AfterBackupManifestSynced,
            "AfterBackupDirectoryRenamedBeforeSync" => AfterBackupDirectoryRenamedBeforeSync,
            _ => panic!("unsupported subprocess crash point: {point_name}"),
        };
        let path = PathBuf::from(std::env::var_os("ANANTA_WAL_CRASH_DATABASE").unwrap());
        if operation == "lock-probe" {
            match Database::open(&path) {
                Err(DbError::DatabaseLocked(_)) => return,
                Err(error) => panic!("unexpected lock probe error: {error}"),
                Ok(_) => panic!("a second process unexpectedly acquired the exclusive writer lock"),
            }
        }
        // Open and replay pre-existing transactions before arming the hook:
        // the checkpoint must target the operation under test, not startup.
        let mut database = Database::open(path).unwrap();
        crate::wal_recovery::arm_wal_recovery_crash_point(point);
        let rows = vec![vec![
            Value::TimestampMicros(9_000),
            Value::Utf8(format!("subprocess-{operation}-{point_name}")),
            Value::Float64(29.5),
        ]];
        match operation.as_str() {
            "anf" => {
                database.insert_batch("telemetry", &rows).unwrap();
            }
            "chronicle" => {
                database.ingest_batch(
                    "telemetry",
                    BatchId::from_bytes([0xD4; 16]).unwrap(),
                    &rows,
                ).unwrap();
            }
            "ddl" => {
                database.add_nullable_column("telemetry", "humidity", DataType::Float64).unwrap();
            }
            "capsule" => {
                database.build_capsule("telemetry", 900, 1, 1, 1, Some("timestamp")).unwrap();
            }
            "navigation-map" => {
                database.build_navigation_map("telemetry", 901).unwrap();
            }
            "checkpoint" => {
                database.create_checkpoint().unwrap();
            }
            "backup" => {
                let destination = PathBuf::from(std::env::var_os("ANANTA_WAL_CRASH_BACKUP").unwrap());
                database.backup_to(destination).unwrap();
            }
            _ => panic!("unsupported subprocess operation: {operation}"),
        }
        panic!("subprocess crash checkpoint was not reached");
    }

    fn exercise_wal_recovery_subprocess_crash(
        operation: &str,
        point: &str,
        committed: bool,
    ) {
        use std::process::Command;

        let path = temp_path();
        let mut setup = Database::open(&path).unwrap();
        setup.create_table("telemetry", sample_schema()).unwrap();
        drop(setup);

        let status = Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("tests::wal_recovery_process_crash_worker")
            .arg("--nocapture")
            .env("ANANTA_WAL_CRASH_OPERATION", operation)
            .env("ANANTA_WAL_CRASH_POINT", point)
            .env("ANANTA_WAL_CRASH_DATABASE", &path)
            .env("ANANTA_WAL_CRASH_EXIT", "1")
            .status()
            .unwrap();
        assert_eq!(status.code(), Some(86), "worker did not terminate at {operation}/{point}");

        // The child has exited, so no process can own this test database.
        // Its lock file is intentionally stale because process::exit skipped Drop.
        fs::remove_file(path.join(".ananta.lock")).unwrap();

        let table_dir = path.join("data").join("telemetry");
        let rows = if committed {
            vec![vec![
                Value::TimestampMicros(9_000),
                Value::Utf8(format!("subprocess-{operation}-{point}")),
                Value::Float64(29.5),
            ]]
        } else {
            Vec::new()
        };
        let mut recovered = Database::open(&path).unwrap();
        assert_eq!(
            recovered.scan("telemetry").unwrap(),
            rows,
            "wrong recovered rows after {operation}/{point}",
        );
        let extension = if operation == "anf" { "anf" } else { "chr" };
        assert_recovered_artifact_inventory(&table_dir, extension, committed);
        if operation == "chronicle" {
            let stats = recovered.ingestion_stats("telemetry").unwrap();
            assert_eq!(stats.accepted_batches, if committed { 1 } else { 0 });
            assert_eq!(stats.accepted_rows, if committed { 1 } else { 0 });
            if committed {
                let duplicate = recovered.ingest_batch(
                    "telemetry",
                    BatchId::from_bytes([0xD4; 16]).unwrap(),
                    &rows,
                ).unwrap();
                assert_eq!(duplicate.outcome, IngestOutcome::Duplicate);
                assert_eq!(duplicate.ingestion_sequence, 1);
            }
        }
        drop(recovered);

        // A second process-level reopen must preserve the same state.
        let recovered_again = Database::open(&path).unwrap();
        assert_eq!(recovered_again.scan("telemetry").unwrap(), rows);
        assert_recovered_artifact_inventory(&table_dir, extension, committed);
        drop(recovered_again);
        fs::remove_dir_all(path).unwrap();
    }

    fn exercise_catalog_ddl_subprocess_crash(point: &str) {
        use std::process::Command;

        let path = temp_path();
        let mut setup = Database::open(&path).unwrap();
        setup.create_table("telemetry", sample_schema()).unwrap();
        drop(setup);

        let status = Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("tests::wal_recovery_process_crash_worker")
            .arg("--nocapture")
            .env("ANANTA_WAL_CRASH_OPERATION", "ddl")
            .env("ANANTA_WAL_CRASH_POINT", point)
            .env("ANANTA_WAL_CRASH_DATABASE", &path)
            .env("ANANTA_WAL_CRASH_EXIT", "1")
            .status()
            .unwrap();
        assert_eq!(status.code(), Some(86), "DDL worker did not terminate at {point}");

        // The child exited without running DatabaseLock::drop.
        fs::remove_file(path.join(".ananta.lock")).unwrap();

        let recovered = Database::open(&path).unwrap();
        assert_eq!(recovered.catalog_revision(), 2, "at {point}");
        assert_eq!(recovered.table_schema_revision("telemetry").unwrap(), 2, "at {point}");
        assert_eq!(recovered.schema("telemetry").unwrap().len(), 4, "at {point}");
        assert_eq!(recovered.column_id("telemetry", "humidity").unwrap().0, 5);
        assert!(recovered.scan("telemetry").unwrap().is_empty());
        assert!(recovered.check_schema_snapshot_consistency().unwrap().is_clean());
        assert!(SchemaSnapshot::path(
            &path,
            recovered.table_id("telemetry").unwrap(),
            2,
        ).unwrap().is_file());
        assert!(path.join("catalog").join("catalog-00000000000000000002.anc").is_file());
        drop(recovered);

        let reopened = Database::open(&path).unwrap();
        assert_eq!(reopened.catalog_revision(), 2);
        assert_eq!(reopened.table_schema_revision("telemetry").unwrap(), 2);
        assert_eq!(reopened.schema("telemetry").unwrap().len(), 4);
        assert!(reopened.check_schema_snapshot_consistency().unwrap().is_clean());
        drop(reopened);
        fs::remove_dir_all(path).unwrap();
    }

    fn exercise_derived_artifact_subprocess_crash(
        operation: &str,
        point: &str,
        committed: bool,
    ) {
        use std::process::Command;

        let path = temp_path();
        let rows = vec![vec![
            Value::TimestampMicros(9_000),
            Value::Utf8(format!("subprocess-{operation}-{point}")),
            Value::Float64(29.5),
        ]];
        let mut setup = Database::open(&path).unwrap();
        setup.create_table("telemetry", sample_schema()).unwrap();
        setup.ingest_batch(
            "telemetry",
            BatchId::from_bytes([0xD4; 16]).unwrap(),
            &rows,
        ).unwrap();
        if operation == "navigation-map" {
            setup.build_capsule("telemetry", 900, 1, 1, 1, Some("timestamp")).unwrap();
        }
        drop(setup);

        let status = Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("tests::wal_recovery_process_crash_worker")
            .arg("--nocapture")
            .env("ANANTA_WAL_CRASH_OPERATION", operation)
            .env("ANANTA_WAL_CRASH_POINT", point)
            .env("ANANTA_WAL_CRASH_DATABASE", &path)
            .env("ANANTA_WAL_CRASH_EXIT", "1")
            .status()
            .unwrap();
        assert_eq!(status.code(), Some(86), "worker did not terminate at {operation}/{point}");
        fs::remove_file(path.join(".ananta.lock")).unwrap();

        let capsules = path.join("data").join("telemetry").join("capsules");
        let expected_artifact = if operation == "capsule" {
            capsules.join("capsule-00000000000000000900-g00000000000000000001.cap")
        } else {
            capsules.join("navigation-00000000000000000901.nvm")
        };
        assert_eq!(expected_artifact.is_file(), committed, "publication state at {operation}/{point}");

        let recovered = Database::open(&path).unwrap();
        assert_eq!(recovered.scan("telemetry").unwrap(), rows);
        if committed && operation == "capsule" {
            let metadata = recovered.validate_capsule("telemetry", 900, 1).unwrap();
            assert_eq!(metadata.row_count, 1);
            assert_eq!(recovered.scan_capsule("telemetry", 900, 1).unwrap(), rows);
        }
        if committed && operation == "navigation-map" {
            let request = CapsuleScanRequest {
                ranges: vec![ColumnRangePredicate::new(
                    "timestamp",
                    Some(Value::TimestampMicros(0)),
                    true,
                    Some(Value::TimestampMicros(10_000)),
                    true,
                )],
                projection: None,
            };
            let mapped = recovered.scan_capsules("telemetry", &request, Some(901)).unwrap();
            assert_eq!(mapped.rows, rows);
        }
        let consistency = recovered.check_consistency(1).unwrap();
        assert_eq!(consistency.rows_checked, 1);
        drop(recovered);

        let reopened = Database::open(&path).unwrap();
        assert_eq!(reopened.scan("telemetry").unwrap(), rows);
        reopened.check_consistency(1).unwrap();
        drop(reopened);
        fs::remove_dir_all(path).unwrap();
    }

    fn exercise_checkpoint_process_crash(point: &str, rotated: bool) {
        use std::process::Command;

        let path = temp_path();
        let rows = vec![vec![
            Value::TimestampMicros(1_000),
            Value::Utf8("checkpoint-source".into()),
            Value::Float64(15.0),
        ]];
        let mut setup = Database::open(&path).unwrap();
        setup.create_table("telemetry", sample_schema()).unwrap();
        setup.ingest_batch(
            "telemetry",
            BatchId::from_bytes([0xC1; 16]).unwrap(),
            &rows,
        ).unwrap();
        drop(setup);

        let status = Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("tests::wal_recovery_process_crash_worker")
            .arg("--nocapture")
            .env("ANANTA_WAL_CRASH_OPERATION", "checkpoint")
            .env("ANANTA_WAL_CRASH_POINT", point)
            .env("ANANTA_WAL_CRASH_DATABASE", &path)
            .env("ANANTA_WAL_CRASH_EXIT", "1")
            .status()
            .unwrap();
        assert_eq!(status.code(), Some(86), "checkpoint worker did not terminate at {point}");
        fs::remove_file(path.join(".ananta.lock")).unwrap();

        let mut recovered = Database::open(&path).unwrap();
        assert_eq!(recovered.scan("telemetry").unwrap(), rows);
        let wal_report = read_wal(path.join("wal").join("database.anwl")).unwrap();
        assert_eq!(
            wal_report.records.first().map(|record| record.kind == WalRecordKind::Checkpoint).unwrap_or(false),
            rotated,
            "checkpoint rotation state at {point}",
        );
        recovered.check_consistency(1).unwrap();
        drop(recovered);

        let reopened = Database::open(&path).unwrap();
        assert_eq!(reopened.scan("telemetry").unwrap(), rows);
        reopened.check_consistency(1).unwrap();
        drop(reopened);
        fs::remove_dir_all(path).unwrap();
    }

    fn exercise_backup_process_crash(point: &str, published: bool) {
        use std::process::Command;

        let root = temp_path();
        let backup = temp_path();
        let rows = vec![vec![
            Value::TimestampMicros(3_000),
            Value::Utf8("backup-crash-source".into()),
            Value::Float64(18.0),
        ]];
        let mut setup = Database::open(&root).unwrap();
        setup.create_table("telemetry", sample_schema()).unwrap();
        setup.ingest_batch(
            "telemetry",
            BatchId::from_bytes([0xB4; 16]).unwrap(),
            &rows,
        ).unwrap();
        drop(setup);

        let status = Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("tests::wal_recovery_process_crash_worker")
            .arg("--nocapture")
            .env("ANANTA_WAL_CRASH_OPERATION", "backup")
            .env("ANANTA_WAL_CRASH_POINT", point)
            .env("ANANTA_WAL_CRASH_DATABASE", &root)
            .env("ANANTA_WAL_CRASH_BACKUP", &backup)
            .env("ANANTA_WAL_CRASH_EXIT", "1")
            .status()
            .unwrap();
        assert_eq!(status.code(), Some(86), "backup worker did not terminate at {point}");
        fs::remove_file(root.join(".ananta.lock")).unwrap();

        assert_eq!(backup.is_dir(), published, "backup publication state at {point}");
        if published {
            crate::backup::verify_backup(&backup).unwrap();
            let restored = temp_path();
            let report = Database::restore_backup(&backup, &restored).unwrap();
            assert!(report.file_count >= 4);
            let restored_db = Database::open(&restored).unwrap();
            assert_eq!(restored_db.scan("telemetry").unwrap(), rows);
            drop(restored_db);
            fs::remove_dir_all(restored).unwrap();
        } else {
            let parent = backup.parent().unwrap_or(Path::new("."));
            let prefix = format!(".{}.backup-", backup.file_name().unwrap().to_string_lossy());
            for entry in fs::read_dir(parent).unwrap().filter_map(Result::ok) {
                let name = entry.file_name().to_string_lossy().into_owned();
                if name.starts_with(&prefix) {
                    fs::remove_dir_all(entry.path()).unwrap();
                }
            }
        }
        let source = Database::open(&root).unwrap();
        assert_eq!(source.scan("telemetry").unwrap(), rows);
        source.check_consistency(1).unwrap();
        drop(source);
        if published {
            fs::remove_dir_all(&backup).unwrap();
        }
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn backup_publication_survives_process_crashes() {
        exercise_backup_process_crash("AfterBackupFileSynced", false);
        exercise_backup_process_crash("AfterBackupManifestSynced", false);
        exercise_backup_process_crash("AfterBackupDirectoryRenamedBeforeSync", true);
    }

    #[test]
    fn second_process_cannot_acquire_database_writer_lock() {
        use std::process::Command;

        let root = temp_path();
        let database = Database::open(&root).unwrap();
        for _ in 0..5 {
            let status = Command::new(std::env::current_exe().unwrap())
                .arg("--exact")
                .arg("tests::wal_recovery_process_crash_worker")
                .arg("--nocapture")
                .env("ANANTA_WAL_CRASH_OPERATION", "lock-probe")
                .env("ANANTA_WAL_CRASH_POINT", "AfterWalCommit")
                .env("ANANTA_WAL_CRASH_DATABASE", &root)
                .status()
                .unwrap();
            assert!(status.success(), "second process did not observe DatabaseLocked");
        }
        assert!(Database::inspect_lock(&root).unwrap().is_locked);
        drop(database);
        assert!(!Database::inspect_lock(&root).unwrap().is_locked);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn checkpoint_wal_replacement_survives_subprocess_crashes() {
        exercise_checkpoint_process_crash("AfterCheckpointManifestTempSynced", false);
        exercise_checkpoint_process_crash("AfterCheckpointManifestPublished", false);
        exercise_checkpoint_process_crash("AfterCheckpointTempSynced", false);
        exercise_checkpoint_process_crash("AfterCheckpointRenamedBeforeDirectorySync", true);
        exercise_checkpoint_process_crash("AfterCheckpointDirectorySync", true);
    }

    #[test]
    fn database_checkpoint_rotates_wal_and_replays_subsequent_commits() {
        let path = temp_path();
        let first_rows = vec![vec![
            Value::TimestampMicros(1),
            Value::Utf8("before-checkpoint".into()),
            Value::Float64(1.0),
        ]];
        let second_rows = vec![vec![
            Value::TimestampMicros(2),
            Value::Utf8("after-checkpoint".into()),
            Value::Float64(2.0),
        ]];
        let mut db = Database::open(&path).unwrap();
        db.create_table("telemetry", sample_schema()).unwrap();
        db.ingest_batch(
            "telemetry",
            BatchId::from_bytes([0xC2; 16]).unwrap(),
            &first_rows,
        ).unwrap();
        let previous = read_wal(path.join("wal").join("database.anwl")).unwrap();
        assert!(previous.last_sequence > 1);

        let checkpoint = db.create_checkpoint().unwrap();
        assert_eq!(checkpoint.base_sequence, previous.last_sequence);
        assert!(checkpoint.manifest_path.is_file());
        let rotated = read_wal(path.join("wal").join("database.anwl")).unwrap();
        assert_eq!(rotated.records.len(), 1);
        assert_eq!(rotated.records[0].kind, WalRecordKind::Checkpoint);
        assert_eq!(rotated.records[0].sequence, checkpoint.base_sequence + 1);
        assert!(rotated.is_clean());

        db.ingest_batch(
            "telemetry",
            BatchId::from_bytes([0xC3; 16]).unwrap(),
            &second_rows,
        ).unwrap();
        drop(db);

        let second_checkpoint = reopened.create_checkpoint().unwrap();
        assert_eq!(second_checkpoint.pruned_manifest_count, 1);
        assert!(!checkpoint.manifest_path.exists());
        assert!(second_checkpoint.manifest_path.is_file());
        let reopened = Database::open(&path).unwrap();
        assert_eq!(reopened.scan("telemetry").unwrap(), vec![first_rows[0].clone(), second_rows[0].clone()]);
        let consistency = reopened.check_consistency(1).unwrap();
        assert_eq!(consistency.rows_checked, 2);
        drop(reopened);
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn checkpoint_replays_catalog_changes_and_revalidates_mutable_state() {
        let path = temp_path();
        let mut db = Database::open(&path).unwrap();
        db.create_table("telemetry", sample_schema()).unwrap();
        let checkpoint = db.create_checkpoint().unwrap();
        assert!(checkpoint.manifest_path.is_file());

        db.add_nullable_column("telemetry", "humidity", DataType::Float64).unwrap();
        drop(db);

        let reopened = Database::open(&path).unwrap();
        assert_eq!(reopened.table_schema_revision("telemetry").unwrap(), 2);
        assert_eq!(reopened.schema("telemetry").unwrap().len(), 4);
        reopened.check_consistency(1).unwrap();
        drop(reopened);
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn checkpoint_manifest_mismatch_fails_database_open_closed() {
        let path = temp_path();
        let mut db = Database::open(&path).unwrap();
        db.create_table("telemetry", sample_schema()).unwrap();
        let row = vec![vec![
            Value::TimestampMicros(50),
            Value::Utf8("checkpoint-integrity".into()),
            Value::Float64(5.0),
        ]];
        db.insert_batch("telemetry", &row).unwrap();
        let checkpoint = db.create_checkpoint().unwrap();
        drop(db);

        let segment = fs::read_dir(path.join("data").join("telemetry")).unwrap()
            .filter_map(Result::ok)
            .map(|entry| entry.path())
            .find(|candidate| candidate.extension().and_then(|value| value.to_str()) == Some("anf"))
            .unwrap();
        let mut bytes = fs::read(&segment).unwrap();
        let last = bytes.len() - 1;
        bytes[last] ^= 1;
        fs::write(&segment, bytes).unwrap();
        assert!(matches!(Database::open(&path), Err(DbError::Wal(_))));
        assert!(checkpoint.manifest_path.is_file());
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn capsule_and_navigation_map_wal_recovery_survives_process_crashes() {
        for operation in ["capsule", "navigation-map"] {
            for (point, committed) in [
                ("AfterStagingFileCreated", false),
                ("AfterStagingBytesWritten", false),
                ("AfterStagingFlush", false),
                ("AfterStagingFileSync", false),
                ("AfterStagingDirectorySync", false),
                ("AfterWalBegin", false),
                ("AfterWalMutation", false),
                ("AfterWalCommit", true),
                ("AfterFinalNameLinked", true),
                ("AfterPublishDirectorySync", true),
                ("AfterStagingNameRemoved", true),
                ("AfterCleanupDirectorySync", true),
            ] {
                exercise_derived_artifact_subprocess_crash(operation, point, committed);
            }
        }
    }

    #[test]
    fn catalog_ddl_recovery_survives_subprocess_termination_at_publication_boundaries() {
        for point in [
            "AfterWalCommit",
            "AfterSchemaSnapshotPublished",
            "AfterCatalogSnapshotPublished",
            "AfterCanonicalCatalogTempSynced",
            "AfterCanonicalCatalogRenamedBeforeDirectorySync",
            "AfterCanonicalCatalogPublished",
        ] {
            exercise_catalog_ddl_subprocess_crash(point);
        }
    }

    #[test]
    fn database_recovery_survives_subprocess_termination_around_wal_commit_and_publication() {
        for operation in ["anf", "chronicle"] {
            exercise_wal_recovery_subprocess_crash(operation, "AfterWalMutation", false);
            exercise_wal_recovery_subprocess_crash(operation, "AfterWalCommit", true);
            exercise_wal_recovery_subprocess_crash(operation, "AfterFinalNameLinked", true);
        }
    }

    fn assert_recovered_artifact_inventory(
        table_dir: &Path,
        extension: &str,
        committed: bool,
    ) {
        let mut final_count = 0_usize;
        let mut staging_count = 0_usize;
        for entry in fs::read_dir(table_dir).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            let filename = entry.file_name();
            let filename = filename.to_string_lossy();
            if filename.ends_with(".tmp") {
                staging_count += 1;
            }
            if path.extension().and_then(|value| value.to_str()) == Some(extension) {
                final_count += 1;
            }
        }
        assert_eq!(final_count, if committed { 1 } else { 0 });
        assert_eq!(staging_count, 0, "staging files must be removed after recovery");
    }

    fn exercise_anf_wal_crash_point(
        point: crate::wal_recovery::WalRecoveryCrashPoint,
        committed: bool,
    ) {
        let path = temp_path();
        let mut database = Database::open(&path).unwrap();
        database.create_table("telemetry", sample_schema()).unwrap();
        let rows = vec![vec![
            Value::TimestampMicros(7_000),
            Value::Utf8(format!("anf-crash-{point:?}")),
            Value::Float64(27.5),
        ]];

        crate::wal_recovery::arm_wal_recovery_crash_point(point);
        let interruption = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = database.insert_batch("telemetry", &rows);
        }));
        assert!(interruption.is_err(), "crash point {point:?} was not reached");
        drop(database);

        let table_dir = path.join("data").join("telemetry");
        let expected = if committed { rows.clone() } else { Vec::new() };
        let recovered = Database::open(&path).unwrap();
        assert_eq!(recovered.scan("telemetry").unwrap(), expected, "at {point:?}");
        assert_recovered_artifact_inventory(&table_dir, "anf", committed);
        drop(recovered);

        // A second restart must not publish a duplicate file or duplicate rows.
        let recovered_again = Database::open(&path).unwrap();
        assert_eq!(recovered_again.scan("telemetry").unwrap(), expected, "second open at {point:?}");
        assert_recovered_artifact_inventory(&table_dir, "anf", committed);
        drop(recovered_again);
        fs::remove_dir_all(path).unwrap();
    }

    fn exercise_chronicle_wal_crash_point(
        point: crate::wal_recovery::WalRecoveryCrashPoint,
        committed: bool,
    ) {
        let path = temp_path();
        let mut database = Database::open(&path).unwrap();
        database.create_table("telemetry", sample_schema()).unwrap();
        let batch_id = BatchId::from_bytes([0xC3; 16]).unwrap();
        let rows = vec![vec![
            Value::TimestampMicros(8_000),
            Value::Utf8(format!("chronicle-crash-{point:?}")),
            Value::Float64(28.5),
        ]];

        crate::wal_recovery::arm_wal_recovery_crash_point(point);
        let interruption = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = database.ingest_batch("telemetry", batch_id, &rows);
        }));
        assert!(interruption.is_err(), "crash point {point:?} was not reached");
        drop(database);

        let table_dir = path.join("data").join("telemetry");
        let expected = if committed { rows.clone() } else { Vec::new() };
        let mut recovered = Database::open(&path).unwrap();
        assert_eq!(recovered.scan("telemetry").unwrap(), expected, "at {point:?}");
        let stats = recovered.ingestion_stats("telemetry").unwrap();
        assert_eq!(stats.accepted_batches, if committed { 1 } else { 0 });
        assert_eq!(stats.accepted_rows, if committed { 1 } else { 0 });
        assert_recovered_artifact_inventory(&table_dir, "chr", committed);

        if committed {
            let duplicate = recovered.ingest_batch("telemetry", batch_id, &rows).unwrap();
            assert_eq!(duplicate.outcome, IngestOutcome::Duplicate);
            assert_eq!(duplicate.ingestion_sequence, 1);
        }
        drop(recovered);

        // Receipt reconstruction and duplicate resolution must survive another restart.
        let recovered_again = Database::open(&path).unwrap();
        assert_eq!(recovered_again.scan("telemetry").unwrap(), expected);
        assert_eq!(
            recovered_again.ingestion_stats("telemetry").unwrap().accepted_batches,
            if committed { 1 } else { 0 },
        );
        assert_recovered_artifact_inventory(&table_dir, "chr", committed);
        drop(recovered_again);
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn database_wal_recovery_survives_every_anf_write_and_publication_interruption() {
        use crate::wal_recovery::WalRecoveryCrashPoint::*;
        for (point, committed) in [
            (AfterStagingFileCreated, false),
            (AfterStagingBytesWritten, false),
            (AfterStagingFlush, false),
            (AfterStagingFileSync, false),
            (AfterStagingDirectorySync, false),
            (AfterWalBegin, false),
            (AfterWalMutation, false),
            (AfterWalCommit, true),
            (AfterFinalNameLinked, true),
            (AfterPublishDirectorySync, true),
            (AfterStagingNameRemoved, true),
            (AfterCleanupDirectorySync, true),
        ] {
            exercise_anf_wal_crash_point(point, committed);
        }
    }

    #[test]
    fn database_wal_recovery_survives_every_chronicle_write_and_publication_interruption() {
        use crate::wal_recovery::WalRecoveryCrashPoint::*;
        for (point, committed) in [
            (AfterStagingFileCreated, false),
            (AfterStagingBytesWritten, false),
            (AfterStagingFlush, false),
            (AfterStagingFileSync, false),
            (AfterStagingDirectorySync, false),
            (AfterWalBegin, false),
            (AfterWalMutation, false),
            (AfterWalCommit, true),
            (AfterFinalNameLinked, true),
            (AfterPublishDirectorySync, true),
            (AfterStagingNameRemoved, true),
            (AfterCleanupDirectorySync, true),
        ] {
            exercise_chronicle_wal_crash_point(point, committed);
        }
    }

    #[test]
    fn database_open_replays_committed_staged_anf_before_scanning() {
        let path = temp_path();
        let mut database = Database::open(&path).unwrap();
        database.create_table("telemetry", sample_schema()).unwrap();
        let rows = vec![vec![
            Value::TimestampMicros(3_000),
            Value::Utf8("replayed-device".into()),
            Value::Float64(23.5),
        ]];
        let definition = database.catalog.tables.get("telemetry").unwrap().clone();
        let column_ids = definition.column_ids.iter().map(|id| id.0).collect::<Vec<_>>();
        let bytes = encode_segment_v2(
            &definition.schema,
            &rows,
            definition.id.0,
            definition.schema_revision,
            &column_ids,
        ).unwrap();
        let table_dir = path.join("data").join("telemetry");
        let file_id = next_segment_id(&table_dir).unwrap();
        let intent = WalMutationIntent {
            artifact_kind: WalArtifactKind::AnfSegment,
            table_id: definition.id.0,
            schema_revision: definition.schema_revision,
            file_id,
            file_len: bytes.len() as u64,
            sha256: chronicle::sha256(&bytes),
        };
        let staging = stage_wal_artifact(&table_dir, &intent, &bytes).unwrap();
        database.commit_wal_intent(&intent, b"fault-injected committed ANF insert").unwrap();
        assert!(staging.is_file());
        drop(database); // Simulates a stop after durable COMMIT but before final publication.

        let reopened = Database::open(&path).unwrap();
        let actual_rows = reopened.scan("telemetry").unwrap();
        assert_eq!(actual_rows, rows);
        assert_eq!(actual_rows.len(), 1);
        assert_eq!(actual_rows[0][1], Value::Utf8("replayed-device".into()));
        assert!(table_dir.join(intent.artifact_kind.final_file_name(file_id)).is_file());
        assert!(!staging.exists());

        drop(reopened);
        let reopened_again = Database::open(&path).unwrap();
        assert_eq!(reopened_again.scan("telemetry").unwrap(), rows);
        drop(reopened_again);
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn database_open_replays_committed_staged_chronicle_before_rebuilding_index() {
        let path = temp_path();
        let mut database = Database::open(&path).unwrap();
        database.create_table("telemetry", sample_schema()).unwrap();
        let batch_id = BatchId::from_bytes([0xB2; 16]).unwrap();
        let rows = vec![vec![
            Value::TimestampMicros(5_000),
            Value::Utf8("replayed-chronicle-device".into()),
            Value::Float64(25.5),
        ]];

        let definition = database.catalog.tables.get("telemetry").unwrap().clone();
        let column_ids = definition.column_ids.iter().map(|id| id.0).collect::<Vec<_>>();
        let payload = encode_segment_v2(
            &definition.schema,
            &rows,
            definition.id.0,
            definition.schema_revision,
            &column_ids,
        ).unwrap();
        let sequence = database.chronicle_indices.get(&definition.id).unwrap()
            .next_sequence().unwrap();
        let (record_bytes, _metadata) = chronicle::encode_record(
            definition.id.0,
            batch_id,
            sequence,
            definition.schema_revision,
            rows.len() as u64,
            &payload,
        ).unwrap();
        let table_dir = path.join("data").join("telemetry");
        let file_id = next_segment_id(&table_dir).unwrap();
        let intent = WalMutationIntent {
            artifact_kind: WalArtifactKind::ChronicleRecord,
            table_id: definition.id.0,
            schema_revision: definition.schema_revision,
            file_id,
            file_len: record_bytes.len() as u64,
            sha256: chronicle::sha256(&record_bytes),
        };
        let staging = stage_wal_artifact(&table_dir, &intent, &record_bytes).unwrap();
        database.commit_wal_intent(&intent, b"fault-injected committed Chronicle ingest").unwrap();
        assert!(staging.is_file());
        drop(database); // Crash after durable COMMIT and before Chronicle final-name publication.

        let mut reopened = Database::open(&path).unwrap();
        let stats = reopened.ingestion_stats("telemetry").unwrap();
        assert_eq!(stats.accepted_batches, 1);
        assert_eq!(stats.accepted_rows, 1);
        assert_eq!(reopened.scan("telemetry").unwrap(), rows);
        let duplicate = reopened.ingest_batch("telemetry", batch_id, &rows).unwrap();
        assert_eq!(duplicate.outcome, IngestOutcome::Duplicate);
        assert_eq!(duplicate.ingestion_sequence, 1);
        assert!(!staging.exists());
        assert!(table_dir.join(intent.artifact_kind.final_file_name(file_id)).is_file());

        drop(reopened);
        let reopened_again = Database::open(&path).unwrap();
        assert_eq!(reopened_again.scan("telemetry").unwrap(), rows);
        assert_eq!(reopened_again.ingestion_stats("telemetry").unwrap().accepted_batches, 1);
        drop(reopened_again);
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn database_open_truncates_torn_commit_and_discards_uncommitted_stage() {
        let path = temp_path();
        let mut database = Database::open(&path).unwrap();
        database.create_table("telemetry", sample_schema()).unwrap();
        let definition = database.catalog.tables.get("telemetry").unwrap().clone();
        let rows = vec![vec![
            Value::TimestampMicros(6_000),
            Value::Utf8("torn-commit-device".into()),
            Value::Float64(26.5),
        ]];
        let column_ids = definition.column_ids.iter().map(|id| id.0).collect::<Vec<_>>();
        let bytes = encode_segment_v2(
            &definition.schema,
            &rows,
            definition.id.0,
            definition.schema_revision,
            &column_ids,
        ).unwrap();
        let table_dir = path.join("data").join("telemetry");
        let file_id = next_segment_id(&table_dir).unwrap();
        let intent = WalMutationIntent {
            artifact_kind: WalArtifactKind::AnfSegment,
            table_id: definition.id.0,
            schema_revision: definition.schema_revision,
            file_id,
            file_len: bytes.len() as u64,
            sha256: chronicle::sha256(&bytes),
        };
        let staging = stage_wal_artifact(&table_dir, &intent, &bytes).unwrap();
        let transaction_id = database.wal.next_sequence();
        database.wal.begin_transaction(transaction_id, b"fault-injected torn COMMIT").unwrap();
        database.wal.append_mutation(transaction_id, &intent.encode().unwrap()).unwrap();
        database.wal.sync().unwrap();
        drop(database);

        // Simulate a power loss part-way through the next COMMIT frame: a
        // complete length prefix followed by only a small fraction of its body.
        let wal_path = path.join("wal").join("database.anwl");
        let mut file = OpenOptions::new().append(true).open(&wal_path).unwrap();
        let commit_body_len = 8_u32 + 8_u32 + 1_u32 + 8_u32 + 32_u32 + 32_u32;
        file.write_all(&commit_body_len.to_le_bytes()).unwrap();
        file.write_all(&[0xA5; 12]).unwrap();
        file.sync_all().unwrap();
        drop(file);

        let reopened = Database::open(&path).unwrap();
        assert!(reopened.scan("telemetry").unwrap().is_empty());
        assert!(!staging.exists());
        assert!(!table_dir.join(intent.artifact_kind.final_file_name(file_id)).exists());
        let report = read_wal(&wal_path).unwrap();
        assert!(report.is_clean());
        assert!(!report.truncated_tail);
        assert_eq!(report.records.last().unwrap().kind, WalRecordKind::Abort);

        drop(reopened);
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn database_open_fails_closed_on_conflicting_committed_final_artifact() {
        let path = temp_path();
        let mut database = Database::open(&path).unwrap();
        database.create_table("telemetry", sample_schema()).unwrap();
        let definition = database.catalog.tables.get("telemetry").unwrap().clone();
        let rows = vec![vec![
            Value::TimestampMicros(7_000),
            Value::Utf8("conflict-device".into()),
            Value::Float64(27.5),
        ]];
        let column_ids = definition.column_ids.iter().map(|id| id.0).collect::<Vec<_>>();
        let bytes = encode_segment_v2(
            &definition.schema,
            &rows,
            definition.id.0,
            definition.schema_revision,
            &column_ids,
        ).unwrap();
        let table_dir = path.join("data").join("telemetry");
        let file_id = next_segment_id(&table_dir).unwrap();
        let intent = WalMutationIntent {
            artifact_kind: WalArtifactKind::AnfSegment,
            table_id: definition.id.0,
            schema_revision: definition.schema_revision,
            file_id,
            file_len: bytes.len() as u64,
            sha256: chronicle::sha256(&bytes),
        };
        let staging = stage_wal_artifact(&table_dir, &intent, &bytes).unwrap();
        database.commit_wal_intent(&intent, b"fault-injected conflicting final").unwrap();
        drop(database);

        let final_path = table_dir.join(intent.artifact_kind.final_file_name(file_id));
        let conflicting_bytes = b"not the committed artifact";
        fs::write(&final_path, conflicting_bytes).unwrap();
        let result = Database::open(&path);
        assert!(matches!(result, Err(DbError::Wal(_))));
        assert_eq!(fs::read(&final_path).unwrap(), conflicting_bytes);
        assert!(staging.is_file());

        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn database_open_discards_incomplete_staged_intent_and_appends_abort() {
        let path = temp_path();
        let mut database = Database::open(&path).unwrap();
        database.create_table("telemetry", sample_schema()).unwrap();
        let definition = database.catalog.tables.get("telemetry").unwrap().clone();
        let rows = vec![vec![
            Value::TimestampMicros(4_000),
            Value::Utf8("uncommitted-device".into()),
            Value::Float64(24.5),
        ]];
        let column_ids = definition.column_ids.iter().map(|id| id.0).collect::<Vec<_>>();
        let bytes = encode_segment_v2(
            &definition.schema,
            &rows,
            definition.id.0,
            definition.schema_revision,
            &column_ids,
        ).unwrap();
        let table_dir = path.join("data").join("telemetry");
        let file_id = next_segment_id(&table_dir).unwrap();
        let intent = WalMutationIntent {
            artifact_kind: WalArtifactKind::AnfSegment,
            table_id: definition.id.0,
            schema_revision: definition.schema_revision,
            file_id,
            file_len: bytes.len() as u64,
            sha256: chronicle::sha256(&bytes),
        };
        let staging = stage_wal_artifact(&table_dir, &intent, &bytes).unwrap();
        let transaction_id = database.wal.next_sequence();
        database.wal.begin_transaction(transaction_id, b"fault-injected incomplete ANF insert").unwrap();
        database.wal.append_mutation(transaction_id, &intent.encode().unwrap()).unwrap();
        database.wal.sync().unwrap();
        drop(database); // Simulates a stop after intent but before COMMIT.

        let reopened = Database::open(&path).unwrap();
        assert!(reopened.scan("telemetry").unwrap().is_empty());
        assert!(!staging.exists());
        assert!(!table_dir.join(intent.artifact_kind.final_file_name(file_id)).exists());
        let report = read_wal(path.join("wal").join("database.anwl")).unwrap();
        assert!(report.is_clean());
        assert_eq!(report.records.last().unwrap().kind, WalRecordKind::Abort);

        drop(reopened);
        fs::remove_dir_all(path).unwrap();
    }


}
