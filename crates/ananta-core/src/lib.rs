use ananta_format::{decode_projected_segment, decode_segment_v2, encode_segment_v2, inspect_segment_v2, segment_version, FormatError, MAX_SEGMENT_BYTES};
use ananta_types::{Column, DataType, Schema, TypeError, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

pub mod schema_snapshot;
mod nullable_ddl;
use schema_snapshot::{SchemaSnapshot, SchemaSnapshotConsistencyReport};

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
    CorruptCatalog(&'static str),
    SchemaCompatibility(&'static str),
    UnsupportedCatalogVersion(u16),
    ChecksumMismatch,
    LengthOverflow,
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
            Self::CorruptCatalog(s) => write!(f, "corrupt ANANTA catalog: {s}"),
            Self::SchemaCompatibility(s) => write!(f, "ANANTA schema compatibility error: {s}"),
            Self::UnsupportedCatalogVersion(v) => write!(f, "unsupported catalog version: {v}"),
            Self::ChecksumMismatch => f.write_str("catalog checksum mismatch"),
            Self::LengthOverflow => f.write_str("catalog field exceeds supported limits"),
        }
    }
}
impl std::error::Error for DbError {}
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

/// An embedded, single-writer ANANTA database handle. No external database is required.
pub struct Database {
    root: PathBuf,
    catalog: Catalog,
}

impl Database {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, DbError> {
        let root = path.as_ref().to_path_buf();
        fs::create_dir_all(root.join("catalog"))?;
        fs::create_dir_all(root.join("data"))?;
        let catalog_path = root.join("catalog").join("catalog.anc");
        let catalog = match fs::metadata(&catalog_path) {
            Ok(_) => load_catalog(&catalog_path)?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Catalog::empty(),
            Err(error) => return Err(DbError::Io(error)),
        };
        ensure_initial_schema_snapshots(&root, &catalog)?;
        Ok(Self { root, catalog })
    }

    pub fn create_table(&mut self, name: impl Into<String>, schema: Schema) -> Result<(), DbError> {
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

        let table_dir = self.root.join("data").join(&name);
        fs::create_dir_all(&table_dir)?;
        candidate.tables.insert(name.clone(), TableDefinition {
            id: table_id,
            schema_revision: 1,
            column_ids,
            schema,
        });

        // Persist revision-one identity before publishing the catalog that references it.
        let definition = candidate.tables.get(&name).expect("new table inserted");
        SchemaSnapshot::new(
            definition.id,
            definition.schema_revision,
            &definition.schema,
            &definition.column_ids,
        )?.write_to(&self.root)?;
        save_catalog(&self.root.join("catalog").join("catalog.anc"), &candidate)?;
        self.catalog = candidate;
        Ok(())
    }

    pub fn schema(&self, table: &str) -> Result<&Schema, DbError> {
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
        self.catalog.tables.get(table)
            .map(|definition| definition.id)
            .ok_or_else(|| DbError::TableNotFound(table.to_owned()))
    }

    /// Stable database-local identity for a column.
    pub fn column_id(&self, table: &str, column: &str) -> Result<ObjectId, DbError> {
        let definition = self.catalog.tables.get(table)
            .ok_or_else(|| DbError::TableNotFound(table.to_owned()))?;
        let index = definition.schema.columns().iter()
            .position(|candidate| candidate.name == column)
            .ok_or_else(|| DbError::ColumnNotFound(column.to_owned()))?;
        Ok(definition.column_ids[index])
    }

    /// Logical schema revision for a table. A newly-created table starts at revision one.
    pub fn table_schema_revision(&self, table: &str) -> Result<u64, DbError> {
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
        let definition = self.catalog.tables.get(table)
            .ok_or_else(|| DbError::TableNotFound(table.to_owned()))?;
        SchemaSnapshot::read_from(&self.root, definition.id, revision)
    }

    pub fn insert_batch(&mut self, table: &str, rows: &[Vec<Value>]) -> Result<Option<PathBuf>, DbError> {
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
        let final_path = table_dir.join(format!("segment-{segment_id:020}.anf"));
        let temp_path = table_dir.join(format!(".segment-{segment_id:020}.tmp"));

        let write_result = (|| -> Result<(), DbError> {
            let mut file = OpenOptions::new().write(true).create_new(true).open(&temp_path)?;
            file.write_all(&bytes)?;
            file.sync_all()?;
            fs::rename(&temp_path, &final_path)?;
            Ok(())
        })();
        if write_result.is_err() { let _ = fs::remove_file(&temp_path); }
        write_result?;
        Ok(Some(final_path))
    }

    pub fn scan(&self, table: &str) -> Result<Vec<Vec<Value>>, DbError> {
        let schema = self.schema(table)?;
        let projection = (0..schema.len()).collect::<Vec<_>>();
        self.scan_projected_indices(table, &projection)
    }

    /// Return only the requested logical columns, preserving caller order.
    /// ANF v0.1 and v0.2 segments are resolved independently using their stored schema identity.
    pub fn scan_columns(&self, table: &str, columns: &[&str]) -> Result<Vec<Vec<Value>>, DbError> {
        let schema = self.schema(table)?;
        let mut projection = Vec::with_capacity(columns.len());
        for name in columns {
            let index = schema.columns().iter().position(|column| column.name == *name)
                .ok_or_else(|| DbError::ColumnNotFound((*name).to_owned()))?;
            projection.push(index);
        }
        self.scan_projected_indices(table, &projection)
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

        let table_dir = self.root.join("data").join(table);
        let mut segments = Vec::new();
        for entry in fs::read_dir(&table_dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.extension().and_then(|s| s.to_str()) == Some("anf") {
                segments.push(path);
            }
        }
        segments.sort();

        let mut rows = Vec::new();
        for segment in segments {
            let mut file = File::open(&segment)?;
            let file_len = file.metadata()?.len();
            if file_len > MAX_SEGMENT_BYTES as u64 {
                return Err(DbError::Format(FormatError::LimitExceeded("segment bytes")));
            }
            let capacity = usize::try_from(file_len).map_err(|_| DbError::LengthOverflow)?;
            let mut bytes = Vec::with_capacity(capacity);
            file.read_to_end(&mut bytes)?;
            let logical_rows = self.decode_and_resolve_segment(table, definition, &bytes)?;
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

fn validate_name(name: &str) -> Result<(), DbError> {
    if name.is_empty() || name.len() > 128 ||
       !name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-') {
        return Err(DbError::InvalidName(name.to_owned()));
    }
    Ok(())
}

fn next_segment_id(directory: &Path) -> Result<u64, DbError> {
    let mut max_id = 0_u64;
    for entry in fs::read_dir(directory)? {
        let path = entry?.path();
        if path.extension().and_then(|s| s.to_str()) != Some("anf") { continue; }
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
        fs::rename(&temp, path)?;
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
    fn nullable_column_catalog_publication_failure_keeps_old_schema_and_reports_orphan() {
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
        let report = db.check_schema_snapshot_consistency().unwrap();
        assert!(report.orphaned_files.contains(&next_snapshot));

        drop(db);
        fs::remove_file(catalog_temp).unwrap();
        let reopened = Database::open(&path).unwrap();
        assert_eq!(reopened.table_schema_revision("telemetry").unwrap(), 1);
        assert_eq!(reopened.catalog_revision(), 1);
        assert_eq!(reopened.schema("telemetry").unwrap().len(), 3);
        assert!(reopened.check_schema_snapshot_consistency().unwrap()
            .orphaned_files.contains(&next_snapshot));
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn nullable_column_retry_reuses_identical_orphan_snapshot() {
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
}
