use ananta_format::{decode_projected_segment, encode_segment, FormatError, MAX_SEGMENT_BYTES};
use ananta_types::{Column, DataType, Schema, TypeError, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

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
    CorruptCatalog(&'static str),
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
            Self::CorruptCatalog(s) => write!(f, "corrupt ANANTA catalog: {s}"),
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
        candidate.tables.insert(name, TableDefinition {
            id: table_id,
            schema_revision: 1,
            column_ids,
            schema,
        });

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

    pub fn insert_batch(&mut self, table: &str, rows: &[Vec<Value>]) -> Result<Option<PathBuf>, DbError> {
        let schema = self.schema(table)?;
        for row in rows { schema.validate_row(row)?; }
        if rows.is_empty() { return Ok(None); }

        let table_dir = self.root.join("data").join(table);
        fs::create_dir_all(&table_dir)?;
        let segment_id = next_segment_id(&table_dir)?;
        let filename = format!("segment-{segment_id:020}.anf");
        let final_path = table_dir.join(filename);
        let temp_path = table_dir.join(format!(".segment-{segment_id:020}.tmp"));
        let bytes = encode_segment(schema, rows)?;

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
        self.scan_projected_indices(table, schema, &projection)
    }

    /// Decode only the requested columns, preserving the caller's column order.
    ///
    /// The current implementation still reads the complete segment into memory
    /// because ANF v0.1 uses a whole-segment checksum. Bounded column-range I/O
    /// requires a later format revision and streaming storage API.
    pub fn scan_columns(&self, table: &str, columns: &[&str]) -> Result<Vec<Vec<Value>>, DbError> {
        let schema = self.schema(table)?;
        let mut projection = Vec::with_capacity(columns.len());
        for name in columns {
            let index = schema.columns().iter().position(|column| column.name == *name)
                .ok_or_else(|| DbError::ColumnNotFound((*name).to_owned()))?;
            projection.push(index);
        }
        self.scan_projected_indices(table, schema, &projection)
    }

    fn scan_projected_indices(
        &self,
        table: &str,
        schema: &Schema,
        projection: &[usize],
    ) -> Result<Vec<Vec<Value>>, DbError> {
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
            let mut file = File::open(segment)?;
            let file_len = file.metadata()?.len();
            if file_len > MAX_SEGMENT_BYTES as u64 {
                return Err(DbError::Format(FormatError::LimitExceeded("segment bytes")));
            }
            let capacity = usize::try_from(file_len).map_err(|_| DbError::LengthOverflow)?;
            let mut bytes = Vec::with_capacity(capacity);
            file.read_to_end(&mut bytes)?;
            rows.extend(decode_projected_segment(&bytes, schema, projection)?);
        }
        Ok(rows)
    }
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

fn save_catalog(path: &Path, catalog: &Catalog) -> Result<(), DbError> {
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

    // Validate the exact bytes before publishing them.
    let decoded = decode_catalog_v2(&bytes)?;
    validate_catalog(&decoded)?;

    let temp = path.with_extension("anc.tmp");
    // Acquire the temporary path before entering the cleanup scope. If it
    // already exists, create_new fails and the pre-existing file is untouched.
    let mut file = OpenOptions::new().write(true).create_new(true).open(&temp)?;
    let write_result = (|| -> Result<(), DbError> {
        file.write_all(&bytes)?;
        file.sync_all()?;
        drop(file);
        // On filesystems with atomic same-directory rename, the old catalog
        // remains readable until this publication step succeeds.
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
