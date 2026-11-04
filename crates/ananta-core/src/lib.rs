use ananta_format::{decode_segment, encode_segment, FormatError};
use ananta_types::{Column, DataType, Schema, TypeError, Value};
use std::collections::BTreeMap;
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

const CATALOG_MAGIC: &[u8; 4] = b"ANC1";
const CATALOG_END: &[u8; 4] = b"CEND";
const CATALOG_VERSION: u16 = 1;
const MAX_CATALOG_ITEMS: usize = 1_000_000;
const MAX_NAME_BYTES: usize = 1024 * 1024;

#[derive(Debug)]
pub enum DbError {
    Io(std::io::Error),
    Format(FormatError),
    Type(TypeError),
    InvalidName(String),
    TableExists(String),
    TableNotFound(String),
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
            Self::CorruptCatalog(s) => write!(f, "corrupt ANANTA catalog: {s}"),
            Self::UnsupportedCatalogVersion(v) => write!(f, "unsupported catalog version: {v}"),
            Self::ChecksumMismatch => f.write_str("catalog checksum mismatch"),
            Self::LengthOverflow => f.write_str("catalog field exceeds supported limits"),
        }
    }
}
impl std::error::Error for DbError {}
impl From<std::io::Error> for DbError { fn from(value: std::io::Error) -> Self { Self::Io(value) } }
impl From<FormatError> for DbError { fn from(value: FormatError) -> Self { Self::Format(value) } }
impl From<TypeError> for DbError { fn from(value: TypeError) -> Self { Self::Type(value) } }

/// An embedded, single-writer ANANTA database handle. No external database is required.
pub struct Database {
    root: PathBuf,
    catalog: BTreeMap<String, Schema>,
}

impl Database {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, DbError> {
        let root = path.as_ref().to_path_buf();
        fs::create_dir_all(root.join("catalog"))?;
        fs::create_dir_all(root.join("data"))?;
        let catalog_path = root.join("catalog").join("catalog.anc");
        let catalog = if catalog_path.exists() { load_catalog(&catalog_path)? } else { BTreeMap::new() };
        Ok(Self { root, catalog })
    }

    pub fn create_table(&mut self, name: impl Into<String>, schema: Schema) -> Result<(), DbError> {
        let name = name.into();
        validate_name(&name)?;
        if self.catalog.contains_key(&name) { return Err(DbError::TableExists(name)); }
        fs::create_dir_all(self.root.join("data").join(&name))?;
        self.catalog.insert(name, schema);
        if let Err(error) = save_catalog(&self.root.join("catalog").join("catalog.anc"), &self.catalog) {
            // In-memory state is rolled back if catalog publication fails.
            // The empty data directory is harmless and may be reused on retry.
            return Err(error);
        }
        Ok(())
    }

    pub fn schema(&self, table: &str) -> Result<&Schema, DbError> {
        self.catalog.get(table).ok_or_else(|| DbError::TableNotFound(table.to_owned()))
    }

    pub fn table_names(&self) -> impl Iterator<Item = &str> {
        self.catalog.keys().map(String::as_str)
    }

    pub fn insert_batch(&self, table: &str, rows: &[Vec<Value>]) -> Result<Option<PathBuf>, DbError> {
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
        let table_dir = self.root.join("data").join(table);
        if !table_dir.exists() { return Ok(Vec::new()); }
        let mut segments = fs::read_dir(&table_dir)?
            .filter_map(Result::ok)
            .map(|entry| entry.path())
            .filter(|path| path.extension().and_then(|s| s.to_str()) == Some("anf"))
            .collect::<Vec<_>>();
        segments.sort();
        let mut rows = Vec::new();
        for segment in segments {
            let mut file = File::open(segment)?;
            let mut bytes = Vec::new();
            file.read_to_end(&mut bytes)?;
            rows.extend(decode_segment(&bytes, schema)?);
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

fn save_catalog(path: &Path, catalog: &BTreeMap<String, Schema>) -> Result<(), DbError> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(CATALOG_MAGIC);
    bytes.extend_from_slice(&CATALOG_VERSION.to_le_bytes());
    bytes.extend_from_slice(&u32::try_from(catalog.len()).map_err(|_| DbError::LengthOverflow)?.to_le_bytes());
    for (name, schema) in catalog {
        put_string(&mut bytes, name)?;
        bytes.extend_from_slice(&u32::try_from(schema.len()).map_err(|_| DbError::LengthOverflow)?.to_le_bytes());
        for column in schema.columns() {
            put_string(&mut bytes, &column.name)?;
            bytes.push(column.data_type as u8);
            bytes.push(u8::from(column.nullable));
        }
    }
    let checksum = checksum64(&bytes);
    bytes.extend_from_slice(&checksum.to_le_bytes());
    bytes.extend_from_slice(CATALOG_END);

    let temp = path.with_extension("anc.tmp");
    let mut file = OpenOptions::new().write(true).create(true).truncate(true).open(&temp)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    fs::rename(&temp, path)?;
    Ok(())
}

fn load_catalog(path: &Path) -> Result<BTreeMap<String, Schema>, DbError> {
    let mut bytes = Vec::new();
    File::open(path)?.read_to_end(&mut bytes)?;
    if bytes.len() < 18 { return Err(DbError::CorruptCatalog("file is truncated")); }
    if &bytes[..4] != CATALOG_MAGIC { return Err(DbError::CorruptCatalog("invalid magic")); }
    let version = u16::from_le_bytes([bytes[4], bytes[5]]);
    if version != CATALOG_VERSION { return Err(DbError::UnsupportedCatalogVersion(version)); }
    let trailer = bytes.len() - 12;
    if &bytes[trailer + 8..] != CATALOG_END { return Err(DbError::CorruptCatalog("invalid end marker")); }
    if checksum64(&bytes[..trailer]) != read_u64(&bytes, trailer)? { return Err(DbError::ChecksumMismatch); }

    let mut cursor = 6_usize;
    let table_count = read_u32(&bytes, &mut cursor, trailer)? as usize;
    if table_count > MAX_CATALOG_ITEMS { return Err(DbError::CorruptCatalog("table count exceeds limit")); }
    let mut catalog = BTreeMap::new();
    for _ in 0..table_count {
        let name = read_string(&bytes, &mut cursor, trailer)?;
        validate_name(&name)?;
        let column_count = read_u32(&bytes, &mut cursor, trailer)? as usize;
        if column_count == 0 || column_count > MAX_CATALOG_ITEMS {
            return Err(DbError::CorruptCatalog("invalid column count"));
        }
        let mut columns = Vec::with_capacity(column_count);
        for _ in 0..column_count {
            let column_name = read_string(&bytes, &mut cursor, trailer)?;
            let type_code = take(&bytes, &mut cursor, trailer, 1)?[0];
            let nullable = match take(&bytes, &mut cursor, trailer, 1)?[0] {
                0 => false, 1 => true, _ => return Err(DbError::CorruptCatalog("invalid nullability flag")),
            };
            let data_type = DataType::from_code(type_code).ok_or(DbError::CorruptCatalog("unknown data type"))?;
            columns.push(Column::new(column_name, data_type, nullable));
        }
        let schema = Schema::new(columns).map_err(DbError::Type)?;
        if catalog.insert(name.clone(), schema).is_some() {
            return Err(DbError::CorruptCatalog("duplicate table name"));
        }
    }
    if cursor != trailer { return Err(DbError::CorruptCatalog("unexpected trailing catalog data")); }
    Ok(catalog)
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
fn read_u64(bytes: &[u8], offset: usize) -> Result<u64, DbError> {
    let slice = bytes.get(offset..offset + 8).ok_or(DbError::CorruptCatalog("truncated checksum"))?;
    Ok(u64::from_le_bytes(slice.try_into().map_err(|_| DbError::CorruptCatalog("invalid checksum"))?))
}
fn take<'a>(bytes: &'a [u8], cursor: &mut usize, end: usize, length: usize) -> Result<&'a [u8], DbError> {
    let next = cursor.checked_add(length).ok_or(DbError::LengthOverflow)?;
    if next > end || next > bytes.len() { return Err(DbError::CorruptCatalog("field exceeds catalog boundary")); }
    let value = &bytes[*cursor..next];
    *cursor = next;
    Ok(value)
}
fn checksum64(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf29ce484222325_u64;
    for byte in bytes { hash ^= u64::from(*byte); hash = hash.wrapping_mul(0x100000001b3); }
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

    #[test]
    fn database_survives_close_and_reopen() {
        let path = temp_path();
        let rows = vec![
            vec![Value::TimestampMicros(10), Value::Utf8("sensor-1".into()), Value::Float64(21.25)],
            vec![Value::TimestampMicros(20), Value::Utf8("sensor-2".into()), Value::Null],
        ];
        {
            let mut db = Database::open(&path).unwrap();
            db.create_table("telemetry", sample_schema()).unwrap();
            db.insert_batch("telemetry", &rows).unwrap();
        }
        {
            let db = Database::open(&path).unwrap();
            assert_eq!(db.scan("telemetry").unwrap(), rows);
        }
        fs::remove_dir_all(path).unwrap();
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
    fn multiple_batches_are_read_in_segment_order() {
        let path = temp_path();
        let mut db = Database::open(&path).unwrap();
        db.create_table("telemetry", sample_schema()).unwrap();
        let a = vec![Value::TimestampMicros(1), Value::Utf8("a".into()), Value::Float64(1.0)];
        let b = vec![Value::TimestampMicros(2), Value::Utf8("b".into()), Value::Float64(2.0)];
        db.insert_batch("telemetry", &[a.clone()]).unwrap();
        db.insert_batch("telemetry", &[b.clone()]).unwrap();
        assert_eq!(db.scan("telemetry").unwrap(), vec![a,b]);
        fs::remove_dir_all(path).unwrap();
    }
}
