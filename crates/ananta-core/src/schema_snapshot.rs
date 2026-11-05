//! Immutable ANANTA table-schema snapshots.
//!
//! Snapshot files preserve the exact logical schema and stable column IDs for
//! one table revision. They are stored separately from the mutable catalog so
//! historical segment readers can resolve their original schema.

use crate::{DbError, ObjectId};
use ananta_types::{Column, DataType, Schema};
use std::collections::BTreeSet;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

const MAGIC: &[u8; 4] = b"ANS1";
const END_MAGIC: &[u8; 4] = b"SEND";
const VERSION: u16 = 1;
const FLAGS: u16 = 0;
const HEADER_LEN: usize = 28;
const TRAILER_LEN: usize = 12;
const DIRECTORY_ENTRY_MIN: usize = 15;
const MAX_COLUMNS: usize = 1_000_000;
const MAX_NAME_BYTES: usize = 1024 * 1024;
const MAX_SNAPSHOT_BYTES: u64 = 64 * 1024 * 1024;

/// One immutable schema revision, keyed by table ObjectId and revision number.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaSnapshot {
    table_id: ObjectId,
    revision: u64,
    columns: Vec<SnapshotColumn>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotColumn {
    id: ObjectId,
    name: String,
    data_type: DataType,
    nullable: bool,
}

impl SchemaSnapshot {
    /// Build a snapshot from a logical schema and its stable column identities.
    pub fn new(
        table_id: ObjectId,
        revision: u64,
        schema: &Schema,
        column_ids: &[ObjectId],
    ) -> Result<Self, DbError> {
        if table_id.0 == 0 {
            return Err(DbError::CorruptCatalog("schema snapshot table ID is zero"));
        }
        if revision == 0 {
            return Err(DbError::CorruptCatalog("schema snapshot revision is zero"));
        }
        if schema.len() != column_ids.len() {
            return Err(DbError::CorruptCatalog("schema snapshot column ID count mismatch"));
        }

        let mut ids = BTreeSet::new();
        let mut columns = Vec::with_capacity(schema.len());
        for (column, id) in schema.columns().iter().zip(column_ids) {
            if id.0 == 0 || !ids.insert(*id) {
                return Err(DbError::CorruptCatalog("schema snapshot has zero or duplicate column ID"));
            }
            if column.name.len() > MAX_NAME_BYTES {
                return Err(DbError::LengthOverflow);
            }
            columns.push(SnapshotColumn {
                id: *id,
                name: column.name.clone(),
                data_type: column.data_type,
                nullable: column.nullable,
            });
        }

        let snapshot = Self { table_id, revision, columns };
        snapshot.validate()?;
        Ok(snapshot)
    }

    pub fn table_id(&self) -> ObjectId {
        self.table_id
    }

    pub fn revision(&self) -> u64 {
        self.revision
    }

    pub fn columns(&self) -> &[SnapshotColumn] {
        &self.columns
    }

    pub fn column_ids(&self) -> impl Iterator<Item = ObjectId> + '_ {
        self.columns.iter().map(|column| column.id)
    }

    pub fn schema(&self) -> Result<Schema, DbError> {
        let columns = self.columns.iter()
            .map(|column| Column::new(column.name.clone(), column.data_type, column.nullable))
            .collect();
        Schema::new(columns).map_err(DbError::Type)
    }

    fn validate(&self) -> Result<(), DbError> {
        if self.table_id.0 == 0 {
            return Err(DbError::CorruptCatalog("schema snapshot table ID is zero"));
        }
        if self.revision == 0 {
            return Err(DbError::CorruptCatalog("schema snapshot revision is zero"));
        }
        if self.columns.is_empty() || self.columns.len() > MAX_COLUMNS {
            return Err(DbError::CorruptCatalog("schema snapshot column count is invalid"));
        }

        let mut ids = BTreeSet::new();
        let mut schema_columns = Vec::with_capacity(self.columns.len());
        for column in &self.columns {
            if column.id.0 == 0 || !ids.insert(column.id) {
                return Err(DbError::CorruptCatalog("schema snapshot has zero or duplicate column ID"));
            }
            if column.name.len() > MAX_NAME_BYTES {
                return Err(DbError::CorruptCatalog("schema snapshot column name exceeds limit"));
            }
            schema_columns.push(Column::new(
                column.name.clone(),
                column.data_type,
                column.nullable,
            ));
        }
        Schema::new(schema_columns).map_err(DbError::Type)?;
        Ok(())
    }

    /// Encode a deterministic ANS1 v1 snapshot. The format is experimental.
    pub fn encode(&self) -> Result<Vec<u8>, DbError> {
        self.validate()?;
        let mut bytes = Vec::new();
        bytes.extend_from_slice(MAGIC);
        bytes.extend_from_slice(&VERSION.to_le_bytes());
        bytes.extend_from_slice(&FLAGS.to_le_bytes());
        bytes.extend_from_slice(&self.table_id.0.to_le_bytes());
        bytes.extend_from_slice(&self.revision.to_le_bytes());
        bytes.extend_from_slice(
            &u32::try_from(self.columns.len())
                .map_err(|_| DbError::LengthOverflow)?
                .to_le_bytes(),
        );

        for column in &self.columns {
            bytes.extend_from_slice(&column.id.0.to_le_bytes());
            put_string(&mut bytes, &column.name)?;
            bytes.push(column.data_type as u8);
            bytes.push(u8::from(column.nullable));
            if bytes.len().checked_add(TRAILER_LEN).ok_or(DbError::LengthOverflow)?
                > MAX_SNAPSHOT_BYTES as usize
            {
                return Err(DbError::LengthOverflow);
            }
        }

        if bytes.len().checked_add(TRAILER_LEN).ok_or(DbError::LengthOverflow)?
            > MAX_SNAPSHOT_BYTES as usize
        {
            return Err(DbError::LengthOverflow);
        }
        let checksum = checksum64(&bytes);
        bytes.extend_from_slice(&checksum.to_le_bytes());
        bytes.extend_from_slice(END_MAGIC);
        Ok(bytes)
    }

    /// Decode and fully validate an ANS1 v1 snapshot.
    pub fn decode(bytes: &[u8]) -> Result<Self, DbError> {
        if bytes.len() < HEADER_LEN + DIRECTORY_ENTRY_MIN + TRAILER_LEN {
            return Err(DbError::CorruptCatalog("schema snapshot is truncated"));
        }
        if bytes.len() as u64 > MAX_SNAPSHOT_BYTES {
            return Err(DbError::CorruptCatalog("schema snapshot exceeds size limit"));
        }
        if &bytes[..4] != MAGIC {
            return Err(DbError::CorruptCatalog("invalid schema snapshot magic"));
        }
        let version = u16::from_le_bytes([bytes[4], bytes[5]]);
        if version != VERSION {
            return Err(DbError::UnsupportedCatalogVersion(version));
        }
        let flags = u16::from_le_bytes([bytes[6], bytes[7]]);
        if flags != FLAGS {
            return Err(DbError::CorruptCatalog("unknown schema snapshot feature flags"));
        }

        let trailer = bytes.len() - TRAILER_LEN;
        if &bytes[trailer + 8..] != END_MAGIC {
            return Err(DbError::CorruptCatalog("invalid schema snapshot end marker"));
        }
        if checksum64(&bytes[..trailer]) != read_u64_at(bytes, trailer)? {
            return Err(DbError::ChecksumMismatch);
        }

        let mut cursor = 8_usize;
        let table_id = ObjectId(read_u64(bytes, &mut cursor, trailer)?);
        let revision = read_u64(bytes, &mut cursor, trailer)?;
        let column_count = read_u32(bytes, &mut cursor, trailer)? as usize;
        if column_count == 0
            || column_count > MAX_COLUMNS
            || column_count > (trailer - cursor) / DIRECTORY_ENTRY_MIN
        {
            return Err(DbError::CorruptCatalog("schema snapshot column count exceeds bounds"));
        }

        let mut columns = Vec::with_capacity(column_count);
        let mut ids = BTreeSet::new();
        for _ in 0..column_count {
            let id = ObjectId(read_u64(bytes, &mut cursor, trailer)?);
            if id.0 == 0 || !ids.insert(id) {
                return Err(DbError::CorruptCatalog("schema snapshot has zero or duplicate column ID"));
            }
            let name = read_string(bytes, &mut cursor, trailer)?;
            let type_code = take(bytes, &mut cursor, trailer, 1)?[0];
            let nullable = match take(bytes, &mut cursor, trailer, 1)?[0] {
                0 => false,
                1 => true,
                _ => return Err(DbError::CorruptCatalog("invalid schema snapshot nullability")),
            };
            let data_type = DataType::from_code(type_code)
                .ok_or(DbError::CorruptCatalog("unknown schema snapshot data type"))?;
            columns.push(SnapshotColumn { id, name, data_type, nullable });
        }
        if cursor != trailer {
            return Err(DbError::CorruptCatalog("unexpected trailing schema snapshot data"));
        }

        let snapshot = Self { table_id, revision, columns };
        snapshot.validate()?;
        Ok(snapshot)
    }

    /// Canonical immutable snapshot path for a database root.
    pub fn path(root: impl AsRef<Path>, table_id: ObjectId, revision: u64) -> Result<PathBuf, DbError> {
        if table_id.0 == 0 || revision == 0 {
            return Err(DbError::CorruptCatalog("schema snapshot identity is invalid"));
        }
        Ok(root.as_ref()
            .join("catalog")
            .join("schemas")
            .join(table_id.0.to_string())
            .join(format!("schema-{revision:020}.ans")))
    }

    /// Persist this snapshot without overwriting an existing revision.
    ///
    /// If the immutable path already exists, identical bytes are accepted;
    /// different bytes return an error. A pre-existing temporary file is
    /// never removed or overwritten.
    pub fn write_to(&self, root: impl AsRef<Path>) -> Result<PathBuf, DbError> {
        let bytes = self.encode()?;
        let destination = Self::path(root, self.table_id, self.revision)?;
        let parent = destination.parent().ok_or(DbError::LengthOverflow)?;
        fs::create_dir_all(parent)?;

        match fs::metadata(&destination) {
            Ok(metadata) => {
                if metadata.len() > MAX_SNAPSHOT_BYTES {
                    return Err(DbError::CorruptCatalog("schema snapshot exceeds size limit"));
                }
                let existing_snapshot = Self::read_from(
                    root.as_ref(),
                    self.table_id,
                    self.revision,
                )?;
                if existing_snapshot == *self {
                    return Ok(destination);
                }
                return Err(DbError::CorruptCatalog("immutable schema snapshot conflict"));
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(DbError::Io(error)),
        }

        let temp = destination.with_extension("ans.tmp");
        let mut file = OpenOptions::new().write(true).create_new(true).open(&temp)?;
        let write_result = (|| -> Result<(), DbError> {
            file.write_all(&bytes)?;
            file.sync_all()?;
            drop(file);
            // create_new on the temporary file and single-writer operation
            // prevent accidental replacement of another revision.
            fs::rename(&temp, &destination)?;
            Ok(())
        })();
        if write_result.is_err() {
            let _ = fs::remove_file(&temp);
        }
        write_result?;
        Ok(destination)
    }

    /// Load and validate one exact table/revision snapshot.
    pub fn read_from(
        root: impl AsRef<Path>,
        table_id: ObjectId,
        revision: u64,
    ) -> Result<Self, DbError> {
        let path = Self::path(root, table_id, revision)?;
        let metadata = fs::metadata(&path)?;
        if metadata.len() > MAX_SNAPSHOT_BYTES {
            return Err(DbError::CorruptCatalog("schema snapshot exceeds size limit"));
        }
        let capacity = usize::try_from(metadata.len()).map_err(|_| DbError::LengthOverflow)?;
        let mut bytes = Vec::with_capacity(capacity);
        File::open(path)?.read_to_end(&mut bytes)?;
        let snapshot = Self::decode(&bytes)?;
        if snapshot.table_id != table_id || snapshot.revision != revision {
            return Err(DbError::CorruptCatalog("schema snapshot identity does not match path"));
        }
        Ok(snapshot)
    }
}

impl SnapshotColumn {
    pub fn id(&self) -> ObjectId { self.id }
    pub fn name(&self) -> &str { &self.name }
    pub fn data_type(&self) -> DataType { self.data_type }
    pub fn nullable(&self) -> bool { self.nullable }
}

fn put_string(bytes: &mut Vec<u8>, value: &str) -> Result<(), DbError> {
    if value.len() > MAX_NAME_BYTES {
        return Err(DbError::LengthOverflow);
    }
    bytes.extend_from_slice(
        &u32::try_from(value.len())
            .map_err(|_| DbError::LengthOverflow)?
            .to_le_bytes(),
    );
    bytes.extend_from_slice(value.as_bytes());
    Ok(())
}

fn read_string(bytes: &[u8], cursor: &mut usize, end: usize) -> Result<String, DbError> {
    let length = read_u32(bytes, cursor, end)? as usize;
    if length > MAX_NAME_BYTES {
        return Err(DbError::CorruptCatalog("schema snapshot name exceeds limit"));
    }
    let raw = take(bytes, cursor, end, length)?;
    String::from_utf8(raw.to_vec())
        .map_err(|_| DbError::CorruptCatalog("schema snapshot name is not valid UTF-8"))
}

fn read_u32(bytes: &[u8], cursor: &mut usize, end: usize) -> Result<u32, DbError> {
    let raw = take(bytes, cursor, end, 4)?;
    Ok(u32::from_le_bytes(raw.try_into()
        .map_err(|_| DbError::CorruptCatalog("truncated schema snapshot integer"))?))
}

fn read_u64(bytes: &[u8], cursor: &mut usize, end: usize) -> Result<u64, DbError> {
    let raw = take(bytes, cursor, end, 8)?;
    Ok(u64::from_le_bytes(raw.try_into()
        .map_err(|_| DbError::CorruptCatalog("truncated schema snapshot integer"))?))
}

fn read_u64_at(bytes: &[u8], offset: usize) -> Result<u64, DbError> {
    let end = offset.checked_add(8).ok_or(DbError::LengthOverflow)?;
    let raw = bytes.get(offset..end)
        .ok_or(DbError::CorruptCatalog("truncated schema snapshot checksum"))?;
    Ok(u64::from_le_bytes(raw.try_into()
        .map_err(|_| DbError::CorruptCatalog("invalid schema snapshot checksum"))?))
}

fn take<'a>(bytes: &'a [u8], cursor: &mut usize, end: usize, length: usize) -> Result<&'a [u8], DbError> {
    let next = cursor.checked_add(length).ok_or(DbError::LengthOverflow)?;
    if next > end || next > bytes.len() {
        return Err(DbError::CorruptCatalog("schema snapshot field exceeds boundary"));
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

    fn temp_root() -> PathBuf {
        let nonce = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
        std::env::temp_dir().join(format!("ananta-schema-snapshot-{nonce}"))
    }

    fn sample_snapshot() -> SchemaSnapshot {
        let schema = Schema::new(vec![
            Column::new("x", DataType::Int32, false),
            Column::new("note", DataType::Utf8, true),
        ]).unwrap();
        SchemaSnapshot::new(
            ObjectId(10),
            3,
            &schema,
            &[ObjectId(11), ObjectId(12)],
        ).unwrap()
    }

    fn refresh_checksum(bytes: &mut [u8]) {
        let trailer = bytes.len() - TRAILER_LEN;
        let checksum = checksum64(&bytes[..trailer]);
        bytes[trailer..trailer + 8].copy_from_slice(&checksum.to_le_bytes());
    }

    fn read_fixture() -> Vec<u8> {
        include_str!("../tests/fixtures/schema-snapshot-v1-single-column.hex")
            .split_whitespace()
            .map(|byte| u8::from_str_radix(byte, 16).expect("valid snapshot fixture byte"))
            .collect()
    }

    #[test]
    fn snapshot_round_trip_preserves_ids_and_schema() {
        let snapshot = sample_snapshot();
        let decoded = SchemaSnapshot::decode(&snapshot.encode().unwrap()).unwrap();
        assert_eq!(decoded, snapshot);
        assert_eq!(decoded.schema().unwrap(), snapshot.schema().unwrap());
        assert_eq!(decoded.column_ids().collect::<Vec<_>>(), vec![ObjectId(11), ObjectId(12)]);
    }

    #[test]
    fn snapshot_v1_golden_fixture_is_byte_for_byte_stable() {
        let schema = Schema::new(vec![
            Column::new("x", DataType::Int32, false),
        ]).unwrap();
        let snapshot = SchemaSnapshot::new(ObjectId(1), 1, &schema, &[ObjectId(2)]).unwrap();
        let fixture = read_fixture();

        assert_eq!(snapshot.encode().unwrap(), fixture);
        assert_eq!(SchemaSnapshot::decode(&fixture).unwrap(), snapshot);
    }

    #[test]
    fn snapshot_write_is_idempotent_and_read_validates_identity() {
        let root = temp_root();
        let snapshot = sample_snapshot();
        let path = snapshot.write_to(&root).unwrap();
        assert_eq!(snapshot.write_to(&root).unwrap(), path);
        assert_eq!(
            SchemaSnapshot::read_from(&root, snapshot.table_id(), snapshot.revision()).unwrap(),
            snapshot
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn snapshot_revision_cannot_be_overwritten() {
        let root = temp_root();
        let first = sample_snapshot();
        first.write_to(&root).unwrap();

        let changed_schema = Schema::new(vec![
            Column::new("different", DataType::Int32, false),
            Column::new("note", DataType::Utf8, true),
        ]).unwrap();
        let conflicting = SchemaSnapshot::new(
            first.table_id(),
            first.revision(),
            &changed_schema,
            &[ObjectId(11), ObjectId(12)],
        ).unwrap();

        assert!(matches!(
            conflicting.write_to(&root),
            Err(DbError::CorruptCatalog("immutable schema snapshot conflict"))
        ));
        assert_eq!(
            SchemaSnapshot::read_from(&root, first.table_id(), first.revision()).unwrap(),
            first
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn preexisting_temp_file_is_never_overwritten_or_removed() {
        let root = temp_root();
        let snapshot = sample_snapshot();
        let destination = SchemaSnapshot::path(&root, snapshot.table_id(), snapshot.revision()).unwrap();
        fs::create_dir_all(destination.parent().unwrap()).unwrap();
        let temp = destination.with_extension("ans.tmp");
        fs::write(&temp, b"reserved").unwrap();

        assert!(matches!(snapshot.write_to(&root), Err(DbError::Io(_))));
        assert_eq!(fs::read(&temp).unwrap(), b"reserved");
        assert!(!destination.exists());
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn snapshot_rejects_checksum_corruption_and_unknown_flags() {
        let snapshot = sample_snapshot();
        let mut bytes = snapshot.encode().unwrap();
        bytes[10] ^= 1;
        assert!(matches!(SchemaSnapshot::decode(&bytes), Err(DbError::ChecksumMismatch)));

        let mut flags = snapshot.encode().unwrap();
        flags[6..8].copy_from_slice(&1_u16.to_le_bytes());
        refresh_checksum(&mut flags);
        assert!(matches!(
            SchemaSnapshot::decode(&flags),
            Err(DbError::CorruptCatalog("unknown schema snapshot feature flags"))
        ));
    }

    #[test]
    fn snapshot_rejects_duplicate_column_ids() {
        let mut bytes = sample_snapshot().encode().unwrap();
        // First record starts at header offset 28. The second record begins
        // after the first ID, name length, UTF-8 name, type and nullability.
        let first_name_len = u32::from_le_bytes(bytes[36..40].try_into().unwrap()) as usize;
        let second_id_offset = 28 + 8 + 4 + first_name_len + 2;
        bytes[second_id_offset..second_id_offset + 8].copy_from_slice(&ObjectId(11).0.to_le_bytes());
        refresh_checksum(&mut bytes);
        assert!(matches!(
            SchemaSnapshot::decode(&bytes),
            Err(DbError::CorruptCatalog("schema snapshot has zero or duplicate column ID"))
        ));
    }

    #[test]
    fn snapshot_rejects_invalid_nullability_and_unsupported_version() {
        let snapshot = sample_snapshot();
        let mut nullable = snapshot.encode().unwrap();
        let first_name_len = u32::from_le_bytes(nullable[36..40].try_into().unwrap()) as usize;
        let nullable_offset = 28 + 8 + 4 + first_name_len + 1;
        nullable[nullable_offset] = 2;
        refresh_checksum(&mut nullable);
        assert!(matches!(
            SchemaSnapshot::decode(&nullable),
            Err(DbError::CorruptCatalog("invalid schema snapshot nullability"))
        ));

        let mut version = snapshot.encode().unwrap();
        version[4..6].copy_from_slice(&2_u16.to_le_bytes());
        assert!(matches!(
            SchemaSnapshot::decode(&version),
            Err(DbError::UnsupportedCatalogVersion(2))
        ));
    }

    #[test]
    fn snapshot_rejects_truncated_and_trailing_data() {
        let snapshot = sample_snapshot();
        let valid = snapshot.encode().unwrap();
        assert!(matches!(
            SchemaSnapshot::decode(&valid[..valid.len() - 1]),
            Err(DbError::ChecksumMismatch) | Err(DbError::CorruptCatalog(_))
        ));

        let mut trailing = valid;
        let trailer = trailing.len() - TRAILER_LEN;
        trailing.splice(trailer..trailer, [0_u8]);
        refresh_checksum(&mut trailing);
        assert!(matches!(
            SchemaSnapshot::decode(&trailing),
            Err(DbError::CorruptCatalog("unexpected trailing schema snapshot data"))
        ));
    }
}
