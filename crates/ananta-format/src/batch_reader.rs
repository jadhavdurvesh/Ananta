use super::*;
use std::io::{Read, Seek, SeekFrom};

const HEADER_V2_LEN: usize = 36;
const DIRECTORY_V2_ENTRY_LEN: usize = 25;
const TRAILER_LEN_LOCAL: usize = 12;
const CHECKSUM_BUFFER_BYTES: usize = 64 * 1024;
pub const MAX_BATCH_ROWS: usize = 65_536;

#[derive(Debug, Clone, Copy)]
struct StreamEntry {
    offset: u64,
    end: u64,
}

#[derive(Debug, Clone)]
struct StreamColumn {
    cursor: u64,
    end: u64,
    data_type: DataType,
    nullable: bool,
    name: String,
}

/// File-backed projected decoder. Verifies the whole segment before exposing rows.
pub struct SegmentBatchReader<R> {
    reader: R,
    row_count: usize,
    rows_read: usize,
    columns: Vec<StreamColumn>,
}

impl<R: Read + Seek> SegmentBatchReader<R> {
    /// For v0.2, expected identity is (table ID, schema revision, ordered column IDs).
    /// For v0.1, pass None.
    pub fn new(
        mut reader: R,
        schema: &Schema,
        projection: &[usize],
        expected_v2_identity: Option<(u64, u64, Vec<u64>)>,
    ) -> Result<Self, FormatError> {
                if let Some(index) = projection.iter().find(|index| **index >= schema.len()) {
            return Err(FormatError::InvalidProjection(*index));
        }

        let file_len = reader.seek(SeekFrom::End(0)).map_err(io_error)?;
        if file_len > MAX_SEGMENT_BYTES as u64 {
            return Err(FormatError::LimitExceeded("segment bytes"));
        }
        if file_len < (HEADER_LEN + TRAILER_LEN_LOCAL) as u64 {
            return Err(FormatError::Truncated);
        }

        reader.seek(SeekFrom::Start(0)).map_err(io_error)?;
        let mut prefix = [0_u8; HEADER_V2_LEN];
        reader.read_exact(&mut prefix[..HEADER_LEN]).map_err(map_read_error)?;
        if &prefix[..4] != MAGIC {
            return Err(FormatError::InvalidMagic);
        }
        let version = u16::from_le_bytes([prefix[4], prefix[5]]);
        let (header_len, entry_len, row_count, column_count) = match version {
            1 => {
                if expected_v2_identity.is_some() {
                    return Err(FormatError::SchemaMismatch);
                }
                let flags = u16::from_le_bytes([prefix[6], prefix[7]]);
                if flags != 0 {
                    return Err(FormatError::InvalidValue("unknown mandatory feature flags"));
                }
                let rows = usize::try_from(u64::from_le_bytes(prefix[8..16].try_into().map_err(|_| FormatError::Truncated)?))
                    .map_err(|_| FormatError::LengthOverflow)?;
                let columns = usize::try_from(u32::from_le_bytes(prefix[16..20].try_into().map_err(|_| FormatError::Truncated)?))
                    .map_err(|_| FormatError::LengthOverflow)?;
                (HEADER_LEN, DIRECTORY_ENTRY_LEN, rows, columns)
            }
            2 => {
                if file_len < (HEADER_V2_LEN + TRAILER_LEN_LOCAL) as u64 {
                    return Err(FormatError::Truncated);
                }
                reader.seek(SeekFrom::Start(0)).map_err(io_error)?;
                reader.read_exact(&mut prefix).map_err(map_read_error)?;
                let flags = u16::from_le_bytes([prefix[6], prefix[7]]);
                if flags != 0 {
                    return Err(FormatError::InvalidValue("unknown mandatory feature flags"));
                }
                let rows = usize::try_from(u64::from_le_bytes(prefix[8..16].try_into().map_err(|_| FormatError::Truncated)?))
                    .map_err(|_| FormatError::LengthOverflow)?;
                let columns = usize::try_from(u32::from_le_bytes(prefix[16..20].try_into().map_err(|_| FormatError::Truncated)?))
                    .map_err(|_| FormatError::LengthOverflow)?;
                let table_id = u64::from_le_bytes(prefix[20..28].try_into().map_err(|_| FormatError::Truncated)?);
                let revision = u64::from_le_bytes(prefix[28..36].try_into().map_err(|_| FormatError::Truncated)?);
                if table_id == 0 || revision == 0 {
                    return Err(FormatError::InvalidValue("table ID and schema revision must be nonzero"));
                }
                if expected_v2_identity.as_ref().map_or(true, |(expected_table, expected_revision, _)| {
                    *expected_table != table_id || *expected_revision != revision
                }) {
                    return Err(FormatError::SchemaMismatch);
                }
                (HEADER_V2_LEN, DIRECTORY_V2_ENTRY_LEN, rows, columns)
            }
            other => return Err(FormatError::UnsupportedVersion(other)),
        };
        validate_counts(row_count, column_count, false)?;
        if column_count != schema.len() {
            return Err(FormatError::SchemaMismatch);
        }

        let directory_len = column_count.checked_mul(entry_len).ok_or(FormatError::LengthOverflow)?;
        let directory_end = (header_len as u64)
            .checked_add(u64::try_from(directory_len).map_err(|_| FormatError::LengthOverflow)?)
            .ok_or(FormatError::LengthOverflow)?;
        let trailer_start = file_len.checked_sub(TRAILER_LEN_LOCAL as u64).ok_or(FormatError::Truncated)?;
        if directory_end > trailer_start {
            return Err(FormatError::Truncated);
        }

        reader.seek(SeekFrom::Start(trailer_start)).map_err(io_error)?;
        let mut trailer = [0_u8; TRAILER_LEN_LOCAL];
        reader.read_exact(&mut trailer).map_err(map_read_error)?;
        if &trailer[8..] != END_MAGIC {
            return Err(FormatError::InvalidEndMarker);
        }
        let expected_checksum = u64::from_le_bytes(trailer[..8].try_into().map_err(|_| FormatError::Truncated)?);
        reader.seek(SeekFrom::Start(0)).map_err(io_error)?;
        let mut remaining = trailer_start;
        let mut buffer = [0_u8; CHECKSUM_BUFFER_BYTES];
        let mut checksum = 0xcbf29ce484222325_u64;
        while remaining > 0 {
            let take = usize::try_from(remaining.min(CHECKSUM_BUFFER_BYTES as u64))
                .map_err(|_| FormatError::LengthOverflow)?;
            reader.read_exact(&mut buffer[..take]).map_err(map_read_error)?;
            for byte in &buffer[..take] {
                checksum ^= u64::from(*byte);
                checksum = checksum.wrapping_mul(0x100000001b3);
            }
            remaining -= take as u64;
        }
        if checksum != expected_checksum {
            return Err(FormatError::ChecksumMismatch);
        }

        reader.seek(SeekFrom::Start(header_len as u64)).map_err(io_error)?;
        let mut selected = Vec::with_capacity(projection.len());
        let mut expected_offset = directory_end;
        let mut v2_ids = Vec::with_capacity(column_count);
        let mut unique_ids = std::collections::BTreeSet::new();
        for (index, column) in schema.columns().iter().enumerate() {
            let mut record = [0_u8; DIRECTORY_V2_ENTRY_LEN];
            reader.read_exact(&mut record[..entry_len]).map_err(map_read_error)?;
            let (type_code, column_id, offset, length) = if version == 1 {
                (
                    record[0],
                    None,
                    u64::from_le_bytes(record[1..9].try_into().map_err(|_| FormatError::Truncated)?),
                    u64::from_le_bytes(record[9..17].try_into().map_err(|_| FormatError::Truncated)?),
                )
            } else {
                let id = u64::from_le_bytes(record[1..9].try_into().map_err(|_| FormatError::Truncated)?);
                if id == 0 || !unique_ids.insert(id) {
                    return Err(FormatError::InvalidValue("column IDs must be nonzero and unique"));
                }
                v2_ids.push(id);
                (
                    record[0],
                    Some(id),
                    u64::from_le_bytes(record[9..17].try_into().map_err(|_| FormatError::Truncated)?),
                    u64::from_le_bytes(record[17..25].try_into().map_err(|_| FormatError::Truncated)?),
                )
            };
            if type_code != column.data_type as u8 || offset != expected_offset {
                return Err(FormatError::InvalidDirectory);
            }
            let end = offset.checked_add(length).ok_or(FormatError::LengthOverflow)?;
            if length < row_count as u64 || end > trailer_start {
                return Err(FormatError::InvalidDirectory);
            }
            if projection.contains(&index) {
                selected.push((index, StreamEntry { offset, end }, column_id));
            }
            expected_offset = end;
        }
        if expected_offset != trailer_start {
            return Err(FormatError::InvalidDirectory);
        }
        if let Some((_, _, expected_ids)) = &expected_v2_identity {
            if version != 2 || *expected_ids != v2_ids {
                return Err(FormatError::SchemaMismatch);
            }
        }

        let mut columns = Vec::with_capacity(projection.len());
        for index in projection {
            let (_, entry, _) = selected.iter().find(|(selected_index, _, _)| selected_index == index)
                .ok_or(FormatError::InvalidProjection(*index))?;
            let column = &schema.columns()[*index];
            columns.push(StreamColumn {
                cursor: entry.offset,
                end: entry.end,
                data_type: column.data_type,
                nullable: column.nullable,
                name: column.name.clone(),
            });
        }
        Ok(Self { reader, row_count, rows_read: 0, columns })
    }

    pub fn row_count(&self) -> usize {
        self.row_count
    }

    pub fn remaining_rows(&self) -> usize {
        self.row_count.saturating_sub(self.rows_read)
    }

    /// Decode at most max_rows rows, returned as one vector per projected column.
    pub fn next_batch(&mut self, max_rows: usize) -> Result<Option<Vec<Vec<Value>>>, FormatError> {
        if max_rows == 0 || max_rows > MAX_BATCH_ROWS {
            return Err(FormatError::LimitExceeded("batch row count must be between 1 and 65536"));
        }
        if self.rows_read >= self.row_count {
            return Ok(None);
        }
        let count = max_rows.min(self.remaining_rows());
        let mut batch = Vec::with_capacity(self.columns.len());
        for column in &mut self.columns {
            let mut values = Vec::with_capacity(count);
            self.reader.seek(SeekFrom::Start(column.cursor)).map_err(io_error)?;
            for _ in 0..count {
                values.push(read_value(
                    &mut self.reader,
                    column.end,
                    column.data_type,
                    column.nullable,
                    &column.name,
                    &mut column.cursor,
                )?);
            }
            if self.rows_read + count == self.row_count && column.cursor != column.end {
                return Err(FormatError::InvalidDirectory);
            }
            batch.push(values);
        }
        self.rows_read += count;
        Ok(Some(batch))
    }
}

fn read_value<R: Read>(
    reader: &mut R,
    end: u64,
    data_type: DataType,
    nullable: bool,
    name: &str,
    cursor: &mut u64,
) -> Result<Value, FormatError> {
    let marker = read_exact_bounded(reader, end, cursor, 1)?[0];
    if marker == 0 {
        if !nullable {
            return Err(FormatError::Type(format!("column '{name}' does not allow NULL")));
        }
        return Ok(Value::Null);
    }
    if marker != 1 {
        return Err(FormatError::InvalidValue("invalid null marker"));
    }
    Ok(match data_type {
        DataType::Boolean => match read_exact_bounded(reader, end, cursor, 1)?[0] {
            0 => Value::Boolean(false),
            1 => Value::Boolean(true),
            _ => return Err(FormatError::InvalidValue("boolean must be 0 or 1")),
        },
        DataType::Int32 => Value::Int32(i32::from_le_bytes(read_exact_bounded(reader, end, cursor, 4)?.try_into().map_err(|_| FormatError::Truncated)?)),
        DataType::Int64 => Value::Int64(i64::from_le_bytes(read_exact_bounded(reader, end, cursor, 8)?.try_into().map_err(|_| FormatError::Truncated)?)),
        DataType::TimestampMicros => Value::TimestampMicros(i64::from_le_bytes(read_exact_bounded(reader, end, cursor, 8)?.try_into().map_err(|_| FormatError::Truncated)?)),
        DataType::Float64 => Value::Float64(f64::from_bits(u64::from_le_bytes(read_exact_bounded(reader, end, cursor, 8)?.try_into().map_err(|_| FormatError::Truncated)?))),
        DataType::Utf8 => {
            let length = usize::try_from(u32::from_le_bytes(read_exact_bounded(reader, end, cursor, 4)?.try_into().map_err(|_| FormatError::Truncated)?))
                .map_err(|_| FormatError::LengthOverflow)?;
            if length > MAX_FIELD_BYTES { return Err(FormatError::LengthOverflow); }
            Value::Utf8(String::from_utf8(read_exact_bounded(reader, end, cursor, length)?)
                .map_err(|_| FormatError::InvalidValue("invalid UTF-8"))?)
        }
        DataType::Binary => {
            let length = usize::try_from(u32::from_le_bytes(read_exact_bounded(reader, end, cursor, 4)?.try_into().map_err(|_| FormatError::Truncated)?))
                .map_err(|_| FormatError::LengthOverflow)?;
            if length > MAX_FIELD_BYTES { return Err(FormatError::LengthOverflow); }
            Value::Binary(read_exact_bounded(reader, end, cursor, length)?)
        }
    })
}

fn read_exact_bounded<R: Read>(
    reader: &mut R,
    end: u64,
    cursor: &mut u64,
    length: usize,
) -> Result<Vec<u8>, FormatError> {
    let length = u64::try_from(length).map_err(|_| FormatError::LengthOverflow)?;
    let next = cursor.checked_add(length).ok_or(FormatError::LengthOverflow)?;
    if next > end { return Err(FormatError::Truncated); }
    let mut bytes = vec![0_u8; usize::try_from(length).map_err(|_| FormatError::LengthOverflow)?];
    reader.read_exact(&mut bytes).map_err(map_read_error)?;
    *cursor = next;
    Ok(bytes)
}

fn io_error(error: std::io::Error) -> FormatError {
    FormatError::Io(error.to_string())
}

fn map_read_error(error: std::io::Error) -> FormatError {
    if error.kind() == std::io::ErrorKind::UnexpectedEof {
        FormatError::Truncated
    } else {
        io_error(error)
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use ananta_types::Column;
    use std::io::Cursor;

    fn schema() -> Schema {
        Schema::new(vec![
            Column::new("id", DataType::Int64, false),
            Column::new("label", DataType::Utf8, true),
            Column::new("enabled", DataType::Boolean, false),
            Column::new("payload", DataType::Binary, true),
        ]).unwrap()
    }

    fn rows() -> Vec<Vec<Value>> {
        vec![
            vec![Value::Int64(1), Value::Utf8("one".into()), Value::Boolean(true), Value::Binary(vec![1, 2])],
            vec![Value::Int64(2), Value::Null, Value::Boolean(false), Value::Null],
            vec![Value::Int64(3), Value::Utf8("three".into()), Value::Boolean(true), Value::Binary(vec![3])],
        ]
    }

    #[test]
    fn v1_file_reader_emits_projected_column_batches() {
        let schema = schema();
        let encoded = encode_segment(&schema, &rows()).unwrap();
        let mut decoder = SegmentBatchReader::new(Cursor::new(encoded), &schema, &[1, 0], None).unwrap();

        let first = decoder.next_batch(2).unwrap().unwrap();
        assert_eq!(first, vec![
            vec![Value::Utf8("one".into()), Value::Null],
            vec![Value::Int64(1), Value::Int64(2)],
        ]);
        assert_eq!(decoder.remaining_rows(), 1);

        let second = decoder.next_batch(2).unwrap().unwrap();
        assert_eq!(second, vec![
            vec![Value::Utf8("three".into())],
            vec![Value::Int64(3)],
        ]);
        assert!(decoder.next_batch(1).unwrap().is_none());
    }

    #[test]
    fn v2_file_reader_checks_identity_and_decodes_batches() {
        let schema = schema();
        let ids = vec![10, 11, 12, 13];
        let encoded = encode_segment_v2(&schema, &rows(), 7, 4, &ids).unwrap();
        let mut decoder = SegmentBatchReader::new(
            Cursor::new(encoded.clone()),
            &schema,
            &[3, 0],
            Some((7, 4, ids.clone())),
        ).unwrap();
        assert_eq!(decoder.next_batch(1).unwrap().unwrap(), vec![
            vec![Value::Binary(vec![1, 2])],
            vec![Value::Int64(1)],
        ]);
        assert!(matches!(
            SegmentBatchReader::new(
                Cursor::new(encoded),
                &schema,
                &[0],
                Some((7, 3, ids)),
            ),
            Err(FormatError::SchemaMismatch)
        ));
    }

    #[test]
    fn null_only_projection_still_advances_rows() {
        let schema = schema();
        let encoded = encode_segment(&schema, &rows()).unwrap();
        let mut decoder = SegmentBatchReader::new(Cursor::new(encoded), &schema, &[], None).unwrap();
        assert_eq!(decoder.next_batch(2).unwrap().unwrap(), Vec::<Vec<Value>>::new());
        assert_eq!(decoder.remaining_rows(), 1);
        assert_eq!(decoder.next_batch(2).unwrap().unwrap(), Vec::<Vec<Value>>::new());
        assert!(decoder.next_batch(1).unwrap().is_none());
    }

    #[test]
    fn checksum_is_verified_before_first_batch() {
        let schema = schema();
        let mut encoded = encode_segment(&schema, &rows()).unwrap();
        encoded[encoded.len() - 13] ^= 0x01;
        assert_eq!(
            SegmentBatchReader::new(Cursor::new(encoded), &schema, &[0], None).err(),
            Some(FormatError::ChecksumMismatch)
        );
    }

    #[test]
    fn batch_size_bounds_are_enforced() {
        let schema = schema();
        let encoded = encode_segment(&schema, &rows()).unwrap();
        let mut decoder = SegmentBatchReader::new(Cursor::new(encoded), &schema, &[0], None).unwrap();
        assert!(matches!(decoder.next_batch(0), Err(FormatError::LimitExceeded(_))));
        assert!(matches!(decoder.next_batch(MAX_BATCH_ROWS + 1), Err(FormatError::LimitExceeded(_))));
    }
}
