//! ANANTA Native Format (ANF) experimental segment codec.
//! v0.1 is a prototype format, not yet a stable compatibility contract.

use ananta_types::{DataType, Schema, Value};
use std::fmt;

const MAGIC: &[u8; 4] = b"ANF1";
const END_MAGIC: &[u8; 4] = b"AEND";
const VERSION: u16 = 1;
const HEADER_LEN: usize = 20;
const DIRECTORY_ENTRY_LEN: usize = 17;
const TRAILER_LEN: usize = 12;
const MAX_FIELD_BYTES: usize = 64 * 1024 * 1024;
const MAX_SEGMENT_BYTES: usize = 256 * 1024 * 1024;
const MAX_ROWS: usize = 1_000_000;
const MAX_COLUMNS: usize = 4_096;
const MAX_DECODED_CELLS: usize = 1_000_000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FormatError {
    Truncated,
    InvalidMagic,
    UnsupportedVersion(u16),
    InvalidEndMarker,
    ChecksumMismatch,
    InvalidDirectory,
    InvalidProjection(usize),
    InvalidValue(&'static str),
    SchemaMismatch,
    Type(String),
    LengthOverflow,
    LimitExceeded(&'static str),
}

impl fmt::Display for FormatError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Truncated => f.write_str("truncated ANF segment"),
            Self::InvalidMagic => f.write_str("invalid ANF magic"),
            Self::UnsupportedVersion(v) => write!(f, "unsupported ANF version {v}"),
            Self::InvalidEndMarker => f.write_str("invalid ANF end marker"),
            Self::ChecksumMismatch => f.write_str("ANF checksum mismatch"),
            Self::InvalidDirectory => f.write_str("invalid ANF column directory"),
            Self::InvalidProjection(i) => write!(f, "column projection index {i} is out of range"),
            Self::InvalidValue(message) => write!(f, "invalid ANF value: {message}"),
            Self::SchemaMismatch => f.write_str("ANF schema does not match requested schema"),
            Self::Type(message) => f.write_str(message),
            Self::LengthOverflow => f.write_str("ANF length exceeds supported limits"),
            Self::LimitExceeded(limit) => write!(f, "ANF limit exceeded: {limit}"),
        }
    }
}
impl std::error::Error for FormatError {}

#[derive(Debug, Clone, Copy)]
struct Entry { type_code: u8, offset: usize, length: usize }

fn validate_counts(row_count: usize, column_count: usize, validate_full_decode: bool) -> Result<(), FormatError> {
    if row_count > MAX_ROWS {
        return Err(FormatError::LimitExceeded("row count"));
    }
    if column_count == 0 || column_count > MAX_COLUMNS {
        return Err(FormatError::LimitExceeded("column count"));
    }
    let cells = row_count.checked_mul(column_count).ok_or(FormatError::LengthOverflow)?;
    if validate_full_decode && cells > MAX_DECODED_CELLS {
        return Err(FormatError::LimitExceeded("decoded cells"));
    }
    Ok(())
}

pub fn encode_segment(schema: &Schema, rows: &[Vec<Value>]) -> Result<Vec<u8>, FormatError> {
    let column_count = schema.len();
    let row_count = rows.len();
    validate_counts(row_count, column_count, true)?;

    for row in rows {
        schema.validate_row(row).map_err(|e| FormatError::Type(e.to_string()))?;
    }

    let column_count_u32 = u32::try_from(column_count).map_err(|_| FormatError::LengthOverflow)?;
    let row_count_u64 = u64::try_from(row_count).map_err(|_| FormatError::LengthOverflow)?;
    let directory_len = column_count.checked_mul(DIRECTORY_ENTRY_LEN).ok_or(FormatError::LengthOverflow)?;
    let payload_start = HEADER_LEN.checked_add(directory_len).ok_or(FormatError::LengthOverflow)?;

    let mut payloads = Vec::with_capacity(schema.len());
    let mut total_payload_bytes = 0_usize;
    for (column_index, column) in schema.columns().iter().enumerate() {
        let mut payload = Vec::new();
        for row in rows {
            encode_value(&mut payload, &row[column_index])?;
        }
        total_payload_bytes = total_payload_bytes
            .checked_add(payload.len())
            .ok_or(FormatError::LengthOverflow)?;
        let estimated_total = payload_start
            .checked_add(total_payload_bytes)
            .and_then(|size| size.checked_add(TRAILER_LEN))
            .ok_or(FormatError::LengthOverflow)?;
        if estimated_total > MAX_SEGMENT_BYTES {
            return Err(FormatError::LimitExceeded("segment bytes"));
        }
        payloads.push(payload);
    }

    let mut output = Vec::new();
    output.extend_from_slice(MAGIC);
    output.extend_from_slice(&VERSION.to_le_bytes());
    output.extend_from_slice(&0_u16.to_le_bytes()); // reserved flags
    output.extend_from_slice(&row_count_u64.to_le_bytes());
    output.extend_from_slice(&column_count_u32.to_le_bytes());

    let mut cursor = payload_start;
    for (column, payload) in schema.columns().iter().zip(&payloads) {
        output.push(column.data_type as u8);
        output.extend_from_slice(&u64::try_from(cursor).map_err(|_| FormatError::LengthOverflow)?.to_le_bytes());
        output.extend_from_slice(&u64::try_from(payload.len()).map_err(|_| FormatError::LengthOverflow)?.to_le_bytes());
        cursor = cursor.checked_add(payload.len()).ok_or(FormatError::LengthOverflow)?;
    }
    for payload in payloads { output.extend_from_slice(&payload); }

    let total_len = output.len().checked_add(TRAILER_LEN).ok_or(FormatError::LengthOverflow)?;
    if total_len > MAX_SEGMENT_BYTES {
        return Err(FormatError::LimitExceeded("segment bytes"));
    }

    let checksum = checksum64(&output);
    output.extend_from_slice(&checksum.to_le_bytes());
    output.extend_from_slice(END_MAGIC);
    Ok(output)
}

pub fn decode_segment(bytes: &[u8], schema: &Schema) -> Result<Vec<Vec<Value>>, FormatError> {
    let projection = (0..schema.len()).collect::<Vec<_>>();
    decode_projected_segment(bytes, schema, &projection)
}

/// Decode only selected columns, preserving the requested projection order.
/// The segment checksum and all directory bounds are still validated.
pub fn decode_projected_segment(
    bytes: &[u8],
    schema: &Schema,
    projection: &[usize],
) -> Result<Vec<Vec<Value>>, FormatError> {
    if bytes.len() < HEADER_LEN + TRAILER_LEN { return Err(FormatError::Truncated); }
    if bytes.len() > MAX_SEGMENT_BYTES {
        return Err(FormatError::LimitExceeded("segment bytes"));
    }
    if &bytes[0..4] != MAGIC { return Err(FormatError::InvalidMagic); }
    let version = u16::from_le_bytes([bytes[4], bytes[5]]);
    if version != VERSION { return Err(FormatError::UnsupportedVersion(version)); }
    let flags = u16::from_le_bytes([bytes[6], bytes[7]]);
    if flags != 0 { return Err(FormatError::InvalidValue("unknown mandatory feature flags")); }

    let row_count_u64 = read_u64(bytes, 8)?;
    let row_count = usize::try_from(row_count_u64).map_err(|_| FormatError::LengthOverflow)?;
    let column_count = usize::try_from(read_u32(bytes, 16)?).map_err(|_| FormatError::LengthOverflow)?;
    if column_count != schema.len() { return Err(FormatError::SchemaMismatch); }
    validate_counts(row_count, column_count, false)?;
    let decoded_cells = row_count.checked_mul(projection.len()).ok_or(FormatError::LengthOverflow)?;
    if decoded_cells > MAX_DECODED_CELLS {
        return Err(FormatError::LimitExceeded("decoded cells"));
    }
    if let Some(index) = projection.iter().find(|index| **index >= column_count) {
        return Err(FormatError::InvalidProjection(*index));
    }

    let directory_len = column_count.checked_mul(DIRECTORY_ENTRY_LEN).ok_or(FormatError::LengthOverflow)?;
    let directory_end = HEADER_LEN.checked_add(directory_len).ok_or(FormatError::LengthOverflow)?;
    let trailer_start = bytes.len().checked_sub(TRAILER_LEN).ok_or(FormatError::Truncated)?;
    if directory_end > trailer_start { return Err(FormatError::Truncated); }

    let expected_checksum = read_u64(bytes, trailer_start)?;
    if &bytes[trailer_start + 8..] != END_MAGIC { return Err(FormatError::InvalidEndMarker); }
    if checksum64(&bytes[..trailer_start]) != expected_checksum { return Err(FormatError::ChecksumMismatch); }

    let mut entries = Vec::with_capacity(column_count);
    let mut dir_cursor = HEADER_LEN;
    let mut expected_offset = directory_end;
    for column in schema.columns() {
        let type_code = bytes[dir_cursor];
        let offset = usize::try_from(read_u64(bytes, dir_cursor + 1)?).map_err(|_| FormatError::LengthOverflow)?;
        let length = usize::try_from(read_u64(bytes, dir_cursor + 9)?).map_err(|_| FormatError::LengthOverflow)?;
        dir_cursor += DIRECTORY_ENTRY_LEN;
        if type_code != column.data_type as u8 || offset != expected_offset {
            return Err(FormatError::InvalidDirectory);
        }
        let end = offset.checked_add(length).ok_or(FormatError::LengthOverflow)?;
        // Every encoded value needs at least one null marker byte. This also
        // bounds row_count allocations by the actual on-disk segment size.
        if length < row_count || end > trailer_start {
            return Err(FormatError::InvalidDirectory);
        }
        entries.push(Entry { type_code, offset, length });
        expected_offset = end;
    }
    if expected_offset != trailer_start { return Err(FormatError::InvalidDirectory); }

    let mut decoded = vec![None; column_count];
    for index in projection.iter().copied().collect::<std::collections::BTreeSet<_>>() {
        let entry = entries[index];
        let column = &schema.columns()[index];
        if entry.type_code != column.data_type as u8 { return Err(FormatError::SchemaMismatch); }
        let end = entry.offset.checked_add(entry.length).ok_or(FormatError::LengthOverflow)?;
        let mut cursor = entry.offset;
        let mut values = Vec::with_capacity(row_count);
        for _ in 0..row_count {
            let value = decode_value(bytes, &mut cursor, end, column.data_type)?;
            if matches!(value, Value::Null) && !column.nullable {
                return Err(FormatError::Type(format!("column '{}' does not allow NULL", column.name)));
            }
            values.push(value);
        }
        if cursor != end { return Err(FormatError::InvalidDirectory); }
        decoded[index] = Some(values);
    }

    let mut rows = Vec::with_capacity(row_count);
    for row_index in 0..row_count {
        let row = projection.iter().map(|index| {
            decoded[*index].as_ref().expect("projected column decoded")[row_index].clone()
        }).collect::<Vec<_>>();
        rows.push(row);
    }
    Ok(rows)
}

fn encode_value(out: &mut Vec<u8>, value: &Value) -> Result<(), FormatError> {
    if matches!(value, Value::Null) { out.push(0); return Ok(()); }
    out.push(1);
    match value {
        Value::Null => unreachable!(),
        Value::Boolean(v) => out.push(u8::from(*v)),
        Value::Int32(v) => out.extend_from_slice(&v.to_le_bytes()),
        Value::Int64(v) | Value::TimestampMicros(v) => out.extend_from_slice(&v.to_le_bytes()),
        Value::Float64(v) => out.extend_from_slice(&v.to_bits().to_le_bytes()),
        Value::Utf8(v) => encode_bytes(out, v.as_bytes())?,
        Value::Binary(v) => encode_bytes(out, v)?,
    }
    Ok(())
}

fn encode_bytes(out: &mut Vec<u8>, value: &[u8]) -> Result<(), FormatError> {
    if value.len() > MAX_FIELD_BYTES { return Err(FormatError::LengthOverflow); }
    out.extend_from_slice(&u32::try_from(value.len()).map_err(|_| FormatError::LengthOverflow)?.to_le_bytes());
    out.extend_from_slice(value);
    Ok(())
}

fn decode_value(bytes: &[u8], cursor: &mut usize, end: usize, data_type: DataType) -> Result<Value, FormatError> {
    let marker = take(bytes, cursor, end, 1)?[0];
    if marker == 0 { return Ok(Value::Null); }
    if marker != 1 { return Err(FormatError::InvalidValue("invalid null marker")); }
    Ok(match data_type {
        DataType::Boolean => match take(bytes, cursor, end, 1)?[0] {
            0 => Value::Boolean(false), 1 => Value::Boolean(true),
            _ => return Err(FormatError::InvalidValue("boolean must be 0 or 1")),
        },
        DataType::Int32 => Value::Int32(i32::from_le_bytes(take(bytes, cursor, end, 4)?.try_into().map_err(|_| FormatError::Truncated)?)),
        DataType::Int64 => Value::Int64(i64::from_le_bytes(take(bytes, cursor, end, 8)?.try_into().map_err(|_| FormatError::Truncated)?)),
        DataType::TimestampMicros => Value::TimestampMicros(i64::from_le_bytes(take(bytes, cursor, end, 8)?.try_into().map_err(|_| FormatError::Truncated)?)),
        DataType::Float64 => Value::Float64(f64::from_bits(u64::from_le_bytes(take(bytes, cursor, end, 8)?.try_into().map_err(|_| FormatError::Truncated)?))),
        DataType::Utf8 => {
            let raw = decode_bytes(bytes, cursor, end)?;
            Value::Utf8(std::str::from_utf8(raw).map_err(|_| FormatError::InvalidValue("invalid UTF-8"))?.to_owned())
        }
        DataType::Binary => Value::Binary(decode_bytes(bytes, cursor, end)?.to_vec()),
    })
}

fn decode_bytes<'a>(bytes: &'a [u8], cursor: &mut usize, end: usize) -> Result<&'a [u8], FormatError> {
    let length = usize::try_from(read_u32_bounded(bytes, cursor, end)?).map_err(|_| FormatError::LengthOverflow)?;
    if length > MAX_FIELD_BYTES { return Err(FormatError::LengthOverflow); }
    take(bytes, cursor, end, length)
}

fn read_u32_bounded(bytes: &[u8], cursor: &mut usize, end: usize) -> Result<u32, FormatError> {
    Ok(u32::from_le_bytes(take(bytes, cursor, end, 4)?.try_into().map_err(|_| FormatError::Truncated)?))
}
fn take<'a>(bytes: &'a [u8], cursor: &mut usize, end: usize, length: usize) -> Result<&'a [u8], FormatError> {
    let next = cursor.checked_add(length).ok_or(FormatError::LengthOverflow)?;
    if next > end || next > bytes.len() { return Err(FormatError::Truncated); }
    let result = &bytes[*cursor..next];
    *cursor = next;
    Ok(result)
}
fn read_u32(bytes: &[u8], offset: usize) -> Result<u32, FormatError> {
    let slice = bytes.get(offset..offset + 4).ok_or(FormatError::Truncated)?;
    Ok(u32::from_le_bytes(slice.try_into().map_err(|_| FormatError::Truncated)?))
}
fn read_u64(bytes: &[u8], offset: usize) -> Result<u64, FormatError> {
    let slice = bytes.get(offset..offset + 8).ok_or(FormatError::Truncated)?;
    Ok(u64::from_le_bytes(slice.try_into().map_err(|_| FormatError::Truncated)?))
}

/// FNV-1a is used only for accidental-corruption detection, not authentication.
fn checksum64(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf29ce484222325_u64;
    for byte in bytes { hash ^= u64::from(*byte); hash = hash.wrapping_mul(0x100000001b3); }
    hash
}

#[cfg(test)]
mod tests {
    use super::*;
    use ananta_types::Column;

    fn schema() -> Schema {
        Schema::new(vec![
            Column::new("time", DataType::TimestampMicros, false),
            Column::new("device", DataType::Utf8, false),
            Column::new("value", DataType::Float64, true),
            Column::new("active", DataType::Boolean, false),
        ]).unwrap()
    }

    #[test]
    fn segment_round_trip() {
        let schema = schema();
        let rows = vec![
            vec![Value::TimestampMicros(1_000_000), Value::Utf8("sensor-a".into()), Value::Float64(23.5), Value::Boolean(true)],
            vec![Value::TimestampMicros(2_000_000), Value::Utf8("sensor-b".into()), Value::Null, Value::Boolean(false)],
        ];
        let encoded = encode_segment(&schema, &rows).unwrap();
        assert_eq!(decode_segment(&encoded, &schema).unwrap(), rows);
    }

    #[test]
    fn checksum_detects_mutation() {
        let schema = schema();
        let rows = vec![vec![Value::TimestampMicros(1), Value::Utf8("x".into()), Value::Null, Value::Boolean(true)]];
        let mut encoded = encode_segment(&schema, &rows).unwrap();
        let mutation_index = encoded.len() - TRAILER_LEN - 1;
        encoded[mutation_index] ^= 0x01;
        assert_eq!(decode_segment(&encoded, &schema), Err(FormatError::ChecksumMismatch));
    }

    #[test]
    fn projected_decoder_only_returns_requested_columns() {
        let schema = schema();
        let rows = vec![
            vec![Value::TimestampMicros(1), Value::Utf8("a".into()), Value::Float64(12.0), Value::Boolean(true)],
            vec![Value::TimestampMicros(2), Value::Utf8("b".into()), Value::Null, Value::Boolean(false)],
        ];
        let encoded = encode_segment(&schema, &rows).unwrap();
        let projected = decode_projected_segment(&encoded, &schema, &[3, 1]).unwrap();
        assert_eq!(projected, vec![
            vec![Value::Boolean(true), Value::Utf8("a".into())],
            vec![Value::Boolean(false), Value::Utf8("b".into())],
        ]);
        assert!(matches!(
            decode_projected_segment(&encoded, &schema, &[schema.len()]),
            Err(FormatError::InvalidProjection(_))
        ));
    }

    fn read_fixture_hex() -> Vec<u8> {
        include_str!("../tests/fixtures/anf-v0.1-int32-single-row.hex")
            .split_whitespace()
            .map(|byte| u8::from_str_radix(byte, 16).expect("valid hex fixture byte"))
            .collect()
    }

    fn refresh_checksum(bytes: &mut [u8]) {
        let trailer_start = bytes.len() - TRAILER_LEN;
        let checksum = checksum64(&bytes[..trailer_start]);
        bytes[trailer_start..trailer_start + 8].copy_from_slice(&checksum.to_le_bytes());
    }

    fn column_payload_offset(bytes: &[u8], column_index: usize) -> usize {
        let entry = HEADER_LEN + column_index * DIRECTORY_ENTRY_LEN;
        let offset = u64::from_le_bytes(bytes[entry + 1..entry + 9].try_into().unwrap());
        usize::try_from(offset).unwrap()
    }

    #[test]
    fn anf_v0_1_golden_fixture_is_byte_for_byte_stable() {
        let schema = Schema::new(vec![
            Column::new("id", DataType::Int32, false),
        ]).unwrap();
        let rows = vec![vec![Value::Int32(7)]];
        let fixture = read_fixture_hex();

        assert_eq!(encode_segment(&schema, &rows).unwrap(), fixture);
        assert_eq!(decode_segment(&fixture, &schema).unwrap(), rows);
    }

    #[test]
    fn malformed_null_marker_is_rejected_after_checksum_validation() {
        let schema = schema();
        let rows = vec![vec![
            Value::TimestampMicros(1),
            Value::Utf8("sensor".into()),
            Value::Float64(1.0),
            Value::Boolean(true),
        ]];
        let mut bytes = encode_segment(&schema, &rows).unwrap();
        let payload = column_payload_offset(&bytes, 0);
        bytes[payload] = 2;
        refresh_checksum(&mut bytes);

        assert_eq!(
            decode_segment(&bytes, &schema),
            Err(FormatError::InvalidValue("invalid null marker"))
        );
    }

    #[test]
    fn malformed_boolean_payload_is_rejected() {
        let schema = schema();
        let rows = vec![vec![
            Value::TimestampMicros(1),
            Value::Utf8("sensor".into()),
            Value::Float64(1.0),
            Value::Boolean(true),
        ]];
        let mut bytes = encode_segment(&schema, &rows).unwrap();
        let payload = column_payload_offset(&bytes, 3);
        bytes[payload + 1] = 2;
        refresh_checksum(&mut bytes);

        assert_eq!(
            decode_segment(&bytes, &schema),
            Err(FormatError::InvalidValue("boolean must be 0 or 1"))
        );
    }

    #[test]
    fn invalid_utf8_payload_is_rejected() {
        let schema = schema();
        let rows = vec![vec![
            Value::TimestampMicros(1),
            Value::Utf8("x".into()),
            Value::Float64(1.0),
            Value::Boolean(true),
        ]];
        let mut bytes = encode_segment(&schema, &rows).unwrap();
        let payload = column_payload_offset(&bytes, 1);
        bytes[payload + 5] = 0xff;
        refresh_checksum(&mut bytes);

        assert_eq!(
            decode_segment(&bytes, &schema),
            Err(FormatError::InvalidValue("invalid UTF-8"))
        );
    }

    #[test]
    fn invalid_directory_offset_is_rejected() {
        let schema = schema();
        let rows = vec![vec![
            Value::TimestampMicros(1),
            Value::Utf8("x".into()),
            Value::Float64(1.0),
            Value::Boolean(true),
        ]];
        let mut bytes = encode_segment(&schema, &rows).unwrap();
        let first_entry = HEADER_LEN;
        bytes[first_entry + 1..first_entry + 9]
            .copy_from_slice(&0_u64.to_le_bytes());
        refresh_checksum(&mut bytes);

        assert_eq!(decode_segment(&bytes, &schema), Err(FormatError::InvalidDirectory));
    }

    #[test]
    fn unsupported_versions_and_feature_flags_are_rejected() {
        let schema = schema();
        let rows = vec![vec![
            Value::TimestampMicros(1),
            Value::Utf8("x".into()),
            Value::Float64(1.0),
            Value::Boolean(true),
        ]];
        let valid = encode_segment(&schema, &rows).unwrap();

        let mut unsupported = valid.clone();
        unsupported[4..6].copy_from_slice(&2_u16.to_le_bytes());
        assert_eq!(decode_segment(&unsupported, &schema), Err(FormatError::UnsupportedVersion(2)));

        let mut flags = valid;
        flags[6..8].copy_from_slice(&1_u16.to_le_bytes());
        assert_eq!(
            decode_segment(&flags, &schema),
            Err(FormatError::InvalidValue("unknown mandatory feature flags"))
        );
    }

    #[test]
    fn decoder_does_not_panic_on_deterministic_arbitrary_inputs() {
        let schema = schema();
        let mut state = 0x4d59_5df4_d0f3_3173_u64;

        for case in 0..2_000_usize {
            let length = case % 512;
            let mut bytes = vec![0_u8; length];
            for byte in &mut bytes {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                *byte = state as u8;
            }

            let outcome = std::panic::catch_unwind(|| decode_segment(&bytes, &schema));
            assert!(outcome.is_ok(), "decoder panicked for generated input case {case}");
        }
    }

    #[test]
    fn row_and_decoded_cell_limits_are_enforced() {
        assert_eq!(
            validate_counts(MAX_ROWS + 1, 1, false),
            Err(FormatError::LimitExceeded("row count"))
        );
        assert_eq!(
            validate_counts(1, MAX_COLUMNS + 1, false),
            Err(FormatError::LimitExceeded("column count"))
        );
        assert_eq!(
            validate_counts(MAX_DECODED_CELLS + 1, 1, true),
            Err(FormatError::LimitExceeded("decoded cells"))
        );
    }

    #[test]
    fn encoder_rejects_a_schema_with_too_many_columns() {
        let columns = (0..=MAX_COLUMNS)
            .map(|index| Column::new(format!("c{index}"), DataType::Int32, false))
            .collect::<Vec<_>>();
        let schema = Schema::new(columns).unwrap();

        assert_eq!(
            encode_segment(&schema, &[]),
            Err(FormatError::LimitExceeded("column count"))
        );
    }

    #[test]
    fn truncated_segment_is_rejected() {
        let schema = schema();
        let encoded = encode_segment(&schema, &[]).unwrap();
        assert_eq!(decode_segment(&encoded[..encoded.len() - 2], &schema), Err(FormatError::Truncated));
    }
}
