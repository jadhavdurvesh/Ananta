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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FormatError {
    Truncated,
    InvalidMagic,
    UnsupportedVersion(u16),
    InvalidEndMarker,
    ChecksumMismatch,
    InvalidDirectory,
    InvalidValue(&'static str),
    SchemaMismatch,
    Type(String),
    LengthOverflow,
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
            Self::InvalidValue(message) => write!(f, "invalid ANF value: {message}"),
            Self::SchemaMismatch => f.write_str("ANF schema does not match requested schema"),
            Self::Type(message) => f.write_str(message),
            Self::LengthOverflow => f.write_str("ANF length exceeds supported limits"),
        }
    }
}
impl std::error::Error for FormatError {}

#[derive(Debug, Clone, Copy)]
struct Entry { type_code: u8, offset: usize, length: usize }

pub fn encode_segment(schema: &Schema, rows: &[Vec<Value>]) -> Result<Vec<u8>, FormatError> {
    for row in rows {
        schema.validate_row(row).map_err(|e| FormatError::Type(e.to_string()))?;
    }

    let column_count = u32::try_from(schema.len()).map_err(|_| FormatError::LengthOverflow)?;
    let row_count = u64::try_from(rows.len()).map_err(|_| FormatError::LengthOverflow)?;
    let directory_len = schema.len().checked_mul(DIRECTORY_ENTRY_LEN).ok_or(FormatError::LengthOverflow)?;
    let payload_start = HEADER_LEN.checked_add(directory_len).ok_or(FormatError::LengthOverflow)?;

    let mut payloads = Vec::with_capacity(schema.len());
    for (column_index, column) in schema.columns().iter().enumerate() {
        let mut payload = Vec::new();
        for row in rows {
            encode_value(&mut payload, &row[column_index])?;
        }
        payloads.push(payload);
    }

    let mut output = Vec::new();
    output.extend_from_slice(MAGIC);
    output.extend_from_slice(&VERSION.to_le_bytes());
    output.extend_from_slice(&0_u16.to_le_bytes()); // reserved flags
    output.extend_from_slice(&row_count.to_le_bytes());
    output.extend_from_slice(&column_count.to_le_bytes());

    let mut cursor = payload_start;
    for (column, payload) in schema.columns().iter().zip(&payloads) {
        output.push(column.data_type as u8);
        output.extend_from_slice(&u64::try_from(cursor).map_err(|_| FormatError::LengthOverflow)?.to_le_bytes());
        output.extend_from_slice(&u64::try_from(payload.len()).map_err(|_| FormatError::LengthOverflow)?.to_le_bytes());
        cursor = cursor.checked_add(payload.len()).ok_or(FormatError::LengthOverflow)?;
    }
    for payload in payloads { output.extend_from_slice(&payload); }

    let checksum = checksum64(&output);
    output.extend_from_slice(&checksum.to_le_bytes());
    output.extend_from_slice(END_MAGIC);
    Ok(output)
}

pub fn decode_segment(bytes: &[u8], schema: &Schema) -> Result<Vec<Vec<Value>>, FormatError> {
    if bytes.len() < HEADER_LEN + TRAILER_LEN { return Err(FormatError::Truncated); }
    if &bytes[0..4] != MAGIC { return Err(FormatError::InvalidMagic); }
    let version = u16::from_le_bytes([bytes[4], bytes[5]]);
    if version != VERSION { return Err(FormatError::UnsupportedVersion(version)); }
    let flags = u16::from_le_bytes([bytes[6], bytes[7]]);
    if flags != 0 { return Err(FormatError::InvalidValue("unknown mandatory feature flags")); }

    let row_count_u64 = read_u64(bytes, 8)?;
    let row_count = usize::try_from(row_count_u64).map_err(|_| FormatError::LengthOverflow)?;
    let column_count = usize::try_from(read_u32(bytes, 16)?).map_err(|_| FormatError::LengthOverflow)?;
    if column_count != schema.len() { return Err(FormatError::SchemaMismatch); }

    let directory_end = HEADER_LEN.checked_add(column_count.checked_mul(DIRECTORY_ENTRY_LEN).ok_or(FormatError::LengthOverflow)?)
        .ok_or(FormatError::LengthOverflow)?;
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
        if end > trailer_start { return Err(FormatError::InvalidDirectory); }
        entries.push(Entry { type_code, offset, length });
        expected_offset = end;
    }
    if expected_offset != trailer_start { return Err(FormatError::InvalidDirectory); }

    let mut columns = Vec::with_capacity(column_count);
    for (column, entry) in schema.columns().iter().zip(entries) {
        if entry.type_code != column.data_type as u8 { return Err(FormatError::SchemaMismatch); }
        let end = entry.offset + entry.length;
        let mut cursor = entry.offset;
        let mut values = Vec::with_capacity(row_count);
        for _ in 0..row_count {
            values.push(decode_value(bytes, &mut cursor, end, column.data_type)?);
        }
        if cursor != end { return Err(FormatError::InvalidDirectory); }
        columns.push(values);
    }

    let mut rows = Vec::with_capacity(row_count);
    for row_index in 0..row_count {
        let row = columns.iter().map(|column| column[row_index].clone()).collect::<Vec<_>>();
        schema.validate_row(&row).map_err(|e| FormatError::Type(e.to_string()))?;
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
        encoded[encoded.len() - TRAILER_LEN - 1] ^= 0x01;
        assert_eq!(decode_segment(&encoded, &schema), Err(FormatError::ChecksumMismatch));
    }

    #[test]
    fn truncated_segment_is_rejected() {
        let schema = schema();
        let encoded = encode_segment(&schema, &[]).unwrap();
        assert_eq!(decode_segment(&encoded[..encoded.len() - 2], &schema), Err(FormatError::Truncated));
    }
}
