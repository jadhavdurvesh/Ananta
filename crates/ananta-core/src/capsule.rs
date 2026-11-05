use crate::DbError;
use ananta_format::{inspect_segment_v2_details, SegmentInspectionV2, MAX_SEGMENT_BYTES};
use ananta_types::{DataType, Schema, Value};

const MAGIC: &[u8; 4] = b"CAP1";
const END: &[u8; 4] = b"CEND";
const VERSION: u16 = 1;
const HEADER_LEN: usize = 144;
const COLUMN_DESCRIPTOR_LEN: usize = 48;
const TRAILER_LEN: usize = 12;
const FLAG_EVENT_TIME_CONFIGURED: u16 = 1;
const FLAG_TEMPORAL_BOUNDS_PRESENT: u16 = 2;
const ALLOWED_FLAGS: u16 = FLAG_EVENT_TIME_CONFIGURED | FLAG_TEMPORAL_BOUNDS_PRESENT;
pub const MAX_CAPSULE_BYTES: usize =
    MAX_SEGMENT_BYTES + HEADER_LEN + (4_096 * COLUMN_DESCRIPTOR_LEN) + TRAILER_LEN;

#[derive(Debug, Clone, PartialEq)]
pub struct CapsuleColumnStats {
    pub column_id: u64,
    pub data_type: DataType,
    pub null_count: u64,
    pub encoded_bytes: u64,
    pub minimum: Option<Value>,
    pub maximum: Option<Value>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CapsuleMetadata {
    pub table_id: u64,
    pub capsule_id: u64,
    pub generation: u64,
    pub schema_revision: u64,
    pub row_count: u64,
    pub first_ingestion_sequence: u64,
    pub last_ingestion_sequence: u64,
    pub event_time_column_id: Option<u64>,
    pub temporal_min: Option<i64>,
    pub temporal_max: Option<i64>,
    pub payload_len: u64,
    pub payload_sha256: [u8; 32],
    pub columns: Vec<CapsuleColumnStats>,
}

pub(crate) fn derive_column_stats(
    schema: &Schema,
    column_ids: &[u64],
    rows: &[Vec<Value>],
    inspection: &SegmentInspectionV2,
) -> Result<Vec<CapsuleColumnStats>, DbError> {
    if schema.len() != column_ids.len()
        || schema.len() != inspection.columns.len()
        || rows.len() as u64 != inspection.row_count
    {
        return Err(DbError::CapsuleCorrupt("schema, rows and ANF directory dimensions differ"));
    }
    let mut stats = Vec::with_capacity(schema.len());
    for (index, column) in schema.columns().iter().enumerate() {
        let mut null_count = 0_u64;
        let mut non_null = Vec::new();
        for row in rows {
            let value = row.get(index).ok_or(DbError::CapsuleCorrupt("row width differs from schema"))?;
            if value.data_type().is_none() {
                null_count = null_count.checked_add(1).ok_or(DbError::LengthOverflow)?;
            } else {
                non_null.push(value);
            }
        }
        let (minimum, maximum) = min_max(column.data_type, &non_null);
        let physical = &inspection.columns[index];
        if physical.column_id != column_ids[index] || physical.data_type != column.data_type {
            return Err(DbError::CapsuleCorrupt("ANF column identity differs from current schema"));
        }
        stats.push(CapsuleColumnStats {
            column_id: column_ids[index],
            data_type: column.data_type,
            null_count,
            encoded_bytes: physical.encoded_bytes,
            minimum,
            maximum,
        });
    }
    Ok(stats)
}

fn min_max(data_type: DataType, values: &[&Value]) -> (Option<Value>, Option<Value>) {
    if values.is_empty() {
        return (None, None);
    }
    match data_type {
        DataType::Boolean => {
            let mut min = true;
            let mut max = false;
            for value in values {
                if let Value::Boolean(value) = value {
                    min &= *value;
                    max |= *value;
                }
            }
            (Some(Value::Boolean(min)), Some(Value::Boolean(max)))
        }
        DataType::Int32 => {
            let mut min = i32::MAX;
            let mut max = i32::MIN;
            for value in values {
                if let Value::Int32(value) = value {
                    min = min.min(*value);
                    max = max.max(*value);
                }
            }
            (Some(Value::Int32(min)), Some(Value::Int32(max)))
        }
        DataType::Int64 => {
            let mut min = i64::MAX;
            let mut max = i64::MIN;
            for value in values {
                if let Value::Int64(value) = value {
                    min = min.min(*value);
                    max = max.max(*value);
                }
            }
            (Some(Value::Int64(min)), Some(Value::Int64(max)))
        }
        DataType::TimestampMicros => {
            let mut min = i64::MAX;
            let mut max = i64::MIN;
            for value in values {
                if let Value::TimestampMicros(value) = value {
                    min = min.min(*value);
                    max = max.max(*value);
                }
            }
            (Some(Value::TimestampMicros(min)), Some(Value::TimestampMicros(max)))
        }
        DataType::Float64 => {
            let Some(Value::Float64(first)) = values.first().copied() else {
                return (None, None);
            };
            let mut min = *first;
            let mut max = *first;
            for value in values.iter().skip(1) {
                if let Value::Float64(value) = value {
                    if value.total_cmp(&min).is_lt() { min = *value; }
                    if value.total_cmp(&max).is_gt() { max = *value; }
                }
            }
            (Some(Value::Float64(min)), Some(Value::Float64(max)))
        }
        DataType::Utf8 | DataType::Binary => (None, None),
    }
}

pub(crate) fn derive_temporal_bounds(
    stats: &[CapsuleColumnStats],
    event_time_column_id: Option<u64>,
) -> Result<(Option<i64>, Option<i64>), DbError> {
    let Some(column_id) = event_time_column_id else {
        return Ok((None, None));
    };
    let column = stats.iter().find(|column| column.column_id == column_id)
        .ok_or(DbError::CapsuleCorrupt("event-time ColumnId is not present"))?;
    if column.data_type != DataType::TimestampMicros {
        return Err(DbError::CapsuleCorrupt("event-time column is not TIMESTAMP_MICROS"));
    }
    match (&column.minimum, &column.maximum) {
        (Some(Value::TimestampMicros(min)), Some(Value::TimestampMicros(max))) => Ok((Some(*min), Some(*max))),
        (None, None) => Ok((None, None)),
        _ => Err(DbError::CapsuleCorrupt("event-time bounds do not match the timestamp column")),
    }
}

pub(crate) fn encode_capsule(
    mut metadata: CapsuleMetadata,
    payload: &[u8],
) -> Result<Vec<u8>, DbError> {
    if payload.len() > MAX_SEGMENT_BYTES {
        return Err(DbError::CapsuleCorrupt("embedded ANF payload exceeds size limit"));
    }
    let inspection = inspect_segment_v2_details(payload)?;
    metadata.payload_len = u64::try_from(payload.len()).map_err(|_| DbError::LengthOverflow)?;
    metadata.payload_sha256 = crate::chronicle::sha256(payload);
    validate_metadata(&metadata, &inspection)?;

    let descriptor_len = metadata.columns.len()
        .checked_mul(COLUMN_DESCRIPTOR_LEN)
        .ok_or(DbError::LengthOverflow)?;
    let total = HEADER_LEN.checked_add(descriptor_len)
        .and_then(|value| value.checked_add(payload.len()))
        .and_then(|value| value.checked_add(TRAILER_LEN))
        .ok_or(DbError::LengthOverflow)?;
    if total > MAX_CAPSULE_BYTES {
        return Err(DbError::CapsuleCorrupt("Capsule exceeds maximum size"));
    }

    let mut flags = 0_u16;
    if metadata.event_time_column_id.is_some() {
        flags |= FLAG_EVENT_TIME_CONFIGURED;
    }
    if metadata.temporal_min.is_some() {
        flags |= FLAG_TEMPORAL_BOUNDS_PRESENT;
    }

    let mut output = Vec::with_capacity(total);
    output.extend_from_slice(MAGIC);
    output.extend_from_slice(&VERSION.to_le_bytes());
    output.extend_from_slice(&flags.to_le_bytes());
    output.extend_from_slice(&(HEADER_LEN as u32).to_le_bytes());
    output.extend_from_slice(&metadata.table_id.to_le_bytes());
    output.extend_from_slice(&metadata.capsule_id.to_le_bytes());
    output.extend_from_slice(&metadata.generation.to_le_bytes());
    output.extend_from_slice(&metadata.schema_revision.to_le_bytes());
    output.extend_from_slice(&metadata.row_count.to_le_bytes());
    output.extend_from_slice(&metadata.first_ingestion_sequence.to_le_bytes());
    output.extend_from_slice(&metadata.last_ingestion_sequence.to_le_bytes());
    output.extend_from_slice(&metadata.event_time_column_id.unwrap_or(0).to_le_bytes());
    output.extend_from_slice(&metadata.temporal_min.unwrap_or(0).to_le_bytes());
    output.extend_from_slice(&metadata.temporal_max.unwrap_or(0).to_le_bytes());
    output.extend_from_slice(&metadata.payload_len.to_le_bytes());
    output.extend_from_slice(&u32::try_from(metadata.columns.len()).map_err(|_| DbError::LengthOverflow)?.to_le_bytes());
    output.extend_from_slice(&u32::try_from(descriptor_len).map_err(|_| DbError::LengthOverflow)?.to_le_bytes());
    output.extend_from_slice(&metadata.payload_sha256);
    output.extend_from_slice(&0_u32.to_le_bytes());
    if output.len() != HEADER_LEN {
        return Err(DbError::CapsuleCorrupt("internal Capsule header length mismatch"));
    }

    for column in &metadata.columns {
        encode_column_descriptor(&mut output, column)?;
    }
    output.extend_from_slice(payload);
    let checksum = checksum64(&output);
    output.extend_from_slice(&checksum.to_le_bytes());
    output.extend_from_slice(END);
    // Round-trip through the validator so the writer never publishes invalid metadata.
    let _ = decode_capsule(&output)?;
    Ok(output)
}

pub(crate) fn decode_capsule(bytes: &[u8]) -> Result<(CapsuleMetadata, Vec<u8>), DbError> {
    if bytes.len() < HEADER_LEN + TRAILER_LEN {
        return Err(DbError::CapsuleCorrupt("Capsule is truncated"));
    }
    if bytes.len() > MAX_CAPSULE_BYTES {
        return Err(DbError::CapsuleCorrupt("Capsule exceeds maximum size"));
    }
    if &bytes[..4] != MAGIC {
        return Err(DbError::CapsuleCorrupt("invalid Capsule magic"));
    }
    if read_u16(bytes, 4)? != VERSION {
        return Err(DbError::CapsuleCorrupt("unsupported Capsule version"));
    }
    let flags = read_u16(bytes, 6)?;
    if flags & !ALLOWED_FLAGS != 0 {
        return Err(DbError::CapsuleCorrupt("unknown mandatory Capsule flags"));
    }
    if read_u32(bytes, 8)? as usize != HEADER_LEN {
        return Err(DbError::CapsuleCorrupt("invalid Capsule header length"));
    }
    if bytes[140..144] != [0, 0, 0, 0] {
        return Err(DbError::CapsuleCorrupt("nonzero Capsule reserved header bytes"));
    }

    let table_id = read_u64(bytes, 12)?;
    let capsule_id = read_u64(bytes, 20)?;
    let generation = read_u64(bytes, 28)?;
    let schema_revision = read_u64(bytes, 36)?;
    let row_count = read_u64(bytes, 44)?;
    let first_ingestion_sequence = read_u64(bytes, 52)?;
    let last_ingestion_sequence = read_u64(bytes, 60)?;
    let event_time_raw = read_u64(bytes, 68)?;
    let temporal_min_raw = read_i64(bytes, 76)?;
    let temporal_max_raw = read_i64(bytes, 84)?;
    let payload_len = read_u64(bytes, 92)?;
    let column_count = usize::try_from(read_u32(bytes, 100)?).map_err(|_| DbError::LengthOverflow)?;
    let descriptor_len = usize::try_from(read_u32(bytes, 104)?).map_err(|_| DbError::LengthOverflow)?;
    let mut payload_sha256 = [0_u8; 32];
    payload_sha256.copy_from_slice(&bytes[108..140]);

    let expected_descriptor_len = column_count.checked_mul(COLUMN_DESCRIPTOR_LEN).ok_or(DbError::LengthOverflow)?;
    if descriptor_len != expected_descriptor_len {
        return Err(DbError::CapsuleCorrupt("Capsule descriptor length does not match column count"));
    }
    let payload_start = HEADER_LEN.checked_add(descriptor_len).ok_or(DbError::LengthOverflow)?;
    let payload_start_u64 = u64::try_from(payload_start).map_err(|_| DbError::LengthOverflow)?;
    let payload_end_u64 = payload_start_u64.checked_add(payload_len).ok_or(DbError::LengthOverflow)?;
    let trailer_start = bytes.len().checked_sub(TRAILER_LEN).ok_or(DbError::CapsuleCorrupt("missing Capsule trailer"))?;
    if payload_end_u64 != trailer_start as u64 {
        return Err(DbError::CapsuleCorrupt("Capsule payload length does not match file size"));
    }
    if &bytes[trailer_start + 8..] != END {
        return Err(DbError::CapsuleCorrupt("invalid Capsule end marker"));
    }
    if checksum64(&bytes[..trailer_start]) != read_u64(bytes, trailer_start)? {
        return Err(DbError::CapsuleCorrupt("Capsule checksum mismatch"));
    }

    let mut columns = Vec::with_capacity(column_count);
    let mut cursor = HEADER_LEN;
    for _ in 0..column_count {
        columns.push(decode_column_descriptor(&bytes[cursor..cursor + COLUMN_DESCRIPTOR_LEN])?);
        cursor += COLUMN_DESCRIPTOR_LEN;
    }
    let payload = bytes[payload_start..trailer_start].to_vec();
    if crate::chronicle::sha256(&payload) != payload_sha256 {
        return Err(DbError::CapsuleCorrupt("embedded ANF payload fingerprint mismatch"));
    }

    let event_time_column_id = if flags & FLAG_EVENT_TIME_CONFIGURED != 0 {
        if event_time_raw == 0 { return Err(DbError::CapsuleCorrupt("event-time flag set with zero ColumnId")); }
        Some(event_time_raw)
    } else {
        if event_time_raw != 0 { return Err(DbError::CapsuleCorrupt("event-time ColumnId set without flag")); }
        None
    };
    let has_temporal = flags & FLAG_TEMPORAL_BOUNDS_PRESENT != 0;
    let (temporal_min, temporal_max) = if has_temporal {
        (Some(temporal_min_raw), Some(temporal_max_raw))
    } else {
        if temporal_min_raw != 0 || temporal_max_raw != 0 {
            return Err(DbError::CapsuleCorrupt("temporal values set without bounds flag"));
        }
        (None, None)
    };

    let metadata = CapsuleMetadata {
        table_id,
        capsule_id,
        generation,
        schema_revision,
        row_count,
        first_ingestion_sequence,
        last_ingestion_sequence,
        event_time_column_id,
        temporal_min,
        temporal_max,
        payload_len,
        payload_sha256,
        columns,
    };
    let inspection = inspect_segment_v2_details(&payload)?;
    validate_metadata(&metadata, &inspection)?;
    Ok((metadata, payload))
}

fn validate_metadata(metadata: &CapsuleMetadata, inspection: &SegmentInspectionV2) -> Result<(), DbError> {
    if metadata.table_id == 0 || metadata.capsule_id == 0 || metadata.generation == 0 || metadata.schema_revision == 0 {
        return Err(DbError::CapsuleCorrupt("table, Capsule, generation and schema identities must be nonzero"));
    }
    if metadata.row_count == 0 || metadata.first_ingestion_sequence == 0
        || metadata.first_ingestion_sequence > metadata.last_ingestion_sequence
    {
        return Err(DbError::CapsuleCorrupt("invalid Capsule row count or source sequence range"));
    }
    if metadata.table_id != inspection.identity.table_id
        || metadata.schema_revision != inspection.identity.schema_revision
        || metadata.row_count != inspection.row_count
        || metadata.payload_len == 0
    {
        return Err(DbError::CapsuleCorrupt("Capsule header does not match embedded ANF identity"));
    }
    if metadata.columns.len() != inspection.columns.len() || metadata.columns.is_empty() {
        return Err(DbError::CapsuleCorrupt("Capsule column count differs from embedded ANF"));
    }
    for (stats, physical) in metadata.columns.iter().zip(&inspection.columns) {
        if stats.column_id == 0 || stats.column_id != physical.column_id
            || stats.data_type != physical.data_type || stats.encoded_bytes != physical.encoded_bytes
            || stats.null_count > metadata.row_count
        {
            return Err(DbError::CapsuleCorrupt("Capsule column stats differ from embedded ANF directory"));
        }
        match (&stats.minimum, &stats.maximum) {
            (Some(min), Some(max)) => {
                if min.data_type() != Some(stats.data_type) || max.data_type() != Some(stats.data_type) {
                    return Err(DbError::CapsuleCorrupt("Capsule min/max data type mismatch"));
                }
                if stats.null_count == metadata.row_count {
                    return Err(DbError::CapsuleCorrupt("all-null column cannot have min/max"));
                }
                validate_scalar_type(stats.data_type)?;
                if compare_values(min, max).is_gt() {
                    return Err(DbError::CapsuleCorrupt("Capsule min value exceeds max value"));
                }
            }
            (None, None) => {}
            _ => return Err(DbError::CapsuleCorrupt("Capsule min/max must be present as a pair")),
        }
    }
    match metadata.event_time_column_id {
        Some(id) => {
            let column = metadata.columns.iter().find(|column| column.column_id == id)
                .ok_or(DbError::CapsuleCorrupt("event-time ColumnId is absent from Capsule"))?;
            if column.data_type != DataType::TimestampMicros {
                return Err(DbError::CapsuleCorrupt("event-time column is not TIMESTAMP_MICROS"));
            }
            match (&metadata.temporal_min, &metadata.temporal_max, &column.minimum, &column.maximum) {
                (Some(min), Some(max), Some(Value::TimestampMicros(column_min)), Some(Value::TimestampMicros(column_max)))
                    if min <= max && min == column_min && max == column_max => {}
                (None, None, None, None) => {}
                _ => return Err(DbError::CapsuleCorrupt("temporal bounds differ from event-time column statistics")),
            }
        }
        None if metadata.temporal_min.is_none() && metadata.temporal_max.is_none() => {}
        None => return Err(DbError::CapsuleCorrupt("temporal bounds exist without an event-time column")),
    }
    Ok(())
}

pub(crate) fn encode_column_descriptor(out: &mut Vec<u8>, stats: &CapsuleColumnStats) -> Result<(), DbError> {
    let (min_bytes, min_len) = encode_scalar(stats.data_type, stats.minimum.as_ref())?;
    let (max_bytes, max_len) = encode_scalar(stats.data_type, stats.maximum.as_ref())?;
    let flags = u8::from(stats.minimum.is_some());
    out.extend_from_slice(&stats.column_id.to_le_bytes());
    out.push(stats.data_type as u8);
    out.push(flags);
    out.extend_from_slice(&0_u16.to_le_bytes());
    out.extend_from_slice(&stats.null_count.to_le_bytes());
    out.extend_from_slice(&stats.encoded_bytes.to_le_bytes());
    out.push(min_len);
    out.push(max_len);
    out.extend_from_slice(&0_u16.to_le_bytes());
    out.extend_from_slice(&min_bytes);
    out.extend_from_slice(&max_bytes);
    Ok(())
}

pub(crate) fn decode_column_descriptor(bytes: &[u8]) -> Result<CapsuleColumnStats, DbError> {
    if bytes.len() != COLUMN_DESCRIPTOR_LEN {
        return Err(DbError::CapsuleCorrupt("invalid Capsule descriptor length"));
    }
    let column_id = read_u64(bytes, 0)?;
    let data_type = DataType::from_code(bytes[8]).ok_or(DbError::CapsuleCorrupt("unknown Capsule data type"))?;
    let flags = bytes[9];
    if flags & !1 != 0 || bytes[10..12] != [0, 0] || bytes[30..32] != [0, 0] {
        return Err(DbError::CapsuleCorrupt("unknown Capsule descriptor flags or reserved bytes"));
    }
    let null_count = read_u64(bytes, 12)?;
    let encoded_bytes = read_u64(bytes, 20)?;
    let min_len = bytes[28];
    let max_len = bytes[29];
    let mut min_raw = [0_u8; 8];
    min_raw.copy_from_slice(&bytes[32..40]);
    let mut max_raw = [0_u8; 8];
    max_raw.copy_from_slice(&bytes[40..48]);
    let has_minmax = flags & 1 != 0;
    let (minimum, maximum) = if has_minmax {
        if min_len == 0 || min_len != max_len {
            return Err(DbError::CapsuleCorrupt("invalid Capsule min/max scalar length"));
        }
        (
            Some(decode_scalar(data_type, &min_raw, min_len)?),
            Some(decode_scalar(data_type, &max_raw, max_len)?),
        )
    } else {
        if min_len != 0 || max_len != 0 || min_raw != [0; 8] || max_raw != [0; 8] {
            return Err(DbError::CapsuleCorrupt("min/max bytes present without descriptor flag"));
        }
        (None, None)
    };
    Ok(CapsuleColumnStats { column_id, data_type, null_count, encoded_bytes, minimum, maximum })
}

fn encode_scalar(data_type: DataType, value: Option<&Value>) -> Result<([u8; 8], u8), DbError> {
    let mut bytes = [0_u8; 8];
    let Some(value) = value else { return Ok((bytes, 0)); };
    let length = match (data_type, value) {
        (DataType::Boolean, Value::Boolean(value)) => { bytes[0] = u8::from(*value); 1 }
        (DataType::Int32, Value::Int32(value)) => { bytes[..4].copy_from_slice(&value.to_le_bytes()); 4 }
        (DataType::Int64, Value::Int64(value)) | (DataType::TimestampMicros, Value::TimestampMicros(value)) => {
            bytes.copy_from_slice(&value.to_le_bytes()); 8
        }
        (DataType::Float64, Value::Float64(value)) => { bytes.copy_from_slice(&value.to_bits().to_le_bytes()); 8 }
        _ => return Err(DbError::CapsuleCorrupt("min/max is unsupported for this scalar type")),
    };
    Ok((bytes, length))
}

fn decode_scalar(data_type: DataType, bytes: &[u8; 8], length: u8) -> Result<Value, DbError> {
    let value = match data_type {
        DataType::Boolean if length == 1 && bytes[0] <= 1 && bytes[1..] == [0; 7] => Value::Boolean(bytes[0] == 1),
        DataType::Int32 if length == 4 && bytes[4..] == [0, 0, 0, 0] => {
            Value::Int32(i32::from_le_bytes(bytes[..4].try_into().map_err(|_| DbError::LengthOverflow)?))
        }
        DataType::Int64 if length == 8 => Value::Int64(i64::from_le_bytes(*bytes)),
        DataType::TimestampMicros if length == 8 => Value::TimestampMicros(i64::from_le_bytes(*bytes)),
        DataType::Float64 if length == 8 => Value::Float64(f64::from_bits(u64::from_le_bytes(*bytes))),
        _ => return Err(DbError::CapsuleCorrupt("invalid min/max scalar encoding")),
    };
    Ok(value)
}

fn validate_scalar_type(data_type: DataType) -> Result<(), DbError> {
    match data_type {
        DataType::Boolean | DataType::Int32 | DataType::Int64 | DataType::TimestampMicros | DataType::Float64 => Ok(()),
        DataType::Utf8 | DataType::Binary => Err(DbError::CapsuleCorrupt("min/max unsupported for variable-length columns")),
    }
}

pub(crate) fn compare_values(left: &Value, right: &Value) -> std::cmp::Ordering {
    match (left, right) {
        (Value::Boolean(left), Value::Boolean(right)) => left.cmp(right),
        (Value::Int32(left), Value::Int32(right)) => left.cmp(right),
        (Value::Int64(left), Value::Int64(right)) => left.cmp(right),
        (Value::TimestampMicros(left), Value::TimestampMicros(right)) => left.cmp(right),
        (Value::Float64(left), Value::Float64(right)) => left.total_cmp(right),
        _ => std::cmp::Ordering::Equal,
    }
}

fn read_u16(bytes: &[u8], offset: usize) -> Result<u16, DbError> {
    Ok(u16::from_le_bytes(bytes.get(offset..offset + 2).ok_or(DbError::CapsuleCorrupt("truncated Capsule integer"))?
        .try_into().map_err(|_| DbError::LengthOverflow)?))
}

fn read_u32(bytes: &[u8], offset: usize) -> Result<u32, DbError> {
    Ok(u32::from_le_bytes(bytes.get(offset..offset + 4).ok_or(DbError::CapsuleCorrupt("truncated Capsule integer"))?
        .try_into().map_err(|_| DbError::LengthOverflow)?))
}

fn read_u64(bytes: &[u8], offset: usize) -> Result<u64, DbError> {
    Ok(u64::from_le_bytes(bytes.get(offset..offset + 8).ok_or(DbError::CapsuleCorrupt("truncated Capsule integer"))?
        .try_into().map_err(|_| DbError::LengthOverflow)?))
}

fn read_i64(bytes: &[u8], offset: usize) -> Result<i64, DbError> {
    Ok(i64::from_le_bytes(bytes.get(offset..offset + 8).ok_or(DbError::CapsuleCorrupt("truncated Capsule integer"))?
        .try_into().map_err(|_| DbError::LengthOverflow)?))
}

fn checksum64(bytes: &[u8]) -> u64 {
    let mut checksum = 0xcbf29ce484222325_u64;
    for byte in bytes {
        checksum ^= u64::from(*byte);
        checksum = checksum.wrapping_mul(0x100000001b3);
    }
    checksum
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn capsule_round_trip_preserves_identity_statistics_and_temporal_bounds() {
        use ananta_types::Column;

        let schema = Schema::new(vec![
            Column::new("event_time", DataType::TimestampMicros, false),
            Column::new("temperature", DataType::Float64, true),
            Column::new("device", DataType::Utf8, false),
            Column::new("all_null", DataType::Int64, true),
        ]).unwrap();
        let rows = vec![
            vec![Value::TimestampMicros(100), Value::Float64(21.5), Value::Utf8("a".into()), Value::Null],
            vec![Value::TimestampMicros(300), Value::Null, Value::Utf8("bb".into()), Value::Null],
            vec![Value::TimestampMicros(200), Value::Float64(18.5), Value::Utf8("c".into()), Value::Null],
        ];
        let ids = vec![101, 102, 103, 104];
        let payload = ananta_format::encode_segment_v2(&schema, &rows, 7, 2, &ids).unwrap();
        let inspection = inspect_segment_v2_details(&payload).unwrap();
        let columns = derive_column_stats(&schema, &ids, &rows, &inspection).unwrap();
        let (temporal_min, temporal_max) = derive_temporal_bounds(&columns, Some(101)).unwrap();
        let metadata = CapsuleMetadata {
            table_id: 7,
            capsule_id: 5,
            generation: 2,
            schema_revision: 2,
            row_count: 3,
            first_ingestion_sequence: 10,
            last_ingestion_sequence: 12,
            event_time_column_id: Some(101),
            temporal_min,
            temporal_max,
            payload_len: 0,
            payload_sha256: [0; 32],
            columns,
        };

        let encoded = encode_capsule(metadata, &payload).unwrap();
        let (decoded, decoded_payload) = decode_capsule(&encoded).unwrap();
        assert_eq!(decoded.table_id, 7);
        assert_eq!(decoded.capsule_id, 5);
        assert_eq!(decoded.generation, 2);
        assert_eq!(decoded.schema_revision, 2);
        assert_eq!(decoded.row_count, 3);
        assert_eq!((decoded.first_ingestion_sequence, decoded.last_ingestion_sequence), (10, 12));
        assert_eq!((decoded.temporal_min, decoded.temporal_max), (Some(100), Some(300)));
        assert_eq!(decoded_payload, payload);
        assert_eq!(decoded.columns[1].null_count, 1);
        assert_eq!(decoded.columns[1].minimum, Some(Value::Float64(18.5)));
        assert_eq!(decoded.columns[1].maximum, Some(Value::Float64(21.5)));
        assert_eq!(decoded.columns[2].minimum, None);
        assert_eq!(decoded.columns[3].null_count, 3);
        assert_eq!(decoded.columns[3].minimum, None);
        assert_eq!(decoded.columns[3].maximum, None);
    }

    #[test]
    fn capsule_rejects_checksum_and_payload_fingerprint_corruption() {
        use ananta_types::Column;

        let schema = Schema::new(vec![
            Column::new("time", DataType::TimestampMicros, false),
        ]).unwrap();
        let rows = vec![vec![Value::TimestampMicros(1)]];
        let payload = ananta_format::encode_segment_v2(&schema, &rows, 7, 1, &[9]).unwrap();
        let inspection = inspect_segment_v2_details(&payload).unwrap();
        let columns = derive_column_stats(&schema, &[9], &rows, &inspection).unwrap();
        let metadata = CapsuleMetadata {
            table_id: 7, capsule_id: 1, generation: 1, schema_revision: 1,
            row_count: 1, first_ingestion_sequence: 1, last_ingestion_sequence: 1,
            event_time_column_id: None, temporal_min: None, temporal_max: None,
            payload_len: 0, payload_sha256: [0; 32], columns,
        };
        let encoded = encode_capsule(metadata, &payload).unwrap();

        let mut checksum_corrupt = encoded.clone();
        checksum_corrupt[HEADER_LEN] ^= 1;
        assert!(matches!(decode_capsule(&checksum_corrupt), Err(DbError::CapsuleCorrupt("Capsule checksum mismatch"))));

        let mut fingerprint_corrupt = encoded;
        let payload_start = HEADER_LEN + COLUMN_DESCRIPTOR_LEN;
        fingerprint_corrupt[payload_start] ^= 1;
        let trailer_start = fingerprint_corrupt.len() - TRAILER_LEN;
        let checksum = checksum64(&fingerprint_corrupt[..trailer_start]);
        fingerprint_corrupt[trailer_start..trailer_start + 8].copy_from_slice(&checksum.to_le_bytes());
        assert!(matches!(decode_capsule(&fingerprint_corrupt), Err(DbError::CapsuleCorrupt("embedded ANF payload fingerprint mismatch"))));
    }

    #[test]
    fn scalar_stats_round_trip_preserves_f64_bits() {
        let metadata = CapsuleColumnStats {
            column_id: 9,
            data_type: DataType::Float64,
            null_count: 0,
            encoded_bytes: 9,
            minimum: Some(Value::Float64(-0.0)),
            maximum: Some(Value::Float64(f64::NAN)),
        };
        let mut bytes = Vec::new();
        encode_column_descriptor(&mut bytes, &metadata).unwrap();
        let decoded = decode_column_descriptor(&bytes).unwrap();
        let (Some(Value::Float64(min)), Some(Value::Float64(max))) = (decoded.minimum, decoded.maximum) else {
            panic!("float values expected");
        };
        assert_eq!(min.to_bits(), (-0.0_f64).to_bits());
        assert_eq!(max.to_bits(), f64::NAN.to_bits());
    }
}
