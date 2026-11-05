use crate::capsule::{decode_column_descriptor, encode_column_descriptor, compare_values, CapsuleColumnStats, CapsuleMetadata};
use crate::DbError;
use ananta_types::{DataType, Value};
use std::collections::BTreeSet;

const MAGIC: &[u8; 4] = b"NVM1";
const END: &[u8; 4] = b"NEND";
const VERSION: u16 = 1;
const HEADER_LEN: usize = 32;
const TRAILER_LEN: usize = 12;
const ENTRY_FIXED_LEN: usize = 156;
const MAX_MAP_BYTES: usize = 256 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnRangePredicate {
    pub column: String,
    pub lower: Option<Value>,
    pub lower_inclusive: bool,
    pub upper: Option<Value>,
    pub upper_inclusive: bool,
}

impl ColumnRangePredicate {
    pub fn new(
        column: impl Into<String>,
        lower: Option<Value>,
        lower_inclusive: bool,
        upper: Option<Value>,
        upper_inclusive: bool,
    ) -> Self {
        Self { column: column.into(), lower, lower_inclusive, upper, upper_inclusive }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct CapsuleScanRequest {
    pub ranges: Vec<ColumnRangePredicate>,
    pub projection: Option<Vec<String>>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CapsuleScanDiagnostics {
    pub capsules_considered: u64,
    pub capsules_skipped: u64,
    pub capsules_read: u64,
    pub rows_examined: u64,
    pub rows_returned: u64,
    /// Bytes read to verify Capsule files against the immutable Navigation Map.
    pub bytes_verified: u64,
    /// Embedded ANF bytes decoded from candidate Capsules.
    pub payload_bytes_decoded: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CapsuleScanResult {
    pub rows: Vec<Vec<Value>>,
    pub diagnostics: CapsuleScanDiagnostics,
}

#[derive(Debug, Clone)]
pub(crate) struct NavigationMapEntry {
    pub metadata: CapsuleMetadata,
    pub file_len: u64,
    pub capsule_sha256: [u8; 32],
}

#[derive(Debug, Clone)]
pub(crate) struct NavigationMap {
    pub table_id: u64,
    pub generation: u64,
    pub entries: Vec<NavigationMapEntry>,
}

impl NavigationMap {
    pub fn encode(&self) -> Result<Vec<u8>, DbError> {
        validate_map(self)?;
        let mut output = Vec::new();
        output.extend_from_slice(MAGIC);
        output.extend_from_slice(&VERSION.to_le_bytes());
        output.extend_from_slice(&0_u16.to_le_bytes());
        output.extend_from_slice(&self.table_id.to_le_bytes());
        output.extend_from_slice(&self.generation.to_le_bytes());
        output.extend_from_slice(&u32::try_from(self.entries.len()).map_err(|_| DbError::LengthOverflow)?.to_le_bytes());
        output.extend_from_slice(&0_u32.to_le_bytes());
        if output.len() != HEADER_LEN {
            return Err(DbError::NavigationMapCorrupt("internal header length mismatch"));
        }

        for entry in &self.entries {
            let mut record = Vec::new();
            let metadata = &entry.metadata;
            record.extend_from_slice(&metadata.capsule_id.to_le_bytes());
            record.extend_from_slice(&metadata.generation.to_le_bytes());
            record.extend_from_slice(&entry.file_len.to_le_bytes());
            record.extend_from_slice(&entry.capsule_sha256);
            record.extend_from_slice(&metadata.schema_revision.to_le_bytes());
            record.extend_from_slice(&metadata.row_count.to_le_bytes());
            record.extend_from_slice(&metadata.first_ingestion_sequence.to_le_bytes());
            record.extend_from_slice(&metadata.last_ingestion_sequence.to_le_bytes());
            record.extend_from_slice(&metadata.event_time_column_id.unwrap_or(0).to_le_bytes());
            let mut flags = 0_u8;
            if metadata.event_time_column_id.is_some() { flags |= 1; }
            if metadata.temporal_min.is_some() { flags |= 2; }
            record.push(flags);
            record.extend_from_slice(&[0_u8; 7]);
            record.extend_from_slice(&metadata.temporal_min.unwrap_or(0).to_le_bytes());
            record.extend_from_slice(&metadata.temporal_max.unwrap_or(0).to_le_bytes());
            record.extend_from_slice(&metadata.payload_len.to_le_bytes());
            record.extend_from_slice(&metadata.payload_sha256);
            record.extend_from_slice(&u32::try_from(metadata.columns.len()).map_err(|_| DbError::LengthOverflow)?.to_le_bytes());
            if record.len() != ENTRY_FIXED_LEN {
                return Err(DbError::NavigationMapCorrupt("internal entry length mismatch"));
            }
            for column in &metadata.columns {
                encode_column_descriptor(&mut record, column)?;
            }
            output.extend_from_slice(&u32::try_from(record.len()).map_err(|_| DbError::LengthOverflow)?.to_le_bytes());
            output.extend_from_slice(&record);
            if output.len().checked_add(TRAILER_LEN).ok_or(DbError::LengthOverflow)? > MAX_MAP_BYTES {
                return Err(DbError::NavigationMapCorrupt("Navigation Map exceeds size limit"));
            }
        }

        let checksum = checksum64(&output);
        output.extend_from_slice(&checksum.to_le_bytes());
        output.extend_from_slice(END);
        Ok(output)
    }

    pub fn decode(bytes: &[u8], expected_table_id: u64, expected_generation: u64) -> Result<Self, DbError> {
        if bytes.len() < HEADER_LEN + TRAILER_LEN {
            return Err(DbError::NavigationMapCorrupt("map is truncated"));
        }
        if bytes.len() > MAX_MAP_BYTES {
            return Err(DbError::NavigationMapCorrupt("map exceeds size limit"));
        }
        if &bytes[..4] != MAGIC || read_u16(bytes, 4)? != VERSION {
            return Err(DbError::NavigationMapCorrupt("invalid map magic or version"));
        }
        if read_u16(bytes, 6)? != 0 || read_u32(bytes, 28)? != 0 {
            return Err(DbError::NavigationMapCorrupt("unknown flags or nonzero reserved header bytes"));
        }
        let table_id = read_u64(bytes, 8)?;
        let generation = read_u64(bytes, 16)?;
        let entry_count = usize::try_from(read_u32(bytes, 24)?).map_err(|_| DbError::LengthOverflow)?;
        if table_id == 0 || generation == 0 || table_id != expected_table_id || generation != expected_generation {
            return Err(DbError::NavigationMapCorrupt("table or map generation identity mismatch"));
        }

        let trailer_start = bytes.len().checked_sub(TRAILER_LEN).ok_or(DbError::NavigationMapCorrupt("missing trailer"))?;
        if &bytes[trailer_start + 8..] != END || checksum64(&bytes[..trailer_start]) != read_u64(bytes, trailer_start)? {
            return Err(DbError::NavigationMapCorrupt("map checksum or end marker mismatch"));
        }

        let mut entries = Vec::with_capacity(entry_count);
        let mut cursor = HEADER_LEN;
        let mut previous_identity = None;
        for _ in 0..entry_count {
            let record_len = usize::try_from(read_u32(bytes, cursor)?).map_err(|_| DbError::LengthOverflow)?;
            cursor = cursor.checked_add(4).ok_or(DbError::LengthOverflow)?;
            let record_end = cursor.checked_add(record_len).ok_or(DbError::LengthOverflow)?;
            if record_len < ENTRY_FIXED_LEN || record_end > trailer_start {
                return Err(DbError::NavigationMapCorrupt("invalid entry length"));
            }
            let record = &bytes[cursor..record_end];
            let mut position = 0_usize;
            let capsule_id = take_u64(record, &mut position)?;
            let capsule_generation = take_u64(record, &mut position)?;
            let file_len = take_u64(record, &mut position)?;
            let capsule_sha256 = take_array::<32>(record, &mut position)?;
            let schema_revision = take_u64(record, &mut position)?;
            let row_count = take_u64(record, &mut position)?;
            let first_ingestion_sequence = take_u64(record, &mut position)?;
            let last_ingestion_sequence = take_u64(record, &mut position)?;
            let event_time_raw = take_u64(record, &mut position)?;
            let flags = take_u8(record, &mut position)?;
            if flags & !3 != 0 || take_array::<7>(record, &mut position)? != [0; 7] {
                return Err(DbError::NavigationMapCorrupt("unknown entry flags or reserved bytes"));
            }
            let temporal_min_raw = take_i64(record, &mut position)?;
            let temporal_max_raw = take_i64(record, &mut position)?;
            let payload_len = take_u64(record, &mut position)?;
            let payload_sha256 = take_array::<32>(record, &mut position)?;
            let column_count = usize::try_from(take_u32(record, &mut position)?).map_err(|_| DbError::LengthOverflow)?;
            if column_count == 0 || column_count > 4_096
                || record_len != ENTRY_FIXED_LEN + column_count * 48
                || position != ENTRY_FIXED_LEN
            {
                return Err(DbError::NavigationMapCorrupt("invalid column descriptor count or entry size"));
            }

            let event_time_column_id = if flags & 1 != 0 {
                if event_time_raw == 0 { return Err(DbError::NavigationMapCorrupt("zero event-time ColumnId")); }
                Some(event_time_raw)
            } else {
                if event_time_raw != 0 { return Err(DbError::NavigationMapCorrupt("event-time ColumnId without flag")); }
                None
            };
            let has_temporal = flags & 2 != 0;
            let (temporal_min, temporal_max) = if has_temporal {
                (Some(temporal_min_raw), Some(temporal_max_raw))
            } else {
                if temporal_min_raw != 0 || temporal_max_raw != 0 {
                    return Err(DbError::NavigationMapCorrupt("temporal values without flag"));
                }
                (None, None)
            };

            let mut columns = Vec::with_capacity(column_count);
            for _ in 0..column_count {
                let end = position.checked_add(48).ok_or(DbError::LengthOverflow)?;
                columns.push(decode_column_descriptor(record.get(position..end)
                    .ok_or(DbError::NavigationMapCorrupt("truncated column descriptor"))?)?);
                position = end;
            }
            let metadata = CapsuleMetadata {
                table_id,
                capsule_id,
                generation: capsule_generation,
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
            let identity = (capsule_id, capsule_generation);
            if previous_identity.is_some_and(|previous| previous >= identity) {
                return Err(DbError::NavigationMapCorrupt("Capsule entries are not in unique canonical order"));
            }
            previous_identity = Some(identity);
            if capsule_id == 0 || capsule_generation == 0 || file_len == 0 || schema_revision == 0
                || row_count == 0 || first_ingestion_sequence == 0
                || first_ingestion_sequence > last_ingestion_sequence || payload_len == 0
            {
                return Err(DbError::NavigationMapCorrupt("invalid Capsule entry identity or bounds"));
            }
            validate_entry_columns(&metadata)?;
            entries.push(NavigationMapEntry { metadata, file_len, capsule_sha256 });
            cursor = record_end;
        }
        if cursor != trailer_start {
            return Err(DbError::NavigationMapCorrupt("trailing bytes before map checksum"));
        }
        let map = Self { table_id, generation, entries };
        validate_map(&map)?;
        Ok(map)
    }
}

fn validate_map(map: &NavigationMap) -> Result<(), DbError> {
    if map.table_id == 0 || map.generation == 0 {
        return Err(DbError::NavigationMapCorrupt("map identity must be nonzero"));
    }
    let mut previous = None;
    for entry in &map.entries {
        let metadata = &entry.metadata;
        let identity = (metadata.capsule_id, metadata.generation);
        if previous.is_some_and(|value| value >= identity) {
            return Err(DbError::NavigationMapCorrupt("Capsule identities are not uniquely sorted"));
        }
        previous = Some(identity);
        if metadata.table_id != map.table_id || metadata.capsule_id == 0 || metadata.generation == 0
            || entry.file_len == 0 || metadata.row_count == 0
            || metadata.first_ingestion_sequence == 0
            || metadata.first_ingestion_sequence > metadata.last_ingestion_sequence
            || metadata.columns.is_empty()
        {
            return Err(DbError::NavigationMapCorrupt("invalid Capsule entry"));
        }
        validate_entry_columns(metadata)?;
    }
    Ok(())
}

fn validate_entry_columns(metadata: &CapsuleMetadata) -> Result<(), DbError> {
    let mut ids = BTreeSet::new();
    for column in &metadata.columns {
        if column.column_id == 0 || !ids.insert(column.column_id)
            || column.null_count > metadata.row_count
        {
            return Err(DbError::NavigationMapCorrupt("invalid or duplicate column statistics"));
        }
        match (&column.minimum, &column.maximum) {
            (Some(min), Some(max)) => {
                if min.data_type() != Some(column.data_type) || max.data_type() != Some(column.data_type)
                    || compare_values(min, max).is_gt()
                {
                    return Err(DbError::NavigationMapCorrupt("invalid column min/max statistics"));
                }
            }
            (None, None) => {}
            _ => return Err(DbError::NavigationMapCorrupt("column min/max must be a pair")),
        }
    }
    if let Some(event_id) = metadata.event_time_column_id {
        let column = metadata.columns.iter().find(|column| column.column_id == event_id)
            .ok_or(DbError::NavigationMapCorrupt("event-time column is absent"))?;
        if column.data_type != DataType::TimestampMicros {
            return Err(DbError::NavigationMapCorrupt("event-time column is not TIMESTAMP_MICROS"));
        }
        match (&metadata.temporal_min, &metadata.temporal_max, &column.minimum, &column.maximum) {
            (Some(min), Some(max), Some(Value::TimestampMicros(col_min)), Some(Value::TimestampMicros(col_max)))
                if min <= max && min == col_min && max == col_max => {}
            (None, None, None, None) => {}
            _ => return Err(DbError::NavigationMapCorrupt("temporal bounds differ from event-time statistics")),
        }
    } else if metadata.temporal_min.is_some() || metadata.temporal_max.is_some() {
        return Err(DbError::NavigationMapCorrupt("temporal bounds exist without event-time column"));
    }
    Ok(())
}

pub(crate) fn range_may_match(
    minimum: Option<&Value>,
    maximum: Option<&Value>,
    predicate: &ColumnRangePredicate,
) -> bool {
    let (Some(minimum), Some(maximum)) = (minimum, maximum) else {
        // Missing statistics must never eliminate a Capsule.
        return true;
    };
    if let Some(lower) = predicate.lower.as_ref() {
        let ordering = compare_values(maximum, lower);
        if ordering.is_lt() || (ordering.is_eq() && !predicate.lower_inclusive) {
            return false;
        }
    }
    if let Some(upper) = predicate.upper.as_ref() {
        let ordering = compare_values(minimum, upper);
        if ordering.is_gt() || (ordering.is_eq() && !predicate.upper_inclusive) {
            return false;
        }
    }
    true
}

pub(crate) fn value_matches(value: &Value, predicate: &ColumnRangePredicate) -> bool {
    if matches!(value, Value::Null) { return false; }
    if let Some(lower) = predicate.lower.as_ref() {
        let ordering = compare_values(value, lower);
        if ordering.is_lt() || (ordering.is_eq() && !predicate.lower_inclusive) { return false; }
    }
    if let Some(upper) = predicate.upper.as_ref() {
        let ordering = compare_values(value, upper);
        if ordering.is_gt() || (ordering.is_eq() && !predicate.upper_inclusive) { return false; }
    }
    true
}

pub(crate) fn file_sha256(bytes: &[u8]) -> [u8; 32] {
    crate::chronicle::sha256(bytes)
}

pub(crate) fn map_filename(generation: u64) -> String {
    format!("navigation-{generation:020}.nvm")
}

fn checksum64(bytes: &[u8]) -> u64 {
    let mut checksum = 0xcbf29ce484222325_u64;
    for byte in bytes {
        checksum ^= u64::from(*byte);
        checksum = checksum.wrapping_mul(0x100000001b3);
    }
    checksum
}

fn take_u8(bytes: &[u8], cursor: &mut usize) -> Result<u8, DbError> {
    let value = *bytes.get(*cursor).ok_or(DbError::NavigationMapCorrupt("truncated map entry"))?;
    *cursor += 1;
    Ok(value)
}

fn take_array<const N: usize>(bytes: &[u8], cursor: &mut usize) -> Result<[u8; N], DbError> {
    let end = cursor.checked_add(N).ok_or(DbError::LengthOverflow)?;
    let value = bytes.get(*cursor..end).ok_or(DbError::NavigationMapCorrupt("truncated map entry"))?;
    *cursor = end;
    value.try_into().map_err(|_| DbError::LengthOverflow)
}

fn take_u32(bytes: &[u8], cursor: &mut usize) -> Result<u32, DbError> {
    Ok(u32::from_le_bytes(take_array::<4>(bytes, cursor)?))
}

fn take_u64(bytes: &[u8], cursor: &mut usize) -> Result<u64, DbError> {
    Ok(u64::from_le_bytes(take_array::<8>(bytes, cursor)?))
}

fn take_i64(bytes: &[u8], cursor: &mut usize) -> Result<i64, DbError> {
    Ok(i64::from_le_bytes(take_array::<8>(bytes, cursor)?))
}

fn read_u16(bytes: &[u8], offset: usize) -> Result<u16, DbError> {
    Ok(u16::from_le_bytes(bytes.get(offset..offset + 2)
        .ok_or(DbError::NavigationMapCorrupt("truncated map integer"))?
        .try_into().map_err(|_| DbError::LengthOverflow)?))
}

fn read_u32(bytes: &[u8], offset: usize) -> Result<u32, DbError> {
    Ok(u32::from_le_bytes(bytes.get(offset..offset + 4)
        .ok_or(DbError::NavigationMapCorrupt("truncated map integer"))?
        .try_into().map_err(|_| DbError::LengthOverflow)?))
}

fn read_u64(bytes: &[u8], offset: usize) -> Result<u64, DbError> {
    Ok(u64::from_le_bytes(bytes.get(offset..offset + 8)
        .ok_or(DbError::NavigationMapCorrupt("truncated map integer"))?
        .try_into().map_err(|_| DbError::LengthOverflow)?))
}

fn read_i64(bytes: &[u8], offset: usize) -> Result<i64, DbError> {
    Ok(i64::from_le_bytes(bytes.get(offset..offset + 8)
        .ok_or(DbError::NavigationMapCorrupt("truncated map integer"))?
        .try_into().map_err(|_| DbError::LengthOverflow)?))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn range_pruning_keeps_missing_stats_and_respects_inclusive_exclusive_edges() {
        let touching = ColumnRangePredicate::new("temperature", Some(Value::Float64(20.0)), true, Some(Value::Float64(30.0)), true);
        let exclusive_touch = ColumnRangePredicate::new("temperature", Some(Value::Float64(20.0)), false, Some(Value::Float64(30.0)), true);
        assert!(range_may_match(Some(&Value::Float64(10.0)), Some(&Value::Float64(20.0)), &touching));
        assert!(!range_may_match(Some(&Value::Float64(10.0)), Some(&Value::Float64(20.0)), &exclusive_touch));
        assert!(range_may_match(None, None, &exclusive_touch));
        assert!(!value_matches(&Value::Float64(20.0), &exclusive_touch));
    }

    #[test]
    fn navigation_map_round_trip_checks_identity_and_integrity() {
        let stats = CapsuleColumnStats {
            column_id: 10,
            data_type: DataType::TimestampMicros,
            null_count: 0,
            encoded_bytes: 20,
            minimum: Some(Value::TimestampMicros(10)),
            maximum: Some(Value::TimestampMicros(20)),
        };
        let metadata = CapsuleMetadata {
            table_id: 1,
            capsule_id: 2,
            generation: 3,
            schema_revision: 1,
            row_count: 2,
            first_ingestion_sequence: 1,
            last_ingestion_sequence: 2,
            event_time_column_id: Some(10),
            temporal_min: Some(10),
            temporal_max: Some(20),
            payload_len: 100,
            payload_sha256: [8; 32],
            columns: vec![stats],
        };
        let map = NavigationMap {
            table_id: 1,
            generation: 5,
            entries: vec![NavigationMapEntry { metadata, file_len: 300, capsule_sha256: [9; 32] }],
        };
        let bytes = map.encode().unwrap();
        let decoded = NavigationMap::decode(&bytes, 1, 5).unwrap();
        assert_eq!(decoded.entries.len(), 1);
        assert_eq!(decoded.entries[0].metadata.temporal_min, Some(10));
        assert_eq!(decoded.entries[0].capsule_sha256, [9; 32]);
        assert!(NavigationMap::decode(&bytes, 1, 6).is_err());
        let mut corrupt = bytes;
        corrupt[HEADER_LEN] ^= 1;
        assert!(NavigationMap::decode(&corrupt, 1, 5).is_err());
    }
}
