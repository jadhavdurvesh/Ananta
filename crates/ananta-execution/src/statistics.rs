use crate::{estimate_batch_bytes, ExecutionError};
use ananta_core::Database;
use ananta_types::{Column, DataType, Value};
use std::fs::{self, File};
use std::io::Write;
use std::path::Path;
use std::cmp::Ordering;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

const HLL_REGISTERS: usize = 64;

/// Per-column statistics collected from a full bounded scan. Distinct counts
/// are approximate; NULL counts and min/max are exact for the scanned snapshot.
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnStatistics {
    pub column: Column,
    pub null_count: u64,
    pub non_null_count: u64,
    pub approximate_distinct_count: u64,
    pub min_value: Option<Value>,
    pub max_value: Option<Value>,
}

/// Snapshot statistics for one table. scanned_rows is exact for this scan;
/// Chronicle accepted rows/bytes describe persisted ingestion accounting.
#[derive(Debug, Clone, PartialEq)]
pub struct TableStatistics {
    pub table_name: String,
    pub table_id: u64,
    pub schema_revision: u64,
    /// Fingerprint of the immutable scan-file inventory and active Weave identity
    /// at analysis time. EXPLAIN rejects the statistics if this token has changed.
    pub snapshot_token: [u8; 32],
    pub scanned_rows: u64,
    pub estimated_batch_payload_bytes: u64,
    pub chronicle_accepted_rows: u64,
    pub chronicle_accepted_encoded_bytes: u64,
    pub columns: Vec<ColumnStatistics>,
}

#[derive(Debug, Clone)]
struct HyperLogLog {
    registers: [u8; HLL_REGISTERS],
}

impl Default for HyperLogLog {
    fn default() -> Self {
        Self { registers: [0; HLL_REGISTERS] }
    }
}

impl HyperLogLog {
    fn insert(&mut self, value: &Value) {
        let hash = hash_value(value);
        let index = (hash >> 58) as usize;
        let remaining = hash << 6;
        let rank = (remaining.leading_zeros() + 1).min(59) as u8;
        self.registers[index] = self.registers[index].max(rank);
    }

    fn estimate(&self) -> u64 {
        let m = HLL_REGISTERS as f64;
        let sum = self.registers
            .iter()
            .map(|rank| 2.0_f64.powi(-i32::from(*rank)))
            .sum::<f64>();
        let raw = 0.709 * m * m / sum;
        let zero_registers = self.registers.iter().filter(|rank| **rank == 0).count();
        let estimate = if zero_registers > 0 && raw <= 2.5 * m {
            m * (m / zero_registers as f64).ln()
        } else {
            raw
        };
        if estimate.is_finite() && estimate > 0.0 {
            estimate.round().min(u64::MAX as f64) as u64
        } else {
            0
        }
    }
}

fn hash_value(value: &Value) -> u64 {
    let mut hasher = DefaultHasher::new();
    match value {
        Value::Null => 0_u8.hash(&mut hasher),
        Value::Boolean(value) => {
            1_u8.hash(&mut hasher);
            value.hash(&mut hasher);
        }
        Value::Int32(value) => {
            2_u8.hash(&mut hasher);
            value.hash(&mut hasher);
        }
        Value::Int64(value) => {
            3_u8.hash(&mut hasher);
            value.hash(&mut hasher);
        }
        Value::Float64(value) => {
            4_u8.hash(&mut hasher);
            let bits = if *value == 0.0 {
                0
            } else if value.is_nan() {
                f64::NAN.to_bits()
            } else {
                value.to_bits()
            };
            bits.hash(&mut hasher);
        }
        Value::Utf8(value) => {
            5_u8.hash(&mut hasher);
            value.hash(&mut hasher);
        }
        Value::TimestampMicros(value) => {
            6_u8.hash(&mut hasher);
            value.hash(&mut hasher);
        }
        Value::Binary(value) => {
            7_u8.hash(&mut hasher);
            value.hash(&mut hasher);
        }
    }
    hasher.finish()
}

fn compare_stat_values(left: &Value, right: &Value) -> Result<Ordering, ExecutionError> {
    match (left, right) {
        (Value::Boolean(left), Value::Boolean(right)) => Ok(left.cmp(right)),
        (Value::Int32(left), Value::Int32(right)) => Ok(left.cmp(right)),
        (Value::Int64(left), Value::Int64(right)) => Ok(left.cmp(right)),
        (Value::TimestampMicros(left), Value::TimestampMicros(right)) => Ok(left.cmp(right)),
        (Value::Float64(left), Value::Float64(right)) => Ok(left.total_cmp(right)),
        (Value::Utf8(left), Value::Utf8(right)) => Ok(left.cmp(right)),
        (Value::Binary(left), Value::Binary(right)) => Ok(left.cmp(right)),
        _ => Err(ExecutionError::TypeMismatch(
            "column statistics encountered values with incompatible data types".into(),
        )),
    }
}

struct ColumnAccumulator {
    column: Column,
    null_count: u64,
    non_null_count: u64,
    distinct: HyperLogLog,
    min_value: Option<Value>,
    max_value: Option<Value>,
}

impl ColumnAccumulator {
    fn new(column: Column) -> Self {
        Self {
            column,
            null_count: 0,
            non_null_count: 0,
            distinct: HyperLogLog::default(),
            min_value: None,
            max_value: None,
        }
    }

    fn add(&mut self, value: &Value) -> Result<(), ExecutionError> {
        if matches!(value, Value::Null) {
            self.null_count = self.null_count
                .checked_add(1)
                .ok_or(ExecutionError::ArithmeticOverflow)?;
            return Ok(());
        }
        self.non_null_count = self.non_null_count
            .checked_add(1)
            .ok_or(ExecutionError::ArithmeticOverflow)?;
        self.distinct.insert(value);

        let replace_min = match &self.min_value {
            None => true,
            Some(current) => compare_stat_values(value, current)? == Ordering::Less,
        };
        if replace_min {
            self.min_value = Some(value.clone());
        }
        let replace_max = match &self.max_value {
            None => true,
            Some(current) => compare_stat_values(value, current)? == Ordering::Greater,
        };
        if replace_max {
            self.max_value = Some(value.clone());
        }
        Ok(())
    }

    fn finish(self) -> ColumnStatistics {
        ColumnStatistics {
            column: self.column,
            null_count: self.null_count,
            non_null_count: self.non_null_count,
            approximate_distinct_count: self.distinct.estimate(),
            min_value: self.min_value,
            max_value: self.max_value,
        }
    }
}

/// Collect exact row/null/min/max counts and approximate distinct counts using
/// the database's bounded storage scan. This is an explicit analysis scan and
/// can read the full table; it is not automatically run for every query.
pub fn analyze_table(
    database: &Database,
    table: &str,
    batch_rows: usize,
) -> Result<TableStatistics, ExecutionError> {
    if batch_rows == 0 {
        return Err(ExecutionError::InvalidPlan(
            "statistics batch_rows must be greater than zero".into(),
        ));
    }
    let schema = database.schema(table)?;
    let table_id = database.table_id(table)?.0;
    let schema_revision = database.table_schema_revision(table)?;
    let snapshot_token = database.table_scan_snapshot_token(table)?;
    let mut columns = schema.columns().iter().cloned()
        .map(ColumnAccumulator::new)
        .collect::<Vec<_>>();
    let mut scanned_rows = 0_u64;
    let mut estimated_batch_payload_bytes = 0_u64;
    let mut scan = database.scan_batches(table, batch_rows)?;
    while let Some(batch) = scan.next() {
        let batch = batch?;
        scanned_rows = scanned_rows
            .checked_add(u64::try_from(batch.row_count).map_err(|_| ExecutionError::ArithmeticOverflow)?)
            .ok_or(ExecutionError::ArithmeticOverflow)?;
        estimated_batch_payload_bytes = estimated_batch_payload_bytes
            .checked_add(u64::try_from(estimate_batch_bytes(&batch)?)
                .map_err(|_| ExecutionError::ArithmeticOverflow)?)
            .ok_or(ExecutionError::ArithmeticOverflow)?;
        for (column_stats, column) in columns.iter_mut().zip(&batch.columns) {
            for value in &column.values {
                column_stats.add(value)?;
            }
        }
    }
    let final_snapshot_token = database.table_scan_snapshot_token(table)?;
    if final_snapshot_token != snapshot_token {
        return Err(ExecutionError::InvalidPlan(
            "table scan inventory changed during analysis; rerun table analysis".into(),
        ));
    }
    let ingestion = database.ingestion_stats(table)?;
    Ok(TableStatistics {
        table_name: table.to_owned(),
        table_id,
        schema_revision,
        snapshot_token,
        scanned_rows,
        estimated_batch_payload_bytes,
        chronicle_accepted_rows: ingestion.accepted_rows,
        chronicle_accepted_encoded_bytes: ingestion.accepted_encoded_bytes,
        columns: columns.into_iter().map(ColumnAccumulator::finish).collect(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use ananta_core::Database;
    use ananta_types::{DataType, Schema};
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn table_analysis_reports_exact_null_min_max_and_bounded_ndv_estimate() {
        let nonce = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
        let root = std::env::temp_dir().join(format!("ananta-statistics-{nonce}"));
        let mut database = Database::open(&root).unwrap();
        database.create_table(
            "readings",
            Schema::new(vec![
                Column::new("device", DataType::Utf8, false),
                Column::new("value", DataType::Int64, true),
            ]).unwrap(),
        ).unwrap();
        database.insert_batch("readings", &[
            vec![Value::Utf8("a".into()), Value::Int64(10)],
            vec![Value::Utf8("b".into()), Value::Null],
            vec![Value::Utf8("a".into()), Value::Int64(30)],
            vec![Value::Utf8("c".into()), Value::Int64(20)],
        ]).unwrap();

        let stats = analyze_table(&database, "readings", 2).unwrap();
        assert_eq!(stats.scanned_rows, 4);
        assert_eq!(stats.columns[1].null_count, 1);
        assert_eq!(stats.columns[1].non_null_count, 3);
        assert_eq!(stats.columns[1].min_value, Some(Value::Int64(10)));
        assert_eq!(stats.columns[1].max_value, Some(Value::Int64(30)));
        assert!(stats.columns[0].approximate_distinct_count >= 2);
        assert!(stats.columns[0].approximate_distinct_count <= 4);
        assert!(stats.estimated_batch_payload_bytes > 0);
        assert_eq!(stats.chronicle_accepted_rows, 0);

        let encoded = stats.encode().unwrap();
        let decoded = TableStatistics::decode(&encoded).unwrap();
        assert_eq!(decoded, stats);

        let cache_path = root.join("readings.ast");
        stats.save_to(&cache_path).unwrap();
        let loaded = TableStatistics::load_from(&cache_path).unwrap();
        assert_eq!(loaded, stats);

        let mut corrupt = encoded;
        corrupt[14] ^= 0x01;
        assert!(matches!(
            TableStatistics::decode(&corrupt),
            Err(StatisticsFormatError::Corrupt("checksum mismatch"))
        ));

        std::fs::remove_file(cache_path).unwrap();
        drop(database);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn analysis_rejects_zero_batch_size() {
        let nonce = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
        let root = std::env::temp_dir().join(format!("ananta-statistics-invalid-{nonce}"));
        let database = Database::open(&root).unwrap();
        assert!(matches!(
            analyze_table(&database, "missing", 0),
            Err(ExecutionError::InvalidPlan(_))
        ));
        drop(database);
        std::fs::remove_dir_all(root).unwrap();
    }
}


const STATS_MAGIC: &[u8; 4] = b"AST1";
const STATS_VERSION: u16 = 1;
const MAX_STATS_FILE_BYTES: usize = 64 * 1024 * 1024;
const MAX_STATS_COLUMNS: usize = 65_536;
const MAX_STATS_STRING_BYTES: usize = 1_048_576;

/// Errors while reading or writing a persisted analysis snapshot.
#[derive(Debug)]
pub enum StatisticsFormatError {
    Io(std::io::Error),
    Corrupt(&'static str),
    LimitExceeded(&'static str),
}

impl std::fmt::Display for StatisticsFormatError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "statistics I/O error: {error}"),
            Self::Corrupt(message) => write!(formatter, "corrupt ANANTA statistics snapshot: {message}"),
            Self::LimitExceeded(message) => write!(formatter, "statistics snapshot limit exceeded: {message}"),
        }
    }
}

impl std::error::Error for StatisticsFormatError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            _ => None,
        }
    }
}

impl From<std::io::Error> for StatisticsFormatError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

impl TableStatistics {
    /// Encode a versioned, checksummed analysis snapshot.
    ///
    /// This file is a disposable statistics cache, not a transaction log or
    /// source of table data. A caller must still pass the decoded result through
    /// EXPLAIN's table/schema/snapshot freshness checks before planning.
    pub fn encode(&self) -> Result<Vec<u8>, StatisticsFormatError> {
        validate_statistics(self)?;
        if self.columns.len() > MAX_STATS_COLUMNS {
            return Err(StatisticsFormatError::LimitExceeded("column count"));
        }

        let mut body = StatsWriter::default();
        body.string(&self.table_name)?;
        body.u64(self.table_id);
        body.u64(self.schema_revision);
        body.bytes(&self.snapshot_token);
        body.u64(self.scanned_rows);
        body.u64(self.estimated_batch_payload_bytes);
        body.u64(self.chronicle_accepted_rows);
        body.u64(self.chronicle_accepted_encoded_bytes);
        body.u32(u32::try_from(self.columns.len())
            .map_err(|_| StatisticsFormatError::LimitExceeded("column count"))?);

        for column in &self.columns {
            body.string(&column.column.name)?;
            body.u8(column.column.data_type as u8);
            body.u8(u8::from(column.column.nullable));
            body.u64(column.null_count);
            body.u64(column.non_null_count);
            body.u64(column.approximate_distinct_count);
            body.optional_value(column.min_value.as_ref())?;
            body.optional_value(column.max_value.as_ref())?;
        }

        let body = body.finish();
        let total_len = 4_usize
            .checked_add(2)
            .and_then(|value| value.checked_add(8))
            .and_then(|value| value.checked_add(body.len()))
            .and_then(|value| value.checked_add(8))
            .ok_or(StatisticsFormatError::LimitExceeded("file length"))?;
        if total_len > MAX_STATS_FILE_BYTES {
            return Err(StatisticsFormatError::LimitExceeded("file size"));
        }

        let mut output = Vec::with_capacity(total_len);
        output.extend_from_slice(STATS_MAGIC);
        output.extend_from_slice(&STATS_VERSION.to_le_bytes());
        output.extend_from_slice(&u64::try_from(body.len())
            .map_err(|_| StatisticsFormatError::LimitExceeded("body length"))?
            .to_le_bytes());
        output.extend_from_slice(&body);
        output.extend_from_slice(&checksum64(&body).to_le_bytes());
        Ok(output)
    }

    /// Decode a versioned statistics snapshot and reject malformed or
    /// inconsistent values. Data freshness is checked separately by EXPLAIN.
    pub fn decode(bytes: &[u8]) -> Result<Self, StatisticsFormatError> {
        if bytes.len() > MAX_STATS_FILE_BYTES {
            return Err(StatisticsFormatError::LimitExceeded("file size"));
        }
        if bytes.len() < 22 || &bytes[..4] != STATS_MAGIC {
            return Err(StatisticsFormatError::Corrupt("invalid header"));
        }
        let version = u16::from_le_bytes([bytes[4], bytes[5]]);
        if version != STATS_VERSION {
            return Err(StatisticsFormatError::Corrupt("unsupported version"));
        }
        let body_len_u64 = u64::from_le_bytes(bytes[6..14].try_into()
            .map_err(|_| StatisticsFormatError::Corrupt("invalid body length"))?);
        let body_len = usize::try_from(body_len_u64)
            .map_err(|_| StatisticsFormatError::LimitExceeded("body length"))?;
        let expected_len = 14_usize
            .checked_add(body_len)
            .and_then(|value| value.checked_add(8))
            .ok_or(StatisticsFormatError::LimitExceeded("file length"))?;
        if expected_len != bytes.len() {
            return Err(StatisticsFormatError::Corrupt("body length does not match file size"));
        }
        let body = &bytes[14..14 + body_len];
        let expected_checksum = u64::from_le_bytes(bytes[14 + body_len..]
            .try_into()
            .map_err(|_| StatisticsFormatError::Corrupt("missing checksum"))?);
        if checksum64(body) != expected_checksum {
            return Err(StatisticsFormatError::Corrupt("checksum mismatch"));
        }

        let mut reader = StatsReader::new(body);
        let table_name = reader.string()?;
        let table_id = reader.u64()?;
        let schema_revision = reader.u64()?;
        let mut snapshot_token = [0_u8; 32];
        snapshot_token.copy_from_slice(reader.take(32)?);
        let scanned_rows = reader.u64()?;
        let estimated_batch_payload_bytes = reader.u64()?;
        let chronicle_accepted_rows = reader.u64()?;
        let chronicle_accepted_encoded_bytes = reader.u64()?;
        let column_count = usize::try_from(reader.u32()?)
            .map_err(|_| StatisticsFormatError::LimitExceeded("column count"))?;
        if column_count == 0 || column_count > MAX_STATS_COLUMNS {
            return Err(StatisticsFormatError::LimitExceeded("column count"));
        }

        let mut columns = Vec::with_capacity(column_count);
        for _ in 0..column_count {
            let name = reader.string()?;
            let data_type = DataType::from_code(reader.u8()?)
                .ok_or(StatisticsFormatError::Corrupt("unknown column data type"))?;
            let nullable = match reader.u8()? {
                0 => false,
                1 => true,
                _ => return Err(StatisticsFormatError::Corrupt("invalid nullability flag")),
            };
            let null_count = reader.u64()?;
            let non_null_count = reader.u64()?;
            let approximate_distinct_count = reader.u64()?;
            let min_value = reader.optional_value()?;
            let max_value = reader.optional_value()?;
            columns.push(ColumnStatistics {
                column: Column::new(name, data_type, nullable),
                null_count,
                non_null_count,
                approximate_distinct_count,
                min_value,
                max_value,
            });
        }
        if !reader.is_finished() {
            return Err(StatisticsFormatError::Corrupt("unexpected trailing body data"));
        }

        let statistics = Self {
            table_name,
            table_id,
            schema_revision,
            snapshot_token,
            scanned_rows,
            estimated_batch_payload_bytes,
            chronicle_accepted_rows,
            chronicle_accepted_encoded_bytes,
            columns,
        };
        validate_statistics(&statistics)?;
        Ok(statistics)
    }

    /// Persist the cache at a caller-selected path. Statistics are disposable:
    /// a partial/corrupt file is rejected by load_from and can be regenerated.
    pub fn save_to(&self, path: impl AsRef<Path>) -> Result<(), StatisticsFormatError> {
        let bytes = self.encode()?;
        let mut file = File::create(path)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        Ok(())
    }

    pub fn load_from(path: impl AsRef<Path>) -> Result<Self, StatisticsFormatError> {
        let metadata = fs::metadata(path.as_ref())?;
        let length = usize::try_from(metadata.len())
            .map_err(|_| StatisticsFormatError::LimitExceeded("file size"))?;
        if length > MAX_STATS_FILE_BYTES {
            return Err(StatisticsFormatError::LimitExceeded("file size"));
        }
        let bytes = fs::read(path)?;
        Self::decode(&bytes)
    }
}

fn validate_statistics(statistics: &TableStatistics) -> Result<(), StatisticsFormatError> {
    if statistics.table_name.is_empty()
        || statistics.table_id == 0
        || statistics.schema_revision == 0
        || statistics.columns.is_empty()
    {
        return Err(StatisticsFormatError::Corrupt("missing table identity or columns"));
    }
    if statistics.table_name.len() > MAX_STATS_STRING_BYTES
        || statistics.columns.len() > MAX_STATS_COLUMNS
    {
        return Err(StatisticsFormatError::LimitExceeded("table name or column count"));
    }
    for column in &statistics.columns {
        if column.column.name.is_empty()
            || column.column.name.len() > MAX_STATS_STRING_BYTES
        {
            return Err(StatisticsFormatError::LimitExceeded("column name"));
        }
        let row_total = column.null_count.checked_add(column.non_null_count)
            .ok_or(StatisticsFormatError::Corrupt("column row count overflow"))?;
        if row_total != statistics.scanned_rows {
            return Err(StatisticsFormatError::Corrupt("column counts do not equal scanned row count"));
        }
        if let Some(minimum) = &column.min_value {
            if minimum.data_type() != Some(column.column.data_type) {
                return Err(StatisticsFormatError::Corrupt("minimum value has the wrong type"));
            }
        }
        if let Some(maximum) = &column.max_value {
            if maximum.data_type() != Some(column.column.data_type) {
                return Err(StatisticsFormatError::Corrupt("maximum value has the wrong type"));
            }
        }
        if (column.non_null_count == 0) != (column.min_value.is_none() && column.max_value.is_none()) {
            return Err(StatisticsFormatError::Corrupt("minimum/maximum presence differs from non-NULL count"));
        }
        if column.min_value.is_some() != column.max_value.is_some() {
            return Err(StatisticsFormatError::Corrupt("only one of minimum/maximum is present"));
        }
    }
    Ok(())
}

#[derive(Default)]
struct StatsWriter {
    bytes: Vec<u8>,
}

impl StatsWriter {
    fn u8(&mut self, value: u8) { self.bytes.push(value); }
    fn u32(&mut self, value: u32) { self.bytes.extend_from_slice(&value.to_le_bytes()); }
    fn u64(&mut self, value: u64) { self.bytes.extend_from_slice(&value.to_le_bytes()); }
    fn bytes(&mut self, value: &[u8]) { self.bytes.extend_from_slice(value); }

    fn string(&mut self, value: &str) -> Result<(), StatisticsFormatError> {
        if value.len() > MAX_STATS_STRING_BYTES {
            return Err(StatisticsFormatError::LimitExceeded("string length"));
        }
        self.u32(u32::try_from(value.len())
            .map_err(|_| StatisticsFormatError::LimitExceeded("string length"))?);
        self.bytes(value.as_bytes());
        Ok(())
    }

    fn optional_value(&mut self, value: Option<&Value>) -> Result<(), StatisticsFormatError> {
        match value {
            None => self.u8(0),
            Some(value) => {
                self.u8(1);
                self.value(value)?;
            }
        }
        Ok(())
    }

    fn value(&mut self, value: &Value) -> Result<(), StatisticsFormatError> {
        match value {
            Value::Null => self.u8(0),
            Value::Boolean(value) => { self.u8(1); self.u8(u8::from(*value)); }
            Value::Int32(value) => { self.u8(2); self.bytes(&value.to_le_bytes()); }
            Value::Int64(value) => { self.u8(3); self.bytes(&value.to_le_bytes()); }
            Value::Float64(value) => { self.u8(4); self.bytes(&value.to_bits().to_le_bytes()); }
            Value::Utf8(value) => { self.u8(5); self.string(value)?; }
            Value::TimestampMicros(value) => { self.u8(6); self.bytes(&value.to_le_bytes()); }
            Value::Binary(value) => {
                self.u8(7);
                if value.len() > MAX_STATS_FILE_BYTES {
                    return Err(StatisticsFormatError::LimitExceeded("binary statistic value"));
                }
                self.u32(u32::try_from(value.len())
                    .map_err(|_| StatisticsFormatError::LimitExceeded("binary value length"))?);
                self.bytes(value);
            }
        }
        Ok(())
    }

    fn finish(self) -> Vec<u8> { self.bytes }
}

struct StatsReader<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> StatsReader<'a> {
    fn new(bytes: &'a [u8]) -> Self { Self { bytes, position: 0 } }

    fn take(&mut self, count: usize) -> Result<&'a [u8], StatisticsFormatError> {
        let end = self.position.checked_add(count)
            .ok_or(StatisticsFormatError::Corrupt("offset overflow"))?;
        if end > self.bytes.len() {
            return Err(StatisticsFormatError::Corrupt("truncated body"));
        }
        let value = &self.bytes[self.position..end];
        self.position = end;
        Ok(value)
    }

    fn u8(&mut self) -> Result<u8, StatisticsFormatError> {
        Ok(self.take(1)?[0])
    }

    fn u32(&mut self) -> Result<u32, StatisticsFormatError> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into()
            .map_err(|_| StatisticsFormatError::Corrupt("invalid u32"))?))
    }

    fn u64(&mut self) -> Result<u64, StatisticsFormatError> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into()
            .map_err(|_| StatisticsFormatError::Corrupt("invalid u64"))?))
    }

    fn string(&mut self) -> Result<String, StatisticsFormatError> {
        let length = usize::try_from(self.u32()?)
            .map_err(|_| StatisticsFormatError::LimitExceeded("string length"))?;
        if length > MAX_STATS_STRING_BYTES {
            return Err(StatisticsFormatError::LimitExceeded("string length"));
        }
        String::from_utf8(self.take(length)?.to_vec())
            .map_err(|_| StatisticsFormatError::Corrupt("invalid UTF8 string"))
    }

    fn optional_value(&mut self) -> Result<Option<Value>, StatisticsFormatError> {
        match self.u8()? {
            0 => Ok(None),
            1 => Ok(Some(self.value()?)),
            _ => Err(StatisticsFormatError::Corrupt("invalid optional value tag")),
        }
    }

    fn value(&mut self) -> Result<Value, StatisticsFormatError> {
        Ok(match self.u8()? {
            0 => Value::Null,
            1 => match self.u8()? {
                0 => Value::Boolean(false),
                1 => Value::Boolean(true),
                _ => return Err(StatisticsFormatError::Corrupt("invalid boolean value")),
            },
            2 => Value::Int32(i32::from_le_bytes(self.take(4)?.try_into()
                .map_err(|_| StatisticsFormatError::Corrupt("invalid INT32"))?)),
            3 => Value::Int64(i64::from_le_bytes(self.take(8)?.try_into()
                .map_err(|_| StatisticsFormatError::Corrupt("invalid INT64"))?)),
            4 => Value::Float64(f64::from_bits(u64::from_le_bytes(self.take(8)?.try_into()
                .map_err(|_| StatisticsFormatError::Corrupt("invalid FLOAT64"))?))),
            5 => Value::Utf8(self.string()?),
            6 => Value::TimestampMicros(i64::from_le_bytes(self.take(8)?.try_into()
                .map_err(|_| StatisticsFormatError::Corrupt("invalid timestamp"))?)),
            7 => {
                let length = usize::try_from(self.u32()?)
                    .map_err(|_| StatisticsFormatError::LimitExceeded("binary value length"))?;
                if length > MAX_STATS_FILE_BYTES {
                    return Err(StatisticsFormatError::LimitExceeded("binary value length"));
                }
                Value::Binary(self.take(length)?.to_vec())
            }
            _ => return Err(StatisticsFormatError::Corrupt("unknown value tag")),
        })
    }

    fn is_finished(&self) -> bool { self.position == self.bytes.len() }
}

fn checksum64(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf29ce484222325_u64;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}
