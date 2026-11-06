use crate::{estimate_batch_bytes, ExecutionError};
use ananta_core::Database;
use ananta_types::{Column, DataType, Value};
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
    let ingestion = database.ingestion_stats(table)?;
    Ok(TableStatistics {
        table_name: table.to_owned(),
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
    use ananta_types::Schema;
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
        assert_eq!(stats.chronicle_accepted_rows, 4);
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
