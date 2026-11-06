use crate::aggregation::{encode_group_key, AggregateFunction, AggregateSpec, GroupByOperator};
use crate::{
    estimate_batch_bytes, BatchColumn, ComparisonOperator, ExecutionContext, ExecutionError,
    ExecutionOperator, FilterOperator, FilterPredicate, Operand, RecordBatch, VectorBatch,
    VectorSchema,
};
use ananta_types::{Column, DataType, Value};
use std::collections::{btree_map::IntoIter, BTreeMap, VecDeque};
use std::mem::size_of;

/// One inclusive or exclusive boundary for a UTC TIMESTAMP_MICROS range.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TimestampBound {
    pub micros: i64,
    pub inclusive: bool,
}

impl TimestampBound {
    pub fn inclusive(micros: i64) -> Self {
        Self { micros, inclusive: true }
    }

    pub fn exclusive(micros: i64) -> Self {
        Self { micros, inclusive: false }
    }
}

/// Filters a timestamp column using optional lower and upper bounds. NULL
/// timestamps are excluded by SQL-style three-valued comparison semantics.
pub struct TimestampRangeOperator {
    input: FilterOperator,
}

impl TimestampRangeOperator {
    pub fn new(
        input: Box<dyn ExecutionOperator>,
        timestamp_column: usize,
        lower: Option<TimestampBound>,
        upper: Option<TimestampBound>,
    ) -> Result<Self, ExecutionError> {
        let schema = input.schema().clone();
        let column = schema.columns().get(timestamp_column).ok_or_else(|| {
            ExecutionError::InvalidPlan(format!(
                "timestamp range references missing input column {timestamp_column}"
            ))
        })?;
        if column.data_type != DataType::TimestampMicros {
            return Err(ExecutionError::TypeMismatch(format!(
                "timestamp range requires TIMESTAMP_MICROS, received {}",
                column.data_type
            )));
        }
        if let (Some(lower), Some(upper)) = (lower, upper) {
            if lower.micros > upper.micros {
                return Err(ExecutionError::InvalidPlan(
                    "timestamp range lower bound exceeds its upper bound".into(),
                ));
            }
        }

        let mut predicates = Vec::with_capacity(2);
        if let Some(bound) = lower {
            predicates.push(FilterPredicate::Compare {
                left: Operand::Column(timestamp_column),
                operator: if bound.inclusive {
                    ComparisonOperator::GreaterThanOrEqual
                } else {
                    ComparisonOperator::GreaterThan
                },
                right: Operand::Literal(Value::TimestampMicros(bound.micros)),
            });
        }
        if let Some(bound) = upper {
            predicates.push(FilterPredicate::Compare {
                left: Operand::Column(timestamp_column),
                operator: if bound.inclusive {
                    ComparisonOperator::LessThanOrEqual
                } else {
                    ComparisonOperator::LessThan
                },
                right: Operand::Literal(Value::TimestampMicros(bound.micros)),
            });
        }
        let predicate = predicates
            .into_iter()
            .reduce(|left, right| FilterPredicate::And(Box::new(left), Box::new(right)))
            .unwrap_or(FilterPredicate::Constant(true));
        Ok(Self {
            input: FilterOperator::new(input, predicate)?,
        })
    }
}

impl ExecutionOperator for TimestampRangeOperator {
    fn schema(&self) -> &VectorSchema {
        self.input.schema()
    }

    fn next_batch(
        &mut self,
        context: &mut ExecutionContext,
    ) -> Result<Option<VectorBatch>, ExecutionError> {
        self.input.next_batch(context)
    }
}

struct BucketInputOperator {
    input: Box<dyn ExecutionOperator>,
    input_schema: VectorSchema,
    schema: VectorSchema,
    timestamp_column: usize,
    interval_micros: i64,
    series_columns: Vec<usize>,
    aggregate_input_columns: Vec<usize>,
    finished: bool,
}

impl ExecutionOperator for BucketInputOperator {
    fn schema(&self) -> &VectorSchema {
        &self.schema
    }

    fn next_batch(
        &mut self,
        context: &mut ExecutionContext,
    ) -> Result<Option<VectorBatch>, ExecutionError> {
        if self.finished {
            return Ok(None);
        }
        loop {
            context.check_cancelled()?;
            let Some(input) = self.input.next_batch(context)? else {
                self.finished = true;
                return Ok(None);
            };
            self.input_schema.validate_batch(&input)?;

            let mut kept_rows = Vec::with_capacity(input.row_count);
            let mut bucket_starts = Vec::with_capacity(input.row_count);
            for row in 0..input.row_count {
                if row % 1024 == 0 {
                    context.check_cancelled()?;
                }
                let timestamp = match &input.columns[self.timestamp_column].values[row] {
                    Value::TimestampMicros(timestamp) => *timestamp,
                    Value::Null => continue,
                    _ => {
                        return Err(ExecutionError::InvalidBatch(
                            "timestamp column contains a value inconsistent with its schema".into(),
                        ))
                    }
                };
                let bucket_start = timestamp
                    .div_euclid(self.interval_micros)
                    .checked_mul(self.interval_micros)
                    .ok_or(ExecutionError::ArithmeticOverflow)?;
                kept_rows.push(row);
                bucket_starts.push(Value::TimestampMicros(bucket_start));
            }
            if kept_rows.is_empty() {
                continue;
            }

            let mut columns = Vec::with_capacity(self.schema.len());
            for source_index in &self.series_columns {
                let source = &input.columns[*source_index];
                columns.push(BatchColumn {
                    column: source.column.clone(),
                    values: kept_rows
                        .iter()
                        .map(|row| source.values[*row].clone())
                        .collect(),
                });
            }
            columns.push(BatchColumn {
                column: self.schema.columns()[self.series_columns.len()].clone(),
                values: bucket_starts,
            });
            for source_index in &self.aggregate_input_columns {
                let source = &input.columns[*source_index];
                columns.push(BatchColumn {
                    column: source.column.clone(),
                    values: kept_rows
                        .iter()
                        .map(|row| source.values[*row].clone())
                        .collect(),
                });
            }
            let output = RecordBatch {
                columns,
                row_count: kept_rows.len(),
            };
            context.check_transition(&self.input_schema, &input, &self.schema, &output)?;
            return Ok(Some(output));
        }
    }
}

/// UTC fixed-width time bucket aggregation. Buckets are left-closed/right-open
/// and anchored at the Unix epoch; negative timestamps use Euclidean floor.
pub struct TimeBucketOperator {
    input: GroupByOperator,
    schema: VectorSchema,
}

impl TimeBucketOperator {
    pub fn new(
        input: Box<dyn ExecutionOperator>,
        timestamp_column: usize,
        interval_micros: i64,
        series_columns: Vec<usize>,
        aggregates: Vec<AggregateSpec>,
    ) -> Result<Self, ExecutionError> {
        if interval_micros <= 0 {
            return Err(ExecutionError::InvalidPlan(
                "time bucket interval must be a positive number of microseconds".into(),
            ));
        }
        if aggregates.is_empty() {
            return Err(ExecutionError::InvalidPlan(
                "time bucket aggregation requires at least one aggregate expression".into(),
            ));
        }
        let input_schema = input.schema().clone();
        let timestamp = input_schema.columns().get(timestamp_column).ok_or_else(|| {
            ExecutionError::InvalidPlan(format!(
                "time bucket references missing timestamp column {timestamp_column}"
            ))
        })?;
        if timestamp.data_type != DataType::TimestampMicros {
            return Err(ExecutionError::TypeMismatch(format!(
                "time bucket requires TIMESTAMP_MICROS, received {}",
                timestamp.data_type
            )));
        }

        let mut seen_series = std::collections::BTreeSet::new();
        for index in &series_columns {
            if *index >= input_schema.len() {
                return Err(ExecutionError::InvalidPlan(format!(
                    "time bucket references missing series column {index}"
                )));
            }
            if *index == timestamp_column || !seen_series.insert(*index) {
                return Err(ExecutionError::InvalidPlan(
                    "series columns must be unique and cannot include the timestamp column".into(),
                ));
            }
        }

        let mut aggregate_input_columns = Vec::new();
        for aggregate in &aggregates {
            if let Some(source) = aggregate_input_column(&aggregate.function) {
                if source >= input_schema.len() {
                    return Err(ExecutionError::InvalidPlan(format!(
                        "aggregate references missing input column {source}"
                    )));
                }
                if !aggregate_input_columns.contains(&source) {
                    aggregate_input_columns.push(source);
                }
            }
        }

        let mut derived_columns = series_columns
            .iter()
            .map(|index| input_schema.columns()[*index].clone())
            .collect::<Vec<_>>();
        derived_columns.push(Column::new(
            "__ananta_bucket_start",
            DataType::TimestampMicros,
            false,
        ));
        derived_columns.extend(
            aggregate_input_columns
                .iter()
                .map(|index| input_schema.columns()[*index].clone()),
        );
        let derived_schema = VectorSchema::new(derived_columns)?;

        let mut aggregate_slot = BTreeMap::new();
        for (offset, source_index) in aggregate_input_columns.iter().enumerate() {
            aggregate_slot.insert(
                *source_index,
                series_columns.len() + 1 + offset,
            );
        }
        let remapped_aggregates = aggregates
            .into_iter()
            .map(|aggregate| {
                Ok(AggregateSpec {
                    function: remap_aggregate_function(
                        aggregate.function,
                        &aggregate_slot,
                    )?,
                    output: aggregate.output,
                })
            })
            .collect::<Result<Vec<_>, ExecutionError>>()?;

        let bucket_input = BucketInputOperator {
            input,
            input_schema,
            schema: derived_schema,
            timestamp_column,
            interval_micros,
            series_columns: series_columns.clone(),
            aggregate_input_columns,
            finished: false,
        };
        let group_columns = (0..=series_columns.len()).collect();
        let grouped = GroupByOperator::new(
            Box::new(bucket_input),
            group_columns,
            remapped_aggregates,
        )?;
        let schema = grouped.schema().clone();
        Ok(Self {
            input: grouped,
            schema,
        })
    }
}

impl ExecutionOperator for TimeBucketOperator {
    fn schema(&self) -> &VectorSchema {
        &self.schema
    }

    fn next_batch(
        &mut self,
        context: &mut ExecutionContext,
    ) -> Result<Option<VectorBatch>, ExecutionError> {
        self.input.next_batch(context)
    }
}

fn aggregate_input_column(function: &AggregateFunction) -> Option<usize> {
    match function {
        AggregateFunction::CountAll => None,
        AggregateFunction::Count { column }
        | AggregateFunction::Sum { column }
        | AggregateFunction::Min { column }
        | AggregateFunction::Max { column }
        | AggregateFunction::Average { column } => Some(*column),
    }
}

fn remap_aggregate_function(
    function: AggregateFunction,
    slots: &BTreeMap<usize, usize>,
) -> Result<AggregateFunction, ExecutionError> {
    let mapped = |column: usize| {
        slots.get(&column).copied().ok_or_else(|| {
            ExecutionError::InvalidPlan(format!(
                "time bucket aggregate input column {column} was not projected"
            ))
        })
    };
    Ok(match function {
        AggregateFunction::CountAll => AggregateFunction::CountAll,
        AggregateFunction::Count { column } => AggregateFunction::Count {
            column: mapped(column)?,
        },
        AggregateFunction::Sum { column } => AggregateFunction::Sum {
            column: mapped(column)?,
        },
        AggregateFunction::Min { column } => AggregateFunction::Min {
            column: mapped(column)?,
        },
        AggregateFunction::Max { column } => AggregateFunction::Max {
            column: mapped(column)?,
        },
        AggregateFunction::Average { column } => AggregateFunction::Average {
            column: mapped(column)?,
        },
    })
}

#[derive(Debug)]
struct LatestEntry {
    timestamp: i64,
    ordinal: u64,
    values: Vec<Value>,
    estimated_bytes: usize,
}

/// Returns the most recent full row per distinct series key. NULL timestamps
/// are ignored; for equal timestamps, the last encountered row wins.
pub struct LatestPerSeriesOperator {
    input: Box<dyn ExecutionOperator>,
    input_schema: VectorSchema,
    schema: VectorSchema,
    timestamp_column: usize,
    series_columns: Vec<usize>,
    latest: Option<IntoIter<Vec<u8>, LatestEntry>>,
    remaining_state_bytes: usize,
    batch_rows: usize,
    finished: bool,
}

impl LatestPerSeriesOperator {
    pub fn new(
        input: Box<dyn ExecutionOperator>,
        timestamp_column: usize,
        series_columns: Vec<usize>,
    ) -> Result<Self, ExecutionError> {
        let schema = input.schema().clone();
        let timestamp = schema.columns().get(timestamp_column).ok_or_else(|| {
            ExecutionError::InvalidPlan(format!(
                "latest-per-series references missing timestamp column {timestamp_column}"
            ))
        })?;
        if timestamp.data_type != DataType::TimestampMicros {
            return Err(ExecutionError::TypeMismatch(format!(
                "latest-per-series requires TIMESTAMP_MICROS, received {}",
                timestamp.data_type
            )));
        }
        let mut seen = std::collections::BTreeSet::new();
        for index in &series_columns {
            if *index >= schema.len() {
                return Err(ExecutionError::InvalidPlan(format!(
                    "latest-per-series references missing series column {index}"
                )));
            }
            if *index == timestamp_column || !seen.insert(*index) {
                return Err(ExecutionError::InvalidPlan(
                    "series columns must be unique and cannot include the timestamp column".into(),
                ));
            }
        }
        Ok(Self {
            input,
            input_schema: schema.clone(),
            schema,
            timestamp_column,
            series_columns,
            latest: None,
            remaining_state_bytes: 0,
            batch_rows: 1,
            finished: false,
        })
    }

    fn consume_input(
        &mut self,
        context: &mut ExecutionContext,
    ) -> Result<(), ExecutionError> {
        let mut latest: BTreeMap<Vec<u8>, LatestEntry> = BTreeMap::new();
        let mut state_bytes = 0usize;
        let mut ordinal = 0_u64;
        loop {
            context.check_cancelled()?;
            let Some(batch) = self.input.next_batch(context)? else {
                break;
            };
            self.input_schema.validate_batch(&batch)?;
            for row_index in 0..batch.row_count {
                if row_index % 1024 == 0 {
                    context.check_cancelled()?;
                }
                let current_ordinal = ordinal;
                ordinal = ordinal
                    .checked_add(1)
                    .ok_or(ExecutionError::ArithmeticOverflow)?;
                let timestamp = match &batch.columns[self.timestamp_column].values[row_index] {
                    Value::TimestampMicros(timestamp) => *timestamp,
                    Value::Null => continue,
                    _ => {
                        return Err(ExecutionError::InvalidBatch(
                            "timestamp column contains a value inconsistent with its schema".into(),
                        ))
                    }
                };
                let key_values = self
                    .series_columns
                    .iter()
                    .map(|index| batch.columns[*index].values[row_index].clone())
                    .collect::<Vec<_>>();
                let key = encode_group_key(&key_values)?;
                let should_replace = latest.get(&key).map_or(true, |current| {
                    timestamp > current.timestamp
                        || (timestamp == current.timestamp
                            && current_ordinal > current.ordinal)
                });
                if !should_replace {
                    continue;
                }

                let values = batch
                    .columns
                    .iter()
                    .map(|column| column.values[row_index].clone())
                    .collect::<Vec<_>>();
                let estimated_bytes = estimate_latest_entry_bytes(&key, &values)?;
                let previous_bytes = latest
                    .get(&key)
                    .map_or(0, |entry| entry.estimated_bytes);
                let next_state_bytes = state_bytes
                    .checked_sub(previous_bytes)
                    .and_then(|value| value.checked_add(estimated_bytes))
                    .ok_or(ExecutionError::ArithmeticOverflow)?;
                if next_state_bytes > context.config().max_working_set_bytes {
                    return Err(ExecutionError::ResourceLimit {
                        resource: "latest-per-series state bytes",
                        actual: next_state_bytes,
                        limit: context.config().max_working_set_bytes,
                    });
                }
                latest.insert(
                    key,
                    LatestEntry {
                        timestamp,
                        ordinal: current_ordinal,
                        values,
                        estimated_bytes,
                    },
                );
                state_bytes = next_state_bytes;
            }
        }

        if self.schema.len() > context.config().max_batch_cells {
            return Err(ExecutionError::ResourceLimit {
                resource: "latest-per-series output columns",
                actual: self.schema.len(),
                limit: context.config().max_batch_cells,
            });
        }
        self.batch_rows = context
            .config()
            .max_batch_rows
            .min(context.config().max_batch_cells / self.schema.len())
            .max(1);
        self.remaining_state_bytes = state_bytes;
        self.latest = Some(latest.into_iter());
        Ok(())
    }
}

impl ExecutionOperator for LatestPerSeriesOperator {
    fn schema(&self) -> &VectorSchema {
        &self.schema
    }

    fn next_batch(
        &mut self,
        context: &mut ExecutionContext,
    ) -> Result<Option<VectorBatch>, ExecutionError> {
        if self.finished {
            return Ok(None);
        }
        context.check_cancelled()?;
        if self.latest.is_none() {
            if let Err(error) = self.consume_input(context) {
                self.finished = true;
                return Err(error);
            }
        }
        let latest = self.latest.as_mut().ok_or_else(|| {
            ExecutionError::InvalidPlan("latest-per-series input was not initialised".into())
        })?;
        let mut columns = self
            .schema
            .columns()
            .iter()
            .cloned()
            .map(|column| BatchColumn {
                column,
                values: Vec::with_capacity(self.batch_rows),
            })
            .collect::<Vec<_>>();

        let mut rows = 0usize;
        while rows < self.batch_rows {
            context.check_cancelled()?;
            let Some((_key, entry)) = latest.next() else {
                break;
            };
            self.remaining_state_bytes = self
                .remaining_state_bytes
                .checked_sub(entry.estimated_bytes)
                .ok_or(ExecutionError::ArithmeticOverflow)?;
            for (column, value) in columns.iter_mut().zip(entry.values) {
                column.values.push(value);
            }
            rows += 1;
        }

        if rows == 0 {
            self.finished = true;
            return Ok(None);
        }
        let output = RecordBatch {
            columns,
            row_count: rows,
        };
        let output_bytes = estimate_batch_bytes(&output)?;
        let total_working_set = self
            .remaining_state_bytes
            .checked_add(output_bytes)
            .ok_or(ExecutionError::ArithmeticOverflow)?;
        if total_working_set > context.config().max_working_set_bytes {
            self.finished = true;
            return Err(ExecutionError::ResourceLimit {
                resource: "latest-per-series output working set",
                actual: total_working_set,
                limit: context.config().max_working_set_bytes,
            });
        }
        context.check_batch(&self.schema, &output)?;
        Ok(Some(output))
    }
}

fn estimate_latest_entry_bytes(
    encoded_key: &[u8],
    values: &[Value],
) -> Result<usize, ExecutionError> {
    let mut bytes = size_of::<LatestEntry>()
        .checked_add(encoded_key.len())
        .and_then(|value| value.checked_add(values.len().checked_mul(size_of::<Value>())?))
        .ok_or(ExecutionError::ArithmeticOverflow)?;
    for value in values {
        let dynamic_bytes = match value {
            Value::Utf8(value) => value.capacity(),
            Value::Binary(value) => value.capacity(),
            _ => 0,
        };
        bytes = bytes
            .checked_add(dynamic_bytes)
            .ok_or(ExecutionError::ArithmeticOverflow)?;
    }
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ExecutionConfig, ExecutionContext, ExecutionOperator};
    use std::collections::VecDeque;

    struct MockOperator {
        schema: VectorSchema,
        batches: VecDeque<VectorBatch>,
    }

    impl MockOperator {
        fn new(schema: VectorSchema, batches: Vec<VectorBatch>) -> Self {
            Self {
                schema,
                batches: batches.into(),
            }
        }
    }

    impl ExecutionOperator for MockOperator {
        fn schema(&self) -> &VectorSchema {
            &self.schema
        }

        fn next_batch(
            &mut self,
            context: &mut ExecutionContext,
        ) -> Result<Option<VectorBatch>, ExecutionError> {
            context.check_cancelled()?;
            let Some(batch) = self.batches.pop_front() else {
                return Ok(None);
            };
            context.check_batch(&self.schema, &batch)?;
            Ok(Some(batch))
        }
    }

    fn schema() -> VectorSchema {
        VectorSchema::new(vec![
            Column::new("device", DataType::Utf8, false),
            Column::new("timestamp", DataType::TimestampMicros, true),
            Column::new("reading", DataType::Float64, true),
        ])
        .unwrap()
    }

    fn batch(schema: &VectorSchema, rows: Vec<Vec<Value>>) -> VectorBatch {
        let mut columns = schema.columns().iter().cloned().map(|column| BatchColumn {
            column,
            values: Vec::new(),
        }).collect::<Vec<_>>();
        for row in &rows {
            for (column, value) in columns.iter_mut().zip(row) {
                column.values.push(value.clone());
            }
        }
        RecordBatch { columns, row_count: rows.len() }
    }

    #[test]
    fn timestamp_range_obeys_inclusive_and_exclusive_utc_microsecond_bounds() {
        let schema = schema();
        let input = MockOperator::new(
            schema.clone(),
            vec![batch(&schema, vec![
                vec![Value::Utf8("a".into()), Value::TimestampMicros(9), Value::Float64(1.0)],
                vec![Value::Utf8("b".into()), Value::TimestampMicros(10), Value::Float64(2.0)],
                vec![Value::Utf8("c".into()), Value::TimestampMicros(15), Value::Float64(3.0)],
                vec![Value::Utf8("d".into()), Value::TimestampMicros(20), Value::Float64(4.0)],
                vec![Value::Utf8("e".into()), Value::Null, Value::Float64(5.0)],
            ])],
        );
        let mut operator = TimestampRangeOperator::new(
            Box::new(input),
            1,
            Some(TimestampBound::inclusive(10)),
            Some(TimestampBound::exclusive(20)),
        ).unwrap();
        let mut context = ExecutionContext::default();
        let result = operator.next_batch(&mut context).unwrap().unwrap();
        assert_eq!(result.row_count, 2);
        assert_eq!(
            result.columns[0].values,
            vec![Value::Utf8("b".into()), Value::Utf8("c".into())]
        );
        assert!(operator.next_batch(&mut context).unwrap().is_none());
    }

    #[test]
    fn time_buckets_use_epoch_anchored_euclidean_floor_and_accept_late_rows() {
        let schema = schema();
        let input = MockOperator::new(
            schema.clone(),
            vec![
                batch(&schema, vec![
                    vec![Value::Utf8("sensor".into()), Value::TimestampMicros(1_500_000), Value::Float64(3.0)],
                    vec![Value::Utf8("sensor".into()), Value::TimestampMicros(500_000), Value::Float64(1.0)],
                    vec![Value::Utf8("sensor".into()), Value::TimestampMicros(-1), Value::Float64(2.0)],
                ]),
                batch(&schema, vec![
                    vec![Value::Utf8("sensor".into()), Value::TimestampMicros(1_999_999), Value::Float64(5.0)],
                    vec![Value::Utf8("sensor".into()), Value::Null, Value::Float64(9.0)],
                ]),
            ],
        );
        let aggregate = AggregateSpec::new(
            AggregateFunction::Average { column: 2 },
            Column::new("average", DataType::Float64, true),
        );
        let mut operator = TimeBucketOperator::new(
            Box::new(input),
            1,
            1_000_000,
            vec![0],
            vec![aggregate],
        ).unwrap();
        let mut context = ExecutionContext::default();
        let mut output = Vec::new();
        while let Some(batch) = operator.next_batch(&mut context).unwrap() {
            for row in 0..batch.row_count {
                output.push((
                    batch.columns[1].values[row].clone(),
                    batch.columns[2].values[row].clone(),
                ));
            }
        }
        output.sort_by_key(|row| match &row.0 {
            Value::TimestampMicros(timestamp) => *timestamp,
            _ => i64::MAX,
        });
        assert_eq!(output, vec![
            (Value::TimestampMicros(-1_000_000), Value::Float64(2.0)),
            (Value::TimestampMicros(0), Value::Float64(1.0)),
            (Value::TimestampMicros(1_000_000), Value::Float64(4.0)),
        ]);
    }

    #[test]
    fn latest_per_series_uses_max_timestamp_and_last_seen_tie_breaker() {
        let schema = schema();
        let input = MockOperator::new(
            schema.clone(),
            vec![
                batch(&schema, vec![
                    vec![Value::Utf8("a".into()), Value::TimestampMicros(100), Value::Float64(1.0)],
                    vec![Value::Utf8("b".into()), Value::TimestampMicros(80), Value::Float64(9.0)],
                ]),
                batch(&schema, vec![
                    vec![Value::Utf8("a".into()), Value::TimestampMicros(90), Value::Float64(2.0)],
                    vec![Value::Utf8("a".into()), Value::TimestampMicros(100), Value::Float64(3.0)],
                    vec![Value::Utf8("null-time".into()), Value::Null, Value::Float64(5.0)],
                ]),
            ],
        );
        let mut operator = LatestPerSeriesOperator::new(Box::new(input), 1, vec![0]).unwrap();
        let mut context = ExecutionContext::default();
        let result = operator.next_batch(&mut context).unwrap().unwrap();
        assert_eq!(result.row_count, 2);
        assert_eq!(
            result.columns[0].values,
            vec![Value::Utf8("a".into()), Value::Utf8("b".into())]
        );
        assert_eq!(
            result.columns[2].values,
            vec![Value::Float64(3.0), Value::Float64(9.0)]
        );
        assert!(operator.next_batch(&mut context).unwrap().is_none());
    }

    #[test]
    fn latest_per_series_enforces_distinct_state_memory_budget() {
        struct UnmeteredSource {
            schema: VectorSchema,
            batches: VecDeque<VectorBatch>,
        }
        impl ExecutionOperator for UnmeteredSource {
            fn schema(&self) -> &VectorSchema {
                &self.schema
            }
            fn next_batch(
                &mut self,
                context: &mut ExecutionContext,
            ) -> Result<Option<VectorBatch>, ExecutionError> {
                context.check_cancelled()?;
                Ok(self.batches.pop_front())
            }
        }
        let schema = schema();
        let source = UnmeteredSource {
            schema: schema.clone(),
            batches: VecDeque::from([batch(&schema, vec![
                vec![Value::Utf8("device".repeat(16)), Value::TimestampMicros(1), Value::Float64(1.0)],
            ])]),
        };
        let mut operator = LatestPerSeriesOperator::new(Box::new(source), 1, vec![0]).unwrap();
        let mut context = ExecutionContext::new(ExecutionConfig {
            max_batch_rows: 10,
            max_batch_cells: 100,
            max_working_set_bytes: 128,
        }).unwrap();
        assert!(matches!(
            operator.next_batch(&mut context),
            Err(ExecutionError::ResourceLimit { resource: "latest-per-series state bytes", .. })
        ));
    }

    #[test]
    fn time_bucket_rejects_non_positive_intervals_and_non_timestamp_columns() {
        let schema = schema();
        let input = || {
            Box::new(MockOperator::new(schema.clone(), Vec::new()))
                as Box<dyn ExecutionOperator>
        };
        let aggregate = || vec![AggregateSpec::new(
            AggregateFunction::CountAll,
            Column::new("rows", DataType::Int64, false),
        )];
        assert!(matches!(
            TimeBucketOperator::new(input(), 1, 0, vec![0], aggregate()),
            Err(ExecutionError::InvalidPlan(_))
        ));
        assert!(matches!(
            TimeBucketOperator::new(input(), 0, 1_000_000, vec![], aggregate()),
            Err(ExecutionError::TypeMismatch(_))
        ));
    }
}
