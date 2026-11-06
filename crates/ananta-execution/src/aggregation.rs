use crate::{
    BatchColumn, ExecutionContext, ExecutionError, ExecutionOperator, RecordBatch, VectorBatch,
    VectorSchema,
};
use ananta_types::{Column, DataType, Value};
use std::cmp::Ordering;
use std::collections::{BTreeMap, btree_map::IntoIter};
use std::mem::size_of;

/// Aggregate operation over one input batch column, or over each row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AggregateFunction {
    CountAll,
    Count { column: usize },
    Sum { column: usize },
    Min { column: usize },
    Max { column: usize },
    Average { column: usize },
}

/// Output descriptor and aggregate definition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AggregateSpec {
    pub function: AggregateFunction,
    pub output: Column,
}

impl AggregateSpec {
    pub fn new(function: AggregateFunction, output: Column) -> Self {
        Self { function, output }
    }
}

#[derive(Debug)]
enum Accumulator {
    Count(u64),
    IntegerSum { sum: i128, count: u64 },
    FloatSum { sum: f64, count: u64 },
    IntegerAverage { sum: i128, count: u64 },
    FloatAverage { sum: f64, count: u64 },
    Extreme { value: Option<Value>, minimum: bool },
}

impl Accumulator {
    fn new(function: &AggregateFunction, schema: &VectorSchema) -> Self {
        match function {
            AggregateFunction::CountAll | AggregateFunction::Count { .. } => Self::Count(0),
            AggregateFunction::Sum { column } => {
                if schema.columns()[*column].data_type == DataType::Float64 {
                    Self::FloatSum { sum: 0.0, count: 0 }
                } else {
                    Self::IntegerSum { sum: 0, count: 0 }
                }
            }
            AggregateFunction::Average { column } => {
                if schema.columns()[*column].data_type == DataType::Float64 {
                    Self::FloatAverage { sum: 0.0, count: 0 }
                } else {
                    Self::IntegerAverage { sum: 0, count: 0 }
                }
            }
            AggregateFunction::Min { .. } => Self::Extreme {
                value: None,
                minimum: true,
            },
            AggregateFunction::Max { .. } => Self::Extreme {
                value: None,
                minimum: false,
            },
        }
    }

    fn update(
        &mut self,
        function: &AggregateFunction,
        batch: &VectorBatch,
        row: usize,
    ) -> Result<(), ExecutionError> {
        match function {
            AggregateFunction::CountAll => {
                let Self::Count(count) = self else {
                    return Err(ExecutionError::InvalidPlan(
                        "COUNT(*) accumulator is incompatible with its specification".into(),
                    ));
                };
                *count = count
                    .checked_add(1)
                    .ok_or(ExecutionError::ArithmeticOverflow)?;
            }
            AggregateFunction::Count { column } => {
                let value = input_value(batch, *column, row)?;
                if !matches!(value, Value::Null) {
                    let Self::Count(count) = self else {
                        return Err(ExecutionError::InvalidPlan(
                            "COUNT accumulator is incompatible with its specification".into(),
                        ));
                    };
                    *count = count
                        .checked_add(1)
                        .ok_or(ExecutionError::ArithmeticOverflow)?;
                }
            }
            AggregateFunction::Sum { column } => {
                let value = input_value(batch, *column, row)?;
                if matches!(value, Value::Null) {
                    return Ok(());
                }
                match (self, value) {
                    (Self::IntegerSum { sum, count }, Value::Int32(value)) => {
                        *sum = sum
                            .checked_add(i128::from(*value))
                            .ok_or(ExecutionError::ArithmeticOverflow)?;
                        *count = count
                            .checked_add(1)
                            .ok_or(ExecutionError::ArithmeticOverflow)?;
                    }
                    (Self::IntegerSum { sum, count }, Value::Int64(value)) => {
                        *sum = sum
                            .checked_add(i128::from(*value))
                            .ok_or(ExecutionError::ArithmeticOverflow)?;
                        *count = count
                            .checked_add(1)
                            .ok_or(ExecutionError::ArithmeticOverflow)?;
                    }
                    (Self::FloatSum { sum, count }, Value::Float64(value)) => {
                        *sum += *value;
                        *count = count
                            .checked_add(1)
                            .ok_or(ExecutionError::ArithmeticOverflow)?;
                    }
                    _ => {
                        return Err(ExecutionError::InvalidPlan(
                            "SUM accumulator does not match the input type".into(),
                        ))
                    }
                }
            }
            AggregateFunction::Average { column } => {
                let value = input_value(batch, *column, row)?;
                if matches!(value, Value::Null) {
                    return Ok(());
                }
                match (self, value) {
                    (Self::IntegerAverage { sum, count }, Value::Int32(value)) => {
                        *sum = sum
                            .checked_add(i128::from(*value))
                            .ok_or(ExecutionError::ArithmeticOverflow)?;
                        *count = count
                            .checked_add(1)
                            .ok_or(ExecutionError::ArithmeticOverflow)?;
                    }
                    (Self::IntegerAverage { sum, count }, Value::Int64(value)) => {
                        *sum = sum
                            .checked_add(i128::from(*value))
                            .ok_or(ExecutionError::ArithmeticOverflow)?;
                        *count = count
                            .checked_add(1)
                            .ok_or(ExecutionError::ArithmeticOverflow)?;
                    }
                    (Self::FloatAverage { sum, count }, Value::Float64(value)) => {
                        *sum += *value;
                        *count = count
                            .checked_add(1)
                            .ok_or(ExecutionError::ArithmeticOverflow)?;
                    }
                    _ => {
                        return Err(ExecutionError::InvalidPlan(
                            "AVG accumulator does not match the input type".into(),
                        ))
                    }
                }
            }
            AggregateFunction::Min { column } | AggregateFunction::Max { column } => {
                let value = input_value(batch, *column, row)?;
                if matches!(value, Value::Null) {
                    return Ok(());
                }
                let Self::Extreme {
                    value: current,
                    minimum,
                } = self
                else {
                    return Err(ExecutionError::InvalidPlan(
                        "MIN/MAX accumulator is incompatible with its specification".into(),
                    ));
                };
                let should_replace = match current {
                    None => true,
                    Some(existing) => {
                        let ordering = compare_aggregate_values(value, existing)?;
                        if *minimum {
                            ordering == Ordering::Less
                        } else {
                            ordering == Ordering::Greater
                        }
                    }
                };
                if should_replace {
                    *current = Some(value.clone());
                }
            }
        }
        Ok(())
    }

    fn finish(self, function: &AggregateFunction) -> Result<Value, ExecutionError> {
        match (function, self) {
            (AggregateFunction::CountAll | AggregateFunction::Count { .. }, Self::Count(count)) => {
                i64::try_from(count)
                    .map(Value::Int64)
                    .map_err(|_| ExecutionError::AggregateOverflow("COUNT".into()))
            }
            (
                AggregateFunction::Sum { .. },
                Self::IntegerSum { sum, count },
            ) => {
                if count == 0 {
                    Ok(Value::Null)
                } else {
                    i64::try_from(sum)
                        .map(Value::Int64)
                        .map_err(|_| ExecutionError::AggregateOverflow("SUM".into()))
                }
            }
            (AggregateFunction::Sum { .. }, Self::FloatSum { sum, count }) => {
                Ok(if count == 0 {
                    Value::Null
                } else {
                    Value::Float64(sum)
                })
            }
            (
                AggregateFunction::Average { .. },
                Self::IntegerAverage { sum, count },
            ) => Ok(if count == 0 {
                Value::Null
            } else {
                Value::Float64(sum as f64 / count as f64)
            }),
            (AggregateFunction::Average { .. }, Self::FloatAverage { sum, count }) => {
                Ok(if count == 0 {
                    Value::Null
                } else {
                    Value::Float64(sum / count as f64)
                })
            }
            (AggregateFunction::Min { .. } | AggregateFunction::Max { .. }, Self::Extreme { value, .. }) => {
                Ok(value.unwrap_or(Value::Null))
            }
            _ => Err(ExecutionError::InvalidPlan(
                "aggregate accumulator does not match its specification".into(),
            )),
        }
    }
}

#[derive(Debug)]
struct GroupState {
    keys: Vec<Value>,
    accumulators: Vec<Accumulator>,
}

/// Blocking GROUP BY operator. It consumes its input once, holds only distinct
/// group state subject to the execution working-set limit, and emits bounded
/// result batches in deterministic key order.
pub struct GroupByOperator {
    input: Box<dyn ExecutionOperator>,
    input_schema: VectorSchema,
    schema: VectorSchema,
    group_columns: Vec<usize>,
    aggregates: Vec<AggregateSpec>,
    groups: Option<IntoIter<Vec<u8>, GroupState>>,
    batch_rows: usize,
    finished: bool,
}

impl GroupByOperator {
    pub fn new(
        input: Box<dyn ExecutionOperator>,
        group_columns: Vec<usize>,
        aggregates: Vec<AggregateSpec>,
    ) -> Result<Self, ExecutionError> {
        if aggregates.is_empty() {
            return Err(ExecutionError::InvalidPlan(
                "GROUP BY requires at least one aggregate expression".into(),
            ));
        }
        let input_schema = input.schema().clone();
        for index in &group_columns {
            if *index >= input_schema.len() {
                return Err(ExecutionError::InvalidPlan(format!(
                    "GROUP BY references missing input column {index}"
                )));
            }
        }

        for aggregate in &aggregates {
            validate_aggregate(&aggregate.function, &aggregate.output, &input_schema)?;
        }

        let mut output_columns = group_columns
            .iter()
            .map(|index| input_schema.columns()[*index].clone())
            .collect::<Vec<_>>();
        output_columns.extend(aggregates.iter().map(|aggregate| aggregate.output.clone()));
        let schema = VectorSchema::new(output_columns)?;
        Ok(Self {
            input,
            input_schema,
            schema,
            group_columns,
            aggregates,
            groups: None,
            batch_rows: 1,
            finished: false,
        })
    }

    fn consume_input(
        &mut self,
        context: &mut ExecutionContext,
    ) -> Result<(), ExecutionError> {
        let mut groups: BTreeMap<Vec<u8>, GroupState> = BTreeMap::new();
        let mut estimated_state_bytes = 0usize;
        loop {
            context.check_cancelled()?;
            let Some(batch) = self.input.next_batch(context)? else {
                break;
            };
            self.input_schema.validate_batch(&batch)?;
            for row in 0..batch.row_count {
                if row % 1024 == 0 {
                    context.check_cancelled()?;
                }
                let keys = self
                    .group_columns
                    .iter()
                    .map(|index| input_value(&batch, *index, row).cloned())
                    .collect::<Result<Vec<_>, _>>()?;
                let encoded_key = encode_group_key(&keys)?;
                if !groups.contains_key(&encoded_key) {
                    let state_size = estimate_group_state_bytes(&encoded_key, &keys, self.aggregates.len())?;
                    estimated_state_bytes = estimated_state_bytes
                        .checked_add(state_size)
                        .ok_or(ExecutionError::ArithmeticOverflow)?;
                    if estimated_state_bytes > context.config().max_working_set_bytes {
                        return Err(ExecutionError::ResourceLimit {
                            resource: "group state bytes",
                            actual: estimated_state_bytes,
                            limit: context.config().max_working_set_bytes,
                        });
                    }
                    groups.insert(
                        encoded_key.clone(),
                        GroupState {
                            keys,
                            accumulators: self
                                .aggregates
                                .iter()
                                .map(|aggregate| Accumulator::new(&aggregate.function, &self.input_schema))
                                .collect(),
                        },
                    );
                }

                let state = groups
                    .get_mut(&encoded_key)
                    .ok_or_else(|| ExecutionError::InvalidPlan(
                        "group state disappeared during aggregation".into(),
                    ))?;
                for (aggregate, accumulator) in
                    self.aggregates.iter().zip(&mut state.accumulators)
                {
                    accumulator.update(&aggregate.function, &batch, row)?;
                }
            }
        }

        // A scalar aggregate without GROUP BY returns one row even for empty input.
        if self.group_columns.is_empty() && groups.is_empty() {
            groups.insert(
                Vec::new(),
                GroupState {
                    keys: Vec::new(),
                    accumulators: self
                        .aggregates
                        .iter()
                        .map(|aggregate| Accumulator::new(&aggregate.function, &self.input_schema))
                        .collect(),
                },
            );
        }

        let output_columns = self.schema.len();
        if output_columns > context.config().max_batch_cells {
            return Err(ExecutionError::ResourceLimit {
                resource: "aggregate output columns",
                actual: output_columns,
                limit: context.config().max_batch_cells,
            });
        }
        self.batch_rows = context
            .config()
            .max_batch_rows
            .min(context.config().max_batch_cells / output_columns.max(1))
            .max(1);
        self.groups = Some(groups.into_iter());
        Ok(())
    }
}

impl ExecutionOperator for GroupByOperator {
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
        if self.groups.is_none() {
            if let Err(error) = self.consume_input(context) {
                self.finished = true;
                return Err(error);
            }
        }
        let groups = self.groups.as_mut().ok_or_else(|| {
            ExecutionError::InvalidPlan("GROUP BY input was not initialised".into())
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
            let Some((_key, mut group)) = groups.next() else {
                break;
            };
            for (slot, value) in columns.iter_mut().zip(group.keys.drain(..)) {
                slot.values.push(value);
            }
            for (slot, (aggregate, accumulator)) in columns
                .iter_mut()
                .skip(self.group_columns.len())
                .zip(self.aggregates.iter().zip(group.accumulators))
            {
                slot.values
                    .push(accumulator.finish(&aggregate.function)?);
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
        context.check_batch(&self.schema, &output)?;
        Ok(Some(output))
    }
}

fn validate_aggregate(
    function: &AggregateFunction,
    output: &Column,
    input: &VectorSchema,
) -> Result<(), ExecutionError> {
    let expected = match function {
        AggregateFunction::CountAll => None,
        AggregateFunction::Count { column }
        | AggregateFunction::Sum { column }
        | AggregateFunction::Min { column }
        | AggregateFunction::Max { column }
        | AggregateFunction::Average { column } => Some(
            input.columns().get(*column).ok_or_else(|| {
                ExecutionError::InvalidPlan(format!(
                    "aggregate references missing input column {column}"
                ))
            })?,
        ),
    };

    let (expected_type, expected_nullable) = match function {
        AggregateFunction::CountAll | AggregateFunction::Count { .. } => {
            (DataType::Int64, false)
        }
        AggregateFunction::Sum { .. } => match expected.map(|column| column.data_type) {
            Some(DataType::Int32 | DataType::Int64) => (DataType::Int64, true),
            Some(DataType::Float64) => (DataType::Float64, true),
            _ => {
                return Err(ExecutionError::TypeMismatch(
                    "SUM supports INT32, INT64 and FLOAT64 inputs".into(),
                ))
            }
        },
        AggregateFunction::Average { .. } => match expected.map(|column| column.data_type) {
            Some(DataType::Int32 | DataType::Int64 | DataType::Float64) => {
                (DataType::Float64, true)
            }
            _ => {
                return Err(ExecutionError::TypeMismatch(
                    "AVG supports INT32, INT64 and FLOAT64 inputs".into(),
                ))
            }
        },
        AggregateFunction::Min { .. } | AggregateFunction::Max { .. } => {
            let Some(column) = expected else {
                return Err(ExecutionError::InvalidPlan(
                    "MIN/MAX requires an input column".into(),
                ));
            };
            (column.data_type, true)
        }
    };
    if output.data_type != expected_type || output.nullable != expected_nullable {
        return Err(ExecutionError::TypeMismatch(format!(
            "aggregate output {} must be {}{}",
            output.name,
            expected_type,
            if expected_nullable { " NULLABLE" } else { " NOT NULL" }
        )));
    }
    Ok(())
}

fn input_value(
    batch: &VectorBatch,
    column: usize,
    row: usize,
) -> Result<&Value, ExecutionError> {
    batch
        .columns
        .get(column)
        .and_then(|field| field.values.get(row))
        .ok_or_else(|| {
            ExecutionError::InvalidBatch(format!(
                "aggregate input is missing column {column}, row {row}"
            ))
        })
}

fn compare_aggregate_values(left: &Value, right: &Value) -> Result<Ordering, ExecutionError> {
    let ordering = match (left, right) {
        (Value::Boolean(left), Value::Boolean(right)) => left.cmp(right),
        (Value::Int32(left), Value::Int32(right)) => left.cmp(right),
        (Value::Int64(left), Value::Int64(right)) => left.cmp(right),
        (Value::TimestampMicros(left), Value::TimestampMicros(right)) => left.cmp(right),
        (Value::Float64(left), Value::Float64(right)) => left.total_cmp(right),
        (Value::Utf8(left), Value::Utf8(right)) => left.cmp(right),
        (Value::Binary(left), Value::Binary(right)) => left.cmp(right),
        _ => {
            return Err(ExecutionError::TypeMismatch(
                "MIN/MAX values have incompatible data types".into(),
            ))
        }
    };
    Ok(ordering)
}

fn encode_group_key(values: &[Value]) -> Result<Vec<u8>, ExecutionError> {
    let mut bytes = Vec::new();
    for value in values {
        match value {
            Value::Null => bytes.push(0),
            Value::Boolean(value) => {
                bytes.push(1);
                bytes.push(u8::from(*value));
            }
            Value::Int32(value) => {
                bytes.push(2);
                bytes.extend_from_slice(&value.to_le_bytes());
            }
            Value::Int64(value) => {
                bytes.push(3);
                bytes.extend_from_slice(&value.to_le_bytes());
            }
            Value::Float64(value) => {
                bytes.push(4);
                let bits = if *value == 0.0 {
                    0
                } else if value.is_nan() {
                    f64::NAN.to_bits()
                } else {
                    value.to_bits()
                };
                bytes.extend_from_slice(&bits.to_le_bytes());
            }
            Value::Utf8(value) => {
                bytes.push(5);
                encode_len_prefixed(&mut bytes, value.as_bytes())?;
            }
            Value::TimestampMicros(value) => {
                bytes.push(6);
                bytes.extend_from_slice(&value.to_le_bytes());
            }
            Value::Binary(value) => {
                bytes.push(7);
                encode_len_prefixed(&mut bytes, value)?;
            }
        }
    }
    Ok(bytes)
}

fn encode_len_prefixed(output: &mut Vec<u8>, value: &[u8]) -> Result<(), ExecutionError> {
    let len = u64::try_from(value.len()).map_err(|_| ExecutionError::ArithmeticOverflow)?;
    output.extend_from_slice(&len.to_le_bytes());
    output.extend_from_slice(value);
    Ok(())
}

fn estimate_group_state_bytes(
    encoded_key: &[u8],
    keys: &[Value],
    aggregate_count: usize,
) -> Result<usize, ExecutionError> {
    let mut bytes = size_of::<GroupState>();
    bytes = bytes
        .checked_add(encoded_key.len())
        .and_then(|value| {
            value.checked_add(
                keys.len()
                    .checked_mul(size_of::<Value>())?,
            )
        })
        .and_then(|value| {
            value.checked_add(
                aggregate_count.checked_mul(size_of::<Accumulator>())?,
            )
        })
        .ok_or(ExecutionError::ArithmeticOverflow)?;
    for key in keys {
        let owned_payload = match key {
            Value::Utf8(value) => value.capacity(),
            Value::Binary(value) => value.capacity(),
            _ => 0,
        };
        bytes = bytes
            .checked_add(owned_payload)
            .ok_or(ExecutionError::ArithmeticOverflow)?;
    }
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{BatchColumn, ExecutionConfig, ExecutionContext, ExecutionOperator, RecordBatch, VectorSchema};
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
            Column::new("group", DataType::Utf8, true),
            Column::new("amount", DataType::Int64, true),
            Column::new("measure", DataType::Float64, true),
        ]).unwrap()
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

    fn aggregate_specs() -> Vec<AggregateSpec> {
        vec![
            AggregateSpec::new(
                AggregateFunction::CountAll,
                Column::new("rows", DataType::Int64, false),
            ),
            AggregateSpec::new(
                AggregateFunction::Count { column: 1 },
                Column::new("amount_count", DataType::Int64, false),
            ),
            AggregateSpec::new(
                AggregateFunction::Sum { column: 1 },
                Column::new("amount_sum", DataType::Int64, true),
            ),
            AggregateSpec::new(
                AggregateFunction::Min { column: 1 },
                Column::new("amount_min", DataType::Int64, true),
            ),
            AggregateSpec::new(
                AggregateFunction::Max { column: 1 },
                Column::new("amount_max", DataType::Int64, true),
            ),
            AggregateSpec::new(
                AggregateFunction::Average { column: 1 },
                Column::new("amount_avg", DataType::Float64, true),
            ),
            AggregateSpec::new(
                AggregateFunction::Average { column: 2 },
                Column::new("measure_avg", DataType::Float64, true),
            ),
            AggregateSpec::new(
                AggregateFunction::Sum { column: 2 },
                Column::new("measure_sum", DataType::Float64, true),
            ),
        ]
    }

    #[test]
    fn group_by_computes_all_aggregates_and_ignores_null_inputs() {
        let input_schema = schema();
        let input = MockOperator::new(
            input_schema.clone(),
            vec![
                batch(&input_schema, vec![
                    vec![Value::Utf8("north".into()), Value::Int64(10), Value::Float64(2.0)],
                    vec![Value::Utf8("north".into()), Value::Null, Value::Float64(4.0)],
                ]),
                batch(&input_schema, vec![
                    vec![Value::Utf8("south".into()), Value::Int64(7), Value::Null],
                    vec![Value::Utf8("north".into()), Value::Int64(20), Value::Float64(6.0)],
                ]),
            ],
        );
        let mut operator = GroupByOperator::new(Box::new(input), vec![0], aggregate_specs()).unwrap();
        let mut context = ExecutionContext::default();
        let first = operator.next_batch(&mut context).unwrap().unwrap();
        assert_eq!(first.row_count, 2);
        // BTree ordering makes output groups deterministic.
        assert_eq!(first.columns[0].values, vec![
            Value::Utf8("north".into()),
            Value::Utf8("south".into()),
        ]);
        assert_eq!(first.columns[1].values, vec![Value::Int64(3), Value::Int64(1)]);
        assert_eq!(first.columns[2].values, vec![Value::Int64(2), Value::Int64(1)]);
        assert_eq!(first.columns[3].values, vec![Value::Int64(30), Value::Int64(7)]);
        assert_eq!(first.columns[4].values, vec![Value::Int64(10), Value::Int64(7)]);
        assert_eq!(first.columns[5].values, vec![Value::Int64(20), Value::Int64(7)]);
        assert_eq!(first.columns[6].values, vec![Value::Float64(15.0), Value::Float64(7.0)]);
        assert_eq!(first.columns[7].values, vec![Value::Float64(4.0), Value::Null]);
        assert_eq!(first.columns[8].values, vec![Value::Float64(12.0), Value::Null]);
        assert!(operator.next_batch(&mut context).unwrap().is_none());
    }

    #[test]
    fn scalar_aggregates_return_one_row_on_empty_input() {
        let input_schema = schema();
        let input = MockOperator::new(input_schema, Vec::new());
        let mut operator = GroupByOperator::new(Box::new(input), vec![], aggregate_specs()).unwrap();
        let mut context = ExecutionContext::default();
        let result = operator.next_batch(&mut context).unwrap().unwrap();
        assert_eq!(result.row_count, 1);
        assert_eq!(result.columns[0].values, vec![Value::Int64(0)]);
        assert_eq!(result.columns[1].values, vec![Value::Int64(0)]);
        assert_eq!(result.columns[2].values, vec![Value::Null]);
        assert_eq!(result.columns[3].values, vec![Value::Null]);
        assert_eq!(result.columns[4].values, vec![Value::Null]);
        assert_eq!(result.columns[5].values, vec![Value::Null]);
        assert_eq!(result.columns[6].values, vec![Value::Null]);
        assert_eq!(result.columns[7].values, vec![Value::Null]);
    }

    #[test]
    fn group_by_empty_input_returns_no_groups() {
        let input_schema = schema();
        let input = MockOperator::new(input_schema, Vec::new());
        let mut operator = GroupByOperator::new(
            Box::new(input),
            vec![0],
            vec![AggregateSpec::new(
                AggregateFunction::CountAll,
                Column::new("rows", DataType::Int64, false),
            )],
        ).unwrap();
        let mut context = ExecutionContext::default();
        assert!(operator.next_batch(&mut context).unwrap().is_none());
    }

    #[test]
    fn high_cardinality_groups_are_emitted_in_bounded_deterministic_batches() {
        let input_schema = schema();
        let input_rows = (0..5)
            .map(|index| vec![
                Value::Utf8(format!("group-{index}")),
                Value::Int64(index),
                Value::Float64(index as f64),
            ])
            .collect::<Vec<_>>();
        let input = MockOperator::new(
            input_schema.clone(),
            vec![
                batch(&input_schema, input_rows[..2].to_vec()),
                batch(&input_schema, input_rows[2..4].to_vec()),
                batch(&input_schema, input_rows[4..].to_vec()),
            ],
        );
        let mut operator = GroupByOperator::new(
            Box::new(input),
            vec![0],
            vec![AggregateSpec::new(
                AggregateFunction::CountAll,
                Column::new("rows", DataType::Int64, false),
            )],
        ).unwrap();
        let mut context = ExecutionContext::new(ExecutionConfig {
            max_batch_rows: 2,
            max_batch_cells: 20,
            max_working_set_bytes: 64 * 1024,
        }).unwrap();

        let first = operator.next_batch(&mut context).unwrap().unwrap();
        let second = operator.next_batch(&mut context).unwrap().unwrap();
        let third = operator.next_batch(&mut context).unwrap().unwrap();
        assert_eq!(first.row_count, 2);
        assert_eq!(second.row_count, 2);
        assert_eq!(third.row_count, 1);
        assert_eq!(
            first.columns[0].values,
            vec![Value::Utf8("group-0".into()), Value::Utf8("group-1".into())]
        );
        assert_eq!(
            second.columns[0].values,
            vec![Value::Utf8("group-2".into()), Value::Utf8("group-3".into())]
        );
        assert_eq!(
            third.columns[0].values,
            vec![Value::Utf8("group-4".into())]
        );
        assert!(operator.next_batch(&mut context).unwrap().is_none());
    }

    #[test]
    fn group_state_respects_memory_budget() {
        let input_schema = VectorSchema::new(vec![
            Column::new("key", DataType::Utf8, true),
        ]).unwrap();
        let input = MockOperator::new(
            input_schema.clone(),
            vec![batch(&input_schema, vec![
                vec![Value::Utf8("x".repeat(128))],
            ])],
        );
        let mut operator = GroupByOperator::new(
            Box::new(input),
            vec![0],
            vec![AggregateSpec::new(
                AggregateFunction::CountAll,
                Column::new("rows", DataType::Int64, false),
            )],
        ).unwrap();
        let mut context = ExecutionContext::new(ExecutionConfig {
            max_batch_rows: 10,
            max_batch_cells: 100,
            max_working_set_bytes: 320,
        }).unwrap();
        assert!(matches!(
            operator.next_batch(&mut context),
            Err(ExecutionError::ResourceLimit { resource: "group state bytes", .. })
        ));
    }

    #[test]
    fn aggregate_output_types_are_checked_before_input_is_consumed() {
        let input_schema = schema();
        let input = MockOperator::new(input_schema, Vec::new());
        let result = GroupByOperator::new(
            Box::new(input),
            vec![],
            vec![AggregateSpec::new(
                AggregateFunction::Sum { column: 1 },
                Column::new("bad_sum", DataType::Float64, true),
            )],
        );
        assert!(matches!(result, Err(ExecutionError::TypeMismatch(_))));
    }

    #[test]
    fn integer_sum_overflow_is_reported_at_finalize() {
        let input_schema = schema();
        let input = MockOperator::new(
            input_schema.clone(),
            vec![batch(&input_schema, vec![
                vec![Value::Utf8("all".into()), Value::Int64(i64::MAX), Value::Null],
                vec![Value::Utf8("all".into()), Value::Int64(1), Value::Null],
            ])],
        );
        let mut operator = GroupByOperator::new(
            Box::new(input),
            vec![0],
            vec![AggregateSpec::new(
                AggregateFunction::Sum { column: 1 },
                Column::new("sum", DataType::Int64, true),
            )],
        ).unwrap();
        let mut context = ExecutionContext::default();
        assert!(matches!(
            operator.next_batch(&mut context),
            Err(ExecutionError::AggregateOverflow(_))
        ));
    }
}
