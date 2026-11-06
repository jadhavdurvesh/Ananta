pub mod aggregation;
pub mod statistics;
pub mod time_series;
pub use aggregation::{AggregateFunction, AggregateSpec, GroupByOperator};
pub use statistics::{analyze_table, ColumnStatistics, StatisticsFormatError, TableStatistics};
pub use time_series::{
    LatestPerSeriesOperator, TimeBucketOperator, TimestampBound, TimestampRangeOperator,
};

use ananta_core::{BatchColumn, Database, DbError, RecordBatch, ScanBatchIter};
use ananta_types::{Column, DataType, Schema, Value};
use std::cmp::Ordering;
use std::fmt;
use std::sync::{
    atomic::{AtomicBool, Ordering as AtomicOrdering},
    Arc,
};

/// The execution engine uses ANANTA's owned storage RecordBatch as its canonical
/// vector batch. Keeping one representation avoids copies at the storage boundary.
pub type VectorBatch = RecordBatch;

/// An execution schema. Duplicate names are permitted because projections may
/// intentionally emit aliases or select the same source column more than once.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VectorSchema {
    columns: Vec<Column>,
}

impl VectorSchema {
    pub fn new(columns: Vec<Column>) -> Result<Self, ExecutionError> {
        if columns.is_empty() {
            return Err(ExecutionError::InvalidSchema(
                "an execution batch must contain at least one column".into(),
            ));
        }
        if columns.iter().any(|column| column.name.trim().is_empty()) {
            return Err(ExecutionError::InvalidSchema(
                "execution column names cannot be empty".into(),
            ));
        }
        Ok(Self { columns })
    }

    pub fn from_storage(schema: &Schema) -> Result<Self, ExecutionError> {
        Self::new(schema.columns().to_vec())
    }

    pub fn columns(&self) -> &[Column] {
        &self.columns
    }

    pub fn len(&self) -> usize {
        self.columns.len()
    }

    pub fn is_empty(&self) -> bool {
        self.columns.is_empty()
    }

    pub fn validate_batch(&self, batch: &VectorBatch) -> Result<(), ExecutionError> {
        if batch.columns.len() != self.columns.len() {
            return Err(ExecutionError::InvalidBatch(format!(
                "batch has {} columns; schema requires {}",
                batch.columns.len(),
                self.columns.len()
            )));
        }

        for (index, (actual, expected)) in batch.columns.iter().zip(&self.columns).enumerate() {
            if actual.column != *expected {
                return Err(ExecutionError::InvalidBatch(format!(
                    "column {index} descriptor does not match the operator schema"
                )));
            }
            if actual.values.len() != batch.row_count {
                return Err(ExecutionError::InvalidBatch(format!(
                    "column {index} has {} values; batch declares {} rows",
                    actual.values.len(),
                    batch.row_count
                )));
            }
            for (row, value) in actual.values.iter().enumerate() {
                match value.data_type() {
                    None if !expected.nullable => {
                        return Err(ExecutionError::InvalidBatch(format!(
                            "row {row}, column {} contains NULL but the column is NOT NULL",
                            expected.name
                        )));
                    }
                    None => {}
                    Some(actual_type) if actual_type != expected.data_type => {
                        return Err(ExecutionError::InvalidBatch(format!(
                            "row {row}, column {} contains {actual_type}; expected {}",
                            expected.name, expected.data_type
                        )));
                    }
                    Some(_) => {}
                }
            }
        }
        Ok(())
    }
}

/// Execution limits apply to every operator boundary. The working-set limit
/// includes both input and output batches while a transforming operator runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExecutionConfig {
    pub max_batch_rows: usize,
    pub max_batch_cells: usize,
    pub max_working_set_bytes: usize,
}

impl Default for ExecutionConfig {
    fn default() -> Self {
        Self {
            max_batch_rows: 65_536,
            max_batch_cells: 1_000_000,
            max_working_set_bytes: 64 * 1024 * 1024,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ExecutionMetrics {
    /// Sum of batches emitted at operator boundaries in this execution context.
    pub output_batches: u64,
    /// Sum of rows emitted at operator boundaries in this execution context.
    pub output_rows: u64,
    /// Batches actually returned by the native storage scan source.
    pub scan_input_batches: u64,
    /// Rows actually decoded and returned by the native storage scan source.
    pub scan_input_rows: u64,
    /// Measured value-body bytes in decoded scan columns (not file/disk bytes).
    pub scan_input_value_payload_bytes: u64,
    pub peak_batch_bytes: usize,
    pub peak_working_set_bytes: usize,
}

pub struct ExecutionContext {
    config: ExecutionConfig,
    cancelled: Arc<AtomicBool>,
    metrics: ExecutionMetrics,
}

impl ExecutionContext {
    pub fn new(config: ExecutionConfig) -> Result<Self, ExecutionError> {
        Self::with_cancellation(config, Arc::new(AtomicBool::new(false)))
    }

    pub fn with_cancellation(
        config: ExecutionConfig,
        cancelled: Arc<AtomicBool>,
    ) -> Result<Self, ExecutionError> {
        if config.max_batch_rows == 0
            || config.max_batch_cells == 0
            || config.max_working_set_bytes == 0
        {
            return Err(ExecutionError::InvalidPlan(
                "execution resource limits must all be greater than zero".into(),
            ));
        }
        Ok(Self {
            config,
            cancelled,
            metrics: ExecutionMetrics::default(),
        })
    }

    pub fn config(&self) -> ExecutionConfig {
        self.config
    }

    /// Clone this token and set it from another thread to request cancellation.
    pub fn cancellation_token(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.cancelled)
    }

    pub fn cancel(&self) {
        self.cancelled.store(true, AtomicOrdering::Relaxed);
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(AtomicOrdering::Relaxed)
    }

    pub fn metrics(&self) -> &ExecutionMetrics {
        &self.metrics
    }

    pub fn check_cancelled(&self) -> Result<(), ExecutionError> {
        if self.is_cancelled() {
            Err(ExecutionError::Cancelled)
        } else {
            Ok(())
        }
    }

    fn validate_and_measure(
        &self,
        schema: &VectorSchema,
        batch: &VectorBatch,
    ) -> Result<usize, ExecutionError> {
        schema.validate_batch(batch)?;

        if batch.row_count > self.config.max_batch_rows {
            return Err(ExecutionError::ResourceLimit {
                resource: "batch rows",
                actual: batch.row_count,
                limit: self.config.max_batch_rows,
            });
        }

        let cells = batch
            .row_count
            .checked_mul(batch.columns.len())
            .ok_or(ExecutionError::ArithmeticOverflow)?;
        if cells > self.config.max_batch_cells {
            return Err(ExecutionError::ResourceLimit {
                resource: "batch cells",
                actual: cells,
                limit: self.config.max_batch_cells,
            });
        }

        let bytes = estimate_batch_bytes(batch)?;
        if bytes > self.config.max_working_set_bytes {
            return Err(ExecutionError::ResourceLimit {
                resource: "batch bytes",
                actual: bytes,
                limit: self.config.max_working_set_bytes,
            });
        }
        Ok(bytes)
    }

    /// Validate and account for a batch emitted by a source or in-place operator.
    pub fn check_batch(
        &mut self,
        schema: &VectorSchema,
        batch: &VectorBatch,
    ) -> Result<(), ExecutionError> {
        self.check_cancelled()?;
        let bytes = self.validate_and_measure(schema, batch)?;
        self.metrics.peak_batch_bytes = self.metrics.peak_batch_bytes.max(bytes);
        self.metrics.peak_working_set_bytes =
            self.metrics.peak_working_set_bytes.max(bytes);
        self.record_output(batch.row_count);
        Ok(())
    }

    /// Validate an input/output pair and account for the temporary working set of
    /// an operator that must materialise a transformed output batch.
    pub fn check_transition(
        &mut self,
        input_schema: &VectorSchema,
        input: &VectorBatch,
        output_schema: &VectorSchema,
        output: &VectorBatch,
    ) -> Result<(), ExecutionError> {
        self.check_cancelled()?;
        let input_bytes = self.validate_and_measure(input_schema, input)?;
        let output_bytes = self.validate_and_measure(output_schema, output)?;
        let working_set = input_bytes
            .checked_add(output_bytes)
            .ok_or(ExecutionError::ArithmeticOverflow)?;
        if working_set > self.config.max_working_set_bytes {
            return Err(ExecutionError::ResourceLimit {
                resource: "operator working set bytes",
                actual: working_set,
                limit: self.config.max_working_set_bytes,
            });
        }
        self.metrics.peak_batch_bytes = self
            .metrics
            .peak_batch_bytes
            .max(input_bytes)
            .max(output_bytes);
        self.metrics.peak_working_set_bytes =
            self.metrics.peak_working_set_bytes.max(working_set);
        self.record_output(output.row_count);
        Ok(())
    }

    fn record_output(&mut self, rows: usize) {
        self.metrics.output_batches = self.metrics.output_batches.saturating_add(1);
        self.metrics.output_rows = self
            .metrics
            .output_rows
            .saturating_add(u64::try_from(rows).unwrap_or(u64::MAX));
    }

    fn record_scan_input(&mut self, batch: &VectorBatch) {
        self.metrics.scan_input_batches = self.metrics.scan_input_batches.saturating_add(1);
        self.metrics.scan_input_rows = self.metrics.scan_input_rows
            .saturating_add(u64::try_from(batch.row_count).unwrap_or(u64::MAX));
        self.metrics.scan_input_value_payload_bytes = self.metrics.scan_input_value_payload_bytes
            .saturating_add(measured_value_payload_bytes(batch));
    }
}

impl Default for ExecutionContext {
    fn default() -> Self {
        Self::new(ExecutionConfig::default())
            .expect("the built-in execution limits are valid")
    }
}

fn measured_value_payload_bytes(batch: &VectorBatch) -> u64 {
    batch.columns.iter().flat_map(|column| column.values.iter()).fold(0_u64, |total, value| {
        let value_bytes = match value {
            Value::Null => 0,
            Value::Boolean(_) => 1,
            Value::Int32(_) => 4,
            Value::Int64(_) | Value::TimestampMicros(_) | Value::Float64(_) => 8,
            Value::Utf8(text) => u64::try_from(text.len()).unwrap_or(u64::MAX),
            Value::Binary(bytes) => u64::try_from(bytes.len()).unwrap_or(u64::MAX),
        };
        total.saturating_add(value_bytes)
    })
}

/// Estimate owned batch payload capacity, including vector, string and binary
/// allocations. This is a conservative accounting estimate, not allocator RSS.
pub fn estimate_batch_bytes(batch: &VectorBatch) -> Result<usize, ExecutionError> {
    let mut total = std::mem::size_of::<VectorBatch>();
    total = checked_add(
        total,
        batch
            .columns
            .capacity()
            .checked_mul(std::mem::size_of::<BatchColumn>())
            .ok_or(ExecutionError::ArithmeticOverflow)?,
    )?;

    for column in &batch.columns {
        total = checked_add(total, std::mem::size_of::<Column>())?;
        total = checked_add(total, column.column.name.capacity())?;
        total = checked_add(
            total,
            column
                .values
                .capacity()
                .checked_mul(std::mem::size_of::<Value>())
                .ok_or(ExecutionError::ArithmeticOverflow)?,
        )?;
        for value in &column.values {
            let dynamic_bytes = match value {
                Value::Utf8(value) => value.capacity(),
                Value::Binary(value) => value.capacity(),
                _ => 0,
            };
            total = checked_add(total, dynamic_bytes)?;
        }
    }
    Ok(total)
}

fn checked_add(left: usize, right: usize) -> Result<usize, ExecutionError> {
    left.checked_add(right)
        .ok_or(ExecutionError::ArithmeticOverflow)
}

#[derive(Debug)]
pub enum ExecutionError {
    Storage(DbError),
    AggregateOverflow(String),
    InvalidSchema(String),
    InvalidBatch(String),
    InvalidPlan(String),
    TypeMismatch(String),
    ResourceLimit {
        resource: &'static str,
        actual: usize,
        limit: usize,
    },
    ArithmeticOverflow,
    Cancelled,
}

impl fmt::Display for ExecutionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Storage(error) => write!(f, "storage error: {error}"),
            Self::AggregateOverflow(function) => write!(f, "{function} aggregate result exceeds the supported output range"),
            Self::InvalidSchema(message) => write!(f, "invalid execution schema: {message}"),
            Self::InvalidBatch(message) => write!(f, "invalid vector batch: {message}"),
            Self::InvalidPlan(message) => write!(f, "invalid execution plan: {message}"),
            Self::TypeMismatch(message) => write!(f, "execution type mismatch: {message}"),
            Self::ResourceLimit {
                resource,
                actual,
                limit,
            } => write!(
                f,
                "execution resource limit exceeded for {resource}: {actual} > {limit}"
            ),
            Self::ArithmeticOverflow => f.write_str("execution size arithmetic overflow"),
            Self::Cancelled => f.write_str("query execution was cancelled"),
        }
    }
}

impl std::error::Error for ExecutionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Storage(error) => Some(error),
            _ => None,
        }
    }
}

impl From<DbError> for ExecutionError {
    fn from(value: DbError) -> Self {
        Self::Storage(value)
    }
}

/// Pull-based operator interface. Each call yields at most one owned vector batch.
pub trait ExecutionOperator {
    fn schema(&self) -> &VectorSchema;

    fn next_batch(
        &mut self,
        context: &mut ExecutionContext,
    ) -> Result<Option<VectorBatch>, ExecutionError>;
}

/// Storage scan source. The database owns its native ANF/Chronicle readers; this
/// operator only adapts the existing bounded scan iterator to the execution API.
pub struct ScanOperator {
    input: ScanBatchIter,
    schema: VectorSchema,
    finished: bool,
}

impl ScanOperator {
    pub fn new(
        database: &Database,
        table: &str,
        batch_rows: usize,
    ) -> Result<Self, ExecutionError> {
        let column_count = database.schema(table)?.len();
        let columns = (0..column_count).collect::<Vec<_>>();
        Self::new_projected(database, table, &columns, batch_rows)
    }

    /// Construct a storage scan that reads only the requested logical columns.
    /// Column indices are resolved against the current table schema and their
    /// order (including intentional duplicates) is preserved.
    pub fn new_projected(
        database: &Database,
        table: &str,
        column_indices: &[usize],
        batch_rows: usize,
    ) -> Result<Self, ExecutionError> {
        Self::new_projected_with_limit(database, table, column_indices, batch_rows, None)
    }

    /// Projected storage scan with an optional storage-level row ceiling. The
    /// caller must only push a limit here when no filtering operator can discard
    /// rows before that limit is applied.
    pub fn new_projected_with_limit(
        database: &Database,
        table: &str,
        column_indices: &[usize],
        batch_rows: usize,
        row_limit: Option<u64>,
    ) -> Result<Self, ExecutionError> {
        if batch_rows == 0 {
            return Err(ExecutionError::InvalidPlan(
                "scan batch_rows must be greater than zero".into(),
            ));
        }
        if column_indices.is_empty() {
            return Err(ExecutionError::InvalidPlan(
                "projected scan requires at least one column".into(),
            ));
        }
        let storage_schema = database.schema(table)?;
        let mut columns = Vec::with_capacity(column_indices.len());
        let mut names = Vec::with_capacity(column_indices.len());
        for index in column_indices {
            let column = storage_schema.columns().get(*index).ok_or_else(|| {
                ExecutionError::InvalidPlan(format!(
                    "projected scan references missing column {index}"
                ))
            })?;
            columns.push(column.clone());
            names.push(column.name.as_str());
        }
        let schema = VectorSchema::new(columns)?;
        let input = match row_limit.and_then(|limit| usize::try_from(limit).ok()) {
            Some(limit) => database.scan_columns_batches_with_limit(
                table,
                &names,
                batch_rows,
                limit,
            )?,
            None => database.scan_columns_batches(table, &names, batch_rows)?,
        };
        Ok(Self {
            input,
            schema,
            finished: false,
        })
    }
}

impl ExecutionOperator for ScanOperator {
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
        match self.input.next() {
            Some(Ok(batch)) => {
                context.check_batch(&self.schema, &batch)?;
                context.record_scan_input(&batch);
                Ok(Some(batch))
            }
            Some(Err(error)) => {
                self.finished = true;
                Err(ExecutionError::Storage(error))
            }
            None => {
                self.finished = true;
                Ok(None)
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectionField {
    pub input_index: usize,
    pub output_column: Column,
}

impl ProjectionField {
    pub fn new(input_index: usize, output_column: Column) -> Self {
        Self {
            input_index,
            output_column,
        }
    }
}

pub struct ProjectionOperator {
    input: Box<dyn ExecutionOperator>,
    input_schema: VectorSchema,
    schema: VectorSchema,
    fields: Vec<ProjectionField>,
    finished: bool,
}

impl ProjectionOperator {
    pub fn new(
        input: Box<dyn ExecutionOperator>,
        fields: Vec<ProjectionField>,
    ) -> Result<Self, ExecutionError> {
        if fields.is_empty() {
            return Err(ExecutionError::InvalidPlan(
                "projection must contain at least one field".into(),
            ));
        }
        let input_schema = input.schema().clone();
        for field in &fields {
            let source = input_schema.columns().get(field.input_index).ok_or_else(|| {
                ExecutionError::InvalidPlan(format!(
                    "projection references missing input column {}",
                    field.input_index
                ))
            })?;
            if source.data_type != field.output_column.data_type {
                return Err(ExecutionError::TypeMismatch(format!(
                    "projection of {} changes type from {} to {}",
                    source.name, source.data_type, field.output_column.data_type
                )));
            }
            if source.nullable && !field.output_column.nullable {
                return Err(ExecutionError::InvalidPlan(format!(
                    "projection cannot mark nullable column {} as NOT NULL",
                    source.name
                )));
            }
        }
        let schema = VectorSchema::new(
            fields
                .iter()
                .map(|field| field.output_column.clone())
                .collect(),
        )?;
        Ok(Self {
            input,
            input_schema,
            schema,
            fields,
            finished: false,
        })
    }
}

impl ExecutionOperator for ProjectionOperator {
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
        let Some(input) = self.input.next_batch(context)? else {
            self.finished = true;
            return Ok(None);
        };
        let columns = self
            .fields
            .iter()
            .map(|field| BatchColumn {
                column: field.output_column.clone(),
                values: input.columns[field.input_index].values.clone(),
            })
            .collect();
        let output = RecordBatch {
            columns,
            row_count: input.row_count,
        };
        context.check_transition(&self.input_schema, &input, &self.schema, &output)?;
        Ok(Some(output))
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Operand {
    Column(usize),
    Literal(Value),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ComparisonOperator {
    Equal,
    NotEqual,
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,
}

/// A typed, NULL-aware predicate tree for the first native filter operator.
#[derive(Debug, Clone, PartialEq)]
pub enum FilterPredicate {
    Constant(bool),
    Compare {
        left: Operand,
        operator: ComparisonOperator,
        right: Operand,
    },
    IsNull {
        operand: Operand,
        negated: bool,
    },
    And(Box<FilterPredicate>, Box<FilterPredicate>),
    Or(Box<FilterPredicate>, Box<FilterPredicate>),
    Not(Box<FilterPredicate>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TruthValue {
    True,
    False,
    Unknown,
}

impl TruthValue {
    fn and(self, other: Self) -> Self {
        match (self, other) {
            (Self::False, _) | (_, Self::False) => Self::False,
            (Self::True, Self::True) => Self::True,
            _ => Self::Unknown,
        }
    }

    fn or(self, other: Self) -> Self {
        match (self, other) {
            (Self::True, _) | (_, Self::True) => Self::True,
            (Self::False, Self::False) => Self::False,
            _ => Self::Unknown,
        }
    }

    fn not(self) -> Self {
        match self {
            Self::True => Self::False,
            Self::False => Self::True,
            Self::Unknown => Self::Unknown,
        }
    }
}

impl FilterPredicate {
    fn validate_for(&self, schema: &VectorSchema) -> Result<(), ExecutionError> {
        match self {
            Self::Constant(_) => Ok(()),
            Self::Compare { left, right, .. } => {
                let left_type = operand_type(left, schema)?;
                let right_type = operand_type(right, schema)?;
                if let (Some(left_type), Some(right_type)) = (left_type, right_type) {
                    if left_type != right_type {
                        return Err(ExecutionError::TypeMismatch(format!(
                            "cannot compare {left_type} with {right_type}"
                        )));
                    }
                }
                Ok(())
            }
            Self::IsNull { operand, .. } => {
                operand_type(operand, schema)?;
                Ok(())
            }
            Self::And(left, right) | Self::Or(left, right) => {
                left.validate_for(schema)?;
                right.validate_for(schema)
            }
            Self::Not(expression) => expression.validate_for(schema),
        }
    }

    fn evaluate(
        &self,
        batch: &VectorBatch,
        row: usize,
    ) -> Result<TruthValue, ExecutionError> {
        match self {
            Self::Constant(value) => Ok(if *value {
                TruthValue::True
            } else {
                TruthValue::False
            }),
            Self::Compare {
                left,
                operator,
                right,
            } => {
                let left = operand_value(left, batch, row)?;
                let right = operand_value(right, batch, row)?;
                let Some(ordering) = compare_values(left, right)? else {
                    return Ok(TruthValue::Unknown);
                };
                let result = match operator {
                    ComparisonOperator::Equal => ordering == Ordering::Equal,
                    ComparisonOperator::NotEqual => ordering != Ordering::Equal,
                    ComparisonOperator::LessThan => ordering == Ordering::Less,
                    ComparisonOperator::LessThanOrEqual => ordering != Ordering::Greater,
                    ComparisonOperator::GreaterThan => ordering == Ordering::Greater,
                    ComparisonOperator::GreaterThanOrEqual => ordering != Ordering::Less,
                };
                Ok(if result {
                    TruthValue::True
                } else {
                    TruthValue::False
                })
            }
            Self::IsNull { operand, negated } => {
                let is_null = matches!(operand_value(operand, batch, row)?, Value::Null);
                Ok(if is_null != *negated {
                    TruthValue::True
                } else {
                    TruthValue::False
                })
            }
            Self::And(left, right) => {
                Ok(left.evaluate(batch, row)?.and(right.evaluate(batch, row)?))
            }
            Self::Or(left, right) => {
                Ok(left.evaluate(batch, row)?.or(right.evaluate(batch, row)?))
            }
            Self::Not(expression) => Ok(expression.evaluate(batch, row)?.not()),
        }
    }
}

fn operand_type(
    operand: &Operand,
    schema: &VectorSchema,
) -> Result<Option<DataType>, ExecutionError> {
    match operand {
        Operand::Column(index) => schema
            .columns()
            .get(*index)
            .map(|column| Some(column.data_type))
            .ok_or_else(|| {
                ExecutionError::InvalidPlan(format!(
                    "predicate references missing input column {index}"
                ))
            }),
        Operand::Literal(value) => Ok(value.data_type()),
    }
}

fn operand_value<'a>(
    operand: &'a Operand,
    batch: &'a VectorBatch,
    row: usize,
) -> Result<&'a Value, ExecutionError> {
    match operand {
        Operand::Column(index) => batch
            .columns
            .get(*index)
            .and_then(|column| column.values.get(row))
            .ok_or_else(|| {
                ExecutionError::InvalidBatch(format!(
                    "predicate references missing value at column {index}, row {row}"
                ))
            }),
        Operand::Literal(value) => Ok(value),
    }
}

fn compare_values(
    left: &Value,
    right: &Value,
) -> Result<Option<Ordering>, ExecutionError> {
    let ordering = match (left, right) {
        (Value::Null, _) | (_, Value::Null) => return Ok(None),
        (Value::Boolean(left), Value::Boolean(right)) => left.cmp(right),
        (Value::Int32(left), Value::Int32(right)) => left.cmp(right),
        (Value::Int64(left), Value::Int64(right)) => left.cmp(right),
        (Value::TimestampMicros(left), Value::TimestampMicros(right)) => left.cmp(right),
        (Value::Float64(left), Value::Float64(right)) => {
            let Some(ordering) = left.partial_cmp(right) else {
                return Ok(None);
            };
            ordering
        }
        (Value::Utf8(left), Value::Utf8(right)) => left.cmp(right),
        (Value::Binary(left), Value::Binary(right)) => left.cmp(right),
        _ => {
            return Err(ExecutionError::TypeMismatch(
                "comparison values have different data types".into(),
            ))
        }
    };
    Ok(Some(ordering))
}

pub struct FilterOperator {
    input: Box<dyn ExecutionOperator>,
    schema: VectorSchema,
    predicate: FilterPredicate,
    finished: bool,
}

impl FilterOperator {
    pub fn new(
        input: Box<dyn ExecutionOperator>,
        predicate: FilterPredicate,
    ) -> Result<Self, ExecutionError> {
        let schema = input.schema().clone();
        predicate.validate_for(&schema)?;
        Ok(Self {
            input,
            schema,
            predicate,
            finished: false,
        })
    }
}

impl ExecutionOperator for FilterOperator {
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

            let mut selected = Vec::with_capacity(input.row_count);
            for row in 0..input.row_count {
                if row % 1024 == 0 {
                    context.check_cancelled()?;
                }
                if self.predicate.evaluate(&input, row)? == TruthValue::True {
                    selected.push(row);
                }
            }
            if selected.is_empty() {
                continue;
            }

            let columns = input
                .columns
                .iter()
                .map(|column| BatchColumn {
                    column: column.column.clone(),
                    values: selected
                        .iter()
                        .map(|row| column.values[*row].clone())
                        .collect(),
                })
                .collect();
            let output = RecordBatch {
                columns,
                row_count: selected.len(),
            };
            context.check_transition(&self.schema, &input, &self.schema, &output)?;
            return Ok(Some(output));
        }
    }
}

pub struct LimitOperator {
    input: Box<dyn ExecutionOperator>,
    schema: VectorSchema,
    remaining: u64,
    finished: bool,
}

impl LimitOperator {
    pub fn new(input: Box<dyn ExecutionOperator>, rows: u64) -> Self {
        Self {
            schema: input.schema().clone(),
            input,
            remaining: rows,
            finished: rows == 0,
        }
    }
}

impl ExecutionOperator for LimitOperator {
    fn schema(&self) -> &VectorSchema {
        &self.schema
    }

    fn next_batch(
        &mut self,
        context: &mut ExecutionContext,
    ) -> Result<Option<VectorBatch>, ExecutionError> {
        if self.finished || self.remaining == 0 {
            self.finished = true;
            return Ok(None);
        }
        context.check_cancelled()?;
        let Some(mut batch) = self.input.next_batch(context)? else {
            self.finished = true;
            return Ok(None);
        };

        let keep = usize::try_from(self.remaining)
            .unwrap_or(usize::MAX)
            .min(batch.row_count);
        if keep < batch.row_count {
            for column in &mut batch.columns {
                column.values.truncate(keep);
            }
            batch.row_count = keep;
            self.remaining = 0;
            self.finished = true;
        } else {
            self.remaining -= u64::try_from(keep).unwrap_or(u64::MAX);
        }
        context.check_batch(&self.schema, &batch)?;
        Ok(Some(batch))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;
    use ananta_types::{Column, DataType, Value};

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

    fn test_schema() -> VectorSchema {
        VectorSchema::new(vec![
            Column::new("id", DataType::Int64, false),
            Column::new("region", DataType::Utf8, true),
            Column::new("active", DataType::Boolean, false),
        ])
        .unwrap()
    }

    fn make_batch(schema: &VectorSchema, rows: Vec<Vec<Value>>) -> VectorBatch {
        let row_count = rows.len();
        let mut columns = schema
            .columns()
            .iter()
            .cloned()
            .map(|column| BatchColumn {
                column,
                values: Vec::with_capacity(row_count),
            })
            .collect::<Vec<_>>();
        for row in rows {
            assert_eq!(row.len(), columns.len());
            for (column, value) in columns.iter_mut().zip(row) {
                column.values.push(value);
            }
        }
        RecordBatch { columns, row_count }
    }

    #[test]
    fn vector_schema_rejects_wrong_width_types_and_nullability() {
        let schema = test_schema();
        let wrong_type = VectorBatch {
            columns: vec![BatchColumn {
                column: Column::new("id", DataType::Int64, false),
                values: vec![Value::Int32(1)],
            }],
            row_count: 1,
        };
        assert!(matches!(
            schema.validate_batch(&wrong_type),
            Err(ExecutionError::InvalidBatch(_))
        ));

        let null_id = make_batch(
            &schema,
            vec![vec![Value::Null, Value::Utf8("west".into()), Value::Boolean(true)]],
        );
        assert!(matches!(
            schema.validate_batch(&null_id),
            Err(ExecutionError::InvalidBatch(_))
        ));
    }

    #[test]
    fn projection_filter_and_limit_form_a_streaming_pipeline() {
        let schema = test_schema();
        let input = MockOperator::new(
            schema,
            vec![make_batch(
                &test_schema(),
                vec![
                    vec![Value::Int64(1), Value::Utf8("west".into()), Value::Boolean(true)],
                    vec![Value::Int64(2), Value::Null, Value::Boolean(true)],
                    vec![Value::Int64(3), Value::Utf8("east".into()), Value::Boolean(false)],
                ],
            )],
        );
        let predicate = FilterPredicate::And(
            Box::new(FilterPredicate::Compare {
                left: Operand::Column(0),
                operator: ComparisonOperator::GreaterThan,
                right: Operand::Literal(Value::Int64(1)),
            }),
            Box::new(FilterPredicate::IsNull {
                operand: Operand::Column(1),
                negated: true,
            }),
        );
        let filter = FilterOperator::new(Box::new(input), predicate).unwrap();
        let projection = ProjectionOperator::new(
            Box::new(filter),
            vec![
                ProjectionField::new(0, Column::new("device_id", DataType::Int64, false)),
                ProjectionField::new(0, Column::new("device_id_copy", DataType::Int64, false)),
            ],
        )
        .unwrap();
        let mut limit = LimitOperator::new(Box::new(projection), 1);
        let mut context = ExecutionContext::default();

        let result = limit.next_batch(&mut context).unwrap().unwrap();
        assert_eq!(result.row_count, 1);
        assert_eq!(result.columns[0].values, vec![Value::Int64(3)]);
        assert_eq!(result.columns[1].values, vec![Value::Int64(3)]);
        assert!(limit.next_batch(&mut context).unwrap().is_none());
        assert!(context.metrics().output_batches >= 3);
        assert!(context.metrics().peak_batch_bytes > 0);
    }

    #[test]
    fn filter_uses_three_valued_logic_for_null_comparisons() {
        let schema = test_schema();
        let input = MockOperator::new(
            schema.clone(),
            vec![make_batch(
                &schema,
                vec![
                    vec![Value::Int64(1), Value::Utf8("west".into()), Value::Boolean(true)],
                    vec![Value::Int64(2), Value::Null, Value::Boolean(true)],
                ],
            )],
        );
        let predicate = FilterPredicate::Compare {
            left: Operand::Column(1),
            operator: ComparisonOperator::Equal,
            right: Operand::Literal(Value::Utf8("west".into())),
        };
        let mut filter = FilterOperator::new(Box::new(input), predicate).unwrap();
        let mut context = ExecutionContext::default();
        let result = filter.next_batch(&mut context).unwrap().unwrap();
        assert_eq!(result.row_count, 1);
        assert_eq!(result.columns[0].values, vec![Value::Int64(1)]);
        assert!(filter.next_batch(&mut context).unwrap().is_none());
    }

    #[test]
    fn filter_rejects_invalid_column_and_mismatched_comparison_types() {
        let schema = test_schema();
        let input = || {
            Box::new(MockOperator::new(schema.clone(), Vec::new()))
                as Box<dyn ExecutionOperator>
        };
        assert!(matches!(
            FilterOperator::new(
                input(),
                FilterPredicate::IsNull {
                    operand: Operand::Column(99),
                    negated: false,
                }
            ),
            Err(ExecutionError::InvalidPlan(_))
        ));
        assert!(matches!(
            FilterOperator::new(
                input(),
                FilterPredicate::Compare {
                    left: Operand::Column(0),
                    operator: ComparisonOperator::Equal,
                    right: Operand::Literal(Value::Utf8("wrong".into())),
                }
            ),
            Err(ExecutionError::TypeMismatch(_))
        ));
    }

    #[test]
    fn execution_context_enforces_batch_row_cell_and_memory_limits() {
        let schema = test_schema();
        let batch = make_batch(
            &schema,
            vec![
                vec![Value::Int64(1), Value::Null, Value::Boolean(true)],
                vec![Value::Int64(2), Value::Null, Value::Boolean(false)],
            ],
        );
        let mut context = ExecutionContext::new(ExecutionConfig {
            max_batch_rows: 1,
            max_batch_cells: 2,
            max_working_set_bytes: 1024,
        })
        .unwrap();
        assert!(matches!(
            context.check_batch(&schema, &batch),
            Err(ExecutionError::ResourceLimit {
                resource: "batch rows",
                ..
            })
        ));

        let mut invalid = ExecutionConfig::default();
        invalid.max_working_set_bytes = 0;
        assert!(matches!(
            ExecutionContext::new(invalid),
            Err(ExecutionError::InvalidPlan(_))
        ));
    }

    #[test]
    fn cancellation_is_checked_before_a_source_emits_another_batch() {
        let schema = test_schema();
        let input = MockOperator::new(
            schema.clone(),
            vec![make_batch(
                &schema,
                vec![vec![
                    Value::Int64(1),
                    Value::Utf8("west".into()),
                    Value::Boolean(true),
                ]],
            )],
        );
        let mut input = LimitOperator::new(Box::new(input), 10);
        let context = ExecutionContext::default();
        context.cancel();
        let mut context = context;
        assert!(matches!(
            input.next_batch(&mut context),
            Err(ExecutionError::Cancelled)
        ));
    }

    #[test]
    fn null_test_operator_handles_is_null_and_not_is_null() {
        let schema = test_schema();
        let input = MockOperator::new(
            schema.clone(),
            vec![make_batch(
                &schema,
                vec![
                    vec![Value::Int64(1), Value::Null, Value::Boolean(true)],
                    vec![Value::Int64(2), Value::Utf8("east".into()), Value::Boolean(true)],
                ],
            )],
        );
        let mut filter = FilterOperator::new(
            Box::new(input),
            FilterPredicate::IsNull {
                operand: Operand::Column(1),
                negated: false,
            },
        )
        .unwrap();
        let mut context = ExecutionContext::default();
        let result = filter.next_batch(&mut context).unwrap().unwrap();
        assert_eq!(result.row_count, 1);
        assert_eq!(result.columns[0].values, vec![Value::Int64(1)]);
    }
}
