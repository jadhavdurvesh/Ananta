use crate::{
    AqlError, AqlErrorKind, BinaryOperator, ColumnReference, Expr, ExprKind, Identifier,
    Literal, ParameterBindings, ParameterRef, Projection, Query, Statement, UnaryOperator,
};
use ananta_core::{Database, DbError, ScanBatchIter};
use ananta_execution::{
    ComparisonOperator as NativeComparison, ExecutionContext, ExecutionError, ExecutionOperator,
    FilterOperator, FilterPredicate, LimitOperator, Operand, ProjectionField, ProjectionOperator,
    ScanOperator, TableStatistics, ColumnStatistics,
};
use ananta_types::{Column, DataType, Schema, Value};
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AqlErrorCode {
    InvalidQuery,
    TableNotFound,
    ColumnNotFound,
    TypeMismatch,
    InvalidParameter,
    InvalidCast,
    Arithmetic,
    Cancelled,
    Storage,
    ResourceLimit,
    Execution,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AqlExecutionError {
    pub code: AqlErrorCode,
    pub message: String,
    pub span: crate::Span,
}
impl std::fmt::Display for AqlExecutionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} at {}:{}", self.message, self.span.start.line, self.span.start.column)
    }
}
impl std::error::Error for AqlExecutionError {}

#[derive(Debug, Clone, PartialEq)]
pub struct AqlResultBatch {
    pub schema: Vec<Column>,
    /// One owned value vector per output column.
    pub columns: Vec<Vec<Value>>,
    pub row_count: usize,
}
impl AqlResultBatch {
    pub fn column(&self, index: usize) -> Option<&[Value]> {
        self.columns.get(index).map(Vec::as_slice)
    }
    pub fn column_by_name(&self, name: &str) -> Option<&[Value]> {
        self.schema.iter().position(|column| column.name == name)
            .and_then(|index| self.column(index))
    }
    pub fn row(&self, index: usize) -> Option<Vec<Value>> {
        if index >= self.row_count { return None; }
        Some(self.columns.iter().map(|column| column[index].clone()).collect())
    }
}

#[derive(Debug, Clone)]
struct CompiledExpr {
    kind: CompiledExprKind,
    data_type: Option<DataType>,
    nullable: bool,
    span: crate::Span,
}
#[derive(Debug, Clone)]
enum CompiledExprKind {
    Column(usize),
    Literal(Literal),
    Parameter(ParameterRef),
    Unary { operator: UnaryOperator, expression: Box<CompiledExpr> },
    Binary { left: Box<CompiledExpr>, operator: BinaryOperator, right: Box<CompiledExpr> },
    IsNull { expression: Box<CompiledExpr>, negated: bool },
    Cast { expression: Box<CompiledExpr>, target: DataType },
}

#[derive(Debug, Clone)]
struct CompiledProjection {
    expression: CompiledExpr,
    column: Column,
}

struct NativeAqlExecution {
    operator: Box<dyn ExecutionOperator>,
    context: ExecutionContext,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AqlExecutionMetrics {
    pub output_batches: u64,
    pub output_rows: u64,
    pub output_cells: u64,
    /// Decoded input batches returned by the native storage scan.
    pub scan_input_batches: u64,
    /// Decoded input rows returned by the native storage scan.
    pub scan_input_rows: u64,
    /// Measured value-body bytes in decoded scan input; not physical disk bytes.
    pub scan_input_value_payload_bytes: u64,
    /// Measured value-body bytes returned to the caller. This excludes vector,
    /// descriptor, allocator and storage-file overhead; it is not disk I/O bytes.
    pub output_value_payload_bytes: u64,
    /// Estimated owned result-batch footprint, not allocator RSS or total query memory.
    pub output_bytes: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AqlPhysicalPlan {
    pub table: String,
    pub native_execution: bool,
    pub scan_columns: Vec<String>,
    pub predicate_pushed: bool,
    pub storage_limit_pushed: bool,
    pub chronicle_accepted_rows: u64,
    pub chronicle_accepted_encoded_bytes: u64,
    pub estimated_rows_after_filter: Option<u64>,
    pub estimated_output_rows: Option<u64>,
    pub estimated_projected_payload_bytes: Option<u64>,
    pub notes: Vec<String>,
}

impl AqlPhysicalPlan {
    pub fn explain(&self) -> String {
        let mut lines = Vec::new();
        lines.push(format!("ANANTA PHYSICAL PLAN — {}", self.table));
        lines.push(format!(
            "  Native vector execution: {}",
            if self.native_execution { "enabled" } else { "fallback" }
        ));
        lines.push(format!(
            "  Scan: {}",
            if self.native_execution {
                format!("projected columns [{}]", self.scan_columns.join(", "))
            } else {
                "all table columns (reference evaluator)".to_owned()
            }
        ));
        if self.predicate_pushed {
            lines.push("  Filter: native vector predicate".to_owned());
        } else {
            lines.push("  Filter: reference evaluator".to_owned());
        }
        lines.push("  Projection: output expressions".to_owned());
        if self.storage_limit_pushed {
            lines.push("  Limit: pushed into bounded storage scan".to_owned());
        } else {
            lines.push("  Limit: applied by execution operator/evaluator".to_owned());
        }
        lines.push(format!(
            "  Chronicle table rows: {} (ingestion index)",
            self.chronicle_accepted_rows
        ));
        lines.push(format!(
            "  Chronicle encoded bytes: {} (ingestion index)",
            self.chronicle_accepted_encoded_bytes
        ));
        lines.push(format!(
            "  Estimated rows after filter: {}",
            self.estimated_rows_after_filter
                .map_or_else(|| "unknown (no table row-count stats)".to_owned(), |rows| rows.to_string())
        ));
        lines.push(format!(
            "  Estimated output rows: {}",
            self.estimated_output_rows
                .map_or_else(|| "unknown (no table row-count stats)".to_owned(), |rows| rows.to_string())
        ));
        lines.push(format!(
            "  Estimated projected payload bytes: {}",
            self.estimated_projected_payload_bytes
                .map_or_else(|| "unknown (run analyze_table first)".to_owned(), |bytes| bytes.to_string())
        ));
        for note in &self.notes {
            lines.push(format!("  Note: {note}"));
        }
        lines.join("\n")
    }
}

pub struct AqlResultIter {
    input: Option<ScanBatchIter>,
    native: Option<NativeAqlExecution>,
    projection: Vec<CompiledProjection>,
    predicate: Option<CompiledExpr>,
    bindings: ParameterBindings,
    remaining_limit: Option<u64>,
    cancellation: Option<Arc<AtomicBool>>,
    metrics: AqlExecutionMetrics,
    finished: bool,
}

pub fn execute(
    database: &Database,
    query: &Query,
    bindings: &ParameterBindings,
    batch_rows: usize,
    cancellation: Option<Arc<AtomicBool>>,
) -> Result<AqlResultIter, AqlExecutionError> {
    execute_with_options(database, query, bindings, batch_rows, cancellation, true)
}

/// Execute with the native physical optimizer enabled or disabled. The disabled
/// path is retained as a correctness reference and fallback for expressions
/// that are not yet lowerable to native vector operators.
pub fn execute_with_options(
    database: &Database,
    query: &Query,
    bindings: &ParameterBindings,
    batch_rows: usize,
    cancellation: Option<Arc<AtomicBool>>,
    optimizer_enabled: bool,
) -> Result<AqlResultIter, AqlExecutionError> {
    crate::validate_ast(query).map_err(from_parse_error)?;
    bindings.validate_for(query).map_err(from_parse_error)?;
    if batch_rows == 0 {
        return Err(query_error(
            AqlErrorCode::InvalidQuery,
            "AQL batch_rows must be greater than zero",
            crate::Span::default(),
        ));
    }
    let Statement::Select(select) = &query.statement;
    let source = resolve_table_name(database, &select.source)?;
    let schema = database.schema(&source).map_err(from_storage_error)?.clone();

    let projection = compile_projection(select, &schema, &source, bindings)?;
    let predicate = compile_predicate(select, &schema, &source, bindings)?;

    let native = if optimizer_enabled {
        try_build_native_pipeline(
            database,
            &source,
            &schema,
            &projection,
            predicate.as_ref(),
            bindings,
            select.limit,
            batch_rows,
            cancellation.clone(),
        )?
    } else {
        None
    };
    let input = if native.is_none() {
        Some(database.scan_batches(&source, batch_rows).map_err(from_storage_error)?)
    } else {
        None
    };

    Ok(AqlResultIter {
        input,
        native,
        projection,
        predicate,
        bindings: bindings.clone(),
        remaining_limit: select.limit,
        cancellation,
        metrics: AqlExecutionMetrics::default(),
        finished: false,
    })
}

fn compile_projection(
    select: &crate::SelectStatement,
    schema: &Schema,
    source: &str,
    bindings: &ParameterBindings,
) -> Result<Vec<CompiledProjection>, AqlExecutionError> {
    match &select.projection {
        Projection::All => Ok(schema.columns().iter().enumerate().map(|(index, column)| {
            CompiledProjection {
                expression: CompiledExpr {
                    kind: CompiledExprKind::Column(index),
                    data_type: Some(column.data_type),
                    nullable: column.nullable,
                    span: select.source.span,
                },
                column: column.clone(),
            }
        }).collect()),
        Projection::Items(items) => {
            let mut compiled = Vec::with_capacity(items.len());
            for (index, item) in items.iter().enumerate() {
                let expression = compile_expr(&item.expression, schema, source, bindings)?;
                let data_type = expression.data_type.ok_or_else(|| query_error(
                    AqlErrorCode::TypeMismatch,
                    "an untyped NULL cannot be projected without an explicit CAST",
                    item.expression.span,
                ))?;
                let name = if let Some(alias) = &item.alias {
                    alias.value.clone()
                } else if let ExprKind::Column(reference) = &item.expression.kind {
                    reference.parts.last().map_or_else(|| format!("expr_{}", index + 1),
                        |identifier| identifier.value.clone())
                } else {
                    format!("expr_{}", index + 1)
                };
                let nullable = expression.nullable;
                compiled.push(CompiledProjection {
                    expression,
                    column: Column::new(name, data_type, nullable),
                });
            }
            Ok(compiled)
        }
    }
}

fn compile_predicate(
    select: &crate::SelectStatement,
    schema: &Schema,
    source: &str,
    bindings: &ParameterBindings,
) -> Result<Option<CompiledExpr>, AqlExecutionError> {
    match &select.selection {
        Some(expression) => {
            let compiled = compile_expr(expression, schema, source, bindings)?;
            if compiled.data_type.is_some_and(|kind| kind != DataType::Boolean) {
                return Err(query_error(
                    AqlErrorCode::TypeMismatch,
                    "WHERE expression must have type BOOLEAN",
                    expression.span,
                ));
            }
            Ok(Some(compiled))
        }
        None => Ok(None),
    }
}

/// Compile and explain the physical path without consuming table rows.
/// Cardinality and projected-byte figures are estimates; persisted Chronicle
/// ingestion counters are reported separately as measured accounting values.
pub fn explain(
    database: &Database,
    query: &Query,
    bindings: &ParameterBindings,
    optimizer_enabled: bool,
) -> Result<AqlPhysicalPlan, AqlExecutionError> {
    explain_with_statistics(database, query, bindings, optimizer_enabled, None)
}

/// Explain using optional table statistics collected explicitly by
/// ananta_execution::analyze_table. Statistics are never scanned implicitly.
pub fn explain_with_statistics(
    database: &Database,
    query: &Query,
    bindings: &ParameterBindings,
    optimizer_enabled: bool,
    table_statistics: Option<&TableStatistics>,
) -> Result<AqlPhysicalPlan, AqlExecutionError> {
    crate::validate_ast(query).map_err(from_parse_error)?;
    bindings.validate_for(query).map_err(from_parse_error)?;
    let Statement::Select(select) = &query.statement;
    let table = resolve_table_name(database, &select.source)?;
    let schema = database.schema(&table).map_err(from_storage_error)?.clone();
    let projection = compile_projection(select, &schema, &table, bindings)?;
    let predicate = compile_predicate(select, &schema, &table, bindings)?;
    let native_predicate = match predicate.as_ref() {
        Some(expression) => compiled_filter_predicate(expression, bindings),
        None => None,
    };
    let direct_projection = projection.iter().all(|item| {
        matches!(&item.expression.kind, CompiledExprKind::Column(_))
    });
    let mut required_columns = BTreeSet::new();
    for item in &projection {
        if let CompiledExprKind::Column(index) = &item.expression.kind {
            required_columns.insert(*index);
        }
    }
    if let Some(predicate) = &native_predicate {
        collect_filter_columns(predicate, &mut required_columns);
    }
    let native_execution = optimizer_enabled
        && direct_projection
        && predicate.as_ref().map_or(true, |_| native_predicate.is_some())
        && !required_columns.is_empty();
    let scan_columns = if native_execution {
        required_columns.iter().map(|index| schema.columns()[*index].name.clone()).collect::<Vec<_>>()
    } else {
        schema.columns().iter().map(|column| column.name.clone()).collect::<Vec<_>>()
    };
    let ingestion = database.ingestion_stats(&table).map_err(from_storage_error)?;
    // Prefer explicitly supplied analysis results; otherwise load the
    // database-managed snapshot only if it still matches the current table.
    // This never performs a full analysis scan inside EXPLAIN.
    let cached_statistics = if table_statistics.is_none() {
        ananta_execution::load_table_statistics(database, &table)
            .map_err(from_execution_error)?
    } else {
        None
    };
    let statistics_to_use = table_statistics.or(cached_statistics.as_ref());
    let table_statistics = match statistics_to_use {
        Some(statistics)
            if statistics.table_name == table
                && statistics.table_id == database.table_id(&table).map_err(from_storage_error)?.0
                && statistics.schema_revision == database.table_schema_revision(&table).map_err(from_storage_error)?
                && statistics.columns.len() == schema.len()
                && statistics.columns.iter().zip(schema.columns())
                    .all(|(stats, column)| stats.column == *column) =>
        {
            let current_snapshot = database
                .table_scan_snapshot_token(&table)
                .map_err(from_storage_error)?;
            if statistics.snapshot_token != current_snapshot {
                return Err(query_error(
                    AqlErrorCode::InvalidQuery,
                    "EXPLAIN statistics are stale because table data or active Weave changed; rerun analyze_table",
                    select.source.span,
                ));
            }
            Some(statistics)
        }
        Some(_) => {
            return Err(query_error(
                AqlErrorCode::InvalidQuery,
                "EXPLAIN statistics do not match the current table identity/schema",
                select.source.span,
            ));
        }
        None => None,
    };
    let selectivity = predicate.as_ref()
        .map(|expression| estimate_compiled_selectivity(
            expression,
            table_statistics,
            bindings,
        ))
        .unwrap_or(1.0)
        .clamp(0.0, 1.0);
    let estimated_rows_after_filter = table_statistics.map(|statistics| {
        (statistics.scanned_rows as f64 * selectivity)
            .round()
            .min(u64::MAX as f64) as u64
    });
    let estimated_output_rows = estimated_rows_after_filter.map(|rows| {
        select.limit.map_or(rows, |limit| rows.min(limit))
    });
    let projected_fraction = if native_execution && !schema.columns().is_empty() {
        scan_columns.len() as f64 / schema.columns().len() as f64
    } else {
        1.0
    };
    let estimated_projected_payload_bytes = table_statistics.map(|statistics| {
        (statistics.estimated_batch_payload_bytes as f64 * projected_fraction)
            .round()
            .min(u64::MAX as f64) as u64
    });

    let predicate_pushed = native_execution && predicate.is_some();
    let storage_limit_pushed =
        native_execution && predicate.is_none() && select.limit.is_some();
    let mut notes = vec![
        "row cardinality uses analyzed NULL/NDV/min-max statistics when available; estimates remain heuristic without histograms".to_owned(),
        "projected payload bytes assume uniform per-column batch payload; actual bytes can differ".to_owned(),
        "Chronicle row/byte values are accepted-ingestion counters, not an execution profile".to_owned(),
    ];
    if table_statistics.is_none() {
        notes.push("row-count and payload estimates are unknown until explicit table analysis is supplied".to_owned());
    }
    if !native_execution {
        notes.push(
            "query uses the reference evaluator because a projection or predicate is not yet lowerable".to_owned(),
        );
    }
    Ok(AqlPhysicalPlan {
        table,
        native_execution,
        scan_columns,
        predicate_pushed,
        storage_limit_pushed,
        chronicle_accepted_rows: ingestion.accepted_rows,
        chronicle_accepted_encoded_bytes: ingestion.accepted_encoded_bytes,
        estimated_rows_after_filter,
        estimated_output_rows,
        estimated_projected_payload_bytes,
        notes,
    })
}

fn estimate_compiled_selectivity(
    expression: &CompiledExpr,
    statistics: Option<&TableStatistics>,
    bindings: &ParameterBindings,
) -> f64 {
    match &expression.kind {
        CompiledExprKind::Literal(Literal::Boolean(true)) => 1.0,
        CompiledExprKind::Literal(Literal::Boolean(false)) => 0.0,
        CompiledExprKind::IsNull { expression, negated } => {
            if let (Some(statistics), CompiledExprKind::Column(index)) =
                (statistics, &expression.kind)
            {
                if let Some(column) = statistics.columns.get(*index) {
                    if statistics.scanned_rows > 0 {
                        let null_fraction =
                            column.null_count as f64 / statistics.scanned_rows as f64;
                        return if *negated {
                            1.0 - null_fraction
                        } else {
                            null_fraction
                        };
                    }
                }
            }
            if *negated { 0.9 } else { 0.1 }
        }
        CompiledExprKind::Unary {
            operator: UnaryOperator::Not,
            expression,
        } => 1.0 - estimate_compiled_selectivity(expression, statistics, bindings),
        CompiledExprKind::Binary {
            left,
            operator: BinaryOperator::And,
            right,
        } => {
            estimate_compiled_selectivity(left, statistics, bindings)
                * estimate_compiled_selectivity(right, statistics, bindings)
        }
        CompiledExprKind::Binary {
            left,
            operator: BinaryOperator::Or,
            right,
        } => {
            let left = estimate_compiled_selectivity(left, statistics, bindings);
            let right = estimate_compiled_selectivity(right, statistics, bindings);
            left + right - left * right
        }
        CompiledExprKind::Binary {
            left,
            operator,
            right,
        } => {
            if let Some(estimate) =
                estimate_column_comparison(left, *operator, right, statistics, bindings)
            {
                return estimate;
            }
            match operator {
                BinaryOperator::Equal => 0.1,
                BinaryOperator::NotEqual => 0.9,
                BinaryOperator::Less
                | BinaryOperator::LessEqual
                | BinaryOperator::Greater
                | BinaryOperator::GreaterEqual => 0.33,
                _ => 0.5,
            }
        }
        CompiledExprKind::Column(_) => 0.5,
        CompiledExprKind::Parameter(_) => 0.5,
        _ => 0.5,
    }
}

fn estimate_column_comparison(
    left: &CompiledExpr,
    operator: BinaryOperator,
    right: &CompiledExpr,
    statistics: Option<&TableStatistics>,
    bindings: &ParameterBindings,
) -> Option<f64> {
    let statistics = statistics?;
    let (column_index, value, operator) = match (
        &left.kind,
        compiled_scalar_value(right, bindings),
    ) {
        (CompiledExprKind::Column(index), Some(value)) => (*index, value, operator),
        _ => match (
            &right.kind,
            compiled_scalar_value(left, bindings),
        ) {
            (CompiledExprKind::Column(index), Some(value)) => {
                (*index, value, reverse_comparison(operator)?)
            }
            _ => return None,
        },
    };
    let column = statistics.columns.get(column_index)?;
    if statistics.scanned_rows == 0 || matches!(value, Value::Null) {
        return Some(0.0);
    }

    let total = statistics.scanned_rows as f64;
    let non_null_fraction = column.non_null_count as f64 / total;
    if non_null_fraction <= 0.0 {
        return Some(0.0);
    }

    if matches!(operator, BinaryOperator::Equal | BinaryOperator::NotEqual) {
        let distinct = column.approximate_distinct_count.max(1) as f64;
        let equal = (non_null_fraction / distinct).min(non_null_fraction);
        return Some(if operator == BinaryOperator::Equal {
            equal
        } else {
            (non_null_fraction - equal).max(0.0)
        });
    }

    if column.histogram_sample_count > 0 && !column.histogram.is_empty() {
        let less_than = histogram_less_than(column, &value, false)?;
        let less_equal = histogram_less_than(column, &value, true)?;
        let fraction = match operator {
            BinaryOperator::Less => less_than,
            BinaryOperator::LessEqual => less_equal,
            BinaryOperator::Greater => 1.0 - less_equal,
            BinaryOperator::GreaterEqual => 1.0 - less_than,
            _ => return None,
        };
        return Some(non_null_fraction * fraction.clamp(0.0, 1.0));
    }

    let minimum = column.min_value.as_ref()?;
    let maximum = column.max_value.as_ref()?;
    let min_numeric = statistics_numeric_value(minimum)?;
    let max_numeric = statistics_numeric_value(maximum)?;
    let target = statistics_numeric_value(&value)?;
    if max_numeric <= min_numeric {
        let ordering = compare_statistics_values(&value, minimum)?;
        let matches = match operator {
            BinaryOperator::Less => ordering == Ordering::Greater,
            BinaryOperator::LessEqual => ordering != Ordering::Less,
            BinaryOperator::Greater => ordering == Ordering::Less,
            BinaryOperator::GreaterEqual => ordering != Ordering::Greater,
            _ => return None,
        };
        return Some(if matches { non_null_fraction } else { 0.0 });
    }

    let below = ((target - min_numeric) / (max_numeric - min_numeric)).clamp(0.0, 1.0);
    // Min/max provide a uniform-range estimate. Unlike equality predicates,
    // a range histogram is not available, so do not add an invented point mass
    // at inclusive bounds; that would systematically overcount boundary rows.
    let fraction = match operator {
        BinaryOperator::Less | BinaryOperator::LessEqual => below,
        BinaryOperator::Greater | BinaryOperator::GreaterEqual => 1.0 - below,
        _ => return None,
    };
    Some(non_null_fraction * fraction)
}

fn histogram_less_than(
    column: &ColumnStatistics,
    target: &Value,
    inclusive: bool,
) -> Option<f64> {
    let sample_total = column.histogram_sample_count as f64;
    if sample_total <= 0.0 {
        return None;
    }
    let mut matched = 0.0_f64;
    for bucket in &column.histogram {
        let lower_order = compare_statistics_values(target, &bucket.lower_bound)?;
        let upper_order = compare_statistics_values(target, &bucket.upper_bound)?;
        let bucket_count = bucket.sample_count as f64;

        if upper_order == Ordering::Greater {
            matched += bucket_count;
            continue;
        }
        if upper_order == Ordering::Equal {
            if inclusive {
                matched += bucket_count;
            } else if lower_order == Ordering::Greater {
                let equality_mass = (bucket_count
                    / column.approximate_distinct_count.max(1) as f64)
                    .min(bucket_count);
                matched += (bucket_count - equality_mass).max(0.0);
            }
            break;
        }
        if lower_order == Ordering::Less {
            break;
        }
        if lower_order == Ordering::Equal {
            if inclusive {
                let equality_mass = if compare_statistics_values(
                    &bucket.lower_bound,
                    &bucket.upper_bound,
                )? == Ordering::Equal {
                    bucket_count
                } else {
                    (bucket_count
                        / column.approximate_distinct_count.max(1) as f64)
                        .min(bucket_count)
                };
                matched += equality_mass;
            }
            break;
        }

        let fraction = match (
            statistics_numeric_value(&bucket.lower_bound),
            statistics_numeric_value(&bucket.upper_bound),
            statistics_numeric_value(target),
        ) {
            (Some(lower), Some(upper), Some(value)) if upper > lower => {
                ((value - lower) / (upper - lower)).clamp(0.0, 1.0)
            }
            _ => 0.5,
        };
        matched += bucket_count * fraction;
        break;
    }
    Some((matched / sample_total).clamp(0.0, 1.0))
}

fn compiled_scalar_value(
    expression: &CompiledExpr,
    bindings: &ParameterBindings,
) -> Option<Value> {
    match &expression.kind {
        CompiledExprKind::Literal(literal) => Some(literal_value(literal)),
        CompiledExprKind::Parameter(parameter) => {
            binding_value(parameter, bindings, expression.span).ok().cloned()
        }
        _ => None,
    }
}

fn reverse_comparison(operator: BinaryOperator) -> Option<BinaryOperator> {
    Some(match operator {
        BinaryOperator::Less => BinaryOperator::Greater,
        BinaryOperator::LessEqual => BinaryOperator::GreaterEqual,
        BinaryOperator::Greater => BinaryOperator::Less,
        BinaryOperator::GreaterEqual => BinaryOperator::LessEqual,
        BinaryOperator::Equal => BinaryOperator::Equal,
        BinaryOperator::NotEqual => BinaryOperator::NotEqual,
        _ => return None,
    })
}

fn statistics_numeric_value(value: &Value) -> Option<f64> {
    match value {
        Value::Int32(value) => Some(f64::from(*value)),
        Value::Int64(value) => Some(*value as f64),
        Value::TimestampMicros(value) => Some(*value as f64),
        Value::Float64(value) if value.is_finite() => Some(*value),
        _ => None,
    }
}

fn compare_statistics_values(left: &Value, right: &Value) -> Option<Ordering> {
    Some(match (left, right) {
        (Value::Boolean(left), Value::Boolean(right)) => left.cmp(right),
        (Value::Int32(left), Value::Int32(right)) => left.cmp(right),
        (Value::Int64(left), Value::Int64(right)) => left.cmp(right),
        (Value::TimestampMicros(left), Value::TimestampMicros(right)) => left.cmp(right),
        (Value::Float64(left), Value::Float64(right)) => left.total_cmp(right),
        (Value::Utf8(left), Value::Utf8(right)) => left.cmp(right),
        (Value::Binary(left), Value::Binary(right)) => left.cmp(right),
        _ => return None,
    })
}

pub fn execute_source(
    database: &Database,
    source: &str,
    bindings: &ParameterBindings,
    batch_rows: usize,
    cancellation: Option<Arc<AtomicBool>>,
) -> Result<AqlResultIter, AqlExecutionError> {
    let query = crate::parse(source).map_err(from_parse_error)?;
    execute(database, &query, bindings, batch_rows, cancellation)
}

impl Iterator for AqlResultIter {
    type Item = Result<AqlResultBatch, AqlExecutionError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }

        let cancelled = self.is_cancelled();
        if self.native.is_some() {
            let (next_batch, scan_metrics) = {
                let native = self.native.as_mut().expect("native execution checked above");
                if cancelled {
                    native.context.cancel();
                }
                let next_batch = native.operator.next_batch(&mut native.context);
                let metrics = native.context.metrics().clone();
                (next_batch, metrics)
            };
            self.metrics.scan_input_batches = scan_metrics.scan_input_batches;
            self.metrics.scan_input_rows = scan_metrics.scan_input_rows;
            self.metrics.scan_input_value_payload_bytes =
                scan_metrics.scan_input_value_payload_bytes;
            return match next_batch {
                Ok(Some(batch)) => {
                    let result = AqlResultBatch {
                        schema: batch.columns.iter().map(|column| column.column.clone()).collect(),
                        columns: batch.columns.into_iter().map(|column| column.values).collect(),
                        row_count: batch.row_count,
                    };
                    self.record_metrics(&result);
                    Some(Ok(result))
                }
                Ok(None) => {
                    self.finished = true;
                    None
                }
                Err(error) => {
                    self.finished = true;
                    Some(Err(from_execution_error(error)))
                }
            };
        }

        if self.remaining_limit == Some(0) {
            self.finished = true;
            return None;
        }
        loop {
            if self.is_cancelled() {
                self.finished = true;
                return Some(Err(query_error(
                    AqlErrorCode::Cancelled,
                    "AQL query was cancelled",
                    crate::Span::default(),
                )));
            }
            let next_input = match self.input.as_mut() {
                Some(input) => input.next(),
                None => {
                    self.finished = true;
                    return None;
                }
            };
            let input = match next_input {
                Some(Ok(batch)) => batch,
                Some(Err(error)) => {
                    self.finished = true;
                    return Some(Err(from_storage_error(error)));
                }
                None => {
                    self.finished = true;
                    return None;
                }
            };
            let capacity = self.remaining_limit
                .map_or(input.row_count, |limit| input.row_count.min(usize::try_from(limit).unwrap_or(usize::MAX)));
            let mut output_columns = vec![Vec::with_capacity(capacity); self.projection.len()];
            for row_index in 0..input.row_count {
                if self.is_cancelled() {
                    self.finished = true;
                    return Some(Err(query_error(
                        AqlErrorCode::Cancelled,
                        "AQL query was cancelled",
                        crate::Span::default(),
                    )));
                }
                if self.remaining_limit == Some(0) {
                    self.finished = true;
                    break;
                }
                if let Some(predicate) = &self.predicate {
                    match evaluate(predicate, &input, row_index, &self.bindings) {
                        Ok(Value::Boolean(true)) => {}
                        Ok(Value::Boolean(false) | Value::Null) => continue,
                        Ok(_) => {
                            self.finished = true;
                            return Some(Err(query_error(
                                AqlErrorCode::TypeMismatch,
                                "WHERE expression did not evaluate to BOOLEAN",
                                predicate.span,
                            )));
                        }
                        Err(error) => {
                            self.finished = true;
                            return Some(Err(error));
                        }
                    }
                }
                for (index, selected) in self.projection.iter().enumerate() {
                    match evaluate(&selected.expression, &input, row_index, &self.bindings) {
                        Ok(value) => output_columns[index].push(value),
                        Err(error) => {
                            self.finished = true;
                            return Some(Err(error));
                        }
                    }
                }
                if let Some(limit) = &mut self.remaining_limit {
                    *limit -= 1;
                }
            }
            let row_count = output_columns.first().map_or(0, Vec::len);
            if row_count == 0 {
                if self.finished || self.remaining_limit == Some(0) {
                    self.finished = true;
                    return None;
                }
                continue;
            }
            let result = AqlResultBatch {
                schema: self.projection.iter().map(|item| item.column.clone()).collect(),
                columns: output_columns,
                row_count,
            };
            self.record_metrics(&result);
            return Some(Ok(result));
        }
    }
}

impl AqlResultIter {
    pub fn is_cancelled(&self) -> bool {
        self.cancellation.as_ref().is_some_and(|flag| flag.load(AtomicOrdering::Acquire))
    }

    pub fn used_native_execution(&self) -> bool {
        self.native.is_some()
    }

    pub fn metrics(&self) -> &AqlExecutionMetrics {
        &self.metrics
    }

    fn record_metrics(&mut self, batch: &AqlResultBatch) {
        self.metrics.output_batches = self.metrics.output_batches.saturating_add(1);
        self.metrics.output_rows = self.metrics.output_rows
            .saturating_add(u64::try_from(batch.row_count).unwrap_or(u64::MAX));
        let cells = batch.columns.iter()
            .map(|column| u64::try_from(column.len()).unwrap_or(u64::MAX))
            .fold(0_u64, u64::saturating_add);
        self.metrics.output_cells = self.metrics.output_cells.saturating_add(cells);
        self.metrics.output_value_payload_bytes = self.metrics.output_value_payload_bytes
            .saturating_add(measured_value_payload_bytes(batch));
        self.metrics.output_bytes = self.metrics.output_bytes
            .saturating_add(estimate_result_batch_bytes(batch));
    }
}


fn compile_expr(
    expression: &Expr,
    schema: &Schema,
    table: &str,
    bindings: &ParameterBindings,
) -> Result<CompiledExpr, AqlExecutionError> {
    let span = expression.span;
    let (kind, data_type, nullable) = match &expression.kind {
        ExprKind::Column(reference) => {
            let index = resolve_column(reference, schema, table)?;
            let column = &schema.columns()[index];
            (CompiledExprKind::Column(index), Some(column.data_type), column.nullable)
        }
        ExprKind::Literal(literal) => {
            let kind = match literal {
                Literal::Null => None,
                Literal::Boolean(_) => Some(DataType::Boolean),
                Literal::Int64(_) => Some(DataType::Int64),
                Literal::Float64(_) => Some(DataType::Float64),
                Literal::Utf8(_) => Some(DataType::Utf8),
            };
            (CompiledExprKind::Literal(literal.clone()), kind, matches!(literal, Literal::Null))
        }
        ExprKind::Parameter(parameter) => {
            let value = binding_value(parameter, bindings, span)?;
            (CompiledExprKind::Parameter(parameter.clone()), value.data_type(), matches!(value, Value::Null))
        }
        ExprKind::Unary { operator, expression: inner } => {
            let inner = compile_expr(inner, schema, table, bindings)?;
            match operator {
                UnaryOperator::Not => require_type(inner.data_type, DataType::Boolean, inner.span, "NOT requires BOOLEAN")?,
                UnaryOperator::Positive | UnaryOperator::Negative => {
                    if inner.data_type.is_some_and(|kind| !is_numeric(kind)) {
                        return Err(query_error(AqlErrorCode::TypeMismatch,
                            "unary numeric sign requires INT32, INT64 or FLOAT64", inner.span));
                    }
                }
            }
            (CompiledExprKind::Unary { operator: *operator, expression: Box::new(inner.clone()) },
                if *operator == UnaryOperator::Not { Some(DataType::Boolean) } else { inner.data_type },
                inner.nullable)
        }
        ExprKind::Binary { left, operator, right } => {
            let left = compile_expr(left, schema, table, bindings)?;
            let right = compile_expr(right, schema, table, bindings)?;
            let result_type = match operator {
                BinaryOperator::And | BinaryOperator::Or => {
                    require_type(left.data_type, DataType::Boolean, left.span, "AND/OR requires BOOLEAN")?;
                    require_type(right.data_type, DataType::Boolean, right.span, "AND/OR requires BOOLEAN")?;
                    Some(DataType::Boolean)
                }
                BinaryOperator::Equal | BinaryOperator::NotEqual | BinaryOperator::Less
                | BinaryOperator::LessEqual | BinaryOperator::Greater | BinaryOperator::GreaterEqual => {
                    if let (Some(a), Some(b)) = (left.data_type, right.data_type) {
                        if a != b {
                            return Err(query_error(AqlErrorCode::TypeMismatch,
                                "comparison operands must have the same type; use CAST explicitly", span));
                        }
                        if a == DataType::Boolean && !matches!(operator, BinaryOperator::Equal | BinaryOperator::NotEqual) {
                            return Err(query_error(AqlErrorCode::TypeMismatch,
                                "BOOLEAN supports equality and inequality comparisons only", span));
                        }
                    }
                    Some(DataType::Boolean)
                }
                BinaryOperator::Add | BinaryOperator::Subtract | BinaryOperator::Multiply
                | BinaryOperator::Divide | BinaryOperator::Modulo => {
                    let kind = match (left.data_type, right.data_type) {
                        (Some(a), Some(b)) if a == b && is_numeric(a) => Some(a),
                        (Some(a), None) if is_numeric(a) => Some(a),
                        (None, Some(b)) if is_numeric(b) => Some(b),
                        (None, None) => None,
                        _ => return Err(query_error(AqlErrorCode::TypeMismatch,
                            "arithmetic requires operands of the same numeric type", span)),
                    };
                    kind
                }
            };
            let nullable = left.nullable || right.nullable;
            (CompiledExprKind::Binary { left: Box::new(left), operator: *operator, right: Box::new(right) },
                result_type, nullable)
        }
        ExprKind::IsNull { expression: inner, negated } => {
            let inner = compile_expr(inner, schema, table, bindings)?;
            (CompiledExprKind::IsNull { expression: Box::new(inner), negated: *negated },
                Some(DataType::Boolean), false)
        }
        ExprKind::Cast { expression: inner, target } => {
            let inner = compile_expr(inner, schema, table, bindings)?;
            if let Some(source) = inner.data_type {
                if !cast_supported(source, *target) {
                    return Err(query_error(AqlErrorCode::InvalidCast,
                        "CAST conversion is not supported by AQL v0.1", span));
                }
            }
            (CompiledExprKind::Cast { expression: Box::new(inner.clone()), target: *target },
                Some(*target), inner.nullable)
        }
    };
    Ok(CompiledExpr { kind, data_type, nullable, span })
}

fn resolve_table_name(database: &Database, identifier: &Identifier) -> Result<String, AqlExecutionError> {
    let found = database.table_names().find(|name| {
        if identifier.quoted { *name == identifier.value }
        else { name.eq_ignore_ascii_case(&identifier.value) }
    });
    found.map(str::to_owned).ok_or_else(|| query_error(AqlErrorCode::TableNotFound,
        format!("table '{}' does not exist", identifier.value), identifier.span))
}

fn resolve_column(reference: &ColumnReference, schema: &Schema, table: &str) -> Result<usize, AqlExecutionError> {
    if reference.parts.len() == 2 {
        let qualifier = &reference.parts[0];
        let matches = if qualifier.quoted { qualifier.value == table }
            else { qualifier.value.eq_ignore_ascii_case(table) };
        if !matches {
            return Err(query_error(AqlErrorCode::ColumnNotFound,
                format!("qualifier '{}' does not match source table '{table}'", qualifier.value),
                qualifier.span));
        }
    }
    let identifier = reference.parts.last().expect("column reference has at least one identifier");
    schema.columns().iter().position(|column| {
        if identifier.quoted { column.name == identifier.value }
        else { column.name.eq_ignore_ascii_case(&identifier.value) }
    }).ok_or_else(|| query_error(AqlErrorCode::ColumnNotFound,
        format!("column '{}' does not exist in table '{table}'", identifier.value), identifier.span))
}

fn evaluate(
    expression: &CompiledExpr,
    batch: &ananta_core::RecordBatch,
    row_index: usize,
    bindings: &ParameterBindings,
) -> Result<Value, AqlExecutionError> {
    match &expression.kind {
        CompiledExprKind::Column(index) => batch.columns.get(*index)
            .and_then(|column| column.values.get(row_index)).cloned()
            .ok_or_else(|| query_error(AqlErrorCode::Storage, "scan batch did not contain a referenced column", expression.span)),
        CompiledExprKind::Literal(literal) => Ok(literal_value(literal)),
        CompiledExprKind::Parameter(parameter) => Ok(binding_value(parameter, bindings, expression.span)?.clone()),
        CompiledExprKind::Unary { operator, expression: inner } => {
            let value = evaluate(inner, batch, row_index, bindings)?;
            match operator {
                UnaryOperator::Not => match value {
                    Value::Null => Ok(Value::Null),
                    Value::Boolean(value) => Ok(Value::Boolean(!value)),
                    _ => Err(query_error(AqlErrorCode::TypeMismatch, "NOT requires BOOLEAN", expression.span)),
                },
                UnaryOperator::Positive => Ok(value),
                UnaryOperator::Negative => negate(value, expression.span),
            }
        }
        CompiledExprKind::IsNull { expression: inner, negated } => {
            let is_null = matches!(evaluate(inner, batch, row_index, bindings)?, Value::Null);
            Ok(Value::Boolean(if *negated { !is_null } else { is_null }))
        }
        CompiledExprKind::Cast { expression: inner, target } => {
            cast_value(evaluate(inner, batch, row_index, bindings)?, *target, expression.span)
        }
        CompiledExprKind::Binary { left, operator, right } => {
            let left = evaluate(left, batch, row_index, bindings)?;
            let right = evaluate(right, batch, row_index, bindings)?;
            evaluate_binary(left, *operator, right, expression.span)
        }
    }
}

fn evaluate_binary(
    left: Value,
    operator: BinaryOperator,
    right: Value,
    span: crate::Span,
) -> Result<Value, AqlExecutionError> {
    use BinaryOperator::*;
    if matches!(operator, And | Or) {
        let a = as_truth(left, span)?;
        let b = as_truth(right, span)?;
        return Ok(truth_value(match operator {
            And => match (a, b) {
                (Truth::False, _) | (_, Truth::False) => Truth::False,
                (Truth::True, Truth::True) => Truth::True,
                _ => Truth::Unknown,
            },
            Or => match (a, b) {
                (Truth::True, _) | (_, Truth::True) => Truth::True,
                (Truth::False, Truth::False) => Truth::False,
                _ => Truth::Unknown,
            },
            _ => unreachable!(),
        }));
    }
    if matches!(operator, Equal | NotEqual | Less | LessEqual | Greater | GreaterEqual) {
        if matches!(&left, Value::Null) || matches!(&right, Value::Null) { return Ok(Value::Null); }
        let ordering = compare_values(&left, &right, span)?;
        let result = match operator {
            Equal => ordering == Ordering::Equal,
            NotEqual => ordering != Ordering::Equal,
            Less => ordering == Ordering::Less,
            LessEqual => ordering != Ordering::Greater,
            Greater => ordering == Ordering::Greater,
            GreaterEqual => ordering != Ordering::Less,
            _ => unreachable!(),
        };
        return Ok(Value::Boolean(result));
    }
    if matches!(left, Value::Null) || matches!(right, Value::Null) { return Ok(Value::Null); }
    arithmetic(left, operator, right, span)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Truth { True, False, Unknown }
fn as_truth(value: Value, span: crate::Span) -> Result<Truth, AqlExecutionError> {
    match value {
        Value::Boolean(true) => Ok(Truth::True),
        Value::Boolean(false) => Ok(Truth::False),
        Value::Null => Ok(Truth::Unknown),
        _ => Err(query_error(AqlErrorCode::TypeMismatch, "boolean operator requires BOOLEAN", span)),
    }
}
fn truth_value(value: Truth) -> Value {
    match value { Truth::True => Value::Boolean(true), Truth::False => Value::Boolean(false), Truth::Unknown => Value::Null }
}

fn compare_values(left: &Value, right: &Value, span: crate::Span) -> Result<Ordering, AqlExecutionError> {
    let ordering = match (left, right) {
        (Value::Boolean(a), Value::Boolean(b)) => a.cmp(b),
        (Value::Int32(a), Value::Int32(b)) => a.cmp(b),
        (Value::Int64(a), Value::Int64(b)) => a.cmp(b),
        (Value::Float64(a), Value::Float64(b)) => a.total_cmp(b),
        (Value::Utf8(a), Value::Utf8(b)) => a.cmp(b),
        (Value::TimestampMicros(a), Value::TimestampMicros(b)) => a.cmp(b),
        (Value::Binary(a), Value::Binary(b)) => a.cmp(b),
        _ => return Err(query_error(AqlErrorCode::TypeMismatch,
            "comparison operands must have identical types", span)),
    };
    Ok(ordering)
}

fn arithmetic(left: Value, operator: BinaryOperator, right: Value, span: crate::Span) -> Result<Value, AqlExecutionError> {
    use BinaryOperator::*;
    let value = match (left, right) {
        (Value::Int32(a), Value::Int32(b)) => {
            let result = match operator {
                Add => a.checked_add(b), Subtract => a.checked_sub(b), Multiply => a.checked_mul(b),
                Divide => a.checked_div(b), Modulo => a.checked_rem(b),
                _ => None,
            };
            Value::Int32(result.ok_or_else(|| arithmetic_error(span, "INT32 overflow or division by zero"))?)
        }
        (Value::Int64(a), Value::Int64(b)) => {
            let result = match operator {
                Add => a.checked_add(b), Subtract => a.checked_sub(b), Multiply => a.checked_mul(b),
                Divide => a.checked_div(b), Modulo => a.checked_rem(b),
                _ => None,
            };
            Value::Int64(result.ok_or_else(|| arithmetic_error(span, "INT64 overflow or division by zero"))?)
        }
        (Value::Float64(a), Value::Float64(b)) => {
            let result = match operator {
                Add => a + b, Subtract => a - b, Multiply => a * b,
                Divide if b != 0.0 => a / b,
                Modulo if b != 0.0 => a % b,
                Divide | Modulo => return Err(arithmetic_error(span, "FLOAT64 division or modulo by zero")),
                _ => return Err(arithmetic_error(span, "invalid FLOAT64 arithmetic operator")),
            };
            if !result.is_finite() { return Err(arithmetic_error(span, "FLOAT64 arithmetic produced a non-finite result")); }
            Value::Float64(result)
        }
        _ => return Err(query_error(AqlErrorCode::TypeMismatch,
            "arithmetic requires operands of the same numeric type", span)),
    };
    Ok(value)
}

fn negate(value: Value, span: crate::Span) -> Result<Value, AqlExecutionError> {
    match value {
        Value::Null => Ok(Value::Null),
        Value::Int32(value) => value.checked_neg().map(Value::Int32)
            .ok_or_else(|| arithmetic_error(span, "INT32 negation overflow")),
        Value::Int64(value) => value.checked_neg().map(Value::Int64)
            .ok_or_else(|| arithmetic_error(span, "INT64 negation overflow")),
        Value::Float64(value) => {
            let result = -value;
            if result.is_finite() { Ok(Value::Float64(result)) }
            else { Err(arithmetic_error(span, "FLOAT64 negation produced a non-finite result")) }
        }
        _ => Err(query_error(AqlErrorCode::TypeMismatch, "unary sign requires a numeric value", span)),
    }
}

fn cast_value(value: Value, target: DataType, span: crate::Span) -> Result<Value, AqlExecutionError> {
    if matches!(&value, Value::Null) { return Ok(Value::Null); }
    let result = match (value, target) {
        (value, target) if value.data_type() == Some(target) => value,
        (Value::Int32(value), DataType::Int64) => Value::Int64(i64::from(value)),
        (Value::Int32(value), DataType::Float64) => Value::Float64(f64::from(value)),
        (Value::Int64(value), DataType::Int32) => Value::Int32(i32::try_from(value)
            .map_err(|_| query_error(AqlErrorCode::InvalidCast, "INT64 value is outside INT32 range", span))?),
        (Value::Int64(value), DataType::Float64) => {
            let float = value as f64;
            if float as i128 != value as i128 {
                return Err(query_error(AqlErrorCode::InvalidCast, "INT64 value is not exactly representable as FLOAT64", span));
            }
            Value::Float64(float)
        }
        (Value::Float64(value), DataType::Int32) => {
            let number = exact_float_to_i64(value, span)?;
            Value::Int32(i32::try_from(number)
                .map_err(|_| query_error(AqlErrorCode::InvalidCast, "FLOAT64 value is outside INT32 range", span))?)
        }
        (Value::Float64(value), DataType::Int64) => Value::Int64(exact_float_to_i64(value, span)?),
        _ => return Err(query_error(AqlErrorCode::InvalidCast, "CAST conversion is not supported by AQL v0.1", span)),
    };
    Ok(result)
}
fn exact_float_to_i64(value: f64, span: crate::Span) -> Result<i64, AqlExecutionError> {
    if !value.is_finite() || value.fract() != 0.0
        || value < i64::MIN as f64 || value >= -(i64::MIN as f64)
    {
        return Err(query_error(AqlErrorCode::InvalidCast,
            "FLOAT64 must be finite, integral and in INT64 range", span));
    }
    let integer = value as i64;
    if integer as f64 != value {
        return Err(query_error(AqlErrorCode::InvalidCast, "FLOAT64 value is not exactly representable as INT64", span));
    }
    Ok(integer)
}

fn literal_value(literal: &Literal) -> Value {
    match literal {
        Literal::Null => Value::Null,
        Literal::Boolean(value) => Value::Boolean(*value),
        Literal::Int64(value) => Value::Int64(*value),
        Literal::Float64(value) => Value::Float64(*value),
        Literal::Utf8(value) => Value::Utf8(value.clone()),
    }
}
fn binding_value<'a>(
    parameter: &ParameterRef,
    bindings: &'a ParameterBindings,
    span: crate::Span,
) -> Result<&'a Value, AqlExecutionError> {
    match parameter {
        ParameterRef::Named(name) => bindings.named.get(name).ok_or_else(|| query_error(
            AqlErrorCode::InvalidParameter, format!("missing binding for :{name}"), span)),
        ParameterRef::Positional(index) => {
            let position = index.checked_sub(1).ok_or_else(|| query_error(
                AqlErrorCode::InvalidParameter, "positional parameters are one-based", span))?;
            bindings.positional.get(position as usize).ok_or_else(|| query_error(
                AqlErrorCode::InvalidParameter, "missing positional parameter binding", span))
        }
    }
}
fn require_type(
    actual: Option<DataType>, expected: DataType, span: crate::Span, message: &str,
) -> Result<(), AqlExecutionError> {
    if actual.is_some_and(|kind| kind != expected) {
        Err(query_error(AqlErrorCode::TypeMismatch, message, span))
    } else { Ok(()) }
}
fn is_numeric(kind: DataType) -> bool {
    matches!(kind, DataType::Int32 | DataType::Int64 | DataType::Float64)
}
fn cast_supported(source: DataType, target: DataType) -> bool {
    source == target || matches!((source, target),
        (DataType::Int32, DataType::Int64 | DataType::Float64)
        | (DataType::Int64, DataType::Int32 | DataType::Float64)
        | (DataType::Float64, DataType::Int32 | DataType::Int64))
}
fn arithmetic_error(span: crate::Span, message: &str) -> AqlExecutionError {
    query_error(AqlErrorCode::Arithmetic, message, span)
}
fn try_build_native_pipeline(
    database: &Database,
    table: &str,
    schema: &Schema,
    projection: &[CompiledProjection],
    predicate: Option<&CompiledExpr>,
    bindings: &ParameterBindings,
    limit: Option<u64>,
    batch_rows: usize,
    cancellation: Option<Arc<AtomicBool>>,
) -> Result<Option<NativeAqlExecution>, AqlExecutionError> {
    let native_predicate = match predicate {
        Some(expression) => match compiled_filter_predicate(expression, bindings) {
            Some(predicate) => Some(predicate),
            None => return Ok(None),
        },
        None => None,
    };

    let mut required_columns = BTreeSet::new();
    for item in projection {
        let CompiledExprKind::Column(index) = &item.expression.kind else {
            return Ok(None);
        };
        required_columns.insert(*index);
    }
    if let Some(predicate) = &native_predicate {
        collect_filter_columns(predicate, &mut required_columns);
    }
    if required_columns.is_empty() {
        return Ok(None);
    }
    let scan_columns = required_columns.iter().copied().collect::<Vec<_>>();
    let scan_mapping = scan_columns
        .iter()
        .enumerate()
        .map(|(scan_index, logical_index)| (*logical_index, scan_index))
        .collect::<BTreeMap<_, _>>();

    let pushed_scan_limit = if native_predicate.is_none() { limit } else { None };
    let scan = ScanOperator::new_projected_with_limit(
        database,
        table,
        &scan_columns,
        batch_rows,
        pushed_scan_limit,
    )
    .map_err(from_execution_error)?;
    let mut operator: Box<dyn ExecutionOperator> = Box::new(scan);

    if let Some(predicate) = native_predicate {
        let predicate = remap_filter_columns(predicate, &scan_mapping)?;
        operator = Box::new(
            FilterOperator::new(operator, predicate).map_err(from_execution_error)?,
        );
    }

    let fields = projection
        .iter()
        .map(|item| {
            let CompiledExprKind::Column(index) = &item.expression.kind else {
                return Err(query_error(
                    AqlErrorCode::Execution,
                    "native projection contains a computed expression",
                    item.expression.span,
                ));
            };
            let scan_index = scan_mapping.get(index).copied().ok_or_else(|| {
                query_error(
                    AqlErrorCode::Execution,
                    "native projection column was not included in the scan",
                    item.expression.span,
                )
            })?;
            Ok(ProjectionField::new(scan_index, item.column.clone()))
        })
        .collect::<Result<Vec<_>, AqlExecutionError>>()?;
    operator = Box::new(
        ProjectionOperator::new(operator, fields).map_err(from_execution_error)?,
    );
    if let Some(limit) = limit {
        operator = Box::new(LimitOperator::new(operator, limit));
    }

    let cancellation = cancellation.unwrap_or_else(|| Arc::new(AtomicBool::new(false)));
    let context = ExecutionContext::with_cancellation(Default::default(), cancellation)
        .map_err(from_execution_error)?;
    let _ = schema; // schema was used to compile and validate every logical column reference.
    Ok(Some(NativeAqlExecution { operator, context }))
}

fn compiled_filter_predicate(
    expression: &CompiledExpr,
    bindings: &ParameterBindings,
) -> Option<FilterPredicate> {
    match &expression.kind {
        CompiledExprKind::Literal(Literal::Boolean(value)) => {
            Some(FilterPredicate::Constant(*value))
        }
        CompiledExprKind::Parameter(parameter) => {
            match binding_value(parameter, bindings, expression.span).ok()? {
                Value::Boolean(value) => Some(FilterPredicate::Constant(*value)),
                _ => None,
            }
        }
        CompiledExprKind::Column(index)
            if expression.data_type == Some(DataType::Boolean) =>
        {
            Some(FilterPredicate::Compare {
                left: Operand::Column(*index),
                operator: NativeComparison::Equal,
                right: Operand::Literal(Value::Boolean(true)),
            })
        }
        CompiledExprKind::Unary {
            operator: UnaryOperator::Not,
            expression,
        } => Some(FilterPredicate::Not(Box::new(
            compiled_filter_predicate(expression, bindings)?,
        ))),
        CompiledExprKind::IsNull {
            expression,
            negated,
        } => Some(FilterPredicate::IsNull {
            operand: compiled_operand(expression, bindings)?,
            negated: *negated,
        }),
        CompiledExprKind::Binary {
            left,
            operator: BinaryOperator::And,
            right,
        } => Some(FilterPredicate::And(
            Box::new(compiled_filter_predicate(left, bindings)?),
            Box::new(compiled_filter_predicate(right, bindings)?),
        )),
        CompiledExprKind::Binary {
            left,
            operator: BinaryOperator::Or,
            right,
        } => Some(FilterPredicate::Or(
            Box::new(compiled_filter_predicate(left, bindings)?),
            Box::new(compiled_filter_predicate(right, bindings)?),
        )),
        CompiledExprKind::Binary {
            left,
            operator,
            right,
        } => {
            let operator = match operator {
                BinaryOperator::Equal => NativeComparison::Equal,
                BinaryOperator::NotEqual => NativeComparison::NotEqual,
                BinaryOperator::Less => NativeComparison::LessThan,
                BinaryOperator::LessEqual => NativeComparison::LessThanOrEqual,
                BinaryOperator::Greater => NativeComparison::GreaterThan,
                BinaryOperator::GreaterEqual => NativeComparison::GreaterThanOrEqual,
                _ => return None,
            };
            Some(FilterPredicate::Compare {
                left: compiled_operand(left, bindings)?,
                operator,
                right: compiled_operand(right, bindings)?,
            })
        }
        _ => None,
    }
}

fn compiled_operand(
    expression: &CompiledExpr,
    bindings: &ParameterBindings,
) -> Option<Operand> {
    match &expression.kind {
        CompiledExprKind::Column(index) => Some(Operand::Column(*index)),
        CompiledExprKind::Literal(literal) => {
            Some(Operand::Literal(literal_value(literal)))
        }
        CompiledExprKind::Parameter(parameter) => binding_value(
            parameter,
            bindings,
            expression.span,
        )
        .ok()
        .cloned()
        .map(Operand::Literal),
        _ => None,
    }
}

fn collect_filter_columns(
    predicate: &FilterPredicate,
    output: &mut BTreeSet<usize>,
) {
    match predicate {
        FilterPredicate::Constant(_) => {}
        FilterPredicate::Compare { left, right, .. } => {
            collect_operand_column(left, output);
            collect_operand_column(right, output);
        }
        FilterPredicate::IsNull { operand, .. } => {
            collect_operand_column(operand, output);
        }
        FilterPredicate::And(left, right) | FilterPredicate::Or(left, right) => {
            collect_filter_columns(left, output);
            collect_filter_columns(right, output);
        }
        FilterPredicate::Not(inner) => collect_filter_columns(inner, output),
    }
}

fn collect_operand_column(operand: &Operand, output: &mut BTreeSet<usize>) {
    if let Operand::Column(index) = operand {
        output.insert(*index);
    }
}

fn remap_filter_columns(
    predicate: FilterPredicate,
    mapping: &BTreeMap<usize, usize>,
) -> Result<FilterPredicate, AqlExecutionError> {
    let remap_operand = |operand: Operand| -> Result<Operand, AqlExecutionError> {
        match operand {
            Operand::Column(index) => mapping
                .get(&index)
                .copied()
                .map(Operand::Column)
                .ok_or_else(|| query_error(
                    AqlErrorCode::Execution,
                    format!("predicate column {index} was not included in the physical scan"),
                    crate::Span::default(),
                )),
            literal => Ok(literal),
        }
    };
    Ok(match predicate {
        FilterPredicate::Constant(value) => FilterPredicate::Constant(value),
        FilterPredicate::Compare { left, operator, right } => FilterPredicate::Compare {
            left: remap_operand(left)?,
            operator,
            right: remap_operand(right)?,
        },
        FilterPredicate::IsNull { operand, negated } => FilterPredicate::IsNull {
            operand: remap_operand(operand)?,
            negated,
        },
        FilterPredicate::And(left, right) => FilterPredicate::And(
            Box::new(remap_filter_columns(*left, mapping)?),
            Box::new(remap_filter_columns(*right, mapping)?),
        ),
        FilterPredicate::Or(left, right) => FilterPredicate::Or(
            Box::new(remap_filter_columns(*left, mapping)?),
            Box::new(remap_filter_columns(*right, mapping)?),
        ),
        FilterPredicate::Not(inner) => {
            FilterPredicate::Not(Box::new(remap_filter_columns(*inner, mapping)?))
        }
    })
}

fn measured_value_payload_bytes(batch: &AqlResultBatch) -> u64 {
    batch.columns.iter().flatten().fold(0_u64, |total, value| {
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

fn estimate_result_batch_bytes(batch: &AqlResultBatch) -> u64 {
    let mut total = std::mem::size_of::<AqlResultBatch>();
    total = total.saturating_add(
        batch.schema.capacity().saturating_mul(std::mem::size_of::<Column>()),
    );
    total = total.saturating_add(
        batch.columns.capacity().saturating_mul(std::mem::size_of::<Vec<Value>>()),
    );
    for (column, values) in batch.schema.iter().zip(&batch.columns) {
        total = total.saturating_add(column.name.capacity());
        total = total.saturating_add(
            values.capacity().saturating_mul(std::mem::size_of::<Value>()),
        );
        for value in values {
            total = total.saturating_add(match value {
                Value::Utf8(text) => text.capacity(),
                Value::Binary(bytes) => bytes.capacity(),
                _ => 0,
            });
        }
    }
    u64::try_from(total).unwrap_or(u64::MAX)
}

fn from_execution_error(error: ExecutionError) -> AqlExecutionError {
    match error {
        ExecutionError::Storage(error) => from_storage_error(error),
        ExecutionError::Cancelled => query_error(
            AqlErrorCode::Cancelled,
            "AQL query was cancelled",
            crate::Span::default(),
        ),
        ExecutionError::ResourceLimit { resource, actual, limit } => query_error(
            AqlErrorCode::ResourceLimit,
            format!("execution resource limit exceeded for {resource}: {actual} > {limit}"),
            crate::Span::default(),
        ),
        ExecutionError::ArithmeticOverflow => query_error(
            AqlErrorCode::Arithmetic,
            "execution arithmetic overflow",
            crate::Span::default(),
        ),
        ExecutionError::AggregateOverflow(function) => query_error(
            AqlErrorCode::Arithmetic,
            format!("{function} aggregate overflow"),
            crate::Span::default(),
        ),
        ExecutionError::StatisticsFormat(message) => query_error(
            AqlErrorCode::Storage,
            format!("statistics cache error: {message}"),
            crate::Span::default(),
        ),
        other @ (ExecutionError::InvalidSchema(_)
        | ExecutionError::InvalidBatch(_)
        | ExecutionError::InvalidPlan(_)
        | ExecutionError::TypeMismatch(_)) => query_error(
            AqlErrorCode::Execution,
            other.to_string(),
            crate::Span::default(),
        ),
    }
}

fn query_error(code: AqlErrorCode, message: impl Into<String>, span: crate::Span) -> AqlExecutionError {
    AqlExecutionError { code, message: message.into(), span }
}
fn from_parse_error(error: AqlError) -> AqlExecutionError {
    let code = if error.kind == AqlErrorKind::InvalidParameter {
        AqlErrorCode::InvalidParameter
    } else { AqlErrorCode::InvalidQuery };
    query_error(code, error.message, error.span)
}
fn from_storage_error(error: DbError) -> AqlExecutionError {
    let code = match &error {
        DbError::TableNotFound(_) => AqlErrorCode::TableNotFound,
        DbError::ColumnNotFound(_) => AqlErrorCode::ColumnNotFound,
        _ => AqlErrorCode::Storage,
    };
    query_error(code, error.to_string(), crate::Span::default())
}

#[cfg(test)]
mod tests {
    use super::*;
    use ananta_core::Database;
    use ananta_types::{Schema, Value};
    use std::fs;
    use std::sync::atomic::AtomicBool;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn fixture() -> (Database, std::path::PathBuf, Vec<Vec<Value>>) {
        let nonce = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
        let root = std::env::temp_dir().join(format!("ananta-aql-exec-{nonce}"));
        let mut database = Database::open(&root).unwrap();
        let schema = Schema::new(vec![
            Column::new("event_time", DataType::TimestampMicros, false),
            Column::new("device", DataType::Utf8, false),
            Column::new("reading", DataType::Float64, true),
            Column::new("active", DataType::Boolean, true),
        ]).unwrap();
        database.create_table("telemetry", schema).unwrap();
        let rows = vec![
            vec![Value::TimestampMicros(100), Value::Utf8("a".into()), Value::Float64(20.0), Value::Boolean(true)],
            vec![Value::TimestampMicros(200), Value::Utf8("b".into()), Value::Float64(25.0), Value::Null],
            vec![Value::TimestampMicros(300), Value::Utf8("c".into()), Value::Null, Value::Boolean(false)],
            vec![Value::TimestampMicros(400), Value::Utf8("d".into()), Value::Float64(30.0), Value::Boolean(true)],
        ];
        database.insert_batch("telemetry", &rows).unwrap();
        (database, root, rows)
    }

    #[test]
    fn projection_filter_limit_matches_direct_rust_rows_and_emits_bounded_batches() {
        let (db, root, _) = fixture();
        let query = crate::parse(
            "SELECT device, reading FROM telemetry WHERE reading >= :minimum LIMIT 2"
        ).unwrap();
        let bindings = ParameterBindings {
            named: [("minimum".into(), Value::Float64(25.0))].into_iter().collect(),
            positional: vec![],
        };
        let output = execute(&db, &query, &bindings, 1, None).unwrap()
            .collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(output.len(), 2);
        assert!(output.iter().all(|batch| batch.row_count <= 1));
        let actual = output.iter().flat_map(|batch| (0..batch.row_count).map(|row| batch.row(row).unwrap()))
            .collect::<Vec<_>>();
        let reference_rows = db.scan("telemetry").unwrap();
        let expected = reference_rows.iter().filter(|row| matches!(&row[2], Value::Float64(v) if *v >= 25.0))
            .take(2).map(|row| vec![row[1].clone(), row[2].clone()]).collect::<Vec<_>>();
        assert_eq!(actual, expected);
        assert_eq!(output[0].schema[0].name, "device");
        drop(db);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn simple_aql_queries_use_native_operators_and_match_reference_execution() {
        let (db, root, _) = fixture();
        let query = crate::parse(
            "SELECT device, reading FROM telemetry WHERE reading >= :minimum LIMIT 2"
        ).unwrap();
        let bindings = ParameterBindings {
            named: [("minimum".into(), Value::Float64(25.0))].into_iter().collect(),
            positional: vec![],
        };

        let mut optimized = execute(&db, &query, &bindings, 1, None).unwrap();
        assert!(optimized.used_native_execution());
        let optimized_rows = optimized.by_ref()
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
            .iter()
            .flat_map(|batch| (0..batch.row_count).map(|row| batch.row(row).unwrap()))
            .collect::<Vec<_>>();
        assert_eq!(optimized.metrics().output_rows, 2);
        assert!(optimized.metrics().output_bytes > 0);
        assert_eq!(optimized.metrics().output_cells, 4);
        assert_eq!(optimized.metrics().output_value_payload_bytes, 18);
        assert_eq!(optimized.metrics().scan_input_batches, 4);
        assert_eq!(optimized.metrics().scan_input_rows, 4);
        assert_eq!(optimized.metrics().scan_input_value_payload_bytes, 28);

        let mut reference = execute_with_options(&db, &query, &bindings, 1, None, false).unwrap();
        assert!(!reference.used_native_execution());
        let reference_rows = reference.by_ref()
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
            .iter()
            .flat_map(|batch| (0..batch.row_count).map(|row| batch.row(row).unwrap()))
            .collect::<Vec<_>>();
        assert_eq!(optimized_rows, reference_rows);
        assert_eq!(
            optimized_rows,
            vec![
                vec![Value::Utf8("b".into()), Value::Float64(25.0)],
                vec![Value::Utf8("d".into()), Value::Float64(30.0)],
            ]
        );
        drop(db);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn computed_projection_falls_back_without_changing_semantics() {
        let (db, root, _) = fixture();
        let query = crate::parse(
            "SELECT reading + 1.0 AS next_reading FROM telemetry WHERE reading >= 20.0"
        ).unwrap();
        let mut iterator = execute(&db, &query, &ParameterBindings::default(), 2, None).unwrap();
        assert!(!iterator.used_native_execution());
        let rows = iterator
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
            .iter()
            .flat_map(|batch| (0..batch.row_count).map(|row| batch.row(row).unwrap()))
            .collect::<Vec<_>>();
        assert_eq!(
            rows,
            vec![
                vec![Value::Float64(21.0)],
                vec![Value::Float64(26.0)],
                vec![Value::Float64(31.0)],
            ]
        );
        drop(db);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn explain_reports_projection_filter_and_safe_limit_pushdown() {
        let (db, root, _) = fixture();
        let query = crate::parse(
            "SELECT device FROM telemetry WHERE reading >= 25.0 LIMIT 2"
        ).unwrap();
        let plan = explain(&db, &query, &ParameterBindings::default(), true).unwrap();
        assert!(plan.native_execution);
        assert!(plan.predicate_pushed);
        assert!(!plan.storage_limit_pushed);
        assert_eq!(plan.scan_columns, vec!["device", "reading"]);
        assert_eq!(plan.chronicle_accepted_rows, 0);
        assert_eq!(plan.estimated_rows_after_filter, None);
        assert_eq!(plan.estimated_output_rows, None);
        assert!(plan.explain().contains("unknown (no table row-count stats)"));

        let table_stats = ananta_execution::analyze_table(&db, "telemetry", 2).unwrap();
        let analyzed_plan = explain_with_statistics(
            &db,
            &query,
            &ParameterBindings::default(),
            true,
            Some(&table_stats),
        ).unwrap();
        assert_eq!(analyzed_plan.estimated_rows_after_filter, Some(2));
        assert_eq!(analyzed_plan.estimated_output_rows, Some(2));
        assert!(analyzed_plan.estimated_projected_payload_bytes.unwrap() > 0);
        assert!(analyzed_plan.explain().contains("native vector predicate"));

        // ANALYZE persists a versioned cache; ordinary EXPLAIN reuses it
        // without rescanning the table, but the cache is validated against
        // the current immutable scan inventory before use.
        ananta_execution::analyze_table_and_persist(&db, "telemetry", 2).unwrap();
        let cached_plan = explain(&db, &query, &ParameterBindings::default(), true).unwrap();
        assert_eq!(cached_plan.estimated_rows_after_filter, Some(2));
        assert_eq!(cached_plan.estimated_output_rows, Some(2));

        let mut profiled = execute(&db, &query, &ParameterBindings::default(), 2, None).unwrap();
        let _profiled_rows = profiled.by_ref().collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(
            analyzed_plan.estimated_output_rows,
            Some(profiled.metrics().output_rows)
        );
        assert_eq!(profiled.metrics().scan_input_rows, 4);
        assert_eq!(profiled.metrics().output_rows, 2);
        assert_eq!(profiled.metrics().scan_input_value_payload_bytes, 28);
        assert_eq!(profiled.metrics().output_value_payload_bytes, 2);

        let no_filter = crate::parse("SELECT device FROM telemetry LIMIT 2").unwrap();
        let limit_plan = explain(
            &db,
            &no_filter,
            &ParameterBindings::default(),
            true,
        ).unwrap();
        assert!(limit_plan.native_execution);
        assert!(limit_plan.storage_limit_pushed);

        let reference = explain(&db, &query, &ParameterBindings::default(), false).unwrap();
        assert!(!reference.native_execution);
        drop(db);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn analyzed_statistics_drive_equality_null_and_range_selectivity() {
        let (db, root, _) = fixture();
        let table_stats = ananta_execution::analyze_table(&db, "telemetry", 2).unwrap();

        let equality = crate::parse(
            "SELECT device FROM telemetry WHERE reading = 25.0"
        ).unwrap();
        let equality_plan = explain_with_statistics(
            &db, &equality, &ParameterBindings::default(), true, Some(&table_stats),
        ).unwrap();
        assert_eq!(equality_plan.estimated_rows_after_filter, Some(1));

        let is_null = crate::parse(
            "SELECT device FROM telemetry WHERE reading IS NULL"
        ).unwrap();
        let null_plan = explain_with_statistics(
            &db, &is_null, &ParameterBindings::default(), true, Some(&table_stats),
        ).unwrap();
        assert_eq!(null_plan.estimated_rows_after_filter, Some(1));

        let reverse_range = crate::parse(
            "SELECT device FROM telemetry WHERE 25.0 <= reading"
        ).unwrap();
        let range_plan = explain_with_statistics(
            &db, &reverse_range, &ParameterBindings::default(), true, Some(&table_stats),
        ).unwrap();
        assert_eq!(range_plan.estimated_rows_after_filter, Some(2));

        let outside_range = crate::parse(
            "SELECT device FROM telemetry WHERE reading > 40.0"
        ).unwrap();
        let outside_plan = explain_with_statistics(
            &db, &outside_range, &ParameterBindings::default(), true, Some(&table_stats),
        ).unwrap();
        assert_eq!(outside_plan.estimated_rows_after_filter, Some(0));

        drop(db);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn explain_rejects_statistics_from_a_different_schema_snapshot() {
        let (db, root, _) = fixture();
        let mut stale = ananta_execution::analyze_table(&db, "telemetry", 2).unwrap();
        stale.columns[1].column.name = "renamed_device".to_owned();
        let query = crate::parse("SELECT device FROM telemetry").unwrap();
        let result = explain_with_statistics(
            &db,
            &query,
            &ParameterBindings::default(),
            true,
            Some(&stale),
        );
        assert_eq!(result.unwrap_err().code, AqlErrorCode::InvalidQuery);
        drop(db);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn explain_rejects_stale_statistics_after_a_new_segment_is_published() {
        let (mut db, root, _) = fixture();
        let table_stats = ananta_execution::analyze_table(&db, "telemetry", 2).unwrap();
        db.insert_batch("telemetry", &[vec![
            Value::TimestampMicros(500),
            Value::Utf8("e".into()),
            Value::Float64(35.0),
            Value::Boolean(true),
        ]]).unwrap();
        let query = crate::parse("SELECT device FROM telemetry").unwrap();
        let result = explain_with_statistics(
            &db,
            &query,
            &ParameterBindings::default(),
            true,
            Some(&table_stats),
        );
        assert_eq!(result.unwrap_err().code, AqlErrorCode::InvalidQuery);
        drop(db);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn explain_discards_persisted_statistics_after_table_changes() {
        let (mut db, root, _) = fixture();
        let query = crate::parse(
            "SELECT device FROM telemetry WHERE reading >= 25.0"
        ).unwrap();
        ananta_execution::analyze_table_and_persist(&db, "telemetry", 2).unwrap();
        let cached = explain(&db, &query, &ParameterBindings::default(), true).unwrap();
        assert_eq!(cached.estimated_rows_after_filter, Some(2));

        db.insert_batch("telemetry", &[vec![
            Value::TimestampMicros(900),
            Value::Utf8("e".into()),
            Value::Float64(35.0),
            Value::Boolean(true),
        ]]).unwrap();

        let after_write = explain(&db, &query, &ParameterBindings::default(), true).unwrap();
        assert_eq!(after_write.estimated_rows_after_filter, None);
        assert_eq!(after_write.estimated_output_rows, None);
        let cache_path = db.statistics_cache_path("telemetry").unwrap();
        assert!(!cache_path.exists(), "stale statistics cache should be discarded");

        drop(db);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn optimizer_matches_reference_for_nested_boolean_and_null_predicates() {
        let (db, root, _) = fixture();
        let query = crate::parse(
            "SELECT device FROM telemetry WHERE (reading >= 25.0 AND active IS NOT NULL) OR device = 'a'"
        ).unwrap();

        let mut optimized = execute(&db, &query, &ParameterBindings::default(), 2, None).unwrap();
        assert!(optimized.used_native_execution());
        let optimized_rows = optimized
            .by_ref()
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
            .iter()
            .flat_map(|batch| (0..batch.row_count).map(|row| batch.row(row).unwrap()))
            .collect::<Vec<_>>();

        let mut reference = execute_with_options(
            &db,
            &query,
            &ParameterBindings::default(),
            2,
            None,
            false,
        ).unwrap();
        let reference_rows = reference
            .by_ref()
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
            .iter()
            .flat_map(|batch| (0..batch.row_count).map(|row| batch.row(row).unwrap()))
            .collect::<Vec<_>>();
        assert_eq!(optimized_rows, reference_rows);
        drop(db);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn native_projection_preserves_aliases_and_repeated_columns() {
        let (db, root, _) = fixture();
        let query = crate::parse(
            "SELECT device AS primary_device, device AS duplicate_device FROM telemetry WHERE reading >= 25.0"
        ).unwrap();
        let mut iterator = execute(&db, &query, &ParameterBindings::default(), 2, None).unwrap();
        assert!(iterator.used_native_execution());
        let output = iterator.collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(output.len(), 2);
        assert!(output.iter().all(|batch| {
            batch.schema[0].name == "primary_device"
                && batch.schema[1].name == "duplicate_device"
        }));
        let first = output.iter().flat_map(|batch| batch.columns[0].iter().cloned()).collect::<Vec<_>>();
        let second = output.iter().flat_map(|batch| batch.columns[1].iter().cloned()).collect::<Vec<_>>();
        assert_eq!(first, second);
        assert_eq!(first, vec![Value::Utf8("b".into()), Value::Utf8("d".into())]);
        drop(db);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn timestamp_range_and_three_valued_null_filtering_work() {
        let (db, root, _) = fixture();
        let query = crate::parse(
            "SELECT device FROM telemetry WHERE event_time >= $1 AND event_time < $2 AND active IS NOT NULL AND active = TRUE"
        ).unwrap();
        let bindings = ParameterBindings {
            named: Default::default(),
            positional: vec![Value::TimestampMicros(100), Value::TimestampMicros(400)],
        };
        let output = execute(&db, &query, &bindings, 2, None).unwrap()
            .collect::<Result<Vec<_>, _>>().unwrap();
        let result = output.iter().flat_map(|batch| batch.columns[0].iter().cloned()).collect::<Vec<_>>();
        assert_eq!(result, vec![Value::Utf8("a".into())]);
        drop(db);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn cancellation_and_semantic_errors_have_stable_error_codes() {
        let (db, root, _) = fixture();
        let query = crate::parse("SELECT missing FROM telemetry").unwrap();
        let error = match execute(&db, &query, &ParameterBindings::default(), 10, None) {
            Ok(_) => panic!("missing column query unexpectedly compiled"),
            Err(error) => error,
        };
        assert_eq!(error.code, AqlErrorCode::ColumnNotFound);

        let query = crate::parse("SELECT * FROM telemetry").unwrap();
        let cancelled = Arc::new(AtomicBool::new(true));
        let mut iterator = execute(&db, &query, &ParameterBindings::default(), 10, Some(cancelled)).unwrap();
        assert_eq!(iterator.next().unwrap().unwrap_err().code, AqlErrorCode::Cancelled);
        drop(db);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn null_comparisons_are_unknown_and_are_filtered_out() {
        let (db, root, _) = fixture();
        let query = crate::parse("SELECT device FROM telemetry WHERE reading = NULL").unwrap();
        let output = execute(&db, &query, &ParameterBindings::default(), 4, None).unwrap()
            .collect::<Result<Vec<_>, _>>().unwrap();
        assert!(output.is_empty());
        drop(db);
        fs::remove_dir_all(root).unwrap();
    }
}
