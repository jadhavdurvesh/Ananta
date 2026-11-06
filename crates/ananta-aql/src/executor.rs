use crate::{
    AqlError, AqlErrorKind, BinaryOperator, ColumnReference, Expr, ExprKind, Identifier,
    Literal, ParameterBindings, ParameterRef, Projection, Query, Statement, UnaryOperator,
};
use ananta_core::{Database, DbError, ScanBatchIter};
use ananta_types::{Column, DataType, Schema, Value};
use std::cmp::Ordering;
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

pub struct AqlResultIter {
    input: ScanBatchIter,
    projection: Vec<CompiledProjection>,
    predicate: Option<CompiledExpr>,
    bindings: ParameterBindings,
    remaining_limit: Option<u64>,
    cancellation: Option<Arc<AtomicBool>>,
    finished: bool,
}

pub fn execute(
    database: &Database,
    query: &Query,
    bindings: &ParameterBindings,
    batch_rows: usize,
    cancellation: Option<Arc<AtomicBool>>,
) -> Result<AqlResultIter, AqlExecutionError> {
    crate::validate_ast(query).map_err(from_parse_error)?;
    bindings.validate_for(query).map_err(from_parse_error)?;
    let Statement::Select(select) = &query.statement;
    let source = resolve_table_name(database, &select.source)?;
    let schema = database.schema(&source).map_err(from_storage_error)?.clone();

    let projection = match &select.projection {
        Projection::All => schema.columns().iter().enumerate().map(|(index, column)| {
            CompiledProjection {
                expression: CompiledExpr {
                    kind: CompiledExprKind::Column(index),
                    data_type: Some(column.data_type),
                    nullable: column.nullable,
                    span: select.source.span,
                },
                column: column.clone(),
            }
        }).collect(),
        Projection::Items(items) => {
            let mut compiled = Vec::with_capacity(items.len());
            for (index, item) in items.iter().enumerate() {
                let expression = compile_expr(
                    &item.expression, &schema, &source, bindings,
                )?;
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
            compiled
        }
    };

    let predicate = match &select.selection {
        Some(expression) => {
            let compiled = compile_expr(expression, &schema, &source, bindings)?;
            if compiled.data_type.is_some_and(|kind| kind != DataType::Boolean) {
                return Err(query_error(AqlErrorCode::TypeMismatch,
                    "WHERE expression must have type BOOLEAN", expression.span));
            }
            Some(compiled)
        }
        None => None,
    };

    let input = database.scan_batches(&source, batch_rows).map_err(from_storage_error)?;
    Ok(AqlResultIter {
        input,
        projection,
        predicate,
        bindings: bindings.clone(),
        remaining_limit: select.limit,
        cancellation,
        finished: false,
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
        if self.finished || self.remaining_limit == Some(0) {
            self.finished = true;
            return None;
        }
        loop {
            if self.is_cancelled() {
                self.finished = true;
                return Some(Err(query_error(AqlErrorCode::Cancelled,
                    "AQL query was cancelled", crate::Span::default())));
            }
            let input = match self.input.next() {
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
                    return Some(Err(query_error(AqlErrorCode::Cancelled,
                        "AQL query was cancelled", crate::Span::default())));
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
                            return Some(Err(query_error(AqlErrorCode::TypeMismatch,
                                "WHERE expression did not evaluate to BOOLEAN", predicate.span)));
                        }
                        Err(error) => { self.finished = true; return Some(Err(error)); }
                    }
                }
                for (index, selected) in self.projection.iter().enumerate() {
                    match evaluate(&selected.expression, &input, row_index, &self.bindings) {
                        Ok(value) => output_columns[index].push(value),
                        Err(error) => { self.finished = true; return Some(Err(error)); }
                    }
                }
                if let Some(limit) = &mut self.remaining_limit { *limit -= 1; }
            }
            let row_count = output_columns.first().map_or(0, Vec::len);
            if row_count == 0 {
                if self.finished || self.remaining_limit == Some(0) {
                    self.finished = true;
                    return None;
                }
                continue;
            }
            let schema = self.projection.iter().map(|item| item.column.clone()).collect();
            return Some(Ok(AqlResultBatch { schema, columns: output_columns, row_count }));
        }
    }
}

impl AqlResultIter {
    pub fn is_cancelled(&self) -> bool {
        self.cancellation.as_ref().is_some_and(|flag| flag.load(AtomicOrdering::Acquire))
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
