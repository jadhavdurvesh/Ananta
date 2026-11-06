mod lexer;
mod parser;
mod executor;
mod plan;
mod vector;
mod aggregate;
mod timeseries;
mod planner;
mod vectorized;

pub use ananta_types::DataType;
pub use lexer::{lex, Token, TokenKind};
pub use parser::{parse, BinaryOperator, ColumnReference, Expr, ExprKind, Identifier, Literal,
    ParameterRef, Projection, Query, SelectItem, SelectStatement, Statement, UnaryOperator};

pub use executor::{
    execute, execute_source, execute_with_options, explain, explain_with_statistics, AqlErrorCode, AqlExecutionError,
    AqlExecutionMetrics, AqlPhysicalPlan, AqlResultBatch, AqlResultIter,
};
pub use plan::{logical_plan, LogicalPlan, LogicalPlanNode};
pub use vector::{
    CapsuleScanOperator, ExecutionContext, ExecutionError, ExecutionPipeline, ExecutionProfile,
    FilterOperator, LimitOperator, PhysicalOperator, ProjectionOperator, ResourceLimits,
    ScanOperator, Vector, VectorBatch, VectorExpr,
};
pub use aggregate::{aggregate_batches, AggregateKind, AggregateResult, AggregateSpec};
pub use timeseries::{
    bucketed_aggregate, filter_time_range, latest_per_series, time_bucket_start, TimeRange,
};
pub use planner::{
    explain as explain_plan, extract_timestamp_range, ExplainPlan, PhysicalPlanNode,
    PlannerOptions, ColumnStatistics, TableStatistics, PlanCostEstimate,
};
pub use ananta_execution::{
    analyze_table, analyze_table_and_persist, load_table_statistics,
    ColumnStatistics as AnalyzedColumnStatistics, HistogramBucket, StatisticsFormatError,
    TableStatistics as AnalyzedTableStatistics,
};
pub use vectorized::{execute_vectorized, execute_vectorized_source, execute_vectorized_source_with_options, execute_vectorized_with_options};

pub const AQL_VERSION: &str = "0.1";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Position { pub offset: usize, pub line: usize, pub column: usize }
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Span { pub start: Position, pub end: Position }
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AqlErrorKind { Lexical, Parse, InvalidLiteral, InvalidLimit, InvalidParameter, Semantic }
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AqlError { pub kind: AqlErrorKind, pub message: String, pub span: Span }

impl AqlError {
    pub fn new(kind: AqlErrorKind, message: impl Into<String>, span: Span) -> Self {
        Self { kind, message: message.into(), span }
    }
}
impl std::fmt::Display for AqlError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} at {}:{} (bytes {}..{})", self.message,
            self.span.start.line, self.span.start.column, self.span.start.offset, self.span.end.offset)
    }
}
impl std::error::Error for AqlError {}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct ParameterBindings {
    pub named: std::collections::BTreeMap<String, ananta_types::Value>,
    pub positional: Vec<ananta_types::Value>,
}
impl ParameterBindings {
    pub fn validate_for(&self, query: &Query) -> Result<(), AqlError> {
        let Statement::Select(select) = &query.statement;
        let mut referenced = std::collections::BTreeSet::new();
        if let Projection::Items(items) = &select.projection {
            for item in items { collect_parameters(&item.expression, &mut referenced); }
        }
        if let Some(selection) = &select.selection { collect_parameters(selection, &mut referenced); }
        for parameter in referenced {
            match parameter {
                ParameterRef::Named(name) if !self.named.contains_key(&name) =>
                    return Err(AqlError::new(AqlErrorKind::InvalidParameter,
                        format!("missing binding for :{name}"), Span::default())),
                ParameterRef::Positional(0) =>
                    return Err(AqlError::new(AqlErrorKind::InvalidParameter,
                        "positional parameter indices are one-based", Span::default())),
                ParameterRef::Positional(index) if self.positional.get((index - 1) as usize).is_none() =>
                    return Err(AqlError::new(AqlErrorKind::InvalidParameter,
                        "missing positional parameter binding", Span::default())),
                _ => {}
            }
        }
        Ok(())
    }
}
fn collect_parameters(expression: &Expr, out: &mut std::collections::BTreeSet<ParameterRef>) {
    match &expression.kind {
        ExprKind::Parameter(parameter) => { out.insert(parameter.clone()); }
        ExprKind::Unary { expression, .. } | ExprKind::IsNull { expression, .. }
        | ExprKind::Cast { expression, .. } => collect_parameters(expression, out),
        ExprKind::Binary { left, right, .. } => {
            collect_parameters(left, out); collect_parameters(right, out);
        }
        ExprKind::Column(_) | ExprKind::Literal(_) => {}
    }
}


/// Validate the public AST shape before planning or execution. Catalog name/type
/// resolution is performed against a Database by the executor.
pub fn validate_ast(query: &Query) -> Result<(), AqlError> {
    let Statement::Select(select) = &query.statement;
    validate_identifier(&select.source)?;
    if let Projection::Items(items) = &select.projection {
        if items.is_empty() {
            return Err(AqlError::new(AqlErrorKind::Semantic,
                "SELECT projection cannot be empty", select.span));
        }
        for item in items {
            validate_expr(&item.expression)?;
            if let Some(alias) = &item.alias { validate_identifier(alias)?; }
        }
    }
    if let Some(predicate) = &select.selection { validate_expr(predicate)?; }
    Ok(())
}

fn validate_identifier(identifier: &Identifier) -> Result<(), AqlError> {
    if identifier.value.is_empty() {
        return Err(AqlError::new(AqlErrorKind::Semantic,
            "identifier cannot be empty", identifier.span));
    }
    if !identifier.quoted {
        let mut chars = identifier.value.chars();
        let valid_start = chars.next().is_some_and(|ch| ch.is_ascii_alphabetic() || ch == '_');
        if !valid_start || !chars.all(|ch| ch.is_ascii_alphanumeric() || ch == '_') {
            return Err(AqlError::new(AqlErrorKind::Semantic,
                "unquoted identifier has invalid characters", identifier.span));
        }
    }
    Ok(())
}

fn validate_expr(expression: &Expr) -> Result<(), AqlError> {
    if expression.span.start.offset > expression.span.end.offset {
        return Err(AqlError::new(AqlErrorKind::Semantic,
            "expression source span is reversed", expression.span));
    }
    match &expression.kind {
        ExprKind::Column(reference) => {
            if !(1..=2).contains(&reference.parts.len()) {
                return Err(AqlError::new(AqlErrorKind::Semantic,
                    "column reference must contain one or two identifiers", reference.span));
            }
            for part in &reference.parts { validate_identifier(part)?; }
        }
        ExprKind::Literal(Literal::Float64(value)) if !value.is_finite() => {
            return Err(AqlError::new(AqlErrorKind::InvalidLiteral,
                "FLOAT64 literal must be finite", expression.span));
        }
        ExprKind::Parameter(ParameterRef::Named(name)) => {
            if name.is_empty() || !name.chars().next().is_some_and(|ch| ch.is_ascii_alphabetic() || ch == '_')
                || !name.chars().all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
            {
                return Err(AqlError::new(AqlErrorKind::InvalidParameter,
                    "named parameter must be an ASCII identifier", expression.span));
            }
        }
        ExprKind::Parameter(ParameterRef::Positional(0)) => {
            return Err(AqlError::new(AqlErrorKind::InvalidParameter,
                "positional parameters are one-based", expression.span));
        }
        ExprKind::Unary { expression, .. } | ExprKind::IsNull { expression, .. }
        | ExprKind::Cast { expression, .. } => validate_expr(expression)?,
        ExprKind::Binary { left, right, .. } => {
            validate_expr(left)?;
            validate_expr(right)?;
        }
        _ => {}
    }
    Ok(())
}


#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn structural_ast_validation_rejects_invalid_unquoted_identifiers_and_zero_parameters() {
        let zero = Position::default();
        let span = Span { start: zero, end: zero };
        let invalid_identifier = Query {
            statement: Statement::Select(SelectStatement {
                projection: Projection::All,
                source: Identifier { value: "bad table".into(), quoted: false, span },
                selection: None,
                limit: None,
                span,
            }),
        };
        assert_eq!(validate_ast(&invalid_identifier).unwrap_err().kind, AqlErrorKind::Semantic);

        let invalid_parameter = Query {
            statement: Statement::Select(SelectStatement {
                projection: Projection::Items(vec![SelectItem {
                    expression: Expr {
                        kind: ExprKind::Parameter(ParameterRef::Positional(0)),
                        span,
                    },
                    alias: None,
                    span,
                }]),
                source: Identifier { value: "telemetry".into(), quoted: false, span },
                selection: None,
                limit: None,
                span,
            }),
        };
        assert_eq!(validate_ast(&invalid_parameter).unwrap_err().kind, AqlErrorKind::InvalidParameter);
    }

    #[test]
    fn version_and_named_parameter_validation_are_explicit() {
        assert_eq!(AQL_VERSION, "0.1");
        let query = parse("SELECT * FROM t WHERE id = :id").unwrap();
        assert_eq!(ParameterBindings::default().validate_for(&query).unwrap_err().kind,
            AqlErrorKind::InvalidParameter);
        let bindings = ParameterBindings {
            named: [("id".to_owned(), ananta_types::Value::Int64(7))].into_iter().collect(),
            positional: vec![],
        };
        assert!(bindings.validate_for(&query).is_ok());
    }
    #[test]
    fn parameter_values_are_never_interpolated_into_source_text() {
        let query = parse("SELECT * FROM t WHERE name = :name").unwrap();
        let bindings = ParameterBindings {
            named: [("name".to_owned(), ananta_types::Value::Utf8("x' OR TRUE --".into()))]
                .into_iter().collect(),
            positional: vec![],
        };
        assert!(bindings.validate_for(&query).is_ok());
    }
}
