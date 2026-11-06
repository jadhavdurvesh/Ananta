mod lexer;
mod parser;
mod executor;

pub use ananta_types::DataType;
pub use lexer::{lex, Token, TokenKind};
pub use parser::{parse, BinaryOperator, ColumnReference, Expr, ExprKind, Identifier, Literal,
    ParameterRef, Projection, Query, SelectItem, SelectStatement, Statement, UnaryOperator};

pub use executor::{
    execute, execute_source, AqlErrorCode, AqlExecutionError, AqlRecordBatch, AqlResultIter,
};

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

#[cfg(test)]
mod tests {
    use super::*;
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
