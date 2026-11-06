use crate::lexer::{lex, Token, TokenKind};
use crate::{AqlError, AqlErrorKind, Position, Span};
use ananta_types::DataType;

#[derive(Debug, Clone, PartialEq)]
pub struct Query { pub statement: Statement }
#[derive(Debug, Clone, PartialEq)]
pub enum Statement { Select(SelectStatement) }
#[derive(Debug, Clone, PartialEq)]
pub struct SelectStatement {
    pub projection: Projection, pub source: Identifier, pub selection: Option<Expr>,
    pub limit: Option<u64>, pub span: Span,
}
#[derive(Debug, Clone, PartialEq)]
pub enum Projection { All, Items(Vec<SelectItem>) }
#[derive(Debug, Clone, PartialEq)]
pub struct SelectItem { pub expression: Expr, pub alias: Option<Identifier>, pub span: Span }
#[derive(Debug, Clone, PartialEq)]
pub struct Identifier { pub value: String, pub quoted: bool, pub span: Span }
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnReference { pub parts: Vec<Identifier>, pub span: Span }
#[derive(Debug, Clone, PartialEq)]
pub struct Expr { pub kind: ExprKind, pub span: Span }
#[derive(Debug, Clone, PartialEq)]
pub enum ExprKind {
    Column(ColumnReference), Literal(Literal), Parameter(ParameterRef),
    Unary { operator: UnaryOperator, expression: Box<Expr> },
    Binary { left: Box<Expr>, operator: BinaryOperator, right: Box<Expr> },
    IsNull { expression: Box<Expr>, negated: bool },
    Cast { expression: Box<Expr>, target: DataType },
}
#[derive(Debug, Clone, PartialEq)]
pub enum Literal { Null, Boolean(bool), Int64(i64), Float64(f64), Utf8(String) }
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum ParameterRef { Named(String), Positional(u32) }
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnaryOperator { Not, Positive, Negative }
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinaryOperator {
    Or, And, Equal, NotEqual, Less, LessEqual, Greater, GreaterEqual,
    Add, Subtract, Multiply, Divide, Modulo,
}
pub fn parse(source: &str) -> Result<Query, AqlError> {
    let query = Parser::new(lex(source)?).query()?;
    crate::validate_ast(&query)?;
    Ok(query)
}

struct Parser { tokens: Vec<Token>, cursor: usize }
impl Parser {
    fn new(tokens: Vec<Token>) -> Self { Self { tokens, cursor: 0 } }
    fn query(mut self) -> Result<Query, AqlError> {
        let statement = self.select_statement()?;
        if self.consume(&TokenKind::Semicolon).is_some() && !self.at_eof() {
            return Err(self.unexpected("only one statement is allowed"));
        }
        if !self.at_eof() { return Err(self.unexpected("unexpected token after query")); }
        Ok(Query { statement: Statement::Select(statement) })
    }
    fn select_statement(&mut self) -> Result<SelectStatement, AqlError> {
        let start = self.expect(&TokenKind::Select, "expected SELECT")?.span.start;
        let projection = self.projection()?;
        self.expect(&TokenKind::From, "expected FROM after projection")?;
        let source = self.identifier("expected a table name after FROM")?;
        let selection = if self.consume(&TokenKind::Where).is_some() { Some(self.expression(0)?) } else { None };
        let limit = if self.consume(&TokenKind::Limit).is_some() {
            let token = self.advance().clone();
            match token.kind {
                TokenKind::Integer(digits) => Some(digits.parse::<u64>().map_err(|_| AqlError::new(
                    AqlErrorKind::InvalidLimit, "LIMIT exceeds the supported u64 range", token.span,
                ))?),
                _ => return Err(AqlError::new(AqlErrorKind::InvalidLimit,
                    "LIMIT requires a non-negative integer literal", token.span)),
            }
        } else { None };
        let end = self.previous().span.end;
        Ok(SelectStatement { projection, source, selection, limit, span: Span { start, end } })
    }
    fn projection(&mut self) -> Result<Projection, AqlError> {
        if self.consume(&TokenKind::Star).is_some() { return Ok(Projection::All); }
        let mut items = Vec::new();
        loop {
            let expression = self.expression(0)?;
            let alias = if self.consume(&TokenKind::As).is_some() {
                Some(self.identifier("expected an alias after AS")?)
            } else { None };
            let end = alias.as_ref().map_or(expression.span.end, |value| value.span.end);
            items.push(SelectItem { span: Span { start: expression.span.start, end }, expression, alias });
            if self.consume(&TokenKind::Comma).is_none() { break; }
        }
        if items.is_empty() { return Err(self.unexpected("SELECT requires '*' or at least one projection")); }
        Ok(Projection::Items(items))
    }
    fn expression(&mut self, min_bp: u8) -> Result<Expr, AqlError> {
        let mut left = self.prefix()?;
        loop {
            if self.at_eof() { break; }
            if self.check(&TokenKind::Is) {
                if 6 < min_bp { break; }
                let start = left.span.start;
                self.advance();
                let negated = self.consume(&TokenKind::Not).is_some();
                self.expect(&TokenKind::Null, "expected NULL after IS or IS NOT")?;
                let end = self.previous().span.end;
                left = Expr { kind: ExprKind::IsNull { expression: Box::new(left), negated },
                    span: Span { start, end } };
                continue;
            }
            let Some((operator, left_bp, right_bp)) = self.infix_operator() else { break; };
            if left_bp < min_bp { break; }
            let op_span = self.advance().span;
            let right = self.expression(right_bp)?;
            let comparison = matches!(operator, BinaryOperator::Equal | BinaryOperator::NotEqual
                | BinaryOperator::Less | BinaryOperator::LessEqual | BinaryOperator::Greater | BinaryOperator::GreaterEqual);
            if comparison && self.next_is_comparison() {
                return Err(AqlError::new(AqlErrorKind::Parse,
                    "comparison operators cannot be chained; combine predicates with AND", op_span));
            }
            let span = Span { start: left.span.start, end: right.span.end };
            left = Expr { kind: ExprKind::Binary { left: Box::new(left), operator, right: Box::new(right) }, span };
        }
        Ok(left)
    }
    fn prefix(&mut self) -> Result<Expr, AqlError> {
        let token = self.advance().clone();
        match token.kind {
            TokenKind::Not | TokenKind::Plus | TokenKind::Minus => {
                let operator = match token.kind {
                    TokenKind::Not => UnaryOperator::Not, TokenKind::Plus => UnaryOperator::Positive,
                    _ => UnaryOperator::Negative,
                };
                let expression = self.expression(if operator == UnaryOperator::Not { 6 } else { 11 })?;
                let span = Span { start: token.span.start, end: expression.span.end };
                Ok(Expr { kind: ExprKind::Unary { operator, expression: Box::new(expression) }, span })
            }
            TokenKind::LParen => {
                let mut expression = self.expression(0)?;
                let end = self.expect(&TokenKind::RParen, "expected ')' to close expression")?.span.end;
                expression.span = Span { start: token.span.start, end };
                Ok(expression)
            }
            TokenKind::Identifier(value) => self.column_expression(Identifier { value, quoted: false, span: token.span }),
            TokenKind::QuotedIdentifier(value) => self.column_expression(Identifier { value, quoted: true, span: token.span }),
            TokenKind::String(value) => Ok(Expr { kind: ExprKind::Literal(Literal::Utf8(value)), span: token.span }),
            TokenKind::Integer(value) => {
                let integer = value.parse::<i64>().map_err(|_| AqlError::new(
                    AqlErrorKind::InvalidLiteral, "integer literal exceeds INT64 range", token.span))?;
                Ok(Expr { kind: ExprKind::Literal(Literal::Int64(integer)), span: token.span })
            }
            TokenKind::Number(value) => {
                let number = value.parse::<f64>().map_err(|_| AqlError::new(
                    AqlErrorKind::InvalidLiteral, "invalid FLOAT64 literal", token.span))?;
                if !number.is_finite() { return Err(AqlError::new(AqlErrorKind::InvalidLiteral,
                    "FLOAT64 literal must be finite", token.span)); }
                Ok(Expr { kind: ExprKind::Literal(Literal::Float64(number)), span: token.span })
            }
            TokenKind::True => Ok(Expr { kind: ExprKind::Literal(Literal::Boolean(true)), span: token.span }),
            TokenKind::False => Ok(Expr { kind: ExprKind::Literal(Literal::Boolean(false)), span: token.span }),
            TokenKind::Null => Ok(Expr { kind: ExprKind::Literal(Literal::Null), span: token.span }),
            TokenKind::NamedParameter(name) => Ok(Expr { kind: ExprKind::Parameter(ParameterRef::Named(name)), span: token.span }),
            TokenKind::PositionalParameter(index) => Ok(Expr { kind: ExprKind::Parameter(ParameterRef::Positional(index)), span: token.span }),
            TokenKind::Cast => self.cast_expression(token.span.start),
            _ => Err(AqlError::new(AqlErrorKind::Parse, "expected an expression", token.span)),
        }
    }
    fn column_expression(&mut self, first: Identifier) -> Result<Expr, AqlError> {
        let mut parts = vec![first];
        while self.consume(&TokenKind::Dot).is_some() {
            parts.push(self.identifier("expected an identifier after '.'")?);
            if parts.len() > 2 { return Err(self.unexpected("column references may contain at most one qualifier")); }
        }
        let span = Span { start: parts[0].span.start, end: parts.last().unwrap().span.end };
        Ok(Expr { kind: ExprKind::Column(ColumnReference { parts, span }), span })
    }
    fn cast_expression(&mut self, start: Position) -> Result<Expr, AqlError> {
        self.expect(&TokenKind::LParen, "expected '(' after CAST")?;
        let expression = self.expression(0)?;
        self.expect(&TokenKind::As, "expected AS in CAST expression")?;
        let token = self.advance().clone();
        let target = match token.kind {
            TokenKind::Boolean => DataType::Boolean, TokenKind::Int32 => DataType::Int32,
            TokenKind::Int64 => DataType::Int64, TokenKind::Float64 => DataType::Float64,
            TokenKind::Utf8 => DataType::Utf8, TokenKind::TimestampMicros => DataType::TimestampMicros,
            TokenKind::Binary => DataType::Binary,
            _ => return Err(AqlError::new(AqlErrorKind::Parse, "expected a supported AQL type after AS", token.span)),
        };
        let end = self.expect(&TokenKind::RParen, "expected ')' after CAST target type")?.span.end;
        Ok(Expr { kind: ExprKind::Cast { expression: Box::new(expression), target },
            span: Span { start, end } })
    }
    fn infix_operator(&self) -> Option<(BinaryOperator, u8, u8)> {
        let (operator, bp) = match self.current().kind {
            TokenKind::Or => (BinaryOperator::Or, 1), TokenKind::And => (BinaryOperator::And, 3),
            TokenKind::Eq => (BinaryOperator::Equal, 6), TokenKind::NotEq => (BinaryOperator::NotEqual, 6),
            TokenKind::Lt => (BinaryOperator::Less, 6), TokenKind::LtEq => (BinaryOperator::LessEqual, 6),
            TokenKind::Gt => (BinaryOperator::Greater, 6), TokenKind::GtEq => (BinaryOperator::GreaterEqual, 6),
            TokenKind::Plus => (BinaryOperator::Add, 8), TokenKind::Minus => (BinaryOperator::Subtract, 8),
            TokenKind::Star => (BinaryOperator::Multiply, 10), TokenKind::Slash => (BinaryOperator::Divide, 10),
            TokenKind::Percent => (BinaryOperator::Modulo, 10),
            _ => return None,
        };
        Some((operator, bp, bp + 1))
    }
    fn next_is_comparison(&self) -> bool {
        matches!(self.current().kind, TokenKind::Eq | TokenKind::NotEq | TokenKind::Lt
            | TokenKind::LtEq | TokenKind::Gt | TokenKind::GtEq | TokenKind::Is)
    }
    fn identifier(&mut self, message: &str) -> Result<Identifier, AqlError> {
        let token = self.advance().clone();
        match token.kind {
            TokenKind::Identifier(value) => Ok(Identifier { value, quoted: false, span: token.span }),
            TokenKind::QuotedIdentifier(value) => Ok(Identifier { value, quoted: true, span: token.span }),
            _ => Err(AqlError::new(AqlErrorKind::Parse, message, token.span)),
        }
    }
    fn expect(&mut self, expected: &TokenKind, message: &str) -> Result<Token, AqlError> {
        if self.check(expected) { Ok(self.advance().clone()) } else { Err(self.unexpected(message)) }
    }
    fn consume(&mut self, expected: &TokenKind) -> Option<Token> {
        if self.check(expected) { Some(self.advance().clone()) } else { None }
    }
    fn check(&self, expected: &TokenKind) -> bool {
        std::mem::discriminant(&self.current().kind) == std::mem::discriminant(expected)
    }
    fn current(&self) -> &Token { &self.tokens[self.cursor] }
    fn previous(&self) -> &Token { &self.tokens[self.cursor.saturating_sub(1)] }
    fn advance(&mut self) -> &Token {
        let index = self.cursor;
        if !self.at_eof() { self.cursor += 1; }
        &self.tokens[index]
    }
    fn at_eof(&self) -> bool { matches!(self.current().kind, TokenKind::Eof) }
    fn unexpected(&self, message: &str) -> AqlError {
        AqlError::new(AqlErrorKind::Parse, message, self.current().span)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn parses_projection_filter_limit_and_quoted_names() {
        let query = parse(r#"SELECT "device name", reading AS value FROM telemetry WHERE reading >= 21.5 AND "device name" <> 'x' LIMIT 10;"#).unwrap();
        let Statement::Select(select) = query.statement;
        assert_eq!(select.source.value, "telemetry");
        assert_eq!(select.limit, Some(10));
        assert!(matches!(select.projection, Projection::Items(ref items) if items.len() == 2));
        assert!(select.selection.is_some());
    }
    #[test]
    fn boolean_precedence_and_parentheses_are_preserved() {
        let query = parse("SELECT * FROM t WHERE a = 1 OR b = 2 AND NOT (c IS NULL)").unwrap();
        let Statement::Select(select) = query.statement;
        assert!(matches!(select.selection.unwrap().kind, ExprKind::Binary { operator: BinaryOperator::Or, .. }));
    }
    #[test]
    fn parses_casts_and_named_or_positional_parameters() {
        assert!(parse("SELECT CAST(value AS FLOAT64) FROM t WHERE id = :id OR id = $2").is_ok());
    }
    #[test]
    fn rejects_chained_comparisons_multiple_statements_and_invalid_limits() {
        assert!(parse("SELECT * FROM t WHERE 1 < 2 < 3").is_err());
        assert!(parse("SELECT * FROM t; SELECT * FROM t").is_err());
        assert!(parse("SELECT * FROM t LIMIT -1").is_err());
        assert!(parse("SELECT * FROM t LIMIT 1.5").is_err());
        assert!(parse("SELECT * FROM t LIMIT 999999999999999999999999999").is_err());
        assert!(parse("SELECT * FROM t WHERE").is_err());
    }
    #[test]
    fn preserves_line_and_column_for_parser_errors() {
        let err = parse("SELECT *\nFROM t WHERE a =").unwrap_err();
        assert_eq!(err.kind, AqlErrorKind::Parse);
        assert_eq!(err.span.start.line, 2);
    }
}
