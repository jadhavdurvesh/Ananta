use crate::{AqlError, AqlErrorKind, Position, Span};

#[derive(Debug, Clone, PartialEq)]
pub enum TokenKind {
    Select, From, Where, Limit, As, And, Or, Not, Is, Null, True, False, Cast,
    Int32, Int64, Float64, Utf8, Boolean, TimestampMicros, Binary,
    Identifier(String), QuotedIdentifier(String), String(String),
    Integer(String), Number(String), NamedParameter(String), PositionalParameter(u32),
    Star, Comma, Dot, LParen, RParen, Semicolon, Plus, Minus, Slash, Percent,
    Eq, NotEq, Lt, LtEq, Gt, GtEq, Eof,
}
#[derive(Debug, Clone, PartialEq)]
pub struct Token { pub kind: TokenKind, pub span: Span }
pub fn lex(source: &str) -> Result<Vec<Token>, AqlError> { Lexer::new(source).run() }

struct Lexer<'a> { source: &'a str, offset: usize, line: usize, column: usize, tokens: Vec<Token> }
impl<'a> Lexer<'a> {
    fn new(source: &'a str) -> Self { Self { source, offset: 0, line: 1, column: 1, tokens: Vec::new() } }
    fn run(mut self) -> Result<Vec<Token>, AqlError> {
        while let Some(ch) = self.peek() {
            if ch.is_whitespace() { self.bump(); continue; }
            if self.starts_with("--") { self.line_comment(); continue; }
            if self.starts_with("/*") { self.block_comment()?; continue; }
            let start = self.position();
            let kind = match ch {
                'a'..='z' | 'A'..='Z' | '_' => self.identifier_or_keyword(),
                '0'..='9' => self.number()?,
                '\'' => self.string_literal()?,
                '"' => self.quoted_identifier()?,
                ':' => self.named_parameter()?,
                '$' => self.positional_parameter()?,
                '*' => { self.bump(); TokenKind::Star }
                ',' => { self.bump(); TokenKind::Comma }
                '.' => { self.bump(); TokenKind::Dot }
                '(' => { self.bump(); TokenKind::LParen }
                ')' => { self.bump(); TokenKind::RParen }
                ';' => { self.bump(); TokenKind::Semicolon }
                '+' => { self.bump(); TokenKind::Plus }
                '-' => { self.bump(); TokenKind::Minus }
                '/' => { self.bump(); TokenKind::Slash }
                '%' => { self.bump(); TokenKind::Percent }
                '=' => { self.bump(); TokenKind::Eq }
                '!' => {
                    self.bump();
                    if self.peek() == Some('=') { self.bump(); TokenKind::NotEq }
                    else { return Err(self.error("expected '=' after '!'", start)); }
                }
                '<' => {
                    self.bump();
                    match self.peek() {
                        Some('=') => { self.bump(); TokenKind::LtEq }
                        Some('>') => { self.bump(); TokenKind::NotEq }
                        _ => TokenKind::Lt,
                    }
                }
                '>' => {
                    self.bump();
                    if self.peek() == Some('=') { self.bump(); TokenKind::GtEq }
                    else { TokenKind::Gt }
                }
                _ => { self.bump(); return Err(self.error(&format!("unexpected character {ch:?}"), start)); }
            };
            self.push(kind, start);
        }
        let end = self.position();
        self.tokens.push(Token { kind: TokenKind::Eof, span: Span { start: end, end } });
        Ok(self.tokens)
    }
    fn identifier_or_keyword(&mut self) -> TokenKind {
        let start = self.offset;
        self.bump();
        while self.peek().is_some_and(|c| c.is_ascii_alphanumeric() || c == '_') { self.bump(); }
        let text = &self.source[start..self.offset];
        match text.to_ascii_uppercase().as_str() {
            "SELECT" => TokenKind::Select, "FROM" => TokenKind::From, "WHERE" => TokenKind::Where,
            "LIMIT" => TokenKind::Limit, "AS" => TokenKind::As, "AND" => TokenKind::And,
            "OR" => TokenKind::Or, "NOT" => TokenKind::Not, "IS" => TokenKind::Is,
            "NULL" => TokenKind::Null, "TRUE" => TokenKind::True, "FALSE" => TokenKind::False,
            "CAST" => TokenKind::Cast, "INT32" => TokenKind::Int32, "INT64" => TokenKind::Int64,
            "FLOAT64" => TokenKind::Float64, "UTF8" => TokenKind::Utf8, "BOOLEAN" => TokenKind::Boolean,
            "TIMESTAMP_MICROS" => TokenKind::TimestampMicros, "BINARY" => TokenKind::Binary,
            _ => TokenKind::Identifier(text.to_owned()),
        }
    }
    fn number(&mut self) -> Result<TokenKind, AqlError> {
        let start_pos = self.position();
        let start = self.offset;
        while self.peek().is_some_and(|c| c.is_ascii_digit()) { self.bump(); }
        let mut float = false;
        if self.peek() == Some('.') && self.source[self.offset + 1..].chars().next().is_some_and(|c| c.is_ascii_digit()) {
            float = true; self.bump();
            while self.peek().is_some_and(|c| c.is_ascii_digit()) { self.bump(); }
        }
        if self.peek().is_some_and(|c| c == 'e' || c == 'E') {
            float = true; self.bump();
            if self.peek().is_some_and(|c| c == '+' || c == '-') { self.bump(); }
            if !self.peek().is_some_and(|c| c.is_ascii_digit()) {
                return Err(self.error("exponent requires at least one digit", start_pos));
            }
            while self.peek().is_some_and(|c| c.is_ascii_digit()) { self.bump(); }
        }
        let text = self.source[start..self.offset].to_owned();
        Ok(if float { TokenKind::Number(text) } else { TokenKind::Integer(text) })
    }
    fn string_literal(&mut self) -> Result<TokenKind, AqlError> {
        let start = self.position(); self.bump();
        let mut value = String::new();
        loop {
            match self.peek() {
                None => return Err(self.error("unterminated string literal", start)),
                Some('\'') => {
                    self.bump();
                    if self.peek() == Some('\'') { self.bump(); value.push('\''); } else { break; }
                }
                Some(ch) => { self.bump(); value.push(ch); }
            }
        }
        Ok(TokenKind::String(value))
    }
    fn quoted_identifier(&mut self) -> Result<TokenKind, AqlError> {
        let start = self.position(); self.bump();
        let mut value = String::new();
        loop {
            match self.peek() {
                None => return Err(self.error("unterminated quoted identifier", start)),
                Some('"') => {
                    self.bump();
                    if self.peek() == Some('"') { self.bump(); value.push('"'); } else { break; }
                }
                Some(ch) => { self.bump(); value.push(ch); }
            }
        }
        if value.is_empty() { return Err(self.error("quoted identifier cannot be empty", start)); }
        Ok(TokenKind::QuotedIdentifier(value))
    }
    fn named_parameter(&mut self) -> Result<TokenKind, AqlError> {
        let start = self.position(); self.bump();
        if !self.peek().is_some_and(|c| c.is_ascii_alphabetic() || c == '_') {
            return Err(self.error("named parameter must be written as :name", start));
        }
        let begin = self.offset; self.bump();
        while self.peek().is_some_and(|c| c.is_ascii_alphanumeric() || c == '_') { self.bump(); }
        Ok(TokenKind::NamedParameter(self.source[begin..self.offset].to_owned()))
    }
    fn positional_parameter(&mut self) -> Result<TokenKind, AqlError> {
        let start = self.position(); self.bump();
        if !self.peek().is_some_and(|c| c.is_ascii_digit()) {
            return Err(self.error("positional parameter must be written as $1, $2, ...", start));
        }
        let begin = self.offset;
        while self.peek().is_some_and(|c| c.is_ascii_digit()) { self.bump(); }
        let index = self.source[begin..self.offset].parse::<u32>()
            .map_err(|_| self.error("positional parameter index exceeds u32", start))?;
        if index == 0 { return Err(self.error("positional parameters are one-based; $0 is invalid", start)); }
        Ok(TokenKind::PositionalParameter(index))
    }
    fn line_comment(&mut self) { while let Some(ch) = self.peek() { self.bump(); if ch == '\n' { break; } } }
    fn block_comment(&mut self) -> Result<(), AqlError> {
        let start = self.position(); self.bump(); self.bump();
        while self.peek().is_some() {
            if self.starts_with("*/") { self.bump(); self.bump(); return Ok(()); }
            self.bump();
        }
        Err(self.error("unterminated block comment", start))
    }
    fn push(&mut self, kind: TokenKind, start: Position) {
        let end = self.position(); self.tokens.push(Token { kind, span: Span { start, end } });
    }
    fn error(&self, message: &str, start: Position) -> AqlError {
        AqlError::new(AqlErrorKind::Lexical, message, Span { start, end: self.position() })
    }
    fn position(&self) -> Position { Position { offset: self.offset, line: self.line, column: self.column } }
    fn peek(&self) -> Option<char> { self.source[self.offset..].chars().next() }
    fn starts_with(&self, value: &str) -> bool { self.source[self.offset..].starts_with(value) }
    fn bump(&mut self) -> Option<char> {
        let ch = self.peek()?; self.offset += ch.len_utf8();
        if ch == '\n' { self.line += 1; self.column = 1; } else { self.column += 1; }
        Some(ch)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn lexes_keywords_case_insensitively_and_preserves_identifier_spelling() {
        let tokens = lex("select Device FROM telemetry").unwrap();
        assert_eq!(tokens[0].kind, TokenKind::Select);
        assert_eq!(tokens[1].kind, TokenKind::Identifier("Device".into()));
        assert_eq!(tokens[2].kind, TokenKind::From);
    }
    #[test]
    fn handles_escaped_strings_identifiers_and_parameters() {
        let tokens = lex(r#"SELECT "a""b", 'it''s', :device, $2 FROM t"#).unwrap();
        assert_eq!(tokens[1].kind, TokenKind::QuotedIdentifier("a\"b".into()));
        assert_eq!(tokens[3].kind, TokenKind::String("it's".into()));
        assert_eq!(tokens[5].kind, TokenKind::NamedParameter("device".into()));
        assert_eq!(tokens[7].kind, TokenKind::PositionalParameter(2));
    }
    #[test]
    fn handles_comments_and_unicode_string_spans() {
        let tokens = lex("-- heading\nSELECT '温度' /* units */ FROM t").unwrap();
        assert_eq!(tokens[1].kind, TokenKind::String("温度".into()));
        assert_eq!(tokens[1].span.start.line, 2);
        assert_eq!(tokens[1].span.start.column, 8);
    }
    #[test]
    fn rejects_bad_parameters_and_unterminated_tokens_with_spans() {
        let err = lex("SELECT : FROM t").unwrap_err();
        assert_eq!(err.kind, AqlErrorKind::Lexical);
        assert_eq!(err.span.start.column, 8);
        assert!(lex("SELECT 'oops").is_err());
        assert!(lex("SELECT \"oops").is_err());
        assert!(lex("SELECT 1e+ FROM t").is_err());
        assert!(lex("SELECT $0 FROM t").is_err());
        assert!(lex("SELECT /*").is_err());
    }
}
