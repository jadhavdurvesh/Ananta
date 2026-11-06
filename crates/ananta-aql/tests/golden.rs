use ananta_aql::{
    parse, BinaryOperator, Expr, ExprKind, Identifier, Literal, ParameterRef,
    Projection, Query, Statement, UnaryOperator,
};

fn identifier(value: &Identifier) -> String {
    if value.quoted {
        format!("\"{}\"", value.value.replace('"', "\"\""))
    } else { value.value.clone() }
}
fn expression(value: &Expr) -> String {
    match &value.kind {
        ExprKind::Column(reference) => reference.parts.iter().map(identifier).collect::<Vec<_>>().join("."),
        ExprKind::Literal(Literal::Null) => "NULL".into(),
        ExprKind::Literal(Literal::Boolean(true)) => "TRUE".into(),
        ExprKind::Literal(Literal::Boolean(false)) => "FALSE".into(),
        ExprKind::Literal(Literal::Int64(number)) => number.to_string(),
        ExprKind::Literal(Literal::Float64(number)) => number.to_string(),
        ExprKind::Literal(Literal::Utf8(text)) => format!("'{}'", text.replace('\'', "''")),
        ExprKind::Parameter(ParameterRef::Named(name)) => format!(":{name}"),
        ExprKind::Parameter(ParameterRef::Positional(index)) => "$".to_owned() + &index.to_string(),
        ExprKind::Unary { operator, expression: child } => match operator {
            UnaryOperator::Not => format!("NOT ({})", expression(child)),
            UnaryOperator::Positive => format!("+{}", expression(child)),
            UnaryOperator::Negative => format!("-{}", expression(child)),
        },
        ExprKind::Binary { left, operator, right } => {
            let op = match operator {
                BinaryOperator::Or => "OR", BinaryOperator::And => "AND",
                BinaryOperator::Equal => "=", BinaryOperator::NotEqual => "<>",
                BinaryOperator::Less => "<", BinaryOperator::LessEqual => "<=",
                BinaryOperator::Greater => ">", BinaryOperator::GreaterEqual => ">=",
                BinaryOperator::Add => "+", BinaryOperator::Subtract => "-",
                BinaryOperator::Multiply => "*", BinaryOperator::Divide => "/",
                BinaryOperator::Modulo => "%",
            };
            format!("({} {op} {})", expression(left), expression(right))
        }
        ExprKind::IsNull { expression: child, negated } =>
            format!("{} IS{} NULL", expression(child), if *negated { " NOT" } else { "" }),
        ExprKind::Cast { expression: child, target } =>
            format!("CAST({} AS {target})", expression(child)),
    }
}
fn snapshot(query: &Query) -> String {
    let Statement::Select(select) = &query.statement;
    let projection = match &select.projection {
        Projection::All => "*".to_owned(),
        Projection::Items(items) => items.iter().map(|item| {
            let mut value = expression(&item.expression);
            if let Some(alias) = &item.alias { value.push_str(&format!(" AS {}", identifier(alias))); }
            value
        }).collect::<Vec<_>>().join(", "),
    };
    let mut result = format!("SELECT {projection} FROM {}", identifier(&select.source));
    if let Some(predicate) = &select.selection { result.push_str(&format!(" WHERE {}", expression(predicate))); }
    if let Some(limit) = select.limit { result.push_str(&format!(" LIMIT {limit}")); }
    result
}

#[test]
fn select_filter_precedence_alias_and_limit_golden() {
    let parsed = parse(
        "select device, temperature as reading from telemetry where temperature >= 20 and active is not null limit 2;"
    ).unwrap();
    assert_eq!(
        snapshot(&parsed),
        "SELECT device, temperature AS reading FROM telemetry WHERE ((temperature >= 20) AND active IS NOT NULL) LIMIT 2"
    );
}

#[test]
fn literals_parameters_casts_and_identifier_quoting_golden() {
    let parsed = parse(r#"SELECT CAST("Reading""C" AS FLOAT64), :threshold, $2, 'it''s' FROM telemetry"#).unwrap();
    assert_eq!(
        snapshot(&parsed),
        "SELECT CAST(\"Reading\"\"C\" AS FLOAT64), :threshold, $2, 'it''s' FROM telemetry"
    );
}

#[test]
fn rejected_grammar_golden_cases() {
    for input in [
        "SELECT * FROM t ORDER BY x",
        "SELECT * FROM t; SELECT * FROM other",
        "SELECT * FROM t WHERE a < b < c",
        "SELECT * FROM t LIMIT -1",
        "SELECT * FROM t WHERE id = $0",
        "SELECT * FROM t WHERE id = :",
    ] {
        assert!(parse(input).is_err(), "expected rejection for {input}");
    }
}
