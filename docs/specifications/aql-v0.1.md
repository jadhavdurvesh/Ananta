# ANANTA Query Language (AQL) v0.1

Status: M4.1 language contract; initial lexer/parser implementation in progress.
Language version: 0.1 (AQL_VERSION).
Execution scope: read-only single-table SELECT only.

## 1. Goals

AQL is ANANTA's native, typed query interface. It is designed around ANANTA catalog identities, typed values, Capsule/Weave storage and bounded batch APIs. The first version prioritises explicit semantics, safe parameter binding, predictable diagnostics and a small implementable grammar.

AQL is not a SQL compatibility layer. It has no dependency on PostgreSQL, SQLite, ClickHouse or an external parser. Its syntax may be SQL-familiar, but its semantics and execution belong to ANANTA.

## 2. Non-goals for v0.1

No INSERT/UPDATE/DELETE/DDL, joins, subqueries, CTEs, aggregates, GROUP BY, HAVING, ORDER BY, DISTINCT, window functions, UNION, transactions, user-defined functions, implicit collation, dynamic SQL, arbitrary function calls or multi-statement batches. No query can modify stored data.

## 3. Statement grammar

One query contains exactly one statement and may end with one optional semicolon.

    query = select_statement, [ ";" ] ;
    select_statement = "SELECT", projection, "FROM", identifier,
                       [ "WHERE", expression ], [ "LIMIT", unsigned_integer ] ;
    projection = "*" | select_item, { ",", select_item } ;
    select_item = expression, [ "AS", identifier ] ;
    expression = or_expression ;
    or_expression = and_expression, { "OR", and_expression } ;
    and_expression = not_expression, { "AND", not_expression } ;
    not_expression = [ "NOT" ], comparison ;
    comparison = additive, [ comparison_operator, additive
                           | "IS", [ "NOT" ], "NULL" ] ;
    additive = multiplicative, { ( "+" | "-" ), multiplicative } ;
    multiplicative = unary, { ( "*" | "/" | "%" ), unary } ;
    unary = ( "+" | "-" ), unary | primary ;
    primary = identifier, [ ".", identifier ] | literal | parameter
            | "(", expression, ")"
            | "CAST", "(", expression, "AS", type_name, ")" ;
    comparison_operator = "=" | "!=" | "<>" | "<" | "<=" | ">" | ">=" ;
    type_name = "BOOLEAN" | "INT32" | "INT64" | "FLOAT64" | "UTF8"
              | "TIMESTAMP_MICROS" | "BINARY" ;
    literal = "NULL" | "TRUE" | "FALSE" | integer | float | string ;
    parameter = ":" , parameter_name | "$" , positive_integer ;

Keywords are ASCII case-insensitive. The grammar is intentionally one table per query. Qualified columns have exactly two parts and are reserved for a future table-alias rule; until aliases exist, the qualifier must be the source table name.

## 4. Identifiers and quoting

Unquoted identifiers begin with ASCII letter or underscore and continue with ASCII letters, digits or underscore. Unquoted names resolve case-insensitively against the catalog while preserving source spelling in the AST. Quoted identifiers use double quotes; a quote inside a quoted name is doubled ("a""b" means a"b). Quoted names preserve exact spelling. Empty quoted identifiers are invalid. A quoted keyword is always an identifier.

Unquoted keywords are reserved. A reserved word used as a table/column name must be quoted. Unicode is supported in string values and quoted identifiers, not in unquoted identifier syntax.

## 5. Literals and data types

- NULL, TRUE, FALSE are null and boolean literals.
- Integer tokens are signed-64-bit values after unary parsing; overflow is a parse error.
- Decimal/exponent tokens are FLOAT64 and must parse to a finite value. NaN and infinities are not literals.
- Strings use single quotes; an embedded quote is doubled ('it''s'). Backslash has no special escape meaning. UTF-8 is preserved.
- AQL v0.1 maps BOOLEAN, INT32, INT64, FLOAT64, UTF8, TIMESTAMP_MICROS and BINARY to ANANTA native DataType.
- Integer literals default to INT64. Decimal/exponent literals default to FLOAT64. No implicit numeric widening or string-to-number conversion.
- CAST(expr AS type) is explicit. Runtime cast support is checked by the execution layer; parsing a cast does not claim the conversion succeeded.

## 6. Null and boolean logic

AQL uses three-valued logic: TRUE, FALSE and UNKNOWN. Comparisons involving NULL yield UNKNOWN. IS NULL and IS NOT NULL always yield TRUE or FALSE. NOT UNKNOWN is UNKNOWN. WHERE retains rows only when its predicate is TRUE; FALSE and UNKNOWN are filtered out.

| A | B | A AND B | A OR B |
|---|---|---|---|
| TRUE | TRUE | TRUE | TRUE |
| TRUE | FALSE | FALSE | TRUE |
| TRUE | UNKNOWN | UNKNOWN | TRUE |
| FALSE | TRUE | FALSE | TRUE |
| FALSE | FALSE | FALSE | FALSE |
| FALSE | UNKNOWN | FALSE | UNKNOWN |
| UNKNOWN | TRUE | UNKNOWN | TRUE |
| UNKNOWN | FALSE | FALSE | UNKNOWN |
| UNKNOWN | UNKNOWN | UNKNOWN | UNKNOWN |

## 7. Comparison and arithmetic semantics

Comparisons are type-strict: operands must resolve to the same logical type after explicit casts and parameter binding. No implicit collation or timezone conversion exists. UTF8 uses deterministic Unicode scalar lexicographic order. TIMESTAMP_MICROS is a signed count of microseconds from the Unix epoch in UTC; no timezone-dependent literal syntax exists in v0.1.

BOOLEAN supports equality/inequality only. Numeric types support comparisons and arithmetic; integer arithmetic is checked for overflow, and division/modulo by zero is an execution error. FLOAT64 operations must remain finite; non-finite results are errors. UTF8, BINARY and TIMESTAMP_MICROS support typed comparisons; arithmetic on them is undefined. Chained comparisons are rejected; combine predicates with AND/OR.

## 8. CAST contract

Initial supported explicit conversions:
- INT32 -> INT64 (exact widening)
- INT32/INT64 -> FLOAT64 only when exactly representable
- INT64 -> INT32 only if in range
- FLOAT64 -> INT32/INT64 only if finite, integral and in range
- Same-type casts are identity
- Unsupported casts return a typed execution error

String parsing, binary formatting, boolean text parsing and timestamp textual parsing are not implicit and are not in the initial cast subset. The parser accepts the target type; runtime validation owns conversion.

## 9. Parameters

Named parameters use :name; positional parameters use one-based $1, $2, etc. Names are ASCII and case-sensitive. Parameter values are bound as typed ananta_types::Value objects through the API. A binding never splices source text, identifiers or SQL fragments. Missing referenced bindings are rejected before execution. Extra bindings may be ignored. Parameters cannot stand in for table or column identifiers, type names, operators or LIMIT.

## 10. Error locations and diagnostics

Every token and AST node carries a half-open source span. Offsets are UTF-8 byte offsets; line and column are one-based, with column measured in Unicode scalar values. Errors carry stable AqlErrorKind, a human-readable message and a span. Lexing errors point to the malformed token; parser errors point to the unexpected token. Semantic errors will point to the relevant expression span. Diagnostics include line:column and byte range. No parser recovery is attempted in v0.1; the first error is returned.

## 11. Versioning and compatibility

The crate exports AQL_VERSION = "0.1". Version 0.1 grammar and semantics are the compatibility baseline. A grammar/semantic change that can change meaning requires an explicit language-version change and updated golden tests. Unknown future tokens/operators are rejected. On-disk ANF/Capsule/Weave versions are independent from AQL versioning.

## 12. Accepted and rejected examples

Accepted:

    SELECT * FROM telemetry;
    SELECT event_time, device AS source FROM telemetry LIMIT 100
    SELECT reading FROM telemetry WHERE reading >= 21.5 AND device <> 'sensor-7'
    SELECT "device name" FROM telemetry WHERE "device name" IS NOT NULL
    SELECT * FROM telemetry WHERE id = :device_id OR id = $2
    SELECT CAST(reading AS INT64) FROM telemetry

Rejected:

    SELECT * FROM telemetry; SELECT * FROM other_table
    SELECT * FROM telemetry LIMIT -1
    SELECT * FROM telemetry WHERE a < b < c
    SELECT * FROM telemetry WHERE reading =
    SELECT * FROM telemetry WHERE id = $0
    SELECT * FROM telemetry WHERE id = :
    SELECT * FROM telemetry ORDER BY event_time

## 13. Implementation boundaries

The first implementation provides a standalone Rust lexer/parser and AST. It does not yet execute queries or resolve column names/types against a catalog. Those belong to M4.3 semantic validation and execution. The engine remains single-writer; query cancellation and result batches must use the existing bounded scan API. AQL never executes by constructing SQL for another database.


## 14. Native execution contract — initial M4.3 implementation

The Rust crate exposes execute and execute_source. A query is resolved against the open ANANTA Database catalog before the first batch is read. Unquoted table/column identifiers resolve case-insensitively; quoted identifiers resolve exactly. A qualified column is accepted only when the qualifier matches the one source table. Unknown names are rejected before iteration.

The executor consumes Database::scan_batches and evaluates its compiled expression tree over one owned input RecordBatch at a time. It does not implement another file reader and does not collect the complete table. Output is an owned AqlResultBatch with one typed value vector per output column. Every output batch contains no more than the requested scan batch size; LIMIT is applied globally after filtering. The iterator yields one error and then stops.

The first evaluator supports projection expressions, literals, typed named/positional parameters, unary signs and NOT, arithmetic, strict comparisons, AND/OR, IS NULL/IS NOT NULL and the initial explicit numeric casts. It uses three-valued boolean logic. A WHERE predicate retains only Boolean TRUE; FALSE and NULL/UNKNOWN are filtered. Integer overflow, division/modulo by zero, non-finite FLOAT64 arithmetic, invalid casts and operand type mismatches are execution errors.

The public error code set is InvalidQuery, TableNotFound, ColumnNotFound, TypeMismatch, InvalidParameter, InvalidCast, Arithmetic, Cancelled and Storage. Source spans are preserved for parser/semantic errors where available. Cancellation uses an optional shared atomic flag and is checked between input batches and rows.

M4.2 also defines a logical plan tree in semantic order: Scan, optional Filter, Project, optional Limit. The initial executor currently compiles that structure directly into a bounded scan plus expression plan; physical predicate pruning, vectorised operators and cost-based planning remain M5 work.

This implementation is single-table and read-only. Timestamp range predicates are ordinary typed TIMESTAMP_MICROS comparisons (for example >= lower AND < upper); there is no timezone parsing or index/Weave-zone-map pruning yet. Execution is native and uses ANANTA's own Database API; it never generates SQL for an external database.
