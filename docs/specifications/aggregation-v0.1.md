# Aggregation and Grouping Contract v0.1

**Status:** M5.2 implementation baseline  
**Crate:** `ananta-execution`

## 1. Scope

M5.2 adds blocking aggregate and GROUP BY execution over the existing pull-based execution operator interface. It consumes bounded input batches from upstream operators and stores one aggregate state per distinct group. It does not add ORDER BY, joins, HAVING, windows, spill-to-disk, or AQL syntax.

## 2. Aggregate functions

| Function | Input | Output | NULL behavior |
|---|---|---|---|
| COUNT(*) | no input column | INT64 NOT NULL | counts every row |
| COUNT(column) | any supported type | INT64 NOT NULL | ignores NULL |
| SUM(column) | INT32 / INT64 | INT64 NULLABLE | ignores NULL; NULL for no non-NULL values |
| SUM(column) | FLOAT64 | FLOAT64 NULLABLE | ignores NULL; NULL for no non-NULL values |
| MIN(column) | any ordered scalar | same type NULLABLE | ignores NULL; NULL for no non-NULL values |
| MAX(column) | any ordered scalar | same type NULLABLE | ignores NULL; NULL for no non-NULL values |
| AVG(column) | INT32 / INT64 / FLOAT64 | FLOAT64 NULLABLE | ignores NULL; NULL for no non-NULL values |

BINARY values support lexicographic MIN/MAX. FLOAT64 MIN/MAX use Rust's total ordering so results are deterministic for infinities, signed zero and NaN payloads. Aggregate output descriptors are checked against the above return-type and nullability contract before any input is consumed.

## 3. Integer overflow

Integer SUM accumulates in checked signed 128-bit state. On finalisation, it must fit in INT64; otherwise the operator returns `ExecutionError::AggregateOverflow`. COUNT uses checked unsigned 64-bit state and must fit in INT64 at finalisation. Internal arithmetic overflow is an execution error, never a wrapping result.

## 4. Floating-point behavior

FLOAT64 SUM and AVG process values in source-batch and row order. Given the same input order, results are reproducible; this is not an order-independent or compensated summation guarantee. The implementation follows IEEE-754 arithmetic: infinities and NaN propagate according to ordinary floating-point operations. AVG divides the accumulated sum by the count of non-NULL values.

## 5. Group key identity and output order

A group key is the ordered tuple of selected input columns. NULL values form a normal group-key value; equal tuples are combined. Typed encodings distinguish values of different types and length-prefix UTF8/BINARY payloads. Floating group-key encoding canonicalises signed zero and NaN so logically equivalent special values do not create separate groups.

Groups are stored by canonical encoded key in a BTreeMap. This provides deterministic output order for a fixed encoding version and input type schema. It is not a promise of SQL ORDER BY value ordering; query ordering remains a separate operator/planner feature.

With an empty grouping-column list, the operator performs a scalar aggregate and returns exactly one row even when the input is empty: COUNT returns zero and SUM/MIN/MAX/AVG return NULL. With one or more grouping columns, empty input returns zero groups.

## 6. Memory and batching

GROUP BY is a blocking operator: it consumes its input once before returning the first output batch. Its distinct-group state is checked against `ExecutionConfig.max_working_set_bytes` as groups are created. The estimate includes encoded keys, copied key values and aggregate accumulator payload sizes; it is a conservative logical estimate, not allocator RSS or a spill guarantee. Exceeding the budget returns a resource-limit error rather than continuing unbounded growth.

Output batches obey `max_batch_rows` and `max_batch_cells`. The operator emits deterministic, bounded batches and supports cooperative cancellation while consuming input and while materialising output. There is no spill-to-disk in v0.1; high-cardinality workloads must fit the configured group-state budget.

## 7. Validation coverage

Tests cover COUNT(*), COUNT(column), integer and float SUM, MIN, MAX, integer/float AVG, NULL skipping, scalar aggregate over empty input, grouped aggregate over empty input, checked integer overflow, invalid result types, group-state memory limits, deterministic group iteration and bounded output batches.

## 8. Non-goals

- No SQL/AQL parser or planner integration in M5.2.
- No ORDER BY guarantee.
- No partitioned/parallel aggregation or spill-to-disk.
- No approximate aggregates or sketches.
- No join, HAVING, window or time-series bucket operations.
