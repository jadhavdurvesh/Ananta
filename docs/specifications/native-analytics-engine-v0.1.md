# ANANTA Native Analytics Engine v0.1 — M5 Contract

Status: implementation target for M5; experimental single-node engine.
Storage: ANF/Chronicle/Capsule/Weave through ANANTA native APIs.
Execution: bounded columnar batches, no external query engine.

## 1. Goals and exclusions

M5 adds a native physical operator layer, bounded vector batches, projection/filter/limit, aggregation/grouping, time-series utilities and a correctness-first planner. M5 is not a production query engine and does not add joins, window functions, distributed execution, spill-to-disk aggregation, transactions, MVCC or a server protocol.

Every operator owns its input and output batches. A returned batch is immutable to downstream operators and can be retained after the next call. Memory use is bounded by configured batch-cell, output-cell, operator-memory and group-cardinality limits. Operators fail with explicit errors when a limit is reached; they do not silently drop rows.

## 2. Vector batch

A VectorBatch contains:
- one Column descriptor per output vector;
- one immutable shared value slice per column;
- a row_count shared by every vector.

All columns must have exactly row_count values. Null is represented by Value::Null. A batch cannot exceed 65,536 rows or 1,000,000 cells by default. Projection shares immutable value storage; filter and limit produce new compacted selections. No batch borrows the Database or a decoder after it is returned.

## 3. Operator interface and lifecycle

Each PhysicalOperator exposes its output schema and next_batch. next_batch returns either one owned VectorBatch, end-of-stream, or one terminal error. After an error the operator is terminal. Inputs are consumed in pull order. Cancellation is checked between batches and at row work-unit boundaries. Operators report actual input/output row and batch counters, not estimates.

Initial operators:
- Scan: owns a bounded Database::scan_columns_batches iterator.
- Projection: evaluates selected column expressions and preserves projection order.
- Filter: evaluates a typed predicate and compacts rows whose value is TRUE; FALSE and NULL/UNKNOWN are discarded.
- Limit: enforces one global row budget across child batch boundaries.

## 4. Resource accounting

Execution options include maximum batch rows/cells, estimated retained bytes, aggregate groups and aggregate state bytes. Value bytes are measured as the owned payload length plus fixed per-value/descriptor accounting; this is a conservative in-memory estimate, not allocator RSS. Scan reports rows and batches. Resource exhaustion, cancellation, invalid schemas and arithmetic failures have stable operator error kinds. There is no spill-to-disk path in M5.

## 5. Aggregate states and null policy

Aggregate definitions:
- COUNT(*) counts every input row.
- COUNT(expr) counts non-NULL values.
- SUM(expr) preserves INT32/INT64/FLOAT64 input type and uses checked integer arithmetic.
- MIN/MAX support ordered non-NULL BOOLEAN, INT32, INT64, FLOAT64, UTF8, TIMESTAMP_MICROS and BINARY values.
- AVG accepts numeric values and returns FLOAT64.

For SUM/MIN/MAX/AVG, NULL inputs are ignored; a group with no non-NULL values returns NULL. COUNT returns zero for an empty input. Ungrouped aggregation over an empty input emits one result row; grouped aggregation over an empty input emits no groups.

Integer SUM overflow is an error. Floating SUM is accumulated in deterministic input order with compensated summation. Any non-finite input/result is rejected. AVG returns NULL when there are no non-NULL values.

Group keys use typed equality: NULL equals NULL for grouping; float keys use IEEE total ordering with canonical NaN rejected. Output group order is deterministic lexicographic key order. The group-count and estimated-state-byte limits are checked before adding a new group.

## 6. Time-series semantics

All time-series values use TIMESTAMP_MICROS: signed microseconds since 1970-01-01T00:00:00Z. There are no local-time or daylight-saving conversions in M5.

TIME_BUCKET(width_micros, timestamp, origin_micros=0) uses a positive fixed-width duration and mathematical floor alignment:
  origin + floor((timestamp - origin) / width) * width
with Euclidean division for negative timestamps. Width must be positive; overflow is an error. Buckets are half-open [start, start + width). Missing intervals are not synthesized by default. Late-arriving records are assigned by event timestamp, not ingestion order.

Bucketed COUNT/SUM/MIN/MAX/AVG are implemented by adding the bucket-start timestamp as a grouping key and applying the same aggregate state rules.

Latest-value-per-series groups by the requested series key columns and chooses the row with the greatest event timestamp. Equal timestamps use later input order as the deterministic tie-break. Late arrivals with older timestamps do not replace the latest row.

## 7. Planner and optimisation

The planner separates:
- Logical plan: source scan, filter, projection, aggregate/group, limit.
- Physical plan: bounded scan with a required-column projection, filter operator, projection operator, aggregate operator and limit operator.

Projection pushdown computes the union of columns referenced by output expressions, predicates and group keys, then requests only those columns from the storage scan. Predicate pushdown places row filtering directly above scan before downstream projection/aggregation. The initial cost model uses catalog/schema shape and measured table statistics when available; absent statistics are marked unknown rather than fabricated.

Capsule time pruning uses existing verified Navigation Map range scans only when the request can be represented by exact supported range predicates. The map remains an advisory metadata layer: final row predicates are still evaluated. The existing scan_capsules API currently materialises the selected result; this M5 prototype therefore applies a configured maximum materialised-row cap and does not claim a bounded streaming Capsule scan. Ordinary AQL table scans remain bounded through scan batches.

Explain output includes logical/physical nodes, required columns, estimated rows, whether statistics were known, and enabled optimisations. Profiling records actual rows/batches in/out, decoded value bytes estimate and elapsed wall time. Estimates and measured counters are clearly separated.

Optimised and optimiser-disabled paths must return identical ordered rows for operators whose ordering is defined; grouped aggregate output uses deterministic key order. Optimisations may change work performed, never query semantics.

## 8. Acceptance and non-goals

M5 acceptance requires operator contract tests, aggregate edge/reference tests, time-series boundary/late-arrival tests, optimised/unoptimised equivalence tests and a native example with explain/profile output.

No claim is made for disk-spilled hash aggregation, SIMD, parallel execution, stable production workload speedups or reduced physical I/O where the current storage API still reads/fingerprints full Capsule files.
