# Vector Execution Contract v0.1

**Status:** M5.1 implementation baseline  
**Crate:** ananta-execution  
**Milestone:** M5.1 — Execution model

## 1. Purpose and boundaries

The execution crate supplies reusable pull-based operators over ANANTA's existing native storage batches. It does not introduce a second storage format, replace the database scanner, or claim transaction isolation. Storage remains responsible for ANF/Chronicle decoding and schema-history resolution. Execution owns operator composition, typed vector-batch validation, filter semantics, cancellation checks and per-batch resource accounting.

The canonical execution batch is an alias of ananta_core::RecordBatch. Its columns are owned, typed vectors in caller-visible order. A batch may contain duplicate or aliased output column names where a projection explicitly requests them. A batch has one row count shared by every column; every vector length must equal that count.

## 2. Vector schema and validation

VectorSchema contains ordered Column descriptors. It requires at least one non-empty column name but permits duplicates for projected output. For every batch, validation requires:

- Exactly one vector per schema column, in the same order.
- Exact column descriptor agreement (name, type and nullability).
- One value per row in every vector.
- Every non-NULL value to match the declared data type.
- NULL values only in nullable columns.

Storage schema snapshots and stable ColumnIds remain owned by ananta-core; this execution schema is an operator-local view, not a replacement catalog.

## 3. Pull-based operator contract

Every operator implements:

~~~rust
pub trait ExecutionOperator {
    fn schema(&self) -> &VectorSchema;
    fn next_batch(
        &mut self,
        context: &mut ExecutionContext,
    ) -> Result<Option<VectorBatch>, ExecutionError>;
}
~~~

One call returns at most one owned batch. None means end-of-stream. Operators are composed by passing an upstream operator as a boxed input. A source must not eagerly materialise the complete table.

### Initial operators

- **ScanOperator:** adapts Database::scan_batches; the storage engine remains the file decoder and bounded scan implementation.
- **ProjectionOperator:** selects columns by input index, preserves caller order, supports aliases and repeated source columns, and rejects invalid indices, incompatible types or narrowing nullable input to NOT NULL.
- **FilterOperator:** evaluates a typed predicate tree and retains only TRUE rows. FALSE and UNKNOWN rows are discarded, matching WHERE-style three-valued logic.
- **LimitOperator:** enforces one global row limit across all input batches and stops pulling once the limit is reached.

## 4. Predicate semantics

The first predicate representation supports constants, comparisons, IS NULL / IS NOT NULL, AND, OR and NOT. Comparisons require compatible types. Supported ordering is type-preserving for BOOLEAN, INT32, INT64, TIMESTAMP_MICROS, FLOAT64, UTF8 and BINARY. Comparisons involving NULL evaluate to UNKNOWN. A FLOAT64 comparison involving NaN also evaluates to UNKNOWN because no partial ordering exists. Logical operators use three-valued truth tables; only TRUE passes a filter.

This is an execution primitive, not a complete AQL expression compiler. The AQL planner's existing compiled expressions remain separate until the planned M5.4 integration.

## 5. Ownership and lifetime

Batches are owned values; no operator exposes borrowed slices beyond a call. The storage scan yields one owned batch at a time. Projection and filter create owned output vectors, so they may temporarily retain both an input and output batch. Limit truncates the owned input in place. No operator stores an unbounded history of batches.

This first version intentionally uses cloning for projection/filter values. Reference-counted buffers, selection vectors and zero-copy views are deferred until profiling demonstrates that the copy cost justifies a more complex lifetime model.

## 6. Cancellation and errors

ExecutionContext carries a clonable atomic cancellation token. Every operator checks it before pulling or emitting work. Cancellation returns ExecutionError::Cancelled. Storage failures are preserved as ExecutionError::Storage; malformed batches, invalid plans, type mismatches, resource-limit failures and checked-size arithmetic overflow have distinct variants.

An operator error is terminal for the current pipeline. Callers should discard the pipeline after an error rather than resume it.

## 7. Resource accounting

Default limits are:

| Limit | Default |
|---|---:|
| Rows per batch | 65,536 |
| Cells per batch | 1,000,000 |
| Operator working set | 64 MiB |

The byte estimator includes vector capacities, column descriptors and names, the Value vector capacity, and owned UTF8/BINARY payload capacities. For transforming operators, the working-set check includes both input and output estimates. This is a conservative payload estimate, not allocator RSS: allocator metadata, stack memory, file buffers and process-wide memory are outside this contract.

Metrics record the sum of output batches/rows at operator boundaries and the peak estimated batch and operator working-set bytes. In a multi-operator pipeline, the output counters count each stage's output; they are not the final query row count.

## 8. Non-goals and known limitations

- No joins, aggregation, sorting, window functions or physical query optimisation.
- No automatic predicate/projection pushdown.
- No zero-copy batch ownership or spill-to-disk.
- No global query memory accounting beyond the batch/transition budget.
- No transactional snapshot isolation or multi-writer support.
- No AQL planner integration in M5.1.

## 9. Validation

The crate tests cover vector-schema validation, wrong types and nullability, a filter → projection → limit pipeline, duplicate projections, NULL-aware comparison, IS NULL, invalid predicate references, type mismatch, cancellation and row/working-set ceilings. The workspace CI remains the authoritative formatting, test and Clippy gate.
