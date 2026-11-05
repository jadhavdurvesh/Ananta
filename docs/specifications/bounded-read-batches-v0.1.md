# ANANTA Bounded Read Batches — M1.3.1 Contract

**Status:** M1.3 implementation target  
**API:** `Database::scan_batches`, `Database::scan_columns_batches` and row-limit variants  
**Storage compatibility:** ANF v0.1/v0.2 + ANC1 catalog v2 + ANS1 snapshot v1  
**Initial batch limit:** 1–65,536 rows per batch

## 1. Purpose

The existing `scan` and `scan_columns` APIs materialise the complete result in memory. The batch API must return one bounded columnar batch at a time and allow the caller to consume, process and discard it before requesting the next batch.

This is an additive API. Existing scan APIs and on-disk formats remain unchanged.

## 2. Public API

- `scan_batches(table, batch_rows)`: all current logical columns.
- `scan_columns_batches(table, columns, batch_rows)`: selected columns in caller order.
- `scan_batches_with_limit(table, batch_rows, row_limit)`: full projection with an early row limit.
- `scan_columns_batches_with_limit(table, columns, batch_rows, row_limit)`: projected scan with an early row limit.

Each returns `Result<ScanBatchIter, DbError>`. The iterator yields `Result<RecordBatch, DbError>`. Errors are returned once and stop the iterator; they are never silently skipped.

A `RecordBatch` is columnar: it owns a projected logical `Schema`, one value vector per projected column, and an explicit row count. Every value vector has exactly that row count. Batches do not share mutable buffers with the database or later batches.

## 3. Bounds and ordering

- `batch_rows` must be in the inclusive range 1..=65,536.
- A single returned batch contains no more than `batch_rows` rows.
- The optional row limit is applied across segment boundaries and may truncate the final batch.
- A zero row limit returns an empty iterator.
- Segment order remains the current deterministic lexicographic segment-file order.
- Column projection order is exactly the caller's order, including repeated requested columns.
- Empty projections are rejected rather than producing ambiguous zero-column batches.

## 4. Streaming decoder

A file-backed ANF batch decoder:
- Reads and validates the bounded header and column directory.
- Validates file size, version, flags, row/column counts, contiguous payload ranges, type codes and v0.2 identity metadata.
- Verifies the whole-segment checksum incrementally using a fixed-size scratch buffer before yielding any rows.
- Seeks to the selected column payloads and retains their row cursors between batches.
- Decodes only the next requested number of values from selected columns.
- Enforces nullability, boolean markers, UTF-8 validity, field-length bounds and payload boundaries.
- Supports both ANF v0.1 and v0.2 without changing their bytes.

The checksum pass reads the entire file sequentially but does not retain it in memory. The decoder uses a fixed 64 KiB checksum scratch buffer plus selected output values for the current batch.

## 5. Schema compatibility

For each segment:
- ANF v0.1 resolves against the immutable revision-one schema snapshot.
- ANF v0.2 resolves against its exact TableId/schema-revision snapshot after identity checks.
- Requested current columns are mapped to historical physical positions by stable ColumnId.
- If a requested current nullable column did not exist in the historical snapshot, the batch synthesises NULL for that column.
- Same-ID type mismatches, missing required historical values, wrong table IDs, future revisions and corrupt snapshots fail closed.
- Rename/reorder and nullable-column additions therefore preserve the current logical output schema without rewriting old segments.

## 6. Failure behaviour

- Segment metadata and checksum errors are returned before any rows from that segment are exposed.
- A payload error encountered while decoding a later batch is returned as an iterator error; iteration stops immediately afterward.
- Directory-entry and file-read errors are propagated.
- The iterator opens one segment at a time and drops its reader when the segment is exhausted.
- The API does not provide snapshot isolation against concurrent writers. The existing exclusive single-writer usage rule remains in force.

## 7. Required tests

- Batch sizes 1, exact row count, and row count + 1.
- Reject zero and greater-than-maximum batch sizes.
- Empty table and empty segments.
- Projected columns, reordered projection and repeated projection.
- Row limit 0, within one segment, exactly on a boundary and across multiple segments.
- Mixed v0.1/v0.2 segments and schema revisions after nullable addition, rename and reorder.
- Close/reopen before batch scanning.
- Checksum, directory, null marker, boolean, UTF-8 and payload truncation errors.
- Iterator returns an error once and never skips a bad segment.
- Verify no batch exceeds the configured row bound and no complete result collection is built by the iterator.

## 8. Non-goals

This increment does not add AQL, predicates, vectorised operators, parallel scans, mmap, physical column-range I/O guarantees, concurrent-writer safety or ChronoWeave Capsules/Weaves. Predicate and time-range pruning belong to later query/storage milestones.
