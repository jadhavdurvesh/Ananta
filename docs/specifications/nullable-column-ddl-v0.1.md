# ANANTA Nullable Column DDL — First Schema Change Contract

**Status:** M1.2.3.4 first operation proposal and implementation target  
**API:** `Database::add_nullable_column(table, name, data_type)`  
**Compatibility:** ANC1 catalog v2 + ANS1 snapshot v1 + ANF v0.1/v0.2  
**Scope:** Add one nullable column only

## 1. Purpose

This is the first schema-changing operation permitted in ANANTA. It validates the snapshot-first publication protocol and the mixed-segment resolver without requiring backfill or physical rewrite.

The operation adds a column whose value is NULL for every existing segment that predates the new ColumnId. New inserts can supply a real value or NULL.

## 2. API contract

Proposed Rust API:

```rust
let new_column_id = db.add_nullable_column(
    "telemetry",
    "humidity",
    DataType::Float64,
)?;
```

Inputs:
- Existing table name.
- New, non-empty column name unique under ANANTA's current case-insensitive comparison.
- A supported logical DataType.

The operation always creates the column with `nullable = true`. There is no default expression or backfill in this first API.

Return value: the newly allocated stable ColumnId.

Errors:
- Table does not exist.
- Column name is empty/invalid or already exists (case-insensitive).
- The current schema snapshot is missing, corrupt or inconsistent.
- ObjectId, schema revision or catalog revision counter overflows.
- Snapshot or catalog file I/O/publication fails.
- Existing orphan snapshot at the proposed next revision conflicts with the requested schema.

## 3. State transition

For a table at schema revision N and catalog revision C:

1. Read the current table definition.
2. Load snapshot (TableId, N).
3. Verify snapshot schema and ordered ColumnIds exactly match the current catalog entry.
4. Validate the new column name and supported DataType.
5. Clone the catalog into a candidate state.
6. Allocate one new database-wide ObjectId from the candidate's next-object-ID counter.
7. Append a nullable column to the candidate schema and append its new ID.
8. Set table schema revision to N + 1 and database catalog revision to C + 1 using checked arithmetic.
9. Build and validate immutable snapshot (TableId, N + 1).
10. Persist the snapshot first using immutable exclusive-temp write/sync/rename.
11. Encode and validate the candidate ANC1 v2 catalog.
12. Persist the catalog via its exclusive temporary file, sync and same-directory rename.
13. Only after catalog publication succeeds, replace the live in-memory catalog with the candidate.

The table's logical schema changes atomically from the perspective of this single Database handle: before publication, the old schema is active; after publication, the new schema is active.

## 4. Failure semantics

- Failure before snapshot publication: old catalog and schema remain active.
- Failure after snapshot publication but before catalog publication: old catalog remains active; the new snapshot is an orphan/future revision, reported by the consistency checker.
- The failed operation does not advance in-memory ObjectId allocation, schema revision or catalog revision.
- A retry with the identical candidate schema may reuse the byte-identical orphan snapshot idempotently.
- A different candidate schema at the same next revision conflicts with the immutable snapshot. It must not overwrite or delete the orphan automatically.
- Recovery/cleanup of orphan revisions remains an explicit administrative action until segment references and safe reclamation are implemented.

The API requires the existing exclusive single-writer usage model. Multi-handle and cross-process DDL concurrency is not supported.

## 5. Read semantics after addition

For segments with historical schema snapshots that do not include the new ColumnId:
- ANF v0.1 segments resolve against the revision-one snapshot.
- ANF v0.2 segments resolve against their stored revision.
- The resolver maps old values by ColumnId into current logical order.
- The new nullable column is synthesized as `Value::Null`.

For new ANF v0.2 segments written at revision N + 1:
- The new ColumnId appears in the segment directory.
- Values are written using the new schema.
- Reads return the stored value or NULL according to the new schema's nullability.

No old segment is rewritten by this operation.

## 6. Required tests

- Add a nullable column to an empty table.
- Add a nullable column after ANF v0.1 rows exist; old rows return NULL for the new field.
- Add a nullable column after ANF v0.2 rows exist; historical v0.2 rows return NULL.
- New inserts after the change use the new schema revision and preserve provided values.
- Mixed v0.1, v0.2 old-revision and v0.2 new-revision segments scan correctly after close/reopen.
- Existing TableId and ColumnIds remain unchanged; new ColumnId is unique and monotonic.
- Schema revision and catalog revision increment exactly once.
- Duplicate column names are rejected case-insensitively.
- Empty names and unsupported schemas are rejected.
- Missing/corrupt current snapshot prevents the operation.
- Injected catalog publication failure leaves the old catalog active and reports the new snapshot as orphaned.
- A conflicting orphan revision is not overwritten.
- A full scan and projected scan return current logical column order.

## 7. Explicit non-goals

This operation does not add:
- Non-null columns.
- Default expressions or backfill.
- Type alteration.
- Column rename or reorder.
- Column drop/tombstones.
- Table rename/drop.
- Physical compaction or segment rewrite.
- Multi-writer or concurrent DDL safety.

Those operations need their own compatibility, publication and recovery contracts.
