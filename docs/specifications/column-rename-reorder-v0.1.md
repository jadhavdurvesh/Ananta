# ANANTA Column Rename and Reorder — Schema Change Contract

**Status:** M1.2.3.4.2 implementation target  
**APIs:** `Database::rename_column(table, old_name, new_name)` and `Database::reorder_columns(table, ordered_names)`  
**Compatibility:** ANC1 catalog v2 + ANS1 snapshot v1 + ANF v0.1/v0.2  
**Scope:** Rename one existing column and reorder the complete column list

## 1. Purpose

This is the next schema-evolution increment after nullable-column addition. It changes logical names and ordering without changing the identity, type or nullability of any column, and without rewriting data segments.

The schema snapshot for each revision records the display names and logical order for that revision. Segment compatibility continues to depend on stable TableId and ColumnIds, not display names or column positions.

## 2. API contracts

### Rename

```rust
let column_id = db.rename_column("telemetry", "device", "sensor")?;
```

- The table and source column must exist.
- The new name must satisfy the existing `Schema::new` column-name rules.
- Names are unique case-insensitively, consistent with `Schema::new`.
- Renaming to another column's name returns `DbError::ColumnExists`.
- The returned ObjectId is the original stable ColumnId.
- An exact same-name request is a no-op and does not advance revisions. A case-only spelling change is a real rename.

### Reorder

```rust
db.reorder_columns(
    "telemetry",
    &["timestamp", "temperature", "sensor"],
)?;
```

- The requested list must contain every current column name exactly once.
- Names are matched against their current display spelling.
- Unknown names return `DbError::ColumnNotFound`.
- Duplicated, incomplete or extra orderings return `DbError::InvalidColumnOrder`.
- The operation changes logical schema order and the corresponding ColumnId vector in lockstep.
- The requested order, if already active, is a no-op and does not advance revisions.

## 3. Common state transition

For either operation at table schema revision N and database catalog revision C:

1. Resolve the current table and load its exact (TableId, N) immutable snapshot.
2. Verify snapshot TableId, revision, schema and ordered ColumnIds exactly match the current catalog definition.
3. Validate the requested rename or complete ordering without mutating live state.
4. Clone the catalog into a candidate state and apply the name/order change while preserving every ObjectId, data type and nullability bit.
5. Increment the table schema revision to N + 1 and catalog revision to C + 1 using checked arithmetic.
6. Build and validate the immutable (TableId, N + 1) snapshot.
7. Publish the snapshot first using exclusive temp-file creation, sync and rename.
8. Publish the candidate ANC1 v2 catalog using its existing safe temp-file protocol.
9. Replace the in-memory catalog only after catalog publication succeeds.

A no-op leaves the catalog bytes, schema revision, catalog revision and snapshot directory unchanged.

## 4. Failure semantics

- Missing, corrupt or catalog-inconsistent current snapshots reject the operation.
- Failure before the new snapshot is published leaves the old schema active.
- Catalog publication failure after snapshot publication leaves the old in-memory and on-disk catalog active; the future snapshot is reported as an orphan.
- Identical retries may reuse the byte-identical immutable snapshot.
- A conflicting snapshot for the same next revision is rejected and never overwritten or deleted.
- Checked counter overflow is rejected before snapshot publication.
- These operations retain the existing exclusive single-writer assumption. Concurrent DDL or multi-process coordination is not introduced.

## 5. Read and write compatibility

- ANF v0.1 segments continue to resolve against the revision-one snapshot.
- ANF v0.2 segments continue to resolve against their exact historical snapshot.
- Resolution maps physical values to the current logical schema by ColumnId; names and historical positions are not used as identity.
- A rename changes the current display name, not the ID. Old segments and old snapshots remain valid.
- A reorder changes logical order and the current catalog's parallel ColumnId order. Historical segment values are mapped into the new order by ID.
- New ANF v0.2 segments carry the new schema revision and its matching ordered ColumnIds.
- Full scans return current logical order. Projected scans return the caller's requested projection order.

No historical snapshot or data segment is rewritten.

## 6. Required tests

- Rename preserves the original ColumnId and changes schema lookup/projection names.
- Historical v0.1 and v0.2 segments remain readable after rename.
- Reorder is a complete permutation, preserves each name-to-ID association and changes full-scan logical order.
- Historical v0.1 and v0.2 segments remain readable after reorder.
- New v0.2 writes use the new revision and ordered ColumnIds.
- Rename followed by reorder and close/reopen preserves all logical results.
- Duplicate/case-insensitive rename, empty name, missing table/column and invalid orderings are rejected without changing revisions.
- Exact rename and current-order requests are no-ops.
- Missing, corrupt and catalog-inconsistent current snapshots reject both operations.
- Catalog publication failure leaves the old catalog active and reports the next snapshot as orphaned.
- Identical retry succeeds; a conflicting orphan revision is not overwritten.
- Schema/catalog revision overflow is rejected without partial publication.

## 7. Explicit non-goals

This increment does not add column drop/tombstones, table rename/drop, type changes, non-null additions, defaults/backfill, physical segment rewrite/compaction, concurrency control, WAL or transactions.
