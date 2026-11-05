# ANANTA Schema Evolution and Segment Compatibility — Design Proposal

**Status:** M1.2.3.1 design proposal; implementation not started  
**Baseline:** ANC1 catalog v2 and ANF v0.1  
**Target:** Future ANF v0.2 + immutable schema snapshots  
**Scope:** Schema changes without losing the ability to read older segments

## 1. The compatibility problem

Catalog v2 stores stable table/column ObjectIds and a current schema revision. However, ANF v0.1 segments contain:
- Positional columns and type codes.
- No table ObjectId.
- No schema revision.
- No column ObjectIds.

The v0.1 reader therefore requires the exact schema shape used to write a segment. Merely changing the current catalog schema would make old segments ambiguous or unreadable.

**Rule:** no schema-changing operation may be exposed until the engine can identify the schema used by every segment and correctly map that segment's values into the current logical schema.

## 2. Design goals

- Preserve stable table and column identity across supported changes.
- Keep old data readable after additive and metadata-only schema changes.
- Permit new segments to use the new schema while old segments retain their original physical representation.
- Ensure compaction can eventually rewrite old physical data without changing logical query results.
- Keep existing ANF v0.1 files readable; no forced rewrite on upgrade.
- Fail closed when a segment's schema identity cannot be resolved.
- Never synthesize a value except where a documented schema rule allows it (initially nullable-column addition -> NULL).

## 3. Proposed components

### 3.1 Immutable schema snapshots

Every table schema revision is an immutable snapshot:
- Table ObjectId.
- Schema revision.
- Ordered columns with stable column ObjectIds, names, types and nullability.
- A canonical schema fingerprint.
- Format version and checksum.

Snapshots are stored separately from the mutable current catalog entry, for example:

```text
catalog/
  catalog.anc
  schemas/
    <table-object-id>/
      00000000000000000001.ancs
      00000000000000000002.ancs
```

The exact snapshot file layout is a later binary-format task. A snapshot is written and validated before the catalog publishes a new schema revision. Once published, snapshots are immutable. Do not delete a snapshot while any segment or retained generation can reference it.

### 3.2 ANF v0.2 segment identity

ANF v0.2 should extend the segment header/directory with:
- Table ObjectId.
- Schema revision.
- Stable Column ObjectId on every column directory entry.
- Explicit format feature flags.
- Existing row-count, payload boundaries and integrity validation.

This allows the reader to map stored columns by identity rather than assuming that ordinal position and name are permanent.

ANF v0.1 remains supported as a legacy format. Its segments are interpreted against the table's initial schema snapshot (revision 1), because the v0.1 format cannot carry another revision identity. The table's original schema snapshot must therefore be retained for the lifetime of all v0.1 segments.

### 3.3 Schema resolver

The read path resolves every segment independently:

1. Identify segment encoding version.
2. For ANF v0.1, resolve table revision 1 from the immutable initial schema snapshot.
3. For ANF v0.2, read table ObjectId + schema revision + column ObjectIds.
4. Load and validate that historical schema snapshot.
5. Decode stored values using the historical physical schema.
6. Map values by stable Column ObjectId into the current logical schema.
7. Apply only explicitly permitted compatibility rules.
8. Return values in current logical column order.

If the required snapshot is missing, the table ID/revision does not match, or a conversion rule is unsupported, return a structured schema-compatibility error. Never guess based on names.

## 4. Initial schema compatibility matrix

| Change | Existing segments | New segments | Initial policy |
|---|---|---|---|
| Add nullable column | Synthesize NULL for old segments where ID is absent | Write the new column | Support after resolver and ANF v0.2 pass |
| Rename column | Map old physical values by stable ColumnId | Use same ID with new display name | Support after resolver |
| Reorder columns | Map by ColumnId, not old ordinal | Write current logical order | Support after resolver |
| Drop column | Ignore old physical values for dropped ID | Omit dropped column | Defer until tombstone/history and compaction policy exist |
| Add non-null column to non-empty table | Requires backfill/default | Write new column | Reject until explicit default/backfill/rewrite semantics exist |
| Change data type | Requires conversion or rewrite | Use new type | Reject until conversion semantics and rewrite are implemented |
| Nullable -> non-null | Existing NULLs may exist | New writes enforce non-null | Reject until full-data validation and rewrite protocol exists |
| Rename table | Current data path is name-based | Path and catalog name must change | Defer until table-ID-based directories or recoverable directory migration exist |
| Drop table | Data directory and IDs need retention policy | No new writes | Defer until tombstones and physical reclamation are specified |

## 5. DDL publication protocol

A schema change must use a failure-safe publication sequence:

1. Validate requested change against the current snapshot.
2. Construct a new immutable schema snapshot with incremented schema revision.
3. Preserve every existing ObjectId; allocate new IDs only for genuinely new columns.
4. Write the new snapshot to a unique temporary file.
5. Sync, close, reopen/decode and validate the snapshot.
6. Publish the immutable snapshot without overwriting an existing revision.
7. Build a candidate catalog with the new current schema revision and incremented catalog revision.
8. Validate and atomically publish the catalog.
9. Only after catalog publication succeeds, expose the new schema to the live handle.

If the catalog publication fails, the previous catalog remains active. The newly written snapshot is an unreferenced orphan and can be reported by a consistency checker; it must not be treated as active schema. Recovery/cleanup rules must be idempotent.

## 6. Ingestion rules during and after schema changes

- Each newly written ANF v0.2 segment records the exact table ID and schema revision it uses.
- A batch is validated against one immutable schema snapshot captured at the start of the insert.
- A segment must not contain a mixture of schema revisions.
- Do not allow schema changes and inserts through multiple handles until the single-writer/concurrency model is enforced.
- Existing ANF v0.1 segments remain readable against revision 1.
- No segment is silently relabelled to a newer schema revision.

## 7. Compaction interaction

Compaction is the eventual path for rewriting old segments into a current physical representation:
- Read with the source segment's historical schema.
- Resolve values by ColumnId.
- Apply only supported compatibility rules.
- Write a new segment at the current schema revision.
- Verify logical row equivalence and counts.
- Promote a new generation only after validation.
- Retain original segments/snapshots until the new generation is verified and rollback retention expires.

Compaction is not required for the first nullable-column or column-rename release if the resolver supports mixed segment revisions safely.

## 8. Table directory identity

The current layout is `data/<table-name>/`. That makes table rename a filesystem operation and creates path coupling to the display name.

Preferred long-term direction:
`data/<table-object-id>/`

Do not switch directory layout in the same change as schema snapshots and ANF v0.2. A separate migration must:
- Detect legacy name-based paths.
- Move/copy data safely.
- Preserve the original directory until the new location is validated.
- Be restartable after interruption.
- Handle platform-specific rename semantics.
- Avoid path collisions and symlink traversal.
- Update diagnostic/storage inspection tools.

Until then, table rename stays unsupported.

## 9. Required test matrix

### Identity and snapshots
- Schema snapshot round trip and deterministic byte fixture.
- Same table/column IDs survive each supported operation.
- New column IDs are unique and monotonic.
- Snapshot revision is immutable and never overwritten.
- Missing/corrupt snapshot returns an explicit error.

### Mixed segment revisions
- Scan table with only ANF v0.1 revision-1 segments.
- Scan table with only ANF v0.2 current-revision segments.
- Scan a mixture of v0.1 and v0.2 segments.
- Verify old segments synthesize NULL for a newly added nullable column.
- Verify new segments preserve actual nullable/non-null values.
- Verify column rename preserves identity and data.
- Verify reorder returns current logical order.
- Verify dropped/unknown physical column handling only when drop is implemented.

### Failure handling
- Crash/failure while writing a schema snapshot.
- Failure after snapshot publish but before catalog publish.
- Failure during catalog publish.
- Failed operation leaves old schema active.
- Orphan snapshot is detected but not treated as active.
- Reopen after each injected failure yields a consistent catalog and readable data.

## 10. Recommended implementation order

1. Define and implement immutable schema snapshot model and byte format.
2. Add snapshot registry/history to catalog metadata without breaking ANC1 v2. If the current catalog format must change, use a new explicit catalog version and migration.
3. Define ANF v0.2 header/directory with table ID, schema revision and column IDs.
4. Implement dual-version ANF reader (v0.1 + v0.2).
5. Implement v0.2 writer using current schema snapshot.
6. Implement resolver to map historical segments to current logical schema.
7. Add nullable-column addition.
8. Add column rename and reorder.
9. Revisit table rename/drop/type changes after directory identity and compaction semantics are designed.

**No schema-changing API is approved by this document alone. Each supported operation must pass the mixed-revision and failure tests above.**
