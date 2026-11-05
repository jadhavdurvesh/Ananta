# 0005: Schema evolution requires immutable snapshots and segment identity

- **Status:** Proposed for M1.2.3.1
- **Date:** 2026-11-05
- **Scope:** Compatibility design only; no DDL API is enabled by this ADR

## Context

ANC1 catalog v2 now stores stable table/column ObjectIds and a current schema revision. ANF v0.1 segments, however, contain only positional column metadata; they do not identify a table, column ObjectId or schema revision. Changing the current schema without preserving the segment's original interpretation would make old data ambiguous.

## Proposed decision

Adopt immutable schema snapshots and explicit segment-level schema identity:

1. Each logical table schema revision is an immutable, checksummed snapshot keyed by table ObjectId and schema revision.
2. ANF v0.2 should carry table ObjectId, schema revision and stable column ObjectIds in its header/directory.
3. ANF v0.1 remains readable as a legacy encoding against the table's initial revision-1 snapshot.
4. The reader resolves each segment using its own historical schema, then maps stored values by ColumnId into the current logical schema.
5. Initially support only operations with straightforward compatibility: add nullable columns (old segments synthesize NULL), rename columns and reorder columns.
6. Defer table rename until data directories no longer depend on display names; defer drop/type changes/non-null additions until tombstones, backfill and compaction semantics are specified.
7. Publish a new immutable snapshot before publishing a catalog pointer to it; on catalog publication failure, the old schema remains active and the unreferenced snapshot is recoverable as an orphan.

## Consequences

- The next implementation work must define schema-snapshot and ANF v0.2 binary formats before exposing DDL.
- A new catalog format version may be needed to register snapshot history; ANC1 v2 must not be silently extended with incompatible bytes.
- Existing v0.1 segments need not be rewritten immediately.
- Reads can contain mixed segment revisions and require a resolver.
- Compaction eventually rewrites legacy segments into the current revision and can reclaim old snapshots only after verified promotion.
- Table rename/drop and type changes remain unavailable until their storage and recovery semantics are implemented.

## Validation

Before the first schema-altering API is enabled, tests must prove:
- Old v0.1 segments and new v0.2 segments can be read together.
- Nullable-column addition synthesizes NULL for old segments and preserves actual values for new segments.
- Column identity survives rename/reorder.
- Missing/corrupt snapshots fail closed.
- Injected failures never expose a catalog revision that references an invalid or missing snapshot.
