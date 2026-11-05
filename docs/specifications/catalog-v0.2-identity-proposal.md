# ANANTA Native Catalog v0.2 — Identity and Schema Revision Proposal

**Status:** Design proposal for M1.2.1; no catalog v0.2 implementation yet  
**Current catalog:** ANC1 / version 1  
**Proposed catalog:** ANC1 / version 2  
**Scope:** Stable object identity, display names and schema revision semantics

## 1. Purpose

The current prototype identifies tables and columns primarily by their names and their position in a schema. That is sufficient for the first storage demonstration, but it is fragile for rename operations, schema evolution, query-plan references and future segment compaction.

Catalog v0.2 should introduce persistent object IDs independent of names and positions.

This document defines the proposed semantics only. The byte layout and migration implementation are a separate M1.2.2 task.

## 2. Object identity

### 2.1 ObjectId

Use a database-local unsigned 64-bit integer:

- Zero is reserved and invalid.
- IDs are allocated monotonically from a persisted `next_object_id`, initially 1.
- Table IDs and column IDs share one namespace and therefore cannot collide within a database.
- An ID is never reused, including after an object is dropped.
- Allocation must use checked arithmetic. Exhaustion returns an explicit error.
- Object IDs are stable across renames and catalog rewrites.
- IDs are scoped to one database directory; they are not globally unique across independent databases or copies.

A separate globally unique database identifier is not required in this milestone. Revisit only when database attach/import/replication requires it.

### 2.2 Tables

Each table has:
- `table_id: ObjectId`
- `name: String` (user-visible name)
- `schema_revision: u64`
- Ordered columns, each with its own ID.

Table names remain validated by the existing rules. A table rename changes only the display name and catalog revision; the table ID and data identity remain stable.

### 2.3 Columns

Each column has:
- `column_id: ObjectId`
- `name: String` (user-visible name)
- `data_type: DataType`
- `nullable: bool`
- A stable ordinal represented by its order in the table schema.

A column rename preserves its column ID. A column reorder changes ordinal but not identity. Column IDs are never recycled.

Names remain subject to the current schema rules: non-empty, no duplicate names under the current case-insensitive comparison, and exact type/nullability validation.

## 3. Revision model

Use two separate monotonic counters.

### 3.1 Catalog revision

A database-level `catalog_revision: u64` increments whenever catalog-visible metadata changes:
- Table create.
- Table rename.
- Table drop.
- Column/schema change.
- Future index or other catalog-object creation/drop.

It is useful for invalidating catalog caches and detecting stale metadata snapshots.

### 3.2 Schema revision

Each table has `schema_revision: u64`, starting at 1.

Increment it when the logical column schema changes:
- Column add/drop.
- Column rename.
- Column reorder.
- Data type or nullability change (if/when supported).

A table rename alone does not change schema revision because the table's logical columns are unchanged.

Revision increments use checked arithmetic. Overflow must return an error; revisions must never silently wrap.

## 4. Initial schema evolution policy

The catalog may record identities and revisions before the storage engine supports every evolution operation. The following policies avoid promising unsafe behaviour.

| Operation | Proposed v0.2 policy |
|---|---|
| Create table | Allocate a table ID and unique column IDs; schema revision = 1 |
| Rename table | Preserve table ID; increment catalog revision |
| Drop table | Remove catalog visibility; never reuse its ID; physical reclamation is deferred |
| Rename column | Preserve column ID; increment schema and catalog revisions |
| Reorder columns | Preserve column IDs; update ordinals and both revisions |
| Add nullable column | Defer until old-segment reads can synthesize NULL for missing column IDs |
| Add non-null column to non-empty table | Reject until an explicit default/backfill/rewrite protocol exists |
| Drop column | Defer physical reclamation until old segments and schema revisions are understood |
| Change data type | Reject unless a validated conversion/rewrite path is implemented |
| Change nullable to non-null | Reject while existing NULL values may exist; require validation/backfill first |

The initial catalog implementation should support create and persistent identity. Rename/drop operations should not be exposed until their atomic catalog update and data-directory semantics are implemented.

## 5. Segment relationship

Future ANF versions should identify the table and schema revision (or an equivalent schema fingerprint) used when the segment was written. Column blocks should identify columns by stable column ID, not only by ordinal/type.

This is not part of the current ANF v0.1 format. Do not add object IDs to v0.1 files in place; that requires an explicit ANF version change and migration/compatibility design.

Until that future format exists, the reader must continue requiring the exact schema shape used by the segment.

## 6. Catalog v2 migration requirements

Catalog v2 must not silently reinterpret catalog v1 bytes.

- A v1 catalog is detected by its version field.
- The v2 reader rejects unsupported versions with a specific error.
- A later migration tool may read v1, assign stable IDs deterministically in existing catalog order, set each table schema revision to 1, and write a v2 catalog to a temporary file.
- Migration must verify the new catalog checksum and decode it again before publication.
- The original catalog must remain intact until the v2 replacement is fully written and validated.
- Migration must be restartable or leave the original v1 catalog usable after failure.
- Data-directory renaming/movement is a separate decision. Do not move table directories as a side effect of catalog migration without a tested recovery plan.

## 7. Proposed catalog v2 logical record

The byte-level format will be specified in M1.2.2. Its logical fields should include:

**Header**
- Magic and version.
- Reserved/feature flags.
- Catalog revision.
- Next object ID.
- Table count.

**Table record**
- Table ID.
- Display name.
- Schema revision.
- Column count.

**Column record**
- Column ID.
- Display name.
- Type code.
- Nullability.
- Ordinal/order.

**Footer**
- Checksum and end marker.

All lengths, counts, IDs, revisions and offsets must be validated before allocation or use. Unknown mandatory feature flags and duplicate IDs must be rejected.

## 8. Required tests for implementation

- Create and reopen a table; table and column IDs remain unchanged.
- Create multiple tables; table and column IDs do not collide.
- Drop/rename semantics do not recycle IDs (when operations are introduced).
- Catalog revision increments on each catalog mutation.
- Schema revision increments only on logical schema changes.
- Table rename preserves schema revision.
- Counter overflow returns an error.
- Duplicate/zero IDs are rejected during catalog load.
- Unsupported catalog versions are rejected.
- V1 migration preserves schemas and data visibility if migration is introduced.
- Interrupted migration never destroys the last valid catalog.

## 9. Deferred decisions

- Whether object IDs should later become 128-bit.
- Database-global IDs for replication/attach operations.
- Tombstone retention and physical data reclamation.
- Full schema evolution and old-segment compatibility.
- Catalog journaling/WAL integration.
- Data-directory layout based on stable table IDs.
- Global catalog snapshots and concurrent DDL.

These decisions require the first v2 prototype and explicit tests. Do not expand M1.2.1 to solve them prematurely.
