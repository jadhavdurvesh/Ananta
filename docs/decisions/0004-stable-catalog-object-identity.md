# 0004: Stable catalog object identity

- **Status:** Accepted for M1.2.1; implementation is underway in M1.2.2
- **Date:** 2026-11-05
- **Decision scope:** Native catalog identity semantics; implementation is deferred to M1.2.2

## Context

The current prototype catalog identifies tables by name and columns by name/order. Names can change, and positions can change as a schema evolves. A native engine needs stable identities so query plans, future indexes, segment metadata and schema evolution can refer to objects without depending on display names.

## Proposed decision

Introduce a database-local unsigned 64-bit `ObjectId` namespace shared by table and column objects:
- Zero is reserved.
- Allocate monotonically from a persisted next-object-ID counter.
- Never reuse IDs after drop.
- Keep names as user-visible labels, separate from identity.
- Keep IDs stable over renames and catalog rewrites.

Maintain a global catalog revision and a per-table schema revision. Catalog revision changes for any catalog-visible mutation; schema revision changes for logical column-schema changes. A table rename changes catalog revision but not schema revision.

The exact catalog v2 byte layout, migration and implementation are separate tasks. The current ANC1 v1 catalog must never be silently reinterpreted as v2.

## Consequences

- AQL and future query plans can resolve display names to stable IDs.
- Future ANF versions can identify table/schema/column metadata independently of names and ordinal positions.
- Schema-evolution operations must define compatibility for old segments before being exposed.
- IDs and revisions need checked allocation/increment semantics and corruption validation.
- This proposal does not yet change the current native catalog or data files.

## Safety policy

Until catalog v2 and segment schema identity are implemented:
- Require the exact schema shape expected by existing ANF v0.1 segments.
- Do not expose table/column rename, drop or schema alteration APIs.
- Reject non-null schema additions to non-empty tables until a default/backfill/rewrite protocol is designed.
- Keep physical data reclamation and directory moves outside the catalog identity change.

## Validation required before implementation is accepted

Catalog v2 must round-trip IDs and revisions; reject zero/duplicate IDs and unsupported versions; check counter overflow; and preserve the last valid v1 catalog if a migration attempt fails.
