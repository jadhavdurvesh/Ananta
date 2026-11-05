# ANANTA Catalog ANC1 v2 — Binary Format and Migration

**Status:** Experimental implementation specification for M1.2.2  
**Magic:** `ANC1`  
**Version:** 2  
**Byte order:** Little-endian  
**Integrity:** FNV-1a 64-bit accidental-corruption checksum only; not authentication  
**Maximum file size:** 64 MiB

## 1. Scope

Catalog v2 persists stable database-local 64-bit table and column ObjectIds, a database-wide catalog revision, a per-table schema revision and the next object ID to allocate.

The canonical path remains `catalog/catalog.anc`. The existing v1 catalog is still recognised and automatically migrated on open. ANF v0.1 segment bytes and the existing `data/<table-name>/` directory layout are unchanged.

## 2. Header

The header is exactly 28 bytes.

| Offset | Size | Field |
|---:|---:|---|
| 0 | 4 | Magic: ASCII `ANC1` |
| 4 | 2 | Catalog version: u16, value 2 |
| 6 | 2 | Mandatory feature flags: u16, currently zero |
| 8 | 8 | Database-wide catalog revision: u64 |
| 16 | 8 | Next object ID: u64 |
| 24 | 4 | Table count: u32 |

## 3. Table records

Table records are emitted in ascending UTF-8 table-name byte order by the current BTreeMap catalog.

| Field | Size |
|---|---:|
| Table ObjectId | u64 |
| Table name length | u32 |
| Table name | variable UTF-8 bytes |
| Schema revision | u64 |
| Column count | u32 |

Table ObjectIds and column ObjectIds share one database-local namespace. Zero is invalid. IDs are never reused. The next-object-ID value must be nonzero and greater than every allocated ID.

## 4. Column records

Columns are emitted in logical schema order.

| Field | Size |
|---|---:|
| Column ObjectId | u64 |
| Column name length | u32 |
| Column name | variable UTF-8 bytes |
| Data type code | u8 |
| Nullable flag | u8 |

The column ordinal is represented by record order. A future format can make ordinals explicit if the catalog needs sparse or tombstoned columns.

## 5. Footer

The footer is 12 bytes:
- FNV-1a 64-bit checksum over every preceding byte, encoded little-endian.
- ASCII end marker `CEND`.

The checksum detects accidental corruption only. It is not resistant to malicious modification.

## 6. Defensive validation

The reader validates before allocating or trusting fields:
- Maximum catalog file size (64 MiB).
- Minimum header + footer length.
- Magic, version and mandatory feature flags.
- Trailer marker and checksum.
- Table and column counts bounded by remaining file bytes and the global object limit.
- Nonzero and unique table/column IDs.
- Nonzero schema revisions.
- Next object ID greater than all allocated IDs.
- Valid UTF-8 names and supported data-type codes.
- Nullability flags exactly 0 or 1.
- Valid schemas, unique table names and unique column names.
- Exact consumption of the catalog body; no trailing bytes.

Unknown catalog versions return an explicit unsupported-version error. Unknown mandatory feature flags are rejected.

## 7. V1 migration

When opening a v1 catalog:
1. Read and validate the complete v1 file, including checksum and end marker.
2. Decode schemas in the legacy catalog's deterministic table-name order.
3. Assign IDs starting at 1: each table ID followed by its column IDs in schema order.
4. Set every migrated table schema revision to 1.
5. Increment catalog revision once for each migrated table.
6. Set next-object-ID to the first unused ID.
7. Encode catalog v2 in memory.
8. Decode the just-encoded bytes and validate all invariants.
9. Create `catalog.anc.tmp` with exclusive create semantics.
10. Write all bytes, sync the temporary file, close it, then rename it over the canonical catalog path.
11. If writing or rename fails, remove only the temporary file created by this attempt. Never delete or truncate the original catalog.

If a pre-existing `catalog.anc.tmp` exists, migration fails without touching either it or the original v1 catalog. The same rule applies to ordinary catalog writes.

The atomic replacement and durability guarantees depend on host filesystem/platform semantics. Parent-directory syncing and systematic crash fault-injection remain part of the later safe-publication work.

## 8. Compatibility boundaries

- ANC1 v1 is read only for migration; all new writes use v2.
- V2 does not change the ANF v0.1 segment layout.
- Table and column IDs are not yet embedded in segment files.
- The reader still requires the exact table schema shape expected by existing ANF v0.1 segments.
- Table/column rename, drop and schema alteration are not exposed by this implementation.
- The existing data directory remains keyed by table display name. Table rename is therefore deferred until a separate directory migration design exists.

## 9. Current implementation APIs

The embedded Rust API exposes:
- `Database::catalog_revision()`
- `Database::table_id(table)`
- `Database::column_id(table, column)`
- `Database::table_schema_revision(table)`

`ObjectId` is a public newtype around u64. IDs are stable across close/reopen and deterministic during legacy migration. The runtime does not yet provide rename/drop/schema-evolution operations.

## 10. Required validation

The catalog implementation must test:
- V2 golden-byte round trip.
- Stable IDs and revisions after reopen.
- Unique IDs across tables and columns.
- V1 migration preserving table schemas and existing ANF segments.
- Migration failure preserving the exact v1 bytes and any pre-existing temp file.
- Zero/duplicate IDs and invalid revisions.
- Unsupported versions and flags.
- Count/length bounds, checksum, footer and trailing-byte rejection.
- ID and revision counter overflow.
- Failed catalog write not advancing in-memory catalog state.
