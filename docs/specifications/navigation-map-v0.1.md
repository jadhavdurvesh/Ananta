# Navigation Map Format and Pruning Contract — v0.1

**Status:** M2.4 prototype  
**Extension:** .nvm  
**Scope:** capsule metadata index and safe range-pruning diagnostics

## 1. Role

A Navigation Map is an immutable, checksummed metadata generation for the exact set of Capsule files in one table directory. It contains copied Capsule identity, lineage, file fingerprint, temporal bounds and selected-column statistics. It does not contain row data.

Capsule files remain authoritative. A map may reject a Capsule only when its metadata proves no row can satisfy the query. Missing statistics always retain the Capsule.

## 2. File format

All integers are little-endian.

### 32-byte header

| Offset | Bytes | Field |
|---:|---:|---|
| 0 | 4 | Magic NVM1 |
| 4 | 2 | Version 1 |
| 6 | 2 | Flags (zero) |
| 8 | 8 | TableId |
| 16 | 8 | Map generation |
| 24 | 4 | Entry count |
| 28 | 4 | Reserved zero |

Each record is preceded by a 4-byte record length. The fixed record body is 164 bytes followed by one 48-byte descriptor per Capsule column.

The fixed record stores CapsuleId, Capsule generation, file length, SHA-256 of the exact Capsule file bytes, schema revision, row count, source sequence bounds, optional event-time ColumnId and bounds, payload length and payload SHA-256, and column count. Column descriptors reuse the 48-byte Capsule v0.1 descriptor layout.

The trailer is an 8-byte little-endian FNV-1a checksum over the header and records, followed by NEND. SHA-256 file fingerprints bind index entries to the exact Capsule bytes. FNV-1a is an accidental-corruption check, not authentication.

## 3. Publication and freshness

Maps are immutable generations named navigation-{generation:020}.nvm and are published using the M1.4 no-overwrite temp/file-sync/hard-link/directory-sync protocol.

At query time:
1. load and validate the requested map generation;
2. enumerate the current Capsule file set;
3. require exact identity-set equality between map entries and current Capsule files;
4. verify each Capsule's file length and SHA-256 fingerprint against its map entry;
5. only then evaluate pruning metadata.

A missing/new/removed/modified Capsule makes the map stale and returns NavigationMapStale. The engine never uses incomplete or stale metadata to skip data. Build a new immutable map generation after changing the Capsule set.

## 4. Pruning rules

The first predicate is a single typed lower/upper range on one logical column. For a Capsule with stored min/max:
- skip when Capsule maximum is below the query lower bound;
- also skip equality at the lower edge when the query lower edge is exclusive;
- skip when Capsule minimum is above the query upper bound;
- also skip equality at the upper edge when the query upper edge is exclusive.

All other cases retain the Capsule. Missing min/max, including all-NULL and UTF8/BINARY v0.1 columns, always retain it. Exact row-level range evaluation is applied to every candidate Capsule after pruning.

Column names are resolved to stable current ColumnIds. A Capsule created before a nullable column was added has no statistic for that ID and is retained; its logical NULL values are then evaluated by the row predicate.

## 5. Lineage and generation constraints

A map cannot contain overlapping source ingestion sequence ranges. This prevents the first navigation scan API from returning duplicate logical rows when two derived Capsules describe the same source rows. Multiple Capsule file generations may coexist on disk, but generation selection/promotion and safe replacement are deferred to M3's manifest protocol.

Legacy ANF inserts without Chronicle receipts are not covered by Capsule maps in v0.1.

## 6. Diagnostics

The scan result reports:
- Capsules considered, skipped and read;
- rows examined and returned;
- bytes verified against the map's Capsule fingerprints;
- embedded ANF payload bytes decoded.

Fingerprint verification may read complete Capsule files before pruning. Therefore diagnostics distinguish integrity-verification bytes from ANF payload bytes decoded. This initial correctness-first map does not claim reduced physical I/O until a later independently verifiable block/page layout is implemented.

## 7. API

- Database::build_navigation_map(table, map_generation) publishes one immutable map.
- Database::scan_capsules(table, request, None) is the unpruned reference path.
- Database::scan_capsules(table, request, Some(map_generation)) uses the verified map and safe pruning.

Both paths run identical row predicates and projection logic. The test suite asserts result equality at inclusive/exclusive boundaries, for columns with absent statistics, and after schema evolution.
