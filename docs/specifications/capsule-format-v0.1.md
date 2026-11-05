# Capsule Format and Prototype Contract — v0.1

**Status:** M2.3 prototype contract  
**Extension:** .cap  
**Payload:** one canonical ANF v0.2 segment  
**Publication:** immutable no-overwrite file under data/<table>/capsules/

## 1. Purpose and scope

A Capsule is a bounded, self-describing physical unit derived from one contiguous range of committed Chronicle ingestion sequences. It contains one ANF v0.2 columnar payload plus independently verifiable identity, lineage and statistics metadata.

M2.3 is an offline, explicit prototype. Building a Capsule does not delete, hide, compact or replace Chronicle records. No active-generation manifest or query planner is introduced in this milestone.

## 2. Identity and lineage

Every Capsule is identified by the tuple:

- stable TableId;
- caller-selected nonzero CapsuleId;
- caller-selected nonzero Generation.

The tuple is immutable and cannot be overwritten. A new generation is a separate file; the old generation remains available.

The source lineage is an inclusive contiguous sequence range [first_ingestion_sequence, last_ingestion_sequence]. Every sequence in the range must resolve to exactly one verified Chronicle receipt for the table. Legacy ANF inserts have no Chronicle sequence and cannot be silently included in this first Capsule builder.

The payload is materialised using the table's current logical schema at build time. Historical Chronicle payloads are resolved by stable ColumnId into that schema. The Capsule header references the current schema revision and the embedded ANF v0.2 payload repeats TableId, schema revision and ordered ColumnIds.

## 3. Event time

The builder accepts an optional explicit timestamp column name. It never guesses a time column.

When supplied, the column must exist and have type TIMESTAMP_MICROS. The header stores its stable ColumnId. If the selected column contains at least one non-NULL timestamp, temporal minimum and maximum are stored. If every value is NULL, the selected ColumnId is present but temporal bounds are absent.

When no event-time column is supplied, the ColumnId and temporal bounds are absent. Timestamp columns still receive their ordinary per-column min/max statistics.

## 4. Per-column statistics

One fixed-width 48-byte descriptor is stored for every column, in the same order as the embedded ANF payload.

Each descriptor stores:
- stable ColumnId and type code;
- null count;
- exact encoded byte length of that ANF column payload;
- optional min/max scalar values.

Min/max is produced for BOOLEAN, INT32, INT64, TIMESTAMP_MICROS and FLOAT64. Float64 uses Rust's deterministic total ordering, including signed zero and NaN payloads. UTF8 and BINARY min/max are omitted in v0.1 to avoid storing potentially large variable-length values in metadata; their null count and encoded byte size are still recorded. All-null columns have no min/max.

Null counts are between zero and Capsule row count. Descriptor ColumnIds and type codes must exactly match the embedded ANF schema identity. Encoded byte lengths must exactly match the embedded ANF v0.2 column directory.

## 5. File layout

A Capsule file is:

CAP1 | 144-byte fixed header | column descriptors | ANF v0.2 payload | checksum | CEND

All integers are little-endian.

### Fixed header

| Offset | Bytes | Field |
|---:|---:|---|
| 0 | 4 | Magic: CAP1 |
| 4 | 2 | Version: 1 |
| 6 | 2 | Flags: bit 0 event-time column configured, bit 1 temporal bounds present |
| 8 | 4 | Header length: 144 |
| 12 | 8 | TableId |
| 20 | 8 | CapsuleId |
| 28 | 8 | Generation |
| 36 | 8 | Schema revision |
| 44 | 8 | Row count |
| 52 | 8 | First source ingestion sequence |
| 60 | 8 | Last source ingestion sequence |
| 68 | 8 | Event-time ColumnId, zero when absent |
| 76 | 8 | Temporal minimum, zero when absent |
| 84 | 8 | Temporal maximum, zero when absent |
| 92 | 8 | Embedded ANF payload length |
| 100 | 4 | Column count |
| 104 | 4 | Descriptor section length |
| 108 | 32 | SHA-256 of exact embedded ANF payload |
| 140 | 4 | Reserved zero bytes |

### 48-byte column descriptor

| Offset | Bytes | Field |
|---:|---:|---|
| 0 | 8 | ColumnId |
| 8 | 1 | DataType code |
| 9 | 1 | Flags: bit 0 min/max present |
| 10 | 2 | Reserved zero |
| 12 | 8 | Null count |
| 20 | 8 | Encoded bytes in embedded ANF column payload |
| 28 | 1 | Min scalar byte length |
| 29 | 1 | Max scalar byte length |
| 30 | 2 | Reserved zero |
| 32 | 8 | Min scalar bytes, zero padded |
| 40 | 8 | Max scalar bytes, zero padded |

The checksum trailer is 8-byte little-endian FNV-1a 64-bit over header, descriptors and payload, followed by the 4-byte CEND marker. SHA-256 binds the embedded payload to its header; FNV-1a detects accidental whole-file corruption. Neither is authentication.

## 6. Bounds and validation

- TableId, CapsuleId, Generation and schema revision must be nonzero.
- Row count must be nonzero and stay within ANF v0.2 limits.
- First sequence must not exceed last sequence.
- The source sequence range must be contiguous, committed and verified.
- Column count is limited by existing ANF v0.2 maximums.
- Embedded payload must be a valid ANF v0.2 segment with identical table, schema revision, row count, ordered ColumnIds and per-column encoded lengths.
- Capsule descriptor section length must equal column_count × 48.
- Metadata fields, flags, reserved bytes, min/max lengths, null counts and event-time references must be internally consistent.
- The Capsule checksum and payload SHA-256 are both verified.
- Capsule creation fails if the complete Capsule exceeds the configured prototype size ceiling. It is never silently split.
- A public validator re-decodes the embedded payload against the referenced immutable schema snapshot and recomputes all per-column statistics.

## 7. Publication and naming

Capsules are stored under data/<table>/capsules/ with filename:

capsule-{capsule_id:020}-g{generation:020}.cap

The matching temporary name is dot-prefixed and ends in .tmp. Publication uses exclusive temp creation, write/flush/file sync, same-directory no-overwrite hard-link creation, directory sync on Unix, temp removal and a second directory sync. The temporary name is never readable as a committed Capsule. A post-link failure is reported as SegmentPublicationUncertain and the final path must be inspected before retry.

## 8. Prototype API

The initial embedded API is conceptually:

build_capsule(table, capsule_id, generation, first_sequence, last_sequence, event_time_column) -> Capsule path

validate_capsule(table, capsule_id, generation) -> verified Capsule metadata

Capsule generation is explicit; there is no automatic compaction scheduler or active-generation switch.

## 9. Non-goals

M2.3 does not implement a Navigation Map, Bloom filters, sketches, compression, independent per-column payload encoding, active Capsule manifests, Chronicle deletion, compaction, Weaves, AQL planning or automatic Capsule sizing. Capsules retain the existing ANF payload as the correctness baseline so metadata and validation can be evaluated independently.
