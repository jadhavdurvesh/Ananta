# Chronicle Record Format v0.1

**Status:** M2.2 prototype format  
**Extension:** `.chr`  
**Embedded payload:** ANF v0.2 segment, unchanged  
**Byte order:** little-endian integer fields  
**Publication:** immutable no-overwrite file publication through the M1.4 writer protocol

## 1. File layout

A Chronicle file is one independently verifiable commit record:

`CHR1 | fixed header | ANF v0.2 payload | checksum | CEND`

The fixed header is 96 bytes.

| Offset | Bytes | Field |
|---:|---:|---|
| 0 | 4 | Magic: ASCII `CHR1` |
| 4 | 2 | Format version: 1 |
| 6 | 2 | Mandatory flags: zero in v0.1 |
| 8 | 8 | Stable TableId |
| 16 | 16 | BatchId |
| 32 | 8 | Per-table ingestion sequence |
| 40 | 8 | Table schema revision |
| 48 | 8 | Row count |
| 56 | 8 | Embedded ANF payload byte length |
| 64 | 32 | SHA-256 fingerprint of the exact embedded ANF bytes |

The payload starts at byte 96 and is exactly one complete ANF v0.2 segment. It must agree with the Chronicle header on TableId, schema revision and row count.

The trailer is 12 bytes:
- 8-byte little-endian FNV-1a 64-bit integrity checksum over the complete header and payload.
- 4-byte ASCII end marker `CEND`.

SHA-256 is the idempotency fingerprint. The FNV-1a trailer is a fast accidental-corruption check over the complete record. Neither is an authentication/signature scheme.

## 2. Identity and ordering

- BatchId is 16 nonzero bytes.
- Sequence begins at 1 for the first Chronicle commit on a table and increases by exactly one per first-time accepted BatchId.
- Sequence is the durable commit/acceptance order, not event time.
- Chronicle sequence is stored in the header and is independent of the physical filename number.
- The physical filename is `segment-{physical_id:020}.chr`; physical IDs share the segment directory's monotonically increasing namespace with legacy `.anf` files, preserving mixed scan order.
- Chronicle files are ordered by logical sequence when rebuilding the receipt index. Missing/duplicate logical sequences, duplicate BatchIds, a table identity mismatch, noncanonical filename, invalid checksum or fingerprint mismatch fail database open.

Legacy ANF segments have no BatchId receipt and are not retroactively assigned synthetic client identities. They remain readable and are retained in physical append order. Chronicle sequence 1 starts with the first Chronicle commit even when legacy ANF files already exist.

## 3. Atomic commit point

1. Validate the full batch and encode one canonical ANF v0.2 payload.
2. Compute SHA-256 over that payload.
3. Construct the complete Chronicle record in memory.
4. Write a canonical temporary record with exclusive creation.
5. Flush and sync the temporary file.
6. Publish the final `.chr` name with same-directory no-overwrite hard-link creation.
7. Sync the directory on Unix.
8. Remove the temporary name and sync the directory again on Unix.
9. Only after publication succeeds, update the in-memory receipt index and return `Accepted`.

The published Chronicle file is the single durable commit point for data, identity, fingerprint and sequence. There is no separate receipt/catalog write to get out of sync.

A failure before final-name publication is a failed attempt and consumes no sequence. A failure after final-name creation returns the existing `SegmentPublicationUncertain` error with the final path; the current process reconciles a visible valid record into its in-memory index. After restart the index is rebuilt from committed `.chr` files. Callers retry the same BatchId to resolve an uncertain response.

## 4. Recovery and receipt index

On open, ANANTA:
1. acquires the exclusive local writer lock;
2. removes recognized orphan Chronicle temporary names using M1.4 startup cleanup;
3. scans each table's `.chr` files;
4. verifies framing, header, whole-record checksum, payload SHA-256 and embedded ANF identity;
5. sorts by per-table ingestion sequence and rebuilds the BatchId-to-receipt index;
6. rejects sequence gaps, duplicate IDs or malformed Chronicle files.

The index stores each receipt's metadata and physical path. Durable accepted batch/row/encoded-byte counters are reconstructed from receipts. Duplicate/rejected/publication-failure request counters are process-local and reset after restart.

## 5. Scan compatibility

Legacy full/projected scans and bounded batch scans both accept `.anf` and `.chr` files. For a Chronicle file, the scanner validates the Chronicle envelope and then presents a bounded seekable view of the embedded ANF payload to the existing ANF decoder. The ANF decoder remains responsible for validating the embedded column directory and historical ColumnIds/schema snapshot.

## 6. Prototype limitations

- One Chronicle record contains one ANF segment; the existing 256 MiB payload, row/cell/column and per-value limits apply.
- The record wrapper is read and written as one encoded byte vector at ingestion; streaming ingestion encoding is not implemented.
- Startup currently verifies every Chronicle record and rebuilds the complete receipt index in memory.
- Receipt retention is permanent for the table's current logical lifetime; pruning/compaction semantics are not implemented.
- Single local writer only. No multi-table transactions, asynchronous queue, concurrent ingestion or event-time deduplication.
- SHA-256 is implemented locally and checked against standard known-answer test vectors; replacing it with a maintained crypto crate can be considered if repository dependency/lock management is introduced.
