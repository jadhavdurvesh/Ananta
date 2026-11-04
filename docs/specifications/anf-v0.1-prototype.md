# ANANTA Native Format — ANF v0.1 Prototype Layout

**Status:** Experimental implementation specification  
**Compatibility:** No stable compatibility guarantee  
**Byte order:** Little-endian for fixed-width integers  
**Integrity:** FNV-1a 64-bit checksum for accidental corruption detection only; not cryptographic authentication

This document describes the first prototype codec implemented in `crates/ananta-format`. It is intentionally simple and will evolve through research. Do not treat it as a long-term stable file contract.

## Segment layout

All fields are contiguous with no implicit padding.

| Offset | Size | Field |
|---:|---:|---|
| 0 | 4 | Magic bytes: ASCII `ANF1` |
| 4 | 2 | Format version: unsigned 16-bit, value 1 |
| 6 | 2 | Feature flags: unsigned 16-bit, currently must be zero |
| 8 | 8 | Row count: unsigned 64-bit |
| 16 | 4 | Column count: unsigned 32-bit |
| 20 | 17 × column count | Column directory |
| variable | variable | Column payloads in directory order |
| final - 12 | 8 | FNV-1a 64-bit checksum over all preceding bytes |
| final - 4 | 4 | End marker: ASCII `AEND` |

### Column directory entry

Each entry is 17 bytes:

| Entry offset | Size | Field |
|---:|---:|---|
| 0 | 1 | Logical type code |
| 1 | 8 | Absolute payload offset, unsigned 64-bit |
| 9 | 8 | Payload length, unsigned 64-bit |

Payload ranges are contiguous and ordered. The reader verifies the directory against the supplied schema and rejects gaps, overlaps, out-of-bounds lengths and unreferenced payload bytes.

## Type codes

| Code | Logical type | Payload value |
|---:|---|---|
| 1 | BOOLEAN | One byte, 0 or 1 |
| 2 | INT32 | Signed 32-bit little-endian |
| 3 | INT64 | Signed 64-bit little-endian |
| 4 | FLOAT64 | IEEE-754 bits as little-endian u64 |
| 5 | UTF8 | u32 byte length followed by UTF-8 bytes |
| 6 | TIMESTAMP_MICROS | Signed 64-bit UTC microseconds |
| 7 | BINARY | u32 byte length followed by bytes |

Every row value begins with one null marker byte: 0 means NULL and 1 means a following non-null payload. Other marker values are invalid. Nullability is checked against the table schema after decoding.

UTF-8 and binary fields are currently limited to 64 MiB each. This is a defensive prototype limit, not a product promise.

## Reader validation

The reader must validate:
- Minimum file size, magic, version and flags.
- Column count and type codes against the supplied schema.
- Directory bounds, contiguous payload offsets and lengths.
- Trailer marker and checksum.
- Null markers, boolean encoding, UTF-8 validity and field size limits.
- Exact row count and exact consumption of every column payload.
- Schema nullability and value types after reconstruction.

No untrusted length or offset may be used for allocation or seeking before bounds checks.

## Catalog format (prototype)

The native catalog is stored at `catalog/catalog.anc`. The initial format contains:
- Magic `ANC1`.
- Version u16 (1).
- Table count u32.
- Repeated table entries: UTF-8 table name (u32 length + bytes), column count u32, then repeated column name (u32 length + bytes), type code u8 and nullability u8.
- FNV-1a u64 checksum and `CEND` marker.

The catalog is rewritten through a temporary file and rename. It currently stores schema metadata only; segment discovery scans each table's data directory for ordered `.anf` files.

## Projected-read behaviour in v0.1

The decoder can decode only selected column values, but the current database API still reads the complete segment file into memory before decoding. This is required by the current whole-segment checksum design. Therefore v0.1 projection reduces value decoding and result width; it does **not** yet guarantee reduced disk I/O or memory proportional only to selected columns. Range-based column I/O and bounded streaming require a later format/storage revision.

## Prototype limitations

- FNV-1a is not cryptographic and must not be used for tamper resistance.
- No compression, encryption, row groups, page cache, secondary indexes or compaction.
- No WAL, multi-writer transactions, MVCC or crash-recovery guarantee.
- The single-writer assumption is required.
- Durability and atomic-rename semantics depend on the host filesystem and platform.
- The reader supports projected-column decoding, but the current database API materialises the returned result rows in memory. Bounded streaming batch iteration remains a follow-up task.

Any incompatible format change must update the version, this document and golden fixtures before compatibility is claimed.
