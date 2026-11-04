# ANANTA Native Engine — Technical Specification v0.1

**Status:** Proposed research specification  
**Implementation language:** Rust  
**Initial topology:** Single-node, embedded  
**Workload focus:** Hybrid analytical and time-series  
**Format:** ANANTA Native Format (ANF), version 0.1 (proposed)

## 1. Purpose

The native engine is a new research track within the ANANTA repository. It will investigate database internals directly: typed data representation, columnar storage, ingestion, query execution, time-aware organisation, compression, indexing and durability.

The existing ClickHouse/PostgreSQL environment remains the reference platform and benchmark harness. It is not the implementation of the native engine and must not be removed as part of this work.

## 2. Product boundary

### v0.1 goals
- Open and close a local database directory.
- Define and validate typed table schemas.
- Ingest batches of records.
- Persist data in ANANTA-owned columnar segment files.
- Reopen the database and read persisted values.
- Read selected columns without decoding unrelated columns.
- Detect unsupported format versions, truncated files and checksum failures.
- Establish correctness and baseline performance tests.

### Explicit non-goals
- Distributed execution, replication or consensus.
- PostgreSQL wire/protocol compatibility.
- Full SQL standard support.
- Multi-writer transactions or MVCC.
- Production multi-tenant security.
- A dashboard or hosted service.
- Mandatory Fire Bot integration.
- Claims of performance superiority over established engines.

## 3. Architecture

The engine is a Rust workspace with narrow crate boundaries:

| Crate | Responsibility |
|---|---|
| ananta-types | Data types, typed values, schemas, timestamps |
| ananta-format | ANF binary encoding/decoding and format validation |
| ananta-storage | Database files, segments, column blocks, read/write primitives |
| ananta-catalog | Table/schema metadata and catalog persistence |
| ananta-ingest | Batch validation and ingestion API |
| ananta-timeseries | Time semantics, time-range metadata and partition abstractions |
| ananta-core | Database lifecycle, configuration and public embedded API |
| ananta-query | Query representation and (later) SQL parsing |
| ananta-execution | Vector batches and physical operators |
| ananta-optimizer | Logical rewrites and plan optimisation |
| ananta-wal | Future write-ahead log and recovery |
| ananta-cli | Developer command-line interface |
| ananta-bench | Native engine benchmark driver |

Dependency direction must remain layered. Low-level types and format crates must not depend on query, CLI or workload crates.

## 4. Data model

Initial logical types:
- Boolean
- Int32
- Int64
- Float64
- UTF-8 string
- Timestamp (signed 64-bit UTC microseconds)
- Binary

Nullability is explicit at schema level. Numeric and timestamp values must have deterministic, documented encodings. Timestamp conversion and overflow behaviour must be tested.

## 5. ANF v0.1 conceptual format

ANF is a proposed, versioned, little-endian binary format. The first implementation should favour unambiguous parsing and corruption detection over compression complexity.

A segment contains:
1. Fixed header: magic bytes, format version, flags and header length.
2. Segment metadata: schema identifier, row count, column count and metadata length.
3. Column directory: column identifier/type, offset, encoded length, null information and checksum.
4. Column payloads: independently encoded column blocks.
5. Footer: segment checksum and end marker.

All offsets and lengths must be bounds-checked before allocation or seeking. Reject unknown mandatory feature flags and unsupported versions. Checksums must cover the documented byte ranges. The exact field widths and checksum algorithm must be frozen in a format document before the first compatibility promise.

The first prototype may use uncompressed fixed-width numeric blocks and length-prefixed UTF-8/binary blocks. Compression is a later measured experiment; do not bake codec assumptions into the file structure.

## 6. Time-series behaviour

Timestamps use UTC microseconds internally. In v0.1, time-range metadata may be stored per segment and used by a scan API to skip segments whose min/max timestamp range cannot match. Physical time partitioning, late-arrival policies, downsampling and retention are later milestones.

## 7. Ingestion and consistency scope

The first writer is single-process and single-writer. Batch validation must complete before a segment is published. Segment publication should use a temporary file followed by a platform-appropriate atomic rename; metadata publication ordering must be documented and tested.

Do not advertise transaction guarantees in v0.1. Until WAL/recovery is implemented, document the exact durability boundary and possible loss window. Never silently accept a partially written segment.

## 8. Read API

The first read interface supports:
- Full segment scan.
- Selected-column scan.
- Iteration in bounded batches.
- Optional time-range pruning based on segment statistics.

Memory use must be bounded by batch size and the selected columns, rather than requiring the entire table to be materialised.

## 9. Errors and observability

Errors must distinguish invalid schema/value, I/O failure, unsupported format version, malformed metadata, checksum mismatch, truncated input and resource limit violations. Use structured tracing for diagnostics; never treat logging as a correctness mechanism.

## 10. Validation requirements

- Unit tests for every type encoder/decoder.
- Property tests for valid value round trips and arbitrary malformed byte input.
- Golden format fixtures for compatibility.
- Integration test: create → schema → ingest → close → reopen → scan.
- Corruption tests for truncation, invalid offsets, invalid lengths, unknown versions and checksum mismatch.
- Deterministic tests independent of wall-clock time and machine locale.
- Benchmarks report hardware, OS, compiler profile, dataset shape, row count, batch size, warm/cold methodology and raw results.

## 11. Research policy

The ClickHouse setup remains a reference implementation. Compare equivalent logical workloads and verify identical results before comparing latency or storage. Publish raw benchmark data and do not claim superiority without repeatable evidence.

## 12. Versioning

ANF v0.1 is experimental and may change before a stable release. Any incompatible format change requires a version increment, migration/compatibility note and updated golden fixtures.
