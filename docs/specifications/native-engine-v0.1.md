# ANANTA Native Engine — Technical Specification v0.2

**Status:** Proposed research specification  
**Implementation language:** Rust  
**Initial topology:** Single-node, embedded  
**Workload focus:** Hybrid analytical and time-series  
**Query interface:** Native ANANTA Query Language (working name: AQL)  
**Storage:** ANANTA-owned native storage subsystem and ANANTA Native Format (ANF)

## 1. Product definition

ANANTA is intended to become an independently engineered database system—not an application layer, hosted wrapper or compatibility skin over another database.

The native implementation will own its:
- Database engine and lifecycle.
- Persistent storage subsystem and on-disk format.
- Catalog and metadata model.
- Ingestion and data organisation.
- Query language and query interface.
- Query planner and execution engine.
- Analytical and time-series operators.
- Indexing, compaction, caching and durability mechanisms as they are implemented.

ANANTA will use Rust and the host operating system's filesystem and I/O primitives. "Custom storage" means ANANTA controls its own database-level persistence format, metadata, allocation/layout policies, buffering and recovery logic; it does not mean implementing a filesystem or disk driver.

The current ClickHouse and PostgreSQL services remain external reference systems for research and comparison. They are not runtime dependencies, storage backends, query interpreters or control-plane requirements of the native engine.

## 2. Goals and non-goals

### Long-term goals
1. A native database engine designed and implemented in Rust.
2. A custom, versioned, columnar/time-aware storage system.
3. A native query language rather than SQL compatibility as the defining interface.
4. A native analytical execution layer with aggregation, filtering, grouping, time-window and later advanced analytical operators.
5. A research-driven optimiser and physical-layout strategy.
6. Independent catalog, ingestion, indexing, compaction and durability subsystems.
7. Benchmarkable operation without requiring ClickHouse or PostgreSQL to be installed.

### v0.1 goals
- Open and close a local ANANTA database.
- Define and persist typed schemas in ANANTA-owned catalog metadata.
- Ingest batches and persist them in ANF columnar segments.
- Reopen the database and read persisted values.
- Read selected columns without decoding unrelated columns.
- Detect unsupported format versions, truncated files and checksum failures.
- Establish correctness and baseline performance tests.

### Explicit non-goals for the first milestone
- Distributed execution, replication or consensus.
- Multi-writer transactions or MVCC.
- Full query language implementation.
- Production multi-tenant security.
- A dashboard or hosted service.
- Mandatory Fire Bot integration.
- Claims of performance superiority over established engines.

## 3. Independence and permitted external foundations

The native engine must not require PostgreSQL, ClickHouse, SQLite, DuckDB or another database to store or query its data.

External libraries may be evaluated for bounded supporting tasks such as CLI parsing, serialization of non-core configuration, compression primitives, checksums, property testing and benchmark harnesses. Each dependency must be documented, and core semantics must remain ANANTA-owned.

The project may implement established computer-science algorithms and use published standards. Originality does not require inventing a new hash function, compression algorithm or mathematical method. It requires owning the engine's data model, implementation, execution semantics and integration.

The native engine may use the host filesystem for file creation, reads, writes, synchronization and atomic rename where supported. It owns the database-level file format, metadata, segment lifecycle, buffering policy and recovery protocol.

## 4. Native architecture

| Subsystem | Responsibility |
|---|---|
| ananta-types | Types, values, schemas, timestamps and null semantics |
| ananta-catalog | Native catalog, object IDs, schema versions and metadata persistence |
| ananta-format | ANF binary encoding/decoding and format validation |
| ananta-storage | Segment files, column blocks, buffer management, allocation and lifecycle |
| ananta-ingest | Batch validation, write path and segment publication |
| ananta-timeseries | Time semantics, temporal metadata, range pruning and later time-window primitives |
| ananta-query | AQL tokens, grammar, parser, AST and logical plan |
| ananta-optimizer | ANANTA-owned logical/physical rewrites and cost/statistics decisions |
| ananta-execution | Vector batches, pipelines and physical operators |
| ananta-analytics | Native aggregates and analytical/time-series operators |
| ananta-wal | Future write-ahead log, checkpoints and recovery |
| ananta-core | Database lifecycle, configuration and public embedded API |
| ananta-cli | Native command-line client |
| ananta-bench | Native benchmark driver and result capture |

Crates should be introduced when implementation needs them; do not create empty crates merely to make the tree look complete. Dependency direction must remain layered and acyclic.

## 5. Native query language

The working name is **AQL — ANANTA Query Language**. It is a native language designed around ANANTA's logical data model and execution capabilities. It is not intended to be SQL with renamed keywords, nor a PostgreSQL dialect.

The grammar and semantics are a separate research deliverable. The initial design should consider:
- Explicit source/table selection.
- Column projection and predicates.
- Grouping and aggregation.
- Time-range and time-window operations.
- Typed parameters and clear null semantics.
- Explainable logical/physical plans.
- A stable programmatic API that does not require text parsing.

Do not finalise syntax before the storage model and first operator interfaces are understood. SQL may be used in external benchmark definitions or comparison adapters, but SQL must not become a hidden runtime dependency of the native engine.

## 6. Native storage system and ANF

ANF is the first proposed on-disk format within ANANTA's own storage subsystem. The format is experimental until its byte-level specification and golden fixtures are frozen.

The initial storage design uses independently readable column blocks grouped into immutable segments. A conceptual segment contains:
1. Fixed header: magic bytes, format version, feature flags and header length.
2. Segment metadata: object/schema identifiers, row count, column count and metadata length.
3. Column directory: column identifier/type, offsets, encoded lengths, null metadata and checksums.
4. Column payloads: independently encoded blocks.
5. Footer: integrity checksum and end marker.

The broader storage subsystem is responsible for catalog persistence, segment naming and publication, buffer policy, safe file access, later compaction, indexes, WAL and recovery. These are engine responsibilities, not delegated to ClickHouse or PostgreSQL.

All offsets and lengths must be bounds-checked before allocation or seeking. Reject unknown mandatory feature flags and unsupported versions. The first prototype should favour unambiguous parsing and corruption detection over compression complexity. Start with uncompressed fixed-width numeric blocks and length-prefixed UTF-8/binary blocks; evaluate codecs later.

## 7. Data model and time-series semantics

Initial logical types:
- Boolean
- Int32
- Int64
- Float64
- UTF-8 string
- Timestamp (signed 64-bit UTC microseconds)
- Binary

Nullability is explicit in schema metadata. Timestamp conversion, overflow, ordering and time-zone boundary behaviour must be tested.

For the first storage prototype, record per-segment temporal min/max statistics. Later work can evaluate physical time partitions, late-arrival policies, retention, downsampling, time buckets and series-oriented indexes.

## 8. Native analytics and execution

ANANTA owns the semantics and implementation of its analytical operators. Initial operators should be built incrementally:
- Column projection and predicate filtering.
- Count, sum, min, max, average.
- Group-by aggregation.
- Time-range filtering and time-bucket aggregation.
- Later: window functions, quantiles, joins, approximate summaries and parallel execution.

Use vectorised batches and bounded memory. Operator contracts must define null handling, overflow, floating-point behaviour, ordering guarantees and determinism. Use known algorithms where appropriate, document them and benchmark alternatives; do not claim novelty merely for reimplementing a known algorithm.

The engine must not load an entire dataset into memory to answer a scan or aggregation.

## 9. Ingestion and consistency scope

The first writer is single-process and single-writer. Validate a complete batch before publishing it. Write segments to temporary files and publish them using a documented platform-appropriate sequence. Metadata publication ordering must be tested.

Do not advertise transaction guarantees in v0.1. Until WAL and recovery are implemented, document the exact durability boundary and possible loss window. Never silently accept partially written segments.

## 10. Validation and research policy

- Unit tests for every type and format encoder/decoder.
- Property tests for valid round trips and malformed byte input.
- Golden ANF fixtures.
- Integration: create → schema → ingest → close → reopen → scan.
- Corruption tests: truncation, invalid offsets/lengths, unknown versions and checksum mismatch.
- Deterministic tests independent of wall-clock time and machine locale.
- Analytical correctness tests including nulls, empty groups, overflow and timestamp boundaries.
- Benchmarks record hardware, OS, rustc version, build profile, dataset shape, row count, batch size, warm/cold methodology and raw results.

ClickHouse and PostgreSQL may serve as external baselines. Compare equivalent logical workloads and verify identical results before performance comparisons. Publish raw benchmark data. The native engine must also be usable and testable with those services stopped.

## 11. Versioning

ANF v0.1 is experimental and may change before a stable release. Any incompatible format change requires a version increment, compatibility/migration note and updated golden fixtures. AQL syntax and semantics will also require explicit language-versioning rules before public stability is promised.
