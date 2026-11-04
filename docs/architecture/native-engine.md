# Native ANANTA Architecture — Independent Engine

## Architecture decision

ANANTA's long-term product is a native database system implemented in Rust. It will own its database engine, database-level storage subsystem and file format, catalog, ingestion path, query language, query planner, execution engine and analytical operators.

The engine must not depend on PostgreSQL, ClickHouse, SQLite, DuckDB or another database to store/query its data or to start. Existing ClickHouse and PostgreSQL services remain external reference systems for comparison and research only.

The existing repository's benchmark runner, layout lab, SQL schemas and workload adapters are retained as research infrastructure. They are not the implementation underneath the native engine.

## What "custom storage" means

ANANTA will implement its own database storage subsystem: catalog persistence, segment lifecycle, native file format, column/block encoding, metadata, buffering strategy, later allocation/compaction, indexes, WAL and recovery.

The host operating system still provides filesystem and device I/O primitives. ANANTA is not initially building a filesystem, block driver or storage device firmware.

## Top-level repository view

```text
ANANTA — independent database research platform
|
+-- database/ (existing reference schemas)
|   +-- clickhouse/
|   +-- postgres/
+-- deploy/ (existing local reference stack)
+-- services/ (existing benchmark-runner and layout-lab)
|
+-- workloads/
|   +-- synthetic event workloads
|   +-- optional Fire Bot and future workload adapters
|
+-- crates/ (new native Rust engine)
    +-- ananta-types
    +-- ananta-catalog
    +-- ananta-format
    +-- ananta-storage
    +-- ananta-ingest
    +-- ananta-timeseries
    +-- ananta-query       native AQL parser/AST/logical plans
    +-- ananta-optimizer   native optimisation rules
    +-- ananta-execution   vectorised physical operators
    +-- ananta-analytics   native analytical/time-series operators
    +-- ananta-wal         later durability/recovery
    +-- ananta-core
    +-- ananta-cli
    +-- ananta-bench

+-- studio/ (planned first-party dashboard)
    +-- ANANTA Studio frontend, built against versioned engine APIs
```

Crates are added when the relevant interfaces are ready; this is a target decomposition, not a requirement to create empty crates immediately.

## First-party platform

The planned `studio/` application is documented in `docs/roadmap/studio-and-platform.md`; it is not implemented yet. A future versioned server/management API will expose real catalog, query, storage and telemetry capabilities. Studio must not read raw database files or become a runtime dependency of the engine.

## Native execution flow

```text
Rust API or AQL client
        |
        v
AQL lexer / parser / AST
        |
        v
Logical plan
        |
        v
ANANTA optimizer + catalog statistics
        |
        v
Physical plan / vector pipeline
        |
        v
Native scan, filter, projection, aggregation, time operators
        |
        v
ANANTA catalog + storage manager
        |
        v
ANF segments and native metadata on local filesystem
```

Phase 1 begins below the query-language layer. The storage lifecycle and typed model must work before the language and full analytical planner are designed.

## Dependency boundaries

- The native engine owns its runtime catalog; PostgreSQL is not required.
- The native engine owns its file format and storage path; ClickHouse is not required.
- AQL is the intended native query interface; SQL parsing is not a native engine requirement.
- The analytics crate owns operator semantics and implementations. It may use established algorithms, but it must not delegate query execution to another database.
- Workload modules must not be imported by engine core.
- Fire Bot remains an optional producer of telemetry data, not a database dependency.
- Reference systems communicate through later benchmark adapters and offline data exchange, not through an obligatory live database connection.

## Research and benchmark integration

Create a common logical benchmark description over time: schemas, deterministic input data, query intent, parameters and expected results. Provide separate adapters for native ANANTA and external reference databases. SQL may appear in a ClickHouse adapter, but never as the required native query representation.

Correctness comparison precedes performance comparison. Report engine version, compiler/build profile, hardware, dataset, cache protocol, repetitions and raw measurements. Do not use ClickHouse layout-lab promotion rules as proof about native ANANTA; they apply to the ClickHouse experiments in that lab.

## Native subsystem responsibilities

| Subsystem | Owns |
|---|---|
| Types | Values, schemas, null and timestamp semantics |
| Catalog | Object identity, schema versions and metadata persistence |
| Format | ANF binary contract, versioning and checksums |
| Storage | Segment lifecycle, block reads/writes, buffering and later compaction |
| Ingest | Validation, batching and publication |
| Query | AQL grammar, parser, AST and logical plans |
| Optimizer | Logical/physical rewrites and cost decisions |
| Execution | Vector batches, pipelines and physical operators |
| Analytics | Aggregations, time windows and later analytical operators |
| WAL | Future commit log, checkpoint and recovery |
| Core | Embedded database API and lifecycle |

## First-party dashboard boundary

ANANTA Studio is the first-party visual management and analytics application. It connects through a versioned management/query API exposed by a future server/session layer. It must not read, mutate or depend directly on raw storage files. Engine correctness, local embedded use, CLI operation and recovery must all work when Studio is unavailable.

Studio sections are planned in `docs/roadmap/studio-and-platform.md`. Only capabilities backed by actual engine telemetry and APIs may be presented as implemented.

## Deferred research decisions

- AQL grammar and syntax.
- Exact ANF byte-level layout and checksum algorithm.
- Catalog encoding and atomic publication protocol.
- Buffer-pool policy and memory limits.
- Segment compaction strategy.
- Index structures and statistics.
- WAL, transaction semantics, MVCC and concurrency.
- Network protocol and server mode.
- Distributed execution and replication.
- Compression codec defaults and SIMD strategy.

These decisions must follow prototypes, correctness tests and measurements rather than assumptions.
