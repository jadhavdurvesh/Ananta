# ANANTA

**ANANTA is an independent database management system (DBMS) and data platform research project by DMJ.** The goal is to build a database system in Rust with its own engine, native storage subsystem and file format, catalog, query language, planner, execution engine, analytics, administration APIs and first-party dashboard.

ANANTA is not a SQL wrapper, PostgreSQL extension or ClickHouse backend. Its native engine must create, store, retrieve and analyse data without requiring another database. The host operating system provides filesystem and disk I/O primitives; ANANTA owns the database-level storage design and persistence format.

## Product pillars

- **ANANTA Engine:** native database runtime, lifecycle, catalog and resource management.
- **ANANTA Storage:** ANF native files and the proposed ChronoWeave research architecture.
- **AQL:** working name for a native query language with its own grammar and execution semantics. SQL is not the required interface.
- **ANANTA Analytics:** native vectorised operators, aggregations and time-series processing.
- **ANANTA Studio:** planned first-party dashboard for database exploration, AQL workbench, storage inspection, visual explain, analytics, ingestion monitoring and administration.
- **Developer platform:** Rust API, CLI, future server protocol, SDKs and workload integrations.

## Native engine status

The initial Rust implementation currently includes:
- Typed values and schema validation.
- Experimental ANF v0.1/v0.2 columnar segment encoder/decoder, golden binary fixtures, projected decoding and defensive input-size limits.
- Native local catalog with persisted schemas, stable table/column IDs and immutable schema snapshots.
- Embedded database APIs for table creation, nullable-column evolution, column rename/reorder, ANF v0.2 batch insertion, dual-version scans and bounded projected columnar scan batches.
- File-backed batch decoding with incremental whole-segment checksum verification, row/cell output limits and early-stop support.
- Exclusive local writer locking, no-overwrite segment publication, directory syncing on Unix, and startup cleanup of orphan temporary segment files.
- A basic telemetry example and unit/integration tests.

This is an experimental first vertical slice, not a complete or production-ready DBMS. The legacy `scan` APIs still materialise their complete result; the additive batch APIs bound decoded output per batch but do not yet provide predicates or physical column-range I/O. Only one local database handle/process may own a database at a time. A crash can leave a stale `.ananta.lock` file; after verifying no writer remains, an operator must remove it before reopening. Non-Unix targets do not have portable directory-entry flushing in this implementation. AQL, query planning, analytics operators, compression, indexes, compaction, WAL, transactions, MVCC and a complete crash-recovery protocol remain unimplemented.

## Master execution roadmap

The [ANANTA Master Roadmap](docs/roadmap/master-roadmap.md) is the single source of truth for task order, small implementation steps, milestone exit gates and deferred ideas. **Current focus: M1 — native storage correctness and streaming.** M0 passed its exit gate on 2026-11-05.

## Native design documents

- [Product vision: complete native database platform](docs/product/vision.md)
- [Native engine technical specification](docs/specifications/native-engine-v0.2.md)
- [Native architecture and independence boundaries](docs/architecture/native-engine.md)
- [Phase 1 implementation plan](docs/roadmap/phase-1-native-engine.md)
- [ANF v0.1 prototype byte layout](docs/specifications/anf-v0.1-prototype.md)
- [Catalog v0.2 identity proposal](docs/specifications/catalog-v0.2-identity-proposal.md)
- [Catalog v2 binary format](docs/specifications/catalog-v2-binary-format.md)
- [Schema evolution compatibility proposal](docs/specifications/schema-evolution-compatibility.md)
- [ADR 0005: immutable schema snapshots](docs/decisions/0005-schema-evolution-needs-immutable-snapshots.md)
- [ANS1 v1 schema snapshot format](docs/specifications/schema-snapshot-ans1-v1.md)
- [ANF v0.2 segment identity format](docs/specifications/anf-v0.2-prototype.md)
- [Bounded read batch contract](docs/specifications/bounded-read-batches-v0.1.md)
- [Segment publication and recovery contract](docs/specifications/segment-publication-recovery-v0.1.md)
- [ADR 0006: atomic segment publication and exclusive local writer](docs/decisions/0006-atomic-segment-publication-and-writer-lock.md)
- [Nullable-column DDL specification](docs/specifications/nullable-column-ddl-v0.1.md)
- [Column rename and reorder specification](docs/specifications/column-rename-reorder-v0.1.md)
- [ADR 0004: stable catalog object identity](docs/decisions/0004-stable-catalog-object-identity.md)
- [ChronoWeave storage research proposal](docs/specifications/chronoweave-storage-research.md)
- [ANANTA Studio and developer platform roadmap](docs/roadmap/studio-and-platform.md)

## Build and test the native engine

Install the Rust stable toolchain, then run from the repository root:

```bash
cargo fmt --all
cargo test --workspace
cargo clippy --workspace --all-targets
cargo run -p ananta-core --example basic
```

The example creates/opens `./ananta-data`, creates a telemetry table if needed, writes a batch to ANF segments and reads the rows back. Running it repeatedly appends another batch.

## Existing reference research environment

The existing ClickHouse/PostgreSQL setup is retained for external comparisons and historical experiments. It is not required to build or run the native engine.

The benchmark runner and layout lab remain research tools for reference database experiments. Native ANANTA will need its own benchmark implementation and correctness-verified comparisons before direct performance claims.

See `docs/decisions/` and `workloads/README.md`. Workloads are optional; Fire Bot is not a database dependency.

## Native scan APIs

The embedded Rust API provides bounded columnar reads:

- `Database::scan_batches(table, batch_rows)` scans all logical columns.
- `Database::scan_columns_batches(table, columns, batch_rows)` reads a projection in caller order.
- `Database::scan_batches_with_limit(table, batch_rows, row_limit)` stops after a global row count.
- `Database::scan_columns_batches_with_limit(table, columns, batch_rows, row_limit)` combines projection and early stop.

Each call returns a `ScanBatchIter` yielding owned `RecordBatch` values. The file-backed decoder verifies each whole-segment checksum in fixed-size chunks, then decodes only selected column values for the next batch. Batch size is limited by the requested row bound, a 65,536-row maximum and a 1,000,000-cell output cap. ANF v0.1 and v0.2 segments are resolved against their historical schema snapshots; newly added nullable columns are emitted as NULL for older segments. This remains an experimental single-writer scan API, not a transactionally isolated query engine.

## Segment publication and recovery

New segment writes use a same-directory temporary file, flush and sync its contents, then atomically publish the final `.anf` name with a no-overwrite hard link. On Unix the containing directory is synced after publication and temp-name removal. Opening a database acquires `.ananta.lock` and removes only canonical orphan temporary segment files. A stale lock left by a crash is not broken automatically: verify that no writer process remains, then remove `.ananta.lock` before reopening. A post-publication error is reported as `SegmentPublicationUncertain`; inspect the final segment path before retrying rather than risking a duplicate insert.

## Bounded scan memory benchmark

Run the isolated Linux memory benchmark with:

```bash
cargo build -p ananta-core --example memory_benchmark
BENCH=target/debug/examples/memory_benchmark
$BENCH generate /tmp/ananta-memory 50000 2048
$BENCH scan /tmp/ananta-memory 512
rm -rf /tmp/ananta-memory
```

Generation and scanning are separate process invocations. The scanner reports rows consumed, maximum decoded payload bytes in one batch, peak RSS from Linux `/proc/self/status` and elapsed time. The CI workflow runs the dataset-size and batch-size comparisons defined in the [benchmark specification](docs/specifications/bounded-scan-memory-benchmark-v0.1.md). In the initial Ubuntu CI measurement, a 10× increase in rows (5k to 50k) at 512-row batches changed peak RSS from 4,824 to 5,144 KiB; increasing the batch from 256 to 1,024 to 4,096 rows on 50k rows changed peak RSS from 3,992 to 7,240 to 20,036 KiB. These are workload-specific CI measurements, not universal memory guarantees.
