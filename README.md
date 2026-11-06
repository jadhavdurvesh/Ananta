# ANANTA

**ANANTA is an independent database management system (DBMS) and data platform research project by DMJ.** The goal is to build a database system in Rust with its own engine, native storage subsystem and file format, catalog, query language, planner, execution engine, analytics, administration APIs and first-party dashboard.

ANANTA is not a SQL wrapper, PostgreSQL extension or ClickHouse backend. Its native engine must create, store, retrieve and analyse data without requiring another database. The host operating system provides filesystem and disk I/O primitives; ANANTA owns the database-level storage design and persistence format.

## Product pillars

- **ANANTA Engine:** native database runtime, lifecycle, catalog and resource management.
- **ANANTA Storage:** ANF native files and the proposed ChronoWeave research architecture.
- **AQL:** native query language with its own grammar and execution semantics. AQL v0.1 lexer, parser and AST are under implementation; SQL is not the required interface.
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
- Immutable Chronicle ingestion records with durable BatchId receipts, commit-order sequences and duplicate retry resolution.
- Derived immutable Capsule v0.1 files with temporal bounds, per-column statistics, integrity checks and an independent validator.
- A standalone AQL v0.1 Rust crate with a source-spanned lexer/parser, SELECT AST, literals, casts, boolean/comparison expressions and typed parameter-binding validation.
- A basic telemetry example and unit/integration tests.

This is an experimental first vertical slice, not a complete or production-ready DBMS. The legacy `scan` APIs still materialise their complete result; the additive batch APIs bound decoded output per batch but do not yet provide predicates or physical column-range I/O. Only one local database handle/process may own a database at a time. A crash can leave a stale `.ananta.lock` file; after verifying no writer remains, an operator must remove it before reopening. Non-Unix targets do not have portable directory-entry flushing in this implementation. AQL parsing is an early v0.1 foundation; catalog semantic validation and native query execution remain unimplemented. Query planning, analytics operators, compression, indexes, WAL, transactions, MVCC and a complete crash-recovery protocol also remain unimplemented.



## AQL v0.1 status

AQL v0.1 is implemented as a native Rust language crate, not SQL passed through to another database. It includes a source-spanned lexer/parser and AST, structural validation, logical plans, catalog-aware name/type resolution, parameter binding, NULL-aware predicates, projections, limits, timestamp comparisons and bounded typed result batches. The embedded API executes against ANANTA's native scan batches; joins, aggregation, ordering and physical query optimisation remain future work.

- [AQL v0.1 grammar and semantics](docs/specifications/aql-v0.1.md)

## Native execution v0.1 status

The separate Rust `ananta-execution` crate provides M5 pull-based operators over the existing owned `ananta_core::RecordBatch` representation. M5.1 provides typed vector-schema validation, storage scan, projection, NULL-aware filtering, limit, cancellation and resource budgets. M5.2 adds COUNT, SUM, MIN, MAX, AVG and deterministic GROUP BY with NULL semantics, overflow checking and group-state memory limits. M5.3 adds UTC-microsecond range filtering, epoch-anchored fixed time buckets, late-arrival aggregation and latest-value-per-series. M5.4 routes supported simple AQL SELECT queries through native scan/filter/project/limit operators, including projection pushdown and safe limit pushdown. AQL has physical EXPLAIN, analyzed-table estimates driven by exact null/min/max and approximate-NDV statistics, table/schema/data-inventory freshness checks and checksummed save/load snapshots with bounded sample histograms for analysis statistics, optimizer-on/off differential tests, and runtime input/output profiling (measured decoded scan batches/rows/value bytes and returned batches/rows/cells/value bytes, plus a separate owned-batch size estimate). Computed projections remain on the reference evaluator; statistics have an atomic, checksummed database-managed cache with freshness validation and are consumed by explain_plan (explicit ANALYZE/refresh, not an implicit query-time full scan). Vectorized timestamp queries may use Capsule/Navigation Map pruning only with complete Chronicle coverage, no legacy ANF files, a configured map generation, and materialisation limits; other queries fall back to the authoritative scan. Analyzed selectivity estimates are compared with measured native input/output on representative fixtures.

## Master execution roadmap

The [ANANTA Master Roadmap](docs/roadmap/master-roadmap.md) is the single source of truth for task order, small implementation steps, milestone exit gates and deferred ideas. **Current focus: M5.2 — aggregation and group-by operators.** M1, M2 and M3 are complete. M3 delivered bounded ChronoWeave candidate execution, three layout strategies, provenance verification, active-generation promotion/rollback, crash recovery and reproducible benchmark evidence.

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
- [Ingestion contract v0.1](docs/specifications/ingestion-contract-v0.1.md)
- [Chronicle record format v0.1](docs/specifications/chronicle-record-v0.1.md)
- [ADR 0008: Chronicle record as the ingestion commit point](docs/decisions/0008-chronicle-record-commit-point.md)
- [ADR 0007: idempotent commit-ordered ingestion](docs/decisions/0007-idempotent-commit-ordered-ingestion.md)
- [Compaction Manifest and Job Contract v0.1](docs/specifications/compaction-manifest-v0.1.md)
- [ADR 0010: immutable compaction inputs and candidate outputs](docs/decisions/0010-compaction-input-snapshots-and-candidate-outputs.md)
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

## Ingestion contract status

M2.1's idempotent ingestion contract now has an M2.2 runtime prototype: caller-supplied 128-bit BatchIds scoped to stable TableIds, SHA-256 payload fingerprints, whole-batch validation, per-table commit-order sequences, duplicate retry resolution and explicit result counters. Chronicle records make the data and receipt one immutable commit unit. See the [contract](docs/specifications/ingestion-contract-v0.1.md), [Chronicle format](docs/specifications/chronicle-record-v0.1.md) and [ADR 0007](docs/decisions/0007-idempotent-commit-ordered-ingestion.md).

## Chronicle ingestion prototype

The additive `Database::ingest_batch(table, batch_id, rows)` API writes one immutable Chronicle file containing the complete ANF v0.2 payload, 128-bit BatchId, SHA-256 payload fingerprint, TableId, schema revision, per-table commit sequence and whole-record checksum. Identical retries return the original receipt; conflicting reuse fails. The receipt index and accepted totals are rebuilt from verified Chronicle records on open. The existing `insert_batch` API remains available and writes legacy ANF segment files; both formats are readable by full and bounded scans. This is an experimental single-writer prototype: startup currently verifies and indexes all Chronicle records, and ingestion encodes the complete record in memory.

## Chronicle performance benchmark

The CI workflow runs the reproducible ingestion and startup benchmark described in [Chronicle Performance Benchmark v0.1](docs/specifications/chronicle-performance-benchmark-v0.1.md). It compares the legacy insert_batch path with idempotent Chronicle ingestion on identical rows and measures fresh-process Database::open time as receipt count grows. Results are workload-specific and should be treated as a baseline, not a throughput guarantee.

The initial optimized CI benchmark measured 12.5% higher end-to-end ingestion elapsed time for Chronicle than legacy ANF on 100 × 128-row batches with 512-byte payloads. The Chronicle envelope added 108 bytes per batch. Fresh-process Database::open took 10.818 ms for 25 receipts and 41.876 ms for 100 receipts. See the full [benchmark report](docs/specifications/chronicle-performance-benchmark-v0.1.md); these are prototype CI results, not production guarantees.

## Capsule prototype

The M2.3 Capsule prototype builds an immutable .cap file from a contiguous range of committed Chronicle sequences. Each Capsule contains an ANF v0.2 payload, stable table/schema/column identity, explicit source sequence lineage, optional explicitly selected event-time bounds, per-column null counts, exact encoded byte lengths, supported scalar min/max statistics, a SHA-256 payload fingerprint and a whole-file checksum. Use Database::build_capsule to create one and Database::validate_capsule to independently verify its contents and recompute its statistics. Capsules are derived copies: the source Chronicle records remain readable. The M2.4 Navigation Map can prune by typed ranges while retaining result equivalence, but active-generation manifests, promotion/rollback and automatic compaction remain M3 work. See the [Capsule Format v0.1 specification](docs/specifications/capsule-format-v0.1.md) and [ADR 0009](docs/decisions/0009-immutable-capsules-as-derived-chronicle-units.md).

## Navigation Map prototype

The M2.4 Navigation Map is an immutable, checksummed metadata generation over the exact Capsule file set. Build one with Database::build_navigation_map(table, map_generation). Database::scan_capsules(table, request, None) runs the unpruned reference path; passing Some(map_generation) runs the mapped path. Range predicates are type-checked, use inclusive/exclusive lower and upper boundaries, and are evaluated against actual rows after any metadata pruning. A map is rejected as stale if a Capsule is added, removed or modified after publication. Missing column statistics always retain the Capsule, so schema evolution and UTF8/BINARY columns cannot create false negatives.

The result includes capsules considered/skipped/read, rows examined/returned, bytes fingerprint-verified and embedded ANF payload bytes decoded. Fingerprint verification currently reads the complete Capsule file before trusting its pruning metadata; this prototype demonstrates safe metadata pruning and reduced decoding, not yet guaranteed reduced physical I/O. Overlapping source sequence ranges are rejected when building a map. Generation selection and promotion remain M3 work. See the [Navigation Map v0.1 specification](docs/specifications/navigation-map-v0.1.md).

## M2 benchmark results

The Capsule size benchmark compared 512-row, 2,048-row and 8,192-row Capsules over identical 16,384-row datasets. The Navigation Map skipped 24/32, 6/8 and 1/2 Capsules respectively for a 4,096-row time range; all mapped outputs exactly matched the unpruned reference. The mapped path decoded less ANF payload but still verified the full Capsule bytes, so physical I/O reduction is not yet established. Full measurements are in the [Capsule benchmark report](docs/specifications/capsule-size-scan-benchmark-v0.1.md).

**M2 is complete.** Chronicle ingestion, Capsule v0.1 and Navigation Map v0.1 have correctness tests and reproducible CI benchmarks. M3 begins with the compaction input/output manifest, generation lineage, temporary output, resource limits and failure/restart contract.


The AQL crate re-exports `analyze_table`, `analyze_table_and_persist` and `load_table_statistics` for explicit table analysis and cache management. `EXPLAIN` reuses only fresh cached analysis and never starts an implicit full-table scan.


The vectorized `EXPLAIN` plan includes a transparent relative cost breakdown (scan rows, estimated projected payload, filter evaluations, projected cells and total cost units). The units are documented heuristics, not measured latency.


Planner cost weights are configurable through `PlannerOptions::cost_weights`. The default relative weights are surfaced in EXPLAIN output and can be tuned against measured vectorized execution profiles; they are not treated as wall-clock predictions.

The `analytics_demo` example explicitly analyzes its fixture and reports estimated-versus-measured scan rows, output rows, projected payload and optimized/reference execution profiles as a reproducible cost-calibration workload.


### Planner cost calibration

Run the reproducible workload matrix with:

```bash
cargo run -p ananta-aql --example cost_calibration
```

The example creates a deterministic 2,048-row telemetry dataset, persists ANALYZE statistics, and emits CSV comparing estimated scan/output rows and projected payload with measured decoded input/output profiles across six workloads. It reports the current configurable cost weights and elapsed time as observations; it does not automatically rewrite defaults or claim that relative cost units are milliseconds.


The standalone ANWL WAL reader additionally tests truncated frame-body recovery, sequence-gap rejection and transaction commit-digest validation. Database write-path integration and startup replay remain M6.2 work.


M6.2 is being developed against a redo-only staged-file publication protocol. Its crash matrix, mutation-intent envelope, recovery ordering, Chronicle idempotency rules and fail-closed cases are specified in docs/specifications/wal-recovery-v0.1.md. This remains design work until Database mutation paths and startup replay are implemented and fault-injection tested.


M6.2 now includes typed AMUT v1 mutation intents and a tested recovery-plan classifier for committed, incomplete and aborted WAL transactions. These are primitives only; Database::open replay and ANF/Chronicle mutation-path integration remain in progress.
