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
- Experimental ANF columnar segment encoder/decoder, including projected-column decoding (the full segment is currently read to verify its checksum).
- Native local catalog file with persisted schemas.
- Embedded database API for open, create-table, insert-batch, full scan and projected scan.
- A basic telemetry example and unit/integration tests.

This is an experimental first vertical slice, not a complete or production-ready DBMS. It currently reads complete segment files and materialises scan results in memory; projected scans skip decoding unselected column values but do not yet provide physical column-range I/O. It assumes one exclusive writer. Cross-process/multiple-handle locking is not implemented. AQL, query planning, analytics operators, bounded streaming scans, compression, indexes, compaction, WAL, transactions, MVCC and crash recovery are not yet implemented.

## Master execution roadmap

The [ANANTA Master Roadmap](docs/roadmap/master-roadmap.md) is the single source of truth for task order, small implementation steps, milestone exit gates and deferred ideas. **Current focus: M0 — verify the existing Rust foundation.** Do not begin later milestones until M0 passes.

## Native design documents

- [Product vision: complete native database platform](docs/product/vision.md)
- [Native engine technical specification](docs/specifications/native-engine-v0.2.md)
- [Native architecture and independence boundaries](docs/architecture/native-engine.md)
- [Phase 1 implementation plan](docs/roadmap/phase-1-native-engine.md)
- [ANF v0.1 prototype byte layout](docs/specifications/anf-v0.1-prototype.md)
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
