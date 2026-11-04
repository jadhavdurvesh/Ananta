# ANANTA

**ANANTA is an independent database system research project.** The goal is to build a database management system (DBMS) in Rust with its own database engine, native storage subsystem and file format, catalog, query language, planner, execution engine and analytical/time-series operators.

ANANTA is not a SQL wrapper, PostgreSQL extension or ClickHouse backend. Its native engine must create, store, retrieve and analyse data without requiring another database. The host operating system provides filesystem and disk I/O primitives; ANANTA owns the database-level storage design and persistence format.

## Native engine

- **Storage:** ANANTA Native Format (ANF), custom catalog and columnar segments.
- **Query language:** AQL (working name), a planned native language with its own grammar and execution semantics. SQL is not the required interface.
- **Analytics:** Native vectorised operators, aggregations and time-series processing.
- **Implementation:** Rust workspace under `crates/`.
- **Initial topology:** Single-node, embedded, single-writer prototype.

The first implementation slice currently includes typed values/schemas, an ANF segment encoder/decoder, a local catalog, database open/create-table/insert/scan APIs, and a basic example. It is experimental: no WAL, transactional crash recovery, compression, indexes, query language, planner or production guarantees yet.

## Native engine documents

- [Native engine technical specification](docs/specifications/native-engine-v0.2.md)
- [Native architecture and independence boundaries](docs/architecture/native-engine.md)
- [Phase 1 implementation plan](docs/roadmap/phase-1-native-engine.md)
- [ANF v0.1 prototype byte layout](docs/specifications/anf-v0.1-prototype.md)

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

### Reference benchmark baseline

```bash
cd deploy && docker compose up -d
cd ../services/benchmark-runner && pip install -r requirements.txt
python generate.py --rows 1000000 --dist uniform --seed 7
python run.py --label baseline-1M-uniform
```

### Reference layout lab

```bash
cd services/layout-lab && pip install -r requirements.txt
python lab.py --label lab-1M-uniform
```

The layout lab's promotion policy applies to the ClickHouse layouts it tests. Native ANANTA requires its own benchmark adapter and comparable correctness-verified results before direct performance claims.

## Workloads

Workloads are optional inputs to the database, not imports or runtime dependencies of engine core. Fire Bot is one optional telemetry workload. See `workloads/README.md` and `docs/decisions/`.
