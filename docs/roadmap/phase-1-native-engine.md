# Phase 1 — Native ANANTA Engine Foundation

**Release target:** experimental `0.1.0` native storage prototype  
**Outcome:** create an ANANTA database, define a typed schema, ingest a batch, persist it using ANANTA's own storage subsystem and ANF files, close/reopen the database and read back identical values.

## Guiding boundary

ANANTA must run without PostgreSQL, ClickHouse, SQLite, DuckDB or any other database. Those systems are external research baselines only. Phase 1 implements database-level storage and catalog primitives using Rust and operating-system filesystem/I/O facilities.

The native query language (working name AQL) and native analytical engine are later work packages. SQL is not the required native interface.

## Scope

In scope:
- Rust workspace and crate boundaries.
- Typed schema/value model.
- Native catalog and database directory lifecycle.
- Versioned ANF header, column directory, payload and footer.
- Native segment writer, reader and lifecycle.
- Batch validation and bounded column scans.
- Temporal segment statistics.
- Corruption and round-trip tests.

Out of scope:
- AQL grammar/parser and full query planner.
- Full analytical query execution.
- Distributed system.
- Multi-writer transactions or MVCC.
- WAL and crash-recovery guarantees.
- Compression tuning and secondary indexes.
- Web UI and hosted deployment.

## Work packages

### P1. Workspace bootstrap
- Add root Cargo workspace and Rust toolchain policy.
- Create manifests for only the crates needed by the first vertical slice.
- Configure formatting, linting and CI.
- Verify existing Python/Docker reference workflows remain functional.

Acceptance: native workspace builds and tests independently of the reference databases.

### P2. Types and schemas
- Implement Boolean, Int32, Int64, Float64, UTF-8, Timestamp (UTC microseconds) and Binary.
- Implement explicit nullable/non-nullable columns.
- Validate row width and type compatibility.
- Define stable error categories and documented null/timestamp semantics.

Acceptance: unit and property tests cover valid values, nulls, incorrect types and timestamp boundaries.

### P3. Native catalog and database lifecycle
- Define ANANTA-owned database directory and catalog structure.
- Persist versioned database/table metadata without PostgreSQL.
- Implement open, create-table, list-table and close lifecycle.
- Validate metadata before trusting offsets or lengths.

Acceptance: metadata survives close/reopen; malformed metadata returns a defined error; lifecycle works with reference services stopped.

### P4. ANANTA Native Format (ANF)
- Freeze a written v0.1 byte-level specification before declaring compatibility.
- Implement writer and reader for header, metadata, column directory, payload and footer.
- Use explicit endianness and checksums.
- Bound lengths/offsets and reject unknown mandatory flags.
- Document the platform filesystem assumptions used by segment publication.

Acceptance: golden fixtures, round-trip and corruption tests pass.

### P5. Native storage and ingestion
- Validate complete batches before writing.
- Write segments to temporary paths and publish using a documented safe sequence.
- Provide full-column and selected-column scans in bounded batches.
- Record per-segment min/max timestamp metadata.
- Keep catalog and segment publication ordering explicit.

Acceptance: create → ingest → close → reopen → scan returns exactly the inserted logical values; projection reads do not decode unrelated columns.

### P6. Research baseline
- Create deterministic native-engine datasets and benchmark harness.
- Measure ingestion throughput, scan throughput, projected scan latency, on-disk size and peak memory.
- Record machine, OS, rustc version, build profile and dataset parameters.
- Add external reference adapters later; do not couple Phase 1 to ClickHouse or PostgreSQL.

Acceptance: raw machine-readable results and documented methodology; no unsupported performance claims.

## Proposed initial workspace

```text
Cargo.toml
rust-toolchain.toml
crates/
  ananta-types/
  ananta-format/
  ananta-catalog/
  ananta-storage/
  ananta-ingest/
  ananta-core/
docs/
  specifications/native-engine-v0.2.md
  architecture/native-engine.md
  roadmap/phase-1-native-engine.md
```

Add query, optimizer, execution, analytics, time-series and WAL crates when their first interfaces and implementation tasks are ready. Do not create empty crates merely to fill out the diagram.

## Exit criteria

Phase 1 is complete only when:
1. Workspace passes formatting, lint and tests.
2. Database opens and closes without any external database.
3. A native typed table schema persists in ANANTA's catalog.
4. A batch is validated and written in ANF.
5. Full and projected scans reproduce the input values.
6. Corrupt/truncated/unsupported files are rejected safely.
7. Existing ClickHouse benchmark and layout-lab workflows remain unchanged.
8. Tests run with ClickHouse and PostgreSQL unavailable.
9. Documentation clearly states that transactional crash recovery is not yet implemented.

## Engineering sequence

Implement bottom-up: types → format → catalog → storage → ingest → core API → corruption/integration tests → native baseline benchmarks. Each work package should be a small reviewable commit or PR. Keep binary format changes isolated and add golden fixtures whenever the format evolves.
