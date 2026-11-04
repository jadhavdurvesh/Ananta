# Phase 1 — Native Engine Foundation

**Release target:** experimental `0.1.0` storage prototype  
**Outcome:** create a database, define a schema, ingest a batch, write ANF segment files, close/reopen the database and read back identical values.

## Scope

In scope:
- Rust workspace and crate boundaries.
- Typed schema/value model.
- Database directory lifecycle.
- Versioned ANF header, column directory, payload and footer.
- Safe segment writer and reader.
- Batch validation and bounded scans.
- Corruption and round-trip tests.

Out of scope:
- Full SQL engine.
- Query optimizer.
- Distributed system.
- Multi-writer transactions.
- WAL and crash recovery guarantees.
- Compression tuning and secondary indexes.
- Web UI and hosted deployment.

## Work packages

### P1. Workspace bootstrap
- Add root Cargo workspace and pinned Rust toolchain policy.
- Create minimal crate manifests for the initial crates.
- Configure formatting, linting and CI.
- Keep existing Python/Docker workflows functional.

Acceptance: workspace builds and tests; existing reference stack remains unaffected.

### P2. Types and schemas
- Implement Boolean, Int32, Int64, Float64, UTF-8, Timestamp (UTC microseconds) and Binary.
- Implement explicit nullable/non-nullable columns.
- Validate row width and type compatibility.
- Define stable error categories.

Acceptance: unit and property tests cover valid values, nulls, incorrect types and timestamp boundaries.

### P3. Catalog and database lifecycle
- Define local database directory layout.
- Persist versioned database/table metadata.
- Implement open, create-table, list-table and close lifecycle.
- Validate metadata before trusting offsets or lengths.

Acceptance: metadata survives close/reopen and malformed metadata returns an error.

### P4. ANF file format
- Freeze a written v0.1 byte-level specification before declaring compatibility.
- Implement writer and reader for header, metadata, column directory, payload and footer.
- Use explicit endianness and checksums.
- Bound all lengths/offsets and reject unknown mandatory flags.

Acceptance: golden fixtures, round-trip and corruption tests pass.

### P5. Batch ingestion and scan
- Validate complete batches before writing.
- Write segments to a temporary path, flush/sync according to documented policy, then publish.
- Provide full-column and selected-column scans in bounded batches.
- Record per-segment min/max timestamp metadata for pruning.

Acceptance: create → ingest → close → reopen → scan returns exactly the inserted logical values; projection reads do not decode unrelated columns.

### P6. Research baseline
- Create deterministic native-engine datasets and benchmark harness.
- Measure ingestion throughput, scan throughput, projected scan latency, on-disk size and peak memory.
- Record machine, OS, rustc version, build profile and dataset parameters.
- Keep ClickHouse reference runs separate until a common benchmark adapter exists.

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
  ananta-timeseries/
  ananta-core/
docs/
  specifications/native-engine-v0.1.md
  architecture/native-engine.md
  roadmap/phase-1-native-engine.md
```

Do not create empty query/execution/optimizer/WAL crates in the first implementation commit unless a concrete interface is needed. Add them when the first working storage path justifies them.

## Exit criteria

Phase 1 is complete only when:
1. Workspace passes formatting, lint and tests.
2. A database can be created and reopened.
3. A typed table schema persists.
4. A batch is validated and written in ANF.
5. Full and projected scans reproduce the input values.
6. Corrupt/truncated/unsupported files are rejected safely.
7. Existing ClickHouse benchmark and layout-lab workflows remain unchanged.
8. Documentation states clearly that transactional crash recovery is not yet implemented.

## Engineering sequence

Implement bottom-up: types → format → catalog → storage → ingest → core API → tests/benchmarks. Each work package should be a small reviewable commit or pull request. Keep binary format changes isolated and add golden fixtures whenever the format evolves.
