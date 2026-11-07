# Embedded API Contract v0.1

**Status:** M7.1 in progress  
**API version:** `ananta_core::EMBEDDED_API_VERSION = "0.1"`

This document defines the first stable embedded-API contract surface. The API version is independent of persisted file-format versions such as ANF, ANWL, ANCP and ANBK. A future API contract change must not silently reinterpret an existing database directory.

## 1. Compatibility rules

- Public machine-readable identifiers are contractual; human-readable error messages are diagnostic and may become more descriptive.
- Public classification enums are non-exhaustive. Consumers must include a fallback arm when matching them.
- Existing identifier strings retain their meaning. New identifiers are additive.
- Additive Rust API changes may be made within the 0.x line when they preserve existing signatures and semantics. Breaking changes require an explicit API-version decision and migration note.
- Database file-format compatibility is governed by each format specification, not by the embedded API version.
- Configuration structs must gain explicit versioning and documented defaults before they are treated as a stable persisted or wire contract. The current `ExecutionConfig` is an in-process execution budget, not a durable database configuration file.

## 2. Structured error identifiers

Never parse `Display` output to classify an error. Use the typed code and its stable string identifier.

### 2.1 Storage/core

- `DbError::code() -> DbErrorCode`
- `DbErrorCode::as_str() -> &'static str`

| Identifier | Meaning |
|---|---|
| `io` | Operating-system or filesystem I/O failure |
| `format` | ANANTA binary-format validation failed |
| `type` | Schema or value type validation failed |
| `invalid_name` | Database object name is invalid |
| `table_exists` / `table_not_found` | Table identity lookup or creation conflict |
| `column_not_found` / `column_exists` / `invalid_column_order` | Column lookup or schema mutation conflict |
| `corrupt_catalog` / `schema_compatibility` | Catalog integrity or historical schema mismatch |
| `unsupported_catalog_version` | Catalog format version is unsupported |
| `checksum_mismatch` | Catalog checksum validation failed |
| `length_overflow` | A bounded format or size calculation overflowed |
| `database_locked` | Another writer owns the database lock |
| `segment_publication_uncertain` | Segment publication reached an uncertain durability boundary |
| `invalid_batch_id` / `empty_batch` / `batch_id_conflict` | Ingestion identity or payload contract was violated |
| `chronicle_corrupt` / `ingest_row_validation` | Chronicle integrity or row validation failed |
| `capsule_corrupt` / `navigation_map_corrupt` / `navigation_map_stale` | Derived storage artifact integrity/freshness failure |
| `invalid_range` | Capsule range request is invalid |
| `compaction_execution` / `compaction_manifest` / `compaction_preflight` | Compaction input, manifest or execution failure |
| `wal` / `recovery_required` | WAL validation or recovery-required state |
| `backup` | Backup/restore validation or publication failed |

### 2.2 Execution

- `ExecutionError::code() -> ExecutionErrorCode`
- `ExecutionErrorCode::as_str() -> &'static str`
- `ExecutionError::storage_code()` returns the nested `DbErrorCode` when the execution failure wraps a storage error.

Identifiers: `storage`, `aggregate_overflow`, `statistics_format`, `invalid_schema`, `invalid_batch`, `invalid_plan`, `type_mismatch`, `resource_limit`, `arithmetic_overflow`, `cancelled`.

### 2.3 AQL

- `AqlExecutionError.code` is an `AqlErrorCode`.
- `AqlErrorCode::as_str() -> &'static str` returns the stable identifier.

Identifiers: `invalid_query`, `invalid_options`, `table_not_found`, `column_not_found`, `type_mismatch`, `invalid_parameter`, `invalid_cast`, `arithmetic`, `cancelled`, `storage`, `resource_limit`, `execution`.

The AQL error retains its source span. The code is the programmatic classification; the message and source span are diagnostic context.

## 3. Query and result lifecycle currently implemented

AQL `execute` validates and plans a query, then returns a lazy `AqlResultIter`. The iterator yields owned `AqlResultBatch` values:

1. Create the iterator after opening the database and constructing bindings.
2. Pull one result batch at a time with the standard Rust `Iterator` interface.
3. Each batch owns its schema and column values; consumers may retain it after the next call.
4. `None` means the result is complete. An execution error is yielded once and the iterator then finishes.
5. Dropping the iterator releases its owned scan/execution state. No server session or cross-request cursor is implied.

The legacy full-result storage scan remains materialising. Applications needing bounded result memory should use the batch iterator APIs.

## 4. Cancellation and resource budgets currently implemented

The native execution layer supports `ExecutionContext` with an `ExecutionConfig`, a shared cancellation signal, metrics and checks at operator boundaries. The configuration bounds:

- `max_batch_rows`
- `max_batch_cells`
- `max_working_set_bytes`

Defaults are 65,536 rows, 1,000,000 cells and 64 MiB of working-set bytes. `ExecutionOptionsV1` wraps those limits in an explicitly versioned, validated in-process options object; `ExecutionContext::with_options` rejects unsupported versions and zero limits. It is not a persisted database configuration file or a server wire format.

`CancellationToken` is cloneable and thread-safe. All clones share one signal; calling `cancel` on any clone is observed by `ExecutionContext` and AQL execution. New AQL integrations can use `execute_with_cancellation_token`. The legacy `Option<Arc<AtomicBool>>` entry points remain supported as compatibility adapters.

AQL exposes a cloneable `CancellationToken` and a versioned `AqlExecutionOptionsV1` object through `execute_with_v1_options`. The options object composes the AQL-options version, `ExecutionOptionsV1` row/cell/working-set limits, optimizer enablement and an optional cancellation token. Existing `execute`, `execute_with_options` and token-only helpers remain compatibility adapters.

## 5. Unified AQL query options

`AqlExecutionOptionsV1` is the single versioned options surface for AQL query execution. It contains the AQL-options version, validated `ExecutionOptionsV1` limits, optimizer enablement and an optional cloneable cancellation token. Use `execute_with_v1_options(database, query, bindings, options)` for new integrations.

The native path applies row/cell/working-set limits at operator boundaries. The fallback path caps scan batches by row and cell budgets, validates input and result batches, and checks the combined estimated input/output working set. The legacy materialising storage `Database::scan` API remains outside this bounded AQL result contract.

## 6. Runnable embedded example

The recommended end-to-end application example is `crates/ananta-api/examples/embedded.rs`. It imports only the curated `ananta-api` facade, then opens ANANTA's native database, creates a typed table, inserts data, parses and executes an AQL query, consumes owned result batches, reads stable error identifiers, validates versioned execution options, and uses a cloneable cancellation token. Run it from the repository root with:

```bash
cargo run -p ananta-api --example embedded
```

The Native Rust CI workflow builds and executes this example so the recommended facade stays usable.

## 7. Remaining M7.1 work

- Review the curated `ananta-api` re-export list and document which lower-level storage/format modules remain implementation details.
- Extend versioned configuration beyond the in-process `ExecutionOptionsV1` contract when durable database or server-wire configuration is introduced.
- Extend compatibility tests for the unified AQL options object and its invalid-version/resource-limit errors.
- Keep the documented exception for legacy materialising storage scans; all AQL iterator paths now enforce the versioned row/cell/working-set limits.
- Publish complete embedded API examples and compatibility tests.
