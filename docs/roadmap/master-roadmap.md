# ANANTA Master Roadmap

**Document type:** Single source of truth for project execution  
**Product:** Independent Rust-native Database Management System (DBMS)  
**Owner:** DMJ  
**Current release target:** 0.1.0 — Native Storage Prototype  
**Roadmap status:** Active; review this file before starting work outside the current milestone
**Current focus:** M7.1 embedded API hardening; M6 is closed after Linux workspace validation and targeted Windows durability, locking, WAL rotation and backup/recovery checks

---

## 0. How to use this roadmap

This roadmap exists to stop scope drift. Work from top to bottom. A later phase is not permission to begin it early.

### Priority rules

1. Work only on the **Current focus** section unless a task is blocking it.
2. Finish and verify the current milestone before opening the next one.
3. Every implementation task should be a small PR with one main purpose.
4. A feature is not complete because code exists; its tests, failure cases and documentation must also be complete.
5. Do not build Studio screens against mocked engine capabilities and then treat them as delivered product functionality.
6. Do not add a new subsystem merely because it appears in the architecture diagram. Add it when the current milestone needs it.
7. Do not replace the current ClickHouse/PostgreSQL reference experiments. They are comparison infrastructure, not ANANTA runtime dependencies.
8. Do not start distributed execution, a cloud service, a new filesystem, a custom operating-system kernel or GPU execution during the current roadmap.
9. New ideas go into **Parking Lot** first. They enter the active plan only after impact, dependencies and acceptance criteria are understood.
10. If scope or architecture changes, update this roadmap and add an Architecture Decision Record (ADR) in the same PR.

### Status legend

- [ ] Not started
- [x] Implemented in repository (still requires the relevant validation gate)
- [~] In progress / implemented but not yet verified
- [!] Blocked or needs a decision

A checked item means the code/document exists. The milestone exit gate determines whether it is actually complete.

---

# 1. Product boundary — keep this fixed

ANANTA is intended to become a complete, independent database system. Its native engine must create, persist, reopen, query and analyse its own databases without requiring PostgreSQL, ClickHouse, SQLite, DuckDB or another DBMS.

ANANTA owns:
- Native database lifecycle and catalog.
- Database-level storage manager and ANANTA Native Format (ANF).
- Ingestion, segment lifecycle, indexing, compaction and buffer management.
- Native query language (working name: AQL).
- Query planning and execution.
- Analytical and time-series operators.
- Durability, transactions and recovery as they are implemented.
- Server/API, CLI, SDKs and first-party ANANTA Studio.

The operating system still provides filesystem and device I/O. External libraries may provide bounded primitives (for example compression codecs, checksums, cryptography, HTTP and UI components), but they must not supply ANANTA's database engine, catalog, query execution or persistence.

## Current repository reality

Already present on `main`:
- [x] Rust workspace bootstrap.
- [x] `ananta-types`: initial data types, values and schema validation.
- [x] `ananta-format`: experimental ANF encoder/decoder and projected-column decoding.
- [x] `ananta-core`: local catalog, open/create-table/insert/scan APIs.
- [x] Basic telemetry example.
- [x] Initial schema, format, persistence and corruption tests.
- [x] CI workflow definition.
- [x] Existing ClickHouse/PostgreSQL reference schemas and research tools preserved.

Validation evidence:
- [x] PR #4 merged the M0 catalog/scan correctness fixes.
- [x] PR #4 Actions passed formatting, tests, Clippy and the native example.
- [x] CI example verifies a non-empty catalog file and at least one ANF segment.
- [x] Unit/integration tests cover close/reopen, corrupted/truncated catalog and segment cases.

Still pending beyond the M0 exit gate:
- [ ] Validate on Windows.
- [ ] Run the legacy Python benchmark stack independently when that reference environment is next used.
- [ ] Inspect crash behaviour with systematic fault injection (planned in M1.4/M6).

The current native prototype is not production-ready. It currently materialises scan results, assumes exclusive single-writer use, has no cross-process lock, WAL, transactions or crash-recovery guarantee. FNV-1a in the prototype is an accidental-corruption check, not a security mechanism.

---

# 2. Completed milestone — M0: verify the foundation

**Status: completed on 2026-11-05.** The native storage prototype passed the M0 exit gate after PR #4 was merged and its GitHub Actions run succeeded. AQL, analytics, Studio and ChronoWeave compaction remain deferred.

## M0.1 Repository and CI verification
- [ ] Confirm the current `main` branch contains the merged native-engine foundation.
- [ ] Check the latest GitHub Actions run for `main`.
- [ ] Fix Rust compilation failures before adding features.
- [ ] Fix formatting failures.
- [ ] Fix Clippy warnings that indicate correctness or maintainability problems.
- [ ] Run all workspace unit tests.
- [ ] Run the basic example from a clean checkout.
- [ ] Confirm example output and generated directory structure.
- [ ] Confirm the existing Python benchmark tools still work independently.
- [ ] Save the successful CI run URL in the milestone notes.

## M0.2 Establish a reproducible local developer workflow
- [ ] Document required Rust stable toolchain.
- [ ] Document exact clean-clone build commands.
- [ ] Document exact test commands.
- [ ] Document how to run the native example.
- [ ] Document how to remove the example database safely.
- [ ] Confirm Cargo.lock is committed and consistent.
- [ ] Confirm generated files and local databases are ignored.
- [ ] Confirm no secrets are needed to run the native engine.

## M0.3 Correctness review of the current implementation
- [ ] Review all integer conversions and checked arithmetic.
- [ ] Review all file length and offset validation.
- [ ] Review empty table and zero-row batch behaviour.
- [ ] Review duplicate table and duplicate column behaviour.
- [ ] Review table/column name validation.
- [ ] Review nullability validation on write and read.
- [ ] Review UTF-8 and binary length limits.
- [ ] Review invalid boolean encoding rejection.
- [ ] Review unsupported format version rejection.
- [ ] Review unknown feature flag rejection.
- [ ] Review checksum validation.
- [ ] Review partial/truncated catalog rejection.
- [ ] Review partial/truncated segment rejection.
- [ ] Review temporary-file cleanup after write failure.
- [ ] Review catalog update rollback after publication failure.
- [ ] Review deterministic segment ordering.
- [ ] Add tests for every discovered defect before fixing it.
- [ ] Update ANF prototype documentation when bytes or semantics change.

## M0.35 Correctness review actions

The M0 fixes were merged in PR #4 and their own GitHub Actions run passed:
- [x] Propagate directory-entry errors during scans instead of silently dropping entries.
- [x] Distinguish a missing catalog from other filesystem metadata errors.
- [x] Reject truncated catalogs using the actual minimum file size.
- [x] Bound untrusted table/column counts by the bytes remaining before allocating vectors.
- [x] Add regression tests for truncated catalogs, impossible counts, missing table storage directories, empty catalogs and empty batches.
- [x] Clarify that projected decoding still reads the whole segment in ANF v0.1 because checksum verification covers the entire segment.

Validation: [PR #4 CI run — success](https://github.com/jadhavdurvesh/Ananta/actions/runs/41880244252).

## M0.4 Write-access boundary
- [ ] Document that only one exclusive writer may use a database directory.
- [ ] Add a clear error for unsupported concurrent open/write where feasible.
- [ ] Decide how a process-level database lock will work.
- [ ] Add lock-file tests before advertising multi-process safety.
- [ ] Do not claim multi-writer safety yet.

### M0 exit gate
M0 is complete only when:
- [x] PR #4 checks pass formatting, tests, Clippy and the example.
- [x] CI example creates a non-empty catalog and at least one ANF segment.
- [x] Create → schema → insert → close → reopen → scan passes in tests.
- [x] Corrupt and truncated catalog/segment files are rejected.
- [x] Existing reference research source files remain unchanged by the M0 fix PR.
- [x] Known limitations are accurately documented.

**Deliverable:** verified Native Storage Prototype baseline. M0 is closed; remaining platform validation is tracked separately.

---

# 3. Current focus — M1: native storage correctness and streaming

**Goal:** make the native storage path safe, inspectable and capable of processing datasets without materialising the whole result in memory.

## M1.1 Freeze the experimental ANF v0.1 contract
- [x] Review magic bytes and version field.
- [x] Review endian rules.
- [x] Review header and directory lengths.
- [x] Review type codes.
- [x] Review null markers.
- [x] Review UTF-8 and binary encoding.
- [x] Review footer and checksum coverage.
- [x] Set maximum file, row, column and field sizes.
- [x] Add golden binary fixture.
- [x] Add byte-for-byte fixture test.
- [x] Add malformed-directory and payload tests.
- [x] Add deterministic arbitrary-byte no-panic test.
- [x] Define the format-version bump policy.
- [x] Keep the format explicitly experimental until compatibility tests are stable.

**Completed:** [PR #5 merged](https://github.com/jadhavdurvesh/Ananta/pull/5). [CI passed formatting, tests, Clippy and the native example](https://github.com/jadhavdurvesh/Ananta/actions/runs/41990538328).

## M1.2 Improve native catalog

### M1.2.1 Define catalog identity and schema revision model — complete
- [x] Define a stable database-local ObjectId for tables and columns.
- [x] Separate display names from internal object identity.
- [x] Define monotonic ID allocation and no-reuse semantics.
- [x] Separate database catalog revision from per-table schema revision.
- [x] Define rename, drop, add-column and type-change policies.
- [x] Define catalog v1-to-v2 migration safety requirements.
- [x] Define the logical fields required by catalog v2.

Design: `docs/specifications/catalog-v0.2-identity-proposal.md`.
ADR: `docs/decisions/0004-stable-catalog-object-identity.md`.
**Completed:** [PR #6 merged](https://github.com/jadhavdurvesh/Ananta/pull/6); [CI passed](https://github.com/jadhavdurvesh/Ananta/actions/runs/41990915185).

### M1.2.2 Implement catalog v2 — complete
- [x] Add typed ObjectId and catalog object models.
- [x] Implement catalog v2 header, feature flags and revisions.
- [x] Persist table IDs, column IDs and schema revisions.
- [x] Reject zero and duplicate IDs.
- [x] Reject unsupported mandatory feature flags and versions.
- [x] Add v2 golden-byte fixture and corruption tests.
- [x] Implement deterministic v1-to-v2 migration on open.
- [x] Verify failed migration preserves the original catalog and pre-existing temp files.
- [x] Ensure failed catalog writes do not advance in-memory state.

Binary specification: `docs/specifications/catalog-v2-binary-format.md`.
**Completed:** [PR #7 merged](https://github.com/jadhavdurvesh/Ananta/pull/7); [CI passed](https://github.com/jadhavdurvesh/Ananta/actions/runs/41992080767).

### M1.2.3 Schema evolution compatibility — current work

#### M1.2.3.1 Define immutable snapshots and mixed-segment resolution — complete
- [x] Define immutable per-table schema snapshots.
- [x] Define ANF v0.2 table ID/schema revision/column ID metadata.
- [x] Define ANF v0.1 legacy interpretation using revision-1 snapshots.
- [x] Define resolver mapping historical columns by stable ColumnId.
- [x] Define initial compatibility policy for nullable-column addition, rename and reorder.
- [x] Defer unsafe table rename, drop, non-null additions and type changes.
- [x] Define snapshot-first/catalog-second publication and recovery rules.

Design: `docs/specifications/schema-evolution-compatibility.md`.
ADR: `docs/decisions/0005-schema-evolution-needs-immutable-snapshots.md`.
**Completed:** [PR #8 merged](https://github.com/jadhavdurvesh/Ananta/pull/8); [CI passed](https://github.com/jadhavdurvesh/Ananta/actions/runs/41992649744).

#### M1.2.3.2 Implement immutable schema snapshot files — complete
- [x] Define ANS1 v1 sidecar format keyed by table ObjectId and schema revision.
- [x] Implement snapshot encode/decode with bounds, checksum, flags, IDs and schema validation.
- [x] Implement immutable write/read APIs with exclusive temporary-file creation.
- [x] Reject overwrites where an existing revision has different contents.
- [x] Add golden-byte fixture, round-trip, corruption and immutability tests.
- [x] Expose explicit database methods to write/read a current or historical snapshot.
- [x] Add catalog consistency checker for catalog-to-snapshot references.
- [x] Add safe orphan snapshot detection and reporting.

Specification: `docs/specifications/schema-snapshot-ans1-v1.md`.
**Completed:** [PR #9 merged](https://github.com/jadhavdurvesh/Ananta/pull/9); [CI passed](https://github.com/jadhavdurvesh/Ananta/actions/runs/41993765088).

#### M1.2.3.3 Implement ANF v0.2 and segment schema resolver — complete
- [x] Specify ANF v0.2 header and directory with TableId, schema revision and ColumnIds.
- [x] Add ANF v0.2 writer for new inserts.
- [x] Preserve the ANF v0.1 decoder for legacy segments.
- [x] Add segment-version dispatch and ANF v0.2 identity inspection.
- [x] Resolve v0.1 segments against the immutable revision-one snapshot.
- [x] Resolve v0.2 segments against their exact table/revision snapshot.
- [x] Map historical values into current logical column order by stable ColumnId.
- [x] Synthesize NULL only for current nullable columns absent from an older segment.
- [x] Reject wrong table IDs, future revisions, identity mismatches and incompatible type/nullability.
- [x] Add ANF v0.2 golden fixture and malformed identity tests.
- [x] Add mixed v0.1/v0.2 test including a newly added nullable column and close/reopen.
- [x] Pass formatting, workspace tests, Clippy and example CI.
- [x] Merge implementation into main.

**Completed:** [PR #10 merged](https://github.com/jadhavdurvesh/Ananta/pull/10). [CI passed](https://github.com/jadhavdurvesh/Ananta/actions/runs/41995096651).

Specification: `docs/specifications/anf-v0.2-prototype.md`.

### M1.2.3.4 Enable the first safe schema changes — active

#### M1.2.3.4.1 Add nullable column — implementation and CI complete
- [x] Specify `Database::add_nullable_column(table, name, data_type)`.
- [x] Require the current immutable schema snapshot to match the catalog before a change.
- [x] Allocate one new stable ColumnId without changing existing IDs.
- [x] Increment table schema revision and catalog revision with checked arithmetic.
- [x] Publish the new immutable schema snapshot before the catalog pointer.
- [x] Keep the old catalog/in-memory schema active if publication fails.
- [x] Preserve orphan snapshots, permit identical retry and reject conflicting revisions.
- [x] Implement the nullable-column API.
- [x] Test mixed v0.1/v0.2 historical segments with NULL synthesis.
- [x] Test new v0.2 writes at the new schema revision.
- [x] Test close/reopen, full/projected scans, stable IDs and revision counters.
- [x] Test duplicate names, empty names, missing/corrupt/mismatched snapshots, publication failure and counter overflow.
- [x] Pass formatting, workspace tests, Clippy and the native example.

**Validated in [PR #11](https://github.com/jadhavdurvesh/Ananta/pull/11); [latest CI run passed](https://github.com/jadhavdurvesh/Ananta/actions/runs/42017366696).**

Specification: `docs/specifications/nullable-column-ddl-v0.1.md`.

#### M1.2.3.4.2 Rename and reorder columns — complete
- [x] Define rename/reorder snapshot transition and compatibility rules.
- [x] Implement `Database::rename_column` while preserving ColumnId.
- [x] Implement `Database::reorder_columns` while preserving ColumnIds.
- [x] Verify historical v0.1/v0.2 segment mapping by ColumnId after rename/reorder.
- [x] Test new v0.2 writes, close/reopen and projection order.
- [x] Test no-ops, invalid names/permutations, missing/corrupt/inconsistent snapshots, publication failure/retry, immutable conflicts and counter overflow.
- [x] Pass formatting, workspace tests, Clippy and the native example.

**Implemented in [PR #12](https://github.com/jadhavdurvesh/Ananta/pull/12); CI verification recorded after the final PR run.**

Specification: `docs/specifications/column-rename-reorder-v0.1.md`.

### M1.2.3.5 Column drop and table identity changes — not started
- [ ] Define tombstone and historical-read semantics before allowing column drops.
- [ ] Keep table rename/drop disabled until data-directory identity and recovery rules are specified.

Table rename/drop, column drop, type changes and non-null additions remain deferred until path identity, tombstones, backfill and compaction protocols are designed.

## M1.3 Implement bounded read batches — complete
- [x] Define the columnar RecordBatch and streaming iterator contract.
- [x] Set batch row bounds to 1–65,536, with a 1,000,000-cell output ceiling and optional row limits.
- [x] Define a fixed 64 KiB checksum scratch buffer and selected-column batch memory model.
- [x] Implement file-backed batch decoding for fixed-width numeric columns.
- [x] Implement file-backed batch decoding for UTF-8 and binary columns.
- [x] Validate null markers and current schema nullability while decoding batches.
- [x] Expose full and projected scan batch APIs for ANF v0.1/v0.2.
- [x] Add row-limit/early-stop support across segment boundaries.
- [x] Ensure iterator errors are surfaced once and stop iteration.
- [x] Test empty, one-row, exact batch size and batch-size-plus-one boundaries.
- [x] Test projection order, repeated projections and mixed schema revisions.
- [x] Measure peak memory while scanning a generated dataset in isolated Linux processes.
- [x] Demonstrate with a reproducible benchmark that decoded memory scales with configured batch output, not total result rows.

Benchmark: `docs/specifications/bounded-scan-memory-benchmark-v0.1.md`. A 10× row-count increase at 512-row batches changed peak RSS from 4,824 KiB to 5,144 KiB; a 256→1,024→4,096 batch sweep on 50k rows reported 3,992→7,240→20,036 KiB peak RSS.

Specification: `docs/specifications/bounded-read-batches-v0.1.md`.

## M1.4 Safe segment publication — implementation complete
- [x] Define temporary and published segment states; defer obsolete state until deletion/compaction exists.
- [x] Use deterministic monotonically allocated segment IDs under an exclusive local writer lock.
- [x] Document write, flush, file sync and no-overwrite atomic hard-link publication.
- [x] Sync the containing directory after publication and temp-name removal on Unix.
- [x] Document Unix and non-Unix durability guarantees and hard-link requirements.
- [x] Clean up operation-owned temporary files after pre-publication failures.
- [x] Keep temporary names invisible to scans; only final `.anf` names are published segments.
- [x] Fault-inject every publication boundary, including post-publication uncertain outcomes.
- [x] Detect canonical orphan temporary segment files.
- [x] Remove recognized orphan temp files on startup after acquiring the writer lock.
- [x] Enforce one local database handle/process with `.ananta.lock`; fail closed on stale locks.

Specification: `docs/specifications/segment-publication-recovery-v0.1.md`.
ADR: `docs/decisions/0006-atomic-segment-publication-and-writer-lock.md`.

## M1 exit gate
- [x] Golden ANF fixtures pass.
- [x] Deterministic malformed-input tests reject invalid data without panics.
- [x] Catalog survives repeated close/reopen.
- [x] Projected streaming scans work without full-result materialisation.
- [x] Memory is measured and bounded by configured batches for the documented benchmark workload.
- [x] Publication failure tests pass.
- [x] Exclusive-writer boundary is enforced with a fail-closed local lock.

**Deliverable:** Native Storage Core v0.2.

---

# 4. Milestone M2: ingestion and the first ChronoWeave concepts

**Goal:** implement a robust append-oriented ingestion path and determine whether ChronoWeave is a useful storage design.

## M2.1 Ingestion contract — design accepted
- [x] Define caller-supplied 128-bit BatchId scoped by stable TableId.
- [x] Define per-table monotonically increasing 64-bit ingestion sequence assigned at durable commit.
- [x] Define whole-batch accepted/rejected behavior (no partial-row acceptance).
- [x] Define all-or-nothing validation against one captured schema revision.
- [x] Define duplicate-batch idempotency using a canonical payload fingerprint; conflicting ID reuse fails.
- [x] Define accepted BatchId retention for the logical lifetime of the table.
- [x] Define late-arriving timestamp behavior (accept; do not reorder by event time).
- [x] Define out-of-order event behavior and preserve source row order.
- [x] Define initial ANF v0.2 batch limits (rows, cells, columns, field and segment bytes).
- [x] Define synchronous backpressure and publication failure/uncertain-outcome behavior.
- [x] Define IngestResult outcomes and accepted/duplicate/rejected/publication counter semantics.

Specification: `docs/specifications/ingestion-contract-v0.1.md`.
Decision: `docs/decisions/0007-idempotent-commit-ordered-ingestion.md`.

**Status boundary:** M2.1 is a design/contract milestone only. Runtime BatchId, receipt persistence, sequence allocation, result types and counters are not implemented yet; they are explicit M2.2 work.

## M2.2 Chronicle prototype — complete
- [x] Implement the public 128-bit BatchId and IngestResult types.
- [x] Implement deterministic SHA-256 fingerprinting over canonical ANF v0.2 batch bytes and standard known-answer tests.
- [x] Define and implement Chronicle commit-record metadata (BatchId, fingerprint, TableId, schema revision, sequence, row count, encoded bytes and embedded ANF payload).
- [x] Make the Chronicle record the single durable publication point for data identity and sequence.
- [x] Rebuild per-table contiguous sequence state from immutable Chronicle records on open.
- [x] Persist accepted BatchId receipts and resolve identical retries after restart.
- [x] Reject accepted BatchId reuse with a different fingerprint.
- [x] Expose ingestion result and counter snapshots; accepted totals are durable, request counters are process-local.
- [x] Define append order and sequence semantics in Chronicle metadata, independent of event time.
- [x] Publish immutable Chronicle records through the M1.4 no-overwrite durable publication protocol.
- [x] Map incoming batches to Chronicle units while preserving legacy ANF files.
- [x] Add Chronicle envelope validation and an embedded-payload reader for bounded scans.
- [x] Test repeated ingestion and duplicate retry after restart.
- [x] Test conflicting BatchId reuse and TableId scoping.
- [x] Test out-of-order timestamps, source row-order preservation and mixed legacy/Chronicle scans.
- [x] Test pre-commit publication failures and post-commit uncertain outcomes at the Chronicle publication primitive.
- [x] Test schema changes between calls and sequence overflow.
- [x] Benchmark Chronicle startup/index rebuild and ingestion overhead.

Benchmark: `docs/specifications/chronicle-performance-benchmark-v0.1.md`. Release-mode CI measured about 12.5% ingestion-time overhead and 10.8 ms/41.9 ms open time for 25/100 receipts.

Specification: `docs/specifications/chronicle-record-v0.1.md`.
Decision: `docs/decisions/0008-chronicle-record-commit-point.md`.

**Exit status:** M2.2 is complete. Identity, durable receipts, sequence allocation, retry/recovery, scan compatibility, schema-evolution and overflow tests pass; release-mode ingestion and startup-index costs are recorded. Capsule design and implementation are now the active M2.3 work.

## M2.3 Capsule prototype — complete
- [x] Define immutable Capsule identity and explicit generation.
- [x] Store row count, TableId, stable ColumnIds and schema revision reference.
- [x] Add optional temporal min/max bounds using an explicitly selected TIMESTAMP_MICROS column.
- [x] Add typed per-column min/max for BOOLEAN, INT32, INT64, TIMESTAMP_MICROS and FLOAT64.
- [x] Add per-column null counts, including all-NULL columns.
- [x] Record exact encoded byte lengths from the embedded ANF v0.2 column directory.
- [x] Add SHA-256 payload fingerprint and whole-Capsule checksum.
- [x] Add independent Capsule validator that verifies the wrapper, schema snapshot, embedded ANF and recomputed statistics.
- [x] Build Capsules from contiguous, committed Chronicle sequence ranges while preserving historical ColumnId resolution.
- [x] Publish Capsules immutably beside Chronicle data; keep source Chronicle files intact.
- [x] Compare 512-row, 2,048-row and 8,192-row Capsules on identical datasets.
- [x] Record Capsule build overhead, storage amplification, map build and scan behaviour.

Benchmark: docs/specifications/capsule-size-scan-benchmark-v0.1.md.
Specification: docs/specifications/capsule-format-v0.1.md.
Decision: docs/decisions/0009-immutable-capsules-as-derived-chronicle-units.md.

**Exit status:** M2.3 is complete. The size benchmark demonstrates the expected pruning-granularity trade-off; smaller Capsules skip more irrelevant units for the tested time range.

## M2.4 Navigation Map prototype — correctness-first pruning implemented
- [x] Define a versioned metadata structure separate from Capsule payloads (NVM1, immutable map generations).
- [x] Add safe typed range pruning using per-column min/max and explicit temporal metadata.
- [x] Add selected-column identity and metadata through stable ColumnIds and per-column statistics.
- [x] Verify no false negatives for available statistics; missing statistics always retain Capsules.
- [x] Test inclusive/exclusive time/range boundaries.
- [x] Record Capsules considered, skipped and read.
- [x] Add explainable diagnostics for rows examined/returned, verified bytes and decoded payload bytes.
- [x] Reject stale maps when the Capsule file set or fingerprint changes.
- [x] Reject overlapping source sequence ranges to prevent duplicate logical rows.

Specification: docs/specifications/navigation-map-v0.1.md.

**Scope boundary:** the map verifies complete Capsule file fingerprints before pruning. It can reduce ANF decoding but does not yet guarantee fewer physical bytes read. Generation promotion, active manifests and partial-page reads remain M3 work.

## M2 exit gate — complete
- [x] Ingestion is deterministic and validates whole batches.
- [x] Chronicle units are immutable after publication.
- [x] Capsules can be independently read and verified.
- [x] Navigation Map pruning is correctness-tested.
- [x] Query results are identical with pruning enabled or disabled.
- [x] Ingestion and storage overhead are recorded.

**Deliverable:** ChronoWeave research prototype v0.1 (Chronicle, derived Capsules and correctness-first Navigation Map). This is not yet adaptive production storage: there is no Weave compaction, active-generation manifest, promotion/rollback or guaranteed page-level I/O pruning.

---

# 5. Milestone M3: compaction and physical organisation

**Goal:** test whether reorganising Capsules into Weaves improves actual workloads.

## M3.1 Compaction foundation
- [x] Define compaction input and output manifests.
- [x] Define source-generation references.
- [x] Define compaction job states.
- [x] Define temporary output handling.
- [x] Define duplicate/missing row detection through stable row provenance.
- [x] Define compaction memory and disk limits.
- [x] Define compaction cancellation boundaries.
- [x] Define safely discardable attempts and fresh-attempt retry semantics.
- [x] Implement deterministic compaction job transitions, a fixed-width checksummed event codec and strict journal replay validation.
- [x] Implement finite resource-budget validation for input/output/temp bytes, working memory, rows, open files and work-unit size.
- [x] Persist append-only CJE1 job events with file sync and verify the complete journal on read.
- [x] Recover unfinished attempts by appending one Interrupted event; reject partial/corrupt tails without truncating evidence.
- [x] Implement canonical CIM1 input manifest codec, SHA-256 fingerprinting and structural Capsule/Chronicle coverage validation.
- [x] Implement canonical OWM1 output manifest codec, artifact fingerprints, canonical relative paths and provenance-count checks.
- [x] Verify source Capsule and Chronicle files against CIM1 length/SHA-256 references before compaction; validate Capsule checksums, statistics and target-schema compatibility.
- [x] Verify candidate output files and PVM1 provenance sidecars against OWM1 before publication; require exact row-origin and typed-fingerprint set equality.
- [x] Add budget-checked candidate execution with attempt-owned staging, chunked ANF-v2 output artifacts, work-unit cancellation and CJE1 progress.
- [x] Implement append-only, time-first and entity-first candidate ordering with deterministic origin tie-breaking.
- [x] Record compaction wall/process CPU time, temporary disk amplification, final storage size and five warm read/decode samples for each layout in the CI benchmark artifact; document the current full-scan experiment as not beneficial because the reader does not exploit physical order.
- [x] Evaluate mixed layout; not justified by the current query/read workload, so it is intentionally not implemented.
- [x] Define AWP1 active-generation pointer retaining the previous immutable generation.
- [x] Verify full logical table equivalence before atomic promotion.
- [x] Publish active pointer with same-directory atomic replacement and directory sync.
- [x] Implement rollback by pointer swap while retaining both generations.
- [x] Recover invalid active candidate to retained previous generation on database open.
- [x] Test interruption at each active-pointer publication boundary and recovery from a missing/corrupt active candidate using the retained previous generation.
- [x] Add bounded compaction execution, attempt-owned staging and crash-safe candidate publication.
- [x] Add database-level compaction correctness and fault-injection tests for output tampering, row-lineage mismatch, cancellation isolation, publication interruption and restart recovery.

Specification: docs/specifications/compaction-manifest-v0.1.md.
Decision: docs/decisions/0010-compaction-input-snapshots-and-candidate-outputs.md.

## M3.2 First Weave strategies
- [x] Implement baseline append-only layout.
- [x] Implement time-first candidate.
- [x] Implement entity-first candidate.
- [x] Evaluate mixed candidate; not justified by the current measured full-scan workload, so it is intentionally deferred.
- [x] Preserve logical row equivalence across all layouts with one-to-one provenance and canonical typed-row fingerprints.
- [x] Record compaction wall duration and Linux process CPU time (or null when unavailable) in the benchmark output.
- [x] Record staged temporary bytes and disk amplification relative to source bytes.
- [x] Record total final candidate storage bytes, including OWM1 and CIM1.
- [x] Record five warm artifact read/decode latency and process CPU samples for each layout.

## M3.3 Generation promotion
- [x] Define active generation pointer.
- [x] Build candidate generation off the active path.
- [x] Verify row counts and checksums.
- [x] Run reference query equivalence checks.
- [x] Publish the new pointer safely after candidate verification.
- [x] Retain previous generation until verification completes.
- [x] Implement rollback to previous generation.
- [x] Test interruption before and after atomic pointer replacement; after replacement the complete new pointer is visible and recovery revalidates it.
- [x] Test recovery when the current active candidate is missing an artifact; startup selects the retained verified previous generation.

## M3 exit gate
- [x] Multiple Weave candidates (append-only, time-first, entity-first) produce identical logical row fingerprints and exact provenance inventories.
- [x] Cancellation and output-tampering tests prove the active pointer and previous candidate remain unchanged until a verified promotion.
- [x] Promotion/rollback and each active-pointer publication failure boundary are tested; restart recovery validates the selected candidate.
- [x] The current full-table reader does not use physical ordering for pruning; the 10k-row three-layout experiment is documented as not beneficial for current read operations, with raw measurements uploaded by CI.

**Deliverable:** ChronoWeave compaction research report and prototype.

---

# 6. Milestone M4: AQL native query language

**Goal:** create a native query language with semantics designed around ANANTA rather than implementing SQL compatibility as the core interface.

Do not start the full AQL parser before M1 storage and batch read interfaces are stable.

## M4.1 Language design
- [x] Define AQL's goals and non-goals.
- [x] Define identifiers and quoting.
- [x] Define literals.
- [x] Define data types and casts.
- [x] Define null semantics.
- [x] Define comparison semantics.
- [x] Define boolean logic.
- [x] Define error locations and diagnostics.
- [x] Define parameter binding.
- [x] Define language versioning.
- [x] Write grammar document.
- [x] Add grammar examples and invalid examples.

Specification: docs/specifications/aql-v0.1.md

## M4.2 Parser and representation
- [x] Implement tokenizer/lexer.
- [x] Test string and numeric literals.
- [x] Test escaped identifiers.
- [x] Test malformed input diagnostics.
- [x] Implement parser.
- [x] Define AST nodes.
- [x] Implement structural AST validation.
- [x] Define logical-plan representation (Scan → Filter → Project → Limit).
- [x] Add parser golden tests and rejected-grammar cases.

## M4.3 First query capabilities
- [x] Select source table with quoted/unquoted catalog resolution.
- [x] Project selected columns and expressions.
- [x] Filter with typed predicates and three-valued NULL logic.
- [x] Limit rows globally across bounded input batches.
- [x] Add timestamp-range filtering using typed TIMESTAMP_MICROS comparisons.
- [x] Bind named and positional parameters as typed values; never interpolate source text.
- [x] Return owned typed columnar result batches.
- [x] Add cooperative query cancellation checks.
- [x] Add stable query execution error codes.

## M4 exit gate
- [x] AQL grammar and semantics are documented.
- [x] Lexer, parser, AST validation and golden tests pass.
- [x] Query results match equivalent direct Rust API operations.
- [x] Query execution uses ANANTA's native storage and does not rely on an external database.
- [x] A native telemetry AQL example runs in CI using the embedded API.

**Deliverable:** AQL v0.1 (read-only, single-table SELECT; joins, aggregation, ORDER BY and physical pruning deferred).

---

# 7. Milestone M5: native execution and analytics

**Goal:** build the actual analytical engine as native execution operators over ANANTA storage batches.

## M5.1 Execution model
- [x] Define vector batch schema.
- [x] Define operator interface.
- [x] Define batch ownership/lifetime rules.
- [x] Define operator errors and cancellation.
- [x] Define memory limits and resource accounting.
- [x] Implement scan operator.
- [x] Implement projection operator.
- [x] Implement filter operator.
- [x] Implement limit operator.
- [x] Add operator-level unit tests.

Specification: docs/specifications/vector-execution-v0.1.md.

**M5.1 status:** Complete as the first reusable operator runtime. The canonical vector batch reuses ananta-core RecordBatch; scan/projection/filter/limit are pull-based, filters use three-valued NULL logic, cancellation is cooperative, and the execution context enforces row/cell/working-set ceilings. AQL is not yet routed through these operators; that integration is planned for M5.4.

## M5.2 Aggregation — complete
- [x] Define aggregate state interface.
- [x] Implement count (COUNT(*) and COUNT(column)).
- [x] Implement SUM for INT32, INT64 and FLOAT64.
- [x] Implement MIN and MAX for supported ordered scalar types.
- [x] Implement AVG for INT32, INT64 and FLOAT64.
- [x] Define integer overflow behaviour (checked i128 accumulator; checked INT64 output).
- [x] Define floating-point determinism policy (source-order IEEE-754 accumulation).
- [x] Define NULL handling for each aggregate.
- [x] Implement deterministic GROUP BY over canonical typed keys.
- [x] Test empty scalar input and empty grouped input.
- [x] Test high-cardinality groups, bounded output batches and group-state memory limit.

Specification: docs/specifications/aggregation-v0.1.md.

**Exit status:** M5.2 passed the full Native Rust GitHub Actions workflow, including formatting, all workspace tests, Clippy, native AQL/storage examples and the existing storage/ChronoWeave benchmarks.

## M5.3 Time-series analytics — implementation submitted for CI validation
- [x] Define time bucket semantics (fixed-width, epoch-anchored, left-closed/right-open).
- [x] Define UTC and signed microsecond behavior (including Euclidean floor for pre-epoch timestamps).
- [x] Implement inclusive/exclusive timestamp range filter.
- [x] Implement bucketed COUNT through the shared aggregate engine.
- [x] Implement bucketed MIN/MAX/AVG through the shared aggregate engine.
- [x] Implement latest-value-per-series.
- [x] Define missing-interval behavior (no synthetic empty buckets).
- [x] Test late-arriving records and negative/boundary timestamps.

Specification: docs/specifications/time-series-execution-v0.1.md.

**Validation boundary:** time-series operators reuse the M5.2 group-state and resource controls. Timezone conversion, calendar buckets, gap filling, windows, watermarks and AQL integration remain deferred.

## M5.4 Planner and optimisation — partial implementation, validation pending
- [x] Define logical plan.
- [x] Define initial physical operator pipeline.
- [x] Implement direct-column projection pushdown into storage scan.
- [x] Implement supported WHERE predicate execution through native filter.
- [x] Implement safe LIMIT pushdown when no filter can discard rows.
- [x] Add physical EXPLAIN output.
- [x] Add table/column analysis statistics (explicit bounded scan; exact null/min/max, approximate distinct).
- [x] Bind analysis statistics to table/schema identity and immutable scan-inventory fingerprint; reject stale statistics after writes/Weave changes.
- [x] Add initial heuristic cardinality and projected-byte estimates with explicit provenance.
- [x] Refine equality, NULL and numeric range selectivity using analyzed NDV, null fraction and min/max statistics.
- [x] Add versioned, checksummed statistics snapshot encode/decode, atomic save/load, and a database-managed cache path.
- [x] Build bounded deterministic sample histograms (256-value sample, up to 16 buckets) and use them for numeric/timestamp range selectivity.
- [x] Add explicit analyze-and-persist and load-if-fresh APIs for the database-managed statistics cache.
- [x] Reuse a fresh database-managed statistics snapshot automatically in EXPLAIN without triggering an implicit full-table analysis scan.
- [x] Re-export the analyze/persist/load statistics workflow and snapshot types from the AQL crate public API.
- [x] Automatically invalidate and discard stale/corrupt analysis caches after writes, schema changes or Weave promotion; expose explicit re-analysis via analyze_table_and_persist.
- [x] Add transparent weighted physical plan cost estimates for scanned rows, estimated projected payload, filter evaluations and output cells, with explicit assumptions and unknown-value propagation.
- [x] Expose configurable milli-unit cost weights via PlannerOptions and report the exact weights used by each EXPLAIN result.
- [x] Add a reproducible analytics demo that compares estimated scan/output rows and projected payload with measured reference scan/profile counters.
- [x] Add a deterministic six-workload cost-calibration harness that compares EXPLAIN estimates with measured scan/output profiles and emits CSV observations.
- [ ] Review repeated benchmark runs and calibrate default cost weights; persist additional analysis metadata in the catalog.
- [x] Integrate time/Capsule pruning and exact-row reporting behind verified Chronicle sequence/row coverage, legacy-ANF exclusion, map-generation and materialisation-limit gates.
- [x] Add optimizer-disabled reference execution and compare simple optimized query results.
- [x] Test aliases/repeated projections, NULL predicates, computed-expression fallback and safe LIMIT decisions.
- [x] Record measured decoded scan input batches, rows and value bytes alongside returned output metrics.
- [x] Compare analyzed row estimates with measured output rows and decoded scan input bytes on representative fixtures.
- [x] Record runtime output batches, rows, cells and measured value-body bytes; keep owned-batch footprint estimate separate.

Specification: docs/specifications/physical-planning-v0.1.md.

**M5.4 scope note:** AQL direct-column projections and supported predicates use native scan/filter/project/limit operators. Computed projection expressions fall back to the original evaluator. Cardinality/byte figures are heuristic, while analyze_table performs an explicit bounded full scan. Vectorized time-range queries may use Capsule/Navigation Map pruning only after the core proves complete Chronicle coverage and excludes legacy ANF files; otherwise they fall back to the authoritative table scan.

## M5 exit gate
- [x] First AQL queries execute through native operators.
- [x] Aggregate results pass operator/reference expectation tests.
- [x] Time-series boundary/null semantics are covered.
- [x] Query results remain identical with optimiser on/off for the supported comparison, boolean and NULL predicate set.
- [x] Query profiling reports measured decoded scan input and returned output batches, rows and value bytes; physical file/disk I/O bytes and per-operator timings remain outstanding.

**Deliverable:** Native Analytics Engine v0.1.

---

# 8. Milestone M6: durability, concurrency and recovery

**Goal:** move from an experimental single-writer prototype toward a reliable DBMS.

This milestone is a hard prerequisite for production use. Do not imply transactional safety before it passes.

## M6.1 Write-ahead log — standalone framing and append primitives implemented
- [x] Define WAL record format and version.
- [x] Define record checksums (SHA-256 over record content).
- [x] Define sequence and transaction commit/abort markers.
- [x] Define flush/sync policy (manual, on commit, every record).
- [x] Define segment/catalog relationship to WAL and the future M6.2 replay/publication boundary.
- [x] Implement WAL writer with append-only frames and transaction digest tracking.
- [x] Implement WAL reader with header, length, checksum, sequence and transaction-state validation.
- [x] Test truncated final length prefixes and frame bodies; retain the last verified byte offset.
- [x] Test complete corrupt frames, checksum mismatch, sequence gaps and bad commit digests.
- [x] Test open transaction reporting, explicit abort after reopen, sync modes and invalid transaction ordering.

**M6.1 boundary:** ANWL v1 framing is now used by the M6.2 ANF insert and Chronicle ingest publication paths. Catalog DDL, compaction/Weave publication, checkpoint installation and general transaction semantics remain outside this boundary; no general ACID guarantee is claimed.

## M6.2 Recovery — immutable data, metadata, Capsule and Navigation Map WAL replay integrated (validation outstanding)
- [x] Define startup recovery algorithm for immutable ANF/Chronicle artifacts.
- [x] Define committed-only redo semantics and handling of incomplete transactions.
- [x] Define versioned AMUT v1 mutation-intent envelope with canonical file names, size and SHA-256.
- [x] Define staged-file -> durable WAL COMMIT -> no-replace final publication ordering.
- [x] Implement streaming checksum-verified staging and idempotent final publication helpers.
- [x] Implement recovery-plan classification and replay driver for committed/incomplete/aborted transactions.
- [x] Run WAL recovery in Database::open before Chronicle receipt index reconstruction.
- [x] Synchronise WAL parent-directory creation and newly created table-directory entries before relying on staged artifacts.
- [x] Integrate WAL intents with Database::insert_batch (ANF) and Database::ingest_batch (Chronicle).
- [x] Rebuild Chronicle sequence/BatchId receipt state from recovered published files.
- [x] Mark the Database handle recovery-required if a WAL append/commit/publication outcome is uncertain.
- [x] Test normal ANF/Chronicle writes across close/reopen.
- [x] Fault-inject the stop after durable ANF COMMIT and before final publication; verify startup replay.
- [x] Fault-inject the stop after Chronicle COMMIT and before final publication; verify receipt rebuild and idempotent duplicate BatchId retry.
- [x] Test incomplete intent cleanup and durable ABORT on startup.
- [x] Test torn COMMIT frame truncation followed by incomplete-intent cleanup.
- [x] Test Database::open fails closed and preserves a conflicting final artifact.
- [x] Verify ANF embedded TableId/schema revision and Chronicle record TableId/schema revision against each committed intent before publication.
- [x] Preserve `DbError::Io` for pre-commit staging conflicts and `DbError::ChronicleCorrupt` for checksum failures in committed Chronicle records.
- [x] Add deterministic test-only interruption checkpoints after staging create/write/flush/file sync/directory sync, WAL BEGIN/MUTATION/COMMIT, final-name link, publication directory sync, staging-name removal and cleanup directory sync.
- [x] Exercise the ANF insert and Chronicle ingest paths at every checkpoint; reopen twice, verify row/file counts, and verify Chronicle receipt reconstruction plus duplicate BatchId resolution.
- [x] Add subprocess-termination tests at WAL MUTATION, durable COMMIT and final-name link for both ANF and Chronicle; reopen after process exit and reopen a second time to verify rows, receipts, and artifact counts.
- [x] WAL-protect `create_table`, nullable-column addition, column rename and column reorder with ordered schema-snapshot + versioned-catalog multi-intent transactions; replay committed catalog mutations before later data writes.
- [x] Add in-process pre-COMMIT DDL interruption coverage for staging and incomplete WAL intents, plus in-process and subprocess crash/reopen coverage after WAL COMMIT, schema publication, versioned catalog publication, canonical replacement-temp sync, atomic rename before directory sync, and completed canonical catalog replacement.
- [x] WAL-protect immutable compaction candidate-directory/OWM publication with a versioned manifest intent; recover staged-to-final directory rename before compaction journal reconciliation.
- [x] WAL-protect active Weave promotion, rollback and damaged-generation fallback using immutable versioned AWP snapshots; replay committed pointers before active-generation validation.
- [x] Integrate Capsule publication with explicit WAL intent, immutable Capsule ID/generation naming, format/identity verification and startup redo.
- [x] Integrate Navigation Map publication with explicit WAL intent, map identity verification, and referenced Capsule length/SHA-256/metadata validation during redo.
- [x] Add post-COMMIT interruption/reopen/idempotence coverage for Capsule and Navigation Map publication.
- [x] Add subprocess crash/reopen coverage for Capsule and Navigation Map at WAL MUTATION, durable COMMIT and final-name link; verify data, artifacts, consistency and a second reopen.
- [x] Add ANCP v1 immutable checkpoint manifests with SHA-256 inventory of stable database artifacts and a WAL checkpoint reference.
- [x] Validate checkpoint references and recorded artifacts on startup; validate mutable catalog/active-pointer fingerprints unless a committed post-checkpoint intent owns the update.
- [x] Atomically rotate ANWL on Unix and Windows to a checkpoint anchor at previous high-water sequence + 1 after manifest publication; preserve monotonic IDs and redo for all later WAL transactions. Unix uses same-volume rename and directory sync. Windows syncs/closes the current WAL handle, uses `MoveFileExW(REPLACE_EXISTING | WRITE_THROUGH)`, then reopens the replacement; no delete-then-rename fallback is used.
- [x] Add subprocess crash/reopen coverage at checkpoint-manifest temp sync/publication and WAL temp sync/rename/directory-sync boundaries.
- [x] Extend Capsule/Navigation Map subprocess crash coverage to all 12 staging, WAL, final-publication and cleanup checkpoints for both artifact types.
- [x] Retain only the checkpoint manifest referenced by the durable active WAL; prune superseded manifests only after validating the new anchor.
- [x] Implement and validate Windows atomic WAL replacement; do not use delete-then-rename.

**Guarantee boundary:** ANF insert, Chronicle ingest, table creation, nullable-column addition, column rename, column reorder, active Weave promotion/rollback/fallback pointers, immutable compaction candidate-directory/OWM publication, Capsule publication and Navigation Map publication use WAL-backed redo. Capsule intent uses the AMUT v1 revision field for its independent generation; the embedded Capsule header retains and validates the logical schema revision. Navigation Map redo verifies all referenced immutable Capsule files before accepting the map. General multi-operation application transactions remain outside this boundary; do not claim general ACID semantics.

Specification: docs/specifications/wal-recovery-v0.1.md.

## M6.3 Transactions and concurrency
- [x] Define current transaction scope: each WAL-backed immutable publication or catalog DDL operation commits as one atomic redo unit; callers cannot group multiple API calls into one transaction.
- [x] Define current isolation boundary: one exclusive database owner process/handle; shared-reference reads may run concurrently, while mutations require exclusive mutable access.
- [x] Enforce a database-wide exclusive writer lock and reject a second opener.
- [x] Add read-only lock inspection reporting the lock path and owner PID without deleting stale locks.
- [x] Add concurrent shared-handle reader coverage.
- [x] Define schema/catalog concurrency: one writer owns the database; Rust exclusive mutable access serializes mutations, and readers observe the committed catalog held by their handle.
- [x] Defer MVCC: it is not justified for the current single-process, single-writer embedded contract; revisit when multi-writer/server workloads are introduced.
- [x] Add lock-conflict and idempotency/conflicting-BatchId regression tests.
- [x] Define cancellation boundary: scan iterators can be cancelled by dropping them; the current nonblocking lock acquisition and absence of public multi-call transactions mean transaction wait-timeout APIs are not applicable yet.
- [x] Document per-operation durability and the explicit absence of general multi-operation ACID transactions.
- [x] Add repeated cross-process writer-lock denial probes while another process owns the database.
- [x] Add platform-specific lock validation and multi-process stress on supported operating systems (Unix full-suite subprocess probes; Windows targeted cross-process writer-lock validation).

## M6.4 Operational integrity
- [x] Add bounded-memory database consistency checker for WAL, logical scans, Chronicle, Capsules and Navigation Maps.
- [x] Add storage inventory with artifact classes and byte counts.
- [x] Add read-only orphan/temp/staging path reporting.
- [x] Add versioned ANBK v1 full-directory backup format with manifest SHA-256 and per-file fingerprints.
- [x] Add backup verification and fresh-directory restore validation through normal database open/recovery plus consistency scan.
- [x] Execute backup/restore into a new directory and backup-manifest corruption rejection tests on the Linux workspace suite and the Windows targeted backup/restore job.
- [x] Document corruption repair boundaries and fail-closed recovery guidance.
- [x] Add backup subprocess interruption tests after copied-file sync, manifest sync and final directory rename; verify incomplete backup absence or complete backup restore.
- [x] Add and execute a multi-artifact backup/restore workload with 8,192 rows, 64 Chronicle records, 16 Capsules and one Navigation Map.
- [x] Add and execute large-database and cross-platform backup/directory-publication tests, including Windows subprocess crashes after copied-file sync, manifest sync and final directory publication.

## M6 exit gate
- [x] Fault-injection tests pass (complete Linux workspace suite plus targeted Windows WAL and backup subprocess recovery tests).
- [x] Recovery is deterministic and repeatable (reopen/idempotence, active-Weave fallback, WAL/checkpoint and backup crash boundaries).
- [x] Transaction semantics are documented and tested (per-operation atomic redo units; no general multi-operation ACID or MVCC claim).
- [x] Locking rules are enforced (exclusive writer ownership, lock inspection, repeated Linux subprocess denial and Windows cross-process denial).
- [x] Backup/restore integrity tests pass (manifest corruption, fresh restore, 8,192-row multi-artifact workload and Linux/Windows publication crash tests).

**Deliverable:** M6 is closed for the single-node, single-writer engine contract. Linux workspace formatting, all-target tests, Clippy, native examples and benchmark suite pass; targeted Windows validation passes for compilation, atomic WAL replacement, writer locking, large backup/restore and backup-publication crash recovery. No cluster or general multi-operation ACID claims. Physical power-loss/device-cache behavior is not certified; Windows write-through publication is not claimed to provide the same parent-directory flush semantics as Unix directory `fsync`.

---

# 9. Milestone M7: native server and developer interfaces

**Goal:** expose the engine to applications without making the browser dashboard a dependency.

## M7.1 Embedded API hardening
- [~] Stabilise public Rust API boundaries: publish API version, typed error codes and first versioned execution-options surface; facade visibility review remains.
- [~] Define configuration compatibility: add validated in-process ExecutionOptionsV1; durable database and server-wire configuration contracts remain pending.
- [x] Define structured error codes with stable machine-readable string identifiers for database, execution and AQL errors; keep display text diagnostic-only.
- [x] Define query/result lifecycle: lazy AqlResultIter yields owned batches, ends at None, and yields terminal errors once; document dropping the iterator boundary.
- [x] Add cancellation API: cloneable CancellationToken shared across threads, ExecutionContext integration and token-aware AQL entry point; retain legacy Arc adapter.
- [~] Add resource limits: version ExecutionOptionsV1 over row/cell/working-set budgets; uniform propagation across all query paths remains.
- [ ] Publish API examples.

## M7.2 Server/session layer
- [ ] Decide process architecture.
- [ ] Define versioned native wire protocol.
- [ ] Define handshake and protocol version.
- [ ] Define authentication requirements.
- [ ] Define session lifecycle.
- [ ] Define request IDs and cancellation.
- [ ] Define result streaming.
- [ ] Add connection limits.
- [ ] Add idle/session timeout.
- [ ] Add protocol fuzz tests.

## M7.3 Client tools
- [ ] Build ANANTA CLI.
- [ ] Add database create/list/inspect commands.
- [ ] Add table/schema inspection.
- [ ] Add AQL execution command.
- [ ] Add import/export commands only after format contracts are stable.
- [ ] Add Rust client documentation.
- [ ] Design future language SDKs from the protocol, not by duplicating engine logic.

## M7 exit gate
- [ ] Native engine works embedded and as a standalone server.
- [ ] Client can connect without PostgreSQL/ClickHouse.
- [ ] Protocol is versioned and tested.
- [ ] Cancellation, timeouts and resource limits work.

**Deliverable:** ANANTA Server and CLI v0.1.

---

# 10. Milestone M8: ANANTA Studio

**Goal:** build the first-party dashboard over the real native server/management API.

Do not make Studio read raw ANF files or invent telemetry values. Do not start this milestone before M7 has a stable API and the relevant engine metrics exist.

## M8.1 Studio foundation
- [ ] Choose frontend stack and document it.
- [ ] Define Studio visual design system.
- [ ] Define API compatibility/version handling.
- [ ] Define authentication/session integration.
- [ ] Define error/loading/empty states.
- [ ] Define accessible navigation and keyboard support.

## M8.2 Core pages
- [ ] System overview.
- [ ] Database explorer.
- [ ] Table and schema detail.
- [ ] Data preview using bounded result APIs.
- [ ] AQL workbench.
- [ ] Query result grid.
- [ ] Query history.
- [ ] Saved queries.

## M8.3 Advanced pages
- [ ] Visual Explain.
- [ ] Storage Explorer.
- [ ] Chronicle/Capsule/Weave viewer.
- [ ] Analytics Studio.
- [ ] Ingestion Monitor.
- [ ] Performance Lab.
- [ ] Administration.
- [ ] Backup/restore interface.

## M8.4 Security and UX
- [ ] Enforce permissions in the server, not only frontend controls.
- [ ] Avoid exposing credentials in logs/history.
- [ ] Add session expiry handling.
- [ ] Add cancellation for long queries.
- [ ] Add clear resource-limit feedback.
- [ ] Test API/Studio version mismatch.
- [ ] Test Studio unavailable while the engine continues operating.

## M8 exit gate
- [ ] Studio talks only to documented ANANTA APIs.
- [ ] Every displayed metric comes from real engine telemetry.
- [ ] User permissions are server-enforced.
- [ ] Core engine remains operable without Studio.

**Deliverable:** ANANTA Studio v0.1.

---

# 11. Milestone M9: benchmark, release and hardening

**Goal:** produce defensible research results and a usable single-node beta.

## M9.1 Benchmark methodology
- [ ] Define standard synthetic workloads.
- [ ] Define telemetry/time-series workloads.
- [ ] Define uniform and skewed distributions.
- [ ] Define bursty and late-arriving input.
- [ ] Define correctness oracle for each query.
- [ ] Record CPU, memory, disk and OS details.
- [ ] Record Rust version and build profile.
- [ ] Separate cold and warm measurements.
- [ ] Run multiple repetitions.
- [ ] Store raw machine-readable results.
- [ ] Compare with ClickHouse and other relevant systems on equivalent workloads.
- [ ] Publish limitations and negative results.

## M9.2 Production hardening
- [ ] Review threat model.
- [ ] Review authentication and authorization.
- [ ] Review input and file parser attack surface.
- [ ] Review resource exhaustion limits.
- [ ] Review backup and restore.
- [ ] Review logs for secret leakage.
- [ ] Review dependency vulnerabilities.
- [ ] Add release build reproducibility.
- [ ] Add versioned migration/upgrade policy.
- [ ] Add operational runbook.

## M9.3 Release preparation
- [ ] Define supported platforms.
- [ ] Define installation packages.
- [ ] Define upgrade compatibility.
- [ ] Define data directory compatibility.
- [ ] Define release notes template.
- [ ] Define known limitations.
- [ ] Tag a pre-release.
- [ ] Test installation on a clean machine.
- [ ] Test upgrade from the previous prototype.
- [ ] Publish benchmark methodology and raw data.

## M9 exit gate
- [ ] Correctness and recovery gates pass.
- [ ] Security and resource-limit review completed.
- [ ] Benchmark report is reproducible.
- [ ] Installation and upgrade tested.
- [ ] Known limitations are public.

**Deliverable:** ANANTA Single-Node Beta.

---

# 12. Later research — parking lot, not current work

These ideas are deliberately not in the active execution path. Revisit only after a stable single-node engine exists.

- [ ] Multi-node distributed query execution.
- [ ] Replication and consensus.
- [ ] Sharding and rebalancing.
- [ ] Multi-region operation.
- [ ] Advanced adaptive/automatic Weave selection.
- [ ] Approximate analytics and sketches beyond the first measured needs.
- [ ] SIMD and specialised hardware optimisation.
- [ ] GPU execution.
- [ ] Columnar joins and advanced window functions.
- [ ] Multi-tenant cloud control plane.
- [ ] Managed hosted ANANTA service.
- [ ] Alternate storage media research.
- [ ] Experimental transport/network protocol.

---

# 13. Parking Lot — ideas that must not interrupt active work

Add new ideas here with:
- Problem it solves.
- Who needs it.
- Which milestone it belongs to.
- Dependencies.
- A measurable acceptance test.
- Why it cannot wait until its planned milestone.

No parked idea becomes active without an ADR/roadmap update.

| Idea | Reason parked | Revisit gate |
|---|---|---|
| New query syntax experiments | Storage and type semantics must stabilise first | After M1 |
| Online adaptive storage | Can create unsafe rewrite/promotion complexity | After offline Weave experiments in M3 |
| Full dashboard design/build | Must be driven by real APIs and metrics | After M7 |
| Distributed engine | Premature before single-node correctness/durability | After M9 |
| Custom filesystem/kernel | Outside initial DBMS scope; OS I/O is sufficient | Separate research proposal only |

---

# 14. Definition of Done — every task

A task is done only when all applicable items are true:

- [ ] Its purpose and scope are documented.
- [ ] The smallest useful implementation is complete.
- [ ] Positive-path tests pass.
- [ ] Invalid-input tests pass.
- [ ] Failure-path tests pass where applicable.
- [ ] No unrelated feature was bundled into the PR.
- [ ] Formatting and lint checks pass.
- [ ] API and format documentation are updated.
- [ ] Compatibility and migration consequences are stated.
- [ ] Benchmarks are added if performance is the claim.
- [ ] No unsupported capability or performance claim appears in README/docs.
- [ ] The milestone acceptance gate is updated.

---

# 15. Immediate next actions

1. Stabilise the embedded Rust API boundary, configuration compatibility, structured error codes and result lifecycle under M7.1.
2. Keep the native engine independent of Studio and any browser dashboard.
3. Add compatibility tests before exposing new public API contracts.
4. Preserve the documented boundary: one writer, per-operation WAL redo, and no general multi-operation ACID or cluster guarantees.

**Current focus: M7.1 embedded API hardening. M6 is closed; do not broaden the storage guarantee beyond the validated single-node contract.**
