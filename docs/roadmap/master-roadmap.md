# ANANTA Master Roadmap

**Document type:** Single source of truth for project execution  
**Product:** Independent Rust-native Database Management System (DBMS)  
**Owner:** DMJ  
**Current release target:** 0.1.0 — Native Storage Prototype  
**Roadmap status:** Active; review this file before starting work outside the current milestone

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

Still unverified:
- [ ] Successful CI run on the current main commit.
- [ ] Run the example from a clean environment.
- [ ] Inspect actual generated database files and test close/reopen manually.
- [ ] Test on Windows as well as Linux.
- [ ] Review file publication and catalog failure behaviour.

The current native prototype is not production-ready. It currently materialises scan results, assumes exclusive single-writer use, has no cross-process lock, WAL, transactions or crash-recovery guarantee. FNV-1a in the prototype is an accidental-corruption check, not a security mechanism.

---

# 2. Current focus — Milestone M0: verify the foundation

**This is the only active milestone until its exit gate passes.** Do not start AQL, analytics, Studio or ChronoWeave compaction before then.

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

The current M0 review branch addresses these findings. Treat each as implemented-but-unverified until its PR checks pass:
- [~] Propagate directory-entry errors during scans instead of silently dropping entries.
- [~] Distinguish a missing catalog from other filesystem metadata errors.
- [~] Reject truncated catalogs using the actual minimum file size.
- [~] Bound untrusted table/column counts by the bytes remaining before allocating vectors.
- [~] Add regression tests for truncated catalogs, impossible counts, missing table storage directories, empty catalogs and empty batches.
- [~] Clarify that projected decoding still reads the whole segment in ANF v0.1 because checksum verification covers the entire segment.

The current GitHub Actions run for the merged baseline passed formatting, tests, Clippy and the example at commit `fcbea310`. New main-branch runs for later commits are queued; the M0 review changes still require their own successful CI run.

## M0.4 Write-access boundary
- [ ] Document that only one exclusive writer may use a database directory.
- [ ] Add a clear error for unsupported concurrent open/write where feasible.
- [ ] Decide how a process-level database lock will work.
- [ ] Add lock-file tests before advertising multi-process safety.
- [ ] Do not claim multi-writer safety yet.

### M0 exit gate
M0 is complete only when:
- [ ] Latest main-branch CI passes formatting, tests and lint.
- [ ] The native example runs from a clean checkout.
- [ ] Create → schema → insert → close → reopen → scan passes.
- [ ] Corrupt and truncated files are rejected.
- [ ] Existing reference research tools remain untouched and independently runnable.
- [ ] Known limitations are accurately documented.

**Deliverable:** verified Native Storage Prototype baseline. No new major feature is included in M0.

---

# 3. Milestone M1: native storage correctness and streaming

**Goal:** make the native storage path safe, inspectable and capable of processing datasets without materialising the whole result in memory.

## M1.1 Freeze the experimental ANF v0.1 contract
- [ ] Review magic bytes and version field.
- [ ] Review endian rules.
- [ ] Review header and directory lengths.
- [ ] Review type codes.
- [ ] Review null markers.
- [ ] Review UTF-8 and binary encoding.
- [ ] Review footer and checksum coverage.
- [ ] Set maximum file, row, column and field sizes.
- [ ] Add golden binary fixtures.
- [ ] Add byte-for-byte fixture tests.
- [ ] Add malformed-directory tests.
- [ ] Add fuzz/property tests for arbitrary byte input.
- [ ] Define the format-version bump policy.
- [ ] Mark the format experimental until compatibility tests are stable.

## M1.2 Improve native catalog
- [ ] Define stable database, table and column identifiers.
- [ ] Separate display names from internal object IDs.
- [ ] Add catalog format version and feature flags.
- [ ] Add catalog checksum and bounds validation tests.
- [ ] Define schema revision numbers.
- [ ] Define rules for table rename and drop.
- [ ] Define rules for adding nullable columns.
- [ ] Define rules for adding non-null columns with existing rows.
- [ ] Define incompatible schema-change behaviour.
- [ ] Add catalog consistency checker.
- [ ] Add catalog inspection command or test utility.

## M1.3 Implement bounded read batches
- [ ] Define a column-vector/batch representation.
- [ ] Define batch row-count limits.
- [ ] Define memory budget per scan.
- [ ] Implement batch decoder for fixed-width numeric columns.
- [ ] Implement batch decoder for UTF-8 and binary columns.
- [ ] Implement null bitmap or a clearly documented initial null representation.
- [ ] Expose projected scan batches.
- [ ] Add early-stop/limit support to the scan iterator.
- [ ] Ensure iterator errors are surfaced and not silently skipped.
- [ ] Test batch boundaries at 0, 1, exact batch size and batch size + 1.
- [ ] Measure peak memory while scanning a generated dataset.
- [ ] Demonstrate memory bounded by batch size and selected columns.

## M1.4 Safe segment publication
- [ ] Define segment states: temporary, published and obsolete.
- [ ] Define deterministic unique segment IDs.
- [ ] Document the exact write, flush, sync and rename sequence.
- [ ] Sync the containing directory where supported and required.
- [ ] Document platform-specific guarantees.
- [ ] Ensure failed temporary writes are cleaned up.
- [ ] Ensure incomplete segments are never listed as committed.
- [ ] Test interruption at each publication step using fault injection.
- [ ] Add orphan temporary-file detection.
- [ ] Add safe startup handling for orphan temporary files.

## M1 exit gate
- [ ] Golden ANF fixtures pass.
- [ ] Property/fuzz tests reject malformed data safely.
- [ ] Catalog survives repeated close/reopen.
- [ ] Projected streaming scans work without full-result materialisation.
- [ ] Memory is measured and bounded by configured batches.
- [ ] Publication failure tests pass.
- [ ] Exclusive-writer boundary is enforced or clearly guarded.

**Deliverable:** Native Storage Core v0.2.

---

# 4. Milestone M2: ingestion and the first ChronoWeave concepts

**Goal:** implement a robust append-oriented ingestion path and determine whether ChronoWeave is a useful storage design.

## M2.1 Ingestion contract
- [ ] Define batch identity.
- [ ] Define monotonically ordered ingestion sequence per table.
- [ ] Define accepted/rejected row behaviour.
- [ ] Define all-or-nothing batch validation.
- [ ] Define duplicate-batch behaviour.
- [ ] Define late-arriving timestamp behaviour.
- [ ] Define out-of-order event behaviour.
- [ ] Define maximum batch size.
- [ ] Define backpressure/error behaviour.
- [ ] Add ingestion result counters.

## M2.2 Chronicle prototype
- [ ] Define Chronicle metadata.
- [ ] Define append order and sequence semantics.
- [ ] Define immutable publication boundary.
- [ ] Map incoming batches to Chronicle units.
- [ ] Add Chronicle reader.
- [ ] Add Chronicle integrity summary.
- [ ] Test repeated ingestion.
- [ ] Test out-of-order timestamps.
- [ ] Test duplicate batch submission.
- [ ] Test partially failed ingestion.

## M2.3 Capsule prototype
- [ ] Define Capsule identity and generation.
- [ ] Define row count and schema revision reference.
- [ ] Add temporal min/max bounds.
- [ ] Add per-column min/max where meaningful.
- [ ] Add null counts.
- [ ] Add column byte sizes.
- [ ] Add capsule checksum.
- [ ] Add capsule validation tool.
- [ ] Compare multiple capsule sizes on identical datasets.
- [ ] Record ingestion overhead and scan behaviour.

## M2.4 Navigation Map prototype
- [ ] Define metadata structure separate from data payloads.
- [ ] Add safe time-range pruning.
- [ ] Add selected-column metadata.
- [ ] Verify no false negatives in pruning.
- [ ] Add tests for inclusive/exclusive time boundaries.
- [ ] Record capsules considered, skipped and read.
- [ ] Add explainable pruning diagnostics.

## M2 exit gate
- [ ] Ingestion is deterministic and validates whole batches.
- [ ] Chronicle units are immutable after publication.
- [ ] Capsules can be independently read and verified.
- [ ] Navigation Map pruning is correctness-tested.
- [ ] Query results are identical with pruning enabled or disabled.
- [ ] Ingestion and storage overhead are recorded.

**Deliverable:** ChronoWeave prototype v0.1. This is a research prototype, not yet adaptive production storage.

---

# 5. Milestone M3: compaction and physical organisation

**Goal:** test whether reorganising Capsules into Weaves improves actual workloads.

## M3.1 Compaction foundation
- [ ] Define compaction input and output manifests.
- [ ] Define source-generation references.
- [ ] Define compaction job states.
- [ ] Define temporary output handling.
- [ ] Define duplicate/missing row detection.
- [ ] Define compaction memory and disk limits.
- [ ] Define compaction cancellation.
- [ ] Make compaction restartable or safely discardable.
- [ ] Add compaction correctness tests.

## M3.2 First Weave strategies
- [ ] Implement baseline append-only layout.
- [ ] Implement time-first candidate.
- [ ] Implement entity-first candidate.
- [ ] Implement mixed candidate only if workloads justify it.
- [ ] Preserve logical row equivalence across all layouts.
- [ ] Record compaction duration and CPU.
- [ ] Record temporary disk amplification.
- [ ] Record final storage size.
- [ ] Record read latency for each workload.

## M3.3 Generation promotion
- [ ] Define active generation pointer.
- [ ] Build candidate generation off the active path.
- [ ] Verify row counts and checksums.
- [ ] Run reference query equivalence checks.
- [ ] Publish the new manifest safely.
- [ ] Retain previous generation until verification completes.
- [ ] Implement rollback to previous generation.
- [ ] Test interruption during promotion.
- [ ] Test recovery from missing/incomplete candidate generation.

## M3 exit gate
- [ ] Multiple Weave candidates produce identical logical results.
- [ ] Failed compaction cannot corrupt the active generation.
- [ ] Promotion and rollback are tested under injected failures.
- [ ] At least one workload shows a repeatable measurable benefit, or the experiment is documented as not beneficial.

**Deliverable:** ChronoWeave compaction research report and prototype.

---

# 6. Milestone M4: AQL native query language

**Goal:** create a native query language with semantics designed around ANANTA rather than implementing SQL compatibility as the core interface.

Do not start the full AQL parser before M1 storage and batch read interfaces are stable.

## M4.1 Language design
- [ ] Define AQL's goals and non-goals.
- [ ] Define identifiers and quoting.
- [ ] Define literals.
- [ ] Define data types and casts.
- [ ] Define null semantics.
- [ ] Define comparison semantics.
- [ ] Define boolean logic.
- [ ] Define error locations and diagnostics.
- [ ] Define parameter binding.
- [ ] Define language versioning.
- [ ] Write grammar document.
- [ ] Add grammar examples and invalid examples.

## M4.2 Parser and representation
- [ ] Implement tokenizer/lexer.
- [ ] Test string and numeric literals.
- [ ] Test escaped identifiers.
- [ ] Test malformed input diagnostics.
- [ ] Implement parser.
- [ ] Define AST nodes.
- [ ] Implement AST validation.
- [ ] Define logical-plan representation.
- [ ] Add parser golden tests.

## M4.3 First query capabilities
- [ ] Select source table.
- [ ] Project selected columns.
- [ ] Filter with simple predicates.
- [ ] Limit rows.
- [ ] Add timestamp-range filtering.
- [ ] Bind parameters safely.
- [ ] Return typed result batches.
- [ ] Add query cancellation checks.
- [ ] Add query error codes.

## M4 exit gate
- [ ] AQL grammar and semantics are documented.
- [ ] Parser tests pass.
- [ ] Query results match equivalent direct Rust API operations.
- [ ] Query execution does not rely on an external database.
- [ ] AQL examples run using the native CLI/API.

**Deliverable:** AQL v0.1.

---

# 7. Milestone M5: native execution and analytics

**Goal:** build the actual analytical engine as native execution operators over ANANTA storage batches.

## M5.1 Execution model
- [ ] Define vector batch schema.
- [ ] Define operator interface.
- [ ] Define batch ownership/lifetime rules.
- [ ] Define operator errors and cancellation.
- [ ] Define memory limits and resource accounting.
- [ ] Implement scan operator.
- [ ] Implement projection operator.
- [ ] Implement filter operator.
- [ ] Implement limit operator.
- [ ] Add operator-level unit tests.

## M5.2 Aggregation
- [ ] Define aggregate state interface.
- [ ] Implement count.
- [ ] Implement sum.
- [ ] Implement min and max.
- [ ] Implement average.
- [ ] Define integer overflow behaviour.
- [ ] Define floating-point determinism policy.
- [ ] Define null handling for each aggregate.
- [ ] Implement group-by.
- [ ] Test empty inputs and empty groups.
- [ ] Test high-cardinality grouping.

## M5.3 Time-series analytics
- [ ] Define time bucket semantics.
- [ ] Define timezone and UTC behaviour.
- [ ] Implement time-range filter.
- [ ] Implement bucketed count.
- [ ] Implement bucketed min/max/average.
- [ ] Implement latest-value-per-series.
- [ ] Test gaps and missing intervals.
- [ ] Test late-arriving records.
- [ ] Test boundary timestamps.

## M5.4 Planner and optimisation
- [ ] Define logical plan.
- [ ] Define physical plan.
- [ ] Implement projection pushdown.
- [ ] Implement predicate pushdown.
- [ ] Implement time/capsule pruning.
- [ ] Add table/column statistics.
- [ ] Add cardinality estimates.
- [ ] Add explain output.
- [ ] Compare planned and actual rows/bytes.
- [ ] Test optimiser correctness against optimiser-disabled results.

## M5 exit gate
- [ ] First AQL queries execute through native operators.
- [ ] Aggregate results pass reference tests.
- [ ] Time-series boundary/null semantics are covered.
- [ ] Query results remain identical with optimiser on/off.
- [ ] Query profiling reports real measured data.

**Deliverable:** Native Analytics Engine v0.1.

---

# 8. Milestone M6: durability, concurrency and recovery

**Goal:** move from an experimental single-writer prototype toward a reliable DBMS.

This milestone is a hard prerequisite for production use. Do not imply transactional safety before it passes.

## M6.1 Write-ahead log
- [ ] Define WAL record format and version.
- [ ] Define record checksums.
- [ ] Define sequence and commit markers.
- [ ] Define flush/sync policy.
- [ ] Define segment/catalog relationship to WAL.
- [ ] Implement WAL writer.
- [ ] Implement WAL reader.
- [ ] Test truncated WAL records.
- [ ] Test corrupt WAL records.

## M6.2 Recovery
- [ ] Define startup recovery algorithm.
- [ ] Replay committed operations only.
- [ ] Ignore or roll back incomplete operations according to documented semantics.
- [ ] Define checkpoint format.
- [ ] Implement checkpoint creation.
- [ ] Implement checkpoint loading.
- [ ] Inject process interruption at each write boundary.
- [ ] Test repeated recovery.
- [ ] Verify recovered row counts and values.

## M6.3 Transactions and concurrency
- [ ] Define transaction scope and isolation goals.
- [ ] Define atomicity boundary.
- [ ] Implement one-writer lock and lock diagnostics.
- [ ] Add concurrent reader tests.
- [ ] Define schema/catalog concurrency.
- [ ] Design MVCC only if workload requirements justify it.
- [ ] Add conflict tests.
- [ ] Add transaction timeout/cancellation.
- [ ] Document durability guarantees precisely.

## M6.4 Operational integrity
- [ ] Add database consistency checker.
- [ ] Add segment inventory.
- [ ] Add orphan/temp-file reporting.
- [ ] Add backup format.
- [ ] Add restore validation.
- [ ] Test restore into a new directory.
- [ ] Document corruption repair boundaries.

## M6 exit gate
- [ ] Fault-injection tests pass.
- [ ] Recovery is deterministic and repeatable.
- [ ] Transaction semantics are documented and tested.
- [ ] Locking rules are enforced.
- [ ] Backup/restore integrity tests pass.

**Deliverable:** Reliable single-node engine milestone. No cluster claims.

---

# 9. Milestone M7: native server and developer interfaces

**Goal:** expose the engine to applications without making the browser dashboard a dependency.

## M7.1 Embedded API hardening
- [ ] Stabilise public Rust API boundaries.
- [ ] Define configuration compatibility.
- [ ] Define structured error codes.
- [ ] Define query/result lifecycle.
- [ ] Add cancellation API.
- [ ] Add resource limits.
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

Do these in order. Do not skip ahead.

1. [ ] Check latest GitHub Actions run for `main`.
2. [ ] If queued, inspect the runner; do not create many duplicate CI runs.
3. [ ] If failed, read the failing job logs and fix only those failures.
4. [ ] Run `cargo fmt --all -- --check`.
5. [ ] Run `cargo test --workspace --all-targets`.
6. [ ] Run `cargo clippy --workspace --all-targets`.
7. [ ] Run `cargo run -p ananta-core --example basic` from a clean checkout.
8. [ ] Inspect the generated `ananta-data` directory and segment files.
9. [ ] Add/repair tests for issues found in the storage correctness review.
10. [ ] Mark M0 complete only after all exit criteria pass.
11. [ ] Start M1 with exactly one small PR: the ANF format/golden-fixture and safety work package.

**Current focus: M0 — verify the foundation. Everything else waits.**
