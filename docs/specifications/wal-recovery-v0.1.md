# ANANTA WAL Recovery and Publication Protocol v0.1

**Milestone:** M6.2 — startup recovery and redo  
**Status:** Design contract plus tested mutation-intent/recovery-plan primitives; not yet connected to Database mutation paths

## 1. Guarantee boundary

M6.1 supplies an ANWL v1 append log with checksummed frames, contiguous sequence validation, BEGIN/MUTATION/COMMIT/ABORT state tracking, sync policies and torn-tail truncation.

This document specifies the first safe database integration. Until implementation and fault-injection tests are complete, the database does NOT claim WAL-backed crash recovery, transaction atomicity, or ACID semantics. The first integration scope is immutable ANF inserts and Chronicle ingests. Catalog DDL, compaction publication, active Weave swaps, Capsule/Navigation Map publication and multi-table transactions remain separate operations until their own intents and replay rules are implemented.

## 2. Recovery model

The first database integration is redo-only for immutable data-file publication:

- A committed WAL transaction is authoritative intent to make each listed immutable artifact visible at its canonical final name.
- An uncommitted transaction has no committed data effect. Any staging files owned by its intents are discarded.
- A replayed committed intent is idempotent: an already-published final file is accepted only when its exact length and SHA-256 match the WAL intent.
- Recovery never overwrites a different final file and never silently skips a committed intent whose staged and final files are both missing or invalid.
- After committed artifacts have been published, Chronicle indexes are rebuilt from disk so receipts and ingestion sequence state agree with the recovered file inventory.

This is not general undo/redo for arbitrary in-place mutations. It works because the first target operations publish new immutable files and do not mutate existing segment contents.

## 3. Mutation intent envelope

Each WAL MUTATION payload uses a versioned binary envelope. Version 1 contains:

| Field | Meaning |
|---|---|
| Magic and version | AMUT, version 1 |
| Artifact kind | ANF segment or Chronicle record |
| Stable table identity | TableId, not only a user-supplied table name |
| Schema revision | Historical schema snapshot used to encode the artifact |
| Physical file identity | Preallocated non-zero segment/file ID |
| Expected byte length | Exact size of the staged/final file |
| SHA-256 | Hash of the complete immutable file bytes |
| Canonical name | Derived from artifact kind and file ID; never an arbitrary path |
| Staging name | Derived from artifact kind and file ID; never an arbitrary path |

The table directory is resolved from the current catalog entry matching TableId. A missing table or mismatched identity is a recovery error. All file names are regenerated from validated numeric IDs and fixed prefixes/suffixes; replay must not trust absolute paths supplied by a WAL payload.

Chronicle-specific identity such as BatchId and ingestion sequence is verified by decoding the published Chronicle file. The stable TableId/schema revision in the intent must agree with the Chronicle header and referenced schema snapshot.

## 4. Write protocol

For one immutable artifact:

1. Validate the input and encode the complete immutable artifact bytes.
2. Allocate its physical file ID before publication.
3. Write the complete bytes to the canonical staging name in the same table directory using create_new.
4. Flush and sync_all the staging file. Then sync the table directory so the staging name is durable where directory syncing is supported.
5. Append WAL BEGIN and one or more MUTATION intents. The intent records staging/final identity, exact length and SHA-256.
6. Append COMMIT using WalSyncPolicy::OnCommit or an explicit equivalent sync barrier. The commit must be durable before final-name publication is attempted.
7. Publish the staging file under the final canonical name using a no-replace operation (hard link on the same filesystem), verify exact bytes/length, sync the table directory, remove the staging name, and sync the directory again.
8. Return success only after the final name is published and verified.

The staged bytes and their table-directory entry must be durable before the WAL COMMIT can become durable. This ordering makes redo possible after a crash between COMMIT and final-name publication.

If step 7 fails after COMMIT, the operation is committed but publication is pending. The current Database handle must be marked recovery-required and must not accept further reads or writes that could expose a partial logical state. The caller is told to reopen/recover; a retry must not allocate a second artifact for the same committed intent.

## 5. Startup recovery algorithm

Database open must acquire the existing exclusive writer lock before recovery. It creates the WAL directory, synchronises the database root and WAL directory, opens the WAL (which synchronises a new header), then synchronises the WAL directory again so the WAL filename is durable before it can acknowledge transactions. Table creation synchronises the data root and new table directory before subsequent staged artifacts depend on that directory.

1. Open the WAL writer. M6.1 validates the header and complete frames and truncates only an incomplete final frame to the last verified offset. A checksum mismatch or any complete-frame corruption fails open.
2. Read the complete verified WAL records and group transaction records by transaction ID.
3. Validate every transaction through the M6.1 transaction state machine. A transaction is committed only when its valid COMMIT frame exists.
4. For each committed transaction, decode every mutation intent and validate version, artifact kind, TableId, schema revision, file ID, length, checksum and canonical names.
5. Resolve the table directory from the catalog TableId mapping.
6. If the final file exists, verify exact length and SHA-256. If it matches, the intent is already applied; remove a matching leftover staging file if present.
7. Otherwise, require the staging file to exist and match exact length and SHA-256. Publish it under the final name without replacement, sync the directory, remove the staging name, and sync again.
8. For each incomplete transaction, decode its intents and remove only its canonical staging files after verifying their names are derived from the logged IDs. Do not delete final files based only on an uncommitted intent.
9. Append explicit ABORT markers for incomplete transactions so a subsequent open sees a clean transaction boundary.
10. Rebuild Chronicle receipt indexes from the now-recovered .chr files. Validate contiguous per-table ingestion sequences, BatchId uniqueness and Chronicle headers.
11. Continue existing compaction-job and active-Weave recovery only after data-file recovery and index reconstruction.

If a committed intent has neither a matching final file nor a matching staging file, or if either file conflicts with its logged checksum, recovery fails closed. It must not open the database with fewer rows than the committed WAL describes.

## 6. Crash-point matrix

| Crash point | WAL state | Files | Required recovery |
|---|---|---|---|
| Before staging file sync | No committed intent | Partial/temporary staging may exist | Remove orphan temp only when ownership/name is proven; no data effect |
| After staging sync, before BEGIN | No WAL intent | Complete staging file | Remove as orphan after validating canonical temp naming |
| After BEGIN/MUTATION, before COMMIT | Incomplete transaction | Staging file exists; no final file | Discard staged file and append ABORT |
| During COMMIT frame write | Truncated final WAL frame | Staging file exists | M6.1 truncates torn tail; treat transaction as incomplete and discard staging |
| After durable COMMIT, before final link | Committed intent | Staging file exists | Verify and publish staging file |
| After final link, before directory sync | Committed intent | Final and staging names may exist | Verify final; sync directory; remove staging |
| After directory sync, before temp cleanup | Committed intent | Final plus staging link | Verify final; remove staging and sync directory |
| After final publication and cleanup | Committed intent | Final exists only | Verify final and continue |
| Final file exists with wrong hash | Committed intent | Conflicting final | Fail open; never overwrite |
| Staging and final are both missing | Committed intent | No artifact | Fail open; report committed data loss |

## 7. ANF insert and Chronicle ingest specifics

### ANF insert_batch

The intent includes TableId, schema revision, segment ID, file length and complete-file SHA-256. The schema snapshot revision used for encoding must exist before commit. On recovery, the final .anf is verified against the intent and then becomes part of the normal table segment inventory.

### Chronicle ingest_batch

The intent includes the same artifact fields and the Chronicle record is independently decoded during index rebuild. The Chronicle header must agree on TableId, BatchId, ingestion sequence, schema revision and payload fingerprint. The idempotency key remains BatchId; a retry after an uncertain caller response must return the existing receipt when its payload fingerprint matches and reject a conflicting payload.

The TableChronicleIndex must not be advanced before the final .chr artifact is durably published. After recovery, index state is reconstructed from the published Chronicle files, not inferred solely from WAL counters.

## 8. DDL and metadata publication

create_table currently publishes a schema snapshot and then atomically replaces catalog.anc. A later WAL DDL intent must include the complete canonical catalog/schema state or a deterministic operation with allocated ObjectIds and expected catalog revision. The same staged-bytes/commit/publish rule applies to a new catalog snapshot, but schema snapshot and catalog publication must be recovered as one logical operation.

Column rename/reorder, table drop, compaction manifests, active-Weave pointer swaps, Capsule and Navigation Map creation are not covered by the first ANF/Chronicle-only replay implementation. They must not be described as WAL-protected until they have explicit redo intents and crash-point tests.

## 9. Checkpoint and log retention

M6.2 must first complete deterministic replay. WAL truncation/rotation is intentionally not defined here. A CHECKPOINT record must not cause old intent frames to be deleted until a checksummed checkpoint state has been atomically published and recovery can prove it covers every committed transaction through the checkpoint sequence. Retention and directory fsync semantics remain separate tasks.

## 10. Required validation

- Clean close/reopen after ANF insert and Chronicle ingest.
- Recovery after every write/publication crash point in the matrix.
- Missing staged file and missing committed final file must fail open.
- Conflicting final/staging file checksum must fail open and preserve the conflicting file.
- Replaying the same committed WAL twice must not duplicate ANF segments or Chronicle receipts.
- Incomplete transaction must not publish data and must be explicitly aborted.
- Chronicle sequence and BatchId indexes must match the final .chr inventory.
- Existing segment, Chronicle, Capsule, compaction and active-Weave tests must continue to pass.
- Reopen twice after recovery to prove idempotence.

Until these tests pass on actual Database mutation paths, the engine must continue to describe M6.2 as incomplete.


## Implemented recovery primitives

The core crate now exposes AMUT v1 typed mutation intents for ANF segment and Chronicle record artifacts. The fixed-width envelope carries TableId, schema revision, preallocated file ID, exact file length and SHA-256. Final and staging filenames are derived from artifact kind and numeric ID; the WAL payload does not contain an arbitrary filesystem path.

The recovery planner consumes an already-verified WalReadReport and classifies transactions into committed redo work, incomplete staging cleanup work, or explicitly aborted work. Committed and incomplete transaction lists preserve BEGIN order in the WAL. Unit tests cover intent round-trip, canonical naming, malformed artifact kinds, zero IDs, unsupported intent versions and committed/incomplete/aborted classification.

These primitives do not yet publish or replay database files; Database::open and the ANF/Chronicle write paths are the next integration stage.


## Implemented staged publication and replay driver

The core recovery module now provides staging, publishing and discard helpers. Staging uses create_new, verifies input size/SHA-256 against the mutation intent, syncs the file and its containing directory. Publication derives canonical names from artifact kind/file ID, rejects symlinks and non-regular files, verifies exact file length and a streaming SHA-256 digest, uses a no-replace hard link, and syncs directory changes. Repeating publication for a matching final file succeeds without duplicating it; a conflicting final file is preserved and reported as an error.

The recover_staged_artifacts driver consumes a verified WAL report, publishes all committed intents, discards only staging names owned by incomplete transactions, appends explicit ABORT records, and syncs those markers. Its tests cover committed replay, repeat replay, incomplete cleanup, missing committed artifacts, conflicting final checksums and canonical file derivation.

These helpers are still not invoked by Database::open or the insert/ingest mutation methods. The production integration checkboxes remain open until that wiring and process-interruption tests are committed.


## Initial Database integration status

The core Database now owns a WAL writer at catalog-independent path wal/database.anwl. Database::open acquires the exclusive writer lock, validates/truncates the WAL through the M6.1 reader, replays committed ANF/Chronicle artifact intents, discards staged files for incomplete transactions and appends ABORT markers before it rebuilds per-table Chronicle receipt indexes.

Database::insert_batch and Database::ingest_batch now stage and sync complete immutable artifacts, append AMUT v1 BEGIN/MUTATION/COMMIT intent records using OnCommit sync, and publish the final filename with no-replace semantics. A failed/uncertain WAL or final-publication outcome marks the current Database handle recovery-required; data-facing APIs reject further use through that handle and ask the caller to reopen.

Integration tests cover normal ANF/Chronicle close/reopen, fault injection after a durable ANF COMMIT but before final publication, the equivalent Chronicle replay with receipt-index reconstruction and duplicate BatchId behavior, and uncommitted intent discard/ABORT. More publication-boundary crash points, catalog DDL, compaction/Weave publication and checkpoint/rotation remain open. This is not yet a general ACID transaction implementation.


The current Database integration also synchronises the WAL directory after header creation, and synchronises both the data root and newly-created table directory during table creation. This closes the parent-directory entry durability gap for the initial ANF/Chronicle write path on platforms where directory fsync is supported.
