# ANANTA WAL Recovery and Publication Protocol v0.1

**Milestone:** M6.2 — startup recovery and redo  
**Status:** WAL replay is integrated for ANF, Chronicle, catalog DDL, active-Weave pointers, immutable compaction candidates, Capsules and Navigation Maps. Crash-boundary validation remains outstanding. General ACID semantics remain out of scope.

## 1. Guarantee boundary

M6.1 supplies an ANWL v1 append log with checksummed frames, contiguous sequence validation, BEGIN/MUTATION/COMMIT/ABORT state tracking, sync policies and torn-tail truncation.

The current safe integration uses redo for immutable ANF inserts, Chronicle ingests, supported catalog DDL, active-Weave pointer snapshots, immutable compaction candidate manifests, Capsules and Navigation Maps. Recovery claims are limited to these documented operation boundaries and remain subject to the stated filesystem/platform validation. The engine does not claim general multi-operation transaction atomicity or ACID semantics.

## 2. Recovery model

The database integration is redo-oriented for immutable artifact publication and the documented catalog/Weave pointer metadata operations:

- A committed WAL transaction is authoritative intent to make each listed immutable artifact visible at its canonical final name.
- An uncommitted transaction has no committed data effect. Any staging files owned by its intents are discarded.
- A replayed committed intent is idempotent: an already-published final file is accepted only when its exact length and SHA-256 match the WAL intent.
- Recovery never overwrites a different final file and never silently skips a committed intent whose staged and final files are both missing or invalid.
- Catalog/schema snapshots, compaction candidates and active Weave pointer state are applied in WAL order; after committed data artifacts are published, Chronicle indexes are rebuilt from disk so receipts and ingestion sequence state agree with the recovered file inventory.
- A validated ANCP checkpoint can replace the WAL prefix on Unix; the checkpoint inventory establishes the committed state through its base sequence and every later WAL intent is replayed normally.

This is not general undo/redo for arbitrary in-place mutations. The supported publication protocols use immutable artifacts or separately defined atomic metadata replacement operations; other mutations are outside the guarantee boundary.

## 3. Mutation intent envelope

Each WAL MUTATION payload uses a versioned binary envelope. Version 1 contains:

| Field | Meaning |
|---|---|
| Magic and version | AMUT, version 1 |
| Artifact kind | ANF segment, Chronicle record, schema/catalog snapshot, Active Weave pointer, compaction output manifest, Capsule or Navigation Map |
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

The initial DDL WAL integration now covers `create_table`, `add_nullable_column`, `rename_column` and `reorder_columns`. A DDL transaction contains two ordered AMUT v1 intents: an immutable schema snapshot and a versioned immutable catalog snapshot. Both artifacts are staged and synced before the single durable COMMIT. After COMMIT, the engine publishes the schema snapshot first, publishes the versioned catalog snapshot second, and atomically replaces the canonical `catalog.anc` file from that verified snapshot. The canonical catalog parent directory is synced after replacement.

At startup, committed metadata intents are replayed in WAL transaction and intent order before subsequent ANF/Chronicle intents are resolved. This ordering permits a committed table creation or schema change to be replayed before later data writes for that table. Recovery verifies catalog bytes, schema identity, TableId, schema revision, canonical revision progression, immutable destination conflicts and directory types; it fails closed rather than overwriting mismatched artifacts. Stale catalog replacement temps are removed under the exclusive writer lock before catalog loading, and unowned canonical metadata staging temps are cleaned only after WAL recovery.

Table drop, Capsule and Navigation Map creation are not covered by the DDL integration. Active Weave pointer and compaction candidate publication are covered by their separate WAL protocols below.

## 9. Checkpoint and log retention

ANCP v1 checkpoint installation and Unix WAL rotation are specified in Section 17. A checkpoint marker is accepted only when it is the unique first frame in the active WAL, its sequence equals the manifest base sequence plus one, its referenced manifest is checksummed, and the manifest's artifact inventory validates. The checkpoint captures stable artifact state through the base sequence; all later transactions remain in the rotated WAL and replay normally. Checkpoint manifests are retained and are not pruned. Atomic replacement is unsupported on non-Unix platforms.

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

The core crate exposes AMUT v1 typed mutation intents for ANF segments, Chronicle records, immutable schema snapshots and versioned catalog snapshots. The fixed-width envelope carries TableId, schema revision, artifact-specific file ID, exact file length and SHA-256. Final and staging filenames are derived from artifact kind and numeric ID; the WAL payload does not contain an arbitrary filesystem path.

The recovery planner consumes an already-verified WalReadReport and classifies transactions into committed redo work, incomplete staging cleanup work, or explicitly aborted work. Committed and incomplete transaction lists preserve BEGIN order in the WAL. Unit tests cover intent round-trip, canonical naming, malformed artifact kinds, zero IDs, unsupported intent versions and committed/incomplete/aborted classification.

These primitives are connected to `Database::open` and to the ANF, Chronicle and supported catalog DDL write paths. Compaction/Weave/Capsule/Navigation Map publication is still outside the current replay boundary.


## Implemented staged publication and replay driver

The core recovery module now provides staging, publishing and discard helpers. Staging uses create_new, verifies input size/SHA-256 against the mutation intent, syncs the file and its containing directory. Publication derives canonical names from artifact kind/file ID, rejects symlinks and non-regular files, verifies exact file length and a streaming SHA-256 digest, uses a no-replace hard link, and syncs directory changes. Repeating publication for a matching final file succeeds without duplicating it; a conflicting final file is preserved and reported as an error.

The recover_staged_artifacts driver consumes a verified WAL report, publishes all committed intents, discards only staging names owned by incomplete transactions, appends explicit ABORT records, and syncs those markers. Its tests cover committed replay, repeat replay, incomplete cleanup, missing committed artifacts, conflicting final checksums and canonical file derivation.

The helpers are invoked by `Database::open`, ANF/Chronicle writes, catalog DDL, active Weave pointer changes, compaction candidate publication, Capsule publication and Navigation Map publication. The test suite includes deterministic interruption coverage and child-process termination cases; the tests have not been executed in the current environment.


## Initial Database integration status

The core Database owns a WAL writer at catalog-independent path `wal/database.anwl`. `Database::open` acquires the exclusive writer lock, validates/truncates the WAL through the M6.1 reader, replays committed ANF/Chronicle and catalog metadata intents in transaction order, discards staged files for incomplete transactions and appends ABORT markers before it rebuilds per-table Chronicle receipt indexes.

Database::insert_batch and Database::ingest_batch now stage and sync complete immutable artifacts, append AMUT v1 BEGIN/MUTATION/COMMIT intent records using OnCommit sync, and publish the final filename with no-replace semantics. A failed/uncertain WAL or final-publication outcome marks the current Database handle recovery-required; data-facing APIs reject further use through that handle and ask the caller to reopen.

Integration tests cover normal ANF/Chronicle close/reopen, deterministic interruption boundaries, subprocess recovery for ANF/Chronicle and DDL, WAL-backed compaction/Weave/Capsule/Navigation Map publication, ANCP checkpoint creation/rotation, and backup/restore interruption boundaries. These tests have been added but have not been executed in the current environment. This is not a general ACID transaction implementation.


The current Database integration synchronises the WAL directory after header creation, and synchronises the data root/table directory before catalog DDL COMMIT as well as before staged data publication. Directory fsync is applied on platforms that support it.


## Error compatibility at recovery boundaries

Failures before the WAL COMMIT (for example, a canonical staging filename already owned by another operation) remain ordinary I/O errors. They do not consume the Chronicle ingestion sequence or advance the receipt index; Chronicle publication-failure diagnostics are incremented.

A committed Chronicle file that fails the WAL intent SHA-256/length validation is reported as `DbError::ChronicleCorrupt`, preserving the existing Chronicle integrity error contract even though recovery detects the mismatch before the receipt-index scan. ANF committed-artifact conflicts remain WAL recovery errors and fail open.


## Additional Database-level crash tests

The Database test suite now simulates a torn COMMIT frame by appending a valid frame-length prefix and only a partial body after a durable BEGIN/MUTATION. Database::open truncates the WAL tail to the last verified record, classifies the transaction as incomplete, removes its staged ANF file, appends ABORT, and opens with no row visible.

A second test simulates a committed intent whose final ANF filename is occupied by different bytes while the matching staging file remains. Database::open fails closed, does not replace or delete the conflicting final file, and does not claim the committed row is available.


## Artifact identity validation

Before a committed artifact is accepted as published, replay verifies its SHA-256 and format identity against AMUT:
- ANF intent requires a valid v2 header with matching TableId and schema revision.
- Chronicle intent is checked by the streaming Chronicle inspector, including record framing, whole-record checksum, embedded payload fingerprint, TableId and schema revision.

An ANF identity mismatch is a WAL file conflict and fails open. A committed Chronicle record checksum/identity failure retains the public `DbError::ChronicleCorrupt` classification for compatibility with existing callers. The Database unit suite exercises an ANF schema-revision mismatch.


## 11. Deterministic recovery interruption coverage

The core test build exposes one-shot interruption checkpoints after each operation boundary in the ANF/Chronicle write path:

- Staging file creation, payload write, flush, file sync and table-directory sync.
- WAL BEGIN, MUTATION and durable COMMIT.
- Final-name hard-link publication and the first directory sync.
- Staging-name removal and the post-cleanup directory sync.

The Database-level tests run both `insert_batch` and `ingest_batch` through every checkpoint. Pre-COMMIT interruptions must reopen with no visible row or receipt and no canonical staging file. Post-COMMIT interruptions must reopen with exactly one immutable artifact; Chronicle receipts must be rebuilt and an identical BatchId retry must resolve as a duplicate. Every case is opened a second time to check replay idempotence.

These checkpoints are deterministic in-process panic injections at the same code boundaries. They prove that recovery handles the resulting filesystem/WAL states, but they are not a substitute for subprocess-kill tests, block-device fault injection, or real power-loss testing. Those remain part of the M6.2 validation backlog.


## 12. Subprocess termination validation

The unit-test binary includes a narrowly scoped worker test that can be launched as a child process. The child exits with code 86 at selected recovery checkpoints without running Rust destructors, leaving the database writer-lock file stale as a real abruptly terminated process would.

The parent test waits for child exit before removing that stale lock, then opens the database and verifies recovery. The matrix covers:
- ANF and Chronicle interruption after a durable staging artifact and WAL MUTATION but before COMMIT, immediately after durable COMMIT, and immediately after final-name link.
- Catalog DDL interruption after durable COMMIT, schema snapshot publication, versioned catalog publication, canonical replacement-temp sync, atomic rename before directory sync, and completed canonical catalog replacement.

Every subprocess case is reopened a second time. Chronicle committed cases also verify receipt reconstruction and identical BatchId duplicate resolution. DDL cases verify catalog/schema revisions, the new column and immutable snapshot consistency. These are process-termination tests on the test filesystem; they do not simulate power loss, volatile controller caches, or storage-device write reordering. Extend the subprocess matrix to all remaining sync/cleanup boundaries and add filesystem-level fault injection before claiming the full M6.2 crash matrix complete.


## 13. Catalog DDL WAL implementation boundary

The DDL path encodes the full candidate catalog snapshot and the affected immutable schema snapshot before it begins the WAL transaction. It preflights the canonical catalog replacement temp path and both immutable destinations before COMMIT, stages both artifacts with file and directory syncs, then commits the two intents together. The schema intent precedes the catalog intent. The versioned catalog file is immutable and checksum-verified; `catalog.anc` is the canonical atomically replaced current catalog.

The current DDL crash suite covers pre-COMMIT interruption after staging file creation/write/flush/file sync/directory sync, after WAL BEGIN and after the first mutation (confirming no table becomes visible and staging is cleaned), plus `create_table` after COMMIT, after schema publication, after versioned catalog publication, after the canonical replacement temp has been synced, after atomic rename but before catalog-directory sync, and after completed canonical catalog replacement. Each case reopens twice and checks catalog/schema revisions and snapshot consistency. Child-process termination covers the same six DDL publication boundaries. The replacement-temp crash case verifies startup removes the stale temp under the writer lock and replays the committed snapshot. The pre-existing catalog-temp collision regression verifies that a known publication blocker is rejected before WAL COMMIT and leaves no orphan schema snapshot.

The DDL and later M6 process-exit/in-process interruption tests have been added but have not been executed in the current development environment. These tests do not constitute physical power-loss or storage-controller cache-loss validation. Cross-platform and broader filesystem fault injection remain open.


## 14. Active Weave pointer WAL integration

Active Weave pointer promotion, explicit rollback and startup fallback from a damaged active generation now use AMUT v1 `ActiveWeavePointer` intents. Each pointer revision is encoded as an immutable `pointer-{revision}.awp` snapshot under the table's Weave directory. The snapshot is staged and synced, committed using OnCommit WAL durability, published without replacement, and then atomically installed as the canonical `active.awp` pointer with directory sync.

Startup replays committed pointer revisions in WAL order after catalog metadata replay. It verifies the pointer checksum/format, TableId, schema revision and monotonic pointer revision; replay installs the canonical pointer idempotently and re-syncs the directory when the canonical bytes already match a committed version. Incomplete pointer staging is discarded by the normal WAL cleanup path; unowned pointer and legacy pointer-replacement temps are cleaned under the writer lock.

If startup detects that an active generation is damaged but a retained previous generation verifies, the fallback pointer revision is itself committed to the WAL. This prevents recovery from silently changing the active generation outside the log.

The existing compaction integration test injects interruption after versioned pointer publication, immediately after WAL COMMIT for a later promotion, and after canonical pointer rename but before directory fsync during rollback. Reopen restores/verifies the committed pointer and preserves logical scans. Immutable candidate directory rename and OWM1 manifest publication are also WAL-backed. These tests are committed but have not been executed here.


## 15. Immutable compaction candidate publication

After an OWM1 candidate directory has been fully written and verified, the executor syncs the staged directory and appends a `CompactionOutputManifest` AMUT v1 intent. The intent is stored as a versioned `candidate-{wal-sequence}.owm` file in the table's Weave root. It is staged and file/directory-synced before the durable WAL COMMIT. Only after COMMIT does the executor publish the versioned manifest and rename the complete `.stage-{job}-a{attempt}` directory to its canonical `weave-{weave_id}-g{generation}` name, followed by a Weave-root directory sync.

The OWM1 body supplies the exact TableId, schema revision, job/attempt, input fingerprint, WeaveId/generation, and all data/provenance artifact lengths and SHA-256 values. Recovery publishes committed versioned manifest intents in WAL order, derives the only permitted stage/final directory names from the checksummed OWM1 identity, verifies the embedded CIM1 and every referenced data/PVM artifact, completes the directory rename if needed, re-syncs the parent directory, and invokes the existing full candidate verifier. The compaction-job journal is reconciled only after this recovery step, so a committed candidate is not marked Interrupted merely because the process stopped before its directory rename or Completed journal event.

Incomplete intents have their staged manifest removed by generic WAL recovery; the existing compaction-job recovery then marks the Publishing attempt Interrupted and removes its uncommitted stage directory. Canonical unowned manifest temp files are also cleaned after WAL replay. Fully published historical candidates remain immutable across later schema revisions; an unfinished committed candidate for an obsolete schema fails closed.

The existing compaction execution fixture injects interruptions after durable WAL COMMIT, after versioned OWM publication, and after the candidate-directory rename but before parent-directory sync. Reopen must reconstruct the final candidate, verify its full input/output/provenance identity, and preserve the logical table scan. These tests are committed but have not been executed in this environment. Candidate execution/promotion remains separate: this WAL intent publishes an immutable candidate only and does not make it active.


## 16. Capsule and Navigation Map WAL integration

Capsule and Navigation Map files are derived immutable artifacts stored under each table's `capsules/` directory. Both now follow the ANWL staged-file -> durable COMMIT -> no-replace final publication protocol. Their names remain native to their formats: `capsule-{capsule_id}-g{generation}.cap` and `navigation-{generation}.nvm`.

AMUT v1 has a fixed-size identity envelope. For a Capsule intent, `table_id` is the stable TableId, `file_id` is the CapsuleId, and the envelope's `schema_revision` slot carries the Capsule generation. The Capsule's own checked header continues to contain the actual logical schema revision and is decoded/validated during both normal publication and recovery. This avoids changing existing AMUT v1 record sizes while preserving both Capsule identity values.

For a Navigation Map intent, `table_id` is the stable TableId, `file_id` is the map generation, and `schema_revision` remains the current logical schema revision. Recovery decodes the map against the logged TableId/generation, then checks every referenced Capsule is a regular file of the recorded length, matches its SHA-256, decodes successfully, and has metadata identical to the map entry. Missing or conflicting dependencies fail open; the map is never treated as authoritative data.

Both builders reject an already-existing immutable generation before committing another intent. In-process and subprocess crash/reopen tests cover both artifact types. Child-process termination is exercised at WAL MUTATION (incomplete transaction), durable COMMIT and final-name link before directory sync; recovery checks artifact/data state, consistency, and a second reopen. Subprocess coverage now exercises all 12 staging/flush/file-sync/directory-sync/WAL/final-publication/cleanup checkpoints for both Capsule and Navigation Map builders. Broader filesystem fault injection remains outstanding. These tests have been added but were not executed in this environment.

## 17. ANCP checkpoint installation and ANWL rotation

ANCP v1 is an immutable checkpoint manifest under `wal/checkpoints/checkpoint-{base_sequence}.anck`. It records the checkpoint base sequence, current catalog revision and a sorted inventory of the stable artifacts that form the database state: ANF, Chronicle, Capsule, Navigation Map, schema/catalog snapshots, versioned Active Weave pointers, OWM1 manifests and immutable Weave data/provenance/input files. Every entry stores a canonical relative path, exact byte length and SHA-256. The manifest itself has a SHA-256 trailer and bounded entry/path/size limits.

Mutable canonical state (`catalog/catalog.anc` and each `active.awp`) is also fingerprinted. During startup validation, a mismatch is permitted only when the post-checkpoint WAL contains a committed CatalogSnapshot or ActiveWeavePointer intent for that state. Those intents are then replayed in WAL order and perform their ordinary format/identity validation. Otherwise the canonical mutable file must still match the checkpoint fingerprint.

Checkpoint installation requires an operational handle, a clean WAL, no incomplete transaction, a successful bounded consistency pass and no staging/temp artifacts. The implementation:
1. Builds the ANCP manifest from the current database state.
2. Writes a checkpoint-specific temporary manifest, flushes/syncs it, publishes it with a no-replace hard link and syncs the checkpoint directory.
3. Encodes a fixed ANCPREF1 reference (base sequence, catalog revision, manifest length and SHA-256) as the payload of an ANWL CHECKPOINT frame.
4. Writes a new ANWL header plus a CHECKPOINT frame at `base_sequence + 1` to a sibling temporary file, syncs it and the WAL directory. The marker preserves the global WAL sequence high-water mark so future transaction IDs and sequence-derived artifact IDs cannot be reused.
5. Atomically replaces `database.anwl` with the new file using POSIX rename, then syncs the WAL directory and reopens the new writer at `base_sequence + 2`.

The original WAL path remains authoritative until the atomic rename. A process stop before rename leaves the original log usable; stale checkpoint temporary files are removed under the exclusive writer lock. A stop after rename leaves a durable manifest available to validate the new anchor. The replacement WAL begins with the CHECKPOINT record at the previous high-water sequence plus one. Every later transaction continues contiguously from the next sequence; rotation never resets transaction IDs or sequence-derived artifact IDs. The checkpoint inventory is verified before replay. WAL prefix history before the checkpoint is therefore no longer needed after the manifest and replacement log are durable.

Subprocess tests cover stop after checkpoint-manifest temp sync, after manifest publication, after replacement-WAL temp sync, after WAL rename before directory sync, and after directory sync. Additional tests cover successful rotation followed by new writes/reopen, a post-checkpoint DDL commit, and fail-closed detection of a changed checkpointed immutable file.

Atomic replacement is currently implemented only on Unix. On other platforms `Database::create_checkpoint` returns an unsupported error and leaves the database usable; it does not use an unsafe delete-then-rename fallback. After the replacement WAL is reopened and the new anchor is validated, checkpoint retention removes all older canonical ANCP manifests and keeps the one referenced by the active WAL. This deletion occurs only after the new manifest and WAL directory entry are durable; a crash during pruning leaves the active anchor intact. The active manifest is never pruned. Physical power-loss/device-cache validation and large-database stress remain open. The full Rust test suite has not been executed in the current environment.
