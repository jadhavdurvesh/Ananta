# ANANTA Transaction and Concurrency Scope v0.1

**Milestone:** M6.3  
**Status:** Single-writer operation model defined; concurrent shared-reference reader test added. Multi-operation application transactions and MVCC are not implemented.

## 1. Transaction boundary

ANANTA currently exposes operation-scoped atomic redo units, not a public BEGIN/COMMIT transaction API.

Each supported mutating API operation independently:
1. validates and encodes its complete immutable artifact(s) or catalog snapshot(s);
2. stages and syncs all required artifacts;
3. writes one ANWL BEGIN and its ordered MUTATION intents;
4. durably commits the WAL transaction;
5. publishes its immutable output(s), or marks the handle recovery-required if publication becomes uncertain.

A supported API operation returns success only after its committed publication path completes. Startup replays committed intents and discards incomplete staging. Catalog DDL operations that require both schema and catalog snapshots group those two artifacts in one WAL transaction.

Separate calls such as ingest followed by Capsule construction remain separate transactions. ANANTA does not currently provide arbitrary user-defined multi-statement atomicity, rollback of committed operations, savepoints or general ACID claims.

## 2. Isolation and locking

The embedded database uses one exclusive writer owner process/handle. Database::open creates the database-local .ananta.lock file with the owner process ID using create_new; a second opener fails immediately with DatabaseLocked.

Methods that mutate the database require exclusive mutable access to Database. Shared-reference read operations may be called concurrently against the same open handle. A reader sees the catalog and immutable artifact inventory owned by that handle; DDL cannot mutate the same handle while shared borrows are active.

The current API does not allow independent database handles in the same process or across processes to write concurrently. There is no MVCC, row-level locking, multi-writer scheduler or cross-process read-sharing protocol in this milestone.

## 3. Lock inspection and stale lock handling

Database::inspect_lock(directory) reads the lock marker and reports its path, whether it exists, and the recorded PID when parseable. It does not remove or rewrite the marker and does not claim that a PID is alive or dead.

A stale lock must never be removed solely because the PID looks old or cannot be parsed. An operator must establish that no process owns the database (including a process whose PID was reused), then remove the marker and reopen. Automatic stale-lock reclamation is intentionally excluded.

## 4. Reader concurrency and cancellation

Read methods accepting shared references do not mutate the WAL writer or catalog. ScanBatchIter is pull-based; abandoning a scan by dropping the iterator stops further reads. There is no public long-running transaction or lock-wait API requiring transaction timeout semantics yet.

The concurrent-reader unit test exercises multiple scans through one shared Database handle. A subprocess lock probe is repeated five times while the parent owns the database and verifies that every second process is denied. Multi-process read sharing is not supported by the exclusive-owner contract; platform-specific filesystem/lock behavior and high-contention single-writer stress remain additional validation requirements.

## 5. Durability guarantees

The supported per-operation durability boundary is documented in the WAL recovery specification. ANWL COMMIT is synced before immutable artifact publication. Recovery validates checksums and embedded identities and fails closed if a committed artifact cannot be recovered without conflict.

This is not a claim of physical power-loss immunity on every filesystem/device, nor general ACID semantics for arbitrary sequences of API calls. ANCP v1 checkpoints fingerprint stable database artifacts and anchor an atomically replaced ANWL log on Unix. The checkpoint frame uses the prior WAL high-water sequence plus one; future sequence-derived IDs continue monotonically. Only the checkpoint manifest referenced by the active WAL is retained; superseded manifests are pruned after the replacement WAL is durably installed. Windows atomic replacement is not yet implemented, so checkpoint rotation returns unsupported there without changing the active WAL.
