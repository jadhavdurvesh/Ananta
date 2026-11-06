# ANANTA Write-Ahead Log Format v0.1

**Milestone:** M6.1 — WAL framing and append primitives  
**Status:** Implemented as a standalone subsystem; database mutation replay is M6.2 work.

## 1. Scope and guarantee boundary

The WAL module provides versioned append-only record framing, checksums, sequence validation, transaction markers, append/sync policies, and incomplete-tail detection. It is not yet the database's source of truth and is not yet called by Database::insert_batch, Database::ingest_batch, catalog DDL, or compaction publication. Until M6.2 connects WAL records to actual mutation payloads and startup replay, enabling this module alone does not make database writes crash-recoverable.

The WAL stores opaque mutation payloads. Their operation-specific schemas and replay handlers belong to the database/recovery layer, not the framing layer.

## 2. File header

All multi-byte integers are unsigned little-endian unless otherwise stated.

| Offset | Width | Field | Value |
|---:|---:|---|---|
| 0 | 4 | Magic | ASCII ANWL |
| 4 | 2 | Format version | 1 |
| 6 | 2 | Header length | 8 |

Unknown versions and invalid header lengths fail closed. A future incompatible encoding requires a new version rather than reinterpretation of existing bytes.

## 3. Frame format

Each frame is a four-byte little-endian body length followed by the body. The body is record content plus a 32-byte SHA-256 checksum.

Record content fields:

| Offset | Width | Field |
|---:|---:|---|
| 0 | 8 | Monotonic sequence number, starting at 1 |
| 8 | 8 | Transaction ID; zero reserved for CHECKPOINT |
| 16 | 1 | Record kind |
| 17 | variable | Opaque payload |

The checksum is SHA-256 of the record content (not including the checksum). The u32 body length includes content and checksum, but excludes the four-byte length prefix. The current maximum body size is 64 MiB.

Record kinds:

| Value | Kind | Transaction ID | Payload |
|---:|---|---|---|
| 1 | BEGIN | non-zero | Caller metadata |
| 2 | MUTATION | non-zero | One opaque database mutation |
| 3 | COMMIT | non-zero | u64 mutation count + 32-byte rolling digest |
| 4 | ABORT | non-zero | Optional opaque abort reason |
| 5 | CHECKPOINT | zero | Opaque checkpoint metadata/state reference |

Unknown kinds, non-contiguous sequence numbers, illegal transaction ordering, malformed commit payloads, and checksum mismatches are errors.

## 4. Transaction digest and ordering

A transaction must begin with a unique non-zero BEGIN. MUTATION records are accepted only while that transaction is open. For every mutation payload, the rolling digest is updated as:

SHA256(previous_digest || payload_length_u64_le || payload_bytes)

The initial previous digest is 32 zero bytes. The mutation count is checked u64 state. COMMIT stores the final mutation count and digest; the reader recomputes both and rejects a mismatch. A successful COMMIT removes the transaction from the open set. ABORT closes an open transaction without making it committed.

CHECKPOINT is restricted to transaction ID zero and a log boundary with no open transactions. Its payload is opaque; actual catalog/segment checkpoint semantics are M6.2.

A transaction with BEGIN but no COMMIT or ABORT at end-of-file is reported as incomplete. The M6.1 reader does not replay it or decide whether its mutations are safe to apply.

## 5. Append and synchronisation policy

WalWriter appends frames in sequence order and validates transaction state before writing. It advances in-memory state only after frame bytes are written. A partial write or sync failure poisons the current writer handle; callers must close it and reopen/inspect the WAL rather than continue appending through a potentially torn frame.

- **Manual:** append writes are not synchronised implicitly. The owner must call sync().
- **OnCommit:** COMMIT and CHECKPOINT synchronise all preceding WAL data using sync_data.
- **EveryRecord:** every frame is synchronised before append returns.

A newly created WAL writes and synchronises its file header. Directory-entry durability, WAL rotation, segment ordering and catalog publication ordering are reserved for database integration.

## 6. Reader and incomplete-tail behavior

The reader validates the header, frame lengths, checksums, sequence continuity and transaction state. It returns the last verified byte offset and distinguishes:

- **Clean EOF:** ends exactly at a record boundary.
- **Truncated tail:** a partial final length prefix or frame body that ends before its declared length. The report retains only complete verified records and marks the tail.
- **Corruption:** a complete frame with a bad checksum, impossible size, invalid kind, broken sequence or invalid transaction sequence. This is an error; the reader must not silently skip it.

On open, the writer truncates only an incomplete final frame to the reader's last verified boundary and synchronises the truncation. It does not truncate or ignore a complete corrupt frame. Open transactions remain reported; the caller may append an explicit ABORT or use M6.2 recovery policy.

## 7. Relationship to ANF, Chronicle and catalog files

ANF segments and Chronicle records remain immutable data artifacts. The intended M6.2 write protocol is:

1. Append the transaction's BEGIN and complete mutation intent records.
2. Make the required WAL durability boundary explicit according to the selected sync policy.
3. Publish immutable ANF/Chronicle data and any receipt/catalog metadata using their existing publication protocols.
4. Append COMMIT only after all operation-specific publication steps have succeeded.
5. On startup, validate WAL frames and reconcile committed intent with published files; incomplete transactions must not be replayed as committed.

This ordering is a design target, not an implemented guarantee in M6.1. M6.2 must settle idempotent replay keys, file inventory verification, and interruption points before the Database write paths are wired to the WAL.

## 8. Validation coverage

The WAL tests cover header creation and transaction/commit/checkpoint round trip, mutation count and rolling digest validation, sequence progression and invalid transaction order, manual and automatic sync policies, truncated final length prefix, retained valid offset and writer truncation, open transaction reporting and explicit abort after reopen, rejection of a complete frame with a corrupted checksum, truncated frame-body repair, non-contiguous sequence rejection, and commit count/digest mismatch rejection.

## 9. M6.1 non-goals

- No database mutation is currently recorded to the WAL.
- No startup replay or rollback is implemented.
- No atomic commit relationship exists yet between WAL, ANF/Chronicle, catalog or Weave files.
- No WAL rotation, checkpoint installation or retention policy.
- No concurrent writers; the existing database-wide exclusive writer lock remains unchanged.
- No claim of ACID transactions or crash-recoverable database writes.
