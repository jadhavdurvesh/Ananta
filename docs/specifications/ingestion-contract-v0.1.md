# ANANTA Ingestion Contract — M2.1

**Version:** 0.1  
**Status:** Accepted design contract; runtime implementation is M2.2 work  
**Applies to:** native table ingestion into the future Chronicle commit stream  
**Compatibility:** preserves the existing ANF v0.2 segment bytes as the first data payload

## 1. Goals and invariants

The ingestion API accepts a complete batch, validates it against one captured table schema revision, and publishes it as one immutable, ordered unit.

The contract guarantees:

1. **Atomic batch acceptance:** all input rows are accepted together or none are.
2. **Stable identity:** every accepted batch has a caller-supplied, stable idempotency identity.
3. **Durable idempotency:** a retry of an accepted identity cannot append the same batch again.
4. **Deterministic order:** each newly accepted batch receives the next per-table ingestion sequence.
5. **Event-time neutrality:** timestamps in rows do not determine ingestion order and late events are not silently dropped.
6. **Immutable receipt:** a committed identity, fingerprint, sequence and data reference remain available for duplicate detection.
7. **Explicit outcome:** callers can distinguish a new acceptance from an identical retry and from an identity conflict.

A successful response is only returned after the durable commit record is published. Acknowledged batches must remain discoverable after a clean restart.

## 2. Batch identity and idempotency

The public API will accept a caller-generated **128-bit BatchId**, represented as 16 bytes (or 32 lowercase hexadecimal characters at text boundaries). The all-zero ID is reserved and invalid.

The uniqueness scope is the stable database-local **TableId**, not the table's display name. Therefore a table rename does not reset the idempotency namespace. The same BatchId may be used for different tables, but not for two different payloads in the same table.

For each accepted batch, the engine computes a versioned SHA-256 fingerprint over its canonical ANF v0.2 bytes. Those bytes already capture the TableId, schema revision, ordered ColumnIds, row count and ordered typed values.

| Request | Required result |
|---|---|
| New BatchId | Validate and attempt one atomic commit |
| Existing BatchId + same fingerprint | Return the original receipt as a duplicate; do not append data or allocate a sequence |
| Existing BatchId + different fingerprint | Return `BatchIdConflict`; do not modify storage |
| Previously rejected BatchId | It has no durable receipt and may be retried after correcting the input |
| Empty batch | Reject as `EmptyBatch`; allocate no sequence and persist no receipt |

Accepted BatchIds are retained for the lifetime of the logical table. Future compaction may relocate receipt metadata, but it must not discard the idempotency history while the table remains addressable. Reusing an accepted ID is never a supported way to overwrite or amend data.

The fingerprint is an idempotency comparison, not an authentication mechanism. SHA-256 collision resistance is relied on for practical payload identity; the receipt is not a digital signature.

## 3. Per-table ingestion sequence

Each table has an independent unsigned 64-bit sequence beginning at **1**.

- The next sequence is the previous committed sequence plus one.
- A sequence is allocated only for a first-time accepted batch.
- Duplicate retries return the original sequence.
- Rejected batches and failed pre-commit attempts consume no sequence.
- Sequence order is commit/acceptance order, not timestamp order, client clock, batch ID or segment filename order.
- Sequence overflow fails closed; the counter never wraps.
- Sequence and receipt publication must be atomic from the reader's perspective. No visible gap may be introduced by a rejected or uncommitted batch.

Within a batch, source row order is preserved. A stable row position is its zero-based ordinal within the batch. The logical ingestion position is therefore `(table_id, ingestion_sequence, row_ordinal)`.

The durable Chronicle commit record is the authority for the next sequence. It must not be reconstructed from event timestamps.

## 4. Validation and atomic acceptance

Validation occurs against one captured current schema revision before any durable write begins.

The engine validates:
- BatchId format and nonzero value.
- Table existence and stable TableId.
- Every row's exact field count, data types and nullability.
- All scalar encoding limits and the complete encoded segment size.
- Schema revision and ordered ColumnIds used by the encoded payload.
- The batch-level row, cell and column ceilings.

**Existing ANF v0.2 limits are the initial hard ceilings for one atomic batch:**

| Limit | Maximum |
|---|---:|
| Rows in one batch/segment | 1,000,000 |
| Decoded cells in one complete batch | 1,000,000 |
| Columns | 4,096 |
| Encoded ANF segment | 256 MiB |
| One UTF-8 or binary value | 64 MiB |

The batch must fit one initial ANF segment. It is rejected rather than silently split across multiple independently visible segments. Multi-segment atomic batches require a later Chronicle transaction/manifest protocol and are outside this version.

If any row or encoded value fails validation, the whole batch is rejected. No prefix of the rows is inserted, no commit receipt is written and no ingestion sequence is consumed. Validation errors identify the failing row ordinal and field where available, without echoing sensitive payload contents.

An empty batch is a rejected request with zero rejected rows. This differs intentionally from the legacy `insert_batch(..., &[])` no-op behavior; the future idempotent ingestion API is explicit and does not create meaningless receipts.

## 5. Event time and out-of-order arrivals

Ingestion sequence is independent of event time.

- Late-arriving events are accepted if their values satisfy the schema and size constraints.
- Out-of-order timestamps are accepted and kept in source batch order.
- The engine does not silently reorder, deduplicate or discard rows based on timestamp.
- Event-time minimum/maximum and disorder statistics are derived metadata for later storage/navigation work; they do not replace the ingestion sequence.
- No timestamp column is inferred automatically. A future table policy may identify a designated event-time column explicitly.

A replayed BatchId is deduplicated as a whole batch. Two different BatchIds carrying identical row values are two distinct accepted batches unless a later, explicit row-level deduplication feature is designed.

## 6. Outcomes and result counters

The future API shape is:

```rust
ingest_batch(table, batch_id, rows) -> Result<IngestResult, IngestError>
```

`IngestResult` contains:
- `outcome`: `Accepted` or `Duplicate`;
- `batch_id` and stable `table_id`;
- original `ingestion_sequence`;
- `row_count` and encoded byte count;
- committed segment/Chronicle reference.

A conflicting ID, schema validation failure, empty batch, size limit, lock failure or storage error is an error result, never a partial-success result. A failure after the durable commit point but before a response is delivered is an uncertain client outcome; retrying the same BatchId resolves it to the original receipt or a new acceptance, without creating a second copy.

Operational counters are exposed as a snapshot and are not the source of truth. The minimum set is:

| Counter | Meaning |
|---|---|
| `accepted_batches` | First-time batches durably committed |
| `accepted_rows` | Rows in first-time committed batches |
| `accepted_encoded_bytes` | Encoded bytes in first-time committed batches |
| `duplicate_requests` | Requests resolved to an existing identical receipt |
| `rejected_requests` | Requests rejected before commit validation/publication |
| `publication_failures` | Failures before a commit record is published |
| `uncertain_publications` | Failures where the commit point may already be durable |

Duplicates do not increment accepted batch/row/byte counters. Rejected requests do not change accepted counters or sequence state. Counter semantics must be tested across process restart; durable totals are derived from Chronicle receipts, while process-local operational counters may reset.

## 7. Backpressure and failure behavior

The initial API is synchronous and has no hidden in-memory queue.

- One local database writer remains enforced by the M1.4 lock.
- Input exceeding a hard limit is rejected before publication.
- Disk-full, permission, filesystem and sync errors are returned to the caller.
- The engine does not silently drop, partially accept or retry batches in the background.
- The caller controls retry policy and must reuse the same BatchId for the same logical submission.
- A publication failure after the durable commit point is reported as uncertain; the caller must resolve it by retrying with the same BatchId or inspecting the authoritative receipt, never by generating a new ID.

The receipt and ingestion sequence must share one durable commit boundary. M2.2 must implement this using the Chronicle unit/commit record so a crash cannot leave accepted data without its deduplication receipt, or a receipt without its data.

## 8. Required M2.2 implementation tests

- Accept a new BatchId and return sequence 1.
- Accept a second BatchId and return sequence 2.
- Replay the first ID and return the original sequence without adding rows.
- Reuse an accepted ID with different content and reject with `BatchIdConflict`.
- Reuse the same ID on a different TableId.
- Rename a table and verify its idempotency namespace remains its TableId.
- Restart and replay an accepted ID.
- Reject a batch with one invalid row and verify no rows or sequence were committed.
- Reject empty, oversized row/cell/column/field/segment batches.
- Preserve source row order within batches.
- Accept late and out-of-order timestamps without sorting or dropping rows.
- Inject failures before and after the durable commit point and verify retry resolution.
- Verify accepted/duplicate/rejected counters and durable totals.
- Verify sequence overflow fails closed.

## 9. Compatibility with the current API

The existing `Database::insert_batch(table, rows)` API remains unchanged for source compatibility. It does not currently accept a BatchId, expose a durable ingestion receipt, or assign a logical ingestion sequence. It must not be described as idempotent.

The future `ingest_batch` API is additive and is backed by Chronicle commit records. During M2.2, the implementation must define how pre-existing ANF segments created by legacy `insert_batch` are represented in the initial Chronicle history. Legacy data must remain readable, and migration/reconciliation must not invent BatchIds or claim duplicate protection for submissions made before receipts existed.

## 10. Non-goals

This contract does not implement Chronicle, Capsule, Navigation Map, compaction, Weaves, row-level deduplication, partial-row acceptance, asynchronous queues, parallel ingestion, multi-writer concurrency, transactions across multiple tables or event-time ordering. Those require later milestones and separate evidence.
