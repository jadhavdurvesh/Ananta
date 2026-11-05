# ADR 0007: Idempotent, Commit-Ordered Batch Ingestion

**Status:** Accepted design contract for M2  
**Date:** 2026-11-05  
**Scope:** future native table ingestion API

## Context

ANANTA currently writes each `insert_batch` call to an ANF v0.2 segment. M2 introduces a stable ingestion history (Chronicle). A robust ingestion interface needs to answer: which client submission is this, whether it was already accepted, what order it occupies, and what the caller should do when publication outcome is uncertain.

Event timestamps cannot supply these answers: they can be late, duplicated or out of order. Segment filenames are physical storage details and should not define logical ingestion identity.

## Decision

1. Every future idempotent ingestion request supplies a caller-generated 128-bit BatchId, scoped by stable TableId.
2. A versioned SHA-256 fingerprint over canonical ANF v0.2 bytes distinguishes an identical retry from reuse of the same ID with different data.
3. A table receives a monotonically increasing 64-bit ingestion sequence for each first-time committed batch. Sequence assignment follows durable commit order, not event time.
4. A batch is accepted atomically or rejected as a whole. The initial batch must fit one ANF v0.2 segment and all current codec limits.
5. Row order within the input batch is preserved. Late and out-of-order event timestamps are accepted and never silently sorted or discarded.
6. Accepted IDs and their receipts remain available for the logical lifetime of the table. Identical retries return the original receipt; conflicting reuse fails.
7. The receipt, sequence and data reference must become visible at one durable Chronicle commit point. M2.2 must not publish data and deduplication metadata independently.
8. The first API is synchronous, single-writer and has no hidden queue. Operational counters are snapshots; durable totals derive from receipts.

## Consequences

- A new caller-visible API needs BatchId and explicit accepted/duplicate result variants.
- The same ID on a renamed table remains in the same namespace because TableId is stable.
- Two separate IDs containing the same rows are separate batches; row-level deduplication is not implied.
- Empty batches are rejected by the new API. Legacy `insert_batch` empty-input behavior remains unchanged for compatibility.
- A pre-commit failure consumes no sequence. A client retry with the same ID resolves an uncertain post-commit response.
- Existing ANF v0.2 is retained as the initial payload format. Chronicle metadata must carry the batch identity, fingerprint, sequence and receipt reference.
- M2.1 defines semantics; the durable implementation and counters belong to M2.2.

## Validation

The normative acceptance and failure matrix is in [Ingestion Contract v0.1](../specifications/ingestion-contract-v0.1.md). M2.2 must implement its restart, idempotency, failure-injection, ordering, limit and counter tests before M2.1 can be called implemented.
