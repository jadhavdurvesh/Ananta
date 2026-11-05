# ADR 0008: Chronicle Record as the Ingestion Commit Point

**Status:** Accepted for the M2.2 prototype  
**Date:** 2026-11-05

## Context

M2.1 requires data, BatchId receipt, fingerprint and per-table ingestion sequence to become durable together. Publishing an ANF data file and a separate receipt/index file independently could leave accepted data without an idempotency receipt or a receipt pointing to missing data.

## Decision

Store one immutable Chronicle file per accepted batch. Each file contains a fixed metadata header (TableId, BatchId, per-table sequence, schema revision, row count, payload length and SHA-256 payload fingerprint), one canonical ANF v0.2 payload, a whole-record checksum and an end marker.

Use M1.4's exclusive temp creation, file sync, same-directory no-overwrite hard-link publication and directory sync. The final .chr path is the single commit point. Receipt indexes and accepted counters are rebuilt from verified Chronicle files on open; request counters remain process-local.

Logical sequence and physical filename ID are independent. Physical IDs share the segment-directory namespace with .anf files to preserve mixed legacy/Chronicle scan ordering. Legacy ANF data remains readable and is not assigned fabricated BatchIds.

## Consequences

- Identical retries are resolved from the BatchId receipt index and original fingerprint.
- Conflicting BatchId reuse is rejected.
- A post-publication error is uncertain and is resolved by retrying the same BatchId.
- Startup verifies all Chronicle files and fails closed on malformed records, sequence gaps or duplicate BatchIds.
- Initial implementation writes one complete record buffer in memory and rebuilds the receipt index by scanning records. An on-disk index/checkpoint can be introduced only after correctness is established and benchmarks justify it.

See the Chronicle Record Format v0.1 and the Ingestion Contract v0.1.
