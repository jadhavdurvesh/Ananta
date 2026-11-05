# ADR 0009: Immutable Capsules as Derived Chronicle Units

**Status:** Accepted for the M2.3 prototype  
**Date:** 2026-11-05

## Context

Chronicle provides an append-oriented, idempotent ingestion log. ChronoWeave's next unit must be independently verifiable and carry useful statistics without changing or weakening the committed source records.

## Decision

A Capsule is an immutable wrapper around one ANF v0.2 payload materialised from a contiguous range of verified Chronicle sequences. It carries TableId, caller-assigned CapsuleId and Generation, schema revision, row count, source sequence bounds, optional explicitly selected event-time bounds, per-column null counts, exact encoded column byte lengths, supported scalar min/max statistics, a SHA-256 payload fingerprint and whole-file checksum.

Historical source records are resolved by stable ColumnId into the current schema revision at Capsule build time. The embedded ANF segment remains the data correctness baseline and preserves the existing codec. Capsule files are published under a separate capsules directory using the M1.4 no-overwrite publication protocol.

Capsules are derived copies. M2.3 does not delete or supersede Chronicle data, publish an active generation manifest, or introduce query pruning.

## Consequences

- A source range must consist of contiguous committed Chronicle sequences; legacy ANF writes without receipts are excluded.
- Event time must be explicitly selected by the caller; the engine never guesses a timestamp column.
- Min/max metadata is supported for BOOLEAN, INT32, INT64, TIMESTAMP_MICROS and FLOAT64. UTF8/BINARY min/max are omitted in v0.1.
- Schema evolution is handled at build time by resolving historic source payloads through immutable schema snapshots.
- Validation checks wrapper integrity, payload fingerprint, embedded ANF identity, column byte sizes, schema snapshot and recomputed statistics.
- Explicit CapsuleId/Generation pairs can coexist. Active-generation promotion and Chronicle reclamation are deferred.
- Capsule sizing remains a benchmark parameter; no fixed optimal size is asserted.

See Capsule Format v0.1 for byte-level details and validation invariants.
