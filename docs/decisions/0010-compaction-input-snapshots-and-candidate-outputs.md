# ADR 0010: Pin Compaction Inputs and Publish Immutable Candidate Outputs

**Status:** Accepted for the M3.1 design  
**Date:** 2026-11-05

## Context

M2 completed the Chronicle, Capsule and Navigation Map prototypes. Capsules and Chronicle records are immutable, but there is not yet a defined protocol for reorganising those units into physical Weaves.

Compaction must never make a partial output visible, lose or duplicate logical rows, overwrite a source object, or silently change the active read path. The existing Capsule and Navigation Map formats already use explicit identity, checksums and no-overwrite publication; M3 must preserve those guarantees across a multi-file job.

## Decision

Compaction is an explicit offline operation over an immutable, fingerprinted input manifest. The input manifest pins exact Capsule identities and file hashes, and enumerates every Chronicle receipt in the contiguous source-sequence interval. It also pins the target immutable schema revision.

Each compaction attempt has a stable JobId and monotonically increasing Attempt. It writes only into its own staging directory and produces an immutable candidate output manifest after all data and provenance files have been independently verified.

Every physical output row carries a stable source origin identity (TableId, IngestionSequence, RowOrdinalWithinBatch). The provenance mapping is verified as an exact set against the input receipt inventory; row count alone is insufficient to establish that no rows were duplicated or omitted.

Job state is append-only. M3.1 chooses safe discard and retry with a fresh attempt rather than resuming partially written sort state. Candidate output is not active. M3.3 must separately implement and test active-generation promotion and rollback.

## Consequences

- Chronicle and Capsule inputs remain authoritative and are never deleted by compaction.
- A stale or modified input invalidates the attempt before output can be published.
- Gaps, overlaps, duplicated source ranges, missing receipts and invalid row provenance fail closed.
- Resource budgets and cooperative cancellation are part of the job contract, not optional operational enhancements.
- An interrupted attempt may leave staging files, but only canonical files owned by that attempt may be cleaned.
- Candidate generations can coexist with their inputs; storage reclamation is deferred until a later retention protocol.
- The first implementation is single-writer and offline, consistent with current database ownership semantics.
- Performance benefit is not assumed; M3.2 must measure alternative layouts before M3.3 can promote one.

See [Compaction Manifest and Job Contract v0.1](../specifications/compaction-manifest-v0.1.md) for the normative invariants and acceptance tests.
