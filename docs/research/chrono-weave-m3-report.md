# ChronoWeave M3 research report

**Status:** prototype experiment and reproducible CI measurement harness  
**Milestone:** M3 — offline compaction, candidate layouts and generation promotion

## Experiment question

Does physical row ordering in an immutable Weave candidate improve the current ANANTA read path without changing the logical result set?

The experiment compares three candidates generated from the exact same CIM1 snapshot:

- **Append-only:** ingestion sequence, then row ordinal.
- **Time-first:** ascending first TIMESTAMP_MICROS column, then stable origin.
- **Entity-first:** ascending first UTF8/integer entity column, then stable origin.

The mixed layout is intentionally not implemented. There is no workload evidence that a fourth multi-key ordering would improve the current reader, and adding it now would increase the verification surface without a query workload that could use it.

## Reproducible workload

The CI example \`chrono_weave_m3\` creates a fresh database with:

- 10 Chronicle batches × 1,000 rows = 10,000 logical rows.
- Timestamp, entity key, FLOAT64 reading and INT64 sample identifier.
- One immutable Capsule per Chronicle sequence.
- One identical, full-coverage CIM1 source manifest per layout (with a distinct JobId).
- A fixed finite budget for source bytes, candidate bytes, temporary disk, resident working memory, rows, open files and work-unit rows.

For each layout the harness records:

- ingestion and Capsule-build wall time;
- compaction wall time and Linux process CPU time when available;
- source bytes, candidate output bytes and final candidate storage bytes;
- peak staged candidate bytes and temporary-disk amplification;
- artifact count;
- five warm candidate read/decode wall-time and process-CPU samples;
- independently verified row count and data bytes read.

Every native Rust CI run uploads the raw JSON as the \`chrono-weave-m3-benchmark\` artifact. The artifact is the machine-readable record for that exact runner, commit and environment. CPU time is \`null\` when the platform cannot provide the Linux procfs measurement; it is never inferred from wall time.

## Correctness result

All three layouts are required to pass the same independent checks before they can be promoted:

1. The CIM1 source snapshot is revalidated against the actual Chronicle and Capsule files.
2. Every Capsule row is matched to its Chronicle source row at the same ingestion sequence and row ordinal using canonical typed-row fingerprints.
3. Every output ANF-v2 artifact and PVM1 sidecar is re-opened and fingerprint-checked.
4. Every physical output row is paired with its provenance record; the complete origin/fingerprint set must equal the source inventory exactly.
5. Candidate rows are compared with the reference logical table scan by canonical typed-row fingerprint multiset.
6. The active pointer is changed only after all of those checks pass.

The end-to-end test runs append-only, time-first and entity-first candidates against one four-row dataset, verifies all three, promotes the first two in turn, rolls back to the first, and checks the logical rows after each view change. The larger CI example exercises the same API with 10,000 rows.

## Performance conclusion for the current prototype

**No read-performance benefit is claimed for time-first or entity-first in the current M3 reader.**

The current full-table scan decodes every selected Weave artifact. It does not yet have a predicate-aware Weave index, zone-map pruning, clustering-aware range selection or AQL query planning. Sorting therefore changes physical order, but does not reduce the number of rows decoded by the existing full-scan workload. The sorting and merge work adds CPU and candidate-build cost. Append-only is the appropriate default for the current API.

This is a documented negative result for the currently implemented read workload, not a claim that sorted layouts can never help. Time-first and entity-first remain experimental layout options for future query workloads that can exploit their ordering. Mixed layout is not justified until such a workload and a repeatable benefit are demonstrated.

The uploaded JSON is retained so future changes to the reader can be compared against the same deterministic data generator. Do not compare measurements across different runner types, Rust versions or build profiles without recording those environment details.

## Limits and known boundaries

- The executor uses a finite in-memory working set and rejects jobs whose conservative memory estimate exceeds the configured budget. It does not yet implement disk-spilled external sorting.
- The current benchmark is a synthetic single-node workload, not a claim of production-scale performance.
- Read timing is a full artifact read/decode measurement, not a predicate query latency.
- Process CPU timing is available on Linux through procfs and \`getconf CLK_TCK\`; other platforms may return \`null\`.
- Candidate data and source Chronicle/Capsule files remain immutable. Promotion only changes the active-generation pointer. The previous generation is retained for rollback.
- Atomic active-pointer replacement is validated on Unix filesystems with same-directory rename and directory sync. Other platforms must not claim the same crash guarantee until a platform-specific atomic replacement primitive is implemented.

## Acceptance decision

For M3, the layout experiment is complete as a correctness comparison plus a documented negative performance result for the current full-scan workload. Append-only is the default. Time-first and entity-first are valid experimental candidates, but no performance advantage is asserted. Mixed layout is deferred because it is not justified by the measured workload.
