# ChronoWeave Storage — ANANTA Research Proposal

**Working name:** ChronoWeave  
**Status:** ChronoWeave research prototype v0.1 complete for Chronicle, derived Capsule and correctness-first Navigation Map; Weave compaction and generation promotion remain M3 work  
**Scope:** Hybrid analytical and time-series storage  
**Related format:** ANANTA Native Format (ANF)

## 1. Research question

Can one native storage architecture support rapid append ingestion and efficient analytical reads by organising immutable data into time-aware, independently navigable columnar units, then adapting compacted physical layout to measured query workloads?

ChronoWeave is proposed as ANANTA's storage research direction. Its differentiator must be measured; the name and conceptual model do not establish novelty.

## 2. Core vocabulary

### Chronicle
An append-oriented ingestion history. New batches are recorded in sequence and become immutable after publication. A Chronicle gives the engine a straightforward write path and a canonical order of accepted batches.

### Capsule
A bounded, self-describing columnar unit produced from one or more ingestion batches. A capsule contains:
- Schema/object identity and format version.
- Row count and sequence/generation metadata.
- Temporal minimum/maximum where the schema contains time.
- A column directory with offsets, lengths and type codes.
- Per-column null counts and min/max where meaningful.
- Optional cardinality sketches and probabilistic membership filters.
- Integrity checks and independently encoded column payloads.

Capsule sizing is a benchmark parameter, not a fixed promise.

### Weave
A verified physical generation made by compacting capsules and arranging data for an observed access pattern. Candidate profiles include:
- Time-first: recent/range scans and time-window aggregates.
- Entity-first: repeated lookups or series scans by device/entity.
- Mixed: a measured compromise for workloads combining time and entity predicates.

The logical table and row values must be identical regardless of active weave. AQL queries target logical tables, not physical profile names.

### Navigation Map
A compact index over capsules and columns. It enables the engine to reject irrelevant capsules and blocks before decoding column payloads. Candidate metadata includes temporal bounds, zone maps, null counts, cardinality summaries and optional Bloom filters.

False positives in probabilistic filters are acceptable; false negatives are not. A navigation map may never exclude a row that could satisfy a query.

## Relationship to the M2.1 ingestion contract

The accepted [Ingestion Contract v0.1](ingestion-contract-v0.1.md) defines BatchId, payload fingerprint, per-table commit-order sequence, atomic batch acceptance, event-time neutrality and caller-visible outcomes. Chronicle units/commit records must preserve that receipt and sequence information at the same durable commit boundary as the accepted data. The current ANF segment plus `insert_batch` API is the pre-Chronicle baseline and does not yet provide durable idempotency receipts.

## 3. Conceptual write and read paths

### Write path
1. Validate the incoming batch against the native catalog schema.
2. Assign an ingestion sequence and stable batch identity.
3. Append/publish an immutable Chronicle unit.
4. Build or flush columnar Capsules.
5. Update the catalog/manifest using a crash-conscious publication protocol.
6. Later, schedule compaction when measured thresholds justify it.

### Read path
1. AQL produces a logical query.
2. The planner derives time, entity and column requirements.
3. The Navigation Map prunes impossible capsules/blocks.
4. The planner selects the available Weave generation or Chronicle path.
5. Storage decodes only required columns.
6. Vectorised operators filter, aggregate and return results.

## 4. Adaptive storage experiment

Do not continuously rewrite data based on noisy query samples. The first adaptation experiment should be offline and explicit:

1. Capture a representative workload profile.
2. Build candidate weave generations in temporary storage.
3. Verify row counts, checksums and query-result equivalence.
4. Benchmark ingestion overhead, storage amplification, compaction cost and query latency.
5. Promote only when a documented threshold is met.
6. Update the manifest atomically and retain the previous generation until validation/rollback conditions are satisfied.

Any online or background automatic adaptation requires a later reliability and resource-governance design.

## 5. Failure and correctness rules

- Chronicle/segment publication must not expose partial files as committed data.
- Catalog/manifest updates must be ordered and validated.
- A weave must be traceable to source generations and have a verifiable row-count/checksum summary.
- Compaction must be idempotent or safely resumable.
- Promotion must not create duplicate or missing logical rows.
- If a candidate weave fails validation, the previous generation remains active.
- Navigation metadata may skip data only when its bounds prove the data cannot match a query.
- The system must be able to rebuild derived navigation structures from verified source data where practical.

## 6. Research comparisons

Compare each candidate with:
- A simple append-only columnar baseline.
- Time-first and entity-first physical ordering.
- Existing ClickHouse and other relevant systems using equivalent logical datasets and queries.

Measure:
- Ingestion throughput and p50/p95 latency.
- Capsule count and size distribution.
- Compressed and uncompressed bytes.
- Time-range and entity-range query latency.
- Columns/bytes read and capsules skipped.
- Compaction CPU, elapsed time, peak memory and temporary disk amplification.
- Tail latency under concurrent ingestion and reads.

Run uniform, skewed, bursty and late-arriving datasets. Keep input seeds, query parameters, hardware, software versions and raw result files.

## 7. Risks and open questions

- One physical weave may not serve changing workloads well.
- Multiple active generations can amplify storage usage.
- Compaction can contend with ingestion and query reads.
- Tiny capsules increase metadata overhead; huge capsules reduce pruning granularity.
- Statistics can become stale or expensive to maintain.
- Adaptive decisions can oscillate on unstable workloads.
- Checksums detect accidental corruption but are not authentication.
- Schema evolution and column addition/removal require explicit format/catalog rules.

## 8. Prototype sequence

1. Keep the current ANF segment codec as the first baseline, not as the final ChronoWeave format.
2. [Implemented prototype] Add Capsule identity, temporal bounds and column statistics to a versioned format revision (Capsule Format v0.1).
3. [Implemented prototype] Build a simple versioned Navigation Map and test safe range pruning with result-equivalence checks; full-file fingerprint verification currently reduces decoding rather than guaranteeing lower physical I/O.
4. Implement immutable append/flush and manifest publication.
5. Add offline compaction to time-first and entity-first weaves.
6. Add correctness-preserving generation promotion and rollback.
7. Build a workload profiler only after the basic compaction experiment is reproducible.

ChronoWeave remains a research prototype, not an adaptive production storage engine. M2 measured Capsule sizes and demonstrated correctness-first Navigation Map pruning. M3 must now test whether time-first/entity-first Weaves improve representative workloads and introduce safe generation promotion/rollback. Do not claim that ChronoWeave is unique in the industry without a prior-art review and published evidence.
