# Capsule Size and Scan Benchmark — M2.3.1

**Status:** Reproducible benchmark harness  
**Platform:** Linux GitHub Actions runner  
**Build profile:** Rust release (optimized)

## Purpose

Compare three Capsule sizes on identical logical data and measure how size affects build time, storage size, and Navigation Map scan work.

## Dataset

Every run builds the same table:

- 256 Chronicle ingestion batches;
- 64 rows per batch (16,384 total rows);
- event_time TIMESTAMP_MICROS, device UTF8, payload UTF8, reading FLOAT64;
- 512-byte payload per row;
- deterministic values and stable BatchIds.

The only variable is the number of Chronicle batches represented by each Capsule:

| Profile | Chronicle batches per Capsule | Rows per Capsule | Expected Capsule count |
|---|---:|---:|---:|
| Small | 8 | 512 | 32 |
| Medium | 32 | 2,048 | 8 |
| Large | 128 | 8,192 | 2 |

The benchmark creates separate databases per profile to avoid overlapping source ranges or duplicate copies in a single logical scan.

## Query workload

A timestamp range selects event_time values 4,096 through 8,191 inclusive (4,096 rows), projecting device and reading. The benchmark runs the same scan twice:
- unpruned reference scan (all Capsules);
- Navigation Map scan with time-range pruning.

It verifies that both result row vectors are exactly equal. A mismatch fails CI.

## Reported metrics

- Capsule count and min/max rows per Capsule;
- total, minimum and maximum Capsule bytes;
- total ingestion time;
- total Capsule build time;
- Navigation Map build time;
- unpruned and mapped scan elapsed microseconds;
- Capsules considered, skipped and read;
- rows examined and returned;
- bytes verified against the immutable map;
- embedded ANF payload bytes decoded by each path.

Map verification bytes are reported separately because M2.4 verifies the exact Capsule fingerprint before trusting pruning metadata. This correctness-first prototype can read the full Capsule bytes to check the fingerprint even when it then skips ANF decoding. Therefore fewer decoded payload bytes does not automatically mean fewer physical bytes read or lower elapsed time.

## Interpretation limits

This benchmark is a controlled, one-run CI comparison, not a statistically stable performance claim. Capsule groups are bounded by Chronicle batch boundaries. It does not benchmark concurrent ingestion, OS cold-cache behavior, compression, predicate pushdown inside one Capsule, or cross-system comparisons. The benchmark's role is to establish a reproducible size/granularity baseline before M3 compaction research.

## Observed optimized CI results

**Workflow:** [Final Native Rust run 42052298912](https://github.com/jadhavdurvesh/Ananta/actions/runs/42052298912)  
**Build:** Rust release profile (optimized)  
**Runner:** GitHub-hosted Ubuntu Linux  
**Rows:** 16,384 per profile  
**Query:** 4,096 returned rows (event_time 4,096–8,191 inclusive)

| Profile | Capsule count | Rows/Capsule | Capsule bytes total | Capsule build | Map build | Baseline scan | Mapped scan | Capsules skipped/read |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Small | 32 | 512 | 9,005,568 | 363.124 ms | 123.952 ms | 271.537 ms | 84.087 ms | 24 / 8 |
| Medium | 8 | 2,048 | 8,993,664 | 346.532 ms | 125.930 ms | 277.840 ms | 87.648 ms | 6 / 2 |
| Large | 2 | 8,192 | 8,990,688 | 349.158 ms | 127.558 ms | 282.192 ms | 131.695 ms | 1 / 1 |

### Decode and verification work

| Profile | Baseline rows examined | Mapped rows examined | Bytes verified against map | Baseline payload bytes decoded | Mapped payload bytes decoded |
|---|---:|---:|---:|---:|---:|
| Small | 16,384 | 4,096 | 9,005,568 | 8,994,432 | 2,248,864 |
| Medium | 16,384 | 4,096 | 8,993,664 | 8,990,880 | 2,247,976 |
| Large | 16,384 | 8,192 | 8,990,688 | 8,989,992 | 4,492,436 |

All three mapped result sets were byte-for-byte/value-for-value equal to their unpruned reference result sets. Each returned 4,096 rows.

### Interpretation

Smaller Capsules provide finer pruning: the small profile skipped 24 of 32 Capsules, the medium profile skipped 6 of 8, and the large profile skipped 1 of 2. The small and medium profiles decoded approximately one quarter of the ANF payload bytes that the baseline decoded for this query; the large profile decoded approximately half.

Capsule total storage differed by only about 14.9 KiB between the smallest and largest layouts for this fixed payload-heavy dataset. The fixed per-Capsule metadata overhead is visible but small relative to the payload.

**Important:** M2.4 currently reads and SHA-256 verifies the complete bytes of every Capsule before using its statistics. Therefore the map reduces row examination and ANF decoding, but this benchmark does not establish reduced physical disk bytes read. Timings are one CI run and may be affected by filesystem cache and runner variance; they are not latency guarantees.
