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
