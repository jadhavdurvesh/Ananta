# Bounded Scan Memory Benchmark — M1.3.2

**Status:** Reproducible benchmark harness  
**Platform:** Linux CI runner (uses `/proc/self/status` `VmHWM`)  
**API under test:** `Database::scan_batches`

## Purpose

Measure the process peak resident set size (RSS) of a bounded scan while changing (a) total persisted rows at a fixed batch size and (b) batch size against a fixed persisted dataset.

This is intended to validate that decoded-result memory is driven primarily by the configured batch and selected columns, not by total table cardinality. It is not a throughput benchmark and it does not establish a universal memory limit for all payloads.

## Isolation

The example has separate `generate` and `scan` commands. CI runs generation and each scan as separate processes. The scan child starts with a fresh address space and does not retain generator vectors. The scanner consumes each `RecordBatch`, counts its rows and payload bytes, then drops it at the end of the loop iteration before requesting the next batch.

## Dataset

- Schema: `sequence INT64 NOT NULL`, `payload UTF8 NOT NULL`.
- Payload length: 2,048 bytes per row.
- Rows are persisted in 2,000-row insert segments.
- Small dataset: 5,000 rows (about 10 MiB of raw UTF-8 payload).
- Large dataset: 50,000 rows (about 100 MiB of raw UTF-8 payload).

The payload is deterministic. The exact ANF file size includes headers, directories and per-value encoding markers/lengths.

## Measurements

**Dataset-growth comparison:** scan 5,000 and 50,000 rows with a fixed 512-row batch.

**Batch-growth comparison:** scan the 50,000-row dataset with batches of 256, 1,024 and 4,096 rows.

Each process reports:
- rows scanned;
- configured and observed maximum batch rows;
- maximum decoded UTF-8 payload bytes in one batch;
- peak process RSS (Linux `VmHWM`, KiB);
- scan elapsed milliseconds.

The payload-byte metric is a deterministic decoded-output lower-bound/proxy, while RSS includes the runtime, database metadata, checksum buffer, allocator overhead and decoded values. Both are reported so RSS is not misrepresented as pure batch allocation.

## Interpretation and limitations

For the fixed-batch comparison, peak RSS should remain in the same general range as row count grows tenfold; it need not be bit-for-bit equal because allocator and OS behavior vary. For the batch sweep, maximum decoded payload bytes should scale linearly with batch rows (512 KiB, 2 MiB and 8 MiB respectively), and RSS should show the same directional trend.

This is a single Linux-runner measurement. It does not replace repeated multi-platform performance testing, allocator profiling, or a workload-specific memory budget. The benchmark is run by the native Rust GitHub Actions workflow and its raw output is retained in the workflow log.
