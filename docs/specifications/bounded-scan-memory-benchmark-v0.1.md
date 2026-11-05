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
- Rows are persisted in 10,000-row insert segments.
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

## Observed CI results

**Run:** [Native Rust workflow run 42038587833](https://github.com/jadhavdurvesh/Ananta/actions/runs/42038587833)  
**Runner:** GitHub-hosted Ubuntu Linux  
**Commit under test:** `7cb637d982822d42df6409007aea2fe1026130ce`

### Dataset-size comparison — 512-row batches

| Dataset rows | Max batch rows | Max decoded payload per batch | Peak RSS | Scan time |
|---:|---:|---:|---:|---:|
| 5,000 | 512 | 1,048,576 bytes (1 MiB) | 4,824 KiB | 131 ms |
| 50,000 | 512 | 1,048,576 bytes (1 MiB) | 5,144 KiB | 1,004 ms |

The table grew by 10× while peak RSS rose by 320 KiB (about 6.6%). This is consistent with the bounded scan not retaining the complete result.

### Batch-size comparison — 50,000-row dataset

| Configured batch rows | Observed max batch rows | Max decoded payload per batch | Peak RSS | Scan time |
|---:|---:|---:|---:|---:|
| 256 | 256 | 524,288 bytes (0.5 MiB) | 3,992 KiB | 1,006 ms |
| 1,024 | 1,024 | 2,097,152 bytes (2 MiB) | 7,240 KiB | 1,011 ms |
| 4,096 | 4,096 | 8,388,608 bytes (8 MiB) | 20,036 KiB | 1,024 ms |

The largest requested batch was fully exercised after setting generator segment size to 10,000 rows. Decoded payload bytes scale exactly with the batch row count. Peak RSS increases directionally with batch payload size and also includes runtime, allocator and metadata overhead.

### Conclusion

The M1.3 measurement gate is satisfied for this benchmark workload: increasing total rows tenfold at a fixed batch size did not materially increase peak RSS, while increasing batch size increased the measured memory footprint as expected.

These are one-run Linux CI measurements, not a universal production memory guarantee. Different allocators, payload sizes, selected-column widths, operating systems and filesystem caches can change absolute RSS.
