# Chronicle Ingestion and Startup Benchmark — M2.2.1

**Status:** Reproducible benchmark harness  
**Platform:** Linux GitHub Actions runner  
**Build profile:** Rust release (optimized)  
**Implementation under test:** native Database::ingest_batch and Database::open

## Purpose

Measure two M2.2 costs:

1. Per-batch ingestion overhead of Chronicle versus the pre-Chronicle insert_batch path.
2. Fresh-process startup time when opening a table with Chronicle receipts, including validation of each record and rebuilding the BatchId/sequence receipt index.

The comparison is a workload-level measurement. It includes row construction and API/encoding work in the generator process, not just time spent in the storage system call.

## Dataset and workload

Each generated database contains one table with:

- event_time TIMESTAMP_MICROS NOT NULL
- source UTF8 NOT NULL
- payload UTF8 NOT NULL
- reading FLOAT64 NOT NULL

The generator creates the same row contents for both modes:

- 128 rows per submitted batch;
- 512-byte UTF-8 payload per row;
- 16 deterministic source names;
- monotonically generated event timestamps;
- one data file per API call.

Chronicle mode assigns deterministic, unique 128-bit BatchIds. Legacy mode uses the unchanged insert_batch API.

The CI comparison contains 100 batches (12,800 rows) for both modes. A separate Chronicle dataset contains 25 batches (3,200 rows) for startup scaling.

## Process isolation

The benchmark binary is built with Rust's release profile so elapsed-time comparisons use optimized code. Generation and open measurements are separate process invocations. The generator exits after closing the database. The open process starts with a fresh address space, calls Database::open, and reports the time around that call. This includes lock acquisition, catalog/schema loading, orphan-temp cleanup, Chronicle checksum/fingerprint verification and in-memory receipt-index reconstruction.

## Reported metrics

Generation reports:
- API mode (legacy or chronicle);
- batch count and total rows;
- rows per batch and payload bytes per row;
- data-file count and total stored bytes;
- total ingestion elapsed microseconds;
- average elapsed microseconds per batch and per row.

Open reports:
- Chronicle record count;
- accepted batch and row totals reconstructed from receipts;
- complete Database::open elapsed microseconds.

The storage-byte comparison is descriptive. Chronicle files include a metadata envelope and fingerprint, while legacy ANF files contain only the ANF payload.

## CI comparisons

1. Generate 100 × 128 rows with 512-byte payloads through Chronicle.
2. Generate the identical workload through legacy insert_batch.
3. Open each database in a fresh process to compare baseline open versus Chronicle index rebuild.
4. Generate 25 Chronicle receipts and open that database.
5. Reopen the 100-receipt database again to compare startup cost as receipt count grows.

The benchmark is intentionally small enough for regular CI. It is not a high-scale throughput claim. Repeated runs on controlled hardware, larger receipt counts, allocator/RSS profiling and concurrent workload tests remain future performance work.

## Interpretation

Chronicle ingestion is expected to cost more than legacy insertion because it computes SHA-256, builds the metadata envelope, writes a larger record and publishes one durable receipt per accepted batch. Startup cost grows with Chronicle file count and payload bytes because this prototype verifies each receipt and payload fingerprint before accepting the database.

These costs are recorded before Capsule and Navigation Map work so later changes have a baseline. If startup growth becomes material, an authenticated/checksummed checkpoint or receipt index may be evaluated, but it must not weaken replay detection or record verification.

## Observed optimized CI results

**Workflow:** [Native Rust run 42045182231](https://github.com/jadhavdurvesh/Ananta/actions/runs/42045182231)  
**Build:** Rust release profile (optimized)  
**Runner:** GitHub-hosted Ubuntu Linux

### Ingestion comparison — 100 batches × 128 rows, 512-byte payloads

| Mode | Rows | Data files | Stored bytes | Total ingest time | Per batch | Per row |
|---|---:|---:|---:|---:|---:|---:|
| Legacy ANF insert_batch | 12,800 | 100 | 7,034,000 | 134.643 ms | 1.346 ms | 10 µs |
| Chronicle ingest_batch | 12,800 | 100 | 7,044,800 | 151.465 ms | 1.514 ms | 11 µs |

Chronicle took 16.822 ms more for 100 batches, approximately **12.5%** higher elapsed time in this run. Stored bytes increased by exactly 10,800 bytes, or **108 bytes per record** (the 96-byte Chronicle header plus 12-byte trailer), approximately **0.154%** over the legacy ANF bytes.

This is a whole-generator measurement: deterministic row construction, payload cloning, encoding, SHA-256, wrapping and publication are included. It is not a pure storage syscall benchmark.

### Fresh-process database open and receipt-index rebuild

| Dataset | Chronicle receipts | Rebuilt rows | Database::open elapsed |
|---|---:|---:|---:|
| Legacy ANF baseline | 0 | 0 Chronicle rows | 0.765 ms |
| Chronicle small | 25 | 3,200 | 10.818 ms |
| Chronicle large | 100 | 12,800 | 41.876 ms |

A repeated open of the 100-receipt database in the same workflow measured 40.721 ms. The 25-to-100 receipt increase produced approximately 3.9× startup time, close to the 4× record-count increase. The open timer includes lock/catalog/schema startup, full Chronicle record and payload verification, and in-memory receipt-index reconstruction.

The OS page cache was not cleared between processes. These are fresh-process measurements, not cold-storage latency.

### M2.2 performance-gate conclusion

For this bounded CI workload, ingestion overhead and receipt-index rebuild time are measured and recorded. Chronicle adds a modest per-record storage envelope and measurable validation work at startup. The startup behavior is approximately linear over 25–100 records; a receipt checkpoint/index optimization may be justified at larger scale, but should be driven by larger-workload measurements and must preserve the current recovery correctness.

These results close the M2.2 benchmark requirement for the prototype. They do not establish a production-scale performance or latency guarantee.
