# Physical Planning, Explain and Statistics Contract v0.2

**Status:** M5.4 implementation baseline  
**Crates:** ananta-aql, ananta-execution

## 1. Planning pipeline

The existing AQL logical SELECT plan is compiled into an initial physical operator pipeline:

Storage Scan -> optional Vector Filter -> Projection -> optional Limit

The scan requests only the columns needed by the projection and supported predicate. The filter is evaluated before projection so predicate-only columns need not appear in the output. Projection preserves caller order, aliases and repeated output columns.

Native lowering is intentionally conservative:
- Direct column projection items are supported.
- WHERE supports typed comparisons, AND, OR, NOT, IS NULL / IS NOT NULL, boolean columns and boolean constants/parameters.
- Unsupported predicates or computed projection expressions use the existing AQL reference evaluator.
- A storage-level LIMIT is pushed only when there is no WHERE predicate. With a filter, LIMIT stays above the filter so rejected rows do not reduce the requested result count.

The optimizer can be disabled explicitly through execute_with_options; this preserves the reference evaluator as a differential correctness path.

## 2. Physical explain

explain(database, query, bindings, optimizer_enabled) returns an AqlPhysicalPlan with the selected operator path, scan columns, pushdown decisions, Chronicle ingestion counters and estimate provenance.

AqlPhysicalPlan::explain() renders the plan in readable text. Calling explain without table-analysis results deliberately reports row and payload estimates as unknown; Chronicle ingestion counters are not assumed to equal the full table size because tables can also contain legacy inserted segments.

explain_with_statistics accepts TableStatistics returned by an explicit analyze_table call. It checks that the statistics belong to the resolved table and then estimates rows and projected payload using those analyzed table counts.

## 3. Cardinality estimates

The first estimator uses a documented heuristic, not histograms:
- Equality: 10% selectivity.
- Inequality: 90%.
- Ordered range comparisons: 33%.
- IS NULL: 10%; IS NOT NULL: 90%.
- AND: product of child selectivities.
- OR: inclusion-exclusion estimate.
- NOT: one minus child selectivity.
- Unknown expression: 50%.

Estimated filtered rows are rounded from analyzed scanned rows times selectivity. LIMIT caps the estimated output count. With explicit table statistics, equality/inequality selectivity uses approximate distinct counts and the non-NULL fraction; IS NULL / IS NOT NULL uses the exact observed NULL fraction. Numeric and timestamp range predicates use the sampled histogram, interpolating within a bucket and using exact observed min/max as the fallback when no histogram is available. These estimates remain approximate for skewed or clustered data and must not be presented as measured cardinality. When no analyzed statistics are supplied, the fixed fallback heuristics are used.

## 4. Runtime query profile

AqlExecutionMetrics records measured output batches, output rows and output cells for values returned to the caller. output_value_payload_bytes counts the actual value-body bytes returned (fixed-width scalar bodies and UTF8/BINARY payload bytes); it excludes null markers, vector/column descriptors, allocator overhead and storage-file encoding overhead. output_bytes is a separate estimate of the owned result-batch footprint, not allocator RSS or physical disk I/O.

These counters describe returned output, not rows scanned or bytes read from disk. Scan input bytes are decoded value-body bytes, not bytes physically read from ANF/Chronicle files. Runtime physical file I/O counters and operator-by-operator timings remain future profiling work. A fixture-level plan/profile comparison checks estimated output rows against actual output rows and separately asserts decoded scan input and returned value-body bytes.

## 5. Encoded-byte and payload estimate

Chronicle accepted encoded-byte counters are reported separately from analyzed batch payload. The explicit table analyzer measures scanned rows and sums estimated owned batch-payload sizes. A projected payload estimate scales that analysis value by the fraction of columns required by the query, assuming uniform per-column payload. This is a planning heuristic; variable-length values and storage layout can make actual operator allocations differ.

## 6. Table and column analysis statistics

analyze_table(database, table, batch_rows) performs an explicit bounded full-table scan. It returns:
- Exact scanned row count for this scan.
- Exact per-column NULL and non-NULL counts.
- Exact per-column minimum and maximum for supported scalar types (FLOAT64 uses total ordering).
- Approximate per-column distinct count using a fixed-memory 64-register HyperLogLog estimator.
- A deterministic bottom-hash sample of up to 256 non-NULL values per column, emitted as up to 16 ordered histogram buckets.
- Sum of estimated owned batch payload sizes.
- Chronicle accepted row and encoded-byte counters.

The analysis scan can read the full table. TableStatistics supports a versioned AST1 binary encoding with a checksum, atomic save_to/load_from persistence, and a database-managed cache path keyed by stable table ID. analyze_table_and_persist writes this cache atomically; load_table_statistics returns it only while table ID, schema revision, column descriptors and the scan-inventory fingerprint remain current. The richer AQL explain_plan API consumes a valid cached snapshot and displays exact scanned row count, exact NULL count/min/max, approximate NDV, and histogram bucket availability per column. After a write or active Weave change, the cache becomes unavailable to the planner. Version 2 stores histogram buckets; the decoder remains able to read version-1 snapshots that contain no histogram. It captures table identity, schema revision and a lightweight fingerprint of the immutable scan-file inventory plus active Weave identity before and after the scan. If the inventory changes during analysis, the analysis fails and must be rerun. EXPLAIN checks the fingerprint and rejects statistics after new data files or an active Weave generation change. Analysis is explicit and is not run implicitly before every query. The database-managed snapshot is a disposable cache; a corrupt file is reported and a stale file is treated as a cache miss that must be regenerated. The cache is stored beside catalog metadata under catalog/statistics, but it is not a transactional catalog record and automatic refresh scheduling remains future work. Equality, NULL and numeric/timestamp range estimates consume HLL, exact null fractions, min/max and sampled histogram data when a fresh snapshot is supplied.

## 7. Capsule / Navigation Map pruning boundary

Capsules and Navigation Maps are derived analytical artifacts. A table may contain Chronicle rows not represented in a particular Capsule generation, or legacy ANF segments outside Chronicle accounting. Therefore, an AQL table query must not replace a full table scan with a Capsule-only scan, nor report Chronicle ingestion rows as exact table cardinality, unless complete and current coverage can be proven for that table and snapshot. The coverage proof also rejects any standalone legacy ANF file. The scalar AQL execute path continues to use its bounded base-table scan. The separate vectorized AQL path may use Capsule/Navigation Map pruning for extracted timestamp ranges only when the core proves contiguous non-overlapping Capsule coverage of every Chronicle sequence and accepted row, no standalone legacy ANF file remains, a map generation is configured, and materialisation row/byte caps are met. Any failed check causes a fallback to the authoritative table scan.

## 8. Validation

Tests compare native execution with optimizer-disabled reference execution on supported SELECT queries, verify computed projections fall back, exercise NULL behavior, aliases/repeated projections, cancellation, explain decisions, safe LIMIT pushdown, and vectorized Capsule pruning equivalence with fallback after a legacy ANF insert. The workspace CI runs formatting, tests, Clippy, native examples and storage benchmarks.

## 9. Non-goals

- Cost-based join ordering or join operators.
- Persisted histograms and automatic ANALYZE scheduling.
- Adaptive runtime re-optimization.
- Capsule coverage inference or unsafe implicit Capsule substitution.
- Distributed query planning.
