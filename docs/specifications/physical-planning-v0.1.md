# Physical Planning, Explain and Statistics Contract v0.1

**Status:** M5.4 implementation baseline  
**Crates:** ananta-aql, ananta-execution

## 1. Planning pipeline

The existing AQL logical SELECT plan is compiled into an initial physical operator pipeline:

Storage Scan -> optional Vector Filter -> Projection -> optional Limit

The scan requests only the columns needed by the projection and supported predicate. The filter is evaluated before projection so predicate-only columns need not appear in the output. Projection preserves caller order, aliases and repeated output columns.

AQL expressions are compiled once against the catalog schema. Native lowering is intentionally conservative:
- Direct column projection items are supported.
- WHERE supports typed comparisons, AND, OR, NOT, IS NULL / IS NOT NULL, boolean columns and boolean constants/parameters.
- Unsupported predicates or computed projection expressions use the existing AQL reference evaluator.
- A storage-level LIMIT is pushed only when there is no WHERE predicate. With a filter, LIMIT stays above the filter so rejected rows do not reduce the requested result count.

The optimizer can be disabled explicitly through execute_with_options; this preserves the reference evaluator as a differential correctness path.

## 2. Physical explain

explain(database, query, bindings, optimizer_enabled) returns an AqlPhysicalPlan containing:
- Resolved table and scan column list.
- Whether native execution is selected.
- Whether the predicate and LIMIT are pushed down.
- Chronicle ingestion row and encoded-byte counters.
- Estimated rows after filtering and after LIMIT.
- Estimated projected encoded bytes.
- Notes describing the estimate's provenance and limitations.

AqlPhysicalPlan::explain() renders a human-readable operator summary. The output is a planned physical path; it is not a runtime profile.

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

Estimated filtered rows are rounded from Chronicle's accepted-row counter times selectivity. LIMIT caps the estimated output count. These heuristics may be inaccurate for skewed data and must not be presented as measured cardinality.

## 4. Encoded-byte estimate

Chronicle's accepted encoded-byte counter is reported separately from the projected-byte estimate. The first projected-byte estimate scales the table byte counter by the fraction of required columns, assuming uniform column encoded widths. It is a planning heuristic only; compression, variable-length data, file layouts and metadata mean actual I/O can differ.

Runtime AqlExecutionMetrics report output batches, output rows and an estimated owned output payload size accumulated from returned result batches. The payload estimate is not allocator RSS or physical disk bytes.

## 5. Table and column analysis statistics

analyze_table(database, table, batch_rows) performs an explicit bounded full-table scan. It returns:
- Exact scanned row count for this scan.
- Exact per-column NULL and non-NULL counts.
- Exact per-column minimum and maximum for supported scalar types (FLOAT64 uses total ordering).
- Approximate per-column distinct count using a fixed-memory 64-register HyperLogLog estimator.
- Sum of estimated owned batch payload sizes.
- Chronicle accepted row and encoded-byte counters.

The analysis scan is explicit and can read the entire table. It is not run automatically before a query and these statistics are not persisted in the catalog in v0.1. The planner does not currently consume the HLL/min/max statistics for selectivity; integrating persisted stats and histogram-based estimates is future work.

## 6. Capsule / Navigation Map pruning boundary

Capsules and Navigation Maps are derived analytical artifacts. A table may contain Chronicle rows not represented in a particular Capsule generation. Therefore, an AQL table query must not replace a full table scan with a Capsule-only scan unless complete and current coverage can be proven for that table and snapshot. M5.4 does not yet enable Capsule pruning for ordinary AQL table queries. The existing core Capsule/Navigation Map API remains separately available and has its own correctness tests.

## 7. Validation

Tests compare native execution with optimizer-disabled reference execution on supported SELECT queries, verify computed projections fall back, exercise NULL behavior, aliases/repeated projections, cancellation, explain decisions and safe LIMIT pushdown. The workspace CI runs formatting, tests, Clippy, native examples and storage benchmarks.

## 8. Non-goals

- Cost-based join ordering or join operators.
- Persisted histograms and automatic ANALYZE scheduling.
- Adaptive runtime re-optimization.
- Capsule coverage inference or unsafe implicit Capsule substitution.
- Distributed query planning.
