# Time-Series Execution Contract v0.1

**Status:** M5.3 implementation baseline  
**Crate:** `ananta-execution`

## 1. Timestamp model

The first time-series operators consume ANANTA `TIMESTAMP_MICROS` values as signed 64-bit microseconds from the Unix epoch. Bucket boundaries are UTC/epoch based; no session-local timezone or daylight-saving conversion is applied inside the execution engine. Applications that ingest local wall-clock time must convert it to UTC before writing timestamp values.

NULL timestamps do not belong to a time range, time bucket or latest-per-series result and are excluded.

## 2. Time-range filter

`TimestampRangeOperator` accepts optional inclusive/exclusive lower and upper bounds. The lower bound uses >= or >; the upper uses <= or <. Bounds are compared in microseconds without lossy conversion. A lower bound greater than the upper bound is rejected. Equal bounds are permitted; when either side is exclusive, the interval naturally returns no rows.

The operator preserves the input schema and uses the existing three-valued filter semantics, so NULL timestamps are removed.

## 3. Fixed time buckets

`TimeBucketOperator` accepts a positive fixed interval in microseconds, a timestamp column, optional series-key columns and aggregate specifications. Bucket starts are computed as:

```
bucket_start = floor(timestamp_micros / interval_micros) * interval_micros
```

The floor is Euclidean, which matters for pre-1970 negative timestamps. Buckets are left-closed/right-open: [bucket_start, bucket_start + interval). They are anchored at epoch zero. Multiplication is checked; a bucket start outside INT64 is an execution overflow error.

Rows may arrive late or out of event-time order. All rows mapping to the same series/bucket are combined; source order does not affect integer results, while floating-point aggregates retain the M5.2 source-order IEEE-754 policy.

## 4. Aggregates and missing intervals

Time buckets reuse the M5.2 COUNT, SUM, MIN, MAX and AVG implementations. The v0.1 convenience operator exposes any of these aggregate specifications after the grouping keys (series columns followed by `__ananta_bucket_start`). COUNT(*) counts rows; column aggregates ignore NULL values.

Only buckets represented by at least one non-NULL timestamp row are emitted. Missing intervals are not synthesized and no gap filling or interpolation occurs. This avoids implying values for periods with no observations.

## 5. Latest value per series

`LatestPerSeriesOperator` returns the full source row with the maximum timestamp for each series key. With no series columns it returns the single latest row globally. NULL timestamps are ignored. When timestamps tie, the last row encountered in the deterministic upstream scan order wins.

The operator is blocking and stores one candidate row per distinct series key. Its estimated retained state is bounded by `max_working_set_bytes`; the output is emitted in batches bounded by `max_batch_rows` and `max_batch_cells`. It does not spill to disk.

## 6. Resource and cancellation behavior

Time-range filtering and bucket expansion are pull-based and check cancellation between batches and periodically within large batches. Bucket aggregation is blocking because it reuses the M5.2 group-state implementation and inherits its group-state memory ceiling. Latest-per-series maintains one row per key and fails closed when its state or output working set exceeds the configured budget.

## 7. Validation coverage

Tests cover inclusive/exclusive range boundaries, NULL timestamps, UTC microsecond comparison, epoch-anchored floor behavior for negative timestamps, late-arriving events, bucket aggregates, latest timestamp selection, last-seen tie breaking, and distinct-series state memory limits.

## 8. Non-goals

- Calendar months/years, timezone-aware buckets and daylight-saving transitions.
- Empty-bucket generation, interpolation and gap filling.
- Watermarks, streaming windows and incremental state persistence.
- Parallel aggregation or spill-to-disk.
- AQL parser/planner integration; this is M5.4 work.
