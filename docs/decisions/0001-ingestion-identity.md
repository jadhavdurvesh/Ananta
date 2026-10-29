# 0001: Batch identity and dedup
Each insert batch gets a `batch_id` (UUID) used as `insert_deduplication_token`, so a retried batch is ignored by
ClickHouse (within the replicated-dedup window). `event_id` alone does NOT guarantee uniqueness. Guarantee promised:
at-least-once delivery with batch-level idempotent retry. Row-level dedup across different batches is out of scope for v1.
