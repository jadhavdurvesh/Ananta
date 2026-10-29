CREATE DATABASE IF NOT EXISTS ananta;

CREATE TABLE IF NOT EXISTS ananta.events
(
    tenant_id  UInt32,
    event_time DateTime64(3, 'UTC'),
    event_id   UUID,
    dataset_id UUID,
    entity_id  UInt64,
    event_type LowCardinality(String),
    severity   UInt8,
    value      Float64,
    attributes Map(String, String),
    batch_id   UUID            -- retry-safe ingestion identity (see docs/decisions/0001)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_time)
ORDER BY (tenant_id, dataset_id, event_time, event_type, entity_id);

-- Fire Bot: first real-world application (privacy-minimized telemetry only)
CREATE TABLE IF NOT EXISTS ananta.firebot_events
(
    event_time  DateTime64(3, 'UTC'),
    event_id    UUID,
    session_id  UUID,
    event_type  LowCardinality(String),
    model_name  LowCardinality(String),
    duration_ms UInt32,
    success     Bool,
    confidence  Nullable(Float32),
    metadata    Map(String, String),
    batch_id    UUID
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_time)
ORDER BY (event_type, event_time, session_id);
