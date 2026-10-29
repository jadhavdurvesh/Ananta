"""Candidate physical layouts. {T} = variant table name. Each has: ddl, post (statements run after load, e.g. projections)."""
COLS = """tenant_id UInt32, event_time DateTime64(3,'UTC'), event_id UUID, dataset_id UUID, entity_id UInt64,
 event_type LowCardinality(String), severity UInt8, value Float64, attributes Map(String,String), batch_id UUID"""

VARIANTS = {
 "v0_baseline": {
   "desc": "Plan schema: monthly partition, ORDER BY (tenant,dataset,time,type,entity), default codecs",
   "ddl": f"CREATE TABLE {{T}} ({COLS}) ENGINE=MergeTree PARTITION BY toYYYYMM(event_time) ORDER BY (tenant_id,dataset_id,event_time,event_type,entity_id)",
   "post": []},
 "v1_daily_partition": {
   "desc": "Same key, daily partitions (more parts; better pruning for 1-day windows?)",
   "ddl": f"CREATE TABLE {{T}} ({COLS}) ENGINE=MergeTree PARTITION BY toYYYYMMDD(event_time) ORDER BY (tenant_id,dataset_id,event_time,event_type,entity_id)",
   "post": []},
 "v2_codecs": {
   "desc": "Baseline key + per-column codecs (Delta/ZSTD for time, Gorilla for value, T64 for ids)",
   "ddl": ("CREATE TABLE {T} (tenant_id UInt32 CODEC(T64,ZSTD(1)), event_time DateTime64(3,'UTC') CODEC(DoubleDelta,ZSTD(1)), "
           "event_id UUID CODEC(ZSTD(1)), dataset_id UUID CODEC(ZSTD(1)), entity_id UInt64 CODEC(T64,ZSTD(1)), "
           "event_type LowCardinality(String), severity UInt8 CODEC(T64,ZSTD(1)), value Float64 CODEC(Gorilla,ZSTD(1)), "
           "attributes Map(String,String) CODEC(ZSTD(3)), batch_id UUID CODEC(ZSTD(1))) ENGINE=MergeTree "
           "PARTITION BY toYYYYMM(event_time) ORDER BY (tenant_id,dataset_id,event_time,event_type,entity_id)"),
   "post": []},
 "v3_entity_key": {
   "desc": "ORDER BY (tenant,dataset,entity,time): favours per-entity access, hurts pure time scans?",
   "ddl": f"CREATE TABLE {{T}} ({COLS}) ENGINE=MergeTree PARTITION BY toYYYYMM(event_time) ORDER BY (tenant_id,dataset_id,entity_id,event_time)",
   "post": []},
 "v4_time_bucket_key": {
   "desc": "ORDER BY (tenant,dataset,toStartOfHour(time),event_type,entity,time): coarser time in key",
   "ddl": f"CREATE TABLE {{T}} ({COLS}) ENGINE=MergeTree PARTITION BY toYYYYMM(event_time) ORDER BY (tenant_id,dataset_id,toStartOfHour(event_time),event_type,entity_id,event_time)",
   "post": []},
 "v5_severity_projection": {
   "desc": "Baseline + projection sorted by (tenant,severity,time) for high-severity lookups (query C)",
   "ddl": f"CREATE TABLE {{T}} ({COLS}, PROJECTION p_sev (SELECT * ORDER BY tenant_id, severity, event_time)) ENGINE=MergeTree PARTITION BY toYYYYMM(event_time) ORDER BY (tenant_id,dataset_id,event_time,event_type,entity_id)",
   "post": ["ALTER TABLE {T} MATERIALIZE PROJECTION p_sev SETTINGS mutations_sync=2"]},
 "v6_entity_agg_projection": {
   "desc": "Baseline + aggregating projection per (tenant,hour,entity) for top-entity queries (query D)",
   "ddl": f"CREATE TABLE {{T}} ({COLS}, PROJECTION p_ent (SELECT tenant_id, toStartOfHour(event_time) AS h, entity_id, count() AS n GROUP BY tenant_id, h, entity_id)) ENGINE=MergeTree PARTITION BY toYYYYMM(event_time) ORDER BY (tenant_id,dataset_id,event_time,event_type,entity_id)",
   "post": ["ALTER TABLE {T} MATERIALIZE PROJECTION p_ent SETTINGS mutations_sync=2"]},
}
