CREATE TABLE tenants (
  tenant_id INTEGER PRIMARY KEY, name TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'active',
  max_concurrent_queries INT NOT NULL DEFAULT 8, max_bytes_per_query BIGINT NOT NULL DEFAULT 10737418240,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now());
CREATE TABLE datasets (
  dataset_id UUID PRIMARY KEY, tenant_id INT NOT NULL REFERENCES tenants, name TEXT NOT NULL,
  schema_json JSONB NOT NULL, retention_days INT, lifecycle TEXT NOT NULL DEFAULT 'active',
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(), UNIQUE (tenant_id, name));
CREATE TABLE dataset_versions (
  version_id UUID PRIMARY KEY, dataset_id UUID NOT NULL REFERENCES datasets, revision INT NOT NULL,
  schema_json JSONB NOT NULL, checksum TEXT, source_manifest JSONB, created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (dataset_id, revision));
CREATE TABLE ingestion_jobs (
  job_id UUID PRIMARY KEY, dataset_id UUID NOT NULL REFERENCES datasets, status TEXT NOT NULL,
  rows_ok BIGINT DEFAULT 0, rows_rejected BIGINT DEFAULT 0, retries INT DEFAULT 0,
  checkpoint JSONB, started_at TIMESTAMPTZ, finished_at TIMESTAMPTZ);
CREATE TABLE saved_queries (
  query_id UUID PRIMARY KEY, tenant_id INT NOT NULL REFERENCES tenants, owner TEXT NOT NULL,
  name TEXT NOT NULL, sql TEXT NOT NULL, params JSONB, viz_config JSONB, created_at TIMESTAMPTZ DEFAULT now());
CREATE TABLE analysis_runs (
  run_id UUID PRIMARY KEY, dataset_id UUID REFERENCES datasets, algorithm TEXT NOT NULL,
  algorithm_version TEXT NOT NULL, params JSONB, input_snapshot JSONB, output_ref TEXT, created_at TIMESTAMPTZ DEFAULT now());
CREATE TABLE query_history (
  id BIGSERIAL PRIMARY KEY, tenant_id INT, fingerprint TEXT NOT NULL, duration_ms INT,
  rows_read BIGINT, bytes_read BIGINT, memory_bytes BIGINT, error TEXT, at TIMESTAMPTZ DEFAULT now());
CREATE INDEX ON query_history (fingerprint, at);
CREATE TABLE audit_log (
  id BIGSERIAL PRIMARY KEY, actor TEXT, action TEXT NOT NULL, target TEXT, allowed BOOLEAN, detail JSONB, at TIMESTAMPTZ DEFAULT now());
INSERT INTO tenants (tenant_id, name) VALUES (42, 'bench'), (1, 'firebot-local');
