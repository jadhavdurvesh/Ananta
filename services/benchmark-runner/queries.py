# Fixed time windows => reproducible. Tie-breaks make results deterministic so variants can be compared for correctness.
# {T} is the table under test.
DS = "toUUID('00000000-0000-0000-0000-000000000001')"
QUERIES = {
 "A_time_range": f"""SELECT event_time, event_type, entity_id, value FROM {{T}}
   WHERE tenant_id=42 AND dataset_id={DS} AND event_time>='2026-10-01 00:00:00' AND event_time<'2026-10-02 00:00:00'
   ORDER BY event_time DESC, entity_id DESC, event_type, value LIMIT 1000""",
 "B_timeseries_agg": """SELECT toStartOfMinute(event_time) AS minute, count() AS n, round(avg(value),6) AS mean_value FROM {T}
   WHERE tenant_id=42 AND event_time>='2026-10-14 23:00:00' AND event_time<'2026-10-15 00:00:00' GROUP BY minute ORDER BY minute""",
 "C_high_severity": """SELECT event_time, entity_id, event_type, severity FROM {T}
   WHERE tenant_id=42 AND severity>=4 AND event_time>='2026-10-14 00:00:00' AND event_time<'2026-10-15 00:00:00'
   ORDER BY event_time DESC, entity_id DESC, event_type LIMIT 100""",
 "D_top_entities": """SELECT entity_id, count() AS n FROM {T}
   WHERE tenant_id=42 AND event_time>='2026-10-14 00:00:00' AND event_time<'2026-10-15 00:00:00'
   GROUP BY entity_id ORDER BY n DESC, entity_id LIMIT 100""",
 "E_wide_agg": """SELECT event_type, count() AS n, round(avg(value),6) AS m, quantile(0.95)(value) AS p95 FROM {T}
   WHERE tenant_id=42 AND event_time>='2026-10-01 00:00:00' AND event_time<'2026-10-15 00:00:00'
   GROUP BY event_type ORDER BY event_type""",
}
# Query "E" of the plan (performance history) lives in Postgres query_history; E_wide_agg here is a heavier scan for layout testing.
