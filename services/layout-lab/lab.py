"""Layout lab: load identical data into each candidate layout, then measure ingestion cost, storage, query latency,
and verify every variant returns IDENTICAL results to the baseline. Applies a transparent rule-based promotion verdict.
  python lab.py --source ananta.events --runs 20 [--only v0_baseline,v3_entity_key]
NOTE: projections change plans, so 'projection used' is recorded from system.query_log.projections when available."""
import argparse, hashlib, json, sys, time, uuid, numpy as np, clickhouse_connect
from datetime import datetime, timezone
from pathlib import Path
sys.path.insert(0, "../benchmark-runner")
from queries import QUERIES
from variants import VARIANTS

THRESH_SPEEDUP = 0.20      # promote only if p95 improves >=20% on at least one query...
MAX_STORAGE_GROWTH = 0.15  # ...storage grows <=15%...
MAX_INGEST_SLOWDOWN = 0.25 # ...ingestion slows <=25%...
                           # ...and no query regresses p95 by >25%, and all results match baseline.

def digest(rows): return hashlib.sha256(repr(rows).encode()).hexdigest()[:16]

def measure(ch, T, runs):
    out = {}
    for name, sql in QUERIES.items():
        q = sql.replace("{T}", T); lat, qids = {"cold": [], "warm": []}, []
        res = ch.query(q, settings={"use_query_cache": 0}).result_rows       # also the correctness sample
        for mode in ("cold", "warm"):
            for _ in range(runs):
                if mode == "cold": ch.command("SYSTEM DROP MARK CACHE"); ch.command("SYSTEM DROP UNCOMPRESSED CACHE")
                qid = str(uuid.uuid4()); t = time.perf_counter()
                ch.query(q, settings={"query_id": qid, "use_query_cache": 0}); lat[mode].append((time.perf_counter() - t) * 1000)
                qids.append(qid)
        ch.command("SYSTEM FLUSH LOGS"); ids = ",".join(f"'{x}'" for x in qids)
        r = ch.query(f"SELECT avg(read_rows), avg(read_bytes) FROM system.query_log WHERE type='QueryFinish' AND query_id IN ({ids})").result_rows[0]
        try: proj = ch.query(f"SELECT arrayStringConcat(arrayDistinct(arrayFlatten(groupArray(projections))),',') FROM system.query_log WHERE type='QueryFinish' AND query_id IN ({ids})").result_rows[0][0]
        except Exception: proj = "n/a"
        out[name] = {"digest": digest(res), "rows_read": r[0], "bytes_read": r[1], "projections": proj,
                     **{f"{m}_p{p}": float(np.percentile(lat[m], p)) for m in lat for p in (50, 95, 99)}}
    return out

def main():
    ap = argparse.ArgumentParser(); ap.add_argument("--source", default="ananta.events"); ap.add_argument("--runs", type=int, default=20)
    ap.add_argument("--host", default="localhost"); ap.add_argument("--only", default=""); ap.add_argument("--keep", action="store_true")
    ap.add_argument("--label", default="layout-lab"); ap.add_argument("--out", default="../../results"); a = ap.parse_args()
    ch = clickhouse_connect.get_client(host=a.host, port=8123, username="ananta", password="ananta_dev")
    names = [n for n in VARIANTS if not a.only or n in a.only.split(",")]
    if "v0_baseline" not in names: names.insert(0, "v0_baseline")
    rows = ch.query(f"SELECT count() FROM {a.source}").result_rows[0][0]
    report = {"label": a.label, "time": datetime.now(timezone.utc).isoformat(), "source_rows": rows,
              "clickhouse": ch.query("SELECT version()").result_rows[0][0], "variants": {}}
    for n in names:
        T = f"ananta_lab.{n}"; v = VARIANTS[n]
        ch.command("CREATE DATABASE IF NOT EXISTS ananta_lab"); ch.command(f"DROP TABLE IF EXISTS {T} SYNC")
        ch.command(v["ddl"].replace("{T}", T))
        t = time.perf_counter()
        ch.command(f"INSERT INTO {T} SELECT tenant_id,event_time,event_id,dataset_id,entity_id,event_type,severity,value,attributes,batch_id FROM {a.source}")
        for s in v["post"]: ch.command(s.replace("{T}", T))
        ingest_s = time.perf_counter() - t
        ch.command(f"OPTIMIZE TABLE {T} FINAL")
        sz = ch.query(f"SELECT sum(data_compressed_bytes), sum(data_uncompressed_bytes), count() FROM system.parts WHERE active AND database='ananta_lab' AND table='{n}'").result_rows[0]
        print(f"[{n}] ingest {ingest_s:.1f}s, {sz[0]/1e6:.1f} MB compressed, {sz[2]} parts")
        report["variants"][n] = {"desc": v["desc"], "ingest_s": ingest_s, "compressed_bytes": sz[0], "uncompressed_bytes": sz[1], "parts": sz[2],
                                 "queries": measure(ch, T, a.runs)}
        if not a.keep and n != "v0_baseline": pass
    base = report["variants"]["v0_baseline"]
    for n, v in report["variants"].items():                                   # rule-based verdicts
        if n == "v0_baseline": v["verdict"] = {"decision": "baseline"}; continue
        correct = all(v["queries"][q]["digest"] == base["queries"][q]["digest"] for q in QUERIES)
        gains = {q: 1 - v["queries"][q]["warm_p95"] / base["queries"][q]["warm_p95"] for q in QUERIES}
        storage = v["compressed_bytes"] / base["compressed_bytes"] - 1; ing = v["ingest_s"] / base["ingest_s"] - 1
        regress = [q for q, g in gains.items() if g < -0.25]
        ok = correct and max(gains.values()) >= THRESH_SPEEDUP and storage <= MAX_STORAGE_GROWTH and ing <= MAX_INGEST_SLOWDOWN and not regress
        reasons = []
        if not correct: reasons.append("RESULTS DIFFER from baseline")
        if max(gains.values()) < THRESH_SPEEDUP: reasons.append("no query improved >=20% p95")
        if storage > MAX_STORAGE_GROWTH: reasons.append(f"storage +{storage:.0%}")
        if ing > MAX_INGEST_SLOWDOWN: reasons.append(f"ingest +{ing:.0%}")
        if regress: reasons.append("regressions: " + ",".join(regress))
        v["verdict"] = {"decision": "PROMOTE-CANDIDATE" if ok else "REJECT", "correct": correct, "p95_gain_by_query": gains,
                        "storage_delta": storage, "ingest_delta": ing, "reasons": reasons}
    out = Path(a.out); out.mkdir(parents=True, exist_ok=True); (out / f"{a.label}.json").write_text(json.dumps(report, indent=2, default=str))
    h = [f"<html><body style='font-family:sans-serif;background:#0b0b1a;color:#ddd'><h1>ANANTA layout lab: {a.label}</h1><p>{rows:,} rows, ClickHouse {report['clickhouse']}</p>"
         "<table border=1 cellpadding=6><tr><th>variant</th><th>verdict</th><th>MB</th><th>ingest s</th>" + "".join(f"<th>{q} warm p95</th>" for q in QUERIES) + "<th>notes</th></tr>"]
    for n, v in report["variants"].items():
        vd = v["verdict"]; notes = "; ".join(vd.get("reasons", [])) or v["desc"]
        h.append(f"<tr><td>{n}</td><td>{vd['decision']}</td><td>{v['compressed_bytes']/1e6:.1f}</td><td>{v['ingest_s']:.1f}</td>" +
                 "".join(f"<td>{v['queries'][q]['warm_p95']:.1f} ms</td>" for q in QUERIES) + f"<td>{notes}</td></tr>")
    h.append("</table><p>Promotion rule: correct results AND >=20% p95 gain on some query AND storage <=+15% AND ingest <=+25% AND no query regresses >25%.</p></body></html>")
    (out / f"{a.label}.html").write_text("".join(h)); print("wrote", out / f"{a.label}.html")

if __name__ == "__main__": main()
