"""Run reference queries; record p50/p95/p99, rows/bytes read; emit JSON + HTML.
  python run.py --runs 30 --label baseline-1M"""
import argparse, json, time, platform, uuid, numpy as np, clickhouse_connect
from datetime import datetime, timezone
from pathlib import Path
from queries import QUERIES

def pct(xs, p): return float(np.percentile(xs, p))

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--runs", type=int, default=30); ap.add_argument("--label", default="run"); ap.add_argument("--host", default="localhost")
    ap.add_argument("--table", default="ananta.events"); ap.add_argument("--out", default="../../results"); a = ap.parse_args()
    ch = clickhouse_connect.get_client(host=a.host, port=8123, username="ananta", password="ananta_dev")
    rows_total = ch.query("SELECT count() FROM " + a.table).result_rows[0][0]
    ver = ch.query("SELECT version()").result_rows[0][0]
    res = {"label": a.label, "time": datetime.now(timezone.utc).isoformat(), "rows": rows_total, "clickhouse": ver,
           "host": platform.platform(), "queries": {}}
    for name, sql in QUERIES.items():
        sql = sql.replace("{T}", a.table)
        for cache in ("cold", "warm"):
            lat, qids = [], []
            for i in range(a.runs):
                if cache == "cold": ch.command("SYSTEM DROP MARK CACHE"); ch.command("SYSTEM DROP UNCOMPRESSED CACHE")
                qid = str(uuid.uuid4()); t = time.perf_counter()
                ch.query(sql, settings={"query_id": qid, "use_query_cache": 0}); lat.append((time.perf_counter() - t) * 1000); qids.append(qid)
            ch.command("SYSTEM FLUSH LOGS")
            ids = ",".join(f"'{q}'" for q in qids)
            r = ch.query(f"SELECT avg(read_rows), avg(read_bytes), avg(memory_usage) FROM system.query_log WHERE type='QueryFinish' AND query_id IN ({ids})").result_rows[0]
            res["queries"][f"{name}:{cache}"] = {"p50_ms": pct(lat,50), "p95_ms": pct(lat,95), "p99_ms": pct(lat,99),
                "rows_read": r[0], "bytes_read": r[1], "memory_bytes": r[2]}
            print(name, cache, f"p95={pct(lat,95):.1f}ms rows_read={r[0]:,.0f}")
    out = Path(a.out); out.mkdir(exist_ok=True, parents=True)
    (out / f"{a.label}.json").write_text(json.dumps(res, indent=2))
    html = ["<html><body style='font-family:sans-serif;background:#0b0b1a;color:#ddd'><h1>ANANTA benchmark: %s</h1><p>%s rows, ClickHouse %s</p><table border=1 cellpadding=6><tr><th>query</th><th>p50</th><th>p95</th><th>p99</th><th>rows read</th><th>bytes read</th></tr>" % (a.label, f"{rows_total:,}", ver)]
    for k, v in res["queries"].items():
        html.append(f"<tr><td>{k}</td><td>{v['p50_ms']:.1f}</td><td>{v['p95_ms']:.1f}</td><td>{v['p99_ms']:.1f}</td><td>{v['rows_read']:,.0f}</td><td>{v['bytes_read']:,.0f}</td></tr>")
    html.append("</table></body></html>"); (out / f"{a.label}.html").write_text("".join(html))

if __name__ == "__main__": main()
