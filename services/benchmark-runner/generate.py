"""Deterministic synthetic event generator. Usage:
  python generate.py --rows 1000000 --dist skewed --seed 7
Loads into ClickHouse in batches, each with its own batch_id (retry-safe via insert_deduplication_token)."""
import argparse, uuid, numpy as np, clickhouse_connect
from datetime import datetime, timedelta, timezone

DATASET = uuid.UUID("00000000-0000-0000-0000-000000000001")
TYPES = ["login","click","error","metric","heartbeat","purchase","timeout","deploy"]

def make_batch(rng, n, dist, start, span_s, bidx, seed):
    t = start + (rng.random(n) * span_s)
    if dist == "skewed":
        tenant = np.where(rng.random(n) < 0.7, 42, rng.integers(1, 50, n))      # one dominant tenant
        entity = (rng.zipf(1.3, n) % 100_000).astype(np.uint64)                 # hot entities
    else:
        tenant = rng.integers(1, 50, n); entity = rng.integers(0, 100_000, n).astype(np.uint64)
    tenant = np.where(tenant == 0, 1, tenant)
    sev = rng.choice([0,1,2,3,4,5], n, p=[.4,.3,.15,.08,.05,.02])
    val = rng.normal(100, 15, n)
    et = rng.choice(TYPES, n)
    batch_id = uuid.UUID(int=int(np.random.default_rng([seed, bidx]).integers(0, 2**62)))
    rows = []
    for i in range(n):
        ev = uuid.UUID(int=int(rng.integers(0, 2**62)) << 62 | int(rng.integers(0, 2**62)))
        rows.append([int(tenant[i]), datetime.fromtimestamp(float(t[i]), timezone.utc), ev, DATASET,
                     int(entity[i]), str(et[i]), int(sev[i]), float(val[i]), {"src": "synthetic"}, batch_id])
    return rows, batch_id

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=1_000_000); ap.add_argument("--dist", default="uniform", choices=["uniform","skewed"])
    ap.add_argument("--seed", type=int, default=7); ap.add_argument("--batch", type=int, default=100_000)
    ap.add_argument("--host", default="localhost"); ap.add_argument("--days", type=int, default=30)
    a = ap.parse_args()
    ch = clickhouse_connect.get_client(host=a.host, port=8123, username="ananta", password="ananta_dev")
    rng = np.random.default_rng(a.seed)
    end = datetime(2026, 10, 15, tzinfo=timezone.utc); start = (end - timedelta(days=a.days)).timestamp(); span = a.days * 86400
    cols = ["tenant_id","event_time","event_id","dataset_id","entity_id","event_type","severity","value","attributes","batch_id"]
    done, b = 0, 0
    while done < a.rows:
        n = min(a.batch, a.rows - done)
        rows, bid = make_batch(rng, n, a.dist, start, span, b, a.seed)
        ch.insert("ananta.events", rows, column_names=cols, settings={"insert_deduplicate": 1, "insert_deduplication_token": str(bid)})
        done += n; b += 1; print(f"{done:,}/{a.rows:,}")

if __name__ == "__main__": main()
