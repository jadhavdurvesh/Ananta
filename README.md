# ANANTA
Workload-agnostic analytics platform. See docs/decisions and workloads/README.md.

## Phase 1 — baseline
```
cd deploy && docker compose up -d
cd ../services/benchmark-runner && pip install -r requirements.txt
python generate.py --rows 1000000 --dist uniform --seed 7
python run.py --label baseline-1M-uniform            # add --table to benchmark any table
```
## Phase 2 — layout lab (storage & query optimization)
```
cd services/layout-lab && pip install -r requirements.txt
python lab.py --label lab-1M-uniform                 # smoke test the harness at 1M
python generate.py ... --rows 10000000 (benchmark-runner)  then: python lab.py --label lab-10M-uniform --runs 20
# repeat with --dist skewed (truncate ananta.events first), then 100M if disk/RAM allow
```
Output: results/<label>.json + .html with per-variant storage, ingest time, p50/p95/p99, correctness check and a PROMOTE/REJECT verdict.
Variants live in services/layout-lab/variants.py — add your own (codecs, keys, projections, partition sizes).
Fire Bot is an optional test workload (workloads/firebot), not a dependency.
