# 0002: Evidence-based layout promotion
A layout (sort key, partitioning, codec, projection) is only a promote-candidate if, versus the baseline on identical data:
results are byte-identical for every reference query; >=20% warm-p95 gain on at least one query; compressed storage <=+15%;
ingestion time <=+25%; no query regresses >25%. Thresholds are constants in services/layout-lab/lab.py. Run on uniform AND skewed
data at 10M and 100M before any decision is final; 1M results are for debugging the harness only.
