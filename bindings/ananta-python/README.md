# ANANTA Python binding

This is a thin PyO3 binding over ANANTA's existing Rust crates. It does not
reimplement storage, ANF, Chronicle, Capsules or AQL in Python.

## Build locally

Requirements: Rust 1.75+, Python 3.11+, and Maturin.

From this directory:

```bash
python -m pip install maturin
maturin develop --release
python -c "from ananta_native import AnantaDatabase; print('ANANTA binding loaded')"
```

For a wheel:

```bash
maturin build --release
```

## Minimal use

```python
from ananta_native import AnantaDatabase

db = AnantaDatabase("./var/ananta")
db.create_table("telemetry", [
    ("device_id", "UTF8", False),
    ("measured_at", "TIMESTAMP_MICROS", False),
    ("value", "FLOAT64", True),
])
receipt = db.ingest_batch(
    "telemetry",
    bytes.fromhex("00112233445566778899aabbccddeeff"),
    [["sensor-01", 1793966400000000, 42.5]],
)
print(receipt)
print(db.query("SELECT device_id, value FROM telemetry LIMIT 10"))
```

The engine uses an exclusive local writer lock. Keep one long-lived
`AnantaDatabase` instance in one worker process; do not instantiate it per
FastAPI request or run multiple writer processes against the same data path.

The initial binding supports table creation/inspection, idempotent Chronicle
batch ingestion, bounded scans and AQL SELECT execution. The caller must
supply rows in the declared ANANTA logical types. AQL parameter bindings and
the M5-only analytics surface are intentionally not claimed here.
