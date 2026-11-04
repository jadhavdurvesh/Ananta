# Native Engine Architecture and Existing Platform Boundary

## Decision

Add a native Rust database engine as a first-class research track inside ANANTA. Preserve the existing ClickHouse/PostgreSQL services, synthetic workload generator, benchmark runner, layout lab, decision records and optional workload connectors.

The existing platform is the reference and experimental environment. The Rust engine is a separate implementation under development. Neither should be presented as the other.

## Top-level view

```text
ANANTA — research platform
|
+-- Reference systems
|   +-- ClickHouse: analytical/time-series reference workload
|   +-- PostgreSQL: existing metadata/control-plane reference
|
+-- Shared research infrastructure
|   +-- workloads/                 synthetic and optional real workloads
|   +-- services/benchmark-runner  reference benchmark harness
|   +-- services/layout-lab        ClickHouse layout experiments
|   +-- docs/decisions             recorded architectural decisions
|
+-- Native Rust engine (new)
    +-- crates/ananta-types
    +-- crates/ananta-format
    +-- crates/ananta-storage
    +-- crates/ananta-catalog
    +-- crates/ananta-ingest
    +-- crates/ananta-timeseries
    +-- crates/ananta-core
    +-- crates/ananta-query       later
    +-- crates/ananta-execution   later
    +-- crates/ananta-optimizer   later
    +-- crates/ananta-wal         later
    +-- crates/ananta-cli         later
    +-- crates/ananta-bench       later
```

## Rust workspace layout

The native engine should live in a root Cargo workspace using `crates/*`. Keep the existing Python services, SQL schemas, Docker Compose environment and workload directories in place.

Suggested dependency layering:

```text
ananta-types
    ^       ^
    |       |
ananta-format   ananta-catalog
    ^               ^
    |               |
ananta-storage <----+
    ^       ^
    |       |
ananta-ingest  ananta-timeseries
       \       /
        ananta-core
```

The exact crate graph may be adjusted when APIs become concrete, but low-level crates must not depend on high-level crates. Avoid circular dependencies.

## Integration principles

1. The reference environment remains runnable independently of the Rust workspace.
2. Workloads remain pluggable. Engine core must not import from `workloads/`.
3. Fire Bot remains an optional telemetry workload. It must not be a runtime dependency of the database.
4. Existing benchmark and layout-lab decisions remain valid for ClickHouse experiments only. Native ANANTA needs its own engine adapter and benchmark cases before direct comparison.
5. Keep secrets out of committed deployment files. Move credentials to environment variables and document local-only examples in a separate hardening task.
6. Remove generated Python cache artifacts and add ignore rules in repository hygiene work; do not mix that cleanup with database implementation commits.

## Future benchmark adapter

A later benchmark interface should describe a logical workload (schema, deterministic input, query set, expected result) and provide adapters for ClickHouse and native ANANTA. Correctness comparison precedes performance comparison. Engine-specific DDL and tuning remain adapter-owned.

## Deferred decisions

- SQL dialect and parser selection.
- Whether PostgreSQL remains a durable control plane or is replaced by native catalog metadata.
- Network protocol and server mode.
- Multi-writer transactions, MVCC and WAL.
- Compaction, background merges and retention.
- Compression codec defaults.
- Distributed execution and replication.

These decisions require working prototypes and measurements; they are not assumed in this architecture.
