# ANANTA

**ANANTA is an independent database research project.** The long-term goal is a database engine implemented in Rust with its own native storage subsystem and file format, catalog, query language, query planner, execution engine and analytical/time-series operators.

It is not intended to be a SQL wrapper or a PostgreSQL/ClickHouse extension. The native engine must operate without requiring another database. Existing ClickHouse and PostgreSQL services are retained as external research references and benchmark targets, not as native runtime dependencies.

## Project tracks

- **Native engine (in development):** Rust workspace, native catalog, ANANTA Native Format (ANF), storage and ingestion foundation. The working name for the future native query language is AQL.
- **Reference research environment (existing):** ClickHouse analytical/time-series experiments, PostgreSQL metadata/control-plane experiments, synthetic workload generator, benchmark runner and physical layout lab.
- **Workloads:** pluggable workload definitions. Fire Bot is an optional telemetry workload and is not a dependency of the database.

## Native engine design documents

- [Native engine technical specification](docs/specifications/native-engine-v0.2.md)
- [Native architecture and independence boundaries](docs/architecture/native-engine.md)
- [Phase 1 implementation plan](docs/roadmap/phase-1-native-engine.md)

The native engine is currently in the design/foundation stage. The documents describe proposed architecture, not already implemented features.

## Existing reference research environment

The commands below operate the existing ClickHouse/PostgreSQL reference stack; they do not run the future native Rust engine.

### Reference benchmark baseline

```bash
cd deploy && docker compose up -d
cd ../services/benchmark-runner && pip install -r requirements.txt
python generate.py --rows 1000000 --dist uniform --seed 7
python run.py --label baseline-1M-uniform
```

### Reference layout lab

```bash
cd services/layout-lab && pip install -r requirements.txt
python lab.py --label lab-1M-uniform
```

The layout lab writes JSON and HTML results with storage, ingestion, latency, correctness and promotion measurements. Its current promotion policy applies to the ClickHouse layouts it tests; native ANANTA will need its own benchmark adapter and results before direct comparison.

See `docs/decisions/` and `workloads/README.md` for existing research decisions and workload conventions.
