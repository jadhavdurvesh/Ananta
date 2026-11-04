# ANANTA — Product Vision

**Product:** Independent database management system and data platform  
**Organisation:** DMJ  
**Implementation:** Rust-native engine and first-party platform  
**Initial workloads:** Hybrid analytical and time-series

## 1. Product identity

ANANTA is a database system, not only a storage library, a SQL frontend, a dashboard, or a hosted wrapper around somebody else's database.

The intended product is an integrated, independently implemented system:
- Native database engine and runtime.
- Native catalog and schema system.
- ANANTA-owned storage manager and on-disk format.
- Native ingestion and data organisation.
- Native query language (working name: AQL).
- Native query planner and execution engine.
- Native analytical and time-series operators.
- Native indexing, compression, compaction, caching and durability mechanisms as they are implemented.
- First-party dashboard and administration experience, ANANTA Studio.
- CLI, client APIs, SDKs and integration interfaces.

The native engine must create, persist, reopen, query and analyse its databases without requiring PostgreSQL, ClickHouse, SQLite, DuckDB or another database.

## 2. What "our own" means

ANANTA owns the database-level design and behaviour: catalog, object identities, schemas, file format, segments, blocks, read/write paths, query semantics, plans, operators and recovery protocol.

ANANTA still uses the operating system for filesystem and device I/O. It may use well-reviewed libraries for bounded infrastructure primitives such as checksums, compression codecs, networking, cryptography, CLI parsing and UI rendering. Such use does not delegate database semantics or persistence to another database.

SQL is not the defining native interface. AQL is the working name for a native language whose grammar and semantics will be designed around ANANTA's own data model and operators. AQL must not be a superficial keyword-renamed SQL clone. SQL may exist in external comparison adapters or optional import/export tools.

## 3. Product pillars

### Engine
Owns lifecycle, catalog access, concurrency, resource limits, query coordination, error handling and embedded/server execution modes.

### Storage
Owns ANF, segment/capsule lifecycle, catalog persistence, write publication, block layout, buffering, integrity, compaction, indexes, WAL and recovery.

### Query and analytics
Owns AQL, parser, AST, logical and physical plans, vectorised execution, aggregation, temporal operators, optimisation and explainability.

### Studio
ANANTA Studio is the first-party visual control and analysis surface. It exposes real engine capabilities through a versioned management API; it is not a separate database or a required dependency for core storage/query execution.

### Developer platform
CLI, embedded Rust API, client protocol, SDKs, workload adapters, import/export utilities and documentation.

## 4. Research differentiation

ANANTA should not claim uniqueness merely because it has a custom file extension, different query syntax or a different dashboard. Its differentiation should come from measurable storage and execution research.

The first research candidate is **ChronoWeave Storage** (working name):
- **Chronicles:** append-oriented immutable ingestion history, providing a clear source sequence for newly ingested data.
- **Capsules:** self-describing, independently verifiable columnar micro-segments with temporal bounds, schema/version identity and column statistics.
- **Weaves:** compacted physical generations that reorganise capsules around measured workload patterns, such as time-first, entity-first or mixed analytical access, without changing logical table semantics.
- **Navigation Map:** per-capsule/per-column metadata such as min/max, null counts, cardinality summaries and optional probabilistic membership filters, enabling skip-first reads.
- **Adaptive promotion:** propose a new weave only when workload evidence shows a benefit and correctness checks pass. Maintain a safe generation/manifest transition and rollback capability until the new generation is verified.

This is a research hypothesis, not a claim that no existing system uses similar mechanisms. We must survey related database designs and benchmark against baselines before claiming novelty or superiority.

## 5. ANANTA Studio experience

The first-party dashboard should eventually provide:
1. **System Overview:** database status, memory/disk use, active queries, ingestion rates, segment counts and health.
2. **Database Explorer:** databases, tables, schemas, columns, types, partitions/time ranges and catalog metadata.
3. **AQL Workbench:** query editor, parameters, results grid, history and saved queries.
4. **Visual Explain:** logical/physical plan, scanned capsules, skipped blocks, selected columns, estimates and actual timings.
5. **Storage Explorer:** visual view of Chronicles, Capsules, Weaves, manifests, segment statistics, compression and compaction history.
6. **Analytics Studio:** time-series charts, groupings, filters, time windows and reusable analysis views.
7. **Ingestion Monitor:** batch state, accepted/rejected rows, throughput, backpressure and errors.
8. **Performance Lab:** benchmark runs, query profiles, layout experiments and reproducible comparisons.
9. **Administration:** users/roles, resource limits, configuration, backup/restore and audit history when those capabilities exist.

Dashboard wording and metrics must correspond to actual engine telemetry. Do not display features as available before the engine implements them.

## 6. Deployment shape

The first usable native engine is embedded and local. The architecture should preserve a later standalone server mode. ANANTA Studio can connect to a local server process through a documented management API; it should not be coupled directly to internal storage files.

- Embedded library: core database functionality.
- Server: remote protocol, authentication, sessions and resource governance.
- Studio: browser-based administration and analytical interface.
- CLI: local administration and scripting.

## 7. Product principles

- Offline/local-first operation for the embedded engine.
- No mandatory cloud service.
- No mandatory external database.
- Deterministic correctness before performance claims.
- Observable storage and query behaviour.
- Explicit format and language versioning.
- Recoverability and integrity as first-class design constraints.
- Workloads, including Fire Bot, remain pluggable and optional.
- Benchmark every proposed differentiator against the simplest baseline and established reference engines.

## 8. Delivery order

1. Native storage and catalog correctness.
2. Native ingestion and bounded reads.
3. Native AQL and operator execution.
4. Native analytical/time-series capabilities.
5. Native planner, explain and performance instrumentation.
6. Standalone server and management API.
7. ANANTA Studio.
8. Durability, administration, backup/restore and production hardening.
9. Advanced adaptive storage, parallelism and distributed research.

The dashboard is part of the product vision, but it must sit on a working and stable engine API rather than delaying foundational storage work.
