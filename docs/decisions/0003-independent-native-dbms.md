# 0003: ANANTA is an independent native DBMS

- **Status:** Accepted direction
- **Date:** 2026-11-04

## Context

The existing ANANTA repository contains a ClickHouse-backed analytical/time-series reference setup, a PostgreSQL metadata/control-plane schema, deterministic benchmark tooling, physical-layout experiments and an optional Fire Bot workload. These provide useful research evidence, but they do not constitute an independently implemented database engine.

The project direction is to build ANANTA as a database system in its own right, with custom database-level storage and query/analytics internals.

## Decision

ANANTA's native product will be an independent database management system implemented in Rust. It will own:
- Database lifecycle and native catalog.
- Database-level storage subsystem and ANANTA Native Format (ANF).
- Ingestion, segment and column-block management.
- Native query language (working name: AQL).
- Query planning, execution and analytics operators.
- Indexing, compaction, caching, WAL and recovery as those capabilities are implemented.

The native engine will not require SQL, PostgreSQL, ClickHouse, SQLite, DuckDB or another database to store or query its data.

The host operating system's filesystem and I/O services remain permitted foundations. This is not a decision to build a new filesystem or block device driver.

## Consequences

- Existing ClickHouse/PostgreSQL services stay as optional external comparison/reference systems.
- Existing benchmark and layout-lab research remains useful but does not implement the native engine.
- Fire Bot remains an optional workload producer and may not become a dependency of engine core.
- Native features must be implemented and tested independently of reference databases.
- External libraries may provide bounded supporting primitives, but ANANTA owns its data model, format contract, language semantics and execution behaviour.
- Research claims must be benchmarked against equivalent workloads and independently verified results.

## First implementation milestone

A single-node, embedded, single-writer prototype with typed schemas, native catalog metadata, ANF segment persistence, batch ingestion and close/reopen scans. SQL/AQL, analytics planning and transactional crash recovery are deliberately not claimed as implemented in this first slice.
