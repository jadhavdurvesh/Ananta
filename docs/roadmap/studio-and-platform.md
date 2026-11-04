# ANANTA Studio and Developer Platform Roadmap

**Status:** Product roadmap; future implementation  
**Dashboard name:** ANANTA Studio

## Principle

ANANTA Studio is a first-party management and analytics interface for the native ANANTA engine. It is not the database engine and does not store application data in a separate PostgreSQL/ClickHouse backend.

It must consume versioned ANANTA management/query APIs. Core database functions remain usable through embedded Rust APIs and CLI without Studio.

## Intended sections

| Area | Capabilities | Dependency |
|---|---|---|
| Overview | Engine state, uptime, resources, query/ingest rates, storage health | Engine telemetry and server |
| Database Explorer | Database/table catalog, schema, types, temporal ranges | Native catalog API |
| AQL Workbench | Editor, parameters, result grid, history, saved queries | AQL parser, executor, query API |
| Visual Explain | Logical/physical plans, estimates, actuals, pruning and column reads | Planner and profiling |
| Storage Explorer | Chronicles, Capsules, Weaves, manifests, segment statistics | ChronoWeave metadata API |
| Analytics Studio | Time-series charts, filters, group-by, time windows and reusable views | Analytics operators and result API |
| Ingestion Monitor | Batch throughput, accepted/rejected rows, errors and backpressure | Ingest metrics |
| Performance Lab | Query profiles, benchmarks and layout experiment reports | Native benchmark/profiling API |
| Administration | Configuration, users/roles, resource policy, backups, restore and audit | Server, auth and reliability features |

A section must remain hidden or explicitly marked as planned until its backend capability exists. Never simulate production engine metrics with placeholder values.

## Architecture

```text
Browser
  |
  v
ANANTA Studio (first-party frontend)
  |
  v
Versioned management/query API
  |
  v
ANANTA Server / session layer (future)
  |
  v
ANANTA Core
  |-- Catalog
  |-- Query/AQL
  |-- Optimizer
  |-- Execution/Analytics
  `-- Storage/ChronoWeave
        |
        v
     ANF files on host filesystem
```

The embedded API and CLI remain available without the web frontend. Studio should not read or modify raw storage files directly.

## Delivery gates

1. Stable embedded database API and catalog.
2. Native read/query operations and AQL.
3. Versioned management API and engine telemetry.
4. Server process, sessions, authentication and resource limits.
5. Studio overview and database explorer.
6. AQL Workbench and Visual Explain.
7. Storage Explorer and Analytics Studio.
8. Ingestion monitor, performance lab and administration.
9. Backup/restore and production security hardening.

## Design requirements

- Clear state distinction: running, stopped, degraded, unavailable.
- Show units and sample windows on metrics.
- Explain query plans with both estimates and measured execution data.
- Treat long-running queries, cancellation and resource limits explicitly.
- Do not expose secrets in query history or logs.
- Role-based access must be enforced by the server, not only hidden frontend controls.
- Studio release version must declare compatible engine/API versions.
- Dashboard availability must never be required for database correctness or recovery.
