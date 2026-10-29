# Workloads (pluggable)
ANANTA is workload-agnostic. A workload = a ClickHouse DDL + a loader + a set of reference queries.
`synthetic-events/` is the primary benchmark workload (scale tests to 1B rows).
`firebot/` is ONE optional real-world test workload. Nothing in the core (API, Studio, advisor, benchmark runner)
may import from `workloads/`. Add new workloads (logs, IoT, finance ticks, ...) as sibling folders.
