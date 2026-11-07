//! Curated public facade for embedding the ANANTA database engine.
//!
//! Application code should depend on this crate rather than the lower-level
//! storage, format, planner or operator implementation crates. Those crates
//! remain available for engine development but are not the recommended
//! application integration boundary.

/// Version of this curated public embedded API contract.
pub const API_VERSION: &str = ananta_core::EMBEDDED_API_VERSION;

/// Native database lifecycle, storage access and operational reports.
pub mod storage {
    pub use ananta_core::{
        BackupReport, BatchId, CheckpointReport, ConsistencyReport, Database,
        DatabaseLockStatus, DbError, DbErrorCode, IngestOutcome, IngestResult,
        IngestionStats, ObjectId, RecordBatch, ScanBatchIter, StorageFileInfo,
        StorageInventory,
    };
}

/// Logical schema and scalar value types shared across the embedded API.
pub mod types {
    pub use ananta_types::{Column, DataType, Schema, TypeError, Value};
}

/// Bounded execution configuration, cancellation and execution error types.
pub mod execution {
    pub use ananta_execution::{
        CancellationToken, ExecutionConfig, ExecutionContext, ExecutionError,
        ExecutionErrorCode, ExecutionMetrics, ExecutionOptionsV1, VectorBatch,
        VectorSchema,
    };
}

/// AQL parsing, parameter binding, streaming results and query diagnostics.
pub mod aql {
    pub use ananta_aql::{
        execute, execute_source, execute_with_cancellation_token, execute_with_options,
        explain, explain_with_statistics, parse, AqlError, AqlErrorCode,
        AqlExecutionError, AqlExecutionMetrics, AqlPhysicalPlan, AqlResultBatch,
        AqlResultIter, ParameterBindings, Query, AQL_VERSION,
    };
}

/// The most common integration surface is also re-exported at the crate root.
pub use aql::{
    execute_with_cancellation_token, parse, AqlErrorCode, AqlExecutionError,
    AqlResultBatch, AqlResultIter, ParameterBindings, Query,
};
pub use execution::{CancellationToken, ExecutionOptionsV1};
pub use storage::{Database, DbError, DbErrorCode, ObjectId};
pub use types::{Column, DataType, Schema, Value};
