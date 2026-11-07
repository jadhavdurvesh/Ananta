//! End-to-end example using only the curated ananta-api facade.
//!
//! Run with: cargo run -p ananta-api --example embedded

use ananta_api::{
    execute_with_v1_options, parse, AqlExecutionOptionsV1, CancellationToken, Column,
    DataType, Database, ExecutionConfig, ExecutionOptionsV1, ParameterBindings, Schema,
    Value, API_VERSION,
};
use std::error::Error;
use std::fs;

fn main() -> Result<(), Box<dyn Error>> {
    let root = std::env::temp_dir().join(format!(
        "ananta-api-facade-example-{}",
        std::process::id(),
    ));
    if root.exists() {
        fs::remove_dir_all(&root)?;
    }

    println!("ANANTA embedded API v{API_VERSION}");
    ExecutionOptionsV1::default().validate()?;

    let mut database = Database::open(&root)?;
    database.create_table(
        "telemetry",
        Schema::new(vec![
            Column::new("event_time", DataType::TimestampMicros, false),
            Column::new("device", DataType::Utf8, false),
            Column::new("temperature", DataType::Float64, false),
        ])?,
    )?;
    database.insert_batch(
        "telemetry",
        &[
            vec![
                Value::TimestampMicros(100),
                Value::Utf8("sensor-01".into()),
                Value::Float64(21.5),
            ],
            vec![
                Value::TimestampMicros(200),
                Value::Utf8("sensor-02".into()),
                Value::Float64(24.0),
            ],
            vec![
                Value::TimestampMicros(300),
                Value::Utf8("sensor-03".into()),
                Value::Float64(18.0),
            ],
        ],
    )?;

    let bindings = ParameterBindings {
        named: [("minimum".into(), Value::Float64(20.0))].into_iter().collect(),
        positional: Vec::new(),
    };
    let query = parse(
        "SELECT device, temperature FROM telemetry WHERE temperature >= :minimum LIMIT 10",
    )?;
    let cancellation = CancellationToken::new();
    let cancel_handle = cancellation.clone();
    let mut query_options = AqlExecutionOptionsV1::default();
    query_options.execution = ExecutionOptionsV1::new(ExecutionConfig {
        max_batch_rows: 2,
        max_batch_cells: 6,
        max_working_set_bytes: 1024 * 1024,
    });
    query_options.cancellation = Some(cancellation);
    let results = execute_with_v1_options(
        &database,
        &query,
        &bindings,
        query_options,
    )
    .map_err(|error| {
        std::io::Error::other(format!("AQL {}: {error}", error.code.as_str()))
    })?;

    for batch in results {
        let batch = batch.map_err(|error| {
            std::io::Error::other(format!("AQL {}: {error}", error.code.as_str()))
        })?;
        for row_index in 0..batch.row_count {
            println!("{:?}", batch.row(row_index).expect("valid batch row"));
        }
    }

    // The caller can cancel from another thread or UI by calling cancel_handle.cancel().
    assert!(!cancel_handle.is_cancelled());
    drop(database);
    fs::remove_dir_all(&root)?;
    Ok(())
}
