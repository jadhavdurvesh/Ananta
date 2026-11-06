use ananta_aql::{execute_source, ParameterBindings};
use ananta_core::Database;
use ananta_types::{Column, DataType, Schema, Value};
use std::error::Error;
use std::fs;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

fn main() -> Result<(), Box<dyn Error>> {
    let root = std::env::temp_dir().join("ananta-aql-native-example");
    if root.exists() { fs::remove_dir_all(&root)?; }
    let mut database = Database::open(&root)?;
    let schema = Schema::new(vec![
        Column::new("event_time", DataType::TimestampMicros, false),
        Column::new("device", DataType::Utf8, false),
        Column::new("temperature", DataType::Float64, false),
    ])?;
    database.create_table("telemetry", schema)?;
    database.insert_batch("telemetry", &[
        vec![Value::TimestampMicros(100), Value::Utf8("sensor-01".into()), Value::Float64(21.5)],
        vec![Value::TimestampMicros(200), Value::Utf8("sensor-02".into()), Value::Float64(24.0)],
        vec![Value::TimestampMicros(300), Value::Utf8("sensor-03".into()), Value::Float64(18.0)],
    ])?;

    let bindings = ParameterBindings {
        named: [("minimum".into(), Value::Float64(20.0))].into_iter().collect(),
        positional: vec![],
    };
    let cancellation = Arc::new(AtomicBool::new(false));
    let results = execute_source(
        &database,
        "SELECT device, temperature FROM telemetry WHERE temperature >= :minimum LIMIT 10",
        &bindings,
        2,
        Some(cancellation),
    )?;
    for batch in results {
        let batch = batch?;
        for row_index in 0..batch.row_count {
            println!("{:?}", batch.row(row_index).expect("valid row index"));
        }
    }

    drop(database);
    fs::remove_dir_all(root)?;
    Ok(())
}
