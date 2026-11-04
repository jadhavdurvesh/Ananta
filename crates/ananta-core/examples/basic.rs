use ananta_core::Database;
use ananta_types::{Column, DataType, Schema, Value};
use std::error::Error;

fn main() -> Result<(), Box<dyn Error>> {
    let mut database = Database::open("./ananta-data")?;

    if !database.table_names().any(|name| name == "telemetry") {
        let schema = Schema::new(vec![
            Column::new("timestamp", DataType::TimestampMicros, false),
            Column::new("device_id", DataType::Utf8, false),
            Column::new("temperature", DataType::Float64, true),
        ])?;
        database.create_table("telemetry", schema)?;
    }

    let batch = vec![
        vec![
            Value::TimestampMicros(1_793_707_200_000_000),
            Value::Utf8("sensor-01".to_owned()),
            Value::Float64(24.5),
        ],
        vec![
            Value::TimestampMicros(1_793_707_260_000_000),
            Value::Utf8("sensor-02".to_owned()),
            Value::Null,
        ],
    ];

    database.insert_batch("telemetry", &batch)?;
    let rows = database.scan("telemetry")?;
    println!("ANANTA returned {} telemetry rows", rows.len());
    for row in rows {
        println!("{row:?}");
    }

    Ok(())
}
