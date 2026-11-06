use ananta_aql::{
    aggregate_batches, bucketed_aggregate, execute_vectorized_source,
    execute_vectorized_source_with_options, explain_plan, latest_per_series,
    analyze_table_and_persist, AggregateKind, AggregateSpec, ParameterBindings, PlannerOptions,
    ResourceLimits,
};
use ananta_core::{BatchId, Database};
use ananta_types::{Column, DataType, Schema, Value};
use std::error::Error;
use std::fs;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

const TABLE: &str = "telemetry";

fn main() -> Result<(), Box<dyn Error>> {
    let root = std::env::temp_dir().join("ananta-m5-analytics-demo");
    if root.exists() { fs::remove_dir_all(&root)?; }
    let mut database = Database::open(&root)?;
    database.create_table(TABLE, Schema::new(vec![
        Column::new("event_time", DataType::TimestampMicros, false),
        Column::new("device", DataType::Utf8, false),
        Column::new("temperature", DataType::Float64, true),
        Column::new("active", DataType::Boolean, true),
    ])?)?;

    let batches = vec![
        vec![
            vec![Value::TimestampMicros(-5), Value::Utf8("sensor-a".into()), Value::Float64(20.0), Value::Boolean(true)],
            vec![Value::TimestampMicros(5), Value::Utf8("sensor-b".into()), Value::Float64(24.0), Value::Boolean(true)],
            vec![Value::TimestampMicros(15), Value::Utf8("sensor-a".into()), Value::Float64(22.0), Value::Boolean(true)],
        ],
        vec![
            vec![Value::TimestampMicros(25), Value::Utf8("sensor-b".into()), Value::Float64(28.0), Value::Boolean(false)],
            vec![Value::TimestampMicros(35), Value::Utf8("sensor-a".into()), Value::Null, Value::Boolean(true)],
            vec![Value::TimestampMicros(45), Value::Utf8("sensor-b".into()), Value::Float64(30.0), Value::Boolean(true)],
        ],
    ];
    for (index, rows) in batches.iter().enumerate() {
        let mut id = [0_u8; 16];
        id[0] = 0xA5;
        id[15] = (index + 1) as u8;
        database.ingest_batch(TABLE, BatchId::from_bytes(id)?, rows)?;
        database.build_capsule(TABLE, (index + 1) as u64, 1, (index + 1) as u64,
            (index + 1) as u64, Some("event_time"))?;
    }
    database.build_navigation_map(TABLE, 1)?;
    let analysis = analyze_table_and_persist(&database, TABLE, 2)?;
    println!(
        "ANALYZE rows={} estimated_batch_payload_bytes={} columns={}",
        analysis.scanned_rows,
        analysis.estimated_batch_payload_bytes,
        analysis.columns.len(),
    );

    let source = "SELECT device, event_time, temperature FROM telemetry \
        WHERE event_time >= :start AND event_time < :end AND temperature >= :minimum LIMIT 20";
    let bindings = ParameterBindings {
        named: [
            ("start".into(), Value::TimestampMicros(-10)),
            ("end".into(), Value::TimestampMicros(50)),
            ("minimum".into(), Value::Float64(21.0)),
        ].into_iter().collect(),
        positional: vec![],
    };
    let options = PlannerOptions {
        navigation_map_generation: Some(1),
        ..PlannerOptions::default()
    };
    let parsed = ananta_aql::parse(source)?;
    let plan = explain_plan(&database, &parsed, &bindings, options)?;
    println!("=== EXPLAIN ===\n{}", plan.render());

    let mut optimized = execute_vectorized_source_with_options(
        &database, source, &bindings, 2, ResourceLimits::default(),
        Arc::new(AtomicBool::new(false)), options,
    )?;
    let result_schema = optimized.schema().to_vec();
    let optimized_batches = optimized.by_ref().collect::<Result<Vec<_>, _>>()?;
    let optimized_profile = optimized.profile();
    let optimized_rows = optimized_batches.iter().flat_map(|batch| batch.rows()).collect::<Vec<_>>();

    let reference_options = PlannerOptions { navigation_map_generation: None, ..options };
    let mut reference = execute_vectorized_source_with_options(
        &database, source, &bindings, 2, ResourceLimits::default(),
        Arc::new(AtomicBool::new(false)), reference_options,
    )?;
    let reference_batches = reference.by_ref().collect::<Result<Vec<_>, _>>()?;
    let reference_profile = reference.profile();
    let reference_output_rows = reference_batches.iter()
        .map(|batch| batch.row_count as u64)
        .sum::<u64>();
    let estimated_output_rows = plan.cost.estimated_output_rows;
    println!("=== COST CALIBRATION ===");
    println!(
        "scan rows: estimated={:?}, reference_measured={}",
        plan.cost.estimated_scan_rows,
        reference_profile.input_rows,
    );
    println!(
        "output rows: estimated={:?}, reference_measured={}",
        estimated_output_rows,
        reference_output_rows,
    );
    println!(
        "projected payload bytes: estimated={:?}, full_scan_decoded_estimate={}",
        plan.cost.estimated_scan_payload_bytes,
        reference_profile.decoded_value_bytes_estimate,
    );
    println!("reference profile: {reference_profile:#?}");
    let mut optimized_keys = optimized_rows.iter().map(|row| format!("{row:?}")).collect::<Vec<_>>();
    let mut reference_keys = reference_batches.iter().flat_map(|batch| batch.rows())
        .map(|row| format!("{row:?}")).collect::<Vec<_>>();
    optimized_keys.sort();
    reference_keys.sort();
    if optimized_keys != reference_keys {
        return Err("optimized Capsule/Navigation Map results differ from the reference scan".into());
    }
    println!("=== OPTIMIZED PROFILE ===\n{optimized_profile:#?}");
    println!("optimized/reference result rows: {}", optimized_rows.len());

    let grouped = aggregate_batches(
        optimized_batches.iter().cloned().map(Ok),
        &result_schema,
        &[0],
        &[
            AggregateSpec::count_all("samples"),
            AggregateSpec::new(AggregateKind::Sum, 2, "temperature_sum"),
            AggregateSpec::new(AggregateKind::Avg, 2, "temperature_average"),
        ],
        &ananta_aql::ExecutionContext::default(),
    )?;
    println!("=== GROUPED AGGREGATES ===");
    for row in grouped.rows() { println!("{row:?}"); }

    let bucketed = bucketed_aggregate(
        optimized_batches.iter().cloned().map(Ok),
        &result_schema,
        1,
        10,
        0,
        &[0],
        &[AggregateSpec::new(AggregateKind::Count, 2, "samples")],
        &ananta_aql::ExecutionContext::default(),
    )?;
    println!("=== TIME BUCKETS ===");
    for row in bucketed.rows() { println!("{row:?}"); }

    let all_query = "SELECT device, event_time, temperature FROM telemetry";
    let mut all = execute_vectorized_source(
        &database, all_query, &ParameterBindings::default(), 2,
        ResourceLimits::default(), Arc::new(AtomicBool::new(false)),
    )?;
    let all_schema = all.schema().to_vec();
    let all_batches = all.by_ref().collect::<Result<Vec<_>, _>>()?;
    let latest = latest_per_series(
        all_batches.iter().cloned().map(Ok), &all_schema, &[0], 1,
        &ananta_aql::ExecutionContext::default(),
    )?;
    println!("=== LATEST PER SERIES ===");
    for row in latest.rows() { println!("{row:?}"); }

    drop(database);
    fs::remove_dir_all(root)?;
    Ok(())
}
