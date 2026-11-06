use ananta_aql::{
    execute_vectorized_source_with_options, explain_plan, analyze_table_and_persist,
    ParameterBindings, PlannerOptions, ResourceLimits,
};
use ananta_core::Database;
use ananta_types::{Column, DataType, Schema, Value};
use std::collections::BTreeMap;
use std::error::Error;
use std::fs;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

const TABLE: &str = "calibration_telemetry";
const ROWS: usize = 2_048;

struct Workload {
    name: &'static str,
    sql: &'static str,
    bindings: ParameterBindings,
}

fn bindings(entries: &[(&str, Value)]) -> ParameterBindings {
    ParameterBindings {
        named: entries.iter()
            .map(|(name, value)| ((*name).to_owned(), value.clone()))
            .collect::<BTreeMap<_, _>>(),
        positional: Vec::new(),
    }
}

fn workloads() -> Vec<Workload> {
    vec![
        Workload {
            name: "narrow_full_scan",
            sql: "SELECT device FROM calibration_telemetry",
            bindings: ParameterBindings::default(),
        },
        Workload {
            name: "wide_full_scan",
            sql: "SELECT event_time, device, temperature, active, payload FROM calibration_telemetry",
            bindings: ParameterBindings::default(),
        },
        Workload {
            name: "equality_filter",
            sql: "SELECT device FROM calibration_telemetry WHERE device = :device",
            bindings: bindings(&[("device", Value::Utf8("device-07".into()))]),
        },
        Workload {
            name: "numeric_range",
            sql: "SELECT device, temperature FROM calibration_telemetry WHERE temperature >= :minimum",
            bindings: bindings(&[("minimum", Value::Float64(42.0))]),
        },
        Workload {
            name: "limit_without_filter",
            sql: "SELECT device FROM calibration_telemetry LIMIT 25",
            bindings: ParameterBindings::default(),
        },
        Workload {
            name: "limit_after_filter",
            sql: "SELECT device FROM calibration_telemetry WHERE active = TRUE LIMIT 25",
            bindings: ParameterBindings::default(),
        },
    ]
}

fn main() -> Result<(), Box<dyn Error>> {
    let nonce = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let root = std::env::temp_dir().join(format!("ananta-cost-calibration-{nonce}"));
    let mut database = Database::open(&root)?;
    database.create_table(TABLE, Schema::new(vec![
        Column::new("event_time", DataType::TimestampMicros, false),
        Column::new("device", DataType::Utf8, false),
        Column::new("temperature", DataType::Float64, true),
        Column::new("active", DataType::Boolean, false),
        Column::new("payload", DataType::Utf8, false),
    ])?)?;

    let rows = (0..ROWS).map(|index| {
        let index = index as i64;
        let temperature = 15.0 + ((index * 17) % 500) as f64 / 10.0;
        let payload = format!("telemetry-sample-{:04}-{}", index, "x".repeat((index as usize % 48) + 8));
        vec![
            Value::TimestampMicros(index * 1_000),
            Value::Utf8(format!("device-{:02}", index.rem_euclid(32))),
            Value::Float64(temperature),
            Value::Boolean(index % 4 != 0),
            Value::Utf8(payload),
        ]
    }).collect::<Vec<_>>();
    database.insert_batch(TABLE, &rows)?;
    let analysis = analyze_table_and_persist(&database, TABLE, 256)?;
    println!(
        "dataset rows={} columns={} decoded_payload_bytes_estimate={}",
        analysis.scanned_rows,
        analysis.columns.len(),
        analysis.estimated_batch_payload_bytes,
    );
    println!(
        "name,estimated_scan_rows,measured_input_rows,estimated_output_rows,measured_output_rows,estimated_scan_payload_bytes,measured_decoded_input_bytes,scan_cost_units,filter_cost_units,projection_cost_units,total_cost_units,output_row_error_pct,elapsed_micros"
    );

    for workload in workloads() {
        let query = ananta_aql::parse(workload.sql)?;
        let options = PlannerOptions::default();
        let plan = explain_plan(&database, &query, &workload.bindings, options)?;
        let mut execution = execute_vectorized_source_with_options(
            &database,
            workload.sql,
            &workload.bindings,
            128,
            ResourceLimits::default(),
            Arc::new(AtomicBool::new(false)),
            options,
        )?;
        let batches = execution.by_ref().collect::<Result<Vec<_>, _>>()?;
        let profile = execution.profile();

        let estimated_output = plan.cost.estimated_output_rows;
        let error_pct = estimated_output.map(|estimated| {
            let error = estimated.abs_diff(profile.output_rows) as f64;
            if profile.output_rows == 0 {
                if estimated == 0 { 0.0 } else { 100.0 }
            } else {
                error * 100.0 / profile.output_rows as f64
            }
        });
        let csv_number = |value: Option<f64>| {
            value.map_or_else(|| "unknown".to_owned(), |value| format!("{value:.3}"))
        };
        println!(
            "{},{},{},{},{},{},{},{},{},{},{},{},{}",
            workload.name,
            plan.cost.estimated_scan_rows.map_or_else(|| "unknown".into(), |v| v.to_string()),
            profile.input_rows,
            estimated_output.map_or_else(|| "unknown".into(), |v| v.to_string()),
            profile.output_rows,
            plan.cost.estimated_scan_payload_bytes.map_or_else(|| "unknown".into(), |v| v.to_string()),
            profile.decoded_value_bytes_estimate,
            csv_number(plan.cost.scan_cost_units),
            csv_number(plan.cost.filter_cost_units),
            csv_number(plan.cost.projection_cost_units),
            csv_number(plan.cost.total_cost_units),
            csv_number(error_pct),
            profile.elapsed_micros,
        );
        // Keep the result batches live through profiling to make sure the full
        // output path is exercised even when the CSV only reports row counts.
        drop(batches);
    }

    drop(database);
    fs::remove_dir_all(root)?;
    Ok(())
}
