use ananta_core::{
    BatchId, CompactionJobId, CompactionResourceLimits, Database, WeaveLayoutProfile,
};
use ananta_types::{Column, DataType, Schema, Value};
use std::error::Error;
use std::fs;
use std::path::PathBuf;
use std::time::Instant;

const TABLE: &str = "chrono_weave_m3";
const BATCHES: usize = 10;
const ROWS_PER_BATCH: usize = 1_000;

fn main() -> Result<(), Box<dyn Error>> {
    let root = std::env::args().nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| std::env::temp_dir().join("ananta-chrono-weave-m3"));
    if root.exists() {
        fs::remove_dir_all(&root)?;
    }
    let mut database = Database::open(&root)?;
    let schema = Schema::new(vec![
        Column::new("event_time", DataType::TimestampMicros, false),
        Column::new("entity", DataType::Utf8, false),
        Column::new("reading", DataType::Float64, false),
        Column::new("sample_id", DataType::Int64, false),
    ])?;
    database.create_table(TABLE, schema)?;

    let ingestion_started = Instant::now();
    let mut capsule_ids = Vec::with_capacity(BATCHES);
    for batch_index in 0..BATCHES {
        let rows = (0..ROWS_PER_BATCH).map(|row_index| {
            let ordinal = batch_index * ROWS_PER_BATCH + row_index;
            vec![
                Value::TimestampMicros(1_700_000_000_000_000_i64 + (ordinal as i64 * 1_000)),
                Value::Utf8(format!("device-{:03}", (ordinal * 17) % 128)),
                Value::Float64((ordinal % 20_000) as f64 / 10.0),
                Value::Int64(ordinal as i64),
            ]
        }).collect::<Vec<_>>();
        let mut batch_bytes = [0_u8; 16];
        batch_bytes[0] = 0x41;
        batch_bytes[8..].copy_from_slice(&((batch_index + 1) as u64).to_be_bytes());
        database.ingest_batch(TABLE, BatchId::from_bytes(batch_bytes)?, &rows)?;
        let capsule_id = (batch_index + 1) as u64;
        database.build_capsule(
            TABLE, capsule_id, 1, capsule_id, capsule_id, Some("event_time"),
        )?;
        capsule_ids.push((capsule_id, 1));
    }
    let ingestion_and_capsule_build_micros =
        u64::try_from(ingestion_started.elapsed().as_micros()).unwrap_or(u64::MAX);

    let limits = CompactionResourceLimits {
        max_input_bytes: 128 * 1024 * 1024,
        max_output_bytes: 128 * 1024 * 1024,
        max_temporary_bytes: 256 * 1024 * 1024,
        max_working_memory_bytes: 512 * 1024 * 1024,
        max_rows: (BATCHES * ROWS_PER_BATCH) as u64,
        max_open_files: 4,
        max_work_unit_rows: 512,
    };

    let mut json_records = Vec::new();
    for (offset, (name, layout)) in [
        ("append_only", WeaveLayoutProfile::AppendOnly),
        ("time_first", WeaveLayoutProfile::TimeFirst),
        ("entity_first", WeaveLayoutProfile::EntityFirst),
    ].into_iter().enumerate() {
        let mut job_bytes = [0_u8; 16];
        job_bytes.fill(0x60 + offset as u8);
        let job_id = CompactionJobId::from_bytes(job_bytes)?;
        let input = database.create_compaction_input_manifest(
            TABLE,
            job_id,
            1,
            1,
            (BATCHES) as u64,
            &capsule_ids,
        )?;
        let weave_id = 900 + offset as u64;
        let report = database.execute_compaction(
            TABLE, &input, weave_id, 1, layout, limits, None,
        )?;
        let independent_verification =
            database.verify_compaction_candidate(TABLE, &input, weave_id, 1)?;
        if independent_verification != report.output_manifest {
            return Err(format!("{name}: candidate verification mismatch").into());
        }
        let mut read_latency_samples = Vec::new();
        let mut read_cpu_samples = Vec::new();
        let mut read_data_bytes = 0_u64;
        for _ in 0..5 {
            let read = database.measure_compaction_candidate_read(TABLE, weave_id, 1)?;
            if read.rows_read != (BATCHES * ROWS_PER_BATCH) as u64 {
                return Err(format!("{name}: incorrect candidate row count").into());
            }
            read_latency_samples.push(read.read_latency_micros.to_string());
            read_cpu_samples.push(read.cpu_time_micros.map_or("null".to_owned(), |v| v.to_string()));
            read_data_bytes = read.data_bytes_read;
        }
        json_records.push(format!(
            "{{\"layout\":\"{name}\",\"rows\":{},\"input_bytes\":{},\"output_bytes\":{},\"temporary_bytes\":{},\"final_storage_bytes\":{},\"temporary_disk_amplification_milli\":{},\"build_wall_micros\":{},\"build_process_cpu_micros\":{},\"warm_read_latency_samples_micros\":[{}],\"warm_read_process_cpu_samples_micros\":[{}],\"read_data_bytes\":{},\"artifact_count\":{}}}",
            report.rows_processed,
            report.input_bytes,
            report.output_bytes,
            report.temporary_bytes,
            report.final_storage_bytes,
            report.temporary_disk_amplification_milli,
            report.elapsed_micros,
            report.cpu_time_micros.map_or("null".to_owned(), |v| v.to_string()),
            read_latency_samples.join(","),
            read_cpu_samples.join(","),
            read_data_bytes,
            report.output_manifest.artifacts.len(),
        ));
    }

    let output = format!(
        "{{\"workload\":\"chronicle_capsule_telemetry_10k\",\"batches\":{},\"rows_per_batch\":{},\"total_rows\":{},\"ingestion_and_capsule_build_micros\":{},\"layouts\":[{}]}}\n",
        BATCHES,
        ROWS_PER_BATCH,
        BATCHES * ROWS_PER_BATCH,
        ingestion_and_capsule_build_micros,
        json_records.join(","),
    );
    fs::write("chrono-weave-m3-results.json", &output)?;
    println!("{output}");
    drop(database);
    fs::remove_dir_all(root)?;
    Ok(())
}
