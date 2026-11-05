use ananta_core::{BatchId, CapsuleScanRequest, ColumnRangePredicate, Database};
use ananta_types::{Column, DataType, Schema, Value};
use std::error::Error;
use std::fs;
use std::path::Path;
use std::time::Instant;

const TOTAL_BATCHES: usize = 256;
const ROWS_PER_BATCH: usize = 64;
const TABLE: &str = "capsule_benchmark";

fn main() -> Result<(), Box<dyn Error>> {
    let mut args = std::env::args().skip(1);
    let path = args.next().ok_or("usage: capsule_benchmark PATH GROUP_BATCHES PAYLOAD_BYTES")?;
    let group_batches: usize = parse_arg(args.next(), "group batch count")?;
    let payload_bytes: usize = parse_arg(args.next(), "payload bytes")?;
    if group_batches == 0 || TOTAL_BATCHES % group_batches != 0 || payload_bytes == 0 {
        return Err("group size must divide 256 and payload bytes must be positive".into());
    }
    run(Path::new(&path), group_batches, payload_bytes)
}

fn parse_arg<T: std::str::FromStr>(value: Option<String>, name: &str) -> Result<T, Box<dyn Error>>
where
    T::Err: Error + 'static,
{
    Ok(value.ok_or_else(|| format!("missing {name}"))?.parse()?)
}

fn run(path: &Path, group_batches: usize, payload_bytes: usize) -> Result<(), Box<dyn Error>> {
    if path.exists() {
        fs::remove_dir_all(path)?;
    }
    let mut db = Database::open(path)?;
    let schema = Schema::new(vec![
        Column::new("event_time", DataType::TimestampMicros, false),
        Column::new("device", DataType::Utf8, false),
        Column::new("payload", DataType::Utf8, false),
        Column::new("reading", DataType::Float64, false),
    ])?;
    db.create_table(TABLE, schema)?;

    let payload = "p".repeat(payload_bytes);
    let ingest_started = Instant::now();
    for batch_index in 0..TOTAL_BATCHES {
        let rows = (0..ROWS_PER_BATCH).map(|offset| {
            let ordinal = batch_index * ROWS_PER_BATCH + offset;
            vec![
                Value::TimestampMicros(ordinal as i64),
                Value::Utf8(format!("device-{}", (ordinal / 512) % 32)),
                Value::Utf8(payload.clone()),
                Value::Float64((ordinal % 10_000) as f64 / 10.0),
            ]
        }).collect::<Vec<_>>();
        let mut id_bytes = [0_u8; 16];
        id_bytes[..8].copy_from_slice(&0x4341_5053_554c_4501_u64.to_be_bytes());
        id_bytes[8..].copy_from_slice(&((batch_index as u64) + 1).to_be_bytes());
        db.ingest_batch(TABLE, BatchId::from_bytes(id_bytes)?, &rows)?;
    }
    let ingest_us = ingest_started.elapsed().as_micros();

    let capsule_count = TOTAL_BATCHES / group_batches;
    let capsule_build_started = Instant::now();
    let mut built_files = Vec::with_capacity(capsule_count);
    for group_index in 0..capsule_count {
        let first = (group_index * group_batches + 1) as u64;
        let last = ((group_index + 1) * group_batches) as u64;
        let id = (group_index + 1) as u64;
        let capsule_path = db.build_capsule(TABLE, id, 1, first, last, Some("event_time"))?;
        let metadata = db.validate_capsule(TABLE, id, 1)?;
        built_files.push((metadata.row_count, fs::metadata(capsule_path)?.len()));
    }
    let capsule_build_us = capsule_build_started.elapsed().as_micros();

    let map_started = Instant::now();
    db.build_navigation_map(TABLE, 1)?;
    let map_build_us = map_started.elapsed().as_micros();

    let request = CapsuleScanRequest {
        ranges: vec![ColumnRangePredicate::new(
            "event_time",
            Some(Value::TimestampMicros(4_096)),
            true,
            Some(Value::TimestampMicros(8_191)),
            true,
        )],
        projection: Some(vec!["device".into(), "reading".into()]),
    };

    let baseline_started = Instant::now();
    let baseline = db.scan_capsules(TABLE, &request, None)?;
    let baseline_us = baseline_started.elapsed().as_micros();

    let mapped_started = Instant::now();
    let mapped = db.scan_capsules(TABLE, &request, Some(1))?;
    let mapped_us = mapped_started.elapsed().as_micros();

    if baseline.rows != mapped.rows {
        return Err("Navigation Map changed logical query results".into());
    }
    let stored_bytes = built_files.iter().map(|(_, bytes)| *bytes).sum::<u64>();
    let min_rows = built_files.iter().map(|(rows, _)| *rows).min().unwrap_or(0);
    let max_rows = built_files.iter().map(|(rows, _)| *rows).max().unwrap_or(0);
    let min_bytes = built_files.iter().map(|(_, bytes)| *bytes).min().unwrap_or(0);
    let max_bytes = built_files.iter().map(|(_, bytes)| *bytes).max().unwrap_or(0);
    println!(
        "group_batches={group_batches} rows_per_capsule_min={min_rows} rows_per_capsule_max={max_rows} \
         capsule_count={capsule_count} payload_bytes_per_row={payload_bytes} capsule_bytes_total={stored_bytes} \
         capsule_bytes_min={min_bytes} capsule_bytes_max={max_bytes} ingest_total_us={ingest_us} \
         capsule_build_total_us={capsule_build_us} navigation_map_build_us={map_build_us} \
         baseline_scan_us={baseline_us} mapped_scan_us={mapped_us} baseline_rows_examined={} \
         mapped_rows_examined={} capsules_skipped={} capsules_read={} bytes_verified={} \
         baseline_payload_bytes_decoded={} mapped_payload_bytes_decoded={} returned_rows={}",
        baseline.diagnostics.rows_examined,
        mapped.diagnostics.rows_examined,
        mapped.diagnostics.capsules_skipped,
        mapped.diagnostics.capsules_read,
        mapped.diagnostics.bytes_verified,
        baseline.diagnostics.payload_bytes_decoded,
        mapped.diagnostics.payload_bytes_decoded,
        mapped.rows.len(),
    );
    Ok(())
}
