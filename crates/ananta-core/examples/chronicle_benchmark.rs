use ananta_core::{BatchId, Database};
use ananta_types::{Column, DataType, Schema, Value};
use std::error::Error;
use std::fs;
use std::path::Path;
use std::time::Instant;

const TABLE: &str = "chronicle_benchmark";

fn main() -> Result<(), Box<dyn Error>> {
    let mut args = std::env::args().skip(1);
    let command = args.next().ok_or("usage: chronicle_benchmark generate|open PATH ...")?;
    let path = args.next().ok_or("missing database path")?;

    match command.as_str() {
        "generate" => {
            let mode = args.next().ok_or("missing mode (chronicle|legacy)")?;
            let batches = parse_arg(args.next(), "batch count")?;
            let rows_per_batch = parse_arg(args.next(), "rows per batch")?;
            let payload_bytes = parse_arg(args.next(), "payload bytes")?;
            generate(Path::new(&path), &mode, batches, rows_per_batch, payload_bytes)?;
        }
        "open" => open_and_measure(Path::new(&path))?,
        _ => return Err(format!("unknown command: {command}").into()),
    }
    Ok(())
}

fn parse_arg<T: std::str::FromStr>(value: Option<String>, name: &str) -> Result<T, Box<dyn Error>>
where
    T::Err: Error + 'static,
{
    Ok(value.ok_or_else(|| format!("missing {name}"))?.parse()?)
}

fn generate(
    path: &Path,
    mode: &str,
    batch_count: usize,
    rows_per_batch: usize,
    payload_bytes: usize,
) -> Result<(), Box<dyn Error>> {
    if !matches!(mode, "chronicle" | "legacy") {
        return Err("mode must be chronicle or legacy".into());
    }
    if batch_count == 0 || rows_per_batch == 0 || payload_bytes == 0 {
        return Err("batch count, rows per batch and payload bytes must be positive".into());
    }
    if path.exists() {
        fs::remove_dir_all(path)?;
    }

    let mut db = Database::open(path)?;
    let schema = Schema::new(vec![
        Column::new("event_time", DataType::TimestampMicros, false),
        Column::new("source", DataType::Utf8, false),
        Column::new("payload", DataType::Utf8, false),
        Column::new("reading", DataType::Float64, false),
    ])?;
    db.create_table(TABLE, schema)?;

    let payload = "x".repeat(payload_bytes);
    let started = Instant::now();
    let mut inserted_rows = 0_usize;
    for batch_index in 0..batch_count {
        let rows = (0..rows_per_batch)
            .map(|row_index| {
                let ordinal = batch_index * rows_per_batch + row_index;
                vec![
                    Value::TimestampMicros(1_700_000_000_000_000_i64 + ordinal as i64),
                    Value::Utf8(format!("sensor-{}", row_index % 16)),
                    Value::Utf8(payload.clone()),
                    Value::Float64((ordinal % 10_000) as f64 / 10.0),
                ]
            })
            .collect::<Vec<_>>();

        match mode {
            "chronicle" => {
                let mut raw = [0_u8; 16];
                raw[..8].copy_from_slice(&1_u64.to_be_bytes());
                raw[8..].copy_from_slice(&((batch_index as u64) + 1).to_be_bytes());
                let id = BatchId::from_bytes(raw)?;
                db.ingest_batch(TABLE, id, &rows)?;
            }
            "legacy" => {
                db.insert_batch(TABLE, &rows)?;
            }
            _ => unreachable!(),
        }
        inserted_rows += rows.len();
    }
    let elapsed = started.elapsed();
    let directory = path.join("data").join(TABLE);
    let extension = if mode == "chronicle" { "chr" } else { "anf" };
    let files = fs::read_dir(&directory)?
        .filter_map(Result::ok)
        .filter(|entry| entry.path().extension().and_then(|ext| ext.to_str()) == Some(extension))
        .count();
    let bytes = fs::read_dir(&directory)?
        .filter_map(Result::ok)
        .filter(|entry| entry.path().extension().and_then(|ext| ext.to_str()) == Some(extension))
        .map(|entry| entry.metadata().map(|metadata| metadata.len()).unwrap_or(0))
        .sum::<u64>();

    println!(
        "mode={mode} batches={batch_count} rows={inserted_rows} rows_per_batch={rows_per_batch} \
         payload_bytes_per_row={payload_bytes} records={files} stored_bytes={bytes} \
         ingest_total_us={} ingest_us_per_batch={} ingest_us_per_row={}",
        elapsed.as_micros(),
        elapsed.as_micros() / batch_count as u128,
        elapsed.as_micros() / inserted_rows as u128,
    );
    Ok(())
}

fn open_and_measure(path: &Path) -> Result<(), Box<dyn Error>> {
    let started = Instant::now();
    let db = Database::open(path)?;
    let open_elapsed = started.elapsed();
    let accepted_batches = db.ingestion_stats(TABLE)?.accepted_batches;
    let accepted_rows = db.ingestion_stats(TABLE)?.accepted_rows;
    let directory = path.join("data").join(TABLE);
    let chronicle_records = match fs::read_dir(&directory) {
        Ok(entries) => entries
            .filter_map(Result::ok)
            .filter(|entry| entry.path().extension().and_then(|ext| ext.to_str()) == Some("chr"))
            .count(),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => 0,
        Err(error) => return Err(error.into()),
    };
    println!(
        "open_chronicle_records={chronicle_records} rebuilt_accepted_batches={accepted_batches} \
         rebuilt_accepted_rows={accepted_rows} startup_index_rebuild_us={}",
        open_elapsed.as_micros()
    );
    Ok(())
}
