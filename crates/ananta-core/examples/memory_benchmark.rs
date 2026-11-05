use ananta_core::Database;
use ananta_types::{Column, DataType, Schema, Value};
use std::error::Error;
use std::fs;
use std::path::Path;
use std::time::Instant;

const TABLE: &str = "memory_benchmark";
const GENERATION_CHUNK_ROWS: usize = 10_000;

fn main() -> Result<(), Box<dyn Error>> {
    let mut args = std::env::args().skip(1);
    let command = args.next().ok_or("usage: memory_benchmark generate|scan PATH ...")?;
    let path = args.next().ok_or("missing database path")?;

    match command.as_str() {
        "generate" => {
            let rows = parse_arg(args.next(), "row count")?;
            let payload_bytes = parse_arg(args.next(), "payload bytes")?;
            if rows == 0 || payload_bytes == 0 {
                return Err("row count and payload bytes must be greater than zero".into());
            }
            generate(Path::new(&path), rows, payload_bytes)?;
        }
        "scan" => {
            let batch_rows = parse_arg(args.next(), "batch rows")?;
            scan(Path::new(&path), batch_rows)?;
        }
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

fn generate(path: &Path, rows: usize, payload_bytes: usize) -> Result<(), Box<dyn Error>> {
    if path.exists() {
        fs::remove_dir_all(path)?;
    }
    let mut db = Database::open(path)?;
    let schema = Schema::new(vec![
        Column::new("sequence", DataType::Int64, false),
        Column::new("payload", DataType::Utf8, false),
    ])?;
    db.create_table(TABLE, schema)?;

    let payload = (0..payload_bytes)
        .map(|index| b'a' + ((index * 31 + index / 7) % 26) as u8)
        .collect::<Vec<_>>();
    let payload = String::from_utf8(payload)?;
    let started = Instant::now();
    let mut inserted = 0_usize;
    while inserted < rows {
        let count = GENERATION_CHUNK_ROWS.min(rows - inserted);
        let batch = (0..count)
            .map(|offset| {
                vec![
                    Value::Int64((inserted + offset) as i64),
                    Value::Utf8(payload.clone()),
                ]
            })
            .collect::<Vec<_>>();
        db.insert_batch(TABLE, &batch)?;
        inserted += count;
    }
    println!(
        "generated_rows={rows} payload_bytes={payload_bytes} elapsed_ms={}",
        started.elapsed().as_millis()
    );
    Ok(())
}

fn scan(path: &Path, batch_rows: usize) -> Result<(), Box<dyn Error>> {
    let db = Database::open(path)?;
    let started = Instant::now();
    let mut scanned_rows = 0_usize;
    let mut max_batch_rows = 0_usize;
    let mut max_batch_payload_bytes = 0_usize;

    for result in db.scan_batches(TABLE, batch_rows)? {
        let batch = result?;
        max_batch_rows = max_batch_rows.max(batch.row_count);
        let payload_index = batch.columns.iter()
            .position(|column| column.column.name == "payload")
            .ok_or("payload projection missing")?;
        let decoded_payload_bytes = batch.columns[payload_index].values.iter()
            .map(|value| match value {
                Value::Utf8(text) => text.len(),
                _ => 0,
            })
            .sum::<usize>();
        max_batch_payload_bytes = max_batch_payload_bytes.max(decoded_payload_bytes);
        scanned_rows += batch.row_count;
        drop(batch);
    }

    let elapsed_ms = started.elapsed().as_millis();
    let peak_rss_kib = peak_rss_kib()?;
    println!(
        "scan_rows={scanned_rows} configured_batch_rows={batch_rows} \
         max_batch_rows={max_batch_rows} max_batch_payload_bytes={max_batch_payload_bytes} \
         peak_rss_kib={peak_rss_kib} elapsed_ms={elapsed_ms}"
    );
    Ok(())
}

#[cfg(target_os = "linux")]
fn peak_rss_kib() -> Result<u64, Box<dyn Error>> {
    let status = fs::read_to_string("/proc/self/status")?;
    for line in status.lines() {
        if let Some(value) = line.strip_prefix("VmHWM:") {
            let kib = value.split_whitespace().next().ok_or("invalid VmHWM")?.parse()?;
            return Ok(kib);
        }
    }
    Err("VmHWM was not present in /proc/self/status".into())
}

#[cfg(not(target_os = "linux"))]
fn peak_rss_kib() -> Result<u64, Box<dyn Error>> {
    Err("peak RSS reporting currently requires Linux /proc".into())
}
