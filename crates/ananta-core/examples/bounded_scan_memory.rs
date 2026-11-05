use ananta_core::Database;
use ananta_types::{Column, DataType, Schema, Value};
use std::error::Error;
use std::io;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

const SMALL_ROWS: usize = 60_000;
const LARGE_ROWS: usize = 300_000;
const SEGMENT_ROWS: usize = 5_000;
const FIXED_BATCH_ROWS: usize = 1_024;
const LARGE_BATCH_ROWS: usize = 8_192;
const PAYLOAD_BYTES: usize = 1_024;
const FIXED_BATCH_RSS_SLACK_KIB: usize = 16 * 1_024;

#[derive(Debug)]
struct ScanResult {
    rows: usize,
    requested_batch_rows: usize,
    max_batch_rows: usize,
    max_batch_cells: usize,
    rss_before_kib: usize,
    rss_peak_kib: usize,
    rss_delta_kib: usize,
    checksum: i128,
}

fn main() -> Result<(), Box<dyn Error>> {
    let mut args = std::env::args().skip(1);
    match args.next().as_deref() {
        Some("scan-child") => {
            let path = PathBuf::from(args.next().ok_or("missing database path")?);
            let batch_rows = args.next().ok_or("missing batch row count")?.parse::<usize>()?;
            if args.next().is_some() {
                return Err("unexpected scan-child argument".into());
            }
            let result = scan_child(&path, batch_rows)?;
            println!(
                "ANANTA_MEMORY_RESULT rows={} requested_batch_rows={} max_batch_rows={} max_batch_cells={} rss_before_kib={} rss_peak_kib={} rss_delta_kib={} checksum={}",
                result.rows,
                result.requested_batch_rows,
                result.max_batch_rows,
                result.max_batch_cells,
                result.rss_before_kib,
                result.rss_peak_kib,
                result.rss_delta_kib,
                result.checksum,
            );
            Ok(())
        }
        None => run_benchmark(),
        Some(other) => Err(format!("unknown command: {other}").into()),
    }
}

fn run_benchmark() -> Result<(), Box<dyn Error>> {
    if peak_rss_kib()?.is_none() {
        return Err("this benchmark requires Linux /proc/self/status VmHWM".into());
    }

    let nonce = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let root = std::env::temp_dir().join(format!(
        "ananta-memory-benchmark-{}-{nonce}",
        std::process::id()
    ));
    std::fs::create_dir_all(&root)?;

    let outcome = (|| -> Result<(), Box<dyn Error>> {
        let small_path = root.join("small");
        let large_path = root.join("large");

        println!(
            "ANANTA_MEMORY_BENCHMARK platform=linux small_rows={SMALL_ROWS} large_rows={LARGE_ROWS} segment_rows={SEGMENT_ROWS} payload_bytes={PAYLOAD_BYTES}"
        );
        generate_database(&small_path, SMALL_ROWS)?;
        generate_database(&large_path, LARGE_ROWS)?;

        let small = run_scan_child(&small_path, FIXED_BATCH_ROWS)?;
        let large_fixed = run_scan_child(&large_path, FIXED_BATCH_ROWS)?;
        let large_wide = run_scan_child(&large_path, LARGE_BATCH_ROWS)?;

        print_result("small_fixed_batch", &small);
        print_result("large_fixed_batch", &large_fixed);
        print_result("large_wide_batch", &large_wide);

        if small.rows != SMALL_ROWS || large_fixed.rows != LARGE_ROWS || large_wide.rows != LARGE_ROWS {
            return Err("scanner returned an unexpected row count".into());
        }
        if small.max_batch_rows > FIXED_BATCH_ROWS
            || large_fixed.max_batch_rows > FIXED_BATCH_ROWS
            || large_wide.max_batch_rows > LARGE_BATCH_ROWS
        {
            return Err("scanner exceeded the configured batch row bound".into());
        }
        if small.max_batch_cells > 1_000_000
            || large_fixed.max_batch_cells > 1_000_000
            || large_wide.max_batch_cells > 1_000_000
        {
            return Err("scanner exceeded the one-million-cell output bound".into());
        }

        let fixed_batch_growth = large_fixed.rss_delta_kib.saturating_sub(small.rss_delta_kib);
        println!(
            "ANANTA_MEMORY_COMPARISON fixed_batch_dataset_growth_rows={} fixed_batch_small_delta_kib={} fixed_batch_large_delta_kib={} fixed_batch_growth_kib={} allowed_growth_kib={}",
            LARGE_ROWS - SMALL_ROWS,
            small.rss_delta_kib,
            large_fixed.rss_delta_kib,
            fixed_batch_growth,
            FIXED_BATCH_RSS_SLACK_KIB,
        );
        if fixed_batch_growth > FIXED_BATCH_RSS_SLACK_KIB {
            return Err(format!(
                "peak RSS grew by {fixed_batch_growth} KiB when result rows increased fivefold at a fixed batch size; allowed noise is {FIXED_BATCH_RSS_SLACK_KIB} KiB"
            ).into());
        }

        println!(
            "ANANTA_MEMORY_PASS result_rows_grew={}x_at_fixed_batch=true batch_size_comparison={}rows_to_{}rows",
            LARGE_ROWS / SMALL_ROWS,
            FIXED_BATCH_ROWS,
            LARGE_BATCH_ROWS,
        );
        Ok(())
    })();

    let _ = std::fs::remove_dir_all(&root);
    outcome
}

fn generate_database(path: &Path, row_count: usize) -> Result<(), Box<dyn Error>> {
    let mut database = Database::open(path)?;
    let schema = Schema::new(vec![
        Column::new("sequence", DataType::Int64, false),
        Column::new("payload", DataType::Utf8, false),
        Column::new("reading", DataType::Float64, false),
    ])?;
    database.create_table("benchmark", schema)?;
    let payload = "x".repeat(PAYLOAD_BYTES);

    for start in (0..row_count).step_by(SEGMENT_ROWS) {
        let end = (start + SEGMENT_ROWS).min(row_count);
        let rows = (start..end)
            .map(|index| {
                vec![
                    Value::Int64(index as i64),
                    Value::Utf8(payload.clone()),
                    Value::Float64((index % 10_000) as f64 / 100.0),
                ]
            })
            .collect::<Vec<_>>();
        database.insert_batch("benchmark", &rows)?;
    }
    drop(database);
    Ok(())
}

fn run_scan_child(path: &Path, batch_rows: usize) -> Result<ScanResult, Box<dyn Error>> {
    let output = Command::new(std::env::current_exe()?)
        .arg("scan-child")
        .arg(path)
        .arg(batch_rows.to_string())
        .output()?;
    if !output.status.success() {
        return Err(format!(
            "scanner subprocess failed: {}",
            String::from_utf8_lossy(&output.stderr)
        )
        .into());
    }

    let stdout = String::from_utf8(output.stdout)?;
    let line = stdout
        .lines()
        .find(|line| line.starts_with("ANANTA_MEMORY_RESULT "))
        .ok_or("scanner subprocess did not return its memory result")?;
    parse_result(line)
}

fn parse_result(line: &str) -> Result<ScanResult, Box<dyn Error>> {
    let mut fields = std::collections::BTreeMap::new();
    for field in line.split_whitespace().skip(1) {
        let (key, value) = field.split_once('=').ok_or("invalid scanner result field")?;
        fields.insert(key, value);
    }
    let number = |key: &str| -> Result<usize, Box<dyn Error>> {
        Ok(fields.get(key).ok_or("missing scanner result field")?.parse()?)
    };
    Ok(ScanResult {
        rows: number("rows")?,
        requested_batch_rows: number("requested_batch_rows")?,
        max_batch_rows: number("max_batch_rows")?,
        max_batch_cells: number("max_batch_cells")?,
        rss_before_kib: number("rss_before_kib")?,
        rss_peak_kib: number("rss_peak_kib")?,
        rss_delta_kib: number("rss_delta_kib")?,
        checksum: fields
            .get("checksum")
            .ok_or("missing checksum")?
            .parse()?,
    })
}

fn scan_child(path: &Path, batch_rows: usize) -> Result<ScanResult, Box<dyn Error>> {
    let database = Database::open(path)?;
    let batches = database.scan_batches("benchmark", batch_rows)?;
    let rss_before_kib = peak_rss_kib()?.ok_or("Linux VmHWM is unavailable")?;
    let mut rows = 0_usize;
    let mut max_batch_rows = 0_usize;
    let mut max_batch_cells = 0_usize;
    let mut checksum = 0_i128;

    for batch in batches {
        let batch = batch?;
        rows = rows.checked_add(batch.row_count).ok_or("row count overflow")?;
        max_batch_rows = max_batch_rows.max(batch.row_count);
        let cells = batch.columns.iter().try_fold(0_usize, |total, column| {
            total.checked_add(column.values.len()).ok_or("batch cell count overflow")
        })?;
        max_batch_cells = max_batch_cells.max(cells);
        let sequence = batch.column(0).ok_or("sequence column missing")?;
        for value in sequence {
            match value {
                Value::Int64(value) => checksum += i128::from(*value),
                _ => return Err("sequence column returned an unexpected type".into()),
            }
        }
        // The batch is dropped here before the next one is requested.
    }
    drop(database);

    let rss_peak_kib = peak_rss_kib()?.ok_or("Linux VmHWM is unavailable")?;
    Ok(ScanResult {
        rows,
        requested_batch_rows: batch_rows,
        max_batch_rows,
        max_batch_cells,
        rss_before_kib,
        rss_peak_kib,
        rss_delta_kib: rss_peak_kib.saturating_sub(rss_before_kib),
        checksum,
    })
}

fn print_result(label: &str, result: &ScanResult) {
    println!(
        "ANANTA_MEMORY_CASE name={label} rows={} requested_batch_rows={} max_batch_rows={} max_batch_cells={} rss_before_kib={} rss_peak_kib={} rss_delta_kib={} checksum={}",
        result.rows,
        result.requested_batch_rows,
        result.max_batch_rows,
        result.max_batch_cells,
        result.rss_before_kib,
        result.rss_peak_kib,
        result.rss_delta_kib,
        result.checksum,
    );
}

fn peak_rss_kib() -> Result<Option<usize>, io::Error> {
    let status = match std::fs::read_to_string("/proc/self/status") {
        Ok(status) => status,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    for line in status.lines() {
        if let Some(value) = line.strip_prefix("VmHWM:") {
            let kib = value.split_whitespace().next().ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "VmHWM has no numeric value")
            })?;
            return Ok(Some(kib.parse().map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidData, "VmHWM is not numeric")
            })?));
        }
    }
    Ok(None)
}
