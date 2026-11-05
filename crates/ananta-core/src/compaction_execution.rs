use crate::chronicle::sha256;
use crate::compaction::{
    append_compaction_journal_event, CompactionJobJournal, CompactionJobState,
    CompactionProgress, CompactionResourceLimits,
};
use crate::compaction_manifest::{CompactionInputManifest, ManifestError};
use crate::compaction_output_manifest::{
    CompactionOutputManifest, WeaveArtifactReference, WeaveLayoutProfile,
};
use crate::{canonical_typed_row_fingerprint, DbError, Database, Value};
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
use std::time::Instant;

const PVM_MAGIC: &[u8; 4] = b"PVM1";
const PVM_END: &[u8; 4] = b"PEND";
const PVM_VERSION: u16 = 1;
const PVM_HEADER: usize = 40;
const PVM_ENTRY: usize = 48;
const PVM_TRAILER: usize = 36;
const MAX_PVM_BYTES: usize = 64 * 1024 * 1024;
const MAX_ARTIFACT_ROWS: usize = 65_536;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct RowOrigin {
    pub ingestion_sequence: u64,
    pub row_ordinal: u64,
    pub row_fingerprint: [u8; 32],
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactionExecutionReport {
    pub output_manifest: CompactionOutputManifest,
    pub candidate_directory: PathBuf,
    pub input_bytes: u64,
    pub output_bytes: u64,
    pub temporary_bytes: u64,
    pub rows_processed: u64,
    pub elapsed_micros: u64,
    pub cpu_time_micros: Option<u64>,
    pub temporary_disk_amplification_milli: u64,
}

#[derive(Debug, Clone)]
struct ExecutionRow {
    values: Vec<Value>,
    origin: RowOrigin,
}

#[derive(Debug, Clone)]
struct ProvenanceSidecar {
    table_id: u64,
    schema_revision: u64,
    entries: Vec<RowOrigin>,
}

impl ProvenanceSidecar {
    fn encode(&self) -> Result<Vec<u8>, ManifestError> {
        if self.table_id == 0 || self.schema_revision == 0 || self.entries.is_empty() {
            return Err(ManifestError::Invalid("PVM1 identity or entry inventory is empty"));
        }
        let body_len = PVM_HEADER.checked_add(
            self.entries.len().checked_mul(PVM_ENTRY)
                .ok_or(ManifestError::Invalid("PVM1 entry length overflows"))?
        ).ok_or(ManifestError::Invalid("PVM1 body length overflows"))?;
        let total_len = body_len.checked_add(PVM_TRAILER)
            .ok_or(ManifestError::Invalid("PVM1 total length overflows"))?;
        if total_len > MAX_PVM_BYTES {
            return Err(ManifestError::Invalid("PVM1 exceeds 64 MiB"));
        }
        let mut seen = std::collections::BTreeSet::new();
        let mut out = Vec::with_capacity(total_len);
        out.extend_from_slice(PVM_MAGIC);
        out.extend_from_slice(&PVM_VERSION.to_le_bytes());
        out.extend_from_slice(&0_u16.to_le_bytes());
        out.extend_from_slice(&(PVM_HEADER as u32).to_le_bytes());
        out.extend_from_slice(&self.table_id.to_le_bytes());
        out.extend_from_slice(&self.schema_revision.to_le_bytes());
        out.extend_from_slice(&(u64::try_from(self.entries.len())
            .map_err(|_| ManifestError::Invalid("PVM1 entry count exceeds u64"))?).to_le_bytes());
        out.extend_from_slice(&0_u32.to_le_bytes());
        if out.len() != PVM_HEADER {
            return Err(ManifestError::Invalid("PVM1 header layout mismatch"));
        }
        for origin in &self.entries {
            if origin.ingestion_sequence == 0 || origin.row_fingerprint.iter().all(|b| *b == 0)
                || !seen.insert((origin.ingestion_sequence, origin.row_ordinal))
            {
                return Err(ManifestError::Invalid("PVM1 contains a zero or duplicate row origin"));
            }
            out.extend_from_slice(&origin.ingestion_sequence.to_le_bytes());
            out.extend_from_slice(&origin.row_ordinal.to_le_bytes());
            out.extend_from_slice(&origin.row_fingerprint);
        }
        let digest = sha256(&out);
        out.extend_from_slice(&digest);
        out.extend_from_slice(PVM_END);
        Ok(out)
    }

    fn decode(bytes: &[u8]) -> Result<Self, ManifestError> {
        if bytes.len() < PVM_HEADER + PVM_TRAILER || bytes.len() > MAX_PVM_BYTES {
            return Err(ManifestError::Corrupt("PVM1 length is outside valid bounds"));
        }
        let body_len = bytes.len() - PVM_TRAILER;
        if &bytes[body_len + 32..] != PVM_END {
            return Err(ManifestError::Corrupt("PVM1 PEND marker mismatch"));
        }
        if bytes[body_len..body_len + 32] != sha256(&bytes[..body_len])[..] {
            return Err(ManifestError::Corrupt("PVM1 SHA-256 mismatch"));
        }
        let mut p = 0_usize;
        if take::<4>(bytes, &mut p)? != *PVM_MAGIC
            || take_u16(bytes, &mut p)? != PVM_VERSION
            || take_u16(bytes, &mut p)? != 0
            || take_u32(bytes, &mut p)? != PVM_HEADER as u32
        {
            return Err(ManifestError::Corrupt("PVM1 magic, version, flags or header length invalid"));
        }
        let table_id = take_u64(bytes, &mut p)?;
        let schema_revision = take_u64(bytes, &mut p)?;
        let count = usize::try_from(take_u64(bytes, &mut p)?)
            .map_err(|_| ManifestError::Corrupt("PVM1 entry count overflows"))?;
        if take_u32(bytes, &mut p)? != 0 || p != PVM_HEADER || count == 0 {
            return Err(ManifestError::Corrupt("PVM1 reserved header or entry count invalid"));
        }
        let expected_body = PVM_HEADER.checked_add(
            count.checked_mul(PVM_ENTRY)
                .ok_or(ManifestError::Corrupt("PVM1 entry byte count overflows"))?
        ).ok_or(ManifestError::Corrupt("PVM1 body length overflows"))?;
        if expected_body != body_len {
            return Err(ManifestError::Corrupt("PVM1 entry count does not match file length"));
        }
        let mut entries = Vec::with_capacity(count);
        let mut seen = std::collections::BTreeSet::new();
        for _ in 0..count {
            let origin = RowOrigin {
                ingestion_sequence: take_u64(bytes, &mut p)?,
                row_ordinal: take_u64(bytes, &mut p)?,
                row_fingerprint: take::<32>(bytes, &mut p)?,
            };
            if origin.ingestion_sequence == 0 || origin.row_fingerprint.iter().all(|b| *b == 0)
                || !seen.insert((origin.ingestion_sequence, origin.row_ordinal))
            {
                return Err(ManifestError::Corrupt("PVM1 contains a zero or duplicate row origin"));
            }
            entries.push(origin);
        }
        if p != body_len || table_id == 0 || schema_revision == 0 {
            return Err(ManifestError::Corrupt("PVM1 trailing bytes or zero identity"));
        }
        Ok(Self { table_id, schema_revision, entries })
    }
}

#[derive(Debug, Clone)]
struct HeapEntry {
    key: Value,
    origin: RowOrigin,
    run_index: usize,
    row_index: usize,
}
impl PartialEq for HeapEntry {
    fn eq(&self, other: &Self) -> bool {
        compare_value(&self.key, &other.key) == Ordering::Equal && self.origin == other.origin
    }
}
impl Eq for HeapEntry {}
impl PartialOrd for HeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> { Some(self.cmp(other)) }
}
impl Ord for HeapEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        compare_value(&other.key, &self.key)
            .then_with(|| other.origin.cmp(&self.origin))
            .then_with(|| other.run_index.cmp(&self.run_index))
    }
}

impl Database {
    /// Build an immutable candidate Weave using finite resource budgets and bounded
    /// output artifacts. The resulting directory is not the active table generation.
    pub fn execute_compaction(
        &self,
        table: &str,
        input: &CompactionInputManifest,
        weave_id: u64,
        generation: u64,
        layout: WeaveLayoutProfile,
        limits: CompactionResourceLimits,
        cancel: Option<&AtomicBool>,
    ) -> Result<CompactionExecutionReport, DbError> {
        let started = Instant::now();
        limits.validate().map_err(|e| DbError::CompactionExecution(e.to_string()))?;
        input.validate()?;
        if weave_id == 0 || generation == 0 {
            return Err(DbError::CompactionPreflight("candidate WeaveId and generation must be nonzero"));
        }
        if limits.max_open_files < 2 {
            return Err(DbError::CompactionPreflight("compaction requires at least two open-file slots"));
        }

        let preflight = self.preflight_compaction_inputs(table, input)?;
        limits.check(&CompactionProgress {
            input_bytes: preflight.total_source_bytes,
            open_files: 2,
            ..CompactionProgress::default()
        }).map_err(|e| DbError::CompactionExecution(e.to_string()))?;
        let definition = self.catalog.tables.get(table)
            .ok_or_else(|| DbError::TableNotFound(table.to_owned()))?;
        let schema = &definition.schema;
        let column_ids = definition.column_ids.iter().map(|id| id.0).collect::<Vec<_>>();
        let estimated_memory = preflight.total_source_bytes.checked_mul(4)
            .and_then(|n| n.checked_add(input.expected_row_count.checked_mul(160)?))
            .ok_or(DbError::LengthOverflow)?;
        if estimated_memory > limits.max_working_memory_bytes {
            return Err(DbError::CompactionExecution(format!(
                "working memory estimate {estimated_memory} exceeds budget {}",
                limits.max_working_memory_bytes
            )));
        }
        limits.check(&CompactionProgress {
            input_bytes: preflight.total_source_bytes,
            working_memory_bytes: estimated_memory,
            open_files: 2,
            ..CompactionProgress::default()
        }).map_err(|e| DbError::CompactionExecution(e.to_string()))?;

        let data_root = self.root.join("data");
        let table_dir = data_root.join(table);
        let weave_root = table_dir.join("weaves");
        fs::create_dir_all(&weave_root)?;
        require_directory(&data_root)?;
        require_directory(&table_dir)?;
        require_directory(&weave_root)?;

        let job_hex = hex(input.job_id.as_bytes());
        let compaction_root = table_dir.join("compaction");
        fs::create_dir_all(&compaction_root)?;
        require_directory(&compaction_root)?;
        let jobs_root = compaction_root.join("jobs");
        fs::create_dir_all(&jobs_root)?;
        require_directory(&jobs_root)?;
        let jobs_dir = jobs_root.join(&job_hex);
        fs::create_dir_all(&jobs_dir)?;
        require_directory(&jobs_dir)?;
        let journal_path = jobs_dir.join(format!("attempt-{:020}.cje", input.attempt));
        let stage_dir = weave_root.join(format!(".stage-{job_hex}-a{:020}", input.attempt));
        let final_dir = weave_root.join(format!("weave-{weave_id:020}-g{generation:020}"));
        if fs::symlink_metadata(&stage_dir).is_ok() || fs::symlink_metadata(&final_dir).is_ok() {
            return Err(DbError::CompactionPreflight("candidate staging or final generation path already exists"));
        }

        let input_fingerprint = input.fingerprint()?;
        let mut journal = CompactionJobJournal::new(input.job_id, input.attempt, input_fingerprint)
            .map_err(|e| DbError::CompactionExecution(e.to_string()))?;
        let mut progress = CompactionProgress::default();
        append_state(&mut journal, &journal_path, CompactionJobState::Planned, progress)?;
        append_state(&mut journal, &journal_path, CompactionJobState::ValidatingInputs, progress)?;
        if is_cancelled(cancel) {
            return finish_cancelled(&mut journal, &journal_path, progress);
        }
        fs::create_dir(&stage_dir)?;

        let result = (|| -> Result<CompactionExecutionReport, DbError> {
            let rechecked = self.preflight_compaction_inputs(table, input)?;
            if rechecked.input_manifest_sha256 != input_fingerprint
                || rechecked.total_source_bytes != preflight.total_source_bytes
            {
                return Err(DbError::CompactionPreflight("input snapshot changed after planning"));
            }
            append_state(&mut journal, &journal_path, CompactionJobState::Running, progress)?;

            let mut rows = Vec::with_capacity(usize::try_from(input.expected_row_count)
                .map_err(|_| DbError::LengthOverflow)?);
            let mut expected = Vec::with_capacity(rows.capacity());
            for capsule in &input.capsules {
                check_cancel(cancel)?;
                let capsule_rows = self.scan_capsule(table, capsule.capsule_id, capsule.generation)?;
                let mut capsule_index = 0_usize;
                for receipt in input.receipts.iter().filter(|r| {
                    r.sequence >= capsule.first_sequence && r.sequence <= capsule.last_sequence
                }) {
                    let count = usize::try_from(receipt.row_count).map_err(|_| DbError::LengthOverflow)?;
                    for ordinal in 0..count {
                        let row = capsule_rows.get(capsule_index)
                            .ok_or(DbError::CompactionPreflight("Capsule ended before its declared receipts"))?;
                        let fingerprint = canonical_typed_row_fingerprint(
                            schema, definition.id.0, definition.schema_revision, &column_ids, row,
                        )?;
                        let origin = RowOrigin {
                            ingestion_sequence: receipt.sequence,
                            row_ordinal: u64::try_from(ordinal).map_err(|_| DbError::LengthOverflow)?,
                            row_fingerprint: fingerprint,
                        };
                        expected.push(origin);
                        rows.push(ExecutionRow { values: row.clone(), origin });
                        capsule_index = capsule_index.checked_add(1).ok_or(DbError::LengthOverflow)?;
                        progress.rows_processed = progress.rows_processed.checked_add(1).ok_or(DbError::LengthOverflow)?;
                        if progress.rows_processed > limits.max_rows {
                            return Err(DbError::CompactionExecution("row budget exceeded while reading inputs".into()));
                        }
                    }
                }
                if capsule_index != capsule_rows.len() {
                    return Err(DbError::CompactionPreflight("Capsule has rows outside its source receipt interval"));
                }
                progress.working_memory_bytes = estimated_memory;
                progress.current_work_unit_rows = u64::try_from(capsule_index)
                    .map_err(|_| DbError::LengthOverflow)?.min(limits.max_work_unit_rows);
                limits.check(&progress).map_err(|e| DbError::CompactionExecution(e.to_string()))?;
            }
            if rows.len() as u64 != input.expected_row_count {
                return Err(DbError::CompactionPreflight("materialised row count differs from CIM1"));
            }
            expected.sort();

            let key_index = match layout {
                WeaveLayoutProfile::AppendOnly => None,
                WeaveLayoutProfile::TimeFirst => Some(schema.columns().iter()
                    .position(|c| c.data_type == crate::DataType::TimestampMicros)
                    .ok_or(DbError::CompactionPreflight("time-first requires a TIMESTAMP_MICROS column"))?),
                WeaveLayoutProfile::EntityFirst => Some(schema.columns().iter()
                    .position(|c| matches!(c.data_type, crate::DataType::Utf8 | crate::DataType::Int32 | crate::DataType::Int64))
                    .ok_or(DbError::CompactionPreflight("entity-first requires a UTF8 or integer column"))?),
            };
            let work_rows = usize::try_from(limits.max_work_unit_rows)
                .map_err(|_| DbError::LengthOverflow)?
                .min(MAX_ARTIFACT_ROWS)
                .min(crate::MAX_BATCH_CELLS / schema.len())
                .max(1);

            let mut artifact_refs = Vec::new();
            let mut artifact_bytes_total = 0_u64;
            let mut artifact_ordinal = 0_u32;
            let mut emit = |chunk: &[ExecutionRow]| -> Result<(), DbError> {
                if chunk.is_empty() { return Ok(()); }
                check_cancel(cancel)?;
                let values = chunk.iter().map(|row| row.values.clone()).collect::<Vec<_>>();
                let data = crate::encode_segment_v2(schema, &values, definition.id.0, definition.schema_revision, &column_ids)?;
                let pvm = ProvenanceSidecar {
                    table_id: definition.id.0,
                    schema_revision: definition.schema_revision,
                    entries: chunk.iter().map(|row| row.origin).collect(),
                }.encode().map_err(|e| DbError::CompactionExecution(e.to_string()))?;
                let data_path = stage_dir.join(format!("data-{artifact_ordinal:08}.wvf"));
                let pvm_path = stage_dir.join(format!("provenance-{artifact_ordinal:08}.pvm"));
                write_new_synced(&data_path, &data)?;
                write_new_synced(&pvm_path, &pvm)?;
                let data_len = u64::try_from(data.len()).map_err(|_| DbError::LengthOverflow)?;
                let pvm_len = u64::try_from(pvm.len()).map_err(|_| DbError::LengthOverflow)?;
                artifact_bytes_total = artifact_bytes_total.checked_add(data_len)
                    .and_then(|n| n.checked_add(pvm_len)).ok_or(DbError::LengthOverflow)?;
                progress.output_bytes = artifact_bytes_total;
                progress.temporary_bytes = artifact_bytes_total;
                progress.current_work_unit_rows = u64::try_from(chunk.len()).map_err(|_| DbError::LengthOverflow)?;
                progress.open_files = 2;
                limits.check(&progress).map_err(|e| DbError::CompactionExecution(e.to_string()))?;
                artifact_refs.push(WeaveArtifactReference {
                    ordinal: artifact_ordinal,
                    data_file_len: data_len,
                    data_file_sha256: sha256(&data),
                    row_count: u64::try_from(chunk.len()).map_err(|_| DbError::LengthOverflow)?,
                    provenance_file_len: pvm_len,
                    provenance_file_sha256: sha256(&pvm),
                    provenance_entry_count: u64::try_from(chunk.len()).map_err(|_| DbError::LengthOverflow)?,
                });
                artifact_ordinal = artifact_ordinal.checked_add(1).ok_or(DbError::LengthOverflow)?;
                Ok(())
            };

            if let Some(key_index) = key_index {
                let mut runs = Vec::new();
                let mut start = 0_usize;
                while start < rows.len() {
                    check_cancel(cancel)?;
                    let end = start.saturating_add(work_rows).min(rows.len());
                    rows[start..end].sort_by(|a, b| {
                        compare_value(&a.values[key_index], &b.values[key_index])
                            .then_with(|| (a.origin.ingestion_sequence, a.origin.row_ordinal)
                                .cmp(&(b.origin.ingestion_sequence, b.origin.row_ordinal)))
                    });
                    runs.push((start, end));
                    start = end;
                }
                let mut heap = BinaryHeap::new();
                for (run_index, (start, end)) in runs.iter().enumerate() {
                    if start < end {
                        heap.push(HeapEntry {
                            key: rows[*start].values[key_index].clone(),
                            origin: rows[*start].origin,
                            run_index,
                            row_index: *start,
                        });
                    }
                }
                let mut chunk = Vec::with_capacity(work_rows);
                while let Some(entry) = heap.pop() {
                    chunk.push(rows[entry.row_index].clone());
                    if chunk.len() == work_rows {
                        check_cancel(cancel)?;
                        emit(&chunk)?;
                        chunk.clear();
                    }
                    let (_, end) = runs[entry.run_index];
                    let next = entry.row_index + 1;
                    if next < end {
                        heap.push(HeapEntry {
                            key: rows[next].values[key_index].clone(),
                            origin: rows[next].origin,
                            run_index: entry.run_index,
                            row_index: next,
                        });
                    }
                }
                if !chunk.is_empty() { emit(&chunk)?; }
            } else {
                for chunk in rows.chunks(work_rows) {
                    check_cancel(cancel)?;
                    emit(chunk)?;
                }
            }
            drop(emit);
            if artifact_refs.is_empty() {
                return Err(DbError::CompactionPreflight("compaction produced no output artifacts"));
            }

            append_state(&mut journal, &journal_path, CompactionJobState::Verifying, progress)?;
            let output_manifest = CompactionOutputManifest {
                table_id: definition.id.0,
                job_id: input.job_id,
                attempt: input.attempt,
                weave_id,
                generation,
                input_manifest_sha256: input_fingerprint,
                target_schema_revision: definition.schema_revision,
                first_sequence: input.first_sequence,
                last_sequence: input.last_sequence,
                layout,
                expected_row_count: input.expected_row_count,
                artifacts: artifact_refs,
            };
            let manifest_bytes = output_manifest.encode()
                .map_err(|e| DbError::CompactionExecution(e.to_string()))?;
            verify_candidate_directory(&stage_dir, &output_manifest, schema, definition.id.0,
                definition.schema_revision, &column_ids, &expected)?;

            append_state(&mut journal, &journal_path, CompactionJobState::Publishing, progress)?;
            write_new_synced(&stage_dir.join("manifest.owm"), &manifest_bytes)?;
            sync_directory(&stage_dir)?;
            if fs::symlink_metadata(&final_dir).is_ok() {
                return Err(DbError::CompactionPreflight("candidate generation already exists at publication"));
            }
            fs::rename(&stage_dir, &final_dir)?;
            sync_directory(&weave_root)?;
            append_state(&mut journal, &journal_path, CompactionJobState::Completed, progress)?;

            let elapsed_micros = u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX);
            let amplification = if preflight.total_source_bytes == 0 { 0 }
                else { artifact_bytes_total.saturating_mul(1000) / preflight.total_source_bytes };
            Ok(CompactionExecutionReport {
                output_manifest,
                candidate_directory: final_dir.clone(),
                input_bytes: preflight.total_source_bytes,
                output_bytes: artifact_bytes_total,
                temporary_bytes: artifact_bytes_total,
                rows_processed: progress.rows_processed,
                elapsed_micros,
                cpu_time_micros: None,
                temporary_disk_amplification_milli: amplification,
            })
        })();

        if let Err(error) = result {
            if let Some(state) = journal_state(&journal_path) {
                if !state.is_terminal() {
                    if error.to_string().contains("cancelled")
                        && matches!(state, CompactionJobState::ValidatingInputs | CompactionJobState::Running | CompactionJobState::Verifying)
                    {
                        if let Ok(requested) = journal.append(CompactionJobState::CancellationRequested, progress) {
                            if append_compaction_journal_event(&journal_path, &requested).is_ok() {
                                if let Ok(cancelled) = journal.append(CompactionJobState::Cancelled, progress) {
                                    let _ = append_compaction_journal_event(&journal_path, &cancelled);
                                }
                            }
                        }
                    } else if state != CompactionJobState::Publishing || !final_dir.exists() {
                        if let Ok(event) = journal.append(CompactionJobState::Failed, progress) {
                            let _ = append_compaction_journal_event(&journal_path, &event);
                        }
                    }
                }
            }
            return Err(error);
        }
        result
    }

    /// Re-open a candidate by identity and independently verify its OWM1,
    /// data artifacts, provenance sidecars, typed-row hashes and origin set.
    pub fn verify_compaction_candidate(
        &self,
        table: &str,
        input: &CompactionInputManifest,
        weave_id: u64,
        generation: u64,
    ) -> Result<CompactionOutputManifest, DbError> {
        input.validate()?;
        let definition = self.catalog.tables.get(table)
            .ok_or_else(|| DbError::TableNotFound(table.to_owned()))?;
        let input_fingerprint = input.fingerprint()?;
        let weave_root = self.root.join("data").join(table).join("weaves");
        let directory = weave_root.join(format!("weave-{weave_id:020}-g{generation:020}"));
        require_directory(&weave_root)?;
        require_directory(&directory)?;
        let bytes = read_regular_file(&directory.join("manifest.owm"))?;
        let output = CompactionOutputManifest::decode(&bytes)
            .map_err(|e| DbError::CompactionExecution(e.to_string()))?;
        if output.table_id != definition.id.0 || output.job_id != input.job_id
            || output.attempt != input.attempt || output.weave_id != weave_id
            || output.generation != generation || output.input_manifest_sha256 != input_fingerprint
            || output.target_schema_revision != definition.schema_revision
            || output.first_sequence != input.first_sequence || output.last_sequence != input.last_sequence
            || output.expected_row_count != input.expected_row_count
        {
            return Err(DbError::CompactionPreflight("candidate manifest identity differs from pinned attempt"));
        }
        let mut expected = Vec::with_capacity(usize::try_from(input.expected_row_count).map_err(|_| DbError::LengthOverflow)?);
        for capsule in &input.capsules {
            let capsule_rows = self.scan_capsule(table, capsule.capsule_id, capsule.generation)?;
            let mut row_index = 0_usize;
            for receipt in input.receipts.iter().filter(|r| r.sequence >= capsule.first_sequence && r.sequence <= capsule.last_sequence) {
                for ordinal in 0..usize::try_from(receipt.row_count).map_err(|_| DbError::LengthOverflow)? {
                    let row = capsule_rows.get(row_index).ok_or(DbError::CompactionPreflight("Capsule ended during candidate verification"))?;
                    expected.push(RowOrigin {
                        ingestion_sequence: receipt.sequence,
                        row_ordinal: u64::try_from(ordinal).map_err(|_| DbError::LengthOverflow)?,
                        row_fingerprint: canonical_typed_row_fingerprint(
                            &definition.schema, definition.id.0, definition.schema_revision,
                            &definition.column_ids.iter().map(|id| id.0).collect::<Vec<_>>(), row,
                        )?,
                    });
                    row_index += 1;
                }
            }
            if row_index != capsule_rows.len() {
                return Err(DbError::CompactionPreflight("Capsule contains unmanifested rows during candidate verification"));
            }
        }
        expected.sort();
        let column_ids = definition.column_ids.iter().map(|id| id.0).collect::<Vec<_>>();
        let mut actual = Vec::with_capacity(expected.len());
        for artifact in &output.artifacts {
            let data = read_verified_artifact(&directory.join(artifact.data_relative_path()),
                artifact.data_file_len, artifact.data_file_sha256)?;
            let pvm_bytes = read_verified_artifact(&directory.join(artifact.provenance_relative_path()),
                artifact.provenance_file_len, artifact.provenance_file_sha256)?;
            let sidecar = ProvenanceSidecar::decode(&pvm_bytes)
                .map_err(|e| DbError::CompactionExecution(e.to_string()))?;
            if sidecar.table_id != definition.id.0 || sidecar.schema_revision != definition.schema_revision
                || sidecar.entries.len() as u64 != artifact.provenance_entry_count
            {
                return Err(DbError::CompactionPreflight("candidate PVM1 identity/count mismatch"));
            }
            let rows = crate::decode_segment_v2(&data, &definition.schema,
                definition.id.0, definition.schema_revision, &column_ids)?;
            if rows.len() as u64 != artifact.row_count || rows.len() != sidecar.entries.len() {
                return Err(DbError::CompactionPreflight("candidate data/provenance row counts differ"));
            }
            for (row, origin) in rows.iter().zip(&sidecar.entries) {
                if canonical_typed_row_fingerprint(&definition.schema, definition.id.0,
                    definition.schema_revision, &column_ids, row)? != origin.row_fingerprint
                {
                    return Err(DbError::CompactionPreflight("candidate row differs from provenance fingerprint"));
                }
                actual.push(*origin);
            }
        }
        actual.sort();
        if actual != expected {
            return Err(DbError::CompactionPreflight("candidate provenance is not an exact match for input origins"));
        }
        Ok(output)
    }
}

fn append_state(journal: &mut CompactionJobJournal, path: &Path, state: CompactionJobState,
    progress: CompactionProgress) -> Result<(), DbError> {
    let event = journal.append(state, progress)
        .map_err(|e| DbError::CompactionExecution(e.to_string()))?;
    append_compaction_journal_event(path, &event)
        .map_err(|e| DbError::CompactionExecution(e.to_string()))
}
fn finish_cancelled(journal: &mut CompactionJobJournal, path: &Path, progress: CompactionProgress)
    -> Result<CompactionExecutionReport, DbError> {
    append_state(journal, path, CompactionJobState::CancellationRequested, progress)?;
    append_state(journal, path, CompactionJobState::Cancelled, progress)?;
    Err(DbError::CompactionExecution("compaction cancelled before execution".into()))
}
fn journal_state(path: &Path) -> Option<CompactionJobState> {
    crate::compaction::read_compaction_journal(path).ok()?.last().map(|event| event.state)
}
fn check_cancel(cancel: Option<&AtomicBool>) -> Result<(), DbError> {
    if cancel.is_some_and(|flag| flag.load(AtomicOrdering::Acquire)) {
        Err(DbError::CompactionExecution("compaction cancelled at a bounded work-unit boundary".into()))
    } else { Ok(()) }
}
fn compare_value(a: &Value, b: &Value) -> Ordering {
    match (a, b) {
        (Value::Null, Value::Null) => Ordering::Equal,
        (Value::Null, _) => Ordering::Less,
        (_, Value::Null) => Ordering::Greater,
        (Value::Boolean(a), Value::Boolean(b)) => a.cmp(b),
        (Value::Int32(a), Value::Int32(b)) => a.cmp(b),
        (Value::Int64(a), Value::Int64(b)) => a.cmp(b),
        (Value::TimestampMicros(a), Value::TimestampMicros(b)) => a.cmp(b),
        (Value::Float64(a), Value::Float64(b)) => a.total_cmp(b),
        (Value::Utf8(a), Value::Utf8(b)) => a.cmp(b),
        (Value::Binary(a), Value::Binary(b)) => a.cmp(b),
        _ => Ordering::Equal,
    }
}
fn require_directory(path: &Path) -> Result<(), DbError> {
    let metadata = fs::symlink_metadata(path)?;
    if !metadata.file_type().is_dir() {
        return Err(DbError::CompactionPreflight("compaction path is not a real directory"));
    }
    Ok(())
}
fn read_regular_file(path: &Path) -> Result<Vec<u8>, DbError> {
    let metadata = fs::symlink_metadata(path)?;
    if !metadata.file_type().is_file() {
        return Err(DbError::CompactionPreflight("candidate path is not a regular non-symlink file"));
    }
    Ok(fs::read(path)?)
}
fn read_verified_artifact(path: &Path, length: u64, hash: [u8; 32]) -> Result<Vec<u8>, DbError> {
    let bytes = read_regular_file(path)?;
    if bytes.len() as u64 != length || sha256(&bytes) != hash {
        return Err(DbError::CompactionPreflight("candidate artifact length or SHA-256 mismatch"));
    }
    Ok(bytes)
}
fn write_new_synced(path: &Path, bytes: &[u8]) -> Result<(), DbError> {
    let mut file = OpenOptions::new().write(true).create_new(true).open(path)?;
    file.write_all(bytes)?;
    file.flush()?;
    file.sync_all()?;
    Ok(())
}
#[cfg(unix)]
fn sync_directory(path: &Path) -> Result<(), DbError> { File::open(path)?.sync_all()?; Ok(()) }
#[cfg(not(unix))]
fn sync_directory(_path: &Path) -> Result<(), DbError> { Ok(()) }

fn verify_candidate_directory(
    candidate: &Path, manifest: &CompactionOutputManifest, schema: &crate::Schema,
    table_id: u64, schema_revision: u64, column_ids: &[u64], expected: &[RowOrigin],
) -> Result<(), DbError> {
    let mut actual = Vec::with_capacity(expected.len());
    for artifact in &manifest.artifacts {
        let data = read_regular_file(&candidate.join(artifact.data_relative_path()))?;
        let pvm_bytes = read_regular_file(&candidate.join(artifact.provenance_relative_path()))?;
        if data.len() as u64 != artifact.data_file_len || sha256(&data) != artifact.data_file_sha256
            || pvm_bytes.len() as u64 != artifact.provenance_file_len
            || sha256(&pvm_bytes) != artifact.provenance_file_sha256
        {
            return Err(DbError::CompactionPreflight("staged output artifact fingerprint mismatch"));
        }
        let sidecar = ProvenanceSidecar::decode(&pvm_bytes)
            .map_err(|e| DbError::CompactionExecution(e.to_string()))?;
        if sidecar.table_id != table_id || sidecar.schema_revision != schema_revision
            || sidecar.entries.len() as u64 != artifact.provenance_entry_count
        {
            return Err(DbError::CompactionPreflight("staged PVM1 identity/count mismatch"));
        }
        let rows = crate::decode_segment_v2(&data, schema, table_id, schema_revision, column_ids)?;
        if rows.len() as u64 != artifact.row_count || rows.len() != sidecar.entries.len() {
            return Err(DbError::CompactionPreflight("staged data/provenance row counts differ"));
        }
        for (row, origin) in rows.iter().zip(&sidecar.entries) {
            if canonical_typed_row_fingerprint(schema, table_id, schema_revision, column_ids, row)?
                != origin.row_fingerprint
            {
                return Err(DbError::CompactionPreflight("staged row differs from its provenance fingerprint"));
            }
            actual.push(*origin);
        }
    }
    actual.sort();
    if actual != expected {
        return Err(DbError::CompactionPreflight("staged provenance does not exactly match input row origins"));
    }
    Ok(())
}
fn hex(bytes: &[u8]) -> String { bytes.iter().map(|byte| format!("{byte:02x}")).collect() }
fn take<const N: usize>(bytes: &[u8], p: &mut usize) -> Result<[u8; N], ManifestError> {
    let end = (*p).checked_add(N).ok_or(ManifestError::Corrupt("PVM1 field offset overflows"))?;
    let slice = bytes.get(*p..end).ok_or(ManifestError::Corrupt("PVM1 field is truncated"))?;
    let mut out = [0_u8; N];
    out.copy_from_slice(slice);
    *p = end;
    Ok(out)
}
fn take_u16(bytes: &[u8], p: &mut usize) -> Result<u16, ManifestError> { Ok(u16::from_le_bytes(take::<2>(bytes, p)?)) }
fn take_u32(bytes: &[u8], p: &mut usize) -> Result<u32, ManifestError> { Ok(u32::from_le_bytes(take::<4>(bytes, p)?)) }
fn take_u64(bytes: &[u8], p: &mut usize) -> Result<u64, ManifestError> { Ok(u64::from_le_bytes(take::<8>(bytes, p)?)) }

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn pvm1_round_trip_and_duplicate_origin_rejection() {
        let sidecar = ProvenanceSidecar {
            table_id: 4, schema_revision: 2,
            entries: vec![
                RowOrigin { ingestion_sequence: 1, row_ordinal: 0, row_fingerprint: [1; 32] },
                RowOrigin { ingestion_sequence: 1, row_ordinal: 1, row_fingerprint: [2; 32] },
            ],
        };
        let encoded = sidecar.encode().unwrap();
        assert_eq!(ProvenanceSidecar::decode(&encoded).unwrap(), sidecar);
        let mut duplicate = sidecar;
        duplicate.entries[1].row_ordinal = 0;
        assert!(duplicate.encode().is_err());
    }
    #[test]
    fn key_comparison_is_total_and_deterministic() {
        assert_eq!(compare_value(&Value::Float64(f64::NAN), &Value::Float64(f64::NAN)), Ordering::Equal);
        assert_eq!(compare_value(&Value::Int64(-2), &Value::Int64(3)), Ordering::Less);
        assert_eq!(compare_value(&Value::Null, &Value::Utf8("a".into())), Ordering::Less);
    }

    #[test]
    fn all_three_weave_layouts_preserve_rows_and_verify_exact_provenance() {
        use crate::compaction_manifest::{CapsuleSourceReference, ChronicleReceiptReference};
        use crate::{BatchId, Column, DataType, Schema};
        use std::time::{SystemTime, UNIX_EPOCH};

        let nonce = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
        let root = std::env::temp_dir().join(format!("ananta-weave-test-{nonce}"));
        let mut db = Database::open(&root).unwrap();
        let schema = Schema::new(vec![
            Column::new("ts", DataType::TimestampMicros, false),
            Column::new("entity", DataType::Utf8, false),
            Column::new("value", DataType::Int64, false),
        ]).unwrap();
        db.create_table("telemetry", schema).unwrap();

        let batch_one = BatchId::from_bytes([41; 16]).unwrap();
        let batch_two = BatchId::from_bytes([42; 16]).unwrap();
        let rows_one = vec![
            vec![Value::TimestampMicros(300), Value::Utf8("z".into()), Value::Int64(30)],
            vec![Value::TimestampMicros(100), Value::Utf8("b".into()), Value::Int64(10)],
        ];
        let rows_two = vec![
            vec![Value::TimestampMicros(200), Value::Utf8("a".into()), Value::Int64(20)],
            vec![Value::TimestampMicros(400), Value::Utf8("c".into()), Value::Int64(40)],
        ];
        db.ingest_batch("telemetry", batch_one, &rows_one).unwrap();
        db.ingest_batch("telemetry", batch_two, &rows_two).unwrap();
        db.build_capsule("telemetry", 1, 1, 1, 1, None).unwrap();
        db.build_capsule("telemetry", 2, 1, 2, 2, None).unwrap();

        let table_id = db.table_id("telemetry").unwrap();
        let index = db.chronicle_indices.get(&table_id).unwrap();
        let mut receipts = Vec::new();
        for batch_id in [batch_one, batch_two] {
            let receipt = index.by_batch_id.get(&batch_id).unwrap();
            let bytes = fs::read(&receipt.path).unwrap();
            receipts.push(ChronicleReceiptReference {
                sequence: receipt.metadata.sequence,
                batch_id: *receipt.metadata.batch_id.as_bytes(),
                payload_sha256: receipt.metadata.fingerprint,
                row_count: receipt.metadata.row_count,
                record_file_len: bytes.len() as u64,
                record_file_sha256: sha256(&bytes),
            });
        }

        let mut capsules = Vec::new();
        for capsule_id in [1_u64, 2_u64] {
            let metadata = db.validate_capsule("telemetry", capsule_id, 1).unwrap();
            let path = root.join("data").join("telemetry").join("capsules")
                .join(format!("capsule-{capsule_id:020}-g{:020}.cap", 1));
            let bytes = fs::read(path).unwrap();
            capsules.push(CapsuleSourceReference {
                capsule_id,
                generation: 1,
                schema_revision: metadata.schema_revision,
                first_sequence: capsule_id,
                last_sequence: capsule_id,
                row_count: metadata.row_count,
                file_len: bytes.len() as u64,
                file_sha256: sha256(&bytes),
            });
        }

        let limits = CompactionResourceLimits {
            max_input_bytes: 8 * 1024 * 1024,
            max_output_bytes: 8 * 1024 * 1024,
            max_temporary_bytes: 16 * 1024 * 1024,
            max_working_memory_bytes: 64 * 1024 * 1024,
            max_rows: 100,
            max_open_files: 4,
            max_work_unit_rows: 2,
        };

        let mut first_timestamps = Vec::new();
        let mut first_entities = Vec::new();
        for (offset, layout) in [
            WeaveLayoutProfile::AppendOnly,
            WeaveLayoutProfile::TimeFirst,
            WeaveLayoutProfile::EntityFirst,
        ].into_iter().enumerate() {
            let mut job_bytes = [0_u8; 16];
            job_bytes[0] = 60 + offset as u8;
            let input = CompactionInputManifest {
                table_id: table_id.0,
                job_id: crate::CompactionJobId::from_bytes(job_bytes).unwrap(),
                attempt: 1,
                target_schema_revision: 1,
                first_sequence: 1,
                last_sequence: 2,
                expected_row_count: 4,
                capsules: capsules.clone(),
                receipts: receipts.clone(),
            };
            let report = db.execute_compaction(
                "telemetry", &input, 100 + offset as u64, 1, layout, limits, None,
            ).unwrap();
            let verified = db.verify_compaction_candidate(
                "telemetry", &input, 100 + offset as u64, 1,
            ).unwrap();
            assert_eq!(verified, report.output_manifest);
            assert_eq!(verified.layout, layout);
            assert_eq!(verified.expected_row_count, 4);
            assert_eq!(verified.artifacts.iter().map(|a| a.row_count).sum::<u64>(), 4);
            assert_eq!(verified.artifacts.len(), 2);

            let first_artifact = &verified.artifacts[0];
            let data = fs::read(report.candidate_directory.join(first_artifact.data_relative_path())).unwrap();
            let column_ids = db.catalog.tables.get("telemetry").unwrap()
                .column_ids.iter().map(|id| id.0).collect::<Vec<_>>();
            let decoded = crate::decode_segment_v2(&data, &schema, table_id.0, 1, &column_ids).unwrap();
            first_timestamps.push(decoded[0][0].clone());
            first_entities.push(decoded[0][1].clone());
            assert!(report.elapsed_micros > 0);
            assert!(report.temporary_bytes >= report.output_bytes);
        }

        assert_eq!(first_timestamps[0], Value::TimestampMicros(300));
        assert_eq!(first_timestamps[1], Value::TimestampMicros(100));
        assert_eq!(first_entities[2], Value::Utf8("a".into()));

        drop(db);
        fs::remove_dir_all(root).unwrap();
    }


}
