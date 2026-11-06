use crate::chronicle::sha256;
use crate::compaction_manifest::CompactionInputManifest;
use crate::compaction_output_manifest::CompactionOutputManifest;
use crate::{DbError, Database, TableDefinition, WalArtifactKind, WalMutationIntent};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::UNIX_EPOCH;

const MAGIC: &[u8; 4] = b"AWP1";
const END: &[u8; 4] = b"AEND";
const HEADER: usize = 128;
const TRAILER: usize = 36;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ActiveWeaveGeneration {
    pub weave_id: u64,
    pub generation: u64,
    pub manifest_sha256: [u8; 32],
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActiveWeavePointer {
    pub table_id: u64,
    pub revision: u64,
    pub active: ActiveWeaveGeneration,
    pub previous: Option<ActiveWeaveGeneration>,
}

impl ActiveWeavePointer {
    pub fn encode(&self) -> Result<Vec<u8>, DbError> {
        self.validate()?;
        let mut out = Vec::with_capacity(HEADER + TRAILER);
        out.extend_from_slice(MAGIC);
        out.extend_from_slice(&1_u16.to_le_bytes());
        out.extend_from_slice(&0_u16.to_le_bytes());
        out.extend_from_slice(&(HEADER as u32).to_le_bytes());
        out.extend_from_slice(&self.table_id.to_le_bytes());
        out.extend_from_slice(&self.revision.to_le_bytes());
        out.extend_from_slice(&self.active.weave_id.to_le_bytes());
        out.extend_from_slice(&self.active.generation.to_le_bytes());
        out.extend_from_slice(&self.active.manifest_sha256);
        if let Some(p) = self.previous {
            out.extend_from_slice(&p.weave_id.to_le_bytes());
            out.extend_from_slice(&p.generation.to_le_bytes());
            out.extend_from_slice(&p.manifest_sha256);
        } else {
            out.extend_from_slice(&[0_u8; 48]);
        }
        out.extend_from_slice(&0_u32.to_le_bytes());
        if out.len() != HEADER {
            return Err(DbError::CompactionExecution("AWP1 header layout mismatch".into()));
        }
        let digest = sha256(&out);
        out.extend_from_slice(&digest);
        out.extend_from_slice(END);
        Ok(out)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, DbError> {
        if bytes.len() != HEADER + TRAILER || &bytes[HEADER + 32..] != END
            || bytes[HEADER..HEADER + 32] != sha256(&bytes[..HEADER])[..]
        {
            return Err(DbError::CompactionExecution("AWP1 length, checksum or end marker invalid".into()));
        }
        let mut p = 0;
        if take::<4>(bytes, &mut p)? != *MAGIC || take_u16(bytes, &mut p)? != 1
            || take_u16(bytes, &mut p)? != 0 || take_u32(bytes, &mut p)? as usize != HEADER
        {
            return Err(DbError::CompactionExecution("AWP1 magic, version, flags or header length invalid".into()));
        }
        let table_id = take_u64(bytes, &mut p)?;
        let revision = take_u64(bytes, &mut p)?;
        let active = ActiveWeaveGeneration {
            weave_id: take_u64(bytes, &mut p)?,
            generation: take_u64(bytes, &mut p)?,
            manifest_sha256: take::<32>(bytes, &mut p)?,
        };
        let previous_id = take_u64(bytes, &mut p)?;
        let previous_generation = take_u64(bytes, &mut p)?;
        let previous_hash = take::<32>(bytes, &mut p)?;
        if take_u32(bytes, &mut p)? != 0 || p != HEADER {
            return Err(DbError::CompactionExecution("AWP1 reserved bytes are nonzero".into()));
        }
        let previous = if previous_id == 0 && previous_generation == 0
            && previous_hash.iter().all(|b| *b == 0)
        {
            None
        } else if previous_id != 0 && previous_generation != 0
            && previous_hash.iter().any(|b| *b != 0)
        {
            Some(ActiveWeaveGeneration {
                weave_id: previous_id,
                generation: previous_generation,
                manifest_sha256: previous_hash,
            })
        } else {
            return Err(DbError::CompactionExecution("AWP1 previous identity is partial".into()));
        };
        let pointer = Self { table_id, revision, active, previous };
        pointer.validate()?;
        Ok(pointer)
    }

    fn validate(&self) -> Result<(), DbError> {
        if self.table_id == 0 || self.revision == 0 || self.active.weave_id == 0
            || self.active.generation == 0 || self.active.manifest_sha256.iter().all(|b| *b == 0)
        {
            return Err(DbError::CompactionExecution("AWP1 active identity is invalid".into()));
        }
        if self.previous.is_some_and(|p| p.weave_id == 0 || p.generation == 0
            || p.manifest_sha256.iter().all(|b| *b == 0)
            || (p.weave_id == self.active.weave_id && p.generation == self.active.generation))
        {
            return Err(DbError::CompactionExecution("AWP1 previous identity is invalid".into()));
        }
        Ok(())
    }
}

impl Database {
    pub fn promote_compaction_candidate(
        &mut self, table: &str, input: &CompactionInputManifest, weave_id: u64, generation: u64,
    ) -> Result<ActiveWeavePointer, DbError> {
        self.ensure_operational()?;
        let definition = self.catalog.tables.get(table)
            .ok_or_else(|| DbError::TableNotFound(table.to_owned()))?;
        let index = self.chronicle_indices.get(&definition.id)
            .ok_or(DbError::CompactionPreflight("table Chronicle index is missing"))?;
        if input.first_sequence != 1 || input.last_sequence != index.last_sequence
            || input.receipts.len() as u64 != index.last_sequence
        {
            return Err(DbError::CompactionPreflight("promotion requires complete Chronicle coverage from sequence one"));
        }
        let table_dir = self.root.join("data").join(table);
        for entry in fs::read_dir(&table_dir)? {
            let entry = entry?;
            if entry.file_type()?.is_file()
                && entry.path().extension().and_then(|v| v.to_str()) == Some("anf")
            {
                return Err(DbError::CompactionPreflight("legacy ANF segments are outside the candidate source inventory"));
            }
        }
        self.verify_compaction_candidate(table, input, weave_id, generation)?;
        self.verify_candidate_matches_logical_scan(table, weave_id, generation)?;
        let weave_root = table_dir.join("weaves");
        require_directory(&weave_root)?;
        let candidate = candidate_dir(&weave_root, weave_id, generation);
        let manifest_bytes = read_regular(&candidate.join("manifest.owm"))?;
        let existing = read_pointer(&weave_root, definition.id.0)?;
        let revision = existing.as_ref().map_or(Ok(1), |p| p.revision.checked_add(1).ok_or(DbError::LengthOverflow))?;
        let next = ActiveWeavePointer {
            table_id: definition.id.0,
            revision,
            active: ActiveWeaveGeneration { weave_id, generation, manifest_sha256: sha256(&manifest_bytes) },
            previous: existing.map(|p| p.active),
        };
        self.publish_active_pointer_wal(&weave_root, &next, b"promote_active_weave")?;
        Ok(next)
    }

    pub fn rollback_active_weave(&mut self, table: &str) -> Result<ActiveWeavePointer, DbError> {
        self.ensure_operational()?;
        let definition = self.catalog.tables.get(table)
            .ok_or_else(|| DbError::TableNotFound(table.to_owned()))?;
        let root = self.root.join("data").join(table).join("weaves");
        let current = read_pointer(&root, definition.id.0)?
            .ok_or(DbError::CompactionPreflight("there is no active Weave"))?;
        let previous = current.previous.ok_or(DbError::CompactionPreflight("there is no retained previous generation"))?;
        self.verify_generation(table, previous)?;
        let next = ActiveWeavePointer {
            table_id: current.table_id,
            revision: current.revision.checked_add(1).ok_or(DbError::LengthOverflow)?,
            active: previous,
            previous: Some(current.active),
        };
        self.publish_active_pointer_wal(&root, &next, b"rollback_active_weave")?;
        Ok(next)
    }

    fn publish_active_pointer_wal(
        &mut self,
        weave_root: &Path,
        pointer: &ActiveWeavePointer,
        operation: &[u8],
    ) -> Result<(), DbError> {
        require_directory(weave_root)?;
        let bytes = pointer.encode()?;
        let schema_revision = self.catalog.tables.values()
            .find(|definition| definition.id.0 == pointer.table_id)
            .map(|definition| definition.schema_revision)
            .ok_or(DbError::CompactionPreflight("active pointer TableId is not in the catalog"))?;
        let intent = WalMutationIntent {
            artifact_kind: WalArtifactKind::ActiveWeavePointer,
            table_id: pointer.table_id,
            schema_revision,
            file_id: pointer.revision,
            file_len: u64::try_from(bytes.len()).map_err(|_| DbError::LengthOverflow)?,
            sha256: sha256(&bytes),
        };
        let final_path = weave_root.join(intent.artifact_kind.final_file_name(intent.file_id));
        match fs::symlink_metadata(&final_path) {
            Ok(metadata) if metadata.file_type().is_file() && metadata.len() == intent.file_len => {
                if fs::read(&final_path)? != bytes {
                    return Err(DbError::CompactionExecution("immutable active pointer snapshot conflict".into()));
                }
            }
            Ok(_) => return Err(DbError::CompactionExecution("immutable active pointer snapshot conflict".into())),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(DbError::Io(error)),
        }
        let staging = weave_root.join(intent.artifact_kind.staging_file_name(intent.file_id));
        if fs::symlink_metadata(&staging).is_ok() {
            return Err(DbError::CompactionExecution("active pointer staging path already exists".into()));
        }
        crate::stage_wal_artifact(weave_root, &intent, &bytes)
            .map_err(|error| DbError::CompactionExecution(error.to_string()))?;
        self.commit_wal_intent(&intent, operation)?;
        let versioned = self.publish_wal_intent(weave_root, &intent)?;
        crate::wal_recovery::maybe_crash_after(
            crate::wal_recovery::WalRecoveryCrashPoint::AfterActiveWeavePointerVersionedPublished,
        );
        publish_pointer(weave_root, pointer)?;
        crate::wal_recovery::maybe_crash_after(
            crate::wal_recovery::WalRecoveryCrashPoint::AfterActiveWeaveCanonicalPublished,
        );
        let _ = versioned;
        Ok(())
    }

    pub fn active_weave_pointer(&self, table: &str) -> Result<Option<ActiveWeavePointer>, DbError> {
        let definition = self.catalog.tables.get(table)
            .ok_or_else(|| DbError::TableNotFound(table.to_owned()))?;
        read_pointer(&self.root.join("data").join(table).join("weaves"), definition.id.0)
    }

    pub(crate) fn install_recovered_pointer(
        root: &Path,
        pointer: &ActiveWeavePointer,
    ) -> Result<(), DbError> {
        require_directory(root)?;
        let stale_temp = root.join(format!(".active-{:020}.tmp", pointer.revision));
        match fs::symlink_metadata(&stale_temp) {
            Ok(metadata) if metadata.file_type().is_file() => {
                fs::remove_file(&stale_temp)?;
                sync_directory(root)?;
            }
            Ok(_) => return Err(DbError::CompactionExecution(
                "active pointer replacement temp is not a regular file".into(),
            )),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(DbError::Io(error)),
        }
        match read_pointer(root, pointer.table_id) {
            Ok(Some(current)) if current.revision > pointer.revision => Ok(()),
            Ok(Some(current)) if current.revision == pointer.revision => {
                if current == *pointer {
                    // A crash can occur after rename and before the directory
                    // fsync. Re-sync even when the canonical bytes are present.
                    sync_directory(root)
                } else {
                    Err(DbError::CompactionExecution(
                        "active pointer revision conflicts with committed WAL snapshot".into(),
                    ))
                }
            }
            Ok(Some(current)) => {
                if current.revision.checked_add(1) != Some(pointer.revision) {
                    return Err(DbError::CompactionExecution(
                        "active pointer WAL revisions are not contiguous".into(),
                    ));
                }
                publish_pointer(root, pointer)
            }
            Ok(None) => {
                if pointer.revision != 1 {
                    return Err(DbError::CompactionExecution(
                        "active pointer WAL history does not begin at revision one".into(),
                    ));
                }
                publish_pointer(root, pointer)
            }
            Err(error) => Err(error),
        }
    }

    pub(crate) fn cleanup_orphan_active_pointer_staging(
        root: &Path,
        catalog: &std::collections::BTreeMap<String, TableDefinition>,
    ) -> Result<(), DbError> {
        for table in catalog.keys() {
            let weave_root = root.join("data").join(table).join("weaves");
            let entries = match fs::read_dir(&weave_root) {
                Ok(entries) => entries,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                Err(error) => return Err(DbError::Io(error)),
            };
            let mut changed = false;
            for entry in entries {
                let entry = entry?;
                if !entry.file_type()?.is_file() {
                    continue;
                }
                let name = entry.file_name();
                let name = name.to_string_lossy();
                let canonical = |prefix: &str, suffix: &str| -> bool {
                    let Some(number) = name.strip_prefix(prefix).and_then(|value| value.strip_suffix(suffix)) else {
                        return false;
                    };
                    number.len() == 20
                        && number.bytes().all(|byte| byte.is_ascii_digit())
                        && number.parse::<u64>().is_ok_and(|id| id != 0 && format!("{id:020}") == number)
                };
                if canonical(".pointer-", ".tmp") || canonical(".active-", ".tmp") {
                    fs::remove_file(entry.path())?;
                    changed = true;
                }
            }
            if changed {
                sync_directory(&weave_root)?;
            }
        }
        Ok(())
    }

    pub(crate) fn recover_active_weaves(&mut self) -> Result<(), DbError> {
        for (table, definition) in &self.catalog.tables {
            let root = self.root.join("data").join(table).join("weaves");
            let Some(pointer) = read_pointer(&root, definition.id.0)? else { continue; };
            if self.verify_generation(table, pointer.active).is_ok() { continue; }
            let previous = pointer.previous.ok_or(DbError::CompactionPreflight(
                "active Weave is invalid and no previous generation is available"
            ))?;
            self.verify_generation(table, previous)?;
            let fallback = ActiveWeavePointer {
                table_id: pointer.table_id,
                revision: pointer.revision.checked_add(1).ok_or(DbError::LengthOverflow)?,
                active: previous,
                previous: None,
            };
            self.publish_active_pointer_wal(&root, &fallback, b"recover_active_weave_fallback")?;
        }
        Ok(())
    }

    /// Stable metadata fingerprint for the logical scan inventory of a table.
    ///
    /// ANF/Chronicle and active Weave artifacts are immutable once published;
    /// the token includes their sorted names, sizes and modification timestamps,
    /// together with table/schema and active-Weave identity. It is designed to
    /// invalidate explicit ANALYZE statistics after normal database writes or
    /// compaction publication without reading the table payload a second time.
    pub fn table_scan_snapshot_token(&self, table: &str) -> Result<[u8; 32], DbError> {
        let definition = self.catalog.tables.get(table)
            .ok_or_else(|| DbError::TableNotFound(table.to_owned()))?;
        let mut identity = Vec::new();
        identity.extend_from_slice(&definition.id.0.to_le_bytes());
        identity.extend_from_slice(&definition.schema_revision.to_le_bytes());

        let table_dir = self.root.join("data").join(table);
        let weave_root = table_dir.join("weaves");
        if let Some(pointer) = read_pointer(&weave_root, definition.id.0)? {
            identity.push(1);
            identity.extend_from_slice(&pointer.revision.to_le_bytes());
            identity.extend_from_slice(&pointer.active.weave_id.to_le_bytes());
            identity.extend_from_slice(&pointer.active.generation.to_le_bytes());
            identity.extend_from_slice(&pointer.active.manifest_sha256);
        } else {
            identity.push(0);
        }

        let mut paths = self.list_table_scan_paths(table, definition)?;
        paths.sort();
        for path in paths {
            let metadata = fs::metadata(&path)?;
            let name = path.file_name()
                .ok_or(DbError::CompactionPreflight("scan path has no file name"))?
                .to_string_lossy();
            let name_bytes = name.as_bytes();
            let name_len = u64::try_from(name_bytes.len()).map_err(|_| DbError::LengthOverflow)?;
            identity.extend_from_slice(&name_len.to_le_bytes());
            identity.extend_from_slice(name_bytes);
            identity.extend_from_slice(&metadata.len().to_le_bytes());
            let modified = metadata.modified()?.duration_since(UNIX_EPOCH).unwrap_or_default();
            identity.extend_from_slice(&modified.as_secs().to_le_bytes());
            identity.extend_from_slice(&modified.subsec_nanos().to_le_bytes());
        }
        Ok(sha256(&identity))
    }

    pub(super) fn list_table_scan_paths(
        &self, table: &str, definition: &TableDefinition,
    ) -> Result<Vec<PathBuf>, DbError> {
        let table_dir = self.root.join("data").join(table);
        let weave_root = table_dir.join("weaves");
        let pointer = read_pointer(&weave_root, definition.id.0)?;
        let output = if let Some(p) = pointer.as_ref() {
            Some(self.load_output(&weave_root, p.active)?)
        } else { None };
        let mut paths = Vec::new();
        for entry in fs::read_dir(&table_dir)? {
            let entry = entry?;
            if !entry.file_type()?.is_file() { continue; }
            let path = entry.path();
            match path.extension().and_then(|v| v.to_str()) {
                Some("anf") => paths.push(path),
                Some("chr") => {
                    let Some(out) = output.as_ref() else { paths.push(path); continue; };
                    let sequence = self.chronicle_indices.get(&definition.id).and_then(|idx|
                        idx.by_batch_id.values().find(|r| r.path == path).map(|r| r.metadata.sequence));
                    if !sequence.is_some_and(|s| s >= out.first_sequence && s <= out.last_sequence) {
                        paths.push(path);
                    }
                }
                _ => {}
            }
        }
        if let (Some(p), Some(out)) = (pointer, output) {
            let dir = candidate_dir(&weave_root, p.active.weave_id, p.active.generation);
            for artifact in out.artifacts { paths.push(dir.join(artifact.data_relative_path())); }
        }
        paths.sort();
        Ok(paths)
    }

    fn load_output(&self, root: &Path, generation: ActiveWeaveGeneration) -> Result<CompactionOutputManifest, DbError> {
        let dir = candidate_dir(root, generation.weave_id, generation.generation);
        require_directory(&dir)?;
        let bytes = read_regular(&dir.join("manifest.owm"))?;
        if sha256(&bytes) != generation.manifest_sha256 {
            return Err(DbError::CompactionPreflight("active OWM1 fingerprint mismatch"));
        }
        CompactionOutputManifest::decode(&bytes).map_err(|e| DbError::CompactionExecution(e.to_string()))
    }

    fn verify_generation(&self, table: &str, generation: ActiveWeaveGeneration) -> Result<(), DbError> {
        let root = self.root.join("data").join(table).join("weaves");
        let dir = candidate_dir(&root, generation.weave_id, generation.generation);
        require_directory(&dir)?;
        let input = CompactionInputManifest::decode(&read_regular(&dir.join("input.cim"))?)?;
        let manifest = read_regular(&dir.join("manifest.owm"))?;
        if sha256(&manifest) != generation.manifest_sha256 {
            return Err(DbError::CompactionPreflight("active OWM1 fingerprint mismatch"));
        }
        self.verify_compaction_candidate(table, &input, generation.weave_id, generation.generation)?;
        Ok(())
    }

    fn verify_candidate_matches_logical_scan(&self, table: &str, weave_id: u64, generation: u64) -> Result<(), DbError> {
        let definition = self.catalog.tables.get(table).ok_or_else(|| DbError::TableNotFound(table.to_owned()))?;
        let input_dir = candidate_dir(&self.root.join("data").join(table).join("weaves"), weave_id, generation);
        let input = CompactionInputManifest::decode(&read_regular(&input_dir.join("input.cim"))?)?;
        let output = self.verify_compaction_candidate(table, &input, weave_id, generation)?;
        let ids = definition.column_ids.iter().map(|id| id.0).collect::<Vec<_>>();
        let mut reference = self.scan(table)?.iter().map(|row|
            crate::canonical_typed_row_fingerprint(&definition.schema, definition.id.0, definition.schema_revision, &ids, row)
        ).collect::<Result<Vec<_>, _>>()?;
        let mut candidate = Vec::new();
        for artifact in &output.artifacts {
            let bytes = read_regular(&input_dir.join(artifact.data_relative_path()))?;
            let rows = crate::decode_segment_v2(&bytes, &definition.schema, definition.id.0, definition.schema_revision, &ids)?;
            for row in &rows {
                candidate.push(crate::canonical_typed_row_fingerprint(
                    &definition.schema, definition.id.0, definition.schema_revision, &ids, row,
                )?);
            }
        }
        reference.sort();
        candidate.sort();
        if reference != candidate {
            return Err(DbError::CompactionPreflight("candidate differs from the current logical table scan"));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PublishStage { TempCreated, BytesWritten, TempSynced, Replaced, DirectorySynced }

fn publish_pointer(root: &Path, pointer: &ActiveWeavePointer) -> Result<(), DbError> {
    publish_pointer_with_hook(root, pointer, |_| Ok(()))
}

fn publish_pointer_with_hook<F>(root: &Path, pointer: &ActiveWeavePointer, mut hook: F) -> Result<(), DbError>
where F: FnMut(PublishStage) -> std::io::Result<()> {
    require_directory(root)?;
    let bytes = pointer.encode()?;
    let target = root.join("active.awp");
    let temp = root.join(format!(".active-{:020}.tmp", pointer.revision));
    let mut owned = false;
    let mut replaced = false;
    let result = (|| -> Result<(), DbError> {
        let mut file = OpenOptions::new().write(true).create_new(true).open(&temp)?;
        owned = true;
        hook(PublishStage::TempCreated).map_err(DbError::Io)?;
        file.write_all(&bytes)?;
        file.flush()?;
        hook(PublishStage::BytesWritten).map_err(DbError::Io)?;
        file.sync_all()?;
        drop(file);
        hook(PublishStage::TempSynced).map_err(DbError::Io)?;
        crate::wal_recovery::maybe_crash_after(
            crate::wal_recovery::WalRecoveryCrashPoint::AfterActivePointerTempSynced,
        );
        fs::rename(&temp, &target)?;
        owned = false;
        replaced = true;
        hook(PublishStage::Replaced).map_err(DbError::Io)?;
        crate::wal_recovery::maybe_crash_after(
            crate::wal_recovery::WalRecoveryCrashPoint::AfterActivePointerRenamedBeforeDirectorySync,
        );
        sync_directory(root)?;
        hook(PublishStage::DirectorySynced).map_err(DbError::Io)?;
        Ok(())
    })();
    if let Err(error) = result {
        if owned { let _ = fs::remove_file(&temp); }
        if replaced {
            return Err(DbError::CompactionExecution(format!(
                "active pointer publication durability uncertain; reopen and reconcile: {error}"
            )));
        }
        return Err(error);
    }
    Ok(())
}

fn read_pointer(root: &Path, table_id: u64) -> Result<Option<ActiveWeavePointer>, DbError> {
    let root_metadata = match fs::symlink_metadata(root) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(DbError::Io(error)),
    };
    if !root_metadata.file_type().is_dir() {
        return Err(DbError::CompactionExecution("Weave root is not a real non-symlink directory".into()));
    }
    let path = root.join("active.awp");
    let metadata = match fs::symlink_metadata(&path) {
        Ok(m) => m,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(DbError::Io(e)),
    };
    if !metadata.file_type().is_file() {
        return Err(DbError::CompactionExecution("active pointer is not a regular file".into()));
    }
    let pointer = ActiveWeavePointer::decode(&fs::read(path)?)?;
    if pointer.table_id != table_id {
        return Err(DbError::CompactionExecution("active pointer TableId mismatch".into()));
    }
    Ok(Some(pointer))
}
fn candidate_dir(root: &Path, weave_id: u64, generation: u64) -> PathBuf {
    root.join(format!("weave-{weave_id:020}-g{generation:020}"))
}
fn require_directory(path: &Path) -> Result<(), DbError> {
    let m = fs::symlink_metadata(path)?;
    if !m.file_type().is_dir() { return Err(DbError::CompactionExecution("expected a real directory".into())); }
    Ok(())
}
fn read_regular(path: &Path) -> Result<Vec<u8>, DbError> {
    let m = fs::symlink_metadata(path)?;
    if !m.file_type().is_file() { return Err(DbError::CompactionExecution("expected a regular file".into())); }
    Ok(fs::read(path)?)
}
#[cfg(unix)]
fn sync_directory(path: &Path) -> Result<(), DbError> { File::open(path)?.sync_all()?; Ok(()) }
#[cfg(not(unix))]
fn sync_directory(_path: &Path) -> Result<(), DbError> { Ok(()) }

fn take<const N: usize>(b: &[u8], p: &mut usize) -> Result<[u8; N], DbError> {
    let end = (*p).checked_add(N).ok_or(DbError::LengthOverflow)?;
    let slice = b.get(*p..end).ok_or(DbError::LengthOverflow)?;
    let mut value = [0_u8; N];
    value.copy_from_slice(slice);
    *p = end;
    Ok(value)
}
fn take_u16(b: &[u8], p: &mut usize) -> Result<u16, DbError> { Ok(u16::from_le_bytes(take::<2>(b,p)?)) }
fn take_u32(b: &[u8], p: &mut usize) -> Result<u32, DbError> { Ok(u32::from_le_bytes(take::<4>(b,p)?)) }
fn take_u64(b: &[u8], p: &mut usize) -> Result<u64, DbError> { Ok(u64::from_le_bytes(take::<8>(b,p)?)) }

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn awp1_round_trip_and_checksum_validation() {
        let p = ActiveWeavePointer {
            table_id: 7, revision: 2,
            active: ActiveWeaveGeneration { weave_id: 10, generation: 1, manifest_sha256: [1;32] },
            previous: Some(ActiveWeaveGeneration { weave_id: 9, generation: 1, manifest_sha256: [2;32] }),
        };
        let bytes = p.encode().unwrap();
        assert_eq!(ActiveWeavePointer::decode(&bytes).unwrap(), p);
        let mut bad = bytes;
        bad[35] ^= 1;
        assert!(ActiveWeavePointer::decode(&bad).is_err());
    }

    #[test]
    fn atomic_pointer_faults_leave_only_a_complete_old_or_new_pointer() {
        for (case, fail_at, expected_new) in [
            (0_u8, PublishStage::TempCreated, false),
            (1_u8, PublishStage::BytesWritten, false),
            (2_u8, PublishStage::TempSynced, false),
            (3_u8, PublishStage::Replaced, true),
            (4_u8, PublishStage::DirectorySynced, true),
        ] {
            let root = std::env::temp_dir().join(format!(
                "ananta-awp-fault-{}-{case}", std::process::id()
            ));
            let _ = fs::remove_dir_all(&root);
            fs::create_dir_all(&root).unwrap();
            let old = ActiveWeavePointer {
                table_id: 1, revision: 1,
                active: ActiveWeaveGeneration { weave_id: 1, generation: 1, manifest_sha256: [1;32] },
                previous: None,
            };
            let new = ActiveWeavePointer {
                table_id: 1, revision: 2,
                active: ActiveWeaveGeneration { weave_id: 2, generation: 1, manifest_sha256: [2;32] },
                previous: Some(old.active),
            };
            publish_pointer(&root, &old).unwrap();
            let result = publish_pointer_with_hook(&root, &new, |stage| {
                if stage == fail_at { Err(std::io::Error::other("injected promotion interruption")) } else { Ok(()) }
            });
            assert!(result.is_err());
            let observed = read_pointer(&root, 1).unwrap().unwrap();
            assert_eq!(observed, if expected_new { new } else { old });
            fs::remove_dir_all(root).unwrap();
        }
    }
}
