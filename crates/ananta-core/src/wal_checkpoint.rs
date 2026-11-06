//! Durable ANCP v1 checkpoint snapshots for ANANTA.
//!
//! The checkpoint manifest fingerprints immutable database artifacts. A
//! checkpoint reference is then written as the first frame of a newly rotated
//! ANWL log. Recovery validates the reference and its immutable inventory
//! before replaying transactions appended after the checkpoint.

use std::collections::BTreeSet;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Component, Path, PathBuf};

const MANIFEST_MAGIC: &[u8; 4] = b"ANCP";
const MANIFEST_END: &[u8; 4] = b"CPND";
const MANIFEST_VERSION: u16 = 1;
const REFERENCE_MAGIC: &[u8; 8] = b"ANCPREF1";
const REFERENCE_LEN: usize = 8 + 8 + 8 + 8 + 32;
const MAX_ENTRIES: usize = 1_000_000;
const MAX_PATH_BYTES: usize = 4096;
const MAX_MANIFEST_BYTES: usize = 64 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckpointReport {
    pub base_sequence: u64,
    pub catalog_revision: u64,
    pub file_count: u64,
    pub total_bytes: u64,
    pub manifest_path: PathBuf,
    pub manifest_sha256: [u8; 32],
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Entry {
    path: String,
    length: u64,
    sha256: [u8; 32],
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CheckpointReference {
    pub base_sequence: u64,
    pub catalog_revision: u64,
    pub manifest_len: u64,
    pub manifest_sha256: [u8; 32],
}

impl CheckpointReference {
    pub fn encode(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(REFERENCE_LEN);
        bytes.extend_from_slice(REFERENCE_MAGIC);
        bytes.extend_from_slice(&self.base_sequence.to_le_bytes());
        bytes.extend_from_slice(&self.catalog_revision.to_le_bytes());
        bytes.extend_from_slice(&self.manifest_len.to_le_bytes());
        bytes.extend_from_slice(&self.manifest_sha256);
        bytes
    }

    pub fn decode(bytes: &[u8]) -> io::Result<Self> {
        if bytes.len() != REFERENCE_LEN || &bytes[..8] != REFERENCE_MAGIC {
            return Err(invalid("invalid checkpoint reference header or length"));
        }
        let mut manifest_sha256 = [0_u8; 32];
        manifest_sha256.copy_from_slice(&bytes[32..64]);
        Ok(Self {
            base_sequence: read_u64(bytes, 8)?,
            catalog_revision: read_u64(bytes, 16)?,
            manifest_len: read_u64(bytes, 24)?,
            manifest_sha256,
        })
    }

    pub fn manifest_filename(&self) -> String {
        format!("checkpoint-{:020}.anck", self.base_sequence)
    }
}

pub(crate) fn create_checkpoint(
    root: &Path,
    base_sequence: u64,
    catalog_revision: u64,
) -> io::Result<(CheckpointReference, CheckpointReport)> {
    let entries = collect_entries(root)?;
    let manifest = encode_manifest(base_sequence, catalog_revision, &entries)?;
    let reference = CheckpointReference {
        base_sequence,
        catalog_revision,
        manifest_len: u64::try_from(manifest.len()).map_err(|_| invalid("checkpoint manifest length overflow"))?,
        manifest_sha256: crate::chronicle::sha256(&manifest),
    };
    let checkpoint_directory = root.join("wal").join("checkpoints");
    fs::create_dir_all(&checkpoint_directory)?;
    let path = checkpoint_directory.join(reference.manifest_filename());
    if path.exists() {
        let metadata = fs::symlink_metadata(&path)?;
        if !metadata.file_type().is_file()
            || metadata.len() != reference.manifest_len
            || crate::chronicle::sha256_file(&path)? != reference.manifest_sha256
            || fs::read(&path)? != manifest
        {
            return Err(invalid("existing checkpoint manifest conflicts with the current snapshot"));
        }
    } else {
        let temporary = checkpoint_directory.join(format!(
            ".checkpoint-{:020}.tmp", base_sequence,
        ));
        match fs::symlink_metadata(&temporary) {
            Ok(metadata) if metadata.file_type().is_file() => fs::remove_file(&temporary)?,
            Ok(_) => return Err(invalid("checkpoint temporary path is not a regular file")),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
        let mut file = OpenOptions::new().write(true).create_new(true).open(&temporary)?;
        file.write_all(&manifest)?;
        file.flush()?;
        file.sync_all()?;
        drop(file);
        crate::wal_recovery::maybe_crash_after(
            crate::wal_recovery::WalRecoveryCrashPoint::AfterCheckpointManifestTempSynced,
        );
        fs::hard_link(&temporary, &path)?;
        sync_directory(&checkpoint_directory)?;
        crate::wal_recovery::maybe_crash_after(
            crate::wal_recovery::WalRecoveryCrashPoint::AfterCheckpointManifestPublished,
        );
        fs::remove_file(&temporary)?;
        sync_directory(&checkpoint_directory)?;
    }
    sync_directory(&checkpoint_directory)?;
    sync_directory(&checkpoint_directory.parent().ok_or_else(|| invalid("checkpoint directory has no parent"))?)?;

    let total_bytes = entries.iter().try_fold(0_u64, |total, entry| {
        total.checked_add(entry.length).ok_or_else(|| invalid("checkpoint byte count overflow"))
    })?;
    let report = CheckpointReport {
        base_sequence,
        catalog_revision,
        file_count: u64::try_from(entries.len()).map_err(|_| invalid("checkpoint file count overflow"))?,
        total_bytes,
        manifest_path: path,
        manifest_sha256: reference.manifest_sha256,
    };
    Ok((reference, report))
}

/// Validate the manifest selected by the checkpoint marker and every immutable
/// file that existed when the snapshot was installed. Later mutable catalog and
/// active-pointer replacement paths are intentionally excluded from the manifest.
pub(crate) fn validate_checkpoint(
    root: &Path,
    payload: &[u8],
    permitted_changed_paths: &BTreeSet<String>,
) -> io::Result<CheckpointReport> {
    let reference = CheckpointReference::decode(payload)?;
    if reference.manifest_len > MAX_MANIFEST_BYTES as u64 {
        return Err(invalid("checkpoint manifest exceeds size limit"));
    }
    let path = root.join("wal").join("checkpoints").join(reference.manifest_filename());
    let metadata = fs::symlink_metadata(&path)?;
    if !metadata.file_type().is_file() || metadata.len() != reference.manifest_len {
        return Err(invalid("checkpoint manifest is missing or has an invalid file type/length"));
    }
    if crate::chronicle::sha256_file(&path)? != reference.manifest_sha256 {
        return Err(invalid("checkpoint manifest SHA-256 mismatch"));
    }
    let bytes = fs::read(&path)?;
    let (base_sequence, catalog_revision, entries) = decode_manifest(&bytes)?;
    if base_sequence != reference.base_sequence || catalog_revision != reference.catalog_revision {
        return Err(invalid("checkpoint manifest identity differs from WAL marker"));
    }
    for entry in &entries {
        if permitted_changed_paths.contains(&entry.path) {
            continue;
        }
        let relative = checked_relative_path(&entry.path)?;
        let artifact_path = root.join(relative);
        let metadata = fs::symlink_metadata(&artifact_path)?;
        if !metadata.file_type().is_file() || metadata.len() != entry.length {
            return Err(invalid("checkpoint immutable artifact is missing or has changed length"));
        }
        if crate::chronicle::sha256_file(&artifact_path)? != entry.sha256 {
            return Err(invalid("checkpoint immutable artifact SHA-256 mismatch"));
        }
    }
    let total_bytes = entries.iter().try_fold(0_u64, |total, entry| {
        total.checked_add(entry.length).ok_or_else(|| invalid("checkpoint byte count overflow"))
    })?;
    Ok(CheckpointReport {
        base_sequence,
        catalog_revision,
        file_count: u64::try_from(entries.len()).map_err(|_| invalid("checkpoint file count overflow"))?,
        total_bytes,
        manifest_path: path,
        manifest_sha256: reference.manifest_sha256,
    })
}

fn collect_entries(root: &Path) -> io::Result<Vec<Entry>> {
    let mut files = Vec::new();
    collect_paths(root, root, &mut files)?;
    files.sort();
    if files.len() > MAX_ENTRIES {
        return Err(invalid("checkpoint artifact count exceeds limit"));
    }
    let mut entries = Vec::with_capacity(files.len());
    for path in files {
        let relative = path.strip_prefix(root).map_err(|_| invalid("checkpoint path escaped database root"))?;
        let text = relative.to_str().ok_or_else(|| invalid("checkpoint path is not UTF-8"))?
            .replace('\\', "/");
        if text.len() > MAX_PATH_BYTES {
            return Err(invalid("checkpoint path exceeds size limit"));
        }
        let metadata = fs::symlink_metadata(&path)?;
        if !metadata.file_type().is_file() {
            return Err(invalid("checkpoint artifact is not a regular file"));
        }
        entries.push(Entry {
            path: text,
            length: metadata.len(),
            sha256: crate::chronicle::sha256_file(&path)?,
        });
    }
    Ok(entries)
}

fn collect_paths(root: &Path, directory: &Path, output: &mut Vec<PathBuf>) -> io::Result<()> {
    let mut children = fs::read_dir(directory)?.collect::<Result<Vec<_>, _>>()?;
    children.sort_by_key(|entry| entry.file_name());
    for child in children {
        let name = child.file_name();
        let path = child.path();
        let relative = path.strip_prefix(root).map_err(|_| invalid("checkpoint path escaped root"))?;
        if should_exclude(relative, &name) {
            continue;
        }
        let file_type = child.file_type()?;
        if file_type.is_symlink() {
            return Err(invalid("checkpoint refuses symbolic links"));
        }
        if file_type.is_dir() {
            collect_paths(root, &path, output)?;
        } else if file_type.is_file() {
            if is_checkpoint_artifact(&path) {
                output.push(path);
            }
        } else {
            return Err(invalid("checkpoint found unsupported filesystem entry"));
        }
    }
    Ok(())
}

fn should_exclude(relative: &Path, name: &std::ffi::OsStr) -> bool {
    let text = relative.to_string_lossy().replace('\\', "/");
    if text == ".ananta.lock" || text.starts_with("wal/") || text == "wal" {
        return true;
    }
    if name.to_str().is_some_and(|value| {
        value.starts_with(".stage-") || value.starts_with(".candidate-")
            || value.starts_with(".active-") || value.ends_with(".tmp")
    }) {
        return true;
    }
    // Append-only CJE1 journals change independently of database row state;
    // they are not recovery inputs once the corresponding immutable candidate
    // publication has been checkpointed.
    if text.contains("/jobs/") || text.ends_with(".cje") {
        return true;
    }
    false
}

fn is_checkpoint_artifact(path: &Path) -> bool {
    let filename = path.file_name().and_then(|value| value.to_str()).unwrap_or("");
    let extension = path.extension().and_then(|value| value.to_str()).unwrap_or("");
    match extension {
        "anf" | "chr" | "cap" | "nvm" | "ans" | "wvf" | "pvm" | "cim" => true,
        "anc" => filename.starts_with("catalog-") || filename == "catalog.anc",
        "awp" => filename.starts_with("pointer-") || filename == "active.awp",
        "owm" => filename.starts_with("candidate-") || filename == "manifest.owm",
        _ => false,
    }
}

fn encode_manifest(base_sequence: u64, catalog_revision: u64, entries: &[Entry]) -> io::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(MANIFEST_MAGIC);
    bytes.extend_from_slice(&MANIFEST_VERSION.to_le_bytes());
    bytes.extend_from_slice(&0_u16.to_le_bytes());
    bytes.extend_from_slice(&base_sequence.to_le_bytes());
    bytes.extend_from_slice(&catalog_revision.to_le_bytes());
    bytes.extend_from_slice(&u32::try_from(entries.len()).map_err(|_| invalid("checkpoint entry count overflow"))?.to_le_bytes());
    for entry in entries {
        checked_relative_path(&entry.path)?;
        let path = entry.path.as_bytes();
        bytes.extend_from_slice(&u32::try_from(path.len()).map_err(|_| invalid("checkpoint path length overflow"))?.to_le_bytes());
        bytes.extend_from_slice(path);
        bytes.extend_from_slice(&entry.length.to_le_bytes());
        bytes.extend_from_slice(&entry.sha256);
        if bytes.len().checked_add(36).ok_or_else(|| invalid("checkpoint manifest overflow"))? > MAX_MANIFEST_BYTES {
            return Err(invalid("checkpoint manifest exceeds size limit"));
        }
    }
    let digest = crate::chronicle::sha256(&bytes);
    bytes.extend_from_slice(&digest);
    bytes.extend_from_slice(MANIFEST_END);
    Ok(bytes)
}

fn decode_manifest(bytes: &[u8]) -> io::Result<(u64, u64, Vec<Entry>)> {
    if bytes.len() < 4 + 2 + 2 + 8 + 8 + 4 + 32 + 4 || bytes.len() > MAX_MANIFEST_BYTES {
        return Err(invalid("checkpoint manifest has an invalid size"));
    }
    if &bytes[..4] != MANIFEST_MAGIC
        || u16::from_le_bytes([bytes[4], bytes[5]]) != MANIFEST_VERSION
        || bytes[6..8] != [0, 0]
        || &bytes[bytes.len() - 4..] != MANIFEST_END
    {
        return Err(invalid("checkpoint manifest header/version/end marker is invalid"));
    }
    let body_end = bytes.len() - 36;
    if crate::chronicle::sha256(&bytes[..body_end]).as_slice() != &bytes[body_end..body_end + 32] {
        return Err(invalid("checkpoint manifest checksum mismatch"));
    }
    let base_sequence = read_u64(bytes, 8)?;
    let catalog_revision = read_u64(bytes, 16)?;
    let count = read_u32(bytes, 24)? as usize;
    if count > MAX_ENTRIES {
        return Err(invalid("checkpoint entry count exceeds limit"));
    }
    let mut cursor = 28_usize;
    let mut entries = Vec::with_capacity(count);
    let mut seen = BTreeSet::new();
    for _ in 0..count {
        let path_len = read_u32(bytes, cursor)? as usize;
        cursor = cursor.checked_add(4).ok_or_else(|| invalid("checkpoint offset overflow"))?;
        if path_len == 0 || path_len > MAX_PATH_BYTES {
            return Err(invalid("checkpoint path length is invalid"));
        }
        let path_end = cursor.checked_add(path_len).ok_or_else(|| invalid("checkpoint path overflow"))?;
        let path = std::str::from_utf8(bytes.get(cursor..path_end).ok_or_else(|| invalid("truncated checkpoint path"))?)
            .map_err(|_| invalid("checkpoint path is not UTF-8"))?.to_owned();
        checked_relative_path(&path)?;
        cursor = path_end;
        let length = read_u64(bytes, cursor)?;
        cursor = cursor.checked_add(8).ok_or_else(|| invalid("checkpoint offset overflow"))?;
        let digest_end = cursor.checked_add(32).ok_or_else(|| invalid("checkpoint digest overflow"))?;
        let digest = bytes.get(cursor..digest_end).ok_or_else(|| invalid("truncated checkpoint digest"))?;
        let mut sha256 = [0_u8; 32];
        sha256.copy_from_slice(digest);
        cursor = digest_end;
        if !seen.insert(path.clone()) {
            return Err(invalid("checkpoint contains duplicate artifact paths"));
        }
        entries.push(Entry { path, length, sha256 });
    }
    if cursor != body_end {
        return Err(invalid("checkpoint manifest has trailing bytes"));
    }
    Ok((base_sequence, catalog_revision, entries))
}

fn checked_relative_path(value: &str) -> io::Result<PathBuf> {
    if value.is_empty() || value.len() > MAX_PATH_BYTES || value.contains('\\') {
        return Err(invalid("invalid checkpoint relative path"));
    }
    let path = Path::new(value);
    if path.is_absolute() || path.components().any(|part| !matches!(part, Component::Normal(_))) {
        return Err(invalid("checkpoint path escapes database root"));
    }
    Ok(path.to_path_buf())
}

fn read_u32(bytes: &[u8], offset: usize) -> io::Result<u32> {
    let end = offset.checked_add(4).ok_or_else(|| invalid("checkpoint offset overflow"))?;
    Ok(u32::from_le_bytes(bytes.get(offset..end).ok_or_else(|| invalid("truncated checkpoint integer"))?
        .try_into().map_err(|_| invalid("invalid checkpoint integer"))?))
}
fn read_u64(bytes: &[u8], offset: usize) -> io::Result<u64> {
    let end = offset.checked_add(8).ok_or_else(|| invalid("checkpoint offset overflow"))?;
    Ok(u64::from_le_bytes(bytes.get(offset..end).ok_or_else(|| invalid("truncated checkpoint integer"))?
        .try_into().map_err(|_| invalid("invalid checkpoint integer"))?))
}
fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}
fn sync_directory(path: &Path) -> io::Result<()> {
    #[cfg(unix)]
    { File::open(path)?.sync_all() }
    #[cfg(not(unix))]
    { let _ = path; Ok(()) }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_path() -> PathBuf {
        let nonce = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
        std::env::temp_dir().join(format!("ananta-checkpoint-{nonce}"))
    }

    #[test]
    fn reference_round_trips_and_rejects_malformed_payloads() {
        let reference = CheckpointReference {
            base_sequence: 44,
            catalog_revision: 3,
            manifest_len: 100,
            manifest_sha256: [9; 32],
        };
        assert_eq!(CheckpointReference::decode(&reference.encode()).unwrap(), reference);
        assert!(CheckpointReference::decode(b"invalid").is_err());
        assert_eq!(reference.manifest_filename(), "checkpoint-00000000000000000044.anck");
    }

    #[test]
    fn checkpoint_manifest_validates_immutable_file_inventory() {
        let root = temp_path();
        fs::create_dir_all(root.join("data/table")).unwrap();
        fs::create_dir_all(root.join("catalog")).unwrap();
        fs::write(root.join("data/table/segment-00000000000000000001.anf"), b"immutable").unwrap();
        fs::write(root.join("catalog/catalog.anc"), b"mutable catalog").unwrap();
        let (reference, report) = create_checkpoint(&root, 12, 2).unwrap();
        assert_eq!(report.file_count, 1);
        let validated = validate_checkpoint(&root, &reference.encode(), &BTreeSet::new()).unwrap();
        assert_eq!(validated.base_sequence, 12);
        fs::write(root.join("data/table/segment-00000000000000000001.anf"), b"tampered").unwrap();
        assert!(validate_checkpoint(&root, &reference.encode(), &BTreeSet::new()).is_err());
        fs::remove_dir_all(root).unwrap();
    }
}
