//! Versioned, checksummed directory backup for an ANANTA database.
//!
//! A backup is built in a sibling staging directory and becomes visible only
//! after every file and the manifest have been synced. Restore verifies the
//! complete manifest before copying into a fresh destination directory.

use std::collections::BTreeSet;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Component, Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

const MAGIC: &[u8; 4] = b"ANBK";
const END: &[u8; 4] = b"BKND";
const VERSION: u16 = 1;
const MANIFEST_NAME: &str = "backup.anbk";
const MAX_FILES: usize = 1_000_000;
const MAX_PATH_BYTES: usize = 4096;
const MAX_MANIFEST_BYTES: usize = 64 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackupReport {
    pub directory: PathBuf,
    pub file_count: u64,
    pub total_bytes: u64,
    pub manifest_sha256: [u8; 32],
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Entry {
    path: String,
    length: u64,
    sha256: [u8; 32],
}

pub(crate) fn create_backup(source: &Path, destination: &Path) -> io::Result<BackupReport> {
    let source = fs::canonicalize(source)?;
    let destination = prepare_new_destination(&source, destination)?;
    let parent = destination.parent().ok_or_else(|| invalid("backup destination has no parent"))?;
    let staging = unique_sibling(parent, destination.file_name().ok_or_else(|| invalid("invalid backup destination name"))?, "backup")?;
    fs::create_dir(&staging)?;

    let result = (|| {
        let mut entries = Vec::new();
        copy_tree(&source, &staging, Path::new(""), true, &mut entries)?;
        entries.sort_by(|a, b| a.path.cmp(&b.path));
        let manifest = encode_manifest(&entries)?;
        let manifest_path = staging.join(MANIFEST_NAME);
        let mut file = OpenOptions::new().write(true).create_new(true).open(&manifest_path)?;
        file.write_all(&manifest)?;
        file.flush()?;
        file.sync_all()?;
        sync_tree_directories(&staging)?;
        sync_directory(parent)?;
        fs::rename(&staging, &destination)?;
        sync_directory(parent)?;
        report(&destination, &entries, &manifest)
    })();

    if result.is_err() {
        let _ = fs::remove_dir_all(&staging);
    }
    result
}

pub(crate) fn restore_backup(backup: &Path, destination: &Path) -> io::Result<BackupReport> {
    let backup = fs::canonicalize(backup)?;
    let (entries, manifest) = verify_backup(&backup)?;
    let destination = prepare_new_destination(&backup, destination)?;
    let parent = destination.parent().ok_or_else(|| invalid("restore destination has no parent"))?;
    let staging = unique_sibling(parent, destination.file_name().ok_or_else(|| invalid("invalid restore destination name"))?, "restore")?;
    fs::create_dir(&staging)?;

    let result = (|| {
        for entry in &entries {
            let relative = checked_relative_path(&entry.path)?;
            let source = backup.join(&relative);
            let target = staging.join(&relative);
            let metadata = fs::symlink_metadata(&source)?;
            if !metadata.file_type().is_file() || metadata.len() != entry.length {
                return Err(invalid("backup file changed after manifest verification"));
            }
            if let Some(directory) = target.parent() {
                fs::create_dir_all(directory)?;
            }
            fs::copy(&source, &target)?;
            File::open(&target)?.sync_all()?;
            if crate::chronicle::sha256_file(&target)? != entry.sha256 {
                return Err(invalid("restored file fingerprint differs from backup manifest"));
            }
        }
        sync_tree_directories(&staging)?;
        sync_directory(parent)?;
        fs::rename(&staging, &destination)?;
        sync_directory(parent)?;
        report(&destination, &entries, &manifest)
    })();

    if result.is_err() {
        let _ = fs::remove_dir_all(&staging);
    }
    result
}

pub(crate) fn verify_backup(directory: &Path) -> io::Result<(Vec<Entry>, Vec<u8>)> {
    let metadata = fs::symlink_metadata(directory)?;
    if !metadata.file_type().is_dir() {
        return Err(invalid("backup path is not a real directory"));
    }
    let manifest_path = directory.join(MANIFEST_NAME);
    let manifest_metadata = fs::symlink_metadata(&manifest_path)?;
    if !manifest_metadata.file_type().is_file() || manifest_metadata.len() > MAX_MANIFEST_BYTES as u64 {
        return Err(invalid("backup manifest is not a bounded regular file"));
    }
    let manifest = fs::read(&manifest_path)?;
    let entries = decode_manifest(&manifest)?;
    let mut observed = BTreeSet::new();
    collect_file_names(directory, Path::new(""), &mut observed, true)?;
    let expected = entries.iter().map(|entry| entry.path.clone()).collect::<BTreeSet<_>>();
    if observed != expected {
        return Err(invalid("backup file inventory differs from its manifest"));
    }
    for entry in &entries {
        let relative = checked_relative_path(&entry.path)?;
        let path = directory.join(relative);
        let metadata = fs::symlink_metadata(&path)?;
        if !metadata.file_type().is_file() || metadata.len() != entry.length {
            return Err(invalid("backup file type or length differs from manifest"));
        }
        if crate::chronicle::sha256_file(&path)? != entry.sha256 {
            return Err(invalid("backup file SHA-256 differs from manifest"));
        }
    }
    Ok((entries, manifest))
}

fn prepare_new_destination(source: &Path, requested: &Path) -> io::Result<PathBuf> {
    let requested_parent = requested.parent().filter(|p| !p.as_os_str().is_empty()).unwrap_or(Path::new("."));
    fs::create_dir_all(requested_parent)?;
    let parent = fs::canonicalize(requested_parent)?;
    let name = requested.file_name().ok_or_else(|| invalid("destination must name a directory"))?;
    let destination = parent.join(name);
    if destination.exists() || fs::symlink_metadata(&destination).is_ok() {
        return Err(io::Error::new(io::ErrorKind::AlreadyExists, "destination already exists"));
    }
    if destination.starts_with(source) || source.starts_with(&destination) {
        return Err(invalid("backup/restore source and destination must not contain one another"));
    }
    Ok(destination)
}

fn unique_sibling(parent: &Path, name: &std::ffi::OsStr, kind: &str) -> io::Result<PathBuf> {
    let base = name.to_string_lossy();
    for attempt in 0..16_u8 {
        let nonce = SystemTime::now().duration_since(UNIX_EPOCH)
            .map_err(|_| invalid("system clock is before Unix epoch"))?
            .as_nanos();
        let candidate = parent.join(format!(".{base}.{kind}-{nonce}-{attempt}.tmp"));
        if fs::create_dir(&candidate).is_ok() {
            fs::remove_dir(&candidate)?;
            return Ok(candidate);
        }
    }
    Err(io::Error::new(io::ErrorKind::AlreadyExists, "could not allocate a unique staging directory"))
}

fn copy_tree(
    source: &Path,
    destination: &Path,
    relative: &Path,
    is_root: bool,
    entries: &mut Vec<Entry>,
) -> io::Result<()> {
    let mut children = fs::read_dir(source)?.collect::<Result<Vec<_>, _>>()?;
    children.sort_by_key(|entry| entry.file_name());
    for child in children {
        let name = child.file_name();
        if is_root && name == ".ananta.lock" {
            continue;
        }
        let source_path = child.path();
        let target_path = destination.join(&name);
        let child_relative = relative.join(&name);
        let file_type = child.file_type()?;
        if file_type.is_symlink() {
            return Err(invalid("database contains a symbolic link; refusing an unsafe backup"));
        }
        if file_type.is_dir() {
            fs::create_dir(&target_path)?;
            copy_tree(&source_path, &target_path, &child_relative, false, entries)?;
        } else if file_type.is_file() {
            if entries.len() >= MAX_FILES {
                return Err(invalid("database contains too many files for the backup format"));
            }
            if let Some(parent) = target_path.parent() {
                fs::create_dir_all(parent)?;
            }
            fs::copy(&source_path, &target_path)?;
            File::open(&target_path)?.sync_all()?;
            let length = fs::metadata(&target_path)?.len();
            let sha256 = crate::chronicle::sha256_file(&target_path)?;
            let path_text = child_relative.to_str().ok_or_else(|| invalid("database path is not valid UTF-8"))?
                .replace('\\', "/");
            if path_text.len() > MAX_PATH_BYTES {
                return Err(invalid("database path exceeds backup format limit"));
            }
            entries.push(Entry { path: path_text, length, sha256 });
        } else {
            return Err(invalid("database contains an unsupported filesystem entry"));
        }
    }
    Ok(())
}

fn collect_file_names(
    directory: &Path,
    relative: &Path,
    names: &mut BTreeSet<String>,
    is_root: bool,
) -> io::Result<()> {
    for child in fs::read_dir(directory)? {
        let child = child?;
        let name = child.file_name();
        if is_root && name == MANIFEST_NAME {
            continue;
        }
        let file_type = child.file_type()?;
        if file_type.is_symlink() {
            return Err(invalid("backup contains a symbolic link"));
        }
        let child_relative = relative.join(&name);
        if file_type.is_dir() {
            collect_file_names(&child.path(), &child_relative, names, false)?;
        } else if file_type.is_file() {
            let text = child_relative.to_str().ok_or_else(|| invalid("backup path is not valid UTF-8"))?
                .replace('\\', "/");
            names.insert(text);
        } else {
            return Err(invalid("backup contains an unsupported filesystem entry"));
        }
    }
    Ok(())
}

fn checked_relative_path(value: &str) -> io::Result<PathBuf> {
    if value.is_empty() || value.len() > MAX_PATH_BYTES || value.contains('\\') {
        return Err(invalid("invalid backup manifest path"));
    }
    let path = Path::new(value);
    if path.is_absolute() || path.components().any(|part| !matches!(part, Component::Normal(_))) {
        return Err(invalid("backup manifest path escapes the backup root"));
    }
    Ok(path.to_path_buf())
}

fn encode_manifest(entries: &[Entry]) -> io::Result<Vec<u8>> {
    if entries.len() > MAX_FILES {
        return Err(invalid("too many backup manifest entries"));
    }
    let mut bytes = Vec::new();
    bytes.extend_from_slice(MAGIC);
    bytes.extend_from_slice(&VERSION.to_le_bytes());
    bytes.extend_from_slice(&0_u16.to_le_bytes());
    bytes.extend_from_slice(&u32::try_from(entries.len()).map_err(|_| invalid("backup entry count overflow"))?.to_le_bytes());
    for entry in entries {
        checked_relative_path(&entry.path)?;
        let path = entry.path.as_bytes();
        bytes.extend_from_slice(&u32::try_from(path.len()).map_err(|_| invalid("backup path length overflow"))?.to_le_bytes());
        bytes.extend_from_slice(path);
        bytes.extend_from_slice(&entry.length.to_le_bytes());
        bytes.extend_from_slice(&entry.sha256);
        if bytes.len().checked_add(36).ok_or_else(|| invalid("backup manifest length overflow"))? > MAX_MANIFEST_BYTES {
            return Err(invalid("backup manifest exceeds size limit"));
        }
    }
    let digest = crate::chronicle::sha256(&bytes);
    bytes.extend_from_slice(&digest);
    bytes.extend_from_slice(END);
    Ok(bytes)
}

fn decode_manifest(bytes: &[u8]) -> io::Result<Vec<Entry>> {
    if bytes.len() < 4 + 2 + 2 + 4 + 32 + 4 || bytes.len() > MAX_MANIFEST_BYTES {
        return Err(invalid("backup manifest has an invalid size"));
    }
    if &bytes[..4] != MAGIC || u16::from_le_bytes([bytes[4], bytes[5]]) != VERSION
        || bytes[6..8] != [0, 0] || &bytes[bytes.len() - 4..] != END
    {
        return Err(invalid("backup manifest header, version or end marker is invalid"));
    }
    let body_end = bytes.len() - 36;
    if crate::chronicle::sha256(&bytes[..body_end]) != bytes[body_end..body_end + 32] {
        return Err(invalid("backup manifest checksum mismatch"));
    }
    let count = u32::from_le_bytes(bytes[8..12].try_into().map_err(|_| invalid("invalid backup entry count"))?) as usize;
    if count > MAX_FILES {
        return Err(invalid("backup manifest entry count exceeds limit"));
    }
    let mut cursor = 12_usize;
    let mut entries = Vec::with_capacity(count);
    let mut seen = BTreeSet::new();
    for _ in 0..count {
        let path_len = read_u32(bytes, cursor)? as usize;
        cursor = cursor.checked_add(4).ok_or_else(|| invalid("backup manifest offset overflow"))?;
        if path_len == 0 || path_len > MAX_PATH_BYTES {
            return Err(invalid("backup manifest path length is invalid"));
        }
        let path_end = cursor.checked_add(path_len).ok_or_else(|| invalid("backup manifest path overflow"))?;
        let path = std::str::from_utf8(bytes.get(cursor..path_end).ok_or_else(|| invalid("truncated backup manifest path"))?)
            .map_err(|_| invalid("backup manifest path is not UTF-8"))?.to_owned();
        checked_relative_path(&path)?;
        cursor = path_end;
        let length = read_u64(bytes, cursor)?;
        cursor = cursor.checked_add(8).ok_or_else(|| invalid("backup manifest offset overflow"))?;
        let digest_end = cursor.checked_add(32).ok_or_else(|| invalid("backup manifest digest overflow"))?;
        let digest_bytes = bytes.get(cursor..digest_end).ok_or_else(|| invalid("truncated backup manifest digest"))?;
        let mut sha256 = [0_u8; 32];
        sha256.copy_from_slice(digest_bytes);
        cursor = digest_end;
        if !seen.insert(path.clone()) {
            return Err(invalid("backup manifest contains a duplicate path"));
        }
        entries.push(Entry { path, length, sha256 });
    }
    if cursor != body_end {
        return Err(invalid("backup manifest has trailing or unparsed bytes"));
    }
    Ok(entries)
}

fn read_u32(bytes: &[u8], offset: usize) -> io::Result<u32> {
    let end = offset.checked_add(4).ok_or_else(|| invalid("backup offset overflow"))?;
    Ok(u32::from_le_bytes(bytes.get(offset..end).ok_or_else(|| invalid("truncated backup integer"))?
        .try_into().map_err(|_| invalid("invalid backup integer"))?))
}
fn read_u64(bytes: &[u8], offset: usize) -> io::Result<u64> {
    let end = offset.checked_add(8).ok_or_else(|| invalid("backup offset overflow"))?;
    Ok(u64::from_le_bytes(bytes.get(offset..end).ok_or_else(|| invalid("truncated backup integer"))?
        .try_into().map_err(|_| invalid("invalid backup integer"))?))
}
fn report(directory: &Path, entries: &[Entry], manifest: &[u8]) -> io::Result<BackupReport> {
    let total_bytes = entries.iter().try_fold(0_u64, |total, entry| {
        total.checked_add(entry.length).ok_or_else(|| invalid("backup total byte count overflow"))
    })?;
    Ok(BackupReport {
        directory: directory.to_path_buf(),
        file_count: u64::try_from(entries.len()).map_err(|_| invalid("backup file count overflow"))?,
        total_bytes,
        manifest_sha256: crate::chronicle::sha256(manifest),
    })
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
fn sync_tree_directories(root: &Path) -> io::Result<()> {
    let mut directories = Vec::new();
    collect_directories(root, &mut directories)?;
    directories.sort_by_key(|path| std::cmp::Reverse(path.components().count()));
    for directory in directories { sync_directory(&directory)?; }
    Ok(())
}
fn collect_directories(root: &Path, output: &mut Vec<PathBuf>) -> io::Result<()> {
    output.push(root.to_path_buf());
    for child in fs::read_dir(root)? {
        let child = child?;
        if child.file_type()?.is_dir() {
            collect_directories(&child.path(), output)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_path(label: &str) -> PathBuf {
        let nonce = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
        std::env::temp_dir().join(format!("ananta-backup-{label}-{nonce}"))
    }

    #[test]
    fn backup_manifest_rejects_traversal_duplicate_and_corruption() {
        let entries = vec![Entry { path: "catalog/catalog.anc".into(), length: 3, sha256: [7; 32] }];
        let mut bytes = encode_manifest(&entries).unwrap();
        assert_eq!(decode_manifest(&bytes).unwrap(), entries);
        let offset = 12 + 4;
        bytes[offset] = b'.';
        // The old digest no longer matches; checksum corruption must be caught first.
        assert!(decode_manifest(&bytes).is_err());
        assert!(checked_relative_path("../outside").is_err());
        assert!(checked_relative_path("/absolute").is_err());
    }

    #[test]
    fn backup_restore_verifies_exact_files_and_rejects_tampering() {
        let root = temp_path("source");
        let backup = temp_path("backup");
        let restored = temp_path("restored");
        fs::create_dir_all(root.join("catalog")).unwrap();
        fs::create_dir_all(root.join("data/table")).unwrap();
        fs::create_dir_all(root.join("wal")).unwrap();
        fs::write(root.join("catalog/catalog.anc"), b"catalog").unwrap();
        fs::write(root.join("data/table/segment.anf"), b"segment").unwrap();
        fs::write(root.join("wal/database.anwl"), b"wal").unwrap();
        fs::write(root.join(".ananta.lock"), b"pid=123").unwrap();

        let report = create_backup(&root, &backup).unwrap();
        assert_eq!(report.file_count, 3);
        verify_backup(&backup).unwrap();
        let restored_report = restore_backup(&backup, &restored).unwrap();
        assert_eq!(restored_report.file_count, 3);
        assert_eq!(fs::read(restored.join("catalog/catalog.anc")).unwrap(), b"catalog");
        fs::write(backup.join("data/table/segment.anf"), b"tampered").unwrap();
        assert!(verify_backup(&backup).is_err());

        fs::remove_dir_all(root).unwrap();
        fs::remove_dir_all(backup).unwrap();
        fs::remove_dir_all(restored).unwrap();
    }
}
