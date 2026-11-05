use crate::DbError;
use std::fs::{self, OpenOptions};
#[cfg(unix)]
use std::fs::File;
use std::io::{self, Write};
use std::path::{Path, PathBuf};

const SEGMENT_PREFIX: &str = "segment-";
const TEMP_SEGMENT_PREFIX: &str = ".segment-";
const SEGMENT_SUFFIX: &str = ".anf";
const TEMP_SEGMENT_SUFFIX: &str = ".tmp";
const CHRONICLE_PREFIX: &str = "segment-";
const TEMP_CHRONICLE_PREFIX: &str = ".chronicle-";
const CHRONICLE_SUFFIX: &str = ".chr";
const CAPSULE_PREFIX: &str = "capsule-";
const TEMP_CAPSULE_PREFIX: &str = ".capsule-";
const CAPSULE_SUFFIX: &str = ".cap";

/// Exclusive local writer lock. Stale locks are deliberately not broken automatically.
pub(crate) struct DatabaseLock {
    path: PathBuf,
}

impl DatabaseLock {
    pub(crate) fn acquire(root: &Path) -> Result<Self, DbError> {
        let path = root.join(".ananta.lock");
        let mut file = match OpenOptions::new().write(true).create_new(true).open(&path) {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                return Err(DbError::DatabaseLocked(path));
            }
            Err(error) => return Err(DbError::Io(error)),
        };
        let file_result = writeln!(file, "pid={}", std::process::id())
            .and_then(|()| file.sync_all());
        drop(file);
        if let Err(error) = file_result {
            let _ = fs::remove_file(&path);
            return Err(DbError::Io(error));
        }
        if let Err(error) = sync_directory(root) {
            let _ = fs::remove_file(&path);
            return Err(error);
        }
        Ok(Self { path })
    }
}

impl Drop for DatabaseLock {
    fn drop(&mut self) {
        if fs::remove_file(&self.path).is_ok() {
            if let Some(parent) = self.path.parent() {
                let _ = sync_directory(parent);
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PublicationStage {
    TemporaryCreated,
    BytesWritten,
    TemporaryFileSynced,
    FinalNamePublished,
    DirectorySyncedAfterPublish,
    TemporaryNameRemoved,
    DirectorySyncedAfterCleanup,
}

pub(crate) fn publish_segment(
    directory: &Path,
    segment_id: u64,
    bytes: &[u8],
) -> Result<PathBuf, DbError> {
    publish_segment_with_hook(directory, segment_id, bytes, |_| Ok(()))
}

fn publish_segment_with_hook<F>(
    directory: &Path,
    segment_id: u64,
    bytes: &[u8],
    mut checkpoint: F,
) -> Result<PathBuf, DbError>
where
    F: FnMut(PublicationStage) -> io::Result<()>,
{
    if segment_id == 0 {
        return Err(DbError::LengthOverflow);
    }
    let final_path = directory.join(format!("{SEGMENT_PREFIX}{segment_id:020}{SEGMENT_SUFFIX}"));
    let temp_path = directory.join(format!("{TEMP_SEGMENT_PREFIX}{segment_id:020}{TEMP_SEGMENT_SUFFIX}"));

    let mut temporary_owned = false;
    let mut published = false;
    let result = (|| -> Result<(), DbError> {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temp_path)?;
        temporary_owned = true;
        checkpoint(PublicationStage::TemporaryCreated).map_err(DbError::Io)?;

        file.write_all(bytes)?;
        file.flush()?;
        checkpoint(PublicationStage::BytesWritten).map_err(DbError::Io)?;

        file.sync_all()?;
        drop(file);
        checkpoint(PublicationStage::TemporaryFileSynced).map_err(DbError::Io)?;

        // Unlike rename on common platforms, hard_link never replaces an
        // existing final path. Both names are in the same table directory.
        fs::hard_link(&temp_path, &final_path)?;
        published = true;
        checkpoint(PublicationStage::FinalNamePublished).map_err(DbError::Io)?;

        sync_directory(directory)?;
        checkpoint(PublicationStage::DirectorySyncedAfterPublish).map_err(DbError::Io)?;

        fs::remove_file(&temp_path)?;
        temporary_owned = false;
        checkpoint(PublicationStage::TemporaryNameRemoved).map_err(DbError::Io)?;

        sync_directory(directory)?;
        checkpoint(PublicationStage::DirectorySyncedAfterCleanup).map_err(DbError::Io)?;
        Ok(())
    })();

    if let Err(error) = result {
        if !published && temporary_owned {
            let _ = fs::remove_file(&temp_path);
        }
        if published {
            return Err(DbError::SegmentPublicationUncertain {
                path: final_path,
                reason: error.to_string(),
            });
        }
        return Err(error);
    }
    Ok(final_path)
}


pub(crate) fn publish_chronicle(
    directory: &Path,
    file_id: u64,
    bytes: &[u8],
) -> Result<PathBuf, DbError> {
    publish_chronicle_with_hook(directory, file_id, bytes, |_| Ok(()))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ChroniclePublicationStage {
    TemporaryCreated,
    BytesWritten,
    TemporaryFileSynced,
    FinalNamePublished,
    DirectorySyncedAfterPublish,
    TemporaryNameRemoved,
    DirectorySyncedAfterCleanup,
}

fn publish_chronicle_with_hook<F>(
    directory: &Path,
    file_id: u64,
    bytes: &[u8],
    mut checkpoint: F,
) -> Result<PathBuf, DbError>
where
    F: FnMut(ChroniclePublicationStage) -> io::Result<()>,
{
    if file_id == 0 {
        return Err(DbError::LengthOverflow);
    }
    let final_path = directory.join(format!("{CHRONICLE_PREFIX}{file_id:020}{CHRONICLE_SUFFIX}"));
    let temp_path = directory.join(format!("{TEMP_CHRONICLE_PREFIX}{file_id:020}{TEMP_SEGMENT_SUFFIX}"));
    let mut temporary_owned = false;
    let mut published = false;
    let result = (|| -> Result<(), DbError> {
        let mut file = OpenOptions::new().write(true).create_new(true).open(&temp_path)?;
        temporary_owned = true;
        checkpoint(ChroniclePublicationStage::TemporaryCreated).map_err(DbError::Io)?;

        file.write_all(bytes)?;
        file.flush()?;
        checkpoint(ChroniclePublicationStage::BytesWritten).map_err(DbError::Io)?;

        file.sync_all()?;
        drop(file);
        checkpoint(ChroniclePublicationStage::TemporaryFileSynced).map_err(DbError::Io)?;

        fs::hard_link(&temp_path, &final_path)?;
        published = true;
        checkpoint(ChroniclePublicationStage::FinalNamePublished).map_err(DbError::Io)?;

        sync_directory(directory)?;
        checkpoint(ChroniclePublicationStage::DirectorySyncedAfterPublish).map_err(DbError::Io)?;

        fs::remove_file(&temp_path)?;
        temporary_owned = false;
        checkpoint(ChroniclePublicationStage::TemporaryNameRemoved).map_err(DbError::Io)?;

        sync_directory(directory)?;
        checkpoint(ChroniclePublicationStage::DirectorySyncedAfterCleanup).map_err(DbError::Io)?;
        Ok(())
    })();

    if let Err(error) = result {
        if !published && temporary_owned {
            let _ = fs::remove_file(&temp_path);
        }
        if published {
            return Err(DbError::SegmentPublicationUncertain {
                path: final_path,
                reason: error.to_string(),
            });
        }
        return Err(error);
    }
    Ok(final_path)
}

pub(crate) fn publish_capsule(
    directory: &Path,
    capsule_id: u64,
    generation: u64,
    bytes: &[u8],
) -> Result<PathBuf, DbError> {
    if capsule_id == 0 || generation == 0 {
        return Err(DbError::LengthOverflow);
    }
    let final_path = directory.join(format!("{CAPSULE_PREFIX}{capsule_id:020}-g{generation:020}{CAPSULE_SUFFIX}"));
    let temp_path = directory.join(format!("{TEMP_CAPSULE_PREFIX}{capsule_id:020}-g{generation:020}{TEMP_SEGMENT_SUFFIX}"));
    let mut temporary_owned = false;
    let mut published = false;
    let result = (|| -> Result<(), DbError> {
        let mut file = OpenOptions::new().write(true).create_new(true).open(&temp_path)?;
        temporary_owned = true;
        file.write_all(bytes)?;
        file.flush()?;
        file.sync_all()?;
        drop(file);
        fs::hard_link(&temp_path, &final_path)?;
        published = true;
        sync_directory(directory)?;
        fs::remove_file(&temp_path)?;
        temporary_owned = false;
        sync_directory(directory)?;
        Ok(())
    })();

    if let Err(error) = result {
        if !published && temporary_owned {
            let _ = fs::remove_file(&temp_path);
        }
        if published {
            return Err(DbError::SegmentPublicationUncertain {
                path: final_path,
                reason: error.to_string(),
            });
        }
        return Err(error);
    }
    Ok(final_path)
}

/// Remove only canonical regular-file temp segments. Temp names are never committed.
pub(crate) fn cleanup_orphan_segment_temps(data_root: &Path) -> Result<Vec<PathBuf>, DbError> {
    let mut removed = Vec::new();
    for table_entry in fs::read_dir(data_root)? {
        let table_entry = table_entry?;
        if !table_entry.file_type()?.is_dir() {
            continue;
        }
        let table_dir = table_entry.path();
        let mut changed = false;
        for entry in fs::read_dir(&table_dir)? {
            let entry = entry?;
            let entry_type = entry.file_type()?;
            let path = entry.path();
            if entry_type.is_dir() && entry.file_name() == "capsules" {
                for capsule_entry in fs::read_dir(&path)? {
                    let capsule_entry = capsule_entry?;
                    if !capsule_entry.file_type()?.is_file() {
                        continue;
                    }
                    let capsule_path = capsule_entry.path();
                    let Some(filename) = capsule_path.file_name().and_then(|name| name.to_str()) else {
                        continue;
                    };
                    if is_canonical_temp_capsule_name(filename) {
                        fs::remove_file(&capsule_path)?;
                        removed.push(capsule_path);
                        sync_directory(&path)?;
                    }
                }
                continue;
            }
            if !entry_type.is_file() {
                continue;
            }
            let Some(filename) = path.file_name().and_then(|name| name.to_str()) else {
                continue;
            };
            if is_canonical_temp_segment_name(filename) || is_canonical_temp_chronicle_name(filename) {
                fs::remove_file(&path)?;
                removed.push(path);
                changed = true;
            }
        }
        if changed {
            sync_directory(&table_dir)?;
        }
    }
    removed.sort();
    Ok(removed)
}



fn is_canonical_temp_capsule_name(filename: &str) -> bool {
    let Some(value) = filename.strip_prefix(TEMP_CAPSULE_PREFIX)
        .and_then(|value| value.strip_suffix(TEMP_SEGMENT_SUFFIX))
    else { return false; };
    let Some((capsule_id, generation)) = value.split_once("-g") else { return false; };
    let canonical = |text: &str| {
        text.len() == 20
            && text.bytes().all(|byte| byte.is_ascii_digit())
            && text.parse::<u64>().is_ok_and(|id| id != 0 && format!("{id:020}") == text)
    };
    canonical(capsule_id) && canonical(generation)
}

fn is_canonical_temp_chronicle_name(filename: &str) -> bool {
    let Some(id_text) = filename
        .strip_prefix(TEMP_CHRONICLE_PREFIX)
        .and_then(|value| value.strip_suffix(TEMP_SEGMENT_SUFFIX))
    else {
        return false;
    };
    if id_text.len() != 20 || !id_text.bytes().all(|byte| byte.is_ascii_digit()) {
        return false;
    }
    id_text.parse::<u64>().is_ok_and(|id| id != 0 && format!("{id:020}") == id_text)
}

fn is_canonical_temp_segment_name(filename: &str) -> bool {
    let Some(id_text) = filename
        .strip_prefix(TEMP_SEGMENT_PREFIX)
        .and_then(|value| value.strip_suffix(TEMP_SEGMENT_SUFFIX))
    else {
        return false;
    };
    if id_text.len() != 20 || !id_text.bytes().all(|byte| byte.is_ascii_digit()) {
        return false;
    }
    id_text.parse::<u64>().is_ok_and(|id| {
        id != 0 && format!("{id:020}") == id_text
    })
}

#[cfg(unix)]
fn sync_directory(path: &Path) -> Result<(), DbError> {
    File::open(path)?.sync_all()?;
    Ok(())
}

#[cfg(not(unix))]
fn sync_directory(_path: &Path) -> Result<(), DbError> {
    // Portable std does not expose directory-entry flushing on all targets.
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_path() -> PathBuf {
        let nonce = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
        std::env::temp_dir().join(format!("ananta-publication-{nonce}"))
    }

    #[test]
    fn removes_only_canonical_regular_temp_segments() {
        let root = temp_path();
        let table = root.join("telemetry");
        fs::create_dir_all(&table).unwrap();
        let canonical = table.join(".segment-00000000000000000001.tmp");
        let unrelated = table.join(".segment-1.tmp");
        let published = table.join("segment-00000000000000000001.anf");
        let nested = table.join(".segment-00000000000000000002.tmp");
        fs::write(&canonical, b"interrupted").unwrap();
        fs::write(&unrelated, b"leave me").unwrap();
        fs::write(&published, b"published").unwrap();
        fs::create_dir(&nested).unwrap();

        let removed = cleanup_orphan_segment_temps(&root).unwrap();
        assert_eq!(removed, vec![canonical.clone()]);
        assert!(!canonical.exists());
        assert_eq!(fs::read(unrelated).unwrap(), b"leave me");
        assert_eq!(fs::read(published).unwrap(), b"published");
        assert!(nested.is_dir());
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn publication_checkpoints_cleanup_before_link_and_report_uncertainty_after_link() {
        let root = temp_path();
        fs::create_dir_all(&root).unwrap();
        let bytes = b"complete ANF payload";

        for failing_stage in [
            PublicationStage::TemporaryCreated,
            PublicationStage::BytesWritten,
            PublicationStage::TemporaryFileSynced,
        ] {
            let result = publish_segment_with_hook(&root, 1, bytes, |stage| {
                if stage == failing_stage {
                    Err(io::Error::other("injected interruption"))
                } else {
                    Ok(())
                }
            });
            assert!(matches!(result, Err(DbError::Io(_))));
            assert!(!root.join("segment-00000000000000000001.anf").exists());
            assert!(!root.join(".segment-00000000000000000001.tmp").exists());
        }

        for (index, failing_stage) in [
            PublicationStage::FinalNamePublished,
            PublicationStage::DirectorySyncedAfterPublish,
            PublicationStage::TemporaryNameRemoved,
            PublicationStage::DirectorySyncedAfterCleanup,
        ].into_iter().enumerate() {
            let segment_id = 2 + index as u64;
            let result = publish_segment_with_hook(&root, segment_id, bytes, |stage| {
                if stage == failing_stage {
                    Err(io::Error::new(io::ErrorKind::Other, "injected interruption"))
                } else {
                    Ok(())
                }
            });
            assert!(matches!(result, Err(DbError::SegmentPublicationUncertain { .. })));
            let final_path = root.join(format!("segment-{segment_id:020}.anf"));
            assert_eq!(fs::read(final_path).unwrap(), bytes);
            // Cleanup is safe after an uncertain post-link error whether or not
            // the temp name survived the injected checkpoint.
            cleanup_orphan_segment_temps(&root).unwrap();
        }
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn chronicle_publication_faults_are_classified_before_and_after_commit_link() {
        let root = temp_path();
        fs::create_dir_all(&root).unwrap();
        let bytes = b"complete Chronicle receipt and payload";

        for stage in [
            ChroniclePublicationStage::TemporaryCreated,
            ChroniclePublicationStage::BytesWritten,
            ChroniclePublicationStage::TemporaryFileSynced,
        ] {
            let result = publish_chronicle_with_hook(&root, 1, bytes, |current| {
                if current == stage { Err(io::Error::other("injected failure")) } else { Ok(()) }
            });
            assert!(matches!(result, Err(DbError::Io(_))));
            assert!(!root.join("segment-00000000000000000001.chr").exists());
            assert!(!root.join(".chronicle-00000000000000000001.tmp").exists());
        }

        for (index, stage) in [
            ChroniclePublicationStage::FinalNamePublished,
            ChroniclePublicationStage::DirectorySyncedAfterPublish,
            ChroniclePublicationStage::TemporaryNameRemoved,
            ChroniclePublicationStage::DirectorySyncedAfterCleanup,
        ].into_iter().enumerate() {
            let file_id = 2 + index as u64;
            let result = publish_chronicle_with_hook(&root, file_id, bytes, |current| {
                if current == stage { Err(io::Error::other("injected failure")) } else { Ok(()) }
            });
            assert!(matches!(result, Err(DbError::SegmentPublicationUncertain { .. })));
            let final_path = root.join(format!("segment-{file_id:020}.chr"));
            assert_eq!(fs::read(final_path).unwrap(), bytes);
            cleanup_orphan_segment_temps(&root).unwrap();
        }
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn final_path_collision_never_overwrites_published_data() {
        let root = temp_path();
        fs::create_dir_all(&root).unwrap();
        let final_path = root.join("segment-00000000000000000001.anf");
        fs::write(&final_path, b"original").unwrap();

        assert!(matches!(
            publish_segment(&root, 1, b"replacement"),
            Err(DbError::Io(_))
        ));
        assert_eq!(fs::read(final_path).unwrap(), b"original");
        assert!(!root.join(".segment-00000000000000000001.tmp").exists());
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn preexisting_temp_is_never_overwritten_or_removed() {
        let root = temp_path();
        fs::create_dir_all(&root).unwrap();
        let temp = root.join(".segment-00000000000000000001.tmp");
        fs::write(&temp, b"owned by someone else").unwrap();

        assert!(matches!(publish_segment(&root, 1, b"new"), Err(DbError::Io(_))));
        assert_eq!(fs::read(temp).unwrap(), b"owned by someone else");
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn database_lock_is_exclusive_and_released_on_drop() {
        let root = temp_path();
        fs::create_dir_all(&root).unwrap();
        let lock = DatabaseLock::acquire(&root).unwrap();
        assert!(matches!(
            DatabaseLock::acquire(&root),
            Err(DbError::DatabaseLocked(_))
        ));
        drop(lock);
        assert!(!root.join(".ananta.lock").exists());
        let second = DatabaseLock::acquire(&root).unwrap();
        drop(second);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn stale_database_lock_fails_closed() {
        let root = temp_path();
        fs::create_dir_all(&root).unwrap();
        let lock_path = root.join(".ananta.lock");
        fs::write(&lock_path, b"pid=unknown\\n").unwrap();
        assert!(matches!(
            DatabaseLock::acquire(&root),
            Err(DbError::DatabaseLocked(path)) if path == lock_path
        ));
        fs::remove_dir_all(root).unwrap();
    }
}
