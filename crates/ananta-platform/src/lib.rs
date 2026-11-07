//! Small, audited operating-system primitives used by ANANTA storage.
//!
//! This crate deliberately isolates the Windows FFI boundary so the database
//! and storage-engine crates can keep their `unsafe_code = "forbid"` policy.

use std::io;
use std::path::Path;

#[cfg(windows)]
fn move_file_ex(source: &Path, destination: &Path, flags: u32) -> io::Result<()> {
    use std::os::windows::ffi::OsStrExt;

    #[link(name = "kernel32")]
    extern "system" {
        #[link_name = "MoveFileExW"]
        fn move_file_ex_w(
            existing_file_name: *const u16,
            new_file_name: *const u16,
            flags: u32,
        ) -> i32;
    }

    let source_wide = source
        .as_os_str()
        .encode_wide()
        .chain(std::iter::once(0))
        .collect::<Vec<_>>();
    let destination_wide = destination
        .as_os_str()
        .encode_wide()
        .chain(std::iter::once(0))
        .collect::<Vec<_>>();
    // SAFETY: both arguments are valid, NUL-terminated UTF-16 buffers that
    // remain alive for the duration of the synchronous Win32 call.
    let result = unsafe {
        move_file_ex_w(
            source_wide.as_ptr(),
            destination_wide.as_ptr(),
            flags,
        )
    };
    if result != 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

/// Atomically replace `destination` with `source`.
///
/// The files must be on the same volume. On Unix this uses rename semantics.
/// On Windows it uses MoveFileExW with REPLACE_EXISTING and WRITE_THROUGH;
/// it never emulates replacement by deleting the destination first.
pub fn atomic_replace(source: &Path, destination: &Path) -> io::Result<()> {
    #[cfg(unix)]
    {
        std::fs::rename(source, destination)
    }

    #[cfg(windows)]
    {
        const MOVEFILE_REPLACE_EXISTING: u32 = 0x0000_0001;
        const MOVEFILE_WRITE_THROUGH: u32 = 0x0000_0008;
        move_file_ex(
            source,
            destination,
            MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH,
        )
    }

    #[cfg(not(any(unix, windows)))]
    {
        let _ = (source, destination);
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "atomic file replacement is unsupported on this target",
        ))
    }
}

/// Move a file or directory to a new, previously unused name on the same
/// volume. Windows uses MoveFileExW with WRITE_THROUGH; this is suitable for
/// publishing completed backup/restore directory trees without a delete gap.
pub fn atomic_move(source: &Path, destination: &Path) -> io::Result<()> {
    #[cfg(unix)]
    {
        std::fs::rename(source, destination)
    }

    #[cfg(windows)]
    {
        const MOVEFILE_WRITE_THROUGH: u32 = 0x0000_0008;
        move_file_ex(source, destination, MOVEFILE_WRITE_THROUGH)
    }

    #[cfg(not(any(unix, windows)))]
    {
        let _ = (source, destination);
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "atomic move is unsupported on this target",
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::{atomic_move, atomic_replace};
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn replaces_existing_target_and_consumes_source() {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "ananta-platform-{}-{nonce}",
            std::process::id(),
        ));
        fs::create_dir_all(&root).unwrap();
        let source = root.join("replacement.tmp");
        let destination = root.join("active.wal");
        fs::write(&source, b"new durable contents").unwrap();
        fs::write(&destination, b"old durable contents").unwrap();

        atomic_replace(&source, &destination).unwrap();

        assert_eq!(fs::read(&destination).unwrap(), b"new durable contents");
        assert!(!source.exists());

        let tree = root.join("backup.staging");
        let published = root.join("backup");
        fs::create_dir_all(&tree).unwrap();
        fs::write(tree.join("manifest.anbk"), b"verified backup").unwrap();
        atomic_move(&tree, &published).unwrap();
        assert!(!tree.exists());
        assert_eq!(
            fs::read(published.join("manifest.anbk")).unwrap(),
            b"verified backup",
        );
        fs::remove_dir_all(root).unwrap();
    }
}
