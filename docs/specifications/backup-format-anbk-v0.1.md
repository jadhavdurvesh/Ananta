# ANANTA Backup Container ANBK v1

**Milestone:** M6.4 — operational integrity  
**Status:** Full-directory backup and fresh-directory restore APIs implemented. Subprocess interruption tests cover copied-file sync, manifest sync and final directory rename; execution validation is outstanding.

## 1. Purpose and scope

ANBK v1 is a native ANANTA backup container represented as a directory containing the exact database files plus one backup.anbk manifest. It does not depend on SQLite, PostgreSQL, ZIP, tar, or another database/storage engine.

A backup is a consistent, full copy of one locally owned database directory. It includes the catalog, immutable schema snapshots, ANF segments, Chronicle records, Capsules, Navigation Maps, Weave generations/manifests, compaction journals and the complete ANWL log. The live .ananta.lock file is excluded; a restored database acquires its own lock on open.

Incremental backup, remote backup transport, encryption, compression and point-in-time recovery from a backup plus later WAL archives are not part of v1.

## 2. Manifest encoding

All integer fields are little-endian.

| Field | Size | Meaning |
|---|---:|---|
| Magic | 4 | ANBK |
| Version | 2 | 1 |
| Reserved | 2 | Must be zero |
| Entry count | 4 | Number of file entries |
| Repeated file entry | variable | Path length, UTF-8 relative path, byte length, SHA-256 |
| Manifest checksum | 32 | SHA-256 over all preceding manifest bytes |
| End marker | 4 | BKND |

Each entry stores a u32 path length, UTF-8 relative path bytes, u64 file length and a 32-byte SHA-256 of the complete file. Paths must be relative, contain only normal path components, and cannot escape the backup root. Duplicate entries, unknown versions, oversized manifests, truncated entries, extra/unlisted files and checksum mismatches are rejected.

The format currently bounds the manifest to 64 MiB, the file inventory to one million entries and each path to 4096 bytes.

## 3. Backup protocol

1. The source Database handle must be operational and owns the exclusive writer lock.
2. Run the bounded consistency checker over the current WAL, logical table scans, Chronicle receipt inventory, Capsules and Navigation Maps.
3. Refuse backup while staging/temp files or directories remain. Inspect the storage inventory and resolve the condition first.
4. Recursively copy the database tree without following symbolic links. Sync copied files and destination directories.
5. Write and sync backup.anbk, then rename the completed staging directory to the requested new destination and sync its parent directory.
6. Never overwrite an existing backup destination.

The API is Database::backup_to(path) -> BackupReport. The report includes the destination, file count, total payload bytes and manifest SHA-256. The report's payload-byte count excludes the manifest itself.

## 4. Restore protocol

1. Verify the manifest header, version, bounded size, checksum, canonical paths, duplicate-free inventory and exact file inventory.
2. Verify every listed file is a regular file with the expected size and SHA-256.
3. Copy verified files to a sibling staging directory under a new destination.
4. Sync files/directories, then publish the destination directory.
5. Open the restored database with the normal Database::open recovery path and run the consistency checker before returning success.

The API is Database::restore_backup(backup_path, new_database_path) -> BackupReport. Existing destinations and source/destination containment are rejected.

## 5. Integrity and repair boundaries

Database::check_consistency(batch_rows) performs bounded-memory scans and validates:
- ANWL frame/transaction structure and absence of an incomplete/truncated WAL tail.
- Logical table rows through the normal storage scan path.
- Chronicle framing, checksums, table identity, BatchId uniqueness and sequence reconstruction.
- Capsule binary validation and recomputed statistics.
- Navigation Map binary identity and every referenced Capsule's regular-file type, exact length, SHA-256 and decoded metadata.
- The storage inventory, including leftover temporary files/directories.

Database::storage_inventory() reports relative paths, byte lengths, artifact classes, total bytes and temporary/staging paths. It never deletes files.

ANANTA does not attempt automatic repair of conflicting committed artifacts, checksum failures, unknown file formats or missing committed WAL dependencies. These conditions fail closed. Recovery of a committed publication is allowed only through its validated WAL intent. For other corruption, preserve the original database, restore a separately verified backup into a new directory, and compare consistency reports before changing operational pointers. Manual deletion or rewriting of committed files is not a supported repair method.

## 6. Security and limitations

SHA-256 detects accidental corruption and binds manifest entries to file contents; it is not authentication or a digital signature. ANBK v1 has no encryption or access-control layer: protect the backup directory using operating-system permissions and trusted storage. Backups reject symbolic links and do not preserve ownership/ACL metadata or special filesystem attributes. Directory syncing is implemented on Unix; portable standard-library directory-entry flushing is not available on all targets.

The current implementation is a full local directory backup. Subprocess tests stop after a copied file is synced, after the manifest is synced, and after final directory rename before its parent sync. The incomplete cases must leave the requested destination absent; the post-rename case must verify and restore successfully. These tests have not been run in the current development environment. Physical power-loss validation, Windows-specific durability validation, external destination media, and large-scale backup performance tests remain outstanding.
