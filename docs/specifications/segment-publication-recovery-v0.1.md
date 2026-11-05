# ANANTA Segment Publication and Recovery — M1.4.1

**Status:** M1.4 implementation contract  
**Scope:** local single-writer segment publication  
**ANF compatibility:** no changes to ANF v0.1 or v0.2 bytes

## 1. Segment states

A segment has two filesystem states in this increment:

- **Temporary:** `.segment-{id:020}.tmp`, created exclusively and not visible to scans.
- **Published:** `segment-{id:020}.anf`, a complete, checksummed ANF segment visible to scans.

The temporary name is never treated as committed data. This increment does not add deletion/compaction or an obsolete segment state.

## 2. Publication protocol

All operations occur in the table's data directory, so both names reside on the same filesystem.

1. Acquire the database's exclusive writer lock.
2. Encode and validate the complete ANF segment before touching the filesystem.
3. Create the temporary path with `create_new`; never truncate or overwrite a pre-existing temp file.
4. Write all bytes, flush the file handle and call `sync_all` on the temporary file.
5. Publish by creating a hard link from the temporary path to the final `.anf` path. Hard-link creation is atomic and fails if the final path already exists; it never replaces an existing segment.
6. Sync the containing directory on Unix platforms to persist the published name.
7. Remove the temporary name.
8. Sync the containing directory again on Unix platforms to persist temp-name removal.

A failure before step 5 leaves no published segment; the operation makes a best-effort removal of its own temp file. A failure after step 5 returns `SegmentPublicationUncertain` with the final path. Callers must not blindly retry such an operation: the final name may already be visible and must be inspected first.

If the filesystem does not support same-directory hard links, publication fails closed rather than falling back to an overwrite-prone rename.

## 3. Recovery

After acquiring the writer lock and loading the catalog, opening a database scans its `data/<table>/` directories and removes only regular files matching the exact canonical temporary-segment name pattern `.segment-{20 decimal digits}.tmp`.

- Published `.anf` files are never deleted or rewritten by recovery.
- Unknown files and non-regular paths are left untouched.
- Directory enumeration, metadata and cleanup errors are propagated.
- The table directory is synced after temporary-file cleanup on Unix.
- A process crash before publication leaves an ignored temp file that startup cleanup removes.
- A crash after hard-link publication may leave both names; startup removes the temp name and retains the published segment.
- A crash around the first directory sync may leave the final name's durability uncertain; this operation could not have returned success before that sync completed.

## 4. Exclusive writer boundary

Opening a database creates `.ananta.lock` using exclusive file creation. A second handle/process fails with `DatabaseLocked`. The lock is removed when the handle is dropped.

A process crash can leave a stale lock file. Open fails closed and reports its path; an operator must verify that no process owns the database and then remove the stale lock before reopening. ANANTA does not guess whether a PID is alive or automatically break a lock, because stale-lock removal could admit two writers. This is a conservative local-filesystem guard, not distributed locking or network-filesystem coordination.

## 5. Platform durability

- On Unix, file contents are synced before publication, then the containing directory is synced after adding the final name and after removing the temp name.
- On non-Unix targets, the file is synced and hard-link publication is used, but this implementation does not claim portable directory-entry flushing. Filesystem, OS and hardware write-cache guarantees still apply.
- The database is supported only for a single local filesystem writer; network filesystems and cross-host locking are not covered.

## 6. Validation

Fault-injection checkpoints cover temp creation, completed write, file sync, final-link publication, directory sync, temp removal and final directory sync. Tests verify pre-publication cleanup, no overwrite on collisions, uncertainty after publication, startup cleanup of canonical temp files, preservation of published and unrelated files, lock exclusion and lock release after drop.
