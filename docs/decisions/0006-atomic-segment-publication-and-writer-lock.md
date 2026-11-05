# ADR 0006: Atomic Segment Publication and Exclusive Local Writer

**Status:** Accepted for the experimental native storage core  
**Date:** 2026-11-05  
**Decision owners:** ANANTA maintainers

## Context

The previous insert path wrote a temporary segment, synced its contents and renamed it to the final `.anf` name. Rename semantics can replace an existing destination on common platforms. Segment IDs were derived from directory contents, but no database-handle lock prevented concurrent writers from selecting the same ID. Startup also did not remove interrupted temporary segment files.

A published segment must never be replaced by a retry or concurrent writer. A temporary or incomplete segment must never be read as committed data. A successful insert must make file contents durable before publishing the final name and persist the directory entry on platforms where directory syncing is supported.

## Decision

1. A `Database` handle acquires `.ananta.lock` through exclusive file creation and holds the lock for its lifetime. A second handle/process fails closed with `DatabaseLocked`.
2. The lock contains the process ID for operator diagnostics. It is removed on normal handle drop.
3. Stale locks are not automatically broken. After a crash, an operator must verify that no writer remains and remove the stale lock before reopening.
4. A segment is written to a canonical same-directory temporary name, created with `create_new`.
5. ANF bytes are written, flushed and synced before publication.
6. The final name is published using same-directory hard-link creation. This operation fails when the destination exists and never replaces it.
7. The containing directory is synced after final-name creation and again after temp-name removal on Unix.
8. Startup removes only canonical regular-file temporary segment names after it has acquired the database lock. Published `.anf` files and unrelated names are untouched.
9. A failure before final-name creation returns an ordinary error and best-effort removes the operation's temp file. A failure after final-name creation returns `SegmentPublicationUncertain` with the final path; callers must inspect the path before retrying.

## Consequences

- A second database handle cannot concurrently write through this local API.
- A crash can leave a stale lock, requiring deliberate operator recovery rather than unsafe automatic lock breaking.
- A crash before publication can leave a temp file that is cleaned during the next successful open.
- A crash after final link creation can leave both names; startup cleanup removes the temp name and retains the final segment.
- Filesystems without same-directory hard-link support fail closed. The implementation does not fall back to an overwrite-prone rename.
- Unix gets file and directory sync ordering. Non-Unix targets sync file contents but do not claim portable directory-entry flush guarantees.
- This does not provide multi-host locking, network filesystem guarantees, transactions, WAL or a general crash-recovery protocol.

## Validation

Fault-injection checkpoints exercise each publication boundary. Regression tests verify that existing final files and pre-existing temporary files are never overwritten, pre-publication temp files are cleaned, post-publication errors retain the final path and report uncertainty, startup removes only recognized temp files, and writer locks exclude a second handle and release on drop.
