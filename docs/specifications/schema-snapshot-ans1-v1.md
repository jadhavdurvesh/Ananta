# ANANTA Immutable Schema Snapshot Format — ANS1 v1

**Status:** Experimental implementation specification for M1.2.3.2  
**Magic:** `ANS1`  
**Version:** 1  
**Byte order:** Little-endian  
**Integrity:** FNV-1a 64-bit accidental-corruption checksum only  
**Maximum file size:** 64 MiB

## 1. Purpose

An immutable schema snapshot records the exact logical table schema for one stable table ObjectId and one schema revision. It preserves the stable ColumnIds, names, logical data types, nullability and column order needed to interpret older segments after later schema changes.

Snapshots are sidecar files. They do not change ANC1 catalog v2 or ANF v0.1 bytes.

## 2. Canonical path

```text
catalog/
  catalog.anc
  schemas/
    <table-object-id>/
      schema-<20-digit-revision>.ans
```

Example:
`catalog/schemas/42/schema-00000000000000000003.ans`

A snapshot path is immutable. Writing identical content to an existing revision is idempotent. Writing different content to an existing path is rejected.

## 3. Header

Exactly 28 bytes:

| Offset | Size | Field |
|---:|---:|---|
| 0 | 4 | Magic: ASCII `ANS1` |
| 4 | 2 | Snapshot format version: u16, value 1 |
| 6 | 2 | Mandatory feature flags: u16, currently zero |
| 8 | 8 | Table ObjectId: u64, nonzero |
| 16 | 8 | Schema revision: u64, nonzero |
| 24 | 4 | Column count: u32 |

## 4. Column records

Columns are stored in logical ordinal order.

| Field | Size |
|---|---:|
| Column ObjectId | u64 |
| Column name length | u32 |
| Column name | variable UTF-8 bytes |
| Data type code | u8 |
| Nullable flag | u8 |

The snapshot format preserves IDs and ordering. The reader rejects zero or duplicate ColumnIds, empty/duplicate names, invalid UTF-8, unknown type codes and nullability values other than zero or one.

Current defensive limits:
- 64 MiB maximum complete snapshot file.
- 1,000,000 columns maximum.
- 1 MiB maximum column-name byte length.

These are prototype limits, not permanent product promises.

## 5. Footer

The final 12 bytes contain:
- FNV-1a 64-bit checksum over all preceding bytes, encoded little-endian.
- ASCII end marker `SEND`.

FNV-1a detects accidental corruption only. It does not authenticate or prevent malicious modification.

## 6. Write and immutability rules

1. Validate table ID, schema revision, all ColumnIds and the logical schema.
2. Encode the complete snapshot.
3. Decode and validate the encoded bytes before publication.
4. Create the table-ID snapshot directory.
5. If the destination already exists, read it and accept only byte-equivalent logical content.
6. Create the `.ans.tmp` file with exclusive creation; never truncate or remove a pre-existing temp file.
7. Write all bytes and sync the temporary file.
8. Close the file and rename it to the immutable destination.
9. If publication fails, remove only the temporary file created by this attempt.

Same-directory rename and durability guarantees depend on the host filesystem/platform. Directory syncing and systematic crash fault-injection remain future hardening work.

## 7. Read rules

The reader:
- Checks file size before allocating the read buffer.
- Validates minimum size, magic, version and flags.
- Validates checksum and end marker.
- Bounds column count by both the configured maximum and remaining encoded bytes.
- Validates all IDs, names, types, nullability flags and schema uniqueness.
- Requires exact consumption of the body (no trailing bytes).
- Verifies that the decoded table ID and revision match the requested path identity.

A missing snapshot is an error. The reader must not guess a schema from the current catalog when a segment refers to a historical revision.

## 8. Current integration

The embedded database currently exposes:
- `Database::write_schema_snapshot(table)`
- `Database::read_schema_snapshot(table, revision)`

Writing a snapshot does not mutate the active catalog revision. It is currently an explicit operation; catalog-driven automatic snapshot publication will be wired in with schema evolution DDL after the segment resolver is implemented. `Database::check_schema_snapshot_consistency()` reports missing current snapshots, malformed/corrupt files, temporary files and unreferenced table/future-revision snapshots. Valid historical revisions below the current revision are retained because segment references are not yet encoded in ANF.

This snapshot file is not yet registered in a catalog history index. Its canonical key is the pair (table ObjectId, schema revision). A consistency checker/orphan scanner is planned before DDL is enabled.

## 9. Compatibility boundary

- ANC1 catalog v2 remains unchanged.
- ANF v0.1 segment files remain unchanged and do not yet embed table IDs or schema revisions.
- No table/column rename, drop, add-column or type-change operation is enabled by this snapshot implementation.
- ANF v0.2 and a dual-version segment resolver are separate M1.2.3.3 work.

## 10. Validation requirements

- Golden-byte fixture round trip.
- Snapshot round trip preserves all IDs and schema semantics.
- Read verifies path table ID and revision.
- Identical writes are idempotent; conflicting writes are rejected.
- Pre-existing temp files are never overwritten or removed.
- Corrupt checksum, flags, version, invalid names, duplicate IDs, invalid type/nullability and trailing bytes are rejected.
- Snapshot creation does not mutate the active catalog revision.
