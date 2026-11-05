# ANANTA Native Format — ANF v0.2 Prototype Segment Layout

**Status:** Experimental implementation specification for M1.2.3.3  
**Magic:** `ANF1`  
**Version:** 2  
**Byte order:** Little-endian for fixed-width integers  
**Integrity:** FNV-1a 64-bit checksum for accidental corruption detection only; not cryptographic authentication  
**Maximum segment size:** 256 MiB

## 1. Purpose

ANF v0.2 retains the ANF v0.1 column payload encoding but adds stable identity metadata so the reader can resolve a segment against its exact historical table schema.

New writes use v0.2. Existing v0.1 segments remain readable as legacy segments against the immutable revision-1 schema snapshot. This is a prototype, not a stable compatibility guarantee.

## 2. Header

The v0.2 header is exactly 36 bytes.

| Offset | Size | Field |
|---:|---:|---|
| 0 | 4 | Magic: ASCII `ANF1` |
| 4 | 2 | Format version: u16, value 2 |
| 6 | 2 | Mandatory feature flags: u16, currently zero |
| 8 | 8 | Row count: u64 |
| 16 | 4 | Column count: u32 |
| 20 | 8 | Table ObjectId: u64, nonzero |
| 28 | 8 | Schema revision: u64, nonzero |

## 3. Column directory

Each directory record is 25 bytes. Records are emitted in the schema snapshot's logical column order.

| Entry offset | Size | Field |
|---:|---:|---|
| 0 | 1 | Logical type code |
| 1 | 8 | Stable Column ObjectId: u64, nonzero and unique within the segment |
| 9 | 8 | Absolute payload offset: u64 |
| 17 | 8 | Payload length: u64 |

Payload ranges are contiguous and ordered. The reader validates type code and ColumnId against the historical schema snapshot before decoding.

## 4. Payloads and footer

Payload encoding is unchanged from ANF v0.1:
- One null marker byte per value (0 = NULL; 1 = non-null).
- BOOLEAN uses one byte, 0 or 1.
- INT32 / INT64 / TIMESTAMP_MICROS use little-endian fixed-width values.
- FLOAT64 stores IEEE-754 bits as little-endian u64.
- UTF8 and BINARY use u32 byte length followed by payload bytes.

The final 12 bytes remain:
- FNV-1a 64-bit checksum over all preceding bytes, encoded little-endian.
- ASCII end marker `AEND`.

The checksum is for accidental corruption detection only, not tamper resistance.

## 5. Defensive limits

The v0.2 writer and reader use the existing ANF prototype guards:
- 256 MiB complete segment maximum.
- 1,000,000 rows maximum.
- 4,096 columns maximum.
- 1,000,000 decoded cells per full decode.
- 64 MiB per UTF8/BINARY field.

Column ObjectIds must be nonzero and unique. Table ObjectId and schema revision must be nonzero. Unknown mandatory flags, unsupported versions, malformed offsets and unsupported type codes are rejected.

## 6. Dual-version read path

The database reader inspects the version field and then:

### ANF v0.1
- The segment has no table ID, schema revision or ColumnIds.
- It is interpreted only using the table's immutable revision-1 ANS1 snapshot.
- Positional values are decoded using that snapshot's original column order and types.

### ANF v0.2
- Read Table ObjectId, schema revision and ordered ColumnIds.
- Verify table identity matches the catalog table.
- Verify the segment revision is not newer than the current table revision.
- Load the exact ANS1 snapshot identified by table ID + revision.
- Verify ordered ColumnIds and types match the snapshot.
- Decode values using that historical schema.

### Historical-to-current mapping

After either segment version is decoded, values are mapped into current logical column order by stable ColumnId:
- Existing IDs retain their values despite column rename/reorder.
- A current nullable column absent from an older snapshot is synthesized as NULL.
- A current non-nullable column absent from an older snapshot is an error.
- A same-ID type mismatch is an error.
- Missing/invalid snapshots and table identity mismatches fail closed.

The current scan API still materializes result rows. Bounded streaming and physical column-range I/O remain separate work.

## 7. Write behavior

New `Database::insert_batch` writes ANF v0.2 using the current catalog table ID, schema revision and ordered column IDs. A batch is validated against one catalog schema and is encoded into a single segment revision.

The initial schema snapshot (revision 1) is written before the catalog first publishes a newly created table. On opening older catalog-v2 databases, missing revision-1 snapshots are deterministically reconstructed from the persisted catalog schema. This allows pre-v0.2 installations and existing v0.1 segments to remain readable after upgrade.

## 8. Compatibility boundaries

- The v0.1 decoder remains supported and its on-disk format is unchanged.
- All new inserts now use v0.2.
- Existing v0.1 files are not rewritten.
- ANC1 catalog v2 and ANS1 snapshot v1 are unchanged.
- No DDL API is enabled by this work.
- Snapshot-first catalog publication for schema-changing DDL remains a future requirement.
- The current table data path remains `data/<table-name>/`.
- Table rename/drop, type changes and non-null column additions remain deferred.
- FNV-1a is not a cryptographic integrity/authentication mechanism.

## 9. Required validation

- v0.1 golden fixture remains byte-for-byte stable.
- v0.2 golden fixture remains byte-for-byte stable.
- v0.2 rejects zero/duplicate ColumnIds, zero table/revision, unsupported flags and invalid directory metadata.
- A segment with wrong table ID, schema revision or ColumnIds is rejected.
- New insert writes v0.2.
- Mixed v0.1 revision-1 and v0.2 current-revision segments return identical logical rows.
- Old v0.1 segments synthesize NULL only for newly added nullable columns.
- Mixed-version reads work after close/reopen.
- Projection over the current logical schema returns columns in requested order.
