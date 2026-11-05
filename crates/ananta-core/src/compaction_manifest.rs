use crate::chronicle::sha256;
use crate::compaction::CompactionJobId;
use std::collections::BTreeSet;
use std::fmt;

const MAGIC: &[u8; 4] = b"CIM1";
const END: &[u8; 4] = b"IEND";
const VERSION: u16 = 1;
const HEADER: usize = 88;
const CAPSULE_REF: usize = 88;
const RECEIPT_REF: usize = 104;
const TRAILER: usize = 36;
const MAX_BYTES: usize = 64 * 1024 * 1024;
const MAX_ITEMS: usize = 250_000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ManifestError {
    Invalid(&'static str),
    Corrupt(&'static str),
}

impl fmt::Display for ManifestError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Invalid(reason) => write!(f, "invalid compaction input manifest: {reason}"),
            Self::Corrupt(reason) => write!(f, "corrupt compaction input manifest: {reason}"),
        }
    }
}
impl std::error::Error for ManifestError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CapsuleSourceReference {
    pub capsule_id: u64,
    pub generation: u64,
    pub schema_revision: u64,
    pub first_sequence: u64,
    pub last_sequence: u64,
    pub row_count: u64,
    pub file_len: u64,
    pub file_sha256: [u8; 32],
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChronicleReceiptReference {
    pub sequence: u64,
    pub batch_id: [u8; 16],
    pub payload_sha256: [u8; 32],
    pub row_count: u64,
    pub record_file_len: u64,
    pub record_file_sha256: [u8; 32],
}

/// Immutable, canonical inventory of the exact Chronicle receipts and Capsules
/// selected for one compaction attempt. Paths are derived from stable IDs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactionInputManifest {
    pub table_id: u64,
    pub job_id: CompactionJobId,
    pub attempt: u64,
    pub target_schema_revision: u64,
    pub first_sequence: u64,
    pub last_sequence: u64,
    pub expected_row_count: u64,
    pub capsules: Vec<CapsuleSourceReference>,
    pub receipts: Vec<ChronicleReceiptReference>,
}

impl CompactionInputManifest {
    /// Structural validation only; the database preflight must verify the actual
    /// file lengths, SHA-256 values, decoded metadata and schema compatibility.
    pub fn validate(&self) -> Result<(), ManifestError> {
        if self.table_id == 0 || self.attempt == 0 || self.target_schema_revision == 0
            || self.first_sequence == 0 || self.last_sequence < self.first_sequence
            || self.expected_row_count == 0 || self.capsules.is_empty() || self.receipts.is_empty()
        {
            return Err(ManifestError::Invalid("missing identity, source coverage or rows"));
        }
        if self.capsules.len() > MAX_ITEMS || self.receipts.len() > MAX_ITEMS {
            return Err(ManifestError::Invalid("item count exceeds format limit"));
        }
        self.body_len()?;

        let sequence_count = self.last_sequence.checked_sub(self.first_sequence)
            .and_then(|n| n.checked_add(1))
            .ok_or(ManifestError::Invalid("source sequence interval overflows"))?;
        if sequence_count > MAX_ITEMS as u64 || sequence_count as usize != self.receipts.len() {
            return Err(ManifestError::Invalid("one receipt is required for every source sequence"));
        }

        let mut batch_ids = BTreeSet::new();
        let mut receipt_rows = 0_u64;
        for (index, receipt) in self.receipts.iter().enumerate() {
            let expected = self.first_sequence.checked_add(index as u64)
                .ok_or(ManifestError::Invalid("receipt sequence overflows"))?;
            if receipt.sequence != expected || receipt.batch_id.iter().all(|b| *b == 0)
                || receipt.payload_sha256.iter().all(|b| *b == 0)
                || receipt.row_count == 0 || receipt.record_file_len == 0
                || receipt.record_file_sha256.iter().all(|b| *b == 0)
            {
                return Err(ManifestError::Invalid("Chronicle receipt identity, fingerprint or sequence is invalid"));
            }
            if !batch_ids.insert(receipt.batch_id) {
                return Err(ManifestError::Invalid("duplicate Chronicle BatchId"));
            }
            receipt_rows = receipt_rows.checked_add(receipt.row_count)
                .ok_or(ManifestError::Invalid("total Chronicle rows overflow"))?;
        }
        if receipt_rows != self.expected_row_count {
            return Err(ManifestError::Invalid("Chronicle row total differs from expected row count"));
        }

        let mut capsule_ids = BTreeSet::new();
        let mut next_sequence = self.first_sequence;
        let mut capsule_rows = 0_u64;
        let mut reached_end = false;
        for capsule in &self.capsules {
            if reached_end || capsule.capsule_id == 0 || capsule.generation == 0
                || capsule.schema_revision == 0 || capsule.schema_revision > self.target_schema_revision
                || capsule.first_sequence != next_sequence || capsule.last_sequence < capsule.first_sequence
                || capsule.last_sequence > self.last_sequence || capsule.row_count == 0
                || capsule.file_len == 0 || capsule.file_sha256.iter().all(|b| *b == 0)
            {
                return Err(ManifestError::Invalid("Capsule identity, fingerprint or interval is invalid"));
            }
            if !capsule_ids.insert((capsule.capsule_id, capsule.generation)) {
                return Err(ManifestError::Invalid("duplicate Capsule identity/generation"));
            }
            let start = (capsule.first_sequence - self.first_sequence) as usize;
            let end = (capsule.last_sequence - self.first_sequence + 1) as usize;
            let rows = self.receipts.get(start..end)
                .ok_or(ManifestError::Invalid("Capsule range has no matching receipts"))?
                .iter().try_fold(0_u64, |sum, r| sum.checked_add(r.row_count))
                .ok_or(ManifestError::Invalid("Capsule receipt row total overflows"))?;
            if rows != capsule.row_count {
                return Err(ManifestError::Invalid("Capsule row count differs from its Chronicle receipt range"));
            }
            capsule_rows = capsule_rows.checked_add(capsule.row_count)
                .ok_or(ManifestError::Invalid("total Capsule rows overflow"))?;
            if capsule.last_sequence == self.last_sequence {
                reached_end = true;
            } else {
                next_sequence = capsule.last_sequence.checked_add(1)
                    .ok_or(ManifestError::Invalid("Capsule sequence interval overflows"))?;
            }
        }
        if !reached_end || capsule_rows != self.expected_row_count {
            return Err(ManifestError::Invalid("Capsule ranges do not tile the source interval or row total"));
        }
        Ok(())
    }

    pub fn encode(&self) -> Result<Vec<u8>, ManifestError> {
        self.validate()?;
        let body_len = self.body_len()?;
        let mut out = Vec::with_capacity(body_len + TRAILER);
        out.extend_from_slice(MAGIC);
        out.extend_from_slice(&VERSION.to_le_bytes());
        out.extend_from_slice(&0_u16.to_le_bytes());
        out.extend_from_slice(&(HEADER as u32).to_le_bytes());
        out.extend_from_slice(&self.table_id.to_le_bytes());
        out.extend_from_slice(self.job_id.as_bytes());
        out.extend_from_slice(&self.attempt.to_le_bytes());
        out.extend_from_slice(&self.target_schema_revision.to_le_bytes());
        out.extend_from_slice(&self.first_sequence.to_le_bytes());
        out.extend_from_slice(&self.last_sequence.to_le_bytes());
        out.extend_from_slice(&self.expected_row_count.to_le_bytes());
        out.extend_from_slice(&(self.capsules.len() as u32).to_le_bytes());
        out.extend_from_slice(&(self.receipts.len() as u32).to_le_bytes());
        out.extend_from_slice(&0_u32.to_le_bytes());
        if out.len() != HEADER {
            return Err(ManifestError::Invalid("CIM1 header layout mismatch"));
        }
        for c in &self.capsules {
            out.extend_from_slice(&c.capsule_id.to_le_bytes());
            out.extend_from_slice(&c.generation.to_le_bytes());
            out.extend_from_slice(&c.schema_revision.to_le_bytes());
            out.extend_from_slice(&c.first_sequence.to_le_bytes());
            out.extend_from_slice(&c.last_sequence.to_le_bytes());
            out.extend_from_slice(&c.row_count.to_le_bytes());
            out.extend_from_slice(&c.file_len.to_le_bytes());
            out.extend_from_slice(&c.file_sha256);
        }
        for r in &self.receipts {
            out.extend_from_slice(&r.sequence.to_le_bytes());
            out.extend_from_slice(&r.batch_id);
            out.extend_from_slice(&r.payload_sha256);
            out.extend_from_slice(&r.row_count.to_le_bytes());
            out.extend_from_slice(&r.record_file_len.to_le_bytes());
            out.extend_from_slice(&r.record_file_sha256);
        }
        if out.len() != body_len {
            return Err(ManifestError::Invalid("CIM1 body layout mismatch"));
        }
        let digest = sha256(&out);
        out.extend_from_slice(&digest);
        out.extend_from_slice(END);
        Ok(out)
    }

    pub fn fingerprint(&self) -> Result<[u8; 32], ManifestError> {
        let bytes = self.encode()?;
        Ok(sha256(&bytes[..bytes.len() - TRAILER]))
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, ManifestError> {
        if bytes.len() < HEADER + TRAILER || bytes.len() > MAX_BYTES {
            return Err(ManifestError::Corrupt("record length is outside valid bounds"));
        }
        let body_len = bytes.len() - TRAILER;
        if &bytes[body_len + 32..] != END {
            return Err(ManifestError::Corrupt("IEND marker mismatch"));
        }
        if bytes[body_len..body_len + 32] != sha256(&bytes[..body_len])[..] {
            return Err(ManifestError::Corrupt("SHA-256 mismatch"));
        }
        let mut p = 0_usize;
        if take::<4>(bytes, &mut p)? != *MAGIC || u16_at(bytes, &mut p)? != VERSION
            || u16_at(bytes, &mut p)? != 0 || u32_at(bytes, &mut p)? != HEADER as u32
        {
            return Err(ManifestError::Corrupt("magic, version, flags or header length invalid"));
        }
        let table_id = u64_at(bytes, &mut p)?;
        let job_id = CompactionJobId::from_bytes(take::<16>(bytes, &mut p)?)
            .map_err(|_| ManifestError::Corrupt("JobId is zero"))?;
        let attempt = u64_at(bytes, &mut p)?;
        let target_schema_revision = u64_at(bytes, &mut p)?;
        let first_sequence = u64_at(bytes, &mut p)?;
        let last_sequence = u64_at(bytes, &mut p)?;
        let expected_row_count = u64_at(bytes, &mut p)?;
        let capsule_count = u32_at(bytes, &mut p)? as usize;
        let receipt_count = u32_at(bytes, &mut p)? as usize;
        if u32_at(bytes, &mut p)? != 0 || p != HEADER
            || capsule_count > MAX_ITEMS || receipt_count > MAX_ITEMS
        {
            return Err(ManifestError::Corrupt("reserved header or item count invalid"));
        }
        let expected_len = HEADER.checked_add(capsule_count.checked_mul(CAPSULE_REF)
            .ok_or(ManifestError::Corrupt("Capsule list size overflows"))?)
            .and_then(|n| n.checked_add(receipt_count.checked_mul(RECEIPT_REF)?))
            .ok_or(ManifestError::Corrupt("receipt list size overflows"))?;
        if expected_len != body_len {
            return Err(ManifestError::Corrupt("item counts do not match record length"));
        }
        let mut capsules = Vec::with_capacity(capsule_count);
        for _ in 0..capsule_count {
            capsules.push(CapsuleSourceReference {
                capsule_id: u64_at(bytes, &mut p)?,
                generation: u64_at(bytes, &mut p)?,
                schema_revision: u64_at(bytes, &mut p)?,
                first_sequence: u64_at(bytes, &mut p)?,
                last_sequence: u64_at(bytes, &mut p)?,
                row_count: u64_at(bytes, &mut p)?,
                file_len: u64_at(bytes, &mut p)?,
                file_sha256: take::<32>(bytes, &mut p)?,
            });
        }
        let mut receipts = Vec::with_capacity(receipt_count);
        for _ in 0..receipt_count {
            receipts.push(ChronicleReceiptReference {
                sequence: u64_at(bytes, &mut p)?,
                batch_id: take::<16>(bytes, &mut p)?,
                payload_sha256: take::<32>(bytes, &mut p)?,
                row_count: u64_at(bytes, &mut p)?,
                record_file_len: u64_at(bytes, &mut p)?,
                record_file_sha256: take::<32>(bytes, &mut p)?,
            });
        }
        if p != body_len {
            return Err(ManifestError::Corrupt("unexpected bytes before checksum"));
        }
        let manifest = Self {
            table_id, job_id, attempt, target_schema_revision, first_sequence,
            last_sequence, expected_row_count, capsules, receipts,
        };
        manifest.validate().map_err(|_| ManifestError::Corrupt("identity or lineage invariants failed"))?;
        Ok(manifest)
    }

    fn body_len(&self) -> Result<usize, ManifestError> {
        let caps = self.capsules.len().checked_mul(CAPSULE_REF)
            .ok_or(ManifestError::Invalid("Capsule list size overflows"))?;
        let receipts = self.receipts.len().checked_mul(RECEIPT_REF)
            .ok_or(ManifestError::Invalid("receipt list size overflows"))?;
        let body = HEADER.checked_add(caps).and_then(|n| n.checked_add(receipts))
            .ok_or(ManifestError::Invalid("manifest body size overflows"))?;
        if body.checked_add(TRAILER).filter(|n| *n <= MAX_BYTES).is_none() {
            return Err(ManifestError::Invalid("manifest exceeds 64 MiB"));
        }
        Ok(body)
    }
}

fn take<const N: usize>(bytes: &[u8], p: &mut usize) -> Result<[u8; N], ManifestError> {
    let end = (*p).checked_add(N).ok_or(ManifestError::Corrupt("field offset overflows"))?;
    let slice = bytes.get(*p..end).ok_or(ManifestError::Corrupt("truncated field"))?;
    let mut out = [0_u8; N];
    out.copy_from_slice(slice);
    *p = end;
    Ok(out)
}
fn u16_at(b: &[u8], p: &mut usize) -> Result<u16, ManifestError> { Ok(u16::from_le_bytes(take::<2>(b, p)?)) }
fn u32_at(b: &[u8], p: &mut usize) -> Result<u32, ManifestError> { Ok(u32::from_le_bytes(take::<4>(b, p)?)) }
fn u64_at(b: &[u8], p: &mut usize) -> Result<u64, ManifestError> { Ok(u64::from_le_bytes(take::<8>(b, p)?)) }

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> CompactionInputManifest {
        CompactionInputManifest {
            table_id: 7,
            job_id: CompactionJobId::from_bytes([5; 16]).unwrap(),
            attempt: 2,
            target_schema_revision: 3,
            first_sequence: 1,
            last_sequence: 3,
            expected_row_count: 6,
            capsules: vec![
                CapsuleSourceReference { capsule_id: 10, generation: 1, schema_revision: 2,
                    first_sequence: 1, last_sequence: 2, row_count: 3, file_len: 2048, file_sha256: [1; 32] },
                CapsuleSourceReference { capsule_id: 11, generation: 1, schema_revision: 3,
                    first_sequence: 3, last_sequence: 3, row_count: 3, file_len: 1024, file_sha256: [2; 32] },
            ],
            receipts: vec![
                ChronicleReceiptReference { sequence: 1, batch_id: [10; 16], payload_sha256: [3; 32],
                    row_count: 2, record_file_len: 256, record_file_sha256: [4; 32] },
                ChronicleReceiptReference { sequence: 2, batch_id: [11; 16], payload_sha256: [5; 32],
                    row_count: 1, record_file_len: 256, record_file_sha256: [6; 32] },
                ChronicleReceiptReference { sequence: 3, batch_id: [12; 16], payload_sha256: [7; 32],
                    row_count: 3, record_file_len: 512, record_file_sha256: [8; 32] },
            ],
        }
    }

    #[test]
    fn cim1_round_trip_and_fingerprint_are_stable() {
        let m = sample();
        let bytes = m.encode().unwrap();
        assert_eq!(CompactionInputManifest::decode(&bytes).unwrap(), m);
        assert_eq!(m.fingerprint().unwrap(), sha256(&bytes[..bytes.len() - TRAILER]));
        assert_eq!(m.encode().unwrap(), bytes);
    }

    #[test]
    fn rejects_coverage_gaps_overlaps_and_row_mismatches() {
        let mut gap = sample();
        gap.capsules[1].first_sequence = 4;
        assert!(matches!(gap.validate(), Err(ManifestError::Invalid(_))));
        let mut overlap = sample();
        overlap.capsules[1].first_sequence = 2;
        assert!(matches!(overlap.validate(), Err(ManifestError::Invalid(_))));
        let mut wrong = sample();
        wrong.capsules[0].row_count = 4;
        assert!(matches!(wrong.validate(), Err(ManifestError::Invalid(_))));
    }

    #[test]
    fn rejects_duplicate_batches_and_corrupted_checksum() {
        let mut duplicate = sample();
        duplicate.receipts[1].batch_id = duplicate.receipts[0].batch_id;
        assert!(matches!(duplicate.validate(), Err(ManifestError::Invalid(_))));
        let mut bytes = sample().encode().unwrap();
        bytes[100] ^= 1;
        assert_eq!(CompactionInputManifest::decode(&bytes).unwrap_err(),
            ManifestError::Corrupt("SHA-256 mismatch"));
    }

    #[test]
    fn rejects_noncanonical_reserved_bytes_and_overflow() {
        let mut bytes = sample().encode().unwrap();
        bytes[87] = 1;
        let end = bytes.len() - TRAILER;
        bytes[end..end + 32].copy_from_slice(&sha256(&bytes[..end]));
        assert!(matches!(CompactionInputManifest::decode(&bytes), Err(ManifestError::Corrupt(_))));
        let mut invalid = sample();
        invalid.receipts[2].row_count = u64::MAX;
        assert!(matches!(invalid.validate(), Err(ManifestError::Invalid(_))));
    }
}
