use crate::chronicle::sha256;
use crate::compaction::CompactionJobId;
use crate::compaction_manifest::ManifestError;

const MAGIC: &[u8; 4] = b"OWM1";
const END: &[u8; 4] = b"OEND";
const VERSION: u16 = 1;
const HEADER_LEN: usize = 136;
const ARTIFACT_LEN: usize = 104;
const TRAILER_LEN: usize = 36;
const MAX_MANIFEST_BYTES: usize = 64 * 1024 * 1024;
const MAX_ARTIFACTS: usize = 250_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum WeaveLayoutProfile {
    AppendOnly = 0,
    TimeFirst = 1,
    EntityFirst = 2,
}

impl WeaveLayoutProfile {
    fn decode(value: u8) -> Result<Self, ManifestError> {
        match value {
            0 => Ok(Self::AppendOnly),
            1 => Ok(Self::TimeFirst),
            2 => Ok(Self::EntityFirst),
            _ => Err(ManifestError::Corrupt("unknown OWM1 layout profile")),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WeaveArtifactReference {
    pub ordinal: u32,
    pub data_file_len: u64,
    pub data_file_sha256: [u8; 32],
    pub row_count: u64,
    pub provenance_file_len: u64,
    pub provenance_file_sha256: [u8; 32],
    pub provenance_entry_count: u64,
}

impl WeaveArtifactReference {
    pub fn data_relative_path(&self) -> String {
        format!("data-{:08}.wvf", self.ordinal)
    }

    pub fn provenance_relative_path(&self) -> String {
        format!("provenance-{:08}.pvm", self.ordinal)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactionOutputManifest {
    pub table_id: u64,
    pub job_id: CompactionJobId,
    pub attempt: u64,
    pub weave_id: u64,
    pub generation: u64,
    pub input_manifest_sha256: [u8; 32],
    pub target_schema_revision: u64,
    pub first_sequence: u64,
    pub last_sequence: u64,
    pub layout: WeaveLayoutProfile,
    pub expected_row_count: u64,
    pub artifacts: Vec<WeaveArtifactReference>,
}

impl CompactionOutputManifest {
    pub fn validate(&self) -> Result<(), ManifestError> {
        if self.table_id == 0 || self.attempt == 0 || self.weave_id == 0 || self.generation == 0
            || self.input_manifest_sha256.iter().all(|b| *b == 0)
            || self.target_schema_revision == 0 || self.first_sequence == 0
            || self.last_sequence < self.first_sequence || self.expected_row_count == 0
            || self.artifacts.is_empty()
        {
            return Err(ManifestError::Invalid("missing candidate identity, source coverage or rows"));
        }
        if self.artifacts.len() > MAX_ARTIFACTS {
            return Err(ManifestError::Invalid("artifact count exceeds OWM1 format limit"));
        }

        let mut total_rows = 0_u64;
        for (index, artifact) in self.artifacts.iter().enumerate() {
            let ordinal = u32::try_from(index)
                .map_err(|_| ManifestError::Invalid("artifact ordinal exceeds u32"))?;
            if artifact.ordinal != ordinal || artifact.data_file_len == 0
                || artifact.data_file_sha256.iter().all(|b| *b == 0)
                || artifact.row_count == 0 || artifact.provenance_file_len == 0
                || artifact.provenance_file_sha256.iter().all(|b| *b == 0)
                || artifact.provenance_entry_count != artifact.row_count
            {
                return Err(ManifestError::Invalid("artifact identity, fingerprint, ordinal or provenance count is invalid"));
            }
            total_rows = total_rows.checked_add(artifact.row_count)
                .ok_or(ManifestError::Invalid("output artifact row total overflows"))?;
        }
        if total_rows != self.expected_row_count {
            return Err(ManifestError::Invalid("output artifact row total differs from expected row count"));
        }
        self.body_len()?;
        Ok(())
    }

    pub fn encode(&self) -> Result<Vec<u8>, ManifestError> {
        self.validate()?;
        let body_len = self.body_len()?;
        let mut out = Vec::with_capacity(body_len + TRAILER_LEN);
        out.extend_from_slice(MAGIC);
        out.extend_from_slice(&VERSION.to_le_bytes());
        out.extend_from_slice(&0_u16.to_le_bytes());
        out.extend_from_slice(&(HEADER_LEN as u32).to_le_bytes());
        out.extend_from_slice(&self.table_id.to_le_bytes());
        out.extend_from_slice(self.job_id.as_bytes());
        out.extend_from_slice(&self.attempt.to_le_bytes());
        out.extend_from_slice(&self.weave_id.to_le_bytes());
        out.extend_from_slice(&self.generation.to_le_bytes());
        out.extend_from_slice(&self.input_manifest_sha256);
        out.extend_from_slice(&self.target_schema_revision.to_le_bytes());
        out.extend_from_slice(&self.first_sequence.to_le_bytes());
        out.extend_from_slice(&self.last_sequence.to_le_bytes());
        out.push(self.layout as u8);
        out.extend_from_slice(&[0_u8; 3]);
        out.extend_from_slice(&self.expected_row_count.to_le_bytes());
        out.extend_from_slice(&(u32::try_from(self.artifacts.len())
            .map_err(|_| ManifestError::Invalid("artifact count exceeds u32"))?).to_le_bytes());
        out.extend_from_slice(&0_u32.to_le_bytes());
        if out.len() != HEADER_LEN {
            return Err(ManifestError::Invalid("OWM1 header layout mismatch"));
        }

        for artifact in &self.artifacts {
            out.extend_from_slice(&artifact.ordinal.to_le_bytes());
            out.extend_from_slice(&0_u32.to_le_bytes());
            out.extend_from_slice(&artifact.data_file_len.to_le_bytes());
            out.extend_from_slice(&artifact.data_file_sha256);
            out.extend_from_slice(&artifact.row_count.to_le_bytes());
            out.extend_from_slice(&artifact.provenance_file_len.to_le_bytes());
            out.extend_from_slice(&artifact.provenance_file_sha256);
            out.extend_from_slice(&artifact.provenance_entry_count.to_le_bytes());
        }
        if out.len() != body_len {
            return Err(ManifestError::Invalid("OWM1 body layout mismatch"));
        }
        let digest = sha256(&out);
        out.extend_from_slice(&digest);
        out.extend_from_slice(END);
        Ok(out)
    }

    pub fn fingerprint(&self) -> Result<[u8; 32], ManifestError> {
        let bytes = self.encode()?;
        Ok(sha256(&bytes[..bytes.len() - TRAILER_LEN]))
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, ManifestError> {
        if bytes.len() < HEADER_LEN + TRAILER_LEN || bytes.len() > MAX_MANIFEST_BYTES {
            return Err(ManifestError::Corrupt("OWM1 length is outside valid bounds"));
        }
        let body_len = bytes.len() - TRAILER_LEN;
        if &bytes[body_len + 32..] != END {
            return Err(ManifestError::Corrupt("OWM1 OEND marker mismatch"));
        }
        if bytes[body_len..body_len + 32] != sha256(&bytes[..body_len])[..] {
            return Err(ManifestError::Corrupt("OWM1 SHA-256 mismatch"));
        }

        let mut cursor = 0_usize;
        if take::<4>(bytes, &mut cursor)? != *MAGIC
            || take_u16(bytes, &mut cursor)? != VERSION
            || take_u16(bytes, &mut cursor)? != 0
            || take_u32(bytes, &mut cursor)? != HEADER_LEN as u32
        {
            return Err(ManifestError::Corrupt("OWM1 magic, version, flags or header length invalid"));
        }

        let table_id = take_u64(bytes, &mut cursor)?;
        let job_id = CompactionJobId::from_bytes(take::<16>(bytes, &mut cursor)?)
            .map_err(|_| ManifestError::Corrupt("OWM1 JobId is zero"))?;
        let attempt = take_u64(bytes, &mut cursor)?;
        let weave_id = take_u64(bytes, &mut cursor)?;
        let generation = take_u64(bytes, &mut cursor)?;
        let input_manifest_sha256 = take::<32>(bytes, &mut cursor)?;
        let target_schema_revision = take_u64(bytes, &mut cursor)?;
        let first_sequence = take_u64(bytes, &mut cursor)?;
        let last_sequence = take_u64(bytes, &mut cursor)?;
        let layout = WeaveLayoutProfile::decode(take::<1>(bytes, &mut cursor)?[0])?;
        if take::<3>(bytes, &mut cursor)? != [0_u8; 3] {
            return Err(ManifestError::Corrupt("OWM1 layout reserved bytes are nonzero"));
        }
        let expected_row_count = take_u64(bytes, &mut cursor)?;
        let artifact_count = usize::try_from(take_u32(bytes, &mut cursor)?)
            .map_err(|_| ManifestError::Corrupt("OWM1 artifact count overflows"))?;
        if take_u32(bytes, &mut cursor)? != 0 || cursor != HEADER_LEN
            || artifact_count == 0 || artifact_count > MAX_ARTIFACTS
        {
            return Err(ManifestError::Corrupt("OWM1 reserved header or artifact count invalid"));
        }
        let expected_body_len = HEADER_LEN.checked_add(
            artifact_count.checked_mul(ARTIFACT_LEN)
                .ok_or(ManifestError::Corrupt("OWM1 artifact bytes overflow"))?
        ).ok_or(ManifestError::Corrupt("OWM1 body length overflows"))?;
        if expected_body_len != body_len {
            return Err(ManifestError::Corrupt("OWM1 artifact count does not match record length"));
        }

        let mut artifacts = Vec::with_capacity(artifact_count);
        for _ in 0..artifact_count {
            let ordinal = take_u32(bytes, &mut cursor)?;
            if take_u32(bytes, &mut cursor)? != 0 {
                return Err(ManifestError::Corrupt("OWM1 artifact reserved bytes are nonzero"));
            }
            artifacts.push(WeaveArtifactReference {
                ordinal,
                data_file_len: take_u64(bytes, &mut cursor)?,
                data_file_sha256: take::<32>(bytes, &mut cursor)?,
                row_count: take_u64(bytes, &mut cursor)?,
                provenance_file_len: take_u64(bytes, &mut cursor)?,
                provenance_file_sha256: take::<32>(bytes, &mut cursor)?,
                provenance_entry_count: take_u64(bytes, &mut cursor)?,
            });
        }
        if cursor != body_len {
            return Err(ManifestError::Corrupt("unexpected bytes before OWM1 checksum"));
        }

        let manifest = Self {
            table_id, job_id, attempt, weave_id, generation, input_manifest_sha256,
            target_schema_revision, first_sequence, last_sequence, layout,
            expected_row_count, artifacts,
        };
        manifest.validate().map_err(|_| ManifestError::Corrupt("OWM1 identity or artifact invariants failed"))?;
        Ok(manifest)
    }

    fn body_len(&self) -> Result<usize, ManifestError> {
        let artifact_bytes = self.artifacts.len().checked_mul(ARTIFACT_LEN)
            .ok_or(ManifestError::Invalid("OWM1 artifact bytes overflow"))?;
        let body = HEADER_LEN.checked_add(artifact_bytes)
            .ok_or(ManifestError::Invalid("OWM1 body length overflows"))?;
        if body.checked_add(TRAILER_LEN).filter(|n| *n <= MAX_MANIFEST_BYTES).is_none() {
            return Err(ManifestError::Invalid("OWM1 exceeds 64 MiB limit"));
        }
        Ok(body)
    }
}

fn take<const N: usize>(bytes: &[u8], cursor: &mut usize) -> Result<[u8; N], ManifestError> {
    let end = (*cursor).checked_add(N).ok_or(ManifestError::Corrupt("OWM1 field offset overflows"))?;
    let slice = bytes.get(*cursor..end).ok_or(ManifestError::Corrupt("OWM1 field is truncated"))?;
    let mut out = [0_u8; N];
    out.copy_from_slice(slice);
    *cursor = end;
    Ok(out)
}
fn take_u16(bytes: &[u8], cursor: &mut usize) -> Result<u16, ManifestError> {
    Ok(u16::from_le_bytes(take::<2>(bytes, cursor)?))
}
fn take_u32(bytes: &[u8], cursor: &mut usize) -> Result<u32, ManifestError> {
    Ok(u32::from_le_bytes(take::<4>(bytes, cursor)?))
}
fn take_u64(bytes: &[u8], cursor: &mut usize) -> Result<u64, ManifestError> {
    Ok(u64::from_le_bytes(take::<8>(bytes, cursor)?))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> CompactionOutputManifest {
        CompactionOutputManifest {
            table_id: 7,
            job_id: CompactionJobId::from_bytes([5; 16]).unwrap(),
            attempt: 2,
            weave_id: 12,
            generation: 1,
            input_manifest_sha256: [9; 32],
            target_schema_revision: 3,
            first_sequence: 1,
            last_sequence: 3,
            layout: WeaveLayoutProfile::TimeFirst,
            expected_row_count: 6,
            artifacts: vec![
                WeaveArtifactReference {
                    ordinal: 0, data_file_len: 4096, data_file_sha256: [1; 32], row_count: 4,
                    provenance_file_len: 96, provenance_file_sha256: [2; 32], provenance_entry_count: 4,
                },
                WeaveArtifactReference {
                    ordinal: 1, data_file_len: 2048, data_file_sha256: [3; 32], row_count: 2,
                    provenance_file_len: 48, provenance_file_sha256: [4; 32], provenance_entry_count: 2,
                },
            ],
        }
    }

    #[test]
    fn owm1_round_trip_and_fingerprint_are_stable() {
        let manifest = sample();
        let encoded = manifest.encode().unwrap();
        let decoded = CompactionOutputManifest::decode(&encoded).unwrap();
        assert_eq!(decoded, manifest);
        assert_eq!(decoded.encode().unwrap(), encoded);
        assert_eq!(manifest.fingerprint().unwrap(), sha256(&encoded[..encoded.len() - TRAILER_LEN]));
    }

    #[test]
    fn derives_safe_artifact_paths_from_ordinals() {
        let artifact = &sample().artifacts[0];
        assert_eq!(artifact.data_relative_path(), "data-00000000.wvf");
        assert_eq!(artifact.provenance_relative_path(), "provenance-00000000.pvm");
    }

    #[test]
    fn rejects_row_count_provenance_and_ordinal_mismatches() {
        let mut wrong_rows = sample();
        wrong_rows.artifacts[1].row_count = 3;
        assert!(matches!(wrong_rows.validate(), Err(ManifestError::Invalid(_))));
        let mut wrong_provenance = sample();
        wrong_provenance.artifacts[0].provenance_entry_count = 3;
        assert!(matches!(wrong_provenance.validate(), Err(ManifestError::Invalid(_))));
        let mut wrong_ordinal = sample();
        wrong_ordinal.artifacts[1].ordinal = 3;
        assert!(matches!(wrong_ordinal.validate(), Err(ManifestError::Invalid(_))));
    }

    #[test]
    fn rejects_checksum_end_marker_layout_and_reserved_corruption() {
        let encoded = sample().encode().unwrap();
        let mut corrupt = encoded.clone();
        corrupt[100] ^= 1;
        assert_eq!(CompactionOutputManifest::decode(&corrupt).unwrap_err(),
            ManifestError::Corrupt("OWM1 SHA-256 mismatch"));
        let mut bad_end = encoded.clone();
        let last = bad_end.len() - 1;
        bad_end[last] ^= 1;
        assert_eq!(CompactionOutputManifest::decode(&bad_end).unwrap_err(),
            ManifestError::Corrupt("OWM1 OEND marker mismatch"));
        let mut bad_reserved = encoded;
        bad_reserved[117] = 1;
        let body_len = bad_reserved.len() - TRAILER_LEN;
        let digest = sha256(&bad_reserved[..body_len]);
        bad_reserved[body_len..body_len + 32].copy_from_slice(&digest);
        assert_eq!(CompactionOutputManifest::decode(&bad_reserved).unwrap_err(),
            ManifestError::Corrupt("OWM1 layout reserved bytes are nonzero"));
    }
}
