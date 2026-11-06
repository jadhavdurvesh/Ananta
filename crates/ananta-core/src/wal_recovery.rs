//! Typed mutation intents and recovery-plan construction for the ANWL log.
//!
//! This module does not publish files or mutate the database. It validates the
//! WAL payload contract and classifies committed versus incomplete transactions
//! so Database::open can apply the recovery protocol without reinterpreting
//! opaque payload bytes at every call site.

use crate::wal::{WalReadReport, WalRecordKind};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

const INTENT_MAGIC: &[u8; 4] = b"AMUT";
const INTENT_VERSION: u16 = 1;
const INTENT_LEN: usize = 4 + 2 + 1 + 8 + 8 + 8 + 8 + 32;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum WalArtifactKind {
    AnfSegment = 1,
    ChronicleRecord = 2,
}

impl WalArtifactKind {
    fn from_byte(value: u8) -> Result<Self, WalRecoveryError> {
        match value {
            1 => Ok(Self::AnfSegment),
            2 => Ok(Self::ChronicleRecord),
            _ => Err(WalRecoveryError::CorruptIntent("unknown artifact kind")),
        }
    }

    pub fn final_file_name(self, file_id: u64) -> String {
        match self {
            Self::AnfSegment => format!("segment-{file_id:020}.anf"),
            Self::ChronicleRecord => format!("segment-{file_id:020}.chr"),
        }
    }

    pub fn staging_file_name(self, file_id: u64) -> String {
        match self {
            Self::AnfSegment => format!(".segment-{file_id:020}.tmp"),
            Self::ChronicleRecord => format!(".chronicle-{file_id:020}.tmp"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalMutationIntent {
    pub artifact_kind: WalArtifactKind,
    pub table_id: u64,
    pub schema_revision: u64,
    pub file_id: u64,
    pub file_len: u64,
    pub sha256: [u8; 32],
}

impl WalMutationIntent {
    pub fn encode(&self) -> Result<Vec<u8>, WalRecoveryError> {
        self.validate()?;
        let mut bytes = Vec::with_capacity(INTENT_LEN);
        bytes.extend_from_slice(INTENT_MAGIC);
        bytes.extend_from_slice(&INTENT_VERSION.to_le_bytes());
        bytes.push(self.artifact_kind as u8);
        bytes.extend_from_slice(&self.table_id.to_le_bytes());
        bytes.extend_from_slice(&self.schema_revision.to_le_bytes());
        bytes.extend_from_slice(&self.file_id.to_le_bytes());
        bytes.extend_from_slice(&self.file_len.to_le_bytes());
        bytes.extend_from_slice(&self.sha256);
        Ok(bytes)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, WalRecoveryError> {
        if bytes.len() != INTENT_LEN || &bytes[..4] != INTENT_MAGIC {
            return Err(WalRecoveryError::CorruptIntent("invalid mutation intent header or length"));
        }
        let version = u16::from_le_bytes([bytes[4], bytes[5]]);
        if version != INTENT_VERSION {
            return Err(WalRecoveryError::UnsupportedIntentVersion(version));
        }
        let artifact_kind = WalArtifactKind::from_byte(bytes[6])?;
        let table_id = u64::from_le_bytes(bytes[7..15].try_into()
            .map_err(|_| WalRecoveryError::CorruptIntent("invalid TableId"))?);
        let schema_revision = u64::from_le_bytes(bytes[15..23].try_into()
            .map_err(|_| WalRecoveryError::CorruptIntent("invalid schema revision"))?);
        let file_id = u64::from_le_bytes(bytes[23..31].try_into()
            .map_err(|_| WalRecoveryError::CorruptIntent("invalid file ID"))?);
        let file_len = u64::from_le_bytes(bytes[31..39].try_into()
            .map_err(|_| WalRecoveryError::CorruptIntent("invalid file length"))?);
        let mut sha256 = [0_u8; 32];
        sha256.copy_from_slice(&bytes[39..71]);
        let intent = Self {
            artifact_kind,
            table_id,
            schema_revision,
            file_id,
            file_len,
            sha256,
        };
        intent.validate()?;
        Ok(intent)
    }

    fn validate(&self) -> Result<(), WalRecoveryError> {
        if self.table_id == 0 || self.schema_revision == 0 || self.file_id == 0 {
            return Err(WalRecoveryError::CorruptIntent(
                "TableId, schema revision and file ID must be non-zero",
            ));
        }
        if self.file_len == 0 {
            return Err(WalRecoveryError::CorruptIntent("immutable artifact cannot be empty"));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoveredWalTransaction {
    pub transaction_id: u64,
    pub intents: Vec<WalMutationIntent>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WalRecoveryPlan {
    /// Transactions with a valid COMMIT frame, in WAL order.
    pub committed: Vec<RecoveredWalTransaction>,
    /// Transactions with BEGIN but no COMMIT/ABORT marker at EOF.
    pub incomplete: Vec<RecoveredWalTransaction>,
}

impl WalRecoveryPlan {
    pub fn committed_intent_count(&self) -> usize {
        self.committed.iter().map(|transaction| transaction.intents.len()).sum()
    }

    pub fn incomplete_intent_count(&self) -> usize {
        self.incomplete.iter().map(|transaction| transaction.intents.len()).sum()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WalRecoveryError {
    CorruptIntent(&'static str),
    UnsupportedIntentVersion(u16),
    TransactionClassification(&'static str),
}

impl fmt::Display for WalRecoveryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CorruptIntent(message) => write!(formatter, "corrupt WAL mutation intent: {message}"),
            Self::UnsupportedIntentVersion(version) => {
                write!(formatter, "unsupported WAL mutation intent version {version}")
            }
            Self::TransactionClassification(message) => {
                write!(formatter, "invalid WAL recovery transaction classification: {message}")
            }
        }
    }
}

impl std::error::Error for WalRecoveryError {}

/// Classify a verified ANWL read report into committed redo work and incomplete
/// work whose staging artifacts may be discarded. The low-level WAL reader has
/// already validated frame checksum, sequence and transaction ordering.
pub fn build_recovery_plan(report: &WalReadReport) -> Result<WalRecoveryPlan, WalRecoveryError> {
    let incomplete_ids = report.incomplete_transaction_ids.iter().copied().collect::<BTreeSet<_>>();
    let mut transaction_intents: BTreeMap<u64, Vec<WalMutationIntent>> = BTreeMap::new();
    let mut transaction_order = Vec::new();
    let mut committed_ids = BTreeSet::new();
    let mut aborted_ids = BTreeSet::new();

    for record in &report.records {
        match record.kind {
            WalRecordKind::Begin => {
                transaction_order.push(record.transaction_id);
                transaction_intents.entry(record.transaction_id).or_default();
            }
            WalRecordKind::Mutation => {
                let intent = WalMutationIntent::decode(&record.payload)?;
                transaction_intents.get_mut(&record.transaction_id)
                    .ok_or(WalRecoveryError::TransactionClassification(
                        "MUTATION was not preceded by BEGIN",
                    ))?
                    .push(intent);
            }
            WalRecordKind::Commit => {
                committed_ids.insert(record.transaction_id);
            }
            WalRecordKind::Abort => {
                aborted_ids.insert(record.transaction_id);
            }
            WalRecordKind::Checkpoint => {}
        }
    }

    let mut plan = WalRecoveryPlan::default();
    for transaction_id in transaction_order {
        let intents = transaction_intents.remove(&transaction_id)
            .ok_or(WalRecoveryError::TransactionClassification(
                "BEGIN transaction lost its intent accumulator",
            ))?;
        if committed_ids.contains(&transaction_id) {
            plan.committed.push(RecoveredWalTransaction {
                transaction_id,
                intents,
            });
        } else if incomplete_ids.contains(&transaction_id) {
            plan.incomplete.push(RecoveredWalTransaction {
                transaction_id,
                intents,
            });
        } else if aborted_ids.contains(&transaction_id) {
            // Explicitly aborted transactions have no redo/discard work left
            // in this plan. Their staging files are handled by the abort path.
        } else {
            return Err(WalRecoveryError::TransactionClassification(
                "transaction is neither committed, aborted nor incomplete",
            ));
        }
    }

    if plan.incomplete.iter().any(|transaction| committed_ids.contains(&transaction.transaction_id)) {
        return Err(WalRecoveryError::TransactionClassification(
            "transaction is both committed and incomplete",
        ));
    }
    Ok(plan)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::{read_wal, WalSyncPolicy, WalWriter};
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn path(label: &str) -> PathBuf {
        let nonce = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
        std::env::temp_dir().join(format!("ananta-wal-plan-{label}-{nonce}.wal"))
    }

    fn intent(file_id: u64) -> WalMutationIntent {
        WalMutationIntent {
            artifact_kind: WalArtifactKind::AnfSegment,
            table_id: 4,
            schema_revision: 2,
            file_id,
            file_len: 128,
            sha256: [file_id as u8; 32],
        }
    }

    #[test]
    fn mutation_intent_round_trips_and_derives_only_canonical_file_names() {
        let original = intent(91);
        let bytes = original.encode().unwrap();
        assert_eq!(bytes.len(), INTENT_LEN);
        assert_eq!(WalMutationIntent::decode(&bytes).unwrap(), original);
        assert_eq!(original.artifact_kind.final_file_name(91), "segment-00000000000000000091.anf");
        assert_eq!(original.artifact_kind.staging_file_name(91), ".segment-00000000000000000091.tmp");

        let mut corrupt = bytes;
        corrupt[6] = 0xff;
        assert_eq!(
            WalMutationIntent::decode(&corrupt),
            Err(WalRecoveryError::CorruptIntent("unknown artifact kind")),
        );

        let mut zero_id = original;
        zero_id.file_id = 0;
        assert_eq!(
            zero_id.encode(),
            Err(WalRecoveryError::CorruptIntent("TableId, schema revision and file ID must be non-zero")),
        );
    }

    #[test]
    fn recovery_plan_separates_committed_and_incomplete_intents() {
        let path = path("classify");
        let mut writer = WalWriter::open(&path, WalSyncPolicy::Manual).unwrap();

        let committed = writer.next_sequence();
        writer.begin_transaction(committed, b"insert").unwrap();
        writer.append_mutation(committed, &intent(11).encode().unwrap()).unwrap();
        writer.commit_transaction(committed).unwrap();

        let incomplete = writer.next_sequence();
        writer.begin_transaction(incomplete, b"ingest").unwrap();
        writer.append_mutation(
            incomplete,
            &WalMutationIntent {
                artifact_kind: WalArtifactKind::ChronicleRecord,
                file_id: 12,
                ..intent(12)
            }.encode().unwrap(),
        ).unwrap();

        let aborted = writer.next_sequence();
        writer.begin_transaction(aborted, b"aborted").unwrap();
        writer.append_mutation(aborted, &intent(13).encode().unwrap()).unwrap();
        writer.abort_transaction(aborted, b"not committed").unwrap();
        drop(writer);

        let report = read_wal(&path).unwrap();
        let plan = build_recovery_plan(&report).unwrap();
        assert_eq!(plan.committed.len(), 1);
        assert_eq!(plan.committed[0].transaction_id, committed);
        assert_eq!(plan.committed_intent_count(), 1);
        assert_eq!(plan.incomplete.len(), 1);
        assert_eq!(plan.incomplete[0].transaction_id, incomplete);
        assert_eq!(plan.incomplete_intent_count(), 1);
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn recovery_plan_rejects_unknown_mutation_intent_versions() {
        let path = path("bad-version");
        let mut writer = WalWriter::open(&path, WalSyncPolicy::Manual).unwrap();
        let transaction = writer.next_sequence();
        writer.begin_transaction(transaction, b"future").unwrap();
        let mut bytes = intent(21).encode().unwrap();
        bytes[4..6].copy_from_slice(&99_u16.to_le_bytes());
        writer.append_mutation(transaction, &bytes).unwrap();
        writer.commit_transaction(transaction).unwrap();
        drop(writer);

        let report = read_wal(&path).unwrap();
        assert_eq!(
            build_recovery_plan(&report),
            Err(WalRecoveryError::UnsupportedIntentVersion(99)),
        );
        fs::remove_file(path).unwrap();
    }
}
