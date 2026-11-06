use crate::DbError;
use std::collections::BTreeMap;
use std::fs::File;
use std::io::{self, Cursor, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

const MAGIC: &[u8; 4] = b"CHR1";
const END: &[u8; 4] = b"CEND";
const VERSION: u16 = 1;
const HEADER_LEN: usize = 96;
const TRAILER_LEN: usize = 12;
const IO_BUFFER: usize = 64 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BatchId(pub(crate) [u8; 16]);

impl BatchId {
    pub fn from_bytes(bytes: [u8; 16]) -> Result<Self, DbError> {
        if bytes.iter().all(|byte| *byte == 0) {
            return Err(DbError::InvalidBatchId);
        }
        Ok(Self(bytes))
    }

    pub fn from_hex(value: &str) -> Result<Self, DbError> {
        if value.len() != 32 || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Err(DbError::InvalidBatchId);
        }
        let mut bytes = [0_u8; 16];
        for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
            let high = hex_nibble(pair[0]).ok_or(DbError::InvalidBatchId)?;
            let low = hex_nibble(pair[1]).ok_or(DbError::InvalidBatchId)?;
            bytes[index] = (high << 4) | low;
        }
        Self::from_bytes(bytes)
    }

    pub fn as_bytes(&self) -> &[u8; 16] { &self.0 }

    pub fn to_hex(self) -> String {
        const HEX: &[u8; 16] = b"0123456789abcdef";
        let mut output = String::with_capacity(32);
        for byte in self.0 {
            output.push(HEX[(byte >> 4) as usize] as char);
            output.push(HEX[(byte & 0x0f) as usize] as char);
        }
        output
    }
}

impl std::fmt::Display for BatchId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.to_hex())
    }
}

fn hex_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IngestOutcome { Accepted, Duplicate }

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IngestResult {
    pub outcome: IngestOutcome,
    pub batch_id: BatchId,
    pub table_id: u64,
    pub ingestion_sequence: u64,
    pub row_count: u64,
    pub encoded_bytes: u64,
    pub chronicle_path: PathBuf,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct IngestionStats {
    pub accepted_batches: u64,
    pub accepted_rows: u64,
    pub accepted_encoded_bytes: u64,
    pub duplicate_requests: u64,
    pub rejected_requests: u64,
    pub publication_failures: u64,
    pub uncertain_publications: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ChronicleMetadata {
    pub table_id: u64,
    pub batch_id: BatchId,
    pub sequence: u64,
    pub schema_revision: u64,
    pub row_count: u64,
    pub payload_len: u64,
    pub fingerprint: [u8; 32],
}

#[derive(Debug, Clone)]
pub(crate) struct BatchReceipt {
    pub metadata: ChronicleMetadata,
    pub path: PathBuf,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct TableChronicleIndex {
    pub by_batch_id: BTreeMap<BatchId, BatchReceipt>,
    pub last_sequence: u64,
    pub stats: IngestionStats,
}

impl TableChronicleIndex {
    pub fn next_sequence(&self) -> Result<u64, DbError> {
        self.last_sequence.checked_add(1).ok_or(DbError::LengthOverflow)
    }

    pub fn record(&mut self, receipt: BatchReceipt) -> Result<(), DbError> {
        if receipt.metadata.sequence != self.next_sequence()? {
            return Err(DbError::ChronicleCorrupt("ingestion sequence is not contiguous"));
        }
        if self.by_batch_id.contains_key(&receipt.metadata.batch_id) {
            return Err(DbError::ChronicleCorrupt("duplicate BatchId receipt"));
        }
        self.last_sequence = receipt.metadata.sequence;
        self.stats.accepted_batches = self.stats.accepted_batches.checked_add(1).ok_or(DbError::LengthOverflow)?;
        self.stats.accepted_rows = self.stats.accepted_rows.checked_add(receipt.metadata.row_count).ok_or(DbError::LengthOverflow)?;
        self.stats.accepted_encoded_bytes = self.stats.accepted_encoded_bytes.checked_add(receipt.metadata.payload_len).ok_or(DbError::LengthOverflow)?;
        self.by_batch_id.insert(receipt.metadata.batch_id, receipt);
        Ok(())
    }
}

pub(crate) fn encode_record(
    table_id: u64,
    batch_id: BatchId,
    sequence: u64,
    schema_revision: u64,
    row_count: u64,
    payload: &[u8],
) -> Result<(Vec<u8>, ChronicleMetadata), DbError> {
    if table_id == 0 || sequence == 0 || schema_revision == 0 || row_count == 0 {
        return Err(DbError::ChronicleCorrupt("zero table, sequence, schema revision or row count"));
    }
    if payload.len() > ananta_format::MAX_SEGMENT_BYTES {
        return Err(DbError::LengthOverflow);
    }
    let fingerprint = sha256(payload);
    let metadata = ChronicleMetadata {
        table_id,
        batch_id,
        sequence,
        schema_revision,
        row_count,
        payload_len: u64::try_from(payload.len()).map_err(|_| DbError::LengthOverflow)?,
        fingerprint,
    };
    let mut bytes = Vec::with_capacity(HEADER_LEN + payload.len() + TRAILER_LEN);
    bytes.extend_from_slice(MAGIC);
    bytes.extend_from_slice(&VERSION.to_le_bytes());
    bytes.extend_from_slice(&0_u16.to_le_bytes());
    bytes.extend_from_slice(&table_id.to_le_bytes());
    bytes.extend_from_slice(batch_id.as_bytes());
    bytes.extend_from_slice(&sequence.to_le_bytes());
    bytes.extend_from_slice(&schema_revision.to_le_bytes());
    bytes.extend_from_slice(&row_count.to_le_bytes());
    bytes.extend_from_slice(&metadata.payload_len.to_le_bytes());
    bytes.extend_from_slice(&fingerprint);
    debug_assert_eq!(bytes.len(), HEADER_LEN);
    bytes.extend_from_slice(payload);
    let checksum = checksum64(&bytes);
    bytes.extend_from_slice(&checksum.to_le_bytes());
    bytes.extend_from_slice(END);
    Ok((bytes, metadata))
}

pub(crate) fn inspect_file(
    path: &Path,
    expected_table_id: u64,
) -> Result<(ChronicleMetadata, u64), DbError> {
    let mut file = File::open(path)?;
    inspect_reader(&mut file, expected_table_id)
}

/// Verifies Chronicle framing, whole-record checksum and payload SHA-256 in a
/// bounded-memory streaming pass. Returns metadata and the embedded ANF offset.
pub(crate) fn inspect_reader<R: Read + Seek>(
    reader: &mut R,
    expected_table_id: u64,
) -> Result<(ChronicleMetadata, u64), DbError> {
    let file_len = reader.seek(SeekFrom::End(0))?;
    if file_len < (HEADER_LEN + TRAILER_LEN) as u64 {
        return Err(DbError::ChronicleCorrupt("record is truncated"));
    }
    reader.seek(SeekFrom::Start(0))?;
    let mut header = [0_u8; HEADER_LEN];
    reader.read_exact(&mut header)?;
    if &header[..4] != MAGIC {
        return Err(DbError::ChronicleCorrupt("invalid magic"));
    }
    if u16::from_le_bytes([header[4], header[5]]) != VERSION {
        return Err(DbError::ChronicleCorrupt("unsupported version"));
    }
    if header[6] != 0 || header[7] != 0 {
        return Err(DbError::ChronicleCorrupt("unknown mandatory feature flags"));
    }
    let table_id = u64::from_le_bytes(header[8..16].try_into().map_err(|_| DbError::LengthOverflow)?);
    let mut batch_bytes = [0_u8; 16];
    batch_bytes.copy_from_slice(&header[16..32]);
    let batch_id = BatchId::from_bytes(batch_bytes)?;
    let sequence = u64::from_le_bytes(header[32..40].try_into().map_err(|_| DbError::LengthOverflow)?);
    let schema_revision = u64::from_le_bytes(header[40..48].try_into().map_err(|_| DbError::LengthOverflow)?);
    let row_count = u64::from_le_bytes(header[48..56].try_into().map_err(|_| DbError::LengthOverflow)?);
    let payload_len = u64::from_le_bytes(header[56..64].try_into().map_err(|_| DbError::LengthOverflow)?);
    let mut expected_fingerprint = [0_u8; 32];
    expected_fingerprint.copy_from_slice(&header[64..96]);
    if table_id != expected_table_id || table_id == 0 {
        return Err(DbError::ChronicleCorrupt("table identity mismatch"));
    }
    if sequence == 0 || schema_revision == 0 || row_count == 0 || payload_len == 0 {
        return Err(DbError::ChronicleCorrupt("zero sequence, revision, row count or payload length"));
    }
    if payload_len > ananta_format::MAX_SEGMENT_BYTES as u64 {
        return Err(DbError::ChronicleCorrupt("payload exceeds ANF size limit"));
    }
    let trailer_offset = (HEADER_LEN as u64).checked_add(payload_len).ok_or(DbError::LengthOverflow)?;
    if trailer_offset.checked_add(TRAILER_LEN as u64).ok_or(DbError::LengthOverflow)? != file_len {
        return Err(DbError::ChronicleCorrupt("payload length does not match file size"));
    }

    reader.seek(SeekFrom::Start(trailer_offset))?;
    let mut trailer = [0_u8; TRAILER_LEN];
    reader.read_exact(&mut trailer)?;
    if &trailer[8..] != END {
        return Err(DbError::ChronicleCorrupt("invalid end marker"));
    }
    let expected_checksum = u64::from_le_bytes(trailer[..8].try_into().map_err(|_| DbError::LengthOverflow)?);

    reader.seek(SeekFrom::Start(0))?;
    let mut remaining = trailer_offset;
    let mut buffer = [0_u8; IO_BUFFER];
    let mut checksum = 0xcbf29ce484222325_u64;
    let mut payload_hash = Sha256::new();
    let mut payload_position = 0_u64;
    while remaining > 0 {
        let take = usize::try_from(remaining.min(IO_BUFFER as u64)).map_err(|_| DbError::LengthOverflow)?;
        reader.read_exact(&mut buffer[..take])?;
        for byte in &buffer[..take] {
            checksum ^= u64::from(*byte);
            checksum = checksum.wrapping_mul(0x100000001b3);
        }
        let chunk_start = payload_position;
        let chunk_end = chunk_start + take as u64;
        let payload_start = HEADER_LEN as u64;
        let start = chunk_start.max(payload_start);
        let end = chunk_end.min(trailer_offset);
        if start < end {
            let local_start = usize::try_from(start - chunk_start).map_err(|_| DbError::LengthOverflow)?;
            let local_end = usize::try_from(end - chunk_start).map_err(|_| DbError::LengthOverflow)?;
            payload_hash.update(&buffer[local_start..local_end]);
        }
        payload_position = chunk_end;
        remaining -= take as u64;
    }
    if checksum != expected_checksum {
        return Err(DbError::ChronicleCorrupt("record checksum mismatch"));
    }
    if payload_hash.finalize() != expected_fingerprint {
        return Err(DbError::ChronicleCorrupt("payload fingerprint mismatch"));
    }

    // ANF v0.2 identity must agree with the receipt header before any scan is exposed.
    if payload_len < 36 {
        return Err(DbError::ChronicleCorrupt("embedded ANF header is truncated"));
    }
    reader.seek(SeekFrom::Start(HEADER_LEN as u64))?;
    let mut anf_header = [0_u8; 36];
    reader.read_exact(&mut anf_header)?;
    if &anf_header[..4] != b"ANF1"
        || u16::from_le_bytes([anf_header[4], anf_header[5]]) != 2
        || u64::from_le_bytes(anf_header[8..16].try_into().map_err(|_| DbError::LengthOverflow)?) != row_count
        || u64::from_le_bytes(anf_header[20..28].try_into().map_err(|_| DbError::LengthOverflow)?) != table_id
        || u64::from_le_bytes(anf_header[28..36].try_into().map_err(|_| DbError::LengthOverflow)?) != schema_revision
    {
        return Err(DbError::ChronicleCorrupt("embedded ANF identity mismatch"));
    }

    Ok((ChronicleMetadata {
        table_id, batch_id, sequence, schema_revision, row_count, payload_len,
        fingerprint: expected_fingerprint,
    }, HEADER_LEN as u64))
}


pub(crate) fn extract_payload(
    bytes: &[u8],
    expected_table_id: u64,
) -> Result<(ChronicleMetadata, Vec<u8>), DbError> {
    let mut cursor = Cursor::new(bytes);
    let (metadata, offset) = inspect_reader(&mut cursor, expected_table_id)?;
    let start = usize::try_from(offset).map_err(|_| DbError::LengthOverflow)?;
    let length = usize::try_from(metadata.payload_len).map_err(|_| DbError::LengthOverflow)?;
    let end = start.checked_add(length).ok_or(DbError::LengthOverflow)?;
    let payload = bytes.get(start..end).ok_or(DbError::ChronicleCorrupt("payload range is truncated"))?;
    Ok((metadata, payload.to_vec()))
}

pub(crate) struct PayloadReader<R> {
    inner: R,
    start: u64,
    length: u64,
    position: u64,
}

impl<R: Read + Seek> PayloadReader<R> {
    pub fn new(inner: R, start: u64, length: u64) -> Self {
        Self { inner, start, length, position: 0 }
    }
}

impl<R: Read + Seek> Read for PayloadReader<R> {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        if self.position >= self.length || output.is_empty() { return Ok(0); }
        let remaining = self.length - self.position;
        let take = output.len().min(usize::try_from(remaining).unwrap_or(usize::MAX));
        self.inner.seek(SeekFrom::Start(self.start + self.position))?;
        let count = self.inner.read(&mut output[..take])?;
        self.position += count as u64;
        Ok(count)
    }
}

impl<R: Read + Seek> Seek for PayloadReader<R> {
    fn seek(&mut self, position: SeekFrom) -> io::Result<u64> {
        let next = match position {
            SeekFrom::Start(offset) => i128::from(offset),
            SeekFrom::Current(offset) => i128::from(self.position) + i128::from(offset),
            SeekFrom::End(offset) => i128::from(self.length) + i128::from(offset),
        };
        if next < 0 || next > i128::from(self.length) {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "seek outside Chronicle payload"));
        }
        self.position = next as u64;
        Ok(self.position)
    }
}

pub(crate) fn scan_table_index(
    directory: &Path,
    table_id: u64,
) -> Result<TableChronicleIndex, DbError> {
    let mut files = Vec::new();
    let entries = match std::fs::read_dir(directory) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(TableChronicleIndex::default()),
        Err(error) => return Err(DbError::Io(error)),
    };
    for entry in entries {
        let entry = entry?;
        if entry.file_type()?.is_file()
            && entry.path().extension().and_then(|value| value.to_str()) == Some("chr")
        {
            files.push(entry.path());
        }
    }
    let mut records = Vec::with_capacity(files.len());
    for path in files {
        let filename = path.file_name().and_then(|value| value.to_str())
            .ok_or(DbError::ChronicleCorrupt("Chronicle filename is not UTF-8"))?;
        let file_id = filename.strip_prefix("segment-")
            .and_then(|value| value.strip_suffix(".chr"))
            .filter(|value| value.len() == 20 && value.bytes().all(|byte| byte.is_ascii_digit()))
            .and_then(|value| value.parse::<u64>().ok())
            .filter(|value| *value != 0)
            .ok_or(DbError::ChronicleCorrupt("noncanonical Chronicle filename"))?;
        let (metadata, _) = inspect_file(&path, table_id)?;
        records.push((metadata.sequence, file_id, metadata, path));
    }
    records.sort_by_key(|(sequence, _, _, _)| *sequence);
    let mut index = TableChronicleIndex::default();
    for (_, _, metadata, path) in records {
        index.record(BatchReceipt { metadata, path })?;
    }
    Ok(index)
}

pub(crate) fn sha256(bytes: &[u8]) -> [u8; 32] {
    let mut state = Sha256::new();
    state.update(bytes);
    state.finalize()
}

/// Hash a file incrementally so recovery can verify large immutable artifacts
/// without allocating a buffer proportional to the complete file size.
pub(crate) fn sha256_file(path: &Path) -> io::Result<[u8; 32]> {
    let mut file = File::open(path)?;
    let mut state = Sha256::new();
    let mut buffer = [0_u8; IO_BUFFER];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        state.update(&buffer[..read]);
    }
    Ok(state.finalize())
}

fn checksum64(bytes: &[u8]) -> u64 {
    let mut value = 0xcbf29ce484222325_u64;
    for byte in bytes {
        value ^= u64::from(*byte);
        value = value.wrapping_mul(0x100000001b3);
    }
    value
}

// Small, self-contained SHA-256 implementation. Kept private and checked against
// standard test vectors; it hashes only canonical ANF payload bytes.
struct Sha256 {
    state: [u32; 8],
    buffer: [u8; 64],
    buffered: usize,
    total: u64,
}

impl Sha256 {
    fn new() -> Self {
        Self {
            state: [
                0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a,
                0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19,
            ],
            buffer: [0; 64],
            buffered: 0,
            total: 0,
        }
    }

    fn update(&mut self, mut input: &[u8]) {
        self.total = self.total.wrapping_add(input.len() as u64);
        if self.buffered > 0 {
            let take = (64 - self.buffered).min(input.len());
            self.buffer[self.buffered..self.buffered + take].copy_from_slice(&input[..take]);
            self.buffered += take;
            input = &input[take..];
            if self.buffered == 64 {
                let block = self.buffer;
                self.compress(&block);
                self.buffered = 0;
            }
        }
        while input.len() >= 64 {
            let block: &[u8; 64] = input[..64].try_into().expect("64-byte chunk");
            self.compress(block);
            input = &input[64..];
        }
        if !input.is_empty() {
            self.buffer[..input.len()].copy_from_slice(input);
            self.buffered = input.len();
        }
    }

    fn finalize(mut self) -> [u8; 32] {
        let bit_len = self.total.wrapping_mul(8);
        self.buffer[self.buffered] = 0x80;
        self.buffered += 1;
        if self.buffered > 56 {
            self.buffer[self.buffered..].fill(0);
            let block = self.buffer;
            self.compress(&block);
            self.buffer = [0; 64];
            self.buffered = 0;
        }
        self.buffer[self.buffered..56].fill(0);
        self.buffer[56..64].copy_from_slice(&bit_len.to_be_bytes());
        let block = self.buffer;
        self.compress(&block);
        let mut output = [0_u8; 32];
        for (index, word) in self.state.iter().enumerate() {
            output[index * 4..index * 4 + 4].copy_from_slice(&word.to_be_bytes());
        }
        output
    }

    fn compress(&mut self, block: &[u8; 64]) {
        const K: [u32; 64] = [
            0x428a2f98,0x71374491,0xb5c0fbcf,0xe9b5dba5,0x3956c25b,0x59f111f1,0x923f82a4,0xab1c5ed5,
            0xd807aa98,0x12835b01,0x243185be,0x550c7dc3,0x72be5d74,0x80deb1fe,0x9bdc06a7,0xc19bf174,
            0xe49b69c1,0xefbe4786,0x0fc19dc6,0x240ca1cc,0x2de92c6f,0x4a7484aa,0x5cb0a9dc,0x76f988da,
            0x983e5152,0xa831c66d,0xb00327c8,0xbf597fc7,0xc6e00bf3,0xd5a79147,0x06ca6351,0x14292967,
            0x27b70a85,0x2e1b2138,0x4d2c6dfc,0x53380d13,0x650a7354,0x766a0abb,0x81c2c92e,0x92722c85,
            0xa2bfe8a1,0xa81a664b,0xc24b8b70,0xc76c51a3,0xd192e819,0xd6990624,0xf40e3585,0x106aa070,
            0x19a4c116,0x1e376c08,0x2748774c,0x34b0bcb5,0x391c0cb3,0x4ed8aa4a,0x5b9cca4f,0x682e6ff3,
            0x748f82ee,0x78a5636f,0x84c87814,0x8cc70208,0x90befffa,0xa4506ceb,0xbef9a3f7,0xc67178f2,
        ];
        let mut w = [0_u32; 64];
        for (index, chunk) in block.chunks_exact(4).enumerate() {
            w[index] = u32::from_be_bytes(chunk.try_into().expect("4-byte word"));
        }
        for index in 16..64 {
            let s0 = w[index - 15].rotate_right(7) ^ w[index - 15].rotate_right(18) ^ (w[index - 15] >> 3);
            let s1 = w[index - 2].rotate_right(17) ^ w[index - 2].rotate_right(19) ^ (w[index - 2] >> 10);
            w[index] = w[index - 16].wrapping_add(s0).wrapping_add(w[index - 7]).wrapping_add(s1);
        }
        let [mut a, mut b, mut c, mut d, mut e, mut f, mut g, mut h] = self.state;
        for index in 0..64 {
            let s1 = e.rotate_right(6) ^ e.rotate_right(11) ^ e.rotate_right(25);
            let choice = (e & f) ^ ((!e) & g);
            let temp1 = h.wrapping_add(s1).wrapping_add(choice).wrapping_add(K[index]).wrapping_add(w[index]);
            let s0 = a.rotate_right(2) ^ a.rotate_right(13) ^ a.rotate_right(22);
            let majority = (a & b) ^ (a & c) ^ (b & c);
            let temp2 = s0.wrapping_add(majority);
            h = g; g = f; f = e; e = d.wrapping_add(temp1);
            d = c; c = b; b = a; a = temp1.wrapping_add(temp2);
        }
        self.state[0] = self.state[0].wrapping_add(a);
        self.state[1] = self.state[1].wrapping_add(b);
        self.state[2] = self.state[2].wrapping_add(c);
        self.state[3] = self.state[3].wrapping_add(d);
        self.state[4] = self.state[4].wrapping_add(e);
        self.state[5] = self.state[5].wrapping_add(f);
        self.state[6] = self.state[6].wrapping_add(g);
        self.state[7] = self.state[7].wrapping_add(h);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|byte| format!("{byte:02x}")).collect()
    }

    #[test]
    fn ingestion_sequence_overflow_fails_closed() {
        let mut index = TableChronicleIndex::default();
        index.last_sequence = u64::MAX;
        assert!(matches!(index.next_sequence(), Err(DbError::LengthOverflow)));
    }

    #[test]
    fn sha256_matches_standard_vectors() {
        assert_eq!(
            hex(&sha256(b"")),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
        assert_eq!(
            hex(&sha256(b"abc")),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        assert_eq!(
            hex(&sha256(&vec![b'a'; 1_000_000])),
            "cdc76e5c9914fb9281a1c7e284d73e67f1809a48a497200e046d39ccc7112cd0"
        );
    }

    #[test]
    fn batch_id_hex_round_trip_and_zero_rejection() {
        let id = BatchId::from_hex("00112233445566778899aabbccddeeff").unwrap();
        assert_eq!(id.to_hex(), "00112233445566778899aabbccddeeff");
        assert_eq!(BatchId::from_hex(&id.to_hex()).unwrap(), id);
        assert!(BatchId::from_hex("00000000000000000000000000000000").is_err());
        assert!(BatchId::from_hex("xyz").is_err());
    }
}
