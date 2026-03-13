// src/core/format.rs
// ──────────────────────────────────────────────────────────────────────────────
// Binary format constants, header parsing, and the in-memory index.
// Zero PyO3 knowledge — used by both writer and reader.
//
// File layout (big-endian, version 2)
// ────────────────────────────────────
//   HEADER  40 bytes
//     magic[4]         b"FSDB"
//     version[1]       2
//     flags[1]         reserved
//     pad[2]           reserved
//     num_keys[4]
//     num_objects[4]
//     index_offset[8]
//     data_offset[8]   always 40 (= HEADER_SIZE)
//     file_crc32[4]    CRC32 of everything after the header
//     reserved[4]
//
//   DATA SECTION
//     [obj_len(4)][obj_crc32(4)][raw_bytes]  ×  num_objects
//
//   INDEX SECTION  (sorted lexicographically by key bytes)
//     [key_len(2)][key_bytes][data_offset(8)]  ×  num_keys

use ahash::AHashMap;
use memmap2::Mmap;

use super::error::{FuseError, FuseResult};

// ─── public constants ─────────────────────────────────────────────────────────

pub const MAGIC:       &[u8; 4] = b"FSDB";
pub const VERSION:     u8       = 2;
pub const HEADER_SIZE: usize    = 40;
pub const OBJ_HDR_SZ:  usize    = 8; // obj_len(4) + crc32(4)

// ─── CRC32 ───────────────────────────────────────────────────────────────────

#[inline]
pub fn crc32(data: &[u8]) -> u32 {
    let mut h = crc32fast::Hasher::new();
    h.update(data);
    h.finalize()
}

// ─── Header ──────────────────────────────────────────────────────────────────

pub struct Header {
    pub num_keys:     u32,
    pub num_objects:  u32,
    pub index_offset: u64,
    pub file_crc32:   u32,
}

pub fn parse_header(data: &[u8]) -> FuseResult<Header> {
    if data.len() < HEADER_SIZE {
        return Err(FuseError::Corrupt("file too small for header".into()));
    }
    if &data[0..4] != MAGIC.as_slice() {
        return Err(FuseError::Corrupt(format!("bad magic {:?}", &data[0..4])));
    }
    if data[4] != VERSION {
        return Err(FuseError::Version(data[4]));
    }
    Ok(Header {
        num_keys:     u32::from_be_bytes(data[8..12].try_into().unwrap()),
        num_objects:  u32::from_be_bytes(data[12..16].try_into().unwrap()),
        index_offset: u64::from_be_bytes(data[16..24].try_into().unwrap()),
        file_crc32:   u32::from_be_bytes(data[32..36].try_into().unwrap()),
    })
}

// ─── Index ───────────────────────────────────────────────────────────────────

/// In-memory representation of the index section.
///
/// `hash` enables O(1) exact lookups.
/// `sorted_keys` + `sorted_offsets` enable O(log n + k) prefix scans.
pub struct Index {
    pub hash:           AHashMap<Vec<u8>, u64>,
    pub sorted_keys:    Vec<Vec<u8>>,
    pub sorted_offsets: Vec<u64>,
    pub num_keys:       u32,
    pub num_objects:    u32,
    pub index_offset:   u64,
    pub stored_crc:     u32,
    pub file_size:      u64,
}

impl Index {
    /// Parse the index section from an open mmap.
    /// If `verify` is true, validates the whole-file CRC32 before parsing.
    pub fn load(mm: &Mmap, verify: bool) -> FuseResult<Self> {
        let data = mm.as_ref();
        let hdr  = parse_header(data)?;

        if verify {
            let computed = crc32(&data[HEADER_SIZE..]);
            if computed != hdr.file_crc32 {
                return Err(FuseError::Corrupt(format!(
                    "file CRC32 mismatch  stored={:#010x}  computed={:#010x}",
                    hdr.file_crc32, computed
                )));
            }
        }

        let n = hdr.num_keys as usize;
        let mut hash           = AHashMap::with_capacity(n);
        let mut sorted_keys    = Vec::with_capacity(n);
        let mut sorted_offsets = Vec::with_capacity(n);
        let mut pos            = hdr.index_offset as usize;

        for _ in 0..n {
            if pos + 2 > data.len() {
                return Err(FuseError::Corrupt("index section truncated (key_len)".into()));
            }
            let klen = u16::from_be_bytes(data[pos..pos + 2].try_into().unwrap()) as usize;
            pos += 2;

            if pos + klen + 8 > data.len() {
                return Err(FuseError::Corrupt("index section truncated (key + offset)".into()));
            }
            let key    = data[pos..pos + klen].to_vec();
            pos += klen;
            let offset = u64::from_be_bytes(data[pos..pos + 8].try_into().unwrap());
            pos += 8;

            hash.insert(key.clone(), offset);
            sorted_keys.push(key);
            sorted_offsets.push(offset);
        }

        Ok(Self {
            hash,
            sorted_keys,
            sorted_offsets,
            num_keys:     hdr.num_keys,
            num_objects:  hdr.num_objects,
            index_offset: hdr.index_offset,
            stored_crc:   hdr.file_crc32,
            file_size:    data.len() as u64,
        })
    }
}

// ─── mmap read helper ─────────────────────────────────────────────────────────

/// Read the raw object bytes stored at `offset` in the mmap.
/// Returns a copy — callers own the returned `Vec<u8>`.
#[inline]
pub fn read_raw(mm: &Mmap, offset: u64) -> FuseResult<Vec<u8>> {
    let data = mm.as_ref();
    let o    = offset as usize;
    if o + OBJ_HDR_SZ > data.len() {
        return Err(FuseError::Corrupt(format!("offset {offset} out of bounds")));
    }
    let len = u32::from_be_bytes(data[o..o + 4].try_into().expect("4 bytes")) as usize;
    if o + OBJ_HDR_SZ + len > data.len() {
        return Err(FuseError::Corrupt(format!(
            "object at {offset} extends beyond file end"
        )));
    }
    Ok(data[o + OBJ_HDR_SZ..o + OBJ_HDR_SZ + len].to_vec())
}