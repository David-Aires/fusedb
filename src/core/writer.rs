// src/core/writer.rs
// ──────────────────────────────────────────────────────────────────────────────
// Pure Rust writer. No PyO3, no Python types anywhere.
// Usable from benchmarks, integration tests, and any future Rust consumer.

use std::collections::HashMap;
use std::fs::{self, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::Path;

use crc32fast::Hasher as Crc32Hasher;

use super::error::{FuseError, FuseResult};
use super::format::{crc32, HEADER_SIZE, MAGIC, VERSION};

/// Builds a `.fsdb` file from raw (pre-encoded) object bytes.
///
/// Objects are stored once; any number of string or byte keys can point to
/// the same object.  Call [`build`](WriterCore::build) to flush atomically.
///
/// # Example
/// ```no_run
/// use fusedb::core::WriterCore;
///
/// let mut w = WriterCore::new();
/// let oid = w.add_object(b"\x81\xa3org\xabGoogle LLC"); // msgpack bytes
/// w.add_key(b"8.8.8.8",   oid)?;
/// w.add_key(b"8.8.4.4",   oid)?;
/// w.build("geo.fsdb")?;
/// # Ok::<(), fusedb::core::FuseError>(())
/// ```
pub struct WriterCore {
    pub(crate) objects: Vec<Vec<u8>>,
    pub(crate) keys: HashMap<Vec<u8>, usize>,
}

impl WriterCore {
    /// Create a new, empty writer.
    pub fn new() -> Self {
        Self {
            objects: Vec::new(),
            keys: HashMap::new(),
        }
    }

    /// Store *raw_bytes* as one unique object. Returns its integer ID.
    ///
    /// No deduplication is performed here — duplicate detection is the
    /// caller's responsibility (see `merge()` in the Python layer).
    pub fn add_object(&mut self, raw_bytes: &[u8]) -> usize {
        let id = self.objects.len();
        self.objects.push(raw_bytes.to_vec());
        id
    }

    /// Map `key` to an existing object ID.
    ///
    /// `key` can be any byte string: UTF-8 text, binary, IP text representation…
    /// If the same key is added twice, the second call overwrites the first.
    pub fn add_key(&mut self, key: &[u8], obj_id: usize) -> FuseResult<()> {
        if obj_id >= self.objects.len() {
            return Err(FuseError::InvalidArg(format!(
                "obj_id {obj_id} out of range (have {} objects)",
                self.objects.len()
            )));
        }
        self.keys.insert(key.to_vec(), obj_id);
        Ok(())
    }

    /// Write the `.fsdb` file atomically: tmp → fsync → rename.
    ///
    /// The rename is atomic on POSIX systems.  Readers that have the previous
    /// file open continue to see the old data via their existing mmap.
    pub fn build(&self, path: &str) -> FuseResult<()> {
        let path = Path::new(path);
        let tmp = path.with_extension("fsdb.tmp");

        // ── 1. data section ──────────────────────────────────────────────────
        let mut data_sec: Vec<u8> = Vec::new();
        let mut obj_offsets: Vec<u64> = Vec::with_capacity(self.objects.len());

        for raw in &self.objects {
            obj_offsets.push((HEADER_SIZE + data_sec.len()) as u64);
            data_sec.extend_from_slice(&(raw.len() as u32).to_be_bytes());
            data_sec.extend_from_slice(&crc32(raw).to_be_bytes());
            data_sec.extend_from_slice(raw);
        }

        // ── 2. index section (sorted by key bytes) ───────────────────────────
        let mut sorted: Vec<(&Vec<u8>, usize)> = self.keys.iter().map(|(k, &id)| (k, id)).collect();
        sorted.sort_by_key(|(k, _)| k.as_slice());

        let mut idx_sec: Vec<u8> = Vec::new();
        for (key, obj_id) in &sorted {
            idx_sec.extend_from_slice(&(key.len() as u16).to_be_bytes());
            idx_sec.extend_from_slice(key);
            idx_sec.extend_from_slice(&obj_offsets[*obj_id].to_be_bytes());
        }

        // ── 3. whole-file CRC32 (over data + index sections) ─────────────────
        let mut h = Crc32Hasher::new();
        h.update(&data_sec);
        h.update(&idx_sec);
        let file_crc = h.finalize();
        let index_offset = (HEADER_SIZE + data_sec.len()) as u64;

        // ── 4. header ────────────────────────────────────────────────────────
        let mut hdr = [0u8; HEADER_SIZE];
        hdr[0..4].copy_from_slice(MAGIC.as_slice());
        hdr[4] = VERSION;
        hdr[8..12].copy_from_slice(&(self.keys.len() as u32).to_be_bytes());
        hdr[12..16].copy_from_slice(&(self.objects.len() as u32).to_be_bytes());
        hdr[16..24].copy_from_slice(&index_offset.to_be_bytes());
        hdr[24..32].copy_from_slice(&(HEADER_SIZE as u64).to_be_bytes());
        hdr[32..36].copy_from_slice(&file_crc.to_be_bytes());

        // ── 5. write  tmp → fsync → rename ───────────────────────────────────
        {
            let file = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(&tmp)
                .map_err(|e| FuseError::Io(format!("open tmp: {e}")))?;

            let mut w = BufWriter::new(&file);
            w.write_all(&hdr)
                .and_then(|_| w.write_all(&data_sec))
                .and_then(|_| w.write_all(&idx_sec))
                .and_then(|_| w.flush())
                .map_err(|e| FuseError::Io(e.to_string()))?;

            file.sync_all().map_err(|e| FuseError::Io(e.to_string()))?;
        }

        fs::rename(&tmp, path).map_err(|e| FuseError::Io(format!("rename: {e}")))?;

        let kb = fs::metadata(path).map(|m| m.len()).unwrap_or(0) as f64 / 1024.0;

        println!(
            "✅  {}  —  {} objects · {} keys · {:.1} KB",
            path.display(),
            self.objects.len(),
            self.keys.len(),
            kb,
        );
        Ok(())
    }

    /// Number of objects currently staged.
    #[inline]
    pub fn num_objects(&self) -> usize {
        self.objects.len()
    }

    /// Number of keys currently staged.
    #[inline]
    pub fn num_keys(&self) -> usize {
        self.keys.len()
    }
}

impl Default for WriterCore {
    fn default() -> Self {
        Self::new()
    }
}
