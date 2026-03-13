// src/core/reader.rs
// ──────────────────────────────────────────────────────────────────────────────
// Pure Rust reader. No PyO3, no Python types anywhere.
// Thread-safe for reads: the index is immutable after `open()` and the mmap
// is read-only. Share one `ReaderCore` instance freely across threads.

use std::fs::File;
use std::path::{Path, PathBuf};

use ahash::AHashMap;
use memmap2::Mmap;

use super::error::{FuseError, FuseResult};
use super::format::{crc32, read_raw, Index, OBJ_HDR_SZ, HEADER_SIZE};

/// Memory-mapped reader for `.fsdb` files.
///
/// # Example
/// ```no_run
/// use fusedb::core::ReaderCore;
///
/// let db = ReaderCore::open("geo.fsdb", true)?;
/// if let Some(raw) = db.get(b"8.8.8.8")? {
///     // raw is msgpack bytes — decode with your preferred crate
///     println!("{} bytes", raw.len());
/// }
/// # Ok::<(), fusedb::core::FuseError>(())
/// ```
pub struct ReaderCore {
    pub(crate) path: PathBuf,
    pub(crate) mm:   Mmap,
    pub(crate) idx:  Index,
}

impl ReaderCore {
    /// Open and memory-map a `.fsdb` file.
    ///
    /// If `verify` is `true`, validates the whole-file CRC32 on open
    /// (recommended for untrusted files; skip for hot-paths after initial check).
    pub fn open(path: &str, verify: bool) -> FuseResult<Self> {
        let path = Path::new(path);
        let file = File::open(path)
            .map_err(|e| FuseError::Io(format!("cannot open {:?}: {e}", path)))?;

        // SAFETY: We never write through this mapping.  The file is opened
        // read-only; concurrent writers use atomic rename (tmp → rename), so
        // the mapped region is always a complete and consistent snapshot.
        let mm = unsafe { Mmap::map(&file) }
            .map_err(|e| FuseError::Io(format!("mmap failed: {e}")))?;

        let idx = Index::load(&mm, verify)?;

        Ok(Self { path: path.to_path_buf(), mm, idx })
    }

    // ── lookups ───────────────────────────────────────────────────────────────

    /// O(1) exact-match lookup. Returns the raw object bytes or `None`.
    ///
    /// The returned `Vec<u8>` is a copy out of the mmap page — the caller
    /// may hold it without keeping the reader alive.
    pub fn get(&self, key: &[u8]) -> FuseResult<Option<Vec<u8>>> {
        match self.idx.hash.get(key) {
            None          => Ok(None),
            Some(&offset) => read_raw(&self.mm, offset).map(Some),
        }
    }

    /// Key presence check — no I/O beyond the in-memory hash index.
    #[inline]
    pub fn exists(&self, key: &[u8]) -> bool {
        self.idx.hash.contains_key(key)
    }

    /// Sorted prefix scan. Returns `(key_utf8_lossy, raw_bytes)` pairs for
    /// every key that starts with `prefix`.
    pub fn prefix(&self, prefix: &[u8]) -> FuseResult<Vec<(String, Vec<u8>)>> {
        let pos = self
            .idx
            .sorted_keys
            .partition_point(|k| k.as_slice() < prefix);

        let mut out = Vec::new();
        let mut i   = pos;
        while i < self.idx.sorted_keys.len()
            && self.idx.sorted_keys[i].starts_with(prefix)
        {
            let key_str = String::from_utf8_lossy(&self.idx.sorted_keys[i]).into_owned();
            let raw     = read_raw(&self.mm, self.idx.sorted_offsets[i])?;
            out.push((key_str, raw));
            i += 1;
        }
        Ok(out)
    }

    /// All keys in sorted order (UTF-8 lossy decoded).
    pub fn keys(&self) -> Vec<String> {
        self.idx
            .sorted_keys
            .iter()
            .map(|k| String::from_utf8_lossy(k).into_owned())
            .collect()
    }

    /// All `(key, raw_bytes)` pairs in sorted key order.
    ///
    /// Objects that share a file offset are read once and cloned in-memory —
    /// no redundant mmap reads.
    pub fn items(&self) -> FuseResult<Vec<(String, Vec<u8>)>> {
        let mut cache: AHashMap<u64, Vec<u8>> = AHashMap::new();
        let mut out   = Vec::with_capacity(self.idx.num_keys as usize);

        for (key_bytes, &offset) in self
            .idx
            .sorted_keys
            .iter()
            .zip(self.idx.sorted_offsets.iter())
        {
            let raw = match cache.get(&offset) {
                Some(cached) => cached.clone(),
                None => {
                    let b = read_raw(&self.mm, offset)?;
                    cache.insert(offset, b.clone());
                    b
                }
            };
            out.push((String::from_utf8_lossy(key_bytes).into_owned(), raw));
        }
        Ok(out)
    }

    /// Unique objects only, deduplicated by file offset.
    pub fn objects(&self) -> FuseResult<Vec<Vec<u8>>> {
        let mut seen: AHashMap<u64, ()> = AHashMap::new();
        let mut out  = Vec::new();
        for &offset in &self.idx.sorted_offsets {
            if seen.insert(offset, ()).is_none() {
                out.push(read_raw(&self.mm, offset)?);
            }
        }
        Ok(out)
    }

    // ── introspection ─────────────────────────────────────────────────────────

    /// Number of index entries (keys).
    #[inline]
    pub fn num_keys(&self) -> u32 { self.idx.num_keys }

    /// Number of unique objects in the data section.
    #[inline]
    pub fn num_objects(&self) -> u32 { self.idx.num_objects }

    /// File path this reader was opened from.
    pub fn path(&self) -> &Path { &self.path }

    /// File size in bytes.
    #[inline]
    pub fn file_size(&self) -> u64 { self.idx.file_size }

    /// Stored whole-file CRC32.
    #[inline]
    pub fn stored_crc(&self) -> u32 { self.idx.stored_crc }

    /// Byte offset of the index section.
    #[inline]
    pub fn index_offset(&self) -> u64 { self.idx.index_offset }

    // ── integrity ─────────────────────────────────────────────────────────────

    /// Deep integrity check: whole-file CRC32 + per-object CRC32.
    ///
    /// Returns `Ok(true)` on success. Errors with `FuseError::Corrupt` on
    /// the first mismatch found.
    pub fn verify(&self) -> FuseResult<bool> {
        let data     = self.mm.as_ref();
        let computed = crc32(&data[HEADER_SIZE..]);
        if computed != self.idx.stored_crc {
            return Err(FuseError::Corrupt(format!(
                "file CRC32 mismatch  stored={:#010x}  computed={:#010x}",
                self.idx.stored_crc, computed
            )));
        }
        let mut seen: AHashMap<u64, ()> = AHashMap::new();
        for &offset in &self.idx.sorted_offsets {
            if seen.insert(offset, ()).is_none() {
                let o = offset as usize;
                if o + OBJ_HDR_SZ > data.len() {
                    return Err(FuseError::Corrupt(format!(
                        "object offset {offset} out of bounds"
                    )));
                }
                let obj_len = u32::from_be_bytes(data[o..o + 4].try_into().unwrap()) as usize;
                let stored  = u32::from_be_bytes(data[o + 4..o + 8].try_into().unwrap());
                let raw     = &data[o + OBJ_HDR_SZ..o + OBJ_HDR_SZ + obj_len];
                if crc32(raw) != stored {
                    return Err(FuseError::Corrupt(format!(
                        "object CRC32 mismatch at offset {offset}"
                    )));
                }
            }
        }
        Ok(true)
    }
}