// src/api/reader.rs
// ──────────────────────────────────────────────────────────────────────────────
// Public Rust reader.  Wraps `ReaderCore` with generic paths, generic keys,
// a lazy prefix iterator and (with the default `msgpack` feature) typed values.

use std::path::Path;

use crate::core::format::{HEADER_SIZE, VERSION};
use crate::core::{FuseError, FuseResult, ReaderCore};

use super::stats::Stats;

#[cfg(feature = "msgpack")]
use serde::de::DeserializeOwned;

/// Memory-mapped, read-only handle on a `.fsdb` file.
///
/// Cheap to clone-by-sharing: the index is immutable after [`open`](Self::open)
/// and the mapping is read-only, so one `FuseReader` can be shared across
/// threads behind an `Arc` with no locking.
///
/// # Example
/// ```no_run
/// use fusedb::FuseReader;
/// use std::collections::BTreeMap;
///
/// let db = FuseReader::open("db.fsdb")?;
///
/// let hit: Option<BTreeMap<String, String>> = db.get("google.com")?;
/// println!("{hit:?}");
///
/// for entry in db.prefix_iter(b"8.8.") {
///     let (key, raw) = entry?;
///     println!("{key} -> {} bytes", raw.len());
/// }
/// # Ok::<(), fusedb::FuseError>(())
/// ```
pub struct FuseReader {
    core: ReaderCore,
}

impl FuseReader {
    /// Open a `.fsdb` file, validating the whole-file CRC32 first.
    pub fn open(path: impl AsRef<Path>) -> FuseResult<Self> {
        Ok(Self {
            core: ReaderCore::open(path, true)?,
        })
    }

    /// Open without the CRC32 check.
    ///
    /// Skips a full pass over the file, which matters for multi-gigabyte
    /// databases opened on a hot path. Only use it on files you trust or have
    /// already verified.
    pub fn open_unverified(path: impl AsRef<Path>) -> FuseResult<Self> {
        Ok(Self {
            core: ReaderCore::open(path, false)?,
        })
    }

    /// Borrow the underlying raw-bytes core.
    #[inline]
    pub fn core(&self) -> &ReaderCore {
        &self.core
    }

    // ── raw-bytes lookups ────────────────────────────────────────────────────

    /// O(1) exact lookup returning the stored bytes untouched.
    pub fn get_raw(&self, key: impl AsRef<[u8]>) -> FuseResult<Option<Vec<u8>>> {
        self.core.get(key.as_ref())
    }

    /// Key presence check. Touches only the in-memory index — no page faults.
    #[inline]
    pub fn exists(&self, key: impl AsRef<[u8]>) -> bool {
        self.core.exists(key.as_ref())
    }

    /// Number of keys. Named `len` so `FuseReader` reads like a collection.
    #[inline]
    pub fn len(&self) -> usize {
        self.core.num_keys() as usize
    }

    /// Does the database contain no keys?
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.core.num_keys() == 0
    }

    /// All keys, sorted, as raw bytes.
    #[inline]
    pub fn keys_raw(&self) -> &[Vec<u8>] {
        self.core.sorted_keys()
    }

    /// All keys, sorted, UTF-8 lossy decoded.
    pub fn keys(&self) -> Vec<String> {
        self.core.keys()
    }

    /// Every `(key, raw_bytes)` pair in sorted key order.
    pub fn items_raw(&self) -> FuseResult<Vec<(String, Vec<u8>)>> {
        self.core.items()
    }

    /// Unique objects only, deduplicated by data-section offset.
    pub fn objects_raw(&self) -> FuseResult<Vec<Vec<u8>>> {
        self.core.objects()
    }

    /// Sorted prefix scan, collected into a `Vec`.
    ///
    /// Prefer [`prefix_iter`](Self::prefix_iter) when the match set is large —
    /// it reads objects lazily instead of materialising all of them.
    pub fn prefix_raw(&self, prefix: impl AsRef<[u8]>) -> FuseResult<Vec<(String, Vec<u8>)>> {
        self.core.prefix(prefix.as_ref())
    }

    /// Lazy sorted prefix scan.
    ///
    /// Yields `FuseResult<(String, Vec<u8>)>` for every key starting with
    /// `prefix`, reading each object from the mmap only when the iterator
    /// reaches it.
    pub fn prefix_iter(&self, prefix: impl AsRef<[u8]>) -> PrefixIter<'_> {
        let prefix = prefix.as_ref().to_vec();
        let pos = self.core.lower_bound(&prefix);
        PrefixIter {
            reader: &self.core,
            prefix,
            pos,
        }
    }

    // ── typed lookups ────────────────────────────────────────────────────────

    /// O(1) exact lookup, decoding the stored MessagePack into `T`.
    ///
    /// Values written by the Python package decode here without conversion.
    #[cfg(feature = "msgpack")]
    pub fn get<T: DeserializeOwned>(&self, key: impl AsRef<[u8]>) -> FuseResult<Option<T>> {
        match self.core.get(key.as_ref())? {
            None => Ok(None),
            Some(raw) => Ok(Some(super::codec::decode(&raw)?)),
        }
    }

    /// Sorted prefix scan, decoding each value into `T`.
    #[cfg(feature = "msgpack")]
    pub fn prefix<T: DeserializeOwned>(
        &self,
        prefix: impl AsRef<[u8]>,
    ) -> FuseResult<Vec<(String, T)>> {
        self.core
            .prefix(prefix.as_ref())?
            .into_iter()
            .map(|(k, raw)| Ok((k, super::codec::decode(&raw)?)))
            .collect()
    }

    /// Every `(key, value)` pair in sorted key order, decoded into `T`.
    #[cfg(feature = "msgpack")]
    pub fn items<T: DeserializeOwned>(&self) -> FuseResult<Vec<(String, T)>> {
        self.core
            .items()?
            .into_iter()
            .map(|(k, raw)| Ok((k, super::codec::decode(&raw)?)))
            .collect()
    }

    /// Unique objects only, decoded into `T`.
    #[cfg(feature = "msgpack")]
    pub fn objects<T: DeserializeOwned>(&self) -> FuseResult<Vec<T>> {
        self.core
            .objects()?
            .into_iter()
            .map(|raw| super::codec::decode(&raw))
            .collect()
    }

    // ── metadata and integrity ───────────────────────────────────────────────

    /// File metadata — the Rust equivalent of Python's `FuseReader.stats()`.
    pub fn stats(&self) -> Stats {
        Stats {
            path: self.core.path().to_path_buf(),
            version: VERSION,
            num_keys: self.core.num_keys(),
            num_objects: self.core.num_objects(),
            index_offset: self.core.index_offset(),
            data_offset: HEADER_SIZE as u64,
            file_size: self.core.file_size(),
            file_crc32: self.core.stored_crc(),
        }
    }

    /// Path this reader was opened from.
    #[inline]
    pub fn path(&self) -> &Path {
        self.core.path()
    }

    /// Deep integrity check: whole-file CRC32 plus every object's CRC32.
    ///
    /// # Errors
    /// [`FuseError::Corrupt`] naming the first mismatch found.
    pub fn verify(&self) -> FuseResult<()> {
        self.core.verify().map(|_| ())
    }

    /// Non-throwing form of [`verify`](Self::verify) — `false` on any
    /// corruption, without surfacing the reason.
    pub fn is_valid(&self) -> bool {
        matches!(self.core.verify(), Ok(true))
    }
}

impl std::fmt::Debug for FuseReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FuseReader")
            .field("path", &self.core.path())
            .field("keys", &self.core.num_keys())
            .field("objects", &self.core.num_objects())
            .finish()
    }
}

// ── prefix iterator ───────────────────────────────────────────────────────────

/// Lazy iterator over a sorted prefix range. Created by
/// [`FuseReader::prefix_iter`].
pub struct PrefixIter<'a> {
    reader: &'a ReaderCore,
    prefix: Vec<u8>,
    pos: usize,
}

impl Iterator for PrefixIter<'_> {
    type Item = Result<(String, Vec<u8>), FuseError>;

    fn next(&mut self) -> Option<Self::Item> {
        let keys = self.reader.sorted_keys();
        if self.pos >= keys.len() || !keys[self.pos].starts_with(&self.prefix) {
            return None;
        }
        let key = String::from_utf8_lossy(&keys[self.pos]).into_owned();
        let offset = self.reader.sorted_offsets()[self.pos];
        self.pos += 1;
        Some(self.reader.read_at(offset).map(|raw| (key, raw)))
    }
}
