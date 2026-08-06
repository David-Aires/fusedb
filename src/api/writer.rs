// src/api/writer.rs
// ──────────────────────────────────────────────────────────────────────────────
// Public Rust writer.  Wraps `WriterCore` with generic keys, generic paths and
// (with the default `msgpack` feature) typed serde values.

use std::collections::HashMap;
use std::path::Path;

use crate::core::{BuildReport, FuseResult, WriterCore};

#[cfg(feature = "msgpack")]
use serde::Serialize;

/// Builds a `.fsdb` file.
///
/// An *object* is stored once; any number of keys may point at it. That is the
/// whole point of the format — a million keys sharing one payload cost one copy
/// on disk.
///
/// # Example
/// ```no_run
/// use fusedb::FuseWriter;
/// use std::collections::BTreeMap;
///
/// let mut w = FuseWriter::new();
///
/// let mut google = BTreeMap::new();
/// google.insert("company", "Google");
///
/// // one object, three keys
/// let oid = w.add_object(&google)?;
/// w.add_key("google.com", oid)?;
/// w.add_key("8.8.8.8", oid)?;
/// w.add_key("8.8.4.4", oid)?;
///
/// w.build("db.fsdb")?;
/// # Ok::<(), fusedb::FuseError>(())
/// ```
///
/// # Deduplication
///
/// [`add_object`](Self::add_object) always appends a new object, exactly like
/// the Python `FuseWriter`. Use [`add_object_deduped`](Self::add_object_deduped)
/// when you want identical payloads collapsed into a single stored object.
#[derive(Default)]
pub struct FuseWriter {
    core: WriterCore,
    /// Content → object id, populated only by the `*_deduped` methods.
    dedup: HashMap<Vec<u8>, usize>,
}

impl FuseWriter {
    /// Create a new, empty writer.
    pub fn new() -> Self {
        Self {
            core: WriterCore::new(),
            dedup: HashMap::new(),
        }
    }

    // ── raw-bytes API (no serialisation, no feature required) ────────────────

    /// Store pre-encoded bytes as a new object. Returns its object ID.
    ///
    /// The bytes are written to disk verbatim. If you want the file to be
    /// readable by the Python package, they must be MessagePack — which is
    /// what [`add_object`](Self::add_object) produces for you.
    pub fn add_object_raw(&mut self, raw: impl AsRef<[u8]>) -> usize {
        self.core.add_object(raw.as_ref())
    }

    /// Store pre-encoded bytes, reusing an existing object when the payload
    /// is byte-identical to one already added through this method.
    pub fn add_object_raw_deduped(&mut self, raw: impl AsRef<[u8]>) -> usize {
        let raw = raw.as_ref();
        if let Some(&id) = self.dedup.get(raw) {
            return id;
        }
        let id = self.core.add_object(raw);
        self.dedup.insert(raw.to_vec(), id);
        id
    }

    /// Map `key` to an existing object ID.
    ///
    /// Keys are arbitrary byte strings — UTF-8 text, binary, packed IPs.
    /// Adding the same key twice overwrites the earlier mapping.
    ///
    /// # Errors
    /// [`FuseError::InvalidArg`](crate::FuseError::InvalidArg) if `obj_id` was
    /// never returned by an `add_object*` call, or if `key` is longer than
    /// [`MAX_KEY_LEN`](crate::core::format::MAX_KEY_LEN) bytes.
    pub fn add_key(&mut self, key: impl AsRef<[u8]>, obj_id: usize) -> FuseResult<()> {
        self.core.add_key(key.as_ref(), obj_id)
    }

    /// Store raw bytes and point `key` at them, in one call.
    pub fn add_raw(&mut self, key: impl AsRef<[u8]>, raw: impl AsRef<[u8]>) -> FuseResult<usize> {
        let oid = self.add_object_raw(raw);
        self.add_key(key, oid)?;
        Ok(oid)
    }

    // ── typed API ────────────────────────────────────────────────────────────

    /// Serialise `value` to MessagePack and store it as a new object.
    ///
    /// The encoding matches Python's `msgpack.packb(value, use_bin_type=True)`,
    /// so anything written here decodes in the Python package unchanged.
    #[cfg(feature = "msgpack")]
    pub fn add_object<T: Serialize + ?Sized>(&mut self, value: &T) -> FuseResult<usize> {
        let raw = super::codec::encode(value)?;
        Ok(self.core.add_object(&raw))
    }

    /// Like [`add_object`](Self::add_object), but collapses values that encode
    /// to identical bytes into a single stored object.
    #[cfg(feature = "msgpack")]
    pub fn add_object_deduped<T: Serialize + ?Sized>(&mut self, value: &T) -> FuseResult<usize> {
        let raw = super::codec::encode(value)?;
        Ok(self.add_object_raw_deduped(raw))
    }

    /// Convenience: [`add_object`](Self::add_object) + [`add_key`](Self::add_key).
    #[cfg(feature = "msgpack")]
    pub fn add<T: Serialize + ?Sized>(
        &mut self,
        key: impl AsRef<[u8]>,
        value: &T,
    ) -> FuseResult<usize> {
        let oid = self.add_object(value)?;
        self.add_key(key, oid)?;
        Ok(oid)
    }

    // ── output ───────────────────────────────────────────────────────────────

    /// Write the `.fsdb` file atomically: tmp → fsync → rename.
    ///
    /// Readers holding the previous file open keep seeing consistent data
    /// through their existing mmap.
    pub fn build(&self, path: impl AsRef<Path>) -> FuseResult<BuildReport> {
        self.core.build(path)
    }

    /// Objects staged so far.
    #[inline]
    pub fn num_objects(&self) -> usize {
        self.core.num_objects()
    }

    /// Keys staged so far.
    #[inline]
    pub fn num_keys(&self) -> usize {
        self.core.num_keys()
    }

    /// Is the writer empty?
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.core.num_objects() == 0 && self.core.num_keys() == 0
    }
}

impl std::fmt::Debug for FuseWriter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FuseWriter")
            .field("objects", &self.num_objects())
            .field("keys", &self.num_keys())
            .finish()
    }
}
