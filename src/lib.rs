// src/lib.rs — FuseDB Rust core  (pyo3 ≥ 0.23 / 0.28 compatible)
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
//
// Architecture
// ────────────
// Rust owns : file I/O, mmap, CRC32, index build, binary format.
// Python owns: msgpack serialisation (via the `msgpack` package).
//
// The Python wrapper (python/fusedb/__init__.py) calls:
//   encode → msgpack.packb(obj)  → bytes  → Rust write
//   decode → Rust read          → bytes  → msgpack.unpackb(raw)
//
// File format (big-endian, v2 — identical to Python fsdb.py)
// ────────────────────────────────────────────────────────────
//   HEADER  40 bytes
//     magic[4]  version[1]  flags[1]  pad[2]
//     num_keys[4]  num_objects[4]
//     index_offset[8]  data_offset[8]
//     file_crc32[4]    reserved[4]
//
//   DATA SECTION
//     [obj_len(4)][crc32(4)][raw_bytes]  × M
//
//   INDEX SECTION  (sorted by key bytes)
//     [key_len(2)][key][offset(8)]        × K

#![allow(clippy::needless_pass_by_value)]

use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

use ahash::AHashMap;
use crc32fast::Hasher as Crc32Hasher;
use memmap2::Mmap;
use pyo3::exceptions::{PyIOError, PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList};

// ─── constants ───────────────────────────────────────────────────────────────

const MAGIC:       &[u8; 4] = b"FSDB";
const VERSION:     u8       = 2;
const HEADER_SIZE: usize    = 40;
const OBJ_HDR_SZ:  usize    = 8; // obj_len(4) + crc32(4)

// ─── helpers ─────────────────────────────────────────────────────────────────

#[inline]
fn crc32(data: &[u8]) -> u32 {
    let mut h = Crc32Hasher::new();
    h.update(data);
    h.finalize()
}

fn corrupt(msg: impl Into<String>) -> PyErr {
    PyRuntimeError::new_err(format!("FuseCorruptError: {}", msg.into()))
}

fn version_err(v: u8) -> PyErr {
    PyRuntimeError::new_err(format!(
        "FuseVersionError: unsupported version {v} (expected {VERSION})"
    ))
}

/// Accept str or bytes as a lookup key; always return owned bytes.
fn extract_key(key: &Bound<'_, PyAny>) -> PyResult<Vec<u8>> {
    if let Ok(s) = key.extract::<String>() {
        return Ok(s.into_bytes());
    }
    if let Ok(b) = key.extract::<Vec<u8>>() {
        return Ok(b);
    }
    Err(PyValueError::new_err("key must be str or bytes"))
}

/// Read raw object bytes at *offset* — returns a slice copy as Vec<u8>.
#[inline]
fn read_raw_bytes(mm: &Mmap, offset: u64) -> PyResult<Vec<u8>> {
    let data = mm.as_ref();
    let o    = offset as usize;
    if o + OBJ_HDR_SZ > data.len() {
        return Err(corrupt(format!("offset {offset} out of bounds")));
    }
    let len = u32::from_be_bytes(
        data[o..o + 4].try_into().expect("4 bytes"),
    ) as usize;
    if o + OBJ_HDR_SZ + len > data.len() {
        return Err(corrupt(format!(
            "object at {offset} extends beyond file end"
        )));
    }
    Ok(data[o + OBJ_HDR_SZ..o + OBJ_HDR_SZ + len].to_vec())
}

// ─── Header ──────────────────────────────────────────────────────────────────

struct Header {
    num_keys:     u32,
    num_objects:  u32,
    index_offset: u64,
    file_crc32:   u32,
}

fn parse_header(data: &[u8]) -> PyResult<Header> {
    if data.len() < HEADER_SIZE {
        return Err(corrupt("file too small for header"));
    }
    if &data[0..4] != MAGIC {
        return Err(corrupt(format!("bad magic {:?}", &data[0..4])));
    }
    if data[4] != VERSION {
        return Err(version_err(data[4]));
    }
    Ok(Header {
        num_keys:     u32::from_be_bytes(data[8..12].try_into().unwrap()),
        num_objects:  u32::from_be_bytes(data[12..16].try_into().unwrap()),
        index_offset: u64::from_be_bytes(data[16..24].try_into().unwrap()),
        file_crc32:   u32::from_be_bytes(data[32..36].try_into().unwrap()),
    })
}

// ─── In-memory index ─────────────────────────────────────────────────────────

struct Index {
    hash:             AHashMap<Vec<u8>, u64>,
    sorted_keys:      Vec<Vec<u8>>,
    sorted_offsets:   Vec<u64>,
    num_keys:         u32,
    num_objects:      u32,
    index_offset:     u64,
    stored_crc:       u32,
    file_size:        u64,
}

impl Index {
    fn build(mm: &Mmap, verify: bool) -> PyResult<Self> {
        let data = mm.as_ref();
        let hdr  = parse_header(data)?;

        if verify {
            let computed = crc32(&data[HEADER_SIZE..]);
            if computed != hdr.file_crc32 {
                return Err(corrupt(format!(
                    "file CRC32 mismatch  stored={:#010x}  computed={:#010x}",
                    hdr.file_crc32, computed
                )));
            }
        }

        let n = hdr.num_keys as usize;
        let mut hash            = AHashMap::with_capacity(n);
        let mut sorted_keys     = Vec::with_capacity(n);
        let mut sorted_offsets  = Vec::with_capacity(n);
        let mut pos             = hdr.index_offset as usize;

        for _ in 0..n {
            if pos + 2 > data.len() {
                return Err(corrupt("index section truncated (key_len)"));
            }
            let klen = u16::from_be_bytes(
                data[pos..pos + 2].try_into().unwrap(),
            ) as usize;
            pos += 2;
            if pos + klen + 8 > data.len() {
                return Err(corrupt("index section truncated (key + offset)"));
            }
            let key = data[pos..pos + klen].to_vec();
            pos += klen;
            let offset = u64::from_be_bytes(
                data[pos..pos + 8].try_into().unwrap(),
            );
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

// ─── _FuseWriter ─────────────────────────────────────────────────────────────

/// Low-level writer — accepts pre-serialised msgpack bytes per object.
/// **Use `fusedb.FuseWriter` (Python wrapper) instead of this directly.**
#[pyclass(name = "_FuseWriter")]
pub struct FuseWriter {
    objects: Vec<Vec<u8>>,
    keys:    HashMap<Vec<u8>, usize>,
}

#[pymethods]
impl FuseWriter {
    #[new]
    fn new() -> Self {
        Self {
            objects: Vec::new(),
            keys:    HashMap::new(),
        }
    }

    /// Store pre-encoded *raw_bytes* (msgpack). Returns the object ID.
    fn add_object_raw(&mut self, raw_bytes: &[u8]) -> usize {
        let id = self.objects.len();
        self.objects.push(raw_bytes.to_vec());
        id
    }

    /// Map *key* (str or bytes) to *obj_id*.
    fn add_key(&mut self, key: &Bound<'_, PyAny>, obj_id: usize) -> PyResult<()> {
        if obj_id >= self.objects.len() {
            return Err(PyValueError::new_err(format!(
                "obj_id {obj_id} out of range (have {} objects)",
                self.objects.len()
            )));
        }
        self.keys.insert(extract_key(key)?, obj_id);
        Ok(())
    }

    /// Atomically write the .fsdb file: tmp → fsync → rename.
    fn build(&self, path: &str) -> PyResult<()> {
        let path = Path::new(path);
        let tmp  = path.with_extension("fsdb.tmp");

        // ── 1. data section ──────────────────────────────────────────────────
        let mut data_sec: Vec<u8>   = Vec::new();
        let mut obj_offsets: Vec<u64> = Vec::with_capacity(self.objects.len());

        for raw in &self.objects {
            obj_offsets.push((HEADER_SIZE + data_sec.len()) as u64);
            data_sec.extend_from_slice(&(raw.len() as u32).to_be_bytes());
            data_sec.extend_from_slice(&crc32(raw).to_be_bytes());
            data_sec.extend_from_slice(raw);
        }

        // ── 2. index section (sorted) ────────────────────────────────────────
        let mut sorted: Vec<(&Vec<u8>, usize)> = self
            .keys
            .iter()
            .map(|(k, &id)| (k, id))
            .collect();
        sorted.sort_by_key(|(k, _)| k.as_slice());

        let mut idx_sec: Vec<u8> = Vec::new();
        for (key, obj_id) in &sorted {
            idx_sec.extend_from_slice(&(key.len() as u16).to_be_bytes());
            idx_sec.extend_from_slice(key);
            idx_sec.extend_from_slice(&obj_offsets[*obj_id].to_be_bytes());
        }

        // ── 3. whole-file CRC32 ──────────────────────────────────────────────
        let mut h = Crc32Hasher::new();
        h.update(&data_sec);
        h.update(&idx_sec);
        let file_crc     = h.finalize();
        let index_offset = (HEADER_SIZE + data_sec.len()) as u64;

        // ── 4. header ────────────────────────────────────────────────────────
        let mut hdr = [0u8; HEADER_SIZE];
        hdr[0..4].copy_from_slice(MAGIC);
        hdr[4] = VERSION;
        hdr[8..12].copy_from_slice(&(self.keys.len() as u32).to_be_bytes());
        hdr[12..16].copy_from_slice(&(self.objects.len() as u32).to_be_bytes());
        hdr[16..24].copy_from_slice(&index_offset.to_be_bytes());
        hdr[24..32].copy_from_slice(&(HEADER_SIZE as u64).to_be_bytes());
        hdr[32..36].copy_from_slice(&file_crc.to_be_bytes());

        // ── 5. write tmp → fsync → rename ───────────────────────────────────
        {
            let file = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(&tmp)
                .map_err(|e| PyIOError::new_err(format!("open tmp: {e}")))?;
            let mut w = BufWriter::new(&file);
            w.write_all(&hdr)
                .and_then(|_| w.write_all(&data_sec))
                .and_then(|_| w.write_all(&idx_sec))
                .and_then(|_| w.flush())
                .map_err(|e| PyIOError::new_err(e.to_string()))?;
            file.sync_all()
                .map_err(|e| PyIOError::new_err(e.to_string()))?;
        }

        fs::rename(&tmp, path)
            .map_err(|e| PyIOError::new_err(format!("rename: {e}")))?;

        let kb = fs::metadata(path)
            .map(|m| m.len())
            .unwrap_or(0) as f64
            / 1024.0;
        println!(
            "✅  {}  —  {} objects · {} keys · {:.1} KB",
            path.display(),
            self.objects.len(),
            self.keys.len(),
            kb
        );
        Ok(())
    }

    fn __repr__(&self) -> String {
        format!(
            "_FuseWriter(objects={}, keys={})",
            self.objects.len(),
            self.keys.len()
        )
    }
}

// ─── _FuseReader ─────────────────────────────────────────────────────────────

/// Low-level reader — returns raw msgpack bytes.
/// **Use `fusedb.FuseReader` (Python wrapper) instead of this directly.**
///
/// All read methods are GIL-safe and thread-safe:
///   • `sorted_keys`, `sorted_offsets`, `hash` are read-only after `__init__`
///   • `mm` (mmap) is read-only; the OS shares physical pages across processes
#[pyclass(name = "_FuseReader")]
pub struct FuseReader {
    path: PathBuf,
    mm:   Mmap,
    idx:  Index,
}

#[pymethods]
impl FuseReader {
    #[new]
    #[pyo3(signature = (path, verify = true))]
    fn new(path: &str, verify: bool) -> PyResult<Self> {
        let path = PathBuf::from(path);
        let file = File::open(&path)
            .map_err(|e| PyIOError::new_err(format!("cannot open {path:?}: {e}")))?;
        // SAFETY: We never write through this mapping. The file is opened
        // read-only; concurrent writers use atomic rename, so the mapped
        // region is always a complete, consistent snapshot.
        let mm = unsafe { Mmap::map(&file) }
            .map_err(|e| PyIOError::new_err(format!("mmap failed: {e}")))?;
        let idx = Index::build(&mm, verify)?;
        Ok(Self { path, mm, idx })
    }

    // ── lookups ───────────────────────────────────────────────────────────────

    /// O(1) exact-match lookup. Returns raw msgpack bytes or `None`.
    fn get_raw<'py>(
        &self,
        py: Python<'py>,
        key: &Bound<'_, PyAny>,
    ) -> PyResult<Option<Bound<'py, PyBytes>>> {
        let k = extract_key(key)?;
        match self.idx.hash.get(&k) {
            None          => Ok(None),
            Some(&offset) => {
                let raw = read_raw_bytes(&self.mm, offset)?;
                Ok(Some(PyBytes::new(py, &raw)))
            }
        }
    }

    /// Key presence check — no I/O beyond the in-memory hash index.
    fn exists(&self, key: &Bound<'_, PyAny>) -> PyResult<bool> {
        Ok(self.idx.hash.contains_key(&extract_key(key)?))
    }

    /// Sorted prefix scan. Returns `list[tuple[str, bytes]]`.
    fn prefix_raw(
        &self,
        py: Python<'_>,
        prefix: &Bound<'_, PyAny>,
    ) -> PyResult<Py<PyList>> {
        let p   = extract_key(prefix)?;
        let pos = self
            .idx
            .sorted_keys
            .partition_point(|k| k.as_slice() < p.as_slice());

        let list = PyList::empty(py);
        let mut i = pos;
        while i < self.idx.sorted_keys.len()
            && self.idx.sorted_keys[i].starts_with(&p)
        {
            let key_str = String::from_utf8_lossy(&self.idx.sorted_keys[i]).into_owned();
            let raw     = read_raw_bytes(&self.mm, self.idx.sorted_offsets[i])?;
            let pair    = (key_str, PyBytes::new(py, &raw));
            list.append(pair)?;
            i += 1;
        }
        Ok(list.into())
    }

    /// All keys in sorted order.
    fn keys(&self) -> Vec<String> {
        self.idx
            .sorted_keys
            .iter()
            .map(|k| String::from_utf8_lossy(k).into_owned())
            .collect()
    }

    /// All `(key, raw_bytes)` pairs in sorted key order.
    /// Objects that share a file offset are read once and cloned.
    fn items_raw(&self, py: Python<'_>) -> PyResult<Py<PyList>> {
        let list = PyList::empty(py);
        // Cache raw bytes by offset to avoid re-reading identical objects.
        let mut byte_cache: AHashMap<u64, Vec<u8>> = AHashMap::new();

        for (key_bytes, &offset) in self
            .idx
            .sorted_keys
            .iter()
            .zip(self.idx.sorted_offsets.iter())
        {
            let raw = match byte_cache.get(&offset) {
                Some(cached) => cached.clone(),
                None => {
                    let b = read_raw_bytes(&self.mm, offset)?;
                    byte_cache.insert(offset, b.clone());
                    b
                }
            };
            let key_str = String::from_utf8_lossy(key_bytes).into_owned();
            let pair    = (key_str, PyBytes::new(py, &raw));
            list.append(pair)?;
        }
        Ok(list.into())
    }

    /// Unique objects only (deduplicated by file offset).
    fn objects_raw(&self, py: Python<'_>) -> PyResult<Py<PyList>> {
        let list = PyList::empty(py);
        let mut seen: AHashMap<u64, ()> = AHashMap::new();
        for &offset in &self.idx.sorted_offsets {
            if seen.insert(offset, ()).is_none() {
                let raw = read_raw_bytes(&self.mm, offset)?;
                list.append(PyBytes::new(py, &raw))?;
            }
        }
        Ok(list.into())
    }

    // ── introspection ─────────────────────────────────────────────────────────

    /// Return file metadata as a `dict`.
    fn stats<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let d = PyDict::new(py);
        d.set_item("path",         self.path.to_string_lossy().as_ref())?;
        d.set_item("version",      VERSION)?;
        d.set_item("num_keys",     self.idx.num_keys)?;
        d.set_item("num_objects",  self.idx.num_objects)?;
        d.set_item("index_offset", self.idx.index_offset)?;
        d.set_item("data_offset",  HEADER_SIZE as u64)?;
        d.set_item("file_size_kb", self.idx.file_size as f64 / 1024.0)?;
        d.set_item("file_crc32",   format!("{:#010x}", self.idx.stored_crc))?;
        Ok(d)
    }

    /// Deep integrity check: whole-file CRC32 + per-object CRC32.
    fn verify(&self) -> PyResult<bool> {
        let data     = self.mm.as_ref();
        let computed = crc32(&data[HEADER_SIZE..]);
        if computed != self.idx.stored_crc {
            return Err(corrupt(format!(
                "file CRC32 mismatch  stored={:#010x}  computed={:#010x}",
                self.idx.stored_crc, computed
            )));
        }
        let mut seen: AHashMap<u64, ()> = AHashMap::new();
        for &offset in &self.idx.sorted_offsets {
            if seen.insert(offset, ()).is_none() {
                let o       = offset as usize;
                if o + OBJ_HDR_SZ > data.len() {
                    return Err(corrupt(format!(
                        "object offset {offset} out of bounds"
                    )));
                }
                let obj_len = u32::from_be_bytes(
                    data[o..o + 4].try_into().unwrap(),
                ) as usize;
                let stored  = u32::from_be_bytes(
                    data[o + 4..o + 8].try_into().unwrap(),
                );
                let raw     = &data[o + OBJ_HDR_SZ..o + OBJ_HDR_SZ + obj_len];
                if crc32(raw) != stored {
                    return Err(corrupt(format!(
                        "object CRC32 mismatch at offset {offset}"
                    )));
                }
            }
        }
        Ok(true)
    }

    // ── context-manager protocol ──────────────────────────────────────────────

    fn close(&self) {}

    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __exit__(
        &self,
        _exc_type: Option<Bound<'_, PyAny>>,
        _exc_val:  Option<Bound<'_, PyAny>>,
        _exc_tb:   Option<Bound<'_, PyAny>>,
    ) -> bool {
        false // do not suppress exceptions
    }

    fn __repr__(&self) -> String {
        format!(
            "_FuseReader('{}', keys={}, objects={})",
            self.path.display(),
            self.idx.num_keys,
            self.idx.num_objects,
        )
    }
}

// ─── module entry-point ──────────────────────────────────────────────────────

/// fusedb._fusedb — Rust extension module
#[pymodule]
fn _fusedb(m: &Bound<'_, pyo3::types::PyModule>) -> PyResult<()> {
    m.add_class::<FuseWriter>()?;
    m.add_class::<FuseReader>()?;
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}