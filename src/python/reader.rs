// src/python/reader.rs
// ──────────────────────────────────────────────────────────────────────────────
// Thin PyO3 wrapper around `ReaderCore`.
//
// Responsibilities:
//   • Accept Python types (str/bytes keys)
//   • Convert Rust-native results to Python objects (PyBytes, PyList, PyDict)
//   • Implement the Python context-manager protocol (__enter__ / __exit__)
//   • Delegate every operation to `ReaderCore`
//   • Convert `FuseError` → `PyErr` via the `From` impl in `python::error`
//
// This struct contains no business logic — it is purely a type-conversion shim.

use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList};

use crate::core::format::{HEADER_SIZE, VERSION};
use crate::core::ReaderCore;

use super::util::extract_key;

/// Low-level reader — returns raw msgpack bytes.
/// **Use `fusedb.FuseReader` (Python wrapper) instead of this directly.**
///
/// Thread-safe for reads: the index is immutable after construction and the
/// mmap is read-only.  Share one instance freely across threads.
#[pyclass(name = "_FuseReader")]
pub struct FuseReader {
    inner: ReaderCore,
}

#[pymethods]
impl FuseReader {
    #[new]
    #[pyo3(signature = (path, verify = true))]
    fn new(path: &str, verify: bool) -> PyResult<Self> {
        Ok(Self {
            inner: ReaderCore::open(path, verify)?,
        })
    }

    // ── lookups ───────────────────────────────────────────────────────────────

    /// O(1) exact-match lookup. Returns raw msgpack bytes or `None`.
    fn get_raw<'py>(
        &self,
        py: Python<'py>,
        key: &Bound<'_, PyAny>,
    ) -> PyResult<Option<Bound<'py, PyBytes>>> {
        let k = extract_key(key)?;
        match self.inner.get(&k)? {
            None => Ok(None),
            Some(raw) => Ok(Some(PyBytes::new(py, &raw))),
        }
    }

    /// Key presence check — no I/O beyond the in-memory hash index.
    fn exists(&self, key: &Bound<'_, PyAny>) -> PyResult<bool> {
        Ok(self.inner.exists(&extract_key(key)?))
    }

    /// Sorted prefix scan. Returns `list[tuple[str, bytes]]`.
    fn prefix_raw(&self, py: Python<'_>, prefix: &Bound<'_, PyAny>) -> PyResult<Py<PyList>> {
        let p = extract_key(prefix)?;
        let list = PyList::empty(py);
        for (key_str, raw) in self.inner.prefix(&p)? {
            list.append((key_str, PyBytes::new(py, &raw)))?;
        }
        Ok(list.into())
    }

    /// All keys in sorted order.
    fn keys(&self) -> Vec<String> {
        self.inner.keys()
    }

    /// All `(key, raw_bytes)` pairs in sorted key order.
    fn items_raw(&self, py: Python<'_>) -> PyResult<Py<PyList>> {
        let list = PyList::empty(py);
        for (key_str, raw) in self.inner.items()? {
            list.append((key_str, PyBytes::new(py, &raw)))?;
        }
        Ok(list.into())
    }

    /// Unique objects only (deduplicated by file offset).
    fn objects_raw(&self, py: Python<'_>) -> PyResult<Py<PyList>> {
        let list = PyList::empty(py);
        for raw in self.inner.objects()? {
            list.append(PyBytes::new(py, &raw))?;
        }
        Ok(list.into())
    }

    // ── introspection ─────────────────────────────────────────────────────────

    /// Return file metadata as a `dict`.
    fn stats<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let d = PyDict::new(py);
        d.set_item("path", self.inner.path().to_string_lossy().as_ref())?;
        d.set_item("version", VERSION)?;
        d.set_item("num_keys", self.inner.num_keys())?;
        d.set_item("num_objects", self.inner.num_objects())?;
        d.set_item("index_offset", self.inner.index_offset())?;
        d.set_item("data_offset", HEADER_SIZE as u64)?;
        d.set_item("file_size_kb", self.inner.file_size() as f64 / 1024.0)?;
        d.set_item("file_crc32", format!("{:#010x}", self.inner.stored_crc()))?;
        Ok(d)
    }

    /// Deep CRC32 integrity check (whole-file + per-object).
    fn verify(&self) -> PyResult<bool> {
        Ok(self.inner.verify()?)
    }

    // ── context-manager protocol ──────────────────────────────────────────────

    fn close(&self) {}

    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __exit__(
        &self,
        _exc_type: Option<Bound<'_, PyAny>>,
        _exc_val: Option<Bound<'_, PyAny>>,
        _exc_tb: Option<Bound<'_, PyAny>>,
    ) -> bool {
        false // do not suppress exceptions
    }

    fn __repr__(&self) -> String {
        format!(
            "_FuseReader('{}', keys={}, objects={})",
            self.inner.path().display(),
            self.inner.num_keys(),
            self.inner.num_objects(),
        )
    }
}
