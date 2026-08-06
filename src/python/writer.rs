// src/python/writer.rs
// ──────────────────────────────────────────────────────────────────────────────
// Thin PyO3 wrapper around `WriterCore`.
//
// Responsibilities:
//   • Accept Python types (str/bytes keys, raw bytes from msgpack)
//   • Delegate every operation to `WriterCore`
//   • Convert `FuseError` → `PyErr` via the `From` impl in `python::error`
//
// This struct contains no business logic — it is purely a type-conversion shim.

use pyo3::prelude::*;

use crate::core::WriterCore;

use super::util::extract_key;

/// Low-level writer — accepts pre-serialised msgpack bytes per object.
/// **Use `fusedb.FuseWriter` (Python wrapper) instead of this directly.**
#[pyclass(name = "_FuseWriter")]
pub struct FuseWriter {
    inner: WriterCore,
}

#[pymethods]
impl FuseWriter {
    #[new]
    fn new() -> Self {
        Self {
            inner: WriterCore::new(),
        }
    }

    /// Store pre-encoded *raw_bytes* (msgpack). Returns the object ID.
    fn add_object_raw(&mut self, raw_bytes: &[u8]) -> usize {
        self.inner.add_object(raw_bytes)
    }

    /// Map *key* (str or bytes) to *obj_id*.
    fn add_key(&mut self, key: &Bound<'_, PyAny>, obj_id: usize) -> PyResult<()> {
        Ok(self.inner.add_key(&extract_key(key)?, obj_id)?)
    }

    /// Atomically write the `.fsdb` file: tmp → fsync → rename.
    fn build(&self, path: &str) -> PyResult<()> {
        let report = self.inner.build(path)?;
        // The progress line is Python-package UX, not library behaviour —
        // `WriterCore` stays silent so Rust consumers control their own output.
        println!(
            "✅  {}  —  {} objects · {} keys · {:.1} KB",
            path,
            report.num_objects,
            report.num_keys,
            report.file_size as f64 / 1024.0,
        );
        Ok(())
    }

    fn __repr__(&self) -> String {
        format!(
            "_FuseWriter(objects={}, keys={})",
            self.inner.num_objects(),
            self.inner.num_keys(),
        )
    }
}
