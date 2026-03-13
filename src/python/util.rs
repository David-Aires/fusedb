// src/python/util.rs
// ──────────────────────────────────────────────────────────────────────────────
// Shared helpers used by the Python shims.

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

/// Accept `str` or `bytes` as a lookup key and return owned bytes.
///
/// Every public Python method that accepts a key goes through this function —
/// it is the single place where "Python key → Rust bytes" is handled.
pub fn extract_key(key: &Bound<'_, PyAny>) -> PyResult<Vec<u8>> {
    if let Ok(s) = key.extract::<String>() {
        return Ok(s.into_bytes());
    }
    if let Ok(b) = key.extract::<Vec<u8>>() {
        return Ok(b);
    }
    Err(PyValueError::new_err("key must be str or bytes"))
}
