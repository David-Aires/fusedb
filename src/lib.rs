// src/lib.rs — FuseDB crate root
// ──────────────────────────────────────────────────────────────────────────────
//
// Module layout
// ─────────────
//   crate::core          Pure Rust — zero PyO3.  Usable from any Rust consumer.
//     ::error            FuseError, FuseResult
//     ::format           Binary format constants, Header, Index, read_raw, crc32
//     ::reader           ReaderCore
//     ::writer           WriterCore
//
//   crate::python        PyO3 shims — the only place pyo3 is imported.
//     ::error            From<FuseError> for PyErr
//     ::util             extract_key  (str/bytes → Vec<u8>)
//     ::writer           #[pyclass] _FuseWriter  wraps WriterCore
//     ::reader           #[pyclass] _FuseReader  wraps ReaderCore
//
// The rule: `core` never imports `pyo3`.  `python` never contains logic.

pub mod core;
pub mod python;

use pyo3::prelude::*;

/// fusedb._fusedb — native extension module entry-point
#[pymodule]
fn _fusedb(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<python::FuseWriter>()?;
    m.add_class::<python::FuseReader>()?;
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}