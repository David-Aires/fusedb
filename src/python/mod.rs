// src/python/mod.rs
// ──────────────────────────────────────────────────────────────────────────────
// All PyO3-specific code lives here.  Nothing outside this module imports pyo3.

pub mod error;  // From<FuseError> for PyErr
pub mod reader; // #[pyclass] _FuseReader
pub mod util;   // extract_key
pub mod writer; // #[pyclass] _FuseWriter

pub use reader::FuseReader;
pub use writer::FuseWriter;