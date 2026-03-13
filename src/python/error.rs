// src/python/error.rs
// ──────────────────────────────────────────────────────────────────────────────
// The single point where `FuseError` crosses into PyO3-land.
// Core code never imports pyo3; this module imports both.

use pyo3::exceptions::{PyIOError, PyRuntimeError, PyValueError};
use pyo3::PyErr;

use crate::core::error::FuseError;

impl From<FuseError> for PyErr {
    fn from(e: FuseError) -> Self {
        match &e {
            FuseError::Io(_)          => PyIOError::new_err(e.to_string()),
            FuseError::InvalidArg(_)  => PyValueError::new_err(e.to_string()),
            FuseError::Corrupt(_)
            | FuseError::Version(_)   => PyRuntimeError::new_err(e.to_string()),
        }
    }
}