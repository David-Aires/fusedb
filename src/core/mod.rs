// src/core/mod.rs
// ──────────────────────────────────────────────────────────────────────────────
// Pure-Rust core library.  Zero PyO3 knowledge.
//
// Consumers (benchmarks, integration tests, future Rust crates) import from
// here.  The Python shim in `crate::python` wraps these types.

pub mod error;
pub mod format;
pub mod reader;
pub mod writer;

// Flatten the most-used types to `fusedb::core::*`
pub use error::{FuseError, FuseResult};
pub use reader::ReaderCore;
pub use writer::WriterCore;