// src/lib.rs — FuseDB crate root
// ──────────────────────────────────────────────────────────────────────────────
//
// Module layout
// ─────────────
//   crate::api           The public Rust API.  What `cargo add fusedb` gives you.
//     ::reader           FuseReader, PrefixIter
//     ::writer           FuseWriter
//     ::merge            merge, merge_into
//     ::watch            ReloadableReader, FuseWatcher, WatcherBuilder
//     ::stats            Stats
//
//   crate::core          Format engine — raw bytes only, zero PyO3.
//     ::error            FuseError, FuseResult
//     ::format           Binary format constants, Header, Index, read_raw, crc32
//     ::reader           ReaderCore
//     ::writer           WriterCore, BuildReport
//
//   crate::python        PyO3 shims, behind the `python` feature.  The only
//                        place pyo3 is imported, enabled only by maturin.
//
// The rule: `core` and `api` never import `pyo3`.  `python` never contains logic.

//! A read-optimised binary database where many keys share one object — with
//! zero duplication on disk.
//!
//! FuseDB stores each payload once and lets any number of keys point at it.
//! Reads are memory-mapped: opening a database is a `mmap` plus an index parse,
//! and a lookup is a hash probe followed by a single page read.
//!
//! # Quick start
//!
//! ```no_run
//! use fusedb::{FuseReader, FuseWriter};
//! use std::collections::BTreeMap;
//!
//! let mut w = FuseWriter::new();
//!
//! let mut google = BTreeMap::new();
//! google.insert("company", "Google");
//!
//! let oid = w.add_object(&google)?;   // stored once…
//! w.add_key("google.com", oid)?;      // …reached by three keys
//! w.add_key("8.8.8.8", oid)?;
//! w.add_key("8.8.4.4", oid)?;
//! w.build("db.fsdb")?;
//!
//! let db = FuseReader::open("db.fsdb")?;
//! let hit: Option<BTreeMap<String, String>> = db.get("8.8.8.8")?;
//! assert_eq!(hit.unwrap()["company"], "Google");
//! # Ok::<(), fusedb::FuseError>(())
//! ```
//!
//! # One format, two languages
//!
//! There is no "Rust format" and no "Python format" — only the FuseDB format.
//! A file written by this crate opens in the [`fusedb` Python package] and vice
//! versa, with no conversion step: same magic and version, same big-endian
//! header, same CRC32 placement, same sorted index layout, and — with the
//! default `msgpack` feature — the same MessagePack value encoding.
//!
//! ```no_run
//! # use fusedb::FuseReader;
//! // reads a file `FuseWriter().build("db.fsdb")` produced in Python
//! let db = FuseReader::open("db.fsdb")?;
//! # Ok::<(), fusedb::FuseError>(())
//! ```
//!
//! [`fusedb` Python package]: https://pypi.org/project/fusedb/
//!
//! # Live updates
//!
//! `.fsdb` files are immutable once built; you update one by rebuilding it and
//! swapping. [`ReloadableReader`] does the swap on demand, [`FuseWatcher`] does
//! it from a background thread. Either way you read them exactly like a
//! [`FuseReader`] — every call resolves the current file, so there is no swap
//! bookkeeping and no way to keep reading a replaced database.
//!
//! ```no_run
//! use std::time::Duration;
//! use fusedb::FuseWatcher;
//!
//! let watcher = FuseWatcher::builder("live.fsdb")
//!     .interval(Duration::from_secs(30))
//!     .on_reload(|db| eprintln!("reloaded: {} keys", db.len()))
//!     .spawn()?;
//!
//! println!("{:?}", watcher.get_raw("8.8.8.8")?);   // always the newest build
//!
//! // For a batch, resolve the reader once with load() and reuse it.
//! let db = watcher.load();
//! for key in ["8.8.8.8", "8.8.4.4"] {
//!     println!("{:?}", db.get_raw(key)?);
//! }
//! # Ok::<(), fusedb::FuseError>(())
//! ```
//!
//! # Features
//!
//! - **`msgpack`** *(default)* — the typed [`FuseWriter::add`] /
//!   [`FuseReader::get`] API, encoding values exactly the way the Python
//!   package does. Disable it (`default-features = false`) for a
//!   dependency-light build that speaks only raw bytes.
//! - **`python`** — PyO3 bindings. Enabled by maturin when building the Python
//!   wheel; Rust consumers never need it.

pub mod api;
pub mod core;

#[cfg(feature = "python")]
pub mod python;

#[cfg(feature = "msgpack")]
pub use api::{decode, encode};
pub use api::{
    merge, merge_into, FuseReader, FuseWatcher, FuseWriter, PrefixIter, ReloadableReader, Stats,
    WatcherBuilder,
};
pub use core::format::{MAGIC, MAX_KEY_LEN, MAX_OBJECT_LEN, VERSION};
pub use core::{BuildReport, FuseError, FuseResult};

/// Version of this crate, as published on crates.io.
pub const CRATE_VERSION: &str = env!("CARGO_PKG_VERSION");

/// fusedb._fusedb — native extension module entry-point
#[cfg(feature = "python")]
#[pyo3::pymodule]
fn _fusedb(m: &pyo3::Bound<'_, pyo3::types::PyModule>) -> pyo3::PyResult<()> {
    use pyo3::prelude::*;

    m.add_class::<python::FuseWriter>()?;
    m.add_class::<python::FuseReader>()?;
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}
