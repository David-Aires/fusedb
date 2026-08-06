// src/api/mod.rs
// ──────────────────────────────────────────────────────────────────────────────
// The public, idiomatic Rust API.
//
// `core` is the format engine and speaks only raw bytes.  This layer adds the
// ergonomics a Rust consumer expects — `AsRef<Path>`, `AsRef<[u8]>` keys,
// iterators, and (behind the default `msgpack` feature) typed serde values
// encoded exactly the way the Python package encodes them.
//
// Nothing here changes a single byte of the file format.

pub mod merge;
pub mod reader;
pub mod stats;
pub mod watch;
pub mod writer;

pub use merge::{merge, merge_into};
pub use reader::{FuseReader, PrefixIter};
pub use stats::Stats;
pub use watch::{FuseWatcher, ReloadableReader, WatcherBuilder};
pub use writer::FuseWriter;

#[cfg(feature = "msgpack")]
pub mod codec;

#[cfg(feature = "msgpack")]
pub use codec::{decode, encode};
