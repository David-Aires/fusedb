// src/api/stats.rs
// ──────────────────────────────────────────────────────────────────────────────
// File metadata, mirroring the dict returned by Python's `FuseReader.stats()`.

use std::path::PathBuf;

/// Metadata describing an open `.fsdb` file.
///
/// Field-for-field the same information Python's `FuseReader.stats()` returns.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Stats {
    /// Path the reader was opened from.
    pub path: PathBuf,
    /// On-disk format version (always [`crate::core::format::VERSION`] for files this build can read).
    pub version: u8,
    /// Number of index entries.
    pub num_keys: u32,
    /// Number of unique objects in the data section.
    pub num_objects: u32,
    /// Byte offset where the index section starts.
    pub index_offset: u64,
    /// Byte offset where the data section starts (always the header size).
    pub data_offset: u64,
    /// Total file size in bytes.
    pub file_size: u64,
    /// Whole-file CRC32 as stored in the header.
    pub file_crc32: u32,
}

impl Stats {
    /// File size in kibibytes — the unit Python's `stats()["file_size_kb"]` uses.
    #[inline]
    pub fn file_size_kb(&self) -> f64 {
        self.file_size as f64 / 1024.0
    }
}
