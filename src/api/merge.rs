// src/api/merge.rs
// ──────────────────────────────────────────────────────────────────────────────
// Merging several .fsdb files into one, content-addressing objects on the way.
//
// The algorithm is deliberately identical to Python's `fusedb.merge()`:
// sources in argument order, keys in sorted order within each source, objects
// deduplicated by exact payload bytes, later keys overwriting earlier ones.
// Same inputs therefore produce the same file in both languages, byte for byte.

use std::path::Path;

use crate::core::{BuildReport, FuseResult};

use super::reader::FuseReader;
use super::writer::FuseWriter;

/// Merge two or more `.fsdb` files into `output`.
///
/// Objects with identical payloads are stored once, so merging overlapping
/// databases costs disk space only for what is genuinely new.
///
/// # Example
/// ```no_run
/// fusedb::merge(&["geo_v1.fsdb", "geo_v2.fsdb"], "geo_merged.fsdb")?;
/// # Ok::<(), fusedb::FuseError>(())
/// ```
///
/// # Errors
/// Propagates any read error from a source (including CRC failure — every
/// source is verified on open) and any write error on `output`.
pub fn merge<P, Q>(sources: &[P], output: Q) -> FuseResult<BuildReport>
where
    P: AsRef<Path>,
    Q: AsRef<Path>,
{
    let mut writer = FuseWriter::new();
    for src in sources {
        merge_into(&mut writer, src)?;
    }
    writer.build(output)
}

/// Fold one `.fsdb` file into an existing [`FuseWriter`].
///
/// Use this to merge databases with freshly-added records in a single pass:
///
/// ```no_run
/// use fusedb::{merge_into, FuseWriter};
///
/// let mut w = FuseWriter::new();
/// merge_into(&mut w, "geo_v1.fsdb")?;
/// w.add("new.example.com", &"fresh record")?;
/// w.build("geo_v2.fsdb")?;
/// # Ok::<(), fusedb::FuseError>(())
/// ```
pub fn merge_into(writer: &mut FuseWriter, source: impl AsRef<Path>) -> FuseResult<()> {
    let db = FuseReader::open(source)?;
    let core = db.core();

    for (key, &offset) in core.sorted_keys().iter().zip(core.sorted_offsets()) {
        let raw = core.read_at(offset)?;
        let oid = writer.add_object_raw_deduped(&raw);
        writer.add_key(key, oid)?;
    }
    Ok(())
}
