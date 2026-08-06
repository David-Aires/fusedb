//! Build a database, read it back, and inspect it.
//!
//!     cargo run --example quickstart

use std::collections::BTreeMap;

use fusedb::{FuseReader, FuseWriter};

fn main() -> Result<(), fusedb::FuseError> {
    let path = std::env::temp_dir().join("fusedb-quickstart.fsdb");

    // ── write ────────────────────────────────────────────────────────────────
    let mut w = FuseWriter::new();

    let mut google = BTreeMap::new();
    google.insert("company", "Google");
    google.insert("cc", "US");

    // One object, three keys — the payload is stored exactly once.
    let oid = w.add_object(&google)?;
    w.add_key("google.com", oid)?;
    w.add_key("8.8.8.8", oid)?;
    w.add_key("8.8.4.4", oid)?;

    // add() is add_object + add_key in one step.
    let mut cloudflare = BTreeMap::new();
    cloudflare.insert("company", "Cloudflare");
    cloudflare.insert("cc", "US");
    w.add("1.1.1.1", &cloudflare)?;

    let report = w.build(&path)?;
    println!(
        "wrote {} — {} objects, {} keys, {} bytes",
        path.display(),
        report.num_objects,
        report.num_keys,
        report.file_size,
    );

    // ── read ─────────────────────────────────────────────────────────────────
    let db = FuseReader::open(&path)?;

    let hit: Option<BTreeMap<String, String>> = db.get("8.8.8.8")?;
    println!("8.8.8.8      -> {hit:?}");
    println!("exists(1.1.1.1) = {}", db.exists("1.1.1.1"));

    // Lazy, sorted prefix scan.
    for entry in db.prefix_iter("8.8.") {
        let (key, raw) = entry?;
        println!("prefix 8.8.  -> {key} ({} bytes)", raw.len());
    }

    let stats = db.stats();
    println!(
        "{} keys over {} objects, {:.1} KB, crc32 {:#010x}",
        stats.num_keys,
        stats.num_objects,
        stats.file_size_kb(),
        stats.file_crc32,
    );

    db.verify()?;
    println!("integrity: ok");

    std::fs::remove_file(&path).ok();
    Ok(())
}
