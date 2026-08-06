// tests/rust_api.rs
// ──────────────────────────────────────────────────────────────────────────────
// The public Rust surface, exercised without Python in the picture.
//
// `interop.rs` proves the two implementations agree; this file proves the Rust
// one is correct on its own terms, and runs everywhere `cargo test` runs.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::json;
use tempfile::TempDir;

use fusedb::{merge, merge_into, FuseError, FuseReader, FuseWriter, MAX_KEY_LEN, VERSION};

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
struct Org {
    company: String,
    asn: u32,
}

fn org(company: &str, asn: u32) -> Org {
    Org {
        company: company.into(),
        asn,
    }
}

/// One object reached by three keys, plus a second standalone record.
fn sample(dir: &TempDir) -> std::path::PathBuf {
    let path = dir.path().join("sample.fsdb");
    let mut w = FuseWriter::new();

    let google = w.add_object(&org("Google", 15169)).unwrap();
    w.add_key("google.com", google).unwrap();
    w.add_key("8.8.8.8", google).unwrap();
    w.add_key("8.8.4.4", google).unwrap();
    w.add("cloudflare.com", &org("Cloudflare", 13335)).unwrap();

    w.build(&path).unwrap();
    path
}

#[test]
fn round_trip_typed_values() {
    let dir = TempDir::new().unwrap();
    let db = FuseReader::open(sample(&dir)).unwrap();

    assert_eq!(
        db.get::<Org>("google.com").unwrap(),
        Some(org("Google", 15169))
    );
    assert_eq!(
        db.get::<Org>("8.8.4.4").unwrap(),
        Some(org("Google", 15169))
    );
    assert_eq!(db.get::<Org>("nope.example").unwrap(), None);
    assert!(db.exists("8.8.8.8"));
    assert!(!db.exists("8.8.8.9"));
    assert_eq!(db.len(), 4);
    assert!(!db.is_empty());
}

#[test]
fn build_report_matches_file() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("report.fsdb");

    let mut w = FuseWriter::new();
    let oid = w.add_object(&org("Google", 15169)).unwrap();
    w.add_key("a", oid).unwrap();
    w.add_key("b", oid).unwrap();

    let report = w.build(&path).unwrap();
    assert_eq!(report.num_keys, 2);
    assert_eq!(report.num_objects, 1);
    assert_eq!(report.file_size, std::fs::metadata(&path).unwrap().len());
}

#[test]
fn deduped_and_plain_object_storage_differ() {
    let mut plain = FuseWriter::new();
    plain.add_object(&org("Google", 15169)).unwrap();
    plain.add_object(&org("Google", 15169)).unwrap();
    assert_eq!(plain.num_objects(), 2, "add_object always appends");

    let mut deduped = FuseWriter::new();
    let first = deduped.add_object_deduped(&org("Google", 15169)).unwrap();
    let second = deduped.add_object_deduped(&org("Google", 15169)).unwrap();
    assert_eq!(first, second);
    assert_eq!(deduped.num_objects(), 1, "identical payloads collapse");
}

#[test]
fn prefix_scan_is_sorted_and_lazy() {
    let dir = TempDir::new().unwrap();
    let db = FuseReader::open(sample(&dir)).unwrap();

    let hits: Vec<String> = db.prefix_iter(b"8.8.").map(|e| e.unwrap().0).collect();
    assert_eq!(hits, ["8.8.4.4", "8.8.8.8"]);

    let collected: Vec<String> = db
        .prefix::<Org>("8.8.")
        .unwrap()
        .into_iter()
        .map(|(k, _)| k)
        .collect();
    assert_eq!(collected, hits, "prefix() and prefix_iter() must agree");

    assert_eq!(db.prefix_iter(b"zzz").count(), 0);
    // An empty prefix matches everything, in index order.
    assert_eq!(db.prefix_iter(b"").count(), db.len());
}

#[test]
fn items_objects_and_keys() {
    let dir = TempDir::new().unwrap();
    let db = FuseReader::open(sample(&dir)).unwrap();

    assert_eq!(
        db.keys(),
        ["8.8.4.4", "8.8.8.8", "cloudflare.com", "google.com"]
    );
    assert_eq!(db.items::<Org>().unwrap().len(), 4);
    assert_eq!(
        db.objects::<Org>().unwrap().len(),
        2,
        "three aliases resolve to one stored object"
    );
}

#[test]
fn stats_describe_the_file() {
    let dir = TempDir::new().unwrap();
    let path = sample(&dir);
    let db = FuseReader::open(&path).unwrap();
    let stats = db.stats();

    assert_eq!(stats.version, VERSION);
    assert_eq!(stats.num_keys, 4);
    assert_eq!(stats.num_objects, 2);
    assert_eq!(stats.data_offset, 40);
    assert_eq!(stats.file_size, std::fs::metadata(&path).unwrap().len());
    assert!(stats.index_offset > stats.data_offset);
    assert!(stats.file_size_kb() > 0.0);
    assert_eq!(stats.path, path);
}

#[test]
fn verify_accepts_intact_and_rejects_corrupt() {
    let dir = TempDir::new().unwrap();
    let path = sample(&dir);

    assert!(FuseReader::open(&path).unwrap().verify().is_ok());
    assert!(FuseReader::open(&path).unwrap().is_valid());

    let mut bytes = std::fs::read(&path).unwrap();
    let victim = bytes.len() - 1;
    bytes[victim] ^= 0xff;
    let corrupt = dir.path().join("corrupt.fsdb");
    std::fs::write(&corrupt, &bytes).unwrap();

    // open() verifies by default…
    assert!(matches!(
        FuseReader::open(&corrupt),
        Err(FuseError::Corrupt(_))
    ));
    // …and open_unverified() defers the check to verify().
    let lazy = FuseReader::open_unverified(&corrupt).unwrap();
    assert!(matches!(lazy.verify(), Err(FuseError::Corrupt(_))));
    assert!(!lazy.is_valid());
}

#[test]
fn bad_magic_and_bad_version_are_rejected() {
    let dir = TempDir::new().unwrap();
    let path = sample(&dir);

    let mut bytes = std::fs::read(&path).unwrap();
    bytes[0] = b'X';
    let bad_magic = dir.path().join("magic.fsdb");
    std::fs::write(&bad_magic, &bytes).unwrap();
    assert!(matches!(
        FuseReader::open(&bad_magic),
        Err(FuseError::Corrupt(_))
    ));

    let mut bytes = std::fs::read(&path).unwrap();
    bytes[4] = 99;
    let bad_version = dir.path().join("version.fsdb");
    std::fs::write(&bad_version, &bytes).unwrap();
    assert!(matches!(
        FuseReader::open(&bad_version),
        Err(FuseError::Version(99))
    ));
}

#[test]
fn oversized_key_is_rejected_not_truncated() {
    let mut w = FuseWriter::new();
    let oid = w.add_object_raw(b"payload");
    let huge = vec![b'k'; MAX_KEY_LEN + 1];

    // A silent `as u16` truncation here would write an index entry whose length
    // prefix disagrees with its bytes — an unreadable file.
    assert!(matches!(
        w.add_key(&huge, oid),
        Err(FuseError::InvalidArg(_))
    ));
    assert!(w.add_key(vec![b'k'; MAX_KEY_LEN], oid).is_ok());
}

#[test]
fn unknown_object_id_is_rejected() {
    let mut w = FuseWriter::new();
    assert!(matches!(
        w.add_key("orphan", 0),
        Err(FuseError::InvalidArg(_))
    ));
}

#[test]
fn binary_keys_survive_a_round_trip() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("bin.fsdb");
    let key: &[u8] = &[0x00, 0xff, 0x10, 0x80];

    let mut w = FuseWriter::new();
    w.add_raw(key, b"payload").unwrap();
    w.build(&path).unwrap();

    let db = FuseReader::open(&path).unwrap();
    assert_eq!(db.get_raw(key).unwrap().as_deref(), Some(&b"payload"[..]));
    assert_eq!(db.keys_raw(), [key.to_vec()]);
}

#[test]
fn last_write_wins_for_duplicate_keys() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("dup.fsdb");

    let mut w = FuseWriter::new();
    let a = w.add_object_raw(b"first");
    let b = w.add_object_raw(b"second");
    w.add_key("k", a).unwrap();
    w.add_key("k", b).unwrap();
    w.build(&path).unwrap();

    let db = FuseReader::open(&path).unwrap();
    assert_eq!(db.len(), 1);
    assert_eq!(db.get_raw("k").unwrap().as_deref(), Some(&b"second"[..]));
}

#[test]
fn empty_database_is_valid() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("empty.fsdb");

    let w = FuseWriter::new();
    assert!(w.is_empty());
    w.build(&path).unwrap();

    let db = FuseReader::open(&path).unwrap();
    assert!(db.is_empty());
    assert_eq!(db.len(), 0);
    assert!(db.verify().is_ok());
    assert_eq!(db.get_raw("anything").unwrap(), None);
}

#[test]
fn merge_dedups_across_sources() {
    let dir = TempDir::new().unwrap();
    let a = dir.path().join("a.fsdb");
    let b = dir.path().join("b.fsdb");
    let out = dir.path().join("merged.fsdb");

    let mut wa = FuseWriter::new();
    wa.add("google.com", &org("Google", 15169)).unwrap();
    wa.add("shared.example", &org("Shared", 1)).unwrap();
    wa.build(&a).unwrap();

    let mut wb = FuseWriter::new();
    wb.add("cloudflare.com", &org("Cloudflare", 13335)).unwrap();
    wb.add("shared.example", &org("Shared", 1)).unwrap();
    wb.build(&b).unwrap();

    let report = merge(&[&a, &b], &out).unwrap();
    assert_eq!(report.num_keys, 3);
    assert_eq!(report.num_objects, 3, "the shared payload is stored once");

    let db = FuseReader::open(&out).unwrap();
    assert_eq!(
        db.get::<Org>("shared.example").unwrap(),
        Some(org("Shared", 1))
    );
    assert!(db.verify().is_ok());
}

#[test]
fn merge_into_folds_a_source_into_a_live_writer() {
    let dir = TempDir::new().unwrap();
    let base = dir.path().join("base.fsdb");
    let out = dir.path().join("out.fsdb");

    let mut w = FuseWriter::new();
    w.add("old.example", &org("Old", 1)).unwrap();
    w.build(&base).unwrap();

    let mut w = FuseWriter::new();
    merge_into(&mut w, &base).unwrap();
    w.add("new.example", &org("New", 2)).unwrap();
    w.build(&out).unwrap();

    let db = FuseReader::open(&out).unwrap();
    assert_eq!(db.len(), 2);
    assert!(db.exists("old.example") && db.exists("new.example"));
}

#[test]
fn later_sources_win_on_key_conflicts() {
    let dir = TempDir::new().unwrap();
    let a = dir.path().join("a.fsdb");
    let b = dir.path().join("b.fsdb");
    let out = dir.path().join("out.fsdb");

    let mut wa = FuseWriter::new();
    wa.add("k", &org("Stale", 1)).unwrap();
    wa.build(&a).unwrap();

    let mut wb = FuseWriter::new();
    wb.add("k", &org("Fresh", 2)).unwrap();
    wb.build(&b).unwrap();

    merge(&[&a, &b], &out).unwrap();
    let db = FuseReader::open(&out).unwrap();
    assert_eq!(db.get::<Org>("k").unwrap(), Some(org("Fresh", 2)));
}

#[test]
fn raw_and_typed_apis_see_the_same_bytes() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("mixed.fsdb");

    let value = json!({"company": "Google", "asn": 15169});
    let mut w = FuseWriter::new();
    w.add("typed", &value).unwrap();
    w.add_raw("raw", fusedb::encode(&value).unwrap()).unwrap();
    w.build(&path).unwrap();

    let db = FuseReader::open(&path).unwrap();
    assert_eq!(db.get_raw("typed").unwrap(), db.get_raw("raw").unwrap());
    assert_eq!(
        db.get::<BTreeMap<String, serde_json::Value>>("raw")
            .unwrap(),
        Some(value.as_object().unwrap().clone().into_iter().collect()),
    );
}

#[test]
fn build_is_atomic_and_leaves_no_temp_file() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("atomic.fsdb");

    let mut w = FuseWriter::new();
    w.add_raw("k", b"v").unwrap();
    w.build(&path).unwrap();

    assert!(path.exists());
    assert!(
        !path.with_extension("fsdb.tmp").exists(),
        "the temp file must be renamed away, not left behind"
    );
}
