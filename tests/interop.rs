// tests/interop.rs
// ──────────────────────────────────────────────────────────────────────────────
// Cross-language compatibility suite.
//
// These tests are the contract behind the project's central promise: there is
// one FuseDB format, not a Rust one and a Python one. Every test here drives
// both implementations over the *same file* and fails if they ever disagree.
//
// Running them needs the Python package importable (`maturin develop`). When it
// is not, they skip — unless FUSEDB_REQUIRE_INTEROP=1, which CI sets so a
// broken environment surfaces as a failure instead of a silent green run.

use std::path::{Path, PathBuf};
use std::process::Command;

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tempfile::TempDir;

use fusedb::{merge, FuseReader, FuseWriter};

// ── the canonical dataset, mirrored from tests/interop_helper.py ─────────────

/// `(key, company)` — the two Google aliases and the two Cloudflare aliases
/// share one object each, which is what makes deduplication observable.
const RECORDS: &[(&str, &str)] = &[
    ("google.com", "Google"),
    ("cloudflare.com", "Cloudflare"),
    ("8.8.8.8", "Google"),
    ("8.8.4.4", "Google"),
    ("1.1.1.1", "Cloudflare"),
];

fn record(company: &str) -> Value {
    match company {
        "Google" => json!({"asn": 15169, "cc": "US", "company": "Google"}),
        "Cloudflare" => json!({"asn": 13335, "cc": "US", "company": "Cloudflare"}),
        other => panic!("unknown company {other}"),
    }
}

/// Field order matters: `to_vec_named` emits struct fields in declaration
/// order, and this order matches the literal in `interop_helper.py`.
#[derive(Serialize, Deserialize, PartialEq, Debug)]
struct Mixed {
    bool: bool,
    float: f64,
    int: u32,
    list: Vec<u8>,
    nested: Nested,
    neg: i32,
    null: Option<u8>,
    text: String,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
struct Nested {
    a: Inner,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
struct Inner {
    b: String,
}

fn mixed() -> Mixed {
    Mixed {
        bool: true,
        float: 1.5,
        int: 42,
        list: vec![1, 2, 3],
        nested: Nested {
            a: Inner { b: "c".into() },
        },
        neg: -7,
        null: None,
        text: "héllo wörld".into(),
    }
}

/// Build the canonical database with the Rust API, mirroring `cmd_write`.
fn write_canonical(path: &Path) {
    let mut w = FuseWriter::new();
    let mut by_company: std::collections::HashMap<&str, usize> = Default::default();

    for (key, company) in RECORDS {
        let oid = match by_company.get(company) {
            Some(&oid) => oid,
            None => {
                let oid = w.add_object(&record(company)).expect("encode record");
                by_company.insert(company, oid);
                oid
            }
        };
        w.add_key(key, oid).expect("add_key");
    }
    w.add("mixed", &mixed()).expect("add mixed");
    w.build(path).expect("build");
}

// ── Python harness ────────────────────────────────────────────────────────────

struct Python {
    exe: PathBuf,
    helper: PathBuf,
}

impl Python {
    /// Locate an interpreter that can `import fusedb`, preferring the repo venv.
    fn find() -> Option<Self> {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let helper = root.join("tests").join("interop_helper.py");

        let venv = if cfg!(windows) {
            root.join(".venv").join("Scripts").join("python.exe")
        } else {
            root.join(".venv").join("bin").join("python")
        };

        let mut candidates: Vec<PathBuf> = Vec::new();
        if let Ok(explicit) = std::env::var("FUSEDB_PYTHON") {
            candidates.push(PathBuf::from(explicit));
        }
        candidates.push(venv);
        candidates.push(PathBuf::from("python3"));
        candidates.push(PathBuf::from("python"));

        candidates.into_iter().find_map(|exe| {
            let ok = Command::new(&exe)
                .args(["-c", "import fusedb"])
                .output()
                .map(|o| o.status.success())
                .unwrap_or(false);
            ok.then(|| Self {
                exe,
                helper: helper.clone(),
            })
        })
    }

    /// Skip the test unless CI demanded interop coverage.
    fn find_or_skip(test: &str) -> Option<Self> {
        match Self::find() {
            Some(py) => Some(py),
            None => {
                assert!(
                    std::env::var("FUSEDB_REQUIRE_INTEROP").as_deref() != Ok("1"),
                    "FUSEDB_REQUIRE_INTEROP=1 but no interpreter can `import fusedb` \
                     — run `maturin develop` first"
                );
                eprintln!("skipping {test}: python `fusedb` package not importable");
                None
            }
        }
    }

    fn run(&self, args: &[&str]) -> String {
        let out = Command::new(&self.exe)
            .arg(&self.helper)
            .args(args)
            // The helper writes UTF-8 to `sys.stdout.buffer` explicitly; this is
            // belt-and-braces for anything that reaches stdout another way.
            // Without it, Python on Windows encodes with the ANSI code page and
            // "héllo wörld" comes back as cp1252, which is not valid UTF-8.
            .env("PYTHONIOENCODING", "utf-8")
            .env("PYTHONUTF8", "1")
            .output()
            .unwrap_or_else(|e| panic!("spawning {:?} failed: {e}", self.exe));

        assert!(
            out.status.success(),
            "python {args:?} exited {:?}\nstderr:\n{}",
            out.status.code(),
            String::from_utf8_lossy(&out.stderr),
        );
        String::from_utf8(out.stdout).unwrap_or_else(|e| {
            panic!(
                "python stdout is not UTF-8 at byte {}: {:?}",
                e.utf8_error().valid_up_to(),
                String::from_utf8_lossy(e.as_bytes()),
            )
        })
    }
}

/// The same canonical-JSON shape `cmd_read` prints, produced from Rust.
fn rust_read_report(path: &Path) -> Value {
    let db = FuseReader::open(path).expect("open");
    let stats = db.stats();

    let mut items = serde_json::Map::new();
    for (key, value) in db.items::<Value>().expect("items") {
        items.insert(key, value);
    }

    json!({
        "keys": db.keys(),
        "items": Value::Object(items),
        "num_keys": stats.num_keys,
        "num_objects": stats.num_objects,
        "unique_objects": db.objects_raw().expect("objects").len(),
        "prefix_8_8": db.prefix_iter(b"8.8.")
            .map(|e| e.expect("prefix entry").0)
            .collect::<Vec<_>>(),
        "index_offset": stats.index_offset,
        "data_offset": stats.data_offset,
        "file_crc32": format!("{:#010x}", stats.file_crc32),
        "version": stats.version,
        "verified": db.verify().is_ok(),
    })
}

fn expected_report() -> Value {
    // Keys as the index stores them: sorted by raw bytes.
    let mut keys: Vec<&str> = RECORDS.iter().map(|(k, _)| *k).collect();
    keys.push("mixed");
    keys.sort_unstable();

    let mut items = serde_json::Map::new();
    for (key, company) in RECORDS {
        items.insert((*key).into(), record(company));
    }
    items.insert(
        "mixed".into(),
        serde_json::to_value(mixed()).expect("mixed to json"),
    );

    json!({
        "keys": keys,
        "items": Value::Object(items),
        "num_keys": 6,
        "num_objects": 3,          // Google, Cloudflare, mixed
        "unique_objects": 3,
        "prefix_8_8": ["8.8.4.4", "8.8.8.8"],
        "verified": true,
        "version": 2,
    })
}

/// Compare only the keys present in `expected` — the offsets and CRC in a full
/// report are file-specific and get asserted separately.
fn assert_subset(actual: &Value, expected: &Value, context: &str) {
    for (key, want) in expected.as_object().expect("expected is an object") {
        let got = actual.get(key).unwrap_or_else(|| {
            panic!("{context}: report is missing `{key}`\nfull report: {actual}")
        });
        assert_eq!(got, want, "{context}: `{key}` differs");
    }
}

// ── 1. Rust writes → Python reads ─────────────────────────────────────────────

#[test]
fn rust_writes_python_reads() {
    let Some(py) = Python::find_or_skip("rust_writes_python_reads") else {
        return;
    };
    let dir = TempDir::new().unwrap();
    let db = dir.path().join("rust.fsdb");

    write_canonical(&db);

    let report: Value =
        serde_json::from_str(&py.run(&["read", db.to_str().unwrap()])).expect("python report");

    assert_subset(&report, &expected_report(), "python reading a Rust file");
}

// ── 2. Python writes → Rust reads ─────────────────────────────────────────────

#[test]
fn python_writes_rust_reads() {
    let Some(py) = Python::find_or_skip("python_writes_rust_reads") else {
        return;
    };
    let dir = TempDir::new().unwrap();
    let db = dir.path().join("python.fsdb");

    py.run(&["write", db.to_str().unwrap()]);

    assert_subset(
        &rust_read_report(&db),
        &expected_report(),
        "Rust reading a Python file",
    );

    // Typed decode of a Python-written value into a concrete Rust struct.
    let reader = FuseReader::open(&db).unwrap();
    assert_eq!(
        reader.get::<Mixed>("mixed").unwrap().unwrap(),
        mixed(),
        "Python-encoded msgpack must decode into the Rust struct"
    );
}

// ── 3. Both implementations agree on the exact bytes ──────────────────────────

#[test]
fn both_report_identical_metadata() {
    let Some(py) = Python::find_or_skip("both_report_identical_metadata") else {
        return;
    };
    let dir = TempDir::new().unwrap();

    for (label, path) in [
        ("rust-written", dir.path().join("r.fsdb")),
        ("python-written", dir.path().join("p.fsdb")),
    ] {
        if label == "rust-written" {
            write_canonical(&path);
        } else {
            py.run(&["write", path.to_str().unwrap()]);
        }

        let from_python: Value =
            serde_json::from_str(&py.run(&["read", path.to_str().unwrap()])).unwrap();
        let from_rust = rust_read_report(&path);

        for field in [
            "num_keys",
            "num_objects",
            "unique_objects",
            "index_offset",
            "data_offset",
            "file_crc32",
            "version",
            "keys",
            "items",
            "prefix_8_8",
        ] {
            assert_eq!(
                from_rust[field], from_python[field],
                "{label}: `{field}` differs between implementations",
            );
        }
    }
}

// ── 4. CRC validation is the same check on both sides ─────────────────────────

#[test]
fn crc_validation_matches() {
    let Some(py) = Python::find_or_skip("crc_validation_matches") else {
        return;
    };
    let dir = TempDir::new().unwrap();
    let db = dir.path().join("crc.fsdb");

    write_canonical(&db);
    assert_eq!(py.run(&["verify", db.to_str().unwrap()]), "ok");
    assert!(FuseReader::open(&db).unwrap().verify().is_ok());

    // Flip one payload byte. Both implementations must reject the file, and
    // both must reject it at open() — the whole-file CRC covers everything
    // after the 40-byte header.
    let mut bytes = std::fs::read(&db).unwrap();
    let victim = bytes.len() - 1;
    bytes[victim] ^= 0xff;
    let corrupt = dir.path().join("corrupt.fsdb");
    std::fs::write(&corrupt, &bytes).unwrap();

    let py_result = py.run(&["verify", corrupt.to_str().unwrap()]);
    assert!(
        py_result.starts_with("fail:"),
        "python accepted a corrupted file: {py_result}"
    );
    assert!(
        FuseReader::open(&corrupt).is_err(),
        "Rust accepted a corrupted file that Python rejected"
    );
}

// ── 5. Deduplication survives the round-trip ──────────────────────────────────

#[test]
fn deduplication_matches() {
    let Some(py) = Python::find_or_skip("deduplication_matches") else {
        return;
    };
    let dir = TempDir::new().unwrap();
    let rust_db = dir.path().join("r.fsdb");
    let py_db = dir.path().join("p.fsdb");

    write_canonical(&rust_db);
    py.run(&["write", py_db.to_str().unwrap()]);

    for path in [&rust_db, &py_db] {
        let db = FuseReader::open(path).unwrap();
        assert_eq!(db.len(), 6, "{path:?}: key count");
        assert_eq!(db.stats().num_objects, 3, "{path:?}: stored object count");
        assert_eq!(
            db.objects_raw().unwrap().len(),
            3,
            "{path:?}: four aliased keys must resolve to two shared objects",
        );
        // The aliases really are the same object, not two equal copies.
        assert_eq!(
            db.get_raw("8.8.8.8").unwrap(),
            db.get_raw("google.com").unwrap()
        );
    }
}

// ── 6. merge() produces the same file in both languages ───────────────────────

#[test]
fn merge_is_byte_identical() {
    let Some(py) = Python::find_or_skip("merge_is_byte_identical") else {
        return;
    };
    let dir = TempDir::new().unwrap();

    // Two overlapping sources: `b` re-states one of `a`'s records and adds one.
    let a = dir.path().join("a.fsdb");
    let b = dir.path().join("b.fsdb");
    write_canonical(&a);

    let mut w = FuseWriter::new();
    let oid = w.add_object(&record("Cloudflare")).unwrap();
    w.add_key("1.0.0.1", oid).unwrap();
    w.add_key("1.1.1.1", oid).unwrap();
    w.add("fastly.com", &json!({"company": "Fastly"})).unwrap();
    w.build(&b).unwrap();

    let rust_out = dir.path().join("merged_rust.fsdb");
    let py_out = dir.path().join("merged_py.fsdb");

    merge(&[&a, &b], &rust_out).unwrap();
    py.run(&[
        "merge",
        py_out.to_str().unwrap(),
        a.to_str().unwrap(),
        b.to_str().unwrap(),
    ]);

    assert_eq!(
        std::fs::read(&rust_out).unwrap(),
        std::fs::read(&py_out).unwrap(),
        "merge() must be byte-identical across implementations",
    );

    let merged = FuseReader::open(&rust_out).unwrap();
    assert!(merged.exists("fastly.com") && merged.exists("google.com"));
    assert_eq!(
        merged.stats().num_objects,
        4,
        "Cloudflare appears in both sources and must be stored once",
    );
}

// ── 7. The value encoders agree byte for byte ─────────────────────────────────

#[test]
fn msgpack_encoding_is_identical() {
    let Some(py) = Python::find_or_skip("msgpack_encoding_is_identical") else {
        return;
    };

    let from_python = py.run(&["pack-mixed"]);
    let from_rust = fusedb::encode(&mixed())
        .unwrap()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect::<String>();

    assert_eq!(
        from_rust, from_python,
        "rmp_serde::to_vec_named must match msgpack.packb(use_bin_type=True)",
    );
}

// ── 8. Versions stay in lockstep ──────────────────────────────────────────────

#[test]
fn crate_and_package_versions_match() {
    let Some(py) = Python::find_or_skip("crate_and_package_versions_match") else {
        return;
    };
    assert_eq!(
        py.run(&["version"]),
        fusedb::CRATE_VERSION,
        "python __version__ and Cargo package version have drifted",
    );
}
