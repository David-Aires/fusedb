// benches/lookup.rs — Criterion benchmarks for FuseDB
//
// Imports from `fusedb::core` — the pure-Rust layer with zero PyO3 involvement.
// This is the correct way to benchmark a PyO3 library: measure the core logic,
// not the Python boundary overhead.

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use fusedb::core::{ReaderCore, WriterCore};
use tempfile::NamedTempFile;

// ─── helpers ─────────────────────────────────────────────────────────────────

/// Minimal hand-encoded msgpack payload — no extra dependency needed.
/// Produces: fixmap(3) { "id": u32, "asn": u16, "cc": "US" }
fn make_payload(id: u32, asn: u16) -> Vec<u8> {
    let mut buf = Vec::with_capacity(32);
    buf.push(0x83); // fixmap, 3 entries
    buf.push(0xa2);
    buf.extend_from_slice(b"id");
    buf.push(0xce);
    buf.extend_from_slice(&id.to_be_bytes());
    buf.push(0xa3);
    buf.extend_from_slice(b"asn");
    buf.push(0xcd);
    buf.extend_from_slice(&asn.to_be_bytes());
    buf.push(0xa2);
    buf.extend_from_slice(b"cc");
    buf.push(0xa2);
    buf.extend_from_slice(b"US");
    buf
}

/// Build a `.fsdb` file with `n_objects` unique objects, each with two keys
/// (one IP-style, one email-style) to simulate realistic enrichment data.
fn make_db(n_objects: usize) -> NamedTempFile {
    let tmp = NamedTempFile::new().expect("tempfile");
    let mut w = WriterCore::new();

    for i in 0..n_objects {
        let payload = make_payload(i as u32, (i % 65535) as u16);
        let oid = w.add_object(&payload);
        let ip_key = format!("10.{}.{}.1", i / 256, i % 256);
        let email_key = format!("user{i}@domain.com");
        w.add_key(ip_key.as_bytes(), oid).expect("add_key ip");
        w.add_key(email_key.as_bytes(), oid).expect("add_key email");
    }

    w.build(tmp.path().to_str().expect("utf-8 path"))
        .expect("build");
    tmp
}

// ─── benchmarks ──────────────────────────────────────────────────────────────

fn bench_lookup(c: &mut Criterion) {
    let tmp = make_db(10_000);
    // Open without CRC verification — we're benchmarking the lookup, not verify()
    let db = ReaderCore::open(tmp.path().to_str().expect("utf-8 path"), false).expect("open");

    let mut g = c.benchmark_group("lookup");
    g.throughput(Throughput::Elements(1));

    g.bench_function("get/hit", |b| b.iter(|| black_box(db.get(b"10.1.1.1"))));
    g.bench_function("get/miss", |b| b.iter(|| black_box(db.get(b"192.168.1.1"))));
    g.bench_function("exists/hit", |b| {
        b.iter(|| black_box(db.exists(b"10.1.1.1")))
    });
    g.bench_function("exists/miss", |b| {
        b.iter(|| black_box(db.exists(b"192.168.1.1")))
    });
    g.bench_function("prefix/scan", |b| {
        b.iter(|| black_box(db.prefix(b"10.10.")))
    });

    g.finish();
}

fn bench_build(c: &mut Criterion) {
    let mut g = c.benchmark_group("build");

    for n in [1_000usize, 10_000, 50_000] {
        g.throughput(Throughput::Elements(n as u64));
        g.bench_with_input(BenchmarkId::new("objects", n), &n, |b, &n| {
            b.iter(|| black_box(make_db(n)))
        });
    }

    g.finish();
}

fn bench_verify(c: &mut Criterion) {
    let tmp = make_db(10_000);
    let db = ReaderCore::open(tmp.path().to_str().expect("utf-8 path"), false).expect("open");

    c.bench_function("verify/10k", |b| b.iter(|| black_box(db.verify())));
}

criterion_group!(benches, bench_lookup, bench_build, bench_verify);
criterion_main!(benches);
