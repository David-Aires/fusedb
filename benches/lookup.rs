// benches/lookup.rs — criterion benchmarks for FuseDB
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use fusedb::{FuseReader, FuseWriter};
use std::collections::HashMap;

fn make_db(n_objects: usize) -> tempfile::NamedTempFile {
    let f  = tempfile::NamedTempFile::new().unwrap();
    let mut w = FuseWriter::new();
    for i in 0..n_objects {
        let mut obj = HashMap::new();
        obj.insert("id".to_string(),      i.to_string());
        obj.insert("country".to_string(), "US".to_string());
        obj.insert("asn".to_string(),     (i % 65535).to_string());
        let oid = w.add_object_map(obj).unwrap();
        w.add_key(format!("10.{}.{}.1", i / 256, i % 256), oid).unwrap();
        w.add_key(format!("user{}@d.com", i), oid).unwrap();
    }
    w.build(f.path().to_str().unwrap()).unwrap();
    f
}

fn bench_lookup(c: &mut Criterion) {
    let tmp = make_db(10_000);
    let db  = FuseReader::open(tmp.path().to_str().unwrap(), false).unwrap();

    let mut g = c.benchmark_group("lookup");
    g.throughput(Throughput::Elements(1));

    g.bench_function("get_cold", |b| {
        b.iter(|| black_box(db.get_raw(b"10.1.1.1")))
    });

    g.bench_function("exists", |b| {
        b.iter(|| black_box(db.exists_raw(b"10.1.1.1")))
    });

    g.bench_function("prefix_scan", |b| {
        b.iter(|| black_box(db.prefix_raw(b"10.10.")))
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

criterion_group!(benches, bench_lookup, bench_build);
criterion_main!(benches);