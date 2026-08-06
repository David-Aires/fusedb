# Changelog

All notable changes are documented here.
Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
Versioning follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [Released]

## [0.3.0] — 2026-08-06

FuseDB is now a first-class Rust library as well as a Python one. Both read and
write the same `.fsdb` files, with no conversion step in either direction.

### Added
- **Native Rust crate**, published on crates.io — `cargo add fusedb`
  - `FuseWriter` — `add`, `add_object`, `add_object_deduped`, `add_key`, `add_raw`,
    `build` (returns a `BuildReport`)
  - `FuseReader` — `open` / `open_unverified`, typed `get::<T>` and raw `get_raw`,
    `exists`, `keys`, `items`, `objects`, `stats`, `verify`
  - `prefix_iter()` — lazy sorted prefix iteration, reading objects only as
    the iterator reaches them
  - `merge()` and `merge_into()` — content-addressed merging in Rust
  - `Stats` — the metadata Python's `FuseReader.stats()` returns
  - `encode()` / `decode()` — the MessagePack codec shared with the Python package
- **Hot-swap reloading in Rust**, the counterpart to Python's
  `ReloadableFuseReader` and `FuseWatcher`:
  - `ReloadableReader` — swaps in a rebuilt file on demand. The replacement is
    built outside the lock, and a failed reload leaves the previous file serving.
  - Both types carry the full `FuseReader` read surface — `get`, `get_raw`,
    `exists`, `prefix`, `keys`, `items`, `objects`, `len`, `stats`, `verify` —
    and every call resolves the current file, so reads follow a swap with no
    bookkeeping and a replaced database can never be read by accident.
  - `load()` returns an `Arc<FuseReader>` snapshot for batching a run of reads or
    pinning a stable view across a multi-step read. Snapshots stay valid across
    swaps, so a request in flight is never disturbed.
  - `FuseWatcher` — polls on a background thread and hot-swaps. Configured
    through a builder (`interval`, `verify`, `on_reload`, `on_error`); stopped
    and joined on `Drop`, so the thread cannot outlive its handle. Shutdown is
    signalled through a condvar, so `stop()` never waits out the poll interval.
  - Change detection compares modification time *and* length, catching rebuilds
    that land inside one filesystem timestamp tick. A file that briefly vanishes
    during the writer's rename is treated as unchanged, not as an error.
  - Neither type writes to stdout — failures reach the caller through `on_error`.
- **Cargo features.** `msgpack` (default) enables the typed serde API; `python`
  gates the PyO3 bindings and is enabled only by maturin.
- **Cross-language test suite** (`tests/interop.rs`, 8 tests) covering both read
  directions, CRC validation, metadata, deduplication, prefix scans, byte-identical
  `merge()` output, and byte-identical msgpack encoding. Runs on Linux, macOS, and
  Windows in CI.
- **Rust API test suite** (`tests/rust_api.rs`, 18 tests), **hot-swap suite**
  (`tests/watch.rs`, 25 tests), and runnable examples (`examples/quickstart.rs`,
  `examples/watch.rs`, `examples/interop.rs`).
- **Selective release pipeline.** `release.yml` now takes `publish_python` and
  `publish_rust` inputs, so PyPI and crates.io can be released independently or
  together. Tag pushes publish both. `dry_run` verifies everything without
  publishing.
- CI now builds the crate with and without default features and fails if PyO3
  reaches the default dependency graph.

### Changed
- `pyo3` is now an **optional** dependency behind the `python` feature. Rust
  consumers of the crate no longer compile PyO3.
- `WriterCore::build()` returns a `BuildReport` instead of `()` and no longer
  prints to stdout — the progress line moved to the Python shim, where it was
  always a Python-package behaviour rather than a library one.
- `WriterCore::build()` and `ReaderCore::open()` accept any `AsRef<Path>`.
- Python's `merge()` now round-trips keys as bytes, so non-UTF-8 keys survive a
  merge unchanged and the output matches Rust's `merge()` byte for byte.

### Fixed
- Keys longer than 65,535 bytes were silently truncated by an `as u16` cast,
  producing an index entry whose length prefix disagreed with its bytes — an
  unreadable file. Oversized keys are now rejected with `FuseError::InvalidArg`.
  Object and key counts exceeding their `u32` header fields are rejected too.

## [0.2.0] — 2026-03-12

### Added
- Rust core via PyO3
- `FuseWriter` — builds `.fsdb` files atomically (tmp → fsync → rename)
- `FuseReader` — mmap-based reader with O(1) hash index + sorted prefix index
- `ReloadableFuseReader` — thread-safe hot-swap reload with mtime check
- `FuseWatcher` — background daemon thread, auto-reloads on file change
- `FusePool` — round-robin reader pool with atomic `swap()`
- `merge()` — content-addressed merge of multiple `.fsdb` files
- `FuseReader.objects()` — iterate unique objects only (deduplicated by offset)
- `FuseReader.items()` — sorted `(key, object)` pairs
- `FuseReader.verify()` — deep CRC32 integrity check
- GitHub Actions: CI (lint + test matrix), Release (manylinux + musl + macOS + Windows), Audit
- Full pytest suite with coverage
- `uv` as package manager