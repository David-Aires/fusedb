# Changelog

All notable changes are documented here.
Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
Versioning follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [Unreleased]

## [0.2.0] — 2026-03-12

### Added
- Full Rust core rewrite via PyO3 — same file format as Python v2
- `FuseWriter` — builds `.bhdb` files atomically (tmp → fsync → rename)
- `FuseReader` — mmap-based reader with O(1) hash index + sorted prefix index
- `ReloadableFuseReader` — thread-safe hot-swap reload with mtime check
- `FuseWatcher` — background daemon thread, auto-reloads on file change
- `FusePool` — round-robin reader pool with atomic `swap()`
- `merge()` — content-addressed merge of multiple `.bhdb` files
- `FuseReader.objects()` — iterate unique objects only (deduplicated by offset)
- `FuseReader.items()` — sorted `(key, object)` pairs
- `FuseReader.verify()` — deep CRC32 integrity check
- GitHub Actions: CI (lint + test matrix), Release (manylinux + musl + macOS + Windows), Audit
- Full pytest suite with coverage
- `uv` as package manager

### Changed
- Package renamed from `bhdb` → `fusedb`

## [0.1.0] — 2026-03-10

### Added
- Initial Python implementation (`bhdb.py`)
- BHDBWriter, BHDBReader, ReloadableBHDBReader, BHDBWatcher, BHDBPool, merge()
- Binary format v2: big-endian, msgpack serialisation, CRC32 per object + whole-file
- mmap reads, hash index + sorted list for prefix scan
- LRU cache, multi-thread / multi-process safety