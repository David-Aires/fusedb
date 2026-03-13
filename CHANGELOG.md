# Changelog

All notable changes are documented here.
Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
Versioning follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [Unreleased]

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