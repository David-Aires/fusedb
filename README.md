<p align="center">
  <img src="assets/logo.svg" alt="FuseDB" width="700"/>
</p>

<p align="center">
  <a href="https://github.com/David-Aires/fusedb/actions/workflows/ci.yml">
    <img src="https://github.com/David-Aires/fusedb/actions/workflows/ci.yml/badge.svg" alt="CI"/>
  </a>
  <a href="https://pypi.org/project/fusedb/">
    <img src="https://img.shields.io/pypi/v/fusedb?color=1de9c4&labelColor=0d1420" alt="PyPI"/>
  </a>
  <a href="https://pypi.org/project/fusedb/">
    <img src="https://img.shields.io/pypi/pyversions/fusedb?labelColor=0d1420" alt="Python"/>
  </a>
  <a href="LICENSE">
    <img src="https://img.shields.io/badge/license-MIT-a78bfa?labelColor=0d1420" alt="MIT License"/>
  </a>
  <a href="https://github.com/David-Aires/fusedb">
    <img src="https://img.shields.io/badge/built%20with-Rust-orange?labelColor=0d1420" alt="Rust"/>
  </a>
</p>

<p align="center">
  <strong>A read-optimised binary database where many keys share one object — with zero duplication on disk.</strong><br/>
  Inspired by the MMDB format. Built in Rust. Exposed as a native Python library.
</p>

---

## What is FuseDB?

FuseDB is a file-based key-value store purpose-built for **enrichment lookups**: scenarios where many different identifiers (IP addresses, domain names, email addresses, CIDRs, user IDs…) all resolve to the same piece of structured data.

The fundamental insight is simple. In a traditional database, if 500 IP addresses belong to the same network, you store the same organisation record 500 times. FuseDB stores it **once** and points every key at that single byte offset in the file.

```
8.8.8.8       ──┐
8.8.4.4       ──┤
8.8.0.0/16    ──┼──►  { "org": "Google LLC", "asn": 15169, "cc": "US" }  (stored ONCE)
gmail.com     ──┤
googlemail.com──┘
```

The result: files that are dramatically smaller, lookups that are dramatically faster, and a design that stays read-only — making it safe to share across threads and processes without any locking.

---

## Key features

- **Native deduplication.** Objects are stored exactly once. Keys are pointers — not copies. A million aliases for the same record cost only index space.
- **Sub-microsecond lookups.** The entire index fits in memory as a hash map. A `get()` is a hash lookup followed by a single mmap read. No query planner, no transaction log, no overhead.
- **Prefix scan.** The sorted key index supports efficient prefix queries over arbitrary string keys. Enumerate every IP in a subnet, every user in a domain, every path under a prefix — in a single call.
- **Memory-mapped reads.** The file is never loaded into a buffer. The OS page cache handles eviction. Cold lookups fault in one page; warm lookups hit L2/L3 cache.
- **Atomic writes.** `build()` writes to a `.fsdb.tmp` file, fsyncs, then renames. The on-disk file is always a complete and consistent snapshot.
- **CRC32 integrity.** Every object has an individual CRC32. The whole file has a header CRC32. `verify()` checks both in one pass.
- **Zero runtime dependencies.** The Python package ships pre-built wheels. End users need nothing but `pip install fusedb` and `msgpack`.
- **Thread-safe readers.** `FuseReader` is fully lock-free for reads. Share one instance across 100 threads.
- **Hot-swap reloading.** `ReloadableFuseReader` swaps to a new file atomically without dropping a single request. Background `FuseWatcher` polls for changes automatically.
- **Reader pool.** `FusePool` round-robins across N readers for high-concurrency workloads. `swap()` replaces all readers atomically.
- **Merge.** Content-addressed `merge()` combines multiple `.fsdb` files, deduplicating objects that appear in more than one source.
- **Python 3.10 – 3.13.** Pre-built wheels for Linux (x86_64, aarch64, musl), macOS (Intel + Apple Silicon), Windows (x64, x86).

---

## Installation

```bash
pip install fusedb
# or with uv
uv add fusedb
```

No Rust required at runtime. Pre-built wheels are available for all major platforms.

To build from source (requires Rust ≥ 1.83):

```bash
git clone https://github.com/David-Aires/fusedb
cd fusedb
uv sync
uv run maturin develop --release
```

---

## Quick start

### Build a database

```python
from fusedb import FuseWriter

w = FuseWriter()

# Add an object — returns an integer ID
google = w.add_object({
    "org":     "Google LLC",
    "asn":     15169,
    "cc":      "US",
    "abuse":   "network-abuse@google.com",
})

# Map as many keys as you like to that one object
w.add_key("8.8.8.8",         google)
w.add_key("8.8.4.4",         google)   # same object on disk, different key
w.add_key("8.8.0.0/16",      google)
w.add_key("gmail.com",       google)
w.add_key("googlemail.com",  google)

cloudflare = w.add_object({"org": "Cloudflare Inc.", "asn": 13335, "cc": "US"})
w.add_key("1.1.1.1", cloudflare)
w.add_key("1.0.0.1", cloudflare)

# Atomic write: tmp file → fsync → rename
w.build("geo.fsdb")
```

Or use the shorthand `add()` when each key has its own object:

```python
w = FuseWriter()
w.add("8.8.8.8",   {"org": "Google LLC",      "asn": 15169})
w.add("1.1.1.1",   {"org": "Cloudflare Inc.", "asn": 13335})
w.build("simple.fsdb")
```

### Read a database

```python
from fusedb import FuseReader

with FuseReader("geo.fsdb") as db:
    # Exact lookup — O(1)
    print(db.get("8.8.8.8"))
    # → {'org': 'Google LLC', 'asn': 15169, 'cc': 'US', ...}

    # Aliases resolve to the same object
    print(db.get("gmail.com"))
    # → {'org': 'Google LLC', 'asn': 15169, 'cc': 'US', ...}

    # Presence check — no deserialisation
    print(db.exists("1.1.1.1"))
    # → True

    # Prefix scan — sorted results
    for key, obj in db.prefix("8.8."):
        print(f"  {key:20s}  →  {obj['org']}")

    # Inspect the file
    print(db.stats())
    # → {'num_keys': 7, 'num_objects': 2, 'file_size_kb': 1.4, ...}

    # Deep integrity check
    assert db.verify()
```

---

## API reference

### `FuseWriter`

| Method | Description |
|---|---|
| `add_object(data) → int` | Serialise any Python object as msgpack. Returns its integer ID. |
| `add_key(key, obj_id)` | Map a key (str or bytes) to an object ID. Many keys can share one ID. |
| `add(key, data) → int` | Convenience — `add_object` + `add_key` in one call. |
| `build(path)` | Write the file atomically. Safe to call while readers are open. |

### `FuseReader`

| Method | Description |
|---|---|
| `get(key) → Any \| None` | O(1) exact-match lookup. Returns the deserialised object or `None`. |
| `exists(key) → bool` | Presence check without deserialisation. |
| `prefix(prefix) → list[tuple[str, Any]]` | Sorted prefix scan. Returns all `(key, object)` pairs whose key starts with `prefix`. |
| `keys() → list[str]` | All keys in sorted order. |
| `items() → list[tuple[str, Any]]` | All `(key, object)` pairs in sorted key order. |
| `objects() → list[Any]` | Unique objects only — deduplicated by file offset. |
| `stats() → dict` | File metadata: key count, object count, file size, CRC32, offsets. |
| `verify() → bool` | Deep CRC32 integrity check (whole-file + per-object). Raises on failure. |
| `close()` | Release the memory map. Called automatically by the context manager. |

All methods accept `str` or `bytes` as keys. `FuseReader` is fully thread-safe for reads — share one instance freely.

### `ReloadableFuseReader`

A drop-in replacement for `FuseReader` that supports hot-swapping the underlying file. Uses a `threading.RLock` internally; reads and reloads never block each other for more than a single pointer swap.

```python
db = ReloadableFuseReader("live.fsdb")

# Later, after the file has been rebuilt on disk:
changed = db.reload()   # checks mtime; swaps atomically if changed
                        # returns True if a reload occurred
```

### `FuseWatcher`

Wraps `ReloadableFuseReader` with a background daemon thread that polls the file every `interval` seconds.

```python
watcher = FuseWatcher(
    "live.fsdb",
    interval  = 30.0,
    on_reload = lambda db: print(f"Reloaded: {db.stats()['num_keys']} keys"),
)
watcher.start()

# Use exactly like FuseReader:
result = watcher.get("8.8.8.8")

watcher.stop()
```

### `FusePool`

Round-robin reader pool for maximising throughput under heavy concurrency. `swap()` atomically replaces all readers without dropping any in-flight calls.

```python
pool = FusePool("live.fsdb", size=8)

pool.get("8.8.8.8")            # dispatched to one of 8 readers

pool.swap("live_v2.fsdb")      # zero-downtime upgrade — all 8 readers replaced atomically

pool.close()
```

### `merge()`

Content-addressed merge across files. Objects with identical msgpack bytes are stored only once in the output, regardless of which source file they came from.

```python
from fusedb import merge

merge("geo_us.fsdb", "geo_eu.fsdb", output="geo_global.fsdb")
```

### Exceptions

| Exception | When raised |
|---|---|
| `FuseError` | Base class for all FuseDB errors. |
| `FuseCorruptError` | CRC32 mismatch, truncated file, or bad magic bytes. |
| `FuseVersionError` | File was written with an unsupported format version. |

---

## File format

The `.fsdb` format is a compact, append-once binary file. All integers are big-endian.

```
HEADER  (40 bytes)
  magic[4]          — b"FSDB"
  version[1]        — currently 2
  flags[1]          — reserved
  pad[2]            — reserved
  num_keys[4]       — total number of index entries
  num_objects[4]    — number of unique objects
  index_offset[8]   — byte offset of the index section
  data_offset[8]    — byte offset of the data section (always 40)
  file_crc32[4]     — CRC32 of everything after the header
  reserved[4]

DATA SECTION
  For each unique object:
  [obj_len(4)][obj_crc32(4)][msgpack_bytes]

INDEX SECTION  (sorted lexicographically by key bytes)
  For each key:
  [key_len(2)][key_bytes][data_offset(8)]
```

The index section is sorted, enabling O(log n) prefix scans via binary search. Multiple entries can point to the same `data_offset` — that is the deduplication mechanism.

---

## Design decisions

**Why a file format rather than a server?**
FuseDB is designed for enrichment at read time — decorating events with contextual data as they flow through a pipeline. A file loaded into memory has zero network latency and zero serialisation overhead. It can be deployed alongside every process that needs it without infrastructure.

**Why Rust?**
The hot path (hash lookup + mmap read) needs to be as close to the metal as possible. PyO3 lets us expose a clean Python API while the core runs at native speed. The extension module compiles down to a single `.so`/`.pyd` file with no transitive native dependencies.

**Why msgpack?**
msgpack is the most compact general-purpose binary serialisation format for Python objects. It handles dicts, lists, strings, ints, floats, booleans, and `None` with smaller wire size than JSON and no schema requirement like protobuf. The Python `msgpack` library is mature and fast.

**Why is the format inspired by MMDB?**
MaxMind's MMDB format pioneered the idea of a read-only binary file where many IP ranges point to shared data records. FuseDB extends that concept to arbitrary key types (any string, any bytes) and arbitrary Python objects, while using a simpler flat-file layout that is easier to inspect and implement.

**Why is there no update operation?**
FuseDB files are immutable once built. Updates are handled by rebuilding the file and hot-swapping with `ReloadableFuseReader` or `FusePool.swap()`. This keeps the read path completely lock-free and makes the format trivially safe for multi-process use.

---

## Use cases

FuseDB is well suited for any pipeline that needs fast, read-heavy enrichment lookups:

- **IP enrichment** — map IP addresses or CIDR ranges to ASN, organisation, country, or abuse contact
- **Domain classification** — map domains to categories, reputation scores, or registrar data
- **Email routing** — map email addresses or domains to provider metadata or spam scores
- **User enrichment** — map user IDs to profile data, tier, or feature flags
- **Threat intelligence** — distribute indicator-of-compromise datasets as a single portable file
- **Geolocation** — embed city/region/country data in a deployable artefact that requires no database server

---

## Performance

FuseDB is designed around the reality that enrichment lookups happen millions of times per second in high-throughput pipelines. The architecture reflects that:

- The index is a hash map in memory — lookups are O(1) and cache-friendly
- Objects are read directly from an mmap — no copy, no deserialisation until you call `get()`
- `exists()` never touches the data section — it's a pure hash probe
- `prefix()` uses a sorted array and `partition_point` — O(log n) entry point, O(k) scan
- The file is a single contiguous allocation — the OS prefetches pages naturally under sequential access patterns

---

## Contributing

Contributions of all kinds are welcome: bug fixes, new features, documentation improvements, benchmark results, or just opening a discussion.

### Setting up the development environment

```bash
# 1. Fork and clone
git clone https://github.com/YOUR_USERNAME/fusedb
cd fusedb

# 2. Install Rust (https://rustup.rs) — requires 1.83+
rustup update stable

# 3. Install uv (https://docs.astral.sh/uv)
curl -LsSf https://astral.sh/uv/install.sh | sh

# 4. Create the virtual environment and install all dev dependencies
uv sync

# 5. Build the Rust extension in development mode
uv run maturin develop

# 6. Verify everything works
uv run pytest
```

### Development workflow

```bash
# Format and lint Rust
cargo fmt
cargo clippy --all-targets -- -D warnings

# Format and lint Python
uv run ruff check python/ tests/
uv run ruff format python/ tests/

# Type-check Python
uv run mypy python/fusedb/

# Run the full test suite
uv run pytest

# Run tests with coverage
uv run pytest --cov=fusedb --cov-report=html

# Run benchmarks
cargo bench
```

### Project structure

```
fusedb/
├── src/
│   └── lib.rs              ← Rust core: binary format, mmap, CRC32, index, PyO3 bindings
├── python/
│   └── fusedb/
│       ├── __init__.py     ← Python layer: FuseWriter, FuseReader, Watcher, Pool, merge()
│       └── py.typed        ← PEP 561 marker
├── tests/
│   └── test_fusedb.py      ← Full pytest suite (writer, reader, types, integrity,
│                               deduplication, concurrency, merge, watcher, pool)
├── benches/
│   └── lookup.rs           ← Criterion benchmarks
├── .github/
│   └── workflows/
│       ├── ci.yml          ← PR checks: fmt + clippy + pytest matrix (3.10–3.13 × 3 OS)
│       ├── release.yml     ← Wheel builder + PyPI publish on git tag
│       └── audit.yml       ← Weekly dependency security audit
├── Cargo.toml
├── pyproject.toml
└── rust-toolchain.toml
```

### Submitting a pull request

1. **Open an issue first** for non-trivial changes. It avoids wasted effort if the direction isn't a fit.
2. **Branch from `main`**: `git checkout -b feat/my-feature`
3. **Write tests** for any new behaviour. The CI enforces coverage.
4. **Run the full suite locally** before pushing: `uv run pytest && cargo clippy && cargo fmt --check`
5. **Keep commits focused.** One logical change per commit with a clear message.
6. **Update `CHANGELOG.md`** under `[Unreleased]`.
7. Open the PR and fill in the template. The CI will run automatically.

### Code style

- **Rust**: `rustfmt` defaults. Clippy warnings are treated as errors in CI. Prefer explicit error messages in `PyErr` constructors — users see these.
- **Python**: `ruff` with the project config. Docstrings on all public classes and methods. Type annotations on all public functions.
- **Tests**: one `class Test*` per feature area. Test names describe the expected behaviour, not the implementation. No mocks for the Rust layer — use real files in `tempfile.mkdtemp()`.

---

## Reporting a bug

Please open an issue at [github.com/David-Aires/fusedb/issues](https://github.com/David-Aires/fusedb/issues) and include:

- **FuseDB version**: `python -c "import fusedb; print(fusedb.__version__)"`
- **Python version**: `python --version`
- **Operating system and architecture**
- **Minimal reproduction**: the smallest code that triggers the bug
- **Expected behaviour** vs **actual behaviour**
- **Full traceback** if applicable

For security vulnerabilities, please do **not** open a public issue. Email `you@example.com` directly with the details.

---

## Requesting a feature

Open a [GitHub Discussion](https://github.com/David-Aires/fusedb/discussions) rather than an issue for feature requests. Describe:

- **The problem you're trying to solve** — not the specific solution
- **How you currently work around it**
- **What a good API for this would look like**

Feature requests that come with a prototype or a clear design rationale are much more likely to be implemented quickly.

---

## Releasing (maintainers)

1. Update `version` in both `Cargo.toml` and `pyproject.toml` to the new version.
2. Add a release entry to `CHANGELOG.md` under the new version number.
3. Commit: `git commit -m "chore: release v0.3.0"`
4. Tag and push: `git tag v0.3.0 && git push origin main v0.3.0`
5. The `release.yml` workflow builds wheels for all platforms and publishes to PyPI automatically via OIDC trusted publishing.

PyPI trusted publishing is configured at:
`https://pypi.org/manage/project/fusedb/settings/publishing/`

---

## Changelog

See [CHANGELOG.md](CHANGELOG.md) for the full history of changes.

---

## License

FuseDB is released under the [MIT License](LICENSE).

---

<p align="center">
  Built with <a href="https://pyo3.rs">PyO3</a> · Packaged with <a href="https://github.com/PyO3/maturin">maturin</a> · Managed with <a href="https://docs.astral.sh/uv">uv</a>
</p>