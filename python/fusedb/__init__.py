"""
fusedb — fast file-based key-value store
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Multiple keys → one shared object, zero duplication on disk.
Rust core (I/O, CRC32, mmap, index) + Python layer (msgpack, live-reload, pool).

Quick start
-----------
    from fusedb import FuseWriter, FuseReader

    w = FuseWriter()
    g = w.add_object({"org": "Google", "asn": 15169})
    w.add_key("8.8.8.8",  g)
    w.add_key("gmail.com", g)   # alias → same bytes on disk
    w.build("geo.fsdb")

    with FuseReader("geo.fsdb") as db:
        print(db.get("8.8.8.8"))
        for k, v in db.prefix("8.8."):
            print(k, v)
"""

from __future__ import annotations

import os
import threading
from pathlib import Path
from typing import Any, Callable

import msgpack

from fusedb._fusedb import _FuseReader as _RustReader
from fusedb._fusedb import _FuseWriter as _RustWriter

__version__: str = "0.2.0"
__all__ = [
    "FuseWriter",
    "FuseReader",
    "ReloadableFuseReader",
    "FuseWatcher",
    "FusePool",
    "merge",
    "FuseError",
    "FuseCorruptError",
    "FuseVersionError",
]

# ── helpers ───────────────────────────────────────────────────────────────────

def _pack(obj: Any) -> bytes:
    return msgpack.packb(obj, use_bin_type=True)

def _unpack(raw: bytes) -> Any:
    return msgpack.unpackb(raw, raw=False)


# ── Exceptions ────────────────────────────────────────────────────────────────

class FuseError(Exception):
    """Base exception for all FuseDB errors."""

class FuseCorruptError(FuseError):
    """CRC32 mismatch or truncated file."""

class FuseVersionError(FuseError):
    """Unsupported format version."""


# ── FuseWriter ────────────────────────────────────────────────────────────────

class FuseWriter:
    """
    Build a .fsdb file.

    Example
    -------
    w = FuseWriter()
    goog = w.add_object({"org": "Google", "asn": 15169})
    w.add_key("8.8.8.8",  goog)
    w.add_key("8.8.4.4",  goog)  # alias
    w.add_key("gmail.com", goog)
    w.build("geo.fsdb")
    """

    def __init__(self) -> None:
        self._w = _RustWriter()

    def add_object(self, data: Any) -> int:
        """Serialise *data* and store it once. Returns an object ID."""
        return self._w.add_object_raw(_pack(data))

    def add_key(self, key: str | bytes, obj_id: int) -> None:
        """Map *key* to an existing object ID."""
        self._w.add_key(key, obj_id)

    def add(self, key: str | bytes, data: Any) -> int:
        """Convenience: add_object + add_key in one call."""
        oid = self.add_object(data)
        self.add_key(key, oid)
        return oid

    def build(self, path: str | Path) -> None:
        """Write the .fsdb file atomically (tmp → fsync → rename)."""
        self._w.build(str(path))

    def __repr__(self) -> str:
        return repr(self._w).replace("_FuseWriter", "FuseWriter")


# ── FuseReader ────────────────────────────────────────────────────────────────

class FuseReader:
    """
    Read-only, memory-mapped FuseDB reader.

    Thread-safe: share ONE instance across all threads — no locking needed.

    Example
    -------
    with FuseReader("geo.fsdb") as db:
        print(db.get("8.8.8.8"))
        print(db.exists("gmail.com"))
        for k, v in db.prefix("8.8."):
            print(k, v)
    """

    def __init__(self, path: str | Path, verify: bool = True) -> None:
        self._r = _RustReader(str(path), verify=verify)

    # ── lookups ───────────────────────────────────────────────────────────────

    def get(self, key: str | bytes) -> Any | None:
        """O(1) exact-match lookup. Returns the object or None."""
        raw = self._r.get_raw(key)
        return _unpack(raw) if raw is not None else None

    def exists(self, key: str | bytes) -> bool:
        """Key presence check — no deserialisation."""
        return self._r.exists(key)

    def prefix(self, prefix: str | bytes) -> list[tuple[str, Any]]:
        """Return all (key, object) pairs whose key starts with *prefix* (sorted)."""
        return [(k, _unpack(raw)) for k, raw in self._r.prefix_raw(prefix)]

    def keys(self) -> list[str]:
        """All keys in sorted order."""
        return self._r.keys()

    def items(self) -> list[tuple[str, Any]]:
        """All (key, object) pairs in sorted key order."""
        return [(k, _unpack(raw)) for k, raw in self._r.items_raw()]

    def objects(self) -> list[Any]:
        """Unique objects only (deduplicated by file offset)."""
        return [_unpack(raw) for raw in self._r.objects_raw()]

    # ── introspection ─────────────────────────────────────────────────────────

    def stats(self) -> dict:
        return self._r.stats()

    def verify(self) -> bool:
        """Deep CRC32 integrity check (whole-file + per-object)."""
        return self._r.verify()

    def close(self) -> None:
        self._r.close()

    def __enter__(self) -> "FuseReader":
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()

    def __repr__(self) -> str:
        s = self._r.stats()
        return f"FuseReader('{Path(s['path']).name}', keys={s['num_keys']}, objects={s['num_objects']})"


# ── ReloadableFuseReader ──────────────────────────────────────────────────────

class ReloadableFuseReader:
    """
    FuseReader with thread-safe hot-swap reload().

    Builds the new index outside the lock then swaps state atomically —
    concurrent get() calls are never interrupted.

    Example
    -------
    db = ReloadableFuseReader("live.fsdb")
    db.get("8.8.8.8")
    changed = db.reload()       # True if file changed
    """

    def __init__(self, path: str | Path, verify: bool = True) -> None:
        self._path   = Path(path)
        self._lock   = threading.RLock()
        self._verify = verify
        self._mtime: float | None = None
        self._db     = FuseReader(str(self._path), verify=verify)
        self._mtime  = os.path.getmtime(self._path)

    def get(self, key: str | bytes) -> Any | None:
        with self._lock: return self._db.get(key)

    def exists(self, key: str | bytes) -> bool:
        with self._lock: return self._db.exists(key)

    def prefix(self, prefix: str | bytes) -> list[tuple[str, Any]]:
        with self._lock: return self._db.prefix(prefix)

    def keys(self) -> list[str]:
        with self._lock: return self._db.keys()

    def stats(self) -> dict:
        with self._lock: return self._db.stats()

    def verify(self) -> bool:
        with self._lock: return self._db.verify()

    def reload(self) -> bool:
        """Hot-swap to current on-disk file if mtime changed. Returns True if reloaded."""
        try:
            new_mtime = os.path.getmtime(self._path)
        except FileNotFoundError:
            return False
        if self._mtime is not None and new_mtime == self._mtime:
            return False
        new_db = FuseReader(str(self._path), verify=self._verify)
        with self._lock:
            self._db    = new_db
            self._mtime = new_mtime
        s = new_db.stats()
        print(f"  ♻️   Reloaded {self._path.name}  ({s['num_keys']} keys · {s['num_objects']} objects)")
        return True

    def close(self) -> None:
        with self._lock: self._db.close()

    def __enter__(self) -> "ReloadableFuseReader":
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()

    def __repr__(self) -> str:
        s = self._db.stats()
        return f"ReloadableFuseReader('{self._path.name}', keys={s['num_keys']})"


# ── FuseWatcher ───────────────────────────────────────────────────────────────

class FuseWatcher:
    """
    Auto-reload a FuseDB file when its mtime changes.

    Polls every *interval* seconds in a background daemon thread.

    Example
    -------
    watcher = FuseWatcher("live.fsdb", interval=30.0,
                          on_reload=lambda db: print("reloaded"))
    watcher.start()
    watcher.get("8.8.8.8")
    watcher.stop()
    """

    def __init__(
        self,
        path: str | Path,
        interval: float = 30.0,
        on_reload: Callable[["ReloadableFuseReader"], None] | None = None,
        verify: bool = True,
    ) -> None:
        self._interval  = interval
        self._on_reload = on_reload
        self._stop_evt  = threading.Event()
        self._thread    = threading.Thread(target=self._watch, daemon=True)
        self.db         = ReloadableFuseReader(path, verify=verify)

    def start(self) -> None:
        self._thread.start()
        print(f"  👁   Watching {self.db._path.name} every {self._interval}s")

    def stop(self) -> None:
        self._stop_evt.set()
        self._thread.join()
        self.db.close()

    def _watch(self) -> None:
        while not self._stop_evt.wait(self._interval):
            try:
                if self.db.reload() and self._on_reload:
                    self._on_reload(self.db)
            except Exception as e:
                print(f"  ⚠️   Watcher error: {e}")

    def get(self, key: str | bytes) -> Any | None:
        return self.db.get(key)

    def exists(self, key: str | bytes) -> bool:
        return self.db.exists(key)

    def prefix(self, prefix: str | bytes) -> list[tuple[str, Any]]:
        return self.db.prefix(prefix)

    def stats(self) -> dict:
        return self.db.stats()


# ── FusePool ──────────────────────────────────────────────────────────────────

class FusePool:
    """
    Thread-safe round-robin reader pool with atomic swap().

    Example
    -------
    pool = FusePool("live.fsdb", size=4)
    pool.get("8.8.8.8")
    pool.swap("live_v2.fsdb")   # zero-downtime
    pool.close()
    """

    def __init__(self, path: str | Path, size: int = 4, verify: bool = True) -> None:
        self._path    = Path(path)
        self._size    = size
        self._lock    = threading.RLock()
        self._idx     = 0
        self._readers = [FuseReader(str(path), verify=verify) for _ in range(size)]
        print(f"  🏊  Pool ready: {size} readers → {self._path.name}")

    def _next(self) -> FuseReader:
        with self._lock:
            r = self._readers[self._idx % self._size]
            self._idx += 1
            return r

    def get(self, key: str | bytes) -> Any | None:
        return self._next().get(key)

    def exists(self, key: str | bytes) -> bool:
        return self._next().exists(key)

    def prefix(self, prefix: str | bytes) -> list[tuple[str, Any]]:
        return self._next().prefix(prefix)

    def swap(self, new_path: str | Path, verify: bool = True) -> None:
        """Atomically replace all readers with ones pointing to *new_path*."""
        new_path    = Path(new_path)
        new_readers = [FuseReader(str(new_path), verify=verify) for _ in range(self._size)]
        with self._lock:
            old, self._readers, self._path = self._readers, new_readers, new_path
        for r in old: r.close()
        s = new_readers[0].stats()
        print(f"  🔄  Pool swapped → {new_path.name} ({s['num_keys']} keys)")

    def close(self) -> None:
        with self._lock:
            for r in self._readers: r.close()

    def stats(self) -> dict:
        with self._lock:
            return {"path": str(self._path), "size": self._size,
                    "readers": [r.stats() for r in self._readers]}

    def __repr__(self) -> str:
        return f"FusePool('{self._path.name}', size={self._size})"


# ── merge() ───────────────────────────────────────────────────────────────────

def merge(*sources: str | Path, output: str | Path) -> None:
    """
    Merge two or more .fsdb files into one.
    Objects with identical content are stored only once (content-addressed).

    Example
    -------
    merge("geo_v1.fsdb", "geo_v2.fsdb", output="geo_merged.fsdb")
    """
    w:    FuseWriter      = FuseWriter()
    seen: dict[bytes, int] = {}

    for src in sources:
        db = FuseReader(str(src), verify=True)
        try:
            for key, raw in db._r.items_raw():
                if raw not in seen:
                    seen[raw] = w._w.add_object_raw(raw)
                w._w.add_key(key, seen[raw])
        finally:
            db.close()

    w._w.build(str(output))