#!/usr/bin/env python3
"""
Driver used by the Rust interoperability suite (``tests/interop.rs``).

Every subcommand is one half of a cross-language round-trip: the Rust test
writes a file and asks this script to read it, or the reverse. Output is
canonical JSON on stdout (sorted keys, no spaces) so both sides can compare
strings instead of reimplementing each other's value model.

Kept dependency-free beyond the ``fusedb`` package itself.
"""

from __future__ import annotations

import json
import sys
from typing import Any

import fusedb
import msgpack
from fusedb import FuseReader, FuseWriter, merge

# ── The canonical dataset, mirrored verbatim in tests/interop.rs ──────────────
#
# Two keys share the Google record on purpose: it is what proves object
# deduplication survives a round-trip between the two implementations.

RECORDS: list[tuple[str, Any]] = [
    ("google.com", {"asn": 15169, "cc": "US", "company": "Google"}),
    ("cloudflare.com", {"asn": 13335, "cc": "US", "company": "Cloudflare"}),
    ("8.8.8.8", {"asn": 15169, "cc": "US", "company": "Google"}),
    ("8.8.4.4", {"asn": 15169, "cc": "US", "company": "Google"}),
    ("1.1.1.1", {"asn": 13335, "cc": "US", "company": "Cloudflare"}),
]

# Exercises every msgpack type the two encoders must agree on.
MIXED: Any = {
    "bool": True,
    "float": 1.5,
    "int": 42,
    "list": [1, 2, 3],
    "nested": {"a": {"b": "c"}},
    "neg": -7,
    "null": None,
    "text": "héllo wörld",
}


def _canonical(obj: Any) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _emit(text: str) -> None:
    """Write *text* to stdout as UTF-8, whatever the platform thinks the console is.

    ``sys.stdout`` encodes with the locale encoding, which on Windows is the ANSI
    code page — cp1252 turns "héllo" into 0xE9 and the Rust side, which decodes
    strictly as UTF-8, rejects it. Going through the raw buffer makes the wire
    format explicit and identical everywhere.
    """
    sys.stdout.buffer.write(text.encode("utf-8"))
    sys.stdout.buffer.flush()


# ── subcommands ───────────────────────────────────────────────────────────────


def cmd_write(path: str) -> None:
    """Write the canonical dataset, sharing one object between aliased keys."""
    w = FuseWriter()
    by_company: dict[str, int] = {}
    for key, value in RECORDS:
        company = value["company"]
        if company not in by_company:
            by_company[company] = w.add_object(value)
        w.add_key(key, by_company[company])
    w.add("mixed", MIXED)
    w.build(path)


def cmd_read(path: str) -> None:
    """Dump everything a Rust test needs to assert on, as canonical JSON."""
    with FuseReader(path) as db:
        stats = db.stats()
        payload = {
            "keys": db.keys(),
            "items": dict(db.items()),
            "num_keys": stats["num_keys"],
            "num_objects": stats["num_objects"],
            "unique_objects": len(db.objects()),
            "prefix_8_8": [k for k, _ in db.prefix("8.8.")],
            "index_offset": stats["index_offset"],
            "data_offset": stats["data_offset"],
            "file_crc32": stats["file_crc32"],
            "version": stats["version"],
            "verified": db.verify(),
        }
    _emit(_canonical(payload))


def cmd_get(path: str, key: str) -> None:
    with FuseReader(path) as db:
        _emit(_canonical(db.get(key)))


def cmd_verify(path: str) -> None:
    """Print ``ok`` when the file passes deep CRC32 validation, else ``fail: …``."""
    try:
        with FuseReader(path) as db:
            _emit("ok" if db.verify() else "fail: verify returned False")
    except Exception as exc:  # noqa: BLE001 — the Rust side asserts on the text
        _emit(f"fail: {type(exc).__name__}: {exc}")


def cmd_merge(output: str, *sources: str) -> None:
    merge(*sources, output=output)


def cmd_pack_mixed() -> None:
    """Hex of Python's msgpack encoding of MIXED — the encoder-parity oracle."""
    _emit(msgpack.packb(MIXED, use_bin_type=True).hex())


def cmd_version() -> None:
    _emit(fusedb.__version__)


COMMANDS = {
    "write": cmd_write,
    "read": cmd_read,
    "get": cmd_get,
    "verify": cmd_verify,
    "merge": cmd_merge,
    "pack-mixed": cmd_pack_mixed,
    "version": cmd_version,
}


def main() -> int:
    if len(sys.argv) < 2 or sys.argv[1] not in COMMANDS:
        sys.stderr.write(f"usage: {sys.argv[0]} {{{'|'.join(COMMANDS)}}} [args...]\n")
        return 2
    COMMANDS[sys.argv[1]](*sys.argv[2:])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
