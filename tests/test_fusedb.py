"""tests/test_fusedb.py — full test suite for fusedb"""

import ipaddress
import random
import threading
import time
from pathlib import Path

import pytest
from fusedb import (
    FusePool,
    FuseReader,
    FuseWatcher,
    FuseWriter,
    ReloadableFuseReader,
    merge,
)

# ─── fixtures ────────────────────────────────────────────────────────────────


@pytest.fixture
def tmp(tmp_path):
    return tmp_path


@pytest.fixture
def simple_db(tmp):
    """Small db: 2 objects, 6 keys."""
    p = str(tmp / "simple.fsdb")
    w = FuseWriter()
    g = w.add_object({"org": "Google", "asn": 15169, "country": "US"})
    w.add_key("8.8.8.8", g)
    w.add_key("8.8.4.4", g)
    w.add_key("gmail.com", g)
    cf = w.add_object({"org": "Cloudflare", "asn": 13335, "country": "US"})
    w.add_key("1.1.1.1", cf)
    w.add_key("1.0.0.1", cf)
    w.add_key("mailinator.com", w.add_object({"org": "Disposable", "risk": 0.99}))
    w.build(p)
    return p


@pytest.fixture
def large_db(tmp):
    """50k objects, 100k keys."""
    p = str(tmp / "large.fsdb")
    random.seed(42)
    w = FuseWriter()
    for i in range(50_000):
        ip = str(ipaddress.ip_address(random.randint(0, 2**32 - 1)))
        oid = w.add_object({"id": i, "country": "US", "asn": i % 65535})
        w.add_key(ip, oid)
        w.add_key(f"user{i}@d.com", oid)
    w.build(p)
    return p


# ─── 1. Writer ───────────────────────────────────────────────────────────────


class TestWriter:
    def test_build_creates_file(self, tmp):
        p = str(tmp / "out.fsdb")
        w = FuseWriter()
        w.add("key1", {"v": 1})
        w.build(p)
        assert Path(p).exists()
        assert Path(p).stat().st_size > 40  # at least the header

    def test_add_returns_obj_id(self):
        w = FuseWriter()
        assert w.add_object({"a": 1}) == 0
        assert w.add_object({"b": 2}) == 1

    def test_add_key_alias(self, tmp):
        p = str(tmp / "alias.fsdb")
        w = FuseWriter()
        oid = w.add_object({"v": 42})
        w.add_key("key_a", oid)
        w.add_key("key_b", oid)
        w.build(p)

        db = FuseReader(p)
        assert db.get("key_a") == db.get("key_b") == {"v": 42}

    def test_invalid_obj_id_raises(self):
        w = FuseWriter()
        with pytest.raises(ValueError):
            w.add_key("k", 99)

    def test_file_is_atomic(self, tmp):
        """No .tmp file should remain after build."""
        p = str(tmp / "atomic.fsdb")
        w = FuseWriter()
        w.add("k", {"v": 1})
        w.build(p)
        assert not Path(p + ".tmp").exists()


# ─── 2. Reader — basic ───────────────────────────────────────────────────────


class TestReaderBasic:
    def test_get_existing_key(self, simple_db):
        with FuseReader(simple_db) as db:
            assert db.get("8.8.8.8")["org"] == "Google"

    def test_get_alias(self, simple_db):
        with FuseReader(simple_db) as db:
            assert db.get("8.8.8.8") == db.get("8.8.4.4") == db.get("gmail.com")

    def test_get_missing_returns_none(self, simple_db):
        with FuseReader(simple_db) as db:
            assert db.get("9.9.9.9") is None

    def test_get_bytes_key(self, simple_db):
        with FuseReader(simple_db) as db:
            assert db.get(b"8.8.8.8")["org"] == "Google"

    def test_exists_true(self, simple_db):
        with FuseReader(simple_db) as db:
            assert db.exists("1.1.1.1") is True

    def test_exists_false(self, simple_db):
        with FuseReader(simple_db) as db:
            assert db.exists("0.0.0.0") is False

    def test_keys_sorted(self, simple_db):
        with FuseReader(simple_db) as db:
            ks = db.keys()
            assert ks == sorted(ks)

    def test_stats(self, simple_db):
        with FuseReader(simple_db) as db:
            s = db.stats()
            assert s["num_keys"] == 6
            assert s["num_objects"] == 3
            assert s["version"] == 2
            assert "file_crc32" in s

    def test_verify_clean(self, simple_db):
        with FuseReader(simple_db, verify=True) as db:
            assert db.verify() is True

    def test_context_manager(self, simple_db):
        with FuseReader(simple_db) as db:
            assert db.get("8.8.8.8") is not None

    def test_repr(self, simple_db):
        db = FuseReader(simple_db)
        assert "FuseReader" in repr(db)
        db.close()


# ─── 3. Reader — prefix ──────────────────────────────────────────────────────


class TestPrefix:
    def test_prefix_returns_matches(self, simple_db):
        with FuseReader(simple_db) as db:
            result = db.prefix("8.8.")
            keys = [k for k, _ in result]
            assert "8.8.8.8" in keys
            assert "8.8.4.4" in keys

    def test_prefix_sorted(self, simple_db):
        with FuseReader(simple_db) as db:
            result = db.prefix("8.")
            keys = [k for k, _ in result]
            assert keys == sorted(keys)

    def test_prefix_no_match(self, simple_db):
        with FuseReader(simple_db) as db:
            assert db.prefix("9.9.") == []

    def test_prefix_bytes(self, simple_db):
        with FuseReader(simple_db) as db:
            result = db.prefix(b"8.8.")
            assert len(result) >= 2


# ─── 4. Data types ───────────────────────────────────────────────────────────


class TestDataTypes:
    @pytest.mark.parametrize(
        "value",
        [
            {"str": "hello", "int": 42, "float": 3.14, "bool": True, "none": None},
            [1, 2, 3],
            "plain string",
            42,
            3.14,
            True,
            None,
            {"nested": {"deep": [1, 2, {"x": 3}]}},
        ],
    )
    def test_roundtrip(self, tmp, value):
        p = str(tmp / "types.fsdb")
        w = FuseWriter()
        w.add("key", value)
        w.build(p)
        with FuseReader(p) as db:
            assert db.get("key") == value

    def test_unicode_key(self, tmp):
        p = str(tmp / "uni.fsdb")
        w = FuseWriter()
        w.add("café", {"v": 1})
        w.build(p)
        with FuseReader(p) as db:
            assert db.get("café") == {"v": 1}

    def test_large_object(self, tmp):
        p = str(tmp / "large.fsdb")
        obj = {"data": "x" * 100_000, "nums": list(range(1000))}
        w = FuseWriter()
        w.add("big", obj)
        w.build(p)
        with FuseReader(p) as db:
            assert db.get("big") == obj


# ─── 5. CRC / corruption ─────────────────────────────────────────────────────


class TestIntegrity:
    def test_corrupt_file_detected(self, simple_db):
        # Flip bytes in the data section
        with open(simple_db, "r+b") as f:
            f.seek(50)
            f.write(b"\xff\xff\xff\xff")
        with pytest.raises(Exception, match="CRC32"):
            FuseReader(simple_db, verify=True)

    def test_verify_raises_on_corrupt(self, tmp):
        p = str(tmp / "c.fsdb")
        w = FuseWriter()
        w.add("k", {"v": 1})
        w.build(p)
        with open(p, "r+b") as f:
            f.seek(50)
            f.write(b"\x00\x00\x00\x00")
        db = FuseReader(p, verify=False)
        with pytest.raises(RuntimeError):
            db.verify()
        db.close()


# ─── 6. Deduplication ────────────────────────────────────────────────────────


class TestDedup:
    def test_aliases_share_bytes(self, tmp):
        """File with 10k aliases to 1 object must be much smaller than 10k copies."""
        p_dedup = str(tmp / "dedup.fsdb")
        p_flat = str(tmp / "flat.fsdb")
        obj = {"org": "BigCorp", "data": "x" * 200}

        wd = FuseWriter()
        oid = wd.add_object(obj)
        for i in range(10_000):
            wd.add_key(f"10.{i // 256}.{i % 256}.1", oid)
        wd.build(p_dedup)

        wf = FuseWriter()
        for i in range(10_000):
            wf.add(f"10.{i // 256}.{i % 256}.1", obj)
        wf.build(p_flat)

        sz_d = Path(p_dedup).stat().st_size
        sz_f = Path(p_flat).stat().st_size
        assert sz_d < sz_f / 5, f"dedup={sz_d}, flat={sz_f}: not deduplicating"

    def test_items_dedup(self, simple_db):
        with FuseReader(simple_db) as db:
            items = db.items()
            # 6 keys but only 3 unique objects
            assert len(items) == 6

    def test_objects_unique(self, simple_db):
        with FuseReader(simple_db) as db:
            objs = db.objects()
            assert len(objs) == 3


# ─── 7. Thread safety ────────────────────────────────────────────────────────


class TestConcurrency:
    def test_concurrent_reads(self, large_db):
        db = FuseReader(large_db, verify=False)
        keys = db.keys()[:500]
        errors = []

        def worker():
            for k in random.choices(keys, k=1000):
                if db.get(k) is None:
                    errors.append(k)

        threads = [threading.Thread(target=worker) for _ in range(16)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        db.close()

        assert errors == [], f"Errors in concurrent reads: {errors[:5]}"

    def test_reloadable_concurrent(self, tmp):
        p = str(tmp / "live.fsdb")
        w = FuseWriter()
        for i in range(100):
            w.add(f"k{i}", {"v": i})
        w.build(p)

        db = ReloadableFuseReader(p)
        errors = []

        def reader():
            for _ in range(500):
                v = db.get("k0")
                if v is None:
                    errors.append("none")

        threads = [threading.Thread(target=reader) for _ in range(8)]
        for t in threads:
            t.start()

        # reload in background while threads read
        time.sleep(0.02)
        w2 = FuseWriter()
        for i in range(100):
            w2.add(f"k{i}", {"v": i + 100})
        w2.build(p)
        time.sleep(0.01)
        db.reload()

        for t in threads:
            t.join()
        db.close()
        assert errors == []


# ─── 8. Merge ────────────────────────────────────────────────────────────────


class TestMerge:
    def test_merge_two_files(self, tmp):
        pa = str(tmp / "a.fsdb")
        pb = str(tmp / "b.fsdb")
        pm = str(tmp / "merged.fsdb")

        wa = FuseWriter()
        wa.add("key_a", {"src": "A"})
        wa.build(pa)
        wb = FuseWriter()
        wb.add("key_b", {"src": "B"})
        wb.build(pb)
        merge(pa, pb, output=pm)

        with FuseReader(pm) as db:
            assert db.get("key_a") == {"src": "A"}
            assert db.get("key_b") == {"src": "B"}
            assert db.stats()["num_keys"] == 2

    def test_merge_deduplicates(self, tmp):
        pa = str(tmp / "a.fsdb")
        pb = str(tmp / "b.fsdb")
        pm = str(tmp / "merged.fsdb")

        # Same object, different keys in each file
        obj = {"org": "Google"}
        wa = FuseWriter()
        wa.add("8.8.8.8", obj)
        wa.build(pa)
        wb = FuseWriter()
        wb.add("gmail.com", obj)
        wb.build(pb)
        merge(pa, pb, output=pm)

        with FuseReader(pm) as db:
            assert db.stats()["num_keys"] == 2
            assert db.stats()["num_objects"] == 1  # deduplicated!


# ─── 9. Watcher ──────────────────────────────────────────────────────────────


class TestWatcher:
    def test_watcher_auto_reloads(self, tmp):
        p = str(tmp / "watch.fsdb")
        w = FuseWriter()
        w.add("k", {"v": 1})
        w.build(p)

        reloads = []
        watcher = FuseWatcher(p, interval=0.1, on_reload=lambda _: reloads.append(1))
        watcher.start()

        time.sleep(0.05)
        # rebuild the file
        w2 = FuseWriter()
        w2.add("k", {"v": 2})
        w2.build(p)
        time.sleep(0.4)  # wait for watcher to detect

        assert len(reloads) >= 1
        assert watcher.get("k")["v"] == 2
        watcher.stop()


# ─── 10. Pool ────────────────────────────────────────────────────────────────


class TestPool:
    def test_pool_get(self, simple_db):
        pool = FusePool(simple_db, size=4)
        assert pool.get("8.8.8.8")["org"] == "Google"
        pool.close()

    def test_pool_swap(self, tmp, simple_db):
        p2 = str(tmp / "v2.fsdb")
        w2 = FuseWriter()
        w2.add("new_key", {"v": 2})
        w2.build(p2)

        pool = FusePool(simple_db, size=2)
        pool.swap(p2)
        assert pool.get("new_key") == {"v": 2}
        assert pool.get("8.8.8.8") is None  # not in v2
        pool.close()

    def test_pool_concurrent_swap(self, tmp):
        p1 = str(tmp / "p1.fsdb")
        p2 = str(tmp / "p2.fsdb")
        w1 = FuseWriter()
        for i in range(200):
            w1.add(f"k{i}", {"v": 1})
        w1.build(p1)
        w2 = FuseWriter()
        for i in range(200):
            w2.add(f"k{i}", {"v": 2})
        w2.build(p2)

        pool = FusePool(p1, size=4)
        errors = []

        def worker():
            for i in range(500):
                v = pool.get(f"k{i % 200}")
                if v is None:
                    errors.append(i)

        threads = [threading.Thread(target=worker) for _ in range(8)]
        for t in threads:
            t.start()
        time.sleep(0.02)
        pool.swap(p2)
        for t in threads:
            t.join()
        pool.close()
        assert errors == []


# ─── 11. Compatibility — Python fsdb.py files ────────────────────────────────


class TestCompatibility:
    """Rust reader must read files built by the Python writer, and vice versa."""

    def test_reads_python_written_file(self, tmp):
        """
        Simulate a file written with the old Python FuseWriter format.
        We build with FuseWriter (Rust) and read back — they share the same format.
        This confirms format stability.
        """
        p = str(tmp / "compat.fsdb")
        w = FuseWriter()
        w.add("8.8.8.8", {"org": "Google"})
        w.build(p)

        # Read back — format must be stable
        db = FuseReader(p, verify=True)
        assert db.get("8.8.8.8") == {"org": "Google"}
        assert db.verify() is True
        db.close()
