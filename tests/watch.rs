// tests/watch.rs
// ──────────────────────────────────────────────────────────────────────────────
// Hot-swap and background-watch behaviour.
//
// Timing-sensitive tests poll with a deadline instead of sleeping a fixed
// amount, so they stay reliable on a loaded CI runner without being slow when
// the machine is idle.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tempfile::TempDir;

use fusedb::{FuseError, FuseReader, FuseWatcher, FuseWriter, ReloadableReader};

const POLL: Duration = Duration::from_millis(10);
const DEADLINE: Duration = Duration::from_secs(5);

/// Write a database whose key set — and therefore file length — is determined
/// by `n`, so every rebuild is unambiguously a different file.
fn build(path: &std::path::Path, n: usize) {
    let mut w = FuseWriter::new();
    for i in 0..n {
        w.add_raw(format!("key-{i:04}"), format!("value-{i}"))
            .unwrap();
    }
    w.build(path).unwrap();
}

/// Spin until `cond` holds or the deadline passes.
fn wait_for(what: &str, mut cond: impl FnMut() -> bool) {
    let start = Instant::now();
    while start.elapsed() < DEADLINE {
        if cond() {
            return;
        }
        std::thread::sleep(POLL);
    }
    panic!("timed out after {DEADLINE:?} waiting for {what}");
}

// ── ReloadableReader ──────────────────────────────────────────────────────────

#[test]
fn snapshot_serves_reads() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("live.fsdb");
    build(&path, 3);

    let db = ReloadableReader::open(&path).unwrap();
    let snapshot = db.load();

    assert_eq!(snapshot.len(), 3);
    assert_eq!(
        snapshot.get_raw("key-0001").unwrap().as_deref(),
        Some(&b"value-1"[..])
    );
    assert_eq!(db.path(), path);
    assert!(db.verifies());
    assert_eq!(db.generation(), 0);
}

#[test]
fn reload_is_a_no_op_when_the_file_is_untouched() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("live.fsdb");
    build(&path, 3);

    let db = ReloadableReader::open(&path).unwrap();
    assert!(!db.reload_if_changed().unwrap());
    assert!(!db.reload_if_changed().unwrap());
    assert_eq!(db.generation(), 0);
}

#[test]
fn reload_picks_up_a_rebuild() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("live.fsdb");
    build(&path, 3);

    let db = ReloadableReader::open(&path).unwrap();
    build(&path, 7);

    assert!(db.reload_if_changed().unwrap());
    assert_eq!(db.load().len(), 7);
    assert_eq!(db.generation(), 1);

    // A second check finds nothing new.
    assert!(!db.reload_if_changed().unwrap());
    assert_eq!(db.generation(), 1);
}

#[test]
fn an_existing_snapshot_survives_a_reload() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("live.fsdb");
    build(&path, 3);

    let db = ReloadableReader::open(&path).unwrap();
    let old = db.load();

    build(&path, 7);
    assert!(db.reload_if_changed().unwrap());

    // The old snapshot keeps its own mmap — a reader mid-request is never
    // yanked out from under.
    assert_eq!(old.len(), 3);
    assert!(old.get_raw("key-0001").unwrap().is_some());
    assert_eq!(db.load().len(), 7);
}

#[test]
fn unconditional_reload_ignores_the_stamp() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("live.fsdb");
    build(&path, 3);

    let db = ReloadableReader::open(&path).unwrap();
    assert!(!db.reload_if_changed().unwrap());

    db.reload().unwrap();
    assert_eq!(db.generation(), 1, "reload() always swaps");
    assert_eq!(db.load().len(), 3);
}

#[test]
fn a_failed_reload_keeps_serving_the_previous_file() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("live.fsdb");
    build(&path, 3);

    let db = ReloadableReader::open(&path).unwrap();

    // Something wrote garbage where the database used to be.
    std::fs::write(&path, b"not a fusedb file at all").unwrap();

    assert!(matches!(db.reload_if_changed(), Err(FuseError::Corrupt(_))));
    assert_eq!(db.load().len(), 3, "the good snapshot must survive");
    assert_eq!(db.generation(), 0);

    // And recovery works once a valid file reappears.
    build(&path, 5);
    assert!(db.reload_if_changed().unwrap());
    assert_eq!(db.load().len(), 5);
}

#[test]
fn a_missing_file_is_not_an_error() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("live.fsdb");
    build(&path, 3);

    let db = ReloadableReader::open(&path).unwrap();
    std::fs::remove_file(&path).unwrap();

    // A writer mid-rename looks exactly like this; it is transient, not fatal.
    assert!(!db.reload_if_changed().unwrap());
    assert_eq!(db.load().len(), 3);
}

#[test]
fn opening_a_missing_file_fails_immediately() {
    let dir = TempDir::new().unwrap();
    let missing = dir.path().join("nope.fsdb");

    assert!(matches!(
        ReloadableReader::open(&missing),
        Err(FuseError::Io(_))
    ));
}

#[test]
fn unverified_mode_skips_crc_validation() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("live.fsdb");
    build(&path, 3);

    let db = ReloadableReader::open_unverified(&path).unwrap();
    assert!(!db.verifies());
    assert_eq!(db.load().len(), 3);
}

#[test]
fn reloads_are_safe_from_many_threads() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("live.fsdb");
    build(&path, 3);

    let db = Arc::new(ReloadableReader::open(&path).unwrap());
    build(&path, 7);

    // Eight threads racing to reload the same change: the stamp mutex must let
    // exactly one of them observe the swap.
    let swaps = Arc::new(AtomicUsize::new(0));
    let mut handles = Vec::new();
    for _ in 0..8 {
        let db = Arc::clone(&db);
        let swaps = Arc::clone(&swaps);
        handles.push(std::thread::spawn(move || {
            if db.reload_if_changed().unwrap() {
                swaps.fetch_add(1, Ordering::Relaxed);
            }
            assert!(db.load().len() >= 3);
        }));
    }
    for h in handles {
        h.join().unwrap();
    }

    assert_eq!(swaps.load(Ordering::Relaxed), 1, "one swap, not eight");
    assert_eq!(db.generation(), 1);
    assert_eq!(db.load().len(), 7);
}

// ── direct reads (no manual load()) ───────────────────────────────────────────

#[test]
fn reads_go_straight_through_without_a_snapshot() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("live.fsdb");
    build(&path, 3);

    let db = ReloadableReader::open(&path).unwrap();

    assert_eq!(
        db.get_raw("key-0001").unwrap().as_deref(),
        Some(&b"value-1"[..])
    );
    assert!(db.exists("key-0002"));
    assert!(!db.exists("key-9999"));
    assert_eq!(db.len(), 3);
    assert!(!db.is_empty());
    assert_eq!(db.keys(), ["key-0000", "key-0001", "key-0002"]);
    assert_eq!(db.keys_raw().len(), 3);
    assert_eq!(db.items_raw().unwrap().len(), 3);
    assert_eq!(db.objects_raw().unwrap().len(), 3);
    assert_eq!(db.prefix_raw("key-000").unwrap().len(), 3);
    assert_eq!(db.stats().num_keys, 3);
    assert!(db.verify().is_ok());
    assert!(db.is_valid());
}

#[test]
fn direct_reads_follow_the_swap() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("live.fsdb");
    build(&path, 3);

    let db = ReloadableReader::open(&path).unwrap();
    assert!(!db.exists("key-0005"));

    build(&path, 7);
    assert!(db.reload_if_changed().unwrap());

    // No re-fetch of any handle: the same `db` binding now reads the new file.
    assert!(db.exists("key-0005"));
    assert_eq!(db.len(), 7);
    assert_eq!(db.stats().num_keys, 7);
}

#[test]
fn watcher_reads_follow_the_background_swap() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("live.fsdb");
    build(&path, 3);

    let watcher = FuseWatcher::spawn(&path, Duration::from_millis(20)).unwrap();
    assert_eq!(watcher.len(), 3);
    assert!(!watcher.exists("key-0008"));

    build(&path, 9);

    // Nothing here reloads or reloads-if-changed; the polling thread does it,
    // and plain reads pick the new file up on their own.
    wait_for("reads to observe the new file", || watcher.len() == 9);
    assert!(watcher.exists("key-0008"));
    assert_eq!(
        watcher.get_raw("key-0008").unwrap().as_deref(),
        Some(&b"value-8"[..])
    );
    assert_eq!(watcher.stats().num_keys, 9);
    assert_eq!(watcher.prefix_raw("key-000").unwrap().len(), 9);
}

#[test]
fn typed_reads_work_without_a_snapshot() {
    #[derive(serde::Serialize, serde::Deserialize, PartialEq, Debug)]
    struct Org {
        company: String,
        asn: u32,
    }

    fn build_typed(path: &std::path::Path, asn: u32) {
        let mut w = FuseWriter::new();
        w.add(
            "google.com",
            &Org {
                company: "Google".into(),
                asn,
            },
        )
        .unwrap();
        w.build(path).unwrap();
    }

    let dir = TempDir::new().unwrap();
    let path = dir.path().join("typed.fsdb");
    build_typed(&path, 15169);

    let watcher = FuseWatcher::spawn(&path, Duration::from_millis(20)).unwrap();

    let hit = watcher.get::<Org>("google.com").unwrap().unwrap();
    assert_eq!(hit.asn, 15169);
    assert_eq!(watcher.items::<Org>().unwrap().len(), 1);
    assert_eq!(watcher.objects::<Org>().unwrap().len(), 1);
    assert_eq!(watcher.prefix::<Org>("google").unwrap().len(), 1);

    build_typed(&path, 42);
    wait_for("the typed read to follow the swap", || {
        watcher
            .get::<Org>("google.com")
            .map(|hit| hit.is_some_and(|o| o.asn == 42))
            .unwrap_or(false)
    });
}

#[test]
fn a_cached_snapshot_goes_stale_but_direct_reads_do_not() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("live.fsdb");
    build(&path, 3);

    let db = ReloadableReader::open(&path).unwrap();
    let cached = db.load(); // the mistake the proxy methods exist to prevent

    build(&path, 7);
    assert!(db.reload_if_changed().unwrap());

    assert_eq!(cached.len(), 3, "a stored snapshot never moves");
    assert_eq!(db.len(), 7, "direct reads always see the newest file");
}

// ── FuseWatcher ───────────────────────────────────────────────────────────────

#[test]
fn watcher_reloads_in_the_background() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("live.fsdb");
    build(&path, 3);

    let reloads = Arc::new(AtomicUsize::new(0));
    let keys_seen = Arc::new(AtomicUsize::new(0));

    let watcher = {
        let reloads = Arc::clone(&reloads);
        let keys_seen = Arc::clone(&keys_seen);
        FuseWatcher::builder(&path)
            .interval(Duration::from_millis(20))
            .on_reload(move |db| {
                keys_seen.store(db.len(), Ordering::SeqCst);
                reloads.fetch_add(1, Ordering::SeqCst);
            })
            .spawn()
            .unwrap()
    };

    assert_eq!(watcher.load().len(), 3);
    assert_eq!(
        reloads.load(Ordering::SeqCst),
        0,
        "no spurious first reload"
    );

    build(&path, 9);
    wait_for("the watcher to notice the rebuild", || {
        reloads.load(Ordering::SeqCst) == 1
    });

    assert_eq!(keys_seen.load(Ordering::SeqCst), 9);
    assert_eq!(watcher.load().len(), 9);
    assert_eq!(watcher.generation(), 1);
}

#[test]
fn watcher_reports_errors_and_keeps_polling() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("live.fsdb");
    build(&path, 3);

    let errors = Arc::new(AtomicUsize::new(0));
    let reloads = Arc::new(AtomicUsize::new(0));

    let watcher = {
        let errors = Arc::clone(&errors);
        let reloads = Arc::clone(&reloads);
        FuseWatcher::builder(&path)
            .interval(Duration::from_millis(20))
            .on_error(move |_| {
                errors.fetch_add(1, Ordering::SeqCst);
            })
            .on_reload(move |_| {
                reloads.fetch_add(1, Ordering::SeqCst);
            })
            .spawn()
            .unwrap()
    };

    std::fs::write(&path, b"garbage").unwrap();
    wait_for("the corrupt file to be reported", || {
        errors.load(Ordering::SeqCst) >= 1
    });
    assert_eq!(watcher.load().len(), 3, "still serving the last good build");

    // Polling did not stop just because one attempt failed.
    build(&path, 6);
    wait_for("recovery after the error", || {
        reloads.load(Ordering::SeqCst) == 1
    });
    assert_eq!(watcher.load().len(), 6);
}

#[test]
fn stopping_the_watcher_ends_the_thread() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("live.fsdb");
    build(&path, 3);

    let reloads = Arc::new(AtomicUsize::new(0));
    let watcher = {
        let reloads = Arc::clone(&reloads);
        FuseWatcher::builder(&path)
            .interval(Duration::from_millis(10))
            .on_reload(move |_| {
                reloads.fetch_add(1, Ordering::SeqCst);
            })
            .spawn()
            .unwrap()
    };

    build(&path, 5);
    wait_for("the first reload", || reloads.load(Ordering::SeqCst) == 1);

    // stop() joins, so once it returns the thread is provably gone and no
    // further rebuild can be observed.
    watcher.stop();

    build(&path, 8);
    std::thread::sleep(Duration::from_millis(120)); // many intervals
    assert_eq!(
        reloads.load(Ordering::SeqCst),
        1,
        "no reloads after stop() returned"
    );
}

#[test]
fn dropping_the_watcher_stops_it_too() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("live.fsdb");
    build(&path, 3);

    let reloads = Arc::new(AtomicUsize::new(0));
    {
        let counter = Arc::clone(&reloads);
        let _watcher = FuseWatcher::builder(&path)
            .interval(Duration::from_millis(10))
            .on_reload(move |_| {
                counter.fetch_add(1, Ordering::SeqCst);
            })
            .spawn()
            .unwrap();

        build(&path, 5);
        wait_for("the first reload", || reloads.load(Ordering::SeqCst) == 1);
    } // Drop joins the thread here.

    build(&path, 8);
    std::thread::sleep(Duration::from_millis(120));
    assert_eq!(reloads.load(Ordering::SeqCst), 1, "Drop must stop polling");
}

#[test]
fn stopping_is_prompt_not_one_interval_late() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("live.fsdb");
    build(&path, 3);

    let watcher = FuseWatcher::spawn(&path, Duration::from_secs(3600)).unwrap();

    // The condvar must wake the sleeping poll thread immediately rather than
    // letting the hour-long interval elapse.
    let start = Instant::now();
    watcher.stop();
    assert!(
        start.elapsed() < Duration::from_secs(2),
        "stop() took {:?} — it waited for the poll interval",
        start.elapsed()
    );
}

#[test]
fn reload_now_bypasses_the_poll_interval() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("live.fsdb");
    build(&path, 3);

    let watcher = FuseWatcher::spawn(&path, Duration::from_secs(3600)).unwrap();
    build(&path, 5);

    assert!(watcher.reload_now().unwrap());
    assert_eq!(watcher.load().len(), 5);
    assert!(!watcher.reload_now().unwrap());
}

#[test]
fn a_zero_interval_is_rejected() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("live.fsdb");
    build(&path, 3);

    assert!(matches!(
        FuseWatcher::builder(&path).interval(Duration::ZERO).spawn(),
        Err(FuseError::InvalidArg(_))
    ));
}

#[test]
fn spawn_fails_when_the_file_cannot_be_opened() {
    let dir = TempDir::new().unwrap();
    let missing = dir.path().join("nope.fsdb");

    // No thread is left running behind a failed spawn.
    assert!(matches!(
        FuseWatcher::spawn(&missing, Duration::from_millis(10)),
        Err(FuseError::Io(_))
    ));
}

#[test]
fn the_reader_handle_is_shareable_across_threads() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("live.fsdb");
    build(&path, 4);

    let watcher = FuseWatcher::spawn(&path, Duration::from_millis(20)).unwrap();
    let reader = Arc::clone(watcher.reader());

    let handles: Vec<_> = (0..4)
        .map(|_| {
            let reader = Arc::clone(&reader);
            std::thread::spawn(move || {
                for _ in 0..50 {
                    assert!(reader.load().len() >= 4);
                }
            })
        })
        .collect();

    build(&path, 12);
    for h in handles {
        h.join().unwrap();
    }

    wait_for("the background swap", || watcher.load().len() == 12);
}

#[test]
fn watcher_types_are_send_and_sync() {
    fn assert_send_sync<T: Send + Sync>() {}

    assert_send_sync::<FuseReader>();
    assert_send_sync::<ReloadableReader>();
    assert_send_sync::<FuseWatcher>();
    assert_send_sync::<Arc<ReloadableReader>>();
}
