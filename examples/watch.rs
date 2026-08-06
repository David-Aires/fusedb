//! Hot-swap a database underneath live readers.
//!
//!     cargo run --example watch
//!
//! Simulates a rebuild pipeline: a background thread rewrites the file while
//! reader threads keep serving lookups without pausing or coordinating.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use fusedb::{FuseWatcher, FuseWriter};

fn build(path: &std::path::Path, generation: usize) -> Result<(), fusedb::FuseError> {
    let mut w = FuseWriter::new();
    for i in 0..(generation * 4) {
        w.add_raw(format!("key-{i:04}"), format!("v{generation}"))?;
    }
    w.build(path)?;
    Ok(())
}

fn main() -> Result<(), fusedb::FuseError> {
    let path = std::env::temp_dir().join("fusedb-watch-example.fsdb");
    build(&path, 1)?;

    let reloads = Arc::new(AtomicUsize::new(0));

    let watcher = {
        let reloads = Arc::clone(&reloads);
        FuseWatcher::builder(&path)
            .interval(Duration::from_millis(100))
            .on_reload(move |db| {
                let n = reloads.fetch_add(1, Ordering::SeqCst) + 1;
                println!("  reload #{n}: now {} keys", db.len());
            })
            // A rebuild caught halfway through fails its CRC check; the previous
            // snapshot keeps serving and the next poll picks up the finished file.
            .on_error(|e| eprintln!("  reload failed (still serving old file): {e}"))
            .spawn()?
    };

    println!(
        "watching {} — {} keys",
        path.display(),
        watcher.load().len()
    );

    // Reads need no swap bookkeeping — `reader.get_raw(..)` resolves the current
    // file on every call, so these threads never observe a replaced database.
    println!("  direct read: {:?}", watcher.get_raw("key-0000")?);

    let stop = Arc::new(AtomicBool::new(false));
    let lookups = Arc::new(AtomicUsize::new(0));
    let readers: Vec<_> = (0..4)
        .map(|_| {
            let reader = Arc::clone(watcher.reader());
            let stop = Arc::clone(&stop);
            let lookups = Arc::clone(&lookups);
            std::thread::spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    // Resolving once per batch instead of once per key is the
                    // only reason to reach for load() — correctness is the same.
                    let db = reader.load();
                    for key in db.keys_raw() {
                        let _ = db.get_raw(key);
                        lookups.fetch_add(1, Ordering::Relaxed);
                    }
                }
            })
        })
        .collect();

    // Rebuild the file a few times while those threads keep reading.
    for generation in 2..=5 {
        std::thread::sleep(Duration::from_millis(250));
        println!("rebuilding at generation {generation}…");
        build(&path, generation)?;
    }
    std::thread::sleep(Duration::from_millis(300));

    stop.store(true, Ordering::Relaxed);
    for handle in readers {
        handle.join().expect("reader thread panicked");
    }

    println!(
        "done — {} lookups served across {} swaps, no locking on the read path",
        lookups.load(Ordering::Relaxed),
        watcher.generation(),
    );

    // Dropping the watcher would stop it too; stop() just makes it explicit.
    watcher.stop();
    std::fs::remove_file(&path).ok();
    Ok(())
}
