// src/api/watch.rs
// ──────────────────────────────────────────────────────────────────────────────
// Hot-swapping a `.fsdb` file underneath live readers.
//
// `.fsdb` files are immutable once built; updates mean rebuilding and swapping.
// `WriterCore::build` renames the finished file into place, so a swap is always
// a whole-file transition — there is no torn intermediate state to guard against.
//
// Two pieces:
//   ReloadableReader  the swap itself — you decide when to check
//   FuseWatcher       a background thread that checks for you
//
// Reads come in two shapes:
//
//   watcher.get("k")          convenience — resolves the current reader per call
//   watcher.load().get("k")   snapshot    — resolve once, reuse for a batch
//
// Both always see the newest successful swap. The snapshot form exists because
// resolving once and reading a thousand keys beats resolving a thousand times,
// and because a held snapshot is a stable view: it cannot change underneath a
// multi-step read even if the file is swapped mid-way.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::thread::{self, JoinHandle};
use std::time::{Duration, SystemTime};

use crate::core::{FuseError, FuseResult};

use super::reader::FuseReader;
use super::stats::Stats;

#[cfg(feature = "msgpack")]
use serde::de::DeserializeOwned;

// ── lock helpers ──────────────────────────────────────────────────────────────
//
// A panic while a lock is held cannot corrupt what we store — an `Arc` swap and
// a `Copy` stamp are both atomic with respect to the data they describe. So
// poisoning carries no information here, and propagating it would turn an
// unrelated panic in someone's callback into a panic in every later `load()`.

fn read_ignoring_poison<T>(lock: &RwLock<T>) -> RwLockReadGuard<'_, T> {
    lock.read().unwrap_or_else(|e| e.into_inner())
}

fn write_ignoring_poison<T>(lock: &RwLock<T>) -> RwLockWriteGuard<'_, T> {
    lock.write().unwrap_or_else(|e| e.into_inner())
}

fn lock_ignoring_poison<T>(lock: &Mutex<T>) -> MutexGuard<'_, T> {
    lock.lock().unwrap_or_else(|e| e.into_inner())
}

// ── read proxies ──────────────────────────────────────────────────────────────

/// Generates the read surface shared by [`ReloadableReader`] and [`FuseWatcher`].
///
/// Every method resolves the current reader through `load()` and forwards to it,
/// so callers never hold a stale handle by accident — the common mistake when
/// the only entry point is a snapshot you are free to store in a struct field.
///
/// The macro exists so the two types cannot drift apart. Both already have
/// `load()`; that is the only thing it assumes.
macro_rules! impl_reader_proxy {
    ($ty:ident) => {
        impl $ty {
            /// O(1) exact lookup against the current file, raw stored bytes.
            pub fn get_raw(&self, key: impl AsRef<[u8]>) -> FuseResult<Option<Vec<u8>>> {
                self.load().get_raw(key)
            }

            /// Key presence check against the current file.
            pub fn exists(&self, key: impl AsRef<[u8]>) -> bool {
                self.load().exists(key)
            }

            /// Number of keys in the current file.
            pub fn len(&self) -> usize {
                self.load().len()
            }

            /// Does the current file contain no keys?
            pub fn is_empty(&self) -> bool {
                self.load().is_empty()
            }

            /// All keys in the current file, sorted, UTF-8 lossy decoded.
            pub fn keys(&self) -> Vec<String> {
                self.load().keys()
            }

            /// All keys in the current file, sorted, as raw bytes.
            ///
            /// Allocates a copy — [`load`](Self::load) then
            /// [`FuseReader::keys_raw`] borrows instead.
            pub fn keys_raw(&self) -> Vec<Vec<u8>> {
                self.load().keys_raw().to_vec()
            }

            /// Sorted prefix scan over the current file, raw bytes.
            ///
            /// For a lazy scan, take a snapshot: `db.load().prefix_iter(..)`.
            /// The iterator borrows its reader, so it needs a snapshot you own.
            pub fn prefix_raw(
                &self,
                prefix: impl AsRef<[u8]>,
            ) -> FuseResult<Vec<(String, Vec<u8>)>> {
                self.load().prefix_raw(prefix)
            }

            /// Every `(key, raw_bytes)` pair in the current file, in sorted key order.
            pub fn items_raw(&self) -> FuseResult<Vec<(String, Vec<u8>)>> {
                self.load().items_raw()
            }

            /// Unique objects in the current file, deduplicated by offset.
            pub fn objects_raw(&self) -> FuseResult<Vec<Vec<u8>>> {
                self.load().objects_raw()
            }

            /// Metadata for the current file.
            pub fn stats(&self) -> Stats {
                self.load().stats()
            }

            /// Deep CRC32 integrity check of the current file.
            pub fn verify(&self) -> FuseResult<()> {
                self.load().verify()
            }

            /// Non-throwing form of [`verify`](Self::verify).
            pub fn is_valid(&self) -> bool {
                self.load().is_valid()
            }

            /// O(1) exact lookup against the current file, decoded into `T`.
            #[cfg(feature = "msgpack")]
            pub fn get<T: DeserializeOwned>(&self, key: impl AsRef<[u8]>) -> FuseResult<Option<T>> {
                self.load().get(key)
            }

            /// Sorted prefix scan over the current file, values decoded into `T`.
            #[cfg(feature = "msgpack")]
            pub fn prefix<T: DeserializeOwned>(
                &self,
                prefix: impl AsRef<[u8]>,
            ) -> FuseResult<Vec<(String, T)>> {
                self.load().prefix(prefix)
            }

            /// Every `(key, value)` pair in the current file, decoded into `T`.
            #[cfg(feature = "msgpack")]
            pub fn items<T: DeserializeOwned>(&self) -> FuseResult<Vec<(String, T)>> {
                self.load().items()
            }

            /// Unique objects in the current file, decoded into `T`.
            #[cfg(feature = "msgpack")]
            pub fn objects<T: DeserializeOwned>(&self) -> FuseResult<Vec<T>> {
                self.load().objects()
            }
        }
    };
}

// ── change detection ──────────────────────────────────────────────────────────

/// What we compare to decide whether the file on disk is the one we mapped.
///
/// Modification time alone is not enough: some filesystems report whole-second
/// granularity, so two rebuilds within the same second look identical. Pairing
/// it with the length catches the overwhelming majority of those.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct FileStamp {
    modified: Option<SystemTime>,
    len: u64,
}

impl FileStamp {
    /// `Ok(None)` when the file is absent — a transient state during the
    /// writer's rename, not an error worth surfacing.
    fn read(path: &Path) -> FuseResult<Option<Self>> {
        match std::fs::metadata(path) {
            Ok(meta) => Ok(Some(Self {
                modified: meta.modified().ok(),
                len: meta.len(),
            })),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(FuseError::Io(format!("stat {}: {e}", path.display()))),
        }
    }
}

// ── ReloadableReader ──────────────────────────────────────────────────────────

/// A [`FuseReader`] that can be swapped for a newer build of the same file.
///
/// Read from it exactly like a `FuseReader` — every method resolves the current
/// reader for you, so there is no way to keep reading a file that has already
/// been replaced.
///
/// # Example
/// ```no_run
/// use fusedb::ReloadableReader;
///
/// let db = ReloadableReader::open("live.fsdb")?;
///
/// // Reads always hit the newest successful swap.
/// println!("{:?}", db.get_raw("8.8.8.8")?);
/// println!("{} keys", db.len());
///
/// if db.reload_if_changed()? {
///     println!("swapped to a newer build");
/// }
/// # Ok::<(), fusedb::FuseError>(())
/// ```
///
/// # Snapshots
///
/// Each call above resolves the reader once — cheap, but not free. For a batch
/// of lookups, resolve once with [`load`](Self::load) and reuse the result:
///
/// ```no_run
/// # use fusedb::ReloadableReader;
/// # let db = ReloadableReader::open("live.fsdb")?;
/// let snapshot = db.load();
/// for key in ["8.8.8.8", "8.8.4.4", "1.1.1.1"] {
///     println!("{:?}", snapshot.get_raw(key)?);
/// }
/// # Ok::<(), fusedb::FuseError>(())
/// ```
///
/// A snapshot is also a *stable* view: it keeps its own memory mapping, so a
/// swap partway through a multi-step read cannot make the results inconsistent.
/// The flip side is that a snapshot stored long-term goes stale and pins the old
/// mapping in memory — take one per request or per batch, not per process.
///
/// # Concurrency
///
/// `load()` takes a read lock and clones an `Arc`. The replacement reader is
/// built entirely outside that lock, so a reload never blocks readers for longer
/// than the pointer swap itself.
///
/// This is the Rust counterpart to Python's `ReloadableFuseReader`.
pub struct ReloadableReader {
    path: PathBuf,
    verify: bool,
    current: RwLock<Arc<FuseReader>>,
    /// Guards the reload operation, so two threads never build the same
    /// replacement twice. Held across file I/O — readers do not touch it.
    stamp: Mutex<Option<FileStamp>>,
    generation: AtomicU64,
}

impl ReloadableReader {
    /// Open `path`, validating its CRC32 now and on every reload.
    pub fn open(path: impl AsRef<Path>) -> FuseResult<Self> {
        Self::with_verify(path, true)
    }

    /// Open without CRC32 validation, now or on reload.
    ///
    /// Skips a full pass over the file on every swap, which matters for large
    /// databases reloaded often. Only for files you trust.
    pub fn open_unverified(path: impl AsRef<Path>) -> FuseResult<Self> {
        Self::with_verify(path, false)
    }

    fn with_verify(path: impl AsRef<Path>, verify: bool) -> FuseResult<Self> {
        let path = path.as_ref().to_path_buf();
        let reader = Self::open_one(&path, verify)?;
        let stamp = FileStamp::read(&path)?;

        Ok(Self {
            path,
            verify,
            current: RwLock::new(Arc::new(reader)),
            stamp: Mutex::new(stamp),
            generation: AtomicU64::new(0),
        })
    }

    fn open_one(path: &Path, verify: bool) -> FuseResult<FuseReader> {
        if verify {
            FuseReader::open(path)
        } else {
            FuseReader::open_unverified(path)
        }
    }

    /// Take a snapshot of the current reader.
    ///
    /// Only needed to optimise a batch of reads or to pin a stable view — the
    /// read methods on this type resolve the reader themselves. Cheap: one lock
    /// acquisition and an `Arc` clone.
    ///
    /// A reload that happens while you hold a snapshot leaves it intact; the
    /// next `load()` returns the new reader. Do not cache a snapshot for the
    /// lifetime of your program — it will never see a swap, and it keeps the
    /// old file's mapping alive.
    pub fn load(&self) -> Arc<FuseReader> {
        Arc::clone(&read_ignoring_poison(&self.current))
    }

    /// Swap in a fresh reader if the file changed since the last successful load.
    ///
    /// Returns `Ok(true)` when a swap happened. Detection compares modification
    /// time and length; a file that vanished (a writer mid-rename) reports
    /// `Ok(false)` rather than an error.
    ///
    /// The new reader is built before the old one is released, so a failed
    /// reload leaves the previous snapshot serving reads untouched.
    ///
    /// # Errors
    /// Whatever [`FuseReader::open`] would return for the new file — a CRC
    /// failure on a half-written file, for instance.
    pub fn reload_if_changed(&self) -> FuseResult<bool> {
        let mut last = lock_ignoring_poison(&self.stamp);

        let Some(current) = FileStamp::read(&self.path)? else {
            return Ok(false); // file is momentarily absent
        };
        if *last == Some(current) {
            return Ok(false);
        }

        let fresh = Arc::new(Self::open_one(&self.path, self.verify)?);
        *write_ignoring_poison(&self.current) = fresh;
        *last = Some(current);
        self.generation.fetch_add(1, Ordering::Release);
        Ok(true)
    }

    /// Reload unconditionally, ignoring the change stamp.
    ///
    /// Use this when the file may have been rewritten with identical length
    /// inside one filesystem timestamp tick — the one case
    /// [`reload_if_changed`](Self::reload_if_changed) can miss.
    pub fn reload(&self) -> FuseResult<()> {
        let mut last = lock_ignoring_poison(&self.stamp);

        let fresh = Arc::new(Self::open_one(&self.path, self.verify)?);
        *write_ignoring_poison(&self.current) = fresh;
        *last = FileStamp::read(&self.path)?;
        self.generation.fetch_add(1, Ordering::Release);
        Ok(())
    }

    /// How many successful swaps have happened since this reader was opened.
    ///
    /// Useful as a cache-invalidation token: keep the value alongside anything
    /// you derived from a snapshot, and recompute when it moves.
    #[inline]
    pub fn generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }

    /// The file being watched.
    #[inline]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Whether reloads validate the CRC32.
    #[inline]
    pub fn verifies(&self) -> bool {
        self.verify
    }
}

impl_reader_proxy!(ReloadableReader);

impl std::fmt::Debug for ReloadableReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReloadableReader")
            .field("path", &self.path)
            .field("generation", &self.generation())
            .field("keys", &self.load().len())
            .finish()
    }
}

// ── shutdown signalling ───────────────────────────────────────────────────────

/// An interruptible sleep. `park_timeout` can wake spuriously and a plain
/// `thread::sleep` would make shutdown take up to one full interval; a condvar
/// gives us both an exact timeout and an immediate wake on stop.
#[derive(Default)]
struct Shutdown {
    stopped: Mutex<bool>,
    signal: Condvar,
}

impl Shutdown {
    /// Sleep up to `timeout`. Returns `true` if a stop was requested.
    fn sleep(&self, timeout: Duration) -> bool {
        let stopped = lock_ignoring_poison(&self.stopped);
        let (stopped, _) = self
            .signal
            .wait_timeout_while(stopped, timeout, |stopped| !*stopped)
            .unwrap_or_else(|e| e.into_inner());
        *stopped
    }

    fn stop(&self) {
        *lock_ignoring_poison(&self.stopped) = true;
        self.signal.notify_all();
    }
}

// ── FuseWatcher ───────────────────────────────────────────────────────────────

type ReloadHook = Box<dyn Fn(&Arc<FuseReader>) + Send + Sync + 'static>;
type ErrorHook = Box<dyn Fn(&FuseError) + Send + Sync + 'static>;

/// Polls a `.fsdb` file on a background thread and hot-swaps readers when it changes.
///
/// The Rust counterpart to Python's `FuseWatcher`, with two deliberate
/// differences: the watcher never writes to stdout — failures go to
/// [`on_error`](WatcherBuilder::on_error) — and the polling thread is stopped
/// and joined when the handle drops, so it cannot outlive its owner.
///
/// # Example
/// ```no_run
/// use std::time::Duration;
/// use fusedb::FuseWatcher;
///
/// let watcher = FuseWatcher::builder("live.fsdb")
///     .interval(Duration::from_secs(30))
///     .on_reload(|db| eprintln!("reloaded: {} keys", db.len()))
///     .on_error(|e| eprintln!("reload failed, still serving the old file: {e}"))
///     .spawn()?;
///
/// // Read it like a FuseReader — no swap bookkeeping, no stale handles.
/// println!("{:?}", watcher.get_raw("8.8.8.8")?);
/// println!("{}", watcher.exists("1.1.1.1"));
///
/// // For a batch, resolve the reader once and reuse it.
/// let snapshot = watcher.load();
/// for key in ["8.8.8.8", "8.8.4.4"] {
///     println!("{:?}", snapshot.get_raw(key)?);
/// }
///
/// // Dropping the watcher stops the thread; stop() does it explicitly.
/// watcher.stop();
/// # Ok::<(), fusedb::FuseError>(())
/// ```
///
/// Sharing across threads goes through [`reader`](Self::reader), which hands
/// back an `Arc<ReloadableReader>` carrying the same read surface.
///
/// # Polling, not filesystem events
///
/// Polling needs no platform-specific dependency and is a good fit for files
/// that get rebuilt on a schedule. If you need sub-second reaction to writes,
/// drive [`ReloadableReader::reload_if_changed`] from your own `notify` watcher
/// instead — the swap machinery is the same.
pub struct FuseWatcher {
    reader: Arc<ReloadableReader>,
    shutdown: Arc<Shutdown>,
    /// `None` only after `stop()` has taken it to join.
    handle: Option<JoinHandle<()>>,
}

impl FuseWatcher {
    /// Start configuring a watcher for `path`.
    pub fn builder(path: impl AsRef<Path>) -> WatcherBuilder {
        WatcherBuilder::new(path)
    }

    /// Watch `path`, polling every `interval`, with no callbacks.
    pub fn spawn(path: impl AsRef<Path>, interval: Duration) -> FuseResult<Self> {
        Self::builder(path).interval(interval).spawn()
    }

    /// Take a snapshot of the current reader — see [`ReloadableReader::load`].
    ///
    /// Only needed to optimise a batch of reads; the read methods on this type
    /// resolve the reader themselves.
    #[inline]
    pub fn load(&self) -> Arc<FuseReader> {
        self.reader.load()
    }

    /// The underlying swap handle, for sharing across threads or reloading by hand.
    #[inline]
    pub fn reader(&self) -> &Arc<ReloadableReader> {
        &self.reader
    }

    /// Check for a new build right now instead of waiting for the next poll.
    ///
    /// Does not run the `on_reload` callback — you are already on the calling
    /// thread and can react directly.
    pub fn reload_now(&self) -> FuseResult<bool> {
        self.reader.reload_if_changed()
    }

    /// The file being watched.
    #[inline]
    pub fn path(&self) -> &Path {
        self.reader.path()
    }

    /// Successful swaps so far — see [`ReloadableReader::generation`].
    #[inline]
    pub fn generation(&self) -> u64 {
        self.reader.generation()
    }

    /// Stop the polling thread and wait for it to finish.
    ///
    /// Dropping the watcher does the same thing; call this when you want the
    /// join to happen at a point you chose.
    pub fn stop(mut self) {
        self.shutdown_and_join();
    }

    fn shutdown_and_join(&mut self) {
        self.shutdown.stop();
        if let Some(handle) = self.handle.take() {
            // A panic inside a user callback already unwound the polling
            // thread; there is nothing left to clean up and nothing useful to
            // report from a Drop impl, so the join result is dropped.
            let _ = handle.join();
        }
    }
}

impl_reader_proxy!(FuseWatcher);

impl Drop for FuseWatcher {
    fn drop(&mut self) {
        self.shutdown_and_join();
    }
}

impl std::fmt::Debug for FuseWatcher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FuseWatcher")
            .field("path", &self.reader.path())
            .field("generation", &self.generation())
            .field("running", &self.handle.is_some())
            .finish()
    }
}

// ── builder ───────────────────────────────────────────────────────────────────

/// Configures a [`FuseWatcher`]. Created by [`FuseWatcher::builder`].
pub struct WatcherBuilder {
    path: PathBuf,
    interval: Duration,
    verify: bool,
    on_reload: Option<ReloadHook>,
    on_error: Option<ErrorHook>,
}

impl WatcherBuilder {
    /// Default: poll every 30 seconds, verify CRC32 on every load.
    fn new(path: impl AsRef<Path>) -> Self {
        Self {
            path: path.as_ref().to_path_buf(),
            interval: Duration::from_secs(30),
            verify: true,
            on_reload: None,
            on_error: None,
        }
    }

    /// How often to check the file. Must be non-zero.
    pub fn interval(mut self, interval: Duration) -> Self {
        self.interval = interval;
        self
    }

    /// Whether each load validates the whole-file CRC32. Defaults to `true`.
    ///
    /// Leaving it on is what stops a half-written file from being swapped in:
    /// the reload fails, the error reaches `on_error`, and the previous
    /// snapshot keeps serving.
    pub fn verify(mut self, verify: bool) -> Self {
        self.verify = verify;
        self
    }

    /// Called on the polling thread after each successful swap.
    ///
    /// Keep it short — the next poll waits for it to return. A panic here kills
    /// the polling thread; readers keep working against the last good snapshot.
    pub fn on_reload(mut self, hook: impl Fn(&Arc<FuseReader>) + Send + Sync + 'static) -> Self {
        self.on_reload = Some(Box::new(hook));
        self
    }

    /// Called when a reload attempt fails. Polling continues either way.
    ///
    /// Without a handler, failures are silently ignored — a library has no
    /// business writing to stdout, and a transient failure mid-rebuild is
    /// expected rather than exceptional.
    pub fn on_error(mut self, hook: impl Fn(&FuseError) + Send + Sync + 'static) -> Self {
        self.on_error = Some(Box::new(hook));
        self
    }

    /// Open the file and start the polling thread.
    ///
    /// # Errors
    /// [`FuseError::InvalidArg`] for a zero interval, or whatever
    /// [`FuseReader::open`] returns for the initial load — a watcher is never
    /// handed back in a state where reads would fail.
    pub fn spawn(self) -> FuseResult<FuseWatcher> {
        if self.interval.is_zero() {
            return Err(FuseError::InvalidArg(
                "watcher interval must be greater than zero".into(),
            ));
        }

        let reader = Arc::new(if self.verify {
            ReloadableReader::open(&self.path)?
        } else {
            ReloadableReader::open_unverified(&self.path)?
        });

        let shutdown = Arc::new(Shutdown::default());
        let interval = self.interval;
        let on_reload = self.on_reload;
        let on_error = self.on_error;

        let handle = {
            let reader = Arc::clone(&reader);
            let shutdown = Arc::clone(&shutdown);

            thread::Builder::new()
                .name("fusedb-watch".into())
                .spawn(move || {
                    while !shutdown.sleep(interval) {
                        match reader.reload_if_changed() {
                            Ok(true) => {
                                if let Some(hook) = &on_reload {
                                    hook(&reader.load());
                                }
                            }
                            Ok(false) => {}
                            Err(e) => {
                                if let Some(hook) = &on_error {
                                    hook(&e);
                                }
                            }
                        }
                    }
                })
                .map_err(|e| FuseError::Io(format!("spawning watcher thread: {e}")))?
        };

        Ok(FuseWatcher {
            reader,
            shutdown,
            handle: Some(handle),
        })
    }
}

impl std::fmt::Debug for WatcherBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WatcherBuilder")
            .field("path", &self.path)
            .field("interval", &self.interval)
            .field("verify", &self.verify)
            .field("on_reload", &self.on_reload.is_some())
            .field("on_error", &self.on_error.is_some())
            .finish()
    }
}
