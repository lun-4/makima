//! Same-process serialization of mutable-path tool mutations.
//!
//! `FileWriteLocks` is a keyed write-lock registry shared by a root agent
//! context and everything cloned from it (batch siblings, subagents). It
//! serializes read-modify-write critical sections for registered tools that
//! declare a `mutable_path`, so two concurrent calls mutating the same file
//! run in non-overlapping order instead of losing updates. Different keys
//! stay fully concurrent.
//!
//! The registry is process-local: it does not coordinate separate maki
//! processes, shell commands, editors, or anything outside a dispatched
//! tool invocation. Keys are normalized display paths used only for
//! synchronization identity; they are never passed to filesystem APIs.
//!
//! Reentrancy is detected, not supported: when the owner chain of a
//! dispatch already holds the requested key (a recursive same-path call
//! from inside a locked mutable tool), acquisition fails immediately with
//! `SAME_PATH_MUTATION_IN_PROGRESS` instead of waiting on its own lock.

use std::collections::HashMap;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use maki_storage::paths::incremental_canonicalize;

use super::{DEADLINE_EXCEEDED, Deadline, resolve_path_from};
use crate::cancel::CancelToken;

pub const SAME_PATH_MUTATION_IN_PROGRESS: &str = "same-path mutation is already in progress";
pub(crate) const CANCELLED: &str = "cancelled";

/// How often `entry` sweeps idle entries. Quiescent keys (no waiter or
/// guard holds their `Arc`) are dropped so a long session touching many
/// generated paths does not grow the registry monotonically; entries that
/// are held are never removed, so there is no remove-after-unlock race.
const RETAIN_INTERVAL: usize = 128;

/// A keyed write-lock registry. Entries are retained for the lifetime of
/// the registry (`Arc<FileWriteLocks>` lives as long as any cloned tool
/// context can), so one key always maps to one gate with no
/// remove-after-unlock race. Memory cost is one small entry per path
/// touched during a root context.
#[derive(Clone, Default)]
pub struct FileWriteLocks {
    inner: Arc<Inner>,
}

#[derive(Default)]
struct Inner {
    entries: Mutex<HashMap<PathBuf, Arc<KeyEntry>>>,
    next_owner: AtomicU64,
}

struct KeyEntry {
    gate: Arc<async_lock::Mutex<()>>,
    holder: Mutex<Option<u64>>,
}

impl FileWriteLocks {
    pub fn new() -> Self {
        Self::default()
    }

    /// Expansion and canonicalization shared by every lock key: `~` and
    /// relative paths resolve against the same cwd/home semantics as agent
    /// paths, existing components are walked through symlinks, and a
    /// missing tail falls back to lexical normalization. The result is only
    /// a synchronization key and is never used for filesystem access.
    pub(crate) fn lock_key(path: &str, cwd: &Path) -> Result<PathBuf, String> {
        let resolved = resolve_path_from(path, cwd)?;
        let canonical =
            incremental_canonicalize(std::path::Path::new(&resolved)).unwrap_or_else(|| {
                maki_storage::paths::canonicalize_clean(std::path::Path::new(&resolved))
            });
        Ok(canonical)
    }

    fn entry(&self, key: PathBuf) -> Arc<KeyEntry> {
        let mut entries = self.inner.entries.lock().expect("lock registry poisoned");
        if entries.len().is_multiple_of(RETAIN_INTERVAL) {
            // A count of 1 means only the map holds the entry: no waiter or
            // guard can hold it, so dropping it is safe under the same mutex
            // that `entry` uses for insertion. The next acquire recreates it.
            entries.retain(|_, entry| Arc::strong_count(entry) > 1);
        }
        entries
            .entry(key)
            .or_insert_with(|| {
                Arc::new(KeyEntry {
                    gate: Arc::new(async_lock::Mutex::new(())),
                    holder: Mutex::new(None),
                })
            })
            .clone()
    }

    /// Acquire the gate for `key`, waiting like an independent owner unless
    /// an ancestor owner of this dispatch already holds it (reentry) or the
    /// wait is cut short by cancellation or an expired deadline. The race
    /// order is deterministic: cancellation wins over an expired deadline,
    /// which wins over acquisition.
    pub(crate) async fn acquire(
        &self,
        key: PathBuf,
        ancestors: &[u64],
        cancel: &CancelToken,
        deadline: Deadline,
    ) -> Result<WriteLockGuard, String> {
        if cancel.is_cancelled() {
            return Err(CANCELLED.into());
        }
        let entry = self.entry(key.clone());
        let holder = *entry.holder.lock().expect("lock holder poisoned");
        if holder.is_some_and(|h| ancestors.contains(&h)) {
            return Err(SAME_PATH_MUTATION_IN_PROGRESS.into());
        }
        deadline.check()?;
        let owner = self.inner.next_owner.fetch_add(1, Ordering::Relaxed);
        let gate = wait_for_gate(&entry, cancel, deadline).await?;
        *entry.holder.lock().expect("lock holder poisoned") = Some(owner);
        Ok(WriteLockGuard { entry, owner, gate })
    }
}

/// Cancellation-safe racer over the keyed gate. Once acquired, the guard
/// drops whenever the caller's future returns; this layer never interrupts
/// a permanently hung holder.
async fn wait_for_gate(
    entry: &KeyEntry,
    cancel: &CancelToken,
    deadline: Deadline,
) -> Result<async_lock::MutexGuardArc<()>, String> {
    enum WaitOutcome {
        Gate(async_lock::MutexGuardArc<()>),
        Cancelled,
        TimedOut,
    }
    let mut lock = Box::pin(entry.gate.lock_arc());
    let mut cancel_fut = Box::pin(cancel.cancelled());
    let mut timer: Option<Pin<Box<dyn Future<Output = ()> + Send>>> = match deadline {
        Deadline::None => None,
        Deadline::At(instant) => Some(Box::pin(async move {
            smol::Timer::after(instant.saturating_duration_since(Instant::now())).await;
        })),
    };
    let race = futures_lite::future::or(
        futures_lite::future::or(async { WaitOutcome::Gate(lock.as_mut().await) }, async {
            cancel_fut.as_mut().await;
            WaitOutcome::Cancelled
        }),
        async {
            match timer.as_mut() {
                Some(t) => t.await,
                None => futures_lite::future::pending::<()>().await,
            }
            WaitOutcome::TimedOut
        },
    );
    let outcome = race.await;
    // Tie-break after the race: cancellation beats an expired deadline, and
    // an expired deadline beats holding a freshly granted gate.
    if cancel.is_cancelled() {
        return Err(CANCELLED.into());
    }
    if deadline.check().is_err() {
        return Err(DEADLINE_EXCEEDED.into());
    }
    match outcome {
        WaitOutcome::Gate(gate) => Ok(gate),
        WaitOutcome::Cancelled => Err(CANCELLED.into()),
        WaitOutcome::TimedOut => Err(DEADLINE_EXCEEDED.into()),
    }
}

/// Held keyed gate; releases the holder slot and the gate on drop.
pub(crate) struct WriteLockGuard {
    entry: Arc<KeyEntry>,
    owner: u64,
    /// Held for RAII only: the gate must stay locked while the mutation
    /// critical section runs, and the drop order (hook clears the holder
    /// first, then this releases the gate) is load-bearing for waiters.
    #[expect(dead_code)]
    gate: async_lock::MutexGuardArc<()>,
}

impl WriteLockGuard {
    /// The owner token this dispatch acquired the gate with, appended to
    /// the handler context's chain for recursive reentry detection.
    pub(crate) fn owner(&self) -> u64 {
        self.owner
    }
}

impl Drop for WriteLockGuard {
    fn drop(&mut self) {
        let mut holder = self.entry.holder.lock().expect("lock holder poisoned");
        if *holder == Some(self.owner) {
            *holder = None;
        }
    }
}
#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;

    fn key(path: &str) -> PathBuf {
        FileWriteLocks::lock_key(path, &std::env::current_dir().unwrap()).expect("key resolves")
    }

    #[test]
    fn lock_key_normalizes_lexical_aliases() {
        let cwd = std::env::current_dir().unwrap();
        let base = cwd.join("a").join("b");
        let aliases = [
            cwd.join("a").join(".").join("b"),
            cwd.join("a").join("x").join("..").join("b"),
            cwd.join("a").join("c").join("..").join("b"),
        ];
        for alias in aliases {
            assert_eq!(
                key(&base.to_string_lossy()),
                key(&alias.to_string_lossy()),
                "alias {alias:?} must match {base:?}"
            );
        }
    }

    #[test]
    fn lock_key_requires_resolvable_cwd_for_relative_paths() {
        // A relative path resolves against the cwd; the key must be absolute.
        let k = key("some_relative_target");
        assert!(k.is_absolute());
    }

    #[test]
    fn idle_entries_are_reclaimed_while_held_ones_survive() {
        smol::block_on(async {
            let locks = Arc::new(FileWriteLocks::new());
            let (_trigger, cancel) = CancelToken::new();
            let held = PathBuf::from("/reclaim/held");
            let guard = locks
                .acquire(held.clone(), &[], &cancel, Deadline::None)
                .await
                .unwrap();
            for i in 0..RETAIN_INTERVAL + 8 {
                let k = PathBuf::from(format!("/reclaim/idle/{i}"));
                let _ = locks
                    .acquire(k, &[], &cancel, Deadline::None)
                    .await
                    .unwrap();
            }
            drop(guard);
            // The next insert crosses the retain boundary and sweeps the
            // quiescent entries; the held entry must survive.
            let k = PathBuf::from("/reclaim/new");
            let _ = locks
                .acquire(k, &[], &cancel, Deadline::None)
                .await
                .unwrap();
            let entries = locks.inner.entries.lock().expect("lock registry poisoned");
            assert!(
                entries.len() < RETAIN_INTERVAL,
                "idle entries must be reclaimed, got {} retained",
                entries.len()
            );
            assert!(
                entries.contains_key(&held),
                "the held entry must survive reclamation"
            );
        });
    }

    #[test]
    fn acquire_and_release_roundtrip() {
        smol::block_on(async {
            let locks = Arc::new(FileWriteLocks::new());
            let (_trigger, cancel) = CancelToken::new();
            let k = PathBuf::from("/roundtrip");
            let guard = locks
                .acquire(k.clone(), &[], &cancel, Deadline::None)
                .await
                .unwrap();
            // A second independent owner queues: it cannot acquire while held.
            let locks2 = Arc::clone(&locks);
            let cancel2 = cancel.clone();
            let second = smol::spawn(async move {
                locks2
                    .acquire(k.clone(), &[], &cancel2, Deadline::None)
                    .await
                    .unwrap()
            });
            for _ in 0..5 {
                smol::future::yield_now().await;
            }
            drop(guard);
            let _ = second.await;
        });
    }
}
