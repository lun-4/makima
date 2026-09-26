//! Coalescing write-behind cache with incremental JSONL persistence.
//!
//! Apps post session snapshots keyed by session id; the writer thread drains
//! the newest snapshot of every session per wake and performs O(delta)
//! appends. Deletes travel through the same per-session slot as saves, so
//! whichever the app asked for last is what reaches disk.

use std::collections::{HashMap, HashSet};
use std::io;
use std::mem;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use maki_storage::checkpoint::{
    CheckpointAck, CheckpointError, CheckpointFuture, CheckpointRequest, CheckpointVersion,
    CheckpointWriter,
};
use maki_storage::id::MakiId;
use maki_storage::sessions::{HistoryIdentity, SESSIONS_DIR, SessionError, SessionLog};
use maki_storage::{StateDir, StorageError};
use tracing::warn;

use maki_agent::ThinkingConfig;
use maki_agent::session_coordinator::{CHECKPOINT_REBASE_EPOCH, SessionCheckpoint};
use maki_agent::session_options::{
    ENABLED_VALUE, FAST_OPTION_ID, SessionOptionOwner, THINKING_OPTION_ID, WORKFLOW_OPTION_ID,
    YOLO_OPTION_ID,
};

use crate::AppSession;

const SAVE_FAILED_PREFIX: &str = "Session save failed";
const SAVE_RECOVERED: &str = "Session save recovered";
#[cfg(not(test))]
const CHECKPOINT_RETRY_BACKOFFS: &[Duration] = &[
    Duration::from_millis(100),
    Duration::from_millis(500),
    Duration::from_secs(2),
];
#[cfg(test)]
const CHECKPOINT_RETRY_BACKOFFS: &[Duration] =
    &[Duration::from_millis(600), Duration::from_millis(1200)];
#[cfg(not(test))]
const BACKGROUND_RETRY_BACKOFFS: &[Duration] = &[
    Duration::from_secs(1),
    Duration::from_secs(5),
    Duration::from_secs(30),
];
#[cfg(test)]
const BACKGROUND_RETRY_BACKOFFS: &[Duration] =
    &[Duration::from_millis(20), Duration::from_millis(50)];

type Pending = Arc<Mutex<PendingState>>;

#[derive(Default)]
struct PendingState {
    entries: HashMap<MakiId, Entry>,
    /// Latest authoritative state from the app or a successful coordinator
    /// checkpoint. It may still be newer than durable storage.
    latest: HashMap<MakiId, Arc<AppSession>>,
    latest_generations: HashMap<MakiId, u64>,
    coordinator_pending: HashMap<MakiId, CoordinatorPending>,
    in_flight: HashMap<MakiId, InFlightSave>,
    coordinator_history_bases: HashMap<MakiId, HistoryIdentity>,
    fail_closed: HashMap<MakiId, Arc<AppSession>>,
    next_generation: u64,
}

struct CoordinatorPending {
    generation: u64,
    session: Arc<AppSession>,
    history_base: Option<HistoryIdentity>,
}

struct InFlightSave {
    generation: u64,
    session: Arc<AppSession>,
    history_base: Option<HistoryIdentity>,
}

type DeleteCallback = Box<dyn FnOnce(Result<(), SessionError>) + Send>;

/// One slot per session, holding whatever the app asked for last. Deletes
/// used to ride a side channel, where a flush queued before a delete could
/// drain a save enqueued after it, so the delete unlinked a session the app
/// had just saved.
enum Entry {
    Save(PendingSave),
    Delete(DeleteCallback),
}

struct PendingSave {
    session: Arc<AppSession>,
    waiters: Vec<CheckpointWaiter>,
    generation: u64,
    coordinator_generation: Option<u64>,
    coordinator_history_base: Option<HistoryIdentity>,
    retry_attempt: usize,
    retry_at: Option<Instant>,
}

struct CheckpointWaiter {
    version: CheckpointVersion,
    reply: flume::Sender<Result<CheckpointAck, CheckpointError>>,
}

pub struct StorageWriter {
    pending: Pending,
    wake: flume::Sender<()>,
    done_rx: flume::Receiver<()>,
    /// Asks the writer thread to finish. Closing the wake channel is not
    /// enough: every coordinator's checkpoint writer holds a clone of the
    /// sender, and those live in tasks that exit on their own schedule, so
    /// waiting for the last one to drop makes exit take as long as the
    /// timeout allows.
    stop: Arc<AtomicBool>,
}

#[derive(Clone)]
struct CoordinatorCheckpointWriter {
    pending: Pending,
    wake: flume::Sender<()>,
}

impl CheckpointWriter<AppSession> for StorageWriter {
    fn checkpoint(&self, request: CheckpointRequest<AppSession>) -> CheckpointFuture {
        self.enqueue_checkpoint(request)
    }
}

impl CheckpointWriter<SessionCheckpoint> for CoordinatorCheckpointWriter {
    fn checkpoint(&self, request: CheckpointRequest<SessionCheckpoint>) -> CheckpointFuture {
        let session_id = request.session_id;
        let version = request.version;
        let mut state = lock(&self.pending);
        let rebase = version.epoch == CHECKPOINT_REBASE_EPOCH;
        let Some(latest) = state.latest.get(&session_id) else {
            return Box::pin(async move {
                Err(CheckpointError::Save {
                    session_id,
                    message: Arc::from("session snapshot is unavailable"),
                })
            });
        };
        let (base, mut history_base) = if rebase {
            (
                Arc::clone(latest),
                state.coordinator_history_bases.get(&session_id).copied(),
            )
        } else if let Some(pending) = state.coordinator_pending.get(&session_id) {
            (Arc::clone(&pending.session), pending.history_base)
        } else {
            (
                Arc::clone(latest),
                state.coordinator_history_bases.get(&session_id).copied(),
            )
        };
        let mut merged = merge_checkpoint(&base, &request.snapshot);
        if rebase {
            let valid_base = |history_base: Option<HistoryIdentity>| {
                history_base.is_some_and(|history_base| {
                    base.history_identity() == history_base
                        || state.coordinator_history_bases.get(&session_id) == Some(&history_base)
                })
            };
            let history_candidate = state
                .coordinator_pending
                .get(&session_id)
                .filter(|pending| valid_base(pending.history_base))
                .map(|pending| (pending.generation, &pending.session, pending.history_base))
                .into_iter()
                .chain(
                    state
                        .in_flight
                        .get(&session_id)
                        .filter(|save| valid_base(save.history_base))
                        .map(|save| (save.generation, &save.session, save.history_base)),
                )
                .max_by_key(|(generation, _, _)| *generation);
            if let Some((_, history, candidate_base)) = history_candidate {
                merged.adopt_history(&history.history_snapshot());
                history_base = candidate_base;
            }
        }
        if history_base.is_none() && request.snapshot.history.is_some() {
            history_base = Some(base.history_identity());
        }
        if let Some(authoritative) = state.fail_closed.get(&session_id) {
            merged = merge_tui_snapshot(&merged, authoritative, false);
        }
        if state.fail_closed.contains_key(&session_id) {
            history_base = None;
        }
        let merged = Arc::new(merged);
        let generation = next_generation(&mut state);
        state.latest_generations.insert(session_id, generation);
        if rebase {
            state.latest.insert(session_id, Arc::clone(&merged));
            state.coordinator_pending.remove(&session_id);
        }
        state.coordinator_pending.insert(
            session_id,
            CoordinatorPending {
                generation,
                session: Arc::clone(&merged),
                history_base,
            },
        );
        let (reply, response) = flume::bounded(1);
        enqueue_locked(
            &mut state.entries,
            PendingSave {
                session: merged,
                waiters: vec![CheckpointWaiter { version, reply }],
                generation,
                coordinator_generation: Some(generation),
                coordinator_history_base: history_base,
                retry_attempt: 0,
                retry_at: None,
            },
        );
        drop(state);
        wake_checkpoint(&self.pending, &self.wake, session_id);
        Box::pin(async move {
            response
                .recv_async()
                .await
                .map_err(|_| CheckpointError::Closed(session_id))?
        })
    }
}

impl StorageWriter {
    pub fn new(dir: StateDir, warn_tx: flume::Sender<String>) -> Self {
        let pending: Pending = Arc::default();
        let writer_pending = Arc::clone(&pending);
        let (wake, wake_rx) = flume::unbounded::<()>();
        let (done_tx, done_rx) = flume::bounded::<()>(1);
        let stop: Arc<AtomicBool> = Arc::default();
        let writer_stop = Arc::clone(&stop);

        std::thread::Builder::new()
            .name("storage-writer".into())
            .spawn(move || {
                let mut writer = Writer {
                    dir,
                    warn_tx,
                    logs: HashMap::new(),
                    failing: HashSet::new(),
                };
                let mut retry_at: Option<Instant> = None;
                loop {
                    let wake = if let Some(deadline) = retry_at {
                        wake_rx.recv_timeout(deadline.saturating_duration_since(Instant::now()))
                    } else {
                        wake_rx
                            .recv()
                            .map_err(|_| flume::RecvTimeoutError::Disconnected)
                    };
                    if matches!(wake, Err(flume::RecvTimeoutError::Disconnected)) {
                        break;
                    }
                    retry_at = writer.flush(&writer_pending);
                    if writer_stop.load(Ordering::Acquire) {
                        break;
                    }
                }
                for entry in lock(&writer_pending).entries.values_mut() {
                    if let Entry::Save(save) = entry {
                        save.retry_at = None;
                    }
                }
                writer.flush(&writer_pending);
                let _ = done_tx.send(());
            })
            .expect("failed to spawn storage writer thread");

        Self {
            pending,
            wake,
            done_rx,
            stop,
        }
    }

    pub fn seed(&self, session: Arc<AppSession>) {
        let id = session.id;
        let mut state = lock(&self.pending);
        let generation = next_generation(&mut state);
        let session = state
            .fail_closed
            .get(&id)
            .map(|guard| Arc::new(merge_tui_snapshot(&session, guard, false)))
            .unwrap_or(session);
        state.latest.insert(id, session);
        state.latest_generations.insert(id, generation);
    }

    pub fn send(&self, session: Arc<AppSession>) {
        let id = session.id;
        let mut state = lock(&self.pending);
        let preserve_history = state
            .coordinator_history_bases
            .get(&id)
            .is_some_and(|base| session.history_identity() == *base);
        if !preserve_history {
            state.coordinator_history_bases.remove(&id);
        }
        let incoming_history = session.history_identity();
        let mut authoritative = state
            .latest
            .get(&id)
            .map(|latest| merge_tui_snapshot(&session, latest, preserve_history))
            .unwrap_or_else(|| session.as_ref().clone());
        if let Some(guard) = state.fail_closed.get(&id) {
            authoritative = merge_tui_snapshot(&authoritative, guard, false);
        }
        let authoritative = Arc::new(authoritative);
        let generation = next_generation(&mut state);
        state.latest.insert(id, Arc::clone(&authoritative));
        state.latest_generations.insert(id, generation);
        let coordinator_generation = state
            .coordinator_pending
            .contains_key(&id)
            .then_some(generation);
        let guard = state.fail_closed.get(&id).cloned();
        let (session, coordinator_history_base) = state
            .coordinator_pending
            .get_mut(&id)
            .map(|pending| {
                let preserve_pending_history = pending
                    .history_base
                    .is_some_and(|base| incoming_history == base);
                let mut merged =
                    merge_tui_snapshot(&authoritative, &pending.session, preserve_pending_history);
                if let Some(guard) = guard.as_ref() {
                    merged = merge_tui_snapshot(&merged, guard, false);
                }
                let merged = Arc::new(merged);
                if !preserve_pending_history {
                    pending.history_base = None;
                }
                pending.generation = coordinator_generation.unwrap();
                pending.session = Arc::clone(&merged);
                (merged, pending.history_base)
            })
            .unwrap_or((authoritative, None));
        enqueue_locked(
            &mut state.entries,
            PendingSave {
                session,
                waiters: Vec::new(),
                generation,
                coordinator_generation,
                coordinator_history_base,
                retry_attempt: 0,
                retry_at: None,
            },
        );
        drop(state);
        wake_checkpoint(&self.pending, &self.wake, id);
    }

    pub fn fail_closed_checkpoint(
        &self,
        session: Arc<AppSession>,
        timeout: Duration,
    ) -> Result<CheckpointAck, CheckpointError> {
        let id = session.id;
        let version = CheckpointVersion {
            revision: session.revision(),
            epoch: CHECKPOINT_REBASE_EPOCH,
        };
        let (reply, response) = flume::bounded(1);
        let mut state = lock(&self.pending);
        let mut authoritative = session.as_ref().clone();
        let history_base = session.history_identity();
        let candidate = state
            .coordinator_pending
            .get(&id)
            .filter(|pending| pending.history_base == Some(history_base))
            .map(|pending| (pending.generation, &pending.session))
            .into_iter()
            .chain(
                state
                    .in_flight
                    .get(&id)
                    .filter(|save| save.history_base == Some(history_base))
                    .map(|save| (save.generation, &save.session)),
            )
            .max_by_key(|(generation, _)| *generation);
        if let Some((_, history)) = candidate {
            authoritative.adopt_history(&history.history_snapshot());
        } else if state.coordinator_history_bases.get(&id) == Some(&history_base)
            && let Some(latest) = state.latest.get(&id)
        {
            authoritative.adopt_history(&latest.history_snapshot());
        }
        let authoritative = state
            .fail_closed
            .get(&id)
            .map(|guard| Arc::new(merge_tui_snapshot(&authoritative, guard, false)))
            .unwrap_or_else(|| Arc::new(authoritative));
        state.fail_closed.insert(id, Arc::clone(&authoritative));
        state.coordinator_pending.remove(&id);
        state.coordinator_history_bases.remove(&id);
        let generation = next_generation(&mut state);
        state.latest.insert(id, Arc::clone(&authoritative));
        state.latest_generations.insert(id, generation);
        enqueue_locked(
            &mut state.entries,
            PendingSave {
                session: authoritative,
                waiters: vec![CheckpointWaiter { version, reply }],
                generation,
                coordinator_generation: None,
                coordinator_history_base: None,
                retry_attempt: 0,
                retry_at: None,
            },
        );
        if let Some(Entry::Save(save)) = state.entries.get_mut(&id) {
            save.retry_attempt = 0;
            save.retry_at = None;
        }
        drop(state);
        wake_checkpoint(&self.pending, &self.wake, id);
        response
            .recv_timeout(timeout)
            .map_err(|error| match error {
                flume::RecvTimeoutError::Timeout => CheckpointError::Save {
                    session_id: id,
                    message: Arc::from("authoritative session checkpoint timed out"),
                },
                flume::RecvTimeoutError::Disconnected => CheckpointError::Closed(id),
            })?
    }

    pub fn coordinator_checkpoint(&self) -> Arc<dyn CheckpointWriter<SessionCheckpoint>> {
        Arc::new(CoordinatorCheckpointWriter {
            pending: Arc::clone(&self.pending),
            wake: self.wake.clone(),
        })
    }

    #[cfg(test)]
    pub(crate) fn latest_snapshot(&self, id: MakiId) -> Option<Arc<AppSession>> {
        lock(&self.pending).latest.get(&id).cloned()
    }

    fn enqueue_checkpoint(&self, request: CheckpointRequest<AppSession>) -> CheckpointFuture {
        let session_id = request.session_id;
        let version = request.version;
        let (reply, response) = flume::bounded(1);
        let mut state = lock(&self.pending);
        let generation = next_generation(&mut state);
        let snapshot = state
            .fail_closed
            .get(&session_id)
            .map(|guard| Arc::new(merge_tui_snapshot(&request.snapshot, guard, false)))
            .unwrap_or(request.snapshot);
        state.latest.insert(session_id, Arc::clone(&snapshot));
        state.latest_generations.insert(session_id, generation);
        enqueue_locked(
            &mut state.entries,
            PendingSave {
                session: snapshot,
                waiters: vec![CheckpointWaiter { version, reply }],
                generation,
                coordinator_generation: None,
                coordinator_history_base: None,
                retry_attempt: 0,
                retry_at: None,
            },
        );
        drop(state);
        wake_checkpoint(&self.pending, &self.wake, session_id);
        Box::pin(async move {
            response
                .recv_async()
                .await
                .map_err(|_| CheckpointError::Closed(session_id))?
        })
    }

    /// Drops an in-memory snapshot kept alive by `delete_empty`. A session
    /// cleaned up as empty is never deleted through the picker, so without
    /// this its snapshot outlives it: `/new` on an untouched session would
    /// leave one behind every time.
    pub fn forget(&self, id: MakiId) {
        let mut state = lock(&self.pending);
        state.latest.remove(&id);
        state.latest_generations.remove(&id);
        state.coordinator_pending.remove(&id);
        state.in_flight.remove(&id);
        state.coordinator_history_bases.remove(&id);
        state.fail_closed.remove(&id);
    }

    /// Removes an empty session's files while keeping its in-memory snapshot.
    /// The session is still live in its tab, and its coordinator checkpoints by
    /// merging into that snapshot, so forgetting it would leave the next
    /// checkpoint with no base to merge into. The files come back if the
    /// coordinator later has something worth saving.
    pub fn delete_empty(&self, id: MakiId) {
        self.delete_inner(id, |_| {}, false);
    }

    /// Delete a session's files on the writer thread; `done` fires there, so
    /// callers never block on disk. Deleting a session that was never written
    /// reports success, and a save enqueued afterwards supersedes the delete.
    /// The session is forgotten entirely: this is the user asking for it to be
    /// gone, not the cleanup of a session that has not earned a file yet.
    pub fn delete(&self, id: MakiId, done: impl FnOnce(Result<(), SessionError>) + Send + 'static) {
        self.delete_inner(id, done, true);
    }

    fn delete_inner(
        &self,
        id: MakiId,
        done: impl FnOnce(Result<(), SessionError>) + Send + 'static,
        forget_snapshot: bool,
    ) {
        let mut state = lock(&self.pending);
        if forget_snapshot {
            state.latest.remove(&id);
            state.latest_generations.remove(&id);
            state.coordinator_pending.remove(&id);
            state.in_flight.remove(&id);
            state.coordinator_history_bases.remove(&id);
            state.fail_closed.remove(&id);
        }
        let replaced = state.entries.insert(id, Entry::Delete(Box::new(done)));
        drop(state);
        if let Some(Entry::Save(save)) = replaced {
            discard_coordinator_candidate(&self.pending, id, &save);
            fail_waiters(
                id,
                save.waiters,
                "checkpoint superseded by session deletion",
            );
        }
        if self.wake.send(()).is_err()
            && let Some(entry) = lock(&self.pending).entries.remove(&id)
        {
            match entry {
                Entry::Delete(done) => done(Err(writer_gone())),
                Entry::Save(save) => fail_waiters(id, save.waiters, "storage writer unavailable"),
            }
        }
    }

    pub fn shutdown(self, timeout: Duration) {
        self.stop.store(true, Ordering::Release);
        // Wake it so it observes the flag; the final flush still runs.
        let _ = self.wake.send(());
        drop(self.wake);
        if self.done_rx.recv_timeout(timeout).is_err() {
            warn!("storage writer did not drain within {timeout:?}");
        }
    }
}

fn lock(pending: &Pending) -> std::sync::MutexGuard<'_, PendingState> {
    pending.lock().unwrap_or_else(|e| e.into_inner())
}

fn next_generation(state: &mut PendingState) -> u64 {
    let generation = state.next_generation;
    state.next_generation = generation.wrapping_add(1);
    generation
}

fn enqueue_locked(entries: &mut HashMap<MakiId, Entry>, mut save: PendingSave) {
    let id = save.session.id;
    if let Some(Entry::Save(previous)) = entries.remove(&id) {
        if !previous.waiters.is_empty() {
            save.retry_attempt = previous.retry_attempt;
            save.retry_at = previous.retry_at;
        }
        save.waiters.extend(previous.waiters);
    }
    entries.insert(id, Entry::Save(save));
}

fn wake_checkpoint(pending: &Pending, wake: &flume::Sender<()>, id: MakiId) {
    if wake.send(()).is_err()
        && let Some(Entry::Save(save)) = lock(pending).entries.remove(&id)
    {
        discard_coordinator_candidate(pending, id, &save);
        fail_waiters(id, save.waiters, "storage writer unavailable");
    }
}

fn commit_save(pending: &Pending, id: MakiId, save: &PendingSave) {
    let mut state = lock(pending);
    if state.latest_generations.get(&id) != Some(&save.generation) {
        return;
    }
    let Some(generation) = save.coordinator_generation else {
        return;
    };
    let authoritative = state
        .latest
        .get(&id)
        .map(|latest| {
            Arc::new(merge_tui_snapshot(
                latest,
                &save.session,
                save.coordinator_history_base.is_some(),
            ))
        })
        .unwrap_or_else(|| Arc::clone(&save.session));
    state.latest.insert(id, authoritative);
    if let Some(history_base) = save.coordinator_history_base {
        state.coordinator_history_bases.insert(id, history_base);
    }
    if state
        .coordinator_pending
        .get(&id)
        .is_some_and(|pending| pending.generation == generation)
    {
        state.coordinator_pending.remove(&id);
    }
}

fn discard_coordinator_candidate(pending: &Pending, id: MakiId, save: &PendingSave) {
    let Some(generation) = save.coordinator_generation else {
        return;
    };
    let mut state = lock(pending);
    if state
        .coordinator_pending
        .get(&id)
        .is_some_and(|pending| pending.generation == generation)
    {
        state.coordinator_pending.remove(&id);
    }
}

fn requeue_save(
    pending: &Pending,
    id: MakiId,
    save: PendingSave,
) -> Result<(), Vec<CheckpointWaiter>> {
    let mut state = lock(pending);
    match state.entries.remove(&id) {
        Some(Entry::Save(mut newer)) => {
            if !save.waiters.is_empty() && !state.fail_closed.contains_key(&id) {
                newer.retry_attempt = newer.retry_attempt.max(save.retry_attempt);
                newer.retry_at = save.retry_at;
            }
            newer.waiters.extend(save.waiters);
            state.entries.insert(id, Entry::Save(newer));
            Ok(())
        }
        Some(Entry::Delete(done)) => {
            state.entries.insert(id, Entry::Delete(done));
            Err(save.waiters)
        }
        None => {
            let mut save = save;
            if let Some(guard) = state.fail_closed.get(&id) {
                let current = state.latest.get(&id).unwrap_or(guard);
                save.session = Arc::clone(current);
                save.coordinator_generation = None;
                save.coordinator_history_base = None;
                save.generation = *state
                    .latest_generations
                    .get(&id)
                    .unwrap_or(&save.generation);
            }
            state.entries.insert(id, Entry::Save(save));
            Ok(())
        }
    }
}

fn merge_tui_snapshot(
    incoming: &AppSession,
    latest: &AppSession,
    preserve_history: bool,
) -> AppSession {
    let mut session = incoming.clone();
    if preserve_history && session.history_identity() != latest.history_identity() {
        session.adopt_history(&latest.history_snapshot());
    }
    session.set_model(latest.model.clone());
    session.set_cwd(latest.cwd.clone());
    session.meta.yolo = latest.meta.yolo;
    session.meta.fast = latest.meta.fast;
    session.meta.workflow = latest.meta.workflow;
    session.meta.thinking = latest.meta.thinking;
    session.meta.session_options = latest.meta.session_options.clone();
    session
}

fn merge_checkpoint(base: &AppSession, checkpoint: &SessionCheckpoint) -> AppSession {
    let mut session = base.clone();
    // Only a history replacement carries messages. An option or model change
    // leaves them alone: while a turn holds the lease the coordinator's copy
    // is the pre-turn one, and writing it would rewind the stored session to
    // before the running turn.
    if let Some(history) = &checkpoint.history {
        session.replace_shared_messages(Arc::clone(history));
    }
    session.set_model(checkpoint.model.to_string());
    session.set_cwd(checkpoint.cwd.to_string_lossy().into_owned());
    session.meta.yolo = Some(option_enabled(&checkpoint.options, YOLO_OPTION_ID));
    session.meta.fast = option_enabled(&checkpoint.options, FAST_OPTION_ID);
    session.meta.workflow = option_enabled(&checkpoint.options, WORKFLOW_OPTION_ID);
    session.meta.thinking = checkpoint
        .options
        .options
        .iter()
        .find(|state| state.definition.id.as_ref() == THINKING_OPTION_ID)
        .and_then(|state| state.current_value.parse::<ThinkingConfig>().ok())
        .map(Into::into);
    session.meta.session_options = checkpoint
        .options
        .options
        .iter()
        .filter(|state| {
            state.definition.persistent
                && matches!(state.definition.owner, SessionOptionOwner::Plugin { .. })
        })
        .map(|state| {
            (
                state.definition.id.to_string(),
                state.current_value.to_string(),
            )
        })
        .collect();
    session
}

fn option_enabled(options: &maki_agent::session_options::SessionOptionsSnapshot, id: &str) -> bool {
    options
        .options
        .iter()
        .find(|state| state.definition.id.as_ref() == id)
        .is_some_and(|state| state.current_value.as_ref() == ENABLED_VALUE)
}

fn writer_gone() -> SessionError {
    StorageError::Io(io::Error::other("storage writer unavailable")).into()
}

fn acknowledge_waiters(id: MakiId, waiters: Vec<CheckpointWaiter>) {
    for waiter in waiters {
        let _ = waiter.reply.send(Ok(CheckpointAck {
            session_id: id,
            version: waiter.version,
        }));
    }
}

fn fail_waiters(id: MakiId, waiters: Vec<CheckpointWaiter>, message: &str) {
    for waiter in waiters {
        let _ = waiter.reply.send(Err(CheckpointError::Save {
            session_id: id,
            message: Arc::from(message),
        }));
    }
}

/// Everything the writer thread owns. It never leaves that thread, so nothing
/// here needs a lock.
struct Writer {
    dir: StateDir,
    warn_tx: flume::Sender<String>,
    /// Only cursors that still describe their file.
    logs: HashMap<MakiId, SessionLog>,
    /// Sessions whose last write failed, so a sick disk warns once instead of
    /// once per frame.
    failing: HashSet<MakiId>,
}

impl Writer {
    fn forget(&mut self, id: MakiId) {
        self.logs.remove(&id);
        self.failing.remove(&id);
    }

    fn flush(&mut self, pending: &Pending) -> Option<Instant> {
        // Bound first: a `for` head temporary lives for the whole loop, so
        // iterating the guard directly would deadlock the re-insert below.
        let batch = {
            let mut state = lock(pending);
            let batch = mem::take(&mut state.entries);
            for (&id, entry) in &batch {
                if let Entry::Save(save) = entry {
                    state.in_flight.insert(
                        id,
                        InFlightSave {
                            generation: save.generation,
                            session: Arc::clone(&save.session),
                            history_base: save.coordinator_history_base,
                        },
                    );
                }
            }
            batch
        };
        for (id, entry) in batch {
            match entry {
                Entry::Save(mut save) => {
                    if save
                        .retry_at
                        .is_some_and(|retry_at| retry_at > Instant::now())
                    {
                        if let Err(waiters) = requeue_save(pending, id, save) {
                            fail_waiters(id, waiters, "checkpoint superseded by session deletion");
                        }
                        lock(pending).in_flight.remove(&id);
                        continue;
                    }
                    save.retry_at = None;
                    match self.write(&save.session) {
                        Ok(()) => {
                            commit_save(pending, id, &save);
                            acknowledge_waiters(id, save.waiters);
                            self.report(id, Ok::<(), &str>(()));
                        }
                        Err(error) => {
                            let message = error.to_string();
                            self.report(id, Err(&message));
                            if !save.waiters.is_empty()
                                && save.retry_attempt < CHECKPOINT_RETRY_BACKOFFS.len()
                            {
                                save.retry_at = Some(
                                    Instant::now() + CHECKPOINT_RETRY_BACKOFFS[save.retry_attempt],
                                );
                                save.retry_attempt += 1;
                                if let Err(waiters) = requeue_save(pending, id, save) {
                                    fail_waiters(
                                        id,
                                        waiters,
                                        "checkpoint superseded by session deletion",
                                    );
                                }
                                lock(pending).in_flight.remove(&id);
                                continue;
                            }

                            let waiters = mem::take(&mut save.waiters);
                            if save.coordinator_generation.is_some() {
                                discard_coordinator_candidate(pending, id, &save);
                                fail_waiters(id, waiters, &message);
                                lock(pending).in_flight.remove(&id);
                                continue;
                            }
                            if !waiters.is_empty() {
                                save.retry_attempt = 0;
                            }
                            let backoff = BACKGROUND_RETRY_BACKOFFS
                                [save.retry_attempt.min(BACKGROUND_RETRY_BACKOFFS.len() - 1)];
                            save.retry_attempt = save.retry_attempt.saturating_add(1);
                            save.retry_at = Some(Instant::now() + backoff);
                            let replaced_by_delete = requeue_save(pending, id, save).is_err();
                            if !waiters.is_empty() {
                                fail_waiters(
                                    id,
                                    waiters,
                                    if replaced_by_delete {
                                        "checkpoint superseded by session deletion"
                                    } else {
                                        &message
                                    },
                                );
                            }
                        }
                    }
                }
                Entry::Delete(done) => {
                    self.forget(id);
                    done(match AppSession::delete(id, &self.dir) {
                        Err(SessionError::Storage(StorageError::NotFound(_))) => Ok(()),
                        result => result,
                    });
                }
            }
            lock(pending).in_flight.remove(&id);
        }
        lock(pending)
            .entries
            .values()
            .filter_map(|entry| match entry {
                Entry::Save(save) => save.retry_at,
                Entry::Delete(_) => None,
            })
            .min()
    }

    fn write(&mut self, session: &AppSession) -> Result<(), SessionError> {
        let sessions_dir = self.dir.ensure_subdir(SESSIONS_DIR)?;
        if let Some(mut log) = self.logs.remove(&session.id) {
            // A failed `append` rolls the file back to the last record boundary,
            // so the cursor still fits and is worth keeping: rebuilding it costs
            // a second full write, the last thing a failing disk needs.
            // Divergence is the one answer a cursor cannot survive.
            let appended = log.append(session);
            if !matches!(appended, Err(SessionError::LogDiverged { .. })) {
                self.logs.insert(session.id, log);
                return appended;
            }
        }
        // No usable cursor, whether because this thread never wrote the file
        // or because the log diverged, so the file starts over. Reading the
        // old file back gains nothing: a cursor recovered from disk describes
        // the session that was stored, never the live one.
        self.logs
            .insert(session.id, SessionLog::rewrite(&sessions_dir, session)?);
        Ok(())
    }

    fn report(&mut self, id: MakiId, result: Result<(), impl std::fmt::Display>) {
        match result {
            Ok(()) => {
                if self.failing.remove(&id) {
                    let _ = self.warn_tx.send(SAVE_RECOVERED.to_string());
                }
            }
            Err(e) => {
                warn!(error = %e, %id, "session write failed");
                if self.failing.insert(id) {
                    let _ = self.warn_tx.send(format!("{SAVE_FAILED_PREFIX}: {e}"));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use maki_agent::SessionMailbox;
    use maki_agent::session_coordinator::{
        DirectoryAdoptionFuture, ModelAdoptionFuture, SessionCoordinatorError,
        SessionCoordinatorHandle, SessionCoordinatorParams, builtin_option_definitions,
    };
    use maki_config::ModelPolicy;
    use maki_providers::Model;
    use std::sync::atomic::AtomicBool;
    use tempfile::TempDir;

    const DRAIN_TIMEOUT: Duration = Duration::from_secs(30);
    const MODEL: &str = "test-model";
    const FAILED_MODEL: &str = "failed-model";
    const CWD: &str = "/tmp/writer";
    const MSG_PREFIX: &str = "msg-";
    const RESUMED_MSG: &str = "resumed";
    const TOOL_ID: &str = "tool-1";
    const TOOL_TEXT: &str = "tool output";
    const TITLE: &str = "renamed after reload";

    fn state_dir() -> (TempDir, StateDir) {
        let tmp = TempDir::new().unwrap();
        let dir = StateDir::from_path(tmp.path().to_path_buf());
        (tmp, dir)
    }

    fn writer(dir: &StateDir) -> (StorageWriter, flume::Receiver<String>) {
        let (warn_tx, warn_rx) = flume::unbounded();
        (StorageWriter::new(dir.clone(), warn_tx), warn_rx)
    }

    const PREVIOUS_MODEL: &str = "ollama/llama3";
    const ADOPTED_MODEL: &str = "anthropic/claude-opus-4-8";

    fn coordinator(writer: &StorageWriter, session: &AppSession) -> SessionCoordinatorHandle {
        coordinator_with_checkpoint(session, writer.coordinator_checkpoint())
    }

    fn coordinator_with_checkpoint(
        session: &AppSession,
        checkpoint: Arc<dyn CheckpointWriter<SessionCheckpoint>>,
    ) -> SessionCoordinatorHandle {
        SessionCoordinatorHandle::register(SessionCoordinatorParams {
            session_id: session.id,
            catalog: Default::default(),
            definitions: builtin_option_definitions(
                PREVIOUS_MODEL,
                [Arc::from(PREVIOUS_MODEL)],
                false,
                false,
                false,
                ThinkingConfig::Off,
            ),
            persisted_options: Default::default(),
            history: session.messages().to_vec(),
            model: Arc::from(PREVIOUS_MODEL),
            cwd: CWD.into(),
            model_policy: Arc::new(ModelPolicy::default()),
            model_adopter: Arc::new(|_: Model| Box::pin(async { Ok(()) }) as ModelAdoptionFuture),
            directory_adopter: Arc::new(|path| {
                Box::pin(async move { Ok(path) }) as DirectoryAdoptionFuture
            }),
            checkpoint,
            mailbox: SessionMailbox::new(session.id),
        })
        .unwrap()
    }

    fn message_texts(session: &AppSession) -> Vec<String> {
        session
            .messages()
            .iter()
            .map(|m| m.user_text().unwrap_or_default().to_string())
            .collect()
    }

    fn msg_text(n: usize) -> String {
        format!("{MSG_PREFIX}{n}")
    }

    fn user_message(n: usize) -> maki_providers::Message {
        maki_providers::Message::user(msg_text(n))
    }

    /// A plain file where the sessions dir should be. `create_dir_all` cannot
    /// turn that into a directory, so every flush fails until it is removed.
    fn block_sessions_dir(dir: &StateDir) {
        std::fs::write(dir.path().join(SESSIONS_DIR), "").unwrap();
    }

    /// Snapshots must coalesce per session id, not into one `latest` slot:
    /// two racing sessions used to silently drop one.
    #[test]
    fn shutdown_drains_newest_snapshot_of_every_session() {
        let (_tmp, dir) = state_dir();
        let (writer, _warn_rx) = writer(&dir);
        let a = AppSession::new("test-model", "/tmp/a");
        let mut b = AppSession::new("test-model", "/tmp/b");
        let (a_id, b_id) = (a.id, b.id);
        writer.send(Arc::new(a));
        writer.send(Arc::new(b.clone()));
        b.set_title("renamed".into());
        writer.send(Arc::new(b));
        writer.shutdown(DRAIN_TIMEOUT);

        assert!(AppSession::load(a_id, &dir).is_ok());
        assert_eq!(AppSession::load(b_id, &dir).unwrap().title, "renamed");
    }

    #[test]
    fn divergent_tui_send_replaces_pending_coordinator_history() {
        let (_tmp, dir) = state_dir();
        let pending: Pending = Arc::default();
        let (wake, _wake_rx) = flume::unbounded();
        let (_done_tx, done_rx) = flume::bounded(1);
        let writer = StorageWriter {
            pending: Arc::clone(&pending),
            wake: wake.clone(),
            done_rx,
            stop: Arc::default(),
        };
        let checkpoint = CoordinatorCheckpointWriter {
            pending: Arc::clone(&pending),
            wake,
        };
        let mut session = AppSession::new(PREVIOUS_MODEL, CWD);
        session.push_message(user_message(0));
        let id = session.id;
        writer.seed(Arc::new(session.clone()));
        let options = maki_agent::session_options::SessionOptions::new(
            builtin_option_definitions(
                ADOPTED_MODEL,
                [Arc::from(ADOPTED_MODEL)],
                false,
                false,
                false,
                ThinkingConfig::Off,
            ),
            &Default::default(),
        )
        .unwrap()
        .snapshot();
        let ack = checkpoint.checkpoint(CheckpointRequest {
            session_id: id,
            version: CheckpointVersion {
                revision: 1,
                epoch: 1,
            },
            snapshot: Arc::new(SessionCheckpoint {
                history: Some(Arc::new(vec![user_message(0), user_message(1)])),
                model: Arc::from(ADOPTED_MODEL),
                cwd: CWD.into(),
                options,
            }),
        });
        session.replace_messages(vec![user_message(2)]);
        writer.send(Arc::new(session));
        assert_eq!(message_texts(&lock(&pending).latest[&id]), [msg_text(2)]);
        let state = lock(&pending);
        assert_eq!(
            message_texts(&state.coordinator_pending[&id].session),
            [msg_text(2)]
        );
        assert!(state.coordinator_pending[&id].history_base.is_none());
        drop(state);
        let (warn_tx, _warn_rx) = flume::unbounded();
        let mut disk_writer = Writer {
            dir: dir.clone(),
            warn_tx,
            logs: HashMap::new(),
            failing: HashSet::new(),
        };
        disk_writer.flush(&pending);
        assert_eq!(smol::block_on(ack).unwrap().version.revision, 1);
        assert_eq!(
            message_texts(&AppSession::load(id, &dir).unwrap()),
            [msg_text(2)]
        );
    }

    #[test]
    fn rebase_preserves_only_history_with_matching_tui_base() {
        for in_flight in [false, true] {
            for divergent in [false, true] {
                let (_tmp, dir) = state_dir();
                let pending: Pending = Arc::default();
                let (wake, _wake_rx) = flume::unbounded();
                let checkpoint = CoordinatorCheckpointWriter {
                    pending: Arc::clone(&pending),
                    wake: wake.clone(),
                };
                let mut session = AppSession::new(PREVIOUS_MODEL, CWD);
                session.push_message(user_message(0));
                let id = session.id;
                lock(&pending).latest.insert(id, Arc::new(session.clone()));
                let candidate_options = maki_agent::session_options::SessionOptions::new(
                    builtin_option_definitions(
                        ADOPTED_MODEL,
                        [Arc::from(ADOPTED_MODEL)],
                        true,
                        true,
                        true,
                        ThinkingConfig::Off,
                    ),
                    &Default::default(),
                )
                .unwrap()
                .snapshot();
                let restored_options = maki_agent::session_options::SessionOptions::new(
                    builtin_option_definitions(
                        PREVIOUS_MODEL,
                        [Arc::from(PREVIOUS_MODEL)],
                        false,
                        false,
                        false,
                        ThinkingConfig::Off,
                    ),
                    &Default::default(),
                )
                .unwrap()
                .snapshot();
                let history_ack = checkpoint.checkpoint(CheckpointRequest {
                    session_id: id,
                    version: CheckpointVersion {
                        revision: 1,
                        epoch: 1,
                    },
                    snapshot: Arc::new(SessionCheckpoint {
                        history: Some(Arc::new(vec![user_message(0), user_message(1)])),
                        model: Arc::from(ADOPTED_MODEL),
                        cwd: CWD.into(),
                        options: candidate_options,
                    }),
                });
                let writing = if in_flight {
                    let mut state = lock(&pending);
                    let Entry::Save(save) = state.entries.remove(&id).unwrap() else {
                        unreachable!()
                    };
                    state.in_flight.insert(
                        id,
                        InFlightSave {
                            generation: save.generation,
                            session: Arc::clone(&save.session),
                            history_base: save.coordinator_history_base,
                        },
                    );
                    Some(save)
                } else {
                    None
                };
                if divergent {
                    let (_done_tx, done_rx) = flume::bounded(1);
                    let writer = StorageWriter {
                        pending: Arc::clone(&pending),
                        wake,
                        done_rx,
                        stop: Arc::default(),
                    };
                    session.replace_messages(vec![user_message(2)]);
                    writer.send(Arc::new(session));
                }
                let rebase_ack = checkpoint.checkpoint(CheckpointRequest {
                    session_id: id,
                    version: CheckpointVersion {
                        revision: 2,
                        epoch: CHECKPOINT_REBASE_EPOCH,
                    },
                    snapshot: Arc::new(SessionCheckpoint {
                        history: None,
                        model: Arc::from(PREVIOUS_MODEL),
                        cwd: CWD.into(),
                        options: restored_options,
                    }),
                });
                if let Some(save) = writing {
                    assert!(requeue_save(&pending, id, save).is_ok());
                    lock(&pending).in_flight.remove(&id);
                }
                let (warn_tx, _warn_rx) = flume::unbounded();
                let mut writer = Writer {
                    dir: dir.clone(),
                    warn_tx,
                    logs: HashMap::new(),
                    failing: HashSet::new(),
                };
                writer.flush(&pending);
                assert_eq!(smol::block_on(history_ack).unwrap().version.revision, 1);
                assert_eq!(
                    smol::block_on(rebase_ack).unwrap().version.epoch,
                    CHECKPOINT_REBASE_EPOCH
                );
                let saved = AppSession::load(id, &dir).unwrap();
                let expected = if divergent {
                    vec![msg_text(2)]
                } else {
                    vec![msg_text(0), msg_text(1)]
                };
                assert_eq!(message_texts(&saved), expected);
                assert_eq!(saved.model, PREVIOUS_MODEL);
                assert_eq!(saved.meta.yolo, Some(false));
                assert!(!saved.meta.fast);
                assert!(!saved.meta.workflow);
                assert_eq!(pending.lock().unwrap().latest[&id].model, PREVIOUS_MODEL);
                assert_eq!(
                    message_texts(&pending.lock().unwrap().latest[&id]),
                    expected
                );
            }
        }
    }

    #[test]
    fn fail_closed_checkpoint_overwrites_in_flight_candidate_and_retains_history() {
        let (_tmp, dir) = state_dir();
        let pending: Pending = Arc::default();
        let (wake, wake_rx) = flume::unbounded();
        let (_done_tx, done_rx) = flume::bounded(1);
        let writer = Arc::new(StorageWriter {
            pending: Arc::clone(&pending),
            wake: wake.clone(),
            done_rx,
            stop: Arc::default(),
        });
        let checkpoint = writer.coordinator_checkpoint();
        let mut session = AppSession::new(PREVIOUS_MODEL, CWD);
        session.push_message(user_message(0));
        let id = session.id;
        writer.seed(Arc::new(session.clone()));
        let options = maki_agent::session_options::SessionOptions::new(
            builtin_option_definitions(
                ADOPTED_MODEL,
                [Arc::from(ADOPTED_MODEL)],
                true,
                true,
                true,
                ThinkingConfig::Off,
            ),
            &Default::default(),
        )
        .unwrap()
        .snapshot();
        let candidate_ack = checkpoint.checkpoint(CheckpointRequest {
            session_id: id,
            version: CheckpointVersion {
                revision: 1,
                epoch: 1,
            },
            snapshot: Arc::new(SessionCheckpoint {
                history: Some(Arc::new(vec![user_message(0), user_message(1)])),
                model: Arc::from(ADOPTED_MODEL),
                cwd: CWD.into(),
                options,
            }),
        });
        let in_flight = {
            let mut state = lock(&pending);
            let Entry::Save(save) = state.entries.remove(&id).unwrap() else {
                unreachable!()
            };
            state.in_flight.insert(
                id,
                InFlightSave {
                    generation: save.generation,
                    session: Arc::clone(&save.session),
                    history_base: save.coordinator_history_base,
                },
            );
            save
        };
        let fallback_writer = Arc::clone(&writer);
        let fallback = std::thread::spawn(move || {
            fallback_writer.fail_closed_checkpoint(Arc::new(session), DRAIN_TIMEOUT)
        });
        while !lock(&pending).fail_closed.contains_key(&id) {
            wake_rx.recv_timeout(DRAIN_TIMEOUT).unwrap();
        }
        let (warn_tx, _warn_rx) = flume::unbounded();
        let mut disk_writer = Writer {
            dir: dir.clone(),
            warn_tx,
            logs: HashMap::new(),
            failing: HashSet::new(),
        };
        disk_writer.write(&in_flight.session).unwrap();
        commit_save(&pending, id, &in_flight);
        acknowledge_waiters(id, in_flight.waiters);
        lock(&pending).in_flight.remove(&id);
        assert_eq!(AppSession::load(id, &dir).unwrap().model, ADOPTED_MODEL);
        disk_writer.flush(&pending);
        assert_eq!(fallback.join().unwrap().unwrap().session_id, id);
        assert!(smol::block_on(candidate_ack).is_ok());
        let saved = AppSession::load(id, &dir).unwrap();
        assert_eq!(saved.model, PREVIOUS_MODEL);
        assert_eq!(message_texts(&saved), [msg_text(0), msg_text(1)]);
        assert!(!saved.meta.fast);
        assert!(!saved.meta.workflow);
    }

    #[test]
    fn fail_closed_blocks_future_coordinator_and_tui_models() {
        let (_tmp, dir) = state_dir();
        let (writer, _warn_rx) = writer(&dir);
        let mut session = AppSession::new(PREVIOUS_MODEL, CWD);
        session.push_message(user_message(0));
        let id = session.id;
        writer.seed(Arc::new(session.clone()));
        writer
            .fail_closed_checkpoint(Arc::new(session.clone()), DRAIN_TIMEOUT)
            .unwrap();
        let options = maki_agent::session_options::SessionOptions::new(
            builtin_option_definitions(
                ADOPTED_MODEL,
                [Arc::from(ADOPTED_MODEL)],
                true,
                true,
                true,
                ThinkingConfig::Off,
            ),
            &Default::default(),
        )
        .unwrap()
        .snapshot();
        let ack = writer
            .coordinator_checkpoint()
            .checkpoint(CheckpointRequest {
                session_id: id,
                version: CheckpointVersion {
                    revision: 1,
                    epoch: 1,
                },
                snapshot: Arc::new(SessionCheckpoint {
                    history: Some(Arc::new(vec![user_message(0), user_message(1)])),
                    model: Arc::from(ADOPTED_MODEL),
                    cwd: CWD.into(),
                    options,
                }),
            });
        assert!(smol::block_on(ack).is_ok());
        assert!(
            writer
                .fail_closed_checkpoint(Arc::new(session.clone()), DRAIN_TIMEOUT)
                .is_ok()
        );
        session.set_model(ADOPTED_MODEL.into());
        session.push_message(user_message(2));
        writer.send(Arc::new(session));
        writer.shutdown(DRAIN_TIMEOUT);
        let saved = AppSession::load(id, &dir).unwrap();
        assert_eq!(saved.model, PREVIOUS_MODEL);
        assert!(!saved.meta.fast);
        assert!(!saved.meta.workflow);
        assert_eq!(message_texts(&saved), [msg_text(0), msg_text(2)]);
    }

    #[test]
    fn fail_closed_checkpoint_times_out_without_a_writer() {
        let pending: Pending = Arc::default();
        let (wake, _wake_rx) = flume::unbounded();
        let (_done_tx, done_rx) = flume::bounded(1);
        let writer = StorageWriter {
            pending: Arc::clone(&pending),
            wake,
            done_rx,
            stop: Arc::default(),
        };
        let session = Arc::new(AppSession::new(PREVIOUS_MODEL, CWD));
        let id = session.id;
        assert!(matches!(
            writer.fail_closed_checkpoint(session, Duration::ZERO),
            Err(CheckpointError::Save { session_id, .. }) if session_id == id
        ));
        assert!(lock(&pending).fail_closed.contains_key(&id));
    }

    #[test]
    fn older_in_flight_success_cannot_replace_newer_authoritative_snapshot() {
        let pending: Pending = Arc::default();
        let mut old = AppSession::new(MODEL, CWD);
        let id = old.id;
        old.set_title("old write".into());
        let old = Arc::new(old);
        {
            let mut state = lock(&pending);
            state.latest.insert(id, Arc::clone(&old));
            state.latest_generations.insert(id, 1);
            state.entries.insert(
                id,
                Entry::Save(PendingSave {
                    session: Arc::clone(&old),
                    waiters: Vec::new(),
                    generation: 1,
                    coordinator_generation: Some(1),
                    coordinator_history_base: None,
                    retry_attempt: 0,
                    retry_at: None,
                }),
            );
        }
        let Entry::Save(old_in_flight) = lock(&pending).entries.remove(&id).unwrap() else {
            unreachable!()
        };
        let mut newer = old.as_ref().clone();
        newer.set_title("new authoritative".into());
        {
            let mut state = lock(&pending);
            state.latest.insert(id, Arc::new(newer));
            state.latest_generations.insert(id, 2);
        }

        commit_save(&pending, id, &old_in_flight);

        assert_eq!(lock(&pending).latest[&id].title, "new authoritative");
    }

    #[test]
    fn deferred_retry_merges_into_newer_generation_without_losing_waiters() {
        const OLD_GENERATION: u64 = 1;
        const NEW_GENERATION: u64 = 2;
        const RETRY_ATTEMPT: usize = 1;

        let pending: Pending = Arc::default();
        let mut old = AppSession::new(MODEL, CWD);
        let id = old.id;
        old.set_title("old retry".into());
        let mut newer = old.clone();
        newer.set_title("new generation".into());
        let old_version = CheckpointVersion {
            revision: old.revision(),
            epoch: OLD_GENERATION,
        };
        let newer_version = CheckpointVersion {
            revision: newer.revision(),
            epoch: NEW_GENERATION,
        };
        let (old_reply, old_ack) = flume::bounded(1);
        let (newer_reply, newer_ack) = flume::bounded(1);
        let retry_at = Instant::now() + Duration::from_secs(60);
        let old_retry = PendingSave {
            session: Arc::new(old),
            waiters: vec![CheckpointWaiter {
                version: old_version,
                reply: old_reply,
            }],
            generation: OLD_GENERATION,
            coordinator_generation: Some(OLD_GENERATION),
            coordinator_history_base: None,
            retry_attempt: RETRY_ATTEMPT,
            retry_at: Some(retry_at),
        };
        lock(&pending).entries.insert(
            id,
            Entry::Save(PendingSave {
                session: Arc::new(newer),
                waiters: vec![CheckpointWaiter {
                    version: newer_version,
                    reply: newer_reply,
                }],
                generation: NEW_GENERATION,
                coordinator_generation: Some(NEW_GENERATION),
                coordinator_history_base: None,
                retry_attempt: 0,
                retry_at: None,
            }),
        );

        assert!(requeue_save(&pending, id, old_retry).is_ok());

        let Entry::Save(merged) = lock(&pending).entries.remove(&id).unwrap() else {
            unreachable!()
        };
        assert_eq!(merged.session.title, "new generation");
        assert_eq!(merged.generation, NEW_GENERATION);
        assert_eq!(merged.coordinator_generation, Some(NEW_GENERATION));
        assert_eq!(merged.retry_attempt, RETRY_ATTEMPT);
        assert_eq!(merged.retry_at, Some(retry_at));
        acknowledge_waiters(id, merged.waiters);
        assert_eq!(old_ack.recv().unwrap().unwrap().version, old_version);
        assert_eq!(newer_ack.recv().unwrap().unwrap().version, newer_version);
    }

    #[test]
    fn coalesced_checkpoints_complete_every_acknowledgement() {
        smol::block_on(async {
            let (_tmp, dir) = state_dir();
            let (writer, _warn_rx) = writer(&dir);
            let mut first = AppSession::new(MODEL, CWD);
            let id = first.id;
            first.set_title("first".into());
            let mut second = first.clone();
            second.set_title("second".into());
            let first_version = CheckpointVersion {
                revision: first.revision(),
                epoch: 1,
            };
            let second_version = CheckpointVersion {
                revision: second.revision(),
                epoch: 1,
            };

            let first_ack = writer.checkpoint(CheckpointRequest {
                session_id: id,
                version: first_version,
                snapshot: Arc::new(first),
            });
            let second_ack = writer.checkpoint(CheckpointRequest {
                session_id: id,
                version: second_version,
                snapshot: Arc::new(second),
            });
            let (first_ack, second_ack) = futures_lite::future::zip(first_ack, second_ack).await;

            assert_eq!(first_ack.unwrap().version, first_version);
            assert_eq!(second_ack.unwrap().version, second_version);
            writer.shutdown(DRAIN_TIMEOUT);
            assert_eq!(AppSession::load(id, &dir).unwrap().title, "second");
        });
    }

    /// Every coordinator's checkpoint writer holds a clone of the wake
    /// sender, and those live in tasks that exit on their own schedule. If
    /// shutdown waited for the last clone to drop, exit would take the whole
    /// timeout whenever one outlived the event loop.
    #[test]
    fn shutdown_does_not_wait_for_a_lingering_checkpoint_writer() {
        let (_tmp, dir) = state_dir();
        let (writer, _warn_rx) = writer(&dir);
        // Stands in for a coordinator task that has not been polled yet.
        let lingering = writer.coordinator_checkpoint();

        let started = std::time::Instant::now();
        writer.shutdown(Duration::from_secs(3));
        let elapsed = started.elapsed();

        drop(lingering);
        assert!(
            elapsed < Duration::from_secs(1),
            "shutdown waited {elapsed:?} for a checkpoint writer that outlived the loop"
        );
    }

    /// A session with nothing in it yet gets its files cleaned up, but it is
    /// still live in its tab and its coordinator checkpoints by merging into
    /// the writer's snapshot. Forgetting that snapshot left the next
    /// coordinator checkpoint -- the model change on a freshly `/new`ed
    /// session -- failing with "session snapshot is unavailable".
    #[test]
    fn cleaning_up_an_empty_session_keeps_its_coordinator_base() {
        smol::block_on(async {
            let (_tmp, dir) = state_dir();
            let (writer, _warn_rx) = writer(&dir);
            let session = AppSession::new(MODEL, CWD);
            let id = session.id;
            writer.send(Arc::new(session));

            writer.delete_empty(id);

            let options = maki_agent::session_options::SessionOptions::new(
                maki_agent::session_coordinator::builtin_option_definitions(
                    "next/model",
                    [Arc::from("next/model")],
                    false,
                    false,
                    false,
                    maki_agent::ThinkingConfig::Off,
                ),
                &Default::default(),
            )
            .unwrap()
            .snapshot();
            let ack = writer
                .coordinator_checkpoint()
                .checkpoint(CheckpointRequest {
                    session_id: id,
                    version: CheckpointVersion {
                        revision: 1,
                        epoch: 1,
                    },
                    snapshot: Arc::new(SessionCheckpoint {
                        history: Some(Arc::new(Vec::new())),
                        model: Arc::from("next/model"),
                        cwd: std::path::PathBuf::from(CWD),
                        options,
                    }),
                })
                .await;
            assert!(
                ack.is_ok(),
                "an emptied session must still accept a coordinator checkpoint: {ack:?}"
            );
        });
    }

    /// A user-requested delete is different: the session is meant to be gone,
    /// so its snapshot goes too.
    #[test]
    fn deleting_a_session_forgets_its_coordinator_base() {
        smol::block_on(async {
            let (_tmp, dir) = state_dir();
            let (writer, _warn_rx) = writer(&dir);
            let session = AppSession::new(MODEL, CWD);
            let id = session.id;
            writer.send(Arc::new(session));

            let (done_tx, done_rx) = flume::bounded(1);
            writer.delete(id, move |res| {
                let _ = done_tx.send(res);
            });
            done_rx.recv_async().await.unwrap().unwrap();

            let options = maki_agent::session_options::SessionOptions::new(
                maki_agent::session_coordinator::builtin_option_definitions(
                    "next/model",
                    [Arc::from("next/model")],
                    false,
                    false,
                    false,
                    maki_agent::ThinkingConfig::Off,
                ),
                &Default::default(),
            )
            .unwrap()
            .snapshot();
            let ack = writer
                .coordinator_checkpoint()
                .checkpoint(CheckpointRequest {
                    session_id: id,
                    version: CheckpointVersion {
                        revision: 1,
                        epoch: 1,
                    },
                    snapshot: Arc::new(SessionCheckpoint {
                        history: Some(Arc::new(Vec::new())),
                        model: Arc::from("next/model"),
                        cwd: std::path::PathBuf::from(CWD),
                        options,
                    }),
                })
                .await;
            assert!(ack.is_err(), "a deleted session must not be resurrected");
        });
    }

    #[test]
    fn coordinator_checkpoint_merges_without_losing_tui_state() {
        smol::block_on(async {
            let (_tmp, dir) = state_dir();
            let (writer, _warn_rx) = writer(&dir);
            let mut session = AppSession::new(MODEL, CWD);
            session.set_title(TITLE.into());
            session.meta.mode = Some(maki_storage::sessions::StoredMode::Plan);
            session.meta.queued_messages = vec!["queued".into()];
            let id = session.id;
            writer.send(Arc::new(session.clone()));

            let options = maki_agent::session_options::SessionOptions::new(
                maki_agent::session_coordinator::builtin_option_definitions(
                    "next/model",
                    [Arc::from("next/model")],
                    true,
                    true,
                    true,
                    maki_agent::ThinkingConfig::Effort(maki_providers::Effort::High),
                ),
                &Default::default(),
            )
            .unwrap()
            .snapshot();
            let history = vec![maki_providers::Message::user("coordinator history".into())];
            let version = CheckpointVersion {
                revision: 1,
                epoch: 1,
            };
            let checkpoint = writer.coordinator_checkpoint();
            let ack = checkpoint
                .checkpoint(CheckpointRequest {
                    session_id: id,
                    version,
                    snapshot: Arc::new(SessionCheckpoint {
                        history: Some(Arc::new(history)),
                        model: Arc::from("next/model"),
                        cwd: "/tmp/next".into(),
                        options,
                    }),
                })
                .await
                .unwrap();

            assert_eq!(ack.version, version);
            let persisted = AppSession::load(id, &dir).unwrap();
            assert_eq!(
                persisted.meta.thinking,
                Some(maki_storage::sessions::StoredThinking::Effort {
                    level: maki_providers::Effort::High
                })
            );
            let mut later_ui = session;
            later_ui.set_title("later UI title".into());
            later_ui.meta.mode = Some(maki_storage::sessions::StoredMode::Plan);
            later_ui.meta.queued_messages = vec!["queued".into()];
            writer.send(Arc::new(later_ui));
            drop(checkpoint);
            writer.shutdown(DRAIN_TIMEOUT);
            let loaded = AppSession::load(id, &dir).unwrap();
            assert_eq!(loaded.title, "later UI title");
            assert_eq!(
                loaded.meta.mode,
                Some(maki_storage::sessions::StoredMode::Plan)
            );
            assert_eq!(loaded.meta.queued_messages, ["queued"]);
            assert_eq!(loaded.model, "next/model");
            assert_eq!(loaded.cwd, "/tmp/next");
            assert_eq!(loaded.meta.yolo, Some(true));
            assert!(loaded.meta.fast);
            assert!(loaded.meta.workflow);
            assert_eq!(
                loaded.meta.thinking,
                Some(maki_storage::sessions::StoredThinking::Effort {
                    level: maki_providers::Effort::High
                })
            );
            assert_eq!(message_texts(&loaded), ["coordinator history"]);
        });
    }

    #[test]
    fn canceled_conditional_adoption_never_reaches_saved_app_snapshot() {
        smol::block_on(async {
            let (_tmp, dir) = state_dir();
            let (writer, _warn_rx) = writer(&dir);
            let mut session = AppSession::new(PREVIOUS_MODEL, CWD);
            session.push_message(user_message(0));
            let id = session.id;
            writer.seed(Arc::new(session.clone()));
            let coordinator = coordinator(&writer, &session);
            let receipt = coordinator
                .set_model_if_active(Arc::from(ADOPTED_MODEL), Arc::new(AtomicBool::new(true)))
                .await
                .unwrap();
            assert_eq!(coordinator.read().model().as_ref(), PREVIOUS_MODEL);

            session.set_title(TITLE.into());
            writer.send(Arc::new(session.clone()));
            let saved = writer
                .checkpoint(CheckpointRequest {
                    session_id: id,
                    version: CheckpointVersion {
                        revision: session.revision(),
                        epoch: 1,
                    },
                    snapshot: Arc::new(session.clone()),
                })
                .await
                .unwrap();
            assert_eq!(saved.session_id, id);
            assert_eq!(AppSession::load(id, &dir).unwrap().model, PREVIOUS_MODEL);
            assert!(
                coordinator
                    .rollback_model_if_version(receipt)
                    .await
                    .unwrap()
            );
            coordinator.close().await.unwrap();
            writer.shutdown(DRAIN_TIMEOUT);

            let loaded = AppSession::load(id, &dir).unwrap();
            assert_eq!(loaded.model, PREVIOUS_MODEL);
            assert_eq!(loaded.title, TITLE);
            assert_eq!(message_texts(&loaded), [msg_text(0)]);
        });
    }

    #[test]
    fn app_send_while_adoption_is_pending_does_not_persist_unapproved_model() {
        smol::block_on(async {
            let (_tmp, dir) = state_dir();
            let (writer, _warn_rx) = writer(&dir);
            let mut session = AppSession::new(PREVIOUS_MODEL, CWD);
            session.push_message(user_message(0));
            let id = session.id;
            writer.seed(Arc::new(session.clone()));
            let coordinator = coordinator(&writer, &session);
            let receipt = coordinator
                .set_model_if_active(Arc::from(ADOPTED_MODEL), Arc::new(AtomicBool::new(true)))
                .await
                .unwrap();
            session.set_title(TITLE.into());
            writer.send(Arc::new(session));
            assert!(
                coordinator
                    .rollback_model_if_version(receipt)
                    .await
                    .unwrap()
            );
            coordinator.close().await.unwrap();
            writer.shutdown(DRAIN_TIMEOUT);

            let loaded = AppSession::load(id, &dir).unwrap();
            assert_eq!(loaded.model, PREVIOUS_MODEL);
            assert_eq!(loaded.title, TITLE);
            assert_eq!(message_texts(&loaded), [msg_text(0)]);
        });
    }

    #[test]
    fn conditional_adoption_commits_only_after_finalize_and_preserves_app_state() {
        smol::block_on(async {
            let (_tmp, dir) = state_dir();
            let (writer, _warn_rx) = writer(&dir);
            let mut session = AppSession::new(PREVIOUS_MODEL, CWD);
            session.push_message(user_message(0));
            let id = session.id;
            writer.seed(Arc::new(session.clone()));
            let coordinator = coordinator(&writer, &session);
            let receipt = coordinator
                .set_model_if_active(Arc::from(ADOPTED_MODEL), Arc::new(AtomicBool::new(true)))
                .await
                .unwrap();

            session.set_title(TITLE.into());
            writer.send(Arc::new(session.clone()));
            let saved = writer
                .checkpoint(CheckpointRequest {
                    session_id: id,
                    version: CheckpointVersion {
                        revision: session.revision(),
                        epoch: 1,
                    },
                    snapshot: Arc::new(session.clone()),
                })
                .await
                .unwrap();
            assert_eq!(saved.session_id, id);
            assert_eq!(AppSession::load(id, &dir).unwrap().model, PREVIOUS_MODEL);
            assert!(coordinator.finalize_model_if_active(receipt).await.unwrap());
            assert_eq!(coordinator.read().model().as_ref(), ADOPTED_MODEL);
            coordinator.close().await.unwrap();
            writer.shutdown(DRAIN_TIMEOUT);

            let loaded = AppSession::load(id, &dir).unwrap();
            assert_eq!(loaded.model, ADOPTED_MODEL);
            assert_eq!(loaded.title, TITLE);
            assert_eq!(message_texts(&loaded), [msg_text(0)]);
        });
    }

    #[test]
    fn failed_conditional_finalize_does_not_contaminate_next_app_save() {
        smol::block_on(async {
            let (_tmp, dir) = state_dir();
            let (writer, warn_rx) = writer(&dir);
            let mut session = AppSession::new(PREVIOUS_MODEL, CWD);
            session.push_message(user_message(0));
            let id = session.id;
            writer
                .checkpoint(CheckpointRequest {
                    session_id: id,
                    version: CheckpointVersion {
                        revision: session.revision(),
                        epoch: 1,
                    },
                    snapshot: Arc::new(session.clone()),
                })
                .await
                .unwrap();
            let coordinator = coordinator(&writer, &session);
            let receipt = coordinator
                .set_model_if_active(Arc::from(ADOPTED_MODEL), Arc::new(AtomicBool::new(true)))
                .await
                .unwrap();
            let sessions_dir = dir.path().join(SESSIONS_DIR);
            let parked_dir = dir.path().join("parked-sessions");
            std::fs::rename(&sessions_dir, &parked_dir).unwrap();
            block_sessions_dir(&dir);

            let result = coordinator.finalize_model_if_active(receipt).await;
            assert!(matches!(
                result,
                Err(SessionCoordinatorError::Checkpoint(_))
            ));
            assert!(
                warn_rx
                    .recv_async()
                    .await
                    .unwrap()
                    .starts_with(SAVE_FAILED_PREFIX)
            );
            assert_eq!(coordinator.read().model().as_ref(), PREVIOUS_MODEL);
            std::fs::remove_file(&sessions_dir).unwrap();
            std::fs::rename(parked_dir, &sessions_dir).unwrap();
            assert_eq!(AppSession::load(id, &dir).unwrap().model, PREVIOUS_MODEL);
            session.set_title(TITLE.into());
            writer.send(Arc::new(session));
            coordinator.close().await.unwrap();
            writer.shutdown(DRAIN_TIMEOUT);

            let loaded = AppSession::load(id, &dir).unwrap();
            assert_eq!(loaded.model, PREVIOUS_MODEL);
            assert_eq!(loaded.title, TITLE);
        });
    }

    #[test]
    fn divergent_tui_history_is_not_overwritten_by_coordinator_history() {
        smol::block_on(async {
            let (_tmp, dir) = state_dir();
            let (writer, _warn_rx) = writer(&dir);
            let mut session = AppSession::new(MODEL, CWD);
            session.push_message(maki_providers::Message::user("base".into()));
            let id = session.id;
            writer.send(Arc::new(session.clone()));

            let options = maki_agent::session_options::SessionOptions::new(
                maki_agent::session_coordinator::builtin_option_definitions(
                    MODEL,
                    [Arc::from(MODEL)],
                    false,
                    false,
                    false,
                    maki_agent::ThinkingConfig::Off,
                ),
                &Default::default(),
            )
            .unwrap()
            .snapshot();
            writer
                .coordinator_checkpoint()
                .checkpoint(CheckpointRequest {
                    session_id: id,
                    version: CheckpointVersion {
                        revision: 1,
                        epoch: 1,
                    },
                    snapshot: Arc::new(SessionCheckpoint {
                        history: Some(Arc::new(vec![maki_providers::Message::user(
                            "coordinator".into(),
                        )])),
                        model: Arc::from(MODEL),
                        cwd: CWD.into(),
                        options,
                    }),
                })
                .await
                .unwrap();

            session.replace_messages(vec![maki_providers::Message::user("tui".into())]);
            writer.send(Arc::new(session));
            writer.shutdown(DRAIN_TIMEOUT);

            let loaded = AppSession::load(id, &dir).unwrap();
            assert_eq!(message_texts(&loaded), ["tui"]);
        });
    }

    #[test]
    fn finalize_ack_failure_after_write_compensates_before_later_app_save() {
        smol::block_on(async {
            const ACK_FAILURE: &str = "ack lost after write";
            let (_tmp, dir) = state_dir();
            let (writer, _warn_rx) = writer(&dir);
            let mut session = AppSession::new(PREVIOUS_MODEL, CWD);
            session.push_message(user_message(0));
            let id = session.id;
            writer.seed(Arc::new(session.clone()));
            let fail_once = Arc::new(AtomicBool::new(true));
            let backend = writer.coordinator_checkpoint();
            let checkpoint: Arc<dyn CheckpointWriter<SessionCheckpoint>> = Arc::new({
                let fail_once = Arc::clone(&fail_once);
                move |request: CheckpointRequest<SessionCheckpoint>| {
                    let ack = backend.checkpoint(request);
                    let fail = fail_once.swap(false, Ordering::AcqRel);
                    Box::pin(async move {
                        let saved = ack.await?;
                        if fail {
                            Err(CheckpointError::Save {
                                session_id: saved.session_id,
                                message: Arc::from(ACK_FAILURE),
                            })
                        } else {
                            Ok(saved)
                        }
                    }) as CheckpointFuture
                }
            });
            let coordinator = coordinator_with_checkpoint(&session, checkpoint);
            let before = coordinator.read().options();
            let receipt = coordinator
                .set_model_if_active(Arc::from(ADOPTED_MODEL), Arc::new(AtomicBool::new(true)))
                .await
                .unwrap();
            assert!(matches!(
                coordinator.finalize_model_if_active(receipt).await,
                Err(SessionCoordinatorError::Checkpoint(_))
            ));
            assert_eq!(coordinator.read().model().as_ref(), PREVIOUS_MODEL);
            assert_eq!(coordinator.read().options(), before);
            assert_eq!(writer.latest_snapshot(id).unwrap().model, PREVIOUS_MODEL);
            assert_eq!(AppSession::load(id, &dir).unwrap().model, PREVIOUS_MODEL);

            session.set_title(TITLE.into());
            writer.send(Arc::new(session));
            coordinator.close().await.unwrap();
            writer.shutdown(DRAIN_TIMEOUT);
            let loaded = AppSession::load(id, &dir).unwrap();
            assert_eq!(loaded.model, PREVIOUS_MODEL);
            assert_eq!(loaded.title, TITLE);
            assert_eq!(message_texts(&loaded), [msg_text(0)]);
        });
    }

    #[test]
    fn two_failed_finalize_acks_rebase_next_app_save() {
        smol::block_on(async {
            const ACK_FAILURE: &str = "ack lost after write";
            const FAILED_ACKS: usize = 2;
            let (_tmp, dir) = state_dir();
            let (writer, _warn_rx) = writer(&dir);
            let mut session = AppSession::new(PREVIOUS_MODEL, CWD);
            session.push_message(user_message(0));
            let id = session.id;
            writer.seed(Arc::new(session.clone()));
            let seen = Arc::new(Mutex::new(Vec::new()));
            let backend = writer.coordinator_checkpoint();
            let checkpoint: Arc<dyn CheckpointWriter<SessionCheckpoint>> = Arc::new({
                let seen = Arc::clone(&seen);
                move |request: CheckpointRequest<SessionCheckpoint>| {
                    let version = request.version;
                    let ack = backend.checkpoint(request);
                    let seen = Arc::clone(&seen);
                    Box::pin(async move {
                        let saved = ack.await?;
                        let mut versions = seen.lock().unwrap();
                        versions.push(version);
                        if versions.len() <= FAILED_ACKS {
                            Err(CheckpointError::Save {
                                session_id: saved.session_id,
                                message: Arc::from(ACK_FAILURE),
                            })
                        } else {
                            Ok(saved)
                        }
                    }) as CheckpointFuture
                }
            });
            let coordinator = coordinator_with_checkpoint(&session, checkpoint);
            let before = coordinator.read().options();
            let receipt = coordinator
                .set_model_if_active(Arc::from(ADOPTED_MODEL), Arc::new(AtomicBool::new(true)))
                .await
                .unwrap();
            assert!(matches!(
                coordinator.finalize_model_if_active(receipt).await,
                Err(SessionCoordinatorError::Checkpoint(_))
            ));
            {
                let versions = seen.lock().unwrap();
                assert_eq!(versions.len(), FAILED_ACKS);
                assert_eq!(versions[1].epoch, CHECKPOINT_REBASE_EPOCH);
                assert!(versions[1].revision > versions[0].revision);
            }
            assert_eq!(AppSession::load(id, &dir).unwrap().model, PREVIOUS_MODEL);
            assert_eq!(writer.latest_snapshot(id).unwrap().model, PREVIOUS_MODEL);
            assert_eq!(coordinator.read().model().as_ref(), PREVIOUS_MODEL);
            assert_eq!(coordinator.read().options(), before);

            session.set_title(TITLE.into());
            writer.send(Arc::new(session));
            coordinator.close().await.unwrap();
            writer.shutdown(DRAIN_TIMEOUT);
            let loaded = AppSession::load(id, &dir).unwrap();
            assert_eq!(loaded.model, PREVIOUS_MODEL);
            assert_eq!(loaded.title, TITLE);
            assert_eq!(message_texts(&loaded), [msg_text(0)]);
        });
    }

    #[test]
    fn expiry_after_candidate_write_rebases_even_when_compensation_ack_fails() {
        smol::block_on(async {
            const ACK_FAILURE: &str = "compensating ack lost";
            let (_tmp, dir) = state_dir();
            let (writer, _warn_rx) = writer(&dir);
            let mut session = AppSession::new(PREVIOUS_MODEL, CWD);
            session.push_message(user_message(0));
            let id = session.id;
            writer.seed(Arc::new(session.clone()));
            let active = Arc::new(AtomicBool::new(true));
            let backend = writer.coordinator_checkpoint();
            let checkpoint: Arc<dyn CheckpointWriter<SessionCheckpoint>> = Arc::new({
                let active = Arc::clone(&active);
                move |request: CheckpointRequest<SessionCheckpoint>| {
                    let rebase = request.version.epoch == CHECKPOINT_REBASE_EPOCH;
                    let ack = backend.checkpoint(request);
                    let active = Arc::clone(&active);
                    Box::pin(async move {
                        let saved = ack.await?;
                        if rebase {
                            Err(CheckpointError::Save {
                                session_id: saved.session_id,
                                message: Arc::from(ACK_FAILURE),
                            })
                        } else {
                            active.store(false, Ordering::Release);
                            Ok(saved)
                        }
                    }) as CheckpointFuture
                }
            });
            let coordinator = coordinator_with_checkpoint(&session, checkpoint);
            let receipt = coordinator
                .set_model_if_active(Arc::from(ADOPTED_MODEL), active)
                .await
                .unwrap();
            assert!(matches!(
                coordinator.finalize_model_if_active(receipt).await,
                Err(SessionCoordinatorError::Checkpoint(_))
            ));
            assert_eq!(writer.latest_snapshot(id).unwrap().model, PREVIOUS_MODEL);
            assert_eq!(coordinator.read().model().as_ref(), PREVIOUS_MODEL);
            session.set_title(TITLE.into());
            writer.send(Arc::new(session));
            coordinator.close().await.unwrap();
            writer.shutdown(DRAIN_TIMEOUT);
            let loaded = AppSession::load(id, &dir).unwrap();
            assert_eq!(loaded.model, PREVIOUS_MODEL);
            assert_eq!(loaded.title, TITLE);
        });
    }

    #[test]
    fn failed_coordinator_checkpoint_is_not_a_later_tui_merge_base() {
        smol::block_on(async {
            let (_tmp, dir) = state_dir();
            block_sessions_dir(&dir);
            let (writer, warn_rx) = writer(&dir);
            let session = AppSession::new(MODEL, CWD);
            let id = session.id;
            writer.send(Arc::new(session.clone()));
            let warning = warn_rx.recv_async().await.unwrap();
            assert!(warning.starts_with(SAVE_FAILED_PREFIX), "{warning}");

            let options = maki_agent::session_options::SessionOptions::new(
                maki_agent::session_coordinator::builtin_option_definitions(
                    FAILED_MODEL,
                    [Arc::from(FAILED_MODEL)],
                    true,
                    true,
                    true,
                    maki_agent::ThinkingConfig::Off,
                ),
                &Default::default(),
            )
            .unwrap()
            .snapshot();
            let result = writer
                .coordinator_checkpoint()
                .checkpoint(CheckpointRequest {
                    session_id: id,
                    version: CheckpointVersion {
                        revision: 1,
                        epoch: 1,
                    },
                    snapshot: Arc::new(SessionCheckpoint {
                        history: None,
                        model: Arc::from(FAILED_MODEL),
                        cwd: CWD.into(),
                        options,
                    }),
                })
                .await;
            assert!(matches!(result, Err(CheckpointError::Save { .. })));

            std::fs::remove_file(dir.path().join(SESSIONS_DIR)).unwrap();
            writer.send(Arc::new(session));
            writer.shutdown(DRAIN_TIMEOUT);

            let loaded = AppSession::load(id, &dir).unwrap();
            assert_eq!(loaded.model, MODEL);
            assert!(loaded.meta.yolo.is_none());
            assert!(!loaded.meta.fast);
            assert!(!loaded.meta.workflow);
            assert!(loaded.meta.session_options.is_empty());
        });
    }

    #[test]
    fn failed_checkpoint_retries_without_another_wake() {
        smol::block_on(async {
            let (_tmp, dir) = state_dir();
            block_sessions_dir(&dir);
            let (writer, warn_rx) = writer(&dir);
            let session = AppSession::new(MODEL, CWD);
            let id = session.id;
            let version = CheckpointVersion {
                revision: session.revision(),
                epoch: 1,
            };
            let ack = writer.checkpoint(CheckpointRequest {
                session_id: id,
                version,
                snapshot: Arc::new(session),
            });

            let warning = warn_rx.recv_async().await.unwrap();
            assert!(warning.starts_with(SAVE_FAILED_PREFIX), "{warning}");
            std::fs::remove_file(dir.path().join(SESSIONS_DIR)).unwrap();

            assert_eq!(ack.await.unwrap().version, version);
            assert_eq!(warn_rx.recv_async().await.unwrap(), SAVE_RECOVERED);
            writer.shutdown(DRAIN_TIMEOUT);
            assert!(AppSession::load(id, &dir).is_ok());
        });
    }

    #[test]
    fn failed_checkpoint_does_not_delay_another_session() {
        smol::block_on(async {
            let (_tmp, dir) = state_dir();
            let sessions_dir = dir.path().join(SESSIONS_DIR);
            std::fs::create_dir(&sessions_dir).unwrap();
            let (writer, warn_rx) = writer(&dir);
            let blocked = AppSession::new(MODEL, CWD);
            let blocked_id = blocked.id;
            let blocked_tmp = sessions_dir.join(format!("{blocked_id}.jsonl.tmp"));
            std::fs::create_dir(&blocked_tmp).unwrap();
            let blocked_ack = writer.checkpoint(CheckpointRequest {
                session_id: blocked_id,
                version: CheckpointVersion {
                    revision: blocked.revision(),
                    epoch: 1,
                },
                snapshot: Arc::new(blocked),
            });
            let warning = warn_rx.recv_async().await.unwrap();
            assert!(warning.starts_with(SAVE_FAILED_PREFIX), "{warning}");
            let blocked_retry_at = match lock(&writer.pending).entries.get(&blocked_id) {
                Some(Entry::Save(save)) => {
                    save.retry_at.expect("blocked session must have retry_at")
                }
                _ => panic!("blocked session entry missing"),
            };

            let healthy = AppSession::new(MODEL, CWD);
            let healthy_id = healthy.id;
            let healthy_version = CheckpointVersion {
                revision: healthy.revision(),
                epoch: 1,
            };
            let healthy_ack = writer.checkpoint(CheckpointRequest {
                session_id: healthy_id,
                version: healthy_version,
                snapshot: Arc::new(healthy),
            });
            let ack = healthy_ack
                .await
                .expect("healthy checkpoint was delayed or failed");
            assert_eq!(ack.version, healthy_version);
            assert!(
                Instant::now() < blocked_retry_at,
                "healthy checkpoint was delayed past the retry backoff of the failed session"
            );

            std::fs::remove_dir(blocked_tmp).unwrap();
            assert!(blocked_ack.await.is_ok());
            writer.shutdown(DRAIN_TIMEOUT);
            assert!(AppSession::load(healthy_id, &dir).is_ok());
            assert!(AppSession::load(blocked_id, &dir).is_ok());
        });
    }

    #[test]
    fn failed_checkpoint_exhausts_retries_without_false_ack() {
        smol::block_on(async {
            let (_tmp, dir) = state_dir();
            block_sessions_dir(&dir);
            let (writer, warn_rx) = writer(&dir);
            let session = AppSession::new(MODEL, CWD);
            let id = session.id;
            let result = writer
                .checkpoint(CheckpointRequest {
                    session_id: id,
                    version: CheckpointVersion {
                        revision: session.revision(),
                        epoch: 1,
                    },
                    snapshot: Arc::new(session),
                })
                .await;

            assert!(matches!(
                result,
                Err(CheckpointError::Save { session_id, .. }) if session_id == id
            ));
            let warning = warn_rx.recv_async().await.unwrap();
            assert!(warning.starts_with(SAVE_FAILED_PREFIX), "{warning}");
            writer.shutdown(DRAIN_TIMEOUT);
        });
    }

    #[test]
    fn delete_discards_pending_snapshot() {
        let (_tmp, dir) = state_dir();
        let (writer, _warn_rx) = writer(&dir);
        let session = AppSession::new("test-model", "/tmp/c");
        let id = session.id;
        writer.send(Arc::new(session));
        let (done_tx, done_rx) = flume::bounded(1);
        writer.delete(id, move |res| {
            let _ = done_tx.send(res);
        });
        writer.shutdown(DRAIN_TIMEOUT);

        assert!(done_rx.recv().unwrap().is_ok());
        assert!(AppSession::load(id, &dir).is_err());
    }

    /// A fresh writer over an existing file has no cursor, so it re-opens the
    /// log and gets cursors for the loaded session, not the live one. The first
    /// append must diverge into a full rewrite instead of landing on stale
    /// offsets.
    #[test]
    fn reopened_log_rewrites_diverged_file_instead_of_appending() {
        let (_tmp, dir) = state_dir();
        let mut session = AppSession::new(MODEL, CWD);
        let id = session.id;
        for i in 0..5 {
            session.push_message(user_message(i));
        }
        let (first, _first_warn_rx) = writer(&dir);
        first.send(Arc::new(session.clone()));
        first.shutdown(DRAIN_TIMEOUT);

        session.truncate_messages(2);
        session.push_message(maki_providers::Message::user(RESUMED_MSG.into()));
        session.insert_tool_output(
            TOOL_ID.into(),
            maki_agent::ToolOutput::Plain(TOOL_TEXT.to_string().into()),
        );
        session.set_title(TITLE.into());

        let (second, second_warn_rx) = writer(&dir);
        second.send(Arc::new(session.clone()));
        second.shutdown(DRAIN_TIMEOUT);

        let loaded = AppSession::load(id, &dir).unwrap();
        assert_eq!(
            message_texts(&loaded),
            [msg_text(0), msg_text(1), RESUMED_MSG.to_string()]
        );
        assert_eq!(loaded.title, TITLE);
        match loaded.tool_outputs().get(TOOL_ID).map(Arc::as_ref) {
            Some(maki_agent::ToolOutput::Plain(out)) => assert_eq!(out.text, TOOL_TEXT),
            other => panic!("tool output lost: {other:?}"),
        }
        assert!(second_warn_rx.is_empty());
    }

    /// A disk that keeps failing warns once, not once per frame, and says so
    /// exactly once when writes start working again.
    #[test]
    fn failing_flush_warns_once_and_reports_recovery() {
        let (_tmp, dir) = state_dir();
        block_sessions_dir(&dir);
        let (writer, warn_rx) = writer(&dir);
        let session = Arc::new(AppSession::new(MODEL, CWD));
        let id = session.id;

        writer.send(Arc::clone(&session));
        let warning = warn_rx.recv_timeout(DRAIN_TIMEOUT).unwrap();
        assert!(warning.starts_with(SAVE_FAILED_PREFIX), "{warning}");

        // The save is enqueued before the delete, so the flush that runs the
        // delete has already drained it; a repeat failure must stay silent.
        writer.send(Arc::clone(&session));
        let (done_tx, done_rx) = flume::bounded(1);
        writer.delete(MakiId::generate(), move |res| {
            let _ = done_tx.send(res);
        });
        assert!(done_rx.recv_timeout(DRAIN_TIMEOUT).unwrap().is_err());
        assert!(warn_rx.is_empty(), "second failure warned again");

        std::fs::remove_file(dir.path().join(SESSIONS_DIR)).unwrap();
        writer.send(session);
        let recovered = warn_rx.recv_timeout(DRAIN_TIMEOUT).unwrap();
        assert_eq!(recovered, SAVE_RECOVERED);
        writer.shutdown(DRAIN_TIMEOUT);

        assert!(warn_rx.is_empty());
        assert!(AppSession::load(id, &dir).is_ok());
    }

    /// A save enqueued after a delete must win: clear a draft and retype it
    /// fast enough, and the queued delete used to unlink the file the retype
    /// had just saved.
    #[test]
    fn save_enqueued_after_delete_survives() {
        let (_tmp, dir) = state_dir();
        let (writer, warn_rx) = writer(&dir);
        let mut session = AppSession::new(MODEL, CWD);
        let id = session.id;
        session.push_message(user_message(0));
        writer.send(Arc::new(session.clone()));
        writer.delete(id, |_| {});
        session.push_message(maki_providers::Message::user(RESUMED_MSG.into()));
        writer.send(Arc::new(session));
        writer.shutdown(DRAIN_TIMEOUT);

        let loaded = AppSession::load(id, &dir).unwrap();
        assert_eq!(
            message_texts(&loaded),
            [msg_text(0), RESUMED_MSG.to_string()]
        );
        assert!(warn_rx.is_empty());
    }

    #[test]
    fn failed_write_is_retried_without_an_external_wake() {
        let (_tmp, dir) = state_dir();
        block_sessions_dir(&dir);
        let (writer, warn_rx) = writer(&dir);
        let session = Arc::new(AppSession::new(MODEL, CWD));
        let id = session.id;

        writer.send(session);
        let warning = warn_rx.recv_timeout(DRAIN_TIMEOUT).unwrap();
        assert!(warning.starts_with(SAVE_FAILED_PREFIX), "{warning}");

        std::fs::remove_file(dir.path().join(SESSIONS_DIR)).unwrap();
        assert_eq!(warn_rx.recv_timeout(DRAIN_TIMEOUT).unwrap(), SAVE_RECOVERED);
        assert!(AppSession::load(id, &dir).is_ok());
        writer.shutdown(DRAIN_TIMEOUT);
    }

    /// After a delete the cursor still holds an open handle to the unlinked
    /// file, which still looks unchanged, so an append would write the session
    /// into nothing. Forgetting the cursor makes the next snapshot write a
    /// whole file.
    #[test]
    fn session_recreated_after_delete_is_written_in_full() {
        let (_tmp, dir) = state_dir();
        let (writer, warn_rx) = writer(&dir);
        let mut session = AppSession::new(MODEL, CWD);
        let id = session.id;
        session.push_message(user_message(0));
        writer.send(Arc::new(session.clone()));

        let (done_tx, done_rx) = flume::bounded(1);
        writer.delete(id, move |res| {
            let _ = done_tx.send(res);
        });
        done_rx.recv_timeout(DRAIN_TIMEOUT).unwrap().unwrap();
        assert!(AppSession::load(id, &dir).is_err());

        session.push_message(maki_providers::Message::user(RESUMED_MSG.into()));
        writer.send(Arc::new(session));
        writer.shutdown(DRAIN_TIMEOUT);

        let loaded = AppSession::load(id, &dir).unwrap();
        assert_eq!(
            message_texts(&loaded),
            [msg_text(0), RESUMED_MSG.to_string()]
        );
        assert!(warn_rx.is_empty());
    }
}
