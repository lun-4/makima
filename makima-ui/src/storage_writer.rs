//! Coalescing write-behind cache with incremental JSONL persistence.
//!
//! Apps post session snapshots keyed by session id; the writer thread drains
//! the newest snapshot of every session per wake and performs O(delta)
//! appends. Deletes travel through the same per-session slot as saves, so
//! whichever the app asked for last is what reaches disk.

use std::collections::{HashMap, HashSet};
use std::io;
use std::mem;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use async_lock::RwLockReadGuardArc;
use makima_storage::checkpoint::{
    CheckpointAck, CheckpointClaim, CheckpointError, CheckpointFuture, CheckpointPublication,
    CheckpointRequest, CheckpointVersion, CheckpointWriter,
};
use makima_storage::id::MakimaId;
use makima_storage::session_lock::SessionPublicationGuard;
use makima_storage::session_state::StoredBranch;
use makima_storage::sessions::{
    HistoryDestination, HistoryIdentity, HistorySnapshot, PublicationOutcome, SESSIONS_DIR,
    SessionError, SessionLog, StoredSubagent,
};
use makima_storage::{StateDir, StorageError};
use tracing::warn;

use makima_agent::actor::EffectiveAgentConfig;
use makima_agent::journal::{SessionJournal, SessionJournalSnapshot};
use makima_agent::session_checkpoint::{apply_session_checkpoint, validate_checkpoint_history};
use makima_agent::session_coordinator::{
    SessionCheckpoint, SessionCheckpointKind, publication_claim,
};
use makima_agent::session_recovery::{RecoveredSession, RecoveryCandidate};
use makima_agent::tools::offload::OffloadStore;

use crate::AppSession;
use makima_agent::CancelToken;
use makima_providers::Message;

pub(crate) const SAVE_FAILED_PREFIX: &str = "Session save failed";
const SAVE_RECOVERED: &str = "Session save recovered";
const RESTORE_UNSYNCED: &str =
    "Session restore is live, but directory sync failed; retrying durability confirmation";
const RESTORE_SYNC_RECOVERED: &str = "Session restore durability confirmed";
const RESTORE_SYNC_ABANDONED: &str =
    "Session restore durability is unconfirmed; session lock was lost";
const JOURNAL_UNSAVED: &str = "changes applied but not saved";
const JOURNAL_POLL_INTERVAL: Duration = Duration::from_secs(1);
const CHECKPOINT_REPLACED: &str = "checkpoint payload superseded by authoritative session state";
const CHECKPOINT_CANCELLED: &str = "session checkpoint cancelled";
pub(crate) const LOCK_LOST_REPLACEMENT_ERR: &str = "session lock was lost; replacement is disabled";
const CONFIG_REQUIRES_CLAIM: &str = "configuration checkpoints require a publication claim";
const SNAPSHOT_UNAVAILABLE: &str = "session snapshot is unavailable";
const STAGED_HISTORY_INCOMPLETE: &str = "staged history requires a destination and exact snapshot";
const SELECTED_CONFIG_DESTINATION: &str =
    "selected config requires its child checkpoint destination";
const SELECTED_CONFIG_MISSING: &str = "selected child config requires existing stored metadata";
const RESTORE_PROJECTION_MISMATCH: &str =
    "restore session does not match its exact journal or branch";
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
#[cfg(test)]
type RestoreSyncGate = (flume::Sender<MakimaId>, flume::Receiver<Result<(), String>>);

#[derive(Default)]
struct PendingState {
    closed: bool,
    entries: HashMap<MakimaId, Entry>,
    /// Latest authoritative state from the app or a successful coordinator
    /// checkpoint. It may still be newer than durable storage.
    latest: HashMap<MakimaId, Arc<AppSession>>,
    acknowledged: HashMap<MakimaId, Arc<AppSession>>,
    latest_generations: HashMap<MakimaId, u64>,
    coordinator_pending: HashMap<MakimaId, CoordinatorPending>,
    coordinator_history_bases: HashMap<MakimaId, HistoryIdentity>,
    config_versions: HashMap<MakimaId, (u64, u64)>,
    history_versions: HashMap<MakimaId, HashMap<Option<String>, (u64, u64)>>,
    live_children: HashMap<MakimaId, Vec<String>>,
    journals: HashMap<MakimaId, JournalCheckpoint>,
    prepared_histories: HashMap<MakimaId, Vec<(HistoryDestination, HistorySnapshot<Message>)>>,
    next_generation: u64,
    staged_checkpoints: Vec<StagedCheckpointSave>,
    recoveries: Vec<RecoverySave>,
    recovery_rollbacks: Vec<RecoveryRollback>,
    recovery_transactions: HashMap<MakimaId, Arc<()>>,
    publications: HashMap<MakimaId, Vec<CheckpointPublication>>,
    incarnations: HashMap<MakimaId, Arc<()>>,
    authoritative_sessions: HashSet<MakimaId>,
    save_incarnations: HashMap<MakimaId, Arc<()>>,
    restore_checkpoints: Vec<RestoreCheckpointSave>,
    publication_syncs: HashMap<MakimaId, PublicationSync>,
    publication_sync_failures: HashMap<MakimaId, String>,
    publication_guards: HashMap<MakimaId, SessionPublicationGuard>,
    write_barrier: Arc<Mutex<()>>,
    current_checkpoints: Vec<CurrentCheckpointSave>,
    generation_floors: HashMap<MakimaId, u64>,
    #[cfg(test)]
    restore_sync_error: Option<StorageError>,
    #[cfg(test)]
    current_sync_error: Option<StorageError>,
    #[cfg(test)]
    restore_sync_gate: Option<RestoreSyncGate>,
    #[cfg(test)]
    child_stage_gate: Option<(flume::Sender<()>, flume::Receiver<()>)>,
    #[cfg(test)]
    save_gate: Option<(flume::Sender<()>, flume::Receiver<()>)>,
}

pub struct StagedSessionCheckpoint {
    pub session_id: MakimaId,
    pub checkpoint: Option<Arc<SessionCheckpoint>>,
    pub journal: Option<SessionJournalSnapshot>,
    pub selected_config: Option<(HistoryDestination, Arc<EffectiveAgentConfig>)>,
    pub history: Option<(HistoryDestination, HistorySnapshot<Message>)>,
    pub publication: CheckpointPublication,
    pub claim: CheckpointClaim,
    pub guard: SessionPublicationGuard,
}

pub struct CurrentSessionCheckpoint {
    pub session: Arc<AppSession>,
    pub journal: SessionJournalSnapshot,
    pub publication: CheckpointPublication,
    pub claim: CheckpointClaim,
    pub guard: SessionPublicationGuard,
    pub fence: SessionSaveFence,
}

struct CurrentCheckpointSave {
    request: CurrentSessionCheckpoint,
    journal: JournalCheckpoint,
    incarnation: Arc<()>,
    generation: Option<u64>,
    reply: flume::Sender<Result<(), String>>,
}

pub struct RestoredSessionCheckpoint {
    pub session_id: MakimaId,
    pub session: Arc<AppSession>,
    pub journal: SessionJournalSnapshot,
    pub branch: StoredBranch,
    pub publication: CheckpointPublication,
    pub claim: CheckpointClaim,
    pub guard: SessionPublicationGuard,
}

#[derive(Debug, Clone)]
pub struct SessionSaveFence {
    session_id: MakimaId,
    incarnation: Arc<()>,
}

struct PublicationSync {
    guard: SessionPublicationGuard,
    retry_at: Instant,
    retry_attempt: usize,
    waiters: Vec<flume::Sender<Result<(), String>>>,
}

struct RestoreCheckpointSave {
    request: RestoredSessionCheckpoint,
    journal: JournalCheckpoint,
    incarnation: Arc<()>,
    generation: Option<u64>,
    reply: flume::Sender<Result<SessionSaveFence, String>>,
}

#[derive(Clone)]
struct JournalCheckpoint {
    publication: Arc<Mutex<()>>,
    journal: Arc<SessionJournal>,
    snapshot: Arc<SessionJournalSnapshot>,
}

struct StagedCheckpointSave {
    journal: Option<JournalCheckpoint>,
    request: StagedSessionCheckpoint,
    incarnation: Arc<()>,
    config_version: Option<(u64, u64)>,
    history_version: Option<(u64, u64)>,
    reply: flume::Sender<Result<(), String>>,
}

struct RecoverySave {
    candidate: RecoveryCandidate,
    guard: SessionPublicationGuard,
    incarnation: Arc<()>,
    generation: Option<u64>,
    transactional: bool,
    wake: flume::Sender<()>,
    reply: flume::Sender<Result<RecoveryResult, CheckpointError>>,
}

struct RecoveryResult {
    recovered: RecoveredSession,
    publication: Option<RecoveryPublication>,
}

pub(crate) struct RecoveryPublication {
    pending: Pending,
    wake: flume::Sender<()>,
    id: MakimaId,
    incarnation: Arc<()>,
    guard: SessionPublicationGuard,
    previous: Arc<AppSession>,
    recovered: Arc<AppSession>,
    journal: Arc<SessionJournal>,
    _lane: Option<RwLockReadGuardArc<()>>,
}

struct RecoveryRollback {
    publication: RecoveryPublication,
    reply: flume::Sender<Result<(), CheckpointError>>,
}

impl RecoveryPublication {
    pub(crate) fn commit(self) -> Result<(), CheckpointError> {
        let mut state = lock(&self.pending);
        if state.closed
            || !state
                .incarnations
                .get(&self.id)
                .is_some_and(|current| Arc::ptr_eq(current, &self.incarnation))
            || !state
                .recovery_transactions
                .get(&self.id)
                .is_some_and(|current| Arc::ptr_eq(current, &self.incarnation))
        {
            return Err(CheckpointError::Save {
                session_id: self.id,
                message: Arc::from(CHECKPOINT_REPLACED),
            });
        }
        let failure = |message: String| CheckpointError::Save {
            session_id: self.id,
            message: Arc::from(message),
        };
        self.guard
            .publish(|| {
                install_recovered(
                    &mut state,
                    self.id,
                    Arc::clone(&self.recovered),
                    Arc::clone(&self.journal),
                );
                state.recovery_transactions.remove(&self.id);
            })
            .map_err(|error| failure(error.to_string()))?
            .ok_or_else(|| failure(LOCK_LOST_REPLACEMENT_ERR.into()))?;
        drop(state);
        let _ = self.wake.send(());
        Ok(())
    }

    pub(crate) async fn rollback(self) -> Result<(), CheckpointError> {
        let (reply, response) = flume::bounded(1);
        let id = self.id;
        let pending = Arc::clone(&self.pending);
        let wake = self.wake.clone();
        lock(&pending).recovery_rollbacks.push(RecoveryRollback {
            publication: self,
            reply,
        });
        wake.send(()).map_err(|_| CheckpointError::Closed(id))?;
        response
            .recv_async()
            .await
            .map_err(|_| CheckpointError::Closed(id))?
    }
}

struct CoordinatorPending {
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
    Delete(
        DeleteCallback,
        Option<SessionPublicationGuard>,
        Option<Arc<OffloadStore>>,
    ),
}

struct PendingSave {
    journal: Option<JournalCheckpoint>,
    session: Arc<AppSession>,
    waiters: Vec<CheckpointWaiter>,
    generation: u64,
    coordinator_generation: Option<u64>,
    config_version: Option<(u64, u64)>,
    coordinator_history_base: Option<HistoryIdentity>,
    retry_attempt: usize,
    retry_at: Option<Instant>,
}

enum RequiredHistory {
    Root(HistorySnapshot<Message>, Option<u64>),
    Child {
        key: String,
        history: HistorySnapshot<Message>,
    },
}

impl RequiredHistory {
    fn retained_by(&self, session: &AppSession) -> bool {
        match self {
            Self::Root(history, _) => {
                let saved = session.history_snapshot();
                saved.epoch == history.epoch
                    && saved.compacted == history.compacted
                    && saved.conversation == history.conversation
                    && Arc::ptr_eq(&history.messages, &saved.messages)
            }
            Self::Child { key, history } => {
                session.subagent_history_snapshot(key).is_some_and(|saved| {
                    saved.epoch == history.epoch && Arc::ptr_eq(&saved.messages, &history.messages)
                })
            }
        }
    }
}

struct CheckpointWaiter {
    version: CheckpointVersion,
    histories: Vec<RequiredHistory>,
    reply: flume::Sender<Result<CheckpointAck, CheckpointError>>,
}

fn required_histories(session: &AppSession) -> Vec<RequiredHistory> {
    let mut histories = vec![RequiredHistory::Root(session.history_snapshot(), None)];
    histories.extend(session.subagent_messages().keys().filter_map(|key| {
        session
            .subagent_history_snapshot(key)
            .map(|history| RequiredHistory::Child {
                key: key.clone(),
                history,
            })
    }));
    histories
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

/// The coordinator's writer. Guarded publication needs the session lock's
/// guard, which the runtime rebinds whenever its lock state changes.
pub(crate) struct CoordinatorCheckpointWriter {
    pending: Pending,
    wake: flume::Sender<()>,
    publication: Mutex<Option<SessionPublicationGuard>>,
    save_incarnations: Mutex<HashMap<MakimaId, Arc<()>>>,
}

impl CoordinatorCheckpointWriter {
    pub(crate) fn refresh_fence(&self, fence: &SessionSaveFence) {
        self.save_incarnations
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(fence.session_id, Arc::clone(&fence.incarnation));
    }

    pub(crate) fn bind(&self, guard: Option<SessionPublicationGuard>) {
        *self
            .publication
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = guard;
    }
}

impl CheckpointWriter<AppSession> for StorageWriter {
    fn checkpoint(&self, request: CheckpointRequest<AppSession>) -> CheckpointFuture {
        self.enqueue_checkpoint(request)
    }
}

impl CheckpointWriter<SessionCheckpoint> for CoordinatorCheckpointWriter {
    fn checkpoint_guarded(
        &self,
        request: CheckpointRequest<SessionCheckpoint>,
        publication: CheckpointPublication,
        claim: CheckpointClaim,
    ) -> CheckpointFuture {
        let session_id = request.session_id;
        let version = request.version;
        let Some(guard) = self
            .publication
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
        else {
            return Box::pin(async move {
                Err(CheckpointError::Save {
                    session_id,
                    message: Arc::from(LOCK_LOST_REPLACEMENT_ERR),
                })
            });
        };
        let fence = self
            .save_incarnations
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&session_id)
            .map(|incarnation| SessionSaveFence {
                session_id,
                incarnation: Arc::clone(incarnation),
            });
        let mut checkpoint = request.snapshot.as_ref().clone();
        let history = checkpoint
            .history
            .take()
            .filter(|_| checkpoint.kind != SessionCheckpointKind::Options)
            .map(|history| (HistoryDestination::Main, history));
        let response = enqueue_staged_checkpoint_inner(
            &self.pending,
            &self.wake,
            StagedSessionCheckpoint {
                session_id,
                checkpoint: Some(Arc::new(checkpoint)),
                journal: None,
                selected_config: None,
                history,
                publication,
                claim,
                guard,
            },
            fence.as_ref(),
        );
        Box::pin(async move {
            let response = response.map_err(|message| CheckpointError::Save {
                session_id,
                message: Arc::from(message),
            })?;
            response
                .recv_async()
                .await
                .map_err(|_| CheckpointError::Closed(session_id))?
                .map_err(|message| CheckpointError::Save {
                    session_id,
                    message: Arc::from(message),
                })?;
            Ok(CheckpointAck {
                session_id,
                version,
            })
        })
    }

    fn checkpoint(&self, request: CheckpointRequest<SessionCheckpoint>) -> CheckpointFuture {
        let session_id = request.session_id;
        let version = request.version;
        let fail = |message: &str| -> CheckpointFuture {
            let message = Arc::from(message);
            Box::pin(async move {
                Err(CheckpointError::Save {
                    session_id,
                    message,
                })
            })
        };
        if request.snapshot.journal.is_some() {
            return self.checkpoint_guarded(
                request,
                CheckpointPublication::default(),
                publication_claim(),
            );
        }
        if request.snapshot.kind == SessionCheckpointKind::Config {
            return fail(CONFIG_REQUIRES_CLAIM);
        }
        let journal = capture_journal(&self.pending, session_id);
        let mut state = lock(&self.pending);
        if state.closed {
            return Box::pin(async move { Err(CheckpointError::Closed(session_id)) });
        }
        let fence = self
            .save_incarnations
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&session_id)
            .map(|incarnation| SessionSaveFence {
                session_id,
                incarnation: Arc::clone(incarnation),
            });
        if !save_fence_current(&state, session_id, fence.as_ref()) {
            return fail(CHECKPOINT_REPLACED);
        }
        if let Err(message) =
            validate_checkpoint_history(&request.snapshot, request.snapshot.history.is_some())
        {
            return fail(message);
        }
        let history = request
            .snapshot
            .history
            .as_ref()
            .filter(|_| request.snapshot.kind != SessionCheckpointKind::Options);
        let incoming = history
            .zip(request.snapshot.history_revision)
            .map(|(history, revision)| (history.epoch, revision));
        if incoming.is_some_and(|incoming| {
            history_version(&state, session_id, &HistoryDestination::Main)
                .is_some_and(|current| incoming < current)
        }) {
            return fail(CHECKPOINT_REPLACED);
        }
        let (base, history_base) = if let Some(pending) = state.coordinator_pending.get(&session_id)
        {
            (Arc::clone(&pending.session), pending.history_base)
        } else if let Some(latest) = state.latest.get(&session_id) {
            (
                Arc::clone(latest),
                state.coordinator_history_bases.get(&session_id).copied(),
            )
        } else {
            return fail(SNAPSHOT_UNAVAILABLE);
        };
        let history_base =
            history_base.or_else(|| history.is_some().then(|| base.history_identity()));
        let live_ids = state.live_children.get(&session_id).map(Vec::as_slice);
        let mut merged = match merge_checkpoint(&base, &request.snapshot, live_ids) {
            Ok(session) => Arc::new(session),
            Err(error) => return fail(&error.to_string()),
        };
        if let Err(message) = project_journal(&state, session_id, journal.as_ref(), &mut merged) {
            return fail(&message);
        }
        remember_journal(&mut state, session_id, journal.as_ref());
        if let Some(incoming) = incoming {
            set_history_version(&mut state, session_id, &HistoryDestination::Main, incoming);
        }
        let generation = next_generation(&mut state);
        state.latest_generations.insert(session_id, generation);
        state.coordinator_pending.insert(
            session_id,
            CoordinatorPending {
                generation,
                session: Arc::clone(&merged),
                history_base,
            },
        );
        let (reply, response) = flume::bounded(1);
        let config_version = state.config_versions.get(&session_id).copied();
        let histories = history
            .map(|history| {
                RequiredHistory::Root(history.clone(), request.snapshot.history_revision)
            })
            .into_iter()
            .collect();
        enqueue_locked(
            &mut state.entries,
            PendingSave {
                journal,
                session: merged,
                waiters: vec![CheckpointWaiter {
                    version,
                    histories,
                    reply,
                }],
                generation,
                coordinator_generation: Some(generation),
                config_version,
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

    pub fn bind_journal(&self, id: MakimaId, journal: Arc<SessionJournal>) {
        if let Err(error) = journal.bind_session(id) {
            warn!(%error, session_id = %id, "session journal binding rejected");
            return;
        }
        let snapshot = Arc::new(journal.snapshot());
        let publication = lock(&self.pending)
            .journals
            .get(&id)
            .map(|bound| Arc::clone(&bound.publication))
            .unwrap_or_default();
        let _publication = publication.lock().unwrap_or_else(PoisonError::into_inner);
        let mut state = lock(&self.pending);
        if state.closed || state.recovery_transactions.contains_key(&id) {
            return;
        }
        if state.journals.get(&id).is_some_and(|bound| {
            Arc::ptr_eq(&bound.journal, &journal) && bound.snapshot.epoch == snapshot.epoch
        }) {
            return;
        }
        cancel_publications(&mut state, id);
        state.prepared_histories.remove(&id);
        state.journals.insert(
            id,
            JournalCheckpoint {
                publication: Arc::clone(&publication),
                journal,
                snapshot,
            },
        );
        drop(state);
        let _ = self.wake.send(());
    }

    pub fn bind_config_runtime(&self, id: MakimaId, runtime_epoch: u64) {
        let mut state = lock(&self.pending);
        if state.recovery_transactions.contains_key(&id) {
            return;
        }
        if state
            .config_versions
            .get(&id)
            .is_some_and(|(epoch, _)| *epoch >= runtime_epoch)
        {
            return;
        }
        cancel_publications(&mut state, id);
        state.config_versions.insert(id, (runtime_epoch, 0));
        state.coordinator_pending.remove(&id);
        let generation = next_generation(&mut state);
        state.latest_generations.insert(id, generation);
        let latest = state.latest.get(&id).cloned();
        if let Some(Entry::Save(save)) = state.entries.get_mut(&id) {
            fail_waiters(id, mem::take(&mut save.waiters), CHECKPOINT_REPLACED);
            if let Some(latest) = latest {
                save.session = latest;
            }
            save.generation = generation;
            save.config_version = Some((runtime_epoch, 0));
            save.coordinator_generation = None;
            save.coordinator_history_base = None;
        }
    }

    pub fn seed(&self, session: Arc<AppSession>) {
        let id = session.id;
        let mut state = lock(&self.pending);
        cancel_publications(&mut state, id);
        let generation = next_generation(&mut state);
        state.prepared_histories.remove(&id);
        state.acknowledged.insert(id, Arc::clone(&session));
        state.latest.insert(id, session);
        state.latest_generations.insert(id, generation);
    }

    pub fn set_live_children(&self, session_id: MakimaId, live_ids: Vec<String>) {
        let mut state = lock(&self.pending);
        if !state.closed {
            state.live_children.insert(session_id, live_ids);
        }
    }

    pub fn session_save_fence(&self, id: MakimaId) -> SessionSaveFence {
        SessionSaveFence {
            session_id: id,
            incarnation: Arc::clone(lock(&self.pending).save_incarnations.entry(id).or_default()),
        }
    }

    pub fn send(&self, session: Arc<AppSession>) {
        self.send_inner(session, None);
    }

    pub fn send_fenced(&self, session: Arc<AppSession>, fence: &SessionSaveFence) {
        self.send_inner(session, Some(fence));
    }

    fn send_inner(&self, session: Arc<AppSession>, fence: Option<&SessionSaveFence>) {
        let id = session.id;
        let incoming_history = session.history_identity();
        let journal = capture_journal(&self.pending, id);
        let mut state = lock(&self.pending);
        if state.closed || !save_fence_current(&state, id, fence) {
            return;
        }
        let preserve_history = state
            .coordinator_history_bases
            .get(&id)
            .is_some_and(|base| incoming_history == *base);
        if !preserve_history {
            state.coordinator_history_bases.remove(&id);
        }
        let mut authoritative = state
            .latest
            .get(&id)
            .map(|latest| {
                let mut merged = merge_tui_snapshot(
                    &session,
                    latest,
                    preserve_history,
                    state.live_children.get(&id).map(Vec::as_slice),
                );
                if state.config_versions.contains_key(&id) && merged.meta.mode != latest.meta.mode {
                    let merged = Arc::make_mut(&mut merged);
                    merged.meta.mode = latest.meta.mode.clone();
                }
                merged
            })
            .unwrap_or(session);
        if let Err(error) = project_journal(&state, id, journal.as_ref(), &mut authoritative) {
            warn!(%error, session_id = %id, "session journal projection rejected");
            return;
        }
        remember_journal(&mut state, id, journal.as_ref());
        let generation = next_generation(&mut state);
        state.latest.insert(id, Arc::clone(&authoritative));
        state.latest_generations.insert(id, generation);
        let coordinator_generation = state
            .coordinator_pending
            .contains_key(&id)
            .then_some(generation);
        let live_ids = state.live_children.get(&id).cloned();
        let (mut session, coordinator_history_base) = state
            .coordinator_pending
            .get_mut(&id)
            .map(|pending| {
                if pending
                    .history_base
                    .is_some_and(|base| base != authoritative.history_identity())
                {
                    pending.history_base = None;
                }
                let merged = merge_tui_snapshot(
                    &authoritative,
                    &pending.session,
                    pending.history_base.is_some(),
                    live_ids.as_deref(),
                );
                pending.generation = coordinator_generation.unwrap();
                pending.session = Arc::clone(&merged);
                (merged, pending.history_base)
            })
            .unwrap_or((authoritative, None));
        if let Err(error) = project_journal(&state, id, journal.as_ref(), &mut session) {
            warn!(%error, session_id = %id, "session journal projection rejected");
            return;
        }
        let config_version = state.config_versions.get(&id).copied();
        enqueue_locked(
            &mut state.entries,
            PendingSave {
                journal,
                session,
                waiters: Vec::new(),
                config_version,
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

    #[cfg(test)]
    pub async fn checkpoint_staged(&self, request: StagedSessionCheckpoint) -> Result<(), String> {
        enqueue_staged_checkpoint(&self.pending, &self.wake, request)?
            .recv_async()
            .await
            .map_err(|error| error.to_string())?
    }

    pub async fn checkpoint_staged_fenced(
        &self,
        request: StagedSessionCheckpoint,
        fence: &SessionSaveFence,
    ) -> Result<(), String> {
        enqueue_staged_checkpoint_inner(&self.pending, &self.wake, request, Some(fence))?
            .recv_async()
            .await
            .map_err(|error| error.to_string())?
    }

    pub fn bind_publication_guard(&self, id: MakimaId, guard: SessionPublicationGuard) {
        let barrier = Arc::clone(&lock(&self.pending).write_barrier);
        let _barrier = barrier.lock().unwrap_or_else(PoisonError::into_inner);
        lock(&self.pending).publication_guards.insert(id, guard);
    }

    #[cfg(test)]
    pub async fn checkpoint_fenced_guarded(
        &self,
        request: CheckpointRequest<AppSession>,
        fence: &SessionSaveFence,
        guard: SessionPublicationGuard,
    ) -> Result<(), String> {
        if request.session_id != request.snapshot.id {
            return Err(RESTORE_PROJECTION_MISMATCH.into());
        }
        let journal =
            capture_journal(&self.pending, request.session_id).ok_or(SNAPSHOT_UNAVAILABLE)?;
        let mut session = request.snapshot;
        project_journal(
            &lock(&self.pending),
            request.session_id,
            Some(&journal),
            &mut session,
        )?;
        self.checkpoint_current_guarded(CurrentSessionCheckpoint {
            session,
            journal: journal.snapshot.as_ref().clone(),
            publication: CheckpointPublication::default(),
            claim: publication_claim(),
            guard,
            fence: fence.clone(),
        })
        .await
    }

    pub async fn checkpoint_current_guarded(
        &self,
        request: CurrentSessionCheckpoint,
    ) -> Result<(), String> {
        self.bind_publication_guard(request.session.id, request.guard.clone());
        enqueue_current_checkpoint(&self.pending, &self.wake, request)?
            .recv_async()
            .await
            .map_err(|error| error.to_string())?
    }

    pub async fn checkpoint_restore(
        &self,
        request: RestoredSessionCheckpoint,
    ) -> Result<SessionSaveFence, String> {
        enqueue_restore_checkpoint(&self.pending, &self.wake, request)?
            .recv_async()
            .await
            .map_err(|error| error.to_string())?
    }

    pub async fn await_publication_sync(
        &self,
        fence: &SessionSaveFence,
        cancel: &CancelToken,
    ) -> Result<(), String> {
        cancel
            .race(async {
                #[cfg(test)]
                {
                    let gate = {
                        let state = lock(&self.pending);
                        state
                            .publication_syncs
                            .contains_key(&fence.session_id)
                            .then(|| state.restore_sync_gate.clone())
                            .flatten()
                    };
                    if let Some((entered, release)) = gate {
                        let _ = entered.send(fence.session_id);
                        release
                            .recv_async()
                            .await
                            .map_err(|error| error.to_string())??;
                    }
                }
                publication_sync_waiter(&self.pending, fence)?
                    .recv_async()
                    .await
                    .map_err(|error| error.to_string())?
            })
            .await?
    }

    #[cfg(test)]
    pub(crate) fn hold_restore_sync(
        &self,
        error: &str,
    ) -> (flume::Receiver<MakimaId>, flume::Sender<Result<(), String>>) {
        let (entered, observed) = flume::bounded(1);
        let (release, gate) = flume::bounded(1);
        let mut state = lock(&self.pending);
        state.restore_sync_error = Some(StorageError::from(io::Error::other(error.to_owned())));
        state.restore_sync_gate = Some((entered, gate));
        (observed, release)
    }

    #[cfg(test)]
    pub(crate) fn hold_current_sync(
        &self,
        error: &str,
    ) -> (flume::Receiver<MakimaId>, flume::Sender<Result<(), String>>) {
        let gates = self.hold_restore_sync(error);
        let mut state = lock(&self.pending);
        state.current_sync_error = state.restore_sync_error.take();
        gates
    }

    #[cfg(test)]
    pub(crate) fn release_restore_sync(&self) {
        lock(&self.pending).restore_sync_gate = None;
        let _ = self.wake.send(());
    }

    pub async fn checkpoint_files_restore(
        &self,
        request: RestoredSessionCheckpoint,
    ) -> Result<SessionSaveFence, String> {
        let response = enqueue_restore_checkpoint_inner(&self.pending, &self.wake, request, true)?;
        response
            .recv_async()
            .await
            .map_err(|_| writer_gone().to_string())?
    }

    pub async fn recover_checkpoint(
        &self,
        candidate: RecoveryCandidate,
        guard: SessionPublicationGuard,
    ) -> Result<RecoveredSession, CheckpointError> {
        Ok(self
            .enqueue_recovery(candidate, guard, false)
            .await?
            .recovered)
    }

    pub(crate) async fn recover_replacing(
        &self,
        candidate: RecoveryCandidate,
        guard: SessionPublicationGuard,
    ) -> Result<(RecoveredSession, RecoveryPublication), CheckpointError> {
        let result = self.enqueue_recovery(candidate, guard, true).await?;
        Ok((
            result.recovered,
            result.publication.expect("replacement publication"),
        ))
    }

    async fn enqueue_recovery(
        &self,
        candidate: RecoveryCandidate,
        guard: SessionPublicationGuard,
        transactional: bool,
    ) -> Result<RecoveryResult, CheckpointError> {
        let id = candidate.session().id;
        let (reply, response) = flume::bounded(1);
        {
            let mut state = lock(&self.pending);
            if state.closed {
                return Err(CheckpointError::Closed(id));
            }
            let incarnation = Arc::clone(state.incarnations.entry(id).or_default());
            let generation = state.latest_generations.get(&id).copied();
            state.recoveries.push(RecoverySave {
                candidate,
                guard,
                incarnation,
                generation,
                transactional,
                wake: self.wake.clone(),
                reply,
            });
        }
        self.wake
            .send(())
            .map_err(|_| CheckpointError::Closed(id))?;
        response
            .recv_async()
            .await
            .map_err(|_| CheckpointError::Closed(id))?
    }

    pub(crate) fn coordinator_checkpoint(&self) -> Arc<CoordinatorCheckpointWriter> {
        Arc::new(CoordinatorCheckpointWriter {
            pending: Arc::clone(&self.pending),
            wake: self.wake.clone(),
            publication: Mutex::default(),
            save_incarnations: Mutex::new(lock(&self.pending).save_incarnations.clone()),
        })
    }

    #[cfg(test)]
    pub(crate) fn latest_snapshot(&self, id: MakimaId) -> Option<Arc<AppSession>> {
        lock(&self.pending).latest.get(&id).cloned()
    }

    #[cfg(test)]
    pub fn checkpoint_fenced(
        &self,
        request: CheckpointRequest<AppSession>,
        fence: &SessionSaveFence,
    ) -> CheckpointFuture {
        self.enqueue_checkpoint_inner(request, Some(fence))
    }

    fn enqueue_checkpoint(&self, request: CheckpointRequest<AppSession>) -> CheckpointFuture {
        self.enqueue_checkpoint_inner(request, None)
    }

    fn enqueue_checkpoint_inner(
        &self,
        request: CheckpointRequest<AppSession>,
        fence: Option<&SessionSaveFence>,
    ) -> CheckpointFuture {
        let session_id = request.session_id;
        let version = request.version;
        let (reply, response) = flume::bounded(1);
        let journal = capture_journal(&self.pending, session_id);
        let mut state = lock(&self.pending);
        if state.closed {
            return Box::pin(async move { Err(CheckpointError::Closed(session_id)) });
        }
        if !save_fence_current(&state, session_id, fence) {
            return Box::pin(async move {
                Err(CheckpointError::Save {
                    session_id,
                    message: Arc::from(CHECKPOINT_REPLACED),
                })
            });
        }
        let generation = next_generation(&mut state);
        let config_version = state.config_versions.get(&session_id).copied();
        let histories = required_histories(&request.snapshot);
        let mut snapshot = if config_version.is_some() {
            state
                .latest
                .get(&session_id)
                .map(|latest| {
                    let mut merged = merge_tui_snapshot(&request.snapshot, latest, false, None);
                    if merged.meta.mode != latest.meta.mode {
                        let merged = Arc::make_mut(&mut merged);
                        merged.meta.mode = latest.meta.mode.clone();
                    }
                    merged
                })
                .unwrap_or(request.snapshot)
        } else {
            let mut snapshot = request.snapshot;
            if let Some(latest) = state.latest.get(&session_id) {
                preserve_newer_child_histories(&mut snapshot, latest);
                preserve_child_identity(&mut snapshot, latest);
                if snapshot.history_snapshot().epoch < latest.history_snapshot().epoch {
                    let session = Arc::make_mut(&mut snapshot);
                    session.adopt_history(&latest.history_snapshot());
                    prune_main_history(
                        session,
                        state.live_children.get(&session_id).map(Vec::as_slice),
                    );
                }
            }
            snapshot
        };
        if let Err(message) = project_journal(&state, session_id, journal.as_ref(), &mut snapshot) {
            return Box::pin(async move {
                Err(CheckpointError::Save {
                    session_id,
                    message: Arc::from(message),
                })
            });
        }
        let histories = if journal.is_some() {
            required_histories(&snapshot)
        } else {
            histories
        };
        remember_journal(&mut state, session_id, journal.as_ref());
        state.latest.insert(session_id, Arc::clone(&snapshot));
        state.latest_generations.insert(session_id, generation);
        enqueue_locked(
            &mut state.entries,
            PendingSave {
                journal,
                session: snapshot,
                config_version,
                waiters: vec![CheckpointWaiter {
                    version,
                    histories,
                    reply,
                }],
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
    pub fn forget(&self, id: MakimaId) {
        let mut state = lock(&self.pending);
        cancel_publications(&mut state, id);
        state.latest.remove(&id);
        state.acknowledged.remove(&id);
        state.latest_generations.remove(&id);
        state.coordinator_pending.remove(&id);
        state.coordinator_history_bases.remove(&id);
        state.config_versions.remove(&id);
        state.live_children.remove(&id);
        state.journals.remove(&id);
        state.prepared_histories.remove(&id);
    }

    /// Removes an empty session's files while keeping its in-memory snapshot.
    /// The session is still live in its tab, and its coordinator checkpoints by
    /// merging into that snapshot, so forgetting it would leave the next
    /// checkpoint with no base to merge into. The files come back if the
    /// coordinator later has something worth saving.
    pub fn delete_empty(&self, id: MakimaId) {
        self.delete_inner(id, |_| {}, false);
    }

    /// Delete a session's files on the writer thread; `done` fires there, so
    /// callers never block on disk. Deleting a session that was never written
    /// reports success, and a save enqueued afterwards supersedes the delete.
    /// The session is forgotten entirely: this is the user asking for it to be
    /// gone, not the cleanup of a session that has not earned a file yet.
    #[cfg(test)]
    pub fn delete(
        &self,
        id: MakimaId,
        done: impl FnOnce(Result<(), SessionError>) + Send + 'static,
    ) {
        self.delete_inner(id, done, true);
    }

    fn delete_inner(
        &self,
        id: MakimaId,
        done: impl FnOnce(Result<(), SessionError>) + Send + 'static,
        forget_snapshot: bool,
    ) {
        self.delete_inner_guarded(id, done, forget_snapshot, None, None);
    }

    fn delete_inner_guarded(
        &self,
        id: MakimaId,
        done: impl FnOnce(Result<(), SessionError>) + Send + 'static,
        forget_snapshot: bool,
        guard: Option<SessionPublicationGuard>,
        store: Option<Arc<OffloadStore>>,
    ) {
        let mut state = lock(&self.pending);
        if state.closed {
            drop(state);
            done(Err(writer_gone()));
            return;
        }
        cancel_publications(&mut state, id);
        if forget_snapshot {
            state.latest.remove(&id);
            state.acknowledged.remove(&id);
            state.latest_generations.remove(&id);
            state.coordinator_pending.remove(&id);
            state.coordinator_history_bases.remove(&id);
            state.config_versions.remove(&id);
            state.live_children.remove(&id);
            state.journals.remove(&id);
            state.prepared_histories.remove(&id);
            state.recovery_transactions.remove(&id);
        }
        let replaced = state
            .entries
            .insert(id, Entry::Delete(Box::new(done), guard, store));
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
                Entry::Delete(done, _, _) => done(Err(writer_gone())),
                Entry::Save(save) => fail_waiters(id, save.waiters, "storage writer unavailable"),
            }
        }
    }

    pub fn delete_guarded(
        &self,
        id: MakimaId,
        guard: SessionPublicationGuard,
        store: Option<Arc<OffloadStore>>,
        done: impl FnOnce(Result<(), SessionError>) + Send + 'static,
    ) {
        self.delete_inner_guarded(id, done, true, Some(guard), store);
    }

    pub fn shutdown(&self, timeout: Duration) {
        {
            let mut state = lock(&self.pending);
            state.closed = true;
            for publications in state.publications.values() {
                for publication in publications {
                    publication.cancel();
                }
            }
            self.stop.store(true, Ordering::Release);
        }
        // Wake it so it observes the flag; the final flush still runs.
        let _ = self.wake.send(());
        if self.done_rx.recv_timeout(timeout).is_err() {
            warn!("storage writer did not drain within {timeout:?}");
        }
    }
}

fn save_fence_current(
    state: &PendingState,
    id: MakimaId,
    fence: Option<&SessionSaveFence>,
) -> bool {
    match fence {
        Some(fence) => {
            fence.session_id == id
                && state
                    .save_incarnations
                    .get(&id)
                    .is_some_and(|current| Arc::ptr_eq(current, &fence.incarnation))
        }
        None => !state.authoritative_sessions.contains(&id),
    }
}

fn enqueue_current_checkpoint(
    pending: &Pending,
    wake: &flume::Sender<()>,
    request: CurrentSessionCheckpoint,
) -> Result<flume::Receiver<Result<(), String>>, String> {
    let id = request.session.id;
    if request.session.state() != request.journal.state
        || request.session.agent_revisions() != request.journal.agent_revisions
    {
        return Err(RESTORE_PROJECTION_MISMATCH.into());
    }
    let journal = capture_journal(pending, id).ok_or(SNAPSHOT_UNAVAILABLE)?;
    let (reply, response) = flume::bounded(1);
    {
        let mut state = lock(pending);
        if state.closed
            || state.recovery_transactions.contains_key(&id)
            || !save_fence_current(&state, id, Some(&request.fence))
            || !journal_current(&state, id, Some(&journal))
            || journal.snapshot.epoch != request.journal.epoch
            || journal.snapshot.revision != request.journal.revision
        {
            return Err(CHECKPOINT_REPLACED.into());
        }
        if matches!(state.entries.get(&id), Some(Entry::Delete(_, _, _))) {
            return Err(CHECKPOINT_REPLACED.into());
        }
        if let Some(Entry::Save(save)) = state.entries.remove(&id) {
            fail_waiters(id, save.waiters, CHECKPOINT_REPLACED);
        }
        state.coordinator_pending.remove(&id);
        if let Some(publications) = state.publications.remove(&id) {
            for publication in publications {
                publication.cancel();
            }
        }
        let incarnation = Arc::clone(state.incarnations.entry(id).or_default());
        let generation = state.latest_generations.get(&id).copied();
        state
            .publications
            .entry(id)
            .or_default()
            .push(request.publication.clone());
        state.current_checkpoints.push(CurrentCheckpointSave {
            request,
            journal,
            incarnation,
            generation,
            reply,
        });
    }
    wake.send(()).map_err(|error| error.to_string())?;
    Ok(response)
}

fn publication_sync_waiter(
    pending: &Pending,
    fence: &SessionSaveFence,
) -> Result<flume::Receiver<Result<(), String>>, String> {
    let (reply, response) = flume::bounded(1);
    let mut state = lock(pending);
    if !save_fence_current(&state, fence.session_id, Some(fence)) {
        return Err(CHECKPOINT_REPLACED.into());
    }
    if let Some(error) = state.publication_sync_failures.get(&fence.session_id) {
        return Err(error.clone());
    }
    if let Some(sync) = state.publication_syncs.get_mut(&fence.session_id) {
        sync.waiters.push(reply);
    } else {
        let _ = reply.send(Ok(()));
    }
    Ok(response)
}

fn enqueue_restore_checkpoint(
    pending: &Pending,
    wake: &flume::Sender<()>,
    request: RestoredSessionCheckpoint,
) -> Result<flume::Receiver<Result<SessionSaveFence, String>>, String> {
    enqueue_restore_checkpoint_inner(pending, wake, request, false)
}

fn enqueue_restore_checkpoint_inner(
    pending: &Pending,
    wake: &flume::Sender<()>,
    request: RestoredSessionCheckpoint,
    files_only: bool,
) -> Result<flume::Receiver<Result<SessionSaveFence, String>>, String> {
    let id = request.session_id;
    if request.session.id != id
        || request.session.state() != request.journal.state
        || request.session.agent_revisions() != request.journal.agent_revisions
        || (!files_only && !request.journal.authoritative)
        || !request.session.branches().contains(&request.branch)
    {
        return Err(RESTORE_PROJECTION_MISMATCH.into());
    }
    let mut projection = request.session.as_ref().clone();
    request
        .journal
        .apply_to(&mut projection)
        .map_err(|error| error.to_string())?;
    if request.journal.histories.iter().any(|(id, history)| {
        let Some(agent) = request.session.agents().get(id) else {
            return true;
        };
        let epoch = if agent.transcript_key.is_empty() {
            Some(request.session.history_snapshot().epoch)
        } else {
            request
                .session
                .subagent_history_snapshot(&agent.transcript_key)
                .map(|history| history.epoch)
        };
        epoch != Some(history.epoch)
    }) || serde_json::to_value(projection.history_snapshot().messages.as_ref())
        .map_err(|error| error.to_string())?
        != serde_json::to_value(request.session.messages()).map_err(|error| error.to_string())?
        || serde_json::to_value(projection.subagent_messages())
            .map_err(|error| error.to_string())?
            != serde_json::to_value(request.session.subagent_messages())
                .map_err(|error| error.to_string())?
    {
        return Err(RESTORE_PROJECTION_MISMATCH.into());
    }
    let journal = capture_journal(pending, id).ok_or(SNAPSHOT_UNAVAILABLE)?;
    let (reply, response) = flume::bounded(1);
    {
        let mut state = lock(pending);
        if state.closed {
            return Err(CheckpointError::Closed(id).to_string());
        }
        if state.recovery_transactions.contains_key(&id)
            || !journal_current(&state, id, Some(&journal))
            || if files_only {
                journal.snapshot.epoch != request.journal.epoch
                    || journal.snapshot.revision != request.journal.revision
                    || journal.snapshot.seq != request.journal.seq
                    || journal.snapshot.state != request.journal.state
                    || journal.snapshot.agent_revisions != request.journal.agent_revisions
                    || request
                        .branch
                        .transition
                        .as_ref()
                        .is_none_or(|reference| !request.session.restore_refs().contains(reference))
            } else {
                journal.snapshot.epoch == request.journal.epoch
                    || journal.snapshot.revision >= request.journal.revision
            }
        {
            return Err(CHECKPOINT_REPLACED.into());
        }
        let incarnation = Arc::clone(state.incarnations.entry(id).or_default());
        let generation = state.latest_generations.get(&id).copied();
        state
            .publications
            .entry(id)
            .or_default()
            .push(request.publication.clone());
        state.restore_checkpoints.push(RestoreCheckpointSave {
            request,
            journal,
            incarnation,
            generation,
            reply,
        });
    }
    wake.send(()).map_err(|error| error.to_string())?;
    Ok(response)
}

fn install_recovered(
    state: &mut PendingState,
    id: MakimaId,
    session: Arc<AppSession>,
    journal: Arc<SessionJournal>,
) {
    cancel_publications(state, id);
    if let Some(Entry::Save(save)) = state.entries.remove(&id) {
        fail_waiters(id, save.waiters, CHECKPOINT_REPLACED);
    }
    state.coordinator_pending.remove(&id);
    state.coordinator_history_bases.remove(&id);
    state.config_versions.remove(&id);
    state.live_children.remove(&id);
    state.prepared_histories.remove(&id);
    state.acknowledged.insert(id, Arc::clone(&session));
    state.latest.insert(id, session);
    let generation = next_generation(state);
    state.latest_generations.insert(id, generation);
    state.journals.insert(
        id,
        JournalCheckpoint {
            publication: Arc::default(),
            snapshot: Arc::new(journal.snapshot()),
            journal,
        },
    );
}

fn history_version(
    state: &PendingState,
    id: MakimaId,
    destination: &HistoryDestination,
) -> Option<(u64, u64)> {
    let versions = state.history_versions.get(&id)?;
    match destination {
        HistoryDestination::Main => versions.get(&None),
        HistoryDestination::Child(key) => versions.get(&Some(key.clone())),
    }
    .copied()
}

fn set_history_version(
    state: &mut PendingState,
    id: MakimaId,
    destination: &HistoryDestination,
    version: (u64, u64),
) {
    let key = match destination {
        HistoryDestination::Main => None,
        HistoryDestination::Child(key) => Some(key.clone()),
    };
    state
        .history_versions
        .entry(id)
        .or_default()
        .entry(key)
        .and_modify(|current| *current = (*current).max(version))
        .or_insert(version);
}

#[cfg(test)]
fn enqueue_staged_checkpoint(
    pending: &Pending,
    wake: &flume::Sender<()>,
    request: StagedSessionCheckpoint,
) -> Result<flume::Receiver<Result<(), String>>, String> {
    enqueue_staged_checkpoint_inner(pending, wake, request, None)
}

fn enqueue_staged_checkpoint_inner(
    pending: &Pending,
    wake: &flume::Sender<()>,
    request: StagedSessionCheckpoint,
    fence: Option<&SessionSaveFence>,
) -> Result<flume::Receiver<Result<(), String>>, String> {
    let session_id = request.session_id;
    let (reply, response) = flume::bounded(1);
    let journal = capture_journal(pending, session_id);
    {
        let mut state = lock(pending);
        if state.recovery_transactions.contains_key(&session_id)
            || !save_fence_current(&state, session_id, fence)
        {
            return Err(CHECKPOINT_REPLACED.into());
        }
        if !journal_current(&state, session_id, journal.as_ref()) {
            return Err(CHECKPOINT_REPLACED.into());
        }
        if state.closed {
            return Err(CheckpointError::Closed(session_id).to_string());
        }
        if request
            .checkpoint
            .as_ref()
            .is_some_and(|checkpoint| checkpoint.history.is_some())
        {
            return Err(STAGED_HISTORY_INCOMPLETE.into());
        }
        if let Some(checkpoint) = &request.checkpoint {
            validate_checkpoint_history(checkpoint, request.history.is_some())
                .map_err(str::to_owned)?;
        }
        if let Some((destination, _)) = &request.selected_config
            && (!matches!(destination, HistoryDestination::Child(_))
                || request
                    .history
                    .as_ref()
                    .is_some_and(|(history_destination, _)| history_destination != destination)
                || request
                    .checkpoint
                    .as_ref()
                    .is_some_and(|checkpoint| checkpoint.kind != SessionCheckpointKind::History))
        {
            return Err(SELECTED_CONFIG_DESTINATION.into());
        }
        let config_version = state.config_versions.get(&session_id).copied();
        if request
            .checkpoint
            .as_ref()
            .filter(|checkpoint| checkpoint.kind == SessionCheckpointKind::Config)
            .and_then(|checkpoint| checkpoint.config.as_ref())
            .is_some_and(|config| {
                config_version
                    .is_some_and(|current| (config.runtime_epoch, config.generation) < current)
            })
        {
            return Err(CHECKPOINT_REPLACED.into());
        }
        let incoming_history_version =
            request.history.as_ref().and_then(|(destination, history)| {
                let revision = request.checkpoint.as_ref()?.history_revision?;
                Some((destination, (history.epoch, revision)))
            });
        if incoming_history_version.is_some_and(|(destination, incoming)| {
            history_version(&state, session_id, destination)
                .is_some_and(|current| incoming < current)
        }) {
            return Err(CHECKPOINT_REPLACED.into());
        }
        let history_version = incoming_history_version.map(|(_, version)| version);
        let incarnation = Arc::clone(state.incarnations.entry(session_id).or_default());
        state
            .publications
            .entry(session_id)
            .or_default()
            .push(request.publication.clone());
        remember_journal(&mut state, session_id, journal.as_ref());
        state.staged_checkpoints.push(StagedCheckpointSave {
            journal,
            request,
            incarnation,
            config_version,
            history_version,
            reply,
        });
    }
    wake.send(()).map_err(|error| error.to_string())?;
    Ok(response)
}

fn capture_journal(pending: &Pending, id: MakimaId) -> Option<JournalCheckpoint> {
    let bound = lock(pending).journals.get(&id).cloned()?;
    let snapshot = Arc::new(bound.journal.snapshot());
    Some(JournalCheckpoint {
        publication: bound.publication,
        journal: bound.journal,
        snapshot,
    })
}

fn journal_current(
    state: &PendingState,
    id: MakimaId,
    captured: Option<&JournalCheckpoint>,
) -> bool {
    match (state.journals.get(&id), captured) {
        (None, None) => true,
        (Some(bound), Some(captured)) => {
            Arc::ptr_eq(&bound.journal, &captured.journal)
                && Arc::ptr_eq(&bound.publication, &captured.publication)
                && bound.snapshot.epoch == captured.snapshot.epoch
                && bound.snapshot.revision <= captured.snapshot.revision
        }
        _ => false,
    }
}

fn project_journal(
    state: &PendingState,
    id: MakimaId,
    captured: Option<&JournalCheckpoint>,
    session: &mut Arc<AppSession>,
) -> Result<(), String> {
    if state.recovery_transactions.contains_key(&id) {
        return Err(CHECKPOINT_REPLACED.into());
    }
    if !journal_current(state, id, captured) {
        return Err(CHECKPOINT_REPLACED.into());
    }
    if let Some(captured) = captured {
        if let Some(error) = journal_persistence_error(captured) {
            return Err(error);
        }
        let projected = Arc::make_mut(session);
        projected.replace_state(captured.snapshot.state.clone());
        projected.replace_agent_revisions(captured.snapshot.agent_revisions.clone());
        captured
            .snapshot
            .apply_to(projected)
            .map_err(|error| error.to_string())?;
        if let Some(histories) = state.prepared_histories.get(&id) {
            for (destination, history) in histories {
                if !journal_has_history(&captured.snapshot, destination, history) {
                    adopt_checkpoint_history(
                        Arc::make_mut(session),
                        destination,
                        history,
                        state.live_children.get(&id).map(Vec::as_slice),
                    );
                }
            }
        }
    }
    Ok(())
}

fn prepared_journal(request: &StagedSessionCheckpoint) -> Option<&SessionJournalSnapshot> {
    request
        .journal
        .as_ref()
        .or_else(|| request.checkpoint.as_ref()?.journal.as_ref())
}

fn project_staged_journal(
    state: &PendingState,
    save: &StagedCheckpointSave,
    session: &mut Arc<AppSession>,
) -> Result<(), String> {
    let Some(snapshot) = prepared_journal(&save.request) else {
        return project_journal(
            state,
            save.request.session_id,
            save.journal.as_ref(),
            session,
        );
    };
    if !journal_current(state, save.request.session_id, save.journal.as_ref())
        || save.journal.as_ref().is_none_or(|journal| {
            journal.snapshot.epoch != snapshot.epoch
                || journal.snapshot.revision > snapshot.revision
        })
    {
        return Err(CHECKPOINT_REPLACED.into());
    }
    let session = Arc::make_mut(session);
    session.replace_state(snapshot.state.clone());
    session.replace_agent_revisions(snapshot.agent_revisions.clone());
    snapshot
        .apply_to(session)
        .map_err(|error| error.to_string())
}

fn journal_has_history(
    snapshot: &SessionJournalSnapshot,
    destination: &HistoryDestination,
    history: &HistorySnapshot<Message>,
) -> bool {
    snapshot.agent_revisions.iter().rev().any(|(_, agent)| {
        let matches = match destination {
            HistoryDestination::Main => agent.transcript_key.is_empty(),
            HistoryDestination::Child(key) => agent.transcript_key == *key,
        };
        matches
            && snapshot
                .histories
                .get(&agent.id)
                .is_some_and(|current| current.epoch >= history.epoch)
    })
}

fn journal_prefix_superseded(captured: Option<&JournalCheckpoint>) -> bool {
    captured
        .is_some_and(|captured| captured.snapshot.revision < captured.journal.published_revision())
}

fn journal_persistence_error(captured: &JournalCheckpoint) -> Option<String> {
    captured
        .journal
        .persistence_error()
        .map(|error| format!("{JOURNAL_UNSAVED}: {error}"))
}

fn remember_journal(state: &mut PendingState, id: MakimaId, captured: Option<&JournalCheckpoint>) {
    if let Some(captured) = captured {
        if let Some(histories) = state.prepared_histories.get_mut(&id) {
            histories.retain(|(destination, history)| {
                !journal_has_history(&captured.snapshot, destination, history)
            });
        }
        state.journals.insert(id, captured.clone());
    }
}

fn acknowledge_journal(captured: Option<&JournalCheckpoint>) {
    if let Some(captured) = captured
        && let Err(error) = captured.journal.acknowledge(&captured.snapshot)
    {
        warn!(%error, "journal checkpoint acknowledgement rejected");
    }
}

fn lock(pending: &Pending) -> std::sync::MutexGuard<'_, PendingState> {
    pending.lock().unwrap_or_else(|e| e.into_inner())
}

fn cancel_publications(state: &mut PendingState, id: MakimaId) {
    state.history_versions.remove(&id);
    state.incarnations.insert(id, Arc::new(()));
    if let Some(publications) = state.publications.remove(&id) {
        for publication in publications {
            publication.cancel();
        }
    }
}

fn next_generation(state: &mut PendingState) -> u64 {
    let generation = state.next_generation;
    state.next_generation = generation.wrapping_add(1);
    generation
}

fn enqueue_locked(entries: &mut HashMap<MakimaId, Entry>, mut save: PendingSave) {
    let id = save.session.id;
    if let Some(Entry::Save(mut previous)) = entries.remove(&id) {
        retain_history_waiters(id, &save.session, &mut previous.waiters);
        if !previous.waiters.is_empty() {
            save.retry_attempt = previous.retry_attempt;
            save.retry_at = previous.retry_at;
        }
        save.waiters.extend(previous.waiters);
    }
    entries.insert(id, Entry::Save(save));
}

fn wake_checkpoint(pending: &Pending, wake: &flume::Sender<()>, id: MakimaId) {
    if wake.send(()).is_err()
        && let Some(Entry::Save(save)) = lock(pending).entries.remove(&id)
    {
        discard_coordinator_candidate(pending, id, &save);
        fail_waiters(id, save.waiters, "storage writer unavailable");
    }
}

fn commit_save(pending: &Pending, id: MakimaId, save: &PendingSave) {
    lock(pending)
        .acknowledged
        .insert(id, Arc::clone(&save.session));
    let Some(generation) = save.coordinator_generation else {
        return;
    };
    let mut state = lock(pending);
    let Some(latest) = state.latest.get(&id) else {
        return;
    };
    let preserve_root = save.coordinator_history_base.is_some_and(|base| {
        latest.history_identity() == base
            || Arc::ptr_eq(
                &latest.history_snapshot().messages,
                &save.session.history_snapshot().messages,
            )
    });
    let live_ids = state.live_children.get(&id).map(Vec::as_slice);
    let mut authoritative = if state.config_versions.get(&id).copied() == save.config_version {
        merge_tui_snapshot(latest, &save.session, false, live_ids)
    } else {
        Arc::clone(latest)
    };
    preserve_newer_child_histories(&mut authoritative, &save.session);
    if preserve_root {
        let authoritative = Arc::make_mut(&mut authoritative);
        authoritative.adopt_history(&save.session.history_snapshot());
        prune_main_history(authoritative, live_ids);
    }
    let committed_base = preserve_root
        .then_some(save.coordinator_history_base)
        .flatten();
    state.latest.insert(id, Arc::clone(&authoritative));
    if let Some(history_base) = committed_base {
        state
            .coordinator_history_bases
            .entry(id)
            .or_insert(history_base);
    }
    let candidate_committed = state.coordinator_pending.get(&id).is_some_and(|candidate| {
        candidate.generation == generation
            || (same_histories(&candidate.session, &authoritative)
                && same_plan_metadata(&candidate.session, &authoritative))
    });
    if candidate_committed {
        state.coordinator_pending.remove(&id);
        if let Some(Entry::Save(queued)) = state.entries.get_mut(&id) {
            queued.coordinator_generation = None;
            queued.coordinator_history_base = committed_base;
        }
    } else if let Some(candidate) = state.coordinator_pending.get_mut(&id) {
        if candidate.history_base == save.coordinator_history_base && preserve_root {
            candidate.history_base = Some(authoritative.history_identity());
        }
        let candidate_generation = candidate.generation;
        let history_base = candidate.history_base;
        let candidate_session = Arc::clone(&candidate.session);
        if let Some(Entry::Save(queued)) = state.entries.get_mut(&id)
            && queued.coordinator_generation == Some(candidate_generation)
        {
            queued.coordinator_history_base = history_base;
            queued.session = candidate_session;
        }
    }
}

fn same_histories(left: &AppSession, right: &AppSession) -> bool {
    left.subagent_messages().len() == right.subagent_messages().len()
        && required_histories(left)
            .iter()
            .all(|history| history.retained_by(right))
}

fn discard_coordinator_candidate(pending: &Pending, id: MakimaId, save: &PendingSave) {
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
    id: MakimaId,
    mut save: PendingSave,
) -> Result<(), Vec<CheckpointWaiter>> {
    let mut state = lock(pending);
    if !journal_current(&state, id, save.journal.as_ref())
        || state
            .generation_floors
            .get(&id)
            .is_some_and(|floor| save.generation < *floor)
    {
        fail_waiters(id, mem::take(&mut save.waiters), CHECKPOINT_REPLACED);
        return Ok(());
    }
    let current_config = state.config_versions.get(&id).copied();
    let history_retained = save
        .config_version
        .zip(current_config)
        .is_some_and(|(saved, current)| saved.0 == current.0)
        && matches!(
            state.entries.get(&id),
            Some(Entry::Save(newer))
                if newer.session.history_identity() == save.session.history_identity()
        );
    if current_config.is_some_and(|version| Some(version) != save.config_version)
        && !history_retained
    {
        if let Some(latest) = state.latest.get(&id) {
            fail_waiters(id, mem::take(&mut save.waiters), CHECKPOINT_REPLACED);
            save.session = Arc::clone(latest);
            save.generation = state.latest_generations[&id];
            save.config_version = state.config_versions.get(&id).copied();
            save.coordinator_generation = None;
            save.coordinator_history_base = None;
        } else {
            return Err(save.waiters);
        }
    }
    match state.entries.remove(&id) {
        Some(Entry::Save(mut newer)) => {
            retain_history_waiters(id, &newer.session, &mut save.waiters);
            if !save.waiters.is_empty() {
                newer.retry_attempt = newer.retry_attempt.max(save.retry_attempt);
                newer.retry_at = save.retry_at;
            }
            newer.waiters.extend(save.waiters);
            state.entries.insert(id, Entry::Save(newer));
            Ok(())
        }
        Some(Entry::Delete(done, guard, store)) => {
            state.entries.insert(id, Entry::Delete(done, guard, store));
            Err(save.waiters)
        }
        None => {
            state.entries.insert(id, Entry::Save(save));
            Ok(())
        }
    }
}

fn preserve_newer_child_histories(incoming: &mut Arc<AppSession>, latest: &AppSession) {
    for key in latest.subagent_messages().keys() {
        let Some(history) = latest.subagent_history_snapshot(key) else {
            continue;
        };
        if incoming
            .subagent_history_snapshot(key)
            .is_none_or(|incoming| {
                incoming.epoch < history.epoch
                    || (incoming.epoch == history.epoch
                        && incoming.messages.len() < history.messages.len())
            })
        {
            let incoming = Arc::make_mut(incoming);
            incoming.adopt_subagent_history(key.clone(), &history);
            incoming.prune_posts(Some(key), history.messages.len());
        }
    }
}

fn adopt_checkpoint_history(
    session: &mut AppSession,
    destination: &HistoryDestination,
    history: &HistorySnapshot<Message>,
    live_ids: Option<&[String]>,
) -> bool {
    match destination {
        HistoryDestination::Main => {
            if session.history_snapshot().epoch > history.epoch {
                return false;
            }
            session.adopt_history(history);
            prune_main_history(session, live_ids);
        }
        HistoryDestination::Child(key) => {
            if !session.adopt_subagent_history(key.clone(), history) {
                return false;
            }
            session.prune_posts(Some(key), history.messages.len());
        }
    }
    true
}

fn child_identity(child: &StoredSubagent) -> StoredSubagent {
    StoredSubagent {
        model: None,
        thinking: None,
        fast: false,
        ..child.clone()
    }
}

fn preserve_child_identity(incoming: &mut Arc<AppSession>, latest: &AppSession) {
    let mut subagents = incoming.subagents().to_vec();
    let mut incoming_ids = HashSet::new();
    let incoming_duplicates: HashSet<_> = subagents
        .iter()
        .filter_map(|child| {
            (!incoming_ids.insert(&child.tool_use_id)).then_some(&child.tool_use_id)
        })
        .cloned()
        .collect();
    let mut latest_ids = HashSet::new();
    let latest_duplicates: HashSet<_> = latest
        .subagents()
        .iter()
        .filter_map(|child| (!latest_ids.insert(&child.tool_use_id)).then_some(&child.tool_use_id))
        .cloned()
        .collect();
    let mut agent_keys: HashMap<&str, HashSet<&str>> = HashMap::new();
    for child in incoming.subagents().iter().chain(latest.subagents()) {
        if let Some(agent_id) = child.agent_id.as_deref() {
            agent_keys
                .entry(agent_id)
                .or_default()
                .insert(&child.tool_use_id);
        }
    }
    let colliding_keys: HashSet<_> = agent_keys
        .values()
        .filter(|keys| keys.len() > 1)
        .flat_map(|keys| keys.iter().copied())
        .collect();
    for child in &mut subagents {
        if incoming_duplicates.contains(&child.tool_use_id)
            || latest_duplicates.contains(&child.tool_use_id)
            || colliding_keys.contains(child.tool_use_id.as_str())
            || (child.agent_id.is_some()
                && latest.subagent_messages().contains_key(&child.tool_use_id)
                && !latest_ids.contains(&child.tool_use_id))
        {
            child.ambiguous = true;
            child.agent_id = None;
        }
    }
    for current in latest.subagents() {
        if let Some(child) = subagents
            .iter_mut()
            .find(|child| child.tool_use_id == current.tool_use_id)
        {
            child.ambiguous |= current.ambiguous
                || matches!((&child.agent_id, &current.agent_id), (Some(a), Some(b)) if a != b)
                || (child.agent_id.is_none()
                    && current.agent_id.is_some()
                    && incoming
                        .subagent_messages()
                        .contains_key(&child.tool_use_id))
                || (current.agent_id.is_none()
                    && child.agent_id.is_some()
                    && latest.subagent_messages().contains_key(&child.tool_use_id));
            if child.ambiguous {
                child.agent_id = None;
            } else if child.agent_id.is_none() {
                child.agent_id.clone_from(&current.agent_id);
            }
        } else if incoming
            .subagent_messages()
            .contains_key(&current.tool_use_id)
            || latest
                .subagent_messages()
                .contains_key(&current.tool_use_id)
            || current.ambiguous
            || latest_duplicates.contains(&current.tool_use_id)
            || colliding_keys.contains(current.tool_use_id.as_str())
        {
            let mut child = child_identity(current);
            if (child.agent_id.is_some()
                && incoming
                    .subagent_messages()
                    .contains_key(&current.tool_use_id))
                || latest_duplicates.contains(&child.tool_use_id)
                || colliding_keys.contains(child.tool_use_id.as_str())
            {
                child.ambiguous = true;
                child.agent_id = None;
            }
            subagents.push(child);
        }
    }
    if subagents != incoming.subagents() {
        Arc::make_mut(incoming).set_subagents(subagents);
    }
}

fn promote_staged_checkpoint(
    session: &mut Arc<AppSession>,
    request: &StagedSessionCheckpoint,
    published: &AppSession,
    live_ids: Option<&[String]>,
) -> Result<(), SessionError> {
    if let Some(checkpoint) = &request.checkpoint {
        *session = Arc::new(merge_checkpoint(session, checkpoint, live_ids)?);
    }
    if let Some((destination, history)) = &request.history {
        adopt_checkpoint_history(Arc::make_mut(session), destination, history, live_ids);
    }
    if let Some((HistoryDestination::Child(key), _)) = &request.selected_config
        && !session
            .subagents()
            .iter()
            .any(|child| &child.tool_use_id == key)
        && let Some(child) = published
            .subagents()
            .iter()
            .find(|child| &child.tool_use_id == key)
    {
        let session = Arc::make_mut(session);
        let mut subagents = session.subagents().to_vec();
        subagents.push(child_identity(child));
        session.set_subagents(subagents);
    }
    Ok(())
}

fn merge_tui_snapshot(
    incoming: &Arc<AppSession>,
    latest: &AppSession,
    preserve_history: bool,
    live_ids: Option<&[String]>,
) -> Arc<AppSession> {
    let mut session = Arc::clone(incoming);
    preserve_child_identity(&mut session, latest);
    preserve_newer_child_histories(&mut session, latest);
    if (preserve_history || session.history_snapshot().epoch < latest.history_snapshot().epoch)
        && session.history_identity() != latest.history_identity()
    {
        let session = Arc::make_mut(&mut session);
        session.adopt_history(&latest.history_snapshot());
        prune_main_history(session, live_ids);
    }
    if !same_plan_metadata(&session, latest) {
        let session = Arc::make_mut(&mut session);
        session.meta.mode = latest.meta.mode.clone();
    }
    session
}

fn same_plan_metadata(left: &AppSession, right: &AppSession) -> bool {
    left.meta.mode == right.meta.mode
}

fn prune_main_history(session: &mut AppSession, live_ids: Option<&[String]>) {
    let preserved;
    let live_ids = match live_ids {
        Some(ids) => ids,
        None => {
            preserved = session
                .subagent_messages()
                .keys()
                .cloned()
                .collect::<Vec<_>>();
            &preserved
        }
    };
    session.prune_orphans_preserving(
        |message| {
            message
                .tool_uses()
                .map(|(id, _, _)| id.to_owned())
                .collect()
        },
        live_ids,
    );
}

fn merge_checkpoint(
    base: &AppSession,
    checkpoint: &SessionCheckpoint,
    live_ids: Option<&[String]>,
) -> Result<AppSession, SessionError> {
    let mut session = base.clone();
    apply_session_checkpoint(&mut session, checkpoint)?;
    session.latest_state()?;
    if checkpoint.kind != SessionCheckpointKind::Options && checkpoint.history.is_some() {
        prune_main_history(&mut session, live_ids);
    }
    Ok(session)
}

fn writer_gone() -> SessionError {
    StorageError::Io(io::Error::other("storage writer unavailable")).into()
}

fn retain_history_waiters(id: MakimaId, session: &AppSession, waiters: &mut Vec<CheckpointWaiter>) {
    waiters.retain(|waiter| {
        let retained = waiter
            .histories
            .iter()
            .all(|history| history.retained_by(session));
        if !retained {
            let _ = waiter.reply.send(Err(CheckpointError::Save {
                session_id: id,
                message: Arc::from(CHECKPOINT_REPLACED),
            }));
        }
        retained
    });
}

fn acknowledge_waiters(
    pending: &Pending,
    session: &AppSession,
    mut waiters: Vec<CheckpointWaiter>,
) {
    let id = session.id;
    retain_history_waiters(id, session, &mut waiters);
    let state = lock(pending);
    for waiter in waiters {
        if waiter.histories.iter().any(|history| {
                matches!(history, RequiredHistory::Root(snapshot, Some(revision))
                    if history_version(&state, id, &HistoryDestination::Main).is_some_and(|current| (snapshot.epoch, *revision) < current))
            })
        {
            fail_waiters(
                id,
                vec![waiter],
                "configuration checkpoint superseded by a newer generation",
            );
        } else {
            let _ = waiter.reply.send(Ok(CheckpointAck {
                session_id: id,
                version: waiter.version,
            }));
        }
    }
}

fn fail_waiters(id: MakimaId, waiters: Vec<CheckpointWaiter>, message: &str) {
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
    logs: HashMap<MakimaId, SessionLog>,
    /// Sessions whose last write failed, so a sick disk warns once instead of
    /// once per frame.
    failing: HashSet<MakimaId>,
}

impl Writer {
    fn forget(&mut self, id: MakimaId) {
        self.logs.remove(&id);
        self.failing.remove(&id);
    }

    fn poll_journals(&mut self, pending: &Pending) {
        let journals: Vec<_> = lock(pending)
            .journals
            .iter()
            .map(|(id, journal)| (*id, journal.clone()))
            .collect();
        for (id, mut journal) in journals {
            journal.snapshot = match journal.journal.checkpoint_snapshot() {
                Ok(snapshot) => Arc::new(snapshot),
                Err(error) => {
                    self.report(id, Err(format!("{JOURNAL_UNSAVED}: {error}")));
                    continue;
                }
            };
            if journal.snapshot.revision <= journal.journal.published_revision() {
                continue;
            }
            let mut state = lock(pending);
            if state.closed
                || !journal_current(&state, id, Some(&journal))
                || state.recovery_transactions.contains_key(&id)
                || state.entries.contains_key(&id)
                || state.coordinator_pending.contains_key(&id)
                || state
                    .staged_checkpoints
                    .iter()
                    .any(|save| save.request.session_id == id)
            {
                continue;
            }
            let Some(mut session) = state.acknowledged.get(&id).cloned() else {
                continue;
            };
            if let Err(error) = project_journal(&state, id, Some(&journal), &mut session) {
                drop(state);
                self.report(id, Err(error));
                continue;
            }
            let generation = state
                .latest_generations
                .get(&id)
                .copied()
                .unwrap_or_default();
            let config_version = state.config_versions.get(&id).copied();
            remember_journal(&mut state, id, Some(&journal));
            state.entries.insert(
                id,
                Entry::Save(PendingSave {
                    journal: Some(journal),
                    session,
                    waiters: Vec::new(),
                    generation,
                    coordinator_generation: None,
                    config_version,
                    coordinator_history_base: None,
                    retry_attempt: 0,
                    retry_at: None,
                }),
            );
        }
    }

    fn flush(&mut self, pending: &Pending) -> Option<Instant> {
        self.retry_publication_syncs(pending, Instant::now(), SessionLog::sync_publication);
        let rollbacks = mem::take(&mut lock(pending).recovery_rollbacks);
        for rollback in rollbacks {
            let reply = rollback.reply.clone();
            let id = rollback.publication.id;
            let result = self.rollback_recovery(rollback.publication);
            self.report(id, result.as_ref().map(|_| ()).map_err(ToString::to_string));
            let _ = reply.send(result);
        }
        let current = mem::take(&mut lock(pending).current_checkpoints);
        for save in current {
            let id = save.request.session.id;
            let result = self.write_current_checkpoint(pending, &save);
            self.report(id, result.as_ref().map(|_| ()).map_err(String::as_str));
            if let Some(publications) = lock(pending).publications.get_mut(&id) {
                publications
                    .retain(|publication| !publication.same_request(&save.request.publication));
            }
            let _ = save.reply.send(result);
        }
        let restores = mem::take(&mut lock(pending).restore_checkpoints);
        for save in restores {
            let id = save.request.session_id;
            let result = self.write_restore_checkpoint(pending, &save);
            self.report(id, result.as_ref().map(|_| ()).map_err(String::as_str));
            if let Some(publications) = lock(pending).publications.get_mut(&id) {
                publications
                    .retain(|publication| !publication.same_request(&save.request.publication));
            }
            let _ = save.reply.send(result);
        }
        let recoveries = mem::take(&mut lock(pending).recoveries);
        for save in recoveries {
            let id = save.candidate.session().id;
            let reply = save.reply.clone();
            let result = self.write_recovery(pending, save);
            self.report(id, result.as_ref().map(|_| ()).map_err(ToString::to_string));
            let _ = reply.send(result);
        }
        self.poll_journals(pending);
        // Bound first: a `for` head temporary lives for the whole loop, so
        // iterating the guard directly would deadlock the re-insert below.
        let batch = mem::take(&mut lock(pending).entries);
        for (id, entry) in batch {
            match entry {
                Entry::Save(mut save) => {
                    if lock(pending).recovery_transactions.contains_key(&id) {
                        save.retry_at = Some(Instant::now() + JOURNAL_POLL_INTERVAL);
                        if let Err(waiters) = requeue_save(pending, id, save) {
                            fail_waiters(id, waiters, CHECKPOINT_REPLACED);
                        }
                        continue;
                    }
                    if save
                        .retry_at
                        .is_some_and(|retry_at| retry_at > Instant::now())
                    {
                        if let Err(waiters) = requeue_save(pending, id, save) {
                            fail_waiters(id, waiters, "checkpoint superseded by session deletion");
                        }
                        continue;
                    }
                    save.retry_at = None;
                    let lane = if let Some(journal) = &save.journal {
                        match smol::block_on(futures_lite::future::poll_once(
                            journal.journal.publication_read(),
                        )) {
                            Some(lane) => Some(lane),
                            None => {
                                save.retry_at = Some(Instant::now() + JOURNAL_POLL_INTERVAL);
                                if let Err(waiters) = requeue_save(pending, id, save) {
                                    fail_waiters(id, waiters, CHECKPOINT_REPLACED);
                                }
                                continue;
                            }
                        }
                    } else {
                        None
                    };
                    let _lane = lane;
                    if journal_prefix_superseded(save.journal.as_ref()) {
                        discard_coordinator_candidate(pending, id, &save);
                        fail_waiters(id, save.waiters, CHECKPOINT_REPLACED);
                        continue;
                    }
                    let publication = save
                        .journal
                        .as_ref()
                        .map(|journal| Arc::clone(&journal.publication));
                    let _publication = publication.as_ref().map(|publication| {
                        publication.lock().unwrap_or_else(PoisonError::into_inner)
                    });
                    if !journal_current(&lock(pending), id, save.journal.as_ref()) {
                        fail_waiters(id, save.waiters, CHECKPOINT_REPLACED);
                        continue;
                    }
                    #[cfg(test)]
                    {
                        let gate = lock(pending).save_gate.clone();
                        if let Some((entered, release)) = gate {
                            let _ = entered.send(());
                            let _ = release.recv();
                        }
                    }
                    if let Some(error) = save.journal.as_ref().and_then(journal_persistence_error) {
                        self.report(id, Err(&error));
                        let waiters = mem::take(&mut save.waiters);
                        if save.coordinator_generation.is_some() {
                            discard_coordinator_candidate(pending, id, &save);
                            let state = lock(pending);
                            let Some(session) = state.latest.get(&id) else {
                                fail_waiters(id, waiters, &error);
                                continue;
                            };
                            save.session = Arc::clone(session);
                            save.generation = state.latest_generations[&id];
                            save.config_version = state.config_versions.get(&id).copied();
                            save.coordinator_generation = None;
                            save.coordinator_history_base = None;
                        }
                        save.retry_at = Some(Instant::now() + JOURNAL_POLL_INTERVAL);
                        if let Err(replaced) = requeue_save(pending, id, save) {
                            fail_waiters(id, replaced, CHECKPOINT_REPLACED);
                        }
                        fail_waiters(id, waiters, &error);
                        continue;
                    }
                    let barrier = Arc::clone(&lock(pending).write_barrier);
                    let _barrier = barrier.lock().unwrap_or_else(PoisonError::into_inner);
                    let guard = {
                        let state = lock(pending);
                        if state
                            .generation_floors
                            .get(&id)
                            .is_some_and(|floor| save.generation < *floor)
                        {
                            fail_waiters(id, save.waiters, CHECKPOINT_REPLACED);
                            continue;
                        }
                        state.publication_guards.get(&id).cloned()
                    };
                    let result = match guard {
                        Some(guard) => guard
                            .publish(|| self.write(&save.session))
                            .map_err(StorageError::from)
                            .map_err(SessionError::from)
                            .and_then(|result| result.unwrap_or(Err(SessionError::OpenElsewhere))),
                        None => self.write(&save.session),
                    };
                    drop(_barrier);
                    match result {
                        Ok(()) => {
                            commit_save(pending, id, &save);
                            acknowledge_journal(save.journal.as_ref());
                            self.report_save(pending, &save, Ok(()));
                            acknowledge_waiters(pending, &save.session, save.waiters);
                        }
                        Err(error) => {
                            let message = error.to_string();
                            self.report_save(pending, &save, Err(&message));
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
                                continue;
                            }

                            let mut waiters = mem::take(&mut save.waiters);
                            if save.coordinator_generation.is_some() {
                                discard_coordinator_candidate(pending, id, &save);
                                fail_waiters(id, mem::take(&mut waiters), &message);
                                let state = lock(pending);
                                if save.config_version.is_none() || !state.latest.contains_key(&id)
                                {
                                    continue;
                                }
                                save.session = Arc::clone(&state.latest[&id]);
                                save.generation = state.latest_generations[&id];
                                save.config_version = state.config_versions.get(&id).copied();
                                save.coordinator_generation = None;
                                save.coordinator_history_base = None;
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
                Entry::Delete(done, guard, store) => {
                    self.forget(id);
                    let result = match guard {
                        Some(guard) => guard
                            .publish(|| {
                                let result = AppSession::delete(id, &self.dir);
                                if (result.is_ok()
                                    || matches!(result, Err(SessionError::Storage(StorageError::NotFound(_)))))
                                    && let Some(store) = store
                                    && let Err(error) = store.close_and_remove()
                                {
                                    warn!(%error, session_id = %id, "offload cleanup failed after session deletion");
                                }
                                result
                            })
                            .map_err(StorageError::from)
                            .map_err(SessionError::from)
                            .and_then(|result| result.unwrap_or(Err(SessionError::OpenElsewhere))),
                        None => AppSession::delete(id, &self.dir),
                    };
                    done(match result {
                        Err(SessionError::Storage(StorageError::NotFound(_))) => Ok(()),
                        result => result,
                    });
                }
            }
        }
        let checkpoints = mem::take(&mut lock(pending).staged_checkpoints);
        for save in checkpoints {
            let id = save.request.session_id;
            if save.request.publication.is_cancelled() {
                let _ = save.reply.send(Err(CHECKPOINT_CANCELLED.into()));
                continue;
            }
            if !journal_current(&lock(pending), id, save.journal.as_ref()) {
                let _ = save.reply.send(Err(CHECKPOINT_REPLACED.into()));
                continue;
            }
            if lock(pending)
                .recovery_transactions
                .contains_key(&save.request.session_id)
            {
                lock(pending).staged_checkpoints.push(save);
                continue;
            }
            let _lane = if prepared_journal(&save.request).is_none() {
                if let Some(journal) = &save.journal {
                    match smol::block_on(futures_lite::future::poll_once(
                        journal.journal.publication_read(),
                    )) {
                        Some(lane) => Some(lane),
                        None => {
                            lock(pending).staged_checkpoints.push(save);
                            continue;
                        }
                    }
                } else {
                    None
                }
            } else {
                None
            };
            let result = if prepared_journal(&save.request).is_none()
                && journal_prefix_superseded(save.journal.as_ref())
            {
                Err(CHECKPOINT_REPLACED.into())
            } else {
                self.write_staged_checkpoint(pending, &save)
            };
            if result.is_ok() && prepared_journal(&save.request).is_none() {
                acknowledge_journal(save.journal.as_ref());
            } else if let Some(error) = save.journal.as_ref().and_then(journal_persistence_error) {
                self.report(save.request.session_id, Err(error));
            }
            if let Some(publications) = lock(pending).publications.get_mut(&save.request.session_id)
            {
                publications
                    .retain(|publication| !publication.same_request(&save.request.publication));
            }
            let _ = save.reply.send(result);
        }
        let state = lock(pending);
        state
            .entries
            .values()
            .filter_map(|entry| match entry {
                Entry::Save(save) => save.retry_at,
                Entry::Delete(_, _, _) => None,
            })
            .chain(state.publication_syncs.values().map(|sync| sync.retry_at))
            .chain((!state.journals.is_empty()).then(|| Instant::now() + JOURNAL_POLL_INTERVAL))
            .min()
    }

    fn retry_publication_syncs(
        &mut self,
        pending: &Pending,
        now: Instant,
        mut sync_directory: impl FnMut(&Path) -> Result<(), StorageError>,
    ) {
        #[cfg(test)]
        if lock(pending).restore_sync_gate.is_some() {
            return;
        }
        let directory = self.dir.path().join(SESSIONS_DIR);
        let retries = {
            let state = lock(pending);
            state
                .publication_syncs
                .iter()
                .filter(|(_, sync)| sync.retry_at <= now)
                .map(|(id, sync)| (*id, sync.guard.clone()))
                .collect::<Vec<_>>()
        };
        for (id, guard) in retries {
            let result = guard
                .publish(|| sync_directory(&directory))
                .map_err(StorageError::from)
                .and_then(|result| result.transpose());
            let mut state = lock(pending);
            match result {
                Ok(Some(())) => {
                    if let Some(sync) = state.publication_syncs.remove(&id) {
                        for waiter in sync.waiters {
                            let _ = waiter.send(Ok(()));
                        }
                    }
                    let _ = self.warn_tx.send(RESTORE_SYNC_RECOVERED.into());
                }
                Ok(None) => {
                    state
                        .publication_sync_failures
                        .insert(id, RESTORE_SYNC_ABANDONED.into());
                    if let Some(sync) = state.publication_syncs.remove(&id) {
                        for waiter in sync.waiters {
                            let _ = waiter.send(Err(RESTORE_SYNC_ABANDONED.into()));
                        }
                    }
                    warn!(session_id = %id, "restore directory sync abandoned after lock loss");
                    let _ = self.warn_tx.send(RESTORE_SYNC_ABANDONED.into());
                }
                Err(error) => {
                    warn!(%error, session_id = %id, "restore directory sync retry failed");
                    if let Some(retry) = state.publication_syncs.get_mut(&id) {
                        let backoff = BACKGROUND_RETRY_BACKOFFS
                            [retry.retry_attempt.min(BACKGROUND_RETRY_BACKOFFS.len() - 1)];
                        retry.retry_attempt = retry.retry_attempt.saturating_add(1);
                        retry.retry_at = now + backoff;
                    }
                }
            }
        }
    }

    fn write_recovery(
        &mut self,
        pending: &Pending,
        save: RecoverySave,
    ) -> Result<RecoveryResult, CheckpointError> {
        let id = save.candidate.session().id;
        let failure = |message: String| CheckpointError::Save {
            session_id: id,
            message: Arc::from(message),
        };
        let old_journal = lock(pending).journals.get(&id).cloned();
        let lane = if save.transactional {
            let journal = old_journal
                .as_ref()
                .ok_or_else(|| failure(SNAPSHOT_UNAVAILABLE.into()))?;
            Some(
                smol::block_on(futures_lite::future::poll_once(
                    journal.journal.publication_read(),
                ))
                .ok_or_else(|| failure(CHECKPOINT_REPLACED.into()))?,
            )
        } else {
            None
        };
        let previous = if save.transactional {
            let state = lock(pending);
            let mut previous = state
                .acknowledged
                .get(&id)
                .cloned()
                .ok_or_else(|| failure(SNAPSHOT_UNAVAILABLE.into()))?;
            let snapshot = old_journal
                .as_ref()
                .unwrap()
                .journal
                .checkpoint_snapshot()
                .map_err(failure)?;
            let captured = JournalCheckpoint {
                snapshot: Arc::new(snapshot),
                ..old_journal.as_ref().unwrap().clone()
            };
            project_journal(&state, id, Some(&captured), &mut previous).map_err(failure)?;
            Some(previous)
        } else {
            None
        };
        let directory = self
            .dir
            .ensure_subdir(SESSIONS_DIR)
            .map_err(|error| failure(error.to_string()))?;
        let staged = SessionLog::stage_checkpoint(&directory, save.candidate.session())
            .map_err(|error| failure(error.to_string()))?;
        #[cfg(test)]
        {
            let gate = lock(pending).child_stage_gate.clone();
            if let Some((entered, release)) = gate {
                let _ = entered.send(());
                let _ = release.recv();
            }
        }
        let mut state = lock(pending);
        if state.closed {
            return Err(CheckpointError::Closed(id));
        }
        let published = staged
            .publish(&save.guard, || {
                state
                    .incarnations
                    .get(&id)
                    .is_some_and(|current| Arc::ptr_eq(current, &save.incarnation))
                    && !state.recovery_transactions.contains_key(&id)
                    && state.latest_generations.get(&id).copied() == save.generation
                    && !matches!(state.entries.get(&id), Some(Entry::Delete(_, _, _)))
            })
            .map_err(|error| failure(error.to_string()))?;
        if !published {
            return Err(failure(CHECKPOINT_REPLACED.into()));
        }
        let recovered = save.candidate.finalize_after_publication();
        let journal = recovered.journal();
        let session = Arc::new(recovered.session().clone());
        self.logs.remove(&id);
        let publication = if let Some(previous) = previous {
            state
                .recovery_transactions
                .insert(id, Arc::clone(&save.incarnation));
            Some(RecoveryPublication {
                pending: Arc::clone(pending),
                wake: save.wake,
                id,
                incarnation: save.incarnation,
                guard: save.guard,
                previous,
                recovered: session,
                journal,
                _lane: lane,
            })
        } else {
            install_recovered(&mut state, id, session, journal);
            None
        };
        Ok(RecoveryResult {
            recovered,
            publication,
        })
    }

    fn rollback_recovery(
        &mut self,
        publication: RecoveryPublication,
    ) -> Result<(), CheckpointError> {
        let id = publication.id;
        let failure = |message: String| CheckpointError::Save {
            session_id: id,
            message: Arc::from(message),
        };
        let directory = self
            .dir
            .ensure_subdir(SESSIONS_DIR)
            .map_err(|error| failure(error.to_string()))?;
        let staged = SessionLog::stage_checkpoint(&directory, publication.previous.as_ref())
            .map_err(|error| failure(error.to_string()))?;
        let mut state = lock(&publication.pending);
        let published = staged
            .publish(&publication.guard, || {
                !state.closed
                    && state
                        .incarnations
                        .get(&id)
                        .is_some_and(|current| Arc::ptr_eq(current, &publication.incarnation))
                    && state
                        .recovery_transactions
                        .get(&id)
                        .is_some_and(|current| Arc::ptr_eq(current, &publication.incarnation))
            })
            .map_err(|error| failure(error.to_string()))?;
        if !published {
            return Err(failure(CHECKPOINT_REPLACED.into()));
        }
        self.logs.remove(&id);
        state
            .acknowledged
            .insert(id, Arc::clone(&publication.previous));
        state.recovery_transactions.remove(&id);
        Ok(())
    }

    fn write_current_checkpoint(
        &mut self,
        pending: &Pending,
        save: &CurrentCheckpointSave,
    ) -> Result<(), String> {
        let request = &save.request;
        let id = request.session.id;
        let directory = self
            .dir
            .ensure_subdir(SESSIONS_DIR)
            .map_err(|error| error.to_string())?;
        let staged = SessionLog::stage_checkpoint(&directory, request.session.as_ref())
            .map_err(|error| error.to_string())?;
        #[cfg(test)]
        {
            let gate = lock(pending).child_stage_gate.clone();
            if let Some((entered, release)) = gate {
                let _ = entered.send(());
                let _ = release.recv();
            }
        }
        let _publication = save
            .journal
            .publication
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let mut state = lock(pending);
        let mut claim_error = None;
        let outcome = staged
            .publish_outcome(&request.guard, || {
                if state.closed
                    || state.recovery_transactions.contains_key(&id)
                    || !save_fence_current(&state, id, Some(&request.fence))
                    || !journal_current(&state, id, Some(&save.journal))
                    || !state
                        .incarnations
                        .get(&id)
                        .is_some_and(|current| Arc::ptr_eq(current, &save.incarnation))
                    || state.latest_generations.get(&id).copied() != save.generation
                    || matches!(state.entries.get(&id), Some(Entry::Delete(_, _, _)))
                {
                    return false;
                }
                match (request.claim)(&request.publication) {
                    Ok(()) => true,
                    Err(error) => {
                        claim_error = Some(error);
                        false
                    }
                }
            })
            .map_err(|error| error.to_string())?;
        #[cfg(test)]
        let outcome = match (outcome, state.current_sync_error.take()) {
            (PublicationOutcome::Published, Some(error)) => {
                PublicationOutcome::PublishedUnsynced(error)
            }
            (outcome, _) => outcome,
        };
        if matches!(outcome, PublicationOutcome::NotPublished) {
            return Err(
                claim_error.map_or_else(|| CHECKPOINT_REPLACED.into(), |error| error.to_string())
            );
        }
        self.logs.remove(&id);
        state.acknowledged.insert(id, Arc::clone(&request.session));
        state.latest.insert(id, Arc::clone(&request.session));
        let generation = next_generation(&mut state);
        state.latest_generations.insert(id, generation);
        state.generation_floors.insert(id, generation);
        remember_journal(
            &mut state,
            id,
            Some(&JournalCheckpoint {
                snapshot: Arc::new(request.journal.clone()),
                ..save.journal.clone()
            }),
        );
        state.publication_syncs.remove(&id);
        state.publication_sync_failures.remove(&id);
        if let PublicationOutcome::PublishedUnsynced(error) = outcome {
            warn!(%error, session_id = %id, "current session published without confirmed directory durability");
            let _ = self.warn_tx.send(format!("{RESTORE_UNSYNCED}: {error}"));
            state.publication_syncs.insert(
                id,
                PublicationSync {
                    guard: request.guard.clone(),
                    retry_at: Instant::now() + BACKGROUND_RETRY_BACKOFFS[0],
                    retry_attempt: 1,
                    waiters: Vec::new(),
                },
            );
        }
        drop(state);
        if let Err(error) = save.journal.journal.acknowledge(&request.journal) {
            warn!(%error, session_id = %id, "current journal acknowledgement rejected after publication");
        }
        Ok(())
    }

    fn write_restore_checkpoint(
        &mut self,
        pending: &Pending,
        save: &RestoreCheckpointSave,
    ) -> Result<SessionSaveFence, String> {
        let request = &save.request;
        let id = request.session_id;
        if request.publication.is_cancelled() {
            return Err(CHECKPOINT_CANCELLED.into());
        }
        let directory = self
            .dir
            .ensure_subdir(SESSIONS_DIR)
            .map_err(|error| error.to_string())?;
        let staged = SessionLog::stage_checkpoint_with_branch(
            &directory,
            request.session.as_ref(),
            request.branch.clone(),
        )
        .map_err(|error| error.to_string())?;
        #[cfg(test)]
        {
            let gate = lock(pending).child_stage_gate.clone();
            if let Some((entered, release)) = gate {
                let _ = entered.send(());
                let _ = release.recv();
            }
        }
        let _publication = save
            .journal
            .publication
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let mut state = lock(pending);
        let mut claim_error = None;
        let outcome = staged
            .publish_outcome(&request.guard, || {
                if state.closed
                    || state.recovery_transactions.contains_key(&id)
                    || !journal_current(&state, id, Some(&save.journal))
                    || !state
                        .incarnations
                        .get(&id)
                        .is_some_and(|current| Arc::ptr_eq(current, &save.incarnation))
                    || state.latest_generations.get(&id).copied() != save.generation
                    || matches!(state.entries.get(&id), Some(Entry::Delete(_, _, _)))
                {
                    return false;
                }
                match (request.claim)(&request.publication) {
                    Ok(()) => true,
                    Err(error) => {
                        claim_error = Some(error);
                        false
                    }
                }
            })
            .map_err(|error| error.to_string())?;
        #[cfg(test)]
        let outcome = match (outcome, state.restore_sync_error.take()) {
            (PublicationOutcome::Published, Some(error)) => {
                PublicationOutcome::PublishedUnsynced(error)
            }
            (outcome, _) => outcome,
        };
        if matches!(outcome, PublicationOutcome::NotPublished) {
            return Err(
                claim_error.map_or_else(|| CHECKPOINT_REPLACED.into(), |error| error.to_string())
            );
        }
        self.logs.remove(&id);
        cancel_publications(&mut state, id);
        if let Some(Entry::Save(queued)) = state.entries.remove(&id) {
            fail_waiters(id, queued.waiters, CHECKPOINT_REPLACED);
        }
        state.coordinator_pending.remove(&id);
        state.coordinator_history_bases.remove(&id);
        state.config_versions.remove(&id);
        state.history_versions.remove(&id);
        state.live_children.remove(&id);
        state.prepared_histories.remove(&id);
        state.acknowledged.insert(id, Arc::clone(&request.session));
        state.latest.insert(id, Arc::clone(&request.session));
        let generation = next_generation(&mut state);
        state.latest_generations.insert(id, generation);
        state.journals.insert(
            id,
            JournalCheckpoint {
                publication: Arc::default(),
                journal: Arc::clone(&save.journal.journal),
                snapshot: Arc::new(request.journal.clone()),
            },
        );
        state.authoritative_sessions.insert(id);
        let incarnation = Arc::new(());
        state.save_incarnations.insert(id, Arc::clone(&incarnation));
        state.publication_syncs.remove(&id);
        state.publication_sync_failures.remove(&id);
        if let PublicationOutcome::PublishedUnsynced(error) = outcome {
            warn!(%error, session_id = %id, "restored session published without confirmed directory durability");
            let _ = self.warn_tx.send(format!("{RESTORE_UNSYNCED}: {error}"));
            state.publication_syncs.insert(
                id,
                PublicationSync {
                    guard: request.guard.clone(),
                    retry_at: Instant::now() + BACKGROUND_RETRY_BACKOFFS[0],
                    retry_attempt: 1,
                    waiters: Vec::new(),
                },
            );
        }
        Ok(SessionSaveFence {
            session_id: id,
            incarnation,
        })
    }

    fn write_staged_checkpoint(
        &mut self,
        pending: &Pending,
        save: &StagedCheckpointSave,
    ) -> Result<(), String> {
        let request = &save.request;
        let id = request.session_id;
        if request.publication.is_cancelled() {
            return Err(CHECKPOINT_CANCELLED.into());
        }
        let (mut candidate, live_ids) = {
            let state = lock(pending);
            if state.closed {
                return Err(CheckpointError::Closed(id).to_string());
            }
            let mut candidate = state
                .acknowledged
                .get(&id)
                .cloned()
                .ok_or_else(|| CHECKPOINT_REPLACED.to_owned())?;
            if let Some((HistoryDestination::Child(key), _)) = &request.selected_config
                && !candidate
                    .subagents()
                    .iter()
                    .any(|child| &child.tool_use_id == key)
                && let Some(child) = state.latest.get(&id).and_then(|latest| {
                    latest
                        .subagents()
                        .iter()
                        .find(|child| &child.tool_use_id == key)
                })
            {
                let candidate = Arc::make_mut(&mut candidate);
                let mut subagents = candidate.subagents().to_vec();
                subagents.push(child_identity(child));
                candidate.set_subagents(subagents);
            }
            (candidate, state.live_children.get(&id).cloned())
        };
        {
            let state = lock(pending);
            project_staged_journal(&state, save, &mut candidate)?;
        }
        if let Some(checkpoint) = &request.checkpoint {
            candidate = Arc::new(
                merge_checkpoint(&candidate, checkpoint, live_ids.as_deref())
                    .map_err(|error| error.to_string())?,
            );
        }
        if let Some((destination, history)) = &request.history
            && !adopt_checkpoint_history(
                Arc::make_mut(&mut candidate),
                destination,
                history,
                live_ids.as_deref(),
            )
        {
            return Err(CHECKPOINT_REPLACED.into());
        }
        if let Some((HistoryDestination::Child(key), _)) = &request.selected_config
            && !candidate
                .subagents()
                .iter()
                .any(|child| &child.tool_use_id == key)
        {
            return Err(SELECTED_CONFIG_MISSING.into());
        }
        let directory = self
            .dir
            .ensure_subdir(SESSIONS_DIR)
            .map_err(|error| error.to_string())?;
        let staged = SessionLog::stage_checkpoint(&directory, candidate.as_ref())
            .map_err(|error| error.to_string())?;
        #[cfg(test)]
        {
            let gate = lock(pending).child_stage_gate.clone();
            if let Some((entered, release)) = gate {
                let _ = entered.send(());
                let _ = release.recv();
            }
        }
        if let Some(error) = save.journal.as_ref().and_then(journal_persistence_error) {
            return Err(error);
        }
        let mut state = lock(pending);
        let live_ids = state.live_children.get(&id).map(Vec::as_slice);
        let prepare_promotion = |session: &Arc<AppSession>| -> Result<Arc<AppSession>, String> {
            let mut promoted = Arc::clone(session);
            project_staged_journal(&state, save, &mut promoted)?;
            promote_staged_checkpoint(&mut promoted, request, &candidate, live_ids)
                .map_err(|error| error.to_string())?;
            Ok(promoted)
        };
        let latest = state.latest.get(&id).map(prepare_promotion).transpose()?;
        let provisional = state
            .coordinator_pending
            .get(&id)
            .map(|pending| prepare_promotion(&pending.session))
            .transpose()?;
        let queued = match state.entries.get(&id) {
            Some(Entry::Save(queued)) => Some(prepare_promotion(&queued.session)?),
            _ => None,
        };
        let mut claim_error = None;
        let published = staged
            .publish(&request.guard, || {
                if state.closed
                    || !journal_current(&state, id, save.journal.as_ref())
                    || !state
                        .incarnations
                        .get(&id)
                        .is_some_and(|incarnation| Arc::ptr_eq(incarnation, &save.incarnation))
                    || (request.checkpoint.as_ref().is_some_and(|checkpoint| {
                        checkpoint.kind != SessionCheckpointKind::History
                    }) && state.config_versions.get(&id).copied() != save.config_version)
                    || save.history_version.is_some_and(|incoming| {
                        request.history.as_ref().is_some_and(|(destination, _)| {
                            history_version(&state, id, destination)
                                .is_some_and(|current| incoming < current)
                        })
                    })
                    || request
                        .history
                        .as_ref()
                        .is_some_and(|(destination, history)| {
                            state.latest.get(&id).is_some_and(|session| {
                                let current = match destination {
                                    HistoryDestination::Main => Some(session.history_snapshot()),
                                    HistoryDestination::Child(key) => {
                                        session.subagent_history_snapshot(key)
                                    }
                                };
                                current.is_some_and(|current| current.epoch > history.epoch)
                            })
                        })
                {
                    return false;
                }
                match (request.claim)(&request.publication) {
                    Ok(()) => true,
                    Err(error) => {
                        claim_error = Some(error);
                        false
                    }
                }
            })
            .map_err(|error| error.to_string())?;
        if !published {
            return Err(
                claim_error.map_or_else(|| CHECKPOINT_CANCELLED.into(), |error| error.to_string())
            );
        }
        self.logs.remove(&id);
        if !state
            .incarnations
            .get(&id)
            .is_some_and(|incarnation| Arc::ptr_eq(incarnation, &save.incarnation))
        {
            return Ok(());
        }
        state.acknowledged.insert(id, Arc::clone(&candidate));
        if let Some(snapshot) = prepared_journal(request)
            && let Some(journal) = &save.journal
        {
            let captured = JournalCheckpoint {
                snapshot: Arc::new(snapshot.clone()),
                ..journal.clone()
            };
            remember_journal(&mut state, id, Some(&captured));
        }
        if save.journal.is_some()
            && let Some((destination, history)) = &request.history
        {
            let histories = state.prepared_histories.entry(id).or_default();
            histories.retain(|(current, _)| current != destination);
            histories.push((destination.clone(), history.clone()));
        }
        if let Some(version) = save.history_version
            && let Some((destination, _)) = &request.history
        {
            set_history_version(&mut state, id, destination, version);
        }
        let config_version = request
            .checkpoint
            .as_ref()
            .filter(|checkpoint| checkpoint.kind == SessionCheckpointKind::Config)
            .and_then(|checkpoint| checkpoint.config.as_ref())
            .map(|config| (config.runtime_epoch, config.generation));
        if let Some(version) = config_version {
            state.config_versions.insert(id, version);
        }
        let history_base = state
            .latest
            .get(&id)
            .map(|session| session.history_identity());
        if let Some(latest) = latest {
            state.latest.insert(id, latest);
        }
        if matches!(&request.history, Some((HistoryDestination::Main, _)))
            && let Some(base) = history_base
        {
            state.coordinator_history_bases.insert(id, base);
        }
        if let Some(promoted) = provisional
            && let Some(provisional) = state.coordinator_pending.get_mut(&id)
        {
            provisional.session = promoted;
            if matches!(&request.history, Some((HistoryDestination::Main, _))) {
                provisional.history_base = history_base;
            }
        }
        let generation = next_generation(&mut state);
        state.latest_generations.insert(id, generation);
        if let Some(promoted) = queued
            && let Some(Entry::Save(queued)) = state.entries.get_mut(&id)
        {
            queued.session = promoted;
            queued.generation = generation;
            if let Some(version) = config_version {
                queued.config_version = Some(version);
            }
            retain_history_waiters(id, &queued.session, &mut queued.waiters);
        }
        Ok(())
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

    fn report_save(&mut self, pending: &Pending, save: &PendingSave, result: Result<(), &str>) {
        let id = save.session.id;
        let state = lock(pending);
        let current = (save.config_version.is_none() && !state.config_versions.contains_key(&id))
            || (state.latest_generations.get(&id) == Some(&save.generation)
                && state.config_versions.get(&id) == save.config_version.as_ref());
        if !current {
            return;
        }
        self.report(id, result);
    }

    fn report(&mut self, id: MakimaId, result: Result<(), impl std::fmt::Display>) {
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
    use crate::event_loop::rewind::tests::assert_persisted_state;
    use makima_agent::actor::AgentContextSnapshot;
    use makima_agent::journal::{JournalChange, JournalCommit};
    use makima_agent::session_checkpoint::stored_agent_config;
    use makima_agent::session_coordinator::CommittedConfigProjection;
    use makima_agent::session_coordinator::{builtin_option_definitions, publication_claim};
    use makima_agent::session_options::SessionOptions;
    use makima_agent::session_options::{ENABLED_VALUE, YOLO_OPTION_ID};
    use makima_agent::{AgentMode, ThinkingConfig};
    use makima_agent::{RunSettings, ToolOutput};
    use makima_domain::{Post, PostLevel};
    use makima_providers::provider::{BoxFuture, Provider};
    use makima_providers::{
        AgentError, Invocation, Model, ProviderEvent, RequestDelivery, RequestOptions,
        StreamResponse,
    };
    use makima_providers::{Effort, Message};
    use makima_storage::id::AgentId;
    use makima_storage::id::SessionRef;
    use makima_storage::session_lock;
    use makima_storage::session_state::{
        StateChange, StateFoldError, StoredAgentConfig, StoredStateRecord,
    };
    use makima_storage::session_state::{StoredAgentLifecycle, StoredAgentRecord};
    use makima_storage::sessions::{StoredMode, StoredPost, StoredSubagent, StoredThinking};
    use std::path::PathBuf;
    use tempfile::TempDir;
    use test_case::test_case;

    const DRAIN_TIMEOUT: Duration = Duration::from_secs(30);
    const DEFERRED_RETRY: Duration = Duration::from_secs(3600);
    const MODEL: &str = "test-model";
    const FAILED_MODEL: &str = "ollama/failed-model";
    const NEXT_MODEL: &str = "ollama/next-model";
    const NEXT_CWD: &str = "/tmp/next";
    const CWD: &str = "/tmp/writer";
    const MSG_PREFIX: &str = "msg-";
    const RESUMED_MSG: &str = "resumed";
    const TOOL_ID: &str = "tool-1";
    const SIBLING_AGENT_ID: &str = "sibling";
    const TOOL_TEXT: &str = "tool output";
    const TITLE: &str = "renamed after reload";
    const BASE_MSG: &str = "base";
    const COORDINATOR_MSG: &str = "coordinator";
    const REWOUND_MSG: &str = "rewound";
    const POST_TEXT: &str = "checkpoint post";
    const LIVE_CHILD: &str = "live-child";
    const ORPHAN_CHILD: &str = "orphan-child";

    struct ConfigTestProvider;

    impl Provider for ConfigTestProvider {
        fn stream_message<'a>(
            &'a self,
            _: &'a Model,
            _: &'a [Message],
            _: &'a str,
            _: &'a serde_json::Value,
            _: &'a flume::Sender<ProviderEvent>,
            _: RequestOptions,
            _: Option<&'a SessionRef>,
            _invocation: &'a Invocation,
            _delivery: RequestDelivery,
        ) -> BoxFuture<'a, Result<StreamResponse, AgentError>> {
            Box::pin(std::future::pending())
        }
    }

    fn config_checkpoint(epoch: u64, generation: u64) -> SessionCheckpoint {
        SessionCheckpoint {
            kind: SessionCheckpointKind::Config,
            history_revision: None,
            history: None,
            options: SessionOptions::new(
                builtin_option_definitions(
                    FAILED_MODEL,
                    [Arc::from(FAILED_MODEL)],
                    false,
                    true,
                    true,
                    ThinkingConfig::Off,
                ),
                &Default::default(),
            )
            .unwrap()
            .snapshot(),
            config: Some(config_projection(FAILED_MODEL, CWD, epoch, generation)),
            journal: None,
        }
    }

    fn config_projection(
        model: &str,
        cwd: &str,
        runtime_epoch: u64,
        generation: u64,
    ) -> CommittedConfigProjection {
        let mut config = EffectiveAgentConfig::new(
            RunSettings {
                provider: Arc::new(ConfigTestProvider),
                model: makima_providers::Model::from_spec(model).unwrap(),
                fast: true,
                workflow: true,
                thinking: ThinkingConfig::Off,
            },
            AgentMode::Plan(CWD.into()),
        );
        config.context = AgentContextSnapshot::new(PathBuf::from(cwd));
        CommittedConfigProjection {
            runtime: Arc::new(()),
            runtime_epoch,
            generation,
            config: Arc::new(config),
        }
    }

    fn stored_config(model: &str, cwd: &str) -> StoredAgentConfig {
        let mut config = (*config_projection(model, cwd, 1, 1).config).clone();
        config.mode = AgentMode::Build;
        config.mode_def = Some(makima_agent::ModeDef::default_for(config.mode.id()));
        stored_agent_config(&config, Default::default()).unwrap()
    }

    /// Claims the session lock and returns the writer production binds to it.
    fn guarded_writer(
        writer: &StorageWriter,
        dir: &StateDir,
        id: MakimaId,
    ) -> (
        session_lock::ClaimedSessionLock,
        Arc<dyn CheckpointWriter<SessionCheckpoint>>,
    ) {
        let lease = session_lock::claim(&dir.ensure_subdir(SESSIONS_DIR).unwrap(), &id)
            .unwrap()
            .unwrap();
        let checkpoint = writer.coordinator_checkpoint();
        checkpoint.bind(Some(lease.publication_guard()));
        (lease, checkpoint)
    }

    fn publish_config(
        checkpoint: &Arc<dyn CheckpointWriter<SessionCheckpoint>>,
        request: CheckpointRequest<SessionCheckpoint>,
    ) -> CheckpointFuture {
        checkpoint.checkpoint_guarded(
            request,
            CheckpointPublication::default(),
            publication_claim(),
        )
    }

    fn manual_writer() -> (StorageWriter, flume::Receiver<()>) {
        let (wake, wake_rx) = flume::unbounded();
        let (_, done_rx) = flume::bounded(1);
        (
            StorageWriter {
                pending: Arc::default(),
                wake,
                done_rx,
                stop: Arc::default(),
            },
            wake_rx,
        )
    }

    #[test_case(false; "root_epoch")]
    #[test_case(true; "child_epoch")]
    fn restore_checkpoint_rejects_history_epoch_mismatch(child: bool) {
        let (_tmp, dir) = state_dir();
        let (writer, wake) = manual_writer();
        let mut session = AppSession::new(MODEL, CWD);
        let root = AgentId::generate();
        let child_id = AgentId::generate();
        session.bind_root_id(root).unwrap();
        session.push_message(Message::user(BASE_MSG.into()));
        let mut record = session.agents()[&root].clone();
        record.id = child_id;
        record.parent = Some(root);
        record.depth = 1;
        record.transcript_key = LIVE_CHILD.into();
        let seq = session.next_state_seq();
        record.created_seq = seq;
        record.updated_seq = seq;
        session.push_agent_revision(seq, record);
        session.adopt_subagent_history(
            LIVE_CHILD.into(),
            &HistorySnapshot::new(vec![Message::user(RESUMED_MSG.into())]),
        );
        let mut journal = SessionJournal::from_session(&session).unwrap().snapshot();
        journal.authoritative = true;
        let branch = StoredBranch {
            id: MakimaId::generate(),
            created_at: session.updated_at,
            at_seq: journal.seq,
            agent: root,
            offset: session.messages().len(),
            transition: None,
        };
        session.push_branch(branch.clone());
        session.save(&dir).unwrap();
        let path = dir
            .path()
            .join(SESSIONS_DIR)
            .join(format!("{}.jsonl", session.id));
        let before = std::fs::read(&path).unwrap();
        if child {
            let history =
                HistorySnapshot::new(session.subagent_messages()[LIVE_CHILD].as_ref().clone());
            session.adopt_subagent_history(LIVE_CHILD.into(), &history);
        } else {
            let history = HistorySnapshot::new(session.messages().to_vec());
            session.adopt_history(&history);
        }
        let (lease, _coordinator) = guarded_writer(&writer, &dir, session.id);
        let result = enqueue_restore_checkpoint(
            &writer.pending,
            &writer.wake,
            RestoredSessionCheckpoint {
                session_id: session.id,
                session: Arc::new(session),
                journal,
                branch,
                publication: CheckpointPublication::default(),
                claim: publication_claim(),
                guard: lease.publication_guard(),
            },
        );
        assert_eq!(result.unwrap_err(), RESTORE_PROJECTION_MISMATCH);
        assert!(lock(&writer.pending).restore_checkpoints.is_empty());
        assert!(wake.is_empty());
        assert_eq!(std::fs::read(path).unwrap(), before);
    }

    #[test_case(false, false, false; "exact_replacement")]
    #[test_case(true, false, false; "unchanged_replacement")]
    #[test_case(false, true, false; "detached_exact_before_install")]
    #[test_case(true, true, false; "detached_unchanged_before_install")]
    #[test_case(false, true, true; "detached_exact_after_install")]
    #[test_case(true, true, true; "detached_unchanged_after_install")]
    fn restore_checkpoint_replaces_exactly_and_fences_old_producers(
        unchanged: bool,
        detached: bool,
        installed: bool,
    ) {
        smol::block_on(async {
            let (_tmp, dir) = state_dir();
            let (writer, _wake_rx) = manual_writer();
            let mut target = AppSession::new(MODEL, CWD);
            let root = AgentId::generate();
            target.bind_root_id(root).unwrap();
            target.push_message(Message::user(BASE_MSG.into()));
            let mut old = target.clone();
            if !unchanged {
                old.push_message(Message::user(COORDINATOR_MSG.into()));
                old.push_state(StoredStateRecord {
                    seq: old.sequence_high_water().unwrap() + 1,
                    agent: None,
                    generation: 1,
                    change: StateChange::Rules {
                        rules: Vec::new(),
                        yolo: Some(true),
                    },
                });
                old.set_subagent_messages(
                    ORPHAN_CHILD.into(),
                    vec![Message::user(RESUMED_MSG.into())],
                );
                old.push_post(stored_post(None, 1));
                old.insert_tool_output(TOOL_ID.into(), ToolOutput::Plain(TOOL_TEXT.into()));
                old.set_title(TITLE.into());
            }
            let id = old.id;
            let journal = Arc::new(SessionJournal::from_session(&old).unwrap());
            writer.seed(Arc::new(old.clone()));
            writer.bind_journal(id, Arc::clone(&journal));
            let old_fence = writer.session_save_fence(id);
            let (lease, old_coordinator) = guarded_writer(&writer, &dir, id);
            let (warn_tx, _warnings) = flume::unbounded();
            let mut disk = Writer {
                dir: dir.clone(),
                warn_tx,
                logs: HashMap::new(),
                failing: HashSet::new(),
            };
            disk.write(&old).unwrap();
            let old_capture = capture_journal(&writer.pending, id).unwrap();
            let version = CheckpointVersion {
                epoch: 1,
                revision: 1,
            };
            let queued = writer.checkpoint_fenced(
                CheckpointRequest {
                    session_id: id,
                    version,
                    snapshot: Arc::new(old.clone()),
                },
                &old_fence,
            );
            let staged_publication = CheckpointPublication::default();
            let staged = enqueue_staged_checkpoint(
                &writer.pending,
                &writer.wake,
                StagedSessionCheckpoint {
                    session_id: id,
                    checkpoint: None,
                    journal: None,
                    selected_config: None,
                    history: None,
                    publication: staged_publication.clone(),
                    claim: publication_claim(),
                    guard: lease.publication_guard(),
                },
            )
            .unwrap();
            {
                let mut state = lock(&writer.pending);
                let generation = state.latest_generations[&id];
                state.coordinator_pending.insert(
                    id,
                    CoordinatorPending {
                        generation,
                        session: Arc::new(old.clone()),
                        history_base: Some(old.history_identity()),
                    },
                );
                state.config_versions.insert(id, (1, 1));
                state
                    .prepared_histories
                    .insert(id, vec![(HistoryDestination::Main, old.history_snapshot())]);
            }
            let detached_saves = detached.then(|| {
                let mut state = lock(&writer.pending);
                (
                    state.entries.remove(&id).unwrap(),
                    state.staged_checkpoints.remove(0),
                )
            });
            let restored = SessionJournal::from_session(&target).unwrap().snapshot();
            let mut token = journal
                .prepare_branch_restore(root, restored)
                .await
                .unwrap();
            let snapshot = token.snapshot().clone();
            snapshot.apply_to(&mut target).unwrap();
            let branch = StoredBranch {
                id: MakimaId::generate(),
                created_at: old.updated_at,
                at_seq: old.sequence_high_water().unwrap(),
                agent: root,
                offset: old.messages().len(),
                transition: None,
            };
            target.push_branch(branch.clone());
            let expected = Arc::new(target);
            let response = enqueue_restore_checkpoint(
                &writer.pending,
                &writer.wake,
                RestoredSessionCheckpoint {
                    session_id: id,
                    session: Arc::clone(&expected),
                    journal: snapshot.clone(),
                    branch: branch.clone(),
                    publication: CheckpointPublication::default(),
                    claim: publication_claim(),
                    guard: lease.publication_guard(),
                },
            )
            .unwrap();
            disk.flush(&writer.pending);
            let fence = response.recv().unwrap().unwrap();
            if installed {
                token.install().unwrap();
            }
            if let Some((ordinary, staged)) = detached_saves {
                let path = dir.path().join(SESSIONS_DIR).join(format!("{id}.jsonl"));
                let published = std::fs::read(&path).unwrap();
                {
                    let mut state = lock(&writer.pending);
                    state.entries.insert(id, ordinary);
                    state.staged_checkpoints.push(staged);
                }
                disk.flush(&writer.pending);
                assert_eq!(std::fs::read(path).unwrap(), published);
                assert!(!lock(&writer.pending).entries.contains_key(&id));
                assert!(lock(&writer.pending).staged_checkpoints.is_empty());
            }
            token.install().unwrap();
            assert!(
                matches!(queued.await, Err(CheckpointError::Save { message, .. }) if message.as_ref() == CHECKPOINT_REPLACED)
            );
            assert!(staged.recv().unwrap().is_err());
            assert!(staged_publication.is_cancelled());
            {
                let state = lock(&writer.pending);
                assert!(Arc::ptr_eq(&state.latest[&id], &expected));
                assert!(Arc::ptr_eq(&state.acknowledged[&id], &expected));
                assert!(!state.coordinator_pending.contains_key(&id));
                assert!(!state.coordinator_history_bases.contains_key(&id));
                assert!(!state.config_versions.contains_key(&id));
                assert!(!state.prepared_histories.contains_key(&id));
                assert!(!journal_current(&state, id, Some(&old_capture)));
            }
            let loaded = AppSession::load(id, &dir).unwrap();
            assert_persisted_state(&loaded, &snapshot);
            assert_eq!(loaded.agent_revisions(), expected.agent_revisions());
            assert_eq!(message_texts(&loaded), message_texts(&expected));
            assert_eq!(loaded.posts(), expected.posts());
            assert_eq!(loaded.subagents(), expected.subagents());
            assert_eq!(
                loaded.subagent_messages().len(),
                expected.subagent_messages().len()
            );
            assert_eq!(loaded.tool_outputs().len(), expected.tool_outputs().len());
            assert_eq!(loaded.title, expected.title);
            let saved_branch = loaded
                .load_branch_from(branch.id, &dir.ensure_subdir(SESSIONS_DIR).unwrap())
                .unwrap();
            assert_persisted_state(
                &saved_branch,
                &SessionJournal::from_session(&old).unwrap().snapshot(),
            );
            assert_eq!(message_texts(&saved_branch), message_texts(&old));
            let mut stale = old;
            stale.set_title(RESUMED_MSG.into());
            writer.send(Arc::new(stale.clone()));
            writer.send_fenced(Arc::new(stale.clone()), &old_fence);
            assert!(
                writer
                    .checkpoint_fenced(
                        CheckpointRequest {
                            session_id: id,
                            version,
                            snapshot: Arc::new(stale)
                        },
                        &old_fence
                    )
                    .await
                    .is_err()
            );
            assert!(Arc::ptr_eq(&writer.latest_snapshot(id).unwrap(), &expected));
            let mut checkpoint = config_checkpoint(1, 1);
            checkpoint.kind = SessionCheckpointKind::Options;
            assert!(
                old_coordinator
                    .checkpoint(CheckpointRequest {
                        session_id: id,
                        version,
                        snapshot: Arc::new(checkpoint.clone())
                    })
                    .await
                    .is_err()
            );
            checkpoint.config = None;
            checkpoint.journal = Some(snapshot);
            assert!(
                old_coordinator
                    .checkpoint_guarded(
                        CheckpointRequest {
                            session_id: id,
                            version,
                            snapshot: Arc::new(checkpoint.clone()),
                        },
                        CheckpointPublication::default(),
                        publication_claim(),
                    )
                    .await
                    .is_err()
            );
            let coordinator = writer.coordinator_checkpoint();
            coordinator.bind(Some(lease.publication_guard()));
            let saved = coordinator.checkpoint_guarded(
                CheckpointRequest {
                    session_id: id,
                    version,
                    snapshot: Arc::new(checkpoint),
                },
                CheckpointPublication::default(),
                publication_claim(),
            );
            disk.flush(&writer.pending);
            assert_eq!(saved.await.unwrap().version, version);
            let mut fresh = expected.as_ref().clone();
            fresh.set_title(RESUMED_MSG.into());
            writer.send_fenced(Arc::new(fresh), &fence);
            disk.flush(&writer.pending);
            assert_eq!(AppSession::load(id, &dir).unwrap().title, RESUMED_MSG);
        });
    }

    #[test_case(false; "ordinary_write_at_takeover")]
    #[test_case(true; "guarded_current_at_takeover")]
    fn lease_takeover_rejects_current_and_already_queued_writes(current: bool) {
        smol::block_on(async {
            let (_tmp, dir) = state_dir();
            let (writer, _wake_rx) = manual_writer();
            let mut session = AppSession::new(MODEL, CWD);
            let root = AgentId::generate();
            session.bind_root_id(root).unwrap();
            session.push_message(Message::user(BASE_MSG.into()));
            let id = session.id;
            writer.seed(Arc::new(session.clone()));
            writer.bind_journal(
                id,
                Arc::new(SessionJournal::from_session(&session).unwrap()),
            );
            let fence = writer.session_save_fence(id);
            let (lease, _) = guarded_writer(&writer, &dir, id);
            writer.bind_publication_guard(id, lease.publication_guard());
            let (warn_tx, _warnings) = flume::unbounded();
            let mut disk = Writer {
                dir: dir.clone(),
                warn_tx,
                logs: HashMap::new(),
                failing: HashSet::new(),
            };
            disk.write(&session).unwrap();
            let version = CheckpointVersion {
                epoch: 1,
                revision: 1,
            };
            let queued = writer.checkpoint_fenced(
                CheckpointRequest {
                    session_id: id,
                    version,
                    snapshot: Arc::new(session.clone()),
                },
                &fence,
            );
            let mut guarded = current.then(|| {
                Box::pin(writer.checkpoint_fenced_guarded(
                    CheckpointRequest {
                        session_id: id,
                        version,
                        snapshot: Arc::new(session.clone()),
                    },
                    &fence,
                    lease.publication_guard(),
                ))
            });
            if let Some(guarded) = &mut guarded {
                assert!(
                    futures_lite::future::poll_once(guarded.as_mut())
                        .await
                        .is_none()
                );
            }
            let (entered, staged) = flume::bounded(1);
            let (release, released) = flume::bounded(1);
            {
                let mut state = lock(&writer.pending);
                if current {
                    state.child_stage_gate = Some((entered, released));
                } else {
                    state.save_gate = Some((entered, released));
                }
            }
            let pending = Arc::clone(&writer.pending);
            let worker = std::thread::spawn(move || disk.flush(&pending));
            staged.recv_timeout(DRAIN_TIMEOUT).unwrap();
            lease.release().unwrap();
            let directory = dir.ensure_subdir(SESSIONS_DIR).unwrap();
            let new_owner = session_lock::claim(&directory, &id).unwrap().unwrap();
            let mut replacement = session.clone();
            replacement.replace_messages(vec![Message::user(RESUMED_MSG.into())]);
            replacement.set_title(TITLE.into());
            new_owner
                .publication_guard()
                .publish(|| SessionLog::rewrite(&directory, &replacement))
                .unwrap()
                .unwrap()
                .unwrap();
            let path = directory.join(format!("{id}.jsonl"));
            let published = std::fs::read(&path).unwrap();
            release.send(()).unwrap();
            worker.join().unwrap();
            if let Some(guarded) = guarded {
                assert_eq!(guarded.await, Err(CHECKPOINT_REPLACED.into()));
            }
            if !current {
                lock(&writer.pending).entries.remove(&id);
            }
            assert!(queued.await.is_err());
            assert_eq!(std::fs::read(path).unwrap(), published);
            assert!(save_fence_current(&lock(&writer.pending), id, Some(&fence)));
        });
    }

    #[test]
    fn guarded_current_publication_keeps_incarnations_and_has_no_branch() {
        smol::block_on(async {
            let (_tmp, dir) = state_dir();
            let (writer, _wake_rx) = manual_writer();
            let mut session = AppSession::new(MODEL, CWD);
            session.bind_root_id(AgentId::generate()).unwrap();
            session.push_message(Message::user(BASE_MSG.into()));
            let id = session.id;
            writer.seed(Arc::new(session.clone()));
            let journal = Arc::new(SessionJournal::from_session(&session).unwrap());
            writer.bind_journal(id, Arc::clone(&journal));
            let fence = writer.session_save_fence(id);
            let captured = capture_journal(&writer.pending, id).unwrap();
            let (lease, _) = guarded_writer(&writer, &dir, id);
            let incarnation = Arc::clone(&lock(&writer.pending).incarnations[&id]);
            let mut result = Box::pin(writer.checkpoint_fenced_guarded(
                CheckpointRequest {
                    session_id: id,
                    version: CheckpointVersion {
                        epoch: 1,
                        revision: 1,
                    },
                    snapshot: Arc::new(session.clone()),
                },
                &fence,
                lease.publication_guard(),
            ));
            assert!(
                futures_lite::future::poll_once(result.as_mut())
                    .await
                    .is_none()
            );
            let (warn_tx, _warnings) = flume::unbounded();
            let mut disk = Writer {
                dir: dir.clone(),
                warn_tx,
                logs: HashMap::new(),
                failing: HashSet::new(),
            };
            disk.flush(&writer.pending);
            result.await.unwrap();
            let state = lock(&writer.pending);
            assert!(save_fence_current(&state, id, Some(&fence)));
            assert!(Arc::ptr_eq(&state.incarnations[&id], &incarnation));
            assert!(journal_current(&state, id, Some(&captured)));
            drop(state);
            let loaded = AppSession::load(id, &dir).unwrap();
            assert_eq!(message_texts(&loaded), [BASE_MSG]);
            assert!(loaded.branches().is_empty());
            assert_eq!(journal.published_revision(), journal.snapshot().revision);
        });
    }

    #[test_case(false; "directory_sync_recovers")]
    #[test_case(true; "directory_sync_loses_guard")]
    fn unsynced_restore_installs_and_retries_only_directory_sync(lose_guard: bool) {
        const SYNC_ERROR: &str = "injected directory sync failure";
        smol::block_on(async {
            let (_tmp, dir) = state_dir();
            let (writer, _wake_rx) = manual_writer();
            let mut session = AppSession::new(MODEL, CWD);
            let root = AgentId::generate();
            session.bind_root_id(root).unwrap();
            session.push_message(Message::user(BASE_MSG.into()));
            let id = session.id;
            writer.seed(Arc::new(session.clone()));
            let journal = Arc::new(SessionJournal::from_session(&session).unwrap());
            writer.bind_journal(id, Arc::clone(&journal));
            let stale_fence = writer.session_save_fence(id);
            let (lease, _) = guarded_writer(&writer, &dir, id);
            let (warn_tx, warnings) = flume::unbounded();
            let mut disk = Writer {
                dir: dir.clone(),
                warn_tx,
                logs: HashMap::new(),
                failing: HashSet::new(),
            };
            disk.write(&session).unwrap();
            let mut token = journal
                .prepare_branch_restore(root, journal.snapshot())
                .await
                .unwrap();
            token.snapshot().apply_to(&mut session).unwrap();
            let branch = StoredBranch {
                id: MakimaId::generate(),
                created_at: session.updated_at,
                at_seq: session.sequence_high_water().unwrap(),
                agent: root,
                offset: session.messages().len(),
                transition: None,
            };
            session.push_branch(branch.clone());
            let expected = Arc::new(session);
            let claims = Arc::new(Mutex::new(0));
            let claimed = Arc::clone(&claims);
            let response = enqueue_restore_checkpoint(
                &writer.pending,
                &writer.wake,
                RestoredSessionCheckpoint {
                    session_id: id,
                    session: Arc::clone(&expected),
                    journal: token.snapshot().clone(),
                    branch: branch.clone(),
                    publication: CheckpointPublication::default(),
                    claim: Arc::new(move |publication| {
                        *claimed.lock().unwrap() += 1;
                        publication_claim()(publication)
                    }),
                    guard: lease.publication_guard(),
                },
            )
            .unwrap();
            lock(&writer.pending).restore_sync_error =
                Some(StorageError::from(io::Error::other(SYNC_ERROR)));
            let deadline = disk.flush(&writer.pending).unwrap();
            let fence = response.recv().unwrap().unwrap();
            assert_eq!(
                warnings.recv().unwrap(),
                format!("{RESTORE_UNSYNCED}: {SYNC_ERROR}")
            );
            {
                let state = lock(&writer.pending);
                assert!(Arc::ptr_eq(&state.latest[&id], &expected));
                assert!(Arc::ptr_eq(&state.acknowledged[&id], &expected));
                assert!(!save_fence_current(&state, id, Some(&stale_fence)));
                assert!(save_fence_current(&state, id, Some(&fence)));
                assert_eq!(state.publication_syncs.len(), 1);
                assert!(state.restore_checkpoints.is_empty());
                assert!(state.entries.is_empty());
                assert!(state.staged_checkpoints.is_empty());
                assert!(deadline <= state.publication_syncs[&id].retry_at);
            }
            let synced = publication_sync_waiter(&writer.pending, &fence).unwrap();
            assert!(synced.is_empty());
            let cancel = CancelToken::none();
            let mut wait = Box::pin(writer.await_publication_sync(&fence, &cancel));
            assert!(
                futures_lite::future::poll_once(wait.as_mut())
                    .await
                    .is_none()
            );
            let (trigger, cancelled) = CancelToken::new();
            let mut cancelled_wait = Box::pin(writer.await_publication_sync(&fence, &cancelled));
            assert!(
                futures_lite::future::poll_once(cancelled_wait.as_mut())
                    .await
                    .is_none()
            );
            token.install().unwrap();
            let directory = dir.path().join(SESSIONS_DIR);
            let path = directory.join(format!("{id}.jsonl"));
            let published = std::fs::read(&path).unwrap();
            let branch_count = std::fs::read_dir(directory.join("branches").join(id.to_string()))
                .unwrap()
                .count();
            let retry_at = lock(&writer.pending).publication_syncs[&id].retry_at;
            let mut attempts = 0;
            disk.retry_publication_syncs(
                &writer.pending,
                retry_at - BACKGROUND_RETRY_BACKOFFS[0],
                |_| {
                    attempts += 1;
                    Ok(())
                },
            );
            assert_eq!(attempts, 0);
            if lose_guard {
                lease.release().unwrap();
                disk.retry_publication_syncs(&writer.pending, retry_at, |_| {
                    attempts += 1;
                    Ok(())
                });
                assert_eq!(attempts, 0);
                assert_eq!(warnings.recv().unwrap(), RESTORE_SYNC_ABANDONED);
                assert_eq!(wait.await, Err(RESTORE_SYNC_ABANDONED.into()));
                assert_eq!(synced.recv().unwrap(), Err(RESTORE_SYNC_ABANDONED.into()));
                assert_eq!(
                    publication_sync_waiter(&writer.pending, &fence).unwrap_err(),
                    RESTORE_SYNC_ABANDONED
                );
            } else {
                disk.retry_publication_syncs(&writer.pending, retry_at, |path| {
                    assert_eq!(path, directory.as_path());
                    attempts += 1;
                    Err(StorageError::from(io::Error::other(SYNC_ERROR)))
                });
                assert_eq!(attempts, 1);
                assert!(warnings.is_empty());
                assert!(synced.is_empty());
                assert!(
                    futures_lite::future::poll_once(wait.as_mut())
                        .await
                        .is_none()
                );
                trigger.cancel();
                assert!(cancelled_wait.await.is_err());
                assert!(lock(&writer.pending).publication_syncs.contains_key(&id));
                let retry_at = lock(&writer.pending).publication_syncs[&id].retry_at;
                disk.retry_publication_syncs(&writer.pending, retry_at, |path| {
                    attempts += 1;
                    SessionLog::sync_publication(path)
                });
                assert_eq!(attempts, 2);
                assert_eq!(warnings.recv().unwrap(), RESTORE_SYNC_RECOVERED);
                assert_eq!(wait.await, Ok(()));
                assert_eq!(synced.recv().unwrap(), Ok(()));
            }
            assert!(lock(&writer.pending).publication_syncs.is_empty());
            assert_eq!(*claims.lock().unwrap(), 1);
            assert_eq!(std::fs::read(path).unwrap(), published);
            assert_eq!(
                std::fs::read_dir(directory.join("branches").join(id.to_string()))
                    .unwrap()
                    .count(),
                branch_count
            );
            let loaded = AppSession::load(id, &dir).unwrap();
            assert_eq!(loaded.branches(), expected.branches());
            assert!(loaded.load_branch_from(branch.id, &directory).is_ok());
            assert!(Arc::ptr_eq(&writer.latest_snapshot(id).unwrap(), &expected));
        });
    }

    #[test_case(0; "cancelled")]
    #[test_case(1; "lost_guard")]
    #[test_case(2; "newer_save")]
    #[test_case(3; "rejected_claim")]
    fn failed_restore_checkpoint_keeps_durable_and_pending_state(failure: u8) {
        smol::block_on(async {
            let (_tmp, dir) = state_dir();
            let (writer, _wake_rx) = manual_writer();
            let mut old = AppSession::new(MODEL, CWD);
            let root = AgentId::generate();
            old.bind_root_id(root).unwrap();
            old.push_message(Message::user(BASE_MSG.into()));
            let id = old.id;
            writer.seed(Arc::new(old.clone()));
            let journal = Arc::new(SessionJournal::from_session(&old).unwrap());
            writer.bind_journal(id, Arc::clone(&journal));
            let old_fence = writer.session_save_fence(id);
            let (lease, _) = guarded_writer(&writer, &dir, id);
            let (warn_tx, _warnings) = flume::unbounded();
            let mut disk = Writer {
                dir: dir.clone(),
                warn_tx,
                logs: HashMap::new(),
                failing: HashSet::new(),
            };
            disk.write(&old).unwrap();
            let directory = dir.ensure_subdir(SESSIONS_DIR).unwrap();
            let path = directory.join(format!("{id}.jsonl"));
            let before = std::fs::read(&path).unwrap();
            let token = journal
                .prepare_branch_restore(root, journal.snapshot())
                .await
                .unwrap();
            let mut replacement = old.clone();
            token.snapshot().apply_to(&mut replacement).unwrap();
            let branch = StoredBranch {
                id: MakimaId::generate(),
                created_at: old.updated_at,
                at_seq: old.sequence_high_water().unwrap(),
                agent: root,
                offset: old.messages().len(),
                transition: None,
            };
            replacement.push_branch(branch.clone());
            let publication = CheckpointPublication::default();
            let claim = if failure == 3 {
                Arc::new(|_: &CheckpointPublication| Err(Arc::from(CHECKPOINT_CANCELLED)))
                    as CheckpointClaim
            } else {
                publication_claim()
            };
            let response = enqueue_restore_checkpoint(
                &writer.pending,
                &writer.wake,
                RestoredSessionCheckpoint {
                    session_id: id,
                    session: Arc::new(replacement),
                    journal: token.snapshot().clone(),
                    branch,
                    publication: publication.clone(),
                    claim,
                    guard: lease.publication_guard(),
                },
            )
            .unwrap();
            if failure == 0 {
                publication.cancel();
            }
            if failure == 1 {
                lease.release().unwrap();
            }
            if failure == 2 {
                let mut fresh = old.clone();
                fresh.set_title(TITLE.into());
                writer.send_fenced(Arc::new(fresh), &old_fence);
            }
            let latest = writer.latest_snapshot(id).unwrap();
            let save = lock(&writer.pending).restore_checkpoints.remove(0);
            let error = disk
                .write_restore_checkpoint(&writer.pending, &save)
                .unwrap_err();
            assert_eq!(
                error,
                if failure == 0 || failure == 3 {
                    CHECKPOINT_CANCELLED
                } else {
                    CHECKPOINT_REPLACED
                }
            );
            assert_eq!(std::fs::read(path).unwrap(), before);
            assert!(Arc::ptr_eq(&writer.latest_snapshot(id).unwrap(), &latest));
            assert!(save_fence_current(
                &lock(&writer.pending),
                id,
                Some(&old_fence)
            ));
            save.reply.send(Err(error)).unwrap();
            assert!(response.recv().unwrap().is_err());
            assert_eq!(
                std::fs::read_dir(directory.join("branches").join(id.to_string()))
                    .map(|entries| entries.count())
                    .unwrap_or(0),
                0
            );
            drop(token);
        });
    }

    #[test_case(0; "candidate")]
    #[test_case(1; "latest_promotion")]
    #[test_case(2; "provisional_promotion")]
    #[test_case(3; "queued_promotion")]
    fn config_merge_failure_precedes_publication(target: usize) {
        let (_tmp, dir) = state_dir();
        let (writer, _wake_rx) = manual_writer();
        let session = AppSession::new(MODEL, CWD);
        let id = session.id;
        writer.seed(Arc::new(session.clone()));
        writer.bind_config_runtime(id, 1);
        let (_lease, checkpoint) = guarded_writer(&writer, &dir, id);
        let (warn_tx, _warnings) = flume::unbounded();
        let mut disk = Writer {
            dir: dir.clone(),
            warn_tx,
            logs: HashMap::new(),
            failing: HashSet::new(),
        };
        disk.write(&session).unwrap();
        let path = dir.path().join(SESSIONS_DIR).join(format!("{id}.jsonl"));
        let before = std::fs::read(&path).unwrap();
        let mut invalid = session.clone();
        invalid.push_state(StoredStateRecord {
            seq: 1,
            agent: None,
            generation: 0,
            change: StateChange::Compaction { epoch: 1 },
        });
        let invalid = Arc::new(invalid);
        {
            let mut state = lock(&writer.pending);
            match target {
                0 => {
                    state.acknowledged.insert(id, invalid);
                }
                1 => {
                    state.latest.insert(id, invalid);
                }
                2 => {
                    state.coordinator_pending.insert(
                        id,
                        CoordinatorPending {
                            generation: 1,
                            session: invalid,
                            history_base: None,
                        },
                    );
                }
                _ => {
                    state.entries.insert(
                        id,
                        Entry::Save(PendingSave {
                            journal: None,
                            session: invalid,
                            waiters: Vec::new(),
                            generation: 1,
                            coordinator_generation: None,
                            config_version: None,
                            coordinator_history_base: None,
                            retry_attempt: 0,
                            retry_at: None,
                        }),
                    );
                }
            }
        }
        let mut snapshot = config_checkpoint(1, 1);
        let config = Arc::make_mut(&mut snapshot.config.as_mut().unwrap().config);
        config.mode_def = Some(makima_agent::ModeDef::default_for(config.mode.id()));
        let publication = CheckpointPublication::default();
        let result = checkpoint.checkpoint_guarded(
            CheckpointRequest {
                session_id: id,
                version: CheckpointVersion {
                    epoch: 1,
                    revision: 1,
                },
                snapshot: Arc::new(snapshot),
            },
            publication.clone(),
            publication_claim(),
        );
        let save = lock(&writer.pending).staged_checkpoints.remove(0);
        let error = disk
            .write_staged_checkpoint(&writer.pending, &save)
            .unwrap_err();
        let expected =
            SessionError::InvalidState(StateFoldError::MissingCompactionAgent { seq: 1 })
                .to_string();
        assert_eq!(error, expected);
        assert!(publication.cancel());
        assert_eq!(std::fs::read(path).unwrap(), before);
        save.reply.send(Err(error)).unwrap();
        assert!(smol::block_on(result).is_err());
    }

    #[test_case(false, false, false; "config_then_terminal_history")]
    #[test_case(true, false, false; "terminal_history_then_config")]
    #[test_case(false, true, false; "rejected_claim_keeps_config_and_history")]
    #[test_case(false, false, true; "queued_config_then_terminal_history")]
    #[test_case(true, false, true; "queued_terminal_history_then_config")]
    fn guarded_config_preserves_acknowledged_history(
        history_first: bool,
        reject: bool,
        queued: bool,
    ) {
        let (_tmp, dir) = state_dir();
        let (writer, _wake_rx) = manual_writer();
        let mut session = AppSession::new(MODEL, CWD);
        session.push_message(Message::user(BASE_MSG.into()));
        session.set_subagent_messages(TOOL_ID.into(), vec![Message::user(BASE_MSG.into())]);
        let id = session.id;
        writer.seed(Arc::new(session.clone()));
        writer.bind_config_runtime(id, 1);
        let (_lease, checkpoint) = guarded_writer(&writer, &dir, id);
        let (warn_tx, _warn_rx) = flume::unbounded();
        let mut disk = Writer {
            dir: dir.clone(),
            warn_tx,
            logs: HashMap::new(),
            failing: HashSet::new(),
        };
        disk.write(&session).unwrap();
        session.push_message(Message::user(COORDINATOR_MSG.into()));
        let mut terminal_history = session.history_snapshot();
        terminal_history.compacted = true;
        let version = CheckpointVersion {
            revision: 2,
            epoch: terminal_history.epoch,
        };
        let mut terminal = config_checkpoint(1, 0);
        terminal.kind = SessionCheckpointKind::History;
        terminal.history_revision = Some(2);
        terminal.history = Some(terminal_history.clone());
        let terminal = Arc::new(terminal);
        let claim: CheckpointClaim = Arc::new(|publication| {
            publication
                .try_publish()
                .then_some(())
                .ok_or_else(|| Arc::from(CHECKPOINT_CANCELLED))
        });
        let submit = |revision: u64, snapshot: Arc<SessionCheckpoint>| {
            checkpoint.checkpoint_guarded(
                CheckpointRequest {
                    session_id: id,
                    version: CheckpointVersion {
                        revision,
                        ..version
                    },
                    snapshot,
                },
                CheckpointPublication::default(),
                Arc::clone(&claim),
            )
        };
        let mut queued_history_ack = None;
        if history_first {
            let ack = submit(2, Arc::clone(&terminal));
            if queued {
                queued_history_ack = Some(ack);
            } else {
                disk.flush(&writer.pending);
                assert!(smol::block_on(ack).is_ok());
            }
        }
        let publication = CheckpointPublication::default();
        let config_claim: CheckpointClaim = Arc::new(move |publication| {
            if reject {
                return Err(Arc::from(CHECKPOINT_CANCELLED));
            }
            publication
                .try_publish()
                .then_some(())
                .ok_or_else(|| Arc::from(CHECKPOINT_CANCELLED))
        });
        let ack = checkpoint.checkpoint_guarded(
            CheckpointRequest {
                session_id: id,
                version,
                snapshot: Arc::new(config_checkpoint(1, 1)),
            },
            publication.clone(),
            config_claim,
        );
        if !history_first {
            let mut state = lock(&writer.pending);
            state.latest.insert(id, Arc::new(session.clone()));
            assert_eq!(message_texts(&state.acknowledged[&id]), [BASE_MSG]);
        }
        if queued && !history_first {
            queued_history_ack = Some(submit(2, Arc::clone(&terminal)));
        }
        disk.flush(&writer.pending);
        assert_eq!(smol::block_on(ack).is_err(), reject);
        if let Some(ack) = queued_history_ack {
            assert!(smol::block_on(ack).is_ok());
        }
        let expected_model = MODEL;
        let assert_saved = |saved: &AppSession| {
            assert_eq!(saved.model, expected_model);
            let child = &saved.subagent_messages()[TOOL_ID];
            assert_eq!(child.len(), 1);
            assert_eq!(child[0].user_text(), Some(BASE_MSG));
        };
        let stored = AppSession::load(id, &dir).unwrap();
        assert_saved(&stored);
        assert_saved(&writer.latest_snapshot(id).unwrap());
        assert_eq!(
            message_texts(&stored),
            if history_first || queued {
                vec![BASE_MSG, COORDINATOR_MSG]
            } else {
                vec![BASE_MSG]
            }
        );
        let terminal_ack = submit(3, Arc::clone(&terminal));
        disk.flush(&writer.pending);
        assert!(smol::block_on(terminal_ack).is_ok());
        let stored = AppSession::load(id, &dir).unwrap();
        assert_saved(&stored);
        assert_eq!(message_texts(&stored), [BASE_MSG, COORDINATOR_MSG]);
        assert!(stored.meta.compacted);
        let mut stale = terminal.as_ref().clone();
        stale.history_revision = Some(1);
        stale.history = Some(HistorySnapshot {
            messages: Arc::new(vec![Message::user(REWOUND_MSG.into())]),
            ..terminal_history
        });
        assert!(smol::block_on(submit(4, Arc::new(stale))).is_err());
        assert_eq!(
            message_texts(&AppSession::load(id, &dir).unwrap()),
            [BASE_MSG, COORDINATOR_MSG]
        );
        let mut options = config_checkpoint(1, 0);
        options.kind = SessionCheckpointKind::Options;
        options.history = Some(HistorySnapshot::new(Vec::new()));
        Arc::make_mut(&mut options.options.options)
            .iter_mut()
            .find(|state| state.definition.id.as_ref() == YOLO_OPTION_ID)
            .unwrap()
            .current_value = Arc::from(ENABLED_VALUE);
        let options_ack = submit(5, Arc::new(options));
        disk.flush(&writer.pending);
        assert!(smol::block_on(options_ack).is_ok());
        let stored = AppSession::load(id, &dir).unwrap();
        assert_saved(&stored);
        assert_eq!(stored.meta.yolo, None);
        assert_eq!(writer.latest_snapshot(id).unwrap().meta.yolo, None);
        assert!(stored.meta.compacted);
        assert_eq!(message_texts(&stored), [BASE_MSG, COORDINATOR_MSG]);
        if !reject {
            assert!(!stored.meta.fast);
            assert!(!stored.meta.workflow);
            assert!(!publication.cancel());
        }
    }

    #[test_case(false; "acknowledged_metadata")]
    #[test_case(true; "pending_metadata")]
    fn child_config_payload_does_not_author_legacy_metadata(pending_metadata: bool) {
        let (_tmp, dir) = state_dir();
        let (writer, _wake_rx) = manual_writer();
        let mut session = AppSession::new(MODEL, CWD);
        session.push_message(Message::user(BASE_MSG.into()));
        session.set_subagent_messages(TOOL_ID.into(), vec![Message::user(BASE_MSG.into())]);
        session.set_subagents(
            [TOOL_ID, SIBLING_AGENT_ID]
                .into_iter()
                .map(|key| StoredSubagent {
                    tool_use_id: key.into(),
                    agent_id: None,
                    ambiguous: false,
                    name: key.into(),
                    model: Some(MODEL.into()),
                    thinking: Some(StoredThinking::Adaptive),
                    fast: false,
                })
                .collect(),
        );
        let id = session.id;
        let sibling = session.subagents()[1].clone();
        let mut root_meta = serde_json::to_value(&session.meta).unwrap();
        root_meta.as_object_mut().unwrap().remove("yolo");
        writer.seed(Arc::new(session.clone()));
        if pending_metadata {
            let mut state = lock(&writer.pending);
            let acknowledged = Arc::make_mut(state.acknowledged.get_mut(&id).unwrap());
            acknowledged.set_subagents(vec![sibling.clone()]);
        }
        let lease = session_lock::claim(&dir.ensure_subdir(SESSIONS_DIR).unwrap(), &id)
            .unwrap()
            .unwrap();
        let (warn_tx, _warn_rx) = flume::unbounded();
        let mut disk = Writer {
            dir: dir.clone(),
            warn_tx,
            logs: HashMap::new(),
            failing: HashSet::new(),
        };
        disk.write(&session).unwrap();
        let stage = |child: &str| {
            enqueue_staged_checkpoint(
                &writer.pending,
                &writer.wake,
                StagedSessionCheckpoint {
                    session_id: id,
                    checkpoint: None,
                    journal: None,
                    selected_config: Some((
                        HistoryDestination::Child(child.into()),
                        config_checkpoint(1, 1).config.unwrap().config,
                    )),
                    history: None,
                    publication: CheckpointPublication::default(),
                    claim: publication_claim(),
                    guard: lease.publication_guard(),
                },
            )
            .unwrap()
        };
        let selected = stage(TOOL_ID);
        disk.flush(&writer.pending);
        selected.recv().unwrap().unwrap();
        let orphan = stage(ORPHAN_CHILD);
        disk.flush(&writer.pending);
        assert_eq!(orphan.recv().unwrap().unwrap_err(), SELECTED_CONFIG_MISSING);
        let stored = AppSession::load(id, &dir).unwrap();
        assert_eq!(stored.model, MODEL);
        assert_eq!(message_texts(&stored), [BASE_MSG]);
        let mut metadata = serde_json::to_value(&stored.meta).unwrap();
        metadata.as_object_mut().unwrap().remove("yolo");
        assert_eq!(metadata, root_meta);
        let stored_child = |key: &str| {
            stored
                .subagents()
                .iter()
                .find(|stored| stored.tool_use_id == key)
                .cloned()
        };
        let child = stored_child(TOOL_ID).unwrap();
        assert_eq!(child.model.as_deref(), (!pending_metadata).then_some(MODEL));
        assert_eq!(
            child.thinking,
            (!pending_metadata).then_some(StoredThinking::Adaptive)
        );
        assert!(!child.fast);
        assert_eq!(stored_child(SIBLING_AGENT_ID), Some(sibling));
        assert_eq!(stored_child(ORPHAN_CHILD), None);
    }

    #[test_case(true, false, false; "history_then_config")]
    #[test_case(false, false, false; "config_then_history")]
    #[test_case(true, true, false; "queued_history_then_config")]
    #[test_case(false, true, false; "queued_config_then_history")]
    #[test_case(true, false, true; "rejected_config_keeps_history")]
    fn staged_child_config_and_history_keep_each_other(
        history_first: bool,
        queued: bool,
        reject: bool,
    ) {
        let (_tmp, dir) = state_dir();
        let (writer, _wake_rx) = manual_writer();
        let mut session = AppSession::new(MODEL, CWD);
        session.push_message(Message::user(BASE_MSG.into()));
        session.set_subagent_messages(TOOL_ID.into(), vec![Message::user(BASE_MSG.into())]);
        session.set_subagents(vec![StoredSubagent {
            tool_use_id: TOOL_ID.into(),
            agent_id: None,
            ambiguous: false,
            name: TOOL_ID.into(),
            model: Some(MODEL.into()),
            thinking: Some(StoredThinking::Adaptive),
            fast: false,
        }]);
        let id = session.id;
        writer.seed(Arc::new(session.clone()));
        let lease = session_lock::claim(&dir.ensure_subdir(SESSIONS_DIR).unwrap(), &id)
            .unwrap()
            .unwrap();
        let (warn_tx, _warn_rx) = flume::unbounded();
        let mut disk = Writer {
            dir: dir.clone(),
            warn_tx,
            logs: HashMap::new(),
            failing: HashSet::new(),
        };
        disk.write(&session).unwrap();
        let destination = || HistoryDestination::Child(TOOL_ID.into());
        let edited = HistorySnapshot::new(vec![
            Message::user(BASE_MSG.into()),
            Message::user(COORDINATOR_MSG.into()),
        ]);
        let history_model = if history_first {
            NEXT_MODEL
        } else {
            FAILED_MODEL
        };
        let config_claim: CheckpointClaim = if reject {
            Arc::new(|_| Err(Arc::from(CHECKPOINT_CANCELLED)))
        } else {
            publication_claim()
        };
        let mut replies = Vec::new();
        for history in [history_first, !history_first] {
            let (staged_history, model, claim) = if history {
                (
                    Some((destination(), edited.clone())),
                    history_model,
                    publication_claim(),
                )
            } else {
                (None, FAILED_MODEL, Arc::clone(&config_claim))
            };
            let reply = enqueue_staged_checkpoint(
                &writer.pending,
                &writer.wake,
                StagedSessionCheckpoint {
                    session_id: id,
                    checkpoint: None,
                    journal: None,
                    selected_config: Some((
                        destination(),
                        config_projection(model, CWD, 1, 1).config,
                    )),
                    history: staged_history,
                    publication: CheckpointPublication::default(),
                    claim,
                    guard: lease.publication_guard(),
                },
            )
            .unwrap();
            if !queued {
                disk.flush(&writer.pending);
            }
            replies.push((history, reply));
        }
        disk.flush(&writer.pending);
        for (history, reply) in replies {
            assert_eq!(reply.recv().unwrap().is_err(), reject && !history);
        }
        let stored = AppSession::load(id, &dir).unwrap();
        for saved in [&stored, writer.latest_snapshot(id).unwrap().as_ref()] {
            assert_eq!(message_texts(saved), [BASE_MSG]);
            let child: Vec<_> = saved.subagent_messages()[TOOL_ID]
                .iter()
                .map(|message| message.user_text().unwrap())
                .collect();
            assert_eq!(child, [BASE_MSG, COORDINATOR_MSG]);
            assert_eq!(saved.subagents()[0].model.as_deref(), Some(MODEL));
        }
    }

    #[test_case(false; "no_incoming_history")]
    #[test_case(true; "unidentified_incoming_history")]
    fn missing_child_metadata_is_ambiguous_only_with_incoming_history(has_history: bool) {
        let mut latest = AppSession::new(MODEL, CWD);
        latest.set_subagents(vec![StoredSubagent {
            tool_use_id: TOOL_ID.into(),
            agent_id: Some(SIBLING_AGENT_ID.into()),
            ambiguous: false,
            name: TOOL_ID.into(),
            model: None,
            thinking: None,
            fast: false,
        }]);
        let mut incoming = AppSession::new(MODEL, CWD);
        if has_history {
            incoming.set_subagent_messages(TOOL_ID.into(), vec![Message::user(BASE_MSG.into())]);
        }
        let merged = merge_tui_snapshot(&Arc::new(incoming), &latest, false, None);
        if has_history {
            assert!(merged.subagents()[0].ambiguous);
            assert_eq!(merged.subagents()[0].agent_id, None);
        } else {
            assert!(merged.subagents().is_empty());
        }
    }

    #[test_case(false; "no_latest_history")]
    #[test_case(true; "unidentified_latest_history")]
    fn missing_latest_metadata_is_ambiguous_only_with_history(has_history: bool) {
        let mut latest = AppSession::new(MODEL, CWD);
        if has_history {
            latest.set_subagent_messages(TOOL_ID.into(), vec![Message::user(BASE_MSG.into())]);
        }
        let mut incoming = AppSession::new(MODEL, CWD);
        incoming.set_subagents(vec![StoredSubagent {
            tool_use_id: TOOL_ID.into(),
            agent_id: Some(SIBLING_AGENT_ID.into()),
            ambiguous: false,
            name: TOOL_ID.into(),
            model: None,
            thinking: None,
            fast: false,
        }]);
        let merged = merge_tui_snapshot(&Arc::new(incoming), &latest, false, None);
        let child = &merged.subagents()[0];
        assert_eq!(child.ambiguous, has_history);
        assert_eq!(
            child.agent_id.as_deref(),
            (!has_history).then_some(SIBLING_AGENT_ID)
        );
        if has_history {
            assert_eq!(
                merged.subagent_messages()[TOOL_ID][0].user_text(),
                Some(BASE_MSG)
            );
            let merged_again = merge_tui_snapshot(&merged, &latest, false, None);
            assert!(merged_again.subagents()[0].ambiguous);
            assert!(merged_again.subagents()[0].agent_id.is_none());
        }
    }

    #[test]
    fn legacy_child_without_history_adopts_known_identity() {
        let mut latest = AppSession::new(MODEL, CWD);
        let mut child = StoredSubagent {
            tool_use_id: TOOL_ID.into(),
            agent_id: Some(SIBLING_AGENT_ID.into()),
            ambiguous: false,
            name: TOOL_ID.into(),
            model: None,
            thinking: None,
            fast: false,
        };
        latest.set_subagents(vec![child.clone()]);
        latest.set_subagent_messages(TOOL_ID.into(), vec![Message::user(BASE_MSG.into())]);
        child.agent_id = None;
        let mut incoming = AppSession::new(MODEL, CWD);
        incoming.set_subagents(vec![child]);
        let merged = merge_tui_snapshot(&Arc::new(incoming), &latest, false, None);
        assert!(!merged.subagents()[0].ambiguous);
        assert_eq!(
            merged.subagents()[0].agent_id.as_deref(),
            Some(SIBLING_AGENT_ID)
        );
    }

    #[test]
    fn agent_id_collision_across_snapshots_is_sticky() {
        let child = StoredSubagent {
            tool_use_id: TOOL_ID.into(),
            agent_id: Some(SIBLING_AGENT_ID.into()),
            ambiguous: false,
            name: TOOL_ID.into(),
            model: None,
            thinking: None,
            fast: false,
        };
        let mut latest = AppSession::new(MODEL, CWD);
        latest.set_subagents(vec![child.clone()]);
        let mut incoming = AppSession::new(MODEL, CWD);
        let mut other = child;
        other.tool_use_id = LIVE_CHILD.into();
        incoming.set_subagents(vec![other]);
        let merged = merge_tui_snapshot(&Arc::new(incoming), &latest, false, None);
        assert_eq!(merged.subagents().len(), 2);
        assert!(
            merged
                .subagents()
                .iter()
                .all(|child| child.ambiguous && child.agent_id.is_none())
        );
        let merged_again = merge_tui_snapshot(&merged, &latest, false, None);
        assert!(
            merged_again
                .subagents()
                .iter()
                .all(|child| child.ambiguous && child.agent_id.is_none())
        );
    }

    #[test]
    fn child_identity_collision_survives_snapshot_merge() {
        let mut latest = AppSession::new(MODEL, CWD);
        let child = StoredSubagent {
            tool_use_id: TOOL_ID.into(),
            agent_id: Some("first".into()),
            ambiguous: false,
            name: TOOL_ID.into(),
            model: None,
            thinking: None,
            fast: false,
        };
        latest.set_subagents(vec![child.clone()]);
        let mut incoming = latest.clone();
        let mut other = child;
        other.agent_id = Some("second".into());
        incoming.set_subagents(vec![other]);
        let mut incoming = Arc::new(incoming);
        preserve_child_identity(&mut incoming, &latest);
        assert!(incoming.subagents()[0].ambiguous);
        assert_eq!(incoming.subagents()[0].agent_id, None);
        preserve_child_identity(&mut incoming, &latest);
        assert!(incoming.subagents()[0].ambiguous);
        assert_eq!(incoming.subagents()[0].agent_id, None);
    }

    #[test]
    fn child_config_payload_does_not_reintroduce_root_config_projection() {
        let (_tmp, dir) = state_dir();
        let (writer, _wake_rx) = manual_writer();
        let mut session = AppSession::new(MODEL, CWD);
        session.set_subagents(vec![StoredSubagent {
            tool_use_id: TOOL_ID.into(),
            agent_id: None,
            ambiguous: false,
            name: TOOL_ID.into(),
            model: Some(MODEL.into()),
            thinking: Some(StoredThinking::Adaptive),
            fast: false,
        }]);
        let id = session.id;
        writer.seed(Arc::new(session.clone()));
        writer.bind_config_runtime(id, 1);
        let (lease, checkpoint) = guarded_writer(&writer, &dir, id);
        let (warn_tx, _warn_rx) = flume::unbounded();
        let mut disk = Writer {
            dir: dir.clone(),
            warn_tx,
            logs: HashMap::new(),
            failing: HashSet::new(),
        };
        disk.write(&session).unwrap();
        let root = publish_config(
            &checkpoint,
            CheckpointRequest {
                session_id: id,
                version: CheckpointVersion {
                    revision: 1,
                    epoch: session.history_snapshot().epoch,
                },
                snapshot: Arc::new(config_checkpoint(1, 1)),
            },
        );
        let child = enqueue_staged_checkpoint(
            &writer.pending,
            &writer.wake,
            StagedSessionCheckpoint {
                session_id: id,
                checkpoint: None,
                journal: None,
                selected_config: Some((
                    HistoryDestination::Child(TOOL_ID.into()),
                    config_projection(NEXT_MODEL, CWD, 1, 1).config,
                )),
                history: None,
                publication: CheckpointPublication::default(),
                claim: publication_claim(),
                guard: lease.publication_guard(),
            },
        )
        .unwrap();
        disk.flush(&writer.pending);
        smol::block_on(root).unwrap();
        child.recv().unwrap().unwrap();
        let stored = AppSession::load(id, &dir).unwrap();
        for saved in [&stored, writer.latest_snapshot(id).unwrap().as_ref()] {
            assert_eq!(saved.model, MODEL);
            assert_eq!(saved.subagents()[0].model.as_deref(), Some(MODEL));
        }
    }

    #[test_case(0, false; "published_config_keeps_pending_history")]
    #[test_case(0, true; "rejected_config_keeps_pending_history")]
    #[test_case(1, false; "transient_failure_preserves_pending_history")]
    #[test_case(CHECKPOINT_RETRY_BACKOFFS.len() + 1, false; "exhausted_history_is_not_promoted")]
    fn pending_history_then_guarded_config_keeps_truthful_acks(failures: usize, reject: bool) {
        let (_tmp, dir) = state_dir();
        let (writer, _wake_rx) = manual_writer();
        let mut session = AppSession::new(MODEL, CWD);
        session.push_message(Message::user(BASE_MSG.into()));
        let id = session.id;
        writer.seed(Arc::new(session));
        let (_lease, checkpoint) = guarded_writer(&writer, &dir, id);
        let mut history = config_checkpoint(1, 1);
        history.config = None;
        history.kind = SessionCheckpointKind::History;
        history.history_revision = Some(1);
        history.history = Some(HistorySnapshot::new(vec![Message::user(
            COORDINATOR_MSG.into(),
        )]));
        let history_ack = checkpoint.checkpoint(CheckpointRequest {
            session_id: id,
            version: CheckpointVersion {
                revision: 1,
                epoch: 1,
            },
            snapshot: Arc::new(history),
        });
        let claim: CheckpointClaim = if reject {
            Arc::new(|_| Err(Arc::from(CHECKPOINT_CANCELLED)))
        } else {
            publication_claim()
        };
        let config_ack = checkpoint.checkpoint_guarded(
            CheckpointRequest {
                session_id: id,
                version: CheckpointVersion {
                    revision: 2,
                    epoch: 1,
                },
                snapshot: Arc::new(config_checkpoint(1, 1)),
            },
            CheckpointPublication::default(),
            claim,
        );
        let (warn_tx, _warn_rx) = flume::unbounded();
        let mut disk = Writer {
            dir: dir.clone(),
            warn_tx,
            logs: HashMap::new(),
            failing: HashSet::new(),
        };
        let sessions_path = dir.path().join(SESSIONS_DIR);
        let saved_path = dir.path().join(TITLE);
        if failures > 0 {
            std::fs::rename(&sessions_path, &saved_path).unwrap();
            block_sessions_dir(&dir);
        }
        for _ in 0..failures {
            if let Some(Entry::Save(save)) = lock(&writer.pending).entries.get_mut(&id) {
                save.retry_at = None;
            }
            disk.flush(&writer.pending);
        }
        if failures > 0 {
            std::fs::remove_file(&sessions_path).unwrap();
            std::fs::rename(&saved_path, &sessions_path).unwrap();
        }
        if let Some(Entry::Save(save)) = lock(&writer.pending).entries.get_mut(&id) {
            save.retry_at = None;
        }
        disk.flush(&writer.pending);
        let exhausted = failures > CHECKPOINT_RETRY_BACKOFFS.len();
        assert_eq!(smol::block_on(history_ack).is_ok(), !exhausted);
        let published = smol::block_on(config_ack).is_ok();
        assert_eq!(published, !reject && failures == 0);
        let expected_model = MODEL;
        let expected_text = if exhausted { BASE_MSG } else { COORDINATOR_MSG };
        let reopened = AppSession::load(id, &dir).ok();
        assert_eq!(reopened.is_none(), exhausted);
        let latest = writer.latest_snapshot(id).unwrap();
        for saved in reopened.iter().chain([latest.as_ref()]) {
            assert_eq!(message_texts(saved), [expected_text]);
            assert_eq!(saved.model, expected_model);
        }
        assert!(lock(&writer.pending).coordinator_pending.is_empty());
    }

    #[test_case(false, false; "before_first_config_projection")]
    #[test_case(true, false; "after_config_projection")]
    #[test_case(true, true; "replacement_history_publishes_after_waiter")]
    fn in_flight_history_retry_handles_config_advancement(
        initial_config: bool,
        replace_history: bool,
    ) {
        let (_tmp, dir) = state_dir();
        let (writer, _wake_rx) = manual_writer();
        let mut session = AppSession::new(MODEL, CWD);
        session.push_message(Message::user(BASE_MSG.into()));
        let id = session.id;
        writer.seed(Arc::new(session));
        writer.bind_config_runtime(id, 1);
        let (_lease, checkpoint) = guarded_writer(&writer, &dir, id);
        let (warn_tx, _warn_rx) = flume::unbounded();
        let mut disk = Writer {
            dir: dir.clone(),
            warn_tx,
            logs: HashMap::new(),
            failing: HashSet::new(),
        };
        if initial_config {
            let ack = publish_config(
                &checkpoint,
                CheckpointRequest {
                    session_id: id,
                    version: CheckpointVersion {
                        revision: 1,
                        epoch: 1,
                    },
                    snapshot: Arc::new(config_checkpoint(1, 1)),
                },
            );
            disk.flush(&writer.pending);
            assert!(smol::block_on(ack).is_ok());
        }
        let mut history = config_checkpoint(1, 1);
        history.config = None;
        history.kind = SessionCheckpointKind::History;
        history.history_revision = Some(2);
        history.history = Some(HistorySnapshot::new(vec![Message::user(
            COORDINATOR_MSG.into(),
        )]));
        let mut history_ack = checkpoint.checkpoint(CheckpointRequest {
            session_id: id,
            version: CheckpointVersion {
                revision: 2,
                epoch: 1,
            },
            snapshot: Arc::new(history),
        });
        let Entry::Save(in_flight) = lock(&writer.pending).entries.remove(&id).unwrap() else {
            unreachable!()
        };
        let mut config = config_checkpoint(1, 2);
        if replace_history {
            config.history_revision = Some(3);
            config.history = Some(HistorySnapshot::new(vec![Message::user(BASE_MSG.into())]));
        }
        let config_ack = publish_config(
            &checkpoint,
            CheckpointRequest {
                session_id: id,
                version: CheckpointVersion {
                    revision: 3,
                    epoch: 1,
                },
                snapshot: Arc::new(config),
            },
        );
        assert!(requeue_save(&writer.pending, id, in_flight).is_ok());
        if !replace_history {
            assert!(smol::block_on(futures_lite::future::poll_once(&mut history_ack)).is_none());
        }
        disk.flush(&writer.pending);
        assert!(smol::block_on(history_ack).is_ok());
        assert!(smol::block_on(config_ack).is_ok());
        let expected = if replace_history {
            BASE_MSG
        } else {
            COORDINATOR_MSG
        };
        assert_eq!(
            message_texts(&AppSession::load(id, &dir).unwrap()),
            [expected]
        );
        assert_eq!(
            message_texts(&writer.latest_snapshot(id).unwrap()),
            [expected]
        );
        assert!(lock(&writer.pending).coordinator_pending.is_empty());
    }

    #[test_case(false; "older_config_save")]
    #[test_case(true; "older_untagged_save")]
    fn stale_save_does_not_publish_or_clear_latest_config_warning(untagged: bool) {
        const FAILURE: &str = "test disk failure";
        let (_tmp, dir) = state_dir();
        let (writer, _wake_rx) = manual_writer();
        let session = Arc::new(AppSession::new(MODEL, CWD));
        let id = session.id;
        writer.seed(Arc::clone(&session));
        let enqueue = |epoch| {
            writer.bind_config_runtime(id, epoch);
            writer.send(Arc::clone(&session));
            let Entry::Save(save) = lock(&writer.pending).entries.remove(&id).unwrap() else {
                unreachable!()
            };
            save
        };
        let mut old = enqueue(1);
        if untagged {
            old.config_version = None;
        }
        let latest = enqueue(2);
        let (warn_tx, warn_rx) = flume::unbounded();
        let mut disk = Writer {
            dir,
            warn_tx,
            logs: HashMap::new(),
            failing: HashSet::new(),
        };
        disk.report_save(&writer.pending, &old, Err(FAILURE));
        assert!(warn_rx.try_recv().is_err());
        assert!(!disk.failing.contains(&id));
        disk.report_save(&writer.pending, &latest, Err(FAILURE));
        assert_eq!(
            warn_rx.try_recv().unwrap(),
            format!("{SAVE_FAILED_PREFIX}: {FAILURE}")
        );
        disk.report_save(&writer.pending, &latest, Err(FAILURE));
        assert!(warn_rx.try_recv().is_err());
        disk.report_save(&writer.pending, &old, Ok(()));
        assert!(disk.failing.contains(&id));
        assert!(warn_rx.try_recv().is_err());
        disk.report_save(&writer.pending, &latest, Ok(()));
        assert_eq!(warn_rx.try_recv().unwrap(), SAVE_RECOVERED);
        assert!(!disk.failing.contains(&id));
    }

    #[test_case(false, false; "bind_before_flush")]
    #[test_case(true, false; "stale_requeue_without_newer_save")]
    #[test_case(true, true; "stale_requeue_with_newer_config")]
    fn replaced_history_payload_fails_waiter_and_persists_authoritative_state(
        in_flight: bool,
        newer_config: bool,
    ) {
        let (_tmp, dir) = state_dir();
        let (writer, _wake_rx) = manual_writer();
        let mut session = AppSession::new(MODEL, CWD);
        session.push_message(Message::user(BASE_MSG.into()));
        let root = AgentId::generate();
        session.bind_root_id(root).unwrap();
        let id = session.id;
        let journal = Arc::new(SessionJournal::from_session(&session).unwrap());
        writer.seed(Arc::new(session));
        writer.bind_journal(id, Arc::clone(&journal));
        writer.bind_config_runtime(id, 1);
        let (_lease, checkpoint) = guarded_writer(&writer, &dir, id);
        let mut history = config_checkpoint(1, 1);
        history.config = None;
        history.kind = SessionCheckpointKind::History;
        history.history_revision = Some(1);
        history.history = Some(HistorySnapshot::new(vec![Message::user(
            COORDINATOR_MSG.into(),
        )]));
        let history_ack = checkpoint.checkpoint(CheckpointRequest {
            session_id: id,
            version: CheckpointVersion {
                revision: 1,
                epoch: 1,
            },
            snapshot: Arc::new(history),
        });
        let old_save = if in_flight {
            let Entry::Save(save) = lock(&writer.pending).entries.remove(&id).unwrap() else {
                unreachable!()
            };
            Some(save)
        } else {
            None
        };
        writer.bind_config_runtime(id, 2);
        let mut prepared = newer_config.then(|| {
            smol::block_on(journal.prepare(JournalCommit {
                changes: vec![JournalChange {
                    agent: Some(root),
                    generation: 1,
                    change: StateChange::Config(stored_config(FAILED_MODEL, CWD)),
                }],
                ..Default::default()
            }))
            .unwrap()
        });
        let config_ack = newer_config.then(|| {
            let mut config = config_checkpoint(2, 1);
            config.journal = prepared.as_ref().map(|token| token.snapshot().clone());
            publish_config(
                &checkpoint,
                CheckpointRequest {
                    session_id: id,
                    version: CheckpointVersion {
                        revision: 2,
                        epoch: 2,
                    },
                    snapshot: Arc::new(config),
                },
            )
        });
        if let Some(save) = old_save {
            assert!(requeue_save(&writer.pending, id, save).is_ok());
        }
        let Err(CheckpointError::Save { message, .. }) = smol::block_on(history_ack) else {
            panic!("discarded history must fail its checkpoint")
        };
        assert_eq!(message.as_ref(), CHECKPOINT_REPLACED);
        {
            let state = lock(&writer.pending);
            let Entry::Save(save) = &state.entries[&id] else {
                unreachable!()
            };
            assert!(save.waiters.is_empty());
            assert_eq!(message_texts(&save.session), [BASE_MSG]);
        }
        let (warn_tx, _warn_rx) = flume::unbounded();
        let mut disk = Writer {
            dir: dir.clone(),
            warn_tx,
            logs: HashMap::new(),
            failing: HashSet::new(),
        };
        disk.flush(&writer.pending);
        if let Some(ack) = config_ack {
            assert!(smol::block_on(ack).is_ok());
        }
        if let Some(prepared) = prepared.take() {
            prepared.commit().unwrap();
        }
        let reopened = AppSession::load(id, &dir).unwrap();
        assert_eq!(message_texts(&reopened), [BASE_MSG]);
        assert_eq!(
            reopened.model,
            if newer_config { FAILED_MODEL } else { MODEL }
        );
        let view = reopened.latest_state().unwrap();
        assert_eq!(
            view.scopes
                .get(&Some(root))
                .and_then(|scope| scope.config.as_ref()),
            newer_config
                .then(|| stored_config(FAILED_MODEL, CWD))
                .as_ref()
        );
    }

    #[test]
    fn registered_runtime_rejects_old_config_before_first_new_projection() {
        let (_tmp, dir) = state_dir();
        let (writer, _wake_rx) = manual_writer();
        let session = AppSession::new(MODEL, CWD);
        let id = session.id;
        writer.seed(Arc::new(session));
        let (_lease, checkpoint) = guarded_writer(&writer, &dir, id);
        let enqueue = || {
            publish_config(
                &checkpoint,
                CheckpointRequest {
                    session_id: id,
                    version: CheckpointVersion {
                        revision: 1,
                        epoch: 1,
                    },
                    snapshot: Arc::new(config_checkpoint(1, 1)),
                },
            )
        };
        let old_ack = enqueue();
        writer.bind_config_runtime(id, 2);
        assert!(smol::block_on(enqueue()).is_err());
        let (warn_tx, _warn_rx) = flume::unbounded();
        Writer {
            dir,
            warn_tx,
            logs: HashMap::new(),
            failing: HashSet::new(),
        }
        .flush(&writer.pending);
        assert!(smol::block_on(old_ack).is_err());
        assert_eq!(lock(&writer.pending).config_versions[&id], (2, 0));
    }

    fn journal_commit(journal: &SessionJournal, agent: AgentId, closed: bool) {
        journal
            .commit(JournalCommit {
                agents: vec![StoredAgentRecord {
                    id: agent,
                    parent: None,
                    depth: 0,
                    transcript_key: String::new(),
                    lifecycle: if closed {
                        StoredAgentLifecycle::Closed
                    } else {
                        StoredAgentLifecycle::Open
                    },
                    ancestry_unknown: false,
                    label: None,
                    spawned_by_tool_use_id: None,
                    spec: None,
                    restorable: Ok(()),
                    usage: Default::default(),
                    created_seq: 0,
                    updated_seq: 0,
                    legacy: false,
                }],
                changes: vec![JournalChange {
                    agent: Some(agent),
                    generation: 0,
                    change: StateChange::Rules {
                        rules: Vec::new(),
                        yolo: Some(closed),
                    },
                }],
                ..Default::default()
            })
            .unwrap();
    }

    #[test_case(false; "background")]
    #[test_case(true; "checkpoint")]
    fn tui_snapshot_cannot_author_config_or_rules(checkpoint: bool) {
        use makima_agent::{ModeDef, ModeId};

        let (_tmp, dir) = state_dir();
        let (writer, _wake_rx) = manual_writer();
        let mut session = AppSession::new(MODEL, CWD);
        let root = AgentId::generate();
        session.bind_root_id(root).unwrap();
        session.push_message(Message::user(BASE_MSG.into()));
        writer.seed(Arc::new(session.clone()));
        let journal = Arc::new(SessionJournal::from_session(&session).unwrap());
        let mut config = (*config_projection(NEXT_MODEL, NEXT_CWD, 1, 1).config).clone();
        config.mode = AgentMode::Build;
        config.mode_def = Some(ModeDef::default_for(ModeId::parse("build")));
        let stored = stored_agent_config(&config, Default::default()).unwrap();
        journal
            .commit(JournalCommit {
                changes: vec![
                    JournalChange {
                        agent: Some(root),
                        generation: 1,
                        change: StateChange::Config(stored.clone()),
                    },
                    JournalChange {
                        agent: None,
                        generation: 1,
                        change: StateChange::Rules {
                            rules: Vec::new(),
                            yolo: Some(true),
                        },
                    },
                ],
                ..Default::default()
            })
            .unwrap();
        writer.bind_journal(session.id, Arc::clone(&journal));
        session.push_state(StoredStateRecord {
            seq: journal.snapshot().seq + 1,
            agent: Some(root),
            generation: 2,
            change: StateChange::Rules {
                rules: Vec::new(),
                yolo: Some(false),
            },
        });
        session.model = FAILED_MODEL.into();
        session.cwd = CWD.into();
        session.meta.yolo = Some(false);
        let id = session.id;
        let ack = if checkpoint {
            Some(writer.checkpoint(CheckpointRequest {
                session_id: id,
                version: CheckpointVersion {
                    epoch: 1,
                    revision: 1,
                },
                snapshot: Arc::new(session),
            }))
        } else {
            writer.send(Arc::new(session));
            None
        };
        let (warn_tx, _warnings) = flume::unbounded();
        let mut disk = Writer {
            dir: dir.clone(),
            warn_tx,
            logs: HashMap::new(),
            failing: HashSet::new(),
        };
        disk.flush(&writer.pending);
        if let Some(ack) = ack {
            smol::block_on(ack).unwrap();
        }
        let loaded = AppSession::load(id, &dir).unwrap();
        assert_eq!(loaded.state(), journal.snapshot().state);
        assert_eq!(
            writer.latest_snapshot(id).unwrap().state(),
            journal.snapshot().state
        );
        assert!(loaded.state().iter().any(
            |record| matches!(&record.change, StateChange::Config(config) if config == &stored)
        ));
        assert_eq!(message_texts(&loaded), [BASE_MSG]);
    }

    #[test]
    fn child_identity_merge_does_not_copy_legacy_config() {
        let mut latest = AppSession::new(NEXT_MODEL, NEXT_CWD);
        latest.set_subagent_messages(TOOL_ID.into(), vec![Message::user(BASE_MSG.into())]);
        latest.set_subagents(vec![StoredSubagent {
            tool_use_id: TOOL_ID.into(),
            agent_id: Some(SIBLING_AGENT_ID.into()),
            ambiguous: false,
            name: TOOL_ID.into(),
            model: Some(NEXT_MODEL.into()),
            thinking: Some(StoredThinking::Adaptive),
            fast: true,
        }]);
        let mut incoming = latest.clone();
        incoming.set_subagents(vec![StoredSubagent {
            model: Some(MODEL.into()),
            thinking: Some(StoredThinking::Off),
            fast: false,
            ..latest.subagents()[0].clone()
        }]);
        let merged = merge_tui_snapshot(&Arc::new(incoming), &latest, false, None);
        let child = &merged.subagents()[0];
        assert_eq!(child.agent_id.as_deref(), Some(SIBLING_AGENT_ID));
        assert!(!child.ambiguous);
        assert_eq!(child.model.as_deref(), Some(MODEL));
        assert_eq!(child.thinking, Some(StoredThinking::Off));
        assert!(!child.fast);
    }

    #[test_case(false; "background")]
    #[test_case(true; "checkpoint")]
    fn journal_projection_survives_old_ui_snapshots(checkpoint: bool) {
        let (_tmp, dir) = state_dir();
        let (writer, _wake_rx) = manual_writer();
        let session = Arc::new(AppSession::new(MODEL, CWD));
        let id = session.id;
        writer.seed(Arc::clone(&session));
        let journal = Arc::new(SessionJournal::from_session(&session).unwrap());
        let agent = AgentId::generate();
        writer.bind_journal(id, Arc::clone(&journal));
        journal_commit(&journal, agent, false);
        writer.send(Arc::clone(&session));
        journal_commit(&journal, agent, true);
        let ack = if checkpoint {
            Some(writer.checkpoint(CheckpointRequest {
                session_id: id,
                version: CheckpointVersion {
                    epoch: 0,
                    revision: 0,
                },
                snapshot: Arc::clone(&session),
            }))
        } else {
            writer.send(Arc::clone(&session));
            None
        };
        let (warn_tx, _warnings) = flume::unbounded();
        let mut disk = Writer {
            dir: dir.clone(),
            warn_tx,
            logs: HashMap::new(),
            failing: HashSet::new(),
        };
        assert_eq!(journal.published_revision(), 0);
        disk.flush(&writer.pending);
        if let Some(ack) = ack {
            smol::block_on(ack).unwrap();
        }
        assert_eq!(journal.published_revision(), 2);
        let reopened = AppSession::load(id, &dir).unwrap();
        assert_eq!(
            reopened.agent_revisions(),
            journal.snapshot().agent_revisions
        );
        assert_eq!(reopened.state(), journal.snapshot().state);
        assert_eq!(
            reopened.agents()[&agent].lifecycle,
            StoredAgentLifecycle::Closed
        );
    }

    #[test]
    fn journal_failure_retries_captured_prefix_without_acknowledging_newer_commits() {
        let (tmp, dir) = state_dir();
        let (writer, _wake_rx) = manual_writer();
        let session = Arc::new(AppSession::new(MODEL, CWD));
        let id = session.id;
        writer.seed(Arc::clone(&session));
        let journal = Arc::new(SessionJournal::from_session(&session).unwrap());
        writer.bind_journal(id, Arc::clone(&journal));
        let agent = AgentId::generate();
        journal_commit(&journal, agent, false);
        writer.send(Arc::clone(&session));
        let captured = journal.snapshot();
        journal_commit(&journal, agent, true);
        std::fs::write(tmp.path().join(SESSIONS_DIR), BASE_MSG).unwrap();
        let (warn_tx, _warnings) = flume::unbounded();
        let mut disk = Writer {
            dir: dir.clone(),
            warn_tx,
            logs: HashMap::new(),
            failing: HashSet::new(),
        };
        disk.flush(&writer.pending);
        assert_eq!(journal.published_revision(), 0);
        std::fs::remove_file(tmp.path().join(SESSIONS_DIR)).unwrap();
        if let Some(Entry::Save(save)) = lock(&writer.pending).entries.get_mut(&id) {
            save.retry_at = None;
        }
        disk.flush(&writer.pending);
        assert_eq!(journal.published_revision(), captured.revision);
        let reopened = AppSession::load(id, &dir).unwrap();
        assert_eq!(reopened.agent_revisions(), captured.agent_revisions);
        assert_eq!(reopened.state(), captured.state);
    }

    #[test_case(false, false, false; "state_only")]
    #[test_case(true, false, false; "retained_retry_prefix")]
    #[test_case(false, true, false; "failure_after_checkpoint_enqueue")]
    #[test_case(false, true, true; "failure_after_coordinator_enqueue")]
    fn journal_conversion_failure_retries_without_ui_events(
        retain_prefix: bool,
        fail_after_enqueue: bool,
        coordinator: bool,
    ) {
        use makima_agent::tools::offload::OffloadBackend;
        use makima_agent::{DoneReason, TurnOutcome, TurnResult};
        use makima_storage::id::TurnId;
        use makima_storage::session_state::MAX_INLINE_TURN_RESULT_BYTES;

        const CONVERSION_FAILED: &str = "journal conversion failed";

        struct GatedConversion {
            attempts: Mutex<usize>,
            first_entered: flume::Sender<()>,
            first_release: flume::Receiver<()>,
            entered: flume::Sender<()>,
            release: flume::Receiver<()>,
            path: PathBuf,
        }

        impl OffloadBackend for GatedConversion {
            fn create_new(&self, name: &str, bytes: &[u8]) -> io::Result<bool> {
                let mut attempts = self.attempts.lock().unwrap();
                *attempts += 1;
                if *attempts == 1 {
                    self.first_entered.send(()).unwrap();
                    self.first_release.recv_timeout(DRAIN_TIMEOUT).unwrap();
                    return Err(io::Error::other(CONVERSION_FAILED));
                }
                if *attempts == 2 {
                    self.entered.send(()).unwrap();
                    self.release.recv_timeout(DRAIN_TIMEOUT).unwrap();
                }
                std::fs::write(self.path.join(name), bytes)?;
                Ok(true)
            }

            fn total_bytes(&self) -> io::Result<u64> {
                Ok(0)
            }

            fn remove_all(&self) -> io::Result<()> {
                Ok(())
            }

            fn path(&self, name: &str) -> PathBuf {
                self.path.join(name)
            }
        }

        let (tmp, dir) = state_dir();
        let mut session = AppSession::new(MODEL, CWD);
        session.push_message(Message::user(BASE_MSG.into()));
        session.save(&dir).unwrap();
        let id = session.id;
        let journal = Arc::new(SessionJournal::from_session(&session).unwrap());
        journal.bind_session(id).unwrap();
        let agent = AgentId::generate();
        journal_commit(&journal, agent, false);
        let prefix = Arc::new(journal.snapshot());
        let (entered, retry_entered) = flume::bounded(1);
        let (release, released) = flume::bounded(1);
        let (first_entered, first_attempt) = flume::bounded(1);
        let (first_release, first_released) = flume::bounded(1);
        let store = Arc::new(OffloadStore::new(Box::new(GatedConversion {
            attempts: Mutex::new(0),
            first_entered,
            first_release: first_released,
            entered,
            release: released,
            path: tmp.path().to_path_buf(),
        })));
        let turn = TurnId::generate();
        journal
            .record_terminal(
                TurnResult {
                    outcome: TurnOutcome::completed(
                        agent,
                        turn,
                        Default::default(),
                        1,
                        DoneReason::EndTurn,
                    ),
                    text: BASE_MSG.repeat(MAX_INLINE_TURN_RESULT_BYTES),
                    output: Vec::new(),
                    provenance: Default::default(),
                    duration_ms: 0,
                },
                Some(store),
            )
            .unwrap();
        journal.release_terminal(agent, turn);
        first_attempt.recv_timeout(DRAIN_TIMEOUT).unwrap();
        let early = fail_after_enqueue.then(|| {
            let (writer, warnings) = writer(&dir);
            writer.seed(Arc::new(session.clone()));
            let (saving, save_entered) = flume::unbounded();
            let (save_release, save_released) = flume::unbounded();
            lock(&writer.pending).save_gate = Some((saving, save_released));
            writer.bind_journal(id, Arc::clone(&journal));
            let checkpoint = if coordinator {
                let mut checkpoint = config_checkpoint(1, 1);
                checkpoint.kind = SessionCheckpointKind::History;
                checkpoint.history = Some(session.history_snapshot());
                checkpoint.history_revision = Some(1);
                writer
                    .coordinator_checkpoint()
                    .checkpoint(CheckpointRequest {
                        session_id: id,
                        version: CheckpointVersion {
                            epoch: 1,
                            revision: 1,
                        },
                        snapshot: Arc::new(checkpoint),
                    })
            } else {
                writer.checkpoint(CheckpointRequest {
                    session_id: id,
                    version: CheckpointVersion {
                        epoch: 1,
                        revision: 1,
                    },
                    snapshot: Arc::new(session.clone()),
                })
            };
            save_entered.recv_timeout(DRAIN_TIMEOUT).unwrap();
            (writer, warnings, checkpoint, save_release)
        });
        first_release.send(()).unwrap();
        retry_entered.recv_timeout(DRAIN_TIMEOUT).unwrap();
        let (writer, warnings) = if let Some((writer, warnings, checkpoint, save_release)) = early {
            lock(&writer.pending).save_gate = None;
            save_release.send(()).unwrap();
            let error = smol::block_on(async {
                futures_lite::future::or(checkpoint, async {
                    smol::Timer::after(DRAIN_TIMEOUT).await;
                    panic!("checkpoint waiter was not failed after conversion error")
                })
                .await
            })
            .unwrap_err()
            .to_string();
            assert!(error.contains(JOURNAL_UNSAVED), "{error}");
            let state = lock(&writer.pending);
            assert!(!state.coordinator_pending.contains_key(&id));
            if let Some(Entry::Save(save)) = state.entries.get(&id) {
                assert!(save.waiters.is_empty());
                assert!(save.coordinator_generation.is_none());
            }
            drop(state);
            (writer, warnings)
        } else {
            let (writer, warnings) = writer(&dir);
            writer.seed(Arc::new(session.clone()));
            writer.bind_journal(id, Arc::clone(&journal));
            (writer, warnings)
        };
        if retain_prefix {
            let mut state = lock(&writer.pending);
            let generation = state.latest_generations[&id];
            let captured = JournalCheckpoint {
                snapshot: Arc::clone(&prefix),
                ..state.journals[&id].clone()
            };
            let mut snapshot = Arc::new(session.clone());
            prefix.apply_to(Arc::make_mut(&mut snapshot)).unwrap();
            state.entries.insert(
                id,
                Entry::Save(PendingSave {
                    journal: Some(captured),
                    session: snapshot,
                    waiters: Vec::new(),
                    generation,
                    coordinator_generation: None,
                    config_version: None,
                    coordinator_history_base: None,
                    retry_attempt: 1,
                    retry_at: Some(Instant::now() + DEFERRED_RETRY),
                }),
            );
        }
        let warning = warnings.recv_timeout(DRAIN_TIMEOUT).unwrap();
        assert!(warning.starts_with(SAVE_FAILED_PREFIX), "{warning}");
        assert!(warning.contains(JOURNAL_UNSAVED), "{warning}");
        assert!(warning.contains(CONVERSION_FAILED), "{warning}");
        assert_eq!(journal.published_revision(), 0);
        assert!(AppSession::load(id, &dir).unwrap().state().is_empty());
        let captured = capture_journal(&writer.pending, id);
        assert!(
            project_journal(
                &lock(&writer.pending),
                id,
                captured.as_ref(),
                &mut Arc::new(session.clone()),
            )
            .unwrap_err()
            .contains(JOURNAL_UNSAVED)
        );
        if retain_prefix {
            let mut state = lock(&writer.pending);
            let Some(Entry::Save(save)) = state.entries.get_mut(&id) else {
                unreachable!()
            };
            assert!(Arc::ptr_eq(
                &save.journal.as_ref().unwrap().snapshot,
                &prefix
            ));
            save.retry_at = None;
        }
        let (lease, _) = guarded_writer(&writer, &dir, id);
        let publication = CheckpointPublication::default();
        let replacement = enqueue_staged_checkpoint(
            &writer.pending,
            &writer.wake,
            StagedSessionCheckpoint {
                session_id: id,
                checkpoint: None,
                journal: None,
                selected_config: None,
                history: Some((HistoryDestination::Main, HistorySnapshot::new(Vec::new()))),
                publication: publication.clone(),
                claim: publication_claim(),
                guard: lease.publication_guard(),
            },
        )
        .unwrap();
        assert!(
            replacement
                .recv_timeout(DRAIN_TIMEOUT)
                .unwrap()
                .unwrap_err()
                .contains(JOURNAL_UNSAVED)
        );
        assert!(publication.cancel());
        assert_eq!(journal.published_revision(), 0);
        assert_eq!(
            message_texts(&AppSession::load(id, &dir).unwrap()),
            [BASE_MSG]
        );
        let (saving, save_entered) = flume::unbounded();
        let (save_release, save_released) = flume::unbounded();
        lock(&writer.pending).save_gate = Some((saving, save_released));
        release.send(()).unwrap();
        let mut remaining = if retain_prefix || fail_after_enqueue {
            2
        } else {
            1
        };
        let deadline = Instant::now() + DRAIN_TIMEOUT;
        while remaining > 0 {
            save_entered
                .recv_timeout(deadline.saturating_duration_since(Instant::now()))
                .unwrap();
            if journal.persistence_error().is_none() {
                remaining -= 1;
            }
            save_release.send(()).unwrap();
        }
        assert_eq!(
            warnings.recv_timeout(DRAIN_TIMEOUT).unwrap(),
            SAVE_RECOVERED
        );
        writer.shutdown(DRAIN_TIMEOUT);
        let reopened = AppSession::load(id, &dir).unwrap();
        assert_eq!(reopened.state(), journal.snapshot().state);
        assert!(reopened.state().iter().any(|record| {
            matches!(record.change, StateChange::Turn { turn: saved, .. } if saved == turn)
        }));
        assert_eq!(journal.published_revision(), journal.snapshot().revision);
    }

    #[test]
    fn journal_binding_rejects_foreign_session_without_mutating_pending() {
        let (writer, _wake_rx) = manual_writer();
        let id = MakimaId::generate();
        let journal = Arc::new(SessionJournal::default());
        writer.bind_journal(id, Arc::clone(&journal));
        assert_eq!(journal.session_id(), Some(id));
        let captured = capture_journal(&writer.pending, id).unwrap();
        let publication = CheckpointPublication::default();
        let incarnation = Arc::clone(&lock(&writer.pending).incarnations[&id]);
        lock(&writer.pending)
            .publications
            .insert(id, vec![publication.clone()]);
        let foreign_id = MakimaId::generate();
        let foreign = Arc::new(SessionJournal::default());
        foreign.bind_session(foreign_id).unwrap();
        writer.bind_journal(id, Arc::clone(&foreign));
        assert_eq!(foreign.session_id(), Some(foreign_id));
        assert!(!publication.is_cancelled());
        let state = lock(&writer.pending);
        assert!(journal_current(&state, id, Some(&captured)));
        assert!(Arc::ptr_eq(&state.journals[&id].journal, &journal));
        assert!(Arc::ptr_eq(&state.incarnations[&id], &incarnation));
        assert_eq!(state.publications[&id].len(), 1);
    }

    #[test_case(false, false; "commit")]
    #[test_case(false, true; "lost_guard_commit_stays_suspended")]
    #[test_case(true, false; "rollback")]
    #[test_case(true, true; "failed_rollback_stays_suspended")]
    fn replacement_recovery_keeps_old_binding_until_resolution(rollback: bool, lose_guard: bool) {
        let (_tmp, dir) = state_dir();
        let (writer, _wake_rx) = manual_writer();
        let mut session = AppSession::new(MODEL, CWD);
        session.push_message(Message::user(BASE_MSG.into()));
        session.save(&dir).unwrap();
        let id = session.id;
        writer.seed(Arc::new(session.clone()));
        let journal = Arc::new(SessionJournal::from_session(&session).unwrap());
        writer.bind_journal(id, Arc::clone(&journal));
        let lease = session_lock::claim(&dir.ensure_subdir(SESSIONS_DIR).unwrap(), &id)
            .unwrap()
            .unwrap();
        let mut replacement = session.clone();
        replacement.adopt_history(&HistorySnapshot::new(vec![Message::user(
            REWOUND_MSG.into(),
        )]));
        let candidate = RecoveryCandidate::prepare(replacement).unwrap();
        let mut recovering =
            Box::pin(writer.recover_replacing(candidate, lease.publication_guard()));
        assert!(smol::block_on(futures_lite::future::poll_once(recovering.as_mut())).is_none());
        let (warn_tx, _warnings) = flume::unbounded();
        let mut disk = Writer {
            dir: dir.clone(),
            warn_tx,
            logs: HashMap::new(),
            failing: HashSet::new(),
        };
        disk.flush(&writer.pending);
        let (_, publication) = smol::block_on(recovering).unwrap();
        assert_eq!(
            message_texts(&AppSession::load(id, &dir).unwrap()),
            [REWOUND_MSG]
        );
        assert!(Arc::ptr_eq(
            &lock(&writer.pending).journals[&id].journal,
            &journal
        ));
        writer.send(Arc::new(session.clone()));
        disk.flush(&writer.pending);
        assert_eq!(
            message_texts(&AppSession::load(id, &dir).unwrap()),
            [REWOUND_MSG]
        );
        if rollback {
            if lose_guard {
                lease.release().unwrap();
            }
            let mut rolling_back = Box::pin(publication.rollback());
            assert!(
                smol::block_on(futures_lite::future::poll_once(rolling_back.as_mut())).is_none()
            );
            disk.flush(&writer.pending);
            let result = smol::block_on(rolling_back);
            assert_eq!(result.is_err(), lose_guard);
            assert_eq!(
                lock(&writer.pending)
                    .recovery_transactions
                    .contains_key(&id),
                lose_guard
            );
            if !lose_guard {
                assert_eq!(
                    message_texts(&AppSession::load(id, &dir).unwrap()),
                    [BASE_MSG]
                );
                assert!(Arc::ptr_eq(
                    &lock(&writer.pending).journals[&id].journal,
                    &journal
                ));
                writer.send(Arc::new(session));
                disk.flush(&writer.pending);
            }
        } else if lose_guard {
            lease.release().unwrap();
            assert!(publication.commit().is_err());
            assert!(
                lock(&writer.pending)
                    .recovery_transactions
                    .contains_key(&id)
            );
            assert!(Arc::ptr_eq(
                &lock(&writer.pending).journals[&id].journal,
                &journal
            ));
            assert_eq!(
                message_texts(&AppSession::load(id, &dir).unwrap()),
                [REWOUND_MSG]
            );
        } else {
            publication.commit().unwrap();
            assert!(
                !lock(&writer.pending)
                    .recovery_transactions
                    .contains_key(&id)
            );
            assert!(!Arc::ptr_eq(
                &lock(&writer.pending).journals[&id].journal,
                &journal
            ));
            assert_eq!(
                message_texts(&AppSession::load(id, &dir).unwrap()),
                [REWOUND_MSG]
            );
        }
    }

    #[test]
    fn coordinator_options_publish_while_holding_prepared_journal_token() {
        use makima_agent::actor::{ActorHistory, BackendResult, TurnContext, WorkKind};
        use makima_agent::mailbox::SessionMailbox;
        use makima_agent::permissions::PermissionManager;
        use makima_agent::session_coordinator::{
            SessionCoordinatorHandle, SessionCoordinatorParams, SessionOptionCatalog,
        };
        use makima_agent::{
            ActorBackend, AgentInput, AgentLimits, AgentManagerHandle, ControlWork, ModeDef, ModeId,
        };
        use makima_config::{ModelPolicy, ProjectConfig};

        struct Backend;
        impl ActorBackend for Backend {
            fn run_turn<'a>(
                &'a mut self,
                _: ActorHistory<'a>,
                _: TurnContext,
                _: AgentInput,
                _: WorkKind,
            ) -> BoxFuture<'a, BackendResult> {
                Box::pin(async { unreachable!() })
            }
            fn run_control<'a>(
                &'a mut self,
                _: ActorHistory<'a>,
                _: TurnContext,
                _: &'a ControlWork,
            ) -> BoxFuture<'a, BackendResult> {
                Box::pin(async { unreachable!() })
            }
            fn run_compact<'a>(
                &'a mut self,
                _: ActorHistory<'a>,
                _: TurnContext,
                _: Option<&'a str>,
            ) -> BoxFuture<'a, BackendResult> {
                Box::pin(async { unreachable!() })
            }
        }

        smol::block_on(async {
            let (_tmp, dir) = state_dir();
            let (writer, _warnings) = writer(&dir);
            let session = AppSession::new(MODEL, CWD);
            let id = session.id;
            writer.seed(Arc::new(session));
            let manager = AgentManagerHandle::new(AgentLimits::default()).unwrap();
            let root = manager
                .create_root(Vec::new(), None, Box::new(Backend))
                .unwrap();
            let mut config = (*config_projection(FAILED_MODEL, CWD, 1, 1).config).clone();
            config.mode = AgentMode::Build;
            config.mode_def = Some(ModeDef::default_for(ModeId::parse("build")));
            root.actor().unwrap().initialize_config(config).unwrap();
            writer.bind_journal(id, manager.journal());
            let (_lease, checkpoint) = guarded_writer(&writer, &dir, id);
            let coordinator = SessionCoordinatorHandle::register(SessionCoordinatorParams {
                session_id: id,
                catalog: SessionOptionCatalog::default(),
                definitions: builtin_option_definitions(
                    FAILED_MODEL,
                    [Arc::from(FAILED_MODEL)],
                    false,
                    true,
                    true,
                    ThinkingConfig::Off,
                ),
                persisted_options: Default::default(),
                model_policy: Arc::new(ModelPolicy::default()),
                checkpoint,
                mailbox: SessionMailbox::new(id),
            })
            .unwrap();
            let permissions = Arc::new(PermissionManager::new(
                Default::default(),
                PathBuf::from(CWD),
                ProjectConfig::for_project(PathBuf::from(CWD).as_path()),
                Arc::default(),
            ));
            permissions
                .bind_journal(
                    manager.journal(),
                    Some(manager.journal().incarnation(root.id())),
                )
                .unwrap();
            coordinator
                .bind_permissions(Arc::clone(&permissions))
                .unwrap();
            coordinator
                .bind_actor(
                    root.clone(),
                    Arc::new(|_| Box::pin(async { unreachable!() })),
                    Arc::new(|_| {}),
                )
                .unwrap();
            let result = futures_lite::future::or(
                coordinator.set_option(YOLO_OPTION_ID, ENABLED_VALUE),
                async {
                    smol::Timer::after(DRAIN_TIMEOUT).await;
                    panic!("prepared coordinator option checkpoint did not complete")
                },
            )
            .await;
            result.unwrap();
            assert_eq!(permissions.snapshot().yolo, Some(true));
            let journal = manager.journal();
            assert_eq!(
                AppSession::load(id, &dir).unwrap().state(),
                journal.snapshot().state
            );
            assert_eq!(journal.published_revision(), journal.snapshot().revision);
            coordinator.retire();
            manager.shutdown(DRAIN_TIMEOUT).await;
            writer.shutdown(DRAIN_TIMEOUT);
        });
    }

    #[test_case(false; "ordinary_save")]
    #[test_case(true; "ordinary_staged_save")]
    fn old_capture_cannot_overwrite_installed_prepared_checkpoint(staged: bool) {
        smol::block_on(async {
            let (_tmp, dir) = state_dir();
            let (writer, _wake_rx) = manual_writer();
            let mut session = AppSession::new(MODEL, CWD);
            session.push_message(Message::user(BASE_MSG.into()));
            session.save(&dir).unwrap();
            let id = session.id;
            writer.seed(Arc::new(session.clone()));
            let journal = Arc::new(SessionJournal::from_session(&session).unwrap());
            let agent = AgentId::generate();
            journal_commit(&journal, agent, false);
            journal
                .commit(JournalCommit {
                    histories: [(agent, session.history_snapshot())].into(),
                    managed_histories: [agent].into(),
                    ..Default::default()
                })
                .unwrap();
            writer.bind_journal(id, Arc::clone(&journal));
            let (lease, _) = guarded_writer(&writer, &dir, id);
            let ordinary = if staged {
                Some(
                    enqueue_staged_checkpoint(
                        &writer.pending,
                        &writer.wake,
                        StagedSessionCheckpoint {
                            session_id: id,
                            checkpoint: None,
                            journal: None,
                            selected_config: None,
                            history: None,
                            publication: CheckpointPublication::default(),
                            claim: publication_claim(),
                            guard: lease.publication_guard(),
                        },
                    )
                    .unwrap(),
                )
            } else {
                None
            };
            let waiter = if !staged {
                Some(writer.checkpoint(CheckpointRequest {
                    session_id: id,
                    version: CheckpointVersion {
                        epoch: 1,
                        revision: 1,
                    },
                    snapshot: Arc::new(session.clone()),
                }))
            } else {
                None
            };
            let token = journal
                .prepare(JournalCommit {
                    histories: [(
                        agent,
                        HistorySnapshot::new(vec![Message::user(REWOUND_MSG.into())]),
                    )]
                    .into(),
                    managed_histories: [agent].into(),
                    changes: vec![JournalChange {
                        agent: Some(agent),
                        generation: 1,
                        change: StateChange::Rules {
                            rules: Vec::new(),
                            yolo: Some(true),
                        },
                    }],
                    ..Default::default()
                })
                .await
                .unwrap();
            let expected = token.snapshot().clone();
            let (warn_tx, _warnings) = flume::unbounded();
            let mut disk = Writer {
                dir: dir.clone(),
                warn_tx,
                logs: HashMap::new(),
                failing: HashSet::new(),
            };
            disk.flush(&writer.pending);
            assert_eq!(
                message_texts(&AppSession::load(id, &dir).unwrap()),
                [BASE_MSG]
            );
            let mut published = session.clone();
            expected.apply_to(&mut published).unwrap();
            let directory = dir.ensure_subdir(SESSIONS_DIR).unwrap();
            assert!(
                SessionLog::stage_checkpoint(&directory, &published)
                    .unwrap()
                    .publish(&lease.publication_guard(), || true)
                    .unwrap()
            );
            token.commit().unwrap();
            if let Some(Entry::Save(save)) = lock(&writer.pending).entries.get_mut(&id) {
                save.retry_at = None;
            }
            disk.flush(&writer.pending);
            if let Some(ordinary) = ordinary {
                assert_eq!(
                    ordinary.recv_timeout(DRAIN_TIMEOUT).unwrap().unwrap_err(),
                    CHECKPOINT_REPLACED
                );
            }
            if let Some(waiter) = waiter {
                assert!(waiter.await.is_err());
            }
            let reopened = AppSession::load(id, &dir).unwrap();
            assert_eq!(reopened.state(), expected.state);
            assert_eq!(message_texts(&reopened), [REWOUND_MSG]);
        });
    }

    #[test]
    fn ordinary_write_holds_journal_publication_lane_until_disk_completion() {
        smol::block_on(async {
            let (_tmp, dir) = state_dir();
            let (writer, _wake_rx) = manual_writer();
            let session = Arc::new(AppSession::new(MODEL, CWD));
            let id = session.id;
            writer.seed(Arc::clone(&session));
            let journal = Arc::new(SessionJournal::from_session(&session).unwrap());
            writer.bind_journal(id, Arc::clone(&journal));
            writer.send(session);
            let (entered, entering) = flume::bounded(1);
            let (release, released) = flume::bounded(1);
            lock(&writer.pending).save_gate = Some((entered, released));
            let pending = Arc::clone(&writer.pending);
            let (warn_tx, _warnings) = flume::unbounded();
            let worker = std::thread::spawn(move || {
                Writer {
                    dir,
                    warn_tx,
                    logs: HashMap::new(),
                    failing: HashSet::new(),
                }
                .flush(&pending)
            });
            entering.recv_async().await.unwrap();
            let mut preparing = Box::pin(journal.prepare(JournalCommit {
                changes: vec![JournalChange {
                    agent: None,
                    generation: 0,
                    change: StateChange::Rules {
                        rules: Vec::new(),
                        yolo: Some(true),
                    },
                }],
                ..Default::default()
            }));
            assert!(
                futures_lite::future::poll_once(preparing.as_mut())
                    .await
                    .is_none()
            );
            release.send(()).unwrap();
            worker.join().unwrap();
            drop(preparing.await.unwrap());
        });
    }

    #[test]
    fn prepared_config_publishes_exact_journal_without_acknowledging_live_prefix() {
        smol::block_on(async {
            let (_tmp, dir) = state_dir();
            let (writer, _wake_rx) = manual_writer();
            let mut session = AppSession::new(MODEL, CWD);
            session.push_message(Message::user(BASE_MSG.into()));
            let id = session.id;
            writer.seed(Arc::new(session.clone()));
            let journal = Arc::new(SessionJournal::from_session(&session).unwrap());
            let agent = AgentId::generate();
            journal_commit(&journal, agent, false);
            journal
                .commit(JournalCommit {
                    histories: [(agent, session.history_snapshot())].into(),
                    managed_histories: [agent].into(),
                    ..Default::default()
                })
                .unwrap();
            writer.bind_journal(id, Arc::clone(&journal));
            let (_lease, checkpoint) = guarded_writer(&writer, &dir, id);
            let token = journal
                .prepare(JournalCommit {
                    changes: vec![JournalChange {
                        agent: Some(agent),
                        generation: 1,
                        change: StateChange::Rules {
                            rules: Vec::new(),
                            yolo: Some(true),
                        },
                    }],
                    ..Default::default()
                })
                .await
                .unwrap();
            let expected = token.snapshot().clone();
            let mut checkpoint_snapshot = config_checkpoint(1, 1);
            checkpoint_snapshot.journal = Some(expected.clone());
            let ack = publish_config(
                &checkpoint,
                CheckpointRequest {
                    session_id: id,
                    version: CheckpointVersion {
                        epoch: 1,
                        revision: 1,
                    },
                    snapshot: Arc::new(checkpoint_snapshot),
                },
            );
            let (warn_tx, _warnings) = flume::unbounded();
            let mut disk = Writer {
                dir: dir.clone(),
                warn_tx,
                logs: HashMap::new(),
                failing: HashSet::new(),
            };
            disk.flush(&writer.pending);
            ack.await.unwrap();
            assert_eq!(AppSession::load(id, &dir).unwrap().state(), expected.state);
            assert_eq!(
                lock(&writer.pending).acknowledged[&id].state(),
                expected.state
            );
            assert!(journal.snapshot().state.len() < expected.state.len());
            assert_eq!(journal.published_revision(), 0);
            token.commit().unwrap();
            assert_eq!(journal.published_revision(), expected.revision);
        });
    }

    #[test]
    fn prepared_child_journal_publishes_without_waiting_for_its_own_lane() {
        smol::block_on(async {
            let (_tmp, dir) = state_dir();
            let (writer, _wake_rx) = manual_writer();
            let mut session = AppSession::new(MODEL, CWD);
            session.push_message(Message::user(BASE_MSG.into()));
            session.set_subagent_messages(TOOL_ID.into(), vec![Message::user(BASE_MSG.into())]);
            session.set_subagents(vec![StoredSubagent {
                tool_use_id: TOOL_ID.into(),
                agent_id: None,
                ambiguous: false,
                name: TOOL_ID.into(),
                model: Some(MODEL.into()),
                thinking: None,
                fast: false,
            }]);
            let id = session.id;
            writer.seed(Arc::new(session.clone()));
            let journal = Arc::new(SessionJournal::from_session(&session).unwrap());
            let root = AgentId::generate();
            journal_commit(&journal, root, false);
            let child = AgentId::generate();
            let mut record = journal.agent(root).unwrap();
            record.id = child;
            record.parent = Some(root);
            record.depth = 1;
            record.transcript_key = TOOL_ID.into();
            journal
                .commit(JournalCommit {
                    agents: vec![record],
                    histories: [(child, session.subagent_history_snapshot(TOOL_ID).unwrap())]
                        .into(),
                    managed_histories: [child].into(),
                    ..Default::default()
                })
                .unwrap();
            writer.bind_journal(id, Arc::clone(&journal));
            let (lease, _) = guarded_writer(&writer, &dir, id);
            let history = HistorySnapshot::new(vec![Message::user(REWOUND_MSG.into())]);
            let token = journal
                .prepare(JournalCommit {
                    histories: [(child, history.clone())].into(),
                    managed_histories: [child].into(),
                    changes: vec![JournalChange {
                        agent: Some(child),
                        generation: 1,
                        change: StateChange::Rules {
                            rules: Vec::new(),
                            yolo: Some(true),
                        },
                    }],
                    ..Default::default()
                })
                .await
                .unwrap();
            let expected = token.snapshot().clone();
            let response = enqueue_staged_checkpoint(
                &writer.pending,
                &writer.wake,
                StagedSessionCheckpoint {
                    session_id: id,
                    checkpoint: None,
                    journal: Some(expected.clone()),
                    selected_config: Some((
                        HistoryDestination::Child(TOOL_ID.into()),
                        config_projection(NEXT_MODEL, CWD, 1, 1).config,
                    )),
                    history: Some((HistoryDestination::Child(TOOL_ID.into()), history)),
                    publication: CheckpointPublication::default(),
                    claim: publication_claim(),
                    guard: lease.publication_guard(),
                },
            )
            .unwrap();
            let (warn_tx, _warnings) = flume::unbounded();
            let mut disk = Writer {
                dir: dir.clone(),
                warn_tx,
                logs: HashMap::new(),
                failing: HashSet::new(),
            };
            disk.flush(&writer.pending);
            response.recv_timeout(DRAIN_TIMEOUT).unwrap().unwrap();
            let reopened = AppSession::load(id, &dir).unwrap();
            assert_eq!(reopened.state(), expected.state);
            assert_eq!(
                reopened.subagent_messages()[TOOL_ID][0].user_text(),
                Some(REWOUND_MSG)
            );
            assert_eq!(journal.published_revision(), 0);
            token.commit().unwrap();
            assert_eq!(journal.published_revision(), expected.revision);
        });
    }

    #[test_case(0; "valid_guard")]
    #[test_case(1; "lost_guard")]
    #[test_case(2; "replaced_incarnation")]
    #[test_case(3; "newer_snapshot")]
    fn recovery_publishes_full_candidate_before_returning_bundle(failure: u8) {
        use makima_storage::id::TurnId;
        use makima_storage::session_state::StoredTurnOrigin;

        let (_tmp, dir) = state_dir();
        let (writer, _wake_rx) = manual_writer();
        let mut session = AppSession::new(MODEL, CWD);
        let root = AgentId::generate();
        session.bind_root_id(root).unwrap();
        session.push_message(Message::user(BASE_MSG.into()));
        session.set_subagent_messages(TOOL_ID.into(), vec![Message::user(COORDINATOR_MSG.into())]);
        session.push_state(StoredStateRecord {
            seq: 2,
            agent: Some(root),
            generation: 0,
            change: StateChange::Admitted {
                turn: TurnId::generate(),
                origin: StoredTurnOrigin::User,
            },
        });
        session.save(&dir).unwrap();
        let id = session.id;
        writer.seed(Arc::new(session.clone()));
        let original_journal = Arc::new(SessionJournal::from_session(&session).unwrap());
        writer.bind_journal(id, Arc::clone(&original_journal));
        let lease = session_lock::claim(&dir.ensure_subdir(SESSIONS_DIR).unwrap(), &id)
            .unwrap()
            .unwrap();
        let guard = lease.publication_guard();
        let candidate = RecoveryCandidate::prepare(session.clone()).unwrap();
        let initial_save_fence = writer.session_save_fence(id);
        let expected_state = candidate.session().state().to_vec();
        let path = dir.path().join(SESSIONS_DIR).join(format!("{id}.jsonl"));
        let before = std::fs::read(&path).unwrap();
        let mut recovery = Box::pin(writer.recover_checkpoint(candidate, guard));
        assert!(smol::block_on(futures_lite::future::poll_once(recovery.as_mut())).is_none());
        assert_eq!(std::fs::read(&path).unwrap(), before);
        match failure {
            1 => lease.release().unwrap(),
            2 => cancel_publications(&mut lock(&writer.pending), id),
            3 => {
                let mut state = lock(&writer.pending);
                let generation = next_generation(&mut state);
                state.latest_generations.insert(id, generation);
            }
            _ => {}
        }
        let (warn_tx, _warnings) = flume::unbounded();
        let mut disk = Writer {
            dir: dir.clone(),
            warn_tx,
            logs: HashMap::new(),
            failing: HashSet::new(),
        };
        disk.flush(&writer.pending);
        let result = smol::block_on(recovery);
        assert!(save_fence_current(
            &lock(&writer.pending),
            id,
            Some(&initial_save_fence),
        ));
        if failure != 0 {
            assert!(result.is_err());
            assert_eq!(std::fs::read(&path).unwrap(), before);
            assert!(Arc::ptr_eq(
                &lock(&writer.pending).journals[&id].journal,
                &original_journal,
            ));
        } else {
            let recovered = result.unwrap();
            let reopened = AppSession::load(id, &dir).unwrap();
            assert_eq!(reopened.state(), expected_state);
            assert_eq!(recovered.session().state(), expected_state);
            assert_eq!(
                reopened.subagent_messages()[TOOL_ID][0].user_text(),
                Some(COORDINATOR_MSG)
            );
            let state = lock(&writer.pending);
            assert_eq!(state.acknowledged[&id].state(), expected_state);
            assert!(Arc::ptr_eq(
                &state.journals[&id].journal,
                &recovered.journal()
            ));
        }
    }

    #[test_case(false; "newer_revision")]
    #[test_case(true; "new_binding")]
    fn journal_rejects_superseded_captures(rebind: bool) {
        let (writer, _wake_rx) = manual_writer();
        let session = Arc::new(AppSession::new(MODEL, CWD));
        let id = session.id;
        let journal = Arc::new(SessionJournal::from_session(&session).unwrap());
        writer.bind_journal(id, Arc::clone(&journal));
        let stale = capture_journal(&writer.pending, id);
        if rebind {
            writer.bind_journal(
                id,
                Arc::new(SessionJournal::from_session(&session).unwrap()),
            );
        } else {
            journal_commit(&journal, AgentId::generate(), false);
            writer.send(Arc::clone(&session));
        }
        let mut candidate = Arc::clone(&session);
        assert_eq!(
            project_journal(&lock(&writer.pending), id, stale.as_ref(), &mut candidate),
            Err(CHECKPOINT_REPLACED.into())
        );
        assert!(candidate.agent_revisions().is_empty());
        assert_eq!(journal.published_revision(), 0);
    }

    #[test]
    fn journal_disk_write_does_not_block_ui_send() {
        let (_tmp, dir) = state_dir();
        let (writer, _wake_rx) = manual_writer();
        let session = Arc::new(AppSession::new(MODEL, CWD));
        let id = session.id;
        writer.seed(Arc::clone(&session));
        writer.bind_journal(
            id,
            Arc::new(SessionJournal::from_session(&session).unwrap()),
        );
        writer.send(Arc::clone(&session));
        let (entered, writing) = flume::bounded(1);
        let (release, released) = flume::bounded(1);
        lock(&writer.pending).save_gate = Some((entered, released));
        let pending = Arc::clone(&writer.pending);
        let (warn_tx, _warnings) = flume::unbounded();
        let worker = std::thread::spawn(move || {
            let mut disk = Writer {
                dir,
                warn_tx,
                logs: HashMap::new(),
                failing: HashSet::new(),
            };
            disk.flush(&pending);
        });
        writing.recv_timeout(DRAIN_TIMEOUT).unwrap();
        let (sent, received) = flume::bounded(1);
        let sender = std::thread::spawn(move || {
            writer.send(session);
            sent.send(()).unwrap();
        });
        let result = received.recv_timeout(DRAIN_TIMEOUT);
        release.send(()).unwrap();
        worker.join().unwrap();
        sender.join().unwrap();
        result.unwrap();
    }

    #[test_case(false, false; "clear_root_context")]
    #[test_case(true, false; "replace_root_context")]
    #[test_case(false, true; "clear_child_context")]
    #[test_case(true, true; "replace_child_context")]
    fn bound_journal_preserves_prepared_history_replacement(replace: bool, child: bool) {
        let (_tmp, dir) = state_dir();
        let (writer, _wake_rx) = manual_writer();
        let mut session = AppSession::new(MODEL, CWD);
        session.push_message(Message::user(BASE_MSG.into()));
        let id = session.id;
        let root = AgentId::generate();
        let journal = Arc::new(SessionJournal::from_session(&session).unwrap());
        journal_commit(&journal, root, false);
        let agent = if child {
            let agent = AgentId::generate();
            let mut record = journal.agent(root).unwrap();
            record.id = agent;
            record.parent = Some(root);
            record.depth = 1;
            record.transcript_key = TOOL_ID.into();
            journal
                .commit(JournalCommit {
                    agents: vec![record],
                    ..Default::default()
                })
                .unwrap();
            session.set_subagents(vec![StoredSubagent {
                tool_use_id: TOOL_ID.into(),
                agent_id: Some(agent.to_string()),
                ambiguous: false,
                name: TOOL_ID.into(),
                model: Some(MODEL.into()),
                thinking: None,
                fast: false,
            }]);
            session.set_subagent_messages(TOOL_ID.into(), vec![Message::user(BASE_MSG.into())]);
            agent
        } else {
            root
        };
        let snapshot = |session: &AppSession| {
            if child {
                session.subagent_history_snapshot(TOOL_ID).unwrap()
            } else {
                session.history_snapshot()
            }
        };
        let destination = if child {
            HistoryDestination::Child(TOOL_ID.into())
        } else {
            HistoryDestination::Main
        };
        let original = snapshot(&session);
        journal
            .commit(JournalCommit {
                histories: [(agent, original.clone())].into(),
                managed_histories: [agent].into(),
                ..Default::default()
            })
            .unwrap();
        writer.seed(Arc::new(session.clone()));
        writer.bind_journal(id, Arc::clone(&journal));
        let (lease, _checkpoint) = guarded_writer(&writer, &dir, id);
        let mut history = original.clone();
        history.epoch += 1;
        history.messages = Arc::new(if replace {
            vec![Message::user(REWOUND_MSG.into())]
        } else {
            Vec::new()
        });
        let response = enqueue_staged_checkpoint(
            &writer.pending,
            &writer.wake,
            StagedSessionCheckpoint {
                session_id: id,
                checkpoint: None,
                journal: None,
                selected_config: None,
                history: Some((destination, history.clone())),
                publication: CheckpointPublication::default(),
                claim: publication_claim(),
                guard: lease.publication_guard(),
            },
        )
        .unwrap();
        let (warn_tx, _warnings) = flume::unbounded();
        let mut disk = Writer {
            dir: dir.clone(),
            warn_tx,
            logs: HashMap::new(),
            failing: HashSet::new(),
        };
        disk.write(&session).unwrap();
        disk.flush(&writer.pending);
        response.recv().unwrap().unwrap();
        let assert_history = |session: &AppSession| {
            let messages = if child {
                session
                    .subagent_messages()
                    .get(TOOL_ID)
                    .map(|messages| messages.as_slice())
            } else {
                Some(session.messages())
            };
            assert_eq!(messages.map_or(0, <[Message]>::len), usize::from(replace));
            if replace {
                assert_eq!(messages.unwrap()[0].user_text(), Some(REWOUND_MSG));
            }
            if child {
                assert_eq!(message_texts(session), [BASE_MSG]);
            }
            assert_eq!(
                session.agent_revisions(),
                journal.snapshot().agent_revisions
            );
            assert_eq!(session.state(), journal.snapshot().state);
        };
        assert_eq!(journal.snapshot().histories[&agent].epoch, original.epoch);
        assert_eq!(journal.published_revision(), journal.snapshot().revision);
        assert_history(&AppSession::load(id, &dir).unwrap());
        assert_history(&writer.latest_snapshot(id).unwrap());
        assert_eq!(
            snapshot(&writer.latest_snapshot(id).unwrap()).epoch,
            history.epoch
        );
        assert_history(&lock(&writer.pending).acknowledged[&id]);
        journal_commit(&journal, root, true);
        disk.flush(&writer.pending);
        assert_eq!(journal.snapshot().histories[&agent].epoch, original.epoch);
        assert_history(&AppSession::load(id, &dir).unwrap());
        assert_history(&lock(&writer.pending).acknowledged[&id]);
        assert_eq!(lock(&writer.pending).prepared_histories[&id].len(), 1);
        writer.send(Arc::new(session.clone()));
        disk.flush(&writer.pending);
        assert_history(&AppSession::load(id, &dir).unwrap());
        assert_history(&writer.latest_snapshot(id).unwrap());
        journal
            .commit(JournalCommit {
                histories: [(agent, history.clone())].into(),
                ..Default::default()
            })
            .unwrap();
        let checkpoint = writer.checkpoint(CheckpointRequest {
            session_id: id,
            version: CheckpointVersion {
                epoch: session.history_snapshot().epoch,
                revision: session.revision(),
            },
            snapshot: Arc::new(session.clone()),
        });
        disk.flush(&writer.pending);
        smol::block_on(checkpoint).unwrap();
        assert!(lock(&writer.pending).prepared_histories[&id].is_empty());
        writer.send(Arc::new(session));
        disk.flush(&writer.pending);
        assert_history(&AppSession::load(id, &dir).unwrap());
        assert_history(&writer.latest_snapshot(id).unwrap());
        assert_eq!(journal.published_revision(), journal.snapshot().revision);
    }

    #[test]
    fn journal_staged_publication_preserves_graph_and_acknowledges_after_claim() {
        let (_tmp, dir) = state_dir();
        let (writer, _wake_rx) = manual_writer();
        let session = Arc::new(AppSession::new(MODEL, CWD));
        let id = session.id;
        writer.seed(Arc::clone(&session));
        let journal = Arc::new(SessionJournal::from_session(&session).unwrap());
        writer.bind_journal(id, Arc::clone(&journal));
        journal_commit(&journal, AgentId::generate(), false);
        let (_lease, checkpoint) = guarded_writer(&writer, &dir, id);
        let (warn_tx, _warnings) = flume::unbounded();
        let mut disk = Writer {
            dir: dir.clone(),
            warn_tx,
            logs: HashMap::new(),
            failing: HashSet::new(),
        };
        disk.write(&session).unwrap();
        let pending = Arc::clone(&writer.pending);
        let ack = checkpoint.checkpoint_guarded(
            CheckpointRequest {
                session_id: id,
                version: CheckpointVersion {
                    epoch: 1,
                    revision: 1,
                },
                snapshot: Arc::new(config_checkpoint(1, 1)),
            },
            CheckpointPublication::default(),
            Arc::new(move |publication| {
                assert!(pending.try_lock().is_err());
                publication_claim()(publication)
            }),
        );
        disk.flush(&writer.pending);
        smol::block_on(ack).unwrap();
        assert_eq!(journal.published_revision(), 1);
        let reopened = AppSession::load(id, &dir).unwrap();
        assert_eq!(
            reopened.agent_revisions(),
            journal.snapshot().agent_revisions
        );
        writer.send(session);
        disk.flush(&writer.pending);
        assert_eq!(
            AppSession::load(id, &dir).unwrap().state(),
            journal.snapshot().state
        );
    }

    fn state_dir() -> (TempDir, StateDir) {
        let tmp = TempDir::new().unwrap();
        let dir = StateDir::from_path(tmp.path().to_path_buf());
        (tmp, dir)
    }

    fn writer(dir: &StateDir) -> (StorageWriter, flume::Receiver<String>) {
        let (warn_tx, warn_rx) = flume::unbounded();
        (StorageWriter::new(dir.clone(), warn_tx), warn_rx)
    }

    #[test_case(false; "failed_log_delete_preserves_artifacts")]
    #[test_case(true; "cleanup_failure_does_not_fail_log_delete")]
    fn guarded_deletion_preserves_artifacts_until_log_removal(cleanup_failure: bool) {
        use makima_agent::tools::offload::OffloadBackend;
        use makima_storage::session_lock;
        use std::path::PathBuf;

        struct FailingCleanup {
            path: PathBuf,
            attempted: Arc<AtomicBool>,
        }

        impl OffloadBackend for FailingCleanup {
            fn create_new(&self, _: &str, _: &[u8]) -> io::Result<bool> {
                unreachable!()
            }

            fn total_bytes(&self) -> io::Result<u64> {
                Ok(0)
            }

            fn remove_all(&self) -> io::Result<()> {
                self.attempted.store(true, Ordering::Release);
                Err(io::Error::other("gated cleanup failure"))
            }

            fn path(&self, _: &str) -> PathBuf {
                self.path.clone()
            }
        }

        const BODY: &str = "referenced output";
        let (_temp, dir) = state_dir();
        let mut session = AppSession::new(MODEL, CWD);
        session.push_message(Message::observation(BODY.into()));
        session.save(&dir).unwrap();
        let id = session.id;
        let sessions_dir = dir.path().join(SESSIONS_DIR);
        let lease = session_lock::claim(&sessions_dir, &id).unwrap().unwrap();
        let guard = lease.publication_guard();
        let artifact = dir.path().join("retained-artifact.txt");
        std::fs::write(&artifact, BODY).unwrap();
        let attempted = Arc::new(AtomicBool::new(false));
        let store = Arc::new(OffloadStore::new(Box::new(FailingCleanup {
            path: artifact.clone(),
            attempted: Arc::clone(&attempted),
        })));
        store.close_and_drain();
        let log = sessions_dir.join(format!("{id}.jsonl"));
        let backup = sessions_dir.join("retained-log.jsonl");
        if !cleanup_failure {
            std::fs::rename(&log, &backup).unwrap();
            std::fs::create_dir(&log).unwrap();
        }
        let (writer, _) = writer(&dir);
        let (done_tx, done_rx) = flume::bounded(1);
        writer.delete_guarded(id, guard, Some(store), move |result| {
            done_tx.send(result).unwrap();
        });
        let result = done_rx.recv_timeout(DRAIN_TIMEOUT).unwrap();
        assert_eq!(result.is_ok(), cleanup_failure);
        assert_eq!(attempted.load(Ordering::Acquire), cleanup_failure);
        assert_eq!(std::fs::read_to_string(&artifact).unwrap(), BODY);
        if cleanup_failure {
            assert!(AppSession::load(id, &dir).is_err());
        } else {
            std::fs::remove_dir(&log).unwrap();
            std::fs::rename(&backup, &log).unwrap();
            assert_eq!(message_texts(&AppSession::load(id, &dir).unwrap()), [BODY]);
        }
        writer.shutdown(DRAIN_TIMEOUT);
        drop(lease);
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

    fn user_message(n: usize) -> Message {
        Message::user(msg_text(n))
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
    fn acknowledged_in_flight_history_survives_newer_metadata_failure() {
        let (_tmp, dir) = state_dir();
        let (writer, _wake_rx) = manual_writer();
        let mut session = AppSession::new(MODEL, CWD);
        session.push_message(Message::user(BASE_MSG.into()));
        session.set_subagent_messages(TOOL_ID.into(), vec![Message::user(BASE_MSG.into())]);
        let id = session.id;
        writer.seed(Arc::new(session.clone()));
        let ack = {
            let mut history = config_checkpoint(1, 1);
            history.config = None;
            history.kind = SessionCheckpointKind::History;
            history.history_revision = Some(1);
            history.history = Some(HistorySnapshot::new(vec![Message::user(
                COORDINATOR_MSG.into(),
            )]));
            writer
                .coordinator_checkpoint()
                .checkpoint(CheckpointRequest {
                    session_id: id,
                    version: CheckpointVersion {
                        revision: 1,
                        epoch: 1,
                    },
                    snapshot: Arc::new(history),
                })
        };
        let Entry::Save(in_flight) = lock(&writer.pending).entries.remove(&id).unwrap() else {
            unreachable!()
        };
        session.set_title(TITLE.into());
        writer.send(Arc::new(session.clone()));
        let (warn_tx, _warn_rx) = flume::unbounded();
        let mut disk = Writer {
            dir: dir.clone(),
            warn_tx,
            logs: HashMap::new(),
            failing: HashSet::new(),
        };
        disk.write(&in_flight.session).unwrap();
        commit_save(&writer.pending, id, &in_flight);
        acknowledge_waiters(&writer.pending, &in_flight.session, in_flight.waiters);
        assert!(smol::block_on(ack).is_ok());
        let sessions_path = dir.path().join(SESSIONS_DIR);
        let saved_path = dir.path().join(TITLE);
        std::fs::rename(&sessions_path, &saved_path).unwrap();
        block_sessions_dir(&dir);
        disk.logs.clear();
        disk.flush(&writer.pending);
        assert!(disk.failing.contains(&id));
        assert!(lock(&writer.pending).entries.contains_key(&id));
        std::fs::remove_file(&sessions_path).unwrap();
        std::fs::rename(&saved_path, &sessions_path).unwrap();
        let latest = writer.latest_snapshot(id).unwrap();
        let messages = latest.messages();
        assert_eq!(messages[0].first_text_content(), Some(COORDINATOR_MSG));
        assert_eq!(latest.title, TITLE);
        writer.send(Arc::new(session));
        if let Some(Entry::Save(save)) = lock(&writer.pending).entries.get_mut(&id) {
            save.retry_at = None;
        }
        disk.flush(&writer.pending);
        let recovered = AppSession::load(id, &dir).unwrap();
        let messages = recovered.messages();
        assert_eq!(messages[0].first_text_content(), Some(COORDINATOR_MSG));
        assert_eq!(recovered.title, TITLE);
    }

    #[test_case(false; "unversioned_metadata")]
    #[test_case(true; "versioned_config")]
    fn in_flight_history_keeps_newer_metadata_until_promoted(versioned: bool) {
        let (_tmp, dir) = state_dir();
        let (writer, _wake_rx) = manual_writer();
        let mut session = AppSession::new(MODEL, CWD);
        session.push_message(Message::user(BASE_MSG.into()));
        let id = session.id;
        writer.seed(Arc::new(session.clone()));
        let (_lease, checkpoint) = guarded_writer(&writer, &dir, id);
        let mut history = config_checkpoint(1, 1);
        history.config = None;
        history.kind = SessionCheckpointKind::History;
        history.history_revision = Some(1);
        history.history = Some(HistorySnapshot::new(vec![Message::user(
            COORDINATOR_MSG.into(),
        )]));
        let history_ack = checkpoint.checkpoint(CheckpointRequest {
            session_id: id,
            version: CheckpointVersion {
                revision: 1,
                epoch: 1,
            },
            snapshot: Arc::new(history),
        });
        let Entry::Save(in_flight) = lock(&writer.pending).entries.remove(&id).unwrap() else {
            unreachable!()
        };
        let mut metadata = config_checkpoint(1, 1);
        if !versioned {
            metadata.kind = SessionCheckpointKind::Options;
        }
        Arc::make_mut(&mut metadata.options.options)
            .iter_mut()
            .find(|state| state.definition.id.as_ref() == YOLO_OPTION_ID)
            .unwrap()
            .current_value = Arc::from(ENABLED_VALUE);
        let expected_model = MODEL;
        let metadata = CheckpointRequest {
            session_id: id,
            version: CheckpointVersion {
                revision: 2,
                epoch: 1,
            },
            snapshot: Arc::new(metadata),
        };
        let metadata_ack = if versioned {
            publish_config(&checkpoint, metadata)
        } else {
            checkpoint.checkpoint(metadata)
        };
        let (warn_tx, _warn_rx) = flume::unbounded();
        let mut disk = Writer {
            dir: dir.clone(),
            warn_tx,
            logs: HashMap::new(),
            failing: HashSet::new(),
        };
        disk.write(&in_flight.session).unwrap();
        commit_save(&writer.pending, id, &in_flight);
        acknowledge_waiters(&writer.pending, &in_flight.session, in_flight.waiters);
        assert!(smol::block_on(history_ack).is_ok());
        assert_eq!(writer.latest_snapshot(id).unwrap().model, MODEL);
        disk.flush(&writer.pending);
        assert!(smol::block_on(metadata_ack).is_ok());
        assert_eq!(AppSession::load(id, &dir).unwrap().model, expected_model);
        assert_eq!(writer.latest_snapshot(id).unwrap().model, expected_model);
        session.set_title(TITLE.into());
        writer.send(Arc::new(session));
        disk.flush(&writer.pending);
        let reopened = AppSession::load(id, &dir).unwrap();
        assert_eq!(reopened.model, expected_model);
        assert_eq!(reopened.meta.yolo, None);
        assert_eq!(reopened.title, TITLE);
        assert_eq!(message_texts(&reopened), [COORDINATOR_MSG]);
        assert_eq!(writer.latest_snapshot(id).unwrap().model, expected_model);
        assert!(lock(&writer.pending).coordinator_pending.is_empty());
    }

    #[test_case(false; "rollback")]
    #[test_case(true; "metadata_before_success")]
    fn in_flight_success_rebases_newer_history_rollback(succeed: bool) {
        let (_tmp, dir) = state_dir();
        let (writer, _wake_rx) = manual_writer();
        let mut session = AppSession::new(MODEL, CWD);
        session.push_message(Message::user(BASE_MSG.into()));
        session.set_subagent_messages(TOOL_ID.into(), vec![Message::user(BASE_MSG.into())]);
        let id = session.id;
        writer.seed(Arc::new(session.clone()));
        let checkpoint = |text: &str| {
            let mut history = config_checkpoint(1, 1);
            history.config = None;
            history.kind = SessionCheckpointKind::History;
            history.history_revision = Some(1);
            history.history = Some(HistorySnapshot::new(vec![Message::user(text.into())]));
            writer
                .coordinator_checkpoint()
                .checkpoint(CheckpointRequest {
                    session_id: id,
                    version: CheckpointVersion {
                        revision: 1,
                        epoch: 1,
                    },
                    snapshot: Arc::new(history),
                })
        };
        let first_ack = checkpoint(COORDINATOR_MSG);
        let Entry::Save(first) = lock(&writer.pending).entries.remove(&id).unwrap() else {
            unreachable!()
        };
        let newer_ack = checkpoint(REWOUND_MSG);
        let (warn_tx, _warn_rx) = flume::unbounded();
        let mut disk = Writer {
            dir: dir.clone(),
            warn_tx,
            logs: HashMap::new(),
            failing: HashSet::new(),
        };
        disk.write(&first.session).unwrap();
        commit_save(&writer.pending, id, &first);
        acknowledge_waiters(&writer.pending, &first.session, first.waiters);
        let first_result = smol::block_on(first_ack);
        assert!(
            matches!(first_result, Err(CheckpointError::Save { session_id, .. }) if session_id == id)
        );

        {
            let state = lock(&writer.pending);
            let candidate = &state.coordinator_pending[&id].session;
            let messages = candidate.messages();
            assert_eq!(messages[0].first_text_content(), Some(REWOUND_MSG));
        }
        if succeed {
            session.set_title(TITLE.into());
            writer.send(Arc::new(session));
            disk.flush(&writer.pending);
            assert!(smol::block_on(newer_ack).is_ok());
            let latest = writer.latest_snapshot(id).unwrap();
            let reopened = AppSession::load(id, &dir).unwrap();
            for saved in [&*latest, &reopened] {
                let messages = saved.messages();
                assert_eq!(messages[0].first_text_content(), Some(REWOUND_MSG));
                assert_eq!(saved.title, TITLE);
            }
            return;
        }
        let sessions_path = dir.path().join(SESSIONS_DIR);
        let saved_path = dir.path().join(TITLE);
        std::fs::rename(&sessions_path, &saved_path).unwrap();
        block_sessions_dir(&dir);
        disk.logs.clear();
        if let Some(Entry::Save(save)) = lock(&writer.pending).entries.get_mut(&id) {
            save.retry_attempt = CHECKPOINT_RETRY_BACKOFFS.len();
        }
        disk.flush(&writer.pending);
        assert!(smol::block_on(newer_ack).is_err());
        std::fs::remove_file(&sessions_path).unwrap();
        std::fs::rename(&saved_path, &sessions_path).unwrap();
        let latest = writer.latest_snapshot(id).unwrap();
        let messages = latest.messages();
        assert_eq!(messages[0].first_text_content(), Some(COORDINATOR_MSG));
        let recovered = AppSession::load(id, &dir).unwrap();
        let messages = recovered.messages();
        assert_eq!(messages[0].first_text_content(), Some(COORDINATOR_MSG));
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
            journal: None,
            session: Arc::new(old),
            waiters: vec![CheckpointWaiter {
                version: old_version,
                histories: required_histories(&newer),
                reply: old_reply,
            }],
            generation: OLD_GENERATION,
            coordinator_generation: Some(OLD_GENERATION),
            config_version: None,
            coordinator_history_base: None,
            retry_attempt: RETRY_ATTEMPT,
            retry_at: Some(retry_at),
        };
        let newer_histories = required_histories(&newer);
        lock(&pending).entries.insert(
            id,
            Entry::Save(PendingSave {
                journal: None,
                session: Arc::new(newer),
                waiters: vec![CheckpointWaiter {
                    version: newer_version,
                    histories: newer_histories,
                    reply: newer_reply,
                }],
                generation: NEW_GENERATION,
                coordinator_generation: Some(NEW_GENERATION),
                config_version: None,
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
        acknowledge_waiters(&pending, &merged.session, merged.waiters);
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

            let options = SessionOptions::new(
                builtin_option_definitions(
                    "next/model",
                    [Arc::from("next/model")],
                    false,
                    false,
                    false,
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
                        kind: SessionCheckpointKind::History,
                        history_revision: Some(1),
                        history: Some(HistorySnapshot::new(Vec::new())),
                        options,
                        config: None,
                        journal: None,
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

            let options = SessionOptions::new(
                builtin_option_definitions(
                    "next/model",
                    [Arc::from("next/model")],
                    false,
                    false,
                    false,
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
                        kind: SessionCheckpointKind::History,
                        history_revision: Some(1),
                        history: Some(HistorySnapshot::new(Vec::new())),
                        options,
                        config: None,
                        journal: None,
                    }),
                })
                .await;
            assert!(ack.is_err(), "a deleted session must not be resurrected");
        });
    }

    fn stored_post(chat: Option<&str>, after_message: usize) -> StoredPost {
        StoredPost {
            chat: chat.map(str::to_owned),
            after_message,
            post: Post {
                text: POST_TEXT.into(),
                level: PostLevel::Info,
                agent: None,
            },
        }
    }

    #[test]
    fn send_reuses_unchanged_snapshot_and_ignores_closed_writer() {
        let (writer, _wake_rx) = manual_writer();
        let session = Arc::new(AppSession::new(MODEL, CWD));
        writer.seed(Arc::clone(&session));
        writer.send(Arc::clone(&session));
        assert!(Arc::ptr_eq(
            &writer.latest_snapshot(session.id).unwrap(),
            &session
        ));
        let mut state = lock(&writer.pending);
        state.closed = true;
        state.entries.clear();
        drop(state);
        writer.send(Arc::clone(&session));
        assert!(lock(&writer.pending).entries.is_empty());
        assert!(Arc::ptr_eq(
            &writer.latest_snapshot(session.id).unwrap(),
            &session
        ));
    }

    #[test]
    fn sending_compacted_history_keeps_child_state() {
        let (writer, _wake_rx) = manual_writer();
        let mut session = AppSession::new(MODEL, CWD);
        session.set_subagent_messages(ORPHAN_CHILD.into(), vec![Message::user(BASE_MSG.into())]);
        session.push_post(stored_post(None, 2));
        session.push_post(stored_post(Some(ORPHAN_CHILD), 2));
        writer.seed(Arc::new(session.clone()));
        let mut history = HistorySnapshot::new(vec![Message::user(COORDINATOR_MSG.into())]);
        history.compacted = true;
        session.adopt_history(&history);
        let session = Arc::new(session);
        writer.send(Arc::clone(&session));
        let latest = writer.latest_snapshot(session.id).unwrap();
        assert_eq!(latest.posts(), session.posts());
        assert!(latest.subagent_messages().contains_key(ORPHAN_CHILD));
    }

    #[test]
    fn main_checkpoint_prunes_posts_and_orphans_before_acknowledgement() {
        smol::block_on(async {
            let (_tmp, dir) = state_dir();
            let (writer, _warn_rx) = writer(&dir);
            let mut session = AppSession::new(MODEL, CWD);
            session.push_message(Message::user(BASE_MSG.into()));
            for child in [LIVE_CHILD, ORPHAN_CHILD] {
                session.set_subagent_messages(child.into(), vec![Message::user(BASE_MSG.into())]);
            }
            session.push_post(stored_post(None, 1));
            session.push_post(stored_post(None, 2));
            let id = session.id;
            writer.send(Arc::new(session));
            writer.set_live_children(id, vec![LIVE_CHILD.into()]);
            let lease = session_lock::claim(&dir.ensure_subdir(SESSIONS_DIR).unwrap(), &id)
                .unwrap()
                .unwrap();
            writer
                .checkpoint_staged(StagedSessionCheckpoint {
                    session_id: id,
                    checkpoint: None,
                    journal: None,
                    selected_config: None,
                    history: Some((
                        HistoryDestination::Main,
                        HistorySnapshot::new(vec![Message::user(REWOUND_MSG.into())]),
                    )),
                    publication: CheckpointPublication::default(),
                    claim: Arc::new(|publication| {
                        publication
                            .try_publish()
                            .then_some(())
                            .ok_or_else(|| Arc::from(CHECKPOINT_CANCELLED))
                    }),
                    guard: lease.publication_guard(),
                })
                .await
                .unwrap();
            let loaded = AppSession::load(id, &dir).unwrap();
            assert_eq!(loaded.posts(), [stored_post(None, 1)]);
            assert!(loaded.subagent_messages().contains_key(LIVE_CHILD));
            assert!(!loaded.subagent_messages().contains_key(ORPHAN_CHILD));
            let latest = writer.latest_snapshot(id).unwrap();
            assert_eq!(latest.posts(), loaded.posts());
            assert!(!latest.subagent_messages().contains_key(ORPHAN_CHILD));
            writer.shutdown(DRAIN_TIMEOUT);
        });
    }

    #[test]
    fn child_history_checkpoint_preserves_root_sibling_and_rejects_stale_save() {
        smol::block_on(async {
            let (_tmp, dir) = state_dir();
            let (writer, _warn_rx) = writer(&dir);
            let mut session = AppSession::new(MODEL, CWD);
            session.push_message(Message::user(BASE_MSG.into()));
            session.set_subagent_messages(TOOL_ID.into(), vec![Message::user(BASE_MSG.into())]);
            session.set_subagent_messages(
                SIBLING_AGENT_ID.into(),
                vec![Message::user(BASE_MSG.into())],
            );
            session.push_post(stored_post(None, 2));
            session.push_post(stored_post(Some(TOOL_ID), 1));
            session.push_post(stored_post(Some(TOOL_ID), 2));
            session.push_post(stored_post(Some(SIBLING_AGENT_ID), 2));
            let id = session.id;
            writer.send(Arc::new(session.clone()));
            let lease = session_lock::claim(&dir.ensure_subdir(SESSIONS_DIR).unwrap(), &id)
                .unwrap()
                .unwrap();
            writer
                .checkpoint_staged(StagedSessionCheckpoint {
                    session_id: id,
                    checkpoint: None,
                    journal: None,
                    selected_config: None,
                    history: Some((
                        HistoryDestination::Child(TOOL_ID.into()),
                        HistorySnapshot::new(vec![Message::user(REWOUND_MSG.into())]),
                    )),
                    publication: CheckpointPublication::default(),
                    claim: Arc::new(|publication| {
                        publication
                            .try_publish()
                            .then_some(())
                            .ok_or_else(|| Arc::from(CHECKPOINT_CANCELLED))
                    }),
                    guard: lease.publication_guard(),
                })
                .await
                .unwrap();
            let expected_posts = [
                stored_post(None, 2),
                stored_post(Some(TOOL_ID), 1),
                stored_post(Some(SIBLING_AGENT_ID), 2),
            ];
            assert_eq!(AppSession::load(id, &dir).unwrap().posts(), expected_posts);
            assert_eq!(writer.latest_snapshot(id).unwrap().posts(), expected_posts);
            session.set_title(TITLE.into());
            session.set_subagent_messages(
                SIBLING_AGENT_ID.into(),
                vec![Message::user(RESUMED_MSG.into())],
            );
            writer.send(Arc::new(session));
            writer.shutdown(DRAIN_TIMEOUT);
            let loaded = AppSession::load(id, &dir).unwrap();
            assert_eq!(loaded.title, TITLE);
            assert_eq!(loaded.messages().len(), 1);
            assert_eq!(loaded.posts(), expected_posts);
            let child = serde_json::to_value(&loaded.subagent_messages()[TOOL_ID]).unwrap();
            let expected = serde_json::to_value(vec![Message::user(REWOUND_MSG.into())]).unwrap();
            assert_eq!(child, expected);
            let sibling =
                serde_json::to_value(&loaded.subagent_messages()[SIBLING_AGENT_ID]).unwrap();
            assert_eq!(
                sibling,
                serde_json::to_value(vec![Message::user(RESUMED_MSG.into())]).unwrap()
            );
        });
    }

    #[test_case(false; "retry_after_child_publish")]
    #[test_case(true; "cancel_before_root_retry")]
    fn child_approval_does_not_revert_acknowledged_root_history(cancel: bool) {
        let (_tmp, dir) = state_dir();
        let (writer, _wake) = manual_writer();
        let mut session = AppSession::new(MODEL, CWD);
        session.set_subagent_messages(TOOL_ID.into(), vec![Message::user(BASE_MSG.into())]);
        let id = session.id;
        writer.seed(Arc::new(session));
        let (warn_tx, _warnings) = flume::unbounded();
        let mut disk = Writer {
            dir: dir.clone(),
            warn_tx,
            logs: HashMap::new(),
            failing: HashSet::new(),
        };
        let checkpoint = |message: &str, revision| {
            let mut snapshot = config_checkpoint(1, revision);
            snapshot.config = None;
            snapshot.kind = SessionCheckpointKind::History;
            snapshot.history_revision = Some(revision);
            snapshot.history = Some(HistorySnapshot::new(vec![Message::user(message.into())]));
            writer
                .coordinator_checkpoint()
                .checkpoint(CheckpointRequest {
                    session_id: id,
                    version: CheckpointVersion {
                        revision,
                        epoch: revision,
                    },
                    snapshot: Arc::new(snapshot),
                })
        };
        let accepted = checkpoint(REWOUND_MSG, 1);
        disk.flush(&writer.pending);
        smol::block_on(accepted).unwrap();
        let newer = checkpoint(RESUMED_MSG, 2);
        let sessions_path = dir.path().join(SESSIONS_DIR);
        let saved_path = dir.path().join(TITLE);
        std::fs::rename(&sessions_path, &saved_path).unwrap();
        block_sessions_dir(&dir);
        disk.logs.clear();
        disk.flush(&writer.pending);
        assert!(disk.failing.contains(&id));
        std::fs::remove_file(&sessions_path).unwrap();
        std::fs::rename(&saved_path, &sessions_path).unwrap();
        if let Some(Entry::Save(save)) = lock(&writer.pending).entries.get_mut(&id) {
            save.retry_at = Some(Instant::now() + DRAIN_TIMEOUT);
        }
        let publication = CheckpointPublication::default();
        let lease = session_lock::claim(&dir.ensure_subdir(SESSIONS_DIR).unwrap(), &id)
            .unwrap()
            .unwrap();
        let mut child = Box::pin(writer.checkpoint_staged(StagedSessionCheckpoint {
            session_id: id,
            checkpoint: None,
            journal: None,
            selected_config: None,
            history: Some((
                HistoryDestination::Child(TOOL_ID.into()),
                HistorySnapshot::new(Vec::new()),
            )),
            publication: publication.clone(),
            claim: Arc::new(|publication| {
                publication
                    .try_publish()
                    .then_some(())
                    .ok_or_else(|| Arc::from(CHECKPOINT_CANCELLED))
            }),
            guard: lease.publication_guard(),
        }));
        assert!(smol::block_on(futures_lite::future::poll_once(child.as_mut())).is_none());
        if cancel {
            assert!(publication.cancel());
        }
        disk.flush(&writer.pending);
        assert_eq!(smol::block_on(child).is_err(), cancel);
        let loaded = AppSession::load(id, &dir).unwrap();
        assert_eq!(loaded.messages()[0].user_text(), Some(REWOUND_MSG));
        if let Some(Entry::Save(save)) = lock(&writer.pending).entries.get_mut(&id) {
            save.retry_at = None;
        }
        disk.flush(&writer.pending);
        smol::block_on(newer).unwrap();
        let loaded = AppSession::load(id, &dir).unwrap();
        assert_eq!(loaded.messages()[0].user_text(), Some(RESUMED_MSG));
        assert_eq!(
            loaded
                .subagent_messages()
                .get(TOOL_ID)
                .map_or(0, |messages| messages.len()),
            usize::from(cancel)
        );
    }

    #[test_case(false, false; "main_without_later_save")]
    #[test_case(true, false; "child_without_later_save")]
    #[test_case(false, true; "main_with_later_save")]
    #[test_case(true, true; "child_with_later_save")]
    fn staged_checkpoint_cancelled_before_dequeue(child: bool, interleave: bool) {
        let (_tmp, dir) = state_dir();
        let (writer, _wake) = manual_writer();
        let mut session = AppSession::new(MODEL, CWD);
        session.push_message(Message::user(BASE_MSG.into()));
        session.set_subagent_messages(TOOL_ID.into(), vec![Message::user(BASE_MSG.into())]);
        let id = session.id;
        writer.seed(Arc::new(session.clone()));
        let publication = CheckpointPublication::default();
        let lease = session_lock::claim(&dir.ensure_subdir(SESSIONS_DIR).unwrap(), &id)
            .unwrap()
            .unwrap();
        let mut checkpoint = Box::pin(writer.checkpoint_staged(StagedSessionCheckpoint {
            session_id: id,
            checkpoint: None,
            journal: None,
            selected_config: None,
            history: Some((
                if child {
                    HistoryDestination::Child(TOOL_ID.into())
                } else {
                    HistoryDestination::Main
                },
                HistorySnapshot::new(Vec::new()),
            )),
            publication: publication.clone(),
            claim: Arc::new(|publication| {
                publication
                    .try_publish()
                    .then_some(())
                    .ok_or_else(|| Arc::from(CHECKPOINT_CANCELLED))
            }),
            guard: lease.publication_guard(),
        }));
        assert!(smol::block_on(futures_lite::future::poll_once(checkpoint.as_mut())).is_none());
        assert!(publication.cancel());
        if interleave {
            session.set_title(TITLE.into());
        }
        writer.send(Arc::new(session));
        let (warn_tx, _warn_rx) = flume::unbounded();
        let mut disk = Writer {
            dir: dir.clone(),
            warn_tx,
            logs: HashMap::new(),
            failing: HashSet::new(),
        };
        disk.flush(&writer.pending);
        assert_eq!(
            smol::block_on(checkpoint).unwrap_err(),
            CHECKPOINT_CANCELLED
        );
        let loaded = AppSession::load(id, &dir).unwrap();
        let latest = writer.latest_snapshot(id).unwrap();
        for saved in [&loaded, &*latest] {
            assert_eq!(saved.messages()[0].user_text(), Some(BASE_MSG));
            assert_eq!(
                saved.subagent_messages()[TOOL_ID][0].user_text(),
                Some(BASE_MSG)
            );
        }
        assert!(lock(&writer.pending).coordinator_pending.is_empty());
        if interleave {
            assert_eq!(loaded.title, TITLE);
        }
    }

    #[test_case(false, false, false, false, false; "main_publish_rebases_later_save")]
    #[test_case(true, false, false, false, false; "child_publish_rebases_later_save")]
    #[test_case(false, true, false, false, false; "main_cancel_preserves_later_save")]
    #[test_case(true, true, false, false, false; "child_cancel_preserves_later_save")]
    #[test_case(false, false, true, false, false; "main_replacement_fences_staged_publication")]
    #[test_case(true, false, true, false, false; "child_replacement_fences_staged_publication")]
    #[test_case(false, false, false, true, false; "main_runtime_fences_staged_publication")]
    #[test_case(true, false, false, true, false; "child_runtime_fences_staged_publication")]
    #[test_case(false, false, false, false, true; "main_lock_loss_fences_staged_publication")]
    #[test_case(true, false, false, false, true; "child_lock_loss_fences_staged_publication")]
    fn staged_checkpoint_is_isolated(
        child: bool,
        cancel: bool,
        replace: bool,
        rebind: bool,
        lose_lock: bool,
    ) {
        smol::block_on(async {
            let (_tmp, dir) = state_dir();
            let (writer, _warnings) = writer(&dir);
            let writer = Arc::new(writer);
            let mut session = AppSession::new(MODEL, CWD);
            session.push_message(Message::user(BASE_MSG.into()));
            session.set_subagent_messages(TOOL_ID.into(), vec![Message::user(BASE_MSG.into())]);
            session.set_subagent_messages(
                SIBLING_AGENT_ID.into(),
                vec![Message::user(BASE_MSG.into())],
            );
            session.push_post(stored_post(Some(TOOL_ID), 2));
            session.push_post(stored_post(None, 2));
            let id = session.id;
            writer.set_live_children(id, vec![TOOL_ID.into(), SIBLING_AGENT_ID.into()]);
            writer.send(Arc::new(session.clone()));
            let publication = CheckpointPublication::default();
            let history = HistorySnapshot::new(Vec::new());
            let lease = session_lock::claim(&dir.ensure_subdir(SESSIONS_DIR).unwrap(), &id)
                .unwrap()
                .unwrap();
            let guard = lease.publication_guard();
            let (entered, entry) = flume::bounded(1);
            let (release, gate) = flume::bounded(1);
            lock(&writer.pending).child_stage_gate = Some((entered, gate));
            let checkpoint = smol::spawn({
                let writer = Arc::clone(&writer);
                let publication = publication.clone();
                async move {
                    writer
                        .checkpoint_staged(StagedSessionCheckpoint {
                            session_id: id,
                            checkpoint: None,
                            journal: None,
                            selected_config: None,
                            history: Some((
                                if child {
                                    HistoryDestination::Child(TOOL_ID.into())
                                } else {
                                    HistoryDestination::Main
                                },
                                history,
                            )),
                            publication,
                            claim: Arc::new(|publication| {
                                publication
                                    .try_publish()
                                    .then_some(())
                                    .ok_or_else(|| Arc::from(CHECKPOINT_CANCELLED))
                            }),
                            guard,
                        })
                        .await
                }
            });
            entry.recv_async().await.unwrap();
            let latest = writer.latest_snapshot(id).unwrap();
            for messages in [latest.messages(), &latest.subagent_messages()[TOOL_ID]] {
                assert_eq!(messages.len(), 1);
                assert_eq!(messages[0].user_text(), Some(BASE_MSG));
            }
            assert!(lock(&writer.pending).coordinator_pending.is_empty());
            session.set_title(TITLE.into());
            session.push_message(Message::user(RESUMED_MSG.into()));
            session.set_subagent_messages(
                SIBLING_AGENT_ID.into(),
                vec![Message::user(RESUMED_MSG.into())],
            );
            if replace {
                writer.forget(id);
                writer.seed(Arc::new(session.clone()));
            }
            if rebind {
                writer.bind_config_runtime(id, 2);
            }
            if lose_lock {
                lease.release().unwrap();
            }
            writer.send(Arc::new(session));
            if cancel {
                assert!(publication.cancel());
            }
            let cancelled = cancel || replace || rebind || lose_lock;
            release.send(()).unwrap();
            let result = checkpoint.await;
            assert_eq!(result.is_err(), cancelled);
            if !cancelled {
                assert!(!publication.cancel());
            }
            writer.shutdown(DRAIN_TIMEOUT);
            let loaded = AppSession::load(id, &dir).unwrap();
            assert_eq!(loaded.title, TITLE);
            assert_eq!(
                loaded.messages().len(),
                if child || cancelled { 2 } else { 0 }
            );
            assert_eq!(
                loaded
                    .subagent_messages()
                    .get(TOOL_ID)
                    .map_or(0, |messages| messages.len()),
                usize::from(!child || cancelled)
            );
            assert_eq!(loaded.posts().len(), 1 + usize::from(cancelled));
            assert_eq!(
                serde_json::to_value(&loaded.subagent_messages()[SIBLING_AGENT_ID]).unwrap(),
                serde_json::to_value(vec![Message::user(RESUMED_MSG.into())]).unwrap()
            );
        });
    }

    #[test_case(false; "without_interleaved_save")]
    #[test_case(true; "with_interleaved_save")]
    fn failed_replacement_keeps_committed_history(interleaved: bool) {
        let (_tmp, dir) = state_dir();
        block_sessions_dir(&dir);
        let (writer, _wake_rx) = manual_writer();
        let mut session = AppSession::new(MODEL, CWD);
        session.push_message(Message::user(BASE_MSG.into()));
        session.set_subagent_messages(TOOL_ID.into(), vec![Message::user(BASE_MSG.into())]);
        let id = session.id;
        writer.seed(Arc::new(session.clone()));
        let history = HistorySnapshot::new(vec![Message::user(REWOUND_MSG.into())]);
        let checkpoint = {
            let mut snapshot = config_checkpoint(1, 1);
            snapshot.config = None;
            snapshot.kind = SessionCheckpointKind::History;
            snapshot.history_revision = Some(1);
            let epoch = history.epoch;
            snapshot.history = Some(history);
            writer
                .coordinator_checkpoint()
                .checkpoint(CheckpointRequest {
                    session_id: id,
                    version: CheckpointVersion { revision: 1, epoch },
                    snapshot: Arc::new(snapshot),
                })
        };
        if interleaved {
            session.set_title(TITLE.into());
            writer.send(Arc::new(session.clone()));
        }
        let (warn_tx, _warn_rx) = flume::unbounded();
        let mut disk = Writer {
            dir: dir.clone(),
            warn_tx,
            logs: HashMap::new(),
            failing: HashSet::new(),
        };
        for _ in 0..=CHECKPOINT_RETRY_BACKOFFS.len() {
            if let Some(Entry::Save(save)) = lock(&writer.pending).entries.get_mut(&id) {
                save.retry_at = None;
            }
            disk.flush(&writer.pending);
        }
        assert!(
            matches!(smol::block_on(checkpoint), Err(CheckpointError::Save { session_id, .. }) if session_id == id)
        );
        assert!(lock(&writer.pending).coordinator_pending.is_empty());
        let latest = writer.latest_snapshot(id).unwrap();
        for messages in [latest.messages(), &latest.subagent_messages()[TOOL_ID]] {
            assert_eq!(
                serde_json::to_value(messages).unwrap(),
                serde_json::to_value(vec![Message::user(BASE_MSG.into())]).unwrap()
            );
        }
        std::fs::remove_file(dir.path().join(SESSIONS_DIR)).unwrap();
        let mut retry = latest.as_ref().clone();
        retry.set_title(TITLE.into());
        writer.send(Arc::new(retry));
        disk.flush(&writer.pending);
        let stored = AppSession::load(id, &dir).unwrap();
        assert_eq!(stored.title, TITLE);
        for messages in [stored.messages(), &stored.subagent_messages()[TOOL_ID]] {
            assert_eq!(
                serde_json::to_value(messages).unwrap(),
                serde_json::to_value(vec![Message::user(BASE_MSG.into())]).unwrap()
            );
        }
    }

    #[test_case(false; "main_history")]
    #[test_case(true; "child_history")]
    fn successive_replacements_fence_stale_snapshots_and_reconstructed_events(child: bool) {
        smol::block_on(async {
            let (_tmp, dir) = state_dir();
            let (writer, _warn_rx) = writer(&dir);
            let mut session = AppSession::new(MODEL, CWD);
            session.push_message(Message::user(BASE_MSG.into()));
            session.set_subagent_messages(TOOL_ID.into(), vec![Message::user(BASE_MSG.into())]);
            session.set_subagent_messages(
                SIBLING_AGENT_ID.into(),
                vec![Message::user(BASE_MSG.into())],
            );
            let id = session.id;
            let original = session.clone();
            writer.set_live_children(id, vec![TOOL_ID.into(), SIBLING_AGENT_ID.into()]);
            writer.send(Arc::new(session.clone()));
            let lease = session_lock::claim(&dir.ensure_subdir(SESSIONS_DIR).unwrap(), &id)
                .unwrap()
                .unwrap();
            let destination = if child {
                HistoryDestination::Child(TOOL_ID.into())
            } else {
                HistoryDestination::Main
            };
            let checkpoint = |history| {
                writer.checkpoint_staged(StagedSessionCheckpoint {
                    session_id: id,
                    checkpoint: None,
                    journal: None,
                    selected_config: None,
                    history: Some((destination.clone(), history)),
                    publication: CheckpointPublication::default(),
                    claim: Arc::new(|publication| {
                        publication
                            .try_publish()
                            .then_some(())
                            .ok_or_else(|| Arc::from(CHECKPOINT_CANCELLED))
                    }),
                    guard: lease.publication_guard(),
                })
            };
            let earlier = HistorySnapshot::new(vec![Message::user(REWOUND_MSG.into())]);
            checkpoint(earlier.clone()).await.unwrap();
            assert!(adopt_checkpoint_history(
                &mut session,
                &destination,
                &earlier,
                None
            ));
            let replacement = HistorySnapshot::new(vec![Message::user(RESUMED_MSG.into())]);
            checkpoint(replacement.clone()).await.unwrap();
            // A delayed save from B must not resurrect B after the B -> C replacement.
            writer.send(Arc::new(session.clone()));
            // Reallocation cannot turn an old producer epoch into a new one.
            let reconstructed = HistorySnapshot {
                messages: Arc::new(earlier.messages.as_ref().clone()),
                ..earlier
            };
            adopt_checkpoint_history(&mut session, &destination, &reconstructed, None);
            session.set_title(TITLE.into());
            session.set_subagent_messages(
                SIBLING_AGENT_ID.into(),
                vec![Message::user(REWOUND_MSG.into())],
            );
            writer.send(Arc::new(session.clone()));
            let latest = writer.latest_snapshot(id).unwrap();
            let saved = if child {
                latest.subagent_history_snapshot(TOOL_ID).unwrap()
            } else {
                latest.history_snapshot()
            };
            assert_eq!(saved.epoch, replacement.epoch);
            assert!(adopt_checkpoint_history(
                &mut session,
                &destination,
                &replacement,
                None
            ));
            assert!(!adopt_checkpoint_history(
                &mut session,
                &destination,
                &reconstructed,
                None
            ));
            writer.send(Arc::new(original));
            writer.send(Arc::new(session));
            writer.shutdown(DRAIN_TIMEOUT);
            let loaded = AppSession::load(id, &dir).unwrap();
            assert_eq!(loaded.title, TITLE);
            let selected = if child {
                &loaded.subagent_messages()[TOOL_ID]
            } else {
                loaded.messages()
            };
            assert_eq!(
                serde_json::to_value(selected).unwrap(),
                serde_json::to_value(&replacement.messages).unwrap()
            );
            let untouched = if child {
                loaded.messages()
            } else {
                &loaded.subagent_messages()[TOOL_ID]
            };
            assert_eq!(
                serde_json::to_value(untouched).unwrap(),
                serde_json::to_value(vec![Message::user(BASE_MSG.into())]).unwrap()
            );
            assert_eq!(
                serde_json::to_value(&loaded.subagent_messages()[SIBLING_AGENT_ID]).unwrap(),
                serde_json::to_value(vec![Message::user(REWOUND_MSG.into())]).unwrap()
            );
        });
    }

    #[test]
    fn coordinator_checkpoint_merges_without_losing_tui_state() {
        smol::block_on(async {
            let (_tmp, dir) = state_dir();
            let (writer, _warn_rx) = writer(&dir);
            let mut session = AppSession::new(MODEL, CWD);
            session.set_title(TITLE.into());
            session.meta.mode = Some(StoredMode::Plan);
            session.meta.queued_messages = vec!["queued".into()];
            let root = AgentId::generate();
            session.bind_root_id(root).unwrap();
            let id = session.id;
            let journal = Arc::new(SessionJournal::from_session(&session).unwrap());
            writer.seed(Arc::new(session.clone()));
            writer.bind_journal(id, Arc::clone(&journal));

            let options = SessionOptions::new(
                builtin_option_definitions(
                    "next/model",
                    [Arc::from("next/model")],
                    true,
                    true,
                    true,
                    ThinkingConfig::Effort(Effort::High),
                ),
                &Default::default(),
            )
            .unwrap()
            .snapshot();
            let history = HistorySnapshot::new(vec![Message::user(COORDINATOR_MSG.into())]);
            let stored = stored_config(NEXT_MODEL, NEXT_CWD);
            let token = journal
                .prepare(JournalCommit {
                    changes: vec![JournalChange {
                        agent: Some(root),
                        generation: 1,
                        change: StateChange::Config(stored.clone()),
                    }],
                    histories: [(root, history.clone())].into(),
                    managed_histories: [root].into(),
                    ..Default::default()
                })
                .await
                .unwrap();
            let version = CheckpointVersion {
                revision: 1,
                epoch: 1,
            };
            let (_lease, checkpoint) = guarded_writer(&writer, &dir, id);
            let ack = publish_config(
                &checkpoint,
                CheckpointRequest {
                    session_id: id,
                    version,
                    snapshot: Arc::new(SessionCheckpoint {
                        kind: SessionCheckpointKind::Config,
                        history_revision: Some(1),
                        history: Some(history),
                        options,
                        config: Some(config_projection(NEXT_MODEL, NEXT_CWD, 1, 1)),
                        journal: Some(token.snapshot().clone()),
                    }),
                },
            )
            .await
            .unwrap();

            assert_eq!(ack.version, version);
            token.commit().unwrap();
            let persisted = AppSession::load(id, &dir).unwrap();
            assert_eq!(persisted.meta.thinking, None);
            let mut later_ui = session;
            later_ui.set_title("later UI title".into());
            later_ui.meta.mode = Some(StoredMode::Plan);
            later_ui.meta.queued_messages = vec!["queued".into()];
            writer.send(Arc::new(later_ui));
            drop(checkpoint);
            writer.shutdown(DRAIN_TIMEOUT);
            let loaded = AppSession::load(id, &dir).unwrap();
            assert_eq!(loaded.title, "later UI title");
            assert_eq!(
                loaded.latest_state().unwrap().scopes[&Some(root)]
                    .config
                    .as_ref(),
                Some(&stored)
            );
            assert_eq!(loaded.meta.queued_messages, ["queued"]);
            assert_eq!(loaded.model, stored.model);
            assert_eq!(loaded.cwd, stored.cwd);
            assert_eq!(loaded.meta.yolo, None);
            assert!(!loaded.meta.fast);
            assert!(!loaded.meta.workflow);
            assert_eq!(loaded.meta.thinking, None);
            assert_eq!(message_texts(&loaded), [COORDINATOR_MSG]);
        });
    }

    #[test]
    fn divergent_tui_history_is_not_overwritten_by_coordinator_history() {
        smol::block_on(async {
            let (_tmp, dir) = state_dir();
            let (writer, _warn_rx) = writer(&dir);
            let mut session = AppSession::new(MODEL, CWD);
            session.push_message(Message::user("base".into()));
            let id = session.id;
            writer.send(Arc::new(session.clone()));

            let options = SessionOptions::new(
                builtin_option_definitions(
                    MODEL,
                    [Arc::from(MODEL)],
                    false,
                    false,
                    false,
                    ThinkingConfig::Off,
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
                        kind: SessionCheckpointKind::History,
                        history_revision: Some(1),
                        history: Some(HistorySnapshot::new(vec![Message::user(
                            "coordinator".into(),
                        )])),
                        options,
                        config: None,
                        journal: None,
                    }),
                })
                .await
                .unwrap();

            session.replace_messages(vec![Message::user("tui".into())]);
            writer.send(Arc::new(session));
            writer.shutdown(DRAIN_TIMEOUT);

            let loaded = AppSession::load(id, &dir).unwrap();
            assert_eq!(message_texts(&loaded), ["tui"]);
        });
    }

    /// A retiring coordinator's history flush can still be pending when the
    /// TUI rewinds the same session. Merging that stale history into every
    /// later TUI snapshot silently undid the rewind on disk.
    #[test_case(true, &[COORDINATOR_MSG] ; "matching_history_adopts_pending_coordinator_history")]
    #[test_case(false, &[REWOUND_MSG] ; "divergent_history_discards_pending_coordinator_history")]
    fn tui_snapshot_merges_pending_coordinator_history_only_from_its_base(
        history_matches_base: bool,
        expected: &[&str],
    ) {
        let (wake, _wake_rx) = flume::unbounded();
        let (_done_tx, done_rx) = flume::bounded(1);
        let writer = StorageWriter {
            pending: Arc::default(),
            wake,
            done_rx,
            stop: Arc::default(),
        };
        let mut session = AppSession::new(MODEL, CWD);
        session.push_message(Message::user(BASE_MSG.into()));
        let id = session.id;
        writer.seed(Arc::new(session.clone()));

        let options = SessionOptions::new(
            builtin_option_definitions(
                MODEL,
                [Arc::from(MODEL)],
                false,
                false,
                false,
                ThinkingConfig::Off,
            ),
            &Default::default(),
        )
        .unwrap()
        .snapshot();
        let pending_ack = writer
            .coordinator_checkpoint()
            .checkpoint(CheckpointRequest {
                session_id: id,
                version: CheckpointVersion {
                    revision: 1,
                    epoch: 1,
                },
                snapshot: Arc::new(SessionCheckpoint {
                    kind: SessionCheckpointKind::History,
                    history_revision: Some(1),
                    history: Some(HistorySnapshot::new(vec![Message::user(
                        COORDINATOR_MSG.into(),
                    )])),
                    options,
                    config: None,
                    journal: None,
                }),
            });

        if !history_matches_base {
            session.replace_messages(vec![Message::user(REWOUND_MSG.into())]);
        }
        for _ in 0..2 {
            writer.send(Arc::new(session.clone()));
            let state = lock(&writer.pending);
            let Some(Entry::Save(save)) = state.entries.get(&id) else {
                unreachable!()
            };
            assert_eq!(message_texts(&save.session), expected);
            assert_eq!(
                save.coordinator_history_base.is_some(),
                history_matches_base
            );
        }
        let (_tmp, dir) = state_dir();
        let (warn_tx, _warn_rx) = flume::unbounded();
        let mut disk = Writer {
            dir: dir.clone(),
            warn_tx,
            logs: HashMap::new(),
            failing: HashSet::new(),
        };
        disk.flush(&writer.pending);
        let ack = smol::block_on(pending_ack);
        if history_matches_base {
            assert!(ack.is_ok());
        } else {
            let Err(CheckpointError::Save { message, .. }) = ack else {
                panic!("superseded history checkpoint must fail")
            };
            assert_eq!(message.as_ref(), CHECKPOINT_REPLACED);
        }
        assert_eq!(
            message_texts(&AppSession::load(id, &dir).unwrap()),
            expected
        );
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

            let options = SessionOptions::new(
                builtin_option_definitions(
                    FAILED_MODEL,
                    [Arc::from(FAILED_MODEL)],
                    true,
                    true,
                    true,
                    ThinkingConfig::Off,
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
                        kind: SessionCheckpointKind::Options,
                        history_revision: None,
                        history: None,
                        options,
                        config: None,
                        journal: None,
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

    /// Drives the flushes by hand: the threaded writer leaves the gap
    /// between the failure warning, the requeue and the retry deadline to the
    /// scheduler, so asserting on them raced under load.
    #[test]
    fn failed_checkpoint_does_not_delay_another_session() {
        let (_tmp, dir) = state_dir();
        let sessions_dir = dir.path().join(SESSIONS_DIR);
        std::fs::create_dir(&sessions_dir).unwrap();
        let (writer, _wake_rx) = manual_writer();
        let (warn_tx, warn_rx) = flume::unbounded();
        let mut disk = Writer {
            dir: dir.clone(),
            warn_tx,
            logs: HashMap::new(),
            failing: HashSet::new(),
        };
        let retry_state = |id| match lock(&writer.pending).entries.get(&id) {
            Some(Entry::Save(save)) => (save.retry_attempt, save.retry_at),
            _ => panic!("blocked session entry missing"),
        };

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
        disk.flush(&writer.pending);
        let warning = warn_rx.try_recv().unwrap();
        assert!(warning.starts_with(SAVE_FAILED_PREFIX), "{warning}");
        let blocked_retry_at = Instant::now() + DEFERRED_RETRY;
        if let Some(Entry::Save(save)) = lock(&writer.pending).entries.get_mut(&blocked_id) {
            save.retry_at = Some(blocked_retry_at);
        }
        let blocked_retry = retry_state(blocked_id);

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
        let next_wake = disk.flush(&writer.pending);
        let ack = smol::block_on(healthy_ack).expect("healthy checkpoint was delayed or failed");
        assert_eq!(ack.version, healthy_version);
        assert_eq!(
            retry_state(blocked_id),
            blocked_retry,
            "the failed session must keep backing off while another session saves"
        );
        assert_eq!(next_wake, Some(blocked_retry_at));

        std::fs::remove_dir(blocked_tmp).unwrap();
        if let Some(Entry::Save(save)) = lock(&writer.pending).entries.get_mut(&blocked_id) {
            save.retry_at = None;
        }
        disk.flush(&writer.pending);
        assert!(smol::block_on(blocked_ack).is_ok());
        assert!(AppSession::load(healthy_id, &dir).is_ok());
        assert!(AppSession::load(blocked_id, &dir).is_ok());
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
        session.push_message(Message::user(RESUMED_MSG.into()));
        session.insert_tool_output(
            TOOL_ID.into(),
            ToolOutput::Plain(TOOL_TEXT.to_string().into()),
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
            Some(ToolOutput::Plain(out)) => assert_eq!(out.text, TOOL_TEXT),
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
        writer.delete(MakimaId::generate(), move |res| {
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
        session.push_message(Message::user(RESUMED_MSG.into()));
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

        session.push_message(Message::user(RESUMED_MSG.into()));
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
