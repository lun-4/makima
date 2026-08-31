//! Coalescing write-behind cache with incremental JSONL persistence.
//!
//! Apps post session snapshots keyed by session id; the writer thread drains
//! the newest snapshot of every session per wake and performs O(delta)
//! appends. Deletes travel through the same per-session slot as saves, so
//! whichever the app asked for last is what reaches disk.

use std::collections::{HashMap, HashSet};
use std::io;
use std::mem;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use maki_storage::checkpoint::{
    CheckpointAck, CheckpointError, CheckpointFuture, CheckpointRequest, CheckpointVersion,
    CheckpointWriter,
};
use maki_storage::id::MakiId;
use maki_storage::sessions::{SESSIONS_DIR, SessionError, SessionLog};
use maki_storage::{StateDir, StorageError};
use tracing::warn;

use maki_agent::session_coordinator::SessionCheckpoint;
use maki_agent::session_options::{
    ENABLED_VALUE, FAST_OPTION_ID, SessionOptionOwner, WORKFLOW_OPTION_ID, YOLO_OPTION_ID,
};

use crate::AppSession;

const SAVE_FAILED_PREFIX: &str = "Session save failed";
const SAVE_RECOVERED: &str = "Session save recovered";

type Pending = Arc<Mutex<PendingState>>;

#[derive(Default)]
struct PendingState {
    entries: HashMap<MakiId, Entry>,
    latest: HashMap<MakiId, Arc<AppSession>>,
    coordinator_history_bases: HashMap<MakiId, Arc<Vec<maki_providers::Message>>>,
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
}

struct CheckpointWaiter {
    version: CheckpointVersion,
    reply: flume::Sender<Result<CheckpointAck, CheckpointError>>,
}

pub struct StorageWriter {
    pending: Pending,
    wake: flume::Sender<()>,
    done_rx: flume::Receiver<()>,
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
        let Some(base) = state.latest.get(&session_id).cloned() else {
            return Box::pin(async move {
                Err(CheckpointError::Save {
                    session_id,
                    message: Arc::from("session snapshot is unavailable"),
                })
            });
        };
        if !histories_match(base.messages(), &request.snapshot.history) {
            state
                .coordinator_history_bases
                .entry(session_id)
                .or_insert_with(|| Arc::new(base.messages().to_vec()));
        }
        let merged = Arc::new(merge_checkpoint(&base, &request.snapshot));
        state.latest.insert(session_id, Arc::clone(&merged));
        let (reply, response) = flume::bounded(1);
        enqueue_locked(
            &mut state.entries,
            PendingSave {
                session: merged,
                waiters: vec![CheckpointWaiter { version, reply }],
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

        std::thread::Builder::new()
            .name("storage-writer".into())
            .spawn(move || {
                let mut writer = Writer {
                    dir,
                    warn_tx,
                    logs: HashMap::new(),
                    failing: HashSet::new(),
                };
                while wake_rx.recv().is_ok() {
                    writer.flush(&writer_pending);
                }
                writer.flush(&writer_pending);
                let _ = done_tx.send(());
            })
            .expect("failed to spawn storage writer thread");

        Self {
            pending,
            wake,
            done_rx,
        }
    }

    pub fn send(&self, session: Arc<AppSession>) {
        let id = session.id;
        let mut state = lock(&self.pending);
        let preserve_history = state
            .coordinator_history_bases
            .get(&id)
            .is_some_and(|base| histories_match(session.messages(), base));
        if !preserve_history {
            state.coordinator_history_bases.remove(&id);
        }
        let session = state
            .latest
            .get(&id)
            .map(|latest| Arc::new(merge_tui_snapshot(&session, latest, preserve_history)))
            .unwrap_or(session);
        state.latest.insert(id, Arc::clone(&session));
        enqueue_locked(
            &mut state.entries,
            PendingSave {
                session,
                waiters: Vec::new(),
            },
        );
        drop(state);
        wake_checkpoint(&self.pending, &self.wake, id);
    }

    pub fn coordinator_checkpoint(&self) -> Arc<dyn CheckpointWriter<SessionCheckpoint>> {
        Arc::new(CoordinatorCheckpointWriter {
            pending: Arc::clone(&self.pending),
            wake: self.wake.clone(),
        })
    }

    fn enqueue_checkpoint(&self, request: CheckpointRequest<AppSession>) -> CheckpointFuture {
        let session_id = request.session_id;
        let version = request.version;
        let (reply, response) = flume::bounded(1);
        let mut state = lock(&self.pending);
        state
            .latest
            .insert(session_id, Arc::clone(&request.snapshot));
        enqueue_locked(
            &mut state.entries,
            PendingSave {
                session: request.snapshot,
                waiters: vec![CheckpointWaiter { version, reply }],
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

    /// Delete a session's files on the writer thread; `done` fires there, so
    /// callers never block on disk. Deleting a session that was never written
    /// reports success, and a save enqueued afterwards supersedes the delete.
    pub fn delete(&self, id: MakiId, done: impl FnOnce(Result<(), SessionError>) + Send + 'static) {
        let mut state = lock(&self.pending);
        state.latest.remove(&id);
        state.coordinator_history_bases.remove(&id);
        let replaced = state.entries.insert(id, Entry::Delete(Box::new(done)));
        drop(state);
        if let Some(Entry::Save(save)) = replaced {
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
        drop(self.wake);
        if self.done_rx.recv_timeout(timeout).is_err() {
            warn!("storage writer did not drain within {timeout:?}");
        }
    }
}

fn lock(pending: &Pending) -> std::sync::MutexGuard<'_, PendingState> {
    pending.lock().unwrap_or_else(|e| e.into_inner())
}

fn enqueue_locked(entries: &mut HashMap<MakiId, Entry>, mut save: PendingSave) {
    let id = save.session.id;
    if let Some(Entry::Save(previous)) = entries.remove(&id) {
        save.waiters.extend(previous.waiters);
    }
    entries.insert(id, Entry::Save(save));
}

fn wake_checkpoint(pending: &Pending, wake: &flume::Sender<()>, id: MakiId) {
    if wake.send(()).is_err()
        && let Some(Entry::Save(save)) = lock(pending).entries.remove(&id)
    {
        fail_waiters(id, save.waiters, "storage writer unavailable");
    }
}

fn merge_tui_snapshot(
    incoming: &AppSession,
    latest: &AppSession,
    preserve_history: bool,
) -> AppSession {
    let mut session = incoming.clone();
    if preserve_history && !histories_match(session.messages(), latest.messages()) {
        session.replace_messages(latest.messages().to_vec());
    }
    session.set_model(latest.model.clone());
    session.set_cwd(latest.cwd.clone());
    session.meta.yolo = latest.meta.yolo;
    session.meta.fast = latest.meta.fast;
    session.meta.workflow = latest.meta.workflow;
    session.meta.session_options = latest.meta.session_options.clone();
    session
}

fn merge_checkpoint(base: &AppSession, checkpoint: &SessionCheckpoint) -> AppSession {
    let mut session = base.clone();
    if !histories_match(session.messages(), &checkpoint.history) {
        session.replace_messages(checkpoint.history.as_ref().clone());
    }
    session.set_model(checkpoint.model.to_string());
    session.set_cwd(checkpoint.cwd.to_string_lossy().into_owned());
    session.meta.yolo = option_enabled(&checkpoint.options, YOLO_OPTION_ID);
    session.meta.fast = option_enabled(&checkpoint.options, FAST_OPTION_ID);
    session.meta.workflow = option_enabled(&checkpoint.options, WORKFLOW_OPTION_ID);
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

fn histories_match(
    current: &[maki_providers::Message],
    candidate: &[maki_providers::Message],
) -> bool {
    current.len() == candidate.len()
        && matches!(
            (serde_json::to_vec(current), serde_json::to_vec(candidate)),
            (Ok(current), Ok(candidate)) if current == candidate
        )
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

    fn flush(&mut self, pending: &Pending) {
        // Bound first: a `for` head temporary lives for the whole loop, so
        // iterating the guard directly would deadlock the re-insert below.
        let batch = mem::take(&mut lock(pending).entries);
        for (id, entry) in batch {
            match entry {
                Entry::Save(save) => {
                    let result = self.write(&save.session);
                    match result {
                        Ok(()) => acknowledge_waiters(id, save.waiters),
                        Err(error) => {
                            let message: Arc<str> = Arc::from(error.to_string());
                            if save.waiters.is_empty() {
                                lock(pending).entries.entry(id).or_insert(Entry::Save(save));
                            } else {
                                fail_waiters(id, save.waiters, &message);
                            }
                            self.report(id, Err(message));
                            continue;
                        }
                    }
                    self.report(id, Ok::<(), &str>(()));
                }
                Entry::Delete(done) => {
                    self.forget(id);
                    done(match AppSession::delete(id, &self.dir) {
                        Err(SessionError::Storage(StorageError::NotFound(_))) => Ok(()),
                        result => result,
                    });
                }
            }
        }
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
    use tempfile::TempDir;

    const DRAIN_TIMEOUT: Duration = Duration::from_secs(30);
    const MODEL: &str = "test-model";
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
                        history: Arc::new(history),
                        model: Arc::from("next/model"),
                        cwd: "/tmp/next".into(),
                        options,
                    }),
                })
                .await
                .unwrap();

            assert_eq!(ack.version, version);
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
            assert!(loaded.meta.yolo);
            assert!(loaded.meta.fast);
            assert!(loaded.meta.workflow);
            assert_eq!(message_texts(&loaded), ["coordinator history"]);
        });
    }

    #[test]
    fn acknowledged_checkpoint_returns_save_failure() {
        smol::block_on(async {
            let (_tmp, dir) = state_dir();
            block_sessions_dir(&dir);
            let (writer, _warn_rx) = writer(&dir);
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

    /// A failed write stays queued: `checkpoint` never resends an unchanged
    /// revision, so the writer owns the retry, and the shutdown flush is the
    /// last one.
    #[test]
    fn failed_write_is_retried_by_a_later_flush() {
        let (_tmp, dir) = state_dir();
        block_sessions_dir(&dir);
        let (writer, warn_rx) = writer(&dir);
        let session = Arc::new(AppSession::new(MODEL, CWD));
        let id = session.id;

        writer.send(session);
        let warning = warn_rx.recv_timeout(DRAIN_TIMEOUT).unwrap();
        assert!(warning.starts_with(SAVE_FAILED_PREFIX), "{warning}");

        std::fs::remove_file(dir.path().join(SESSIONS_DIR)).unwrap();
        writer.shutdown(DRAIN_TIMEOUT);

        assert!(AppSession::load(id, &dir).is_ok());
        assert_eq!(warn_rx.recv_timeout(DRAIN_TIMEOUT).unwrap(), SAVE_RECOVERED);
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
