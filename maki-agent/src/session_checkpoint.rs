use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use maki_providers::{Message, ThinkingConfig, TokenUsage};
use maki_storage::checkpoint::{
    CheckpointAck, CheckpointClaim, CheckpointError, CheckpointFuture, CheckpointPublication,
    CheckpointRequest, CheckpointWriter,
};
use maki_storage::id::MakiId;
use maki_storage::sessions::{
    HistorySnapshot, SESSIONS_DIR, Session, SessionError, SessionLog, StoredMode, StoredSubagent,
};
use maki_storage::{StateDir, StorageError, session_lock::SessionPublicationGuard};

use crate::session_coordinator::{SessionCheckpoint, SessionCheckpointKind};
use crate::session_options::{
    ENABLED_VALUE, FAST_OPTION_ID, SessionOptionOwner, THINKING_OPTION_ID, WORKFLOW_OPTION_ID,
    YOLO_OPTION_ID,
};
use crate::{AgentMode, ToolOutput};

type StoredSession = Session<Message, TokenUsage, ToolOutput>;

const CHECKPOINT_CANCELLED: &str = "checkpoint publication cancelled";
const HISTORY_SUPERSEDED: &str = "history checkpoint superseded by a newer revision";
const HISTORY_MISSING_REVISION: &str = "history checkpoint has no history revision";
const HISTORY_MISSING_SNAPSHOT: &str = "history checkpoint has no exact history snapshot";

struct RetainedSession {
    session: StoredSession,
    history_version: Option<(u64, u64)>,
}

impl RetainedSession {
    fn validate_history(
        &self,
        checkpoint: &SessionCheckpoint,
    ) -> Result<Option<(u64, u64)>, &'static str> {
        validate_checkpoint_history(checkpoint, checkpoint.history.is_some())?;
        let (Some(history), Some(revision), false) = (
            &checkpoint.history,
            checkpoint.history_revision,
            checkpoint.kind == SessionCheckpointKind::Options,
        ) else {
            return Ok(self.history_version);
        };
        let incoming = (history.epoch, revision);
        if self
            .history_version
            .is_some_and(|current| incoming < current)
        {
            return Err(HISTORY_SUPERSEDED);
        }
        Ok(Some(incoming))
    }
}

type SaveJob =
    Box<dyn FnOnce(&StateDir, &Mutex<RetainedSession>, Option<&SessionPublicationGuard>) + Send>;
type SaveFuture<R> = Pin<Box<dyn Future<Output = Result<R, CheckpointError>> + Send>>;

#[derive(Clone)]
pub struct SessionLogCheckpoint {
    session_id: MakiId,
    save_tx: flume::Sender<SaveJob>,
    #[cfg(test)]
    session: Arc<Mutex<RetainedSession>>,
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

impl SessionLogCheckpoint {
    pub fn open(
        dir: StateDir,
        session_id: MakiId,
        model: &str,
        cwd: &str,
        publication_guard: Option<SessionPublicationGuard>,
    ) -> Result<Self, CheckpointError> {
        let session = match StoredSession::load(session_id, &dir) {
            Ok(session) => session,
            Err(SessionError::Storage(StorageError::NotFound(_))) => {
                let mut session = StoredSession::new(model, cwd);
                session.id = session_id;
                session
            }
            Err(error) => {
                return Err(CheckpointError::Save {
                    session_id,
                    message: Arc::from(error.to_string()),
                });
            }
        };
        let session = Arc::new(Mutex::new(RetainedSession {
            session,
            history_version: None,
        }));
        let (save_tx, save_rx) = flume::unbounded::<SaveJob>();
        let worker_session = Arc::clone(&session);
        smol::spawn(async move {
            while let Ok(job) = save_rx.recv_async().await {
                let dir = dir.clone();
                let session = Arc::clone(&worker_session);
                let publication_guard = publication_guard.clone();
                smol::unblock(move || job(&dir, &session, publication_guard.as_ref())).await;
            }
        })
        .detach();
        Ok(Self {
            session_id,
            save_tx,
            #[cfg(test)]
            session,
        })
    }

    /// Runs `job` on the blocking pool after every earlier job has finished.
    fn submit<R: Send + 'static>(
        &self,
        job: impl FnOnce(
            &StateDir,
            &Mutex<RetainedSession>,
            Option<&SessionPublicationGuard>,
        ) -> Result<R, CheckpointError>
        + Send
        + 'static,
    ) -> SaveFuture<R> {
        let session_id = self.session_id;
        let (reply, response) = flume::bounded(1);
        let sent = self.save_tx.send(Box::new(move |dir, session, guard| {
            let _ = reply.send(job(dir, session, guard));
        }));
        Box::pin(async move {
            sent.map_err(|_| CheckpointError::Closed(session_id))?;
            response
                .recv_async()
                .await
                .unwrap_or(Err(CheckpointError::Closed(session_id)))
        })
    }

    /// `child.tool_use_id` is the canonical history key supplied by the host.
    /// Returns after publication is attempted, without waiting on the child runner.
    pub async fn checkpoint_child(
        &self,
        child: StoredSubagent,
        history: HistorySnapshot<Message>,
    ) -> Result<(), CheckpointError> {
        let session_id = self.session_id;
        self.submit(move |dir, session, guard| {
            Self::save_child(dir, session, session_id, child, &history, guard)
        })
        .await
    }

    /// Publishes only metadata for the host-authorized canonical child key.
    /// Histories and posts come exclusively from the acknowledged retained session.
    pub async fn checkpoint_child_config(
        &self,
        child: StoredSubagent,
        publication: CheckpointPublication,
        claim: CheckpointClaim,
    ) -> Result<(), CheckpointError> {
        self.submit(move |dir, session, guard| {
            Self::save_child_config(dir, session, child, guard, &publication, &claim)
        })
        .await
    }

    pub async fn drain(&self) -> Result<(), CheckpointError> {
        self.submit(|_, _, _| Ok(())).await
    }

    fn save(
        dir: &StateDir,
        session: &Mutex<RetainedSession>,
        request: CheckpointRequest<SessionCheckpoint>,
        publication_guard: Option<&SessionPublicationGuard>,
    ) -> Result<CheckpointAck, CheckpointError> {
        let (mut candidate, history_version) = {
            let retained = lock(session);
            let revision = retained
                .validate_history(&request.snapshot)
                .map_err(|message| CheckpointError::Save {
                    session_id: request.session_id,
                    message: Arc::from(message),
                })?;
            (retained.session.clone(), revision)
        };
        apply_session_checkpoint(&mut candidate, &request.snapshot);
        let saved = match publication_guard {
            Some(guard) => guard
                .publish(|| candidate.save(dir))
                .map_err(|error| CheckpointError::Save {
                    session_id: request.session_id,
                    message: Arc::from(error.to_string()),
                })?
                .ok_or(CheckpointError::Closed(request.session_id))?,
            None => candidate.save(dir),
        };
        if let Err(error) = saved {
            if let Ok(published) = StoredSession::load(request.session_id, dir) {
                lock(session).session = published;
            }
            return Err(CheckpointError::Save {
                session_id: request.session_id,
                message: Arc::from(error.to_string()),
            });
        }
        *lock(session) = RetainedSession {
            session: candidate,
            history_version,
        };
        Ok(CheckpointAck::for_request(&request))
    }

    fn save_child(
        dir: &StateDir,
        session: &Mutex<RetainedSession>,
        session_id: MakiId,
        child: StoredSubagent,
        history: &HistorySnapshot<Message>,
        guard: Option<&SessionPublicationGuard>,
    ) -> Result<(), CheckpointError> {
        let failure = |message| CheckpointError::Save {
            session_id,
            message,
        };
        let mut candidate = lock(session).session.clone();
        let key = child.tool_use_id.clone();
        if candidate
            .subagent_history_snapshot(&key)
            .is_some_and(|selected| {
                selected.epoch == history.epoch && selected.messages.len() > history.messages.len()
            })
        {
            return Err(failure(Arc::from(HISTORY_SUPERSEDED)));
        }
        if !candidate.adopt_subagent_history(key.clone(), history) {
            return Err(failure(Arc::from(HISTORY_SUPERSEDED)));
        }
        candidate.prune_posts(Some(&key), history.messages.len());
        if !candidate
            .subagents()
            .iter()
            .any(|current| current.tool_use_id == key)
        {
            upsert_child(&mut candidate, child);
        }
        let directory = dir
            .ensure_subdir(SESSIONS_DIR)
            .map_err(|error| failure(Arc::from(error.to_string())))?;
        let staged = SessionLog::stage_checkpoint(&directory, &candidate)
            .map_err(|error| failure(Arc::from(error.to_string())))?;
        let published = match guard {
            Some(guard) => staged.publish(guard, || true),
            None => staged.publish_unowned(|| true),
        }
        .map_err(|error| failure(Arc::from(error.to_string())))?;
        if !published {
            return Err(CheckpointError::Closed(session_id));
        }
        lock(session).session = candidate;
        Ok(())
    }

    fn save_child_config(
        dir: &StateDir,
        session: &Mutex<RetainedSession>,
        child: StoredSubagent,
        guard: Option<&SessionPublicationGuard>,
        publication: &CheckpointPublication,
        claim: &CheckpointClaim,
    ) -> Result<(), CheckpointError> {
        let mut candidate = lock(session).session.clone();
        upsert_child(&mut candidate, child);
        Self::publish_candidate(dir, &candidate, guard, publication, claim)?;
        lock(session).session = candidate;
        Ok(())
    }

    fn save_guarded(
        dir: &StateDir,
        session: &Mutex<RetainedSession>,
        request: CheckpointRequest<SessionCheckpoint>,
        guard: Option<&SessionPublicationGuard>,
        publication: &CheckpointPublication,
        claim: &CheckpointClaim,
    ) -> Result<CheckpointAck, CheckpointError> {
        let failure = |message| CheckpointError::Save {
            session_id: request.session_id,
            message,
        };
        if publication.is_cancelled() {
            return Err(failure(Arc::from(CHECKPOINT_CANCELLED)));
        }
        let (mut candidate, history_version) = {
            let retained = lock(session);
            let revision = retained
                .validate_history(&request.snapshot)
                .map_err(|message| failure(Arc::from(message)))?;
            (retained.session.clone(), revision)
        };
        apply_session_checkpoint(&mut candidate, &request.snapshot);
        Self::publish_candidate(dir, &candidate, guard, publication, claim)?;
        *lock(session) = RetainedSession {
            session: candidate,
            history_version,
        };
        Ok(CheckpointAck::for_request(&request))
    }

    fn publish_candidate(
        dir: &StateDir,
        candidate: &StoredSession,
        guard: Option<&SessionPublicationGuard>,
        publication: &CheckpointPublication,
        claim: &CheckpointClaim,
    ) -> Result<(), CheckpointError> {
        let failure = |message| CheckpointError::Save {
            session_id: candidate.id,
            message,
        };
        if publication.is_cancelled() {
            return Err(failure(Arc::from(CHECKPOINT_CANCELLED)));
        }
        let directory = dir
            .ensure_subdir(SESSIONS_DIR)
            .map_err(|error| failure(Arc::from(error.to_string())))?;
        let staged = SessionLog::stage_checkpoint(&directory, candidate)
            .map_err(|error| failure(Arc::from(error.to_string())))?;
        let mut claim_error = None;
        let mut authorized = || match claim(publication) {
            Ok(()) => true,
            Err(error) => {
                publication.cancel();
                claim_error = Some(error);
                false
            }
        };
        let published = match guard {
            Some(guard) => staged.publish(guard, authorized),
            None => staged.publish_unowned(&mut authorized),
        }
        .map_err(|error| failure(Arc::from(error.to_string())))?;
        if !published {
            publication.cancel();
            return Err(failure(
                claim_error.unwrap_or_else(|| Arc::from(CHECKPOINT_CANCELLED)),
            ));
        }
        Ok(())
    }
}

impl CheckpointWriter<SessionCheckpoint> for SessionLogCheckpoint {
    fn checkpoint_guarded(
        &self,
        request: CheckpointRequest<SessionCheckpoint>,
        publication: CheckpointPublication,
        claim: CheckpointClaim,
    ) -> CheckpointFuture {
        self.submit(move |dir, session, guard| {
            Self::save_guarded(dir, session, request, guard, &publication, &claim)
        })
    }

    fn checkpoint(&self, request: CheckpointRequest<SessionCheckpoint>) -> CheckpointFuture {
        self.submit(move |dir, session, guard| Self::save(dir, session, request, guard))
    }
}

fn upsert_child(candidate: &mut StoredSession, child: StoredSubagent) {
    let mut children = candidate.subagents().to_vec();
    if let Some(current) = children
        .iter_mut()
        .find(|current| current.tool_use_id == child.tool_use_id)
    {
        *current = child;
    } else {
        children.push(child);
    }
    candidate.set_subagents(children);
}

/// Rejects a checkpoint whose history fields cannot be applied exactly.
pub fn validate_checkpoint_history(
    checkpoint: &SessionCheckpoint,
    has_history: bool,
) -> Result<(), &'static str> {
    match checkpoint.kind {
        SessionCheckpointKind::Options => Ok(()),
        SessionCheckpointKind::History if !has_history => Err(HISTORY_MISSING_SNAPSHOT),
        _ if has_history && checkpoint.history_revision.is_none() => Err(HISTORY_MISSING_REVISION),
        _ => Ok(()),
    }
}

/// Projects a coordinator checkpoint onto the stored session. Only options
/// present in the snapshot are written; plugin options replace the stored map.
pub fn apply_session_checkpoint(session: &mut StoredSession, checkpoint: &SessionCheckpoint) {
    if checkpoint.kind != SessionCheckpointKind::Options
        && let Some(history) = &checkpoint.history
    {
        session.adopt_history(history);
        session.update_title_if_default();
    }
    if checkpoint.kind == SessionCheckpointKind::History {
        return;
    }
    let config = checkpoint.kind == SessionCheckpointKind::Config;
    if config && let Some(projection) = &checkpoint.config {
        let config = &projection.config;
        session.set_model(config.model.spec());
        session.set_cwd(config.context.cwd().to_string_lossy().into_owned());
        session.meta.mode = Some(match &config.mode {
            AgentMode::Build => StoredMode::Build,
            AgentMode::Plan(_) => StoredMode::Plan,
            AgentMode::Custom(mode) => StoredMode::Custom(mode.to_string()),
        });
        session.meta.plan_path = match &config.mode {
            AgentMode::Plan(path) => Some(path.to_string_lossy().into_owned()),
            _ => None,
        };
    }
    let meta = &mut session.meta;
    meta.session_options.clear();
    for option in checkpoint.options.options.iter() {
        let id = option.definition.id.as_ref();
        let enabled = option.current_value.as_ref() == ENABLED_VALUE;
        match id {
            YOLO_OPTION_ID => meta.yolo = Some(enabled),
            FAST_OPTION_ID if config => meta.fast = enabled,
            WORKFLOW_OPTION_ID if config => meta.workflow = enabled,
            // Thinking predates session options and keeps its own field, so
            // the projection writes there rather than into the generic map.
            THINKING_OPTION_ID if config => {
                meta.thinking = option
                    .current_value
                    .parse::<ThinkingConfig>()
                    .ok()
                    .map(Into::into);
            }
            _ if option.definition.persistent
                && matches!(option.definition.owner, SessionOptionOwner::Plugin { .. }) =>
            {
                meta.session_options
                    .insert(id.to_string(), option.current_value.to_string());
            }
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::future::Future;
    use std::path::PathBuf;
    use std::task::{Context, Poll, Waker};

    use maki_storage::checkpoint::{CheckpointRequest, CheckpointVersion};
    use maki_storage::id::MakiId;
    use maki_storage::session_lock;
    use maki_storage::sessions::StoredPost;
    use tempfile::TempDir;

    use super::*;
    use crate::RunSettings;
    use crate::actor::{AgentContextSnapshot, EffectiveAgentConfig};
    use crate::session_coordinator::{CommittedConfigProjection, builtin_option_definitions};
    use crate::session_options::{DISABLED_VALUE, SessionOptions};
    use crate::tools::NullProvider;
    use crate::{Post, PostLevel};
    use maki_providers::Model;

    const MODEL: &str = "ollama/model";
    const COMMITTED_MODEL: &str = "ollama/committed";
    const CWD: &str = "/project";
    const COMMITTED_CWD: &str = "/committed-project";
    const ORIGINAL: &str = "original";
    const TERMINAL: &str = "terminal";
    const REJECTED: &str = "rejected";
    const CLAIM_ERROR: &str = "checkpoint authority revoked";
    const CATALOG_ID: &str = "test.selection";
    const CATALOG_PLUGIN: &str = "test";
    const CATALOG_VALUE: &str = "updated";
    const CHILD_KEY: &str = "task/child";
    const SIBLING_KEY: &str = "task/sibling";
    const CHILD_NAME: &str = "child";
    const POST_TEXT: &str = "child observation";

    fn child(key: &str) -> StoredSubagent {
        StoredSubagent {
            tool_use_id: key.into(),
            name: CHILD_NAME.into(),
            model: Some(MODEL.into()),
            thinking: Some(ThinkingConfig::Off.into()),
            fast: false,
        }
    }

    fn stored_value(session: &StoredSession) -> serde_json::Value {
        serde_json::json!({
            "id": session.id,
            "title": session.title,
            "cwd": session.cwd,
            "model": session.model,
            "messages": session.messages(),
            "token_usage": session.token_usage,
            "tool_outputs": session.tool_outputs(),
            "subagent_messages": session.subagent_messages(),
            "subagents": session.subagents(),
            "posts": session.posts(),
            "usage_by_model": session.usage_by_model(),
            "meta": session.meta,
            "created_at": session.created_at,
            "updated_at": session.updated_at,
        })
    }

    fn child_session(id: MakiId) -> StoredSession {
        let mut session = StoredSession::new(MODEL, CWD);
        session.id = id;
        session.push_message(Message::user(ORIGINAL.into()));
        session.set_subagents(vec![child(CHILD_KEY), child(SIBLING_KEY)]);
        for key in [CHILD_KEY, SIBLING_KEY] {
            session.set_subagent_messages(
                key.into(),
                vec![
                    Message::user(ORIGINAL.into()),
                    Message::user(ORIGINAL.into()),
                ],
            );
        }
        for chat in [None, Some(CHILD_KEY), Some(SIBLING_KEY)] {
            session.push_post(StoredPost {
                chat: chat.map(Into::into),
                after_message: 2,
                post: Post {
                    text: POST_TEXT.into(),
                    level: PostLevel::Info,
                    agent: None,
                },
            });
        }
        session
    }

    fn request(
        id: MakiId,
        revision: u64,
        model: &'static str,
        options: &SessionOptions,
    ) -> CheckpointRequest<SessionCheckpoint> {
        CheckpointRequest {
            session_id: id,
            version: CheckpointVersion { revision, epoch: 1 },
            snapshot: Arc::new(SessionCheckpoint {
                kind: SessionCheckpointKind::Config,
                history_revision: None,
                history: None,
                options: options.snapshot(),
                config: Some(projection(model, CWD)),
            }),
        }
    }

    fn projection(model: &str, cwd: &str) -> CommittedConfigProjection {
        let mut config = EffectiveAgentConfig::new(
            RunSettings {
                provider: Arc::new(NullProvider),
                model: Model::from_spec(model).unwrap(),
                fast: false,
                workflow: false,
                thinking: ThinkingConfig::Off,
            },
            AgentMode::Build,
        );
        config.context = AgentContextSnapshot::new(PathBuf::from(cwd));
        CommittedConfigProjection {
            runtime: Arc::new(()),
            generation: 1,
            runtime_epoch: 1,
            config: Arc::new(config),
        }
    }

    #[test]
    fn checkpoint_call_does_not_run_disk_save_on_the_executor() {
        smol::block_on(async {
            let tmp = TempDir::new().unwrap();
            let dir = StateDir::from_path(tmp.path().to_path_buf());
            let id = MakiId::generate();
            let options = SessionOptions::new(
                builtin_option_definitions(
                    "test/model",
                    [Arc::from("test/model")],
                    false,
                    false,
                    false,
                    ThinkingConfig::Off,
                ),
                &BTreeMap::new(),
            )
            .unwrap();
            let writer = Arc::new(SessionLogCheckpoint::open(dir, id, MODEL, CWD, None).unwrap());
            let session_guard = lock(&writer.session);
            let (checkpoint_tx, checkpoint_rx) = flume::bounded(1);
            let checkpoint_writer = Arc::clone(&writer);
            let checkpoint_request = request(id, 1, MODEL, &options);
            let call = std::thread::spawn(move || {
                checkpoint_tx
                    .send(checkpoint_writer.checkpoint(checkpoint_request))
                    .unwrap();
            });

            let checkpoint = checkpoint_rx
                .recv_timeout(std::time::Duration::from_secs(1))
                .expect("checkpoint construction must not perform the save");
            drop(session_guard);
            call.join().unwrap();
            checkpoint.await.unwrap();
        });
    }

    #[test_case::test_case(false; "existing_child_metadata")]
    #[test_case::test_case(true; "new_child_metadata")]
    fn child_completion_waits_for_serialized_publication(new_child: bool) {
        smol::block_on(async {
            let tmp = TempDir::new().unwrap();
            let dir = StateDir::from_path(tmp.path().to_path_buf());
            let id = MakiId::generate();
            let mut initial = child_session(id);
            if new_child {
                initial.set_subagents(vec![child(SIBLING_KEY)]);
            }
            initial.save(&dir).unwrap();
            let writer = SessionLogCheckpoint::open(dir.clone(), id, MODEL, CWD, None).unwrap();
            let sibling_writer = writer.clone();
            let history = HistorySnapshot::new(vec![Message::user(TERMINAL.into())]);
            let sibling_history = HistorySnapshot::new(vec![Message::user(COMMITTED_MODEL.into())]);
            let publication = CheckpointPublication::default();
            let mut configured_child = child(CHILD_KEY);
            configured_child.name = TERMINAL.into();
            configured_child.model = Some(COMMITTED_MODEL.into());
            configured_child.thinking = Some(ThinkingConfig::Adaptive.into());
            configured_child.fast = true;
            let (root_history, child_history, sibling_before, posts, history_version) = {
                let retained = lock(&writer.session);
                (
                    retained.session.history_snapshot(),
                    retained
                        .session
                        .subagent_history_snapshot(CHILD_KEY)
                        .unwrap(),
                    retained
                        .session
                        .subagent_history_snapshot(SIBLING_KEY)
                        .unwrap(),
                    retained.session.posts().to_vec(),
                    retained.history_version,
                )
            };
            writer
                .checkpoint_child_config(
                    configured_child.clone(),
                    CheckpointPublication::default(),
                    Arc::new(|publication| {
                        assert!(publication.try_publish());
                        Ok(())
                    }),
                )
                .await
                .unwrap();
            {
                let retained = lock(&writer.session);
                assert_eq!(retained.history_version, history_version);
                assert_eq!(retained.session.posts(), posts);
                for (before, after) in [
                    (root_history, retained.session.history_snapshot()),
                    (
                        child_history,
                        retained
                            .session
                            .subagent_history_snapshot(CHILD_KEY)
                            .unwrap(),
                    ),
                    (
                        sibling_before,
                        retained
                            .session
                            .subagent_history_snapshot(SIBLING_KEY)
                            .unwrap(),
                    ),
                ] {
                    assert_eq!(before.epoch, after.epoch);
                    assert!(Arc::ptr_eq(&before.messages, &after.messages));
                }
                let mut metadata_only = initial.clone();
                upsert_child(&mut metadata_only, configured_child.clone());
                metadata_only.updated_at = retained.session.updated_at;
                assert_eq!(
                    stored_value(&StoredSession::load(id, &dir).unwrap()),
                    stored_value(&metadata_only)
                );
            }
            let (completed, sibling) = {
                let session_guard = lock(&writer.session);
                let mut config = Box::pin(writer.checkpoint_child_config(
                    configured_child.clone(),
                    publication.clone(),
                    Arc::new(|publication| {
                        assert!(publication.try_publish());
                        Ok(())
                    }),
                ));
                let mut completed =
                    Box::pin(writer.checkpoint_child(child(CHILD_KEY), history.clone()));
                let mut sibling = Box::pin(
                    sibling_writer.checkpoint_child(child(SIBLING_KEY), sibling_history.clone()),
                );
                let mut context = Context::from_waker(Waker::noop());
                assert!(config.as_mut().poll(&mut context).is_pending());
                assert!(completed.as_mut().poll(&mut context).is_pending());
                assert!(sibling.as_mut().poll(&mut context).is_pending());
                assert_eq!(
                    StoredSession::load(id, &dir).unwrap().subagent_messages()[CHILD_KEY][0]
                        .first_text_content(),
                    Some(ORIGINAL)
                );
                assert!(
                    session_guard
                        .session
                        .subagents()
                        .contains(&configured_child)
                );
                drop(config);
                (completed, sibling)
            };
            let (completed, sibling) = futures_lite::future::zip(completed, sibling).await;
            assert!(!publication.cancel());
            completed.unwrap();
            sibling.unwrap();
            for stale in [
                HistorySnapshot {
                    epoch: history.epoch - 1,
                    messages: Arc::new(vec![Message::user(REJECTED.into())]),
                    ..history.clone()
                },
                HistorySnapshot {
                    messages: Arc::new(Vec::new()),
                    ..history.clone()
                },
            ] {
                assert!(
                    matches!(writer.checkpoint_child(child(CHILD_KEY), stale).await, Err(CheckpointError::Save { message, .. }) if message.as_ref() == HISTORY_SUPERSEDED)
                );
            }
            let loaded = StoredSession::load(id, &dir).unwrap();
            {
                let retained = lock(&writer.session);
                for saved in [&loaded, &retained.session] {
                    assert_eq!(saved.messages()[0].first_text_content(), Some(ORIGINAL));
                    assert_eq!(saved.model, MODEL);
                    assert_eq!(saved.cwd, CWD);
                    let expected = if new_child {
                        vec![child(SIBLING_KEY), configured_child.clone()]
                    } else {
                        vec![configured_child.clone(), child(SIBLING_KEY)]
                    };
                    assert_eq!(saved.subagents(), expected);
                    assert_eq!(
                        saved.subagent_messages()[CHILD_KEY][0].first_text_content(),
                        Some(TERMINAL)
                    );
                    assert_eq!(
                        saved.subagent_messages()[SIBLING_KEY][0].first_text_content(),
                        Some(COMMITTED_MODEL)
                    );
                    assert_eq!(saved.posts().len(), 1);
                    assert!(saved.posts()[0].chat.is_none());
                }
                assert!(retained.history_version.is_none());
                assert!(Arc::ptr_eq(
                    &retained
                        .session
                        .subagent_history_snapshot(CHILD_KEY)
                        .unwrap()
                        .messages,
                    &history.messages
                ));
                assert!(Arc::ptr_eq(
                    &retained
                        .session
                        .subagent_history_snapshot(SIBLING_KEY)
                        .unwrap()
                        .messages,
                    &sibling_history.messages
                ));
            }
        });
    }

    #[test_case::test_case(false, false; "ordinary_history_then_config")]
    #[test_case::test_case(false, true; "ordinary_config_then_history")]
    #[test_case::test_case(true, false; "guarded_history_then_config")]
    #[test_case::test_case(true, true; "guarded_config_then_history")]
    fn concurrent_checkpoints_preserve_submission_order(guarded: bool, config_first: bool) {
        smol::block_on(async {
            let tmp = TempDir::new().unwrap();
            let dir = StateDir::from_path(tmp.path().to_path_buf());
            let id = MakiId::generate();
            let options = SessionOptions::new(
                builtin_option_definitions(
                    "test/model",
                    [Arc::from("test/model")],
                    false,
                    false,
                    false,
                    ThinkingConfig::Off,
                ),
                &BTreeMap::new(),
            )
            .unwrap();
            child_session(id).save(&dir).unwrap();
            let owner = config_first.then(|| {
                session_lock::claim(&dir.ensure_subdir(SESSIONS_DIR).unwrap(), &id)
                    .unwrap()
                    .unwrap()
            });
            let writer = SessionLogCheckpoint::open(
                dir.clone(),
                id,
                MODEL,
                CWD,
                owner.as_ref().map(|owner| owner.publication_guard()),
            )
            .unwrap();
            let publication = CheckpointPublication::default();
            let claim: CheckpointClaim = Arc::new(|publication| {
                publication
                    .try_publish()
                    .then_some(())
                    .ok_or_else(|| Arc::from(CHECKPOINT_CANCELLED))
            });
            let child_history = HistorySnapshot::new(vec![Message::user(TERMINAL.into())]);
            let mut completed_child = child(CHILD_KEY);
            completed_child.model = Some(COMMITTED_MODEL.into());
            completed_child.thinking = Some(ThinkingConfig::Adaptive.into());
            completed_child.fast = true;
            writer
                .checkpoint_child(completed_child.clone(), child_history.clone())
                .await
                .unwrap();
            completed_child.name = TERMINAL.into();
            writer
                .checkpoint_child_config(
                    completed_child.clone(),
                    CheckpointPublication::default(),
                    Arc::clone(&claim),
                )
                .await
                .unwrap();
            writer
                .checkpoint_child(child(CHILD_KEY), child_history)
                .await
                .unwrap();
            assert_eq!(
                StoredSession::load(id, &dir).unwrap().subagents(),
                &[completed_child.clone(), child(SIBLING_KEY)]
            );
            let mut history = HistorySnapshot::new(vec![Message::user(TERMINAL.into())]);
            history.compacted = true;
            let mut config = request(id, u64::from(!config_first) + 1, COMMITTED_MODEL, &options);
            let candidate = Arc::make_mut(&mut config.snapshot);
            candidate.config = Some(projection(COMMITTED_MODEL, COMMITTED_CWD));
            for option in Arc::make_mut(&mut candidate.options.options) {
                if matches!(
                    option.definition.id.as_ref(),
                    FAST_OPTION_ID | WORKFLOW_OPTION_ID
                ) {
                    option.current_value = Arc::from(ENABLED_VALUE);
                }
            }
            let mut terminal = request(id, u64::from(config_first) + 1, MODEL, &options);
            let candidate = Arc::make_mut(&mut terminal.snapshot);
            candidate.kind = SessionCheckpointKind::History;
            candidate.history_revision = Some(1);
            candidate.history = Some(history.clone());
            let mut config = Some(config);
            let mut submit_config = || {
                let request = config.take().unwrap();
                if guarded {
                    writer.checkpoint_guarded(request, publication.clone(), Arc::clone(&claim))
                } else {
                    writer.checkpoint(request)
                }
            };
            let mut terminal = Some(terminal);
            let first = if config_first {
                submit_config()
            } else {
                writer.checkpoint(terminal.take().unwrap())
            };
            let mut child_config = Box::pin(writer.checkpoint_child_config(
                completed_child.clone(),
                CheckpointPublication::default(),
                Arc::clone(&claim),
            ));
            let child_config_result = child_config
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()));
            let second = if config_first {
                writer.checkpoint(terminal.take().unwrap())
            } else {
                submit_config()
            };
            let (first, second) = futures_lite::future::zip(first, second).await;
            match child_config_result {
                Poll::Ready(result) => result,
                Poll::Pending => child_config.await,
            }
            .unwrap();
            for (ack, revision) in [(first.unwrap(), 1), (second.unwrap(), 2)] {
                assert_eq!(ack.session_id, id);
                assert_eq!(ack.version, CheckpointVersion { revision, epoch: 1 });
            }
            if guarded {
                assert!(!publication.cancel());
                assert_eq!(
                    writer
                        .checkpoint_guarded(request(id, 3, MODEL, &options), publication, claim)
                        .await
                        .unwrap_err(),
                    CheckpointError::Save {
                        session_id: id,
                        message: Arc::from(CHECKPOINT_CANCELLED)
                    }
                );
            }
            writer.drain().await.unwrap();
            let loaded: StoredSession = StoredSession::load(id, &dir).unwrap();
            {
                let retained = lock(&writer.session);
                for saved in [&loaded, &retained.session] {
                    assert_eq!(saved.model, COMMITTED_MODEL);
                    assert_eq!(saved.cwd, COMMITTED_CWD);
                    assert!(saved.meta.fast);
                    assert!(saved.meta.workflow);
                    assert!(saved.meta.compacted);
                    assert_eq!(saved.messages()[0].first_text_content(), Some(TERMINAL));
                    assert_eq!(
                        saved.subagents(),
                        &[completed_child.clone(), child(SIBLING_KEY)]
                    );
                    assert_eq!(
                        saved.subagent_messages()[CHILD_KEY][0].first_text_content(),
                        Some(TERMINAL)
                    );
                    assert_eq!(saved.subagent_messages()[SIBLING_KEY].len(), 2);
                    assert_eq!(
                        saved
                            .posts()
                            .iter()
                            .map(|post| post.chat.as_deref())
                            .collect::<Vec<_>>(),
                        [None, Some(SIBLING_KEY)]
                    );
                }
                assert_eq!(retained.session.history_snapshot().epoch, history.epoch);
                assert!(Arc::ptr_eq(
                    &retained.session.history_snapshot().messages,
                    &history.messages
                ));
                assert_eq!(retained.history_version, Some((history.epoch, 1)));
            }
            let mut stale = request(id, 3, MODEL, &options);
            let candidate = Arc::make_mut(&mut stale.snapshot);
            candidate.kind = SessionCheckpointKind::History;
            candidate.history_revision = Some(0);
            candidate.history = Some(HistorySnapshot {
                messages: Arc::new(vec![Message::user(REJECTED.into())]),
                ..history
            });
            assert!(
                matches!(writer.checkpoint(stale).await, Err(CheckpointError::Save { message, .. }) if message.as_ref() == HISTORY_SUPERSEDED)
            );
            assert_eq!(
                StoredSession::load(id, &dir).unwrap().messages()[0].first_text_content(),
                Some(TERMINAL)
            );
            if let Some(owner) = owner {
                owner.release().unwrap();
            }
        });
    }

    #[test_case::test_case(false, false, false; "ordinary_io_error")]
    #[test_case::test_case(true, false, false; "guarded_io_error")]
    #[test_case::test_case(true, true, false; "rejected_claim")]
    #[test_case::test_case(true, true, true; "claimed_error")]
    fn failed_candidate_is_not_retained_for_later_checkpoint(
        guarded: bool,
        claim_error: bool,
        claimed: bool,
    ) {
        let tmp = TempDir::new().unwrap();
        let dir = StateDir::from_path(tmp.path().to_path_buf());
        let id = MakiId::generate();
        let options = SessionOptions::new(
            builtin_option_definitions(
                "test/model",
                [Arc::from("test/model")],
                false,
                false,
                false,
                ThinkingConfig::Off,
            ),
            &BTreeMap::new(),
        )
        .unwrap();
        child_session(id).save(&dir).unwrap();
        let writer = SessionLogCheckpoint::open(dir.clone(), id, MODEL, CWD, None).unwrap();
        let original = Message::user(ORIGINAL.into());
        let history = HistorySnapshot::new(vec![original.clone()]);
        let mut accepted = request(id, 1, MODEL, &options);
        let candidate = Arc::make_mut(&mut accepted.snapshot);
        candidate.kind = SessionCheckpointKind::History;
        candidate.history = Some(history.clone());
        candidate.history_revision = Some(1);
        smol::block_on(writer.checkpoint(accepted)).unwrap();
        let sessions = dir.ensure_subdir(SESSIONS_DIR).unwrap();
        let path = sessions.join(format!("{id}.jsonl"));
        if !claim_error {
            std::fs::remove_file(&path).unwrap();
            std::fs::create_dir(&path).unwrap();
        }
        let mut failed = request(id, 2, COMMITTED_MODEL, &options);
        let rejected = Arc::make_mut(&mut failed.snapshot);
        rejected.history = Some(HistorySnapshot {
            messages: Arc::new(vec![Message::user(REJECTED.into())]),
            ..history.clone()
        });
        rejected.history_revision = Some(2);

        let publication = CheckpointPublication::default();
        let failed = if guarded {
            writer.checkpoint_guarded(
                failed,
                publication.clone(),
                Arc::new(move |publication| {
                    if !claim_error || claimed {
                        assert!(publication.try_publish());
                    }
                    if claim_error {
                        Err(Arc::from(CLAIM_ERROR))
                    } else {
                        Ok(())
                    }
                }),
            )
        } else {
            writer.checkpoint(failed)
        };
        let error = smol::block_on(failed).unwrap_err();
        let mut rejected_child = child(CHILD_KEY);
        rejected_child.name = REJECTED.into();
        rejected_child.model = Some(COMMITTED_MODEL.into());
        rejected_child.thinking = Some(ThinkingConfig::Adaptive.into());
        rejected_child.fast = true;
        let child_publication = CheckpointPublication::default();
        let child_error = smol::block_on(writer.checkpoint_child_config(
            rejected_child.clone(),
            child_publication.clone(),
            Arc::new(move |publication| {
                if !claim_error || claimed {
                    assert!(publication.try_publish());
                }
                if claim_error {
                    Err(Arc::from(CLAIM_ERROR))
                } else {
                    Ok(())
                }
            }),
        ))
        .unwrap_err();
        if claim_error {
            assert_eq!(
                child_error,
                CheckpointError::Save {
                    session_id: id,
                    message: Arc::from(CLAIM_ERROR),
                }
            );
        }
        assert_eq!(child_publication.is_cancelled(), claim_error && !claimed);
        if claim_error {
            assert_eq!(
                error,
                CheckpointError::Save {
                    session_id: id,
                    message: Arc::from(CLAIM_ERROR)
                }
            );
            assert_eq!(publication.is_cancelled(), !claimed);
            assert_eq!(
                StoredSession::load(id, &dir).unwrap().messages()[0].first_text_content(),
                Some(ORIGINAL)
            );
            std::fs::remove_file(&path).unwrap();
            std::fs::create_dir(&path).unwrap();
        }
        assert!(matches!(
            smol::block_on(writer.checkpoint_child(
                rejected_child,
                HistorySnapshot::new(vec![Message::user(REJECTED.into())]),
            )),
            Err(CheckpointError::Save { session_id, .. }) if session_id == id
        ));
        {
            let retained = lock(&writer.session);
            assert_eq!(
                retained.session.subagents(),
                &[child(CHILD_KEY), child(SIBLING_KEY)]
            );
            assert_eq!(retained.session.subagent_messages()[CHILD_KEY].len(), 2);
            assert_eq!(retained.session.posts().len(), 3);
            assert_eq!(retained.session.model, MODEL);
            assert_eq!(
                retained.session.messages()[0].first_text_content(),
                Some(ORIGINAL)
            );
            assert_eq!(retained.history_version, Some((history.epoch, 1)));
        }
        std::fs::remove_dir_all(&path).unwrap();
        smol::block_on(writer.checkpoint(request(id, 3, COMMITTED_MODEL, &options))).unwrap();

        let loaded: StoredSession = StoredSession::load(id, &dir).unwrap();
        assert_eq!(loaded.model, COMMITTED_MODEL);
        assert_eq!(loaded.subagents(), &[child(CHILD_KEY), child(SIBLING_KEY)]);
        assert_eq!(loaded.subagent_messages()[CHILD_KEY].len(), 2);
        assert_eq!(loaded.subagent_messages()[SIBLING_KEY].len(), 2);
        assert_eq!(loaded.posts().len(), 3);
        assert_eq!(
            serde_json::to_value(loaded.messages()).unwrap(),
            serde_json::to_value([original]).unwrap()
        );
    }

    #[test_case::test_case(false; "stale_owner")]
    #[test_case::test_case(true; "cancelled_before_claim")]
    fn stale_owner_cannot_publish_after_reclaim(cancelled: bool) {
        let tmp = TempDir::new().unwrap();
        let dir = StateDir::from_path(tmp.path().to_path_buf());
        let sessions = dir
            .ensure_subdir(maki_storage::sessions::SESSIONS_DIR)
            .unwrap();
        let id = MakiId::generate();
        let old = session_lock::claim(&sessions, &id).unwrap().unwrap();
        let writer =
            SessionLogCheckpoint::open(dir.clone(), id, MODEL, CWD, Some(old.publication_guard()))
                .unwrap();
        let owner = if cancelled {
            old
        } else {
            old.release().unwrap();
            session_lock::claim(&sessions, &id).unwrap().unwrap()
        };
        let options = SessionOptions::new(
            builtin_option_definitions(
                MODEL,
                [Arc::from(MODEL)],
                false,
                false,
                false,
                ThinkingConfig::Off,
            ),
            &BTreeMap::new(),
        )
        .unwrap();
        let publication = CheckpointPublication::default();
        if cancelled {
            assert!(publication.cancel());
        }
        let checkpoint = writer.checkpoint_guarded(
            request(id, 1, COMMITTED_MODEL, &options),
            publication.clone(),
            Arc::new(|_| panic!("stale publication claim ran")),
        );
        assert!(
            matches!(smol::block_on(checkpoint), Err(CheckpointError::Save { message, .. }) if message.as_ref() == CHECKPOINT_CANCELLED)
        );
        assert!(publication.is_cancelled());
        let child_publication = CheckpointPublication::default();
        if cancelled {
            assert!(child_publication.cancel());
        }
        assert!(matches!(
            smol::block_on(writer.checkpoint_child_config(
                child(CHILD_KEY),
                child_publication.clone(),
                Arc::new(|_| panic!("stale child configuration claim ran")),
            )),
            Err(CheckpointError::Save { message, .. }) if message.as_ref() == CHECKPOINT_CANCELLED
        ));
        assert!(child_publication.is_cancelled());
        assert_eq!(lock(&writer.session).session.model, MODEL);
        if !cancelled {
            assert!(matches!(
                smol::block_on(writer.checkpoint_child(
                    child(CHILD_KEY),
                    HistorySnapshot::new(vec![Message::user(REJECTED.into())]),
                )),
                Err(CheckpointError::Closed(session_id)) if session_id == id
            ));
        }
        assert!(lock(&writer.session).session.subagents().is_empty());
        assert!(matches!(
            StoredSession::load(id, &dir),
            Err(SessionError::Storage(StorageError::NotFound(_)))
        ));
        owner.release().unwrap();
    }

    #[test]
    fn checkpoint_errors_survive_unblock() {
        smol::block_on(async {
            let tmp = TempDir::new().unwrap();
            let state_path = tmp.path().join("not-a-directory");
            std::fs::write(&state_path, "file").unwrap();
            let dir = StateDir::from_path(state_path);
            let id = MakiId::generate();
            assert!(matches!(
                SessionLogCheckpoint::open(dir, id, MODEL, CWD, None),
                Err(CheckpointError::Save { session_id, .. }) if session_id == id
            ));
        });
    }

    #[test]
    fn corrupt_session_is_rejected_without_overwrite() {
        let tmp = TempDir::new().unwrap();
        let dir = StateDir::from_path(tmp.path().to_path_buf());
        let id = MakiId::generate();
        let mut session = StoredSession::new("test/model", "/project");
        session.id = id;
        session.save(&dir).unwrap();
        let sessions = dir
            .ensure_subdir(maki_storage::sessions::SESSIONS_DIR)
            .unwrap();
        let path = std::fs::read_dir(sessions)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .find(|path| {
                path.extension()
                    .is_some_and(|extension| extension == "jsonl")
            })
            .unwrap();
        const CORRUPT: &[u8] = b"not json\n";
        std::fs::write(&path, CORRUPT).unwrap();

        assert!(matches!(
            SessionLogCheckpoint::open(dir, id, MODEL, CWD, None),
            Err(CheckpointError::Save { session_id, .. }) if session_id == id
        ));
        assert_eq!(std::fs::read(path).unwrap(), CORRUPT);
    }

    #[test]
    fn projected_options_round_trip_session_storage() {
        smol::block_on(async {
            let tmp = TempDir::new().unwrap();
            let dir = StateDir::from_path(tmp.path().to_path_buf());
            let id = MakiId::generate();
            let options = SessionOptions::new(
                builtin_option_definitions(
                    "test/model",
                    [Arc::from("test/model")],
                    true,
                    true,
                    true,
                    ThinkingConfig::Effort(maki_providers::Effort::High),
                ),
                &BTreeMap::new(),
            )
            .unwrap();
            let writer = SessionLogCheckpoint::open(dir.clone(), id, MODEL, CWD, None).unwrap();

            writer
                .checkpoint(CheckpointRequest {
                    session_id: id,
                    version: CheckpointVersion {
                        revision: 1,
                        epoch: 1,
                    },
                    snapshot: Arc::new(SessionCheckpoint {
                        kind: SessionCheckpointKind::Config,
                        history_revision: Some(1),
                        history: Some(HistorySnapshot::new(vec![Message::user("hello".into())])),
                        options: options.snapshot(),
                        config: Some(projection(MODEL, CWD)),
                    }),
                })
                .await
                .unwrap();

            let loaded: StoredSession = StoredSession::load(id, &dir).unwrap();
            assert_eq!(loaded.meta.yolo, Some(true));
            assert!(loaded.meta.fast);
            assert!(loaded.meta.workflow);
            assert_eq!(
                loaded.meta.thinking,
                Some(maki_storage::sessions::StoredThinking::Effort {
                    level: maki_providers::Effort::High
                }),
                "thinking is projected into the field it has always been saved in"
            );
            assert_eq!(loaded.messages().len(), 1);
            assert_eq!(loaded.model, MODEL);
            assert_eq!(loaded.cwd, CWD);
            let acknowledged = lock(&writer.session).session.history_snapshot();
            let mut catalog = request(id, 2, COMMITTED_MODEL, &options);
            let candidate = Arc::make_mut(&mut catalog.snapshot);
            candidate.kind = SessionCheckpointKind::Options;
            candidate.config = Some(projection(COMMITTED_MODEL, COMMITTED_CWD));
            candidate.history = Some(HistorySnapshot::new(vec![Message::user(REJECTED.into())]));
            for option in Arc::make_mut(&mut candidate.options.options) {
                option.current_value = Arc::from(DISABLED_VALUE);
            }
            let mut entries = candidate.options.options.to_vec();
            let mut persistent = entries[0].clone();
            persistent.definition.id = Arc::from(CATALOG_ID);
            persistent.definition.owner = SessionOptionOwner::Plugin {
                plugin: Arc::from(CATALOG_PLUGIN),
                generation: 1,
            };
            persistent.definition.persistent = true;
            persistent.current_value = Arc::from(CATALOG_VALUE);
            entries.push(persistent);
            candidate.options.options = entries.into();
            writer.checkpoint(catalog).await.unwrap();
            let catalog = StoredSession::load(id, &dir).unwrap();
            assert_eq!(catalog.model, loaded.model);
            assert_eq!(catalog.cwd, loaded.cwd);
            assert_eq!(catalog.meta.fast, loaded.meta.fast);
            assert_eq!(catalog.meta.workflow, loaded.meta.workflow);
            assert_eq!(catalog.meta.thinking, loaded.meta.thinking);
            assert_eq!(catalog.meta.yolo, Some(false));
            assert_eq!(
                catalog.meta.session_options,
                BTreeMap::from([(CATALOG_ID.to_owned(), CATALOG_VALUE.to_owned())]),
                "only plugin-owned persistent options enter the stored map"
            );
            assert_eq!(
                serde_json::to_value(catalog.messages()).unwrap(),
                serde_json::to_value(loaded.messages()).unwrap()
            );
            let retained = lock(&writer.session);
            assert_eq!(
                retained.session.history_snapshot().epoch,
                acknowledged.epoch
            );
            assert!(Arc::ptr_eq(
                &retained.session.history_snapshot().messages,
                &acknowledged.messages
            ));
        });
    }
}
