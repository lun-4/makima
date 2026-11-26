use crate::{
    AgentId, AgentMode, ModeId, ModeRegistry, ToolOutput,
    agent::UNAVAILABLE_RESULT,
    journal::{DeliveryInboxUsage, SessionJournal, SessionJournalSnapshot, restored_preview_bytes},
    modes, plan_state,
    session_coordinator::{SessionCoordinatorError, SessionCoordinatorHandle},
    session_writer::{SessionWriter, WriterError},
};
use makima_config::ModelPolicy;
use makima_providers::{
    ContentBlock, Message, Model, ProviderRegistry, Role, TokenUsage, settle_session,
};
use makima_storage::{
    StateDir, StorageError,
    checkpoint::{CheckpointClaim, CheckpointError, CheckpointPublication},
    id::MakimaId,
    session_lock::{
        self, ClaimedSessionLock, LockHeartbeat, OPEN_ELSEWHERE_MSG, ResumeBlock,
        SessionPublicationGuard,
    },
    session_state::{
        StateChange, StoredCancellationReason, StoredDelivery, StoredDeliveryOp, StoredPayload,
        StoredStateRecord, StoredTurnFailure, StoredTurnFailureKind, StoredTurnOutcome,
        StoredTurnProvenance, StoredTurnResult, StoredTurnStatus,
    },
    sessions::{HistorySnapshot, SESSIONS_DIR, Session, SessionError, SessionRecord, next_epoch},
};
use std::{
    collections::{BTreeMap, BTreeSet},
    env, io,
    path::PathBuf,
    sync::Arc,
};

pub(crate) const CONTINUATION_DISABLED: &str =
    "continuation_disabled: MAKIMA_CONTINUE_ALL_AGENTS=0";
const CONTINUE_ALL_AGENTS_ENV: &str = "MAKIMA_CONTINUE_ALL_AGENTS";
const SEQUENCE_EXHAUSTED: &str = "session recovery exhausted the state sequence";
const RECOVERY_CANCELLED: &str = "session recovery publication cancelled";
const WRONG_WRITER: &str = "recovery writer belongs to another session";

type StoredSession = Session<Message, TokenUsage, ToolOutput>;

pub enum SessionSource {
    Existing(MakimaId),
    New(StoredSession),
    ExistingOrNew(StoredSession),
}

pub struct OpenModel<'a> {
    pub startup: &'a Model,
    pub explicit: bool,
    pub providers: &'a ProviderRegistry,
    pub policy: &'a ModelPolicy,
}

#[derive(Debug, thiserror::Error)]
pub enum OpenSessionError {
    #[error("session {0}: {OPEN_ELSEWHERE_MSG}")]
    Busy(MakimaId),
    #[error("session {0} already exists")]
    Exists(MakimaId),
    #[error("session {0}: lock ownership was lost")]
    LockLost(MakimaId),
    #[error(transparent)]
    ExistingBlocked(#[from] ResumeBlock),
    #[error("{0}")]
    Compose(String),
    #[error(transparent)]
    Coordinator(#[from] SessionCoordinatorError),
    #[error(transparent)]
    Load(#[from] SessionError),
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Lock(#[from] io::Error),
    #[error(transparent)]
    Writer(#[from] WriterError),
    #[error(transparent)]
    Recovery(#[from] CheckpointError),
}

pub struct SessionOpening {
    pub session: StoredSession,
    pub model: Model,
    pub resumed: bool,
    pub warning: Option<String>,
    lease: ClaimedSessionLock,
    directory: PathBuf,
}

pub fn open_session(
    storage: &StateDir,
    source: SessionSource,
    model: OpenModel<'_>,
    resume_cwd: Option<&str>,
) -> Result<SessionOpening, OpenSessionError> {
    let id = match &source {
        SessionSource::Existing(id) => *id,
        SessionSource::New(seed) | SessionSource::ExistingOrNew(seed) => seed.id,
    };
    let directory = storage.ensure_subdir(SESSIONS_DIR)?;
    let lease = session_lock::claim(&directory, &id)?.ok_or(OpenSessionError::Busy(id))?;
    let loaded = lease
        .publication_guard()
        .publish(|| StoredSession::load_from(id, &directory))?
        .ok_or(OpenSessionError::LockLost(id))?;
    let (mut session, resumed) = match (source, loaded) {
        (SessionSource::New(_), Ok(_)) => return Err(OpenSessionError::Exists(id)),
        (_, Ok(session)) => {
            if let Some(cwd) = resume_cwd
                && let Some(block) = session_lock::resume_block(&session.cwd, cwd, false)
            {
                return Err(block.into());
            }
            (session, true)
        }
        (
            SessionSource::New(seed) | SessionSource::ExistingOrNew(seed),
            Err(SessionError::Storage(StorageError::NotFound(_))),
        ) => (seed, false),
        (_, Err(error)) => return Err(error.into()),
    };
    let (selected, warning) = model.select(&session)?;
    if resumed
        && (session.token_usage != TokenUsage::default() || !session.usage_by_model().is_empty())
    {
        let view = session.latest_state()?;
        let fast = view
            .scopes
            .get(&session.root_agent_id())
            .and_then(|scope| scope.config.as_ref())
            .map_or(session.meta.fast, |config| config.fast);
        let tokens = session.token_usage;
        settle_session(
            &tokens,
            session.usage_by_model_mut(),
            &selected,
            fast,
            model.providers,
        );
    }
    Ok(SessionOpening {
        session,
        model: selected,
        resumed,
        warning,
        lease,
        directory,
    })
}

impl OpenModel<'_> {
    pub fn select(&self, session: &StoredSession) -> Result<(Model, Option<String>), SessionError> {
        let view = session.latest_state()?;
        if self.explicit {
            return Ok((self.startup.clone(), None));
        }
        let recorded = view
            .scopes
            .get(&session.root_agent_id())
            .and_then(|scope| scope.config.as_ref())
            .map_or(session.model.as_str(), |config| config.model.as_str());
        if recorded == self.startup.spec() && self.policy.allows(recorded) {
            return Ok((self.startup.clone(), None));
        }
        let reason = match self.providers.model(recorded) {
            Ok(model) if self.policy.allows(&model.spec()) => return Ok((model, None)),
            Ok(_) => "rejected by model policy".to_owned(),
            Err(error) => error.to_string(),
        };
        Ok((
            self.startup.clone(),
            Some(format!(
                "failed to restore model {recorded}: {reason}; using {}",
                self.startup.spec()
            )),
        ))
    }
}

impl SessionOpening {
    pub fn into_claimed(self) -> (StoredSession, ClaimedSessionLock) {
        (self.session, self.lease)
    }

    pub async fn recover(
        self,
        storage: &StateDir,
        modes: &ModeRegistry,
    ) -> Result<(Arc<SessionWriter>, Arc<LockHeartbeat>, RecoveredSession), OpenSessionError> {
        self.recover_with_continuation(storage, modes, continue_all_agents())
            .await
    }

    pub(crate) async fn recover_with_continuation(
        mut self,
        storage: &StateDir,
        modes: &ModeRegistry,
        continue_turns: bool,
    ) -> Result<(Arc<SessionWriter>, Arc<LockHeartbeat>, RecoveredSession), OpenSessionError> {
        let id = self.session.id;
        let root = self
            .session
            .root_agent_id()
            .unwrap_or_else(AgentId::generate);
        self.session.bind_root_id(root)?;
        if self.resumed
            && let Some(warning) = resolve_legacy_mode(&mut self.session, modes, storage)?
        {
            tracing::warn!(%warning, "legacy mode restoration fell back to read-only");
        }
        let writer = Arc::new(SessionWriter::open_leased(
            &self.directory,
            &self.session,
            &mut self.lease,
        )?);
        let lock = Arc::new(LockHeartbeat::spawn(self.lease, move || {
            if let Ok(coordinator) = SessionCoordinatorHandle::resolve(id) {
                coordinator.retire();
            }
        }));
        let candidate =
            RecoveryCandidate::prepare_with_continuation(self.session.clone(), continue_turns)?;
        let recovered = candidate
            .publish(
                &writer,
                CheckpointPublication::default(),
                Arc::new(|publication| {
                    publication
                        .try_publish()
                        .then_some(())
                        .ok_or_else(|| Arc::from(RECOVERY_CANCELLED))
                }),
            )
            .await?;
        Ok((writer, lock, recovered))
    }
}

pub fn continuation_notice(count: usize) -> Option<String> {
    (count != 0).then(|| {
        format!(
            "continuing {count} interrupted turns (set {CONTINUE_ALL_AGENTS_ENV}=0 to stop this)"
        )
    })
}

pub fn continue_all_agents() -> bool {
    env::var(CONTINUE_ALL_AGENTS_ENV).as_deref() != Ok("0")
}

pub struct RecoveryCandidate {
    session: StoredSession,
    journal: Arc<SessionJournal>,
    records: Vec<SessionRecord<Message, TokenUsage, ToolOutput>>,
}

pub struct RecoveredSession {
    session: StoredSession,
    journal: Arc<SessionJournal>,
}

pub fn resolve_legacy_mode(
    session: &mut StoredSession,
    registry: &ModeRegistry,
    state_dir: &StateDir,
) -> Result<Option<String>, CheckpointError> {
    let session_id = session.id;
    let failure = |message: String| CheckpointError::Save {
        session_id,
        message: message.into(),
    };
    let view = session
        .latest_state()
        .map_err(|error| failure(error.to_string()))?;
    if view
        .scopes
        .get(&session.root_agent_id())
        .is_some_and(|scope| scope.config.is_some())
    {
        return Ok(None);
    }
    let plans = plan_state::plan_entries(&view).map_err(failure)?;
    let mode = modes::saved(&session.meta).map(|(key, _)| key);
    let restored = if mode == Some(ModeId::Plan.key()) {
        let current = session
            .root_agent_id()
            .and_then(|root| plans.get(&root))
            .map_or(AgentMode::Build, |entry| {
                AgentMode::Plan(entry.path.clone())
            });
        modes::resolve(registry, ModeId::Plan.key(), &current, state_dir)
    } else {
        modes::restored(registry, &session.meta, state_dir)
    };
    let (mode, definition, warning) = match restored {
        Ok((mode, definition)) => (mode, definition, None),
        Err(error) => {
            let fallback = modes::read_only_fallback(error);
            (fallback.mode, fallback.definition, fallback.warning)
        }
    };
    let restored = modes::prepare_restore(&definition.stored_snapshot(&mode), registry)
        .map_err(|error| failure(error.to_string()))?;
    session
        .resolve_legacy_mode(restored.definition.stored_snapshot(&restored.mode))
        .map_err(|error| failure(error.to_string()))?;
    Ok(restored.warning.or(warning))
}

impl RecoveryCandidate {
    pub fn from_exact(
        session: StoredSession,
        snapshot: SessionJournalSnapshot,
    ) -> Result<Self, CheckpointError> {
        let journal = SessionJournal::from_exact_snapshot(&session, snapshot).map_err(|error| {
            CheckpointError::Save {
                session_id: session.id,
                message: Arc::from(error.to_string()),
            }
        })?;
        Ok(Self {
            session,
            journal: Arc::new(journal),
            records: Vec::new(),
        })
    }

    pub fn prepare(session: StoredSession) -> Result<Self, CheckpointError> {
        Self::prepare_with_continuation(session, continue_all_agents())
    }

    pub fn prepare_with_continuation(
        mut session: StoredSession,
        continue_turns: bool,
    ) -> Result<Self, CheckpointError> {
        let session_id = session.id;
        let failure = |message: String| CheckpointError::Save {
            session_id,
            message: Arc::from(message),
        };
        let mut records = Vec::new();
        let view = session
            .latest_state()
            .map_err(|error| failure(error.to_string()))?;
        let mut seq = session
            .state()
            .iter()
            .map(|record| record.seq)
            .chain(session.agent_revisions().iter().map(|(seq, _)| *seq))
            .chain(session.sequence_high_water())
            .max()
            .unwrap_or(0);
        let previous_epoch = session.history_snapshot().epoch;
        let covered = session.last_message_id();
        let history = if continue_turns {
            session.history_snapshot()
        } else {
            sanitize(session.history_snapshot())
        };
        session.adopt_history(&history);
        if history.epoch != previous_epoch
            && let Some(root) = session.root_agent_id()
        {
            records.push(SessionRecord::Compaction {
                agent: root,
                messages: history.messages.as_ref().clone(),
                ids: history.ids.as_ref().clone(),
                covered,
            });
            seq = seq
                .checked_add(1)
                .ok_or_else(|| failure(SEQUENCE_EXHAUSTED.into()))?;
            let record = StoredStateRecord {
                seq,
                agent: Some(root),
                generation: view
                    .scopes
                    .get(&Some(root))
                    .map_or(0, |scope| scope.generation),
                change: StateChange::Compaction {
                    epoch: history.epoch,
                },
            };
            session.push_state(record.clone());
            records.push(SessionRecord::State { d: record });
        }
        let keys: BTreeSet<_> = session
            .subagent_messages()
            .keys()
            .cloned()
            .chain(
                view.agents
                    .values()
                    .filter(|agent| !agent.transcript_key.is_empty())
                    .map(|agent| agent.transcript_key.clone()),
            )
            .collect();
        for key in keys {
            let history = session.subagent_history_snapshot(&key).unwrap_or_default();
            let previous_epoch = history.epoch;
            let covered = session.chat_message_ids(Some(&key)).last().copied();
            let history = if continue_turns {
                history
            } else {
                sanitize(history)
            };
            session.adopt_subagent_history(key.clone(), &history);
            if let Some(agent) = view
                .agents
                .values()
                .find(|agent| agent.transcript_key == key)
                && history.epoch != previous_epoch
            {
                records.push(SessionRecord::Compaction {
                    agent: agent.id,
                    messages: history.messages.as_ref().clone(),
                    ids: history.ids.as_ref().clone(),
                    covered,
                });
                seq = seq
                    .checked_add(1)
                    .ok_or_else(|| failure(SEQUENCE_EXHAUSTED.into()))?;
                let record = StoredStateRecord {
                    seq,
                    agent: Some(agent.id),
                    generation: view
                        .scopes
                        .get(&Some(agent.id))
                        .map_or(0, |scope| scope.generation),
                    change: StateChange::Compaction {
                        epoch: history.epoch,
                    },
                };
                session.push_state(record.clone());
                records.push(SessionRecord::State { d: record });
            }
        }
        // Queue failure reports before terminal results. Read historical queue records here
        // because a parent can consume its report before the child's turn is settled.
        let mut reported: BTreeMap<_, _> = session
            .state()
            .iter()
            .filter_map(|record| {
                let StateChange::Delivery {
                    id: (child, turn),
                    op: StoredDeliveryOp::Queued(delivery),
                } = &record.change
                else {
                    return None;
                };
                (record.agent == Some(view.agents.get(child)?.parent?)
                    && view.scopes.get(&Some(*child))?.admitted.contains_key(turn)
                    && !matches!(
                        delivery.result.outcome.status,
                        StoredTurnStatus::Completed { .. }
                    ))
                .then(|| ((*child, *turn), delivery.result.clone()))
            })
            .collect();
        let mut inbox_usage = BTreeMap::new();
        for (&agent, scope) in &view.scopes {
            let Some(agent) = agent else { continue };
            for (&turn, origin) in &scope.admitted {
                let result = if let Some(result) = reported.remove(&(agent, turn)) {
                    result
                } else if continue_turns || scope.holds_input(origin) {
                    continue;
                } else {
                    let child = view
                        .agents
                        .get(&agent)
                        .filter(|record| record.parent.is_some());
                    let status = if child.is_some() {
                        StoredTurnStatus::Failed {
                            failure: StoredTurnFailure {
                                kind: StoredTurnFailureKind::Internal,
                                diagnostic: CONTINUATION_DISABLED.into(),
                                user_message: CONTINUATION_DISABLED.into(),
                                retryable: false,
                            },
                        }
                    } else {
                        StoredTurnStatus::Cancelled {
                            reason: StoredCancellationReason::Interrupted,
                        }
                    };
                    let result = StoredTurnResult {
                        outcome: StoredTurnOutcome {
                            agent_id: agent,
                            turn_id: turn,
                            usage: Default::default(),
                            num_turns: 0,
                            cost: None,
                            list_cost: None,
                            context_size: 0,
                            context_window: 0,
                            status,
                        },
                        text: StoredPayload::Inline(String::new()),
                        output: StoredPayload::Inline(Vec::new()),
                        provenance: StoredTurnProvenance {
                            origin: origin.clone(),
                            plugin: None,
                            plugin_generation: None,
                            source_agent: None,
                            source_turn: None,
                        },
                        duration_ms: 0,
                    };
                    if let Some(child) = child
                        && let Some(parent) = child.parent
                        && child
                            .spec
                            .as_ref()
                            .is_some_and(|spec| spec.auto_deliver && !spec.silent)
                    {
                        let parent_scope = view.scopes.get(&Some(parent));
                        let usage = inbox_usage.entry(parent).or_insert_with(|| {
                            DeliveryInboxUsage::from_sizes(parent_scope.into_iter().flat_map(
                                |scope| {
                                    scope.deliveries.values().filter_map(|op| match op {
                                        StoredDeliveryOp::Queued(delivery) => {
                                            Some(restored_preview_bytes(delivery))
                                        }
                                        _ => None,
                                    })
                                },
                            ))
                        });
                        let mut report = StoredDelivery {
                            label: child.label.clone(),
                            result: result.clone(),
                            held: false,
                            preview_bytes: None,
                        };
                        let bytes = restored_preview_bytes(&report);
                        if usage.admit(bytes).is_ok() {
                            report.preview_bytes = Some(bytes);
                            seq = seq
                                .checked_add(1)
                                .ok_or_else(|| failure(SEQUENCE_EXHAUSTED.into()))?;
                            let delivery = StoredStateRecord {
                                seq,
                                agent: Some(parent),
                                generation: parent_scope.map_or(0, |scope| scope.generation),
                                change: StateChange::Delivery {
                                    id: (agent, turn),
                                    op: StoredDeliveryOp::Queued(Box::new(report)),
                                },
                            };
                            session.push_state(delivery.clone());
                            records.push(SessionRecord::State { d: delivery });
                        }
                    }
                    result
                };
                seq = seq
                    .checked_add(1)
                    .ok_or_else(|| failure(SEQUENCE_EXHAUSTED.into()))?;
                let record = StoredStateRecord {
                    seq,
                    agent: Some(agent),
                    generation: scope.generation,
                    change: StateChange::Turn { turn, result },
                };
                session.push_state(record.clone());
                records.push(SessionRecord::State { d: record });
            }
        }
        let journal = Arc::new(
            SessionJournal::from_session(&session).map_err(|error| failure(error.to_string()))?,
        );
        Ok(Self {
            session,
            journal,
            records,
        })
    }

    pub async fn publish(
        self,
        writer: &SessionWriter,
        publication: CheckpointPublication,
        claim: CheckpointClaim,
    ) -> Result<RecoveredSession, CheckpointError> {
        let session_id = self.session.id;
        if writer.session_id() != session_id {
            return Err(CheckpointError::Save {
                session_id,
                message: Arc::from(WRONG_WRITER),
            });
        }
        claim(&publication).map_err(|message| CheckpointError::Save {
            session_id,
            message,
        })?;
        self.journal
            .bind_writer(writer.clone())
            .map_err(|error| CheckpointError::Save {
                session_id,
                message: error.to_string().into(),
            })?;
        writer
            .append(self.records)
            .await
            .map_err(|error| CheckpointError::Save {
                session_id,
                message: Arc::from(error.to_string()),
            })?;
        writer.attach_journal(&self.journal);
        Ok(RecoveredSession {
            session: self.session,
            journal: self.journal,
        })
    }

    pub(crate) fn journal(&self) -> Arc<SessionJournal> {
        Arc::clone(&self.journal)
    }

    pub(crate) async fn publish_staged(
        mut self,
        writer: &SessionWriter,
        guard: SessionPublicationGuard,
    ) -> Result<(), OpenSessionError> {
        if writer.session_id() != self.session.id {
            return Err(OpenSessionError::Compose(WRONG_WRITER.into()));
        }
        let snapshot = self.journal.projection();
        self.records.extend(self.journal.take_unwritten());
        writer.append_recovery(self.records, guard).await?;
        self.journal
            .acknowledge(&snapshot)
            .and_then(|()| self.journal.bind_writer(writer.clone()))
            .map_err(|error| OpenSessionError::Compose(error.to_string()))?;
        writer.attach_journal(&self.journal);
        Ok(())
    }

    pub fn session(&self) -> &StoredSession {
        &self.session
    }

    pub fn finalize_after_publication(self) -> RecoveredSession {
        RecoveredSession {
            session: self.session,
            journal: self.journal,
        }
    }
}

impl SessionWriter {
    pub async fn recover(
        &self,
        session: StoredSession,
        publication: CheckpointPublication,
        claim: CheckpointClaim,
    ) -> Result<RecoveredSession, CheckpointError> {
        RecoveryCandidate::prepare(session)?
            .publish(self, publication, claim)
            .await
    }
}

impl RecoveredSession {
    pub fn session(&self) -> &StoredSession {
        &self.session
    }

    pub fn journal(&self) -> Arc<SessionJournal> {
        Arc::clone(&self.journal)
    }

    pub fn into_parts(self) -> (StoredSession, Arc<SessionJournal>) {
        (self.session, self.journal)
    }
}

pub(crate) fn sanitize(mut history: HistorySnapshot<Message>) -> HistorySnapshot<Message> {
    let mut messages: Vec<Message> = Vec::with_capacity(history.messages.len());
    let mut ids = Vec::with_capacity(history.messages.len());
    let mut calls: Vec<(&str, bool)> = Vec::new();
    for (index, original) in history.messages.iter().enumerate() {
        let mut message = original.clone();
        let (mut had_results, mut kept_results) = (false, false);
        if matches!(message.role, Role::User) {
            message.content.retain(|block| match block {
                ContentBlock::ToolResult { tool_use_id, .. } => {
                    had_results = true;
                    let mut matched = false;
                    for (id, seen) in &mut calls {
                        if *id == tool_use_id {
                            *seen = true;
                            matched = true;
                        }
                    }
                    kept_results |= matched;
                    matched
                }
                _ => true,
            });
            if had_results && !kept_results {
                message
                    .content
                    .retain(|block| !matches!(block, ContentBlock::Image { .. }));
                if message.content.is_empty() {
                    continue;
                }
            }
        }
        if !kept_results {
            if had_results
                && messages
                    .last()
                    .is_some_and(|last| last.role == Role::Assistant)
            {
                message.content.extend(missing_results(&mut calls));
            } else {
                complete_tool_results(&mut messages, &mut ids, &mut calls);
            }
        }
        if matches!(message.role, Role::Assistant) {
            calls.extend(original.tool_uses().map(|(id, _, _)| (id, false)));
        }
        if !message.content.is_empty() {
            messages.push(message);
            ids.push(history.ids[index]);
        }
    }
    complete_tool_results(&mut messages, &mut ids, &mut calls);
    if history.messages.as_ref() != &messages {
        history.epoch = next_epoch();
        history.covered = history.ids.last().copied();
        history.ids = Arc::new(ids);
        history.messages = Arc::new(messages);
    }
    history
}

fn unavailable(tool_use_id: String) -> ContentBlock {
    ContentBlock::ToolResult {
        tool_use_id,
        content: UNAVAILABLE_RESULT.to_owned(),
        is_error: true,
    }
}

fn missing_results<'a>(
    calls: &'a mut Vec<(&str, bool)>,
) -> impl Iterator<Item = ContentBlock> + 'a {
    calls
        .drain(..)
        .filter(|(_, seen)| !seen)
        .map(|(id, _)| unavailable(id.to_owned()))
}

fn complete_tool_results(
    messages: &mut Vec<Message>,
    ids: &mut Vec<MakimaId>,
    calls: &mut Vec<(&str, bool)>,
) {
    if calls.is_empty() {
        return;
    }
    let missing = missing_results(calls);
    if let Some(last) = messages.last_mut().filter(|last| last.role == Role::User) {
        last.content.extend(missing);
    } else {
        messages.push(Message {
            role: Role::User,
            content: missing.collect(),
            display_text: Some(String::new()),
            ..Default::default()
        });
        ids.push(MakimaId::generate());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        AgentLimits, AgentManagerHandle,
        journal::{JournalChange, JournalCommit},
    };
    use makima_storage::{
        StateDir,
        checkpoint::{CheckpointClaim, CheckpointPublication},
        id::{AgentId, TurnId},
        plans::{PLAN_STATE_KEY, PLAN_STATE_VERSION, PlanEntry, PlanSlot},
        session_state::StoredTurnOrigin,
        sessions::SESSIONS_DIR,
    };
    use serde_json::json;
    use tempfile::TempDir;
    use test_case::test_case;

    const MODEL: &str = "test/model";
    const CWD: &str = "/tmp";
    const CHILD_KEY: &str = "child";
    const TOOL: &str = "tool";
    const ORPHAN: &str = "orphan";
    const CLAIM_REJECTED: &str = "recovery claim rejected";
    const DRAFT: &str = "inactive draft";

    const STARTUP_MODEL: &str = "startup/default";
    const UNAVAILABLE_MODEL: &str = "unavailable/model";
    const POLICY_REJECTION: &str = "rejected by model policy";

    #[test_case(MODEL, false, false, MODEL, None; "recorded")]
    #[test_case(MODEL, true, false, STARTUP_MODEL, None; "explicit")]
    #[test_case(MODEL, false, true, STARTUP_MODEL, Some(POLICY_REJECTION); "policy_fallback")]
    #[test_case(UNAVAILABLE_MODEL, false, false, STARTUP_MODEL, Some(UNAVAILABLE_MODEL); "unavailable_fallback")]
    fn open_session_selects_authoritative_model(
        recorded: &str,
        explicit: bool,
        excluded: bool,
        expected: &str,
        warning: Option<&str>,
    ) {
        let tmp = TempDir::new().unwrap();
        let storage = StateDir::from_path(tmp.path().to_path_buf());
        let startup = Model::from_spec(STARTUP_MODEL).unwrap();
        let providers = makima_providers::test_util::offline_ids(&["test", "startup"]);
        let excluded = if excluded {
            vec![recorded.to_owned()]
        } else {
            Vec::new()
        };
        let policy = ModelPolicy::new(&[], &excluded).unwrap();
        let mut session = StoredSession::new(STARTUP_MODEL, CWD);
        let root = AgentId::generate();
        session.bind_root_id(root).unwrap();
        session
            .resolve_legacy_config(
                crate::session_checkpoint::stored_agent_config(
                    &crate::EffectiveAgentConfig::new(
                        crate::RunSettings {
                            provider: makima_providers::provider::unconfigured(),
                            model: Model::from_spec(recorded).unwrap(),
                            thinking: Default::default(),
                            fast: false,
                            workflow: false,
                        },
                        AgentMode::Build,
                    )
                    .with_mode_def(Some(ModeRegistry::builtin().current(&AgentMode::Build)))
                    .with_context(crate::AgentContextSnapshot::new(CWD.into())),
                    Default::default(),
                )
                .unwrap(),
            )
            .unwrap();
        session.save(&storage).unwrap();
        let id = session.id;
        let open = || {
            open_session(
                &storage,
                SessionSource::Existing(id),
                OpenModel {
                    startup: &startup,
                    explicit,
                    providers: &providers,
                    policy: &policy,
                },
                Some(CWD),
            )
        };
        let opening = open().unwrap();
        assert_eq!(opening.model.spec(), expected);
        assert_eq!(opening.warning.is_some(), warning.is_some());
        if let Some(warning) = warning {
            assert!(opening.warning.as_deref().unwrap().contains(warning));
        }
        assert!(matches!(open(), Err(OpenSessionError::Busy(busy)) if busy == id));
        let (_writer, lease, recovered) =
            smol::block_on(opening.recover(&storage, &ModeRegistry::builtin())).unwrap();
        assert_eq!(recovered.session().root_agent_id(), Some(root));
        assert!(recovered.journal().has_writer());
        drop(lease);
        assert!(open().is_ok());
    }

    #[test_case(false; "exact_candidate_preserves_history_coordinates")]
    #[test_case(true; "mismatched_candidate_is_rejected")]
    fn exact_candidate_does_not_sanitize_or_remint(mismatch: bool) {
        let mut session = StoredSession::new(MODEL, CWD);
        let root = AgentId::generate();
        session.bind_root_id(root).unwrap();
        session.push_message(Message::user(DRAFT.into()));
        let journal = SessionJournal::from_session(&session).unwrap();
        let mut snapshot = journal.snapshot();
        snapshot.revision = 17;
        snapshot.histories.get_mut(&root).unwrap().epoch = 73;
        snapshot.managed_histories.insert(root);
        if mismatch {
            snapshot.histories.get_mut(&root).unwrap().messages = Arc::new(Vec::new());
        }
        let expected = snapshot.clone();
        let result = RecoveryCandidate::from_exact(session, snapshot);
        if mismatch {
            assert!(result.is_err());
        } else {
            let recovered = result.unwrap().finalize_after_publication();
            let actual = recovered.journal().snapshot();
            assert_eq!(actual.epoch, expected.epoch);
            assert_eq!(actual.seq, expected.seq);
            assert_eq!(actual.revision, expected.revision);
            assert_eq!(actual.state, expected.state);
            assert_eq!(
                actual.histories[&root].epoch,
                expected.histories[&root].epoch
            );
            assert!(Arc::ptr_eq(
                &actual.histories[&root].messages,
                &expected.histories[&root].messages
            ));
        }
    }

    #[test]
    fn sanitization_repairs_missing_results_once() {
        let mut session = StoredSession::new(MODEL, CWD);
        let root = AgentId::generate();
        session.bind_root_id(root).unwrap();
        session.push_message(Message {
            role: Role::Assistant,
            content: vec![ContentBlock::tool_use(TOOL, TOOL, json!({}))],
            ..Default::default()
        });
        let original_epoch = session.history_snapshot().epoch;
        let candidate = RecoveryCandidate::prepare_with_continuation(session, false).unwrap();
        let history = candidate.session.history_snapshot();
        assert_eq!(history.messages.len(), 2);
        assert_ne!(history.epoch, original_epoch);
        let again = RecoveryCandidate::prepare_with_continuation(candidate.session, false).unwrap();
        assert_eq!(again.session.history_snapshot().epoch, history.epoch);
        assert_eq!(again.session.messages(), history.messages.as_ref());
    }

    #[test_case(false; "inserted_result_gets_a_new_id")]
    #[test_case(true; "repaired_result_keeps_its_id")]
    fn sanitization_preserves_surviving_message_ids(repair_existing: bool) {
        let mut messages = vec![
            Message {
                role: Role::User,
                content: vec![unavailable(ORPHAN.into())],
                ..Default::default()
            },
            Message {
                role: Role::Assistant,
                content: vec![ContentBlock::tool_use(TOOL, TOOL, json!({}))],
                ..Default::default()
            },
        ];
        if repair_existing {
            messages.push(Message {
                role: Role::User,
                content: vec![
                    unavailable(ORPHAN.into()),
                    ContentBlock::Text { text: DRAFT.into() },
                ],
                ..Default::default()
            });
        } else {
            messages.push(Message::user(DRAFT.into()));
        }
        let original = HistorySnapshot::new(messages);
        let history = sanitize(original.clone());
        assert_ne!(history.epoch, original.epoch);
        assert_eq!(history.covered, original.ids.last().copied());
        assert_eq!(history.ids.len(), history.messages.len());
        assert_eq!(history.ids[0], original.ids[1]);
        if repair_existing {
            assert_eq!(history.ids[1], original.ids[2]);
        } else {
            assert!(!original.ids.contains(&history.ids[1]));
            assert_eq!(history.ids[2], original.ids[2]);
        }
        let again = sanitize(history.clone());
        assert_eq!(again.epoch, history.epoch);
        assert!(Arc::ptr_eq(&again.ids, &history.ids));
        assert!(Arc::ptr_eq(&again.messages, &history.messages));
    }

    #[test_case(false; "completed_batch_is_unchanged")]
    #[test_case(true; "partial_batch_only_fills_missing_result")]
    fn disabled_continuation_preserves_split_results(partial: bool) {
        const FIRST_CALL: &str = "first";
        const SECOND_CALL: &str = "second";
        const MISSING_CALL: &str = "missing";

        let mut session = StoredSession::new(MODEL, CWD);
        session.bind_root_id(AgentId::generate()).unwrap();
        let mut calls = vec![FIRST_CALL, SECOND_CALL];
        if partial {
            calls.push(MISSING_CALL);
        }
        session.push_message(Message {
            role: Role::Assistant,
            content: calls
                .into_iter()
                .map(|id| ContentBlock::tool_use(id, TOOL, json!({})))
                .collect(),
            ..Default::default()
        });
        for id in [FIRST_CALL, SECOND_CALL] {
            session.push_message(Message {
                role: Role::User,
                content: vec![ContentBlock::ToolResult {
                    tool_use_id: id.into(),
                    content: id.into(),
                    is_error: false,
                }],
                ..Default::default()
            });
        }
        session.push_message(Message::user(DRAFT.into()));
        let original = session.history_snapshot();
        let candidate = RecoveryCandidate::prepare_with_continuation(session, false).unwrap();
        let recovered = candidate.session.history_snapshot();
        let mut expected = original.messages.as_ref().clone();
        if partial {
            expected[2].content.push(unavailable(MISSING_CALL.into()));
        } else {
            assert_eq!(recovered.epoch, original.epoch);
        }
        assert_eq!(recovered.messages.as_ref(), &expected);
        assert_eq!(recovered.ids, original.ids);
    }

    #[test]
    fn interrupted_admission_keeps_policy_generation_after_history_sanitization() {
        const GENERATION: u64 = 37;
        let mut session = StoredSession::new(MODEL, CWD);
        let root = AgentId::generate();
        let turn = TurnId::generate();
        session.bind_root_id(root).unwrap();
        session.push_message(Message {
            role: Role::Assistant,
            content: vec![ContentBlock::tool_use(TOOL, TOOL, json!({}))],
            ..Default::default()
        });
        let epoch = session.history_snapshot().epoch;
        session.push_state(StoredStateRecord {
            seq: session.next_state_seq(),
            agent: Some(root),
            generation: GENERATION,
            change: StateChange::Admitted {
                turn,
                origin: StoredTurnOrigin::User,
            },
        });
        let candidate = RecoveryCandidate::prepare_with_continuation(session, false).unwrap();
        assert_ne!(candidate.session.history_snapshot().epoch, epoch);
        let terminal = candidate.session.state().iter().find(|record| {
            matches!(&record.change, StateChange::Turn { turn: id, .. } if *id == turn)
        }).unwrap();
        assert_eq!(terminal.generation, GENERATION);
        assert_eq!(
            candidate.session.latest_state().unwrap().scopes[&Some(root)].generation,
            GENERATION
        );
    }

    #[test]
    fn legacy_plan_config_uses_the_migrated_slot_path() {
        let directory = TempDir::new().unwrap();
        let storage = StateDir::from_path(directory.path().to_path_buf());
        let path = directory.path().join(CHILD_KEY);
        std::fs::write(&path, DRAFT).unwrap();
        let mut session = StoredSession::new(MODEL, CWD);
        let root = AgentId::generate();
        session.bind_root_id(root).unwrap();
        session.meta.mode = Some(makima_storage::sessions::StoredMode::Plan);
        let plans: PlanSlot = [(
            root,
            PlanEntry {
                path: path.clone(),
                written: true,
                approved: None,
                staged_model: None,
            },
        )]
        .into();
        session.push_state(StoredStateRecord {
            seq: session.agent_revisions().last().unwrap().0 + 1,
            agent: None,
            generation: 0,
            change: StateChange::Plugin {
                key: PLAN_STATE_KEY.into(),
                version: PLAN_STATE_VERSION,
                value: Some(serde_json::to_value(plans).unwrap()),
            },
        });
        assert!(
            resolve_legacy_mode(&mut session, &ModeRegistry::builtin(), &storage)
                .unwrap()
                .is_none()
        );
        let view = session.latest_state().unwrap();
        assert_eq!(
            view.scopes[&Some(root)]
                .config
                .as_ref()
                .unwrap()
                .mode
                .restrict_write_to
                .as_ref(),
            Some(&path)
        );
        assert!(plan_state::plan_entries(&view).unwrap()[&root].written);
        assert_eq!(std::fs::read_to_string(path).unwrap(), DRAFT);
    }

    #[test_case(false; "known_mode")]
    #[test_case(true; "removed_mode_falls_back_once")]
    fn legacy_mode_resolution_is_once_and_preserves_existing_config(removed: bool) {
        const REMOVED_MODE: &str = "removed_mode";
        const LEGACY_OPTION: &str = "legacy.option";
        let directory = TempDir::new().unwrap();
        let storage = StateDir::from_path(directory.path().to_path_buf());
        let registry = ModeRegistry::builtin();
        let mut session = StoredSession::new(MODEL, CWD);
        session.meta.fast = true;
        session.meta.workflow = true;
        session.meta.thinking = Some(makima_storage::sessions::StoredThinking::Off);
        session
            .meta
            .session_options
            .insert(LEGACY_OPTION.into(), DRAFT.into());
        if removed {
            session.meta.mode = Some(makima_storage::sessions::StoredMode::Custom(
                REMOVED_MODE.into(),
            ));
        }
        let warning = resolve_legacy_mode(&mut session, &registry, &storage).unwrap();
        assert_eq!(warning.is_some(), removed);
        let before = session.state().to_vec();
        let state = session.latest_state().unwrap();
        let config = state.scopes[&session.root_agent_id()]
            .config
            .as_ref()
            .unwrap();
        assert_eq!(config.model, MODEL);
        assert_eq!(config.cwd, CWD);
        assert!(config.fast && config.workflow);
        assert_eq!(config.options[LEGACY_OPTION], DRAFT);
        if removed {
            assert!(config.mode.tools.as_ref().unwrap().is_empty());
            assert!(modes::is_read_only_fallback(
                &modes::from_snapshot(&config.mode).unwrap().0
            ));
        } else {
            assert_eq!(config.mode.id, AgentMode::Build.id().key());
        }
        registry
            .define(crate::ModeDefSpec {
                name: AgentMode::Build.id().key().into(),
                system_prompt: Some(REMOVED_MODE.into()),
                ..Default::default()
            })
            .unwrap();
        assert!(
            resolve_legacy_mode(&mut session, &registry, &storage)
                .unwrap()
                .is_none()
        );
        assert_eq!(session.state(), before);
    }

    fn fixture() -> (StoredSession, AgentId, AgentId, TurnId, TurnId) {
        let mut session = StoredSession::new(MODEL, CWD);
        let root = AgentId::generate();
        let child = AgentId::generate();
        session.bind_root_id(root).unwrap();
        let mut record = session.agents()[&root].clone();
        record.id = child;
        record.parent = Some(root);
        record.depth = 1;
        record.transcript_key = CHILD_KEY.into();
        record.created_seq = 2;
        record.updated_seq = 2;
        session.push_agent_revision(2, record);
        let calls = Message {
            role: Role::Assistant,
            content: vec![ContentBlock::ToolUse {
                id: TOOL.into(),
                name: TOOL.into(),
                input: json!({}),
                thought_signature: None,
            }],
            ..Default::default()
        };
        let orphan = Message {
            role: Role::User,
            content: vec![unavailable(ORPHAN.into())],
            ..Default::default()
        };
        session.adopt_history(&HistorySnapshot::new(vec![
            orphan.clone(),
            calls.clone(),
            Message::user(DRAFT.into()),
        ]));
        session
            .adopt_subagent_history(CHILD_KEY.into(), &HistorySnapshot::new(vec![orphan, calls]));
        let root_turn = TurnId::generate();
        let child_turn = TurnId::generate();
        for (seq, agent, turn, origin) in [
            (4, root, root_turn, StoredTurnOrigin::User),
            (5, child, child_turn, StoredTurnOrigin::Internal),
        ] {
            session.push_state(StoredStateRecord {
                seq,
                agent: Some(agent),
                generation: 0,
                change: StateChange::Admitted { turn, origin },
            });
        }
        session.meta.input_draft = Some(DRAFT.into());
        (session, root, child, root_turn, child_turn)
    }

    fn claim() -> CheckpointClaim {
        Arc::new(|publication| {
            publication
                .try_publish()
                .then_some(())
                .ok_or_else(|| Arc::from(CLAIM_REJECTED))
        })
    }

    #[test]
    fn orphan_result_before_actual_result_preserves_root_and_child_results() {
        let (mut session, _, _, _, _) = fixture();
        let calls = Message {
            role: Role::Assistant,
            content: vec![ContentBlock::ToolUse {
                id: TOOL.into(),
                name: TOOL.into(),
                input: json!({}),
                thought_signature: None,
            }],
            ..Default::default()
        };
        let orphan = Message {
            role: Role::User,
            content: vec![unavailable(ORPHAN.into())],
            ..Default::default()
        };
        let actual = Message {
            role: Role::User,
            content: vec![ContentBlock::ToolResult {
                tool_use_id: TOOL.into(),
                content: DRAFT.into(),
                is_error: false,
            }],
            ..Default::default()
        };
        let original = vec![calls, orphan, actual];
        let expected = crate::agent::History::restored(original.clone()).into_vec();
        let mut history = session.history_snapshot();
        history.ids = Arc::new(original.iter().map(|_| MakimaId::generate()).collect());
        history.messages = Arc::new(original.clone());
        session.adopt_history(&history);
        let mut history = session.subagent_history_snapshot(CHILD_KEY).unwrap();
        history.ids = Arc::new(original.iter().map(|_| MakimaId::generate()).collect());
        history.messages = Arc::new(original);
        session.adopt_subagent_history(CHILD_KEY.into(), &history);
        let candidate = RecoveryCandidate::prepare_with_continuation(session, false).unwrap();
        assert_eq!(candidate.session().messages(), &expected);
        assert_eq!(
            candidate.session().subagent_messages()[CHILD_KEY].as_ref(),
            &expected
        );
    }

    #[test_case(false; "stop_unfinished_turns")]
    #[test_case(true; "continue_unfinished_turns")]
    fn restart_policy_preserves_admissions_and_inflight_calls(continue_turns: bool) {
        let (session, root, child, root_turn, child_turn) = fixture();
        let original = session.history_snapshot();
        let candidate =
            RecoveryCandidate::prepare_with_continuation(session, continue_turns).unwrap();
        let view = candidate.session().latest_state().unwrap();
        assert_eq!(candidate.session().meta.input_draft.as_deref(), Some(DRAFT));
        for (agent, turn) in [(root, root_turn), (child, child_turn)] {
            let scope = &view.scopes[&Some(agent)];
            assert_eq!(scope.admitted.contains_key(&turn), continue_turns);
            assert_eq!(
                scope.results.iter().any(|(id, _)| *id == turn),
                !continue_turns
            );
        }
        if continue_turns {
            let restored = candidate.session().history_snapshot();
            assert_eq!(restored.messages, original.messages);
            assert_eq!(restored.epoch, original.epoch);
            assert_eq!(restored.ids, original.ids);
            assert!(candidate.records.is_empty());
        }
    }

    #[test_case(false; "continuation_disabled")]
    #[test_case(true; "continuation_enabled")]
    fn recovery_preserves_held_user_admission(continue_turns: bool) {
        let (mut session, root, child, root_turn, child_turn) = fixture();
        for agent in [root, child] {
            session.push_state(StoredStateRecord {
                seq: session.next_state_seq(),
                agent: Some(agent),
                generation: 0,
                change: StateChange::InputHold { held: true },
            });
        }
        let candidate =
            RecoveryCandidate::prepare_with_continuation(session, continue_turns).unwrap();
        let view = candidate.session().latest_state().unwrap();
        let root_scope = &view.scopes[&Some(root)];
        assert!(root_scope.holds_input(&StoredTurnOrigin::User));
        assert_eq!(
            root_scope.admitted.get(&root_turn),
            Some(&StoredTurnOrigin::User)
        );
        assert!(
            !root_scope
                .results
                .iter()
                .any(|(turn, _)| *turn == root_turn)
        );
        assert_eq!(candidate.session().meta.input_draft.as_deref(), Some(DRAFT));

        let child_scope = &view.scopes[&Some(child)];
        assert_eq!(
            child_scope.admitted.contains_key(&child_turn),
            continue_turns
        );
        assert_eq!(
            child_scope
                .results
                .iter()
                .any(|(turn, _)| *turn == child_turn),
            !continue_turns
        );
    }

    #[test]
    fn recovery_terminalization_is_durable_and_idempotent() {
        smol::block_on(async {
            let directory = TempDir::new().unwrap();
            let storage = StateDir::from_path(directory.path().to_path_buf());
            let (mut session, root, child, root_turn, child_turn) = fixture();
            let id = session.id;
            session.save(&storage).unwrap();
            let original_state = session.state().to_vec();
            let sessions = storage.ensure_subdir(SESSIONS_DIR).unwrap();
            let path = sessions.join(format!("{id}.jsonl"));
            let original = std::fs::read(&path).unwrap();
            let loaded = StoredSession::load(id, &storage).unwrap();
            let writer = SessionWriter::open_session(&sessions, &loaded).unwrap();
            let recovered = RecoveryCandidate::prepare_with_continuation(loaded, false)
                .unwrap()
                .publish(&writer, CheckpointPublication::default(), claim())
                .await
                .unwrap();
            assert!(std::fs::read(&path).unwrap().starts_with(&original));
            assert_eq!(
                &recovered.session().state()[..original_state.len()],
                &original_state
            );
            let stored = StoredSession::load(id, &storage).unwrap();
            for chat in [None, Some(CHILD_KEY)] {
                assert_eq!(
                    stored.chat_message_ids(chat),
                    recovered.session().chat_message_ids(chat)
                );
            }
            let view = recovered.session().latest_state().unwrap();
            for (agent, turn) in [(root, root_turn), (child, child_turn)] {
                let scope = &view.scopes[&Some(agent)];
                assert!(scope.admitted.is_empty());
                let result = scope.results.iter().find(|(id, _)| *id == turn).unwrap();
                if agent == root {
                    assert_eq!(
                        result.1.outcome.status,
                        StoredTurnStatus::Cancelled {
                            reason: StoredCancellationReason::Interrupted
                        }
                    );
                } else {
                    assert!(matches!(&result.1.outcome.status,
                        StoredTurnStatus::Failed { failure } if failure.diagnostic == CONTINUATION_DISABLED));
                }
                assert!(scope.deliveries.is_empty());
            }
            assert_eq!(recovered.session().messages().len(), 3);
            assert_eq!(recovered.session().subagent_messages()[CHILD_KEY].len(), 2);
            let journal = recovered.journal();
            let snapshot = journal.snapshot();
            let manager = AgentManagerHandle::restore_graph(
                AgentLimits::default(),
                journal,
                view,
                snapshot.histories,
            )
            .unwrap();
            assert!(manager.lookup(root).unwrap().actor().is_err());
            for agent in [root, child] {
                assert!(
                    recovered.session().state()[original_state.len()..]
                        .iter()
                        .any(|record| {
                            record.agent == Some(agent)
                                && matches!(record.change, StateChange::Compaction { .. })
                        })
                );
            }
            let durable_bytes = std::fs::read(&path).unwrap();
            let durable_scopes = recovered.session().latest_state().unwrap().scopes;
            let durable_state = recovered.session().state().to_vec();
            drop(writer);
            let loaded = StoredSession::load(id, &storage).unwrap();
            let writer = SessionWriter::open_session(&sessions, &loaded).unwrap();
            let again = RecoveryCandidate::prepare_with_continuation(loaded, false)
                .unwrap()
                .publish(&writer, CheckpointPublication::default(), claim())
                .await
                .unwrap();
            assert_eq!(std::fs::read(&path).unwrap(), durable_bytes);
            let after = again.session().latest_state().unwrap();
            assert_eq!(durable_state.len(), again.session().state().len());
            assert_eq!(durable_scopes, after.scopes);
            assert_eq!(again.session().meta, recovered.session().meta);
            assert_eq!(again.session().messages(), recovered.session().messages());
            assert_eq!(
                again.session().subagent_messages(),
                recovered.session().subagent_messages()
            );
        });
    }

    #[test]
    fn recovery_rejects_a_writer_for_another_session() {
        smol::block_on(async {
            let directory = TempDir::new().unwrap();
            let writer_session = StoredSession::new(MODEL, CWD);
            let writer = SessionWriter::open_session(directory.path(), &writer_session).unwrap();
            let (session, _, _, _, _) = fixture();
            let result = writer
                .recover(
                    session,
                    CheckpointPublication::default(),
                    Arc::new(|_| unreachable!()),
                )
                .await;
            assert!(
                matches!(result, Err(CheckpointError::Save { message, .. }) if message.as_ref() == WRONG_WRITER)
            );
        });
    }

    #[test_case(false; "ownership_lost")]
    #[test_case(true; "published")]
    fn staged_recovery_acknowledges_and_attaches_only_on_success(owned: bool) {
        smol::block_on(async {
            const KEY: &str = "staged-recovery-test";
            let directory = TempDir::new().unwrap();
            let session = StoredSession::new(MODEL, CWD);
            let writer = SessionWriter::open_session(directory.path(), &session).unwrap();
            let previous = Arc::new(SessionJournal::from_session(&session).unwrap());
            writer.attach_journal(&previous);
            let lease = session_lock::claim(directory.path(), &session.id)
                .unwrap()
                .unwrap();
            let guard = lease.publication_guard();
            let candidate = RecoveryCandidate::prepare_with_continuation(session, false).unwrap();
            let journal = candidate.journal();
            let revision = journal
                .commit(JournalCommit {
                    changes: vec![JournalChange {
                        agent: None,
                        generation: 0,
                        change: StateChange::Plugin {
                            key: KEY.into(),
                            version: 1,
                            value: Some(json!(true)),
                        },
                    }],
                    ..JournalCommit::default()
                })
                .unwrap();
            let before = journal.published_revision();
            assert!(revision > before);
            if !owned {
                lease.release().unwrap();
            }
            assert_eq!(
                candidate.publish_staged(&writer, guard).await.is_ok(),
                owned
            );
            assert_eq!(
                journal.published_revision(),
                if owned { revision } else { before }
            );
            assert!(Arc::ptr_eq(
                writer.journals().as_slice().first().unwrap(),
                if owned { &journal } else { &previous },
            ));
            writer.close().await.unwrap();
        });
    }

    #[test]
    fn rejected_recovery_does_not_publish_or_expose_results() {
        smol::block_on(async {
            let directory = TempDir::new().unwrap();
            let storage = StateDir::from_path(directory.path().to_path_buf());
            let (mut session, root, _, _, _) = fixture();
            let id = session.id;
            session.save(&storage).unwrap();
            let sessions = storage.ensure_subdir(SESSIONS_DIR).unwrap();
            let path = sessions.join(format!("{id}.jsonl"));
            let original = std::fs::read(&path).unwrap();
            let writer = SessionWriter::open_session(&sessions, &session).unwrap();
            let claim: CheckpointClaim = Arc::new(|_| Err(Arc::from(CLAIM_REJECTED)));
            assert!(
                writer
                    .recover(session, CheckpointPublication::default(), claim)
                    .await
                    .is_err()
            );
            assert_eq!(std::fs::read(&path).unwrap(), original);
            let stored = StoredSession::load(id, &storage).unwrap();
            assert!(
                stored.latest_state().unwrap().scopes[&Some(root)]
                    .results
                    .is_empty()
            );
        });
    }
}
