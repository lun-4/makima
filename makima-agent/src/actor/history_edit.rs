use std::collections::HashSet;
use std::sync::Arc;

use makima_providers::provider::Provider;
use makima_providers::{ContentBlock, ConversationEnd, Message, TokenUsage};
use makima_storage::session_state::StoredAgentConfig;
use makima_storage::sessions::{HistorySnapshot, next_epoch};

use super::prepared::{CancelPublication, PreparedOperation};
use super::{
    ActorError, ActorIdleGuard, ActorLifecycle, AgentActorHandle, ChildDelivery, ConfigChange,
    ConfigCommit, PreparedCommit, PreparedModel, PreparedOperationTicket, PreparedTransaction,
    TurnResult, TurnTicket,
};
use crate::{AgentId, AgentMode, History, ModeDef, TurnId, end_conversation_detached};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistoryBoundary {
    pub agent_id: AgentId,
    pub epoch: u64,
    pub offset: usize,
}

#[derive(Clone)]
pub struct HistoryChange {
    pub agent_id: AgentId,
    pub epoch: u64,
    pub messages: Arc<Vec<Message>>,
    pub draft: Message,
}

#[derive(Clone, Default)]
pub struct AgentStateSnapshot {
    pub results: Vec<TurnResult>,
    pub deliveries: Vec<Arc<ChildDelivery>>,
    pub delivery_seen: Vec<TurnId>,
    pub usage: TokenUsage,
}

pub struct PreparedActorRewind {
    idle: ActorIdleGuard,
    history: HistorySnapshot<Message>,
    commit: ConfigCommit,
    state: AgentStateSnapshot,
    ended: Option<(Arc<dyn Provider>, Arc<str>)>,
}

pub struct InstalledActorRewind {
    idle: ActorIdleGuard,
    commit: ConfigCommit,
    history: HistoryChange,
    ended: Option<(Arc<dyn Provider>, Arc<str>)>,
}

impl PreparedActorRewind {
    pub fn config(&self) -> &ConfigCommit {
        &self.commit
    }

    pub fn history(&self) -> &HistorySnapshot<Message> {
        &self.history
    }

    pub fn agent_id(&self) -> AgentId {
        self.idle.actor.agent_id()
    }

    pub fn with_state(mut self, state: AgentStateSnapshot) -> Result<Self, ActorError> {
        if state
            .results
            .iter()
            .any(|result| result.outcome.agent_id() != self.idle.actor.agent_id())
        {
            return Err(ActorError::InvalidConfig(
                "rewind result belongs to another actor".into(),
            ));
        }
        self.state = state;
        Ok(self)
    }

    pub fn install_held(self) -> InstalledActorRewind {
        self.install_held_inner(
            #[cfg(test)]
            || {},
        )
    }

    fn install_held_inner(
        self,
        #[cfg(test)] after_outcomes: impl FnOnce(),
    ) -> InstalledActorRewind {
        let Self {
            idle,
            history,
            commit,
            state: restored,
            ended,
        } = self;
        {
            let inner = &idle.actor.inner;
            let mut outcomes = inner
                .outcomes
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            #[cfg(test)]
            after_outcomes();
            let mut state = inner
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            state.policy_generation = commit.generation;
            state.policy = Some(Arc::clone(&commit.config));
            state.latest = None;
            state.latest_history_turn = None;
            state.settled = None;
            state.usage = restored.usage;
            state.instructions.clear();
            state.turn_work.clear();
            state.deliveries.clear();
            state.delivery_order.clear();
            state.delivery_runs.clear();
            state.delivery_seen = restored.delivery_seen.into();
            state.delivery_suspended = true;
            state.delivery_held = true;
            for delivery in restored.deliveries {
                super::delivery::restore_pending(&mut state, delivery, Vec::new());
            }
            state.retained_order.clear();
            inner
                .tickets
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .clear();
            let mut tickets = inner
                .retained_tickets
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            outcomes.clear();
            tickets.clear();
            let previous_history = inner.history.load_full();
            let shared_prefix_len = previous_history
                .messages
                .iter()
                .zip(history.messages.iter())
                .take_while(|(previous, restored)| previous == restored)
                .count();
            let mut coverage = inner
                .coverage
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            for boundary in coverage.values_mut() {
                *boundary = boundary.and_then(|(epoch, end)| {
                    (epoch == previous_history.epoch && end <= shared_prefix_len)
                        .then_some((history.epoch, end))
                });
            }
            for result in restored
                .results
                .into_iter()
                .rev()
                .take(super::MAX_RETAINED_TURN_RESULTS)
                .collect::<Vec<_>>()
                .into_iter()
                .rev()
            {
                let turn = result.outcome.turn_id();
                state.latest = Some(result.outcome.clone());
                state.retained_order.push_back(turn);
                outcomes.insert(turn, result.outcome.clone());
                tickets.insert(
                    turn,
                    TurnTicket::from_result_for_actor(result, Arc::clone(&inner.identity)),
                );
            }
            coverage.retain(|turn, _| outcomes.contains_key(turn));
            for turn in outcomes.keys() {
                coverage.entry(*turn).or_insert(None);
            }
            if let Some(latest) = &state.latest {
                let turn = latest.turn_id();
                let boundary = coverage
                    .entry(turn)
                    .or_default()
                    .get_or_insert((history.epoch, history.messages.len()));
                state.latest_history_turn = Some((turn, boundary.0, boundary.1));
            }
            *inner
                .latest_admitted
                .lock()
                .unwrap_or_else(|error| error.into_inner()) =
                state.latest.as_ref().map(crate::TurnOutcome::turn_id);
            inner.history.store(Arc::new(history.clone()));
            if let Some(reservation) = state.owned_idle(&idle.owner) {
                reservation.committing = false;
            }
        }
        InstalledActorRewind {
            history: HistoryChange {
                agent_id: idle.actor.agent_id(),
                epoch: history.epoch,
                messages: history.messages,
                draft: Message::user(String::new()),
            },
            idle,
            commit,
            ended,
        }
    }
}

impl InstalledActorRewind {
    pub fn config(&self) -> &ConfigCommit {
        &self.commit
    }

    pub fn notify(self) -> (ActorIdleGuard, ConfigCommit, HistoryChange) {
        let Self {
            idle,
            commit,
            history,
            ended,
        } = self;
        {
            let mut state = idle
                .actor
                .inner
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            state
                .config_observers
                .retain(|observer| observer.send(commit.clone()).is_ok());
            state.emit(super::ActorEvent::Config(commit.clone()));
        }
        if let Some((provider, ended)) = ended {
            end_conversation_detached(&provider, ended, ConversationEnd::Rewound);
        }
        idle.actor.inner.policy_changed.notify(usize::MAX);
        (idle, commit, history)
    }
}

pub struct HistoryEditWork {
    owner: Arc<()>,
    action: HistoryEditAction,
}

type PreparedEditResult =
    Result<(ActorIdleGuard, PreparedCommit, Option<HistoryChange>), ActorError>;

pub(super) type HistoryEditCompletion = (flume::Sender<PreparedEditResult>, PreparedEditResult);

struct HistoryEditAction {
    operation: Box<PreparedOperation>,
    replacement: Option<HistorySnapshot<Message>>,
    transaction: PreparedTransaction,
    completion: flume::Sender<PreparedEditResult>,
}

impl AgentActorHandle {
    pub fn history_boundary_message(
        &self,
        boundary: &HistoryBoundary,
    ) -> Result<Message, ActorError> {
        let history = self.inner.history.load_full();
        if boundary.agent_id != self.agent_id()
            || boundary.epoch != history.epoch
            || !valid_boundary(&history.messages, boundary.offset)
        {
            return Err(ActorError::StaleHistoryBoundary);
        }
        Ok(history.messages[boundary.offset].clone())
    }

    /// The boundary's message, to restore as the draft, and the history
    /// before it, to replace the current history with.
    pub fn rewind_snapshot(
        &self,
        boundary: &HistoryBoundary,
    ) -> Result<(Message, HistorySnapshot<Message>), ActorError> {
        let draft = self.history_boundary_message(boundary)?;
        let history = self.inner.history.load_full();
        let mut replacement = HistorySnapshot::new(history.messages[..boundary.offset].to_vec());
        replacement.compacted = history.compacted;
        Ok((draft, replacement))
    }

    pub fn history_boundaries(&self) -> Result<Vec<HistoryBoundary>, ActorError> {
        Ok(self
            .history_boundary_messages()?
            .into_iter()
            .map(|(boundary, _)| boundary)
            .collect())
    }

    pub fn history_boundary_messages(&self) -> Result<Vec<(HistoryBoundary, Message)>, ActorError> {
        {
            let state = self.inner.state.lock().unwrap_or_else(|e| e.into_inner());
            if state.lifecycle != ActorLifecycle::Open {
                return Err(super::lifecycle_error(state.lifecycle));
            }
        }
        let history = self.inner.history.load_full();
        let mut pending = HashSet::new();
        let mut valid = true;
        let mut boundaries = Vec::new();
        for (offset, message) in history.messages.iter().enumerate() {
            if valid && pending.is_empty() && History::is_boundary_message(message) {
                boundaries.push((
                    HistoryBoundary {
                        agent_id: self.agent_id(),
                        epoch: history.epoch,
                        offset,
                    },
                    message.clone(),
                ));
            }
            valid &= update_pending_tools(message, &mut pending);
        }
        Ok(boundaries)
    }
}

impl ActorIdleGuard {
    pub async fn prepare_rewind(
        self,
        history: HistorySnapshot<Message>,
        saved: StoredAgentConfig,
        prepared: PreparedModel,
        mode: (AgentMode, ModeDef),
    ) -> Result<PreparedActorRewind, ActorError> {
        let source = self.source_snapshot()?;
        let config = ConfigChange::resolved_restore(&saved, prepared, mode, &source.config.config)?;
        self.actor.inner.validate_mode_config(&config)?;
        let generation = source.config.generation.checked_add(1).ok_or_else(|| {
            ActorError::InvalidConfig("configuration generation exhausted".into())
        })?;
        let mut history = history;
        history.epoch = next_epoch();
        history.conversation = source.history.conversation;
        let mut replacement = History::restored_exact(history);
        let ended = if replacement.is_empty() {
            replacement.restart_conversation()
        } else {
            replacement.continue_conversation()
        }
        .map(|ended| (Arc::clone(&source.config.config.provider), ended));
        {
            let mut state = self
                .actor
                .inner
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if state.lifecycle != ActorLifecycle::Open {
                return Err(super::lifecycle_error(state.lifecycle));
            }
            state
                .owned_idle(&self.owner)
                .filter(|reservation| {
                    !reservation.committing
                        && reservation.permission.is_none()
                        && reservation.edit.is_none()
                })
                .ok_or(ActorError::Busy)?
                .committing = true;
        }
        Ok(PreparedActorRewind {
            commit: ConfigCommit {
                identity: Arc::clone(&self.actor.inner.identity),
                generation,
                config: Arc::new(config),
            },
            history: replacement.snapshot().clone(),
            idle: self,
            state: AgentStateSnapshot::default(),
            ended,
        })
    }

    pub async fn commit_prepared_transaction_held(
        mut self,
        operation: PreparedOperationTicket,
        replacement: Option<HistorySnapshot<Message>>,
        transaction: PreparedTransaction,
    ) -> PreparedEditResult {
        if !operation.belongs_to(&self.actor) || transaction.publication.is_cancelled() {
            return Err(ActorError::PolicyCancelled);
        }
        let publication = transaction.publication.clone();
        let _cancellation = CancelPublication(publication.clone());
        let (completion, response) = flume::bounded(1);
        let actor = self.actor.clone();
        {
            let mut state = actor.inner.state.lock().unwrap_or_else(|e| e.into_inner());
            if state.lifecycle != ActorLifecycle::Open {
                return Err(super::lifecycle_error(state.lifecycle));
            }
            state
                .owned_idle(&self.owner)
                .filter(|reservation| {
                    reservation.permission.is_none() && reservation.edit.is_none()
                })
                .ok_or(ActorError::Busy)?;
            let pending = operation.take_ready_locked(&mut state)?;
            state.begin_prepared_publication(&pending, publication.clone());
            state.owned_idle(&self.owner).unwrap().committing = true;
            state.owned_idle(&self.owner).unwrap().edit = Some(HistoryEditWork {
                owner: Arc::clone(&self.owner),
                action: HistoryEditAction {
                    operation: Box::new(pending),
                    replacement,
                    transaction,
                    completion,
                },
            });
            self.release = false;
            actor.inner.queue.notify();
        }
        {
            let result = futures_lite::future::or(
                async {
                    publication.cancelled().await;
                    let edit = {
                        let mut state = actor.inner.state.lock().unwrap_or_else(|e| e.into_inner());
                        let edit = state
                            .owned_idle(&self.owner)
                            .and_then(|idle| idle.edit.take());
                        if edit.is_some() {
                            state.end_prepared_publication(operation.operation_id());
                        }
                        edit
                    };
                    if let Some(edit) = edit {
                        edit.fail(ActorError::PolicyCancelled);
                        self.release = true;
                    }
                    None
                },
                async { Some(response.recv_async().await) },
            )
            .await;
            if let Some(result) = result {
                return result.map_err(|_| ActorError::Closed)?;
            }
        }
        response
            .recv_async()
            .await
            .map_err(|_| ActorError::Closed)?
    }
}

impl HistoryEditWork {
    pub(super) fn prepared_id(&self) -> u64 {
        self.action.operation.id
    }

    pub(super) fn fail(self, error: ActorError) {
        let HistoryEditAction {
            operation,
            completion,
            ..
        } = self.action;
        (*operation).fail(error.clone());
        let _ = completion.send(Err(error));
    }

    pub(super) async fn execute(
        self,
        actor: &AgentActorHandle,
        history: &mut History,
    ) -> HistoryEditCompletion {
        history.synchronize_prepared_mirror();
        let mut guard = ActorIdleGuard {
            actor: actor.clone(),
            owner: Arc::clone(&self.owner),
            release: true,
        };
        let HistoryEditAction {
            operation,
            replacement,
            transaction,
            completion,
        } = self.action;
        let snapshot = replacement
            .clone()
            .unwrap_or_else(|| (*actor.inner.history.load_full()).clone());
        let snapshot = {
            let state = actor.inner.state.lock().unwrap_or_else(|e| e.into_inner());
            operation.transaction_snapshot(&actor.inner, &state, snapshot)
        };
        let result = transaction.publish(snapshot).await;
        let id = operation.id;
        let result = match result {
            Err(error) => {
                (*operation).fail(error.clone());
                let mut state = actor.inner.state.lock().unwrap_or_else(|e| e.into_inner());
                state.end_prepared_publication(id);
                super::drive_operations(&actor.inner, &mut state);
                Err(error)
            }
            Ok(finalize) => {
                let mut state = actor.inner.state.lock().unwrap_or_else(|e| e.into_inner());
                let change = replacement.map(|snapshot| {
                    history.adopt_prepared_snapshot(snapshot.clone());
                    // Nothing kept is a fresh start, not a rewind to build on.
                    let (ended, reason) = if history.is_empty() {
                        (history.restart_conversation(), ConversationEnd::Closed)
                    } else {
                        (history.continue_conversation(), ConversationEnd::Rewound)
                    };
                    if let Some(policy) = &state.policy
                        && let Some(ended) = ended
                    {
                        end_conversation_detached(&policy.settings.provider, ended, reason);
                    }
                    HistoryChange {
                        agent_id: actor.agent_id(),
                        epoch: snapshot.epoch,
                        messages: snapshot.messages,
                        draft: Message::user(String::new()),
                    }
                });
                let committed = (*operation).commit_locked(&actor.inner, &mut state, finalize);
                state.end_prepared_publication(id);
                if committed.is_ok()
                    && let Some(reservation) = state.owned_idle(&self.owner)
                {
                    reservation.committing = false;
                    guard.release = false;
                }
                super::drive_operations(&actor.inner, &mut state);
                committed.map(|committed| (committed, change))
            }
        };
        if let Ok((committed, _)) = &result
            && let Some(ticket) = &committed.ticket
        {
            let cancelled = {
                let state = actor.inner.state.lock().unwrap_or_else(|e| e.into_inner());
                let reason = match state.lifecycle {
                    ActorLifecycle::Open => None,
                    ActorLifecycle::Closed => Some(crate::TurnCancellationReason::Closed),
                    ActorLifecycle::Shutdown => Some(crate::TurnCancellationReason::Shutdown),
                };
                if let Some(reason) = reason {
                    actor
                        .inner
                        .queue
                        .remove_turn(ticket.turn_id())
                        .zip(Some(reason))
                } else {
                    None
                }
            };
            if let Some((admission, reason)) = cancelled {
                super::terminalize_work(
                    &actor.inner,
                    vec![super::ActorWork::Turn(Box::new(admission))],
                    reason,
                );
            }
        }
        let result = result.map(|(commit, history)| {
            let idle = ActorIdleGuard {
                actor: actor.clone(),
                owner: Arc::clone(&self.owner),
                release: true,
            };
            (idle, commit, history)
        });
        drop(guard);
        (completion, result)
    }
}

fn valid_boundary(messages: &[Message], offset: usize) -> bool {
    let Some(message) = messages.get(offset) else {
        return false;
    };
    let mut pending = HashSet::new();
    History::is_boundary_message(message)
        && messages[..offset]
            .iter()
            .all(|message| update_pending_tools(message, &mut pending))
        && pending.is_empty()
}

fn update_pending_tools<'a>(message: &'a Message, pending: &mut HashSet<&'a str>) -> bool {
    for block in &message.content {
        match block {
            ContentBlock::ToolUse { id, .. } => {
                pending.insert(id.as_str());
            }
            ContentBlock::ToolResult { tool_use_id, .. }
                if !pending.remove(tool_use_id.as_str()) =>
            {
                return false;
            }
            _ => {}
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::{Arc, Mutex};

    use arc_swap::ArcSwap;
    use futures_lite::future;
    use makima_providers::provider::{BoxFuture, Provider};
    use makima_providers::{
        AgentError, ContentBlock, ConversationEnd, Invocation, Message, Model, ProviderEvent,
        RequestDelivery, RequestOptions, Role, StreamResponse, TokenUsage,
    };
    use makima_storage::checkpoint::CheckpointPublication;
    use makima_storage::conversation::ConversationState;
    use makima_storage::id::SessionRef;
    use makima_storage::sessions::{HistorySnapshot, next_epoch};
    use serde_json::Value;
    use test_case::test_case;

    use super::{AgentStateSnapshot, HistoryBoundary, HistoryChange, valid_boundary};
    use crate::actor::tests::{ScriptedBackend, input, policy};
    use crate::actor::{
        ActorBackend, ActorError, ActorHistory, AgentActorHandle, BackendResult, ChildDelivery,
        ConfigChange, ConfigPatch, ControlWork, EffectiveAgentConfig, PreparedPublication,
        PreparedTransaction, PreparedTurn, TurnContext, TurnResult, WorkKind,
    };
    use crate::{
        AgentId, AgentInput, AgentMode, DoneReason, TurnCancellationReason, TurnId, TurnOutcome,
    };

    const CHECKPOINT_ERROR: &str = "checkpoint unavailable";
    const RESTORED_CWD: &str = "/rewind-fixture";

    fn idle_actor(messages: Vec<Message>) -> (AgentActorHandle, smol::Task<()>) {
        let (actor, runner) =
            AgentActorHandle::spawn(AgentId::generate(), messages, None, Box::new(IdleBackend));
        actor
            .initialize_config(EffectiveAgentConfig::new(policy(false), AgentMode::Build))
            .unwrap();
        (actor, runner)
    }

    #[test_case(false; "install_is_silent_until_notified")]
    #[test_case(true; "dropping_candidate_preserves_live_state")]
    fn prepared_rewind_installs_exact_candidate_without_journaling(abort: bool) {
        smol::block_on(async {
            use crate::actor::PreparedModel;
            use crate::session_checkpoint::stored_agent_config;
            use std::collections::BTreeMap;

            const SAVED_CWD: &str = "/rewound";
            const SAVED_TEXT: &str = "saved boundary";
            let (actor, runner) = idle_actor(vec![Message::user("displaced".into())]);
            let source = actor.config_snapshot().unwrap();
            let before = actor.inner.history.load_full();
            let observer = actor.subscribe_config_commits();
            while observer.try_recv().is_ok() {}
            let mut saved = stored_agent_config(&source.config, BTreeMap::new()).unwrap();
            saved.cwd = SAVED_CWD.into();
            saved.fast = true;
            let mode = crate::modes::from_snapshot(&saved.mode).unwrap();
            let candidate = actor
                .prepare_idle()
                .unwrap()
                .prepare_rewind(
                    HistorySnapshot::new(vec![Message::user(SAVED_TEXT.into())]),
                    saved,
                    PreparedModel {
                        provider: Arc::clone(&source.config.provider),
                        model: source.config.model.clone(),
                    },
                    mode,
                )
                .await
                .unwrap();
            assert_eq!(candidate.config().generation, source.generation + 1);
            assert_eq!(
                actor.config_snapshot().unwrap().generation,
                source.generation
            );
            assert!(Arc::ptr_eq(
                &actor.inner.history.load_full().messages,
                &before.messages
            ));
            if abort {
                drop(candidate);
                assert_eq!(
                    actor.config_snapshot().unwrap().generation,
                    source.generation
                );
                assert!(Arc::ptr_eq(
                    &actor.inner.history.load_full().messages,
                    &before.messages
                ));
                assert!(observer.try_recv().is_err());
                drop(actor.prepare_idle().unwrap());
            } else {
                let installed = candidate.install_held();
                assert!(observer.try_recv().is_err());
                assert_eq!(
                    actor.inner.history.load_full().messages[0].first_text_content(),
                    Some(SAVED_TEXT)
                );
                let config = actor.config_snapshot().unwrap();
                assert!(config.config.fast);
                assert_eq!(config.config.context.cwd(), std::path::Path::new(SAVED_CWD));
                let (idle, committed, change) = installed.notify();
                assert_eq!(
                    observer.try_recv().unwrap().generation,
                    committed.generation
                );
                assert_eq!(change.epoch, actor.inner.history.load_full().epoch);
                drop(idle);
            }
            actor.close();
            runner.await;
        });
    }

    async fn prepared_rewind(
        actor: &AgentActorHandle,
        history: HistorySnapshot<Message>,
        state: AgentStateSnapshot,
    ) -> super::PreparedActorRewind {
        use crate::actor::PreparedModel;
        use crate::session_checkpoint::stored_agent_config;
        use std::collections::BTreeMap;

        let source = actor.config_snapshot().unwrap();
        let mut saved = stored_agent_config(&source.config, BTreeMap::new()).unwrap();
        saved.cwd = RESTORED_CWD.into();
        let mode = crate::modes::from_snapshot(&saved.mode).unwrap();
        actor
            .prepare_idle()
            .unwrap()
            .prepare_rewind(
                history,
                saved,
                PreparedModel {
                    provider: Arc::clone(&source.config.provider),
                    model: source.config.model.clone(),
                },
                mode,
            )
            .await
            .unwrap()
            .with_state(state)
            .unwrap()
    }

    #[test]
    fn rewind_install_locks_outcomes_before_state() {
        smol::block_on(async {
            let (start, rx) = flume::bounded(1);
            let (actor, runner) = AgentActorHandle::spawn_inner(
                AgentId::generate(),
                Vec::new(),
                None,
                Box::new(IdleBackend),
                None,
                None,
                Some(rx),
            );
            actor
                .initialize_config(EffectiveAgentConfig::new(policy(false), AgentMode::Build))
                .unwrap();
            let candidate = prepared_rewind(
                &actor,
                HistorySnapshot::new(Vec::new()),
                AgentStateSnapshot::default(),
            )
            .await;
            let installed = candidate.install_held_inner(|| {
                assert!(actor.inner.outcomes.try_lock().is_err());
                assert!(actor.inner.state.try_lock().is_ok());
            });
            drop(installed.notify());
            actor.close();
            drop(start);
            runner.await;
        });
    }

    #[test_case(false, false, None; "exact_prefix_rebases_older_boundaries")]
    #[test_case(true, false, None; "divergent_suffix_does_not_expose_later_messages")]
    #[test_case(false, true, None; "stale_epoch_is_unavailable")]
    #[test_case(false, false, Some(1); "known_latest_boundary_is_not_widened")]
    fn rewind_rebuilds_retained_transcript_coverage(
        divergent: bool,
        stale: bool,
        latest_end: Option<usize>,
    ) {
        smol::block_on(async {
            use crate::actor::TranscriptRequest;

            const FIRST: &str = "first retained turn";
            const SECOND: &str = "second retained turn";
            const REPLACEMENT: &str = "restored replacement";
            const DISPLACED: &str = "discarded turn";
            const LAST_MESSAGES: usize = 10;
            const MAX_BYTES: usize = 4096;
            let (actor, runner) = idle_actor(vec![
                Message::user(FIRST.into()),
                Message::user(SECOND.into()),
                Message::user(DISPLACED.into()),
            ]);
            let previous_epoch = actor.inner.history.load().epoch;
            let turns = [TurnId::generate(), TurnId::generate(), TurnId::generate()];
            let discarded = TurnId::generate();
            let epoch = if stale { next_epoch() } else { previous_epoch };
            actor.inner.coverage.lock().unwrap().extend([
                (turns[0], Some((epoch, 1))),
                (turns[1], Some((epoch, 2))),
                (discarded, Some((previous_epoch, 3))),
            ]);
            if let Some(end) = latest_end {
                actor
                    .inner
                    .coverage
                    .lock()
                    .unwrap()
                    .insert(turns[2], Some((previous_epoch, end)));
            }
            let messages = vec![
                Message::user(FIRST.into()),
                Message::user(if divergent { REPLACEMENT } else { SECOND }.into()),
            ];
            let results = turns
                .iter()
                .map(|turn| TurnResult {
                    outcome: TurnOutcome::completed(
                        actor.agent_id(),
                        *turn,
                        TokenUsage::default(),
                        1,
                        DoneReason::EndTurn,
                    ),
                    text: String::new(),
                    output: Vec::new(),
                    provenance: Default::default(),
                    duration_ms: 0,
                })
                .collect();
            let candidate = prepared_rewind(
                &actor,
                HistorySnapshot::new(messages.clone()),
                AgentStateSnapshot {
                    results,
                    ..Default::default()
                },
            )
            .await;
            let epoch = candidate.history().epoch;
            let installed = candidate.install_held();
            let request = TranscriptRequest {
                through_turn: None,
                last_messages: LAST_MESSAGES,
                max_bytes: MAX_BYTES,
            };
            let snapshot = actor.transcript(request.clone()).unwrap();
            let latest_end = latest_end.unwrap_or(messages.len());
            assert_eq!(snapshot.through_turn, Some(turns[2]));
            assert_eq!(
                snapshot.messages,
                serde_json::to_value(&messages[..latest_end]).unwrap()
            );
            assert_eq!(snapshot.epoch, epoch);
            for (index, turn) in turns.iter().enumerate() {
                assert!(actor.turn_ticket(*turn).unwrap().peek_result().is_some());
                let snapshot = actor.transcript(TranscriptRequest {
                    through_turn: Some(*turn),
                    ..request.clone()
                });
                if index == 2 || (!stale && (index == 0 || !divergent)) {
                    let expected = if index == 2 { latest_end } else { index + 1 };
                    assert_eq!(
                        snapshot.unwrap().messages,
                        serde_json::to_value(&messages[..expected]).unwrap()
                    );
                } else {
                    assert!(matches!(
                        snapshot,
                        Err(ActorError::UnavailableTurnHistory(id)) if id == *turn
                    ));
                }
            }
            assert!(matches!(
                actor.transcript(TranscriptRequest {
                    through_turn: Some(discarded),
                    ..request.clone()
                }),
                Err(ActorError::UnknownTurn(id)) if id == discarded
            ));
            let mut active_history = actor.committed_history_snapshot();
            Arc::make_mut(&mut active_history.messages).push(Message::user(DISPLACED.into()));
            actor.inner.history.store(Arc::new(active_history));
            assert_eq!(
                actor.transcript(request).unwrap().messages,
                serde_json::to_value(&messages[..latest_end]).unwrap()
            );
            drop(installed.notify());
            actor.close();
            runner.await;
        });
    }

    fn published() -> PreparedPublication {
        Box::new(|_, publication| {
            Box::pin(async move {
                assert!(publication.try_publish());
                Ok(())
            })
        })
    }

    async fn rewind(
        actor: &AgentActorHandle,
        boundary: &HistoryBoundary,
        publish: PreparedPublication,
    ) -> Result<HistoryChange, ActorError> {
        let guard = actor.prepare_idle()?;
        let (_, replacement) = actor.rewind_snapshot(boundary)?;
        let operation = guard.reserve_prepared_operation(None)?;
        operation.resolve(None)?;
        operation.wait_ready().await?;
        let transaction = PreparedTransaction {
            state_edits: Vec::new(),
            options: None,
            history_changed: true,
            boundary: false,
            publication: CheckpointPublication::default(),
            publish,
            finalize: Box::new(|_| {}),
        };
        let (_, _, change) = guard
            .commit_prepared_transaction_held(operation, Some(replacement), transaction)
            .await?;
        Ok(change.expect("rewind replaces history"))
    }

    #[test]
    fn approval_receipt_round_trips_recovery_and_rewinds_approved_plan() {
        use crate::{
            AgentContextSnapshot, AgentManagerHandle, ModeRegistry, ToolOutput, plan_state,
            session_checkpoint::stored_agent_config, session_recovery::RecoveryCandidate,
        };
        use makima_providers::ModelSelection;
        use makima_storage::plans::PlanSlot;
        use makima_storage::session_state::{SessionStateView, StateChange};
        use makima_storage::sessions::Session;
        smol::block_on(async {
            const PLAN: &str = "# Approved implementation plan\n";
            const TEXT: &str = "planning conversation";
            const LATER: &str = "later conversation";
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("approved.md");
            std::fs::write(&path, PLAN).unwrap();
            let mode = AgentMode::Plan(path.clone());
            let config = EffectiveAgentConfig::new(policy(false), mode.clone())
                .with_context(AgentContextSnapshot::new(directory.path().to_path_buf()))
                .with_mode_def(Some(ModeRegistry::builtin().current(&mode)));
            let manager = AgentManagerHandle::new(Default::default()).unwrap();
            let root = manager
                .create_root_with_config(
                    Some(config),
                    vec![Message::user(TEXT.into())],
                    None,
                    |_| Ok::<Box<dyn ActorBackend>, String>(Box::new(IdleBackend)),
                )
                .unwrap();
            let journal = manager.journal();
            let actor = root.actor().unwrap();
            let original = actor.committed_history_snapshot();
            let saved_config =
                stored_agent_config(&actor.effective_config().unwrap(), Default::default())
                    .unwrap();
            let selection = ModelSelection {
                spec: Some(saved_config.model.clone().into()),
                ..Default::default()
            };
            let edit =
                plan_state::approval_edit(&root, &path, PLAN.into(), selection.clone()).unwrap();
            let operation = actor.reserve_prepared_operation(None).unwrap();
            operation.resolve(None).unwrap();
            operation.wait_ready().await.unwrap();
            let committed = operation
                .commit_durable(
                    PreparedTransaction {
                        publication: CheckpointPublication::default(),
                        options: None,
                        state_edits: vec![edit],
                        history_changed: false,
                        boundary: false,
                        publish: published(),
                        finalize: Box::new(|_| {}),
                    }
                    .with_boundary(),
                )
                .await
                .unwrap();
            assert!(committed.ticket.is_none());
            let receipt = committed.boundary.unwrap();
            assert_eq!(committed.config.config.mode, mode);
            let later_config = stored_agent_config(
                &EffectiveAgentConfig::new(policy(false), AgentMode::Build)
                    .normalize_initial_mode(),
                Default::default(),
            )
            .unwrap();
            let mut later_history = original.clone();
            Arc::make_mut(&mut later_history.messages).push(Message::user(LATER.into()));
            journal
                .commit(crate::journal::JournalCommit {
                    changes: vec![crate::journal::JournalChange {
                        agent: Some(root.id()),
                        generation: 1,
                        change: StateChange::Config(later_config),
                    }],
                    histories: [(root.id(), later_history)].into(),
                    ..Default::default()
                })
                .unwrap();
            let mut session = Session::<Message, TokenUsage, ToolOutput>::new(
                &saved_config.model,
                &directory.path().to_string_lossy(),
            );
            journal.snapshot().apply_to(&mut session).unwrap();
            session.save_to(directory.path()).unwrap();
            let session = Session::load_from(session.id, directory.path()).unwrap();
            let recovered = RecoveryCandidate::prepare(session)
                .unwrap()
                .finalize_after_publication();
            let journal = recovered.journal();
            let mut replacement = journal
                .prepare_rewind(crate::rewind::RewindBoundary {
                    epoch: journal.snapshot().epoch,
                    ..receipt
                })
                .await
                .unwrap();
            let view = SessionStateView::fold(
                &replacement.snapshot().state,
                &replacement.snapshot().agent_revisions,
                replacement.snapshot().seq,
            )
            .unwrap();
            assert_eq!(
                view.scopes[&Some(root.id())].config.as_ref(),
                Some(&saved_config)
            );
            assert_eq!(
                replacement.snapshot().histories[&root.id()].messages,
                original.messages
            );
            let plans: PlanSlot = serde_json::from_value(
                view.scopes[&None].plugins[makima_storage::plans::PLAN_STATE_KEY]
                    .value
                    .clone()
                    .unwrap(),
            )
            .unwrap();
            let plan = &plans[&root.id()];
            assert_eq!(plan.path, path);
            assert_eq!(plan.approved.as_ref().unwrap().content, PLAN);
            assert_eq!(plan.staged_model.as_ref(), Some(&selection));
            replacement.install().unwrap();
            drop(replacement);
            actor.shutdown();
        });
    }

    #[test_case(false, false; "boundary_only_published")]
    #[test_case(false, true; "boundary_only_cancelled")]
    #[test_case(true, false; "approval_and_boundary_published")]
    #[test_case(true, true; "approval_and_boundary_cancelled")]
    fn prepared_boundary_receipt_matches_atomic_publication(with_edit: bool, cancel: bool) {
        smol::block_on(async {
            use std::time::Duration;

            use crate::{
                AgentManagerHandle, ModeDef, StateScope, StateSlotSpec, journal::SessionJournal,
            };
            use makima_storage::session_state::StateChange;

            const SLOT_KEY: &str = "fixture/approval";
            const TEXT: &str = "unchanged history";
            let journal = Arc::new(SessionJournal::default());
            let manager =
                AgentManagerHandle::new_with_journal(Default::default(), Arc::clone(&journal))
                    .unwrap();
            let config = EffectiveAgentConfig::new(policy(false), AgentMode::Build)
                .with_mode_def(Some(ModeDef::default_for(AgentMode::Build.id())));
            let root = manager
                .create_root_with_config(
                    Some(config),
                    vec![Message::user(TEXT.into())],
                    None,
                    |_| Ok::<Box<dyn ActorBackend>, String>(Box::new(IdleBackend)),
                )
                .unwrap();
            let actor = root.actor().unwrap();
            let before_config = actor.config_snapshot().unwrap();
            let before_history = actor.committed_history_snapshot();
            let before_journal = journal.snapshot();
            let observer = actor.subscribe_config_commits();
            while observer.try_recv().is_ok() {}
            let slot = journal
                .register_state::<Value>(StateSlotSpec {
                    key: SLOT_KEY.into(),
                    scope: StateScope::Session,
                    version: 1,
                })
                .unwrap();
            let access = root.state_access();
            let state_edits = if with_edit {
                vec![slot.edit(&access, Some(Value::Bool(true)), None).unwrap()]
            } else {
                Vec::new()
            };
            let (captured, capture) = flume::bounded(1);
            let (finalized, finalization) = flume::bounded(1);
            let callback_journal = Arc::clone(&journal);
            let callback_before = before_journal.clone();
            let final_slot = slot.clone();
            let final_access = access.clone();
            let operation = actor.reserve_prepared_operation(None).unwrap();
            operation.resolve(None).unwrap();
            operation.wait_ready().await.unwrap();
            let result = operation
                .commit_durable(
                    PreparedTransaction {
                        state_edits,
                        options: None,
                        history_changed: false,
                        boundary: false,
                        publication: CheckpointPublication::default(),
                        publish: Box::new(move |snapshot, publication| {
                            Box::pin(async move {
                                let candidate = snapshot.journal.unwrap();
                                let boundary = snapshot.boundary.unwrap();
                                assert_eq!(boundary.agent, final_access.target());
                                assert_eq!(boundary.epoch, candidate.epoch);
                                assert_eq!(callback_journal.snapshot().state, callback_before.state);
                                assert_eq!(candidate.state[..callback_before.state.len()], callback_before.state);
                                let changes = &candidate.state[callback_before.state.len()..];
                                assert_eq!(changes.len(), usize::from(with_edit) + 1);
                                if with_edit {
                                    assert!(matches!(
                                        &changes[0].change,
                                        StateChange::Plugin { key, value: Some(Value::Bool(true)), .. }
                                            if key == SLOT_KEY
                                    ));
                                    assert!(changes[0].seq < boundary.seq);
                                }
                                let record = changes.last().unwrap();
                                assert_eq!(record.seq, boundary.seq);
                                assert_eq!(record.generation, snapshot.commit.generation);
                                let StateChange::Boundary { offset, lens } = &record.change else {
                                    panic!("publication must end at its boundary")
                                };
                                assert_eq!(*offset, snapshot.history.messages.len());
                                assert_eq!(
                                    lens[&boundary.agent],
                                    (snapshot.history.epoch, snapshot.history.messages.len())
                                );
                                for (id, history) in &callback_before.histories {
                                    let candidate_history = &candidate.histories[id];
                                    assert_eq!(candidate_history.epoch, history.epoch);
                                    assert_eq!(candidate_history.compacted, history.compacted);
                                    assert_eq!(candidate_history.conversation, history.conversation);
                                    assert!(Arc::ptr_eq(&candidate_history.messages, &history.messages));
                                }
                                captured.send((boundary, candidate.state)).unwrap();
                                if cancel {
                                    assert!(publication.cancel());
                                } else {
                                    assert!(publication.try_publish());
                                }
                                Ok(())
                            })
                        }),
                        finalize: Box::new(move |commit| {
                            assert_eq!(
                                final_slot.get(&access).unwrap().value,
                                with_edit.then_some(Value::Bool(true))
                            );
                            finalized.send(commit.generation).unwrap();
                        }),
                    }
                    .with_boundary(),
                )
                .await;
            let (boundary, candidate_state) = capture.recv().unwrap();
            if cancel {
                assert!(matches!(result, Err(ActorError::PolicyCancelled)));
                assert_eq!(journal.snapshot().state, before_journal.state);
                assert_eq!(slot.get(&root.state_access()).unwrap().value, None);
                assert!(finalization.try_recv().is_err());
            } else {
                let committed = result.unwrap();
                assert_eq!(committed.boundary, Some(boundary));
                assert!(committed.ticket.is_none());
                assert_eq!(committed.config.generation, before_config.generation);
                assert_eq!(journal.snapshot().state, candidate_state);
                assert_eq!(finalization.recv().unwrap(), before_config.generation);
                assert_eq!(
                    slot.get(&root.state_access()).unwrap().value,
                    with_edit.then_some(Value::Bool(true))
                );
                let operation = actor.reserve_prepared_operation(None).unwrap();
                operation.resolve(None).unwrap();
                operation.wait_ready().await.unwrap();
                let committed = operation
                    .commit_durable(PreparedTransaction {
                        state_edits: Vec::new(),
                        options: None,
                        history_changed: false,
                        boundary: false,
                        publication: CheckpointPublication::default(),
                        publish: Box::new(|snapshot, publication| {
                            Box::pin(async move {
                                assert!(snapshot.boundary.is_none());
                                assert!(publication.try_publish());
                                Ok(())
                            })
                        }),
                        finalize: Box::new(|_| {}),
                    })
                    .await
                    .unwrap();
                assert!(committed.boundary.is_none());
                assert!(committed.ticket.is_none());
                assert_eq!(journal.snapshot().state, candidate_state);
            }
            let after_config = actor.config_snapshot().unwrap();
            assert_eq!(after_config.generation, before_config.generation);
            assert!(Arc::ptr_eq(&after_config.config, &before_config.config));
            let after_history = actor.committed_history_snapshot();
            assert_eq!(after_history.epoch, before_history.epoch);
            assert_eq!(after_history.compacted, before_history.compacted);
            assert_eq!(after_history.conversation, before_history.conversation);
            assert!(Arc::ptr_eq(
                &after_history.messages,
                &before_history.messages
            ));
            assert!(observer.try_recv().is_err());
            manager.shutdown(Duration::from_secs(5)).await;
        });
    }

    #[test_case(false; "published")]
    #[test_case(true; "publication_failed")]
    fn nonpersistent_prepared_state_installs_only_after_publication(fail: bool) {
        smol::block_on(async {
            use crate::{AgentManagerHandle, StateScope, StateSlotSpec, journal::SessionJournal};
            use makima_storage::session_state::StateChange;

            const SLOT_KEY: &str = "fixture/runtime-state";
            let journal = Arc::new(SessionJournal::nonpersistent());
            let manager =
                AgentManagerHandle::new_with_journal(Default::default(), Arc::clone(&journal))
                    .unwrap();
            let root = manager
                .create_root_with_config(
                    Some(EffectiveAgentConfig::new(policy(false), AgentMode::Build)),
                    Vec::new(),
                    None,
                    |_| Ok::<Box<dyn ActorBackend>, String>(Box::new(IdleBackend)),
                )
                .unwrap();
            let actor = root.actor().unwrap();
            assert!(actor.inner.journal.is_none());
            let access = root.state_access();
            let slot = journal
                .register_state::<Value>(StateSlotSpec {
                    key: SLOT_KEY.into(),
                    scope: StateScope::Agent,
                    version: 1,
                })
                .unwrap();
            let edit = slot.edit(&access, Some(Value::Bool(true)), None).unwrap();
            let operation = actor.reserve_prepared_operation(None).unwrap();
            operation
                .resolve(Some(ConfigChange::Patch(ConfigPatch {
                    fast: Some(true),
                    ..Default::default()
                })))
                .unwrap();
            operation.wait_ready().await.unwrap();
            let result = operation
                .commit_durable(PreparedTransaction {
                    state_edits: vec![edit],
                    options: None,
                    history_changed: false,
                    boundary: false,
                    publication: CheckpointPublication::default(),
                    publish: Box::new(move |snapshot, publication| {
                        Box::pin(async move {
                            assert!(snapshot.journal.is_none());
                            if fail {
                                return Err(CHECKPOINT_ERROR.into());
                            }
                            assert!(publication.try_publish());
                            Ok(())
                        })
                    }),
                    finalize: Box::new(|_| {}),
                })
                .await;
            assert_eq!(result.is_ok(), !fail);
            assert_eq!(actor.config_snapshot().unwrap().config.fast, !fail);
            assert_eq!(
                slot.get(&access).unwrap().value,
                (!fail).then_some(Value::Bool(true))
            );
            slot.set(&access, Value::Bool(false), None).unwrap();
            assert!(!journal.snapshot().state.iter().any(|record| matches!(
                record.change,
                StateChange::Turn { .. } | StateChange::Delivery { .. }
            )));
            manager.shutdown(std::time::Duration::from_secs(5)).await;
        });
    }

    #[test_case(false; "installed")]
    #[test_case(true; "publication_failed")]
    fn nonpersistent_prepared_identical_history_fences_without_state_edit(fail: bool) {
        smol::block_on(async {
            use crate::{
                AgentManagerHandle, StateError, StateScope, StateSlotSpec, journal::SessionJournal,
            };

            const SLOT_KEY: &str = "fixture/runtime-history";
            let journal = Arc::new(SessionJournal::nonpersistent());
            let manager =
                AgentManagerHandle::new_with_journal(Default::default(), Arc::clone(&journal))
                    .unwrap();
            let root = manager
                .create_root_with_config(
                    Some(EffectiveAgentConfig::new(policy(false), AgentMode::Build)),
                    Vec::new(),
                    None,
                    |_| Ok::<Box<dyn ActorBackend>, String>(Box::new(IdleBackend)),
                )
                .unwrap();
            let actor = root.actor().unwrap();
            assert!(actor.inner.journal.is_none());
            let slot = journal
                .register_state::<Value>(StateSlotSpec {
                    key: SLOT_KEY.into(),
                    scope: StateScope::Agent,
                    version: 1,
                })
                .unwrap();
            let old = root.state_access();
            slot.set(&old, Value::Bool(true), None).unwrap();
            let before = journal.snapshot().state;
            let guard = actor.prepare_idle().unwrap();
            let operation = guard.reserve_prepared_operation(None).unwrap();
            operation.resolve(None).unwrap();
            operation.wait_ready().await.unwrap();
            let snapshot = actor.committed_history_snapshot();
            let result = guard
                .commit_prepared_transaction_held(
                    operation,
                    Some(snapshot),
                    PreparedTransaction {
                        state_edits: Vec::new(),
                        options: None,
                        history_changed: true,
                        boundary: false,
                        publication: CheckpointPublication::default(),
                        publish: Box::new(move |snapshot, publication| {
                            Box::pin(async move {
                                assert!(snapshot.journal.is_none());
                                if fail {
                                    return Err(CHECKPOINT_ERROR.into());
                                }
                                assert!(publication.try_publish());
                                Ok(())
                            })
                        }),
                        finalize: Box::new(|_| {}),
                    },
                )
                .await;
            assert_eq!(result.is_ok(), !fail);
            drop(result);
            assert_eq!(journal.snapshot().state, before);
            if fail {
                slot.set(&old, Value::Bool(false), None).unwrap();
            } else {
                assert!(matches!(
                    slot.set(&old, Value::Bool(false), None),
                    Err(StateError::StaleAccess)
                ));
            }
            slot.set(&root.state_access(), Value::Bool(false), None)
                .unwrap();
            manager.shutdown(std::time::Duration::from_secs(5)).await;
        });
    }

    #[test_case(false; "close")]
    #[test_case(true; "shutdown")]
    fn queued_history_edit_resolves_when_actor_closes(shutdown: bool) {
        smol::block_on(async {
            const TEXT: &str = "draft";
            let (_start, rx) = flume::bounded(1);
            let (actor, runner) = AgentActorHandle::spawn_inner(
                AgentId::generate(),
                vec![Message::user(TEXT.into())],
                None,
                Box::new(IdleBackend),
                None,
                None,
                Some(rx),
            );
            actor
                .initialize_config(EffectiveAgentConfig::new(policy(false), AgentMode::Build))
                .unwrap();
            let guard = actor.prepare_idle().unwrap();
            let operation = guard.reserve_prepared_operation(None).unwrap();
            operation.resolve(None).unwrap();
            operation.wait_ready().await.unwrap();
            let transaction = PreparedTransaction {
                state_edits: Vec::new(),
                options: None,
                history_changed: true,
                boundary: false,
                publication: CheckpointPublication::default(),
                publish: Box::new(|_, _| panic!("closed edit must not publish")),
                finalize: Box::new(|_| panic!("closed edit must not finalize")),
            };
            let edit = async move {
                guard
                    .commit_prepared_transaction_held(
                        operation,
                        Some(HistorySnapshot::new(Vec::new())),
                        transaction,
                    )
                    .await
                    .map(|(held, _, _)| drop(held))
            };
            let mut edit = Box::pin(edit);
            assert!(future::poll_once(&mut edit).await.is_none());
            if shutdown {
                actor.shutdown();
            } else {
                actor.close();
            }
            runner.await;
            let result = future::poll_once(edit)
                .await
                .expect("queued edit must resolve after close");
            assert!(matches!(
                (shutdown, result),
                (false, Err(ActorError::Closed)) | (true, Err(ActorError::Shutdown))
            ));
        });
    }

    #[test]
    fn edit_response_waits_for_runner_cleanup() {
        smol::block_on(async {
            const DRAFT: &str = "draft";
            let (actor, runner) = AgentActorHandle::spawn(
                AgentId::generate(),
                vec![Message::user(DRAFT.into())],
                None,
                Box::new(IdleBackend),
            );
            actor
                .initialize_config(EffectiveAgentConfig::new(policy(false), AgentMode::Build))
                .unwrap();
            let (executed, execution) = flume::bounded(1);
            let (release, released) = flume::bounded(1);
            *actor.inner.after_work_execution.lock().unwrap() = Some((executed, released));
            let guard = actor.prepare_idle().unwrap();
            let operation = guard.reserve_prepared_operation(None).unwrap();
            operation.resolve(None).unwrap();
            operation.wait_ready().await.unwrap();
            let transaction = PreparedTransaction {
                state_edits: Vec::new(),
                options: None,
                history_changed: true,
                boundary: false,
                publication: CheckpointPublication::default(),
                publish: Box::new(|snapshot, publication| {
                    Box::pin(async move {
                        assert_eq!(snapshot.history.messages.len(), 1);
                        assert!(publication.try_publish());
                        Ok(())
                    })
                }),
                finalize: Box::new(|_| {}),
            };
            let mut edit = Box::pin(async move {
                guard
                    .commit_prepared_transaction_held(operation, None, transaction)
                    .await
                    .map(|(held, _, _)| drop(held))
            });
            assert!(future::poll_once(&mut edit).await.is_none());
            execution.recv_async().await.unwrap();
            let premature = future::poll_once(&mut edit).await;
            let premature_idle = premature.as_ref().map(|_| actor.prepare_idle().is_ok());
            release.send(()).unwrap();
            if premature.is_none() {
                edit.await.unwrap();
            }
            let idle = actor.prepare_idle();
            let idle_after_response = idle.is_ok();
            drop(idle);
            actor.close();
            runner.await;
            assert!(
                premature.is_none(),
                "edit response must not precede runner cleanup: idle acquisition was {premature_idle:?}"
            );
            assert!(idle_after_response);
        });
    }

    #[test]
    fn held_prepared_commit_waits_for_presentation_permission() {
        smol::block_on(async {
            const IMPLEMENT: &str = "implement after presentation";
            let (actor, runner) = AgentActorHandle::spawn(
                AgentId::generate(),
                Vec::new(),
                None,
                Box::new(ScriptedBackend::new()),
            );
            actor
                .initialize_config(EffectiveAgentConfig::new(policy(false), AgentMode::Build))
                .unwrap();
            let idle = actor.prepare_idle().unwrap();
            let operation = idle
                .reserve_prepared_operation(Some(PreparedTurn {
                    input: input(IMPLEMENT),
                    event_sender: None,
                    correlation: IMPLEMENT.into(),
                }))
                .unwrap();
            operation.resolve(None).unwrap();
            operation.wait_ready().await.unwrap();
            let (published, publication) = flume::bounded(1);
            let transaction = PreparedTransaction {
                state_edits: Vec::new(),
                options: None,
                history_changed: true,
                boundary: false,
                publication: CheckpointPublication::default(),
                publish: Box::new(move |snapshot, token| {
                    Box::pin(async move {
                        assert!(snapshot.history.messages.is_empty());
                        assert!(token.try_publish());
                        published.send(()).unwrap();
                        Ok(())
                    })
                }),
                finalize: Box::new(move |_| publication.try_recv().unwrap()),
            };
            let (guard, committed, history) = idle
                .commit_prepared_transaction_held(operation, None, transaction)
                .await
                .unwrap();
            let ticket = committed.ticket.unwrap();
            assert!(history.is_none());
            actor.runtime_barrier().await.unwrap();
            assert!(ticket.peek_result().is_none());
            assert_eq!(actor.committed_message_count(), 0);
            guard.allow_turn(&ticket).unwrap();
            assert!(matches!(
                ticket.wait().await,
                crate::TurnOutcome::Completed { .. }
            ));
            assert_eq!(actor.committed_message_count(), 1);
            drop(guard);
            actor.close();
            runner.await;
        });
    }

    struct IdleBackend;

    impl ActorBackend for IdleBackend {
        fn run_turn<'a>(
            &'a mut self,
            mut history: ActorHistory<'a>,
            context: TurnContext,
            _: AgentInput,
            _: WorkKind,
        ) -> Pin<Box<dyn Future<Output = BackendResult> + Send + 'a>> {
            Box::pin(async move {
                let inbox = context.inbox.unwrap();
                inbox.commit_completions(&mut |message| history.push_observation(message).unwrap());
                assert!(inbox.finish());
                BackendResult::EnteredRun(TurnOutcome::completed(
                    context.agent_id,
                    context.turn_id.unwrap(),
                    TokenUsage::default(),
                    0,
                    DoneReason::EndTurn,
                ))
            })
        }

        fn run_control<'a>(
            &'a mut self,
            _: ActorHistory<'a>,
            _: TurnContext,
            _: &'a ControlWork,
        ) -> Pin<Box<dyn Future<Output = BackendResult> + Send + 'a>> {
            Box::pin(async { unreachable!() })
        }

        fn run_compact<'a>(
            &'a mut self,
            mut history: ActorHistory<'a>,
            _: TurnContext,
            _: Option<&'a str>,
        ) -> Pin<Box<dyn Future<Output = BackendResult> + Send + 'a>> {
            Box::pin(async move {
                history
                    .replace_compacted(history.as_slice().to_vec())
                    .await
                    .unwrap();
                BackendResult::CompactDone
            })
        }
    }

    #[test_case(false, false; "preserve_history")]
    #[test_case(true, false; "clear_history")]
    #[test_case(true, true; "clear_empty_history")]
    fn prepared_commit_preserves_identity(clear: bool, empty: bool) {
        smol::block_on(async {
            let messages = if empty {
                Vec::new()
            } else {
                vec![Message::user("draft".into())]
            };
            let (actor, runner) =
                AgentActorHandle::spawn(AgentId::generate(), messages, None, Box::new(IdleBackend));
            actor
                .initialize_config(EffectiveAgentConfig::new(policy(false), AgentMode::Build))
                .unwrap();
            let epoch = actor.inner.history.load().epoch;
            let earlier = actor.reserve_prepared_operation(None).unwrap();
            assert!(matches!(actor.prepare_idle(), Err(ActorError::Busy)));
            earlier
                .commit_when_ready(Some(ConfigChange::ToggleFast))
                .await
                .unwrap();
            let guard = actor.prepare_idle().unwrap();
            let source = guard.source_snapshot().unwrap();
            assert!(matches!(
                actor.reserve_prepared_operation(None),
                Err(ActorError::Busy)
            ));
            assert!(Arc::ptr_eq(
                &source.config.config,
                &guard.source_snapshot().unwrap().config.config
            ));
            drop(guard);
            let original = actor.config_snapshot().unwrap();
            let guard = actor.prepare_idle().unwrap();
            let source = guard.source_snapshot().unwrap();
            let operation = guard.reserve_prepared_operation(None).unwrap();
            operation.resolve(None).unwrap();
            operation.wait_ready().await.unwrap();
            assert!(matches!(
                actor.reserve_prepared_operation(None),
                Err(ActorError::Busy)
            ));
            assert!(Arc::ptr_eq(
                &source.config.config,
                &guard.source_snapshot().unwrap().config.config
            ));
            let (saved, checkpointed) = flume::bounded(1);
            let transaction = PreparedTransaction {
                state_edits: Vec::new(),
                options: None,
                history_changed: true,
                boundary: false,
                publication: CheckpointPublication::default(),
                publish: Box::new(move |snapshot, publication| {
                    Box::pin(async move {
                        assert_eq!(snapshot.history.messages.is_empty(), clear || empty);
                        assert!(publication.try_publish());
                        saved.send(snapshot.history).unwrap();
                        Ok(())
                    })
                }),
                finalize: Box::new(|_| {}),
            };
            let replacement = clear.then(|| {
                let mut snapshot = HistorySnapshot::new(Vec::new());
                snapshot.compacted = actor.inner.history.load().compacted;
                snapshot
            });
            let (held, commit, change) = guard
                .commit_prepared_transaction_held(operation, replacement, transaction)
                .await
                .unwrap();
            assert!(Arc::ptr_eq(
                &source.config.config,
                &held.source_snapshot().unwrap().config.config
            ));
            drop(held);
            assert!(Arc::ptr_eq(&original.identity, &commit.config.identity));
            assert_eq!(original.generation, commit.config.generation);
            assert_eq!(change.is_some(), clear);
            if let Some(change) = change {
                let snapshot = checkpointed.recv().unwrap();
                assert_eq!(change.epoch, snapshot.epoch);
                assert!(Arc::ptr_eq(&change.messages, &snapshot.messages));
                let published = actor.inner.history.load();
                assert_eq!(published.epoch, snapshot.epoch);
                assert_eq!(published.compacted, snapshot.compacted);
                assert!(Arc::ptr_eq(&published.messages, &snapshot.messages));
                assert_eq!(change.agent_id, actor.agent_id());
                assert_ne!(change.epoch, epoch);
                assert!(change.messages.is_empty());
                assert!(matches!(change.draft.role, Role::User));
            } else {
                assert_eq!(actor.inner.history.load().epoch, epoch);
            }
            actor.runtime_barrier().await.unwrap();
            assert!(actor.prepare_idle().is_ok());
            actor.close();
            runner.await;
        });
    }

    #[test]
    fn prepared_commit_rejects_another_actor() {
        smol::block_on(async {
            let (actor, runner) = AgentActorHandle::spawn(
                AgentId::generate(),
                Vec::new(),
                None,
                Box::new(IdleBackend),
            );
            let (other, other_runner) = AgentActorHandle::spawn(
                AgentId::generate(),
                Vec::new(),
                None,
                Box::new(IdleBackend),
            );
            let operation = other.reserve_prepared_operation(None).unwrap();
            let transaction = PreparedTransaction {
                state_edits: Vec::new(),
                options: None,
                history_changed: true,
                boundary: false,
                publication: CheckpointPublication::default(),
                publish: Box::new(|_, _| panic!("foreign operation must not publish")),
                finalize: Box::new(|_| panic!("foreign operation must not finalize")),
            };
            let result = actor
                .prepare_idle()
                .unwrap()
                .commit_prepared_transaction_held(
                    operation,
                    Some(HistorySnapshot::new(Vec::new())),
                    transaction,
                )
                .await;
            assert!(matches!(result, Err(ActorError::PolicyCancelled)));
            assert!(actor.prepare_idle().is_ok());
            assert!(other.prepare_idle().is_ok());
            actor.close();
            other.close();
            runner.await;
            other_runner.await;
        });
    }

    #[test_case(false, false; "local_cancel")]
    #[test_case(true, false; "local_cancel_and_close")]
    #[test_case(false, true; "local_cancel_and_shutdown")]
    fn prepared_checkpoint_survives_cancel_and_close(close: bool, shutdown: bool) {
        smol::block_on(async {
            let (actor, runner) = AgentActorHandle::spawn(
                AgentId::generate(),
                vec![Message::user("draft".into())],
                None,
                Box::new(IdleBackend),
            );
            actor
                .initialize_config(EffectiveAgentConfig::new(policy(false), AgentMode::Build))
                .unwrap();
            let guard = actor.prepare_idle().unwrap();
            let turn = (close || shutdown).then(|| PreparedTurn {
                input: AgentInput {
                    message: String::new(),
                    images: Vec::new(),
                    preamble: Vec::new(),
                    prompt: None,
                },
                event_sender: None,
                correlation: "prepared-close".into(),
            });
            let operation = guard.reserve_prepared_operation(turn).unwrap();
            operation.resolve(Some(ConfigChange::ToggleFast)).unwrap();
            operation.wait_ready().await.unwrap();
            let (entered, entry) = flume::bounded(1);
            let (release, released) = flume::bounded(1);
            let transaction = PreparedTransaction {
                state_edits: Vec::new(),
                options: None,
                history_changed: true,
                boundary: false,
                publication: CheckpointPublication::default(),
                publish: Box::new(move |snapshot, publication| {
                    Box::pin(async move {
                        assert!(snapshot.config.fast);
                        assert!(snapshot.history.messages.is_empty());
                        assert!(publication.try_publish());
                        entered.send(()).unwrap();
                        released.recv_async().await.unwrap();
                        Ok(())
                    })
                }),
                finalize: Box::new(|commit| assert!(commit.config.fast)),
            };
            let commit = smol::spawn(guard.commit_prepared_transaction_held(
                operation,
                Some(HistorySnapshot::new(Vec::new())),
                transaction,
            ));
            entry.recv_async().await.unwrap();
            assert!(!actor.effective_config().unwrap().fast);
            assert_eq!(actor.inner.history.load().messages.len(), 1);
            assert!(matches!(actor.prepare_idle(), Err(ActorError::Busy)));
            actor.cancel_local(0);
            if shutdown {
                actor.shutdown();
            } else if close {
                actor.close();
            }
            release.send(()).unwrap();
            let (held, committed, change) = commit.await.unwrap();
            drop(held);
            assert!(committed.config.config.fast);
            assert!(actor.effective_config().unwrap().fast);
            assert!(change.unwrap().messages.is_empty());
            assert!(actor.inner.history.load().messages.is_empty());
            if let Some(ticket) = committed.ticket {
                assert!(ticket.peek_result().is_some());
                ticket.wait().await;
                assert!(actor.inner.queue.remove_turn(ticket.turn_id()).is_none());
            }
            actor.close();
            runner.await;
        });
    }

    #[test_case(true, true; "cancel_before_checkpoint")]
    #[test_case(true, false; "cancel_gated_checkpoint")]
    #[test_case(false, true; "cancel_before_keep_checkpoint")]
    #[test_case(false, false; "cancel_gated_keep_checkpoint")]
    fn cancellable_prepared_commit_releases_reservation(clear_history: bool, cancel_before: bool) {
        smol::block_on(async {
            let (actor, runner) = AgentActorHandle::spawn(
                AgentId::generate(),
                vec![Message::user(CHECKPOINT_ERROR.into())],
                None,
                Box::new(IdleBackend),
            );
            actor
                .initialize_config(EffectiveAgentConfig::new(policy(false), AgentMode::Build))
                .unwrap();
            let history = actor.inner.history.load_full();
            let guard = actor.prepare_idle().unwrap();
            let operation = guard.reserve_prepared_operation(None).unwrap();
            operation.resolve(Some(ConfigChange::ToggleFast)).unwrap();
            operation.wait_ready().await.unwrap();
            let publication = CheckpointPublication::default();
            if cancel_before {
                assert!(publication.cancel());
            }
            let (entered, entry) = flume::bounded(1);
            let transaction = PreparedTransaction {
                state_edits: Vec::new(),
                options: None,
                history_changed: true,
                boundary: false,
                publication: publication.clone(),
                publish: Box::new(move |snapshot, publication| {
                    Box::pin(async move {
                        assert!(snapshot.config.fast);
                        assert_eq!(snapshot.history.messages.is_empty(), clear_history);
                        entered.send(()).unwrap();
                        publication.cancelled().await;
                        Err(CHECKPOINT_ERROR.into())
                    })
                }),
                finalize: Box::new(|_| panic!("cancelled transaction must not finalize")),
            };
            let replacement = clear_history.then(|| HistorySnapshot::new(Vec::new()));
            let commit = smol::spawn(guard.commit_prepared_transaction_held(
                operation,
                replacement,
                transaction,
            ));
            if !cancel_before {
                entry.recv_async().await.unwrap();
                assert!(publication.cancel());
            }
            assert!(matches!(
                commit.await,
                Err(ActorError::PolicyCancelled | ActorError::HistoryCheckpoint(_))
            ));
            drop(actor.prepare_idle().expect(CHECKPOINT_ERROR));
            assert!(!actor.effective_config().unwrap().fast);
            assert!(Arc::ptr_eq(
                &history.messages,
                &actor.inner.history.load().messages
            ));
            assert_eq!(actor.inner.state.lock().unwrap().pending_publications, 0);
            actor.close();
            runner.await;
        });
    }

    #[test_case(false; "published_while_runner_gated")]
    #[test_case(true; "cancelled_while_runner_gated")]
    fn cancellable_prepared_commit_does_not_need_runner(cancel: bool) {
        smol::block_on(async {
            let (start, gate) = flume::bounded(1);
            let (actor, runner) = AgentActorHandle::spawn_inner(
                AgentId::generate(),
                vec![Message::user(CHECKPOINT_ERROR.into())],
                None,
                Box::new(IdleBackend),
                None,
                None,
                Some(gate),
            );
            actor
                .initialize_config(EffectiveAgentConfig::new(policy(false), AgentMode::Build))
                .unwrap();
            let operation = actor.reserve_prepared_operation(None).unwrap();
            operation.resolve(Some(ConfigChange::ToggleFast)).unwrap();
            operation.wait_ready().await.unwrap();
            let transaction = PreparedTransaction {
                state_edits: Vec::new(),
                options: None,
                history_changed: true,
                boundary: false,
                publication: CheckpointPublication::default(),
                publish: Box::new(move |snapshot, publication| {
                    Box::pin(async move {
                        assert!(snapshot.config.fast);
                        assert_eq!(snapshot.history.messages.len(), 1);
                        if cancel {
                            assert!(publication.cancel());
                            return Err(CHECKPOINT_ERROR.into());
                        }
                        assert!(publication.try_publish());
                        Ok(())
                    })
                }),
                finalize: Box::new(|commit| assert!(commit.config.fast)),
            };
            let result = operation.commit_durable(transaction).await;
            assert_eq!(result.is_ok(), !cancel);
            assert_eq!(actor.effective_config().unwrap().fast, !cancel);
            assert_eq!(actor.inner.history.load().messages.len(), 1);
            assert_eq!(actor.inner.state.lock().unwrap().pending_publications, 0);
            start.send(()).unwrap();
            actor.close();
            runner.await;
        });
    }

    #[test_case(false, false, "cancel", false; "config_survives_turn_cancel_preclaim")]
    #[test_case(false, true, "cancel", false; "config_survives_turn_cancel_postclaim")]
    #[test_case(false, false, "cancel_all", false; "config_survives_cancel_all_preclaim")]
    #[test_case(false, false, "close", false; "config_close_preclaim")]
    #[test_case(false, true, "close", false; "config_close_postclaim")]
    #[test_case(false, false, "drop", false; "config_drop_preclaim")]
    #[test_case(false, true, "drop", false; "config_drop_postclaim")]
    #[test_case(false, true, "close", true; "config_claimed_storage_failure")]
    #[test_case(false, false, "unclaimed", false; "config_rejects_unclaimed_success")]
    #[test_case(true, false, "cancel", false; "history_survives_turn_cancel_preclaim")]
    #[test_case(true, true, "cancel", false; "history_survives_turn_cancel_postclaim")]
    #[test_case(true, false, "cancel_all", false; "history_survives_cancel_all_preclaim")]
    #[test_case(true, false, "close", false; "history_close_preclaim")]
    #[test_case(true, true, "close", false; "history_close_postclaim")]
    #[test_case(true, false, "drop", false; "history_drop_preclaim")]
    #[test_case(true, true, "drop", false; "history_drop_postclaim")]
    #[test_case(true, true, "close", true; "history_claimed_storage_failure")]
    fn cancellable_prepared_commit_observes_publication(
        replace_history: bool,
        claimed: bool,
        invalidation: &str,
        fail: bool,
    ) {
        smol::block_on(async {
            let (actor, runner) = AgentActorHandle::spawn(
                AgentId::generate(),
                vec![Message::user(CHECKPOINT_ERROR.into())],
                None,
                Box::new(IdleBackend),
            );
            actor
                .initialize_config(EffectiveAgentConfig::new(policy(false), AgentMode::Build))
                .unwrap();
            let events = actor.subscribe_events();
            let commits = actor.subscribe_config_commits();
            let history = actor.inner.history.load_full();
            let initial = actor.config_snapshot().unwrap();
            let staged_commit = Arc::new(Mutex::new(None));
            let published_commit = Arc::clone(&staged_commit);
            let guard = replace_history.then(|| actor.prepare_idle().unwrap());
            let operation = match &guard {
                Some(guard) => guard.reserve_prepared_operation(None),
                None => actor.reserve_prepared_operation(None),
            }
            .unwrap();
            operation.resolve(Some(ConfigChange::ToggleFast)).unwrap();
            operation.wait_ready().await.unwrap();
            let publication = CheckpointPublication::default();
            let (entered, entry) = flume::bounded(1);
            let (release, released) = flume::bounded(1);
            let unclaimed = invalidation == "unclaimed";
            let (settled, settlement) = flume::bounded(1);
            let (finalized, finalization) = flume::bounded(1);
            let transaction = PreparedTransaction {
                state_edits: Vec::new(),
                options: None,
                history_changed: true,
                boundary: false,
                publication: publication.clone(),
                publish: Box::new(move |snapshot, publication| {
                    Box::pin(async move {
                        assert!(snapshot.config.fast);
                        assert_eq!(snapshot.history.messages.is_empty(), replace_history);
                        assert_eq!(
                            snapshot.commit.generation,
                            initial.generation.wrapping_add(1)
                        );
                        assert!(Arc::ptr_eq(&snapshot.commit.identity, &initial.identity));
                        assert!(snapshot.commit.config.fast);
                        *published_commit.lock().unwrap() = Some(snapshot.commit);
                        if claimed {
                            assert!(publication.try_publish());
                        }
                        entered.send(()).unwrap();
                        released.recv_async().await.unwrap();
                        settled.send(()).unwrap();
                        if fail || (!claimed && !unclaimed && !publication.try_publish()) {
                            Err(CHECKPOINT_ERROR.into())
                        } else {
                            Ok(())
                        }
                    })
                }),
                finalize: Box::new(move |commit| {
                    assert!(commit.config.fast);
                    assert!(commits.is_empty());
                    finalized.send(()).unwrap();
                }),
            };
            let mut commit = Some(Box::pin(async move {
                if let Some(guard) = guard {
                    guard
                        .commit_prepared_transaction_held(
                            operation,
                            Some(HistorySnapshot::new(Vec::new())),
                            transaction,
                        )
                        .await
                        .map(|(guard, _, _)| drop(guard))
                } else {
                    operation.commit_durable(transaction).await.map(|_| ())
                }
            }));
            assert!(
                future::poll_once(commit.as_mut().unwrap().as_mut())
                    .await
                    .is_none()
            );
            entry.recv_async().await.unwrap();
            let later = if replace_history {
                assert!(matches!(
                    actor.reserve_prepared_operation(None),
                    Err(ActorError::Busy)
                ));
                None
            } else {
                Some(actor.reserve_prepared_operation(None).unwrap())
            };
            let mut predecessor = later
                .as_ref()
                .map(|later| Box::pin(later.wait_predecessor()));
            if let (Some(later), Some(predecessor)) = (&later, &mut predecessor) {
                assert!(future::poll_once(predecessor).await.is_none());
                later.resolve(None).unwrap();
                assert!(matches!(
                    later.ready_config(),
                    Err(ActorError::PolicyPending)
                ));
            }
            assert_eq!(actor.inner.state.lock().unwrap().pending_publications, 1);
            assert!(!actor.effective_config().unwrap().fast);
            match invalidation {
                "cancel" => actor.cancel_local(0),
                "cancel_all" => {
                    actor.cancel_all();
                    (None, 0)
                }
                "close" => {
                    actor.close();
                    (None, 0)
                }
                "drop" => {
                    drop(commit.take());
                    (None, 0)
                }
                "unclaimed" => (None, 0),
                _ => unreachable!(),
            };
            assert_eq!(
                publication.is_cancelled(),
                !claimed && matches!(invalidation, "close" | "drop")
            );
            assert!(
                !events
                    .try_iter()
                    .any(|event| matches!(event, crate::actor::ActorEvent::Close { .. }))
            );
            assert_eq!(actor.inner.state.lock().unwrap().pending_publications, 1);
            release.send(()).unwrap();
            settlement.recv_async().await.unwrap();
            let succeeds = !fail && (claimed || invalidation.starts_with("cancel"));
            if let Some(commit) = commit {
                let result = commit.await;
                assert_eq!(result.is_ok(), succeeds);
                if fail {
                    assert!(matches!(result, Err(ActorError::HistoryCheckpoint(_))));
                }
            }
            loop {
                let listener = actor.inner.policy_changed.listen();
                if actor.inner.state.lock().unwrap().pending_publications == 0 {
                    break;
                }
                listener.await;
            }
            assert_eq!(finalization.try_recv().is_ok(), succeeds);
            assert_eq!(actor.effective_config().unwrap().fast, succeeds);
            if succeeds {
                let staged_commit = staged_commit.lock().unwrap().take().unwrap();
                let committed = actor.config_snapshot().unwrap();
                assert_eq!(staged_commit.generation, committed.generation);
                assert!(Arc::ptr_eq(&staged_commit.config, &committed.config));
            }
            if succeeds && replace_history {
                assert!(actor.inner.history.load().messages.is_empty());
            } else {
                assert!(Arc::ptr_eq(
                    &history.messages,
                    &actor.inner.history.load().messages
                ));
            }
            if let (Some(later), Some(predecessor)) = (&later, predecessor) {
                let predecessor = predecessor.await;
                if invalidation == "close" {
                    assert!(matches!(predecessor, Err(ActorError::Closed)));
                } else {
                    let predecessor = predecessor.unwrap();
                    assert_eq!(predecessor.fast, succeeds);
                    assert!(Arc::ptr_eq(
                        &predecessor,
                        &actor.config_snapshot().unwrap().config
                    ));
                    later.wait_ready().await.unwrap();
                    assert_eq!(later.ready_config().unwrap().fast, succeeds);
                }
            }
            if let Some(later) = later
                && invalidation != "close"
            {
                later.commit().unwrap();
            }
            actor.close();
            runner.await;
        });
    }

    #[test]
    fn prepared_checkpoint_failure_preserves_config_and_history() {
        smol::block_on(async {
            let (actor, runner) = AgentActorHandle::spawn(
                AgentId::generate(),
                vec![Message::user("draft".into())],
                None,
                Box::new(IdleBackend),
            );
            actor
                .initialize_config(EffectiveAgentConfig::new(policy(false), AgentMode::Build))
                .unwrap();
            let original = actor.config_snapshot().unwrap();
            let epoch = actor.inner.history.load().epoch;
            let guard = actor.prepare_idle().unwrap();
            let operation = guard.reserve_prepared_operation(None).unwrap();
            operation
                .resolve(Some(ConfigChange::Patch(ConfigPatch {
                    fast: Some(true),
                    ..Default::default()
                })))
                .unwrap();
            operation.wait_ready().await.unwrap();
            let transaction = PreparedTransaction {
                state_edits: Vec::new(),
                options: None,
                history_changed: true,
                boundary: false,
                publication: CheckpointPublication::default(),
                publish: Box::new(|snapshot, _| {
                    Box::pin(async move {
                        assert!(snapshot.config.fast);
                        assert!(snapshot.history.messages.is_empty());
                        Err(CHECKPOINT_ERROR.into())
                    })
                }),
                finalize: Box::new(|_| panic!("failed checkpoint must not finalize")),
            };
            let result = guard
                .commit_prepared_transaction_held(
                    operation,
                    Some(HistorySnapshot::new(Vec::new())),
                    transaction,
                )
                .await;
            assert!(
                matches!(result, Err(ActorError::HistoryCheckpoint(error)) if error == CHECKPOINT_ERROR)
            );
            assert_eq!(
                actor.config_snapshot().unwrap().generation,
                original.generation
            );
            assert!(!actor.effective_config().unwrap().fast);
            assert_eq!(actor.inner.history.load().epoch, epoch);
            assert_eq!(actor.inner.history.load().messages.len(), 1);
            actor.runtime_barrier().await.unwrap();
            assert!(actor.prepare_idle().is_ok());
            actor.close();
            runner.await;
        });
    }

    #[test]
    fn rewind_serializes_concurrent_child_report() {
        smol::block_on(async {
            let (actor, runner) = idle_actor(vec![Message::user("draft".into())]);
            let boundary = actor.history_boundaries().unwrap().remove(0);
            let (entered, entry) = flume::bounded(1);
            let (release, released) = flume::bounded(1);
            let publish: PreparedPublication = Box::new(move |_, publication| {
                Box::pin(async move {
                    entered.send(()).unwrap();
                    released.recv_async().await.unwrap();
                    assert!(publication.try_publish());
                    Ok(())
                })
            });
            let rewound = actor.clone();
            let edit = smol::spawn(async move { rewind(&rewound, &boundary, publish).await });
            entry.recv_async().await.unwrap();
            let turn_id = TurnId::generate();
            let report = ChildDelivery {
                label: None,
                result: TurnResult {
                    outcome: TurnOutcome::cancelled(
                        AgentId::generate(),
                        turn_id,
                        TokenUsage::default(),
                        0,
                        TurnCancellationReason::User,
                    ),
                    text: "child report".into(),
                    output: Vec::new(),
                    provenance: Default::default(),
                    duration_ms: 0,
                },
            };
            super::super::delivery::admit_delivery(&actor.inner, Arc::new(report)).unwrap();
            {
                let mut state = actor.inner.state.lock().unwrap();
                super::super::delivery::schedule(&actor.inner, &mut state);
                assert!(state.deliveries.contains_key(&turn_id));
                assert!(state.pop_work(&actor.inner.queue).is_none());
            }
            assert_eq!(actor.inner.history.load().messages.len(), 1);
            release.send(()).unwrap();
            assert!(edit.await.unwrap().messages.is_empty());
            actor.runtime_barrier().await.unwrap();
            assert_eq!(actor.committed_message_count(), 1);
            assert!(
                actor.inner.history.load().messages[0]
                    .first_text_content()
                    .unwrap()
                    .contains("child report")
            );
            assert!(!actor.withdraw_child_delivery(turn_id));
            actor.close();
            runner.await;
        });
    }

    #[test]
    fn rewind_callback_panic_releases_reservation_without_publication() {
        smol::block_on(async {
            let (actor, runner) = idle_actor(vec![Message::user("draft".into())]);
            let boundary = actor.history_boundaries().unwrap().remove(0);
            let epoch = boundary.epoch;
            let result = rewind(&actor, &boundary, Box::new(|_, _| panic!("callback panic"))).await;
            assert!(matches!(result, Err(ActorError::HistoryCheckpoint(_))));
            assert_eq!(actor.inner.history.load().epoch, epoch);
            assert_eq!(actor.inner.history.load().messages.len(), 1);
            assert!(actor.prepare_idle().is_ok());
            actor.close();
            runner.await;
        });
    }

    #[test]
    fn rewind_checkpoint_failure_restores_history() {
        smol::block_on(async {
            let (actor, runner) = idle_actor(vec![
                Message::user("first".into()),
                Message::user("draft".into()),
            ]);
            let boundary = actor.history_boundaries().unwrap().remove(1);
            let original = actor.inner.history.load_full();
            let result = rewind(
                &actor,
                &boundary,
                Box::new(|_, _| Box::pin(async { Err(CHECKPOINT_ERROR.to_owned()) })),
            )
            .await;
            assert!(matches!(result, Err(ActorError::HistoryCheckpoint(_))));
            assert_eq!(actor.inner.history.load().epoch, original.epoch);
            assert_eq!(actor.inner.history.load().messages.len(), 2);
            let changed = rewind(&actor, &boundary, published()).await.unwrap();
            assert_eq!(changed.messages.len(), 1);
            assert_ne!(changed.epoch, original.epoch);
            let stale = rewind(&actor, &boundary, published()).await;
            assert!(matches!(stale, Err(ActorError::StaleHistoryBoundary)));
            actor.close();
            runner.await;
        });
    }

    struct EndRecorder(flume::Sender<(Arc<str>, ConversationEnd)>);

    impl Provider for EndRecorder {
        fn stream_message<'a>(
            &'a self,
            _: &'a Model,
            _: &'a [Message],
            _: &'a str,
            _: &'a Value,
            _: &'a flume::Sender<ProviderEvent>,
            _: RequestOptions,
            _: Option<&'a SessionRef>,
            _: &'a Invocation,
            _delivery: RequestDelivery,
        ) -> BoxFuture<'a, Result<StreamResponse, AgentError>> {
            Box::pin(async { unreachable!() })
        }

        fn end_conversation(
            &self,
            conversation_id: &str,
            reason: ConversationEnd,
        ) -> BoxFuture<'_, ()> {
            let _ = self.0.send((conversation_id.into(), reason));
            Box::pin(async {})
        }
    }

    #[test_case(1, Some(ConversationEnd::Rewound) ; "keeping_messages_rewinds")]
    #[test_case(0, Some(ConversationEnd::Closed) ; "keeping_nothing_closes")]
    #[test_case(1, None ; "undelivered_is_not_ended")]
    fn rewind_ends_the_outgoing_conversation_and_close_keeps_the_new_one(
        boundary: usize,
        expected: Option<ConversationEnd>,
    ) {
        smol::block_on(async {
            let (ended_tx, ended_rx) = flume::unbounded();
            let messages = vec![Message::user("first".into()), Message::user("draft".into())];
            let mut seed = HistorySnapshot::new(messages.clone());
            seed.conversation = Some(ConversationState {
                sent: expected.map(|_| messages.len()),
                ..ConversationState::fresh(None)
            });
            let (actor, runner) = AgentActorHandle::spawn(
                AgentId::generate(),
                messages,
                Some(Arc::new(ArcSwap::from_pointee(seed))),
                Box::new(IdleBackend),
            );
            let settings = crate::RunSettings {
                provider: Arc::new(EndRecorder(ended_tx)),
                ..policy(false)
            };
            actor
                .initialize_config(EffectiveAgentConfig::new(settings, AgentMode::Build))
                .unwrap();
            let boundary = actor.history_boundaries().unwrap().remove(boundary);
            rewind(&actor, &boundary, published()).await.unwrap();
            actor.close();
            runner.await;
            drop(actor);
            // Once the actor is gone only a detached end call still holds the
            // provider, and with it the sender.
            let mut reasons = Vec::new();
            while let Ok((_, reason)) = ended_rx.recv_async().await {
                reasons.push(reason);
            }
            assert_eq!(reasons, Vec::from_iter(expected));
        });
    }

    #[test_case(false; "foreign_actor")]
    #[test_case(true; "invalidated_by_compaction")]
    fn rewind_rejects_foreign_or_compacted_boundary(compacted: bool) {
        smol::block_on(async {
            const DRAFT: &str = "draft";
            const RUN: u64 = 1;
            let (actor, runner) = AgentActorHandle::spawn(
                AgentId::generate(),
                vec![Message::user(DRAFT.into())],
                None,
                Box::new(IdleBackend),
            );
            let (other, other_runner) = AgentActorHandle::spawn(
                AgentId::generate(),
                vec![Message::user(DRAFT.into())],
                None,
                Box::new(IdleBackend),
            );
            let boundary = if compacted {
                actor.history_boundaries().unwrap().remove(0)
            } else {
                other.history_boundaries().unwrap().remove(0)
            };
            if compacted {
                actor.push_compact(RUN, None).unwrap();
                actor.runtime_barrier().await.unwrap();
                assert!(actor.inner.history.load().compacted);
                assert_ne!(boundary.epoch, actor.inner.history.load().epoch);
            }
            assert!(matches!(
                actor.rewind_snapshot(&boundary),
                Err(ActorError::StaleHistoryBoundary)
            ));
            actor.close();
            other.close();
            runner.await;
            other_runner.await;
        });
    }

    #[test]
    fn boundary_messages_match_one_committed_snapshot() {
        smol::block_on(async {
            const FIRST: &str = "first";
            const NEXT: &str = "next";
            let messages = vec![
                Message::user(FIRST.into()),
                Message {
                    role: Role::Assistant,
                    content: vec![ContentBlock::Text {
                        text: "answer".into(),
                    }],
                    ..Default::default()
                },
                Message::user(NEXT.into()),
            ];
            let (actor, runner) = idle_actor(messages);
            let boundaries = actor.history_boundary_messages().unwrap();
            assert_eq!(
                boundaries
                    .iter()
                    .map(|(boundary, message)| (
                        boundary.offset,
                        message.first_text_content().unwrap()
                    ))
                    .collect::<Vec<_>>(),
                vec![(0, FIRST), (2, NEXT)]
            );
            assert!(
                boundaries
                    .iter()
                    .all(|(boundary, _)| boundary.agent_id == actor.agent_id()
                        && boundary.epoch == boundaries[0].0.epoch)
            );
            let boundary = boundaries[1].0.clone();
            rewind(&actor, &boundary, published()).await.unwrap();
            assert!(actor.prepare_idle().is_ok());
            assert_eq!(boundaries[1].1.first_text_content(), Some(NEXT));
            assert!(matches!(
                actor.history_boundary_message(&boundary),
                Err(ActorError::StaleHistoryBoundary)
            ));
            actor.close();
            runner.await;
        });
    }

    #[test]
    fn rewind_boundaries_preserve_complete_tool_groups() {
        let messages = vec![
            Message::user("first".into()),
            Message {
                role: Role::Assistant,
                content: vec![ContentBlock::tool_use(
                    "tool",
                    "read",
                    serde_json::json!({}),
                )],
                ..Default::default()
            },
            Message::user("would split group".into()),
        ];
        assert!(valid_boundary(&messages, 0));
        assert!(!valid_boundary(&messages, 1));
        assert!(!valid_boundary(&messages, 2));
        assert!(!valid_boundary(&messages, 3));
    }
}
