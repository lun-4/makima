use std::collections::HashSet;
use std::sync::Arc;

use maki_providers::{ContentBlock, Message, MessageKind, Role};
use maki_storage::sessions::HistorySnapshot;

use super::prepared::{CancelPublication, PreparedOperation};
use super::{
    ActorError, ActorIdleGuard, ActorLifecycle, AgentActorHandle, PreparedCommit,
    PreparedOperationTicket, PreparedTransaction,
};
use crate::{AgentId, History};

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
            if valid && pending.is_empty() && is_boundary_message(message) {
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
                    history.replace_snapshot(snapshot.clone());
                    HistoryChange {
                        agent_id: actor.agent_id(),
                        epoch: snapshot.epoch,
                        messages: snapshot.messages,
                        draft: Message::user(String::new()),
                    }
                });
                let committed = (*operation).commit_locked(&actor.inner, &mut state, finalize);
                state.end_prepared_publication(id);
                if let Some(reservation) = state.owned_idle(&self.owner) {
                    reservation.committing = false;
                    guard.release = false;
                }
                super::drive_operations(&actor.inner, &mut state);
                Ok((committed, change))
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
    is_boundary_message(message)
        && messages[..offset]
            .iter()
            .all(|message| update_pending_tools(message, &mut pending))
        && pending.is_empty()
}

fn is_boundary_message(message: &Message) -> bool {
    matches!(message.role, Role::User)
        && matches!(message.kind, MessageKind::Turn)
        && !message.content.is_empty()
        && !message
            .content
            .iter()
            .any(|block| matches!(block, ContentBlock::ToolResult { .. }))
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

    use futures_lite::future;
    use maki_providers::{ContentBlock, Message, Role, TokenUsage};
    use maki_storage::checkpoint::CheckpointPublication;
    use maki_storage::sessions::HistorySnapshot;
    use test_case::test_case;

    use super::{HistoryBoundary, HistoryChange, valid_boundary};
    use crate::actor::tests::{ScriptedBackend, input, policy};
    use crate::actor::{
        ActorBackend, ActorError, AgentActorHandle, BackendResult, ChildDelivery, ConfigChange,
        ConfigPatch, ControlWork, EffectiveAgentConfig, PreparedPublication, PreparedTransaction,
        PreparedTurn, TurnContext, TurnResult, WorkKind,
    };
    use crate::{
        AgentId, AgentInput, AgentMode, DoneReason, History, TurnCancellationReason, TurnId,
        TurnOutcome,
    };

    const CHECKPOINT_ERROR: &str = "checkpoint unavailable";

    fn idle_actor(messages: Vec<Message>) -> (AgentActorHandle, smol::Task<()>) {
        let (actor, runner) =
            AgentActorHandle::spawn(AgentId::generate(), messages, None, Box::new(IdleBackend));
        actor
            .initialize_config(EffectiveAgentConfig::new(policy(false), AgentMode::Build))
            .unwrap();
        (actor, runner)
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
            publication: CheckpointPublication::default(),
            publish,
            finalize: Box::new(|_| {}),
        };
        let (_, _, change) = guard
            .commit_prepared_transaction_held(operation, Some(replacement), transaction)
            .await?;
        Ok(change.expect("rewind replaces history"))
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
            history: &'a mut History,
            context: TurnContext,
            _: AgentInput,
            _: WorkKind,
        ) -> Pin<Box<dyn Future<Output = BackendResult> + Send + 'a>> {
            Box::pin(async move {
                let inbox = context.inbox.unwrap();
                inbox.commit_completions(&mut |message| history.push(message));
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
            _: &'a mut History,
            _: TurnContext,
            _: &'a ControlWork,
        ) -> Pin<Box<dyn Future<Output = BackendResult> + Send + 'a>> {
            Box::pin(async { unreachable!() })
        }

        fn run_compact<'a>(
            &'a mut self,
            history: &'a mut History,
            _: TurnContext,
            _: Option<&'a str>,
        ) -> Pin<Box<dyn Future<Output = BackendResult> + Send + 'a>> {
            Box::pin(async move {
                history.replace_compacted(history.as_slice().to_vec());
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
