use std::path::PathBuf;
use std::sync::Arc;

use makima_agent::actor::{
    ActorIdleGuard, ActorSourceSnapshot, ConfigChange, ConfigCommit, HistoryChange,
    PreparedOperationTicket, PreparedTransaction, PreparedTurn, TurnTicket,
};
use makima_agent::config_change::prepare_selection;
use makima_agent::rewind::RewindBoundary;
use makima_agent::session_checkpoint::stored_agent_config;
use makima_agent::session_options::{FAST_PENDING_MODEL_OPTION_ID, SessionOptionError};
use makima_agent::{
    AgentId, AgentInput, AgentMode, AgentRef, CancelToken, EffectiveAgentConfig, EventSender,
};
use makima_providers::{Message, ModelSelection, PreparedProvider};
use makima_storage::checkpoint::CheckpointPublication;
use makima_storage::session_state::ParentLink;
use makima_storage::sessions::{HistorySnapshot, StoredRule};

use super::{
    EventLoop, InternalEvent, SESSION_OP_TIMEOUT, SessionLockState, SessionRuntime,
    bounded_session_op,
};
use crate::AppSession;
use crate::components::{SessionReplacementKind, SessionReplacementRequest};
use crate::plan_approval::{
    APPROVAL_CANCELLED, APPROVAL_CHANGED, APPROVAL_LOCK_LOST, ApprovedPlan, idle_error_message,
    read_plan,
};

#[cfg(test)]
mod tests;

const APPROVAL_CHECKPOINT_TIMEOUT: &str =
    "Plan approval timed out before its checkpoint was published";
const IMPLEMENTATION_CORRELATION: &str = "plan-implementation";

pub(super) type HistoryTransaction =
    Box<dyn FnOnce(Arc<EffectiveAgentConfig>, bool) -> Result<PreparedTransaction, String> + Send>;

pub(super) struct PendingPlanApproval {
    pub(super) identity: Arc<()>,
    pub(super) cancel: Option<makima_agent::CancelTrigger>,
    pub(super) waiting_for_lock: Option<ApprovalStep>,
    committing: bool,
    target: AgentId,
    publication: CheckpointPublication,
}

impl Drop for PendingPlanApproval {
    fn drop(&mut self) {
        self.publication.cancel();
        if let Some(cancel) = self.cancel.take() {
            cancel.cancel();
        }
    }
}

pub(super) enum ApprovalStep {
    Prepared(Box<PreparedPlanApproval>),
    Committed(Box<CommittedPlanApproval>),
}

pub(super) struct PreparedPlanApproval {
    target: AgentRef,
    request: PlanApprovalRequest,
    source: ActorSourceSnapshot,
    plan: ApprovedPlan,
    operation: PreparedOperationTicket,
    idle: ActorIdleGuard,
    transaction: PreparedTransaction,
    linked_config: Option<EffectiveAgentConfig>,
}

pub(super) struct CommittedPlanApproval {
    target: AgentRef,
    request: PlanApprovalRequest,
    plan: ApprovedPlan,
    config: ConfigCommit,
    history: Option<HistoryChange>,
    presentation: Option<ApprovalPresentation>,
    linked: Option<(EffectiveAgentConfig, RewindBoundary, usize)>,
}

struct ApprovalPresentation {
    idle: Option<ActorIdleGuard>,
    ticket: TurnTicket,
    target: AgentRef,
}

impl ApprovalPresentation {
    fn release(mut self) -> Result<(), String> {
        let idle = self
            .idle
            .take()
            .ok_or_else(|| APPROVAL_CHANGED.to_owned())?;
        if let Err(error) = idle.allow_turn(&self.ticket) {
            if let Ok(actor) = self.target.actor() {
                let _ = actor.cancel_turn(self.ticket.turn_id());
            }
            return Err(error.to_string());
        }
        Ok(())
    }
}

impl Drop for ApprovalPresentation {
    fn drop(&mut self) {
        if self.idle.is_some()
            && let Ok(actor) = self.target.actor()
        {
            let _ = actor.cancel_turn(self.ticket.turn_id());
        }
    }
}

struct PlanApprovalRequest {
    path: PathBuf,
    selection: ModelSelection,
    parallel: bool,
    clear_context: bool,
    linked: bool,
    run_id: u64,
    yolo: Option<bool>,
    rules: Vec<StoredRule>,
}

impl SessionRuntime {
    pub(super) fn begin_plan_approval(&mut self, pending: PendingPlanApproval) {
        self.pending_approval = Some(pending);
        self.app.plan_approval_pending = true;
    }

    pub(super) fn end_plan_approval(&mut self) {
        if let Some(pending) = self.pending_approval.as_ref()
            && pending.committing
        {
            pending.publication.cancel();
            return;
        }
        if let Some(pending) = self.pending_approval.take() {
            self.app.clear_plan_approval_pending(pending.target);
        } else {
            self.app.plan_approval_pending = false;
        }
    }

    pub(super) fn settle_plan_approval(&mut self) {
        if !self.app.plan_approval_pending
            && !self.pending_approval.as_ref().is_some_and(|pending| {
                pending.committing || self.app.selected_agent_id() != Some(pending.target)
            })
        {
            self.pending_approval = None;
        }
    }

    fn abort_plan_approval(&mut self, message: String) {
        self.end_plan_approval();
        self.app.flash(message);
    }

    fn check_plan_approval(&self, prepared: &PreparedPlanApproval) -> Result<(), String> {
        let source = prepared
            .idle
            .source_snapshot()
            .map_err(|_| APPROVAL_CHANGED.to_owned())?;
        let permissions = self.app.permissions.snapshot();
        if self.session_lock.is_none()
            || source.agent_id != prepared.target.id()
            || !Arc::ptr_eq(&source.config.identity, &prepared.source.config.identity)
            || source.config.generation != prepared.source.config.generation
            || !Arc::ptr_eq(&source.config.config, &prepared.source.config.config)
            || !self.app.plan_approval_matches(
                prepared.target.id(),
                &prepared.request.selection,
                prepared.request.parallel,
            )
            || prepared.request.yolo != permissions.yolo
            || prepared.request.rules != permissions.stored_rules()
        {
            return Err(APPROVAL_CHANGED.into());
        }
        Ok(())
    }
}

fn implementation_turn(message: String, sender: Option<EventSender>) -> PreparedTurn {
    PreparedTurn {
        input: AgentInput::new(message, Vec::new()),
        event_sender: sender,
        correlation: IMPLEMENTATION_CORRELATION.into(),
    }
}

impl EventLoop<'_> {
    pub(super) fn complete_plan_approval(
        &mut self,
        idx: usize,
        result: Result<ApprovalStep, String>,
    ) {
        if let Some(pending) = self.sessions[idx].pending_approval.as_mut() {
            pending.committing = false;
        }
        let prepared = match result {
            Ok(ApprovalStep::Committed(committed)) => {
                self.finish_plan_approval(idx, *committed);
                return;
            }
            Ok(ApprovalStep::Prepared(prepared)) => prepared,
            Err(error) => {
                let runtime = &mut self.sessions[idx];
                return if runtime.lock_lost {
                    runtime.end_plan_approval()
                } else {
                    runtime.abort_plan_approval(error)
                };
            }
        };
        let runtime = &mut self.sessions[idx];
        if runtime.lock_lost {
            return runtime.end_plan_approval();
        }
        if let Err(error) = runtime.check_plan_approval(&prepared) {
            return runtime.abort_plan_approval(error);
        }
        if matches!(runtime.session_lock, Some(SessionLockState::InFlight(_))) {
            if let Some(pending) = runtime.pending_approval.as_mut() {
                pending.waiting_for_lock = Some(ApprovalStep::Prepared(prepared));
            }
            return;
        }
        self.commit_plan_approval(idx, *prepared);
    }

    fn commit_plan_approval(&mut self, idx: usize, prepared: PreparedPlanApproval) {
        self.commit_plan_approval_with_deadline(idx, prepared, async {
            smol::Timer::after(SESSION_OP_TIMEOUT).await;
        });
    }

    fn commit_plan_approval_with_deadline(
        &mut self,
        idx: usize,
        prepared: PreparedPlanApproval,
        deadline: impl Future<Output = ()> + Send + 'static,
    ) {
        if let Err(error) = self
            .target_actor(idx, &prepared.target)
            .and_then(|_| self.sessions[idx].check_plan_approval(&prepared))
        {
            return self.sessions[idx].abort_plan_approval(error);
        }
        if !prepared.request.linked
            && let Err(error) = prepared.operation.set_turn_displayed(true)
        {
            return self.sessions[idx].abort_plan_approval(error.to_string());
        }
        let runtime = &mut self.sessions[idx];
        let Some(pending) = runtime.pending_approval.as_mut() else {
            return;
        };
        pending.committing = true;
        pending.cancel.take();
        let publication = pending.publication.clone();
        let request_identity = Arc::clone(&pending.identity);
        let session = runtime.id();
        let identity = runtime.handles.identity();
        let internal_tx = self.internal_tx.clone();
        smol::spawn(async move {
            let PreparedPlanApproval {
                target,
                request,
                source,
                plan,
                idle,
                operation,
                transaction,
                linked_config,
            } = prepared;
            let replacement =
                (request.clear_context && !request.linked).then(|| HistorySnapshot::<Message> {
                    epoch: source.history.epoch.wrapping_add(1),
                    compacted: false,
                    conversation: None,
                    messages: Arc::new(Vec::new()),
                });
            let observer = async move {
                let (idle, committed, history) = idle
                    .commit_prepared_transaction_held(operation, replacement, transaction)
                    .await
                    .map_err(|error| error.to_string())?;
                let (presentation, linked) = if let Some(config) = linked_config {
                    let boundary = committed
                        .boundary
                        .ok_or_else(|| APPROVAL_CHANGED.to_owned())?;
                    let message_count = source.history.messages.len();
                    drop(idle);
                    (None, Some((config, boundary, message_count)))
                } else {
                    let ticket = committed
                        .ticket
                        .ok_or_else(|| APPROVAL_CHANGED.to_owned())?;
                    (
                        Some(ApprovalPresentation {
                            idle: Some(idle),
                            ticket,
                            target: target.clone(),
                        }),
                        None,
                    )
                };
                Ok(ApprovalStep::Committed(Box::new(CommittedPlanApproval {
                    presentation,
                    linked,
                    target,
                    request,
                    plan,
                    config: committed.config,
                    history,
                })))
            };
            let mut observer = Box::pin(observer);
            let result =
                match futures_lite::future::or(async { Some(observer.as_mut().await) }, async {
                    deadline.await;
                    None
                })
                .await
                {
                    Some(result) => result,
                    None => {
                        let cancelled = publication.cancel();
                        let result = observer.await;
                        if cancelled {
                            Err(APPROVAL_CHECKPOINT_TIMEOUT.into())
                        } else {
                            result
                        }
                    }
                };
            let _ = internal_tx.send(InternalEvent::PlanApprovalReady {
                session,
                runtime: identity,
                request: request_identity,
                result,
            });
        })
        .detach();
    }

    fn finish_plan_approval(&mut self, idx: usize, committed: CommittedPlanApproval) {
        let runtime = &mut self.sessions[idx];
        let CommittedPlanApproval {
            target,
            request,
            plan,
            config,
            history,
            presentation,
            linked,
        } = committed;
        if let Some((config, boundary, message_count)) = linked {
            runtime.app.clear_plan_approval_pending(target.id());
            runtime.pending_approval.take();
            let mut session = AppSession::new(
                &config.model.spec(),
                &config.context.cwd().to_string_lossy(),
            );
            session.set_parent(Some(ParentLink {
                session: runtime.id(),
                message_count,
                seq: boundary.seq,
            }));
            session.meta.yolo = request.yolo;
            session.meta.session_rules = request.rules;
            session.meta.session_options = runtime.coordinator.read().options().config_values();
            if request.selection.spec.is_some() || request.selection.fast.is_some() {
                session
                    .meta
                    .session_options
                    .remove(FAST_PENDING_MODEL_OPTION_ID);
            }
            session.meta.input_draft = Some(plan.message.clone());
            let root = AgentId::generate();
            let prepared = (|| -> Result<_, String> {
                session
                    .bind_root_id(root)
                    .map_err(|error| error.to_string())?;
                session
                    .resolve_legacy_config(
                        stored_agent_config(&config, session.meta.session_options.clone())
                            .map_err(|error| error.to_string())?,
                    )
                    .map_err(|error| error.to_string())?;
                runtime.app.checkpoint_now();
                Ok(session)
            })();
            match prepared {
                Ok(session) => self.handle_action(
                    idx,
                    crate::components::Action::ReplaceSession(Box::new(
                        SessionReplacementRequest {
                            session,
                            kind: SessionReplacementKind::Linked,
                            provider: Some(PreparedProvider {
                                model: config.model.clone(),
                                provider: Arc::clone(&config.provider),
                            }),
                        },
                    )),
                ),
                Err(error) => self.sessions[idx].app.flash(error),
            }
            return;
        }
        if let Some(history) = history {
            let draft = runtime.app.input_box.buffer.value().to_owned();
            runtime.app.apply_history_change(history);
            runtime.app.input_box.set_input(draft);
        }
        let (_, root) = runtime.handles.manager_and_root();
        if target.id() == root {
            runtime.project_config(&config);
            runtime.reset_run_notifications();
            runtime.app.record_plan_implementation(
                request.run_id,
                plan.content,
                plan.path.display().to_string(),
                plan.message,
            );
            runtime.app.record_recent_model(&config.config.model.spec());
        } else if let Some(chat) = runtime
            .app
            .chats
            .iter_mut()
            .find(|chat| chat.agent_id == Some(target.id()))
        {
            chat.push(crate::components::DisplayMessage::plan(
                plan.content,
                plan.path.display().to_string(),
            ));
            chat.show_user_message(plan.message, Vec::new());
        }
        runtime.app.finish_child_plan_approval(target.id());
        runtime.pending_approval.take();
        if let Some(presentation) = presentation
            && let Err(error) = presentation.release()
        {
            runtime.app.flash(error);
        }
    }

    fn spawn_approval_step<F>(
        &mut self,
        idx: usize,
        target: AgentId,
        publication: CheckpointPublication,
        step: F,
    ) where
        F: Future<Output = Result<ApprovalStep, String>> + Send + 'static,
    {
        let runtime = &mut self.sessions[idx];
        let identity = Arc::new(());
        let (trigger, cancel) = CancelToken::new();
        runtime.begin_plan_approval(PendingPlanApproval {
            identity: Arc::clone(&identity),
            cancel: Some(trigger),
            waiting_for_lock: None,
            committing: false,
            target,
            publication,
        });
        let session = runtime.id();
        let runtime_identity = runtime.handles.identity();
        let internal_tx = self.internal_tx.clone();
        smol::spawn(async move {
            let result = cancel
                .race(bounded_session_op(step, SESSION_OP_TIMEOUT))
                .await
                .unwrap_or_else(|_| Err(APPROVAL_CANCELLED.into()));
            let _ = internal_tx.send(InternalEvent::PlanApprovalReady {
                session,
                runtime: runtime_identity,
                request: identity,
                result,
            });
        })
        .detach();
    }

    pub(super) fn prepare_plan_approval(
        &mut self,
        idx: usize,
        clear_context: bool,
        selection: ModelSelection,
        parallel: bool,
        path: PathBuf,
    ) {
        if self.sessions[idx].pending_approval.is_some() {
            return;
        }
        let prepared = (|| -> Result<_, String> {
            let runtime = &self.sessions[idx];
            if runtime.lock_lost || runtime.session_lock.is_none() {
                return Err(APPROVAL_LOCK_LOST.into());
            }
            let selected = runtime
                .app
                .selected_agent_id()
                .ok_or_else(|| APPROVAL_CHANGED.to_owned())?;
            let (manager, _) = runtime.handles.manager_and_root();
            let idle = manager
                .prepare_idle_subtree(selected)
                .map_err(idle_error_message)?;
            let target = manager.lookup(selected).map_err(idle_error_message)?;
            let actor = self.target_actor(idx, &target)?;
            let template = makima_lua::orchestration::OrchestrationServices::template(
                &*self.orchestration,
                &target,
            )?;
            let mode = template.modes.current(&AgentMode::Build);
            actor
                .validate_mode_change(&AgentMode::Build, &mode)
                .map_err(|error| error.to_string())?;
            let source = idle.source_snapshot().map_err(|error| error.to_string())?;
            if source.config.config.mode.plan_path() != Some(path.as_path()) {
                return Err(APPROVAL_CHANGED.into());
            }
            let (_, root) = runtime.handles.manager_and_root();
            let sender =
                (target.id() == root).then(|| EventSender::new(runtime.handles.agent_tx.clone()));
            let linked = clear_context && target.id() == root;
            let operation = idle
                .reserve_prepared_operation(
                    (!linked).then(|| implementation_turn(String::new(), sender)),
                )
                .map_err(|error| error.to_string())?;
            let permissions = runtime.app.permissions.snapshot();
            let request = PlanApprovalRequest {
                path,
                selection,
                parallel,
                clear_context,
                linked,
                run_id: operation.presentation_id(),
                yolo: permissions.yolo,
                rules: permissions.stored_rules(),
            };
            let publication = CheckpointPublication::default();
            operation
                .register_publication(&publication)
                .map_err(|error| error.to_string())?;
            let transaction =
                self.reserve_history_transaction(idx, &target, publication.clone())?;
            Ok((
                target,
                request,
                source,
                template,
                mode,
                idle,
                operation,
                publication,
                transaction,
            ))
        })();
        let (target, request, source, template, mode, idle, operation, publication, transaction) =
            match prepared {
                Ok(prepared) => prepared,
                Err(error) => return self.sessions[idx].abort_plan_approval(error),
            };
        let absolute = if request.path.is_absolute() {
            request.path.clone()
        } else {
            source.config.config.context.cwd().join(&request.path)
        };
        let prepare = Arc::clone(&self.ctx.prepare_provider);
        self.spawn_approval_step(idx, target.id(), publication, async move {
            let parallel = request.parallel;
            let plan = smol::unblock(move || read_plan(absolute, parallel)).await?;
            if !request.linked {
                operation
                    .replace_turn_input(AgentInput::new(plan.message.clone(), Vec::new()))
                    .map_err(|error| error.to_string())?;
            }
            let patch = prepare_selection(&request.selection, &template.model_policy, &prepare)
                .await
                .map_err(|error| error.to_string())?;
            let (change, linked_config) = if request.linked {
                let mut stored = stored_agent_config(&source.config.config, Default::default())
                    .map_err(|error| error.to_string())?;
                let prepared = patch.model.unwrap_or_else(|| PreparedProvider {
                    model: source.config.config.model.clone(),
                    provider: Arc::clone(&source.config.config.provider),
                });
                if patch.fast == Some(true) && !prepared.model.supports_fast() {
                    return Err(SessionOptionError::FastUnsupported.to_string());
                }
                if patch.thinking.is_some_and(|thinking| thinking.is_enabled())
                    && prepared.model.supports_thinking() != Some(true)
                {
                    return Err(SessionOptionError::ThinkingUnsupported.to_string());
                }
                stored.model = prepared.model.spec();
                if let Some(thinking) = patch.thinking {
                    stored.thinking = Some(thinking.into());
                }
                if let Some(fast) = patch.fast {
                    stored.fast = fast;
                }
                let config =
                    EffectiveAgentConfig::from_stored(&stored, prepared, (AgentMode::Build, mode))
                        .map_err(|error| error.to_string())?;
                (None, Some(config))
            } else {
                (
                    Some(ConfigChange::PatchAndMode {
                        patch,
                        mode: AgentMode::Build,
                        mode_def: mode,
                    }),
                    None,
                )
            };
            operation
                .resolve(change)
                .map_err(|error| error.to_string())?;
            operation
                .wait_ready()
                .await
                .map_err(|error| error.to_string())?;
            let state_edit = makima_agent::plan_state::approval_edit(
                &target,
                &request.path,
                plan.content.clone(),
                request.selection.clone(),
            )
            .map_err(|error| error.to_string())?;
            let transaction = transaction(
                operation
                    .ready_config()
                    .map_err(|error| error.to_string())?,
                request.clear_context && !request.linked,
            )?
            .with_state_edit(state_edit);
            let transaction = if request.linked {
                transaction.with_boundary()
            } else {
                transaction
            };
            Ok(ApprovalStep::Prepared(Box::new(PreparedPlanApproval {
                target,
                request,
                source,
                plan,
                idle,
                operation,
                transaction,
                linked_config,
            })))
        });
    }
}
