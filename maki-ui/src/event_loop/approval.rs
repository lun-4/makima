use std::path::PathBuf;
use std::sync::Arc;

use maki_agent::actor::{
    ActorIdleGuard, ActorSourceSnapshot, ConfigChange, ConfigCommit, ConfigPatch, HistoryChange,
    PreparedOperationTicket, PreparedTransaction, PreparedTurn, TurnTicket,
};
use maki_agent::config_change::prepare_patch;
use maki_agent::{
    AgentId, AgentInput, AgentMode, AgentRef, CancelToken, EffectiveAgentConfig, EventSender,
};
use maki_providers::Message;
use maki_storage::checkpoint::CheckpointPublication;
use maki_storage::sessions::{HistorySnapshot, StoredRule};

use super::{
    EventLoop, InternalEvent, SESSION_OP_TIMEOUT, SessionLockState, SessionRuntime,
    bounded_session_op, model_preparer,
};
use crate::app::session_state::rules_to_stored;
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
    pub(super) cancel: Option<maki_agent::CancelTrigger>,
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
}

pub(super) struct CommittedPlanApproval {
    target: AgentRef,
    request: PlanApprovalRequest,
    plan: ApprovedPlan,
    config: ConfigCommit,
    history: Option<HistoryChange>,
    presentation: ApprovalPresentation,
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
    model: Option<String>,
    parallel: bool,
    clear_context: bool,
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
            let (_, root) = self.handles.manager_and_root();
            if pending.target == root {
                self.app.plan_approval_pending = false;
            } else {
                self.app.finish_child_plan_approval(pending.target);
            }
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
        if self.session_lock.is_none()
            || source.agent_id != prepared.target.id()
            || !Arc::ptr_eq(&source.config.identity, &prepared.source.config.identity)
            || source.config.generation != prepared.source.config.generation
            || !Arc::ptr_eq(&source.config.config, &prepared.source.config.config)
            || (self.app.selected_agent_id() == Some(prepared.target.id())
                && !self.app.plan_approval_matches(
                    prepared.request.model.as_deref(),
                    prepared.request.parallel,
                ))
            || prepared.request.yolo != self.app.permissions.persisted_yolo()
            || prepared.request.rules
                != rules_to_stored(&self.app.permissions.session_rules_snapshot())
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
        if let Err(error) = prepared.operation.set_turn_displayed(true) {
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
            } = prepared;
            let replacement = request.clear_context.then(|| HistorySnapshot::<Message> {
                epoch: source.history.epoch.wrapping_add(1),
                compacted: false,
                messages: Arc::new(Vec::new()),
            });
            let observer = async move {
                let (idle, committed, history) = idle
                    .commit_prepared_transaction_held(operation, replacement, transaction)
                    .await
                    .map_err(|error| error.to_string())?;
                let ticket = committed
                    .ticket
                    .ok_or_else(|| APPROVAL_CHANGED.to_owned())?;
                Ok(ApprovalStep::Committed(Box::new(CommittedPlanApproval {
                    presentation: ApprovalPresentation {
                        idle: Some(idle),
                        ticket,
                        target: target.clone(),
                    },
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
        } = committed;
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
        if let Err(error) = presentation.release() {
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
        model: Option<String>,
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
            let template = maki_lua::orchestration::OrchestrationServices::template(
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
            let operation = idle
                .reserve_prepared_operation(Some(implementation_turn(String::new(), sender)))
                .map_err(|error| error.to_string())?;
            let request = PlanApprovalRequest {
                path,
                model,
                parallel,
                clear_context,
                run_id: operation.presentation_id(),
                yolo: runtime.app.permissions.persisted_yolo(),
                rules: rules_to_stored(&runtime.app.permissions.session_rules_snapshot()),
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
        let prepare = model_preparer(Arc::clone(&self.ctx.prepare_provider), template.timeouts);
        self.spawn_approval_step(idx, target.id(), publication, async move {
            let parallel = request.parallel;
            let plan = smol::unblock(move || read_plan(absolute, parallel)).await?;
            operation
                .replace_turn_input(AgentInput::new(plan.message.clone(), Vec::new()))
                .map_err(|error| error.to_string())?;
            let spec = request.model.as_deref();
            let patch = prepare_patch(
                ConfigPatch::default(),
                spec,
                &template.model_policy,
                &prepare,
            )
            .await
            .map_err(|error| error.to_string())?;
            let change = ConfigChange::PatchAndMode {
                patch,
                mode: AgentMode::Build,
                mode_def: mode,
            };
            operation
                .resolve(Some(change))
                .map_err(|error| error.to_string())?;
            operation
                .wait_ready()
                .await
                .map_err(|error| error.to_string())?;
            let transaction = transaction(
                operation
                    .ready_config()
                    .map_err(|error| error.to_string())?,
                request.clear_context,
            )?;
            Ok(ApprovalStep::Prepared(Box::new(PreparedPlanApproval {
                target,
                request,
                source,
                plan,
                idle,
                operation,
                transaction,
            })))
        });
    }
}
