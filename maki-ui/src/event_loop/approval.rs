use std::path::PathBuf;
use std::sync::Arc;

use maki_agent::CancelToken;
use maki_storage::session_lock::ClaimedSessionLock;
use tracing::warn;

use super::{
    EventLoop, InternalEvent, PreparedProvider, PreparedSessionRuntime, SESSION_OP_TIMEOUT,
    SessionLockState, SessionRuntime, bounded_session_op, claim_lock, release_lock_state,
};
use crate::AppSession;

#[cfg(test)]
mod tests;

pub(super) struct PendingPlanApproval {
    pub(super) identity: Arc<()>,
    pub(super) cancel: Option<maki_agent::CancelTrigger>,
    pub(super) waiting_for_lock: Option<Box<PreparedPlanApproval>>,
}

impl Drop for PendingPlanApproval {
    fn drop(&mut self) {
        if let Some(cancel) = self.cancel.take() {
            cancel.cancel();
        }
    }
}

pub(super) struct PreparedPlanApproval {
    request: PlanApprovalRequest,
    plan: crate::plan_approval::ApprovedPlan,
    operation: maki_agent::actor::PreparedOperationTicket,
    source: maki_agent::session_coordinator::IdleSessionLease,
    idle: maki_agent::manager::IdleSubtreeGuard,
    candidate: Option<PreparedSessionRuntime>,
    candidate_commit: Option<maki_agent::actor::PreparedCommit>,
    candidate_idle: Option<maki_agent::manager::IdleSubtreeGuard>,
    candidate_lock: Option<ClaimedSessionLock>,
}

struct PlanApprovalRequest {
    identity: Arc<()>,
    path: PathBuf,
    model: Option<String>,
    parallel: bool,
    clear_context: bool,
    run_id: u64,
    yolo: Option<bool>,
    rules: Vec<maki_storage::sessions::StoredRule>,
    build_mode: maki_agent::ModeDef,
}

impl EventLoop<'_> {
    pub(super) fn complete_plan_approval(
        &mut self,
        idx: usize,
        result: Result<PreparedPlanApproval, String>,
    ) {
        let runtime = &mut self.sessions[idx];
        let prepared = match result {
            Ok(prepared) => prepared,
            Err(error) => {
                runtime.pending_approval.take();
                runtime.app.plan_approval_pending = false;
                runtime.app.flash(error);
                return;
            }
        };
        if runtime.lock_lost
            || runtime.session_lock.is_none()
            || !runtime
                .app
                .plan_approval_matches(prepared.request.model.as_deref(), prepared.request.parallel)
            || !runtime
                .pending_approval
                .as_ref()
                .is_some_and(|pending| Arc::ptr_eq(&pending.identity, &prepared.request.identity))
            || runtime.app.run_id.wrapping_add(1) != prepared.request.run_id
            || runtime.app.state.plan.path() != Some(prepared.request.path.as_path())
            || prepared.source.revalidate().is_err()
        {
            runtime.pending_approval.take();
            runtime.app.plan_approval_pending = false;
            runtime
                .app
                .flash(crate::plan_approval::APPROVAL_CHANGED.into());
            return;
        }
        if prepared.request.yolo != runtime.app.permissions.persisted_yolo()
            || prepared.request.rules
                != crate::app::session_state::rules_to_stored(
                    &runtime.app.permissions.session_rules_snapshot(),
                )
        {
            self.complete_plan_approval(idx, Err(crate::plan_approval::APPROVAL_CHANGED.into()));
            return;
        }
        if prepared.request.clear_context && prepared.candidate.is_none() {
            self.prepare_fresh_plan_approval(idx, prepared);
            return;
        }
        if matches!(runtime.session_lock, Some(SessionLockState::InFlight(_))) {
            if let Some(pending) = runtime.pending_approval.as_mut() {
                pending.waiting_for_lock = Some(Box::new(prepared));
            }
            return;
        }
        let PreparedPlanApproval {
            request,
            plan,
            operation,
            source,
            idle,
            candidate,
            candidate_commit,
            candidate_idle,
            candidate_lock,
        } = prepared;
        if let Some(mut candidate) = candidate {
            let target = candidate.app.session_id();
            let Some(lock) = candidate_lock else {
                self.complete_plan_approval(
                    idx,
                    Err(crate::plan_approval::APPROVAL_CHANGED.into()),
                );
                return;
            };
            let Some(commit) = candidate_commit else {
                self.complete_plan_approval(
                    idx,
                    Err(crate::plan_approval::APPROVAL_CHANGED.into()),
                );
                return;
            };
            let Some(candidate_idle) = candidate_idle else {
                self.complete_plan_approval(
                    idx,
                    Err(crate::plan_approval::APPROVAL_CHANGED.into()),
                );
                return;
            };
            if commit
                .ticket
                .as_ref()
                .is_none_or(|ticket| ticket.peek().is_some())
            {
                self.complete_plan_approval(
                    idx,
                    Err(crate::plan_approval::APPROVAL_CHANGED.into()),
                );
                return;
            }
            if let Some((_, session)) = candidate.seed_snapshot.as_mut() {
                let session = Arc::make_mut(session);
                session.model = commit.config.config.model.spec();
                session.meta.mode = Some(maki_storage::sessions::StoredMode::Build);
                session.meta.thinking = Some(commit.config.config.thinking.into());
                session.meta.fast = commit.config.config.fast;
                session.meta.workflow = commit.config.config.workflow;
                session.meta.plan_path = None;
                session.meta.plan_written = false;
            }
            let seed = candidate.seed_snapshot.clone();
            let candidate_slot = Arc::clone(&candidate.model_slot);
            let mut next = match candidate.activate_replacing(
                &candidate_slot,
                Some(SessionLockState::Held(lock)),
                &runtime.coordinator,
            ) {
                Ok(next) => next,
                Err((error, lock)) => {
                    if let Err(release_error) = release_lock_state(lock) {
                        warn!(%release_error, "approval candidate lock release failed");
                    }
                    self.complete_plan_approval(idx, Err(error.to_string()));
                    return;
                }
            };
            if let Some((writer, session)) = seed {
                writer.seed(session);
            }
            next.model_slot.project(
                commit.config.config.model.clone(),
                Arc::clone(&commit.config.config.settings.provider),
            );
            next.project_config(&commit.config);
            next.app.exit_on_done = runtime.app.exit_on_done;
            next.app
                .input_box
                .replace_history_from(&mut runtime.app.input_box);
            next.app.record_plan_implementation(
                request.run_id,
                plan.content,
                plan.path.display().to_string(),
                plan.message,
            );
            next.approved_turn = commit.ticket.map(|ticket| (candidate_idle, ticket));
            next.approval_runner_pending = true;
            next.app
                .record_recent_model(&commit.config.config.model.spec());
            let old = std::mem::replace(runtime, next);
            let retired_id = old.id();
            let storage_writer = Arc::clone(&self.ctx.storage_writer);
            let identity = runtime.handles.identity();
            let internal_tx = self.internal_tx.clone();
            smol::spawn(async move {
                let SessionRuntime {
                    handles,
                    coordinator,
                    session_lock,
                    ..
                } = old;
                handles.shutdown().await;
                if let Err(error) = smol::unblock(move || release_lock_state(session_lock)).await {
                    warn!(%error, "outgoing approval session lock release failed");
                }
                drop(operation);
                drop(source);
                drop(idle);
                let _ = coordinator.close().await;
                storage_writer.forget(retired_id);
                let _ = internal_tx.send(InternalEvent::ApprovalRunnerReady {
                    session: target,
                    runtime: identity,
                });
            })
            .detach();
            return;
        }
        match operation.commit() {
            Ok(commit) => {
                runtime.project_config(&commit.config);
                runtime.reset_run_notifications();
                runtime.app.record_plan_implementation(
                    request.run_id,
                    plan.content,
                    plan.path.display().to_string(),
                    plan.message,
                );
                if let Some(ticket) = commit.ticket {
                    if let Err(error) = idle.allow_turn(&ticket) {
                        let (manager, root) = runtime.handles.manager_and_root();
                        if let Ok(actor) = manager.actor(root) {
                            let _ = actor.cancel_turn(ticket.turn_id());
                        }
                        runtime.app.flash(error.to_string());
                    }
                    runtime.approved_turn = Some((idle, ticket));
                }
                runtime.pending_approval.take();
                drop(source);
                self.sessions[idx]
                    .app
                    .record_recent_model(&commit.config.config.model.spec());
            }
            Err(error) => {
                runtime.pending_approval.take();
                runtime.app.plan_approval_pending = false;
                runtime.app.flash(error.to_string());
            }
        }
    }

    fn advance_plan_approval(
        &mut self,
        idx: usize,
        prepared: &mut PreparedPlanApproval,
    ) -> CancelToken {
        let identity = Arc::new(());
        let (trigger, cancel) = CancelToken::new();
        prepared.request.identity = Arc::clone(&identity);
        self.sessions[idx].pending_approval = Some(PendingPlanApproval {
            identity,
            cancel: Some(trigger),
            waiting_for_lock: None,
        });
        cancel
    }

    fn prepare_fresh_plan_approval(&mut self, idx: usize, mut prepared: PreparedPlanApproval) {
        let result = (|| -> Result<_, String> {
            let config = prepared
                .operation
                .ready_config()
                .map_err(|error| error.to_string())?;
            let mut session = AppSession::new(
                &config.model.spec(),
                &prepared.source.snapshot().cwd().to_string_lossy(),
            );
            session.meta.yolo = prepared.request.yolo;
            session.meta.session_rules = prepared.request.rules.clone();
            let provider = PreparedProvider {
                model: config.model.clone(),
                provider: Arc::clone(&config.settings.provider),
            };
            let candidate = self
                .ctx
                .prepare_runtime_with_config(
                    session,
                    Some(provider),
                    &self.sessions[idx].app.permissions,
                    true,
                    Some((*config).clone()),
                )
                .map_err(|error| error.to_string())?;
            let (manager, root) = candidate.handles.manager_and_root();
            let idle = manager
                .prepare_idle_subtree(root)
                .map_err(|error| error.to_string())?;
            let actor = manager.actor(root).map_err(|error| error.to_string())?;
            let turn = maki_agent::actor::PreparedTurn {
                input: maki_agent::AgentInput::from_defaults(
                    prepared.plan.message.clone(),
                    maki_agent::AgentMode::Build,
                    Vec::new(),
                    maki_agent::SessionDefaults::default(),
                ),
                event_sender: Some(maki_agent::EventSender::new(
                    candidate.handles.agent_tx(),
                    prepared.request.run_id,
                )),
                correlation: crate::agent::shared_queue::correlation(prepared.request.run_id),
            };
            let operation = actor
                .reserve_prepared_operation(Some(turn))
                .map_err(|error| error.to_string())?;
            let target = candidate.app.session_id();
            prepared.candidate = Some(candidate);
            Ok((operation, idle, target))
        })();
        let (operation, candidate_idle, target) = match result {
            Ok(result) => result,
            Err(error) => {
                self.complete_plan_approval(idx, Err(error));
                return;
            }
        };
        let cancel = self.advance_plan_approval(idx, &mut prepared);
        let session = self.sessions[idx].id();
        let runtime = self.sessions[idx].handles.identity();
        let request = Arc::clone(&prepared.request.identity);
        let policy = Arc::clone(&self.ctx.model_policy);
        let provider = Arc::clone(&self.ctx.prepare_provider);
        let timeouts = self.ctx.timeouts;
        let mode = prepared.request.build_mode.clone();
        let sessions_dir = self.ctx.sessions_dir.clone();
        #[cfg(test)]
        let lock_prepare_gate = self.ctx.lock_prepare_gate.clone();
        let internal_tx = self.internal_tx.clone();
        smol::spawn(async move {
            let result = cancel
                .race(bounded_session_op(
                    async move {
                        let spec = prepared.request.model.clone();
                        let change = smol::unblock(move || {
                            crate::plan_approval::prepare_change(
                                spec, &policy, timeouts, &provider, mode,
                            )
                        })
                        .await?;
                        operation
                            .resolve(Ok(Some(change)))
                            .map_err(|error| error.to_string())?;
                        operation
                            .wait_ready()
                            .await
                            .map_err(|error| error.to_string())?;
                        let commit = operation.commit().map_err(|error| error.to_string())?;
                        if let Some(ticket) = &commit.ticket {
                            candidate_idle
                                .allow_turn(ticket)
                                .map_err(|error| error.to_string())?;
                        }
                        prepared.candidate_commit = Some(commit);
                        let lock = smol::unblock(move || {
                            #[cfg(test)]
                            let guard = lock_prepare_gate.map(|gate| gate(target));
                            let lock = claim_lock(&sessions_dir, &target)
                                .map_err(|error| error.to_string());
                            #[cfg(test)]
                            {
                                (lock, guard)
                            }
                            #[cfg(not(test))]
                            {
                                lock
                            }
                        })
                        .await;
                        #[cfg(test)]
                        let (lock, _guard) = lock;
                        prepared.candidate_lock = Some(lock?);
                        prepared.candidate_idle = Some(candidate_idle);
                        Ok(prepared)
                    },
                    SESSION_OP_TIMEOUT,
                ))
                .await
                .unwrap_or_else(|_| Err("Implementation preparation cancelled.".into()));
            let _ = internal_tx.send(InternalEvent::PlanApprovalReady {
                session,
                runtime,
                request,
                result: result.map(Box::new),
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
        let runtime = &mut self.sessions[idx];
        if runtime.pending_approval.is_some() {
            return;
        }
        let (manager, root) = runtime.handles.manager_and_root();
        let idle = match manager.prepare_idle_subtree(root) {
            Ok(idle) if !runtime.lock_lost => idle,
            _ => {
                runtime.app.plan_approval_pending = false;
                runtime
                    .app
                    .flash(crate::plan_approval::APPROVAL_BUSY.into());
                return;
            }
        };
        let actor = match manager.actor(root) {
            Ok(actor) => actor,
            Err(error) => {
                runtime.app.plan_approval_pending = false;
                runtime.app.flash(error.to_string());
                return;
            }
        };
        let identity = Arc::new(());
        let request = PlanApprovalRequest {
            identity: Arc::clone(&identity),
            path,
            model,
            parallel,
            clear_context,
            run_id: runtime.app.run_id.wrapping_add(1),
            yolo: runtime.app.permissions.persisted_yolo(),
            rules: crate::app::session_state::rules_to_stored(
                &runtime.app.permissions.session_rules_snapshot(),
            ),
            build_mode: self
                .ctx
                .lua_event_handle
                .mode_registry()
                .current(&maki_agent::AgentMode::Build),
        };
        let absolute = if request.path.is_absolute() {
            request.path.clone()
        } else {
            PathBuf::from(&runtime.app.state.session.cwd).join(&request.path)
        };
        let turn = (!clear_context).then(|| maki_agent::actor::PreparedTurn {
            input: maki_agent::AgentInput::from_defaults(
                String::new(),
                maki_agent::AgentMode::Build,
                Vec::new(),
                maki_agent::SessionDefaults::default(),
            ),
            event_sender: Some(maki_agent::EventSender::new(
                runtime.handles.agent_tx.clone(),
                request.run_id,
            )),
            correlation: crate::agent::shared_queue::correlation(request.run_id),
        });
        let operation = match actor.reserve_prepared_operation(turn) {
            Ok(operation) => operation,
            Err(error) => {
                runtime.app.plan_approval_pending = false;
                runtime.app.flash(error.to_string());
                return;
            }
        };
        let (trigger, cancel) = CancelToken::new();
        runtime.pending_approval = Some(PendingPlanApproval {
            identity: Arc::clone(&identity),
            cancel: Some(trigger),
            waiting_for_lock: None,
        });
        let session = runtime.id();
        let runtime_identity = runtime.handles.identity();
        let coordinator = runtime.coordinator.clone();
        let mode_def = request.build_mode.clone();
        let policy = Arc::clone(&self.ctx.model_policy);
        let prepare = Arc::clone(&self.ctx.prepare_provider);
        let timeouts = self.ctx.timeouts;
        let internal_tx = self.internal_tx.clone();
        smol::spawn(async move {
            let result = cancel
                .race(bounded_session_op(
                    async move {
                        let source = coordinator
                            .try_acquire_idle_lease()
                            .await
                            .map_err(|error| error.to_string())?;
                        let path = if request.path.is_absolute() {
                            request.path.clone()
                        } else {
                            source.snapshot().cwd().join(&request.path)
                        };
                        if path != absolute {
                            return Err(crate::plan_approval::APPROVAL_CHANGED.into());
                        }
                        let parallel = request.parallel;
                        let plan =
                            smol::unblock(move || crate::plan_approval::read_plan(path, parallel))
                                .await?;
                        if !request.clear_context {
                            operation
                                .replace_turn_input(maki_agent::AgentInput::from_defaults(
                                    plan.message.clone(),
                                    maki_agent::AgentMode::Build,
                                    Vec::new(),
                                    maki_agent::SessionDefaults::default(),
                                ))
                                .map_err(|error| error.to_string())?;
                        }
                        let change = if request.clear_context {
                            None
                        } else {
                            let spec = request.model.clone();
                            Some(
                                smol::unblock(move || {
                                    crate::plan_approval::prepare_change(
                                        spec, &policy, timeouts, &prepare, mode_def,
                                    )
                                })
                                .await?,
                            )
                        };
                        operation
                            .resolve(Ok(change))
                            .map_err(|error| error.to_string())?;
                        operation
                            .wait_ready()
                            .await
                            .map_err(|error| error.to_string())?;
                        Ok(PreparedPlanApproval {
                            request,
                            plan,
                            operation,
                            source,
                            idle,
                            candidate: None,
                            candidate_commit: None,
                            candidate_idle: None,
                            candidate_lock: None,
                        })
                    },
                    SESSION_OP_TIMEOUT,
                ))
                .await
                .unwrap_or_else(|_| Err("Implementation preparation cancelled.".into()));
            let _ = internal_tx.send(InternalEvent::PlanApprovalReady {
                session,
                runtime: runtime_identity,
                request: identity,
                result: result.map(Box::new),
            });
        })
        .detach();
    }
}
