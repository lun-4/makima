use std::future::Future;
use std::path::PathBuf;
use std::sync::Arc;

use maki_agent::actor::{
    ConfigCommit, PreparedCommit, PreparedOperationTicket, PreparedTurn, TurnTicket,
};
use maki_agent::manager::IdleSubtreeGuard;
use maki_agent::session_coordinator::IdleSessionLease;
use maki_agent::{AgentInput, AgentMode, CancelToken, EventSender, ModeDef, SessionDefaults};
use maki_storage::id::MakiId;
use maki_storage::session_lock::ClaimedSessionLock;
use maki_storage::sessions::{StoredMode, StoredRule};
use tracing::warn;

#[cfg(test)]
use super::LockPrepareGate;
use super::{
    EventLoop, InternalEvent, PreparedProvider, PreparedSessionRuntime, SESSION_OP_TIMEOUT,
    SessionLockState, SessionRuntime, SpawnCtx, bounded_session_op, claim_lock, release_lock_state,
};
use crate::AppSession;
use crate::agent::shared_queue::correlation;
use crate::app::session_state::rules_to_stored;
use crate::plan_approval::{
    APPROVAL_BUSY, APPROVAL_CANCELLED, APPROVAL_CHANGED, ApprovedPlan, prepare_change, read_plan,
};

#[cfg(test)]
mod tests;

pub(super) struct PendingPlanApproval {
    pub(super) identity: Arc<()>,
    pub(super) cancel: Option<maki_agent::CancelTrigger>,
    pub(super) waiting_for_lock: Option<ApprovalStep>,
}

impl Drop for PendingPlanApproval {
    fn drop(&mut self) {
        if let Some(cancel) = self.cancel.take() {
            cancel.cancel();
        }
    }
}

pub(super) enum ApprovalStep {
    Captured(Box<PreparedPlanApproval>),
    Fresh(Box<FreshPlanApproval>),
}

impl ApprovalStep {
    fn prepared(&self) -> &PreparedPlanApproval {
        match self {
            Self::Captured(prepared) => prepared,
            Self::Fresh(fresh) => &fresh.source,
        }
    }
}

pub(super) struct PreparedPlanApproval {
    request: PlanApprovalRequest,
    plan: ApprovedPlan,
    operation: PreparedOperationTicket,
    source: IdleSessionLease,
    idle: IdleSubtreeGuard,
}

/// The source stays prepared until the swap so its config, history and
/// cwd cannot move under the committed candidate.
pub(super) struct FreshPlanApproval {
    source: PreparedPlanApproval,
    candidate: PreparedSessionRuntime,
    config: ConfigCommit,
    ticket: TurnTicket,
    idle: IdleSubtreeGuard,
    lock: ClaimedSessionLock,
}

struct TargetLockClaim {
    sessions_dir: PathBuf,
    #[cfg(test)]
    gate: Option<LockPrepareGate>,
}

impl TargetLockClaim {
    /// Under test, the gate's guard is returned alongside the lock so a
    /// cancelled claim drops the lock before the guard observes it.
    async fn claim(self, target: MakiId) -> Result<ClaimedSessionLock, String> {
        let claimed = smol::unblock(move || {
            #[cfg(test)]
            let guard = self.gate.map(|gate| gate(target));
            let lock = claim_lock(&self.sessions_dir, &target).map_err(|error| error.to_string());
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
        let (claimed, _guard) = claimed;
        claimed
    }
}

impl SpawnCtx {
    fn target_lock_claim(&self) -> TargetLockClaim {
        TargetLockClaim {
            sessions_dir: self.sessions_dir.clone(),
            #[cfg(test)]
            gate: self.lock_prepare_gate.clone(),
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
    build_mode: ModeDef,
}

impl SessionRuntime {
    fn abort_plan_approval(&mut self, message: String) {
        self.pending_approval.take();
        self.app.plan_approval_pending = false;
        self.app.flash(message);
    }

    fn check_plan_approval(&self, prepared: &PreparedPlanApproval) -> Result<(), String> {
        let request = &prepared.request;
        let unchanged = !self.lock_lost
            && self.session_lock.is_some()
            && self
                .app
                .plan_approval_matches(request.model.as_deref(), request.parallel)
            && self.app.run_id.wrapping_add(1) == request.run_id
            && self.app.state.plan.path() == Some(request.path.as_path())
            && prepared.source.revalidate().is_ok()
            && request.yolo == self.app.permissions.persisted_yolo()
            && request.rules == rules_to_stored(&self.app.permissions.session_rules_snapshot());
        if unchanged {
            Ok(())
        } else {
            Err(APPROVAL_CHANGED.into())
        }
    }
}

fn implementation_turn(message: String, sender: EventSender, run_id: u64) -> PreparedTurn {
    PreparedTurn {
        input: build_input(message),
        event_sender: Some(sender),
        correlation: correlation(run_id),
    }
}

fn build_input(message: String) -> AgentInput {
    AgentInput::from_defaults(
        message,
        AgentMode::Build,
        Vec::new(),
        SessionDefaults::default(),
    )
}

impl EventLoop<'_> {
    pub(super) fn complete_plan_approval(
        &mut self,
        idx: usize,
        result: Result<ApprovalStep, String>,
    ) {
        let runtime = &mut self.sessions[idx];
        let step = match result.and_then(|step| {
            runtime.check_plan_approval(step.prepared())?;
            Ok(step)
        }) {
            Ok(step) => step,
            Err(error) => return runtime.abort_plan_approval(error),
        };
        match step {
            ApprovalStep::Captured(prepared) if prepared.request.clear_context => {
                self.prepare_fresh_plan_approval(idx, *prepared);
            }
            step if matches!(runtime.session_lock, Some(SessionLockState::InFlight(_))) => {
                if let Some(pending) = runtime.pending_approval.as_mut() {
                    pending.waiting_for_lock = Some(step);
                }
            }
            ApprovalStep::Captured(prepared) => self.commit_plan_approval(idx, *prepared),
            ApprovalStep::Fresh(fresh) => self.activate_fresh_plan_approval(idx, *fresh),
        }
    }

    fn commit_plan_approval(&mut self, idx: usize, prepared: PreparedPlanApproval) {
        let runtime = &mut self.sessions[idx];
        let PreparedPlanApproval {
            request,
            plan,
            operation,
            source,
            idle,
        } = prepared;
        let commit = match operation.commit() {
            Ok(commit) => commit,
            Err(error) => return runtime.abort_plan_approval(error.to_string()),
        };
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
        runtime
            .app
            .record_recent_model(&commit.config.config.model.spec());
    }

    fn activate_fresh_plan_approval(&mut self, idx: usize, fresh: FreshPlanApproval) {
        let FreshPlanApproval {
            source,
            mut candidate,
            config,
            ticket,
            idle,
            lock,
        } = fresh;
        if ticket.peek().is_some() {
            return self.sessions[idx].abort_plan_approval(APPROVAL_CHANGED.into());
        }
        let runtime = &mut self.sessions[idx];
        let target = candidate.app.session_id();
        if let Some((_, session)) = candidate.seed_snapshot.as_mut() {
            let session = Arc::make_mut(session);
            session.model = config.config.model.spec();
            session.meta.mode = Some(StoredMode::Build);
            session.meta.thinking = Some(config.config.thinking.into());
            session.meta.fast = config.config.fast;
            session.meta.workflow = config.config.workflow;
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
                return runtime.abort_plan_approval(error.to_string());
            }
        };
        if let Some((writer, session)) = seed {
            writer.seed(session);
        }
        next.model_slot.project(
            config.config.model.clone(),
            Arc::clone(&config.config.settings.provider),
        );
        next.project_config(&config);
        next.app.exit_on_done = runtime.app.exit_on_done;
        next.app
            .input_box
            .replace_history_from(&mut runtime.app.input_box);
        let PreparedPlanApproval {
            request,
            plan,
            operation,
            source,
            idle: source_idle,
        } = source;
        next.app.record_plan_implementation(
            request.run_id,
            plan.content,
            plan.path.display().to_string(),
            plan.message,
        );
        next.approved_turn = Some((idle, ticket));
        next.approval_runner_pending = true;
        next.app.record_recent_model(&config.config.model.spec());
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
            drop(source_idle);
            let _ = coordinator.close().await;
            storage_writer.forget(retired_id);
            let _ = internal_tx.send(InternalEvent::ApprovalRunnerReady {
                session: target,
                runtime: identity,
            });
        })
        .detach();
    }

    fn spawn_approval_step<F>(&mut self, idx: usize, step: F)
    where
        F: Future<Output = Result<ApprovalStep, String>> + Send + 'static,
    {
        let runtime = &mut self.sessions[idx];
        let identity = Arc::new(());
        let (trigger, cancel) = CancelToken::new();
        runtime.pending_approval = Some(PendingPlanApproval {
            identity: Arc::clone(&identity),
            cancel: Some(trigger),
            waiting_for_lock: None,
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

    fn prepare_fresh_plan_approval(&mut self, idx: usize, source: PreparedPlanApproval) {
        let result = (|| -> Result<_, String> {
            let config = source
                .operation
                .ready_config()
                .map_err(|error| error.to_string())?;
            let mut session = AppSession::new(
                &config.model.spec(),
                &source.source.snapshot().cwd().to_string_lossy(),
            );
            session.meta.yolo = source.request.yolo;
            session.meta.session_rules = source.request.rules.clone();
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
            let turn = implementation_turn(
                source.plan.message.clone(),
                EventSender::new(candidate.handles.agent_tx(), source.request.run_id),
                source.request.run_id,
            );
            let operation = actor
                .reserve_prepared_operation(Some(turn))
                .map_err(|error| error.to_string())?;
            Ok((candidate, operation, idle))
        })();
        let (candidate, operation, idle) = match result {
            Ok(result) => result,
            Err(error) => return self.sessions[idx].abort_plan_approval(error),
        };
        let target = candidate.app.session_id();
        let policy = Arc::clone(&self.ctx.model_policy);
        let provider = Arc::clone(&self.ctx.prepare_provider);
        let timeouts = self.ctx.timeouts;
        let spec = source.request.model.clone();
        let mode = source.request.build_mode.clone();
        let lock_claim = self.ctx.target_lock_claim();
        self.spawn_approval_step(idx, async move {
            let change =
                smol::unblock(move || prepare_change(spec, &policy, timeouts, &provider, mode))
                    .await?;
            operation
                .resolve(Ok(Some(change)))
                .map_err(|error| error.to_string())?;
            operation
                .wait_ready()
                .await
                .map_err(|error| error.to_string())?;
            let PreparedCommit { config, ticket } =
                operation.commit().map_err(|error| error.to_string())?;
            let ticket = ticket.ok_or_else(|| APPROVAL_CHANGED.to_string())?;
            idle.allow_turn(&ticket)
                .map_err(|error| error.to_string())?;
            let lock = lock_claim.claim(target).await?;
            Ok(ApprovalStep::Fresh(Box::new(FreshPlanApproval {
                source,
                candidate,
                config,
                ticket,
                idle,
                lock,
            })))
        });
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
            _ => return runtime.abort_plan_approval(APPROVAL_BUSY.into()),
        };
        let actor = match manager.actor(root) {
            Ok(actor) => actor,
            Err(error) => return runtime.abort_plan_approval(error.to_string()),
        };
        let request = PlanApprovalRequest {
            path,
            model,
            parallel,
            clear_context,
            run_id: runtime.app.run_id.wrapping_add(1),
            yolo: runtime.app.permissions.persisted_yolo(),
            rules: rules_to_stored(&runtime.app.permissions.session_rules_snapshot()),
            build_mode: self
                .ctx
                .lua_event_handle
                .mode_registry()
                .current(&AgentMode::Build),
        };
        let absolute = if request.path.is_absolute() {
            request.path.clone()
        } else {
            PathBuf::from(&runtime.app.state.session.cwd).join(&request.path)
        };
        let turn = (!clear_context).then(|| {
            implementation_turn(
                String::new(),
                EventSender::new(runtime.handles.agent_tx.clone(), request.run_id),
                request.run_id,
            )
        });
        let operation = match actor.reserve_prepared_operation(turn) {
            Ok(operation) => operation,
            Err(error) => return runtime.abort_plan_approval(error.to_string()),
        };
        let coordinator = runtime.coordinator.clone();
        let policy = Arc::clone(&self.ctx.model_policy);
        let prepare = Arc::clone(&self.ctx.prepare_provider);
        let timeouts = self.ctx.timeouts;
        self.spawn_approval_step(idx, async move {
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
                return Err(APPROVAL_CHANGED.into());
            }
            let parallel = request.parallel;
            let plan = smol::unblock(move || read_plan(path, parallel)).await?;
            let change = if request.clear_context {
                None
            } else {
                operation
                    .replace_turn_input(build_input(plan.message.clone()))
                    .map_err(|error| error.to_string())?;
                let spec = request.model.clone();
                let mode = request.build_mode.clone();
                Some(
                    smol::unblock(move || prepare_change(spec, &policy, timeouts, &prepare, mode))
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
            Ok(ApprovalStep::Captured(Box::new(PreparedPlanApproval {
                request,
                plan,
                operation,
                source,
                idle,
            })))
        });
    }
}
