use async_lock::Mutex;
use makima_agent::tools::FileWriteLocks;
use makima_agent::{CancelToken, ModeRegistry};
use makima_storage::StateDir;
use makima_storage::session_state::ParentLink;
use std::any::Any;
use std::collections::BTreeSet;
use std::path::Path;
use std::sync::{Arc, Mutex as SyncMutex};

use makima_agent::actor::EffectiveAgentConfig;
use makima_agent::actor::{ActorIdleGuard, InstalledActorRewind};
use makima_agent::cancel::PublicationRegistration;
use makima_agent::config_change::prepare_restore;
use makima_agent::journal::{
    JournalChange, JournalReplacement, PreparedRewindNotification, SessionJournal,
    SessionJournalSnapshot,
};
use makima_agent::manager::{ManagerRewindGuard, ManagerTransitionGuard};
use makima_agent::permissions::PermissionSnapshot;
use makima_agent::plan_state::PlanProjectionRollback;
use makima_agent::rewind::RewindBoundary;
use makima_agent::session_coordinator::PreparedRewindProjection;
use makima_agent::{AgentId, AgentRef};
use makima_storage::checkpoint::{CheckpointClaim, CheckpointPublication};
use makima_storage::id::MakimaId;
use makima_storage::now_epoch;
use makima_storage::session_lock;
use makima_storage::session_state::StoredBranch;
use tracing::warn;

use super::{EventLoop, InternalEvent, LOCK_LOST_REPLACEMENT_ERR};
use crate::AppSession;
use crate::app::RESTORE_IN_PROGRESS;
use crate::storage_writer::{
    CurrentSessionCheckpoint, RestoredSessionCheckpoint, SessionSaveFence,
};

#[cfg(test)]
pub(crate) mod tests;

const INVALID_PARENT_ROUTE: &str = "Parent restore route changed or contains a cycle";
const PUBLISHED_INSTALL_FAILED: &str = "Published restore could not be installed";
const PARENT_INITIALIZATION_CHANGED: &str =
    "Parent runtime initialization changed the prepared journal";
const ABORT_REPAIR_FAILED: &str =
    "Restore aborted and current plan projection could not be repaired";

pub(super) enum RestoreTarget {
    Boundary(RewindBoundary),
    Branch(MakimaId),
}

struct PendingParentFiles(Option<PlanRollback>);

impl PendingParentFiles {
    fn abort(mut self) -> Result<(), String> {
        self.0.take().ok_or(super::NOT_LIVE_ERR)?.abort()
    }

    fn retain_barrier(&mut self, barrier: impl Any + Send) {
        if let Some(receipt) = &mut self.0 {
            receipt.retain_barrier(barrier);
        }
    }

    fn complete(mut self) -> Result<(), String> {
        self.0.take().ok_or(super::NOT_LIVE_ERR)?.complete()
    }
}

impl Drop for PendingParentFiles {
    fn drop(&mut self) {
        if let Some(receipt) = self.0.take()
            && let Err(error) = receipt.abort()
        {
            warn!(%error, "abandoned parent restore could not settle; receipt barriers retained");
        }
    }
}

pub(super) struct PreparedParentRestore {
    receipt: PendingParentFiles,
    session: AppSession,
    config: EffectiveAgentConfig,
    lease: RouteLease,
    replacement: JournalReplacement,
    warnings: Vec<String>,
    publication: CheckpointPublication,
    claim: CheckpointClaim,
    branch: StoredBranch,
    _publication: PublicationRegistration,
    source_guard: Arc<ManagerTransitionGuard>,
}

impl PreparedParentRestore {
    pub(super) fn abort(self) -> Result<(), String> {
        self.publication.cancel();
        self.receipt.abort().map_err(|error| error.to_string())
    }
}

pub(super) struct PublishedParentRestore {
    prepared: super::ReservedSessionRuntime,
    notifications: PreparedRewindNotification,
    warnings: Vec<String>,
    _publication: PublicationRegistration,
    source_guard: Arc<ManagerTransitionGuard>,
}

pub(super) struct InstalledRestore {
    session: AppSession,
    target: AgentId,
    draft: Option<String>,
    affected: BTreeSet<AgentId>,
    fence: SessionSaveFence,
    replacement: JournalReplacement,
    manager: Arc<Mutex<ManagerRewindGuard>>,
    actors: Vec<InstalledActorRewind>,
    projection: Option<PreparedRewindProjection>,
    warnings: Vec<String>,
    receipt: PlanRollback,
}

impl EventLoop<'_> {
    pub(super) fn restore_failed(&mut self, idx: usize, error: String) {
        if error.starts_with(ABORT_REPAIR_FAILED) || error.starts_with(PUBLISHED_INSTALL_FAILED) {
            super::stop_runtime_after_recovery_failure(&mut self.sessions[idx], &error);
            self.sessions[idx].app.restore_in_progress = true;
        } else {
            self.sessions[idx].app.restore_in_progress = false;
            self.sessions[idx].app.flash(error);
        }
    }

    pub(super) fn dispatch_parent_restore(
        &mut self,
        idx: usize,
        session: MakimaId,
        seq: u64,
    ) -> Result<(), String> {
        let runtime = &mut self.sessions[idx];
        if runtime.lock_lost {
            return Err(LOCK_LOST_REPLACEMENT_ERR.into());
        }
        if runtime.app.restore_in_progress || runtime.pending_approval.is_some() {
            return Err(RESTORE_IN_PROGRESS.into());
        }
        let source_guard = Arc::new(
            runtime
                .handles
                .manager_and_root()
                .0
                .reserve_graph_transition()
                .map_err(|error| error.to_string())?,
        );
        let source_session = runtime.app.state.session.as_ref().clone();
        let source_lease = runtime.share_session_lease()?;
        let (parent, link, route_leases) = parent_route(
            &source_session,
            session,
            seq,
            &self.ctx.sessions_dir,
            &self.ctx.storage,
        )?;
        if !makima_agent::parent_rewind::boundaries(&parent, &link)
            .map_err(|error| error.to_string())?
            .iter()
            .any(|entry| entry.seq == seq)
        {
            return Err(super::NOT_LIVE_ERR.into());
        }
        let renewing = route_leases
            .iter()
            .find(|(id, _)| *id == session)
            .map(|(_, lease)| Arc::clone(lease))
            .ok_or(super::NOT_LIVE_ERR)?;
        let route_leases: Vec<_> = route_leases.into_iter().map(|(_, lease)| lease).collect();
        let implementation_guard = source_lease.publication_guard();
        runtime.app.checkpoint_now();
        let mut implementation = runtime.app.capture_restore_snapshot();
        let implementation_journal = runtime
            .handles
            .manager_and_root()
            .0
            .journal()
            .checkpoint_snapshot()?;
        implementation_journal
            .apply_to(&mut implementation)
            .map_err(|error| error.to_string())?;
        let implementation_checkpoint = CurrentSessionCheckpoint {
            session: Arc::new(implementation),
            journal: implementation_journal,
            publication: CheckpointPublication::default(),
            claim: super::publication_claim(),
            guard: implementation_guard,
            fence: runtime.app.storage_fence.clone(),
        };
        let writer = Arc::clone(&self.ctx.storage_writer);
        let locks = Arc::clone(&self.ctx.file_write_locks);
        let lua = self.ctx.lua_event_handle.clone();
        let modes = lua.mode_registry();
        let prepare = Arc::clone(&self.ctx.prepare_provider);
        let policy = Arc::clone(&self.ctx.model_policy);
        let cancel = runtime.lifetime_cancel.clone();
        let identity = runtime.handles.identity();
        runtime.app.restore_in_progress = true;
        let internal = self.internal_tx.clone();
        smol::spawn(async move {
            let result = async {
                let baseline_fence = implementation_checkpoint.fence.clone();
                writer
                    .checkpoint_current_guarded(implementation_checkpoint)
                    .await?;
                writer
                    .await_publication_sync(&baseline_fence, &cancel)
                    .await
                    .map_err(|error| format!("{ABORT_REPAIR_FAILED}: {error}"))?;
                let journal = Arc::new(
                    SessionJournal::from_session(&parent).map_err(|error| error.to_string())?,
                );
                let root = parent.root_agent_id().ok_or(super::NOT_LIVE_ERR)?;
                let boundary = RewindBoundary {
                    agent: root,
                    seq,
                    epoch: journal.snapshot().epoch,
                };
                let mut files: Option<PlanRollback> = None;
                let outcome = async {
                    let mut replacement = journal
                        .prepare_rewind(boundary)
                        .await
                        .map_err(|error| error.to_string())?;
                    let publication = CheckpointPublication::default();
                    let registration = cancel.register_publication(&publication);
                    let state = lua
                        .prepare_state_restore(
                            replacement.snapshot().clone(),
                            replacement.affected_agents().clone(),
                            true,
                            publication.clone(),
                        )
                        .await?;
                    replacement
                        .apply_migrations(state.changes)
                        .map_err(|error| error.to_string())?;
                    let receipt = capture_plan_rollback(
                        replacement.snapshot().clone(),
                        replacement.affected_agents().clone(),
                        &locks,
                        &cancel,
                        (
                            Arc::clone(&source_guard),
                            route_leases,
                            Arc::clone(&source_lease),
                        ),
                    )
                    .await?;
                    files = Some(receipt);
                    let (changes, warnings) =
                        files.as_mut().ok_or(super::NOT_LIVE_ERR)?.apply(&modes)?;
                    replacement
                        .apply_configs(changes)
                        .map_err(|error| error.to_string())?;
                    let snapshot = replacement.snapshot().clone();
                    let view = snapshot
                        .fold(snapshot.seq)
                        .map_err(|error| error.to_string())?;
                    let saved = view
                        .scopes
                        .get(&Some(root))
                        .and_then(|scope| scope.config.clone())
                        .ok_or(super::NOT_LIVE_ERR)?;
                    let prepared = prepare_restore(saved, &policy, &prepare, &modes)
                        .await
                        .map_err(|error| error.to_string())?;
                    let config = EffectiveAgentConfig::from_stored(
                        &prepared.saved,
                        prepared.model,
                        prepared.mode,
                    )
                    .map_err(|error| error.to_string())?;
                    let mut session = parent.clone();
                    snapshot
                        .apply_to(&mut session)
                        .map_err(|error| error.to_string())?;
                    project_presentations(&mut session, &snapshot, replacement.affected_agents())?;
                    session.meta.queued_messages.clear();
                    session.meta.input_draft = None;
                    let branch = StoredBranch {
                        id: MakimaId::generate(),
                        created_at: now_epoch(),
                        at_seq: journal.snapshot().seq,
                        agent: root,
                        offset: parent.messages().len(),
                    };
                    session.push_branch(branch.clone());
                    writer.seed(Arc::new(parent));
                    writer.bind_journal(session.id, Arc::clone(&journal));
                    Ok(PreparedParentRestore {
                        session,
                        config,
                        lease: renewing,
                        receipt: PendingParentFiles(Some(files.take().ok_or(super::NOT_LIVE_ERR)?)),
                        replacement,
                        warnings,
                        publication,
                        claim: state.claim,
                        branch,
                        _publication: registration,
                        source_guard,
                    })
                }
                .await;
                if outcome.is_err()
                    && let Some(files) = files
                {
                    files
                        .abort()
                        .map_err(|error| format!("{ABORT_REPAIR_FAILED}: {error}"))?;
                }
                outcome
            }
            .await;
            if let Err(error) = internal.send(InternalEvent::ParentRestoreReady {
                runtime: identity,
                result: result.map(Box::new),
            }) && let InternalEvent::ParentRestoreReady {
                result: Ok(restored),
                ..
            } = error.0
                && let Err(error) = restored.abort()
            {
                warn!(%error, "abandoned parent restore projection rollback failed");
            }
        })
        .detach();
        Ok(())
    }

    pub(super) fn complete_parent_restore(
        &mut self,
        idx: usize,
        result: Result<Box<PreparedParentRestore>, String>,
    ) {
        let restored = match result {
            Ok(restored) => *restored,
            Err(error) => {
                self.restore_failed(idx, error);
                return;
            }
        };
        let PreparedParentRestore {
            session,
            config,
            lease,
            receipt,
            replacement,
            warnings,
            publication,
            claim,
            branch,
            _publication,
            source_guard,
        } = restored;
        let guard = lease.publication_guard();
        let snapshot = replacement.snapshot().clone();
        let prepared = (|| -> Result<_, String> {
            if self.sessions[idx].lock_lost || self.sessions[idx].lifetime_cancel.is_cancelled() {
                return Err(LOCK_LOST_REPLACEMENT_ERR.into());
            }
            let runtime = &self.sessions[idx];
            let view = session.latest_state().map_err(|error| error.to_string())?;
            let permissions = view
                .scopes
                .get(&None)
                .map_or_else(
                    || {
                        PermissionSnapshot::from_stored(
                            &session.meta.session_rules,
                            session.meta.yolo,
                        )
                    },
                    |scope| PermissionSnapshot::from_stored(&scope.rules, scope.yolo),
                )
                .map_err(|error| error.to_string())?;
            let prepared = self
                .ctx
                .prepare_runtime_candidate(
                    session,
                    Some(makima_providers::PreparedProvider {
                        model: config.model.clone(),
                        provider: Arc::clone(&config.provider),
                    }),
                    &runtime.app.permissions,
                    super::RuntimeOwnership::Reuse(guard.clone()),
                    Some((config, permissions)),
                    None,
                    Some(replacement.snapshot().clone()),
                )
                .map_err(|error| error.to_string())?;
            let prepared = prepared.reserve_before_publication(lease)?;
            let initialized = prepared
                .handles
                .initialized()
                .manager_and_root()
                .0
                .journal()
                .snapshot();
            let expected = replacement.snapshot();
            if initialized.epoch != expected.epoch
                || initialized.revision != expected.revision
                || initialized.seq != expected.seq
            {
                return Err(PARENT_INITIALIZATION_CHANGED.into());
            }
            let journal = prepared
                .handles
                .initialized()
                .manager_and_root()
                .0
                .journal();
            let notifications = replacement
                .prepare_handoff(journal)
                .map_err(|error| error.to_string())?;
            Ok((prepared, notifications))
        })();
        let (prepared, notifications) = match prepared {
            Ok(prepared) => prepared,
            Err(error) => {
                let error = match receipt.abort() {
                    Ok(()) => error,
                    Err(repair) => format!("{ABORT_REPAIR_FAILED}: {repair}; {error}"),
                };
                self.restore_failed(idx, error);
                return;
            }
        };
        let session = prepared.app.app.state.session.as_ref().clone();
        let writer = Arc::clone(&self.ctx.storage_writer);
        let identity = self.sessions[idx].handles.identity();
        let cancel = self.sessions[idx].lifetime_cancel.clone();
        let internal = self.internal_tx.clone();
        smol::spawn(async move {
            let result = async {
                let checkpoint = writer
                    .checkpoint_restore(RestoredSessionCheckpoint {
                        session_id: session.id,
                        session: Arc::new(session),
                        journal: snapshot,
                        branch,
                        publication,
                        claim,
                        guard,
                    })
                    .await;
                let fence = match checkpoint {
                    Ok(fence) => fence,
                    Err(error) => {
                        return Err(match receipt.abort() {
                            Ok(()) => error,
                            Err(repair) => format!("{ABORT_REPAIR_FAILED}: {repair}; {error}"),
                        });
                    }
                };
                let prepared = Arc::new(SyncMutex::new(Some(prepared)));
                let mut receipt = receipt;
                receipt.retain_barrier(Arc::clone(&prepared));
                writer
                    .await_publication_sync(&fence, &cancel)
                    .await
                    .map_err(|error| format!("{ABORT_REPAIR_FAILED}: {error}"))?;
                receipt
                    .complete()
                    .map_err(|error| format!("{ABORT_REPAIR_FAILED}: {error}"))?;
                let prepared = prepared
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .take()
                    .ok_or(super::NOT_LIVE_ERR)?;
                Ok(PublishedParentRestore {
                    prepared,
                    notifications,
                    warnings,
                    _publication,
                    source_guard,
                })
            }
            .await;
            let _ = internal.send(InternalEvent::ParentRestorePublished {
                runtime: identity,
                result: result.map(Box::new),
            });
        })
        .detach();
    }

    pub(super) fn install_parent_restore(
        &mut self,
        idx: usize,
        result: Result<Box<PublishedParentRestore>, String>,
    ) {
        self.sessions[idx].app.restore_in_progress = false;
        let result = result.map(|restored| {
            let PublishedParentRestore {
                prepared,
                notifications,
                warnings,
                _publication,
                source_guard,
            } = *restored;
            let mut runtime = prepared.publish(&self.ctx.storage_writer);
            runtime.app.exit_on_done = self.sessions[idx].app.exit_on_done;
            runtime
                .app
                .input_box
                .replace_history_from(&mut self.sessions[idx].app.input_box);
            let mut old = std::mem::replace(&mut self.sessions[idx], runtime);
            old.cancel_background_work();
            smol::spawn(self.retire_runtime(idx, old, ())).detach();
            self.activate_parent_restore(idx, notifications);
            for warning in warnings {
                self.sessions[idx].app.flash(warning);
            }
            drop(source_guard);
        });
        if let Err(error) = result {
            self.restore_failed(idx, error);
        }
        self.orchestration_revisions.clear();
        self.sync_orchestration();
    }

    pub(super) fn install_detached_parent_restore(&mut self, restored: PublishedParentRestore) {
        let PublishedParentRestore {
            prepared,
            notifications,
            warnings,
            _publication,
            source_guard,
        } = restored;
        let mut runtime = prepared.publish(&self.ctx.storage_writer);
        runtime.restore_pending = false;
        self.sessions.push(runtime);
        let idx = self.sessions.len() - 1;
        self.activate_parent_restore(idx, notifications);
        for warning in warnings {
            self.sessions[idx].app.flash(warning);
        }
        drop(source_guard);
        self.orchestration_revisions.clear();
        self.sync_orchestration();
    }

    fn activate_parent_restore(&mut self, idx: usize, notifications: PreparedRewindNotification) {
        self.sessions[idx].restore_pending = false;
        self.sessions[idx].app.restore_in_progress = true;
        self.sessions[idx].app.restore_resumed_session();
        self.orchestration_revisions.clear();
        self.sync_orchestration();
        let runtime = &self.sessions[idx];
        let identity = runtime.handles.identity();
        let (manager, root) = runtime.handles.manager_and_root();
        let lua = runtime.app.lua_event_handle.clone();
        let internal = self.internal_tx.clone();
        smol::spawn(async move {
            let result =
                smol::unblock(move || lua.observe_state_restored(manager.reference(root))).await;
            let _ = internal.send(InternalEvent::ParentRestoreObserved {
                runtime: identity,
                notifications,
                result,
            });
        })
        .detach();
    }

    pub(super) fn complete_parent_observation(
        &mut self,
        idx: usize,
        notifications: PreparedRewindNotification,
        result: Result<(), String>,
    ) {
        let runtime = &mut self.sessions[idx];
        match result {
            Ok(()) => notifications.notify(),
            Err(error) => {
                warn!(%error, "parent rewind state observation unavailable");
                runtime.app.flash(format!(
                    "Parent restored, but state notifications are unavailable: {error}"
                ));
            }
        }
        runtime.app.restore_in_progress = false;
        runtime.handles.release_runner();
        self.orchestration_revisions.clear();
        self.sync_orchestration();
    }

    pub(super) fn dispatch_full_restore(
        &mut self,
        idx: usize,
        target: AgentRef,
        selection: RestoreTarget,
    ) -> Result<(), String> {
        let runtime = &mut self.sessions[idx];
        if runtime.lock_lost {
            return Err(LOCK_LOST_REPLACEMENT_ERR.into());
        }
        if runtime.app.restore_in_progress {
            return Err(RESTORE_IN_PROGRESS.into());
        }
        let (manager, root) = runtime.handles.manager_and_root();
        if !manager.reference(root).same_manager(&target) {
            return Err(super::NOT_LIVE_ERR.into());
        }
        let source_lease = runtime.share_session_lease()?;
        let guard = source_lease.publication_guard();
        let current = runtime.app.state.session.as_ref().clone();
        let save_fence = runtime.app.storage_fence.clone();
        let restored = match &selection {
            RestoreTarget::Branch(id) => Some(
                current
                    .load_branch_from(*id, &self.ctx.sessions_dir)
                    .map_err(|error| error.to_string())?,
            ),
            RestoreTarget::Boundary(_) => None,
        };
        let projection_preview = match &selection {
            RestoreTarget::Boundary(boundary) => manager
                .journal()
                .preview_rewind(boundary.clone())
                .map_err(|error| error.to_string())?,
            RestoreTarget::Branch(_) => {
                SessionJournal::from_session(restored.as_ref().ok_or(super::NOT_LIVE_ERR)?)
                    .map_err(|error| error.to_string())?
                    .snapshot()
            }
        };
        let draft = if matches!(selection, RestoreTarget::Boundary(_)) {
            projection_preview
                .histories
                .get(&target.id())
                .and_then(|history| {
                    manager
                        .journal()
                        .snapshot()
                        .histories
                        .get(&target.id())?
                        .messages
                        .get(history.messages.len())?
                        .user_text()
                        .map(str::to_owned)
                })
        } else {
            None
        };
        let file_write_locks = Arc::clone(&self.ctx.file_write_locks);
        let cancel = runtime.lifetime_cancel.clone();
        runtime.app.restore_in_progress = true;
        let session_id = runtime.id();
        let identity = runtime.handles.identity();
        let coordinator = runtime.coordinator.clone();
        let writer = Arc::clone(&self.ctx.storage_writer);
        let prepare = Arc::clone(&self.ctx.prepare_provider);
        let policy = Arc::clone(&self.ctx.model_policy);
        let lua = self.ctx.lua_event_handle.clone();
        let modes = lua.mode_registry();
        let internal = self.internal_tx.clone();
        smol::spawn(async move {
            let result = async {
                let held = Arc::new(Mutex::new(
                    manager
                        .begin_rewind(target.id())
                        .await
                        .map_err(|error| error.to_string())?,
                ));
                let journal = manager.journal();
                let mut rollback: Option<PlanRollback> = None;
                let mut published = false;
                let result = async {
                    let (reply, snapshot) = flume::bounded(1);
                    internal
                        .send(InternalEvent::RestoreSnapshot {
                            session: session_id,
                            runtime: identity.clone(),
                            reply,
                        })
                        .map_err(|_| super::RUNTIME_GONE_ERR.to_owned())?;
                    let mut current = snapshot
                        .recv_async()
                        .await
                        .map_err(|_| super::RUNTIME_GONE_ERR.to_owned())??;
                    let checkpoint = journal.checkpoint_snapshot()?;
                    checkpoint
                        .apply_to(&mut current)
                        .map_err(|error| error.to_string())?;
                    writer
                        .checkpoint_current_guarded(CurrentSessionCheckpoint {
                            session: Arc::new(current.clone()),
                            journal: checkpoint,
                            publication: CheckpointPublication::default(),
                            claim: super::publication_claim(),
                            guard: guard.clone(),
                            fence: save_fence.clone(),
                        })
                        .await
                        .map_err(|error| error.to_string())?;
                    writer
                        .await_publication_sync(&save_fence, &cancel)
                        .await
                        .map_err(|error| format!("{ABORT_REPAIR_FAILED}: {error}"))?;
                    let mut replacement = match &selection {
                        RestoreTarget::Boundary(boundary) => {
                            journal.prepare_rewind(boundary.clone()).await
                        }
                        RestoreTarget::Branch(_) => {
                            let restored = restored
                                .as_ref()
                                .ok_or_else(|| super::NOT_LIVE_ERR.to_owned())?;
                            let snapshot = SessionJournal::from_session(restored)
                                .map_err(|error| error.to_string())?
                                .snapshot();
                            journal.prepare_branch_restore(target.id(), snapshot).await
                        }
                    }
                    .map_err(|error| error.to_string())?;
                    let affected = replacement.affected_agents().clone();
                    let session_scope = replacement.session_scope();
                    let publication = CheckpointPublication::default();
                    let prepared_state = lua
                        .prepare_state_restore(
                            replacement.snapshot().clone(),
                            affected.clone(),
                            session_scope,
                            publication.clone(),
                        )
                        .await?;
                    replacement
                        .apply_migrations(prepared_state.changes)
                        .map_err(|error| error.to_string())?;
                    let receipt = capture_plan_rollback(
                        replacement.snapshot().clone(),
                        affected.clone(),
                        &file_write_locks,
                        &cancel,
                        (Arc::clone(&held), Arc::clone(&source_lease)),
                    )
                    .await?;
                    rollback = Some(receipt);
                    let (changes, warnings) = rollback
                        .as_mut()
                        .ok_or(super::NOT_LIVE_ERR)?
                        .apply(&modes)?;
                    replacement
                        .apply_configs(changes)
                        .map_err(|error| error.to_string())?;
                    let mut manager_guard = held.lock().await;
                    let actors = manager_guard
                        .prepare_actors(replacement.snapshot(), &policy, &prepare, &modes)
                        .await
                        .map_err(|error| error.to_string())?;
                    replacement = replacement
                        .with_prepared_actors(&actors)
                        .map_err(|error| error.to_string())?;
                    let snapshot = replacement.snapshot().clone();
                    manager_guard
                        .validate_replacement(&snapshot)
                        .map_err(|error| error.to_string())?;
                    let root_commit = actors
                        .iter()
                        .find(|actor| actor.agent_id() == root)
                        .map(|actor| actor.config().clone());
                    let mut projection = if session_scope {
                        Some(
                            coordinator
                                .prepare_rewind_projection(
                                    root_commit.ok_or_else(|| super::NOT_LIVE_ERR.to_owned())?,
                                    &snapshot,
                                    true,
                                )
                                .map_err(|error| error.to_string())?,
                        )
                    } else {
                        None
                    };
                    let mut session = if session_scope {
                        restored.unwrap_or_else(|| current.clone())
                    } else {
                        current.clone()
                    };
                    snapshot
                        .apply_to(&mut session)
                        .map_err(|error| error.to_string())?;
                    project_presentations(&mut session, &snapshot, &affected)?;
                    if session_scope {
                        session.meta.queued_messages.clear();
                        session.meta.input_draft = draft.clone();
                    }
                    let branch = StoredBranch {
                        id: MakimaId::generate(),
                        created_at: now_epoch(),
                        at_seq: journal.snapshot().seq,
                        agent: target.id(),
                        offset: journal
                            .snapshot()
                            .histories
                            .get(&target.id())
                            .map_or(0, |history| history.messages.len()),
                    };
                    let mut authoritative = current;
                    authoritative
                        .replace_authoritative(session)
                        .map_err(|error| error.to_string())?;
                    authoritative.push_branch(branch.clone());
                    let _registration = coordinator
                        .register_publication(&publication)
                        .map_err(|error| error.to_string())?;
                    let fence = writer
                        .checkpoint_restore(RestoredSessionCheckpoint {
                            session_id,
                            session: Arc::new(authoritative.clone()),
                            journal: snapshot.clone(),
                            branch,
                            publication,
                            claim: prepared_state.claim,
                            guard: guard.clone(),
                        })
                        .await?;
                    published = true;
                    if let Err(error) = writer.await_publication_sync(&fence, &cancel).await {
                        drop(rollback.take());
                        return Err(format!("{ABORT_REPAIR_FAILED}: {error}"));
                    }
                    let installed = (|| -> Result<_, String> {
                        replacement.install().map_err(|error| error.to_string())?;
                        if let Some(projection) = &mut projection {
                            projection.install().map_err(|error| error.to_string())?;
                        }
                        let actors = actors
                            .into_iter()
                            .map(|actor| actor.install_held())
                            .collect();
                        manager_guard
                            .reconcile(&snapshot)
                            .map_err(|error| error.to_string())?;
                        let checkpoint = writer.coordinator_checkpoint();
                        checkpoint.bind(Some(guard));
                        coordinator
                            .rebind_checkpoint_writer(checkpoint)
                            .map_err(|error| error.to_string())?;
                        Ok(actors)
                    })();
                    let actors = match installed {
                        Ok(actors) => actors,
                        Err(error) => {
                            coordinator.retire();
                            for (_, actor) in manager_guard.resident_actors() {
                                actor.shutdown();
                            }
                            return Err(format!("{PUBLISHED_INSTALL_FAILED}: {error}"));
                        }
                    };
                    drop(manager_guard);
                    Ok(InstalledRestore {
                        session: authoritative,
                        target: target.id(),
                        draft,
                        affected,
                        fence,
                        replacement,
                        manager: Arc::clone(&held),
                        actors,
                        projection,
                        warnings,
                        receipt: rollback
                            .take()
                            .ok_or_else(|| super::NOT_LIVE_ERR.to_owned())?,
                    })
                }
                .await;
                if result.is_err()
                    && let Some(receipt) = rollback
                {
                    let repair = if published {
                        receipt.complete()
                    } else {
                        receipt.abort()
                    };
                    if let Err(error) = repair {
                        coordinator.retire();
                        for (_, actor) in held.lock().await.resident_actors() {
                            actor.shutdown();
                        }
                        return Err(format!("{ABORT_REPAIR_FAILED}: {error}"));
                    }
                }
                result
            }
            .await;
            let _ = internal.send(InternalEvent::RestoreReady {
                session: session_id,
                runtime: identity,
                result: result.map(Box::new),
            });
        })
        .detach();
        Ok(())
    }

    pub(super) fn complete_full_restore(
        &mut self,
        idx: usize,
        result: Result<Box<InstalledRestore>, String>,
    ) {
        self.sessions[idx].app.restore_in_progress = false;
        let mut restored = match result {
            Ok(restored) => restored,
            Err(error) => {
                if error.starts_with(PUBLISHED_INSTALL_FAILED)
                    || error.starts_with(ABORT_REPAIR_FAILED)
                {
                    super::stop_runtime_after_recovery_failure(&mut self.sessions[idx], &error);
                    self.sessions[idx].app.restore_in_progress = true;
                } else {
                    self.sessions[idx].app.flash(error);
                }
                return;
            }
        };
        let runtime = &mut self.sessions[idx];
        let mut deferred = Vec::new();
        for envelope in runtime.handles.agent_rx.try_iter() {
            let agent = envelope
                .identity
                .map(|identity| identity.agent_id)
                .or_else(|| envelope.subagent.as_ref().map(|subagent| subagent.agent_id))
                .unwrap_or(restored.session.root_agent_id().unwrap_or(restored.target));
            if !restored.affected.contains(&agent) {
                deferred.push(envelope);
            }
        }
        for event in runtime.handles.actor_rx.try_iter() {
            if !matches!(&event, makima_agent::actor::ActorEvent::Delivery(delivery)
                if restored.affected.contains(&delivery.delivery.result.outcome.agent_id()))
            {
                runtime.app.handle_actor_event(event);
            }
        }
        runtime.app.storage_fence = restored.fence.clone();
        let snapshot = restored.replacement.snapshot();
        let (_, root) = runtime.handles.manager_and_root();
        if let Some(history) = snapshot.histories.get(&root) {
            runtime.handles.history.store(Arc::new(history.clone()));
        }
        runtime.app.apply_full_restore(
            restored.session,
            &restored.affected,
            restored.target,
            restored.draft,
        );
        let mut receipt = restored.receipt;
        let actors = Arc::new(SyncMutex::new(Some(restored.actors)));
        receipt.retain_barrier(Arc::clone(&actors));
        if let Err(error) = receipt.complete() {
            let error = format!("{ABORT_REPAIR_FAILED}: {error}");
            super::stop_runtime_after_recovery_failure(runtime, &error);
            runtime.app.restore_in_progress = true;
            return;
        }
        let mut held: Vec<ActorIdleGuard> = Vec::new();
        for actor in actors
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take()
            .unwrap_or_default()
        {
            let (idle, config, _) = actor.notify();
            runtime.project_config(&config);
            held.push(idle);
        }
        if let Some(projection) = &restored.projection {
            projection.publish();
        }
        for warning in restored.warnings {
            runtime.app.flash(warning);
        }
        runtime.notifications.reset();
        for envelope in deferred {
            self.handle_agent(idx, Box::new(envelope));
        }
        restored.replacement.notify();
        drop(held);
        drop(restored.manager);
        self.orchestration_revisions.clear();
        self.sync_orchestration();
    }
}

type RouteLease = Arc<super::RenewingDeletionLease>;
type ParentRoute = (AppSession, ParentLink, Vec<(MakimaId, RouteLease)>);

struct PlanRollback {
    projection: PlanProjectionRollback,
    snapshot: SessionJournalSnapshot,
    affected: BTreeSet<AgentId>,
    barriers: Vec<Box<dyn Any + Send>>,
}

impl PlanRollback {
    fn retain_barrier(&mut self, barrier: impl Any + Send) {
        self.barriers.push(Box::new(barrier));
    }

    fn apply(&mut self, modes: &ModeRegistry) -> Result<(Vec<JournalChange>, Vec<String>), String> {
        self.projection
            .project(&self.snapshot, &self.affected, modes)
    }

    fn abort(self) -> Result<(), String> {
        self.projection.rollback()
    }

    fn complete(self) -> Result<(), String> {
        Ok(())
    }
}

async fn capture_plan_rollback(
    snapshot: SessionJournalSnapshot,
    affected: BTreeSet<AgentId>,
    locks: &FileWriteLocks,
    cancel: &CancelToken,
    barrier: impl Any + Send,
) -> Result<PlanRollback, String> {
    let projection =
        PlanProjectionRollback::capture_locked(&snapshot, &affected, locks, cancel).await?;
    Ok(PlanRollback {
        projection,
        snapshot,
        affected,
        barriers: vec![Box::new(barrier)],
    })
}

fn parent_route(
    source: &AppSession,
    target: MakimaId,
    seq: u64,
    sessions_dir: &Path,
    storage: &StateDir,
) -> Result<ParentRoute, String> {
    let mut route = vec![source.clone()];
    let mut visited = BTreeSet::from([source.id]);
    let link = loop {
        let current = route.last().ok_or(super::NOT_LIVE_ERR)?;
        let link = current.parent().cloned().ok_or(super::NOT_LIVE_ERR)?;
        if !visited.insert(link.session) {
            return Err(INVALID_PARENT_ROUTE.into());
        }
        let parent = AppSession::load(link.session, storage).map_err(|error| error.to_string())?;
        if let Some(block) = session_lock::resume_block(&parent.cwd, &source.cwd, false) {
            return Err(block.to_string());
        }
        if parent.cwd != source.cwd {
            return Err(INVALID_PARENT_ROUTE.into());
        }
        route.push(parent);
        if link.session == target {
            break link;
        }
    };
    if seq > link.seq {
        return Err(INVALID_PARENT_ROUTE.into());
    }
    let mut leases = Vec::new();
    for id in visited.into_iter().filter(|id| *id != source.id) {
        let lease = super::claim_lock(sessions_dir, &id).map_err(|error| error.to_string())?;
        leases.push((id, Arc::new(super::RenewingDeletionLease::new(lease))));
    }
    for session in route.iter_mut().filter(|session| session.id != source.id) {
        let reloaded = AppSession::load(session.id, storage).map_err(|error| error.to_string())?;
        if session.parent() != reloaded.parent() {
            return Err(INVALID_PARENT_ROUTE.into());
        }
        *session = reloaded;
    }
    let parent = route.pop().ok_or(super::NOT_LIVE_ERR)?;
    Ok((parent, link, leases))
}

fn project_presentations(
    session: &mut AppSession,
    snapshot: &SessionJournalSnapshot,
    affected: &BTreeSet<AgentId>,
) -> Result<(), String> {
    let view = snapshot
        .fold(snapshot.seq)
        .map_err(|error| error.to_string())?;
    for agent in affected {
        let Some(record) = view.agents.get(agent) else {
            continue;
        };
        let Some(history) = snapshot.histories.get(agent) else {
            continue;
        };
        session.prune_posts(
            record.parent.map(|_| record.transcript_key.as_str()),
            history.messages.len(),
        );
    }
    let mut children = session.subagents().to_vec();
    children.retain(|child| {
        view.agents
            .values()
            .any(|agent| agent.parent.is_some() && agent.transcript_key == child.tool_use_id)
    });
    session.set_subagents(children);
    Ok(())
}
