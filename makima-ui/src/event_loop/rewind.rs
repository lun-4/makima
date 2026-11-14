use async_lock::Mutex;
use makima_agent::tools::FileWriteLocks;
use makima_agent::{CancelToken, ModeRegistry};
use makima_storage::StateDir;
use makima_storage::session_lock::SessionPublicationGuard;
use makima_storage::session_state::ParentLink;
use std::any::Any;
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::{Arc, Mutex as SyncMutex};

use makima_agent::actor::EffectiveAgentConfig;
use makima_agent::actor::{ActorIdleGuard, InstalledActorRewind};
use makima_agent::cancel::PublicationRegistration;
use makima_agent::config_change::prepare_restore;
use makima_agent::file_restore::AppliedRestore;
#[cfg(unix)]
use makima_agent::file_restore::PreparedRestore;
use makima_agent::file_restore::{
    self, OwnedRestoreLease, PlanProjection, RestoreRequest, RestoreSegment,
};
use makima_agent::journal::{
    JournalReplacement, PreparedRewindNotification, SessionJournal, SessionJournalSnapshot,
};
use makima_agent::manager::{ManagerRewindGuard, ManagerTransitionGuard};
use makima_agent::permissions::PermissionSnapshot;
#[cfg(not(unix))]
use makima_agent::plan_state::PlanProjectionRollback;
use makima_agent::rewind::RewindBoundary;
use makima_agent::session_coordinator::PreparedRewindProjection;
use makima_agent::{AgentId, AgentRef};
use makima_storage::checkpoint::{CheckpointClaim, CheckpointPublication};
use makima_storage::id::MakimaId;
use makima_storage::now_epoch;
#[cfg(not(unix))]
use makima_storage::restore::RestoreStore;
use makima_storage::restore::{RestoreChoice, RestoreEndpoint, TransitionRef};
use makima_storage::session_lock;
use makima_storage::session_lock::ClaimedSessionLock;
use makima_storage::session_state::{SessionStateView, StoredBranch};
use tracing::warn;

use super::{EventLoop, InternalEvent, LOCK_LOST_REPLACEMENT_ERR, SessionLockState};
use crate::AppSession;
use crate::app::RESTORE_IN_PROGRESS;
use crate::storage_writer::{
    CurrentSessionCheckpoint, RestoredSessionCheckpoint, SessionSaveFence,
};

#[cfg(test)]
pub(crate) mod tests;

const FILE_RESTORE_UNSUPPORTED: &str =
    "File restore is not supported on this platform; choose Conversation";
const RESTORE_CANCELLED: &str = "Restore cancelled";
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

struct PendingParentFiles(Option<RestoreFiles>);

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
    receipt: RestoreFiles,
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

    pub(super) fn complete_files_restore(
        &mut self,
        idx: usize,
        result: Result<Box<FilesRestore>, String>,
    ) {
        let restored = match result {
            Ok(restored) => restored,
            Err(error) => {
                self.restore_failed(idx, error);
                return;
            }
        };
        let runtime = &mut self.sessions[idx];
        Arc::make_mut(&mut runtime.app.state.session).push_branch(restored.branch);
        Arc::make_mut(&mut runtime.app.state.session).push_restore_ref(restored.reference);
        runtime.app.storage_fence = restored.fence;
        let checkpoint = self.ctx.storage_writer.coordinator_checkpoint();
        checkpoint.bind(
            runtime
                .session_lock
                .as_ref()
                .and_then(SessionLockState::publication_guard),
        );
        if let Err(error) = runtime.coordinator.rebind_checkpoint_writer(checkpoint) {
            self.restore_failed(idx, format!("{PUBLISHED_INSTALL_FAILED}: {error}"));
            return;
        }
        for warning in restored.warnings {
            runtime.app.flash(warning);
        }
        runtime.app.restore_in_progress = false;
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
        let source_id = runtime.id();
        let sessions_dir = self.ctx.sessions_dir.clone();
        let storage = self.ctx.storage.clone();
        let source_session = runtime.app.state.session.as_ref().clone();
        let source_lease = runtime.share_session_lease()?;
        let supplied = BTreeMap::from([(source_id, owned_lease(&source_lease))]);
        let (parent, link, route_leases, mut segments) = parent_route(
            &source_session,
            session,
            seq,
            &sessions_dir,
            &storage,
            supplied,
            Arc::clone(&self.ctx.file_write_locks),
        )?;
        if let Some(block) = session_lock::resume_block(&parent.cwd, &self.session_cwd, false) {
            return Err(block.to_string());
        }
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
            session: Arc::new(implementation.clone()),
            journal: implementation_journal.clone(),
            publication: CheckpointPublication::default(),
            claim: super::publication_claim(),
            guard: implementation_guard.clone(),
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
                if let Some(segment) = segments.last_mut() {
                    segment.session = implementation.clone();
                    segment.through_seq = implementation.sequence_high_water().unwrap_or_default();
                    segment.affected = implementation.agents().keys().copied().collect();
                }
                let preview = plan_restore_files(&segments).map_err(|error| error.to_string())?;
                let choice = confirm_restore(
                    &internal,
                    source_id,
                    identity.clone(),
                    &preview,
                    RestoreChoice::ConversationAndFiles,
                )
                .await?;
                if choice == RestoreChoice::FilesOnly {
                    let barrier = (
                        Arc::clone(&source_guard),
                        route_leases,
                        Arc::clone(&source_lease),
                    );
                    let result = publish_files_only(
                        &writer,
                        &sessions_dir,
                        implementation,
                        implementation_journal,
                        preview,
                        locks,
                        cancel,
                        &modes,
                        implementation_guard,
                        barrier,
                    )
                    .await;
                    let _ = internal.send(InternalEvent::FilesRestoreReady {
                        session: source_id,
                        runtime: identity.clone(),
                        result: result.map(Box::new),
                    });
                    return Ok(None);
                }
                let mut files: Option<RestoreFiles> = None;
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
                    let request = RestoreRequest {
                        sessions_dir: sessions_dir.clone(),
                        owner: source_id,
                        source: endpoint(&implementation, None)?,
                        target: endpoint(&parent, None)?,
                        choice: choice.clone(),
                        preview: if choice == RestoreChoice::ConversationOnly {
                            file_restore::RestorePreview::default()
                        } else {
                            preview
                        },
                        projection: Some(PlanProjection {
                            snapshot: replacement.snapshot().clone(),
                            affected: replacement.affected_agents().clone(),
                        }),
                        locks,
                        cancel: cancel.clone(),
                    };
                    let receipt = prepare_restore_files(
                        request,
                        (
                            Arc::clone(&source_guard),
                            route_leases,
                            Arc::clone(&source_lease),
                        ),
                    )
                    .await?;
                    files = Some(receipt);
                    let applied = files.as_mut().ok_or(super::NOT_LIVE_ERR)?.apply(&modes)?;
                    let warnings = applied.warnings;
                    replacement
                        .apply_configs(applied.changes)
                        .map_err(|error| error.to_string())?;
                    let snapshot = replacement.snapshot().clone();
                    let view = SessionStateView::fold(
                        &snapshot.state,
                        &snapshot.agent_revisions,
                        snapshot.seq,
                    )
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
                        transition: files.as_ref().ok_or(super::NOT_LIVE_ERR)?.transition_ref(),
                    };
                    session.push_branch(branch.clone());
                    if let Some(reference) =
                        files.as_ref().ok_or(super::NOT_LIVE_ERR)?.transition_ref()
                    {
                        session.push_restore_ref(reference);
                    }
                    writer.seed(Arc::new(parent));
                    writer.bind_journal(session.id, Arc::clone(&journal));
                    Ok(Some(PreparedParentRestore {
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
                    }))
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
            if let Ok(None) = result {
                return;
            }
            if let Err(error) = internal.send(InternalEvent::ParentRestoreReady {
                runtime: identity,
                result: result
                    .and_then(|value| value.ok_or_else(|| super::NOT_LIVE_ERR.to_owned()))
                    .map(Box::new),
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
        let sessions_dir = self.ctx.sessions_dir.clone();
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
                let mut rollback: Option<RestoreFiles> = None;
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
                    let preview = match &selection {
                        RestoreTarget::Boundary(boundary) => {
                            plan_restore_files(&[RestoreSegment {
                                session: current.clone(),
                                after_seq: boundary.seq,
                                through_seq: journal.snapshot().seq,
                                affected: held.lock().await.affected_agents().clone(),
                            }])
                        }
                        RestoreTarget::Branch(id) => match current
                            .branches()
                            .iter()
                            .find(|branch| branch.id == *id)
                            .and_then(|branch| branch.transition.as_ref())
                        {
                            Some(reference) => inverse_restore_files(&sessions_dir, reference),
                            None => Ok(file_restore::RestorePreview::default()),
                        },
                    }
                    .map_err(|error| error.to_string())?;
                    let choice = confirm_restore(
                        &internal,
                        session_id,
                        identity.clone(),
                        &preview,
                        RestoreChoice::ConversationOnly,
                    )
                    .await?;
                    if choice == RestoreChoice::FilesOnly {
                        let restored = publish_files_only(
                            &writer,
                            &sessions_dir,
                            current,
                            journal.checkpoint_snapshot()?,
                            preview,
                            file_write_locks,
                            cancel.clone(),
                            &modes,
                            guard.clone(),
                            (Arc::clone(&held), Arc::clone(&source_lease)),
                        )
                        .await;
                        let _ = internal.send(InternalEvent::FilesRestoreReady {
                            session: session_id,
                            runtime: identity.clone(),
                            result: restored.map(Box::new),
                        });
                        return Ok(None);
                    }
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
                    let request = RestoreRequest {
                        sessions_dir: sessions_dir.clone(),
                        owner: session_id,
                        source: endpoint(&current, None)?,
                        target: match &selection {
                            RestoreTarget::Boundary(_) => endpoint(&current, None)?,
                            RestoreTarget::Branch(id) => {
                                endpoint(restored.as_ref().ok_or(super::NOT_LIVE_ERR)?, Some(*id))?
                            }
                        },
                        choice: choice.clone(),
                        preview: if choice == RestoreChoice::ConversationOnly {
                            file_restore::RestorePreview::default()
                        } else {
                            preview
                        },
                        projection: Some(PlanProjection {
                            snapshot: replacement.snapshot().clone(),
                            affected: affected.clone(),
                        }),
                        locks: file_write_locks,
                        cancel: cancel.clone(),
                    };
                    let receipt = prepare_restore_files(
                        request,
                        (Arc::clone(&held), Arc::clone(&source_lease)),
                    )
                    .await?;
                    rollback = Some(receipt);
                    let applied = rollback
                        .as_mut()
                        .ok_or(super::NOT_LIVE_ERR)?
                        .apply(&modes)?;
                    let warnings = applied.warnings;
                    replacement
                        .apply_configs(applied.changes)
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
                    session.meta.queued_messages.clear();
                    if session_scope {
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
                        transition: rollback
                            .as_ref()
                            .ok_or(super::NOT_LIVE_ERR)?
                            .transition_ref(),
                    };
                    let mut authoritative = current;
                    authoritative
                        .replace_authoritative(session)
                        .map_err(|error| error.to_string())?;
                    authoritative.push_branch(branch.clone());
                    if let Some(reference) = rollback
                        .as_ref()
                        .ok_or(super::NOT_LIVE_ERR)?
                        .transition_ref()
                    {
                        authoritative.push_restore_ref(reference);
                    }
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
                    Ok(Some(InstalledRestore {
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
                    }))
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
            if let Ok(None) = result {
                return;
            }
            let _ = internal.send(InternalEvent::RestoreReady {
                session: session_id,
                runtime: identity,
                result: result
                    .and_then(|value| value.ok_or_else(|| super::NOT_LIVE_ERR.to_owned()))
                    .map(Box::new),
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

async fn reconcile_owned_sessions(
    sessions_dir: &Path,
    session: MakimaId,
    supplied: BTreeMap<MakimaId, OwnedRestoreLease>,
    locks: Arc<FileWriteLocks>,
    cancel: CancelToken,
) -> Result<(), file_restore::RestoreError> {
    #[cfg(unix)]
    {
        file_restore::reconcile_session_guarded(sessions_dir, session, supplied, locks, cancel)
            .await
    }
    #[cfg(not(unix))]
    {
        let _ = (supplied, locks, cancel);
        let state = sessions_dir
            .parent()
            .ok_or_else(|| file_restore::RestoreError::Invalid(FILE_RESTORE_UNSUPPORTED.into()))?;
        if RestoreStore::new(StateDir::from_path(state.to_path_buf()))
            .pending_for_session(session)?
            .is_empty()
        {
            Ok(())
        } else {
            Err(file_restore::RestoreError::Invalid(
                FILE_RESTORE_UNSUPPORTED.into(),
            ))
        }
    }
}

pub(super) fn reconcile_startup(
    sessions_dir: &Path,
    _storage: &StateDir,
    session: MakimaId,
    lease: Option<ClaimedSessionLock>,
    locks: Arc<FileWriteLocks>,
) -> Result<Option<ClaimedSessionLock>, String> {
    let Some(lease) = lease else {
        return Err(format!(
            "{ABORT_REPAIR_FAILED}: startup requires an owned session lease"
        ));
    };
    let lease = Arc::new(super::RenewingDeletionLease::new(lease));
    let supplied = BTreeMap::from([(session, owned_lease(&lease))]);
    smol::block_on(reconcile_owned_sessions(
        sessions_dir,
        session,
        supplied,
        locks,
        CancelToken::none(),
    ))
    .map_err(|error| format!("{ABORT_REPAIR_FAILED}: {error}"))?;
    Arc::try_unwrap(lease)
        .map_err(|_| super::NOT_LIVE_ERR.to_owned())?
        .into_claimed_lock()
        .map(Some)
}

type RouteLease = Arc<super::RenewingDeletionLease>;
type ParentRoute = (
    AppSession,
    ParentLink,
    Vec<(MakimaId, RouteLease)>,
    Vec<RestoreSegment>,
);

fn owned_lease(lease: &RouteLease) -> OwnedRestoreLease {
    OwnedRestoreLease {
        guard: lease.publication_guard(),
        keepalive: lease.clone(),
    }
}

pub(super) struct FilesRestore {
    branch: StoredBranch,
    reference: TransitionRef,
    fence: SessionSaveFence,
    warnings: Vec<String>,
}

fn endpoint(session: &AppSession, branch: Option<MakimaId>) -> Result<RestoreEndpoint, String> {
    Ok(RestoreEndpoint {
        session: session.id,
        branch,
        seq: session.sequence_high_water().unwrap_or_default(),
        digest: file_restore::endpoint_digest(session).map_err(|error| error.to_string())?,
    })
}

async fn confirm_restore(
    internal: &flume::Sender<InternalEvent>,
    session: MakimaId,
    runtime: Arc<()>,
    preview: &file_restore::RestorePreview,
    default_choice: RestoreChoice,
) -> Result<RestoreChoice, String> {
    let (reply, choice) = flume::bounded(1);
    internal
        .send(InternalEvent::RestorePreview {
            session,
            runtime,
            preview: crate::components::restore_form::RestorePreview {
                affected_paths: preview.paths.iter().map(|path| path.path.clone()).collect(),
                shell_count: preview.shell_commands,
            },
            default_choice: if cfg!(unix) {
                default_choice
            } else {
                RestoreChoice::ConversationOnly
            },
            reply,
        })
        .map_err(|_| super::RUNTIME_GONE_ERR.to_owned())?;
    let choice = choice
        .recv_async()
        .await
        .map_err(|_| RESTORE_CANCELLED.to_owned())?;
    validate_restore_choice(&choice)?;
    Ok(choice)
}

#[cfg(unix)]
fn protect_restore_receipt(
    result: Result<PreparedRestore, file_restore::RestoreError>,
    barrier: impl Any + Send,
) -> Result<PreparedRestore, String> {
    match result {
        Ok(mut receipt) => {
            receipt.retain_barrier(barrier);
            Ok(receipt)
        }
        Err(file_restore::RestoreError::Unresolved {
            reason,
            mut receipt,
        }) => {
            receipt.retain_barrier(barrier);
            match (*receipt).abort() {
                Ok(()) => Err(reason),
                Err(error) => Err(format!("{ABORT_REPAIR_FAILED}: {error}; {reason}")),
            }
        }
        Err(error) => Err(error.to_string()),
    }
}

enum RestoreFiles {
    #[cfg(unix)]
    Secure(PreparedRestore),
    #[cfg(not(unix))]
    Legacy {
        projection: PlanProjectionRollback,
        snapshot: SessionJournalSnapshot,
        affected: BTreeSet<AgentId>,
        barriers: Vec<Box<dyn Any + Send>>,
    },
}

impl RestoreFiles {
    fn transition_ref(&self) -> Option<TransitionRef> {
        match self {
            #[cfg(unix)]
            Self::Secure(receipt) => Some(receipt.transition_ref()),
            #[cfg(not(unix))]
            Self::Legacy { .. } => None,
        }
    }
    fn retain_barrier(&mut self, barrier: impl Any + Send) {
        match self {
            #[cfg(unix)]
            Self::Secure(receipt) => receipt.retain_barrier(barrier),
            #[cfg(not(unix))]
            Self::Legacy { barriers, .. } => barriers.push(Box::new(barrier)),
        }
    }
    fn apply(&mut self, modes: &ModeRegistry) -> Result<AppliedRestore, String> {
        match self {
            #[cfg(unix)]
            Self::Secure(receipt) => receipt.apply(modes).map_err(|error| error.to_string()),
            #[cfg(not(unix))]
            Self::Legacy {
                projection,
                snapshot,
                affected,
                ..
            } => {
                let (changes, warnings) = projection.project(snapshot, affected, modes)?;
                Ok(AppliedRestore { changes, warnings })
            }
        }
    }
    fn abort(self) -> Result<(), String> {
        match self {
            #[cfg(unix)]
            Self::Secure(receipt) => receipt.abort().map_err(|error| error.to_string()),
            #[cfg(not(unix))]
            Self::Legacy { projection, .. } => projection.rollback(),
        }
    }
    fn complete(self) -> Result<(), String> {
        match self {
            #[cfg(unix)]
            Self::Secure(receipt) => receipt.complete().map_err(|error| error.to_string()),
            #[cfg(not(unix))]
            Self::Legacy { .. } => Ok(()),
        }
    }
}

#[cfg(all(test, not(unix)))]
#[test_case::test_case(RestoreChoice::ConversationOnly, true)]
#[test_case::test_case(RestoreChoice::ConversationAndFiles, false)]
#[test_case::test_case(RestoreChoice::FilesOnly, false)]
fn platform_restore_choice_is_explicit(choice: RestoreChoice, supported: bool) {
    let result = validate_restore_choice(&choice);
    assert_eq!(result.is_ok(), supported);
    if !supported {
        assert_eq!(result.unwrap_err(), FILE_RESTORE_UNSUPPORTED);
    }
}

fn validate_restore_choice(choice: &RestoreChoice) -> Result<(), String> {
    if !cfg!(unix) && *choice != RestoreChoice::ConversationOnly {
        return Err(FILE_RESTORE_UNSUPPORTED.into());
    }
    Ok(())
}

async fn prepare_restore_files(
    request: RestoreRequest,
    barrier: impl Any + Send,
) -> Result<RestoreFiles, String> {
    validate_restore_choice(&request.choice)?;
    #[cfg(unix)]
    {
        protect_restore_receipt(file_restore::prepare(request).await, barrier)
            .map(RestoreFiles::Secure)
    }
    #[cfg(not(unix))]
    {
        let projection = request.projection.ok_or(FILE_RESTORE_UNSUPPORTED)?;
        let captured = PlanProjectionRollback::capture_locked(
            &projection.snapshot,
            &projection.affected,
            &request.locks,
            &request.cancel,
        )
        .await?;
        Ok(RestoreFiles::Legacy {
            projection: captured,
            snapshot: projection.snapshot,
            affected: projection.affected,
            barriers: vec![Box::new(barrier)],
        })
    }
}

fn plan_restore_files(
    segments: &[RestoreSegment],
) -> Result<file_restore::RestorePreview, file_restore::RestoreError> {
    #[cfg(unix)]
    {
        file_restore::plan(segments)
    }
    #[cfg(not(unix))]
    {
        let _ = segments;
        Ok(file_restore::RestorePreview::default())
    }
}

fn inverse_restore_files(
    sessions_dir: &Path,
    reference: &TransitionRef,
) -> Result<file_restore::RestorePreview, file_restore::RestoreError> {
    #[cfg(unix)]
    {
        file_restore::plan_inverse(sessions_dir, reference)
    }
    #[cfg(not(unix))]
    {
        let _ = (sessions_dir, reference);
        Ok(file_restore::RestorePreview::default())
    }
}

#[allow(clippy::too_many_arguments)]
async fn publish_files_only(
    writer: &crate::storage_writer::StorageWriter,
    sessions_dir: &Path,
    mut session: AppSession,
    snapshot: SessionJournalSnapshot,
    preview: file_restore::RestorePreview,
    locks: Arc<FileWriteLocks>,
    cancel: CancelToken,
    modes: &ModeRegistry,
    guard: SessionPublicationGuard,
    barrier: impl Any + Send,
) -> Result<FilesRestore, String> {
    validate_restore_choice(&RestoreChoice::FilesOnly)?;
    let root = session.root_agent_id().ok_or(super::NOT_LIVE_ERR)?;
    let endpoint = endpoint(&session, None)?;
    let request = RestoreRequest {
        sessions_dir: sessions_dir.to_path_buf(),
        owner: session.id,
        source: endpoint.clone(),
        target: endpoint,
        choice: RestoreChoice::FilesOnly,
        preview,
        projection: None,
        locks,
        cancel: cancel.clone(),
    };
    let mut receipt = prepare_restore_files(request, barrier).await?;
    let applied = match receipt.apply(modes) {
        Ok(applied) => applied,
        Err(error) => {
            return match receipt.abort() {
                Ok(()) => Err(error.to_string()),
                Err(repair) => Err(format!("{ABORT_REPAIR_FAILED}: {repair}; {error}")),
            };
        }
    };
    let reference = receipt.transition_ref().ok_or(super::NOT_LIVE_ERR)?;
    let branch = StoredBranch {
        id: MakimaId::generate(),
        created_at: now_epoch(),
        at_seq: snapshot.seq,
        agent: root,
        offset: session.messages().len(),
        transition: Some(reference.clone()),
    };
    session.push_branch(branch.clone());
    session.push_restore_ref(reference.clone());
    let publication = CheckpointPublication::default();
    let _registration = cancel.register_publication(&publication);
    let result = writer
        .checkpoint_files_restore(RestoredSessionCheckpoint {
            session_id: session.id,
            session: Arc::new(session),
            journal: snapshot,
            branch: branch.clone(),
            publication,
            claim: super::publication_claim(),
            guard,
        })
        .await;
    match result {
        Ok(fence) => {
            writer
                .await_publication_sync(&fence, &cancel)
                .await
                .map_err(|error| format!("{ABORT_REPAIR_FAILED}: {error}"))?;
            receipt
                .complete()
                .map_err(|error| format!("{ABORT_REPAIR_FAILED}: {error}"))?;
            Ok(FilesRestore {
                branch,
                reference,
                fence,
                warnings: applied.warnings,
            })
        }
        Err(error) => match receipt.abort() {
            Ok(()) => Err(error),
            Err(repair) => Err(format!("{ABORT_REPAIR_FAILED}: {repair}; {error}")),
        },
    }
}

fn parent_route(
    source: &AppSession,
    target: MakimaId,
    seq: u64,
    sessions_dir: &Path,
    storage: &StateDir,
    mut supplied: BTreeMap<MakimaId, OwnedRestoreLease>,
    locks: Arc<FileWriteLocks>,
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
        let lease = Arc::new(super::RenewingDeletionLease::new(lease));
        supplied.insert(id, owned_lease(&lease));
        leases.push((id, lease));
    }
    for (id, _) in &leases {
        smol::block_on(reconcile_owned_sessions(
            sessions_dir,
            *id,
            supplied.clone(),
            Arc::clone(&locks),
            CancelToken::none(),
        ))
        .map_err(|error| format!("{ABORT_REPAIR_FAILED}: {error}"))?;
    }
    for session in route.iter().filter(|session| session.id != source.id) {
        let reloaded = AppSession::load(session.id, storage).map_err(|error| error.to_string())?;
        if file_restore::endpoint_digest(session).map_err(|error| error.to_string())?
            != file_restore::endpoint_digest(&reloaded).map_err(|error| error.to_string())?
            || session.parent() != reloaded.parent()
        {
            return Err(INVALID_PARENT_ROUTE.into());
        }
    }
    route.reverse();
    let parent = route.first().cloned().ok_or(super::NOT_LIVE_ERR)?;
    let mut segments = Vec::new();
    for (index, session) in route.iter().enumerate() {
        let through_seq = route
            .get(index + 1)
            .and_then(|child| child.parent())
            .map_or_else(
                || session.sequence_high_water().unwrap_or_default(),
                |link| link.seq,
            );
        segments.push(RestoreSegment {
            session: session.clone(),
            after_seq: if index == 0 { seq } else { 0 },
            through_seq,
            affected: session.agents().keys().copied().collect(),
        });
    }
    plan_restore_files(&segments).map_err(|error| error.to_string())?;
    Ok((parent, link, leases, segments))
}

fn project_presentations(
    session: &mut AppSession,
    snapshot: &SessionJournalSnapshot,
    affected: &BTreeSet<AgentId>,
) -> Result<(), String> {
    let view = SessionStateView::fold(&snapshot.state, &snapshot.agent_revisions, snapshot.seq)
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
