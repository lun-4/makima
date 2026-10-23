//! Multi-session supervisor: every session owns an `App` + `AgentHandles` and
//! keeps draining agent events while backgrounded; only the focused session
//! renders and receives input. `SpawnCtx` carries the shared resources needed
//! to spawn session runtimes at any point.
//!
//! Terminal input arrives on a channel (see [`InputReader`]), so the loop
//! waits on every event source at once and wakes the moment a plugin action,
//! agent event, or keypress arrives instead of sleeping in `event::poll`.

use std::collections::HashMap;
use std::future::Future;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use color_eyre::Result;
use color_eyre::eyre::{Context, eyre};

use crossterm::event::{
    Event, KeyEventKind, MouseButton, MouseEvent as CtMouseEvent, MouseEventKind,
};
use maki_agent::actor::{
    ActorEvent, ConfigChange, ConfigCommit, ConfigPatch, HistoryChange, PreparedModel,
    PreparedTransaction,
};
use maki_agent::command::CustomCommand;
use maki_agent::config_change::{
    ConfigChangeError, ConfigChangeTicket, prepare_model, prepare_patch,
};
#[cfg(test)]
use maki_agent::permissions::PermissionAnswer;
use maki_agent::permissions::PermissionManager;
use maki_agent::session_coordinator::{
    ModelPreparer, PreparedSessionCoordinator, SessionCoordinatorHandle, SessionCoordinatorParams,
    builtin_option_definitions, canonical_directory, publication_claim,
};
use maki_agent::session_options::{
    ENABLED_VALUE, FAST_OPTION_ID, SessionOptionOwner, SessionOptionsSnapshot, THINKING_OPTION_ID,
    WORKFLOW_OPTION_ID, YOLO_OPTION_ID,
};
use maki_agent::tools::offload::OffloadStore;
use maki_agent::{
    AgentConfig, AgentEvent, AgentRef, CancelToken, Envelope, McpCommand, McpConfigErrors,
    McpHandle, McpSnapshotReader, mcp,
};
use maki_commands::{BuiltinOperation, CommandError, HostResponse};
use maki_config::project::TrustQuestion;
use maki_config::{ModelPolicy, ProjectConfig, SessionDefaults, UiConfig};
use maki_lua::orchestration::{
    AgentOrchestration, ConfigMutationReserver, DURABLE_CONFIG_UNAVAILABLE, Discovery,
    ModelCatalog, OrchestrationServices, TrustedTarget, project_yolo,
};
use maki_lua::session_snapshot::{
    STATUS_IDLE, STATUS_NEEDS_INPUT, STATUS_WORKING, SessionSnapshot, session_usage,
};
use maki_lua::ui_sink::UiSink;
use maki_lua::{
    EventHandle, HintReader, KeymapReader, ProviderUsageAck, ProviderUsageInvalidation,
    ProviderUsageLimit, ProviderUsageReply, ProviderUsageSnapshot, ProviderUsageWindow,
    SessionRequest, StatusContentReader, UiAction, UiReply,
};
use maki_providers::Timeouts;
use maki_providers::provider::{Provider, from_model};
use maki_providers::{Message, Model};
use maki_storage::StateDir;
use maki_storage::StorageError;
use maki_storage::checkpoint::CheckpointPublication;
use maki_storage::id::{MakiId, MakiIdParseError, SessionRef};
use maki_storage::session_lock::{self, ClaimedSessionLock};
use maki_storage::sessions::{SESSIONS_DIR, SessionError, normalize_title};
use serde_json::json;
use tracing::{info, warn};

fn claim_lock(dir: &Path, id: &MakiId) -> Result<ClaimedSessionLock> {
    session_lock::claim(dir, id)?.ok_or_else(|| eyre!(session_lock::OPEN_ELSEWHERE_MSG))
}

use crate::AppSession;
use crate::agent::{
    AgentCommand, AgentHandles, PreparedAgentHandles, ProviderChange, ProviderSlot,
    SystemPromptOverride, shared_queue::QueueItem,
};
use crate::app::EMPTY_PROMPT_ERR;
use crate::app::session_state::SessionState;
use crate::app::shell::{ShellEvent, spawn_shell};
use crate::app::{
    App, Msg, Notification, PreparedApp, SubmitOutcome, session_has_content, turn_response,
};
use crate::color_compat;
use crate::command_runtime::{CLOSED_TARGET_ERR, CommandEvent, CommandRuntime};
use crate::components::arg_completion::{ModelArgSource, ThemeArgSource};
use crate::components::input::Submission;
use crate::components::{
    Action, ExitRequest, SessionReplacementKind, SessionReplacementRequest, Status,
};
use crate::input::InputReader;
use crate::plan_approval::PrepareProvider;
use crate::provider_usage::{
    ProviderIdentity, ProviderUsageCoordinator, ProviderUsageFetch, ProviderUsageFetchId,
    ProviderUsageFetchResult, ProviderUsageInput, ProviderUsageOutput, ProviderUsageRequestKind,
    ProviderUsageResolution,
};
use crate::repaint::{Dirty, IDLE_POLL};

use crate::storage_writer::{
    CoordinatorCheckpointWriter, LOCK_LOST_REPLACEMENT_ERR, StorageWriter,
};
use crate::terminal;
use crate::theme::ThemesProvider;

mod approval;
#[cfg(test)]
mod discovery_tests;
#[cfg(test)]
mod focus_tests;

use approval::{ApprovalStep, PendingPlanApproval};

/// Max events handled per frame so a flood cannot starve rendering.
const DRAIN_BUDGET: usize = 256;
const AGENT_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(3);
const SESSION_OP_TIMEOUT: Duration = Duration::from_secs(30);
const SESSION_OP_TIMEOUT_ERR: &str = "session operation timed out";
const CONFIG_EXPIRED_ERR: &str = "configuration preparation expired";
const UNSUPPORTED_AGENT_OPTION_PREFIX: &str = "unsupported agent option";
const DELETE_FOCUSED_ERR: &str = "cannot delete the focused session";
const PROVIDER_USAGE_CHANGED_ERR: &str = "provider changed while fetching usage";
const PROVIDER_USAGE_SHUTDOWN_ERR: &str = "UI shut down while fetching usage";
const NOT_LIVE_ERR: &str = "session not live";
const CHILD_HISTORY_IDENTITY_ERR: &str = "child history storage identity is unavailable";
const LOCK_LOST_MSG: &str = "Session lock lost to another process; stopping without saving";
const LOCK_UNAVAILABLE_REPLACEMENT_ERR: &str =
    "session lock ownership is unavailable; replacement is disabled";

static NEXT_RUNTIME_GENERATION: AtomicU64 = AtomicU64::new(1);

/// Tabs carry their in-memory sessions so `/reload` reopens them without a
/// disk round-trip; `session_has_content` tells which ones were saved.
pub(crate) struct ShutdownReport {
    pub exit: ExitRequest,
    pub tabs: Vec<AppSession>,
    pub focused: usize,
}

pub struct EventLoopParams {
    pub cwd: PathBuf,
    pub model: Model,
    pub needs_login: bool,
    pub explicit_model: bool,
    pub commands: Vec<CustomCommand>,
    pub sessions: Vec<AppSession>,
    pub focused: usize,
    pub startup_warnings: Vec<String>,
    pub storage: StateDir,
    pub config: AgentConfig,
    pub ui_config: UiConfig,
    pub input_history_size: usize,
    pub permissions: Arc<PermissionManager>,
    pub timeouts: Timeouts,
    pub exit_on_done: bool,
    pub command_registry: maki_commands::CommandRegistry,
    /// One-shot startup request to open the session picker (bare `-c`).
    pub session_picker: bool,
    pub keymap_reader: KeymapReader,
    pub hint_reader: HintReader,
    pub status_content_reader: StatusContentReader,
    pub ui_action_rx: flume::Receiver<UiAction>,
    pub lua_event_handle: EventHandle,
    pub model_policy: Arc<ModelPolicy>,
    pub session_defaults: SessionDefaults,
    pub project_config: ProjectConfig,
    pub trust_question: Option<TrustQuestion>,
    pub startup_notice: Option<String>,
    pub system_prompt_override: Option<String>,
    pub append_system_prompt: Option<String>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum SessionStatus {
    Working,
    NeedsInput,
    Idle,
}

enum PendingCompletion {
    WaitingForQueueDrain(Notification),
    Due(Notification),
}

#[derive(Default)]
struct RunNotificationState {
    response_candidate: Option<String>,
    pending_completion: Option<PendingCompletion>,
    last_attention: Option<Notification>,
}

impl RunNotificationState {
    fn reset(&mut self) {
        *self = Self::default();
    }

    fn on_queue_item_consumed(&mut self) {
        self.response_candidate = None;
        self.pending_completion = None;
    }

    fn on_turn_complete(&mut self, message: &Message) {
        self.response_candidate = turn_response(message);
    }

    fn on_done(&mut self, event: &AgentEvent) {
        let notification = match event {
            AgentEvent::TurnOutcome(maki_agent::TurnOutcome::Completed { .. })
            | AgentEvent::TurnOutcome(maki_agent::TurnOutcome::Cancelled { .. }) => {
                Notification::TurnComplete {
                    response: self.response_candidate.take(),
                }
            }
            AgentEvent::TurnOutcome(maki_agent::TurnOutcome::Failed { .. })
            | AgentEvent::ControlError { .. } => {
                self.response_candidate = None;
                Notification::error_completion()
            }
            _ => return,
        };
        self.pending_completion = Some(PendingCompletion::WaitingForQueueDrain(notification));
    }

    fn on_drain(&mut self) {
        self.pending_completion = match self.pending_completion.take() {
            Some(PendingCompletion::WaitingForQueueDrain(notification)) => {
                Some(PendingCompletion::Due(notification))
            }
            pending => pending,
        };
    }

    fn on_manual_exit(&mut self) {
        self.pending_completion = None;
    }

    /// True between `Done`/`Error` and the run's `QueueSettled`. An exit must
    /// not fire in that window: a queued follow-up may still start a new run.
    fn waiting_for_drain(&self) -> bool {
        matches!(
            self.pending_completion,
            Some(PendingCompletion::WaitingForQueueDrain(_))
        )
    }

    fn reconcile(
        &mut self,
        attention: Option<Notification>,
        status: SessionStatus,
        queue_empty: bool,
        terminal_focused: bool,
    ) -> Option<Notification> {
        let settled = attention.is_none() && status == SessionStatus::Idle && queue_empty;
        let prompt = (attention != self.last_attention)
            .then(|| attention.clone())
            .flatten();
        self.last_attention = attention;

        // A due completion is decided on its first reconcile: fire if the
        // session settled, otherwise drop it for good.
        let completion = match self.pending_completion.take() {
            Some(PendingCompletion::Due(notification)) => settled.then_some(notification),
            waiting => {
                self.pending_completion = waiting;
                None
            }
        };
        (!terminal_focused).then(|| prompt.or(completion)).flatten()
    }
}

impl SessionStatus {
    fn of(app: &App) -> Self {
        if app.awaiting_input() {
            Self::NeedsInput
        } else if app.status == Status::Streaming {
            Self::Working
        } else {
            Self::Idle
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Working => STATUS_WORKING,
            Self::NeedsInput => STATUS_NEEDS_INPUT,
            Self::Idle => STATUS_IDLE,
        }
    }
}

fn prepend_preamble(preamble: &mut Vec<Message>, mut leading: Vec<Message>) {
    leading.append(preamble);
    *preamble = leading;
}

fn is_current_top_level(current_run_id: u64, envelope: &Envelope) -> bool {
    envelope.presentation() == Some(current_run_id) && envelope.subagent.is_none()
}

fn select_notification(
    selected: Option<Notification>,
    candidate: Option<Notification>,
) -> Option<Notification> {
    match (selected, candidate) {
        (Some(current), Some(candidate)) if candidate.is_urgent() && !current.is_urgent() => {
            Some(candidate)
        }
        (selected @ Some(_), _) => selected,
        (None, candidate) => candidate,
    }
}

#[cfg(not(windows))]
fn terminal_focus_event(event: &Event) -> Option<bool> {
    match event {
        Event::FocusGained => Some(true),
        Event::FocusLost => Some(false),
        _ => None,
    }
}

#[cfg(windows)]
fn terminal_focus_event(_event: &Event) -> Option<bool> {
    None
}

#[cfg(not(windows))]
fn terminal_input_proves_focus(event: &Event) -> bool {
    match event {
        Event::Key(key) => key.kind == KeyEventKind::Press,
        Event::Paste(_) | Event::Mouse(_) => true,
        _ => false,
    }
}

#[cfg(windows)]
fn terminal_input_proves_focus(_event: &Event) -> bool {
    false
}

fn route_terminal_lifecycle(app: &mut App, event: &Event) {
    if matches!(event, Event::FocusLost | Event::Resize(..)) {
        let dirty = app.cancel_middle_scroll();
        app.pending_dirty |= dirty;
    }
}

fn assign_session_focus(outgoing: &mut App, focused: &mut usize, next: usize) {
    if *focused != next {
        let dirty = outgoing.cancel_middle_scroll();
        outgoing.pending_dirty |= dirty;
        *focused = next;
    }
}

fn prepare_terminal_handoff<'a>(
    apps: impl IntoIterator<Item = &'a mut App>,
    terminal_focused: &mut bool,
) {
    for app in apps {
        let dirty = app.cancel_middle_scroll();
        app.pending_dirty |= dirty;
    }
    *terminal_focused = false;
}

fn tick_session(app: &mut App, focused: bool, now: Instant) -> (Dirty, Vec<Action>) {
    let actions = app.poll_login_picker();
    let mut dirty = Dirty::NO;
    if focused {
        dirty |= app.tick_at(now);
    } else {
        dirty |= app.float_mgr.tick();
        dirty |= app.tick_edge_scroll();
        dirty |= app.tick_error_expiry();
        dirty |= app.poll_image_paste();
        dirty |= app.btw_modal.poll();
        dirty |= app.status_bar.poll_branch_update();
        dirty |= app.mcp_picker.refresh();
    }
    (dirty, actions)
}

fn parse_session_id(id: &str) -> Result<MakiId, String> {
    id.parse().map_err(|e: MakiIdParseError| e.to_string())
}

enum SessionLockState {
    Held(ClaimedSessionLock),
    InFlight(flume::Receiver<HeartbeatCompletion>),
}

struct HeartbeatCompletion {
    result: io::Result<session_lock::LockBeat>,
    lease: Option<ClaimedSessionLock>,
}

impl Drop for HeartbeatCompletion {
    fn drop(&mut self) {
        if let Some(lease) = self.lease.take() {
            let _ = lease.release();
        }
    }
}

fn mark_runtime_lock_lost(runtime: &mut SessionRuntime) {
    runtime.session_lock = None;
    runtime.lock_lost = true;
    runtime.end_plan_approval();
    runtime.app.flash(LOCK_LOST_MSG.into());
    runtime.app.exit_request = ExitRequest::Error;
    let _ = runtime.handles.cmd_tx.try_send(AgentCommand::CancelAll);
    warn!(id = %runtime.id(), "session lock lost to another process; stopping without saving");
}

fn apply_heartbeat_completion(runtime: &mut SessionRuntime, mut completion: HeartbeatCompletion) {
    match completion.result {
        Ok(session_lock::LockBeat::Held | session_lock::LockBeat::Claimed) => {
            let lease = completion.lease.take().expect("heartbeat completion lease");
            runtime.session_lock = Some(SessionLockState::Held(lease));
        }
        Ok(session_lock::LockBeat::Lost) => mark_runtime_lock_lost(runtime),
        Err(ref error) => {
            let lease = completion.lease.take().expect("heartbeat completion lease");
            runtime.session_lock = Some(SessionLockState::Held(lease));
            warn!(id = %runtime.id(), %error, "session lock heartbeat failed");
        }
    }
}

fn complete_runtime_heartbeat(runtime: &mut SessionRuntime) {
    let completion_rx = match runtime.session_lock.take() {
        Some(SessionLockState::InFlight(completion_rx)) => completion_rx,
        state => {
            runtime.session_lock = state;
            return;
        }
    };
    let Some(completion) = collect_heartbeat(completion_rx.clone(), None) else {
        runtime.session_lock = Some(SessionLockState::InFlight(completion_rx));
        return;
    };
    apply_heartbeat_completion(runtime, completion);
}

fn start_runtime_heartbeat(
    runtime: &mut SessionRuntime,
    internal_tx: &flume::Sender<InternalEvent>,
) {
    start_runtime_heartbeat_with(runtime, internal_tx, |mut lease| {
        let result = lease.heartbeat();
        (lease, result)
    });
}

fn start_runtime_heartbeat_with<F>(
    runtime: &mut SessionRuntime,
    internal_tx: &flume::Sender<InternalEvent>,
    heartbeat: F,
) where
    F: FnOnce(ClaimedSessionLock) -> (ClaimedSessionLock, io::Result<session_lock::LockBeat>)
        + Send
        + 'static,
{
    let lease = match runtime.session_lock.take() {
        Some(SessionLockState::Held(lease)) => lease,
        state => {
            runtime.session_lock = state;
            return;
        }
    };
    let runtime_generation = runtime.generation;
    let internal_tx = internal_tx.clone();
    let (completion_tx, completion_rx) = flume::bounded(1);
    smol::spawn(async move {
        let (lease, result) = smol::unblock(move || heartbeat(lease)).await;
        let completion = HeartbeatCompletion {
            result,
            lease: Some(lease),
        };
        if completion_tx.send(completion).is_ok() {
            let _ = internal_tx.send(InternalEvent::SessionHeartbeat(runtime_generation));
        }
    })
    .detach();
    runtime.session_lock = Some(SessionLockState::InFlight(completion_rx));
}

fn collect_heartbeat(
    completion_rx: flume::Receiver<HeartbeatCompletion>,
    timeout: Option<Duration>,
) -> Option<HeartbeatCompletion> {
    match timeout {
        Some(timeout) => completion_rx.recv_timeout(timeout).ok(),
        None => completion_rx.try_recv().ok(),
    }
}

struct RenewingDeletionLease {
    lease: Arc<Mutex<ClaimedSessionLock>>,
    wake: flume::Sender<()>,
    done: flume::Receiver<()>,
}

impl RenewingDeletionLease {
    fn new(lease: ClaimedSessionLock) -> Self {
        let lease = Arc::new(Mutex::new(lease));
        let worker_lease = Arc::clone(&lease);
        let (wake, wake_rx) = flume::bounded::<()>(1);
        let (done_tx, done) = flume::bounded(1);
        std::thread::spawn(move || {
            loop {
                match worker_lease
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .heartbeat()
                {
                    Ok(session_lock::LockBeat::Lost) => break,
                    Err(error) => warn!(%error, "pending deletion heartbeat failed"),
                    Ok(_) => {}
                }
                match wake_rx.recv_timeout(session_lock::HEARTBEAT_INTERVAL) {
                    Err(flume::RecvTimeoutError::Timeout) => {}
                    Ok(()) | Err(flume::RecvTimeoutError::Disconnected) => break,
                }
            }
            drop(worker_lease);
            let _ = done_tx.send(());
        });
        Self { lease, wake, done }
    }

    fn publication_guard(&self) -> session_lock::SessionPublicationGuard {
        self.lease
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .publication_guard()
    }
}

impl Drop for RenewingDeletionLease {
    fn drop(&mut self) {
        let _ = self.wake.send(());
        let _ = self.done.recv_timeout(AGENT_SHUTDOWN_TIMEOUT);
    }
}

enum LockSettlement {
    Held(ClaimedSessionLock),
    Lost,
    TimedOut,
    None,
}

fn settle_lock_state(state: Option<SessionLockState>, timeout: Duration) -> LockSettlement {
    match state {
        Some(SessionLockState::Held(lease)) => LockSettlement::Held(lease),
        Some(SessionLockState::InFlight(completion_rx)) => {
            let Some(mut completion) = collect_heartbeat(completion_rx, Some(timeout)) else {
                return LockSettlement::TimedOut;
            };
            match completion.result {
                Ok(session_lock::LockBeat::Lost) => LockSettlement::Lost,
                Ok(session_lock::LockBeat::Held | session_lock::LockBeat::Claimed) | Err(_) => {
                    LockSettlement::Held(
                        completion.lease.take().expect("heartbeat completion lease"),
                    )
                }
            }
        }
        None => LockSettlement::None,
    }
}

fn release_lock_state(state: Option<SessionLockState>) -> io::Result<()> {
    match settle_lock_state(state, AGENT_SHUTDOWN_TIMEOUT) {
        LockSettlement::Held(lease) => lease.release(),
        LockSettlement::Lost | LockSettlement::None => Ok(()),
        LockSettlement::TimedOut => Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "session lock heartbeat did not finish before shutdown timeout",
        )),
    }
}

/// Commits mode `id` on `target` and reports it as one session op.
fn dispatch_mode_commit(
    runtime: &SessionRuntime,
    target: Result<AgentRef, String>,
    storage: &StateDir,
    modes: Arc<maki_agent::ModeRegistry>,
    services: Option<Arc<AgentOrchestration>>,
    internal_tx: flume::Sender<InternalEvent>,
    id: String,
) {
    let session = runtime.id();
    let identity = runtime.handles.identity();
    let reservation = target.map_err(ConfigChangeError::from).and_then(|target| {
        reserve_config_change(&target, &runtime.coordinator, services)
            .map(|reservation| (target, reservation))
    });
    let storage = storage.clone();
    smol::spawn(async move {
        let result = match reservation {
            Ok((target, reservation)) => reservation
                .commit_mode(&target, &modes, &id, &storage, None, async |_, _| {
                    Ok(publication_claim())
                })
                .await
                .map(|_| ()),
            Err(error) => Err(error),
        }
        .map_err(|error| error.to_string());
        let _ = internal_tx.send(InternalEvent::SessionOp {
            session,
            runtime: identity,
            kind: SessionOpKind::ModeChanged,
            result,
        });
    })
    .detach();
}

/// The agent a session's commands and keymaps address: the selected chat's
/// agent. A closed agent addresses nothing; there is no fallback to the root.
fn selected_agent(runtime: &SessionRuntime) -> Option<AgentRef> {
    let (manager, _) = runtime.handles.manager_and_root();
    manager.lookup(runtime.app.selected_agent_id()?).ok()
}

fn complete_option_toggle(app: &mut App, id: &'static str, enabled: bool) {
    app.apply_toggled_option(id, enabled);
}

fn observe_config_commits(
    handles: &AgentHandles,
    model_slot: Arc<ProviderSlot>,
    coordinator: SessionCoordinatorHandle,
    prepare: PrepareProvider,
) -> Result<flume::Receiver<ConfigCommit>, ConfigChangeError> {
    let (manager, root) = handles.manager_and_root();
    let (ui_tx, ui_rx) = flume::unbounded();
    let actor = manager.actor(root)?;
    {
        let target = manager.lookup(root);
        let timeouts = handles.orchestration_template.timeouts;
        let cwd = handles.cwd_slot();
        let permissions = Arc::clone(&handles.orchestration_template.permissions);
        project_yolo(&coordinator, Arc::clone(&permissions));
        let binding = target.map_err(ConfigChangeError::from).and_then(|target| {
            coordinator.bind_actor(
                target,
                model_preparer(prepare, timeouts),
                Arc::new(move |commit| {
                    model_slot.project(
                        commit.config.model.clone(),
                        Arc::clone(&commit.config.provider),
                    );
                    let path = commit.config.context.cwd().to_path_buf();
                    cwd.store(Arc::new(path.clone()));
                    permissions.set_cwd(path);
                }),
            )
        });
        if let Err(error) = binding {
            coordinator.retire();
            actor.close();
            return Err(error);
        }
        let commits = actor.subscribe_config_commits();
        smol::spawn(async move {
            while let Ok(commit) = commits.recv_async().await {
                let _ = ui_tx.send(commit);
            }
        })
        .detach();
    }
    Ok(ui_rx)
}

fn apply_options_to_session(session: &mut AppSession, snapshot: &SessionOptionsSnapshot) {
    for option in snapshot.options.iter() {
        let id = option.definition.id.as_ref();
        let enabled = option.current_value.as_ref() == ENABLED_VALUE;
        match id {
            YOLO_OPTION_ID => session.meta.yolo = Some(enabled),
            FAST_OPTION_ID => session.meta.fast = enabled,
            WORKFLOW_OPTION_ID => session.meta.workflow = enabled,
            THINKING_OPTION_ID => {
                session.meta.thinking = option
                    .current_value
                    .parse::<maki_agent::ThinkingConfig>()
                    .ok()
                    .map(Into::into);
            }
            _ if option.definition.persistent
                && matches!(option.definition.owner, SessionOptionOwner::Plugin { .. }) =>
            {
                session
                    .meta
                    .session_options
                    .insert(id.to_owned(), option.current_value.to_string());
            }
            _ => {}
        }
    }
}

fn checkpoint_runtime(runtime: &mut SessionRuntime) {
    if !runtime.lock_lost {
        runtime.app.checkpoint();
    }
}

fn rollback_startup_runtimes(mut runtimes: Vec<SessionRuntime>) {
    for runtime in &mut runtimes {
        if let Err(error) = release_lock_state(runtime.session_lock.take()) {
            warn!(id = %runtime.id(), %error, "startup rollback lock release failed");
        }
    }
    for runtime in runtimes {
        runtime.handles.shutdown().detach();
    }
}

struct CoordinatorRetirement(SessionCoordinatorHandle);

impl Drop for CoordinatorRetirement {
    fn drop(&mut self) {
        self.0.retire();
    }
}

struct SessionRuntime {
    generation: u64,
    config_commits: flume::Receiver<ConfigCommit>,
    /// The last root commit the app followed, so the next one can tell what
    /// changed.
    projected_config: Option<ConfigCommit>,
    app: App,
    handles: AgentHandles,
    model_slot: Arc<ProviderSlot>,
    coordinator: SessionCoordinatorHandle,
    _coordinator_retirement: CoordinatorRetirement,
    shell_tx: flume::Sender<ShellEvent>,
    shell_rx: flume::Receiver<ShellEvent>,
    last_status: SessionStatus,
    notifications: RunNotificationState,
    session_lock: Option<SessionLockState>,
    lock_lost: bool,
    restore_pending: bool,
    pending_approval: Option<PendingPlanApproval>,
}

struct PendingReplacement {
    prepared: PreparedSessionRuntime,
    kind: SessionReplacementKind,
}

#[derive(Clone)]
struct PreparedProvider {
    model: Model,
    provider: Arc<dyn Provider>,
}

struct PreparedSessionRuntime {
    storage_writer: Arc<StorageWriter>,
    app: PreparedApp,
    handles: PreparedAgentHandles,
    model_slot: Arc<ProviderSlot>,
    coordinator: PreparedSessionCoordinator,
    checkpoint: Arc<CoordinatorCheckpointWriter>,
    prepare_provider: PrepareProvider,
    provider: Option<PreparedProvider>,
    shell_tx: flume::Sender<ShellEvent>,
    shell_rx: flume::Receiver<ShellEvent>,
    resumed: bool,
    seed_snapshot: Option<(Arc<StorageWriter>, Arc<AppSession>)>,
}

impl PreparedSessionRuntime {
    fn seed_storage(&self) {
        if let Some((writer, session)) = &self.seed_snapshot {
            writer.seed(Arc::clone(session));
        }
    }

    #[cfg(test)]
    fn snapshot(
        &self,
    ) -> (
        MakiId,
        maki_commands::InvocationTargetId,
        maki_agent::AgentManagerHandle,
        maki_agent::AgentId,
    ) {
        let (manager, root_id) = self.handles.manager_and_root();
        (
            self.app.session_id(),
            self.app.command_target_id(),
            manager,
            root_id,
        )
    }

    fn activate(
        self,
        startup_model_slot: &ProviderSlot,
        mut session_lock: Option<SessionLockState>,
    ) -> Result<SessionRuntime, ConfigChangeError> {
        let Self {
            storage_writer,
            app,
            handles,
            model_slot,
            coordinator,
            provider,
            shell_tx,
            shell_rx,
            resumed,
            seed_snapshot: _,
            checkpoint,
            prepare_provider,
        } = self;
        checkpoint.bind(match &session_lock {
            Some(SessionLockState::Held(lock)) => Some(lock.publication_guard()),
            _ => None,
        });
        let coordinator = match coordinator.activate() {
            Ok(coordinator) => coordinator,
            Err(error) => {
                if let Err(release_error) = release_lock_state(session_lock.take()) {
                    warn!(%release_error, "session lock release failed after activation error");
                }
                return Err(error.into());
            }
        };
        storage_writer.bind_config_runtime(coordinator.session_id(), coordinator.runtime_epoch());
        if let Some(provider) = provider {
            startup_model_slot.install(provider.model, provider.provider);
        }
        let handles = handles.activate();
        let mut app = app.activate();
        handles.apply_to_app(&mut app);
        app.coordinator = Some(coordinator.clone());
        let config_commits = match observe_config_commits(
            &handles,
            Arc::clone(&model_slot),
            coordinator.clone(),
            prepare_provider,
        ) {
            Ok(commits) => commits,
            Err(error) => {
                coordinator.retire();
                handles.shutdown().detach();
                if let Err(release_error) = release_lock_state(session_lock.take()) {
                    warn!(%release_error, "session lock release failed after binding error");
                }
                return Err(error);
            }
        };
        Ok(SessionRuntime {
            generation: NEXT_RUNTIME_GENERATION.fetch_add(1, Ordering::Relaxed),
            config_commits,
            projected_config: None,
            app,
            handles,
            model_slot,
            _coordinator_retirement: CoordinatorRetirement(coordinator.clone()),
            coordinator,
            shell_tx,
            shell_rx,
            last_status: SessionStatus::Idle,
            notifications: RunNotificationState::default(),
            session_lock,
            lock_lost: false,
            restore_pending: resumed,
            pending_approval: None,
        })
    }

    fn activate_replacing(
        self,
        startup_model_slot: &ProviderSlot,
        session_lock: Option<SessionLockState>,
        current: &SessionCoordinatorHandle,
    ) -> std::result::Result<SessionRuntime, (ConfigChangeError, Option<SessionLockState>)> {
        let Self {
            storage_writer,
            app,
            handles,
            model_slot,
            coordinator,
            provider,
            shell_tx,
            shell_rx,
            resumed,
            seed_snapshot: _,
            checkpoint,
            prepare_provider,
        } = self;
        checkpoint.bind(match &session_lock {
            Some(SessionLockState::Held(lock)) => Some(lock.publication_guard()),
            _ => None,
        });
        let session_id = app.session_id();
        let activated = if session_id == current.read().session_id() {
            coordinator.activate_replacing(current)
        } else {
            coordinator.activate()
        };
        let coordinator = match activated {
            Ok(coordinator) => coordinator,
            Err(error) => return Err((error.into(), session_lock)),
        };
        storage_writer.bind_config_runtime(coordinator.session_id(), coordinator.runtime_epoch());
        if let Some(provider) = provider {
            startup_model_slot.install(provider.model, provider.provider);
        }
        let handles = handles.activate();
        let mut app = app.activate();
        handles.apply_to_app(&mut app);
        app.coordinator = Some(coordinator.clone());
        let config_commits = match observe_config_commits(
            &handles,
            Arc::clone(&model_slot),
            coordinator.clone(),
            prepare_provider,
        ) {
            Ok(commits) => commits,
            Err(error) => {
                coordinator.retire();
                handles.shutdown().detach();
                return Err((error, session_lock));
            }
        };
        Ok(SessionRuntime {
            generation: NEXT_RUNTIME_GENERATION.fetch_add(1, Ordering::Relaxed),
            config_commits,
            projected_config: None,
            app,
            handles,
            model_slot,
            _coordinator_retirement: CoordinatorRetirement(coordinator.clone()),
            coordinator,
            shell_tx,
            shell_rx,
            last_status: SessionStatus::Idle,
            notifications: RunNotificationState::default(),
            session_lock,
            lock_lost: false,
            restore_pending: resumed,
            pending_approval: None,
        })
    }
}

fn ensure_replacement_lock_available(
    current: &SessionRuntime,
    target_id: MakiId,
) -> Result<(), &'static str> {
    if current.id() == target_id
        && matches!(current.session_lock, Some(SessionLockState::InFlight(_)))
    {
        Err(LOCK_UNAVAILABLE_REPLACEMENT_ERR)
    } else {
        Ok(())
    }
}

/// Activates `prepared` in place of `current` and hands back the outgoing
/// runtime. A deferred runner stays parked until the caller releases it. On
/// failure `current` is untouched and the target lock is returned.
fn swap_session_runtime(
    current: &mut SessionRuntime,
    prepared: PreparedSessionRuntime,
    target_lock: Option<SessionLockState>,
    model_slot: &ProviderSlot,
) -> std::result::Result<SessionRuntime, (String, Option<SessionLockState>)> {
    let seed_snapshot = prepared.seed_snapshot.clone();
    let mut runtime = prepared
        .activate_replacing(model_slot, target_lock, &current.coordinator)
        .map_err(|(error, target_lock)| (error.to_string(), target_lock))?;
    if let Some((writer, session)) = seed_snapshot {
        writer.seed(session);
    }
    runtime.app.exit_on_done = current.app.exit_on_done;
    runtime
        .app
        .input_box
        .replace_history_from(&mut current.app.input_box);
    let old = std::mem::replace(current, runtime);
    old.app
        .command_runtime
        .finish_theme_preview(old.app.command_target.id(), false);
    current.activate_deferred();
    Ok(old)
}

fn replace_session_runtime(
    current: &mut SessionRuntime,
    prepared: PreparedSessionRuntime,
    sessions_dir: &Path,
    model_slot: &ProviderSlot,
) -> Result<SessionRuntime, String> {
    let target_id = prepared.app.session_id();
    let same_id = current.id() == target_id;
    if current.lock_lost {
        return Err(LOCK_LOST_REPLACEMENT_ERR.into());
    }
    let target_lock = if same_id {
        match current.session_lock.take() {
            Some(SessionLockState::Held(lease)) => Some(SessionLockState::Held(lease)),
            state => {
                current.session_lock = state;
                return Err(LOCK_UNAVAILABLE_REPLACEMENT_ERR.into());
            }
        }
    } else {
        Some(SessionLockState::Held(
            claim_lock(sessions_dir, &target_id).map_err(|error| error.to_string())?,
        ))
    };
    match swap_session_runtime(current, prepared, target_lock, model_slot) {
        Ok(old) => Ok(old),
        Err((error, target_lock)) => {
            if same_id {
                current.session_lock = target_lock;
            } else if let Err(release_error) = release_lock_state(target_lock) {
                warn!(%release_error, "replacement lock release failed after activation error");
            }
            Err(error)
        }
    }
}

impl SessionRuntime {
    fn project_config(&mut self, commit: &ConfigCommit) -> bool {
        let (manager, root) = self.handles.manager_and_root();
        let Ok(actor) = manager.actor(root) else {
            return false;
        };
        if !Arc::ptr_eq(&actor.identity(), &commit.identity)
            || self
                .projected_config
                .as_ref()
                .is_some_and(|projected| commit.generation <= projected.generation)
        {
            return false;
        }
        let previous = self.projected_config.replace(commit.clone()).map_or_else(
            || self.app.seed_mode(),
            |projected| projected.config.mode.clone(),
        );
        self.app.update_model(&commit.config.model);
        if self
            .app
            .state
            .resolve_pending_fast(commit.config.fast, &commit.config.model)
        {
            self.restore_pending_fast(commit.config.model.spec());
        }
        self.app.project_root_mode(&previous, &commit.config.mode);
        self.settle_plan_approval();
        true
    }

    /// Commits a saved `/fast` that was restored while discovery was pending,
    /// once discovery says `spec` supports it. The reservation takes over the
    /// wish, and the patch does nothing if an earlier queued change already
    /// moved the root off `spec` or turned fast on.
    fn restore_pending_fast(&mut self, spec: String) {
        let (manager, root) = self.handles.manager_and_root();
        let mut reservation = match manager
            .lookup(root)
            .map_err(ConfigChangeError::from)
            .and_then(|target| reserve_config_change(&target, &self.coordinator, None))
        {
            Ok(reservation) => reservation,
            Err(error) => {
                warn!(%error, "failed to reserve the restored fast mode");
                return;
            }
        };
        self.app.state.fast_pending = false;
        smol::spawn(async move {
            let result = match reservation.wait_predecessor().await {
                Ok(current) if current.model.spec() == spec && !current.fast => reservation
                    .commit_change(ConfigChange::Patch(ConfigPatch {
                        fast: Some(true),
                        ..Default::default()
                    }))
                    .await
                    .map(|_| ()),
                Ok(_) => Ok(()),
                Err(error) => Err(error),
            };
            if let Err(error) = result {
                warn!(%error, "failed to commit the restored fast mode");
            }
        })
        .detach();
    }

    /// An explicit choice of model or fast for the root replaces a restored
    /// `/fast` wish. Returns whether one was pending.
    fn drop_pending_fast(&mut self, target: &AgentRef) -> bool {
        let (_, root) = self.handles.manager_and_root();
        target.id() == root && std::mem::take(&mut self.app.state.fast_pending)
    }

    fn config(&self) -> Option<Arc<maki_agent::EffectiveAgentConfig>> {
        let (manager, root) = self.handles.manager_and_root();
        manager.actor(root).ok()?.effective_config()
    }

    fn id(&self) -> MakiId {
        self.app.state.session.id
    }

    fn update(&mut self, msg: Msg) -> Vec<Action> {
        self.app.update(msg)
    }

    fn submit_text(&mut self, text: String) -> Result<SubmitOutcome, String> {
        if text.trim().is_empty() {
            return Err(EMPTY_PROMPT_ERR.to_owned());
        }
        let text = self.app.lua_event_handle.expand_references(&text)?;
        self.config().ok_or(NOT_LIVE_ERR)?;
        let (manager, root) = self.handles.manager_and_root();
        let actor = manager.actor(root).map_err(|error| error.to_string())?;
        let snapshot = actor.snapshot();
        let starting = snapshot.active_turn.is_none() && snapshot.queued == 0;
        let mut preamble = self.app.shell.results_for(Some(root)).to_vec();
        preamble.extend_from_slice(self.app.shell.results_for(None));
        let input = maki_agent::AgentInput {
            message: text,
            images: Vec::new(),
            preamble,
            prompt: None,
        };
        if let Err(error) = self.handles.queue.try_push(QueueItem {
            input,
            displayed: None,
        }) {
            return Err(error.to_string());
        }
        self.app.shell.drain_results_for(Some(root));
        self.app.shell.drain_results();
        self.reset_run_notifications();
        Ok(if starting {
            SubmitOutcome::Started(Vec::new())
        } else {
            SubmitOutcome::Queued
        })
    }

    fn activate_deferred(&mut self) {
        if std::mem::take(&mut self.restore_pending) {
            self.app.restore_resumed_session();
        }
        self.handles.release_runner();
    }

    /// New work cancels an `exit_on_done` exit still waiting on its drain.
    fn reset_run_notifications(&mut self) {
        if self.notifications.waiting_for_drain() {
            self.app.clear_exit_request();
        }
        self.notifications.reset();
    }

    /// A wake may only start a background run when the session is fully
    /// quiescent. Idle status alone is not enough: restored queue items start
    /// runs without `start_run` (the app only learns of them via
    /// `QueueItemConsumed`), and `start_run` destroys text held for recovery
    /// after an agent error.
    fn quiescent(&self) -> bool {
        SessionStatus::of(&self.app) == SessionStatus::Idle
            && self.handles.queue.is_empty()
            && !self.app.holds_recovery_text()
    }
}

/// Everything needed to bring up a new session runtime after startup.
struct SpawnCtx {
    prepare_provider: PrepareProvider,
    storage: StateDir,
    sessions_dir: PathBuf,
    config: AgentConfig,
    ui_config: UiConfig,
    input_history_size: usize,
    /// Prototype only: every runtime forks its own manager so session
    /// rules stay per-session.
    permissions: Arc<PermissionManager>,
    timeouts: Timeouts,
    keymap_reader: KeymapReader,
    hint_reader: HintReader,
    status_content_reader: StatusContentReader,
    lua_event_handle: EventHandle,
    mcp_handle: Option<McpHandle>,
    mcp_config_errors: McpConfigErrors,
    model_slot: Arc<ProviderSlot>,
    catalog: ModelCatalog,
    storage_writer: Arc<StorageWriter>,
    model_policy: Arc<ModelPolicy>,
    session_defaults: SessionDefaults,
    system_prompt: SystemPromptOverride,
    command_runtime: Arc<CommandRuntime>,
    trust_question: Option<TrustQuestion>,
}

struct CoordinatorDeps {
    catalog: maki_agent::session_coordinator::SessionOptionCatalog,
    model_policy: Arc<ModelPolicy>,
    checkpoint: Arc<CoordinatorCheckpointWriter>,
}

#[allow(clippy::too_many_arguments)]
fn prepare_coordinator(
    deps: &CoordinatorDeps,
    session: &AppSession,
    available_models: Vec<Arc<str>>,
    handles: &PreparedAgentHandles,
    permissions: &Arc<PermissionManager>,
    config: &maki_agent::EffectiveAgentConfig,
) -> Result<PreparedSessionCoordinator> {
    let mailbox = handles
        .mailbox()
        .ok_or_else(|| eyre!("session mailbox unavailable"))?;
    let model_spec = session.model.clone();
    let definitions = builtin_option_definitions(
        Arc::from(model_spec.as_str()),
        available_models,
        permissions.is_yolo(),
        config.fast,
        config.workflow,
        config.thinking,
    );
    PreparedSessionCoordinator::new(SessionCoordinatorParams {
        session_id: session.id,
        catalog: deps.catalog.clone(),
        definitions,
        persisted_options: session.meta.session_options.clone(),
        model_policy: Arc::clone(&deps.model_policy),
        checkpoint: deps.checkpoint.clone(),
        mailbox,
    })
    .map_err(|error| eyre!(error))
}

impl SpawnCtx {
    fn coordinator_deps(&self) -> CoordinatorDeps {
        CoordinatorDeps {
            catalog: self.lua_event_handle.session_option_catalog(),
            model_policy: Arc::clone(&self.model_policy),
            checkpoint: self.storage_writer.coordinator_checkpoint(),
        }
    }

    #[cfg(test)]
    fn prepare_runtime(&self, session: AppSession) -> Result<PreparedSessionRuntime> {
        self.prepare_runtime_with_provider(session, None)
    }

    fn prepare_replacement_runtime(
        &self,
        mut session: AppSession,
        current_id: MakiId,
        permissions: &PermissionManager,
        current_offload: Option<&Arc<OffloadStore>>,
    ) -> Result<PreparedSessionRuntime, String> {
        session.meta.yolo = permissions.persisted_yolo();
        let provider = self.prepare_replacement_provider(&session)?;
        let seed_snapshot = session.id != current_id;
        let offload = if seed_snapshot {
            None
        } else {
            current_offload.cloned()
        };
        self.prepare_runtime_with_config(
            session,
            provider,
            permissions,
            seed_snapshot,
            None,
            offload,
        )
        .map_err(|error| error.to_string())
    }

    fn prepare_replacement_provider(
        &self,
        session: &AppSession,
    ) -> Result<Option<PreparedProvider>, String> {
        let model_spec = &session.model;
        if model_spec == &self.model_slot.load().model.spec()
            || !self.model_policy.allows(model_spec)
        {
            return Ok(None);
        }
        let model = Model::from_spec(model_spec).map_err(|error| error.to_string())?;
        let prepared =
            (self.prepare_provider)(model, self.timeouts).map_err(|error| error.to_string())?;
        Ok(Some(PreparedProvider {
            model: prepared.model,
            provider: prepared.provider,
        }))
    }

    fn prepare_runtime_with_provider(
        &self,
        mut session: AppSession,
        provider: Option<PreparedProvider>,
    ) -> Result<PreparedSessionRuntime> {
        if !session_has_content(&session) {
            session.meta.yolo = self.permissions.persisted_yolo();
        }
        self.prepare_runtime_with_provider_and_permissions(
            session,
            provider,
            &self.permissions,
            true,
        )
    }

    fn prepare_runtime_with_provider_and_permissions(
        &self,
        session: AppSession,
        provider: Option<PreparedProvider>,
        permissions: &PermissionManager,
        seed_snapshot: bool,
    ) -> Result<PreparedSessionRuntime> {
        self.prepare_runtime_with_config(session, provider, permissions, seed_snapshot, None, None)
    }

    fn prepare_runtime_with_config(
        &self,
        mut session: AppSession,
        provider: Option<PreparedProvider>,
        permissions: &PermissionManager,
        seed_snapshot: bool,
        effective_config: Option<maki_agent::EffectiveAgentConfig>,
        offload: Option<Arc<OffloadStore>>,
    ) -> Result<PreparedSessionRuntime> {
        let resumed = session_has_content(&session);
        let session_id = session.id;
        let offload = offload.or_else(|| {
            Some(Arc::new(OffloadStore::for_session(
                self.storage.path(),
                &SessionRef::from(session_id),
            )))
        });
        let cwd = PathBuf::from(&session.cwd);
        let (model, runtime_provider): (Model, Arc<dyn Provider>) = match provider.as_ref() {
            Some(provider) => (provider.model.clone(), Arc::clone(&provider.provider)),
            None => {
                let startup = self.model_slot.load();
                (
                    startup.model.clone(),
                    Arc::clone(&startup.provider) as Arc<dyn Provider>,
                )
            }
        };
        session.model = model.spec();
        let model_slot = ProviderSlot::with_change_tx(
            model.clone(),
            runtime_provider,
            self.model_slot.change_tx(),
        );
        let permissions = Arc::new(permissions.fork());
        permissions.set_session_yolo(session.meta.yolo);
        permissions.load_session_rules(crate::app::stored_to_rules(&session.meta.session_rules));
        // Resolve the agent's configuration before the app exists, so mode, fast,
        // workflow and thinking have one starting value: the one the agent is
        // seeded with. `effective_config` only carries a caller's explicit
        // override; everything else comes from the session being resumed.
        let config = effective_config.unwrap_or_else(|| {
            let state = SessionState::from_session(session.clone(), &model, &self.model_policy);
            let provider = Arc::clone(&model_slot.load().provider) as Arc<dyn Provider>;
            state.starting_config(
                provider,
                &model,
                cwd.clone(),
                &self.lua_event_handle.mode_registry(),
                &self.storage,
                self.session_defaults,
            )
        });
        let coordinator_config = config.clone();
        let mut app = App::prepare(
            config.clone(),
            session.clone(),
            self.storage.clone(),
            self.catalog.specs(),
            self.mcp_handle
                .as_ref()
                .map(McpHandle::reader)
                .unwrap_or_else(McpSnapshotReader::empty),
            self.mcp_config_errors.clone(),
            self.keymap_reader.clone(),
            self.hint_reader.clone(),
            self.status_content_reader.clone(),
            Arc::clone(&self.storage_writer),
            self.ui_config.clone(),
            self.input_history_size,
            Arc::clone(&permissions),
            self.lua_event_handle.clone(),
            Arc::clone(&self.model_policy),
            crate::theme::default_provider().clone(),
            Arc::clone(&self.command_runtime),
        );
        let handles = AgentHandles::prepare(
            &model_slot,
            config,
            session.history_snapshot(),
            self.config.clone(),
            self.ui_config.tool_output_lines,
            &permissions,
            cwd,
            Some(SessionRef::from(session_id)),
            offload,
            self.timeouts,
            self.lua_event_handle.clone(),
            self.mcp_handle.clone(),
            Arc::clone(&self.model_policy),
            self.system_prompt.clone(),
        );
        let deps = self.coordinator_deps();
        let coordinator = prepare_coordinator(
            &deps,
            &session,
            self.catalog.snapshot(),
            &handles,
            &permissions,
            &coordinator_config,
        )?;
        let seed_snapshot =
            seed_snapshot.then(|| (Arc::clone(&self.storage_writer), Arc::new(session.clone())));
        app.app.trust_question = self.trust_question.clone();
        let (shell_tx, shell_rx) = flume::unbounded::<ShellEvent>();
        Ok(PreparedSessionRuntime {
            storage_writer: Arc::clone(&self.storage_writer),
            app,
            handles,
            model_slot,
            coordinator,
            checkpoint: deps.checkpoint,
            prepare_provider: Arc::clone(&self.prepare_provider),
            provider,
            shell_tx,
            shell_rx,
            resumed,
            seed_snapshot,
        })
    }

    #[cfg(test)]
    fn spawn_runtime(&self, session: AppSession) -> Result<SessionRuntime> {
        self.spawn_runtime_with_provider(session, None)
    }

    fn spawn_runtime_with_provider(
        &self,
        session: AppSession,
        provider: Option<PreparedProvider>,
    ) -> Result<SessionRuntime> {
        let id = session.id;
        let prepared = self.prepare_runtime_with_provider(session, provider)?;
        let session_lock = claim_lock(&self.sessions_dir, &id)?;
        prepared.seed_storage();
        let mut runtime = prepared
            .activate(&self.model_slot, Some(SessionLockState::Held(session_lock)))
            .map_err(|error| eyre!(error))?;
        runtime.activate_deferred();
        Ok(runtime)
    }
}

enum InternalEvent {
    ModelsDiscovered,
    ProviderUsageFetched {
        fetch_id: ProviderUsageFetchId,
        provider: ProviderIdentity,
        result: ProviderUsageFetchResult,
    },
    /// A coordinator operation dispatched off the event-loop thread has
    /// finished. See [`SessionOpKind`] for why they cannot run inline.
    SessionOp {
        session: MakiId,
        runtime: Arc<()>,
        kind: SessionOpKind,
        result: Result<(), String>,
    },
    PlanApprovalReady {
        session: MakiId,
        runtime: Arc<()>,
        request: Arc<()>,
        result: Result<ApprovalStep, String>,
    },
    SessionHeartbeat(u64),
}

/// The event loop must never await a coordinator operation on its own thread:
/// the operation can wait on the actor's queue and on a durable save, and if
/// the actor is itself waiting on the UI (a permission prompt, a question) the
/// wait is circular. So the operation is dispatched, the loop keeps rendering,
/// and the follow-up work named here runs when the result comes back.
enum SessionOpKind {
    /// `/model` from a keybinding or command: apply the adopted model.
    ModelChanged { spec: String },
    /// OAuth refresh rebuilt the focused model's provider without changing its spec.
    ProviderRefreshed,
    HistoryRewound {
        change: Arc<Mutex<Option<HistoryChange>>>,
    },
    /// `/yolo`, `/fast`, `/workflow`: apply the toggle the coordinator took.
    OptionToggled {
        id: &'static str,
        committed: Arc<std::sync::Mutex<Option<bool>>>,
    },
    /// A committed mode change; the projection follows the new mode.
    ModeChanged,
    /// `/cd`: apply the canonical path the coordinator resolved, which is not
    /// necessarily the one that was typed.
    DirectoryChanged {
        adopted: Arc<std::sync::Mutex<Option<PathBuf>>>,
    },
}

struct OrchestrationRevision {
    runtime: u64,
    selected: Option<maki_agent::AgentId>,
    live: bool,
    root_config: Option<Arc<maki_agent::EffectiveAgentConfig>>,
    selected_config: Option<Arc<maki_agent::EffectiveAgentConfig>>,
    model_policy: Arc<ModelPolicy>,
    cwd: Arc<PathBuf>,
}

impl OrchestrationRevision {
    fn matches(&self, other: &Self) -> bool {
        let same_config =
            |left: &Option<Arc<maki_agent::EffectiveAgentConfig>>,
             right: &Option<Arc<maki_agent::EffectiveAgentConfig>>| {
                match (left, right) {
                    (Some(left), Some(right)) => Arc::ptr_eq(left, right),
                    (None, None) => true,
                    _ => false,
                }
            };
        self.runtime == other.runtime
            && self.selected == other.selected
            && self.live == other.live
            && self.cwd == other.cwd
            && same_config(&self.root_config, &other.root_config)
            && same_config(&self.selected_config, &other.selected_config)
            && Arc::ptr_eq(&self.model_policy, &other.model_policy)
    }
}

fn orchestration_revision(runtime: &SessionRuntime) -> OrchestrationRevision {
    let (manager, root) = runtime.handles.manager_and_root();
    let selected = runtime
        .app
        .selected_agent_id()
        .or_else(|| runtime.app.is_main_chat().then_some(root));
    let config = |id| manager.effective_config(id).ok().flatten();
    OrchestrationRevision {
        runtime: runtime.generation,
        selected,
        live: selected.is_some_and(|id| manager.lookup(id).is_ok()),
        root_config: config(root),
        selected_config: selected.and_then(config),
        model_policy: Arc::clone(&runtime.handles.orchestration_template.model_policy),
        cwd: config(root)
            .map(|config| Arc::new(config.context.cwd().to_path_buf()))
            .unwrap_or_else(|| runtime.handles.cwd_slot().load_full()),
    }
}

pub(crate) struct EventLoop<'t> {
    terminal: &'t mut ratatui::DefaultTerminal,
    sessions: Vec<SessionRuntime>,
    focused: usize,
    session_picker: bool,
    last_focused: Option<MakiId>,
    terminal_focused: bool,
    notifier: Option<terminal::TerminalNotifier>,
    ctx: SpawnCtx,
    orchestration: Arc<AgentOrchestration>,
    orchestration_revisions: Vec<OrchestrationRevision>,
    orchestration_focus: Option<usize>,
    sessions_dir: PathBuf,
    session_cwd: String,
    last_heartbeat: Instant,
    input: InputReader,
    warn_rx: flume::Receiver<String>,
    warn_tx: flume::Sender<String>,
    ui_action_rx: flume::Receiver<UiAction>,
    command_rx: flume::Receiver<CommandEvent>,
    provider_change_rx: flume::Receiver<ProviderChange>,
    provider_usage: ProviderUsageCoordinator<flume::Sender<ProviderUsageReply>>,
    next_status_invalidation: u64,
    pending_status_invalidation: Option<ProviderUsageInvalidation>,
    internal_tx: flume::Sender<InternalEvent>,
    internal_rx: flume::Receiver<InternalEvent>,
    model_discovery: Discovery,
}

/// One item from any of the event loop's sources; `None` from `next_wake`
/// means the wait timed out (animation/idle tick).
enum Wake {
    Input(Event),
    InputGone,
    Ui(UiAction),
    Agent(usize, Box<maki_agent::Envelope>),
    Actor(usize, ActorEvent),
    Shell(usize, ShellEvent),
    Warn(String),
    Command(CommandEvent),
    ProviderChanged(ProviderChange),
    Internal(InternalEvent),
}

fn new_session_from_slot(slot: &ProviderSlot, cwd: &str) -> (AppSession, PreparedProvider) {
    let current = slot.load();
    (
        AppSession::new(&current.model.spec(), cwd),
        PreparedProvider {
            model: current.model.clone(),
            provider: Arc::clone(&current.provider) as Arc<dyn Provider>,
        },
    )
}

/// Runs {catalog}'s discovery; each applied batch's warnings flash and the
/// loop re-projects the catalog.
fn discover_models(
    catalog: &ModelCatalog,
    policy: Arc<ModelPolicy>,
    active: String,
    warn_tx: flume::Sender<String>,
    internal_tx: flume::Sender<InternalEvent>,
) -> Discovery {
    catalog.discover(policy, active, move |warnings| {
        for warning in warnings {
            let _ = warn_tx.try_send(warning);
        }
        let _ = internal_tx.send(InternalEvent::ModelsDiscovered);
    })
}

impl<'t> EventLoop<'t> {
    pub(crate) fn new(
        terminal: &'t mut ratatui::DefaultTerminal,
        params: EventLoopParams,
    ) -> Result<Self> {
        let EventLoopParams {
            cwd,
            mut model,
            needs_login,
            explicit_model,
            commands,
            sessions,
            focused,
            mut startup_warnings,
            storage,
            config,
            ui_config,
            input_history_size,
            permissions,
            timeouts,
            exit_on_done,
            command_registry,
            session_picker,
            keymap_reader,
            hint_reader,
            status_content_reader,
            ui_action_rx,
            lua_event_handle,
            model_policy,
            session_defaults,
            project_config,
            trust_question,
            startup_notice,
            system_prompt_override,
            append_system_prompt,
        } = params;

        lua_event_handle.set_clock_format(crate::clock::resolved(ui_config.clock_format));

        // Apply the config theme before the warmup thread spawns, or warmup
        // could bake the syntax palette from the old theme. Only the
        // in-memory name is set, so the user's saved pick survives.
        if let Some(ref name) = ui_config.theme {
            let provider = crate::theme::default_provider();
            match provider.install(name) {
                Ok(()) => provider.select(name),
                Err(e) => startup_warnings.push(format!("config ui.theme: {e}")),
            }
        }

        // Seed the highlight palette for the active theme synchronously. For a
        // saved (non-config) theme `install` never runs, so without this the
        // palette only lands on the warmup thread and plugins that cache
        // `theme_color` at load (e.g. the splash) bake the fallback instead of
        // the real theme. Cheap: it only swaps the theme + a few UI colors; the
        // expensive syntax-set build stays on the warmup thread.
        crate::highlight::refresh_syntax_theme();

        static PROCESS_WARMUP: std::sync::Once = std::sync::Once::new();
        PROCESS_WARMUP.call_once(|| {
            maki_highlight::pool::spawn(crate::highlight::warmup);
            crate::update::spawn_check();
        });

        let provider: Arc<dyn Provider> = if needs_login {
            Arc::from(maki_providers::provider::from_model_fallback(
                &mut model, timeouts,
            ))
        } else {
            Arc::from(from_model(&mut model, timeouts).context("create provider")?)
        };
        let (model_slot, provider_change_rx) = ProviderSlot::new(model.clone(), provider);
        let initial_provider = model_slot.load().provider.identity();
        let (internal_tx, internal_rx) = flume::unbounded();
        let (warn_tx, warn_rx) = flume::unbounded::<String>();
        let catalog = ModelCatalog::offline(&model_policy);
        let storage_writer = Arc::new(StorageWriter::new(storage.clone(), warn_tx.clone()));
        let sessions_dir = storage.ensure_subdir(SESSIONS_DIR)?;

        let notifier = terminal::TerminalNotifier::new(ui_config.notifications);
        let model_completion = Arc::new(ModelArgSource::new(catalog.specs()));
        let theme_completion = Arc::new(ThemeArgSource::new(
            crate::theme::default_provider().clone(),
        ));
        let (command_runtime, command_rx) = CommandRuntime::new(
            &commands,
            command_registry,
            model_completion,
            theme_completion,
        );
        let command_runtime = Arc::new(command_runtime);
        let (mcp_handle, mcp_config_errors) =
            smol::block_on(mcp::start(&cwd, project_config.clone()));
        let ctx = SpawnCtx {
            prepare_provider: Arc::new(prepare_provider),
            storage,
            sessions_dir: sessions_dir.clone(),
            config,
            ui_config,
            input_history_size,
            permissions,
            timeouts,
            keymap_reader,
            hint_reader,
            status_content_reader,
            lua_event_handle,
            mcp_handle,
            mcp_config_errors,
            model_slot,
            catalog,
            storage_writer,
            model_policy,
            session_defaults,
            system_prompt: SystemPromptOverride {
                override_text: system_prompt_override,
                append_text: append_system_prompt,
            },
            command_runtime,
            trust_question,
        };

        let mut runtimes = Vec::with_capacity(sessions.len());
        for session in sessions {
            let provider = if explicit_model {
                None
            } else {
                match ctx.prepare_replacement_provider(&session) {
                    Ok(provider) => provider,
                    Err(error) => {
                        startup_warnings.push(format!(
                            "failed to restore model {}: {error}; using {}",
                            session.model,
                            ctx.model_slot.load().model.spec()
                        ));
                        None
                    }
                }
            };
            match ctx.spawn_runtime_with_provider(session, provider) {
                Ok(runtime) => runtimes.push(runtime),
                Err(error) => {
                    rollback_startup_runtimes(runtimes);
                    return Err(error);
                }
            }
        }
        for runtime in &mut runtimes {
            runtime.activate_deferred();
        }
        if runtimes.is_empty() {
            return Err(eyre!("event loop needs at least one session"));
        }
        let focused = focused.min(runtimes.len() - 1);
        let app = &mut runtimes[focused].app;
        app.exit_on_done = exit_on_done;
        if needs_login {
            app.login_picker.open(app.storage.clone());
        }
        if !ctx.mcp_config_errors.is_empty() {
            let msg = format!("MCP config error: {}", ctx.mcp_config_errors);
            app.flash(msg);
        }
        if let Some(notice) = startup_notice {
            app.flash(notice);
        }
        for w in startup_warnings {
            app.flash(w);
        }

        let session_cwd = runtimes[focused].app.state.session.cwd.clone();
        let orchestration = Arc::new(AgentOrchestration::new(ctx.catalog.specs()));
        ctx.catalog.attach(&orchestration);
        ctx.lua_event_handle
            .install_orchestration_services(orchestration.clone());
        let model_discovery = discover_models(
            &ctx.catalog,
            Arc::clone(&ctx.model_policy),
            model.provider.to_string(),
            warn_tx.clone(),
            internal_tx.clone(),
        );
        Ok(Self {
            terminal,
            sessions: runtimes,
            focused,
            session_picker,
            last_focused: None,
            terminal_focused: false,
            notifier,
            ctx,
            orchestration,
            orchestration_revisions: Vec::new(),
            orchestration_focus: None,
            sessions_dir,
            session_cwd,
            last_heartbeat: Instant::now(),
            input: InputReader::spawn(),
            warn_rx,
            warn_tx,
            ui_action_rx,
            command_rx,
            provider_change_rx,
            provider_usage: ProviderUsageCoordinator::new(initial_provider),
            next_status_invalidation: 0,
            pending_status_invalidation: None,
            internal_tx,
            internal_rx,
            model_discovery,
        })
    }

    fn sync_orchestration(&mut self) {
        if self.orchestration_focus == Some(self.focused)
            && self.sessions.len() == self.orchestration_revisions.len()
            && self
                .sessions
                .iter()
                .zip(&self.orchestration_revisions)
                .all(|(runtime, previous)| orchestration_revision(runtime).matches(previous))
        {
            return;
        }
        self.orchestration_focus = Some(self.focused);
        self.orchestration_revisions = self.sessions.iter().map(orchestration_revision).collect();
        let mut admissions = HashMap::new();
        let mut focused = None;
        for (index, runtime) in self.sessions.iter().enumerate() {
            let (manager, root_id) = runtime.handles.manager_and_root();
            let Ok(root) = manager.lookup(root_id) else {
                continue;
            };
            let mut template = runtime.handles.orchestration_template.clone();
            if let Some(config) = runtime.config() {
                template.cwd = config.context.cwd().to_path_buf();
            }
            self.orchestration.register_root(
                TrustedTarget {
                    target: root.clone(),
                    template,
                },
                Some(runtime_config_mutations(
                    runtime,
                    Arc::clone(&self.ctx.storage_writer),
                    Arc::clone(&self.orchestration),
                )),
            );
            self.orchestration.bind_commands(
                &root,
                self.ctx
                    .command_runtime
                    .command_runner(&runtime.app.command_target),
            );
            if let Some(target) = selected_agent(runtime) {
                if let Ok(template) = OrchestrationServices::template(&*self.orchestration, &target)
                {
                    admissions.insert(
                        runtime.app.command_target.id(),
                        TrustedTarget {
                            target: target.clone(),
                            template,
                        },
                    );
                }
                if index == self.focused {
                    focused = Some(target);
                }
            }
        }
        self.ctx.command_runtime.sync_admissions(admissions);
        self.orchestration.select(focused);
        self.sync_model_discovery();
    }

    fn sync_model_discovery(&mut self) {
        for runtime in &mut self.sessions {
            let target = if runtime.app.model_picker.is_open() {
                runtime
                    .app
                    .model_picker_target
                    .clone()
                    .or_else(|| selected_agent(runtime))
            } else {
                selected_agent(runtime)
            };
            let specs = target
                .and_then(|target| {
                    OrchestrationServices::available_models(&*self.orchestration, &target).ok()
                })
                .unwrap_or_default();
            runtime.app.project_model_discovery(specs);
        }
    }

    fn focused_app(&mut self) -> &mut App {
        &mut self.sessions[self.focused].app
    }

    fn start(&mut self, initial_prompt: Option<String>) {
        self.sync_orchestration();
        if let Some(prompt) = initial_prompt {
            let sub = Submission {
                text: prompt,
                images: Vec::new(),
            };
            let actions = self.focused_app().handle_submit(sub);
            self.dispatch(self.focused, actions);
        } else if self.session_picker {
            self.focused_app().open_startup_session_picker();
        }
    }

    pub(crate) fn run(mut self, initial_prompt: Option<String>) -> Result<ShutdownReport> {
        self.start(initial_prompt);
        // The first frame always paints. After that only a poller, an event or
        // an animation tick owes another.
        let mut dirty = Dirty::YES;
        let result = loop {
            self.sync_orchestration();
            dirty |= self.tick();
            if self.sessions[self.focused].app.take_pending_bell() {
                ring_bell();
            }
            match self.drain_channels() {
                Ok(d) => dirty |= d,
                Err(e) => break Err(e),
            }
            self.checkpoint_all();
            if dirty.take() {
                let app = &mut self.sessions[self.focused].app;
                if let Err(e) = self.terminal.draw(|f| {
                    app.view(f);
                    color_compat::downgrade_if_needed(f.buffer_mut());
                }) {
                    break Err(e.into());
                }
            }

            if let Some(i) = self.sessions.iter().position(|rt| {
                rt.app.exit_request != ExitRequest::None && !rt.notifications.waiting_for_drain()
            }) {
                // A backgrounded session can finish an `exit_on_done` turn;
                // focus it so shutdown reports its exit code and id.
                self.set_focused(i);
                self.emit_notifications();
                break Ok(());
            }

            // Sleeping a whole frame instead of a fraction of one is what
            // makes a spinner cost 12 paints a second instead of 62.
            let cadence = self.sessions[self.focused].app.cadence();
            match self.next_wake(cadence.frame().unwrap_or(IDLE_POLL)) {
                // Any event can change the screen, so paint after handling it
                // rather than asking every handler to prove it did.
                Some(wake) => {
                    dirty = Dirty::YES;
                    if let Err(e) = self.handle_wake(wake) {
                        break Err(e);
                    }
                }
                // Only the clock moved, so motion alone owes the frame. The
                // cadence is the one from before the sleep, so motion that
                // just stopped still gets a last paint to clear itself off
                // the screen.
                None => dirty |= Dirty::from(cadence.moves()),
            }
        };
        // Fatal errors still save every session, kill MCP process groups,
        // and drain the storage writer before the process exits.
        let report = self.shutdown();
        result.map(|()| report)
    }

    /// Wait for the next event from any source, or time out so animations
    /// and periodic polls keep running. `Duration::ZERO` drains whatever is
    /// already pending.
    fn next_wake(&self, timeout: Duration) -> Option<Wake> {
        let mut sel = flume::Selector::new().recv(self.input.receiver(), |res| match res {
            Ok(ev) => Some(Wake::Input(ev)),
            Err(_) => Some(Wake::InputGone),
        });
        if !self.ui_action_rx.is_disconnected() {
            sel = sel.recv(&self.ui_action_rx, |res| res.ok().map(Wake::Ui));
        }
        sel = sel.recv(&self.warn_rx, |res| res.ok().map(Wake::Warn));
        sel = sel.recv(&self.command_rx, |res| res.ok().map(Wake::Command));
        sel = sel.recv(&self.provider_change_rx, |res| {
            res.ok().map(Wake::ProviderChanged)
        });
        sel = sel.recv(&self.internal_rx, |res| res.ok().map(Wake::Internal));
        for (i, rt) in self.sessions.iter().enumerate() {
            if !rt.handles.agent_rx.is_disconnected() {
                sel = sel.recv(&rt.handles.agent_rx, move |res| {
                    res.ok().map(|env| Wake::Agent(i, Box::new(env)))
                });
            }
            if !rt.handles.actor_rx.is_disconnected() {
                sel = sel.recv(&rt.handles.actor_rx, move |res| {
                    res.ok().map(|event| Wake::Actor(i, event))
                });
            }
            sel = sel.recv(&rt.shell_rx, move |res| {
                res.ok().map(|ev| Wake::Shell(i, ev))
            });
        }
        sel.wait_timeout(timeout).ok().flatten()
    }

    fn handle_wake(&mut self, wake: Wake) -> Result<()> {
        match wake {
            Wake::Input(ev) => self.handle_input(ev),
            Wake::InputGone => return Err(eyre!("terminal input reader stopped")),
            Wake::Ui(action) => self.render(action),
            Wake::Agent(i, envelope) => self.handle_agent(i, envelope),
            Wake::Actor(i, event) => {
                self.handle_actor_settlement(i, &event);
                self.sessions[i].app.handle_actor_event(event)
            }
            Wake::Shell(i, event) => self.sessions[i].app.handle_shell_event(event),
            Wake::Warn(warning) => self.focused_app().flash(warning),
            Wake::Command(command) => self.handle_command(command),
            Wake::ProviderChanged(change) => self.handle_provider_change(change),
            Wake::Internal(event) => self.handle_internal(event),
        }
        Ok(())
    }

    fn handle_internal(&mut self, event: InternalEvent) {
        match event {
            InternalEvent::PlanApprovalReady {
                session,
                runtime,
                request,
                result,
            } => {
                if let Some(idx) = self.position(session).filter(|idx| {
                    Arc::ptr_eq(&runtime, &self.sessions[*idx].handles.identity())
                        && self.sessions[*idx]
                            .pending_approval
                            .as_ref()
                            .is_some_and(|pending| Arc::ptr_eq(&pending.identity, &request))
                }) {
                    self.complete_plan_approval(idx, result);
                }
            }
            InternalEvent::ModelsDiscovered => self.refresh_discovered_models(),
            InternalEvent::SessionOp {
                session,
                runtime,
                kind,
                result,
            } => {
                let current = self.position(session).is_some_and(|idx| {
                    Arc::ptr_eq(&runtime, &self.sessions[idx].handles.identity())
                });
                if current {
                    self.handle_session_op(session, kind, result);
                }
            }
            InternalEvent::ProviderUsageFetched {
                fetch_id,
                provider,
                result,
            } => {
                let current = self.focused_model_slot().load().provider.identity();
                if provider != current {
                    let outputs = self
                        .provider_usage
                        .handle(ProviderUsageInput::Transition { provider: current });
                    self.handle_provider_usage_outputs(outputs);
                }
                let outputs = self.provider_usage.handle(ProviderUsageInput::Completed {
                    fetch_id,
                    provider,
                    result,
                });
                self.handle_provider_usage_outputs(outputs);
            }
            InternalEvent::SessionHeartbeat(runtime_generation) => {
                let Some(idx) = self
                    .sessions
                    .iter()
                    .position(|runtime| runtime.generation == runtime_generation)
                else {
                    return;
                };
                complete_runtime_heartbeat(&mut self.sessions[idx]);
            }
        }
    }

    fn handle_provider_change(&mut self, _change: ProviderChange) {
        let provider = self.focused_model_slot().load().provider.identity();
        for runtime in &self.sessions {
            runtime
                .app
                .suppress_status_content
                .store(true, Ordering::Release);
        }
        let outputs = self
            .provider_usage
            .handle(ProviderUsageInput::Transition { provider });
        self.handle_provider_usage_outputs(outputs);
        self.next_status_invalidation = self.next_status_invalidation.wrapping_add(1);
        let invalidation = ProviderUsageInvalidation(self.next_status_invalidation);
        self.pending_status_invalidation = Some(invalidation);
        if !self
            .ctx
            .lua_event_handle
            .provider_usage_changed(self.provider_usage_loading_snapshot(), Some(invalidation))
        {
            self.pending_status_invalidation = None;
            for runtime in &self.sessions {
                runtime
                    .app
                    .suppress_status_content
                    .store(false, Ordering::Release);
            }
        }
        let current = self.focused_model_slot().load();
        self.ctx.lua_event_handle.fire_autocmd(
            "ProviderChanged",
            serde_json::json!({
                "provider_id": format!("{}:{}:{}", current.model.provider, provider.instance.0, provider.auth.0),
                "provider": current.model.provider_display_name(),
                "model": current.model.id,
            }),
        );
    }

    /// The one save trigger. A checkpoint writes only on a real change, so
    /// every tool result reaches disk within a frame while an idle session
    /// writes nothing.
    fn handle_command(&mut self, event: CommandEvent) {
        let target = match &event {
            CommandEvent::Host { target, .. } | CommandEvent::Outcome { target, .. } => *target,
        };
        let Some(index) = command_target_index(
            self.sessions
                .iter()
                .map(|runtime| runtime.app.command_target.id()),
            target,
        ) else {
            if let CommandEvent::Host { reply, .. } = event {
                let _ = reply.send(Err(CommandError::StaleTarget));
            }
            return;
        };
        match event {
            CommandEvent::Host {
                request,
                admission,
                reply,
                ..
            } => {
                if let maki_commands::HostRequest::Builtin(operation) = &request {
                    if matches!(
                        operation,
                        BuiltinOperation::ToggleFast | BuiltinOperation::ToggleWorkflow
                    ) {
                        let id = if matches!(operation, BuiltinOperation::ToggleFast) {
                            FAST_OPTION_ID
                        } else {
                            WORKFLOW_OPTION_ID
                        };
                        let result = admission
                            .as_ref()
                            .ok_or(CommandError::StaleTarget)
                            .and_then(|pinned| {
                                self.dispatch_target_toggle(
                                    index,
                                    &pinned.target,
                                    id,
                                    Some(reply.clone()),
                                )
                                .map_err(|error| CommandError::Producer(Arc::from(error)))
                            });
                        if let Err(error) = result {
                            let _ = reply.send(Err(error));
                        }
                        return;
                    }
                    if let BuiltinOperation::Compact(instructions) = operation {
                        let result = admission
                            .as_ref()
                            .ok_or(CommandError::StaleTarget)
                            .and_then(|pinned| {
                                self.target_actor(index, &pinned.target)
                                    .map_err(|error| CommandError::Producer(Arc::from(error)))
                            })
                            .and_then(|actor| {
                                let run_id = actor.reserve_presentation(actor.presentation_id());
                                actor
                                    .push_compact(run_id, instructions.clone())
                                    .map_err(|error| {
                                        CommandError::Producer(Arc::from(error.to_string()))
                                    })
                            });
                        let _ = reply.send(result.map(|()| HostResponse::Completed));
                        return;
                    }
                    let selection = match operation {
                        BuiltinOperation::SelectModel(selection) => Some(selection.clone()),
                        BuiltinOperation::SetModel { spec } => {
                            Some(maki_commands::ModelSelection {
                                spec: Some(Arc::clone(spec)),
                                ..Default::default()
                            })
                        }
                        _ => None,
                    };
                    if let Some(selection) = selection {
                        let Some(pinned) = admission else {
                            let _ = reply.send(Err(CommandError::StaleTarget));
                            return;
                        };
                        self.dispatch_model_selection(
                            index,
                            Some(pinned.target),
                            selection,
                            Some(reply),
                        );
                        return;
                    }
                }
                if let maki_commands::HostRequest::Builtin(BuiltinOperation::ChangeDirectory {
                    path,
                }) = &request
                {
                    let result = admission
                        .as_ref()
                        .ok_or(CommandError::StaleTarget)
                        .and_then(|pinned| {
                            self.dispatch_target_directory(index, &pinned.target, path.clone())
                                .map_err(|error| CommandError::Producer(Arc::from(error)))
                        });
                    let _ = reply.send(result.map(|()| HostResponse::Completed));
                    return;
                }
                if matches!(
                    request,
                    maki_commands::HostRequest::Context(
                        maki_commands::HostContextRequest::WorkingDirectory
                    )
                ) {
                    let result = admission
                        .as_ref()
                        .ok_or(CommandError::StaleTarget)
                        .and_then(|pinned| {
                            self.target_actor(index, &pinned.target)
                                .map_err(|_| CommandError::StaleTarget)
                        })
                        .and_then(|actor| actor.effective_config().ok_or(CommandError::StaleTarget))
                        .map(|config| {
                            HostResponse::Context(
                                maki_commands::HostContextResponse::WorkingDirectory(
                                    config.context.cwd().to_path_buf(),
                                ),
                            )
                        });
                    let _ = reply.send(result);
                    return;
                }
                if matches!(
                    request,
                    maki_commands::HostRequest::Context(
                        maki_commands::HostContextRequest::FastModeSupported
                    )
                ) {
                    let result = admission
                        .as_ref()
                        .ok_or(CommandError::StaleTarget)
                        .and_then(|pinned| {
                            self.target_actor(index, &pinned.target)
                                .map_err(|_| CommandError::StaleTarget)
                        })
                        .and_then(|actor| actor.effective_config().ok_or(CommandError::StaleTarget))
                        .map(|config| {
                            HostResponse::Context(
                                maki_commands::HostContextResponse::FastModeSupported(
                                    config.model.supports_fast() || config.model.fast_pending(),
                                ),
                            )
                        });
                    let _ = reply.send(result);
                    return;
                }
                if matches!(
                    request,
                    maki_commands::HostRequest::Context(
                        maki_commands::HostContextRequest::ModelSpecs
                    )
                ) {
                    let result = admission
                        .as_ref()
                        .ok_or(CommandError::StaleTarget)
                        .and_then(|pinned| self.target_model_specs(&pinned.target));
                    let _ = reply.send(result);
                    return;
                }
                if matches!(
                    request,
                    maki_commands::HostRequest::Builtin(BuiltinOperation::OpenModelPicker)
                ) {
                    let result = admission
                        .ok_or(CommandError::StaleTarget)
                        .and_then(|pinned| {
                            self.target_actor(index, &pinned.target)
                                .map_err(|_| CommandError::StaleTarget)?;
                            self.sessions[index]
                                .app
                                .open_pinned_model_picker(pinned.target)
                        });
                    if result.is_ok() {
                        self.handle_action(index, Action::RefreshModels);
                        self.sync_model_discovery();
                    }
                    let _ = reply.send(result.map(|()| HostResponse::Completed));
                    return;
                }
                let result = self.sessions[index].app.execute_host_request(request);
                match result {
                    Ok((response, actions)) => {
                        self.dispatch(index, actions);
                        let _ = reply.send(Ok(response));
                    }
                    Err(error) => {
                        let _ = reply.send(Err(error));
                    }
                }
            }
            CommandEvent::Outcome {
                outcome, admission, ..
            } => match outcome {
                maki_commands::CommandOutcome::AgentTurn(turn) => {
                    let actions = self.sessions[index]
                        .app
                        .submit_command_turn(turn, admission.as_deref());
                    self.dispatch(index, actions);
                }
                maki_commands::CommandOutcome::IsolatedTurn(turn) => {
                    let actions = self.sessions[index]
                        .app
                        .submit_isolated_turn(turn, admission.as_deref());
                    self.dispatch(index, actions);
                }
                maki_commands::CommandOutcome::FrontendFeedback(feedback) => {
                    self.sessions[index].app.present_frontend_feedback(feedback);
                }
                maki_commands::CommandOutcome::Failed(error) => {
                    self.sessions[index].app.flash(error.to_string());
                }
                maki_commands::CommandOutcome::ManualCompaction(_)
                | maki_commands::CommandOutcome::Completed => {}
            },
        }
    }

    fn checkpoint_all(&mut self) {
        for runtime in &mut self.sessions {
            checkpoint_runtime(runtime);
        }
    }

    /// Only the focused session is drawn, so only it can owe a frame; focusing
    /// another is an event, and events always repaint. Background sessions
    /// still drain their floats, or a plugin writing to a window nobody is
    /// looking at would lose the output.
    fn tick(&mut self) -> Dirty {
        let mut dirty = Dirty::NO;
        let now = Instant::now();
        if now.duration_since(self.last_heartbeat) >= session_lock::HEARTBEAT_INTERVAL {
            self.last_heartbeat = now;
            for runtime in &mut self.sessions {
                start_runtime_heartbeat(runtime, &self.internal_tx);
            }
        }
        let mut login_actions: Vec<(usize, Vec<Action>)> = Vec::new();
        let mut approvals = Vec::new();
        for (i, rt) in self.sessions.iter_mut().enumerate() {
            if matches!(rt.session_lock, Some(SessionLockState::InFlight(_))) {
                complete_runtime_heartbeat(rt);
            }
            if !matches!(rt.session_lock, Some(SessionLockState::InFlight(_)))
                && let Some(step) = rt
                    .pending_approval
                    .as_mut()
                    .and_then(|pending| pending.waiting_for_lock.take())
            {
                approvals.push((i, step));
            }
            while let Ok(commit) = rt.config_commits.try_recv() {
                if rt.project_config(&commit) {
                    dirty = Dirty::YES;
                }
            }
            let (session_dirty, actions) = tick_session(&mut rt.app, i == self.focused, now);
            dirty |= session_dirty;
            if !actions.is_empty() {
                login_actions.push((i, actions));
            }
        }
        for (i, actions) in login_actions {
            self.dispatch(i, actions);
        }
        for (i, step) in approvals {
            self.complete_plan_approval(i, Ok(step));
            dirty = Dirty::YES;
        }
        dirty
    }

    /// The actor settled: publish the one identity-bearing queue-settled
    /// notification on the same guaranteed agent channel every other frontend
    /// event uses. The actor already checked emptiness and ticket drain under
    /// its own lock, so this never races a concurrent push.
    fn handle_actor_settlement(&mut self, idx: usize, event: &ActorEvent) {
        let ActorEvent::Idle {
            settled: Some(identity),
            ..
        } = event
        else {
            return;
        };
        let rt = &mut self.sessions[idx];
        let _ =
            maki_agent::EventSender::with_identity(rt.handles.agent_tx.clone(), Some(*identity))
                .send(AgentEvent::QueueSettled);
    }

    fn handle_agent(&mut self, idx: usize, envelope: Box<maki_agent::Envelope>) {
        let rt = &mut self.sessions[idx];
        rt.app.adopt_actor_presentation(&envelope);
        if let AgentEvent::Post(post) = envelope.event {
            let after_message = envelope.subagent.as_ref().and_then(|info| {
                let (manager, _) = rt.handles.manager_and_root();
                manager
                    .actor(info.agent_id)
                    .ok()
                    .map(|actor| actor.committed_message_count())
            });
            rt.app
                .push_post(envelope.subagent.as_ref(), post, after_message);
            return;
        }
        let current = is_current_top_level(rt.app.run_id, &envelope);
        match &envelope.event {
            AgentEvent::QueueSettled => {
                if current {
                    rt.notifications.on_drain();
                }
                return;
            }
            AgentEvent::QueueItemConsumed { .. } if current => {
                rt.notifications.on_queue_item_consumed();
                if rt.app.exit_on_done {
                    rt.app.clear_exit_request();
                }
            }
            AgentEvent::TurnComplete(turn) if current => {
                rt.notifications.on_turn_complete(&turn.message);
            }
            event if current => rt.notifications.on_done(event),
            _ => {}
        }
        let actions = self.sessions[idx].app.update(Msg::Agent(envelope));
        self.dispatch(idx, actions);
        if self.sessions[idx].app.take_pending_bell() {
            ring_bell();
        }
    }

    fn drain_channels(&mut self) -> Result<Dirty> {
        let mut dirty = Dirty::NO;
        // Leftovers beyond the budget are picked up right after the next draw.
        for _ in 0..DRAIN_BUDGET {
            match self.next_wake(Duration::ZERO) {
                Some(wake) => {
                    self.handle_wake(wake)?;
                    dirty = Dirty::YES;
                }
                None => break,
            }
        }

        // These two only fire Lua autocmds. Anything a handler does comes back
        // as a `UiAction` on the next wake, which repaints then.
        self.emit_focus_change();
        dirty |= self.start_mailbox_runs();
        self.emit_status_changes();
        self.emit_notifications();
        // An `exit_on_done` exit waits on the settled notification; a dead agent
        // loop can never send it, so fail instead of hanging forever.
        if let Some(runtime) = self.sessions.iter().find(|rt| {
            rt.app.exit_request != ExitRequest::None
                && rt.notifications.waiting_for_drain()
                && rt.handles.is_finished()
                && rt.handles.agent_rx.is_empty()
        }) {
            return Err(eyre!(
                "agent for session {} stopped before queue drain",
                runtime.id()
            ));
        }
        Ok(dirty)
    }

    fn handle_provider_usage_ack(&mut self, ack: ProviderUsageAck) {
        if self.pending_status_invalidation != Some(ack.invalidation) {
            return;
        }
        let generations = self
            .sessions
            .iter_mut()
            .map(|runtime| {
                let (generation, d) = runtime.app.reconcile_status_content();
                runtime.app.pending_dirty |= d;
                generation
            })
            .collect::<Vec<_>>();
        if generations
            .iter()
            .any(|generation| *generation < ack.status_generation)
        {
            return;
        }
        self.pending_status_invalidation = None;
        for runtime in &self.sessions {
            runtime
                .app
                .suppress_status_content
                .store(false, Ordering::Release);
        }
    }

    fn handle_provider_usage_outputs(
        &mut self,
        outputs: Vec<ProviderUsageOutput<flume::Sender<ProviderUsageReply>>>,
    ) {
        for output in outputs {
            match output {
                ProviderUsageOutput::StartFetch(fetch) => self.start_provider_usage_fetch(fetch),
                ProviderUsageOutput::Resolve {
                    waiters,
                    resolution,
                } => {
                    let reply = match resolution {
                        ProviderUsageResolution::CompletedWithIdentity { provider, result } => {
                            let Some(snapshot) = self.provider_usage_snapshot(provider, result)
                            else {
                                for waiter in waiters {
                                    let _ = waiter.send(Err(PROVIDER_USAGE_CHANGED_ERR.to_owned()));
                                }
                                continue;
                            };
                            if self
                                .ctx
                                .lua_event_handle
                                .provider_usage_changed(snapshot.clone(), None)
                            {
                                Ok(snapshot)
                            } else {
                                Err(PROVIDER_USAGE_SHUTDOWN_ERR.to_owned())
                            }
                        }
                        ProviderUsageResolution::Completed(result) => {
                            let provider = *self.provider_usage.provider();
                            let Some(snapshot) = self.provider_usage_snapshot(provider, result)
                            else {
                                for waiter in waiters {
                                    let _ = waiter.send(Err(PROVIDER_USAGE_CHANGED_ERR.to_owned()));
                                }
                                continue;
                            };
                            if self
                                .ctx
                                .lua_event_handle
                                .provider_usage_changed(snapshot.clone(), None)
                            {
                                Ok(snapshot)
                            } else {
                                Err(PROVIDER_USAGE_SHUTDOWN_ERR.to_owned())
                            }
                        }
                        ProviderUsageResolution::ProviderChanged { .. } => {
                            Err(PROVIDER_USAGE_CHANGED_ERR.to_owned())
                        }
                        ProviderUsageResolution::Shutdown => {
                            Err(PROVIDER_USAGE_SHUTDOWN_ERR.to_owned())
                        }
                    };
                    for waiter in waiters {
                        let _ = waiter.send(reply.clone());
                    }
                }
            }
        }
    }

    fn start_provider_usage_fetch(&self, fetch: ProviderUsageFetch) {
        let current = self.focused_model_slot().load();
        if current.provider.identity() != fetch.provider {
            let _ = self.internal_tx.send(InternalEvent::ProviderUsageFetched {
                fetch_id: fetch.id,
                provider: fetch.provider,
                result: ProviderUsageFetchResult::Error(PROVIDER_USAGE_CHANGED_ERR.to_owned()),
            });
            return;
        }
        let provider = Arc::clone(&current.provider);
        drop(current);
        self.ctx
            .lua_event_handle
            .provider_usage_changed(self.provider_usage_loading_snapshot(), None);
        let internal_tx = self.internal_tx.clone();
        smol::spawn(async move {
            let result = match provider.fetch_usage().await {
                Ok(Some(usage)) => ProviderUsageFetchResult::Ready(usage),
                Ok(None) => ProviderUsageFetchResult::Unsupported,
                Err(error) => ProviderUsageFetchResult::Error(error.user_message()),
            };
            let _ = internal_tx.send(InternalEvent::ProviderUsageFetched {
                fetch_id: fetch.id,
                provider: fetch.provider,
                result,
            });
        })
        .detach();
    }

    fn provider_usage_loading_snapshot(&self) -> ProviderUsageSnapshot {
        let current = self.focused_model_slot().load();
        ProviderUsageSnapshot {
            provider_id: format!(
                "{}:{}:{}",
                current.model.provider,
                current.provider.identity().instance.0,
                current.provider.identity().auth.0
            ),
            provider: current.model.provider_display_name().to_owned(),
            model: current.model.id.clone(),
            status: "loading".into(),
            limits: Vec::new(),
            plan: None,
            error: None,
        }
    }

    fn provider_usage_snapshot(
        &self,
        provider: ProviderIdentity,
        result: ProviderUsageFetchResult,
    ) -> Option<ProviderUsageSnapshot> {
        let current = self.focused_model_slot().load();
        if current.provider.identity() != provider {
            return None;
        }
        let (status, limits, plan, error) = match result {
            ProviderUsageFetchResult::Ready(usage) => (
                "ready".into(),
                usage.limits.into_iter().map(provider_usage_limit).collect(),
                usage.plan,
                None,
            ),
            ProviderUsageFetchResult::Unsupported => ("unsupported".into(), Vec::new(), None, None),
            ProviderUsageFetchResult::Error(message) => {
                ("error".into(), Vec::new(), None, Some(message))
            }
        };
        Some(ProviderUsageSnapshot {
            provider_id: format!(
                "{}:{}:{}",
                current.model.provider,
                current.provider.identity().instance.0,
                current.provider.identity().auth.0
            ),
            provider: current.model.provider_display_name().to_owned(),
            model: current.model.id.clone(),
            status,
            limits,
            plan,
            error,
        })
    }

    fn prepare_terminal_handoff(&mut self) {
        prepare_terminal_handoff(
            self.sessions.iter_mut().map(|rt| &mut rt.app),
            &mut self.terminal_focused,
        );
    }

    fn target_runtime_index(&self, target: &AgentRef) -> Result<usize, String> {
        self.sessions
            .iter()
            .position(|runtime| {
                let (manager, root) = runtime.handles.manager_and_root();
                manager
                    .lookup(root)
                    .is_ok_and(|root| root.same_manager(target))
            })
            .ok_or_else(|| NOT_LIVE_ERR.to_owned())
    }

    fn pinned_editor_path(
        &self,
        target: &AgentRef,
        path: &Path,
    ) -> Result<(usize, PathBuf), String> {
        let index = self.target_runtime_index(target)?;
        let actor = self.target_actor(index, target)?;
        let config = actor.effective_config().ok_or(NOT_LIVE_ERR)?;
        Ok((index, config.context.cwd().join(path)))
    }

    /// Exits with the editor's status code; `-1` (flashed on the session's
    /// app) when the editor could not be launched.
    fn open_editor(&mut self, idx: usize, path: &Path) -> i32 {
        self.prepare_terminal_handoff();
        let result = {
            let _pause = self.input.pause();
            terminal::open_in_editor(path, self.terminal)
        };
        self.terminal_focused = false;
        match result {
            Ok(code) => code,
            Err(e) => {
                self.sessions[idx].app.flash(e);
                -1
            }
        }
    }

    fn emit_status_changes(&mut self) {
        let handle = &self.ctx.lua_event_handle;
        for (i, rt) in self.sessions.iter_mut().enumerate() {
            let status = SessionStatus::of(&rt.app);
            if status == rt.last_status {
                continue;
            }
            rt.last_status = status;
            handle.fire_autocmd(
                "SessionStatusChanged",
                json!({
                    "session_id": rt.id(),
                    "title": rt.app.state.session.title,
                    "status": status.as_str(),
                    "focused": i == self.focused,
                }),
            );
        }
    }

    fn emit_notifications(&mut self) {
        let Some(notifier) = &self.notifier else {
            return;
        };
        let mut selected = None;
        for rt in &mut self.sessions {
            let candidate = rt.notifications.reconcile(
                rt.app.attention(),
                rt.last_status,
                rt.handles.queue.is_empty(),
                self.terminal_focused,
            );
            selected = select_notification(selected, candidate);
        }
        if let Some(notification) = selected
            && let Err(error) = notifier.notify(&notification.message())
        {
            warn!(notifier = ?notifier.notifier(), %error, "terminal notifications disabled after write failure");
            self.notifier = None;
        }
    }

    fn emit_focus_change(&mut self) {
        let id = self.sessions[self.focused].id();
        if self.last_focused == Some(id) {
            return;
        }
        let mut data = json!({ "session_id": id });
        if let Some(previous) = self.last_focused {
            data["previous_session_id"] = json!(previous.to_string());
        }
        self.last_focused = Some(id);
        self.ctx
            .lua_event_handle
            .fire_autocmd("SessionFocusChanged", data);
    }

    fn start_mailbox_runs(&mut self) -> Dirty {
        let ready: Vec<_> = self
            .sessions
            .iter()
            .enumerate()
            .filter_map(|(index, runtime)| {
                if !runtime.quiescent() {
                    return None;
                }
                let preamble = runtime.handles.claim_mailbox_wake();
                (!preamble.is_empty()).then_some((index, preamble))
            })
            .collect();

        let dirty = Dirty::from(!ready.is_empty());
        for (index, preamble) in ready {
            let actions = self.sessions[index].app.start_mailbox_run(preamble);
            self.dispatch(index, actions);
        }
        dirty
    }

    /// `List` and `ListAll` reply from a background task (the scan can be
    /// slow); every other request is answered synchronously by the event
    /// loop, which owns the live runtimes.
    fn handle_session_request(&mut self, req: SessionRequest, reply_tx: flume::Sender<UiReply>) {
        match req {
            SessionRequest::List => {
                let storage = self.ctx.storage.clone();
                let cwd = self.session_cwd.clone();
                smol::unblock(move || {
                    let reply = AppSession::list(&cwd, &storage)
                        .map_err(|e| e.to_string())
                        .and_then(|list| serde_json::to_value(list).map_err(|e| e.to_string()));
                    let _ = reply_tx.send(reply);
                })
                .detach();
            }
            SessionRequest::ListAll => {
                let storage = self.ctx.storage.clone();
                smol::unblock(move || {
                    let reply = AppSession::list_all(&storage)
                        .map_err(|e| e.to_string())
                        .and_then(|list| serde_json::to_value(list).map_err(|e| e.to_string()));
                    let _ = reply_tx.send(reply);
                })
                .detach();
            }
            SessionRequest::Delete { id } => {
                let id = match parse_session_id(&id) {
                    Ok(id) => id,
                    Err(e) => {
                        let _ = reply_tx.send(Err(e));
                        return;
                    }
                };
                let mut runtime = if let Some(i) = self.position(id) {
                    if i == self.focused {
                        let _ = reply_tx.send(Err(DELETE_FOCUSED_ERR.into()));
                        return;
                    }
                    let session_lock = self.sessions[i].session_lock.take();
                    let mut runtime = self.remove_runtime(i);
                    runtime.session_lock = session_lock;
                    Some(runtime)
                } else {
                    None
                };
                let store = runtime
                    .as_ref()
                    .and_then(|rt| rt.handles.offload.as_ref())
                    .cloned();
                if let Some(store) = &store {
                    store.request_close();
                }
                let storage_writer = Arc::clone(&self.ctx.storage_writer);
                let sessions_dir = self.ctx.sessions_dir.clone();
                smol::spawn(async move {
                    let session_lock = runtime.as_mut().and_then(|rt| rt.session_lock.take());
                    let lease = smol::unblock(move || {
                        match settle_lock_state(session_lock, AGENT_SHUTDOWN_TIMEOUT) {
                            LockSettlement::Held(lease) => Ok(lease),
                            LockSettlement::None => claim_lock(&sessions_dir, &id),
                            LockSettlement::Lost | LockSettlement::TimedOut => {
                                Err(eyre!(LOCK_LOST_MSG))
                            }
                        }
                    })
                    .await;
                    let lease = match lease {
                        Ok(lease) => RenewingDeletionLease::new(lease),
                        Err(error) => {
                            if let Some(rt) = runtime {
                                let shutdown = rt.handles.shutdown();
                                let _ = rt.coordinator.close().await;
                                shutdown.await;
                            }
                            let _ = reply_tx.send(Err(error.to_string()));
                            return;
                        }
                    };
                    if let Some(rt) = runtime {
                        let shutdown = rt.handles.shutdown();
                        let _ = rt.coordinator.close().await;
                        shutdown.await;
                    }
                    let (lease, guard) = smol::unblock(move || {
                        let guard = lease.publication_guard();
                        (lease, guard)
                    })
                    .await;
                    let store = smol::unblock(move || {
                        if let Some(store) = &store {
                            store.close_and_drain();
                        }
                        store
                    })
                    .await;
                    let (done_tx, done_rx) = flume::bounded(1);
                    storage_writer.delete_guarded(id, guard, store, move |res| {
                        let _ = done_tx.send(res);
                    });
                    let reply = match done_rx.recv_async().await {
                        Ok(Ok(())) | Ok(Err(SessionError::Storage(StorageError::NotFound(_)))) => {
                            Ok(json!(true))
                        }
                        Ok(Err(error)) => Err(error.to_string()),
                        Err(error) => Err(error.to_string()),
                    };
                    smol::unblock(move || drop(lease)).await;
                    let _ = reply_tx.send(reply);
                })
                .detach();
            }
            SessionRequest::Live => {
                let list: Vec<_> = self
                    .sessions
                    .iter()
                    .enumerate()
                    .map(|(i, rt)| live_session_row(rt.id(), &rt.app, i == self.focused))
                    .collect();
                let _ = reply_tx.send(Ok(json!(list)));
            }
            SessionRequest::Read { id } => {
                let reply = match self.resolve_session_index(id) {
                    Ok(idx) => Ok(self.session_snapshot_json(idx)),
                    Err(e) => Err(e),
                };
                let _ = reply_tx.send(reply);
            }
            SessionRequest::New { prompt, focus } => {
                let (session, provider) =
                    new_session_from_slot(self.focused_model_slot(), &self.session_cwd);
                let mut runtime = match self
                    .ctx
                    .spawn_runtime_with_provider(session, Some(provider))
                {
                    Ok(runtime) => runtime,
                    Err(error) => {
                        let _ = reply_tx.send(Err(error.to_string()));
                        return;
                    }
                };
                if let Some(prompt) = prompt
                    && let Err(error) = runtime.submit_text(prompt)
                {
                    self.discard_rejected_runtime(runtime, error, reply_tx);
                    return;
                }
                let idx = self.push_runtime(runtime);
                let id = self.sessions[idx].id();
                if focus {
                    self.set_focused(idx);
                }
                let _ = reply_tx.send(Ok(json!(id)));
            }
            SessionRequest::Focus { id } => {
                let reply = parse_session_id(&id)
                    .and_then(|id| self.focus_session(id))
                    .map(|()| json!(true));
                let _ = reply_tx.send(reply);
            }
            SessionRequest::SetTitle { id, title } => {
                let title = normalize_title(&title);
                let reply = (|| {
                    let id = parse_session_id(&id)?;
                    if let Some(i) = self.position(id) {
                        self.sessions[i].app.state.session_mut().set_title(title);
                    } else {
                        let mut session =
                            AppSession::load(id, &self.ctx.storage).map_err(|e| e.to_string())?;
                        session.set_title(title);
                        self.ctx.storage_writer.send(Arc::new(session));
                    }
                    Ok(json!(true))
                })();
                let _ = reply_tx.send(reply);
            }
        }
    }

    #[cfg(test)]
    fn submit_text(&mut self, idx: usize, text: String) -> UiReply {
        self.sync_orchestration();
        match self.sessions[idx].submit_text(text)? {
            SubmitOutcome::Started(actions) => {
                self.dispatch(idx, actions);
                Ok(json!("started"))
            }
            SubmitOutcome::Queued => Ok(json!("queued")),
            SubmitOutcome::Rejected(error) => Err(error),
        }
    }

    fn position(&self, id: MakiId) -> Option<usize> {
        self.sessions.iter().position(|rt| rt.id() == id)
    }

    /// A plugin holding the id of a tab that has since closed gets `session
    /// not live` back, so it knows to stop.
    fn resolve_session_index(&self, id: MakiId) -> Result<usize, String> {
        self.position(id).ok_or_else(|| NOT_LIVE_ERR.into())
    }

    /// The totals live on the session, so a plugin that reloads mid run keeps
    /// the accounting it would lose by summing `TurnEnd` payloads itself.
    fn session_snapshot_json(&self, idx: usize) -> serde_json::Value {
        let app = &self.sessions[idx].app;
        let snapshot = SessionSnapshot {
            id: self.sessions[idx].id().to_string(),
            title: Some(app.state.session.title.clone()),
            status: SessionStatus::of(app).as_str(),
            updated_at: app.state.session.updated_at,
            usage: session_usage(
                &app.state.token_usage,
                app.state.cost,
                app.state.session.usage_by_model(),
            ),
        };
        serde_json::to_value(snapshot).unwrap_or_default()
    }

    /// The single place that removes a runtime: keeps `focused` pointing at
    /// the same session afterwards. The focused runtime itself is never
    /// removable, so `sessions` stays non-empty.
    fn remove_runtime(&mut self, idx: usize) -> SessionRuntime {
        debug_assert_ne!(idx, self.focused);
        let mut rt = self.sessions.remove(idx);
        if let Err(error) = release_lock_state(rt.session_lock.take()) {
            warn!(id = %rt.id(), %error, "session lock release failed");
        }
        if idx < self.focused {
            self.focused -= 1;
        }
        rt
    }

    fn discard_rejected_runtime(
        &self,
        runtime: SessionRuntime,
        error: String,
        reply: flume::Sender<UiReply>,
    ) {
        let writer = Arc::clone(&self.ctx.storage_writer);
        smol::spawn(async move {
            let id = runtime.id();
            let SessionRuntime {
                app,
                handles,
                coordinator,
                session_lock,
                ..
            } = runtime;
            coordinator.retire();
            drop(app);
            handles.shutdown().await;
            let _ = coordinator.close().await;
            writer.forget(id);
            let cleanup = match settle_lock_state(session_lock, AGENT_SHUTDOWN_TIMEOUT) {
                LockSettlement::Held(lease) => {
                    let lease = RenewingDeletionLease::new(lease);
                    let guard = lease.publication_guard();
                    let (done, deleted) = flume::bounded(1);
                    writer.delete_guarded(id, guard, None, move |result| {
                        let _ = done.send(result);
                    });
                    let result = deleted.recv_async().await;
                    drop(lease);
                    result
                        .map_err(|error| error.to_string())
                        .and_then(|result| result.map_err(|error| error.to_string()))
                }
                _ => Err(LOCK_UNAVAILABLE_REPLACEMENT_ERR.into()),
            };
            let error = match cleanup {
                Ok(()) => error,
                Err(cleanup) => format!("{error}; new-session cleanup failed: {cleanup}"),
            };
            let _ = reply.send(Err(error));
        })
        .detach();
    }

    fn push_runtime(&mut self, rt: SessionRuntime) -> usize {
        if self.pending_status_invalidation.is_some() {
            rt.app
                .suppress_status_content
                .store(true, Ordering::Release);
        }
        self.sessions.push(rt);
        let idx = self.sessions.len() - 1;
        self.sessions[idx].activate_deferred();
        idx
    }

    fn replace_runtime(&mut self, idx: usize, session: AppSession) -> Result<(), String> {
        if self.sessions[idx].lock_lost {
            return Err(LOCK_LOST_REPLACEMENT_ERR.into());
        }
        self.sessions[idx].app.checkpoint_now();
        let prepared = self.ctx.prepare_replacement_runtime(
            session,
            self.sessions[idx].id(),
            self.sessions[idx].app.permissions.as_ref(),
            self.sessions[idx].handles.offload.as_ref(),
        )?;
        self.replace_prepared_runtime(idx, prepared)
    }

    fn replace_prepared_runtime(
        &mut self,
        idx: usize,
        prepared: PreparedSessionRuntime,
    ) -> Result<(), String> {
        let target_id = prepared.app.session_id();
        let current_id = self.sessions[idx].id();
        let old = match replace_session_runtime(
            &mut self.sessions[idx],
            prepared,
            &self.sessions_dir,
            &self.ctx.model_slot,
        ) {
            Ok(old) => old,
            Err(error) => {
                if target_id != current_id {
                    self.ctx.storage_writer.forget(target_id);
                }
                return Err(error);
            }
        };
        smol::spawn(self.retire_runtime(idx, old, ())).detach();
        Ok(())
    }

    /// Releases the outgoing session's lock now. The returned future shuts
    /// its agents down, then drops `held`, then closes a replaced session's
    /// coordinator and forgets its cached snapshot.
    fn retire_runtime(
        &self,
        idx: usize,
        old: SessionRuntime,
        held: impl Send + 'static,
    ) -> impl Future<Output = ()> + Send + 'static {
        let SessionRuntime {
            app,
            handles,
            coordinator,
            session_lock,
            ..
        } = old;
        let retired_id = app.state.session.id;
        let replaced_session = retired_id != self.sessions[idx].id();
        if let Err(error) = release_lock_state(session_lock) {
            warn!(%error, "old session lock release failed");
        }
        drop(app);
        let shutdown = handles.shutdown();
        let storage_writer = Arc::clone(&self.ctx.storage_writer);
        async move {
            shutdown.await;
            drop(held);
            if replaced_session {
                let _ = coordinator.close().await;
                storage_writer.forget(retired_id);
            }
        }
    }

    fn fire_session_reset(&self, idx: usize, ended_id: MakiId) {
        self.sessions[idx]
            .app
            .lua_event_handle
            .fire_autocmd("SessionReset", json!({ "session_id": ended_id }));
    }

    fn prepare_replacement(
        &self,
        idx: usize,
        request: SessionReplacementRequest,
    ) -> Result<PendingReplacement, String> {
        let SessionReplacementRequest { mut session, kind } = request;
        if session.id == self.sessions[idx].id() {
            apply_options_to_session(
                &mut session,
                &self.sessions[idx].coordinator.read().options(),
            );
        }
        let prepared = self.ctx.prepare_replacement_runtime(
            session,
            self.sessions[idx].id(),
            self.sessions[idx].app.permissions.as_ref(),
            self.sessions[idx].handles.offload.as_ref(),
        )?;
        Ok(PendingReplacement { prepared, kind })
    }

    fn commit_replacement(&mut self, idx: usize, pending: PendingReplacement) {
        let PendingReplacement { prepared, kind } = pending;
        match self.replace_prepared_runtime(idx, prepared) {
            Ok(()) => match kind {
                SessionReplacementKind::Reset { ended_id } => {
                    self.fire_session_reset(idx, ended_id)
                }
                #[cfg(test)]
                SessionReplacementKind::Load => {}
            },
            Err(error) => self.sessions[idx].app.flash(error),
        }
    }

    fn request_replacement(&mut self, idx: usize, request: SessionReplacementRequest) {
        if let Err(error) =
            ensure_replacement_lock_available(&self.sessions[idx], request.session.id)
        {
            self.sessions[idx].app.flash(error.into());
            return;
        }
        let pending = match self.prepare_replacement(idx, request) {
            Ok(pending) => pending,
            Err(error) => {
                self.sessions[idx].app.flash(error);
                return;
            }
        };
        self.commit_replacement(idx, pending);
    }

    fn set_focused(&mut self, next: usize) {
        assign_session_focus(
            &mut self.sessions[self.focused].app,
            &mut self.focused,
            next,
        );
    }

    /// Focus a live session, or bring a stored one up: in place when the
    /// focused session is a blank idle one (nothing worth keeping), otherwise
    /// as a new runtime so the session you came from stays live. A stored
    /// session from another directory, or one held open by a live process
    /// elsewhere, is rejected.
    fn focus_session(&mut self, id: MakiId) -> Result<(), String> {
        if let Some(i) = self.position(id) {
            self.set_focused(i);
            return Ok(());
        }
        let session = AppSession::load(id, &self.ctx.storage)
            .map_err(|e| format!("Failed to load session: {e}"))?;
        let cwd = self.session_cwd.clone();
        let open_elsewhere = session_lock::open_elsewhere(&self.sessions_dir, &id);
        if let Some(block) = session_lock::resume_block(&session.cwd, &cwd, open_elsewhere) {
            return Err(block.to_string());
        }
        let current = &self.sessions[self.focused];
        let (manager, root) = current.handles.manager_and_root();
        let actor_idle = manager.actor(root).is_ok_and(|actor| {
            let snapshot = actor.snapshot();
            snapshot.status == maki_agent::actor::ActorStatus::Idle && snapshot.queued == 0
        });
        if current.quiescent() && actor_idle && !current.app.has_content() {
            return self.replace_runtime(self.focused, session);
        }
        let provider = self.ctx.prepare_replacement_provider(&session)?;
        let runtime = self
            .ctx
            .spawn_runtime_with_provider(session, provider)
            .map_err(|error| error.to_string())?;
        let idx = self.push_runtime(runtime);
        self.set_focused(idx);
        Ok(())
    }

    /// Handles one input event plus any leftover produced while coalescing
    /// bursts of scroll/drag events.
    fn handle_input(&mut self, raw: Event) {
        let mut pending = Some(raw);
        while let Some(ev) = pending.take() {
            self.sync_orchestration();
            let (msg, leftover) = self.translate(ev);
            if let Some(msg) = msg {
                let actions = self.sessions[self.focused].update(msg);
                self.dispatch(self.focused, actions);
                if self.sessions[self.focused].app.take_pending_bell() {
                    ring_bell();
                }
            }
            pending = leftover;
        }
    }

    fn translate(&mut self, raw: Event) -> (Option<Msg>, Option<Event>) {
        route_terminal_lifecycle(self.focused_app(), &raw);
        let supports_focus_reporting = self
            .notifier
            .as_ref()
            .is_some_and(terminal::TerminalNotifier::supports_focus_reporting);
        if let Some(focused) = terminal_focus_event(&raw) {
            if supports_focus_reporting {
                self.terminal_focused = focused;
            }
            return (None, None);
        }
        if supports_focus_reporting && terminal_input_proves_focus(&raw) {
            self.terminal_focused = true;
        }
        match raw {
            Event::Key(key) if key.kind == KeyEventKind::Press => (Some(Msg::Key(key)), None),
            Event::Key(_) => (None, None),
            Event::Paste(text) => (Some(Msg::Paste(text)), None),
            Event::Mouse(mouse) => self.translate_mouse(mouse),
            _ => (None, None),
        }
    }

    fn translate_mouse(&mut self, mouse: CtMouseEvent) -> (Option<Msg>, Option<Event>) {
        match mouse.kind {
            MouseEventKind::ScrollUp | MouseEventKind::ScrollDown => {
                let scroll_lines = self.focused_app().ui_config.mouse_scroll_lines;
                let (msg, leftover) = self.aggregate_scroll(mouse, scroll_lines);
                (Some(msg), leftover)
            }
            MouseEventKind::Drag(MouseButton::Left) => {
                let (drag, leftover) = self.coalesce_drag(mouse);
                (Some(Msg::Mouse(drag)), leftover)
            }
            _ => (Some(Msg::Mouse(mouse)), None),
        }
    }

    /// Sums queued scroll events into one delta; the first non-scroll event
    /// drained along the way is returned so it isn't lost.
    fn aggregate_scroll(&self, first: CtMouseEvent, scroll_lines: u32) -> (Msg, Option<Event>) {
        let mut delta = scroll_delta(first.kind, scroll_lines);
        let mut leftover = None;
        while let Ok(next) = self.input.receiver().try_recv() {
            match next {
                Event::Mouse(m)
                    if matches!(
                        m.kind,
                        MouseEventKind::ScrollUp | MouseEventKind::ScrollDown
                    ) =>
                {
                    delta += scroll_delta(m.kind, scroll_lines);
                }
                other => {
                    leftover = Some(other);
                    break;
                }
            }
        }
        (
            Msg::Scroll {
                column: first.column,
                row: first.row,
                delta,
            },
            leftover,
        )
    }

    /// Keeps only the newest queued drag position; the first non-drag event
    /// drained along the way is returned so it isn't lost.
    fn coalesce_drag(&self, mut latest: CtMouseEvent) -> (CtMouseEvent, Option<Event>) {
        let mut leftover = None;
        while let Ok(next) = self.input.receiver().try_recv() {
            match next {
                Event::Mouse(m) if matches!(m.kind, MouseEventKind::Drag(MouseButton::Left)) => {
                    latest = m;
                }
                other => {
                    leftover = Some(other);
                    break;
                }
            }
        }
        (latest, leftover)
    }

    fn dispatch(&mut self, idx: usize, actions: Vec<Action>) {
        for action in actions {
            self.handle_action(idx, action);
        }
    }

    fn handle_action(&mut self, idx: usize, action: Action) {
        self.sync_orchestration();
        match action {
            Action::SendMessage(input) => {
                let rt = &mut self.sessions[idx];
                rt.reset_run_notifications();
                let mut input = *input;
                prepend_preamble(&mut input.preamble, rt.app.shell.drain_results());
                let run_id = rt.app.run_id;
                rt.handles.queue.push(QueueItem {
                    input,
                    displayed: Some(run_id),
                });
            }
            Action::CancelAgent { .. } => {
                self.sessions[idx].notifications.reset();
            }
            Action::CancelSubagent { tool_use_id } => {
                let _ = self.sessions[idx]
                    .handles
                    .cmd_tx
                    .try_send(AgentCommand::CancelSubagent { tool_use_id });
            }
            Action::ReplaceSession(request) => self.request_replacement(idx, *request),
            Action::RewindAgent { target, boundary } => {
                if let Err(error) = self.dispatch_rewind(idx, target, boundary) {
                    self.sessions[idx].app.flash(error);
                }
            }
            Action::ToggleSessionOption { id } => {
                dispatch_option_toggle(&self.sessions[idx], id, &self.internal_tx);
            }
            Action::ChangeDirectory(path) => {
                let result = self
                    .selected_target(idx)
                    .ok_or_else(|| CLOSED_TARGET_ERR.to_owned())
                    .and_then(|target| self.dispatch_target_directory(idx, &target, path));
                if let Err(error) = result {
                    self.sessions[idx].app.flash(format!("cd: {error}"));
                }
            }
            Action::ChangeModel(spec) => self.dispatch_model_selection(
                idx,
                self.selected_target(idx),
                maki_commands::ModelSelection {
                    spec: Some(Arc::from(spec)),
                    ..Default::default()
                },
                None,
            ),
            Action::ChangePinnedModel { target, spec } => self.dispatch_model_selection(
                idx,
                Some(target),
                maki_commands::ModelSelection {
                    spec: Some(Arc::from(spec)),
                    ..Default::default()
                },
                None,
            ),
            Action::RunKeybind(id) => self.run_keybind(idx, id),
            Action::SelectModel(selection) => {
                self.note_if_deferred(idx, "model");
                let target = self.selected_target(idx);
                self.dispatch_model_selection(idx, target, selection, None);
            }
            Action::ChangeMode(mode) => dispatch_mode_commit(
                &self.sessions[idx],
                selected_agent(&self.sessions[idx]).ok_or_else(|| CLOSED_TARGET_ERR.to_owned()),
                &self.ctx.storage,
                self.ctx.lua_event_handle.mode_registry(),
                Some(Arc::clone(&self.orchestration)),
                self.internal_tx.clone(),
                mode,
            ),
            Action::ApprovePlan {
                clear_context,
                model,
                parallel,
                path,
            } => {
                self.prepare_plan_approval(idx, clear_context, model, parallel, path);
            }
            Action::CancelPlanApproval => self.sessions[idx].end_plan_approval(),
            Action::RefreshProvider { slug } => self.refresh_provider(slug),
            Action::AssignTier(spec, tier) => {
                maki_providers::model_registry::set_and_persist(spec, tier, &self.ctx.storage);
            }
            Action::UnassignTier(spec, tier) => {
                maki_providers::model_registry::unset_and_persist(&spec, tier, &self.ctx.storage);
            }
            Action::Compact(instructions) => {
                let result = self
                    .selected_target(idx)
                    .ok_or_else(|| CLOSED_TARGET_ERR.to_owned())
                    .and_then(|target| self.target_actor(idx, &target))
                    .and_then(|actor| {
                        let run_id = actor.reserve_presentation(actor.presentation_id());
                        actor
                            .push_compact(run_id, instructions)
                            .map_err(|error| error.to_string())
                    });
                if let Err(error) = result {
                    self.sessions[idx].app.flash(error);
                }
            }
            Action::ToggleMcp(server_name, enabled) => {
                self.sessions[idx].handles.send_mcp(McpCommand::Toggle {
                    server: server_name,
                    enabled,
                });
            }
            Action::ShellCommand {
                target,
                cwd,
                id,
                command,
                visible,
            } => {
                let (manager, _) = self.sessions[idx].handles.manager_and_root();
                let validation = target
                    .map(|id| manager.lookup(id).map_err(|error| error.to_string()))
                    .unwrap_or_else(|| {
                        self.selected_target(idx)
                            .ok_or_else(|| CLOSED_TARGET_ERR.to_owned())
                    })
                    .and_then(|target| self.target_actor(idx, &target).map(|_| ()));
                if let Err(error) = validation {
                    self.sessions[idx].app.flash(error);
                    return;
                }
                let rt = &mut self.sessions[idx];
                let (trigger, cancel) = CancelToken::new();
                rt.app.shell.add_trigger_for(id.clone(), trigger);
                spawn_shell(
                    command,
                    id,
                    visible,
                    rt.shell_tx.clone(),
                    cancel,
                    self.ctx.config.clone(),
                    cwd,
                );
            }
            Action::OpenEditor(path) => {
                self.open_editor(idx, &path);
            }
            Action::EditInputInEditor => {
                self.prepare_terminal_handoff();
                let current_text = self.sessions[idx].app.input_box.buffer.value();
                let result = {
                    let _pause = self.input.pause();
                    terminal::edit_temp_content(&current_text, self.terminal)
                };
                self.terminal_focused = false;
                match result {
                    Ok(edited) => {
                        self.sessions[idx].app.refresh_at_ref_labels(&edited);
                        self.sessions[idx].app.input_box.set_input(edited);
                    }
                    Err(e) => self.sessions[idx].app.flash(e),
                }
            }
            Action::Btw(question, images) => {
                let slot = self.sessions[idx].model_slot.load();
                self.sessions[idx].app.start_btw(
                    question,
                    images,
                    Arc::clone(&slot.provider) as Arc<dyn Provider>,
                    slot.model.clone(),
                );
            }
            Action::Suspend => {
                self.prepare_terminal_handoff();
                let _pause = self.input.pause();
                terminal::suspend(self.terminal);
                self.terminal_focused = false;
            }
            Action::Bell => ring_bell(),
            Action::RefreshModels => self.refresh_models(),
            Action::ManualExit => self.sessions[idx].notifications.on_manual_exit(),
        }
    }

    /// A change accepted while a turn runs applies to later turns. Say so: a
    /// command that is accepted and deferred should not look ignored.
    fn note_if_deferred(&mut self, idx: usize, what: &str) {
        if SessionStatus::of(&self.sessions[idx].app) != SessionStatus::Idle {
            self.sessions[idx]
                .app
                .flash(format!("{what} applies when this turn finishes"));
        }
    }

    fn handle_session_op(
        &mut self,
        session: MakiId,
        kind: SessionOpKind,
        result: Result<(), String>,
    ) {
        // The tab may have been closed or reordered while the operation ran.
        let Some(idx) = self.position(session) else {
            return;
        };
        match kind {
            SessionOpKind::ModelChanged { spec } => match result {
                Ok(()) => self.apply_model_change(idx, &spec),
                Err(error) => self.sessions[idx].app.flash(error),
            },
            SessionOpKind::ProviderRefreshed => {
                if let Err(error) = result {
                    self.sessions[idx].app.flash(error);
                }
            }
            SessionOpKind::HistoryRewound { change } => match result {
                Ok(()) => {
                    if let Some(change) = change
                        .lock()
                        .unwrap_or_else(|error| error.into_inner())
                        .take()
                    {
                        let app = &mut self.sessions[idx].app;
                        if app.chats[0].agent_id == Some(change.agent_id) {
                            app.state.session_mut().meta.queued_messages.clear();
                            app.flush_restored_queue();
                        }
                        app.apply_history_change(change);
                    }
                }
                Err(error) => self.sessions[idx].app.flash(error),
            },
            SessionOpKind::OptionToggled { id, committed } => match result {
                Ok(()) => {
                    if let Some(enabled) = committed
                        .lock()
                        .unwrap_or_else(|error| error.into_inner())
                        .take()
                    {
                        complete_option_toggle(&mut self.sessions[idx].app, id, enabled);
                    }
                }
                Err(error) => self.sessions[idx].app.flash(error),
            },
            SessionOpKind::ModeChanged => match result {
                Ok(()) => self.sessions[idx].settle_plan_approval(),
                Err(error) => self.sessions[idx].app.flash(error),
            },
            SessionOpKind::DirectoryChanged { adopted } => match result {
                Ok(()) => {
                    let adopted = adopted.lock().unwrap_or_else(|e| e.into_inner()).take();
                    if let Some(path) = adopted {
                        self.sessions[idx].app.apply_directory_change(path);
                    }
                }
                Err(error) => self.sessions[idx].app.flash(format!("cd: {error}")),
            },
        }
        let (manager, root) = self.sessions[idx].handles.manager_and_root();
        if let Some(commit) = manager
            .actor(root)
            .ok()
            .and_then(|actor| actor.config_snapshot())
        {
            self.sessions[idx].project_config(&commit);
        }
    }

    /// The app-side half of a model change, run once the coordinator has
    /// adopted the model into the session's slot.
    fn apply_model_change(&mut self, idx: usize, spec: &str) {
        let model = self.sessions[idx].model_slot.load().model.clone();
        let app = &mut self.sessions[idx].app;
        app.update_model(&model);
        app.record_recent_model(spec);
    }

    /// The slot the status line, usage panel, and new-session default are
    /// about. Sessions own their models, so "current provider" means the
    /// focused tab's, falling back to the startup slot before any tab exists.
    fn focused_model_slot(&self) -> &Arc<ProviderSlot> {
        self.sessions
            .get(self.focused)
            .map_or(&self.ctx.model_slot, |rt| &rt.model_slot)
    }

    fn change_model(&mut self, idx: usize, spec: &str) {
        self.note_if_deferred(idx, "model");
        let target = self.selected_target(idx);
        self.dispatch_model_selection(
            idx,
            target,
            maki_commands::ModelSelection {
                spec: Some(Arc::from(spec)),
                ..Default::default()
            },
            None,
        );
    }

    /// Runs a command for a keymap or event callback against the agent that
    /// callback was pinned to, in the session that owns it.
    /// Pins the keymap callback to the agent selected when the key was
    /// pressed, so later focus changes do not retarget it.
    fn run_keybind(&mut self, idx: usize, id: u64) {
        self.sync_orchestration();
        let target = self.selected_target(idx).and_then(|target| {
            OrchestrationServices::template(&*self.orchestration, &target)
                .ok()
                .map(|template| TrustedTarget { target, template })
        });
        if !self.sessions[idx]
            .app
            .lua_event_handle
            .run_keybind_callback(id, target)
        {
            tracing::warn!(keybind_id = id, "keybind callback queue is full");
        }
    }

    /// The model catalog filtered by {target}'s model policy, so `/model`
    /// fragments resolve only to models the pinned agent may use.
    fn target_model_specs(&self, target: &AgentRef) -> Result<HostResponse, CommandError> {
        let specs = OrchestrationServices::available_models(&*self.orchestration, target)
            .map_err(|_| CommandError::StaleTarget)?;
        Ok(HostResponse::Context(
            maki_commands::HostContextResponse::Values(
                specs.iter().map(|spec| Arc::from(spec.as_str())).collect(),
            ),
        ))
    }

    fn selected_target(&self, idx: usize) -> Option<AgentRef> {
        selected_agent(&self.sessions[idx])
    }

    fn dispatch_rewind(
        &self,
        idx: usize,
        target: AgentRef,
        boundary: maki_agent::actor::HistoryBoundary,
    ) -> Result<(), String> {
        let actor = self.target_actor(idx, &target)?;
        let guard = actor.prepare_idle().map_err(|error| error.to_string())?;
        let (draft, replacement) = actor
            .rewind_snapshot(&boundary)
            .map_err(|error| error.to_string())?;
        if target.id() == self.sessions[idx].handles.manager_and_root().1 {
            self.sessions[idx].app.sync_live_children();
        }
        let operation = guard
            .reserve_prepared_operation(None)
            .map_err(|error| error.to_string())?;
        let publication = CheckpointPublication::default();
        operation
            .register_publication(&publication)
            .map_err(|error| error.to_string())?;
        let transaction = self.reserve_history_transaction(idx, &target, publication)?;
        let session = self.sessions[idx].id();
        let runtime = self.sessions[idx].handles.identity();
        let internal_tx = self.internal_tx.clone();
        smol::spawn(async move {
            let change = Arc::new(Mutex::new(None));
            let result = async {
                operation.resolve(None)?;
                operation.wait_ready().await?;
                let config = operation.ready_config()?;
                Ok::<_, maki_agent::ActorError>(config)
            }
            .await
            .map_err(|error| error.to_string());
            let result = match result.and_then(|config| transaction(config, true)) {
                Ok(transaction) => guard
                    .commit_prepared_transaction_held(operation, Some(replacement), transaction)
                    .await
                    .map(|(_, _, updated)| {
                        *change.lock().unwrap_or_else(|error| error.into_inner()) =
                            updated.map(|updated| HistoryChange { draft, ..updated });
                    })
                    .map_err(|error| error.to_string()),
                Err(error) => Err(error),
            };
            let _ = internal_tx.send(InternalEvent::SessionOp {
                session,
                runtime,
                kind: SessionOpKind::HistoryRewound { change },
                result,
            });
        })
        .detach();
        Ok(())
    }

    /// Commits the toggle in the background. `reply` hears the settled
    /// commit; a reservation refusal (such as a held idle guard) returns
    /// synchronously instead.
    fn dispatch_target_toggle(
        &mut self,
        idx: usize,
        target: &AgentRef,
        id: &'static str,
        reply: Option<flume::Sender<Result<HostResponse, CommandError>>>,
    ) -> Result<(), String> {
        self.target_actor(idx, target)?;
        let change = match id {
            FAST_OPTION_ID if self.sessions[idx].drop_pending_fast(target) => {
                ConfigChange::Patch(ConfigPatch {
                    fast: Some(false),
                    ..Default::default()
                })
            }
            FAST_OPTION_ID => ConfigChange::ToggleFast,
            WORKFLOW_OPTION_ID => ConfigChange::ToggleWorkflow,
            _ => return Err(format!("{UNSUPPORTED_AGENT_OPTION_PREFIX}: {id}")),
        };
        let ticket = reserve_config_change(
            target,
            &self.sessions[idx].coordinator,
            Some(Arc::clone(&self.orchestration)),
        )
        .map_err(|error| error.to_string())?;
        let session = self.sessions[idx].id();
        let runtime = self.sessions[idx].handles.identity();
        let internal_tx = self.internal_tx.clone();
        smol::spawn(async move {
            let result = ticket
                .commit_change(change)
                .await
                .map(|_| ())
                .map_err(|error| error.to_string());
            if let Some(reply) = reply {
                let _ = reply.send(
                    result
                        .clone()
                        .map(|()| HostResponse::Completed)
                        .map_err(|error| CommandError::Producer(Arc::from(error))),
                );
            }
            let _ = internal_tx.send(InternalEvent::SessionOp {
                session,
                runtime,
                kind: SessionOpKind::ProviderRefreshed,
                result,
            });
        })
        .detach();
        Ok(())
    }

    fn dispatch_target_directory(
        &self,
        idx: usize,
        target: &AgentRef,
        path: PathBuf,
    ) -> Result<(), String> {
        self.target_actor(idx, target)?;
        let ticket = reserve_config_change(
            target,
            &self.sessions[idx].coordinator,
            Some(Arc::clone(&self.orchestration)),
        )
        .map_err(|error| error.to_string())?;
        let root = self.sessions[idx].handles.manager_and_root().1;
        let is_root = target.id() == root;
        let session = self.sessions[idx].id();
        let runtime = self.sessions[idx].handles.identity();
        let internal_tx = self.internal_tx.clone();
        smol::spawn(async move {
            let adopted = Arc::new(Mutex::new(None));
            let result = ticket
                .commit(async |_, predecessor| {
                    let path = canonical_directory(predecessor.context.cwd(), path).await?;
                    Ok((publication_claim(), ConfigChange::WorkingDirectory(path)))
                })
                .await
                .map(|commit| {
                    if is_root {
                        *adopted.lock().unwrap_or_else(|error| error.into_inner()) =
                            Some(commit.config.context.cwd().to_path_buf());
                    }
                })
                .map_err(|error| error.to_string());
            let _ = internal_tx.send(InternalEvent::SessionOp {
                session,
                runtime,
                kind: SessionOpKind::DirectoryChanged { adopted },
                result,
            });
        })
        .detach();
        Ok(())
    }

    fn reserve_history_transaction(
        &self,
        idx: usize,
        target: &AgentRef,
        publication: CheckpointPublication,
    ) -> Result<approval::HistoryTransaction, String> {
        let root = self.sessions[idx].handles.manager_and_root().1;
        if target.id() == root {
            let reservation = self.sessions[idx]
                .coordinator
                .reserve_transaction(publication)
                .map_err(|error| error.to_string())?;
            return Ok(Box::new(move |config, history_changed| {
                reservation
                    .prepare(config, publication_claim(), history_changed)
                    .map_err(|error| error.to_string())
            }));
        }
        let key = self.sessions[idx]
            .app
            .chats
            .iter()
            .find(|chat| chat.agent_id == Some(target.id()))
            .and_then(|chat| chat.subagent_id.clone())
            .ok_or_else(|| CHILD_HISTORY_IDENTITY_ERR.to_owned())?;
        let guard = match &self.sessions[idx].session_lock {
            Some(SessionLockState::Held(lock)) => lock.publication_guard(),
            _ => return Err(LOCK_LOST_REPLACEMENT_ERR.to_owned()),
        };
        let writer = Arc::clone(&self.ctx.storage_writer);
        let session_id = self.sessions[idx].id();
        let registration = self.sessions[idx]
            .coordinator
            .register_publication(&publication)
            .map_err(|error| error.to_string())?;
        Ok(Box::new(move |_, history_changed| {
            Ok(PreparedTransaction {
                publication,
                publish: Box::new(move |snapshot, publication| {
                    Box::pin(async move {
                        let _registration = registration;
                        writer
                            .checkpoint_staged(crate::storage_writer::StagedSessionCheckpoint {
                                session_id,
                                checkpoint: None,
                                history: history_changed.then_some((
                                    maki_storage::sessions::HistoryDestination::Child(key.clone()),
                                    snapshot.history,
                                )),
                                selected_config: Some((
                                    maki_storage::sessions::HistoryDestination::Child(key),
                                    snapshot.config,
                                )),
                                publication,
                                claim: publication_claim(),
                                guard,
                            })
                            .await
                    })
                }),
                finalize: Box::new(|_| {}),
            })
        }))
    }

    fn target_actor(
        &self,
        idx: usize,
        target: &AgentRef,
    ) -> Result<maki_agent::AgentActorHandle, String> {
        let (manager, root) = self.sessions[idx].handles.manager_and_root();
        let root = manager.lookup(root).map_err(|error| error.to_string())?;
        if !root.same_manager(target) {
            return Err(NOT_LIVE_ERR.to_owned());
        }
        target.validate_live().map_err(|error| error.to_string())?;
        target
            .manager()
            .actor(target.id())
            .map_err(|error| error.to_string())
    }

    fn dispatch_model_selection(
        &mut self,
        idx: usize,
        target: Option<AgentRef>,
        selection: maki_commands::ModelSelection,
        reply: Option<flume::Sender<Result<HostResponse, CommandError>>>,
    ) {
        if let Some(target) = &target
            && (selection.spec.is_some() || selection.fast.is_some())
        {
            self.sessions[idx].drop_pending_fast(target);
        }
        let (_, root) = self.sessions[idx].handles.manager_and_root();
        let root_spec = target
            .as_ref()
            .filter(|target| target.id() == root)
            .and_then(|_| selection.spec.clone());
        let target_policy = target.as_ref().and_then(|target| {
            OrchestrationServices::template(&*self.orchestration, target)
                .ok()
                .map(|template| template.model_policy)
        });
        let reservation = target
            .ok_or_else(|| CLOSED_TARGET_ERR.into())
            .and_then(|target| {
                self.target_actor(idx, &target)?;
                reserve_config_change(
                    &target,
                    &self.sessions[idx].coordinator,
                    Some(Arc::clone(&self.orchestration)),
                )
            });
        let policy = target_policy.unwrap_or_else(|| Arc::clone(&self.ctx.model_policy));
        let prepare = model_preparer(Arc::clone(&self.ctx.prepare_provider), self.ctx.timeouts);
        let internal_tx = self.internal_tx.clone();
        let session = self.sessions[idx].id();
        let runtime = self.sessions[idx].handles.identity();
        smol::spawn(async move {
            let patch = ConfigPatch {
                thinking: selection.thinking,
                fast: selection.fast,
                ..Default::default()
            };
            let spec = selection.spec.as_deref();
            let result = match reservation {
                Ok(ticket) => commit_model_patch(ticket, patch, spec, &policy, &prepare)
                    .await
                    .map(|_| ()),
                Err(error) => Err(error),
            }
            .map_err(|error| error.to_string());
            if result.is_ok()
                && let Some(spec) = root_spec
            {
                let _ = internal_tx.send(InternalEvent::SessionOp {
                    session,
                    runtime: Arc::clone(&runtime),
                    kind: SessionOpKind::ModelChanged {
                        spec: spec.to_string(),
                    },
                    result: Ok(()),
                });
            }
            if let Some(reply) = reply {
                let _ = reply.send(
                    result
                        .map(|()| HostResponse::Completed)
                        .map_err(|error| CommandError::Producer(Arc::from(error))),
                );
            } else {
                let _ = internal_tx.send(InternalEvent::SessionOp {
                    session,
                    runtime,
                    kind: SessionOpKind::ProviderRefreshed,
                    result,
                });
            }
        })
        .detach();
    }

    /// Re-prepares the root's current model, for instance after logging in to its provider.
    fn reprepare_root_model(&self, idx: usize, spec: String) {
        let (manager, root) = self.sessions[idx].handles.manager_and_root();
        let reserved = manager
            .actor(root)
            .map_err(ConfigChangeError::from)
            .and_then(|actor| {
                let expected = actor
                    .effective_config()
                    .ok_or("missing actor configuration")?;
                let target = manager.lookup(root)?;
                let ticket = reserve_config_change(&target, &self.sessions[idx].coordinator, None)?;
                Ok((ticket, expected))
            });
        let session = self.sessions[idx].id();
        let identity = self.sessions[idx].handles.identity();
        let internal_tx = self.internal_tx.clone();
        let policy = Arc::clone(&self.ctx.model_policy);
        let prepare = model_preparer(Arc::clone(&self.ctx.prepare_provider), self.ctx.timeouts);
        smol::spawn(async move {
            let result = match reserved {
                Ok((ticket, expected)) => prepare_config_change(
                    ticket,
                    async move {
                        let replacement = prepare_model(&spec, &policy, &prepare).await?;
                        Ok(ConfigChange::Refresh {
                            expected: PreparedModel {
                                provider: Arc::clone(&expected.provider),
                                model: expected.model.clone(),
                            },
                            replacement,
                        })
                    },
                    async { smol::Timer::after(SESSION_OP_TIMEOUT).await },
                )
                .await
                .map(|_| ()),
                Err(error) => Err(error),
            }
            .map_err(|error| error.to_string());
            let _ = internal_tx.send(InternalEvent::SessionOp {
                session,
                runtime: identity,
                kind: SessionOpKind::ProviderRefreshed,
                result,
            });
        })
        .detach();
    }

    fn refresh_models(&mut self) {
        self.model_discovery = discover_models(
            &self.ctx.catalog,
            Arc::clone(&self.ctx.model_policy),
            self.focused_model_slot().load().model.provider.to_string(),
            self.warn_tx.clone(),
            self.internal_tx.clone(),
        );
    }

    /// The UI slot only seeds new tabs, so it is written directly; the
    /// catalog's discovery refreshes live agents.
    fn refresh_discovered_models(&mut self) {
        let current = self.ctx.model_slot.load();
        if let Some(model) =
            maki_agent::actor::rediscovered_model(&*current.provider, &current.model)
        {
            let provider = Arc::clone(&current.provider) as Arc<dyn Provider>;
            drop(current);
            self.ctx.model_slot.project(model, provider);
        }
        self.sync_orchestration();
        self.sync_model_discovery();
    }

    fn refresh_provider(&mut self, slug: String) {
        let idx = self.focused;
        let slot = Arc::clone(self.focused_model_slot());
        let model = slot.load().model.clone();
        if model.provider.to_string() == slug {
            self.reprepare_root_model(idx, model.spec());
        } else if let Some(builtin) = maki_config::providers::builtin_provider(&slug) {
            self.change_model(self.focused, builtin.default_model);
        }
    }

    fn shutdown(mut self) -> ShutdownReport {
        self.model_discovery = Discovery::idle();
        let outputs = self.provider_usage.handle(ProviderUsageInput::Shutdown);
        self.handle_provider_usage_outputs(outputs);
        let started = Instant::now();
        let mut phase_start = started;
        let mut lap = || {
            let elapsed = phase_start.elapsed().as_millis() as u64;
            phase_start = Instant::now();
            elapsed
        };
        let exit = self.sessions[self.focused].app.exit_request;
        if let Some(ref h) = self.ctx.mcp_handle {
            mcp::kill_process_groups(&h.reader().load().pids);
        }
        for rt in &self.sessions {
            let _ = rt.handles.cmd_tx.try_send(AgentCommand::CancelAll);
        }
        let kill_mcp_ms = lap();
        let heartbeat_deadline = Instant::now() + AGENT_SHUTDOWN_TIMEOUT;
        let mut tabs = Vec::with_capacity(self.sessions.len());
        let mut agent_tasks = Vec::with_capacity(self.sessions.len());
        let mut coordinators = Vec::with_capacity(self.sessions.len());
        let mut session_leases = Vec::with_capacity(self.sessions.len());
        for rt in self.sessions.drain(..) {
            let SessionRuntime {
                mut app,
                handles,
                coordinator,
                session_lock,
                lock_lost,
                ..
            } = rt;
            let heartbeat_timeout = heartbeat_deadline.saturating_duration_since(Instant::now());
            let settled_lock = settle_lock_state(session_lock, heartbeat_timeout);
            match settled_lock {
                LockSettlement::Held(lease) => {
                    let snapshot = coordinator.read().options();
                    if !lock_lost {
                        app.checkpoint_now();
                    }
                    let mut session = Arc::unwrap_or_clone(Arc::clone(&app.state.session));
                    apply_options_to_session(&mut session, &snapshot);
                    session.meta.yolo = app.permissions.persisted_yolo();
                    app.state.session = Arc::new(session);
                    session_leases.push((app.state.session.id, lease));
                }
                LockSettlement::TimedOut => {
                    warn!(id = %app.state.session.id, "session lock heartbeat timed out during shutdown");
                }
                LockSettlement::Lost | LockSettlement::None => {}
            }
            coordinators.push(coordinator);
            // `app` drops at the end of this iteration, closing the
            // channels the agent loop waits on, so `join_all` can finish.
            tabs.push(Arc::unwrap_or_clone(app.state.session));
            agent_tasks.push(handles.shutdown());
        }
        let save_sessions_ms = lap();
        crate::agent::join_all(agent_tasks, AGENT_SHUTDOWN_TIMEOUT);
        // Dropping the handles is the teardown: the coordinator's loop ends
        // when its channel closes and unregisters on the way out, and `Close`
        // persists nothing. Awaiting it would buy nothing and could hang exit
        // behind a turn that never finishes.
        drop(coordinators);
        let join_agents_ms = lap();
        if let Some(ref h) = self.ctx.mcp_handle {
            smol::block_on(h.shutdown());
        }
        let mcp_shutdown_ms = lap();
        self.ctx.storage_writer.shutdown(AGENT_SHUTDOWN_TIMEOUT);
        let storage_drain_ms = lap();
        for (id, lease) in session_leases {
            if let Err(error) = lease.release() {
                warn!(%id, %error, "session lock release failed");
            }
        }
        let release_locks_ms = lap();
        info!(
            kill_mcp_ms,
            save_sessions_ms,
            join_agents_ms,
            mcp_shutdown_ms,
            storage_drain_ms,
            release_locks_ms,
            total_ms = started.elapsed().as_millis() as u64,
            "ui shutdown phases"
        );
        ShutdownReport {
            exit,
            tabs,
            focused: self.focused,
        }
    }
}

fn live_session_row(id: MakiId, app: &App, focused: bool) -> serde_json::Value {
    json!({
        "id": id,
        "title": app.state.session.title,
        "status": SessionStatus::of(app).as_str(),
        "updated_at": app.state.session.updated_at,
        "message_count": live_message_count(app),
        "focused": focused,
    })
}

/// The agent publishes its in-flight history to the mirror as it works, so a
/// turn that has not reached a checkpoint yet is still visible there. The
/// mirrored history is the main transcript only; subagent messages live
/// elsewhere.
fn live_message_count(app: &App) -> usize {
    app.shared_history
        .as_ref()
        .map(|h| h.load().messages.len())
        .unwrap_or_else(|| app.state.session.messages().len())
}

impl UiSink for EventLoop<'_> {
    fn notify(&mut self, text: String) {
        self.focused_app().flash(text);
    }

    fn render(&mut self, action: UiAction) {
        match action {
            UiAction::Flash(text) => self.notify(text),
            UiAction::SetWindowTitle(title) => {
                if let Err(error) = terminal::set_window_title(&title) {
                    tracing::warn!(%error, "failed to set window title");
                }
            }
            UiAction::OpenEditor {
                target,
                path,
                reply_tx,
            } => {
                let pinned = target
                    .as_ref()
                    .ok_or_else(|| CLOSED_TARGET_ERR.to_owned())
                    .and_then(|target| self.pinned_editor_path(target, &path));
                let code = match pinned {
                    Ok((index, path)) => self.open_editor(index, &path),
                    Err(error) => {
                        self.focused_app().flash(error);
                        -1
                    }
                };
                let _ = reply_tx.send(code);
            }
            UiAction::OpenListPicker {
                id,
                items,
                config,
                reply_tx,
            } => {
                self.focused_app()
                    .lua_picker
                    .open(id, items, config, reply_tx);
            }
            UiAction::OpenWin {
                buf,
                config,
                focus,
                event_tx,
                cmd_rx,
            } => {
                let app = self.focused_app();
                app.handle_open_win(buf, config, focus, event_tx, cmd_rx);
                // An active (non-deferred) ask rings now; a deferred one rings
                // on promotion. Draining here keeps the active path immediate.
                if app.take_pending_bell() {
                    ring_bell();
                }
            }
            UiAction::Session { req, reply_tx } => {
                self.handle_session_request(req, reply_tx);
            }
            UiAction::ProviderUsageAck(ack) => {
                self.handle_provider_usage_ack(ack);
            }
            UiAction::UsageFetch { force, reply_tx } => {
                let provider = self.focused_model_slot().load().provider.identity();
                let outputs = self
                    .provider_usage
                    .handle(ProviderUsageInput::Transition { provider });
                self.handle_provider_usage_outputs(outputs);
                let kind = if force {
                    ProviderUsageRequestKind::Forced
                } else {
                    ProviderUsageRequestKind::Ordinary
                };
                let outputs = self.provider_usage.handle(ProviderUsageInput::Request {
                    provider,
                    kind,
                    waiter: reply_tx,
                });
                self.handle_provider_usage_outputs(outputs);
            }
            UiAction::WinSaveView { reply_tx } => {
                let _ = reply_tx.send(self.focused_app().win_view());
            }
            UiAction::WinRestView { scroll_top } => {
                self.focused_app().set_scroll_top(scroll_top);
            }
            UiAction::Builtin(action) => {
                let actions = self.focused_app().run_builtin(action);
                self.dispatch(self.focused, actions);
            }
        }
    }
}

fn provider_usage_limit(limit: maki_providers::UsageLimit) -> ProviderUsageLimit {
    ProviderUsageLimit {
        window: match limit.kind {
            maki_providers::UsageWindow::Hours(value) => ProviderUsageWindow::Hours { value },
            maki_providers::UsageWindow::Days(value) => ProviderUsageWindow::Days { value },
            maki_providers::UsageWindow::Monthly => ProviderUsageWindow::Monthly,
            maki_providers::UsageWindow::Weekly { model } => ProviderUsageWindow::Weekly { model },
            maki_providers::UsageWindow::Credits => ProviderUsageWindow::Credits,
            maki_providers::UsageWindow::Subscription => ProviderUsageWindow::Subscription,
            maki_providers::UsageWindow::Other(label) => ProviderUsageWindow::Other { label },
        },
        percentage: limit.percentage,
        reset_at_ms: limit.reset_at,
        detail: limit.detail,
    }
}

/// The TUI's provider preparation as the coordinator's async preparer.
fn model_preparer(prepare: PrepareProvider, timeouts: Timeouts) -> ModelPreparer {
    Arc::new(move |model| {
        let prepare = Arc::clone(&prepare);
        Box::pin(smol::unblock(move || prepare(model, timeouts)))
    })
}

fn runtime_config_mutations(
    runtime: &SessionRuntime,
    writer: Arc<StorageWriter>,
    orchestration: Arc<AgentOrchestration>,
) -> ConfigMutationReserver {
    let coordinator = runtime.coordinator.clone();
    let session_id = runtime.id();
    let orchestration = Arc::downgrade(&orchestration);
    let (manager, root_id) = runtime.handles.manager_and_root();
    let guard = match &runtime.session_lock {
        Some(SessionLockState::Held(lock)) => Some(lock.publication_guard()),
        _ => None,
    };
    Arc::new(move |target, publication| {
        if !manager.lookup(root_id)?.same_manager(target) {
            return Err(NOT_LIVE_ERR.into());
        }
        target.validate_live()?;
        if target.id() == root_id {
            return Ok(coordinator
                .reserve_transaction(publication)?
                .into_config_mutation());
        }
        let key = orchestration
            .upgrade()
            .ok_or(NOT_LIVE_ERR)?
            .history_key(target)?;
        let guard = guard.clone().ok_or(LOCK_LOST_REPLACEMENT_ERR)?;
        let writer = Arc::clone(&writer);
        let registration = coordinator.register_publication(&publication)?;
        Ok(Box::new(move |_, claim| {
            Ok(PreparedTransaction {
                publication,
                publish: Box::new(move |snapshot, publication| {
                    Box::pin(async move {
                        let _registration = registration;
                        writer
                            .checkpoint_staged(crate::storage_writer::StagedSessionCheckpoint {
                                session_id,
                                checkpoint: None,
                                history: None,
                                selected_config: Some((
                                    maki_storage::sessions::HistoryDestination::Child(key),
                                    snapshot.config,
                                )),
                                publication,
                                claim,
                                guard,
                            })
                            .await
                    })
                }),
                finalize: Box::new(|_| {}),
            })
        }))
    })
}

fn reserve_config_change(
    target: &AgentRef,
    coordinator: &SessionCoordinatorHandle,
    services: Option<Arc<AgentOrchestration>>,
) -> Result<ConfigChangeTicket<'static>, ConfigChangeError> {
    let root = target.snapshot()?.parent_id.is_none();
    let coordinator = coordinator.clone();
    let child = target.clone();
    ConfigChangeTicket::reserve(
        target,
        CheckpointPublication::default(),
        move |publication| {
            if root {
                return Ok(coordinator
                    .reserve_transaction(publication)?
                    .into_config_mutation());
            }
            services
                .as_deref()
                .ok_or(DURABLE_CONFIG_UNAVAILABLE)?
                .reserve_config_mutation(&child, publication)
        },
    )
}

/// Commits {patch} with {spec}'s model prepared, unless preparation outlives
/// the session op timeout.
async fn commit_model_patch(
    ticket: ConfigChangeTicket<'_>,
    patch: ConfigPatch,
    spec: Option<&str>,
    policy: &ModelPolicy,
    prepare: &ModelPreparer,
) -> Result<ConfigCommit, ConfigChangeError> {
    let change = async {
        Ok(ConfigChange::Patch(
            prepare_patch(patch, spec, policy, prepare).await?,
        ))
    };
    prepare_config_change(ticket, change, async {
        smol::Timer::after(SESSION_OP_TIMEOUT).await
    })
    .await
}

/// Commits the change `prepare` yields once the predecessor settles, unless
/// `expiry`, first polled then, fires before the publication is claimed.
async fn prepare_config_change<T>(
    mut ticket: ConfigChangeTicket<'_>,
    prepare: impl Future<Output = Result<ConfigChange, ConfigChangeError>>,
    expiry: impl Future<Output = T>,
) -> Result<ConfigCommit, ConfigChangeError> {
    ticket.wait_predecessor().await?;
    let publication = ticket.publication();
    let operation = ticket.commit(async |_, _| Ok((publication_claim(), prepare.await?)));
    futures_lite::pin!(operation);
    match futures_lite::future::or(async { Some(operation.as_mut().await) }, async {
        expiry.await;
        None
    })
    .await
    {
        Some(result) => result,
        None if publication.cancel() => Err(CONFIG_EXPIRED_ERR.into()),
        None => operation.await,
    }
}

fn prepare_provider(mut model: Model, timeouts: Timeouts) -> Result<PreparedModel, Arc<str>> {
    let provider =
        from_model(&mut model, timeouts).map_err(|error| Arc::from(error.to_string()))?;
    Ok(PreparedModel {
        provider: Arc::from(provider),
        model,
    })
}

fn command_target_index(
    targets: impl IntoIterator<Item = maki_commands::InvocationTargetId>,
    target: maki_commands::InvocationTargetId,
) -> Option<usize> {
    targets
        .into_iter()
        .position(|candidate| candidate == target)
}

fn scroll_delta(kind: MouseEventKind, lines: u32) -> i32 {
    if kind == MouseEventKind::ScrollUp {
        lines as i32
    } else {
        -(lines as i32)
    }
}

async fn bounded_session_op<T, F>(op: F, timeout: Duration) -> Result<T, String>
where
    F: std::future::Future<Output = Result<T, String>>,
{
    futures_lite::future::or(op, async move {
        smol::Timer::after(timeout).await;
        Err(SESSION_OP_TIMEOUT_ERR.to_owned())
    })
    .await
}

fn dispatch_option_toggle(
    runtime: &SessionRuntime,
    id: &'static str,
    internal_tx: &flume::Sender<InternalEvent>,
) {
    let session = runtime.id();
    let identity = runtime.handles.identity();
    let coordinator = runtime.coordinator.clone();
    let committed: Arc<std::sync::Mutex<Option<bool>>> = Arc::default();
    let slot = Arc::clone(&committed);
    let internal_tx = internal_tx.clone();
    smol::spawn(async move {
        // A toggle prepares nothing, so it has no preparation to bound; it
        // waits its turn behind other changes like any config change.
        let result = coordinator
            .toggle_boolean_option(id)
            .await
            .map(|(enabled, _)| {
                *slot.lock().unwrap_or_else(|error| error.into_inner()) = Some(enabled);
            })
            .map_err(|error| error.to_string());
        let _ = internal_tx.send(InternalEvent::SessionOp {
            session,
            runtime: identity,
            kind: SessionOpKind::OptionToggled { id, committed },
            result,
        });
    })
    .detach();
}

fn ring_bell() {
    use std::io::Write;
    let mut out = std::io::stdout().lock();
    let _ = out.write_all(b"\x07");
    let _ = out.flush();
}

#[cfg(test)]
mod tests {
    use super::*;
    use maki_domain::ThinkingConfig as DomainThinkingConfig;
    use maki_storage::checkpoint::ClaimingWriter;

    /// Projects every pending config commit the way the event loop's tick does,
    /// so a test that mutates config without running a frame still sees what a
    /// running UI would.
    fn drain_config_commits(runtime: &mut SessionRuntime) {
        while let Ok(commit) = runtime.config_commits.try_recv() {
            runtime.project_config(&commit);
        }
    }

    fn spawn_root_workflow_toggle(
        runtime: &SessionRuntime,
    ) -> smol::Task<Result<ConfigCommit, ConfigChangeError>> {
        let (manager, root) = runtime.handles.manager_and_root();
        let target = manager.lookup(root).unwrap();
        let reservation = reserve_config_change(&target, &runtime.coordinator, None).unwrap();
        smol::spawn(reservation.commit_change(ConfigChange::ToggleWorkflow))
    }

    async fn wait_projected_option(runtime: &SessionRuntime, id: &str, value: &str) {
        let read = runtime.coordinator.read();
        let mut subscription = read.subscribe();
        loop {
            if read.options().options.iter().any(|option| {
                option.definition.id.as_ref() == id && option.current_value.as_ref() == value
            }) {
                return;
            }
            subscription.changed().await;
        }
    }
    use crate::selection::SelectionZone;
    use crossterm::event::{KeyCode, KeyModifiers};
    use maki_agent::tools::offload::{MAX_OFFLOAD_SESSION_BYTES, OffloadBackend, OffloadError};
    use maki_agent::{
        AgentId, DoneReason, Envelope, SessionMailbox, SubagentInfo, TurnId, TurnOutcome,
    };
    use maki_config::PermissionsConfig;
    use maki_lua::BuiltinAction;
    use maki_providers::model::FastSupport;
    use maki_providers::{Effort, TokenUsage};
    use ratatui::{Terminal, backend::TestBackend};
    use std::sync::atomic::AtomicBool;
    use tempfile::TempDir;
    use test_case::test_case;

    const OBSERVATION: &str = "failed";
    const PENDING_FAST_MODEL: &str = "pending-fast-model";
    const SWITCHED_FAST_MODEL: &str = "switched-fast-model";
    const DISCOVERY_PROVIDER: &str = "synthetic";
    const STARTUP_MODEL_ID: &str = "tui-discovery-startup";
    const RESUMED_MODEL_ID: &str = "tui-discovery-resumed";
    const UNRELATED_MODEL_ID: &str = "tui-discovery-unrelated";
    const BUSY_MODEL_ID: &str = "tui-discovery-busy";
    const DISCOVERED_WINDOW: u32 = 524_000;

    #[test]
    fn ui_queue_and_mode_model_projection() {
        smol::block_on(async {
            let runtime = test_runtime(model_named("test-model"));
            let (manager, root) = runtime.handles.manager_and_root();
            let actor = manager.actor(root).unwrap();
            let initial = actor.effective_config().unwrap();
            let lease = actor.reserve_prepared_operation(None).unwrap();
            let toggle = spawn_root_workflow_toggle(&runtime);
            runtime.handles.queue.push(QueueItem {
                input: maki_agent::AgentInput {
                    message: "after barrier".into(),
                    images: Vec::new(),
                    preamble: Vec::new(),
                    prompt: None,
                },
                displayed: None,
            });
            assert_eq!(runtime.handles.queue.text_messages(), vec!["after barrier"]);
            drop(lease);
            toggle.await.unwrap();
            let selected = actor.effective_config().unwrap();
            assert!(!initial.workflow);
            assert!(selected.workflow);
            assert_eq!(selected.model.id, runtime.model_slot.load().model.id);
            assert_eq!(runtime.app.state.session.model, selected.model.spec());
            release_runtime(runtime);
        });
    }

    #[test]
    fn workflow_toggle_completion_does_not_mutate_another_runtime() {
        smol::block_on(async {
            let changed = test_runtime(model_named("changed"));
            let untouched = test_runtime(model_named("untouched"));
            let (changed_manager, changed_root) = changed.handles.manager_and_root();
            let changed_actor = changed_manager.actor(changed_root).unwrap();
            let (untouched_manager, untouched_root) = untouched.handles.manager_and_root();
            let untouched_actor = untouched_manager.actor(untouched_root).unwrap();
            let initial_actor_config = untouched_actor.effective_config().unwrap();
            let initial_actor_model = initial_actor_config.settings.model.spec();
            let initial_actor_mode = initial_actor_config.mode.clone();
            let initial_actor_workflow = initial_actor_config.settings.workflow;
            wait_projected_option(&untouched, "model", &initial_actor_config.model.spec()).await;
            let initial_options = untouched.coordinator.read().options();
            let initial_app_model = untouched.app.state.session.model.clone();

            spawn_root_workflow_toggle(&changed).await.unwrap();

            assert!(changed.app.root_config().workflow);
            assert!(changed_actor.effective_config().unwrap().workflow);
            assert!(!untouched.app.root_config().workflow);
            assert_eq!(untouched.app.state.session.model, initial_app_model);
            let untouched_config = untouched_actor.effective_config().unwrap();
            assert_eq!(untouched_config.settings.model.spec(), initial_actor_model);
            assert_eq!(untouched_config.mode, initial_actor_mode);
            assert_eq!(untouched_config.settings.workflow, initial_actor_workflow);
            let values = |snapshot: SessionOptionsSnapshot| {
                snapshot
                    .options
                    .iter()
                    .map(|option| (option.definition.id.clone(), option.current_value.clone()))
                    .collect::<Vec<_>>()
            };
            assert_eq!(
                values(untouched.coordinator.read().options()),
                values(initial_options)
            );

            release_runtime(changed);
            release_runtime(untouched);
        });
    }

    #[test_case(false ; "workflow_host_command_commits_on_the_pinned_actor")]
    #[test_case(true ; "workflow_host_command_waits_for_a_held_idle_guard")]
    fn workflow_host_command_on_the_pinned_actor(held: bool) {
        with_event_loop(|event_loop| {
            let (manager, root) = event_loop.sessions[0].handles.manager_and_root();
            let actor = manager.actor(root).unwrap();
            let guard = held.then(|| actor.prepare_idle().unwrap());
            let commits = actor.subscribe_config_commits();
            let (reply, response) = flume::bounded(1);
            event_loop.handle_command(CommandEvent::Host {
                target: event_loop.sessions[0].app.command_target.id(),
                request: maki_commands::HostRequest::Builtin(BuiltinOperation::ToggleWorkflow),
                admission: Some(Box::new(maki_lua::orchestration::TrustedTarget {
                    target: manager.lookup(root).unwrap(),
                    template: event_loop.sessions[0]
                        .handles
                        .orchestration_template
                        .clone(),
                })),
                reply,
            });
            if held {
                assert!(commits.is_empty());
            }
            drop(guard);
            let response = response.recv_timeout(SESSION_OP_TIMEOUT).unwrap();
            assert_eq!(response.unwrap(), HostResponse::Completed);
            loop {
                let commit = commits.recv_timeout(SESSION_OP_TIMEOUT).unwrap();
                if commit.config.workflow {
                    break;
                }
            }
            assert!(actor.effective_config().unwrap().workflow);
        });
    }

    #[test]
    fn reserved_mode_change_waits_for_predecessor_before_committing() {
        smol::block_on(async {
            let runtime = test_runtime(model_named("test-model"));
            let (manager, root) = runtime.handles.manager_and_root();
            let actor = manager.actor(root).unwrap();
            let initial_mode = runtime.app.root_config().mode.clone();
            let predecessor = actor.reserve_prepared_operation(None).unwrap();
            let (internal_tx, internal_rx) = flume::unbounded();
            let temp_dir = tempfile::tempdir().unwrap();
            let storage = StateDir::from_path(temp_dir.path().to_path_buf());
            dispatch_mode_commit(
                &runtime,
                Ok(manager.lookup(root).unwrap()),
                &storage,
                EventHandle::disconnected_for_test().mode_registry(),
                None,
                internal_tx,
                "plan".into(),
            );

            assert_eq!(runtime.app.root_config().mode, initial_mode);
            assert!(internal_rx.is_empty());
            drop(predecessor);
            let InternalEvent::SessionOp { kind, result, .. } =
                internal_rx.recv_async().await.unwrap()
            else {
                panic!("expected mode change completion");
            };
            result.unwrap();
            assert!(matches!(kind, SessionOpKind::ModeChanged));
            let config = actor.effective_config().unwrap();
            let plan = config.mode.plan_path().unwrap();
            assert!(plan.starts_with(temp_dir.path().join(maki_storage::plans::PLANS_DIR)));
            assert_eq!(
                config.mode_def,
                EventHandle::disconnected_for_test()
                    .mode_registry()
                    .get(&maki_agent::ModeId::Plan)
            );
            drop(temp_dir);
            release_runtime(runtime);
        });
    }

    #[test]
    fn yolo_toggle_commits_while_actor_operation_is_pending() {
        smol::block_on(async {
            let runtime = test_runtime(model_named("test-model"));
            let (manager, root) = runtime.handles.manager_and_root();
            let pending = manager
                .actor(root)
                .unwrap()
                .reserve_prepared_operation(None)
                .unwrap();
            let (internal_tx, internal_rx) = flume::unbounded();

            dispatch_option_toggle(&runtime, YOLO_OPTION_ID, &internal_tx);

            let InternalEvent::SessionOp {
                kind: SessionOpKind::OptionToggled { committed, .. },
                result,
                ..
            } = internal_rx.recv_async().await.unwrap()
            else {
                panic!("expected option toggle completion");
            };
            result.unwrap();
            assert_eq!(committed.lock().unwrap().take(), Some(true));
            assert_eq!(
                runtime
                    .coordinator
                    .read()
                    .options()
                    .options
                    .iter()
                    .find(|option| option.definition.id.as_ref() == YOLO_OPTION_ID)
                    .unwrap()
                    .current_value
                    .as_ref(),
                ENABLED_VALUE
            );
            drop(pending);
            release_runtime(runtime);
        });
    }

    fn fast_model(id: &str, support: FastSupport) -> Model {
        let mut model = model_named(id);
        model.supports_fast_override = Some(support);
        model
    }

    fn model_change(model: Model) -> ConfigChange {
        ConfigChange::Patch(ConfigPatch {
            model: Some(maki_agent::actor::PreparedModel {
                provider: Arc::new(StubProvider),
                model,
            }),
            ..Default::default()
        })
    }

    fn pending_fast_runtime() -> SessionRuntime {
        let mut runtime = test_runtime(fast_model(PENDING_FAST_MODEL, FastSupport::Pending));
        runtime.app.state.fast_pending = true;
        runtime
    }

    async fn discover_fast_support(
        actor: &maki_agent::AgentActorHandle,
    ) -> maki_agent::actor::ConfigCommit {
        actor
            .reserve_prepared_operation(None)
            .unwrap()
            .commit_when_ready(Some(model_change(fast_model(
                PENDING_FAST_MODEL,
                FastSupport::Supported,
            ))))
            .await
            .unwrap()
            .config
    }

    async fn settled_config(
        actor: &maki_agent::AgentActorHandle,
    ) -> Arc<maki_agent::EffectiveAgentConfig> {
        actor
            .reserve_prepared_operation(None)
            .unwrap()
            .wait_predecessor()
            .await
            .unwrap()
    }

    #[test_case(None, PENDING_FAST_MODEL, true ; "queued_change_restores_once")]
    #[test_case(Some(SWITCHED_FAST_MODEL), SWITCHED_FAST_MODEL, false ; "queued_model_switch_skips_restore")]
    fn restored_fast_lands_on_the_discovered_model_once(
        switch: Option<&str>,
        settled_model: &str,
        restored: bool,
    ) {
        smol::block_on(async {
            let mut runtime = pending_fast_runtime();
            let (manager, root) = runtime.handles.manager_and_root();
            let actor = manager.actor(root).unwrap();
            let commits = actor.subscribe_config_commits();
            let discovered = discover_fast_support(&actor).await;
            let queued = actor.reserve_prepared_operation(None).unwrap();
            assert!(runtime.project_config(&discovered));
            let change = match switch {
                Some(id) => model_change(fast_model(id, FastSupport::Supported)),
                None => ConfigChange::Patch(ConfigPatch {
                    workflow: Some(true),
                    ..Default::default()
                }),
            };
            let queued = queued.commit_when_ready(Some(change)).await.unwrap().config;
            assert!(runtime.project_config(&queued));
            let settled = settled_config(&actor).await;
            assert_eq!(&*settled.model.id, settled_model);
            assert_eq!(settled.fast, restored);
            assert_eq!(commits.try_iter().count(), 2 + usize::from(restored));
            assert!(!runtime.app.state.fast_pending);
            release_runtime(runtime);
        });
    }

    #[test]
    fn explicit_fast_toggle_replaces_the_restored_wish() {
        with_event_loop_runtime(
            |_| pending_fast_runtime(),
            |event_loop| {
                let (manager, root) = event_loop.sessions[0].handles.manager_and_root();
                let actor = manager.actor(root).unwrap();
                let target = manager.lookup(root).unwrap();
                event_loop
                    .dispatch_target_toggle(0, &target, FAST_OPTION_ID, None)
                    .unwrap();
                assert!(!event_loop.sessions[0].app.state.fast_pending);
                smol::block_on(async {
                    assert!(!settled_config(&actor).await.fast);
                    let discovered = discover_fast_support(&actor).await;
                    assert!(event_loop.sessions[0].project_config(&discovered));
                    assert!(!settled_config(&actor).await.fast);
                });
            },
        );
    }

    #[test]
    fn combined_model_thinking_toggle_uses_predecessor_atomically() {
        smol::block_on(async {
            let runtime = test_runtime(model_named("test-model"));
            let (manager, root) = runtime.handles.manager_and_root();
            let actor = manager.actor(root).unwrap();
            let predecessor = actor.reserve_prepared_operation(None).unwrap();
            let combined = actor.reserve_prepared_operation(None).unwrap();
            let mut model = model_named("replacement");
            model.thinking_override = Some(maki_providers::ThinkingSupport::Yes);
            let combined = smol::spawn(combined.commit_when_ready(Some(ConfigChange::Patch(
                ConfigPatch {
                    model: Some(maki_agent::actor::PreparedModel {
                        provider: Arc::new(StubProvider),
                        model,
                    }),
                    toggle_thinking: true,
                    ..Default::default()
                },
            ))));
            predecessor
                .commit_when_ready(Some(ConfigChange::Patch(ConfigPatch {
                    workflow: Some(true),
                    ..Default::default()
                })))
                .await
                .unwrap();
            let commit = combined.await.unwrap().config;
            assert!(commit.config.workflow);
            assert!(commit.config.thinking.is_enabled());
            assert_eq!(commit.config.model.id, "replacement");
            release_runtime(runtime);
        });
    }

    #[test]
    fn managed_read_observes_committed_option() {
        smol::block_on(async {
            let runtime = test_runtime(model_named("test-model"));
            spawn_root_workflow_toggle(&runtime).await.unwrap();
            assert!(runtime.config().unwrap().workflow);
            release_runtime(runtime);
        });
    }

    #[test]
    fn expired_provider_preparation_releases_successors() {
        smol::block_on(async {
            let runtime = test_runtime(model_named("test-model"));
            let (manager, root) = runtime.handles.manager_and_root();
            let actor = manager.actor(root).unwrap();
            let target = manager.lookup(root).unwrap();
            let ticket = reserve_config_change(&target, &runtime.coordinator, None).unwrap();
            let (started_tx, started_rx) = flume::bounded(1);
            let (release_tx, release_rx) = flume::bounded(1);
            let (finished_tx, finished_rx) = flume::bounded(1);
            let (expire_tx, expire_rx) = flume::bounded(1);
            let pending = smol::spawn(prepare_config_change(
                ticket,
                async move {
                    smol::unblock(move || {
                        started_tx.send(()).unwrap();
                        release_rx.recv().unwrap();
                        finished_tx.send(()).unwrap();
                        Ok(ConfigChange::ToggleWorkflow)
                    })
                    .await
                },
                async move {
                    expire_rx.recv_async().await.unwrap();
                },
            ));
            started_rx.recv_async().await.unwrap();
            let successor = smol::spawn(
                actor
                    .reserve_prepared_operation(None)
                    .unwrap()
                    .commit_when_ready(Some(ConfigChange::ToggleWorkflow)),
            );
            expire_tx.send(()).unwrap();
            assert_eq!(
                pending.await.err().unwrap(),
                ConfigChangeError::from(CONFIG_EXPIRED_ERR)
            );
            assert!(successor.await.unwrap().config.config.workflow);
            release_tx.send(()).unwrap();
            finished_rx.recv_async().await.unwrap();
            assert!(actor.effective_config().unwrap().workflow);
            release_runtime(runtime);
        });
    }

    #[test_case::test_case(false; "successful_completion")]
    #[test_case::test_case(true; "failed_completion")]
    fn completion_snapshot_cannot_be_regressed_by_queued_commit(failed: bool) {
        with_event_loop(|event_loop| {
            smol::block_on(async {
                const FAILURE: &str = "configuration operation failed";
                let (manager, root) = event_loop.sessions[0].handles.manager_and_root();
                let actor = manager.actor(root).unwrap();
                let (queued_tx, queued_rx) = flume::unbounded();
                let observer =
                    std::mem::replace(&mut event_loop.sessions[0].config_commits, queued_rx);
                let reservation = reserve_config_change(
                    &manager.lookup(root).unwrap(),
                    &event_loop.sessions[0].coordinator,
                    None,
                )
                .unwrap();
                let older = reservation
                    .commit_change(ConfigChange::Patch(ConfigPatch {
                        workflow: Some(true),
                        ..Default::default()
                    }))
                    .await
                    .unwrap();
                commit_root_plan_mode(event_loop);
                let mut completion = event_loop.internal_rx.recv_async().await.unwrap();
                if failed {
                    let InternalEvent::SessionOp { result, .. } = &mut completion else {
                        panic!("session operation");
                    };
                    *result = Err(FAILURE.into());
                }
                let latest = actor.config_snapshot().unwrap();
                assert!(latest.generation > older.generation);
                event_loop.handle_internal(completion);
                assert_eq!(
                    event_loop.sessions[0]
                        .projected_config
                        .as_ref()
                        .map(|commit| commit.generation),
                    Some(latest.generation)
                );
                assert_eq!(event_loop.sessions[0].app.root_mode().id_key(), "plan");
                queued_tx.send(older).unwrap();
                let _ = event_loop.tick();
                assert_eq!(
                    event_loop.sessions[0]
                        .projected_config
                        .as_ref()
                        .map(|commit| commit.generation),
                    Some(latest.generation)
                );
                assert_eq!(event_loop.sessions[0].app.root_mode().id_key(), "plan");
                assert!(event_loop.sessions[0].app.root_config().workflow);
                drop(observer);
            })
        });
    }

    fn commit_root_plan_mode(event_loop: &EventLoop<'_>) {
        let runtime = &event_loop.sessions[0];
        let (manager, root) = runtime.handles.manager_and_root();
        dispatch_mode_commit(
            runtime,
            Ok(manager.lookup(root).unwrap()),
            &event_loop.ctx.storage,
            event_loop.ctx.lua_event_handle.mode_registry(),
            None,
            event_loop.internal_tx.clone(),
            "plan".into(),
        );
    }

    #[test]
    fn retired_runtime_completion_and_projection_are_ignored() {
        with_event_loop(|event_loop| {
            smol::block_on(async {
                let old = &event_loop.sessions[0];
                let (manager, root) = old.handles.manager_and_root();
                let commit =
                    reserve_config_change(&manager.lookup(root).unwrap(), &old.coordinator, None)
                        .unwrap()
                        .commit_change(ConfigChange::Patch(ConfigPatch {
                            model: Some(maki_agent::actor::PreparedModel {
                                model: model_named("retired-model"),
                                provider: Arc::new(StubProvider),
                            }),
                            workflow: Some(true),
                            ..Default::default()
                        }))
                        .await
                        .unwrap();
                let session = old.id();
                commit_root_plan_mode(event_loop);
                let completion = event_loop.internal_rx.recv_async().await.unwrap();
                let mut replacement = test_runtime(model_named("replacement-model"));
                let replacement_thinking = replacement.app.root_config().thinking;
                let (replacement_manager, replacement_root) =
                    replacement.handles.manager_and_root();
                let replacement_actor = replacement_manager.actor(replacement_root).unwrap();
                let initial_config = replacement_actor.config_snapshot().unwrap();
                Arc::make_mut(&mut replacement.app.state.session).id = session;
                let (queued_tx, queued_rx) = flume::unbounded();
                replacement.config_commits = queued_rx;
                let old = std::mem::replace(&mut event_loop.sessions[0], replacement);
                event_loop.handle_internal(completion);
                queued_tx.send(commit).unwrap();
                let _ = event_loop.tick();
                let app = &event_loop.sessions[0].app;
                assert_eq!(
                    app.state.session.model,
                    model_named("replacement-model").spec()
                );
                assert_eq!(app.root_mode().id_key(), "build");
                assert_eq!(app.root_config().thinking, replacement_thinking);
                assert!(!app.root_config().workflow);
                assert_eq!(
                    event_loop.sessions[0]
                        .projected_config
                        .as_ref()
                        .map(|commit| commit.generation),
                    None
                );
                let final_config = replacement_actor.config_snapshot().unwrap();
                assert_eq!(final_config.generation, initial_config.generation);
                assert!(Arc::ptr_eq(
                    &final_config.identity,
                    &initial_config.identity
                ));
                assert!(Arc::ptr_eq(&final_config.config, &initial_config.config));
                release_runtime(old);
            })
        });
    }

    #[test]
    fn lua_model_option_updates_actor_policy() {
        smol::block_on(async {
            let runtime = test_runtime(model_named("test-model"));
            let (manager, root) = runtime.handles.manager_and_root();
            let actor = manager.actor(root).unwrap();
            let reservation = actor.reserve_prepared_operation(None).unwrap();
            let coordinator = runtime.coordinator.clone();
            let target_model = model_named("test-model");
            let version = coordinator.read().options().version;
            let spec = target_model.spec();
            let setter = smol::spawn(async move {
                coordinator
                    .set_option_if_version("model", spec, Some(version))
                    .await
            });
            reservation
                .commit_when_ready(Some(ConfigChange::Patch(ConfigPatch {
                    model: Some(maki_agent::actor::PreparedModel {
                        provider: Arc::new(StubProvider),
                        model: target_model.clone(),
                    }),
                    ..Default::default()
                })))
                .await
                .unwrap();
            setter.await.unwrap();
            runtime.config_commits.recv_async().await.unwrap();
            assert_eq!(
                actor.effective_config().unwrap().model.spec(),
                target_model.spec()
            );
            assert_eq!(
                runtime.coordinator.read().model().unwrap().as_ref(),
                target_model.spec()
            );
            release_runtime(runtime);
        });
    }

    #[test]
    fn reserved_option_change_precedes_root_admission() {
        smol::block_on(async {
            let runtime = test_runtime(model_named("test-model"));
            let (manager, root) = runtime.handles.manager_and_root();
            let actor = manager.actor(root).unwrap();
            let lease = actor.reserve_prepared_operation(None).unwrap();
            let toggle = spawn_root_workflow_toggle(&runtime);
            runtime.handles.queue.push(QueueItem {
                input: maki_agent::AgentInput {
                    message: "queued root".into(),
                    images: Vec::new(),
                    preamble: Vec::new(),
                    prompt: None,
                },
                displayed: None,
            });
            assert!(!runtime.handles.queue.is_empty());
            drop(lease);
            toggle.await.unwrap();
            assert!(
                runtime
                    .handles
                    .manager_and_root()
                    .0
                    .effective_config(root)
                    .unwrap()
                    .unwrap()
                    .workflow
            );
            wait_projected_option(&runtime, WORKFLOW_OPTION_ID, ENABLED_VALUE).await;
            assert!(
                runtime
                    .coordinator
                    .read()
                    .options()
                    .options
                    .iter()
                    .any(|option| {
                        option.definition.id.as_ref() == WORKFLOW_OPTION_ID
                            && option.current_value.as_ref() == ENABLED_VALUE
                    })
            );
            release_runtime(runtime);
        });
    }

    #[test]
    fn toggle_behind_a_held_lease_waits_and_keeps_both_intents() {
        smol::block_on(async {
            let runtime = test_runtime(model_named("test-model"));
            let id = runtime.id();
            let coordinator = runtime.coordinator.clone();
            let (internal_tx, internal_rx) = flume::unbounded();

            let reservation = coordinator
                .reserve_transaction(CheckpointPublication::default())
                .unwrap();
            dispatch_option_toggle(&runtime, YOLO_OPTION_ID, &internal_tx);
            drop(reservation);

            let mut committed = Vec::new();
            for index in 0..2 {
                if index > 0 {
                    dispatch_option_toggle(&runtime, YOLO_OPTION_ID, &internal_tx);
                }
                let InternalEvent::SessionOp {
                    session,
                    kind:
                        SessionOpKind::OptionToggled {
                            committed: value, ..
                        },
                    result,
                    ..
                } = internal_rx.recv_async().await.unwrap()
                else {
                    panic!("expected option toggle completion");
                };
                assert_eq!(session, id);
                result.unwrap();
                committed.push(value.lock().unwrap().take().unwrap());
            }
            assert_eq!(committed, [true, false]);
            let yolo = coordinator
                .read()
                .options()
                .options
                .iter()
                .find(|option| {
                    option.definition.id.as_ref() == maki_agent::session_options::YOLO_OPTION_ID
                })
                .unwrap()
                .current_value
                .clone();
            assert_eq!(yolo.as_ref(), maki_agent::session_options::DISABLED_VALUE);
            release_runtime(runtime);
        });
    }

    #[test_case(STARTUP_MODEL_ID, STARTUP_MODEL_ID, true ; "startup_spec")]
    #[test_case(RESUMED_MODEL_ID, RESUMED_MODEL_ID, true ; "resumed_spec")]
    #[test_case(STARTUP_MODEL_ID, UNRELATED_MODEL_ID, false ; "unrelated_spec_is_unchanged")]
    fn models_discovered_refreshes_live_context_window(
        session_model: &str,
        discovered: &str,
        refreshed: bool,
    ) {
        let spec = |id: &str| format!("{DISCOVERY_PROVIDER}/{id}");
        with_event_loop_runtime(
            |harness| {
                let startup = Model::from_spec(&spec(STARTUP_MODEL_ID)).unwrap();
                harness.ctx.as_mut().unwrap().model_slot =
                    ProviderSlot::new(startup, Arc::new(StubProvider)).0;
                let resumed = (session_model != STARTUP_MODEL_ID).then(|| PreparedProvider {
                    model: Model::from_spec(&spec(session_model)).unwrap(),
                    provider: Arc::new(StubProvider),
                });
                harness
                    .ctx()
                    .spawn_runtime_with_provider(harness.session(), resumed)
                    .unwrap()
            },
            |event_loop| {
                let runtime = &event_loop.sessions[0];
                let before = runtime.config().unwrap();
                assert_eq!(before.model.spec(), spec(session_model));
                assert_ne!(before.model.context_window, DISCOVERED_WINDOW);
                let identity = runtime.model_slot.load().provider.identity();
                maki_providers::model_registry::set_known_models(
                    DISCOVERY_PROVIDER,
                    vec![maki_providers::ModelInfo {
                        context_window: Some(DISCOVERED_WINDOW),
                        ..maki_providers::ModelInfo::id_only(discovered.into())
                    }],
                );

                event_loop.sync_orchestration();
                smol::block_on(event_loop.ctx.catalog.apply(Vec::new()));
                event_loop.handle_internal(InternalEvent::ModelsDiscovered);

                let runtime = &event_loop.sessions[0];
                if refreshed {
                    let commit = smol::block_on(runtime.config_commits.recv_async()).unwrap();
                    assert_eq!(commit.config.model.context_window, DISCOVERED_WINDOW);
                }
                let window = |model: &Model| model.context_window == DISCOVERED_WINDOW;
                assert_eq!(window(&runtime.config().unwrap().model), refreshed);
                let slot = runtime.model_slot.load();
                assert_eq!(window(&slot.model), refreshed);
                assert_eq!(slot.provider.identity(), identity);
                assert_eq!(window(&event_loop.ctx.model_slot.load().model), refreshed);
            },
        );
    }

    #[test_case(true ; "idle_guard")]
    #[test_case(false ; "session_lease")]
    fn busy_discovery_refresh_applies_once_released(idle_guard: bool) {
        let spec = format!("{DISCOVERY_PROVIDER}/{BUSY_MODEL_ID}");
        with_event_loop_runtime(
            |harness| {
                let startup = Model::from_spec(&spec).unwrap();
                harness.ctx.as_mut().unwrap().model_slot =
                    ProviderSlot::new(startup, Arc::new(StubProvider)).0;
                harness
                    .ctx()
                    .spawn_runtime_with_provider(harness.session(), None)
                    .unwrap()
            },
            |event_loop| {
                event_loop.sync_orchestration();
                maki_providers::model_registry::set_known_models(
                    DISCOVERY_PROVIDER,
                    vec![maki_providers::ModelInfo {
                        context_window: Some(DISCOVERED_WINDOW),
                        ..maki_providers::ModelInfo::id_only(BUSY_MODEL_ID.into())
                    }],
                );
                let runtime = &event_loop.sessions[0];
                let (manager, root) = runtime.handles.manager_and_root();
                let guard =
                    idle_guard.then(|| manager.actor(root).unwrap().prepare_idle().unwrap());
                let lease = (!idle_guard).then(|| {
                    runtime
                        .coordinator
                        .reserve_transaction(CheckpointPublication::default())
                        .unwrap()
                });
                let mut refresh = Box::pin(event_loop.ctx.catalog.apply(Vec::new()));

                assert!(smol::block_on(futures_lite::future::poll_once(&mut refresh)).is_none());
                drop((guard, lease));
                smol::block_on(refresh);

                let window = runtime.config().unwrap().model.context_window;
                assert_eq!(window, DISCOVERED_WINDOW);
            },
        );
    }

    fn model_named(id: &str) -> Model {
        let mut model = crate::components::test_model();
        model.id = id.into();
        model
    }

    /// A runtime whose app and provider slot both start on `model`.
    fn test_runtime(model: Model) -> SessionRuntime {
        let (model_slot, _change_rx) = ProviderSlot::new(model.clone(), Arc::new(StubProvider));
        let permissions = Arc::new(PermissionManager::new(
            maki_config::PermissionsConfig::default(),
            PathBuf::from("/tmp"),
            ProjectConfig::for_project(Path::new("/tmp")),
            Arc::default(),
        ));
        let mut app = crate::app::tests::test_app();
        app.update_model(&model);
        let handles = AgentHandles::spawn(
            &model_slot,
            Vec::new(),
            AgentConfig::default(),
            maki_agent::ToolOutputLines::default(),
            &permissions,
            PathBuf::from("/tmp"),
            None,
            Timeouts::default(),
            EventHandle::disconnected_for_test(),
            None,
            Arc::new(ModelPolicy::default()),
            SystemPromptOverride::default(),
        );
        handles.apply_to_app(&mut app);
        let coordinator = test_coordinator(app.state.session.id);
        let (shell_tx, shell_rx) = flume::unbounded();
        let config_commits = observe_config_commits(
            &handles,
            Arc::clone(&model_slot),
            coordinator.clone(),
            Arc::new(|model, _| {
                Ok(maki_agent::PreparedModel {
                    model,
                    provider: Arc::new(StubProvider),
                })
            }),
        )
        .unwrap();
        SessionRuntime {
            generation: NEXT_RUNTIME_GENERATION.fetch_add(1, Ordering::Relaxed),
            config_commits,
            projected_config: None,
            app,
            handles,
            model_slot,
            _coordinator_retirement: CoordinatorRetirement(coordinator.clone()),
            coordinator,
            shell_tx,
            shell_rx,
            last_status: SessionStatus::Idle,
            notifications: RunNotificationState::default(),
            session_lock: None,
            lock_lost: false,
            restore_pending: false,
            pending_approval: None,
        }
    }

    fn test_coordinator(session_id: MakiId) -> SessionCoordinatorHandle {
        PreparedSessionCoordinator::new(SessionCoordinatorParams {
            session_id,
            catalog: Default::default(),
            definitions: builtin_option_definitions(
                "anthropic/test-model",
                [Arc::from("anthropic/test-model")],
                false,
                false,
                false,
                maki_agent::ThinkingConfig::Off,
            ),
            persisted_options: Default::default(),
            model_policy: Arc::default(),
            checkpoint: ClaimingWriter::accept_all(),
            mailbox: maki_agent::SessionMailbox::new(session_id),
        })
        .expect("coordinator preparation")
        .activate()
        .expect("coordinator activation")
    }

    struct StubProvider;

    impl Provider for StubProvider {
        fn stream_message<'a>(
            &'a self,
            _model: &'a Model,
            _messages: &'a [Message],
            _system: &'a str,
            _tools: &'a serde_json::Value,
            _event_tx: &'a flume::Sender<maki_providers::ProviderEvent>,
            _opts: maki_providers::RequestOptions,
            _session_id: Option<&'a SessionRef>,
        ) -> maki_providers::provider::BoxFuture<
            'a,
            Result<maki_providers::StreamResponse, maki_providers::AgentError>,
        > {
            Box::pin(std::future::pending())
        }

        fn list_models(
            &self,
        ) -> maki_providers::provider::BoxFuture<
            '_,
            Result<Vec<maki_providers::ModelInfo>, maki_providers::AgentError>,
        > {
            Box::pin(async { Ok(Vec::new()) })
        }
    }

    struct FocusedProvider;

    impl Provider for FocusedProvider {
        fn stream_message<'a>(
            &'a self,
            _model: &'a Model,
            _messages: &'a [Message],
            _system: &'a str,
            _tools: &'a serde_json::Value,
            _event_tx: &'a flume::Sender<maki_providers::ProviderEvent>,
            _opts: maki_providers::RequestOptions,
            _session_id: Option<&'a SessionRef>,
        ) -> maki_providers::provider::BoxFuture<
            'a,
            Result<maki_providers::StreamResponse, maki_providers::AgentError>,
        > {
            Box::pin(std::future::pending())
        }

        fn list_models(
            &self,
        ) -> maki_providers::provider::BoxFuture<
            '_,
            Result<Vec<maki_providers::ModelInfo>, maki_providers::AgentError>,
        > {
            Box::pin(async { Ok(vec![maki_providers::ModelInfo::id_only("focused".into())]) })
        }
    }

    const SHELL_RESULT: &str = "command finished";
    const RUNTIME_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(1);
    const TEST_DISCOVERY_TIMEOUT: Duration = Duration::from_secs(5);

    pub(super) fn with_event_loop(test: impl FnOnce(&mut EventLoop<'_>)) {
        with_event_loop_runtime(|harness| harness.runtime(harness.session()), test);
    }

    fn with_event_loop_runtime(
        runtime: impl FnOnce(&mut RuntimeHarness) -> SessionRuntime,
        test: impl FnOnce(&mut EventLoop<'_>),
    ) {
        let mut harness = RuntimeHarness::new();
        let runtime = runtime(&mut harness);
        let ctx = harness.ctx.take().unwrap();
        let initial_provider = ctx.model_slot.load().provider.identity();
        let backend = ratatui::backend::CrosstermBackend::new(std::io::stdout());
        let mut terminal = ratatui::Terminal::with_options(
            backend,
            ratatui::TerminalOptions {
                viewport: ratatui::Viewport::Fixed(ratatui::layout::Rect::new(0, 0, 80, 24)),
            },
        )
        .unwrap();
        let (internal_tx, internal_rx) = flume::unbounded();
        let (warn_tx, warn_rx) = flume::unbounded();
        let mut event_loop = EventLoop {
            terminal: &mut terminal,
            sessions: vec![runtime],
            focused: 0,
            session_picker: false,
            last_focused: None,
            terminal_focused: true,
            notifier: None,
            sessions_dir: ctx.sessions_dir.clone(),
            session_cwd: harness._temp_dir.path().to_string_lossy().into_owned(),
            orchestration: {
                let orchestration = Arc::new(AgentOrchestration::new(ctx.catalog.specs()));
                ctx.catalog.attach(&orchestration);
                orchestration
            },
            orchestration_revisions: Vec::new(),
            orchestration_focus: None,
            ctx,
            last_heartbeat: Instant::now(),
            input: InputReader::disconnected(),
            warn_rx,
            warn_tx,
            ui_action_rx: flume::unbounded().1,
            command_rx: flume::unbounded().1,
            provider_change_rx: flume::unbounded().1,
            provider_usage: ProviderUsageCoordinator::new(initial_provider),
            next_status_invalidation: 0,
            pending_status_invalidation: None,
            internal_tx,
            internal_rx,
            model_discovery: Discovery::idle(),
        };
        test(&mut event_loop);
        smol::block_on(
            event_loop
                .model_discovery
                .wait_active(TEST_DISCOVERY_TIMEOUT),
        );
        drop(event_loop.model_discovery);
        event_loop
            .ctx
            .storage_writer
            .shutdown(RUNTIME_SHUTDOWN_TIMEOUT);
        for runtime in event_loop.sessions.drain(..) {
            release_runtime(runtime);
        }
        harness.ctx = Some(event_loop.ctx);
    }

    pub(super) fn select_child_chat(event_loop: &mut EventLoop<'_>, child: AgentId, name: &str) {
        let (manager, root) = event_loop.sessions[0].handles.manager_and_root();
        let target = manager.lookup(child).unwrap();
        let history_key = event_loop.orchestration.history_key(&target).unwrap();
        let actor = manager.actor(child).unwrap();
        let config = actor.effective_config().unwrap();
        event_loop.handle_agent(
            0,
            Box::new(Envelope {
                event: AgentEvent::SubagentHistory {
                    tool_use_id: history_key.clone(),
                    history_epoch: 0,
                    messages: Vec::new(),
                },
                subagent: Some(SubagentInfo {
                    agent_id: child,
                    parent_agent_id: Some(root),
                    parent_tool_use_id: history_key,
                    name: name.into(),
                    prompt: None,
                    model: Some(config.model.spec()),
                    opts: None,
                    answer_tx: None,
                    cancel: None,
                }),
                identity: None,
            }),
        );
        let app = &mut event_loop.sessions[0].app;
        app.active_chat = app.live_chat_index[&child];
        app.sync_live_children();
        event_loop
            .ctx
            .storage_writer
            .send(Arc::clone(&app.state.session));
        event_loop.sync_orchestration();
    }

    #[test_case(false; "bare_model")]
    #[test_case(true; "explicit_model")]
    fn initial_cli_model_prompt_syncs_admission_before_submission(explicit_model: bool) {
        const MODEL: &str = "anthropic/claude-sonnet-4-6";

        with_event_loop(|event_loop| {
            let command_runtime = Arc::clone(&event_loop.ctx.command_runtime);
            let app = &mut event_loop.sessions[0].app;
            app.command_runtime = Arc::clone(&command_runtime);
            app.command_target = command_runtime.bind_target();
            let target = app.command_target.clone();
            assert!(command_runtime.target_agent(&target).is_none());
            assert!(event_loop.orchestration_revisions.is_empty());
            event_loop.ctx.prepare_provider = Arc::new(|model, _| {
                Ok(maki_agent::actor::PreparedModel {
                    model,
                    provider: Arc::new(StubProvider),
                })
            });
            let initial_prompt = if explicit_model {
                format!("/model {MODEL}")
            } else {
                "/model".into()
            };

            event_loop.start(Some(initial_prompt));

            let command = command_runtime
                .recv_timeout_for_test(RUNTIME_SHUTDOWN_TIMEOUT)
                .unwrap();
            let CommandEvent::Host {
                target: captured_target,
                admission: Some(admission),
                ..
            } = &command
            else {
                panic!("initial model prompt must capture admission before dispatch");
            };
            let (manager, root) = event_loop.sessions[0].handles.manager_and_root();
            assert_eq!(*captured_target, target.id());
            assert_eq!(admission.target.id(), root);
            assert!(
                admission
                    .target
                    .same_manager(&manager.lookup(root).unwrap())
            );
            event_loop.handle_command(command);
            assert!(matches!(
                command_runtime
                    .recv_timeout_for_test(RUNTIME_SHUTDOWN_TIMEOUT)
                    .unwrap(),
                CommandEvent::Outcome {
                    outcome: maki_commands::CommandOutcome::Completed,
                    ..
                }
            ));
            if explicit_model {
                assert_eq!(
                    manager
                        .actor(root)
                        .unwrap()
                        .effective_config()
                        .unwrap()
                        .model
                        .spec(),
                    MODEL
                );
            } else {
                let app = &event_loop.sessions[0].app;
                assert!(app.model_picker.is_open());
                assert_eq!(app.model_picker_target.as_ref().unwrap().id(), root);
            }
        });
    }

    struct RuntimeHarness {
        _temp_dir: TempDir,
        ctx: Option<SpawnCtx>,
    }

    impl RuntimeHarness {
        fn with_permissions_config(permissions_config: PermissionsConfig) -> Self {
            let temp_dir = tempfile::tempdir().unwrap();
            let storage = StateDir::from_path(temp_dir.path().to_path_buf());
            let sessions_dir = storage.ensure_subdir(SESSIONS_DIR).unwrap();
            let catalog = ModelCatalog::default();
            let command_registry = maki_commands::CommandRegistry::new();
            let command_runtime = Arc::new(CommandRuntime::new_for_test(
                &[],
                command_registry,
                Arc::new(ModelArgSource::new(catalog.specs())),
                Arc::new(ThemeArgSource::new(
                    crate::theme::default_provider().clone(),
                )),
            ));
            let model = crate::components::test_model();
            let (model_slot, _provider_change_rx) =
                ProviderSlot::new(model, Arc::new(StubProvider));
            let permissions = Arc::new(PermissionManager::new(
                permissions_config,
                temp_dir.path().to_path_buf(),
                ProjectConfig::for_project(temp_dir.path()),
                Arc::default(),
            ));
            let storage_writer =
                Arc::new(StorageWriter::new(storage.clone(), flume::unbounded().0));
            let ctx = SpawnCtx {
                prepare_provider: Arc::new(prepare_provider),
                storage,
                sessions_dir,
                config: AgentConfig::default(),
                ui_config: UiConfig::default(),
                input_history_size: 100,
                permissions,
                timeouts: Timeouts::default(),
                keymap_reader: KeymapReader::empty(),
                hint_reader: HintReader::empty(),
                status_content_reader: StatusContentReader::empty(),
                lua_event_handle: EventHandle::disconnected_for_test(),
                mcp_handle: None,
                mcp_config_errors: McpConfigErrors::new(temp_dir.path().to_path_buf()),
                model_slot,
                catalog,
                storage_writer,
                model_policy: Arc::new(ModelPolicy::default()),
                session_defaults: SessionDefaults::default(),
                system_prompt: SystemPromptOverride::default(),
                command_runtime,
                trust_question: None,
            };
            Self {
                _temp_dir: temp_dir,
                ctx: Some(ctx),
            }
        }

        fn new() -> Self {
            Self::with_permissions_config(PermissionsConfig::default())
        }

        fn ctx(&self) -> &SpawnCtx {
            self.ctx.as_ref().unwrap()
        }

        fn session(&self) -> AppSession {
            let cwd = self._temp_dir.path().to_string_lossy();
            AppSession::new("test-model", cwd.as_ref())
        }

        fn prepare(&self) -> PreparedSessionRuntime {
            self.ctx().prepare_runtime(self.session()).unwrap()
        }

        fn set_startup_yolo(&self) {
            self.ctx().permissions.set_yolo(true);
        }

        fn runtime(&self, session: AppSession) -> SessionRuntime {
            self.ctx().spawn_runtime(session).unwrap()
        }

        fn target_count(&self) -> usize {
            self.ctx().command_runtime.registry.target_count()
        }

        fn shutdown_writer(&mut self) -> StateDir {
            let ctx = self.ctx.take().unwrap();
            let storage = ctx.storage.clone();
            let writer = Arc::clone(&ctx.storage_writer);
            drop(ctx);
            writer.shutdown(RUNTIME_SHUTDOWN_TIMEOUT);
            storage
        }
    }

    impl Drop for RuntimeHarness {
        fn drop(&mut self) {
            let Some(ctx) = self.ctx.take() else {
                return;
            };
            let storage_writer = Arc::clone(&ctx.storage_writer);
            drop(ctx);
            storage_writer.shutdown(RUNTIME_SHUTDOWN_TIMEOUT);
        }
    }

    pub(super) fn shutdown_manager(manager: &maki_agent::AgentManagerHandle) {
        let report = smol::block_on(manager.shutdown(RUNTIME_SHUTDOWN_TIMEOUT));
        assert!(report.timed_out.is_empty());
    }

    fn release_runtime(runtime: SessionRuntime) {
        let SessionRuntime {
            handles,
            session_lock,
            ..
        } = runtime;
        release_lock_state(session_lock).unwrap();
        let manager = handles.manager_and_root().0;
        drop(handles);
        shutdown_manager(&manager);
    }

    #[test]
    fn offload_uses_supplied_storage_root_and_session_deletion() {
        const BODY: &str = "saved in the supplied state directory";
        let mut harness = RuntimeHarness::new();
        let mut session = harness.session();
        session.push_message(Message::user(BODY.into()));
        let id = session.id;
        let mut runtime = harness.runtime(session);
        checkpoint_runtime(&mut runtime);
        let store = Arc::clone(runtime.handles.offload.as_ref().unwrap());
        let expected = maki_storage::sessions::offload_dir(&harness.ctx().sessions_dir, id);
        assert_eq!(store.dir(), expected);
        let saved = store.put(BODY).unwrap();
        let artifact = store.path_of(&saved);
        assert_eq!(artifact.parent(), Some(expected.as_path()));
        assert_eq!(std::fs::read_to_string(&artifact).unwrap(), BODY);
        release_runtime(runtime);
        let storage = harness.shutdown_writer();
        AppSession::load(id, &storage).unwrap();
        AppSession::delete(id, &storage).unwrap();
        assert!(!artifact.exists());
        assert!(!expected.exists());
    }

    struct GatedDiskBackend {
        dir: PathBuf,
        entered: flume::Sender<()>,
        release: flume::Receiver<()>,
    }

    impl OffloadBackend for GatedDiskBackend {
        fn create_new(&self, name: &str, bytes: &[u8]) -> io::Result<bool> {
            self.entered.send(()).unwrap();
            self.release.recv().unwrap();
            std::fs::create_dir_all(&self.dir)?;
            std::fs::write(self.path(name), bytes)?;
            Ok(true)
        }

        fn total_bytes(&self) -> io::Result<u64> {
            Ok(0)
        }

        fn remove_all(&self) -> io::Result<()> {
            match std::fs::remove_dir_all(&self.dir) {
                Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
                result => result,
            }
        }

        fn path(&self, name: &str) -> PathBuf {
            self.dir.join(name)
        }
    }

    #[test_case(false; "deletion_completes")]
    #[test_case(true; "ownership_loss_preserves_transcript_and_artifacts")]
    fn background_deletion_drains_in_flight_offload_persistence(lose_ownership: bool) {
        const BODY: &str = "pending background session output";
        const EXISTING_BODY: &str = "referenced output retained if deletion is rejected";
        const ARTIFACT_NAME: &str = "existing-output.txt";
        with_event_loop(|event_loop| {
            let mut session = AppSession::new("test-model", &event_loop.session_cwd);
            let id = session.id;
            let dir = maki_storage::sessions::offload_dir(&event_loop.ctx.sessions_dir, id);
            let artifact = dir.join(ARTIFACT_NAME);
            std::fs::create_dir_all(&dir).unwrap();
            std::fs::write(&artifact, EXISTING_BODY).unwrap();
            session.push_message(Message::observation(artifact.display().to_string()));
            session.save(&event_loop.ctx.storage).unwrap();
            let (entered_tx, entered_rx) = flume::unbounded();
            let (release_tx, release_rx) = flume::unbounded();
            let store = Arc::new(OffloadStore::new(Box::new(GatedDiskBackend {
                dir: dir.clone(),
                entered: entered_tx,
                release: release_rx,
            })));
            let prepared = event_loop
                .ctx
                .prepare_runtime_with_config(
                    session,
                    None,
                    &event_loop.ctx.permissions,
                    true,
                    None,
                    Some(Arc::clone(&store)),
                )
                .unwrap();
            let lock = claim_lock(&event_loop.ctx.sessions_dir, &id).unwrap();
            prepared.seed_storage();
            let runtime = prepared
                .activate(
                    &event_loop.ctx.model_slot,
                    Some(SessionLockState::Held(lock)),
                )
                .unwrap();
            event_loop.push_runtime(runtime);
            let worker = std::thread::spawn({
                let store = Arc::clone(&store);
                move || store.put(BODY)
            });
            entered_rx.recv_timeout(RUNTIME_SHUTDOWN_TIMEOUT).unwrap();
            let (reply_tx, reply_rx) = flume::unbounded();
            event_loop
                .handle_session_request(SessionRequest::Delete { id: id.to_string() }, reply_tx);
            assert!(event_loop.position(id).is_none());
            assert!(matches!(store.put(BODY), Err(OffloadError::Closed)));
            assert!(reply_rx.is_empty());
            assert!(AppSession::load(id, &event_loop.ctx.storage).is_ok());
            assert!(
                session_lock::claim(&event_loop.ctx.sessions_dir, &id)
                    .unwrap()
                    .is_none()
            );
            assert_eq!(std::fs::read_to_string(&artifact).unwrap(), EXISTING_BODY);
            let replacement = if lose_ownership {
                std::fs::remove_file(session_lock::lock_path(&event_loop.ctx.sessions_dir, &id))
                    .unwrap();
                Some(claim_lock(&event_loop.ctx.sessions_dir, &id).unwrap())
            } else {
                None
            };
            release_tx.send(()).unwrap();
            let saved = worker.join().unwrap().unwrap();
            let reply = reply_rx.recv_timeout(RUNTIME_SHUTDOWN_TIMEOUT).unwrap();
            if lose_ownership {
                assert!(reply.is_err());
                let stored = AppSession::load(id, &event_loop.ctx.storage).unwrap();
                assert_eq!(
                    stored.messages()[0].user_text(),
                    Some(artifact.display().to_string().as_str())
                );
                assert_eq!(std::fs::read_to_string(&artifact).unwrap(), EXISTING_BODY);
                assert_eq!(
                    std::fs::read_to_string(store.path_of(&saved)).unwrap(),
                    BODY
                );
            } else {
                assert_eq!(reply, Ok(json!(true)));
                assert!(AppSession::load(id, &event_loop.ctx.storage).is_err());
                assert!(!dir.exists());
            }
            assert!(matches!(store.put(BODY), Err(OffloadError::Closed)));
            if let Some(replacement) = replacement {
                replacement.release().unwrap();
            }
        });
    }

    #[test]
    fn background_deletion_lease_failure_cancels_and_drains_active_turn() {
        use maki_agent::actor::ActorLifecycle;

        const PROMPT: &str = "hold the background turn";
        const BODY: &str = "retained output after lease failure";

        struct TurnDrop(Arc<AtomicBool>);

        impl Drop for TurnDrop {
            fn drop(&mut self) {
                self.0.store(true, Ordering::Release);
            }
        }

        struct GatedProvider {
            entered: flume::Sender<()>,
            release: flume::Receiver<()>,
            dropped: Arc<AtomicBool>,
        }

        impl Provider for GatedProvider {
            fn stream_message<'a>(
                &'a self,
                _model: &'a Model,
                _messages: &'a [Message],
                _system: &'a str,
                _tools: &'a serde_json::Value,
                _event_tx: &'a flume::Sender<maki_providers::ProviderEvent>,
                _opts: maki_providers::RequestOptions,
                _session_id: Option<&'a SessionRef>,
            ) -> maki_providers::provider::BoxFuture<
                'a,
                Result<maki_providers::StreamResponse, maki_providers::AgentError>,
            > {
                Box::pin(async move {
                    let _turn_drop = TurnDrop(Arc::clone(&self.dropped));
                    self.entered.send(()).unwrap();
                    self.release.recv_async().await.unwrap();
                    std::future::pending().await
                })
            }

            fn list_models(
                &self,
            ) -> maki_providers::provider::BoxFuture<
                '_,
                Result<Vec<maki_providers::ModelInfo>, maki_providers::AgentError>,
            > {
                Box::pin(async { Ok(Vec::new()) })
            }
        }

        with_event_loop(|event_loop| {
            let mut session = AppSession::new("test-model", &event_loop.session_cwd);
            let id = session.id;
            session.save(&event_loop.ctx.storage).unwrap();
            let (entered_tx, entered_rx) = flume::bounded(1);
            let (_release_tx, release_rx) = flume::bounded(1);
            let dropped = Arc::new(AtomicBool::new(false));
            let mut runtime = event_loop
                .ctx
                .spawn_runtime_with_provider(
                    session,
                    Some(PreparedProvider {
                        model: model_named("test-model"),
                        provider: Arc::new(GatedProvider {
                            entered: entered_tx,
                            release: release_rx,
                            dropped: Arc::clone(&dropped),
                        }),
                    }),
                )
                .unwrap();
            let store = Arc::clone(runtime.handles.offload.as_ref().unwrap());
            let saved = store.put(BODY).unwrap();
            let artifact = store.path_of(&saved);
            let (manager, root) = runtime.handles.manager_and_root();
            let actor = manager.actor(root).unwrap();
            let Some(SessionLockState::Held(lease)) = runtime.session_lock.take() else {
                panic!("expected held session lock");
            };
            let (completion_tx, completion_rx) = flume::bounded(1);
            assert!(
                completion_tx
                    .send(HeartbeatCompletion {
                        result: Ok(session_lock::LockBeat::Lost),
                        lease: Some(lease),
                    })
                    .is_ok()
            );
            runtime.session_lock = Some(SessionLockState::InFlight(completion_rx));
            let index = event_loop.push_runtime(runtime);
            event_loop.submit_text(index, PROMPT.into()).unwrap();
            entered_rx.recv_timeout(RUNTIME_SHUTDOWN_TIMEOUT).unwrap();
            assert!(actor.snapshot().active_turn.is_some());
            assert!(!dropped.load(Ordering::Acquire));

            let (reply_tx, reply_rx) = flume::bounded(1);
            event_loop
                .handle_session_request(SessionRequest::Delete { id: id.to_string() }, reply_tx);
            assert!(event_loop.position(id).is_none());
            assert_eq!(
                reply_rx.recv_timeout(RUNTIME_SHUTDOWN_TIMEOUT).unwrap(),
                Err(LOCK_LOST_MSG.into())
            );
            assert!(dropped.load(Ordering::Acquire));
            let snapshot = actor.snapshot();
            assert_eq!(snapshot.lifecycle, ActorLifecycle::Shutdown);
            assert!(snapshot.active_turn.is_none());
            assert!(matches!(
                snapshot.latest,
                Some(TurnOutcome::Cancelled { .. })
            ));
            assert!(
                matches!(manager.node(root), Err(maki_agent::ManagerError::UnknownAgent(id)) if id == root)
            );
            assert!(manager.snapshot().is_empty());
            assert!(AppSession::load(id, &event_loop.ctx.storage).is_ok());
            assert_eq!(std::fs::read_to_string(&artifact).unwrap(), BODY);
            assert!(matches!(store.put(BODY), Err(OffloadError::Closed)));
        });
    }

    struct GatedQuotaBackend {
        entered: flume::Sender<()>,
        release: flume::Receiver<()>,
        created: Arc<AtomicBool>,
    }

    impl OffloadBackend for GatedQuotaBackend {
        fn create_new(&self, _name: &str, _bytes: &[u8]) -> io::Result<bool> {
            self.entered.send(()).unwrap();
            self.release.recv().unwrap();
            self.created.store(true, Ordering::SeqCst);
            Ok(true)
        }

        fn total_bytes(&self) -> io::Result<u64> {
            Ok(MAX_OFFLOAD_SESSION_BYTES - u64::from(!self.created.load(Ordering::SeqCst)))
        }

        fn remove_all(&self) -> io::Result<()> {
            Ok(())
        }

        fn path(&self, name: &str) -> PathBuf {
            PathBuf::from(name)
        }
    }

    #[test]
    fn same_id_replacement_serializes_in_flight_offload_put() {
        const OUTGOING: &str = "a";
        const INCOMING: &str = "b";
        let harness = RuntimeHarness::new();
        let mut session = harness.session();
        let (entered_tx, entered_rx) = flume::unbounded();
        let (release_tx, release_rx) = flume::unbounded();
        let created = Arc::new(AtomicBool::new(false));
        let store = Arc::new(OffloadStore::new(Box::new(GatedQuotaBackend {
            entered: entered_tx,
            release: release_rx,
            created: Arc::clone(&created),
        })));
        let prepared = harness
            .ctx()
            .prepare_runtime_with_config(
                session.clone(),
                None,
                &harness.ctx().permissions,
                true,
                None,
                Some(Arc::clone(&store)),
            )
            .unwrap();
        let lock = claim_lock(&harness.ctx().sessions_dir, &session.id).unwrap();
        prepared.seed_storage();
        let mut runtime = prepared
            .activate(
                &harness.ctx().model_slot,
                Some(SessionLockState::Held(lock)),
            )
            .unwrap();
        runtime.activate_deferred();
        session.model = runtime.app.root_config().model.spec();
        let outgoing = std::thread::spawn({
            let store = Arc::clone(runtime.handles.offload.as_ref().unwrap());
            move || store.put(OUTGOING)
        });
        entered_rx.recv_timeout(RUNTIME_SHUTDOWN_TIMEOUT).unwrap();
        let replacement = harness
            .ctx()
            .prepare_replacement_runtime(
                session,
                runtime.id(),
                runtime.app.permissions.as_ref(),
                runtime.handles.offload.as_ref(),
            )
            .unwrap();
        let old = replace_session_runtime(
            &mut runtime,
            replacement,
            &harness.ctx().sessions_dir,
            &harness.ctx().model_slot,
        )
        .unwrap();
        assert!(Arc::ptr_eq(
            &store,
            runtime.handles.offload.as_ref().unwrap()
        ));
        let incoming = std::thread::spawn({
            let store = Arc::clone(runtime.handles.offload.as_ref().unwrap());
            move || store.put(INCOMING)
        });
        release_tx.send(()).unwrap();
        outgoing.join().unwrap().unwrap();
        assert!(matches!(incoming.join().unwrap(), Err(OffloadError::Quota)));
        assert!(created.load(Ordering::SeqCst));
        release_runtime(old);
        release_runtime(runtime);
    }

    #[test]
    fn failed_runtime_claim_cannot_rewrite_session() {
        const OWNER_CONTENT: &str = "written by lock owner";
        const LOSER_CONTENT: &str = "stale losing snapshot";

        let mut harness = RuntimeHarness::new();
        let mut owner = harness.session();
        let id = owner.id;
        owner.push_message(Message::user(OWNER_CONTENT.into()));
        owner.save(&harness.ctx().storage).unwrap();
        let lock = session_lock::claim(&harness.ctx().sessions_dir, &id)
            .unwrap()
            .unwrap();
        let mut loser = owner.clone();
        loser.replace_messages(vec![Message::user(LOSER_CONTENT.into())]);

        assert!(harness.ctx().spawn_runtime(loser).is_err());
        let storage = harness.shutdown_writer();
        lock.release().unwrap();

        let stored = AppSession::load(id, &storage).unwrap();
        assert_eq!(stored.messages()[0].user_text(), Some(OWNER_CONTENT));
    }

    #[test]
    fn new_session_inherits_focused_model_and_provider() {
        const FOCUSED_MODEL: &str = "openai/gpt-5";

        let harness = RuntimeHarness::new();
        let focused_model = Model::from_spec(FOCUSED_MODEL).unwrap();
        let (focused_slot, _) = ProviderSlot::new(focused_model, Arc::new(FocusedProvider));

        let (session, provider) = new_session_from_slot(&focused_slot, "/tmp");
        assert_eq!(
            smol::block_on(provider.provider.list_models())
                .unwrap()
                .first()
                .unwrap()
                .id,
            "focused"
        );
        let runtime = harness
            .ctx()
            .spawn_runtime_with_provider(session, Some(provider))
            .unwrap();

        let runtime_slot = runtime.model_slot.load();
        assert_eq!(runtime_slot.model.spec(), FOCUSED_MODEL);
        assert_eq!(
            runtime.coordinator.read().model().unwrap().as_ref(),
            FOCUSED_MODEL
        );
        drop(runtime_slot);
        release_runtime(runtime);
    }

    #[test_case("/model anthropic/claude-opus-4-6 thinking=high fast=false"; "trailing_options")]
    #[test_case("/model anthropic/claude-opus-4-6{thinking=high,fast=false}"; "embedded_options")]
    fn live_session_requests_remain_independent_during_changes_and_restore(command: &str) {
        use maki_agent::actor::PreparedModel;
        use maki_providers::{RequestOptions, Role, StopReason, StreamResponse};

        const FIRST: &str = "anthropic/claude-opus-4-6";
        const SECOND: &str = "openai/gpt-5";
        const THIRD: &str = "anthropic/claude-sonnet-4-6";
        const PROMPT: &str = "request isolation";
        const RESPONSE: &str = "complete";

        struct RecordingProvider {
            name: &'static str,
            requests: flume::Sender<(&'static str, String, RequestOptions)>,
            release: flume::Receiver<()>,
        }

        impl Provider for RecordingProvider {
            fn stream_message<'a>(
                &'a self,
                model: &'a Model,
                _messages: &'a [Message],
                _system: &'a str,
                _tools: &'a serde_json::Value,
                _event_tx: &'a flume::Sender<maki_providers::ProviderEvent>,
                opts: RequestOptions,
                _session_id: Option<&'a SessionRef>,
            ) -> maki_providers::provider::BoxFuture<
                'a,
                Result<StreamResponse, maki_providers::AgentError>,
            > {
                Box::pin(async move {
                    self.requests.send((self.name, model.spec(), opts)).unwrap();
                    self.release.recv_async().await.unwrap();
                    Ok(StreamResponse {
                        message: Message {
                            role: Role::Assistant,
                            content: vec![maki_providers::ContentBlock::Text {
                                text: RESPONSE.into(),
                            }],
                            ..Message::user(String::new())
                        },
                        usage: TokenUsage::default(),
                        stop_reason: Some(StopReason::EndTurn),
                    })
                })
            }

            fn list_models(
                &self,
            ) -> maki_providers::provider::BoxFuture<
                '_,
                Result<Vec<maki_providers::ModelInfo>, maki_providers::AgentError>,
            > {
                Box::pin(async { Ok(Vec::new()) })
            }
        }

        fn model(spec: &str) -> Model {
            let mut model = Model::from_spec(spec).unwrap();
            model.supports_fast_override = Some(FastSupport::Supported);
            model
        }

        fn send(event_loop: &mut EventLoop<'_>, index: usize) {
            event_loop.submit_text(index, PROMPT.into()).unwrap();
        }

        with_event_loop(|event_loop| {
            let (requests_tx, requests_rx) = flume::unbounded();
            let mut releases = Vec::new();
            for spec in [FIRST, SECOND] {
                let (release, gate) = flume::unbounded();
                releases.push(release);
                let mut session = AppSession::new(spec, &event_loop.session_cwd);
                session.meta.thinking = Some(DomainThinkingConfig::Off.into());
                session.meta.fast = spec == FIRST;
                let runtime = event_loop
                    .ctx
                    .spawn_runtime_with_provider(
                        session,
                        Some(PreparedProvider {
                            model: model(spec),
                            provider: Arc::new(RecordingProvider {
                                name: spec,
                                requests: requests_tx.clone(),
                                release: gate,
                            }),
                        }),
                    )
                    .unwrap();
                event_loop.push_runtime(runtime);
            }
            let first = event_loop.sessions[1].handles.manager_and_root();
            let first = first.0.actor(first.1).unwrap();
            let original = first.effective_config().unwrap();
            let expected = |spec: &str, fast| {
                RequestOptions {
                    thinking: DomainThinkingConfig::Off,
                    fast,
                }
                .clamped(&model(spec))
            };
            let changed = |fast| {
                RequestOptions {
                    thinking: DomainThinkingConfig::Effort(maki_providers::Effort::High),
                    fast,
                }
                .clamped(&model(FIRST))
            };
            assert_ne!(changed(false), expected(FIRST, false));
            send(event_loop, 1);
            assert_eq!(
                requests_rx.recv_timeout(RUNTIME_SHUTDOWN_TIMEOUT).unwrap(),
                (FIRST, FIRST.into(), expected(FIRST, true))
            );
            send(event_loop, 2);
            assert_eq!(
                requests_rx.recv_timeout(RUNTIME_SHUTDOWN_TIMEOUT).unwrap(),
                (SECOND, SECOND.into(), expected(SECOND, false))
            );

            let (changed_release, changed_gate) = flume::unbounded();
            let (restored_release, restored_gate) = flume::unbounded();
            event_loop.ctx.prepare_provider = Arc::new(move |requested, _| {
                let spec = requested.spec();
                let (name, release) = match spec.as_str() {
                    FIRST => ("changed", changed_gate.clone()),
                    THIRD => ("restored", restored_gate.clone()),
                    _ => panic!("unexpected provider preparation: {spec}"),
                };
                Ok(PreparedModel {
                    model: model(&spec),
                    provider: Arc::new(RecordingProvider {
                        name,
                        requests: requests_tx.clone(),
                        release,
                    }),
                })
            });
            event_loop.set_focused(2);
            event_loop.sync_orchestration();
            let command_target = event_loop.sessions[2].app.command_target.clone();
            let dispatch = event_loop
                .ctx
                .command_runtime
                .registry
                .dispatch_input(&command_target, command.into());
            let dispatch = smol::spawn(dispatch);
            let command = event_loop
                .ctx
                .command_runtime
                .recv_timeout_for_test(RUNTIME_SHUTDOWN_TIMEOUT)
                .unwrap();
            event_loop.set_focused(1);
            event_loop.handle_command(command);
            smol::block_on(dispatch);
            let _ = event_loop.tick();
            releases[1].send(()).unwrap();
            send(event_loop, 2);
            assert_eq!(
                requests_rx.recv_timeout(RUNTIME_SHUTDOWN_TIMEOUT).unwrap(),
                ("changed", FIRST.into(), changed(false))
            );

            event_loop.set_focused(2);
            event_loop.sync_orchestration();
            let dispatch = smol::spawn(
                event_loop
                    .ctx
                    .command_runtime
                    .registry
                    .dispatch_input(&command_target, "/model fast=true".into()),
            );
            let command = event_loop
                .ctx
                .command_runtime
                .recv_timeout_for_test(RUNTIME_SHUTDOWN_TIMEOUT)
                .unwrap();
            event_loop.set_focused(1);
            event_loop.handle_command(command);
            assert!(matches!(
                smol::block_on(dispatch),
                maki_commands::InputDispatch::Dispatched(maki_commands::CommandOutcome::Completed)
            ));
            changed_release.send(()).unwrap();
            send(event_loop, 2);
            assert_eq!(
                requests_rx.recv_timeout(RUNTIME_SHUTDOWN_TIMEOUT).unwrap(),
                ("changed", FIRST.into(), changed(true))
            );

            let mut stored = AppSession::new(THIRD, &event_loop.session_cwd);
            stored.meta.thinking = Some(DomainThinkingConfig::Off.into());
            stored.push_message(Message::user(PROMPT.into()));
            stored.save(&event_loop.ctx.storage).unwrap();
            let (reply_tx, reply_rx) = flume::bounded(1);
            event_loop.handle_session_request(
                SessionRequest::Focus {
                    id: stored.id.to_string(),
                },
                reply_tx,
            );
            assert_eq!(
                reply_rx
                    .recv_timeout(RUNTIME_SHUTDOWN_TIMEOUT)
                    .unwrap()
                    .unwrap(),
                json!(true)
            );
            let _ = event_loop.tick();
            let restored_index = event_loop.position(stored.id).unwrap();
            assert_eq!(event_loop.focused, restored_index);
            assert_eq!(event_loop.sessions.len(), 4);
            assert_eq!(event_loop.sessions[2].app.state.session.model, FIRST);
            send(event_loop, restored_index);
            assert_eq!(
                requests_rx.recv_timeout(RUNTIME_SHUTDOWN_TIMEOUT).unwrap(),
                ("restored", THIRD.into(), expected(THIRD, false))
            );
            assert!(Arc::ptr_eq(&original, &first.effective_config().unwrap()));
            assert_eq!(
                event_loop.sessions[1]
                    .coordinator
                    .read()
                    .model()
                    .unwrap()
                    .as_ref(),
                FIRST
            );
            assert_eq!(event_loop.sessions[1].model_slot.load().model.spec(), FIRST);
            releases[0].send(()).unwrap();
            send(event_loop, 1);
            assert_eq!(
                requests_rx.recv_timeout(RUNTIME_SHUTDOWN_TIMEOUT).unwrap(),
                (FIRST, FIRST.into(), expected(FIRST, true))
            );
            releases[0].send(()).unwrap();
            changed_release.send(()).unwrap();
            restored_release.send(()).unwrap();
        });
    }

    #[test]
    fn resumed_session_uses_its_stored_model_and_provider() {
        const STORED_MODEL: &str = "openai/gpt-5";

        let mut harness = RuntimeHarness::new();
        harness.ctx.as_mut().unwrap().prepare_provider = Arc::new(|model, _| {
            Ok(maki_agent::PreparedModel {
                model,
                provider: Arc::new(FocusedProvider),
            })
        });
        assert_ne!(harness.ctx().model_slot.load().model.spec(), STORED_MODEL);
        let mut session = harness.session();
        session.model = STORED_MODEL.into();
        session.push_message(Message::user("stored history".into()));
        let provider = PreparedProvider {
            model: Model::from_spec(STORED_MODEL).unwrap(),
            provider: Arc::new(FocusedProvider),
        };
        let runtime = harness
            .ctx()
            .spawn_runtime_with_provider(session, Some(provider))
            .unwrap();

        assert_eq!(runtime.model_slot.load().model.spec(), STORED_MODEL);
        assert_eq!(
            smol::block_on(runtime.model_slot.load().provider.list_models()).unwrap()[0].id,
            "focused"
        );
        assert_eq!(
            runtime.coordinator.read().model().unwrap().as_ref(),
            STORED_MODEL
        );
        let options = runtime.coordinator.read().options();
        assert_eq!(
            options
                .options
                .iter()
                .find(|option| {
                    option.definition.id.as_ref() == maki_agent::session_options::MODEL_OPTION_ID
                })
                .unwrap()
                .current_value
                .as_ref(),
            STORED_MODEL
        );
        smol::block_on(
            runtime
                .coordinator
                .set_model(Some(Arc::from(STORED_MODEL)), None, None),
        )
        .unwrap();
        assert_eq!(runtime.model_slot.load().model.spec(), STORED_MODEL);
        release_runtime(runtime);
    }

    #[test_case(false ; "absent_plan_path")]
    #[test_case(true ; "missing_plan_file")]
    fn restored_plan_uses_one_path_for_app_and_actor(missing_file: bool) {
        let harness = RuntimeHarness::new();
        let mut session = harness.session();
        session.meta.mode = Some(maki_storage::sessions::StoredMode::Plan);
        session.meta.plan_path = missing_file.then(|| "/nonexistent/maki-plan-test.md".to_owned());
        let runtime = harness.runtime(session);
        let config = runtime.config().unwrap();
        assert!(matches!(config.mode, maki_agent::AgentMode::Plan(_)));
        assert_eq!(config.mode.plan_path(), runtime.app.state.plan.path());
        assert_ne!(
            config.mode.plan_path(),
            Some(Path::new("/nonexistent/maki-plan-test.md"))
        );
        smol::block_on(async {
            let (manager, root) = runtime.handles.manager_and_root();
            let target = manager.lookup(root).unwrap();
            let ticket = reserve_config_change(&target, &runtime.coordinator, None).unwrap();
            ticket
                .commit_change(ConfigChange::ToggleWorkflow)
                .await
                .unwrap();
        });
        let saved = AppSession::load(runtime.id(), &harness.ctx().storage).unwrap();
        assert_eq!(
            saved.meta.mode,
            Some(maki_storage::sessions::StoredMode::Plan)
        );
        assert_eq!(
            saved.meta.plan_path.as_deref().map(Path::new),
            config.mode.plan_path()
        );
        release_runtime(runtime);
    }

    #[test]
    fn direct_option_changes_project_into_tui_and_reload_state() {
        let harness = RuntimeHarness::new();
        let mut runtime = harness.runtime(harness.session());
        smol::block_on(
            runtime
                .coordinator
                .set_option(YOLO_OPTION_ID, ENABLED_VALUE),
        )
        .unwrap();
        smol::block_on(async {
            spawn_root_workflow_toggle(&runtime).await.unwrap();
            wait_projected_option(&runtime, WORKFLOW_OPTION_ID, ENABLED_VALUE).await;
        });

        drain_config_commits(&mut runtime);

        assert!(runtime.app.permissions.is_yolo());
        assert!(runtime.app.root_config().workflow);
        let (manager, root) = runtime.handles.manager_and_root();
        let policy = manager.effective_config(root).unwrap().unwrap();
        assert_eq!(policy.model.spec(), runtime.model_slot.load().model.spec());
        assert!(policy.workflow);
        assert!(!policy.fast);

        let snapshot = runtime.coordinator.read().options();
        let mut reload = Arc::unwrap_or_clone(Arc::clone(&runtime.app.state.session));
        apply_options_to_session(&mut reload, &snapshot);
        assert_eq!(reload.meta.yolo, Some(true));
        assert!(reload.meta.workflow);
        release_runtime(runtime);
    }

    #[test]
    fn coordinator_directory_adopter_rejects_files() {
        const FILE_NAME: &str = "not-a-directory";

        let harness = RuntimeHarness::new();
        let path = harness._temp_dir.path().join(FILE_NAME);
        std::fs::write(&path, []).unwrap();
        let runtime = harness.runtime(harness.session());
        let expected_cwd = runtime.coordinator.read().cwd().unwrap();

        let error = smol::block_on(runtime.coordinator.change_directory(path)).unwrap_err();

        assert!(error.to_string().contains("not a directory"));
        assert_eq!(runtime.coordinator.read().cwd().unwrap(), expected_cwd);
        assert_eq!(*runtime.handles.cwd_slot().load_full(), expected_cwd);
        release_runtime(runtime);
    }

    #[test]
    fn prepared_runtime_is_inert_until_activate() {
        let harness = RuntimeHarness::new();
        let target_count = harness.target_count();
        let prepared = harness.prepare();
        let (session_id, _, manager, root_id) = prepared.snapshot();

        assert_eq!(harness.target_count(), target_count);
        assert!(SessionMailbox::notify(session_id, "early".into(), false).is_err());
        assert_eq!(manager.root_id().unwrap(), root_id);
        assert!(
            manager
                .snapshot()
                .iter()
                .any(|node| node.agent_id == root_id)
        );

        drop(prepared);
        shutdown_manager(&manager);
    }

    #[test]
    fn activation_publishes_target_mailbox_and_runtime_identities() {
        let harness = RuntimeHarness::new();
        let target_count = harness.target_count();
        let prepared = harness.prepare();
        let (session_id, target_id, manager, root_id) = prepared.snapshot();
        let runtime = prepared.activate(&harness.ctx().model_slot, None).unwrap();
        let (active_manager, active_root_id) = runtime.handles.manager_and_root();

        assert_eq!(harness.target_count(), target_count + 1);
        assert_eq!(runtime.id(), session_id);
        assert_eq!(runtime.app.command_target.id(), target_id);
        assert_eq!(active_manager.generation(), manager.generation());
        assert_eq!(active_root_id, root_id);
        assert!(runtime.handles.queue.is_empty());
        assert!(SessionMailbox::notify(session_id, "ready".into(), false).is_ok());

        drop(runtime);
        shutdown_manager(&manager);
        assert_eq!(harness.target_count(), target_count);
        assert!(SessionMailbox::notify(session_id, "late".into(), false).is_err());
    }

    #[test]
    fn replacement_restore_effects_activate_once_after_publication() {
        let harness = RuntimeHarness::new();
        let mut session = harness.session();
        session.push_message(Message::user("history".into()));
        session.meta.queued_messages = vec!["restored".into()];
        let prepared = harness.ctx().prepare_runtime(session).unwrap();
        let mut runtime = prepared.activate(&harness.ctx().model_slot, None).unwrap();

        assert!(runtime.handles.queue.is_empty());
        assert!(runtime.restore_pending);
        runtime.activate_deferred();
        runtime.activate_deferred();

        let deadline = Instant::now() + RUNTIME_SHUTDOWN_TIMEOUT;
        loop {
            let envelope = runtime
                .handles
                .agent_rx
                .recv_timeout(deadline.saturating_duration_since(Instant::now()))
                .expect("restored queue item was not consumed");
            if let AgentEvent::QueueItemConsumed { text, .. } = envelope.event {
                assert_eq!(text, "restored");
                break;
            }
        }
        assert!(runtime.handles.queue.is_empty());
        assert!(!runtime.restore_pending);

        release_runtime(runtime);
    }

    #[test]
    fn committed_rewind_does_not_restore_failed_turn_queue() {
        const RECOVERY_TEXT: &str = "retained after failure";
        const REWOUND_TEXT: &str = "rewound prompt";
        with_event_loop(|event_loop| {
            let model = event_loop.sessions[0].app.root_config().model.clone();
            let mut session = AppSession::new(&model.spec(), &event_loop.session_cwd);
            session.push_message(Message::user(REWOUND_TEXT.into()));
            let prepared = event_loop
                .ctx
                .prepare_runtime_with_provider(
                    session,
                    Some(PreparedProvider {
                        model,
                        provider: Arc::new(StubProvider),
                    }),
                )
                .unwrap();
            event_loop.replace_prepared_runtime(0, prepared).unwrap();
            let (manager, root) = event_loop.sessions[0].handles.manager_and_root();
            let actor = manager.actor(root).unwrap();
            let guard = actor.prepare_idle().unwrap();
            event_loop.submit_text(0, RECOVERY_TEXT.into()).unwrap();
            let app = &mut event_loop.sessions[0].app;
            app.status = Status::Streaming;
            app.update(Msg::Agent(Box::new(Envelope {
                event: AgentEvent::ControlError {
                    message: OBSERVATION.into(),
                },
                subagent: None,
                identity: Some(crate::app::tests::work_identity(app.run_id)),
            })));
            assert!(app.holds_recovery_text());
            assert!(actor.pending_submissions().is_empty());
            drop(guard);
            let boundary = actor.history_boundaries().unwrap().pop().unwrap();
            event_loop
                .dispatch_rewind(0, manager.lookup(root).unwrap(), boundary)
                .unwrap();
            loop {
                let event = event_loop
                    .internal_rx
                    .recv_timeout(SESSION_OP_TIMEOUT)
                    .unwrap();
                let rewound = matches!(
                    &event,
                    InternalEvent::SessionOp {
                        kind: SessionOpKind::HistoryRewound { .. },
                        result: Ok(()),
                        ..
                    }
                );
                event_loop.handle_internal(event);
                if rewound {
                    break;
                }
            }
            let app = &mut event_loop.sessions[0].app;
            assert_eq!(app.input_box.buffer.value(), REWOUND_TEXT);
            assert!(!app.holds_recovery_text());
            app.checkpoint_now();
            assert!(app.state.session.meta.queued_messages.is_empty());
            assert!(actor.pending_submissions().is_empty());
        });
    }

    #[test]
    fn empty_history_replacement_restores_and_checkpoints_draft() {
        const DRAFT: &str = "first prompt";

        let harness = RuntimeHarness::new();
        let mut session = harness.session();
        session.meta.input_draft = Some(DRAFT.into());
        let prepared = harness.ctx().prepare_runtime(session).unwrap();
        let mut runtime = prepared.activate(&harness.ctx().model_slot, None).unwrap();

        assert!(runtime.restore_pending);
        runtime.activate_deferred();
        assert_eq!(runtime.app.input_box.buffer.value(), DRAFT);

        runtime.app.checkpoint_now();
        assert_eq!(
            runtime.app.state.session.meta.input_draft.as_deref(),
            Some(DRAFT)
        );

        release_runtime(runtime);
    }

    #[test]
    fn replacement_preparation_installs_the_session_provider_on_activation() {
        const REPLACEMENT_MODEL: &str = "synthetic/hf:test-model";

        let harness = RuntimeHarness::new();
        let previous_provider = harness.ctx().model_slot.load().provider.identity();
        let mut session = harness.session();
        session.model = REPLACEMENT_MODEL.into();
        let provider = PreparedProvider {
            model: Model::from_spec(REPLACEMENT_MODEL).unwrap(),
            provider: Arc::new(StubProvider),
        };
        let prepared = harness
            .ctx()
            .prepare_runtime_with_provider(session, Some(provider))
            .unwrap();
        let runtime = prepared.activate(&harness.ctx().model_slot, None).unwrap();
        let installed = harness.ctx().model_slot.load();

        assert_eq!(runtime.app.root_config().model.spec(), REPLACEMENT_MODEL);
        assert_eq!(installed.model.spec(), REPLACEMENT_MODEL);
        assert_ne!(installed.provider.identity(), previous_provider);

        drop(installed);
        release_runtime(runtime);
    }

    #[test]
    fn failed_replacement_provider_preparation_preserves_current_runtime() {
        const INVALID_MODEL: &str = "unsupported-provider/test-model";

        let harness = RuntimeHarness::new();
        let mut runtime = harness.runtime(harness.session());
        let current_id = runtime.id();
        let current_target = runtime.app.command_target.id();
        let (current_manager, current_root) = runtime.handles.manager_and_root();
        let current_provider = harness.ctx().model_slot.load().provider.identity();
        let mut session = harness.session();
        session.model = INVALID_MODEL.into();

        runtime.app.checkpoint_now();
        assert!(
            harness
                .ctx()
                .prepare_replacement_runtime(
                    session,
                    runtime.id(),
                    runtime.app.permissions.as_ref(),
                    runtime.handles.offload.as_ref(),
                )
                .is_err()
        );

        assert_eq!(runtime.id(), current_id);
        assert_eq!(runtime.app.command_target.id(), current_target);
        assert_eq!(
            runtime.handles.manager_and_root().0.generation(),
            current_manager.generation()
        );
        assert_eq!(runtime.handles.manager_and_root().1, current_root);
        assert!(runtime.session_lock.is_some());
        assert_eq!(
            harness.ctx().model_slot.load().provider.identity(),
            current_provider
        );

        release_runtime(runtime);
    }

    #[test]
    fn one_manager_per_runtime() {
        let harness = RuntimeHarness::new();
        let first = harness.prepare();
        let second = harness.prepare();
        let (_, _, first_manager, first_root_id) = first.snapshot();
        let (_, _, second_manager, second_root_id) = second.snapshot();

        assert_ne!(first_manager.generation(), second_manager.generation());
        assert_ne!(first_root_id, second_root_id);

        drop((first, second));
        shutdown_manager(&first_manager);
        shutdown_manager(&second_manager);
    }

    #[test]
    fn empty_replacement_loads_session_permission_rules_during_preparation() {
        let harness = RuntimeHarness::new();
        let mut current = harness.runtime(harness.session());
        current.app.permissions.apply_decision(
            &maki_config::ToolKey::native("bash"),
            &["cargo test".into()],
            &PermissionAnswer::AllowSession,
        );
        current.app.checkpoint();
        let mut replacement = harness.session();
        replacement.meta.session_rules = current.app.state.session.meta.session_rules.clone();
        assert!(replacement.messages().is_empty());

        let prepared = harness.ctx().prepare_runtime(replacement).unwrap();

        assert!(
            prepared
                .app
                .session_rule_allows(&maki_config::ToolKey::native("bash"), "cargo test")
        );
        drop(prepared);
        release_runtime(current);
    }

    #[test_case(true, false ; "persisted_enabled_wins_over_startup_disabled")]
    #[test_case(false, true ; "persisted_disabled_wins_over_startup_enabled")]
    fn resumed_session_yolo_wins_over_startup(persisted_yolo: bool, startup_yolo: bool) {
        let harness = RuntimeHarness::new();
        harness.ctx().permissions.set_yolo(startup_yolo);
        let mut session = harness.session();
        session.meta.yolo = Some(persisted_yolo);
        session.push_message(Message::user("resumed".into()));

        let runtime = harness.runtime(session);

        assert_eq!(runtime.app.permissions.is_yolo(), persisted_yolo);
        assert_eq!(
            runtime
                .coordinator
                .read()
                .options()
                .options
                .iter()
                .find(|option| {
                    option.definition.id.as_ref() == maki_agent::session_options::YOLO_OPTION_ID
                })
                .unwrap()
                .current_value
                .as_ref(),
            if persisted_yolo {
                maki_agent::session_options::ENABLED_VALUE
            } else {
                maki_agent::session_options::DISABLED_VALUE
            }
        );
        release_runtime(runtime);
    }

    #[test]
    fn startup_yolo_seeds_new_session_and_coordinator() {
        let harness = RuntimeHarness::new();
        harness.set_startup_yolo();

        let runtime = harness
            .prepare()
            .activate(&harness.ctx().model_slot, None)
            .unwrap();

        assert!(runtime.app.permissions.is_yolo());
        assert_eq!(runtime.app.state.session.meta.yolo, Some(true));
        let options = runtime.coordinator.read().options();
        let yolo = options
            .options
            .iter()
            .find(|option| {
                option.definition.id.as_ref() == maki_agent::session_options::YOLO_OPTION_ID
            })
            .unwrap();
        assert_eq!(
            yolo.current_value.as_ref(),
            maki_agent::session_options::ENABLED_VALUE
        );
        release_runtime(runtime);
    }

    #[test]
    fn startup_yolo_seed_applies_without_marking_session() {
        let harness = RuntimeHarness::with_permissions_config(PermissionsConfig {
            yolo: true,
            ..Default::default()
        });

        let mut runtime = harness
            .prepare()
            .activate(&harness.ctx().model_slot, None)
            .unwrap();

        assert!(runtime.app.permissions.is_yolo());
        assert_eq!(runtime.app.state.session.meta.yolo, None);
        let options = runtime.coordinator.read().options();
        let yolo = options
            .options
            .iter()
            .find(|option| {
                option.definition.id.as_ref() == maki_agent::session_options::YOLO_OPTION_ID
            })
            .unwrap();
        assert_eq!(
            yolo.current_value.as_ref(),
            maki_agent::session_options::ENABLED_VALUE
        );

        checkpoint_runtime(&mut runtime);
        assert!(runtime.app.permissions.is_yolo());
        assert_eq!(runtime.app.state.session.meta.yolo, None);
        release_runtime(runtime);
    }

    #[test_case(false, true, false ; "rewind_preserves_enabled")]
    #[test_case(true, false, false ; "rewind_preserves_disabled")]
    #[test_case(false, true, true ; "reset_preserves_enabled")]
    #[test_case(true, false, true ; "reset_preserves_disabled")]
    fn replacement_preserves_outgoing_yolo(startup_yolo: bool, current_yolo: bool, reset: bool) {
        let harness = RuntimeHarness::new();
        if startup_yolo {
            harness.ctx().permissions.toggle_yolo();
        }
        let session = harness.session();
        let mut runtime = harness.runtime(session.clone());
        if runtime.app.permissions.is_yolo() != current_yolo {
            runtime.app.permissions.toggle_yolo();
        }
        runtime.app.permissions.apply_decision(
            &maki_config::ToolKey::native("bash"),
            &["outgoing".into()],
            &PermissionAnswer::AllowSession,
        );
        let mut replacement = if reset { harness.session() } else { session };
        replacement.model = runtime.app.root_config().model.spec();
        replacement.meta.session_rules = vec![maki_storage::sessions::StoredRule {
            tool: "bash".into(),
            scope: Some("target".into()),
            effect: maki_storage::sessions::StoredEffect::Allow,
        }];
        let prepared = harness
            .ctx()
            .prepare_replacement_runtime(
                replacement,
                runtime.id(),
                runtime.app.permissions.as_ref(),
                runtime.handles.offload.as_ref(),
            )
            .unwrap();

        let old = replace_session_runtime(
            &mut runtime,
            prepared,
            &harness.ctx().sessions_dir,
            &harness.ctx().model_slot,
        )
        .unwrap();

        assert_eq!(runtime.app.permissions.is_yolo(), current_yolo);
        assert_eq!(
            runtime
                .coordinator
                .read()
                .options()
                .options
                .iter()
                .find(|option| {
                    option.definition.id.as_ref() == maki_agent::session_options::YOLO_OPTION_ID
                })
                .unwrap()
                .current_value
                .as_ref(),
            if current_yolo {
                maki_agent::session_options::ENABLED_VALUE
            } else {
                maki_agent::session_options::DISABLED_VALUE
            }
        );
        let rules = runtime.app.permissions.session_rules_snapshot();
        assert!(
            rules
                .iter()
                .any(|rule| rule.scope.as_deref() == Some("target"))
        );
        assert!(
            !rules
                .iter()
                .any(|rule| rule.scope.as_deref() == Some("outgoing"))
        );
        release_runtime(old);
        release_runtime(runtime);
    }

    #[test_case(false ; "rewind")]
    #[test_case(true ; "reset")]
    fn replacement_preserves_unsaved_input_history(reset: bool) {
        const PROMPT: &str = "not saved to disk";

        let harness = RuntimeHarness::new();
        let session = harness.session();
        let mut runtime = harness.runtime(session.clone());
        runtime.app.input_box.set_input(PROMPT.into());
        assert_eq!(runtime.app.input_box.submit().unwrap().text, PROMPT);
        let replacement = if reset { harness.session() } else { session };
        let prepared = harness.ctx().prepare_runtime(replacement).unwrap();

        let old = replace_session_runtime(
            &mut runtime,
            prepared,
            &harness.ctx().sessions_dir,
            &harness.ctx().model_slot,
        )
        .unwrap();
        runtime.app.input_box.history_up();

        assert_eq!(runtime.app.input_box.buffer.value(), PROMPT);
        release_runtime(old);
        release_runtime(runtime);
    }

    #[test_case(true; "closed_task_selected_before_dispatch")]
    #[test_case(false; "pinned_task_closed_before_handler")]
    fn lua_command_runs_without_authority_for_closed_task(closed_before_dispatch: bool) {
        const SPAWNED: &str = "task spawned";
        const OBSERVED: &str = "ordinary handler executed without authority";
        const TASK_NAME: &str = "command regression task";

        with_event_loop(|event_loop| {
            let command_runtime = Arc::clone(&event_loop.ctx.command_runtime);
            event_loop.sessions[0].app.command_target = command_runtime.bind_target();
            event_loop.sessions[0].app.command_runtime = Arc::clone(&command_runtime);
            let registry = command_runtime.registry.clone();
            let host = maki_lua::PluginHost::with_command_registry(
                Arc::clone(maki_agent::tools::ToolRegistry::global_arc()),
                registry.clone(),
                false,
                maki_lua::ui_sink::HostUi::Frontend,
            )
            .unwrap();
            host.event_handle()
                .install_orchestration_services(event_loop.orchestration.clone());
            host.load_source(
                "closed_task_command",
                &format!(
                    r#"
                    maki.api.register_command({{
                        name = "/spawn-command-task", tui_only = false,
                        handler = function(_, ctx)
                            assert(maki.agent.spawn(ctx, {{
                                inherit_provider = true, silent = true,
                            }}))
                            maki.ui.flash("{SPAWNED}")
                        end,
                    }})
                    maki.api.register_command({{
                        name = "/ordinary-command", tui_only = false,
                        handler = function(_, ctx)
                            assert(ctx == nil)
                            local ok, child = pcall(maki.agent.spawn, ctx, {{
                                inherit_provider = true, silent = true,
                            }})
                            assert(not ok or child == nil)
                            maki.ui.flash("{OBSERVED}")
                        end,
                    }})
                    "#
                ),
            )
            .unwrap();
            let actions = host.ui_action_rx();
            event_loop.sync_orchestration();
            smol::block_on(registry.dispatch_input(
                &event_loop.sessions[0].app.command_target,
                "/spawn-command-task".into(),
            ));
            assert!(matches!(
                actions.recv_timeout(Duration::from_secs(5)).unwrap(),
                UiAction::Flash(text) if text == SPAWNED
            ));
            let (manager, root_id) = event_loop.sessions[0].handles.manager_and_root();
            let child_id = manager
                .snapshot()
                .into_iter()
                .find(|node| node.parent_id == Some(root_id))
                .unwrap()
                .agent_id;
            let app = &mut event_loop.sessions[0].app;
            let mut chat = crate::chat::Chat::new(
                TASK_NAME.into(),
                app.ui_config.clone(),
                app.lua_event_handle.clone(),
                Arc::clone(&app.theme_provider),
            );
            chat.agent_id = Some(child_id);
            chat.mark_finished(crate::components::DisplayRole::Assistant, "done");
            app.chats.push(chat);
            app.active_chat = 1;
            assert_eq!(app.selected_agent_id(), Some(child_id));
            if closed_before_dispatch {
                manager.close_subtree(child_id).unwrap();
            }
            event_loop.sync_orchestration();
            let queued = registry.dispatch_input(
                &event_loop.sessions[0].app.command_target,
                "/ordinary-command".into(),
            );
            if !closed_before_dispatch {
                manager.close_subtree(child_id).unwrap();
            }
            assert!(manager.lookup(child_id).is_err());
            smol::block_on(queued);
            assert!(matches!(
                actions.recv_timeout(Duration::from_secs(5)).unwrap(),
                UiAction::Flash(text) if text == OBSERVED
            ));
            assert!(manager.lookup(root_id).is_ok());
            smol::block_on(futures_lite::future::race(
                async {
                    while !manager.runner_finished(child_id).unwrap() {
                        smol::future::yield_now().await;
                    }
                },
                async {
                    smol::Timer::after(RUNTIME_SHUTDOWN_TIMEOUT).await;
                    panic!("closed child runner did not finish");
                },
            ));
            assert_eq!(manager.snapshot().len(), 1);
        });
    }

    #[test_case(false ; "working_directory")]
    #[test_case(true ; "fast_support")]
    fn delayed_agent_context_request_keeps_admitted_target(fast_support: bool) {
        with_event_loop(|event_loop| {
            let (manager, root) = event_loop.sessions[0].handles.manager_and_root();
            let target = manager.lookup(root).unwrap();
            let expected = manager.actor(root).unwrap().effective_config().unwrap();
            let template = event_loop.sessions[0]
                .handles
                .orchestration_template
                .clone();
            let other = test_runtime(model_named("other-model"));
            event_loop.sessions.push(other);
            event_loop.set_focused(1);
            let (reply, response) = flume::bounded(1);
            event_loop.handle_command(CommandEvent::Host {
                target: event_loop.sessions[0].app.command_target.id(),
                request: maki_commands::HostRequest::Context(if fast_support {
                    maki_commands::HostContextRequest::FastModeSupported
                } else {
                    maki_commands::HostContextRequest::WorkingDirectory
                }),
                admission: Some(Box::new(maki_lua::orchestration::TrustedTarget {
                    target,
                    template,
                })),
                reply,
            });
            let response = response.recv().unwrap().unwrap();
            match response {
                HostResponse::Context(maki_commands::HostContextResponse::WorkingDirectory(
                    path,
                )) => {
                    assert!(!fast_support);
                    assert_eq!(path, expected.context.cwd());
                }
                HostResponse::Context(maki_commands::HostContextResponse::FastModeSupported(
                    supported,
                )) => {
                    assert!(fast_support);
                    assert_eq!(
                        supported,
                        expected.model.supports_fast() || expected.model.fast_pending()
                    );
                }
                _ => panic!("unexpected agent context response"),
            }
        });
    }

    #[test_case(false; "fast")]
    #[test_case(true; "workflow")]
    fn delayed_agent_toggle_keeps_admitted_target(workflow: bool) {
        with_event_loop(|event_loop| {
            if !workflow {
                let mut model = model_named("fast-model");
                model.supports_fast_override = Some(FastSupport::Supported);
                let replacement = test_runtime(model);
                let old = std::mem::replace(&mut event_loop.sessions[0], replacement);
                release_runtime(old);
            }
            let (manager, root) = event_loop.sessions[0].handles.manager_and_root();
            let actor = manager.actor(root).unwrap();
            let original = actor.effective_config().unwrap();
            let target = manager.lookup(root).unwrap();
            let template = event_loop.sessions[0]
                .handles
                .orchestration_template
                .clone();
            event_loop
                .sessions
                .push(test_runtime(model_named("other-model")));
            let (other_manager, other_root) = event_loop.sessions[1].handles.manager_and_root();
            let other = other_manager.actor(other_root).unwrap();
            let other_original = other.effective_config().unwrap();
            let commits = actor.subscribe_config_commits();
            event_loop.set_focused(1);
            let (reply, response) = flume::bounded(1);
            event_loop.handle_command(CommandEvent::Host {
                target: event_loop.sessions[0].app.command_target.id(),
                request: maki_commands::HostRequest::Builtin(if workflow {
                    BuiltinOperation::ToggleWorkflow
                } else {
                    BuiltinOperation::ToggleFast
                }),
                admission: Some(Box::new(maki_lua::orchestration::TrustedTarget {
                    target,
                    template,
                })),
                reply,
            });
            assert!(matches!(
                response.recv().unwrap().unwrap(),
                HostResponse::Completed
            ));
            let commit = commits.recv_timeout(SESSION_OP_TIMEOUT).unwrap();
            if workflow {
                assert_eq!(commit.config.workflow, !original.workflow);
                assert_eq!(commit.config.fast, original.fast);
            } else {
                assert_eq!(commit.config.fast, !original.fast);
                assert_eq!(commit.config.workflow, original.workflow);
            }
            assert!(Arc::ptr_eq(
                &other_original,
                &other.effective_config().unwrap()
            ));
        });
    }

    #[test]
    fn editor_path_keeps_pinned_runtime_and_rejects_closed_target() {
        with_event_loop(|event_loop| {
            let (manager, root) = event_loop.sessions[0].handles.manager_and_root();
            let target = manager.lookup(root).unwrap();
            let cwd = manager
                .actor(root)
                .unwrap()
                .effective_config()
                .unwrap()
                .context
                .cwd()
                .to_path_buf();
            event_loop
                .sessions
                .push(test_runtime(model_named("other-model")));
            event_loop.set_focused(1);
            let (index, path) = event_loop
                .pinned_editor_path(&target, Path::new("plan.md"))
                .unwrap();
            assert_eq!(index, 0);
            assert_eq!(path, cwd.join("plan.md"));
            manager.actor(root).unwrap().close();
            assert!(
                event_loop
                    .pinned_editor_path(&target, Path::new("plan.md"))
                    .is_err()
            );
            let (reply_tx, response) = flume::bounded(1);
            event_loop.render(UiAction::OpenEditor {
                target: None,
                path: PathBuf::from("plan.md"),
                reply_tx,
            });
            assert_eq!(response.recv().unwrap(), -1);
        });
    }

    #[test]
    fn queued_relative_directory_uses_fifo_predecessor() {
        with_event_loop(|event_loop| {
            let (manager, root) = event_loop.sessions[0].handles.manager_and_root();
            let actor = manager.actor(root).unwrap();
            let target = manager.lookup(root).unwrap();
            let directory = tempfile::tempdir().unwrap();
            let child = directory.path().join("child");
            std::fs::create_dir(&child).unwrap();
            let barrier = actor.reserve_prepared_operation(None).unwrap();
            event_loop
                .dispatch_target_directory(0, &target, PathBuf::from("child"))
                .unwrap();
            barrier
                .resolve(Some(ConfigChange::WorkingDirectory(
                    directory.path().to_path_buf(),
                )))
                .unwrap();
            smol::block_on(barrier.wait_ready()).unwrap();
            barrier.commit().unwrap();
            let mut completed = 0;
            while completed < 1 {
                let event = event_loop
                    .internal_rx
                    .recv_timeout(SESSION_OP_TIMEOUT)
                    .unwrap();
                if let InternalEvent::SessionOp {
                    kind: SessionOpKind::DirectoryChanged { .. },
                    result,
                    ..
                } = &event
                {
                    assert!(result.is_ok(), "{result:?}");
                    completed += 1;
                }
                event_loop.handle_internal(event);
            }
            assert_eq!(actor.effective_config().unwrap().context.cwd(), child);
            assert_eq!(
                event_loop.sessions[0].coordinator.read().cwd().unwrap(),
                child
            );
        });
    }

    #[test]
    fn failed_directory_persistence_does_not_publish_actor_context() {
        with_event_loop(|event_loop| {
            let model = event_loop.sessions[0].app.root_config().model.clone();
            let session = AppSession::new(&model.spec(), &event_loop.session_cwd);
            let prepared = event_loop
                .ctx
                .prepare_runtime_with_provider(
                    session,
                    Some(PreparedProvider {
                        model,
                        provider: Arc::new(StubProvider),
                    }),
                )
                .unwrap();
            event_loop.replace_prepared_runtime(0, prepared).unwrap();
            let (manager, root) = event_loop.sessions[0].handles.manager_and_root();
            let actor = manager.actor(root).unwrap();
            let original = actor.effective_config().unwrap();
            let directory = tempfile::tempdir().unwrap();
            event_loop.sessions[0].coordinator.retire();
            assert!(
                event_loop
                    .dispatch_target_directory(
                        0,
                        &manager.lookup(root).unwrap(),
                        directory.path().to_path_buf(),
                    )
                    .is_err()
            );
            assert!(Arc::ptr_eq(&original, &actor.effective_config().unwrap()));
            assert_eq!(
                event_loop.sessions[0].app.state.session.cwd,
                original.context.cwd().to_string_lossy()
            );
        });
    }

    #[test]
    fn focused_operations_reject_foreign_and_closed_targets() {
        with_event_loop(|event_loop| {
            let foreign = test_runtime(model_named("other-model"));
            let (foreign_manager, foreign_root) = foreign.handles.manager_and_root();
            let target = foreign_manager.lookup(foreign_root).unwrap();
            assert!(event_loop.target_actor(0, &target).is_err());
            let (manager, root) = event_loop.sessions[0].handles.manager_and_root();
            let target = manager.lookup(root).unwrap();
            manager.actor(root).unwrap().close();
            assert!(event_loop.target_actor(0, &target).is_err());
            release_runtime(foreign);
        });
    }

    #[test]
    fn focused_history_operations_preserve_identity_and_children() {
        const FIRST: &str = "first prompt";
        const DRAFT: &str = "second prompt";
        with_event_loop(|event_loop| {
            let mut session = AppSession::new(
                &event_loop.sessions[0].app.root_config().model.spec(),
                &event_loop.session_cwd,
            );
            session.replace_messages(vec![
                Message::user(FIRST.into()),
                Message::user(DRAFT.into()),
            ]);
            let model = event_loop.sessions[0].app.root_config().model.clone();
            let prepared = event_loop
                .ctx
                .prepare_runtime_with_provider(
                    session,
                    Some(PreparedProvider {
                        model,
                        provider: Arc::new(StubProvider),
                    }),
                )
                .unwrap();
            event_loop.replace_prepared_runtime(0, prepared).unwrap();
            let (manager, root) = event_loop.sessions[0].handles.manager_and_root();
            let target = manager.lookup(root).unwrap();
            let actor = manager.actor(root).unwrap();
            let identity = actor.identity();
            let host = maki_lua::PluginHost::with_command_registry(
                Arc::clone(maki_agent::tools::ToolRegistry::global_arc()),
                event_loop.ctx.command_runtime.registry.clone(),
                false,
                maki_lua::ui_sink::HostUi::Frontend,
            )
            .unwrap();
            host.event_handle()
                .install_orchestration_services(event_loop.orchestration.clone());
            host.load_source(
                "rewind_child",
                r#"
                maki.api.register_command({
                    name = "/rewind-child", tui_only = false,
                    handler = function(_, ctx)
                        assert(maki.agent.spawn(ctx, { inherit_provider = true, silent = true }))
                        maki.ui.flash("rewind child ready")
                    end,
                })
            "#,
            )
            .unwrap();
            event_loop.sync_orchestration();
            let outcome = smol::block_on(event_loop.ctx.command_runtime.registry.dispatch_input(
                &event_loop.sessions[0].app.command_target,
                "/rewind-child".into(),
            ));
            assert!(
                matches!(
                    outcome,
                    maki_commands::InputDispatch::Dispatched(
                        maki_commands::CommandOutcome::Completed
                    )
                ),
                "{outcome:?}"
            );
            assert!(
                matches!(host.ui_action_rx().recv_timeout(SESSION_OP_TIMEOUT).unwrap(), UiAction::Flash(text) if text == "rewind child ready")
            );
            let child = manager
                .snapshot()
                .into_iter()
                .find(|node| node.parent_id == Some(root))
                .unwrap()
                .agent_id;
            let child_actor = manager.actor(child).unwrap();
            let child_identity = child_actor.identity();
            let child_before = child_actor.snapshot();
            let boundary = actor.history_boundaries().unwrap().pop().unwrap();
            event_loop.dispatch_rewind(0, target, boundary).unwrap();
            loop {
                let event = event_loop
                    .internal_rx
                    .recv_timeout(SESSION_OP_TIMEOUT)
                    .unwrap();
                let rewound = matches!(
                    &event,
                    InternalEvent::SessionOp {
                        kind: SessionOpKind::HistoryRewound { .. },
                        ..
                    }
                );
                event_loop.handle_internal(event);
                if rewound {
                    break;
                }
            }
            assert!(Arc::ptr_eq(
                &identity,
                &manager.actor(root).unwrap().identity()
            ));
            assert_eq!(event_loop.sessions[0].app.state.session.messages().len(), 1);
            assert_eq!(event_loop.sessions[0].app.input_box.buffer.value(), DRAFT);
            assert_eq!(actor.history_boundaries().unwrap().len(), 1);
            assert!(Arc::ptr_eq(
                &child_identity,
                &manager.actor(child).unwrap().identity()
            ));
            assert_eq!(child_actor.snapshot().status, child_before.status);
            assert_eq!(child_actor.snapshot().latest, child_before.latest);
            assert!(
                manager
                    .snapshot()
                    .iter()
                    .any(|node| { node.agent_id == child && node.parent_id == Some(root) })
            );
        });
    }

    #[test]
    fn session_prompt_rejects_empty_input_without_admission() {
        let mut runtime = test_runtime(model_named("test-model"));
        assert_eq!(
            runtime.submit_text("  ".into()).err().as_deref(),
            Some(EMPTY_PROMPT_ERR)
        );
        assert!(runtime.handles.queue.is_empty());
        release_runtime(runtime);
    }

    #[test]
    fn model_spec_requests_follow_the_pinned_target_policy() {
        const ALLOWED: &str = "anthropic/claude-opus-4-6";
        const EXCLUDED: &str = "openai/gpt-5";
        with_event_loop(|event_loop| {
            let command_runtime = Arc::clone(&event_loop.ctx.command_runtime);
            let app = &mut event_loop.sessions[0].app;
            app.command_runtime = Arc::clone(&command_runtime);
            app.command_target = command_runtime.bind_target();
            event_loop
                .ctx
                .catalog
                .specs()
                .store(Some(Arc::new(vec![ALLOWED.into(), EXCLUDED.into()])));
            event_loop.sessions[0]
                .handles
                .orchestration_template
                .model_policy = Arc::new(ModelPolicy::new(&[], &[EXCLUDED.into()]).unwrap());
            event_loop.sync_orchestration();
            let (manager, root) = event_loop.sessions[0].handles.manager_and_root();
            let target = manager.lookup(root).unwrap();
            let Ok(HostResponse::Context(maki_commands::HostContextResponse::Values(specs))) =
                event_loop.target_model_specs(&target)
            else {
                panic!("model specs request failed");
            };
            assert_eq!(&*specs, [Arc::<str>::from(ALLOWED)]);
            let complete = |target: &maki_commands::TargetHandle| {
                let registry = &command_runtime.registry;
                let command = registry.resolve_for(target, "/model").unwrap();
                let completion = registry.open_completion(command, target.id()).unwrap();
                let maki_commands::CompletionResult::Items(items) = smol::block_on(
                    completion.complete(Arc::from(""), Arc::from(""), 0, Arc::from("insert")),
                ) else {
                    panic!("expected model completions");
                };
                items
                    .iter()
                    .map(|item| item.item().insertion.to_string())
                    .collect::<Vec<_>>()
            };
            let original = event_loop.sessions[0].app.command_target.clone();
            let items = complete(&original);
            assert!(items.iter().any(|spec| spec == ALLOWED));
            assert!(!items.iter().any(|spec| spec == EXCLUDED));
            assert!(items.iter().any(|spec| spec == "thinking=high"));
            assert!(items.iter().any(|spec| spec == "fast=true"));
            let picker = &mut event_loop.sessions[0].app.model_picker;
            picker.set_recents(vec![EXCLUDED.into()]);
            picker.open(EXCLUDED);
            assert!(matches!(
                picker.handle_key(crate::components::key(KeyCode::Enter)),
                crate::components::model_picker::ModelPickerAction::Select(spec) if spec == ALLOWED
            ));

            picker.open(ALLOWED);
            event_loop.sessions[0]
                .handles
                .orchestration_template
                .model_policy = Arc::new(ModelPolicy::new(&[], &[ALLOWED.into()]).unwrap());
            event_loop.sync_orchestration();
            assert!(matches!(
                event_loop.sessions[0].app.model_picker.handle_key(crate::components::key(KeyCode::Enter)),
                crate::components::model_picker::ModelPickerAction::Select(spec) if spec == EXCLUDED
            ));

            let mut second = test_runtime(model_named("other-model"));
            second.app.command_runtime = Arc::clone(&command_runtime);
            second.app.command_target = command_runtime.bind_target();
            second.handles.orchestration_template.model_policy =
                Arc::new(ModelPolicy::new(&[], &[EXCLUDED.into()]).unwrap());
            event_loop.sessions.push(second);
            event_loop.set_focused(1);
            event_loop.sync_orchestration();
            let original_items = complete(&original);
            assert!(!original_items.iter().any(|spec| spec == ALLOWED));
            assert!(original_items.iter().any(|spec| spec == EXCLUDED));
            let focused_items = complete(&event_loop.sessions[1].app.command_target);
            assert!(focused_items.iter().any(|spec| spec == ALLOWED));
            assert!(!focused_items.iter().any(|spec| spec == EXCLUDED));
        });
    }

    #[test]
    fn tasks_selection_routes_keyboard_submission_through_child_actor() {
        const SPAWNED: &str = "tasks child spawned";
        const CHILD_NAME: &str = "tasks child";
        const INPUT: &str = "continue selected child work";

        with_event_loop(|event_loop| {
            let command_runtime = Arc::clone(&event_loop.ctx.command_runtime);
            let app = &mut event_loop.sessions[0].app;
            app.command_target = command_runtime.bind_target();
            app.command_runtime = Arc::clone(&command_runtime);
            let host = maki_lua::PluginHost::with_command_registry(
                Arc::clone(maki_agent::tools::ToolRegistry::global_arc()),
                command_runtime.registry.clone(),
                false,
                maki_lua::ui_sink::HostUi::Frontend,
            )
            .unwrap();
            host.event_handle()
                .install_orchestration_services(event_loop.orchestration.clone());
            host.load_source(
                "tasks_child_input",
                &format!(
                    r#"
                    maki.api.register_command({{
                        name = "/spawn-tasks-child", tui_only = false,
                        handler = function(_, ctx)
                            assert(maki.agent.spawn(ctx, {{ inherit_provider = true, show = false }}))
                            maki.ui.flash("{SPAWNED}")
                        end,
                    }})
                    "#
                ),
            )
            .unwrap();
            event_loop.sync_orchestration();
            smol::block_on(command_runtime.registry.dispatch_input(
                &event_loop.sessions[0].app.command_target,
                "/spawn-tasks-child".into(),
            ));
            assert!(matches!(
                host.ui_action_rx().recv_timeout(SESSION_OP_TIMEOUT).unwrap(),
                UiAction::Flash(text) if text == SPAWNED
            ));
            let (manager, root) = event_loop.sessions[0].handles.manager_and_root();
            let child = manager
                .snapshot()
                .into_iter()
                .find(|node| node.parent_id == Some(root))
                .unwrap()
                .agent_id;
            let root_actor = manager.actor(root).unwrap();
            let child_actor = manager.actor(child).unwrap();
            let _root_guard = root_actor.prepare_idle().unwrap();
            let child_guard = child_actor.prepare_idle().unwrap();
            let app = &mut event_loop.sessions[0].app;
            let mut chat = crate::chat::Chat::new(
                CHILD_NAME.into(),
                app.ui_config.clone(),
                app.lua_event_handle.clone(),
                Arc::clone(&app.theme_provider),
            );
            chat.agent_id = Some(child);
            app.live_chat_index.insert(child, app.chats.len());
            app.chats.push(chat);

            event_loop.handle_input(Event::Key(
                crate::components::keybindings::key::TASKS.to_key_event(),
            ));
            event_loop.handle_input(Event::Key(crate::components::key(KeyCode::Down)));
            event_loop.handle_input(Event::Key(crate::components::key(KeyCode::Enter)));
            assert_eq!(event_loop.sessions[0].app.selected_agent_id(), Some(child));
            for character in INPUT.chars() {
                event_loop
                    .handle_input(Event::Key(crate::components::key(KeyCode::Char(character))));
            }
            event_loop.handle_input(Event::Key(crate::components::key(KeyCode::Enter)));
            let pending = child_actor.pending_submissions();
            assert_eq!(pending.len(), 1);
            assert_eq!(pending[0].text, INPUT);
            assert!(root_actor.pending_submissions().is_empty());
            assert!(
                event_loop.sessions[0]
                    .app
                    .queue
                    .root_text_messages()
                    .is_empty()
            );

            drop(child_guard);
            let deadline = Instant::now() + SESSION_OP_TIMEOUT;
            loop {
                let envelope = event_loop.sessions[0]
                    .handles
                    .agent_rx
                    .recv_timeout(deadline.saturating_duration_since(Instant::now()))
                    .expect("child input must reach the production envelope channel");
                let consumed = matches!(
                    &envelope.event,
                    AgentEvent::QueueItemConsumed { text, .. } if text == INPUT
                );
                if consumed {
                    assert_eq!(envelope.subagent.as_ref().unwrap().agent_id, child);
                }
                event_loop.handle_agent(0, Box::new(envelope));
                if consumed {
                    break;
                }
            }
            let app = &event_loop.sessions[0].app;
            let child_chat = app
                .chats
                .iter()
                .find(|chat| chat.agent_id == Some(child))
                .unwrap();
            assert_eq!(child_chat.last_message_text(), INPUT);
            assert_ne!(app.chats[0].last_message_text(), INPUT);
            assert_eq!(app.selected_agent_id(), Some(child));
            assert!(root_actor.pending_submissions().is_empty());
            assert!(app.queue.root_text_messages().is_empty());
        });
    }

    #[test_case(false, false, false ; "direct_action")]
    #[test_case(true, false, false ; "slash_command")]
    #[test_case(false, true, false ; "direct_closed_target")]
    #[test_case(true, true, false ; "slash_closed_target")]
    #[test_case(false, false, true ; "direct_closed_before_open")]
    #[test_case(true, false, true ; "slash_closed_before_handle")]
    fn model_picker_keeps_admitted_child_through_confirmation(
        slash_command: bool,
        close_child: bool,
        close_before_open: bool,
    ) {
        const CHILD_MODEL: &str = "anthropic/child-model";
        const NEXT_MODEL: &str = "anthropic/claude-sonnet-4-6";
        const CHILD_CHAT: &str = "picker child";
        const SPAWNED: &str = "picker child spawned";
        let open_picker = |event_loop: &mut EventLoop<'_>| {
            if slash_command {
                let target = event_loop.sessions[0].app.command_target.clone();
                let dispatch = smol::spawn(
                    event_loop
                        .ctx
                        .command_runtime
                        .registry
                        .dispatch_input(&target, "/model".into()),
                );
                let command = event_loop
                    .ctx
                    .command_runtime
                    .recv_timeout_for_test(RUNTIME_SHUTDOWN_TIMEOUT)
                    .unwrap();
                if close_before_open {
                    let target = selected_agent(&event_loop.sessions[0]).unwrap();
                    target.manager().actor(target.id()).unwrap().close();
                    event_loop.sync_orchestration();
                }
                event_loop.sessions[0].app.active_chat = 0;
                event_loop.sync_orchestration();
                event_loop.handle_command(command);
                let outcome = smol::block_on(dispatch);
                if close_before_open {
                    assert!(matches!(
                        outcome,
                        maki_commands::InputDispatch::Dispatched(
                            maki_commands::CommandOutcome::Failed(CommandError::StaleTarget)
                        )
                    ));
                } else {
                    assert!(matches!(
                        outcome,
                        maki_commands::InputDispatch::Dispatched(
                            maki_commands::CommandOutcome::Completed
                        )
                    ));
                }
            } else {
                if close_before_open {
                    let target = selected_agent(&event_loop.sessions[0]).unwrap();
                    target.manager().actor(target.id()).unwrap().close();
                    event_loop.sync_orchestration();
                    assert!(
                        event_loop
                            .ctx
                            .command_runtime
                            .target_agent(&event_loop.sessions[0].app.command_target)
                            .is_none()
                    );
                }
                event_loop.sessions[0]
                    .app
                    .run_builtin(BuiltinAction::ModelPicker);
            }
        };
        with_event_loop(|event_loop| {
            let command_runtime = Arc::clone(&event_loop.ctx.command_runtime);
            event_loop.ctx.prepare_provider = Arc::new(|model, _| {
                Ok(maki_agent::actor::PreparedModel {
                    model,
                    provider: Arc::new(StubProvider),
                })
            });
            let app = &mut event_loop.sessions[0].app;
            app.command_target = command_runtime.bind_target();
            app.command_runtime = Arc::clone(&command_runtime);
            let host = maki_lua::PluginHost::with_command_registry(
                Arc::clone(maki_agent::tools::ToolRegistry::global_arc()),
                command_runtime.registry.clone(),
                false,
                maki_lua::ui_sink::HostUi::Frontend,
            )
            .unwrap();
            host.event_handle()
                .install_orchestration_services(event_loop.orchestration.clone());
            host.load_source(
                "model_picker_child",
                &format!(
                    r#"
                    maki.api.register_command({{
                        name = "/spawn-picker-child", tui_only = false,
                        handler = function(_, ctx)
                            assert(maki.agent.spawn(ctx, {{ inherit_provider = true, silent = true }}))
                            maki.ui.flash("{SPAWNED}")
                        end,
                    }})
                    "#
                ),
            )
            .unwrap();
            event_loop.sync_orchestration();
            smol::block_on(command_runtime.registry.dispatch_input(
                &event_loop.sessions[0].app.command_target,
                "/spawn-picker-child".into(),
            ));
            assert!(matches!(
                host.ui_action_rx().recv_timeout(SESSION_OP_TIMEOUT).unwrap(),
                UiAction::Flash(text) if text == SPAWNED
            ));
            event_loop.sessions[0]
                .handles
                .orchestration_template
                .model_policy = Arc::new(ModelPolicy::new(&[], &[NEXT_MODEL.into()]).unwrap());
            let (manager, root) = event_loop.sessions[0].handles.manager_and_root();
            let child = manager
                .snapshot()
                .into_iter()
                .find(|node| node.parent_id == Some(root))
                .unwrap()
                .agent_id;
            let actor = manager.actor(child).unwrap();
            smol::block_on(
                actor
                    .reserve_prepared_operation(None)
                    .unwrap()
                    .commit_when_ready(Some(ConfigChange::Patch(ConfigPatch {
                        model: Some(maki_agent::actor::PreparedModel {
                            provider: Arc::new(StubProvider),
                            model: model_named("child-model"),
                        }),
                        ..Default::default()
                    }))),
            )
            .unwrap();
            select_child_chat(event_loop, child, CHILD_CHAT);
            let root_model = event_loop.sessions[0].app.root_config().model.spec();
            assert_ne!(root_model, CHILD_MODEL);
            event_loop.ctx.catalog.specs().store(Some(Arc::new(vec![
                root_model.clone(),
                CHILD_MODEL.into(),
                NEXT_MODEL.into(),
            ])));
            event_loop.sync_orchestration();
            open_picker(event_loop);
            if close_before_open {
                assert!(!event_loop.sessions[0].app.model_picker.is_open());
                assert!(event_loop.sessions[0].app.model_picker_target.is_none());
                event_loop.sessions[0].app.active_chat = 0;
                event_loop.sync_orchestration();
                let actions = event_loop.sessions[0]
                    .app
                    .update(crate::app::Msg::Key(crate::components::key(KeyCode::Enter)));
                assert!(!actions.iter().any(|action| matches!(
                    action,
                    Action::ChangeModel(_)
                        | Action::ChangePinnedModel { .. }
                        | Action::SelectModel(_)
                )));
                assert_eq!(
                    manager
                        .actor(root)
                        .unwrap()
                        .effective_config()
                        .unwrap()
                        .model
                        .spec(),
                    root_model
                );
                return;
            }
            event_loop.sessions[0].app.active_chat = 1;
            event_loop.sync_orchestration();
            event_loop.sessions[0].app.active_chat = 0;
            event_loop.sync_orchestration();
            event_loop.sync_model_discovery();
            let app = &mut event_loop.sessions[0].app;
            let actions = app.update(crate::app::Msg::Key(crate::components::key(KeyCode::Enter)));
            assert!(
                matches!(actions.as_slice(), [Action::ChangePinnedModel { target, spec }]
                if target.id() == child && spec == CHILD_MODEL)
            );
            assert_eq!(app.root_config().model.spec(), root_model);
            assert_eq!(actor.effective_config().unwrap().model.spec(), CHILD_MODEL);

            app.active_chat = 1;
            event_loop.sync_orchestration();
            open_picker(event_loop);
            event_loop.sessions[0].app.active_chat = 0;
            event_loop.sync_orchestration();
            event_loop.sync_model_discovery();
            if close_child {
                actor.close();
            }
            let app = &mut event_loop.sessions[0].app;
            app.update(crate::app::Msg::Paste(
                NEXT_MODEL.split_once('/').unwrap().1.into(),
            ));
            let actions = app.update(crate::app::Msg::Key(crate::components::key(KeyCode::Enter)));
            assert!(
                matches!(actions.as_slice(), [Action::ChangePinnedModel { target, spec }]
                if target.id() == child && spec == NEXT_MODEL)
            );
            event_loop.dispatch(0, actions);
            let completion = std::iter::repeat_with(|| {
                event_loop
                    .internal_rx
                    .recv_timeout(SESSION_OP_TIMEOUT)
                    .unwrap()
            })
            .find(|event| !matches!(event, InternalEvent::ModelsDiscovered))
            .unwrap();
            assert!(
                matches!(&completion, InternalEvent::SessionOp { result, .. } if result.is_err() == close_child),
                "{:?}",
                match &completion {
                    InternalEvent::SessionOp { result, .. } => Some(result),
                    _ => None,
                },
            );
            event_loop.handle_internal(completion);
            assert_eq!(
                manager
                    .actor(root)
                    .unwrap()
                    .effective_config()
                    .unwrap()
                    .model
                    .spec(),
                root_model
            );
            if !close_child {
                assert_eq!(actor.effective_config().unwrap().model.spec(), NEXT_MODEL);
            }
        });
    }

    #[test]
    fn keymap_run_command_addresses_its_pinned_target_after_focus_moves() {
        const PINNED_THINKING: &str = "high";
        const CHILD_CHAT: &str = "keymap pin child";
        const SPAWNED: &str = "keymap pin child spawned";
        const INNER_DONE: &str = "keymap pin command finished";
        const ACTION_TIMEOUT: Duration = Duration::from_secs(5);

        with_event_loop(|event_loop| {
            let command_runtime = Arc::clone(&event_loop.ctx.command_runtime);
            event_loop.sessions[0].app.command_target = command_runtime.bind_target();
            event_loop.sessions[0].app.command_runtime = Arc::clone(&command_runtime);
            let registry = command_runtime.registry.clone();
            let host = maki_lua::PluginHost::with_command_registry(
                Arc::clone(maki_agent::tools::ToolRegistry::global_arc()),
                registry.clone(),
                false,
                maki_lua::ui_sink::HostUi::Frontend,
            )
            .unwrap();
            host.event_handle()
                .install_orchestration_services(event_loop.orchestration.clone());
            event_loop.sessions[0].app.lua_event_handle = host.event_handle();
            host.load_source(
                "keymap_pin_command",
                &format!(
                    r#"
                    maki.api.register_command({{
                        name = "/spawn-keymap-child", tui_only = false,
                        handler = function(_, ctx)
                            assert(maki.agent.spawn(ctx, {{ inherit_provider = true, silent = true }}))
                            maki.ui.flash("{SPAWNED}")
                        end,
                    }})
                    maki.api.register_command({{
                        name = "/keymap-inner", tui_only = false,
                        handler = function(_, ctx)
                            local target = assert(ctx:agent())
                            assert(target:set_model(ctx, {{ thinking = "{PINNED_THINKING}" }}))
                            maki.ui.flash("{INNER_DONE}")
                        end,
                    }})
                    maki.keymap.set("n", "<C-y>", function(_, ctx)
                        assert(maki.api.run_command(ctx, "/keymap-inner"))
                    end)
                    "#
                ),
            )
            .unwrap();
            let actions = host.ui_action_rx();
            event_loop.sync_orchestration();
            let command_target = event_loop.sessions[0].app.command_target.clone();
            smol::block_on(registry.dispatch_input(&command_target, "/spawn-keymap-child".into()));
            assert!(matches!(
                actions.recv_timeout(ACTION_TIMEOUT).unwrap(),
                UiAction::Flash(text) if text == SPAWNED
            ));
            let (manager, root_id) = event_loop.sessions[0].handles.manager_and_root();
            let child_id = manager
                .snapshot()
                .into_iter()
                .find(|node| node.parent_id == Some(root_id))
                .unwrap()
                .agent_id;
            let thinking = |id| {
                manager
                    .actor(id)
                    .unwrap()
                    .effective_config()
                    .unwrap()
                    .thinking
            };
            let pinned_thinking = DomainThinkingConfig::Effort(Effort::High);
            let root_thinking = thinking(root_id);
            assert_ne!(root_thinking, pinned_thinking);
            assert_ne!(thinking(child_id), pinned_thinking);

            select_child_chat(event_loop, child_id, CHILD_CHAT);
            let keymap = host.keymap_reader().load().entries[0].id;
            event_loop.run_keybind(0, keymap);
            event_loop.sessions[0].app.active_chat = 0;
            event_loop.sync_orchestration();
            assert!(matches!(
                actions.recv_timeout(ACTION_TIMEOUT).unwrap(),
                UiAction::Flash(text) if text == INNER_DONE
            ));
            assert_eq!(thinking(child_id), pinned_thinking);
            assert_eq!(thinking(root_id), root_thinking);
        });
    }

    #[test]
    fn nested_lua_command_acts_on_target_pinned_before_selection_changes() {
        const PINNED_THINKING: &str = "high";
        const CHILD_CHAT: &str = "nested pin child";
        const SPAWNED: &str = "nested pin child spawned";
        const INNER_DONE: &str = "nested pin inner command finished";
        const ACTION_TIMEOUT: Duration = Duration::from_secs(5);

        with_event_loop(|event_loop| {
            let command_runtime = Arc::clone(&event_loop.ctx.command_runtime);
            event_loop.sessions[0].app.command_target = command_runtime.bind_target();
            event_loop.sessions[0].app.command_runtime = Arc::clone(&command_runtime);
            let registry = command_runtime.registry.clone();
            let host = maki_lua::PluginHost::with_command_registry(
                Arc::clone(maki_agent::tools::ToolRegistry::global_arc()),
                registry.clone(),
                false,
                maki_lua::ui_sink::HostUi::Frontend,
            )
            .unwrap();
            host.event_handle()
                .install_orchestration_services(event_loop.orchestration.clone());
            host.load_source(
                "nested_pin_command",
                &format!(
                    r#"
                    maki.api.register_command({{
                        name = "/spawn-pin-child", tui_only = false,
                        handler = function(_, ctx)
                            assert(maki.agent.spawn(ctx, {{ inherit_provider = true, silent = true }}))
                            maki.ui.flash("{SPAWNED}")
                        end,
                    }})
                    maki.api.register_command({{
                        name = "/pin-outer", tui_only = false,
                        handler = function(_, ctx)
                            assert(maki.fn.winsaveview())
                            assert(maki.api.run_command(ctx, "/pin-inner"))
                        end,
                    }})
                    maki.api.register_command({{
                        name = "/pin-inner", tui_only = false,
                        handler = function(_, ctx)
                            local target = assert(ctx:agent())
                            assert(target:set_model(ctx, {{ thinking = "{PINNED_THINKING}" }}))
                            maki.ui.flash("{INNER_DONE}")
                        end,
                    }})
                    "#
                ),
            )
            .unwrap();
            let actions = host.ui_action_rx();
            event_loop.sync_orchestration();
            let command_target = event_loop.sessions[0].app.command_target.clone();
            smol::block_on(registry.dispatch_input(&command_target, "/spawn-pin-child".into()));
            assert!(matches!(
                actions.recv_timeout(ACTION_TIMEOUT).unwrap(),
                UiAction::Flash(text) if text == SPAWNED
            ));
            let (manager, root_id) = event_loop.sessions[0].handles.manager_and_root();
            let child_id = manager
                .snapshot()
                .into_iter()
                .find(|node| node.parent_id == Some(root_id))
                .unwrap()
                .agent_id;
            let thinking = |id| {
                manager
                    .actor(id)
                    .unwrap()
                    .effective_config()
                    .unwrap()
                    .thinking
            };
            let pinned_thinking = DomainThinkingConfig::Effort(Effort::High);
            let child_thinking = thinking(child_id);
            assert_ne!(thinking(root_id), pinned_thinking);
            assert_ne!(child_thinking, pinned_thinking);

            let outer = smol::spawn(registry.dispatch_input(&command_target, "/pin-outer".into()));
            let UiAction::WinSaveView { reply_tx } = actions.recv_timeout(ACTION_TIMEOUT).unwrap()
            else {
                panic!("outer command did not reach its gate");
            };
            let app = &mut event_loop.sessions[0].app;
            let mut chat = crate::chat::Chat::new(
                CHILD_CHAT.into(),
                app.ui_config.clone(),
                app.lua_event_handle.clone(),
                Arc::clone(&app.theme_provider),
            );
            chat.agent_id = Some(child_id);
            app.chats.push(chat);
            app.active_chat = 1;
            assert_eq!(app.selected_agent_id(), Some(child_id));
            event_loop.sync_orchestration();
            reply_tx
                .send(maki_lua::WinView {
                    scroll_top: 0,
                    line_count: 0,
                    height: 0,
                    auto_scroll: false,
                })
                .unwrap();
            assert!(matches!(
                actions.recv_timeout(ACTION_TIMEOUT).unwrap(),
                UiAction::Flash(text) if text == INNER_DONE
            ));
            smol::block_on(outer);
            assert_eq!(thinking(root_id), pinned_thinking);
            assert_eq!(thinking(child_id), child_thinking);
        });
    }

    #[test]
    fn unchanged_orchestration_keeps_registered_templates() {
        with_event_loop(|event_loop| {
            event_loop.sync_orchestration();
            let revision = &event_loop.orchestration_revisions[0];
            let original = revision.root_config.clone().unwrap();
            let revisions = event_loop.orchestration_revisions.as_ptr();
            event_loop.sync_orchestration();
            assert_eq!(event_loop.orchestration_revisions.as_ptr(), revisions);
            assert!(Arc::ptr_eq(
                event_loop.orchestration_revisions[0]
                    .root_config
                    .as_ref()
                    .unwrap(),
                &original,
            ));
            let (manager, root) = event_loop.sessions[0].handles.manager_and_root();
            let reservation = reserve_config_change(
                &manager.lookup(root).unwrap(),
                &event_loop.sessions[0].coordinator,
                None,
            )
            .unwrap();
            smol::block_on(reservation.commit_change(ConfigChange::ToggleWorkflow)).unwrap();
            event_loop.sync_orchestration();
            assert!(!Arc::ptr_eq(
                event_loop.orchestration_revisions[0]
                    .root_config
                    .as_ref()
                    .unwrap(),
                &original,
            ));
        });
    }

    #[test]
    fn queued_command_admission_keeps_agent_after_ui_selection_changes() {
        struct Probe(Arc<Mutex<Option<maki_agent::AgentId>>>);
        impl maki_commands::CommandBehavior for Probe {
            fn execute(
                &self,
                invocation: maki_commands::CommandInvocation,
            ) -> maki_commands::CommandFuture<Result<maki_commands::CommandOutcome, CommandError>>
            {
                *self.0.lock().unwrap() = Some(
                    invocation
                        .admission_context::<maki_lua::orchestration::TrustedTarget>()
                        .unwrap()
                        .target
                        .id(),
                );
                Box::pin(async { Ok(maki_commands::CommandOutcome::Completed) })
            }
        }
        let harness = RuntimeHarness::new();
        let first = harness.runtime(harness.session());
        let second = harness.runtime(harness.session());
        let command_runtime = &harness.ctx().command_runtime;
        let (first_manager, first_id) = first.handles.manager_and_root();
        let (second_manager, second_id) = second.handles.manager_and_root();
        let target_id = first.app.command_target.id();
        command_runtime.sync_admissions(std::collections::HashMap::from([(
            target_id,
            maki_lua::orchestration::TrustedTarget {
                target: first_manager.lookup(first_id).unwrap(),
                template: first.handles.orchestration_template.clone(),
            },
        )]));
        let seen = Arc::new(Mutex::new(None));
        let producer = command_runtime
            .registry
            .create_producer(maki_commands::ProducerPrecedence::Application);
        producer
            .replace(vec![maki_commands::Registration {
                spec: maki_commands::CommandSpec {
                    name: Arc::from("/admission-race"),
                    aliases: Arc::from([]),
                    arguments: maki_commands::CommandArguments::Raw { required: false },
                    docs: maki_commands::CommandDocs {
                        summary: Arc::from("Admission race probe"),
                        argument_hint: None,
                    },
                    required_capabilities: maki_commands::TargetCapabilities::NONE,
                },
                behavior: Arc::new(Probe(seen.clone())),
                argument_completions: Vec::new(),
            }])
            .unwrap();
        let queued = command_runtime
            .registry
            .dispatch_input(&first.app.command_target, "/admission-race".into());
        command_runtime.sync_admissions(std::collections::HashMap::from([(
            target_id,
            maki_lua::orchestration::TrustedTarget {
                target: second_manager.lookup(second_id).unwrap(),
                template: second.handles.orchestration_template.clone(),
            },
        )]));
        smol::block_on(queued);
        assert_eq!(*seen.lock().unwrap(), Some(first_id));
        release_runtime(first);
        release_runtime(second);
    }

    #[test]
    fn committed_replacement_restores_outgoing_accepted_theme_preview() {
        const ORIGINAL_THEME: &str = "dracula";
        const PREVIEW_THEME: &str = "tokyonight";

        let _guard = crate::theme::theme_test_guard();
        let harness = RuntimeHarness::new();
        let mut runtime = harness.runtime(harness.session());
        let provider = Arc::clone(&runtime.app.theme_provider);
        provider.select(ORIGINAL_THEME);
        let target = runtime.app.command_target.id();
        let command = runtime
            .app
            .command_runtime
            .registry
            .resolve_for(&runtime.app.command_target, "/theme")
            .unwrap();
        let completion = runtime
            .app
            .command_runtime
            .registry
            .open_completion(command, target)
            .unwrap();
        let maki_commands::CompletionResult::Items(candidates) = smol::block_on(
            completion.complete(Arc::from(""), Arc::from(""), 0, Arc::from("insert")),
        ) else {
            panic!("expected theme completion items");
        };
        let candidate = candidates
            .iter()
            .find(|candidate| candidate.item().insertion.as_ref() == PREVIEW_THEME)
            .unwrap();
        completion.highlight(candidate).unwrap();
        completion.accept(candidate.clone()).unwrap();
        assert_eq!(provider.current_theme_name(), PREVIEW_THEME);

        let prepared = harness.prepare();
        let old = replace_session_runtime(
            &mut runtime,
            prepared,
            &harness.ctx().sessions_dir,
            &harness.ctx().model_slot,
        )
        .unwrap();

        assert_eq!(provider.current_theme_name(), ORIGINAL_THEME);
        release_runtime(old);
        release_runtime(runtime);
    }

    #[test]
    fn replacement_preserves_exit_on_done() {
        let harness = RuntimeHarness::new();
        let mut runtime = harness.runtime(harness.session());
        runtime.app.exit_on_done = true;
        let prepared = harness.prepare();

        let old = replace_session_runtime(
            &mut runtime,
            prepared,
            &harness.ctx().sessions_dir,
            &harness.ctx().model_slot,
        )
        .unwrap();

        assert!(runtime.app.exit_on_done);
        release_runtime(old);
        release_runtime(runtime);
    }

    #[test]
    fn lost_session_lock_stops_without_saving() {
        let harness = RuntimeHarness::new();
        let mut runtime = harness.runtime(harness.session());
        let path = session_lock::lock_path(&harness.ctx().sessions_dir, &runtime.id());
        std::fs::write(&path, format!("{} replacement", std::process::id())).unwrap();

        let lease = match runtime.session_lock.take().unwrap() {
            SessionLockState::Held(mut lease) => {
                assert_eq!(lease.heartbeat().unwrap(), session_lock::LockBeat::Lost);
                lease
            }
            SessionLockState::InFlight(_) => panic!("heartbeat was already in flight"),
        };
        apply_heartbeat_completion(
            &mut runtime,
            HeartbeatCompletion {
                result: Ok(session_lock::LockBeat::Lost),
                lease: Some(lease),
            },
        );

        assert!(runtime.lock_lost);
        assert!(runtime.session_lock.is_none());
        assert_eq!(runtime.app.exit_request, ExitRequest::Error);
        release_runtime(runtime);
    }

    #[test]
    fn prepared_replacement_cannot_reset_lock_lost_or_save() {
        let harness = RuntimeHarness::new();
        let mut session = harness.session();
        let id = session.id;
        let path = session_lock::lock_path(&harness.ctx().sessions_dir, &id);
        session.push_message(Message::user("kept".into()));
        session.push_message(Message::user("truncated".into()));
        session.save(&harness.ctx().storage).unwrap();
        let stored_before = AppSession::load(id, &harness.ctx().storage).unwrap();
        let mut runtime = harness.runtime(session.clone());
        let lease = match runtime.session_lock.take().unwrap() {
            SessionLockState::Held(lease) => lease,
            SessionLockState::InFlight(_) => panic!("heartbeat was already in flight"),
        };
        std::fs::write(&path, format!("{} replacement", std::process::id())).unwrap();
        runtime.lock_lost = true;
        runtime
            .app
            .state
            .session_mut()
            .push_message(Message::user("must not save".into()));
        session.truncate_messages(1);
        session.model = runtime.app.root_config().model.spec();
        let prepared = harness
            .ctx()
            .prepare_replacement_runtime(
                session,
                runtime.id(),
                runtime.app.permissions.as_ref(),
                runtime.handles.offload.as_ref(),
            )
            .unwrap();
        assert_eq!(
            serde_json::to_value(
                harness
                    .ctx()
                    .storage_writer
                    .latest_snapshot(id)
                    .unwrap()
                    .messages()
            )
            .unwrap(),
            serde_json::to_value(stored_before.messages()).unwrap()
        );

        let error = match replace_session_runtime(
            &mut runtime,
            prepared,
            &harness.ctx().sessions_dir,
            &harness.ctx().model_slot,
        ) {
            Ok(old) => {
                release_runtime(old);
                panic!("lock-lost replacement must fail");
            }
            Err(error) => error,
        };

        assert_eq!(error, LOCK_LOST_REPLACEMENT_ERR);
        assert!(runtime.lock_lost);
        assert!(runtime.session_lock.is_none());
        checkpoint_runtime(&mut runtime);
        let (done_tx, done_rx) = flume::bounded(1);
        harness
            .ctx()
            .storage_writer
            .delete(MakiId::generate(), move |_| done_tx.send(()).unwrap());
        done_rx.recv_timeout(RUNTIME_SHUTDOWN_TIMEOUT).unwrap();
        assert_eq!(
            serde_json::to_value(
                AppSession::load(id, &harness.ctx().storage)
                    .unwrap()
                    .messages()
            )
            .unwrap(),
            serde_json::to_value(stored_before.messages()).unwrap()
        );
        lease.release().unwrap();
        release_runtime(runtime);
    }

    #[test]
    fn heartbeat_tick_then_delayed_notification_keeps_lock_held() {
        with_event_loop(|event_loop| {
            let id = event_loop.sessions[0].id();
            let generation = event_loop.sessions[0].generation;
            let (entered, entry) = flume::bounded(1);
            let (release, gate) = flume::bounded(1);
            start_runtime_heartbeat_with(
                &mut event_loop.sessions[0],
                &event_loop.internal_tx,
                move |lease| {
                    entered.send(()).unwrap();
                    gate.recv().unwrap();
                    (lease, Ok(session_lock::LockBeat::Held))
                },
            );
            entry.recv_timeout(AGENT_SHUTDOWN_TIMEOUT).unwrap();
            release.send(()).unwrap();
            let event = event_loop
                .internal_rx
                .recv_timeout(AGENT_SHUTDOWN_TIMEOUT)
                .unwrap();
            assert!(matches!(event, InternalEvent::SessionHeartbeat(value) if value == generation));
            event_loop.last_heartbeat = Instant::now();
            let _ = event_loop.tick();
            assert!(matches!(
                event_loop.sessions[0].session_lock,
                Some(SessionLockState::Held(_))
            ));
            assert!(
                session_lock::claim(&event_loop.sessions_dir, &id)
                    .unwrap()
                    .is_none()
            );
            event_loop.handle_internal(event);
            event_loop.handle_internal(InternalEvent::SessionHeartbeat(generation));
            assert!(matches!(
                event_loop.sessions[0].session_lock,
                Some(SessionLockState::Held(_))
            ));
            assert!(!event_loop.sessions[0].lock_lost);
            assert!(
                session_lock::claim(&event_loop.sessions_dir, &id)
                    .unwrap()
                    .is_none()
            );
        });
    }

    #[test]
    fn heartbeat_runs_off_thread_and_only_one_is_in_flight() {
        let harness = RuntimeHarness::new();
        let mut runtime = harness.runtime(harness.session());
        let (internal_tx, internal_rx) = flume::unbounded();
        let (entered_tx, entered_rx) = flume::bounded(1);
        let (release_tx, release_rx) = flume::bounded(1);

        start_runtime_heartbeat_with(&mut runtime, &internal_tx, move |lease| {
            entered_tx.send(()).unwrap();
            release_rx.recv().unwrap();
            (lease, Ok(session_lock::LockBeat::Held))
        });
        entered_rx.recv().unwrap();
        assert!(matches!(
            runtime.session_lock,
            Some(SessionLockState::InFlight(_))
        ));

        let second_ran = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let second_probe = Arc::clone(&second_ran);
        start_runtime_heartbeat_with(&mut runtime, &internal_tx, move |lease| {
            second_probe.store(true, Ordering::SeqCst);
            (lease, Ok(session_lock::LockBeat::Held))
        });
        assert!(!second_ran.load(Ordering::SeqCst));

        release_tx.send(()).unwrap();
        let InternalEvent::SessionHeartbeat(generation) = internal_rx.recv().unwrap() else {
            panic!("expected heartbeat completion");
        };
        assert_eq!(generation, runtime.generation);
        let Some(SessionLockState::InFlight(task)) = runtime.session_lock.take() else {
            panic!("expected in-flight heartbeat task");
        };
        let completion = collect_heartbeat(task, None).unwrap();
        apply_heartbeat_completion(&mut runtime, completion);
        assert!(matches!(
            runtime.session_lock,
            Some(SessionLockState::Held(_))
        ));
        release_runtime(runtime);
    }

    #[test]
    fn same_id_replacement_rejects_in_flight_lease() {
        const OUTGOING_DRAFT: &str = "draft before replacement";
        const RESTORED_DRAFT: &str = "replacement restored prompt";

        let harness = RuntimeHarness::new();
        let mut session = harness.session();
        let id = session.id;
        let mut runtime = harness.runtime(session.clone());
        runtime.app.input_box.set_input(OUTGOING_DRAFT.into());
        session.meta.input_draft = Some(RESTORED_DRAFT.into());
        let generation = runtime.generation;
        let target = runtime.app.command_target.id();
        let (manager, root) = runtime.handles.manager_and_root();
        let provider = harness.ctx().model_slot.load().provider.identity();
        let target_count = harness.target_count();
        let (internal_tx, internal_rx) = flume::unbounded();
        let (entered_tx, entered_rx) = flume::bounded(1);
        let (release_tx, release_rx) = flume::bounded(1);
        start_runtime_heartbeat_with(&mut runtime, &internal_tx, move |lease| {
            entered_tx.send(()).unwrap();
            release_rx.recv().unwrap();
            (lease, Ok(session_lock::LockBeat::Held))
        });
        entered_rx.recv().unwrap();

        assert_eq!(
            ensure_replacement_lock_available(&runtime, id),
            Err(LOCK_UNAVAILABLE_REPLACEMENT_ERR)
        );
        assert_eq!(runtime.generation, generation);
        assert_eq!(runtime.app.command_target.id(), target);
        assert_eq!(
            runtime.handles.manager_and_root().0.generation(),
            manager.generation()
        );
        assert_eq!(runtime.handles.manager_and_root().1, root);
        assert_eq!(
            harness.ctx().model_slot.load().provider.identity(),
            provider
        );
        assert_eq!(harness.target_count(), target_count);
        assert_eq!(runtime.app.input_box.buffer.value(), OUTGOING_DRAFT);
        assert!(matches!(
            runtime.session_lock,
            Some(SessionLockState::InFlight(_))
        ));

        release_tx.send(()).unwrap();
        let InternalEvent::SessionHeartbeat(event_generation) = internal_rx.recv().unwrap() else {
            panic!("expected heartbeat completion");
        };
        assert_eq!(event_generation, generation);
        complete_runtime_heartbeat(&mut runtime);
        assert!(matches!(
            runtime.session_lock,
            Some(SessionLockState::Held(_))
        ));
        ensure_replacement_lock_available(&runtime, id).unwrap();

        let prepared = harness
            .ctx()
            .prepare_runtime_with_provider_and_permissions(
                session,
                None,
                runtime.app.permissions.as_ref(),
                false,
            )
            .unwrap();
        let old = replace_session_runtime(
            &mut runtime,
            prepared,
            &harness.ctx().sessions_dir,
            &harness.ctx().model_slot,
        )
        .unwrap();
        assert_eq!(runtime.id(), id);
        assert_ne!(runtime.generation, generation);
        assert_eq!(runtime.app.input_box.buffer.value(), RESTORED_DRAFT);
        release_runtime(old);
        release_runtime(runtime);
    }

    #[test]
    fn graceful_cleanup_joins_in_flight_heartbeat_and_releases_lock() {
        let harness = RuntimeHarness::new();
        let session = harness.session();
        let id = session.id;
        let mut runtime = harness.runtime(session.clone());
        let (internal_tx, _internal_rx) = flume::unbounded();
        let (entered_tx, entered_rx) = flume::bounded(1);
        let (release_tx, release_rx) = flume::bounded(1);
        start_runtime_heartbeat_with(&mut runtime, &internal_tx, move |lease| {
            entered_tx.send(()).unwrap();
            release_rx.recv().unwrap();
            (lease, Ok(session_lock::LockBeat::Held))
        });
        entered_rx.recv().unwrap();

        let prepared = harness.ctx().prepare_runtime(session).unwrap();
        let error = match replace_session_runtime(
            &mut runtime,
            prepared,
            &harness.ctx().sessions_dir,
            &harness.ctx().model_slot,
        ) {
            Ok(old) => {
                release_runtime(old);
                panic!("in-flight same-id replacement must fail");
            }
            Err(error) => error,
        };
        assert_eq!(error, LOCK_UNAVAILABLE_REPLACEMENT_ERR);
        assert!(matches!(
            runtime.session_lock,
            Some(SessionLockState::InFlight(_))
        ));

        let state = runtime.session_lock.take();
        let (cleanup_started_tx, cleanup_started_rx) = flume::bounded(1);
        let (cleanup_done_tx, cleanup_done_rx) = flume::bounded(1);
        let cleanup = std::thread::spawn(move || {
            cleanup_started_tx.send(()).unwrap();
            let result = settle_lock_state(state, AGENT_SHUTDOWN_TIMEOUT);
            cleanup_done_tx.send(result).unwrap();
        });
        cleanup_started_rx.recv().unwrap();
        assert!(cleanup_done_rx.try_recv().is_err());

        release_tx.send(()).unwrap();
        let LockSettlement::Held(lease) = cleanup_done_rx.recv().unwrap() else {
            panic!("heartbeat cleanup did not retain held lease");
        };
        cleanup.join().unwrap();
        assert!(
            session_lock::claim(&harness.ctx().sessions_dir, &id)
                .unwrap()
                .is_none(),
            "settlement released the held lease"
        );
        lease.release().unwrap();
        assert!(!session_lock::open_elsewhere(
            &harness.ctx().sessions_dir,
            &id
        ));
        session_lock::claim(&harness.ctx().sessions_dir, &id)
            .unwrap()
            .unwrap()
            .release()
            .unwrap();
        release_runtime(runtime);
    }

    #[test]
    fn shutdown_retains_held_lock_through_storage_drain() {
        const FINAL_MESSAGE: &str = "final checkpoint";

        let mut harness = RuntimeHarness::new();
        let mut runtime = harness.runtime(harness.session());
        let id = runtime.id();
        runtime.app.input_box.set_input(FINAL_MESSAGE.into());
        let LockSettlement::Held(lease) =
            settle_lock_state(runtime.session_lock.take(), RUNTIME_SHUTDOWN_TIMEOUT)
        else {
            panic!("runtime must retain its held lease");
        };
        runtime.app.checkpoint_now();

        let manager = runtime.handles.manager_and_root().0;
        drop(runtime);
        shutdown_manager(&manager);
        let ctx = harness.ctx.take().unwrap();
        let storage = ctx.storage.clone();
        let sessions_dir = storage.path().join(SESSIONS_DIR);
        let storage_writer = Arc::clone(&ctx.storage_writer);
        drop(ctx);
        assert!(
            session_lock::claim(&sessions_dir, &id).unwrap().is_none(),
            "lock released before storage drain"
        );
        Arc::try_unwrap(storage_writer)
            .unwrap_or_else(|_| panic!("test owns storage writer"))
            .shutdown(RUNTIME_SHUTDOWN_TIMEOUT);
        assert!(
            session_lock::claim(&sessions_dir, &id).unwrap().is_none(),
            "lock released by storage drain"
        );

        lease.release().unwrap();
        let claimed = session_lock::claim(&sessions_dir, &id)
            .unwrap()
            .expect("lock claim succeeds after release");
        claimed.release().unwrap();
        let stored = AppSession::load(id, &storage).unwrap();
        assert_eq!(stored.meta.input_draft.as_deref(), Some(FINAL_MESSAGE));
    }

    #[test]
    fn shutdown_does_not_checkpoint_after_in_flight_heartbeat_loses_lock() {
        const BASELINE: &str = "stored before heartbeat";
        const UNSAVED: &str = "must not reach storage";

        let mut harness = RuntimeHarness::new();
        let mut session = harness.session();
        let id = session.id;
        session.push_message(Message::user(BASELINE.into()));
        session.save(&harness.ctx().storage).unwrap();
        let mut runtime = harness.runtime(session);
        runtime
            .app
            .state
            .session_mut()
            .push_message(Message::user(UNSAVED.into()));
        let (internal_tx, _internal_rx) = flume::unbounded();
        let (entered_tx, entered_rx) = flume::bounded(1);
        let (heartbeat_release_tx, heartbeat_release_rx) = flume::bounded(1);
        start_runtime_heartbeat_with(&mut runtime, &internal_tx, move |lease| {
            entered_tx.send(()).unwrap();
            heartbeat_release_rx.recv().unwrap();
            (lease, Ok(session_lock::LockBeat::Lost))
        });
        entered_rx.recv().unwrap();

        let state = runtime.session_lock.take();
        let (cleanup_done_tx, cleanup_done_rx) = flume::bounded(1);
        let cleanup = std::thread::spawn(move || {
            cleanup_done_tx
                .send(settle_lock_state(state, AGENT_SHUTDOWN_TIMEOUT))
                .unwrap();
        });
        assert!(cleanup_done_rx.try_recv().is_err());
        heartbeat_release_tx.send(()).unwrap();
        assert!(matches!(
            cleanup_done_rx.recv().unwrap(),
            LockSettlement::Lost
        ));
        cleanup.join().unwrap();

        let manager = runtime.handles.manager_and_root().0;
        drop(runtime);
        shutdown_manager(&manager);
        let ctx = harness.ctx.take().unwrap();
        let storage = ctx.storage.clone();
        let storage_writer = Arc::clone(&ctx.storage_writer);
        drop(ctx);
        Arc::try_unwrap(storage_writer)
            .unwrap_or_else(|_| panic!("test owns storage writer"))
            .shutdown(RUNTIME_SHUTDOWN_TIMEOUT);

        let stored = AppSession::load(id, &storage).unwrap();
        assert_eq!(stored.messages().len(), 1);
        assert_eq!(stored.messages()[0].user_text(), Some(BASELINE));
    }

    #[test]
    fn shutdown_does_not_checkpoint_when_in_flight_heartbeat_times_out() {
        const BASELINE: &str = "stored before heartbeat";
        const UNSAVED: &str = "must not reach storage";

        let mut harness = RuntimeHarness::new();
        let mut session = harness.session();
        let id = session.id;
        session.push_message(Message::user(BASELINE.into()));
        session.save(&harness.ctx().storage).unwrap();
        let mut runtime = harness.runtime(session);
        runtime
            .app
            .state
            .session_mut()
            .push_message(Message::user(UNSAVED.into()));
        let (internal_tx, _internal_rx) = flume::unbounded();
        let (entered_tx, entered_rx) = flume::bounded(1);
        let (heartbeat_release_tx, heartbeat_release_rx) = flume::bounded(1);
        start_runtime_heartbeat_with(&mut runtime, &internal_tx, move |lease| {
            entered_tx.send(()).unwrap();
            heartbeat_release_rx.recv().unwrap();
            (lease, Ok(session_lock::LockBeat::Held))
        });
        entered_rx.recv().unwrap();

        let outcome = settle_lock_state(runtime.session_lock.take(), Duration::ZERO);
        assert!(matches!(&outcome, LockSettlement::TimedOut));
        heartbeat_release_tx.send(()).unwrap();
        smol::block_on(async {
            while session_lock::open_elsewhere(&harness.ctx().sessions_dir, &id) {
                smol::future::yield_now().await;
            }
        });

        let manager = runtime.handles.manager_and_root().0;
        drop(runtime);
        shutdown_manager(&manager);
        let ctx = harness.ctx.take().unwrap();
        let storage = ctx.storage.clone();
        let storage_writer = Arc::clone(&ctx.storage_writer);
        drop(ctx);
        Arc::try_unwrap(storage_writer)
            .unwrap_or_else(|_| panic!("test owns storage writer"))
            .shutdown(RUNTIME_SHUTDOWN_TIMEOUT);

        let stored = AppSession::load(id, &storage).unwrap();
        assert_eq!(stored.messages().len(), 1);
        assert_eq!(stored.messages()[0].user_text(), Some(BASELINE));
    }

    #[test]
    fn stale_heartbeat_signal_cannot_poison_replacement() {
        let harness = RuntimeHarness::new();
        let stale = harness.runtime(harness.session());
        let stale_generation = stale.generation;
        let replacement = harness.runtime(harness.session());
        let replacement_generation = replacement.generation;
        let replacement_id = replacement.id();
        let mut runtimes = vec![replacement];

        assert!(
            runtimes
                .iter_mut()
                .find(|runtime| runtime.generation == stale_generation)
                .is_none()
        );
        assert_eq!(runtimes[0].generation, replacement_generation);
        assert_eq!(runtimes[0].id(), replacement_id);
        assert!(!runtimes[0].lock_lost);
        assert!(matches!(
            runtimes[0].session_lock,
            Some(SessionLockState::Held(_))
        ));
        release_runtime(stale);
        release_runtime(runtimes.pop().unwrap());
    }

    #[test]
    fn startup_rollback_releases_previously_claimed_locks() {
        let harness = RuntimeHarness::new();
        let first = harness.runtime(harness.session());
        let second = harness.runtime(harness.session());
        let first_id = first.id();
        let second_id = second.id();

        rollback_startup_runtimes(vec![first, second]);

        assert!(!session_lock::open_elsewhere(
            &harness.ctx().sessions_dir,
            &first_id
        ));
        assert!(!session_lock::open_elsewhere(
            &harness.ctx().sessions_dir,
            &second_id
        ));
    }

    #[test]
    fn same_id_replacement_transfers_the_exact_lock_without_io() {
        let harness = RuntimeHarness::new();
        let session = harness.session();
        let id = session.id;
        let mut runtime = harness.runtime(session.clone());
        let path = session_lock::lock_path(&harness.ctx().sessions_dir, &id);
        let owner_before = std::fs::read(&path).unwrap();
        let prepared = harness.ctx().prepare_runtime(session).unwrap();

        let old = replace_session_runtime(
            &mut runtime,
            prepared,
            &harness.ctx().sessions_dir,
            &harness.ctx().model_slot,
        )
        .unwrap();

        assert!(old.session_lock.is_none());
        assert!(runtime.session_lock.is_some());
        assert_eq!(std::fs::read(&path).unwrap(), owner_before);
        release_runtime(old);
        release_runtime(runtime);
    }

    #[test]
    fn different_id_activation_releases_old_lock_only_after_swap() {
        let harness = RuntimeHarness::new();
        let mut runtime = harness.runtime(harness.session());
        let old_id = runtime.id();
        let old_path = session_lock::lock_path(&harness.ctx().sessions_dir, &old_id);
        let target = harness.session();
        let target_id = target.id;
        let target_path = session_lock::lock_path(&harness.ctx().sessions_dir, &target_id);
        let prepared = harness.ctx().prepare_runtime(target).unwrap();

        let mut old = replace_session_runtime(
            &mut runtime,
            prepared,
            &harness.ctx().sessions_dir,
            &harness.ctx().model_slot,
        )
        .unwrap();

        assert_eq!(runtime.id(), target_id);
        assert!(runtime.session_lock.is_some());
        assert!(target_path.exists());
        assert!(old.session_lock.is_some());
        assert!(old_path.exists());
        release_lock_state(old.session_lock.take()).unwrap();
        assert!(!session_lock::open_elsewhere(
            &harness.ctx().sessions_dir,
            &old_id
        ));
        assert!(
            session_lock::claim(&harness.ctx().sessions_dir, &old_id)
                .unwrap()
                .is_some()
        );
        assert!(target_path.exists());
        release_runtime(old);
        release_runtime(runtime);
    }

    #[test]
    fn different_id_claim_failure_preserves_the_complete_runtime() {
        let harness = RuntimeHarness::new();
        let mut runtime = harness.runtime(harness.session());
        let old_id = runtime.id();
        let old_target = runtime.app.command_target.id();
        let (old_manager, old_root) = runtime.handles.manager_and_root();
        let old_provider = harness.ctx().model_slot.load().provider.identity();
        let old_target_count = harness.target_count();
        let old_lock_path = session_lock::lock_path(&harness.ctx().sessions_dir, &old_id);
        let old_owner = std::fs::read(&old_lock_path).unwrap();
        let target = harness.session();
        let target_id = target.id;
        let target_path = session_lock::lock_path(&harness.ctx().sessions_dir, &target_id);
        std::fs::write(&target_path, "4294967294").unwrap();
        let prepared = harness.ctx().prepare_runtime(target).unwrap();
        let (_, _, candidate_manager, _) = prepared.snapshot();

        let error = match replace_session_runtime(
            &mut runtime,
            prepared,
            &harness.ctx().sessions_dir,
            &harness.ctx().model_slot,
        ) {
            Ok(old) => {
                release_runtime(old);
                panic!("foreign target lock must reject replacement");
            }
            Err(error) => error,
        };

        assert!(error.contains(session_lock::OPEN_ELSEWHERE_MSG));
        assert_eq!(runtime.id(), old_id);
        assert_eq!(runtime.app.command_target.id(), old_target);
        assert_eq!(
            runtime.handles.manager_and_root().0.generation(),
            old_manager.generation()
        );
        assert_eq!(runtime.handles.manager_and_root().1, old_root);
        assert_eq!(
            harness.ctx().model_slot.load().provider.identity(),
            old_provider
        );
        assert_eq!(harness.target_count(), old_target_count);
        assert_eq!(std::fs::read(&old_lock_path).unwrap(), old_owner);
        assert_eq!(std::fs::read_to_string(&target_path).unwrap(), "4294967294");
        shutdown_manager(&candidate_manager);
        std::fs::remove_file(target_path).unwrap();
        release_runtime(runtime);
    }

    const MIDDLE_SCROLL_STEP: Duration = Duration::from_millis(100);
    const MIDDLE_SCROLL_CADENCE: Duration = Duration::from_millis(25);
    const MIDDLE_SCROLL_START: u16 = 60;
    const MIDDLE_SCROLL_LINES: u16 = 12;
    const MIDDLE_SCROLL_WIDTH: u16 = 80;
    const MIDDLE_SCROLL_HEIGHT: u16 = 60;

    fn middle_scroll_app() -> App {
        let mut app = crate::app::tests::test_app();
        app.main_chat()
            .push_user_message("transcript row\n".repeat(200));
        let mut terminal =
            Terminal::new(TestBackend::new(MIDDLE_SCROLL_WIDTH, MIDDLE_SCROLL_HEIGHT)).unwrap();
        terminal.draw(|frame| app.view(frame)).unwrap();
        app.set_scroll_top(MIDDLE_SCROLL_START);
        app
    }

    fn activate_middle_scroll(app: &mut App) -> Instant {
        let area = app.zones.find(SelectionZone::Messages).unwrap().area;
        let anchor = area.bottom() - 2;
        for (kind, row) in [
            (MouseEventKind::Down(MouseButton::Middle), anchor),
            (MouseEventKind::Up(MouseButton::Middle), anchor),
            (MouseEventKind::Moved, area.y),
        ] {
            app.update(Msg::Mouse(CtMouseEvent {
                kind,
                column: area.x + 2,
                row,
                modifiers: KeyModifiers::NONE,
            }));
        }
        assert_eq!(app.cadence().frame(), Some(MIDDLE_SCROLL_CADENCE));
        Instant::now()
    }

    #[test_case(Event::FocusLost ; "focus_loss")]
    #[test_case(Event::Resize(MIDDLE_SCROLL_WIDTH, MIDDLE_SCROLL_HEIGHT) ; "resize")]
    fn middle_scroll_lifecycle_routing(event: Event) {
        let mut outgoing = middle_scroll_app();
        let baseline = outgoing.cadence();
        let now = activate_middle_scroll(&mut outgoing);
        route_terminal_lifecycle(&mut outgoing, &event);
        route_terminal_lifecycle(&mut outgoing, &Event::FocusGained);
        let _ = tick_session(&mut outgoing, true, now + MIDDLE_SCROLL_STEP);
        assert_eq!(outgoing.main_chat().scroll_top(), MIDDLE_SCROLL_START);
        assert_eq!(outgoing.cadence(), baseline);

        let now = activate_middle_scroll(&mut outgoing);
        let mut focused = 0;
        assign_session_focus(&mut outgoing, &mut focused, 0);
        assert_eq!(outgoing.cadence().frame(), Some(MIDDLE_SCROLL_CADENCE));
        assign_session_focus(&mut outgoing, &mut focused, 1);
        assert_eq!(focused, 1);
        let mut incoming = middle_scroll_app();
        assign_session_focus(&mut incoming, &mut focused, 0);
        let _ = tick_session(&mut outgoing, true, now + MIDDLE_SCROLL_STEP);
        assert_eq!(outgoing.main_chat().scroll_top(), MIDDLE_SCROLL_START);
        assert_eq!(outgoing.cadence(), baseline);

        let now = activate_middle_scroll(&mut outgoing);
        activate_middle_scroll(&mut incoming);
        let mut terminal_focused = true;
        prepare_terminal_handoff([&mut outgoing, &mut incoming], &mut terminal_focused);
        assert!(!terminal_focused);
        for app in [&mut outgoing, &mut incoming] {
            route_terminal_lifecycle(app, &Event::FocusGained);
            let _ = tick_session(app, true, now + MIDDLE_SCROLL_STEP);
            assert_eq!(app.main_chat().scroll_top(), MIDDLE_SCROLL_START);
            assert_eq!(app.cadence(), baseline);
        }
    }

    #[test]
    fn middle_scroll_cadence_and_focus() {
        let mut focused = middle_scroll_app();
        let mut background = middle_scroll_app();
        let baseline = focused.cadence();
        assert_ne!(baseline.frame(), Some(MIDDLE_SCROLL_CADENCE));
        activate_middle_scroll(&mut focused);
        let now = activate_middle_scroll(&mut background);
        let next = now + MIDDLE_SCROLL_STEP;
        let (dirty, _) = tick_session(&mut focused, true, next);
        let _ = tick_session(&mut background, false, next);
        assert_eq!(dirty, Dirty::YES);
        assert_eq!(
            focused.main_chat().scroll_top(),
            MIDDLE_SCROLL_START - MIDDLE_SCROLL_LINES
        );
        assert_eq!(background.main_chat().scroll_top(), MIDDLE_SCROLL_START);
        let _ = tick_session(&mut focused, true, next);
        assert_eq!(
            focused.main_chat().scroll_top(),
            MIDDLE_SCROLL_START - MIDDLE_SCROLL_LINES
        );
        let _ = tick_session(&mut focused, true, next + MIDDLE_SCROLL_STEP);
        assert_eq!(
            focused.main_chat().scroll_top(),
            MIDDLE_SCROLL_START - 2 * MIDDLE_SCROLL_LINES
        );
        route_terminal_lifecycle(&mut focused, &Event::FocusLost);
        assert_eq!(focused.cadence(), baseline);
        let _ = tick_session(&mut focused, true, next + 2 * MIDDLE_SCROLL_STEP);
        assert_eq!(
            focused.main_chat().scroll_top(),
            MIDDLE_SCROLL_START - 2 * MIDDLE_SCROLL_LINES
        );
    }

    #[test]
    fn live_session_response_includes_main_message_count() {
        let mut app = crate::app::tests::test_app();
        app.state
            .session_mut()
            .push_message(Message::observation("kept".into()));
        let row = live_session_row(app.state.session.id, &app, true);
        assert_eq!(
            row["message_count"], 1,
            "no mirror falls back to the session"
        );

        let messages = vec![
            Message::observation("one".into()),
            Message::observation("two".into()),
        ];
        let mirror: maki_agent::SharedMessages = Arc::new(arc_swap::ArcSwap::from_pointee(
            maki_agent::HistorySnapshot::new(messages),
        ));
        let mut app = crate::app::tests::test_app();
        app.shared_history = Some(mirror);
        let row = live_session_row(app.state.session.id, &app, true);
        assert_eq!(
            row["message_count"], 2,
            "the live mirror wins over the checkpoint"
        );
        assert_eq!(row["focused"], true);
        assert_eq!(row["status"], "idle");
    }

    struct FakeHost;

    impl maki_commands::CommandHost for FakeHost {
        fn request(
            &self,
            _request: maki_commands::HostRequest,
        ) -> maki_commands::CommandFuture<Result<HostResponse, CommandError>> {
            Box::pin(async { Ok(HostResponse::Completed) })
        }
    }

    fn done_event() -> AgentEvent {
        AgentEvent::TurnOutcome(TurnOutcome::completed(
            AgentId::generate(),
            TurnId::generate(),
            TokenUsage::default(),
            1,
            DoneReason::EndTurn,
        ))
    }

    fn due_completion() -> RunNotificationState {
        let mut state = RunNotificationState::default();
        state.on_done(&done_event());
        state.on_drain();
        state
    }

    #[test]
    fn command_routing_rejects_retired_target_after_session_replacement() {
        let registry = maki_commands::CommandRegistry::new();
        let retired = registry
            .bind_target(
                maki_commands::TargetCapabilities::default(),
                Arc::new(FakeHost),
            )
            .id();
        let live = registry
            .bind_target(
                maki_commands::TargetCapabilities::default(),
                Arc::new(FakeHost),
            )
            .id();

        assert_eq!(command_target_index([live], retired), None);
        assert_eq!(command_target_index([live], live), Some(0));
    }

    #[test]
    fn completion_waits_for_queue_drain() {
        let mut state = RunNotificationState {
            response_candidate: Some("done".into()),
            ..RunNotificationState::default()
        };

        state.on_done(&done_event());
        assert!(state.waiting_for_drain());
        assert_eq!(
            state.reconcile(None, SessionStatus::Idle, true, false),
            None
        );

        state.on_drain();
        assert!(!state.waiting_for_drain());
        assert_eq!(
            state.reconcile(None, SessionStatus::Idle, true, false),
            Some(Notification::TurnComplete {
                response: Some("done".into())
            })
        );
    }

    #[test_case(SessionStatus::Idle, true, false, true ; "fires_when_settled_and_unfocused")]
    #[test_case(SessionStatus::Idle, true, true, false ; "focused_terminal_swallows")]
    #[test_case(SessionStatus::Idle, false, false, false ; "queued_message_swallows")]
    #[test_case(SessionStatus::Working, true, false, false ; "busy_session_swallows")]
    fn due_completion_is_decided_on_first_reconcile(
        status: SessionStatus,
        queue_empty: bool,
        terminal_focused: bool,
        fires: bool,
    ) {
        let mut state = due_completion();

        let first = state.reconcile(None, status, queue_empty, terminal_focused);
        assert_eq!(first.is_some(), fires);
        assert_eq!(
            state.reconcile(None, SessionStatus::Idle, true, false),
            None
        );
    }

    #[test]
    fn prompt_wins_over_completion_and_is_not_repeated_unchanged() {
        let prompt = Notification::QuestionRequested;
        let mut state = due_completion();

        assert_eq!(
            state.reconcile(Some(prompt.clone()), SessionStatus::Idle, true, false),
            Some(prompt.clone())
        );
        assert_eq!(
            state.reconcile(Some(prompt), SessionStatus::NeedsInput, true, false),
            None
        );
        assert_eq!(
            state.reconcile(None, SessionStatus::Idle, true, false),
            None
        );
    }

    #[test]
    fn notification_selection_prefers_priority_then_session_order() {
        let completion = Notification::TurnComplete { response: None };
        let first_prompt = Notification::QuestionRequested;
        let second_prompt = Notification::AuthenticationRequired;

        let selected = select_notification(None, Some(completion));
        let selected = select_notification(selected, Some(first_prompt.clone()));
        let selected = select_notification(selected, Some(second_prompt));

        assert_eq!(selected, Some(first_prompt));
    }

    #[cfg(not(windows))]
    #[test]
    fn focus_events_map_to_terminal_focus_state() {
        assert_eq!(terminal_focus_event(&Event::FocusGained), Some(true));
        assert_eq!(terminal_focus_event(&Event::FocusLost), Some(false));
        assert_eq!(terminal_focus_event(&Event::Resize(80, 24)), None);
    }

    #[cfg(not(windows))]
    #[test]
    fn interactive_input_proves_terminal_focus() {
        let release = crossterm::event::KeyEvent {
            code: crossterm::event::KeyCode::Enter,
            modifiers: crossterm::event::KeyModifiers::NONE,
            kind: KeyEventKind::Release,
            state: crossterm::event::KeyEventState::NONE,
        };

        assert!(terminal_input_proves_focus(&Event::Key(
            crate::components::key(crossterm::event::KeyCode::Enter)
        )));
        assert!(terminal_input_proves_focus(&Event::Paste("text".into())));
        assert!(!terminal_input_proves_focus(&Event::Key(release)));
        assert!(!terminal_input_proves_focus(&Event::Resize(80, 24)));
    }

    #[test]
    fn shell_results_do_not_replace_existing_preamble() {
        let mut preamble = vec![Message::observation(OBSERVATION.into())];

        prepend_preamble(
            &mut preamble,
            vec![Message::observation(SHELL_RESULT.into())],
        );

        let text = preamble.iter().map(Message::user_text).collect::<Vec<_>>();
        assert_eq!(text, [Some(SHELL_RESULT), Some(OBSERVATION)]);
    }
}
