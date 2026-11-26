//! Multi-session supervisor: every session owns an `App` + `RuntimeChannels` and
//! keeps draining agent events while backgrounded; only the focused session
//! renders and receives input. `SpawnCtx` carries the shared resources needed
//! to spawn session runtimes at any point.
//!
//! Terminal input arrives on a channel (see [`InputReader`]), so the loop
//! waits on every event source at once and wakes the moment a plugin action,
//! agent event, or keypress arrives instead of sleeping in `event::poll`.

use makima_domain::LockExt;
use std::collections::HashMap;
use std::future::{Future, pending, poll_fn};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::task::Poll;
use std::time::{Duration, Instant};

use color_eyre::Result;
use color_eyre::eyre::eyre;

use arc_swap::ArcSwap;
use crossterm::event::{
    Event, KeyEventKind, MouseButton, MouseEvent as CtMouseEvent, MouseEventKind,
};
use futures_lite::future::or;
use makima_agent::actor::{
    ActorEvent, ConfigChange, ConfigCommit, PreparedModel, PreparedTransaction,
};
use makima_agent::command::{CONTEXT_RELOADED, CustomCommand};
use makima_agent::config_change::{
    ConfigChangeError, ConfigChangeTicket, prepare_model, prepare_selection,
};
#[cfg(test)]
use makima_agent::context::root_context;
use makima_agent::end_conversation_detached;
use makima_agent::headless::{FrontendProfile, InteractiveParams, default_model_preparer};
use makima_agent::journal::SessionJournalSnapshot;
use makima_agent::manager::AgentResidency;
#[cfg(test)]
use makima_agent::permissions::PermissionAnswer;
use makima_agent::permissions::PermissionManager;
use makima_agent::rewind::RewindOutcome;
use makima_agent::session::{
    self, Durability, FrontendServices, OpenRequest, PrepareOptions, PreparedSession, ResetSeed,
    SessionEvent, SessionPersistence, SessionSource, set_title,
};
use makima_agent::session_coordinator::{
    ModelPreparer, SessionCoordinatorHandle, canonical_directory, publication_claim,
};
use makima_agent::session_options::{
    DISABLED_VALUE, ENABLED_VALUE, FAST_OPTION_ID, WORKFLOW_OPTION_ID,
};
use makima_agent::session_recovery::{OpenModel, continue_all_agents};
use makima_agent::tools::FileWriteLocks;
#[cfg(test)]
use makima_agent::tools::offload::OffloadStore;
use makima_agent::{
    AgentConfig, AgentEvent, AgentId, AgentRef, CancelToken, CancelTrigger, Envelope,
    GraphLifecycle, McpCommand, McpConfigErrors, McpHandle, McpSnapshotReader, mcp,
};
use makima_commands::{BuiltinOperation, CommandError, HostResponse};
use makima_config::project::TrustQuestion;
use makima_config::{ModelPolicy, ProjectConfig, SessionDefaults, UiConfig};
use makima_lua::orchestration::{
    AgentOrchestration, ConfigMutationReserver, DURABLE_CONFIG_UNAVAILABLE, Discovery,
    ModelCatalog, OrchestrationServices, PinnedCommandTarget, TrustedTarget,
};
use makima_lua::session_snapshot::{
    STATUS_IDLE, STATUS_NEEDS_INPUT, STATUS_WORKING, SessionSnapshot, session_usage,
};
use makima_lua::ui_sink::UiSink;
use makima_lua::{
    EventHandle, HintReader, KeymapReader, ProviderUsageAck, ProviderUsageInvalidation,
    ProviderUsageLimit, ProviderUsageReply, ProviderUsageSnapshot, ProviderUsageWindow,
    SessionRequest, StatusContentReader, UiAction, UiReply,
};
use makima_providers::Timeouts;
use makima_providers::provider::{BoxFuture, Provider};
use makima_providers::{
    ConversationEnd, Message, Model, ModelTier, PreparedProvider, ProviderRegistry,
};
use makima_storage::StateDir;
#[cfg(test)]
use makima_storage::StorageError;
use makima_storage::checkpoint::CheckpointPublication;
#[cfg(test)]
use makima_storage::id::SessionRef;
use makima_storage::id::{MakimaId, MakimaIdParseError};
use makima_storage::session_lock::{self, ClaimedSessionLock, SessionPublicationGuard};
use makima_storage::sessions::SESSIONS_DIR;
#[cfg(test)]
use makima_storage::sessions::{SessionError, SessionRecord};
use serde_json::json;
use tracing::{info, warn};

fn claim_lock(dir: &Path, id: &MakimaId) -> Result<ClaimedSessionLock> {
    session_lock::claim(dir, id)?.ok_or_else(|| eyre!(session_lock::OPEN_ELSEWHERE_MSG))
}

use crate::AppSession;
use crate::agent::pending::PendingProvider;
use crate::agent::{
    ProviderChange, ProviderSlot, RuntimeChannels, SystemPromptOverride,
    shared_queue::{QueueItem, QueueSender, actor_queue},
};
use crate::app::EMPTY_PROMPT_ERR;
use crate::app::shell::ShellEvent;
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
use crate::model_capabilities::ModelCapabilityCache;
use crate::provider_usage::{
    ProviderIdentity, ProviderUsageCoordinator, ProviderUsageFetch, ProviderUsageFetchId,
    ProviderUsageFetchResult, ProviderUsageInput, ProviderUsageOutput, ProviderUsageRequestKind,
    ProviderUsageResolution,
};
use crate::repaint::{Dirty, IDLE_POLL};

use crate::terminal;
use crate::theme::ThemesProvider;
#[cfg(test)]
use makima_agent::session_writer::SessionWriter;

mod approval;
#[cfg(test)]
mod discovery_tests;
#[cfg(test)]
mod focus_tests;

use approval::{ApprovalStep, PendingPlanApproval};

/// Max events handled per frame so a flood cannot starve rendering.
const DRAIN_BUDGET: usize = 256;
const GLOBAL_WAKE_SOURCES: usize = 6;
const SESSION_WAKE_SOURCES: usize = 3;
const AGENT_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(3);
const SESSION_OP_TIMEOUT: Duration = Duration::from_secs(30);
const SESSION_OP_TIMEOUT_ERR: &str = "session operation timed out";
const REWIND_UNCERTAIN: &str = "Rewind applied, but durability is uncertain";
const CONFIG_EXPIRED_ERR: &str = "configuration preparation expired";
const UNSUPPORTED_AGENT_OPTION_PREFIX: &str = "unsupported agent option";
const PROVIDER_USAGE_CHANGED_ERR: &str = "provider changed while fetching usage";
const PROVIDER_USAGE_SHUTDOWN_ERR: &str = "UI shut down while fetching usage";
const NOT_LIVE_ERR: &str = "session not live";
const NO_MAILBOX_ERR: &str = "no live session to move a tool call in";
const MOVED_BACKGROUND_PREFIX: &str = "Moved to the background: ";
const STARTUP_FALLBACK_NOT_ADOPTED: &str =
    "startup model fallback did not replace the pending provider";
const UNKNOWN_PROVIDER: &str = "Unknown";
#[cfg(test)]
const RECOVERY_ACTIVATION_FAILED: &str = "recovery activation failed";
const RUNTIME_GONE_ERR: &str = "session closed while its model was preparing";
const LOCK_LOST_MSG: &str = "Session lock lost to another process; stopping without saving";
const LOCK_LOST_REPLACEMENT_ERR: &str = "session lock was lost; replacement is disabled";
const LOCK_UNAVAILABLE_REPLACEMENT_ERR: &str =
    "session lock ownership is unavailable; replacement is disabled";

static NEXT_RUNTIME_GENERATION: AtomicU64 = AtomicU64::new(1);

pub(crate) struct ShutdownReport {
    pub exit: ExitRequest,
    pub tabs: Vec<Arc<SessionJournalSnapshot>>,
    pub focused: usize,
}

pub enum StartupSessions {
    Open(Box<AppSession>),
    Reload(Vec<MakimaId>),
}

pub struct EventLoopParams {
    pub cwd: PathBuf,
    pub model: Model,
    pub needs_login: bool,
    pub explicit_model: bool,
    pub commands: Vec<CustomCommand>,
    pub sessions: StartupSessions,
    pub focused: usize,
    pub startup_warnings: Vec<String>,
    pub storage: StateDir,
    pub config: AgentConfig,
    pub ui_config: UiConfig,
    pub input_history_size: usize,
    pub permissions: Arc<PermissionManager>,
    pub timeouts: Timeouts,
    pub exit_on_done: bool,
    pub command_registry: makima_commands::CommandRegistry,
    /// One-shot startup request to open the session picker (bare `-c`).
    pub session_picker: bool,
    pub keymap_reader: KeymapReader,
    pub hint_reader: HintReader,
    pub status_content_reader: StatusContentReader,
    pub ui_action_rx: flume::Receiver<UiAction>,
    pub lua_event_handle: EventHandle,
    pub model_policy: Arc<ModelPolicy>,
    pub providers: Arc<ProviderRegistry>,
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
            AgentEvent::TurnOutcome(makima_agent::TurnOutcome::Completed { .. })
            | AgentEvent::TurnOutcome(makima_agent::TurnOutcome::Cancelled { .. }) => {
                Notification::TurnComplete {
                    response: self.response_candidate.take(),
                }
            }
            AgentEvent::TurnOutcome(makima_agent::TurnOutcome::Failed { .. })
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
    let (mut dirty, actions) = app.poll_login_picker();
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

fn parse_session_id(id: &str) -> Result<MakimaId, String> {
    id.parse().map_err(|e: MakimaIdParseError| e.to_string())
}

enum SessionLockState {
    Held(ClaimedSessionLock),
    InFlight(flume::Receiver<HeartbeatCompletion>),
}

struct HeartbeatCompletion {
    result: io::Result<session_lock::LockBeat>,
    lease: Option<ClaimedSessionLock>,
}

impl SessionLockState {
    fn publication_guard(&self) -> Option<SessionPublicationGuard> {
        match self {
            Self::Held(lease) => Some(lease.publication_guard()),
            Self::InFlight(_) => None,
        }
    }
}

impl Drop for HeartbeatCompletion {
    fn drop(&mut self) {
        if let Some(lease) = self.lease.take() {
            let _ = lease.release();
        }
    }
}

fn mark_runtime_lock_lost(runtime: &mut SessionRuntime) {
    runtime.cancel_background_work();
    runtime.session_lock = None;
    runtime.lock_lost = true;
    runtime.end_plan_approval();
    runtime.app.flash(LOCK_LOST_MSG.into());
    runtime.app.exit_request = ExitRequest::Error;
    runtime.handles.cancel_root();
    warn!(id = %runtime.id(), "session lock lost to another process; stopping without saving");
}

fn stop_runtime_after_recovery_failure(runtime: &mut SessionRuntime, error: &str) {
    runtime.cancel_background_work();
    runtime.lock_lost = true;
    runtime.end_plan_approval();
    runtime.app.flash(error.to_owned());
    runtime.app.exit_request = ExitRequest::Error;
    runtime.coordinator.retire();
    let (manager, root) = runtime.handles.manager_and_root();
    if let Err(close_error) = manager.close_subtree(root) {
        warn!(%close_error, "closing runtime after recovery activation failed");
    }
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
    lease: Option<Arc<Mutex<ClaimedSessionLock>>>,
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
                match worker_lease.lock_unpoisoned().heartbeat() {
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
        Self {
            lease: Some(lease),
            wake,
            done,
        }
    }

    fn publication_guard(&self) -> session_lock::SessionPublicationGuard {
        self.lease
            .as_ref()
            .expect("held session lease")
            .lock_unpoisoned()
            .publication_guard()
    }
}

impl Drop for RenewingDeletionLease {
    fn drop(&mut self) {
        let _ = self.wake.send(());
        if self.lease.is_some() {
            let _ = self.done.recv_timeout(AGENT_SHUTDOWN_TIMEOUT);
        }
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
    modes: Arc<makima_agent::ModeRegistry>,
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

fn selected_graph_agent(runtime: &SessionRuntime) -> Option<AgentRef> {
    let (manager, _) = runtime.handles.manager_and_root();
    manager.lookup(runtime.app.selected_agent_id()?).ok()
}

fn selected_agent(runtime: &SessionRuntime) -> Option<AgentRef> {
    let target = selected_graph_agent(runtime)?;
    target.validate_live().ok()?;
    Some(target)
}

fn complete_option_toggle(app: &mut App, id: &'static str, enabled: bool) {
    app.apply_toggled_option(id, enabled);
}

fn rollback_startup_runtimes(mut runtimes: Vec<SessionRuntime>) {
    for runtime in &mut runtimes {
        runtime.cancel_background_work();
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
    lifetime_cancel: CancelToken,
    lifetime_trigger: Option<CancelTrigger>,
    config_commits: flume::Receiver<ConfigCommit>,
    /// The last root commit the app followed, so the next one can tell what
    /// changed.
    projected_config: Option<ConfigCommit>,
    app: App,
    handles: RuntimeChannels,
    model_slot: Arc<ProviderSlot>,
    coordinator: SessionCoordinatorHandle,
    checkpoint: SessionPersistence,
    _coordinator_retirement: CoordinatorRetirement,
    shell_tx: flume::Sender<ShellEvent>,
    shell_rx: flume::Receiver<ShellEvent>,
    last_status: SessionStatus,
    notifications: RunNotificationState,
    session_lock: Option<SessionLockState>,
    lock_lost: bool,
    restore_pending: bool,
    pending_approval: Option<PendingPlanApproval>,
    #[cfg(test)]
    test_storage: Option<tempfile::TempDir>,
}

/// Where a session goes once the model it needs is prepared.
enum RuntimeTarget {
    /// A new focused tab, answering the `makima.session.focus` that asked.
    Open { reply: flume::Sender<UiReply> },
    /// In place of the runtime `runtime` identifies.
    Replace {
        runtime: Arc<()>,
        kind: Option<SessionReplacementKind>,
        reply: Option<flume::Sender<UiReply>>,
    },
}

struct PreparedSessionRuntime {
    app: PreparedApp,
    prepared: PreparedSession,
    lease: Option<ClaimedSessionLock>,
    model_slot: Arc<ProviderSlot>,
    config_commits: flume::Receiver<ConfigCommit>,
    actor_rx: flume::Receiver<ActorEvent>,
    queue: QueueSender,
    cwd: Arc<ArcSwap<PathBuf>>,
    mcp_handle: Option<McpHandle>,
    provider: Option<PreparedProvider>,
    resumed: bool,
    #[cfg(test)]
    before_publish: Option<Box<dyn FnOnce() -> Result<(), String> + Send>>,
}

impl PreparedSessionRuntime {
    #[cfg(test)]
    fn snapshot(&self) -> (MakimaId, makima_commands::InvocationTargetId, AgentId) {
        (
            self.app.session_id(),
            self.app.command_target_id(),
            self.prepared.session().root().id(),
        )
    }

    fn take_lease(&mut self) -> Option<SessionLockState> {
        self.lease.take().map(SessionLockState::Held)
    }

    fn activate(
        mut self,
        startup_model_slot: &ProviderSlot,
        session_lock: Option<SessionLockState>,
    ) -> Result<SessionRuntime> {
        let lock = session_lock.or_else(|| self.take_lease());
        self.publish(startup_model_slot, lock)
            .map_err(|(error, _)| eyre!(error))
    }

    fn publish(
        self,
        startup_model_slot: &ProviderSlot,
        session_lock: Option<SessionLockState>,
    ) -> std::result::Result<SessionRuntime, (String, Option<SessionLockState>)> {
        let Some(guard) = session_lock
            .as_ref()
            .and_then(SessionLockState::publication_guard)
        else {
            return Err((LOCK_UNAVAILABLE_REPLACEMENT_ERR.into(), session_lock));
        };
        #[cfg(test)]
        if let Some(hook) = self.before_publish
            && let Err(error) = hook()
        {
            return Err((error, session_lock));
        }
        if let Err(error) = validate_publication_guard(&guard) {
            return Err((error, session_lock));
        }
        let checkpoint = self
            .prepared
            .session()
            .writer()
            .expect("persistent session");
        let session = match smol::block_on(self.prepared.publish()) {
            Ok(session) => session,
            Err(error) => return Err((error.to_string(), session_lock)),
        };
        if let Some(provider) = self.provider {
            startup_model_slot.install(provider.model, provider.provider);
        }
        let coordinator = session.coordinator.clone();
        let handles = RuntimeChannels::new(
            session,
            self.actor_rx,
            self.queue,
            self.cwd,
            self.mcp_handle,
        );
        let mut app = self.app.activate();
        handles.apply_to_app(&mut app);
        app.refresh_session_projection();
        app.coordinator = Some(coordinator.clone());
        let (lifetime_trigger, lifetime_cancel) = CancelToken::new();
        let (shell_tx, shell_rx) = flume::unbounded();
        Ok(SessionRuntime {
            generation: NEXT_RUNTIME_GENERATION.fetch_add(1, Ordering::Relaxed),
            lifetime_cancel,
            lifetime_trigger: Some(lifetime_trigger),
            config_commits: self.config_commits,
            projected_config: None,
            app,
            handles,
            model_slot: self.model_slot,
            _coordinator_retirement: CoordinatorRetirement(coordinator.clone()),
            coordinator,
            checkpoint,
            shell_tx,
            shell_rx,
            last_status: SessionStatus::Idle,
            notifications: RunNotificationState::default(),
            session_lock,
            lock_lost: false,
            restore_pending: self.resumed,
            pending_approval: None,
            #[cfg(test)]
            test_storage: None,
        })
    }

    #[cfg(test)]
    fn activate_replacing(
        mut self,
        startup_model_slot: &ProviderSlot,
        target_lock: Option<SessionLockState>,
        _current: &mut SessionRuntime,
    ) -> std::result::Result<SessionRuntime, (String, Option<SessionLockState>)> {
        let lock = target_lock.or_else(|| self.take_lease());
        self.publish(startup_model_slot, lock)
    }
}

fn validate_publication_guard(guard: &SessionPublicationGuard) -> Result<(), String> {
    guard
        .publish(|| ())
        .map_err(|error| error.to_string())?
        .ok_or_else(|| LOCK_LOST_REPLACEMENT_ERR.into())
}

fn ensure_replacement_lock_available(
    current: &SessionRuntime,
    target_id: MakimaId,
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
/// runtime. A deferred runner stays parked until the caller releases it. A
/// Activation failures return the target lock without rewriting the log.
fn swap_session_runtime(
    current: &mut SessionRuntime,
    prepared: PreparedSessionRuntime,
    target_lock: Option<SessionLockState>,
    model_slot: &ProviderSlot,
) -> std::result::Result<SessionRuntime, (String, Option<SessionLockState>)> {
    let mut runtime = prepared.publish(model_slot, target_lock)?;
    runtime.app.exit_on_done = current.app.exit_on_done;
    runtime
        .app
        .input_box
        .replace_history_from(&mut current.app.input_box);
    current.cancel_background_work();
    let old = std::mem::replace(current, runtime);
    old.app
        .command_runtime
        .finish_theme_preview(old.app.command_target.id(), false);
    #[cfg(test)]
    current.activate_deferred();
    Ok(old)
}

fn replace_session_runtime(
    current: &mut SessionRuntime,
    mut prepared: PreparedSessionRuntime,
    model_slot: &ProviderSlot,
) -> Result<SessionRuntime, String> {
    let target_id = prepared.app.session_id();
    let same_id = current.id() == target_id;
    if current.lock_lost {
        return Err(LOCK_LOST_REPLACEMENT_ERR.into());
    }
    let target_lock = if same_id {
        match current.session_lock.take() {
            Some(state @ SessionLockState::Held(_)) => Some(state),
            state => {
                current.session_lock = state;
                return Err(LOCK_UNAVAILABLE_REPLACEMENT_ERR.into());
            }
        }
    } else {
        Some(
            prepared
                .take_lease()
                .ok_or(LOCK_UNAVAILABLE_REPLACEMENT_ERR)?,
        )
    };
    match swap_session_runtime(current, prepared, target_lock, model_slot) {
        Ok(old) => Ok(old),
        Err((error, target_lock)) => {
            if same_id {
                current.session_lock = target_lock;
                if let Some(guard) = current
                    .session_lock
                    .as_ref()
                    .and_then(SessionLockState::publication_guard)
                    && validate_publication_guard(&guard).is_err()
                {
                    stop_runtime_after_recovery_failure(current, LOCK_LOST_REPLACEMENT_ERR);
                    return Err(LOCK_LOST_REPLACEMENT_ERR.into());
                }
            } else if let Err(release_error) = release_lock_state(target_lock) {
                warn!(%release_error, "replacement lock release failed after activation error");
            }
            Err(error)
        }
    }
}

impl SessionRuntime {
    fn cancel_background_work(&mut self) {
        if let Some(trigger) = self.lifetime_trigger.take() {
            trigger.cancel();
        }
    }

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
        self.app.refresh_session_projection();
        self.app.update_model(&commit.config.model);
        self.app.project_root_mode(&previous, &commit.config.mode);
        self.settle_plan_approval();
        true
    }

    fn config(&self) -> Option<Arc<makima_agent::EffectiveAgentConfig>> {
        let (manager, root) = self.handles.manager_and_root();
        manager.actor(root).ok()?.effective_config()
    }

    fn id(&self) -> MakimaId {
        self.app.state.projection.id
    }

    fn update(&mut self, msg: Msg) -> Vec<Action> {
        self.app.update(msg)
    }

    fn submit_text(&mut self, text: String) -> Result<SubmitOutcome, String> {
        if self.app.restore_in_progress {
            return Err(crate::app::RESTORE_IN_PROGRESS.into());
        }
        if self.lock_lost {
            return Err(LOCK_LOST_REPLACEMENT_ERR.into());
        }
        if text.trim().is_empty() {
            return Err(EMPTY_PROMPT_ERR.to_owned());
        }
        let text = self.app.lua_event_handle.expand_references(&text)?;
        self.config().ok_or(NOT_LIVE_ERR)?;
        let (manager, root) = self.handles.manager_and_root();
        let actor = manager.actor(root).map_err(|error| error.to_string())?;
        let snapshot = actor.snapshot();
        let starting =
            snapshot.active_turn.is_none() && snapshot.queued == 0 && !actor.input_held();
        let input = makima_agent::AgentInput::new(text, Vec::new());
        if let Err(error) = self.handles.queue.try_push(QueueItem {
            input,
            displayed: None,
        }) {
            return Err(error.to_string());
        }
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
            let (manager, root) = self.handles.manager_and_root();
            if let Ok(count) = manager.pending_continuation_count()
                && let Some(message) = makima_agent::session_recovery::continuation_notice(count)
            {
                self.app.flash(message);
            }
            self.app
                .lua_event_handle
                .notify_state_restored(manager.reference(root));
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

    /// A mailbox wake waits until the actor is idle and input is not held.
    fn quiescent(&self) -> bool {
        !self.lock_lost
            && !self.app.restore_in_progress
            && SessionStatus::of(&self.app) == SessionStatus::Idle
            && self.handles.queue.is_empty()
            && !self
                .app
                .root_agent
                .as_ref()
                .is_some_and(AgentRef::input_held)
    }
}

/// Everything needed to bring up a new session runtime after startup.
#[derive(Clone)]
struct SpawnCtx {
    initial_model_override: bool,
    workspace_cwd: String,
    file_write_locks: Arc<FileWriteLocks>,
    prepare_provider: ModelPreparer,
    providers: Arc<ProviderRegistry>,
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
    model_policy: Arc<ModelPolicy>,
    session_defaults: SessionDefaults,
    project_config: ProjectConfig,
    system_prompt: SystemPromptOverride,
    command_runtime: Arc<CommandRuntime>,
    model_capabilities: Arc<ModelCapabilityCache>,
    trust_question: Option<TrustQuestion>,
}

struct UiSessionServices;

impl FrontendServices for UiSessionServices {}

impl SpawnCtx {
    #[cfg(test)]
    fn prepare_runtime(&self, session: AppSession) -> Result<PreparedSessionRuntime> {
        self.prepare_runtime_with_provider(session, None)
    }

    fn prepare_replacement_runtime(
        &self,
        session: AppSession,
        provider: Option<PreparedProvider>,
        current: &SessionRuntime,
    ) -> Result<PreparedSessionRuntime, String> {
        let mut options = PrepareOptions {
            permission_base: Some(Arc::clone(&current.app.permissions)),
            ..Default::default()
        };
        if session.id == current.id() {
            if current.lock_lost {
                return Err(LOCK_LOST_REPLACEMENT_ERR.into());
            }
            let guard = current
                .session_lock
                .as_ref()
                .and_then(SessionLockState::publication_guard)
                .ok_or(LOCK_UNAVAILABLE_REPLACEMENT_ERR)?;
            options.reuse = Some(
                current
                    .handles
                    .session
                    .reuse(guard, session.clone())
                    .map_err(|error| error.to_string())?,
            );
        } else {
            options.expected_cwd = Some(self.workspace_cwd.clone());
        }
        self.prepare_runtime_with_options(session, provider, options, None)
            .map_err(|error| error.to_string())
    }

    fn open_model<'a>(&'a self, startup: &'a Model) -> OpenModel<'a> {
        OpenModel {
            startup,
            explicit: self.initial_model_override,
            providers: &self.providers,
            policy: &self.model_policy,
        }
    }

    fn prepare_runtime_with_provider(
        &self,
        session: AppSession,
        provider: Option<PreparedProvider>,
    ) -> Result<PreparedSessionRuntime> {
        self.prepare_runtime_with_options(
            session,
            provider,
            PrepareOptions {
                expected_cwd: Some(self.workspace_cwd.clone()),
                ..Default::default()
            },
            None,
        )
    }

    fn prepare_runtime_with_options(
        &self,
        mut session: AppSession,
        provider: Option<PreparedProvider>,
        mut options: PrepareOptions,
        pending: Option<Arc<PendingProvider>>,
    ) -> Result<PreparedSessionRuntime> {
        let startup = self.model_slot.load();
        let fallback = PreparedModel {
            model: startup.model.clone(),
            provider: Arc::clone(&startup.provider) as Arc<dyn Provider>,
        };
        drop(startup);
        let model_slot = ProviderSlot::with_change_tx(
            fallback.model.clone(),
            Arc::clone(&fallback.provider),
            self.model_slot.change_tx(),
        );
        let supplied = provider;
        let deferred = pending.is_some();
        options.startup = Some(fallback.clone());
        let supplied_model = supplied.as_ref().map(|p| p.model.clone());
        let prepare_provider = Arc::clone(&self.prepare_provider);
        let track_slot = Arc::clone(&model_slot);
        let prepare = Box::new(move |model: Model| -> BoxFuture<'static, _> {
            Box::pin(async move {
                let spec = model.spec();
                let prepared = if let Some(prepared) = supplied.filter(|p| p.model.spec() == spec) {
                    prepared
                } else if fallback.model.spec() == spec {
                    fallback
                } else if let Some(provider) = pending {
                    PreparedModel { model, provider }
                } else {
                    prepare_provider(Arc::from(spec)).await.map_err(|error| {
                        makima_providers::AgentError::Config {
                            message: error.to_string(),
                        }
                    })?
                };
                Ok(track_slot.track_model(prepared))
            })
        });
        let cwd = Arc::new(ArcSwap::from_pointee(PathBuf::from(&session.cwd)));
        options
            .permission_base
            .get_or_insert_with(|| Arc::clone(&self.permissions));
        options.model_prepare =
            Some(model_slot.track_preparation(Arc::clone(&self.prepare_provider)));
        options.project_config = Some(Arc::new({
            let model_slot = Arc::clone(&model_slot);
            let cwd = Arc::clone(&cwd);
            move |commit: &ConfigCommit| {
                model_slot.project(
                    commit.config.model.clone(),
                    Arc::clone(&commit.config.provider),
                );
                cwd.store(Arc::new(commit.config.context.cwd().to_path_buf()));
            }
        }));
        options.file_write_locks = Some(Arc::clone(&self.file_write_locks));
        let initial_wd = PathBuf::from(&session.cwd);
        // A new tab can be seeded from its focused tab rather than the startup model.
        let model = if !session.checkpoint_required() {
            supplied_model.inspect(|model| session.model = model.spec())
        } else {
            None
        }
        .unwrap_or_else(|| self.model_slot.load().model.clone());
        let source = if session.checkpoint_required() {
            SessionSource::Existing(session.id)
        } else {
            SessionSource::ExistingOrNew(session)
        };
        let (_, init_cancel) = CancelToken::new();
        let mut prepared = smol::block_on(session::prepare(
            OpenRequest {
                params: InteractiveParams {
                    model,
                    config: self.config.clone(),
                    permissions_config: Default::default(),
                    timeouts: self.timeouts,
                    prompt_slots: self.lua_event_handle.prompt_slot_source(),
                    profile: FrontendProfile::tui(self.ui_config.tool_output_lines, init_cancel),
                    mcp_handle: self.mcp_handle.clone(),
                    initial_wd,
                    session_id: None,
                    root_id: None,
                    journal: None,
                    initial_history: Vec::new(),
                    yolo: false,
                    system_prompt_override: self.system_prompt.override_text.clone(),
                    append_system_prompt: self.system_prompt.append_text.clone(),
                    defaults: self.session_defaults,
                    model_policy: Arc::clone(&self.model_policy),
                    providers: Arc::clone(&self.providers),
                    modes: self.lua_event_handle.mode_registry(),
                    plugin_rules: Arc::default(),
                    project_config: self.project_config.clone(),
                    local_tools: Arc::default(),
                    state_dir: Some(self.storage.path().to_path_buf()),
                    answers: true,
                },
                durability: Durability::Persistent {
                    storage: &self.storage,
                    source: Box::new(source),
                    explicit_model: self.initial_model_override,
                    require_matching_cwd: options.expected_cwd.is_some(),
                    skip_startup_if_children_continue: true,
                },
                continue_turns: continue_all_agents(),
                options: self.lua_event_handle.session_option_catalog(),
                model_options: self.catalog.snapshot(),
                prepare,
                services: &mut UiSessionServices,
            },
            options,
        ))?;
        let root = prepared.session().root();
        let actor = root.actor()?;
        let config_commits = actor.subscribe_config_commits();
        let actor_rx = actor.subscribe_events();
        let config = actor
            .effective_config()
            .ok_or_else(|| eyre!(NOT_LIVE_ERR))?;
        let startup_provider: Arc<dyn Provider> = self.model_slot.load().provider.clone();
        let provider =
            (!deferred && !Arc::ptr_eq(&config.provider, &startup_provider)).then(|| {
                PreparedProvider {
                    model: config.model.clone(),
                    provider: Arc::clone(&config.provider),
                }
            });
        model_slot.project(config.model.clone(), Arc::clone(&config.provider));
        cwd.store(Arc::new(config.context.cwd().to_path_buf()));
        let queue = actor_queue(Arc::new(actor));
        let mut app = App::prepare(
            (*config).clone(),
            &self.config,
            prepared.stored().expect("persistent session seed").clone(),
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
            self.ui_config.clone(),
            self.input_history_size,
            Arc::clone(&prepared.session().permissions),
            self.lua_event_handle.clone(),
            Arc::clone(&self.model_policy),
            Arc::clone(&self.providers),
            crate::theme::default_provider().clone(),
            Arc::clone(&self.command_runtime),
            Arc::clone(&self.model_capabilities),
        )?;
        app.app
            .state
            .warnings
            .extend_from_slice(prepared.warnings());
        app.app.trust_question = self.trust_question.clone();
        let resumed = session_has_content(&app.app.state.projection);
        let lease = prepared.take_lease();
        Ok(PreparedSessionRuntime {
            app,
            lease,
            prepared,
            model_slot,
            config_commits,
            actor_rx,
            queue,
            cwd,
            mcp_handle: self.mcp_handle.clone(),
            provider,
            resumed,
            #[cfg(test)]
            before_publish: None,
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
        let prepared = self.prepare_runtime_with_provider(session, provider)?;
        let runtime = prepared.activate(&self.model_slot, None)?;
        #[cfg(test)]
        let runtime = {
            let mut runtime = runtime;
            runtime.activate_deferred();
            runtime
        };
        Ok(runtime)
    }
}

enum InternalEvent {
    AgentWoken {
        runtime: Arc<()>,
        agent: AgentId,
        operation: WakeOperation,
        result: Result<(), String>,
    },
    RewindCommitted {
        runtime: Arc<()>,
        agent: AgentId,
        result: Result<RewindOutcome, String>,
    },
    ModelsDiscovered,
    /// The UI slot's model prepared differently once discovery landed.
    ModelRediscovered {
        spec: String,
        provider: ProviderIdentity,
        model: Model,
    },
    ModelCapabilitiesResolved,
    ProviderUsageFetched {
        fetch_id: ProviderUsageFetchId,
        provider: ProviderIdentity,
        result: ProviderUsageFetchResult,
    },
    /// A coordinator operation dispatched off the event-loop thread has
    /// finished. See [`SessionOpKind`] for why they cannot run inline.
    SessionOp {
        session: MakimaId,
        runtime: Arc<()>,
        kind: SessionOpKind,
        result: Result<(), String>,
    },
    PlanApprovalReady {
        session: MakimaId,
        runtime: Arc<()>,
        request: Arc<()>,
        result: Result<ApprovalStep, String>,
    },
    /// A startup prepare finished; see [`StartupPrepare`].
    StartupPrepared {
        spec: Arc<str>,
        root: bool,
        targets: Vec<(MakimaId, PreparedModel)>,
        result: Result<PreparedModel, String>,
        fallback_waiter: Option<Arc<PendingProvider>>,
    },
    /// A session's model finished preparing off the loop; see
    /// [`EventLoop::bring_up`].
    RuntimePrepared {
        target: RuntimeTarget,
        session: Box<AppSession>,
        result: Result<PreparedProvider, (Arc<str>, ConfigChangeError)>,
    },
    SessionHeartbeat(u64),
}

enum WakeOperation {
    Input(Submission),
    Rewind,
}

/// The event loop must never await a coordinator operation on its own thread:
/// the operation can wait on the actor's queue and on a durable save, and if
/// the actor is itself waiting on the UI (a permission prompt, a question) the
/// wait is circular. So the operation is dispatched, the loop keeps rendering,
/// and the follow-up work named here runs when the result comes back.
enum SessionOpKind {
    /// `/model` from a keybinding or command: apply the adopted model.
    ModelChanged {
        spec: String,
    },
    /// OAuth refresh rebuilt the focused model's provider without changing its spec.
    ProviderRefreshed,
    /// `/yolo`, `/fast`, `/workflow`: apply the toggle the coordinator took.
    OptionToggled {
        id: &'static str,
        committed: Arc<std::sync::Mutex<Option<bool>>>,
    },
    /// A committed mode change; the projection follows the new mode.
    ModeChanged,
    ContextReloaded,
    /// `/cd`: apply the canonical path the coordinator resolved, which is not
    /// necessarily the one that was typed.
    DirectoryChanged {
        adopted: Arc<std::sync::Mutex<Option<PathBuf>>>,
    },
}

struct OrchestrationRevision {
    runtime: u64,
    selected: Option<makima_agent::AgentId>,
    live: bool,
    root_config: Option<Arc<makima_agent::EffectiveAgentConfig>>,
    selected_config: Option<Arc<makima_agent::EffectiveAgentConfig>>,
    model_policy: Arc<ModelPolicy>,
    cwd: Arc<PathBuf>,
}

impl OrchestrationRevision {
    fn matches(&self, other: &Self) -> bool {
        let same_config =
            |left: &Option<Arc<makima_agent::EffectiveAgentConfig>>,
             right: &Option<Arc<makima_agent::EffectiveAgentConfig>>| {
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
        live: selected.is_some_and(|id| {
            manager
                .lookup(id)
                .is_ok_and(|target| target.validate_live().is_ok())
        }),
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
    last_focused: Option<MakimaId>,
    terminal_focused: bool,
    notifier: Option<terminal::TerminalNotifier>,
    ctx: SpawnCtx,
    orchestration: Arc<AgentOrchestration>,
    orchestration_revisions: Vec<OrchestrationRevision>,
    orchestration_focus: Option<usize>,
    sessions_dir: PathBuf,
    last_heartbeat: Instant,
    input: InputReader,
    next_wake_source: usize,
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
    startup_prepares: Vec<smol::Task<()>>,
}

/// One item from any of the event loop's sources; `None` from `next_wake`
/// means the wait timed out (animation/idle tick).
enum Wake {
    Input(Event),
    InputGone,
    Ui(UiAction),
    Agent(usize, Box<makima_agent::Envelope>),
    Actor(usize, ActorEvent),
    Shell(usize, ShellEvent),
    Warn(String),
    Command(CommandEvent),
    ProviderChanged(ProviderChange),
    Internal(InternalEvent),
}

/// A model startup seeded with a [`PendingProvider`] instead of waiting for
/// its prepare. `targets` are the runtimes seeded on it, with the model and
/// provider each started on, so a model switched since is left alone.
struct StartupPrepare {
    spec: Arc<str>,
    pending: Arc<PendingProvider>,
    root: bool,
    targets: Vec<(MakimaId, PreparedModel)>,
}

impl StartupPrepare {
    fn spawn(
        self,
        prepare: ModelPreparer,
        internal_tx: flume::Sender<InternalEvent>,
    ) -> smol::Task<()> {
        smol::spawn(async move {
            let result = prepare(Arc::clone(&self.spec))
                .await
                .map_err(|error| error.to_string());
            let fallback_waiter = match &result {
                Ok(prepared) => {
                    self.pending.fill(Ok(prepared.clone())).await;
                    None
                }
                Err(error) if self.root => {
                    self.pending.fill(Err(Arc::from(error.as_str()))).await;
                    None
                }
                Err(_) => Some(self.pending),
            };
            let _ = internal_tx.send(InternalEvent::StartupPrepared {
                spec: self.spec,
                root: self.root,
                targets: self.targets,
                result,
                fallback_waiter,
            });
        })
    }
}

fn restore_warning(spec: &str, error: &str, fallback: &Model) -> String {
    format!(
        "failed to restore model {spec}: {error}; using {}",
        fallback.spec()
    )
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
        let prepare = default_model_preparer(Arc::clone(&params.providers));
        Self::new_with(terminal, params, prepare, InputReader::spawn)
    }

    fn new_with(
        terminal: &'t mut ratatui::DefaultTerminal,
        params: EventLoopParams,
        prepare_provider: ModelPreparer,
        input: impl FnOnce() -> InputReader,
    ) -> Result<Self> {
        let EventLoopParams {
            cwd,
            model,
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
            providers,
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
            makima_highlight::pool::spawn(crate::highlight::warmup);
            crate::update::spawn_check();
        });

        let startup_pending = (!needs_login).then(|| Arc::new(PendingProvider::default()));
        let provider = match &startup_pending {
            Some(pending) => Arc::clone(pending) as Arc<dyn Provider>,
            None => makima_providers::provider::unconfigured(),
        };
        let (model_slot, provider_change_rx) = ProviderSlot::new(model.clone(), provider);
        let initial_provider = model_slot.load().provider.identity();
        let (internal_tx, internal_rx) = flume::unbounded();
        let (warn_tx, warn_rx) = flume::unbounded::<String>();
        let catalog = ModelCatalog::offline(Arc::clone(&providers), &model_policy);
        let sessions_dir = storage.ensure_subdir(SESSIONS_DIR)?;

        let notifier = terminal::TerminalNotifier::new(ui_config.notifications);
        let model_capabilities = ModelCapabilityCache::provider_adjusted(Arc::clone(&providers), {
            let internal_tx = internal_tx.clone();
            move || {
                let _ = internal_tx.send(InternalEvent::ModelCapabilitiesResolved);
            }
        });
        let model_completion = Arc::new(ModelArgSource::new(
            catalog.specs(),
            Arc::clone(&model_capabilities),
        ));
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
        let mut ctx = SpawnCtx {
            initial_model_override: explicit_model,
            workspace_cwd: cwd.to_string_lossy().into_owned(),
            file_write_locks: Arc::new(FileWriteLocks::new()),
            prepare_provider,
            providers,
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
            model_policy,
            session_defaults,
            project_config,
            system_prompt: SystemPromptOverride {
                override_text: system_prompt_override,
                append_text: append_system_prompt,
            },
            command_runtime,
            model_capabilities,
            trust_question,
        };

        let (sessions, reloading) = match sessions {
            StartupSessions::Open(session) => (vec![*session], false),
            StartupSessions::Reload(ids) => (
                ids.into_iter()
                    .map(|id| AppSession::load(id, &ctx.storage))
                    .collect::<Result<Vec<_>, _>>()?,
                true,
            ),
        };
        let mut runtimes = Vec::with_capacity(sessions.len());
        let mut on_startup_provider = Vec::new();
        let mut prepares = Vec::new();
        for session in sessions {
            let pending = Arc::new(PendingProvider::default());
            let runtime = ctx
                .prepare_runtime_with_options(
                    session,
                    None,
                    PrepareOptions {
                        expected_cwd: (!reloading).then(|| ctx.workspace_cwd.clone()),
                        ..Default::default()
                    },
                    Some(Arc::clone(&pending)),
                )
                .and_then(|prepared| prepared.activate(&ctx.model_slot, None));
            let runtime = match runtime {
                Ok(runtime) => runtime,
                Err(error) => {
                    rollback_startup_runtimes(runtimes);
                    return Err(error);
                }
            };
            if let Some(config) = runtime.config() {
                let seeded = (
                    runtime.id(),
                    PreparedModel {
                        model: config.model.clone(),
                        provider: Arc::clone(&config.provider),
                    },
                );
                let spec = config.model.spec();
                if spec == model.spec() {
                    on_startup_provider.push(seeded);
                } else {
                    prepares.push(StartupPrepare {
                        spec: Arc::from(spec),
                        pending,
                        root: false,
                        targets: vec![seeded],
                    });
                }
            }
            runtimes.push(runtime);
        }
        ctx.initial_model_override = false;
        if let Some(pending) = startup_pending {
            prepares.push(StartupPrepare {
                spec: Arc::from(model.spec()),
                pending,
                root: true,
                targets: on_startup_provider,
            });
        }
        if runtimes.is_empty() {
            return Err(eyre!("event loop needs at least one session"));
        }
        let startup_prepares = prepares
            .into_iter()
            .map(|prepare| prepare.spawn(Arc::clone(&ctx.prepare_provider), internal_tx.clone()))
            .collect();
        let focused = focused.min(runtimes.len() - 1);
        let app = &mut runtimes[focused].app;
        app.exit_on_done = exit_on_done;
        if needs_login {
            app.login_picker.open();
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
        let mut event_loop = Self {
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
            last_heartbeat: Instant::now(),
            input: input(),
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
            next_wake_source: 0,
            model_discovery,
            startup_prepares,
        };
        event_loop.sync_orchestration();
        for runtime in &mut event_loop.sessions {
            runtime.activate_deferred();
        }
        Ok(event_loop)
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
            self.sync_plan_forms();
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
            if root.validate_live().is_err() {
                continue;
            }
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
        self.sync_plan_forms();
    }

    fn sync_model_discovery(&mut self) {
        for runtime in &mut self.sessions {
            let specs = if runtime.app.model_picker.is_open()
                && let Some(target) = &runtime.app.model_picker_target
            {
                target
                    .validate()
                    .ok()
                    .and_then(|()| self.ctx.catalog.specs().load_full())
                    .map(|specs| {
                        specs
                            .iter()
                            .filter(|spec| target.template.model_policy.allows(spec))
                            .cloned()
                            .collect()
                    })
                    .unwrap_or_default()
            } else {
                selected_agent(runtime)
                    .and_then(|target| {
                        OrchestrationServices::available_models(&*self.orchestration, &target).ok()
                    })
                    .unwrap_or_default()
            };
            runtime.app.project_model_discovery(specs);
        }
    }

    /// Retires plan-form staging, and a picker editing it, once the plan it
    /// was staged for is gone.
    fn sync_plan_forms(&mut self) {
        for runtime in &mut self.sessions {
            let (manager, _) = runtime.handles.manager_and_root();
            runtime.app.sync_child_plan_paths(|target| {
                manager
                    .actor(target)
                    .ok()?
                    .config_snapshot()?
                    .config
                    .mode
                    .plan_path()
                    .map(Path::to_path_buf)
            });
            runtime.app.sync_focused_plan_form();
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
        let mut paint_due: Option<Instant> = None;
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
            for (index, runtime) in self.sessions.iter_mut().enumerate() {
                if !runtime.lock_lost {
                    runtime.app.persist_input();
                }
                let changed = runtime.app.refresh_session_projection();
                dirty |= Dirty::from(changed && index == self.focused);
            }
            if dirty.take() {
                paint_due = None;
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
            let now = Instant::now();
            let soonest_paint = cadence.paint().map(|paint| now + paint);
            paint_due = match (paint_due, soonest_paint) {
                (Some(pending), Some(soonest)) => Some(pending.min(soonest)),
                (pending, soonest) => pending.or(soonest),
            };
            let timeout = cadence
                .frame()
                .map_or(IDLE_POLL, |frame| frame.min(IDLE_POLL));
            let timeout = paint_due.map_or(timeout, |due| {
                timeout.min(due.saturating_duration_since(now))
            });
            match self.next_wake(timeout) {
                // Any event can change the screen, so paint after handling it
                // rather than asking every handler to prove it did.
                Some(wake) => {
                    dirty = Dirty::YES;
                    if let Err(e) = self.handle_wake(wake) {
                        break Err(e);
                    }
                }
                // Only the clock moved, so motion alone owes the frame once
                // its own deadline passes, however much sooner a poll woke
                // us. A deadline set before the sleep survives the cadence
                // going idle, so motion that just stopped still gets a last
                // paint to clear itself off the screen.
                None => dirty |= Dirty::from(paint_due.is_some_and(|due| Instant::now() >= due)),
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
    fn next_wake(&mut self, timeout: Duration) -> Option<Wake> {
        let count = GLOBAL_WAKE_SOURCES + SESSION_WAKE_SOURCES * self.sessions.len();
        let ready = (0..count).find_map(|offset| {
            let source = (self.next_wake_source + offset) % count;
            self.try_wake(source).map(|wake| (source, wake))
        });
        let (source, wake) = match ready {
            Some(ready) => ready,
            None if timeout.is_zero() => return None,
            None => smol::block_on(or(async { Some(self.wait_for_wake(count).await) }, async {
                smol::Timer::after(timeout).await;
                None
            }))?,
        };
        self.next_wake_source = (source + 1) % count;
        Some(wake)
    }

    fn try_wake(&self, source: usize) -> Option<Wake> {
        match source {
            0 => match self.input.receiver().try_recv() {
                Ok(event) => Some(Wake::Input(event)),
                Err(flume::TryRecvError::Disconnected) => Some(Wake::InputGone),
                Err(flume::TryRecvError::Empty) => None,
            },
            1 => self.ui_action_rx.try_recv().ok().map(Wake::Ui),
            2 => self.warn_rx.try_recv().ok().map(Wake::Warn),
            3 => self.command_rx.try_recv().ok().map(Wake::Command),
            4 => self
                .provider_change_rx
                .try_recv()
                .ok()
                .map(Wake::ProviderChanged),
            5 => self.internal_rx.try_recv().ok().map(Wake::Internal),
            _ => {
                let source = source - GLOBAL_WAKE_SOURCES;
                let i = source / SESSION_WAKE_SOURCES;
                let rt = &self.sessions[i];
                match source % SESSION_WAKE_SOURCES {
                    0 => rt
                        .handles
                        .events
                        .try_next()
                        .map(|SessionEvent::Agent(envelope)| Wake::Agent(i, Box::new(envelope))),
                    1 => rt
                        .handles
                        .actor_rx
                        .try_recv()
                        .ok()
                        .map(|ev| Wake::Actor(i, ev)),
                    _ => rt.shell_rx.try_recv().ok().map(|ev| Wake::Shell(i, ev)),
                }
            }
        }
    }

    async fn wait_for_wake(&self, count: usize) -> (usize, Wake) {
        async fn recv<T>(rx: &flume::Receiver<T>, wake: impl FnOnce(T) -> Wake) -> Wake {
            match rx.recv_async().await {
                Ok(event) => wake(event),
                Err(_) => pending().await,
            }
        }

        let input = self.input.receiver();
        let mut sources: Vec<BoxFuture<'_, Wake>> = Vec::with_capacity(count);
        sources.extend([
            Box::pin(async {
                input
                    .recv_async()
                    .await
                    .map_or(Wake::InputGone, Wake::Input)
            }) as BoxFuture<'_, Wake>,
            Box::pin(recv(&self.ui_action_rx, Wake::Ui)),
            Box::pin(recv(&self.warn_rx, Wake::Warn)),
            Box::pin(recv(&self.command_rx, Wake::Command)),
            Box::pin(recv(&self.provider_change_rx, Wake::ProviderChanged)),
            Box::pin(recv(&self.internal_rx, Wake::Internal)),
        ]);
        for (i, rt) in self.sessions.iter().enumerate() {
            let events = &rt.handles.events;
            sources.push(Box::pin(async move {
                match events.next().await {
                    Some(SessionEvent::Agent(envelope)) => Wake::Agent(i, Box::new(envelope)),
                    None => pending().await,
                }
            }));
            sources.push(Box::pin(recv(&rt.handles.actor_rx, move |ev| {
                Wake::Actor(i, ev)
            })));
            sources.push(Box::pin(recv(&rt.shell_rx, move |ev| Wake::Shell(i, ev))));
        }
        poll_fn(|cx| {
            for offset in 0..count {
                let i = (self.next_wake_source + offset) % count;
                if let Poll::Ready(wake) = sources[i].as_mut().poll(cx) {
                    return Poll::Ready((i, wake));
                }
            }
            Poll::Pending
        })
        .await
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
            InternalEvent::AgentWoken {
                runtime,
                agent,
                operation,
                result,
            } => {
                let Some(idx) = self.runtime_position(&runtime) else {
                    return;
                };
                self.sync_orchestration();
                let app = &mut self.sessions[idx].app;
                match operation {
                    WakeOperation::Input(_) => app.input_wake_pending = false,
                    WakeOperation::Rewind => app.rewind_pending = false,
                }
                if let Err(error) = result {
                    app.flash(error);
                } else if app.selected_agent_id() != Some(agent) {
                    app.flash(CLOSED_TARGET_ERR.into());
                } else {
                    match operation {
                        WakeOperation::Input(submission) => {
                            let actions = app.handle_submit(submission);
                            self.dispatch(idx, actions);
                        }
                        WakeOperation::Rewind => self.handle_action(idx, Action::BeginRewind),
                    }
                }
            }
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
            InternalEvent::ModelsDiscovered => {
                self.ctx.model_capabilities.clear();
                self.refresh_discovered_models();
            }
            InternalEvent::ModelCapabilitiesResolved => {
                for runtime in &mut self.sessions {
                    if runtime.app.model_picker.is_open() {
                        runtime.app.pending_dirty |= Dirty::YES;
                    }
                }
            }
            InternalEvent::ModelRediscovered {
                spec,
                provider,
                model,
            } => self.apply_rediscovered_model(&spec, provider, model),
            InternalEvent::StartupPrepared {
                spec,
                root,
                targets,
                result,
                fallback_waiter,
            } => self.complete_startup_prepare(spec, root, targets, result, fallback_waiter),
            InternalEvent::RuntimePrepared {
                target,
                session,
                result,
            } => self.complete_bring_up(target, *session, result),
            InternalEvent::RewindCommitted {
                runtime,
                agent,
                result,
            } => {
                if let Some(idx) = self.runtime_position(&runtime) {
                    let rt = &mut self.sessions[idx];
                    rt.app.rewind_pending = false;
                    let (manager, root) = rt.handles.manager_and_root();
                    match result {
                        Ok(RewindOutcome::Fork {
                            history,
                            draft,
                            warning,
                        }) => {
                            rt.app.refresh_session_projection();
                            if agent == root {
                                rt.app.finish_root_rewind(draft);
                            } else {
                                rt.app.finish_child_rewind(
                                    manager.reference(agent),
                                    history,
                                    draft,
                                );
                            }
                            if let Some(warning) = warning {
                                rt.app.flash(format!("{REWIND_UNCERTAIN}: {warning}"));
                            }
                        }
                        Ok(RewindOutcome::PlanningSession(parent)) if agent == root => {
                            self.back_to_planning(idx, parent.session);
                        }
                        Ok(RewindOutcome::PlanningSession(_)) => {}
                        Err(error) => rt.app.flash(error),
                    }
                }
            }
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
        self.ctx.model_capabilities.clear();
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
                "provider": self.provider_display_name(&current.model.provider),
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
        if self.sessions[index].app.restore_in_progress {
            if let CommandEvent::Host { reply, .. } = event {
                let _ = reply.send(Err(CommandError::StaleTarget));
            }
            return;
        }
        match event {
            CommandEvent::Host {
                request,
                admission,
                reply,
                ..
            } => {
                if let Some(pinned) = &admission
                    && (pinned.validate().is_err() || pinned.target.validate_live().is_err())
                {
                    let _ = reply.send(Err(CommandError::StaleTarget));
                    return;
                }
                if let makima_commands::HostRequest::Builtin(operation) = &request {
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
                                self.dispatch_pinned_toggle(index, pinned, id, Some(reply.clone()))
                                    .map_err(|error| CommandError::Producer(Arc::from(error)))
                            });
                        if let Err(error) = result {
                            let _ = reply.send(Err(error));
                        }
                        return;
                    }
                    if matches!(operation, BuiltinOperation::ReloadContext) {
                        let result = admission
                            .as_ref()
                            .ok_or(CommandError::StaleTarget)
                            .and_then(|pinned| {
                                self.dispatch_context_reload(index, pinned, reply.clone())
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
                                self.pinned_actor(index, pinned)
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
                            Some(makima_commands::ModelSelection {
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
                        self.dispatch_model_selection(index, Some(*pinned), selection, Some(reply));
                        return;
                    }
                }
                if let makima_commands::HostRequest::Builtin(BuiltinOperation::ChangeDirectory {
                    path,
                }) = &request
                {
                    let result = admission
                        .as_ref()
                        .ok_or(CommandError::StaleTarget)
                        .and_then(|pinned| {
                            self.dispatch_pinned_directory(index, pinned, path.clone())
                                .map_err(|error| CommandError::Producer(Arc::from(error)))
                        });
                    let _ = reply.send(result.map(|()| HostResponse::Completed));
                    return;
                }
                if matches!(
                    request,
                    makima_commands::HostRequest::Context(
                        makima_commands::HostContextRequest::WorkingDirectory
                    )
                ) {
                    let result = admission
                        .as_ref()
                        .ok_or(CommandError::StaleTarget)
                        .and_then(|pinned| {
                            self.pinned_actor(index, pinned)
                                .map_err(|_| CommandError::StaleTarget)
                        })
                        .and_then(|actor| actor.effective_config().ok_or(CommandError::StaleTarget))
                        .map(|config| {
                            HostResponse::Context(
                                makima_commands::HostContextResponse::WorkingDirectory(
                                    config.context.cwd().to_path_buf(),
                                ),
                            )
                        });
                    let _ = reply.send(result);
                    return;
                }
                if matches!(
                    request,
                    makima_commands::HostRequest::Context(
                        makima_commands::HostContextRequest::FastModeSupported
                    )
                ) {
                    let result = admission
                        .as_ref()
                        .ok_or(CommandError::StaleTarget)
                        .and_then(|pinned| {
                            self.pinned_actor(index, pinned)
                                .map_err(|_| CommandError::StaleTarget)
                        })
                        .and_then(|actor| actor.effective_config().ok_or(CommandError::StaleTarget))
                        .map(|config| {
                            HostResponse::Context(
                                makima_commands::HostContextResponse::FastModeSupported(
                                    config.model.supports_fast() || config.model.fast_pending(),
                                ),
                            )
                        });
                    let _ = reply.send(result);
                    return;
                }
                if matches!(
                    request,
                    makima_commands::HostRequest::Context(
                        makima_commands::HostContextRequest::ModelSpecs
                    )
                ) {
                    let result = admission
                        .as_ref()
                        .ok_or(CommandError::StaleTarget)
                        .and_then(|pinned| self.target_model_specs(pinned));
                    let _ = reply.send(result);
                    return;
                }
                if matches!(
                    request,
                    makima_commands::HostRequest::Builtin(BuiltinOperation::OpenModelPicker)
                ) {
                    let result = admission
                        .ok_or(CommandError::StaleTarget)
                        .and_then(|pinned| {
                            self.pinned_actor(index, &pinned)
                                .map_err(|_| CommandError::StaleTarget)?;
                            self.sessions[index].app.open_pinned_model_picker(*pinned)
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
                makima_commands::CommandOutcome::AgentTurn(turn) => {
                    let actions = self.sessions[index]
                        .app
                        .submit_command_turn(turn, admission.as_deref());
                    self.dispatch(index, actions);
                }
                makima_commands::CommandOutcome::IsolatedTurn(turn) => {
                    let actions = self.sessions[index]
                        .app
                        .submit_isolated_turn(turn, admission.as_deref());
                    self.dispatch(index, actions);
                }
                makima_commands::CommandOutcome::FrontendFeedback(feedback) => {
                    self.sessions[index].app.present_frontend_feedback(feedback);
                }
                makima_commands::CommandOutcome::Failed(error) => {
                    self.sessions[index].app.flash(error.to_string());
                }
                makima_commands::CommandOutcome::ManualCompaction(_)
                | makima_commands::CommandOutcome::Completed => {}
            },
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
            dirty |= rt.app.sync_writer_warning();
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
            makima_agent::EventSender::with_identity(rt.handles.agent_tx.clone(), Some(*identity))
                .send(AgentEvent::QueueSettled);
    }

    fn handle_agent(&mut self, idx: usize, envelope: Box<makima_agent::Envelope>) {
        let rt = &mut self.sessions[idx];
        rt.app.adopt_actor_presentation(&envelope);
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
            AgentEvent::TurnComplete(turn) => {
                rt.app.refresh_session_projection();
                if current {
                    rt.notifications.on_turn_complete(&turn.message);
                }
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
                && rt.handles.events.is_empty()
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

    fn provider_display_name(&self, provider: &str) -> String {
        self.ctx
            .providers
            .display_name(provider)
            .map_or_else(|| UNKNOWN_PROVIDER.into(), |name| name.to_string())
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
            provider: self.provider_display_name(&current.model.provider),
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
            provider: self.provider_display_name(&current.model.provider),
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
                    "title": rt.app.title(),
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
                let cwd = self.ctx.workspace_cwd.clone();
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
                let replacement = match self.position(id) {
                    Some(i) if i == self.focused && self.sessions.len() == 1 => {
                        match self.spawn_blank_runtime() {
                            Ok(runtime) => Some(runtime),
                            Err(error) => {
                                let _ = reply_tx.send(Err(error.to_string()));
                                return;
                            }
                        }
                    }
                    _ => None,
                };
                let mut runtime = if let Some(i) = self.position(id) {
                    let session_lock = self.sessions[i].session_lock.take();
                    let mut runtime = self.remove_runtime(i);
                    runtime.session_lock = session_lock;
                    Some(runtime)
                } else {
                    None
                };
                if let Some(replacement) = replacement {
                    self.push_runtime(replacement);
                }
                let store = runtime
                    .as_ref()
                    .and_then(|rt| rt.handles.offload.as_ref())
                    .cloned();
                if let Some(store) = &store {
                    store.request_close();
                }
                let sessions_dir = self.ctx.sessions_dir.clone();
                smol::spawn(async move {
                    let session_lock = runtime.as_mut().and_then(|rt| rt.session_lock.take());
                    let lock_dir = sessions_dir.clone();
                    let lease = smol::unblock(move || {
                        match settle_lock_state(session_lock, AGENT_SHUTDOWN_TIMEOUT) {
                            LockSettlement::Held(lease) => Ok(lease),
                            LockSettlement::None => claim_lock(&lock_dir, &id),
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
                                let (manager, root) = rt.handles.manager_and_root();
                                if let Ok(actor) = manager.actor(root) {
                                    actor.cancel_all();
                                }
                                let _ = bounded_session_op(
                                    async {
                                        manager.wait_settled().await;
                                        Ok(())
                                    },
                                    AGENT_SHUTDOWN_TIMEOUT,
                                )
                                .await;
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
                    smol::unblock(move || {
                        if let Some(store) = &store {
                            store.close_and_drain();
                        }
                    })
                    .await;
                    let reply = smol::unblock(move || {
                        makima_agent::session_writer::forget(&sessions_dir, id)
                            .map_err(|error| error.to_string())?;
                        guard
                            .publish(|| makima_agent::session_writer::delete(&sessions_dir, id))
                            .map_err(|error| error.to_string())?
                            .ok_or_else(|| LOCK_LOST_REPLACEMENT_ERR.to_owned())?
                            .map(|_| json!(true))
                            .map_err(|error| error.to_string())
                    })
                    .await;
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
                    .map(|(i, rt)| live_session_row(&rt.app, i == self.focused))
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
                let mut runtime = match self.spawn_blank_runtime() {
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
            SessionRequest::Focus { id } => match parse_session_id(&id) {
                Ok(id) => self.focus_session(id, reply_tx),
                Err(error) => {
                    let _ = reply_tx.send(Err(error));
                }
            },
            SessionRequest::SetTitle { id, title } => {
                let reply = (|| {
                    let id = parse_session_id(&id)?;
                    if let Some(i) = self.position(id) {
                        let runtime = &mut self.sessions[i];
                        if runtime.lock_lost {
                            return Err(LOCK_LOST_REPLACEMENT_ERR.into());
                        }
                        runtime.app.set_title(title);
                    } else {
                        smol::block_on(set_title(&self.ctx.sessions_dir, id, title))
                            .map_err(|error| error.to_string())?;
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

    fn position(&self, id: MakimaId) -> Option<usize> {
        self.sessions.iter().position(|rt| rt.id() == id)
    }

    /// A plugin holding the id of a tab that has since closed gets `session
    /// not live` back, so it knows to stop.
    fn resolve_session_index(&self, id: MakimaId) -> Result<usize, String> {
        self.position(id).ok_or_else(|| NOT_LIVE_ERR.into())
    }

    /// The totals live on the session, so a plugin that reloads mid run keeps
    /// the accounting it would lose by summing `TurnEnd` payloads itself.
    fn session_snapshot_json(&self, idx: usize) -> serde_json::Value {
        let runtime = &self.sessions[idx];
        let app = &runtime.app;
        let projection = app.session_projection();
        let usage = projection.usage();
        let snapshot = SessionSnapshot {
            id: runtime.id().to_string(),
            title: Some(projection.title().to_owned()),
            status: SessionStatus::of(app).as_str(),
            updated_at: projection.updated_at,
            usage: session_usage(&usage.tokens, usage.cost, &usage.by_model),
        };
        serde_json::to_value(snapshot).unwrap_or_default()
    }

    /// The single place that removes a runtime: keeps `focused` on the same
    /// session when an earlier one goes, and moves it to the next session, or
    /// the previous one, when the focused session goes. A caller deleting the
    /// last runtime pushes its replacement first, so `sessions` stays
    /// non-empty.
    fn remove_runtime(&mut self, idx: usize) -> SessionRuntime {
        let mut rt = self.sessions.remove(idx);
        rt.cancel_background_work();
        if let Err(error) = release_lock_state(rt.session_lock.take()) {
            warn!(id = %rt.id(), %error, "session lock release failed");
        }
        if idx < self.focused {
            self.focused -= 1;
        } else if idx == self.focused {
            self.focused = idx.min(self.sessions.len().saturating_sub(1));
        }
        rt
    }

    fn discard_rejected_runtime(
        &self,
        runtime: SessionRuntime,
        error: String,
        reply: flume::Sender<UiReply>,
    ) {
        let sessions_dir = self.ctx.sessions_dir.clone();
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
            let cleanup = match settle_lock_state(session_lock, AGENT_SHUTDOWN_TIMEOUT) {
                LockSettlement::Held(lease) => {
                    let lease = RenewingDeletionLease::new(lease);
                    let guard = lease.publication_guard();
                    let result = smol::unblock(move || {
                        makima_agent::session_writer::forget(&sessions_dir, id)
                            .map_err(|error| error.to_string())?;
                        guard
                            .publish(|| makima_agent::session_writer::delete(&sessions_dir, id))
                            .map_err(|error| error.to_string())?
                            .ok_or_else(|| LOCK_LOST_REPLACEMENT_ERR.to_owned())?
                            .map_err(|error| error.to_string())
                    })
                    .await;
                    drop(lease);
                    result
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

    fn spawn_blank_runtime(&self) -> Result<SessionRuntime> {
        let (session, provider) =
            new_session_from_slot(self.focused_model_slot(), &self.ctx.workspace_cwd);
        self.ctx
            .spawn_runtime_with_provider(session, Some(provider))
    }

    fn push_runtime(&mut self, rt: SessionRuntime) -> usize {
        if self.pending_status_invalidation.is_some() {
            rt.app
                .suppress_status_content
                .store(true, Ordering::Release);
        }
        self.sessions.push(rt);
        let idx = self.sessions.len() - 1;
        self.sync_orchestration();
        self.sessions[idx].activate_deferred();
        idx
    }

    /// Brings {session} up at {target}, first preparing its model off the
    /// event loop when it needs one: preparing can read credentials or call
    /// into a plugin, and the loop must keep rendering meanwhile.
    fn bring_up(&mut self, target: RuntimeTarget, session: AppSession) {
        let startup = self.ctx.model_slot.load().model.clone();
        let (selected, _) = match self.ctx.open_model(&startup).select(&session) {
            Ok(selection) => selection,
            Err(error) => return self.report_placement(target, Err(error.to_string())),
        };
        if selected.spec() == startup.spec() {
            let result = self.place_runtime(&target, session, None);
            return self.report_placement(target, result);
        }
        let spec = Arc::<str>::from(selected.spec());
        let prepare = Arc::clone(&self.ctx.prepare_provider);
        let internal_tx = self.internal_tx.clone();
        smol::spawn(async move {
            let result = prepare(Arc::clone(&spec))
                .await
                .map_err(|error| (spec, error));
            let _ = internal_tx.send(InternalEvent::RuntimePrepared {
                target,
                session: Box::new(session),
                result,
            });
        })
        .detach();
    }

    fn complete_bring_up(
        &mut self,
        target: RuntimeTarget,
        session: AppSession,
        result: Result<PreparedProvider, (Arc<str>, ConfigChangeError)>,
    ) {
        let result = match result {
            Ok(provider) => self.place_runtime(&target, session, Some(provider)),
            Err((spec, error)) => {
                // The checkpoint may have changed since prefetch. Reuse the error
                // only if shared preparation selects the same model under its lease.
                let mut ctx = self.ctx.clone();
                let prepare = Arc::clone(&ctx.prepare_provider);
                let failure = Mutex::new(Some(error));
                ctx.prepare_provider = Arc::new(move |selected| {
                    if selected == spec
                        && let Some(error) = failure.lock_unpoisoned().take()
                    {
                        return Box::pin(async move { Err(error) });
                    }
                    prepare(selected)
                });
                self.place_runtime_with(&ctx, &target, session, None)
            }
        };
        self.report_placement(target, result);
    }

    fn place_runtime(
        &mut self,
        target: &RuntimeTarget,
        session: AppSession,
        provider: Option<PreparedProvider>,
    ) -> Result<(), String> {
        self.place_runtime_with(&self.ctx.clone(), target, session, provider)
    }

    fn place_runtime_with(
        &mut self,
        ctx: &SpawnCtx,
        target: &RuntimeTarget,
        session: AppSession,
        provider: Option<PreparedProvider>,
    ) -> Result<(), String> {
        let (runtime, kind) = match target {
            RuntimeTarget::Open { .. } => return self.open_runtime(ctx, session, provider),
            RuntimeTarget::Replace { runtime, kind, .. } => (runtime, kind),
        };
        let idx = self.runtime_position(runtime).ok_or(RUNTIME_GONE_ERR)?;
        // Taking a blank tab was decided before the model was prepared; the
        // user may have typed or started work there since, so look again.
        if kind.is_none() && !self.blank_and_idle(idx) {
            return self.open_runtime(ctx, session, provider);
        }
        if self.sessions[idx].lock_lost {
            return Err(LOCK_LOST_REPLACEMENT_ERR.into());
        }
        let implementation = matches!(kind, Some(SessionReplacementKind::Linked))
            .then(|| session.meta.input_draft.clone())
            .flatten();
        let prepared = if matches!(kind, Some(SessionReplacementKind::Reset { .. })) {
            let runtime = &self.sessions[idx];
            let reset =
                ResetSeed::from_root(runtime.handles.session.root(), &runtime.app.permissions)
                    .map_err(|error| error.to_string())?
                    .with_session(session.clone());
            ctx.prepare_runtime_with_options(
                session,
                provider,
                PrepareOptions {
                    permission_base: Some(Arc::clone(&runtime.app.permissions)),
                    reset: Some(reset),
                    expected_cwd: Some(self.ctx.workspace_cwd.clone()),
                    ..Default::default()
                },
                None,
            )
            .map_err(|error| error.to_string())?
        } else {
            ctx.prepare_replacement_runtime(session, provider, &self.sessions[idx])?
        };
        let reset_journal = matches!(kind, Some(SessionReplacementKind::Reset { .. }))
            .then(|| self.sessions[idx].handles.manager_and_root().0.journal());
        if matches!(kind, Some(SessionReplacementKind::Planning)) {
            let mut old =
                replace_session_runtime(&mut self.sessions[idx], prepared, &self.ctx.model_slot)?;
            self.sync_orchestration();
            self.sessions[idx].activate_deferred();
            old.cancel_background_work();
            smol::spawn(async move {
                let SessionRuntime {
                    handles,
                    coordinator,
                    session_lock,
                    checkpoint,
                    ..
                } = old;
                handles.suspend().await;
                if let Err(error) = checkpoint.drain().await {
                    warn!(%error, "planning switch writer drain failed");
                }
                let _ = coordinator.close().await;
                if let Err(error) = release_lock_state(session_lock) {
                    warn!(%error, "planning switch lock release failed");
                }
            })
            .detach();
        } else {
            self.replace_prepared_runtime(idx, prepared)?;
        }
        if let Some(message) = implementation {
            let runtime = &mut self.sessions[idx];
            runtime.app.input_box.set_input(String::new());
            runtime.app.persist_input();
            match runtime.submit_text(message)? {
                SubmitOutcome::Started(actions) => self.dispatch(idx, actions),
                SubmitOutcome::Queued => {}
                SubmitOutcome::Rejected(error) => return Err(error),
            }
            let model = self.sessions[idx].app.root_config().model.clone();
            self.sessions[idx].app.update_model(&model);
            self.sessions[idx].app.record_recent_model(&model.spec());
        }
        if let Some(journal) = reset_journal {
            journal.invalidate_state_access();
        }
        match kind {
            Some(SessionReplacementKind::Reset { ended_id }) => {
                self.fire_session_reset(idx, *ended_id)
            }
            Some(SessionReplacementKind::Linked | SessionReplacementKind::Planning) => {}
            #[cfg(test)]
            Some(SessionReplacementKind::Load) => {}
            None => {}
        }
        Ok(())
    }

    fn open_runtime(
        &mut self,
        ctx: &SpawnCtx,
        session: AppSession,
        provider: Option<PreparedProvider>,
    ) -> Result<(), String> {
        if let Some(idx) = self.position(session.id) {
            self.set_focused(idx);
            return Ok(());
        }
        let runtime = ctx
            .prepare_runtime_with_provider(session, provider)
            .and_then(|prepared| prepared.activate(&ctx.model_slot, None))
            .map_err(|error| error.to_string())?;
        let idx = self.push_runtime(runtime);
        self.set_focused(idx);
        Ok(())
    }

    /// Whether the session at {idx} holds nothing worth keeping, so a stored
    /// session may take its tab: idle, nothing queued, no messages or draft.
    /// The input box is read too, as a draft typed since the last frame has
    /// not reached the session yet.
    fn blank_and_idle(&self, idx: usize) -> bool {
        let session = &self.sessions[idx];
        let (manager, root) = session.handles.manager_and_root();
        let actor_idle = manager.actor(root).is_ok_and(|actor| {
            let snapshot = actor.snapshot();
            snapshot.status == makima_agent::actor::ActorStatus::Idle && snapshot.queued == 0
        });
        session.quiescent()
            && actor_idle
            && !session.app.has_content()
            && manager.journal().snapshot().posts.is_empty()
            && session.app.input_box.buffer.value().is_empty()
    }

    fn report_placement(&mut self, target: RuntimeTarget, result: Result<(), String>) {
        match target {
            RuntimeTarget::Open { reply }
            | RuntimeTarget::Replace {
                reply: Some(reply), ..
            } => {
                let _ = reply.send(result.map(|()| json!(true)));
            }
            RuntimeTarget::Replace {
                runtime,
                reply: None,
                ..
            } => {
                if let Err(error) = result {
                    let idx = self.runtime_position(&runtime).unwrap_or(self.focused);
                    self.sessions[idx].app.flash(error);
                }
            }
        }
    }

    fn runtime_position(&self, runtime: &Arc<()>) -> Option<usize> {
        self.sessions
            .iter()
            .position(|session| Arc::ptr_eq(runtime, &session.handles.identity()))
    }

    fn replace_prepared_runtime(
        &mut self,
        idx: usize,
        prepared: PreparedSessionRuntime,
    ) -> Result<(), String> {
        let old = replace_session_runtime(&mut self.sessions[idx], prepared, &self.ctx.model_slot)?;
        self.sync_orchestration();
        self.sessions[idx].activate_deferred();
        smol::spawn(self.retire_runtime(idx, old, ())).detach();
        Ok(())
    }

    /// Retains the outgoing lease until agent shutdown and the final writer drain.
    fn retire_runtime(
        &self,
        idx: usize,
        mut old: SessionRuntime,
        held: impl Send + 'static,
    ) -> impl Future<Output = ()> + Send + 'static {
        old.cancel_background_work();
        let SessionRuntime {
            mut app,
            handles,
            coordinator,
            checkpoint,
            session_lock,
            lock_lost,
            ..
        } = old;
        let retired_id = app.state.projection.id;
        let replaced_session = retired_id != self.sessions[idx].id();
        if replaced_session && !lock_lost {
            app.persist_input();
        }
        let ended_conversation = replaced_session
            .then(|| handles.history.load().conversation.clone())
            .flatten()
            .filter(|conversation| conversation.sent.is_some())
            .map(|conversation| {
                (
                    Arc::clone(&app.root_config().settings.provider),
                    conversation.id,
                )
            });
        drop(app);
        let shutdown = handles.shutdown();
        async move {
            shutdown.await;
            if !lock_lost
                && session_lock.is_some()
                && let Err(error) = bounded_session_op(
                    async { checkpoint.drain().await.map_err(|error| error.to_string()) },
                    AGENT_SHUTDOWN_TIMEOUT,
                )
                .await
            {
                warn!(%error, "old session writer drain failed");
            }
            if let Err(error) = smol::unblock(move || release_lock_state(session_lock)).await {
                warn!(%error, "old session lock release failed");
            }
            drop(held);
            if replaced_session {
                if let Some((provider, conversation_id)) = ended_conversation {
                    end_conversation_detached(&provider, conversation_id, ConversationEnd::Closed);
                }
                let _ = coordinator.close().await;
            }
        }
    }

    fn fire_session_reset(&self, idx: usize, ended_id: MakimaId) {
        self.sessions[idx]
            .app
            .lua_event_handle
            .fire_autocmd("SessionReset", json!({ "session_id": ended_id }));
    }

    fn back_to_planning(&mut self, idx: usize, id: MakimaId) {
        self.sessions[idx].app.persist_input();
        if let Some(parent) = self.position(id) {
            self.set_focused(parent);
            let session_lock = self.sessions[idx].session_lock.take();
            let old = self.remove_runtime(idx);
            smol::spawn(async move {
                old.handles.suspend().await;
                if let Err(error) = old.checkpoint.drain().await {
                    warn!(%error, "planning switch writer drain failed");
                }
                let _ = old.coordinator.close().await;
                if let Err(error) = release_lock_state(session_lock) {
                    warn!(%error, "planning switch lock release failed");
                }
            })
            .detach();
            return;
        }
        match AppSession::load(id, &self.ctx.storage) {
            Ok(session) => self.request_replacement(
                idx,
                SessionReplacementRequest {
                    session,
                    kind: SessionReplacementKind::Planning,
                    provider: None,
                },
            ),
            Err(error) => self.sessions[idx].app.flash(error.to_string()),
        }
    }

    fn request_replacement(&mut self, idx: usize, request: SessionReplacementRequest) {
        let SessionReplacementRequest {
            session,
            kind,
            provider,
        } = request;
        if let Err(error) = ensure_replacement_lock_available(&self.sessions[idx], session.id) {
            self.sessions[idx].app.flash(error.into());
            return;
        }
        let target = RuntimeTarget::Replace {
            runtime: self.sessions[idx].handles.identity(),
            kind: Some(kind),
            reply: None,
        };
        if let Some(provider) = provider {
            let result = self.place_runtime(&target, session, Some(provider));
            self.report_placement(target, result);
        } else {
            self.bring_up(target, session);
        }
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
    fn focus_session(&mut self, id: MakimaId, reply: flume::Sender<UiReply>) {
        if let Err(error) = self.request_focus(id, reply.clone()) {
            let _ = reply.send(Err(error));
        }
    }

    fn request_focus(&mut self, id: MakimaId, reply: flume::Sender<UiReply>) -> Result<(), String> {
        if let Some(i) = self.position(id) {
            self.set_focused(i);
            let _ = reply.send(Ok(json!(true)));
            return Ok(());
        }
        let session = AppSession::load(id, &self.ctx.storage)
            .map_err(|e| format!("Failed to load session: {e}"))?;
        let open_elsewhere = session_lock::open_elsewhere(&self.sessions_dir, &id);
        if let Some(block) =
            session_lock::resume_block(&session.cwd, &self.ctx.workspace_cwd, open_elsewhere)
        {
            return Err(block.to_string());
        }
        let target = if self.blank_and_idle(self.focused) {
            let current = &self.sessions[self.focused];
            if current.lock_lost {
                return Err(LOCK_LOST_REPLACEMENT_ERR.into());
            }
            let runtime = current.handles.identity();
            self.sessions[self.focused].app.persist_input();
            RuntimeTarget::Replace {
                runtime,
                kind: None,
                reply: Some(reply),
            }
        } else {
            RuntimeTarget::Open { reply }
        };
        self.bring_up(target, session);
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
        if self.sessions[idx].app.restore_in_progress
            && !matches!(action, Action::ManualExit | Action::CancelAgent { .. })
        {
            self.sessions[idx]
                .app
                .flash(crate::app::RESTORE_IN_PROGRESS.into());
            return;
        }
        self.sync_orchestration();
        match action {
            Action::SendMessage(input) => {
                let rt = &mut self.sessions[idx];
                rt.reset_run_notifications();
                let run_id = rt.app.run_id;
                rt.handles.queue.push_mailbox(QueueItem {
                    input: *input,
                    displayed: Some(run_id),
                });
            }
            Action::CancelAgent { .. } => {
                self.sessions[idx].notifications.reset();
            }
            Action::MoveToBackground { agent } => {
                let moved = self.sessions[idx]
                    .handles
                    .mailbox()
                    .map(|mailbox| mailbox.move_latest_for(agent));
                let message = match moved {
                    Some(Ok(id)) => format!("{MOVED_BACKGROUND_PREFIX}{id}"),
                    Some(Err(error)) => error.to_string(),
                    None => NO_MAILBOX_ERR.into(),
                };
                self.sessions[idx].app.flash(message);
            }
            Action::WakeForInput { target, submission } => {
                self.wake_agent(idx, target, WakeOperation::Input(submission));
            }
            Action::BeginRewind => {
                let target = selected_graph_agent(&self.sessions[idx]);
                if let Some(target) = target.as_ref()
                    && target
                        .snapshot()
                        .is_ok_and(|node| node.graph_lifecycle == GraphLifecycle::Dormant)
                {
                    self.wake_agent(idx, target.clone(), WakeOperation::Rewind);
                    return;
                }
                let target = target.filter(|target| target.validate_live().is_ok());
                let rt = &mut self.sessions[idx];
                let result = target
                    .ok_or_else(|| CLOSED_TARGET_ERR.to_owned())
                    .and_then(|target| target.begin_rewind().map_err(|error| error.to_string()));
                rt.app.rewind_pending = false;
                match result {
                    Ok(guard) => rt.app.show_rewind(guard),
                    Err(error) => rt.app.flash(error),
                }
            }
            Action::CommitRewind(index) => {
                let rt = &mut self.sessions[idx];
                let Some(guard) = rt.app.rewind_guard.take() else {
                    rt.app.rewind_pending = false;
                    return;
                };
                let Some(target) = guard.targets().get(index).map(|target| target.id.clone())
                else {
                    rt.app.rewind_pending = false;
                    return;
                };
                let runtime = rt.handles.identity();
                let agent = guard.agent_id();
                let tx = self.internal_tx.clone();
                smol::spawn(async move {
                    let result = guard
                        .commit(target)
                        .await
                        .map_err(|error| error.to_string());
                    let _ = tx.send(InternalEvent::RewindCommitted {
                        runtime,
                        agent,
                        result,
                    });
                })
                .detach();
            }
            Action::ReplaceSession(request) => self.request_replacement(idx, *request),
            Action::ToggleSessionOption { id } => {
                dispatch_option_toggle(&self.sessions[idx], id, &self.internal_tx);
            }
            Action::SetSessionOption { id, enabled } => {
                dispatch_option_change(&self.sessions[idx], id, Some(enabled), &self.internal_tx);
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
                self.selected_pinned_target(idx),
                makima_commands::ModelSelection {
                    spec: Some(Arc::from(spec)),
                    ..Default::default()
                },
                None,
            ),
            Action::SelectPinnedModel { target, selection } => {
                self.dispatch_model_selection(idx, Some(*target), selection, None);
            }
            Action::RunKeybind(id) => self.run_keybind(idx, id),
            Action::SelectModel(selection) => {
                self.note_if_deferred(idx, "model");
                let target = self.selected_pinned_target(idx);
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
                selection,
                parallel,
                path,
            } => {
                self.prepare_plan_approval(idx, clear_context, selection, parallel, path);
            }
            Action::CancelPlanApproval => self.sessions[idx].end_plan_approval(),
            Action::RefreshProvider { slug } => self.refresh_provider(idx, slug),
            Action::AssignTier(spec, tier) => {
                makima_providers::model_registry::set_and_persist(spec, tier, &self.ctx.storage);
            }
            Action::UnassignTier(spec, tier) => {
                makima_providers::model_registry::unset_and_persist(&spec, tier, &self.ctx.storage);
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
                submission,
                command,
                visible,
            } => {
                let Some(target) = self.selected_target(idx) else {
                    self.sessions[idx].app.flash(CLOSED_TARGET_ERR.into());
                    return;
                };
                let rt = &mut self.sessions[idx];
                let id = rt.app.shell.reserve_for(target.id());
                rt.app.accept_shell_command(&submission, &command, visible);
                let (trigger, cancel) = CancelToken::new();
                rt.app.shell.add_trigger_for(id.clone(), trigger);
                let tx = rt.shell_tx.clone();
                let config = self.ctx.config.clone();
                smol::spawn(async move {
                    let _ = tx.send(ShellEvent::Start {
                        id: id.clone(),
                        command: command.clone(),
                    });
                    let result = target
                        .run_shell(&command, visible, &config, &cancel, |content| {
                            let _ = tx.send(ShellEvent::Output {
                                id: id.clone(),
                                content: content.into(),
                            });
                        })
                        .await;
                    let (output, is_error) = match result {
                        Ok(result) => (result.output, result.is_error),
                        Err(error) => (error.to_string(), true),
                    };
                    let _ = tx.send(ShellEvent::Done {
                        id,
                        output,
                        is_error,
                    });
                })
                .detach();
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
                self.sessions[idx].app.start_btw(question, images);
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
        session: MakimaId,
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
            SessionOpKind::OptionToggled { id, committed } => match result {
                Ok(()) => {
                    if let Some(enabled) = committed.lock_unpoisoned().take() {
                        complete_option_toggle(&mut self.sessions[idx].app, id, enabled);
                    }
                }
                Err(error) => self.sessions[idx].app.flash(error),
            },
            SessionOpKind::ModeChanged => match result {
                Ok(()) => self.sessions[idx].settle_plan_approval(),
                Err(error) => self.sessions[idx].app.flash(error),
            },
            SessionOpKind::ContextReloaded => match result {
                Ok(()) => self.sessions[idx].app.flash(CONTEXT_RELOADED.into()),
                Err(error) => self.sessions[idx].app.flash(error),
            },
            SessionOpKind::DirectoryChanged { adopted } => match result {
                Ok(()) => {
                    let adopted = adopted.lock_unpoisoned().take();
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
    fn target_model_specs(
        &self,
        target: &PinnedCommandTarget,
    ) -> Result<HostResponse, CommandError> {
        target.validate().map_err(|_| CommandError::StaleTarget)?;
        let specs = self
            .ctx
            .catalog
            .specs()
            .load_full()
            .ok_or(CommandError::StaleTarget)?;
        Ok(HostResponse::Context(
            makima_commands::HostContextResponse::Values(
                specs
                    .iter()
                    .filter(|spec| target.template.model_policy.allows(spec))
                    .map(|spec| Arc::from(spec.as_str()))
                    .collect(),
            ),
        ))
    }

    fn wake_agent(&self, idx: usize, target: AgentRef, operation: WakeOperation) {
        let runtime = self.sessions[idx].handles.identity();
        let tx = self.internal_tx.clone();
        smol::spawn(async move {
            let result = target.wake().await.map_err(|error| error.to_string());
            let _ = tx.send(InternalEvent::AgentWoken {
                runtime,
                agent: target.id(),
                operation,
                result,
            });
        })
        .detach();
    }

    fn selected_target(&self, idx: usize) -> Option<AgentRef> {
        selected_agent(&self.sessions[idx])
    }

    fn selected_pinned_target(&self, idx: usize) -> Option<PinnedCommandTarget> {
        self.ctx
            .command_runtime
            .target_admission(&self.sessions[idx].app.command_target)
    }

    fn pinned_actor(
        &self,
        idx: usize,
        pinned: &PinnedCommandTarget,
    ) -> Result<makima_agent::AgentActorHandle, String> {
        let (manager, root) = self.sessions[idx].handles.manager_and_root();
        if !manager.reference(root).same_manager(&pinned.target) {
            return Err(NOT_LIVE_ERR.into());
        }
        pinned
            .target
            .validate_live()
            .map_err(|error| error.to_string())?;
        pinned.actor()
    }

    fn dispatch_context_reload(
        &self,
        idx: usize,
        pinned: &PinnedCommandTarget,
        reply: flume::Sender<Result<HostResponse, CommandError>>,
    ) -> Result<(), String> {
        let actor = self.pinned_actor(idx, pinned)?;
        let runtime = &self.sessions[idx];
        let admission = pinned.clone();
        let publication = CheckpointPublication::default();
        let registration = runtime.lifetime_cancel.register_publication(&publication);
        let ticket = runtime
            .coordinator
            .reserve_context_reload_for_actor(
                actor,
                move || admission.validate().map_err(ConfigChangeError::from),
                publication,
                publication_claim(),
            )
            .map_err(|error| error.to_string())?;
        let session = runtime.id();
        let identity = runtime.handles.identity();
        let internal = self.internal_tx.clone();
        smol::spawn(async move {
            let _registration = registration;
            let result = ticket.commit().await.map_err(|error| error.to_string());
            let _ = reply.send(
                result
                    .clone()
                    .map(|()| HostResponse::Completed)
                    .map_err(|error| CommandError::Producer(Arc::from(error))),
            );
            let _ = internal.send(InternalEvent::SessionOp {
                session,
                runtime: identity,
                kind: SessionOpKind::ContextReloaded,
                result,
            });
        })
        .detach();
        Ok(())
    }

    fn dispatch_pinned_toggle(
        &mut self,
        idx: usize,
        pinned: &PinnedCommandTarget,
        id: &'static str,
        reply: Option<flume::Sender<Result<HostResponse, CommandError>>>,
    ) -> Result<(), String> {
        self.pinned_actor(idx, pinned)?;
        self.dispatch_target_toggle_on(idx, &pinned.target, Some(pinned), id, reply)
    }

    #[cfg(test)]
    fn dispatch_target_toggle(
        &mut self,
        idx: usize,
        target: &AgentRef,
        id: &'static str,
        reply: Option<flume::Sender<Result<HostResponse, CommandError>>>,
    ) -> Result<(), String> {
        self.dispatch_target_toggle_on(idx, target, None, id, reply)
    }

    fn dispatch_target_toggle_on(
        &mut self,
        idx: usize,
        target: &AgentRef,
        pinned: Option<&PinnedCommandTarget>,
        id: &'static str,
        reply: Option<flume::Sender<Result<HostResponse, CommandError>>>,
    ) -> Result<(), String> {
        self.target_actor(idx, target)?;
        let change = match id {
            FAST_OPTION_ID => ConfigChange::ToggleFast,
            WORKFLOW_OPTION_ID => ConfigChange::ToggleWorkflow,
            _ => return Err(format!("{UNSUPPORTED_AGENT_OPTION_PREFIX}: {id}")),
        };
        let ticket = match pinned {
            Some(pinned) => reserve_pinned_config_change(
                pinned,
                &self.sessions[idx].coordinator,
                Some(Arc::clone(&self.orchestration)),
            ),
            None => reserve_config_change(
                target,
                &self.sessions[idx].coordinator,
                Some(Arc::clone(&self.orchestration)),
            ),
        }
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

    fn dispatch_pinned_directory(
        &self,
        idx: usize,
        pinned: &PinnedCommandTarget,
        path: PathBuf,
    ) -> Result<(), String> {
        self.pinned_actor(idx, pinned)?;
        self.dispatch_target_directory_on(idx, &pinned.target, Some(pinned), path)
    }

    fn dispatch_target_directory(
        &self,
        idx: usize,
        target: &AgentRef,
        path: PathBuf,
    ) -> Result<(), String> {
        self.dispatch_target_directory_on(idx, target, None, path)
    }

    fn dispatch_target_directory_on(
        &self,
        idx: usize,
        target: &AgentRef,
        pinned: Option<&PinnedCommandTarget>,
        path: PathBuf,
    ) -> Result<(), String> {
        self.target_actor(idx, target)?;
        let ticket = match pinned {
            Some(pinned) => reserve_pinned_config_change(
                pinned,
                &self.sessions[idx].coordinator,
                Some(Arc::clone(&self.orchestration)),
            ),
            None => reserve_config_change(
                target,
                &self.sessions[idx].coordinator,
                Some(Arc::clone(&self.orchestration)),
            ),
        }
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
                        *adopted.lock_unpoisoned() =
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
        let checkpoint = self.sessions[idx].checkpoint.clone();
        let registration = self.sessions[idx]
            .coordinator
            .register_publication(&publication)
            .map_err(|error| error.to_string())?;
        Ok(Box::new(move |_, history_changed| {
            Ok(PreparedTransaction {
                publication,
                options: None,
                state_edits: Vec::new(),
                history_changed,
                publish: Box::new(move |snapshot, publication| {
                    Box::pin(async move {
                        let _registration = registration;
                        checkpoint
                            .publish_transaction(snapshot, publication, publication_claim())
                            .await
                            .map_err(|error| error.to_string())
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
    ) -> Result<makima_agent::AgentActorHandle, String> {
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
        target: Option<PinnedCommandTarget>,
        selection: makima_commands::ModelSelection,
        reply: Option<flume::Sender<Result<HostResponse, CommandError>>>,
    ) {
        let (_, root) = self.sessions[idx].handles.manager_and_root();
        let root_spec = target
            .as_ref()
            .filter(|target| target.target.id() == root)
            .and_then(|_| selection.spec.clone());
        let target_policy = target
            .as_ref()
            .map(|target| Arc::clone(&target.template.model_policy));
        let reservation = target
            .ok_or_else(|| CLOSED_TARGET_ERR.into())
            .and_then(|target| {
                self.pinned_actor(idx, &target)?;
                let ticket = reserve_pinned_config_change(
                    &target,
                    &self.sessions[idx].coordinator,
                    Some(Arc::clone(&self.orchestration)),
                )?;
                Ok(ticket)
            });
        let policy = target_policy.unwrap_or_else(|| Arc::clone(&self.ctx.model_policy));
        let prepare = self.sessions[idx]
            .model_slot
            .track_preparation(Arc::clone(&self.ctx.prepare_provider));
        let internal_tx = self.internal_tx.clone();
        let session = self.sessions[idx].id();
        let runtime = self.sessions[idx].handles.identity();
        smol::spawn(async move {
            let result = match reservation {
                Ok(ticket) => commit_model_selection(ticket, &selection, &policy, &prepare)
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
        let policy = Arc::clone(&self.ctx.model_policy);
        let prepare = self.sessions[idx]
            .model_slot
            .track_preparation(Arc::clone(&self.ctx.prepare_provider));
        self.refresh_root_model(idx, None, None, async move {
            prepare_model(&spec, &policy, &prepare).await
        });
    }

    /// The startup prepare for {targets} finished. A failed restore falls
    /// back to the startup model, as a failed resolution does.
    fn complete_startup_prepare(
        &mut self,
        spec: Arc<str>,
        root: bool,
        targets: Vec<(MakimaId, PreparedModel)>,
        result: Result<PreparedModel, String>,
        fallback_waiter: Option<Arc<PendingProvider>>,
    ) {
        let replacement = match result {
            Ok(prepared) => {
                if root {
                    self.ctx
                        .model_slot
                        .install(prepared.model.clone(), Arc::clone(&prepared.provider));
                }
                prepared
            }
            Err(error) if root => {
                return self
                    .focused_app()
                    .flash(format!("failed to prepare model {spec}: {error}"));
            }
            Err(error) => {
                let startup = self.ctx.model_slot.load();
                let fallback = PreparedModel {
                    model: startup.model.clone(),
                    provider: Arc::clone(&startup.provider) as Arc<dyn Provider>,
                };
                drop(startup);
                let warning = restore_warning(&spec, &error, &fallback.model);
                self.focused_app().flash(warning);
                fallback
            }
        };
        let mut fallback_waiter = fallback_waiter;
        for (session, expected) in targets {
            if expected.model.spec() != spec.as_ref() {
                continue;
            }
            if let Some(idx) = self.position(session) {
                let replacement = replacement.clone();
                let slot = Arc::clone(&self.sessions[idx].model_slot);
                self.refresh_root_model(idx, Some(expected), fallback_waiter.take(), async move {
                    let replacement =
                        match PendingProvider::from_provider(replacement.provider.as_ref()) {
                            Some(startup) => startup
                                .prepared()
                                .await
                                .map_err(|error| {
                                    ConfigChangeError::ModelPreparation(error.user_message().into())
                                })?
                                .clone(),
                            None => replacement,
                        };
                    Ok(slot.track_model(replacement))
                });
            }
        }
        if let Some(waiter) = fallback_waiter {
            smol::spawn(async move {
                waiter.fill(Err(CLOSED_TARGET_ERR.into())).await;
            })
            .detach();
        }
    }

    /// Commits {replacement} as the root's model while the root still runs
    /// on {expected}, its current model when `None`.
    fn refresh_root_model(
        &self,
        idx: usize,
        expected: Option<PreparedModel>,
        fallback_waiter: Option<Arc<PendingProvider>>,
        replacement: impl Future<Output = Result<PreparedModel, ConfigChangeError>> + Send + 'static,
    ) {
        let (manager, root) = self.sessions[idx].handles.manager_and_root();
        let reserved = manager
            .actor(root)
            .map_err(ConfigChangeError::from)
            .and_then(|actor| {
                let expected = match expected {
                    Some(expected) => expected,
                    None => {
                        let current = actor
                            .effective_config()
                            .ok_or("missing actor configuration")?;
                        PreparedModel {
                            provider: Arc::clone(&current.provider),
                            model: current.model.clone(),
                        }
                    }
                };
                let target = manager.lookup(root)?;
                let ticket = reserve_config_change(&target, &self.sessions[idx].coordinator, None)?;
                Ok((ticket, expected))
            });
        let session = self.sessions[idx].id();
        let identity = self.sessions[idx].handles.identity();
        let internal_tx = self.internal_tx.clone();
        smol::spawn(async move {
            let result = match reserved {
                Ok((ticket, expected)) => {
                    prepare_config_change(
                        ticket,
                        async move {
                            Ok(ConfigChange::Refresh {
                                expected,
                                replacement: replacement.await?,
                            })
                        },
                        async { smol::Timer::after(SESSION_OP_TIMEOUT).await },
                    )
                    .await
                }
                Err(error) => Err(error),
            }
            .map_err(|error| error.to_string());
            let result = if let Some(waiter) = fallback_waiter {
                // The config-only commit can publish while a turn is waiting for this provider.
                let prepared = result.and_then(|commit| {
                    if PendingProvider::from_provider(commit.config.provider.as_ref()).is_some() {
                        return Err(STARTUP_FALLBACK_NOT_ADOPTED.into());
                    }
                    Ok(PreparedModel {
                        model: commit.config.model.clone(),
                        provider: Arc::clone(&commit.config.provider),
                    })
                });
                waiter
                    .fill(prepared.clone().map_err(|error| {
                        Arc::from(format!("could not commit startup model fallback: {error}"))
                    }))
                    .await;
                prepared.map(|_| ())
            } else {
                result.map(|_| ())
            };
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

    /// The UI slot only seeds new tabs, so it is written directly once its
    /// model re-prepares; the catalog's discovery refreshes live agents.
    fn refresh_discovered_models(&mut self) {
        let current = self.ctx.model_slot.load();
        let model = current.model.clone();
        let provider = current.provider.identity();
        drop(current);
        let providers = Arc::clone(&self.ctx.providers);
        let internal_tx = self.internal_tx.clone();
        smol::spawn(async move {
            let Some(refined) = makima_agent::actor::rediscovered_model(&providers, &model).await
            else {
                return;
            };
            let _ = internal_tx.send(InternalEvent::ModelRediscovered {
                spec: model.spec(),
                provider,
                model: refined,
            });
        })
        .detach();
        self.sync_orchestration();
        self.sync_model_discovery();
    }

    fn apply_rediscovered_model(&mut self, spec: &str, provider: ProviderIdentity, model: Model) {
        let current = self.ctx.model_slot.load();
        if current.model.spec() != spec || current.provider.identity() != provider {
            return;
        }
        let tracked = Arc::clone(&current.provider) as Arc<dyn Provider>;
        drop(current);
        self.ctx.model_slot.project(model, tracked);
    }

    fn refresh_provider(&mut self, idx: usize, slug: String) {
        let model = self.sessions[idx].model_slot.load().model.clone();
        if model.provider.to_string() == slug {
            self.reprepare_root_model(idx, model.spec());
            return;
        }
        let Some(spec) = [ModelTier::Strong, ModelTier::Medium, ModelTier::Weak]
            .into_iter()
            .find_map(|tier| {
                self.ctx
                    .providers
                    .model_for_tier(&slug, tier, &self.ctx.model_policy)
                    .ok()
            })
            .map(|model| model.spec())
        else {
            return;
        };
        self.dispatch_model_selection(
            idx,
            self.selected_pinned_target(idx),
            makima_commands::ModelSelection {
                spec: Some(Arc::from(spec)),
                ..Default::default()
            },
            None,
        );
    }

    fn shutdown(mut self) -> ShutdownReport {
        self.startup_prepares.clear();
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
        for rt in &mut self.sessions {
            rt.cancel_background_work();
        }
        let kill_mcp_ms = lap();
        let heartbeat_deadline = Instant::now() + AGENT_SHUTDOWN_TIMEOUT;
        let mut tabs = Vec::with_capacity(self.sessions.len());
        let mut agent_tasks = Vec::with_capacity(self.sessions.len());
        let mut coordinators = Vec::with_capacity(self.sessions.len());
        let mut session_leases = Vec::with_capacity(self.sessions.len());
        let mut writers = Vec::with_capacity(self.sessions.len());
        for rt in self.sessions.drain(..) {
            let SessionRuntime {
                mut app,
                handles,
                coordinator,
                session_lock,
                lock_lost,
                checkpoint,
                ..
            } = rt;
            let heartbeat_timeout = heartbeat_deadline.saturating_duration_since(Instant::now());
            let settled_lock = settle_lock_state(session_lock, heartbeat_timeout);
            match settled_lock {
                LockSettlement::Held(lease) => {
                    if !lock_lost {
                        app.persist_input();
                    }
                    session_leases.push((app.state.projection.id, lease));
                }
                LockSettlement::TimedOut => {
                    warn!(id = %app.state.projection.id, "session lock heartbeat timed out during shutdown");
                }
                LockSettlement::Lost | LockSettlement::None => {}
            }
            writers.push(checkpoint);
            coordinators.push(coordinator);
            // `app` drops at the end of this iteration, closing the
            // channels the agent loop waits on, so `join_all` can finish.
            tabs.push(handles.manager_and_root().0.journal());
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
        for writer in writers {
            let _ = smol::block_on(bounded_session_op(
                async { writer.drain().await.map_err(|error| error.to_string()) },
                AGENT_SHUTDOWN_TIMEOUT,
            ));
            let status = writer.status();
            if status.unsaved_records > 0 {
                eprintln!(
                    "Warning: exiting with {} unsaved session records{}",
                    status.unsaved_records,
                    status
                        .error
                        .map(|error| format!(": {error}"))
                        .unwrap_or_default()
                );
            }
        }
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
            tabs: tabs.iter().map(|journal| journal.projection()).collect(),
            focused: self.focused,
        }
    }
}

fn live_session_row(app: &App, focused: bool) -> serde_json::Value {
    let projection = app.session_projection();
    json!({
        "id": projection.id,
        "title": projection.title(),
        "status": SessionStatus::of(app).as_str(),
        "updated_at": projection.updated_at,
        "message_count": projection.root_history().map_or(0, |history| history.messages.len()),
        "focused": focused,
    })
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

fn provider_usage_limit(limit: makima_providers::UsageLimit) -> ProviderUsageLimit {
    ProviderUsageLimit {
        window: match limit.kind {
            makima_providers::UsageWindow::Hours(value) => ProviderUsageWindow::Hours { value },
            makima_providers::UsageWindow::Days(value) => ProviderUsageWindow::Days { value },
            makima_providers::UsageWindow::Monthly => ProviderUsageWindow::Monthly,
            makima_providers::UsageWindow::Weekly { model } => {
                ProviderUsageWindow::Weekly { model }
            }
            makima_providers::UsageWindow::Credits => ProviderUsageWindow::Credits,
            makima_providers::UsageWindow::Subscription => ProviderUsageWindow::Subscription,
            makima_providers::UsageWindow::Other(label) => ProviderUsageWindow::Other { label },
        },
        percentage: limit.percentage,
        reset_at_ms: limit.reset_at,
        detail: limit.detail,
        ui_shortname_hint: limit.ui_shortname_hint,
    }
}

fn runtime_config_mutations(
    runtime: &SessionRuntime,
    orchestration: Arc<AgentOrchestration>,
) -> ConfigMutationReserver {
    let coordinator = runtime.coordinator.clone();
    let checkpoint = runtime.checkpoint.clone();
    let orchestration = Arc::downgrade(&orchestration);
    let (manager, root_id) = runtime.handles.manager_and_root();
    let guard = match &runtime.session_lock {
        Some(lock) => lock.publication_guard(),
        _ => None,
    };
    Arc::new(move |target, publication| {
        if !manager.lookup(root_id)?.same_manager(target) {
            return Err(NOT_LIVE_ERR.into());
        }
        if target.id() == root_id {
            target.validate_live()?;
            return Ok(coordinator
                .reserve_transaction(publication)?
                .into_config_mutation());
        }
        let incarnation = target.incarnation()?;
        let manager = manager.clone();
        let validate = move || -> Result<(), ConfigChangeError> {
            manager.validate_activation(&incarnation)?;
            let node = manager.node(incarnation.agent_id)?;
            if !matches!(
                (node.graph_lifecycle, node.residency),
                (GraphLifecycle::Dormant, AgentResidency::Activating)
                    | (GraphLifecycle::Live, AgentResidency::Resident)
            ) {
                return Err(NOT_LIVE_ERR.into());
            }
            Ok(())
        };
        validate()?;
        orchestration
            .upgrade()
            .ok_or(NOT_LIVE_ERR)?
            .validate_target(target)?;
        guard.as_ref().ok_or(LOCK_LOST_REPLACEMENT_ERR)?;
        let checkpoint = checkpoint.clone();
        let registration = coordinator.register_publication(&publication)?;
        Ok(Box::new(move |_, claim| {
            validate()?;
            Ok(PreparedTransaction {
                publication,
                options: None,
                state_edits: Vec::new(),
                history_changed: false,
                publish: Box::new(move |snapshot, publication| {
                    Box::pin(async move {
                        let _registration = registration;
                        checkpoint
                            .publish_transaction(snapshot, publication, claim)
                            .await
                            .map_err(|error| error.to_string())
                    })
                }),
                finalize: Box::new(|_| {}),
            })
        }))
    })
}

fn reserve_pinned_config_change(
    pinned: &PinnedCommandTarget,
    coordinator: &SessionCoordinatorHandle,
    services: Option<Arc<AgentOrchestration>>,
) -> Result<ConfigChangeTicket<'static>, ConfigChangeError> {
    let actor = pinned.actor()?;
    let root = pinned.target.snapshot()?.parent_id.is_none();
    let coordinator = coordinator.clone();
    let pinned = pinned.clone();
    ConfigChangeTicket::reserve_for_actor(
        actor,
        CheckpointPublication::default(),
        move |publication| {
            pinned.validate()?;
            if root {
                return Ok(coordinator
                    .reserve_transaction(publication)?
                    .into_config_mutation());
            }
            services
                .as_deref()
                .ok_or(DURABLE_CONFIG_UNAVAILABLE)?
                .reserve_config_mutation(&pinned.target, publication)
        },
    )
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

async fn commit_model_selection(
    ticket: ConfigChangeTicket<'_>,
    selection: &makima_commands::ModelSelection,
    policy: &ModelPolicy,
    prepare: &ModelPreparer,
) -> Result<ConfigCommit, ConfigChangeError> {
    let change = async {
        Ok(ConfigChange::Patch(
            prepare_selection(selection, policy, prepare).await?,
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

fn command_target_index(
    targets: impl IntoIterator<Item = makima_commands::InvocationTargetId>,
    target: makima_commands::InvocationTargetId,
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
    dispatch_option_change(runtime, id, None, internal_tx);
}

fn dispatch_option_change(
    runtime: &SessionRuntime,
    id: &'static str,
    enabled: Option<bool>,
    internal_tx: &flume::Sender<InternalEvent>,
) {
    let session = runtime.id();
    let identity = runtime.handles.identity();
    let coordinator = runtime.coordinator.clone();
    let committed: Arc<std::sync::Mutex<Option<bool>>> = Arc::default();
    let slot = Arc::clone(&committed);
    let internal_tx = internal_tx.clone();
    smol::spawn(async move {
        let result = match enabled {
            Some(enabled) => coordinator
                .set_option(
                    id,
                    if enabled {
                        ENABLED_VALUE
                    } else {
                        DISABLED_VALUE
                    },
                )
                .await
                .map(|_| enabled),
            None => coordinator
                .toggle_boolean_option(id)
                .await
                .map(|(enabled, _)| enabled),
        }
        .map(|enabled| {
            *slot.lock_unpoisoned() = Some(enabled);
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
    use futures_lite::future::poll_once;
    use makima_agent::actor::{
        ActorError, ActorHistory, ConfigPatch, TurnAdmissionOptions, TurnContext, TurnOrigin,
        TurnProvenance,
    };
    use makima_agent::manager::{AgentMetadata, ResidentAgent};
    use makima_agent::plan_state::content_sha256;
    use makima_agent::{
        ActorBackend, ActorWorkIdentity, ActorWorkKind, AgentInput, BackendResult, BufferSnapshot,
        ControlWork, WorkKind,
    };
    use makima_domain::ThinkingConfig as DomainThinkingConfig;
    use makima_providers::provider::BoxFuture;
    use makima_providers::{
        AgentError, ContentBlock, Invocation, RequestDelivery, RequestOptions, Role, StreamResponse,
    };
    use makima_storage::session_state::{StateChange, StoredStateRecord, StoredTurnOrigin};
    use makima_storage::sessions::HistorySnapshot;

    /// A preparer for tests, from a blocking one over the resolved model,
    /// run off the executor as a real prepare may block.
    pub(super) fn sync_preparer(
        prepare: impl Fn(Model) -> Result<PreparedModel, Arc<str>> + Send + Sync + 'static,
    ) -> ModelPreparer {
        let prepare = Arc::new(prepare);
        Arc::new(move |spec| {
            let prepare = Arc::clone(&prepare);
            Box::pin(async move {
                let model = Model::from_spec(&spec)
                    .map_err(makima_agent::config_change::unresolved_model)?;
                smol::unblock(move || prepare(model))
                    .await
                    .map_err(ConfigChangeError::ModelPreparation)
            })
        })
    }

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
    use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
    use makima_agent::session_options::{
        SessionOptionCategory, SessionOptionDefinition, SessionOptionOwner, SessionOptionValue,
        SessionOptionsSnapshot, YOLO_OPTION_ID,
    };
    use makima_agent::session_writer::SessionIo;
    use makima_agent::tools::ToolRegistry;
    use makima_agent::tools::offload::{MAX_OFFLOAD_SESSION_BYTES, OffloadBackend, OffloadError};
    use makima_agent::{
        AgentId, DoneReason, Envelope, SessionMailbox, SubagentInfo, TurnId, TurnOutcome,
    };
    use makima_config::PermissionsConfig;
    use makima_lua::BuiltinAction;
    use makima_lua::PluginHost;
    use makima_lua::agent_autocmd::TURN_END_EVENT;
    use makima_providers::model::FastSupport;
    use makima_providers::{Effort, ModelCapabilities, TokenUsage};
    use ratatui::{Terminal, backend::TestBackend};
    use std::{
        fs,
        io::{Seek, SeekFrom, Write},
        mem,
        pin::Pin,
        sync::atomic::AtomicBool,
    };
    use tempfile::TempDir;
    use test_case::test_case;

    const OBSERVATION: &str = "failed";
    const PENDING_FAST_MODEL: &str = "pending-fast-model";
    const SWITCHED_FAST_MODEL: &str = "switched-fast-model";
    const DISCOVERY_PROVIDER: &str = "tui-discovery";
    const STARTUP_MODEL_ID: &str = "tui-discovery-startup";
    const RESUMED_MODEL_ID: &str = "tui-discovery-resumed";
    const UNRELATED_MODEL_ID: &str = "tui-discovery-unrelated";
    const BUSY_MODEL_ID: &str = "tui-discovery-busy";
    const DISCOVERED_WINDOW: u32 = 524_000;
    const CWD_OBSERVER_PLUGIN: &str = "cwd-observer";
    const SESSION_STATUS_CHANGED: &str = "SessionStatusChanged";
    const STATUS_COMPLETED: &str = "completed";
    const CWD_OBSERVED_PREFIX: &str = "/cwd-observed-";
    const CWD_OBSERVER: &str = r#"
local fires = 0
makima.api.create_autocmd("SessionCwdChanged", { callback = function(ev)
    fires = fires + 1
    makima.api.register_command({
        name = "/cwd-observed-" .. tostring(fires),
        description = ev.data.cwd,
        tui_only = false,
        handler = function() end,
    })
end })
"#;

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
                input: makima_agent::AgentInput {
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
            assert_eq!(runtime.app.state.projection.model(), selected.model.spec());
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
            let initial_app_model = untouched.app.state.projection.model().to_owned();

            spawn_root_workflow_toggle(&changed).await.unwrap();

            assert!(changed.app.root_config().workflow);
            assert!(changed_actor.effective_config().unwrap().workflow);
            assert!(!untouched.app.root_config().workflow);
            assert_eq!(untouched.app.state.projection.model(), initial_app_model);
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

    struct ForwardedCommandBackend;

    impl ActorBackend for ForwardedCommandBackend {
        fn run_turn<'a>(
            &'a mut self,
            _: ActorHistory<'a>,
            _: TurnContext,
            _: AgentInput,
            _: WorkKind,
        ) -> Pin<Box<dyn Future<Output = BackendResult> + Send + 'a>> {
            Box::pin(std::future::pending())
        }

        fn run_control<'a>(
            &'a mut self,
            _: ActorHistory<'a>,
            _: TurnContext,
            _: &'a ControlWork,
        ) -> Pin<Box<dyn Future<Output = BackendResult> + Send + 'a>> {
            Box::pin(async { BackendResult::ControlDone })
        }

        fn run_compact<'a>(
            &'a mut self,
            _: ActorHistory<'a>,
            _: TurnContext,
            _: Option<&'a str>,
        ) -> Pin<Box<dyn Future<Output = BackendResult> + Send + 'a>> {
            Box::pin(async { BackendResult::CompactDone })
        }
    }

    enum ForwardedEffect {
        Compact,
        Workflow,
        Directory,
        Model,
        OpenPicker,
        Turn,
        IsolatedTurn,
        PickerConfirmation,
    }

    #[test_case(ForwardedEffect::Compact; "compact")]
    #[test_case(ForwardedEffect::Workflow; "workflow")]
    #[test_case(ForwardedEffect::Directory; "directory")]
    #[test_case(ForwardedEffect::Model; "model")]
    #[test_case(ForwardedEffect::OpenPicker; "open_picker")]
    #[test_case(ForwardedEffect::Turn; "turn")]
    #[test_case(ForwardedEffect::IsolatedTurn; "isolated_turn")]
    #[test_case(ForwardedEffect::PickerConfirmation; "picker_confirmation")]
    fn queued_command_rejects_reactivated_target(effect: ForwardedEffect) {
        with_event_loop(|event_loop| {
            let (manager, root) = event_loop.sessions[0].handles.manager_and_root();
            let original = manager.actor(root).unwrap();
            let config = (*original.effective_config().unwrap()).clone();
            let pinned = PinnedCommandTarget::pin(TrustedTarget {
                target: manager.lookup(root).unwrap(),
                template: event_loop.sessions[0]
                    .handles
                    .orchestration_template
                    .clone(),
            })
            .unwrap();
            if matches!(effect, ForwardedEffect::PickerConfirmation) {
                event_loop.sessions[0]
                    .app
                    .open_pinned_model_picker(pinned.clone())
                    .unwrap();
            }
            let target = event_loop.sessions[0].app.command_target.id();
            let (reply, response) = flume::bounded(1);
            let event = match effect {
                ForwardedEffect::Turn => CommandEvent::Outcome {
                    target,
                    outcome: makima_commands::CommandOutcome::AgentTurn(
                        makima_commands::AgentTurn {
                            content: OBSERVATION.into(),
                            prompt: None,
                        },
                    ),
                    admission: Some(Box::new(pinned.clone())),
                },
                ForwardedEffect::IsolatedTurn => CommandEvent::Outcome {
                    target,
                    outcome: makima_commands::CommandOutcome::IsolatedTurn(
                        makima_commands::IsolatedTurn {
                            content: OBSERVATION.into(),
                        },
                    ),
                    admission: Some(Box::new(pinned.clone())),
                },
                _ => CommandEvent::Host {
                    target,
                    request: makima_commands::HostRequest::Builtin(match effect {
                        ForwardedEffect::Compact => BuiltinOperation::Compact(None),
                        ForwardedEffect::Workflow => BuiltinOperation::ToggleWorkflow,
                        ForwardedEffect::Directory => BuiltinOperation::ChangeDirectory {
                            path: config.context.cwd().to_path_buf(),
                        },
                        ForwardedEffect::Model | ForwardedEffect::PickerConfirmation => {
                            BuiltinOperation::SelectModel(makima_commands::ModelSelection {
                                fast: Some(!config.fast),
                                ..Default::default()
                            })
                        }
                        ForwardedEffect::OpenPicker => BuiltinOperation::OpenModelPicker,
                        ForwardedEffect::Turn | ForwardedEffect::IsolatedTurn => unreachable!(),
                    }),
                    admission: Some(Box::new(pinned.clone())),
                    reply,
                },
            };
            manager.detach_incarnation(&pinned.incarnation()).unwrap();
            let (_start, wait) = flume::bounded(1);
            let replacement = smol::block_on(manager.ensure_resident_with(root, |_| async move {
                Ok(ResidentAgent {
                    backend: Box::new(ForwardedCommandBackend),
                    config,
                    shared_messages: None,
                    start: Some(wait),
                    initialize: None,
                })
            }))
            .unwrap()
            .actor()
            .unwrap();
            assert!(!Arc::ptr_eq(&original.identity(), &replacement.identity()));
            assert!(pinned.validate().is_err());
            let before = replacement.config_snapshot().unwrap();
            let history = replacement.committed_history();
            if matches!(effect, ForwardedEffect::PickerConfirmation) {
                let actions = event_loop.sessions[0]
                    .app
                    .update(crate::app::Msg::Key(crate::components::key(KeyCode::Enter)));
                assert!(matches!(
                    actions.as_slice(),
                    [Action::SelectPinnedModel { .. }]
                ));
                event_loop.dispatch(0, actions);
                loop {
                    let event = event_loop
                        .internal_rx
                        .recv_timeout(SESSION_OP_TIMEOUT)
                        .unwrap();
                    if let InternalEvent::SessionOp { result, .. } = &event {
                        assert!(result.is_err());
                        event_loop.handle_internal(event);
                        break;
                    }
                    event_loop.handle_internal(event);
                }
            } else {
                event_loop.handle_command(event);
                if !matches!(
                    effect,
                    ForwardedEffect::Turn | ForwardedEffect::IsolatedTurn
                ) {
                    assert!(matches!(
                        response.recv_timeout(SESSION_OP_TIMEOUT).unwrap(),
                        Err(CommandError::StaleTarget)
                    ));
                    assert!(!event_loop.sessions[0].app.model_picker.is_open());
                }
            }
            let snapshot = replacement.snapshot();
            assert!(snapshot.active_turn.is_none());
            assert_eq!(snapshot.queued, 0);
            assert!(replacement.pending_queue().is_empty());
            assert_eq!(
                replacement.config_snapshot().unwrap().generation,
                before.generation
            );
            assert!(Arc::ptr_eq(&replacement.committed_history(), &history));
        });
    }

    #[test_case(true, false; "closed_before_reservation")]
    #[test_case(false, false; "closed_before_mutation")]
    #[test_case(false, true; "reactivated_before_mutation")]
    fn child_config_adapter_rejects_closed_or_replaced_incarnation(
        close_before_reservation: bool,
        reactivate: bool,
    ) {
        use makima_agent::tools::ToolAudience;
        use makima_storage::session_state::{StoredAgentSpec, StoredModeCeiling};

        with_event_loop(|event_loop| {
            event_loop.sync_orchestration();
            let runtime = &event_loop.sessions[0];
            let (manager, root) = runtime.handles.manager_and_root();
            let root = manager.lookup(root).unwrap();
            let config = (*root.effective_config().unwrap().unwrap()).clone();
            let child = smol::block_on(manager.spawn_child_trusted_with_config(
                &root,
                config.clone(),
                makima_agent::manager::AgentMetadata {
                    transcript_key: Some(OBSERVATION.into()),
                    ..Default::default()
                },
                Vec::new(),
                None,
                |_| Ok::<Box<dyn ActorBackend>, String>(Box::new(ForwardedCommandBackend)),
            ))
            .unwrap();
            let reserve = runtime_config_mutations(runtime, Arc::clone(&event_loop.orchestration));
            if close_before_reservation {
                child.close_subtree().unwrap();
                assert!(reserve(&child, CheckpointPublication::default()).is_err());
                return;
            }
            let mutation = reserve(&child, CheckpointPublication::default()).unwrap();
            if reactivate {
                let incarnation = child.incarnation().unwrap();
                let identity = child.actor().unwrap().identity();
                manager
                    .set_agent_restoration(
                        &incarnation,
                        StoredAgentSpec {
                            model: Default::default(),
                            system: String::new(),
                            tools: Vec::new(),
                            audience: ToolAudience::MAIN.name().unwrap().into(),
                            mcp: false,
                            output_schema: None,
                            name: None,
                            silent: true,
                            auto_deliver: false,
                            inherit_provider: false,
                            mode_ceiling: StoredModeCeiling {
                                restrict_write_to: None,
                                tools: None,
                            },
                            plugin: None,
                        },
                        Ok(()),
                    )
                    .unwrap();
                manager.detach_incarnation(&incarnation).unwrap();
                let replacement = config.clone();
                smol::block_on(manager.ensure_resident_with(child.id(), |_| async move {
                    Ok(ResidentAgent {
                        backend: Box::new(ForwardedCommandBackend),
                        config: replacement,
                        shared_messages: None,
                        start: None,
                        initialize: None,
                    })
                }))
                .unwrap();
                assert_ne!(child.incarnation().unwrap(), incarnation);
                assert!(!Arc::ptr_eq(&child.actor().unwrap().identity(), &identity));
            } else {
                child.close_subtree().unwrap();
            }
            let before = manager.journal().snapshot();
            assert!(mutation(Arc::new(config), publication_claim()).is_err());
            assert_eq!(manager.journal().snapshot().state, before.state);
        });
    }

    #[test]
    fn queued_config_retry_rejects_reactivated_target() {
        with_event_loop(|event_loop| {
            let (manager, root) = event_loop.sessions[0].handles.manager_and_root();
            let original = manager.actor(root).unwrap();
            let config = (*original.effective_config().unwrap()).clone();
            let pinned = PinnedCommandTarget::pin(TrustedTarget {
                target: manager.lookup(root).unwrap(),
                template: event_loop.sessions[0]
                    .handles
                    .orchestration_template
                    .clone(),
            })
            .unwrap();
            let guard = original.prepare_idle().unwrap();
            let ticket =
                reserve_pinned_config_change(&pinned, &event_loop.sessions[0].coordinator, None)
                    .unwrap();
            manager.detach_incarnation(&pinned.incarnation()).unwrap();
            let (_start, wait) = flume::bounded(1);
            let replacement = smol::block_on(manager.ensure_resident_with(root, |_| async move {
                Ok(ResidentAgent {
                    backend: Box::new(ForwardedCommandBackend),
                    config,
                    shared_messages: None,
                    start: Some(wait),
                    initialize: None,
                })
            }))
            .unwrap()
            .actor()
            .unwrap();
            let before = replacement.config_snapshot().unwrap();
            assert!(pinned.validate().is_err());
            drop(guard);
            let result = smol::block_on(bounded_session_op(
                async {
                    ticket
                        .commit_change(ConfigChange::ToggleWorkflow)
                        .await
                        .map_err(|error| error.to_string())
                },
                SESSION_OP_TIMEOUT,
            ));
            assert_eq!(result.err(), Some(ActorError::Shutdown.to_string()));
            assert_eq!(
                replacement.config_snapshot().unwrap().generation,
                before.generation
            );
            assert_eq!(
                replacement.effective_config().unwrap().workflow,
                before.config.workflow
            );
            assert!(replacement.pending_queue().is_empty());
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
                request: makima_commands::HostRequest::Builtin(BuiltinOperation::ToggleWorkflow),
                admission: Some(Box::new(
                    PinnedCommandTarget::pin(TrustedTarget {
                        target: manager.lookup(root).unwrap(),
                        template: event_loop.sessions[0]
                            .handles
                            .orchestration_template
                            .clone(),
                    })
                    .unwrap(),
                )),
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

    #[test_case(false; "live_child_reloads_session_context")]
    #[test_case(true; "closed_child_is_rejected")]
    fn context_reload_from_pinned_child(closed: bool) {
        const UPDATED_INSTRUCTIONS: &str = "Reload these session instructions from the child tab.";
        with_event_loop(|event_loop| {
            let runtime = &event_loop.sessions[0];
            let (manager, root) = runtime.handles.manager_and_root();
            let root = manager.lookup(root).unwrap();
            let config = root.effective_config().unwrap().unwrap();
            let access = root.state_access();
            smol::block_on(runtime.coordinator.reload_context()).unwrap();
            let before = root_context(&access).unwrap().unwrap();
            assert!(!before.instructions.text.contains(UPDATED_INSTRUCTIONS));
            std::fs::write(config.context.cwd().join("AGENTS.md"), UPDATED_INSTRUCTIONS).unwrap();
            let child = smol::block_on(manager.spawn_child_trusted_with_config(
                &root,
                (*config).clone(),
                Default::default(),
                Vec::new(),
                None,
                |_| Ok::<Box<dyn ActorBackend>, String>(Box::new(ForwardedCommandBackend)),
            ))
            .unwrap();
            let admission = PinnedCommandTarget::pin(TrustedTarget {
                target: child.clone(),
                template: runtime.handles.orchestration_template.clone(),
            })
            .unwrap();
            if closed {
                child.close_subtree().unwrap();
            }
            let (reply, response) = flume::bounded(1);
            event_loop.handle_command(CommandEvent::Host {
                target: runtime.app.command_target.id(),
                request: makima_commands::HostRequest::Builtin(BuiltinOperation::ReloadContext),
                admission: Some(Box::new(admission)),
                reply,
            });
            let result = response.recv_timeout(SESSION_OP_TIMEOUT).unwrap();
            let recorded = root_context(&access).unwrap().unwrap();
            if closed {
                assert!(matches!(result, Err(CommandError::StaleTarget)));
                assert_eq!(recorded.instructions.text, before.instructions.text);
            } else {
                assert_eq!(result.unwrap(), HostResponse::Completed);
                assert!(recorded.instructions.text.contains(UPDATED_INSTRUCTIONS));
                let child_context = root_context(&child.state_access()).unwrap().unwrap();
                assert_eq!(child_context.instructions.text, recorded.instructions.text);
            }
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
            assert!(plan.starts_with(storage.path().join(makima_storage::plans::PLANS_DIR)));
            assert_eq!(
                config.mode_def,
                EventHandle::disconnected_for_test()
                    .mode_registry()
                    .get(&makima_agent::ModeId::Plan)
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

            let event = bounded_session_op(
                async {
                    internal_rx
                        .recv_async()
                        .await
                        .map_err(|error| error.to_string())
                },
                RUNTIME_SHUTDOWN_TIMEOUT,
            )
            .await
            .expect("YOLO toggle must not wait for the pending actor operation");
            let InternalEvent::SessionOp {
                kind: SessionOpKind::OptionToggled { committed, .. },
                result,
                ..
            } = event
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

    #[test_case(true ; "set_yolo")]
    #[test_case(false ; "set_normal")]
    fn permission_selection_sets_session_option_without_toggling(enabled: bool) {
        smol::block_on(async {
            let runtime = test_runtime(model_named("test-model"));
            let untouched = test_runtime(model_named("test-model"));
            let untouched_snapshot = untouched.coordinator.read().options();
            let (internal_tx, internal_rx) = flume::unbounded();
            for _ in 0..2 {
                dispatch_option_change(&runtime, YOLO_OPTION_ID, Some(enabled), &internal_tx);
                let InternalEvent::SessionOp {
                    kind: SessionOpKind::OptionToggled { committed, .. },
                    result,
                    ..
                } = internal_rx.recv_async().await.unwrap()
                else {
                    panic!("expected option selection completion");
                };
                result.unwrap();
                assert_eq!(committed.lock().unwrap().take(), Some(enabled));
            }
            let session = runtime.handles.manager_and_root().0.journal().snapshot();
            assert_eq!(
                session
                    .fold()
                    .unwrap()
                    .scopes
                    .get(&None)
                    .and_then(|scope| scope.yolo),
                Some(enabled)
            );
            assert_eq!(
                untouched.coordinator.read().options().options,
                untouched_snapshot.options,
            );
            release_runtime(runtime);
            release_runtime(untouched);
        });
    }

    fn fast_model(id: &str, support: FastSupport) -> Model {
        let mut model = model_named(id);
        model.fast_support = Some(support);
        model
    }

    fn model_change(model: Model) -> ConfigChange {
        ConfigChange::Patch(ConfigPatch {
            model: Some(makima_agent::actor::PreparedModel {
                provider: Arc::new(StubProvider),
                model,
            }),
            ..Default::default()
        })
    }

    fn pending_fast_runtime() -> SessionRuntime {
        let runtime = test_runtime(fast_model(PENDING_FAST_MODEL, FastSupport::Pending));
        let (manager, root) = runtime.handles.manager_and_root();
        let journal = manager.journal();
        let snapshot = journal.snapshot();
        let mut config = snapshot
            .state
            .iter()
            .rev()
            .find_map(|record| match &record.change {
                makima_storage::session_state::StateChange::Config(config)
                    if record.agent == Some(root) =>
                {
                    Some(config.clone())
                }
                _ => None,
            })
            .unwrap();
        config.options.insert(
            makima_agent::session_options::FAST_PENDING_MODEL_OPTION_ID.into(),
            config.model.clone(),
        );
        journal
            .commit(makima_agent::journal::JournalCommit {
                changes: vec![makima_agent::journal::JournalChange {
                    agent: Some(root),
                    generation: 0,
                    change: makima_storage::session_state::StateChange::Config(config),
                }],
                ..Default::default()
            })
            .unwrap();
        runtime
    }

    fn pending_fast_model(runtime: &SessionRuntime) -> Option<String> {
        let (manager, root) = runtime.handles.manager_and_root();
        manager
            .journal()
            .snapshot()
            .state
            .iter()
            .rev()
            .find_map(|record| match &record.change {
                makima_storage::session_state::StateChange::Config(config)
                    if record.agent == Some(root) =>
                {
                    Some(
                        config
                            .options
                            .get(makima_agent::session_options::FAST_PENDING_MODEL_OPTION_ID)
                            .cloned(),
                    )
                }
                _ => None,
            })
            .flatten()
    }

    async fn discover_fast_support(
        actor: &makima_agent::AgentActorHandle,
    ) -> makima_agent::actor::ConfigCommit {
        let current = actor.effective_config().unwrap();
        actor
            .reserve_prepared_operation(None)
            .unwrap()
            .commit_when_ready(Some(ConfigChange::Refresh {
                expected: PreparedModel {
                    provider: Arc::clone(&current.provider),
                    model: current.model.clone(),
                },
                replacement: PreparedModel {
                    provider: Arc::clone(&current.provider),
                    model: fast_model(PENDING_FAST_MODEL, FastSupport::Supported),
                },
            }))
            .await
            .unwrap()
            .config
    }

    async fn settled_config(
        actor: &makima_agent::AgentActorHandle,
    ) -> Arc<makima_agent::EffectiveAgentConfig> {
        actor
            .reserve_prepared_operation(None)
            .unwrap()
            .wait_predecessor()
            .await
            .unwrap()
    }

    #[test_case(None, PENDING_FAST_MODEL; "queued_change_restores_once")]
    #[test_case(Some(SWITCHED_FAST_MODEL), SWITCHED_FAST_MODEL; "queued_model_switch_inherits_committed_fast")]
    fn restored_fast_lands_on_the_discovered_model_once(switch: Option<&str>, settled_model: &str) {
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
            assert!(settled.fast);
            assert_eq!(commits.try_iter().count(), 2);
            assert!(pending_fast_model(&runtime).is_none());
            release_runtime(runtime);
        });
    }

    #[test]
    fn model_switch_supersedes_pending_fast_before_stale_discovery() {
        smol::block_on(async {
            let mut runtime = pending_fast_runtime();
            let (manager, root) = runtime.handles.manager_and_root();
            let actor = manager.actor(root).unwrap();
            let original = actor.effective_config().unwrap();
            let switch = actor.reserve_prepared_operation(None).unwrap();
            let discovery = actor.reserve_prepared_operation(None).unwrap();
            let switched = switch
                .commit_when_ready(Some(model_change(fast_model(
                    SWITCHED_FAST_MODEL,
                    FastSupport::Supported,
                ))))
                .await
                .unwrap()
                .config;
            assert!(runtime.project_config(&switched));
            assert!(!switched.config.fast);
            assert!(pending_fast_model(&runtime).is_none());
            let discovered = discovery
                .commit_when_ready(Some(ConfigChange::Refresh {
                    expected: PreparedModel {
                        provider: Arc::clone(&original.provider),
                        model: original.model.clone(),
                    },
                    replacement: PreparedModel {
                        provider: Arc::clone(&original.provider),
                        model: fast_model(PENDING_FAST_MODEL, FastSupport::Supported),
                    },
                }))
                .await
                .unwrap()
                .config;
            assert_eq!(discovered.generation, switched.generation);
            assert!(!runtime.project_config(&discovered));
            let settled = settled_config(&actor).await;
            assert_eq!(&*settled.model.id, SWITCHED_FAST_MODEL);
            assert!(!settled.fast);
            assert!(pending_fast_model(&runtime).is_none());
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
                smol::block_on(async {
                    assert!(!settled_config(&actor).await.fast);
                    assert!(pending_fast_model(&event_loop.sessions[0]).is_none());
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
            model.thinking_support = Some(makima_providers::ThinkingSupport::Yes);
            let combined = smol::spawn(combined.commit_when_ready(Some(ConfigChange::Patch(
                ConfigPatch {
                    model: Some(makima_agent::actor::PreparedModel {
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
                            model: Some(makima_agent::actor::PreparedModel {
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
                event_loop.sessions[0].coordinator.retire();
                let mut replacement =
                    test_runtime_with_id(model_named("replacement-model"), session);
                let replacement_thinking = replacement.app.root_config().thinking;
                let (replacement_manager, replacement_root) =
                    replacement.handles.manager_and_root();
                let replacement_actor = replacement_manager.actor(replacement_root).unwrap();
                let initial_config = replacement_actor.config_snapshot().unwrap();
                let (queued_tx, queued_rx) = flume::unbounded();
                replacement.config_commits = queued_rx;
                let old = std::mem::replace(&mut event_loop.sessions[0], replacement);
                event_loop.handle_internal(completion);
                queued_tx.send(commit).unwrap();
                let _ = event_loop.tick();
                let app = &event_loop.sessions[0].app;
                assert_eq!(
                    app.state.projection.model(),
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
                    model: Some(makima_agent::actor::PreparedModel {
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
                input: makima_agent::AgentInput {
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
                    option.definition.id.as_ref() == makima_agent::session_options::YOLO_OPTION_ID
                })
                .unwrap()
                .current_value
                .clone();
            assert_eq!(yolo.as_ref(), makima_agent::session_options::DISABLED_VALUE);
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
        let (models, providers) = discovery_providers();
        let spec = |id: &str| format!("{DISCOVERY_PROVIDER}/{id}");
        with_event_loop_runtime_providers(
            &providers,
            |harness| {
                let startup = providers.model(&spec(STARTUP_MODEL_ID)).unwrap();
                harness.ctx.as_mut().unwrap().model_slot =
                    ProviderSlot::new(startup, Arc::new(StubProvider)).0;
                let resumed = (session_model != STARTUP_MODEL_ID).then(|| PreparedProvider {
                    model: providers.model(&spec(session_model)).unwrap(),
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
                discover_window(&models, &providers, discovered);

                event_loop.sync_orchestration();
                smol::block_on(
                    event_loop
                        .ctx
                        .catalog
                        .apply(Vec::new(), Some(DISCOVERY_PROVIDER.into())),
                );
                event_loop.handle_internal(InternalEvent::ModelsDiscovered);
                let mut awaiting = refreshed;
                while awaiting {
                    let event = event_loop.internal_rx.recv().unwrap();
                    awaiting = !matches!(event, InternalEvent::ModelRediscovered { .. });
                    event_loop.handle_internal(event);
                }

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
        let (models, providers) = discovery_providers();
        let spec = format!("{DISCOVERY_PROVIDER}/{BUSY_MODEL_ID}");
        with_event_loop_runtime_providers(
            &providers,
            |harness| {
                let startup = providers.model(&spec).unwrap();
                harness.ctx.as_mut().unwrap().model_slot =
                    ProviderSlot::new(startup, Arc::new(StubProvider)).0;
                harness
                    .ctx()
                    .spawn_runtime_with_provider(harness.session(), None)
                    .unwrap()
            },
            |event_loop| {
                event_loop.sync_orchestration();
                discover_window(&models, &providers, BUSY_MODEL_ID);
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
                let mut refresh = Box::pin(
                    event_loop
                        .ctx
                        .catalog
                        .apply(Vec::new(), Some(DISCOVERY_PROVIDER.into())),
                );

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

    fn test_runtime(model: Model) -> SessionRuntime {
        test_runtime_with_id(model, MakimaId::generate())
    }

    fn test_runtime_with_id(model: Model, id: MakimaId) -> SessionRuntime {
        let mut harness = RuntimeHarness::new();
        let ctx = harness.ctx.as_mut().unwrap();
        ctx.model_slot = ProviderSlot::new(model, Arc::new(StubProvider)).0;
        let mut session = harness.session();
        session.id = id;
        let mut runtime = harness.runtime(session);
        runtime.test_storage = Some(harness._temp_dir);
        runtime
    }

    struct StubProvider;

    impl Provider for StubProvider {
        fn stream_message<'a>(
            &'a self,
            _model: &'a Model,
            _messages: &'a [Message],
            _system: &'a str,
            _tools: &'a serde_json::Value,
            _event_tx: &'a flume::Sender<makima_providers::ProviderEvent>,
            _opts: makima_providers::RequestOptions,
            _session_id: Option<&'a SessionRef>,
            _invocation: &'a Invocation,
            _delivery: RequestDelivery,
        ) -> makima_providers::provider::BoxFuture<
            'a,
            Result<makima_providers::StreamResponse, makima_providers::AgentError>,
        > {
            Box::pin(std::future::pending())
        }
    }

    struct FocusedProvider;

    /// Tells [`FocusedProvider`] apart from every other test provider.
    const FOCUSED_KEY_COUNT: usize = 7;

    impl Provider for FocusedProvider {
        fn stream_message<'a>(
            &'a self,
            _model: &'a Model,
            _messages: &'a [Message],
            _system: &'a str,
            _tools: &'a serde_json::Value,
            _event_tx: &'a flume::Sender<makima_providers::ProviderEvent>,
            _opts: makima_providers::RequestOptions,
            _session_id: Option<&'a SessionRef>,
            _invocation: &'a Invocation,
            _delivery: RequestDelivery,
        ) -> makima_providers::provider::BoxFuture<
            'a,
            Result<makima_providers::StreamResponse, makima_providers::AgentError>,
        > {
            Box::pin(std::future::pending())
        }

        fn key_count(&self) -> usize {
            FOCUSED_KEY_COUNT
        }
    }

    const RUNTIME_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(1);
    const PUMP_TIMEOUT: Duration = Duration::from_secs(10);

    /// Runs the loop's internal, agent and tick work until {done} holds.
    pub(super) fn pump_until(
        event_loop: &mut EventLoop<'_>,
        mut done: impl FnMut(&EventLoop<'_>) -> bool,
    ) {
        let deadline = Instant::now() + PUMP_TIMEOUT;
        loop {
            while let Ok(event) = event_loop.internal_rx.try_recv() {
                event_loop.handle_internal(event);
            }
            for index in 0..event_loop.sessions.len() {
                while let Some(SessionEvent::Agent(envelope)) =
                    event_loop.sessions[index].handles.events.try_next()
                {
                    event_loop
                        .handle_wake(Wake::Agent(index, Box::new(envelope)))
                        .unwrap();
                }
            }
            let _ = event_loop.tick();
            if done(event_loop) {
                return;
            }
            assert!(Instant::now() < deadline, "event processing timed out");
            std::thread::yield_now();
        }
    }
    const TEST_DISCOVERY_TIMEOUT: Duration = Duration::from_secs(5);

    pub(super) type DiscoveredModels = Arc<std::sync::Mutex<Vec<makima_providers::ModelInfo>>>;

    /// A registry with the discovery provider, and the models its discovery
    /// reports.
    pub(super) fn discovery_providers() -> (DiscoveredModels, Arc<ProviderRegistry>) {
        let models = DiscoveredModels::default();
        let providers =
            makima_lua::test_support::offline_registry(DISCOVERY_PROVIDER, Arc::clone(&models));
        (models, providers)
    }

    /// Discovery reports {model_id} with the discovered window.
    fn discover_window(models: &DiscoveredModels, providers: &ProviderRegistry, model_id: &str) {
        *models.lock().unwrap() = vec![makima_providers::ModelInfo {
            context_window: Some(DISCOVERED_WINDOW),
            ..makima_providers::ModelInfo::id_only(model_id.into())
        }];
        smol::block_on(providers.fetch_all(&ModelPolicy::default(), false, |_| {}));
    }

    fn with_event_loop_runtime_providers(
        providers: &Arc<ProviderRegistry>,
        runtime: impl FnOnce(&mut RuntimeHarness) -> SessionRuntime,
        test: impl FnOnce(&mut EventLoop<'_>),
    ) {
        let catalog = ModelCatalog::offline(Arc::clone(providers), &ModelPolicy::default());
        run_event_loop(
            RuntimeHarness::with(PermissionsConfig::default(), Arc::clone(providers), catalog),
            runtime,
            test,
        );
    }

    pub(super) fn with_event_loop(test: impl FnOnce(&mut EventLoop<'_>)) {
        with_event_loop_runtime(|harness| harness.runtime(harness.session()), test);
    }

    #[test_case(false; "queued_actor_wake")]
    #[test_case(true; "waiting_for_actor_wake")]
    fn actor_queue_settlement_flows_through_the_session_pump(waiting: bool) {
        with_event_loop(|event_loop| {
            let (_input_tx, input_rx) = flume::unbounded();
            event_loop.input = InputReader::from_receiver(input_rx);
            let (actor_tx, actor_rx) = flume::unbounded();
            let runtime = &mut event_loop.sessions[0];
            runtime.handles.actor_rx = actor_rx;
            while runtime.handles.events.try_next().is_some() {}
            let observer = runtime.handles.session.events.subscribe();
            let (_, root) = runtime.handles.manager_and_root();
            let identity = ActorWorkIdentity::new(
                root,
                ActorWorkKind::Control { operation_id: 0 },
                runtime.app.run_id,
            );
            runtime.notifications.pending_completion = Some(
                PendingCompletion::WaitingForQueueDrain(Notification::error_completion()),
            );
            let settled = ActorEvent::Idle {
                agent_id: root,
                settled: Some(identity),
            };
            let wake = if waiting {
                smol::block_on(async {
                    let mut wake = Box::pin(
                        event_loop.wait_for_wake(GLOBAL_WAKE_SOURCES + SESSION_WAKE_SOURCES),
                    );
                    assert!(poll_once(&mut wake).await.is_none());
                    actor_tx.send(settled).unwrap();
                    bounded_session_op(async { Ok(wake.await.1) }, PUMP_TIMEOUT)
                        .await
                        .unwrap()
                })
            } else {
                actor_tx.send(settled).unwrap();
                event_loop.next_wake(Duration::ZERO).unwrap()
            };
            assert!(matches!(wake, Wake::Actor(0, ActorEvent::Idle { .. })));
            event_loop.handle_wake(wake).unwrap();
            assert!(!event_loop.sessions[0].handles.events.is_empty());

            let wake = event_loop.next_wake(Duration::ZERO).unwrap();
            assert!(
                matches!(&wake, Wake::Agent(0, event) if event.identity == Some(identity)
                && matches!(event.event, AgentEvent::QueueSettled))
            );
            event_loop.handle_wake(wake).unwrap();
            assert!(!event_loop.sessions[0].notifications.waiting_for_drain());
            assert!(
                matches!(observer.try_next(), Some(SessionEvent::Agent(event))
                if event.identity == Some(identity) && matches!(event.event, AgentEvent::QueueSettled))
            );
        });
    }

    fn reopen_app(
        harness: RuntimeHarness,
        session_id: MakimaId,
        test: impl FnOnce(&mut EventLoop<'_>),
    ) {
        run_event_loop(
            harness,
            |harness| {
                let session = AppSession::load(session_id, &harness.ctx().storage).unwrap();
                harness.runtime(session)
            },
            test,
        );
    }

    /// Like `with_event_loop`, with {providers} as the registry.
    pub(super) fn with_event_loop_providers(
        providers: Arc<ProviderRegistry>,
        test: impl FnOnce(&mut EventLoop<'_>),
    ) {
        let catalog = ModelCatalog::offline(Arc::clone(&providers), &ModelPolicy::default());
        run_event_loop(
            RuntimeHarness::with(PermissionsConfig::default(), providers, catalog),
            |harness| harness.runtime(harness.session()),
            test,
        );
    }

    fn with_event_loop_runtime(
        runtime: impl FnOnce(&mut RuntimeHarness) -> SessionRuntime,
        test: impl FnOnce(&mut EventLoop<'_>),
    ) {
        run_event_loop(RuntimeHarness::new(), runtime, test);
    }

    fn test_tty() -> ratatui::DefaultTerminal {
        let backend = ratatui::backend::CrosstermBackend::new(std::io::stdout());
        Terminal::with_options(
            backend,
            ratatui::TerminalOptions {
                viewport: ratatui::Viewport::Fixed(ratatui::layout::Rect::new(0, 0, 80, 24)),
            },
        )
        .unwrap()
    }

    fn run_event_loop(
        mut harness: RuntimeHarness,
        runtime: impl FnOnce(&mut RuntimeHarness) -> SessionRuntime,
        test: impl FnOnce(&mut EventLoop<'_>),
    ) {
        let runtime = runtime(&mut harness);
        let ctx = harness.ctx.take().unwrap();
        let initial_provider = ctx.model_slot.load().provider.identity();
        let mut terminal = test_tty();
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
            next_wake_source: 0,
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
            startup_prepares: Vec::new(),
        };
        test(&mut event_loop);
        smol::block_on(
            event_loop
                .model_discovery
                .wait_active(TEST_DISCOVERY_TIMEOUT),
        );
        drop(event_loop.model_discovery);
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
                    conversation_id: None,
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
                    cancel: None,
                }),
                identity: None,
            }),
        );
        let app = &mut event_loop.sessions[0].app;
        app.active_chat = app.agent_chat_index[&child];
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
            event_loop.ctx.prepare_provider = sync_preparer(|model| {
                Ok(makima_agent::actor::PreparedModel {
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
                    outcome: makima_commands::CommandOutcome::Completed,
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
                assert_eq!(app.model_picker_target.as_ref().unwrap().target.id(), root);
            }
        });
    }

    struct RuntimeHarness {
        _temp_dir: TempDir,
        _host: PluginHost,
        ctx: Option<SpawnCtx>,
    }

    impl RuntimeHarness {
        fn with_permissions_config(permissions_config: PermissionsConfig) -> Self {
            Self::in_dir(tempfile::tempdir().unwrap(), permissions_config)
        }

        fn in_dir(temp_dir: TempDir, permissions_config: PermissionsConfig) -> Self {
            let providers = Arc::new(makima_providers::test_util::offline_ids(&[
                crate::components::TEST_MODEL_PROVIDER,
                "openai",
                "ollama",
            ]));
            Self::with_dir(
                temp_dir,
                permissions_config,
                providers,
                ModelCatalog::default(),
            )
        }

        fn with(
            permissions_config: PermissionsConfig,
            providers: Arc<ProviderRegistry>,
            catalog: ModelCatalog,
        ) -> Self {
            Self::with_dir(
                tempfile::tempdir().unwrap(),
                permissions_config,
                providers,
                catalog,
            )
        }

        fn with_dir(
            temp_dir: TempDir,
            permissions_config: PermissionsConfig,
            providers: Arc<ProviderRegistry>,
            catalog: ModelCatalog,
        ) -> Self {
            let storage = StateDir::from_path(temp_dir.path().to_path_buf());
            let sessions_dir = storage.ensure_subdir(SESSIONS_DIR).unwrap();
            let command_registry = makima_commands::CommandRegistry::new();
            let command_runtime = Arc::new(CommandRuntime::new_for_test(
                &[],
                command_registry,
                Arc::new(ModelArgSource::new(
                    catalog.specs(),
                    crate::model_capabilities::test_support::seeded(&[], &[]),
                )),
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
            let host = PluginHost::with_command_registry(
                Arc::new(ToolRegistry::new()),
                command_runtime.registry.clone(),
                false,
                makima_lua::ui_sink::HostUi::Frontend,
            )
            .unwrap();
            let ctx = SpawnCtx {
                initial_model_override: false,
                workspace_cwd: temp_dir.path().to_string_lossy().into_owned(),
                file_write_locks: Arc::new(FileWriteLocks::new()),
                prepare_provider: default_model_preparer(Arc::clone(&providers)),
                providers,
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
                lua_event_handle: host.event_handle(),
                mcp_handle: None,
                mcp_config_errors: McpConfigErrors::new(temp_dir.path().to_path_buf()),
                model_slot,
                catalog,
                model_policy: Arc::new(ModelPolicy::default()),
                session_defaults: SessionDefaults::default(),
                project_config: ProjectConfig::for_project(temp_dir.path()),
                system_prompt: SystemPromptOverride::default(),
                command_runtime,
                model_capabilities: ModelCapabilityCache::new(|_| ModelCapabilities::NONE, || {}),
                trust_question: None,
            };
            Self {
                _temp_dir: temp_dir,
                _host: host,
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
            AppSession::new(&self.ctx().model_slot.load().model.spec(), cwd.as_ref())
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

        fn append_fixture(&self, session: &AppSession, guard: SessionPublicationGuard) {
            let saved = AppSession::load(session.id, &self.ctx().storage).unwrap();
            let through = saved
                .state()
                .iter()
                .map(|record| record.seq)
                .chain(saved.agent_revisions().iter().map(|(seq, _)| *seq))
                .max();
            let mut records: Vec<_> = session
                .state()
                .iter()
                .filter(|record| through.is_none_or(|seq| record.seq > seq))
                .map(|record| (record.seq, SessionRecord::State { d: record.clone() }))
                .chain(
                    session
                        .agent_revisions()
                        .iter()
                        .filter(|(seq, _)| through.is_none_or(|through| *seq > through))
                        .map(|(seq, record)| {
                            (
                                *seq,
                                SessionRecord::Agent {
                                    seq: *seq,
                                    d: record.clone(),
                                },
                            )
                        }),
                )
                .collect();
            records.sort_by_key(|(seq, _)| *seq);
            let writer = SessionWriter::open_session(&self.ctx().sessions_dir, session).unwrap();
            smol::block_on(writer.append_owned(
                records.into_iter().map(|(_, record)| record).collect(),
                guard,
            ))
            .unwrap();
        }

        fn target_count(&self) -> usize {
            self.ctx().command_runtime.registry.target_count()
        }

        fn persist_runtime(&self, runtime: &mut SessionRuntime) {
            runtime.app.persist_input();
            smol::block_on(runtime.handles.manager_and_root().0.journal().flush()).unwrap();
        }

        fn restart(mut self, mut runtime: SessionRuntime) -> Self {
            self.persist_runtime(&mut runtime);
            release_runtime(runtime);
            self.shutdown_writer();
            let temp_dir = mem::replace(&mut self._temp_dir, tempfile::tempdir().unwrap());
            drop(self);
            Self::in_dir(temp_dir, PermissionsConfig::default())
        }

        fn snapshot_crash(self, runtime: SessionRuntime) -> Self {
            let recovery = tempfile::tempdir().unwrap();
            copy_crash_snapshot(self._temp_dir.path(), recovery.path());
            release_runtime(runtime);
            drop(self);
            Self::in_dir(recovery, PermissionsConfig::default())
        }

        fn shutdown_writer(&mut self) -> StateDir {
            let ctx = self.ctx.take().unwrap();
            let storage = ctx.storage.clone();
            drop(ctx);
            storage
        }
    }

    fn copy_crash_snapshot(source: &Path, destination: &Path) {
        fs::create_dir_all(destination).unwrap();
        for entry in fs::read_dir(source).unwrap() {
            let entry = entry.unwrap();
            let name = entry.file_name();
            let name_str = name.to_string_lossy();
            if name_str.ends_with(".lock") || name_str.ends_with(".lock.owner") {
                continue;
            }
            let target = destination.join(&name);
            let file_type = entry.file_type().unwrap();
            if file_type.is_dir() {
                copy_crash_snapshot(&entry.path(), &target);
            } else {
                assert!(file_type.is_file(), "crash snapshot requires regular files");
                fs::copy(entry.path(), target).unwrap();
            }
        }
    }

    pub(super) fn shutdown_manager(manager: &makima_agent::AgentManagerHandle) {
        let report = smol::block_on(manager.shutdown(RUNTIME_SHUTDOWN_TIMEOUT));
        assert!(report.timed_out.is_empty());
    }

    pub(super) fn release_runtime(runtime: SessionRuntime) {
        let SessionRuntime {
            handles,
            session_lock,
            test_storage: _storage,
            ..
        } = runtime;
        let manager = handles.manager_and_root().0;
        shutdown_manager(&manager);
        release_lock_state(session_lock).unwrap();
        drop(handles);
    }

    const REWIND_TARGET: &str = "rewind target";
    const REWIND_DRAFT: &str = "unfinished draft";
    const REWIND_QUEUED: &str = "queued user turn";
    const REWIND_IO_ERROR: &str = "injected fork I/O failure";
    const REWIND_NOTE: &str = "This conversation was rewound.";

    #[derive(Clone, Copy, PartialEq, Eq)]
    enum ForkFailure {
        Write,
        Sync,
        Rollback,
    }

    struct FailingForkIo {
        log: fs::File,
        failure: ForkFailure,
        writing_fork: bool,
    }

    impl SessionIo for FailingForkIo {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            if self.writing_fork && self.failure == ForkFailure::Write {
                return Err(io::Error::other(REWIND_IO_ERROR));
            }
            self.log.write(bytes)
        }

        fn sync(&mut self) -> io::Result<()> {
            if self.writing_fork && self.failure != ForkFailure::Write {
                return Err(io::Error::other(REWIND_IO_ERROR));
            }
            self.log.sync_all()
        }

        fn position(&mut self) -> io::Result<u64> {
            self.writing_fork = true;
            self.log.seek(SeekFrom::End(0))
        }

        fn truncate(&mut self, len: u64) -> io::Result<()> {
            if self.failure == ForkFailure::Rollback {
                return Err(io::Error::other(REWIND_IO_ERROR));
            }
            self.writing_fork = false;
            self.log.set_len(len)
        }
    }

    fn rewind_runtime(
        harness: &mut RuntimeHarness,
        failure: Option<ForkFailure>,
    ) -> SessionRuntime {
        let mut session = harness.session();
        session.push_message(Message::user(REWIND_TARGET.into()));
        let ctx = harness.ctx();
        let prepared = ctx.prepare_runtime(session).unwrap();
        let mut runtime = prepared.activate(&ctx.model_slot, None).unwrap();
        if let Some(failure) = failure {
            let id = runtime.id();
            let log = fs::OpenOptions::new()
                .read(true)
                .append(true)
                .open(ctx.sessions_dir.join(format!("{id}.jsonl")))
                .unwrap();
            let next_seq = AppSession::load(id, &ctx.storage).unwrap().next_state_seq();
            let writer = SessionWriter::with_io(
                id,
                FailingForkIo {
                    log,
                    failure,
                    writing_fork: false,
                },
                next_seq,
            );
            let guard = runtime.session_lock.as_ref().unwrap().publication_guard();
            writer.bind_publication_guard(guard);
            runtime.checkpoint = SessionPersistence::from_writer(Arc::new(writer.clone()));
            runtime
                .handles
                .session
                .manager
                .journal()
                .replace_writer_for_test(writer);
        }
        runtime.app.restore_display();
        runtime
    }

    fn select_rewind(event_loop: &mut EventLoop<'_>) -> InternalEvent {
        let app = &mut event_loop.sessions[0].app;
        let actions = app.update(Msg::Key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE)));
        assert!(matches!(actions.as_slice(), [Action::CommitRewind(_)]));
        assert!(!app.has_modal_overlay());
        event_loop.dispatch(0, actions);
        loop {
            let completion = event_loop.internal_rx.recv_timeout(PUMP_TIMEOUT).unwrap();
            if matches!(completion, InternalEvent::RewindCommitted { .. }) {
                return completion;
            }
        }
    }

    #[test_case(false; "child_stays_focused")]
    #[test_case(true; "root_focused_before_commit")]
    fn rewind_picker_targets_viewed_child_after_navigation(navigate: bool) {
        const CHILD_NAME: &str = "rewind child";
        const CHILD_KEY: &str = "rewind-child";
        const CHILD_PROMPT: &str = "child rewind target";
        const CHILD_QUEUED: &str = "queued child turn";
        const EDITED_PROMPT: &str = "edited child draft";

        with_event_loop_runtime(
            |harness| rewind_runtime(harness, None),
            |event_loop| {
                let (manager, root_id) = event_loop.sessions[0].handles.manager_and_root();
                let root = manager.lookup(root_id).unwrap();
                let root_actor = root.actor().unwrap();
                let config = (*root_actor.effective_config().unwrap()).clone();
                let child = smol::block_on(manager.spawn_child_trusted_with_config(
                    &root,
                    config,
                    AgentMetadata {
                        transcript_key: Some(CHILD_KEY.into()),
                        ..Default::default()
                    },
                    vec![Message::user(CHILD_PROMPT.into())],
                    None,
                    |_| Ok::<Box<dyn ActorBackend>, String>(Box::new(ForwardedCommandBackend)),
                ))
                .unwrap();
                let root_idle = root_actor.prepare_idle().unwrap();
                let ticket = root_actor
                    .admit_turn(
                        AgentInput::new(REWIND_QUEUED.into(), vec![]),
                        None,
                        String::new(),
                    )
                    .unwrap();
                let app = &mut event_loop.sessions[0].app;
                app.input_box.set_input(REWIND_DRAFT.into());
                app.persist_input();
                event_loop.sync_orchestration();
                select_child_chat(event_loop, child.id(), CHILD_NAME);
                let escape = KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE);
                event_loop.handle_input(Event::Key(escape));
                event_loop.handle_input(Event::Key(escape));
                let app = &mut event_loop.sessions[0].app;
                let [entry] = app.rewind_picker.items().unwrap() else {
                    panic!("expected only the child prompt");
                };
                assert_eq!(entry.label, CHILD_PROMPT);
                assert_eq!(app.rewind_guard.as_ref().unwrap().agent_id(), child.id());
                let child_actor = child.actor().unwrap();
                let child_ticket = child_actor
                    .admit_turn(
                        AgentInput::new(CHILD_QUEUED.into(), vec![]),
                        None,
                        String::new(),
                    )
                    .unwrap();
                if navigate {
                    app.active_chat = 0;
                }
                let completion = select_rewind(event_loop);
                assert!(
                    matches!(&completion, InternalEvent::RewindCommitted { agent, result: Ok(_), .. } if *agent == child.id())
                );
                event_loop.handle_internal(completion);

                let rt = &mut event_loop.sessions[0];
                let app = &mut rt.app;
                assert_eq!(app.main_chat().last_message_text(), REWIND_TARGET);
                assert_eq!(app.state.projection.draft(), Some(CHILD_PROMPT));
                assert!(ticket.peek_result().is_none());
                assert!(child_ticket.peek_result().is_some());
                assert!(child_actor.pending_queue().is_empty());
                assert_eq!(app.input_box.buffer.value(), CHILD_PROMPT);
                assert!(
                    app.chats[app.agent_chat_index[&child.id()]]
                        .last_message_text()
                        .contains(REWIND_NOTE)
                );
                app.input_box.set_input(EDITED_PROMPT.into());
                app.persist_input();
                app.active_chat = 0;
                assert_eq!(app.input_box.buffer.value(), EDITED_PROMPT);
                smol::block_on(rt.checkpoint.drain()).unwrap();
                let reopened = AppSession::load_from(rt.id(), &event_loop.sessions_dir).unwrap();
                assert_eq!(reopened.meta.input_draft.as_deref(), Some(EDITED_PROMPT));
                drop(root_idle);
            },
        );
    }

    #[test_case(ForkFailure::Write; "write")]
    #[test_case(ForkFailure::Sync; "fsync")]
    fn rewind_persistence_failure_preserves_chat_draft_and_queue(failure: ForkFailure) {
        with_event_loop_runtime(
            |harness| rewind_runtime(harness, Some(failure)),
            |event_loop| {
                event_loop.dispatch(0, vec![Action::BeginRewind]);
                let rt = &mut event_loop.sessions[0];
                assert!(rt.app.has_modal_overlay());
                let root = rt.app.root_agent.as_ref().unwrap().clone();
                let journal = root.manager().journal();
                let actor = root.actor().unwrap();
                let ticket = actor
                    .admit_turn(
                        AgentInput::new(REWIND_QUEUED.into(), vec![]),
                        None,
                        String::new(),
                    )
                    .unwrap();
                rt.app.input_box.set_input(REWIND_DRAFT.into());
                rt.app.persist_input();
                rt.app.refresh_session_projection();
                rt.app.sync_focused_queue();
                rt.app.queue.set_focus();
                let events = journal.context_events();
                let saved = journal.snapshot().histories[&root.id()].clone();
                let history = rt.handles.history.load_full();
                assert_eq!(rt.app.main_chat().last_message_text(), REWIND_TARGET);
                assert_eq!(rt.app.queue.focus(), Some(0));
                let completion = select_rewind(event_loop);
                event_loop.handle_internal(completion);
                let rt = &mut event_loop.sessions[0];
                assert!(!rt.app.rewind_pending);
                assert!(
                    rt.app
                        .flash_text()
                        .is_some_and(|text| text.contains(REWIND_IO_ERROR))
                );
                assert_eq!(rt.app.main_chat().last_message_text(), REWIND_TARGET);
                assert_eq!(rt.app.input_box.buffer.value(), REWIND_DRAFT);
                assert_eq!(rt.app.state.projection.draft(), Some(REWIND_DRAFT));
                assert_eq!(rt.app.queue.focus(), Some(0));
                assert_eq!(rt.app.queue.text_messages(), [REWIND_QUEUED]);
                assert_eq!(actor.snapshot().queued, 1);
                assert!(ticket.peek_result().is_none());
                assert!(Arc::ptr_eq(&history, &rt.handles.history.load_full()));
                let snapshot = journal.snapshot();
                let current = &snapshot.histories[&root.id()];
                assert_eq!(current.epoch, saved.epoch);
                assert_eq!(current.ids, saved.ids);
                assert_eq!(current.messages, saved.messages);
                assert_eq!(current.conversation, saved.conversation);
                assert_eq!(journal.context_events(), events);
                assert!(!rt.app.has_modal_overlay());
                smol::block_on(rt.checkpoint.close()).unwrap();
                let reopened = AppSession::load_from(rt.id(), &event_loop.sessions_dir).unwrap();
                assert_eq!(reopened.history_snapshot().messages, history.messages);
                assert_eq!(reopened.history_snapshot().ids, history.ids);
                assert_eq!(reopened.meta.input_draft.as_deref(), Some(REWIND_DRAFT));
                assert!(
                    !reopened
                        .context_events()
                        .iter()
                        .any(|event| event["t"] == "fork")
                );
            },
        );
    }

    #[test_case(None; "durable")]
    #[test_case(Some(ForkFailure::Rollback); "sync_and_rollback_fail")]
    fn applied_rewind_matches_reopened_session(failure: Option<ForkFailure>) {
        with_event_loop_runtime(
            |harness| rewind_runtime(harness, failure),
            |event_loop| {
                event_loop.dispatch(0, vec![Action::BeginRewind]);
                let rt = &mut event_loop.sessions[0];
                let root = rt.app.root_agent.as_ref().unwrap().clone();
                let actor = root.actor().unwrap();
                let ticket = actor
                    .admit_turn(
                        AgentInput::new(REWIND_QUEUED.into(), vec![]),
                        None,
                        String::new(),
                    )
                    .unwrap();
                rt.app.input_box.set_input(REWIND_DRAFT.into());
                rt.app.persist_input();
                let completion = select_rewind(event_loop);
                assert!(matches!(
                    &completion,
                    InternalEvent::RewindCommitted { result: Ok(RewindOutcome::Fork { warning, .. }), .. }
                        if warning.is_some() == failure.is_some()
                ));
                assert_eq!(actor.snapshot().queued, 0);
                assert!(ticket.peek_result().is_some());
                event_loop.handle_internal(completion);
                let app = &mut event_loop.sessions[0].app;
                assert_eq!(app.input_box.buffer.value(), REWIND_TARGET);
                assert!(app.main_chat().last_message_text().contains(REWIND_NOTE));
                assert!(app.queue.is_empty());
                if failure.is_some() {
                    assert!(
                        app.flash_text()
                            .is_some_and(|text| text.starts_with(REWIND_UNCERTAIN)
                                && text.contains(REWIND_IO_ERROR))
                    );
                }
                let rt = &event_loop.sessions[0];
                let reopened = AppSession::load_from(rt.id(), &event_loop.sessions_dir).unwrap();
                assert_eq!(reopened.meta.input_draft.as_deref(), Some(REWIND_TARGET));
                let reopened_view = reopened.latest_state().unwrap();
                assert!(
                    !reopened_view.scopes[&Some(root.id())]
                        .admitted
                        .contains_key(&ticket.turn_id())
                );
                let history = rt.handles.history.load_full();
                assert_eq!(reopened.history_snapshot().messages, history.messages);
                assert_eq!(reopened.history_snapshot().ids, history.ids);
                let view = root.manager().journal().snapshot().fold().unwrap();
                assert!(
                    view.scopes[&Some(root.id())]
                        .results
                        .iter()
                        .any(|(turn, _)| *turn == ticket.turn_id())
                );
            },
        );
    }

    #[test_case(false; "session_picker")]
    #[test_case(true; "back_to_planning")]
    fn in_tui_switch_rejects_a_session_from_another_workspace(planning: bool) {
        with_event_loop(|event_loop| {
            let current = event_loop.sessions[0].id();
            let other = tempfile::tempdir().unwrap();
            let model = event_loop.ctx.model_slot.load().model.spec();
            let mut session = AppSession::new(&model, &other.path().to_string_lossy());
            session.save(&event_loop.ctx.storage).unwrap();
            let expected = session_lock::ResumeBlock::OtherCwd(session.cwd.clone()).to_string();

            if planning {
                event_loop.back_to_planning(0, session.id);
                assert_eq!(
                    event_loop.sessions[0].app.flash_text(),
                    Some(expected.as_str())
                );
            } else {
                let (reply, result) = flume::bounded(1);
                event_loop.focus_session(session.id, reply);
                assert_eq!(result.try_recv().unwrap().unwrap_err(), expected);
            }
            assert_eq!(event_loop.sessions.len(), 1);
            assert_eq!(event_loop.sessions[event_loop.focused].id(), current);
        });
    }

    #[test]
    fn planning_switch_suspends_implementation_without_terminal_results() {
        with_event_loop(|event_loop| {
            const DRAFT: &str = "planning stays ready";
            let implementation_id = event_loop.sessions[0].id();
            let (manager, root) = event_loop.sessions[0].handles.manager_and_root();
            let actor = manager.actor(root).unwrap();
            let idle = actor.prepare_idle().unwrap();
            actor
                .admit_turn(
                    AgentInput::new("queued implementation work".into(), vec![]),
                    None,
                    String::new(),
                )
                .unwrap();
            let before = manager.journal().snapshot().fold().unwrap();
            assert!(!before.scopes[&Some(root)].admitted.is_empty());
            let mut parent = test_runtime(model_named("planning-model"));
            parent.app.input_box.set_input(DRAFT.into());
            parent.app.main_chat().push_user_message(DRAFT);
            let parent_id = parent.id();
            event_loop.push_runtime(parent);
            event_loop.back_to_planning(0, parent_id);
            assert!(event_loop.position(implementation_id).is_none());
            assert_eq!(event_loop.sessions[event_loop.focused].id(), parent_id);
            assert_eq!(
                event_loop.sessions[event_loop.focused]
                    .app
                    .input_box
                    .buffer
                    .value(),
                DRAFT
            );
            assert_eq!(
                event_loop.sessions[event_loop.focused].app.chats[0].last_message_text(),
                DRAFT
            );
            smol::block_on(async {
                let actor_done = async {
                    while !manager.runner_finished(root).unwrap_or(true) {
                        smol::future::yield_now().await;
                    }
                };
                futures_lite::future::or(actor_done, async {
                    smol::Timer::after(RUNTIME_SHUTDOWN_TIMEOUT).await;
                    panic!("planning switch did not suspend implementation");
                })
                .await;
            });
            let after = manager.journal().snapshot().fold().unwrap();
            assert_eq!(
                after.scopes[&Some(root)].admitted,
                before.scopes[&Some(root)].admitted
            );
            assert_eq!(
                after.scopes[&Some(root)].results,
                before.scopes[&Some(root)].results
            );
            drop(idle);
        });
    }

    #[test]
    fn reopen_app_restores_history_after_disk_restart() {
        const HISTORY: &str = "history flushed before reopening";
        const DRAFT: &str = "draft restored into the new app";

        let harness = RuntimeHarness::new();
        let mut session = harness.session();
        let id = session.id;
        assert!(AppSession::load(id, &harness.ctx().storage).is_err());
        session.push_message(Message::user(HISTORY.into()));
        let mut runtime = harness.runtime(session);
        let runtime_generation = runtime.generation;
        let manager_generation = runtime.handles.manager_and_root().0.generation();
        let root_id = runtime.handles.manager_and_root().1;
        assert_eq!(runtime.app.state.projection.root_agent_id(), Some(root_id));
        runtime.app.input_box.set_input(DRAFT.into());

        let restarted = harness.restart(runtime);
        reopen_app(restarted, id, |event_loop| {
            let runtime = &event_loop.sessions[0];
            assert_eq!(runtime.id(), id);
            assert_ne!(runtime.generation, runtime_generation);
            assert_ne!(
                runtime.handles.manager_and_root().0.generation(),
                manager_generation
            );
            assert_eq!(
                runtime
                    .app
                    .state
                    .projection
                    .root_history()
                    .unwrap()
                    .messages
                    .len(),
                1
            );
            assert_eq!(
                runtime
                    .app
                    .state
                    .projection
                    .root_history()
                    .unwrap()
                    .messages[0]
                    .user_text(),
                Some(HISTORY)
            );
            let history = runtime.handles.history.load();
            assert_eq!(history.messages.len(), 1);
            assert_eq!(history.messages[0].user_text(), Some(HISTORY));
            assert_eq!(runtime.handles.manager_and_root().1, root_id);
            assert_eq!(runtime.app.state.projection.root_agent_id(), Some(root_id));
            assert_eq!(runtime.app.input_box.buffer.value(), DRAFT);
        });
    }

    #[test]
    fn journal_spawn_preserves_child_tab_label_after_save_and_reload() {
        const CHILD_LABEL: &str = "journal child";
        const CHILD_KEY: &str = "journal-child-transcript";

        with_event_loop(|event_loop| {
            let runtime = &event_loop.sessions[0];
            let (manager, root_id) = runtime.handles.manager_and_root();
            let root = manager.lookup(root_id).unwrap();
            let config = (*root.actor().unwrap().effective_config().unwrap()).clone();
            let child = smol::block_on(manager.spawn_child_trusted_with_config(
                &root,
                config,
                AgentMetadata {
                    label: Some(CHILD_LABEL.into()),
                    transcript_key: Some(CHILD_KEY.into()),
                    ..Default::default()
                },
                Vec::new(),
                None,
                |_| Ok::<Box<dyn ActorBackend>, String>(Box::new(ForwardedCommandBackend)),
            ))
            .unwrap();
            let child_id = child.id();
            event_loop.sync_orchestration();
            assert!(
                !event_loop.sessions[0]
                    .app
                    .agent_chat_index
                    .contains_key(&child_id)
            );
            select_child_chat(event_loop, child_id, CHILD_LABEL);
            let runtime = event_loop.sessions.pop().unwrap();
            assert_eq!(runtime.app.chats[runtime.app.active_chat].name, CHILD_LABEL);

            let id = runtime.id();
            let journal = manager.journal();
            smol::block_on(journal.flush()).unwrap();
            let session = journal.snapshot().fold().unwrap();
            let journal_child = journal.agent(child_id).unwrap();
            assert_eq!(
                session
                    .agents
                    .values()
                    .filter(|record| record.parent.is_some())
                    .collect::<Vec<_>>(),
                [&journal_child]
            );
            release_runtime(runtime);
            let storage = &event_loop.ctx.storage;
            let loaded = AppSession::load(id, storage).unwrap();
            assert_eq!(
                loaded.agents()[&child_id].label.as_deref(),
                Some(CHILD_LABEL)
            );

            let mut runtime = event_loop.ctx.spawn_runtime(loaded).unwrap();
            runtime.activate_deferred();
            let app = &runtime.app;
            assert_eq!(app.chats.len(), 2);
            let tab = &app.chats[app.agent_chat_index[&child_id]];
            assert_eq!(tab.name, CHILD_LABEL);
            assert_eq!(tab.subagent_id.as_deref(), Some(CHILD_KEY));
            event_loop.sessions.push(runtime);
        });
    }

    #[test]
    fn snapshot_crash_reopens_only_durable_history_and_files() {
        const PERSISTED: &str = "published history";
        const UNSAVED: &str = "unpublished history";
        const UNSAVED_DRAFT: &str = "draft never checkpointed";
        const ARTIFACT: &str = "artifact copied before teardown";

        let harness = RuntimeHarness::new();
        let mut session = harness.session();
        let id = session.id;
        session.push_message(Message::user(PERSISTED.into()));
        let mut runtime = harness.runtime(session);
        harness.persist_runtime(&mut runtime);
        let durable = AppSession::load(id, &harness.ctx().storage).unwrap();
        assert_eq!(durable.messages().len(), 1);
        assert_eq!(durable.messages()[0].user_text(), Some(PERSISTED));
        let manager_generation = runtime.handles.manager_and_root().0.generation();
        let runtime_generation = runtime.generation;
        let source_root = harness.ctx().storage.path().to_path_buf();
        let store = runtime.handles.offload.as_ref().unwrap();
        let artifact = store.put(ARTIFACT).unwrap();
        let artifact_path = store.path_of(&artifact);
        let artifact_relative = artifact_path
            .strip_prefix(&source_root)
            .unwrap()
            .to_path_buf();

        let mut display_history = runtime.handles.history.load().messages.as_ref().clone();
        display_history.push(Message::user(UNSAVED.into()));
        runtime
            .handles
            .history
            .store(Arc::new(HistorySnapshot::new(display_history)));
        runtime.app.input_box.set_input(UNSAVED_DRAFT.into());
        assert_eq!(runtime.handles.history.load().messages.len(), 2);
        assert_eq!(
            runtime
                .app
                .state
                .projection
                .root_history()
                .unwrap()
                .messages
                .len(),
            1
        );

        let mut restarted = harness.snapshot_crash(runtime);
        assert_ne!(restarted._temp_dir.path(), source_root);
        assert!(!source_root.exists());
        assert_eq!(
            fs::read_to_string(restarted._temp_dir.path().join(artifact_relative)).unwrap(),
            ARTIFACT
        );
        let durable = AppSession::load(id, &restarted.ctx().storage).unwrap();
        assert_eq!(durable.messages().len(), 1);
        assert_eq!(durable.messages()[0].user_text(), Some(PERSISTED));
        assert!(durable.meta.input_draft.is_none());
        restarted.ctx.as_mut().unwrap().workspace_cwd = durable.cwd.clone();
        reopen_app(restarted, id, |event_loop| {
            let runtime = &event_loop.sessions[0];
            assert_eq!(runtime.id(), id);
            assert_ne!(runtime.generation, runtime_generation);
            assert_ne!(
                runtime.handles.manager_and_root().0.generation(),
                manager_generation
            );
            assert_eq!(
                runtime
                    .app
                    .state
                    .projection
                    .root_history()
                    .unwrap()
                    .messages
                    .len(),
                1
            );
            assert_eq!(
                runtime
                    .app
                    .state
                    .projection
                    .root_history()
                    .unwrap()
                    .messages[0]
                    .user_text(),
                Some(PERSISTED)
            );
            let history = runtime.handles.history.load();
            assert_eq!(history.messages.len(), 1);
            assert_eq!(history.messages[0].user_text(), Some(PERSISTED));
            assert!(runtime.app.input_box.buffer.value().is_empty());
        });
    }

    #[test]
    fn offload_uses_supplied_storage_root_and_session_deletion() {
        const BODY: &str = "saved in the supplied state directory";
        let mut harness = RuntimeHarness::new();
        let mut session = harness.session();
        session.push_message(Message::user(BODY.into()));
        let id = session.id;
        let runtime = harness.runtime(session);
        smol::block_on(runtime.handles.manager_and_root().0.journal().flush()).unwrap();
        let store = Arc::clone(runtime.handles.offload.as_ref().unwrap());
        let expected = makima_storage::sessions::offload_dir(&harness.ctx().sessions_dir, id);
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

    #[test_case(false; "offline")]
    #[test_case(true; "live")]
    fn session_set_title_persists_without_checkpoint(live: bool) {
        const RAW_TITLE: &str = "  renamed\n session  ";
        const TITLE: &str = "renamed session";
        with_event_loop(|event_loop| {
            let storage = &event_loop.ctx.storage;
            let id = if live {
                event_loop.sessions[event_loop.focused].id()
            } else {
                let mut session = AppSession::new(
                    &event_loop.ctx.model_slot.load().model.spec(),
                    &event_loop.ctx.workspace_cwd,
                );
                session.save(storage).unwrap();
                session.id
            };
            let (reply_tx, reply_rx) = flume::bounded(1);
            event_loop.handle_session_request(
                SessionRequest::SetTitle {
                    id: id.to_string(),
                    title: RAW_TITLE.into(),
                },
                reply_tx,
            );
            assert_eq!(
                reply_rx.recv_timeout(PUMP_TIMEOUT).unwrap(),
                Ok(json!(true))
            );
            if live {
                let runtime = &event_loop.sessions[event_loop.focused];
                assert_eq!(runtime.app.title(), TITLE);
                smol::block_on(runtime.handles.manager_and_root().0.journal().flush()).unwrap();
            }
            assert_eq!(
                AppSession::load(id, &event_loop.ctx.storage).unwrap().title,
                TITLE
            );
        });
    }

    #[test_case(false; "deletion_completes")]
    #[test_case(true; "ownership_loss_preserves_transcript_and_artifacts")]
    fn background_deletion_drains_in_flight_offload_persistence(lose_ownership: bool) {
        const BODY: &str = "pending background session output";
        const EXISTING_BODY: &str = "referenced output retained if deletion is rejected";
        const ARTIFACT_NAME: &str = "existing-output.txt";
        const REPLACEMENT_PID: &str = "4294967295";
        with_event_loop(|event_loop| {
            let mut session = AppSession::new(
                &event_loop.ctx.model_slot.load().model.spec(),
                &event_loop.ctx.workspace_cwd,
            );
            let id = session.id;
            let dir = makima_storage::sessions::offload_dir(&event_loop.ctx.sessions_dir, id);
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
                .prepare_runtime_with_options(
                    session,
                    None,
                    PrepareOptions {
                        offload: Some(Arc::clone(&store)),
                        ..Default::default()
                    },
                    None,
                )
                .unwrap();
            let runtime = prepared.activate(&event_loop.ctx.model_slot, None).unwrap();
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
            if lose_ownership {
                let path = session_lock::lock_path(&event_loop.ctx.sessions_dir, &id);
                std::fs::remove_file(&path).unwrap();
                let replacement = path.with_extension("replacement");
                std::fs::write(&replacement, REPLACEMENT_PID).unwrap();
                std::fs::rename(replacement, path).unwrap();
                assert!(session_lock::open_elsewhere(
                    &event_loop.ctx.sessions_dir,
                    &id
                ));
            }
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
        });
    }

    #[test_case(false; "last_session")]
    #[test_case(true; "next_session")]
    fn deleting_the_focused_session_leaves_a_live_session(next: bool) {
        with_event_loop(|event_loop| {
            let original = event_loop.sessions[0].id();
            if next {
                let mut session = AppSession::new(
                    &event_loop.ctx.model_slot.load().model.spec(),
                    &event_loop.ctx.workspace_cwd,
                );
                session.save(&event_loop.ctx.storage).unwrap();
                let runtime = event_loop.ctx.spawn_runtime(session).unwrap();
                let idx = event_loop.push_runtime(runtime);
                event_loop.set_focused(idx);
            }
            let id = event_loop.sessions[event_loop.focused].id();
            assert!(AppSession::load(id, &event_loop.ctx.storage).is_ok());

            let (reply_tx, reply_rx) = flume::bounded(1);
            event_loop
                .handle_session_request(SessionRequest::Delete { id: id.to_string() }, reply_tx);
            assert_eq!(
                reply_rx.recv_timeout(PUMP_TIMEOUT).unwrap(),
                Ok(json!(true))
            );

            assert_eq!(event_loop.sessions.len(), 1);
            let live_id = event_loop.sessions[event_loop.focused].id();
            assert_ne!(live_id, id);
            if next {
                assert_eq!(live_id, original);
            }

            let (live_tx, live_rx) = flume::bounded(1);
            event_loop.handle_session_request(SessionRequest::Live, live_tx);
            let rows = live_rx.recv_timeout(PUMP_TIMEOUT).unwrap().unwrap();
            let rows = rows.as_array().unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0]["id"], json!(live_id.to_string()));
            assert_eq!(rows[0]["focused"], json!(true));

            assert!(matches!(
                AppSession::load(id, &event_loop.ctx.storage),
                Err(SessionError::Storage(StorageError::NotFound(_)))
            ));
            let lease = session_lock::claim(&event_loop.ctx.sessions_dir, &id)
                .unwrap()
                .expect("deleted session lock released");
            lease.release().unwrap();
        });
    }

    #[test]
    fn background_deletion_lease_failure_cancels_and_drains_active_turn() {
        use makima_agent::actor::ActorLifecycle;

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
                _event_tx: &'a flume::Sender<makima_providers::ProviderEvent>,
                _opts: makima_providers::RequestOptions,
                _session_id: Option<&'a SessionRef>,
                _invocation: &'a Invocation,
                _delivery: RequestDelivery,
            ) -> makima_providers::provider::BoxFuture<
                'a,
                Result<makima_providers::StreamResponse, makima_providers::AgentError>,
            > {
                Box::pin(async move {
                    let _turn_drop = TurnDrop(Arc::clone(&self.dropped));
                    self.entered.send(()).unwrap();
                    self.release.recv_async().await.unwrap();
                    std::future::pending().await
                })
            }
        }

        with_event_loop(|event_loop| {
            let mut session = AppSession::new(
                &event_loop.ctx.model_slot.load().model.spec(),
                &event_loop.ctx.workspace_cwd,
            );
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
            let node = manager.node(root).unwrap();
            assert_eq!(
                node.residency,
                makima_agent::manager::AgentResidency::Dormant
            );
            let retained = node.actor.as_ref().expect("retained shutdown snapshot");
            assert_eq!(retained.lifecycle, ActorLifecycle::Shutdown);
            assert!(retained.active_turn.is_none());
            assert_eq!(retained.queued, 0);
            assert!(manager.lookup(root).unwrap().validate_live().is_err());
            assert!(manager.actor(root).is_err());
            assert!(manager.runner_finished(root).unwrap());
            assert!(matches!(
                actor.submit(
                    makima_agent::AgentInput::new(PROMPT.into(), Vec::new()),
                    None,
                    PROMPT.into(),
                    Default::default(),
                ),
                Err(makima_agent::ActorError::Shutdown)
            ));
            for node in manager.snapshot() {
                assert_eq!(
                    node.residency,
                    makima_agent::manager::AgentResidency::Dormant
                );
                assert!(manager.runner_finished(node.agent_id).unwrap());
                assert!(
                    manager
                        .lookup(node.agent_id)
                        .unwrap()
                        .validate_live()
                        .is_err()
                );
                if let Some(actor) = node.actor {
                    assert_eq!(actor.lifecycle, ActorLifecycle::Shutdown);
                    assert!(actor.active_turn.is_none());
                    assert_eq!(actor.queued, 0);
                }
            }
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
            .prepare_runtime_with_options(
                session.clone(),
                None,
                PrepareOptions {
                    offload: Some(Arc::clone(&store)),
                    ..Default::default()
                },
                None,
            )
            .unwrap();
        let mut runtime = prepared.activate(&harness.ctx().model_slot, None).unwrap();
        runtime.activate_deferred();
        session.model = runtime.app.root_config().model.spec();
        let outgoing = std::thread::spawn({
            let store = Arc::clone(runtime.handles.offload.as_ref().unwrap());
            move || store.put(OUTGOING)
        });
        entered_rx.recv_timeout(RUNTIME_SHUTDOWN_TIMEOUT).unwrap();
        let replacement = harness
            .ctx()
            .prepare_replacement_runtime(session, None, &runtime)
            .unwrap();
        let old =
            replace_session_runtime(&mut runtime, replacement, &harness.ctx().model_slot).unwrap();
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

    #[test_case(false; "open_runtime")]
    #[test_case(true; "replacement_runtime")]
    fn failed_runtime_claim_cannot_rewrite_session_or_approved_plan(replace: bool) {
        use makima_storage::plans::{
            ApprovedPlan, PLAN_STATE_KEY, PLAN_STATE_VERSION, PlanEntry, PlanSlot,
        };
        const OWNER_CONTENT: &str = "written by lock owner";
        const LOSER_CONTENT: &str = "stale losing snapshot";
        const PLAN: &str = "active-approved-plan.md";
        let mut harness = RuntimeHarness::new();
        let prepared = harness.prepare();
        let mut owner = prepared.prepared.stored().unwrap().clone();
        drop(prepared);
        let id = owner.id;
        let root = owner.root_agent_id().unwrap();
        let plan = harness._temp_dir.path().join(PLAN);
        fs::write(&plan, OWNER_CONTENT).unwrap();
        owner.push_message(Message::user(OWNER_CONTENT.into()));
        owner.save(&harness.ctx().storage).unwrap();
        let log = harness.ctx().sessions_dir.join(format!("{id}.jsonl"));
        let log_before = fs::read(&log).unwrap();
        let lock = session_lock::claim(&harness.ctx().sessions_dir, &id)
            .unwrap()
            .unwrap();
        let mut loser = owner.clone();
        loser.replace_messages(vec![Message::user(LOSER_CONTENT.into())]);
        let view = loser.latest_state().unwrap();
        let mut config = view.scopes[&Some(root)].config.clone().unwrap();
        config.mode = makima_agent::ModeDef::default_for(makima_agent::ModeId::Plan)
            .stored_snapshot(&makima_agent::AgentMode::Plan(plan.clone()));
        let seq = loser
            .state()
            .iter()
            .map(|record| record.seq)
            .chain(loser.agent_revisions().iter().map(|(seq, _)| *seq))
            .max()
            .unwrap()
            + 1;
        loser.push_state(makima_storage::session_state::StoredStateRecord {
            seq,
            agent: Some(root),
            generation: 0,
            change: makima_storage::session_state::StateChange::Config(config),
        });
        let mut plans = PlanSlot::new();
        plans.insert(
            root,
            PlanEntry {
                path: plan.clone(),
                written: true,
                approved: Some(ApprovedPlan {
                    content: LOSER_CONTENT.into(),
                    sha256: makima_agent::plan_state::content_sha256(LOSER_CONTENT),
                }),
                staged_model: None,
            },
        );
        loser.push_state(makima_storage::session_state::StoredStateRecord {
            seq: seq + 1,
            agent: None,
            generation: 0,
            change: makima_storage::session_state::StateChange::Plugin {
                key: PLAN_STATE_KEY.into(),
                version: PLAN_STATE_VERSION,
                value: Some(serde_json::to_value(plans).unwrap()),
            },
        });
        if replace {
            let current = harness.runtime(harness.session());
            assert!(
                harness
                    .ctx()
                    .prepare_replacement_runtime(loser, None, &current)
                    .is_err()
            );
            release_runtime(current);
        } else {
            assert!(harness.ctx().spawn_runtime(loser).is_err());
        }
        assert_eq!(fs::read_to_string(&plan).unwrap(), OWNER_CONTENT);
        assert_eq!(fs::read(&log).unwrap(), log_before);
        let storage = harness.shutdown_writer();
        lock.release().unwrap();
        assert_eq!(
            AppSession::load(id, &storage).unwrap().messages()[0].user_text(),
            Some(OWNER_CONTENT)
        );
    }

    #[test]
    fn login_refresh_reprepares_the_originating_session() {
        const FOCUSED: &str = "anthropic/claude-sonnet-4-6";
        const BACKGROUND: &str = "anthropic/claude-opus-4-6";
        const PROVIDER: &str = "anthropic";

        with_event_loop(|event_loop| {
            for spec in [FOCUSED, BACKGROUND] {
                let runtime = event_loop
                    .ctx
                    .spawn_runtime_with_provider(
                        AppSession::new(spec, &event_loop.ctx.workspace_cwd),
                        Some(PreparedProvider {
                            model: Model::from_spec(spec).unwrap(),
                            provider: Arc::new(StubProvider),
                        }),
                    )
                    .unwrap();
                event_loop.push_runtime(runtime);
            }
            let (prepared_tx, prepared_rx) = flume::unbounded();
            event_loop.ctx.prepare_provider = sync_preparer(move |requested| {
                prepared_tx.send(requested.spec()).unwrap();
                Ok(PreparedModel {
                    model: requested.clone(),
                    provider: Arc::new(StubProvider),
                })
            });
            event_loop.set_focused(1);
            event_loop.dispatch(
                2,
                vec![Action::RefreshProvider {
                    slug: PROVIDER.into(),
                }],
            );
            assert_eq!(
                prepared_rx.recv_timeout(RUNTIME_SHUTDOWN_TIMEOUT).unwrap(),
                BACKGROUND
            );
        });
    }

    #[test]
    fn new_session_inherits_focused_model_and_provider() {
        const FOCUSED_MODEL: &str = "openai/gpt-5";

        let harness = RuntimeHarness::new();
        let focused_model = Model::from_spec(FOCUSED_MODEL).unwrap();
        let (focused_slot, _) = ProviderSlot::new(focused_model, Arc::new(FocusedProvider));

        let (session, provider) = new_session_from_slot(&focused_slot, "/tmp");
        assert_eq!(provider.provider.key_count(), FOCUSED_KEY_COUNT);
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
        use makima_agent::actor::PreparedModel;
        use makima_providers::{RequestOptions, Role, StopReason, StreamResponse};

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
                _event_tx: &'a flume::Sender<makima_providers::ProviderEvent>,
                opts: RequestOptions,
                _session_id: Option<&'a SessionRef>,
                _invocation: &'a Invocation,
                _delivery: RequestDelivery,
            ) -> makima_providers::provider::BoxFuture<
                'a,
                Result<StreamResponse, makima_providers::AgentError>,
            > {
                Box::pin(async move {
                    self.requests.send((self.name, model.spec(), opts)).unwrap();
                    self.release.recv_async().await.unwrap();
                    Ok(StreamResponse {
                        message: Message {
                            role: Role::Assistant,
                            content: vec![makima_providers::ContentBlock::Text {
                                text: RESPONSE.into(),
                            }],
                            ..Message::user(String::new())
                        },
                        usage: TokenUsage::default(),
                        stop_reason: Some(StopReason::EndTurn),
                    })
                })
            }
        }

        fn model(spec: &str) -> Model {
            let mut model = Model::from_spec(spec).unwrap();
            model.fast_support = Some(FastSupport::Supported);
            model.thinking_support = Some(makima_providers::ThinkingSupport::Yes);
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
                let mut session = AppSession::new(spec, &event_loop.ctx.workspace_cwd);
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
                    thinking: DomainThinkingConfig::Effort(makima_providers::Effort::High),
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
            event_loop.ctx.prepare_provider = sync_preparer(move |requested| {
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
                makima_commands::InputDispatch::Dispatched(
                    makima_commands::CommandOutcome::Completed
                )
            ));
            changed_release.send(()).unwrap();
            send(event_loop, 2);
            assert_eq!(
                requests_rx.recv_timeout(RUNTIME_SHUTDOWN_TIMEOUT).unwrap(),
                ("changed", FIRST.into(), changed(true))
            );

            let mut stored = AppSession::new(THIRD, &event_loop.ctx.workspace_cwd);
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
            pump_until(event_loop, |_| !reply_rx.is_empty());
            assert_eq!(reply_rx.try_recv().unwrap().unwrap(), json!(true));
            let _ = event_loop.tick();
            let restored_index = event_loop.position(stored.id).unwrap();
            assert_eq!(event_loop.focused, restored_index);
            assert_eq!(event_loop.sessions.len(), 4);
            assert_eq!(event_loop.sessions[2].app.state.projection.model(), FIRST);
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

    #[test_case(false; "submit_releases_held_input")]
    #[test_case(true; "escape_discards_held_input")]
    fn failed_turn_keeps_actor_queue_visible(cancel: bool) {
        const FIRST: &str = "failing request";
        const HELD: &str = "held request";
        const NEXT: &str = "new request";
        const FAILURE: &str = "fixture request failed";

        struct FailingProvider {
            entered: flume::Sender<()>,
            fail: flume::Receiver<()>,
        }

        impl Provider for FailingProvider {
            fn stream_message<'a>(
                &'a self,
                _model: &'a Model,
                _messages: &'a [Message],
                _system: &'a str,
                _tools: &'a serde_json::Value,
                _event_tx: &'a flume::Sender<makima_providers::ProviderEvent>,
                _opts: RequestOptions,
                _session_id: Option<&'a SessionRef>,
                _invocation: &'a Invocation,
                _delivery: RequestDelivery,
            ) -> BoxFuture<'a, Result<StreamResponse, AgentError>> {
                Box::pin(async move {
                    self.entered.send(()).unwrap();
                    self.fail.recv_async().await.unwrap();
                    Err(AgentError::Config {
                        message: FAILURE.into(),
                    })
                })
            }
        }

        let (entered, requests) = flume::unbounded();
        let (fail, gate) = flume::bounded(1);
        with_event_loop_runtime(
            |harness| {
                harness
                    .ctx()
                    .spawn_runtime_with_provider(
                        harness.session(),
                        Some(PreparedProvider {
                            model: crate::components::test_model(),
                            provider: Arc::new(FailingProvider {
                                entered,
                                fail: gate,
                            }),
                        }),
                    )
                    .unwrap()
            },
            |event_loop| {
                let submit = |event_loop: &mut EventLoop<'_>, text: &str| {
                    event_loop.handle_input(Event::Paste(text.into()));
                    event_loop.handle_input(Event::Key(crate::components::key(KeyCode::Enter)));
                };
                let (manager, root) = event_loop.sessions[0].handles.manager_and_root();
                let actor = manager.actor(root).unwrap();
                submit(event_loop, FIRST);
                requests.recv_timeout(SESSION_OP_TIMEOUT).unwrap();
                actor
                    .admit_turn_with_options(
                        AgentInput::new(HELD.into(), vec![]),
                        None,
                        String::new(),
                        TurnAdmissionOptions {
                            provenance: TurnProvenance {
                                origin: TurnOrigin::User,
                                ..Default::default()
                            },
                            ..Default::default()
                        },
                    )
                    .unwrap();
                fail.send(()).unwrap();
                pump_until(event_loop, |event_loop| {
                    actor.input_held()
                        && matches!(&event_loop.sessions[0].app.status, Status::Error { message, .. } if message == FAILURE)
                });
                let runtime = &event_loop.sessions[0];
                assert_eq!(runtime.app.queue.text_messages(), [HELD]);
                assert!(!runtime.quiescent());
                assert!(requests.is_empty());

                if cancel {
                    for _ in 0..2 {
                        event_loop.handle_input(Event::Key(crate::components::key(KeyCode::Esc)));
                    }
                    let app = &event_loop.sessions[0].app;
                    assert!(!actor.input_held());
                    assert!(app.queue.is_empty());
                    assert_eq!(app.status, Status::Idle);
                    assert!(requests.is_empty());
                } else {
                    let presentation = runtime.app.run_id;
                    submit(event_loop, NEXT);
                    let app = &event_loop.sessions[0].app;
                    assert_eq!(app.run_id, presentation);
                    assert_ne!(app.chats[0].last_message_text(), NEXT);
                    requests.recv_timeout(SESSION_OP_TIMEOUT).unwrap();
                    pump_until(event_loop, |event_loop| {
                        let chat = &event_loop.sessions[0].app.chats[0];
                        (0..chat.message_count()).any(|i| {
                            chat.message_at(i)
                                .is_some_and(|message| message.text == HELD)
                        })
                    });
                    assert!(!actor.input_held());
                }
            },
        );
    }

    /// Answers every request at once, reporting the model it was sent.
    struct AnsweringProvider {
        requests: flume::Sender<String>,
        usage: TokenUsage,
    }

    impl Provider for AnsweringProvider {
        fn stream_message<'a>(
            &'a self,
            model: &'a Model,
            _messages: &'a [Message],
            _system: &'a str,
            _tools: &'a serde_json::Value,
            _event_tx: &'a flume::Sender<makima_providers::ProviderEvent>,
            _opts: makima_providers::RequestOptions,
            _session_id: Option<&'a SessionRef>,
            _invocation: &'a Invocation,
            _delivery: RequestDelivery,
        ) -> makima_providers::provider::BoxFuture<
            'a,
            Result<makima_providers::StreamResponse, makima_providers::AgentError>,
        > {
            Box::pin(async move {
                self.requests.send(model.spec()).unwrap();
                Ok(makima_providers::StreamResponse {
                    message: Message {
                        role: makima_providers::Role::Assistant,
                        content: vec![makima_providers::ContentBlock::Text {
                            text: PREPARED_ANSWER.into(),
                        }],
                        ..Message::user(String::new())
                    },
                    usage: self.usage,
                    stop_reason: Some(makima_providers::StopReason::EndTurn),
                })
            })
        }
    }

    #[test]
    fn real_turn_autocmds_reach_the_tui() {
        const PROMPT: &str = "run a turn through the TUI";
        const OBSERVER: &str = r#"
            assert(makima.api.create_autocmd("TurnEnd", { callback = function(ev)
                local data = ev.data
                makima.ui.flash(table.concat({ ev.event, data.agent_id, data.turn_id, data.status }, ":"))
            end }))
            assert(makima.api.create_autocmd("SessionStatusChanged", { callback = function(ev)
                local data = ev.data
                makima.ui.flash(table.concat({ ev.event, data.session_id, data.status }, ":"))
            end }))
        "#;

        let harness = RuntimeHarness::new();
        harness
            ._host
            .load_source("turn_observer", OBSERVER)
            .unwrap();
        let actions = harness._host.ui_action_rx();
        let pending = Arc::new(PendingProvider::default());
        let receive_flash = |event_loop: &mut EventLoop<'_>| {
            let action = actions
                .recv_timeout(PUMP_TIMEOUT)
                .expect("autocmd did not send a UI action");
            assert!(matches!(action, UiAction::Flash(_)));
            event_loop.handle_wake(Wake::Ui(action)).unwrap();
            event_loop.focused_app().flash_text().unwrap().to_owned()
        };
        run_event_loop(
            harness,
            |harness| {
                let runtime = harness
                    .ctx()
                    .spawn_runtime_with_provider(
                        harness.session(),
                        Some(PreparedProvider {
                            model: crate::components::test_model(),
                            provider: pending.clone(),
                        }),
                    )
                    .unwrap();
                harness._host.wait_for_worker_barrier_for_test();
                runtime
            },
            |event_loop| {
                event_loop
                    .ctx
                    .lua_event_handle
                    .install_orchestration_services(event_loop.orchestration.clone());
                let session = event_loop.sessions[0].id();
                let (manager, root) = event_loop.sessions[0].handles.manager_and_root();
                let actor = manager.actor(root).unwrap();

                event_loop.start(Some(PROMPT.into()));
                event_loop.emit_status_changes();
                assert_eq!(
                    receive_flash(event_loop),
                    format!("{SESSION_STATUS_CHANGED}:{session}:{STATUS_WORKING}")
                );

                let (requests, _received) = flume::unbounded();
                smol::block_on(pending.fill(Ok(PreparedModel {
                    model: event_loop.sessions[0].app.root_config().model.clone(),
                    provider: Arc::new(AnsweringProvider {
                        requests,
                        usage: TokenUsage::default(),
                    }),
                })));
                pump_until(event_loop, |event_loop| {
                    SessionStatus::of(&event_loop.sessions[0].app) == SessionStatus::Idle
                        && matches!(actor.snapshot().latest, Some(TurnOutcome::Completed { .. }))
                });
                let turn = actor.snapshot().latest.unwrap().turn_id();

                event_loop.emit_status_changes();
                let mut flashes = [receive_flash(event_loop), receive_flash(event_loop)];
                flashes.sort();
                assert_eq!(
                    flashes,
                    [
                        format!("{SESSION_STATUS_CHANGED}:{session}:{STATUS_IDLE}"),
                        format!("{TURN_END_EVENT}:{root}:{turn}:{STATUS_COMPLETED}"),
                    ]
                );
            },
        );
    }

    #[test]
    fn status_bar_global_cost_grows_after_actor_turns() {
        const CHILD_NAME: &str = "other chat";
        const CHILD_KEY: &str = "cost-test-child";
        const PROMPT: &str = "bill this request";
        const INPUT_TOKENS: u32 = 1_000;
        const TURN_COST: f64 = 0.003;
        const WIDTH: u16 = 120;
        const HEIGHT: u16 = 24;

        let (requests, requests_rx) = flume::unbounded();
        with_event_loop_runtime(
            |harness| {
                let model = crate::components::test_model();
                harness
                    .ctx()
                    .spawn_runtime_with_provider(
                        harness.session(),
                        Some(PreparedProvider {
                            model,
                            provider: Arc::new(AnsweringProvider {
                                requests,
                                usage: TokenUsage {
                                    input: INPUT_TOKENS,
                                    ..TokenUsage::default()
                                },
                            }),
                        }),
                    )
                    .unwrap()
            },
            |event_loop| {
                let (manager, root_id) = event_loop.sessions[0].handles.manager_and_root();
                let root = manager.lookup(root_id).unwrap();
                let config = (*root.actor().unwrap().effective_config().unwrap()).clone();
                let child = smol::block_on(manager.spawn_child_trusted_with_config(
                    &root,
                    config,
                    AgentMetadata {
                        transcript_key: Some(CHILD_KEY.into()),
                        ..Default::default()
                    },
                    Vec::new(),
                    None,
                    |_| Ok::<Box<dyn ActorBackend>, String>(Box::new(ForwardedCommandBackend)),
                ))
                .unwrap();
                event_loop.sync_orchestration();
                select_child_chat(event_loop, child.id(), CHILD_NAME);
                let child_chat = event_loop.sessions[0].app.active_chat;
                let mut terminal = Terminal::new(TestBackend::new(WIDTH, HEIGHT)).unwrap();
                let render = |terminal: &mut Terminal<TestBackend>, app: &mut App| {
                    terminal.draw(|frame| app.view(frame)).unwrap();
                    crate::components::buffer_text(terminal.backend().buffer())
                };
                assert!(!render(&mut terminal, &mut event_loop.sessions[0].app).contains("Σ$"));

                for turns in 1..=2 {
                    let cost = TURN_COST * f64::from(turns);
                    event_loop.sessions[0].app.active_chat = 0;
                    event_loop.submit_text(0, PROMPT.into()).unwrap();
                    pump_until(event_loop, |event_loop| {
                        let app = &event_loop.sessions[0].app;
                        matches!(app.status, Status::Idle) && app.chats[0].cost == Some(cost)
                    });
                    let snapshot = event_loop.session_snapshot_json(0);
                    assert_eq!(snapshot["usage"]["total"]["cost"], json!(cost));
                    assert_eq!(
                        requests_rx.try_recv().unwrap(),
                        crate::components::test_model().spec()
                    );
                    let app = &mut event_loop.sessions[0].app;
                    app.active_chat = child_chat;
                    assert!(render(&mut terminal, app).contains(&format!("Σ${cost:.3}")));
                    assert_eq!(app.chats[child_chat].cost, None);
                }
            },
        );
    }

    const PREPARED_MODEL: &str = "openai/gpt-5";
    const PREPARED_ANSWER: &str = "answered by the prepared provider";
    const STARTUP_PROMPT: &str = "sent before the provider was ready";

    /// A preparer that reports each start and waits for a release.
    fn gated_preparer(
        provider: Arc<dyn Provider>,
    ) -> (ModelPreparer, flume::Receiver<()>, flume::Sender<()>) {
        let (started_tx, started_rx) = flume::unbounded();
        let (release_tx, release_rx) = flume::unbounded::<()>();
        let prepare: ModelPreparer = Arc::new(move |spec| {
            let started_tx = started_tx.clone();
            let release_rx = release_rx.clone();
            let provider = Arc::clone(&provider);
            Box::pin(async move {
                started_tx.send_async(()).await.unwrap();
                release_rx.recv_async().await.unwrap();
                Ok(PreparedModel {
                    model: Model::from_spec(&spec).unwrap(),
                    provider,
                })
            })
        });
        (prepare, started_rx, release_tx)
    }

    #[test]
    fn startup_draws_and_quits_while_saved_model_is_preparing() {
        let (started_tx, started) = flume::bounded(1);
        let (release, gate) = flume::bounded::<()>(1);
        let (finished_tx, finished) = flume::bounded(1);
        let (keys, input) = flume::unbounded();
        let worker = std::thread::spawn(move || {
            let mut harness = RuntimeHarness::new();
            let mut session = harness.session();
            session.model = PREPARED_MODEL.into();
            session.push_message(Message::user(STARTUP_PROMPT.into()));
            let ctx = harness.ctx.take().unwrap();
            session.save(&ctx.storage).unwrap();
            let prepare: ModelPreparer = Arc::new(move |spec| {
                let gate = gate.clone();
                let started = started_tx.clone();
                Box::pin(async move {
                    if spec.as_ref() == PREPARED_MODEL {
                        started.send(()).unwrap();
                        let _ = gate.recv_async().await;
                    }
                    Ok(PreparedModel {
                        model: Model::from_spec(&spec).unwrap(),
                        provider: Arc::new(StubProvider),
                    })
                })
            });
            let mut terminal = test_tty();
            let event_loop = EventLoop::new_with(
                &mut terminal,
                EventLoopParams {
                    cwd: PathBuf::from(ctx.workspace_cwd),
                    model: ctx.model_slot.load().model.clone(),
                    needs_login: false,
                    explicit_model: false,
                    commands: Vec::new(),
                    sessions: StartupSessions::Open(Box::new(session)),
                    focused: 0,
                    startup_warnings: Vec::new(),
                    storage: ctx.storage,
                    config: ctx.config,
                    ui_config: ctx.ui_config,
                    input_history_size: ctx.input_history_size,
                    permissions: ctx.permissions,
                    timeouts: ctx.timeouts,
                    exit_on_done: false,
                    command_registry: makima_commands::CommandRegistry::new(),
                    session_picker: false,
                    keymap_reader: ctx.keymap_reader,
                    hint_reader: ctx.hint_reader,
                    status_content_reader: ctx.status_content_reader,
                    ui_action_rx: harness._host.ui_action_rx(),
                    lua_event_handle: ctx.lua_event_handle,
                    model_policy: ctx.model_policy,
                    providers: ctx.providers,
                    session_defaults: ctx.session_defaults,
                    project_config: ctx.project_config,
                    trust_question: None,
                    startup_notice: None,
                    system_prompt_override: None,
                    append_system_prompt: None,
                },
                prepare,
                || InputReader::from_receiver(input),
            )
            .unwrap();
            assert_eq!(
                event_loop.sessions[0].app.root_config().model.spec(),
                PREPARED_MODEL
            );
            let report = event_loop.run(None).unwrap();
            finished_tx
                .send((report.exit, terminal.get_frame().count()))
                .unwrap();
        });
        let preparing = started.recv_timeout(PUMP_TIMEOUT);
        let _ = keys.send(Event::Key(KeyEvent::new(
            KeyCode::Char('c'),
            KeyModifiers::CONTROL,
        )));
        let result = finished.recv_timeout(PUMP_TIMEOUT);
        drop(release);
        drop(keys);
        worker.join().unwrap();
        preparing.unwrap();
        let (exit, frames) = result.unwrap();
        assert_eq!(exit, ExitRequest::Success);
        assert!(frames > 0);
    }

    #[test]
    fn gated_tab_open_prepare_keeps_loop_running_then_opens_tab() {
        with_event_loop(|event_loop| {
            let (prepare, started, release) = gated_preparer(Arc::new(StubProvider));
            event_loop.ctx.prepare_provider = prepare;
            assert_ne!(
                event_loop.ctx.model_slot.load().model.spec(),
                PREPARED_MODEL
            );
            let mut stored = AppSession::new(PREPARED_MODEL, &event_loop.ctx.workspace_cwd);
            stored.push_message(Message::user(STARTUP_PROMPT.into()));
            stored.save(&event_loop.ctx.storage).unwrap();
            event_loop.sessions[0]
                .app
                .input_box
                .set_input(STARTUP_PROMPT.into());

            let (reply_tx, reply_rx) = flume::bounded(1);
            event_loop.handle_session_request(
                SessionRequest::Focus {
                    id: stored.id.to_string(),
                },
                reply_tx,
            );
            started.recv_timeout(RUNTIME_SHUTDOWN_TIMEOUT).unwrap();
            let _ = event_loop.tick();
            assert!(reply_rx.is_empty());
            assert!(event_loop.position(stored.id).is_none());

            release.send(()).unwrap();
            pump_until(event_loop, |_| !reply_rx.is_empty());
            assert_eq!(reply_rx.try_recv().unwrap().unwrap(), json!(true));
            let index = event_loop.position(stored.id).unwrap();
            assert_eq!(event_loop.focused, index);
            assert_eq!(event_loop.sessions.len(), 2);
            assert_eq!(
                event_loop.sessions[index].model_slot.load().model.spec(),
                PREPARED_MODEL
            );
        });
    }

    #[test]
    fn turn_submitted_at_startup_completes_on_prepared_provider() {
        with_event_loop(|event_loop| {
            let pending = Arc::new(PendingProvider::default());
            let runtime = event_loop
                .ctx
                .spawn_runtime_with_provider(
                    AppSession::new(PREPARED_MODEL, &event_loop.ctx.workspace_cwd),
                    Some(PreparedProvider {
                        model: Model::from_spec(PREPARED_MODEL).unwrap(),
                        provider: Arc::clone(&pending) as Arc<dyn Provider>,
                    }),
                )
                .unwrap();
            let index = event_loop.push_runtime(runtime);
            let seeded = {
                let current = event_loop.sessions[index].model_slot.load();
                PreparedModel {
                    model: current.model.clone(),
                    provider: Arc::clone(&current.provider) as Arc<dyn Provider>,
                }
            };
            event_loop
                .submit_text(index, STARTUP_PROMPT.into())
                .unwrap();

            let (requests_tx, requests_rx) = flume::unbounded();
            let (prepare, started, release) = gated_preparer(Arc::new(AnsweringProvider {
                requests: requests_tx,
                usage: TokenUsage::default(),
            }));
            StartupPrepare {
                spec: Arc::from(PREPARED_MODEL),
                pending,
                root: false,
                targets: vec![(event_loop.sessions[index].id(), seeded)],
            }
            .spawn(prepare, event_loop.internal_tx.clone())
            .detach();
            started.recv_timeout(RUNTIME_SHUTDOWN_TIMEOUT).unwrap();
            assert!(requests_rx.is_empty());

            release.send(()).unwrap();
            assert_eq!(
                requests_rx.recv_timeout(RUNTIME_SHUTDOWN_TIMEOUT).unwrap(),
                PREPARED_MODEL
            );
            let (manager, root) = event_loop.sessions[index].handles.manager_and_root();
            let actor = manager.actor(root).unwrap();
            let before = event_loop.sessions[index]
                .model_slot
                .load()
                .provider
                .identity();
            pump_until(event_loop, |event_loop| {
                matches!(actor.snapshot().latest, Some(TurnOutcome::Completed { .. }))
                    && event_loop.sessions[index]
                        .model_slot
                        .load()
                        .provider
                        .identity()
                        != before
            });
            shutdown_manager(&manager);
        });
    }

    #[test_case(false; "startup_model_ready")]
    #[test_case(true; "startup_model_still_preparing")]
    fn recovered_turn_waits_for_committed_startup_model_fallback(startup_pending: bool) {
        const RESTORE_FAILURE: &str = "saved model credentials unavailable";
        const PREPARED_WINDOW: u32 = 123_456;
        const SAVED_INPUT_PRICE: f64 = 2.0;
        const FALLBACK_INPUT_PRICE: f64 = 4.0;
        const USAGE: TokenUsage = TokenUsage {
            input: 1_000,
            output: 0,
            cache_creation: 0,
            cache_read: 0,
        };
        const FALLBACK_COST: f64 = 0.004;
        let (waiting_tx, waiting_rx) = flume::bounded(1);
        let waiter = Arc::new(PendingProvider::with_request_probe(waiting_tx));
        let runtime_waiter = Arc::clone(&waiter);
        let (requests_tx, requests_rx) = flume::unbounded();
        let fallback: Arc<dyn Provider> = Arc::new(AnsweringProvider {
            requests: requests_tx,
            usage: USAGE,
        });
        let startup = Arc::new(PendingProvider::default());
        let root = AgentId::generate();
        let turn = TurnId::generate();
        with_event_loop_runtime(
            |harness| {
                if startup_pending {
                    let ctx = harness.ctx.as_mut().unwrap();
                    let model = ctx.model_slot.load().model.clone();
                    ctx.model_slot = ProviderSlot::new(model, startup.clone()).0;
                }
                let ctx = harness.ctx();
                let mut session = harness.session();
                session.model = PREPARED_MODEL.into();
                session.bind_root_id(root).unwrap();
                session.push_message(Message::user(STARTUP_PROMPT.into()));
                session.push_state(StoredStateRecord {
                    seq: session.next_state_seq(),
                    agent: Some(root),
                    generation: 0,
                    change: StateChange::Admitted {
                        turn,
                        origin: StoredTurnOrigin::User,
                    },
                });
                session.save(&ctx.storage).unwrap();
                let mut saved_model = Model::from_spec(PREPARED_MODEL).unwrap();
                saved_model.pricing.input = SAVED_INPUT_PRICE;
                let mut runtime = ctx
                    .prepare_runtime_with_options(
                        session,
                        Some(PreparedProvider {
                            model: saved_model,
                            provider: runtime_waiter.clone(),
                        }),
                        PrepareOptions::default(),
                        Some(runtime_waiter),
                    )
                    .unwrap()
                    .activate(&ctx.model_slot, None)
                    .unwrap();
                runtime.activate_deferred();
                runtime
            },
            |event_loop| {
                waiting_rx.recv_timeout(PUMP_TIMEOUT).unwrap();
                let runtime = &event_loop.sessions[0];
                let session = runtime.id();
                let actor = runtime.handles.manager_and_root().0.actor(root).unwrap();
                let ticket = actor.turn_ticket(turn).unwrap();
                let config = actor.effective_config().unwrap();
                assert_eq!(config.model.spec(), PREPARED_MODEL);
                assert_eq!(config.model.pricing.input, SAVED_INPUT_PRICE);
                let mut startup_model = event_loop.ctx.model_slot.load().model.clone();
                startup_model.context_window = PREPARED_WINDOW;
                startup_model.pricing.input = FALLBACK_INPUT_PRICE;
                let startup_spec = startup_model.spec();
                let startup_id = startup_model.id.clone();
                let prepared_startup = PreparedModel {
                    model: startup_model,
                    provider: fallback,
                };
                if !startup_pending {
                    event_loop.ctx.model_slot.install(
                        prepared_startup.model.clone(),
                        prepared_startup.provider.clone(),
                    );
                }
                StartupPrepare {
                    spec: Arc::from(PREPARED_MODEL),
                    pending: waiter,
                    root: false,
                    targets: vec![(
                        session,
                        PreparedModel {
                            model: config.model.clone(),
                            provider: config.provider.clone(),
                        },
                    )],
                }
                .spawn(
                    sync_preparer(|_| Err(Arc::from(RESTORE_FAILURE))),
                    event_loop.internal_tx.clone(),
                )
                .detach();
                let event = event_loop.internal_rx.recv_timeout(PUMP_TIMEOUT).unwrap();
                assert!(matches!(event, InternalEvent::StartupPrepared { .. }));
                event_loop.handle_internal(event);
                assert_eq!(
                    event_loop.sessions[0].app.flash_text(),
                    Some(
                        restore_warning(
                            PREPARED_MODEL,
                            &ConfigChangeError::ModelPreparation(RESTORE_FAILURE.into())
                                .to_string(),
                            &prepared_startup.model,
                        )
                        .as_str()
                    ),
                );
                if startup_pending {
                    assert_eq!(
                        actor.effective_config().unwrap().model.spec(),
                        PREPARED_MODEL
                    );
                    assert!(requests_rx.is_empty());
                    smol::block_on(startup.fill(Ok(prepared_startup)));
                }
                assert_eq!(
                    requests_rx.recv_timeout(PUMP_TIMEOUT).unwrap(),
                    startup_spec
                );
                let outcome = smol::block_on(bounded_session_op(
                    async { Ok(ticket.wait().await) },
                    PUMP_TIMEOUT,
                ))
                .unwrap();
                assert!(matches!(outcome, TurnOutcome::Completed { .. }));
                assert_eq!(outcome.turn_id(), turn);
                assert_eq!(
                    actor.effective_config().unwrap().model.context_window,
                    PREPARED_WINDOW
                );
                let saved = AppSession::load(session, &event_loop.ctx.storage).unwrap();
                let state = saved.latest_state().unwrap();
                assert_eq!(
                    state.scopes[&Some(root)].config.as_ref().unwrap().model,
                    startup_spec
                );
                assert_eq!(saved.usage_by_model().len(), 1);
                assert_eq!(
                    saved.usage_by_model()[&startup_id],
                    USAGE.billed(Some(FALLBACK_COST))
                );

                let mut switched = None;
                let mut completed = None;
                let events = &event_loop.sessions[0].handles.events;
                while let Some(SessionEvent::Agent(envelope)) = events.try_next() {
                    match envelope.event {
                        AgentEvent::ModelSwitched {
                            spec,
                            context_window,
                        } => {
                            switched = Some((spec, context_window));
                        }
                        AgentEvent::TurnComplete(event) => completed = Some(event),
                        _ => {}
                    }
                }
                assert_eq!(switched, Some((startup_spec, PREPARED_WINDOW)));
                let completed = completed.unwrap();
                assert_eq!(completed.model, startup_id);
                assert_eq!(completed.cost, Some(FALLBACK_COST));
            },
        );
    }

    #[test]
    fn resumed_session_uses_its_stored_model_and_provider() {
        const STORED_MODEL: &str = "openai/gpt-5";

        let mut harness = RuntimeHarness::new();
        harness.ctx.as_mut().unwrap().prepare_provider = sync_preparer(|model| {
            Ok(makima_agent::PreparedModel {
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
            runtime.model_slot.load().provider.key_count(),
            FOCUSED_KEY_COUNT
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
                    option.definition.id.as_ref() == makima_agent::session_options::MODEL_OPTION_ID
                })
                .unwrap()
                .current_value
                .as_ref(),
            STORED_MODEL
        );
        smol::block_on(
            runtime
                .coordinator
                .set_model(makima_commands::ModelSelection {
                    spec: Some(Arc::from(STORED_MODEL)),
                    ..Default::default()
                }),
        )
        .unwrap();
        assert_eq!(runtime.model_slot.load().model.spec(), STORED_MODEL);
        release_runtime(runtime);
    }

    #[test_case(false ; "saved_plan_file")]
    #[test_case(true ; "blocked_plan_file")]
    fn restored_plan_prepares_saved_path_before_actor_admission(blocked: bool) {
        use makima_storage::plans::{
            ApprovedPlan, PLAN_STATE_KEY, PLAN_STATE_VERSION, PlanEntry, PlanSlot,
        };
        use makima_storage::session_state::{StateChange, StoredStateRecord};

        const PLAN: &str = "restored-plan.md";
        const CONTENT: &str = "approved plan restored before admission";
        let harness = RuntimeHarness::new();
        let prepared = harness.prepare();
        let mut session = prepared.prepared.stored().unwrap().clone();
        let root = session.root_agent_id().unwrap();
        let mut stored = session.latest_state().unwrap().scopes[&Some(root)]
            .config
            .clone()
            .unwrap();
        let path = harness._temp_dir.path().canonicalize().unwrap().join(PLAN);
        if blocked {
            fs::create_dir(&path).unwrap();
        }
        stored.mode = makima_agent::ModeDef::default_for(makima_agent::ModeId::Plan)
            .stored_snapshot(&makima_agent::AgentMode::Plan(path.clone()));
        let seq = session
            .state()
            .iter()
            .map(|record| record.seq)
            .chain(session.agent_revisions().iter().map(|(seq, _)| *seq))
            .max()
            .unwrap()
            + 1;
        session.push_state(StoredStateRecord {
            seq,
            agent: Some(root),
            generation: 0,
            change: StateChange::Config(stored),
        });
        let plans = PlanSlot::from([(
            root,
            PlanEntry {
                path: path.clone(),
                written: true,
                approved: Some(ApprovedPlan {
                    content: CONTENT.into(),
                    sha256: content_sha256(CONTENT),
                }),
                staged_model: None,
            },
        )]);
        session.push_state(StoredStateRecord {
            seq: seq + 1,
            agent: None,
            generation: 0,
            change: StateChange::Plugin {
                key: PLAN_STATE_KEY.into(),
                version: PLAN_STATE_VERSION,
                value: Some(serde_json::to_value(plans).unwrap()),
            },
        });
        harness.append_fixture(
            &session,
            prepared.lease.as_ref().unwrap().publication_guard(),
        );
        drop(prepared);
        let runtime = harness.runtime(session);
        let config = runtime.config().unwrap();
        if blocked {
            assert!(makima_agent::modes::is_read_only_fallback(&config.mode));
            assert!(runtime.app.state.plan.path().is_none());
        } else {
            assert_eq!(config.mode.plan_path(), Some(path.as_path()));
            assert_eq!(fs::read_to_string(&path).unwrap(), CONTENT);
            assert_eq!(runtime.app.state.plan.path(), Some(path.as_path()));
        }
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
        let root = saved.root_agent_id().unwrap();
        assert_eq!(
            saved.latest_state().unwrap().scopes[&Some(root)]
                .config
                .as_ref()
                .unwrap()
                .mode
                .id,
            config.mode.id().key()
        );
        assert_eq!(
            saved.latest_state().unwrap().scopes[&Some(root)]
                .config
                .as_ref()
                .unwrap()
                .mode
                .restrict_write_to
                .as_deref(),
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

        let view = manager.journal().snapshot().fold().unwrap();
        assert_eq!(view.scopes[&None].yolo, Some(true));
        assert!(view.scopes[&Some(root)].config.as_ref().unwrap().workflow);
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
        let (session_id, _, root_id) = prepared.snapshot();

        assert_eq!(harness.target_count(), target_count);
        assert!(SessionMailbox::notify(session_id, "early".into(), false).is_err());
        assert_eq!(
            prepared.prepared.stored().unwrap().root_agent_id(),
            Some(root_id)
        );
        assert!(prepared.prepared.session().runner_start_is_pending());

        drop(prepared);
    }

    #[test]
    fn activation_publishes_target_mailbox_and_runtime_identities() {
        let harness = RuntimeHarness::new();
        let target_count = harness.target_count();
        let prepared = harness.prepare();
        let (session_id, target_id, root_id) = prepared.snapshot();
        let runtime = prepared.activate(&harness.ctx().model_slot, None).unwrap();
        let (active_manager, active_root_id) = runtime.handles.manager_and_root();

        assert_eq!(harness.target_count(), target_count + 1);
        assert_eq!(runtime.id(), session_id);
        assert_eq!(runtime.app.command_target.id(), target_id);
        assert_eq!(active_manager.root_id().unwrap(), root_id);
        assert_eq!(active_root_id, root_id);
        assert!(runtime.handles.queue.is_empty());
        assert!(SessionMailbox::notify(session_id, "ready".into(), false).is_ok());

        drop(runtime);
        shutdown_manager(&active_manager);
        assert_eq!(harness.target_count(), target_count);
        assert!(SessionMailbox::notify(session_id, "late".into(), false).is_err());
    }

    #[test_case(false; "fresh_session")]
    #[test_case(true; "restored_graph")]
    fn runtime_manager_preserves_all_configured_agent_limits(restored: bool) {
        const CONCURRENT: usize = 2;
        const DEPTH: usize = 3;
        const CHILDREN: usize = 5;
        const ACTIVE: usize = 7;
        const RETAINED: usize = 11;
        let mut harness = RuntimeHarness::new();
        let config = &mut harness.ctx.as_mut().unwrap().config;
        config.max_concurrent_agent_turns = CONCURRENT;
        config.max_agent_depth = DEPTH;
        config.max_children_per_agent = CHILDREN;
        config.max_active_agents = ACTIVE;
        config.max_retained_agents = RETAINED;
        let session = if restored {
            interrupted_session(&harness).0
        } else {
            harness.session()
        };
        let runtime = harness.runtime(session);
        let limits = runtime.handles.manager_and_root().0.limits();
        assert_eq!(limits.max_concurrent_agent_turns, CONCURRENT);
        assert_eq!(limits.max_agent_depth, DEPTH);
        assert_eq!(limits.max_children_per_agent, CHILDREN);
        assert_eq!(limits.max_active_agents, ACTIVE);
        assert_eq!(limits.max_retained_agents, RETAINED);
        release_runtime(runtime);
    }

    fn interrupted_session(
        harness: &RuntimeHarness,
    ) -> (AppSession, AgentId, AgentId, TurnId, TurnId) {
        const CHILD_KEY: &str = "recovered-child";
        const TOOL: &str = "interrupted-tool";
        let mut session = harness.session();
        let root = AgentId::generate();
        let child = AgentId::generate();
        session.bind_root_id(root).unwrap();
        let mut record = session.agents()[&root].clone();
        record.id = child;
        record.parent = Some(root);
        record.depth = 1;
        record.transcript_key = CHILD_KEY.into();
        record.label = Some(CHILD_KEY.into());
        let seq = session.agent_revisions().last().unwrap().0 + 1;
        record.created_seq = seq;
        record.updated_seq = seq;
        session.push_agent_revision(seq, record);
        let tool_call = Message {
            role: makima_providers::Role::Assistant,
            content: vec![makima_providers::ContentBlock::ToolUse {
                id: TOOL.into(),
                name: TOOL.into(),
                input: json!({}),
                thought_signature: None,
            }],
            ..Default::default()
        };
        session.replace_messages(vec![tool_call.clone()]);
        session.replace_journal_histories(&[(child, HistorySnapshot::new(vec![tool_call]))].into());
        let root_turn = TurnId::generate();
        let child_turn = TurnId::generate();
        for (offset, agent, turn) in [(1, root, root_turn), (2, child, child_turn)] {
            session.push_state(makima_storage::session_state::StoredStateRecord {
                seq: seq + offset,
                agent: Some(agent),
                generation: 0,
                change: makima_storage::session_state::StateChange::Admitted {
                    turn,
                    origin: makima_storage::session_state::StoredTurnOrigin::User,
                },
            });
        }
        (session, root, child, root_turn, child_turn)
    }

    #[test_case(false; "startup")]
    #[test_case(true; "replacement")]
    fn recovery_preserves_pending_turns_before_activation(replace: bool) {
        const DRAFT: &str = "unsent draft";
        let harness = RuntimeHarness::new();
        let mut outgoing = replace.then(|| harness.runtime(harness.session()));
        let (mut session, root, child, root_turn, child_turn) = interrupted_session(&harness);
        session.meta.input_draft = Some(DRAFT.into());
        let session_id = session.id;
        let prepared = harness.ctx().prepare_runtime(session).unwrap();
        assert!(prepared.prepared.session().runner_start_is_pending());
        let bootstrap = AppSession::load(session_id, &harness.ctx().storage).unwrap();
        assert_eq!(bootstrap.root_agent_id(), Some(root));
        const NOT_ACTIVATED: &str = "must not enter unpublished mailbox";
        assert!(SessionMailbox::notify(session_id, NOT_ACTIVATED.into(), false).is_err());
        let prepared_history = prepared.prepared.stored().unwrap().history_snapshot();
        let mut runtime = if let Some(current) = outgoing.as_mut() {
            prepared
                .activate_replacing(&harness.ctx().model_slot, None, current)
                .unwrap_or_else(|(error, _)| panic!("{error}"))
        } else {
            prepared.activate(&harness.ctx().model_slot, None).unwrap()
        };
        assert!(runtime.handles.runner_start_is_pending());
        let loaded = AppSession::load(session_id, &harness.ctx().storage).unwrap();
        let view = loaded.latest_state().unwrap();
        for (agent, turn) in [(root, root_turn), (child, child_turn)] {
            let scope = &view.scopes[&Some(agent)];
            assert!(scope.results.is_empty());
            assert!(scope.admitted.contains_key(&turn));
        }
        assert_eq!(
            loaded
                .messages()
                .last()
                .unwrap()
                .content
                .iter()
                .filter(|block| matches!(block, makima_providers::ContentBlock::ToolUse { .. }))
                .count(),
            1
        );
        let child_key = &loaded.agents()[&child].transcript_key;
        assert_eq!(
            loaded
                .subagent_history_snapshot(child_key)
                .unwrap()
                .messages
                .last()
                .unwrap()
                .content
                .iter()
                .filter(|block| matches!(block, makima_providers::ContentBlock::ToolUse { .. }))
                .count(),
            1
        );
        let (manager, root_id) = runtime.handles.manager_and_root();
        assert_eq!(root_id, root);
        assert!(manager.actor(root).is_ok());
        assert!(manager.actor(child).is_err());
        assert_eq!(manager.node(child).unwrap().parent_id, Some(root));
        let history = runtime.handles.history.load_full();
        assert_eq!(history.epoch, prepared_history.epoch);
        assert_eq!(history.messages, prepared_history.messages);
        assert_eq!(history.messages, loaded.history_snapshot().messages);
        assert!(Arc::ptr_eq(&history.messages, &prepared_history.messages));
        assert!(runtime.handles.queue.is_empty());
        runtime.activate_deferred();
        assert_eq!(loaded.meta.input_draft.as_deref(), Some(DRAFT));
        assert_eq!(runtime.app.input_box.buffer.value(), DRAFT);
        assert_eq!(
            manager
                .actor(root)
                .unwrap()
                .latest_ticket()
                .unwrap()
                .turn_id(),
            root_turn
        );
        release_runtime(runtime);
        if let Some(outgoing) = outgoing {
            release_runtime(outgoing);
        }
    }

    #[test_case(false, false; "fresh_startup")]
    #[test_case(true, false; "resumed_startup")]
    #[test_case(true, true; "same_session_replacement")]
    fn startup_publishes_complete_options_before_first_turn(resumed: bool, replace: bool) {
        use makima_storage::session_state::{StateChange, StoredStateRecord};

        const PLUGIN: &str = "startup_options";
        const PERSISTENT: &str = "startup_options.persistent";
        const TRANSIENT: &str = "startup_options.transient";
        const NEW_DEFAULT: &str = "startup_options.new_default";
        let harness = RuntimeHarness::new();
        let catalog = harness.ctx().lua_event_handle.session_option_catalog();
        let definitions = [PERSISTENT, TRANSIENT, NEW_DEFAULT]
            .into_iter()
            .map(|id| SessionOptionDefinition {
                id: id.into(),
                owner: SessionOptionOwner::Plugin {
                    plugin: PLUGIN.into(),
                    generation: 1,
                },
                name: id.into(),
                description: id.into(),
                category: SessionOptionCategory::Mode,
                values: [DISABLED_VALUE, ENABLED_VALUE]
                    .into_iter()
                    .map(|value| SessionOptionValue {
                        value: value.into(),
                        name: value.into(),
                    })
                    .collect(),
                free_value: None,
                initial_value: DISABLED_VALUE.into(),
                persistent: id != TRANSIENT,
            })
            .collect();
        smol::block_on(catalog.replace_plugin_options(PLUGIN, definitions)).unwrap();
        let mut current = replace.then(|| harness.runtime(harness.session()));
        let session = if resumed {
            let prepared = current.is_none().then(|| harness.prepare());
            let (mut session, guard) = if let Some(current) = &current {
                (
                    AppSession::load(current.id(), &harness.ctx().storage).unwrap(),
                    current
                        .session_lock
                        .as_ref()
                        .unwrap()
                        .publication_guard()
                        .unwrap(),
                )
            } else {
                let prepared = prepared.as_ref().unwrap();
                (
                    prepared.prepared.stored().unwrap().clone(),
                    prepared.lease.as_ref().unwrap().publication_guard(),
                )
            };
            let root = session.root_agent_id().unwrap();
            let folded = session.latest_state().unwrap();
            let mut stored = folded.scopes[&Some(root)].config.clone().unwrap();
            stored
                .options
                .insert(PERSISTENT.into(), ENABLED_VALUE.into());
            stored
                .options
                .insert(TRANSIENT.into(), ENABLED_VALUE.into());
            stored.options.remove(NEW_DEFAULT);
            session.push_state(StoredStateRecord {
                seq: session.state().last().unwrap().seq + 1,
                agent: Some(root),
                generation: folded.scopes[&Some(root)].generation,
                change: StateChange::Config(stored),
            });
            harness.append_fixture(&session, guard);
            drop(prepared);
            AppSession::load(session.id, &harness.ctx().storage).unwrap()
        } else {
            harness.session()
        };
        let prepared = match &current {
            Some(current) => harness
                .ctx()
                .prepare_replacement_runtime(session, None, current)
                .unwrap(),
            None => harness.ctx().prepare_runtime(session).unwrap(),
        };
        let runtime = if let Some(mut current) = current.take() {
            let outgoing =
                replace_session_runtime(&mut current, prepared, &harness.ctx().model_slot).unwrap();
            release_runtime(outgoing);
            current
        } else {
            prepared.activate(&harness.ctx().model_slot, None).unwrap()
        };
        let (manager, root) = runtime.handles.manager_and_root();
        assert!(manager.actor(root).unwrap().latest_ticket().is_none());
        let expected = runtime.coordinator.read().options().config_values();
        assert_eq!(
            expected[PERSISTENT],
            if resumed {
                ENABLED_VALUE
            } else {
                DISABLED_VALUE
            }
        );
        assert_eq!(expected[TRANSIENT], DISABLED_VALUE);
        assert_eq!(expected[NEW_DEFAULT], DISABLED_VALUE);
        let snapshot = manager.journal().snapshot();
        assert_eq!(manager.journal().published_revision(), snapshot.revision);
        let folded = snapshot.fold().unwrap();
        assert_eq!(
            folded.scopes[&Some(root)].config.as_ref().unwrap().options,
            expected
        );
        let disk = AppSession::load(runtime.id(), &harness.ctx().storage).unwrap();
        assert_eq!(
            disk.latest_state().unwrap().scopes[&Some(root)]
                .config
                .as_ref()
                .unwrap()
                .options,
            expected
        );
        release_runtime(runtime);
    }

    #[test]
    fn owned_existing_runtime_reloads_checkpoint_before_identity_history_and_config() {
        use makima_storage::session_state::{StateChange, StoredStateRecord};
        const STALE_TEXT: &str = "stale preloaded history";
        const CURRENT_TEXT: &str = "final checkpoint after teardown";
        const CURRENT_DRAFT: &str = "latest saved draft";
        let harness = RuntimeHarness::new();
        let prepared = harness.prepare();
        let mut current = prepared.prepared.stored().unwrap().clone();
        drop(prepared);
        current.replace_messages(vec![Message::user(STALE_TEXT.into())]);
        current.save(&harness.ctx().storage).unwrap();
        let stale = AppSession::load(current.id, &harness.ctx().storage).unwrap();
        let lease = claim_lock(&harness.ctx().sessions_dir, &current.id).unwrap();
        let root = current.root_agent_id().unwrap();
        let mut config = current.latest_state().unwrap().scopes[&Some(root)]
            .config
            .clone()
            .unwrap();
        config.workflow = true;
        current.push_state(StoredStateRecord {
            seq: current.next_state_seq(),
            agent: Some(root),
            generation: 1,
            change: StateChange::Config(config),
        });
        current.replace_messages(vec![Message::user(CURRENT_TEXT.into())]);
        current.meta.input_draft = Some(CURRENT_DRAFT.into());
        current.save(&harness.ctx().storage).unwrap();
        lease.release().unwrap();
        let prepared = harness.ctx().prepare_runtime(stale).unwrap();
        let recovered = prepared.prepared.stored().unwrap();
        assert_eq!(recovered.root_agent_id(), Some(root));
        assert_eq!(recovered.messages().len(), 1);
        assert_eq!(
            serde_json::to_value(recovered.messages()).unwrap(),
            serde_json::to_value(current.messages()).unwrap()
        );
        assert_eq!(recovered.meta.input_draft.as_deref(), Some(CURRENT_DRAFT));
        assert!(prepared.app.app.root_config().workflow);
        let runtime = prepared.activate(&harness.ctx().model_slot, None).unwrap();
        let loaded = AppSession::load(current.id, &harness.ctx().storage).unwrap();
        assert_eq!(
            serde_json::to_value(loaded.messages()).unwrap(),
            serde_json::to_value(current.messages()).unwrap()
        );
        release_runtime(runtime);
    }

    #[test_case(true; "matching_provider")]
    #[test_case(false; "stale_provider")]
    fn owned_existing_runtime_keeps_only_a_provider_matching_the_startup_checkpoint(
        matching: bool,
    ) {
        const STALE_SPEC: &str = "openai/stale-startup-provider";
        let harness = RuntimeHarness::new();
        let mut session = harness.session();
        session.save(&harness.ctx().storage).unwrap();
        let supplied = PreparedProvider {
            model: if matching {
                harness.ctx().model_slot.load().model.clone()
            } else {
                Model::from_spec(STALE_SPEC).unwrap()
            },
            provider: Arc::new(FocusedProvider),
        };
        let provider = if matching {
            Arc::clone(&supplied.provider)
        } else {
            Arc::clone(&harness.ctx().model_slot.load().provider) as Arc<dyn Provider>
        };
        let prepared = harness
            .ctx()
            .prepare_runtime_with_provider(session, Some(supplied))
            .unwrap();
        match prepared.provider.as_ref() {
            Some(retained) => {
                assert!(matching);
                assert_eq!(retained.provider.key_count(), provider.key_count());
            }
            None => assert!(!matching),
        }
        let tracked = Arc::clone(&prepared.model_slot.load().provider) as Arc<dyn Provider>;
        let config = prepared.app.app.root_config();
        assert!(Arc::ptr_eq(&config.provider, &tracked));
        assert_eq!(config.provider.key_count(), provider.key_count());
    }

    #[test_case(false; "authoritative_model")]
    #[test_case(true; "explicit_startup_model")]
    fn owned_existing_runtime_discards_provider_prepared_from_stale_checkpoint(explicit: bool) {
        use makima_storage::session_state::{StateChange, StoredStateRecord};
        let mut harness = RuntimeHarness::new();
        let prepared = harness.prepare();
        let mut current = prepared.prepared.stored().unwrap().clone();
        let stale = AppSession::load(current.id, &harness.ctx().storage).unwrap();
        let root = current.root_agent_id().unwrap();
        let latest_model = Model::from_spec("openai/authoritative-checkpoint").unwrap();
        let startup = harness.ctx().model_slot.load().model.spec();
        harness.ctx.as_mut().unwrap().initial_model_override = explicit;
        let stale_model = Model::from_spec("openai/stale-preparation").unwrap();
        let expected = latest_model.spec();
        let mut config = current.latest_state().unwrap().scopes[&Some(root)]
            .config
            .clone()
            .unwrap();
        config.model = expected.clone();
        current.push_state(StoredStateRecord {
            seq: current.next_state_seq(),
            agent: Some(root),
            generation: 1,
            change: StateChange::Config(config),
        });
        harness.append_fixture(
            &current,
            prepared.lease.as_ref().unwrap().publication_guard(),
        );
        drop(prepared);
        let (requested, requests) = flume::unbounded();
        let sessions = harness.ctx().sessions_dir.clone();
        let id = current.id;
        harness.ctx.as_mut().unwrap().prepare_provider = sync_preparer(move |model| {
            assert!(session_lock::claim(&sessions, &id).unwrap().is_none());
            requested.send(model.spec()).unwrap();
            Ok(PreparedModel {
                model,
                provider: Arc::new(StubProvider),
            })
        });
        let prepared = harness
            .ctx()
            .prepare_runtime_with_provider(
                stale,
                (!explicit).then(|| PreparedProvider {
                    model: stale_model,
                    provider: Arc::new(StubProvider),
                }),
            )
            .unwrap();
        if explicit {
            assert!(requests.is_empty());
            assert_eq!(prepared.app.app.root_config().model.spec(), startup);
        } else {
            assert_eq!(requests.try_recv().unwrap(), expected);
            assert!(requests.is_empty());
            assert_eq!(prepared.app.app.root_config().model.spec(), expected);
            assert_eq!(prepared.provider.as_ref().unwrap().model.spec(), expected);
        }
        let selected = if explicit { startup } else { expected };
        let runtime = prepared.activate(&harness.ctx().model_slot, None).unwrap();
        assert_eq!(runtime.config().unwrap().model.spec(), selected);
        let saved = AppSession::load(current.id, &harness.ctx().storage).unwrap();
        assert_eq!(
            saved.latest_state().unwrap().scopes[&Some(root)]
                .config
                .as_ref()
                .unwrap()
                .model,
            selected
        );
        release_runtime(runtime);
    }

    #[test_case(false; "session_prepare")]
    #[test_case(true; "startup_prepare")]
    fn startup_prepare_does_not_retarget_an_authoritative_runtime(root_prepare: bool) {
        const STALE_SPEC: &str = "openai/stale-startup-preparation";
        with_event_loop(|event_loop| {
            let runtime = &event_loop.sessions[0];
            let (manager, root) = runtime.handles.manager_and_root();
            let actor = manager.actor(root).unwrap();
            let current = actor.effective_config().unwrap();
            let target = (
                runtime.id(),
                PreparedModel {
                    model: current.model.clone(),
                    provider: Arc::clone(&current.provider),
                },
            );
            assert_ne!(current.model.spec(), STALE_SPEC);
            event_loop.complete_startup_prepare(
                Arc::from(STALE_SPEC),
                root_prepare,
                vec![target],
                Ok(PreparedModel {
                    model: Model::from_spec(STALE_SPEC).unwrap(),
                    provider: Arc::new(StubProvider),
                }),
                None,
            );
            let settled = smol::block_on(settled_config(&actor));
            assert_eq!(settled.model.spec(), current.model.spec());
            assert!(Arc::ptr_eq(&settled.provider, &current.provider));
            drain_config_commits(&mut event_loop.sessions[0]);
            assert_eq!(
                event_loop.sessions[0].app.root_config().model.spec(),
                current.model.spec()
            );
        });
    }

    #[test_case(true; "loaded_checkpoint")]
    #[test_case(false; "saved_origin_checkpoint")]
    fn owned_existing_runtime_rejects_deleted_empty_preloaded_checkpoint(reload: bool) {
        let harness = RuntimeHarness::new();
        let mut session = harness.session();
        session.save(&harness.ctx().storage).unwrap();
        let stale = if reload {
            AppSession::load(session.id, &harness.ctx().storage).unwrap()
        } else {
            session.clone()
        };
        assert!(stale.messages().is_empty());
        assert!(stale.agents().is_empty());
        assert!(stale.checkpoint_required());
        let path = harness
            .ctx()
            .sessions_dir
            .join(format!("{}.jsonl", session.id));
        fs::remove_file(&path).unwrap();
        assert!(harness.ctx().prepare_runtime(stale).is_err());
        assert!(!path.exists());
        assert!(
            session_lock::claim(&harness.ctx().sessions_dir, &session.id)
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn owned_existing_runtime_does_not_replace_a_corrupt_checkpoint_with_preloaded_state() {
        let harness = RuntimeHarness::new();
        let mut session = harness.session();
        session.save(&harness.ctx().storage).unwrap();
        let stale = AppSession::load(session.id, &harness.ctx().storage).unwrap();
        let path = harness
            .ctx()
            .sessions_dir
            .join(format!("{}.jsonl", session.id));
        const CORRUPT: &[u8] = b"not a session checkpoint\n";
        fs::write(&path, CORRUPT).unwrap();
        assert!(harness.ctx().prepare_runtime(stale).is_err());
        assert_eq!(fs::read(path).unwrap(), CORRUPT);
        assert!(
            session_lock::claim(&harness.ctx().sessions_dir, &session.id)
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn unsaved_runtime_keeps_transient_seed_without_checkpoint_reload() {
        const DRAFT: &str = "unsaved draft";
        let harness = RuntimeHarness::new();
        let mut session = harness.session();
        session.meta.input_draft = Some(DRAFT.into());
        let id = session.id;
        let prepared = harness.ctx().prepare_runtime(session).unwrap();
        assert_eq!(
            prepared
                .prepared
                .stored()
                .unwrap()
                .meta
                .input_draft
                .as_deref(),
            Some(DRAFT)
        );
        assert_eq!(
            AppSession::load(id, &harness.ctx().storage)
                .unwrap()
                .meta
                .input_draft
                .as_deref(),
            Some(DRAFT)
        );
    }

    #[test]
    fn recovery_restores_durable_root_config_instead_of_legacy_metadata() {
        use makima_storage::session_state::{StateChange, StoredStateRecord};

        const DIRECTORY: &str = "durable-cwd";
        const LABEL: &str = "stored build";
        const PROMPT: &str = "stored mode prompt";
        const TOOL: &str = "read";
        let mut harness = RuntimeHarness::new();
        let mut model = harness.ctx().model_slot.load().model.clone();
        model.fast_support = Some(FastSupport::Supported);
        harness.ctx.as_mut().unwrap().model_slot =
            ProviderSlot::new(model, Arc::new(StubProvider)).0;
        let prepared = harness.prepare();
        let mut session = prepared.prepared.stored().unwrap().clone();
        let root = session.root_agent_id().unwrap();
        let mut stored = session.latest_state().unwrap().scopes[&Some(root)]
            .config
            .clone()
            .unwrap();
        let cwd = harness._temp_dir.path().join(DIRECTORY);
        fs::create_dir(&cwd).unwrap();
        stored.cwd = cwd.to_string_lossy().into_owned();
        stored.fast = true;
        stored.workflow = true;
        stored.thinking = Some(DomainThinkingConfig::Off.into());
        stored.mode.label = LABEL.into();
        stored.mode.system_prompt = Some(PROMPT.into());
        stored.mode.tools = Some(vec![TOOL.into()]);
        let seq = session.state().last().unwrap().seq + 1;
        session.push_state(StoredStateRecord {
            seq,
            agent: Some(root),
            generation: 1,
            change: StateChange::Config(stored.clone()),
        });
        session.set_cwd(stored.cwd.clone());
        harness.append_fixture(
            &session,
            prepared.lease.as_ref().unwrap().publication_guard(),
        );
        drop(prepared);

        harness.ctx.as_mut().unwrap().workspace_cwd = stored.cwd.clone();
        let runtime = harness.runtime(session);
        let config = runtime.config().unwrap();
        assert_eq!(config.context.cwd(), cwd);
        assert!(config.fast);
        assert!(config.workflow);
        assert_eq!(config.thinking, DomainThinkingConfig::Off);
        assert_eq!(
            config
                .mode_def
                .as_ref()
                .unwrap()
                .stored_snapshot(&config.mode),
            stored.mode
        );
        assert_eq!(*runtime.handles.cwd_slot().load_full(), cwd);
        assert_eq!(runtime.app.state.projection.cwd(), stored.cwd);
        let loaded = AppSession::load(runtime.id(), &harness.ctx().storage).unwrap();
        stored.options = runtime.coordinator.read().options().config_values();
        assert_eq!(
            loaded.latest_state().unwrap().scopes[&Some(root)]
                .config
                .as_ref(),
            Some(&stored)
        );
        release_runtime(runtime);
    }

    #[test_case(false; "startup")]
    #[test_case(true; "replacement")]
    fn recovery_publication_failure_does_not_expose_runtime(replace: bool) {
        let (finished, completion) = flume::bounded(1);
        let worker = std::thread::spawn(move || {
            const NOT_EXPOSED: &str = "not exposed";
            let harness = RuntimeHarness::new();
            let mut current = replace.then(|| harness.runtime(harness.session()));
            let (session, _, _, _, _) = interrupted_session(&harness);
            let session_id = session.id;
            let prepared = harness.ctx().prepare_runtime(session).unwrap();
            let target_count = harness.target_count();
            let bootstrap = fs::read(
                harness
                    .ctx()
                    .sessions_dir
                    .join(format!("{session_id}.jsonl")),
            )
            .unwrap();
            smol::block_on(bounded_session_op(
                async {
                    prepared
                        .prepared
                        .writer()
                        .unwrap()
                        .close()
                        .await
                        .map_err(|error| error.to_string())
                },
                PUMP_TIMEOUT,
            ))
            .expect("closing recovery writer timed out");
            if let Some(current) = current.as_mut() {
                let identity = current.handles.identity();
                assert!(
                    replace_session_runtime(current, prepared, &harness.ctx().model_slot).is_err()
                );
                assert!(Arc::ptr_eq(&current.handles.identity(), &identity));
                assert!(matches!(
                    current.session_lock,
                    Some(SessionLockState::Held(_))
                ));
            } else {
                assert!(prepared.activate(&harness.ctx().model_slot, None).is_err());
            }
            assert_eq!(harness.target_count(), target_count);
            assert!(SessionMailbox::notify(session_id, NOT_EXPOSED.into(), false).is_err());
            assert_eq!(
                fs::read(
                    harness
                        .ctx()
                        .sessions_dir
                        .join(format!("{session_id}.jsonl"))
                )
                .unwrap(),
                bootstrap
            );
            let lock = claim_lock(&harness.ctx().sessions_dir, &session_id).unwrap();
            lock.release().unwrap();
            if let Some(current) = current {
                release_runtime(current);
            }
            finished.send(()).unwrap();
        });
        completion
            .recv_timeout(SESSION_OP_TIMEOUT + PUMP_TIMEOUT)
            .expect("recovery publication failure cleanup did not finish");
        worker.join().unwrap();
    }

    #[test]
    fn recovery_failure_stop_rejects_submissions_and_mailbox_runs() {
        const FAILURE: &str = RECOVERY_ACTIVATION_FAILED;
        const PROMPT: &str = "must not be admitted";
        let harness = RuntimeHarness::new();
        let mut runtime = harness.runtime(harness.session());
        let (manager, root) = runtime.handles.manager_and_root();
        let actor = manager.actor(root).unwrap();
        stop_runtime_after_recovery_failure(&mut runtime, FAILURE);
        assert!(runtime.lock_lost);
        assert!(!runtime.quiescent());
        assert!(runtime.submit_text(PROMPT.into()).is_err());
        assert!(runtime.handles.queue.is_empty());
        assert!(actor.latest_ticket().is_none());
        assert_eq!(
            actor.snapshot().lifecycle,
            makima_agent::ActorLifecycle::Closed
        );
        assert_eq!(runtime.app.exit_request, ExitRequest::Error);
        release_runtime(runtime);
    }

    #[test]
    fn guard_loss_after_successful_replacement_initialization_fails_closed() {
        const PROMPT: &str = "must not run after guard loss";
        let harness = RuntimeHarness::new();
        let mut runtime = harness.runtime(harness.session());
        let session_id = runtime.id();
        let generation = runtime.generation;
        let old_actor = runtime
            .handles
            .manager_and_root()
            .0
            .actor(runtime.handles.manager_and_root().1)
            .unwrap();
        let mut prepared = harness
            .ctx()
            .prepare_replacement_runtime(
                AppSession::load(session_id, &harness.ctx().storage).unwrap(),
                None,
                &runtime,
            )
            .unwrap();
        let target = prepared.app.command_target_id();
        let target_count = harness.target_count();
        let startup_provider = harness.ctx().model_slot.load().provider.identity();
        let (entered, entering) = flume::bounded(1);
        let (release, released) = flume::bounded(1);
        prepared.before_publish = Some(Box::new(move || {
            entered.send(()).unwrap();
            released.recv().unwrap();
            Ok(())
        }));
        let lock_path = session_lock::lock_path(&harness.ctx().sessions_dir, &session_id);
        let observer = std::thread::spawn(move || {
            entering.recv_timeout(RUNTIME_SHUTDOWN_TIMEOUT).unwrap();
            fs::write(lock_path, format!("{} replacement", std::process::id())).unwrap();
            release.send(()).unwrap();
        });
        let result = replace_session_runtime(&mut runtime, prepared, &harness.ctx().model_slot);
        observer.join().unwrap();
        let error = result
            .err()
            .expect("guard loss must reject activation commit");
        assert!(error.contains(LOCK_LOST_REPLACEMENT_ERR), "{error}");
        assert_eq!(runtime.generation, generation);
        assert_ne!(runtime.app.command_target.id(), target);
        assert_eq!(harness.target_count(), target_count);
        assert_eq!(
            harness.ctx().model_slot.load().provider.identity(),
            startup_provider
        );
        assert!(runtime.lock_lost);
        assert_eq!(runtime.app.exit_request, ExitRequest::Error);
        assert!(!runtime.quiescent());
        assert!(runtime.submit_text(PROMPT.into()).is_err());
        assert_eq!(
            old_actor.snapshot().lifecycle,
            makima_agent::ActorLifecycle::Closed
        );
        assert!(SessionCoordinatorHandle::resolve(session_id).is_err());
        assert!(SessionMailbox::notify(session_id, PROMPT.into(), false).is_err());
        release_runtime(runtime);
    }

    #[test]
    fn failed_same_session_initialization_keeps_current_runtime() {
        const FAILURE: &str = "replacement root activation failed";
        let harness = RuntimeHarness::new();
        let mut runtime = harness.runtime(harness.session());
        let before = runtime.handles.identity();
        let session = AppSession::load(runtime.id(), &harness.ctx().storage).unwrap();
        let mut prepared = harness
            .ctx()
            .prepare_replacement_runtime(session, None, &runtime)
            .unwrap();
        let (entered, entering) = flume::bounded(1);
        let (release, released) = flume::bounded(1);
        prepared.before_publish = Some(Box::new(move || {
            entered.send(()).unwrap();
            released.recv().unwrap();
            Err(FAILURE.into())
        }));
        let observer = std::thread::spawn(move || {
            entering.recv_timeout(RUNTIME_SHUTDOWN_TIMEOUT).unwrap();
            release.send(()).unwrap();
        });
        let error = replace_session_runtime(&mut runtime, prepared, &harness.ctx().model_slot)
            .err()
            .expect("initialization must fail");
        observer.join().unwrap();
        assert!(error.contains(FAILURE), "{error}");
        assert!(Arc::ptr_eq(&runtime.handles.identity(), &before));
        assert!(!runtime.lock_lost);
        harness.persist_runtime(&mut runtime);
        release_runtime(runtime);
    }

    #[test]
    fn replacement_restore_effects_activate_once_after_publication() {
        const TOOL_ID: &str = "restored-tool";
        const TOOL_NAME: &str = "test_tool";
        const TOOL_RESULT: &str = "saved output";
        const HEADER: &str = "restored header";
        const DRAFT: &str = "restored draft";
        with_event_loop_runtime(
            |harness| {
                let mut session = harness.session();
                session.push_message(Message {
                    role: Role::Assistant,
                    content: vec![ContentBlock::tool_use(TOOL_ID, TOOL_NAME, json!({}))],
                    ..Default::default()
                });
                session.push_message(Message {
                    role: Role::User,
                    content: vec![ContentBlock::ToolResult {
                        tool_use_id: TOOL_ID.into(),
                        content: TOOL_RESULT.into(),
                        is_error: false,
                    }],
                    ..Default::default()
                });
                session.meta.input_draft = Some(DRAFT.into());
                let prepared = harness.ctx().prepare_runtime(session).unwrap();
                prepared.activate(&harness.ctx().model_slot, None).unwrap()
            },
            |event_loop| {
                let (_input_tx, input_rx) = flume::unbounded();
                event_loop.input = InputReader::from_receiver(input_rx);
                let runtime = &mut event_loop.sessions[0];
                assert!(runtime.handles.queue.is_empty());
                assert!(runtime.restore_pending);
                let header = BufferSnapshot::plain_text(HEADER.into());
                runtime
                    .app
                    .restore_event_tx
                    .as_ref()
                    .unwrap()
                    .send(AgentEvent::ToolHeaderSnapshot {
                        id: TOOL_ID.into(),
                        snapshot: header.clone(),
                        theme_gen: None,
                    })
                    .unwrap();
                runtime.activate_deferred();
                runtime.activate_deferred();

                assert!(runtime.handles.queue.is_empty());
                assert_eq!(runtime.app.input_box.buffer.value(), DRAFT);
                let (manager, root_id) = runtime.handles.manager_and_root();
                assert!(manager.actor(root_id).unwrap().latest_ticket().is_none());
                assert!(!runtime.restore_pending);
                assert!(runtime.app.chats[0].has_tool(TOOL_ID));
                while let Some(wake) = event_loop.next_wake(Duration::ZERO) {
                    let restored = matches!(&wake, Wake::Agent(0, event)
                        if matches!(&event.event, AgentEvent::ToolHeaderSnapshot { id, .. } if id == TOOL_ID));
                    event_loop.handle_wake(wake).unwrap();
                    if restored {
                        break;
                    }
                }
                let tool = event_loop.sessions[0].app.chats[0].message_at(0).unwrap();
                assert_eq!(tool.render_header.as_ref(), Some(&header));
            },
        );
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

        assert_eq!(runtime.app.state.projection.draft(), Some(DRAFT));
        smol::block_on(runtime.handles.manager_and_root().0.journal().flush()).unwrap();
        let stored = AppSession::load(runtime.id(), &harness.ctx().storage).unwrap();
        assert_eq!(stored.meta.input_draft.as_deref(), Some(DRAFT));

        release_runtime(runtime);
    }

    #[test]
    fn replacement_preparation_installs_the_session_provider_on_activation() {
        const REPLACEMENT_MODEL: &str = "ollama/hf:test-model";

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
    fn replacement_with_unavailable_saved_model_uses_startup() {
        const INVALID_MODEL: &str = "unsupported-provider/test-model";

        with_event_loop(|event_loop| {
            let startup = event_loop.ctx.model_slot.load().model.spec();
            let mut session = AppSession::new(INVALID_MODEL, &event_loop.ctx.workspace_cwd);
            session.save(&event_loop.ctx.storage).unwrap();
            let (reply_tx, reply_rx) = flume::bounded(1);
            event_loop.bring_up(RuntimeTarget::Open { reply: reply_tx }, session.clone());
            pump_until(event_loop, |_| !reply_rx.is_empty());

            assert!(reply_rx.try_recv().unwrap().is_ok());
            let runtime = &event_loop.sessions[event_loop.position(session.id).unwrap()];
            assert_eq!(runtime.config().unwrap().model.spec(), startup);
            assert!(
                runtime
                    .app
                    .state
                    .warnings
                    .iter()
                    .any(|warning| warning.contains(INVALID_MODEL))
            );
        });
    }

    #[test]
    fn saved_provider_preparation_failure_uses_startup_without_retrying() {
        const PREPARE_FAILURE: &str = "test provider unavailable";
        with_event_loop(|event_loop| {
            let startup = event_loop.ctx.model_slot.load().model.spec();
            let (attempt_tx, attempts) = flume::unbounded();
            event_loop.ctx.prepare_provider = Arc::new(move |spec| {
                attempt_tx.send(spec).unwrap();
                Box::pin(async { Err(ConfigChangeError::from(PREPARE_FAILURE)) })
            });
            let mut session = AppSession::new(PREPARED_MODEL, &event_loop.ctx.workspace_cwd);
            session.save(&event_loop.ctx.storage).unwrap();
            let (reply_tx, reply_rx) = flume::bounded(1);
            event_loop.bring_up(RuntimeTarget::Open { reply: reply_tx }, session.clone());
            pump_until(event_loop, |_| !reply_rx.is_empty());

            assert!(reply_rx.try_recv().unwrap().is_ok());
            let runtime = &event_loop.sessions[event_loop.position(session.id).unwrap()];
            assert_eq!(runtime.config().unwrap().model.spec(), startup);
            assert!(runtime.app.state.warnings.iter().any(|warning| {
                warning.contains(PREPARED_MODEL) && warning.contains(PREPARE_FAILURE)
            }));
            assert_eq!(attempts.try_recv().unwrap().as_ref(), PREPARED_MODEL);
            assert!(attempts.is_empty());
        });
    }

    #[test]
    fn deferred_blank_tab_replacement_keeps_a_tab_that_gained_a_draft() {
        const MODEL: &str = "ollama/hf:test-model";
        const DRAFT: &str = "typed while the model was prepared";

        with_event_loop(|event_loop| {
            let current_id = event_loop.sessions[0].id();
            let session = AppSession::new(MODEL, &event_loop.ctx.workspace_cwd);
            let loaded_id = session.id;
            let (reply_tx, reply_rx) = flume::bounded(1);
            let target = RuntimeTarget::Replace {
                runtime: event_loop.sessions[0].handles.identity(),
                kind: None,
                reply: Some(reply_tx),
            };
            event_loop.sessions[0].app.input_box.set_input(DRAFT.into());
            let provider = PreparedProvider {
                model: Model::from_spec(MODEL).unwrap(),
                provider: Arc::new(StubProvider),
            };

            event_loop.complete_bring_up(target, session, Ok(provider));

            assert!(reply_rx.try_recv().unwrap().is_ok());
            assert_eq!(event_loop.sessions.len(), 2);
            assert_eq!(event_loop.sessions[0].id(), current_id);
            assert_eq!(event_loop.sessions[0].app.input_box.buffer.value(), DRAFT);
            assert_eq!(event_loop.sessions[event_loop.focused].id(), loaded_id);
        });
    }

    #[test]
    fn one_manager_per_runtime() {
        let harness = RuntimeHarness::new();
        let first = harness.runtime(harness.session());
        let second = harness.runtime(harness.session());
        let (first_manager, first_root_id) = first.handles.manager_and_root();
        let (second_manager, second_root_id) = second.handles.manager_and_root();

        assert_ne!(first_manager.generation(), second_manager.generation());
        assert_ne!(first_root_id, second_root_id);

        release_runtime(first);
        release_runtime(second);
    }

    #[test]
    fn empty_replacement_loads_session_permission_rules_during_preparation() {
        let harness = RuntimeHarness::new();
        let current = harness.runtime(harness.session());
        current.app.permissions.apply_decision(
            &makima_config::ToolKey::native("bash"),
            &["cargo test".into()],
            &PermissionAnswer::AllowSession,
        );
        let mut replacement = harness.session();
        replacement.meta.session_rules = current.app.permissions.snapshot().stored_rules();
        assert!(replacement.messages().is_empty());

        let prepared = harness.ctx().prepare_runtime(replacement).unwrap();

        assert!(
            prepared
                .app
                .session_rule_allows(&makima_config::ToolKey::native("bash"), "cargo test")
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
                    option.definition.id.as_ref() == makima_agent::session_options::YOLO_OPTION_ID
                })
                .unwrap()
                .current_value
                .as_ref(),
            if persisted_yolo {
                makima_agent::session_options::ENABLED_VALUE
            } else {
                makima_agent::session_options::DISABLED_VALUE
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
        assert_eq!(runtime.app.permissions.snapshot().yolo, Some(true));
        assert_eq!(
            runtime
                .app
                .state
                .projection
                .fold()
                .unwrap()
                .scopes
                .get(&None)
                .and_then(|scope| scope.yolo),
            Some(true)
        );
        let stored = AppSession::load(runtime.id(), &harness.ctx().storage).unwrap();
        assert_eq!(
            stored.latest_state().unwrap().scopes[&None].yolo,
            Some(true)
        );
        let options = runtime.coordinator.read().options();
        let yolo = options
            .options
            .iter()
            .find(|option| {
                option.definition.id.as_ref() == makima_agent::session_options::YOLO_OPTION_ID
            })
            .unwrap();
        assert_eq!(
            yolo.current_value.as_ref(),
            makima_agent::session_options::ENABLED_VALUE
        );
        release_runtime(runtime);
    }

    #[test]
    fn startup_yolo_seed_applies_without_marking_session() {
        let harness = RuntimeHarness::with_permissions_config(PermissionsConfig {
            yolo: true,
            ..Default::default()
        });

        let runtime = harness
            .prepare()
            .activate(&harness.ctx().model_slot, None)
            .unwrap();

        assert!(runtime.app.permissions.is_yolo());
        assert_eq!(
            runtime
                .app
                .state
                .projection
                .fold()
                .unwrap()
                .scopes
                .get(&None)
                .and_then(|scope| scope.yolo),
            None
        );
        let options = runtime.coordinator.read().options();
        let yolo = options
            .options
            .iter()
            .find(|option| {
                option.definition.id.as_ref() == makima_agent::session_options::YOLO_OPTION_ID
            })
            .unwrap();
        assert_eq!(
            yolo.current_value.as_ref(),
            makima_agent::session_options::ENABLED_VALUE
        );

        smol::block_on(runtime.handles.manager_and_root().0.journal().flush()).unwrap();
        let stored = AppSession::load(runtime.id(), &harness.ctx().storage).unwrap();
        assert_eq!(stored.meta.yolo, None);
        release_runtime(runtime);
    }

    #[test_case(false, true, false ; "same_session_preserves_enabled")]
    #[test_case(true, false, false ; "same_session_preserves_disabled")]
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
            &makima_config::ToolKey::native("bash"),
            &["outgoing".into()],
            &PermissionAnswer::AllowSession,
        );
        let mut replacement = if reset { harness.session() } else { session };
        replacement.model = runtime.app.root_config().model.spec();
        replacement.meta.session_rules = vec![makima_storage::sessions::StoredRule {
            tool: "bash".into(),
            scope: Some("target".into()),
            effect: makima_storage::sessions::StoredEffect::Allow,
        }];
        let prepared = harness
            .ctx()
            .prepare_replacement_runtime(replacement, None, &runtime)
            .unwrap();

        let old =
            replace_session_runtime(&mut runtime, prepared, &harness.ctx().model_slot).unwrap();

        assert_eq!(runtime.app.permissions.is_yolo(), current_yolo);
        assert_eq!(
            runtime
                .coordinator
                .read()
                .options()
                .options
                .iter()
                .find(|option| {
                    option.definition.id.as_ref() == makima_agent::session_options::YOLO_OPTION_ID
                })
                .unwrap()
                .current_value
                .as_ref(),
            if current_yolo {
                makima_agent::session_options::ENABLED_VALUE
            } else {
                makima_agent::session_options::DISABLED_VALUE
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

    #[test_case(false ; "same_session")]
    #[test_case(true ; "reset")]
    fn replacement_preserves_unsaved_input_history(reset: bool) {
        const PROMPT: &str = "not saved to disk";

        let harness = RuntimeHarness::new();
        let session = harness.session();
        let mut runtime = harness.runtime(session.clone());
        runtime.app.input_box.set_input(PROMPT.into());
        assert_eq!(runtime.app.input_box.submit().unwrap().text, PROMPT);
        let replacement = if reset { harness.session() } else { session };
        let prepared = harness
            .ctx()
            .prepare_replacement_runtime(replacement, None, &runtime)
            .unwrap();

        let old =
            replace_session_runtime(&mut runtime, prepared, &harness.ctx().model_slot).unwrap();
        runtime.app.input_box.history_up();

        assert_eq!(runtime.app.input_box.buffer.value(), PROMPT);
        release_runtime(old);
        release_runtime(runtime);
    }

    #[test_case(false; "lua_send")]
    #[test_case(true; "typed_input")]
    fn reactivation_persists_plan_fallback_before_first_provider_call(typed: bool) {
        use makima_agent::manager::{AgentResidency, GraphLifecycle};
        use makima_agent::tools::ToolAudience;
        use makima_agent::{AgentMode, ModeDef, ModeId};
        use makima_storage::session_state::{
            SessionStateView, StateChange, StoredAgentSpec, StoredModeCeiling, StoredStateRecord,
        };

        const PLUGIN: &str = "tui-reactivation-fallback";
        const COMMAND: &str = "/reactivate-plan-child";
        const CHILD: &str = "saved-plan-child";
        const OBSERVED: &str = "reactivated child exposes read-only";
        const READ_ONLY: &str = "recovery_read_only";
        const PLAN: &str = "plan";
        const INPUT: &str = "continue the saved child";

        struct DiskConfigProvider {
            storage: StateDir,
            session: MakimaId,
            requests: flume::Sender<Result<SessionStateView, String>>,
        }

        impl Provider for DiskConfigProvider {
            fn stream_message<'a>(
                &'a self,
                _model: &'a Model,
                _messages: &'a [Message],
                _system: &'a str,
                _tools: &'a serde_json::Value,
                _event_tx: &'a flume::Sender<makima_providers::ProviderEvent>,
                _opts: makima_providers::RequestOptions,
                _session_id: Option<&'a SessionRef>,
                _invocation: &'a Invocation,
                _delivery: RequestDelivery,
            ) -> makima_providers::provider::BoxFuture<
                'a,
                Result<makima_providers::StreamResponse, makima_providers::AgentError>,
            > {
                Box::pin(async move {
                    let saved = AppSession::load(self.session, &self.storage)
                        .and_then(|session| session.latest_state())
                        .map_err(|error| error.to_string());
                    self.requests.send(saved).unwrap();
                    std::future::pending().await
                })
            }
        }

        let child = AgentId::generate();
        let providers = Arc::new(makima_providers::test_util::offline_registry(vec![(
            crate::components::TEST_MODEL_PROVIDER,
            vec![makima_providers::ModelInfo::id_only(
                crate::components::test_model().id,
            )],
        )]));
        with_event_loop_runtime_providers(
            &providers,
            |harness| {
                let prepared = harness.prepare();
                let mut session = prepared.prepared.stored().unwrap().clone();
                let root = session.root_agent_id().unwrap();
                let mut stored = session.latest_state().unwrap().scopes[&Some(root)]
                    .config
                    .clone()
                    .unwrap();
                let plan = harness._temp_dir.path().join(CHILD);
                fs::create_dir(&plan).unwrap();
                stored.mode =
                    ModeDef::default_for(ModeId::Plan).stored_snapshot(&AgentMode::Plan(plan));
                let seq = session
                    .state()
                    .iter()
                    .map(|record| record.seq)
                    .chain(session.agent_revisions().iter().map(|(seq, _)| *seq))
                    .max()
                    .unwrap()
                    + 1;
                let mut record = session.agents()[&root].clone();
                record.id = child;
                record.parent = Some(root);
                record.depth = 1;
                record.transcript_key = CHILD.into();
                record.label = Some(CHILD.into());
                record.created_seq = seq;
                record.updated_seq = seq;
                record.spec = Some(StoredAgentSpec {
                    model: Default::default(),
                    system: String::new(),
                    tools: Vec::new(),
                    audience: ToolAudience::MAIN.name().unwrap().into(),
                    mcp: false,
                    output_schema: None,
                    name: Some(CHILD.into()),
                    silent: true,
                    auto_deliver: false,
                    inherit_provider: false,
                    mode_ceiling: StoredModeCeiling {
                        restrict_write_to: None,
                        tools: None,
                    },
                    plugin: Some(PLUGIN.into()),
                });
                session.push_agent_revision(seq, record);
                session.push_state(StoredStateRecord {
                    seq: seq + 1,
                    agent: Some(child),
                    generation: 0,
                    change: StateChange::Config(stored),
                });
                harness.append_fixture(
                    &session,
                    prepared.lease.as_ref().unwrap().publication_guard(),
                );
                drop(prepared);
                harness.runtime(session)
            },
            |event_loop| {
                let runtime = &event_loop.sessions[0];
                let session = runtime.id();
                let (manager, root) = runtime.handles.manager_and_root();
                let root_config = runtime.config().unwrap();
                let before = AppSession::load(session, &event_loop.ctx.storage)
                    .unwrap()
                    .latest_state()
                    .unwrap();
                assert_eq!(
                    before.scopes[&Some(child)].config.as_ref().unwrap().mode.id,
                    PLAN
                );
                let node = manager.node(child).unwrap();
                assert_eq!(node.graph_lifecycle, GraphLifecycle::Dormant);
                assert_eq!(node.residency, AgentResidency::Dormant);
                let (requests, received) = flume::unbounded();
                let provider: Arc<dyn Provider> = Arc::new(DiskConfigProvider {
                    storage: event_loop.ctx.storage.clone(),
                    session,
                    requests,
                });
                let command_runtime = Arc::clone(&event_loop.ctx.command_runtime);
                let app = &mut event_loop.sessions[0].app;
                app.command_target = command_runtime.bind_target();
                app.command_runtime = Arc::clone(&command_runtime);
                let host = PluginHost::with_session_provider_preparer(
                    Arc::new(ToolRegistry::new()),
                    command_runtime.registry.clone(),
                    false,
                    Arc::new(makima_lua::test_support::InMemoryFs::new()),
                    Some(event_loop.ctx.storage.path().to_path_buf()),
                    Some(Arc::new(move |model| Ok((model, Arc::clone(&provider))))),
                    makima_lua::ui_sink::HostUi::Frontend,
                )
                .unwrap();
                host.event_handle()
                    .install_orchestration_services(event_loop.orchestration.clone());
                host.load_source(
                    PLUGIN,
                    &format!(
                        r#"
                        makima.api.register_command({{
                            name = "{COMMAND}", tui_only = false,
                            handler = function(_, ctx)
                                local child = assert(makima.agent.get(ctx, "{child}"))
                                assert(not child:is_live())
                                assert(assert(child:config(ctx)).mode == "{PLAN}")
                                local ticket, err = child:send(ctx, "{INPUT}", {{ when = "idle" }})
                                assert(ticket, err)
                                assert(child:is_live())
                                assert(assert(child:config(ctx)).mode == "{READ_ONLY}")
                                makima.ui.flash("{OBSERVED}")
                            end,
                        }})
                        "#
                    ),
                )
                .unwrap();
                event_loop.sync_orchestration();
                assert!(received.is_empty());
                if typed {
                    let app = &mut event_loop.sessions[0].app;
                    app.active_chat = app.agent_chat_index[&child];
                    event_loop.handle_input(Event::Paste(INPUT.into()));
                    event_loop.handle_input(Event::Key(KeyEvent::new(
                        KeyCode::Enter,
                        KeyModifiers::NONE,
                    )));
                    assert_eq!(event_loop.sessions[0].app.input_box.buffer.value(), INPUT);
                    loop {
                        let completion = event_loop.internal_rx.recv_timeout(PUMP_TIMEOUT).unwrap();
                        let woke =
                            if let InternalEvent::AgentWoken { agent, result, .. } = &completion {
                                assert_eq!(*agent, child);
                                result.as_ref().unwrap();
                                true
                            } else {
                                false
                            };
                        event_loop.handle_internal(completion);
                        if woke {
                            break;
                        }
                    }
                    let app = &event_loop.sessions[0].app;
                    assert!(app.input_box.is_empty(), "{:?}", app.flash_text());
                } else {
                    let outcome = smol::block_on(bounded_session_op(
                        async {
                            Ok(command_runtime
                                .registry
                                .dispatch_input(
                                    &event_loop.sessions[0].app.command_target,
                                    COMMAND.into(),
                                )
                                .await)
                        },
                        PUMP_TIMEOUT,
                    ))
                    .unwrap();
                    assert!(
                        matches!(
                            outcome,
                            makima_commands::InputDispatch::Dispatched(
                                makima_commands::CommandOutcome::Completed
                            )
                        ),
                        "{outcome:?}"
                    );
                    assert!(matches!(
                        host.ui_action_rx().recv_timeout(PUMP_TIMEOUT).unwrap(),
                        UiAction::Flash(text) if text == OBSERVED
                    ));
                }
                let saved = received.recv_timeout(PUMP_TIMEOUT).unwrap().unwrap();
                let child_config = saved.scopes[&Some(child)].config.as_ref().unwrap();
                assert_eq!(child_config.mode.id, READ_ONLY);
                assert!(child_config.mode.restrict_write_to.is_none());
                assert_eq!(saved.scopes[&Some(root)], before.scopes[&Some(root)]);
                let node = manager.node(child).unwrap();
                assert_eq!(node.graph_lifecycle, GraphLifecycle::Live);
                assert_eq!(node.residency, AgentResidency::Resident);
                let config = manager.actor(child).unwrap().effective_config().unwrap();
                assert!(makima_agent::modes::is_read_only_fallback(&config.mode));
                assert!(
                    config
                        .mode_def
                        .as_ref()
                        .unwrap()
                        .restrict_write_to
                        .is_none()
                );
                assert!(Arc::ptr_eq(
                    &root_config,
                    &event_loop.sessions[0].config().unwrap()
                ));
                assert_eq!(
                    event_loop.sessions[0].app.selected_agent_id(),
                    Some(if typed { child } else { root })
                );
                manager.close_subtree(child).unwrap();
            },
        );
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
            let host = makima_lua::PluginHost::with_command_registry(
                Arc::clone(makima_agent::tools::ToolRegistry::global_arc()),
                registry.clone(),
                false,
                makima_lua::ui_sink::HostUi::Frontend,
            )
            .unwrap();
            host.event_handle()
                .install_orchestration_services(event_loop.orchestration.clone());
            host.load_source(
                "closed_task_command",
                &format!(
                    r#"
                    makima.api.register_command({{
                        name = "/spawn-command-task", tui_only = false,
                        handler = function(_, ctx)
                            assert(makima.agent.spawn(ctx, {{
                                inherit_provider = true, silent = true,
                            }}))
                            makima.ui.flash("{SPAWNED}")
                        end,
                    }})
                    makima.api.register_command({{
                        name = "/ordinary-command", tui_only = false,
                        handler = function(_, ctx)
                            assert(ctx == nil)
                            local ok, child = pcall(makima.agent.spawn, ctx, {{
                                inherit_provider = true, silent = true,
                            }})
                            assert(not ok or child == nil)
                            makima.ui.flash("{OBSERVED}")
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
            assert!(manager.lookup(child_id).unwrap().validate_live().is_err());
            assert!(matches!(
                smol::block_on(queued),
                makima_commands::InputDispatch::Dispatched(
                    makima_commands::CommandOutcome::Completed
                )
            ));
            assert!(matches!(
                actions.recv_timeout(Duration::from_secs(5)).unwrap(),
                UiAction::Flash(text) if text == OBSERVED
            ));
            assert!(manager.lookup(root_id).unwrap().validate_live().is_ok());
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
            let child = manager.node(child_id).unwrap();
            assert_eq!(child.graph_lifecycle, makima_agent::GraphLifecycle::Closed);
            assert_eq!(
                child.residency,
                makima_agent::manager::AgentResidency::Dormant
            );
            assert!(child.actor.is_none());
            assert!(manager.actor(child_id).is_err());
            assert!(manager.lookup(root_id).unwrap().validate_live().is_ok());
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
                request: makima_commands::HostRequest::Context(if fast_support {
                    makima_commands::HostContextRequest::FastModeSupported
                } else {
                    makima_commands::HostContextRequest::WorkingDirectory
                }),
                admission: Some(Box::new(
                    PinnedCommandTarget::pin(TrustedTarget { target, template }).unwrap(),
                )),
                reply,
            });
            let response = response.recv().unwrap().unwrap();
            match response {
                HostResponse::Context(makima_commands::HostContextResponse::WorkingDirectory(
                    path,
                )) => {
                    assert!(!fast_support);
                    assert_eq!(path, expected.context.cwd());
                }
                HostResponse::Context(makima_commands::HostContextResponse::FastModeSupported(
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
                model.fast_support = Some(FastSupport::Supported);
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
                request: makima_commands::HostRequest::Builtin(if workflow {
                    BuiltinOperation::ToggleWorkflow
                } else {
                    BuiltinOperation::ToggleFast
                }),
                admission: Some(Box::new(
                    PinnedCommandTarget::pin(TrustedTarget { target, template }).unwrap(),
                )),
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
            let base = directory.path().canonicalize().unwrap();
            let child = base.join("child");
            std::fs::create_dir(&child).unwrap();
            let barrier = actor.reserve_prepared_operation(None).unwrap();
            event_loop
                .dispatch_target_directory(0, &target, PathBuf::from("child"))
                .unwrap();
            barrier
                .resolve(Some(ConfigChange::WorkingDirectory(base)))
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

    /// A plugin that tracks the working directory refreshes on this event, so
    /// it has to fire once per real move and never on a rejected one.
    #[test]
    fn cwd_changed_fires_only_when_the_move_succeeds() {
        with_event_loop(|event_loop| {
            let host = PluginHost::with_command_registry(
                Arc::new(ToolRegistry::new()),
                event_loop.ctx.command_runtime.registry.clone(),
                false,
                makima_lua::ui_sink::HostUi::Frontend,
            )
            .unwrap();
            host.event_handle()
                .install_orchestration_services(event_loop.orchestration.clone());
            host.load_source(CWD_OBSERVER_PLUGIN, CWD_OBSERVER).unwrap();
            event_loop.ctx.lua_event_handle = host.event_handle();
            event_loop.sessions[0].app.lua_event_handle = host.event_handle();

            let moved = tempfile::tempdir().unwrap();
            let moved = moved.path().canonicalize().unwrap();
            let expected = [moved.to_string_lossy().into_owned()];

            dispatch_cd(event_loop, moved.clone());
            pump_directory_change(event_loop).unwrap();
            assert_eq!(observed_cwds(event_loop, &host), expected);
            let runtime = &event_loop.sessions[0];
            assert_eq!(runtime.app.state.projection.cwd(), expected[0]);
            let stored = AppSession::load(runtime.id(), &event_loop.ctx.storage).unwrap();
            assert_eq!(stored.cwd, expected[0]);

            dispatch_cd(event_loop, moved.join("missing"));
            assert!(pump_directory_change(event_loop).is_err());
            assert_eq!(observed_cwds(event_loop, &host), expected);
            assert_eq!(
                event_loop.sessions[0].app.state.projection.cwd(),
                expected[0]
            );
        });
    }

    fn dispatch_cd(event_loop: &mut EventLoop<'_>, path: PathBuf) {
        let (manager, root) = event_loop.sessions[0].handles.manager_and_root();
        let target = manager.lookup(root).unwrap();
        let template = event_loop.sessions[0]
            .handles
            .orchestration_template
            .clone();
        let (reply, response) = flume::bounded(1);
        event_loop.handle_command(CommandEvent::Host {
            target: event_loop.sessions[0].app.command_target.id(),
            request: makima_commands::HostRequest::Builtin(BuiltinOperation::ChangeDirectory {
                path,
            }),
            admission: Some(Box::new(
                PinnedCommandTarget::pin(TrustedTarget { target, template }).unwrap(),
            )),
            reply,
        });
        assert!(matches!(
            response.recv().unwrap().unwrap(),
            HostResponse::Completed
        ));
    }

    fn pump_directory_change(event_loop: &mut EventLoop<'_>) -> Result<(), String> {
        loop {
            let event = event_loop
                .internal_rx
                .recv_timeout(SESSION_OP_TIMEOUT)
                .expect(SESSION_OP_TIMEOUT_ERR);
            if let InternalEvent::SessionOp {
                kind: SessionOpKind::DirectoryChanged { .. },
                result,
                ..
            } = &event
            {
                let result = result.clone();
                event_loop.handle_internal(event);
                return result;
            }
            event_loop.handle_internal(event);
        }
    }

    fn observed_cwds(event_loop: &EventLoop<'_>, host: &PluginHost) -> Vec<String> {
        host.drain_tasks_for_test();
        event_loop
            .ctx
            .command_runtime
            .registry
            .snapshot_for(&event_loop.sessions[0].app.command_target)
            .unwrap()
            .commands()
            .iter()
            .map(|command| command.spec())
            .filter(|spec| spec.name.starts_with(CWD_OBSERVED_PREFIX))
            .map(|spec| spec.docs.summary.to_string())
            .collect()
    }

    #[test]
    fn failed_directory_persistence_does_not_publish_actor_context() {
        with_event_loop(|event_loop| {
            let model = event_loop.sessions[0].app.root_config().model.clone();
            let session = AppSession::new(&model.spec(), &event_loop.ctx.workspace_cwd);
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
                event_loop.sessions[0].app.state.projection.cwd(),
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
            let Ok(HostResponse::Context(makima_commands::HostContextResponse::Values(specs))) =
                event_loop.target_model_specs(
                    &command_runtime
                        .target_admission(&event_loop.sessions[0].app.command_target)
                        .unwrap(),
                )
            else {
                panic!("model specs request failed");
            };
            assert_eq!(&*specs, [Arc::<str>::from(ALLOWED)]);
            let complete = |target: &makima_commands::TargetHandle| {
                let registry = &command_runtime.registry;
                let command = registry.resolve_for(target, "/model").unwrap();
                let completion = registry.open_completion(command, target.id()).unwrap();
                let makima_commands::CompletionResult::Items(items) = smol::block_on(
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
            let config = target.actor().unwrap().effective_config().unwrap();
            assert_eq!(
                items.iter().any(|spec| spec == "thinking=high"),
                config.model.supports_thinking() == Some(true)
            );
            assert_eq!(
                items.iter().any(|spec| spec == "fast=true"),
                config.model.supports_fast()
            );
            let current = |spec: &str| {
                let mut selection = config.model_selection();
                selection.spec = Arc::from(spec);
                selection
            };
            let picker = &mut event_loop.sessions[0].app.model_picker;
            picker.set_recents(vec![EXCLUDED.into()]);
            picker.open(
                current(EXCLUDED),
                ModelCapabilities::NONE,
                &makima_commands::ModelSelection::default(),
            );
            assert_eq!(picker.highlighted_spec(), Some(EXCLUDED));
            assert!(matches!(
                picker.handle_key(crate::components::key(KeyCode::Enter)),
                crate::components::model_picker::ModelPickerAction::Select(selection)
                    if selection == makima_commands::ModelSelection::default()
            ));

            picker.open(
                current(ALLOWED),
                ModelCapabilities::NONE,
                &makima_commands::ModelSelection::default(),
            );
            event_loop.sessions[0]
                .handles
                .orchestration_template
                .model_policy = Arc::new(ModelPolicy::new(&[], &[ALLOWED.into()]).unwrap());
            event_loop.sync_orchestration();
            assert_eq!(
                event_loop.sessions[0].app.model_picker.highlighted_spec(),
                Some(ALLOWED)
            );
            assert!(matches!(
                event_loop.sessions[0].app.model_picker.handle_key(crate::components::key(KeyCode::Enter)),
                crate::components::model_picker::ModelPickerAction::Select(selection)
                    if selection == makima_commands::ModelSelection::default()
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
            let host = makima_lua::PluginHost::with_command_registry(
                Arc::clone(makima_agent::tools::ToolRegistry::global_arc()),
                command_runtime.registry.clone(),
                false,
                makima_lua::ui_sink::HostUi::Frontend,
            )
            .unwrap();
            host.event_handle()
                .install_orchestration_services(event_loop.orchestration.clone());
            host.load_source(
                "tasks_child_input",
                &format!(
                    r#"
                    makima.api.register_command({{
                        name = "/spawn-tasks-child", tui_only = false,
                        handler = function(_, ctx)
                            assert(makima.agent.spawn(ctx, {{ inherit_provider = true, show = false }}))
                            makima.ui.flash("{SPAWNED}")
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
            pump_until(event_loop, |_| {
                root_actor.prepare_idle().is_ok() && child_actor.prepare_idle().is_ok()
            });
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
            app.agent_chat_index.insert(child, app.chats.len());
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
                let SessionEvent::Agent(envelope) = smol::block_on(bounded_session_op(
                    async {
                        event_loop.sessions[0]
                            .handles
                            .events
                            .next()
                            .await
                            .ok_or_else(|| {
                                "session event stream closed before consuming child input"
                                    .to_owned()
                            })
                    },
                    deadline.saturating_duration_since(Instant::now()),
                ))
                .expect("child input must reach the session event stream");
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
                        makima_commands::InputDispatch::Dispatched(
                            makima_commands::CommandOutcome::Failed(CommandError::StaleTarget)
                        )
                    ));
                } else {
                    assert!(matches!(
                        outcome,
                        makima_commands::InputDispatch::Dispatched(
                            makima_commands::CommandOutcome::Completed
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
            event_loop.ctx.prepare_provider = sync_preparer(|model| {
                Ok(makima_agent::actor::PreparedModel {
                    model,
                    provider: Arc::new(StubProvider),
                })
            });
            let app = &mut event_loop.sessions[0].app;
            app.command_target = command_runtime.bind_target();
            app.command_runtime = Arc::clone(&command_runtime);
            let host = makima_lua::PluginHost::with_command_registry(
                Arc::clone(makima_agent::tools::ToolRegistry::global_arc()),
                command_runtime.registry.clone(),
                false,
                makima_lua::ui_sink::HostUi::Frontend,
            )
            .unwrap();
            host.event_handle()
                .install_orchestration_services(event_loop.orchestration.clone());
            host.load_source(
                "model_picker_child",
                &format!(
                    r#"
                    makima.api.register_command({{
                        name = "/spawn-picker-child", tui_only = false,
                        handler = function(_, ctx)
                            assert(makima.agent.spawn(ctx, {{ inherit_provider = true, silent = true }}))
                            makima.ui.flash("{SPAWNED}")
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
                        model: Some(makima_agent::actor::PreparedModel {
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
                        | Action::SelectPinnedModel { .. }
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
                matches!(actions.as_slice(), [Action::SelectPinnedModel { target, selection }]
                if target.target.id() == child && selection.is_empty())
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
                matches!(actions.as_slice(), [Action::SelectPinnedModel { target, selection }]
                if target.target.id() == child && selection.spec.as_deref() == Some(NEXT_MODEL)
                    && !selection.has_option_overrides())
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
            let host = makima_lua::PluginHost::with_command_registry(
                Arc::clone(makima_agent::tools::ToolRegistry::global_arc()),
                registry.clone(),
                false,
                makima_lua::ui_sink::HostUi::Frontend,
            )
            .unwrap();
            host.event_handle()
                .install_orchestration_services(event_loop.orchestration.clone());
            event_loop.sessions[0].app.lua_event_handle = host.event_handle();
            host.load_source(
                "keymap_pin_command",
                &format!(
                    r#"
                    makima.api.register_command({{
                        name = "/spawn-keymap-child", tui_only = false,
                        handler = function(_, ctx)
                            assert(makima.agent.spawn(ctx, {{ inherit_provider = true, silent = true }}))
                            makima.ui.flash("{SPAWNED}")
                        end,
                    }})
                    makima.api.register_command({{
                        name = "/keymap-inner", tui_only = false,
                        handler = function(_, ctx)
                            local target = assert(ctx:agent())
                            assert(target:set_model(ctx, {{ thinking = "{PINNED_THINKING}" }}))
                            makima.ui.flash("{INNER_DONE}")
                        end,
                    }})
                    makima.keymap.set("n", "<C-y>", function(_, ctx)
                        assert(makima.api.run_command(ctx, "/keymap-inner"))
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
            let host = makima_lua::PluginHost::with_command_registry(
                Arc::clone(makima_agent::tools::ToolRegistry::global_arc()),
                registry.clone(),
                false,
                makima_lua::ui_sink::HostUi::Frontend,
            )
            .unwrap();
            host.event_handle()
                .install_orchestration_services(event_loop.orchestration.clone());
            host.load_source(
                "nested_pin_command",
                &format!(
                    r#"
                    makima.api.register_command({{
                        name = "/spawn-pin-child", tui_only = false,
                        handler = function(_, ctx)
                            assert(makima.agent.spawn(ctx, {{ inherit_provider = true, silent = true }}))
                            makima.ui.flash("{SPAWNED}")
                        end,
                    }})
                    makima.api.register_command({{
                        name = "/pin-outer", tui_only = false,
                        handler = function(_, ctx)
                            assert(makima.fn.winsaveview())
                            assert(makima.api.run_command(ctx, "/pin-inner"))
                        end,
                    }})
                    makima.api.register_command({{
                        name = "/pin-inner", tui_only = false,
                        handler = function(_, ctx)
                            local target = assert(ctx:agent())
                            assert(target:set_model(ctx, {{ thinking = "{PINNED_THINKING}" }}))
                            makima.ui.flash("{INNER_DONE}")
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
                .send(makima_lua::WinView {
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
        struct Probe(Arc<Mutex<Option<makima_agent::AgentId>>>);
        impl makima_commands::CommandBehavior for Probe {
            fn execute(
                &self,
                invocation: makima_commands::CommandInvocation,
            ) -> makima_commands::CommandFuture<Result<makima_commands::CommandOutcome, CommandError>>
            {
                *self.0.lock().unwrap() = Some(
                    invocation
                        .admission_context::<PinnedCommandTarget>()
                        .unwrap()
                        .target
                        .id(),
                );
                Box::pin(async { Ok(makima_commands::CommandOutcome::Completed) })
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
            makima_lua::orchestration::TrustedTarget {
                target: first_manager.lookup(first_id).unwrap(),
                template: first.handles.orchestration_template.clone(),
            },
        )]));
        let seen = Arc::new(Mutex::new(None));
        let producer = command_runtime
            .registry
            .create_producer(makima_commands::ProducerPrecedence::Application);
        producer
            .replace(vec![makima_commands::Registration {
                spec: makima_commands::CommandSpec {
                    name: Arc::from("/admission-race"),
                    aliases: Arc::from([]),
                    arguments: makima_commands::CommandArguments::Raw { required: false },
                    docs: makima_commands::CommandDocs {
                        summary: Arc::from("Admission race probe"),
                        argument_hint: None,
                    },
                    required_capabilities: makima_commands::TargetCapabilities::NONE,
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
            makima_lua::orchestration::TrustedTarget {
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
        let makima_commands::CompletionResult::Items(candidates) = smol::block_on(
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
        let old =
            replace_session_runtime(&mut runtime, prepared, &harness.ctx().model_slot).unwrap();

        assert_eq!(provider.current_theme_name(), ORIGINAL_THEME);
        release_runtime(old);
        release_runtime(runtime);
    }

    #[test]
    fn replacement_preserves_exit_on_done() {
        let harness = RuntimeHarness::new();
        let mut runtime = harness.runtime(harness.session());
        runtime.app.exit_on_done = true;
        let cancel = runtime.lifetime_cancel.clone();
        let prepared = harness.prepare();

        let old =
            replace_session_runtime(&mut runtime, prepared, &harness.ctx().model_slot).unwrap();

        assert!(runtime.app.exit_on_done);
        assert!(cancel.is_cancelled());
        assert!(!runtime.lifetime_cancel.is_cancelled());
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
            SessionLockState::InFlight(_) => {
                panic!("heartbeat was already in flight")
            }
        };
        apply_heartbeat_completion(
            &mut runtime,
            HeartbeatCompletion {
                result: Ok(session_lock::LockBeat::Lost),
                lease: Some(lease),
            },
        );

        assert!(runtime.lock_lost);
        assert!(runtime.lifetime_cancel.is_cancelled());
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
            SessionLockState::InFlight(_) => {
                panic!("heartbeat was already in flight")
            }
        };
        std::fs::write(&path, format!("{} replacement", std::process::id())).unwrap();
        runtime.lock_lost = true;
        session.truncate_messages(1);
        session.model = runtime.app.root_config().model.spec();
        let error = harness
            .ctx()
            .prepare_replacement_runtime(session, None, &runtime)
            .err()
            .expect("lost lease rejects preparation");
        assert_eq!(error, LOCK_LOST_REPLACEMENT_ERR);
        assert!(runtime.lock_lost);
        assert!(runtime.session_lock.is_none());
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
            .prepare_replacement_runtime(session, None, &runtime)
            .unwrap();
        let old =
            replace_session_runtime(&mut runtime, prepared, &harness.ctx().model_slot).unwrap();
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

        let error = harness
            .ctx()
            .prepare_replacement_runtime(session, None, &runtime)
            .err()
            .expect("in-flight lease rejects preparation");
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
        runtime.app.persist_input();

        let writer = runtime.checkpoint.clone();
        let manager = runtime.handles.manager_and_root().0;
        drop(runtime);
        shutdown_manager(&manager);
        let ctx = harness.ctx.take().unwrap();
        let storage = ctx.storage.clone();
        let sessions_dir = storage.path().join(SESSIONS_DIR);
        drop(ctx);
        assert!(
            session_lock::claim(&sessions_dir, &id).unwrap().is_none(),
            "lock released before storage drain"
        );
        smol::block_on(writer.drain()).ok();
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
        runtime.app.input_box.set_input(UNSAVED.into());
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

        let writer = runtime.checkpoint.clone();
        let manager = runtime.handles.manager_and_root().0;
        drop(runtime);
        shutdown_manager(&manager);
        let ctx = harness.ctx.take().unwrap();
        let storage = ctx.storage.clone();
        drop(ctx);
        smol::block_on(writer.drain()).ok();

        let stored = AppSession::load(id, &storage).unwrap();
        assert_eq!(stored.messages().len(), 1);
        assert_eq!(stored.messages()[0].user_text(), Some(BASELINE));
        assert!(stored.meta.input_draft.is_none());
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
        runtime.app.input_box.set_input(UNSAVED.into());
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

        let writer = runtime.checkpoint.clone();
        let manager = runtime.handles.manager_and_root().0;
        drop(runtime);
        shutdown_manager(&manager);
        let ctx = harness.ctx.take().unwrap();
        let storage = ctx.storage.clone();
        drop(ctx);
        smol::block_on(writer.drain()).ok();

        let stored = AppSession::load(id, &storage).unwrap();
        assert_eq!(stored.messages().len(), 1);
        assert_eq!(stored.messages()[0].user_text(), Some(BASELINE));
        assert!(stored.meta.input_draft.is_none());
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
        let prepared = harness
            .ctx()
            .prepare_replacement_runtime(session, None, &runtime)
            .unwrap();

        let old =
            replace_session_runtime(&mut runtime, prepared, &harness.ctx().model_slot).unwrap();

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

        let mut old =
            replace_session_runtime(&mut runtime, prepared, &harness.ctx().model_slot).unwrap();

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
        let runtime = harness.runtime(harness.session());
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
        let error = harness
            .ctx()
            .prepare_runtime(target)
            .err()
            .expect("foreign target lock must reject preparation")
            .to_string();
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
        let now = activate_middle_scroll(&mut outgoing);
        route_terminal_lifecycle(&mut outgoing, &event);
        route_terminal_lifecycle(&mut outgoing, &Event::FocusGained);
        let _ = tick_session(&mut outgoing, true, now + MIDDLE_SCROLL_STEP);
        assert_eq!(outgoing.main_chat().scroll_top(), MIDDLE_SCROLL_START);
        assert_eq!(outgoing.cancel_middle_scroll(), Dirty::NO);

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
        assert_eq!(outgoing.cancel_middle_scroll(), Dirty::NO);

        let now = activate_middle_scroll(&mut outgoing);
        activate_middle_scroll(&mut incoming);
        let mut terminal_focused = true;
        prepare_terminal_handoff([&mut outgoing, &mut incoming], &mut terminal_focused);
        assert!(!terminal_focused);
        for app in [&mut outgoing, &mut incoming] {
            route_terminal_lifecycle(app, &Event::FocusGained);
            let _ = tick_session(app, true, now + MIDDLE_SCROLL_STEP);
            assert_eq!(app.main_chat().scroll_top(), MIDDLE_SCROLL_START);
            assert_eq!(app.cancel_middle_scroll(), Dirty::NO);
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
    fn live_session_response_counts_committed_root_history() {
        with_event_loop_runtime(
            |harness| {
                let mut session = harness.session();
                session.push_message(Message::user(STARTUP_PROMPT.into()));
                harness.runtime(session)
            },
            |event_loop| {
                let runtime = &mut event_loop.sessions[0];
                let row = live_session_row(&runtime.app, true);
                assert_eq!(row["message_count"], 1);
                assert_eq!(row["focused"], true);
                assert_eq!(row["status"], "idle");

                runtime
                    .handles
                    .history
                    .store(Arc::new(HistorySnapshot::new(vec![
                        Message::user(STARTUP_PROMPT.into()),
                        Message::observation(OBSERVATION.into()),
                    ])));
                assert_eq!(live_session_row(&runtime.app, true)["message_count"], 1);
            },
        );
    }

    struct FakeHost;

    impl makima_commands::CommandHost for FakeHost {
        fn request(
            &self,
            _request: makima_commands::HostRequest,
        ) -> makima_commands::CommandFuture<Result<HostResponse, CommandError>> {
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
        let registry = makima_commands::CommandRegistry::new();
        let retired = registry
            .bind_target(
                makima_commands::TargetCapabilities::default(),
                Arc::new(FakeHost),
            )
            .id();
        let live = registry
            .bind_target(
                makima_commands::TargetCapabilities::default(),
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

    #[test_case("high", true ; "enabled_options")]
    #[test_case("off", false ; "explicit_disabled_options")]
    fn session_model_picker_commits_provider_and_options_together(thinking: &str, fast: bool) {
        use crate::components::model_picker::ModelPicker;
        use crate::model_capabilities::test_support::seeded;
        use makima_providers::{RequestOptions, ThinkingSupport};

        const MODEL: &str = "anthropic/picker-committed-model";
        const PROMPT: &str = "record picker selection";

        struct RecordingProvider(flume::Sender<(String, RequestOptions)>);

        impl Provider for RecordingProvider {
            fn stream_message<'a>(
                &'a self,
                model: &'a Model,
                _messages: &'a [Message],
                _system: &'a str,
                _tools: &'a serde_json::Value,
                _event_tx: &'a flume::Sender<makima_providers::ProviderEvent>,
                options: RequestOptions,
                _session_id: Option<&'a SessionRef>,
                _invocation: &'a makima_providers::Invocation,
                _delivery: makima_providers::RequestDelivery,
            ) -> makima_providers::provider::BoxFuture<
                'a,
                Result<makima_providers::StreamResponse, makima_providers::AgentError>,
            > {
                Box::pin(async move {
                    self.0.send((model.spec(), options)).unwrap();
                    std::future::pending().await
                })
            }
        }

        fn adjusted(mut model: Model) -> Model {
            model.thinking_support = Some(ThinkingSupport::Yes);
            model.fast_support = Some(FastSupport::Supported);
            model
        }

        fn finish_selection(event_loop: &mut EventLoop<'_>) {
            loop {
                let event = event_loop
                    .internal_rx
                    .recv_timeout(SESSION_OP_TIMEOUT)
                    .unwrap();
                let done = match &event {
                    InternalEvent::SessionOp {
                        result: Err(error), ..
                    } => panic!("picker selection failed: {error}"),
                    InternalEvent::SessionOp {
                        kind: SessionOpKind::ProviderRefreshed,
                        ..
                    } => true,
                    _ => false,
                };
                event_loop.handle_internal(event);
                if done {
                    return;
                }
            }
        }

        with_event_loop(|event_loop| {
            let (requests, received) = flume::unbounded();
            let provider: Arc<dyn Provider> = Arc::new(RecordingProvider(requests));
            event_loop.ctx.prepare_provider = sync_preparer(move |model| {
                Ok(PreparedModel {
                    model: adjusted(model),
                    provider: Arc::clone(&provider),
                })
            });
            let (manager, root) = event_loop.sessions[0].handles.manager_and_root();
            let actor = manager.actor(root).unwrap();
            let before = actor.config_snapshot().unwrap();
            event_loop
                .ctx
                .catalog
                .specs()
                .store(Some(Arc::new(vec![MODEL.into()])));
            event_loop.sync_orchestration();
            event_loop.sync_model_discovery();
            let app = &mut event_loop.sessions[0].app;
            app.model_picker = ModelPicker::new(
                Arc::clone(&app.available_models),
                Arc::new(makima_providers::test_util::offline_ids(&["anthropic"])),
                seeded(
                    &[(
                        MODEL,
                        ModelCapabilities::from(&adjusted(Model::from_spec(MODEL).unwrap())),
                    )],
                    &[],
                ),
            );
            app.run_builtin(BuiltinAction::ModelPicker);
            app.update(Msg::Paste(MODEL.split_once('/').unwrap().1.into()));
            assert_eq!(app.model_picker.highlighted_spec(), Some(MODEL));
            app.update(Msg::Key(crate::components::key(KeyCode::Tab)));
            app.update(Msg::Paste(thinking.into()));
            app.update(Msg::Key(crate::components::key(KeyCode::Tab)));
            app.update(Msg::Key(crate::components::key(KeyCode::Right)));
            if !fast {
                app.update(Msg::Key(crate::components::key(KeyCode::Right)));
            }
            event_loop.sync_orchestration();
            let unchanged = actor.config_snapshot().unwrap();
            assert_eq!(unchanged.generation, before.generation);

            let actions = event_loop.sessions[0]
                .app
                .update(Msg::Key(crate::components::key(KeyCode::Enter)));
            let expected_thinking = thinking.parse::<DomainThinkingConfig>().unwrap();
            assert!(
                matches!(actions.as_slice(), [Action::SelectPinnedModel { target, selection }]
                if target.target.id() == root && selection.spec.as_deref() == Some(MODEL)
                    && selection.thinking == Some(expected_thinking) && selection.fast == Some(fast))
            );
            event_loop.dispatch(0, actions);
            finish_selection(event_loop);
            drain_config_commits(&mut event_loop.sessions[0]);
            let after = actor.config_snapshot().unwrap();
            assert_eq!(after.generation, before.generation + 1);
            let runtime = &event_loop.sessions[0];
            let tracked = Arc::clone(&runtime.model_slot.load().provider) as Arc<dyn Provider>;
            assert!(Arc::ptr_eq(&after.config.provider, &tracked));
            let view = runtime.app.state.projection.fold().unwrap();
            let projected = view.scopes[&Some(root)].config.as_ref().unwrap();
            assert_eq!(runtime.app.state.projection.model(), MODEL);
            assert_eq!(projected.model, MODEL);
            let stored = AppSession::load(runtime.id(), &event_loop.ctx.storage).unwrap();
            let stored_view = stored.latest_state().unwrap();
            assert_eq!(
                stored_view.scopes[&Some(root)].config.as_ref(),
                Some(projected)
            );
            assert_eq!(after.config.model.spec(), MODEL);
            let expected = RequestOptions {
                thinking: expected_thinking,
                fast,
            }
            .clamped(&after.config.model);
            assert_eq!(projected.thinking, Some(expected.thinking.into()));
            assert_eq!(projected.fast, expected.fast);
            assert_eq!(after.config.thinking, expected.thinking);
            assert_eq!(after.config.fast, expected.fast);

            event_loop.submit_text(0, PROMPT.into()).unwrap();
            assert_eq!(
                received.recv_timeout(SESSION_OP_TIMEOUT).unwrap(),
                (MODEL.into(), expected)
            );
        });
    }
}
