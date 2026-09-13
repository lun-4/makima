//! Multi-session supervisor: every session owns an `App` + `AgentHandles` and
//! keeps draining agent events while backgrounded; only the focused session
//! renders and receives input. `SpawnCtx` carries the shared resources needed
//! to spawn session runtimes at any point.
//!
//! Terminal input arrives on a channel (see [`InputReader`]), so the loop
//! waits on every event source at once and wakes the moment a plugin action,
//! agent event, or keypress arrives instead of sleeping in `event::poll`.

use std::collections::HashMap;
use std::io;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use arc_swap::ArcSwapOption;
use color_eyre::Result;
use color_eyre::eyre::{Context, eyre};

use crossterm::event::{
    Event, KeyEventKind, MouseButton, MouseEvent as CtMouseEvent, MouseEventKind,
};
use maki_agent::command::CustomCommand;
#[cfg(test)]
use maki_agent::permissions::PermissionAnswer;
use maki_agent::permissions::PermissionManager;
use maki_agent::{
    AgentConfig, AgentEvent, CancelToken, Envelope, McpCommand, McpConfigErrors, McpHandle, mcp,
};
use maki_config::{ModelPolicy, UiConfig};
use maki_domain::ThinkingConfig as DomainThinkingConfig;
use maki_lua::{
    EventHandle, HintReader, KeymapReader, ModelRequest, ProviderUsageAck,
    ProviderUsageInvalidation, ProviderUsageLimit, ProviderUsageReply, ProviderUsageSnapshot,
    ProviderUsageWindow, SessionRequest, StatusContentReader, UiAction, UiReply,
};
use maki_providers::Timeouts;
use maki_providers::provider::{Provider, fetch_all_models, from_model};
use maki_providers::{Message, Model, TokenUsage};
use maki_storage::StateDir;
use maki_storage::StorageError;
use maki_storage::id::{MakiId, MakiIdParseError, SessionRef};
use maki_storage::session_lock::{self, ClaimedSessionLock};
use maki_storage::sessions::{
    Prefs, SESSIONS_DIR, SessionError, StoredTokenUsage, normalize_title, write_prefs,
};
use serde_json::json;
use tracing::{info, warn};

fn claim_lock(dir: &std::path::Path, id: &MakiId) -> Result<ClaimedSessionLock> {
    session_lock::claim(dir, id)?.ok_or_else(|| eyre!(session_lock::OPEN_ELSEWHERE_MSG))
}

use crate::AppSession;
use crate::agent::{
    AgentCommand, AgentHandles, PreparedAgentHandles, ProviderChange, ProviderSlot,
    SystemPromptOverride, shared_queue::QueueItem,
};
use crate::app::shell::{ShellEvent, spawn_shell};
use crate::app::{
    App, Msg, Notification, PreparedApp, QueuedMessage, SubmitOutcome, session_has_content,
    turn_response,
};
use crate::color_compat;
use crate::command_runtime::{CommandEvent, CommandRuntime};
use crate::components::arg_completion::{ModelArgSource, ThemeArgSource};
use crate::components::input::Submission;
use crate::components::{
    Action, ExitRequest, ReplacementPostCommit, SessionReplacementKind, SessionReplacementRequest,
    Status,
};
use crate::input::InputReader;
use crate::provider_usage::{
    ProviderIdentity, ProviderUsageCoordinator, ProviderUsageFetch, ProviderUsageFetchId,
    ProviderUsageFetchResult, ProviderUsageInput, ProviderUsageOutput, ProviderUsageRequestKind,
    ProviderUsageResolution,
};
use crate::repaint::{Dirty, IDLE_POLL};

use crate::storage_writer::StorageWriter;
use crate::terminal;
use crate::theme::ThemesProvider;

/// Max events handled per frame so a flood cannot starve rendering.
const DRAIN_BUDGET: usize = 256;
const AGENT_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(3);
const DELETE_FOCUSED_ERR: &str = "cannot delete the focused session";
const MODEL_POLICY_ERR: &str = "Model is not allowed by policy";
const INVALID_MODEL_ERR: &str = "Invalid model";
const PROVIDER_INIT_ERR: &str = "Failed to create provider";
const PROVIDER_USAGE_CHANGED_ERR: &str = "provider changed while fetching usage";
const PROVIDER_USAGE_SHUTDOWN_ERR: &str = "UI shut down while fetching usage";
const NOT_LIVE_ERR: &str = "session not live";
const LOCK_LOST_REPLACEMENT_ERR: &str = "session lock was lost; replacement is disabled";
const LOCK_UNAVAILABLE_REPLACEMENT_ERR: &str =
    "session lock ownership is unavailable; replacement is disabled";
const REPLACEMENT_PENDING_ERR: &str =
    "session replacement is already waiting for lock verification";

static NEXT_RUNTIME_GENERATION: AtomicU64 = AtomicU64::new(1);

/// Tabs carry their in-memory sessions so `/reload` reopens them without a
/// disk round-trip; `session_has_content` tells which ones were saved.
pub(crate) struct ShutdownReport {
    pub exit: ExitRequest,
    pub tabs: Vec<AppSession>,
    pub focused: usize,
}

pub struct EventLoopParams {
    pub model: Model,
    pub needs_login: bool,
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

    /// True between `Done`/`Error` and the run's `QueueDrained`. An exit must
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
            Self::Working => "working",
            Self::NeedsInput => "needs_input",
            Self::Idle => "idle",
        }
    }
}

fn prepend_preamble(preamble: &mut Vec<Message>, mut leading: Vec<Message>) {
    leading.append(preamble);
    *preamble = leading;
}

fn is_current_top_level(current_run_id: u64, envelope: &Envelope) -> bool {
    envelope.run_id == current_run_id && envelope.subagent.is_none()
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
        let _ = app.cancel_middle_scroll();
    }
}

fn assign_session_focus(outgoing: &mut App, focused: &mut usize, next: usize) {
    if *focused != next {
        let _ = outgoing.cancel_middle_scroll();
        *focused = next;
    }
}

fn prepare_terminal_handoff<'a>(
    apps: impl IntoIterator<Item = &'a mut App>,
    terminal_focused: &mut bool,
) {
    for app in apps {
        let _ = app.cancel_middle_scroll();
    }
    *terminal_focused = false;
}

fn tick_session(app: &mut App, focused: bool, now: Instant) -> (Dirty, Vec<Action>) {
    let actions = app.poll_login_picker();
    let mut dirty = Dirty::NO;
    if focused {
        dirty |= app.tick_at(now);
    } else {
        let _ = app.float_mgr.tick();
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
    runtime.pending_replacement = None;
    runtime.lock_lost = true;
    runtime
        .app
        .flash("Session lock lost to another process; stopping without saving".into());
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

fn complete_runtime_heartbeat(runtime: &mut SessionRuntime) -> Option<PendingReplacement> {
    let Some(SessionLockState::InFlight(completion_rx)) = runtime.session_lock.take() else {
        return None;
    };
    let completion = collect_heartbeat(completion_rx, None)?;
    apply_heartbeat_completion(runtime, completion);
    if runtime.lock_lost {
        None
    } else {
        runtime.pending_replacement.take()
    }
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

enum LockReleaseOutcome {
    Completed {
        result: io::Result<()>,
        lock_lost: bool,
    },
    TimedOut,
}

fn release_lock_state(state: Option<SessionLockState>) -> io::Result<()> {
    match release_lock_state_with_timeout(state, AGENT_SHUTDOWN_TIMEOUT) {
        LockReleaseOutcome::Completed { result, .. } => result,
        LockReleaseOutcome::TimedOut => Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "session lock heartbeat did not finish before shutdown timeout",
        )),
    }
}

fn release_lock_state_with_timeout(
    state: Option<SessionLockState>,
    timeout: Duration,
) -> LockReleaseOutcome {
    let (lease, lock_lost) = match state {
        Some(SessionLockState::Held(lease)) => (Some(lease), false),
        Some(SessionLockState::InFlight(completion_rx)) => {
            let Some(mut completion) = collect_heartbeat(completion_rx, Some(timeout)) else {
                return LockReleaseOutcome::TimedOut;
            };
            let lock_lost = matches!(completion.result, Ok(session_lock::LockBeat::Lost));
            (completion.lease.take(), lock_lost)
        }
        None => (None, false),
    };
    LockReleaseOutcome::Completed {
        result: lease.map_or(Ok(()), ClaimedSessionLock::release),
        lock_lost,
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

struct SessionRuntime {
    generation: u64,
    app: App,
    handles: AgentHandles,
    shell_tx: flume::Sender<ShellEvent>,
    shell_rx: flume::Receiver<ShellEvent>,
    last_status: SessionStatus,
    notifications: RunNotificationState,
    session_lock: Option<SessionLockState>,
    lock_lost: bool,
    restore_pending: bool,
    pending_replacement: Option<PendingReplacement>,
}

struct PendingReplacement {
    prepared: PreparedSessionRuntime,
    kind: SessionReplacementKind,
    post_commit: Option<ReplacementPostCommit>,
}

struct PreparedProvider {
    model: Model,
    provider: Arc<dyn Provider>,
}

struct PreparedSessionRuntime {
    app: PreparedApp,
    handles: PreparedAgentHandles,
    shell_tx: flume::Sender<ShellEvent>,
    shell_rx: flume::Receiver<ShellEvent>,
    resumed: bool,
    provider: Option<PreparedProvider>,
}

impl PreparedSessionRuntime {
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
        model_slot: &ProviderSlot,
        session_lock: Option<SessionLockState>,
    ) -> SessionRuntime {
        let Self {
            app,
            handles,
            shell_tx,
            shell_rx,
            resumed,
            provider,
        } = self;
        if let Some(provider) = provider {
            model_slot.install(provider.model, provider.provider);
        }
        let handles = handles.activate();
        let mut app = app.activate();
        handles.apply_to_app(&mut app);
        SessionRuntime {
            generation: NEXT_RUNTIME_GENERATION.fetch_add(1, Ordering::Relaxed),
            app,
            handles,
            shell_tx,
            shell_rx,
            last_status: SessionStatus::Idle,
            notifications: RunNotificationState::default(),
            session_lock,
            lock_lost: false,
            restore_pending: resumed,
            pending_replacement: None,
        }
    }
}

fn defer_replacement_for_heartbeat(
    runtime: &mut SessionRuntime,
    pending: PendingReplacement,
) -> Option<PendingReplacement> {
    let same_id = runtime.id() == pending.prepared.app.session_id();
    if same_id && matches!(runtime.session_lock, Some(SessionLockState::InFlight(_))) {
        if runtime.pending_replacement.is_none() {
            runtime.pending_replacement = Some(pending);
        } else {
            runtime.app.flash(REPLACEMENT_PENDING_ERR.into());
        }
        None
    } else {
        Some(pending)
    }
}

fn replace_session_runtime(
    current: &mut SessionRuntime,
    prepared: PreparedSessionRuntime,
    sessions_dir: &std::path::Path,
    model_slot: &ProviderSlot,
) -> Result<SessionRuntime, String> {
    let target_id = prepared.app.session_id();
    let exit_on_done = current.app.exit_on_done;
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
    let mut runtime = prepared.activate(model_slot, target_lock);
    runtime.app.exit_on_done = exit_on_done;
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

impl SessionRuntime {
    fn id(&self) -> MakiId {
        self.app.state.session.id
    }

    fn update(&mut self, msg: Msg) -> Vec<Action> {
        if self.pending_replacement.is_some() && matches!(msg, Msg::Key(_) | Msg::Paste(_)) {
            return Vec::new();
        }
        self.app.update(msg)
    }

    fn submit_text(&mut self, text: String) -> Result<SubmitOutcome, String> {
        if self.pending_replacement.is_some() {
            return Err(REPLACEMENT_PENDING_ERR.into());
        }
        Ok(self.app.submit_prompt(QueuedMessage {
            text,
            images: Vec::new(),
        }))
    }

    fn activate_deferred(&mut self) {
        if std::mem::take(&mut self.restore_pending) {
            self.app.restore_resumed_session();
        }
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
    available_models: Arc<ArcSwapOption<Vec<String>>>,
    storage_writer: Arc<StorageWriter>,
    model_policy: Arc<ModelPolicy>,
    system_prompt: SystemPromptOverride,
    command_runtime: Arc<CommandRuntime>,
}

impl SpawnCtx {
    fn prepare_runtime(&self, session: AppSession) -> PreparedSessionRuntime {
        self.prepare_runtime_with_provider(session, None)
    }

    fn prepare_replacement_runtime(
        &self,
        session: AppSession,
        permissions: &PermissionManager,
    ) -> Result<PreparedSessionRuntime, String> {
        let provider = self.prepare_replacement_provider(&session)?;
        Ok(self.prepare_runtime_with_provider_and_permissions(session, provider, permissions))
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
        let mut model = Model::from_spec(model_spec).map_err(|error| error.to_string())?;
        let provider = from_model(&mut model, self.timeouts).map_err(|error| error.to_string())?;
        Ok(Some(PreparedProvider {
            model,
            provider: Arc::from(provider),
        }))
    }

    fn prepare_runtime_with_provider(
        &self,
        session: AppSession,
        provider: Option<PreparedProvider>,
    ) -> PreparedSessionRuntime {
        self.prepare_runtime_with_provider_and_permissions(session, provider, &self.permissions)
    }

    fn prepare_runtime_with_provider_and_permissions(
        &self,
        session: AppSession,
        provider: Option<PreparedProvider>,
        permissions: &PermissionManager,
    ) -> PreparedSessionRuntime {
        let resumed = session_has_content(&session);
        let model = provider
            .as_ref()
            .map(|provider| &provider.model)
            .unwrap_or(&self.model_slot.load().model)
            .clone();
        let permissions = Arc::new(permissions.fork());
        let handles = AgentHandles::prepare(
            &self.model_slot,
            session.messages().to_vec(),
            self.config.clone(),
            self.ui_config.tool_output_lines,
            &permissions,
            Some(SessionRef::from(session.id)),
            self.timeouts,
            self.lua_event_handle.clone(),
            self.mcp_handle.clone(),
            self.mcp_config_errors.clone(),
            Arc::clone(&self.model_policy),
            self.system_prompt.clone(),
        );
        let app = App::prepare(
            &model,
            session,
            self.storage.clone(),
            Arc::clone(&self.available_models),
            handles.mcp_reader(),
            self.mcp_config_errors.clone(),
            self.keymap_reader.clone(),
            self.hint_reader.clone(),
            self.status_content_reader.clone(),
            Arc::clone(&self.storage_writer),
            self.ui_config.clone(),
            self.input_history_size,
            permissions,
            self.lua_event_handle.clone(),
            Arc::clone(&self.model_policy),
            crate::theme::default_provider().clone(),
            Arc::clone(&self.command_runtime),
        );
        let (shell_tx, shell_rx) = flume::unbounded::<ShellEvent>();
        PreparedSessionRuntime {
            app,
            handles,
            shell_tx,
            shell_rx,
            resumed,
            provider,
        }
    }

    fn spawn_runtime(&self, session: AppSession) -> Result<SessionRuntime> {
        let id = session.id;
        let prepared = self.prepare_runtime(session);
        let session_lock = claim_lock(&self.sessions_dir, &id)?;
        Ok(prepared.activate(&self.model_slot, Some(SessionLockState::Held(session_lock))))
    }
}

enum InternalEvent {
    ModelCandidate {
        requested_spec: String,
        expected_provider: ProviderIdentity,
        model: Model,
        provider: Arc<dyn Provider>,
    },
    ProviderUsageFetched {
        fetch_id: ProviderUsageFetchId,
        provider: ProviderIdentity,
        result: ProviderUsageFetchResult,
    },
    SessionHeartbeat(u64),
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
    _model_fetch_task: smol::Task<()>,
}

/// One item from any of the event loop's sources; `None` from `next_wake`
/// means the wait timed out (animation/idle tick).
enum Wake {
    Input(Event),
    InputGone,
    Ui(UiAction),
    Agent(usize, Box<maki_agent::Envelope>),
    Shell(usize, ShellEvent),
    Warn(String),
    Command(CommandEvent),
    ProviderChanged(ProviderChange),
    Internal(InternalEvent),
}

struct BackgroundModels {
    available: Arc<ArcSwapOption<Vec<String>>>,
    warn_rx: flume::Receiver<String>,
    warn_tx: flume::Sender<String>,
    task: smol::Task<()>,
}

fn merge_batch(
    available: &Arc<ArcSwapOption<Vec<String>>>,
    batch: maki_providers::provider::ModelBatch,
    warn_tx: &flume::Sender<String>,
) {
    for w in batch.warnings {
        let _ = warn_tx.try_send(w);
    }
    if batch.models.is_empty() {
        return;
    }
    let mut merged = available.load().as_deref().cloned().unwrap_or_default();
    for spec in &batch.models {
        if !merged.contains(spec) {
            merged.push(spec.clone());
        }
    }
    available.store(Some(Arc::new(merged)));
}

fn spawn_model_fetch(
    requested_spec: String,
    expected_provider: ProviderIdentity,
    internal_tx: flume::Sender<InternalEvent>,
    timeouts: Timeouts,
    policy: Arc<ModelPolicy>,
) -> BackgroundModels {
    let available: Arc<ArcSwapOption<Vec<String>>> = Arc::new(ArcSwapOption::empty());
    let bg = Arc::clone(&available);
    let (warn_tx, warn_rx) = flume::unbounded::<String>();
    let warn_tx_bg = warn_tx.clone();
    let task = smol::spawn(async move {
        let warn_tx = warn_tx_bg;
        let done = Box::new(move || {
            let mut resolved = match Model::from_spec(&requested_spec) {
                Ok(model) => model,
                Err(error) => {
                    warn!(spec = %requested_spec, %error, "failed to resolve model after discovery");
                    return;
                }
            };
            let provider = match from_model(&mut resolved, timeouts) {
                Ok(provider) => Arc::from(provider),
                Err(error) => {
                    warn!(spec = %requested_spec, %error, "failed to create provider after discovery");
                    return;
                }
            };
            let _ = internal_tx.send(InternalEvent::ModelCandidate {
                requested_spec,
                expected_provider,
                model: resolved,
                provider,
            });
        });
        fetch_all_models(
            &policy,
            |batch| merge_batch(&bg, batch, &warn_tx),
            Some(done),
        )
        .await;
    });
    BackgroundModels {
        available,
        warn_rx,
        warn_tx,
        task,
    }
}

impl<'t> EventLoop<'t> {
    pub(crate) fn new(
        terminal: &'t mut ratatui::DefaultTerminal,
        params: EventLoopParams,
    ) -> Result<Self> {
        let EventLoopParams {
            mut model,
            needs_login,
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
            std::thread::spawn(crate::highlight::warmup);
            crate::update::spawn_check();
        });

        let cwd = std::env::current_dir().unwrap_or_else(|_| ".".into());

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
        let bg = spawn_model_fetch(
            model.spec(),
            initial_provider,
            internal_tx.clone(),
            timeouts,
            Arc::clone(&model_policy),
        );
        let storage_writer = Arc::new(StorageWriter::new(storage.clone(), bg.warn_tx.clone()));
        let sessions_dir = storage.ensure_subdir(SESSIONS_DIR)?;

        let notifier = terminal::TerminalNotifier::new(ui_config.notifications);
        let model_completion = Arc::new(ModelArgSource::new(Arc::clone(&bg.available)));
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
        let (mcp_handle, mcp_config_errors) = smol::block_on(mcp::start(&cwd));
        let ctx = SpawnCtx {
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
            available_models: bg.available,
            storage_writer,
            model_policy,
            system_prompt: SystemPromptOverride {
                override_text: system_prompt_override,
                append_text: append_system_prompt,
            },
            command_runtime,
        };

        let mut runtimes = Vec::with_capacity(sessions.len());
        for session in sessions {
            match ctx.spawn_runtime(session) {
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
        for w in startup_warnings {
            app.flash(w);
        }

        let session_cwd = runtimes[focused].app.state.session.cwd.clone();
        Ok(Self {
            terminal,
            sessions: runtimes,
            focused,
            session_picker,
            last_focused: None,
            terminal_focused: false,
            notifier,
            ctx,
            sessions_dir,
            session_cwd,
            last_heartbeat: Instant::now(),
            input: InputReader::spawn(),
            warn_rx: bg.warn_rx,
            warn_tx: bg.warn_tx,
            ui_action_rx,
            command_rx,
            provider_change_rx,
            provider_usage: ProviderUsageCoordinator::new(initial_provider),
            next_status_invalidation: 0,
            pending_status_invalidation: None,
            internal_tx,
            internal_rx,
            _model_fetch_task: bg.task,
        })
    }

    fn focused_app(&mut self) -> &mut App {
        &mut self.sessions[self.focused].app
    }

    pub(crate) fn run(mut self, initial_prompt: Option<String>) -> Result<ShutdownReport> {
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
        // The first frame always paints. After that only a poller, an event or
        // an animation tick owes another.
        let mut dirty = Dirty::YES;
        let result = loop {
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
            Wake::Ui(action) => self.handle_ui_action(action),
            Wake::Agent(i, envelope) => self.handle_agent(i, envelope),
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
            InternalEvent::ModelCandidate {
                requested_spec,
                expected_provider,
                model,
                provider,
            } => {
                let current = self.ctx.model_slot.load();
                let still_current = current.model.spec() == requested_spec
                    && current.provider.identity() == expected_provider;
                drop(current);
                if still_current {
                    self.ctx.model_slot.install(model, provider);
                }
            }
            InternalEvent::ProviderUsageFetched {
                fetch_id,
                provider,
                result,
            } => {
                let current = self.ctx.model_slot.load().provider.identity();
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
                let pending = complete_runtime_heartbeat(&mut self.sessions[idx]);
                if let Some(pending) = pending {
                    self.commit_replacement(idx, pending);
                }
            }
        }
    }

    fn handle_provider_change(&mut self, _change: ProviderChange) {
        let provider = self.ctx.model_slot.load().provider.identity();
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
        let current = self.ctx.model_slot.load();
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
                let _ = reply.send(Err(maki_commands::CommandError::StaleTarget));
            }
            return;
        };
        match event {
            CommandEvent::Host { request, reply, .. } => {
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
            CommandEvent::Outcome { outcome, .. } => match outcome {
                maki_commands::CommandOutcome::AgentTurn(turn) => {
                    let actions = self.sessions[index].app.submit_command_turn(turn);
                    self.dispatch(index, actions);
                }
                maki_commands::CommandOutcome::Failed(error) => {
                    self.sessions[index].app.flash(error.to_string());
                }
                maki_commands::CommandOutcome::Completed => {}
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
        for (i, rt) in self.sessions.iter_mut().enumerate() {
            let (session_dirty, actions) = tick_session(&mut rt.app, i == self.focused, now);
            dirty |= session_dirty;
            if !actions.is_empty() {
                login_actions.push((i, actions));
            }
        }
        for (i, actions) in login_actions {
            self.dispatch(i, actions);
        }
        dirty
    }

    fn handle_agent(&mut self, idx: usize, envelope: Box<maki_agent::Envelope>) {
        let rt = &mut self.sessions[idx];
        let current = is_current_top_level(rt.app.run_id, &envelope);
        match &envelope.event {
            AgentEvent::QueueDrained => {
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

        let slot_model = self.ctx.model_slot.load();
        let spec = slot_model.model.spec();
        for rt in &mut self.sessions {
            if rt.app.state.session.model != spec
                || rt.app.state.model.context_window != slot_model.model.context_window
            {
                rt.app.update_model(&slot_model.model);
                dirty = Dirty::YES;
            }
        }
        drop(slot_model);

        // These two only fire Lua autocmds. Anything a handler does comes back
        // as a `UiAction` on the next wake, which repaints then.
        self.emit_focus_change();
        dirty |= self.start_mailbox_runs();
        self.emit_status_changes();
        self.emit_notifications();
        // An `exit_on_done` exit waits on `QueueDrained`; a dead agent loop
        // can never send it, so fail instead of hanging forever.
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

    fn handle_ui_action(&mut self, action: UiAction) {
        match action {
            UiAction::SetMode { id } => {
                self.focused_app().set_mode_id(id);
            }
            UiAction::GetMode { reply_tx } => {
                let id = self.focused_app().state.mode.id_key();
                let _ = reply_tx.try_send(id);
            }
            UiAction::Flash(msg) => {
                self.focused_app().flash(msg);
            }
            UiAction::OpenEditor { path, reply_tx } => {
                let code = self.open_editor(self.focused, &path);
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
            UiAction::Model { req, reply_tx } => {
                let _ = reply_tx.send(self.handle_model_request(req));
            }
            UiAction::ProviderUsageAck(ack) => {
                self.handle_provider_usage_ack(ack);
            }
            UiAction::UsageFetch { force, reply_tx } => {
                let provider = self.ctx.model_slot.load().provider.identity();
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
            UiAction::RunCommand {
                cmdline,
                depth,
                reply_tx,
            } => {
                // Answer before dispatching: the caller only waits on the name
                // resolving, and dispatch may take a while (or exit the app).
                match self.focused_app().run_cmdline(&cmdline, depth) {
                    Ok(actions) => {
                        let _ = reply_tx.send(Ok(()));
                        self.dispatch(self.focused, actions);
                    }
                    Err(e) => {
                        let _ = reply_tx.send(Err(e));
                    }
                }
            }
        }
    }

    fn handle_provider_usage_ack(&mut self, ack: ProviderUsageAck) {
        if self.pending_status_invalidation != Some(ack.invalidation) {
            return;
        }
        let generations = self
            .sessions
            .iter_mut()
            .map(|runtime| runtime.app.reconcile_status_content())
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
        let current = self.ctx.model_slot.load();
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
        let current = self.ctx.model_slot.load();
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
        let current = self.ctx.model_slot.load();
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

    /// Exits with the editor's status code; `-1` (flashed on the session's
    /// app) when the editor could not be launched.
    fn open_editor(&mut self, idx: usize, path: &std::path::Path) -> i32 {
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
            // Deletes run on the storage writer thread after any queued
            // flushes, so the loop never blocks on disk and a queued save
            // cannot resurrect the files.
            SessionRequest::Delete { id } => {
                let id = match parse_session_id(&id) {
                    Ok(id) => id,
                    Err(e) => {
                        let _ = reply_tx.send(Err(e));
                        return;
                    }
                };
                if let Some(i) = self.position(id) {
                    if i == self.focused {
                        let _ = reply_tx.send(Err(DELETE_FOCUSED_ERR.into()));
                        return;
                    }
                    let rt = self.remove_runtime(i);
                    rt.handles.shutdown().detach();
                }
                self.ctx.storage_writer.delete(id, move |res| {
                    let reply = match res {
                        Ok(()) | Err(SessionError::Storage(StorageError::NotFound(_))) => {
                            Ok(json!(true))
                        }
                        Err(e) => Err(e.to_string()),
                    };
                    let _ = reply_tx.send(reply);
                });
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
            SessionRequest::Current => {
                let _ = reply_tx.send(Ok(json!(self.sessions[self.focused].id())));
            }
            SessionRequest::Usage => {
                let app = &self.sessions[self.focused].app;
                let reply = session_usage(
                    &app.state.token_usage,
                    app.state.cost,
                    app.state.session.usage_by_model(),
                );
                let _ = reply_tx.send(Ok(reply));
            }
            SessionRequest::New { prompt, focus } => {
                let session = {
                    let slot = self.ctx.model_slot.load();
                    AppSession::new(&slot.model.spec(), &self.session_cwd)
                };
                let runtime = match self.ctx.spawn_runtime(session) {
                    Ok(runtime) => runtime,
                    Err(error) => {
                        let _ = reply_tx.send(Err(error.to_string()));
                        return;
                    }
                };
                let idx = self.push_runtime(runtime);
                let id = self.sessions[idx].id();
                if let Some(prompt) = prompt {
                    let _ = self.submit_text(idx, prompt);
                }
                if focus {
                    self.set_focused(idx);
                }
                let _ = reply_tx.send(Ok(json!(id)));
            }
            SessionRequest::Prompt { id, text } => {
                let idx = match id {
                    None => Ok(self.focused),
                    Some(id) => parse_session_id(&id).and_then(|id| {
                        self.position(id)
                            .ok_or_else(|| format!("{NOT_LIVE_ERR}: {id}"))
                    }),
                };
                let _ = reply_tx.send(idx.and_then(|idx| self.submit_text(idx, text)));
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
            SessionRequest::GetThinking => {
                let app = &self.sessions[self.focused].app;
                let options = maki_domain::THINKING_OPTIONS.to_vec();
                let reply = Ok(json!({
                    "mode": app.state.thinking.to_string(),
                    "supports_thinking": app.state.model.supports_thinking(),
                    "options": options,
                }));
                let _ = reply_tx.send(reply);
            }
            SessionRequest::SetThinking {
                set_default,
                thinking,
            } => {
                let reply = (|| {
                    let idx = self.focused;
                    if !self.sessions[idx].app.state.model.supports_thinking() {
                        return Err("Thinking requires a model that supports it".into());
                    }
                    let parsed = thinking
                        .parse::<DomainThinkingConfig>()
                        .map_err(|e| e.to_string())?;
                    if set_default {
                        write_prefs(
                            &self.ctx.storage,
                            &Prefs {
                                default_thinking: Some(parsed.into()),
                            },
                        )
                        .map_err(|e| e.to_string())?;
                    }
                    self.sessions[idx].app.state.thinking = parsed;
                    let mode = self.sessions[idx].app.state.thinking.to_string();
                    self.sessions[idx].app.flash(format!("Thinking: {mode}"));
                    Ok(json!({ "mode": mode }))
                })();
                let _ = reply_tx.send(reply);
            }
        }
    }

    /// Lua acts on the focused session, the same target the model picker and
    /// `/thinking` write to.
    fn handle_model_request(&mut self, req: ModelRequest) -> UiReply {
        match req {
            ModelRequest::Get => Ok(self.focused_app().model_state()),
            ModelRequest::Available => {
                let available = self.ctx.available_models.load();
                Ok(json!(
                    available.as_deref().map(Vec::as_slice).unwrap_or(&[])
                ))
            }
            ModelRequest::Set {
                spec,
                thinking,
                fast,
            } => {
                if let Some(spec) = spec {
                    self.change_model(self.focused, &spec)?;
                }
                let app = self.focused_app();
                if let Some(thinking) = thinking {
                    app.set_thinking(&thinking)?;
                }
                if let Some(fast) = fast {
                    app.set_fast(fast)?;
                }
                Ok(app.model_state())
            }
        }
    }

    fn submit_text(&mut self, idx: usize, text: String) -> UiReply {
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
        let prepared = self
            .ctx
            .prepare_replacement_runtime(session, self.sessions[idx].app.permissions.as_ref())?;
        self.replace_prepared_runtime(idx, prepared)
    }

    fn replace_prepared_runtime(
        &mut self,
        idx: usize,
        prepared: PreparedSessionRuntime,
    ) -> Result<(), String> {
        let old = replace_session_runtime(
            &mut self.sessions[idx],
            prepared,
            &self.sessions_dir,
            &self.ctx.model_slot,
        )?;
        let SessionRuntime {
            app,
            handles,
            session_lock,
            ..
        } = old;
        if let Err(error) = release_lock_state(session_lock) {
            warn!(%error, "old session lock release failed");
        }
        drop(app);
        handles.shutdown().detach();
        Ok(())
    }

    fn prepare_replacement(
        &self,
        idx: usize,
        request: SessionReplacementRequest,
    ) -> Result<PendingReplacement, String> {
        let SessionReplacementRequest {
            session,
            kind,
            post_commit,
        } = request;
        let prepared = self
            .ctx
            .prepare_replacement_runtime(session, self.sessions[idx].app.permissions.as_ref())?;
        Ok(PendingReplacement {
            prepared,
            kind,
            post_commit,
        })
    }

    fn commit_replacement(&mut self, idx: usize, pending: PendingReplacement) {
        let PendingReplacement {
            prepared,
            kind,
            post_commit,
        } = pending;
        match self.replace_prepared_runtime(idx, prepared) {
            Ok(()) => {
                if let SessionReplacementKind::Reset { ended_id } = kind {
                    self.sessions[idx].app.lua_event_handle.fire_autocmd(
                        "SessionReset",
                        serde_json::json!({ "session_id": ended_id }),
                    );
                }
                if let Some(post_commit) = post_commit {
                    let actions = self.sessions[idx]
                        .app
                        .apply_replacement_post_commit(post_commit);
                    self.dispatch(idx, actions);
                }
            }
            Err(error) => self.sessions[idx].app.flash(error),
        }
    }

    fn request_replacement(&mut self, idx: usize, request: SessionReplacementRequest) {
        let pending = match self.prepare_replacement(idx, request) {
            Ok(pending) => pending,
            Err(error) => {
                self.sessions[idx].app.flash(error);
                return;
            }
        };
        if let Some(pending) = defer_replacement_for_heartbeat(&mut self.sessions[idx], pending) {
            self.commit_replacement(idx, pending);
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
        if SessionStatus::of(&self.sessions[self.focused].app) == SessionStatus::Idle
            && !self.sessions[self.focused].app.has_content()
        {
            return self.replace_runtime(self.focused, session);
        }
        let runtime = self
            .ctx
            .spawn_runtime(session)
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
        match action {
            Action::SendMessage(input) => {
                let rt = &mut self.sessions[idx];
                rt.reset_run_notifications();
                let mut input = *input;
                prepend_preamble(&mut input.preamble, rt.app.shell.drain_results());
                let run_id = rt.app.run_id;
                rt.handles.queue.push(QueueItem::Message {
                    text: input.message.clone(),
                    image_count: input.images.len(),
                    input,
                    run_id,
                    displayed: true,
                });
            }
            Action::CancelAgent { run_id } => {
                let rt = &mut self.sessions[idx];
                rt.notifications.reset();
                let _ = rt.handles.cmd_tx.try_send(AgentCommand::Cancel { run_id });
            }
            Action::CancelSubagent { tool_use_id } => {
                let _ = self.sessions[idx]
                    .handles
                    .cmd_tx
                    .try_send(AgentCommand::CancelSubagent { tool_use_id });
            }
            Action::ReplaceSession(request) => self.request_replacement(idx, *request),
            Action::ChangeModel(spec) => {
                if let Err(error) = self.change_model(idx, &spec) {
                    self.sessions[idx].app.flash(error);
                }
            }
            Action::RefreshProvider { slug } => self.refresh_provider(slug),
            Action::AssignTier(spec, tier) => {
                maki_providers::model_registry::set_and_persist(spec, tier, &self.ctx.storage);
            }
            Action::UnassignTier(spec, tier) => {
                maki_providers::model_registry::unset_and_persist(&spec, tier, &self.ctx.storage);
            }
            Action::Compact => {
                let rt = &mut self.sessions[idx];
                rt.reset_run_notifications();
                let run_id = rt.app.run_id;
                rt.handles.queue.push(QueueItem::Compact { run_id });
            }
            Action::ToggleMcp(server_name, enabled) => {
                self.sessions[idx].handles.send_mcp(McpCommand::Toggle {
                    server: server_name,
                    enabled,
                });
            }
            Action::ShellCommand {
                id,
                command,
                visible,
            } => {
                let rt = &mut self.sessions[idx];
                let (trigger, cancel) = CancelToken::new();
                rt.app.shell.add_trigger(trigger);
                spawn_shell(
                    command,
                    id,
                    visible,
                    rt.shell_tx.clone(),
                    cancel,
                    self.ctx.config.clone(),
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
                let slot = self.ctx.model_slot.load();
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

    fn change_model(&mut self, idx: usize, spec: &str) -> Result<(), String> {
        if !self.ctx.model_policy.allows(spec) {
            return Err(format!("{MODEL_POLICY_ERR}: {spec}"));
        }
        let mut new_model =
            Model::from_spec(spec).map_err(|e| format!("{INVALID_MODEL_ERR}: {e}"))?;
        let new_provider = from_model(&mut new_model, self.ctx.timeouts)
            .map_err(|e| format!("{PROVIDER_INIT_ERR}: {e}"))?;
        let app = &mut self.sessions[idx].app;
        app.update_model(&new_model);
        app.record_recent_model(spec);
        self.ctx
            .model_slot
            .install(new_model, Arc::from(new_provider));
        Ok(())
    }

    fn refresh_models(&self) {
        let available = Arc::clone(&self.ctx.available_models);
        let warn_tx = self.warn_tx.clone();
        let policy = Arc::clone(&self.ctx.model_policy);
        available.store(None);
        smol::spawn(async move {
            fetch_all_models(
                &policy,
                |batch| merge_batch(&available, batch, &warn_tx),
                None,
            )
            .await;
        })
        .detach();
    }

    fn refresh_provider(&mut self, slug: String) {
        let mut model = self.ctx.model_slot.load().model.clone();
        if model.provider.to_string() == slug {
            if let Ok(provider) =
                maki_providers::provider::from_model(&mut model, self.ctx.timeouts)
            {
                self.ctx.model_slot.install(model, Arc::from(provider));
            }
        } else if let Some(builtin) = maki_config::providers::builtin_provider(&slug)
            && let Err(e) = self.change_model(self.focused, builtin.default_model)
        {
            self.focused_app().flash(e);
        }
    }

    fn shutdown(mut self) -> ShutdownReport {
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
        for rt in self.sessions.drain(..) {
            let SessionRuntime {
                mut app,
                handles,
                session_lock,
                lock_lost,
                ..
            } = rt;
            let heartbeat_timeout = heartbeat_deadline.saturating_duration_since(Instant::now());
            let lock_released_safely = match release_lock_state_with_timeout(
                session_lock,
                heartbeat_timeout,
            ) {
                LockReleaseOutcome::Completed {
                    result,
                    lock_lost: heartbeat_lock_lost,
                } => {
                    if let Err(error) = result {
                        warn!(id = %app.state.session.id, %error, "session lock release failed");
                    }
                    !heartbeat_lock_lost
                }
                LockReleaseOutcome::TimedOut => {
                    warn!(id = %app.state.session.id, "session lock heartbeat timed out during shutdown");
                    false
                }
            };
            if !lock_lost && lock_released_safely {
                app.checkpoint_now();
            }
            // `app` drops at the end of this iteration, closing the
            // channels the agent loop waits on, so `join_all` can finish.
            tabs.push(Arc::unwrap_or_clone(app.state.session));
            agent_tasks.push(handles.shutdown());
        }
        let save_sessions_ms = lap();
        crate::agent::join_all(agent_tasks, AGENT_SHUTDOWN_TIMEOUT);
        let join_agents_ms = lap();
        if let Some(ref h) = self.ctx.mcp_handle {
            smol::block_on(h.shutdown());
        }
        let mcp_shutdown_ms = lap();
        match Arc::try_unwrap(self.ctx.storage_writer) {
            Ok(writer) => writer.shutdown(AGENT_SHUTDOWN_TIMEOUT),
            Err(_) => {
                warn!("storage writer has outstanding references, skipping graceful shutdown")
            }
        }
        let storage_drain_ms = lap();
        info!(
            kill_mcp_ms,
            save_sessions_ms,
            join_agents_ms,
            mcp_shutdown_ms,
            storage_drain_ms,
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

fn session_usage(
    total: &TokenUsage,
    cost: Option<f64>,
    usage_by_model: &HashMap<String, StoredTokenUsage>,
) -> serde_json::Value {
    let mut models: Vec<_> = usage_by_model.iter().collect();
    models.sort_by(|(a_model, a_usage), (b_model, b_usage)| {
        b_usage
            .total()
            .cmp(&a_usage.total())
            .then_with(|| a_model.cmp(b_model))
    });
    let models: Vec<_> = models
        .into_iter()
        .map(|(model, usage)| {
            json!({
                "model": model,
                "input": usage.input,
                "output": usage.output,
                "cache_creation": usage.cache_creation,
                "cache_read": usage.cache_read,
                "cost": usage.cost,
            })
        })
        .collect();

    json!({
        "total": {
            "input": total.input,
            "output": total.output,
            "cache_creation": total.cache_creation,
            "cache_read": total.cache_read,
            "cost": cost,
        },
        "models": models,
    })
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

fn ring_bell() {
    use std::io::Write;
    let mut out = std::io::stdout().lock();
    let _ = out.write_all(b"\x07");
    let _ = out.flush();
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::selection::SelectionZone;
    use crossterm::event::KeyModifiers;
    use maki_agent::{AgentError, AgentId, DoneReason, SessionMailbox, TurnId, TurnOutcome};
    use maki_config::PermissionsConfig;
    use maki_providers::provider::BoxFuture;
    use maki_providers::{ModelInfo, ProviderEvent, RequestOptions, StreamResponse, TokenUsage};
    use ratatui::{Terminal, backend::TestBackend};
    use tempfile::TempDir;
    use test_case::test_case;

    const OBSERVATION: &str = "failed";
    const SHELL_RESULT: &str = "command finished";
    const RUNTIME_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(1);

    struct StubProvider;

    impl Provider for StubProvider {
        fn stream_message<'a>(
            &'a self,
            _model: &'a Model,
            _messages: &'a [Message],
            _system: &'a str,
            _tools: &'a serde_json::Value,
            _event_tx: &'a flume::Sender<ProviderEvent>,
            _opts: RequestOptions,
            _session_id: Option<&'a SessionRef>,
        ) -> BoxFuture<'a, Result<StreamResponse, AgentError>> {
            Box::pin(std::future::pending())
        }

        fn list_models(&self) -> BoxFuture<'_, Result<Vec<ModelInfo>, AgentError>> {
            Box::pin(async { Ok(Vec::new()) })
        }
    }

    struct RuntimeHarness {
        _temp_dir: TempDir,
        ctx: Option<SpawnCtx>,
    }

    impl RuntimeHarness {
        fn new() -> Self {
            let temp_dir = tempfile::tempdir().unwrap();
            let storage = StateDir::from_path(temp_dir.path().to_path_buf());
            let sessions_dir = storage.ensure_subdir(SESSIONS_DIR).unwrap();
            let available_models = Arc::new(ArcSwapOption::empty());
            let command_registry = maki_commands::CommandRegistry::new();
            let command_runtime = Arc::new(CommandRuntime::new_for_test(
                &[],
                command_registry,
                Arc::new(ModelArgSource::new(Arc::clone(&available_models))),
                Arc::new(ThemeArgSource::new(
                    crate::theme::default_provider().clone(),
                )),
            ));
            let model = crate::components::test_model();
            let (model_slot, _provider_change_rx) =
                ProviderSlot::new(model, Arc::new(StubProvider));
            let permissions = Arc::new(PermissionManager::new(
                PermissionsConfig::default(),
                temp_dir.path().to_path_buf(),
                Arc::default(),
            ));
            let storage_writer =
                Arc::new(StorageWriter::new(storage.clone(), flume::unbounded().0));
            let ctx = SpawnCtx {
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
                available_models,
                storage_writer,
                model_policy: Arc::new(ModelPolicy::default()),
                system_prompt: SystemPromptOverride::default(),
                command_runtime,
            };
            Self {
                _temp_dir: temp_dir,
                ctx: Some(ctx),
            }
        }

        fn ctx(&self) -> &SpawnCtx {
            self.ctx.as_ref().unwrap()
        }

        fn session(&self) -> AppSession {
            let cwd = self._temp_dir.path().to_string_lossy();
            AppSession::new("test-model", cwd.as_ref())
        }

        fn prepare(&self) -> PreparedSessionRuntime {
            self.ctx().prepare_runtime(self.session())
        }

        fn runtime(&self, session: AppSession) -> SessionRuntime {
            self.ctx().spawn_runtime(session).unwrap()
        }

        fn target_count(&self) -> usize {
            self.ctx().command_runtime.registry.target_count()
        }
    }

    impl Drop for RuntimeHarness {
        fn drop(&mut self) {
            let Some(ctx) = self.ctx.take() else {
                return;
            };
            let storage_writer = Arc::clone(&ctx.storage_writer);
            drop(ctx);
            let Ok(storage_writer) = Arc::try_unwrap(storage_writer) else {
                panic!("runtime harness owns storage writer");
            };
            storage_writer.shutdown(RUNTIME_SHUTDOWN_TIMEOUT);
        }
    }

    fn shutdown_manager(manager: &maki_agent::AgentManagerHandle) {
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
        let runtime = prepared.activate(&harness.ctx().model_slot, None);
        let (active_manager, active_root_id) = runtime.handles.manager_and_root();

        assert_eq!(harness.target_count(), target_count + 1);
        assert_eq!(runtime.id(), session_id);
        assert_eq!(runtime.app.command_target.id(), target_id);
        assert_eq!(active_manager.generation(), manager.generation());
        assert_eq!(active_root_id, root_id);
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
        let prepared = harness.ctx().prepare_runtime(session);
        let mut runtime = prepared.activate(&harness.ctx().model_slot, None);

        assert!(runtime.handles.queue.is_empty());
        assert!(runtime.restore_pending);
        runtime.activate_deferred();
        runtime.activate_deferred();

        let envelope = runtime
            .handles
            .agent_rx
            .recv_timeout(RUNTIME_SHUTDOWN_TIMEOUT)
            .expect("restored queue item was not consumed");
        assert!(matches!(
            envelope.event,
            AgentEvent::QueueItemConsumed { ref text, .. } if text == "restored"
        ));
        assert!(runtime.handles.queue.is_empty());
        assert!(!runtime.restore_pending);

        release_runtime(runtime);
    }

    #[test]
    fn committed_rewind_does_not_restore_failed_turn_queue() {
        const RECOVERY_TEXT: &str = "retained after failure";
        const REWOUND_TEXT: &str = "rewound prompt";

        let harness = RuntimeHarness::new();
        let mut session = harness.session();
        session.push_message(Message::user(REWOUND_TEXT.into()));
        let mut app = crate::app::tests::test_app();
        app.apply_loaded_session(session.clone(), &harness.ctx().model_slot.load().model);
        app.status = Status::Streaming;
        app.run_id = 1;
        assert!(matches!(
            app.submit_prompt(QueuedMessage {
                text: RECOVERY_TEXT.into(),
                images: Vec::new(),
            }),
            SubmitOutcome::Queued
        ));
        app.update(Msg::Agent(Box::new(Envelope {
            event: AgentEvent::ControlError {
                message: "failed".into(),
            },
            subagent: None,
            run_id: 1,
        })));

        app.update(Msg::Key(crate::components::key(
            crossterm::event::KeyCode::Esc,
        )));
        app.update(Msg::Key(crate::components::key(
            crossterm::event::KeyCode::Esc,
        )));
        let actions = app.update(Msg::Key(crate::components::key(
            crossterm::event::KeyCode::Enter,
        )));
        let Action::ReplaceSession(request) = actions.into_iter().next().unwrap() else {
            panic!("expected replacement request");
        };
        let mut runtime = harness.runtime(session);
        let prepared = harness.ctx().prepare_runtime(request.session);
        let old = replace_session_runtime(
            &mut runtime,
            prepared,
            &harness.ctx().sessions_dir,
            &harness.ctx().model_slot,
        )
        .unwrap();

        assert!(runtime.handles.queue.is_empty());
        assert!(
            runtime
                .handles
                .agent_rx
                .recv_timeout(RUNTIME_SHUTDOWN_TIMEOUT)
                .is_err()
        );

        let actions = runtime.app.update(Msg::Key(crate::components::key(
            crossterm::event::KeyCode::Enter,
        )));
        let Action::SendMessage(input) = actions.into_iter().next().unwrap() else {
            panic!("expected send message action");
        };
        assert_eq!(input.message, REWOUND_TEXT);

        release_runtime(old);
        release_runtime(runtime);
    }

    #[test]
    fn empty_history_replacement_restores_and_checkpoints_draft() {
        const DRAFT: &str = "first prompt";

        let harness = RuntimeHarness::new();
        let mut session = harness.session();
        session.meta.input_draft = Some(DRAFT.into());
        let prepared = harness.ctx().prepare_runtime(session);
        let mut runtime = prepared.activate(&harness.ctx().model_slot, None);

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
            .prepare_runtime_with_provider(session, Some(provider));
        let runtime = prepared.activate(&harness.ctx().model_slot, None);
        let installed = harness.ctx().model_slot.load();

        assert_eq!(runtime.app.state.model.spec(), REPLACEMENT_MODEL);
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
                .prepare_replacement_runtime(session, runtime.app.permissions.as_ref())
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

        let prepared = harness.ctx().prepare_runtime(replacement);

        assert!(
            prepared
                .app
                .session_rule_allows(&maki_config::ToolKey::native("bash"), "cargo test")
        );
        drop(prepared);
        release_runtime(current);
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
        replacement.meta.session_rules = vec![maki_storage::sessions::StoredRule {
            tool: "bash".into(),
            scope: Some("target".into()),
            effect: maki_storage::sessions::StoredEffect::Allow,
        }];
        let prepared = harness.ctx().prepare_runtime_with_provider_and_permissions(
            replacement,
            None,
            runtime.app.permissions.as_ref(),
        );

        let old = replace_session_runtime(
            &mut runtime,
            prepared,
            &harness.ctx().sessions_dir,
            &harness.ctx().model_slot,
        )
        .unwrap();

        assert_eq!(runtime.app.permissions.is_yolo(), current_yolo);
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
        let prepared = harness.ctx().prepare_runtime(replacement);

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
        let prepared = harness.ctx().prepare_runtime(session);

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
        assert_eq!(
            AppSession::load(id, &harness.ctx().storage)
                .unwrap()
                .messages()
                .len(),
            stored_before.messages().len()
        );
        lease.release().unwrap();
        release_runtime(runtime);
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

    #[test_case(session_lock::LockBeat::Held ; "held_commits")]
    #[test_case(session_lock::LockBeat::Lost ; "lost_reports_lock_loss")]
    fn same_id_replacement_waits_for_in_flight_heartbeat(beat: session_lock::LockBeat) {
        const OUTGOING_DRAFT: &str = "draft before replacement";
        const PROGRAMMATIC_PROMPT: &str = "programmatic submission";
        const RESTORED_DRAFT: &str = "replacement restored prompt";

        let harness = RuntimeHarness::new();
        let mut session = harness.session();
        let id = session.id;
        let mut runtime = harness.runtime(session.clone());
        runtime.app.input_box.set_input(OUTGOING_DRAFT.into());
        session.meta.input_draft = Some(RESTORED_DRAFT.into());
        let generation = runtime.generation;
        let (internal_tx, internal_rx) = flume::unbounded();
        let (entered_tx, entered_rx) = flume::bounded(1);
        let (release_tx, release_rx) = flume::bounded(1);
        start_runtime_heartbeat_with(&mut runtime, &internal_tx, move |lease| {
            entered_tx.send(()).unwrap();
            release_rx.recv().unwrap();
            (lease, Ok(beat))
        });
        entered_rx.recv().unwrap();
        let prepared = harness.ctx().prepare_runtime_with_provider_and_permissions(
            session,
            None,
            runtime.app.permissions.as_ref(),
        );
        let pending = PendingReplacement {
            prepared,
            kind: SessionReplacementKind::Rewind,
            post_commit: None,
        };

        assert!(defer_replacement_for_heartbeat(&mut runtime, pending).is_none());
        assert!(runtime.pending_replacement.is_some());
        assert_eq!(runtime.id(), id);

        let edit_actions = runtime.update(Msg::Key(crate::components::key(
            crossterm::event::KeyCode::Char('x'),
        )));
        assert!(edit_actions.is_empty());
        assert_eq!(runtime.app.input_box.buffer.value(), OUTGOING_DRAFT);
        let submit_actions = runtime.update(Msg::Key(crate::components::key(
            crossterm::event::KeyCode::Enter,
        )));
        assert!(submit_actions.is_empty());
        assert_eq!(runtime.app.input_box.buffer.value(), OUTGOING_DRAFT);
        assert!(matches!(
            runtime.submit_text(PROGRAMMATIC_PROMPT.into()),
            Err(ref error) if error == REPLACEMENT_PENDING_ERR
        ));
        assert!(runtime.handles.queue.is_empty());

        release_tx.send(()).unwrap();
        let InternalEvent::SessionHeartbeat(event_generation) = internal_rx.recv().unwrap() else {
            panic!("expected heartbeat completion");
        };
        assert_eq!(event_generation, generation);
        assert!(matches!(
            runtime.session_lock,
            Some(SessionLockState::InFlight(_))
        ));

        let pending = complete_runtime_heartbeat(&mut runtime);
        if beat == session_lock::LockBeat::Held {
            let old = replace_session_runtime(
                &mut runtime,
                pending.expect("replacement remains pending").prepared,
                &harness.ctx().sessions_dir,
                &harness.ctx().model_slot,
            );
            let old = match old {
                Ok(old) => old,
                Err(error) => panic!("verified lease should transfer: {error}"),
            };
            assert_eq!(runtime.id(), id);
            assert!(!runtime.lock_lost);
            assert_eq!(runtime.app.input_box.buffer.value(), RESTORED_DRAFT);
            let actions = runtime.update(Msg::Key(crate::components::key(
                crossterm::event::KeyCode::Enter,
            )));
            assert!(matches!(
                actions.as_slice(),
                [Action::SendMessage(input)] if input.message == RESTORED_DRAFT
            ));
            release_runtime(old);
        } else {
            assert!(pending.is_none());
            assert!(runtime.pending_replacement.is_none());
            assert!(runtime.lock_lost);
            assert_eq!(runtime.app.exit_request, ExitRequest::Error);
        }
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

        let prepared = harness.ctx().prepare_runtime(session);
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
            let result = release_lock_state_with_timeout(state, AGENT_SHUTDOWN_TIMEOUT);
            cleanup_done_tx.send(result).unwrap();
        });
        cleanup_started_rx.recv().unwrap();
        assert!(cleanup_done_rx.try_recv().is_err());

        release_tx.send(()).unwrap();
        let LockReleaseOutcome::Completed { result, lock_lost } = cleanup_done_rx.recv().unwrap()
        else {
            panic!("heartbeat cleanup timed out");
        };
        result.unwrap();
        assert!(!lock_lost);
        cleanup.join().unwrap();
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
                .send(release_lock_state_with_timeout(
                    state,
                    AGENT_SHUTDOWN_TIMEOUT,
                ))
                .unwrap();
        });
        assert!(cleanup_done_rx.try_recv().is_err());
        heartbeat_release_tx.send(()).unwrap();
        let LockReleaseOutcome::Completed {
            result,
            lock_lost: heartbeat_lock_lost,
        } = cleanup_done_rx.recv().unwrap()
        else {
            panic!("heartbeat cleanup timed out");
        };
        result.unwrap();
        cleanup.join().unwrap();

        if !runtime.lock_lost && !heartbeat_lock_lost {
            runtime.app.checkpoint_now();
        }
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

        let outcome = release_lock_state_with_timeout(runtime.session_lock.take(), Duration::ZERO);
        assert!(matches!(&outcome, LockReleaseOutcome::TimedOut));
        if !runtime.lock_lost
            && matches!(
                &outcome,
                LockReleaseOutcome::Completed {
                    lock_lost: false,
                    ..
                }
            )
        {
            runtime.app.checkpoint_now();
        }
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
        let prepared = harness.ctx().prepare_runtime(session);

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
        let prepared = harness.ctx().prepare_runtime(target);

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
        let prepared = harness.ctx().prepare_runtime(target);
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
        ) -> maki_commands::CommandFuture<
            Result<maki_commands::HostResponse, maki_commands::CommandError>,
        > {
            Box::pin(async { Ok(maki_commands::HostResponse::Completed) })
        }
    }

    fn done_event() -> AgentEvent {
        AgentEvent::TurnOutcome(TurnOutcome::Completed {
            agent_id: AgentId::generate(),
            turn_id: TurnId::generate(),
            usage: TokenUsage::default(),
            num_turns: 1,
            reason: DoneReason::EndTurn,
        })
    }

    fn due_completion() -> RunNotificationState {
        let mut state = RunNotificationState::default();
        state.on_done(&done_event());
        state.on_drain();
        state
    }

    #[test]
    fn session_usage_preserves_costs_and_sorts_models_deterministically() {
        let total = TokenUsage {
            input: 10,
            output: 20,
            cache_creation: 30,
            cache_read: 40,
        };
        let usage_by_model = HashMap::from([
            (
                "z/model".to_owned(),
                StoredTokenUsage {
                    input: 5,
                    output: 5,
                    cache_creation: 0,
                    cache_read: 0,
                    cost: None,
                },
            ),
            (
                "b/model".to_owned(),
                StoredTokenUsage {
                    input: 14,
                    output: 1,
                    cache_creation: 2,
                    cache_read: 3,
                    cost: Some(0.25),
                },
            ),
            (
                "a/model".to_owned(),
                StoredTokenUsage {
                    input: 10,
                    output: 0,
                    cache_creation: 0,
                    cache_read: 0,
                    cost: Some(0.5),
                },
            ),
        ]);

        assert_eq!(
            session_usage(&total, None, &usage_by_model),
            json!({
                "total": {
                    "input": 10,
                    "output": 20,
                    "cache_creation": 30,
                    "cache_read": 40,
                    "cost": null,
                },
                "models": [
                    {
                        "model": "b/model",
                        "input": 14,
                        "output": 1,
                        "cache_creation": 2,
                        "cache_read": 3,
                        "cost": 0.25,
                    },
                    {
                        "model": "a/model",
                        "input": 10,
                        "output": 0,
                        "cache_creation": 0,
                        "cache_read": 0,
                        "cost": 0.5,
                    },
                    {
                        "model": "z/model",
                        "input": 5,
                        "output": 5,
                        "cache_creation": 0,
                        "cache_read": 0,
                        "cost": null,
                    },
                ],
            })
        );
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
