//! Elm-style `update(Msg) -> Vec<Action>`; side effects are dispatched by the caller.
//! Double-esc: first esc flashes a hint, second within `flash_duration` cancels/rewinds.
//! `run_id` invalidates in-flight agent events. It bumps in exactly three
//! places, one per transition: `start_run`, `handle_cancel`, and
//! `AgentHandles::respawn`. Everything else only reads it.

mod btw;
mod image_paste;
pub(crate) mod mode;
mod mouse;
mod queue;
mod session;
pub(crate) mod session_state;
pub(crate) mod shell;
#[cfg(test)]
pub(crate) mod tests;
pub(crate) mod view;

use std::collections::{HashMap, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use crate::AppSession;
use crate::chat::Chat;
use crate::chat::{CANCELLED_TEXT, ChatEventResult, DONE_TEXT, ERROR_TEXT};
use crate::clipboard::ClipboardState;
use crate::command_runtime::CommandRuntime;

use crate::components::btw_modal::BtwModal;
#[cfg(test)]
use crate::components::command::ParsedCommand;
use crate::components::command::{CommandAction, CommandPalette, ConfirmedCommand};
use crate::components::file_completion::{
    CompletionAction, CompletionItem, FileCompletionMenu, at_token_range,
};
use crate::components::file_picker::{FilePickerModal, FilePickerModalAction};
use crate::components::help_modal::HelpModal;
use crate::components::input::{InputAction, InputBox, Submission};
use crate::components::keybindings::key;
use crate::components::list_picker::{ListPicker, PickerAction, PickerItem};
use crate::components::login_picker::{LoginPicker, LoginPickerAction};
use crate::components::lua_float::FloatManager;
use crate::components::lua_picker::{LuaPicker, LuaPickerAction};
use crate::components::mcp_picker::{McpPicker, McpPickerAction};
use crate::components::model_picker::{ModelPicker, ModelPickerAction};
use crate::components::permission_prompt::PermissionPrompt;
use crate::components::plan_form::{PlanForm, PlanFormAction};
use crate::components::rewind_picker::{RewindPicker, RewindPickerAction};
use crate::components::scrollbar;
use crate::components::search_modal::{SearchAction, SearchModal};
use crate::components::status_bar::StatusBar;
use crate::components::theme_picker::{ThemePicker, ThemePickerAction};
use crate::components::usage_modal::{UsageFetchState, UsageModal};
use crate::components::{
    Action, DisplayMessage, DisplayRole, ExitRequest, Overlay, RetryInfo, Status, is_ctrl,
};
use crate::image;
use crate::repaint::{Cadence, Dirty, Watch};
use crate::selection::{SelectionState, SelectionZone, ZoneRegistry};
use arc_swap::{ArcSwap, ArcSwapOption};
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers, MouseEvent};
use maki_agent::permissions::PermissionManager;
use maki_agent::{
    AgentEvent, Envelope, ImageSource, McpConfigErrors, McpSnapshotReader, SharedBuf,
    SharedMessages, SubagentInfo,
};
use maki_commands::{
    AgentTurn, BuiltinOperation, CommandAttachment, CommandContent, CommandError,
    HostContextRequest, HostContextResponse, HostRequest, HostResponse, TargetHandle,
};
use maki_config::{ModelPolicy, ToolKey, UiConfig};
use maki_lua::{
    BuiltinAction, CompletionCtx, EventHandle, FloatConfig, HintReader, HintSnapshot, ItemSpec,
    KeymapReader, Split, WinCommand, WinEvent, WinView,
};
use maki_providers::{ContentBlock, Message, Model, Role, ThinkingConfig, add_cost, format_tokens};
use maki_storage::StateDir;
use maki_storage::input_history::InputHistory;
use maki_storage::model::persist_model;

use crate::storage_writer::StorageWriter;
use crate::theme::ThemesProvider;
use ratatui::layout::Position;

pub(crate) use crate::agent::QueuedMessage;

fn command_thinking(config: ThinkingConfig) -> maki_commands::ThinkingConfig {
    use maki_providers::Effort;
    match config {
        ThinkingConfig::Off => maki_commands::ThinkingConfig::Off,
        ThinkingConfig::Adaptive => maki_commands::ThinkingConfig::Adaptive,
        ThinkingConfig::Effort(Effort::Minimal) => maki_commands::ThinkingConfig::Minimal,
        ThinkingConfig::Effort(Effort::Low) => maki_commands::ThinkingConfig::Low,
        ThinkingConfig::Effort(Effort::Medium) => maki_commands::ThinkingConfig::Medium,
        ThinkingConfig::Effort(Effort::High) => maki_commands::ThinkingConfig::High,
        ThinkingConfig::Effort(Effort::XHigh) => maki_commands::ThinkingConfig::XHigh,
        ThinkingConfig::Effort(Effort::Max) => maki_commands::ThinkingConfig::Max,
        ThinkingConfig::Budget(budget) => maki_commands::ThinkingConfig::Budget(budget),
    }
}

fn provider_thinking(config: maki_commands::ThinkingConfig) -> ThinkingConfig {
    use maki_providers::Effort;
    match config {
        maki_commands::ThinkingConfig::Off => ThinkingConfig::Off,
        maki_commands::ThinkingConfig::Adaptive => ThinkingConfig::Adaptive,
        maki_commands::ThinkingConfig::Minimal => ThinkingConfig::Effort(Effort::Minimal),
        maki_commands::ThinkingConfig::Low => ThinkingConfig::Effort(Effort::Low),
        maki_commands::ThinkingConfig::Medium => ThinkingConfig::Effort(Effort::Medium),
        maki_commands::ThinkingConfig::High => ThinkingConfig::Effort(Effort::High),
        maki_commands::ThinkingConfig::XHigh => ThinkingConfig::Effort(Effort::XHigh),
        maki_commands::ThinkingConfig::Max => ThinkingConfig::Effort(Effort::Max),
        maki_commands::ThinkingConfig::Budget(budget) => ThinkingConfig::Budget(budget),
    }
}
pub(crate) use mode::{Mode, PlanState, PlanTrigger};
#[cfg(test)]
use mouse::EDGE_SCROLL_LINES;
pub(crate) use queue::{MessageQueue, SubmitOutcome};
use session::Sent;
pub(crate) use session::session_has_content;
use session_state::SessionState;

const CANCEL_MSG: &str = "Cancelled.";
/// Bypasses the per-run staleness filter because re-bake replies
/// don't belong to any real agent run.
pub(crate) const RESTORE_RUN_ID: u64 = u64::MAX;
const FLASH_CANCEL: &str = "Press esc again to stop...";
const FLASH_REWIND: &str = "Press esc again to rewind...";
const AUTH_EXPIRED_MSG: &str =
    "Token expired. Run `makima auth login` in another terminal, then press Enter to retry.";
const FLASH_NO_PLAN: &str = "No plan file";
const FLASH_NO_PLAN_BODY: &str = "Plan file is empty or unreadable";
const PLAN_SUBMIT_TOOL: &str = "plan_submit";
const SESSION_PICKER_REQUESTED_EVENT: &str = "SessionPickerRequested";
const FAST_UNSUPPORTED_MSG: &str = "Fast mode requires an Anthropic Opus 4.6+ model (API only)";
const THINKING_UNSUPPORTED_MSG: &str = "Thinking requires a model that supports it";
const FAST_ON_MSG: &str = "Fast mode: on";
const FAST_OFF_MSG: &str = "Fast mode: off";
const WORKFLOW_ON_MSG: &str = "Workflow mode: on";
const WORKFLOW_OFF_MSG: &str = "Workflow mode: off";
const IMPLEMENT_MSG_PREFIX: &str = "Implement the plan";
const IMPLEMENT_PARALLEL_HINT: &str = "Use batch+task to parallelize, assign each subagent a separate module and restrict its tests to that module to avoid interference.";
const THEME_APPLIED_PREFIX: &str = "Theme";

const TASK_DONE_DETAIL: &str = "✓ ";
const MISSING_TOOL_COMPLETION: &str = "Tool did not report completion before the turn ended";
/// Wraps an auto-queued subagent reply with its task id so the main agent can
/// tell it apart from a directly typed user message.
const SUBAGENT_REPLY_HEADER: &str = "[msg from ";
const SUBAGENT_REPLY_SUFFIX: &str = "] ";
/// Length cap for the `/tasks` row message snippet (AC.10).
const SNIPPET_CHARS: usize = 64;

const NOTIFICATION_PREVIEW_CHARS: usize = 120;

/// A tool-call demand that needs the user's input is held this long after the
/// last keystroke before it steals focus, so it does not interrupt mid-typing.
const INPUT_DEFER: Duration = Duration::from_secs(2);

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Notification {
    TurnComplete { response: Option<String> },
    PermissionRequested { tool: Option<String> },
    AuthenticationRequired,
    QuestionRequested,
    PlanReady,
}

impl Notification {
    pub(crate) fn is_urgent(&self) -> bool {
        !matches!(self, Self::TurnComplete { .. })
    }

    pub(crate) fn message(&self) -> String {
        match self {
            Self::TurnComplete { response } => response
                .clone()
                .unwrap_or_else(|| "Agent turn complete".into()),
            Self::PermissionRequested { tool: Some(tool) } => {
                format!("Permission requested: {tool}")
            }
            Self::PermissionRequested { tool: None } => "Permission requested".into(),
            Self::AuthenticationRequired => "Authentication required".into(),
            Self::QuestionRequested => "Question requested".into(),
            Self::PlanReady => "Plan ready".into(),
        }
    }

    pub(crate) fn error_completion() -> Self {
        Self::TurnComplete {
            response: Some("Agent stopped with an error".into()),
        }
    }
}

fn notification_preview(text: &str) -> Option<String> {
    let preview: String = text.split_whitespace().collect::<Vec<_>>().join(" ");
    (!preview.is_empty()).then(|| preview.chars().take(NOTIFICATION_PREVIEW_CHARS).collect())
}

fn normalize_preview(text: &str) -> Option<String> {
    notification_preview(text)
}

pub(crate) fn turn_response(message: &Message) -> Option<String> {
    if message.has_tool_calls() {
        return None;
    }
    notification_preview(
        &message
            .content
            .iter()
            .filter_map(|block| match block {
                ContentBlock::Text { text } => Some(text.as_str()),
                _ => None,
            })
            .collect::<Vec<_>>()
            .join(" "),
    )
}

#[derive(Clone)]
pub(super) struct TaskEntry {
    name: String,
    finished: Option<bool>,
    chat_index: usize,
    /// First `SNIPPET_CHARS` of the subagent chat's last message, shown dimly.
    snippet: String,
    context: String,
    started_at: Option<Instant>,
}

impl PickerItem for TaskEntry {
    fn label(&self) -> &str {
        &self.name
    }
    fn detail(&self) -> Option<&str> {
        matches!(self.finished, Some(true)).then_some(TASK_DONE_DETAIL)
    }
    fn suffix(&self) -> Option<&str> {
        (!self.snippet.is_empty()).then_some(self.snippet.as_str())
    }
    fn is_spinning(&self) -> bool {
        matches!(self.finished, Some(false))
    }
    fn context_str(&self) -> Option<&str> {
        (!self.context.is_empty()).then_some(self.context.as_str())
    }
    fn ago(&self) -> Option<String> {
        self.started_at.map(ago)
    }
    fn is_finished(&self) -> bool {
        matches!(self.finished, Some(true))
    }
}

/// Channels held for a live subagent tab.
#[derive(Clone, Default)]
struct SubagentChannels {
    answer_tx: Option<flume::Sender<String>>,
    input_tx: Option<flume::Sender<String>>,
}

fn truncate_snippet(text: &str) -> String {
    let mut chars = text.chars();
    let truncated: String = chars.by_ref().take(SNIPPET_CHARS).collect();
    if chars.next().is_some() {
        format!("{truncated}…")
    } else {
        truncated
    }
}

fn ago(since: Instant) -> String {
    maki_lua::format_ago(since.elapsed().as_secs())
}

/// Last assistant text block in a subagent's flushed history, used to feed an
/// async subagent's reply back into the main agent's queue.
fn terminal_reply(messages: &[Message]) -> Option<String> {
    messages
        .iter()
        .rev()
        .find(|m| matches!(m.role, Role::Assistant))
        .and_then(|m| {
            m.content.iter().find_map(|b| match b {
                ContentBlock::Text { text } => Some(text.clone()),
                _ => None,
            })
        })
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(super) enum PendingInput {
    #[default]
    None,
    AuthRetry {
        subagent_id: Option<String>,
    },
}

pub enum Msg {
    Key(KeyEvent),
    Paste(String),
    Mouse(MouseEvent),
    Scroll { column: u16, row: u16, delta: i32 },
    Agent(Box<Envelope>),
}

/// The two input-demanding surfaces that can be deferred.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum InputKind {
    Permission,
    Question,
}

/// A queued permission request's payload; lives in the queue so the prompt is
/// not opened (and does not contaminate overlay state) until activation.
#[derive(Debug, Clone)]
pub(crate) struct PermissionPayload {
    id: String,
    tool: ToolKey,
    scopes: Vec<String>,
    subagent_id: Option<String>,
}

/// One entry in the input-arbitration queue. FIFO: only the head can activate.
#[derive(Debug, Clone)]
pub(crate) struct InputDemand {
    kind: InputKind,
    blocked_by_modal: bool,
    /// Alt+M manual deferral: held until the next submit instead of the 2s
    /// idle timer. Auto-deferrals keep this `false`.
    hold_until_submit: bool,
    perm: Option<PermissionPayload>,
}

pub struct App {
    pub(super) chats: Vec<Chat>,
    pub(super) active_chat: usize,
    pub(super) chat_index: HashMap<String, usize>,
    pub(crate) input_box: InputBox,
    pub(super) command_palette: CommandPalette,
    pub(crate) command_runtime: Arc<CommandRuntime>,
    pub(crate) command_target: TargetHandle,
    pub(super) task_picker: ListPicker<TaskEntry>,
    pub(super) task_picker_original: Option<usize>,
    pub(super) lua_picker: LuaPicker,
    pub(super) theme_picker: ThemePicker,
    pub(super) model_picker: ModelPicker,
    pub(super) login_picker: LoginPicker,
    pub(super) mcp_picker: McpPicker,
    pub(super) rewind_picker: RewindPicker,
    pub(super) help_modal: HelpModal,
    pub(super) usage_modal: UsageModal,
    usage_readout_watch: Watch<UsageFetchState>,
    pub(super) btw_modal: BtwModal,
    pub(super) float_mgr: FloatManager,
    pub(super) search_modal: SearchModal,
    pub(super) file_picker: FilePickerModal,
    pub(super) file_completion: FileCompletionMenu,
    pub(super) permission_prompt: PermissionPrompt,
    pub(super) plan_form: PlanForm,
    pub(super) status_bar: StatusBar,
    pub status: Status,
    pub(crate) state: session_state::SessionState,
    pub exit_request: ExitRequest,
    pub(crate) exit_on_done: bool,
    pub(crate) queue: MessageQueue,
    recoverable_queue: Vec<String>,
    pub answer_tx: Option<flume::Sender<String>>,
    pub(crate) cmd_tx: Option<flume::Sender<super::AgentCommand>>,
    pub(super) pending_input: PendingInput,
    pub(crate) run_id: u64,
    pub(super) retry_info: Option<RetryInfo>,
    pub(super) zones: ZoneRegistry,
    pub(super) selection_state: Option<SelectionState>,
    pub(super) clipboard: ClipboardState,
    pub(super) last_esc: Option<Instant>,
    /// Last user keystroke/paste; `None` until the first. Drives input deferral.
    pub(super) last_input: Option<Instant>,
    /// The input surface currently shown/focused (`None` when nothing is live).
    pub(super) active_input: Option<InputKind>,
    /// Pending input demands in arrival order; the head promotes when idle.
    pub(super) input_queue: VecDeque<InputDemand>,
    /// Bell owed by a promotion/arrival; drained by the event loop after tick.
    pub(super) pending_bell: bool,
    /// Armed by a keyboard submit (main or subagent input) to release a manual
    /// Alt+M hold; consumed by the next promotion pass.
    pub(super) submit_released: bool,

    pub(crate) storage: StateDir,
    pub(crate) theme_provider: Arc<dyn ThemesProvider>,
    pub(crate) usage_slot: Arc<ArcSwapOption<UsageFetchState>>,
    pub(crate) available_models: Arc<ArcSwapOption<Vec<String>>>,
    pub(crate) shared_history: Option<SharedMessages>,
    pub(crate) btw_system: Option<Arc<ArcSwap<String>>>,
    pub(crate) image_paste_rx: Vec<flume::Receiver<Result<ImageSource, String>>>,
    storage_writer: Arc<StorageWriter>,
    last_sent: Option<Sent>,
    pub(crate) shell: shell::ShellState,
    pub(crate) ui_config: UiConfig,
    pub(crate) permissions: Arc<PermissionManager>,
    pub(crate) model_policy: Arc<ModelPolicy>,
    pub(crate) lua_event_handle: EventHandle,
    pub(super) keymap_reader: KeymapReader,
    pub(super) hint_reader: HintReader,
    hints: Watch<HintSnapshot>,
    pub(crate) restore_event_tx: Option<maki_agent::EventSender>,
    pub(super) restoring: Arc<AtomicBool>,
    /// Per-subagent channels: the `answer_tx` (mid-turn interrupt replies) and,
    /// for async sessions, the driver `input_tx` (tab submits routed to the
    /// subagent). Keyed by `parent_tool_use_id`.
    subagent_channels: HashMap<String, SubagentChannels>,
}

impl App {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        model: &Model,
        session: AppSession,
        storage: StateDir,
        available_models: Arc<ArcSwapOption<Vec<String>>>,
        mcp_reader: McpSnapshotReader,
        mcp_config_errors: McpConfigErrors,
        keymap_reader: KeymapReader,
        hint_reader: HintReader,
        storage_writer: Arc<StorageWriter>,
        ui_config: UiConfig,
        input_history_size: usize,
        permissions: Arc<PermissionManager>,
        lua_event_handle: EventHandle,
        model_policy: Arc<ModelPolicy>,
        theme_provider: Arc<dyn ThemesProvider>,
        command_runtime: Arc<CommandRuntime>,
    ) -> Self {
        scrollbar::set_enabled(ui_config.scrollbar);
        let state = SessionState::from_session(session, model, &storage, &model_policy);
        let typewriter = ui_config.typewriter_ms_per_char;
        let flash = ui_config.flash_duration();
        let input_box = InputBox::new(
            InputHistory::load(&storage, input_history_size),
            ui_config.max_input_lines,
        );
        let command_target = command_runtime.bind_target();
        let mut app = Self {
            chats: vec![Chat::new(
                "Main".into(),
                ui_config.clone(),
                lua_event_handle.clone(),
                Arc::clone(&theme_provider),
            )],
            active_chat: 0,
            chat_index: HashMap::new(),
            input_box,
            command_runtime: Arc::clone(&command_runtime),
            command_target: command_target.clone(),
            command_palette: CommandPalette::new(command_runtime.registry.clone(), command_target),
            task_picker: ListPicker::new(),
            task_picker_original: None,
            lua_picker: LuaPicker::new(lua_event_handle.clone()),
            theme_picker: ThemePicker::new(Arc::clone(&theme_provider)),
            model_picker: ModelPicker::new(Arc::clone(&available_models)),
            login_picker: LoginPicker::new(),
            mcp_picker: McpPicker::new(mcp_reader, mcp_config_errors),
            rewind_picker: RewindPicker::new(),
            help_modal: HelpModal::new(),
            usage_modal: UsageModal::new(),
            usage_readout_watch: Watch::default(),
            btw_modal: BtwModal::new(typewriter),
            float_mgr: FloatManager::new(),
            search_modal: SearchModal::new(),
            file_picker: FilePickerModal::new(),
            file_completion: FileCompletionMenu::new(),
            permission_prompt: PermissionPrompt::new(),
            plan_form: PlanForm::new(),
            status_bar: StatusBar::new(flash),
            status: Status::Idle,
            state,
            exit_request: ExitRequest::None,
            exit_on_done: false,
            queue: MessageQueue::default(),
            recoverable_queue: Vec::new(),
            answer_tx: None,
            cmd_tx: None,
            pending_input: PendingInput::None,
            run_id: 0,
            retry_info: None,
            zones: ZoneRegistry::new(),
            selection_state: None,
            clipboard: ClipboardState::new(),
            last_esc: None,
            last_input: None,
            active_input: None,
            input_queue: VecDeque::new(),
            pending_bell: false,
            submit_released: false,
            storage,
            theme_provider,
            usage_slot: Arc::new(ArcSwapOption::empty()),
            available_models,
            shared_history: None,
            btw_system: None,
            image_paste_rx: vec![],
            storage_writer,
            last_sent: None,
            shell: shell::ShellState::default(),
            ui_config,
            permissions,
            model_policy: Arc::clone(&model_policy),
            lua_event_handle,
            hints: Watch::seeded(hint_reader.load_full()),
            keymap_reader,
            hint_reader,
            restore_event_tx: None,
            restoring: Arc::new(AtomicBool::new(false)),
            subagent_channels: HashMap::new(),
        };
        app.model_picker.set_recents(
            maki_storage::model::read_recents(&app.storage)
                .into_iter()
                .filter(|spec| model_policy.allows(spec))
                .collect(),
        );
        app
    }

    pub(crate) fn main_chat(&mut self) -> &mut Chat {
        &mut self.chats[0]
    }

    fn is_main_chat(&self) -> bool {
        self.active_chat == 0
    }

    fn plan_form_active(&self) -> bool {
        self.state.mode == Mode::Plan && self.plan_form.is_visible()
    }

    pub(crate) fn update_model(&mut self, model: &Model) {
        self.state.update_model(model);
        persist_model(&self.storage, &self.state.session.model);
    }

    pub(crate) fn record_recent_model(&mut self, spec: &str) {
        let recents = maki_storage::model::push_recent(&self.storage, spec)
            .into_iter()
            .filter(|spec| self.model_policy.allows(spec))
            .collect();
        self.model_picker.set_recents(recents);
    }

    pub(crate) fn flash(&mut self, msg: String) {
        self.status_bar.flash(msg);
    }

    pub(crate) fn fire_session_autocmd(&self, event: &str, mut data: serde_json::Value) {
        if let Some(map) = data.as_object_mut() {
            map.insert(
                "session_id".into(),
                serde_json::Value::String(self.state.session.id.to_string()),
            );
        }
        self.lua_event_handle.fire_autocmd(event, data);
    }

    pub(crate) fn set_thinking(&mut self, input: &str) -> Result<ThinkingConfig, String> {
        if !self.state.model.supports_thinking() {
            return Err(THINKING_UNSUPPORTED_MSG.into());
        }
        self.state.thinking =
            ThinkingConfig::parse(input.trim(), self.state.thinking).map_err(str::to_owned)?;
        Ok(self.state.thinking)
    }

    pub(crate) fn set_fast(&mut self, fast: bool) -> Result<(), String> {
        if fast && !self.state.model.supports_fast() {
            return Err(FAST_UNSUPPORTED_MSG.into());
        }
        self.state.fast = fast;
        Ok(())
    }

    pub(crate) fn model_state(&self) -> serde_json::Value {
        let model = &self.state.model;
        serde_json::json!({
            "spec": model.spec(), "id": model.id, "provider": model.provider.to_string(),
            "thinking": self.state.thinking.to_string(), "fast": self.state.fast,
            "supports_thinking": model.supports_thinking(), "supports_fast": model.supports_fast(),
        })
    }

    pub(crate) fn attention(&self) -> Option<Notification> {
        if self.permission_active() {
            let tool = self
                .permission_prompt
                .tool()
                .filter(|t| !matches!(t, ToolKey::Wildcard))
                .and_then(|t| normalize_preview(&t.to_string()));
            return Some(Notification::PermissionRequested { tool });
        }
        if matches!(self.pending_input, PendingInput::AuthRetry { .. }) {
            return Some(Notification::AuthenticationRequired);
        }
        if self.status != Status::Streaming && self.plan_form_active() {
            return Some(Notification::PlanReady);
        }
        self.question_active()
            .then_some(Notification::QuestionRequested)
    }

    pub fn tick_error_expiry(&mut self) -> Dirty {
        if !self.status.is_error_expired() {
            return Dirty::NO;
        }
        self.status = Status::Idle;
        Dirty::YES
    }

    pub fn poll_login_picker(&mut self) -> Vec<Action> {
        match self.login_picker.poll_codex() {
            Some(action) => self.login_picker_actions(action),
            None => Vec::new(),
        }
    }

    fn login_picker_actions(&self, action: LoginPickerAction) -> Vec<Action> {
        match action {
            LoginPickerAction::Consumed | LoginPickerAction::Close => vec![],
            LoginPickerAction::Authenticated { model_spec } => {
                vec![Action::ChangeModel(model_spec), Action::RefreshModels]
            }
            LoginPickerAction::Configured { slug } => {
                vec![Action::RefreshProvider { slug }, Action::RefreshModels]
            }
        }
    }

    fn active_chat(&mut self) -> &mut Chat {
        &mut self.chats[self.active_chat]
    }

    pub(crate) fn win_view(&self) -> WinView {
        self.chats[self.active_chat].win_view()
    }

    pub(crate) fn set_scroll_top(&mut self, top: u16) {
        self.active_chat().set_scroll_top(top);
    }

    fn clear_selection_unless_pending_copy(&mut self) {
        if !self
            .selection_state
            .as_ref()
            .is_some_and(|s| s.is_pending_copy())
        {
            self.selection_state = None;
        }
    }

    pub fn update(&mut self, msg: Msg) -> Vec<Action> {
        let actions = match msg {
            Msg::Key(key) => {
                self.last_input = Some(Instant::now());
                self.handle_key(key)
            }
            Msg::Paste(text) => {
                self.last_input = Some(Instant::now());
                let text = text.replace("\r\n", "\n").replace('\r', "\n");
                if text.is_empty() {
                    if self.is_main_chat() && self.image_paste_rx.is_empty() {
                        self.start_image_paste();
                    }
                } else {
                    let mut any_image = false;
                    if self.is_main_chat() {
                        for line in text.lines() {
                            if let Some((path, mt)) = image::try_parse_image_path(line) {
                                self.start_file_image_paste(path, mt);
                                any_image = true;
                            }
                        }
                    }
                    if !any_image {
                        self.route_text_paste(&text);
                    }
                }
                vec![]
            }
            Msg::Mouse(event) => {
                self.handle_mouse(event);
                vec![]
            }
            Msg::Scroll { column, row, delta } => {
                self.handle_scroll(column, row, delta);
                vec![]
            }
            Msg::Agent(envelope) => self.handle_agent_event(*envelope),
        };
        // A modal-closing key or an answered permission yields the next demand
        // immediately, rather than waiting for the next 100ms tick.
        let _ = self.promote_deferred_if_ready();
        actions
    }

    fn send_answer(&self, answer: String) {
        if let Some(tx) = &self.answer_tx {
            let _ = tx.try_send(answer);
        }
    }

    fn send_to_agent(&self, subagent_id: Option<&str>, answer: String) {
        let routed = subagent_id
            .and_then(|id| self.subagent_channels.get(id))
            .and_then(|c| c.answer_tx.as_ref());
        if let Some(tx) = routed {
            let _ = tx.try_send(answer);
        } else {
            self.send_answer(answer);
        }
    }

    fn scroll_at(&mut self, column: u16, row: u16, delta: i32) -> Option<SelectionZone> {
        if self.btw_modal.is_open() {
            self.btw_modal.scroll(delta);
            return None;
        }
        if self.help_modal.is_open() {
            self.help_modal.scroll(delta);
            return None;
        }
        if self.usage_modal.is_open() {
            self.usage_modal.scroll(delta);
            return None;
        }
        let pos = Position::new(column, row);
        if self.float_mgr.is_focused() && self.float_mgr.contains(pos) {
            self.float_mgr.scroll(delta);
            return None;
        }
        macro_rules! try_picker {
            ($picker:expr) => {
                if $picker.is_open() {
                    if $picker.contains(pos) {
                        $picker.scroll(delta);
                    }
                    return None;
                }
            };
        }
        try_picker!(self.rewind_picker);
        try_picker!(self.task_picker);
        try_picker!(self.model_picker);
        try_picker!(self.file_picker);
        let zone = self.zone_at(row, column)?.zone;
        self.scroll_zone(zone, delta);
        Some(zone)
    }

    fn task_entries(&self) -> Vec<TaskEntry> {
        let mut entries: Vec<TaskEntry> = self
            .chats
            .iter()
            .enumerate()
            .map(|(chat_index, chat)| TaskEntry {
                name: chat.name.clone(),
                finished: (chat_index > 0).then_some(chat.is_finished()),
                chat_index,
                snippet: if chat_index == 0 {
                    String::new()
                } else {
                    truncate_snippet(chat.last_message_text())
                },
                context: if chat_index > 0 && chat.context_size > 0 {
                    format_tokens(chat.context_size)
                } else {
                    String::new()
                },
                started_at: (chat_index > 0).then_some(chat.started_at()).flatten(),
            })
            .collect();
        entries[1..].sort_by(|a, b| {
            a.is_finished()
                .cmp(&b.is_finished())
                .then_with(|| b.started_at.cmp(&a.started_at))
        });
        entries
    }

    fn open_tasks(&mut self) {
        self.task_picker_original = Some(self.active_chat);
        self.task_picker.open(self.task_entries(), " Tasks ");
        self.task_picker
            .select_item_by(|entry| entry.chat_index == self.active_chat);
    }

    fn sync_task_picker(&mut self) {
        if !self.task_picker.is_open() {
            return;
        }
        let selected = self
            .task_picker
            .selected_item()
            .map(|entry| entry.chat_index);
        self.task_picker.replace_items(self.task_entries());
        if let Some(chat_index) = selected {
            self.task_picker
                .select_item_by(|entry| entry.chat_index == chat_index);
        }
    }

    fn sync_command_arguments(&mut self, input: &str, cursor: usize) {
        if self
            .command_palette
            .sync_arguments(input, cursor, &self.state.mode.id_key())
        {
            self.command_runtime
                .finish_theme_preview(self.command_target.id(), false);
        }
    }

    fn close_command_palette(&mut self) {
        if self.command_palette.has_accepted_argument() {
            self.command_runtime
                .finish_theme_preview(self.command_target.id(), false);
        }
        self.command_palette.close();
    }

    fn rotate_command_target(&mut self) {
        self.command_runtime
            .finish_theme_preview(self.command_target.id(), false);
        self.command_palette.close();
        self.command_target = self.command_runtime.bind_target();
        self.command_palette = CommandPalette::new(
            self.command_runtime.registry.clone(),
            self.command_target.clone(),
        );
    }

    fn handle_ctrl(&mut self, key: KeyEvent) -> Option<Vec<Action>> {
        if !is_ctrl(&key) {
            return None;
        }
        if key::QUIT.matches(key) {
            self.close_command_palette();
            self.sync_file_completion();
            return Some(if !self.is_main_chat() || self.input_box.is_empty() {
                if self.status == Status::Streaming {
                    return Some(self.handle_cancel());
                }
                self.quit()
            } else {
                self.input_box.discard();
                vec![]
            });
        }
        if key::HELP.matches(key) {
            return Some(self.run_builtin(BuiltinAction::Help));
        }
        if key::TASKS.matches(key) {
            return Some(self.run_builtin(BuiltinAction::Tasks));
        }
        if key::SCROLL_HALF_UP.matches(key) {
            let half = self.chats[self.active_chat].half_page();
            self.active_chat().scroll(half);
            return Some(vec![]);
        }
        if key::SCROLL_HALF_DOWN.matches(key) {
            let half = self.chats[self.active_chat].half_page();
            self.active_chat().scroll(-half);
            return Some(vec![]);
        }
        if key::SCROLL_TOP.matches(key) {
            self.active_chat().scroll_to_top();
            return Some(vec![]);
        }
        if key::SCROLL_BOTTOM.matches(key) {
            self.active_chat().enable_auto_scroll();
            return Some(vec![]);
        }
        None
    }

    fn dispatch_overlay(&mut self, key: KeyEvent) -> Option<Vec<Action>> {
        // Alt+M hides the active permission/ask surface and returns focus to
        // the input box; pressed again while a demand is held it restores it.
        // It is still consumed when neither side of the toggle applies.
        if key::DEFER_INPUT.matches(key) {
            self.toggle_defer_input();
            return Some(vec![]);
        }

        if self.permission_active() {
            if let Some(answer) = self.permission_prompt.handle_key(key) {
                let subagent_id = self.permission_prompt.subagent_id().map(str::to_owned);
                let encoded = answer.encode();
                self.permission_prompt.close();
                self.send_to_agent(subagent_id.as_deref(), encoded);
            }
            return Some(vec![]);
        }

        // plan_form is non-modal: Passthrough falls through to the rest of dispatch
        if self.plan_form_active() {
            let action = self.plan_form.handle_key(key);
            if action != PlanFormAction::Passthrough {
                return Some(self.handle_plan_form_action(action));
            }
        }

        if self.help_modal.is_open() {
            self.help_modal.handle_key(key);
            return Some(vec![]);
        }

        if self.usage_modal.is_open() {
            if key::REFRESH.matches(key) {
                return Some(vec![Action::RefreshUsage]);
            }
            self.usage_modal.handle_key(key);
            return Some(vec![]);
        }

        if self.btw_modal.is_open() {
            self.btw_modal.handle_key(key);
            return Some(vec![]);
        }

        if self.lua_picker.is_open() {
            return Some(match self.lua_picker.handle_key(key) {
                LuaPickerAction::Confirming(msg) => {
                    self.flash(msg);
                    vec![]
                }
                LuaPickerAction::Consumed
                | LuaPickerAction::Choice(..)
                | LuaPickerAction::Delete(..)
                | LuaPickerAction::Close => vec![],
            });
        }

        if !self.permission_active() && self.float_mgr.handle_key(key) {
            return Some(vec![]);
        }

        if self.search_modal.is_open() {
            match self.search_modal.handle_key(key) {
                SearchAction::Consumed => {
                    let chat = &mut self.chats[self.active_chat];
                    let texts = chat.segment_search_texts();
                    self.search_modal.update_matches(&texts);
                    sync_search_highlight(&self.search_modal, chat);
                }
                SearchAction::Navigate => {
                    sync_search_highlight(&self.search_modal, &mut self.chats[self.active_chat]);
                }
                SearchAction::Select(idx) => {
                    let chat = &mut self.chats[self.active_chat];
                    chat.scroll_to_segment(idx);
                    chat.set_highlight_segment(None);
                    self.search_modal.close();
                }
                SearchAction::Close(saved) => {
                    let chat = &mut self.chats[self.active_chat];
                    chat.set_highlight_segment(None);
                    if let Some((top, auto)) = saved {
                        chat.restore_scroll(top, auto);
                    }
                    self.search_modal.close();
                }
            }
            return Some(vec![]);
        }

        if self.file_picker.is_open() {
            return Some(match self.file_picker.handle_key(key) {
                FilePickerModalAction::Consumed => vec![],
                FilePickerModalAction::Select(path) => {
                    self.file_picker.close();
                    if let InputAction::PaletteSync(val) =
                        self.input_box.handle_paste_with_spaces(&path)
                    {
                        self.command_palette.sync(&val);
                        self.sync_command_arguments(
                            &val,
                            self.input_box.buffer.cursor_byte_offset(),
                        );
                        self.sync_file_completion();
                    }
                    vec![]
                }
                FilePickerModalAction::Close => {
                    self.file_picker.close();
                    vec![]
                }
            });
        }

        if self.queue.focus().is_some() {
            match key.code {
                KeyCode::Up => self.queue.move_focus_up(),
                KeyCode::Down => self.queue.move_focus_down(),
                KeyCode::Enter => {
                    self.queue.remove_focused();
                }
                KeyCode::Esc => self.queue.unfocus(),
                _ if key::QUIT.matches(key) => self.queue.unfocus(),
                _ if key::POP_QUEUE.matches(key) => {
                    self.queue.remove(0);
                }
                _ => {}
            }
            return Some(vec![]);
        }

        if self.task_picker.is_open() {
            if key::TASKS.matches(key) {
                self.task_picker.close();
                return Some(vec![]);
            }
            return Some(match self.task_picker.handle_key(key) {
                PickerAction::Consumed | PickerAction::Toggle(..) | PickerAction::Delete(..) => {
                    vec![]
                }
                PickerAction::Select(entry) => {
                    self.task_picker_original = None;
                    self.active_chat = entry.chat_index;
                    vec![]
                }
                PickerAction::Close => {
                    self.active_chat = self.task_picker_original.take().unwrap_or(0);
                    vec![]
                }
            });
        }

        if self.rewind_picker.is_open() {
            return Some(match self.rewind_picker.handle_key(key) {
                RewindPickerAction::Consumed => vec![],
                RewindPickerAction::Select(entry) => self.rewind_to(entry),
                RewindPickerAction::Close => vec![],
            });
        }

        if self.theme_picker.is_open() {
            return Some(match self.theme_picker.handle_key(key) {
                ThemePickerAction::Consumed => vec![],
                ThemePickerAction::Closed => vec![],
            });
        }

        if self.model_picker.is_open() {
            return Some(match self.model_picker.handle_key(key) {
                ModelPickerAction::Consumed => vec![],
                ModelPickerAction::Select(spec) => {
                    vec![Action::ChangeModel(spec)]
                }
                ModelPickerAction::AssignTier(spec, tier) => {
                    vec![Action::AssignTier(spec, tier)]
                }
                ModelPickerAction::UnassignTier(spec, tier) => {
                    vec![Action::UnassignTier(spec, tier)]
                }
                ModelPickerAction::Close => vec![],
            });
        }

        if self.login_picker.is_open() {
            let action = self.login_picker.handle_key(key);
            return Some(self.login_picker_actions(action));
        }

        if self.mcp_picker.is_open() {
            return Some(match self.mcp_picker.handle_key(key) {
                McpPickerAction::Consumed => vec![],
                McpPickerAction::Toggle {
                    server_name,
                    enabled,
                } => {
                    vec![Action::ToggleMcp(server_name, enabled)]
                }
                McpPickerAction::Close => vec![],
            });
        }

        if key::PLAN_TOGGLE.matches(key) && self.plan_toggle_ready() {
            return Some(self.run_builtin(BuiltinAction::PlanToggle));
        }

        None
    }

    fn plan_toggle_ready(&self) -> bool {
        self.state.mode == Mode::Plan && self.state.plan.is_ready()
    }

    /// True when `plan_submit` is in the active mode's toolset (set by
    /// `mode_plan_override`), flipping the plan auto-hooks on/off.
    fn plan_submit_active(&self) -> bool {
        if self.state.mode != Mode::Plan {
            return false;
        }
        let def = self.state.mode.def(&self.lua_event_handle.mode_registry());
        def.tools
            .as_deref()
            .is_some_and(|tools| tools.iter().any(|t| t.as_str() == PLAN_SUBMIT_TOOL))
    }

    fn submit_plan(&mut self) -> Vec<Action> {
        if self.state.mode != Mode::Plan {
            self.flash(FLASH_NO_PLAN.into());
            return vec![];
        }
        let Some(path) = self.state.plan.path().map(Path::to_path_buf) else {
            self.flash(FLASH_NO_PLAN.into());
            return vec![];
        };
        let content = match std::fs::read_to_string(&path) {
            Ok(c) if !c.trim().is_empty() => c,
            _ => {
                self.flash(FLASH_NO_PLAN_BODY.into());
                return vec![];
            }
        };
        self.state.plan.mark_ready();
        self.plan_form.on_plan_ready();
        self.main_chat()
            .push(DisplayMessage::plan(content, path.display().to_string()));
        vec![]
    }

    /// Single implementation behind both the default keybindings and
    /// `maki.ui.action`, so a Lua rebind can never drift from the
    /// original key's behavior.
    pub(crate) fn run_builtin(&mut self, action: BuiltinAction) -> Vec<Action> {
        match action {
            BuiltinAction::FilePicker => {
                self.file_picker.open(&self.state.session.cwd);
            }
            BuiltinAction::Search => {
                let top = self.chats[self.active_chat].scroll_top();
                let auto = self.chats[self.active_chat].auto_scroll();
                self.search_modal.open(top, auto);
            }
            BuiltinAction::Tasks => {
                if self.task_picker.is_open() {
                    self.task_picker.close();
                } else {
                    self.open_tasks();
                }
            }
            BuiltinAction::Help => self.help_modal.toggle(),
            BuiltinAction::PlanToggle => {
                if self.plan_toggle_ready() {
                    self.plan_form.toggle();
                }
            }
            BuiltinAction::PlanEditor => {
                return match self.state.plan.path() {
                    Some(p) => vec![Action::OpenEditor(p.to_path_buf())],
                    None => {
                        self.flash(FLASH_NO_PLAN.into());
                        vec![]
                    }
                };
            }
            BuiltinAction::PlanSubmit => return self.submit_plan(),
            BuiltinAction::EditInput => return vec![Action::EditInputInEditor],
            BuiltinAction::PopQueue => {
                self.queue.remove(0);
            }
            BuiltinAction::PrevChat => self.active_chat = self.active_chat.saturating_sub(1),
            BuiltinAction::NextChat => {
                self.active_chat = (self.active_chat + 1).min(self.chats.len() - 1);
            }
            BuiltinAction::ModelPicker => {
                self.model_picker.open(&self.state.model.spec());
                return vec![Action::RefreshModels];
            }
        }
        vec![]
    }

    fn handle_key(&mut self, key: KeyEvent) -> Vec<Action> {
        self.clear_selection_unless_pending_copy();

        if key::SUSPEND.matches(key) && cfg!(unix) {
            return vec![Action::Suspend];
        }

        if let Some(actions) = self.dispatch_overlay(key) {
            return actions;
        }

        if !(self.status == Status::Streaming && is_streaming_stop_key(key))
            && self.dispatch_override(key)
        {
            return vec![];
        }

        if let Some(actions) = self.handle_ctrl(key) {
            return actions;
        }

        if !self.is_main_chat() {
            match key.code {
                KeyCode::Tab if !self.is_bash_input() => return self.toggle_mode(),
                KeyCode::Esc if !self.chats[self.active_chat].is_finished() => {
                    return if let Some(t) = self.last_esc.take()
                        && t.elapsed() < self.status_bar.flash_duration
                    {
                        self.handle_subagent_cancel()
                    } else {
                        self.last_esc = Some(Instant::now());
                        self.status_bar.flash(FLASH_CANCEL.into());
                        vec![]
                    };
                }
                _ => {}
            }
            return self.handle_subagent_chat_key(key);
        }

        self.handle_main_chat_key(key)
    }

    fn dispatch_override(&self, key: KeyEvent) -> bool {
        self.dispatch_keymap(key.code, key.modifiers)
    }

    fn dispatch_keymap(&self, key: KeyCode, modifiers: KeyModifiers) -> bool {
        self.keymap_reader.load().entries.iter().any(|entry| {
            entry.key == key
                && entry.modifiers == modifiers
                && self.lua_event_handle.run_keybind_callback(entry.id)
        })
    }

    fn handle_main_chat_key(&mut self, key: KeyEvent) -> Vec<Action> {
        if key::EDIT_INPUT.matches(key) {
            return self.run_builtin(BuiltinAction::EditInput);
        }
        if key::MODEL_PICKER.matches(key) {
            return self.run_builtin(BuiltinAction::ModelPicker);
        }
        if is_ctrl(&key) {
            if key::POP_QUEUE.matches(key) {
                return self.run_builtin(BuiltinAction::PopQueue);
            } else if key::OPEN_EDITOR.matches(key) {
                return self.run_builtin(BuiltinAction::PlanEditor);
            } else if key::SEARCH.matches(key) {
                return self.run_builtin(BuiltinAction::Search);
            } else if key::FILE_PICKER.matches(key) {
                return self.run_builtin(BuiltinAction::FilePicker);
            } else if key.code == KeyCode::Char('v') && self.image_paste_rx.is_empty() {
                self.start_image_paste();
            } else if let InputAction::PaletteSync(val) = self.input_box.handle_key(key) {
                self.command_palette.sync(&val);
                self.sync_command_arguments(&val, self.input_box.buffer.cursor_byte_offset());
                self.sync_file_completion();
            }
            return vec![];
        }

        match self
            .command_palette
            .handle_key(key, &self.input_box.buffer.value())
        {
            CommandAction::Consumed => {
                if key.code == KeyCode::Esc {
                    self.sync_file_completion();
                }
                return vec![];
            }
            CommandAction::SelectionChanged => {
                let input = self.input_box.buffer.value();
                self.sync_command_arguments(&input, self.input_box.buffer.cursor_byte_offset());
                return vec![];
            }
            CommandAction::Execute(cmd) => {
                let attachments = self
                    .input_box
                    .take_pending_images()
                    .into_iter()
                    .map(|image| CommandAttachment {
                        media_type: Arc::from(image.media_type.mime()),
                        data: image.data,
                    })
                    .collect();
                self.input_box.discard();
                self.file_completion.close();
                return self
                    .dispatch_confirmed_command(
                        cmd,
                        CommandContent {
                            text: Arc::from(""),
                            attachments,
                        },
                        0,
                    )
                    .unwrap_or_else(|error| {
                        self.flash(error);
                        Vec::new()
                    });
            }
            CommandAction::AcceptArgument { text, cursor } => {
                self.command_palette.sync(&text);
                self.refresh_at_ref_labels(&text);
                self.input_box.set_input(text.clone());
                self.input_box.buffer.set_cursor_byte_offset(cursor);
                return vec![];
            }
            CommandAction::Complete { text, cursor } => {
                self.command_palette.sync(&text);
                self.refresh_at_ref_labels(&text);
                self.input_box.set_input(text.clone());
                self.input_box.buffer.set_cursor_byte_offset(cursor);
                self.sync_command_arguments(&text, cursor);
                return vec![];
            }
            CommandAction::Passthrough => {}
        }

        if self.file_completion.is_active() {
            match self.file_completion.handle_key(key) {
                CompletionAction::Consumed => return vec![],
                CompletionAction::Close => {
                    self.file_completion.close();
                    return vec![];
                }
                CompletionAction::Select(item) => {
                    self.insert_completion(item);
                    return vec![];
                }
                CompletionAction::Passthrough => {}
            }
        }

        let streaming = self.status == Status::Streaming;
        match self.input_box.handle_key(key) {
            InputAction::Submit(sub) => {
                self.file_completion.close();
                self.handle_submit(sub)
            }
            InputAction::PaletteSync(val) => {
                self.command_palette.sync(&val);
                self.sync_command_arguments(&val, self.input_box.buffer.cursor_byte_offset());
                self.sync_file_completion();
                vec![]
            }
            InputAction::CursorMoved => {
                let val = self.input_box.buffer.value();
                self.command_palette.sync_arguments(
                    &val,
                    self.input_box.buffer.cursor_byte_offset(),
                    &self.state.mode.id_key(),
                );
                self.sync_file_completion();
                vec![]
            }
            InputAction::Passthrough(key) => {
                if key.code != KeyCode::Esc {
                    self.last_esc = None;
                }
                match key.code {
                    KeyCode::Up if streaming => {
                        self.active_chat().scroll(1);
                        vec![]
                    }
                    KeyCode::Down if streaming => {
                        self.active_chat().scroll(-1);
                        vec![]
                    }
                    KeyCode::Tab if !self.is_bash_input() => self.toggle_mode(),
                    KeyCode::Esc => {
                        if let Some(t) = self.last_esc.take()
                            && t.elapsed() < self.status_bar.flash_duration
                        {
                            if streaming {
                                self.handle_cancel()
                            } else {
                                self.open_rewind_picker()
                            }
                        } else {
                            self.last_esc = Some(Instant::now());
                            self.status_bar.flash(
                                if streaming {
                                    FLASH_CANCEL
                                } else {
                                    FLASH_REWIND
                                }
                                .into(),
                            );
                            vec![]
                        }
                    }
                    _ => vec![],
                }
            }
            InputAction::ContinueLine | InputAction::None => vec![],
        }
    }

    /// Store the labels the completion sources currently offer in the input,
    /// replacing any previous set: the sources' full item list *is* the known
    /// set for the current mode and models, so the input can style `@`-tokens
    /// as detected or undetected.
    fn store_at_ref_labels(&mut self, items: &[ItemSpec]) {
        self.input_box.at_ref_labels = items.iter().map(|i| i.label.clone()).collect();
    }

    /// The context completion sources receive: the current mode id and the
    /// available-models list, already loaded.
    fn completion_ctx(&self) -> CompletionCtx {
        CompletionCtx {
            mode: self.state.mode.id_key(),
            models: self
                .available_models
                .load_full()
                .map(|arc| (*arc).clone())
                .unwrap_or_default(),
        }
    }

    /// Refresh the input's known `@`-label set after external text (restore,
    /// rewind, command completion, editor edit) may have put a token in the
    /// input. File references resolve by path existence, so the Lua round-trip
    /// is skipped unless the text has a tagged `@`-token.
    pub(crate) fn refresh_at_ref_labels(&mut self, text: &str) {
        if !maki_lua::parse_at_tokens(text)
            .iter()
            .any(|t| !t.prefix.is_empty())
        {
            return;
        }
        let items = self
            .lua_event_handle
            .collect_completion_items(&self.completion_ctx());
        self.store_at_ref_labels(&items);
    }

    /// Opens, refreshes, or closes the `@` completion popup to match the token
    /// under the input cursor. Suppressed while the command palette or an
    /// overlay owns the screen.
    fn sync_file_completion(&mut self) {
        if self.status == Status::Streaming {
            self.file_completion.close();
            return;
        }
        let range = {
            let buf = &self.input_box.buffer;
            at_token_range(&buf.lines()[buf.y()], buf.x())
        };
        let Some((start, end)) = range else {
            self.file_completion.close();
            return;
        };
        if self.command_palette.is_active() || self.any_overlay_open() {
            self.file_completion.close();
            return;
        }

        let cwd = self.state.session.cwd.clone();
        let query = {
            let line = &self.input_box.buffer.lines()[self.input_box.buffer.y()];
            line[start + 1..end].to_string()
        };
        self.file_completion.set_token_byte_range((start, end));
        if self.file_completion.is_active() {
            self.file_completion.sync_query(&query);
        } else {
            let items = self
                .lua_event_handle
                .collect_completion_items(&self.completion_ctx());
            self.store_at_ref_labels(&items);
            self.file_completion.open(&cwd, items, &query, (start, end));
        }
    }

    /// Replaces the `@`-token with the chosen completion and drops the popup.
    fn insert_completion(&mut self, item: CompletionItem) {
        let replacement = item.replacement();
        let (start, end) = self.file_completion.token_byte_range();
        self.file_completion.close();
        self.input_box
            .buffer
            .replace_range_on_current_line(start, end, &replacement);
        let val = self.input_box.buffer.value();
        self.command_palette.sync(&val);
        self.sync_command_arguments(&val, self.input_box.buffer.cursor_byte_offset());
    }

    /// Typing path for a focused subagent tab: characters go into the shared
    /// input box, and Enter submits to that subagent's driver queue. Tab toggles
    /// mode and Esc cancels the subagent, both handled in `handle_key`.
    fn handle_subagent_chat_key(&mut self, key: KeyEvent) -> Vec<Action> {
        match self.input_box.handle_key(key) {
            InputAction::Submit(sub) if !sub.is_empty() => {
                self.submit_released = true;
                self.submit_or_queue(sub.into())
            }
            _ => vec![],
        }
    }

    fn quit(&mut self) -> Vec<Action> {
        self.quit_with(ExitRequest::Success)
    }

    fn quit_with(&mut self, req: ExitRequest) -> Vec<Action> {
        self.save_input_history();
        self.exit_request = req;
        vec![Action::ManualExit]
    }

    pub(crate) fn clear_exit_request(&mut self) {
        self.exit_request = ExitRequest::None;
    }

    pub(crate) fn handle_submit(&mut self, sub: Submission) -> Vec<Action> {
        // Any main-input submit releases a manual Alt+M hold, so the deferred
        // panel re-promotes once the user has placed their message.
        self.submit_released = true;
        match std::mem::take(&mut self.pending_input) {
            PendingInput::AuthRetry { subagent_id } => {
                self.send_to_agent(subagent_id.as_deref(), String::new());
                return vec![];
            }
            PendingInput::None => {}
        }
        if sub.is_empty() {
            return vec![];
        }
        if sub.text.trim() == "exit" {
            return self.quit();
        }

        if let Some(prefix) = shell::parse_shell_prefix(&sub.text) {
            let cmd = prefix.command.trim();
            if cmd == "cd" || cmd.starts_with("cd ") {
                self.flash("Only /cd can change the working directory".into());
            }
            let id = self.shell.reserve_id();
            let sigil = if prefix.visible { "!" } else { "!!" };
            let display = format!("{sigil} {}", prefix.command);
            self.main_chat().show_user_message(display);
            return vec![Action::ShellCommand {
                id,
                command: prefix.command,
                visible: prefix.visible,
            }];
        }
        self.submit_or_queue(sub.into())
    }

    fn handle_cancel(&mut self) -> Vec<Action> {
        let cancelled_run = self.run_id;
        self.run_id += 1;
        self.retry_info = None;
        self.close_all_overlays();
        self.pending_input = PendingInput::None;
        self.input_queue.clear();
        self.active_input = None;
        self.finish_subagents(DisplayRole::Error, CANCELLED_TEXT);
        self.subagent_channels.clear();
        self.shell.cancel_all();
        for chat in &mut self.chats {
            chat.flush();
            chat.cancel_in_progress();
        }
        self.main_chat()
            .push(DisplayMessage::new(DisplayRole::Error, CANCEL_MSG.into()));
        self.queue.clear();
        self.recoverable_queue.clear();
        self.status = Status::Idle;
        vec![Action::CancelAgent {
            run_id: cancelled_run,
        }]
    }

    fn handle_subagent_cancel(&mut self) -> Vec<Action> {
        let tool_use_id = self
            .chat_index
            .iter()
            .find(|&(_, &idx)| idx == self.active_chat)
            .map(|(id, _)| id.clone());

        let Some(tool_use_id) = tool_use_id else {
            return vec![];
        };

        self.chats[self.active_chat].flush();
        self.chats[self.active_chat].cancel_in_progress();
        self.chats[self.active_chat].mark_finished(DisplayRole::Error, CANCELLED_TEXT);
        self.subagent_channels.remove(&tool_use_id);

        vec![Action::CancelSubagent { tool_use_id }]
    }

    fn handle_agent_event(&mut self, envelope: Envelope) -> Vec<Action> {
        if envelope.run_id == RESTORE_RUN_ID {
            let (id, snapshot, theme_gen, is_header) = match envelope.event {
                AgentEvent::ToolSnapshot {
                    id,
                    snapshot,
                    theme_gen,
                } => (id, snapshot, theme_gen, false),
                AgentEvent::ToolHeaderSnapshot {
                    id,
                    snapshot,
                    theme_gen,
                } => (id, snapshot, theme_gen, true),
                _ => return vec![],
            };
            for chat in &mut self.chats {
                if is_header {
                    chat.tool_header_snapshot(&id, snapshot.clone(), theme_gen);
                } else {
                    chat.tool_snapshot(&id, snapshot.clone(), theme_gen);
                }
            }
            return vec![];
        }
        if envelope.run_id != self.run_id {
            // A snapshot dropped here degrades the tool body to llm_output.
            if let AgentEvent::ToolSnapshot { id, .. }
            | AgentEvent::ToolHeaderSnapshot { id, .. }
            | AgentEvent::LiveToolBuf { id, .. } = &envelope.event
            {
                tracing::debug!(
                    tool_id = %id,
                    event_run_id = envelope.run_id,
                    current_run_id = self.run_id,
                    "tool render event dropped: stale run_id"
                );
            }
            return vec![];
        }

        if let AgentEvent::SubagentHistory {
            tool_use_id,
            messages,
        } = envelope.event
        {
            // Workflow sessions use synthetic ids that no ToolDone will match,
            // so we finish them here on SubagentHistory.
            if let Some(&sub_idx) = self.chat_index.get(tool_use_id.as_str()) {
                self.chats[sub_idx].mark_finished(DisplayRole::Done, DONE_TEXT);
            }
            // An async subagent's reply is delivered to the main agent so it
            // becomes a new turn; one-shot subagents already see their reply as
            // the tool result, so only async (input_tx-named) subtasks queue it.
            let reply = self
                .subagent_channels
                .get(&tool_use_id)
                .filter(|c| c.input_tx.is_some())
                .and_then(|_| terminal_reply(&messages))
                .filter(|r| !r.is_empty());
            if let Some(reply) = reply {
                // Header the reply with the task id so it reads as subagent
                // output rather than a message typed by the user.
                let text =
                    format!("{SUBAGENT_REPLY_HEADER}{tool_use_id}{SUBAGENT_REPLY_SUFFIX}{reply}");
                self.queue_and_notify(QueuedMessage {
                    text,
                    images: Vec::new(),
                });
            }
            self.sync_task_picker();
            self.state
                .session_mut()
                .set_subagent_messages(tool_use_id, messages);
            return vec![];
        }

        match &envelope.event {
            AgentEvent::ToolStart(event) => self.fire_session_autocmd(
                "ToolStart",
                serde_json::json!({
                    "tool_id": event.id,
                    "tool": event.tool,
                }),
            ),
            AgentEvent::ToolDone(event) => self.fire_session_autocmd(
                "ToolDone",
                serde_json::json!({
                    "tool_id": event.id,
                    "tool": event.tool,
                }),
            ),
            _ => {}
        }

        let subagent_id = envelope
            .subagent
            .as_ref()
            .map(|s| s.parent_tool_use_id.clone());

        let chat_idx = match envelope.subagent {
            Some(ref subagent) => self.resolve_or_create_chat(subagent),
            None => 0,
        };

        if let AgentEvent::ToolDone(ref e) = envelope.event {
            if self.state.mode == Mode::Plan
                && !self.plan_submit_active()
                && self.state.plan.path().is_some_and(|pp| e.wrote_to(pp))
            {
                self.transition_plan(PlanTrigger::WriteDone);
            }
            self.state
                .session_mut()
                .insert_tool_output(e.id.clone(), e.output.clone());
            if let Some(&sub_idx) = self.chat_index.get(&e.id) {
                let (role, text) = if e.is_error {
                    (DisplayRole::Error, ERROR_TEXT)
                } else {
                    (DisplayRole::Done, DONE_TEXT)
                };
                self.chats[sub_idx].mark_finished(role, text);
            }
            self.sync_task_picker();
        }

        if let AgentEvent::Retry {
            attempt,
            message,
            delay_ms,
        } = envelope.event
        {
            self.chats[chat_idx].stream_reset();
            if chat_idx == 0 {
                self.retry_info = Some(RetryInfo {
                    attempt,
                    message,
                    deadline: Instant::now() + Duration::from_millis(delay_ms),
                });
            }
            return vec![];
        }

        self.retry_info = None;

        if let AgentEvent::TurnComplete(ref tc) = envelope.event {
            self.state.token_usage += tc.usage;
            add_cost(&mut self.chats[chat_idx].cost, tc.cost);
            self.state
                .session_mut()
                .add_model_usage(&tc.model, tc.usage.billed(tc.cost));
            let ctx_size = tc.context_size.unwrap_or_else(|| tc.usage.context_tokens());
            self.chats[chat_idx].context_size = ctx_size;
            if chat_idx == 0 {
                self.state.context_size = ctx_size;
            }
            self.chats[chat_idx].set_pending_turn_usage(tc.usage.format(tc.cost));
            if let Some(tool_id) = &subagent_id {
                let formatted = tc.usage.format_sum_cost(self.chats[chat_idx].cost);
                self.chats[0].set_tool_turn_usage(tool_id, formatted);
            }
        }

        let plan_path = if self.state.mode == Mode::Plan && !self.plan_submit_active() {
            self.state.plan.path()
        } else {
            None
        };
        let result = self.chats[chat_idx].handle_event(envelope.event, plan_path);

        if let ChatEventResult::QueueItemConsumed { text, image_count } = result {
            if chat_idx == 0 {
                self.on_queue_item_consumed(&text, image_count);
            }
            return vec![];
        }

        if let ChatEventResult::PermissionRequest { id, tool, scopes } = result {
            let demand = InputDemand {
                kind: InputKind::Permission,
                blocked_by_modal: self.has_blocking_modal(),
                hold_until_submit: false,
                perm: Some(PermissionPayload {
                    id,
                    tool,
                    scopes,
                    subagent_id,
                }),
            };
            let defer = self.begin_input_demand(demand);
            if !defer && self.ui_config.bell.permission {
                self.pending_bell = true;
            }
            return vec![];
        }

        if let ChatEventResult::AuthRequired = result {
            self.chats[chat_idx].push(DisplayMessage::new(
                DisplayRole::Error,
                AUTH_EXPIRED_MSG.into(),
            ));
            if chat_idx != 0 {
                self.main_chat().push(DisplayMessage::new(
                    DisplayRole::Error,
                    AUTH_EXPIRED_MSG.into(),
                ));
            }
            self.pending_input = PendingInput::AuthRetry { subagent_id };
            return vec![];
        }

        let mut actions = Vec::new();
        if chat_idx == 0 {
            match result {
                ChatEventResult::Done => {
                    self.status_bar.clear_flash();
                    self.terminalize_turn(MISSING_TOOL_COMPLETION);
                    self.chat_index.clear();
                    self.subagent_channels.clear();
                    self.status = Status::Idle;
                    self.fire_session_autocmd("TurnEnd", serde_json::json!({}));
                    if self.exit_on_done {
                        self.exit_request = ExitRequest::Success;
                    }
                    if self.ui_config.bell.turn_complete {
                        actions.push(Action::Bell);
                    }
                }
                ChatEventResult::Error(message) => {
                    self.status = Status::error(message.clone());
                    self.status_bar.clear_flash();
                    self.subagent_channels.clear();
                    self.terminalize_turn(&message);
                    self.recoverable_queue = self.queue.text_messages();
                    self.queue.clear();
                    self.chat_index.clear();
                    self.fire_session_autocmd(
                        "TurnError",
                        serde_json::json!({ "message": message }),
                    );
                    if self.exit_on_done {
                        self.exit_request = ExitRequest::Error;
                    }
                }
                ChatEventResult::AuthRequired
                | ChatEventResult::PermissionRequest { .. }
                | ChatEventResult::QueueItemConsumed { .. } => unreachable!(),
                ChatEventResult::Continue => {}
            }
        }
        actions
    }

    /// Shared `UiAction::OpenWin` path. Returns `true` when the window went
    /// active immediately, `false` when an input demand was queued. Non-input
    /// windows (tool output, panels, centered popups) always return `true`
    /// and keep their original focus.
    pub(crate) fn handle_open_win(
        &mut self,
        buf: Arc<SharedBuf>,
        config: FloatConfig,
        focus: bool,
        event_tx: flume::Sender<WinEvent>,
        cmd_rx: flume::Receiver<WinCommand>,
    ) -> bool {
        let is_input_demand = config.needs_input && config.split == Split::Below;
        let defer = is_input_demand
            && self.begin_input_demand(InputDemand {
                kind: InputKind::Question,
                blocked_by_modal: self.has_blocking_modal(),
                hold_until_submit: false,
                perm: None,
            });
        let open_focus = if is_input_demand { !defer } else { focus };
        self.float_mgr
            .open(buf, config, open_focus, event_tx, cmd_rx);
        if is_input_demand && !defer {
            self.transition_plan(PlanTrigger::InteractivePrompt);
            if self.ui_config.bell.ask {
                self.pending_bell = true;
            }
        }
        !defer
    }

    fn resolve_or_create_chat(&mut self, subagent: &SubagentInfo) -> usize {
        let id = &subagent.parent_tool_use_id;
        if let Some(&idx) = self.chat_index.get(id.as_str()) {
            return idx;
        }
        let idx = self.chats.len();
        self.chat_index.insert(id.clone(), idx);
        self.subagent_channels.insert(
            id.clone(),
            SubagentChannels {
                answer_tx: subagent.answer_tx.clone(),
                input_tx: subagent.input_tx.clone(),
            },
        );
        self.chats[0].update_tool_summary(id, &subagent.name);
        if let Some(ref model) = subagent.model {
            self.chats[0].update_tool_model(id, model);
        }
        let mut chat = Chat::new(
            subagent.name.clone(),
            self.ui_config.clone(),
            self.lua_event_handle.clone(),
            Arc::clone(&self.theme_provider),
        );
        chat.set_restore_channel(self.restore_event_tx.clone());
        chat.model_id = subagent.model.clone();
        chat.subagent_id = Some(id.clone());
        chat.set_started_at_now();
        if let Some(ref prompt) = subagent.prompt {
            chat.push_user_message(prompt);
        }
        self.chats.push(chat);
        self.sync_task_picker();
        self.sync_subagents();
        idx
    }

    /// Resolves and enqueues `cmdline` against this session. The caller only
    /// waits on resolution; the command's effects are applied by the event
    /// loop on a later iteration, so state inspected immediately after this
    /// call may predate them.
    pub(crate) fn run_cmdline(&mut self, cmdline: &str, depth: u8) -> Result<Vec<Action>, String> {
        let trimmed = cmdline.trim();
        let input = format!("/{}", trimmed.trim_start_matches('/'));
        let resolved = self
            .command_runtime
            .registry
            .resolve_input_for(&self.command_target, &input)
            .map_err(|error| match error {
                maki_commands::ResolutionError::UnknownCommand(name) => {
                    format!("unknown command '{name}'")
                }
                maki_commands::ResolutionError::StaleTarget => error.to_string(),
            })?;
        self.command_runtime.dispatch_command(
            &self.command_target,
            resolved.command,
            resolved.arguments,
            CommandContent::default(),
            depth.into(),
        );
        #[cfg(test)]
        return Ok(self.execute_pending_commands());
        #[cfg(not(test))]
        Ok(Vec::new())
    }

    #[cfg(test)]
    fn execute_command(&mut self, command: ParsedCommand, depth: u8) -> Vec<Action> {
        self.run_cmdline(&format!("{} {}", command.name, command.args), depth)
            .unwrap_or_default()
    }

    fn dispatch_confirmed_command(
        &mut self,
        command: ConfirmedCommand,
        content: CommandContent,
        depth: u8,
    ) -> Result<Vec<Action>, String> {
        self.command_runtime.dispatch_command(
            &self.command_target,
            command.command,
            Arc::from(command.args),
            content,
            depth.into(),
        );
        #[cfg(test)]
        return Ok(self.execute_pending_commands());
        #[cfg(not(test))]
        Ok(Vec::new())
    }

    #[cfg(test)]
    fn execute_pending_commands(&mut self) -> Vec<Action> {
        let mut actions = Vec::new();
        loop {
            let Some(event) = self.command_runtime.recv_for_test() else {
                break;
            };
            match event {
                crate::command_runtime::CommandEvent::Host { request, reply, .. } => {
                    match self.execute_host_request(request) {
                        Ok((response, host_actions)) => {
                            actions.extend(host_actions);
                            let _ = reply.send(Ok(response));
                        }
                        Err(error) => {
                            let _ = reply.send(Err(error));
                        }
                    }
                }
                crate::command_runtime::CommandEvent::Outcome { outcome, .. } => match outcome {
                    maki_commands::CommandOutcome::AgentTurn(turn) => {
                        actions.extend(self.submit_command_turn(turn))
                    }
                    maki_commands::CommandOutcome::Failed(error) => self.flash(error.to_string()),
                    maki_commands::CommandOutcome::Completed => break,
                },
            }
        }
        actions
    }

    pub(crate) fn execute_host_request(
        &mut self,
        request: HostRequest,
    ) -> Result<(HostResponse, Vec<Action>), CommandError> {
        let operation = match request {
            HostRequest::Context(request) => {
                let response = match request {
                    HostContextRequest::ModelSpecs => HostContextResponse::Values(
                        self.available_models
                            .load_full()
                            .map(|models| {
                                models
                                    .iter()
                                    .map(|spec| Arc::from(spec.as_str()))
                                    .collect::<Vec<_>>()
                                    .into()
                            })
                            .unwrap_or_else(|| Arc::from([])),
                    ),
                    HostContextRequest::ThemeNames => HostContextResponse::Values(
                        self.theme_provider
                            .names()
                            .into_iter()
                            .map(Arc::from)
                            .collect::<Vec<_>>()
                            .into(),
                    ),
                    HostContextRequest::WorkingDirectory => HostContextResponse::WorkingDirectory(
                        PathBuf::from(&self.state.session.cwd),
                    ),
                    HostContextRequest::ThinkingConfig => {
                        HostContextResponse::ThinkingConfig(command_thinking(self.state.thinking))
                    }
                };
                return Ok((HostResponse::Context(response), vec![]));
            }
            HostRequest::Builtin(operation) => operation,
        };
        let actions = match operation {
            BuiltinOperation::OpenTasks => {
                self.open_tasks();
                vec![]
            }
            BuiltinOperation::Compact => {
                if self.status == Status::Streaming {
                    if !self.queue_compact() {
                        return Err(CommandError::Producer(Arc::from(
                            "agent queue is unavailable",
                        )));
                    }
                    vec![]
                } else {
                    self.status = Status::Streaming;
                    vec![Action::Compact]
                }
            }
            BuiltinOperation::ResetSession => self.reset_session(),
            BuiltinOperation::ToggleHelp => {
                self.help_modal.toggle();
                vec![]
            }
            BuiltinOperation::ToggleUsage => {
                self.usage_modal.toggle();
                if self.usage_modal.is_open() {
                    vec![Action::RefreshUsage]
                } else {
                    vec![]
                }
            }
            BuiltinOperation::FocusQueue => {
                self.queue.set_focus();
                vec![]
            }
            BuiltinOperation::OpenModelPicker => {
                self.model_picker.open(&self.state.model.spec());
                vec![Action::RefreshModels]
            }
            BuiltinOperation::SetModel { spec } => vec![Action::ChangeModel(spec.to_string())],
            BuiltinOperation::OpenThemePicker => {
                self.theme_picker.open();
                self.command_runtime
                    .finish_theme_preview(self.command_target.id(), false);
                vec![]
            }
            BuiltinOperation::SetTheme { name } => {
                let applied = match self.theme_provider.install(&name) {
                    Ok(()) => {
                        self.theme_provider.persist(&name);
                        self.flash(format!("{THEME_APPLIED_PREFIX}: {name}"));
                        true
                    }
                    Err(error) => {
                        self.flash(error);
                        false
                    }
                };
                self.command_runtime
                    .finish_theme_preview(self.command_target.id(), applied);
                vec![]
            }
            BuiltinOperation::OpenMcpPicker => {
                self.mcp_picker.open();
                vec![]
            }
            BuiltinOperation::OpenLoginPicker => {
                self.login_picker.open(self.storage.clone());
                vec![]
            }
            BuiltinOperation::ChangeDirectory { path } => self.change_directory(path),
            BuiltinOperation::QuickQuestion {
                question,
                attachments,
            } => {
                let images = attachments
                    .iter()
                    .filter_map(|attachment| {
                        maki_agent::ImageMediaType::from_mime(&attachment.media_type).map(
                            |media_type| ImageSource::new(media_type, Arc::clone(&attachment.data)),
                        )
                    })
                    .collect();
                vec![Action::Btw(question.to_string(), images)]
            }
            BuiltinOperation::ToggleYolo => {
                let enabled = self.permissions.toggle_yolo();
                self.flash(
                    if enabled {
                        "YOLO mode enabled"
                    } else {
                        "YOLO mode disabled"
                    }
                    .into(),
                );
                vec![]
            }
            BuiltinOperation::SetThinking { config } => {
                if !self.state.model.supports_thinking() {
                    self.flash("Thinking requires a model that supports it".into());
                } else {
                    self.state.thinking = provider_thinking(config);
                    self.flash(format!("Thinking: {}", self.state.thinking));
                }
                vec![]
            }
            BuiltinOperation::ToggleFast => {
                if !self.state.model.supports_fast() {
                    self.flash(FAST_UNSUPPORTED_MSG.into());
                } else {
                    self.state.fast = !self.state.fast;
                    self.flash(
                        if self.state.fast {
                            FAST_ON_MSG
                        } else {
                            FAST_OFF_MSG
                        }
                        .into(),
                    );
                }
                vec![]
            }
            BuiltinOperation::ToggleWorkflow => {
                self.state.workflow = !self.state.workflow;
                self.flash(
                    if self.state.workflow {
                        WORKFLOW_ON_MSG
                    } else {
                        WORKFLOW_OFF_MSG
                    }
                    .into(),
                );
                vec![]
            }
            BuiltinOperation::Exit => self.quit(),
            BuiltinOperation::Reload => self.quit_with(ExitRequest::Reload),
        };
        Ok((HostResponse::Completed, actions))
    }

    pub(crate) fn submit_command_turn(&mut self, turn: AgentTurn) -> Vec<Action> {
        let images = turn
            .content
            .attachments
            .iter()
            .filter_map(|attachment| {
                maki_agent::ImageMediaType::from_mime(&attachment.media_type)
                    .map(|media_type| ImageSource::new(media_type, Arc::clone(&attachment.data)))
            })
            .collect();
        let message = QueuedMessage {
            text: turn.content.text.to_string(),
            images,
        };
        if let Some(prompt) = turn.prompt {
            let display = match self.lua_event_handle.expand_references(&message.text) {
                Ok(text) => text,
                Err(error) => {
                    self.flash(error);
                    return vec![];
                }
            };
            if self.status == Status::Streaming {
                self.flash("Agent is busy, try again later".into());
                return vec![];
            }
            let mut input = self.build_agent_input(&QueuedMessage {
                text: display.clone(),
                images: message.images,
            });
            input.prompt = Some(Box::new(maki_agent::McpPromptRef {
                qualified_name: prompt.qualified_name.to_string(),
                arguments: prompt
                    .arguments
                    .iter()
                    .map(|(name, value)| (name.to_string(), value.to_string()))
                    .collect(),
            }));
            self.start_run(input, display)
        } else {
            match self.submit_prompt(message) {
                SubmitOutcome::Started(actions) => actions,
                SubmitOutcome::Queued => vec![],
                SubmitOutcome::Rejected(error) => {
                    self.flash(error);
                    vec![]
                }
            }
        }
    }

    pub(crate) fn open_startup_session_picker(&self) {
        self.lua_event_handle
            .fire_autocmd(SESSION_PICKER_REQUESTED_EVENT, serde_json::json!({}));
    }

    fn change_directory(&mut self, path: PathBuf) -> Vec<Action> {
        match path.canonicalize().and_then(|path| {
            path.is_dir()
                .then_some(path)
                .ok_or_else(|| std::io::Error::from(std::io::ErrorKind::NotADirectory))
        }) {
            Ok(path) => {
                self.state
                    .session_mut()
                    .set_cwd(path.to_string_lossy().into_owned());
                self.status_bar.set_cwd(path.clone());
                self.flash(format!("cd {}", path.display()))
            }
            Err(error) => self.flash(format!("cd: {error}")),
        }
        vec![]
    }

    fn overlays(&self) -> [&dyn Overlay; 14] {
        [
            &self.help_modal,
            &self.usage_modal,
            &self.btw_modal,
            &self.float_mgr,
            &self.search_modal,
            &self.file_picker,
            &self.task_picker,
            &self.lua_picker,
            &self.rewind_picker,
            &self.theme_picker,
            &self.model_picker,
            &self.login_picker,
            &self.mcp_picker,
            &self.permission_prompt,
        ]
    }

    fn overlays_mut(&mut self) -> [&mut dyn Overlay; 14] {
        [
            &mut self.help_modal,
            &mut self.usage_modal,
            &mut self.btw_modal,
            &mut self.float_mgr,
            &mut self.search_modal,
            &mut self.file_picker,
            &mut self.task_picker,
            &mut self.lua_picker,
            &mut self.rewind_picker,
            &mut self.theme_picker,
            &mut self.model_picker,
            &mut self.login_picker,
            &mut self.mcp_picker,
            &mut self.permission_prompt,
        ]
    }

    /// Last user keystroke/paste is recent enough that stealing focus would
    /// interrupt mid-typing.
    pub(crate) fn is_busy(&self) -> bool {
        self.last_input.is_some_and(|t| t.elapsed() < INPUT_DEFER)
    }

    /// A modal overlay (e.g. the `/model` picker) is open. Neither the
    /// permission prompt (non-modal) nor a deferred question float
    /// (`focused_id == None`, so not `Overlay::is_open`) self-counts.
    pub(crate) fn has_blocking_modal(&self) -> bool {
        self.has_modal_overlay()
    }

    /// Clears a stale `active_input` once the active surface closed on its own
    /// (permission answered, question float dismissed). Called before any
    /// promotion check so the queue head can become promotable.
    pub(crate) fn reconcile_active(&mut self) {
        match self.active_input {
            Some(InputKind::Permission) if !self.permission_prompt.is_open() => {
                self.active_input = None;
            }
            Some(InputKind::Question) if !self.float_mgr.below_is_input() => {
                self.active_input = None;
            }
            _ => {}
        }
    }

    pub(crate) fn permission_active(&self) -> bool {
        self.active_input == Some(InputKind::Permission) && self.permission_prompt.is_open()
    }

    pub(crate) fn question_active(&self) -> bool {
        self.active_input == Some(InputKind::Question) && self.float_mgr.below_is_input()
    }

    pub(crate) fn any_input_active(&self) -> bool {
        self.permission_active() || self.question_active()
    }

    /// Time until the deferral window (2s after the last keystroke) elapses.
    pub(crate) fn defer_remaining(&self) -> Duration {
        self.last_input
            .map(|t| INPUT_DEFER.saturating_sub(t.elapsed()))
            .unwrap_or(Duration::ZERO)
    }

    /// The single arbitration entry point. Returns `true` when the demand was
    /// queued (caller must not open/ring/transition); `false` when it went
    /// active immediately. For a Permission the prompt is opened here on the
    /// active path; for a Question the caller owns opening the float.
    pub(crate) fn begin_input_demand(&mut self, demand: InputDemand) -> bool {
        if self.is_busy() || self.any_input_active() || !self.input_queue.is_empty() {
            self.input_queue.push_back(demand);
            return true;
        }
        self.active_input = Some(demand.kind);
        if let Some(perm) = demand.perm {
            self.permission_prompt
                .open(perm.id, perm.tool, perm.scopes, perm.subagent_id);
        }
        false
    }

    /// Pops the queue head and activates it once the user is idle or the modal
    /// that was blocking it has closed. Idempotent; sets `pending_bell` on a
    /// promotion (the event loop drains it).
    pub(crate) fn promote_deferred_if_ready(&mut self) -> Dirty {
        self.reconcile_active();
        if self.any_input_active() {
            return Dirty::NO;
        }
        // A submit arms the release once, even for the auto-idle path; consume
        // it here so it cannot leak into later promotions.
        let released = std::mem::take(&mut self.submit_released);
        while let Some((kind, blocked_by_modal, hold_until_submit)) = self
            .input_queue
            .front()
            .map(|d| (d.kind, d.blocked_by_modal, d.hold_until_submit))
        {
            // A queued Question whose float already closed is stale: drop it.
            if kind == InputKind::Question && !self.float_mgr.below_is_input() {
                self.input_queue.pop_front();
                continue;
            }
            // An Alt+M hold ignores the idle/blocking-modal timers and waits
            // for the user's next submit.
            let ready = if hold_until_submit {
                released
            } else {
                !self.is_busy() || (blocked_by_modal && !self.has_blocking_modal())
            };
            if !ready {
                return Dirty::NO;
            }
            let d = self.input_queue.pop_front().expect("front checked above");
            return self.activate_deferred_input(d);
        }
        Dirty::NO
    }

    fn activate_deferred_input(&mut self, d: InputDemand) -> Dirty {
        self.active_input = Some(d.kind);
        let bell = match d.kind {
            InputKind::Permission => {
                if let Some(perm) = d.perm {
                    self.permission_prompt
                        .open(perm.id, perm.tool, perm.scopes, perm.subagent_id);
                }
                self.ui_config.bell.permission
            }
            InputKind::Question => {
                self.float_mgr.focus_input_window();
                self.transition_plan(PlanTrigger::InteractivePrompt);
                self.ui_config.bell.ask
            }
        };
        if bell {
            self.pending_bell = true;
        }
        Dirty::YES
    }

    /// Alt+M toggles: defer the active input surface, or restore one this key
    /// deferred earlier (a hold-until-submit demand at the queue head).
    pub(crate) fn toggle_defer_input(&mut self) -> bool {
        if self.defer_active_input() {
            return true;
        }
        if !self
            .input_queue
            .front()
            .is_some_and(|d| d.hold_until_submit)
        {
            return false;
        }
        // Arm the submit release so `promote_deferred_if_ready` treats the held
        // head as ready regardless of idle/modal timers.
        self.submit_released = true;
        let _ = self.promote_deferred_if_ready();
        true
    }

    /// Alt+M: hide the active input surface and hold it until the user's next
    /// submit (instead of the 2s idle timer). Returns `false` when nothing was
    /// active. The surface is re-promoted by `promote_deferred_if_ready` once
    /// `submit_released` is armed by a keyboard submit.
    pub(crate) fn defer_active_input(&mut self) -> bool {
        if self.permission_active() {
            let demand = InputDemand {
                kind: InputKind::Permission,
                blocked_by_modal: false,
                hold_until_submit: true,
                perm: Some(self.active_permission_payload()),
            };
            self.permission_prompt.close();
            self.active_input = None;
            self.input_queue.push_back(demand);
            return true;
        }
        if self.question_active() {
            self.float_mgr.release_focus();
            self.active_input = None;
            self.input_queue.push_back(InputDemand {
                kind: InputKind::Question,
                blocked_by_modal: false,
                hold_until_submit: true,
                perm: None,
            });
            return true;
        }
        false
    }

    /// Snapshots the open permission prompt into a queueable payload. Only
    /// called while `permission_active()`, so the prompt is guaranteed open.
    fn active_permission_payload(&self) -> PermissionPayload {
        match &self.permission_prompt {
            PermissionPrompt::Open {
                id,
                tool,
                scopes,
                subagent_id,
                ..
            } => PermissionPayload {
                id: id.clone(),
                tool: tool.clone(),
                scopes: scopes.clone(),
                subagent_id: subagent_id.clone(),
            },
            PermissionPrompt::Closed => unreachable!("permission_active requires an open prompt"),
        }
    }

    /// Drains the bell owed by a promotion/arrival. The event loop rings it.
    pub fn take_pending_bell(&mut self) -> bool {
        std::mem::take(&mut self.pending_bell)
    }

    /// A below-split input window that is *not* the active surface (queued).
    pub(crate) fn below_input_hidden(&self) -> bool {
        self.float_mgr.below_is_input() && !self.question_active()
    }

    /// A manually deferred (Alt+M) input demand is waiting in the queue.
    pub(crate) fn held_input_pending(&self) -> bool {
        self.input_queue.iter().any(|d| d.hold_until_submit)
    }

    pub fn any_overlay_open(&self) -> bool {
        self.overlays().iter().any(|o| o.is_open())
    }

    /// True when the agent is parked on user input. Drives the `needs_input`
    /// session status.
    pub(crate) fn awaiting_input(&self) -> bool {
        self.permission_active()
            || self.pending_input != PendingInput::None
            || self.question_active()
    }

    /// True while `recoverable_queue` holds user text captured at an agent
    /// error; a background run would wipe it (`start_run` clears the queue).
    pub(crate) fn holds_recovery_text(&self) -> bool {
        !self.recoverable_queue.is_empty()
    }

    pub fn has_modal_overlay(&self) -> bool {
        self.overlays().iter().any(|o| o.is_open() && o.is_modal())
    }

    pub fn close_all_overlays(&mut self) {
        self.close_command_palette();
        self.file_completion.close();
        self.overlays_mut().iter_mut().for_each(|o| o.close());
    }

    /// Every poller that feeds the screen, in one place and never in `view`;
    /// see [`crate::repaint`] for why.
    pub fn tick(&mut self) -> Dirty {
        // `|` never short-circuits: every poller must run on every tick.
        let mut dirty = self.float_mgr.tick()
            | self.lua_picker.tick()
            | self.tick_edge_scroll()
            | self.tick_error_expiry()
            | self.poll_image_paste()
            | self.btw_modal.poll()
            | self.status_bar.poll_branch_update()
            | self.status_bar.clear_expired_hint()
            | self.mcp_picker.refresh()
            | self.model_picker.refresh()
            | self.usage_modal.poll(&self.usage_slot)
            | self.usage_readout_watch.poll(self.usage_slot.load_full())
            | self.hints.poll(self.hint_reader.load_full())
            | self.tick_file_picker()
            | self.tick_file_completion()
            | self.command_palette.poll_arguments();
        dirty |= self.tick_chats();
        while let Some(shown) = self.chats[0].take_splash_event() {
            // The autocmd is fire-and-forget; repainting is the frame pull's
            // job (which already reports dirty), so the event itself owes none.
            self.fire_session_autocmd(
                if shown { "SplashShown" } else { "SplashHidden" },
                serde_json::json!({}),
            );
        }
        // `float_mgr.tick` above may have closed the active question float; the
        // promote call reconciles that, then pops the queue head when idle.
        dirty |= self.promote_deferred_if_ready();
        dirty
    }

    fn tick_chats(&mut self) -> Dirty {
        Dirty::any(self.chats.iter_mut().map(Chat::tick))
    }

    fn tick_file_picker(&mut self) -> Dirty {
        let (dirty, flash) = self.file_picker.tick();
        if let Some(flash) = flash {
            self.status_bar.flash(flash);
        }
        dirty
    }

    fn tick_file_completion(&mut self) -> Dirty {
        let (dirty, flash) = self.file_completion.tick();
        if let Some(flash) = flash {
            self.status_bar.flash(flash);
        }
        dirty
    }

    /// What moves with the clock alone; changes that come from arriving data
    /// are reported by [`Self::tick`] instead. Overlays answer as a group, so
    /// adding one to [`Self::overlays`] is enough.
    pub fn cadence(&self) -> Cadence {
        Cadence::any([
            Cadence::any(self.overlays().into_iter().map(Overlay::cadence)),
            StatusBar::cadence(
                &self.status,
                self.restoring.load(Ordering::Relaxed),
                self.retry_info.is_some(),
            ),
            self.selection_state
                .as_ref()
                .map_or(Cadence::IDLE, SelectionState::cadence),
            self.file_completion.cadence(),
            Cadence::any(self.chats.iter().map(Chat::cadence)),
            // Wake precisely at the 2s idle mark to promote a queued demand.
            Cadence::when(
                !self.input_queue.is_empty() && !self.any_input_active(),
                Cadence::after(self.defer_remaining()),
            ),
        ])
    }

    fn finish_subagents(&mut self, role: DisplayRole, text: &str) {
        self.retain_resolved_subagents(role, text);
        self.chat_index.clear();
    }

    /// Terminalizes every tool left in progress when a turn ends, sparing
    /// shell commands that outlive the agent.
    fn terminalize_turn(&mut self, message: &str) {
        self.retain_resolved_subagents(DisplayRole::Error, ERROR_TEXT);
        self.chats[0].fail_in_progress_except(message.into(), self.shell.active_ids());
        for chat in self.chats.iter_mut().skip(1) {
            chat.fail_in_progress_with_message(message.into());
        }
        self.sync_task_picker();
    }

    /// Marks unfinished subagent chats as ended and drops them from
    /// `chat_index`, so the session records only the children that really
    /// completed.
    fn retain_resolved_subagents(&mut self, role: DisplayRole, text: &str) {
        self.chat_index.retain(|_, &mut sub_idx| {
            if self.chats[sub_idx].is_finished() {
                true
            } else {
                self.chats[sub_idx].mark_finished(role.clone(), text);
                false
            }
        });
        self.sync_subagents();
    }

    pub fn flush_all_chats(&mut self) {
        for chat in &mut self.chats {
            chat.flush();
        }
    }

    fn route_text_paste(&mut self, text: &str) {
        if self.plan_form_active() {
            return;
        }
        if self.permission_prompt.handle_paste(text) {
            return;
        }
        if self.lua_picker.handle_paste(text) {
            return;
        }
        if self.float_mgr.handle_paste(text) {
            return;
        }
        if self.search_modal.is_open() {
            self.search_modal.handle_paste(text);
            let chat = &mut self.chats[self.active_chat];
            let texts = chat.segment_search_texts();
            self.search_modal.update_matches(&texts);
            sync_search_highlight(&self.search_modal, chat);
            return;
        }
        macro_rules! try_picker {
            ($picker:expr) => {
                if $picker.handle_paste(text) {
                    return;
                }
            };
        }
        try_picker!(self.file_picker);
        try_picker!(self.task_picker);
        try_picker!(self.rewind_picker);
        try_picker!(self.theme_picker);
        try_picker!(self.model_picker);
        try_picker!(self.mcp_picker);
        try_picker!(self.login_picker);
        if !self.is_main_chat() {
            return;
        }
        if let InputAction::PaletteSync(val) = self.input_box.handle_paste(text) {
            self.command_palette.sync(&val);
            self.sync_command_arguments(&val, self.input_box.buffer.cursor_byte_offset());
            self.sync_file_completion();
        }
    }

    fn handle_plan_form_action(&mut self, action: PlanFormAction) -> Vec<Action> {
        match action {
            PlanFormAction::Consumed | PlanFormAction::Passthrough => vec![],
            PlanFormAction::Hide => {
                self.plan_form.hide();
                vec![]
            }
            PlanFormAction::OpenEditor => match self.state.plan.path() {
                Some(p) => vec![Action::OpenEditor(p.to_path_buf())],
                None => {
                    self.flash(FLASH_NO_PLAN.into());
                    vec![]
                }
            },
            PlanFormAction::Implement => self.implement_plan(false),
            PlanFormAction::ClearAndImplement => self.implement_plan(true),
        }
    }

    fn implement_plan(&mut self, clear_context: bool) -> Vec<Action> {
        let parallel = self.plan_form.parallel();
        self.plan_form.reset();
        let plan_snapshot = match std::mem::take(&mut self.state.plan) {
            PlanState::Ready(p) => Some((
                std::fs::read_to_string(&p).unwrap_or_default(),
                p.display().to_string(),
            )),
            _ => None,
        };

        self.state.mode = Mode::Build;

        let mut actions = if clear_context {
            self.reset_session()
        } else {
            vec![]
        };

        let text = if let Some((content, path_str)) = plan_snapshot {
            let text = if parallel {
                format!("{IMPLEMENT_MSG_PREFIX} at `{path_str}`. {IMPLEMENT_PARALLEL_HINT}")
            } else {
                format!("{IMPLEMENT_MSG_PREFIX} at `{path_str}`.")
            };
            self.main_chat()
                .push(DisplayMessage::plan(content, path_str));
            text
        } else {
            format!("{}.", IMPLEMENT_MSG_PREFIX)
        };
        let msg = QueuedMessage {
            text,
            images: vec![],
        };
        actions.extend(self.start_from_queue(&msg));
        actions
    }
}

fn is_streaming_stop_key(key: KeyEvent) -> bool {
    key::QUIT.matches(key) || key.code == KeyCode::Esc
}

fn sync_search_highlight(modal: &SearchModal, chat: &mut Chat) {
    let idx = modal.current_segment_index();
    if let Some(i) = idx {
        chat.scroll_to_segment(i);
    }
    chat.set_highlight_segment(idx);
}

fn format_with_images(text: &str, image_count: usize) -> String {
    match image_count {
        0 => text.to_string(),
        1 => format!("{text} [1 image]"),
        n => format!("{text} [{n} images]"),
    }
}
