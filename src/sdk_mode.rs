//! SDK streaming mode: `makima --print --input-format stream-json`.
//!
//! Wire protocol matches Claude Code's SDK interface so tools like Conductor, Windsurf, and custom
//! orchestrators work without adaptation.
//!
//! Per-message wire ids (`uuid`, assistant `message.id`) use `uuid::Uuid::now_v7()` to emit the
//! hyphenated-hex UUIDv7 shape that Claude Code SDK consumers expect, rather than makima's base58
//! `MakimaId` canonical form.

use std::any::Any;
use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::io::{self, BufRead, Write};
use std::mem;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;
use std::time::Instant;

use event_listener::Event;

use color_eyre::Result;
use color_eyre::eyre::{Context, eyre};
use flume::{Receiver, Sender};
use makima_agent::agent::PromptSlotSource;
use makima_agent::cancel::{CancelToken, CancelTrigger};
use makima_agent::command::{
    CustomCommand, SessionCommandHost, StandardCommands, StandardCompletions, session_capabilities,
};
use makima_agent::config_change::ConfigChangeError;
use makima_agent::file_restore::{OwnedRestoreLease, reconcile_session_guarded};
use makima_agent::headless::{
    self, InteractiveControlSender, InteractiveHandle, InteractiveInputSender, InteractiveParams,
    RootSubmission, WorkCancel,
};
use makima_agent::journal::SessionJournal;
use makima_agent::mcp;
use makima_agent::permissions::{
    PermissionAnswer, PermissionManager, PluginRuleStore, TaggedAnswer,
};
use makima_agent::session_checkpoint::SessionLogCheckpoint;
use makima_agent::session_coordinator::{SessionCoordinatorHandle, SessionReadHandle};
use makima_agent::session_defaults;
use makima_agent::session_options::{
    DISABLED_VALUE, ENABLED_VALUE, MODEL_OPTION_ID, SessionOptionError, YOLO_OPTION_ID,
};
use makima_agent::session_recovery::RecoveredSession;
use makima_agent::tools::{FileWriteLocks, QUESTION_TOOL_NAME, QuestionMode};
use makima_agent::{
    AgentConfig, AgentEvent, AgentId, AgentInput, AgentManagerHandle, AgentMode, Envelope, ModeId,
    ModeRegistry, PermissionsConfig, ToolOutput, TurnId, TurnOutcome,
};
use makima_commands::{
    BuiltinOperation, CommandContent, CommandError, CommandFuture, CommandHost, CommandOutcome,
    CommandRegistry, HostRequest, HostResponse, InputDispatch, Producer, ProducerPrecedence,
    Registration, TargetHandle,
};
use makima_config::{ModelPolicy, ProjectConfig, SessionDefaults};
use makima_lua::orchestration::{
    AgentOrchestration, Discovery, ModelCatalog, PinnedCommandHost, PinnedCommandTarget,
    TrustedTarget,
};
use makima_lua::session_snapshot::HeadlessObserver;
use makima_providers::model::Model;
use makima_providers::{
    ImageSource, Message, ProviderRegistry, StopReason, Timeouts, TokenUsage, add_cost,
};
use makima_storage::StateDir;
use makima_storage::checkpoint::{CheckpointClaim, CheckpointPublication};
use makima_storage::conversation::{ConversationRelation, ConversationState};
use makima_storage::id::{MakimaId, SessionRef};
use makima_storage::session_lock;
use makima_storage::session_state::ParentLink;
use makima_storage::sessions::{SESSIONS_DIR, Session, SessionMeta, read_prefs};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;
use tracing::warn;

use crate::cli::Cli;
use crate::command_attachments;

const RECOVERY_CANCELLED: &str = "session recovery publication cancelled";
const WORK_ACTIVE: &str = "a command is already running";
const OUTPUT_CLOSED: &str = "the SDK output is closed";
const SESSION_ID_CONFLICT: &str =
    "--session-id must match the resumed session; use --fork-session to copy it to a new session";
const FORK_DESTINATION_EXISTS: &str = "--fork-session requires a new --session-id";
const SDK_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);
const RESET_COMMAND: &str = "/reset";
const CHILD_SESSION_RESET: &str = "resetting the session is unsupported for child agents";

const TOOL_NAME_MAP: &[(&str, &str)] = &[
    ("bash", "Bash"),
    ("read", "Read"),
    ("edit", "Edit"),
    ("write", "Write"),
    ("grep", "Grep"),
    ("glob", "Glob"),
    ("todo_write", "TodoWrite"),
    ("webfetch", "WebFetch"),
    ("websearch", "WebSearch"),
    ("task_spawn", "Task"),
    ("multiedit", "MultiEdit"),
    ("code_execution", "CodeExecution"),
    ("index", "Index"),
    ("memory", "Memory"),
    ("question", "Question"),
    ("skill", "Skill"),
];

/// Emits a hyphenated-hex UUIDv7 string for Claude Code SDK wire ids
/// (message.id, assistant message.id).
#[allow(clippy::disallowed_methods)]
fn wire_uuid() -> String {
    uuid::Uuid::now_v7().to_string()
}

#[derive(Clone, Copy, PartialEq, Debug)]
enum PermissionMode {
    Default,
    AcceptEdits,
    Plan,
    BypassPermissions,
}

impl PermissionMode {
    fn resolve(flag: Option<&str>, yolo: bool) -> Self {
        match flag {
            Some(s) => Self::parse(s).unwrap_or_else(|| {
                eprintln!("warning: unknown permission mode '{s}', using default");
                Self::Default
            }),
            None if yolo => Self::BypassPermissions,
            None => Self::Default,
        }
    }

    fn parse(s: &str) -> Option<Self> {
        match s {
            "default" => Some(Self::Default),
            "acceptEdits" => Some(Self::AcceptEdits),
            "plan" => Some(Self::Plan),
            "bypassPermissions" => Some(Self::BypassPermissions),
            _ => None,
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Default => "default",
            Self::AcceptEdits => "acceptEdits",
            Self::Plan => "plan",
            Self::BypassPermissions => "bypassPermissions",
        }
    }
}

#[derive(Serialize)]
struct WireMessage {
    #[serde(flatten)]
    inner: WireInner,
    session_id: SessionRef,
    uuid: String,
}

#[derive(Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum WireInner {
    System(SystemPayload),
    Assistant(AssistantPayload),
    User(UserPayload),
    Result(ResultPayload),
    StreamEvent(StreamEventPayload),
    ControlResponse(ControlResponsePayload),
    ControlRequest(ControlRequestPayload),
}

#[derive(Serialize)]
struct SystemPayload {
    subtype: &'static str,
    #[serde(flatten)]
    extra: Value,
}

#[derive(Serialize)]
struct AssistantPayload {
    message: AssistantMessage,
    #[serde(skip_serializing_if = "Option::is_none")]
    parent_tool_use_id: Option<String>,
}

#[derive(Serialize)]
struct AssistantMessage {
    id: String,
    model: String,
    role: &'static str,
    content: Value,
    stop_reason: Option<StopReason>,
    usage: TokenUsage,
}

#[derive(Serialize)]
struct UserPayload {
    message: UserMessage,
    #[serde(skip_serializing_if = "Option::is_none")]
    parent_tool_use_id: Option<String>,
}

#[derive(Serialize)]
struct UserMessage {
    role: &'static str,
    content: Value,
}

#[derive(Serialize)]
struct ResultPayload {
    subtype: &'static str,
    is_error: bool,
    duration_ms: u128,
    duration_api_ms: u128,
    num_turns: u32,
    result: String,
    total_cost_usd: f64,
    usage: TokenUsage,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    permission_denials: Vec<Value>,
}

#[derive(Serialize)]
struct StreamEventPayload {
    event: Value,
}

#[derive(Serialize)]
struct ControlResponsePayload {
    response: ControlResponseInner,
}

#[derive(Serialize)]
struct ControlResponseInner {
    subtype: &'static str,
    request_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    response: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

#[derive(Serialize)]
struct ControlRequestPayload {
    request_id: String,
    request: ControlRequestInner,
}

#[derive(Serialize)]
struct ControlRequestInner {
    subtype: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    tool_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    input: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tool_use_id: Option<String>,
}

#[derive(Debug, PartialEq, Eq)]
enum InboundMessageType {
    User,
    ControlRequest,
    ControlResponse,
    ControlCancelRequest,
    Unknown(String),
}

impl<'de> Deserialize<'de> for InboundMessageType {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = String::deserialize(deserializer)?;
        Ok(match value.as_str() {
            "user" => Self::User,
            "control_request" => Self::ControlRequest,
            "control_response" => Self::ControlResponse,
            "control_cancel_request" => Self::ControlCancelRequest,
            _ => Self::Unknown(value),
        })
    }
}

#[derive(Deserialize)]
struct InboundMessage {
    #[serde(rename = "type")]
    msg_type: InboundMessageType,
    #[serde(flatten)]
    payload: Value,
}

#[derive(serde::Deserialize)]
struct InboundUser {
    message: InboundUserMessage,
}

#[derive(serde::Deserialize)]
struct InboundUserMessage {
    content: Value,
}

#[derive(serde::Deserialize)]
struct InboundControlRequest {
    request_id: String,
    request: InboundControlRequestInner,
}

#[derive(Debug, PartialEq, Eq)]
enum InboundControlRequestType {
    Initialize,
    Interrupt,
    SetPermissionMode,
    SetModel,
    Unknown(String),
}

impl<'de> Deserialize<'de> for InboundControlRequestType {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = String::deserialize(deserializer)?;
        Ok(match value.as_str() {
            "initialize" => Self::Initialize,
            "interrupt" => Self::Interrupt,
            "set_permission_mode" => Self::SetPermissionMode,
            "set_model" => Self::SetModel,
            _ => Self::Unknown(value),
        })
    }
}

#[derive(Deserialize)]
struct InboundControlRequestInner {
    subtype: InboundControlRequestType,
    #[serde(flatten)]
    extra: Value,
}

#[derive(serde::Deserialize)]
struct InboundControlResponse {
    response: Value,
}

#[derive(serde::Deserialize)]
struct InboundControlCancelRequest {
    request_id: String,
}

// StreamSynth owns all Anthropic stream state. Each method returns every wire
// event the transition needs (closing the old block, opening a new message, ...)
// so callers never have to track block lifecycle themselves.

#[derive(Clone, Copy, PartialEq)]
enum BlockKind {
    Text,
    Thinking,
}

struct StreamSynth {
    block_index: i32,
    started: bool,
    current_block: Option<BlockKind>,
}

impl StreamSynth {
    fn new() -> Self {
        Self {
            block_index: -1,
            started: false,
            current_block: None,
        }
    }

    fn reset(&mut self) {
        *self = Self::new();
    }

    fn text_delta(&mut self, model: &str, text: &str) -> Vec<Value> {
        let mut events = self.ensure_block(model, BlockKind::Text);
        events.push(serde_json::json!({
            "type": "content_block_delta",
            "index": self.block_index,
            "delta": {"type": "text_delta", "text": text}
        }));
        events
    }

    fn thinking_delta(&mut self, model: &str, text: &str) -> Vec<Value> {
        let mut events = self.ensure_block(model, BlockKind::Thinking);
        events.push(serde_json::json!({
            "type": "content_block_delta",
            "index": self.block_index,
            "delta": {"type": "thinking_delta", "thinking": text}
        }));
        events
    }

    fn tool_use(&mut self, model: &str, id: &str, name: &str, input_json: &str) -> Vec<Value> {
        let mut events = self.ensure_started(model);
        events.extend(self.close_block());
        self.block_index += 1;
        events.push(serde_json::json!({
            "type": "content_block_start",
            "index": self.block_index,
            "content_block": {"type": "tool_use", "id": id, "name": name, "input": {}}
        }));
        events.push(serde_json::json!({
            "type": "content_block_delta",
            "index": self.block_index,
            "delta": {"type": "input_json_delta", "partial_json": input_json}
        }));
        events.push(self.block_stop());
        events
    }

    fn finish_message(&mut self, usage: &TokenUsage) -> Vec<Value> {
        if !self.started {
            return Vec::new();
        }
        let mut events: Vec<Value> = self.close_block().into_iter().collect();
        events.push(serde_json::json!({
            "type": "message_delta",
            "delta": {"stop_reason": null},
            "usage": {"output_tokens": usage.output}
        }));
        events.push(serde_json::json!({"type": "message_stop"}));
        self.reset();
        events
    }

    fn ensure_started(&mut self, model: &str) -> Vec<Value> {
        if self.started {
            return Vec::new();
        }
        self.started = true;
        vec![serde_json::json!({
            "type": "message_start",
            "message": {
                "id": wire_uuid(),
                "type": "message",
                "role": "assistant",
                "content": [],
                "model": model,
                "stop_reason": null,
                "usage": {"input_tokens": 0, "output_tokens": 0}
            }
        })]
    }

    fn ensure_block(&mut self, model: &str, kind: BlockKind) -> Vec<Value> {
        let mut events = self.ensure_started(model);
        if self.current_block == Some(kind) {
            return events;
        }
        events.extend(self.close_block());
        self.block_index += 1;
        self.current_block = Some(kind);
        let content_block = match kind {
            BlockKind::Text => serde_json::json!({"type": "text", "text": ""}),
            BlockKind::Thinking => serde_json::json!({"type": "thinking", "thinking": ""}),
        };
        events.push(serde_json::json!({
            "type": "content_block_start",
            "index": self.block_index,
            "content_block": content_block
        }));
        events
    }

    fn close_block(&mut self) -> Option<Value> {
        self.current_block.take().map(|_| self.block_stop())
    }

    fn thinking_block_end(&mut self) -> Vec<Value> {
        if self.current_block == Some(BlockKind::Thinking) {
            self.close_block().into_iter().collect()
        } else {
            Vec::new()
        }
    }

    fn block_stop(&self) -> Value {
        serde_json::json!({
            "type": "content_block_stop",
            "index": self.block_index,
        })
    }
}

fn makima_to_claude_tool_name(name: &str) -> &str {
    TOOL_NAME_MAP
        .iter()
        .find(|(m, _)| *m == name)
        .map(|(_, c)| *c)
        .unwrap_or(name)
}

#[derive(Clone)]
struct SdkWriter {
    session_id: SessionRef,
    out_tx: Sender<String>,
}

impl SdkWriter {
    fn emit(&self, inner: WireInner) -> Result<()> {
        let msg = WireMessage {
            inner,
            session_id: self.session_id.clone(),
            uuid: wire_uuid(),
        };
        self.out_tx
            .send(serde_json::to_string(&msg)?)
            .map_err(|_| eyre!("stdout writer closed"))
    }

    fn emit_system(&self, subtype: &'static str, extra: Value) -> Result<()> {
        self.emit(WireInner::System(SystemPayload { subtype, extra }))
    }

    fn emit_control_response(
        &self,
        request_id: &str,
        response: Option<Value>,
        error: Option<String>,
    ) -> Result<()> {
        self.emit(WireInner::ControlResponse(ControlResponsePayload {
            response: ControlResponseInner {
                subtype: if error.is_some() { "error" } else { "success" },
                request_id: request_id.into(),
                response,
                error,
            },
        }))
    }
}

pub struct SdkParams {
    pub cli: Cli,
    pub cwd: PathBuf,
    pub model: Model,
    pub explicit_model: bool,
    pub config: AgentConfig,
    pub permissions_config: PermissionsConfig,
    pub timeouts: Timeouts,
    pub prompt_slots: PromptSlotSource,
    pub defaults: SessionDefaults,
    pub model_policy: Arc<ModelPolicy>,
    pub providers: Arc<ProviderRegistry>,
    pub plugin_rules: Arc<PluginRuleStore>,
    pub project_config: ProjectConfig,
    pub commands: Vec<CustomCommand>,
    pub command_registry: CommandRegistry,
    /// Plugin-registered session options, so a tool's options resolve here the
    /// same way they do in the TUI.
    pub session_options: makima_agent::session_coordinator::SessionOptionCatalog,
    pub lua_handle: makima_lua::EventHandle,
}

struct Shared {
    turn_start: Instant,
    pending: HashMap<String, (String, Sender<String>)>,
    /// The SDK work in flight: one local operation, or the turns carrying
    /// user messages. A message sent while a turn runs joins it.
    work: Vec<WorkCancel>,
    /// Counts the local operations begun, so one that ends after an
    /// interrupt dropped it leaves the work begun since alone.
    operation: u64,
    /// Stdin ended, so no answer to a new permission ask can arrive.
    input_closed: bool,
    /// The event pump stopped, so nothing reports or ends SDK work any more.
    output_closed: bool,
    idle: Event,
    /// A user message is dispatching: the results of root turns it did not
    /// start, such as the ones a command's handler runs, are held until it
    /// ends, so the message gets one result.
    dispatching: bool,
    held: Vec<ResultPayload>,
    /// Tells the pump a dispatch ended (and whether as a completed command),
    /// and hears back once it reported. The pump reports after the events
    /// already queued, so the turns a handler waited for are held by then.
    dispatch_end: (Sender<DispatchEnd>, Receiver<DispatchEnd>),
}

struct DispatchEnd {
    completed: bool,
    reported: Sender<()>,
}

impl Shared {
    fn new() -> Self {
        Self {
            turn_start: Instant::now(),
            pending: HashMap::new(),
            work: Vec::new(),
            operation: 0,
            input_closed: false,
            output_closed: false,
            idle: Event::new(),
            dispatching: false,
            held: Vec::new(),
            dispatch_end: flume::unbounded(),
        }
    }
}

struct SdkCommands {
    registry: CommandRegistry,
    target: TargetHandle,
    _standard_commands: Arc<StandardCommands>,
}

impl SdkCommands {
    #[cfg(test)]
    fn new(
        registry: CommandRegistry,
        custom: &[CustomCommand],
        host: impl CommandHost,
    ) -> Result<Self> {
        let standard_commands =
            StandardCommands::register(&registry, custom, StandardCompletions::default())?;
        let target = registry.bind_target(session_capabilities(), Arc::new(host));
        Ok(Self {
            target,
            registry,
            _standard_commands: Arc::new(standard_commands),
        })
    }

    fn bind(
        registry: CommandRegistry,
        standard_commands: Arc<StandardCommands>,
        host: impl CommandHost,
    ) -> Self {
        let target = registry.bind_target(session_capabilities(), Arc::new(host));
        Self {
            registry,
            target,
            _standard_commands: standard_commands,
        }
    }

    fn projection(&self) -> Vec<Value> {
        projected_commands(&self.registry, &self.target).unwrap_or_default()
    }

    fn slash_commands(&self) -> Vec<String> {
        self.projection()
            .into_iter()
            .filter_map(|command| command["name"].as_str().map(str::to_owned))
            .collect()
    }

    #[cfg(test)]
    fn dispatch_input(&self, prompt: &str, images: &[ImageSource]) -> CommandFuture<InputDispatch> {
        let content = CommandContent {
            text: Arc::from(prompt),
            attachments: command_attachments::from_images(images),
        };
        self.registry.dispatch_input(&self.target, content)
    }

    fn dispatch_pinned_input(
        &self,
        prompt: &str,
        images: &[ImageSource],
        pinned: PinnedCommandTarget,
    ) -> CommandFuture<InputDispatch> {
        self.registry.dispatch_input_with_admission(
            &self.target,
            CommandContent {
                text: Arc::from(prompt),
                attachments: command_attachments::from_images(images),
            },
            Arc::new(pinned),
        )
    }
}

struct SdkRuntime {
    handle: InteractiveHandle,
    coordinator: SessionCoordinatorHandle,
    checkpoint: Arc<SessionLogCheckpoint>,
    _lock: Arc<session_lock::LockHeartbeat>,
    root: TrustedTarget,
    commands: Arc<SdkCommands>,
    orchestration: Arc<AgentOrchestration>,
    catalog: ModelCatalog,
    writer: SdkWriter,
    shared: Arc<Mutex<Shared>>,
    init: Value,
    resumed: bool,
    tasks: Mutex<Vec<smol::Task<()>>>,
    discovery: Mutex<Option<Discovery>>,
}

struct SdkSession {
    params: SdkParams,
    storage: StateDir,
    standard_commands: Arc<StandardCommands>,
    _reset_alias: Producer,
    out_tx: Sender<String>,
    current: Mutex<Option<Arc<SdkRuntime>>>,
    retired: Mutex<Vec<Arc<SdkRuntime>>>,
    input_closed: AtomicBool,
    reset: smol::lock::Mutex<()>,
}

impl SdkRuntime {
    async fn prepare(session: &Arc<SdkSession>, fresh: bool) -> Result<Arc<Self>> {
        let params = &session.params;
        let cli = &params.cli;
        let cwd = params.cwd.clone();
        let model = params.model.clone();
        let explicit_model = params.explicit_model && !fresh;
        let config = params.config.clone();
        let permissions_config = params.permissions_config.clone();
        let timeouts = params.timeouts;
        let prompt_slots = params.prompt_slots.clone();
        let defaults = params.defaults;
        let model_policy = Arc::clone(&params.model_policy);
        let providers = Arc::clone(&params.providers);
        let plugin_rules = Arc::clone(&params.plugin_rules);
        let project_config = params.project_config.clone();
        let command_registry = params.command_registry.clone();
        let session_options = params.session_options.clone();
        let lua_handle = &params.lua_handle;
        let explicit_permission_mode = cli.permission_mode.is_some();
        let permission_mode = PermissionMode::resolve(
            cli.permission_mode.as_deref(),
            cli.yolo || permissions_config.yolo,
        );
        let startup_permission_mode = permission_mode;

        let working_dir = cwd.to_string_lossy().into_owned();
        let storage = &session.storage;
        let mut restored = if fresh {
            ResolvedSession {
                id: None,
                root_id: None,
                parent: None,
                journal: None,
                seed: None,
                history: Default::default(),
                model: None,
                meta: Default::default(),
            }
        } else {
            resolve_session(cli, &working_dir, storage)?
        };
        let resumed = restored.seed.is_some();
        let session_id = restored
            .id
            .clone()
            .unwrap_or_else(|| SessionRef::from(MakimaId::generate()));
        let root_id = restored.root_id.unwrap_or_else(AgentId::generate);
        let (checkpoint, lock_guard, recovered) = recover_sdk_session(
            &mut restored,
            storage,
            &session_id,
            root_id,
            &model.spec(),
            &working_dir,
            &lua_handle.mode_registry(),
        )
        .await?;
        restored.history = recovered.session().history_snapshot();
        restored.meta = recovered.session().meta.clone();
        restored.root_id = Some(root_id);
        restored.journal = Some(recovered.journal());
        let recovered_state = recovered.session().latest_state()?;
        let recovered_config = recovered_state
            .scopes
            .get(&Some(root_id))
            .and_then(|scope| scope.config.as_ref());
        if let Some(config) = recovered_config {
            restored.model = Some(config.model.clone());
        }
        let cwd = recovered_config
            .map(|config| PathBuf::from(&config.cwd))
            .unwrap_or(cwd);
        let restored_state = restored.model.is_some();
        let model_override = explicit_model.then(|| model.spec());
        let model = if let Some(config) = recovered_config {
            if !model_policy.allows(&config.model) {
                return Err(eyre!(SessionOptionError::PolicyRejected(
                    config.model.clone().into()
                )));
            }
            providers.model(&config.model)?
        } else {
            startup_model(&providers, model, &restored, explicit_model, &model_policy)
        };
        let persisted_yolo = recovered_state
            .scopes
            .get(&None)
            .and_then(|scope| scope.yolo);
        let permission_mode = restored_permission_mode(
            restored_state,
            persisted_yolo,
            startup_permission_mode,
            explicit_permission_mode,
        );
        let yolo = startup_permission_mode == PermissionMode::BypassPermissions;
        let defaults = recovered_config.map_or_else(
            || {
                session_defaults::resolve(
                    defaults,
                    &read_prefs(storage),
                    &restored.meta,
                    restored.history.messages.is_empty(),
                    &model,
                )
            },
            |config| SessionDefaults {
                fast: config.fast,
                workflow: config.workflow,
                thinking: config.thinking,
            },
        );
        let persisted_options = recovered_config
            .map(|config| config.options.clone())
            .unwrap_or_else(|| mem::take(&mut restored.meta.session_options));
        let session_id = Some(session_id);
        let initial_history = restored.history;

        let (mcp_handle, mcp_config_errors) = async {
            let (handle, errors) =
                mcp::start_with_commands(&cwd, project_config.clone(), command_registry.clone())
                    .await;
            if let Some(handle) = &handle {
                handle.ready().await;
            }
            (handle, errors)
        }
        .await;
        if !mcp_config_errors.is_empty() {
            eprintln!("MCP config error: {mcp_config_errors}");
        }

        let startup_model = model.clone();
        let catalog = ModelCatalog::offline(Arc::clone(&providers), &model_policy);
        let (binding, root_target) = makima_lua::orchestration::root_binding();
        let handle = headless::spawn_interactive_with_checkpoint(
            InteractiveParams {
                model,
                config,
                permissions_config,
                timeouts,
                prompt_slots,
                excluded_tools: vec![QUESTION_TOOL_NAME],
                question_mode: QuestionMode::Headless,
                mcp_handle,
                initial_wd: cwd.clone(),
                session_id,
                root_id: restored.root_id,
                journal: restored.journal,
                initial_history: Vec::new(),
                yolo,
                system_prompt_override: cli.system_prompt.clone().filter(|s| !s.is_empty()),
                append_system_prompt: cli.append_system_prompt.clone().filter(|s| !s.is_empty()),
                defaults,
                modes: lua_handle.mode_registry(),
                model_policy: Arc::clone(&model_policy),
                providers: Arc::clone(&providers),
                plugin_rules,
                project_config,
                local_tools: Default::default(),
                state_dir: Some(storage.path().to_path_buf()),
                answers: true,
            },
            headless::default_preparation(Arc::clone(&providers)),
            Some(initial_history),
            Some(binding),
            Arc::clone(&checkpoint),
        );
        handle
            .initialized_root()
            .await
            .map_err(|error| eyre!(error))?;
        let definitions = makima_agent::session_coordinator::builtin_option_definitions(
            startup_model.spec(),
            catalog.snapshot(),
            handle.permissions.is_yolo(),
            defaults.fast,
            defaults.workflow,
            defaults.thinking.into(),
        );
        checkpoint
            .bind_journal(handle.manager.journal())
            .await
            .map_err(|error| eyre!(error))?;
        let coordinator = SessionCoordinatorHandle::register(
            makima_agent::session_coordinator::SessionCoordinatorParams {
                session_id: handle.session_id.id(),
                catalog: session_options.clone(),
                definitions,
                persisted_options,
                model_policy: Arc::clone(&model_policy),
                checkpoint: checkpoint.clone(),
                mailbox: handle.mailbox.clone(),
            },
        )
        .map_err(|error| eyre!("register session coordinator: {error}"))?;
        let prepared: Result<Arc<Self>> = async {
            coordinator
                .bind_permissions(Arc::clone(&handle.permissions))
                .map_err(|error| eyre!("bind session permissions: {error}"))?;
            let trusted_root = makima_lua::orchestration::bind_frontend_root(
                &handle,
                &coordinator,
                &root_target,
                headless::default_model_preparer(Arc::clone(&providers)),
            )
            .await
            .map_err(|error| eyre!("bind session actor: {error}"))?;
            let root = trusted_root.target.clone();
            let orchestration = makima_lua::orchestration::durable_frontend(
                "SDK",
                &catalog,
                trusted_root.clone(),
                coordinator.clone(),
                Arc::clone(&checkpoint),
            );
            let sdk_commands = Arc::new(SdkCommands::bind(
                command_registry,
                Arc::clone(&session.standard_commands),
                SdkCommandHost {
                    inner: PinnedCommandHost::new(
                        SessionCommandHost::new(
                            handle.control_tx.clone(),
                            coordinator.clone(),
                            Arc::clone(&providers),
                        ),
                        trusted_root.clone(),
                        &orchestration,
                        headless::default_model_preparer(Arc::clone(&providers)),
                    ),
                    root: trusted_root.clone(),
                    session: Arc::downgrade(session),
                },
            ));
            orchestration.bind_commands(
                &root,
                makima_lua::orchestration::command_runner(
                    sdk_commands.registry.clone(),
                    sdk_commands.target.clone(),
                    makima_lua::orchestration::headless_outcomes(&handle),
                ),
            );
            let modes = lua_handle.mode_registry();
            if explicit_permission_mode {
                coordinator
                    .set_option(
                        YOLO_OPTION_ID,
                        if permission_mode == PermissionMode::BypassPermissions {
                            ENABLED_VALUE
                        } else {
                            DISABLED_VALUE
                        },
                    )
                    .await
                    .map_err(|error| eyre!(error))?;
                let key = if permission_mode == PermissionMode::Plan {
                    ModeId::Plan.key()
                } else {
                    ModeId::Build.key()
                };
                makima_lua::orchestration::set_mode(&*orchestration, &root, &modes, key, storage)
                    .await
                    .map_err(|error| eyre!("set startup permission mode: {error}"))?;
                handle
                    .permissions
                    .try_set_yolo(permission_mode == PermissionMode::BypassPermissions)
                    .map_err(|error| eyre!(error))?;
            }
            if recovered_config.is_some()
                && let Some(spec) = model_override
                && spec != startup_model.spec()
            {
                coordinator
                    .set_option(MODEL_OPTION_ID, spec)
                    .await
                    .map_err(|error| eyre!(error))?;
            }
            let committed_config = headless::checkpoint_startup_config(&root, &coordinator)
                .await
                .map_err(|error| eyre!(error))?;
            catalog.attach(&orchestration);
            let committed_plan = matches!(committed_config.mode, AgentMode::Plan(_));
            let permission_mode = if committed_plan {
                PermissionMode::Plan
            } else if handle.permissions.is_yolo() {
                PermissionMode::BypassPermissions
            } else if permission_mode == PermissionMode::BypassPermissions {
                PermissionMode::Default
            } else {
                permission_mode
            };

            let writer = SdkWriter {
                session_id: handle.session_id.clone(),
                out_tx: session.out_tx.clone(),
            };
            let tools: Vec<_> = handle
                .tool_names
                .iter()
                .map(|name| makima_to_claude_tool_name(name))
                .collect();
            let init = serde_json::json!({
                "cwd": committed_config.context.cwd(), "tools": tools,
                "model": committed_config.model.id, "permissionMode": permission_mode.as_str(),
                "apiKeySource": "none", "mcp_servers": [],
                "slash_commands": sdk_commands.slash_commands(), "output_style": "default",
            });
            Ok(Arc::new(Self {
                handle,
                coordinator: coordinator.clone(),
                checkpoint,
                _lock: lock_guard,
                root: trusted_root,
                commands: sdk_commands,
                orchestration,
                catalog,
                writer,
                shared: Arc::new(Mutex::new(Shared {
                    input_closed: session.input_closed.load(Ordering::Acquire),
                    ..Shared::new()
                })),
                init,
                resumed,
                tasks: Mutex::new(Vec::new()),
                discovery: Mutex::new(None),
            }))
        }
        .await;
        if prepared.is_err() {
            coordinator.retire();
        }
        prepared
    }

    fn activate(&self, session: &SdkSession) {
        let params = &session.params;
        params
            .lua_handle
            .install_orchestration_services(self.orchestration.clone());
        let observer =
            HeadlessObserver::new(params.lua_handle.clone(), self.handle.session_id.id());
        observer.install(self.root.target.clone());
        if self.resumed {
            params
                .lua_handle
                .notify_state_restored(self.root.target.clone());
        }
        let pump = EventPump {
            writer: self.writer.clone(),
            shared: Arc::clone(&self.shared),
            read: self.coordinator.read(),
            permissions: Arc::clone(&self.handle.permissions),
            answer_tx: self.handle.answer_tx.clone(),
            include_partial_messages: params.cli.include_partial_messages,
            synth: StreamSynth::new(),
            tool_inputs: HashMap::new(),
            result_text: String::new(),
            cost: None,
            request_counter: 0,
            terminal_run_id: None,
            observer,
        }
        .spawn(self.handle.event_rx.clone());
        self.tasks.lock().unwrap().extend([
            pump,
            watch_command_projection(
                self.writer.clone(),
                self.commands.registry.clone(),
                self.commands.target.clone(),
            ),
        ]);
        *self.discovery.lock().unwrap() = Some(self.catalog.discover(
            Arc::clone(&params.model_policy),
            params.model.provider.to_string(),
            |_| {},
        ));
    }

    fn dispatch(&self) -> UserDispatch {
        UserDispatch {
            root: self.root.clone(),
            commands: Arc::clone(&self.commands),
            input_tx: self.handle.input_tx.clone(),
            control_tx: self.handle.control_tx.clone(),
            writer: self.writer.clone(),
            shared: Arc::clone(&self.shared),
        }
    }

    async fn close(&self) -> Result<()> {
        self.discovery.lock().unwrap().take();
        self.handle.manager.shutdown(SDK_SHUTDOWN_TIMEOUT).await;
        let closed = if SessionCoordinatorHandle::resolve(self.handle.session_id.id()).is_ok() {
            self.coordinator.close().await
        } else {
            Ok(())
        };
        let drained = self.checkpoint.drain().await;
        self.handle.manager.journal().invalidate_state_access();
        let tasks = mem::take(&mut *self.tasks.lock().unwrap());
        for task in tasks {
            task.cancel().await;
        }
        closed.map_err(|error| eyre!(error))?;
        drained?;
        Ok(())
    }
}

impl SdkSession {
    fn new(params: SdkParams, storage: StateDir, out_tx: Sender<String>) -> Result<Arc<Self>> {
        let standard_commands = Arc::new(StandardCommands::register(
            &params.command_registry,
            &params.commands,
            StandardCompletions::default(),
        )?);
        let reset_alias = params
            .command_registry
            .create_producer(ProducerPrecedence::Builtin);
        let session = Arc::new(Self {
            params,
            storage,
            standard_commands,
            _reset_alias: reset_alias,
            out_tx,
            current: Mutex::new(None),
            retired: Mutex::new(Vec::new()),
            input_closed: AtomicBool::new(false),
            reset: smol::lock::Mutex::new(()),
        });
        let runtime = smol::block_on(SdkRuntime::prepare(&session, false))?;
        let original = runtime
            .commands
            .registry
            .resolve_input_for(&runtime.commands.target, "/new")?;
        let mut spec = original.command.spec().clone();
        spec.name = RESET_COMMAND.into();
        spec.aliases = Arc::from([]);
        session._reset_alias.replace(vec![Registration {
            spec,
            behavior: original.command.behavior(),
            argument_completions: original.command.argument_completions(),
        }])?;
        let mut init = runtime.init.clone();
        init["slash_commands"] = serde_json::to_value(runtime.commands.slash_commands())?;
        {
            let mut current = session.current.lock().unwrap();
            runtime.writer.emit_system("init", init)?;
            *current = Some(Arc::clone(&runtime));
            runtime.activate(&session);
        }
        Ok(session)
    }

    fn current(&self) -> Arc<SdkRuntime> {
        Arc::clone(
            self.current
                .lock()
                .unwrap()
                .as_ref()
                .expect("SDK runtime initialized"),
        )
    }

    async fn replace(self: &Arc<Self>, pinned: PinnedCommandTarget) -> Result<(), CommandError> {
        let _reset = self.reset.lock().await;
        let old = self.current();
        pinned.validate().map_err(|_| CommandError::StaleTarget)?;
        if pinned.target.id() != old.root.target.id()
            || !pinned.target.same_manager(&old.root.target)
        {
            return Err(CommandError::StaleTarget);
        }
        let active_work = || {
            old.handle.manager.snapshot().iter().any(|node| {
                node.actor
                    .as_ref()
                    .is_some_and(|actor| actor.active_turn.is_some() || actor.queued > 0)
            })
        };
        if active_work() {
            return Err(sdk_command_error(WORK_ACTIVE));
        }
        old.handle.manager.wait_settled().await;
        let prepared = SdkRuntime::prepare(self, true)
            .await
            .map_err(sdk_command_error)?;
        if active_work() {
            let _ = prepared.close().await;
            return Err(sdk_command_error(WORK_ACTIVE));
        }
        old.handle.manager.wait_settled().await;
        if let Err(error) = old.checkpoint.drain().await {
            let _ = prepared.close().await;
            return Err(sdk_command_error(error));
        }
        if let Err(error) = pinned.validate() {
            let _ = prepared.close().await;
            return Err(sdk_command_error(error));
        }
        let published = (|| -> Result<(), CommandError> {
            let mut current = self.current.lock().unwrap();
            prepared
                .writer
                .emit_system("init", prepared.init.clone())
                .map_err(sdk_command_error)?;
            old.handle
                .manager
                .close_incarnation(&pinned.incarnation())
                .map_err(sdk_command_error)?;
            old.coordinator.retire();
            old.handle.manager.journal().invalidate_state_access();
            *current = Some(Arc::clone(&prepared));
            prepared.activate(self);
            Ok(())
        })();
        if let Err(error) = published {
            let _ = prepared.close().await;
            return Err(error);
        }
        self.params.lua_handle.fire_autocmd(
            "SessionReset",
            serde_json::json!({
                "session_id": old.handle.session_id.id(),
            }),
        );
        self.retired.lock().unwrap().push(old);
        Ok(())
    }

    async fn drain_retired(&self) -> Result<()> {
        let retired = mem::take(&mut *self.retired.lock().unwrap());
        for runtime in retired {
            runtime.close().await?;
        }
        Ok(())
    }

    fn spawn_dispatcher(self: &Arc<Self>) -> (Sender<UserInput>, smol::Task<Result<()>>) {
        let (sender, receiver) = flume::unbounded::<UserInput>();
        let session = Arc::clone(self);
        let task = smol::spawn(async move {
            while let Ok((prompt, images)) = receiver.recv_async().await {
                let runtime = session.current();
                let dispatched = runtime.dispatch().dispatch(&prompt, &images).await;
                end_dispatch(&runtime.shared, false).await;
                session.drain_retired().await?;
                if !dispatched? {
                    break;
                }
            }
            Ok(())
        });
        (sender, task)
    }
}

impl Drop for SdkSession {
    fn drop(&mut self) {
        self._reset_alias.remove();
    }
}

struct SdkCommandHost {
    inner: PinnedCommandHost,
    root: TrustedTarget,
    session: Weak<SdkSession>,
}

impl CommandHost for SdkCommandHost {
    fn admission_context(&self) -> Option<Arc<dyn Any + Send + Sync>> {
        self.inner.admission_context()
    }

    fn request(&self, request: HostRequest) -> CommandFuture<Result<HostResponse, CommandError>> {
        self.request_with_admission(request, self.admission_context())
    }

    fn request_with_admission(
        &self,
        request: HostRequest,
        admission: Option<Arc<dyn Any + Send + Sync>>,
    ) -> CommandFuture<Result<HostResponse, CommandError>> {
        if !matches!(
            request,
            HostRequest::Builtin(BuiltinOperation::ResetSession)
        ) {
            return self.inner.request_with_admission(request, admission);
        }
        let pinned = admission
            .and_then(|admission| admission.downcast_ref::<PinnedCommandTarget>().cloned());
        let session = self.session.upgrade();
        let root = self.root.clone();
        Box::pin(async move {
            let pinned = pinned.ok_or(CommandError::StaleTarget)?;
            pinned.validate().map_err(|_| CommandError::StaleTarget)?;
            if pinned.target.id() != root.target.id() || !pinned.target.same_manager(&root.target) {
                return Err(sdk_command_error(CHILD_SESSION_RESET));
            }
            session
                .ok_or(CommandError::StaleTarget)?
                .replace(pinned)
                .await?;
            Ok(HostResponse::Completed)
        })
    }
}

fn sdk_command_error(error: impl ToString) -> CommandError {
    CommandError::Producer(error.to_string().into())
}

pub fn run(params: SdkParams) -> Result<()> {
    params.cli.warn_ignored_flags();
    let storage = StateDir::resolve().context("resolve state dir")?;
    let (out_tx, out_rx) = flume::unbounded::<String>();
    let writer_thread = std::thread::spawn(move || {
        let mut stdout = io::stdout().lock();
        while let Ok(line) = out_rx.recv() {
            if writeln!(stdout, "{line}").and(stdout.flush()).is_err() {
                break;
            }
        }
    });
    let session = SdkSession::new(params, storage, out_tx)?;
    let (user_tx, dispatcher) = session.spawn_dispatcher();
    let input_result = (|| -> Result<()> {
        for line in io::stdin().lock().lines() {
            let line = line.context("read stdin")?;
            if line.is_empty() {
                continue;
            }

            let msg: InboundMessage = match serde_json::from_str(&line) {
                Ok(msg) => msg,
                Err(e) => {
                    eprintln!("warning: ignoring malformed input line: {e}");
                    continue;
                }
            };

            if !handle_sdk_message(&session, &user_tx, msg)? {
                break;
            }
        }
        Ok(())
    })();

    drop(user_tx);
    session.input_closed.store(true, Ordering::Release);
    let ending = session.current();
    let dispatched = smol::block_on(finish_in_flight(
        &ending.shared,
        &ending.handle.manager,
        dispatcher,
    ));
    drop(ending);
    let runtime = session.current();
    let finished = smol::block_on(finish_in_flight(
        &runtime.shared,
        &runtime.handle.manager,
        async { Ok(()) },
    ));
    session.params.lua_handle.clear_orchestration_services();
    smol::block_on(runtime.close())?;
    drop(runtime);
    drop(session);
    let _ = writer_thread.join();
    input_result.and(dispatched).and(finished)
}

fn handle_sdk_message(
    session: &Arc<SdkSession>,
    user_tx: &Sender<UserInput>,
    msg: InboundMessage,
) -> Result<bool> {
    let runtime = session.current();
    let writer = &runtime.writer;
    let coordinator = &runtime.coordinator;
    let shared = &runtime.shared;
    let sdk_commands = &runtime.commands;
    let set_mode = |key: &str| {
        smol::block_on(makima_lua::orchestration::set_mode(
            &*runtime.orchestration,
            &runtime.root.target,
            &session.params.lua_handle.mode_registry(),
            key,
            &session.storage,
        ))
        .map(|_| ())
    };
    match msg.msg_type {
        InboundMessageType::User => {
            let Some(user) = parse_or_warn::<InboundUser>(msg.payload, "user message") else {
                return Ok(true);
            };
            let content = user.message.content;
            let prompt = content_text(&content).unwrap_or_else(|| content.to_string());
            let images = content_images(&content);
            if user_tx.send((prompt, images)).is_err() {
                return Ok(false);
            }
        }
        InboundMessageType::ControlRequest => {
            let Some(cr) = parse_or_warn::<InboundControlRequest>(msg.payload, "control_request")
            else {
                return Ok(true);
            };
            handle_control_request(
                &cr,
                ControlRequestContext {
                    writer,
                    coordinator,
                    shared,
                    commands: sdk_commands,
                    set_mode: &set_mode,
                },
            )?;
        }
        InboundMessageType::ControlResponse => {
            let Some(cr) = parse_or_warn::<InboundControlResponse>(msg.payload, "control_response")
            else {
                return Ok(true);
            };
            let data = cr.response;
            if let Some(req_id) = data.get("request_id").and_then(Value::as_str)
                && let Some((ask_id, answer_tx)) = shared.lock().unwrap().pending.remove(req_id)
            {
                let _ = answer_tx
                    .send(TaggedAnswer::new(ask_id, decode_permission_response(&data)).encode());
            }
        }
        InboundMessageType::ControlCancelRequest => {
            let Some(ccr) =
                parse_or_warn::<InboundControlCancelRequest>(msg.payload, "control_cancel_request")
            else {
                return Ok(true);
            };
            if let Some((ask_id, answer_tx)) =
                shared.lock().unwrap().pending.remove(&ccr.request_id)
            {
                let _ = answer_tx.send(TaggedAnswer::new(ask_id, PermissionAnswer::Deny).encode());
            }
        }
        InboundMessageType::Unknown(message_type) => {
            warn!("unknown inbound message type: {message_type}")
        }
    }
    Ok(true)
}

type UserInput = (String, Vec<ImageSource>);

/// Dispatches user messages in arrival order on a task of their own, so the
/// stdin loop keeps reading answers and interrupts while a command handler
/// waits on them.
struct UserDispatch {
    root: TrustedTarget,
    commands: Arc<SdkCommands>,
    input_tx: InteractiveInputSender,
    control_tx: InteractiveControlSender,
    writer: SdkWriter,
    shared: Arc<Mutex<Shared>>,
}

impl UserDispatch {
    /// `Ok(false)` means the session has ended.
    async fn dispatch(&self, prompt: &str, images: &[ImageSource]) -> Result<bool> {
        let Self {
            root,
            commands,
            input_tx,
            control_tx,
            writer,
            shared,
        } = self;
        {
            let mut shared = shared.lock().unwrap();
            if shared.work.is_empty() {
                shared.turn_start = Instant::now();
            }
            shared.dispatching = true;
        }
        let pinned = match PinnedCommandTarget::pin(root.clone()) {
            Ok(pinned) => pinned,
            Err(error) => {
                emit_command_result(writer, shared, true, error)?;
                return Ok(true);
            }
        };
        let input = match commands
            .dispatch_pinned_input(prompt, images, pinned.clone())
            .await
        {
            InputDispatch::Dispatched(CommandOutcome::AgentTurn(turn)) => {
                command_attachments::agent_input(turn)?
            }
            InputDispatch::LiteralInput(content) => AgentInput::new(
                content.text.to_string(),
                command_attachments::into_images(&content.attachments)?,
            ),
            InputDispatch::Dispatched(CommandOutcome::Completed) => {
                end_dispatch(shared, true).await;
                return Ok(true);
            }
            InputDispatch::Dispatched(CommandOutcome::FrontendFeedback(feedback)) => {
                emit_command_result(writer, shared, false, feedback.to_string())?;
                return Ok(true);
            }
            InputDispatch::Dispatched(CommandOutcome::IsolatedTurn(turn)) => {
                let images = command_attachments::into_images(&turn.content.attachments)?;
                let control = control_tx.clone();
                spawn_sdk_operation(writer, shared, move |cancel| async move {
                    control
                        .ask_isolated_to(
                            pinned.actor()?,
                            turn.content.text.to_string(),
                            images,
                            cancel,
                        )
                        .await
                })?;
                return Ok(true);
            }
            InputDispatch::Dispatched(CommandOutcome::ManualCompaction(instructions)) => {
                let control = control_tx.clone();
                spawn_sdk_operation(writer, shared, move |cancel| async move {
                    control
                        .compact_to(pinned.actor()?, instructions, cancel)
                        .await
                        .map(|()| String::new())
                })?;
                return Ok(true);
            }
            InputDispatch::Dispatched(CommandOutcome::Failed(error)) => {
                emit_command_result(writer, shared, true, error.to_string())?;
                return Ok(true);
            }
        };
        let admitted = pinned.actor().and_then(|actor| {
            admit_sdk_submission(
                shared,
                input_tx,
                input_tx.admitter().for_actor(actor),
                input,
            )
        });
        match admitted {
            Ok(admitted) => Ok(admitted),
            Err(error) => {
                emit_command_result(writer, shared, true, error)?;
                Ok(true)
            }
        }
    }
}

fn projected_commands(registry: &CommandRegistry, target: &TargetHandle) -> Result<Vec<Value>> {
    Ok(registry
        .presented_commands(target)?
        .iter()
        .map(|command| {
            serde_json::json!({
                "name": command.name.trim_start_matches('/'),
                "description": command.description,
                "argumentHint": command.argument_hint.as_deref().unwrap_or_default(),
            })
        })
        .collect())
}

fn watch_command_projection(
    writer: SdkWriter,
    registry: CommandRegistry,
    target: TargetHandle,
) -> smol::Task<()> {
    let subscription = registry.subscribe();
    smol::spawn(async move {
        let mut generation = subscription.generation();
        loop {
            generation = subscription.changed(generation).await;
            let Ok(commands) = projected_commands(&registry, &target) else {
                return;
            };
            let slash_commands = commands
                .iter()
                .filter_map(|command| command["name"].as_str().map(str::to_owned))
                .collect::<Vec<_>>();
            if writer
                .emit_system(
                    "commands_update",
                    serde_json::json!({
                        "commands": commands,
                        "slash_commands": slash_commands,
                    }),
                )
                .is_err()
            {
                return;
            }
        }
    })
}

fn restored_permission_mode(
    restored: bool,
    persisted_yolo: Option<bool>,
    startup: PermissionMode,
    explicit: bool,
) -> PermissionMode {
    if !restored || explicit {
        startup
    } else if let Some(yolo) = persisted_yolo {
        if yolo {
            PermissionMode::BypassPermissions
        } else if startup == PermissionMode::BypassPermissions {
            PermissionMode::Default
        } else {
            startup
        }
    } else {
        startup
    }
}

/// `plan` commits the plan mode and every other permission mode commits
/// build, both through {set_mode}; only `bypassPermissions` turns YOLO on.
fn set_permission_mode(
    mode: PermissionMode,
    coordinator: &SessionCoordinatorHandle,
    set_mode: &ModeSetter<'_>,
) -> Result<(), ConfigChangeError> {
    let value = if mode == PermissionMode::BypassPermissions {
        ENABLED_VALUE
    } else {
        DISABLED_VALUE
    };
    smol::block_on(coordinator.set_option(YOLO_OPTION_ID, value))?;
    set_mode(if mode == PermissionMode::Plan {
        ModeId::Plan.key()
    } else {
        ModeId::Build.key()
    })
}

/// Runs {operation} as the SDK's active work, so an interrupt cancels it,
/// and reports its text as the result.
fn spawn_sdk_operation<F>(
    writer: &SdkWriter,
    shared: &Arc<Mutex<Shared>>,
    operation: impl FnOnce(CancelToken) -> F + Send + 'static,
) -> Result<()>
where
    F: Future<Output = Result<String, String>> + Send + 'static,
{
    let (trigger, cancel) = CancelToken::new();
    let begun = match begin_sdk_operation(shared, trigger) {
        Ok(begun) => begun,
        Err(error) => return emit_command_result(writer, shared, true, error.into()),
    };
    let writer = writer.clone();
    let shared = Arc::clone(shared);
    smol::spawn(async move {
        let result = operation(cancel).await;
        end_sdk_operation(&shared, begun);
        let (is_error, text) = match result {
            Ok(text) => (false, text),
            Err(error) => (true, error),
        };
        if let Err(error) = emit_command_result(&writer, &shared, is_error, text) {
            warn!(%error, "failed to report SDK command result");
        }
    })
    .detach();
    Ok(())
}

fn emit_command_result(
    writer: &SdkWriter,
    shared: &Mutex<Shared>,
    is_error: bool,
    result: String,
) -> Result<()> {
    writer.emit(WireInner::Result(ResultPayload {
        subtype: if is_error {
            "error_during_execution"
        } else {
            "success"
        },
        is_error,
        duration_ms: shared.lock().unwrap().turn_start.elapsed().as_millis(),
        duration_api_ms: 0,
        num_turns: 0,
        result,
        total_cost_usd: 0.0,
        usage: TokenUsage::default(),
        permission_denials: Vec::new(),
    }))
}

type StoredSession = Session<Message, TokenUsage, ToolOutput>;

struct ResolvedSession {
    id: Option<SessionRef>,
    root_id: Option<AgentId>,
    parent: Option<ParentLink>,
    journal: Option<Arc<SessionJournal>>,
    seed: Option<StoredSession>,
    history: makima_agent::agent::HistorySnapshot,
    model: Option<String>,
    meta: SessionMeta,
}

async fn recover_sdk_session(
    restored: &mut ResolvedSession,
    storage: &StateDir,
    session_id: &SessionRef,
    root_id: AgentId,
    model: &str,
    cwd: &str,
    modes: &ModeRegistry,
) -> Result<(
    Arc<SessionLogCheckpoint>,
    Arc<session_lock::LockHeartbeat>,
    RecoveredSession,
)> {
    let sessions_dir = storage
        .ensure_subdir(SESSIONS_DIR)
        .context("create sessions dir")?;
    let claimed_lock = session_lock::claim(&sessions_dir, &session_id.id())
        .context("claim session lock")?
        .ok_or_else(|| eyre!("session is open in another terminal; close it there first"))?;
    let locked_id = session_id.id();
    let lock_guard = Arc::new(session_lock::LockHeartbeat::spawn(
        claimed_lock,
        move || {
            if let Ok(coordinator) = SessionCoordinatorHandle::resolve(locked_id) {
                coordinator.retire();
            }
        },
    ));
    reconcile_session_guarded(
        &sessions_dir,
        session_id.id(),
        BTreeMap::from([(
            session_id.id(),
            OwnedRestoreLease {
                guard: lock_guard.publication_guard(),
                keepalive: lock_guard.clone(),
            },
        )]),
        Arc::new(FileWriteLocks::new()),
        CancelToken::none(),
    )
    .await
    .map_err(|error| eyre!(error.to_string()))
    .context("reconcile pending file restores")?;
    let restore_legacy = restored.seed.is_some();
    let mut seed = restored.seed.take().unwrap_or_else(|| {
        let mut seed = StoredSession::new(model, cwd);
        seed.id = session_id.id();
        seed.adopt_history(&restored.history);
        seed.meta = restored.meta.clone();
        seed
    });
    seed.bind_root_id(root_id)?;
    if restore_legacy
        && let Some(warning) =
            makima_agent::session_recovery::resolve_legacy_mode(&mut seed, modes, storage)?
    {
        warn!(%warning, "legacy mode restoration fell back to build");
    }
    if restored.parent.is_some() {
        seed.set_parent(restored.parent.clone());
    }
    let checkpoint = Arc::new(SessionLogCheckpoint::open_with_session(
        storage.clone(),
        seed,
        Some(lock_guard.publication_guard()),
    )?);
    let claim: CheckpointClaim = Arc::new(|publication| {
        publication
            .try_publish()
            .then_some(())
            .ok_or_else(|| Arc::from(RECOVERY_CANCELLED))
    });
    let recovered = checkpoint
        .recover(CheckpointPublication::default(), claim)
        .await?;
    Ok((checkpoint, lock_guard, recovered))
}

fn startup_model(
    providers: &ProviderRegistry,
    startup: Model,
    restored: &ResolvedSession,
    explicit_model: bool,
    model_policy: &ModelPolicy,
) -> Model {
    if explicit_model {
        return startup;
    }
    restored
        .model
        .as_deref()
        .and_then(|spec| providers.model(spec).ok())
        .filter(|candidate| model_policy.allows(&candidate.spec()))
        .unwrap_or(startup)
}

fn resolve_session(cli: &Cli, cwd: &str, storage: &StateDir) -> Result<ResolvedSession> {
    let sessions_dir = storage
        .ensure_subdir(SESSIONS_DIR)
        .context("create sessions dir")?;
    // A bare continue flag (no ID) is rejected by the TUI guard before SDK
    // mode starts, so only a valued ID reaches this branch.
    let mut root_id = None;
    let mut parent = None;
    let mut journal = None;
    let mut seed = None;
    let (resumed_id, mut history, mut restored_model, mut meta) =
        if let Some(id) = cli.continue_session.as_ref().and_then(|o| o.as_deref()) {
            let session_ref: SessionRef = id
                .parse()
                .map_err(|e| eyre!("invalid session id {id}: {e}"))?;
            let session = StoredSession::load(session_ref.id(), storage)
                .map_err(|e| eyre!("load session {id}: {e}"))?;
            // The fork source is a read-only copy: no resume-block checks on it.
            if !cli.fork_session
                && let Some(block) = resume_block_for(&sessions_dir, &session, cwd)
            {
                return Err(eyre!("session {id}: {block}"));
            }
            let resumed = (!cli.fork_session).then_some(session_ref);
            if !cli.fork_session {
                root_id = session.root_agent_id();
                journal = Some(Arc::new(SessionJournal::from_session(&session)?));
                seed = Some(session.clone());
            }
            parent = cli.fork_session.then(|| fork_parent(&session));
            let model = session.model.clone();
            let meta = session.meta.clone();
            let history = session.history_snapshot();
            (resumed, history, Some(model), meta)
        } else if cli.last_session {
            match StoredSession::latest(cwd, storage) {
                Ok(Some(session)) => {
                    if let Some(block) = resume_block_for(&sessions_dir, &session, cwd) {
                        return Err(eyre!("session {}: {block}", session.id));
                    }
                    if cli.fork_session {
                        parent = Some(fork_parent(&session));
                    } else {
                        root_id = session.root_agent_id();
                        journal = Some(Arc::new(SessionJournal::from_session(&session)?));
                        seed = Some(session.clone());
                    }
                    let model = session.model.clone();
                    let meta = session.meta.clone();
                    (
                        (!cli.fork_session).then(|| SessionRef::from(session.id)),
                        session.history_snapshot(),
                        Some(model),
                        meta,
                    )
                }
                _ => (None, Default::default(), None, SessionMeta::default()),
            }
        } else {
            (None, Default::default(), None, SessionMeta::default())
        };

    if cli.fork_session {
        history.conversation = history.conversation.take().map(|source| {
            ConversationState::fresh(
                source.link(ConversationRelation::Branches, history.messages.len()),
            )
        });
    }
    let cli_session_id = cli.session_id.as_deref().map(|s| {
        s.parse::<SessionRef>()
            .map_err(|e| eyre!("invalid session id {s:?}: {e}"))
    });
    let cli_session_id = match cli_session_id {
        // A successful load proves the pinned ID pre-exists, so it must
        // belong to this directory and be free of other holders; a fresh ID
        // simply starts new.
        Some(Ok(id)) => {
            if resumed_id
                .as_ref()
                .is_some_and(|resumed| resumed.id() != id.id())
            {
                return Err(eyre!(SESSION_ID_CONFLICT));
            }
            match seed
                .as_ref()
                .filter(|session| session.id == id.id())
                .cloned()
                .map(Ok)
                .unwrap_or_else(|| StoredSession::load(id.id(), storage))
            {
                Ok(session) => {
                    if cli.fork_session {
                        return Err(eyre!(FORK_DESTINATION_EXISTS));
                    }
                    root_id = session.root_agent_id();
                    parent = session.parent().cloned();
                    journal = Some(Arc::new(SessionJournal::from_session(&session)?));
                    seed = Some(session.clone());
                    if !cli.fork_session {
                        history = session.history_snapshot();
                        restored_model = Some(session.model.clone());
                        meta = session.meta.clone();
                    }
                    if let Some(block) = resume_block_for(&sessions_dir, &session, cwd) {
                        return Err(eyre!("session {}: {block}", session.id));
                    }
                }
                Err(makima_storage::sessions::SessionError::Storage(
                    makima_storage::StorageError::NotFound(_),
                )) => {}
                Err(error) => return Err(eyre!("load session {id}: {error}")),
            }
            Some(id)
        }
        Some(Err(e)) => return Err(e),
        None => None,
    };

    Ok(ResolvedSession {
        id: cli_session_id.or(resumed_id),
        root_id,
        parent,
        journal,
        seed,
        history,
        model: restored_model,
        meta,
    })
}

fn fork_parent(session: &StoredSession) -> ParentLink {
    ParentLink {
        session: session.id,
        message_count: session.messages().len(),
        seq: session
            .state()
            .iter()
            .map(|record| record.seq)
            .chain(session.agent_revisions().iter().map(|(seq, _)| *seq))
            .max()
            .unwrap_or_default(),
    }
}

fn resume_block_for(
    sessions_dir: &Path,
    session: &StoredSession,
    cwd: &str,
) -> Option<session_lock::ResumeBlock> {
    session_lock::resume_block(
        &session.cwd,
        cwd,
        session_lock::open_elsewhere(sessions_dir, &session.id),
    )
}

fn parse_or_warn<T: serde::de::DeserializeOwned>(payload: Value, what: &str) -> Option<T> {
    match serde_json::from_value(payload) {
        Ok(v) => Some(v),
        Err(e) => {
            eprintln!("warning: ignoring malformed {what}: {e}");
            None
        }
    }
}

fn content_text(content: &Value) -> Option<String> {
    match content {
        Value::String(s) => Some(s.clone()),
        Value::Array(blocks) => Some(
            blocks
                .iter()
                .filter_map(|b| {
                    (b.get("type").and_then(Value::as_str) == Some("text"))
                        .then(|| b.get("text").and_then(Value::as_str))
                        .flatten()
                })
                .collect::<Vec<_>>()
                .join("\n"),
        ),
        _ => None,
    }
}

// Claude Code stream-json block shape:
// {"type":"image","source":{"type":"base64","media_type":"image/png","data":"..."}}
// `source` deserializes straight into ImageSource; malformed blocks are skipped.

fn content_images(content: &Value) -> Vec<ImageSource> {
    let Value::Array(blocks) = content else {
        return Vec::new();
    };
    blocks
        .iter()
        .filter(|b| b.get("type").and_then(Value::as_str) == Some("image"))
        .filter_map(|b| serde_json::from_value::<ImageSource>(b.get("source")?.clone()).ok())
        .collect()
}

struct ControlRequestContext<'a> {
    writer: &'a SdkWriter,
    coordinator: &'a SessionCoordinatorHandle,
    shared: &'a Mutex<Shared>,
    commands: &'a SdkCommands,
    set_mode: &'a ModeSetter<'a>,
}

type ModeSetter<'a> = dyn Fn(&str) -> Result<(), ConfigChangeError> + 'a;

/// Submits {input} like the TUI's input does: it joins the turn in flight or
/// starts one, which an interrupt then cancels. `Ok(false)` means the session
/// has ended; any other refusal, such as a full inbox, is an error.
#[cfg(test)]
async fn admit_sdk_turn(
    shared: &Mutex<Shared>,
    input_tx: &InteractiveInputSender,
    input: AgentInput,
) -> Result<bool, String> {
    let Ok(submission) = input_tx.admitter().ready().await else {
        return Ok(false);
    };
    admit_sdk_submission(shared, input_tx, submission, input)
}

fn admit_sdk_submission(
    shared: &Mutex<Shared>,
    input_tx: &InteractiveInputSender,
    submission: RootSubmission,
    input: AgentInput,
) -> Result<bool, String> {
    let mut shared = shared.lock().unwrap();
    check_sdk_work(&shared, |work| work.turn_id().is_none())?;
    let ticket = match submission.submit(input) {
        Ok((ticket, _)) => ticket,
        Err(error) if error.is_closed() => return Ok(false),
        Err(error) => return Err(error.to_string()),
    };
    if !shared
        .work
        .iter()
        .any(|work| work.turn_id() == Some(ticket.turn_id()))
    {
        shared.work.push(input_tx.turn_cancel(ticket));
    }
    Ok(true)
}

/// Starts a local operation as the SDK's only work in flight and returns
/// its number.
fn begin_sdk_operation(shared: &Mutex<Shared>, cancel: CancelTrigger) -> Result<u64, &'static str> {
    let mut shared = shared.lock().unwrap();
    check_sdk_work(&shared, |_| true)?;
    shared.work.push(WorkCancel::Control(cancel));
    shared.operation += 1;
    Ok(shared.operation)
}

/// Ends local operation {operation}, unless an interrupt already dropped it
/// and other work began since.
fn end_sdk_operation(shared: &Mutex<Shared>, operation: u64) {
    let mut shared = shared.lock().unwrap();
    if shared.operation == operation {
        shared.work.retain(|work| work.turn_id().is_some());
    }
    if shared.work.is_empty() {
        shared.idle.notify(usize::MAX);
    }
}

fn check_sdk_work(shared: &Shared, conflicts: fn(&WorkCancel) -> bool) -> Result<(), &'static str> {
    if shared.output_closed {
        return Err(OUTPUT_CLOSED);
    }
    if shared.work.iter().any(conflicts) {
        return Err(WORK_ACTIVE);
    }
    Ok(())
}

fn end_sdk_work(shared: &Mutex<Shared>) {
    let mut shared = shared.lock().unwrap();
    shared.work.clear();
    shared.idle.notify(usize::MAX);
}

/// Returns whether {turn_id}'s result waits for the dispatch in flight.
fn finish_sdk_turn(shared: &Mutex<Shared>, turn_id: TurnId) -> bool {
    let mut shared = shared.lock().unwrap();
    let tracked = shared.work.len();
    shared.work.retain(|work| work.turn_id() != Some(turn_id));
    if shared.work.is_empty() {
        shared.idle.notify(usize::MAX);
    }
    shared.dispatching && shared.work.len() == tracked
}

/// Cancels all SDK work in flight; the turns still report their results.
fn cancel_sdk_work(shared: &Mutex<Shared>) {
    let work = mem::take(&mut shared.lock().unwrap().work);
    for work in work {
        work.cancel();
    }
    end_sdk_work(shared);
}

/// Waits for the pump to report the dispatch in flight, unless it has
/// stopped or already reported it.
async fn end_dispatch(shared: &Mutex<Shared>, completed: bool) {
    let (reported, done) = flume::bounded(1);
    {
        let shared = shared.lock().unwrap();
        if shared.output_closed || !shared.dispatching {
            return;
        }
        let _ = shared.dispatch_end.0.send(DispatchEnd {
            completed,
            reported,
        });
    }
    let _ = done.recv_async().await;
}

/// The pump stopped (stdout broke): the work in flight can no longer report,
/// so it is cancelled and no new work starts.
fn close_output(shared: &Mutex<Shared>) {
    {
        let mut shared = shared.lock().unwrap();
        shared.output_closed = true;
        while shared.dispatch_end.1.try_recv().is_ok() {}
    }
    cancel_sdk_work(shared);
}

/// Stdin ended but stdout is still read: the user messages already read are
/// dispatched ({dispatched}), and the work in flight runs to its end and
/// reports, background subagents in {graph} and the turns their results start
/// included, while permission asks nobody can answer any more are denied.
/// Once stdout is gone too, nothing is left to wait for, a dispatch still
/// waiting on a command's handler included: it is dropped.
async fn finish_in_flight(
    shared: &Mutex<Shared>,
    graph: &AgentManagerHandle,
    dispatched: impl Future<Output = Result<()>>,
) -> Result<()> {
    let unanswered = {
        let mut shared = shared.lock().unwrap();
        shared.input_closed = true;
        mem::take(&mut shared.pending)
    };
    for (ask_id, answer_tx) in unanswered.into_values() {
        let _ = answer_tx.send(TaggedAnswer::new(ask_id, PermissionAnswer::Deny).encode());
    }
    let until = |done: fn(&Shared) -> bool| async move {
        loop {
            let idle = {
                let shared = shared.lock().unwrap();
                if done(&shared) {
                    return;
                }
                shared.idle.listen()
            };
            idle.await;
        }
    };
    let dispatched = smol::future::or(dispatched, async {
        until(|shared| shared.output_closed).await;
        Ok(())
    })
    .await;
    until(|shared| shared.work.is_empty()).await;
    smol::future::or(graph.wait_settled(), until(|shared| shared.output_closed)).await;
    dispatched
}

fn handle_control_request(
    cr: &InboundControlRequest,
    ctx: ControlRequestContext<'_>,
) -> Result<()> {
    let ControlRequestContext {
        writer,
        coordinator,
        shared,
        commands,
        set_mode,
    } = ctx;
    let ok = Some(Value::Object(Default::default()));
    match &cr.request.subtype {
        InboundControlRequestType::Initialize => {
            if let Some(extra) = cr.request.extra.as_object()
                && (extra.contains_key("hooks") || extra.contains_key("agents"))
            {
                eprintln!("note: hooks/agents payloads are ignored");
            }
            writer.emit_control_response(
                &cr.request_id,
                Some(serde_json::json!({"commands": commands.projection()})),
                None,
            )
        }
        InboundControlRequestType::Interrupt => {
            cancel_sdk_work(shared);
            writer.emit_control_response(&cr.request_id, ok, None)
        }
        InboundControlRequestType::SetPermissionMode => {
            let mode_str = cr.request.extra.get("mode").and_then(Value::as_str);
            match mode_str.and_then(PermissionMode::parse) {
                Some(mode) => match set_permission_mode(mode, coordinator, set_mode) {
                    Ok(()) => writer.emit_control_response(&cr.request_id, ok, None),
                    Err(error) => {
                        writer.emit_control_response(&cr.request_id, None, Some(error.to_string()))
                    }
                },
                None => writer.emit_control_response(
                    &cr.request_id,
                    None,
                    Some(format!(
                        "invalid permission mode: {}",
                        mode_str.unwrap_or("<missing>")
                    )),
                ),
            }
        }
        InboundControlRequestType::SetModel => {
            let read = coordinator.read();
            let current_model = read.model().unwrap_or_default();
            let options = read.options();
            let known: Vec<&str> = options
                .options
                .iter()
                .filter(|option| option.definition.id.as_ref() == MODEL_OPTION_ID)
                .flat_map(|option| option.definition.values.iter())
                .map(|value| value.value.as_ref())
                .collect();
            match resolve_set_model(cr.request.extra.get("model"), &current_model, &known) {
                Some(spec) => match smol::block_on(coordinator.set_option(MODEL_OPTION_ID, spec)) {
                    Ok(_) => writer.emit_control_response(&cr.request_id, ok, None),
                    Err(error) => {
                        writer.emit_control_response(&cr.request_id, None, Some(error.to_string()))
                    }
                },
                None => {
                    writer.emit_control_response(&cr.request_id, None, Some("invalid model".into()))
                }
            }
        }
        InboundControlRequestType::Unknown(subtype) => writer.emit_control_response(
            &cr.request_id,
            None,
            Some(format!("unsupported: {subtype}")),
        ),
    }
}

/// The spec a `set_model` request names; the coordinator validates it.
fn resolve_set_model(
    model_val: Option<&Value>,
    current_model: &str,
    known: &[&str],
) -> Option<String> {
    match model_val? {
        Value::Null => Some(current_model.to_owned()),
        Value::String(model_str) => Some(resolve_model_spec(model_str, known)),
        _ => None,
    }
}

/// A bare model id names the first {known} spec with that id, the way SDK
/// clients send ids without a provider.
fn resolve_model_spec(model_id: &str, known: &[&str]) -> String {
    if model_id.contains('/') {
        return model_id.to_string();
    }
    known
        .iter()
        .find(|spec| spec.split_once('/').is_some_and(|(_, id)| id == model_id))
        .map_or_else(|| model_id.to_string(), |spec| spec.to_string())
}

fn decode_permission_response(data: &Value) -> PermissionAnswer {
    match data.get("behavior").and_then(Value::as_str) {
        Some("allow") if data.get("updatedPermissions").is_some() => PermissionAnswer::AllowSession,
        Some("allow") => PermissionAnswer::AllowOnce,
        Some("deny") => match data.get("message").and_then(Value::as_str) {
            Some(msg) if !msg.is_empty() => PermissionAnswer::DenyWithGuidance(msg.to_string()),
            _ => PermissionAnswer::Deny,
        },
        _ => PermissionAnswer::Deny,
    }
}

struct EventPump {
    writer: SdkWriter,
    shared: Arc<Mutex<Shared>>,
    read: SessionReadHandle,
    permissions: Arc<PermissionManager>,
    answer_tx: Sender<String>,
    include_partial_messages: bool,
    synth: StreamSynth,
    tool_inputs: HashMap<String, (String, Value)>,
    result_text: String,
    /// Summed as the turns land: rates move mid-prompt, and only a turn knows
    /// the rate it paid.
    cost: Option<f64>,
    request_counter: u64,
    terminal_run_id: Option<u64>,
    observer: HeadlessObserver,
}

impl EventPump {
    fn spawn(mut self, event_rx: Receiver<Envelope>) -> smol::Task<()> {
        let dispatch_end = self.shared.lock().unwrap().dispatch_end.1.clone();
        smol::spawn(async move {
            loop {
                let next =
                    smol::future::or(async { event_rx.recv_async().await.map(Ok).ok() }, async {
                        dispatch_end.recv_async().await.map(Err).ok()
                    })
                    .await;
                let pumped = match next {
                    Some(Ok(envelope)) => self.pump(envelope),
                    Some(Err(end)) => self.report_dispatch(&event_rx, end),
                    None => break,
                };
                match pumped {
                    Ok(true) => {}
                    Ok(false) => break,
                    Err(e) => {
                        warn!(error = %e, "sdk event pump stopped");
                        break;
                    }
                }
            }
            close_output(&self.shared);
        })
    }

    /// Handles {envelope}; `Ok(false)` means the stream ended.
    fn pump(&mut self, envelope: Envelope) -> Result<bool> {
        if matches!(envelope.event, AgentEvent::StreamClosed) {
            return Ok(false);
        }
        self.observer.observe(&envelope);
        self.handle(envelope)?;
        Ok(true)
    }

    /// A dispatch ended. The turns a command's handler waited for are queued
    /// by now, so they are held first; a completed command reports the last
    /// of them as its result, like print does, or an empty one. Any other
    /// dispatch reported for itself, and what was held goes out as it came.
    fn report_dispatch(&mut self, event_rx: &Receiver<Envelope>, end: DispatchEnd) -> Result<bool> {
        while let Ok(envelope) = event_rx.try_recv() {
            if !self.pump(envelope)? {
                return Ok(false);
            }
        }
        let mut held = {
            let mut shared = self.shared.lock().unwrap();
            shared.dispatching = false;
            mem::take(&mut shared.held)
        };
        if end.completed {
            match held.pop() {
                Some(result) => self.writer.emit(WireInner::Result(result))?,
                None => emit_command_result(&self.writer, &self.shared, false, String::new())?,
            }
        } else {
            for result in held {
                self.writer.emit(WireInner::Result(result))?;
            }
        }
        let _ = end.reported.send(());
        Ok(true)
    }

    fn model_id(&self) -> String {
        self.read
            .model()
            .and_then(|spec| Model::parse_spec(&spec).ok().map(|(_, id)| id.to_owned()))
            .unwrap_or_default()
    }

    fn emit_stream(&self, events: Vec<Value>) -> Result<()> {
        events.into_iter().try_for_each(|event| {
            self.writer
                .emit(WireInner::StreamEvent(StreamEventPayload { event }))
        })
    }

    fn reset_turn(&mut self) {
        self.synth.reset();
        self.tool_inputs.clear();
        self.result_text.clear();
        self.cost = None;
        self.shared.lock().unwrap().pending.clear();
    }

    /// Emits a turn's result, or holds it for the dispatch in flight.
    fn emit_turn_result(
        &mut self,
        hold: bool,
        is_error: bool,
        result: String,
        num_turns: u32,
        usage: TokenUsage,
    ) -> Result<()> {
        let duration_ms = self.shared.lock().unwrap().turn_start.elapsed().as_millis();
        // Zero on an unpriced model, which is what its turns reported too.
        let total_cost_usd = self.cost.unwrap_or_default();
        let payload = ResultPayload {
            subtype: if is_error {
                "error_during_execution"
            } else {
                "success"
            },
            is_error,
            duration_ms,
            duration_api_ms: duration_ms,
            num_turns,
            result,
            total_cost_usd,
            usage,
            permission_denials: Vec::new(),
        };
        if hold {
            self.shared.lock().unwrap().held.push(payload);
        } else {
            self.writer.emit(WireInner::Result(payload))?;
        }
        self.reset_turn();
        Ok(())
    }

    fn handle(&mut self, envelope: Envelope) -> Result<()> {
        let parent_tool_use_id = envelope
            .subagent
            .as_ref()
            .map(|s| s.parent_tool_use_id.clone());

        match &envelope.event {
            AgentEvent::TextDelta { text } => {
                if self.include_partial_messages {
                    let model = self.model_id();
                    let events = self.synth.text_delta(&model, text);
                    self.emit_stream(events)?;
                }
            }
            AgentEvent::ThinkingDelta { text } => {
                if self.include_partial_messages {
                    let model = self.model_id();
                    let events = self.synth.thinking_delta(&model, text);
                    self.emit_stream(events)?;
                }
            }
            AgentEvent::ThinkingBlockEnd => {
                if self.include_partial_messages {
                    let events = self.synth.thinking_block_end();
                    self.emit_stream(events)?;
                }
            }
            AgentEvent::ToolStart(ts) => {
                let name = ts.tool.to_string();
                let input = ts.raw_input.clone().unwrap_or(Value::Null);

                if self.include_partial_messages {
                    let model = self.model_id();
                    let events = self.synth.tool_use(
                        &model,
                        &ts.id,
                        makima_to_claude_tool_name(&name),
                        &serde_json::to_string(&input)?,
                    );
                    self.emit_stream(events)?;
                }
                self.tool_inputs.insert(ts.id.clone(), (name, input));
            }
            AgentEvent::ToolPending { .. }
            | AgentEvent::ToolExecutionStart { .. }
            | AgentEvent::ToolOutput { .. }
            | AgentEvent::ToolDone(_)
            | AgentEvent::ToolMoved { .. }
            | AgentEvent::BackgroundDone(_)
            | AgentEvent::BackgroundLost { .. }
            | AgentEvent::QueueItemConsumed { .. }
            | AgentEvent::ModelSwitched { .. }
            | AgentEvent::QueueSettled
            | AgentEvent::AutoCompacting { .. }
            | AgentEvent::CompactionDone { .. }
            | AgentEvent::AuthRequired
            | AgentEvent::SubagentHistory { .. }
            | AgentEvent::SubagentClosed
            | AgentEvent::Post(_)
            | AgentEvent::Question { .. }
            | AgentEvent::ToolSnapshot { .. }
            | AgentEvent::ToolHeaderSnapshot { .. }
            | AgentEvent::LiveToolBuf { .. }
            | AgentEvent::Nudge
            | AgentEvent::PromptProgress { .. } => {}
            AgentEvent::Retry {
                attempt,
                message,
                delay_ms,
            } => {
                self.writer.emit_system(
                    "api_retry",
                    serde_json::json!({
                        "attempt": attempt,
                        "retry_delay_ms": delay_ms,
                        "error": message,
                    }),
                )?;
            }
            AgentEvent::TurnComplete(tc) => {
                add_cost(&mut self.cost, tc.cost);
                if self.include_partial_messages {
                    let events = self.synth.finish_message(&tc.usage);
                    self.emit_stream(events)?;
                }

                let content_value = serde_json::to_value(&tc.message.content)?;
                if parent_tool_use_id.is_none() {
                    self.result_text = content_text(&content_value).unwrap_or_default();
                }
                self.writer.emit(WireInner::Assistant(AssistantPayload {
                    message: AssistantMessage {
                        id: wire_uuid(),
                        model: tc.model.clone(),
                        role: "assistant",
                        content: map_tool_names_in_content(&content_value),
                        stop_reason: None,
                        usage: tc.usage,
                    },
                    parent_tool_use_id,
                }))?;
            }
            AgentEvent::ToolResultsSubmitted { message } => {
                self.writer.emit(WireInner::User(UserPayload {
                    message: UserMessage {
                        role: "user",
                        content: serde_json::to_value(&message.content)?,
                    },
                    parent_tool_use_id,
                }))?;
            }
            AgentEvent::PermissionRequest { id, tool, .. } => {
                let answer_tx = if let Some(subagent) = &envelope.subagent {
                    let Some(answer_tx) = &subagent.answer_tx else {
                        warn!(agent_id = %subagent.agent_id, "subagent permission request has no answer channel");
                        return Ok(());
                    };
                    answer_tx.clone()
                } else {
                    self.answer_tx.clone()
                };
                if self.permissions.is_yolo() {
                    let _ = answer_tx
                        .send(TaggedAnswer::new(id, PermissionAnswer::AllowSession).encode());
                    return Ok(());
                }

                let (tool_name, input) = self
                    .tool_inputs
                    .get(id)
                    .cloned()
                    .unwrap_or_else(|| (tool.to_string(), Value::Null));

                self.request_counter += 1;
                let req_id = format!("req_{}", self.request_counter);
                {
                    let mut shared = self.shared.lock().unwrap();
                    if shared.input_closed {
                        drop(shared);
                        let _ =
                            answer_tx.send(TaggedAnswer::new(id, PermissionAnswer::Deny).encode());
                        return Ok(());
                    }
                    shared
                        .pending
                        .insert(req_id.clone(), (id.clone(), answer_tx));
                }

                self.writer
                    .emit(WireInner::ControlRequest(ControlRequestPayload {
                        request_id: req_id,
                        request: ControlRequestInner {
                            subtype: "can_use_tool",
                            tool_name: Some(makima_to_claude_tool_name(&tool_name).into()),
                            input: Some(input),
                            tool_use_id: Some(id.clone()),
                        },
                    }))?;
            }
            AgentEvent::TurnOutcome(_) if envelope.subagent.is_some() => {}
            AgentEvent::TurnOutcome(outcome) => {
                let hold = finish_sdk_turn(&self.shared, outcome.turn_id());
                self.terminal_run_id = envelope.presentation();
                let result = mem::take(&mut self.result_text);
                match outcome {
                    TurnOutcome::Completed {
                        usage, num_turns, ..
                    } => self.emit_turn_result(hold, false, result, *num_turns, *usage)?,
                    TurnOutcome::Failed {
                        usage,
                        num_turns,
                        failure,
                        ..
                    } => self.emit_turn_result(
                        hold,
                        true,
                        if result.is_empty() {
                            failure.user_message.clone()
                        } else {
                            result
                        },
                        *num_turns,
                        *usage,
                    )?,
                    TurnOutcome::Cancelled {
                        usage, num_turns, ..
                    } => self.emit_turn_result(hold, true, result, *num_turns, *usage)?,
                }
            }
            AgentEvent::ControlComplete { .. } | AgentEvent::StreamClosed => {}
            AgentEvent::ControlError { .. } if envelope.subagent.is_some() => {}
            AgentEvent::ControlError { message } => {
                end_sdk_work(&self.shared);
                if self.terminal_run_id == envelope.presentation() {
                    self.writer
                        .emit_system("warning", serde_json::json!({ "message": message }))?;
                } else {
                    self.emit_turn_result(false, true, message.clone(), 0, TokenUsage::default())?;
                }
            }
        }
        Ok(())
    }
}

fn map_tool_names_in_content(content: &Value) -> Value {
    match content {
        Value::Array(blocks) => {
            let mapped: Vec<Value> = blocks
                .iter()
                .map(|block| {
                    if block.get("type").and_then(Value::as_str) == Some("tool_use")
                        && let Some(name) = block.get("name").and_then(Value::as_str)
                    {
                        let mut b = block.clone();
                        b["name"] = Value::String(makima_to_claude_tool_name(name).to_string());
                        return b;
                    }
                    block.clone()
                })
                .collect();
            Value::Array(mapped)
        }
        other => other.clone(),
    }
}

#[cfg(test)]
mod tests {
    use makima_lua::orchestration::AgentOrchestration;
    use makima_storage::id::MakimaId;

    use super::*;
    use makima_agent::actor::{ActorLifecycle, ActorStatus};
    use makima_agent::actor::{ConfigChange, ConfigPatch};
    use makima_agent::manager::{AgentResidency, GraphLifecycle};
    use makima_agent::session_coordinator::{SessionCheckpoint, SessionCheckpointKind};
    use makima_agent::state::{StateError, StateRevision, StateScope, StateSlotSpec};
    use makima_agent::tools::ToolRegistry;
    use makima_agent::{AgentError, AgentId, AgentRef, ModelSource, PreparedModel};
    use makima_commands::{CommandError, CommandFuture, TargetCapabilities, TargetCapability};
    use makima_lua::orchestration::OrchestrationServices;
    use makima_providers::provider::{BoxFuture, Provider};
    use makima_providers::{
        ContentBlock, Invocation, ProviderEvent, RequestDelivery, RequestOptions, Role,
        StreamResponse,
    };
    use makima_storage::blobs::{BlobStore, FileState};
    use makima_storage::checkpoint::{CheckpointWriter, ClaimingWriter};
    use makima_storage::restore::{
        PathRestore, RestoreChoice, RestoreEndpoint, RestoreProgress, RestoreStore,
        RestoreTransition, TransitionRef,
    };
    use makima_storage::restore_digest::session_digest;
    use makima_storage::session_state::StateChange;
    use makima_storage::sessions::Prefs;
    use std::path::PathBuf;
    use std::time::Duration;
    use test_case::test_case;

    const SDK_GATE_TIMEOUT: Duration = Duration::from_secs(10);
    const SDK_NESTED_TOOL: &str = "sdk_nested_task";
    const SDK_ROOT_PROMPT: &str = "sdk root request";
    const SDK_CHILD_PROMPT: &str = "sdk child request";
    const SDK_COMMAND_DONE: &str = "sdk command done";
    const SDK_CONTEXT_MANIFEST: &str = "makima/context-manifest";
    const SDK_GRANDCHILD_PROMPT: &str = "sdk grandchild request";
    const SDK_ROOT_RESPONSE: &str = "sdk root response";
    const SDK_CHILD_RESPONSE: &str = "sdk child response";
    const SDK_GRANDCHILD_RESPONSE: &str = "sdk grandchild response";
    const SDK_SETTLEMENT_TIMEOUT: &str = "SDK managed turn did not settle";
    const RESET_INITIALIZATION_TIMEOUT: &str = "SDK reset session initialization did not settle";
    const RESET_DISPATCH_TIMEOUT: &str = "SDK reset or queued command dispatch did not settle";
    const SDK_GATED_REPLY: &str = "released reply";
    const CHECKPOINT_FAILED: &str = "checkpoint failed";

    const REJECTED_ATTACHMENT: &str = "command rejected attachment";
    const STARTUP_MODEL: &str = "anthropic/claude-sonnet-4-20250514";
    const UNSTARTED_ROOT: &str = "root never starts";
    const TARGET_MODEL: &str = "openai/gpt-5";
    const SDK_DISCOVERED_MODEL: &str = "openai/sdk-discovered";
    const SDK_LATE_MODEL: &str = "openai/sdk-discovered-late";
    const RESTORED_MODEL: &str = "anthropic/claude-opus-4-6";
    const RESET_PROVIDER: &str = "sdk-reset";
    const RESET_MODEL: &str = "sdk-reset/offline";
    const RESET_STATE_KEY: &str = "sdk/reset-state";
    const RESET_STATE_VALUE: &str = "previous session state";
    const RESET_BACKUP: &str = "sessions-before-reset";
    const RESET_TOOL: &str = "sdk_reset_active_tool";
    const RESET_REJECTED: &str = "active reset rejected";
    const LEGACY_PLAN_FILE: &str = "legacy-sdk-plan.md";
    const LEGACY_PLAN_CONTENT: &str = "preserved legacy SDK plan";
    const RESTORE_CHANGED: &str = "pending SDK restore content";
    const RESTORE_CONFLICT: &str = "independent SDK edit";

    fn pending_sdk_restore(
        storage: &StateDir,
        session: &StoredSession,
        path: &Path,
    ) -> TransitionRef {
        let blobs = BlobStore::new(storage.clone());
        let endpoint = RestoreEndpoint {
            session: session.id,
            branch: None,
            seq: session.sequence_high_water().unwrap_or_default(),
            digest: session_digest(session).unwrap(),
        };
        RestoreStore::new(storage.clone())
            .prepare(
                session.id,
                RestoreTransition {
                    id: MakimaId::generate(),
                    choice: RestoreChoice::FilesOnly,
                    source: endpoint.clone(),
                    target: endpoint,
                    inverse_of: None,
                    paths: vec![PathRestore {
                        path: path.to_path_buf(),
                        before: FileState::Blob(
                            blobs
                                .put(session.id, LEGACY_PLAN_CONTENT.as_bytes())
                                .unwrap(),
                        ),
                        after: FileState::Blob(
                            blobs.put(session.id, RESTORE_CHANGED.as_bytes()).unwrap(),
                        ),
                    }],
                    skipped: Vec::new(),
                },
            )
            .unwrap()
    }

    fn reset_sdk_params(cwd: PathBuf) -> SdkParams {
        use clap::Parser;
        SdkParams {
            cli: Cli::parse_from(["makima"]),
            cwd: cwd.clone(),
            model: Model::from_spec(RESET_MODEL).unwrap(),
            explicit_model: false,
            config: Default::default(),
            permissions_config: Default::default(),
            timeouts: Default::default(),
            prompt_slots: Default::default(),
            defaults: Default::default(),
            model_policy: Arc::default(),
            providers: makima_lua::test_support::offline_registry(RESET_PROVIDER, Arc::default()),
            plugin_rules: Arc::default(),
            project_config: ProjectConfig::for_project(&cwd),
            commands: Vec::new(),
            command_registry: CommandRegistry::new(),
            session_options: Default::default(),
            lua_handle: makima_lua::EventHandle::disconnected_for_test(),
        }
    }

    #[test_case(StateScope::Session, false; "fresh_session_scope")]
    #[test_case(StateScope::Agent, false; "fresh_root_scope")]
    #[test_case(StateScope::Session, true; "failed_preparation_keeps_session_scope")]
    #[test_case(StateScope::Agent, true; "failed_preparation_keeps_root_scope")]
    fn protocol_reset_replaces_runtime_without_destroying_previous_state(
        scope: StateScope,
        fail: bool,
    ) {
        smol::block_on(async {
            let directory = tempfile::tempdir().unwrap();
            let storage = StateDir::from_path(directory.path().to_path_buf());
            let (out_tx, out_rx) = flume::unbounded();
            let session = futures_lite::future::or(
                smol::unblock({
                    let storage = storage.clone();
                    let cwd = directory.path().to_path_buf();
                    move || SdkSession::new(reset_sdk_params(cwd), storage, out_tx).unwrap()
                }),
                async {
                    smol::Timer::after(SDK_GATE_TIMEOUT).await;
                    panic!("{RESET_INITIALIZATION_TIMEOUT}");
                },
            )
            .await;
            let previous = session.current();
            let previous_id = previous.handle.session_id.id();
            let previous_root = previous.root.target.id();
            let access = previous.root.target.state_access();
            let spec = StateSlotSpec {
                key: RESET_STATE_KEY.into(),
                scope,
                version: 1,
            };
            let slot = previous
                .handle
                .manager
                .journal()
                .register_state::<String>(spec.clone())
                .unwrap();
            slot.set(&access, RESET_STATE_VALUE.into(), Some(StateRevision(0)))
                .unwrap();
            previous.checkpoint.drain().await.unwrap();
            let sessions = storage.path().join(SESSIONS_DIR);
            let backup = storage.path().join(RESET_BACKUP);
            if fail {
                std::fs::rename(&sessions, &backup).unwrap();
                std::fs::write(&sessions, RESET_STATE_VALUE).unwrap();
            }
            let (user_tx, dispatcher) = session.spawn_dispatcher();
            let send = |prompt: &str| {
                let message =
                    serde_json::json!({ "type": "user", "message": { "content": prompt } });
                assert!(
                    handle_sdk_message(
                        &session,
                        &user_tx,
                        serde_json::from_value(message).unwrap()
                    )
                    .unwrap()
                );
            };
            send(RESET_COMMAND);
            let mut first_result = None;
            if fail {
                first_result = Some(
                    futures_lite::future::or(
                        async {
                            loop {
                                let line = out_rx.recv_async().await.unwrap();
                                let message: Value = serde_json::from_str(&line).unwrap();
                                if message["type"] == "result" {
                                    break message;
                                }
                            }
                        },
                        async {
                            smol::Timer::after(SDK_GATE_TIMEOUT).await;
                            panic!("{SDK_SETTLEMENT_TIMEOUT}");
                        },
                    )
                    .await,
                );
                std::fs::remove_file(&sessions).unwrap();
                std::fs::rename(&backup, &sessions).unwrap();
            }
            send("/yolo");
            drop(user_tx);
            futures_lite::future::or(
                async {
                    dispatcher.await.unwrap();
                },
                async {
                    smol::Timer::after(SDK_GATE_TIMEOUT).await;
                    let current = session.current();
                    let protocol: Vec<_> = out_rx.try_iter().collect();
                    panic!("{RESET_DISPATCH_TIMEOUT}; same_runtime={}; previous={:?}; current={:?}; protocol={protocol:?}",
                        Arc::ptr_eq(&previous, &current), previous.handle.manager.snapshot(), current.handle.manager.snapshot());
                },
            )
            .await;
            let current = session.current();
            let results: Vec<Value> = first_result
                .into_iter()
                .chain(out_rx.try_iter().filter_map(|line| {
                    let message: Value = serde_json::from_str(&line).unwrap();
                    (message["type"] == "result").then_some(message)
                }))
                .collect();
            assert_eq!(results.len(), 2);
            assert_eq!(results[0]["is_error"], fail, "{results:?}");
            assert_eq!(results[1]["is_error"], false, "{results:?}");
            assert_eq!(
                results[1]["session_id"],
                current.handle.session_id.to_string()
            );
            assert!(current.handle.permissions.is_yolo());
            if fail {
                assert_eq!(current.handle.session_id.id(), previous_id);
                assert_eq!(current.root.target.id(), previous_root);
                assert_eq!(
                    slot.get(&access).unwrap().value.as_deref(),
                    Some(RESET_STATE_VALUE)
                );
                slot.set(&access, RESET_STATE_VALUE.into(), None).unwrap();
            } else {
                assert_ne!(current.handle.session_id.id(), previous_id);
                assert_ne!(current.root.target.id(), previous_root);
                let fresh = current
                    .handle
                    .manager
                    .journal()
                    .register_state::<String>(spec)
                    .unwrap()
                    .get(&current.root.target.state_access())
                    .unwrap();
                assert_eq!(fresh.value, None);
                assert_eq!(fresh.revision, StateRevision(0));
                assert!(matches!(slot.get(&access), Err(StateError::StaleAccess)));
                assert!(matches!(
                    slot.set(&access, RESET_STATE_VALUE.into(), None),
                    Err(StateError::StaleAccess)
                ));
            }
            let historical = StoredSession::load(previous_id, &storage).unwrap();
            let state = historical.latest_state().unwrap();
            let agent = match scope {
                StateScope::Session => None,
                StateScope::Agent => Some(previous_root),
            };
            assert_eq!(
                state.scopes[&agent].plugins[RESET_STATE_KEY].value,
                Some(serde_json::json!(RESET_STATE_VALUE))
            );
            current.close().await.unwrap();
        });
    }

    #[test_case(false; "unmodified_v2_resume")]
    #[test_case(true; "pending_restore_after_persisted_v2_migration")]
    fn sdk_legacy_v2_plan_resume_preserves_saved_path_and_written_state_before_idle_reset(
        pending: bool,
    ) {
        use clap::Parser;
        let directory = tempfile::tempdir().unwrap();
        let storage = StateDir::from_path(directory.path().to_path_buf());
        let plan = directory.path().join(LEGACY_PLAN_FILE);
        std::fs::write(&plan, LEGACY_PLAN_CONTENT).unwrap();
        let mut saved = StoredSession::new(RESET_MODEL, &directory.path().to_string_lossy());
        saved.replace_messages(vec![Message::user(SDK_ROOT_PROMPT.into())]);
        saved.save(&storage).unwrap();
        let path = storage
            .path()
            .join(SESSIONS_DIR)
            .join(format!("{}.jsonl", saved.id));
        let records = std::fs::read_to_string(&path).unwrap();
        let mut records: Vec<Value> = records
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        records.retain(|record| !matches!(record["t"].as_str(), Some("agent" | "state")));
        records[0]["v"] = Value::from(2);
        let meta = records
            .iter_mut()
            .find(|record| record["t"] == "meta")
            .unwrap();
        meta["mode"] = Value::String(ModeId::Plan.key().into());
        meta["plan_path"] = serde_json::to_value(&plan).unwrap();
        meta["plan_written"] = Value::Bool(true);
        std::fs::write(
            &path,
            records
                .iter()
                .map(|record| format!("{record}\n"))
                .collect::<String>(),
        )
        .unwrap();
        let reference = pending.then(|| {
            let mut stored = StoredSession::load(saved.id, &storage).unwrap();
            stored.save(&storage).unwrap();
            let stored = StoredSession::load(saved.id, &storage).unwrap();
            let reference = pending_sdk_restore(&storage, &stored, &plan);
            std::fs::write(&plan, RESTORE_CHANGED).unwrap();
            reference
        });
        let mut params = reset_sdk_params(directory.path().to_path_buf());
        params.cli = Cli::parse_from(["makima", "-c", &saved.id.to_string()]);
        let (out_tx, _out_rx) = flume::unbounded();
        let session = SdkSession::new(params, storage.clone(), out_tx).unwrap();
        let resumed = session.current();
        if let Some(reference) = reference {
            let progress = RestoreStore::new(storage.clone())
                .load(&reference)
                .unwrap()
                .progress;
            assert!(progress.contains(&RestoreProgress::RollbackIntent));
            assert_eq!(progress.last(), Some(&RestoreProgress::Complete));
        }
        let root = resumed.root.target.id();
        assert_eq!(resumed.handle.session_id.id(), saved.id);
        assert_eq!(
            resumed
                .root
                .target
                .effective_config()
                .unwrap()
                .unwrap()
                .mode,
            AgentMode::Plan(plan.clone())
        );
        let migrated = StoredSession::load(saved.id, &storage).unwrap();
        let plans =
            makima_agent::plan_state::plan_entries(&migrated.latest_state().unwrap()).unwrap();
        assert_eq!(plans[&root].path, plan);
        assert!(plans[&root].written);
        assert_eq!(std::fs::read_to_string(&plan).unwrap(), LEGACY_PLAN_CONTENT);
        let outcome = smol::block_on(resumed.commands.dispatch_pinned_input(
            RESET_COMMAND,
            &[],
            PinnedCommandTarget::pin(resumed.root.clone()).unwrap(),
        ));
        assert!(
            matches!(
                outcome,
                InputDispatch::Dispatched(CommandOutcome::Completed)
            ),
            "{outcome:?}"
        );
        let fresh = session.current();
        assert_ne!(fresh.handle.session_id.id(), saved.id);
        assert_ne!(fresh.root.target.id(), root);
        let mut replacement = StoredSession::load(fresh.handle.session_id.id(), &storage).unwrap();
        fresh
            .handle
            .manager
            .journal()
            .snapshot()
            .apply_to(&mut replacement)
            .unwrap();
        assert!(
            makima_agent::plan_state::plan_entries(&replacement.latest_state().unwrap())
                .unwrap()
                .is_empty()
        );
        let historical = StoredSession::load(saved.id, &storage).unwrap();
        let plans =
            makima_agent::plan_state::plan_entries(&historical.latest_state().unwrap()).unwrap();
        assert_eq!(plans[&root].path, plan);
        assert!(plans[&root].written);
        assert_eq!(std::fs::read_to_string(&plan).unwrap(), LEGACY_PLAN_CONTENT);
        smol::block_on(session.drain_retired()).unwrap();
        smol::block_on(fresh.close()).unwrap();
    }

    #[test_case(false; "published_forward")]
    #[test_case(true; "published_conflict_blocks_recovery")]
    fn sdk_reconciles_published_restore_before_checkpoint_recovery(conflict: bool) {
        use clap::Parser;
        smol::block_on(async {
            let directory = tempfile::tempdir().unwrap();
            let storage = StateDir::from_path(directory.path().to_path_buf());
            let cwd = directory.path().to_string_lossy().into_owned();
            let mut saved = StoredSession::new(RESET_MODEL, &cwd);
            let root = AgentId::generate();
            saved.bind_root_id(root).unwrap();
            saved.save(&storage).unwrap();
            let path = directory.path().join(LEGACY_PLAN_FILE);
            let reference = pending_sdk_restore(&storage, &saved, &path);
            saved.push_restore_ref(reference.clone());
            saved.save(&storage).unwrap();
            std::fs::write(
                &path,
                if conflict {
                    RESTORE_CONFLICT
                } else {
                    LEGACY_PLAN_CONTENT
                },
            )
            .unwrap();
            let log = storage
                .path()
                .join(SESSIONS_DIR)
                .join(format!("{}.jsonl", saved.id));
            let before = std::fs::read(&log).unwrap();
            let cli = Cli::parse_from(["makima", "-c", &saved.id.to_string()]);
            let mut restored = resolve_session(&cli, &cwd, &storage).unwrap();
            let result = recover_sdk_session(
                &mut restored,
                &storage,
                &SessionRef::from(saved.id),
                root,
                RESET_MODEL,
                &cwd,
                &ModeRegistry::builtin(),
            )
            .await;
            if conflict {
                assert!(result.is_err());
                assert_eq!(std::fs::read(log).unwrap(), before);
                assert_eq!(std::fs::read_to_string(&path).unwrap(), RESTORE_CONFLICT);
                assert!(
                    session_lock::claim(&storage.path().join(SESSIONS_DIR), &saved.id)
                        .unwrap()
                        .is_none()
                );
                let snapshot = RestoreStore::new(storage.clone()).load(&reference).unwrap();
                assert!(matches!(
                    snapshot.progress.last(),
                    Some(RestoreProgress::Conflict { .. })
                ));
                assert!(!snapshot.progress.contains(&RestoreProgress::Complete));
            } else {
                let (checkpoint, lease, _) = result.unwrap();
                assert_eq!(std::fs::read_to_string(&path).unwrap(), RESTORE_CHANGED);
                assert!(
                    RestoreStore::new(storage.clone())
                        .pending(saved.id)
                        .unwrap()
                        .is_empty()
                );
                checkpoint.drain().await.unwrap();
                drop(lease);
            }
        });
    }

    #[test]
    fn live_lua_repeated_reset_rebinds_observers_and_retires_old_commands() {
        let directory = tempfile::tempdir().unwrap();
        let registry = Arc::clone(ToolRegistry::global_arc());
        let (ui, notices) = makima_lua::test_support::RecordingUi::new();
        let host = makima_lua::PluginHost::with_session_provider_preparer(
            registry,
            CommandRegistry::new(),
            true,
            Arc::new(makima_lua::test_support::InMemoryFs::new()),
            Some(directory.path().to_path_buf()),
            None,
            makima_lua::ui_sink::HostUi::Sink(Box::new(ui)),
        )
        .unwrap();
        host.load_source("sdk_reset_live", r#"
            local slot = makima.state.register("value", { scope = "session", version = 1 })
            local held
            local resumes = 0
            slot:on_change(function(_, event)
                if event.reason == "resume" then resumes = resumes + 1 end
            end)
            makima.api.create_autocmd("SessionReset", { callback = function(ev)
                local focus = assert(makima.ui.focused_agent())
                makima.ui.flash(makima.json.encode({ ended = ev.data.session_id, root = focus:id() }))
            end })
            makima.api.register_command({ name = "/sdk_reset_capture", tui_only = false,
                handler = function(_, ctx)
                    local value, revision = slot:get(ctx)
                    assert(value == nil and revision == 0, tostring(revision))
                    assert(slot:set(ctx, "previous"))
                    held = ctx
                end,
            })
            makima.api.register_command({ name = "/sdk_reset_probe", tui_only = false,
                handler = function(_, ctx)
                    assert(resumes == 0, "fresh reset emitted resume")
                    local old, err = slot:get(held)
                    assert(old == nil and type(err) == "string", tostring(err))
                    local value, revision = slot:get(ctx)
                    assert(value == nil and revision == 0, tostring(revision))
                    local snapshot = assert(makima.session.read({ session = ctx:session_id() }))
                    assert(snapshot.id == ctx:session_id())
                    makima.ui.flash(makima.json.encode({ session = snapshot.id, root = ctx:agent():ref():id() }))
                end,
            })
            makima.api.register_tool({ name = "sdk_reset_active_tool", description = "exercise root-tool reset",
                schema = { type = "object", properties = {} }, handler = function(_, ctx)
                    local ok, err = makima.api.run_command(ctx, "/reset")
                    assert(ok == nil and type(err) == "string", tostring(err))
                    makima.ui.flash("active reset rejected")
                    return "active reset rejected"
                end,
            })
        "#).unwrap();
        let mut params = reset_sdk_params(directory.path().to_path_buf());
        params.command_registry = host.command_registry();
        params.lua_handle = host.event_handle();
        let commands = params.command_registry.clone();
        let (out_tx, _out_rx) = flume::unbounded();
        let session = SdkSession::new(
            params,
            StateDir::from_path(directory.path().to_path_buf()),
            out_tx,
        )
        .unwrap();
        host.wait_for_worker_barrier_for_test();
        let run = |runtime: &Arc<SdkRuntime>, command: &str| {
            let outcome = smol::block_on(runtime.commands.dispatch_pinned_input(
                command,
                &[],
                PinnedCommandTarget::pin(runtime.root.clone()).unwrap(),
            ));
            assert!(
                matches!(
                    outcome,
                    InputDispatch::Dispatched(CommandOutcome::Completed)
                ),
                "{outcome:?}"
            );
        };
        for _ in 0..2 {
            let old = session.current();
            run(&old, "/sdk_reset_capture");
            run(&old, RESET_COMMAND);
            let current = session.current();
            let notice = notices.recv_timeout(SDK_GATE_TIMEOUT).unwrap();
            let notice: Value = serde_json::from_str(&notice).unwrap();
            assert_eq!(notice["ended"], old.handle.session_id.to_string());
            assert_eq!(notice["root"], current.root.target.id().to_string());
            assert!(matches!(
                smol::block_on(old.commands.dispatch_input("/yolo", &[])),
                InputDispatch::Dispatched(CommandOutcome::Failed(_))
            ));
            assert!(SessionCoordinatorHandle::resolve(old.handle.session_id.id()).is_err());
            run(&current, "/sdk_reset_probe");
            let notice = notices.recv_timeout(SDK_GATE_TIMEOUT).unwrap();
            let notice: Value = serde_json::from_str(&notice).unwrap();
            assert_eq!(notice["session"], current.handle.session_id.to_string());
            assert_eq!(notice["root"], current.root.target.id().to_string());
            smol::block_on(session.drain_retired()).unwrap();
        }
        let current = session.current();
        let actor = current.root.target.actor().unwrap();
        smol::block_on(async {
            actor
                .reserve_prepared_operation(None)
                .unwrap()
                .commit_when_ready(Some(ConfigChange::Patch(ConfigPatch {
                    model: Some(PreparedModel {
                        model: Model::from_spec(RESET_MODEL).unwrap(),
                        provider: Arc::new(ResetToolProvider),
                    }),
                    ..Default::default()
                })))
                .await
                .unwrap();
        });
        let ticket = smol::block_on(
            current
                .handle
                .input_tx
                .admit(AgentInput::new(SDK_ROOT_PROMPT.into(), Vec::new())),
        )
        .unwrap();
        let outcome = smol::block_on(futures_lite::future::or(ticket.wait(), async {
            smol::Timer::after(SDK_GATE_TIMEOUT).await;
            panic!("{SDK_SETTLEMENT_TIMEOUT}");
        }));
        assert!(
            matches!(outcome, TurnOutcome::Completed { .. }),
            "{outcome:?}"
        );
        assert_eq!(
            notices.recv_timeout(SDK_GATE_TIMEOUT).unwrap(),
            RESET_REJECTED
        );
        assert!(Arc::ptr_eq(&current, &session.current()));
        smol::block_on(current.close()).unwrap();
        session.params.lua_handle.clear_orchestration_services();
        let target = current.commands.target.clone();
        drop(current);
        drop(session);
        assert!(!commands.resolves_input_for(&target, RESET_COMMAND));
    }

    struct ResetToolProvider;

    impl Provider for ResetToolProvider {
        fn stream_message<'a>(
            &'a self,
            _: &'a Model,
            messages: &'a [Message],
            _: &'a str,
            _: &'a Value,
            _: &'a Sender<ProviderEvent>,
            _: RequestOptions,
            _: Option<&'a SessionRef>,
            _: &'a Invocation,
            _: RequestDelivery,
        ) -> BoxFuture<'a, Result<StreamResponse, AgentError>> {
            let finished = messages.iter().flat_map(|message| &message.content).any(|block| {
                matches!(block, ContentBlock::ToolResult { tool_use_id, is_error: false, .. } if tool_use_id == RESET_TOOL)
            });
            let content = if finished {
                ContentBlock::Text {
                    text: RESET_REJECTED.into(),
                }
            } else {
                ContentBlock::tool_use(RESET_TOOL, RESET_TOOL, serde_json::json!({}))
            };
            Box::pin(async move {
                Ok(StreamResponse {
                    message: Message {
                        role: Role::Assistant,
                        content: vec![content],
                        ..Default::default()
                    },
                    usage: Default::default(),
                    stop_reason: Some(if finished {
                        StopReason::EndTurn
                    } else {
                        StopReason::ToolUse
                    }),
                })
            })
        }
    }

    /// {spec} without fast mode, which none of these models offer.
    fn sdk_model(spec: &str) -> Model {
        Model {
            fast_support: Some(makima_providers::model::FastSupport::Unsupported),
            ..Model::from_spec(spec).unwrap()
        }
    }

    #[test_case(false, RESTORED_MODEL; "restores_persisted_model_without_explicit_flag")]
    #[test_case(true, STARTUP_MODEL; "explicit_model_overrides_persisted_model")]
    fn sdk_startup_model_precedence(explicit_model: bool, expected: &str) {
        let restored = ResolvedSession {
            id: None,
            root_id: None,
            parent: None,
            journal: None,
            seed: None,
            history: Default::default(),
            model: Some(RESTORED_MODEL.into()),
            meta: SessionMeta::default(),
        };
        let resolved = startup_model(
            &makima_providers::test_util::offline_ids(&["anthropic", "openai"]),
            Model::from_spec(STARTUP_MODEL).unwrap(),
            &restored,
            explicit_model,
            &ModelPolicy::default(),
        );

        assert_eq!(resolved.spec(), expected);
    }

    struct OutcomeBehavior(CommandOutcome);

    struct ReplaceAttachment;
    struct RejectAttachment;

    impl makima_commands::CommandBehavior for RejectAttachment {
        fn execute(
            &self,
            invocation: makima_commands::CommandInvocation,
        ) -> CommandFuture<Result<CommandOutcome, CommandError>> {
            let has_attachment = !invocation.content.attachments.is_empty();
            Box::pin(async move {
                if has_attachment {
                    Err(CommandError::Producer(Arc::from(REJECTED_ATTACHMENT)))
                } else {
                    Ok(CommandOutcome::Completed)
                }
            })
        }
    }

    impl makima_commands::CommandBehavior for ReplaceAttachment {
        fn execute(
            &self,
            invocation: makima_commands::CommandInvocation,
        ) -> CommandFuture<Result<CommandOutcome, CommandError>> {
            Box::pin(async move {
                let Some(attachment) = invocation.content.attachments.first() else {
                    return Err(CommandError::Producer(Arc::from(
                        "missing command attachment",
                    )));
                };
                if attachment.media_type.as_ref() != "image/png"
                    || attachment.data.as_ref() != "AAAA"
                {
                    return Err(CommandError::Producer(Arc::from(
                        "command attachment changed before dispatch",
                    )));
                }
                Ok(CommandOutcome::AgentTurn(makima_commands::AgentTurn {
                    content: CommandContent {
                        text: Arc::from("inspected"),
                        attachments: Arc::from([makima_commands::CommandAttachment {
                            media_type: Arc::from("image/jpeg"),
                            data: Arc::from("BBBB"),
                        }]),
                    },
                    prompt: None,
                }))
            })
        }
    }

    impl makima_commands::CommandBehavior for OutcomeBehavior {
        fn execute(
            &self,
            _invocation: makima_commands::CommandInvocation,
        ) -> CommandFuture<Result<CommandOutcome, CommandError>> {
            let outcome = self.0.clone();
            Box::pin(async move { Ok(outcome) })
        }
    }

    /// Commands over a session whose root never starts, for dispatch that
    /// never reaches the actor.
    fn sdk_commands(registry: CommandRegistry) -> SdkCommands {
        let handle = headless::spawn_interactive_with_binding(
            idle_params(),
            Box::new(|_| {
                Box::pin(async {
                    Err(AgentError::Config {
                        message: UNSTARTED_ROOT.into(),
                    })
                })
            }),
            None,
            None,
        );
        SdkCommands::new(
            registry,
            &[],
            SessionCommandHost::new(
                handle.control_tx.clone(),
                unbound_coordinator(handle.session_id.id()),
                Arc::default(),
            ),
        )
        .unwrap()
    }

    fn idle_params() -> InteractiveParams {
        let cwd = std::env::temp_dir();
        InteractiveParams {
            model: Model::from_spec(STARTUP_MODEL).unwrap(),
            config: AgentConfig::default(),
            permissions_config: PermissionsConfig::default(),
            timeouts: Default::default(),
            prompt_slots: Default::default(),
            excluded_tools: Vec::new(),
            mcp_handle: None,
            initial_wd: cwd.clone(),
            session_id: None,
            root_id: None,
            journal: None,
            initial_history: Vec::new(),
            yolo: false,
            system_prompt_override: None,
            append_system_prompt: None,
            defaults: SessionDefaults::default(),
            model_policy: Arc::default(),
            providers: Arc::new(makima_providers::test_util::offline_ids(&[
                "anthropic",
                "openai",
            ])),
            modes: Arc::new(makima_agent::ModeRegistry::builtin()),
            question_mode: QuestionMode::Headless,
            plugin_rules: Arc::default(),
            project_config: ProjectConfig::for_project(&cwd),
            local_tools: Default::default(),
            state_dir: None,
            answers: true,
        }
    }

    /// Parks every request until the test releases it, then answers.
    struct GatedProvider {
        entered: Sender<()>,
        release: Receiver<()>,
    }

    impl Provider for GatedProvider {
        fn stream_message<'a>(
            &'a self,
            _: &'a Model,
            _: &'a [Message],
            _: &'a str,
            _: &'a Value,
            events: &'a Sender<ProviderEvent>,
            _: RequestOptions,
            _: Option<&'a SessionRef>,
            _invocation: &'a Invocation,
            _delivery: RequestDelivery,
        ) -> BoxFuture<'a, Result<StreamResponse, AgentError>> {
            Box::pin(async move {
                self.entered.send_async(()).await.unwrap();
                self.release.recv_async().await.unwrap();
                let text = SDK_GATED_REPLY.to_owned();
                events
                    .send(ProviderEvent::TextDelta { text: text.clone() })
                    .unwrap();
                Ok(StreamResponse {
                    message: Message {
                        role: Role::Assistant,
                        content: vec![ContentBlock::Text { text }],
                        ..Default::default()
                    },
                    usage: TokenUsage::default(),
                    stop_reason: Some(StopReason::EndTurn),
                })
            })
        }
    }

    struct PendingSdkSession {
        handle: InteractiveHandle,
        coordinator: makima_agent::session_coordinator::SessionCoordinatorHandle,
        shared: Arc<Mutex<Shared>>,
        pump: smol::Task<()>,
        out_rx: Receiver<String>,
        entering: Receiver<()>,
        release: Sender<()>,
    }

    /// An SDK session whose provider parks every request until released,
    /// with one turn admitted and already streaming.
    fn pending_sdk_session() -> PendingSdkSession {
        let (entered, entering) = flume::bounded(1);
        let (release, released) = flume::unbounded();
        let provider: Arc<dyn Provider> = Arc::new(GatedProvider {
            entered,
            release: released,
        });
        let handle = headless::spawn_interactive_with_binding(
            idle_params(),
            Box::new(move |model| Box::pin(async move { Ok(PreparedModel { model, provider }) })),
            None,
            None,
        );
        smol::block_on(handle.initialized_root()).unwrap();
        let coordinator = unbound_coordinator(handle.session_id.id());
        let shared = Arc::new(Mutex::new(Shared::new()));
        let (out_tx, out_rx) = flume::unbounded();
        let pump = EventPump {
            writer: SdkWriter {
                session_id: handle.session_id.clone(),
                out_tx,
            },
            shared: Arc::clone(&shared),
            read: coordinator.read(),
            permissions: Arc::clone(&handle.permissions),
            answer_tx: handle.answer_tx.clone(),
            include_partial_messages: false,
            synth: StreamSynth::new(),
            tool_inputs: HashMap::new(),
            result_text: String::new(),
            cost: None,
            request_counter: 0,
            terminal_run_id: None,
            observer: HeadlessObserver::new(
                makima_lua::EventHandle::disconnected_for_test(),
                handle.session_id.id(),
            ),
        }
        .spawn(handle.event_rx.clone());
        let session = PendingSdkSession {
            handle,
            coordinator,
            shared,
            pump,
            out_rx,
            entering,
            release,
        };
        session.admit();
        session
    }

    impl PendingSdkSession {
        fn admit(&self) {
            let input = AgentInput::new(SDK_ROOT_PROMPT.into(), Vec::new());
            assert_eq!(
                smol::block_on(admit_sdk_turn(&self.shared, &self.handle.input_tx, input)),
                Ok(true)
            );
            self.entering.recv_timeout(SDK_GATE_TIMEOUT).unwrap();
        }

        fn next_result(&self) -> Value {
            loop {
                let line = self.out_rx.recv_timeout(SDK_GATE_TIMEOUT).unwrap();
                let message: Value = serde_json::from_str(&line).unwrap();
                if message["type"] == "result" {
                    return message;
                }
            }
        }

        /// Ends input the way stdin EOF does: the turn in flight must hold
        /// the session open until the provider is released, then report.
        fn end_input(self) -> Vec<Value> {
            let mut finishing = Box::pin(finish_in_flight(
                &self.shared,
                &self.handle.manager,
                async { Ok(()) },
            ));
            assert!(
                smol::block_on(futures_lite::future::poll_once(&mut finishing)).is_none(),
                "input end must wait for the turn in flight"
            );
            self.release.send(()).unwrap();
            smol::block_on(finishing).unwrap();
            let PendingSdkSession {
                handle,
                coordinator,
                shared,
                pump,
                out_rx,
                ..
            } = self;
            let InteractiveHandle { input_tx, task, .. } = handle;
            drop(input_tx);
            smol::block_on(futures_lite::future::or(
                async {
                    task.await;
                    pump.await;
                },
                async {
                    smol::Timer::after(SDK_GATE_TIMEOUT).await;
                    panic!("{SDK_SETTLEMENT_TIMEOUT}");
                },
            ));
            assert!(shared.lock().unwrap().work.is_empty());
            smol::block_on(coordinator.close()).unwrap();
            out_rx
                .try_iter()
                .map(|line| serde_json::from_str::<Value>(&line).unwrap())
                .filter(|message| message["type"] == "result")
                .collect()
        }
    }

    /// `echo prompt | makima` must work: the end of input waits for the turn
    /// in flight, reports it, then lets the session end. A permission ask
    /// left open is denied, since no answer can arrive any more.
    #[test]
    fn input_end_finishes_in_flight_turn_then_exits() {
        const ASK_ID: &str = "open ask";
        let session = pending_sdk_session();
        let (answer_tx, answer_rx) = flume::unbounded();
        session
            .shared
            .lock()
            .unwrap()
            .pending
            .insert(ASK_ID.into(), (ASK_ID.into(), answer_tx));
        let results = session.end_input();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0]["is_error"], false);
        assert_eq!(results[0]["result"], SDK_GATED_REPLY);
        assert_eq!(
            answer_rx.try_recv().unwrap(),
            TaggedAnswer::new(ASK_ID, PermissionAnswer::Deny).encode()
        );
    }

    /// The parent died mid-turn: stdout broke and the pump stopped before the
    /// turn's outcome, yet input end must not wait on work nobody reports,
    /// nor on a dispatch stuck behind a command's handler.
    #[test_case(false ; "with_dispatch_done")]
    #[test_case(true ; "with_dispatch_stuck_on_a_handler")]
    fn input_end_exits_once_the_output_is_gone(stuck: bool) {
        let PendingSdkSession {
            handle,
            coordinator,
            shared,
            pump,
            out_rx,
            release,
            ..
        } = pending_sdk_session();
        drop(out_rx);
        release.send(()).unwrap();
        let InteractiveHandle {
            input_tx,
            task,
            manager,
            ..
        } = handle;
        smol::block_on(futures_lite::future::or(
            async {
                pump.await;
                finish_in_flight(&shared, &manager, async {
                    if stuck {
                        std::future::pending::<()>().await;
                    }
                    Ok(())
                })
                .await
                .unwrap();
                drop(input_tx);
                task.await;
            },
            async {
                smol::Timer::after(SDK_GATE_TIMEOUT).await;
                panic!("{SDK_SETTLEMENT_TIMEOUT}");
            },
        ));
        assert_eq!(
            begin_sdk_operation(&shared, local_work()),
            Err(OUTPUT_CLOSED)
        );
        smol::block_on(coordinator.close()).unwrap();
    }

    /// A plugin's run_command reaches the agent it pinned, at its hop depth,
    /// as in the TUI, rather than the root at depth zero.
    #[test]
    fn command_runner_runs_on_the_pinned_agent_at_its_depth() {
        struct Pinned(Sender<(Option<AgentId>, usize)>);
        impl makima_commands::CommandBehavior for Pinned {
            fn execute(
                &self,
                invocation: makima_commands::CommandInvocation,
            ) -> CommandFuture<Result<CommandOutcome, CommandError>> {
                let pinned = invocation
                    .admission_context::<PinnedCommandTarget>()
                    .map(|pinned| {
                        pinned.validate().unwrap();
                        pinned.target.id()
                    });
                self.0.send((pinned, invocation.depth)).unwrap();
                Box::pin(async { Ok(CommandOutcome::Completed) })
            }
        }
        let session = pending_sdk_session();
        let root = smol::block_on(session.handle.initialized_root()).unwrap();
        let registry = CommandRegistry::new();
        let (seen_tx, seen) = flume::unbounded();
        let mut pinned = registration(PINNED_COMMAND, CommandOutcome::Completed);
        pinned.behavior = Arc::new(Pinned(seen_tx));
        let plugin = registry.create_producer(makima_commands::ProducerPrecedence::Plugin);
        plugin.replace(vec![pinned]).unwrap();
        let commands = sdk_commands(registry.clone());
        let runner = makima_lua::orchestration::command_runner(
            registry,
            commands.target.clone(),
            makima_lua::orchestration::headless_outcomes(&session.handle),
        );
        smol::block_on(runner(
            PINNED_COMMAND.into(),
            PINNED_DEPTH,
            PinnedCommandTarget::pin(TrustedTarget {
                target: root.clone(),
                template: makima_agent::tools::cli_tool_ctx(std::env::temp_dir()),
            })
            .unwrap(),
        ))
        .unwrap();
        assert_eq!(
            seen.try_recv().unwrap(),
            (Some(root.id()), usize::from(PINNED_DEPTH))
        );
        session.end_input();
    }

    #[test_case(true ; "model_turn_joins_the_root_turn")]
    #[test_case(false ; "handler_error_fails_the_call")]
    fn command_runner_runs_the_outcome_or_fails_with_the_handler(turn: bool) {
        let session = pending_sdk_session();
        let root = smol::block_on(session.handle.initialized_root()).unwrap();
        let actor = root.actor().unwrap();
        let running = actor.latest_ticket().unwrap().turn_id();
        let failure = CommandError::Producer(Arc::from(HANDLER_ERR));
        let outcome = if turn {
            CommandOutcome::AgentTurn(makima_commands::AgentTurn {
                content: CommandContent::from(SDK_ROOT_PROMPT),
                prompt: None,
            })
        } else {
            CommandOutcome::Failed(failure.clone())
        };
        let registry = CommandRegistry::new();
        let plugin = registry.create_producer(makima_commands::ProducerPrecedence::Plugin);
        plugin
            .replace(vec![registration(PINNED_COMMAND, outcome)])
            .unwrap();
        let commands = sdk_commands(registry.clone());
        let runner = makima_lua::orchestration::command_runner(
            registry,
            commands.target.clone(),
            makima_lua::orchestration::headless_outcomes(&session.handle),
        );
        let result = smol::block_on(runner(
            PINNED_COMMAND.into(),
            PINNED_DEPTH,
            PinnedCommandTarget::pin(TrustedTarget {
                target: root.clone(),
                template: makima_agent::tools::cli_tool_ctx(std::env::temp_dir()),
            })
            .unwrap(),
        ));
        assert_eq!(actor.latest_ticket().unwrap().turn_id(), running);
        if turn {
            assert_eq!(result, Ok(()));
            session.release.send(()).unwrap();
            session.entering.recv_timeout(SDK_GATE_TIMEOUT).unwrap();
        } else {
            assert_eq!(result, Err(failure.to_string()));
        }
        assert_eq!(session.end_input().len(), 1);
    }

    #[test]
    fn interrupt_cancels_only_the_admitted_turn() {
        let session = pending_sdk_session();
        cancel_sdk_work(&session.shared);
        assert_eq!(session.next_result()["is_error"], true);
        assert!(session.shared.lock().unwrap().work.is_empty());
        session.admit();
        let results = session.end_input();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0]["is_error"], false);
    }

    /// A message sent while a turn runs joins it, as typing does in the TUI,
    /// so the one result covers both messages.
    #[test]
    fn message_sent_mid_turn_joins_the_running_turn() {
        const FOLLOW_UP: &str = "sdk follow-up";
        let session = pending_sdk_session();
        let running = tracked_turns(&session.shared);
        let input = AgentInput::new(FOLLOW_UP.into(), Vec::new());
        assert_eq!(
            smol::block_on(admit_sdk_turn(
                &session.shared,
                &session.handle.input_tx,
                input
            )),
            Ok(true)
        );
        assert_eq!(tracked_turns(&session.shared), running);
        session.release.send(()).unwrap();
        session.entering.recv_timeout(SDK_GATE_TIMEOUT).unwrap();
        let results = session.end_input();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0]["is_error"], false);
    }

    /// A message the actor refuses for its size is reported as an error; the
    /// session goes on, so it is not read as the session having ended.
    #[test]
    fn refused_message_is_an_error_not_the_end() {
        let session = pending_sdk_session();
        let input = AgentInput::new(
            "a".repeat(makima_agent::actor::MAX_PENDING_INSTRUCTION_BYTES + 1),
            Vec::new(),
        );
        assert_eq!(
            smol::block_on(admit_sdk_turn(
                &session.shared,
                &session.handle.input_tx,
                input
            )),
            Err(makima_agent::ActorError::InstructionOverflow.to_string())
        );
        let results = session.end_input();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0]["is_error"], false);
    }

    #[test]
    fn local_operations_run_one_at_a_time() {
        let shared = Mutex::new(Shared::new());
        let first = begin_sdk_operation(&shared, local_work()).unwrap();
        assert_eq!(begin_sdk_operation(&shared, local_work()), Err(WORK_ACTIVE));
        end_sdk_operation(&shared, first);
        assert!(begin_sdk_operation(&shared, local_work()).is_ok());
    }

    /// An operation an interrupt dropped that ends late leaves the work
    /// begun since in place, so the next interrupt still cancels it.
    #[test]
    fn late_operation_end_keeps_later_work() {
        let shared = Mutex::new(Shared::new());
        let interrupted = begin_sdk_operation(&shared, local_work()).unwrap();
        cancel_sdk_work(&shared);
        let (trigger, later) = CancelToken::new();
        let current = begin_sdk_operation(&shared, trigger).unwrap();
        end_sdk_operation(&shared, interrupted);
        assert_eq!(shared.lock().unwrap().work.len(), 1);
        cancel_sdk_work(&shared);
        assert!(later.is_cancelled());
        end_sdk_operation(&shared, current);
        assert!(shared.lock().unwrap().work.is_empty());
    }

    fn local_work() -> CancelTrigger {
        CancelToken::new().0
    }

    fn tracked_turns(shared: &Mutex<Shared>) -> Vec<TurnId> {
        shared
            .lock()
            .unwrap()
            .work
            .iter()
            .filter_map(WorkCancel::turn_id)
            .collect()
    }

    fn test_permissions() -> PermissionManager {
        PermissionManager::new(
            PermissionsConfig::default(),
            PathBuf::from("/project"),
            ProjectConfig::for_project(std::path::Path::new("/project")),
            Arc::default(),
        )
    }

    /// A coordinator on {STARTUP_MODEL} with no actor bound to it.
    fn unbound_coordinator(
        session_id: MakimaId,
    ) -> makima_agent::session_coordinator::SessionCoordinatorHandle {
        makima_agent::session_coordinator::SessionCoordinatorHandle::register(
            makima_agent::session_coordinator::SessionCoordinatorParams {
                session_id,
                catalog: Default::default(),
                definitions: makima_agent::session_coordinator::builtin_option_definitions(
                    STARTUP_MODEL,
                    [Arc::from(STARTUP_MODEL)],
                    false,
                    false,
                    false,
                    makima_agent::ThinkingConfig::Off,
                ),
                persisted_options: Default::default(),
                model_policy: Arc::default(),
                checkpoint: makima_storage::checkpoint::ClaimingWriter::accept_all(),
                mailbox: makima_agent::SessionMailbox::new(session_id),
            },
        )
        .unwrap()
    }

    fn registration(name: &str, outcome: CommandOutcome) -> makima_commands::Registration {
        makima_commands::Registration {
            spec: makima_commands::CommandSpec {
                name: Arc::from(name),
                aliases: Arc::from([]),
                arguments: makima_commands::CommandArguments::Raw { required: false },
                docs: makima_commands::CommandDocs {
                    summary: Arc::from(format!("{name} description")),
                    argument_hint: Some(Arc::from("<arg>")),
                },
                required_capabilities: TargetCapabilities::default(),
            },
            behavior: Arc::new(OutcomeBehavior(outcome)),
            argument_completions: Vec::new(),
        }
    }

    const PINNED_COMMAND: &str = "/pinned";
    const HANDLER_ERR: &str = "the handler gave up";
    const PINNED_DEPTH: u8 = 3;
    const OTHER_CWD: &str = "/elsewhere";
    const THIS_CWD: &str = "/here";
    /// A pid no live process on this machine has.
    const FAKE_PID: u32 = u32::MAX - 1;

    #[test]
    fn sdk_resolve_session_rejects_session_from_other_cwd() {
        use clap::Parser;
        use tempfile::tempdir;

        let tmp = tempdir().unwrap();
        let storage = StateDir::from_path(tmp.path().to_path_buf());
        let mut session = StoredSession::new("test-model", OTHER_CWD);
        session.save(&storage).unwrap();
        let id = session.id.to_string();
        for flags in [vec!["-c".to_string()], vec!["--session-id".to_string()]] {
            let mut args = vec!["makima".to_string()];
            args.extend(flags);
            args.push(id.clone());
            let cli = Cli::parse_from(args.iter().map(String::as_str));
            let err = resolve_session(&cli, THIS_CWD, &storage)
                .map(|_| ())
                .expect_err("a session stored under another cwd must be rejected");
            assert!(err.to_string().contains(OTHER_CWD));
        }
    }

    #[test_case(false, false; "explicit_fresh_destination")]
    #[test_case(false, true; "explicit_existing_destination")]
    #[test_case(true, false; "latest_fresh_destination")]
    #[test_case(true, true; "latest_existing_destination")]
    fn sdk_resolve_session_rejects_conflicting_destination(latest: bool, existing: bool) {
        use clap::Parser;

        let tmp = tempfile::tempdir().unwrap();
        let storage = StateDir::from_path(tmp.path().to_path_buf());
        let mut source = StoredSession::new(STARTUP_MODEL, THIS_CWD);
        source.push_message(Message::user(SDK_ROOT_PROMPT.into()));
        source.save(&storage).unwrap();
        let mut destination = StoredSession::new(STARTUP_MODEL, OTHER_CWD);
        if existing {
            destination.save(&storage).unwrap();
        }
        let source_path = storage
            .path()
            .join(SESSIONS_DIR)
            .join(format!("{}.jsonl", source.id));
        let destination_path = storage
            .path()
            .join(SESSIONS_DIR)
            .join(format!("{}.jsonl", destination.id));
        let source_bytes = std::fs::read(&source_path).unwrap();
        let destination_bytes = std::fs::read(&destination_path).ok();
        let mut args = vec!["makima".to_owned()];
        if latest {
            args.push("-l".to_owned());
        } else {
            args.extend(["-c".to_owned(), source.id.to_string()]);
        }
        args.extend(["--session-id".to_owned(), destination.id.to_string()]);
        let cli = Cli::parse_from(args);
        let error = resolve_session(&cli, THIS_CWD, &storage)
            .map(|_| ())
            .unwrap_err();
        assert_eq!(error.to_string(), SESSION_ID_CONFLICT);
        assert_eq!(std::fs::read(source_path).unwrap(), source_bytes);
        assert_eq!(std::fs::read(destination_path).ok(), destination_bytes);
    }

    #[test_case("-c"; "valued_continue")]
    #[test_case("--session-id"; "pinned_id")]
    #[test_case("-l"; "last")]
    fn sdk_resolve_session_rejects_session_open_elsewhere(flag: &str) {
        use clap::Parser;
        use std::fs;
        use tempfile::tempdir;

        let tmp = tempdir().unwrap();
        let storage = StateDir::from_path(tmp.path().to_path_buf());
        let mut session = StoredSession::new("test-model", THIS_CWD);
        session.save(&storage).unwrap();
        let sessions_dir = storage.ensure_subdir(SESSIONS_DIR).unwrap();

        let mut args = vec!["makima".to_string()];
        args.push(flag.to_string());
        if flag != "-l" {
            args.push(session.id.to_string());
        }
        let cli = Cli::parse_from(args.iter().map(String::as_str));

        let resolved =
            resolve_session(&cli, THIS_CWD, &storage).expect("an unlocked session loads");
        assert_eq!(resolved.id.map(|r| r.id()), Some(session.id));

        fs::write(
            session_lock::lock_path(&sessions_dir, &session.id),
            FAKE_PID.to_string(),
        )
        .unwrap();
        let err = resolve_session(&cli, THIS_CWD, &storage)
            .map(|_| ())
            .expect_err("a session locked by another instance must be rejected");
        assert!(err.to_string().contains(session_lock::OPEN_ELSEWHERE_MSG));
    }

    #[test]
    fn sdk_resume_interrupts_root_and_child_once_without_reissuing_work() {
        use clap::Parser;
        use makima_agent::TurnCancellationReason;
        use makima_agent::manager::AgentResidency;
        use makima_storage::session_state::{StateChange, StoredStateRecord, StoredTurnOrigin};

        smol::block_on(async {
            let tmp = tempfile::tempdir().unwrap();
            let storage = StateDir::from_path(tmp.path().to_path_buf());
            let cwd = tmp.path().to_string_lossy().into_owned();
            let root_id = AgentId::generate();
            let child_id = AgentId::generate();
            let root_turn = TurnId::generate();
            let child_turn = TurnId::generate();
            let mut session = StoredSession::new(STARTUP_MODEL, &cwd);
            session.bind_root_id(root_id).unwrap();
            let mut record = session.agents()[&root_id].clone();
            record.id = child_id;
            record.parent = Some(root_id);
            record.depth = 1;
            record.transcript_key = SDK_CHILD_PROMPT.into();
            record.created_seq = 2;
            record.updated_seq = 2;
            session.push_agent_revision(2, record);
            session.set_subagent_messages(
                SDK_CHILD_PROMPT.into(),
                vec![Message::user(SDK_CHILD_PROMPT.into())],
            );
            for (seq, agent, turn) in [(3, root_id, root_turn), (4, child_id, child_turn)] {
                session.push_state(StoredStateRecord {
                    seq,
                    agent: Some(agent),
                    generation: 0,
                    change: StateChange::Admitted {
                        turn,
                        origin: StoredTurnOrigin::User,
                    },
                });
            }
            session.meta.input_draft = Some(SDK_ROOT_PROMPT.into());
            session.meta.queued_messages.push(SDK_CHILD_PROMPT.into());
            session.save(&storage).unwrap();
            let id = session.id;
            let mut first_state = None;
            for _ in 0..2 {
                let cli = Cli::parse_from(["makima", "-c", &id.to_string()]);
                let mut restored = resolve_session(&cli, &cwd, &storage).unwrap();
                let (checkpoint, lease, recovered) = recover_sdk_session(
                    &mut restored,
                    &storage,
                    &SessionRef::from(id),
                    root_id,
                    STARTUP_MODEL,
                    &cwd,
                    &ModeRegistry::builtin(),
                )
                .await
                .unwrap();
                let state = recovered.session().state().to_vec();
                if let Some(first) = &first_state {
                    assert_eq!(first, &state);
                } else {
                    first_state = Some(state);
                }
                assert!(recovered.session().meta.queued_messages.is_empty());
                assert_eq!(
                    recovered.session().meta.input_draft.as_deref(),
                    Some(format!("{SDK_ROOT_PROMPT}\n\n{SDK_CHILD_PROMPT}").as_str())
                );
                let (entered, requests) = flume::unbounded();
                let (_release, release) = flume::unbounded();
                let provider: Arc<dyn Provider> = Arc::new(GatedProvider { entered, release });
                let mut params = idle_params();
                params.session_id = Some(SessionRef::from(id));
                params.root_id = Some(root_id);
                params.initial_wd = tmp.path().to_path_buf();
                params.journal = Some(recovered.journal());
                let (binding, target) = makima_lua::orchestration::root_binding();
                let handle = headless::spawn_interactive_with_binding(
                    params,
                    Box::new(move |model| {
                        Box::pin(async move {
                            Ok(makima_agent::actor::PreparedModel { model, provider })
                        })
                    }),
                    Some(recovered.session().history_snapshot()),
                    Some(binding),
                );
                let root = handle.initialized_root().await.unwrap();
                assert_eq!(target.recv_async().await.unwrap().target.id(), root_id);
                let child = handle.manager.lookup(child_id).unwrap();
                assert!(!child.is_live());
                assert_eq!(child.snapshot().unwrap().residency, AgentResidency::Dormant);
                for (agent, turn) in [(&root, root_turn), (&child, child_turn)] {
                    let result = agent.turn_ticket(turn).unwrap().wait_result().await;
                    assert!(matches!(
                        result.outcome,
                        TurnOutcome::Cancelled {
                            reason: TurnCancellationReason::Interrupted,
                            ..
                        }
                    ));
                }
                let snapshot = handle.manager.journal().snapshot();
                assert_eq!(
                    snapshot
                        .state
                        .iter()
                        .filter(|record| matches!(&record.change, StateChange::Turn { .. }))
                        .count(),
                    2
                );
                assert!(
                    snapshot
                        .state
                        .iter()
                        .filter_map(|record| match &record.change {
                            StateChange::Turn { turn, .. } => Some(*turn),
                            _ => None,
                        })
                        .all(|turn| turn == root_turn || turn == child_turn)
                );
                assert_eq!(root.actor().unwrap().snapshot().status, ActorStatus::Idle);
                let actor = root.actor().unwrap().snapshot();
                assert_eq!(actor.queued, 0);
                assert!(actor.active_turn.is_none());
                assert!(requests.is_empty());
                assert!(!handle.event_rx.try_iter().any(|event| matches!(
                    event.event,
                    AgentEvent::ToolStart(_) | AgentEvent::QueueItemConsumed { .. }
                )));
                handle.manager.shutdown(SDK_GATE_TIMEOUT).await;
                handle.task.cancel().await;
                drop(checkpoint);
                drop(lease);
            }
        });
    }

    #[test_case(false, false, false; "v3_resume")]
    #[test_case(true, false, false; "v3_fork")]
    #[test_case(false, true, false; "v2_resume")]
    #[test_case(true, true, false; "v2_fork")]
    #[test_case(false, false, true; "v3_latest_resume")]
    #[test_case(true, false, true; "v3_latest_fork")]
    fn sdk_restoration_preserves_compacted_history(fork: bool, legacy: bool, latest: bool) {
        use clap::Parser;
        use makima_storage::conversation::ConversationLink;
        let tmp = tempfile::tempdir().unwrap();
        let storage = StateDir::from_path(tmp.path().to_path_buf());
        const STORED_CONVERSATION: &str = "stored-conversation";
        const STORED_SENT: usize = 2;
        let mut session = StoredSession::new(STARTUP_MODEL, THIS_CWD);
        session.adopt_history(&makima_storage::sessions::HistorySnapshot::new(vec![
            Message::user("first".into()),
            Message::user("second".into()),
            Message::user("third".into()),
        ]));
        session.bind_root_id(AgentId::generate()).unwrap();
        session.set_subagents(vec![makima_storage::sessions::StoredSubagent {
            tool_use_id: SDK_NESTED_TOOL.into(),
            agent_id: None,
            ambiguous: false,
            name: SDK_NESTED_TOOL.into(),
            model: None,
            thinking: None,
            fast: false,
        }]);
        session.meta.compacted = true;
        session.meta.conversation = Some(ConversationState {
            sent: Some(STORED_SENT),
            ..ConversationState::new(STORED_CONVERSATION.into())
        });
        session.save(&storage).unwrap();
        let source_path = storage
            .path()
            .join(SESSIONS_DIR)
            .join(format!("{}.jsonl", session.id));
        if legacy {
            let records = std::fs::read_to_string(&source_path).unwrap();
            let mut records: Vec<Value> = records
                .lines()
                .map(|line| serde_json::from_str(line).unwrap())
                .collect();
            records.retain(|record| !matches!(record["t"].as_str(), Some("agent" | "state")));
            records[0]["v"] = Value::from(2);
            std::fs::write(
                &source_path,
                records
                    .iter()
                    .map(|record| format!("{record}\n"))
                    .collect::<String>(),
            )
            .unwrap();
        }
        let source_bytes = std::fs::read(&source_path).unwrap();
        let mut args = if latest {
            vec!["makima".to_owned(), "-l".to_owned()]
        } else {
            vec!["makima".to_owned(), "-c".to_owned(), session.id.to_string()]
        };
        if fork {
            args.push("--fork-session".to_owned());
        }
        let cli = Cli::parse_from(args);
        let restored = resolve_session(&cli, THIS_CWD, &storage).unwrap();
        assert!(restored.history.compacted);
        let history = makima_agent::agent::History::restored_snapshot(restored.history.clone());
        assert_eq!(
            history.conversation_id().as_ref() == STORED_CONVERSATION,
            !fork
        );
        let link = fork.then(|| ConversationLink {
            id: STORED_CONVERSATION.into(),
            relation: ConversationRelation::Branches,
            shared: STORED_SENT,
        });
        assert_eq!(history.conversation().previous, link);
        let expected_root = restored.root_id;
        if !legacy && !fork {
            assert_eq!(expected_root, session.root_agent_id());
        }
        assert_eq!(
            restored.parent.as_ref().map(|parent| parent.session),
            fork.then_some(session.id)
        );
        assert_eq!(
            restored.parent.as_ref().map(|parent| parent.message_count),
            fork.then_some(session.messages().len())
        );
        assert_eq!(
            serde_json::to_value(restored.history.messages.as_ref()).unwrap(),
            serde_json::to_value(session.history_snapshot().messages.as_ref()).unwrap()
        );
        smol::block_on(async {
            let (entered, entering) = flume::unbounded();
            let (_release, released) = flume::unbounded();
            let provider: Arc<dyn Provider> = Arc::new(GatedProvider {
                entered,
                release: released,
            });
            let mut params = idle_params();
            params.initial_wd = PathBuf::from(THIS_CWD);
            params.session_id = restored.id;
            params.root_id = restored.root_id;
            params.journal = restored.journal;
            let handle = headless::spawn_interactive_with_binding(
                params,
                Box::new(move |model| {
                    Box::pin(async move { Ok(PreparedModel { model, provider }) })
                }),
                Some(restored.history.clone()),
                None,
            );
            let root = handle.initialized_root().await.unwrap();
            if fork {
                assert_ne!(root.id(), session.root_agent_id().unwrap());
                assert_ne!(handle.session_id.id(), session.id);
            } else {
                assert_eq!(Some(root.id()), expected_root);
            }
            let mut seed = restored.seed.unwrap_or_else(|| {
                let mut seed = StoredSession::new(STARTUP_MODEL, THIS_CWD);
                seed.id = handle.session_id.id();
                seed
            });
            handle
                .manager
                .journal()
                .snapshot()
                .apply_to(&mut seed)
                .unwrap();
            let writer = Arc::new(
                SessionLogCheckpoint::open_with_session(storage.clone(), seed, None).unwrap(),
            );
            writer
                .bind_root_id(root.id(), restored.parent.clone())
                .await
                .unwrap();
            writer.bind_journal(handle.manager.journal()).await.unwrap();
            let coordinator = SessionCoordinatorHandle::register(
                makima_agent::session_coordinator::SessionCoordinatorParams {
                    session_id: handle.session_id.id(),
                    catalog: Default::default(),
                    definitions: makima_agent::session_coordinator::builtin_option_definitions(
                        STARTUP_MODEL,
                        [Arc::from(STARTUP_MODEL)],
                        false,
                        false,
                        false,
                        makima_agent::ThinkingConfig::Off,
                    ),
                    persisted_options: Default::default(),
                    model_policy: Arc::default(),
                    checkpoint: writer.clone(),
                    mailbox: handle.mailbox.clone(),
                },
            )
            .unwrap();
            coordinator
                .bind_permissions(Arc::clone(&handle.permissions))
                .unwrap();
            let source = root
                .actor()
                .unwrap()
                .prepare_idle()
                .unwrap()
                .source_snapshot()
                .unwrap();
            assert_eq!(source.history.epoch == restored.history.epoch, !fork);
            assert_eq!(source.history.compacted, restored.history.compacted);
            assert_eq!(source.history.conversation, restored.history.conversation);
            assert_eq!(source.history.messages, restored.history.messages);
            headless::checkpoint_startup_config(&root, &coordinator)
                .await
                .unwrap();
            let saved = StoredSession::load(handle.session_id.id(), &storage).unwrap();
            assert_eq!(saved.messages(), source.history.messages.as_ref());
            assert_eq!(
                saved.history_snapshot().conversation,
                source.history.conversation
            );
            assert_eq!(saved.root_agent_id(), Some(root.id()));
            let resumed_cli = Cli::parse_from(["makima", "-c", &saved.id.to_string()]);
            let resumed = resolve_session(&resumed_cli, THIS_CWD, &storage).unwrap();
            assert_eq!(resumed.root_id, Some(root.id()));
            assert_eq!(saved.parent(), restored.parent.as_ref());
            assert!(saved.history_snapshot().compacted);
            assert_eq!(saved.messages().len(), session.messages().len());
            if legacy && !fork {
                let child_ids: Vec<_> = handle
                    .manager
                    .journal()
                    .snapshot()
                    .agent_revisions
                    .iter()
                    .filter_map(|(_, record)| record.parent.is_some().then_some(record.id))
                    .collect();
                assert!(!child_ids.is_empty());
                assert!(child_ids.iter().all(|id| saved.agents().contains_key(id)));
            }
            if fork {
                assert_eq!(std::fs::read(source_path).unwrap(), source_bytes);
            }
            assert!(entering.is_empty());
            handle.task.cancel().await;
            coordinator.close().await.unwrap();
        });
    }

    fn claude_to_makima_tool_name(name: &str) -> &str {
        TOOL_NAME_MAP
            .iter()
            .find(|(_, c)| *c == name)
            .map(|(m, _)| *m)
            .unwrap_or(name)
    }

    #[test]
    fn sdk_commands_projection_includes_shared_producers_and_session_builtins() {
        let registry = CommandRegistry::new();
        let plugin = registry.create_producer(makima_commands::ProducerPrecedence::Plugin);
        plugin
            .replace(vec![registration("/lua", CommandOutcome::Completed)])
            .unwrap();
        let mcp = registry.create_producer(makima_commands::ProducerPrecedence::Mcp);
        let mut prompt = registration("/mcp:prompt", CommandOutcome::Completed);
        prompt.spec.required_capabilities =
            TargetCapabilities::from_capability(TargetCapability::AgentTurns);
        mcp.replace(vec![prompt]).unwrap();
        let commands = sdk_commands(registry);

        assert_eq!(
            commands.slash_commands(),
            [
                "lua",
                "mcp:prompt",
                "compact",
                "new",
                "clear",
                "model",
                "cd",
                "btw",
                "yolo",
                "fast",
                "workflow",
                "reload-context"
            ]
        );
        assert!(matches!(
            smol::block_on(commands.dispatch_input("/mcp:prompt", &[])),
            InputDispatch::Dispatched(CommandOutcome::Completed)
        ));
        let InputDispatch::Dispatched(CommandOutcome::IsolatedTurn(turn)) =
            smol::block_on(commands.dispatch_input("/btw explain this", &[]))
        else {
            panic!("quick question did not return an isolated turn");
        };
        assert_eq!(turn.content.text.as_ref(), "explain this");
        assert!(matches!(
            smol::block_on(commands.dispatch_input("/btw", &[])),
            InputDispatch::Dispatched(CommandOutcome::Failed(CommandError::TypedArguments {
                command,
                error: makima_commands::ArgumentParseError::MissingRaw,
            })) if command.as_ref() == "/btw"
        ));
        let projection = commands.projection();
        assert_eq!(projection[0]["description"], "/lua description");
        assert_eq!(projection[0]["argumentHint"], "<arg>");
    }

    struct RecordingSdkProvider {
        requests: Sender<(String, RequestOptions)>,
        grandchild_entered: Sender<()>,
        grandchild_release: Receiver<()>,
    }

    impl Provider for RecordingSdkProvider {
        fn stream_message<'a>(
            &'a self,
            model: &'a Model,
            messages: &'a [Message],
            _: &'a str,
            _: &'a Value,
            events: &'a Sender<ProviderEvent>,
            options: RequestOptions,
            _: Option<&'a SessionRef>,
            _invocation: &'a Invocation,
            _delivery: RequestDelivery,
        ) -> BoxFuture<'a, Result<StreamResponse, AgentError>> {
            Box::pin(async move {
                self.requests.send((model.spec(), options)).unwrap();
                let prompt = messages
                    .iter()
                    .flat_map(|message| &message.content)
                    .find_map(|block| match block {
                        ContentBlock::Text { text }
                            if [SDK_ROOT_PROMPT, SDK_CHILD_PROMPT, SDK_GRANDCHILD_PROMPT]
                                .contains(&text.as_str()) =>
                        {
                            Some(text.as_str())
                        }
                        _ => None,
                    })
                    .unwrap();
                let joined = messages
                    .iter()
                    .flat_map(|message| &message.content)
                    .find_map(|block| match block {
                        ContentBlock::ToolResult {
                            content, is_error, ..
                        } => {
                            assert!(!is_error, "SDK nested tool failed: {content}");
                            Some(content.as_str())
                        }
                        _ => None,
                    });
                let content = if prompt == SDK_GRANDCHILD_PROMPT {
                    self.grandchild_entered.send_async(()).await.unwrap();
                    self.grandchild_release.recv_async().await.unwrap();
                    ContentBlock::Text {
                        text: SDK_GRANDCHILD_RESPONSE.into(),
                    }
                } else if let Some(joined) = joined {
                    let expected = if prompt == SDK_CHILD_PROMPT {
                        SDK_GRANDCHILD_RESPONSE
                    } else {
                        SDK_CHILD_RESPONSE
                    };
                    assert!(joined.contains(expected), "missing joined result: {joined}");
                    ContentBlock::Text {
                        text: if prompt == SDK_CHILD_PROMPT {
                            SDK_CHILD_RESPONSE
                        } else {
                            SDK_ROOT_RESPONSE
                        }
                        .into(),
                    }
                } else {
                    ContentBlock::ToolUse {
                        id: SDK_NESTED_TOOL.into(),
                        name: SDK_NESTED_TOOL.into(),
                        input: serde_json::json!({ "child": prompt == SDK_CHILD_PROMPT }),
                        thought_signature: None,
                    }
                };
                let stop_reason = if let ContentBlock::Text { text } = &content {
                    events
                        .send(ProviderEvent::TextDelta { text: text.clone() })
                        .unwrap();
                    StopReason::EndTurn
                } else {
                    StopReason::ToolUse
                };
                Ok(StreamResponse {
                    message: Message {
                        role: Role::Assistant,
                        content: vec![content],
                        ..Default::default()
                    },
                    usage: TokenUsage::default(),
                    stop_reason: Some(stop_reason),
                })
            })
        }
    }

    fn commit_patch(
        services: &AgentOrchestration,
        target: &AgentRef,
        patch: ConfigPatch,
    ) -> makima_agent::actor::ConfigCommit {
        let change = async |_, _| {
            Ok((
                makima_agent::session_coordinator::publication_claim(),
                ConfigChange::Patch(patch),
            ))
        };
        smol::block_on(makima_lua::orchestration::commit_config_change(
            services, target, change,
        ))
        .unwrap()
    }

    #[test]
    fn sdk_model_route_and_nested_task_use_managed_root() {
        use std::sync::Mutex as StdMutex;

        let state_dir = tempfile::tempdir().unwrap();
        let cwd = state_dir.path().to_path_buf();
        let (requests, received) = flume::unbounded();
        let (grandchild_entered, grandchild_gate) = flume::bounded(1);
        let (grandchild_release, grandchild_resume) = flume::bounded(1);
        let provider: Arc<dyn Provider> = Arc::new(RecordingSdkProvider {
            requests,
            grandchild_entered,
            grandchild_release: grandchild_resume,
        });
        let child_provider = Arc::clone(&provider);
        let (ui, notices) = makima_lua::test_support::RecordingUi::new();
        let host = makima_lua::PluginHost::with_session_provider_preparer(
            Arc::clone(ToolRegistry::global_arc()),
            Default::default(),
            true,
            Arc::new(makima_lua::test_support::InMemoryFs::new()),
            Some(cwd.clone()),
            Some(Arc::new(move |model| {
                Ok((model, Arc::clone(&child_provider)))
            })),
            makima_lua::ui_sink::HostUi::Sink(Box::new(ui)),
        )
        .unwrap();
        host.load_source("sdk_bundled_task", include_str!("../plugins/task/init.lua"))
            .unwrap();
        host.load_source(
            "sdk_command_spawn",
            &format!(
                r#"
            makima.api.register_command({{
                name = "/sdk_spawn", tui_only = false,
                handler = function(_, ctx)
                    local root = assert(ctx:agent())
                    local child = assert(makima.agent.spawn(ctx, {{ name = "SDK command child" }}))
                    assert(child:close(ctx))
                    local view, view_err = makima.fn.winsaveview()
                    assert(view == nil and view_err == "{no_ui}", view_err)
                    makima.ui.flash("{SDK_COMMAND_DONE}")
                end,
            }})
        "#,
                no_ui = makima_lua::ui_sink::NO_UI_ERR
            ),
        )
        .unwrap();
        host.load_source(SDK_NESTED_TOOL, &format!(r#"
            makima.api.register_tool({{
                name = "{SDK_NESTED_TOOL}", description = "Exercise nested SDK tasks",
                schema = {{ type = "object", properties = {{ child = {{ type = "boolean" }} }} }},
                audiences = {{ "main", "general_sub" }},
                permission = "run",
                permission_scopes = function(input)
                    return {{ scopes = input.child and {{ "SDK child permission" }} or {{}}, force_prompt = true }}
                end,
                handler = function(input, ctx)
                    if input.child then
                        local inherited = assert(ctx:model())
                        local child = assert(ctx:agent())
                        local selected = assert(child:set_model(ctx, {{ spec = "{STARTUP_MODEL}", thinking = "off", fast = false }}))
                        assert(selected.spec == "{STARTUP_MODEL}")
                        assert(assert(child:config(ctx)).model.spec == "{STARTUP_MODEL}")
                        local captured = assert(ctx:model())
                        assert(captured.spec == inherited.spec)
                        assert(makima.json.encode(captured.thinking) == makima.json.encode(inherited.thinking))
                        assert(captured.fast == inherited.fast)
                    end
                    local prompt = input.child and "{SDK_GRANDCHILD_PROMPT}" or "{SDK_CHILD_PROMPT}"
                    local spawned, err = makima.agent.call_tool(ctx, "task_spawn", {{
                        description = prompt, prompt = prompt, subagent_type = "general",
                    }})
                    assert(not err, err)
                    local task = makima.json.decode(spawned)
                    local joined, wait_err = makima.agent.call_tool(ctx, "task_wait", {{ task_id = task.task_id, turn_id = task.turn_id }})
                    assert(not wait_err, wait_err)
                    local result = makima.json.decode(joined)
                    local expected = input.child and "{SDK_GRANDCHILD_RESPONSE}" or "{SDK_CHILD_RESPONSE}"
                    assert(result.result.text == expected, joined)
                    return result.result.text
                end,
            }})
        "#)).unwrap();
        let lua_handle = host.event_handle();
        let prepared_provider = Arc::clone(&provider);
        let thinking = makima_providers::ThinkingConfig::Off;
        let model = Model::from_spec(STARTUP_MODEL).unwrap();
        let defaults = session_defaults::resolve(
            SessionDefaults {
                fast: true,
                thinking: Some(
                    makima_providers::ThinkingConfig::Effort(makima_providers::Effort::High).into(),
                ),
                ..Default::default()
            },
            &Prefs::default(),
            &SessionMeta {
                fast: true,
                thinking: Some(thinking.into()),
                ..Default::default()
            },
            false,
            &model,
        );
        let (binding, root_target) = makima_lua::orchestration::root_binding();
        let handle = headless::spawn_interactive_with_binding(
            InteractiveParams {
                model,
                config: AgentConfig {
                    max_concurrent_agent_turns: 1,
                    ..Default::default()
                },
                permissions_config: PermissionsConfig::default(),
                timeouts: Default::default(),
                prompt_slots: lua_handle.prompt_slot_source(),
                excluded_tools: vec![QUESTION_TOOL_NAME],
                mcp_handle: None,
                initial_wd: cwd.clone(),
                session_id: None,
                root_id: None,
                journal: None,
                initial_history: Vec::new(),
                yolo: false,
                system_prompt_override: None,
                append_system_prompt: None,
                defaults,
                model_policy: Arc::default(),
                providers: Arc::new(makima_providers::test_util::offline_ids(&[
                    "anthropic",
                    "openai",
                ])),
                modes: lua_handle.mode_registry(),
                question_mode: QuestionMode::Headless,
                plugin_rules: Arc::default(),
                project_config: ProjectConfig::for_project(&cwd),
                local_tools: Default::default(),
                state_dir: Some(cwd.clone()),
                answers: true,
            },
            Box::new(move |model| {
                Box::pin(async move {
                    Ok(PreparedModel {
                        model,
                        provider: prepared_provider,
                    })
                })
            }),
            None,
            Some(binding),
        );
        let agent = smol::block_on(handle.initialized_root()).unwrap();
        assert_eq!(
            agent.effective_config().unwrap().unwrap().settings.thinking,
            thinking
        );
        host.wait_for_worker_barrier_for_test();
        let checkpointed = Arc::new(StdMutex::new(Vec::new()));
        let checkpointed_clone = Arc::clone(&checkpointed);
        let id = handle.session_id.id();
        let (terminal_started, terminal_gate) = flume::bounded(1);
        let (terminal_release, terminal_resume) = flume::bounded(1);
        let checkpoint: Arc<dyn CheckpointWriter<SessionCheckpoint>> = Arc::new(
            move |request: makima_storage::checkpoint::CheckpointRequest<
                makima_agent::session_coordinator::SessionCheckpoint,
            >| {
                let checkpointed = Arc::clone(&checkpointed_clone);
                let terminal_started = terminal_started.clone();
                let terminal_resume = terminal_resume.clone();
                Box::pin(async move {
                    if request.snapshot.kind == SessionCheckpointKind::History {
                        terminal_started.send_async(()).await.unwrap();
                        terminal_resume.recv_async().await.unwrap();
                    }
                    let ack = makima_storage::checkpoint::CheckpointAck::for_request(&request);
                    checkpointed.lock().unwrap().push(request.snapshot);
                    Ok(ack)
                }) as makima_storage::checkpoint::CheckpointFuture
            },
        );
        let coordinator = makima_agent::session_coordinator::SessionCoordinatorHandle::register(
            makima_agent::session_coordinator::SessionCoordinatorParams {
                session_id: id,
                catalog: Default::default(),
                definitions: makima_agent::session_coordinator::builtin_option_definitions(
                    STARTUP_MODEL,
                    [Arc::from(STARTUP_MODEL), Arc::from(TARGET_MODEL)],
                    false,
                    true,
                    false,
                    thinking,
                ),
                persisted_options: Default::default(),
                model_policy: Arc::new(ModelPolicy::default()),
                checkpoint: Arc::new(ClaimingWriter(checkpoint)),
                mailbox: handle.mailbox.clone(),
            },
        )
        .unwrap();
        let shared = Arc::new(Mutex::new(Shared::new()));
        let trusted_root = smol::block_on(makima_lua::orchestration::bind_frontend_root(
            &handle,
            &coordinator,
            &root_target,
            Arc::new({
                let provider = Arc::clone(&provider);
                move |spec: Arc<str>| {
                    let provider = Arc::clone(&provider);
                    let model = sdk_model(&spec);
                    Box::pin(async move { Ok(PreparedModel { model, provider }) })
                }
            }),
        ))
        .unwrap();
        assert_eq!(
            agent.effective_config().unwrap().unwrap().settings.thinking,
            thinking
        );
        assert_eq!(
            coordinator
                .read()
                .options()
                .options
                .iter()
                .find(|option| {
                    option.definition.id.as_ref()
                        == makima_agent::session_options::THINKING_OPTION_ID
                })
                .unwrap()
                .current_value
                .as_ref(),
            thinking.to_string()
        );
        let (child_completing, child_completion_gate) = flume::bounded(1);
        let (child_completion_release, child_completion_resume) = flume::bounded(1);
        let root_id = agent.id();
        let child_storage = StateDir::from_path(state_dir.path().to_path_buf());
        let child_checkpoint = Arc::new(
            makima_agent::session_checkpoint::SessionLogCheckpoint::open(
                child_storage.clone(),
                id,
                STARTUP_MODEL,
                &cwd.to_string_lossy(),
                None,
            )
            .unwrap(),
        );
        let catalog = ModelCatalog::default();
        smol::block_on(catalog.apply(vec![STARTUP_MODEL.into(), TARGET_MODEL.into()], None));
        let services = Arc::new_cyclic(|weak| {
            let persistence = makima_lua::orchestration::child_persistence(
                "SDK",
                weak.clone(),
                Arc::clone(&child_checkpoint),
            );
            let persist = persistence.1;
            makima_lua::orchestration::frontend_root(
                &catalog,
                trusted_root.clone(),
                persistence.0,
                coordinator.clone(),
            ).with_completion(Arc::new(move |target, history| {
            let save = persist(target, history.clone());
            let is_child = target.manager().node(target.id()).unwrap().parent_id == Some(root_id);
            let entered = child_completing.clone();
            let release = child_completion_resume.clone();
            Box::pin(async move {
                if is_child {
                    assert!(history.messages.iter().flat_map(|message| &message.content).any(|block| matches!(block, ContentBlock::Text { text } if text == SDK_CHILD_RESPONSE)));
                    entered.send_async(()).await.map_err(|error| error.to_string())?;
                    release.recv_async().await.map_err(|error| error.to_string())?;
                }
                save.await
            })
        }))
        });
        lua_handle.install_orchestration_services(services.clone());
        let commands = SdkCommands::new(
            host.command_registry(),
            &[],
            SessionCommandHost::new(
                handle.control_tx.clone(),
                coordinator.clone(),
                Arc::default(),
            )
            .with_admission_context(trusted_root),
        )
        .unwrap();
        host.wait_for_worker_barrier_for_test();
        assert!(matches!(
            smol::block_on(commands.dispatch_input("/sdk_spawn", &[])),
            InputDispatch::Dispatched(CommandOutcome::Completed)
        ));
        assert_eq!(
            notices.recv_timeout(SDK_GATE_TIMEOUT).unwrap(),
            SDK_COMMAND_DONE
        );
        assert_eq!(
            handle
                .manager
                .snapshot()
                .iter()
                .filter(|node| node.graph_lifecycle == makima_agent::GraphLifecycle::Live)
                .count(),
            1
        );
        let closed_child = handle
            .manager
            .snapshot()
            .into_iter()
            .find(|node| node.parent_id == Some(agent.id()))
            .unwrap()
            .agent_id;
        let initial_checkpoint_count = {
            let saved = checkpointed.lock().unwrap();
            assert_eq!(saved.len(), 1);
            let initialized = &saved[0];
            assert_eq!(initialized.kind, SessionCheckpointKind::Config);
            let config = &initialized.config.as_ref().unwrap().config;
            assert_eq!(config.model.spec(), STARTUP_MODEL);
            assert_eq!(config.thinking, thinking);
            let journal = initialized.journal.as_ref().unwrap();
            assert!(journal.state.iter().any(|record| {
                record.agent == Some(agent.id())
                    && matches!(&record.change,
                        StateChange::Context { source, .. }
                        if source == SDK_CONTEXT_MANIFEST)
            }));
            assert!(
                journal
                    .histories
                    .get(&agent.id())
                    .is_none_or(|history| history.messages.is_empty())
            );
            saved.len()
        };
        assert!(received.is_empty());
        let root_actor = agent.actor().unwrap();
        assert!(root_actor.latest_ticket().is_none());
        assert_eq!(root_actor.committed_message_count(), 0);
        assert!(matches!(
            smol::block_on(commands.dispatch_input("/model not-a-model", &[])),
            InputDispatch::Dispatched(CommandOutcome::Failed(_))
        ));
        assert_eq!(coordinator.read().model().unwrap().as_ref(), STARTUP_MODEL);
        assert_eq!(checkpointed.lock().unwrap().len(), initial_checkpoint_count);

        let result =
            smol::block_on(commands.dispatch_input(&format!("/model {TARGET_MODEL}"), &[]));
        assert!(
            matches!(result, InputDispatch::Dispatched(CommandOutcome::Completed)),
            "{result:?}"
        );
        assert_eq!(coordinator.read().model().unwrap().as_ref(), TARGET_MODEL);
        assert_eq!(
            coordinator.read().options().options[0]
                .current_value
                .as_ref(),
            TARGET_MODEL
        );
        assert_eq!(
            checkpointed.lock().unwrap().len(),
            initial_checkpoint_count + 1
        );
        assert_eq!(
            checkpointed.lock().unwrap()[initial_checkpoint_count]
                .config
                .as_ref()
                .unwrap()
                .config
                .model
                .spec(),
            TARGET_MODEL
        );
        assert_eq!(
            agent
                .effective_config()
                .unwrap()
                .unwrap()
                .settings
                .model
                .spec(),
            TARGET_MODEL
        );
        assert_eq!(handle.model.current().unwrap().1.spec(), TARGET_MODEL);

        smol::block_on(catalog.apply(vec![SDK_DISCOVERED_MODEL.into()], None));
        assert!(
            OrchestrationServices::available_models(&*services, &agent)
                .unwrap()
                .iter()
                .any(|spec| spec == SDK_DISCOVERED_MODEL)
        );
        assert_eq!(
            checkpointed.lock().unwrap().len(),
            initial_checkpoint_count + 1
        );
        host.wait_for_worker_barrier_for_test();
        let (out_tx, out_rx) = flume::unbounded();
        let pump = EventPump {
            writer: SdkWriter {
                session_id: handle.session_id.clone(),
                out_tx,
            },
            shared: Arc::clone(&shared),
            read: coordinator.read(),
            permissions: Arc::clone(&handle.permissions),
            answer_tx: handle.answer_tx.clone(),
            include_partial_messages: false,
            synth: StreamSynth::new(),
            tool_inputs: HashMap::new(),
            result_text: String::new(),
            cost: None,
            request_counter: 0,
            terminal_run_id: None,
            observer: HeadlessObserver::new(lua_handle, handle.session_id.id()),
        }
        .spawn(handle.event_rx.clone());
        let input = AgentInput::new(SDK_ROOT_PROMPT.into(), Vec::new());
        assert_eq!(
            smol::block_on(admit_sdk_turn(&shared, &handle.input_tx, input)),
            Ok(true)
        );
        let ticket = match &shared.lock().unwrap().work[..] {
            [WorkCancel::Turn { ticket, .. }] => ticket.clone(),
            _ => panic!("one SDK turn in flight"),
        };
        let permission = loop {
            let line = out_rx.recv_timeout(SDK_GATE_TIMEOUT).unwrap();
            let message: Value = serde_json::from_str(&line).unwrap();
            if message["type"] == "control_request" {
                break message;
            }
        };
        assert_eq!(permission["request"]["tool_name"], SDK_NESTED_TOOL);
        let request_id = permission["request_id"].as_str().unwrap();
        let (ask_id, answer_tx) = shared.lock().unwrap().pending.remove(request_id).unwrap();
        answer_tx
            .send(
                TaggedAnswer::new(
                    ask_id,
                    decode_permission_response(&serde_json::json!({ "behavior": "allow" })),
                )
                .encode(),
            )
            .unwrap();
        grandchild_gate.recv_timeout(SDK_GATE_TIMEOUT).unwrap();
        let graph = handle.manager.snapshot();
        assert_eq!(handle.manager.limits().max_concurrent_agent_turns, 1);
        assert_eq!(graph.len(), 4);
        let retained_child = graph
            .iter()
            .find(|node| node.agent_id == closed_child)
            .unwrap();
        assert_eq!(retained_child.graph_lifecycle, GraphLifecycle::Closed);
        assert!(retained_child.actor.as_ref().is_none_or(|actor| {
            actor.lifecycle == ActorLifecycle::Closed
                && actor.status == ActorStatus::Idle
                && actor.queued == 0
        }));
        let root = graph
            .iter()
            .find(|node| node.agent_id == agent.id())
            .unwrap();
        assert_eq!(root.parent_id, None);
        let grandchild = graph.iter().find(|node| node.depth == 2).unwrap();
        let child = graph
            .iter()
            .find(|node| Some(node.agent_id) == grandchild.parent_id)
            .unwrap();
        assert_eq!(child.parent_id, Some(agent.id()));
        assert_ne!(child.agent_id, closed_child);
        assert_eq!((child.depth, grandchild.depth), (1, 2));
        assert_eq!(grandchild.root_id, agent.id());
        let child_ref = handle.manager.lookup(child.agent_id).unwrap();
        let child_key = services.history_key(&child_ref).unwrap();
        let child_config = child_ref.effective_config().unwrap().unwrap();
        assert_eq!(child_config.model.spec(), STARTUP_MODEL);
        assert_eq!(child_config.thinking, makima_providers::ThinkingConfig::Off);
        assert!(!child_config.fast);
        let persisted = StoredSession::load(id, &child_storage).unwrap();
        let stored = persisted
            .subagents()
            .iter()
            .find(|stored| stored.tool_use_id == child_key)
            .unwrap();
        let mut expected = makima_lua::orchestration::child_metadata(
            child_key.clone(),
            child.metadata.label.clone().unwrap_or_default(),
        );
        expected.agent_id = Some(child.agent_id.to_string());
        assert_eq!(stored, &expected);
        let child_history = &persisted.subagent_messages()[&child_key];
        let current = child_ref.manager().journal().snapshot();
        assert!(
            current.histories[&child_ref.id()]
                .messages
                .starts_with(child_history)
        );
        assert!(child_history.iter().any(|message| {
            message.content.iter().any(
                |block| matches!(block, ContentBlock::Text { text } if text == SDK_CHILD_PROMPT),
            )
        }));
        assert!(!child_history.iter().any(|message| {
            message.content.iter().any(
                |block| matches!(block, ContentBlock::Text { text } if text == SDK_CHILD_RESPONSE),
            )
        }));
        assert_eq!(coordinator.read().model().unwrap().as_ref(), TARGET_MODEL);
        assert_eq!(
            checkpointed.lock().unwrap().len(),
            initial_checkpoint_count + 1
        );
        let child_ticket = child_ref.actor().unwrap().latest_ticket().unwrap();
        assert!(child_ticket.peek_result().is_none());
        grandchild_release.send(()).unwrap();
        child_completion_gate
            .recv_timeout(SDK_GATE_TIMEOUT)
            .unwrap();
        assert!(child_ticket.peek_result().is_none());
        assert!(ticket.peek_result().is_none());
        let mut requests = received.try_iter().collect::<Vec<_>>();
        assert_eq!(requests.len(), 4);
        assert!(terminal_gate.try_recv().is_err());
        let committed = commit_patch(
            services.as_ref(),
            &child_ref,
            ConfigPatch {
                model: Some(PreparedModel {
                    model: sdk_model(TARGET_MODEL),
                    provider: Arc::clone(&provider),
                }),
                ..Default::default()
            },
        );
        assert_eq!(committed.config.model.spec(), TARGET_MODEL);
        let saved = StoredSession::load(id, &child_storage).unwrap();
        assert_eq!(
            saved.latest_state().unwrap().scopes[&Some(child.agent_id)]
                .config
                .as_ref()
                .unwrap()
                .model,
            TARGET_MODEL
        );
        child_completion_release.send(()).unwrap();
        terminal_gate.recv_timeout(SDK_GATE_TIMEOUT).unwrap();
        let persisted = StoredSession::load(id, &child_storage).unwrap();
        let state = persisted.latest_state().unwrap();
        let saved = state.scopes[&Some(child.agent_id)].config.as_ref().unwrap();
        assert_eq!(saved.model, TARGET_MODEL);
        assert_eq!(
            saved.thinking,
            Some(makima_providers::ThinkingConfig::Off.into())
        );
        assert!(!saved.fast);
        let child_history = persisted.subagent_messages().get(&child_key).unwrap();
        assert!(child_history.iter().flat_map(|message| &message.content)
            .any(|block| matches!(block, ContentBlock::Text { text } if text == SDK_CHILD_RESPONSE)));
        let sealed = ticket.peek_result().unwrap();
        assert!(matches!(sealed.outcome, TurnOutcome::Completed { .. }));
        assert_eq!(sealed.text, SDK_ROOT_RESPONSE);
        assert!(ticket.peek().is_none());
        let mut output = Vec::new();
        loop {
            let line = out_rx.recv_timeout(SDK_GATE_TIMEOUT).unwrap();
            let message: Value = serde_json::from_str(&line).unwrap();
            let root_assistant = message["type"] == "assistant"
                && message["parent_tool_use_id"].is_null()
                && content_text(&message["message"]["content"]).as_deref()
                    == Some(SDK_ROOT_RESPONSE);
            output.push(message);
            if root_assistant {
                break;
            }
        }
        output.extend(
            out_rx
                .try_iter()
                .map(|line| serde_json::from_str::<Value>(&line).unwrap()),
        );
        assert!(output.iter().all(|message| message["type"] != "result"));
        assert!(!shared.lock().unwrap().work.is_empty());
        assert_eq!(
            checkpointed.lock().unwrap().len(),
            initial_checkpoint_count + 1
        );
        terminal_release.send(()).unwrap();
        loop {
            let line = out_rx.recv_timeout(SDK_GATE_TIMEOUT).unwrap();
            let message: Value = serde_json::from_str(&line).unwrap();
            let terminal = message["type"] == "result";
            output.push(message);
            if terminal {
                break;
            }
        }
        assert!(matches!(
            smol::block_on(ticket.wait()),
            TurnOutcome::Completed { .. }
        ));
        assert!(shared.lock().unwrap().work.is_empty());
        let before_version = coordinator.read().options().version;
        let selected_thinking =
            makima_providers::ThinkingConfig::Effort(makima_providers::Effort::High);
        let committed = commit_patch(
            services.as_ref(),
            &agent,
            ConfigPatch {
                thinking: Some(selected_thinking),
                ..Default::default()
            },
        );
        assert_eq!(committed.config.model.spec(), TARGET_MODEL);
        assert!(!committed.config.fast);
        assert_eq!(committed.config.thinking, selected_thinking);
        assert_eq!(
            agent.effective_config().unwrap().unwrap().settings.thinking,
            selected_thinking
        );
        assert!(coordinator.read().options().version > before_version);
        assert_eq!(
            checkpointed.lock().unwrap().len(),
            initial_checkpoint_count + 3
        );
        catalog.attach(&services);
        smol::block_on(catalog.apply(vec![SDK_LATE_MODEL.into()], None));
        let options = coordinator.read().options();
        let values = &options.options[0].definition.values;
        for spec in [SDK_DISCOVERED_MODEL, SDK_LATE_MODEL] {
            assert!(values.iter().any(|value| &*value.value == spec));
        }
        {
            let saved = checkpointed.lock().unwrap();
            assert_eq!(saved.len(), initial_checkpoint_count + 4);
            let published_options = &saved[initial_checkpoint_count + 3];
            assert_eq!(published_options.kind, SessionCheckpointKind::Options);
            assert!(published_options.history.is_none());
            assert!(published_options.config.is_none());
            assert_eq!(published_options.options, options);
        }
        smol::block_on(catalog.apply(vec![SDK_LATE_MODEL.into()], None));
        assert_eq!(
            checkpointed.lock().unwrap().len(),
            initial_checkpoint_count + 4
        );
        let manager = handle.manager.clone();
        let InteractiveHandle { input_tx, task, .. } = handle;
        drop(input_tx);
        smol::block_on(futures_lite::future::or(
            async {
                task.await;
                pump.await;
            },
            async {
                smol::Timer::after(SDK_GATE_TIMEOUT).await;
                panic!("{SDK_SETTLEMENT_TIMEOUT}");
            },
        ));
        output.extend(
            out_rx
                .try_iter()
                .map(|line| serde_json::from_str::<Value>(&line).unwrap()),
        );
        let results = output
            .iter()
            .filter(|message| message["type"] == "result")
            .collect::<Vec<_>>();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0]["result"], SDK_ROOT_RESPONSE);
        assert_eq!(results[0]["is_error"], false);
        let saved = checkpointed.lock().unwrap();
        assert_eq!(saved.len(), initial_checkpoint_count + 4);
        let terminal_history = &saved[initial_checkpoint_count + 1];
        assert_eq!(terminal_history.kind, SessionCheckpointKind::History);
        assert!(terminal_history.history.as_ref().unwrap().messages.iter().flat_map(|message| &message.content).any(|block| matches!(block, ContentBlock::Text { text } if text == SDK_ROOT_RESPONSE)));
        drop(saved);
        let graph = manager.snapshot();
        assert!(!graph.is_empty());
        for node in graph {
            assert!(matches!(
                node.graph_lifecycle,
                GraphLifecycle::Closed | GraphLifecycle::Dormant
            ));
            assert_eq!(node.residency, AgentResidency::Dormant);
            assert!(manager.runner_finished(node.agent_id).unwrap());
            let retained = manager.lookup(node.agent_id).unwrap();
            assert!(!retained.is_live());
            assert!(retained.validate_live().is_err());
            assert!(retained.actor().is_err());
            assert!(retained.effective_config().is_err());
            if let Some(actor) = node.actor {
                assert!(matches!(
                    actor.lifecycle,
                    ActorLifecycle::Closed | ActorLifecycle::Shutdown
                ));
                assert_eq!(actor.status, ActorStatus::Idle);
                assert_eq!(actor.active_turn, None);
                assert_eq!(actor.queued, 0);
            }
        }
        requests.extend(received.try_iter());
        assert_eq!(requests.len(), 5);
        assert!(requests.iter().all(|(model, options)| model == TARGET_MODEL
            && options.thinking == thinking
            && !options.fast));
        smol::block_on(coordinator.close()).unwrap();
    }

    #[test_case(PermissionMode::Default ; "default")]
    #[test_case(PermissionMode::AcceptEdits ; "accept_edits")]
    #[test_case(PermissionMode::Plan ; "plan")]
    fn sdk_permission_mode_disables_persisted_yolo(mode: PermissionMode) {
        use std::sync::Mutex as StdMutex;

        let checkpointed = Arc::new(StdMutex::new(Vec::new()));
        let checkpointed_clone = Arc::clone(&checkpointed);
        let id = MakimaId::generate();
        let coordinator = makima_agent::session_coordinator::SessionCoordinatorHandle::register(
            makima_agent::session_coordinator::SessionCoordinatorParams {
                session_id: id,
                catalog: Default::default(),
                definitions: makima_agent::session_coordinator::builtin_option_definitions(
                    STARTUP_MODEL,
                    [Arc::from(STARTUP_MODEL)],
                    true,
                    false,
                    false,
                    makima_agent::ThinkingConfig::Off,
                ),
                persisted_options: Default::default(),
                model_policy: Arc::new(ModelPolicy::default()),
                checkpoint: Arc::new(ClaimingWriter(Arc::new(
                    move |request: makima_storage::checkpoint::CheckpointRequest<
                        makima_agent::session_coordinator::SessionCheckpoint,
                    >| {
                        let checkpointed = Arc::clone(&checkpointed_clone);
                        Box::pin(async move {
                            let ack =
                                makima_storage::checkpoint::CheckpointAck::for_request(&request);
                            checkpointed.lock().unwrap().push(request.snapshot);
                            Ok(ack)
                        }) as makima_storage::checkpoint::CheckpointFuture
                    },
                ))),
                mailbox: makima_agent::SessionMailbox::new(id),
            },
        )
        .unwrap();
        let mut params = idle_params();
        params.session_id = Some(SessionRef::from(id));
        params.yolo = true;
        let (entered, _) = flume::unbounded();
        let (_release, release) = flume::unbounded();
        let provider: Arc<dyn Provider> = Arc::new(GatedProvider { entered, release });
        let prepared_provider = Arc::clone(&provider);
        let handle = headless::spawn_interactive_with_binding(
            params,
            Box::new(move |model| {
                Box::pin(async move {
                    Ok(PreparedModel {
                        model,
                        provider: prepared_provider,
                    })
                })
            }),
            None,
            None,
        );
        let root = smol::block_on(handle.initialized_root()).unwrap();
        let permissions = Arc::clone(&handle.permissions);
        coordinator
            .bind_permissions(Arc::clone(&permissions))
            .unwrap();
        coordinator
            .bind_actor(
                root,
                Arc::new(move |spec| {
                    let provider = Arc::clone(&provider);
                    Box::pin(async move {
                        Ok(PreparedModel {
                            model: Model::from_spec(&spec).unwrap(),
                            provider,
                        })
                    })
                }),
                Arc::new(|_| {}),
            )
            .unwrap();
        assert!(permissions.is_yolo());

        let committed = StdMutex::new(Vec::new());
        let set_mode = |key: &str| {
            committed.lock().unwrap().push(key.to_owned());
            Ok(())
        };
        set_permission_mode(mode, &coordinator, &set_mode).unwrap();

        assert!(!permissions.is_yolo());
        let expected = if mode == PermissionMode::Plan {
            ModeId::Plan.key()
        } else {
            ModeId::Build.key()
        };
        assert_eq!(*committed.lock().unwrap(), [expected]);
        let snapshot = coordinator.read().options();
        let yolo = snapshot
            .options
            .iter()
            .find(|option| {
                option.definition.id.as_ref() == makima_agent::session_options::YOLO_OPTION_ID
            })
            .unwrap();
        assert_eq!(
            yolo.current_value.as_ref(),
            makima_agent::session_options::DISABLED_VALUE
        );
        let checkpointed = checkpointed.lock().unwrap();
        assert_eq!(checkpointed.len(), 1);
        let checkpointed_yolo = checkpointed[0]
            .options
            .options
            .iter()
            .find(|option| {
                option.definition.id.as_ref() == makima_agent::session_options::YOLO_OPTION_ID
            })
            .unwrap();
        assert_eq!(
            checkpointed_yolo.current_value.as_ref(),
            makima_agent::session_options::DISABLED_VALUE
        );
        drop(checkpointed);
        smol::block_on(handle.task.cancel());
        smol::block_on(coordinator.close()).unwrap();
    }

    #[test_case(true, Some(true), PermissionMode::Default, false, PermissionMode::BypassPermissions ; "bare_resume_restores_enabled")]
    #[test_case(true, Some(false), PermissionMode::BypassPermissions, false, PermissionMode::Default ; "bare_resume_restores_disabled")]
    #[test_case(false, None, PermissionMode::BypassPermissions, false, PermissionMode::BypassPermissions ; "new_session_uses_startup")]
    #[test_case(true, Some(false), PermissionMode::Plan, false, PermissionMode::Plan ; "restored_disabled_preserves_plan")]
    #[test_case(true, Some(true), PermissionMode::Plan, true, PermissionMode::Plan ; "explicit_plan_overrides_resumed_yolo")]
    #[test_case(true, Some(true), PermissionMode::Default, true, PermissionMode::Default ; "explicit_default_overrides_resumed_yolo")]
    #[test_case(true, Some(true), PermissionMode::Plan, true, PermissionMode::Plan ; "explicit_plan_overrides_forked_yolo")]
    #[test_case(true, Some(true), PermissionMode::Default, true, PermissionMode::Default ; "explicit_default_overrides_forked_yolo")]
    fn sdk_yolo_restore_precedence(
        restored: bool,
        persisted: Option<bool>,
        startup: PermissionMode,
        explicit: bool,
        expected: PermissionMode,
    ) {
        assert_eq!(
            restored_permission_mode(restored, persisted, startup, explicit),
            expected
        );
    }

    #[test]
    fn sdk_bare_model_returns_shared_usage_error() {
        let commands = sdk_commands(CommandRegistry::new());
        assert!(matches!(
            smol::block_on(commands.dispatch_input("/model", &[])),
            InputDispatch::Dispatched(CommandOutcome::Failed(CommandError::Producer(message)))
                if message.as_ref() == makima_agent::command::NONINTERACTIVE_MODEL_USAGE
        ));
    }

    #[test]
    fn sdk_command_behavior_owns_attachment_policy() {
        let registry = CommandRegistry::new();
        let plugin = registry.create_producer(makima_commands::ProducerPrecedence::Plugin);
        let mut inspect = registration("/inspect", CommandOutcome::Completed);
        inspect.behavior = Arc::new(ReplaceAttachment);
        let mut reject = registration("/reject", CommandOutcome::Completed);
        reject.behavior = Arc::new(RejectAttachment);
        plugin.replace(vec![inspect, reject]).unwrap();
        let commands = sdk_commands(registry);
        let images = vec![ImageSource::new(
            makima_providers::ImageMediaType::Png,
            Arc::from("AAAA"),
        )];

        let InputDispatch::Dispatched(CommandOutcome::AgentTurn(turn)) =
            smol::block_on(commands.dispatch_input("/inspect now", &images))
        else {
            panic!("attachment-aware command did not return an agent turn");
        };
        let input = command_attachments::agent_input(turn).unwrap();
        assert_eq!(input.message, "inspected");
        assert_eq!(input.images.len(), 1);
        assert_eq!(
            input.images[0].media_type,
            makima_providers::ImageMediaType::Jpeg
        );
        assert_eq!(input.images[0].data.as_ref(), "BBBB");

        assert!(matches!(
            smol::block_on(commands.dispatch_input("/reject", &images)),
            InputDispatch::Dispatched(CommandOutcome::Failed(CommandError::Producer(message)))
                if message.as_ref() == REJECTED_ATTACHMENT
        ));
    }

    #[test]
    fn sdk_dispatch_returns_outcomes_and_preserves_literal_input() {
        let registry = CommandRegistry::new();
        let plugin = registry.create_producer(makima_commands::ProducerPrecedence::Plugin);
        plugin
            .replace(vec![registration("/done", CommandOutcome::Completed)])
            .unwrap();
        let commands = sdk_commands(registry.clone());

        assert!(matches!(
            smol::block_on(registry.dispatch_input(&commands.target, "/done now".into())),
            InputDispatch::Dispatched(CommandOutcome::Completed)
        ));
        let images = vec![ImageSource::new(
            makima_providers::ImageMediaType::Gif,
            Arc::from("AAAA"),
        )];
        assert!(matches!(
            smol::block_on(commands.dispatch_input("/unknown literal", &images)),
            InputDispatch::Dispatched(CommandOutcome::Failed(CommandError::UnknownCommand(
                name
            ))) if name.as_ref() == "/unknown"
        ));
        let InputDispatch::LiteralInput(content) =
            smol::block_on(commands.dispatch_input("//unknown literal", &images))
        else {
            panic!("escaped unknown command did not remain literal input");
        };
        assert_eq!(content.text.as_ref(), "/unknown literal");
        assert_eq!(content.attachments.len(), 1);
        assert_eq!(content.attachments[0].media_type.as_ref(), "image/gif");
        assert_eq!(content.attachments[0].data.as_ref(), "AAAA");
        let InputDispatch::LiteralInput(content) =
            smol::block_on(commands.dispatch_input("///unknown literal", &[]))
        else {
            panic!("triple-slash input did not remain literal");
        };
        assert_eq!(content.text.as_ref(), "//unknown literal");
        assert!(matches!(
            smol::block_on(commands.dispatch_input("ordinary literal", &[])),
            InputDispatch::LiteralInput(_)
        ));
    }

    #[test]
    fn sdk_unsupported_builtin_is_rejected() {
        let commands = sdk_commands(CommandRegistry::new());
        assert!(matches!(
            smol::block_on(
                commands
                    .registry
                    .dispatch_input(&commands.target, "/help".into())
            ),
            InputDispatch::Dispatched(CommandOutcome::Failed(CommandError::UnknownCommand(
                name
            ))) if name.as_ref() == "/help"
        ));
    }

    #[test_case("bash", "Bash")]
    #[test_case("read", "Read")]
    #[test_case("edit", "Edit")]
    #[test_case("write", "Write")]
    #[test_case("grep", "Grep")]
    #[test_case("glob", "Glob")]
    #[test_case("todo_write", "TodoWrite")]
    #[test_case("webfetch", "WebFetch")]
    #[test_case("websearch", "WebSearch")]
    #[test_case("task_spawn", "Task")]
    #[test_case("multiedit", "MultiEdit")]
    #[test_case("code_execution", "CodeExecution")]
    #[test_case("index", "Index")]
    #[test_case("memory", "Memory")]
    #[test_case("question", "Question")]
    fn makima_to_claude_roundtrip(makima: &str, claude: &str) {
        assert_eq!(makima_to_claude_tool_name(makima), claude);
        assert_eq!(claude_to_makima_tool_name(claude), makima);
    }

    #[test]
    fn unknown_tool_name_passthrough() {
        assert_eq!(makima_to_claude_tool_name("unknown_tool"), "unknown_tool");
        assert_eq!(claude_to_makima_tool_name("UnknownTool"), "UnknownTool");
    }

    const MODEL: &str = "test-model";

    fn types(events: &[Value]) -> Vec<&str> {
        events.iter().map(|e| e["type"].as_str().unwrap()).collect()
    }

    #[test]
    fn text_delta_starts_message_and_subsequent_is_delta_only() {
        let mut synth = StreamSynth::new();
        let events = synth.text_delta(MODEL, "hi");
        assert_eq!(
            types(&events),
            [
                "message_start",
                "content_block_start",
                "content_block_delta"
            ]
        );
        assert_eq!(events[0]["message"]["model"], MODEL);
        assert_eq!(events[1]["index"], 0);
        assert_eq!(events[1]["content_block"]["type"], "text");
        assert_eq!(events[2]["delta"]["text"], "hi");

        let more = synth.text_delta(MODEL, "again");
        assert_eq!(types(&more), ["content_block_delta"]);
    }

    #[test]
    fn block_transition_closes_previous_and_increments_index() {
        let mut synth = StreamSynth::new();
        synth.text_delta(MODEL, "a");
        let events = synth.thinking_delta(MODEL, "b");
        assert_eq!(
            types(&events),
            [
                "content_block_stop",
                "content_block_start",
                "content_block_delta"
            ]
        );
        assert_eq!(events[0]["index"], 0);
        assert_eq!(events[1]["index"], 1);
        assert_eq!(events[1]["content_block"]["type"], "thinking");
    }

    #[test]
    fn thinking_block_end_closes_thinking_block() {
        let mut synth = StreamSynth::new();
        synth.thinking_delta(MODEL, "a");

        let events = synth.thinking_block_end();
        assert_eq!(types(&events), ["content_block_stop"]);
        assert_eq!(events[0]["index"], 0);

        let events = synth.thinking_delta(MODEL, "b");
        assert_eq!(
            types(&events),
            ["content_block_start", "content_block_delta"]
        );
        assert_eq!(events[0]["index"], 1);
        assert_eq!(events[0]["content_block"]["type"], "thinking");
    }

    #[test]
    fn tool_use_emits_complete_block() {
        let mut synth = StreamSynth::new();
        synth.text_delta(MODEL, "a");
        let events = synth.tool_use(MODEL, "tool_1", "Read", r#"{"path":"t"}"#);
        assert_eq!(
            types(&events),
            [
                "content_block_stop",
                "content_block_start",
                "content_block_delta",
                "content_block_stop"
            ]
        );
        assert_eq!(events[1]["content_block"]["type"], "tool_use");
        assert_eq!(events[1]["content_block"]["name"], "Read");
        assert_eq!(events[2]["delta"]["type"], "input_json_delta");
    }

    #[test]
    fn multiple_tool_uses_increment_block_index() {
        let mut synth = StreamSynth::new();
        synth.text_delta(MODEL, "x");
        let t1 = synth.tool_use(MODEL, "t1", "Read", "{}");
        let t2 = synth.tool_use(MODEL, "t2", "Write", "{}");
        let idx = |events: &[Value]| {
            events
                .iter()
                .find(|e| e["type"] == "content_block_start")
                .unwrap()["index"]
                .as_i64()
        };
        assert_eq!(idx(&t1), Some(1));
        assert_eq!(idx(&t2), Some(2));
    }

    #[test]
    fn finish_message_closes_block_and_resets() {
        let mut synth = StreamSynth::new();
        synth.text_delta(MODEL, "a");
        let usage = TokenUsage {
            output: 5,
            ..Default::default()
        };
        let events = synth.finish_message(&usage);
        assert_eq!(
            types(&events),
            ["content_block_stop", "message_delta", "message_stop"]
        );
        assert_eq!(events[1]["usage"]["output_tokens"], 5);

        assert!(synth.finish_message(&usage).is_empty());

        let next = synth.text_delta(MODEL, "new");
        assert_eq!(next[1]["index"], 0);
    }

    #[test]
    fn finish_message_before_start_is_empty() {
        let mut synth = StreamSynth::new();
        assert!(synth.finish_message(&TokenUsage::default()).is_empty());
    }

    #[test]
    fn tool_use_on_fresh_synth_has_no_spurious_stop() {
        let mut synth = StreamSynth::new();
        let events = synth.tool_use(MODEL, "t1", "Read", r#"{"path":"x"}"#);
        assert_eq!(events[0]["type"], "message_start");
        let start_pos = events
            .iter()
            .position(|e| e["type"] == "content_block_start")
            .unwrap();
        let stop_pos = events
            .iter()
            .position(|e| e["type"] == "content_block_stop")
            .unwrap();
        assert!(stop_pos > start_pos);
    }

    #[test_case("user", InboundMessageType::User)]
    #[test_case("control_request", InboundMessageType::ControlRequest)]
    #[test_case("control_response", InboundMessageType::ControlResponse)]
    #[test_case("control_cancel_request", InboundMessageType::ControlCancelRequest)]
    fn inbound_message_type_deserializes(value: &str, expected: InboundMessageType) {
        let message: InboundMessage =
            serde_json::from_value(serde_json::json!({"type": value})).unwrap();
        assert_eq!(message.msg_type, expected);
    }

    #[test]
    fn unknown_inbound_message_type_is_preserved() {
        let message: InboundMessage =
            serde_json::from_value(serde_json::json!({"type": "future"})).unwrap();
        assert!(matches!(
            message.msg_type,
            InboundMessageType::Unknown(value) if value == "future"
        ));
    }

    #[test_case("initialize", InboundControlRequestType::Initialize)]
    #[test_case("interrupt", InboundControlRequestType::Interrupt)]
    #[test_case("set_permission_mode", InboundControlRequestType::SetPermissionMode)]
    #[test_case("set_model", InboundControlRequestType::SetModel)]
    fn inbound_control_request_type_deserializes(value: &str, expected: InboundControlRequestType) {
        let request: InboundControlRequest = serde_json::from_value(serde_json::json!({
            "request_id": "request",
            "request": {"subtype": value}
        }))
        .unwrap();
        assert_eq!(request.request.subtype, expected);
    }

    #[test]
    fn unknown_inbound_control_request_type_is_preserved() {
        let request: InboundControlRequest = serde_json::from_value(serde_json::json!({
            "request_id": "request",
            "request": {"subtype": "future"}
        }))
        .unwrap();
        assert!(matches!(
            request.request.subtype,
            InboundControlRequestType::Unknown(value) if value == "future"
        ));
    }

    #[test_case("default", PermissionMode::Default)]
    #[test_case("acceptEdits", PermissionMode::AcceptEdits)]
    #[test_case("plan", PermissionMode::Plan)]
    #[test_case("bypassPermissions", PermissionMode::BypassPermissions)]
    fn permission_mode_roundtrip(s: &str, mode: PermissionMode) {
        assert_eq!(PermissionMode::parse(s), Some(mode));
        assert_eq!(mode.as_str(), s);
    }

    #[test]
    fn permission_mode_resolve() {
        assert_eq!(
            PermissionMode::resolve(None, false),
            PermissionMode::Default
        );
        assert_eq!(
            PermissionMode::resolve(None, true),
            PermissionMode::BypassPermissions
        );
        assert_eq!(
            PermissionMode::resolve(Some("plan"), true),
            PermissionMode::Plan
        );
        assert_eq!(
            PermissionMode::resolve(Some("bogus"), false),
            PermissionMode::Default
        );
    }

    #[test]
    fn content_text_extracts_from_all_shapes() {
        assert_eq!(content_text(&serde_json::json!("hi")), Some("hi".into()));
        let blocks = serde_json::json!([
            {"type": "text", "text": "a"},
            {"type": "image", "source": {}},
            {"type": "text", "text": "b"},
        ]);
        assert_eq!(content_text(&blocks), Some("a\nb".into()));
        assert_eq!(content_text(&serde_json::json!(42)), None);
    }

    #[test]
    fn content_images_extracts_base64_blocks() {
        let blocks = serde_json::json!([
            {"type": "text", "text": "look at this"},
            {"type": "image", "source": {"type": "base64", "media_type": "image/png", "data": "AAAA"}},
            {"type": "image", "source": {"type": "base64", "media_type": "image/jpeg", "data": "BBBB"}},
        ]);
        let images = content_images(&blocks);
        assert_eq!(images.len(), 2);
        assert_eq!(&*images[0].data, "AAAA");
        assert_eq!(&*images[1].data, "BBBB");

        // Non-array content and malformed image blocks yield no images.
        assert!(content_images(&serde_json::json!("hi")).is_empty());
        let bad = serde_json::json!([{"type": "image", "source": {"data": "x"}}]);
        assert!(content_images(&bad).is_empty());
    }

    #[test]
    fn subagent_outcome_does_not_retire_sdk_turn() {
        let (out_tx, out_rx) = flume::unbounded();
        let shared = Arc::new(Mutex::new(Shared::new()));
        let (trigger, cancel) = CancelToken::new();
        begin_sdk_operation(&shared, trigger).unwrap();
        let mut pump = EventPump {
            writer: SdkWriter {
                session_id: SessionRef::generate(),
                out_tx,
            },
            shared: Arc::clone(&shared),
            read: unbound_coordinator(MakimaId::generate()).read(),
            permissions: Arc::new(test_permissions()),
            answer_tx: flume::unbounded().0,
            include_partial_messages: false,
            synth: StreamSynth::new(),
            tool_inputs: HashMap::new(),
            result_text: String::new(),
            cost: None,
            request_counter: 0,
            terminal_run_id: None,
            observer: HeadlessObserver::new(
                makima_lua::EventHandle::disconnected_for_test(),
                MakimaId::generate(),
            ),
        };
        let outcome = || {
            TurnOutcome::completed(
                makima_agent::AgentId::generate(),
                makima_agent::TurnId::generate(),
                TokenUsage::default(),
                1,
                makima_agent::DoneReason::EndTurn,
            )
        };
        pump.handle(Envelope {
            event: AgentEvent::TurnOutcome(outcome()),
            subagent: Some(makima_agent::SubagentInfo {
                agent_id: makima_agent::AgentId::generate(),
                parent_agent_id: None,
                parent_tool_use_id: "task".into(),
                name: "research".into(),
                prompt: None,
                model: None,
                opts: None,
                answer_tx: None,
                cancel: None,
            }),
            identity: None,
        })
        .unwrap();

        assert!(!cancel.is_cancelled());
        assert_eq!(shared.lock().unwrap().work.len(), 1);
        assert!(out_rx.is_empty());
        assert_eq!(pump.terminal_run_id, None);

        pump.handle(Envelope {
            event: AgentEvent::TurnOutcome(outcome()),
            subagent: None,
            identity: None,
        })
        .unwrap();
        assert_eq!(
            out_rx
                .try_iter()
                .filter(|line| serde_json::from_str::<Value>(line).unwrap()["type"] == "result")
                .count(),
            1
        );
    }

    #[test_case(true ; "persistence_error_after_outcome_emits_one_result_and_warning")]
    #[test_case(false ; "persistence_error_instead_of_outcome_emits_one_error_result")]
    fn persistence_error_emits_exactly_one_result(outcome_first: bool) {
        let (out_tx, out_rx) = flume::unbounded();
        let mut pump = EventPump {
            writer: SdkWriter {
                session_id: SessionRef::generate(),
                out_tx,
            },
            shared: Arc::new(Mutex::new(Shared::new())),
            read: unbound_coordinator(MakimaId::generate()).read(),
            permissions: Arc::new(test_permissions()),
            answer_tx: flume::unbounded().0,
            include_partial_messages: false,
            synth: StreamSynth::new(),
            tool_inputs: HashMap::new(),
            result_text: String::new(),
            cost: None,
            request_counter: 0,
            terminal_run_id: None,
            observer: HeadlessObserver::new(
                makima_lua::EventHandle::disconnected_for_test(),
                MakimaId::generate(),
            ),
        };
        let agent_id = makima_agent::AgentId::generate();
        let turn_id = makima_agent::TurnId::generate();
        let identity = Some(makima_agent::ActorWorkIdentity::new(
            agent_id,
            makima_agent::ActorWorkKind::Turn {
                operation_id: 0,
                turn_id,
            },
            1,
        ));
        if outcome_first {
            pump.handle(Envelope {
                event: AgentEvent::TurnOutcome(TurnOutcome::completed(
                    agent_id,
                    turn_id,
                    TokenUsage::default(),
                    1,
                    makima_agent::DoneReason::EndTurn,
                )),
                subagent: None,
                identity,
            })
            .unwrap();
        }
        pump.handle(Envelope {
            event: AgentEvent::ControlError {
                message: CHECKPOINT_FAILED.into(),
            },
            subagent: None,
            identity,
        })
        .unwrap();

        let records: Vec<Value> = out_rx
            .try_iter()
            .map(|line| serde_json::from_str(&line).unwrap())
            .collect();
        assert_eq!(
            records
                .iter()
                .filter(|record| record["type"] == "result")
                .count(),
            1
        );
        let warned = records.iter().any(|record| {
            record["type"] == "system"
                && record["subtype"] == "warning"
                && record["message"] == CHECKPOINT_FAILED
        });
        let failed = records.iter().any(|record| {
            record["type"] == "result"
                && record["is_error"] == true
                && record["result"] == CHECKPOINT_FAILED
        });
        assert_eq!((warned, failed), (outcome_first, !outcome_first));
    }

    #[test]
    fn wire_result_serializes_correctly() {
        let msg = WireMessage {
            inner: WireInner::Result(ResultPayload {
                subtype: "success",
                is_error: false,
                duration_ms: 1000,
                duration_api_ms: 1000,
                num_turns: 1,
                result: "done".into(),
                total_cost_usd: 0.01,
                usage: TokenUsage::default(),
                permission_denials: Vec::new(),
            }),
            session_id: SessionRef::generate(),
            uuid: "u".into(),
        };
        let json: Value = serde_json::to_value(&msg).unwrap();
        assert_eq!(json["type"], "result");
        assert_eq!(json["subtype"], "success");
        assert_eq!(json["num_turns"], 1);
        assert!(json.get("session_id").is_some());
    }

    #[test]
    fn wire_init_serializes_correctly() {
        let msg = WireMessage {
            inner: WireInner::System(SystemPayload {
                subtype: "init",
                extra: serde_json::json!({
                    "cwd": "/tmp",
                    "tools": ["Read"],
                    "model": "test",
                    "permissionMode": "default",
                }),
            }),
            session_id: SessionRef::generate(),
            uuid: "u".into(),
        };
        let json: Value = serde_json::to_value(&msg).unwrap();
        assert_eq!(json["type"], "system");
        assert_eq!(json["subtype"], "init");
        assert_eq!(json["cwd"], "/tmp");
    }

    #[test]
    fn wire_control_response_serializes() {
        let msg = WireMessage {
            inner: WireInner::ControlResponse(ControlResponsePayload {
                response: ControlResponseInner {
                    subtype: "success",
                    request_id: "req_1".into(),
                    response: Some(serde_json::json!({"commands": []})),
                    error: None,
                },
            }),
            session_id: SessionRef::generate(),
            uuid: "u".into(),
        };
        let json: Value = serde_json::to_value(&msg).unwrap();
        assert_eq!(json["type"], "control_response");
        assert_eq!(json["response"]["request_id"], "req_1");
    }

    #[test]
    fn wire_control_request_serializes() {
        let msg = WireMessage {
            inner: WireInner::ControlRequest(ControlRequestPayload {
                request_id: "req_5".into(),
                request: ControlRequestInner {
                    subtype: "can_use_tool",
                    tool_name: Some("Read".into()),
                    input: Some(serde_json::json!({"path": "/tmp"})),
                    tool_use_id: Some("tool_123".into()),
                },
            }),
            session_id: SessionRef::generate(),
            uuid: "u".into(),
        };
        let json: Value = serde_json::to_value(&msg).unwrap();
        assert_eq!(json["type"], "control_request");
        assert_eq!(json["request"]["subtype"], "can_use_tool");
        assert_eq!(json["request"]["tool_name"], "Read");
    }

    #[test_case("claude-opus-4-6", "anthropic/claude-opus-4-6"; "bare_id_of_a_known_spec")]
    #[test_case("claude-future", "claude-future"; "bare_claude_id_not_in_registry")]
    #[test_case("openai/gpt-4", "openai/gpt-4"; "explicit_provider")]
    #[test_case("gpt-4o", "gpt-4o"; "unknown_passthrough")]
    fn resolve_model_spec_cases(input: &str, expected: &str) {
        let known = ["openai/gpt-5", "anthropic/claude-opus-4-6"];
        assert_eq!(resolve_model_spec(input, &known), expected);
    }

    #[test]
    fn decode_permission_response_variants() {
        assert!(matches!(
            decode_permission_response(&serde_json::json!({"behavior": "allow"})),
            PermissionAnswer::AllowOnce
        ));
        assert!(matches!(
            decode_permission_response(
                &serde_json::json!({"behavior": "allow", "updatedPermissions": []})
            ),
            PermissionAnswer::AllowSession
        ));
        assert!(matches!(
            decode_permission_response(&serde_json::json!({})),
            PermissionAnswer::Deny
        ));
        assert!(matches!(
            decode_permission_response(&serde_json::json!({"behavior": "something_else"})),
            PermissionAnswer::Deny
        ));
        match decode_permission_response(
            &serde_json::json!({"behavior": "deny", "message": "not now"}),
        ) {
            PermissionAnswer::DenyWithGuidance(msg) => assert_eq!(msg, "not now"),
            other => panic!("expected guidance, got {other:?}"),
        }
    }

    #[test]
    fn resolve_set_model_null_keeps_current_model() {
        let current = "openai/gpt-5";
        let result = resolve_set_model(Some(&Value::Null), current, &[]).unwrap();
        assert_eq!(result, current);
    }

    #[test]
    fn map_tool_names_in_content_maps_known_and_preserves_rest() {
        let content = serde_json::json!([
            {"type": "text", "text": "hello"},
            {"type": "tool_use", "name": "read", "id": "1", "input": {}},
            {"type": "tool_use", "name": "unknown_native", "id": "2", "input": {}},
        ]);
        let mapped = map_tool_names_in_content(&content);
        assert_eq!(mapped[0]["type"], "text");
        assert_eq!(mapped[1]["name"], "Read");
        assert_eq!(mapped[2]["name"], "unknown_native");
    }
}
