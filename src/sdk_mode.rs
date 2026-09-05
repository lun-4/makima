//! SDK streaming mode: `makima --print --input-format stream-json`.
//!
//! Wire protocol matches Claude Code's SDK interface so tools like Conductor, Windsurf, and custom
//! orchestrators work without adaptation.
//!
//! Per-message wire ids (`uuid`, assistant `message.id`) use `uuid::Uuid::now_v7()` to emit the
//! hyphenated-hex UUIDv7 shape that Claude Code SDK consumers expect, rather than makima's base58
//! `MakiId` canonical form.

use std::collections::{HashMap, HashSet};
use std::io::{self, BufRead, Write};
use std::mem;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use color_eyre::Result;
use color_eyre::eyre::{Context, eyre};
use flume::{Receiver, Sender};
use maki_agent::command::{CustomCommand, StandardCommands, StandardCompletions};
use maki_agent::headless::{self, InteractiveHandle, InteractiveParams};
use maki_agent::mcp;
use maki_agent::permissions::{PermissionAnswer, PluginRuleStore};
use maki_agent::prompt::ResolvedSlots;
use maki_agent::tools::{QUESTION_TOOL_NAME, QuestionMode};
use maki_agent::{
    AgentConfig, AgentEvent, AgentInput, AgentMode, Envelope, PermissionsConfig, ToolOutput,
    TurnOutcome,
};
use maki_commands::{
    AgentTurn, BuiltinOperation, CommandContent, CommandError, CommandFuture, CommandHost,
    CommandOutcome, CommandRegistry, HostRequest, HostResponse, InputDispatch, TargetCapabilities,
    TargetCapability, TargetHandle,
};
use maki_config::ModelPolicy;
use maki_providers::model::Model;
use maki_providers::provider::available_model_specs;
use maki_providers::{ImageSource, Message, StopReason, Timeouts, TokenUsage, add_cost};
use maki_storage::StateDir;
use maki_storage::id::{MakiId, SessionRef};
use maki_storage::session_lock;
use maki_storage::sessions::{SESSIONS_DIR, Session};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;
use tracing::warn;

use crate::cli::Cli;
use crate::command_attachments;

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
    ("task", "Task"),
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

    fn agent_mode(self, cwd: &Path) -> AgentMode {
        match self {
            Self::Plan => AgentMode::Plan(cwd.join("plan.md")),
            _ => AgentMode::Build,
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

    fn block_stop(&self) -> Value {
        serde_json::json!({
            "type": "content_block_stop",
            "index": self.block_index,
        })
    }
}

fn maki_to_claude_tool_name(name: &str) -> &str {
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
    pub model: Model,
    pub config: AgentConfig,
    pub permissions_config: PermissionsConfig,
    pub timeouts: Timeouts,
    pub prompt_slots: ResolvedSlots,
    pub fast: bool,
    pub workflow: bool,
    pub model_policy: Arc<ModelPolicy>,
    pub plugin_rules: Arc<PluginRuleStore>,
    pub commands: Vec<CustomCommand>,
    pub command_registry: CommandRegistry,
}

struct Shared {
    model: Model,
    permission_mode: PermissionMode,
    turn_start: Instant,
    pending: HashSet<String>,
}

struct CommandDriverParams {
    route_rx: Receiver<CommandRoute>,
    model_tx: Sender<Model>,
    shared: Arc<Mutex<Shared>>,
    model_policy: Arc<ModelPolicy>,
}

enum CommandRoute {
    Model {
        argument: String,
        response: Sender<Result<HostResponse, CommandError>>,
    },
}

struct SdkCommandHost {
    tx: Sender<CommandRoute>,
    model_specs: Arc<[Arc<str>]>,
}

impl CommandHost for SdkCommandHost {
    fn request(&self, request: HostRequest) -> CommandFuture<Result<HostResponse, CommandError>> {
        let tx = self.tx.clone();
        let model_specs = Arc::clone(&self.model_specs);
        Box::pin(async move {
            match request {
                HostRequest::Context(maki_commands::HostContextRequest::ModelSpecs) => Ok(
                    HostResponse::Context(maki_commands::HostContextResponse::Values(model_specs)),
                ),
                HostRequest::Context(_) => Ok(HostResponse::Context(
                    maki_commands::HostContextResponse::Unavailable,
                )),
                HostRequest::Builtin(BuiltinOperation::SetModel { spec }) => {
                    let (response, response_rx) = flume::bounded(1);
                    tx.send(CommandRoute::Model {
                        argument: spec.to_string(),
                        response,
                    })
                    .map_err(|_| CommandError::StaleTarget)?;
                    response_rx
                        .recv_async()
                        .await
                        .map_err(|_| CommandError::StaleTarget)?
                }
                HostRequest::Builtin(BuiltinOperation::QuickQuestion {
                    question,
                    attachments,
                }) => Ok(HostResponse::AgentTurn(AgentTurn {
                    content: CommandContent {
                        text: question,
                        attachments,
                    },
                    prompt: None,
                })),
                HostRequest::Builtin(operation) => Err(CommandError::Producer(Arc::from(format!(
                    "unsupported command operation: {operation:?}"
                )))),
            }
        })
    }
}

fn sdk_capabilities() -> TargetCapabilities {
    TargetCapabilities::from_slice(&[
        TargetCapability::AgentTurns,
        TargetCapability::ModelSelection,
    ])
}

struct SdkCommands {
    registry: CommandRegistry,
    target: TargetHandle,
    route_rx: Receiver<CommandRoute>,
    _standard_commands: StandardCommands,
}

impl SdkCommands {
    fn new(
        registry: CommandRegistry,
        custom: &[CustomCommand],
        model_specs: Arc<[Arc<str>]>,
    ) -> Result<Self> {
        let (route_tx, route_rx) = flume::unbounded();
        let standard_commands =
            StandardCommands::register(&registry, custom, StandardCompletions::default())?;
        let target = registry.bind_target(
            sdk_capabilities(),
            Arc::new(SdkCommandHost {
                tx: route_tx.clone(),
                model_specs,
            }),
        );
        Ok(Self {
            target,
            registry,
            route_rx,
            _standard_commands: standard_commands,
        })
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

    fn dispatch_input(&self, prompt: &str, images: &[ImageSource]) -> InputDispatch {
        let content = CommandContent {
            text: Arc::from(prompt),
            attachments: command_attachments::from_images(images),
        };
        smol::block_on(self.registry.dispatch_input(&self.target, content))
    }
}

/// Stops the session's heartbeat thread and releases its lock on drop, so
/// every exit path from `run` (including early `?` errors) tears the lock
/// down: the stop token wakes the loop, the join proves no beat is in
/// flight, and only then does the release run.
struct LockGuard {
    stop_tx: flume::Sender<()>,
    thread: Option<std::thread::JoinHandle<()>>,
    dir: PathBuf,
    id: MakiId,
}

impl Drop for LockGuard {
    fn drop(&mut self) {
        let _ = self.stop_tx.send(());
        if let Some(t) = self.thread.take()
            && t.join().is_ok()
        {
            session_lock::release(&self.dir, &self.id);
        }
    }
}

pub fn run(params: SdkParams) -> Result<()> {
    let SdkParams {
        cli,
        model,
        mut config,
        permissions_config,
        timeouts,
        prompt_slots,
        fast,
        workflow,
        model_policy,
        plugin_rules,
        commands,
        command_registry,
    } = params;
    cli.warn_ignored_flags();
    if let Some(max) = cli.max_turns {
        config.max_turns = Some(max);
    }
    let permission_mode = PermissionMode::resolve(cli.permission_mode.as_deref(), cli.yolo);

    let cwd = std::env::current_dir().unwrap_or_else(|_| ".".into());
    let working_dir = cwd.to_string_lossy().into_owned();
    let storage = StateDir::resolve().context("resolve state dir")?;
    let (session_id, initial_history) = resolve_session(&cli, &working_dir, &storage)?;

    let model_specs = available_model_specs(&model_policy)
        .into_iter()
        .map(Arc::from)
        .collect::<Vec<_>>()
        .into();
    let sdk_commands = SdkCommands::new(command_registry, &commands, model_specs)?;
    let (mcp_handle, mcp_config_errors) = smol::block_on(async {
        let (handle, errors) = mcp::start_with_commands(&cwd, sdk_commands.registry.clone()).await;
        if let Some(handle) = &handle {
            handle.ready().await;
        }
        (handle, errors)
    });
    if !mcp_config_errors.is_empty() {
        eprintln!("MCP config error: {mcp_config_errors}");
    }

    let startup_model = model.clone();
    let handle = headless::spawn_interactive(InteractiveParams {
        model,
        config,
        permissions_config,
        timeouts,
        prompt_slots: Arc::new(prompt_slots),
        excluded_tools: vec![QUESTION_TOOL_NAME],
        question_mode: QuestionMode::Headless,
        mcp_handle,
        initial_wd: cwd.clone(),
        session_id,
        initial_history,
        yolo: permission_mode == PermissionMode::BypassPermissions,
        system_prompt_override: cli.system_prompt.clone().filter(|s| !s.is_empty()),
        append_system_prompt: cli.append_system_prompt.clone().filter(|s| !s.is_empty()),
        workflow,
        modes: Arc::new(maki_agent::ModeRegistry::builtin()),
        model_policy: Arc::clone(&model_policy),
        plugin_rules,
        local_tools: Default::default(),
    });

    let (out_tx, out_rx) = flume::unbounded::<String>();
    let writer_thread = std::thread::spawn(move || {
        let mut stdout = io::stdout().lock();
        while let Ok(line) = out_rx.recv() {
            if writeln!(stdout, "{line}").and(stdout.flush()).is_err() {
                break;
            }
        }
    });

    let writer = SdkWriter {
        session_id: handle.session_id.clone(),
        out_tx,
    };
    // This run owns the session from here on: claim its lock and keep it
    // fresh on a dedicated thread, so other instances see it as open
    // elsewhere for the whole run. The guard tears the lock down on every
    // exit path; a std::thread keeps the periodic blocking I/O off smol's
    // global executor and is joinable, which a smol task is not.
    let locked_id = handle.session_id.id();
    let sessions_dir = storage
        .ensure_subdir(SESSIONS_DIR)
        .context("create sessions dir")?;
    if matches!(
        session_lock::heartbeat(&sessions_dir, &locked_id),
        Ok(session_lock::LockBeat::Lost)
    ) {
        return Err(eyre!(
            "session is open in another terminal; close it there first"
        ));
    }
    let (lock_stop_tx, lock_stop_rx) = flume::bounded(1);
    let lock_dir = sessions_dir.clone();
    let lock_heartbeat = std::thread::spawn(move || {
        loop {
            if lock_stop_rx
                .recv_timeout(session_lock::HEARTBEAT_INTERVAL)
                .is_ok()
            {
                return;
            }
            if matches!(
                session_lock::heartbeat(&lock_dir, &locked_id),
                Ok(session_lock::LockBeat::Lost)
            ) {
                return;
            }
        }
    });
    let lock_guard = LockGuard {
        stop_tx: lock_stop_tx,
        thread: Some(lock_heartbeat),
        dir: sessions_dir,
        id: locked_id,
    };
    let tools: Vec<&str> = handle
        .tool_names
        .iter()
        .map(|t| maki_to_claude_tool_name(t))
        .collect();
    writer.emit_system(
        "init",
        serde_json::json!({
            "cwd": working_dir,
            "tools": tools,
            "model": startup_model.id,
            "permissionMode": permission_mode.as_str(),
            "apiKeySource": "none",
            "mcp_servers": [],
            "slash_commands": sdk_commands.slash_commands(),
            "output_style": "default",
        }),
    )?;
    let projection_watcher = watch_command_projection(
        writer.clone(),
        sdk_commands.registry.clone(),
        sdk_commands.target.clone(),
    );

    let shared = Arc::new(Mutex::new(Shared {
        model: startup_model.clone(),
        permission_mode,
        turn_start: Instant::now(),
        pending: HashSet::new(),
    }));

    let pump = EventPump {
        writer: writer.clone(),
        shared: Arc::clone(&shared),
        answer_tx: handle.answer_tx.clone(),
        include_partial_messages: cli.include_partial_messages,
        synth: StreamSynth::new(),
        tool_inputs: HashMap::new(),
        result_text: String::new(),
        cost: None,
        request_counter: 0,
    }
    .spawn(handle.event_rx.clone());
    let command_driver = spawn_command_driver(CommandDriverParams {
        route_rx: sdk_commands.route_rx.clone(),
        model_tx: handle.model_tx.clone(),
        shared: Arc::clone(&shared),
        model_policy: Arc::clone(&model_policy),
    });

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

            match msg.msg_type {
                InboundMessageType::User => {
                    let Some(user) = parse_or_warn::<InboundUser>(msg.payload, "user message")
                    else {
                        continue;
                    };
                    let content = user.message.content;
                    let prompt = content_text(&content).unwrap_or_else(|| content.to_string());
                    let images = content_images(&content);
                    let mode = {
                        let mut shared = shared.lock().unwrap();
                        shared.turn_start = Instant::now();
                        shared.permission_mode
                    };
                    match sdk_commands.dispatch_input(&prompt, &images) {
                        InputDispatch::Dispatched(CommandOutcome::AgentTurn(turn)) => {
                            let input = command_attachments::agent_input(
                                turn,
                                mode.agent_mode(&cwd),
                                fast,
                                workflow,
                            )?;
                            if handle.input_tx.send(input).is_err() {
                                break;
                            }
                        }
                        InputDispatch::Dispatched(CommandOutcome::Completed) => {
                            emit_command_result(&writer, &shared, false, String::new())?
                        }
                        InputDispatch::Dispatched(CommandOutcome::Failed(error)) => {
                            emit_command_result(&writer, &shared, true, error.to_string())?
                        }
                        InputDispatch::LiteralInput(content) => {
                            let input = AgentInput {
                                message: content.text.to_string(),
                                mode: mode.agent_mode(&cwd),
                                images: command_attachments::into_images(&content.attachments)?,
                                preamble: Vec::new(),
                                thinking: Default::default(),
                                fast,
                                workflow,
                                prompt: None,
                            };
                            if handle.input_tx.send(input).is_err() {
                                break;
                            }
                        }
                    }
                }
                InboundMessageType::ControlRequest => {
                    let Some(cr) =
                        parse_or_warn::<InboundControlRequest>(msg.payload, "control_request")
                    else {
                        continue;
                    };
                    handle_control_request(
                        &cr,
                        &writer,
                        &handle,
                        &shared,
                        &startup_model,
                        &model_policy,
                        &sdk_commands,
                    )?;
                }
                InboundMessageType::ControlResponse => {
                    let Some(cr) =
                        parse_or_warn::<InboundControlResponse>(msg.payload, "control_response")
                    else {
                        continue;
                    };
                    let data = cr.response;
                    if let Some(req_id) = data.get("request_id").and_then(Value::as_str)
                        && shared.lock().unwrap().pending.remove(req_id)
                    {
                        let _ = handle
                            .answer_tx
                            .send(decode_permission_response(&data).encode());
                    }
                }
                InboundMessageType::ControlCancelRequest => {
                    let Some(ccr) = parse_or_warn::<InboundControlCancelRequest>(
                        msg.payload,
                        "control_cancel_request",
                    ) else {
                        continue;
                    };
                    if shared.lock().unwrap().pending.remove(&ccr.request_id) {
                        let _ = handle.answer_tx.send(PermissionAnswer::Deny.encode());
                    }
                }
                InboundMessageType::Unknown(message_type) => {
                    warn!("unknown inbound message type: {message_type}")
                }
            }
        }
        Ok(())
    })();

    drop(sdk_commands);
    smol::block_on(stop_sdk_tasks(projection_watcher, command_driver));
    let InteractiveHandle { input_tx, task, .. } = handle;
    drop(input_tx);
    smol::block_on(async {
        task.await;
        pump.await;
    });
    drop(lock_guard);
    drop(writer);
    let _ = writer_thread.join();
    input_result
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

async fn stop_sdk_tasks(projection_watcher: smol::Task<()>, command_driver: smol::Task<()>) {
    projection_watcher.cancel().await;
    command_driver.cancel().await;
}

fn spawn_command_driver(params: CommandDriverParams) -> smol::Task<()> {
    smol::spawn(async move {
        let CommandDriverParams {
            route_rx,
            model_tx,
            shared,
            model_policy,
        } = params;
        while let Ok(route) = route_rx.recv_async().await {
            match route {
                CommandRoute::Model { argument, response } => {
                    let result = match Model::from_spec(&argument)
                        .ok()
                        .filter(|model| model_policy.allows(&model.spec()))
                    {
                        Some(model) if model_tx.send(model.clone()).is_ok() => {
                            shared.lock().unwrap().model = model;
                            Ok(HostResponse::Completed)
                        }
                        Some(_) => Err(CommandError::StaleTarget),
                        None => Err(CommandError::Producer(Arc::from(
                            "invalid or disallowed model",
                        ))),
                    };
                    let _ = response.send(result);
                }
            }
        }
    })
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

fn resolve_session(
    cli: &Cli,
    cwd: &str,
    storage: &StateDir,
) -> Result<(Option<SessionRef>, Vec<Message>)> {
    let sessions_dir = storage
        .ensure_subdir(SESSIONS_DIR)
        .context("create sessions dir")?;
    // A bare continue flag (no ID) is rejected by the TUI guard before SDK
    // mode starts, so only a valued ID reaches this branch.
    let (resumed_id, history) =
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
            (resumed, session.take_messages())
        } else if cli.last_session {
            match StoredSession::latest(cwd, storage) {
                Ok(Some(session)) => {
                    if let Some(block) = resume_block_for(&sessions_dir, &session, cwd) {
                        return Err(eyre!("session {}: {block}", session.id));
                    }
                    (Some(SessionRef::from(session.id)), session.take_messages())
                }
                _ => (None, Vec::new()),
            }
        } else {
            (None, Vec::new())
        };

    let cli_session_id = cli.session_id.as_deref().map(|s| {
        s.parse::<SessionRef>()
            .map_err(|e| eyre!("invalid session id {s:?}: {e}"))
    });
    let cli_session_id = match cli_session_id {
        // A successful load proves the pinned ID pre-exists, so it must
        // belong to this directory and be free of other holders; a fresh ID
        // simply starts new.
        Some(Ok(id)) => {
            if let Ok(session) = StoredSession::load(id.id(), storage)
                && let Some(block) = resume_block_for(&sessions_dir, &session, cwd)
            {
                return Err(eyre!("session {}: {block}", session.id));
            }
            Some(id)
        }
        Some(Err(e)) => return Err(e),
        None => None,
    };

    Ok((cli_session_id.or(resumed_id), history))
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

fn handle_control_request(
    cr: &InboundControlRequest,
    writer: &SdkWriter,
    handle: &InteractiveHandle,
    shared: &Mutex<Shared>,
    startup_model: &Model,
    model_policy: &ModelPolicy,
    commands: &SdkCommands,
) -> Result<()> {
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
            let _ = handle.cancel_tx.try_send(());
            writer.emit_control_response(&cr.request_id, ok, None)
        }
        InboundControlRequestType::SetPermissionMode => {
            let mode_str = cr.request.extra.get("mode").and_then(Value::as_str);
            match mode_str.and_then(PermissionMode::parse) {
                Some(mode) => {
                    shared.lock().unwrap().permission_mode = mode;
                    writer.emit_control_response(&cr.request_id, ok, None)
                }
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
            match resolve_set_model(cr.request.extra.get("model"), startup_model, model_policy) {
                Some(model) => {
                    let _ = handle.model_tx.send(model.clone());
                    shared.lock().unwrap().model = model;
                    writer.emit_control_response(&cr.request_id, ok, None)
                }
                None => writer.emit_control_response(
                    &cr.request_id,
                    None,
                    Some("invalid or disallowed model".into()),
                ),
            }
        }
        InboundControlRequestType::Unknown(subtype) => writer.emit_control_response(
            &cr.request_id,
            None,
            Some(format!("unsupported: {subtype}")),
        ),
    }
}

fn resolve_set_model(
    model_val: Option<&Value>,
    startup_model: &Model,
    model_policy: &ModelPolicy,
) -> Option<Model> {
    match model_val? {
        Value::Null => Some(startup_model.clone()),
        Value::String(model_str) => {
            let spec = resolve_model_spec(model_str);
            if !model_policy.allows(&spec) {
                warn!(model = %spec, "ignoring model disallowed by policy");
                return None;
            }
            match Model::from_spec(&spec) {
                Ok(m) => Some(m),
                Err(e) => {
                    warn!(model = %model_str, error = %e, "ignoring invalid model");
                    None
                }
            }
        }
        _ => None,
    }
}

fn resolve_model_spec(model_id: &str) -> String {
    if model_id.contains('/') {
        return model_id.to_string();
    }
    if model_id.starts_with("claude-") {
        return format!("anthropic/{model_id}");
    }
    model_id.to_string()
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
    answer_tx: Sender<String>,
    include_partial_messages: bool,
    synth: StreamSynth,
    tool_inputs: HashMap<String, (String, Value)>,
    result_text: String,
    /// Summed as the turns land: rates move mid-prompt, and only a turn knows
    /// the rate it paid.
    cost: Option<f64>,
    request_counter: u64,
}

impl EventPump {
    fn spawn(mut self, event_rx: Receiver<Envelope>) -> smol::Task<()> {
        smol::spawn(async move {
            while let Ok(envelope) = event_rx.recv_async().await {
                if let Err(e) = self.handle(envelope) {
                    warn!(error = %e, "sdk event pump stopped");
                    break;
                }
            }
        })
    }

    fn model_id(&self) -> String {
        self.shared.lock().unwrap().model.id.clone()
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

    fn emit_turn_result(
        &mut self,
        is_error: bool,
        result: String,
        num_turns: u32,
        usage: TokenUsage,
    ) -> Result<()> {
        let duration_ms = self.shared.lock().unwrap().turn_start.elapsed().as_millis();
        // Zero on an unpriced model, which is what its turns reported too.
        let total_cost_usd = self.cost.unwrap_or_default();
        self.writer.emit(WireInner::Result(ResultPayload {
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
        }))?;
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
            AgentEvent::ToolStart(ts) => {
                let name = ts.tool.to_string();
                let input = ts.raw_input.clone().unwrap_or(Value::Null);

                if self.include_partial_messages {
                    let model = self.model_id();
                    let events = self.synth.tool_use(
                        &model,
                        &ts.id,
                        maki_to_claude_tool_name(&name),
                        &serde_json::to_string(&input)?,
                    );
                    self.emit_stream(events)?;
                }
                self.tool_inputs.insert(ts.id.clone(), (name, input));
            }
            AgentEvent::ToolPending { .. }
            | AgentEvent::ToolOutput { .. }
            | AgentEvent::ToolDone(_)
            | AgentEvent::QueueItemConsumed { .. }
            | AgentEvent::QueueDrained
            | AgentEvent::AutoCompacting
            | AgentEvent::CompactionDone
            | AgentEvent::AuthRequired
            | AgentEvent::SubagentHistory { .. }
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
                if self.shared.lock().unwrap().permission_mode == PermissionMode::BypassPermissions
                {
                    let _ = self.answer_tx.send(PermissionAnswer::AllowSession.encode());
                    return Ok(());
                }

                let (tool_name, input) = self
                    .tool_inputs
                    .get(id)
                    .cloned()
                    .unwrap_or_else(|| (tool.to_string(), Value::Null));

                self.request_counter += 1;
                let req_id = format!("req_{}", self.request_counter);
                self.shared.lock().unwrap().pending.insert(req_id.clone());

                self.writer
                    .emit(WireInner::ControlRequest(ControlRequestPayload {
                        request_id: req_id,
                        request: ControlRequestInner {
                            subtype: "can_use_tool",
                            tool_name: Some(maki_to_claude_tool_name(&tool_name).into()),
                            input: Some(input),
                            tool_use_id: Some(id.clone()),
                        },
                    }))?;
            }
            AgentEvent::TurnOutcome(outcome) => {
                let result = mem::take(&mut self.result_text);
                match outcome {
                    TurnOutcome::Completed {
                        usage, num_turns, ..
                    } => self.emit_turn_result(false, result, *num_turns, *usage)?,
                    TurnOutcome::Failed {
                        usage,
                        num_turns,
                        failure,
                        ..
                    } => self.emit_turn_result(
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
                    } => self.emit_turn_result(true, result, *num_turns, *usage)?,
                }
            }
            AgentEvent::ControlComplete { .. } => {}
            AgentEvent::ControlError { message } => {
                self.emit_turn_result(true, message.clone(), 0, TokenUsage::default())?;
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
                        b["name"] = Value::String(maki_to_claude_tool_name(name).to_string());
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
    use super::*;
    use test_case::test_case;

    const REJECTED_ATTACHMENT: &str = "command rejected attachment";

    struct OutcomeBehavior(CommandOutcome);

    struct ReplaceAttachment;
    struct RejectAttachment;

    impl maki_commands::CommandBehavior for RejectAttachment {
        fn execute(
            &self,
            invocation: maki_commands::CommandInvocation,
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

    impl maki_commands::CommandBehavior for ReplaceAttachment {
        fn execute(
            &self,
            invocation: maki_commands::CommandInvocation,
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
                Ok(CommandOutcome::AgentTurn(AgentTurn {
                    content: CommandContent {
                        text: Arc::from("inspected"),
                        attachments: Arc::from([maki_commands::CommandAttachment {
                            media_type: Arc::from("image/jpeg"),
                            data: Arc::from("BBBB"),
                        }]),
                    },
                    prompt: None,
                }))
            })
        }
    }

    impl maki_commands::CommandBehavior for OutcomeBehavior {
        fn execute(
            &self,
            _invocation: maki_commands::CommandInvocation,
        ) -> CommandFuture<Result<CommandOutcome, CommandError>> {
            let outcome = self.0.clone();
            Box::pin(async move { Ok(outcome) })
        }
    }

    fn sdk_commands(registry: CommandRegistry) -> SdkCommands {
        SdkCommands::new(
            registry,
            &[],
            Arc::from([Arc::from("openai/gpt-5"), Arc::from("anthropic/claude")]),
        )
        .unwrap()
    }

    fn registration(name: &str, outcome: CommandOutcome) -> maki_commands::Registration {
        maki_commands::Registration {
            spec: maki_commands::CommandSpec {
                name: Arc::from(name),
                aliases: Arc::from([]),
                arguments: maki_commands::ArgumentArity::ANY,
                docs: maki_commands::CommandDocs {
                    summary: Arc::from(format!("{name} description")),
                    argument_hint: Some(Arc::from("<arg>")),
                },
                required_capabilities: TargetCapabilities::default(),
            },
            behavior: Arc::new(OutcomeBehavior(outcome)),
            completion: None,
        }
    }

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
                .expect_err("a session stored under another cwd must be rejected");
            assert!(err.to_string().contains(OTHER_CWD));
        }
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

        let (resumed, _history) =
            resolve_session(&cli, THIS_CWD, &storage).expect("an unlocked session loads");
        assert_eq!(resumed.map(|r| r.id()), Some(session.id));

        fs::write(
            session_lock::lock_path(&sessions_dir, &session.id),
            FAKE_PID.to_string(),
        )
        .unwrap();
        let err = resolve_session(&cli, THIS_CWD, &storage)
            .expect_err("a session locked by another instance must be rejected");
        assert!(err.to_string().contains(session_lock::OPEN_ELSEWHERE_MSG));
    }

    fn claude_to_maki_tool_name(name: &str) -> &str {
        TOOL_NAME_MAP
            .iter()
            .find(|(_, c)| *c == name)
            .map(|(m, _)| *m)
            .unwrap_or(name)
    }

    #[test]
    fn sdk_commands_projection_includes_shared_producers_and_supported_model_only() {
        let registry = CommandRegistry::new();
        let plugin = registry.create_producer(maki_commands::ProducerPrecedence::Plugin);
        plugin
            .replace(vec![registration("/lua", CommandOutcome::Completed)])
            .unwrap();
        let mcp = registry.create_producer(maki_commands::ProducerPrecedence::Mcp);
        let mut prompt = registration("/mcp:prompt", CommandOutcome::Completed);
        prompt.spec.required_capabilities =
            TargetCapabilities::from_capability(TargetCapability::AgentTurns);
        mcp.replace(vec![prompt]).unwrap();
        let commands = sdk_commands(registry);

        assert_eq!(
            commands.slash_commands(),
            ["lua", "mcp:prompt", "model", "btw"]
        );
        assert!(matches!(
            commands.dispatch_input("/mcp:prompt", &[]),
            InputDispatch::Dispatched(CommandOutcome::Completed)
        ));
        let InputDispatch::Dispatched(CommandOutcome::AgentTurn(turn)) =
            commands.dispatch_input("/btw explain this", &[])
        else {
            panic!("quick question did not return an agent turn");
        };
        assert_eq!(turn.content.text.as_ref(), "explain this");
        assert!(matches!(
            commands.dispatch_input("/btw", &[]),
            InputDispatch::Dispatched(CommandOutcome::Failed(CommandError::InvalidArguments {
                command,
                expected: maki_commands::ArgumentArity::ONE_OR_MORE,
                actual: 0,
            })) if command.as_ref() == "/btw"
        ));
        let projection = commands.projection();
        assert_eq!(projection[0]["description"], "/lua description");
        assert_eq!(projection[0]["argumentHint"], "<arg>");
    }

    #[test_case("/model gpt-5", "openai/gpt-5"; "shorthand")]
    #[test_case("/model openai/gpt-5", "openai/gpt-5"; "qualified")]
    fn sdk_model_command_routes_qualified_spec(input: &str, expected: &str) {
        let commands = sdk_commands(CommandRegistry::new());
        let registry = commands.registry.clone();
        let target = commands.target.clone();
        let content = CommandContent::from(input);
        let dispatch = smol::spawn(async move { registry.dispatch_input(&target, content).await });

        let CommandRoute::Model { argument, response } = commands.route_rx.recv().unwrap();
        assert_eq!(argument, expected);
        response.send(Ok(HostResponse::Completed)).unwrap();
        assert!(matches!(
            smol::block_on(dispatch),
            InputDispatch::Dispatched(CommandOutcome::Completed)
        ));
    }

    #[test]
    fn sdk_bare_model_returns_shared_usage_error() {
        let commands = sdk_commands(CommandRegistry::new());
        assert!(matches!(
            commands.dispatch_input("/model", &[]),
            InputDispatch::Dispatched(CommandOutcome::Failed(CommandError::Producer(message)))
                if message.as_ref() == "Usage: /model <model>"
        ));
        assert!(commands.route_rx.is_empty());
    }

    #[test]
    fn sdk_command_behavior_owns_attachment_policy() {
        let registry = CommandRegistry::new();
        let plugin = registry.create_producer(maki_commands::ProducerPrecedence::Plugin);
        let mut inspect = registration("/inspect", CommandOutcome::Completed);
        inspect.behavior = Arc::new(ReplaceAttachment);
        let mut reject = registration("/reject", CommandOutcome::Completed);
        reject.behavior = Arc::new(RejectAttachment);
        plugin.replace(vec![inspect, reject]).unwrap();
        let commands = sdk_commands(registry);
        let images = vec![ImageSource::new(
            maki_providers::ImageMediaType::Png,
            Arc::from("AAAA"),
        )];

        let InputDispatch::Dispatched(CommandOutcome::AgentTurn(turn)) =
            commands.dispatch_input("/inspect now", &images)
        else {
            panic!("attachment-aware command did not return an agent turn");
        };
        let input = command_attachments::agent_input(turn, AgentMode::Build, false, false).unwrap();
        assert_eq!(input.message, "inspected");
        assert_eq!(input.images.len(), 1);
        assert_eq!(
            input.images[0].media_type,
            maki_providers::ImageMediaType::Jpeg
        );
        assert_eq!(input.images[0].data.as_ref(), "BBBB");

        assert!(matches!(
            commands.dispatch_input("/reject", &images),
            InputDispatch::Dispatched(CommandOutcome::Failed(CommandError::Producer(message)))
                if message.as_ref() == REJECTED_ATTACHMENT
        ));
    }

    #[test]
    fn sdk_dispatch_returns_outcomes_and_preserves_literal_input() {
        let registry = CommandRegistry::new();
        let plugin = registry.create_producer(maki_commands::ProducerPrecedence::Plugin);
        plugin
            .replace(vec![registration("/done", CommandOutcome::Completed)])
            .unwrap();
        let commands = sdk_commands(registry.clone());

        assert!(matches!(
            smol::block_on(registry.dispatch_input(&commands.target, "/done now".into())),
            InputDispatch::Dispatched(CommandOutcome::Completed)
        ));
        let images = vec![ImageSource::new(
            maki_providers::ImageMediaType::Gif,
            Arc::from("AAAA"),
        )];
        assert!(matches!(
            commands.dispatch_input("/unknown literal", &images),
            InputDispatch::Dispatched(CommandOutcome::Failed(CommandError::UnknownCommand(
                name
            ))) if name.as_ref() == "/unknown"
        ));
        let InputDispatch::LiteralInput(content) =
            commands.dispatch_input("//unknown literal", &images)
        else {
            panic!("escaped unknown command did not remain literal input");
        };
        assert_eq!(content.text.as_ref(), "/unknown literal");
        assert_eq!(content.attachments.len(), 1);
        assert_eq!(content.attachments[0].media_type.as_ref(), "image/gif");
        assert_eq!(content.attachments[0].data.as_ref(), "AAAA");
        let InputDispatch::LiteralInput(content) =
            commands.dispatch_input("///unknown literal", &[])
        else {
            panic!("triple-slash input did not remain literal");
        };
        assert_eq!(content.text.as_ref(), "//unknown literal");
        assert!(matches!(
            commands.dispatch_input("ordinary literal", &[]),
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
    #[test_case("task", "Task")]
    #[test_case("multiedit", "MultiEdit")]
    #[test_case("code_execution", "CodeExecution")]
    #[test_case("index", "Index")]
    #[test_case("memory", "Memory")]
    #[test_case("question", "Question")]
    fn maki_to_claude_roundtrip(maki: &str, claude: &str) {
        assert_eq!(maki_to_claude_tool_name(maki), claude);
        assert_eq!(claude_to_maki_tool_name(claude), maki);
    }

    #[test]
    fn unknown_tool_name_passthrough() {
        assert_eq!(maki_to_claude_tool_name("unknown_tool"), "unknown_tool");
        assert_eq!(claude_to_maki_tool_name("UnknownTool"), "UnknownTool");
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

    #[test_case("claude-opus-4-6", "anthropic/claude-opus-4-6"; "claude_prefix")]
    #[test_case("openai/gpt-4", "openai/gpt-4"; "explicit_provider")]
    #[test_case("gpt-4o", "gpt-4o"; "unknown_passthrough")]
    fn resolve_model_spec_cases(input: &str, expected: &str) {
        assert_eq!(resolve_model_spec(input), expected);
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
    fn resolve_set_model_null_returns_startup() {
        let startup = Model::from_spec("anthropic/claude-sonnet-4-20250514").unwrap();
        let result = resolve_set_model(
            Some(&Value::Null),
            &startup,
            &maki_config::ModelPolicy::default(),
        )
        .unwrap();
        assert_eq!(result.id, startup.id);
    }

    #[test]
    fn resolve_set_model_rejects_disallowed_exact_spec() {
        let startup = Model::from_spec("anthropic/claude-sonnet-4-20250514").unwrap();
        let raw: maki_config::RawConfig = serde_json::from_value(serde_json::json!({
            "provider": {"allowed_models": [startup.spec()]}
        }))
        .unwrap();
        let policy = raw.into_config(false).unwrap().provider.model_policy;

        assert!(
            resolve_set_model(
                Some(&Value::String("openai/gpt-5".into())),
                &startup,
                &policy
            )
            .is_none()
        );
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
