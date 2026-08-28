use std::collections::HashMap;
use std::io::Write;
use std::iter;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};

use agent_client_protocol_schema::{
    AgentNotification, AgentRequest, AgentResponse, AvailableCommand, AvailableCommandInput,
    AvailableCommandsUpdate, ConfigOptionUpdate, ContentBlock, CreateElicitationResponse,
    CurrentModeUpdate, EmbeddedResourceResource, Error as AcpError, ImageContent,
    InitializeRequest, JsonRpcMessage, LoadSessionRequest, McpServer, NewSessionRequest,
    Notification, PromptRequest, PromptResponse, Request, RequestId, RequestPermissionRequest,
    RequestPermissionResponse, Response, SessionId, SessionModeId, SessionNotification,
    SessionUpdate, SetSessionConfigOptionRequest, SetSessionConfigOptionResponse,
    SetSessionModeRequest, SetSessionModeResponse, StopReason, TextContent, ToolCallId,
    ToolCallUpdate, ToolCallUpdateFields, UnstructuredCommandInput,
};
use flume::{Receiver, Sender, WeakSender};
use maki_agent::headless::{self, InteractiveHandle, InteractiveParams};
use maki_agent::mcp::config::{RawHttpFields, RawStdioFields, RawTransport};
use maki_agent::mcp::{self, McpHandle};
use maki_agent::permissions::PermissionAnswer;
use maki_agent::tools::QuestionMode;
use maki_agent::types::AgentEvent;
use maki_agent::{AgentInput, AgentMode, Envelope, ImageMediaType, ImageSource};
use maki_commands::{
    AgentTurn, CommandAttachment, CommandContent, CommandOutcome, InputDispatch, PresentedCommand,
    TargetHandle,
};
use maki_config::{MAX_SERVER_NAME_LEN, ModelPolicy};
use maki_providers::model::Model;
use maki_providers::provider::{available_model_specs, fetch_all_models};
use maki_providers::{Message, TokenUsage, add_cost, settle_session};
use maki_storage::id::{MakiId, SessionRef};
use maki_storage::session_lock;
use maki_storage::sessions::{SESSIONS_DIR, StoredTokenUsage};
use serde::Serialize;
use serde_json::Value;
use smol::io::AsyncBufReadExt;
use tracing::{debug, warn};

use crate::{AcpParams, methods, permissions, translate};

const FIRST_OUTGOING_REQUEST_ID: i64 = 1000;
const RESTORED_FAST: bool = false;

/// Ids come from here and are never reused, so a late answer for a closed
/// session cannot match a request of the session that replaced it.
static NEXT_OUTGOING_REQUEST_ID: AtomicI64 = AtomicI64::new(FIRST_OUTGOING_REQUEST_ID);

/// What the client still owes us. Only one permission or elicitation can be
/// outstanding: the agent holds the answer channel while it waits for one.
#[derive(Default)]
struct Pending {
    prompt: Option<RequestId>,
    permission: Option<i64>,
    elicitation: Option<i64>,
}

type PendingState = Arc<Mutex<Pending>>;

/// A session's cross-process lock: the heartbeat thread that keeps it fresh
/// and where to release it. Dropping stops the thread and releases the lock;
/// the join in the drop guarantees no beat lands after the release, which
/// also covers process shutdown after stdin EOF, where `close_session` never
/// runs.
struct SessionLock {
    dir: PathBuf,
    id: MakiId,
    stop_tx: flume::Sender<()>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl SessionLock {
    /// Stop the heartbeat thread and release the lock. Only releases if the
    /// thread stopped cleanly, so no beat can land after the release.
    fn shutdown(mut self) {
        self.release();
    }

    fn release(&mut self) {
        let _ = self.stop_tx.send(());
        if let Some(t) = self.thread.take()
            && t.join().is_ok()
        {
            session_lock::release(&self.dir, &self.id);
        }
    }
}

impl Drop for SessionLock {
    fn drop(&mut self) {
        self.release();
    }
}

struct SessionState {
    handle: InteractiveHandle,
    mcp: Option<McpHandle>,
    current_mode: AgentMode,
    command_state: Arc<maki_agent::command::SessionCommandState>,
    pending: PendingState,
    command_registry: maki_commands::CommandRegistry,
    command_target: TargetHandle,
    command_projection_task: smol::Task<()>,
    lock: Option<SessionLock>,
}

struct Server {
    out_tx: Sender<Value>,
    model_specs: Vec<String>,
    model_policy: Arc<ModelPolicy>,
    modes: Arc<maki_agent::ModeRegistry>,
    session: Option<SessionState>,
    /// Whether the client advertised form elicitation support at `initialize`.
    elicitation: bool,
}

impl Server {
    fn respond(&self, id: RequestId, result: Result<AgentResponse, AcpError>) {
        send(&self.out_tx, Response::new(id, result));
    }
}

pub async fn serve(params: AcpParams) -> color_eyre::Result<()> {
    let (out_tx, out_rx) = flume::unbounded::<Value>();

    let writer_task = smol::spawn(async move {
        let stdout = std::io::stdout();
        while let Ok(msg) = out_rx.recv_async().await {
            let mut handle = stdout.lock();
            if serde_json::to_writer(&mut handle, &msg).is_ok() {
                let _ = handle.write_all(b"\n");
                let _ = handle.flush();
            }
        }
    });

    let mut server = Server {
        out_tx,
        model_specs: available_model_specs(&params.model_policy),
        model_policy: Arc::clone(&params.model_policy),
        modes: Arc::clone(&params.modes),
        session: None,
        elicitation: false,
    };

    let (in_tx, in_rx) = flume::unbounded::<Incoming>();
    discover_models(Arc::clone(&params.model_policy), in_tx.downgrade());
    let _reader_task = smol::spawn(read_stdin(in_tx));
    while let Ok(incoming) = in_rx.recv_async().await {
        match incoming {
            Incoming::Line(line) => handle_line(&mut server, &line, &params).await,
            Incoming::Models(batch) => refresh_models(&mut server, batch),
        }
    }

    drop(server);
    writer_task.await;

    Ok(())
}

enum Incoming {
    Line(String),
    Models(Vec<String>),
}

async fn read_stdin(tx: Sender<Incoming>) -> std::io::Result<()> {
    let mut reader = smol::io::BufReader::new(smol::Unblock::new(std::io::stdin()));
    loop {
        let mut line = String::new();
        if reader.read_line(&mut line).await? == 0 {
            return Ok(());
        }
        if tx.send_async(Incoming::Line(line)).await.is_err() {
            return Ok(());
        }
    }
}

fn discover_models(policy: Arc<ModelPolicy>, tx: WeakSender<Incoming>) {
    smol::spawn(async move {
        fetch_all_models(
            &policy,
            |batch| {
                if let Some(tx) = tx.upgrade() {
                    let _ = tx.send(Incoming::Models(batch.models));
                }
            },
            None,
        )
        .await;
    })
    .detach();
}

fn refresh_models(srv: &mut Server, batch: Vec<String>) {
    let old_len = srv.model_specs.len();
    for spec in batch {
        if !srv.model_specs.contains(&spec) {
            srv.model_specs.push(spec);
        }
    }
    if srv.model_specs.len() == old_len {
        return;
    }
    let Some(session) = &srv.session else { return };
    let current_model = session
        .command_state
        .current_model
        .lock()
        .unwrap_or_else(|error| error.into_inner());
    session
        .command_state
        .set_model_specs(srv.model_specs.clone());
    let option = methods::model_config_option(&current_model, &srv.model_specs);
    session_update(
        &srv.out_tx,
        &SessionId::from(session.handle.session_id.to_string()),
        SessionUpdate::ConfigOptionUpdate(ConfigOptionUpdate::new(vec![option])),
    );
}

async fn handle_line(server: &mut Server, line: &str, params: &AcpParams) {
    let trimmed = line.trim();
    if trimmed.is_empty() {
        return;
    }
    let raw: Value = match serde_json::from_str(trimmed) {
        Ok(v) => v,
        Err(e) => {
            warn!(error = %e, "invalid JSON on stdin");
            server.respond(RequestId::Null, Err(AcpError::parse_error()));
            return;
        }
    };
    let id = raw.get("id").map(request_id);
    if raw.get("result").is_some() || raw.get("error").is_some() {
        handle_incoming_response(server, &raw);
    } else if let Some(method) = raw.get("method").and_then(Value::as_str) {
        match id {
            Some(id) => handle_request(server, method, id, &raw, params).await,
            None => handle_notification(server, method),
        }
    } else if let Some(id) = id {
        server.respond(id, Err(AcpError::invalid_request()));
    }
}

fn request_id(v: &Value) -> RequestId {
    serde_json::from_value(v.clone()).unwrap_or(RequestId::Null)
}

async fn handle_request(
    srv: &mut Server,
    method: &str,
    id: RequestId,
    raw: &Value,
    params: &AcpParams,
) {
    let result = match method {
        "initialize" => {
            srv.elicitation = parse_params::<InitializeRequest>(raw).is_ok_and(|req| {
                req.client_capabilities
                    .elicitation
                    .as_ref()
                    .is_some_and(|c| c.form.is_some())
            });
            Ok(AgentResponse::InitializeResponse(
                methods::initialize_response(),
            ))
        }
        "session/new" => new_session(srv, raw, params).await,
        "session/load" => load_session(srv, raw, params).await,
        "session/prompt" => match handle_prompt(srv, raw, &id).await {
            Ok(()) => return,
            Err(e) => Err(e),
        },
        "session/set_mode" => handle_set_mode(srv, raw),
        "session/set_config_option" => handle_set_config(srv, raw),
        _ => Err(AcpError::method_not_found()),
    };
    srv.respond(id, result);
}

async fn new_session(
    srv: &mut Server,
    raw: &Value,
    params: &AcpParams,
) -> Result<AgentResponse, AcpError> {
    let req: NewSessionRequest = parse_params(raw)?;
    close_session(srv).await;
    let cwd = req.cwd.clone();
    let mcp = start_mcp(&req.cwd, &req.mcp_servers, params).await;
    let handle = spawn_session(
        params,
        req.cwd,
        None,
        Vec::new(),
        mcp.clone(),
        srv.elicitation,
    );
    let spec = params.model.spec();
    let resp = methods::new_session_response(handle.session_id.as_str(), &srv.modes)
        .config_options(vec![methods::model_config_option(&spec, &srv.model_specs)]);
    install_session(srv, handle, mcp, spec, None, cwd, params);
    Ok(AgentResponse::NewSessionResponse(resp))
}

async fn load_session(
    srv: &mut Server,
    raw: &Value,
    params: &AcpParams,
) -> Result<AgentResponse, AcpError> {
    let req: LoadSessionRequest = parse_params(raw)?;
    let session_ref: SessionRef = req
        .session_id
        .0
        .parse()
        .map_err(|_| AcpError::resource_not_found(Some(req.session_id.0.to_string())))?;
    let mut restored = load_history(session_ref.id())?;
    close_session(srv).await;
    let mcp = start_mcp(&req.cwd, &req.mcp_servers, params).await;
    let sid = SessionId::from(session_ref.to_string());
    let home = maki_storage::paths::home();
    let replay_cwd = restored.cwd.as_deref().unwrap_or(&req.cwd);
    for update in translate::replay_history(&restored.history, replay_cwd, home.as_deref()) {
        session_update(&srv.out_tx, &sid, update);
    }
    let session_cwd = req.cwd.clone();
    let handle = spawn_session(
        params,
        req.cwd,
        Some(session_ref),
        restored.history,
        mcp.clone(),
        srv.elicitation,
    );
    let spec = params.model.spec();
    let resp = methods::load_session_response(&srv.modes)
        .config_options(vec![methods::model_config_option(&spec, &srv.model_specs)]);
    let recorded_model = Model::from_spec(&restored.model).unwrap_or_else(|_| params.model.clone());
    let restored_cost = settle_session(
        &restored.usage,
        &mut restored.by_model,
        &recorded_model,
        RESTORED_FAST,
    );
    install_session(srv, handle, mcp, spec, restored_cost, session_cwd, params);
    Ok(AgentResponse::LoadSessionResponse(resp))
}

fn spawn_session(
    params: &AcpParams,
    cwd: PathBuf,
    session_id: Option<SessionRef>,
    history: Vec<Message>,
    mcp_handle: Option<McpHandle>,
    elicitation: bool,
) -> InteractiveHandle {
    headless::spawn_interactive(InteractiveParams {
        model: params.model.clone(),
        config: params.config.clone(),
        permissions_config: params.permissions_config.clone(),
        timeouts: params.timeouts,
        prompt_slots: Arc::clone(&params.prompt_slots),
        excluded_tools: Vec::new(),
        mcp_handle,
        initial_wd: cwd,
        session_id,
        modes: Arc::clone(&params.modes),
        initial_history: history,
        yolo: params.yolo,
        system_prompt_override: params.system_prompt_override.clone(),
        append_system_prompt: params.append_system_prompt.clone(),
        workflow: false,
        model_policy: Arc::clone(&params.model_policy),
        question_mode: if elicitation {
            QuestionMode::Elicitation
        } else {
            QuestionMode::Headless
        },
        plugin_rules: Arc::clone(&params.plugin_rules),
        local_tools: Default::default(),
    })
}

/// Servers the client injects on `session/new` and `session/load`. A transport we
/// cannot speak is dropped like a broken `mcp.toml` entry: losing one server beats
/// losing the session.
fn injected_servers(servers: &[McpServer]) -> Vec<(String, RawTransport)> {
    servers
        .iter()
        .filter_map(|server| match server {
            McpServer::Http(http) => Some((
                server_name(&http.name),
                RawTransport::Http(RawHttpFields {
                    url: http.url.clone(),
                    headers: pairs(&http.headers, |h| (&h.name, &h.value)),
                    oauth: None,
                }),
            )),
            McpServer::Stdio(stdio) => Some((
                server_name(&stdio.name),
                RawTransport::Stdio(RawStdioFields {
                    command: iter::once(stdio.command.to_string_lossy().into_owned())
                        .chain(stdio.args.iter().cloned())
                        .collect(),
                    environment: pairs(&stdio.env, |e| (&e.name, &e.value)),
                }),
            )),
            _ => {
                warn!("ignoring injected MCP server, only http and stdio are supported");
                None
            }
        })
        .collect()
}

/// Clients name their servers freely, makima names them like `mcp.toml` does.
fn server_name(name: &str) -> String {
    name.chars()
        .take(MAX_SERVER_NAME_LEN)
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '-' })
        .collect()
}

fn pairs<T>(items: &[T], split: impl Fn(&T) -> (&String, &String)) -> HashMap<String, String> {
    items
        .iter()
        .map(|item| {
            let (name, value) = split(item);
            (name.clone(), value.clone())
        })
        .collect()
}

/// MCP is per session: the client picks the cwd and may inject its own servers.
/// Returns as soon as the config is read, the first prompt waits for the tools.
async fn start_mcp(cwd: &Path, servers: &[McpServer], params: &AcpParams) -> Option<McpHandle> {
    let (handle, errors) = mcp::start_with_extra_and_commands(
        cwd,
        injected_servers(servers),
        params.command_registry.clone(),
    )
    .await;
    if !errors.is_empty() {
        warn!(%errors, "MCP config errors");
    }
    handle
}

/// Stop the old session before the next one starts, so two generations of the
/// same MCP servers never fight over a port or a lock file.
async fn close_session(srv: &mut Server) {
    let Some(mut state) = srv.session.take() else {
        return;
    };
    // The event pump dies with the session, so the prompt it owed an answer to
    // has to be answered here or the client waits on it forever.
    if let Some(id) = state.pending.lock().unwrap().prompt.take() {
        let resp = PromptResponse::new(StopReason::Cancelled);
        send(
            &srv.out_tx,
            Response::new(id, Ok(AgentResponse::PromptResponse(resp))),
        );
    }
    state.command_projection_task.cancel().await;
    state.handle.task.cancel().await;
    if let Some(mcp) = state.mcp {
        mcp.shutdown().await;
    }
    if let Some(lock) = state.lock.take() {
        lock.shutdown();
    }
}

fn install_session(
    srv: &mut Server,
    handle: InteractiveHandle,
    mcp: Option<McpHandle>,
    current_model: String,
    initial_cost: Option<f64>,
    cwd: PathBuf,
    params: &AcpParams,
) {
    let pending = PendingState::default();
    start_event_pump(
        handle.event_rx.clone(),
        handle.session_id.clone(),
        srv.out_tx.clone(),
        Arc::clone(&pending),
        srv.elicitation,
        handle.answer_tx.clone(),
        cwd.clone(),
        maki_storage::paths::home(),
        initial_cost,
    );
    let command_registry = params.command_registry.clone();
    let command_state = Arc::new(maki_agent::command::SessionCommandState::new(
        current_model,
        srv.model_specs
            .iter()
            .map(|spec| Arc::from(spec.as_str()))
            .collect::<Vec<_>>()
            .into(),
        cwd.clone(),
        RESTORED_FAST,
        false,
    ));
    let command_target = command_registry.bind_target(
        maki_agent::command::portable_capabilities(),
        Arc::new(maki_agent::command::SessionCommandHost::new(
            Arc::clone(&params.model_policy),
            handle.model_tx.clone(),
            handle.control_tx.clone(),
            Arc::clone(&command_state),
            Arc::clone(&handle.permissions),
        )),
    );
    let session_id = SessionId::from(handle.session_id.to_string());
    let commands = command_registry
        .presented_commands(&command_target)
        .unwrap_or_default();
    emit_available_commands(&srv.out_tx, &session_id, &commands);
    let command_projection_task = watch_available_commands(
        srv.out_tx.clone(),
        session_id,
        command_registry.clone(),
        command_target.clone(),
    );
    // Claim-if-free heartbeat: beats every interval while the session is
    // open, so the lock goes stale only when this process stops beating. A
    // dedicated std thread keeps the periodic file I/O off the smol executor
    // and, being joinable, lets the drop in `SessionLock` prove no beat is in
    // flight when the lock releases.
    let lock = match maki_storage::StateDir::resolve().and_then(|s| s.ensure_subdir(SESSIONS_DIR)) {
        Ok(dir) => {
            let id = handle.session_id.id();
            if matches!(
                session_lock::heartbeat(&dir, &id),
                Ok(session_lock::LockBeat::Lost)
            ) {
                warn!(session_id = %id, "session lock claim failed: open elsewhere");
                return;
            }
            let (stop_tx, stop_rx) = flume::bounded(1);
            let beat_dir = dir.clone();
            let thread = std::thread::spawn(move || {
                loop {
                    if stop_rx
                        .recv_timeout(session_lock::HEARTBEAT_INTERVAL)
                        .is_ok()
                    {
                        return;
                    }
                    let _ = session_lock::heartbeat(&beat_dir, &id);
                }
            });
            Some(SessionLock {
                dir,
                id,
                stop_tx,
                thread: Some(thread),
            })
        }
        Err(e) => {
            warn!(error = %e, "session lock unavailable, continuing unlocked");
            None
        }
    };
    srv.session = Some(SessionState {
        handle,
        mcp,
        current_mode: AgentMode::Build,
        command_state,
        pending,
        command_registry,
        command_target,
        command_projection_task,
        lock,
    });
}

fn available_commands(commands: &[PresentedCommand]) -> Vec<AvailableCommand> {
    commands
        .iter()
        .map(|command| {
            let mut available = AvailableCommand::new(
                command.name.trim_start_matches('/'),
                command.description.to_string(),
            );
            if let Some(hint) = &command.argument_hint {
                available = available.input(AvailableCommandInput::Unstructured(
                    UnstructuredCommandInput::new(hint.to_string()),
                ));
            }
            available
        })
        .collect()
}

fn emit_available_commands(
    out_tx: &Sender<Value>,
    session_id: &SessionId,
    commands: &[PresentedCommand],
) {
    session_update(
        out_tx,
        session_id,
        SessionUpdate::AvailableCommandsUpdate(AvailableCommandsUpdate::new(available_commands(
            commands,
        ))),
    );
}

fn watch_available_commands(
    out_tx: Sender<Value>,
    session_id: SessionId,
    registry: maki_commands::CommandRegistry,
    target: TargetHandle,
) -> smol::Task<()> {
    let subscription = registry.subscribe();
    smol::spawn(async move {
        let mut generation = subscription.generation();
        loop {
            generation = subscription.changed(generation).await;
            let Ok(commands) = registry.presented_commands(&target) else {
                return;
            };
            emit_available_commands(&out_tx, &session_id, &commands);
        }
    })
}

#[derive(Debug)]
struct Restored {
    history: Vec<Message>,
    cwd: Option<PathBuf>,
    usage: TokenUsage,
    by_model: HashMap<String, StoredTokenUsage>,
    model: String,
}

fn load_history(session_id: MakiId) -> Result<Restored, AcpError> {
    let storage = maki_storage::StateDir::resolve()
        .map_err(|e| AcpError::internal_error().data(json_str(&e)))?;
    load_history_from(&storage, session_id)
}

fn load_history_from(
    storage: &maki_storage::StateDir,
    session_id: MakiId,
) -> Result<Restored, AcpError> {
    let session: maki_storage::sessions::Session<
        Message,
        maki_providers::TokenUsage,
        maki_agent::ToolOutput,
    > = maki_storage::sessions::Session::load(session_id, storage).map_err(|e| {
        AcpError::resource_not_found(Some(format!("session/{session_id}"))).data(json_str(&e))
    })?;
    let sessions_dir = storage
        .ensure_subdir(SESSIONS_DIR)
        .map_err(|e| AcpError::internal_error().data(json_str(&e)))?;
    if session_lock::open_elsewhere(&sessions_dir, &session_id) {
        return Err(AcpError::internal_error().data(json_str(&session_lock::OPEN_ELSEWHERE_MSG)));
    }
    let cwd = Path::new(&session.cwd)
        .is_absolute()
        .then(|| PathBuf::from(&session.cwd));
    let model = session.model.clone();
    let usage = session.token_usage;
    let by_model = session.usage_by_model().clone();
    let history = session.take_messages();
    Ok(Restored {
        history,
        cwd,
        usage,
        by_model,
        model,
    })
}

async fn handle_prompt(srv: &mut Server, raw: &Value, id: &RequestId) -> Result<(), AcpError> {
    let req: PromptRequest = parse_params(raw)?;
    let session = srv.session.as_ref().ok_or_else(no_session)?;
    let content = extract_prompt_content(&req.prompt)?;
    let dispatch = session
        .command_registry
        .dispatch_input(&session.command_target, content)
        .await;
    match dispatch {
        InputDispatch::LiteralInput(content) => send_command_turn(
            session,
            id,
            AgentTurn {
                content,
                prompt: None,
            },
        ),
        InputDispatch::Dispatched(CommandOutcome::Completed) => {
            respond_prompt(&srv.out_tx, id.clone(), StopReason::EndTurn);
            Ok(())
        }
        InputDispatch::Dispatched(CommandOutcome::AgentTurn(turn)) => {
            send_command_turn(session, id, turn)
        }
        InputDispatch::Dispatched(CommandOutcome::Failed(error)) => Err(command_error(error)),
    }
}

fn send_command_turn(
    session: &SessionState,
    id: &RequestId,
    turn: AgentTurn,
) -> Result<(), AcpError> {
    let prompt = turn.prompt.map(|prompt| maki_agent::McpPromptRef {
        qualified_name: prompt.qualified_name.to_string(),
        arguments: prompt
            .arguments
            .iter()
            .map(|(name, value)| (name.to_string(), value.to_string()))
            .collect(),
    });
    let images = turn
        .content
        .attachments
        .iter()
        .map(|attachment| ImageSource {
            media_type: image_media_type(&attachment.media_type),
            data: Arc::clone(&attachment.data),
        })
        .collect();
    send_agent_input(
        session,
        id,
        agent_input(
            turn.content.text.to_string(),
            images,
            session.current_mode.clone(),
            session.command_state.fast(),
            session.command_state.workflow(),
            prompt,
        ),
    )
}

fn agent_input(
    message: String,
    images: Vec<ImageSource>,
    mode: AgentMode,
    fast: bool,
    workflow: bool,
    prompt: Option<maki_agent::McpPromptRef>,
) -> AgentInput {
    AgentInput {
        message,
        mode,
        images,
        preamble: Vec::new(),
        thinking: Default::default(),
        fast,
        workflow,
        prompt: prompt.map(Box::new),
    }
}

fn send_agent_input(
    session: &SessionState,
    id: &RequestId,
    input: AgentInput,
) -> Result<(), AcpError> {
    session
        .handle
        .input_tx
        .send(input)
        .map_err(|_| AcpError::new(-32603, "session ended"))?;
    session.pending.lock().unwrap().prompt = Some(id.clone());
    Ok(())
}

fn command_error(error: maki_commands::CommandError) -> AcpError {
    AcpError::new(-32602, error.to_string())
}

fn respond_prompt(out_tx: &Sender<Value>, id: RequestId, reason: StopReason) {
    send(
        out_tx,
        Response::new(
            id,
            Ok(AgentResponse::PromptResponse(PromptResponse::new(reason))),
        ),
    );
}

fn handle_set_mode(srv: &mut Server, raw: &Value) -> Result<AgentResponse, AcpError> {
    let req: SetSessionModeRequest = parse_params(raw)?;
    let mode_str = req.mode_id.0.to_string();
    let new_mode = methods::mode_id_to_agent_mode(&mode_str, &srv.modes)
        .ok_or_else(|| AcpError::new(-32602, format!("unknown mode: {mode_str}")))?;

    let session = srv.session.as_mut().ok_or_else(no_session)?;
    session.current_mode = new_mode;

    let sid = SessionId::from(session.handle.session_id.to_string());
    session_update(
        &srv.out_tx,
        &sid,
        SessionUpdate::CurrentModeUpdate(CurrentModeUpdate::new(SessionModeId::from(mode_str))),
    );
    Ok(AgentResponse::SetSessionModeResponse(
        SetSessionModeResponse::new(),
    ))
}

fn handle_set_config(srv: &mut Server, raw: &Value) -> Result<AgentResponse, AcpError> {
    let req: SetSessionConfigOptionRequest = parse_params(raw)?;
    if req.config_id.0.as_ref() != methods::MODEL_CONFIG_ID {
        let detail = format!("unknown config option: {}", req.config_id);
        return Err(AcpError::invalid_params().data(json_str(&detail)));
    }

    let spec = req.value.0.to_string();
    if !srv.model_policy.allows(&spec) {
        return Err(AcpError::invalid_params().data(json_str(&"model is not allowed by policy")));
    }
    let model =
        Model::from_spec(&spec).map_err(|e| AcpError::invalid_params().data(json_str(&e)))?;

    let session = srv.session.as_mut().ok_or_else(no_session)?;
    session
        .handle
        .model_tx
        .send(model)
        .map_err(|_| AcpError::new(-32603, "session ended"))?;
    *session
        .command_state
        .current_model
        .lock()
        .unwrap_or_else(|error| error.into_inner()) = spec.clone();

    Ok(AgentResponse::SetSessionConfigOptionResponse(
        SetSessionConfigOptionResponse::new(vec![methods::model_config_option(
            &spec,
            &srv.model_specs,
        )]),
    ))
}

fn handle_notification(srv: &Server, method: &str) {
    match method {
        "session/cancel" => {
            if let Some(session) = &srv.session {
                // Any answer still in flight belongs to the cancelled turn, so
                // forget its id and let it be dropped on arrival.
                let mut pending = session.pending.lock().unwrap();
                pending.permission = None;
                pending.elicitation = None;
                let _ = session.handle.cancel_tx.try_send(());
            }
        }
        _ => debug!(method, "unknown notification"),
    }
}

fn handle_incoming_response(srv: &Server, raw: &Value) {
    let Some(session) = &srv.session else { return };
    let Some(id) = raw.get("id").and_then(Value::as_i64) else {
        return;
    };
    let answer = {
        let mut pending = session.pending.lock().unwrap();
        if pending
            .elicitation
            .take_if(|pending| *pending == id)
            .is_some()
        {
            Some(elicitation_answer(raw))
        } else if pending
            .permission
            .take_if(|pending| *pending == id)
            .is_some()
        {
            Some(permission_answer(raw).encode())
        } else {
            warn!(id, "response for an unknown request id");
            None
        }
    };
    if let Some(answer) = answer {
        let _ = session.handle.answer_tx.send(answer);
    }
}

/// A response we cannot read still has to answer the tool, or it waits on an
/// elicitation that will never come.
fn elicitation_answer(raw: &Value) -> String {
    match raw
        .get("result")
        .map(|result| serde_json::from_value::<CreateElicitationResponse>(result.clone()))
    {
        Some(Ok(resp)) => crate::elicitation::response_payload(resp),
        _ => serde_json::json!({ "dismissed": true }).to_string(),
    }
}

/// A response we cannot read still has to answer the agent, or the tool waits
/// on a permission that will never come.
fn permission_answer(raw: &Value) -> PermissionAnswer {
    match raw
        .get("result")
        .map(|result| serde_json::from_value::<RequestPermissionResponse>(result.clone()))
    {
        Some(Ok(resp)) => permissions::outcome_to_answer(&resp.outcome),
        _ => PermissionAnswer::Deny,
    }
}

const UNSUPPORTED_CONTENT_BLOCK: &str = "unsupported content block in command prompt";

fn extract_prompt_content(blocks: &[ContentBlock]) -> Result<CommandContent, AcpError> {
    let mut text = String::new();
    let mut attachments = Vec::new();

    for block in blocks {
        match block {
            ContentBlock::Text(TextContent { text: part, .. }) => append(&mut text, part),
            ContentBlock::Image(ImageContent {
                data, mime_type, ..
            }) => attachments.push(CommandAttachment {
                media_type: Arc::from(mime_type.as_str()),
                data: Arc::from(data.as_str()),
            }),
            ContentBlock::Resource(res) => match &res.resource {
                EmbeddedResourceResource::TextResourceContents(resource) => {
                    append(
                        &mut text,
                        &format!("--- {} ---\\n{}", resource.uri, resource.text),
                    );
                }
                EmbeddedResourceResource::BlobResourceContents(_) | _ => {
                    return Err(
                        AcpError::invalid_params().data(json_str(&UNSUPPORTED_CONTENT_BLOCK))
                    );
                }
            },
            ContentBlock::ResourceLink(resource) => {
                append(&mut text, &format!("[Resource: {}]", resource.uri));
            }
            ContentBlock::Audio(_) => {
                return Err(AcpError::invalid_params().data(json_str(&UNSUPPORTED_CONTENT_BLOCK)));
            }
            _ => return Err(AcpError::invalid_params().data(json_str(&UNSUPPORTED_CONTENT_BLOCK))),
        }
    }

    Ok(CommandContent {
        text: Arc::from(text),
        attachments: Arc::from(attachments),
    })
}

fn append(text: &mut String, part: &str) {
    if !text.is_empty() {
        text.push('\n');
    }
    text.push_str(part);
}

fn image_media_type(mime: &str) -> ImageMediaType {
    match mime {
        "image/png" => ImageMediaType::Png,
        "image/gif" => ImageMediaType::Gif,
        "image/webp" => ImageMediaType::Webp,
        _ => ImageMediaType::Jpeg,
    }
}

#[allow(clippy::too_many_arguments)]
fn start_event_pump(
    event_rx: Receiver<Envelope>,
    session_id: SessionRef,
    out_tx: Sender<Value>,
    pending: PendingState,
    elicitation: bool,
    answer_tx: Sender<String>,
    cwd: PathBuf,
    home: Option<PathBuf>,
    initial_cost: Option<f64>,
) {
    smol::spawn(async move {
        let sid = SessionId::from(session_id.to_string());
        let mut cost_total = initial_cost;

        while let Ok(Envelope {
            event, subagent, ..
        }) = event_rx.recv_async().await
        {
            if let AgentEvent::TurnComplete(tc) = &event {
                add_cost(&mut cost_total, tc.cost);
            }
            if subagent.is_some() {
                continue;
            }

            let update = match event {
                AgentEvent::TextDelta { text } => translate::text_delta(&text),
                AgentEvent::ThinkingDelta { text } => translate::thinking_delta(&text),
                AgentEvent::ToolPending { id, name } => translate::tool_pending(&id, &name),
                AgentEvent::ToolStart(event) => {
                    translate::tool_start(&event, &cwd, home.as_deref())
                }
                AgentEvent::ToolOutput { id, content } => translate::tool_output(&id, &content),
                AgentEvent::ToolDone(event) => translate::tool_done(&event, &cwd, home.as_deref()),
                AgentEvent::TurnComplete(event) => translate::usage_update(&event, cost_total),
                AgentEvent::PermissionRequest { id, tool, scopes } => {
                    let fields =
                        ToolCallUpdateFields::new().title(format!("{tool}: {}", scopes.join(", ")));
                    let request =
                        AgentRequest::RequestPermissionRequest(RequestPermissionRequest::new(
                            sid.clone(),
                            ToolCallUpdate::new(ToolCallId::from(id), fields),
                            permissions::permission_options(),
                        ));
                    let request_id = NEXT_OUTGOING_REQUEST_ID.fetch_add(1, Ordering::Relaxed);
                    pending.lock().unwrap().permission = Some(request_id);
                    send(
                        &out_tx,
                        Request {
                            id: RequestId::Number(request_id),
                            method: Arc::from(request.method()),
                            params: Some(request),
                        },
                    );
                    continue;
                }
                AgentEvent::Question { id, questions } => {
                    if elicitation
                        && let Some(request) = crate::elicitation::build_form(&questions, &sid, &id)
                    {
                        let request = AgentRequest::CreateElicitationRequest(request);
                        let request_id = NEXT_OUTGOING_REQUEST_ID.fetch_add(1, Ordering::Relaxed);
                        pending.lock().unwrap().elicitation = Some(request_id);
                        send(
                            &out_tx,
                            Request {
                                id: RequestId::Number(request_id),
                                method: Arc::from(request.method()),
                                params: Some(request),
                            },
                        );
                    } else {
                        // A question we cannot render as a form (non-array
                        // input, or a host without elicitation) still has a
                        // tool waiting on `user_response_rx`; dismiss it so
                        // the turn fails gracefully instead of hanging.
                        let _ =
                            answer_tx.send(serde_json::json!({ "dismissed": true }).to_string());
                    }
                    continue;
                }
                AgentEvent::Done { reason, .. } => {
                    if let Some(id) = pending.lock().unwrap().prompt.take() {
                        let resp = PromptResponse::new(translate::map_done_reason(reason));
                        send(
                            &out_tx,
                            Response::new(id, Ok(AgentResponse::PromptResponse(resp))),
                        );
                    }
                    continue;
                }
                AgentEvent::Error { message } => {
                    if let Some(id) = pending.lock().unwrap().prompt.take() {
                        let error = AcpError::internal_error().data(Value::String(message));
                        send(&out_tx, Response::<AgentResponse>::new(id, Err(error)));
                    }
                    continue;
                }
                _ => continue,
            };
            session_update(&out_tx, &sid, update);
        }
    })
    .detach();
}

fn send(out_tx: &Sender<Value>, msg: impl Serialize) {
    if let Ok(json) = serde_json::to_value(JsonRpcMessage::wrap(msg)) {
        let _ = out_tx.send(json);
    }
}

fn session_update(out_tx: &Sender<Value>, sid: &SessionId, update: SessionUpdate) {
    let notification =
        AgentNotification::SessionNotification(SessionNotification::new(sid.clone(), update));
    send(
        out_tx,
        Notification {
            method: Arc::from("session/update"),
            params: Some(notification),
        },
    );
}

fn no_session() -> AcpError {
    AcpError::new(-32600, "no active session")
}

fn parse_params<T: serde::de::DeserializeOwned>(raw: &Value) -> Result<T, AcpError> {
    serde_json::from_value(raw.get("params").cloned().unwrap_or(Value::Null))
        .map_err(|e| AcpError::invalid_params().data(json_str(&e)))
}

fn json_str(e: &impl std::fmt::Display) -> Value {
    Value::String(e.to_string())
}

#[cfg(test)]
mod tests {
    use maki_agent::permissions::PermissionManager;
    use maki_commands::TargetCapabilities;
    use maki_providers::{ContentBlock as MsgBlock, Role, TokenUsage};
    use maki_storage::StateDir;
    use maki_storage::sessions::Session;
    use tempfile::TempDir;
    use test_case::test_case;

    use super::*;

    const ANSWERED_ID: i64 = 1001;
    const UNKNOWN_ID: i64 = 1002;
    const DISCOVERED_SPEC: &str = "openrouter/discovered-model";
    const OFFLINE_SPEC: &str = "openai/gpt-5";

    fn test_registry(
        custom_commands: &[maki_agent::command::CustomCommand],
    ) -> maki_commands::CommandRegistry {
        let registry = maki_commands::CommandRegistry::new();
        let commands = maki_agent::command::StandardCommands::register(
            &registry,
            custom_commands,
            maki_agent::command::StandardCompletions::default(),
        )
        .unwrap();
        std::mem::forget(commands);
        registry
    }

    fn test_target(
        registry: &maki_commands::CommandRegistry,
        model_tx: Sender<Model>,
        control_tx: Sender<maki_agent::headless::InteractiveControl>,
        command_state: Arc<maki_agent::command::SessionCommandState>,
        permissions: Arc<PermissionManager>,
    ) -> TargetHandle {
        registry.bind_target(
            maki_agent::command::portable_capabilities(),
            Arc::new(maki_agent::command::SessionCommandHost::new(
                Arc::new(maki_config::ModelPolicy::default()),
                model_tx,
                control_tx,
                command_state,
                permissions,
            )),
        )
    }

    fn allow_once(id: i64) -> Value {
        serde_json::json!({
            "id": id,
            "result": { "outcome": { "outcome": "selected", "optionId": "allow_once" } },
        })
    }

    #[test_case(allow_once(ANSWERED_ID), PermissionAnswer::AllowOnce ; "selected_option")]
    #[test_case(serde_json::json!({ "id": ANSWERED_ID, "result": { "outcome": { "outcome": "cancelled" } } }), PermissionAnswer::Deny ; "cancelled_outcome")]
    #[test_case(serde_json::json!({ "id": ANSWERED_ID, "result": { "nonsense": true } }), PermissionAnswer::Deny ; "unparsable_result")]
    #[test_case(serde_json::json!({ "id": ANSWERED_ID, "error": { "code": -32603 } }), PermissionAnswer::Deny ; "jsonrpc_error")]
    fn permission_answer_maps_response(raw: Value, expected: PermissionAnswer) {
        assert_eq!(permission_answer(&raw), expected);
    }

    fn server_awaiting_answer() -> (
        Server,
        Receiver<String>,
        Receiver<Value>,
        Receiver<AgentInput>,
    ) {
        let (answer_tx, answer_rx) = flume::unbounded();
        let (out_tx, out_rx) = flume::unbounded();
        let (input_tx, input_rx) = flume::unbounded();
        let handle = InteractiveHandle {
            event_rx: flume::unbounded().1,
            tool_names: Vec::new(),
            input_tx,
            answer_tx,
            cancel_tx: flume::unbounded().0,
            model_tx: flume::unbounded().0,
            control_tx: flume::unbounded().0,
            session_id: SessionRef::from(MakiId::generate()),
            permissions: Arc::new(PermissionManager::new(
                maki_config::PermissionsConfig::default(),
                PathBuf::from("/project"),
                Arc::default(),
            )),
            task: smol::spawn(async {}),
        };
        let command_registry = test_registry(&[]);
        let command_state = Arc::new(maki_agent::command::SessionCommandState::new(
            String::new(),
            Arc::from([]),
            PathBuf::from("/project"),
            false,
            false,
        ));
        let command_target = test_target(
            &command_registry,
            handle.model_tx.clone(),
            handle.control_tx.clone(),
            Arc::clone(&command_state),
            Arc::clone(&handle.permissions),
        );
        let server = Server {
            out_tx,
            modes: Arc::new(maki_agent::ModeRegistry::builtin()),
            model_specs: Vec::new(),
            model_policy: Arc::new(maki_config::ModelPolicy::default()),
            session: Some(SessionState {
                handle,
                mcp: None,
                current_mode: AgentMode::Build,
                command_state,
                pending: Arc::new(Mutex::new(Pending {
                    permission: Some(ANSWERED_ID),
                    ..Default::default()
                })),
                command_registry,
                command_target,
                command_projection_task: smol::spawn(async {}),
                lock: None,
            }),
            elicitation: false,
        };
        (server, answer_rx, out_rx, input_rx)
    }

    #[test]
    fn only_the_outstanding_request_id_is_answered() {
        let (srv, answer_rx, ..) = server_awaiting_answer();

        handle_incoming_response(&srv, &allow_once(UNKNOWN_ID));
        assert!(answer_rx.is_empty(), "an unknown id is dropped");

        handle_incoming_response(&srv, &allow_once(ANSWERED_ID));
        assert_eq!(
            answer_rx.try_recv().ok(),
            Some(PermissionAnswer::AllowOnce.encode())
        );

        handle_incoming_response(&srv, &allow_once(ANSWERED_ID));
        assert!(
            answer_rx.is_empty(),
            "a replayed answer cannot land on the next request"
        );
    }

    #[test]
    fn cancel_drops_the_outstanding_permission_request() {
        let (srv, answer_rx, ..) = server_awaiting_answer();
        handle_notification(&srv, "session/cancel");

        handle_incoming_response(&srv, &allow_once(ANSWERED_ID));
        assert!(answer_rx.is_empty(), "the cancelled turn owns that answer");
    }

    #[test]
    fn elicitation_answer_is_routed_by_id_and_decoded() {
        let (srv, answer_rx, ..) = server_awaiting_answer();
        {
            let mut pending = srv.session.as_ref().unwrap().pending.lock().unwrap();
            pending.permission = None;
            pending.elicitation = Some(ANSWERED_ID);
        }

        handle_incoming_response(
            &srv,
            &serde_json::json!({
                "id": ANSWERED_ID,
                "result": {
                    "action": "accept",
                    "content": { "q1": "a", "q2": ["x", "y"] },
                },
            }),
        );
        assert_eq!(
            answer_rx.try_recv().ok(),
            Some(r#"{"answers":[["a"],["x","y"]]}"#.to_string())
        );

        handle_incoming_response(&srv, &allow_once(ANSWERED_ID));
        assert!(
            answer_rx.is_empty(),
            "a replayed id cannot land on the next request"
        );
    }

    #[test]
    fn unparsable_elicitation_result_dismisses_instead_of_hanging() {
        let (srv, answer_rx, ..) = server_awaiting_answer();
        srv.session
            .as_ref()
            .unwrap()
            .pending
            .lock()
            .unwrap()
            .elicitation = Some(ANSWERED_ID);

        handle_incoming_response(
            &srv,
            &serde_json::json!({ "id": ANSWERED_ID, "result": { "nonsense": true } }),
        );
        assert_eq!(
            answer_rx.try_recv().ok(),
            Some(r#"{"dismissed":true}"#.to_string()),
            "a broken answer must still unblock the waiting tool"
        );
    }

    #[test]
    fn malformed_question_in_elicitation_mode_unblocks_the_tool() {
        let (event_tx, event_rx) = flume::unbounded::<Envelope>();
        let (out_tx, out_rx) = flume::unbounded::<Value>();
        let (answer_tx, answer_rx) = flume::unbounded::<String>();
        let pending = Arc::new(Mutex::new(Pending::default()));
        let session_id = SessionRef::from(MakiId::generate());

        start_event_pump(
            event_rx,
            session_id,
            out_tx,
            Arc::clone(&pending),
            true,
            answer_tx,
            PathBuf::from("."),
            maki_storage::paths::home(),
            None,
        );

        event_tx
            .send(Envelope {
                event: AgentEvent::Question {
                    id: "t1".to_string(),
                    questions: serde_json::json!({ "not": "an array" }),
                },
                subagent: None,
                run_id: 0,
            })
            .unwrap();

        smol::block_on(async {
            let deadline = std::time::Instant::now() + std::time::Duration::from_millis(200);
            loop {
                if let Ok(answer) = answer_rx.try_recv() {
                    assert_eq!(
                        answer, r#"{"dismissed":true}"#,
                        "the pump must dismiss a question it cannot render, not drop it",
                    );
                    return;
                }
                assert!(
                    out_rx.try_recv().is_err(),
                    "no host request should be sent for an unrenderable question",
                );
                assert!(
                    std::time::Instant::now() < deadline,
                    "the pump silently dropped a malformed question and left the tool hanging",
                );
                smol::Timer::after(std::time::Duration::from_millis(5)).await;
            }
        });
    }

    #[test]
    fn available_command_projection_tracks_registry_generation() {
        let registry = test_registry(&[]);
        let target = test_target(
            &registry,
            flume::unbounded().0,
            flume::unbounded().0,
            Arc::new(maki_agent::command::SessionCommandState::new(
                String::new(),
                Arc::from([]),
                PathBuf::from("/project"),
                false,
                false,
            )),
            Arc::new(PermissionManager::new(
                maki_config::PermissionsConfig::default(),
                PathBuf::from("/project"),
                Arc::default(),
            )),
        );
        let initial = registry.snapshot_for(&target).unwrap();
        let commands = available_commands(&registry.presented_commands(&target).unwrap());
        assert_eq!(
            commands
                .iter()
                .map(|command| command.name.as_str())
                .collect::<Vec<_>>(),
            [
                "compact", "new", "clear", "model", "cd", "btw", "yolo", "fast", "workflow"
            ]
        );

        let producer = registry.create_producer(maki_commands::ProducerPrecedence::Plugin);
        producer
            .replace(vec![maki_commands::Registration {
                spec: maki_commands::CommandSpec {
                    name: Arc::from("/review"),
                    aliases: Arc::from([]),
                    arguments: maki_commands::ArgumentArity::unbounded(0),
                    docs: maki_commands::CommandDocs {
                        summary: Arc::from("Review code"),
                        argument_hint: Some(Arc::from("<path>")),
                    },
                    required_capabilities: TargetCapabilities::default(),
                },
                behavior: Arc::new(CompletedCommand),
                completion: None,
            }])
            .unwrap();
        let updated = registry.snapshot_for(&target).unwrap();
        assert!(updated.generation() > initial.generation());
        let commands = available_commands(&registry.presented_commands(&target).unwrap());
        let review = commands
            .iter()
            .find(|command| command.name == "review")
            .unwrap();
        assert_eq!(review.description, "Review code");
        assert!(matches!(
            review.input,
            Some(AvailableCommandInput::Unstructured(ref input)) if input.hint == "<path>"
        ));
    }

    #[test]
    fn discovered_models_are_pushed_to_the_client() {
        let (mut srv, _, out_rx, _) = server_awaiting_answer();
        srv.model_specs = vec![OFFLINE_SPEC.to_owned()];
        refresh_models(&mut srv, vec![DISCOVERED_SPEC.to_owned()]);
        let update = out_rx.try_recv().expect("the fuller list is announced");
        let options = &update["params"]["update"]["configOptions"][0]["options"];
        let selectable: Vec<&str> = options
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|o| o["value"].as_str())
            .collect();
        assert!(selectable.contains(&OFFLINE_SPEC));
        assert!(selectable.contains(&DISCOVERED_SPEC));
        refresh_models(&mut srv, vec![DISCOVERED_SPEC.to_owned()]);
        assert!(out_rx.is_empty());
    }

    fn prompt_request(text: &str, image: bool) -> Value {
        let mut prompt = vec![serde_json::json!({ "type": "text", "text": text })];
        if image {
            prompt.push(serde_json::json!({
                "type": "image",
                "data": "aGVsbG8=",
                "mimeType": "image/png",
            }));
        }
        serde_json::json!({
            "params": {
                "sessionId": MakiId::generate().to_string(),
                "prompt": prompt,
            }
        })
    }

    fn install_registry(srv: &mut Server, registry: maki_commands::CommandRegistry) {
        let session = srv.session.as_mut().unwrap();
        session.command_target = test_target(
            &registry,
            session.handle.model_tx.clone(),
            session.handle.control_tx.clone(),
            Arc::clone(&session.command_state),
            Arc::clone(&session.handle.permissions),
        );
        session.command_registry = registry;
    }

    #[test]
    fn unknown_slash_prompt_is_sent_to_agent_literal() {
        let (mut srv, _, _, input_rx) = server_awaiting_answer();
        smol::block_on(handle_prompt(
            &mut srv,
            &prompt_request("/does-not-exist value", false),
            &RequestId::Number(1),
        ))
        .unwrap();
        assert_eq!(
            input_rx.try_recv().unwrap().message,
            "/does-not-exist value"
        );
    }

    #[test]
    fn custom_slash_prompt_dispatches_rendered_agent_input() {
        let (mut srv, _, _, input_rx) = server_awaiting_answer();
        let registry = test_registry(&[maki_agent::command::CustomCommand {
            name: "review".into(),
            description: "review code".into(),
            content: "Review $ARGUMENTS".into(),
            scope: maki_agent::command::CommandScope::Project,
            accepts_args: true,
            argument_hint: None,
        }]);
        install_registry(&mut srv, registry);

        smol::block_on(handle_prompt(
            &mut srv,
            &prompt_request("/project:review src", false),
            &RequestId::Number(2),
        ))
        .unwrap();
        assert_eq!(input_rx.try_recv().unwrap().message, "Review src");
    }

    #[test]
    fn unavailable_interactive_command_is_sent_to_agent_literal() {
        let (mut srv, _, _, input_rx) = server_awaiting_answer();

        smol::block_on(handle_prompt(
            &mut srv,
            &prompt_request("/help", true),
            &RequestId::Number(3),
        ))
        .unwrap();
        let input = input_rx.try_recv().unwrap();
        assert_eq!(input.message, "/help");
        assert_eq!(input.images.len(), 1);
    }

    #[test]
    fn portable_bare_model_returns_shared_usage_error() {
        let (mut srv, _, _, input_rx) = server_awaiting_answer();

        let error = smol::block_on(handle_prompt(
            &mut srv,
            &prompt_request("/model", false),
            &RequestId::Number(3),
        ))
        .unwrap_err();

        assert_eq!(error.code, AcpError::invalid_params().code);
        assert_eq!(error.message, "command failed: Usage: /model <model>");
        assert!(input_rx.is_empty());
    }

    #[test]
    fn portable_local_builtin_rejects_non_text_content() {
        let (mut srv, _, _, input_rx) = server_awaiting_answer();

        let error = smol::block_on(handle_prompt(
            &mut srv,
            &prompt_request("/compact", true),
            &RequestId::Number(3),
        ))
        .unwrap_err();

        assert_eq!(error.code, AcpError::invalid_params().code);
        assert!(input_rx.is_empty());
    }

    #[test]
    fn agent_turn_preserves_image_content() {
        let (mut srv, _, _, input_rx) = server_awaiting_answer();
        let registry = test_registry(&[maki_agent::command::CustomCommand {
            name: "review".into(),
            description: "review code".into(),
            content: "Review $ARGUMENTS".into(),
            scope: maki_agent::command::CommandScope::Project,
            accepts_args: true,
            argument_hint: None,
        }]);
        install_registry(&mut srv, registry);

        smol::block_on(handle_prompt(
            &mut srv,
            &prompt_request("/project:review src", true),
            &RequestId::Number(3),
        ))
        .unwrap();
        let input = input_rx.try_recv().unwrap();
        assert_eq!(input.message, "Review src");
        assert_eq!(input.images.len(), 1);
    }

    #[test]
    fn custom_command_with_unsupported_content_has_exact_error() {
        let (mut srv, _, _, _) = server_awaiting_answer();
        let registry = test_registry(&[maki_agent::command::CustomCommand {
            name: "review".into(),
            description: "review code".into(),
            content: "Review $ARGUMENTS".into(),
            scope: maki_agent::command::CommandScope::Project,
            accepts_args: true,
            argument_hint: None,
        }]);
        install_registry(&mut srv, registry);
        let raw = serde_json::json!({
            "params": {
                "sessionId": MakiId::generate().to_string(),
                "prompt": [
                    { "type": "text", "text": "/project:review src" },
                    { "type": "audio", "data": "aGVsbG8=", "mimeType": "audio/wav" }
                ]
            }
        });
        let error =
            smol::block_on(handle_prompt(&mut srv, &raw, &RequestId::Number(5))).unwrap_err();
        assert_eq!(error.code, AcpError::invalid_params().code);
        assert_eq!(
            error.data,
            Some(Value::String(UNSUPPORTED_CONTENT_BLOCK.to_owned()))
        );
    }

    struct CompletedCommand;

    impl maki_commands::CommandBehavior for CompletedCommand {
        fn execute(
            &self,
            invocation: maki_commands::CommandInvocation,
        ) -> maki_commands::CommandFuture<Result<CommandOutcome, maki_commands::CommandError>>
        {
            let _ = invocation;
            Box::pin(async { Ok(CommandOutcome::Completed) })
        }
    }

    #[test]
    fn completed_lua_slash_prompt_returns_without_agent_input() {
        let (mut srv, _, out_rx, input_rx) = server_awaiting_answer();
        let registry = test_registry(&[]);
        let producer = registry.create_producer(maki_commands::ProducerPrecedence::Plugin);
        producer
            .replace(vec![maki_commands::Registration {
                spec: maki_commands::CommandSpec {
                    name: Arc::from("/lua-complete"),
                    aliases: Arc::from([]),
                    arguments: maki_commands::ArgumentArity::NONE,
                    docs: maki_commands::CommandDocs {
                        summary: Arc::from("complete without a turn"),
                        argument_hint: None,
                    },
                    required_capabilities: TargetCapabilities::default(),
                },
                behavior: Arc::new(CompletedCommand),
                completion: None,
            }])
            .unwrap();
        install_registry(&mut srv, registry);

        smol::block_on(handle_prompt(
            &mut srv,
            &prompt_request("/lua-complete", false),
            &RequestId::Number(4),
        ))
        .unwrap();
        assert!(input_rx.is_empty());
        let response = out_rx.try_recv().unwrap();
        assert_eq!(response["result"]["stopReason"], "end_turn");
    }

    #[test]
    fn portable_bare_btw_is_rejected_by_registry() {
        let (mut srv, _, _, input_rx) = server_awaiting_answer();
        let error = smol::block_on(handle_prompt(
            &mut srv,
            &prompt_request("/btw", false),
            &RequestId::Number(4),
        ))
        .unwrap_err();
        assert_eq!(error.code, AcpError::invalid_params().code);
        assert_eq!(
            error.message,
            "invalid arguments for /btw: expected 1 or more"
        );
        assert!(input_rx.is_empty());
    }

    #[test]
    fn portable_agent_turn_builtin_is_forwarded_to_agent() {
        let (mut srv, _, _, input_rx) = server_awaiting_answer();
        smol::block_on(handle_prompt(
            &mut srv,
            &prompt_request("/btw explain this", false),
            &RequestId::Number(4),
        ))
        .unwrap();
        assert_eq!(input_rx.try_recv().unwrap().message, "explain this");
    }

    #[test]
    fn load_history_round_trips_stored_messages() {
        let tmp = TempDir::new().unwrap();
        let dir = StateDir::from_path(tmp.path().to_path_buf());
        let messages = vec![
            Message::user("rename foo to bar".into()),
            Message {
                role: Role::Assistant,
                content: vec![MsgBlock::Text {
                    text: "done".into(),
                }],
                display_text: None,
                ..Default::default()
            },
        ];
        let mut session: Session<Message, TokenUsage, maki_agent::ToolOutput> =
            Session::new("anthropic/test-model", "/project");
        session.replace_messages(messages.clone());
        session.save(&dir).unwrap();

        let id: MakiId = session.id;
        let history = load_history_from(&dir, id).unwrap();
        assert_eq!(
            serde_json::to_value(&history.history).unwrap(),
            serde_json::to_value(&messages).unwrap()
        );
    }

    #[test]
    fn load_missing_session_is_resource_not_found() {
        let tmp = TempDir::new().unwrap();
        let dir = StateDir::from_path(tmp.path().to_path_buf());
        let err = load_history_from(&dir, MakiId::generate()).unwrap_err();
        assert_eq!(err.code, AcpError::resource_not_found(None).code);
    }

    #[test]
    fn load_history_from_rejects_session_open_elsewhere() {
        const FAKE_PID: u32 = u32::MAX - 1;
        let tmp = TempDir::new().unwrap();
        let dir = StateDir::from_path(tmp.path().to_path_buf());
        let mut session: Session<Message, TokenUsage, maki_agent::ToolOutput> =
            Session::new("anthropic/test-model", "/project");
        session.replace_messages(vec![Message::user("hi".into())]);
        session.save(&dir).unwrap();
        let id = session.id;

        let sessions_dir = dir.ensure_subdir(SESSIONS_DIR).unwrap();
        let lock = session_lock::lock_path(&sessions_dir, &id);
        std::fs::write(&lock, FAKE_PID.to_string()).unwrap();
        let err = load_history_from(&dir, id).unwrap_err();
        assert_eq!(err.code, AcpError::internal_error().code);
        assert_eq!(
            err.data,
            Some(Value::String(session_lock::OPEN_ELSEWHERE_MSG.to_owned()))
        );

        std::fs::remove_file(&lock).unwrap();
        assert!(load_history_from(&dir, id).is_ok());
    }

    #[test]
    fn converts_injected_mcp_servers() {
        let raw = serde_json::json!({
            "params": {
                "sessionId": MakiId::generate().to_string(),
                "cwd": "/project",
                "mcpServers": [
                    {
                        "type": "http",
                        "name": "kan.dev/mcp",
                        "url": "http://127.0.0.1:41012",
                        "headers": [{ "name": "Authorization", "value": "Bearer abc" }]
                    },
                    {
                        "name": "local",
                        "command": "/usr/bin/mcp",
                        "args": ["--stdio"],
                        "env": [{ "name": "TOKEN", "value": "t" }]
                    },
                    {
                        "type": "sse",
                        "name": "legacy",
                        "url": "http://127.0.0.1:41013",
                        "headers": []
                    }
                ]
            }
        });

        let req: LoadSessionRequest = parse_params(&raw).unwrap();
        let servers = injected_servers(&req.mcp_servers);
        assert_eq!(servers.len(), 2, "sse is dropped, not converted");

        let (name, RawTransport::Http(http)) = &servers[0] else {
            panic!("expected http transport");
        };
        assert_eq!(name, "kan-dev-mcp", "wire names are coerced to valid ones");
        assert_eq!(http.url, "http://127.0.0.1:41012");
        assert_eq!(
            http.headers.get("Authorization").map(String::as_str),
            Some("Bearer abc")
        );

        let (name, RawTransport::Stdio(stdio)) = &servers[1] else {
            panic!("expected stdio transport");
        };
        assert_eq!(name, "local");
        assert_eq!(stdio.command, ["/usr/bin/mcp", "--stdio"]);
        assert_eq!(
            stdio.environment.get("TOKEN").map(String::as_str),
            Some("t")
        );
    }
}
