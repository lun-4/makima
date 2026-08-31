use std::collections::{BTreeMap, HashMap};
use std::io::Write;
use std::iter;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
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

/// Ids come from here and are never reused, so a late answer for a closed
/// session cannot match a request of the session that replaced it.
static NEXT_OUTGOING_REQUEST_ID: AtomicI64 = AtomicI64::new(FIRST_OUTGOING_REQUEST_ID);
static NEXT_OPERATION_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OperationKind {
    PrimaryTurn,
    IsolatedTurn,
    ManualCompaction,
    #[cfg(test)]
    TestLocal,
}

struct PendingOperation {
    id: u64,
    request_id: RequestId,
    kind: OperationKind,
    cancel: Option<maki_agent::cancel::CancelTrigger>,
    _lease: Option<maki_agent::session_coordinator::SessionLease>,
}

/// What the client still owes us. Only one permission or elicitation can be
/// outstanding: the agent holds the answer channel while it waits for one.
#[derive(Default)]
struct Pending {
    operation: Option<PendingOperation>,
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

struct OptionProjection {
    out_tx: Sender<Value>,
    session_id: SessionId,
    read: maki_agent::session_coordinator::SessionReadHandle,
    command_state: Arc<maki_agent::command::SessionCommandState>,
    permissions: Arc<maki_agent::permissions::PermissionManager>,
    emitted_version: AtomicU64,
}

impl OptionProjection {
    fn emit(&self, snapshot: &maki_agent::session_options::SessionOptionsSnapshot) {
        let mut observed = self.emitted_version.load(Ordering::Acquire);
        while snapshot.version > observed {
            match self.emitted_version.compare_exchange_weak(
                observed,
                snapshot.version,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    apply_committed_options(snapshot, &self.command_state, &self.permissions);
                    emit_config_options(&self.out_tx, &self.session_id, snapshot);
                    return;
                }
                Err(current) => observed = current,
            }
        }
    }

    fn emit_current(&self) {
        self.emit(&self.read.options());
    }
}

struct SessionState {
    handle: InteractiveHandle,
    coordinator: Option<maki_agent::session_coordinator::SessionCoordinatorHandle>,
    mcp: Option<McpHandle>,
    current_mode: AgentMode,
    command_state: Arc<maki_agent::command::SessionCommandState>,
    pending: PendingState,
    command_registry: maki_commands::CommandRegistry,
    command_target: TargetHandle,
    command_projection_task: smol::Task<()>,
    option_projection: Option<Arc<OptionProjection>>,
    option_projection_task: smol::Task<()>,
    lock: Option<SessionLock>,
}

struct SpawnSession {
    model: Model,
    cwd: PathBuf,
    session_id: Option<SessionRef>,
    history: Vec<Message>,
    mcp_handle: Option<McpHandle>,
    elicitation: bool,
    yolo: bool,
    workflow: bool,
}

struct InstallSession<'a> {
    handle: InteractiveHandle,
    mcp: Option<McpHandle>,
    current_model: String,
    history: Vec<Message>,
    initial_cost: Option<f64>,
    cwd: PathBuf,
    fast: bool,
    workflow: bool,
    persisted_options: &'a BTreeMap<String, String>,
}

struct Server {
    out_tx: Sender<Value>,
    model_specs: Vec<String>,
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
            Incoming::Models(batch) => refresh_models(&mut server, batch).await,
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

async fn refresh_models(srv: &mut Server, batch: Vec<String>) {
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
    session
        .command_state
        .set_model_specs(srv.model_specs.clone());
    let Some(coordinator) = &session.coordinator else {
        return;
    };
    match coordinator
        .update_model_values(
            srv.model_specs
                .iter()
                .map(|spec| Arc::from(spec.as_str()))
                .collect(),
        )
        .await
    {
        Ok(_) => {
            if let Some(projection) = &session.option_projection {
                projection.emit_current();
            }
        }
        Err(error) => warn!(%error, "failed to publish discovered models"),
    }
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
            None => handle_notification(server, method, &raw),
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
        "session/set_config_option" => handle_set_config(srv, raw).await,
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
        SpawnSession {
            model: params.model.clone(),
            cwd: req.cwd,
            session_id: None,
            history: Vec::new(),
            mcp_handle: mcp.clone(),
            elicitation: srv.elicitation,
            yolo: params.yolo,
            workflow: false,
        },
    );
    let session_id = handle.session_id.to_string();
    let spec = params.model.spec();
    let persisted_options = Default::default();
    let snapshot = install_session(
        srv,
        params,
        InstallSession {
            handle,
            mcp,
            current_model: spec,
            history: Vec::new(),
            initial_cost: None,
            cwd,
            fast: false,
            workflow: false,
            persisted_options: &persisted_options,
        },
    )
    .ok_or_else(|| AcpError::internal_error().data(json_str(&"session registration failed")))?;
    let resp = methods::new_session_response(&session_id, &srv.modes)
        .config_options(methods::session_config_options(&snapshot));
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
    let session_cwd = restored.cwd.clone().unwrap_or(req.cwd);
    let recorded_model = match Model::from_spec(&restored.model) {
        Ok(model) if params.model_policy.allows(&model.spec()) => model,
        _ => params.model.clone(),
    };
    let spec = recorded_model.spec();
    let fast = restored.meta.fast && recorded_model.supports_fast();
    let yolo = restored.meta.yolo;
    let workflow = restored.meta.workflow;
    let coordinator_history = restored.history.clone();
    let handle = spawn_session(
        params,
        SpawnSession {
            model: recorded_model.clone(),
            cwd: session_cwd.clone(),
            session_id: Some(session_ref),
            history: restored.history,
            mcp_handle: mcp.clone(),
            elicitation: srv.elicitation,
            yolo,
            workflow,
        },
    );
    let restored_cost = settle_session(
        &restored.usage,
        &mut restored.by_model,
        &recorded_model,
        fast,
    );
    let snapshot = install_session(
        srv,
        params,
        InstallSession {
            handle,
            mcp,
            current_model: spec,
            history: coordinator_history,
            initial_cost: restored_cost,
            cwd: session_cwd,
            fast,
            workflow,
            persisted_options: &restored.meta.session_options,
        },
    )
    .ok_or_else(|| AcpError::internal_error().data(json_str(&"session registration failed")))?;
    let resp = methods::load_session_response(&srv.modes)
        .config_options(methods::session_config_options(&snapshot));
    Ok(AgentResponse::LoadSessionResponse(resp))
}

fn spawn_session(params: &AcpParams, session: SpawnSession) -> InteractiveHandle {
    let SpawnSession {
        model,
        cwd,
        session_id,
        history,
        mcp_handle,
        elicitation,
        yolo,
        workflow,
    } = session;
    headless::spawn_interactive(InteractiveParams {
        model,
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
        yolo,
        system_prompt_override: params.system_prompt_override.clone(),
        append_system_prompt: params.append_system_prompt.clone(),
        workflow,
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
    let operation = state.pending.lock().unwrap().operation.take();
    if let Some(operation) = operation {
        let resp = PromptResponse::new(StopReason::Cancelled);
        send(
            &srv.out_tx,
            Response::new(
                operation.request_id,
                Ok(AgentResponse::PromptResponse(resp)),
            ),
        );
    }
    state.command_projection_task.cancel().await;
    state.option_projection_task.cancel().await;
    if let Some(coordinator) = &state.coordinator
        && let Err(error) = coordinator.close().await
    {
        warn!(%error, "failed to close session coordinator");
    }
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
    params: &AcpParams,
    session: InstallSession<'_>,
) -> Option<maki_agent::session_options::SessionOptionsSnapshot> {
    let InstallSession {
        handle,
        mcp,
        current_model,
        history,
        initial_cost,
        cwd,
        fast,
        workflow,
        persisted_options,
    } = session;
    let definitions = maki_agent::session_coordinator::builtin_option_definitions(
        Arc::from(current_model.as_str()),
        srv.model_specs.iter().map(|spec| Arc::from(spec.as_str())),
        handle.permissions.is_yolo(),
        fast,
        workflow,
    );
    let checkpoint = match maki_agent::session_checkpoint::SessionLogCheckpoint::resolve(
        handle.session_id.id(),
        &current_model,
        &cwd.to_string_lossy(),
    ) {
        Ok(checkpoint) => Arc::new(checkpoint),
        Err(error) => {
            warn!(%error, "failed to open session checkpoint");
            return None;
        }
    };
    let coordinator = match maki_agent::session_coordinator::SessionCoordinatorHandle::register(
        maki_agent::session_coordinator::SessionCoordinatorParams {
            session_id: handle.session_id.id(),
            catalog: params.session_options.clone(),
            definitions,
            persisted_options: persisted_options.clone(),
            history,
            model: Arc::from(current_model.as_str()),
            cwd: cwd.clone(),
            model_policy: Arc::clone(&params.model_policy),
            model_adopter: Arc::new({
                let control_tx = handle.control_tx.clone();
                move |model: Model| {
                    let control_tx = control_tx.clone();
                    Box::pin(async move {
                        let (reply, response) = flume::bounded(1);
                        control_tx
                            .send_async(maki_agent::headless::InteractiveControl::AdoptModel {
                                model,
                                reply,
                            })
                            .await
                            .map_err(|_| Arc::from("session ended before model adoption"))?;
                        response
                            .recv_async()
                            .await
                            .map_err(|_| Arc::from("session ended during model adoption"))?
                            .map_err(Arc::from)
                    }) as maki_agent::session_coordinator::ModelAdoptionFuture
                }
            }),
            directory_adopter: Arc::new({
                let control_tx = handle.control_tx.clone();
                move |path: PathBuf| {
                    let control_tx = control_tx.clone();
                    Box::pin(async move {
                        let (reply, response) = flume::bounded(1);
                        control_tx
                            .send_async(maki_agent::headless::InteractiveControl::ChangeDirectory {
                                path,
                                reply,
                            })
                            .await
                            .map_err(|_| Arc::from("session ended before directory adoption"))?;
                        response
                            .recv_async()
                            .await
                            .map_err(|_| Arc::from("session ended during directory adoption"))?
                            .map_err(Arc::from)
                    })
                        as maki_agent::session_coordinator::DirectoryAdoptionFuture
                }
            }),
            checkpoint,
            mailbox: handle.mailbox.clone(),
        },
    ) {
        Ok(coordinator) => coordinator,
        Err(error) => {
            warn!(%error, "failed to register session coordinator");
            return None;
        }
    };
    let pending = PendingState::default();
    start_event_pump(
        handle.event_rx.clone(),
        handle.session_id.clone(),
        srv.out_tx.clone(),
        Arc::clone(&pending),
        srv.elicitation,
        handle.answer_tx.clone(),
        coordinator.read(),
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
        fast,
        workflow,
    ));
    let command_target = command_registry.bind_target(
        maki_agent::command::portable_capabilities(),
        Arc::new(
            maki_agent::command::SessionCommandHost::new(
                Arc::clone(&params.model_policy),
                handle.model_tx.clone(),
                handle.control_tx.clone(),
                Arc::clone(&command_state),
                Arc::clone(&handle.permissions),
            )
            .with_coordinator(coordinator.clone()),
        ),
    );
    let session_id = SessionId::from(handle.session_id.to_string());
    let commands = command_registry
        .presented_commands(&command_target)
        .unwrap_or_default();
    emit_available_commands(&srv.out_tx, &session_id, &commands);
    let command_projection_task = watch_available_commands(
        srv.out_tx.clone(),
        session_id.clone(),
        command_registry.clone(),
        command_target.clone(),
    );
    let option_snapshot = coordinator.read().options();
    let option_projection = Arc::new(OptionProjection {
        out_tx: srv.out_tx.clone(),
        session_id,
        read: coordinator.read(),
        command_state: Arc::clone(&command_state),
        permissions: Arc::clone(&handle.permissions),
        emitted_version: AtomicU64::new(option_snapshot.version),
    });
    let option_projection_task = watch_config_options(
        Arc::clone(&option_projection),
        coordinator.read().subscribe(),
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
                return None;
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
        coordinator: Some(coordinator),
        mcp,
        current_mode: AgentMode::Build,
        command_state,
        pending,
        command_registry,
        command_target,
        command_projection_task,
        option_projection: Some(option_projection),
        option_projection_task,
        lock,
    });
    Some(option_snapshot)
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

fn apply_committed_options(
    snapshot: &maki_agent::session_options::SessionOptionsSnapshot,
    command_state: &maki_agent::command::SessionCommandState,
    permissions: &maki_agent::permissions::PermissionManager,
) {
    let value = |id: &str| {
        snapshot
            .options
            .iter()
            .find(|option| option.definition.id.as_ref() == id)
            .map(|option| option.current_value.as_ref())
    };
    if let Some(spec) = value(maki_agent::session_options::MODEL_OPTION_ID) {
        match Model::from_spec(spec) {
            Ok(model) => command_state.set_model(&model),
            Err(error) => warn!(%error, %spec, "committed model could not be parsed"),
        }
    }
    permissions.set_yolo(
        value(maki_agent::session_options::YOLO_OPTION_ID)
            == Some(maki_agent::session_options::ENABLED_VALUE),
    );
    if let Err(error) = command_state.set_fast(
        value(maki_agent::session_options::FAST_OPTION_ID)
            == Some(maki_agent::session_options::ENABLED_VALUE),
    ) {
        warn!(%error, "committed Fast value could not be applied");
    }
    command_state.set_workflow(
        value(maki_agent::session_options::WORKFLOW_OPTION_ID)
            == Some(maki_agent::session_options::ENABLED_VALUE),
    );
}

fn emit_config_options(
    out_tx: &Sender<Value>,
    session_id: &SessionId,
    snapshot: &maki_agent::session_options::SessionOptionsSnapshot,
) {
    session_update(
        out_tx,
        session_id,
        SessionUpdate::ConfigOptionUpdate(ConfigOptionUpdate::new(
            methods::session_config_options(snapshot),
        )),
    );
}

fn watch_config_options(
    projection: Arc<OptionProjection>,
    mut subscription: maki_agent::session_options::SessionOptionsSubscription,
) -> smol::Task<()> {
    smol::spawn(async move {
        loop {
            let snapshot = subscription.changed().await;
            projection.emit(&snapshot);
        }
    })
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
    meta: maki_storage::sessions::SessionMeta,
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
    let meta = session.meta.clone();
    let history = session.take_messages();
    Ok(Restored {
        history,
        cwd,
        usage,
        by_model,
        model,
        meta,
    })
}

fn validate_session<'a>(srv: &'a Server, requested: &str) -> Result<&'a SessionState, AcpError> {
    let session = srv.session.as_ref().ok_or_else(no_session)?;
    let active = session.handle.session_id.to_string();
    if active != requested
        || maki_agent::session_coordinator::SessionCoordinatorHandle::resolve(
            session.handle.session_id.id(),
        )
        .is_err()
    {
        return Err(AcpError::resource_not_found(Some(format!(
            "session/{requested}"
        ))));
    }
    Ok(session)
}

async fn handle_prompt(srv: &mut Server, raw: &Value, id: &RequestId) -> Result<(), AcpError> {
    let req: PromptRequest = parse_params(raw)?;
    let session = validate_session(srv, req.session_id.0.as_ref())?;
    let content = extract_prompt_content(&req.prompt)?;
    let dispatch = session
        .command_registry
        .dispatch_input(&session.command_target, content)
        .await;
    match dispatch {
        InputDispatch::LiteralInput(content) => {
            send_command_turn(
                session,
                id,
                AgentTurn {
                    content,
                    prompt: None,
                },
            )
            .await
        }
        InputDispatch::Dispatched(CommandOutcome::Completed) => {
            if let Some(projection) = &session.option_projection {
                projection.emit_current();
            }
            respond_prompt(&srv.out_tx, id.clone(), StopReason::EndTurn);
            Ok(())
        }
        InputDispatch::Dispatched(CommandOutcome::AgentTurn(turn)) => {
            send_command_turn(session, id, turn).await
        }
        InputDispatch::Dispatched(CommandOutcome::IsolatedTurn(turn)) => {
            send_isolated_turn(session, &srv.out_tx, id, turn).await
        }
        InputDispatch::Dispatched(CommandOutcome::ManualCompaction) => {
            send_manual_compaction(session, &srv.out_tx, id).await
        }
        InputDispatch::Dispatched(CommandOutcome::FrontendFeedback(feedback)) => {
            let text = match feedback {
                maki_commands::FrontendFeedback::WorkingDirectory(path) => {
                    format!("Working directory: {}", path.display())
                }
            };
            let sid = SessionId::from(session.handle.session_id.to_string());
            session_update(&srv.out_tx, &sid, translate::text_delta(&text));
            respond_prompt(&srv.out_tx, id.clone(), StopReason::EndTurn);
            Ok(())
        }
        InputDispatch::Dispatched(CommandOutcome::Failed(error)) => Err(command_error(error)),
    }
}

async fn send_manual_compaction(
    session: &SessionState,
    out_tx: &Sender<Value>,
    id: &RequestId,
) -> Result<(), AcpError> {
    if session.pending.lock().unwrap().operation.is_some() {
        return Err(AcpError::new(
            -32600,
            "session already has an active operation",
        ));
    }
    let lease = session
        .coordinator
        .as_ref()
        .ok_or_else(no_session)?
        .acquire_lease()
        .await
        .map_err(coordinator_error)?;
    let lease_committer = lease.committer();
    let operation_id = NEXT_OPERATION_ID.fetch_add(1, Ordering::Relaxed);
    let tool_id = format!("compact-{operation_id}");
    let (trigger, cancel) = maki_agent::cancel::CancelToken::new();
    {
        let mut pending = session.pending.lock().unwrap();
        if pending.operation.is_some() {
            return Err(AcpError::new(
                -32600,
                "session already has an active operation",
            ));
        }
        pending.operation = Some(PendingOperation {
            id: operation_id,
            request_id: id.clone(),
            kind: OperationKind::ManualCompaction,
            cancel: Some(trigger),
            _lease: Some(lease),
        });
    }
    let sid = SessionId::from(session.handle.session_id.to_string());
    session_update(
        out_tx,
        &sid,
        translate::local_operation_pending(&tool_id, "Compact context"),
    );
    let (output, events) = flume::unbounded();
    if session
        .handle
        .control_tx
        .send(maki_agent::headless::InteractiveControl::ManualCompaction {
            output,
            cancel,
            lease_committer,
        })
        .is_err()
    {
        take_operation(
            &session.pending,
            operation_id,
            OperationKind::ManualCompaction,
        );
        return Err(AcpError::new(-32603, "session ended"));
    }
    let pending = Arc::clone(&session.pending);
    let out_tx = out_tx.clone();
    smol::spawn(async move {
        while let Ok(event) = events.recv_async().await {
            use maki_agent::headless::ManualCompactionEvent;

            match event {
                ManualCompactionEvent::Started => {
                    session_update(&out_tx, &sid, translate::local_operation_started(&tool_id));
                }
                ManualCompactionEvent::Completed => {
                    if let Some(operation) =
                        take_operation(&pending, operation_id, OperationKind::ManualCompaction)
                    {
                        session_update(
                            &out_tx,
                            &sid,
                            translate::local_operation_terminal(&tool_id, None),
                        );
                        respond_prompt(&out_tx, operation.request_id, StopReason::EndTurn);
                    }
                    break;
                }
                ManualCompactionEvent::Cancelled => {
                    if let Some(operation) =
                        take_operation(&pending, operation_id, OperationKind::ManualCompaction)
                    {
                        session_update(
                            &out_tx,
                            &sid,
                            translate::local_operation_terminal(&tool_id, Some("cancelled")),
                        );
                        respond_prompt(&out_tx, operation.request_id, StopReason::Cancelled);
                    }
                    break;
                }
                ManualCompactionEvent::Failed(error) => {
                    if let Some(operation) =
                        take_operation(&pending, operation_id, OperationKind::ManualCompaction)
                    {
                        session_update(
                            &out_tx,
                            &sid,
                            translate::local_operation_terminal(&tool_id, Some(&error)),
                        );
                        let error = AcpError::internal_error().data(Value::String(error));
                        send(
                            &out_tx,
                            Response::new(operation.request_id, Err::<AgentResponse, _>(error)),
                        );
                    }
                    break;
                }
            }
        }
    })
    .detach();
    Ok(())
}

async fn send_isolated_turn(
    session: &SessionState,
    out_tx: &Sender<Value>,
    id: &RequestId,
    turn: maki_commands::IsolatedTurn,
) -> Result<(), AcpError> {
    if session.pending.lock().unwrap().operation.is_some() {
        return Err(AcpError::new(
            -32600,
            "session already has an active operation",
        ));
    }
    let images = turn
        .content
        .attachments
        .iter()
        .map(|attachment| ImageSource {
            media_type: image_media_type(&attachment.media_type),
            data: Arc::clone(&attachment.data),
        })
        .collect();
    let lease = session
        .coordinator
        .as_ref()
        .ok_or_else(no_session)?
        .acquire_lease()
        .await
        .map_err(coordinator_error)?;
    let operation_id = NEXT_OPERATION_ID.fetch_add(1, Ordering::Relaxed);
    let (trigger, cancel) = maki_agent::cancel::CancelToken::new();
    {
        let mut pending = session.pending.lock().unwrap();
        if pending.operation.is_some() {
            return Err(AcpError::new(
                -32600,
                "session already has an active operation",
            ));
        }
        pending.operation = Some(PendingOperation {
            id: operation_id,
            request_id: id.clone(),
            kind: OperationKind::IsolatedTurn,
            cancel: Some(trigger),
            _lease: Some(lease),
        });
    }
    let (output, events) = flume::unbounded();
    if session
        .handle
        .control_tx
        .send(maki_agent::headless::InteractiveControl::IsolatedTurn {
            question: turn.content.text.to_string(),
            images,
            output,
            cancel,
        })
        .is_err()
    {
        take_operation(&session.pending, operation_id, OperationKind::IsolatedTurn);
        return Err(AcpError::new(-32603, "session ended"));
    }
    let pending = Arc::clone(&session.pending);
    let out_tx = out_tx.clone();
    let sid = SessionId::from(session.handle.session_id.to_string());
    smol::spawn(async move {
        while let Ok(event) = events.recv_async().await {
            use maki_agent::agent::isolated_turn::IsolatedTurnEvent;

            match event {
                IsolatedTurnEvent::TextDelta(text) => {
                    session_update(&out_tx, &sid, translate::text_delta(&text));
                }
                IsolatedTurnEvent::ThinkingDelta(text) => {
                    session_update(&out_tx, &sid, translate::thinking_delta(&text));
                }
                IsolatedTurnEvent::Done => {
                    if let Some(operation) =
                        take_operation(&pending, operation_id, OperationKind::IsolatedTurn)
                    {
                        respond_prompt(&out_tx, operation.request_id, StopReason::EndTurn);
                    }
                    break;
                }
                IsolatedTurnEvent::Cancelled => {
                    if let Some(operation) =
                        take_operation(&pending, operation_id, OperationKind::IsolatedTurn)
                    {
                        respond_prompt(&out_tx, operation.request_id, StopReason::Cancelled);
                    }
                    break;
                }
                IsolatedTurnEvent::Error(message) => {
                    if let Some(operation) =
                        take_operation(&pending, operation_id, OperationKind::IsolatedTurn)
                    {
                        let error = AcpError::internal_error().data(Value::String(message));
                        send(
                            &out_tx,
                            Response::new(operation.request_id, Err::<AgentResponse, _>(error)),
                        );
                    }
                    break;
                }
            }
        }
    })
    .detach();
    Ok(())
}

async fn send_command_turn(
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
    let options = session
        .coordinator
        .as_ref()
        .ok_or_else(no_session)?
        .read()
        .options();
    let enabled = |id: &str| {
        options.options.iter().any(|option| {
            option.definition.id.as_ref() == id
                && option.current_value.as_ref() == maki_agent::session_options::ENABLED_VALUE
        })
    };
    let mut input = agent_input(
        turn.content.text.to_string(),
        images,
        session.current_mode.clone(),
        enabled(maki_agent::session_options::FAST_OPTION_ID),
        enabled(maki_agent::session_options::WORKFLOW_OPTION_ID),
        prompt,
    );
    if session.pending.lock().unwrap().operation.is_some() {
        return Err(AcpError::new(
            -32600,
            "session already has an active operation",
        ));
    }
    let lease = session
        .coordinator
        .as_ref()
        .ok_or_else(no_session)?
        .acquire_lease()
        .await
        .map_err(coordinator_error)?;
    input.lease_committer = lease.committer();
    let operation_id = NEXT_OPERATION_ID.fetch_add(1, Ordering::Relaxed);
    {
        let mut pending = session.pending.lock().unwrap();
        if pending.operation.is_some() {
            return Err(AcpError::new(
                -32600,
                "session already has an active operation",
            ));
        }
        pending.operation = Some(PendingOperation {
            id: operation_id,
            request_id: id.clone(),
            kind: OperationKind::PrimaryTurn,
            cancel: None,
            _lease: Some(lease),
        });
    }
    if session.handle.input_tx.send(input).is_err() {
        take_operation(&session.pending, operation_id, OperationKind::PrimaryTurn);
        return Err(AcpError::new(-32603, "session ended"));
    }
    Ok(())
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
        lease_committer: None,
    }
}

fn command_error(error: maki_commands::CommandError) -> AcpError {
    AcpError::new(-32602, error.to_string())
}

fn coordinator_error(error: maki_agent::session_coordinator::SessionCoordinatorError) -> AcpError {
    use maki_agent::session_coordinator::SessionCoordinatorError;
    use maki_agent::session_options::SessionOptionError;

    match error {
        SessionCoordinatorError::StaleSession(id) => {
            AcpError::resource_not_found(Some(format!("session/{id}")))
        }
        SessionCoordinatorError::SessionBusy(_) => AcpError::new(-32600, error.to_string()),
        SessionCoordinatorError::Option(
            SessionOptionError::UnknownId(_)
            | SessionOptionError::InvalidValue { .. }
            | SessionOptionError::FastUnsupported
            | SessionOptionError::PolicyRejected(_),
        ) => AcpError::invalid_params().data(json_str(&error.to_string())),
        _ => AcpError::internal_error().data(json_str(&error.to_string())),
    }
}

fn take_operation(
    pending: &PendingState,
    operation_id: u64,
    kind: OperationKind,
) -> Option<PendingOperation> {
    let mut pending = pending.lock().unwrap();
    if pending
        .operation
        .as_ref()
        .is_some_and(|operation| operation.id == operation_id && operation.kind == kind)
    {
        pending.operation.take()
    } else {
        None
    }
}

fn take_active_operation(pending: &PendingState, kind: OperationKind) -> Option<PendingOperation> {
    let operation_id = pending
        .lock()
        .unwrap()
        .operation
        .as_ref()
        .filter(|operation| operation.kind == kind)
        .map(|operation| operation.id)?;
    take_operation(pending, operation_id, kind)
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
    validate_session(srv, req.session_id.0.as_ref())?;
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

async fn handle_set_config(srv: &mut Server, raw: &Value) -> Result<AgentResponse, AcpError> {
    let req: SetSessionConfigOptionRequest = parse_params(raw)?;
    validate_session(srv, req.session_id.0.as_ref())?;
    let config_id = req.config_id.0.to_string();
    let value = req.value.0.to_string();
    let session = srv.session.as_mut().ok_or_else(no_session)?;
    let coordinator = session.coordinator.as_ref().ok_or_else(no_session)?;
    let snapshot = coordinator
        .set_option(config_id.as_str(), value.as_str())
        .await
        .map_err(coordinator_error)?;
    apply_committed_options(
        &snapshot,
        &session.command_state,
        &session.handle.permissions,
    );
    Ok(AgentResponse::SetSessionConfigOptionResponse(
        SetSessionConfigOptionResponse::new(methods::session_config_options(&snapshot)),
    ))
}

fn handle_notification(srv: &Server, method: &str, raw: &Value) {
    match method {
        "session/cancel" => {
            let Some(requested) = raw
                .get("params")
                .and_then(|params| params.get("sessionId"))
                .and_then(Value::as_str)
            else {
                return;
            };
            if let Ok(session) = validate_session(srv, requested) {
                // Any answer still in flight belongs to the cancelled turn, so
                // forget its id and let it be dropped on arrival.
                let isolated_cancel = {
                    let mut pending = session.pending.lock().unwrap();
                    pending.permission = None;
                    pending.elicitation = None;
                    pending
                        .operation
                        .as_mut()
                        .and_then(|operation| operation.cancel.take())
                };
                if let Some(trigger) = isolated_cancel {
                    trigger.cancel();
                } else {
                    let _ = session.handle.cancel_tx.try_send(());
                }
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
    session: maki_agent::session_coordinator::SessionReadHandle,
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
                    translate::tool_start(&event, &session.cwd(), home.as_deref())
                }
                AgentEvent::ToolOutput { id, content } => translate::tool_output(&id, &content),
                AgentEvent::ToolDone(event) => {
                    translate::tool_done(&event, &session.cwd(), home.as_deref())
                }
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
                AgentEvent::TurnOutcome(outcome) => {
                    if let Some(operation) =
                        take_active_operation(&pending, OperationKind::PrimaryTurn)
                    {
                        match outcome {
                            maki_agent::TurnOutcome::Completed { reason, .. } => {
                                let resp = PromptResponse::new(translate::map_done_reason(reason));
                                send(
                                    &out_tx,
                                    Response::new(
                                        operation.request_id,
                                        Ok(AgentResponse::PromptResponse(resp)),
                                    ),
                                );
                            }
                            maki_agent::TurnOutcome::Cancelled { .. } => {
                                respond_prompt(
                                    &out_tx,
                                    operation.request_id,
                                    StopReason::Cancelled,
                                );
                            }
                            maki_agent::TurnOutcome::Failed { failure, .. } => {
                                let error = AcpError::internal_error()
                                    .data(Value::String(failure.user_message));
                                send(
                                    &out_tx,
                                    Response::<AgentResponse>::new(
                                        operation.request_id,
                                        Err(error),
                                    ),
                                );
                            }
                        }
                    }
                    continue;
                }
                AgentEvent::ControlComplete { .. } => {
                    if let Some(operation) =
                        take_active_operation(&pending, OperationKind::PrimaryTurn)
                    {
                        let resp = PromptResponse::new(StopReason::EndTurn);
                        send(
                            &out_tx,
                            Response::new(
                                operation.request_id,
                                Ok(AgentResponse::PromptResponse(resp)),
                            ),
                        );
                    }
                    continue;
                }
                AgentEvent::ControlError { message } => {
                    if let Some(operation) =
                        take_active_operation(&pending, OperationKind::PrimaryTurn)
                    {
                        let error = AcpError::internal_error().data(Value::String(message));
                        send(
                            &out_tx,
                            Response::<AgentResponse>::new(operation.request_id, Err(error)),
                        );
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
    const FAST_SPEC: &str = "anthropic/claude-opus-4-8";
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

    fn test_coordinator(
        session_id: MakiId,
        model: &str,
        cwd: PathBuf,
    ) -> maki_agent::session_coordinator::SessionCoordinatorHandle {
        let checkpoint: Arc<
            dyn maki_storage::checkpoint::CheckpointWriter<
                    maki_agent::session_coordinator::SessionCheckpoint,
                >,
        > = Arc::new(|request: maki_storage::checkpoint::CheckpointRequest<_>| {
            Box::pin(async move {
                Ok(maki_storage::checkpoint::CheckpointAck {
                    session_id: request.session_id,
                    version: request.version,
                })
            }) as maki_storage::checkpoint::CheckpointFuture
        });
        let mut model_specs = vec![
            Arc::from(model),
            Arc::from(FAST_SPEC),
            Arc::from(OFFLINE_SPEC),
        ];
        model_specs.sort();
        model_specs.dedup();
        maki_agent::session_coordinator::SessionCoordinatorHandle::register(
            maki_agent::session_coordinator::SessionCoordinatorParams {
                session_id,
                catalog: Default::default(),
                definitions: maki_agent::session_coordinator::builtin_option_definitions(
                    model,
                    model_specs,
                    false,
                    false,
                    false,
                ),
                persisted_options: Default::default(),
                history: Vec::new(),
                model: Arc::from(model),
                cwd,
                model_policy: Arc::new(maki_config::ModelPolicy::default()),
                model_adopter: Arc::new(|_: Model| {
                    Box::pin(async { Ok(()) })
                        as maki_agent::session_coordinator::ModelAdoptionFuture
                }),
                directory_adopter: Arc::new(|path: PathBuf| {
                    Box::pin(async move { Ok(path) })
                        as maki_agent::session_coordinator::DirectoryAdoptionFuture
                }),
                checkpoint,
                mailbox: maki_agent::SessionMailbox::new(session_id),
            },
        )
        .unwrap()
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
        let session_id = MakiId::generate();
        let handle = InteractiveHandle {
            event_rx: flume::unbounded().1,
            tool_names: Vec::new(),
            input_tx,
            answer_tx,
            cancel_tx: flume::unbounded().0,
            model_tx: flume::unbounded().0,
            control_tx: flume::unbounded().0,
            session_id: SessionRef::from(session_id),
            mailbox: maki_agent::SessionMailbox::new(session_id),
            permissions: Arc::new(PermissionManager::new(
                maki_config::PermissionsConfig::default(),
                PathBuf::from("/project"),
                Arc::default(),
            )),
            task: smol::spawn(async {}),
        };
        let coordinator = test_coordinator(
            handle.session_id.id(),
            OFFLINE_SPEC,
            PathBuf::from("/project"),
        );
        let command_registry = test_registry(&[]);
        let command_state = Arc::new(maki_agent::command::SessionCommandState::new(
            String::new(),
            Arc::from([]),
            PathBuf::from("/project"),
            false,
            false,
        ));
        let command_target = command_registry.bind_target(
            maki_agent::command::portable_capabilities(),
            Arc::new(
                maki_agent::command::SessionCommandHost::new(
                    Arc::new(maki_config::ModelPolicy::default()),
                    handle.model_tx.clone(),
                    handle.control_tx.clone(),
                    Arc::clone(&command_state),
                    Arc::clone(&handle.permissions),
                )
                .with_coordinator(coordinator.clone()),
            ),
        );
        let server = Server {
            out_tx: out_tx.clone(),
            modes: Arc::new(maki_agent::ModeRegistry::builtin()),
            model_specs: Vec::new(),
            session: Some(SessionState {
                option_projection: Some(Arc::new(OptionProjection {
                    out_tx: out_tx.clone(),
                    session_id: SessionId::from(handle.session_id.to_string()),
                    read: coordinator.read(),
                    command_state: Arc::clone(&command_state),
                    permissions: Arc::clone(&handle.permissions),
                    emitted_version: AtomicU64::new(coordinator.read().options().version),
                })),
                handle,
                coordinator: Some(coordinator),
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
                option_projection_task: smol::spawn(async {}),
                lock: None,
            }),
            elicitation: false,
        };
        (server, answer_rx, out_rx, input_rx)
    }

    #[test]
    fn operation_terminal_is_compare_and_set() {
        let pending = Arc::new(Mutex::new(Pending {
            operation: Some(PendingOperation {
                id: 7,
                request_id: RequestId::Number(41),
                kind: OperationKind::TestLocal,
                cancel: None,
                _lease: None,
            }),
            ..Pending::default()
        }));

        assert!(take_operation(&pending, 6, OperationKind::TestLocal).is_none());
        assert!(take_operation(&pending, 7, OperationKind::PrimaryTurn).is_none());
        let operation = take_operation(&pending, 7, OperationKind::TestLocal).unwrap();
        assert_eq!(operation.request_id, RequestId::Number(41));
        assert!(take_operation(&pending, 7, OperationKind::TestLocal).is_none());
    }

    #[test]
    fn primary_terminal_cannot_finish_isolated_operation() {
        let pending = Arc::new(Mutex::new(Pending {
            operation: Some(PendingOperation {
                id: 9,
                request_id: RequestId::Number(43),
                kind: OperationKind::TestLocal,
                cancel: None,
                _lease: None,
            }),
            ..Pending::default()
        }));

        assert!(take_active_operation(&pending, OperationKind::PrimaryTurn).is_none());
        assert!(take_active_operation(&pending, OperationKind::TestLocal).is_some());
    }

    #[test]
    fn primary_prompt_holds_coordinator_lease_until_terminal() {
        smol::block_on(async {
            let (mut srv, _, _, input_rx) = server_awaiting_answer();
            let session_id = srv.session.as_ref().unwrap().handle.session_id.id();
            let coordinator = srv
                .session
                .as_ref()
                .unwrap()
                .coordinator
                .as_ref()
                .unwrap()
                .clone();
            let request_id = RequestId::Number(41);
            handle_prompt(
                &mut srv,
                &prompt_request(&session_id.to_string(), "hello", false),
                &request_id,
            )
            .await
            .unwrap();
            assert!(input_rx.try_recv().is_ok());

            let (done_tx, done_rx) = flume::bounded(1);
            let queued = coordinator.clone();
            smol::spawn(async move {
                let result = queued
                    .set_option(
                        maki_agent::session_options::YOLO_OPTION_ID,
                        maki_agent::session_options::ENABLED_VALUE,
                    )
                    .await;
                let _ = done_tx.send(result);
            })
            .detach();
            assert!(done_rx.try_recv().is_err());

            let pending = &srv.session.as_ref().unwrap().pending;
            let operation = take_active_operation(pending, OperationKind::PrimaryTurn).unwrap();
            assert_eq!(operation.request_id, request_id);
            drop(operation);
            let snapshot = done_rx.recv_async().await.unwrap().unwrap();
            assert_eq!(
                snapshot.options[1].current_value.as_ref(),
                maki_agent::session_options::ENABLED_VALUE
            );
            coordinator.close().await.unwrap();
        });
    }

    #[test]
    fn slash_update_precedes_prompt_response() {
        smol::block_on(async {
            let (mut srv, _, out_rx, _) = server_awaiting_answer();
            let session = srv.session.as_mut().unwrap();
            let coordinator = session.coordinator.as_ref().unwrap().clone();
            coordinator
                .set_option(maki_agent::session_options::MODEL_OPTION_ID, FAST_SPEC)
                .await
                .unwrap();
            session
                .command_state
                .set_model(&Model::from_spec(FAST_SPEC).unwrap());
            session.option_projection = Some(Arc::new(OptionProjection {
                out_tx: srv.out_tx.clone(),
                session_id: SessionId::from(session.handle.session_id.to_string()),
                read: coordinator.read(),
                command_state: Arc::clone(&session.command_state),
                permissions: Arc::clone(&session.handle.permissions),
                emitted_version: AtomicU64::new(coordinator.read().options().version),
            }));
            let request_id = RequestId::Number(42);
            handle_prompt(
                &mut srv,
                &serde_json::json!({
                    "params": {
                        "sessionId": coordinator.read().session_id().to_string(),
                        "prompt": [{ "type": "text", "text": "/fast" }]
                    }
                }),
                &request_id,
            )
            .await
            .unwrap();

            let update = out_rx.recv_async().await.unwrap();
            assert_eq!(update["method"], "session/update");
            assert_eq!(
                update["params"]["update"]["configOptions"]
                    .as_array()
                    .map(Vec::len),
                Some(4)
            );
            let response = out_rx.recv_async().await.unwrap();
            assert_eq!(response["id"], 42);
            coordinator.close().await.unwrap();
        });
    }

    #[test]
    fn config_model_change_clears_ineligible_fast_mode() {
        let (mut srv, ..) = server_awaiting_answer();
        let coordinator = srv
            .session
            .as_ref()
            .unwrap()
            .coordinator
            .as_ref()
            .unwrap()
            .clone();
        smol::block_on(async {
            coordinator
                .set_option(maki_agent::session_options::MODEL_OPTION_ID, FAST_SPEC)
                .await
                .unwrap();
            coordinator
                .set_option(
                    maki_agent::session_options::FAST_OPTION_ID,
                    maki_agent::session_options::ENABLED_VALUE,
                )
                .await
                .unwrap();
        });
        let session = srv.session.as_mut().unwrap();
        session
            .command_state
            .set_model(&Model::from_spec(FAST_SPEC).expect("fast-capable test model should parse"));
        session.command_state.set_fast(true).unwrap();

        let active_id = srv.session.as_ref().unwrap().handle.session_id.to_string();
        let result = smol::block_on(handle_set_config(
            &mut srv,
            &serde_json::json!({
                "params": {
                    "sessionId": active_id,
                    "configId": methods::MODEL_CONFIG_ID,
                    "value": OFFLINE_SPEC,
                }
            }),
        ));

        let response = result.unwrap();
        let AgentResponse::SetSessionConfigOptionResponse(response) = response else {
            panic!("expected config option response");
        };
        assert_eq!(response.config_options.len(), 4);
        let snapshot = coordinator.read().options();
        assert_eq!(snapshot.options[0].current_value.as_ref(), OFFLINE_SPEC);
        assert_eq!(
            snapshot.options[2].current_value.as_ref(),
            maki_agent::session_options::DISABLED_VALUE
        );
        let state = &srv.session.as_ref().unwrap().command_state;
        assert_eq!(state.current_model(), OFFLINE_SPEC);
        assert!(!state.fast());
        smol::block_on(coordinator.close()).unwrap();
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
    fn acp_rejects_stale_session_ids_for_prompt_set_option_mode_and_cancel() {
        let (mut srv, answer_rx, _, input_rx) = server_awaiting_answer();
        let stale = MakiId::generate().to_string();
        let prompt_error = smol::block_on(handle_prompt(
            &mut srv,
            &prompt_request(&stale, "hello", false),
            &RequestId::Number(9),
        ))
        .unwrap_err();
        assert_eq!(prompt_error.code, AcpError::resource_not_found(None).code);
        assert!(input_rx.is_empty());

        let config_error = smol::block_on(handle_set_config(
            &mut srv,
            &serde_json::json!({
                "params": {
                    "sessionId": stale,
                    "configId": maki_agent::session_options::YOLO_OPTION_ID,
                    "value": maki_agent::session_options::ENABLED_VALUE
                }
            }),
        ))
        .unwrap_err();
        assert_eq!(config_error.code, AcpError::resource_not_found(None).code);

        let mode_error = handle_set_mode(
            &mut srv,
            &serde_json::json!({
                "params": {
                    "sessionId": stale,
                    "modeId": "build"
                }
            }),
        )
        .unwrap_err();
        assert_eq!(mode_error.code, AcpError::resource_not_found(None).code);

        handle_notification(
            &srv,
            "session/cancel",
            &serde_json::json!({ "params": { "sessionId": stale } }),
        );
        handle_incoming_response(&srv, &allow_once(ANSWERED_ID));
        assert_eq!(
            answer_rx.try_recv().ok(),
            Some(PermissionAnswer::AllowOnce.encode()),
            "stale cancellation must not cancel the active session"
        );
    }

    #[test]
    fn cancel_drops_the_outstanding_permission_request() {
        let (srv, answer_rx, ..) = server_awaiting_answer();
        handle_notification(
            &srv,
            "session/cancel",
            &serde_json::json!({
                "params": {
                    "sessionId": srv.session.as_ref().unwrap().handle.session_id.to_string()
                }
            }),
        );

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
        let coordinator = test_coordinator(session_id.id(), OFFLINE_SPEC, PathBuf::from("."));

        start_event_pump(
            event_rx,
            session_id,
            out_tx,
            Arc::clone(&pending),
            true,
            answer_tx,
            coordinator.read(),
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
                    break;
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
        smol::block_on(coordinator.close()).unwrap();
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
            ["compact", "model", "cd", "btw", "yolo", "fast", "workflow"]
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
        smol::block_on(refresh_models(&mut srv, vec![DISCOVERED_SPEC.to_owned()]));
        let update = out_rx.try_recv().expect("the fuller list is announced");
        let config_options = update["params"]["update"]["configOptions"]
            .as_array()
            .unwrap();
        assert_eq!(
            config_options.len(),
            4,
            "discovery publishes the full snapshot"
        );
        assert_eq!(
            config_options[1]["currentValue"],
            maki_agent::session_options::DISABLED_VALUE
        );
        assert_eq!(
            config_options[2]["currentValue"],
            maki_agent::session_options::DISABLED_VALUE
        );
        assert_eq!(
            config_options[3]["currentValue"],
            maki_agent::session_options::DISABLED_VALUE
        );
        let options = &config_options[0]["options"];
        let selectable: Vec<&str> = options
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|o| o["value"].as_str())
            .collect();
        assert!(selectable.contains(&OFFLINE_SPEC));
        assert!(selectable.contains(&DISCOVERED_SPEC));
        smol::block_on(refresh_models(&mut srv, vec![DISCOVERED_SPEC.to_owned()]));
        assert!(out_rx.is_empty());
    }

    fn prompt_request(session_id: &str, text: &str, image: bool) -> Value {
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
                "sessionId": session_id,
                "prompt": prompt,
            }
        })
    }

    async fn dispatch_prompt(
        srv: &mut Server,
        text: &str,
        image: bool,
        id: &RequestId,
    ) -> Result<(), AcpError> {
        let session_id = srv.session.as_ref().unwrap().handle.session_id.to_string();
        let raw = prompt_request(&session_id, text, image);
        handle_prompt(srv, &raw, id).await
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
    fn compact_tool_progress_success_sequence() {
        smol::block_on(async {
            let (mut srv, _, out_rx, input_rx) = server_awaiting_answer();
            let (control_tx, control_rx) = flume::unbounded();
            srv.session.as_mut().unwrap().handle.control_tx = control_tx;
            install_registry(&mut srv, test_registry(&[]));
            let request_id = RequestId::Number(30);

            dispatch_prompt(&mut srv, "/compact", false, &request_id)
                .await
                .unwrap();
            assert!(input_rx.is_empty(), "compaction bypasses primary input");
            let pending = out_rx.recv_async().await.unwrap();
            assert_eq!(pending["params"]["update"]["sessionUpdate"], "tool_call");
            assert_eq!(pending["params"]["update"]["title"], "Compact context");
            let control = control_rx.recv_async().await.unwrap();
            let maki_agent::headless::InteractiveControl::ManualCompaction { output, .. } = control
            else {
                panic!("expected manual compaction control");
            };
            output
                .send_async(maki_agent::headless::ManualCompactionEvent::Started)
                .await
                .unwrap();
            output
                .send_async(maki_agent::headless::ManualCompactionEvent::Completed)
                .await
                .unwrap();

            let started = out_rx.recv_async().await.unwrap();
            let completed = out_rx.recv_async().await.unwrap();
            let terminal = out_rx.recv_async().await.unwrap();
            assert_eq!(started["params"]["update"]["status"], "in_progress");
            assert_eq!(completed["params"]["update"]["status"], "completed");
            assert_eq!(terminal["id"], 30);
            assert_eq!(terminal["result"]["stopReason"], "end_turn");
            assert!(out_rx.is_empty(), "compaction prompt completes once");
        });
    }

    #[test]
    fn compact_prompt_cancellation_completes_once() {
        smol::block_on(async {
            let (mut srv, _, out_rx, _) = server_awaiting_answer();
            let (control_tx, control_rx) = flume::unbounded();
            srv.session.as_mut().unwrap().handle.control_tx = control_tx;
            install_registry(&mut srv, test_registry(&[]));
            let session_id = srv.session.as_ref().unwrap().handle.session_id.to_string();

            dispatch_prompt(&mut srv, "/compact", false, &RequestId::Number(33))
                .await
                .unwrap();
            let _pending = out_rx.recv_async().await.unwrap();
            let control = control_rx.recv_async().await.unwrap();
            let maki_agent::headless::InteractiveControl::ManualCompaction {
                output, cancel, ..
            } = control
            else {
                panic!("expected manual compaction control");
            };
            handle_notification(
                &srv,
                "session/cancel",
                &serde_json::json!({ "params": { "sessionId": session_id } }),
            );
            cancel.cancelled().await;
            output
                .send_async(maki_agent::headless::ManualCompactionEvent::Cancelled)
                .await
                .unwrap();

            let failed = out_rx.recv_async().await.unwrap();
            let terminal = out_rx.recv_async().await.unwrap();
            assert_eq!(failed["params"]["update"]["status"], "failed");
            assert_eq!(terminal["id"], 33);
            assert_eq!(terminal["result"]["stopReason"], "cancelled");
            let _ = output
                .send_async(maki_agent::headless::ManualCompactionEvent::Completed)
                .await;
            smol::future::yield_now().await;
            assert!(
                out_rx.is_empty(),
                "late completion must not terminate twice"
            );
        });
    }

    #[test]
    fn compact_tool_progress_failure_sequence() {
        smol::block_on(async {
            let (mut srv, _, out_rx, _) = server_awaiting_answer();
            let (control_tx, control_rx) = flume::unbounded();
            srv.session.as_mut().unwrap().handle.control_tx = control_tx;
            install_registry(&mut srv, test_registry(&[]));

            dispatch_prompt(&mut srv, "/compact", false, &RequestId::Number(32))
                .await
                .unwrap();
            let _pending = out_rx.recv_async().await.unwrap();
            let control = control_rx.recv_async().await.unwrap();
            let maki_agent::headless::InteractiveControl::ManualCompaction { output, .. } = control
            else {
                panic!("expected manual compaction control");
            };
            output
                .send_async(maki_agent::headless::ManualCompactionEvent::Failed(
                    "save failed".into(),
                ))
                .await
                .unwrap();

            let failed = out_rx.recv_async().await.unwrap();
            let terminal = out_rx.recv_async().await.unwrap();
            assert_eq!(failed["params"]["update"]["status"], "failed");
            assert_eq!(terminal["id"], 32);
            assert_eq!(terminal["error"]["data"], "save failed");
            assert!(out_rx.is_empty(), "failed compaction completes once");
        });
    }

    #[test]
    fn btw_streams_and_completes_active_prompt() {
        smol::block_on(async {
            let (mut srv, _, out_rx, input_rx) = server_awaiting_answer();
            let (control_tx, control_rx) = flume::unbounded();
            srv.session.as_mut().unwrap().handle.control_tx = control_tx;
            install_registry(&mut srv, test_registry(&[]));
            let request_id = RequestId::Number(31);

            dispatch_prompt(&mut srv, "/btw why?", true, &request_id)
                .await
                .unwrap();
            assert!(input_rx.is_empty(), "isolated turns bypass primary input");
            let control = control_rx.recv_async().await.unwrap();
            let maki_agent::headless::InteractiveControl::IsolatedTurn {
                question,
                images,
                output,
                ..
            } = control
            else {
                panic!("expected isolated turn control");
            };
            assert_eq!(question, "why?");
            assert_eq!(images.len(), 1);

            use maki_agent::agent::isolated_turn::IsolatedTurnEvent;
            output
                .send_async(IsolatedTurnEvent::ThinkingDelta("thought".into()))
                .await
                .unwrap();
            output
                .send_async(IsolatedTurnEvent::TextDelta("answer".into()))
                .await
                .unwrap();
            output.send_async(IsolatedTurnEvent::Done).await.unwrap();

            let thought = out_rx.recv_async().await.unwrap();
            let answer = out_rx.recv_async().await.unwrap();
            let terminal = out_rx.recv_async().await.unwrap();
            assert_eq!(
                thought["params"]["update"]["sessionUpdate"],
                "agent_thought_chunk"
            );
            assert_eq!(
                answer["params"]["update"]["sessionUpdate"],
                "agent_message_chunk"
            );
            assert_eq!(terminal["id"], 31);
            assert_eq!(terminal["result"]["stopReason"], "end_turn");
            assert!(out_rx.is_empty(), "operation terminates exactly once");
        });
    }

    #[test]
    fn unknown_slash_prompt_is_sent_to_agent_literal() {
        let (mut srv, _, _, input_rx) = server_awaiting_answer();
        smol::block_on(dispatch_prompt(
            &mut srv,
            "/does-not-exist value",
            false,
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

        smol::block_on(dispatch_prompt(
            &mut srv,
            "/project:review src",
            false,
            &RequestId::Number(2),
        ))
        .unwrap();
        assert_eq!(input_rx.try_recv().unwrap().message, "Review src");
    }

    #[test]
    fn unavailable_interactive_command_is_sent_to_agent_literal() {
        let (mut srv, _, _, input_rx) = server_awaiting_answer();

        smol::block_on(dispatch_prompt(
            &mut srv,
            "/help",
            true,
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

        let error = smol::block_on(dispatch_prompt(
            &mut srv,
            "/model",
            false,
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

        let error = smol::block_on(dispatch_prompt(
            &mut srv,
            "/compact",
            true,
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

        smol::block_on(dispatch_prompt(
            &mut srv,
            "/project:review src",
            true,
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
        let session_id = srv.session.as_ref().unwrap().handle.session_id.to_string();
        let raw = serde_json::json!({
            "params": {
                "sessionId": session_id,
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

        smol::block_on(dispatch_prompt(
            &mut srv,
            "/lua-complete",
            false,
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
        let error = smol::block_on(dispatch_prompt(
            &mut srv,
            "/btw",
            false,
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
    fn isolated_btw_is_not_forwarded_to_primary_agent() {
        let (mut srv, _, _, input_rx) = server_awaiting_answer();
        let error = smol::block_on(dispatch_prompt(
            &mut srv,
            "/btw explain this",
            false,
            &RequestId::Number(4),
        ))
        .unwrap_err();
        assert_eq!(error.code, AcpError::internal_error().code);
        assert!(input_rx.is_empty());
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
    fn load_history_restores_projected_option_metadata() {
        let tmp = TempDir::new().unwrap();
        let dir = StateDir::from_path(tmp.path().to_path_buf());
        let mut session: Session<Message, TokenUsage, maki_agent::ToolOutput> =
            Session::new("anthropic/test-model", "/project");
        session.meta.yolo = true;
        session.meta.fast = true;
        session.meta.workflow = true;
        session
            .meta
            .session_options
            .insert("bash.auto_mode".into(), "enabled".into());
        session.save(&dir).unwrap();

        let restored = load_history_from(&dir, session.id).unwrap();

        assert!(restored.meta.yolo);
        assert!(restored.meta.fast);
        assert!(restored.meta.workflow);
        assert_eq!(
            restored
                .meta
                .session_options
                .get("bash.auto_mode")
                .map(String::as_str),
            Some("enabled")
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
