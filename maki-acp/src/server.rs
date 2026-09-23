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
    RequestPermissionResponse, Response, SessionConfigOptionValue, SessionId, SessionModeId,
    SessionNotification, SessionUpdate, SetSessionConfigOptionRequest,
    SetSessionConfigOptionResponse, SetSessionModeRequest, SetSessionModeResponse, StopReason,
    TextContent, ToolCallStatus, UnstructuredCommandInput,
};
use flume::{Receiver, Sender, WeakSender};
use maki_agent::headless::{self, InteractiveHandle, InteractiveParams};
use maki_agent::mcp::config::{RawHttpFields, RawStdioFields, RawTransport};
use maki_agent::mcp::{self, McpHandle};
use maki_agent::permissions::{PermissionAnswer, TaggedAnswer};
use maki_agent::tools::QuestionMode;
use maki_agent::types::AgentEvent;
use maki_agent::{AgentInput, AgentMode, Envelope, ImageMediaType, ImageSource};
use maki_commands::{
    AgentTurn, CommandAttachment, CommandContent, CommandOutcome, InputDispatch, PresentedCommand,
    TargetHandle,
};
use maki_config::project::{self, TrustAnswer, TrustMode, policy_grant};
use maki_config::{MAX_SERVER_NAME_LEN, ModelPolicy, ProjectConfig, SessionDefaults, TrustConfig};
use maki_providers::model::Model;
use maki_providers::provider::{available_model_specs, fetch_all_models};
use maki_providers::{Message, TokenUsage, add_cost, settle_session};
use maki_storage::StateDir;
use maki_storage::id::{MakiId, SessionRef};
use maki_storage::session_lock;
use maki_storage::sessions::{SESSIONS_DIR, StoredTokenUsage};
use serde::Serialize;
use serde_json::Value;
use smol::io::AsyncBufReadExt;
use tracing::{debug, info, warn};

use crate::{AcpParams, methods, permissions, translate};

const FIRST_OUTGOING_REQUEST_ID: i64 = 1000;
const CANCELLATION_IN_PROGRESS_CODE: i32 = -32001;
const CANCELLATION_IN_PROGRESS_MESSAGE: &str =
    "session cancellation is still in progress; retry the prompt";
const NEW_SESSION_GUIDANCE: &str = "Start a new conversation using the client’s new-session action. This conversation has not been changed.";
const ACTIVE_OPERATION_MESSAGE: &str = "session already has an active operation";
const AUTH_FAILED_MSG: &str =
    "Authentication failed. Run `maki auth login`, then send the prompt again.";

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
    run_id: Option<u64>,
    cancelling: bool,
    cancel: Option<maki_agent::cancel::CancelTrigger>,
    _lease: Option<maki_agent::session_coordinator::SessionLease>,
}

/// What the client still owes us. Subagents have independent answer channels,
/// so more than one permission request can be outstanding.
#[derive(Default)]
struct Pending {
    operation: Option<PendingOperation>,
    retired_primary_run: Option<u64>,
    permissions: HashMap<i64, (String, Sender<String>)>,
    elicitation: Option<i64>,
}

type PendingState = Arc<Mutex<Pending>>;

/// A session's cross-process lock: the heartbeat thread that keeps it fresh
/// and where to release it. Dropping stops the thread and releases the lock;
/// the join in the drop guarantees no beat lands after the release, which
/// also covers process shutdown after stdin EOF, where `close_session` never
/// runs.
struct SessionLock {
    stop_tx: flume::Sender<()>,
    thread: Option<std::thread::JoinHandle<()>>,
    publication_guard: Option<maki_storage::session_lock::SessionPublicationGuard>,
}

impl SessionLock {
    /// Stop the heartbeat thread, wait until it cannot beat again, and release the lock.
    fn shutdown(mut self) {
        self.release();
    }

    fn publication_guard(&self) -> Option<maki_storage::session_lock::SessionPublicationGuard> {
        self.publication_guard.clone()
    }

    fn release(&mut self) {
        let _ = self.stop_tx.send(());
        if self
            .thread
            .take()
            .is_some_and(|thread| thread.join().is_err())
        {
            warn!("session lock heartbeat thread terminated abnormally");
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
    supports_boolean: bool,
    emitted_version: Mutex<u64>,
}

impl OptionProjection {
    fn apply(&self, snapshot: &maki_agent::session_options::SessionOptionsSnapshot) {
        self.update(snapshot, false);
    }

    fn emit(&self, snapshot: &maki_agent::session_options::SessionOptionsSnapshot) {
        self.update(snapshot, true);
    }

    fn update(&self, snapshot: &maki_agent::session_options::SessionOptionsSnapshot, emit: bool) {
        let mut emitted_version = self.emitted_version.lock().unwrap();
        if snapshot.version <= *emitted_version {
            return;
        }
        self.apply_committed_options(snapshot);
        if emit {
            emit_config_options(
                &self.out_tx,
                &self.session_id,
                snapshot,
                self.supports_boolean,
            );
        }
        *emitted_version = snapshot.version;
    }

    fn apply_committed_options(
        &self,
        snapshot: &maki_agent::session_options::SessionOptionsSnapshot,
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
                Ok(model) => self.command_state.set_model(&model),
                Err(error) => warn!(%error, %spec, "committed model could not be parsed"),
            }
        }
        self.permissions.set_yolo(
            value(maki_agent::session_options::YOLO_OPTION_ID)
                == Some(maki_agent::session_options::ENABLED_VALUE),
        );
        if let Err(error) = self.command_state.set_fast(
            value(maki_agent::session_options::FAST_OPTION_ID)
                == Some(maki_agent::session_options::ENABLED_VALUE),
        ) {
            warn!(%error, "committed Fast value could not be applied");
        }
        self.command_state.set_workflow(
            value(maki_agent::session_options::WORKFLOW_OPTION_ID)
                == Some(maki_agent::session_options::ENABLED_VALUE),
        );
    }

    fn emit_current(&self) {
        let snapshot = self.read.options();
        self.emit(&snapshot);
    }
}

struct SessionState {
    handle: InteractiveHandle,
    coordinator: Option<maki_agent::session_coordinator::SessionCoordinatorHandle>,
    checkpoint: Option<Arc<maki_agent::session_checkpoint::SessionLogCheckpoint>>,
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
    defaults: SessionDefaults,
    project_config: ProjectConfig,
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
    thinking: maki_agent::ThinkingConfig,
    persisted_options: &'a BTreeMap<String, String>,
}

struct Server {
    out_tx: Sender<Value>,
    model_specs: Vec<String>,
    modes: Arc<maki_agent::ModeRegistry>,
    session: Option<SessionState>,
    /// Whether the client advertised form elicitation support at `initialize`.
    elicitation: bool,
    supports_boolean: bool,
    lua_event_handle: maki_lua::EventHandle,
    defaults: SessionDefaults,
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
        supports_boolean: false,
        lua_event_handle: params.lua_event_handle.clone(),
        defaults: params.defaults,
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

    close_session(&mut server).await;
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
            srv.supports_boolean = supports_boolean_config(raw);
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
    respond_request(srv, method, id, result);
}

fn respond_request(
    srv: &Server,
    method: &str,
    id: RequestId,
    result: Result<AgentResponse, AcpError>,
) {
    let session_started = matches!(method, "session/new" | "session/load") && result.is_ok();
    srv.respond(id, result);
    if session_started {
        emit_current_commands(srv);
    }
}

fn trusted_project_config(
    cwd: &Path,
    storage: &StateDir,
    mode: TrustMode,
    policy: &TrustConfig,
) -> ProjectConfig {
    let mut decision = project::resolve(storage, cwd, mode);
    let matched = decision
        .state
        .unanswered()
        .and_then(|question| policy_grant(question, policy));
    if let Some(pattern) = matched {
        // ACP has no card, so policy is the only yes a cwd with no stored
        // decision can get. Recorded like any other yes so `maki trust list`
        // shows what this server trusted on the client's behalf. `unanswered`
        // and not `question`: a recorded `Never` is a stored decision, and
        // granting over it would wipe the rejection out of the store.
        info!(%pattern, cwd = %cwd.display(), "ACP folder trusted by trust.paths policy");
        decision = project::apply_answer(storage, decision, TrustAnswer::Trust);
    }
    // ACP never asks, so the restriction notice is part of what it reports.
    for warning in decision.notices() {
        warn!(%warning, "ACP project configuration trust warning");
    }
    decision.project_config
}

async fn new_session(
    srv: &mut Server,
    raw: &Value,
    params: &AcpParams,
) -> Result<AgentResponse, AcpError> {
    let req: NewSessionRequest = parse_params(raw)?;
    close_session(srv).await;
    let cwd = req.cwd.clone();
    let project_config = trusted_project_config(
        &req.cwd,
        &params.storage,
        params.trust_mode,
        &params.trust_policy,
    );
    let mcp = start_mcp(&req.cwd, &req.mcp_servers, project_config.clone(), params).await;
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
            defaults: params.defaults,
            project_config,
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
            thinking: maki_agent::ThinkingConfig::Off,
            persisted_options: &persisted_options,
        },
    )
    .await
    .map_err(session_registration_error)?;
    let resp = methods::new_session_response(&session_id, &srv.modes).config_options(
        methods::session_config_options(&snapshot, srv.supports_boolean),
    );
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
    let mut restored = load_history_from(&params.storage, session_ref.id())?;
    close_session(srv).await;
    let session_cwd = effective_session_cwd(restored.cwd.as_deref(), &req.cwd);
    let project_config = trusted_project_config(
        &session_cwd,
        &params.storage,
        params.trust_mode,
        &params.trust_policy,
    );
    let mcp = start_mcp(
        &session_cwd,
        &req.mcp_servers,
        project_config.clone(),
        params,
    )
    .await;
    let sid = SessionId::from(session_ref.to_string());
    let home = maki_storage::paths::home();
    let recorded_model = match Model::from_spec(&restored.model) {
        Ok(model) if params.model_policy.allows(&model.spec()) => model,
        _ => params.model.clone(),
    };
    let spec = recorded_model.spec();
    let fast = restored.meta.fast && recorded_model.supports_fast();
    let yolo = restored.meta.yolo.unwrap_or(params.yolo);
    let workflow = restored.meta.workflow;
    let thinking = restored
        .meta
        .thinking
        .map(maki_agent::ThinkingConfig::from)
        .filter(|_| recorded_model.supports_thinking())
        .unwrap_or_default();
    let replay_history = restored.history.clone();
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
            defaults: {
                let mut d = params.defaults;
                d.workflow |= workflow;
                d.fast |= fast;
                d
            },
            project_config,
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
            cwd: session_cwd.clone(),
            fast,
            workflow,
            thinking,
            persisted_options: &restored.meta.session_options,
        },
    )
    .await
    .map_err(session_registration_error)?;
    for update in translate::replay_history(&replay_history, &session_cwd, home.as_deref()) {
        session_update(&srv.out_tx, &sid, update);
    }
    let resp = methods::load_session_response(&srv.modes).config_options(
        methods::session_config_options(&snapshot, srv.supports_boolean),
    );
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
        defaults,
        project_config,
    } = session;
    let permissions_config = maki_config::load_permissions(&project_config);
    headless::spawn_interactive(InteractiveParams {
        model,
        config: params.config.clone(),
        permissions_config,
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
        defaults,
        model_policy: Arc::clone(&params.model_policy),
        question_mode: if elicitation {
            QuestionMode::Elicitation
        } else {
            QuestionMode::Headless
        },
        plugin_rules: Arc::clone(&params.plugin_rules),
        project_config,
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

/// MCP is per session: its effective cwd selects project config and the client may inject servers.
/// Returns as soon as the config is read, the first prompt waits for the tools.
async fn start_mcp(
    cwd: &Path,
    servers: &[McpServer],
    project_config: ProjectConfig,
    params: &AcpParams,
) -> Option<McpHandle> {
    let (handle, errors) = mcp::start_with_extra_and_commands(
        cwd,
        project_config,
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
    // The event pump dies with the session, so the requests it owed answers to
    // have to be answered here or the client waits on them forever.
    let (operation, permission_answers, elicitation) = {
        let mut pending = state.pending.lock().unwrap();
        (
            pending.operation.as_mut().map(|operation| {
                operation.cancelling = true;
                (operation.id, operation.kind)
            }),
            std::mem::take(&mut pending.permissions),
            pending.elicitation.take(),
        )
    };
    if let Some((operation_id, kind)) = operation {
        finish_operation(&state.pending, operation_id, kind, |request_id| {
            respond_prompt(&srv.out_tx, request_id, StopReason::Cancelled);
        });
    }
    answer_cancelled_requests(&state.handle, permission_answers, elicitation);
    state.handle.task.cancel().await;
    if let Some(coordinator) = state.coordinator.take()
        && let Err(error) = coordinator.close().await
    {
        warn!(%error, "failed to drain session coordinator");
    }
    if let Some(checkpoint) = state.checkpoint
        && let Err(error) = checkpoint.drain().await
    {
        warn!(%error, "failed to drain session checkpoint");
    }
    state.command_projection_task.cancel().await;
    state.option_projection_task.cancel().await;
    if let Some(mcp) = state.mcp {
        mcp.shutdown().await;
    }
    if let Some(lock) = state.lock.take() {
        lock.shutdown();
    }
}

fn start_session_lock_in(dir: PathBuf, id: MakiId) -> Result<SessionLock, String> {
    let mut lease = session_lock::claim(&dir, &id)
        .map_err(|error| error.to_string())?
        .ok_or_else(|| session_lock::OPEN_ELSEWHERE_MSG.to_owned())?;
    let publication_guard = lease.publication_guard();
    let (stop_tx, stop_rx) = flume::bounded(1);
    let thread = std::thread::spawn(move || {
        loop {
            if stop_rx
                .recv_timeout(session_lock::HEARTBEAT_INTERVAL)
                .is_ok()
            {
                return;
            }
            match lease.heartbeat() {
                Ok(session_lock::LockBeat::Lost) => {
                    warn!(session_id = %id, "session lock heartbeat lost ownership");
                    return;
                }
                Err(error) => warn!(session_id = %id, %error, "session lock heartbeat failed"),
                Ok(session_lock::LockBeat::Claimed | session_lock::LockBeat::Held) => {}
            }
        }
    });
    Ok(SessionLock {
        stop_tx,
        thread: Some(thread),
        publication_guard: Some(publication_guard),
    })
}

async fn rollback_install(
    handle: InteractiveHandle,
    mcp: Option<McpHandle>,
    coordinator: Option<maki_agent::session_coordinator::SessionCoordinatorHandle>,
) {
    if let Some(coordinator) = coordinator {
        coordinator.retire();
    }
    handle.task.cancel().await;
    if let Some(mcp) = mcp {
        mcp.shutdown().await;
    }
}

async fn install_session(
    srv: &mut Server,
    params: &AcpParams,
    session: InstallSession<'_>,
) -> Result<maki_agent::session_options::SessionOptionsSnapshot, String> {
    let sessions_dir = match params.storage.ensure_subdir(SESSIONS_DIR) {
        Ok(sessions_dir) => sessions_dir,
        Err(error) => {
            let InstallSession { handle, mcp, .. } = session;
            rollback_install(handle, mcp, None).await;
            return Err(error.to_string());
        }
    };
    install_session_with_lock(srv, params, session, |id| {
        start_session_lock_in(sessions_dir, id)
    })
    .await
}

async fn install_session_with_lock(
    srv: &mut Server,
    params: &AcpParams,
    session: InstallSession<'_>,
    lock_session: impl FnOnce(MakiId) -> Result<SessionLock, String>,
) -> Result<maki_agent::session_options::SessionOptionsSnapshot, String> {
    let InstallSession {
        handle,
        mcp,
        current_model,
        history,
        initial_cost,
        cwd,
        fast,
        workflow,
        thinking,
        persisted_options,
    } = session;
    let definitions = maki_agent::session_coordinator::builtin_option_definitions(
        Arc::from(current_model.as_str()),
        srv.model_specs.iter().map(|spec| Arc::from(spec.as_str())),
        handle.permissions.is_yolo(),
        fast,
        workflow,
        thinking,
    );
    let lock = match lock_session(handle.session_id.id()) {
        Ok(lock) => lock,
        Err(error) => {
            warn!(%error, "session lock claim failed");
            rollback_install(handle, mcp, None).await;
            return Err(error);
        }
    };
    let checkpoint = match match lock.publication_guard() {
        Some(publication_guard) => {
            maki_agent::session_checkpoint::SessionLogCheckpoint::open_owned(
                params.storage.clone(),
                handle.session_id.id(),
                &current_model,
                &cwd.to_string_lossy(),
                publication_guard,
            )
        }
        None => maki_agent::session_checkpoint::SessionLogCheckpoint::open(
            params.storage.clone(),
            handle.session_id.id(),
            &current_model,
            &cwd.to_string_lossy(),
        ),
    } {
        Ok(checkpoint) => Arc::new(checkpoint),
        Err(error) => {
            warn!(%error, "failed to open session checkpoint");
            rollback_install(handle, mcp, None).await;
            return Err(error.to_string());
        }
    };
    let coordinator = match maki_agent::session_coordinator::SessionCoordinatorHandle::register(
        maki_agent::session_coordinator::SessionCoordinatorParams {
            session_id: handle.session_id.id(),
            catalog: params.lua_event_handle.session_option_catalog(),
            definitions,
            persisted_options: persisted_options.clone(),
            history,
            model: Arc::from(current_model.as_str()),
            cwd: cwd.clone(),
            model_policy: Arc::clone(&params.model_policy),
            model_adopter: Arc::new({
                // Installing into the shared source rather than asking the
                // session loop to adopt: the loop only reads its control
                // channel between turns, so a round-trip here would wait for
                // the running turn -- and that turn cannot finish while the
                // coordinator is blocked on this call. The store also lands on
                // the run's next request instead of its next turn.
                let shared = handle.model.clone();
                let timeouts = params.timeouts;
                move |mut model: Model| {
                    let shared = shared.clone();
                    Box::pin(async move {
                        let provider =
                            maki_providers::provider::from_model_async(&mut model, timeouts)
                                .await
                                .map_err(|error| Arc::from(error.user_message()))?;
                        shared.install(Arc::from(provider), model);
                        Ok(())
                    }) as maki_agent::session_coordinator::ModelAdoptionFuture
                }
            }),
            directory_adopter: maki_agent::headless::interactive_directory_adopter(
                handle.control_tx.clone(),
            ),
            checkpoint: checkpoint.clone(),
            mailbox: handle.mailbox.clone(),
        },
    ) {
        Ok(coordinator) => coordinator,
        Err(error) => {
            warn!(%error, "failed to register session coordinator");
            rollback_install(handle, mcp, None).await;
            return Err(error.to_string());
        }
    };
    let pending = PendingState::default();
    let project_trusted = handle.permissions.project_is_trusted();
    start_event_pump(
        handle.event_rx.clone(),
        handle.session_id.clone(),
        srv.out_tx.clone(),
        Arc::clone(&pending),
        srv.elicitation,
        handle.answer_tx.clone(),
        handle.cancel_tx.clone(),
        coordinator.read(),
        maki_storage::paths::home(),
        project_trusted,
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
    let portable_capabilities = maki_agent::command::portable_capabilities();
    let command_target = command_registry.bind_target_with_presentation(
        portable_capabilities.union(maki_commands::TargetCapabilities::from_capability(
            maki_commands::TargetCapability::SessionReplacement,
        )),
        portable_capabilities,
        Arc::new(
            maki_agent::command::SessionCommandHost::new(
                handle.control_tx.clone(),
                Arc::clone(&command_state),
            )
            .with_reset_session_guidance(NEW_SESSION_GUIDANCE)
            .with_coordinator(coordinator.clone()),
        ),
    );
    let session_id = SessionId::from(handle.session_id.to_string());
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
        supports_boolean: srv.supports_boolean,
        emitted_version: Mutex::new(option_snapshot.version),
    });
    let option_projection_task = watch_config_options(
        Arc::clone(&option_projection),
        coordinator.read().subscribe(),
    );
    srv.session = Some(SessionState {
        handle,
        coordinator: Some(coordinator),
        checkpoint: Some(checkpoint),
        mcp,
        current_mode: AgentMode::Build,
        command_state,
        pending,
        command_registry,
        command_target,
        command_projection_task,
        option_projection: Some(option_projection),
        option_projection_task,
        lock: Some(lock),
    });
    Ok(option_snapshot)
}

fn emit_current_commands(srv: &Server) {
    let Some(session) = &srv.session else { return };
    let commands = session
        .command_registry
        .presented_commands(&session.command_target)
        .unwrap_or_default();
    emit_available_commands(
        &srv.out_tx,
        &SessionId::from(session.handle.session_id.to_string()),
        &commands,
    );
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

fn emit_config_options(
    out_tx: &Sender<Value>,
    session_id: &SessionId,
    snapshot: &maki_agent::session_options::SessionOptionsSnapshot,
    supports_boolean: bool,
) {
    session_update(
        out_tx,
        session_id,
        SessionUpdate::ConfigOptionUpdate(ConfigOptionUpdate::new(
            methods::session_config_options(snapshot, supports_boolean),
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
    admit_prompt(&session.pending)?;
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
                srv.defaults,
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
            send_command_turn(session, id, turn, srv.defaults).await
        }
        InputDispatch::Dispatched(CommandOutcome::IsolatedTurn(turn)) => {
            send_isolated_turn(session, &srv.out_tx, id, turn).await
        }
        InputDispatch::Dispatched(CommandOutcome::ManualCompaction(instructions)) => {
            send_manual_compaction(session, &srv.out_tx, id, instructions).await
        }
        InputDispatch::Dispatched(CommandOutcome::FrontendFeedback(feedback)) => {
            let text = match feedback {
                maki_commands::FrontendFeedback::WorkingDirectory(path) => {
                    format!("Working directory: {}", path.display())
                }
                maki_commands::FrontendFeedback::Text(text) => text.to_string(),
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
    instructions: Option<String>,
) -> Result<(), AcpError> {
    if session.pending.lock().unwrap().operation.is_some() {
        return Err(AcpError::new(-32600, ACTIVE_OPERATION_MESSAGE));
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
            return Err(AcpError::new(-32600, ACTIVE_OPERATION_MESSAGE));
        }
        pending.operation = Some(PendingOperation {
            id: operation_id,
            request_id: id.clone(),
            kind: OperationKind::ManualCompaction,
            run_id: None,
            cancelling: false,
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
            instructions,
            output,
            cancel,
            lease_committer,
        })
        .is_err()
    {
        release_operation(
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
                    finish_operation(
                        &pending,
                        operation_id,
                        OperationKind::ManualCompaction,
                        |request_id| {
                            session_update(
                                &out_tx,
                                &sid,
                                translate::local_operation_terminal(&tool_id, None),
                            );
                            respond_prompt(&out_tx, request_id, StopReason::EndTurn);
                        },
                    );
                    break;
                }
                ManualCompactionEvent::Cancelled => {
                    finish_operation(
                        &pending,
                        operation_id,
                        OperationKind::ManualCompaction,
                        |request_id| {
                            session_update(
                                &out_tx,
                                &sid,
                                translate::local_operation_terminal(&tool_id, Some("cancelled")),
                            );
                            respond_prompt(&out_tx, request_id, StopReason::Cancelled);
                        },
                    );
                    break;
                }
                ManualCompactionEvent::Failed(error) => {
                    finish_operation(
                        &pending,
                        operation_id,
                        OperationKind::ManualCompaction,
                        |request_id| {
                            session_update(
                                &out_tx,
                                &sid,
                                translate::local_operation_terminal(&tool_id, Some(&error)),
                            );
                            let error = AcpError::internal_error().data(Value::String(error));
                            send(
                                &out_tx,
                                Response::new(request_id, Err::<AgentResponse, _>(error)),
                            );
                        },
                    );
                    break;
                }
            }
        }
        finish_operation(
            &pending,
            operation_id,
            OperationKind::ManualCompaction,
            |request_id| {
                session_update(
                    &out_tx,
                    &sid,
                    translate::local_operation_terminal(&tool_id, Some("event stream ended")),
                );
                let error = AcpError::internal_error()
                    .data(Value::String("manual compaction event stream ended".into()));
                send(
                    &out_tx,
                    Response::new(request_id, Err::<AgentResponse, _>(error)),
                );
            },
        );
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
        return Err(AcpError::new(-32600, ACTIVE_OPERATION_MESSAGE));
    }
    let images = turn
        .content
        .attachments
        .iter()
        .map(|attachment| {
            ImageSource::new(
                image_media_type(&attachment.media_type),
                Arc::clone(&attachment.data),
            )
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
            return Err(AcpError::new(-32600, ACTIVE_OPERATION_MESSAGE));
        }
        pending.operation = Some(PendingOperation {
            id: operation_id,
            request_id: id.clone(),
            kind: OperationKind::IsolatedTurn,
            run_id: None,
            cancelling: false,
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
        release_operation(&session.pending, operation_id, OperationKind::IsolatedTurn);
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
                    finish_operation(
                        &pending,
                        operation_id,
                        OperationKind::IsolatedTurn,
                        |request_id| respond_prompt(&out_tx, request_id, StopReason::EndTurn),
                    );
                    break;
                }
                IsolatedTurnEvent::Cancelled => {
                    finish_operation(
                        &pending,
                        operation_id,
                        OperationKind::IsolatedTurn,
                        |request_id| respond_prompt(&out_tx, request_id, StopReason::Cancelled),
                    );
                    break;
                }
                IsolatedTurnEvent::Error(message) => {
                    finish_operation(
                        &pending,
                        operation_id,
                        OperationKind::IsolatedTurn,
                        |request_id| {
                            let error = AcpError::internal_error().data(Value::String(message));
                            send(
                                &out_tx,
                                Response::new(request_id, Err::<AgentResponse, _>(error)),
                            );
                        },
                    );
                    break;
                }
            }
        }
        finish_operation(
            &pending,
            operation_id,
            OperationKind::IsolatedTurn,
            |request_id| {
                let error = AcpError::internal_error()
                    .data(Value::String("isolated turn event stream ended".into()));
                send(
                    &out_tx,
                    Response::new(request_id, Err::<AgentResponse, _>(error)),
                );
            },
        );
    })
    .detach();
    Ok(())
}

async fn send_command_turn(
    session: &SessionState,
    id: &RequestId,
    turn: AgentTurn,
    mut defaults: SessionDefaults,
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
        .map(|attachment| {
            ImageSource::new(
                image_media_type(&attachment.media_type),
                Arc::clone(&attachment.data),
            )
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
    let (cancel_trigger, cancel) = maki_agent::cancel::CancelToken::new();
    defaults.fast |= enabled(maki_agent::session_options::FAST_OPTION_ID);
    defaults.workflow |= enabled(maki_agent::session_options::WORKFLOW_OPTION_ID);
    let mut input = agent_input(
        turn.content.text.to_string(),
        images,
        session.current_mode.clone(),
        defaults,
        prompt,
    );
    if session.pending.lock().unwrap().operation.is_some() {
        return Err(AcpError::new(-32600, ACTIVE_OPERATION_MESSAGE));
    }
    let lease = session
        .coordinator
        .as_ref()
        .ok_or_else(no_session)?
        .acquire_lease()
        .await
        .map_err(coordinator_error)?;
    input.cancel = Some(cancel);
    input.lease_committer = lease.committer();
    let operation_id = NEXT_OPERATION_ID.fetch_add(1, Ordering::Relaxed);
    {
        let mut pending = session.pending.lock().unwrap();
        if pending.operation.is_some() {
            return Err(AcpError::new(-32600, ACTIVE_OPERATION_MESSAGE));
        }
        pending.operation = Some(PendingOperation {
            id: operation_id,
            request_id: id.clone(),
            kind: OperationKind::PrimaryTurn,
            run_id: None,
            cancelling: false,
            cancel: Some(cancel_trigger),
            _lease: Some(lease),
        });
    }
    if session.handle.input_tx.send(input).is_err() {
        release_operation(&session.pending, operation_id, OperationKind::PrimaryTurn);
        return Err(AcpError::new(-32603, "session ended"));
    }
    Ok(())
}

fn agent_input(
    message: String,
    images: Vec<ImageSource>,
    mode: AgentMode,
    defaults: SessionDefaults,
    prompt: Option<maki_agent::McpPromptRef>,
) -> AgentInput {
    let mut input = AgentInput::from_defaults(message, mode, images, defaults);
    input.prompt = prompt.map(Box::new);
    input
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
            | SessionOptionError::PolicyRejected(_)
            | SessionOptionError::CallbackFailed(_),
        ) => AcpError::invalid_params().data(json_str(&error.to_string())),
        _ => AcpError::internal_error().data(json_str(&error.to_string())),
    }
}

fn admit_prompt(pending: &PendingState) -> Result<(), AcpError> {
    match pending.lock().unwrap().operation.as_ref() {
        None => Ok(()),
        Some(operation) if operation.cancelling => Err(AcpError::new(
            CANCELLATION_IN_PROGRESS_CODE,
            CANCELLATION_IN_PROGRESS_MESSAGE,
        )
        .data(serde_json::json!({ "retryable": true }))),
        Some(_) => Err(AcpError::new(-32600, ACTIVE_OPERATION_MESSAGE)),
    }
}

fn finish_operation(
    pending: &PendingState,
    operation_id: u64,
    kind: OperationKind,
    respond: impl FnOnce(RequestId),
) -> bool {
    let operation = {
        let mut pending = pending.lock().unwrap();
        if !pending
            .operation
            .as_ref()
            .is_some_and(|operation| operation.id == operation_id && operation.kind == kind)
        {
            return false;
        }
        pending.permissions.clear();
        pending.elicitation = None;
        pending.operation.take().unwrap()
    };
    let request_id = operation.request_id.clone();
    drop(operation);
    respond(request_id);
    true
}

fn finish_primary_run(
    pending: &PendingState,
    run_id: u64,
    respond: impl FnOnce(RequestId),
) -> bool {
    let operation = {
        let mut pending = pending.lock().unwrap();
        if pending
            .retired_primary_run
            .is_some_and(|retired_run| run_id <= retired_run)
        {
            return false;
        }
        let Some(operation) = pending.operation.as_mut() else {
            return false;
        };
        if operation.kind != OperationKind::PrimaryTurn {
            return false;
        }
        match operation.run_id {
            Some(operation_run_id) if operation_run_id != run_id => return false,
            None => operation.run_id = Some(run_id),
            Some(_) => {}
        }
        pending.retired_primary_run = Some(run_id);
        pending.permissions.clear();
        pending.elicitation = None;
        pending.operation.take().unwrap()
    };
    let request_id = operation.request_id.clone();
    drop(operation);
    respond(request_id);
    true
}

fn finish_active_operation(
    pending: &PendingState,
    kind: OperationKind,
    respond: impl FnOnce(RequestId),
) -> bool {
    let operation_id = pending
        .lock()
        .unwrap()
        .operation
        .as_ref()
        .filter(|operation| operation.kind == kind)
        .map(|operation| operation.id);
    operation_id.is_some_and(|id| finish_operation(pending, id, kind, respond))
}

fn release_operation(pending: &PendingState, operation_id: u64, kind: OperationKind) {
    finish_operation(pending, operation_id, kind, |_| {});
}

#[cfg(test)]
fn take_operation(
    pending: &PendingState,
    operation_id: u64,
    kind: OperationKind,
) -> Option<RequestId> {
    let mut request_id = None;
    finish_operation(pending, operation_id, kind, |id| request_id = Some(id));
    request_id
}

#[cfg(test)]
fn take_active_operation(pending: &PendingState, kind: OperationKind) -> Option<RequestId> {
    let mut request_id = None;
    finish_active_operation(pending, kind, |id| request_id = Some(id));
    request_id
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
    let value = match req.value {
        SessionConfigOptionValue::ValueId { value } => value.0.to_string(),
        SessionConfigOptionValue::Boolean { value } => String::from(if value {
            maki_agent::session_options::ENABLED_VALUE
        } else {
            maki_agent::session_options::DISABLED_VALUE
        }),
        _ => return Err(AcpError::invalid_params()),
    };
    let session = srv.session.as_mut().ok_or_else(no_session)?;
    let coordinator = session.coordinator.as_ref().ok_or_else(no_session)?;
    let snapshot = srv
        .lua_event_handle
        .set_session_option(coordinator.clone(), config_id.as_str(), value.as_str())
        .await
        .map_err(|error| match error {
            maki_lua::SessionOptionMutationError::Coordinator(error) => coordinator_error(error),
            maki_lua::SessionOptionMutationError::HostDead => {
                AcpError::internal_error().data(json_str(&error.to_string()))
            }
        })?;
    let projection = session.option_projection.as_ref().ok_or_else(no_session)?;
    projection.apply(&snapshot);
    Ok(AgentResponse::SetSessionConfigOptionResponse(
        SetSessionConfigOptionResponse::new(methods::session_config_options(
            &snapshot,
            srv.supports_boolean,
        )),
    ))
}

fn effective_session_cwd(restored: Option<&Path>, client: &Path) -> PathBuf {
    restored.unwrap_or(client).to_path_buf()
}

fn answer_cancelled_requests(
    handle: &InteractiveHandle,
    permission_answers: HashMap<i64, (String, Sender<String>)>,
    elicitation: Option<i64>,
) {
    for (request_id, answer_tx) in permission_answers.into_values() {
        let _ = answer_tx.send(TaggedAnswer::new(request_id, PermissionAnswer::Deny).encode());
    }
    if elicitation.is_some() {
        let _ = handle
            .answer_tx
            .send(serde_json::json!({ "dismissed": true }).to_string());
    }
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
                let (cancellation, permission_answers, elicitation) = {
                    let mut pending = session.pending.lock().unwrap();
                    let Some(operation) = pending
                        .operation
                        .as_mut()
                        .filter(|operation| !operation.cancelling)
                    else {
                        return;
                    };
                    operation.cancelling = true;
                    let cancellation = operation.cancel.take();
                    let permission_answers = std::mem::take(&mut pending.permissions);
                    let elicitation = pending.elicitation.take();
                    (cancellation, permission_answers, elicitation)
                };
                answer_cancelled_requests(&session.handle, permission_answers, elicitation);
                if let Some(trigger) = cancellation {
                    trigger.cancel();
                } else {
                    let _ = session.handle.cancel_tx.try_send(());
                }
            }
        }
        _ => debug!(method, "unknown notification"),
    }
}

fn ask_id(id: &Value) -> Option<i64> {
    id.as_i64()
        .or_else(|| id.as_str().and_then(|s| s.parse().ok()))
}

fn handle_incoming_response(srv: &Server, raw: &Value) {
    let Some(session) = &srv.session else { return };
    let id_num = raw.get("id").and_then(ask_id);
    let answer = {
        let mut pending = session.pending.lock().unwrap();
        if let Some(id) = id_num
            && pending
                .elicitation
                .take_if(|pending| *pending == id)
                .is_some()
        {
            Some((session.handle.answer_tx.clone(), elicitation_answer(raw)))
        } else if let Some(id) = id_num
            && let Some((request_id, answer_tx)) = pending.permissions.remove(&id)
        {
            Some((
                answer_tx,
                TaggedAnswer::new(request_id, permission_answer(raw)).encode(),
            ))
        } else {
            warn!(?id_num, "response for an unknown request id");
            None
        }
    };
    if let Some((answer_tx, answer)) = answer {
        let _ = answer_tx.send(answer);
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
    cancel_tx: Sender<()>,
    session: maki_agent::session_coordinator::SessionReadHandle,
    home: Option<PathBuf>,
    project_trusted: bool,
    initial_cost: Option<f64>,
) {
    smol::spawn(async move {
        let sid = SessionId::from(session_id.to_string());
        let mut cost_total = initial_cost;
        let mut tool_inputs: HashMap<String, Value> = HashMap::new();

        while let Ok(Envelope {
            event,
            subagent,
            run_id,
        }) = event_rx.recv_async().await
        {
            if let AgentEvent::TurnComplete(tc) = &event {
                add_cost(&mut cost_total, tc.cost);
            }
            let permission_answer_tx = if let Some(subagent) = subagent {
                if !matches!(&event, AgentEvent::PermissionRequest { .. }) {
                    continue;
                }
                let Some(answer_tx) = subagent.answer_tx else {
                    warn!(agent_id = %subagent.agent_id, "subagent permission request has no answer channel");
                    continue;
                };
                Some(answer_tx)
            } else {
                None
            };

            let update = match event {
                AgentEvent::TextDelta { text } => translate::text_delta(&text),
                AgentEvent::ThinkingDelta { text } => translate::thinking_delta(&text),
                AgentEvent::ThinkingBlockEnd => translate::thinking_block_end(),
                AgentEvent::ToolPending { id, name } => translate::tool_pending(&id, &name),
                AgentEvent::ToolStart(event) => {
                    let update = translate::tool_start(&event, &session.cwd(), home.as_deref());
                    if let Some(raw_input) = &event.raw_input {
                        tool_inputs.insert(event.id.clone(), raw_input.clone());
                    }
                    update
                }
                AgentEvent::ToolExecutionStart { id } => translate::tool_execution_start(&id),
                AgentEvent::ToolOutput { id, content } => translate::tool_output(&id, &content),
                AgentEvent::ToolDone(event) => {
                    translate::tool_done(&event, &session.cwd(), home.as_deref())
                }
                AgentEvent::TurnComplete(event) => translate::usage_update(&event, cost_total),
                AgentEvent::PermissionRequest { id, tool, scopes } => {
                    let raw_input =
                        permission_answer_tx.is_none().then(|| tool_inputs.get(&id)).flatten();
                    let request =
                        AgentRequest::RequestPermissionRequest(RequestPermissionRequest::new(
                            sid.clone(),
                            translate::permission_request(
                                &id,
                                format!("{tool}: {}", scopes.join(", ")),
                                &tool.to_string(),
                                raw_input,
                                &session.cwd(),
                                home.as_deref(),
                            ),
                            permissions::permission_options(project_trusted),
                        ));
                    let request_id = NEXT_OUTGOING_REQUEST_ID.fetch_add(1, Ordering::Relaxed);
                    let should_send = {
                        let mut pending = pending.lock().unwrap();
                        if pending
                            .operation
                            .as_ref()
                            .is_some_and(|operation| operation.cancelling)
                        {
                            false
                        } else {
                            pending.permissions.insert(
                                request_id,
                                (
                                    id.clone(),
                                    permission_answer_tx
                                        .clone()
                                        .unwrap_or_else(|| answer_tx.clone()),
                                ),
                            );
                            true
                        }
                    };
                    if should_send {
                        send_with_pending_tool_status(
                            &out_tx,
                            Request {
                                id: RequestId::Number(request_id),
                                method: Arc::from(request.method()),
                                params: Some(request),
                            },
                            "/params/toolCall",
                        );
                    } else {
                        let answer_tx = permission_answer_tx.unwrap_or_else(|| answer_tx.clone());
                        let _ = answer_tx.send(TaggedAnswer::new(&id, PermissionAnswer::Deny).encode());
                    }
                    continue;
                }
                AgentEvent::AuthRequired => {
                    tool_inputs.clear();
                    finish_active_operation(&pending, OperationKind::PrimaryTurn, |request_id| {
                        let error = AcpError::auth_required().data(Value::String(AUTH_FAILED_MSG.into()));
                        send(&out_tx, Response::<AgentResponse>::new(request_id, Err(error)));
                    });
                    let _ = cancel_tx.try_send(());
                    continue;
                }
                AgentEvent::Question { id, questions } => {
                    if elicitation
                        && let Some(request) = crate::elicitation::build_form(&questions, &sid, &id)
                    {
                        let request = AgentRequest::CreateElicitationRequest(request);
                        let request_id = NEXT_OUTGOING_REQUEST_ID.fetch_add(1, Ordering::Relaxed);
                        let should_send = {
                            let mut pending = pending.lock().unwrap();
                            if pending
                                .operation
                                .as_ref()
                                .is_some_and(|operation| operation.cancelling)
                            {
                                false
                            } else {
                                pending.elicitation = Some(request_id);
                                true
                            }
                        };
                        if should_send {
                            send(
                                &out_tx,
                                Request {
                                    id: RequestId::Number(request_id),
                                    method: Arc::from(request.method()),
                                    params: Some(request),
                                },
                            );
                        } else {
                            let _ = answer_tx
                                .send(serde_json::json!({ "dismissed": true }).to_string());
                        }
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
                    tool_inputs.clear();
                    finish_primary_run(&pending, run_id, |request_id| {
                        match outcome {
                            maki_agent::TurnOutcome::Completed { reason, .. } => {
                                let resp = PromptResponse::new(translate::map_done_reason(reason));
                                send(
                                    &out_tx,
                                    Response::new(
                                        request_id,
                                        Ok(AgentResponse::PromptResponse(resp)),
                                    ),
                                );
                            }
                            maki_agent::TurnOutcome::Cancelled { .. } => {
                                respond_prompt(&out_tx, request_id, StopReason::Cancelled);
                            }
                            maki_agent::TurnOutcome::Failed { failure, .. } => {
                                let error = AcpError::internal_error()
                                    .data(Value::String(failure.user_message));
                                send(
                                    &out_tx,
                                    Response::<AgentResponse>::new(request_id, Err(error)),
                                );
                            }
                        }
                    });
                    continue;
                }
                AgentEvent::ControlComplete { .. } => {
                    tool_inputs.clear();
                    finish_primary_run(&pending, run_id, |request_id| {
                        respond_prompt(&out_tx, request_id, StopReason::EndTurn)
                    });
                    continue;
                }
                AgentEvent::ControlError { message } => {
                    tool_inputs.clear();
                    finish_primary_run(&pending, run_id, |request_id| {
                        let error = AcpError::internal_error().data(Value::String(message));
                        send(
                            &out_tx,
                            Response::<AgentResponse>::new(request_id, Err(error)),
                        );
                    });
                    continue;
                }
                _ => continue,
            };
            session_update(&out_tx, &sid, update);
        }
        finish_active_operation(&pending, OperationKind::PrimaryTurn, |request_id| {
            let error =
                AcpError::internal_error().data(Value::String("session event stream ended".into()));
            send(
                &out_tx,
                Response::<AgentResponse>::new(request_id, Err(error)),
            );
        });
    })
    .detach();
}

fn send(out_tx: &Sender<Value>, msg: impl Serialize) {
    if let Ok(json) = serde_json::to_value(JsonRpcMessage::wrap(msg)) {
        let _ = out_tx.send(json);
    }
}

fn send_with_pending_tool_status(out_tx: &Sender<Value>, msg: impl Serialize, pointer: &str) {
    if let Ok(mut json) = serde_json::to_value(JsonRpcMessage::wrap(msg)) {
        if let Some(tool_call) = json.pointer_mut(pointer).and_then(Value::as_object_mut) {
            tool_call.insert("status".into(), Value::String("pending".into()));
        }
        let _ = out_tx.send(json);
    }
}

fn session_update(out_tx: &Sender<Value>, sid: &SessionId, update: SessionUpdate) {
    let pending = matches!(
        &update,
        SessionUpdate::ToolCall(tool_call) if tool_call.status == ToolCallStatus::Pending
    ) || matches!(
        &update,
        SessionUpdate::ToolCallUpdate(tool_call) if tool_call.fields.status == Some(ToolCallStatus::Pending)
    );
    let notification =
        AgentNotification::SessionNotification(SessionNotification::new(sid.clone(), update));
    let notification = Notification {
        method: Arc::from("session/update"),
        params: Some(notification),
    };
    if pending {
        send_with_pending_tool_status(out_tx, notification, "/params/update");
    } else {
        send(out_tx, notification);
    }
}

fn no_session() -> AcpError {
    AcpError::new(-32600, "no active session")
}

fn session_registration_error(error: String) -> AcpError {
    AcpError::internal_error().data(json_str(&error))
}

fn supports_boolean_config(raw: &Value) -> bool {
    raw.pointer("/params/clientCapabilities/session/configOptions/boolean")
        .is_some_and(Value::is_object)
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
    const PRIMARY_TURN_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(2);
    const LIFECYCLE_STRESS_ITERATIONS: usize = 64;
    const SPAWN_TEST_SPEC: &str = "ollama/acp-end-turn-test";

    struct EndTurnProvider;

    impl maki_providers::provider::Provider for EndTurnProvider {
        fn stream_message<'a>(
            &'a self,
            _: &'a Model,
            _: &'a [Message],
            _: &'a str,
            _: &'a Value,
            _: &'a Sender<maki_providers::ProviderEvent>,
            _: maki_providers::RequestOptions,
            _: Option<&'a SessionRef>,
        ) -> maki_providers::provider::BoxFuture<
            'a,
            Result<maki_providers::StreamResponse, maki_agent::AgentError>,
        > {
            Box::pin(async {
                Ok(maki_providers::StreamResponse {
                    message: Message {
                        role: maki_providers::Role::Assistant,
                        content: vec![maki_providers::ContentBlock::Text {
                            text: "done".into(),
                        }],
                        ..Default::default()
                    },
                    usage: TokenUsage::default(),
                    stop_reason: Some(maki_providers::StopReason::EndTurn),
                })
            })
        }

        fn list_models(
            &self,
        ) -> maki_providers::provider::BoxFuture<
            '_,
            Result<Vec<maki_providers::ModelInfo>, maki_agent::AgentError>,
        > {
            Box::pin(async { Ok(Vec::new()) })
        }
    }

    fn test_params(model: Model, cwd: PathBuf) -> AcpParams {
        AcpParams {
            model,
            config: Default::default(),
            permissions_config: Default::default(),
            timeouts: Default::default(),
            initial_wd: cwd.clone(),
            storage: StateDir::from_path(cwd),
            prompt_slots: Arc::default(),
            modes: Arc::default(),
            yolo: false,
            defaults: Default::default(),
            system_prompt_override: Some(String::new()),
            append_system_prompt: None,
            model_policy: Arc::default(),
            plugin_rules: Arc::default(),
            lua_event_handle: maki_lua::EventHandle::disconnected_for_test(),
            command_registry: test_registry(&[]),
            trust_mode: TrustMode::Consult,
            trust_policy: Arc::default(),
        }
    }

    const PUMP_TRUSTED: bool = true;

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
        control_tx: Sender<maki_agent::headless::InteractiveControl>,
        command_state: Arc<maki_agent::command::SessionCommandState>,
    ) -> TargetHandle {
        {
            let portable_capabilities = maki_agent::command::portable_capabilities();
            registry.bind_target_with_presentation(
                portable_capabilities.union(maki_commands::TargetCapabilities::from_capability(
                    maki_commands::TargetCapability::SessionReplacement,
                )),
                portable_capabilities,
                Arc::new(
                    maki_agent::command::SessionCommandHost::new(control_tx, command_state)
                        .with_reset_session_guidance(NEW_SESSION_GUIDANCE),
                ),
            )
        }
    }

    fn successful_checkpoint() -> Arc<
        dyn maki_storage::checkpoint::CheckpointWriter<
                maki_agent::session_coordinator::SessionCheckpoint,
            >,
    > {
        Arc::new(|request: maki_storage::checkpoint::CheckpointRequest<_>| {
            Box::pin(async move {
                Ok(maki_storage::checkpoint::CheckpointAck {
                    session_id: request.session_id,
                    version: request.version,
                })
            }) as maki_storage::checkpoint::CheckpointFuture
        })
    }

    fn test_coordinator(
        session_id: MakiId,
        model: &str,
        cwd: PathBuf,
    ) -> maki_agent::session_coordinator::SessionCoordinatorHandle {
        test_coordinator_with(
            session_id,
            model,
            cwd,
            Default::default(),
            successful_checkpoint(),
        )
    }

    fn test_coordinator_with(
        session_id: MakiId,
        model: &str,
        cwd: PathBuf,
        catalog: maki_agent::session_coordinator::SessionOptionCatalog,
        checkpoint: Arc<
            dyn maki_storage::checkpoint::CheckpointWriter<
                    maki_agent::session_coordinator::SessionCheckpoint,
                >,
        >,
    ) -> maki_agent::session_coordinator::SessionCoordinatorHandle {
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
                catalog,
                definitions: maki_agent::session_coordinator::builtin_option_definitions(
                    model,
                    model_specs,
                    false,
                    false,
                    false,
                    maki_agent::ThinkingConfig::Off,
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

    #[test_case(serde_json::json!({}) => false ; "missing")]
    #[test_case(serde_json::json!({ "params": { "clientCapabilities": { "session": { "configOptions": { "boolean": {} } } } } }) => true ; "object")]
    #[test_case(serde_json::json!({ "params": { "clientCapabilities": { "session": { "configOptions": { "boolean": true } } } } }) => false ; "non_object")]
    fn boolean_config_capability_requires_object(raw: Value) -> bool {
        supports_boolean_config(&raw)
    }

    fn server_awaiting_answer() -> (
        Server,
        Receiver<String>,
        Receiver<Value>,
        Receiver<AgentInput>,
    ) {
        server_awaiting_answer_with_checkpoint(successful_checkpoint())
    }

    fn server_awaiting_answer_with_checkpoint(
        checkpoint: Arc<
            dyn maki_storage::checkpoint::CheckpointWriter<
                    maki_agent::session_coordinator::SessionCheckpoint,
                >,
        >,
    ) -> (
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
            model: Default::default(),
            event_rx: flume::unbounded().1,
            tool_names: Vec::new(),
            input_tx,
            answer_tx: answer_tx.clone(),
            cancel_tx: flume::unbounded().0,
            model_tx: flume::unbounded().0,
            control_tx: flume::unbounded().0,
            session_id: SessionRef::from(session_id),
            mailbox: maki_agent::SessionMailbox::new(session_id),
            permissions: Arc::new(PermissionManager::new(
                maki_config::PermissionsConfig::default(),
                PathBuf::from("/project"),
                ProjectConfig::for_project(Path::new("/project")),
                Arc::default(),
            )),
            task: smol::spawn(async {}),
        };
        let coordinator = test_coordinator_with(
            handle.session_id.id(),
            OFFLINE_SPEC,
            PathBuf::from("/project"),
            Default::default(),
            checkpoint,
        );
        let command_registry = test_registry(&[]);
        let command_state = Arc::new(maki_agent::command::SessionCommandState::new(
            String::new(),
            Arc::from([]),
            PathBuf::from("/project"),
            false,
            false,
        ));
        let portable_capabilities = maki_agent::command::portable_capabilities();
        let command_target = command_registry.bind_target_with_presentation(
            portable_capabilities.union(maki_commands::TargetCapabilities::from_capability(
                maki_commands::TargetCapability::SessionReplacement,
            )),
            portable_capabilities,
            Arc::new(
                maki_agent::command::SessionCommandHost::new(
                    handle.control_tx.clone(),
                    Arc::clone(&command_state),
                )
                .with_reset_session_guidance(NEW_SESSION_GUIDANCE)
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
                    supports_boolean: false,
                    emitted_version: Mutex::new(coordinator.read().options().version),
                })),
                handle,
                coordinator: Some(coordinator),
                checkpoint: None,
                mcp: None,
                current_mode: AgentMode::Build,
                command_state,
                pending: Arc::new(Mutex::new(Pending {
                    permissions: HashMap::from([(ANSWERED_ID, ("toolu_1".to_string(), answer_tx))]),
                    ..Default::default()
                })),
                command_registry,
                command_target,
                command_projection_task: smol::spawn(async {}),
                option_projection_task: smol::spawn(async {}),
                lock: None,
            }),
            elicitation: false,
            supports_boolean: false,
            lua_event_handle: maki_lua::EventHandle::disconnected_for_test(),
            defaults: SessionDefaults::default(),
        };
        (server, answer_rx, out_rx, input_rx)
    }

    #[test]
    fn abnormal_heartbeat_thread_termination_releases_lock() {
        let dir = TempDir::new().unwrap();
        let id = MakiId::generate();
        let lease = session_lock::claim(dir.path(), &id).unwrap().unwrap();
        let (stop_tx, _) = flume::bounded(1);
        let thread = std::thread::spawn(move || {
            let _lease = lease;
            panic!("heartbeat failed");
        });
        let lock = SessionLock {
            stop_tx,
            thread: Some(thread),
            publication_guard: None,
        };

        drop(lock);

        assert!(session_lock::claim(dir.path(), &id).unwrap().is_some());
    }

    #[test]
    fn registration_error_preserves_open_elsewhere_message() {
        let error = session_registration_error(session_lock::OPEN_ELSEWHERE_MSG.to_owned());

        assert_eq!(error.code, AcpError::internal_error().code);
        assert_eq!(
            error.data,
            Some(json_str(&session_lock::OPEN_ELSEWHERE_MSG))
        );
    }

    #[test]
    fn sessions_directory_failure_rolls_back_install() {
        smol::block_on(async {
            let cwd = TempDir::new().unwrap();
            let state_path = cwd.path().join("not-a-directory");
            std::fs::write(&state_path, "file").unwrap();
            let params = test_params(Model::from_spec(OFFLINE_SPEC).unwrap(), state_path);
            let (out_tx, _) = flume::unbounded();
            let mut srv = Server {
                out_tx,
                model_specs: vec![OFFLINE_SPEC.to_owned()],
                modes: Arc::clone(&params.modes),
                session: None,
                elicitation: false,
                supports_boolean: false,
                lua_event_handle: params.lua_event_handle.clone(),
                defaults: SessionDefaults::default(),
            };
            let handle = spawn_session(
                &params,
                SpawnSession {
                    model: params.model.clone(),
                    cwd: cwd.path().to_path_buf(),
                    session_id: None,
                    history: Vec::new(),
                    mcp_handle: None,
                    elicitation: false,
                    yolo: false,
                    defaults: SessionDefaults::default(),
                    project_config: ProjectConfig::for_project(cwd.path()),
                },
            );
            let input_tx = handle.input_tx.clone();
            let persisted_options = BTreeMap::new();

            let error = install_session(
                &mut srv,
                &params,
                InstallSession {
                    handle,
                    mcp: None,
                    current_model: OFFLINE_SPEC.to_owned(),
                    history: Vec::new(),
                    initial_cost: None,
                    cwd: cwd.path().to_path_buf(),
                    fast: false,
                    workflow: false,
                    thinking: maki_agent::ThinkingConfig::Off,
                    persisted_options: &persisted_options,
                },
            )
            .await
            .unwrap_err();

            assert!(!error.is_empty());
            assert!(input_tx.is_disconnected(), "session task was not cancelled");
            assert!(srv.session.is_none());
        });
    }

    #[test]
    fn post_registration_lock_failure_rolls_back_and_same_id_retries() {
        smol::block_on(async {
            let cwd = TempDir::new().unwrap();
            let params = test_params(
                Model::from_spec(OFFLINE_SPEC).unwrap(),
                cwd.path().to_path_buf(),
            );
            let (out_tx, _) = flume::unbounded();
            let mut srv = Server {
                out_tx,
                model_specs: vec![OFFLINE_SPEC.to_owned()],
                modes: Arc::clone(&params.modes),
                session: None,
                elicitation: false,
                supports_boolean: false,
                lua_event_handle: params.lua_event_handle.clone(),
                defaults: SessionDefaults::default(),
            };
            let session_id = SessionRef::from(MakiId::generate());
            let persisted_options = BTreeMap::new();

            let first = spawn_session(
                &params,
                SpawnSession {
                    model: params.model.clone(),
                    cwd: cwd.path().to_path_buf(),
                    session_id: Some(session_id.clone()),
                    history: Vec::new(),
                    mcp_handle: None,
                    elicitation: false,
                    yolo: false,
                    defaults: SessionDefaults::default(),
                    project_config: ProjectConfig::for_project(cwd.path()),
                },
            );
            let error = install_session_with_lock(
                &mut srv,
                &params,
                InstallSession {
                    handle: first,
                    mcp: None,
                    current_model: OFFLINE_SPEC.to_owned(),
                    history: Vec::new(),
                    initial_cost: None,
                    cwd: cwd.path().to_path_buf(),
                    fast: false,
                    workflow: false,
                    thinking: maki_agent::ThinkingConfig::Off,
                    persisted_options: &persisted_options,
                },
                |_| Err(session_lock::OPEN_ELSEWHERE_MSG.to_owned()),
            )
            .await
            .unwrap_err();
            assert_eq!(error, session_lock::OPEN_ELSEWHERE_MSG);
            assert!(srv.session.is_none());
            assert!(
                maki_agent::session_coordinator::SessionCoordinatorHandle::resolve(session_id.id())
                    .is_err()
            );

            let retry = test_coordinator(session_id.id(), OFFLINE_SPEC, cwd.path().to_path_buf());
            retry.close().await.unwrap();
        });
    }

    #[test]
    fn close_retires_routing_with_retained_committer() {
        smol::block_on(async {
            let (mut srv, ..) = server_awaiting_answer();
            let id = srv.session.as_ref().unwrap().handle.session_id.id();
            let coordinator = srv
                .session
                .as_ref()
                .unwrap()
                .coordinator
                .as_ref()
                .unwrap()
                .clone();
            let lease = coordinator.acquire_lease().await.unwrap();
            let committer = lease.committer().unwrap();
            std::mem::forget(lease);

            close_session(&mut srv).await;
            let replacement = test_coordinator(id, OFFLINE_SPEC, PathBuf::from("/project"));

            drop(committer);
            replacement.close().await.unwrap();
        });
    }

    #[test]
    fn close_denies_outstanding_permission_requests() {
        smol::block_on(async {
            let (mut srv, ..) = server_awaiting_answer();
            let pending = Arc::clone(&srv.session.as_ref().unwrap().pending);
            let (permission_tx, permission_rx) = flume::unbounded();
            pending.lock().unwrap().permissions =
                HashMap::from([(ANSWERED_ID, ("perm-id".to_string(), permission_tx))]);

            close_session(&mut srv).await;

            assert_eq!(
                permission_rx.try_recv().ok(),
                Some(TaggedAnswer::new("perm-id", PermissionAnswer::Deny).encode())
            );
            assert!(pending.lock().unwrap().permissions.is_empty());
        });
    }

    #[test]
    fn close_dismisses_outstanding_elicitation() {
        smol::block_on(async {
            let (mut srv, answer_rx, ..) = server_awaiting_answer();
            let pending = Arc::clone(&srv.session.as_ref().unwrap().pending);
            {
                let mut pending = pending.lock().unwrap();
                pending.permissions.clear();
                pending.elicitation = Some(ANSWERED_ID);
            }

            close_session(&mut srv).await;

            assert_eq!(
                answer_rx.try_recv().ok(),
                Some(r#"{"dismissed":true}"#.to_string())
            );
            assert!(pending.lock().unwrap().elicitation.is_none());
        });
    }

    #[test]
    fn operation_terminal_is_compare_and_set() {
        let pending = Arc::new(Mutex::new(Pending {
            operation: Some(PendingOperation {
                id: 7,
                request_id: RequestId::Number(41),
                kind: OperationKind::TestLocal,
                run_id: None,
                cancelling: false,
                cancel: None,
                _lease: None,
            }),
            ..Pending::default()
        }));

        assert!(take_operation(&pending, 6, OperationKind::TestLocal).is_none());
        assert!(take_operation(&pending, 7, OperationKind::PrimaryTurn).is_none());
        let operation = take_operation(&pending, 7, OperationKind::TestLocal).unwrap();
        assert_eq!(operation, RequestId::Number(41));
        assert!(take_operation(&pending, 7, OperationKind::TestLocal).is_none());
    }

    #[test]
    fn operation_is_removed_before_response() {
        let pending = Arc::new(Mutex::new(Pending {
            operation: Some(PendingOperation {
                id: 8,
                request_id: RequestId::Number(42),
                kind: OperationKind::TestLocal,
                run_id: None,
                cancelling: true,
                cancel: None,
                _lease: None,
            }),
            ..Pending::default()
        }));

        assert!(finish_operation(
            &pending,
            8,
            OperationKind::TestLocal,
            |request_id| {
                assert_eq!(request_id, RequestId::Number(42));
                assert!(pending.try_lock().unwrap().operation.is_none());
            },
        ));
    }

    #[test]
    fn primary_terminal_cannot_finish_isolated_operation() {
        let pending = Arc::new(Mutex::new(Pending {
            operation: Some(PendingOperation {
                id: 9,
                request_id: RequestId::Number(43),
                kind: OperationKind::TestLocal,
                run_id: None,
                cancelling: false,
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

            // The lease guards history, so history replacement is what it
            // holds off. An option change is served while the prompt runs and
            // would not observe the lease at all.
            let (done_tx, done_rx) = flume::bounded(1);
            let queued = coordinator.clone();
            smol::spawn(async move {
                let result = queued
                    .replace_history(vec![maki_providers::Message::user("late".into())])
                    .await;
                let _ = done_tx.send(result);
            })
            .detach();
            assert!(done_rx.try_recv().is_err());

            let pending = &srv.session.as_ref().unwrap().pending;
            let operation = take_active_operation(pending, OperationKind::PrimaryTurn).unwrap();
            assert_eq!(operation, request_id);
            drop(operation);
            done_rx.recv_async().await.unwrap().unwrap();
            assert_eq!(coordinator.read().history().len(), 1);
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
                supports_boolean: srv.supports_boolean,
                emitted_version: Mutex::new(coordinator.read().options().version),
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
                Some(5)
            );
            let response = out_rx.recv_async().await.unwrap();
            assert_eq!(response["id"], 42);
            coordinator.close().await.unwrap();
        });
    }

    #[test_case(false, serde_json::json!(maki_agent::session_options::ENABLED_VALUE) ; "legacy_value_id")]
    #[test_case(true, serde_json::json!({ "type": "boolean", "value": true }) ; "boolean_value")]
    fn set_config_accepts_legacy_and_boolean_values(supports_boolean: bool, value: Value) {
        let (mut srv, _, out_rx, _) = server_awaiting_answer();
        srv.supports_boolean = supports_boolean;
        Arc::get_mut(
            srv.session
                .as_mut()
                .unwrap()
                .option_projection
                .as_mut()
                .unwrap(),
        )
        .unwrap()
        .supports_boolean = supports_boolean;
        let coordinator = srv
            .session
            .as_ref()
            .unwrap()
            .coordinator
            .as_ref()
            .unwrap()
            .clone();
        let active_id = srv.session.as_ref().unwrap().handle.session_id.to_string();
        let mut request = serde_json::json!({
            "params": {
                "sessionId": active_id,
                "configId": maki_agent::session_options::YOLO_OPTION_ID,
            }
        });
        if supports_boolean {
            request["params"]["type"] = value["type"].clone();
            request["params"]["value"] = value["value"].clone();
        } else {
            request["params"]["value"] = value;
        }
        let response = smol::block_on(handle_set_config(&mut srv, &request)).unwrap();
        let AgentResponse::SetSessionConfigOptionResponse(response) = response else {
            panic!("expected config option response");
        };
        let wire = serde_json::to_value(&response.config_options[1]).unwrap();
        assert_eq!(
            wire["type"],
            if supports_boolean {
                "boolean"
            } else {
                "select"
            }
        );
        assert!(srv.session.as_ref().unwrap().handle.permissions.is_yolo());
        assert!(out_rx.is_empty());
        smol::block_on(coordinator.close()).unwrap();
    }

    #[test]
    fn acp_rejected_plugin_option_does_not_mutate_or_checkpoint() {
        const OPTION_ID: &str = "choice.value";
        const REJECTED_VALUE: &str = "rejected";
        const SOURCE: &str = r#"
            maki.api.register_session_option({
                id = "choice.value",
                name = "Choice",
                description = "Test choice",
                category = "mode",
                values = {
                    { value = "accepted", name = "Accepted" },
                    { value = "rejected", name = "Rejected" },
                },
                initial_value = "accepted",
                validate = function(value)
                    if value == "rejected" then return false, "rejected by test" end
                    return true
                end,
            })
        "#;

        let (mut srv, _, out_rx, _) = server_awaiting_answer();
        let old = srv.session.as_ref().unwrap().coordinator.as_ref().unwrap();
        let session_id = old.read().session_id();
        smol::block_on(old.close()).unwrap();

        let host =
            maki_lua::PluginHost::new(Arc::new(maki_agent::tools::ToolRegistry::new())).unwrap();
        host.load_source("choice", SOURCE).unwrap();
        let lua = host.event_handle();
        let checkpoints = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let checkpoint: Arc<
            dyn maki_storage::checkpoint::CheckpointWriter<
                    maki_agent::session_coordinator::SessionCheckpoint,
                >,
        > = {
            let checkpoints = Arc::clone(&checkpoints);
            Arc::new(
                move |request: maki_storage::checkpoint::CheckpointRequest<_>| {
                    checkpoints.fetch_add(1, Ordering::Relaxed);
                    Box::pin(async move {
                        Ok(maki_storage::checkpoint::CheckpointAck {
                            session_id: request.session_id,
                            version: request.version,
                        })
                    }) as maki_storage::checkpoint::CheckpointFuture
                },
            )
        };
        let coordinator = test_coordinator_with(
            session_id,
            OFFLINE_SPEC,
            PathBuf::from("/project"),
            lua.session_option_catalog(),
            checkpoint,
        );
        let before = coordinator.read().options();
        let session = srv.session.as_mut().unwrap();
        session.coordinator = Some(coordinator.clone());
        session.option_projection = Some(Arc::new(OptionProjection {
            out_tx: srv.out_tx.clone(),
            session_id: SessionId::from(session_id.to_string()),
            read: coordinator.read(),
            command_state: Arc::clone(&session.command_state),
            permissions: Arc::clone(&session.handle.permissions),
            supports_boolean: false,
            emitted_version: Mutex::new(before.version),
        }));
        srv.lua_event_handle = lua;

        let error = smol::block_on(handle_set_config(
            &mut srv,
            &serde_json::json!({
                "params": {
                    "sessionId": session_id.to_string(),
                    "configId": OPTION_ID,
                    "value": REJECTED_VALUE,
                }
            }),
        ))
        .unwrap_err();

        assert_eq!(error.code, AcpError::invalid_params().code);
        assert_eq!(
            error.data,
            Some(Value::String(
                "session option callback failed: rejected by test".to_owned()
            ))
        );
        assert_eq!(coordinator.read().options(), before);
        assert_eq!(checkpoints.load(Ordering::Relaxed), 0);
        assert!(out_rx.is_empty());
        smol::block_on(coordinator.close()).unwrap();
    }

    #[test]
    fn set_config_projection_applies_host_state_before_response() {
        let (mut srv, _, out_rx, _) = server_awaiting_answer();
        let coordinator = srv
            .session
            .as_ref()
            .unwrap()
            .coordinator
            .as_ref()
            .unwrap()
            .clone();
        let active_id = srv.session.as_ref().unwrap().handle.session_id.to_string();
        let response = smol::block_on(handle_set_config(
            &mut srv,
            &serde_json::json!({
                "params": {
                    "sessionId": active_id,
                    "configId": maki_agent::session_options::YOLO_OPTION_ID,
                    "value": maki_agent::session_options::ENABLED_VALUE,
                }
            }),
        ))
        .unwrap();
        let AgentResponse::SetSessionConfigOptionResponse(response) = response else {
            panic!("expected config option response");
        };
        assert!(matches!(
            response.config_options[1].kind,
            agent_client_protocol_schema::SessionConfigKind::Select(ref option)
                if option.current_value.to_string() == maki_agent::session_options::ENABLED_VALUE
        ));
        assert!(srv.session.as_ref().unwrap().handle.permissions.is_yolo());
        assert!(out_rx.is_empty(), "direct config set does not emit twice");
        assert_eq!(
            coordinator
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
            maki_agent::session_options::ENABLED_VALUE
        );
        smol::block_on(coordinator.close()).unwrap();
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
        assert_eq!(response.config_options.len(), 5);
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
            Some(TaggedAnswer::new("toolu_1", PermissionAnswer::AllowOnce).encode())
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
            Some(TaggedAnswer::new("toolu_1", PermissionAnswer::AllowOnce).encode()),
            "stale cancellation must not cancel the active session"
        );
    }

    #[test]
    fn cancel_denies_all_outstanding_permission_requests() {
        let (srv, answer_rx, ..) = server_awaiting_answer();
        let pending = &srv.session.as_ref().unwrap().pending;
        let (subagent_answer_tx, subagent_answer_rx) = flume::unbounded();
        let mut pending = pending.lock().unwrap();
        pending.permissions.insert(
            ANSWERED_ID + 1,
            ("sub-perm".to_string(), subagent_answer_tx),
        );
        pending.operation = Some(PendingOperation {
            id: 1,
            request_id: RequestId::Number(41),
            kind: OperationKind::TestLocal,
            run_id: None,
            cancelling: false,
            cancel: None,
            _lease: None,
        });
        drop(pending);
        handle_notification(
            &srv,
            "session/cancel",
            &serde_json::json!({
                "params": {
                    "sessionId": srv.session.as_ref().unwrap().handle.session_id.to_string()
                }
            }),
        );

        assert_eq!(
            answer_rx.try_recv().ok(),
            Some(TaggedAnswer::new("toolu_1", PermissionAnswer::Deny).encode())
        );
        assert_eq!(
            subagent_answer_rx.try_recv().ok(),
            Some(TaggedAnswer::new("sub-perm", PermissionAnswer::Deny).encode())
        );
        handle_incoming_response(&srv, &allow_once(ANSWERED_ID));
        assert!(answer_rx.is_empty(), "the late answer must be ignored");
    }

    #[test]
    fn cancel_dismisses_pending_elicitation_and_rejects_late_answer() {
        let (srv, answer_rx, ..) = server_awaiting_answer();
        let session = srv.session.as_ref().unwrap();
        let mut pending = session.pending.lock().unwrap();
        pending.permissions.clear();
        pending.elicitation = Some(ANSWERED_ID);
        pending.operation = Some(PendingOperation {
            id: 1,
            request_id: RequestId::Number(41),
            kind: OperationKind::TestLocal,
            run_id: None,
            cancelling: false,
            cancel: None,
            _lease: None,
        });
        drop(pending);

        handle_notification(
            &srv,
            "session/cancel",
            &serde_json::json!({
                "params": { "sessionId": session.handle.session_id.to_string() }
            }),
        );

        assert_eq!(
            answer_rx.try_recv().ok(),
            Some(r#"{"dismissed":true}"#.to_string())
        );
        handle_incoming_response(&srv, &allow_once(ANSWERED_ID));
        assert!(answer_rx.is_empty(), "the late answer must be ignored");
    }

    #[test]
    fn cancel_during_permission_allows_the_next_prompt() {
        smol::block_on(async {
            let (mut srv, answer_rx, out_rx, input_rx) = server_awaiting_answer();
            let (event_tx, event_rx) = flume::unbounded::<Envelope>();
            let session = srv.session.as_ref().unwrap();
            session.pending.lock().unwrap().permissions.clear();
            start_event_pump(
                event_rx,
                session.handle.session_id.clone(),
                srv.out_tx.clone(),
                Arc::clone(&session.pending),
                false,
                session.handle.answer_tx.clone(),
                session.handle.cancel_tx.clone(),
                session.coordinator.as_ref().unwrap().read(),
                maki_storage::paths::home(),
                PUMP_TRUSTED,
                None,
            );
            let session_id = session.handle.session_id.to_string();

            handle_prompt(
                &mut srv,
                &prompt_request(&session_id, "first", false),
                &RequestId::Number(41),
            )
            .await
            .unwrap();
            assert_eq!(input_rx.recv_async().await.unwrap().message, "first");
            event_tx
                .send_async(Envelope {
                    event: AgentEvent::PermissionRequest {
                        id: "tool-1".to_string(),
                        tool: maki_config::ToolKey::Native(Arc::from("bash")),
                        scopes: vec!["echo first".to_string()],
                    },
                    subagent: None,
                    run_id: 0,
                })
                .await
                .unwrap();
            let permission = out_rx.recv_async().await.unwrap();
            assert_eq!(permission["method"], "session/request_permission");
            let permission_id = permission["id"].as_i64().unwrap();

            handle_notification(
                &srv,
                "session/cancel",
                &serde_json::json!({ "params": { "sessionId": session_id } }),
            );
            assert_eq!(
                answer_rx.recv_async().await.unwrap(),
                TaggedAnswer::new("tool-1", PermissionAnswer::Deny).encode()
            );
            handle_incoming_response(&srv, &allow_once(permission_id));
            assert!(
                answer_rx.is_empty(),
                "the late permission answer must be dropped"
            );

            event_tx
                .send_async(Envelope {
                    event: AgentEvent::PermissionRequest {
                        id: "tool-stale".to_string(),
                        tool: maki_config::ToolKey::Native(Arc::from("bash")),
                        scopes: vec!["echo stale".to_string()],
                    },
                    subagent: None,
                    run_id: 0,
                })
                .await
                .unwrap();
            assert_eq!(
                answer_rx.recv_async().await.unwrap(),
                TaggedAnswer::new("tool-stale", PermissionAnswer::Deny).encode()
            );
            assert!(
                out_rx.is_empty(),
                "cancelled turn permission must be suppressed"
            );

            let second = prompt_request(&session_id, "second", false);
            let error = handle_prompt(&mut srv, &second, &RequestId::Number(42))
                .await
                .unwrap_err();
            assert_eq!(
                error.code,
                AcpError::new(CANCELLATION_IN_PROGRESS_CODE, "").code
            );
            assert_eq!(error.message, CANCELLATION_IN_PROGRESS_MESSAGE);
            assert_eq!(error.data, Some(serde_json::json!({ "retryable": true })));
            assert!(
                input_rx.is_empty(),
                "successor must not enter the active turn"
            );

            event_tx
                .send_async(Envelope {
                    event: AgentEvent::ControlComplete {
                        usage: TokenUsage::default(),
                    },
                    subagent: None,
                    run_id: 0,
                })
                .await
                .unwrap();
            let cancelled = out_rx.recv_async().await.unwrap();
            assert_eq!(cancelled["id"], 41);
            assert_eq!(cancelled["result"]["stopReason"], "end_turn");
            handle_prompt(&mut srv, &second, &RequestId::Number(43))
                .await
                .unwrap();
            assert_eq!(input_rx.recv_async().await.unwrap().message, "second");
        });
    }

    #[test]
    fn new_session_response_precedes_available_commands() {
        let (srv, _, out_rx, _) = server_awaiting_answer();
        let session_id = srv.session.as_ref().unwrap().handle.session_id.to_string();
        let response = methods::new_session_response(&session_id, &srv.modes);

        respond_request(
            &srv,
            "session/new",
            RequestId::Number(43),
            Ok(AgentResponse::NewSessionResponse(response)),
        );

        let response = out_rx.recv().unwrap();
        assert_eq!(response["id"], 43);
        let update = out_rx.recv().unwrap();
        assert_eq!(
            update["params"]["update"]["sessionUpdate"],
            "available_commands_update"
        );
        assert!(
            update["params"]["update"]["availableCommands"]
                .as_array()
                .unwrap()
                .iter()
                .any(|command| command["name"] == "compact")
        );
    }

    #[test]
    fn cancelled_prompt_rejects_follow_up_and_processes_new_session() {
        smol::block_on(async {
            let (mut srv, _, out_rx, input_rx) = server_awaiting_answer();
            let old_session_id = srv.session.as_ref().unwrap().handle.session_id.to_string();
            let params = test_params(
                Model::from_spec(OFFLINE_SPEC).unwrap(),
                PathBuf::from("/project"),
            );
            handle_prompt(
                &mut srv,
                &prompt_request(&old_session_id, "first", false),
                &RequestId::Number(41),
            )
            .await
            .unwrap();
            input_rx.recv_async().await.unwrap();

            handle_line(
                &mut srv,
                &serde_json::json!({
                    "jsonrpc": "2.0",
                    "method": "session/cancel",
                    "params": { "sessionId": old_session_id }
                })
                .to_string(),
                &params,
            )
            .await;
            handle_line(
                &mut srv,
                &serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": 42,
                    "method": "session/prompt",
                    "params": {
                        "sessionId": old_session_id,
                        "prompt": [{ "type": "text", "text": "second" }]
                    }
                })
                .to_string(),
                &params,
            )
            .await;
            let rejected = out_rx.recv_async().await.unwrap();
            assert_eq!(rejected["id"], 42);
            assert_eq!(rejected["error"]["code"], CANCELLATION_IN_PROGRESS_CODE);
            assert_eq!(
                rejected["error"]["message"],
                CANCELLATION_IN_PROGRESS_MESSAGE
            );
            assert_eq!(rejected["error"]["data"]["retryable"], true);

            let cwd = TempDir::new().unwrap();
            let params = test_params(
                Model::from_spec(OFFLINE_SPEC).unwrap(),
                cwd.path().to_owned(),
            );
            handle_line(
                &mut srv,
                &serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": 43,
                    "method": "session/new",
                    "params": { "cwd": cwd.path(), "mcpServers": [] }
                })
                .to_string(),
                &params,
            )
            .await;

            let mut new_session_response = None;
            while let Ok(message) = out_rx.try_recv() {
                if message["id"] == 43 {
                    new_session_response = Some(message);
                    break;
                }
            }
            let response = new_session_response.expect("new session must receive a response");
            assert_eq!(response["id"], 43);
            assert!(
                srv.session
                    .as_ref()
                    .is_none_or(|session| session.handle.session_id.to_string() != old_session_id),
                "the cancelled session must be closed even if replacement setup fails: {response}"
            );
            close_session(&mut srv).await;
        });
    }

    #[test]
    fn primary_completion_with_reusable_subagent_activity_survives_cancellation_stress() {
        smol::block_on(async {
            for iteration in 0..LIFECYCLE_STRESS_ITERATIONS {
                let (mut srv, _, out_rx, input_rx) = server_awaiting_answer();
                let (event_tx, event_rx) = flume::unbounded::<Envelope>();
                let session = srv.session.as_ref().unwrap();
                session.pending.lock().unwrap().permissions.clear();
                start_event_pump(
                    event_rx,
                    session.handle.session_id.clone(),
                    srv.out_tx.clone(),
                    Arc::clone(&session.pending),
                    false,
                    session.handle.answer_tx.clone(),
                    session.handle.cancel_tx.clone(),
                    session.coordinator.as_ref().unwrap().read(),
                    maki_storage::paths::home(),
                    PUMP_TRUSTED,
                    None,
                );
                let session_id = session.handle.session_id.to_string();
                let request_id = RequestId::Number(iteration as i64 + 1);

                handle_prompt(
                    &mut srv,
                    &prompt_request(&session_id, "primary", false),
                    &request_id,
                )
                .await
                .unwrap();
                input_rx.recv_async().await.unwrap();
                for _ in 0..2 {
                    event_tx
                        .send_async(subagent_activity(AgentEvent::ControlComplete {
                            usage: TokenUsage::default(),
                        }))
                        .await
                        .unwrap();
                }
                smol::future::yield_now().await;
                assert!(
                    out_rx.is_empty(),
                    "subagent completion terminated primary; iteration={iteration}; {}",
                    lifecycle_state(&srv)
                );

                if iteration % 2 == 0 {
                    handle_notification(
                        &srv,
                        "session/cancel",
                        &serde_json::json!({ "params": { "sessionId": session_id } }),
                    );
                }
                let event = if iteration % 2 == 0 {
                    AgentEvent::TurnOutcome(maki_agent::TurnOutcome::cancelled(
                        maki_agent::AgentId::generate(),
                        maki_agent::TurnId::generate(),
                        TokenUsage::default(),
                        1,
                        maki_agent::TurnCancellationReason::User,
                    ))
                } else {
                    AgentEvent::ControlComplete {
                        usage: TokenUsage::default(),
                    }
                };
                event_tx
                    .send_async(Envelope {
                        event,
                        subagent: None,
                        run_id: 0,
                    })
                    .await
                    .unwrap();
                let terminal = out_rx.recv_async().await.unwrap();
                let expected = if iteration % 2 == 0 {
                    "cancelled"
                } else {
                    "end_turn"
                };
                assert_eq!(
                    terminal["result"]["stopReason"],
                    expected,
                    "iteration={iteration}; {}",
                    lifecycle_state(&srv)
                );
                handle_notification(
                    &srv,
                    "session/cancel",
                    &serde_json::json!({ "params": { "sessionId": session_id } }),
                );
                assert!(
                    srv.session
                        .as_ref()
                        .unwrap()
                        .pending
                        .lock()
                        .unwrap()
                        .operation
                        .is_none(),
                    "completed operation remained pending; iteration={iteration}; {}",
                    lifecycle_state(&srv)
                );
                assert!(
                    out_rx.is_empty(),
                    "completion or late cancellation responded twice; iteration={iteration}; {}",
                    lifecycle_state(&srv)
                );
                close_session(&mut srv).await;
            }
        });
    }

    #[test]
    fn cancel_during_completed_turn_checkpoint_does_not_cancel_next_prompt() {
        smol::block_on(async {
            let (checkpoint_started_tx, checkpoint_started_rx) = flume::bounded(1);
            let (checkpoint_release_tx, checkpoint_release_rx) = flume::bounded(1);
            let checkpoint: Arc<
                dyn maki_storage::checkpoint::CheckpointWriter<
                        maki_agent::session_coordinator::SessionCheckpoint,
                    >,
            > = Arc::new(
                move |request: maki_storage::checkpoint::CheckpointRequest<_>| {
                    let checkpoint_started_tx = checkpoint_started_tx.clone();
                    let checkpoint_release_rx = checkpoint_release_rx.clone();
                    Box::pin(async move {
                        checkpoint_started_tx.send_async(()).await.unwrap();
                        checkpoint_release_rx.recv_async().await.unwrap();
                        Ok(maki_storage::checkpoint::CheckpointAck {
                            session_id: request.session_id,
                            version: request.version,
                        })
                    }) as maki_storage::checkpoint::CheckpointFuture
                },
            );
            let (mut srv, _, out_rx, input_rx) = server_awaiting_answer_with_checkpoint(checkpoint);
            let session = srv.session.as_ref().unwrap();
            let pending = Arc::clone(&session.pending);
            let session_id = session.handle.session_id.to_string();
            let (event_tx, event_rx) = flume::unbounded();
            start_event_pump(
                event_rx,
                session.handle.session_id.clone(),
                srv.out_tx.clone(),
                Arc::clone(&pending),
                false,
                session.handle.answer_tx.clone(),
                session.handle.cancel_tx.clone(),
                session.coordinator.as_ref().unwrap().read(),
                None,
                PUMP_TRUSTED,
                None,
            );

            handle_prompt(
                &mut srv,
                &prompt_request(&session_id, "first", false),
                &RequestId::Number(41),
            )
            .await
            .unwrap();
            let first = input_rx.recv_async().await.unwrap();
            let checkpoint_task = smol::spawn(async move {
                first
                    .lease_committer
                    .unwrap()
                    .commit_history(vec![Message::user("first".into())])
                    .await
                    .unwrap();
                event_tx
                    .send_async(Envelope {
                        event: AgentEvent::ControlComplete {
                            usage: TokenUsage::default(),
                        },
                        subagent: None,
                        run_id: 0,
                    })
                    .await
                    .unwrap();
            });
            checkpoint_started_rx.recv_async().await.unwrap();

            handle_notification(
                &srv,
                "session/cancel",
                &serde_json::json!({ "params": { "sessionId": session_id } }),
            );
            checkpoint_release_tx.send_async(()).await.unwrap();
            checkpoint_task.await;
            let terminal = out_rx.recv_async().await.unwrap();
            assert_eq!(terminal["id"], 41);
            assert_eq!(terminal["result"]["stopReason"], "end_turn");

            handle_prompt(
                &mut srv,
                &prompt_request(&session_id, "second", false),
                &RequestId::Number(42),
            )
            .await
            .unwrap();
            let second = input_rx.recv_async().await.unwrap();
            assert_eq!(second.message, "second");
            assert!(!second.cancel.unwrap().is_cancelled());
            take_active_operation(&pending, OperationKind::PrimaryTurn).unwrap();
        });
    }

    #[test]
    fn idle_and_duplicate_cancel_do_not_poison_the_next_prompt() {
        smol::block_on(async {
            let (mut srv, _, _, input_rx) = server_awaiting_answer();
            let session_id = srv.session.as_ref().unwrap().handle.session_id.to_string();
            let cancel = serde_json::json!({ "params": { "sessionId": session_id } });

            handle_notification(&srv, "session/cancel", &cancel);
            handle_prompt(
                &mut srv,
                &prompt_request(&session_id, "first", false),
                &RequestId::Number(41),
            )
            .await
            .unwrap();
            let first = input_rx.recv_async().await.unwrap();
            assert_eq!(first.message, "first");
            let first_cancel = first.cancel.unwrap();

            handle_notification(&srv, "session/cancel", &cancel);
            handle_notification(&srv, "session/cancel", &cancel);
            assert!(
                first_cancel.is_cancelled(),
                "active operation must be cancelled"
            );
            let pending = &srv.session.as_ref().unwrap().pending;
            take_active_operation(pending, OperationKind::PrimaryTurn).unwrap();

            handle_prompt(
                &mut srv,
                &prompt_request(&session_id, "second", false),
                &RequestId::Number(42),
            )
            .await
            .unwrap();
            let second = input_rx.recv_async().await.unwrap();
            assert_eq!(second.message, "second");
            assert!(
                !second.cancel.unwrap().is_cancelled(),
                "next prompt must not inherit cancellation"
            );
        });
    }

    #[test]
    fn elicitation_answer_is_routed_by_id_and_decoded() {
        let (srv, answer_rx, ..) = server_awaiting_answer();
        {
            let mut pending = srv.session.as_ref().unwrap().pending.lock().unwrap();
            pending.permissions.clear();
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
    fn cancelled_operation_dismisses_late_question() {
        let (event_tx, event_rx) = flume::unbounded::<Envelope>();
        let (out_tx, out_rx) = flume::unbounded::<Value>();
        let (answer_tx, answer_rx) = flume::unbounded::<String>();
        let pending = Arc::new(Mutex::new(Pending {
            operation: Some(PendingOperation {
                id: 1,
                request_id: RequestId::Number(41),
                kind: OperationKind::TestLocal,
                run_id: None,
                cancelling: true,
                cancel: None,
                _lease: None,
            }),
            ..Default::default()
        }));
        let session_id = SessionRef::from(MakiId::generate());
        let coordinator = test_coordinator(session_id.id(), OFFLINE_SPEC, PathBuf::from("."));

        start_event_pump(
            event_rx,
            session_id,
            out_tx,
            pending,
            true,
            answer_tx,
            flume::bounded(1).0,
            coordinator.read(),
            maki_storage::paths::home(),
            PUMP_TRUSTED,
            None,
        );
        event_tx
            .send(Envelope {
                event: AgentEvent::Question {
                    id: "t1".to_string(),
                    questions: serde_json::json!([{
                        "question": "Continue?",
                        "header": "Confirm",
                        "options": [{ "label": "Yes", "description": "Continue" }],
                        "multiSelect": false
                    }]),
                },
                subagent: None,
                run_id: 0,
            })
            .unwrap();

        assert_eq!(
            answer_rx
                .recv_timeout(std::time::Duration::from_millis(200))
                .unwrap(),
            r#"{"dismissed":true}"#
        );
        assert!(
            out_rx.is_empty(),
            "cancelled turn must not solicit the client"
        );
        smol::block_on(coordinator.close()).unwrap();
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
            flume::bounded(1).0,
            coordinator.read(),
            maki_storage::paths::home(),
            PUMP_TRUSTED,
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
    fn late_checkpoint_error_cannot_terminate_next_prompt() {
        smol::block_on(async {
            const OLD_RUN_ID: u64 = 7;
            const NEXT_RUN_ID: u64 = 8;

            let (event_tx, event_rx) = flume::unbounded::<Envelope>();
            let (out_tx, out_rx) = flume::unbounded::<Value>();
            let (answer_tx, _) = flume::unbounded::<String>();
            let pending = Arc::new(Mutex::new(Pending {
                operation: Some(PendingOperation {
                    id: 1,
                    request_id: RequestId::Number(41),
                    kind: OperationKind::PrimaryTurn,
                    run_id: None,
                    cancelling: false,
                    cancel: None,
                    _lease: None,
                }),
                ..Default::default()
            }));
            let session_id = SessionRef::from(MakiId::generate());
            let coordinator = test_coordinator(session_id.id(), OFFLINE_SPEC, PathBuf::from("."));
            start_event_pump(
                event_rx,
                session_id,
                out_tx,
                Arc::clone(&pending),
                true,
                answer_tx,
                flume::bounded(1).0,
                coordinator.read(),
                maki_storage::paths::home(),
                PUMP_TRUSTED,
                None,
            );

            let completed = |run_id| Envelope {
                event: AgentEvent::TurnOutcome(maki_agent::TurnOutcome::completed(
                    maki_agent::AgentId::generate(),
                    maki_agent::TurnId::generate(),
                    TokenUsage::default(),
                    1,
                    maki_agent::DoneReason::EndTurn,
                )),
                subagent: None,
                run_id,
            };
            event_tx.send_async(completed(OLD_RUN_ID)).await.unwrap();
            assert_eq!(out_rx.recv_async().await.unwrap()["id"], 41);

            pending.lock().unwrap().operation = Some(PendingOperation {
                id: 2,
                request_id: RequestId::Number(42),
                kind: OperationKind::PrimaryTurn,
                run_id: None,
                cancelling: false,
                cancel: None,
                _lease: None,
            });
            event_tx
                .send_async(Envelope {
                    event: AgentEvent::ControlError {
                        message: "failed to checkpoint completed turn".into(),
                    },
                    subagent: None,
                    run_id: OLD_RUN_ID,
                })
                .await
                .unwrap();
            event_tx.send_async(completed(NEXT_RUN_ID)).await.unwrap();

            let second = out_rx.recv_async().await.unwrap();
            assert_eq!(second["id"], 42);
            assert_eq!(second["result"]["stopReason"], "end_turn");
            assert!(out_rx.is_empty());
            coordinator.close().await.unwrap();
        });
    }

    #[test]
    fn event_pump_projects_tool_permission_lifecycle_in_wire_order() {
        let (event_tx, event_rx) = flume::unbounded::<Envelope>();
        let (out_tx, out_rx) = flume::unbounded::<Value>();
        let (answer_tx, _answer_rx) = flume::unbounded::<String>();
        let pending = Arc::new(Mutex::new(Pending::default()));
        let session_id = SessionRef::from(MakiId::generate());
        let coordinator = test_coordinator(session_id.id(), OFFLINE_SPEC, PathBuf::from("."));
        let tool_id = "tool-1";

        start_event_pump(
            event_rx,
            session_id,
            out_tx,
            pending,
            true,
            answer_tx,
            flume::bounded(1).0,
            coordinator.read(),
            maki_storage::paths::home(),
            PUMP_TRUSTED,
            None,
        );

        for event in [
            AgentEvent::ToolPending {
                id: tool_id.to_string(),
                name: "bash".to_string(),
            },
            AgentEvent::ToolStart(Box::new(maki_agent::ToolStartEvent {
                id: tool_id.to_string(),
                tool: Arc::from("bash"),
                summary: "Run command".to_string(),
                render_header: None,
                annotation: None,
                input: None,
                raw_input: Some(serde_json::json!({ "command": "true" })),
                output: None,
            })),
            AgentEvent::PermissionRequest {
                id: tool_id.to_string(),
                tool: maki_config::ToolKey::native("bash"),
                scopes: vec!["true".to_string()],
            },
            AgentEvent::ToolExecutionStart {
                id: tool_id.to_string(),
            },
        ] {
            event_tx
                .send(Envelope {
                    event,
                    subagent: None,
                    run_id: 0,
                })
                .unwrap();
        }

        smol::block_on(async {
            let pending = out_rx.recv_async().await.unwrap();
            let details = out_rx.recv_async().await.unwrap();
            let permission = out_rx.recv_async().await.unwrap();
            let executing = out_rx.recv_async().await.unwrap();

            assert_eq!(
                pending["params"]["update"]["status"], "pending",
                "{pending}"
            );
            assert_eq!(details["params"]["update"]["toolCallId"], tool_id);
            assert_eq!(details["params"]["update"]["title"], "Run command");
            assert_eq!(details["params"]["update"]["status"], "pending");
            assert_eq!(permission["method"], "session/request_permission");
            assert_eq!(permission["params"]["toolCall"]["toolCallId"], tool_id);
            assert_eq!(permission["params"]["toolCall"]["status"], "pending");
            assert_eq!(executing["params"]["update"]["toolCallId"], tool_id);
            assert_eq!(executing["params"]["update"]["status"], "in_progress");
            assert!(out_rx.is_empty());
        });
        smol::block_on(coordinator.close()).unwrap();
    }

    #[test]
    fn event_pump_includes_subagent_cost_in_next_root_usage_update() {
        const INITIAL_COST: f64 = 0.25;
        const SUBAGENT_COST: f64 = 0.5;
        const ROOT_COST: f64 = 1.0;

        let (event_tx, event_rx) = flume::unbounded::<Envelope>();
        let (out_tx, out_rx) = flume::unbounded::<Value>();
        let (answer_tx, _) = flume::unbounded::<String>();
        let session_id = SessionRef::from(MakiId::generate());
        let coordinator = test_coordinator(session_id.id(), OFFLINE_SPEC, PathBuf::from("."));
        start_event_pump(
            event_rx,
            session_id,
            out_tx,
            PendingState::default(),
            false,
            answer_tx,
            flume::bounded(1).0,
            coordinator.read(),
            None,
            PUMP_TRUSTED,
            Some(INITIAL_COST),
        );
        let turn_complete = |cost| {
            AgentEvent::TurnComplete(Box::new(maki_agent::TurnCompleteEvent {
                message: Message::default(),
                usage: TokenUsage::default(),
                model: OFFLINE_SPEC.to_owned(),
                cost: Some(cost),
                context_size: Some(0),
                context_window: 1,
            }))
        };

        event_tx
            .send(subagent_activity(turn_complete(SUBAGENT_COST)))
            .unwrap();
        event_tx
            .send(Envelope {
                event: turn_complete(ROOT_COST),
                subagent: None,
                run_id: 0,
            })
            .unwrap();

        let update = out_rx
            .recv_timeout(std::time::Duration::from_secs(1))
            .unwrap();
        assert_eq!(update["params"]["update"]["sessionUpdate"], "usage_update");
        assert_eq!(
            update["params"]["update"]["cost"]["amount"],
            INITIAL_COST + SUBAGENT_COST + ROOT_COST
        );
        assert!(out_rx.is_empty(), "subagent completion must stay hidden");
        smol::block_on(coordinator.close()).unwrap();
    }

    #[test]
    fn event_pump_routes_subagent_permission_answer_to_subagent() {
        smol::block_on(async {
            let (srv, root_answer_rx, out_rx, _) = server_awaiting_answer();
            let (event_tx, event_rx) = flume::unbounded::<Envelope>();
            let (subagent_answer_tx, subagent_answer_rx) = flume::unbounded();
            let session = srv.session.as_ref().unwrap();
            session.pending.lock().unwrap().permissions.clear();
            start_event_pump(
                event_rx,
                session.handle.session_id.clone(),
                srv.out_tx.clone(),
                Arc::clone(&session.pending),
                false,
                session.handle.answer_tx.clone(),
                session.handle.cancel_tx.clone(),
                session.coordinator.as_ref().unwrap().read(),
                maki_storage::paths::home(),
                PUMP_TRUSTED,
                None,
            );

            event_tx
                .send_async(subagent_activity(AgentEvent::TextDelta {
                    text: "hidden".to_owned(),
                }))
                .await
                .unwrap();
            event_tx
                .send_async(subagent_activity_with_answer(
                    AgentEvent::PermissionRequest {
                        id: "subagent-tool".to_owned(),
                        tool: maki_config::ToolKey::native("bash"),
                        scopes: vec!["true".to_owned()],
                    },
                    subagent_answer_tx,
                ))
                .await
                .unwrap();

            let request = out_rx.recv_async().await.unwrap();
            assert_eq!(request["method"], "session/request_permission");
            assert_eq!(request["params"]["toolCall"]["toolCallId"], "subagent-tool");
            let request_id = request["id"].as_i64().unwrap();
            handle_incoming_response(&srv, &allow_once(request_id));
            assert_eq!(
                subagent_answer_rx.recv_async().await.unwrap(),
                TaggedAnswer::new("subagent-tool", PermissionAnswer::AllowOnce).encode()
            );
            assert!(root_answer_rx.is_empty());

            event_tx
                .send_async(subagent_activity(AgentEvent::ControlComplete {
                    usage: TokenUsage::default(),
                }))
                .await
                .unwrap();
            smol::future::yield_now().await;
            assert!(out_rx.is_empty());
        });
    }

    #[test]
    fn event_pump_emits_thinking_separator() {
        let (event_tx, event_rx) = flume::unbounded::<Envelope>();
        let (out_tx, out_rx) = flume::unbounded::<Value>();
        let (answer_tx, _answer_rx) = flume::unbounded::<String>();
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
            flume::bounded(1).0,
            coordinator.read(),
            maki_storage::paths::home(),
            PUMP_TRUSTED,
            None,
        );

        event_tx
            .send(Envelope {
                event: AgentEvent::ThinkingDelta {
                    text: "first".to_string(),
                },
                subagent: None,
                run_id: 0,
            })
            .unwrap();
        event_tx
            .send(Envelope {
                event: AgentEvent::ThinkingBlockEnd,
                subagent: None,
                run_id: 0,
            })
            .unwrap();

        smol::block_on(async {
            let deadline = std::time::Instant::now() + std::time::Duration::from_millis(200);
            let mut seen = Vec::new();
            while seen.len() < 2 {
                if let Ok(update) = out_rx.try_recv() {
                    seen.push(update);
                } else {
                    assert!(
                        std::time::Instant::now() < deadline,
                        "the pump dropped the thinking separator"
                    );
                    smol::Timer::after(std::time::Duration::from_millis(5)).await;
                }
            }
            assert_eq!(
                seen[0]["params"]["update"]["sessionUpdate"],
                "agent_thought_chunk"
            );
            assert_eq!(seen[0]["params"]["update"]["content"]["text"], "first");
            assert_eq!(
                seen[1]["params"]["update"]["sessionUpdate"],
                "agent_thought_chunk"
            );
            assert_eq!(seen[1]["params"]["update"]["content"]["text"], "\n\n");
        });
        smol::block_on(coordinator.close()).unwrap();
    }

    #[test]
    fn available_command_projection_tracks_registry_generation() {
        let registry = test_registry(&[]);
        let target = test_target(
            &registry,
            flume::unbounded().0,
            Arc::new(maki_agent::command::SessionCommandState::new(
                String::new(),
                Arc::from([]),
                PathBuf::from("/project"),
                false,
                false,
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
                    arguments: maki_commands::CommandArguments::Raw { required: false },
                    docs: maki_commands::CommandDocs {
                        summary: Arc::from("Review code"),
                        argument_hint: Some(Arc::from("<path>")),
                    },
                    required_capabilities: TargetCapabilities::default(),
                },
                behavior: Arc::new(CompletedCommand),
                argument_completions: Vec::new(),
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
            5,
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

    fn lifecycle_state(srv: &Server) -> String {
        let Some(session) = &srv.session else {
            return "session=none".to_owned();
        };
        let pending = session.pending.lock().unwrap();
        let operation = pending.operation.as_ref().map(|operation| {
            format!(
                "id={},request={:?},kind={:?},cancelling={}",
                operation.id, operation.request_id, operation.kind, operation.cancelling
            )
        });
        let permissions = pending.permissions.keys().copied().collect::<Vec<_>>();
        let elicitation = pending.elicitation;
        drop(pending);
        let coordinator = session.coordinator.as_ref().map(|coordinator| {
            let read = coordinator.read();
            let options = read.options();
            format!(
                "history={},model={},cwd={},options_version={}",
                read.history().len(),
                read.model(),
                read.cwd().display(),
                options.version
            )
        });
        format!(
            "session={},operation={operation:?},permissions={permissions:?},elicitation={elicitation:?},coordinator={coordinator:?}",
            session.handle.session_id
        )
    }

    fn subagent_activity(event: AgentEvent) -> Envelope {
        subagent_activity_with_optional_answer(event, None)
    }

    fn subagent_activity_with_answer(event: AgentEvent, answer_tx: Sender<String>) -> Envelope {
        subagent_activity_with_optional_answer(event, Some(answer_tx))
    }

    fn subagent_activity_with_optional_answer(
        event: AgentEvent,
        answer_tx: Option<Sender<String>>,
    ) -> Envelope {
        Envelope {
            event,
            subagent: Some(maki_agent::SubagentInfo {
                agent_id: maki_agent::AgentId::generate(),
                parent_agent_id: Some(maki_agent::AgentId::generate()),
                parent_is_root: true,
                auto_deliver: true,
                parent_tool_use_id: "task-reusable".to_owned(),
                name: "task".to_owned(),
                prompt: Some("reuse actor".to_owned()),
                model: Some(OFFLINE_SPEC.to_owned()),
                opts: None,
                answer_tx,
                input_tx: None,
                cancel: None,
            }),
            run_id: 0,
        }
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
            session.handle.control_tx.clone(),
            Arc::clone(&session.command_state),
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
    fn spawned_primary_turn_end_turn_releases_pending_operation() {
        smol::block_on(async {
            let previous_host = std::env::var_os("OLLAMA_HOST");
            unsafe { std::env::set_var("OLLAMA_HOST", "http://127.0.0.1:1") };
            let (mut srv, _, out_rx, _) = server_awaiting_answer();
            let session_id = srv.session.as_ref().unwrap().handle.session_id.clone();
            let model = Model::from_spec(SPAWN_TEST_SPEC).unwrap();
            let handle = spawn_session(
                &AcpParams {
                    model: model.clone(),
                    config: Default::default(),
                    permissions_config: Default::default(),
                    timeouts: Default::default(),
                    initial_wd: PathBuf::from("/project"),
                    storage: StateDir::from_path(PathBuf::from("/tmp/maki-acp-test")),
                    prompt_slots: Arc::default(),
                    modes: Arc::default(),
                    yolo: false,
                    defaults: SessionDefaults::default(),
                    system_prompt_override: Some(String::new()),
                    append_system_prompt: None,
                    model_policy: Arc::default(),
                    plugin_rules: Arc::default(),
                    lua_event_handle: maki_lua::EventHandle::disconnected_for_test(),
                    command_registry: test_registry(&[]),
                    trust_mode: TrustMode::Consult,
                    trust_policy: Arc::default(),
                },
                SpawnSession {
                    model,
                    cwd: PathBuf::from("/project"),
                    session_id: Some(session_id),
                    history: Vec::new(),
                    mcp_handle: None,
                    elicitation: false,
                    yolo: false,
                    defaults: SessionDefaults::default(),
                    project_config: ProjectConfig::for_project(Path::new("/project")),
                },
            );
            let provider_ready = smol::future::or(
                async {
                    while maki_agent::ModelSource::current(&handle.model).is_none() {
                        smol::future::yield_now().await;
                    }
                    true
                },
                async {
                    smol::Timer::after(PRIMARY_TURN_TIMEOUT).await;
                    false
                },
            )
            .await;
            assert!(
                provider_ready,
                "spawned session provider did not initialize"
            );
            handle.model.install(
                Arc::new(EndTurnProvider),
                Model::from_spec(OFFLINE_SPEC).unwrap(),
            );
            let session = srv.session.as_mut().unwrap();
            let previous_handle = std::mem::replace(&mut session.handle, handle);
            previous_handle.task.cancel().await;
            start_event_pump(
                session.handle.event_rx.clone(),
                session.handle.session_id.clone(),
                srv.out_tx.clone(),
                Arc::clone(&session.pending),
                false,
                session.handle.answer_tx.clone(),
                session.handle.cancel_tx.clone(),
                session.coordinator.as_ref().unwrap().read(),
                None,
                PUMP_TRUSTED,
                None,
            );
            let session_id = session.handle.session_id.to_string();
            let request_id = RequestId::Number(71);

            handle_prompt(
                &mut srv,
                &prompt_request(&session_id, "first", false),
                &request_id,
            )
            .await
            .unwrap();

            let visible_complete = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let observed_complete = Arc::clone(&visible_complete);
            let response = smol::future::or(
                async {
                    loop {
                        let message = out_rx.recv_async().await.unwrap();
                        if matches!(
                            message["params"]["update"]["sessionUpdate"].as_str(),
                            Some("agent_message_chunk" | "usage_update")
                        ) {
                            observed_complete.store(true, Ordering::SeqCst);
                        }
                        if message["id"] == 71 {
                            break Some(message);
                        }
                    }
                },
                async {
                    smol::Timer::after(PRIMARY_TURN_TIMEOUT).await;
                    None
                },
            )
            .await;
            let pending = srv
                .session
                .as_ref()
                .unwrap()
                .pending
                .lock()
                .unwrap()
                .operation
                .is_some();
            let visible_complete = visible_complete.load(Ordering::SeqCst);
            let terminal = response.unwrap_or_else(|| {
                panic!(
                    "PromptResponse timed out; visible_complete={visible_complete}, pending={pending}"
                )
            });
            assert_eq!(terminal["result"]["stopReason"], "end_turn");
            assert!(
                visible_complete,
                "usage update must precede terminal response"
            );
            assert!(
                !pending,
                "terminal response must release the pending primary operation"
            );
            close_session(&mut srv).await;
            match previous_host {
                Some(host) => unsafe { std::env::set_var("OLLAMA_HOST", host) },
                None => unsafe { std::env::remove_var("OLLAMA_HOST") },
            }
        });
    }

    #[test]
    fn completed_then_cancelled_session_can_be_replaced_and_prompted() {
        smol::block_on(async {
            let previous_host = std::env::var_os("OLLAMA_HOST");
            unsafe { std::env::set_var("OLLAMA_HOST", "http://127.0.0.1:1") };
            let temp = TempDir::new().unwrap();
            let cwd = temp.path().to_path_buf();
            let (mut srv, _, out_rx, input_rx) = server_awaiting_answer();
            let (event_tx, event_rx) = flume::unbounded::<Envelope>();
            let old = srv.session.as_ref().unwrap();
            old.pending.lock().unwrap().permissions.clear();
            start_event_pump(
                event_rx,
                old.handle.session_id.clone(),
                srv.out_tx.clone(),
                Arc::clone(&old.pending),
                false,
                old.handle.answer_tx.clone(),
                old.handle.cancel_tx.clone(),
                old.coordinator.as_ref().unwrap().read(),
                None,
                PUMP_TRUSTED,
                None,
            );
            let old_id = old.handle.session_id.to_string();

            handle_prompt(
                &mut srv,
                &prompt_request(&old_id, "old primary", false),
                &RequestId::Number(81),
            )
            .await
            .unwrap();
            input_rx.recv_async().await.unwrap();
            event_tx
                .send_async(subagent_activity(AgentEvent::ControlComplete {
                    usage: TokenUsage::default(),
                }))
                .await
                .unwrap();
            event_tx
                .send_async(Envelope {
                    event: AgentEvent::ControlComplete {
                        usage: TokenUsage::default(),
                    },
                    subagent: None,
                    run_id: 0,
                })
                .await
                .unwrap();
            let old_terminal = out_rx.recv_async().await.unwrap();
            assert_eq!(old_terminal["id"], 81);
            assert_eq!(old_terminal["result"]["stopReason"], "end_turn");
            handle_notification(
                &srv,
                "session/cancel",
                &serde_json::json!({ "params": { "sessionId": old_id } }),
            );

            let params = test_params(Model::from_spec(SPAWN_TEST_SPEC).unwrap(), cwd.clone());
            new_session(
                &mut srv,
                &serde_json::json!({
                    "params": { "cwd": cwd, "mcpServers": [] }
                }),
                &params,
            )
            .await
            .unwrap();
            let new_id = srv.session.as_ref().unwrap().handle.session_id.to_string();
            assert_ne!(old_id, new_id);
            assert!(
                srv.session
                    .as_ref()
                    .unwrap()
                    .pending
                    .lock()
                    .unwrap()
                    .operation
                    .is_none(),
                "new session inherited pending state; {}",
                lifecycle_state(&srv)
            );
            while out_rx.try_recv().is_ok() {}
            let provider_ready = smol::future::or(
                async {
                    while maki_agent::ModelSource::current(
                        &srv.session.as_ref().unwrap().handle.model,
                    )
                    .is_none()
                    {
                        smol::future::yield_now().await;
                    }
                    true
                },
                async {
                    smol::Timer::after(PRIMARY_TURN_TIMEOUT).await;
                    false
                },
            )
            .await;
            assert!(
                provider_ready,
                "new session provider did not initialize; {}",
                lifecycle_state(&srv)
            );
            srv.session.as_ref().unwrap().handle.model.install(
                Arc::new(EndTurnProvider),
                Model::from_spec(OFFLINE_SPEC).unwrap(),
            );

            handle_prompt(
                &mut srv,
                &prompt_request(&new_id, "new primary", false),
                &RequestId::Number(82),
            )
            .await
            .unwrap();
            let started = std::time::Instant::now();
            let terminal = smol::future::or(
                async {
                    loop {
                        let message = out_rx.recv_async().await.unwrap();
                        if message["id"] == 82 {
                            break Some(message);
                        }
                    }
                },
                async {
                    smol::Timer::after(PRIMARY_TURN_TIMEOUT).await;
                    None
                },
            )
            .await
            .unwrap_or_else(|| {
                panic!(
                    "new PromptResponse timed out after {:?}; {}",
                    started.elapsed(),
                    lifecycle_state(&srv)
                )
            });
            assert_eq!(terminal["result"]["stopReason"], "end_turn");
            assert!(
                srv.session
                    .as_ref()
                    .unwrap()
                    .pending
                    .lock()
                    .unwrap()
                    .operation
                    .is_none(),
                "new prompt completed but remained pending; elapsed={:?}; {}",
                started.elapsed(),
                lifecycle_state(&srv)
            );
            close_session(&mut srv).await;
            match previous_host {
                Some(host) => unsafe { std::env::set_var("OLLAMA_HOST", host) },
                None => unsafe { std::env::remove_var("OLLAMA_HOST") },
            }
        });
    }

    #[test]
    fn unknown_slash_prompt_is_rejected() {
        let (mut srv, _, _, input_rx) = server_awaiting_answer();
        let error = smol::block_on(dispatch_prompt(
            &mut srv,
            "/does-not-exist value",
            false,
            &RequestId::Number(1),
        ))
        .unwrap_err();
        assert_eq!(error.code, AcpError::invalid_params().code);
        assert_eq!(error.message, "unknown command /does-not-exist");
        assert!(input_rx.is_empty());
    }

    #[test]
    fn escaped_slash_prompt_is_sent_literal() {
        let (mut srv, _, _, input_rx) = server_awaiting_answer();

        smol::block_on(dispatch_prompt(
            &mut srv,
            "//does-not-exist value",
            true,
            &RequestId::Number(2),
        ))
        .unwrap();
        let input = input_rx.try_recv().unwrap();
        assert_eq!(input.message, "/does-not-exist value");
        assert_eq!(input.images.len(), 1);
        assert_eq!(
            input.images[0].media_type,
            maki_providers::ImageMediaType::Png
        );
        assert_eq!(input.images[0].data.as_ref(), "aGVsbG8=");
    }

    #[test]
    fn new_and_clear_are_hidden_but_return_local_guidance() {
        smol::block_on(async {
            let (mut srv, _, out_rx, input_rx) = server_awaiting_answer();
            let (control_tx, control_rx) = flume::unbounded();
            srv.session.as_mut().unwrap().handle.control_tx = control_tx;
            install_registry(&mut srv, test_registry(&[]));
            let target = &srv.session.as_ref().unwrap().command_target;
            let presented = srv
                .session
                .as_ref()
                .unwrap()
                .command_registry
                .presented_commands(target)
                .unwrap();
            let names: Vec<_> = presented
                .iter()
                .map(|command| command.name.as_ref())
                .collect();
            assert!(!names.contains(&"/new"));
            assert!(!names.contains(&"/clear"));

            for (index, input) in ["/new", "/clear"].into_iter().enumerate() {
                dispatch_prompt(
                    &mut srv,
                    input,
                    false,
                    &RequestId::Number(index as i64 + 10),
                )
                .await
                .unwrap();
                let message = out_rx.recv_async().await.unwrap();
                assert_eq!(
                    message["params"]["update"]["sessionUpdate"],
                    "agent_message_chunk"
                );
                assert_eq!(
                    message["params"]["update"]["content"]["text"],
                    NEW_SESSION_GUIDANCE
                );
                let response = out_rx.recv_async().await.unwrap();
                assert_eq!(response["result"]["stopReason"], "end_turn");
                assert!(input_rx.is_empty());
                assert!(control_rx.is_empty());
            }
        });
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
    fn unavailable_interactive_command_is_rejected() {
        let (mut srv, _, _, input_rx) = server_awaiting_answer();

        let error = smol::block_on(dispatch_prompt(
            &mut srv,
            "/help",
            true,
            &RequestId::Number(3),
        ))
        .unwrap_err();
        assert_eq!(error.code, AcpError::invalid_params().code);
        assert_eq!(error.message, "unknown command /help");
        assert!(input_rx.is_empty());
    }

    #[test]
    fn consecutive_model_errors_follow_each_prompt() {
        let (mut srv, _, _, input_rx) = server_awaiting_answer();

        let first = smol::block_on(dispatch_prompt(
            &mut srv,
            "/model definitely-invalid-provider/test",
            false,
            &RequestId::Number(3),
        ))
        .unwrap_err();
        let second = smol::block_on(dispatch_prompt(
            &mut srv,
            "/model codex/gpt-5.6-sol",
            false,
            &RequestId::Number(4),
        ))
        .unwrap_err();

        assert_eq!(
            first.message,
            "command failed: unsupported provider 'definitely-invalid-provider'"
        );
        assert_eq!(
            second.message,
            "command failed: unsupported provider 'codex'"
        );
        assert!(input_rx.is_empty());
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
                    arguments: maki_commands::CommandArguments::Positional(Arc::from([])),
                    docs: maki_commands::CommandDocs {
                        summary: Arc::from("complete without a turn"),
                        argument_hint: None,
                    },
                    required_capabilities: TargetCapabilities::default(),
                },
                behavior: Arc::new(CompletedCommand),
                argument_completions: Vec::new(),
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
            "invalid typed arguments for /btw: raw arguments are required"
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
    fn restored_cwd_overrides_client_cwd() {
        let restored = PathBuf::from("/restored/project");
        let client = PathBuf::from("/client/project");

        assert_eq!(effective_session_cwd(Some(&restored), &client), restored);
        assert_eq!(effective_session_cwd(None, &client), client);
    }

    #[test]
    fn load_failure_does_not_replay_transcript() {
        smol::block_on(async {
            let tmp = TempDir::new().unwrap();
            let mut session: Session<Message, TokenUsage, maki_agent::ToolOutput> =
                Session::new(OFFLINE_SPEC, &tmp.path().to_string_lossy());
            session.replace_messages(vec![Message::user("not yet visible".into())]);
            let storage = StateDir::from_path(tmp.path().to_path_buf());
            session.save(&storage).unwrap();
            let coordinator = test_coordinator(session.id, OFFLINE_SPEC, tmp.path().to_path_buf());
            let params = test_params(
                Model::from_spec(OFFLINE_SPEC).unwrap(),
                tmp.path().to_path_buf(),
            );
            let (out_tx, out_rx) = flume::unbounded();
            let mut srv = Server {
                out_tx,
                model_specs: vec![OFFLINE_SPEC.to_owned()],
                modes: Arc::clone(&params.modes),
                session: None,
                elicitation: false,
                supports_boolean: false,
                lua_event_handle: params.lua_event_handle.clone(),
                defaults: SessionDefaults::default(),
            };
            let raw = serde_json::json!({
                "params": {
                    "sessionId": session.id.to_string(),
                    "cwd": tmp.path(),
                    "mcpServers": []
                }
            });

            assert!(load_session(&mut srv, &raw, &params).await.is_err());
            assert!(
                out_rx.is_empty(),
                "failed installation must not replay history"
            );
            coordinator.close().await.unwrap();
        });
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
        session.meta.yolo = Some(true);
        session.meta.fast = true;
        session.meta.workflow = true;
        session
            .meta
            .session_options
            .insert("bash.auto_mode".into(), "enabled".into());
        session.save(&dir).unwrap();

        let restored = load_history_from(&dir, session.id).unwrap();

        assert_eq!(restored.meta.yolo, Some(true));
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

    #[test]
    fn auth_required_ends_the_prompt_instead_of_parking_the_session() {
        smol::block_on(async {
            let (mut srv, _, out_rx, input_rx) = server_awaiting_answer();
            let (event_tx, event_rx) = flume::unbounded::<Envelope>();
            let (cancel_tx, cancel_rx) = flume::bounded(1);
            let (session_id, pending) = {
                let session = srv.session.as_mut().unwrap();
                session.handle.cancel_tx = cancel_tx.clone();
                session.pending.lock().unwrap().permissions.clear();
                (
                    session.handle.session_id.clone(),
                    Arc::clone(&session.pending),
                )
            };
            start_event_pump(
                event_rx,
                session_id.clone(),
                srv.out_tx.clone(),
                Arc::clone(&pending),
                false,
                srv.session.as_ref().unwrap().handle.answer_tx.clone(),
                cancel_tx,
                srv.session
                    .as_ref()
                    .unwrap()
                    .coordinator
                    .as_ref()
                    .unwrap()
                    .read(),
                None,
                PUMP_TRUSTED,
                None,
            );
            let session_id_str = session_id.to_string();
            let request_id = RequestId::Number(41);

            handle_prompt(
                &mut srv,
                &prompt_request(&session_id_str, "first", false),
                &request_id,
            )
            .await
            .unwrap();
            input_rx.recv_async().await.unwrap();

            event_tx
                .send_async(Envelope {
                    event: AgentEvent::AuthRequired,
                    subagent: None,
                    run_id: 0,
                })
                .await
                .unwrap();

            let response = out_rx.recv_async().await.unwrap();
            assert_eq!(response["id"], 41);
            assert_eq!(
                response["error"]["code"],
                i32::from(AcpError::auth_required().code)
            );
            assert_eq!(
                response["error"]["data"], AUTH_FAILED_MSG,
                "the client is told why the turn ended: {response}"
            );
            assert!(
                cancel_rx.recv_async().await.is_ok(),
                "the agent parked on re-authentication is released"
            );

            assert!(pending.lock().unwrap().operation.is_none());
        });
    }

    #[test_case(Value::from(ANSWERED_ID), true ; "a_numeric_id_answers_the_ask")]
    #[test_case(Value::from(ANSWERED_ID.to_string()), true ; "a_string_id_answers_the_same_ask")]
    #[test_case(Value::from("not-an-id"), false ; "an_id_matching_no_ask_answers_nothing")]
    fn a_response_is_delivered_whatever_shape_its_id_has(id: Value, delivered: bool) {
        let (srv, answer_rx, ..) = server_awaiting_answer();

        handle_incoming_response(
            &srv,
            &serde_json::json!({
                "id": id,
                "result": { "outcome": { "outcome": "selected", "optionId": "allow_once" } },
            }),
        );

        let answered = TaggedAnswer::new("toolu_1", PermissionAnswer::AllowOnce).encode();
        assert_eq!(answer_rx.try_recv().ok(), delivered.then_some(answered));
        assert_eq!(
            srv.session
                .as_ref()
                .unwrap()
                .pending
                .lock()
                .unwrap()
                .permissions
                .contains_key(&ANSWERED_ID),
            !delivered,
            "only the ask that was answered is retired"
        );
    }

    #[test]
    fn permission_request_carries_file_context_and_diff() {
        smol::block_on(async {
            let (mut srv, _, out_rx, input_rx) = server_awaiting_answer();
            let (event_tx, event_rx) = flume::unbounded::<Envelope>();
            let session = srv.session.as_ref().unwrap();
            start_event_pump(
                event_rx,
                session.handle.session_id.clone(),
                srv.out_tx.clone(),
                Arc::clone(&session.pending),
                false,
                session.handle.answer_tx.clone(),
                session.handle.cancel_tx.clone(),
                session.coordinator.as_ref().unwrap().read(),
                None,
                PUMP_TRUSTED,
                None,
            );
            let session_id = session.handle.session_id.to_string();

            handle_prompt(
                &mut srv,
                &prompt_request(&session_id, "first", false),
                &RequestId::Number(41),
            )
            .await
            .unwrap();
            input_rx.recv_async().await.unwrap();

            event_tx
                .send_async(Envelope {
                    event: AgentEvent::ToolStart(Box::new(maki_agent::ToolStartEvent {
                        id: "call_write".to_string(),
                        tool: Arc::from("write"),
                        summary: String::new(),
                        render_header: None,
                        annotation: None,
                        input: None,
                        raw_input: Some(serde_json::json!({
                            "path": "/project/file.txt",
                            "content": "new text",
                        })),
                        output: None,
                    })),
                    subagent: None,
                    run_id: 0,
                })
                .await
                .unwrap();

            event_tx
                .send_async(Envelope {
                    event: AgentEvent::PermissionRequest {
                        id: "call_write".to_string(),
                        tool: maki_config::ToolKey::Native(Arc::from("write")),
                        scopes: vec!["/project/file.txt".to_string()],
                    },
                    subagent: None,
                    run_id: 0,
                })
                .await
                .unwrap();

            let request = loop {
                let msg = out_rx.recv_async().await.unwrap();
                if msg.get("method") == Some(&Value::String("session/request_permission".into())) {
                    break msg;
                }
            };
            let call = &request["params"]["toolCall"];
            assert_eq!(call["rawInput"]["content"], "new text");
        });
    }
}
