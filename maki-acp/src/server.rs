use std::collections::{BTreeMap, HashMap, VecDeque};
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
use event_listener::Event;
use flume::{Receiver, Sender};
use maki_agent::config_change::ConfigChangeError;
use maki_agent::headless::{self, InteractiveHandle, InteractiveParams, WorkCancel};
use maki_agent::mcp::config::{RawHttpFields, RawStdioFields, RawTransport};
use maki_agent::mcp::{self, McpHandle};
use maki_agent::permissions::{PermissionAnswer, TaggedAnswer};
use maki_agent::session_coordinator::{
    ModelPreparer, SessionCoordinatorError, SessionCoordinatorHandle,
};
use maki_agent::session_defaults;
use maki_agent::session_options::SessionOptionError;
use maki_agent::tools::QuestionMode;
use maki_agent::types::AgentEvent;
use maki_agent::{AgentInput, Envelope, ImageMediaType, ImageSource, ModeError};
use maki_commands::{
    AgentTurn, CommandAttachment, CommandContent, CommandFuture, CommandOutcome, CommandRegistry,
    InputDispatch, PresentedCommand, TargetHandle,
};
use maki_config::project::{self, TrustAnswer, TrustMode, policy_grant};
use maki_config::{
    MAX_SERVER_NAME_LEN, PermissionsConfig, ProjectConfig, SessionDefaults, TrustConfig,
};
use maki_lua::orchestration::{AgentOrchestration, ModelCatalog, TrustedTarget};
use maki_lua::session_snapshot::HeadlessObserver;
use maki_providers::model::Model;
use maki_providers::{Message, TokenUsage, add_cost, settle_session};
use maki_storage::StateDir;
use maki_storage::id::{MakiId, SessionRef};
use maki_storage::session_lock;
use maki_storage::sessions::{SESSIONS_DIR, SessionMeta, StoredTokenUsage, read_prefs};
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
const SESSION_ENDED_MESSAGE: &str = "session ended";
const EVENT_STREAM_ENDED: &str = "session event stream ended";
const ROOT_CONFIG_UNAVAILABLE: &str = "session root configuration is unavailable";
const GRAPH_SHUTDOWN_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(2);
const AUTH_FAILED_MSG: &str =
    "Authentication failed. Run `maki auth login`, then send the prompt again.";

/// Ids come from here and are never reused, so a late answer for a closed
/// session cannot match a request of the session that replaced it.
static NEXT_OUTGOING_REQUEST_ID: AtomicI64 = AtomicI64::new(FIRST_OUTGOING_REQUEST_ID);
static NEXT_OPERATION_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OperationKind {
    Command,
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
    cancel: Option<WorkCancel>,
    /// Prompts whose message joined this turn, answered with it.
    joined: Vec<RequestId>,
}

impl PendingOperation {
    fn requests(&self) -> impl Iterator<Item = RequestId> + '_ {
        iter::once(&self.request_id).chain(&self.joined).cloned()
    }

    fn start_cancelling(&mut self) -> Option<WorkCancel> {
        self.cancelling = true;
        self.cancel.take()
    }
}

/// What the client still owes us. Subagents have independent answer channels,
/// so more than one permission request can be outstanding.
#[derive(Default)]
struct Pending {
    operation: Option<PendingOperation>,
    /// Prompts whose message started a turn of its own behind the running
    /// one, as typing does in the TUI when it cannot join.
    queued: VecDeque<PendingOperation>,
    permissions: HashMap<i64, (String, Sender<String>)>,
    elicitation: Option<i64>,
    /// The client closed its input, so no answer to a new ask can arrive.
    input_closed: bool,
    /// Counts `session/cancel`s, so a prompt dispatching beside a turn, which
    /// has no operation to mark, sees that it was cancelled meanwhile.
    cancels: u64,
    settled: Event,
}

impl Pending {
    /// Whether a prompt may start now; `true` when it starts beside a turn
    /// its message may join.
    fn admits_prompt(&self) -> Result<bool, AcpError> {
        match &self.operation {
            None => Ok(false),
            Some(operation) if operation.cancelling => Err(AcpError::new(
                CANCELLATION_IN_PROGRESS_CODE,
                CANCELLATION_IN_PROGRESS_MESSAGE,
            )
            .data(serde_json::json!({ "retryable": true }))),
            Some(operation) if operation.kind == OperationKind::PrimaryTurn => Ok(true),
            Some(_) => Err(AcpError::new(-32600, ACTIVE_OPERATION_MESSAGE)),
        }
    }

    fn refuses_asks(&self) -> bool {
        self.input_closed
            || self
                .operation
                .as_ref()
                .is_some_and(|operation| operation.cancelling)
    }

    fn settle(&mut self) -> PendingOperation {
        self.permissions.clear();
        self.elicitation = None;
        self.settled.notify(usize::MAX);
        let settled = self.operation.take().unwrap();
        self.operation = self.queued.pop_front();
        settled
    }

    /// Every operation the client still waits on, the running one first.
    fn take_all(&mut self) -> Vec<PendingOperation> {
        self.settled.notify(usize::MAX);
        self.operation
            .take()
            .into_iter()
            .chain(self.queued.drain(..))
            .collect()
    }
}

type PendingState = Arc<Mutex<Pending>>;

struct OptionProjection {
    out_tx: Sender<Value>,
    session_id: SessionId,
    read: maki_agent::session_coordinator::SessionReadHandle,
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

    fn emit_current(&self) {
        let snapshot = self.read.options();
        self.emit(&snapshot);
    }
}

struct SessionState {
    handle: InteractiveHandle,
    coordinator: Option<maki_agent::session_coordinator::SessionCoordinatorHandle>,
    orchestration: Option<Arc<AgentOrchestration>>,
    checkpoint: Option<Arc<maki_agent::session_checkpoint::SessionLogCheckpoint>>,
    mcp: Option<McpHandle>,
    pending: PendingState,
    command_registry: maki_commands::CommandRegistry,
    command_target: TargetHandle,
    command_projection_task: smol::Task<()>,
    /// The Lua host can outlive the session holding an event sender, so the
    /// pump is stopped explicitly rather than by its stream closing.
    event_pump: EventPump,
    option_projection: Option<Arc<OptionProjection>>,
    option_projection_task: smol::Task<()>,
    lock: Option<session_lock::LockHeartbeat>,
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
    permissions: PermissionsConfig,
}

struct InstallSession<'a> {
    handle: InteractiveHandle,
    root_target: Receiver<TrustedTarget>,
    mcp: Option<McpHandle>,
    current_model: String,
    initial_cost: Option<f64>,
    cwd: PathBuf,
    defaults: SessionDefaults,
    persisted_options: &'a BTreeMap<String, String>,
}

struct Server {
    out_tx: Sender<Value>,
    catalog: ModelCatalog,
    session: Option<SessionState>,
    /// Whether the client advertised form elicitation support at `initialize`.
    elicitation: bool,
    supports_boolean: bool,
    lua_event_handle: maki_lua::EventHandle,
    /// The prompt whose command is still being dispatched. It runs beside the
    /// input loop, so the answers and cancels its handler waits on still
    /// arrive.
    dispatch: Option<PendingDispatch>,
}

struct PendingDispatch {
    /// The prompt's own operation; `None` while it dispatches beside a
    /// running turn its message may join.
    operation_id: Option<u64>,
    /// [`Pending::cancels`] when the dispatch started.
    cancels: u64,
    request_id: RequestId,
    future: CommandFuture<InputDispatch>,
}

enum Step {
    Line(String),
    InputClosed,
    Dispatched(InputDispatch),
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
        catalog: ModelCatalog::offline(&params.model_policy),
        session: None,
        elicitation: false,
        supports_boolean: false,
        lua_event_handle: params.lua_event_handle.clone(),
        dispatch: None,
    };

    let _discovery = server.catalog.discover(
        Arc::clone(&params.model_policy),
        params.model.provider.to_string(),
        |_| {},
    );
    let (in_tx, in_rx) = flume::unbounded::<String>();
    let _reader_task = smol::spawn(read_stdin(in_tx));
    let next_line = || async {
        in_rx
            .recv_async()
            .await
            .map_or(Step::InputClosed, Step::Line)
    };
    loop {
        let step = match server.dispatch.as_mut() {
            Some(dispatch) => {
                smol::future::or(next_line(), async {
                    Step::Dispatched((&mut dispatch.future).await)
                })
                .await
            }
            None => next_line().await,
        };
        match step {
            Step::Line(line) => handle_line(&mut server, &line, &params).await,
            Step::Dispatched(dispatched) => respond_dispatch(&mut server, dispatched).await,
            Step::InputClosed => break,
        }
    }

    if let Some(session) = &server.session {
        refuse_asks(session);
    }
    if let Some(dispatch) = server.dispatch.as_mut() {
        let dispatched = (&mut dispatch.future).await;
        respond_dispatch(&mut server, dispatched).await;
    }
    if let Some(session) = &server.session {
        finish_in_flight(session).await;
    }
    close_session(&mut server).await;
    drop(server);
    writer_task.await;

    Ok(())
}

async fn read_stdin(tx: Sender<String>) -> std::io::Result<()> {
    let mut reader = smol::io::BufReader::new(smol::Unblock::new(std::io::stdin()));
    loop {
        let mut line = String::new();
        if reader.read_line(&mut line).await? == 0 {
            return Ok(());
        }
        if tx.send_async(line).await.is_err() {
            return Ok(());
        }
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
        "session/prompt" => match start_prompt(srv, raw, &id) {
            Ok(()) => return,
            Err(e) => Err(e),
        },
        "session/set_mode" => handle_set_mode(srv, raw, params).await,
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

fn load_permissions(project_config: &ProjectConfig) -> Result<PermissionsConfig, AcpError> {
    maki_config::load_permissions(project_config)
        .map_err(|error| AcpError::new(-32603, error.to_string()))
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
    let cwd = req.cwd.clone();
    let project_config = trusted_project_config(
        &req.cwd,
        &params.storage,
        params.trust_mode,
        &params.trust_policy,
    );
    let permissions = load_permissions(&project_config)?;
    close_session(srv).await;
    let mcp = start_mcp(&req.cwd, &req.mcp_servers, project_config.clone(), params).await;
    let defaults = session_defaults::resolve(
        params.defaults,
        &read_prefs(&params.storage),
        &SessionMeta::default(),
        true,
        &params.model,
    );
    let (handle, root_target) = spawn_session(
        params,
        SpawnSession {
            model: params.model.clone(),
            cwd: req.cwd,
            session_id: None,
            history: Vec::new(),
            mcp_handle: mcp.clone(),
            elicitation: srv.elicitation,
            yolo: params.yolo,
            defaults,
            project_config,
            permissions,
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
            root_target,
            current_model: spec,
            initial_cost: None,
            cwd,
            defaults,
            persisted_options: &persisted_options,
        },
    )
    .await
    .map_err(session_registration_error)?;
    let mode = committed_mode(srv).await?;
    let resp =
        methods::new_session_response(&session_id, &mode, &srv.lua_event_handle.mode_registry())
            .config_options(methods::session_config_options(
                &snapshot,
                srv.supports_boolean,
            ));
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
    let session_cwd = effective_session_cwd(restored.cwd.as_deref(), &req.cwd);
    let project_config = trusted_project_config(
        &session_cwd,
        &params.storage,
        params.trust_mode,
        &params.trust_policy,
    );
    let permissions = load_permissions(&project_config)?;
    close_session(srv).await;
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
    let yolo = restored.meta.yolo.unwrap_or(params.yolo);
    let defaults = session_defaults::resolve(
        params.defaults,
        &read_prefs(&params.storage),
        &restored.meta,
        restored.history.messages.is_empty(),
        &recorded_model,
    );
    let replay_history = Arc::clone(&restored.history.messages);
    let history_snapshot = restored.history;
    let (handle, root_target) = spawn_session_with_history(
        params,
        SpawnSession {
            model: recorded_model.clone(),
            cwd: session_cwd.clone(),
            session_id: Some(session_ref),
            history: Vec::new(),
            mcp_handle: mcp.clone(),
            elicitation: srv.elicitation,
            yolo,
            defaults,
            project_config,
            permissions,
        },
        Some(history_snapshot),
    );
    let restored_cost = settle_session(
        &restored.usage,
        &mut restored.by_model,
        &recorded_model,
        defaults.fast,
    );
    let snapshot = install_session(
        srv,
        params,
        InstallSession {
            handle,
            mcp,
            current_model: spec,
            root_target,
            initial_cost: restored_cost,
            cwd: session_cwd.clone(),
            defaults,
            persisted_options: &restored.meta.session_options,
        },
    )
    .await
    .map_err(session_registration_error)?;
    if let Some(session) = &srv.session
        && let Some(orchestration) = &session.orchestration
        && let Err(error) = async {
            let root = session.handle.initialized_root().await?;
            maki_lua::orchestration::restore_mode(
                &**orchestration,
                &root,
                &srv.lua_event_handle.mode_registry(),
                &restored.meta,
                &params.storage,
            )
            .await
        }
        .await
    {
        warn!(%error, "saved mode not restored; the session resumes in build mode");
    }
    for update in translate::replay_history(&replay_history, Some(&session_cwd), home.as_deref()) {
        session_update(&srv.out_tx, &sid, update);
    }
    let mode = committed_mode(srv).await?;
    let resp = methods::load_session_response(&mode, &srv.lua_event_handle.mode_registry())
        .config_options(methods::session_config_options(
            &snapshot,
            srv.supports_boolean,
        ));
    Ok(AgentResponse::LoadSessionResponse(resp))
}

fn spawn_session(
    params: &AcpParams,
    session: SpawnSession,
) -> (InteractiveHandle, Receiver<TrustedTarget>) {
    spawn_session_with_history(params, session, None)
}

fn spawn_session_with_history(
    params: &AcpParams,
    session: SpawnSession,
    snapshot: Option<maki_agent::agent::HistorySnapshot>,
) -> (InteractiveHandle, Receiver<TrustedTarget>) {
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
        permissions,
    } = session;
    let interactive = InteractiveParams {
        model,
        config: params.config.clone(),
        permissions_config: permissions,
        timeouts: params.timeouts,
        prompt_slots: params.prompt_slots.clone(),
        excluded_tools: Vec::new(),
        mcp_handle,
        initial_wd: cwd,
        session_id,
        modes: params.lua_event_handle.mode_registry(),
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
        state_dir: Some(params.storage.path().to_path_buf()),
        answers: true,
    };
    let (binding, root_target) = maki_lua::orchestration::root_binding();
    let handle = headless::spawn_interactive_with_binding(
        interactive,
        headless::default_preparation(),
        snapshot,
        Some(binding),
    );
    (handle, root_target)
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

/// The client closed its input, so the asks it can no longer answer are
/// refused, now and from here on.
fn refuse_asks(session: &SessionState) {
    let (permission_answers, elicitation) = {
        let mut pending = session.pending.lock().unwrap();
        pending.input_closed = true;
        (
            std::mem::take(&mut pending.permissions),
            pending.elicitation.take(),
        )
    };
    answer_cancelled_requests(&session.handle, permission_answers, elicitation);
}

/// The client closed its input but still reads the output: the work in
/// flight runs to its end and reports, background subagents and the turns
/// their results start included, while the asks it can no longer get
/// answered are refused.
async fn finish_in_flight(session: &SessionState) {
    refuse_asks(session);
    loop {
        let settled = {
            let pending = session.pending.lock().unwrap();
            if pending.operation.is_none() {
                break;
            }
            pending.settled.listen()
        };
        settled.await;
    }
    session.handle.manager.wait_settled().await;
}

/// Stop the old session before the next one starts, so two generations of the
/// same MCP servers never fight over a port or a lock file.
async fn close_session(srv: &mut Server) {
    // A dispatch with an operation is answered with the operations below.
    if let Some(dispatch) = srv.dispatch.take()
        && dispatch.operation_id.is_none()
    {
        respond_prompt(&srv.out_tx, dispatch.request_id, StopReason::Cancelled);
    }
    let Some(mut state) = srv.session.take() else {
        return;
    };
    srv.lua_event_handle.clear_orchestration_services();
    // The event pump dies with the session, so the requests it owed answers to
    // have to be answered here or the client waits on them forever.
    let (operations, permission_answers, elicitation) = {
        let mut pending = state.pending.lock().unwrap();
        (
            pending.take_all(),
            std::mem::take(&mut pending.permissions),
            pending.elicitation.take(),
        )
    };
    for request_id in operations.iter().flat_map(PendingOperation::requests) {
        respond_prompt(&srv.out_tx, request_id, StopReason::Cancelled);
    }
    answer_cancelled_requests(&state.handle, permission_answers, elicitation);
    state.handle.manager.shutdown(GRAPH_SHUTDOWN_TIMEOUT).await;
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
    state.event_pump.finish().await;
    state.option_projection_task.cancel().await;
    if let Some(mcp) = state.mcp {
        mcp.shutdown().await;
    }
    drop(state.lock.take());
}

fn start_session_lock_in(dir: PathBuf, id: MakiId) -> Result<session_lock::LockHeartbeat, String> {
    let lease = session_lock::claim(&dir, &id)
        .map_err(|error| error.to_string())?
        .ok_or_else(|| session_lock::OPEN_ELSEWHERE_MSG.to_owned())?;
    Ok(session_lock::LockHeartbeat::spawn(lease, move || {
        if let Ok(coordinator) =
            maki_agent::session_coordinator::SessionCoordinatorHandle::resolve(id)
        {
            coordinator.retire();
        }
    }))
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
    lock_session: impl FnOnce(MakiId) -> Result<session_lock::LockHeartbeat, String>,
) -> Result<maki_agent::session_options::SessionOptionsSnapshot, String> {
    let InstallSession {
        handle,
        root_target,
        mcp,
        current_model,
        initial_cost,
        cwd,
        defaults,
        persisted_options,
    } = session;
    let definitions = maki_agent::session_coordinator::builtin_option_definitions(
        Arc::from(current_model.as_str()),
        srv.catalog.snapshot(),
        handle.permissions.is_yolo(),
        defaults.fast,
        defaults.workflow,
        defaults.thinking.into(),
    );
    let lock = match lock_session(handle.session_id.id()) {
        Ok(lock) => lock,
        Err(error) => {
            warn!(%error, "session lock claim failed");
            rollback_install(handle, mcp, None).await;
            return Err(error);
        }
    };
    let checkpoint = match maki_agent::session_checkpoint::SessionLogCheckpoint::open(
        params.storage.clone(),
        handle.session_id.id(),
        &current_model,
        &cwd.to_string_lossy(),
        Some(lock.publication_guard()),
    ) {
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
            model_policy: Arc::clone(&params.model_policy),
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
    let trusted_root = match maki_lua::orchestration::bind_frontend_root(
        &handle,
        &coordinator,
        &root_target,
        headless::default_model_preparer(params.timeouts),
    )
    .await
    {
        Ok(root) => root,
        Err(error) => {
            rollback_install(handle, mcp, Some(coordinator)).await;
            return Err(error);
        }
    };
    let orchestration = maki_lua::orchestration::durable_frontend(
        "ACP",
        &srv.catalog,
        trusted_root.clone(),
        coordinator.clone(),
        Arc::clone(&checkpoint),
    );
    params
        .lua_event_handle
        .install_orchestration_services(orchestration.clone());
    let pending = PendingState::default();
    let project_trusted = handle.permissions.project_is_trusted();
    let observer = HeadlessObserver::new(params.lua_event_handle.clone(), handle.session_id.id());
    observer.install(trusted_root.target.clone());
    let event_pump = start_event_pump(
        handle.event_rx.clone(),
        handle.session_id.clone(),
        srv.out_tx.clone(),
        Arc::clone(&pending),
        srv.elicitation,
        handle.answer_tx.clone(),
        coordinator.read(),
        maki_storage::paths::home(),
        project_trusted,
        initial_cost,
        observer,
    );
    let command_registry = params.command_registry.clone();
    let command_target = session_command_target(
        &command_registry,
        &handle,
        &coordinator,
        &trusted_root,
        &orchestration,
        headless::default_model_preparer(params.timeouts),
    );
    orchestration.bind_commands(
        &trusted_root.target,
        maki_lua::orchestration::command_runner(
            command_registry.clone(),
            command_target.clone(),
            maki_lua::orchestration::headless_outcomes(&handle),
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
        orchestration: Some(orchestration),
        checkpoint: Some(checkpoint),
        mcp,
        pending,
        command_registry,
        command_target,
        command_projection_task,
        event_pump,
        option_projection: Some(option_projection),
        option_projection_task,
        lock: Some(lock),
    });
    Ok(option_snapshot)
}

/// The target a session's prompts dispatch on: commands control {handle}'s
/// session and run on {root}, or on the agent a plugin pinned them to.
fn session_command_target(
    registry: &CommandRegistry,
    handle: &InteractiveHandle,
    coordinator: &SessionCoordinatorHandle,
    root: &TrustedTarget,
    orchestration: &Arc<AgentOrchestration>,
    prepare_model: ModelPreparer,
) -> TargetHandle {
    registry.bind_target_with_presentation(
        maki_agent::command::session_capabilities(),
        maki_agent::command::portable_capabilities(),
        Arc::new(maki_lua::orchestration::PinnedCommandHost::new(
            maki_agent::command::SessionCommandHost::new(
                handle.control_tx.clone(),
                coordinator.clone(),
            )
            .with_reset_session_guidance(NEW_SESSION_GUIDANCE),
            root.clone(),
            orchestration,
            prepare_model,
        )),
    )
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

struct Restored {
    history: maki_agent::agent::HistorySnapshot,
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
    let history = session.history_snapshot();
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

/// Starts dispatching the prompt's command; [`finish_prompt`] takes over
/// once it resolves.
fn start_prompt(srv: &mut Server, raw: &Value, id: &RequestId) -> Result<(), AcpError> {
    let req: PromptRequest = parse_params(raw)?;
    let session = validate_session(srv, req.session_id.0.as_ref())?;
    let content = extract_prompt_content(&req.prompt)?;
    if srv.dispatch.is_some() {
        return Err(AcpError::new(-32600, ACTIVE_OPERATION_MESSAGE));
    }
    let mut pending = session.pending.lock().unwrap();
    let operation_id = (!pending.admits_prompt()?).then(|| {
        let operation_id = NEXT_OPERATION_ID.fetch_add(1, Ordering::Relaxed);
        pending.operation = Some(PendingOperation {
            id: operation_id,
            request_id: id.clone(),
            kind: OperationKind::Command,
            run_id: None,
            cancelling: false,
            cancel: None,
            joined: Vec::new(),
        });
        operation_id
    });
    let cancels = pending.cancels;
    drop(pending);
    let future = session
        .command_registry
        .dispatch_input(&session.command_target, content);
    srv.dispatch = Some(PendingDispatch {
        operation_id,
        cancels,
        request_id: id.clone(),
        future,
    });
    Ok(())
}

async fn respond_dispatch(srv: &mut Server, dispatched: InputDispatch) {
    let request_id = srv
        .dispatch
        .as_ref()
        .map(|dispatch| dispatch.request_id.clone());
    if let (Some(request_id), Err(error)) = (request_id, finish_prompt(srv, dispatched).await) {
        srv.respond(request_id, Err(error));
    }
}

/// Runs what the prompt's command resolved to, unless the prompt was
/// cancelled or its session closed while the command ran.
async fn finish_prompt(srv: &mut Server, dispatched: InputDispatch) -> Result<(), AcpError> {
    let Some(PendingDispatch {
        operation_id,
        cancels,
        request_id,
        ..
    }) = srv.dispatch.take()
    else {
        return Ok(());
    };
    let Some(session) = srv.session.as_ref() else {
        return Ok(());
    };
    if operation_id.is_none() && session.pending.lock().unwrap().cancels != cancels {
        respond_prompt(&srv.out_tx, request_id, StopReason::Cancelled);
        return Ok(());
    }
    if let Some(operation_id) = operation_id {
        let cancelling = {
            let mut pending = session.pending.lock().unwrap();
            let Some(operation) = pending.operation.take_if(|operation| {
                operation.id == operation_id && operation.kind == OperationKind::Command
            }) else {
                return Ok(());
            };
            pending.settled.notify(usize::MAX);
            operation.cancelling
        };
        if cancelling {
            respond_prompt(&srv.out_tx, request_id, StopReason::Cancelled);
            return Ok(());
        }
    }
    run_dispatch(srv, &request_id, dispatched).await
}

async fn run_dispatch(
    srv: &Server,
    id: &RequestId,
    dispatch: InputDispatch,
) -> Result<(), AcpError> {
    let session = srv.session.as_ref().ok_or_else(no_session)?;
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
        InputDispatch::Dispatched(CommandOutcome::ManualCompaction(instructions)) => {
            send_manual_compaction(session, &srv.out_tx, id, instructions).await
        }
        InputDispatch::Dispatched(CommandOutcome::FrontendFeedback(feedback)) => {
            let text = feedback.to_string();
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
    session.coordinator.as_ref().ok_or_else(no_session)?;
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
            cancel: Some(WorkCancel::Control(trigger)),
            joined: Vec::new(),
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
        })
        .await
        .is_err()
    {
        release_operation(
            &session.pending,
            operation_id,
            OperationKind::ManualCompaction,
        );
        return Err(AcpError::new(-32603, SESSION_ENDED_MESSAGE));
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
                            let error =
                                AcpError::internal_error().data(Value::String(error.clone()));
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
    session.coordinator.as_ref().ok_or_else(no_session)?;
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
            cancel: Some(WorkCancel::Control(trigger)),
            joined: Vec::new(),
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
        .await
        .is_err()
    {
        release_operation(&session.pending, operation_id, OperationKind::IsolatedTurn);
        return Err(AcpError::new(-32603, SESSION_ENDED_MESSAGE));
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
                            let error =
                                AcpError::internal_error().data(Value::String(message.clone()));
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
    let mut input = AgentInput::new(turn.content.text.to_string(), images);
    input.prompt = prompt.map(Box::new);
    session.coordinator.as_ref().ok_or_else(no_session)?;
    let session_ended = || AcpError::new(-32603, SESSION_ENDED_MESSAGE);
    let submission = session
        .handle
        .input_tx
        .admitter()
        .ready()
        .await
        .map_err(|_| session_ended())?;
    // Submitting under the lock records the turn before its events can
    // reach the pump.
    let mut pending = session.pending.lock().unwrap();
    pending.admits_prompt()?;
    let (ticket, run_id) = submission.submit(input).map_err(|error| {
        if error.is_closed() {
            session_ended()
        } else {
            AcpError::new(-32603, error.to_string())
        }
    })?;
    let pending = &mut *pending;
    if let Some(operation) = pending
        .operation
        .iter_mut()
        .chain(&mut pending.queued)
        .find(|operation| operation.run_id == Some(run_id))
    {
        operation.joined.push(id.clone());
        return Ok(());
    }
    let operation = PendingOperation {
        id: NEXT_OPERATION_ID.fetch_add(1, Ordering::Relaxed),
        request_id: id.clone(),
        kind: OperationKind::PrimaryTurn,
        run_id: Some(run_id),
        cancelling: false,
        cancel: Some(session.handle.input_tx.turn_cancel(ticket)),
        joined: Vec::new(),
    };
    if pending.operation.is_some() {
        pending.queued.push_back(operation);
    } else {
        pending.operation = Some(operation);
    }
    Ok(())
}

fn command_error(error: maki_commands::CommandError) -> AcpError {
    AcpError::new(-32602, error.to_string())
}

fn config_error(error: ConfigChangeError) -> AcpError {
    match &error {
        ConfigChangeError::Coordinator(SessionCoordinatorError::StaleSession(id)) => {
            AcpError::resource_not_found(Some(format!("session/{id}")))
        }
        _ if error.is_busy() => AcpError::new(-32600, error.to_string()),
        ConfigChangeError::Mode(ModeError::Unknown(_)) => AcpError::new(-32602, error.to_string()),
        ConfigChangeError::Option(option)
        | ConfigChangeError::Coordinator(SessionCoordinatorError::Option(option))
            if matches!(
                option,
                SessionOptionError::UnknownId(_)
                    | SessionOptionError::InvalidValue { .. }
                    | SessionOptionError::FastUnsupported
                    | SessionOptionError::PolicyRejected(_)
                    | SessionOptionError::CallbackFailed(_)
            ) =>
        {
            AcpError::invalid_params().data(json_str(&error.to_string()))
        }
        _ => AcpError::internal_error().data(json_str(&error.to_string())),
    }
}

fn finish_operation(
    pending: &PendingState,
    operation_id: u64,
    kind: OperationKind,
    respond: impl FnMut(RequestId),
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
        pending.settle()
    };
    answer(operation, respond);
    true
}

/// Answers the prompts of the turn presented as {run_id}, running or queued.
fn finish_primary_run(pending: &PendingState, run_id: u64, respond: impl FnMut(RequestId)) -> bool {
    let operation = {
        let mut pending = pending.lock().unwrap();
        let primary = |operation: &PendingOperation| {
            operation.kind == OperationKind::PrimaryTurn && operation.run_id == Some(run_id)
        };
        if pending.operation.as_ref().is_some_and(primary) {
            pending.settle()
        } else if let Some(index) = pending.queued.iter().position(primary) {
            pending.queued.remove(index).unwrap()
        } else {
            return false;
        }
    };
    answer(operation, respond);
    true
}

/// Drops {operation} first, so its cancellation fires before the answers.
fn answer(operation: PendingOperation, respond: impl FnMut(RequestId)) {
    let requests: Vec<_> = operation.requests().collect();
    drop(operation);
    requests.into_iter().for_each(respond);
}

fn finish_active_operation(
    pending: &PendingState,
    kind: OperationKind,
    respond: impl FnMut(RequestId),
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

fn respond_prompt(out_tx: &Sender<Value>, id: RequestId, reason: StopReason) {
    send(
        out_tx,
        Response::new(
            id,
            Ok(AgentResponse::PromptResponse(PromptResponse::new(reason))),
        ),
    );
}

async fn handle_set_mode(
    srv: &mut Server,
    raw: &Value,
    params: &AcpParams,
) -> Result<AgentResponse, AcpError> {
    let req: SetSessionModeRequest = parse_params(raw)?;
    validate_session(srv, req.session_id.0.as_ref())?;
    let mode_str = req.mode_id.0.to_string();
    let session = srv.session.as_ref().ok_or_else(no_session)?;
    let orchestration = session.orchestration.as_ref().ok_or_else(no_session)?;
    let root = session
        .handle
        .initialized_root()
        .await
        .map_err(|error| AcpError::new(-32603, error))?;
    maki_lua::orchestration::set_mode(
        &**orchestration,
        &root,
        &srv.lua_event_handle.mode_registry(),
        &mode_str,
        &params.storage,
    )
    .await
    .map_err(config_error)?;
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
            maki_lua::SessionOptionMutationError::Config(error) => config_error(error),
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
                let (cancellations, permission_answers, elicitation) = {
                    let mut pending = session.pending.lock().unwrap();
                    let pending = &mut *pending;
                    pending.cancels += 1;
                    let Some(operation) = pending
                        .operation
                        .as_mut()
                        .filter(|operation| !operation.cancelling)
                    else {
                        return;
                    };
                    let cancellations: Vec<_> = iter::once(operation)
                        .chain(&mut pending.queued)
                        .filter_map(PendingOperation::start_cancelling)
                        .collect();
                    let permission_answers = std::mem::take(&mut pending.permissions);
                    let elicitation = pending.elicitation.take();
                    (cancellations, permission_answers, elicitation)
                };
                answer_cancelled_requests(&session.handle, permission_answers, elicitation);
                for cancellation in cancellations {
                    cancellation.cancel();
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
    session: maki_agent::session_coordinator::SessionReadHandle,
    home: Option<PathBuf>,
    project_trusted: bool,
    initial_cost: Option<f64>,
    observer: HeadlessObserver,
) -> EventPump {
    let (stop, stopped) = flume::bounded(1);
    let task = smol::spawn(async move {
        let sid = SessionId::from(session_id.to_string());
        let mut cost_total = initial_cost;
        let mut tool_inputs: HashMap<String, Value> = HashMap::new();
        let mut stopping = false;

        loop {
            let envelope = if stopping {
                match event_rx.try_recv() {
                    Ok(envelope) => envelope,
                    Err(_) => break,
                }
            } else {
                let next = smol::future::or(async { Some(event_rx.recv_async().await) }, async {
                    match stopped.recv_async().await {
                        Ok(()) => None,
                        Err(_) => std::future::pending().await,
                    }
                })
                .await;
                match next {
                    Some(Ok(envelope)) => envelope,
                    Some(Err(_)) => break,
                    None => {
                        stopping = true;
                        continue;
                    }
                }
            };
            observer.observe(&envelope);
            // ACP fences on the presentation the actor stamped, which is the
            // same identity the rest of the frontend uses.
            let run_id = envelope.presentation().unwrap_or_default();
            let (event, subagent) = (envelope.event, envelope.subagent);
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
                    let update =
                        translate::tool_start(&event, session.cwd().as_deref(), home.as_deref());
                    if let Some(raw_input) = &event.raw_input {
                        tool_inputs.insert(event.id.clone(), raw_input.clone());
                    }
                    update
                }
                AgentEvent::ToolExecutionStart { id } => translate::tool_execution_start(&id),
                AgentEvent::ToolOutput { id, content } => translate::tool_output(&id, &content),
                AgentEvent::ToolDone(event) => {
                    translate::tool_done(&event, session.cwd().as_deref(), home.as_deref())
                }
                AgentEvent::TurnComplete(event) => translate::usage_update(&event, cost_total),
                AgentEvent::PermissionRequest { id, tool, scopes } => {
                    let raw_input = permission_answer_tx
                        .is_none()
                        .then(|| tool_inputs.get(&id))
                        .flatten();
                    let request =
                        AgentRequest::RequestPermissionRequest(RequestPermissionRequest::new(
                            sid.clone(),
                            translate::permission_request(
                                &id,
                                format!("{tool}: {}", scopes.join(", ")),
                                &tool.to_string(),
                                raw_input,
                                session.cwd().as_deref(),
                                home.as_deref(),
                            ),
                            permissions::permission_options(project_trusted),
                        ));
                    let request_id = NEXT_OUTGOING_REQUEST_ID.fetch_add(1, Ordering::Relaxed);
                    let should_send = {
                        let mut pending = pending.lock().unwrap();
                        if pending.refuses_asks() {
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
                        let _ =
                            answer_tx.send(TaggedAnswer::new(&id, PermissionAnswer::Deny).encode());
                    }
                    continue;
                }
                AgentEvent::AuthRequired => {
                    tool_inputs.clear();
                    let cancel = pending
                        .lock()
                        .unwrap()
                        .operation
                        .as_mut()
                        .and_then(|operation| operation.cancel.take());
                    finish_active_operation(&pending, OperationKind::PrimaryTurn, |request_id| {
                        let error =
                            AcpError::auth_required().data(Value::String(AUTH_FAILED_MSG.into()));
                        send(
                            &out_tx,
                            Response::<AgentResponse>::new(request_id, Err(error)),
                        );
                    });
                    if let Some(cancel) = cancel {
                        cancel.cancel();
                    }
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
                            if pending.refuses_asks() {
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
                    finish_primary_run(&pending, run_id, |request_id| match &outcome {
                        maki_agent::TurnOutcome::Completed { reason, .. } => {
                            let resp = PromptResponse::new(translate::map_done_reason(*reason));
                            send(
                                &out_tx,
                                Response::new(request_id, Ok(AgentResponse::PromptResponse(resp))),
                            );
                        }
                        maki_agent::TurnOutcome::Cancelled { .. } => {
                            respond_prompt(&out_tx, request_id, StopReason::Cancelled);
                        }
                        maki_agent::TurnOutcome::Failed { failure, .. } => {
                            let error = AcpError::internal_error()
                                .data(Value::String(failure.user_message.clone()));
                            send(
                                &out_tx,
                                Response::<AgentResponse>::new(request_id, Err(error)),
                            );
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
                        let error = AcpError::internal_error().data(Value::String(message.clone()));
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
        let unanswered = {
            let mut pending = pending.lock().unwrap();
            if pending
                .operation
                .as_ref()
                .is_some_and(|operation| operation.kind == OperationKind::PrimaryTurn)
            {
                pending.take_all()
            } else {
                Vec::new()
            }
        };
        for request_id in unanswered.iter().flat_map(PendingOperation::requests) {
            let error = AcpError::internal_error().data(Value::String(EVENT_STREAM_ENDED.into()));
            send(
                &out_tx,
                Response::<AgentResponse>::new(request_id, Err(error)),
            );
        }
    });
    EventPump { task, stop }
}

/// A session's event pump. Finishing it forwards what the session already
/// emitted, such as a background child's last updates, before it stops.
struct EventPump {
    task: smol::Task<()>,
    stop: Sender<()>,
}

impl EventPump {
    #[cfg(test)]
    fn idle() -> Self {
        Self {
            task: smol::spawn(async {}),
            stop: flume::bounded(1).0,
        }
    }

    async fn finish(self) {
        let _ = self.stop.send(());
        self.task.await;
    }

    #[cfg(test)]
    fn detach(self) {
        self.task.detach();
    }
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

/// The mode the session's root committed, which is what a client must show.
async fn committed_mode(srv: &Server) -> Result<String, AcpError> {
    let session = srv.session.as_ref().ok_or_else(no_session)?;
    let root = session
        .handle
        .initialized_root()
        .await
        .map_err(|error| AcpError::new(-32603, error))?;
    let config = root
        .effective_config()
        .map_err(|error| AcpError::new(-32603, error.to_string()))?
        .ok_or_else(|| AcpError::new(-32603, ROOT_CONFIG_UNAVAILABLE))?;
    Ok(config.mode.id().key().to_owned())
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
    use maki_agent::{AgentMode, TurnTicket};
    use maki_commands::TargetCapabilities;
    use maki_lua::orchestration::{OrchestrationServices, TurnCompletion};
    use maki_providers::{ContentBlock as MsgBlock, Role, TokenUsage};
    use maki_storage::StateDir;
    use maki_storage::checkpoint::{ClaimingWriter, accept_all};
    use maki_storage::sessions::StoredMode;
    use maki_storage::sessions::{Effort, Session, StoredThinking};
    use tempfile::TempDir;
    use test_case::test_case;

    use super::*;

    const ANSWERED_ID: i64 = 1001;
    const UNKNOWN_ID: i64 = 1002;
    const DISCOVERED_SPEC: &str = "openrouter/discovered-model";
    const FAST_SPEC: &str = "anthropic/claude-opus-4-8";
    const OFFLINE_SPEC: &str = "openai/gpt-5";
    const SAVED_PLAN_FILE: &str = "saved-plan.md";
    const THINKING_MODEL_ID: &str = "acp-load-thinker";
    const PRIMARY_TURN_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(2);
    const LIFECYCLE_STRESS_ITERATIONS: usize = 64;
    const SPAWN_TEST_SPEC: &str = "ollama/acp-end-turn-test";
    const DISCOVERY_PROVIDER: &str = "ollama";
    const DISCOVERY_MODEL_ID: &str = "acp-discovery-refresh-test";
    const DISCOVERED_WINDOW: u32 = 524_000;
    const NEW_SESSION_REQUEST: i64 = 91;
    static MANAGED_PLUGIN_TEST: Mutex<()> = Mutex::new(());
    const ACP_TURN_END: &str = "turn end ";
    const PARKED_THOUGHT: &str = "parked thought";
    const PARKED_REPLY: &str = "parked reply";
    const SAVE_FAILED: &str = "save failed";
    const TINY_PNG: &str = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+jRZkAAAAASUVORK5CYIIA";

    /// A prompt from start to its dispatched command's outcome, the way the
    /// input loop drives it.
    async fn handle_prompt(srv: &mut Server, raw: &Value, id: &RequestId) -> Result<(), AcpError> {
        start_prompt(srv, raw, id)?;
        let dispatched = (&mut srv.dispatch.as_mut().expect("prompt dispatch").future).await;
        finish_prompt(srv, dispatched).await
    }

    fn test_observer() -> HeadlessObserver {
        HeadlessObserver::new(
            maki_lua::EventHandle::disconnected_for_test(),
            MakiId::generate(),
        )
    }

    struct RecordingAcpProvider {
        name: &'static str,
        requests: Sender<(&'static str, String, bool)>,
        nested: bool,
        permission: bool,
    }

    impl maki_providers::provider::Provider for RecordingAcpProvider {
        fn stream_message<'a>(
            &'a self,
            model: &'a Model,
            messages: &'a [Message],
            system: &'a str,
            tools: &'a Value,
            events: &'a Sender<maki_providers::ProviderEvent>,
            options: maki_providers::RequestOptions,
            session: Option<&'a SessionRef>,
        ) -> maki_providers::provider::BoxFuture<
            'a,
            Result<maki_providers::StreamResponse, maki_agent::AgentError>,
        > {
            self.requests
                .send((self.name, model.spec(), options.fast))
                .unwrap();
            if self.nested {
                let prompt = messages
                    .iter()
                    .filter(|message| matches!(message.role, Role::User))
                    .flat_map(|message| &message.content)
                    .find_map(|block| match block {
                        MsgBlock::Text { text } => Some(text.as_str()),
                        _ => None,
                    })
                    .unwrap_or_default();
                let last_result = messages
                    .iter()
                    .rev()
                    .flat_map(|message| message.content.iter().rev())
                    .find_map(|block| match block {
                        MsgBlock::ToolResult {
                            content, is_error, ..
                        } => {
                            assert!(!is_error, "nested tool failed: {content}");
                            Some(content)
                        }
                        _ => None,
                    });
                let depth = if prompt.contains("grandchild work") {
                    2
                } else if prompt.contains("child work") {
                    1
                } else {
                    0
                };
                let call = |name: &str, input: Value| MsgBlock::ToolUse {
                    id: format!("{name}-{depth}"),
                    name: name.into(),
                    input,
                    thought_signature: None,
                };
                let follow_up = messages
                    .iter()
                    .filter(|message| matches!(message.role, Role::User))
                    .flat_map(|message| &message.content)
                    .any(
                        |block| matches!(block, MsgBlock::Text { text } if text == "after option"),
                    );
                let block = if follow_up {
                    MsgBlock::Text {
                        text: "root result".into(),
                    }
                } else if depth == 2 {
                    MsgBlock::Text {
                        text: "grandchild result".into(),
                    }
                } else if last_result.is_none() {
                    if self.permission && depth == 1 {
                        call("acp_child_permission", serde_json::json!({}))
                    } else {
                        call(
                            "task_spawn",
                            serde_json::json!({
                                "description": if depth == 0 { "ACP child" } else { "ACP grandchild" },
                                "prompt": if depth == 0 { "child work" } else { "grandchild work" },
                                "subagent_type": "general",
                            }),
                        )
                    }
                } else if let Some(result) = last_result
                    && let Ok(spawned) = serde_json::from_str::<Value>(result)
                    && spawned.get("task_id").is_some()
                {
                    call(
                        "task_wait",
                        serde_json::json!({"task_id": spawned["task_id"], "turn_id": spawned["turn_id"]}),
                    )
                } else {
                    let expected = if self.permission && depth == 1 {
                        "permission granted"
                    } else if depth == 1 {
                        "grandchild result"
                    } else {
                        "child result"
                    };
                    assert!(
                        last_result.is_some_and(|result| result.contains(expected)),
                        "unexpected nested result: {last_result:?}"
                    );
                    MsgBlock::Text {
                        text: if depth == 0 {
                            "root result"
                        } else {
                            "child result"
                        }
                        .into(),
                    }
                };
                let stop_reason = if matches!(block, MsgBlock::ToolUse { .. }) {
                    maki_providers::StopReason::ToolUse
                } else {
                    maki_providers::StopReason::EndTurn
                };
                return Box::pin(async move {
                    Ok(maki_providers::StreamResponse {
                        message: Message {
                            role: Role::Assistant,
                            content: vec![block],
                            ..Default::default()
                        },
                        usage: TokenUsage::default(),
                        stop_reason: Some(stop_reason),
                    })
                });
            }
            maki_providers::provider::Provider::stream_message(
                &EndTurnProvider,
                model,
                messages,
                system,
                tools,
                events,
                options,
                session,
            )
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

    async fn managed_test_session(
        srv: &mut Server,
        nested: bool,
        permission: bool,
        completion: Option<TurnCompletion>,
        checkpoint: Arc<
            dyn maki_storage::checkpoint::CheckpointWriter<
                    maki_agent::session_coordinator::SessionCheckpoint,
                >,
        >,
    ) -> (
        maki_lua::PluginHost,
        Receiver<(&'static str, String, bool)>,
        Receiver<String>,
        TempDir,
        Receiver<String>,
    ) {
        let (requests, received) = flume::unbounded();
        let provider: Arc<dyn maki_providers::provider::Provider> =
            Arc::new(RecordingAcpProvider {
                name: "managed provider",
                requests,
                nested,
                permission,
            });
        let prepared = Arc::clone(&provider);
        let registry = Arc::clone(maki_agent::tools::ToolRegistry::global_arc());
        let (ui, notices) = maki_lua::test_support::RecordingUi::new();
        let host = maki_lua::PluginHost::with_session_provider_preparer(
            registry,
            Default::default(),
            true,
            Arc::new(maki_lua::test_support::InMemoryFs::new()),
            None,
            Some(Arc::new(move |model| Ok((model, Arc::clone(&prepared))))),
            maki_lua::ui_sink::HostUi::Sink(Box::new(ui)),
        )
        .unwrap();
        host.load_source(
            "acp_bundled_task",
            include_str!("../../plugins/task/init.lua"),
        )
        .unwrap();
        host.load_source(
            "acp_options",
            &format!(
                r#"
            maki.api.register_command({{
                name = "/acp_fast", tui_only = false,
                handler = function(_, ctx)
                    local root = assert(ctx:agent())
                    assert(assert(root:set_model(ctx, {{ fast = true }})).fast)
                    assert(assert(root:config(ctx)).model.fast)
                    local config = assert(root:config(ctx))
                    assert(config.model.fast and config.model.spec == "{FAST_SPEC}", maki.json.encode(config))
                    local view, view_err = maki.fn.winsaveview()
                    assert(view == nil and view_err == "{no_ui}", view_err)
                end,
            }})
            maki.api.create_autocmd("TurnEnd", {{
                callback = function(ev)
                    maki.ui.flash("{ACP_TURN_END}" .. ev.data.session_id)
                end,
            }})
        "#,
                no_ui = maki_lua::ui_sink::NO_UI_ERR
            ),
        )
        .unwrap();
        if permission {
            host.load_source("acp_child_permission", r#"
                maki.api.register_tool({
                    name = "acp_child_permission", description = "request a child permission",
                    schema = { type = "object", properties = {} }, audiences = { "main", "general_sub" },
                    permission = "run", permission_scopes = function() return { scopes = { "ACP child permission" }, force_prompt = true } end,
                    handler = function() return "permission granted" end,
                })
            "#).unwrap();
        }
        let session = srv.session.as_mut().unwrap();
        let id = session.handle.session_id.clone();
        session.coordinator.take().unwrap().close().await.unwrap();
        let coordinator = test_coordinator_with(
            id.id(),
            FAST_SPEC,
            host.event_handle().session_option_catalog(),
            checkpoint,
        );
        let initial = Arc::clone(&provider);
        let (root_answers_tx, root_answers_rx) = flume::bounded(1);
        let (binding, root_target) = maki_lua::orchestration::root_binding();
        let handle = headless::spawn_interactive_with_binding(
            InteractiveParams {
                model: Model::from_spec(FAST_SPEC).unwrap(),
                config: maki_agent::AgentConfig {
                    max_concurrent_agent_turns: 1,
                    ..Default::default()
                },
                permissions_config: maki_config::PermissionsConfig {
                    default: maki_config::DefaultEffect::Prompt,
                    ..Default::default()
                },
                timeouts: Default::default(),
                prompt_slots: Default::default(),
                excluded_tools: Vec::new(),
                mcp_handle: None,
                initial_wd: PathBuf::from("/tmp"),
                session_id: Some(id.clone()),
                initial_history: Vec::new(),
                yolo: false,
                system_prompt_override: Some(String::new()),
                append_system_prompt: None,
                defaults: Default::default(),
                model_policy: Arc::default(),
                modes: Arc::new(maki_agent::ModeRegistry::builtin()),
                question_mode: QuestionMode::Headless,
                plugin_rules: Arc::default(),
                project_config: ProjectConfig::for_project(Path::new("/tmp")),
                local_tools: Default::default(),
                state_dir: None,
                answers: true,
            },
            Box::new(move |model, _| Box::pin(async move { Ok((model, initial)) })),
            None,
            Some(Box::new(move |manager, target, template| {
                Box::pin(async move {
                    let answers = template
                        .user_response_rx
                        .as_ref()
                        .unwrap()
                        .lock()
                        .await
                        .clone();
                    root_answers_tx.send_async(answers).await.unwrap();
                    binding(manager, target, template).await
                })
            })),
        );
        let replacement = Arc::clone(&provider);
        let root = maki_lua::orchestration::bind_frontend_root(
            &handle,
            &coordinator,
            &root_target,
            Arc::new(move |model| {
                let provider = Arc::clone(&replacement);
                Box::pin(async move { Ok(maki_agent::actor::PreparedModel { provider, model }) })
            }),
        )
        .await
        .unwrap();
        let state = TempDir::new().unwrap();
        let child_checkpoint = Arc::new(
            maki_agent::session_checkpoint::SessionLogCheckpoint::open(
                StateDir::from_path(state.path().to_path_buf()),
                id.id(),
                FAST_SPEC,
                "/tmp",
                None,
            )
            .unwrap(),
        );
        let services = Arc::new_cyclic(|services| {
            let (child_config, persist) = maki_lua::orchestration::child_persistence(
                "ACP",
                services.clone(),
                child_checkpoint,
            );
            let completion: TurnCompletion = match completion {
                Some(gate) => Arc::new(move |target, history| {
                    let gate = gate(target, history.clone());
                    let persist = persist(target, history);
                    Box::pin(async move {
                        gate.await?;
                        persist.await
                    })
                }),
                None => persist,
            };
            maki_lua::orchestration::frontend_root(
                &ModelCatalog::default(),
                root.clone(),
                child_config,
                coordinator.clone(),
            )
            .with_completion(completion)
        });
        let observer = HeadlessObserver::new(host.event_handle(), id.id());
        observer.install(root.target.clone());
        host.event_handle()
            .install_orchestration_services(services.clone());
        let root_answers = root_answers_rx.recv_async().await.unwrap();
        let previous = std::mem::replace(&mut session.handle, handle);
        previous.task.cancel().await;
        session.command_registry = host.command_registry();
        session.command_target = session_command_target(
            &session.command_registry,
            &session.handle,
            &coordinator,
            &root,
            &services,
            headless::default_model_preparer(Default::default()),
        );
        session.coordinator = Some(coordinator.clone());
        session.option_projection = Some(Arc::new(OptionProjection {
            out_tx: srv.out_tx.clone(),
            session_id: SessionId::from(session.handle.session_id.to_string()),
            read: coordinator.read(),
            supports_boolean: false,
            emitted_version: Mutex::new(coordinator.read().options().version),
        }));
        session.pending.lock().unwrap().permissions.clear();
        start_event_pump(
            session.handle.event_rx.clone(),
            id,
            srv.out_tx.clone(),
            Arc::clone(&session.pending),
            false,
            session.handle.answer_tx.clone(),
            coordinator.read(),
            None,
            PUMP_TRUSTED,
            None,
            observer,
        )
        .detach();
        srv.lua_event_handle = host.event_handle();
        (host, received, root_answers, state, notices)
    }

    async fn prompt_terminal(out: &Receiver<Value>, id: i64) -> Value {
        smol::future::or(
            async {
                loop {
                    let response = out.recv_async().await.unwrap();
                    if response["id"] == id {
                        return response;
                    }
                    assert!(
                        response.get("result").is_none() && response.get("error").is_none(),
                        "leaked terminal: {response}"
                    );
                }
            },
            async {
                smol::Timer::after(PRIMARY_TURN_TIMEOUT).await;
                panic!("ACP prompt {id} did not complete");
            },
        )
        .await
    }

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

    /// Records the last user message of every request and parks it until
    /// the test releases it, then thinks and answers.
    struct ParkedProvider {
        requests: Sender<Message>,
        release: Receiver<()>,
    }

    impl maki_providers::provider::Provider for ParkedProvider {
        fn stream_message<'a>(
            &'a self,
            _: &'a Model,
            messages: &'a [Message],
            _: &'a str,
            _: &'a Value,
            events: &'a Sender<maki_providers::ProviderEvent>,
            _: maki_providers::RequestOptions,
            _: Option<&'a SessionRef>,
        ) -> maki_providers::provider::BoxFuture<
            'a,
            Result<maki_providers::StreamResponse, maki_agent::AgentError>,
        > {
            Box::pin(async move {
                let asked = messages
                    .iter()
                    .rev()
                    .find(|message| matches!(message.role, Role::User))
                    .cloned()
                    .unwrap_or_default();
                let _ = self.requests.send(asked);
                let _ = self.release.recv_async().await;
                for event in [
                    maki_providers::ProviderEvent::ThinkingDelta {
                        text: PARKED_THOUGHT.into(),
                    },
                    maki_providers::ProviderEvent::TextDelta {
                        text: PARKED_REPLY.into(),
                    },
                ] {
                    let _ = events.send(event);
                }
                Ok(maki_providers::StreamResponse {
                    message: Message {
                        role: Role::Assistant,
                        content: vec![MsgBlock::Text {
                            text: PARKED_REPLY.into(),
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

    /// The fixture root's real actor, and what its provider was asked.
    struct RootTurns {
        actor: maki_agent::AgentActorHandle,
        requests: Receiver<Message>,
        release: Sender<()>,
    }

    impl RootTurns {
        async fn next(&self) -> Message {
            within(
                self.requests.recv_async(),
                "no request reached the provider",
            )
            .await
            .unwrap()
        }

        async fn next_text(&self) -> String {
            text_of(&self.next().await)
        }

        /// Lets the oldest parked request answer.
        fn release(&self) {
            self.release.send(()).unwrap();
        }

        fn latest(&self) -> TurnTicket {
            self.actor.latest_ticket().expect("a turn was admitted")
        }

        /// Nothing was admitted or queued on the actor, and nothing reached
        /// the provider.
        fn assert_untouched(&self) {
            let snapshot = self.actor.snapshot();
            assert!(self.actor.latest_ticket().is_none(), "a turn was admitted");
            assert_eq!(snapshot.queued, 0);
            assert!(self.requests.is_empty(), "the provider was asked");
        }

        async fn assert_cancelled(&self, ticket: &TurnTicket) {
            let outcome = within(ticket.wait(), "the turn never settled").await;
            assert!(
                matches!(outcome, maki_agent::TurnOutcome::Cancelled { .. }),
                "{outcome:?}"
            );
        }
    }

    fn images_of(message: &Message) -> Vec<&ImageSource> {
        message
            .content
            .iter()
            .filter_map(|block| match block {
                MsgBlock::Image { source } => Some(source),
                _ => None,
            })
            .collect()
    }

    fn text_of(message: &Message) -> String {
        message
            .content
            .iter()
            .filter_map(|block| match block {
                MsgBlock::Text { text } => Some(text.as_str()),
                _ => None,
            })
            .collect()
    }

    async fn within<T>(work: impl std::future::Future<Output = T>, failure: &str) -> T {
        smol::future::or(work, async {
            smol::Timer::after(PRIMARY_TURN_TIMEOUT).await;
            panic!("{failure}");
        })
        .await
    }

    fn take_operation(
        pending: &PendingState,
        operation_id: u64,
        kind: OperationKind,
    ) -> Option<RequestId> {
        let mut request_id = None;
        finish_operation(pending, operation_id, kind, |id| request_id = Some(id));
        request_id
    }

    fn take_active_operation(pending: &PendingState, kind: OperationKind) -> Option<RequestId> {
        let mut request_id = None;
        finish_active_operation(pending, kind, |id| request_id = Some(id));
        request_id
    }

    fn test_params(model: Model, cwd: PathBuf) -> AcpParams {
        AcpParams {
            model,
            config: Default::default(),
            permissions_config: Default::default(),
            timeouts: Default::default(),
            initial_wd: cwd.clone(),
            storage: StateDir::from_path(cwd),
            prompt_slots: Default::default(),
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
        control_tx: maki_agent::headless::InteractiveControlSender,
        coordinator: maki_agent::session_coordinator::SessionCoordinatorHandle,
    ) -> TargetHandle {
        {
            registry.bind_target_with_presentation(
                maki_agent::command::session_capabilities(),
                maki_agent::command::portable_capabilities(),
                Arc::new(
                    maki_agent::command::SessionCommandHost::new(control_tx, coordinator)
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
        accept_all()
    }

    fn test_coordinator(
        session_id: MakiId,
        model: &str,
    ) -> maki_agent::session_coordinator::SessionCoordinatorHandle {
        test_coordinator_with(
            session_id,
            model,
            Default::default(),
            successful_checkpoint(),
        )
    }

    fn test_coordinator_with(
        session_id: MakiId,
        model: &str,
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
                model_policy: Arc::new(maki_config::ModelPolicy::default()),
                checkpoint: Arc::new(ClaimingWriter(checkpoint)),
                mailbox: maki_agent::SessionMailbox::new(session_id),
            },
        )
        .unwrap()
    }

    #[test]
    fn acp_restoration_preserves_compacted_history() {
        let tmp = TempDir::new().unwrap();
        let storage = StateDir::from_path(tmp.path().to_path_buf());
        let mut session: Session<Message, TokenUsage, maki_agent::ToolOutput> =
            Session::new(OFFLINE_SPEC, "/project");
        session.meta.compacted = true;
        session.save(&storage).unwrap();
        let restored = load_history_from(&storage, session.id).unwrap();
        assert!(restored.history.compacted);
        assert_eq!(
            serde_json::to_value(restored.history.messages.as_ref()).unwrap(),
            serde_json::to_value(session.history_snapshot().messages.as_ref()).unwrap()
        );
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

    fn server_awaiting_answer() -> (Server, Receiver<String>, Receiver<Value>, RootTurns) {
        server_awaiting_answer_with_checkpoint(successful_checkpoint())
    }

    fn server_awaiting_answer_with_checkpoint(
        checkpoint: Arc<
            dyn maki_storage::checkpoint::CheckpointWriter<
                    maki_agent::session_coordinator::SessionCheckpoint,
                >,
        >,
    ) -> (Server, Receiver<String>, Receiver<Value>, RootTurns) {
        let (answer_tx, answer_rx) = flume::unbounded();
        let (out_tx, out_rx) = flume::unbounded();
        let (requests, requests_rx) = flume::unbounded();
        let (release, released) = flume::unbounded();
        let provider: Arc<dyn maki_providers::provider::Provider> = Arc::new(ParkedProvider {
            requests,
            release: released,
        });
        let prepared = Arc::clone(&provider);
        let session_id = MakiId::generate();
        let mut handle = headless::spawn_interactive_with_binding(
            InteractiveParams {
                model: Model::from_spec(OFFLINE_SPEC).unwrap(),
                config: Default::default(),
                permissions_config: Default::default(),
                timeouts: Default::default(),
                prompt_slots: Default::default(),
                excluded_tools: Vec::new(),
                mcp_handle: None,
                initial_wd: PathBuf::from("/project"),
                session_id: Some(SessionRef::from(session_id)),
                modes: Arc::new(maki_agent::ModeRegistry::builtin()),
                initial_history: Vec::new(),
                yolo: false,
                system_prompt_override: Some(String::new()),
                append_system_prompt: None,
                defaults: Default::default(),
                model_policy: Arc::default(),
                question_mode: QuestionMode::Headless,
                plugin_rules: Arc::default(),
                project_config: ProjectConfig::for_project(Path::new("/project")),
                local_tools: Default::default(),
                state_dir: None,
                answers: true,
            },
            Box::new(move |model, _| Box::pin(async move { Ok((model, prepared)) })),
            None,
            None,
        );
        let root = smol::block_on(handle.initialized_root()).unwrap();
        handle.answer_tx = answer_tx.clone();
        let turns = RootTurns {
            actor: root.actor().unwrap(),
            requests: requests_rx,
            release,
        };
        let coordinator = test_coordinator_with(
            handle.session_id.id(),
            OFFLINE_SPEC,
            Default::default(),
            checkpoint,
        );
        coordinator
            .bind_actor(
                root.clone(),
                Arc::new(move |model| {
                    let provider = Arc::clone(&provider);
                    Box::pin(
                        async move { Ok(maki_agent::actor::PreparedModel { model, provider }) },
                    )
                }),
                Arc::new({
                    let model = handle.model.clone();
                    move |commit| {
                        model.install(
                            Arc::clone(&commit.config.provider),
                            commit.config.model.clone(),
                        );
                    }
                }),
            )
            .unwrap();
        maki_lua::orchestration::project_yolo(&coordinator, Arc::clone(&handle.permissions));
        let command_registry = test_registry(&[]);
        let command_target = command_registry.bind_target_with_presentation(
            maki_agent::command::session_capabilities(),
            maki_agent::command::portable_capabilities(),
            Arc::new(
                maki_agent::command::SessionCommandHost::new(
                    handle.control_tx.clone(),
                    coordinator.clone(),
                )
                .with_reset_session_guidance(NEW_SESSION_GUIDANCE),
            ),
        );
        let server = Server {
            out_tx: out_tx.clone(),
            catalog: ModelCatalog::default(),
            session: Some(SessionState {
                option_projection: Some(Arc::new(OptionProjection {
                    out_tx: out_tx.clone(),
                    session_id: SessionId::from(handle.session_id.to_string()),
                    read: coordinator.read(),
                    supports_boolean: false,
                    emitted_version: Mutex::new(coordinator.read().options().version),
                })),
                handle,
                coordinator: Some(coordinator),
                orchestration: None,
                checkpoint: None,
                mcp: None,
                pending: Arc::new(Mutex::new(Pending {
                    permissions: HashMap::from([(ANSWERED_ID, ("toolu_1".to_string(), answer_tx))]),
                    ..Default::default()
                })),
                command_registry,
                command_target,
                command_projection_task: smol::spawn(async {}),
                event_pump: EventPump::idle(),
                option_projection_task: smol::spawn(async {}),
                lock: None,
            }),
            elicitation: false,
            supports_boolean: false,
            lua_event_handle: maki_lua::EventHandle::disconnected_for_test(),
            dispatch: None,
        };
        (server, answer_rx, out_rx, turns)
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
                catalog: ModelCatalog::default(),
                session: None,
                elicitation: false,
                supports_boolean: false,
                lua_event_handle: params.lua_event_handle.clone(),
                dispatch: None,
            };
            let (handle, root_target) = spawn_session(
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
                    permissions: PermissionsConfig::default(),
                },
            );
            let input_tx = handle.input_tx.clone();
            let persisted_options = BTreeMap::new();

            let error = install_session(
                &mut srv,
                &params,
                InstallSession {
                    handle,
                    root_target,
                    mcp: None,
                    current_model: OFFLINE_SPEC.to_owned(),
                    initial_cost: None,
                    cwd: cwd.path().to_path_buf(),
                    defaults: SessionDefaults::default(),
                    persisted_options: &persisted_options,
                },
            )
            .await
            .unwrap_err();

            assert!(!error.is_empty());
            assert!(
                input_tx
                    .admit(AgentInput::new(String::new(), Vec::new()))
                    .await
                    .is_err(),
                "session task was not cancelled"
            );
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
                catalog: ModelCatalog::default(),
                session: None,
                elicitation: false,
                supports_boolean: false,
                lua_event_handle: params.lua_event_handle.clone(),
                dispatch: None,
            };
            let session_id = SessionRef::from(MakiId::generate());
            let persisted_options = BTreeMap::new();

            let (first, root_target) = spawn_session(
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
                    permissions: PermissionsConfig::default(),
                },
            );
            let error = install_session_with_lock(
                &mut srv,
                &params,
                InstallSession {
                    handle: first,
                    root_target,
                    mcp: None,
                    current_model: OFFLINE_SPEC.to_owned(),
                    initial_cost: None,
                    cwd: cwd.path().to_path_buf(),
                    defaults: SessionDefaults::default(),
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

            let retry = test_coordinator(session_id.id(), OFFLINE_SPEC);
            retry.close().await.unwrap();
        });
    }

    #[test]
    fn close_retires_routing_with_retained_coordinator() {
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
            close_session(&mut srv).await;
            let replacement = test_coordinator(id, OFFLINE_SPEC);
            assert!(
                coordinator
                    .set_option(
                        maki_agent::session_options::YOLO_OPTION_ID,
                        maki_agent::session_options::ENABLED_VALUE
                    )
                    .await
                    .is_err()
            );
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
                joined: Vec::new(),
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
                joined: Vec::new(),
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
                joined: Vec::new(),
            }),
            ..Pending::default()
        }));

        assert!(take_active_operation(&pending, OperationKind::PrimaryTurn).is_none());
        assert!(take_active_operation(&pending, OperationKind::TestLocal).is_some());
    }

    #[test]
    fn primary_prompt_retains_pending_operation_until_terminal() {
        smol::block_on(async {
            let (mut srv, _, _, turns) = server_awaiting_answer();
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
            assert_eq!(turns.next_text().await, "hello");

            let pending = &srv.session.as_ref().unwrap().pending;
            assert!(pending.lock().unwrap().operation.is_some());
            let operation = take_active_operation(pending, OperationKind::PrimaryTurn).unwrap();
            assert_eq!(operation, request_id);
            assert!(take_active_operation(pending, OperationKind::PrimaryTurn).is_none());
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
            session.option_projection = Some(Arc::new(OptionProjection {
                out_tx: srv.out_tx.clone(),
                session_id: SessionId::from(session.handle.session_id.to_string()),
                read: coordinator.read(),
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
                        Ok(maki_storage::checkpoint::CheckpointAck::for_request(
                            &request,
                        ))
                    }) as maki_storage::checkpoint::CheckpointFuture
                },
            )
        };
        let coordinator = test_coordinator_with(
            session_id,
            OFFLINE_SPEC,
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

    const GHOST_MODE: &str = "ghost";
    const UNREACHABLE_OLLAMA_HOST: &str = "http://127.0.0.1:1";

    #[test]
    fn closing_a_session_releases_its_orchestration() {
        // SAFETY: nextest runs each test in its own process.
        unsafe { std::env::set_var("OLLAMA_HOST", UNREACHABLE_OLLAMA_HOST) };
        smol::block_on(async {
            let cwd = TempDir::new().unwrap();
            let host = maki_lua::PluginHost::new(Arc::new(maki_agent::tools::ToolRegistry::new()))
                .unwrap();
            let mut params = test_params(
                Model::from_spec(SPAWN_TEST_SPEC).unwrap(),
                cwd.path().to_path_buf(),
            );
            params.lua_event_handle = host.event_handle();
            let (out_tx, _out_rx) = flume::unbounded();
            let mut srv = Server {
                out_tx,
                catalog: ModelCatalog::default(),
                session: None,
                elicitation: false,
                supports_boolean: false,
                lua_event_handle: host.event_handle(),
                dispatch: None,
            };
            new_session(
                &mut srv,
                &serde_json::json!({ "params": { "cwd": cwd.path(), "mcpServers": [] } }),
                &params,
            )
            .await
            .unwrap();
            let orchestration = Arc::clone(
                srv.session
                    .as_ref()
                    .unwrap()
                    .orchestration
                    .as_ref()
                    .unwrap(),
            );
            close_session(&mut srv).await;
            host.wait_for_worker_barrier_for_test();
            assert_eq!(Arc::strong_count(&orchestration), 1);
        });
    }

    /// A malformed permissions file fails session/new before the live
    /// session is closed, so the client keeps the session it had.
    #[test]
    fn broken_permissions_keep_the_live_session() {
        smol::block_on(async {
            let (mut srv, _, _, turns) = server_awaiting_answer();
            let live = srv.session.as_ref().unwrap().handle.session_id.to_string();
            let cwd = TempDir::new().unwrap();
            let project = cwd.path().join(maki_config::PROJECT_DIR);
            std::fs::create_dir_all(&project).unwrap();
            std::fs::write(project.join("permissions.toml"), "[bash]\ndeyn = [\"x\"]\n").unwrap();
            let params = test_params(
                Model::from_spec(SPAWN_TEST_SPEC).unwrap(),
                cwd.path().to_path_buf(),
            );
            let error = new_session(
                &mut srv,
                &serde_json::json!({ "params": { "cwd": cwd.path(), "mcpServers": [] } }),
                &params,
            )
            .await
            .unwrap_err();
            assert_eq!(error.code, (-32603).into(), "{error:?}");
            assert_eq!(
                srv.session.as_ref().unwrap().handle.session_id.to_string(),
                live
            );
            turns.assert_untouched();
        });
    }

    #[test_case("plan", false ; "plan_lands_in_the_state_dir")]
    #[test_case(GHOST_MODE, true ; "unknown_mode_is_invalid_params")]
    fn set_mode_commits_through_the_shared_setter(mode: &str, invalid: bool) {
        // SAFETY: nextest runs each test in its own process.
        unsafe { std::env::set_var("OLLAMA_HOST", UNREACHABLE_OLLAMA_HOST) };
        smol::block_on(async {
            let cwd = TempDir::new().unwrap();
            let params = test_params(
                Model::from_spec(SPAWN_TEST_SPEC).unwrap(),
                cwd.path().to_path_buf(),
            );
            let (out_tx, _out_rx) = flume::unbounded();
            let mut srv = Server {
                out_tx,
                catalog: ModelCatalog::default(),
                session: None,
                elicitation: false,
                supports_boolean: false,
                lua_event_handle: params.lua_event_handle.clone(),
                dispatch: None,
            };
            new_session(
                &mut srv,
                &serde_json::json!({ "params": { "cwd": cwd.path(), "mcpServers": [] } }),
                &params,
            )
            .await
            .unwrap();
            let session = srv.session.as_ref().unwrap();
            let session_id = session.handle.session_id.to_string();
            let root = session.handle.initialized_root().await.unwrap();
            let result = handle_set_mode(
                &mut srv,
                &serde_json::json!({ "params": { "sessionId": session_id, "modeId": mode } }),
                &params,
            )
            .await;
            assert_eq!(
                result.as_ref().err().map(|error| error.code),
                invalid.then(|| AcpError::invalid_params().code)
            );
            let config = root.effective_config().unwrap().unwrap();
            if !invalid {
                let plan = config.mode.plan_path().unwrap();
                assert!(plan.starts_with(cwd.path().join(maki_storage::plans::PLANS_DIR)));
                assert_eq!(
                    config.mode_def,
                    params
                        .lua_event_handle
                        .mode_registry()
                        .get(&maki_agent::ModeId::Plan)
                );
            } else {
                assert_eq!(config.mode, maki_agent::AgentMode::Build);
            }
            close_session(&mut srv).await;
        });
    }

    #[test]
    fn acp_rejects_stale_session_ids_for_prompt_set_option_mode_and_cancel() {
        let (mut srv, answer_rx, _, turns) = server_awaiting_answer();
        let stale = MakiId::generate().to_string();
        let prompt_error = smol::block_on(handle_prompt(
            &mut srv,
            &prompt_request(&stale, "hello", false),
            &RequestId::Number(9),
        ))
        .unwrap_err();
        assert_eq!(prompt_error.code, AcpError::resource_not_found(None).code);
        turns.assert_untouched();

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

        let params = test_params(Model::from_spec(SPAWN_TEST_SPEC).unwrap(), PathBuf::new());
        let mode_error = smol::block_on(handle_set_mode(
            &mut srv,
            &serde_json::json!({
                "params": {
                    "sessionId": stale,
                    "modeId": "build"
                }
            }),
            &params,
        ))
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
            joined: Vec::new(),
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
            joined: Vec::new(),
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
            let (mut srv, answer_rx, out_rx, turns) = server_awaiting_answer();
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
                session.coordinator.as_ref().unwrap().read(),
                maki_storage::paths::home(),
                PUMP_TRUSTED,
                None,
                test_observer(),
            )
            .detach();
            let session_id = session.handle.session_id.to_string();

            handle_prompt(
                &mut srv,
                &prompt_request(&session_id, "first", false),
                &RequestId::Number(41),
            )
            .await
            .unwrap();
            assert_eq!(turns.next_text().await, "first");
            let first = turns.latest();
            event_tx
                .send_async(Envelope {
                    event: AgentEvent::PermissionRequest {
                        id: "tool-1".to_string(),
                        tool: maki_config::ToolKey::Native(Arc::from("bash")),
                        scopes: vec!["echo first".to_string()],
                    },
                    subagent: None,
                    identity: None,
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
            turns.assert_cancelled(&first).await;

            event_tx
                .send_async(Envelope {
                    event: AgentEvent::PermissionRequest {
                        id: "tool-stale".to_string(),
                        tool: maki_config::ToolKey::Native(Arc::from("bash")),
                        scopes: vec!["echo stale".to_string()],
                    },
                    subagent: None,
                    identity: None,
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
            assert_eq!(
                turns.latest().turn_id(),
                first.turn_id(),
                "successor must not enter the active turn"
            );

            let pending = &srv.session.as_ref().unwrap().pending;
            event_tx
                .send_async(primary_envelope(
                    pending,
                    AgentEvent::ControlComplete {
                        usage: TokenUsage::default(),
                    },
                ))
                .await
                .unwrap();
            let cancelled = out_rx.recv_async().await.unwrap();
            assert_eq!(cancelled["id"], 41);
            assert_eq!(cancelled["result"]["stopReason"], "end_turn");
            handle_prompt(&mut srv, &second, &RequestId::Number(43))
                .await
                .unwrap();
            assert_eq!(turns.next_text().await, "second");
        });
    }

    #[test]
    fn new_session_response_precedes_available_commands() {
        let (srv, _, out_rx, _) = server_awaiting_answer();
        let session_id = srv.session.as_ref().unwrap().handle.session_id.to_string();
        let response = methods::new_session_response(
            &session_id,
            maki_agent::ModeId::Build.key(),
            &srv.lua_event_handle.mode_registry(),
        );

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

    /// A prompt the actor refuses for its size is an error with the actor's
    /// reason; the session lives on, so it is not reported as ended.
    #[test]
    fn refused_prompt_is_an_error_not_the_end() {
        smol::block_on(async {
            let (mut srv, _, _, _) = server_awaiting_answer();
            let session_id = srv.session.as_ref().unwrap().handle.session_id.to_string();
            let text = "a".repeat(maki_agent::actor::MAX_PENDING_INSTRUCTION_BYTES + 1);
            let error = handle_prompt(
                &mut srv,
                &prompt_request(&session_id, &text, false),
                &RequestId::Number(41),
            )
            .await
            .unwrap_err();
            assert_eq!(
                error.message,
                maki_agent::ActorError::InstructionOverflow.to_string()
            );
        });
    }

    #[test]
    fn cancelled_prompt_rejects_follow_up_and_processes_new_session() {
        smol::block_on(async {
            let (mut srv, _, out_rx, turns) = server_awaiting_answer();
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
            turns.next().await;
            let first = turns.latest();

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
            turns.assert_cancelled(&first).await;

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
                let (mut srv, _, out_rx, turns) = server_awaiting_answer();
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
                    session.coordinator.as_ref().unwrap().read(),
                    maki_storage::paths::home(),
                    PUMP_TRUSTED,
                    None,
                    test_observer(),
                )
                .detach();
                let session_id = session.handle.session_id.to_string();
                let request_id = RequestId::Number(iteration as i64 + 1);

                handle_prompt(
                    &mut srv,
                    &prompt_request(&session_id, "primary", false),
                    &request_id,
                )
                .await
                .unwrap();
                turns.next().await;
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
                let pending = &srv.session.as_ref().unwrap().pending;
                event_tx
                    .send_async(primary_envelope(pending, event))
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
            // Holds the test's own checkpoint; the cancelled turn's passes.
            let gated = Arc::new(std::sync::atomic::AtomicBool::new(true));
            let checkpoint: Arc<
                dyn maki_storage::checkpoint::CheckpointWriter<
                        maki_agent::session_coordinator::SessionCheckpoint,
                    >,
            > = Arc::new(
                move |request: maki_storage::checkpoint::CheckpointRequest<_>| {
                    let checkpoint_started_tx = checkpoint_started_tx.clone();
                    let checkpoint_release_rx = checkpoint_release_rx.clone();
                    let gate = gated.swap(false, Ordering::Relaxed);
                    Box::pin(async move {
                        if gate {
                            checkpoint_started_tx.send_async(()).await.unwrap();
                            checkpoint_release_rx.recv_async().await.unwrap();
                        }
                        Ok(maki_storage::checkpoint::CheckpointAck::for_request(
                            &request,
                        ))
                    }) as maki_storage::checkpoint::CheckpointFuture
                },
            );
            let (mut srv, _, out_rx, turns) = server_awaiting_answer_with_checkpoint(checkpoint);
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
                session.coordinator.as_ref().unwrap().read(),
                None,
                PUMP_TRUSTED,
                None,
                test_observer(),
            )
            .detach();

            handle_prompt(
                &mut srv,
                &prompt_request(&session_id, "first", false),
                &RequestId::Number(41),
            )
            .await
            .unwrap();
            turns.next().await;
            let first = turns.latest();
            let coordinator = srv
                .session
                .as_ref()
                .unwrap()
                .coordinator
                .as_ref()
                .unwrap()
                .clone();
            // The pump treats a closed stream as session end and settles
            // whatever operation is active, so the sender stays alive for
            // the whole test like the session stream does in production.
            let checkpoint_events = event_tx.clone();
            let checkpoint_complete = primary_envelope(
                &pending,
                AgentEvent::ControlComplete {
                    usage: TokenUsage::default(),
                },
            );
            let checkpoint_task = smol::spawn(async move {
                coordinator
                    .persist_history(maki_agent::agent::HistorySnapshot::new(vec![
                        Message::user("first".into()),
                    ]))
                    .await
                    .unwrap();
                checkpoint_events
                    .send_async(checkpoint_complete)
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
            turns.assert_cancelled(&first).await;

            handle_prompt(
                &mut srv,
                &prompt_request(&session_id, "second", false),
                &RequestId::Number(42),
            )
            .await
            .unwrap();
            assert_eq!(turns.next_text().await, "second");
            assert!(operation_state(&pending).is_some_and(|(cancelling, _)| !cancelling));
            take_active_operation(&pending, OperationKind::PrimaryTurn).unwrap();
            assert!(out_rx.is_empty());
            drop(event_tx);
        });
    }

    /// Whether the active operation is cancelling and still holds a cancel.
    fn operation_state(pending: &PendingState) -> Option<(bool, bool)> {
        pending
            .lock()
            .unwrap()
            .operation
            .as_ref()
            .map(|operation| (operation.cancelling, operation.cancel.is_some()))
    }

    /// stdin closed: the prompt in flight still runs to its end, while the
    /// ask nobody can answer any more is denied and new asks are refused.
    #[test]
    fn input_end_waits_for_the_prompt_in_flight() {
        smol::block_on(async {
            let (mut srv, answer_rx, _, turns) = server_awaiting_answer();
            let session_id = srv.session.as_ref().unwrap().handle.session_id.to_string();
            handle_prompt(
                &mut srv,
                &prompt_request(&session_id, "first", false),
                &RequestId::Number(41),
            )
            .await
            .unwrap();
            turns.next().await;
            let session = srv.session.as_ref().unwrap();
            let mut finishing = Box::pin(finish_in_flight(session));
            assert!(smol::future::poll_once(&mut finishing).await.is_none());
            assert_eq!(
                answer_rx.try_recv().unwrap(),
                TaggedAnswer::new("toolu_1", PermissionAnswer::Deny).encode()
            );
            assert!(session.pending.lock().unwrap().refuses_asks());
            take_active_operation(&session.pending, OperationKind::PrimaryTurn).unwrap();
            assert!(
                smol::future::poll_once(&mut finishing).await.is_none(),
                "input end must wait for the root turn to settle"
            );
            turns.release();
            finishing.await;
        });
    }

    #[test]
    fn idle_and_duplicate_cancel_do_not_poison_the_next_prompt() {
        smol::block_on(async {
            let (mut srv, _, _, turns) = server_awaiting_answer();
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
            assert_eq!(turns.next_text().await, "first");
            let first = turns.latest();

            handle_notification(&srv, "session/cancel", &cancel);
            handle_notification(&srv, "session/cancel", &cancel);
            let pending = &srv.session.as_ref().unwrap().pending;
            assert_eq!(
                operation_state(pending),
                Some((true, false)),
                "active operation must be cancelled, and its cancel fired once"
            );
            turns.assert_cancelled(&first).await;
            take_active_operation(pending, OperationKind::PrimaryTurn).unwrap();

            handle_prompt(
                &mut srv,
                &prompt_request(&session_id, "second", false),
                &RequestId::Number(42),
            )
            .await
            .unwrap();
            assert_eq!(turns.next_text().await, "second");
            assert!(
                operation_state(&srv.session.as_ref().unwrap().pending)
                    .is_some_and(|(cancelling, _)| !cancelling),
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

    /// Closing a session forwards what it already emitted: a background
    /// child's last update is still in the channel when the session closes.
    #[test]
    fn finished_pump_forwards_queued_events() {
        const QUEUED_TEXT: &str = "late background update";
        let (event_tx, event_rx) = flume::unbounded::<Envelope>();
        let (out_tx, out_rx) = flume::unbounded::<Value>();
        let (answer_tx, _answer_rx) = flume::unbounded::<String>();
        let session_id = SessionRef::from(MakiId::generate());
        let coordinator = test_coordinator(session_id.id(), OFFLINE_SPEC);
        let pump = start_event_pump(
            event_rx,
            session_id,
            out_tx,
            Arc::default(),
            true,
            answer_tx,
            coordinator.read(),
            maki_storage::paths::home(),
            PUMP_TRUSTED,
            None,
            test_observer(),
        );
        event_tx
            .send(Envelope {
                event: AgentEvent::TextDelta {
                    text: QUEUED_TEXT.into(),
                },
                subagent: None,
                identity: None,
            })
            .unwrap();
        smol::block_on(pump.finish());
        let forwarded = out_rx
            .drain()
            .map(|message| message.to_string())
            .collect::<Vec<_>>();
        assert!(
            forwarded
                .iter()
                .any(|message| message.contains(QUEUED_TEXT)),
            "{forwarded:?}"
        );
        drop(event_tx);
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
                joined: Vec::new(),
            }),
            ..Default::default()
        }));
        let session_id = SessionRef::from(MakiId::generate());
        let coordinator = test_coordinator(session_id.id(), OFFLINE_SPEC);

        start_event_pump(
            event_rx,
            session_id,
            out_tx,
            pending,
            true,
            answer_tx,
            coordinator.read(),
            maki_storage::paths::home(),
            PUMP_TRUSTED,
            None,
            test_observer(),
        )
        .detach();
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
                identity: None,
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
        let coordinator = test_coordinator(session_id.id(), OFFLINE_SPEC);

        start_event_pump(
            event_rx,
            session_id,
            out_tx,
            Arc::clone(&pending),
            true,
            answer_tx,
            coordinator.read(),
            maki_storage::paths::home(),
            PUMP_TRUSTED,
            None,
            test_observer(),
        )
        .detach();

        event_tx
            .send(Envelope {
                event: AgentEvent::Question {
                    id: "t1".to_string(),
                    questions: serde_json::json!({ "not": "an array" }),
                },
                subagent: None,
                identity: None,
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
                    run_id: Some(OLD_RUN_ID),
                    cancelling: false,
                    cancel: None,
                    joined: Vec::new(),
                }),
                ..Default::default()
            }));
            let session_id = SessionRef::from(MakiId::generate());
            let coordinator = test_coordinator(session_id.id(), OFFLINE_SPEC);
            start_event_pump(
                event_rx,
                session_id,
                out_tx,
                Arc::clone(&pending),
                true,
                answer_tx,
                coordinator.read(),
                maki_storage::paths::home(),
                PUMP_TRUSTED,
                None,
                test_observer(),
            )
            .detach();

            let completed = |run_id| Envelope {
                event: AgentEvent::TurnOutcome(maki_agent::TurnOutcome::completed(
                    maki_agent::AgentId::generate(),
                    maki_agent::TurnId::generate(),
                    TokenUsage::default(),
                    1,
                    maki_agent::DoneReason::EndTurn,
                )),
                subagent: None,
                identity: Some(maki_agent::ActorWorkIdentity::new(
                    maki_agent::AgentId::generate(),
                    maki_agent::ActorWorkKind::Control {
                        operation_id: run_id,
                    },
                    run_id,
                )),
            };
            event_tx.send_async(completed(OLD_RUN_ID)).await.unwrap();
            assert_eq!(out_rx.recv_async().await.unwrap()["id"], 41);

            pending.lock().unwrap().operation = Some(PendingOperation {
                id: 2,
                request_id: RequestId::Number(42),
                kind: OperationKind::PrimaryTurn,
                run_id: Some(NEXT_RUN_ID),
                cancelling: false,
                cancel: None,
                joined: Vec::new(),
            });
            event_tx
                .send_async(Envelope {
                    event: AgentEvent::ControlError {
                        message: "failed to checkpoint completed turn".into(),
                    },
                    subagent: None,
                    identity: Some(maki_agent::ActorWorkIdentity::new(
                        maki_agent::AgentId::generate(),
                        maki_agent::ActorWorkKind::Control {
                            operation_id: OLD_RUN_ID,
                        },
                        OLD_RUN_ID,
                    )),
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

    /// A second prompt sent while a turn runs joins it, as typing does in
    /// the TUI, and both prompts resolve when the turn ends.
    #[test]
    fn prompt_sent_mid_turn_joins_it_and_both_resolve_with_it() {
        const FOLLOW_UP: &str = "follow-up";
        smol::block_on(async {
            let (mut srv, _, out_rx, turns) = server_awaiting_answer();
            let (event_tx, event_rx) = flume::unbounded::<Envelope>();
            let session = srv.session.as_ref().unwrap();
            let pending = Arc::clone(&session.pending);
            start_event_pump(
                event_rx,
                session.handle.session_id.clone(),
                srv.out_tx.clone(),
                Arc::clone(&pending),
                false,
                session.handle.answer_tx.clone(),
                session.coordinator.as_ref().unwrap().read(),
                None,
                PUMP_TRUSTED,
                None,
                test_observer(),
            )
            .detach();
            let session_id = session.handle.session_id.to_string();
            handle_prompt(
                &mut srv,
                &prompt_request(&session_id, "first", false),
                &RequestId::Number(41),
            )
            .await
            .unwrap();
            assert_eq!(turns.next_text().await, "first");
            let first = turns.latest();
            handle_prompt(
                &mut srv,
                &prompt_request(&session_id, FOLLOW_UP, false),
                &RequestId::Number(42),
            )
            .await
            .unwrap();
            assert_eq!(turns.latest().turn_id(), first.turn_id());
            turns.release();
            assert!(turns.next_text().await.contains(FOLLOW_UP));
            event_tx
                .send_async(primary_envelope(
                    &pending,
                    AgentEvent::TurnOutcome(maki_agent::TurnOutcome::completed(
                        maki_agent::AgentId::generate(),
                        first.turn_id(),
                        TokenUsage::default(),
                        1,
                        maki_agent::DoneReason::EndTurn,
                    )),
                ))
                .await
                .unwrap();
            for id in [41, 42] {
                let terminal = out_rx.recv_async().await.unwrap();
                assert_eq!(terminal["id"], id);
                assert_eq!(terminal["result"]["stopReason"], "end_turn");
            }
            assert!(pending.lock().unwrap().operation.is_none());
            drop(event_tx);
        });
    }

    /// A prompt still dispatching beside a running turn has no operation of
    /// its own, yet a cancel or a closing session answers it cancelled, and
    /// a cancelled one starts no turn once its dispatch ends.
    #[test_case(false ; "cancelled")]
    #[test_case(true ; "session_closed")]
    fn prompt_dispatching_beside_a_turn_is_answered_cancelled(close: bool) {
        const FOLLOW_UP: &str = "follow-up";
        smol::block_on(async {
            let (mut srv, _, out_rx, turns) = server_awaiting_answer();
            let session_id = srv.session.as_ref().unwrap().handle.session_id.to_string();
            handle_prompt(
                &mut srv,
                &prompt_request(&session_id, "first", false),
                &RequestId::Number(41),
            )
            .await
            .unwrap();
            assert_eq!(turns.next_text().await, "first");
            let first = turns.latest();
            start_prompt(
                &mut srv,
                &prompt_request(&session_id, FOLLOW_UP, false),
                &RequestId::Number(42),
            )
            .unwrap();
            if close {
                close_session(&mut srv).await;
            } else {
                handle_notification(
                    &srv,
                    "session/cancel",
                    &serde_json::json!({ "params": { "sessionId": session_id } }),
                );
                let dispatched =
                    (&mut srv.dispatch.as_mut().expect("prompt dispatch").future).await;
                finish_prompt(&mut srv, dispatched).await.unwrap();
                assert_eq!(turns.latest().turn_id(), first.turn_id());
            }
            let answered = out_rx
                .try_iter()
                .find(|message| message["id"] == 42)
                .expect("the dispatching prompt is answered");
            assert_eq!(answered["result"]["stopReason"], "cancelled", "{answered}");
        });
    }

    /// A prompt whose message could not join waits for the turn ahead, then
    /// resolves with its own turn; a cancel reaches both.
    #[test]
    fn queued_prompt_takes_over_when_the_turn_ahead_ends() {
        const RUNNING: u64 = 7;
        const QUEUED: u64 = 8;
        smol::block_on(async {
            let (event_tx, event_rx) = flume::unbounded::<Envelope>();
            let (out_tx, out_rx) = flume::unbounded::<Value>();
            let primary = |request: i64, run_id: u64| PendingOperation {
                id: run_id,
                request_id: RequestId::Number(request),
                kind: OperationKind::PrimaryTurn,
                run_id: Some(run_id),
                cancelling: false,
                cancel: None,
                joined: Vec::new(),
            };
            let pending = Arc::new(Mutex::new(Pending {
                operation: Some(PendingOperation {
                    joined: vec![RequestId::Number(43)],
                    ..primary(41, RUNNING)
                }),
                queued: VecDeque::from([primary(42, QUEUED)]),
                ..Default::default()
            }));
            let session_id = SessionRef::from(MakiId::generate());
            let coordinator = test_coordinator(session_id.id(), OFFLINE_SPEC);
            start_event_pump(
                event_rx,
                session_id,
                out_tx,
                Arc::clone(&pending),
                false,
                flume::unbounded().0,
                coordinator.read(),
                None,
                PUMP_TRUSTED,
                None,
                test_observer(),
            )
            .detach();
            let ended = |pending: &PendingState| {
                primary_envelope(
                    pending,
                    AgentEvent::TurnOutcome(maki_agent::TurnOutcome::cancelled(
                        maki_agent::AgentId::generate(),
                        maki_agent::TurnId::generate(),
                        TokenUsage::default(),
                        1,
                        maki_agent::TurnCancellationReason::User,
                    )),
                )
            };
            {
                let mut pending = pending.lock().unwrap();
                let pending = &mut *pending;
                for operation in pending.operation.iter_mut().chain(&mut pending.queued) {
                    operation.start_cancelling();
                }
            }
            event_tx.send_async(ended(&pending)).await.unwrap();
            for id in [41, 43] {
                assert_eq!(out_rx.recv_async().await.unwrap()["id"], id);
            }
            assert!(operation_state(&pending).is_some_and(|(cancelling, _)| cancelling));
            event_tx.send_async(ended(&pending)).await.unwrap();
            let queued = out_rx.recv_async().await.unwrap();
            assert_eq!(queued["id"], 42);
            assert_eq!(queued["result"]["stopReason"], "cancelled");
            assert!(pending.lock().unwrap().operation.is_none());
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
        let coordinator = test_coordinator(session_id.id(), OFFLINE_SPEC);
        let tool_id = "tool-1";

        start_event_pump(
            event_rx,
            session_id,
            out_tx,
            pending,
            true,
            answer_tx,
            coordinator.read(),
            maki_storage::paths::home(),
            PUMP_TRUSTED,
            None,
            test_observer(),
        )
        .detach();

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
                    identity: None,
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
        let coordinator = test_coordinator(session_id.id(), OFFLINE_SPEC);
        start_event_pump(
            event_rx,
            session_id,
            out_tx,
            PendingState::default(),
            false,
            answer_tx,
            coordinator.read(),
            None,
            PUMP_TRUSTED,
            Some(INITIAL_COST),
            test_observer(),
        )
        .detach();
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
                identity: None,
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
        let _plugins = MANAGED_PLUGIN_TEST.lock().unwrap();
        smol::block_on(async {
            let (mut srv, _, out_rx, _) = server_awaiting_answer();
            let (_host, received, root_answer_rx, _state, notices) =
                managed_test_session(&mut srv, true, true, None, successful_checkpoint()).await;
            let session_id = srv.session.as_ref().unwrap().handle.session_id.to_string();
            handle_prompt(
                &mut srv,
                &prompt_request(&session_id, "permission root", false),
                &RequestId::Number(93),
            )
            .await
            .unwrap();
            let request = smol::future::or(
                async {
                    loop {
                        let message = out_rx.recv_async().await.unwrap();
                        assert!(
                            message.get("result").is_none() && message.get("error").is_none(),
                            "early terminal: {message}"
                        );
                        if message["method"] == "session/request_permission" {
                            return message;
                        }
                    }
                },
                async {
                    smol::Timer::after(PRIMARY_TURN_TIMEOUT).await;
                    panic!("managed child did not request permission");
                },
            )
            .await;
            assert_eq!(
                request["params"]["toolCall"]["toolCallId"],
                "acp_child_permission-1"
            );
            let request_id = request["id"].as_i64().unwrap();
            assert!(
                !srv.session
                    .as_ref()
                    .unwrap()
                    .pending
                    .lock()
                    .unwrap()
                    .permissions
                    .is_empty()
            );
            handle_incoming_response(&srv, &allow_once(request_id));
            assert_eq!(
                prompt_terminal(&out_rx, 93).await["result"]["stopReason"],
                "end_turn"
            );
            assert_eq!(
                notices.recv_timeout(PRIMARY_TURN_TIMEOUT).unwrap(),
                format!("{ACP_TURN_END}{session_id}")
            );
            assert!(root_answer_rx.is_empty());
            assert!(
                received.try_iter().count() >= 5,
                "child did not continue after the answer"
            );
            assert!(
                srv.session
                    .as_ref()
                    .unwrap()
                    .pending
                    .lock()
                    .unwrap()
                    .permissions
                    .is_empty()
            );
            assert!(
                srv.session
                    .as_ref()
                    .unwrap()
                    .pending
                    .lock()
                    .unwrap()
                    .operation
                    .is_none()
            );
            close_session(&mut srv).await;
        });
    }

    #[test]
    fn event_pump_emits_thinking_separator() {
        let (event_tx, event_rx) = flume::unbounded::<Envelope>();
        let (out_tx, out_rx) = flume::unbounded::<Value>();
        let (answer_tx, _answer_rx) = flume::unbounded::<String>();
        let pending = Arc::new(Mutex::new(Pending::default()));
        let session_id = SessionRef::from(MakiId::generate());
        let coordinator = test_coordinator(session_id.id(), OFFLINE_SPEC);

        start_event_pump(
            event_rx,
            session_id,
            out_tx,
            Arc::clone(&pending),
            true,
            answer_tx,
            coordinator.read(),
            maki_storage::paths::home(),
            PUMP_TRUSTED,
            None,
            test_observer(),
        )
        .detach();

        event_tx
            .send(Envelope {
                event: AgentEvent::ThinkingDelta {
                    text: "first".to_string(),
                },
                subagent: None,
                identity: None,
            })
            .unwrap();
        event_tx
            .send(Envelope {
                event: AgentEvent::ThinkingBlockEnd,
                subagent: None,
                identity: None,
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
        let (srv, ..) = server_awaiting_answer();
        let session = srv.session.as_ref().unwrap();
        let coordinator = session.coordinator.clone().unwrap();
        let registry = test_registry(&[]);
        let target = test_target(
            &registry,
            session.handle.control_tx.clone(),
            coordinator.clone(),
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
        smol::block_on(coordinator.close()).unwrap();
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
                "model={:?},cwd={:?},options_version={}",
                read.model(),
                read.cwd(),
                options.version
            )
        });
        format!(
            "session={},operation={operation:?},permissions={permissions:?},elicitation={elicitation:?},coordinator={coordinator:?}",
            session.handle.session_id
        )
    }

    /// {event} stamped with the running primary turn's presentation, the
    /// way the actor stamps that turn's own events.
    fn primary_envelope(pending: &PendingState, event: AgentEvent) -> Envelope {
        let run_id = pending
            .lock()
            .unwrap()
            .operation
            .as_ref()
            .and_then(|operation| operation.run_id)
            .unwrap();
        Envelope {
            event,
            subagent: None,
            identity: Some(maki_agent::ActorWorkIdentity::new(
                maki_agent::AgentId::generate(),
                maki_agent::ActorWorkKind::Control {
                    operation_id: run_id,
                },
                run_id,
            )),
        }
    }

    fn subagent_activity(event: AgentEvent) -> Envelope {
        subagent_activity_with_optional_answer(event, None)
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
                parent_tool_use_id: "task-reusable".to_owned(),
                name: "task".to_owned(),
                prompt: Some("reuse actor".to_owned()),
                model: Some(OFFLINE_SPEC.to_owned()),
                opts: None,
                answer_tx,
                cancel: None,
            }),
            identity: None,
        }
    }

    fn prompt_request(session_id: &str, text: &str, image: bool) -> Value {
        let mut prompt = vec![serde_json::json!({ "type": "text", "text": text })];
        if image {
            prompt.push(serde_json::json!({
                "type": "image",
                "data": TINY_PNG,
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
            session.coordinator.clone().unwrap(),
        );
        session.command_registry = registry;
    }

    #[test]
    fn compact_tool_progress_success_sequence() {
        smol::block_on(async {
            let (mut srv, _, out_rx, turns) = server_awaiting_answer();
            install_registry(&mut srv, test_registry(&[]));

            dispatch_prompt(&mut srv, "/compact", false, &RequestId::Number(30))
                .await
                .unwrap();
            let pending = out_rx.recv_async().await.unwrap();
            assert_eq!(pending["params"]["update"]["sessionUpdate"], "tool_call");
            assert_eq!(pending["params"]["update"]["title"], "Compact context");
            turns.next().await;
            turns.release();

            let started = out_rx.recv_async().await.unwrap();
            let completed = out_rx.recv_async().await.unwrap();
            let terminal = out_rx.recv_async().await.unwrap();
            assert_eq!(started["params"]["update"]["status"], "in_progress");
            assert_eq!(completed["params"]["update"]["status"], "completed");
            assert_eq!(terminal["id"], 30);
            assert_eq!(terminal["result"]["stopReason"], "end_turn");
            assert!(out_rx.is_empty(), "compaction prompt completes once");
            assert!(
                turns.actor.latest_ticket().is_none(),
                "compaction bypasses primary input"
            );
        });
    }

    #[test]
    fn compact_prompt_cancellation_completes_once() {
        smol::block_on(async {
            let (mut srv, _, out_rx, turns) = server_awaiting_answer();
            install_registry(&mut srv, test_registry(&[]));
            let session_id = srv.session.as_ref().unwrap().handle.session_id.to_string();

            dispatch_prompt(&mut srv, "/compact", false, &RequestId::Number(33))
                .await
                .unwrap();
            let _pending = out_rx.recv_async().await.unwrap();
            let _started = out_rx.recv_async().await.unwrap();
            turns.next().await;
            handle_notification(
                &srv,
                "session/cancel",
                &serde_json::json!({ "params": { "sessionId": session_id } }),
            );

            let failed = out_rx.recv_async().await.unwrap();
            let terminal = out_rx.recv_async().await.unwrap();
            assert_eq!(failed["params"]["update"]["status"], "failed");
            assert_eq!(terminal["id"], 33);
            assert_eq!(terminal["result"]["stopReason"], "cancelled");
            turns.release();
            within(turns.actor.runtime_barrier(), "the actor never settled")
                .await
                .unwrap();
            assert!(turns.requests.is_empty(), "cancelled compaction retried");
            assert!(out_rx.is_empty(), "cancelled compaction terminated twice");
        });
    }

    #[test]
    fn compact_tool_progress_failure_sequence() {
        smol::block_on(async {
            let checkpoint: Arc<
                dyn maki_storage::checkpoint::CheckpointWriter<
                        maki_agent::session_coordinator::SessionCheckpoint,
                    >,
            > = Arc::new(|request: maki_storage::checkpoint::CheckpointRequest<_>| {
                Box::pin(async move {
                    Err(maki_storage::checkpoint::CheckpointError::Save {
                        session_id: request.session_id,
                        message: Arc::from(SAVE_FAILED),
                    })
                }) as maki_storage::checkpoint::CheckpointFuture
            });
            let (mut srv, _, out_rx, turns) = server_awaiting_answer_with_checkpoint(checkpoint);
            install_registry(&mut srv, test_registry(&[]));

            dispatch_prompt(&mut srv, "/compact", false, &RequestId::Number(32))
                .await
                .unwrap();
            let _pending = out_rx.recv_async().await.unwrap();
            let _started = out_rx.recv_async().await.unwrap();
            turns.release();

            let failed = out_rx.recv_async().await.unwrap();
            let terminal = out_rx.recv_async().await.unwrap();
            assert_eq!(failed["params"]["update"]["status"], "failed");
            assert_eq!(terminal["id"], 32);
            assert!(
                terminal["error"]["data"]
                    .as_str()
                    .is_some_and(|error| error.contains(SAVE_FAILED)),
                "{terminal}"
            );
            assert!(out_rx.is_empty(), "failed compaction completes once");
        });
    }

    #[test]
    fn btw_streams_and_completes_active_prompt() {
        smol::block_on(async {
            let (mut srv, _, out_rx, turns) = server_awaiting_answer();
            install_registry(&mut srv, test_registry(&[]));

            dispatch_prompt(&mut srv, "/btw why?", true, &RequestId::Number(31))
                .await
                .unwrap();
            let asked = turns.next().await;
            assert!(text_of(&asked).ends_with("\n\nwhy?"), "{asked:?}");
            assert_eq!(images_of(&asked).len(), 1);
            turns.release();

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
            assert_eq!(answer["params"]["update"]["content"]["text"], PARKED_REPLY);
            assert_eq!(terminal["id"], 31);
            assert_eq!(terminal["result"]["stopReason"], "end_turn");
            assert!(out_rx.is_empty(), "operation terminates exactly once");
            assert!(
                turns.actor.latest_ticket().is_none(),
                "isolated turns bypass primary input"
            );
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
            let (handle, _root_target) = spawn_session(
                &AcpParams {
                    model: model.clone(),
                    config: Default::default(),
                    permissions_config: Default::default(),
                    timeouts: Default::default(),
                    initial_wd: PathBuf::from("/project"),
                    storage: StateDir::from_path(PathBuf::from("/tmp/maki-acp-test")),
                    prompt_slots: Default::default(),
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
                    permissions: PermissionsConfig::default(),
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
            let actor = handle.initialized_root().await.unwrap().actor().unwrap();
            actor
                .reserve_prepared_operation(None)
                .unwrap()
                .commit_when_ready(Some(maki_agent::actor::ConfigChange::Patch(
                    maki_agent::actor::ConfigPatch {
                        model: Some(maki_agent::actor::PreparedModel {
                            provider: Arc::new(EndTurnProvider),
                            model: Model::from_spec(OFFLINE_SPEC).unwrap(),
                        }),
                        ..Default::default()
                    },
                )))
                .await
                .unwrap();
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
                session.coordinator.as_ref().unwrap().read(),
                None,
                PUMP_TRUSTED,
                None,
                test_observer(),
            )
            .detach();
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

    #[test_case(false, false; "protocol_option")]
    #[test_case(true, false; "lua_root_option")]
    #[test_case(false, true; "nested_task")]
    fn acp_immediate_prompt_and_option_change(lua_root_option: bool, nested: bool) {
        let _plugins = MANAGED_PLUGIN_TEST.lock().unwrap();
        smol::block_on(async {
            let (mut srv, _, out_rx, _) = server_awaiting_answer();
            let (started_tx, started_rx) = flume::bounded(1);
            let (release_tx, release_rx) = flume::bounded(1);
            let checkpoint = Arc::new(
                move |request: maki_storage::checkpoint::CheckpointRequest<
                    maki_agent::session_coordinator::SessionCheckpoint,
                >| {
                    let started = started_tx.clone();
                    let release = release_rx.clone();
                    Box::pin(async move {
                        if request.snapshot.history.is_some() {
                            started.send_async(()).await.unwrap();
                            release.recv_async().await.unwrap();
                        }
                        Ok(maki_storage::checkpoint::CheckpointAck::for_request(
                            &request,
                        ))
                    }) as maki_storage::checkpoint::CheckpointFuture
                },
            );
            let (child_started_tx, child_started_rx) = flume::bounded(1);
            let (child_release_tx, child_release_rx) = flume::bounded(1);
            let completion: TurnCompletion = Arc::new(move |target, history| {
                let child = target.snapshot().unwrap().depth == 1;
                let started = child_started_tx.clone();
                let release = child_release_rx.clone();
                Box::pin(async move {
                    if child {
                        assert!(history.messages.iter().flat_map(|message| &message.content)
                            .any(|block| matches!(block, MsgBlock::Text { text } if text == "child result")));
                        started.send_async(()).await.unwrap();
                        release.recv_async().await.unwrap();
                    }
                    Ok(())
                })
            });
            let (_host, received, _, _state, _) =
                managed_test_session(&mut srv, nested, false, Some(completion), checkpoint).await;
            let session = srv.session.as_ref().unwrap();
            let session_id = session.handle.session_id.to_string();
            let manager = session.handle.manager.clone();
            handle_prompt(
                &mut srv,
                &prompt_request(&session_id, "immediate", false),
                &RequestId::Number(91),
            )
            .await
            .unwrap();
            if nested {
                smol::future::or(
                    async {
                        child_started_rx.recv_async().await.unwrap();
                    },
                    async {
                        smol::Timer::after(PRIMARY_TURN_TIMEOUT).await;
                        panic!("ACP child completion policy did not start");
                    },
                )
                .await;
                assert_eq!(
                    received.len(),
                    6,
                    "root observed child result before completion policy"
                );
                assert!(
                    started_rx.is_empty(),
                    "task_wait observed the child before completion policy"
                );
                assert!(out_rx.try_iter().all(
                    |message| message.get("result").is_none() && message.get("error").is_none()
                ));
                child_release_tx.send_async(()).await.unwrap();
            }
            smol::future::or(
                async {
                    started_rx.recv_async().await.unwrap();
                },
                async {
                    smol::Timer::after(PRIMARY_TURN_TIMEOUT).await;
                    panic!("ACP terminal checkpoint did not start");
                },
            )
            .await;
            let before = out_rx.try_iter().collect::<Vec<_>>();
            assert!(
                before.iter().all(
                    |message| message.get("result").is_none() && message.get("error").is_none()
                ),
                "terminal before persistence: {before:?}"
            );
            assert!(
                srv.session
                    .as_ref()
                    .unwrap()
                    .pending
                    .lock()
                    .unwrap()
                    .operation
                    .is_some()
            );
            if nested {
                let graph = manager.snapshot();
                assert_eq!(manager.limits().max_concurrent_agent_turns, 1);
                assert_eq!(graph.len(), 3, "{graph:?}");
                let root = graph.iter().find(|node| node.parent_id.is_none()).unwrap();
                let child = graph
                    .iter()
                    .find(|node| node.parent_id == Some(root.agent_id))
                    .unwrap();
                assert!(
                    graph
                        .iter()
                        .any(|node| node.parent_id == Some(child.agent_id))
                );
            }
            release_tx.send_async(()).await.unwrap();
            assert_eq!(
                prompt_terminal(&out_rx, 91).await["result"]["stopReason"],
                "end_turn"
            );
            assert!(
                srv.session
                    .as_ref()
                    .unwrap()
                    .pending
                    .lock()
                    .unwrap()
                    .operation
                    .is_none()
            );
            let recorded = received.try_iter().collect::<Vec<_>>();
            assert!(!recorded.is_empty());
            assert!(
                recorded
                    .iter()
                    .all(|(_, spec, fast)| spec == FAST_SPEC && !fast)
            );
            let option_version = srv
                .session
                .as_ref()
                .unwrap()
                .coordinator
                .as_ref()
                .unwrap()
                .read()
                .options()
                .version;
            if lua_root_option {
                handle_prompt(
                    &mut srv,
                    &prompt_request(&session_id, "/acp_fast", false),
                    &RequestId::Number(93),
                )
                .await
                .unwrap();
                assert_eq!(
                    prompt_terminal(&out_rx, 93).await["result"]["stopReason"],
                    "end_turn"
                );
            } else {
                handle_set_config(&mut srv, &serde_json::json!({ "params": {
                    "sessionId": session_id, "configId": maki_agent::session_options::FAST_OPTION_ID,
                    "value": maki_agent::session_options::ENABLED_VALUE,
                }})).await.unwrap();
            }
            let session = srv.session.as_ref().unwrap();
            assert!(
                session
                    .coordinator
                    .as_ref()
                    .unwrap()
                    .read()
                    .options()
                    .version
                    > option_version
            );
            assert!(
                session
                    .handle
                    .initialized_root()
                    .await
                    .unwrap()
                    .effective_config()
                    .unwrap()
                    .unwrap()
                    .fast
            );
            handle_prompt(
                &mut srv,
                &prompt_request(&session_id, "after option", false),
                &RequestId::Number(92),
            )
            .await
            .unwrap();
            started_rx.recv_async().await.unwrap();
            release_tx.send_async(()).await.unwrap();
            assert_eq!(
                prompt_terminal(&out_rx, 92).await["result"]["stopReason"],
                "end_turn"
            );
            assert!(
                received
                    .try_iter()
                    .all(|(_, spec, fast)| spec == FAST_SPEC && fast)
            );
            assert!(
                out_rx.try_iter().all(
                    |message| message.get("result").is_none() && message.get("error").is_none()
                )
            );
            close_session(&mut srv).await;
            assert!(manager.snapshot().iter().all(|node| matches!(
                node.graph_lifecycle,
                maki_agent::manager::GraphLifecycle::Closed
                    | maki_agent::manager::GraphLifecycle::Removed
            )));
        });
    }

    #[test]
    fn prompt_immediately_after_new_session_is_queued() {
        smol::block_on(async {
            let previous_host = std::env::var_os("OLLAMA_HOST");
            unsafe { std::env::set_var("OLLAMA_HOST", "http://127.0.0.1:1") };
            let temp = TempDir::new().unwrap();
            let cwd = temp.path().to_path_buf();
            let (mut srv, _, out_rx, _) = server_awaiting_answer();
            let model = Model::from_spec(SPAWN_TEST_SPEC).unwrap();
            let params = test_params(model, cwd.clone());
            let request = serde_json::json!({
                "params": { "cwd": cwd, "mcpServers": [] }
            });

            handle_request(
                &mut srv,
                "session/new",
                RequestId::Number(82),
                &request,
                &params,
            )
            .await;
            let new_response = out_rx.recv_async().await.unwrap();
            assert_eq!(new_response["id"], 82);
            let session = srv.session.as_ref().unwrap();
            let session_id = session.handle.session_id.to_string();
            let next_cwd = TempDir::new().unwrap();
            let old_scope = temp
                .path()
                .join("before.txt")
                .to_string_lossy()
                .into_owned();
            let next_scope = next_cwd
                .path()
                .join("after.txt")
                .to_string_lossy()
                .into_owned();
            let write = maki_config::ToolKey::Native(Arc::from("write"));
            assert!(matches!(
                session.handle.permissions.check(&write, &old_scope, None),
                maki_agent::permissions::PermissionCheck::Allowed
            ));
            session
                .coordinator
                .as_ref()
                .unwrap()
                .change_directory(next_cwd.path().to_path_buf())
                .await
                .unwrap();
            assert!(matches!(
                session.handle.permissions.check(&write, &old_scope, None),
                maki_agent::permissions::PermissionCheck::NeedsPrompt { .. }
            ));
            assert!(matches!(
                session.handle.permissions.check(&write, &next_scope, None),
                maki_agent::permissions::PermissionCheck::Allowed
            ));
            handle_request(
                &mut srv,
                "session/prompt",
                RequestId::Number(83),
                &prompt_request(&session_id, "immediate prompt", false),
                &params,
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
            assert!(pending, "immediate prompt must remain queued");
            assert!(
                out_rx.try_iter().all(|message| message["id"] != 83),
                "queued prompt must not return a terminal response"
            );
            close_session(&mut srv).await;
            match previous_host {
                Some(host) => unsafe { std::env::set_var("OLLAMA_HOST", host) },
                None => unsafe { std::env::remove_var("OLLAMA_HOST") },
            }
        });
    }

    #[test]
    fn discovery_reaches_the_root_its_catalog_and_the_client() {
        smol::block_on(async {
            let previous_host = std::env::var_os("OLLAMA_HOST");
            unsafe { std::env::set_var("OLLAMA_HOST", "http://127.0.0.1:1") };
            let temp = TempDir::new().unwrap();
            let cwd = temp.path().to_path_buf();
            let (mut srv, _, out_rx, _) = server_awaiting_answer();
            let model =
                Model::from_spec(&format!("{DISCOVERY_PROVIDER}/{DISCOVERY_MODEL_ID}")).unwrap();
            assert_ne!(model.context_window, DISCOVERED_WINDOW);
            let params = test_params(model, cwd.clone());
            handle_request(
                &mut srv,
                "session/new",
                RequestId::Number(NEW_SESSION_REQUEST),
                &serde_json::json!({ "params": { "cwd": cwd, "mcpServers": [] } }),
                &params,
            )
            .await;
            assert_eq!(
                out_rx.recv_async().await.unwrap()["id"],
                NEW_SESSION_REQUEST
            );
            let session = srv.session.as_ref().unwrap();
            let root = session.handle.manager.root().unwrap();
            let orchestration = Arc::clone(session.orchestration.as_ref().unwrap());
            let read = session.coordinator.as_ref().unwrap().read();
            let actor = root.actor().unwrap();
            let commits = actor.subscribe_config_commits();
            maki_providers::model_registry::set_known_models(
                DISCOVERY_PROVIDER,
                vec![maki_providers::ModelInfo {
                    context_window: Some(DISCOVERED_WINDOW),
                    ..maki_providers::ModelInfo::id_only(DISCOVERY_MODEL_ID.into())
                }],
            );

            srv.catalog.apply(vec![DISCOVERED_SPEC.to_owned()]).await;

            let commit = commits.recv_async().await.unwrap();
            assert_eq!(commit.config.model.context_window, DISCOVERED_WINDOW);
            assert_eq!(
                actor.effective_config().unwrap().model.context_window,
                DISCOVERED_WINDOW
            );
            assert!(
                OrchestrationServices::available_models(&*orchestration, &root)
                    .unwrap()
                    .iter()
                    .any(|spec| spec == DISCOVERED_SPEC)
            );
            let options = loop {
                let update = out_rx.recv_async().await.unwrap();
                if let Some(options) = update["params"]["update"]["configOptions"].as_array() {
                    break options.clone();
                }
            };
            assert!(
                options[0]["options"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|option| option["value"] == DISCOVERED_SPEC)
            );
            let version = read.options().version;
            srv.catalog.apply(vec![DISCOVERED_SPEC.to_owned()]).await;
            assert_eq!(read.options().version, version);
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
            let (mut srv, _, out_rx, turns) = server_awaiting_answer();
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
                old.coordinator.as_ref().unwrap().read(),
                None,
                PUMP_TRUSTED,
                None,
                test_observer(),
            )
            .detach();
            let old_id = old.handle.session_id.to_string();

            handle_prompt(
                &mut srv,
                &prompt_request(&old_id, "old primary", false),
                &RequestId::Number(81),
            )
            .await
            .unwrap();
            turns.next().await;
            event_tx
                .send_async(subagent_activity(AgentEvent::ControlComplete {
                    usage: TokenUsage::default(),
                }))
                .await
                .unwrap();
            event_tx
                .send_async(primary_envelope(
                    &srv.session.as_ref().unwrap().pending,
                    AgentEvent::ControlComplete {
                        usage: TokenUsage::default(),
                    },
                ))
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
            let actor = srv
                .session
                .as_ref()
                .unwrap()
                .handle
                .initialized_root()
                .await
                .unwrap()
                .actor()
                .unwrap();
            actor
                .reserve_prepared_operation(None)
                .unwrap()
                .commit_when_ready(Some(maki_agent::actor::ConfigChange::Patch(
                    maki_agent::actor::ConfigPatch {
                        model: Some(maki_agent::actor::PreparedModel {
                            provider: Arc::new(EndTurnProvider),
                            model: Model::from_spec(OFFLINE_SPEC).unwrap(),
                        }),
                        ..Default::default()
                    },
                )))
                .await
                .unwrap();

            let prompt_result = handle_prompt(
                &mut srv,
                &prompt_request(&new_id, "new primary", false),
                &RequestId::Number(82),
            )
            .await;
            assert!(
                prompt_result.is_ok(),
                "prompt after provider initialization must be admitted; {}",
                lifecycle_state(&srv)
            );

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
        let (mut srv, _, _, turns) = server_awaiting_answer();
        let error = smol::block_on(dispatch_prompt(
            &mut srv,
            "/does-not-exist value",
            false,
            &RequestId::Number(1),
        ))
        .unwrap_err();
        assert_eq!(error.code, AcpError::invalid_params().code);
        assert_eq!(error.message, "unknown command /does-not-exist");
        turns.assert_untouched();
    }

    #[test]
    fn escaped_slash_prompt_is_sent_literal() {
        let (mut srv, _, _, turns) = server_awaiting_answer();

        smol::block_on(dispatch_prompt(
            &mut srv,
            "//does-not-exist value",
            true,
            &RequestId::Number(2),
        ))
        .unwrap();
        let asked = smol::block_on(turns.next());
        assert_eq!(text_of(&asked), "/does-not-exist value");
        let images = images_of(&asked);
        assert_eq!(images.len(), 1);
        assert_eq!(images[0].media_type, maki_providers::ImageMediaType::Png);
        assert_eq!(images[0].data.as_ref(), TINY_PNG);
    }

    #[test]
    fn new_and_clear_are_hidden_but_return_local_guidance() {
        smol::block_on(async {
            let (mut srv, _, out_rx, turns) = server_awaiting_answer();
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
                turns.assert_untouched();
            }
        });
    }

    #[test]
    fn custom_slash_prompt_dispatches_rendered_agent_input() {
        let (mut srv, _, _, turns) = server_awaiting_answer();
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
        assert_eq!(smol::block_on(turns.next_text()), "Review src");
    }

    #[test]
    fn unavailable_interactive_command_is_rejected() {
        let (mut srv, _, _, turns) = server_awaiting_answer();

        let error = smol::block_on(dispatch_prompt(
            &mut srv,
            "/help",
            true,
            &RequestId::Number(3),
        ))
        .unwrap_err();
        assert_eq!(error.code, AcpError::invalid_params().code);
        assert_eq!(error.message, "unknown command /help");
        turns.assert_untouched();
    }

    #[test]
    fn consecutive_model_errors_follow_each_prompt() {
        let (mut srv, _, _, turns) = server_awaiting_answer();

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
        turns.assert_untouched();
    }

    #[test]
    fn portable_model_options_commit_atomically_without_an_agent_turn() {
        smol::block_on(async {
            let (mut srv, _, _, turns) = server_awaiting_answer();
            let coordinator = srv
                .session
                .as_ref()
                .unwrap()
                .coordinator
                .as_ref()
                .unwrap()
                .clone();
            let value = |snapshot: &maki_agent::session_options::SessionOptionsSnapshot,
                         id: &str| {
                snapshot
                    .options
                    .iter()
                    .find(|option| option.definition.id.as_ref() == id)
                    .unwrap()
                    .current_value
                    .to_string()
            };
            let before = coordinator.read().options();
            dispatch_prompt(
                &mut srv,
                &format!("/model \"{FAST_SPEC}\" thinking=high fast=true"),
                false,
                &RequestId::Number(3),
            )
            .await
            .unwrap();
            let selected = coordinator.read().options();
            assert_eq!(selected.version, before.version + 1);
            assert_eq!(
                value(&selected, maki_agent::session_options::MODEL_OPTION_ID),
                FAST_SPEC
            );
            assert_eq!(
                value(&selected, maki_agent::session_options::FAST_OPTION_ID),
                maki_agent::session_options::ENABLED_VALUE
            );
            assert_eq!(
                value(&selected, maki_agent::session_options::THINKING_OPTION_ID),
                "high"
            );
            dispatch_prompt(
                &mut srv,
                "/model thinking=off fast=false",
                false,
                &RequestId::Number(4),
            )
            .await
            .unwrap();
            let disabled = coordinator.read().options();
            assert_eq!(disabled.version, selected.version + 1);
            assert_eq!(
                value(&disabled, maki_agent::session_options::MODEL_OPTION_ID),
                FAST_SPEC
            );
            assert_eq!(
                value(&disabled, maki_agent::session_options::FAST_OPTION_ID),
                maki_agent::session_options::DISABLED_VALUE
            );
            assert_eq!(
                value(&disabled, maki_agent::session_options::THINKING_OPTION_ID),
                "off"
            );
            // Repeating explicit values does not toggle either option.
            dispatch_prompt(
                &mut srv,
                "/model thinking=off fast=false",
                false,
                &RequestId::Number(5),
            )
            .await
            .unwrap();
            let repeated = coordinator.read().options();
            assert_eq!(repeated, disabled);
            assert_eq!(
                value(&repeated, maki_agent::session_options::FAST_OPTION_ID),
                maki_agent::session_options::DISABLED_VALUE
            );
            assert_eq!(
                value(&repeated, maki_agent::session_options::THINKING_OPTION_ID),
                "off"
            );
            turns.assert_untouched();
            coordinator.close().await.unwrap();
        });
    }

    #[test_case(&format!("/model {OFFLINE_SPEC} fast=true") ; "fast_unsupported_by_model")]
    #[test_case("/model fast=true fast=false" ; "duplicate_option")]
    #[test_case("/model thinking=toggle" ; "thinking_toggle")]
    fn portable_invalid_model_options_leave_session_options_unchanged(input: &str) {
        smol::block_on(async {
            let (mut srv, _, _, turns) = server_awaiting_answer();
            let coordinator = srv
                .session
                .as_ref()
                .unwrap()
                .coordinator
                .as_ref()
                .unwrap()
                .clone();
            let before = coordinator.read().options();
            dispatch_prompt(&mut srv, input, false, &RequestId::Number(3))
                .await
                .unwrap_err();
            assert_eq!(coordinator.read().options(), before);
            turns.assert_untouched();
            coordinator.close().await.unwrap();
        });
    }

    #[test]
    fn portable_bare_model_returns_shared_usage_error() {
        let (mut srv, _, _, turns) = server_awaiting_answer();

        let error = smol::block_on(dispatch_prompt(
            &mut srv,
            "/model",
            false,
            &RequestId::Number(3),
        ))
        .unwrap_err();

        assert_eq!(error.code, AcpError::invalid_params().code);
        assert_eq!(
            error.message,
            format!(
                "command failed: {}",
                maki_agent::command::NONINTERACTIVE_MODEL_USAGE
            )
        );
        turns.assert_untouched();
    }

    #[test]
    fn portable_local_builtin_rejects_non_text_content() {
        let (mut srv, _, _, turns) = server_awaiting_answer();

        let error = smol::block_on(dispatch_prompt(
            &mut srv,
            "/compact",
            true,
            &RequestId::Number(3),
        ))
        .unwrap_err();

        assert_eq!(error.code, AcpError::invalid_params().code);
        turns.assert_untouched();
    }

    #[test]
    fn agent_turn_preserves_image_content() {
        let (mut srv, _, _, turns) = server_awaiting_answer();
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
        let asked = smol::block_on(turns.next());
        assert_eq!(text_of(&asked), "Review src");
        assert_eq!(images_of(&asked).len(), 1);
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
        let (mut srv, _, out_rx, turns) = server_awaiting_answer();
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
        turns.assert_untouched();
        let response = out_rx.try_recv().unwrap();
        assert_eq!(response["result"]["stopReason"], "end_turn");
    }

    #[test]
    fn portable_bare_btw_is_rejected_by_registry() {
        let (mut srv, _, _, turns) = server_awaiting_answer();
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
        turns.assert_untouched();
    }

    #[test]
    fn restored_cwd_overrides_client_cwd() {
        let restored = PathBuf::from("/restored/project");
        let client = PathBuf::from("/client/project");

        assert_eq!(effective_session_cwd(Some(&restored), &client), restored);
        assert_eq!(effective_session_cwd(None, &client), client);
    }

    /// A loaded session comes back in the mode and thinking it was saved
    /// with, even when `always_thinking` says otherwise.
    #[test]
    fn load_restores_saved_mode_and_thinking_over_config() {
        // SAFETY: nextest runs each test in its own process.
        unsafe { std::env::set_var("OLLAMA_HOST", UNREACHABLE_OLLAMA_HOST) };
        smol::block_on(async {
            let tmp = TempDir::new().unwrap();
            maki_storage::paths::init_at(tmp.path().to_path_buf()).unwrap();
            let plan = tmp.path().join(SAVED_PLAN_FILE);
            std::fs::write(&plan, "").unwrap();
            maki_providers::model_registry::set_known_models(
                DISCOVERY_PROVIDER,
                vec![maki_providers::ModelInfo {
                    supports_thinking: Some(true),
                    ..maki_providers::ModelInfo::id_only(THINKING_MODEL_ID.into())
                }],
            );
            let spec = format!("{DISCOVERY_PROVIDER}/{THINKING_MODEL_ID}");
            let mut session: Session<Message, TokenUsage, maki_agent::ToolOutput> =
                Session::new(&spec, &tmp.path().to_string_lossy());
            session.replace_messages(vec![Message::user("planned".into())]);
            session.meta.mode = Some(StoredMode::Plan);
            session.meta.plan_path = Some(plan.to_string_lossy().into_owned());
            session.meta.thinking = Some(StoredThinking::Off);
            let storage = StateDir::from_path(tmp.path().to_path_buf());
            session.save(&storage).unwrap();
            let model = Model::from_spec(&spec).unwrap();
            assert!(model.supports_thinking());
            let mut params = test_params(model, tmp.path().to_path_buf());
            params.defaults.thinking = Some(StoredThinking::Effort {
                level: Effort::High,
            });
            let (out_tx, _out_rx) = flume::unbounded();
            let mut srv = Server {
                out_tx,
                catalog: ModelCatalog::default(),
                session: None,
                elicitation: false,
                supports_boolean: false,
                lua_event_handle: params.lua_event_handle.clone(),
                dispatch: None,
            };
            let raw = serde_json::json!({
                "params": {
                    "sessionId": session.id.to_string(),
                    "cwd": tmp.path(),
                    "mcpServers": []
                }
            });

            load_session(&mut srv, &raw, &params).await.unwrap();

            let root = srv.session.as_ref().unwrap().handle.manager.root().unwrap();
            let config = root.actor().unwrap().effective_config().unwrap();
            assert_eq!(config.mode, AgentMode::Plan(plan));
            assert_eq!(config.thinking, maki_agent::ThinkingConfig::Off);
            close_session(&mut srv).await;
        });
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
            let coordinator = test_coordinator(session.id, OFFLINE_SPEC);
            let params = test_params(
                Model::from_spec(OFFLINE_SPEC).unwrap(),
                tmp.path().to_path_buf(),
            );
            let (out_tx, out_rx) = flume::unbounded();
            let mut srv = Server {
                out_tx,
                catalog: ModelCatalog::default(),
                session: None,
                elicitation: false,
                supports_boolean: false,
                lua_event_handle: params.lua_event_handle.clone(),
                dispatch: None,
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
            serde_json::to_value(history.history.messages.as_ref()).unwrap(),
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
        let err = load_history_from(&dir, MakiId::generate()).err().unwrap();
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
        let err = load_history_from(&dir, id).err().unwrap();
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
            let (mut srv, _, out_rx, turns) = server_awaiting_answer();
            let (event_tx, event_rx) = flume::unbounded::<Envelope>();
            let (session_id, pending) = {
                let session = srv.session.as_mut().unwrap();
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
                test_observer(),
            )
            .detach();
            let session_id_str = session_id.to_string();
            let request_id = RequestId::Number(41);

            handle_prompt(
                &mut srv,
                &prompt_request(&session_id_str, "first", false),
                &request_id,
            )
            .await
            .unwrap();
            turns.next().await;
            let first = turns.latest();

            event_tx
                .send_async(Envelope {
                    event: AgentEvent::AuthRequired,
                    subagent: None,
                    identity: None,
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
            turns.assert_cancelled(&first).await;
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
            let (mut srv, _, out_rx, turns) = server_awaiting_answer();
            let (event_tx, event_rx) = flume::unbounded::<Envelope>();
            let session = srv.session.as_ref().unwrap();
            start_event_pump(
                event_rx,
                session.handle.session_id.clone(),
                srv.out_tx.clone(),
                Arc::clone(&session.pending),
                false,
                session.handle.answer_tx.clone(),
                session.coordinator.as_ref().unwrap().read(),
                None,
                PUMP_TRUSTED,
                None,
                test_observer(),
            )
            .detach();
            let session_id = session.handle.session_id.to_string();

            handle_prompt(
                &mut srv,
                &prompt_request(&session_id, "first", false),
                &RequestId::Number(41),
            )
            .await
            .unwrap();
            turns.next().await;

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
                    identity: None,
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
                    identity: None,
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
