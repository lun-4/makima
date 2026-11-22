use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
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
use makima_agent::config_change::ConfigChangeError;
use makima_agent::headless::{self, InteractiveHandle, InteractiveParams, WorkCancel};
use makima_agent::journal::SessionJournal;
use makima_agent::mcp::config::{RawHttpFields, RawStdioFields, RawTransport};
use makima_agent::mcp::{self, McpHandle};
use makima_agent::permissions::{PermissionAnswer, TaggedAnswer};
use makima_agent::session_coordinator::{
    ModelPreparer, SessionCoordinatorError, SessionCoordinatorHandle,
};
use makima_agent::session_defaults;
use makima_agent::session_options::{MODEL_OPTION_ID, SessionOptionError};
use makima_agent::session_recovery::{OpenModel, OpenSessionError, SessionSource, open_session};
use makima_agent::session_writer::SessionWriter;
use makima_agent::tools::QuestionMode;
use makima_agent::types::AgentEvent;
use makima_agent::{
    AgentId, AgentInput, AgentRef, Envelope, ImageMediaType, ImageSource, ModeError, TurnId,
    TurnTicket,
};
use makima_commands::{
    AgentTurn, CommandAttachment, CommandContent, CommandFuture, CommandOutcome, CommandRegistry,
    InputDispatch, PresentedCommand, TargetHandle,
};
use makima_config::project::{self, TrustAnswer, TrustMode, policy_grant};
use makima_config::{
    MAX_SERVER_NAME_LEN, PermissionsConfig, ProjectConfig, SessionDefaults, TrustConfig,
};
use makima_lua::orchestration::{
    AgentOrchestration, ModelCatalog, PinnedCommandTarget, TrustedTarget,
};
use makima_lua::session_snapshot::HeadlessObserver;
use makima_providers::model::Model;
use makima_providers::{Message, ProviderRegistry, TokenUsage, add_cost, settle_session};
use makima_storage::StateDir;
use makima_storage::id::{MakimaId, SessionRef};
use makima_storage::session_lock;
use makima_storage::session_state::StoredAgentConfig;
use makima_storage::sessions::{SESSIONS_DIR, SessionMeta, read_prefs};
use serde::Serialize;
use serde_json::Value;
use smol::io::AsyncBufReadExt;
use tracing::{debug, info, warn};

use crate::{AcpParams, methods, permissions, translate};

const ELICITATION_DISMISSED: &str = r#"{"dismissed":true}"#;
const FIRST_OUTGOING_REQUEST_ID: i64 = 1000;
const CANCELLATION_IN_PROGRESS_CODE: i32 = -32001;
const CANCELLATION_IN_PROGRESS_MESSAGE: &str =
    "session cancellation is still in progress; retry the prompt";
const NEW_SESSION_GUIDANCE: &str = "Start a new conversation using the client’s new-session action. This conversation has not been changed.";
const ACTIVE_OPERATION_MESSAGE: &str = "session already has an active operation";
const SESSION_ENDED_MESSAGE: &str = "session ended";
const EVENT_STREAM_ENDED: &str = "session event stream ended";
const GRAPH_SHUTDOWN_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(2);
const AUTH_FAILED_MSG: &str =
    "Authentication failed. Run `makima auth login`, then send the prompt again.";

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

struct PendingElicitation {
    request_id: i64,
    turn_id: TurnId,
}

/// What the client still owes us. Subagents have independent answer channels,
/// so more than one permission request can be outstanding.
#[derive(Default)]
struct Pending {
    operation: Option<PendingOperation>,
    /// Prompts whose message started a turn of its own behind the running
    /// one, as typing does in the TUI when it cannot join.
    queued: VecDeque<PendingOperation>,
    continuing_root: BTreeSet<TurnId>,
    cancelling_continuations: Vec<TurnTicket>,
    permissions: HashMap<i64, (String, Sender<String>)>,
    elicitation: Option<PendingElicitation>,
    question_root: Option<AgentRef>,
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
        if self.cancellation_in_progress() {
            return Err(AcpError::new(
                CANCELLATION_IN_PROGRESS_CODE,
                CANCELLATION_IN_PROGRESS_MESSAGE,
            )
            .data(serde_json::json!({ "retryable": true })));
        }
        match &self.operation {
            None => Ok(false),
            Some(operation) if operation.kind == OperationKind::PrimaryTurn => Ok(true),
            Some(_) => Err(AcpError::new(-32600, ACTIVE_OPERATION_MESSAGE)),
        }
    }

    fn cancellation_in_progress(&self) -> bool {
        self.cancelling_continuations
            .iter()
            .any(|ticket| ticket.peek().is_none())
            || self
                .operation
                .as_ref()
                .is_some_and(|operation| operation.cancelling)
    }

    fn refuses_asks(&self) -> bool {
        self.input_closed || self.cancellation_in_progress()
    }

    fn with_live_question_turn<T>(
        &self,
        turn_id: TurnId,
        deliver: impl FnOnce() -> T,
    ) -> Option<T> {
        self.question_root
            .as_ref()?
            .actor()
            .ok()?
            .with_live_turn(turn_id, deliver)
    }

    fn settle(&mut self) -> PendingOperation {
        self.permissions.clear();
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
    read: makima_agent::session_coordinator::SessionReadHandle,
    supports_boolean: bool,
    emitted_version: Mutex<u64>,
}

impl OptionProjection {
    fn apply(&self, snapshot: &makima_agent::session_options::SessionOptionsSnapshot) {
        self.update(snapshot, false);
    }

    fn emit(&self, snapshot: &makima_agent::session_options::SessionOptionsSnapshot) {
        self.update(snapshot, true);
    }

    fn update(&self, snapshot: &makima_agent::session_options::SessionOptionsSnapshot, emit: bool) {
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
    coordinator: Option<makima_agent::session_coordinator::SessionCoordinatorHandle>,
    orchestration: Option<Arc<AgentOrchestration>>,
    checkpoint: Option<Arc<SessionWriter>>,
    mcp: Option<McpHandle>,
    pending: PendingState,
    command_registry: makima_commands::CommandRegistry,
    command_target: TargetHandle,
    command_root: TrustedTarget,
    command_projection_task: smol::Task<()>,
    /// The Lua host can outlive the session holding an event sender, so the
    /// pump is stopped explicitly rather than by its stream closing.
    event_pump: EventPump,
    option_projection: Option<Arc<OptionProjection>>,
    option_projection_task: smol::Task<()>,
    lock: Option<Arc<session_lock::LockHeartbeat>>,
}

struct SpawnSession {
    model: Model,
    cwd: PathBuf,
    session_id: Option<SessionRef>,
    root_id: Option<AgentId>,
    journal: Option<Arc<SessionJournal>>,
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
    recovered_writer: Option<(Arc<SessionWriter>, Arc<session_lock::LockHeartbeat>)>,
    root_target: Receiver<TrustedTarget>,
    mcp: Option<McpHandle>,
    current_model: String,
    initial_cost: Option<f64>,
    cwd: PathBuf,
    defaults: SessionDefaults,
    persisted_options: &'a BTreeMap<String, String>,
    replay_history: &'a [Message],
    continuation_count: usize,
    continuing_root: BTreeSet<TurnId>,
}

struct Server {
    out_tx: Sender<Value>,
    catalog: ModelCatalog,
    session: Option<SessionState>,
    /// Whether the client advertised form elicitation support at `initialize`.
    elicitation: bool,
    supports_boolean: bool,
    lua_event_handle: makima_lua::EventHandle,
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
    admission: PinnedCommandTarget,
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
        catalog: ModelCatalog::offline(Arc::clone(&params.providers), &params.model_policy),
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
    makima_config::load_permissions(project_config)
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
        // decision can get. Recorded like any other yes so `makima trust list`
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

fn open_model(params: &AcpParams) -> OpenModel<'_> {
    OpenModel {
        startup: &params.model,
        explicit: params.model_explicit,
        providers: &params.providers,
        policy: &params.model_policy,
    }
}

fn session_open_error(id: MakimaId, error: OpenSessionError) -> AcpError {
    match error {
        OpenSessionError::Load(error) => {
            AcpError::resource_not_found(Some(format!("session/{id}"))).data(json_str(&error))
        }
        OpenSessionError::Busy(_) => {
            session_registration_error(session_lock::OPEN_ELSEWHERE_MSG.to_owned())
        }
        error => session_registration_error(error.to_string()),
    }
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
    let seed = StoredSession::new(&params.model.spec(), &cwd.to_string_lossy());
    let id = seed.id;
    let session_ref = SessionRef::from(id);
    let opening = open_session(
        &params.storage,
        SessionSource::Create(seed),
        open_model(params),
        None,
    )
    .map_err(|error| session_open_error(id, error))?;
    let model = opening.model.clone();
    let (checkpoint, lock, recovered) = opening
        .recover(&params.storage, &srv.lua_event_handle.mode_registry())
        .await
        .map_err(|error| session_open_error(id, error))?;
    let defaults = session_defaults::resolve(
        params.defaults,
        &read_prefs(&params.storage),
        &SessionMeta::default(),
        true,
        &model,
    );
    let spec = model.spec();
    let mcp = start_mcp(&req.cwd, &req.mcp_servers, project_config.clone(), params).await;
    let (handle, root_target) = spawn_session_with_checkpoint(
        params,
        SpawnSession {
            model,
            cwd: req.cwd,
            session_id: Some(session_ref),
            root_id: recovered.session().root_agent_id(),
            journal: Some(recovered.journal()),
            history: Vec::new(),
            mcp_handle: mcp.clone(),
            elicitation: srv.elicitation,
            yolo: params.yolo,
            defaults,
            project_config,
            permissions,
        },
        None,
        Some(Arc::clone(&checkpoint)),
    );
    let session_id = handle.session_id.to_string();
    let persisted_options = Default::default();
    let (snapshot, mode) = install_session(
        srv,
        params,
        InstallSession {
            handle,
            recovered_writer: Some((checkpoint, lock)),
            mcp,
            root_target,
            current_model: spec,
            initial_cost: None,
            cwd,
            defaults,
            persisted_options: &persisted_options,
            replay_history: &[],
            continuation_count: 0,
            continuing_root: BTreeSet::new(),
        },
    )
    .await
    .map_err(session_registration_error)?;
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
    let id = session_ref.id();
    // ACP restores the saved cwd; the client's cwd is a fallback for old sessions.
    if srv
        .session
        .as_ref()
        .is_some_and(|session| session.handle.session_id.id() == id)
    {
        // This process already owns the lock. Re-read after draining the old
        // writer so recovery includes changes published during shutdown.
        let seed = StoredSession::load(id, &params.storage)
            .map_err(|error| session_open_error(id, OpenSessionError::Load(error)))?;
        let state = seed
            .latest_state()
            .map_err(|error| session_open_error(id, OpenSessionError::Load(error)))?;
        let config = state
            .scopes
            .get(&seed.root_agent_id())
            .and_then(|scope| scope.config.as_ref());
        load_permissions(&trusted_project_config(
            &effective_session_cwd(config, &seed.cwd, &req.cwd),
            &params.storage,
            params.trust_mode,
            &params.trust_policy,
        ))?;
        close_session(srv).await;
    }
    let opening = open_session(
        &params.storage,
        SessionSource::Resume(id),
        open_model(params),
        None,
    )
    .map_err(|error| session_open_error(id, error))?;
    if let Some(warning) = &opening.warning {
        warn!(%warning, "saved session model could not be restored");
    }
    let recorded_model = opening.model.clone();
    let (checkpoint, lock, recovered) = opening
        .recover(&params.storage, &srv.lua_event_handle.mode_registry())
        .await
        .map_err(|error| session_open_error(id, error))?;
    let history_snapshot = recovered.session().history_snapshot();
    let root_id = recovered.session().root_agent_id();
    let recovered_state = recovered
        .session()
        .latest_state()
        .map_err(|error| session_registration_error(error.to_string()))?;
    let recovered_config = recovered_state
        .scopes
        .get(&root_id)
        .and_then(|scope| scope.config.as_ref());
    let session = recovered.session();
    let session_cwd = effective_session_cwd(recovered_config, &session.cwd, &req.cwd);
    let project_config = trusted_project_config(
        &session_cwd,
        &params.storage,
        params.trust_mode,
        &params.trust_policy,
    );
    let permissions = load_permissions(&project_config)?;
    let spec = recorded_model.spec();
    let yolo = params.yolo;
    let defaults = recovered_config.map_or_else(
        || {
            session_defaults::resolve(
                params.defaults,
                &read_prefs(&params.storage),
                &session.meta,
                history_snapshot.messages.is_empty(),
                &recorded_model,
            )
        },
        |config| SessionDefaults {
            fast: config.fast,
            workflow: config.workflow,
            thinking: config.thinking,
        },
    );
    let mut persisted_options = recovered_config
        .map(|config| &config.options)
        .unwrap_or(&session.meta.session_options)
        .clone();
    persisted_options.remove(MODEL_OPTION_ID);
    let mut continuation_count = recovered_state
        .scopes
        .values()
        .map(|scope| scope.admitted.len())
        .sum::<usize>();
    let mut continuing_root: BTreeSet<_> = recovered_state
        .scopes
        .get(&root_id)
        .into_iter()
        .flat_map(|scope| scope.admitted.keys().copied())
        .collect();
    if let (Some(config), Some(root)) = (recovered_config, root_id) {
        let restored_mode = makima_agent::plan_state::prepare_restore_for_agent(
            &recovered_state,
            root,
            &config.mode,
            &srv.lua_event_handle.mode_registry(),
        )
        .map_err(|error| session_registration_error(error.to_string()))?;
        if makima_agent::modes::is_read_only_fallback(&restored_mode.mode) {
            continuation_count -= continuing_root.len();
            continuing_root.clear();
        }
    }
    let mcp = start_mcp(
        &session_cwd,
        &req.mcp_servers,
        project_config.clone(),
        params,
    )
    .await;
    let replay_history = Arc::clone(&history_snapshot.messages);
    let (handle, root_target) = spawn_session_with_checkpoint(
        params,
        SpawnSession {
            model: recorded_model.clone(),
            cwd: session_cwd.clone(),
            session_id: Some(session_ref),
            root_id,
            journal: Some(recovered.journal()),
            history: Vec::new(),
            mcp_handle: mcp.clone(),
            elicitation: srv.elicitation,
            yolo,
            defaults,
            project_config,
            permissions,
        },
        Some(history_snapshot),
        Some(Arc::clone(&checkpoint)),
    );
    let mut usage_by_model = session.usage_by_model().clone();
    let restored_cost = settle_session(
        &session.token_usage,
        &mut usage_by_model,
        &recorded_model,
        defaults.fast,
        &params.providers,
    );
    let (snapshot, mode) = install_session(
        srv,
        params,
        InstallSession {
            handle,
            recovered_writer: Some((checkpoint, lock)),
            mcp,
            current_model: spec,
            root_target,
            initial_cost: restored_cost,
            cwd: session_cwd.clone(),
            defaults,
            persisted_options: &persisted_options,
            replay_history: &replay_history,
            continuation_count,
            continuing_root,
        },
    )
    .await
    .map_err(session_registration_error)?;
    if let Some(session) = &srv.session {
        srv.lua_event_handle
            .notify_state_restored(session.command_root.target.clone());
    }
    let resp = methods::load_session_response(&mode, &srv.lua_event_handle.mode_registry())
        .config_options(methods::session_config_options(
            &snapshot,
            srv.supports_boolean,
        ));
    Ok(AgentResponse::LoadSessionResponse(resp))
}

#[cfg(test)]
fn spawn_session(
    params: &AcpParams,
    session: SpawnSession,
) -> (InteractiveHandle, Receiver<TrustedTarget>) {
    spawn_session_with_checkpoint(params, session, None, None)
}

fn spawn_session_with_checkpoint(
    params: &AcpParams,
    session: SpawnSession,
    snapshot: Option<makima_agent::agent::HistorySnapshot>,
    checkpoint: Option<Arc<SessionWriter>>,
) -> (InteractiveHandle, Receiver<TrustedTarget>) {
    let SpawnSession {
        model,
        cwd,
        session_id,
        root_id,
        journal,
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
        root_id,
        journal,
        modes: params.lua_event_handle.mode_registry(),
        initial_history: history,
        yolo,
        system_prompt_override: params.system_prompt_override.clone(),
        append_system_prompt: params.append_system_prompt.clone(),
        defaults,
        model_policy: Arc::clone(&params.model_policy),
        providers: Arc::clone(&params.providers),
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
    let (binding, root_target) = makima_lua::orchestration::root_binding();
    let prepare = headless::default_preparation(Arc::clone(&params.providers));
    let handle = match checkpoint {
        Some(checkpoint) => headless::spawn_interactive_with_checkpoint(
            interactive,
            prepare,
            snapshot,
            Some(binding),
            checkpoint,
        ),
        None => {
            headless::spawn_interactive_with_binding(interactive, prepare, snapshot, Some(binding))
        }
    };
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
    let permission_answers = {
        let mut pending = session.pending.lock().unwrap();
        pending.input_closed = true;
        if let Some(elicitation) = pending.elicitation.take() {
            let _ = pending.with_live_question_turn(elicitation.turn_id, || {
                let _ = session
                    .handle
                    .answer_tx
                    .try_send(ELICITATION_DISMISSED.into());
            });
        }
        std::mem::take(&mut pending.permissions)
    };
    deny_permissions(permission_answers);
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
    let (operations, permission_answers) = {
        let mut pending = state.pending.lock().unwrap();
        pending.elicitation = None;
        (pending.take_all(), std::mem::take(&mut pending.permissions))
    };
    for request_id in operations.iter().flat_map(PendingOperation::requests) {
        respond_prompt(&srv.out_tx, request_id, StopReason::Cancelled);
    }
    state.handle.manager.suspend(GRAPH_SHUTDOWN_TIMEOUT).await;
    deny_permissions(permission_answers);
    state.handle.manager.journal().invalidate_state_access();
    state.handle.task.cancel().await;
    if let Some(coordinator) = state.coordinator.take()
        && let Err(error) = coordinator.close().await
    {
        warn!(%error, "failed to drain session coordinator");
    }
    state.command_projection_task.cancel().await;
    // Forwarding the last turn events can enqueue usage records.
    state.event_pump.finish().await;
    if let Some(checkpoint) = state.checkpoint
        && let Err(error) = checkpoint.drain().await
    {
        warn!(%error, "failed to drain session checkpoint");
    }
    state.option_projection_task.cancel().await;
    if let Some(mcp) = state.mcp {
        mcp.shutdown().await;
    }
    drop(state.lock.take());
}

fn start_session_lock_in(
    dir: PathBuf,
    id: MakimaId,
) -> Result<Arc<session_lock::LockHeartbeat>, String> {
    let lease = session_lock::claim(&dir, &id)
        .map_err(|error| error.to_string())?
        .ok_or_else(|| session_lock::OPEN_ELSEWHERE_MSG.to_owned())?;
    Ok(Arc::new(session_lock::LockHeartbeat::spawn(
        lease,
        move || {
            if let Ok(coordinator) =
                makima_agent::session_coordinator::SessionCoordinatorHandle::resolve(id)
            {
                coordinator.retire();
            }
        },
    )))
}

async fn rollback_install(
    handle: InteractiveHandle,
    mcp: Option<McpHandle>,
    coordinator: Option<makima_agent::session_coordinator::SessionCoordinatorHandle>,
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
) -> Result<
    (
        makima_agent::session_options::SessionOptionsSnapshot,
        String,
    ),
    String,
> {
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
    lock_session: impl FnOnce(MakimaId) -> Result<Arc<session_lock::LockHeartbeat>, String>,
) -> Result<
    (
        makima_agent::session_options::SessionOptionsSnapshot,
        String,
    ),
    String,
> {
    let InstallSession {
        handle,
        recovered_writer,
        root_target,
        mcp,
        current_model,
        initial_cost,
        cwd,
        defaults,
        persisted_options,
        replay_history,
        continuation_count,
        continuing_root,
    } = session;
    let definitions = makima_agent::session_coordinator::builtin_option_definitions(
        Arc::from(current_model.as_str()),
        srv.catalog.snapshot(),
        handle.permissions.is_yolo(),
        defaults.fast,
        defaults.workflow,
        defaults.thinking.into(),
    );
    let (recovered_checkpoint, recovered_lock) = recovered_writer.unzip();
    let lock = match recovered_lock
        .map(Ok)
        .unwrap_or_else(|| lock_session(handle.session_id.id()))
    {
        Ok(lock) => lock,
        Err(error) => {
            warn!(%error, "session lock claim failed");
            rollback_install(handle, mcp, None).await;
            return Err(error);
        }
    };
    let mut seed = StoredSession::new(&current_model, &cwd.to_string_lossy());
    seed.id = handle.session_id.id();
    if let Err(error) = handle.initialized_root().await {
        rollback_install(handle, mcp, None).await;
        return Err(error);
    }
    if let Err(error) = handle.manager.journal().snapshot().apply_to(&mut seed) {
        rollback_install(handle, mcp, None).await;
        return Err(error.to_string());
    }
    let checkpoint = match recovered_checkpoint.map(Ok).unwrap_or_else(|| {
        params
            .storage
            .ensure_subdir(SESSIONS_DIR)
            .map_err(|error| makima_agent::session_writer::WriterError {
                session_id: seed.id,
                message: error.to_string().into(),
            })
            .and_then(|directory| SessionWriter::open_session(&directory, &seed))
            .map(Arc::new)
    }) {
        Ok(checkpoint) => checkpoint,
        Err(error) => {
            warn!(%error, "failed to open session checkpoint");
            rollback_install(handle, mcp, None).await;
            return Err(error.to_string());
        }
    };
    checkpoint.bind_publication_guard(Some(lock.publication_guard()));
    let bound = async {
        handle.initialized_root().await?;
        checkpoint
            .bind_journal(handle.manager.journal())
            .await
            .map_err(|error| error.to_string())
    }
    .await;
    if let Err(error) = bound {
        rollback_install(handle, mcp, None).await;
        return Err(error);
    }
    let coordinator = match makima_agent::session_coordinator::SessionCoordinatorHandle::register(
        makima_agent::session_coordinator::SessionCoordinatorParams {
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
    if let Err(error) = coordinator.bind_permissions(Arc::clone(&handle.permissions)) {
        rollback_install(handle, mcp, Some(coordinator)).await;
        return Err(error.to_string());
    }
    let trusted_root = match makima_lua::orchestration::bind_frontend_root(
        &handle,
        &coordinator,
        &root_target,
        headless::default_model_preparer(Arc::clone(&params.providers)),
    )
    .await
    {
        Ok(root) => root,
        Err(error) => {
            rollback_install(handle, mcp, Some(coordinator)).await;
            return Err(error);
        }
    };
    let orchestration = makima_lua::orchestration::durable_frontend(
        "ACP",
        &srv.catalog,
        trusted_root.clone(),
        coordinator.clone(),
        Arc::clone(&checkpoint),
    );
    let usage_seed = match StoredSession::load(handle.session_id.id(), &params.storage) {
        Ok(seed) => seed,
        Err(error) => {
            rollback_install(handle, mcp, Some(coordinator)).await;
            return Err(error.to_string());
        }
    };
    let mode = if continuation_count == 0 {
        checkpoint_root_mode(&handle, &coordinator).await
    } else {
        // The recovered config is already durable and replay may have made the graph busy.
        trusted_root
            .target
            .effective_config()
            .map_err(|error| error.to_string())
            .and_then(|config| config.ok_or_else(|| SESSION_ENDED_MESSAGE.to_owned()))
            .map(|config| config.mode.id().key().to_owned())
    };
    let mode = match mode {
        Ok(mode) => mode,
        Err(error) => {
            rollback_install(handle, mcp, Some(coordinator)).await;
            return Err(error);
        }
    };
    // Do not retire the active session or replace its Lua services until startup succeeds.
    close_session(srv).await;
    srv.catalog.attach(&orchestration);
    params
        .lua_event_handle
        .install_orchestration_services(orchestration.clone());
    let pending = Arc::new(Mutex::new(Pending {
        continuing_root,
        question_root: Some(trusted_root.target.clone()),
        ..Pending::default()
    }));
    let session_id = SessionId::from(handle.session_id.to_string());
    let home = makima_storage::paths::home();
    for update in translate::replay_history(replay_history, Some(&cwd), home.as_deref()) {
        session_update(&srv.out_tx, &session_id, update);
    }
    if let Some(message) = makima_agent::session_recovery::continuation_notice(continuation_count) {
        session_update(
            &srv.out_tx,
            &session_id,
            translate::text_delta(&format!("{message}\n")),
        );
    }
    let project_trusted = handle.permissions.project_is_trusted();
    let observer = HeadlessObserver::new(params.lua_event_handle.clone(), handle.session_id.id());
    observer.persist_usage(
        checkpoint.as_ref().clone(),
        usage_seed.token_usage,
        usage_seed.usage_by_model().clone(),
    );
    observer.install(trusted_root.target.clone());
    let event_pump = start_event_pump(
        handle.event_rx.clone(),
        handle.session_id.clone(),
        srv.out_tx.clone(),
        Arc::clone(&pending),
        srv.elicitation,
        handle.answer_tx.clone(),
        coordinator.read(),
        home,
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
        headless::default_model_preparer(Arc::clone(&params.providers)),
        Arc::clone(&params.providers),
    );
    orchestration.bind_commands(
        &trusted_root.target,
        makima_lua::orchestration::command_runner(
            command_registry.clone(),
            command_target.clone(),
            makima_lua::orchestration::headless_outcomes(&handle),
        ),
    );
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
        command_root: trusted_root,
        command_projection_task,
        event_pump,
        option_projection: Some(option_projection),
        option_projection_task,
        lock: Some(lock),
    });
    Ok((option_snapshot, mode))
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
    providers: Arc<ProviderRegistry>,
) -> TargetHandle {
    registry.bind_target_with_presentation(
        makima_agent::command::session_capabilities(),
        makima_agent::command::portable_capabilities(),
        Arc::new(makima_lua::orchestration::PinnedCommandHost::new(
            makima_agent::command::SessionCommandHost::new(
                handle.control_tx.clone(),
                coordinator.clone(),
                providers,
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
    snapshot: &makima_agent::session_options::SessionOptionsSnapshot,
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
    mut subscription: makima_agent::session_options::SessionOptionsSubscription,
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
    registry: makima_commands::CommandRegistry,
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

type StoredSession =
    makima_storage::sessions::Session<Message, TokenUsage, makima_agent::ToolOutput>;

fn validate_session<'a>(srv: &'a Server, requested: &str) -> Result<&'a SessionState, AcpError> {
    let session = srv.session.as_ref().ok_or_else(no_session)?;
    let active = session.handle.session_id.to_string();
    if active != requested
        || makima_agent::session_coordinator::SessionCoordinatorHandle::resolve(
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
    if !session
        .handle
        .manager
        .same_manager(&session.command_root.target.manager())
        || session.handle.manager.root_id().ok() != Some(session.command_root.target.id())
    {
        return Err(AcpError::new(-32603, SESSION_ENDED_MESSAGE));
    }
    let admission = PinnedCommandTarget::pin(session.command_root.clone())
        .map_err(session_registration_error)?;
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
    let future = session.command_registry.dispatch_input_with_admission(
        &session.command_target,
        content,
        Arc::new(admission.clone()),
    );
    srv.dispatch = Some(PendingDispatch {
        operation_id,
        cancels,
        request_id: id.clone(),
        admission,
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
        admission,
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
    run_dispatch(srv, &request_id, dispatched, &admission).await
}

async fn run_dispatch(
    srv: &Server,
    id: &RequestId,
    dispatch: InputDispatch,
    admission: &PinnedCommandTarget,
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
                admission,
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
            send_command_turn(session, id, turn, admission).await
        }
        InputDispatch::Dispatched(CommandOutcome::IsolatedTurn(turn)) => {
            send_isolated_turn(session, &srv.out_tx, id, turn, admission).await
        }
        InputDispatch::Dispatched(CommandOutcome::ManualCompaction(instructions)) => {
            send_manual_compaction(session, &srv.out_tx, id, instructions, admission).await
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
    admission: &PinnedCommandTarget,
) -> Result<(), AcpError> {
    let actor = admission.actor().map_err(session_registration_error)?;
    if session.pending.lock().unwrap().operation.is_some() {
        return Err(AcpError::new(-32600, ACTIVE_OPERATION_MESSAGE));
    }
    session.coordinator.as_ref().ok_or_else(no_session)?;
    let operation_id = NEXT_OPERATION_ID.fetch_add(1, Ordering::Relaxed);
    let tool_id = format!("compact-{operation_id}");
    let (trigger, cancel) = makima_agent::cancel::CancelToken::new();
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
        .send_to(
            actor,
            makima_agent::headless::InteractiveControl::ManualCompaction {
                instructions,
                output,
                cancel,
            },
        )
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
            use makima_agent::headless::ManualCompactionEvent;

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
    turn: makima_commands::IsolatedTurn,
    admission: &PinnedCommandTarget,
) -> Result<(), AcpError> {
    let actor = admission.actor().map_err(session_registration_error)?;
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
    let (trigger, cancel) = makima_agent::cancel::CancelToken::new();
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
        .send_isolated_to(actor, turn.content.text.to_string(), images, output, cancel)
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
            use makima_agent::agent::isolated_turn::IsolatedTurnEvent;

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
    admission: &PinnedCommandTarget,
) -> Result<(), AcpError> {
    let prompt = turn.prompt.map(|prompt| makima_agent::McpPromptRef {
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
        .for_actor(admission.actor().map_err(session_registration_error)?);
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

fn command_error(error: makima_commands::CommandError) -> AcpError {
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
                    | SessionOptionError::UnresolvedModel(_)
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
    makima_lua::orchestration::set_mode(
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
            makima_agent::session_options::ENABLED_VALUE
        } else {
            makima_agent::session_options::DISABLED_VALUE
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
            makima_lua::SessionOptionMutationError::Config(error) => config_error(error),
            makima_lua::SessionOptionMutationError::HostDead => {
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

fn effective_session_cwd(
    config: Option<&StoredAgentConfig>,
    saved: &str,
    client: &Path,
) -> PathBuf {
    let cwd = Path::new(config.map_or(saved, |config| config.cwd.as_str()));
    if cwd.is_absolute() { cwd } else { client }.to_path_buf()
}

fn deny_permissions(permission_answers: HashMap<i64, (String, Sender<String>)>) {
    for (request_id, answer_tx) in permission_answers.into_values() {
        let _ = answer_tx.send(TaggedAnswer::new(request_id, PermissionAnswer::Deny).encode());
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
                let (cancellations, permission_answers) = {
                    let mut pending = session.pending.lock().unwrap();
                    let pending = &mut *pending;
                    pending.cancels += 1;
                    if pending.cancellation_in_progress() {
                        return;
                    }
                    let mut cancellations: Vec<_> = pending
                        .operation
                        .iter_mut()
                        .chain(&mut pending.queued)
                        .filter_map(PendingOperation::start_cancelling)
                        .collect();
                    pending.cancelling_continuations.clear();
                    if !pending.continuing_root.is_empty()
                        && let Ok(actor) = session.command_root.target.actor()
                    {
                        for turn_id in std::mem::take(&mut pending.continuing_root) {
                            if let Some(ticket) = actor.turn_ticket(turn_id)
                                && ticket.peek().is_none()
                            {
                                if !cancellations
                                    .iter()
                                    .any(|cancel| cancel.turn_id() == Some(turn_id))
                                {
                                    cancellations
                                        .push(session.handle.input_tx.turn_cancel(ticket.clone()));
                                }
                                pending.cancelling_continuations.push(ticket);
                            }
                        }
                    }
                    if pending.operation.is_none() && pending.cancelling_continuations.is_empty() {
                        return;
                    }
                    let permission_answers = std::mem::take(&mut pending.permissions);
                    pending.elicitation = None;
                    (cancellations, permission_answers)
                };
                deny_permissions(permission_answers);
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
            && let Some(elicitation) = pending
                .elicitation
                .take_if(|elicitation| elicitation.request_id == id)
        {
            let answer = elicitation_answer(raw);
            let _ = pending.with_live_question_turn(elicitation.turn_id, || {
                let _ = session.handle.answer_tx.try_send(answer);
            });
            None
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
        _ => ELICITATION_DISMISSED.into(),
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
    session: makima_agent::session_coordinator::SessionReadHandle,
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
            let turn_id = envelope.identity.and_then(|identity| identity.turn_id());
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
                    let mut pending = pending.lock().unwrap();
                    if let Some(turn_id) = turn_id
                        && !pending.operation.as_ref().is_some_and(|operation| {
                            operation.cancelling && operation.run_id == Some(run_id)
                        })
                        && !pending
                            .cancelling_continuations
                            .iter()
                            .any(|ticket| ticket.turn_id() == turn_id)
                    {
                        let form = (elicitation && !pending.input_closed)
                            .then(|| crate::elicitation::build_form(&questions, &sid, &id))
                            .flatten();
                        let request_id = pending
                            .with_live_question_turn(turn_id, || {
                                if let Some(form) = form {
                                    let request = AgentRequest::CreateElicitationRequest(form);
                                    let outgoing_id =
                                        NEXT_OUTGOING_REQUEST_ID.fetch_add(1, Ordering::Relaxed);
                                    send(
                                        &out_tx,
                                        Request {
                                            id: RequestId::Number(outgoing_id),
                                            method: Arc::from(request.method()),
                                            params: Some(request),
                                        },
                                    );
                                    Some(outgoing_id)
                                } else {
                                    let _ = answer_tx.try_send(ELICITATION_DISMISSED.into());
                                    None
                                }
                            })
                            .flatten();
                        if let Some(request_id) = request_id {
                            pending.elicitation = Some(PendingElicitation {
                                request_id,
                                turn_id,
                            });
                        }
                    }
                    continue;
                }
                AgentEvent::TurnOutcome(outcome) => {
                    tool_inputs.clear();
                    {
                        let mut pending = pending.lock().unwrap();
                        let turn_id = outcome.turn_id();
                        pending.continuing_root.remove(&turn_id);
                        if pending
                            .elicitation
                            .as_ref()
                            .is_some_and(|elicitation| elicitation.turn_id == turn_id)
                        {
                            pending.elicitation = None;
                        }
                    }
                    finish_primary_run(&pending, run_id, |request_id| match &outcome {
                        makima_agent::TurnOutcome::Completed { reason, .. } => {
                            let resp = PromptResponse::new(translate::map_done_reason(*reason));
                            send(
                                &out_tx,
                                Response::new(request_id, Ok(AgentResponse::PromptResponse(resp))),
                            );
                        }
                        makima_agent::TurnOutcome::Cancelled { .. } => {
                            respond_prompt(&out_tx, request_id, StopReason::Cancelled);
                        }
                        makima_agent::TurnOutcome::Failed { failure, .. } => {
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
            pending.elicitation = None;
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

async fn checkpoint_root_mode(
    handle: &InteractiveHandle,
    coordinator: &SessionCoordinatorHandle,
) -> Result<String, String> {
    let root = handle.initialized_root().await?;
    let config = headless::checkpoint_startup_config(&root, coordinator).await?;
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
    use makima_agent::actor::{ActorLifecycle, ActorStatus, TurnResult};
    use makima_agent::agent::PromptSlotSource;
    use makima_agent::journal::{JournalChange, JournalCommit};
    use makima_agent::manager::{AgentResidency, GraphLifecycle};
    use makima_agent::state::{StateError, StateRevision, StateScope, StateSlotSpec};
    use makima_agent::types::{ActorWorkIdentity, ActorWorkKind};
    use makima_agent::{AgentMode, DoneReason, ModeId, ToolOutput};
    use makima_agent::{TurnCancellationReason, TurnOutcome};
    use makima_commands::TargetCapabilities;
    use makima_lua::orchestration::{OrchestrationServices, TurnCompletion};
    use makima_providers::login::{AuthSpec, AuthStatus, InteractionService};
    use makima_providers::provider::{BoxFuture, Provider};
    use makima_providers::{
        ContentBlock as MsgBlock, FastSupport, Invocation, ModelInfo, RequestDelivery, Role,
        TokenUsage,
    };
    use makima_providers::{PreparedProvider, ProviderBackend, RegisteredProvider};
    use makima_storage::StateDir;
    use makima_storage::checkpoint::{ClaimingWriter, accept_all};
    use makima_storage::plans::{PLAN_STATE_KEY, PLAN_STATE_VERSION, PlanEntry, PlanSlot};
    use makima_storage::session_state::{
        StateChange, StoredCancellationReason, StoredStateRecord, StoredTurnOrigin,
        StoredTurnStatus,
    };
    use makima_storage::sessions::{Effort, Session, StoredThinking};
    use smol::future::poll_once;
    use tempfile::TempDir;
    use test_case::test_case;

    use super::*;

    const ELICITATION_METHOD: &str = "elicitation/create";
    const ANSWERED_ID: i64 = 1001;
    const UNKNOWN_ID: i64 = 1002;
    const DISCOVERED_SPEC: &str = "openrouter/discovered-model";
    const FAST_SPEC: &str = "anthropic/claude-opus-4-8";
    const OFFLINE_SPEC: &str = "openai/gpt-5";
    const SAVED_PLAN_FILE: &str = "saved-plan.md";
    const SAVED_PLAN_CONTENT: &str = "saved legacy plan content";

    const THINKING_MODEL_ID: &str = "acp-load-thinker";
    const PRIMARY_TURN_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(2);
    const LIFECYCLE_STRESS_ITERATIONS: usize = 64;
    const SPAWN_TEST_SPEC: &str = "acp-test/acp-end-turn-test";
    const DISCOVERY_PROVIDER: &str = "acp-test";
    const DISCOVERY_MODEL_ID: &str = "acp-discovery-refresh-test";
    const DISCOVERED_WINDOW: u32 = 524_000;
    const NEW_SESSION_REQUEST: i64 = 91;
    const RESET_STATE_KEY: &str = "acp/reset-state";
    const RESET_STATE_VALUE: &str = "previous session value";
    static MANAGED_PLUGIN_TEST: Mutex<()> = Mutex::new(());

    /// The providers of the specs above: only [`FAST_SPEC`] supports fast mode.
    fn spec_providers() -> Arc<ProviderRegistry> {
        let model = |spec: &'static str, fast| {
            let (provider, id) = Model::parse_spec(spec).unwrap();
            let info = ModelInfo {
                supports_thinking: Some(true),
                supports_vision: Some(true),
                supports_fast: Some(FastSupport::from(fast)),
                ..ModelInfo::id_only(id.into())
            };
            (provider, vec![info])
        };
        Arc::new(makima_providers::test_util::offline_registry(vec![
            model(FAST_SPEC, true),
            model(OFFLINE_SPEC, false),
            model(SPAWN_TEST_SPEC, false),
        ]))
    }
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
            makima_lua::EventHandle::disconnected_for_test(),
            MakimaId::generate(),
        )
    }

    struct RecordingAcpProvider {
        name: &'static str,
        requests: Sender<(&'static str, String, bool)>,
        nested: bool,
        permission: bool,
    }

    impl makima_providers::provider::Provider for RecordingAcpProvider {
        fn stream_message<'a>(
            &'a self,
            model: &'a Model,
            messages: &'a [Message],
            system: &'a str,
            tools: &'a Value,
            events: &'a Sender<makima_providers::ProviderEvent>,
            options: makima_providers::RequestOptions,
            session: Option<&'a SessionRef>,
            invocation: &'a Invocation,
            delivery: RequestDelivery,
        ) -> makima_providers::provider::BoxFuture<
            'a,
            Result<makima_providers::StreamResponse, makima_agent::AgentError>,
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
                    makima_providers::StopReason::ToolUse
                } else {
                    makima_providers::StopReason::EndTurn
                };
                return Box::pin(async move {
                    Ok(makima_providers::StreamResponse {
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
            makima_providers::provider::Provider::stream_message(
                &EndTurnProvider,
                model,
                messages,
                system,
                tools,
                events,
                options,
                session,
                invocation,
                delivery,
            )
        }
    }

    struct TerminalSyncGate {
        bytes: Vec<u8>,
        root: AgentId,
        turns: Vec<String>,
        gate: (Sender<()>, Receiver<()>),
    }

    impl makima_agent::session_writer::SessionIo for TerminalSyncGate {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.bytes.extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn sync(&mut self) -> std::io::Result<()> {
            let root = self.root.to_string();
            for line in self
                .bytes
                .split(|byte| *byte == b'\n')
                .filter(|line| !line.is_empty())
            {
                let record: Value = serde_json::from_slice(line).map_err(std::io::Error::other)?;
                if record["t"] == "state"
                    && record["d"]["agent"] == root
                    && record["d"]["change"]["kind"] == "turn"
                    && matches!(
                        record["d"]["change"]["result"]["text"]["value"].as_str(),
                        Some("root result" | "done")
                    )
                    && let Some(turn) = record["d"]["change"]["turn"].as_str()
                    && !self.turns.iter().any(|seen| seen == turn)
                {
                    self.turns.push(turn.to_owned());
                    self.gate.0.send(()).map_err(std::io::Error::other)?;
                    self.gate.1.recv().map_err(std::io::Error::other)?;
                }
            }
            Ok(())
        }
    }

    async fn managed_test_session(
        srv: &mut Server,
        nested: bool,
        permission: bool,
        completion: Option<TurnCompletion>,
        sync_gate: Option<(Sender<()>, Receiver<()>)>,
        checkpoint: Arc<
            dyn makima_storage::checkpoint::CheckpointWriter<
                    makima_agent::session_coordinator::SessionCheckpoint,
                >,
        >,
    ) -> (
        makima_lua::PluginHost,
        Receiver<(&'static str, String, bool)>,
        Receiver<String>,
        TempDir,
        Receiver<String>,
    ) {
        let (requests, received) = flume::unbounded();
        let provider: Arc<dyn makima_providers::provider::Provider> =
            Arc::new(RecordingAcpProvider {
                name: "managed provider",
                requests,
                nested,
                permission,
            });
        let prepared = Arc::clone(&provider);
        let registry = Arc::clone(makima_agent::tools::ToolRegistry::global_arc());
        let (ui, notices) = makima_lua::test_support::RecordingUi::new();
        let host = makima_lua::PluginHost::with_session_provider_preparer(
            registry,
            Default::default(),
            true,
            Arc::new(makima_lua::test_support::InMemoryFs::new()),
            None,
            Some(Arc::new(move |model| Ok((model, Arc::clone(&prepared))))),
            makima_lua::ui_sink::HostUi::Sink(Box::new(ui)),
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
            makima.api.register_command({{
                name = "/acp_fast", tui_only = false,
                handler = function(_, ctx)
                    local root = assert(ctx:agent())
                    assert(assert(root:set_model(ctx, {{ fast = true }})).fast)
                    assert(assert(root:config(ctx)).model.fast)
                    local config = assert(root:config(ctx))
                    assert(config.model.fast and config.model.spec == "{FAST_SPEC}", makima.json.encode(config))
                    local view, view_err = makima.fn.winsaveview()
                    assert(view == nil and view_err == "{no_ui}", view_err)
                end,
            }})
            makima.api.create_autocmd("TurnEnd", {{
                callback = function(ev)
                    makima.ui.flash("{ACP_TURN_END}" .. ev.data.session_id)
                end,
            }})
        "#,
                no_ui = makima_lua::ui_sink::NO_UI_ERR
            ),
        )
        .unwrap();
        if permission {
            host.load_source("acp_child_permission", r#"
                makima.api.register_tool({
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
        let (binding, root_target) = makima_lua::orchestration::root_binding();
        let handle = headless::spawn_interactive_with_binding(
            InteractiveParams {
                model: spec_providers().model(FAST_SPEC).unwrap(),
                config: makima_agent::AgentConfig {
                    max_concurrent_agent_turns: 1,
                    ..Default::default()
                },
                permissions_config: makima_config::PermissionsConfig {
                    default: makima_config::DefaultEffect::Prompt,
                    ..Default::default()
                },
                timeouts: Default::default(),
                prompt_slots: Default::default(),
                excluded_tools: Vec::new(),
                mcp_handle: None,
                initial_wd: PathBuf::from("/tmp"),
                session_id: Some(id.clone()),
                root_id: None,
                journal: None,
                initial_history: Vec::new(),
                yolo: false,
                system_prompt_override: Some(String::new()),
                append_system_prompt: None,
                defaults: Default::default(),
                model_policy: Arc::default(),
                providers: spec_providers(),
                modes: Arc::new(makima_agent::ModeRegistry::builtin()),
                question_mode: QuestionMode::Headless,
                plugin_rules: Arc::default(),
                project_config: ProjectConfig::for_project(Path::new("/tmp")),
                local_tools: Default::default(),
                state_dir: None,
                answers: true,
            },
            Box::new(move |model| {
                Box::pin(async move {
                    Ok(makima_agent::actor::PreparedModel {
                        model,
                        provider: initial,
                    })
                })
            }),
            None,
            Some(Box::new(
                move |manager, target, template, initialization| {
                    Box::pin(async move {
                        let answers = template
                            .user_response_rx
                            .as_ref()
                            .unwrap()
                            .lock()
                            .await
                            .clone();
                        root_answers_tx.send_async(answers).await.unwrap();
                        binding(manager, target, template, initialization).await
                    })
                },
            )),
        );
        let replacement = Arc::clone(&provider);
        let root = makima_lua::orchestration::bind_frontend_root(
            &handle,
            &coordinator,
            &root_target,
            Arc::new(move |spec| {
                let provider = Arc::clone(&replacement);
                let model = spec_providers().model(&spec).unwrap();
                Box::pin(async move { Ok(makima_agent::actor::PreparedModel { provider, model }) })
            }),
        )
        .await
        .unwrap();
        let state = TempDir::new().unwrap();
        let mut child_seed = StoredSession::new(FAST_SPEC, "/tmp");
        child_seed.id = id.id();
        let child_checkpoint = Arc::new(match sync_gate {
            Some(gate) => SessionWriter::with_io(
                id.id(),
                TerminalSyncGate {
                    bytes: Vec::new(),
                    root: root.target.id(),
                    turns: Vec::new(),
                    gate,
                },
                1,
            ),
            None => {
                SessionWriter::open_session(&state.path().join(SESSIONS_DIR), &child_seed).unwrap()
            }
        });
        child_checkpoint
            .bind_journal(handle.manager.journal())
            .await
            .unwrap();
        let services = Arc::new_cyclic(|services| {
            let (child_config, persist) = makima_lua::orchestration::child_persistence(
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
            makima_lua::orchestration::frontend_root(
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
        session.command_root = root.clone();
        session.command_registry = host.command_registry();
        session.command_target = session_command_target(
            &session.command_registry,
            &session.handle,
            &coordinator,
            &root,
            &services,
            headless::default_model_preparer(Arc::default()),
            Arc::default(),
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

    impl makima_providers::provider::Provider for EndTurnProvider {
        fn stream_message<'a>(
            &'a self,
            _: &'a Model,
            _: &'a [Message],
            _: &'a str,
            _: &'a Value,
            _: &'a Sender<makima_providers::ProviderEvent>,
            _: makima_providers::RequestOptions,
            _: Option<&'a SessionRef>,
            _invocation: &'a Invocation,
            _delivery: RequestDelivery,
        ) -> makima_providers::provider::BoxFuture<
            'a,
            Result<makima_providers::StreamResponse, makima_agent::AgentError>,
        > {
            Box::pin(async {
                Ok(makima_providers::StreamResponse {
                    message: Message {
                        role: makima_providers::Role::Assistant,
                        content: vec![makima_providers::ContentBlock::Text {
                            text: "done".into(),
                        }],
                        ..Default::default()
                    },
                    usage: TokenUsage::default(),
                    stop_reason: Some(makima_providers::StopReason::EndTurn),
                })
            })
        }
    }

    /// Records the last user message of every request and parks it until
    /// the test releases it, then thinks and answers.
    struct ParkedProvider {
        requests: Sender<Message>,
        release: Receiver<()>,
    }

    impl makima_providers::provider::Provider for ParkedProvider {
        fn stream_message<'a>(
            &'a self,
            _: &'a Model,
            messages: &'a [Message],
            _: &'a str,
            _: &'a Value,
            events: &'a Sender<makima_providers::ProviderEvent>,
            _: makima_providers::RequestOptions,
            _: Option<&'a SessionRef>,
            _invocation: &'a Invocation,
            _delivery: RequestDelivery,
        ) -> makima_providers::provider::BoxFuture<
            'a,
            Result<makima_providers::StreamResponse, makima_agent::AgentError>,
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
                    makima_providers::ProviderEvent::ThinkingDelta {
                        text: PARKED_THOUGHT.into(),
                    },
                    makima_providers::ProviderEvent::TextDelta {
                        text: PARKED_REPLY.into(),
                    },
                ] {
                    let _ = events.send(event);
                }
                Ok(makima_providers::StreamResponse {
                    message: Message {
                        role: Role::Assistant,
                        content: vec![MsgBlock::Text {
                            text: PARKED_REPLY.into(),
                        }],
                        ..Default::default()
                    },
                    usage: TokenUsage::default(),
                    stop_reason: Some(makima_providers::StopReason::EndTurn),
                })
            })
        }
    }

    /// The fixture root's real actor, and what its provider was asked.
    struct RootTurns {
        actor: makima_agent::AgentActorHandle,
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
                matches!(outcome, makima_agent::TurnOutcome::Cancelled { .. }),
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
            model_explicit: false,
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
            providers: test_providers(Arc::default()),
            plugin_rules: Arc::default(),
            lua_event_handle: makima_lua::EventHandle::disconnected_for_test(),
            command_registry: test_registry(&[]),
            trust_mode: TrustMode::Consult,
            trust_policy: Arc::default(),
        }
    }

    const PUMP_TRUSTED: bool = true;

    fn test_registry(
        custom_commands: &[makima_agent::command::CustomCommand],
    ) -> makima_commands::CommandRegistry {
        let registry = makima_commands::CommandRegistry::new();
        let commands = makima_agent::command::StandardCommands::register(
            &registry,
            custom_commands,
            makima_agent::command::StandardCompletions::default(),
        )
        .unwrap();
        std::mem::forget(commands);
        registry
    }

    fn test_target(
        registry: &makima_commands::CommandRegistry,
        control_tx: makima_agent::headless::InteractiveControlSender,
        coordinator: makima_agent::session_coordinator::SessionCoordinatorHandle,
    ) -> TargetHandle {
        {
            registry.bind_target_with_presentation(
                makima_agent::command::session_capabilities(),
                makima_agent::command::portable_capabilities(),
                Arc::new(
                    makima_agent::command::SessionCommandHost::new(
                        control_tx,
                        coordinator,
                        spec_providers(),
                    )
                    .with_reset_session_guidance(NEW_SESSION_GUIDANCE),
                ),
            )
        }
    }

    fn successful_checkpoint() -> Arc<
        dyn makima_storage::checkpoint::CheckpointWriter<
                makima_agent::session_coordinator::SessionCheckpoint,
            >,
    > {
        accept_all()
    }

    fn test_coordinator(
        session_id: MakimaId,
        model: &str,
    ) -> makima_agent::session_coordinator::SessionCoordinatorHandle {
        test_coordinator_with(
            session_id,
            model,
            Default::default(),
            successful_checkpoint(),
        )
    }

    fn test_coordinator_with(
        session_id: MakimaId,
        model: &str,
        catalog: makima_agent::session_coordinator::SessionOptionCatalog,
        checkpoint: Arc<
            dyn makima_storage::checkpoint::CheckpointWriter<
                    makima_agent::session_coordinator::SessionCheckpoint,
                >,
        >,
    ) -> makima_agent::session_coordinator::SessionCoordinatorHandle {
        let mut model_specs = vec![
            Arc::from(model),
            Arc::from(FAST_SPEC),
            Arc::from(OFFLINE_SPEC),
        ];
        model_specs.sort();
        model_specs.dedup();
        makima_agent::session_coordinator::SessionCoordinatorHandle::register(
            makima_agent::session_coordinator::SessionCoordinatorParams {
                session_id,
                catalog,
                definitions: makima_agent::session_coordinator::builtin_option_definitions(
                    model,
                    model_specs,
                    false,
                    false,
                    false,
                    makima_agent::ThinkingConfig::Off,
                ),
                persisted_options: Default::default(),
                model_policy: Arc::new(makima_config::ModelPolicy::default()),
                checkpoint: Arc::new(ClaimingWriter(checkpoint)),
                mailbox: makima_agent::SessionMailbox::new(session_id),
            },
        )
        .unwrap()
    }

    fn spawn_test_providers(provider: Arc<dyn Provider>) -> Arc<ProviderRegistry> {
        let (provider_id, model_id) = Model::parse_spec(SPAWN_TEST_SPEC).unwrap();
        Arc::new(ProviderRegistry::new(vec![RegisteredProvider {
            id: provider_id.into(),
            display_name: provider_id.into(),
            models: vec![ModelInfo::id_only(model_id.into())],
            backend: Arc::new(FixedProviderBackend(provider)),
            auth: AuthSpec::default(),
            docs: None,
        }]))
    }

    struct FixedProviderBackend(Arc<dyn Provider>);

    impl ProviderBackend for FixedProviderBackend {
        fn prepare(
            &self,
            model: Model,
            _: ModelInfo,
        ) -> BoxFuture<'static, Result<PreparedProvider, makima_agent::AgentError>> {
            let provider = Arc::clone(&self.0);
            Box::pin(async move { Ok(PreparedProvider { model, provider }) })
        }
        fn discover(
            &self,
            _: bool,
        ) -> Option<BoxFuture<'static, Result<Vec<ModelInfo>, makima_agent::AgentError>>> {
            None
        }
        fn login(
            &self,
            _: String,
            _: Value,
            _: Arc<dyn InteractionService>,
        ) -> BoxFuture<'static, Result<Option<String>, makima_agent::AgentError>> {
            Box::pin(async { Ok(None) })
        }
        fn auth_status(
            &self,
            _: Value,
        ) -> Option<BoxFuture<'static, Result<AuthStatus, makima_agent::AgentError>>> {
            None
        }
        fn logout(&self) -> Option<BoxFuture<'static, Result<(), makima_agent::AgentError>>> {
            None
        }
    }

    #[test_case(false; "new_session")]
    #[test_case(true; "resumed_session")]
    fn acp_install_binds_permissions_before_first_config_request(resumed: bool) {
        smol::block_on(async {
            let directory = TempDir::new().unwrap();
            let storage = StateDir::from_path(directory.path().to_path_buf());
            let (requests, received) = flume::unbounded();
            let (_release, release) = flume::unbounded();
            let providers = spawn_test_providers(Arc::new(ParkedProvider { requests, release }));
            let mut params = test_params(
                providers.model(SPAWN_TEST_SPEC).unwrap(),
                directory.path().to_path_buf(),
            );
            params.providers = providers;
            let (out_tx, _) = flume::unbounded();
            let mut server = Server {
                out_tx,
                catalog: ModelCatalog::default(),
                session: None,
                elicitation: false,
                supports_boolean: false,
                lua_event_handle: params.lua_event_handle.clone(),
                dispatch: None,
            };
            let raw = if resumed {
                let mut session =
                    StoredSession::new(SPAWN_TEST_SPEC, &directory.path().to_string_lossy());
                session.bind_root_id(AgentId::generate()).unwrap();
                session.save(&storage).unwrap();
                serde_json::json!({ "params": { "sessionId": session.id.to_string(), "cwd": directory.path(), "mcpServers": [] } })
            } else {
                serde_json::json!({ "params": { "cwd": directory.path(), "mcpServers": [] } })
            };
            if resumed {
                within(load_session(&mut server, &raw, &params), EVENT_STREAM_ENDED)
                    .await
                    .unwrap();
            } else {
                within(new_session(&mut server, &raw, &params), EVENT_STREAM_ENDED)
                    .await
                    .unwrap();
            }
            let current = server.session.as_ref().unwrap();
            let id = current.handle.session_id.id();
            assert_eq!(current.handle.permissions.persisted_yolo(), None);
            within(
                handle_set_config(
                    &mut server,
                    &serde_json::json!({
                        "params": {
                            "sessionId": id.to_string(),
                            "configId": makima_agent::session_options::YOLO_OPTION_ID,
                            "value": makima_agent::session_options::ENABLED_VALUE,
                        }
                    }),
                ),
                EVENT_STREAM_ENDED,
            )
            .await
            .unwrap();
            let current = server.session.as_ref().unwrap();
            assert!(current.handle.permissions.is_yolo());
            assert_eq!(current.handle.permissions.persisted_yolo(), Some(true));
            let saved = StoredSession::load(id, &storage).unwrap();
            assert_eq!(saved.latest_state().unwrap().scopes[&None].yolo, Some(true));
            assert!(received.is_empty());
            close_session(&mut server).await;
        });
    }

    #[derive(PartialEq, Eq)]
    enum RestoredRootScenario {
        CancelWhileQueued,
        CancelWhileParked,
        CancelWhileAsking,
        EmitImmediately,
    }

    #[test_case(RestoredRootScenario::CancelWhileQueued; "cancel_queued_root_before_provider_starts")]
    #[test_case(RestoredRootScenario::CancelWhileParked; "cancel_parked_root_before_prompt")]
    #[test_case(RestoredRootScenario::EmitImmediately; "replay_before_immediate_resumed_output")]
    #[test_case(RestoredRootScenario::CancelWhileAsking; "cancel_restored_question_before_prompt")]
    fn acp_restored_root_protocol(scenario: RestoredRootScenario) {
        const LOAD_ID: i64 = 1;
        const PROMPT_ID: i64 = 2;
        const OLD_PROMPT: &str = "previous prompt";
        const OLD_REPLY: &str = "previous reply";
        const RESTORED_PROMPT: &str = "interrupted prompt";
        const NEW_PROMPT: &str = "prompt after cancellation";
        const END_TURN: &str = "end_turn";
        const NEXT_ANSWER: &str = "next answer";
        const QUESTION_BARRIER: &str = "question pump barrier";

        smol::block_on(async {
            let directory = TempDir::new().unwrap();
            let cwd = directory.path();
            let (requests, received) = flume::unbounded();
            let (release, permits) = flume::unbounded();
            if scenario == RestoredRootScenario::EmitImmediately {
                release.send(()).unwrap();
            }
            let providers = spawn_test_providers(Arc::new(ParkedProvider {
                requests,
                release: permits,
            }));
            let mut params = test_params(providers.model(SPAWN_TEST_SPEC).unwrap(), cwd.into());
            params.providers = providers;
            let (slot_requests, slot_replies) = flume::unbounded();
            if scenario == RestoredRootScenario::CancelWhileQueued {
                params.prompt_slots = PromptSlotSource::new(move || {
                    let (reply, slots) = flume::bounded(1);
                    slot_requests.send(reply).unwrap();
                    slots
                });
            }
            let mut seed = StoredSession::new(SPAWN_TEST_SPEC, &cwd.to_string_lossy());
            seed.bind_root_id(AgentId::generate()).unwrap();
            seed.push_message(Message::user(OLD_PROMPT.into()));
            seed.push_message(Message {
                role: Role::Assistant,
                content: vec![MsgBlock::Text {
                    text: OLD_REPLY.into(),
                }],
                ..Default::default()
            });
            seed.push_message(Message::user(RESTORED_PROMPT.into()));
            let turn = TurnId::generate();
            seed.push_state(StoredStateRecord {
                seq: seed.next_state_seq(),
                agent: seed.root_agent_id(),
                generation: 0,
                change: StateChange::Admitted {
                    turn,
                    origin: StoredTurnOrigin::User,
                },
            });
            seed.save(&params.storage).unwrap();
            let session_id = seed.id.to_string();
            let (out_tx, out_rx) = flume::unbounded();
            let mut server = Server {
                out_tx,
                catalog: ModelCatalog::default(),
                session: None,
                elicitation: scenario == RestoredRootScenario::CancelWhileAsking,
                supports_boolean: false,
                lua_event_handle: params.lua_event_handle.clone(),
                dispatch: None,
            };
            within(
                handle_line(
                    &mut server,
                    &serde_json::json!({
                        "jsonrpc": "2.0",
                        "id": LOAD_ID,
                        "method": "session/load",
                        "params": { "sessionId": session_id, "cwd": cwd, "mcpServers": [] }
                    })
                    .to_string(),
                    &params,
                ),
                "ACP session/load did not return",
            )
            .await;

            match scenario {
                RestoredRootScenario::CancelWhileQueued
                | RestoredRootScenario::CancelWhileParked
                | RestoredRootScenario::CancelWhileAsking => {
                    let response = prompt_terminal(&out_rx, LOAD_ID).await;
                    assert!(response.get("result").is_some(), "{response}");
                    let root = server
                        .session
                        .as_ref()
                        .unwrap()
                        .handle
                        .initialized_root()
                        .await
                        .unwrap();
                    let turns = RootTurns {
                        actor: root.actor().unwrap(),
                        requests: received,
                        release,
                    };
                    let parked_slots = if scenario == RestoredRootScenario::CancelWhileQueued {
                        let reply = within(
                            slot_replies.recv_async(),
                            "restored turn never requested prompt slots",
                        )
                        .await
                        .unwrap();
                        let snapshot = turns.actor.snapshot();
                        assert_eq!(snapshot.status, ActorStatus::Idle);
                        assert_eq!(snapshot.queued, 1);
                        Some(reply)
                    } else {
                        assert_eq!(turns.next_text().await, RESTORED_PROMPT);
                        None
                    };
                    let questions = if scenario == RestoredRootScenario::CancelWhileAsking {
                        let events = server
                            .session
                            .as_ref()
                            .unwrap()
                            .command_root
                            .template
                            .event_tx
                            .clone();
                        let answers = Arc::clone(
                            server
                                .session
                                .as_ref()
                                .unwrap()
                                .command_root
                                .template
                                .user_response_rx
                                .as_ref()
                                .unwrap(),
                        );
                        events.send_envelope(test_question(turn)).unwrap();
                        let old_id = next_elicitation(&out_rx).await;
                        Some((events, old_id, answers, test_question(turn)))
                    } else {
                        None
                    };
                    let ticket = root.turn_ticket(turn).unwrap();
                    handle_line(
                        &mut server,
                        &serde_json::json!({
                            "jsonrpc": "2.0",
                            "method": "session/cancel",
                            "params": { "sessionId": session_id }
                        })
                        .to_string(),
                        &params,
                    )
                    .await;
                    let outcome = within(ticket.wait(), "ACP did not cancel the parked root").await;
                    assert!(
                        matches!(
                            outcome,
                            TurnOutcome::Cancelled {
                                reason: TurnCancellationReason::User,
                                ..
                            }
                        ),
                        "{outcome:?}"
                    );

                    let prompt = serde_json::json!({
                        "jsonrpc": "2.0",
                        "id": PROMPT_ID,
                        "method": "session/prompt",
                        "params": {
                            "sessionId": session_id,
                            "prompt": [{ "type": "text", "text": NEW_PROMPT }]
                        }
                    })
                    .to_string();
                    handle_line(&mut server, &prompt, &params).await;
                    let dispatch = server
                        .dispatch
                        .as_mut()
                        .expect("ACP did not admit a prompt after the restored root was cancelled");
                    let dispatched =
                        within(&mut dispatch.future, "prompt dispatch never settled").await;
                    respond_dispatch(&mut server, dispatched).await;
                    if let Some(reply) = parked_slots {
                        assert!(turns.requests.is_empty());
                        reply.send(Default::default()).unwrap();
                        within(
                            slot_replies.recv_async(),
                            "new turn never requested prompt slots",
                        )
                        .await
                        .unwrap()
                        .send(Default::default())
                        .unwrap();
                    }
                    assert_eq!(turns.next_text().await, NEW_PROMPT);
                    if let Some((events, old_id, answers, queued)) = questions {
                        events.send_envelope(queued).unwrap();
                        events
                            .send(AgentEvent::TextDelta {
                                text: QUESTION_BARRIER.into(),
                            })
                            .unwrap();
                        within(
                            async {
                                loop {
                                    let frame = out_rx.recv_async().await.unwrap();
                                    assert_ne!(
                                        frame["method"], ELICITATION_METHOD,
                                        "stale elicitation: {frame}"
                                    );
                                    if frame["params"]["update"]["content"]["text"]
                                        == QUESTION_BARRIER
                                    {
                                        break;
                                    }
                                }
                            },
                            "ACP did not drain the queued question",
                        )
                        .await;
                        events
                            .send_envelope(test_question(turns.latest().turn_id()))
                            .unwrap();
                        let next_id = next_elicitation(&out_rx).await;
                        for (id, answer) in [(old_id, OLD_REPLY), (next_id, NEXT_ANSWER)] {
                            let response = serde_json::json!({
                                "jsonrpc": "2.0", "id": id,
                                "result": { "action": "accept", "content": { "q1": answer } },
                            });
                            handle_line(&mut server, &response.to_string(), &params).await;
                        }
                        let answers = answers.lock().await;
                        assert_eq!(
                            answers.try_recv().unwrap(),
                            serde_json::json!({"answers": [[NEXT_ANSWER]]}).to_string()
                        );
                        assert!(answers.is_empty());
                    }
                    turns.release();
                    let response = prompt_terminal(&out_rx, PROMPT_ID).await;
                    assert_eq!(response["result"]["stopReason"], END_TURN, "{response}");
                }
                RestoredRootScenario::EmitImmediately => {
                    let expected = [
                        ("user_message_chunk", OLD_PROMPT),
                        ("agent_message_chunk", OLD_REPLY),
                        ("user_message_chunk", RESTORED_PROMPT),
                    ];
                    let mut replayed = 0;
                    let mut loaded = false;
                    let mut resumed = false;
                    within(
                        async {
                            while !loaded || !resumed {
                                let frame = out_rx.recv_async().await.unwrap();
                                if frame["id"] == LOAD_ID {
                                    assert!(frame.get("result").is_some(), "{frame}");
                                    loaded = true;
                                    continue;
                                }
                                let update = &frame["params"]["update"];
                                match update["content"]["text"].as_str() {
                                    Some(OLD_PROMPT | OLD_REPLY | RESTORED_PROMPT) => {
                                        let (kind, text) = expected[replayed];
                                        assert_eq!(update["sessionUpdate"], kind);
                                        assert_eq!(update["content"]["text"], text);
                                        assert_eq!(frame["params"]["sessionId"], session_id);
                                        replayed += 1;
                                    }
                                    Some(PARKED_THOUGHT | PARKED_REPLY) => {
                                        assert_eq!(
                                            replayed,
                                            expected.len(),
                                            "live output before replay: {frame}"
                                        );
                                        assert_eq!(frame["params"]["sessionId"], session_id);
                                        resumed |= update["content"]["text"] == PARKED_REPLY;
                                    }
                                    _ => {}
                                }
                            }
                        },
                        "ACP did not send load response and resumed output",
                    )
                    .await;
                }
            }
            close_session(&mut server).await;
        });
    }

    #[test]
    fn acp_load_retains_opted_out_recovery_results_without_reissuing_work() {
        use makima_agent::session_recovery::RecoveryCandidate;

        const CONTINUATION_DISABLED: &str = "continuation_disabled: MAKIMA_CONTINUE_ALL_AGENTS=0";
        smol::block_on(async {
            let tmp = TempDir::new().unwrap();
            let storage = StateDir::from_path(tmp.path().to_path_buf());
            let (requests, received) = flume::unbounded();
            let (_release, release) = flume::unbounded();
            let providers = spawn_test_providers(Arc::new(ParkedProvider { requests, release }));
            let mut session = StoredSession::new(SPAWN_TEST_SPEC, &tmp.path().to_string_lossy());
            let root_id = AgentId::generate();
            let child_id = AgentId::generate();
            let root_turn = TurnId::generate();
            let child_turn = TurnId::generate();
            session.bind_root_id(root_id).unwrap();
            let mut record = session.agents()[&root_id].clone();
            record.id = child_id;
            record.parent = Some(root_id);
            record.depth = 1;
            record.transcript_key = PARKED_THOUGHT.into();
            record.created_seq = 2;
            record.updated_seq = 2;
            session.push_agent_revision(2, record);
            session.set_subagent_messages(
                PARKED_THOUGHT.into(),
                vec![Message::user(PARKED_THOUGHT.into())],
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
            session.meta.input_draft = Some(PARKED_THOUGHT.into());
            session.meta.queued_messages.push(PARKED_REPLY.into());
            session.save(&storage).unwrap();
            let writer = SessionWriter::open_session(
                &storage.ensure_subdir(SESSIONS_DIR).unwrap(),
                &session,
            )
            .unwrap();
            RecoveryCandidate::prepare_with_continuation(session.clone(), false)
                .unwrap()
                .publish(
                    &writer,
                    Default::default(),
                    Arc::new(|publication| {
                        assert!(publication.try_publish());
                        Ok(())
                    }),
                )
                .await
                .unwrap();
            drop(writer);
            let path = storage
                .path()
                .join(SESSIONS_DIR)
                .join(format!("{}.jsonl", session.id));
            let before_recovery = std::fs::read(&path).unwrap();
            let mut params = test_params(
                providers.model(SPAWN_TEST_SPEC).unwrap(),
                tmp.path().to_path_buf(),
            );
            params.providers = providers;
            let (out_tx, out_rx) = flume::unbounded();
            let mut server = Server {
                out_tx,
                catalog: ModelCatalog::default(),
                session: None,
                elicitation: false,
                supports_boolean: false,
                lua_event_handle: params.lua_event_handle.clone(),
                dispatch: None,
            };
            let raw = serde_json::json!({ "params": { "sessionId": session.id.to_string(), "cwd": tmp.path(), "mcpServers": [] } });
            let mut first_results = None;
            for _ in 0..2 {
                within(load_session(&mut server, &raw, &params), EVENT_STREAM_ENDED)
                    .await
                    .unwrap();
                let current = server.session.as_ref().unwrap();
                let root = current.handle.initialized_root().await.unwrap();
                assert_eq!(root.id(), root_id);
                let child = current.handle.manager.lookup(child_id).unwrap();
                assert!(!child.is_live());
                assert_eq!(child.snapshot().unwrap().residency, AgentResidency::Dormant);
                for (agent, turn) in [(&root, root_turn), (&child, child_turn)] {
                    let result = agent.turn_ticket(turn).unwrap().wait_result().await;
                    if agent.id() == root_id {
                        assert!(matches!(
                            result.outcome,
                            TurnOutcome::Cancelled {
                                reason: TurnCancellationReason::Interrupted,
                                ..
                            }
                        ));
                    } else {
                        assert!(
                            matches!(result.outcome, TurnOutcome::Failed { ref failure, .. }
                            if failure.diagnostic == CONTINUATION_DISABLED && !failure.retryable)
                        );
                    }
                }
                let probe = root.actor().unwrap().runtime_barrier().await.unwrap();
                assert!(probe.is_quiescent());
                assert!(received.is_empty());
                let loaded = StoredSession::load(session.id, &storage).unwrap();
                assert!(std::fs::read(&path).unwrap().starts_with(&before_recovery));
                assert!(loaded.meta.queued_messages.is_empty());
                assert_eq!(
                    loaded.meta.input_draft.as_deref(),
                    Some(format!("{PARKED_THOUGHT}\n\n{PARKED_REPLY}").as_str())
                );
                let terminal: Vec<_> = loaded
                    .state()
                    .iter()
                    .filter(|record| matches!(record.change, StateChange::Turn { .. }))
                    .cloned()
                    .collect();
                assert_eq!(terminal.len(), 2);
                for record in &terminal {
                    let StateChange::Turn { result, .. } = &record.change else {
                        unreachable!()
                    };
                    if result.outcome.agent_id == root_id {
                        assert!(matches!(
                            result.outcome.status,
                            StoredTurnStatus::Cancelled {
                                reason: StoredCancellationReason::Interrupted
                            }
                        ));
                    } else {
                        assert!(
                            matches!(&result.outcome.status, StoredTurnStatus::Failed { failure }
                            if failure.diagnostic == CONTINUATION_DISABLED && !failure.retryable)
                        );
                    }
                }
                if let Some(first) = &first_results {
                    assert_eq!(first, &terminal);
                } else {
                    first_results = Some(terminal);
                }
                assert!(
                    !out_rx
                        .try_iter()
                        .any(|event| event["params"]["update"]["sessionUpdate"] == "tool_call")
                );
                close_session(&mut server).await;
            }
        });
    }

    #[test]
    fn acp_reload_active_session_keeps_state_published_during_shutdown() {
        smol::block_on(async {
            let directory = TempDir::new().unwrap();
            let providers = spec_providers();
            let mut params = test_params(
                providers.model(SPAWN_TEST_SPEC).unwrap(),
                directory.path().to_path_buf(),
            );
            params.providers = providers;
            let mut seed = StoredSession::new(SPAWN_TEST_SPEC, &directory.path().to_string_lossy());
            let root_id = AgentId::generate();
            seed.bind_root_id(root_id).unwrap();
            seed.save(&params.storage).unwrap();
            let (out_tx, out_rx) = flume::unbounded();
            let mut server = Server {
                out_tx,
                catalog: ModelCatalog::default(),
                session: None,
                elicitation: false,
                supports_boolean: false,
                lua_event_handle: params.lua_event_handle.clone(),
                dispatch: None,
            };
            let raw = serde_json::json!({ "params": { "sessionId": seed.id.to_string(), "cwd": directory.path(), "mcpServers": [] } });
            within(load_session(&mut server, &raw, &params), EVENT_STREAM_ENDED)
                .await
                .unwrap();
            let current = server.session.as_ref().unwrap();
            current.handle.manager.wait_settled().await;
            current.checkpoint.as_ref().unwrap().drain().await.unwrap();
            let journal = current.handle.manager.journal();
            let publication = journal.reserve_publication().await;
            assert!(journal.try_publication_read().is_none());
            let before = StoredSession::load(seed.id, &params.storage).unwrap();
            let mut config = before.latest_state().unwrap().scopes[&Some(root_id)]
                .config
                .clone()
                .unwrap();
            config.workflow = !config.workflow;
            config.options.insert(
                makima_agent::session_options::WORKFLOW_OPTION_ID.into(),
                if config.workflow {
                    makima_agent::session_options::ENABLED_VALUE
                } else {
                    makima_agent::session_options::DISABLED_VALUE
                }
                .into(),
            );
            let mut history = before.history_snapshot();
            Arc::make_mut(&mut history.messages).extend([
                Message::user(PARKED_THOUGHT.into()),
                Message {
                    role: Role::Assistant,
                    content: vec![MsgBlock::Text {
                        text: PARKED_REPLY.into(),
                    }],
                    ..Default::default()
                },
            ]);
            Arc::make_mut(&mut history.ids).extend([MakimaId::generate(), MakimaId::generate()]);
            let turn_id = TurnId::generate();
            let result = TurnResult {
                outcome: TurnOutcome::completed(
                    root_id,
                    turn_id,
                    TokenUsage::default(),
                    1,
                    DoneReason::EndTurn,
                ),
                text: PARKED_REPLY.into(),
                output: Vec::new(),
                provenance: Default::default(),
                duration_ms: 0,
            }
            .to_stored(seed.id, None)
            .unwrap();
            drop(out_rx.drain());
            let mut loading = Box::pin(load_session(&mut server, &raw, &params));
            assert!(poll_once(loading.as_mut()).await.is_none());
            assert_eq!(
                serde_json::to_value(
                    StoredSession::load(seed.id, &params.storage)
                        .unwrap()
                        .messages()
                )
                .unwrap(),
                serde_json::to_value(before.messages()).unwrap()
            );
            // The publication guard stalls shutdown, not an ordinary journal commit's append.
            journal
                .commit(JournalCommit {
                    changes: vec![
                        JournalChange {
                            agent: Some(root_id),
                            generation: 0,
                            change: StateChange::Config(config.clone()),
                        },
                        JournalChange {
                            agent: Some(root_id),
                            generation: 0,
                            change: StateChange::Turn {
                                turn: turn_id,
                                result: result.clone(),
                            },
                        },
                    ],
                    histories: BTreeMap::from([(root_id, history.clone())]),
                    managed_histories: iter::once(root_id).collect(),
                    ..Default::default()
                })
                .unwrap();
            drop(publication);
            within(loading, EVENT_STREAM_ENDED).await.unwrap();
            let saved = StoredSession::load(seed.id, &params.storage).unwrap();
            assert_eq!(saved.root_agent_id(), Some(root_id));
            assert_eq!(
                serde_json::to_value(saved.messages()).unwrap(),
                serde_json::to_value(history.messages.as_ref()).unwrap()
            );
            let view = saved.latest_state().unwrap();
            let scope = &view.scopes[&Some(root_id)];
            assert_eq!(scope.config.as_ref(), Some(&config));
            assert_eq!(scope.results.back(), Some(&(turn_id, result)));
            assert_eq!(
                scope
                    .results
                    .iter()
                    .filter(|(id, _)| *id == turn_id)
                    .count(),
                1
            );
            let root = server
                .session
                .as_ref()
                .unwrap()
                .handle
                .initialized_root()
                .await
                .unwrap();
            assert_eq!(root.id(), root_id);
            assert_eq!(
                serde_json::to_value(root.actor().unwrap().committed_history().as_ref()).unwrap(),
                serde_json::to_value(history.messages.as_ref()).unwrap()
            );
            assert_eq!(
                root.actor().unwrap().effective_config().unwrap().workflow,
                config.workflow
            );
            assert_eq!(
                root.turn_ticket(turn_id).unwrap().wait_result().await.text,
                PARKED_REPLY
            );
            assert!(
                out_rx
                    .try_iter()
                    .any(|event| { event["params"]["update"]["content"]["text"] == PARKED_REPLY })
            );
            close_session(&mut server).await;
        });
    }

    #[test]
    fn acp_restoration_preserves_compacted_history() {
        let tmp = TempDir::new().unwrap();
        let storage = StateDir::from_path(tmp.path().to_path_buf());
        let mut session: Session<Message, TokenUsage, makima_agent::ToolOutput> =
            Session::new(OFFLINE_SPEC, "/project");
        session.meta.compacted = true;
        session.save(&storage).unwrap();
        let restored = open_stored_session_for_test(&storage, session.id).unwrap();
        assert!(restored.history_snapshot().compacted);
        assert_eq!(
            serde_json::to_value(restored.history_snapshot().messages.as_ref()).unwrap(),
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
            dyn makima_storage::checkpoint::CheckpointWriter<
                    makima_agent::session_coordinator::SessionCheckpoint,
                >,
        >,
    ) -> (Server, Receiver<String>, Receiver<Value>, RootTurns) {
        let (answer_tx, answer_rx) = flume::unbounded();
        let (out_tx, out_rx) = flume::unbounded();
        let (requests, requests_rx) = flume::unbounded();
        let (release, released) = flume::unbounded();
        let provider: Arc<dyn makima_providers::provider::Provider> = Arc::new(ParkedProvider {
            requests,
            release: released,
        });
        let prepared = Arc::clone(&provider);
        let session_id = MakimaId::generate();
        let (binding, root_target) = makima_lua::orchestration::root_binding();
        let mut handle = headless::spawn_interactive_with_binding(
            InteractiveParams {
                model: spec_providers().model(OFFLINE_SPEC).unwrap(),
                config: Default::default(),
                permissions_config: Default::default(),
                timeouts: Default::default(),
                prompt_slots: Default::default(),
                excluded_tools: Vec::new(),
                mcp_handle: None,
                initial_wd: PathBuf::from("/project"),
                session_id: Some(SessionRef::from(session_id)),
                root_id: None,
                journal: None,
                modes: Arc::new(makima_agent::ModeRegistry::builtin()),
                initial_history: Vec::new(),
                yolo: false,
                system_prompt_override: Some(String::new()),
                append_system_prompt: None,
                defaults: Default::default(),
                model_policy: Arc::default(),
                providers: spec_providers(),
                question_mode: QuestionMode::Headless,
                plugin_rules: Arc::default(),
                project_config: ProjectConfig::for_project(Path::new("/project")),
                local_tools: Default::default(),
                state_dir: None,
                answers: true,
            },
            Box::new(move |model| {
                Box::pin(async move {
                    Ok(makima_agent::actor::PreparedModel {
                        model,
                        provider: prepared,
                    })
                })
            }),
            None,
            Some(binding),
        );
        let root = smol::block_on(handle.initialized_root()).unwrap();
        let command_root = root_target.recv().unwrap();
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
            .bind_permissions(Arc::clone(&handle.permissions))
            .unwrap();
        coordinator
            .bind_actor(
                root.clone(),
                Arc::new(move |spec| {
                    let provider = Arc::clone(&provider);
                    let model = spec_providers()
                        .model(&spec)
                        .map_err(makima_agent::config_change::unresolved_model);
                    Box::pin(async move {
                        Ok(makima_agent::actor::PreparedModel {
                            model: model?,
                            provider,
                        })
                    })
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
        let command_registry = test_registry(&[]);
        let command_target = command_registry.bind_target_with_presentation(
            makima_agent::command::session_capabilities(),
            makima_agent::command::portable_capabilities(),
            Arc::new(
                makima_agent::command::SessionCommandHost::new(
                    handle.control_tx.clone(),
                    coordinator.clone(),
                    spec_providers(),
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
                    question_root: Some(command_root.target.clone()),
                    ..Default::default()
                })),
                command_registry,
                command_target,
                command_root,
                command_projection_task: smol::spawn(async {}),
                event_pump: EventPump::idle(),
                option_projection_task: smol::spawn(async {}),
                lock: None,
            }),
            elicitation: false,
            supports_boolean: false,
            lua_event_handle: makima_lua::EventHandle::disconnected_for_test(),
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
            let params = test_params(spec_providers().model(OFFLINE_SPEC).unwrap(), state_path);
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
                    root_id: None,
                    journal: None,
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
                    recovered_writer: None,
                    root_target,
                    mcp: None,
                    current_model: OFFLINE_SPEC.to_owned(),
                    initial_cost: None,
                    cwd: cwd.path().to_path_buf(),
                    defaults: SessionDefaults::default(),
                    persisted_options: &persisted_options,
                    replay_history: &[],
                    continuation_count: 0,
                    continuing_root: BTreeSet::new(),
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
    fn startup_checkpoint_holds_lock_until_close() {
        let (entered, entering) = flume::bounded(1);
        let (release, released) = flume::bounded(1);
        let checkpoint = Arc::new(
            move |request: makima_storage::checkpoint::CheckpointRequest<
                makima_agent::session_coordinator::SessionCheckpoint,
            >| {
                let entered = entered.clone();
                let released = released.clone();
                Box::pin(async move {
                    entered.send_async(()).await.unwrap();
                    released.recv_async().await.unwrap();
                    Err(makima_storage::checkpoint::CheckpointError::Save {
                        session_id: request.session_id,
                        message: Arc::from(SAVE_FAILED),
                    })
                }) as makima_storage::checkpoint::CheckpointFuture
            },
        );
        let (mut srv, _, _, turns) = server_awaiting_answer_with_checkpoint(checkpoint);
        smol::block_on(async {
            let tmp = TempDir::new().unwrap();
            let storage = StateDir::from_path(tmp.path().to_path_buf());
            let sessions_dir = storage.ensure_subdir(SESSIONS_DIR).unwrap();
            let state = srv.session.as_mut().unwrap();
            let id = state.handle.session_id.id();
            let input = state.handle.input_tx.clone();
            state.lock = Some(start_session_lock_in(sessions_dir.clone(), id).unwrap());
            let mut seed = StoredSession::new(OFFLINE_SPEC, "/project");
            seed.id = id;
            state.checkpoint = Some(Arc::new(
                SessionWriter::open_session(&sessions_dir, &seed).unwrap(),
            ));
            let (result, ()) = smol::future::zip(
                checkpoint_root_mode(&state.handle, state.coordinator.as_ref().unwrap()),
                async {
                    entering.recv_async().await.unwrap();
                    assert!(session_lock::claim(&sessions_dir, &id).unwrap().is_none());
                    release.send_async(()).await.unwrap();
                },
            )
            .await;
            assert!(result.unwrap_err().contains(SAVE_FAILED));
            close_session(&mut srv).await;
            assert!(srv.session.is_none());
            assert!(SessionCoordinatorHandle::resolve(id).is_err());
            assert!(
                input
                    .admit(AgentInput::new(String::new(), Vec::new()))
                    .await
                    .is_err()
            );
            assert!(session_lock::claim(&sessions_dir, &id).unwrap().is_some());
            turns.assert_untouched();
        });
    }

    #[test]
    fn post_registration_lock_failure_rolls_back_and_same_id_retries() {
        smol::block_on(async {
            let cwd = TempDir::new().unwrap();
            let params = test_params(
                spec_providers().model(OFFLINE_SPEC).unwrap(),
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
            let session_id = SessionRef::from(MakimaId::generate());
            let persisted_options = BTreeMap::new();

            let (first, root_target) = spawn_session(
                &params,
                SpawnSession {
                    model: params.model.clone(),
                    cwd: cwd.path().to_path_buf(),
                    session_id: Some(session_id.clone()),
                    root_id: None,
                    journal: None,
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
                    recovered_writer: None,
                    root_target,
                    mcp: None,
                    current_model: OFFLINE_SPEC.to_owned(),
                    initial_cost: None,
                    cwd: cwd.path().to_path_buf(),
                    defaults: SessionDefaults::default(),
                    persisted_options: &persisted_options,
                    replay_history: &[],
                    continuation_count: 0,
                    continuing_root: BTreeSet::new(),
                },
                |_| Err(session_lock::OPEN_ELSEWHERE_MSG.to_owned()),
            )
            .await
            .unwrap_err();
            assert_eq!(error, session_lock::OPEN_ELSEWHERE_MSG);
            assert!(srv.session.is_none());
            assert!(
                makima_agent::session_coordinator::SessionCoordinatorHandle::resolve(
                    session_id.id()
                )
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
                        makima_agent::session_options::YOLO_OPTION_ID,
                        makima_agent::session_options::ENABLED_VALUE
                    )
                    .await
                    .is_err()
            );
            replacement.close().await.unwrap();
        });
    }

    #[test]
    fn close_persists_usage_from_queued_turn_events() {
        smol::block_on(async {
            let (mut srv, ..) = server_awaiting_answer();
            let directory = TempDir::new().unwrap();
            let storage = StateDir::from_path(directory.path().to_path_buf());
            let state = srv.session.as_mut().unwrap();
            let session_id = state.handle.session_id.clone();
            let mut seed = StoredSession::new(OFFLINE_SPEC, "/project");
            seed.id = session_id.id();
            let sessions_dir = storage.ensure_subdir(SESSIONS_DIR).unwrap();
            let writer = Arc::new(SessionWriter::open_session(&sessions_dir, &seed).unwrap());
            let observer = test_observer();
            observer.persist_usage(
                writer.as_ref().clone(),
                TokenUsage::default(),
                HashMap::new(),
            );
            let (events, event_rx) = flume::unbounded();
            let usage = TokenUsage {
                output: 17,
                ..Default::default()
            };
            events
                .send(subagent_activity(AgentEvent::TurnComplete(Box::new(
                    makima_agent::TurnCompleteEvent {
                        message: Message::default(),
                        usage,
                        model: OFFLINE_SPEC.into(),
                        cost: None,
                        context_size: None,
                        context_window: 1,
                    },
                ))))
                .unwrap();
            state.checkpoint = Some(writer);
            state.event_pump = start_event_pump(
                event_rx,
                session_id.clone(),
                srv.out_tx.clone(),
                Arc::clone(&state.pending),
                false,
                state.handle.answer_tx.clone(),
                state.coordinator.as_ref().unwrap().read(),
                None,
                PUMP_TRUSTED,
                None,
                observer,
            );

            close_session(&mut srv).await;

            let saved = StoredSession::load(session_id.id(), &storage).unwrap();
            assert_eq!(saved.token_usage, usage);
            assert_eq!(saved.usage_by_model()[OFFLINE_SPEC].output, usage.output);
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
                .set_option(makima_agent::session_options::MODEL_OPTION_ID, FAST_SPEC)
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

    #[test_case(false, serde_json::json!(makima_agent::session_options::ENABLED_VALUE) ; "legacy_value_id")]
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
                "configId": makima_agent::session_options::YOLO_OPTION_ID,
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
            makima.api.register_session_option({
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

        let host = makima_lua::PluginHost::new(Arc::new(makima_agent::tools::ToolRegistry::new()))
            .unwrap();
        host.load_source("choice", SOURCE).unwrap();
        let lua = host.event_handle();
        let checkpoints = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let checkpoint: Arc<
            dyn makima_storage::checkpoint::CheckpointWriter<
                    makima_agent::session_coordinator::SessionCheckpoint,
                >,
        > = {
            let checkpoints = Arc::clone(&checkpoints);
            Arc::new(
                move |request: makima_storage::checkpoint::CheckpointRequest<_>| {
                    checkpoints.fetch_add(1, Ordering::Relaxed);
                    Box::pin(async move {
                        Ok(makima_storage::checkpoint::CheckpointAck::for_request(
                            &request,
                        ))
                    }) as makima_storage::checkpoint::CheckpointFuture
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
                    "configId": makima_agent::session_options::YOLO_OPTION_ID,
                    "value": makima_agent::session_options::ENABLED_VALUE,
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
                if option.current_value.to_string() == makima_agent::session_options::ENABLED_VALUE
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
                    option.definition.id.as_ref() == makima_agent::session_options::YOLO_OPTION_ID
                })
                .unwrap()
                .current_value
                .as_ref(),
            makima_agent::session_options::ENABLED_VALUE
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
                .set_option(makima_agent::session_options::MODEL_OPTION_ID, FAST_SPEC)
                .await
                .unwrap();
            coordinator
                .set_option(
                    makima_agent::session_options::FAST_OPTION_ID,
                    makima_agent::session_options::ENABLED_VALUE,
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
            makima_agent::session_options::DISABLED_VALUE
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

    fn configure_test_paths() {
        makima_storage::paths::init_shared_for_test();
    }

    fn test_providers(models: Arc<Mutex<Vec<ModelInfo>>>) -> Arc<ProviderRegistry> {
        makima_lua::test_support::offline_registry(DISCOVERY_PROVIDER, models)
    }

    fn test_model(spec: &str) -> Model {
        test_providers(Arc::default()).model(spec).unwrap()
    }

    #[test]
    fn closing_a_session_releases_its_orchestration() {
        configure_test_paths();
        smol::block_on(async {
            let cwd = TempDir::new().unwrap();
            let host =
                makima_lua::PluginHost::new(Arc::new(makima_agent::tools::ToolRegistry::new()))
                    .unwrap();
            let mut params = test_params(test_model(SPAWN_TEST_SPEC), cwd.path().to_path_buf());
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

    #[test_case(StateScope::Session; "session_slot")]
    #[test_case(StateScope::Agent; "root_slot")]
    fn new_session_replaces_state_scope_and_preserves_previous_values(scope: StateScope) {
        configure_test_paths();
        smol::block_on(async {
            let cwd = TempDir::new().unwrap();
            let params = test_params(test_model(SPAWN_TEST_SPEC), cwd.path().to_path_buf());
            let (mut srv, _, _out_rx, _) = server_awaiting_answer();
            let request = serde_json::json!({
                "params": { "cwd": cwd.path(), "mcpServers": [] }
            });
            new_session(&mut srv, &request, &params).await.unwrap();
            let previous = srv.session.as_ref().unwrap();
            let previous_id = previous.handle.session_id.id();
            let previous_root = previous.handle.initialized_root().await.unwrap();
            let previous_journal = previous.handle.manager.journal();
            let spec = StateSlotSpec {
                key: RESET_STATE_KEY.into(),
                scope,
                version: 1,
            };
            let slot = previous_journal
                .register_state::<String>(spec.clone())
                .unwrap();
            let access = previous_root.state_access();
            let value = RESET_STATE_VALUE.to_owned();
            slot.set(&access, value.clone(), Some(StateRevision(0)))
                .unwrap();
            previous.checkpoint.as_ref().unwrap().drain().await.unwrap();

            new_session(&mut srv, &request, &params).await.unwrap();

            let replacement = srv.session.as_ref().unwrap();
            let replacement_root = replacement.handle.initialized_root().await.unwrap();
            assert_ne!(replacement.handle.session_id.id(), previous_id);
            assert_ne!(replacement_root.id(), previous_root.id());
            let replacement_slot = replacement
                .handle
                .manager
                .journal()
                .register_state::<String>(spec)
                .unwrap();
            let fresh = replacement_slot
                .get(&replacement_root.state_access())
                .unwrap();
            assert_eq!(fresh.value, None);
            assert_eq!(fresh.revision, StateRevision(0));
            assert!(matches!(slot.get(&access), Err(StateError::StaleAccess)));
            assert!(matches!(
                slot.set(&access, value, None),
                Err(StateError::StaleAccess)
            ));
            assert!(matches!(
                slot.get(&previous_root.state_access()),
                Err(StateError::StaleAccess)
            ));
            let previous_session =
                Session::<Message, TokenUsage, ToolOutput>::load(previous_id, &params.storage)
                    .unwrap();
            let previous_state = previous_session.latest_state().unwrap();
            let agent = match scope {
                StateScope::Session => None,
                StateScope::Agent => Some(previous_root.id()),
            };
            assert_eq!(
                previous_state.scopes[&agent].plugins[RESET_STATE_KEY].value,
                Some(serde_json::json!(RESET_STATE_VALUE)),
            );
            close_session(&mut srv).await;
        });
    }

    /// A malformed permissions file fails session/new before the live
    /// session is closed, so the client keeps the session it had.
    #[test]
    fn broken_permissions_keep_the_live_session() {
        configure_test_paths();
        smol::block_on(async {
            let (mut srv, _, _, turns) = server_awaiting_answer();
            let live = srv.session.as_ref().unwrap().handle.session_id.to_string();
            let cwd = TempDir::new().unwrap();
            let project = cwd.path().join(makima_config::PROJECT_DIR);
            std::fs::create_dir_all(&project).unwrap();
            std::fs::write(project.join("permissions.toml"), "[bash]\ndeyn = [\"x\"]\n").unwrap();
            let params = test_params(test_model(SPAWN_TEST_SPEC), cwd.path().to_path_buf());
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
        configure_test_paths();
        smol::block_on(async {
            let cwd = TempDir::new().unwrap();
            let params = test_params(test_model(SPAWN_TEST_SPEC), cwd.path().to_path_buf());
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
                assert!(plan.starts_with(cwd.path().join(makima_storage::plans::PLANS_DIR)));
                assert_eq!(
                    config.mode_def,
                    params
                        .lua_event_handle
                        .mode_registry()
                        .get(&makima_agent::ModeId::Plan)
                );
            } else {
                assert_eq!(config.mode, makima_agent::AgentMode::Build);
            }
            close_session(&mut srv).await;
        });
    }

    #[test]
    fn acp_rejects_stale_session_ids_for_prompt_set_option_mode_and_cancel() {
        configure_test_paths();
        let (mut srv, answer_rx, _, turns) = server_awaiting_answer();
        let stale = MakimaId::generate().to_string();
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
                    "configId": makima_agent::session_options::YOLO_OPTION_ID,
                    "value": makima_agent::session_options::ENABLED_VALUE
                }
            }),
        ))
        .unwrap_err();
        assert_eq!(config_error.code, AcpError::resource_not_found(None).code);

        let params = test_params(test_model(SPAWN_TEST_SPEC), PathBuf::new());
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
                makima_storage::paths::home(),
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
                        tool: makima_config::ToolKey::Native(Arc::from("bash")),
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
                        tool: makima_config::ToolKey::Native(Arc::from("bash")),
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
            makima_agent::ModeId::Build.key(),
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
            let text = "a".repeat(makima_agent::actor::MAX_PENDING_INSTRUCTION_BYTES + 1);
            let error = handle_prompt(
                &mut srv,
                &prompt_request(&session_id, &text, false),
                &RequestId::Number(41),
            )
            .await
            .unwrap_err();
            assert_eq!(
                error.message,
                makima_agent::ActorError::InstructionOverflow.to_string()
            );
        });
    }

    #[test]
    fn cancelled_prompt_rejects_follow_up_and_processes_new_session() {
        smol::block_on(async {
            let (mut srv, _, out_rx, turns) = server_awaiting_answer();
            let old_session_id = srv.session.as_ref().unwrap().handle.session_id.to_string();
            let params = test_params(
                spec_providers().model(OFFLINE_SPEC).unwrap(),
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
                spec_providers().model(OFFLINE_SPEC).unwrap(),
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
                    makima_storage::paths::home(),
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
                    AgentEvent::TurnOutcome(makima_agent::TurnOutcome::cancelled(
                        makima_agent::AgentId::generate(),
                        TurnId::generate(),
                        TokenUsage::default(),
                        1,
                        makima_agent::TurnCancellationReason::User,
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
                dyn makima_storage::checkpoint::CheckpointWriter<
                        makima_agent::session_coordinator::SessionCheckpoint,
                    >,
            > = Arc::new(
                move |request: makima_storage::checkpoint::CheckpointRequest<_>| {
                    let checkpoint_started_tx = checkpoint_started_tx.clone();
                    let checkpoint_release_rx = checkpoint_release_rx.clone();
                    let gate = gated.swap(false, Ordering::Relaxed);
                    Box::pin(async move {
                        if gate {
                            checkpoint_started_tx.send_async(()).await.unwrap();
                            checkpoint_release_rx.recv_async().await.unwrap();
                        }
                        Ok(makima_storage::checkpoint::CheckpointAck::for_request(
                            &request,
                        ))
                    }) as makima_storage::checkpoint::CheckpointFuture
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
                    .persist_history(makima_agent::agent::HistorySnapshot::new(vec![
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

    #[test_case(serde_json::json!({"action": "accept", "content": {"q1": "a", "q2": ["x", "y"]}}), r#"{"answers":[["a"],["x","y"]]}"#; "accepted")]
    #[test_case(serde_json::json!({"nonsense": true}), ELICITATION_DISMISSED; "malformed")]
    fn decodes_elicitation_answer(result: Value, expected: &str) {
        assert_eq!(
            elicitation_answer(&serde_json::json!({"result": result})),
            expected
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
        let session_id = SessionRef::from(MakimaId::generate());
        let coordinator = test_coordinator(session_id.id(), OFFLINE_SPEC);
        let pump = start_event_pump(
            event_rx,
            session_id,
            out_tx,
            Arc::default(),
            true,
            answer_tx,
            coordinator.read(),
            makima_storage::paths::home(),
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

    fn question_identity(turn_id: TurnId) -> ActorWorkIdentity {
        const QUESTION_RUN: u64 = 1;
        ActorWorkIdentity::new(
            AgentId::generate(),
            ActorWorkKind::Turn {
                operation_id: QUESTION_RUN,
                turn_id,
            },
            QUESTION_RUN,
        )
    }

    fn test_question(turn_id: TurnId) -> Envelope {
        const QUESTION_ID: &str = "question-tool";
        const QUESTION_TEXT: &str = "Continue?";
        Envelope {
            event: AgentEvent::Question {
                id: QUESTION_ID.into(),
                questions: serde_json::json!([{ "question": QUESTION_TEXT }]),
            },
            subagent: None,
            identity: Some(question_identity(turn_id)),
        }
    }

    async fn next_elicitation(out: &Receiver<Value>) -> i64 {
        within(
            async {
                loop {
                    let message = out.recv_async().await.unwrap();
                    if message["method"] == ELICITATION_METHOD {
                        return message["id"].as_i64().unwrap();
                    }
                }
            },
            "ACP did not send an elicitation",
        )
        .await
    }

    #[test_case(())]
    fn malformed_question_in_elicitation_mode_unblocks_the_tool(_: ()) {
        smol::block_on(async {
            const PROMPT: &str = "parked prompt";
            let (mut srv, answers, out_rx, turns) = server_awaiting_answer();
            let session_id = srv.session.as_ref().unwrap().handle.session_id.to_string();
            handle_prompt(
                &mut srv,
                &prompt_request(&session_id, PROMPT, false),
                &RequestId::Number(ANSWERED_ID),
            )
            .await
            .unwrap();
            turns.next().await;
            let session = srv.session.as_ref().unwrap();
            let (events, event_rx) = flume::unbounded();
            let pump = start_event_pump(
                event_rx,
                session.handle.session_id.clone(),
                srv.out_tx.clone(),
                Arc::clone(&session.pending),
                true,
                session.handle.answer_tx.clone(),
                session.coordinator.as_ref().unwrap().read(),
                None,
                PUMP_TRUSTED,
                None,
                test_observer(),
            );
            let mut question = test_question(turns.latest().turn_id());
            if let AgentEvent::Question { questions, .. } = &mut question.event {
                *questions = Value::Null;
            }
            events.send(question).unwrap();
            assert_eq!(
                within(answers.recv_async(), "question was not dismissed")
                    .await
                    .unwrap(),
                ELICITATION_DISMISSED
            );
            assert!(out_rx.is_empty());
            close_session(&mut srv).await;
            pump.finish().await;
        });
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
            let session_id = SessionRef::from(MakimaId::generate());
            let coordinator = test_coordinator(session_id.id(), OFFLINE_SPEC);
            start_event_pump(
                event_rx,
                session_id,
                out_tx,
                Arc::clone(&pending),
                true,
                answer_tx,
                coordinator.read(),
                makima_storage::paths::home(),
                PUMP_TRUSTED,
                None,
                test_observer(),
            )
            .detach();

            let completed = |run_id| Envelope {
                event: AgentEvent::TurnOutcome(makima_agent::TurnOutcome::completed(
                    makima_agent::AgentId::generate(),
                    TurnId::generate(),
                    TokenUsage::default(),
                    1,
                    makima_agent::DoneReason::EndTurn,
                )),
                subagent: None,
                identity: Some(makima_agent::ActorWorkIdentity::new(
                    makima_agent::AgentId::generate(),
                    makima_agent::ActorWorkKind::Control {
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
                    identity: Some(makima_agent::ActorWorkIdentity::new(
                        makima_agent::AgentId::generate(),
                        makima_agent::ActorWorkKind::Control {
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
                    AgentEvent::TurnOutcome(makima_agent::TurnOutcome::completed(
                        makima_agent::AgentId::generate(),
                        first.turn_id(),
                        TokenUsage::default(),
                        1,
                        makima_agent::DoneReason::EndTurn,
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
            let session_id = SessionRef::from(MakimaId::generate());
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
                    AgentEvent::TurnOutcome(makima_agent::TurnOutcome::cancelled(
                        makima_agent::AgentId::generate(),
                        TurnId::generate(),
                        TokenUsage::default(),
                        1,
                        makima_agent::TurnCancellationReason::User,
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
        let session_id = SessionRef::from(MakimaId::generate());
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
            makima_storage::paths::home(),
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
            AgentEvent::ToolStart(Box::new(makima_agent::ToolStartEvent {
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
                tool: makima_config::ToolKey::native("bash"),
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
        let session_id = SessionRef::from(MakimaId::generate());
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
            AgentEvent::TurnComplete(Box::new(makima_agent::TurnCompleteEvent {
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
                managed_test_session(&mut srv, true, true, None, None, successful_checkpoint())
                    .await;
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
        let session_id = SessionRef::from(MakimaId::generate());
        let coordinator = test_coordinator(session_id.id(), OFFLINE_SPEC);

        let pump = start_event_pump(
            event_rx,
            session_id,
            out_tx,
            Arc::clone(&pending),
            true,
            answer_tx,
            coordinator.read(),
            makima_storage::paths::home(),
            PUMP_TRUSTED,
            None,
            test_observer(),
        );

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

        smol::block_on(pump.finish());
        let seen: Vec<_> = out_rx.drain().collect();
        assert_eq!(seen.len(), 2);
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

        let producer = registry.create_producer(makima_commands::ProducerPrecedence::Plugin);
        producer
            .replace(vec![makima_commands::Registration {
                spec: makima_commands::CommandSpec {
                    name: Arc::from("/review"),
                    aliases: Arc::from([]),
                    arguments: makima_commands::CommandArguments::Raw { required: false },
                    docs: makima_commands::CommandDocs {
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
        let elicitation = pending
            .elicitation
            .as_ref()
            .map(|elicitation| elicitation.request_id);
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
            identity: Some(makima_agent::ActorWorkIdentity::new(
                makima_agent::AgentId::generate(),
                makima_agent::ActorWorkKind::Control {
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
            subagent: Some(makima_agent::SubagentInfo {
                agent_id: makima_agent::AgentId::generate(),
                parent_agent_id: Some(makima_agent::AgentId::generate()),
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

    fn install_registry(srv: &mut Server, registry: makima_commands::CommandRegistry) {
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
                dyn makima_storage::checkpoint::CheckpointWriter<
                        makima_agent::session_coordinator::SessionCheckpoint,
                    >,
            > = Arc::new(
                |request: makima_storage::checkpoint::CheckpointRequest<_>| {
                    Box::pin(async move {
                        Err(makima_storage::checkpoint::CheckpointError::Save {
                            session_id: request.session_id,
                            message: Arc::from(SAVE_FAILED),
                        })
                    }) as makima_storage::checkpoint::CheckpointFuture
                },
            );
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
    fn prompt_rejects_command_root_from_another_handle_before_admission() {
        smol::block_on(async {
            let (mut srv, _, _, turns) = server_awaiting_answer();
            let (mut other, _, _, other_turns) = server_awaiting_answer();
            let session = srv.session.as_mut().unwrap();
            let session_id = session.handle.session_id.to_string();
            session.command_root = other.session.as_ref().unwrap().command_root.clone();
            let error = start_prompt(
                &mut srv,
                &prompt_request(&session_id, "first", false),
                &RequestId::Number(71),
            )
            .unwrap_err();
            assert_eq!(error.message, SESSION_ENDED_MESSAGE);
            assert!(srv.dispatch.is_none());
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
            assert!(turns.actor.latest_ticket().is_none());
            assert!(other_turns.actor.latest_ticket().is_none());
            close_session(&mut srv).await;
            close_session(&mut other).await;
        });
    }

    #[test]
    fn spawned_primary_turn_end_turn_releases_pending_operation() {
        smol::block_on(async {
            configure_test_paths();
            let storage = TempDir::new().unwrap();
            let (mut srv, _, out_rx, _) = server_awaiting_answer();
            let session_id = srv.session.as_ref().unwrap().handle.session_id.clone();
            let providers = spawn_test_providers(Arc::new(EndTurnProvider));
            let model = providers.model(SPAWN_TEST_SPEC).unwrap();
            let cwd = storage.path().to_path_buf();
            let mut params = test_params(model.clone(), cwd.clone());
            params.providers = providers;
            let (handle, root_target) = spawn_session(
                &params,
                SpawnSession {
                    model,
                    cwd: cwd.clone(),
                    session_id: Some(session_id),
                    root_id: None,
                    journal: None,
                    history: Vec::new(),
                    mcp_handle: None,
                    elicitation: false,
                    yolo: false,
                    defaults: SessionDefaults::default(),
                    project_config: ProjectConfig::for_project(&cwd),
                    permissions: PermissionsConfig::default(),
                },
            );
            within(handle.initialized_root(), "spawned root did not initialize")
                .await
                .unwrap();
            let command_root = within(root_target.recv_async(), "root target was not bound")
                .await
                .unwrap();
            let session = srv.session.as_mut().unwrap();
            let previous_handle = std::mem::replace(&mut session.handle, handle);
            session.command_root = command_root;
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
            let checkpoint = successful_checkpoint();
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
            let (_host, received, _, _state, _) = managed_test_session(
                &mut srv,
                nested,
                false,
                Some(completion),
                Some((started_tx, release_rx)),
                checkpoint,
            )
            .await;
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
                    "sessionId": session_id, "configId": makima_agent::session_options::FAST_OPTION_ID,
                    "value": makima_agent::session_options::ENABLED_VALUE,
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
            assert!(srv.session.is_none());
            let graph = manager.snapshot();
            assert!(!graph.is_empty());
            assert!(graph.iter().all(|node| {
                node.residency == AgentResidency::Dormant
                    && node.actor.as_ref().is_none_or(|actor| {
                        matches!(
                            actor.lifecycle,
                            ActorLifecycle::Closed | ActorLifecycle::Shutdown
                        ) && actor.status == ActorStatus::Idle
                            && actor.active_turn.is_none()
                            && actor.queued == 0
                    })
                    && matches!(
                        node.graph_lifecycle,
                        GraphLifecycle::Dormant | GraphLifecycle::Closed | GraphLifecycle::Removed
                    )
            }));
        });
    }

    #[test]
    fn prompt_immediately_after_new_session_is_queued() {
        smol::block_on(async {
            configure_test_paths();
            let temp = TempDir::new().unwrap();
            let cwd = temp.path().to_path_buf();
            let (mut srv, _, out_rx, _) = server_awaiting_answer();
            let (requests, received) = flume::unbounded();
            let (_release, release) = flume::unbounded();
            let providers = spawn_test_providers(Arc::new(ParkedProvider { requests, release }));
            let mut params = test_params(providers.model(SPAWN_TEST_SPEC).unwrap(), cwd.clone());
            params.providers = providers;
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
            let write = makima_config::ToolKey::Native(Arc::from("write"));
            assert!(matches!(
                session.handle.permissions.check(&write, &old_scope, None),
                makima_agent::permissions::PermissionCheck::Allowed
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
                makima_agent::permissions::PermissionCheck::NeedsPrompt { .. }
            ));
            assert!(matches!(
                session.handle.permissions.check(&write, &next_scope, None),
                makima_agent::permissions::PermissionCheck::Allowed
            ));
            handle_prompt(
                &mut srv,
                &prompt_request(&session_id, "immediate prompt", false),
                &RequestId::Number(83),
            )
            .await
            .unwrap();

            let request = within(received.recv_async(), "prompt never reached the provider")
                .await
                .unwrap();
            assert_eq!(text_of(&request), "immediate prompt");
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
        });
    }

    #[test]
    fn discovery_reaches_the_root_its_catalog_and_the_client() {
        smol::block_on(async {
            configure_test_paths();
            let temp = TempDir::new().unwrap();
            let cwd = temp.path().to_path_buf();
            let (mut srv, _, out_rx, _) = server_awaiting_answer();
            let models = Arc::default();
            let providers = test_providers(Arc::clone(&models));
            let model = providers
                .model(&format!("{DISCOVERY_PROVIDER}/{DISCOVERY_MODEL_ID}"))
                .unwrap();
            assert_ne!(model.context_window, DISCOVERED_WINDOW);
            let mut params = test_params(model, cwd.clone());
            params.providers = Arc::clone(&providers);
            srv.catalog = ModelCatalog::offline(
                Arc::clone(&providers),
                &makima_config::ModelPolicy::default(),
            );
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
            *models.lock().unwrap() = vec![ModelInfo {
                context_window: Some(DISCOVERED_WINDOW),
                ..ModelInfo::id_only(DISCOVERY_MODEL_ID.into())
            }];
            providers
                .fetch_all(&makima_config::ModelPolicy::default(), false, |_| {})
                .await;

            srv.catalog
                .apply(
                    vec![DISCOVERED_SPEC.to_owned()],
                    Some(DISCOVERY_PROVIDER.into()),
                )
                .await;

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
            srv.catalog
                .apply(
                    vec![DISCOVERED_SPEC.to_owned()],
                    Some(DISCOVERY_PROVIDER.into()),
                )
                .await;
            assert_eq!(read.options().version, version);
            close_session(&mut srv).await;
        });
    }

    #[test]
    fn completed_then_cancelled_session_can_be_replaced_and_prompted() {
        smol::block_on(async {
            configure_test_paths();
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

            let providers = spawn_test_providers(Arc::new(EndTurnProvider));
            let mut params = test_params(providers.model(SPAWN_TEST_SPEC).unwrap(), cwd.clone());
            params.providers = providers;
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
        assert_eq!(images[0].media_type, makima_providers::ImageMediaType::Png);
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
        let registry = test_registry(&[makima_agent::command::CustomCommand {
            name: "review".into(),
            description: "review code".into(),
            content: "Review $ARGUMENTS".into(),
            scope: makima_agent::command::CommandScope::Project,
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
            let value = |snapshot: &makima_agent::session_options::SessionOptionsSnapshot,
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
                value(&selected, makima_agent::session_options::MODEL_OPTION_ID),
                FAST_SPEC
            );
            assert_eq!(
                value(&selected, makima_agent::session_options::FAST_OPTION_ID),
                makima_agent::session_options::ENABLED_VALUE
            );
            assert_eq!(
                value(&selected, makima_agent::session_options::THINKING_OPTION_ID),
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
                value(&disabled, makima_agent::session_options::MODEL_OPTION_ID),
                FAST_SPEC
            );
            assert_eq!(
                value(&disabled, makima_agent::session_options::FAST_OPTION_ID),
                makima_agent::session_options::DISABLED_VALUE
            );
            assert_eq!(
                value(&disabled, makima_agent::session_options::THINKING_OPTION_ID),
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
                value(&repeated, makima_agent::session_options::FAST_OPTION_ID),
                makima_agent::session_options::DISABLED_VALUE
            );
            assert_eq!(
                value(&repeated, makima_agent::session_options::THINKING_OPTION_ID),
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
                makima_agent::command::NONINTERACTIVE_MODEL_USAGE
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
        let registry = test_registry(&[makima_agent::command::CustomCommand {
            name: "review".into(),
            description: "review code".into(),
            content: "Review $ARGUMENTS".into(),
            scope: makima_agent::command::CommandScope::Project,
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
        let registry = test_registry(&[makima_agent::command::CustomCommand {
            name: "review".into(),
            description: "review code".into(),
            content: "Review $ARGUMENTS".into(),
            scope: makima_agent::command::CommandScope::Project,
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

    impl makima_commands::CommandBehavior for CompletedCommand {
        fn execute(
            &self,
            invocation: makima_commands::CommandInvocation,
        ) -> makima_commands::CommandFuture<Result<CommandOutcome, makima_commands::CommandError>>
        {
            let _ = invocation;
            Box::pin(async { Ok(CommandOutcome::Completed) })
        }
    }

    #[test]
    fn completed_lua_slash_prompt_returns_without_agent_input() {
        let (mut srv, _, out_rx, turns) = server_awaiting_answer();
        let registry = test_registry(&[]);
        let producer = registry.create_producer(makima_commands::ProducerPrecedence::Plugin);
        producer
            .replace(vec![makima_commands::Registration {
                spec: makima_commands::CommandSpec {
                    name: Arc::from("/lua-complete"),
                    aliases: Arc::from([]),
                    arguments: makima_commands::CommandArguments::Positional(Arc::from([])),
                    docs: makima_commands::CommandDocs {
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

        assert_eq!(
            effective_session_cwd(None, restored.to_str().unwrap(), &client),
            restored
        );
        assert_eq!(effective_session_cwd(None, "relative", &client), client);
    }

    /// A loaded session comes back in the mode and thinking it was saved
    /// with, even when `always_thinking` says otherwise.
    #[test_case(false; "v3")]
    #[test_case(true; "v2")]
    fn load_restores_saved_mode_and_thinking_over_config(legacy: bool) {
        smol::block_on(async {
            let tmp = TempDir::new().unwrap();
            configure_test_paths();
            let plan = tmp.path().join(SAVED_PLAN_FILE);
            std::fs::write(&plan, SAVED_PLAN_CONTENT).unwrap();
            let providers = test_providers(Arc::new(Mutex::new(vec![ModelInfo {
                supports_thinking: Some(true),
                ..ModelInfo::id_only(THINKING_MODEL_ID.into())
            }])));
            let spec = format!("{DISCOVERY_PROVIDER}/{THINKING_MODEL_ID}");
            let mut session: Session<Message, TokenUsage, makima_agent::ToolOutput> =
                Session::new(&spec, &tmp.path().to_string_lossy());
            let original_root = AgentId::generate();
            session.bind_root_id(original_root).unwrap();
            session.replace_messages(vec![Message::user("planned".into())]);
            session.meta.thinking = Some(StoredThinking::Off);
            if !legacy {
                let mut definition = makima_agent::ModeDef::default_for(ModeId::Plan);
                definition.restrict_write_to = Some(plan.clone());
                session
                    .resolve_legacy_mode(definition.stored_snapshot(&AgentMode::Plan(plan.clone())))
                    .unwrap();
                let mut plans = PlanSlot::new();
                plans.insert(
                    original_root,
                    PlanEntry {
                        path: plan.clone(),
                        written: true,
                        approved: None,
                        staged_model: None,
                    },
                );
                let seq = session
                    .state()
                    .iter()
                    .map(|record| record.seq)
                    .chain(session.agent_revisions().iter().map(|(seq, _)| *seq))
                    .max()
                    .unwrap_or_default()
                    + 1;
                session.push_state(StoredStateRecord {
                    seq,
                    agent: None,
                    generation: 0,
                    change: StateChange::Plugin {
                        key: PLAN_STATE_KEY.into(),
                        version: PLAN_STATE_VERSION,
                        value: Some(serde_json::to_value(plans).unwrap()),
                    },
                });
            }
            let storage = StateDir::from_path(tmp.path().to_path_buf());
            session.save(&storage).unwrap();
            if legacy {
                let path = storage
                    .path()
                    .join(SESSIONS_DIR)
                    .join(format!("{}.jsonl", session.id));
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
                meta["mode"] = Value::String("plan".into());
                meta["plan_path"] = Value::String(plan.to_string_lossy().into_owned());
                meta["plan_written"] = Value::Bool(true);
                meta["thinking"] = serde_json::to_value(StoredThinking::Off).unwrap();
                std::fs::write(
                    path,
                    records
                        .iter()
                        .map(|record| format!("{record}\n"))
                        .collect::<String>(),
                )
                .unwrap();
            }
            let model = providers.model(&spec).unwrap();
            assert_eq!(model.supports_thinking(), Some(true));
            let mut params = test_params(model, tmp.path().to_path_buf());
            params.providers = providers;
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
            assert_eq!(config.mode, AgentMode::Plan(plan.clone()));
            assert_eq!(config.thinking, makima_agent::ThinkingConfig::Off);
            let migrated: Session<Message, TokenUsage, makima_agent::ToolOutput> =
                Session::load(session.id, &storage).unwrap();
            let root_id = migrated.root_agent_id().unwrap();
            assert_eq!(root.id(), root_id);
            if !legacy {
                assert_eq!(root_id, original_root);
            }
            let state = migrated.latest_state().unwrap();
            let plans = makima_agent::plan_state::plan_entries(&state).unwrap();
            assert_eq!(plans[&root_id].path, plan);
            assert!(plans[&root_id].written);
            assert_eq!(std::fs::read_to_string(&plan).unwrap(), SAVED_PLAN_CONTENT);
            let stored = state.scopes[&Some(root_id)].config.as_ref().unwrap();
            assert_eq!(stored.mode.id, ModeId::Plan.key());
            assert_eq!(
                stored.mode.restrict_write_to.as_deref(),
                config.mode.plan_path()
            );
            assert_eq!(stored.thinking, Some(StoredThinking::Off));
            let mut journal_view = migrated.clone();
            srv.session
                .as_ref()
                .unwrap()
                .handle
                .manager
                .journal()
                .snapshot()
                .apply_to(&mut journal_view)
                .unwrap();
            let journal_state = journal_view.latest_state().unwrap();
            assert_eq!(
                journal_state.scopes[&Some(root_id)].config.as_ref(),
                Some(stored)
            );
            close_session(&mut srv).await;
            load_session(&mut srv, &raw, &params).await.unwrap();
            assert_eq!(
                srv.session
                    .as_ref()
                    .unwrap()
                    .handle
                    .initialized_root()
                    .await
                    .unwrap()
                    .id(),
                root_id
            );
            let resumed = Session::<Message, TokenUsage, makima_agent::ToolOutput>::load(
                session.id, &storage,
            )
            .unwrap();
            let resumed_plans =
                makima_agent::plan_state::plan_entries(&resumed.latest_state().unwrap()).unwrap();
            assert_eq!(resumed_plans[&root_id].path, plan);
            assert!(resumed_plans[&root_id].written);
            assert_eq!(std::fs::read_to_string(&plan).unwrap(), SAVED_PLAN_CONTENT);
            close_session(&mut srv).await;
        });
    }

    #[test]
    fn failed_load_keeps_active_session_without_replaying_transcript() {
        smol::block_on(async {
            let tmp = TempDir::new().unwrap();
            let providers = spec_providers();
            let mut params = test_params(
                providers.model(OFFLINE_SPEC).unwrap(),
                tmp.path().to_path_buf(),
            );
            params.providers = providers;
            let mut candidate = StoredSession::new(OFFLINE_SPEC, &tmp.path().to_string_lossy());
            candidate.replace_messages(vec![Message::user(PARKED_THOUGHT.into())]);
            candidate.save(&params.storage).unwrap();
            let conflicting_coordinator = test_coordinator(candidate.id, OFFLINE_SPEC);
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
            new_session(
                &mut srv,
                &serde_json::json!({ "params": { "cwd": tmp.path(), "mcpServers": [] } }),
                &params,
            )
            .await
            .unwrap();
            let active = srv.session.as_ref().unwrap();
            let active_id = active.handle.session_id.to_string();
            let active_root = active.handle.initialized_root().await.unwrap();
            drop(out_rx.drain());
            let raw = serde_json::json!({
                "params": {
                    "sessionId": candidate.id.to_string(),
                    "cwd": tmp.path(),
                    "mcpServers": []
                }
            });

            let error = load_session(&mut srv, &raw, &params).await.unwrap_err();
            assert_eq!(error.code, AcpError::internal_error().code);
            assert!(
                out_rx.is_empty(),
                "failed installation must not replay history"
            );
            let active = validate_session(&srv, &active_id).unwrap();
            assert!(active_root.is_live());
            assert_eq!(
                active.handle.initialized_root().await.unwrap().id(),
                active_root.id()
            );
            handle_set_mode(
                &mut srv,
                &serde_json::json!({ "params": { "sessionId": active_id, "modeId": "plan" } }),
                &params,
            )
            .await
            .unwrap();
            let sessions_dir = params.storage.ensure_subdir(SESSIONS_DIR).unwrap();
            assert!(
                session_lock::claim(&sessions_dir, &candidate.id)
                    .unwrap()
                    .is_some()
            );
            conflicting_coordinator.close().await.unwrap();
            close_session(&mut srv).await;
        });
    }

    #[test]
    fn load_uses_explicit_model_over_folded_root_model_and_options() {
        smol::block_on(async {
            let tmp = TempDir::new().unwrap();
            let providers = spec_providers();
            let mut params = test_params(
                providers.model(OFFLINE_SPEC).unwrap(),
                tmp.path().to_path_buf(),
            );
            params.providers = providers;
            params.model_explicit = true;
            let mut seed = StoredSession::new(FAST_SPEC, &tmp.path().to_string_lossy());
            let root_id = AgentId::generate();
            seed.bind_root_id(root_id).unwrap();
            seed.meta
                .session_options
                .insert(MODEL_OPTION_ID.into(), FAST_SPEC.into());
            seed.resolve_legacy_mode(
                makima_agent::ModeDef::default_for(ModeId::Build)
                    .stored_snapshot(&AgentMode::Build),
            )
            .unwrap();
            seed.save(&params.storage).unwrap();
            let (out_tx, _out_rx) = flume::unbounded();
            let mut srv = Server {
                out_tx,
                catalog: ModelCatalog::offline(Arc::clone(&params.providers), &params.model_policy),
                session: None,
                elicitation: false,
                supports_boolean: false,
                lua_event_handle: params.lua_event_handle.clone(),
                dispatch: None,
            };
            load_session(
                &mut srv,
                &serde_json::json!({ "params": { "sessionId": seed.id.to_string(), "cwd": tmp.path(), "mcpServers": [] } }),
                &params,
            )
            .await
            .unwrap();
            let active = srv.session.as_ref().unwrap();
            let root = active.handle.initialized_root().await.unwrap();
            assert_eq!(
                root.effective_config().unwrap().unwrap().model.spec(),
                OFFLINE_SPEC
            );
            let saved = StoredSession::load(seed.id, &params.storage).unwrap();
            let view = saved.latest_state().unwrap();
            let config = view.scopes[&Some(root_id)].config.as_ref().unwrap();
            assert_eq!(config.model, OFFLINE_SPEC);
            assert_eq!(config.options[MODEL_OPTION_ID], OFFLINE_SPEC);
            close_session(&mut srv).await;
        });
    }

    fn open_stored_session_for_test(
        storage: &StateDir,
        id: MakimaId,
    ) -> Result<StoredSession, AcpError> {
        let providers = spec_providers();
        let model = providers.model(OFFLINE_SPEC).unwrap();
        open_session(
            storage,
            SessionSource::Resume(id),
            OpenModel {
                startup: &model,
                explicit: false,
                providers: &providers,
                policy: &Default::default(),
            },
            None,
        )
        .map(|opening| opening.session)
        .map_err(|error| session_open_error(id, error))
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
        let mut session: Session<Message, TokenUsage, makima_agent::ToolOutput> =
            Session::new(OFFLINE_SPEC, "/project");
        session.replace_messages(messages.clone());
        session.save(&dir).unwrap();

        let id: MakimaId = session.id;
        let history = open_stored_session_for_test(&dir, id).unwrap();
        assert_eq!(
            serde_json::to_value(history.history_snapshot().messages.as_ref()).unwrap(),
            serde_json::to_value(&messages).unwrap()
        );
    }

    #[test]
    fn load_history_restores_folded_config_and_rules() {
        let tmp = TempDir::new().unwrap();
        let dir = StateDir::from_path(tmp.path().to_path_buf());
        let mut session: Session<Message, TokenUsage, makima_agent::ToolOutput> =
            Session::new(OFFLINE_SPEC, "/project");
        session.meta.yolo = Some(true);
        session.meta.fast = true;
        session.meta.workflow = true;
        session
            .meta
            .session_options
            .insert("bash.auto_mode".into(), "enabled".into());
        session.bind_root_id(AgentId::generate()).unwrap();
        session
            .resolve_legacy_mode(
                makima_agent::ModeDef::default_for(ModeId::Build)
                    .stored_snapshot(&AgentMode::Build),
            )
            .unwrap();
        session.push_state(StoredStateRecord {
            seq: session
                .state()
                .iter()
                .map(|record| record.seq)
                .chain(session.agent_revisions().iter().map(|(seq, _)| *seq))
                .max()
                .unwrap_or_default()
                + 1,
            agent: None,
            generation: 0,
            change: StateChange::Rules {
                rules: Vec::new(),
                yolo: Some(true),
            },
        });
        session.save(&dir).unwrap();

        let restored = open_stored_session_for_test(&dir, session.id).unwrap();
        let state = restored.latest_state().unwrap();
        let config = state.scopes[&restored.root_agent_id()]
            .config
            .as_ref()
            .unwrap();
        assert_eq!(state.scopes[&None].yolo, Some(true));
        assert!(config.fast);
        assert!(config.workflow);
        assert_eq!(
            config.options.get("bash.auto_mode").map(String::as_str),
            Some("enabled")
        );
        assert!(restored.meta.yolo.is_none());
        assert!(restored.meta.session_options.is_empty());
    }

    #[test]
    fn load_missing_session_is_resource_not_found() {
        let tmp = TempDir::new().unwrap();
        let dir = StateDir::from_path(tmp.path().to_path_buf());
        let err = open_stored_session_for_test(&dir, MakimaId::generate())
            .err()
            .unwrap();
        assert_eq!(err.code, AcpError::resource_not_found(None).code);
    }

    #[test]
    fn load_rejects_session_open_elsewhere() {
        const FAKE_PID: u32 = u32::MAX - 1;
        let tmp = TempDir::new().unwrap();
        let dir = StateDir::from_path(tmp.path().to_path_buf());
        let mut session: Session<Message, TokenUsage, makima_agent::ToolOutput> =
            Session::new(OFFLINE_SPEC, "/project");
        session.replace_messages(vec![Message::user("hi".into())]);
        session.save(&dir).unwrap();
        let id = session.id;

        let sessions_dir = dir.ensure_subdir(SESSIONS_DIR).unwrap();
        let lock = session_lock::lock_path(&sessions_dir, &id);
        std::fs::write(&lock, FAKE_PID.to_string()).unwrap();
        let err = open_stored_session_for_test(&dir, id).err().unwrap();
        assert_eq!(err.code, AcpError::internal_error().code);
        assert_eq!(
            err.data,
            Some(Value::String(session_lock::OPEN_ELSEWHERE_MSG.to_owned()))
        );

        std::fs::remove_file(&lock).unwrap();
        assert!(open_stored_session_for_test(&dir, id).is_ok());
    }

    #[test]
    fn converts_injected_mcp_servers() {
        let raw = serde_json::json!({
            "params": {
                "sessionId": MakimaId::generate().to_string(),
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
                    event: AgentEvent::ToolStart(Box::new(makima_agent::ToolStartEvent {
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
                        tool: makima_config::ToolKey::Native(Arc::from("write")),
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
