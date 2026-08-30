use std::path::PathBuf;
use std::sync::Arc;

use async_lock::Mutex;
use flume::Receiver;
use futures_lite::future;
use maki_config::ModelPolicy;
use maki_providers::Message;
use maki_providers::Timeouts;
use maki_providers::TokenUsage;
use maki_providers::model::Model;
use maki_providers::provider::{self, Provider};
use maki_storage::StateDir;
use maki_storage::id::{MakiId, SessionRef};
use maki_storage::sessions::Session;
use serde_json::Value;
use tracing::{error, warn};

use crate::agent::{self, History};
use crate::cancel::{CancelMap, CancelToken};
use crate::permissions::{PermissionManager, PluginRuleStore};
use crate::prompt::ResolvedSlots;
use crate::template;
use crate::tools::{
    DescriptionContext, FileReadTracker, LocalTools, ToolAudience, ToolFilter, ToolRegistry,
};
use crate::{
    Agent, AgentConfig, AgentEvent, AgentId, AgentInput, AgentParams, AgentRunParams, Envelope,
    EventSender, McpHandle, McpSession, PermissionsConfig, SessionMailbox, ToolOutput,
    ToolOutputLines, TurnFailure, TurnId, TurnOutcome,
};

type StoredSession = Session<Message, TokenUsage, ToolOutput>;

struct SessionStore {
    dir: StateDir,
    session: StoredSession,
}

impl SessionStore {
    fn open(session_id: MakiId, cwd: &str, model_spec: &str) -> Option<Self> {
        let dir = StateDir::resolve()
            .map_err(|e| warn!(error = %e, "state dir unavailable; session will not be persisted"))
            .ok()?;
        Some(Self::open_in(dir, session_id, cwd, model_spec))
    }

    fn open_in(dir: StateDir, session_id: MakiId, cwd: &str, model_spec: &str) -> Self {
        match StoredSession::load(session_id, &dir) {
            Ok(session) => Self { dir, session },
            Err(_) => {
                let mut session = StoredSession::new(model_spec, cwd);
                session.id = session_id;
                let mut store = Self { dir, session };
                store.save();
                store
            }
        }
    }

    fn save(&mut self) {
        if let Err(e) = self.session.save(&self.dir) {
            warn!(error = %e, session_id = %self.session.id, "failed to persist session");
        }
    }

    fn record_turn(&mut self, messages: &[Message], model_spec: String) {
        self.session.replace_messages(messages.to_vec());
        self.session.set_model(model_spec);
        self.session.update_title_if_default();
        self.save();
    }
}

pub struct HeadlessParams {
    pub model: Model,
    pub config: AgentConfig,
    pub permissions_config: PermissionsConfig,
    pub timeouts: Timeouts,
    pub input: AgentInput,
    pub prompt_slots: ResolvedSlots,
    pub excluded_tools: Vec<&'static str>,
    pub mcp_handle: Option<McpHandle>,
    pub initial_wd: PathBuf,
    pub system_prompt_override: Option<String>,
    pub append_system_prompt: Option<String>,
    pub model_policy: Arc<ModelPolicy>,
    pub plugin_rules: Arc<PluginRuleStore>,
    pub modes: Arc<crate::ModeRegistry>,
}

pub struct HeadlessHandle {
    pub event_rx: Receiver<Envelope>,
    pub tool_names: Vec<String>,
    pub session_id: SessionRef,
    pub cwd: String,
    pub task: smol::Task<()>,
}

struct AgentSetup {
    vars: template::Vars,
    instructions: agent::Instructions,
    tools: Value,
}

fn setup(
    model: &Model,
    config: &AgentConfig,
    excluded_tools: &[&'static str],
    workflow: bool,
) -> AgentSetup {
    let vars = template::env_vars();
    let instructions = agent::load_instructions(&vars.apply("{cwd}"));
    let tools = tool_definitions(
        &vars,
        model,
        config,
        excluded_tools,
        workflow,
        ToolRegistry::global(),
    );

    AgentSetup {
        vars,
        instructions,
        tools,
    }
}

/// Base definitions only. MCP definitions are injected per request by
/// `Agent::request_tools`; storing them here would freeze the catalog.
fn tool_definitions(
    vars: &template::Vars,
    model: &Model,
    config: &AgentConfig,
    excluded_tools: &[&'static str],
    workflow: bool,
    registry: &ToolRegistry,
) -> Value {
    let filter = ToolFilter::from_config(config, model, excluded_tools);
    let ctx = DescriptionContext {
        filter: &filter,
        audience: ToolAudience::MAIN,
        workflow,
    };
    registry.definitions(vars, &ctx, model.supports_tool_examples())
}

/// Names advertised to SDK clients: base tools plus what the first request
/// would carry from MCP (always-load definitions and `tool_search`).
fn advertised_tool_names(tools: &Value, mcp: Option<&McpSession>) -> Vec<String> {
    let mut probe = tools.clone();
    if let Some(mcp) = mcp {
        mcp.extend_tools(&mut probe);
    }
    extract_tool_names(&probe)
}

pub fn spawn(params: HeadlessParams) -> HeadlessHandle {
    let working_dir = params.initial_wd.to_string_lossy().into_owned();
    let mode = params.input.mode.clone();
    let workflow = params.input.workflow;
    let AgentSetup {
        vars,
        instructions,
        tools,
    } = setup(
        &params.model,
        &params.config,
        &params.excluded_tools,
        workflow,
    );

    let mut system = params.system_prompt_override.clone().unwrap_or_else(|| {
        agent::build_system_prompt(
            &vars,
            &params.modes,
            &mode,
            &instructions.text,
            &params.prompt_slots,
            &params.model,
        )
    });
    if let Some(append) = &params.append_system_prompt {
        system.push('\n');
        system.push_str(append);
    }

    let mcp = params.mcp_handle.clone().map(|h| McpSession::new(h, &[]));
    let tool_names = advertised_tool_names(&tools, mcp.as_ref());

    let (raw_tx, event_rx) = flume::unbounded::<Envelope>();

    let session_id = MakiId::generate();
    let session_ref = SessionRef::from(session_id);
    let session_ref_clone = session_ref.clone();
    let mailbox = SessionMailbox::register(session_id);
    let file_write_locks = Arc::new(crate::tools::FileWriteLocks::new());
    let task = smol::spawn({
        let file_write_locks = Arc::clone(&file_write_locks);
        let mcp_shutdown = params.mcp_handle.clone();
        let working_dir_path = params.initial_wd.clone();
        async move {
            let event_tx = EventSender::new(raw_tx, 0);
            let mut model = params.model;
            let provider: Arc<dyn Provider> =
                match provider::from_model_async(&mut model, params.timeouts).await {
                    Ok(p) => Arc::from(p),
                    Err(e) => {
                        error!(error = %e, "provider error");
                        let _ = event_tx.send(AgentEvent::ControlError {
                            message: e.user_message(),
                        });
                        return;
                    }
                };
            let mut history = History::new(Vec::new());
            let mut agent = Agent::new(
                AgentParams {
                    agent_id: AgentId::generate(),
                    provider,
                    model,
                    config: params.config,
                    tool_output_lines: ToolOutputLines::default(),
                    permissions: Arc::new(PermissionManager::new(
                        params.permissions_config,
                        working_dir_path,
                        params.plugin_rules,
                    )),
                    session_id: Some(session_ref_clone.clone()),
                    mailbox: Some(mailbox.clone()),
                    timeouts: params.timeouts,
                    file_tracker: FileReadTracker::fresh(),
                    prompt_slots: Arc::new(params.prompt_slots),
                    modes: Arc::clone(&params.modes),
                    subagent_cancels: Arc::new(CancelMap::new()),
                    registry: Arc::clone(ToolRegistry::global_arc()),
                    audience: ToolAudience::MAIN,
                    question_mode: crate::tools::QuestionMode::Headless,
                    model_policy: Arc::clone(&params.model_policy),
                    file_write_locks: Arc::clone(&file_write_locks),
                },
                AgentRunParams {
                    history: &mut history,
                    system,
                    event_tx,
                    tools,
                },
            )
            .with_loaded_instructions(instructions.loaded)
            .with_mcp(mcp);

            agent.run(TurnId::generate(), params.input).await;
            drop(agent);

            if let Some(handle) = mcp_shutdown {
                handle.shutdown().await;
            }
        }
    });

    HeadlessHandle {
        event_rx,
        tool_names,
        session_id: session_ref,
        cwd: working_dir,
        task,
    }
}

pub struct InteractiveParams {
    pub model: Model,
    pub config: AgentConfig,
    pub permissions_config: PermissionsConfig,
    pub timeouts: Timeouts,
    pub prompt_slots: Arc<ResolvedSlots>,
    pub excluded_tools: Vec<&'static str>,
    pub mcp_handle: Option<McpHandle>,
    pub initial_wd: PathBuf,
    pub session_id: Option<SessionRef>,
    pub initial_history: Vec<Message>,
    pub yolo: bool,
    pub system_prompt_override: Option<String>,
    pub append_system_prompt: Option<String>,
    pub workflow: bool,
    pub model_policy: Arc<ModelPolicy>,
    pub modes: Arc<crate::ModeRegistry>,
    pub question_mode: crate::tools::QuestionMode,
    pub plugin_rules: Arc<PluginRuleStore>,
    /// Host-side overrides that shadow a registered tool's execution while
    /// keeping its advertised schema (e.g. ACP answers `question` via elicitation).
    pub local_tools: LocalTools,
}

pub enum InteractiveControl {
    Compact(flume::Sender<Result<(), String>>),
    Reset(flume::Sender<Result<(), String>>),
    ChangeDirectory {
        path: PathBuf,
        reply: flume::Sender<Result<(), String>>,
    },
}

struct InteractiveControlContext<'a> {
    history: &'a mut History,
    store: &'a mut Option<SessionStore>,
    model: &'a Model,
    provider: &'a dyn Provider,
    raw_tx: &'a flume::Sender<Envelope>,
    run_id: u64,
    config: &'a AgentConfig,
    working_dir: &'a mut PathBuf,
    permissions: &'a PermissionManager,
}

async fn apply_interactive_control(
    control: InteractiveControl,
    context: InteractiveControlContext<'_>,
) {
    let InteractiveControlContext {
        history,
        store,
        model,
        provider,
        raw_tx,
        run_id,
        config,
        working_dir,
        permissions,
    } = context;
    let result = match &control {
        InteractiveControl::Compact(_) => agent::compact(
            provider,
            model,
            history,
            &EventSender::new(raw_tx.clone(), run_id),
            config,
        )
        .await
        .map_err(|error| error.to_string()),
        InteractiveControl::Reset(_) => {
            history.replace(Vec::new());
            if let Some(store) = store {
                store.record_turn(&[], model.spec());
            }
            Ok(())
        }
        InteractiveControl::ChangeDirectory { path, .. } => path
            .canonicalize()
            .and_then(|path| {
                if !path.is_dir() {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::NotADirectory,
                        "path is not a directory",
                    ));
                }
                *working_dir = path;
                permissions.set_cwd(working_dir.clone());
                if let Some(store) = store {
                    store
                        .session
                        .set_cwd(working_dir.to_string_lossy().into_owned());
                    store.save();
                }
                Ok(())
            })
            .map_err(|error| error.to_string()),
    };
    let reply = match control {
        InteractiveControl::Compact(reply) | InteractiveControl::Reset(reply) => reply,
        InteractiveControl::ChangeDirectory { reply, .. } => reply,
    };
    let _ = reply.send(result);
}

pub struct InteractiveHandle {
    pub event_rx: Receiver<Envelope>,
    pub tool_names: Vec<String>,
    pub input_tx: flume::Sender<AgentInput>,
    pub answer_tx: flume::Sender<String>,
    pub cancel_tx: flume::Sender<()>,
    pub model_tx: flume::Sender<Model>,
    pub control_tx: flume::Sender<InteractiveControl>,
    pub session_id: SessionRef,
    pub permissions: Arc<PermissionManager>,
    pub task: smol::Task<()>,
}

pub fn spawn_interactive(params: InteractiveParams) -> InteractiveHandle {
    let initial_tools = tool_definitions(
        &template::env_vars(),
        &params.model,
        &params.config,
        &params.excluded_tools,
        params.workflow,
        ToolRegistry::global(),
    );

    let mcp = params
        .mcp_handle
        .clone()
        .map(|h| McpSession::new(h, &params.initial_history));
    let tool_names = advertised_tool_names(&initial_tools, mcp.as_ref());

    let (raw_tx, event_rx) = flume::unbounded::<Envelope>();
    let (input_tx, input_rx) = flume::unbounded::<AgentInput>();
    let (answer_tx, answer_rx) = flume::unbounded::<String>();
    let (cancel_tx, cancel_rx) = flume::bounded::<()>(1);
    let (model_tx, model_rx) = flume::unbounded::<Model>();
    let (control_tx, control_rx) = flume::unbounded::<InteractiveControl>();

    let (session_id, session_ref) = match params.session_id.clone() {
        Some(w) => (w.id(), w),
        None => {
            let id = MakiId::generate();
            (id, SessionRef::from(id))
        }
    };
    let mailbox = SessionMailbox::register(session_id);

    let working_dir = params.initial_wd.to_string_lossy().into_owned();
    let permissions = Arc::new(PermissionManager::new(
        params.permissions_config.clone(),
        params.initial_wd,
        Arc::clone(&params.plugin_rules),
    ));
    if params.yolo {
        permissions.toggle_yolo();
    }
    let modes = Arc::clone(&params.modes);

    let answer_rx = Arc::new(Mutex::new(answer_rx));
    let file_tracker = FileReadTracker::fresh();
    let file_write_locks = Arc::new(crate::tools::FileWriteLocks::new());

    let session_ref_clone = session_ref.clone();
    let task = smol::spawn({
        let permissions = Arc::clone(&permissions);
        let file_write_locks = Arc::clone(&file_write_locks);
        async move {
            let mut model = params.model;
            let mut provider: Arc<dyn Provider> =
                match provider::from_model_async(&mut model, params.timeouts).await {
                    Ok(p) => Arc::from(p),
                    Err(e) => {
                        error!(error = %e, "provider error");
                        let _ = EventSender::new(raw_tx, 0).send(AgentEvent::ControlError {
                            message: e.user_message(),
                        });
                        return;
                    }
                };

            let mut store = SessionStore::open(session_id, &working_dir, &model.spec());
            let mut history = History::restored(params.initial_history);
            let mut working_dir = PathBuf::from(working_dir);
            let permissions = permissions;
            let agent_id = AgentId::generate();
            let mut run_id: u64 = 0;

            enum Wake {
                Input(AgentInput),
                Control(InteractiveControl),
            }

            loop {
                let wake = if let Ok(control) = control_rx.try_recv() {
                    Some(Wake::Control(control))
                } else {
                    future::or(
                        async { input_rx.recv_async().await.map(Wake::Input) },
                        async { control_rx.recv_async().await.map(Wake::Control) },
                    )
                    .await
                    .ok()
                };
                let input = match wake {
                    Some(Wake::Input(input)) => input,
                    Some(Wake::Control(control)) => {
                        apply_interactive_control(
                            control,
                            InteractiveControlContext {
                                history: &mut history,
                                store: &mut store,
                                model: &model,
                                provider: &*provider,
                                raw_tx: &raw_tx,
                                run_id,
                                config: &params.config,
                                working_dir: &mut working_dir,
                                permissions: &permissions,
                            },
                        )
                        .await;
                        continue;
                    }
                    None => break,
                };
                let turn_id = TurnId::generate();
                let (trigger, cancel) = CancelToken::new();
                let cancel_task = smol::spawn({
                    let cancel_rx = cancel_rx.clone();
                    async move {
                        if cancel_rx.recv_async().await.is_ok() {
                            trigger.cancel();
                        }
                    }
                });

                // MCP connects in the background, so a prompt that beats it waits
                // here instead of shipping a turn without the MCP tools. The wait
                // is racing cancel: a slow server must not pin the whole session.
                if let Some(mcp) = &mcp {
                    let _ = cancel.race(mcp.ready()).await;
                }

                let event_tx = EventSender::new(raw_tx.clone(), run_id);
                let error_tx = event_tx.clone();

                if let Some(mut new_model) = model_rx
                    .try_iter()
                    .last()
                    .filter(|candidate| params.model_policy.allows(&candidate.spec()))
                    && new_model.spec() != model.spec()
                {
                    match provider::from_model_async(&mut new_model, params.timeouts).await {
                        Ok(p) => {
                            provider = Arc::from(p);
                            model = new_model;
                        }
                        Err(e) => {
                            error!(error = %e, agent_id = %agent_id, %turn_id, "provider error");
                            let outcome = TurnOutcome::Failed {
                                agent_id,
                                turn_id,
                                usage: TokenUsage::default(),
                                num_turns: 0,
                                failure: TurnFailure::from_agent_error(&e),
                            };
                            if let Err(send_error) = error_tx.send(AgentEvent::TurnOutcome(outcome))
                            {
                                error!(
                                    %send_error,
                                    agent_id = %agent_id,
                                    %turn_id,
                                    "terminal outcome delivery failed"
                                );
                            }
                            cancel_task.cancel().await;
                            run_id += 1;
                            continue;
                        }
                    }
                }

                let turn_vars = template::env_vars_for(&working_dir);
                let turn_instructions = agent::load_instructions(&working_dir.to_string_lossy());
                let tools = tool_definitions(
                    &turn_vars,
                    &model,
                    &params.config,
                    &params.excluded_tools,
                    input.workflow,
                    ToolRegistry::global(),
                );
                let mut system = params.system_prompt_override.clone().unwrap_or_else(|| {
                    agent::build_system_prompt(
                        &turn_vars,
                        &modes,
                        &input.mode,
                        &turn_instructions.text,
                        &params.prompt_slots,
                        &model,
                    )
                });
                if let Some(append) = &params.append_system_prompt {
                    system.push('\n');
                    system.push_str(append);
                }

                while answer_rx.lock().await.try_recv().is_ok() {}

                let mut agent = Agent::new(
                    AgentParams {
                        agent_id,
                        provider: Arc::clone(&provider),
                        model: model.clone(),
                        config: params.config.clone(),
                        tool_output_lines: ToolOutputLines::default(),
                        permissions: Arc::clone(&permissions),
                        session_id: Some(session_ref_clone.clone()),
                        mailbox: Some(mailbox.clone()),
                        timeouts: params.timeouts,
                        file_tracker: Arc::clone(&file_tracker),
                        prompt_slots: Arc::clone(&params.prompt_slots),
                        modes: Arc::clone(&modes),
                        subagent_cancels: Arc::new(CancelMap::new()),
                        registry: Arc::clone(ToolRegistry::global_arc()),
                        audience: ToolAudience::MAIN,
                        question_mode: params.question_mode,
                        model_policy: Arc::clone(&params.model_policy),
                        file_write_locks: Arc::clone(&file_write_locks),
                    },
                    AgentRunParams {
                        history: &mut history,
                        system,
                        event_tx,
                        tools: tools.clone(),
                    },
                )
                .with_loaded_instructions(turn_instructions.loaded)
                .with_user_response_rx(Arc::clone(&answer_rx))
                .with_cancel(cancel)
                .with_local_tools(Arc::clone(&params.local_tools))
                .with_mcp(mcp.clone());

                agent.run(turn_id, input).await;
                drop(agent);
                cancel_task.cancel().await;

                if let Some(store) = &mut store {
                    store.record_turn(history.as_slice(), model.spec());
                }
                run_id += 1;
            }

            if let Some(handle) = params.mcp_handle {
                handle.shutdown().await;
            }
        }
    });

    InteractiveHandle {
        event_rx,
        tool_names,
        input_tx,
        answer_tx,
        cancel_tx,
        model_tx,
        control_tx,
        session_id: session_ref,
        permissions,
        task,
    }
}

fn extract_tool_names(tools: &Value) -> Vec<String> {
    tools
        .as_array()
        .map(|arr| {
            arr.iter()
                .filter_map(|t| t["name"].as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use maki_storage::sessions::generate_title;
    use tempfile::TempDir;

    use super::*;

    const SESSION_ID: &str = "01965087-4c71-7f00-8000-000000000000";
    const CWD: &str = "/project";
    const MODEL_SPEC: &str = "anthropic/claude-test";

    fn session_id() -> MakiId {
        SESSION_ID.parse().unwrap()
    }

    fn store_in(tmp: &TempDir) -> SessionStore {
        SessionStore::open_in(
            StateDir::from_path(tmp.path().to_path_buf()),
            session_id(),
            CWD,
            MODEL_SPEC,
        )
    }

    fn load(tmp: &TempDir) -> StoredSession {
        StoredSession::load(session_id(), &StateDir::from_path(tmp.path().to_path_buf())).unwrap()
    }

    #[test]
    fn new_session_is_loadable_before_first_turn() {
        let tmp = TempDir::new().unwrap();
        store_in(&tmp);
        let loaded = load(&tmp);
        assert_eq!(loaded.id, session_id());
        assert_eq!(loaded.cwd, CWD);
        assert_eq!(loaded.model, MODEL_SPEC);
        assert!(loaded.messages().is_empty());
    }

    #[test]
    fn record_turn_persists_messages_and_title() {
        let tmp = TempDir::new().unwrap();
        let mut store = store_in(&tmp);
        let messages = vec![Message::user("fix the login bug".into())];
        store.record_turn(&messages, MODEL_SPEC.into());

        let loaded = load(&tmp);
        assert_eq!(loaded.messages().len(), 1);
        assert_eq!(loaded.title, generate_title(&messages));
    }

    #[test]
    fn record_turn_persists_observations() {
        let tmp = TempDir::new().unwrap();
        let mut store = store_in(&tmp);
        store.record_turn(
            &[
                Message::user("fix the login bug".into()),
                Message::observation("build failed".into()),
            ],
            MODEL_SPEC.into(),
        );

        let loaded = load(&tmp);
        assert_eq!(loaded.messages().len(), 2);
        assert!(loaded.messages()[1].is_observation());
    }

    #[test]
    fn reopening_resumes_existing_session() {
        let tmp = TempDir::new().unwrap();
        let mut store = store_in(&tmp);
        store.record_turn(&[Message::user("first prompt".into())], MODEL_SPEC.into());
        drop(store);

        let mut store = store_in(&tmp);
        assert_eq!(store.session.messages().len(), 1);

        let messages = vec![
            Message::user("first prompt".into()),
            Message::user("second prompt".into()),
        ];
        store.record_turn(&messages, "other/model".into());

        let loaded = load(&tmp);
        assert_eq!(loaded.messages().len(), 2);
        assert_eq!(loaded.model, "other/model");
    }

    #[test]
    fn extract_tool_names_filters_valid_entries() {
        let tools = serde_json::json!([{"name": "read"}, {"type": "function"}, {"name": "bash"}]);
        assert_eq!(extract_tool_names(&tools), vec!["read", "bash"]);
    }

    #[test]
    fn advertised_names_show_tool_search_not_deferred_tools() {
        let base = serde_json::json!([{"name": "read"}]);
        let mcp = crate::mcp::stub_session(&[("srv.fetch_issue", "Fetch a GitHub issue")]);
        let names = advertised_tool_names(&base, Some(&mcp));
        assert_eq!(
            names,
            vec!["read", crate::mcp::TOOL_SEARCH_TOOL_NAME],
            "clients must see the search tool, not deferred definitions"
        );
        assert_eq!(
            base,
            serde_json::json!([{"name": "read"}]),
            "probing must not bake MCP entries into the base tools"
        );
        assert_eq!(advertised_tool_names(&base, None), vec!["read"]);
    }
}
