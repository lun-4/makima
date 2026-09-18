use std::path::PathBuf;
use std::sync::Arc;

use async_lock::Mutex;
use flume::Receiver;
use futures_lite::future;
use maki_config::ModelPolicy;
use maki_providers::Message;
use maki_providers::model::Model;
use maki_providers::provider::{self, Provider};
use maki_providers::{Timeouts, TokenUsage};
use maki_storage::id::{MakiId, SessionRef};
use serde_json::Value;
use tracing::error;

use crate::agent::{self, History};
use crate::cancel::{CancelMap, CancelToken};
use crate::permissions::{PermissionManager, PluginRuleStore};
use crate::prompt::ResolvedSlots;
use crate::session_coordinator::SessionOptionCatalog;
use crate::session_coordinator::{
    SessionCoordinatorHandle, SessionCoordinatorParams, builtin_option_definitions,
};
use crate::template;
use crate::tools::{
    DescriptionContext, FileReadTracker, LocalTools, ToolAudience, ToolFilter, ToolRegistry,
};
use crate::{
    Agent, AgentConfig, AgentEvent, AgentId, AgentInput, AgentMode, AgentParams, AgentRunParams,
    Envelope, EventSender, McpHandle, McpSession, PermissionsConfig, SessionMailbox,
    ToolOutputLines, TurnFailure, TurnId, TurnOutcome,
};

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
    /// Plugin-registered session options. A print-mode run still needs a
    /// coordinator: tools read their options through one, and
    /// `SessionMailbox::notify` resolves through one.
    pub session_options: SessionOptionCatalog,
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

pub fn spawn(
    params: HeadlessParams,
) -> Result<HeadlessHandle, crate::session_coordinator::SessionCoordinatorError> {
    spawn_with_session_id(params, MakiId::generate())
}

fn spawn_with_session_id(
    params: HeadlessParams,
    session_id: MakiId,
) -> Result<HeadlessHandle, crate::session_coordinator::SessionCoordinatorError> {
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
        )
    });
    if let Some(append) = &params.append_system_prompt {
        system.push('\n');
        system.push_str(append);
    }

    let mcp = params.mcp_handle.clone().map(|h| McpSession::new(h, &[]));
    let tool_names = advertised_tool_names(&tools, mcp.as_ref());

    let (raw_tx, event_rx) = flume::unbounded::<Envelope>();

    let session_ref = SessionRef::from(session_id);
    let session_ref_clone = session_ref.clone();
    let mailbox = SessionMailbox::new(session_id);
    // A print-mode session is one turn and is never restored, so the
    // checkpoint is a no-op and no options are persisted -- but the
    // coordinator must exist, or every option read and every mailbox
    // notification in this run fails with `session not live`.
    let coordinator = SessionCoordinatorHandle::register(SessionCoordinatorParams {
        session_id,
        catalog: params.session_options.clone(),
        definitions: builtin_option_definitions(
            Arc::from(params.model.spec().as_str()),
            [Arc::from(params.model.spec().as_str())],
            params.permissions_config.yolo,
            false,
            workflow,
            params.input.thinking,
        ),
        persisted_options: Default::default(),
        history: Vec::new(),
        model: Arc::from(params.model.spec().as_str()),
        cwd: params.initial_wd.clone(),
        model_policy: Arc::clone(&params.model_policy),
        // A print run resolves its provider once, up front: there is no
        // mechanism to swap either mid-run, so both adoptions are refused
        // rather than silently accepted and ignored.
        model_adopter: Arc::new(|_: Model| {
            Box::pin(async { Err(Arc::from("model cannot be changed in print mode")) })
                as crate::session_coordinator::ModelAdoptionFuture
        }),
        directory_adopter: Arc::new(|_: PathBuf| {
            Box::pin(async { Err(Arc::from("directory cannot be changed in print mode")) })
                as crate::session_coordinator::DirectoryAdoptionFuture
        }),
        checkpoint: Arc::new(
            |request: maki_storage::checkpoint::CheckpointRequest<
                crate::session_coordinator::SessionCheckpoint,
            >| {
                Box::pin(async move {
                    Ok(maki_storage::checkpoint::CheckpointAck {
                        session_id: request.session_id,
                        version: request.version,
                    })
                }) as maki_storage::checkpoint::CheckpointFuture
            },
        ),
        mailbox: mailbox.clone(),
    })?;
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
                        let _ = coordinator.close().await;
                        return;
                    }
                };
            let mut history = History::new(Vec::new());
            let mut agent = Agent::new(
                AgentParams {
                    settings_source: None,
                    tool_builder: None,
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
                    managed_turn: None,
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
            let _ = coordinator.close().await;
        }
    });

    Ok(HeadlessHandle {
        event_rx,
        tool_names,
        session_id: session_ref,
        cwd: working_dir,
        task,
    })
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ManualCompactionEvent {
    Started,
    Completed,
    Cancelled,
    Failed(String),
}

pub enum InteractiveControl {
    Compact(flume::Sender<Result<(), String>>),
    ManualCompaction {
        output: flume::Sender<ManualCompactionEvent>,
        cancel: CancelToken,
        lease_committer: Option<crate::session_coordinator::SessionLeaseCommitter>,
    },
    Reset(flume::Sender<Result<(), String>>),
    ChangeDirectory {
        path: PathBuf,
        reply: flume::Sender<Result<PathBuf, String>>,
    },
    IsolatedTurn {
        question: String,
        images: Vec<maki_providers::ImageSource>,
        output: flume::Sender<agent::isolated_turn::IsolatedTurnEvent>,
        cancel: CancelToken,
    },
}

struct InteractiveControlContext<'a> {
    session_id: MakiId,
    history: &'a mut History,
    model: &'a Model,
    provider: &'a dyn Provider,
    raw_tx: &'a flume::Sender<Envelope>,
    run_id: u64,
    config: &'a AgentConfig,
}

/// Finish a manual compaction: persist the compacted history, and only then
/// report success. A compaction that reported `Completed` before the write
/// landed would promise durability it does not have, so persistence failure --
/// like compaction failure -- rolls the in-memory history back to `previous`
/// and reports the error instead. A failed compaction is never persisted.
async fn settle_manual_compaction<P, F>(
    compacted: Result<(), String>,
    history: &mut History,
    previous: Vec<Message>,
    cancel: &CancelToken,
    persist: P,
) -> ManualCompactionEvent
where
    P: FnOnce(Vec<Message>) -> F,
    F: Future<Output = Result<(), String>>,
{
    let result = match compacted {
        Ok(()) => persist(history.as_slice().to_vec()).await,
        Err(error) => Err(error),
    };
    match result {
        Ok(()) => ManualCompactionEvent::Completed,
        Err(_) if cancel.is_cancelled() => {
            history.replace(previous);
            ManualCompactionEvent::Cancelled
        }
        Err(error) => {
            history.replace(previous);
            ManualCompactionEvent::Failed(error)
        }
    }
}

async fn persist_history(session_id: MakiId, history: &[Message]) -> Result<(), String> {
    crate::session_coordinator::SessionCoordinatorHandle::resolve(session_id)
        .map_err(|error| error.to_string())?
        .replace_history(history.to_vec())
        .await
        .map_err(|error| error.to_string())
}

async fn checkpoint_and_forward_terminal(
    committer: Option<crate::session_coordinator::SessionLeaseCommitter>,
    session_id: MakiId,
    history: &[Message],
    timeout: std::time::Duration,
    terminal: Option<Envelope>,
    raw_tx: &flume::Sender<Envelope>,
    run_id: u64,
) -> Result<(), String> {
    let commit = match committer {
        Some(committer) => Some(
            committer
                .begin_history_commit(history.to_vec(), Some(timeout))
                .await,
        ),
        None => None,
    };
    let result = match commit {
        Some(Ok(commit)) => commit.wait().await.map_err(|error| error.to_string()),
        Some(Err(error)) => Err(error.to_string()),
        None => persist_history(session_id, history).await,
    };
    match &result {
        Ok(()) => {
            if let Some(terminal) = terminal {
                let _ = raw_tx.send(terminal);
            }
        }
        Err(error) => {
            let _ = EventSender::new(raw_tx.clone(), run_id).send(AgentEvent::ControlError {
                message: format!("failed to checkpoint completed turn: {error}"),
            });
        }
    }
    result
}

enum InteractiveWake {
    Input(AgentInput),
    Control(InteractiveControl),
}

async fn collect_turn_events(
    turn_event_rx: Receiver<Envelope>,
    raw_tx: flume::Sender<Envelope>,
) -> Option<Envelope> {
    while let Ok(envelope) = turn_event_rx.recv_async().await {
        if envelope.subagent.is_none() && matches!(envelope.event, AgentEvent::TurnOutcome(_)) {
            return Some(envelope);
        }
        let _ = raw_tx.send(envelope);
    }
    None
}

async fn receive_wake_and_refresh(
    input_rx: &Receiver<AgentInput>,
    control_rx: &Receiver<InteractiveControl>,
    shared_model: &crate::SharedModel,
    provider: &mut Arc<dyn Provider>,
    model: &mut Model,
) -> Option<InteractiveWake> {
    let wake = if let Ok(control) = control_rx.try_recv() {
        Some(InteractiveWake::Control(control))
    } else {
        future::or(
            async { input_rx.recv_async().await.map(InteractiveWake::Input) },
            async { control_rx.recv_async().await.map(InteractiveWake::Control) },
        )
        .await
        .ok()
    };
    use crate::ModelSource;
    if let Some((current_provider, current_model)) = shared_model.current()
        && current_model.spec() != model.spec()
    {
        *provider = current_provider;
        *model = current_model;
    }
    wake
}

async fn apply_interactive_control(
    control: InteractiveControl,
    context: InteractiveControlContext<'_>,
) {
    let InteractiveControlContext {
        session_id,
        history,
        model,
        provider,
        raw_tx,
        run_id,
        config,
    } = context;
    let result = match &control {
        InteractiveControl::Compact(_) => {
            let previous = history.as_slice().to_vec();
            match agent::compact(
                provider,
                model,
                history,
                &EventSender::new(raw_tx.clone(), run_id),
                &CancelToken::none(),
                config,
                Some(&SessionRef::from(session_id)),
            )
            .await
            {
                Ok(()) => persist_history(session_id, history.as_slice())
                    .await
                    .inspect_err(|_| history.replace(previous)),
                Err(error) => Err(error.to_string()),
            }
        }
        InteractiveControl::ManualCompaction { .. } => {
            Err("manual compaction was not intercepted by the session loop".into())
        }
        InteractiveControl::Reset(_) => {
            let previous = history.as_slice().to_vec();
            history.replace(Vec::new());
            persist_history(session_id, history.as_slice())
                .await
                .inspect_err(|_| history.replace(previous))
        }
        InteractiveControl::ChangeDirectory { .. } => {
            Err("directory adoption was not intercepted by the session loop".into())
        }
        InteractiveControl::IsolatedTurn { .. } => {
            Err("isolated turn was not intercepted by the session loop".into())
        }
    };
    match control {
        InteractiveControl::Compact(reply) | InteractiveControl::Reset(reply) => {
            let _ = reply.send(result);
        }
        InteractiveControl::ChangeDirectory { reply, .. } => {
            let _ = reply.send(Err(
                "directory adoption was not intercepted by the session loop".into(),
            ));
        }
        InteractiveControl::ManualCompaction { .. } | InteractiveControl::IsolatedTurn { .. } => {}
    }
}

pub struct InteractiveHandle {
    pub event_rx: Receiver<Envelope>,
    pub tool_names: Vec<String>,
    pub input_tx: flume::Sender<AgentInput>,
    pub answer_tx: flume::Sender<String>,
    pub cancel_tx: flume::Sender<()>,
    pub model_tx: flume::Sender<Model>,
    pub control_tx: flume::Sender<InteractiveControl>,
    /// Install a model here to change it. Adoption is a store, so it lands on
    /// the run's next request rather than waiting for the turn to end, and it
    /// never blocks a caller behind a running turn.
    pub model: crate::SharedModel,
    pub session_id: SessionRef,
    pub mailbox: SessionMailbox,
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
    let mailbox = SessionMailbox::new(session_id);
    let handle_mailbox = mailbox.clone();
    // Seeded by the task once its provider exists; until then it reports no
    // change, which is what an unstarted session should say.
    let shared_model = crate::SharedModel::default();
    let handle_model = shared_model.clone();

    let working_dir = params.initial_wd.to_string_lossy().into_owned();
    let permissions = Arc::new(PermissionManager::new(
        params.permissions_config.clone(),
        params.initial_wd,
        Arc::clone(&params.plugin_rules),
    ));
    permissions.set_yolo(params.yolo);
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
            shared_model.install(Arc::clone(&provider), model.clone());

            let mut history = History::restored(params.initial_history);
            let mut working_dir = PathBuf::from(working_dir);
            let permissions = permissions;
            let agent_id = AgentId::generate();
            let mut run_id: u64 = 0;

            loop {
                let wake = receive_wake_and_refresh(
                    &input_rx,
                    &control_rx,
                    &shared_model,
                    &mut provider,
                    &mut model,
                )
                .await;
                let input = match wake {
                    Some(InteractiveWake::Input(input)) => input,
                    Some(InteractiveWake::Control(InteractiveControl::ChangeDirectory {
                        path,
                        reply,
                    })) => {
                        let result = path
                            .canonicalize()
                            .and_then(|canonical| {
                                if canonical.is_dir() {
                                    Ok(canonical)
                                } else {
                                    Err(std::io::Error::new(
                                        std::io::ErrorKind::NotADirectory,
                                        "path is not a directory",
                                    ))
                                }
                            })
                            .inspect(|canonical| {
                                working_dir = canonical.clone();
                                permissions.set_cwd(canonical.clone());
                            })
                            .map_err(|error| error.to_string());
                        let _ = reply.send(result);
                        continue;
                    }
                    Some(InteractiveWake::Control(InteractiveControl::ManualCompaction {
                        output,
                        cancel,
                        lease_committer,
                    })) => {
                        let _ = output.send(ManualCompactionEvent::Started);
                        let previous = history.as_slice().to_vec();
                        let (private_tx, _private_rx) = flume::unbounded();
                        let result = agent::compact(
                            &*provider,
                            &model,
                            &mut history,
                            &EventSender::new(private_tx, run_id),
                            &cancel,
                            &params.config,
                            Some(&session_ref_clone),
                        )
                        .await
                        .map_err(|error| error.to_string());
                        let terminal = settle_manual_compaction(
                            result,
                            &mut history,
                            previous,
                            &cancel,
                            |compacted| async move {
                                match lease_committer {
                                    Some(committer) => committer
                                        .commit_history(compacted)
                                        .await
                                        .map_err(|error| error.to_string()),
                                    None => persist_history(session_id, &compacted).await,
                                }
                            },
                        )
                        .await;
                        let _ = output.send(terminal);
                        continue;
                    }
                    Some(InteractiveWake::Control(InteractiveControl::IsolatedTurn {
                        question,
                        images,
                        output,
                        cancel,
                    })) => {
                        let vars = template::env_vars_for(&working_dir);
                        let instructions = agent::load_instructions(&working_dir.to_string_lossy());
                        let mut system =
                            params.system_prompt_override.clone().unwrap_or_else(|| {
                                agent::build_system_prompt(
                                    &vars,
                                    &modes,
                                    &AgentMode::Build,
                                    &instructions.text,
                                    &params.prompt_slots,
                                )
                            });
                        if let Some(append) = &params.append_system_prompt {
                            system.push('\n');
                            system.push_str(append);
                        }
                        agent::isolated_turn::run_isolated_turn(
                            agent::isolated_turn::IsolatedTurnRequest {
                                provider: Arc::clone(&provider),
                                model: model.clone(),
                                history: history.as_slice().to_vec(),
                                system,
                                question,
                                images,
                                session_id: Some(SessionRef::from(session_id)),
                                cancel,
                            },
                            output,
                        )
                        .await;
                        continue;
                    }
                    Some(InteractiveWake::Control(control)) => {
                        apply_interactive_control(
                            control,
                            InteractiveControlContext {
                                session_id,
                                history: &mut history,
                                model: &model,
                                provider: &*provider,
                                raw_tx: &raw_tx,
                                run_id,
                                config: &params.config,
                            },
                        )
                        .await;
                        continue;
                    }
                    None => break,
                };
                let turn_id = TurnId::generate();
                let lease_committer = input.lease_committer.clone();
                let operation_cancel = input.cancel.clone();
                let (cancel_task, cancel) = match operation_cancel {
                    Some(cancel) => (None, cancel),
                    None => {
                        let (trigger, cancel) = CancelToken::new();
                        let cancel_rx = cancel_rx.clone();
                        let task = smol::spawn(async move {
                            if cancel_rx.recv_async().await.is_ok() {
                                trigger.cancel();
                            }
                        });
                        (Some(task), cancel)
                    }
                };

                // MCP connects in the background, so a prompt that beats it waits
                // here instead of shipping a turn without the MCP tools. The wait
                // is racing cancel: a slow server must not pin the whole session.
                if let Some(mcp) = &mcp {
                    let _ = cancel.race(mcp.ready()).await;
                }

                let (turn_event_tx, turn_event_rx) = flume::unbounded::<Envelope>();
                let terminal_task = smol::spawn(collect_turn_events(turn_event_rx, raw_tx.clone()));
                let event_tx = EventSender::new(turn_event_tx.clone(), run_id);
                let error_tx = EventSender::new(raw_tx.clone(), run_id);

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
                            // The run reads the shared source, so an adoption
                            // that stops here is discarded.
                            shared_model.install(Arc::clone(&provider), model.clone());
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
                            if let Some(cancel_task) = cancel_task {
                                cancel_task.cancel().await;
                            }
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
                    )
                });
                if let Some(append) = &params.append_system_prompt {
                    system.push('\n');
                    system.push_str(append);
                }

                while answer_rx.lock().await.try_recv().is_ok() {}

                let mut agent = Agent::new(
                    AgentParams {
                        // Session options travel through the coordinator, so
                        // fast and workflow reach a run in flight the same way
                        // the model does.
                        settings_source: Some(Arc::new(crate::SessionRunSettings {
                            model: Arc::new(shared_model.clone()),
                            session_id,
                        })),
                        // Without this a workflow toggle would flip the flag
                        // while the interpreter kept the schema built for the
                        // old one, and a model switch would carry the previous
                        // model's tool descriptions.
                        tool_builder: Some({
                            let vars = turn_vars.clone();
                            let config = params.config.clone();
                            let excluded = params.excluded_tools.clone();
                            let registry = Arc::clone(ToolRegistry::global_arc());
                            Arc::new(move |model: &Model, workflow: bool| {
                                tool_definitions(
                                    &vars, model, &config, &excluded, workflow, &registry,
                                )
                            })
                        }),
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
                        managed_turn: None,
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

                let outcome = agent.run(turn_id, input).await;
                drop(agent);
                drop(turn_event_tx);
                if let Some(cancel_task) = cancel_task {
                    cancel_task.cancel().await;
                }
                let terminal = terminal_task.await;

                if let TurnOutcome::Failed { failure, .. } = &outcome {
                    error!(error = %failure.user_message, "agent error");
                }
                if let Err(error) = checkpoint_and_forward_terminal(
                    lease_committer,
                    session_id,
                    history.as_slice(),
                    params.timeouts.low_speed,
                    terminal,
                    &raw_tx,
                    run_id,
                )
                .await
                {
                    error!(%error, %session_id, "failed to checkpoint completed turn");
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
        model: handle_model,
        session_id: session_ref,
        mailbox: handle_mailbox,
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
    use super::*;

    fn test_params() -> HeadlessParams {
        HeadlessParams {
            model: Model::from_spec("anthropic/claude-sonnet-4-20250514").unwrap(),
            config: AgentConfig::default(),
            permissions_config: PermissionsConfig::default(),
            timeouts: Timeouts::default(),
            input: AgentInput {
                message: "hello".into(),
                mode: AgentMode::Build,
                images: Vec::new(),
                preamble: Vec::new(),
                thinking: Default::default(),
                fast: false,
                workflow: false,
                prompt: None,
                cancel: None,
                lease_committer: None,
            },
            prompt_slots: ResolvedSlots::default(),
            excluded_tools: Vec::new(),
            mcp_handle: None,
            initial_wd: PathBuf::from("/tmp"),
            system_prompt_override: None,
            append_system_prompt: None,
            model_policy: Arc::default(),
            plugin_rules: Arc::default(),
            modes: Arc::default(),
            session_options: Default::default(),
        }
    }

    /// A print-mode run needs a coordinator like any other session: tools read
    /// their options through one, and `SessionMailbox::notify` resolves through
    /// one. Without it every option read fails with `session not live` and
    /// `bash` cannot run at all.
    #[test]
    fn spawn_registers_a_resolvable_coordinator() {
        let handle = spawn(test_params()).unwrap();

        let session_id = handle.session_id.id();
        let coordinator = SessionCoordinatorHandle::resolve(session_id)
            .expect("print mode must register a coordinator for its session");
        assert_eq!(coordinator.read().session_id(), session_id);
        // The mailbox the coordinator hands out must be the one the run polls,
        // or notifications land nowhere.
        SessionMailbox::notify(session_id, "ping".into(), true)
            .expect("notify resolves through the registered coordinator");

        drop(handle.task);
        let _ = futures_lite::future::block_on(coordinator.close());
    }

    #[test]
    fn spawn_stops_when_coordinator_registration_fails() {
        let session_id = MakiId::generate();
        let existing = SessionCoordinatorHandle::register(SessionCoordinatorParams {
            session_id,
            catalog: Default::default(),
            definitions: Vec::new(),
            persisted_options: Default::default(),
            history: Vec::new(),
            model: Arc::from("test"),
            cwd: PathBuf::from("/tmp"),
            model_policy: Arc::default(),
            model_adopter: Arc::new(|_| {
                Box::pin(async { Ok(()) }) as crate::session_coordinator::ModelAdoptionFuture
            }),
            directory_adopter: Arc::new(|path| {
                Box::pin(async move { Ok(path) })
                    as crate::session_coordinator::DirectoryAdoptionFuture
            }),
            checkpoint: Arc::new(
                |request: maki_storage::checkpoint::CheckpointRequest<
                    crate::session_coordinator::SessionCheckpoint,
                >| {
                    Box::pin(async move {
                        Ok(maki_storage::checkpoint::CheckpointAck {
                            session_id: request.session_id,
                            version: request.version,
                        })
                    }) as maki_storage::checkpoint::CheckpointFuture
                },
            ),
            mailbox: SessionMailbox::new(session_id),
        })
        .unwrap();

        let error = match spawn_with_session_id(test_params(), session_id) {
            Ok(_) => panic!("duplicate coordinator registration succeeded"),
            Err(error) => error,
        };
        assert_eq!(
            error,
            crate::session_coordinator::SessionCoordinatorError::DuplicateSession(session_id)
        );
        futures_lite::future::block_on(existing.close()).unwrap();
    }

    /// `/compact` reports success only when the compacted history is durable.
    /// These pin the order: nothing reports `Completed` unless the persist step
    /// ran first and returned `Ok`, and every other path rolls the in-memory
    /// history back to what it was before the compaction.
    fn as_json(messages: &[Message]) -> Value {
        serde_json::to_value(messages).unwrap()
    }

    type PersistLog = Arc<std::sync::Mutex<Vec<Vec<Message>>>>;

    struct CompactionFixture {
        history: History,
        previous: Vec<Message>,
        persisted: PersistLog,
    }

    fn compaction_fixture() -> CompactionFixture {
        CompactionFixture {
            history: History::new(vec![Message::user("compacted".into())]),
            previous: vec![Message::user("original".into())],
            persisted: Arc::new(std::sync::Mutex::new(Vec::new())),
        }
    }

    #[test]
    fn manual_compaction_persists_before_reporting_success() {
        smol::block_on(async {
            let CompactionFixture {
                mut history,
                previous,
                persisted,
            } = compaction_fixture();
            let terminal = settle_manual_compaction(
                Ok(()),
                &mut history,
                previous,
                &CancelToken::none(),
                |compacted| {
                    let persisted = Arc::clone(&persisted);
                    async move {
                        persisted.lock().unwrap().push(compacted);
                        Ok(())
                    }
                },
            )
            .await;

            assert_eq!(terminal, ManualCompactionEvent::Completed);
            let persisted = persisted.lock().unwrap();
            assert_eq!(persisted.len(), 1, "persistence must run exactly once");
            assert_eq!(
                as_json(&persisted[0]),
                as_json(&[Message::user("compacted".into())]),
                "the compacted history is what must reach persistence"
            );
            assert_eq!(
                as_json(history.as_slice()),
                as_json(&[Message::user("compacted".into())])
            );
        });
    }

    #[test]
    fn failed_persistence_reports_failure_and_rolls_history_back() {
        smol::block_on(async {
            let CompactionFixture {
                mut history,
                previous,
                ..
            } = compaction_fixture();
            let terminal = settle_manual_compaction(
                Ok(()),
                &mut history,
                previous.clone(),
                &CancelToken::none(),
                |_| async { Err("save failed".to_owned()) },
            )
            .await;

            assert_eq!(
                terminal,
                ManualCompactionEvent::Failed("save failed".to_owned()),
                "a compaction nobody saved must not report success"
            );
            assert_eq!(as_json(history.as_slice()), as_json(&previous));
        });
    }

    #[test]
    fn a_failed_compaction_is_never_persisted() {
        smol::block_on(async {
            let CompactionFixture {
                mut history,
                previous,
                persisted,
            } = compaction_fixture();
            let terminal = settle_manual_compaction(
                Err("provider failed".to_owned()),
                &mut history,
                previous.clone(),
                &CancelToken::none(),
                |compacted| {
                    let persisted = Arc::clone(&persisted);
                    async move {
                        persisted.lock().unwrap().push(compacted);
                        Ok(())
                    }
                },
            )
            .await;

            assert_eq!(
                terminal,
                ManualCompactionEvent::Failed("provider failed".to_owned())
            );
            assert!(
                persisted.lock().unwrap().is_empty(),
                "a compaction that failed must not overwrite the saved history"
            );
            assert_eq!(as_json(history.as_slice()), as_json(&previous));
        });
    }

    #[test]
    fn cancelled_persistence_reports_cancellation_not_failure() {
        smol::block_on(async {
            let CompactionFixture {
                mut history,
                previous,
                ..
            } = compaction_fixture();
            let (trigger, cancel) = CancelToken::new();
            trigger.cancel();
            let terminal = settle_manual_compaction(
                Ok(()),
                &mut history,
                previous.clone(),
                &cancel,
                |_| async { Err("interrupted".to_owned()) },
            )
            .await;

            assert_eq!(terminal, ManualCompactionEvent::Cancelled);
            assert_eq!(as_json(history.as_slice()), as_json(&previous));
        });
    }

    struct TestProvider;

    impl maki_providers::provider::Provider for TestProvider {
        fn stream_message<'a>(
            &'a self,
            _: &'a Model,
            _: &'a [Message],
            _: &'a str,
            _: &'a Value,
            _: &'a flume::Sender<maki_providers::ProviderEvent>,
            _: maki_providers::RequestOptions,
            _: Option<&'a SessionRef>,
        ) -> maki_providers::provider::BoxFuture<
            'a,
            Result<maki_providers::StreamResponse, crate::AgentError>,
        > {
            Box::pin(std::future::pending())
        }

        fn list_models(
            &self,
        ) -> maki_providers::provider::BoxFuture<
            '_,
            Result<Vec<maki_providers::ModelInfo>, crate::AgentError>,
        > {
            Box::pin(async { Ok(Vec::new()) })
        }
    }

    #[test]
    fn turn_event_collection_finishes_with_retained_sender() {
        smol::block_on(async {
            let (turn_tx, turn_rx) = flume::unbounded();
            let retained_tx = turn_tx.clone();
            let (raw_tx, raw_rx) = flume::unbounded();
            let run_id = 17;
            let outcome = TurnOutcome::Completed {
                agent_id: AgentId::generate(),
                turn_id: TurnId::generate(),
                usage: TokenUsage::default(),
                num_turns: 1,
                reason: crate::DoneReason::EndTurn,
            };
            let collector = smol::spawn(collect_turn_events(turn_rx, raw_tx));

            turn_tx
                .send(Envelope {
                    event: AgentEvent::TextDelta {
                        text: "before terminal".into(),
                    },
                    subagent: None,
                    run_id,
                })
                .unwrap();
            turn_tx
                .send(Envelope {
                    event: AgentEvent::TurnOutcome(outcome.clone()),
                    subagent: None,
                    run_id,
                })
                .unwrap();
            drop(turn_tx);

            let terminal = collector
                .await
                .expect("root outcome must finish collection");
            assert_eq!(terminal.run_id, run_id);
            assert!(terminal.subagent.is_none());
            assert!(matches!(terminal.event, AgentEvent::TurnOutcome(got) if got == outcome));
            assert!(matches!(
                raw_rx.recv_async().await.unwrap(),
                Envelope {
                    event: AgentEvent::TextDelta { text },
                    subagent: None,
                    run_id: got_run_id,
                } if text == "before terminal" && got_run_id == run_id
            ));
            assert!(
                retained_tx
                    .send(Envelope {
                        event: AgentEvent::TextDelta {
                            text: "retained".into(),
                        },
                        subagent: None,
                        run_id,
                    })
                    .is_err()
            );
        });
    }

    #[test]
    fn terminal_waits_for_checkpoint_then_forwards() {
        smol::block_on(async {
            let session_id = MakiId::generate();
            let (checkpoint_seen_tx, checkpoint_seen_rx) = flume::bounded(1);
            let (release_tx, release_rx) = flume::bounded(1);
            let checkpoint: Arc<
                dyn maki_storage::checkpoint::CheckpointWriter<
                        crate::session_coordinator::SessionCheckpoint,
                    >,
            > = Arc::new(
                move |request: maki_storage::checkpoint::CheckpointRequest<
                    crate::session_coordinator::SessionCheckpoint,
                >| {
                    let checkpoint_seen_tx = checkpoint_seen_tx.clone();
                    let release_rx = release_rx.clone();
                    Box::pin(async move {
                        let history = request
                            .snapshot
                            .history
                            .as_ref()
                            .expect("history commits include history")
                            .as_ref()
                            .clone();
                        checkpoint_seen_tx.send_async(history).await.unwrap();
                        release_rx.recv_async().await.unwrap();
                        Ok(maki_storage::checkpoint::CheckpointAck {
                            session_id: request.session_id,
                            version: request.version,
                        })
                    }) as maki_storage::checkpoint::CheckpointFuture
                },
            );
            let coordinator = SessionCoordinatorHandle::register(SessionCoordinatorParams {
                session_id,
                catalog: Default::default(),
                definitions: builtin_option_definitions(
                    "test/model",
                    [Arc::from("test/model")],
                    false,
                    false,
                    false,
                    Default::default(),
                ),
                persisted_options: Default::default(),
                history: Vec::new(),
                model: Arc::from("test/model"),
                cwd: PathBuf::from("/project"),
                model_policy: Arc::default(),
                model_adopter: Arc::new(|_| {
                    Box::pin(async { Ok(()) }) as crate::session_coordinator::ModelAdoptionFuture
                }),
                directory_adopter: Arc::new(|path| {
                    Box::pin(async move { Ok(path) })
                        as crate::session_coordinator::DirectoryAdoptionFuture
                }),
                checkpoint,
                mailbox: SessionMailbox::new(session_id),
            })
            .unwrap();
            let lease = coordinator.acquire_lease().await.unwrap();
            let committer = lease.committer().unwrap();
            let outcome = TurnOutcome::Completed {
                agent_id: AgentId::generate(),
                turn_id: TurnId::generate(),
                usage: TokenUsage::default(),
                num_turns: 1,
                reason: crate::DoneReason::EndTurn,
            };
            let terminal = Envelope {
                event: AgentEvent::TurnOutcome(outcome.clone()),
                subagent: None,
                run_id: 0,
            };
            let (raw_tx, raw_rx) = flume::unbounded();
            let history = vec![Message::user("first".into())];
            let checkpoint_task = smol::spawn({
                let history = history.clone();
                async move {
                    checkpoint_and_forward_terminal(
                        Some(committer),
                        session_id,
                        &history,
                        std::time::Duration::from_secs(1),
                        Some(terminal),
                        &raw_tx,
                        0,
                    )
                    .await
                }
            });

            let checkpoint_history = checkpoint_seen_rx.recv_async().await.unwrap();
            assert_eq!(as_json(&checkpoint_history), as_json(&history));
            assert!(raw_rx.is_empty());

            release_tx.send_async(()).await.unwrap();
            assert!(checkpoint_task.await.is_ok());
            let forwarded = raw_rx.recv_async().await.unwrap();
            assert!(
                matches!(forwarded.event, AgentEvent::TurnOutcome(got) if got == outcome)
                    && forwarded.subagent.is_none()
                    && forwarded.run_id == 0
            );
            assert_eq!(
                as_json(coordinator.read().history().as_ref()),
                as_json(&history)
            );
            drop(lease);

            let next = coordinator.acquire_lease().await.unwrap();
            assert_eq!(as_json(next.read().history().as_ref()), as_json(&history));
            drop(next);
            coordinator.close().await.unwrap();
        });
    }

    #[test]
    fn checkpoint_failure_after_terminal_is_forwarded_to_frontend() {
        smol::block_on(async {
            const SAVE_ERROR: &str = "save failed";

            let session_id = MakiId::generate();
            let checkpoint: Arc<
                dyn maki_storage::checkpoint::CheckpointWriter<
                        crate::session_coordinator::SessionCheckpoint,
                    >,
            > = Arc::new(
                |request: maki_storage::checkpoint::CheckpointRequest<
                    crate::session_coordinator::SessionCheckpoint,
                >| {
                    Box::pin(async move {
                        Err(maki_storage::checkpoint::CheckpointError::Save {
                            session_id: request.session_id,
                            message: Arc::from(SAVE_ERROR),
                        })
                    }) as maki_storage::checkpoint::CheckpointFuture
                },
            );
            let coordinator = SessionCoordinatorHandle::register(SessionCoordinatorParams {
                session_id,
                catalog: Default::default(),
                definitions: builtin_option_definitions(
                    "test/model",
                    [Arc::from("test/model")],
                    false,
                    false,
                    false,
                    Default::default(),
                ),
                persisted_options: Default::default(),
                history: Vec::new(),
                model: Arc::from("test/model"),
                cwd: PathBuf::from("/project"),
                model_policy: Arc::default(),
                model_adopter: Arc::new(|_| {
                    Box::pin(async { Ok(()) }) as crate::session_coordinator::ModelAdoptionFuture
                }),
                directory_adopter: Arc::new(|path| {
                    Box::pin(async move { Ok(path) })
                        as crate::session_coordinator::DirectoryAdoptionFuture
                }),
                checkpoint,
                mailbox: SessionMailbox::new(session_id),
            })
            .unwrap();
            let outcome = TurnOutcome::Completed {
                agent_id: AgentId::generate(),
                turn_id: TurnId::generate(),
                usage: TokenUsage::default(),
                num_turns: 1,
                reason: crate::DoneReason::EndTurn,
            };
            let terminal = Envelope {
                event: AgentEvent::TurnOutcome(outcome.clone()),
                subagent: None,
                run_id: 7,
            };
            let (raw_tx, raw_rx) = flume::unbounded();

            let result = checkpoint_and_forward_terminal(
                None,
                session_id,
                &[Message::user("first".into())],
                std::time::Duration::from_secs(1),
                Some(terminal),
                &raw_tx,
                7,
            )
            .await;

            assert!(result.is_err());
            assert!(matches!(
                raw_rx.recv_async().await.unwrap(),
                Envelope {
                    event: AgentEvent::ControlError { message },
                    run_id: 7,
                    ..
                } if message.contains(SAVE_ERROR)
            ));
            assert!(raw_rx.is_empty());
            coordinator.close().await.unwrap();
        });
    }

    #[test]
    fn wake_refreshes_model_after_idle_wait() {
        smol::block_on(async {
            let (_input_tx, input_rx) = flume::unbounded();
            let (control_tx, control_rx) = flume::unbounded();
            let initial_model = Model::from_spec("anthropic/claude-sonnet-4-20250514").unwrap();
            let adopted_model = Model::from_spec("anthropic/claude-opus-4-8").unwrap();
            let adopted_provider: Arc<dyn Provider> = Arc::new(TestProvider);
            let mut provider: Arc<dyn Provider> = Arc::new(TestProvider);
            let mut model = initial_model;
            let shared_model = crate::SharedModel::default();
            let mut wake = Box::pin(receive_wake_and_refresh(
                &input_rx,
                &control_rx,
                &shared_model,
                &mut provider,
                &mut model,
            ));
            assert!(futures_lite::future::poll_once(&mut wake).await.is_none());
            shared_model.install(Arc::clone(&adopted_provider), adopted_model.clone());
            control_tx
                .send(InteractiveControl::Reset(flume::bounded(1).0))
                .unwrap();
            let got_control = matches!(
                wake.as_mut().await,
                Some(InteractiveWake::Control(InteractiveControl::Reset(_)))
            );
            drop(wake);

            assert!(got_control);
            assert_eq!(model.spec(), adopted_model.spec());
            assert!(Arc::ptr_eq(&provider, &adopted_provider));
        });
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
