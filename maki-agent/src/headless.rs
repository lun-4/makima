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
    let mailbox = SessionMailbox::new(session_id);
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
    AdoptModel {
        model: Model,
        reply: flume::Sender<Result<(), String>>,
    },
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

async fn persist_history(session_id: MakiId, history: &[Message]) -> Result<(), String> {
    crate::session_coordinator::SessionCoordinatorHandle::resolve(session_id)
        .map_err(|error| error.to_string())?
        .replace_history(history.to_vec())
        .await
        .map_err(|error| error.to_string())
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
        InteractiveControl::AdoptModel { .. } => {
            Err("model adoption was not intercepted by the session loop".into())
        }
        InteractiveControl::ChangeDirectory { .. } => {
            Err("directory adoption was not intercepted by the session loop".into())
        }
        InteractiveControl::IsolatedTurn { .. } => {
            Err("isolated turn was not intercepted by the session loop".into())
        }
    };
    match control {
        InteractiveControl::Compact(reply)
        | InteractiveControl::Reset(reply)
        | InteractiveControl::AdoptModel { reply, .. } => {
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
                    Some(Wake::Control(InteractiveControl::AdoptModel {
                        model: mut candidate,
                        reply,
                    })) => {
                        let result =
                            match provider::from_model_async(&mut candidate, params.timeouts).await
                            {
                                Ok(adopted) => {
                                    provider = Arc::from(adopted);
                                    model = candidate;
                                    Ok(())
                                }
                                Err(error) => Err(error.user_message()),
                            };
                        let _ = reply.send(result);
                        continue;
                    }
                    Some(Wake::Control(InteractiveControl::ChangeDirectory { path, reply })) => {
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
                    Some(Wake::Control(InteractiveControl::ManualCompaction {
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
                        )
                        .await
                        .map_err(|error| error.to_string());
                        let result = match result {
                            Ok(()) => match lease_committer {
                                Some(committer) => committer
                                    .commit_history(history.as_slice().to_vec())
                                    .await
                                    .map_err(|error| error.to_string()),
                                None => persist_history(session_id, history.as_slice()).await,
                            },
                            Err(error) => Err(error),
                        };
                        let terminal = match result {
                            Ok(()) => ManualCompactionEvent::Completed,
                            Err(_) if cancel.is_cancelled() => {
                                history.replace(previous);
                                ManualCompactionEvent::Cancelled
                            }
                            Err(error) => {
                                history.replace(previous);
                                ManualCompactionEvent::Failed(error)
                            }
                        };
                        let _ = output.send(terminal);
                        continue;
                    }
                    Some(Wake::Control(InteractiveControl::IsolatedTurn {
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
                                    &model,
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
                    Some(Wake::Control(control)) => {
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

                let (turn_event_tx, turn_event_rx) = flume::unbounded::<Envelope>();
                let terminal_task = smol::spawn({
                    let raw_tx = raw_tx.clone();
                    async move {
                        let mut terminal = None;
                        while let Ok(envelope) = turn_event_rx.recv_async().await {
                            if matches!(envelope.event, AgentEvent::TurnOutcome(_)) {
                                terminal = Some(envelope);
                            } else {
                                let _ = raw_tx.send(envelope);
                            }
                        }
                        terminal
                    }
                });
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

                let outcome = agent.run(turn_id, input).await;
                drop(agent);
                drop(turn_event_tx);
                cancel_task.cancel().await;
                let terminal = terminal_task.await;

                if let TurnOutcome::Failed { failure, .. } = &outcome {
                    error!(error = %failure.user_message, "agent error");
                }
                let checkpoint = match lease_committer {
                    Some(committer) => committer
                        .commit_history(history.as_slice().to_vec())
                        .await
                        .map_err(|error| error.to_string()),
                    None => persist_history(session_id, history.as_slice()).await,
                };
                match checkpoint {
                    Ok(()) => {
                        if let Some(terminal) = terminal {
                            let _ = raw_tx.send(terminal);
                        }
                    }
                    Err(error) => {
                        error!(%error, %session_id, "failed to checkpoint completed turn");
                        let _ = error_tx.send(AgentEvent::ControlError { message: error });
                    }
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
