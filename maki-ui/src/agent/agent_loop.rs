//! The TUI's root-actor backend.
//!
//! Implements [`ActorBackend`](maki_agent::actor::ActorBackend) for the root
//! agent. The actor owns history, lifecycle, queueing, cancellation, and
//! retained outcomes; this struct owns only the dynamic TUI preparation
//! (initialization, cwd/instruction reload, MCP prompt expansion, prompt
//! slots, model/tools, the answer receiver) and constructs the transient
//! [`Agent`] for each executed turn.
//!
//! `run_id` is an `Arc<AtomicU64>` shared with `AgentHandles`; the app bumps
//! it on each run and the backend reads it to stamp events that arrive with
//! no correlation (standalone compacts).

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use arc_swap::ArcSwap;
use maki_agent::actor::{ActorBackend, BackendResult, ControlWork, TurnContext, WorkKind};
use maki_agent::mcp::config::McpServerStatus;
use maki_agent::mcp::{McpHandle, McpSession};
use maki_agent::permissions::PermissionManager;
use maki_agent::template;
use maki_agent::template::Vars;
use maki_agent::tools::{FileReadTracker, QuestionMode, RequestTools, ToolAudience, ToolRegistry};
use maki_agent::{
    Agent, AgentConfig, AgentEvent, AgentId, AgentInput, AgentParams, AgentRunParams, CancelMap,
    CancelToken, Envelope, EventSender, History, Instructions, McpCommand, PromptRole,
    SessionMailbox, ToolOutputLines, TurnCancellationReason, TurnId, TurnOutcome,
};
use maki_config::ModelPolicy;
use maki_lua::EventHandle;
use maki_providers::{AgentError, Message, Model};
use maki_storage::id::SessionRef;
use tracing::{info, warn};

use super::ProviderSlot;
use super::SystemPromptOverride;

/// Correlation prefix stamped on TUI root/turn admissions. Parsed back into a
/// run id for event envelope correlation.
pub(crate) const ROOT_CORRELATION_PREFIX: &str = "r";
const PROMPT_SLOTS_CLOSED: &str = "the Lua plugin runtime stopped before returning prompt slots";
const PROMPT_INPUTS_CLOSED: &str = "prompt input preparation stopped before it finished";

/// Parses an actor correlation string back into the TUI run id.
pub(crate) fn correlation_to_run_id(correlation: &str) -> u64 {
    correlation
        .strip_prefix(ROOT_CORRELATION_PREFIX)
        .and_then(|rest| rest.parse().ok())
        .unwrap_or_default()
}

/// The TUI's root-actor backend.
pub(crate) struct TuiActorBackend {
    agent_id: AgentId,
    model_slot: Arc<ProviderSlot>,
    cwd: Arc<ArcSwap<PathBuf>>,
    config: AgentConfig,
    tool_output_lines: ToolOutputLines,
    btw_system: Arc<ArcSwap<String>>,
    permissions: Arc<PermissionManager>,
    file_tracker: Arc<FileReadTracker>,
    agent_tx: flume::Sender<Envelope>,
    answer_rx: Arc<async_lock::Mutex<flume::Receiver<String>>>,
    session_id: Option<SessionRef>,
    mailbox: Option<SessionMailbox>,
    timeouts: maki_providers::Timeouts,
    lua_handle: EventHandle,
    subagent_cancels: Arc<CancelMap<String>>,
    model_policy: Arc<ModelPolicy>,
    system_prompt: SystemPromptOverride,
    file_write_locks: Arc<maki_agent::tools::FileWriteLocks>,
    /// Live MCP session; recreated per spawn so deferred tools stay fresh.
    mcp: Option<McpSession>,
    /// Startup cancellation: races env/instruction/MCP initialization.
    init_cancel: CancelToken,
    initialized: bool,
    /// App-visible current run id. Bumped by the app on each run; the backend
    /// reads it to stamp compact/control events that carry no correlation.
    run_id: Arc<AtomicU64>,
    /// Signals the drain driver (in `AgentHandles`) that one work item
    /// finished, so it can publish `QueueDrained` under the actor queue lock.
    drain_tx: flume::Sender<u64>,
    vars: Vars,
    instructions: Instructions,
    tools: RequestTools,
}

#[allow(clippy::too_many_arguments)]
pub(super) fn new_backend(
    agent_id: AgentId,
    model_slot: Arc<ProviderSlot>,
    cwd: Arc<ArcSwap<PathBuf>>,
    config: AgentConfig,
    tool_output_lines: ToolOutputLines,
    btw_system: Arc<ArcSwap<String>>,
    mcp_handle: Option<McpHandle>,
    initial_history: &[Message],
    permissions: Arc<PermissionManager>,
    agent_tx: flume::Sender<Envelope>,
    answer_rx: flume::Receiver<String>,
    session_id: Option<SessionRef>,
    mailbox: Option<SessionMailbox>,
    timeouts: maki_providers::Timeouts,
    lua_handle: EventHandle,
    subagent_cancels: Arc<CancelMap<String>>,
    model_policy: Arc<ModelPolicy>,
    system_prompt: SystemPromptOverride,
    file_write_locks: Arc<maki_agent::tools::FileWriteLocks>,
    init_cancel: CancelToken,
    drain_tx: flume::Sender<u64>,
    run_id: Arc<AtomicU64>,
) -> TuiActorBackend {
    let mcp = mcp_handle.map(|h| McpSession::new(h, initial_history));
    TuiActorBackend {
        agent_id,
        model_slot,
        cwd,
        config,
        tool_output_lines,
        btw_system,
        permissions,
        file_tracker: FileReadTracker::fresh(),
        agent_tx,
        answer_rx: Arc::new(async_lock::Mutex::new(answer_rx)),
        session_id,
        mailbox,
        timeouts,
        lua_handle,
        subagent_cancels,
        model_policy,
        system_prompt,
        file_write_locks,
        mcp,
        init_cancel,
        initialized: false,
        run_id,
        drain_tx,
        vars: Vars::default(),
        instructions: Instructions::default(),
        tools: RequestTools::default(),
    }
}

impl TuiActorBackend {
    /// Build the system prompt, honoring the CLI override and append.
    fn build_system_with(
        &self,
        mode: &maki_agent::AgentMode,
        prompt_slots: &maki_agent::prompt::ResolvedSlots,
        admission: Option<&maki_agent::agent::TurnAdmissionSnapshot>,
    ) -> String {
        let mut system =
            self.system_prompt
                .override_text
                .clone()
                .unwrap_or_else(|| match admission {
                    Some(snapshot) => match &snapshot.mode_def {
                        Some(def) => maki_agent::agent::build_system_prompt_with_def(
                            &self.vars,
                            def,
                            mode,
                            &self.instructions.text,
                            prompt_slots,
                        ),
                        None => maki_agent::agent::build_system_prompt_with_def(
                            &self.vars,
                            &maki_agent::ModeDef::default_for(maki_agent::ModeId::Build),
                            mode,
                            &self.instructions.text,
                            prompt_slots,
                        ),
                    },
                    None => maki_agent::agent::build_system_prompt(
                        &self.vars,
                        &self.lua_handle.mode_registry(),
                        mode,
                        &self.instructions.text,
                        prompt_slots,
                    ),
                });
        if let Some(append) = &self.system_prompt.append_text {
            system.push('\n');
            system.push_str(append);
        }
        system
    }

    /// One-shot startup: env vars, instruction loading, `btw_system`, tool
    /// building, and MCP readiness raced against the init cancel token.
    /// Races cancellation so a canceled startup stops before any run enters.
    async fn initialize(&mut self) -> bool {
        if self.initialized {
            return true;
        }
        self.vars = template::env_vars_for(&self.cwd.load());
        self.reload_instructions().await;
        if self.init_cancel.is_cancelled() {
            return false;
        }
        self.publish_btw_system(&maki_agent::prompt::ResolvedSlots::default());

        let slot = self.model_slot.load();
        self.tools = self.build_tools(&slot.model, false);
        if let Some(ref mcp) = self.mcp {
            // The queue is drained right after this, and a prompt typed during
            // startup must still carry the MCP tools.
            if self.init_cancel.race(mcp.ready()).await.is_err() {
                return false;
            }
            spawn_oauth_for_needs_auth(mcp);
        }
        self.initialized = !self.init_cancel.is_cancelled();
        self.initialized
    }

    /// Per-run preparation: cwd change detection and instruction reload, MCP
    /// prompt expansion into the preamble, prompt-slot collection and system
    /// prompt construction, tool rebuild, and answer-receiver draining.
    async fn prepare_run(
        &mut self,
        input: &mut AgentInput,
        model: &Model,
        admission: Option<&maki_agent::agent::TurnAdmissionSnapshot>,
    ) -> Result<(String, RequestTools, Arc<maki_agent::prompt::ResolvedSlots>), AgentError> {
        if let Some(prompt) = admission.and_then(|snapshot| snapshot.prompt_inputs.as_ref()) {
            self.vars = template::env_vars_for(&prompt.cwd);
            self.instructions = prompt.instructions.clone();
        } else {
            let vars = template::env_vars_for(&self.cwd.load());
            if vars.apply("{cwd}") != self.vars.apply("{cwd}") {
                let cwd = vars.apply("{cwd}").into_owned();
                let instructions =
                    smol::unblock(move || maki_agent::agent::load_instructions(&cwd)).await;
                self.instructions = instructions;
            }
            self.vars = vars;
        }
        self.tools = self.build_tools(model, input.workflow);
        let ready = admission
            .and_then(|snapshot| snapshot.prompt_inputs.as_ref())
            .and_then(|prompt| prompt.ready.clone());
        let resolved = if ready.is_some() {
            ready
        } else if let Some(receiver) = admission
            .and_then(|snapshot| snapshot.prompt_inputs.as_ref())
            .and_then(|prompt| prompt.resolved.as_ref())
        {
            Some(
                receiver
                    .recv_async()
                    .await
                    .map_err(|e| AgentError::Tool {
                        tool: "prompt_inputs".into(),
                        message: e.to_string(),
                    })?
                    .map_err(|message| AgentError::Tool {
                        tool: "prompt_inputs".into(),
                        message,
                    })?,
            )
        } else {
            None
        };

        if let Some(ref prompt_ref) = input.prompt {
            let Some(ref mcp) = self.mcp else {
                return Err(AgentError::Tool {
                    tool: "mcp_prompt".into(),
                    message: "MCP not available".into(),
                });
            };
            let binding = admission
                .and_then(|snapshot| snapshot.prompt_inputs.as_ref())
                .and_then(|prompt| prompt.mcp_prompt.as_ref());
            if binding.is_none() && admission.is_some() {
                return Err(AgentError::Tool {
                    tool: "mcp_prompt".into(),
                    message: format!("unknown MCP prompt: {}", prompt_ref.qualified_name),
                });
            }
            let messages = if let Some(resolved) = &resolved {
                if !binding.is_some_and(|binding| mcp.prompt_is_current(binding)) {
                    return Err(AgentError::Tool {
                        tool: "mcp_prompt".into(),
                        message: format!(
                            "MCP prompt is no longer available: {}",
                            prompt_ref.qualified_name
                        ),
                    });
                }
                resolved.mcp_messages.clone().unwrap_or_default()
            } else {
                match binding {
                    Some(binding) => mcp.get_bound_prompt(binding, &prompt_ref.arguments).await,
                    None => {
                        mcp.get_prompt(&prompt_ref.qualified_name, &prompt_ref.arguments)
                            .await
                    }
                }
                .map_err(|e| AgentError::Tool {
                    tool: "mcp_prompt".into(),
                    message: e.to_string(),
                })?
                .into_iter()
                .map(prompt_message)
                .collect()
            };
            input.preamble.extend(messages);
        }

        let prompt_slots = if let Some(resolved) = resolved {
            if let Some(error) = &resolved.slots_error {
                warn!(%error, "using empty prompt slots after collection failed");
            }
            resolved.slots
        } else {
            Arc::new(self.lua_handle.collect_prompt_slots_async().await)
        };
        let system = self.build_system_with(&input.mode, &prompt_slots, admission);
        self.publish_btw_system(&prompt_slots);
        self.tools = self.build_tools(model, input.workflow);
        let tools = self.tools.clone();

        while self.answer_rx.lock().await.try_recv().is_ok() {}

        Ok((system, tools, prompt_slots))
    }

    /// Resolves this session's coordinator lease for a run. `Ok(None)` when
    /// there is no session, or when it is already gone (nothing left to
    /// commit into); `Err` when the lease itself cannot be taken.
    async fn acquire_lease(
        &self,
    ) -> Result<Option<maki_agent::session_coordinator::SessionLease>, AgentError> {
        use maki_agent::session_coordinator::{SessionCoordinatorError, SessionCoordinatorHandle};

        let Some(session_id) = &self.session_id else {
            return Ok(None);
        };
        match SessionCoordinatorHandle::resolve(session_id.id()) {
            Ok(coordinator) => coordinator
                .acquire_lease()
                .await
                .map(Some)
                .map_err(|error| AgentError::Config {
                    message: error.to_string(),
                }),
            Err(SessionCoordinatorError::StaleSession(_)) => Ok(None),
            Err(error) => Err(AgentError::Config {
                message: error.to_string(),
            }),
        }
    }

    /// A setup failure never enters a run, and the runner drops the outcome it
    /// synthesizes when the turn is a root -- which every TUI prompt is. Emit
    /// the error here or the prompt vanishes with no feedback at all.
    fn report_setup_failure(
        &self,
        run_id: u64,
        turn_id: TurnId,
        context: &str,
        error: &AgentError,
    ) {
        info!(error = %error, %turn_id, "{context}");
        let _ = EventSender::new(self.agent_tx.clone(), run_id).send(AgentEvent::ControlError {
            message: format!("{context}: {}", error.user_message()),
        });
    }

    fn cancel_setup(&self, context: &TurnContext, turn_id: TurnId, run_id: u64) -> TurnOutcome {
        let outcome = TurnOutcome::cancelled(
            context.agent_id,
            turn_id,
            Default::default(),
            0,
            context
                .cancel_reason
                .reason()
                .unwrap_or(TurnCancellationReason::User),
        );
        EventSender::new(self.agent_tx.clone(), run_id)
            .try_send(AgentEvent::TurnOutcome(outcome.clone()));
        outcome
    }

    /// The one place an executed turn constructs the transient [`Agent`].
    /// Returns `Some` outcome when the run entered or setup was cancelled, `None` when setup failed
    /// before `Agent::run` (the actor then synthesizes one `Failed` delivery).
    async fn execute_agent(
        &mut self,
        history: &mut History,
        context: &TurnContext,
        mut input: AgentInput,
        turn_id: TurnId,
        run_id: u64,
    ) -> Option<TurnOutcome> {
        let setup = context
            .cancel
            .race(async {
                let lease = match self.acquire_lease().await {
                    Ok(lease) => lease,
                    Err(error) => {
                        self.report_setup_failure(
                            run_id,
                            turn_id,
                            "session lease unavailable",
                            &error,
                        );
                        return None;
                    }
                };
                let Some(policy) = context.policy.as_deref() else {
                    let error = AgentError::Config {
                        message: "turn was admitted without an effective agent configuration"
                            .into(),
                    };
                    self.report_setup_failure(run_id, turn_id, "agent turn setup failed", &error);
                    return None;
                };
                let provider = Arc::clone(&policy.settings.provider);
                let model = policy.settings.model.clone();
                input.fast = policy.settings.fast;
                input.workflow = policy.settings.workflow;
                input.thinking = policy.settings.thinking;
                input.mode = policy.mode.clone();
                let prepared = match self
                    .prepare_run(&mut input, &model, context.admission.as_ref())
                    .await
                {
                    Ok(prepared) => prepared,
                    Err(error) => {
                        self.report_setup_failure(
                            run_id,
                            turn_id,
                            "agent turn setup failed",
                            &error,
                        );
                        return None;
                    }
                };
                Some((lease, provider, model, prepared))
            })
            .await;
        let (lease, provider, model, (system, tools, prompt_slots)) = match setup {
            Ok(prepared) => prepared?,
            Err(_) => return Some(self.cancel_setup(context, turn_id, run_id)),
        };
        self.run_id.store(run_id, Ordering::Relaxed);
        let mut agent = Agent::new(
            AgentParams {
                agent_id: self.agent_id,
                provider,
                model,
                config: self.config.clone(),
                tool_output_lines: self.tool_output_lines,
                permissions: Arc::clone(&self.permissions),
                session_id: self.session_id.clone(),
                mailbox: self.mailbox.clone(),
                timeouts: self.timeouts,
                file_tracker: Arc::clone(&self.file_tracker),
                prompt_slots,
                modes: Arc::clone(&self.lua_handle.mode_registry()),
                subagent_cancels: Arc::clone(&self.subagent_cancels),
                ledger: Arc::new(maki_agent::RunLedger::default()),
                registry: Arc::clone(maki_agent::tools::ToolRegistry::global_arc()),
                audience: ToolAudience::MAIN,
                question_mode: QuestionMode::Tui,
                model_policy: Arc::clone(&self.model_policy),
                file_write_locks: Arc::clone(&self.file_write_locks),
                managed_turn: context.managed_turn.clone(),
            },
            AgentRunParams {
                history,
                system,
                event_tx: EventSender::new(self.agent_tx.clone(), run_id),
                tools,
            },
        )
        .with_loaded_instructions(self.instructions.loaded.clone())
        .with_user_response_rx(Arc::clone(&self.answer_rx))
        .with_interrupt_source(context.interrupt.clone().unwrap_or_else(noop_interrupt))
        .with_cancel(context.cancel.clone())
        .with_cancel_reason_source(context.cancel_reason.clone())
        .with_mcp(self.mcp.clone())
        .with_admission(context.admission.clone());

        let outcome = agent.run(turn_id, input).await;
        drop(agent);
        // Only a completed turn's history is worth checkpointing; a failure or
        // cancellation leaves the coordinator's copy as the last good one.
        if let (TurnOutcome::Completed { .. }, Some(committer)) =
            (&outcome, lease.as_ref().and_then(|lease| lease.committer()))
            && let Err(error) = committer.commit_history(history.as_slice().to_vec()).await
        {
            warn!(%error, "committing session history after turn failed");
        }
        Some(outcome)
    }

    /// Base tools only. MCP definitions are injected per request by
    /// `Agent::request_tools`; baking them here would freeze the catalog.
    fn build_tools(&self, model: &Model, workflow: bool) -> RequestTools {
        RequestTools::build(
            ToolRegistry::global(),
            &self.vars,
            model,
            &self.config,
            &[],
            workflow,
            self.mcp.is_some(),
        )
    }

    async fn reload_instructions(&mut self) {
        let cwd = self.vars.apply("{cwd}").into_owned();
        self.instructions = smol::unblock(move || maki_agent::agent::load_instructions(&cwd)).await;
    }

    /// Always pins `Build` mode: btw runs no tools, so Plan-mode constraints
    /// would only confuse the model. Everything else matches the live prompt.
    fn publish_btw_system(&mut self, prompt_slots: &maki_agent::prompt::ResolvedSlots) {
        let system = self.build_system_with(&maki_agent::AgentMode::Build, prompt_slots, None);
        self.btw_system.store(Arc::new(system));
    }

    /// Run id for control-event correlation. Controls carry no turn, so the
    /// backend stamps them with the app's current run id.
    fn current_run_id(&self) -> u64 {
        self.run_id.load(Ordering::Relaxed)
    }
}

fn prompt_message(pm: maki_agent::mcp::protocol::PromptMessage) -> Message {
    let text = pm.content.text.unwrap_or_default();
    match pm.role {
        PromptRole::Assistant => Message {
            role: maki_providers::Role::Assistant,
            content: vec![maki_providers::ContentBlock::Text { text }],
            ..Default::default()
        },
        PromptRole::User => Message::user(text),
    }
}

async fn resolve_prompt_slots(
    receiver: flume::Receiver<maki_agent::prompt::ResolvedSlots>,
) -> (Arc<maki_agent::prompt::ResolvedSlots>, Option<String>) {
    match receiver.recv_async().await {
        Ok(slots) => (Arc::new(slots), None),
        Err(_) => (Arc::default(), Some(PROMPT_SLOTS_CLOSED.into())),
    }
}

async fn prepare_prompt_inputs(
    mut snapshot: maki_agent::agent::TurnAdmissionSnapshot,
) -> Result<maki_agent::agent::TurnAdmissionSnapshot, maki_agent::actor::ActorError> {
    // Only prepared (approval) admissions run through this readiness check,
    // and they fail fast on prompt-slot errors. Ordinary turns instead fall
    // back to default slots with a warning in `prepare_run`, because a
    // planning session should not lose its turn to a broken Lua plugin, while
    // an implementation handoff must not silently start with wrong prompt
    // slots.
    if let Some(prompt) = snapshot.prompt_inputs.as_mut() {
        let prompt = Arc::make_mut(prompt);
        if prompt.ready.is_none()
            && let Some(receiver) = prompt.resolved.take()
        {
            prompt.ready = Some(
                receiver
                    .recv_async()
                    .await
                    .map_err(|_| {
                        maki_agent::actor::ActorError::InvalidConfig(PROMPT_INPUTS_CLOSED.into())
                    })?
                    .map_err(maki_agent::actor::ActorError::InvalidConfig)?,
            );
        }
        if let Some(error) = prompt
            .ready
            .as_ref()
            .and_then(|ready| ready.slots_error.as_ref())
        {
            return Err(maki_agent::actor::ActorError::InvalidConfig(error.clone()));
        }
    }
    Ok(snapshot)
}

impl ActorBackend for TuiActorBackend {
    fn prepared_readiness(&self) -> Option<maki_agent::actor::PreparedReadiness> {
        Some(Arc::new(|snapshot| {
            Box::pin(prepare_prompt_inputs(snapshot))
        }))
    }

    fn root_preparation_error_handler(&self) -> Option<Arc<dyn Fn(u64, String) + Send + Sync>> {
        let agent_tx = self.agent_tx.clone();
        Some(Arc::new(move |run_id, message| {
            EventSender::new(agent_tx.clone(), run_id)
                .try_send(AgentEvent::ControlError { message });
        }))
    }

    fn admission_preparation(&self) -> Option<maki_agent::actor::AdmissionPreparation> {
        let registry = Arc::clone(ToolRegistry::global_arc());
        let mcp = self.mcp.clone();
        let cwd = Arc::clone(&self.cwd);
        let lua_handle = self.lua_handle.clone();
        let mcp_reader = mcp.as_ref().map(|mcp| mcp.reader());
        Some(Arc::new(move |input, _mode, config| {
            let mcp_startup_notice = mcp_reader.as_ref().and_then(|reader| {
                let count = reader
                    .load()
                    .infos
                    .iter()
                    .filter(|info| info.status == maki_agent::McpServerStatus::Connecting)
                    .count();
                (count > 0).then_some(count)
            });
            let cwd = (**cwd.load()).clone();
            let instructions = maki_agent::agent::load_instructions(&cwd.to_string_lossy());
            let binding = input
                .prompt
                .as_ref()
                .and_then(|prompt| mcp.as_ref()?.prompt_binding(&prompt.qualified_name));
            let (tx, rx) = flume::bounded(1);
            let lua_handle = lua_handle.clone();
            let mcp_prompt = input.prompt.clone();
            let prompt_mcp = mcp.clone();
            let pinned = binding.clone();
            let slot_request = lua_handle.request_prompt_slots();
            smol::spawn(async move {
                let slots = resolve_prompt_slots(slot_request);
                let messages = async {
                    match (mcp_prompt, pinned, prompt_mcp) {
                        (Some(prompt), Some(binding), Some(mcp)) => mcp
                            .get_bound_prompt(&binding, &prompt.arguments)
                            .await
                            .map(|messages| {
                                Some(messages.into_iter().map(prompt_message).collect())
                            })
                            .map_err(|e| e.to_string()),
                        (Some(prompt), _, _) => {
                            Err(format!("unknown MCP prompt: {}", prompt.qualified_name))
                        }
                        (None, _, _) => Ok(None),
                    }
                };
                let (messages, slots) = futures_lite::future::zip(messages, slots).await;
                let _ =
                    tx.send(
                        messages.map(|mcp_messages| maki_agent::agent::ResolvedPromptInputs {
                            slots: slots.0,
                            slots_error: slots.1,
                            mcp_messages,
                        }),
                    );
            })
            .detach();
            let mode_def = config
                .and_then(|config| config.mode_def.clone())
                .map(Arc::new);
            maki_agent::agent::TurnAdmissionSnapshot {
                mode_def: mode_def.clone(),
                prompt_inputs: Some(Arc::new(maki_agent::agent::TurnPromptInputs {
                    cwd,
                    instructions,
                    mcp_prompt: binding,
                    resolved: Some(rx),
                    ready: None,
                })),
                bindings: Arc::new(maki_agent::tools::TurnToolBindings::capture(
                    &registry,
                    &Default::default(),
                    mcp.as_ref(),
                )),
                mcp_startup_notice,
            }
        }))
    }

    fn run_turn<'a>(
        &'a mut self,
        history: &'a mut History,
        context: TurnContext,
        input: AgentInput,
        work: WorkKind,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = BackendResult> + Send + 'a>> {
        Box::pin(async move {
            let turn_id = context.turn_id.unwrap_or_else(TurnId::generate);
            // Roots carry their own correlation run id in the work metadata;
            // admitted turns correlate through the admission's correlation.
            let run_id = match &work {
                WorkKind::Root { run_id, .. } => *run_id,
                WorkKind::Turn | WorkKind::Control | WorkKind::Compact => {
                    correlation_to_run_id(&context.correlation)
                }
            };
            let result = if matches!(&work, WorkKind::Control | WorkKind::Compact) {
                // The TUI has no standalone controls/compacts; the runner
                // only reaches here with a Turn or a started Root. Treat an
                // unexpected work kind as a setup failure instead of panicking.
                warn!(?work, "unexpected work kind in TUI run_turn");
                BackendResult::SetupFailed {
                    agent_id: context.agent_id,
                    turn_id,
                }
            } else if let Err(_) | Ok(false) = context.cancel.race(self.initialize()).await {
                if context.cancel.is_cancelled() {
                    BackendResult::EnteredRun(self.cancel_setup(&context, turn_id, run_id))
                } else {
                    BackendResult::SetupFailed {
                        agent_id: context.agent_id,
                        turn_id,
                    }
                }
            } else {
                info!(
                    agent_id = %context.agent_id,
                    %turn_id,
                    %run_id,
                    "tui actor turn"
                );
                // A root admitted as a standalone turn carries the presentation
                // metadata: draw the bubble exactly once when the UI has not drawn
                // it yet. Immediate-dispatch roots (`displayed == true`) were drawn
                // by `start_from_queue`, and folded roots never reach here (the
                // active run consumes them through its interrupt source).
                if let WorkKind::Root {
                    displayed,
                    text,
                    images,
                    earlier,
                    ..
                } = &work
                {
                    for item in earlier {
                        let notice = item.mcp_startup_notice;
                        if !item.displayed || notice.is_some() {
                            let _ = EventSender::new(self.agent_tx.clone(), item.run_id).send(
                                AgentEvent::QueueItemConsumed {
                                    text: item.text.clone(),
                                    images: item.images.clone(),
                                    mcp_startup_notice: notice,
                                    already_displayed: item.already_displayed,
                                },
                            );
                        }
                    }
                    let notice = context
                        .admission
                        .as_ref()
                        .and_then(|snapshot| snapshot.mcp_startup_notice);
                    if !displayed || notice.is_some() {
                        let _ = EventSender::new(self.agent_tx.clone(), run_id).send(
                            AgentEvent::QueueItemConsumed {
                                text: text.clone(),
                                images: images.clone(),
                                mcp_startup_notice: notice,
                                already_displayed: *displayed,
                            },
                        );
                    }
                }
                match self
                    .execute_agent(history, &context, input, turn_id, run_id)
                    .await
                {
                    Some(outcome) => BackendResult::EnteredRun(outcome),
                    None => BackendResult::SetupFailed {
                        agent_id: context.agent_id,
                        turn_id,
                    },
                }
            };
            let _ = self.drain_tx.try_send(run_id);
            result
        })
    }

    fn run_control<'a>(
        &'a mut self,
        _history: &'a mut History,
        _context: TurnContext,
        control: &'a ControlWork,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = BackendResult> + Send + 'a>> {
        // The TUI has no standalone controls today; compaction is routed
        // through the actor's compact work.
        Box::pin(async move {
            warn!(control = %control.name, "unexpected control for TUI backend");
            BackendResult::ControlFailed
        })
    }

    fn run_compact<'a>(
        &'a mut self,
        history: &'a mut History,
        context: TurnContext,
        instructions: Option<&'a str>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = BackendResult> + Send + 'a>> {
        Box::pin(async move {
            // Idle compaction: the runner popped a `Compact` work item. Keep
            // the existing `ControlComplete` / `ControlError` contract, both
            // emitted by `agent::compact` itself. In-turn compaction never
            // reaches here: the active `Agent` folds it through its interrupt
            // source and emits `CompactionDone`.
            let run_id = self.current_run_id();
            let event_tx = EventSender::new(self.agent_tx.clone(), run_id);
            let Some(settings) = context.policy.as_deref().map(|config| &config.settings) else {
                let _ = event_tx.send(AgentEvent::ControlError {
                    message: "compaction was admitted without an effective agent configuration"
                        .into(),
                });
                let _ = self.drain_tx.try_send(run_id);
                return BackendResult::CompactDone;
            };
            let (base_provider, base_model) = compaction_source(settings);

            let (provider, model) = maki_agent::agent::resolve_compaction_model(
                &base_provider,
                &base_model,
                self.timeouts,
                &self.model_policy,
            );
            let lease = match self.acquire_lease().await {
                Ok(lease) => lease,
                Err(error) => {
                    warn!(error = %error, "session lease unavailable for idle compaction");
                    let _ = event_tx.send(AgentEvent::ControlError {
                        message: error.user_message(),
                    });
                    let _ = self.drain_tx.try_send(run_id);
                    return BackendResult::CompactDone;
                }
            };
            let mut result = maki_agent::agent::compact(
                &*provider,
                &model,
                history,
                &event_tx,
                &context.cancel,
                &self.config,
                instructions,
                self.session_id.as_ref(),
            )
            .await;
            if result.is_ok()
                && let Some(committer) = lease.as_ref().and_then(|lease| lease.committer())
            {
                result = committer
                    .commit_history(history.as_slice().to_vec())
                    .await
                    .map_err(|error| AgentError::Config {
                        message: error.to_string(),
                    });
            }
            let _ = self.drain_tx.try_send(run_id);
            match result {
                Ok(()) => BackendResult::CompactDone,
                Err(e) => {
                    warn!(error = %e, "idle compaction failed");
                    let _ = event_tx.send(AgentEvent::ControlError {
                        message: e.user_message(),
                    });
                    BackendResult::ControlFailed
                }
            }
        })
    }
}

fn compaction_source(
    settings: &maki_agent::RunSettings,
) -> (Arc<dyn maki_providers::provider::Provider>, Model) {
    (Arc::clone(&settings.provider), settings.model.clone())
}

/// A no-op interrupt source used when the actor provides none (standalone
/// control or compact execution).
fn noop_interrupt() -> Arc<dyn maki_agent::InterruptSource> {
    struct Noop;
    impl maki_agent::InterruptSource for Noop {
        fn poll(&self) -> Option<maki_agent::ExtractedCommand> {
            None
        }
    }
    Arc::new(Noop)
}

fn spawn_oauth_for_needs_auth(handle: &McpHandle) {
    let snapshot = handle.reader().load().clone();
    for info in snapshot.infos.iter() {
        let McpServerStatus::NeedsAuth { ref url } = info.status else {
            continue;
        };
        let Some(ref server_url) = info.url else {
            continue;
        };
        let handle = handle.clone();
        let server_name = info.name.clone();
        let server_url = server_url.clone();
        let www_auth = url.clone();
        let oauth = info.oauth.clone();
        smol::spawn(async move {
            let storage = match maki_storage::StateDir::resolve() {
                Ok(s) => s,
                Err(e) => {
                    tracing::warn!(server = %server_name, error = %e, "cannot resolve storage for OAuth");
                    return;
                }
            };
            if let Err(e) = maki_agent::mcp::oauth::authenticate(
                &server_name,
                &server_url,
                www_auth.as_deref(),
                &storage,
                maki_agent::mcp::oauth::Interaction::Background,
                oauth,
            )
            .await
            {
                tracing::warn!(server = %server_name, error = %e, "background OAuth failed");
                return;
            }
            handle.send(McpCommand::Reconnect {
                server: server_name.clone(),
            });
            tracing::info!(server = %server_name, "MCP server authenticated via OAuth");
        })
        .detach();
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::pin::pin;

    use futures_lite::future::poll_once;
    use maki_agent::{AgentMode, McpPromptRef, ReasonedCancelToken};
    use maki_config::PermissionsConfig;
    use test_case::test_case;

    use super::*;
    use crate::agent::ProviderSlot;

    #[test]
    fn idle_compaction_source_uses_pinned_policy() {
        let mut pinned_model = crate::components::test_model();
        pinned_model.id = "pinned-model".into();
        let settings = maki_agent::RunSettings {
            provider: Arc::new(StubProvider),
            model: pinned_model.clone(),
            fast: false,
            workflow: false,
            thinking: Default::default(),
        };

        let (provider, model) = compaction_source(&settings);

        assert_eq!(model.id, pinned_model.id);
        assert!(Arc::ptr_eq(&provider, &settings.provider));
    }

    #[test]
    fn admitted_prompt_inputs_survive_cwd_change() {
        let (model_slot, _change_rx) =
            ProviderSlot::new(crate::components::test_model(), Arc::new(StubProvider));
        let (agent_tx, _agent_rx) = flume::unbounded();
        let (_answer_tx, answer_rx) = flume::unbounded();
        let (drain_tx, _drain_rx) = flume::unbounded();
        let (_init_trigger, init_cancel) = CancelToken::new();
        let cwd = Arc::new(ArcSwap::from_pointee(PathBuf::from("/tmp/admitted")));
        let mut backend = new_backend(
            AgentId::generate(),
            model_slot,
            Arc::clone(&cwd),
            AgentConfig::default(),
            ToolOutputLines::default(),
            Arc::new(ArcSwap::from_pointee(String::new())),
            None,
            &[],
            Arc::new(PermissionManager::new(
                PermissionsConfig::default(),
                PathBuf::from("/tmp"),
                maki_config::ProjectConfig::for_project(std::path::Path::new("/tmp")),
                Arc::default(),
            )),
            agent_tx,
            answer_rx,
            None,
            None,
            maki_providers::Timeouts::default(),
            EventHandle::disconnected_for_test(),
            Arc::new(CancelMap::new()),
            Arc::new(ModelPolicy::default()),
            SystemPromptOverride::default(),
            Arc::new(maki_agent::tools::FileWriteLocks::new()),
            init_cancel,
            drain_tx,
            Arc::new(AtomicU64::new(0)),
        );
        let mut input = AgentInput::from_defaults(
            "hello".into(),
            AgentMode::Build,
            Vec::new(),
            maki_config::SessionDefaults::default(),
        );
        let mut snapshot = backend.admission_preparation().unwrap()(&input, &input.mode, None);
        let prompt = Arc::make_mut(snapshot.prompt_inputs.as_mut().unwrap());
        prompt.instructions.text = "admitted instructions".into();
        let (tx, rx) = flume::bounded(1);
        prompt.resolved = Some(rx);
        let mut slots = maki_agent::prompt::ResolvedSlots::default();
        slots.insert(
            maki_agent::prompt::PromptId::System,
            maki_agent::prompt::Slot::Identity,
            maki_agent::prompt::SlotEntry {
                plugin: "admission".into(),
                content: "admitted identity".into(),
            },
        );
        tx.send(Ok(maki_agent::agent::ResolvedPromptInputs {
            slots: Arc::new(slots),
            slots_error: None,
            mcp_messages: None,
        }))
        .unwrap();
        let snapshot = smol::block_on(backend.prepared_readiness().unwrap()(snapshot)).unwrap();
        let prompt = snapshot.prompt_inputs.as_ref().unwrap();
        assert!(prompt.resolved.is_none());
        let pinned_slots = Arc::clone(&prompt.ready.as_ref().unwrap().slots);
        let snapshot = smol::block_on(backend.prepared_readiness().unwrap()(snapshot)).unwrap();
        assert!(Arc::ptr_eq(
            &pinned_slots,
            &snapshot
                .prompt_inputs
                .as_ref()
                .unwrap()
                .ready
                .as_ref()
                .unwrap()
                .slots,
        ));
        cwd.store(Arc::new(PathBuf::from("/tmp/changed")));
        let model = crate::components::test_model();
        let (system, _, used_slots) =
            smol::block_on(backend.prepare_run(&mut input, &model, Some(&snapshot))).unwrap();
        assert!(Arc::ptr_eq(&pinned_slots, &used_slots));
        assert!(system.contains("admitted instructions"));
        assert!(system.contains("admitted identity"));
        assert!(!system.contains("/tmp/changed"));
        assert_eq!(backend.vars.apply("{cwd}"), "/tmp/admitted");
    }

    #[test_case::test_case(false; "outer_channel_closed")]
    #[test_case::test_case(true; "mcp_error")]
    fn prepared_prompt_input_errors_fail(mcp_error: bool) {
        const MCP_ERROR: &str = "MCP prompt expansion failed";
        let (backend, input, _) = failing_backend();
        let mut snapshot = backend.admission_preparation().unwrap()(&input, &input.mode, None);
        let (tx, rx) = flume::bounded(1);
        Arc::make_mut(snapshot.prompt_inputs.as_mut().unwrap()).resolved = Some(rx);
        if mcp_error {
            tx.send(Err(MCP_ERROR.into())).unwrap();
        }
        drop(tx);
        let result = smol::block_on(backend.prepared_readiness().unwrap()(snapshot));
        let Err(maki_agent::actor::ActorError::InvalidConfig(message)) = result else {
            panic!("prompt readiness must fail");
        };
        let expected = if mcp_error {
            MCP_ERROR
        } else {
            PROMPT_INPUTS_CLOSED
        };
        assert_eq!(message, expected);
    }

    #[test]
    fn prepared_prompt_slots_reject_inner_channel_closure() {
        smol::block_on(async {
            let (mut backend, mut input, _) = failing_backend();
            input.prompt = None;
            let mut snapshot = backend.admission_preparation().unwrap()(&input, &input.mode, None);
            let (slot_tx, slot_rx) = flume::bounded(1);
            drop(slot_tx);
            let (slots, slots_error) = resolve_prompt_slots(slot_rx).await;
            assert_eq!(slots_error.as_deref(), Some(PROMPT_SLOTS_CLOSED));
            let (tx, rx) = flume::bounded(1);
            Arc::make_mut(snapshot.prompt_inputs.as_mut().unwrap()).resolved = Some(rx);
            tx.send(Ok(maki_agent::agent::ResolvedPromptInputs {
                slots,
                slots_error,
                mcp_messages: None,
            }))
            .unwrap();
            let ordinary = snapshot.clone();
            let Err(maki_agent::actor::ActorError::InvalidConfig(message)) =
                backend.prepared_readiness().unwrap()(snapshot).await
            else {
                panic!("closed slot channel must fail readiness");
            };
            assert_eq!(message, PROMPT_SLOTS_CLOSED);
            tx.send(Ok(maki_agent::agent::ResolvedPromptInputs {
                slots: Arc::default(),
                slots_error: Some(PROMPT_SLOTS_CLOSED.into()),
                mcp_messages: None,
            }))
            .unwrap();
            backend
                .prepare_run(
                    &mut input,
                    &crate::components::test_model(),
                    Some(&ordinary),
                )
                .await
                .unwrap();
        });
    }

    fn failing_backend() -> (TuiActorBackend, AgentInput, flume::Receiver<Envelope>) {
        let (model_slot, _change_rx) =
            ProviderSlot::new(crate::components::test_model(), Arc::new(StubProvider));
        let (agent_tx, agent_rx) = flume::unbounded();
        let (_answer_tx, answer_rx) = flume::unbounded();
        let (drain_tx, _drain_rx) = flume::unbounded();
        let (_init_trigger, init_cancel) = CancelToken::new();
        let agent_id = AgentId::generate();
        let mut backend = new_backend(
            agent_id,
            model_slot,
            Arc::new(ArcSwap::from_pointee(PathBuf::from("/tmp"))),
            AgentConfig::default(),
            ToolOutputLines::default(),
            Arc::new(ArcSwap::from_pointee(String::new())),
            // No MCP session, so a prompt-carrying input fails in `prepare_run`.
            None,
            &[],
            Arc::new(PermissionManager::new(
                PermissionsConfig::default(),
                PathBuf::from("/tmp"),
                maki_config::ProjectConfig::for_project(std::path::Path::new("/tmp")),
                Arc::default(),
            )),
            agent_tx,
            answer_rx,
            None,
            None,
            maki_providers::Timeouts::default(),
            EventHandle::disconnected_for_test(),
            Arc::new(CancelMap::new()),
            Arc::new(ModelPolicy::default()),
            SystemPromptOverride::default(),
            Arc::new(maki_agent::tools::FileWriteLocks::new()),
            init_cancel,
            drain_tx,
            Arc::new(AtomicU64::new(0)),
        );
        backend.initialized = true;

        let input = AgentInput {
            message: "hello".into(),
            mode: AgentMode::Build,
            images: Vec::new(),
            preamble: Vec::new(),
            thinking: Default::default(),
            fast: false,
            workflow: false,
            prompt: Some(Box::new(McpPromptRef {
                qualified_name: "srv.missing".into(),
                arguments: HashMap::new(),
            })),
            cancel: None,
            lease_committer: None,
        };
        (backend, input, agent_rx)
    }

    fn run_failing_turn(
        policy: Option<maki_agent::actor::EffectiveAgentConfig>,
    ) -> (Option<TurnOutcome>, Vec<Envelope>) {
        let (mut backend, input, agent_rx) = failing_backend();
        let context = TurnContext {
            agent_id: backend.agent_id,
            turn_id: None,
            cancel: CancelToken::none(),
            cancel_reason: ReasonedCancelToken::none(),
            correlation: format!("{ROOT_CORRELATION_PREFIX}0"),
            generation: 0,
            policy: policy.map(Arc::new),
            admission: None,
            interrupt: None,
            managed_turn: None,
        };
        let mut history = History::new(Vec::new());
        let outcome = smol::block_on(backend.execute_agent(
            &mut history,
            &context,
            input,
            TurnId::generate(),
            0,
        ));
        drop(backend);
        (outcome, agent_rx.drain().collect())
    }

    /// A setup failure never enters a run, and the runner discards the outcome
    /// it synthesizes when the turn is a root -- which every TUI prompt is. If
    /// the backend stays quiet the prompt vanishes with no feedback at all, so
    /// the error has to be emitted here.
    #[test]
    fn missing_turn_policy_fails_closed() {
        let (outcome, envelopes) = run_failing_turn(None);
        assert!(outcome.is_none());
        assert!(envelopes.iter().any(|envelope| {
            matches!(&envelope.event, AgentEvent::ControlError { message } if message.contains("without an effective agent configuration"))
        }));
    }

    #[test]
    fn a_setup_failure_reaches_the_user() {
        let (outcome, envelopes) =
            run_failing_turn(Some(maki_agent::actor::EffectiveAgentConfig::new(
                maki_agent::RunSettings {
                    provider: Arc::new(StubProvider),
                    model: crate::components::test_model(),
                    fast: false,
                    workflow: false,
                    thinking: Default::default(),
                },
                AgentMode::Build,
            )));
        assert!(outcome.is_none(), "setup failed, so no run was entered");
        let reported = envelopes.iter().any(|envelope| {
            matches!(&envelope.event, AgentEvent::ControlError { message } if message.contains("MCP not available"))
        });
        assert!(
            reported,
            "a turn that dies in setup must say so: {:?}",
            envelopes.iter().map(|e| &e.event).collect::<Vec<_>>()
        );
    }

    #[test_case(TurnCancellationReason::User; "user")]
    #[test_case(TurnCancellationReason::Closed; "closed")]
    #[test_case(TurnCancellationReason::Shutdown; "shutdown")]
    fn cancelled_prompt_setup_releases_backend(reason: TurnCancellationReason) {
        smol::block_on(async {
            let (mut backend, input, events) = failing_backend();
            let mut admission = backend.admission_preparation().unwrap()(&input, &input.mode, None);
            let (pending_prompt, receiver) = flume::bounded(1);
            Arc::make_mut(admission.prompt_inputs.as_mut().unwrap()).resolved = Some(receiver);
            let policy = Arc::new(maki_agent::actor::EffectiveAgentConfig::new(
                maki_agent::RunSettings {
                    provider: Arc::new(StubProvider),
                    model: crate::components::test_model(),
                    fast: false,
                    workflow: false,
                    thinking: Default::default(),
                },
                AgentMode::Build,
            ));
            let (trigger, cancel) = CancelToken::new();
            let (reason_trigger, cancel_reason) = ReasonedCancelToken::new();
            let turn_id = TurnId::generate();
            let context = TurnContext {
                agent_id: backend.agent_id,
                turn_id: Some(turn_id),
                cancel,
                cancel_reason,
                correlation: format!("{ROOT_CORRELATION_PREFIX}0"),
                generation: 0,
                policy: Some(Arc::clone(&policy)),
                admission: Some(admission),
                interrupt: None,
                managed_turn: None,
            };
            let mut successor_input = AgentInput::from_defaults(
                input.message.clone(),
                input.mode.clone(),
                Vec::new(),
                Default::default(),
            );
            successor_input.prompt = input.prompt.clone();
            let mut history = History::new(Vec::new());
            let outcome = {
                let mut run = pin!(backend.run_turn(&mut history, context, input, WorkKind::Turn));
                assert!(poll_once(run.as_mut()).await.is_none());
                assert!(events.is_empty());
                reason_trigger.cancel(reason);
                trigger.cancel();
                let Some(BackendResult::EnteredRun(outcome)) = poll_once(run.as_mut()).await else {
                    panic!("cancelled setup must return a terminal outcome");
                };
                outcome
            };
            assert_eq!(
                outcome,
                TurnOutcome::cancelled(backend.agent_id, turn_id, Default::default(), 0, reason)
            );
            let emitted = events.drain().collect::<Vec<_>>();
            assert_eq!(emitted.len(), 1);
            assert!(
                matches!(&emitted[0].event, AgentEvent::TurnOutcome(actual) if actual == &outcome)
            );
            assert!(pending_prompt.is_disconnected());

            let successor = TurnContext {
                agent_id: backend.agent_id,
                turn_id: Some(TurnId::generate()),
                cancel: CancelToken::none(),
                cancel_reason: ReasonedCancelToken::none(),
                correlation: format!("{ROOT_CORRELATION_PREFIX}1"),
                generation: 1,
                policy: Some(policy),
                admission: None,
                interrupt: None,
                managed_turn: None,
            };
            let result = backend
                .run_turn(&mut history, successor, successor_input, WorkKind::Turn)
                .await;
            assert!(matches!(result, BackendResult::SetupFailed { .. }));
            assert!(
                events
                    .drain()
                    .any(|event| matches!(event.event, AgentEvent::ControlError { .. }))
            );
        });
    }

    struct StubProvider;

    impl maki_providers::provider::Provider for StubProvider {
        fn stream_message<'a>(
            &'a self,
            _model: &'a Model,
            _messages: &'a [Message],
            _system: &'a str,
            _tools: &'a serde_json::Value,
            _event_tx: &'a flume::Sender<maki_providers::ProviderEvent>,
            _opts: maki_providers::RequestOptions,
            _session_id: Option<&'a SessionRef>,
        ) -> maki_providers::provider::BoxFuture<
            'a,
            Result<maki_providers::StreamResponse, AgentError>,
        > {
            Box::pin(std::future::pending())
        }

        fn list_models(
            &self,
        ) -> maki_providers::provider::BoxFuture<
            '_,
            Result<Vec<maki_providers::ModelInfo>, AgentError>,
        > {
            Box::pin(async { Ok(Vec::new()) })
        }
    }
}
