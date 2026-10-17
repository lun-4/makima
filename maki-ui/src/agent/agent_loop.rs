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
use maki_agent::tools::offload::OffloadStore;
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
    offload: Option<Arc<OffloadStore>>,
    /// Live MCP session; recreated per spawn so deferred tools stay fresh.
    mcp: Option<McpSession>,
    /// Startup cancellation: races env/instruction/MCP initialization.
    init_cancel: CancelToken,
    initialized: bool,
    /// App-visible current run id. Bumped by the app on each run; the backend
    /// reads it to stamp compact/control events that carry no correlation.
    run_id: Arc<AtomicU64>,
    /// Signals the drain driver after actor settlement and End publication.
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
    offload: Option<Arc<OffloadStore>>,
    init_cancel: CancelToken,
    drain_tx: flume::Sender<u64>,
    run_id: Arc<AtomicU64>,
) -> TuiActorBackend {
    let mcp = mcp_handle.map(|h| McpSession::new(h, initial_history));
    let vars = template::env_vars_for(&cwd.load());
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
        offload,
        mcp,
        init_cancel,
        initialized: false,
        run_id,
        drain_tx,
        vars,
        instructions: Instructions::default(),
        tools: RequestTools::default(),
    }
}

impl TuiActorBackend {
    pub(super) fn orchestration_template(&self) -> maki_agent::tools::ToolContext {
        let selected = self.model_slot.load();
        let tools = self.build_tools(&selected.model, false);
        let mut history = History::new(Vec::new());
        let agent = Agent::new(
            AgentParams {
                agent_id: self.agent_id,
                provider: Arc::clone(&selected.provider)
                    as Arc<dyn maki_providers::provider::Provider>,
                model: selected.model.clone(),
                config: self.config.clone(),
                tool_output_lines: self.tool_output_lines,
                permissions: Arc::clone(&self.permissions),
                session_id: self.session_id.clone(),
                mailbox: self.mailbox.clone(),
                timeouts: self.timeouts,
                file_tracker: Arc::clone(&self.file_tracker),
                prompt_slots: Arc::new(maki_agent::prompt::ResolvedSlots::default()),
                modes: self.lua_handle.mode_registry(),
                subagent_cancels: Arc::clone(&self.subagent_cancels),
                ledger: Arc::new(maki_agent::RunLedger::default()),
                registry: Arc::clone(ToolRegistry::global_arc()),
                audience: ToolAudience::MAIN,
                question_mode: QuestionMode::Tui,
                model_policy: Arc::clone(&self.model_policy),
                file_write_locks: Arc::clone(&self.file_write_locks),
                offload: self.offload.clone(),
                managed_turn: None,
            },
            AgentRunParams {
                history: &mut history,
                system: String::new(),
                event_tx: EventSender::new(self.agent_tx.clone(), 0),
                tools,
            },
        )
        .with_mcp(self.mcp.clone());
        let mut template = agent.tool_context();
        template.cwd = (**self.cwd.load()).clone();
        template
    }

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
        if let Some(inbox) = &context.inbox {
            inbox.close();
        }
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
        self.run_id.fetch_max(run_id, Ordering::Relaxed);
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
                offload: self.offload.clone(),
                managed_turn: context.managed_turn.clone(),
            },
            AgentRunParams {
                history,
                system,
                event_tx: EventSender::new(self.agent_tx.clone(), run_id)
                    .with_output(context.output.clone()),
                tools,
            },
        )
        .with_loaded_instructions(self.instructions.loaded.clone())
        .with_user_response_rx(Arc::clone(&self.answer_rx))
        .with_interrupt_source(context.interrupt.clone().unwrap_or_else(noop_interrupt))
        .with_turn_inbox(context.inbox.clone())
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
    fn offload_store(&self) -> Option<Arc<OffloadStore>> {
        self.offload.clone()
    }

    fn on_work_settled(&mut self) {
        let _ = self.drain_tx.try_send(self.current_run_id());
    }

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
            let mut context = context;
            let turn_id = context.turn_id.unwrap_or_else(TurnId::generate);
            let admission_run_id = correlation_to_run_id(&context.correlation);
            if context.cancel.is_cancelled() {
                return BackendResult::EnteredRun(self.cancel_setup(
                    &context,
                    turn_id,
                    admission_run_id,
                ));
            }
            let explicit = matches!(&work, WorkKind::Turn)
                && !context.correlation.starts_with(ROOT_CORRELATION_PREFIX);
            let standalone_presentation = explicit || matches!(&work, WorkKind::Root { .. });
            let run_id = if standalone_presentation {
                let displayed_run = match &work {
                    WorkKind::Root {
                        run_id,
                        displayed: true,
                        ..
                    } => Some(*run_id),
                    _ => None,
                };
                if let Some(current) = &context.managed_turn {
                    let allocated = current.manager().actor(context.agent_id).and_then(|actor| {
                        actor
                            .allocate_processing_presentation(turn_id, displayed_run)
                            .map_err(|error| maki_agent::ManagerError::Policy(error.to_string()))
                    });
                    match allocated {
                        Ok(run_id) => {
                            self.run_id.fetch_max(run_id, Ordering::Relaxed);
                            run_id
                        }
                        Err(_) => {
                            return BackendResult::EnteredRun(self.cancel_setup(
                                &context,
                                turn_id,
                                admission_run_id,
                            ));
                        }
                    }
                } else if let Some(displayed_run) = displayed_run
                    && self
                        .run_id
                        .compare_exchange(
                            displayed_run,
                            displayed_run,
                            Ordering::Relaxed,
                            Ordering::Relaxed,
                        )
                        .is_ok()
                {
                    displayed_run
                } else {
                    self.run_id.fetch_add(1, Ordering::Relaxed) + 1
                }
            } else {
                correlation_to_run_id(&context.correlation)
            };
            if standalone_presentation {
                if let Some(current) = &context.managed_turn {
                    let bound = current.manager().actor(context.agent_id).and_then(|actor| {
                        if let WorkKind::Root { earlier, .. } = &work {
                            for item in earlier {
                                actor
                                    .bind_active_correlation(turn_id, item.correlation.clone())
                                    .map_err(|error| {
                                        maki_agent::ManagerError::Policy(error.to_string())
                                    })?;
                            }
                        }
                        actor
                            .bind_active_correlation(
                                turn_id,
                                format!("{ROOT_CORRELATION_PREFIX}{run_id}"),
                            )
                            .map_err(|error| maki_agent::ManagerError::Policy(error.to_string()))
                    });
                    if bound.is_err() || context.cancel.is_cancelled() {
                        return BackendResult::EnteredRun(self.cancel_setup(
                            &context,
                            turn_id,
                            admission_run_id,
                        ));
                    }
                }
                context.correlation = format!("{ROOT_CORRELATION_PREFIX}{run_id}");
            }
            if explicit && (!input.message.is_empty() || !input.images.is_empty()) {
                EventSender::new(self.agent_tx.clone(), run_id).try_send(
                    AgentEvent::QueueItemConsumed {
                        text: input.message.clone(),
                        images: input.images.clone(),
                        mcp_startup_notice: context
                            .admission
                            .as_ref()
                            .and_then(|snapshot| snapshot.mcp_startup_notice),
                        already_displayed: false,
                    },
                );
            }
            // A root admitted as a standalone turn carries the presentation
            // metadata: draw the bubble exactly once when the UI has not drawn
            // it yet. Immediate-dispatch roots (`displayed == true`) were drawn
            // by `start_from_queue`, and folded roots never reach here (the
            // active run consumes them through its interrupt source).
            if let WorkKind::Root {
                text,
                images,
                earlier,
                mcp_startup_notice,
                already_displayed,
                ..
            } = &work
            {
                for item in earlier {
                    let notice = item.mcp_startup_notice;
                    if !item.displayed || notice.is_some() {
                        let _ = EventSender::new(self.agent_tx.clone(), run_id).send(
                            AgentEvent::QueueItemConsumed {
                                text: item.text.clone(),
                                images: item.images.clone(),
                                mcp_startup_notice: notice,
                                already_displayed: item.already_displayed,
                            },
                        );
                    }
                }
                let notice = mcp_startup_notice.or_else(|| {
                    context
                        .admission
                        .as_ref()
                        .and_then(|snapshot| snapshot.mcp_startup_notice)
                });
                let _ = EventSender::new(self.agent_tx.clone(), run_id).send(
                    AgentEvent::QueueItemConsumed {
                        text: text.clone(),
                        images: images.clone(),
                        mcp_startup_notice: notice,
                        already_displayed: *already_displayed,
                    },
                );
            }
            if standalone_presentation && let Some(policy) = &context.policy {
                EventSender::new(self.agent_tx.clone(), run_id).try_send(
                    AgentEvent::ModelSwitched {
                        spec: policy.settings.model.spec(),
                    },
                );
            }
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
            if standalone_presentation && matches!(result, BackendResult::SetupFailed { .. }) {
                if let Some(inbox) = &context.inbox {
                    inbox.close();
                }
                let outcome = TurnOutcome::failed(
                    context.agent_id,
                    turn_id,
                    Default::default(),
                    0,
                    maki_agent::TurnFailure {
                        kind: maki_agent::TurnFailureKind::Internal,
                        diagnostic: "actor setup failed".into(),
                        user_message: "The agent could not start this turn.".into(),
                        retryable: true,
                    },
                );
                EventSender::new(self.agent_tx.clone(), run_id)
                    .try_send(AgentEvent::TurnOutcome(outcome.clone()));
                BackendResult::EnteredRun(outcome)
            } else {
                result
            }
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
    use std::any::Any;
    use std::collections::HashMap;
    use std::pin::pin;

    use futures_lite::future::poll_once;
    use maki_agent::actor::{ActorEvent, DeliveryStatus};
    use maki_agent::{AgentMode, McpPromptRef, ReasonedCancelToken};
    use maki_config::PermissionsConfig;
    use maki_providers::ContentBlock;
    use test_case::test_case;

    use super::*;
    use crate::agent::ProviderSlot;

    #[test_case(false, false; "explicit_complete")]
    #[test_case(true, false; "explicit_cancel")]
    #[test_case(false, true; "mixed_fifo_complete")]
    #[test_case(true, true; "mixed_fifo_cancel")]
    fn explicit_actual_root_turn_uses_current_ui_run_and_cancel_alias(
        cancel: bool,
        deferred: bool,
    ) {
        smol::block_on(async {
            const RUN_ID: u64 = 7;
            const RESPONSE: &str = "visible explicit response";
            let (mut backend, mut input, events) = failing_backend();
            input.prompt = None;
            let (release, released) = flume::bounded(1);
            let provider = Arc::new(ExplicitProvider { release: released });
            let (drain_tx, drain_rx) = flume::unbounded();
            let agent_tx = backend.agent_tx.clone();
            backend.drain_tx = drain_tx;
            backend.run_id.store(RUN_ID - 1, Ordering::Relaxed);
            let actor_run_id = Arc::clone(&backend.run_id);
            let manager = maki_agent::AgentManagerHandle::new(Default::default()).unwrap();
            let root = manager
                .create_root_with_config(
                    Some(
                        maki_agent::EffectiveAgentConfig::new(
                            maki_agent::RunSettings {
                                provider,
                                model: crate::components::test_model(),
                                fast: false,
                                workflow: false,
                                thinking: Default::default(),
                            },
                            AgentMode::Build,
                        )
                        .with_mode_def(
                            maki_agent::ModeRegistry::builtin().get(&maki_agent::ModeId::Build),
                        ),
                    ),
                    Vec::new(),
                    None,
                    |agent_id| {
                        backend.agent_id = agent_id;
                        Ok::<Box<dyn ActorBackend>, String>(Box::new(backend))
                    },
                )
                .unwrap();
            let actor = root.actor().unwrap();
            let mut app = crate::app::tests::actor_presentation_app(
                crate::agent::shared_queue::actor_queue(
                    Arc::new(actor.clone()),
                    Arc::clone(&actor_run_id),
                ),
                RUN_ID - 1,
            );
            let make_input = |text: &str| {
                AgentInput::from_defaults(
                    text.into(),
                    AgentMode::Build,
                    Vec::new(),
                    Default::default(),
                )
            };
            if deferred {
                actor_run_id.store(RUN_ID - 2, Ordering::Relaxed);
                actor.observe_presentation(RUN_ID - 1);
                actor
                    .rush(maki_agent::actor::RootWork::new(
                        make_input("successor"),
                        RUN_ID - 1,
                        true,
                        "successor".into(),
                        Vec::new(),
                        format!("{ROOT_CORRELATION_PREFIX}{}", RUN_ID - 1),
                    ))
                    .unwrap();
                loop {
                    let envelope = events.recv_async().await.unwrap();
                    assert_eq!(envelope.run_id, RUN_ID - 1);
                    if matches!(envelope.event, AgentEvent::TextDelta { .. }) {
                        break;
                    }
                }
                let active_policy_generation = actor.config_snapshot().unwrap().generation;
                actor
                    .rush(maki_agent::actor::RootWork::new(
                        make_input("successor"),
                        RUN_ID - 1,
                        false,
                        "folded user input".into(),
                        Vec::new(),
                        format!("{ROOT_CORRELATION_PREFIX}{}", RUN_ID - 1),
                    ))
                    .unwrap();
                let barrier = actor.reserve_config_update().unwrap();
                barrier
                    .resolve(Err(maki_agent::actor::ActorError::PolicyCancelled))
                    .unwrap();
                assert!(matches!(
                    futures_lite::future::or(barrier.wait(), async {
                        smol::Timer::after(std::time::Duration::from_secs(10)).await;
                        panic!("interrupt admission never became runnable");
                    })
                    .await,
                    Err(maki_agent::actor::ActorError::PolicyCancelled)
                ));
                assert_eq!(
                    actor.config_snapshot().unwrap().generation,
                    active_policy_generation
                );
                assert_eq!(
                    maki_agent::batch_key(&make_input("successor")),
                    Some((AgentMode::Build, false))
                );
                assert_eq!(
                    actor.snapshot().queue,
                    vec![maki_agent::actor::QueueProjection::Message {
                        text: "folded user input".into(),
                        image_count: 0,
                        displayed: false,
                    }]
                );
                release.send(()).unwrap();
                let mut folded_bubble = false;
                loop {
                    let envelope = events.recv_async().await.unwrap();
                    assert_eq!(envelope.run_id, RUN_ID - 1);
                    folded_bubble |= matches!(envelope.event, AgentEvent::QueueItemConsumed { .. });
                    if matches!(envelope.event, AgentEvent::TextDelta { .. }) {
                        break;
                    }
                }
                assert!(folded_bubble);
                assert_eq!(actor_run_id.load(Ordering::Relaxed), RUN_ID - 1);
                release.send(()).unwrap();
                assert_eq!(drain_rx.recv_async().await.unwrap(), RUN_ID - 1);
                events.drain().for_each(drop);
            }
            let ticket = actor
                .admit_turn_with_options(
                    input,
                    None,
                    String::new(),
                    maki_agent::actor::TurnAdmissionOptions {
                        provenance: maki_agent::actor::TurnProvenance {
                            origin: maki_agent::actor::TurnOrigin::Plugin,
                            ..Default::default()
                        },
                        ..Default::default()
                    },
                )
                .unwrap();
            let successor = if deferred {
                let queue = crate::agent::shared_queue::actor_queue(
                    Arc::new(actor.clone()),
                    Arc::clone(&actor_run_id),
                );
                for text in ["earlier deferred", "final deferred"] {
                    let input = make_input(text);
                    queue.push(crate::agent::shared_queue::QueueItem::Message {
                        text: text.into(),
                        image_count: 0,
                        input,
                        run_id: RUN_ID - 1,
                        displayed: false,
                    });
                }
                let barrier = actor.reserve_config_update().unwrap();
                barrier
                    .resolve(Err(maki_agent::actor::ActorError::PolicyCancelled))
                    .unwrap();
                assert!(matches!(
                    barrier.wait().await,
                    Err(maki_agent::actor::ActorError::PolicyCancelled)
                ));
                None
            } else {
                Some(
                    actor
                        .admit_turn(make_input("successor"), None, String::new())
                        .unwrap(),
                )
            };
            let mut visible = Vec::new();
            const TEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);
            loop {
                let envelope = futures_lite::future::or(events.recv_async(), async {
                    smol::Timer::after(TEST_TIMEOUT).await;
                    panic!("waiting for explicit streaming: actor={:?}, outcome={:?}, events={visible:?}", actor.snapshot(), ticket.peek());
                }).await.unwrap();
                assert_eq!(envelope.run_id, RUN_ID);
                let streamed =
                    matches!(&envelope.event, AgentEvent::TextDelta { text } if text == RESPONSE);
                visible.push(matches!(
                    envelope.event,
                    AgentEvent::QueueItemConsumed { .. }
                ));
                app.update(crate::app::Msg::Agent(Box::new(envelope)));
                if streamed {
                    break;
                }
            }
            assert!(visible.iter().any(|consumed| *consumed));
            if cancel {
                manager
                    .cancel_correlation(
                        root.id(),
                        &format!("{ROOT_CORRELATION_PREFIX}{RUN_ID}"),
                        TurnCancellationReason::User,
                    )
                    .unwrap();
            } else {
                release.send(()).unwrap();
            }
            let outcome = futures_lite::future::or(ticket.wait(), async {
                smol::Timer::after(TEST_TIMEOUT).await;
                panic!(
                    "waiting for explicit settlement: actor={:?}, events={:?}",
                    actor.snapshot(),
                    events.drain().collect::<Vec<_>>()
                );
            })
            .await;
            assert_eq!(
                matches!(
                    outcome,
                    TurnOutcome::Cancelled {
                        reason: TurnCancellationReason::User,
                        ..
                    }
                ),
                cancel
            );
            assert_eq!(
                futures_lite::future::or(drain_rx.recv_async(), async {
                    smol::Timer::after(TEST_TIMEOUT).await;
                    panic!("waiting for backend drain: actor={:?}", actor.snapshot());
                })
                .await
                .unwrap(),
                RUN_ID
            );
            let mut terminal = false;
            let mut successor_bubbles = Vec::new();
            loop {
                let envelope = futures_lite::future::or(events.recv_async(), async {
                    smol::Timer::after(TEST_TIMEOUT).await;
                    panic!(
                        "waiting for successor streaming: actor={:?}",
                        actor.snapshot()
                    );
                })
                .await
                .unwrap();
                if envelope.run_id == RUN_ID + 1
                    && let AgentEvent::QueueItemConsumed { text, .. } = &envelope.event
                {
                    successor_bubbles.push(text.clone());
                }
                terminal |= envelope.run_id == RUN_ID
                    && matches!(&envelope.event, AgentEvent::TurnOutcome(_));
                let streamed = envelope.run_id == RUN_ID + 1
                    && matches!(&envelope.event, AgentEvent::TextDelta { .. });
                app.update(crate::app::Msg::Agent(Box::new(envelope)));
                if streamed {
                    break;
                }
            }
            assert!(terminal);
            crate::app::tests::assert_actor_presentation(
                &mut app,
                RUN_ID + 1,
                RESPONSE,
                if deferred {
                    "final deferred"
                } else {
                    "successor"
                },
            );
            if deferred {
                assert_eq!(successor_bubbles, ["earlier deferred", "final deferred"]);
                assert_eq!(actor_run_id.load(Ordering::Relaxed), RUN_ID + 1);
            }
            manager
                .cancel_correlation(
                    root.id(),
                    &format!("{ROOT_CORRELATION_PREFIX}{RUN_ID}"),
                    TurnCancellationReason::User,
                )
                .unwrap();
            if let Some(successor) = &successor {
                assert!(successor.peek().is_none());
            }
            if deferred && cancel {
                manager
                    .cancel_correlation(
                        root.id(),
                        &format!("{ROOT_CORRELATION_PREFIX}{}", RUN_ID + 1),
                        TurnCancellationReason::User,
                    )
                    .unwrap();
            } else {
                release.send(()).unwrap();
            }
            if let Some(successor) = successor {
                assert!(matches!(
                    futures_lite::future::or(successor.wait(), async {
                        smol::Timer::after(TEST_TIMEOUT).await;
                        panic!("waiting for successor settlement");
                    })
                    .await,
                    TurnOutcome::Completed { .. }
                ));
            }
            assert_eq!(drain_rx.recv_async().await.unwrap(), RUN_ID + 1);
            let settled = events.drain().collect::<Vec<_>>();
            assert!(settled.iter().any(|envelope| {
                envelope.run_id == RUN_ID + 1
                    && matches!(&envelope.event, AgentEvent::TurnOutcome(outcome)
                        if matches!(outcome, TurnOutcome::Cancelled { reason: TurnCancellationReason::User, .. }) == (deferred && cancel))
            }));
            for envelope in settled {
                app.update(crate::app::Msg::Agent(Box::new(envelope)));
            }
            assert_eq!(app.status, crate::components::Status::Idle);
            if !(deferred && cancel) {
                crate::app::tests::assert_actor_response(&mut app, RESPONSE);
            }
            futures_lite::future::or(
                async {
                    loop {
                        let mut published = false;
                        actor.publish_if_empty(|| {
                            EventSender::new(agent_tx.clone(), RUN_ID + 1)
                                .try_send(AgentEvent::QueueDrained);
                            published = true;
                        });
                        if published {
                            break;
                        }
                        smol::future::yield_now().await;
                    }
                },
                async {
                    smol::Timer::after(TEST_TIMEOUT).await;
                    panic!("waiting for empty actor queue");
                },
            )
            .await;
            assert!(matches!(
                futures_lite::future::or(events.recv_async(), async {
                    smol::Timer::after(TEST_TIMEOUT).await;
                    panic!("waiting for queue drained: actor={:?}", actor.snapshot());
                })
                .await
                .unwrap(),
                Envelope {
                    run_id,
                    event: AgentEvent::QueueDrained,
                    ..
                } if run_id == RUN_ID + 1
            ));
            actor.close();
            assert!(
                manager
                    .shutdown(std::time::Duration::from_secs(1))
                    .await
                    .timed_out
                    .is_empty()
            );
        });
    }

    #[test_case(false; "explicit_turn")]
    #[test_case(true; "undisplayed_deferred_root")]
    fn ui_cancel_targets_buffered_active_root_without_cancelling_future_turn(deferred: bool) {
        const FIRST_RUN: u64 = 7;
        const SECOND_RUN: u64 = FIRST_RUN + 1;
        const TEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);
        const ROUTER_BARRIER: &str = "cancel-router-barrier";
        const SHARED_CORRELATION: &str = "plugin-shared-correlation";

        smol::block_on(futures_lite::future::or(
            async {
                let (mut backend, _, events) = failing_backend();
                let make_input = || {
                    AgentInput::from_defaults(
                        "explicit root".into(),
                        AgentMode::Build,
                        Vec::new(),
                        Default::default(),
                    )
                };
                let (release, released) = flume::bounded(1);
                let provider = Arc::new(ExplicitProvider {
                    release: released.clone(),
                });
                backend.run_id.store(FIRST_RUN - 1, Ordering::Relaxed);
                let actor_run_id = Arc::clone(&backend.run_id);
                let manager = maki_agent::AgentManagerHandle::new(Default::default()).unwrap();
                let root = manager
                    .create_root_with_config(
                        Some(
                            maki_agent::EffectiveAgentConfig::new(
                                maki_agent::RunSettings {
                                    provider,
                                    model: crate::components::test_model(),
                                    fast: false,
                                    workflow: false,
                                    thinking: Default::default(),
                                },
                                AgentMode::Build,
                            )
                            .with_mode_def(
                                maki_agent::ModeRegistry::builtin().get(&maki_agent::ModeId::Build),
                            ),
                        ),
                        Vec::new(),
                        None,
                        |agent_id| {
                            backend.agent_id = agent_id;
                            Ok::<Box<dyn ActorBackend>, String>(Box::new(backend))
                        },
                    )
                    .unwrap();
                let actor = root.actor().unwrap();
                let queue = crate::agent::shared_queue::actor_queue(
                    Arc::new(actor.clone()),
                    Arc::clone(&actor_run_id),
                );
                let mut app =
                    crate::app::tests::actor_presentation_app(queue.clone(), FIRST_RUN - 1);
                let (cmd_tx, cmd_rx) = flume::unbounded();
                let subagent_cancels = Arc::new(CancelMap::new());
                let (init_trigger, _) = CancelToken::new();
                super::super::command_router::spawn_command_router(
                    cmd_rx,
                    manager.clone(),
                    root.id(),
                    Arc::clone(&subagent_cancels),
                    init_trigger,
                );
                let first = actor.admit_turn(make_input(), None, String::new()).unwrap();
                loop {
                    let envelope = events.recv_async().await.unwrap();
                    assert_eq!(envelope.run_id, FIRST_RUN);
                    let streaming = matches!(envelope.event, AgentEvent::TextDelta { .. });
                    app.update(crate::app::Msg::Agent(Box::new(envelope)));
                    if streaming {
                        break;
                    }
                }
                let second = if deferred {
                    queue.push(crate::agent::shared_queue::QueueItem::Message {
                        text: "deferred root".into(),
                        image_count: 0,
                        input: make_input(),
                        run_id: FIRST_RUN,
                        displayed: false,
                    });
                    None
                } else {
                    Some(
                        actor
                            .admit_turn(make_input(), None, SHARED_CORRELATION.into())
                            .unwrap(),
                    )
                };
                release.send(()).unwrap();
                assert!(matches!(first.wait().await, TurnOutcome::Completed { .. }));

                let mut buffered = Vec::new();
                loop {
                    let envelope = events.recv_async().await.unwrap();
                    let second_streaming = envelope.run_id == SECOND_RUN
                        && matches!(envelope.event, AgentEvent::TextDelta { .. });
                    buffered.push(envelope);
                    if second_streaming {
                        break;
                    }
                }
                let second = second.unwrap_or_else(|| {
                    actor
                        .turn_ticket(actor.snapshot().active_turn.unwrap())
                        .unwrap()
                });
                assert!(buffered.iter().any(|envelope| {
                    envelope.run_id == SECOND_RUN
                        && matches!(envelope.event, AgentEvent::QueueItemConsumed { .. })
                }));
                assert_eq!(app.run_id, FIRST_RUN);
                assert_eq!(app.status, crate::components::Status::Streaming);
                assert_eq!(actor_run_id.load(Ordering::Relaxed), SECOND_RUN);
                assert!(second.peek().is_none());
                const PENDING_RUN: u64 = SECOND_RUN + 1;
                queue.push(crate::agent::shared_queue::QueueItem::Message {
                    text: "pending successor".into(),
                    image_count: 0,
                    input: make_input(),
                    run_id: PENDING_RUN,
                    displayed: true,
                });
                assert_eq!(queue.current_run_id(), PENDING_RUN);
                assert_eq!(actor.snapshot().active_turn, Some(second.turn_id()));
                while !actor.snapshot().queue.iter().any(|item| {
                    matches!(item, maki_agent::actor::QueueProjection::Turn(correlation)
                        if correlation == &format!("{ROOT_CORRELATION_PREFIX}{PENDING_RUN}"))
                }) {
                    smol::future::yield_now().await;
                }
                let actions = app.update(crate::app::Msg::Key(crossterm::event::KeyEvent::new(
                    crossterm::event::KeyCode::Char('c'),
                    crossterm::event::KeyModifiers::CONTROL,
                )));
                let [crate::components::Action::CancelAgent { turn_id, run_id }] =
                    actions.as_slice()
                else {
                    panic!("UI cancel did not dispatch cancellation");
                };
                assert_eq!(*turn_id, Some(second.turn_id()));
                assert_eq!(*run_id, FIRST_RUN);
                let cancelled_turn_id = *turn_id;
                let cancelled_run_id = *run_id;
                assert_eq!(app.run_id, PENDING_RUN + 1);
                cmd_tx
                    .send(crate::agent::AgentCommand::Cancel {
                        turn_id: cancelled_turn_id,
                        run_id: cancelled_run_id,
                    })
                    .unwrap();
                assert!(matches!(
                    second.wait().await,
                    TurnOutcome::Cancelled {
                        reason: TurnCancellationReason::User,
                        ..
                    }
                ));
                assert!(queue.is_empty());
                assert!(released.is_empty());
                for envelope in buffered {
                    app.update(crate::app::Msg::Agent(Box::new(envelope)));
                }
                assert_eq!(app.run_id, PENDING_RUN + 1);
                assert_eq!(app.status, crate::components::Status::Idle);

                let future = if deferred {
                    queue.push(crate::agent::shared_queue::QueueItem::Message {
                        text: "future deferred root".into(),
                        image_count: 0,
                        input: make_input(),
                        run_id: FIRST_RUN,
                        displayed: false,
                    });
                    None
                } else {
                    Some(
                        actor
                            .admit_turn(make_input(), None, SHARED_CORRELATION.into())
                            .unwrap(),
                    )
                };
                loop {
                    let envelope = events.recv_async().await.unwrap();
                    let streaming = envelope.run_id == PENDING_RUN + 2
                        && matches!(envelope.event, AgentEvent::TextDelta { .. });
                    app.update(crate::app::Msg::Agent(Box::new(envelope)));
                    if streaming {
                        break;
                    }
                }
                let future = future.unwrap_or_else(|| {
                    actor
                        .turn_ticket(actor.snapshot().active_turn.unwrap())
                        .unwrap()
                });
                assert_eq!(app.run_id, PENDING_RUN + 2);
                assert_eq!(app.status, crate::components::Status::Streaming);
                let (barrier_trigger, barrier) = CancelToken::new();
                subagent_cancels.insert(ROUTER_BARRIER.into(), barrier_trigger);
                cmd_tx
                    .send(crate::agent::AgentCommand::Cancel {
                        turn_id: cancelled_turn_id,
                        run_id: cancelled_run_id,
                    })
                    .unwrap();
                cmd_tx
                    .send(crate::agent::AgentCommand::CancelSubagent {
                        tool_use_id: ROUTER_BARRIER.into(),
                    })
                    .unwrap();
                barrier.cancelled().await;
                assert!(future.peek().is_none());
                release.send(()).unwrap();
                assert!(matches!(future.wait().await, TurnOutcome::Completed { .. }));
                drop(cmd_tx);
                actor.close();
                assert!(
                    manager
                        .shutdown(std::time::Duration::from_secs(1))
                        .await
                        .timed_out
                        .is_empty()
                );
            },
            async {
                smol::Timer::after(TEST_TIMEOUT).await;
                panic!("buffered-root cancellation regression timed out");
            },
        ));
    }

    #[test]
    fn tui_queue_adapter_folds_into_initial_user_turn() {
        smol::block_on(futures_lite::future::or(
            async {
                use crate::agent::shared_queue::{QueueItem, actor_queue};
                const INITIAL: &str = "initial user instruction";
                const FOLLOWUPS: [&str; 2] =
                    ["first deferred instruction", "second deferred instruction"];
                let (mut backend, _, events) = failing_backend();
                let (requests_tx, requests) = flume::unbounded();
                let (release, released) = flume::bounded(1);
                let run_id = Arc::clone(&backend.run_id);
                let manager = maki_agent::AgentManagerHandle::new(Default::default()).unwrap();
                let root = manager
                    .create_root_with_config(
                        Some(
                            maki_agent::EffectiveAgentConfig::new(
                                maki_agent::RunSettings {
                                    provider: Arc::new(QueueProvider {
                                        requests: requests_tx,
                                        release: released,
                                    }),
                                    model: crate::components::test_model(),
                                    fast: false,
                                    workflow: false,
                                    thinking: Default::default(),
                                },
                                AgentMode::Build,
                            )
                            .with_mode_def(
                                maki_agent::ModeRegistry::builtin().get(&maki_agent::ModeId::Build),
                            ),
                        ),
                        Vec::new(),
                        None,
                        |agent_id| {
                            backend.agent_id = agent_id;
                            Ok::<Box<dyn ActorBackend>, String>(Box::new(backend))
                        },
                    )
                    .unwrap();
                let actor = root.actor().unwrap();
                let queue = actor_queue(Arc::new(actor.clone()), run_id);
                let mut app = crate::app::tests::actor_presentation_app(queue.clone(), 0);
                app.update(crate::app::Msg::Paste(INITIAL.into()));
                let actions = app.update(crate::app::Msg::Key(crate::components::key(
                    crossterm::event::KeyCode::Enter,
                )));
                let Some(crate::components::Action::SendMessage(mut input)) =
                    actions.into_iter().next()
                else {
                    panic!("expected initial user submission");
                };
                input.prompt = None;
                let submitted_run = app.run_id;
                queue.push(QueueItem::Message {
                    text: INITIAL.into(),
                    image_count: 0,
                    input: *input,
                    run_id: submitted_run,
                    displayed: true,
                });
                let first = requests.recv_async().await.unwrap();
                assert!(serde_json::to_string(&first).unwrap().contains(INITIAL));
                let ticket = actor
                    .turn_ticket(actor.snapshot().active_turn.unwrap())
                    .unwrap();
                assert_eq!(
                    ticket.provenance().origin,
                    maki_agent::actor::TurnOrigin::User
                );
                for text in FOLLOWUPS {
                    let images = vec![maki_agent::ImageSource::new(
                        maki_agent::ImageMediaType::Png,
                        Arc::from(
                            "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+jRZkAAAAASUVORK5CYII=",
                        ),
                    )];
                    queue.push(QueueItem::Message {
                        text: text.into(),
                        image_count: images.len(),
                        input: AgentInput::from_defaults(
                            text.into(),
                            AgentMode::Build,
                            images,
                            Default::default(),
                        ),
                        run_id: submitted_run,
                        displayed: false,
                    });
                }
                assert_eq!(queue.panel_len(), FOLLOWUPS.len());
                release.send(()).unwrap();
                let next = requests.recv_async().await.unwrap();
                let serialized = serde_json::to_string(&next).unwrap();
                for text in FOLLOWUPS {
                    assert!(
                        serialized.contains(text),
                        "missing {text} from next request"
                    );
                }
                assert!(serialized.contains("image/png"));
                assert_eq!(actor.snapshot().active_turn, Some(ticket.turn_id()));
                assert!(queue.is_empty());
                release.send(()).unwrap();
                assert!(matches!(ticket.wait().await, TurnOutcome::Completed { .. }));
                for envelope in events.drain() {
                    assert_eq!(envelope.run_id, submitted_run);
                    app.update(crate::app::Msg::Agent(Box::new(envelope)));
                }
                let chat = app.main_chat();
                for text in [INITIAL, FOLLOWUPS[0], FOLLOWUPS[1]] {
                    assert_eq!(
                        (0..chat.message_count())
                            .filter(|&i| chat
                                .message_at(i)
                                .is_some_and(|message| message.text == text))
                            .count(),
                        1
                    );
                }
                actor.close();
                assert!(
                    manager
                        .shutdown(std::time::Duration::from_secs(1))
                        .await
                        .timed_out
                        .is_empty()
                );
            },
            async {
                smol::Timer::after(std::time::Duration::from_secs(10)).await;
                panic!("production queue folding timed out");
            },
        ));
    }

    struct QueueProvider {
        requests: flume::Sender<Vec<Message>>,
        release: flume::Receiver<()>,
    }

    impl maki_providers::provider::Provider for QueueProvider {
        fn stream_message<'a>(
            &'a self,
            _: &'a Model,
            messages: &'a [Message],
            _: &'a str,
            _: &'a serde_json::Value,
            _: &'a flume::Sender<maki_providers::ProviderEvent>,
            _: maki_providers::RequestOptions,
            _: Option<&'a SessionRef>,
        ) -> maki_providers::provider::BoxFuture<
            'a,
            Result<maki_providers::StreamResponse, AgentError>,
        > {
            Box::pin(async move {
                self.requests.send(messages.to_vec()).unwrap();
                self.release.recv_async().await.unwrap();
                Ok(maki_providers::StreamResponse {
                    message: Message {
                        role: maki_providers::Role::Assistant,
                        content: vec![maki_providers::ContentBlock::Text {
                            text: "request completed".into(),
                        }],
                        ..Default::default()
                    },
                    usage: Default::default(),
                    stop_reason: Some(maki_providers::StopReason::EndTurn),
                })
            })
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

    #[test_case(false; "production_immediate_admitted_turn")]
    #[test_case(true; "displayed_root_current_presentation")]
    fn ui_submission_fires_one_turn_start(root_work: bool) {
        smol::block_on(async {
            const PROMPT: &str = "immediate UI prompt";
            const RESPONSE: &str = "visible explicit response";
            let (mut backend, _, events) = failing_backend();
            let (release, released) = flume::bounded(1);
            let (drain_tx, drain_rx) = flume::unbounded();
            backend.drain_tx = drain_tx;
            let run_id = Arc::clone(&backend.run_id);
            let manager = maki_agent::AgentManagerHandle::new(Default::default()).unwrap();
            let root = manager
                .create_root_with_config(
                    Some(
                        maki_agent::EffectiveAgentConfig::new(
                            maki_agent::RunSettings {
                                provider: Arc::new(ExplicitProvider { release: released }),
                                model: crate::components::test_model(),
                                fast: false,
                                workflow: false,
                                thinking: Default::default(),
                            },
                            AgentMode::Build,
                        )
                        .with_mode_def(
                            maki_agent::ModeRegistry::builtin().get(&maki_agent::ModeId::Build),
                        ),
                    ),
                    Vec::new(),
                    None,
                    |agent_id| {
                        backend.agent_id = agent_id;
                        Ok::<Box<dyn ActorBackend>, String>(Box::new(backend))
                    },
                )
                .unwrap();
            let actor = root.actor().unwrap();
            let lifecycle = actor.subscribe_events();
            let queue = crate::agent::shared_queue::actor_queue(
                Arc::new(actor.clone()),
                Arc::clone(&run_id),
            );
            let mut app = crate::app::tests::actor_presentation_app(queue.clone(), 0);
            let host = maki_lua::PluginHost::new(Arc::new(ToolRegistry::new())).unwrap();
            host.load_source(
                "turn_start_count",
                r#"
                local starts = 0
                maki.api.create_autocmd("TurnStart", { callback = function() starts += 1 end })
                maki.api.register_command({ name = "/assert_single_start", tui_only = false, handler = function() assert(starts == 1, "TurnStart count: " .. starts) end })
                "#,
            )
            .unwrap();
            app.lua_event_handle = host.event_handle();
            app.update(crate::app::Msg::Paste(PROMPT.into()));
            let actions = app.update(crate::app::Msg::Key(crate::components::key(
                crossterm::event::KeyCode::Enter,
            )));
            assert_eq!(actions.len(), 1);
            let Some(crate::components::Action::SendMessage(mut input)) =
                actions.into_iter().next()
            else {
                panic!("expected immediate submission");
            };
            input.prompt = None;
            let submitted_run = app.run_id;
            let text = input.message.clone();
            let images = input.images.clone();
            if root_work {
                actor
                    .rush(maki_agent::actor::RootWork::new(
                        *input,
                        submitted_run,
                        true,
                        text,
                        images,
                        format!("{ROOT_CORRELATION_PREFIX}{submitted_run}"),
                    ))
                    .unwrap();
            } else {
                queue.push(crate::agent::shared_queue::QueueItem::Message {
                    text,
                    image_count: images.len(),
                    input: *input,
                    run_id: submitted_run,
                    displayed: true,
                });
            }
            loop {
                let envelope = futures_lite::future::or(events.recv_async(), async {
                    smol::Timer::after(std::time::Duration::from_secs(10)).await;
                    panic!("waiting for immediate response");
                })
                .await
                .unwrap();
                assert_eq!(envelope.run_id, submitted_run);
                let streamed = matches!(&envelope.event, AgentEvent::TextDelta { .. });
                app.update(crate::app::Msg::Agent(Box::new(envelope)));
                if streamed {
                    break;
                }
            }
            release.send(()).unwrap();
            let drained = futures_lite::future::or(drain_rx.recv_async(), async {
                smol::Timer::after(std::time::Duration::from_secs(10)).await;
                panic!("waiting for immediate turn settlement");
            })
            .await
            .unwrap();
            assert_eq!(drained, submitted_run);
            assert!(
                lifecycle
                    .drain()
                    .any(|event| matches!(event, maki_agent::actor::ActorEvent::End(_)))
            );
            for envelope in events.drain() {
                assert_eq!(envelope.run_id, submitted_run);
                app.update(crate::app::Msg::Agent(Box::new(envelope)));
            }
            assert_eq!(app.run_id, submitted_run);
            assert_eq!(run_id.load(Ordering::Relaxed), submitted_run);
            host.wait_for_worker_barrier_for_test();
            assert_eq!(
                host.event_handle()
                    .run_command_for_test(
                        Arc::from("turn_start_count"),
                        Arc::from("/assert_single_start"),
                        String::new(),
                        0,
                    )
                    .recv_timeout(std::time::Duration::from_secs(10))
                    .unwrap(),
                Ok(())
            );
            let chat = app.main_chat();
            assert_eq!(
                (0..chat.message_count())
                    .filter(|&index| chat.message_at(index).is_some_and(|m| m.text == PROMPT))
                    .count(),
                1
            );
            crate::app::tests::assert_actor_response(&mut app, RESPONSE);
            actor.close();
            assert!(
                manager
                    .shutdown(std::time::Duration::from_secs(1))
                    .await
                    .timed_out
                    .is_empty()
            );
        });
    }

    #[test_case(false, false, false; "consumed_before_initialization")]
    #[test_case(false, true, false; "displayed_root_adopts_without_duplicate_bubble")]
    #[test_case(false, true, true; "current_displayed_root_reuses_presentation")]
    #[test_case(true, false, false; "cancelled_admission_cannot_bypass_fence")]
    #[test_case(true, true, true; "cancelled_current_displayed_root_cannot_restart")]
    fn deferred_root_presentation_respects_admission_cancellation(
        cancelled: bool,
        displayed: bool,
        current: bool,
    ) {
        smol::block_on(async {
            const ADMISSION_RUN: u64 = 1;
            const ALLOCATED_RUN: u64 = 7;
            let (mut backend, mut input, events) = failing_backend();
            input.prompt = None;
            backend.initialized = false;
            let allocated_run = if current {
                ADMISSION_RUN
            } else {
                ALLOCATED_RUN
            };
            backend.run_id.store(allocated_run, Ordering::Relaxed);
            let (trigger, cancel) = CancelToken::new();
            let (reason_trigger, cancel_reason) = ReasonedCancelToken::new();
            if cancelled {
                reason_trigger.cancel(TurnCancellationReason::User);
                trigger.cancel();
            } else {
                let (init_trigger, init_cancel) = CancelToken::new();
                init_trigger.cancel();
                backend.init_cancel = init_cancel;
            }
            let context = TurnContext {
                agent_id: backend.agent_id,
                turn_id: Some(TurnId::generate()),
                output: Default::default(),
                provenance: Default::default(),
                cancel,
                cancel_reason,
                correlation: format!("{ROOT_CORRELATION_PREFIX}{ADMISSION_RUN}"),
                generation: 0,
                policy: None,
                admission: None,
                interrupt: None,
                inbox: None,
                managed_turn: None,
            };
            let work = WorkKind::Root {
                run_id: ADMISSION_RUN,
                displayed,
                text: input.message.clone(),
                images: Vec::new(),
                earlier: Vec::new(),
                mcp_startup_notice: None,
                already_displayed: displayed,
            };
            let mut history = History::new(Vec::new());
            let result = backend.run_turn(&mut history, context, input, work).await;
            assert!(matches!(result, BackendResult::EnteredRun(_)));
            let emitted = events.drain().collect::<Vec<_>>();
            assert!(emitted.iter().any(|envelope| {
                matches!(&envelope.event, AgentEvent::TurnOutcome(outcome)
                    if matches!(outcome, TurnOutcome::Cancelled { .. }) == cancelled)
            }));
            let presentation_run = if cancelled || current {
                ADMISSION_RUN
            } else {
                allocated_run + 1
            };
            assert!(emitted.iter().all(|event| event.run_id == presentation_run));
            if displayed && !cancelled {
                assert!(matches!(
                    &emitted[0].event,
                    AgentEvent::QueueItemConsumed {
                        already_displayed: true,
                        ..
                    }
                ));
            }
            assert_eq!(
                emitted
                    .iter()
                    .any(|event| matches!(event.event, AgentEvent::QueueItemConsumed { .. })),
                !cancelled
            );
            assert_eq!(
                backend.run_id.load(Ordering::Relaxed),
                if cancelled || current {
                    allocated_run
                } else {
                    allocated_run + 1
                }
            );
            if displayed && !cancelled {
                crate::app::tests::assert_displayed_root_adoption(
                    emitted.into_iter().next().unwrap(),
                    presentation_run,
                );
            }
        });
    }

    #[test]
    fn inherited_template_has_environment_before_initialization() {
        let (mut backend, _, _) = failing_backend();
        backend.initialized = false;
        assert_eq!(backend.vars.apply("{cwd}"), "/tmp");
        assert_eq!(backend.vars.apply("{platform}"), std::env::consts::OS);
        let date = backend.vars.apply("{date}");
        assert_eq!(date.len(), "YYYY-MM-DD".len());
        assert!(!date.contains('{'));
        let template = backend.orchestration_template();
        assert_eq!(template.cwd, PathBuf::from("/tmp"));
        let tools =
            serde_json::to_string(template.request_tools.as_ref().unwrap().definitions()).unwrap();
        for unresolved in ["{cwd}", "{platform}", "{date}"] {
            assert!(!tools.contains(unresolved));
        }
    }

    #[test_case(true, false; "enabled")]
    #[test_case(false, false; "disabled")]
    #[test_case(true, true; "restricted_parent")]
    fn command_spawn_uses_production_mcp_template(enabled: bool, restricted: bool) {
        use maki_lua::orchestration::TrustedTarget;

        smol::block_on(async {
            const COMMAND: &str = "/ui-mcp-spawn";
            const MCP_EXECUTED: &str = "fake MCP executed";
            const MCP_DENIED: &str = "MCP denied";
            let expected = if enabled && !restricted {
                MCP_EXECUTED
            } else {
                MCP_DENIED
            };
            let (mut backend, _, _) = failing_backend();
            let host = maki_lua::PluginHost::new(Arc::clone(ToolRegistry::global_arc())).unwrap();
            backend.lua_handle = host.event_handle();
            backend.permissions.set_yolo(true);
            backend.mcp = Some(maki_agent::mcp::test_support::stub_session_with_result(
                &[("stub.echo", "echo")],
                MCP_EXECUTED,
            ));
            let mut template = backend.orchestration_template();
            template.provider = Arc::new(CommandMcpProvider);
            let mut config = maki_agent::EffectiveAgentConfig::new(
                maki_agent::RunSettings {
                    provider: Arc::clone(&template.provider),
                    model: (*template.model).clone(),
                    fast: false,
                    workflow: false,
                    thinking: Default::default(),
                },
                AgentMode::Build,
            )
            .with_mode_def(maki_agent::ModeRegistry::builtin().get(&maki_agent::ModeId::Build));
            if restricted {
                let mut mode = maki_agent::ModeRegistry::builtin()
                    .get(&maki_agent::ModeId::Build)
                    .unwrap();
                mode.restrict_write_to = Some(PathBuf::from("/tmp/ui-mcp-plan.md"));
                config = config.with_mode_def(Some(mode));
            }
            let manager = maki_agent::AgentManagerHandle::new(Default::default()).unwrap();
            let root = manager
                .create_root_with_config(Some(config), Vec::new(), None, |agent_id| {
                    backend.agent_id = agent_id;
                    Ok::<Box<dyn ActorBackend>, String>(Box::new(backend))
                })
                .unwrap();
            let orchestration = Arc::new(crate::agent::orchestration::AgentOrchestration::new(
                Arc::new(arc_swap::ArcSwapOption::empty()),
            ));
            let pinned = TrustedTarget {
                target: root.clone(),
                template,
            };
            orchestration.register_root(pinned.clone());
            let pinned = TrustedTarget {
                target: root.clone(),
                template: maki_lua::orchestration::OrchestrationServices::template(
                    &*orchestration,
                    &root,
                )
                .unwrap(),
            };
            orchestration.select(Some(root.clone()));
            host.event_handle()
                .install_orchestration_services(orchestration);
            host.load_source(
                "ui_mcp_spawn",
                &format!(
                    r#"
                maki.api.register_command({{
                    name = "{COMMAND}", tui_only = false,
                    handler = function(_, ctx)
                        local ok, err = pcall(function()
                            assert(ctx ~= nil)
                            local child = assert(maki.agent.spawn(ctx, {{
                                inherit_provider = true, silent = true, mcp = {enabled},
                            }}))
                            local result = assert(assert(child:send(ctx, "invoke MCP")):wait(ctx))
                            assert(result.text == "{expected}", maki.json.encode(result))
                            assert(child:close(ctx))
                            maki.ui.flash(result.text)
                        end)
                        if not ok then maki.ui.flash(tostring(err)) end
                    end,
                }})
            "#,
                ),
            )
            .unwrap();
            let actions = host.ui_action_rx();
            let commands = host.command_registry();
            let target = commands.bind_target(
                maki_commands::TargetCapabilities::ALL,
                Arc::new(McpCommandHost(pinned)),
            );
            let outcome =
                futures_lite::future::or(commands.dispatch_input(&target, COMMAND.into()), async {
                    smol::Timer::after(std::time::Duration::from_secs(10)).await;
                    panic!("command-origin MCP spawn timed out");
                })
                .await;
            assert!(
                matches!(
                    outcome,
                    maki_commands::InputDispatch::Dispatched(
                        maki_commands::CommandOutcome::Completed
                    )
                ),
                "{outcome:?}"
            );
            let action = futures_lite::future::or(actions.recv_async(), async {
                smol::Timer::after(std::time::Duration::from_secs(10)).await;
                panic!("command-origin MCP handler did not finish");
            })
            .await
            .unwrap();
            let maki_lua::UiAction::Flash(text) = action else {
                panic!("unexpected command-origin MCP action");
            };
            assert_eq!(text, expected);
            root.actor().unwrap().close();
            assert!(
                manager
                    .shutdown(std::time::Duration::from_secs(1))
                    .await
                    .timed_out
                    .is_empty()
            );
        });
    }

    struct McpCommandHost(maki_lua::orchestration::TrustedTarget);

    impl maki_commands::CommandHost for McpCommandHost {
        fn admission_context(&self) -> Option<Arc<dyn Any + Send + Sync>> {
            Some(Arc::new(self.0.clone()))
        }

        fn request(
            &self,
            _: maki_commands::HostRequest,
        ) -> maki_commands::CommandFuture<
            Result<maki_commands::HostResponse, maki_commands::CommandError>,
        > {
            Box::pin(async { Ok(maki_commands::HostResponse::Completed) })
        }
    }

    struct CommandMcpProvider;

    impl maki_providers::provider::Provider for CommandMcpProvider {
        fn stream_message<'a>(
            &'a self,
            _: &'a Model,
            messages: &'a [Message],
            _: &'a str,
            _: &'a serde_json::Value,
            events: &'a flume::Sender<maki_providers::ProviderEvent>,
            _: maki_providers::RequestOptions,
            _: Option<&'a SessionRef>,
        ) -> maki_providers::provider::BoxFuture<
            'a,
            Result<maki_providers::StreamResponse, AgentError>,
        > {
            Box::pin(async move {
                let serialized = serde_json::to_string(messages).unwrap();
                let answered = serialized.contains("tool_result");
                let text = if serialized.contains("fake MCP executed") {
                    "fake MCP executed"
                } else {
                    "MCP denied"
                };
                if answered {
                    events
                        .send(maki_providers::ProviderEvent::TextDelta { text: text.into() })
                        .unwrap();
                }
                Ok(maki_providers::StreamResponse {
                    message: Message {
                        role: maki_providers::Role::Assistant,
                        content: if answered {
                            vec![maki_providers::ContentBlock::Text { text: text.into() }]
                        } else {
                            vec![maki_providers::ContentBlock::ToolUse {
                                id: "ui-mcp-call".into(),
                                name: "stub.echo".into(),
                                input: serde_json::json!({}),
                                thought_signature: None,
                            }]
                        },
                        ..Default::default()
                    },
                    usage: Default::default(),
                    stop_reason: Some(if answered {
                        maki_providers::StopReason::EndTurn
                    } else {
                        maki_providers::StopReason::ToolUse
                    }),
                })
            })
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

    #[test_case(false; "idle_continuation")]
    #[test_case(true; "active_parent_boundary")]
    fn child_completion_reaches_production_tui_presentation(active: bool) {
        smol::block_on(async {
            const RUN_ID: u64 = 7;
            const CHILD_INPUT: &str = "child work";
            const TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);
            futures_lite::future::or(async {
                let (mut backend, mut input, events) = failing_backend();
                input.prompt = None;
                backend.run_id.store(RUN_ID - 1, Ordering::Relaxed);
                let run_id = Arc::clone(&backend.run_id);
                let (release, released) = flume::unbounded();
                let (requests, request_rx) = flume::unbounded();
                let config = maki_agent::EffectiveAgentConfig::new(
                    maki_agent::RunSettings {
                        provider: Arc::new(CompletionProvider {
                            inner: ExplicitProvider { release: released }, requests,
                        }),
                        model: crate::components::test_model(),
                        fast: false, workflow: false, thinking: Default::default(),
                    }, AgentMode::Build,
                ).with_mode_def(maki_agent::ModeRegistry::builtin().get(&maki_agent::ModeId::Build));
                let manager = maki_agent::AgentManagerHandle::new(Default::default()).unwrap();
                let root = manager.create_root_with_config(
                    Some(config.clone()), Vec::new(), None, |agent_id| {
                        backend.agent_id = agent_id;
                        Ok::<Box<dyn ActorBackend>, String>(Box::new(backend))
                    },
                ).unwrap();
                let actor = root.actor().unwrap();
                let mut app = crate::app::tests::actor_presentation_app(
                    crate::agent::shared_queue::actor_queue(Arc::new(actor.clone()), run_id),
                    RUN_ID - 1,
                );
                let actor_events = actor.subscribe_events();
                let parent = active.then(|| actor.admit_turn(input, None, String::new()).unwrap());
                if active {
                    request_rx.recv_async().await.unwrap();
                    loop {
                        let envelope = events.recv_async().await.unwrap();
                        let streamed = matches!(envelope.event, AgentEvent::TextDelta { .. });
                        app.update(crate::app::Msg::Agent(Box::new(envelope)));
                        if streamed { break; }
                    }
                }
                let (mut child_backend, mut child_input, _child_events) = failing_backend();
                child_input.prompt = None;
                child_input.message = CHILD_INPUT.into();
                let (child_release, child_released) = flume::unbounded();
                child_release.send(()).unwrap();
                let mut child_config = config;
                child_config.settings.provider = Arc::new(ExplicitProvider { release: child_released });
                let child = manager.spawn_child_trusted_with_config(
                    &root, child_config, Default::default(), Vec::new(), None, |agent_id| {
                        child_backend.agent_id = agent_id;
                        Ok::<Box<dyn ActorBackend>, String>(Box::new(child_backend))
                    },
                ).unwrap();
                let child_actor = child.actor().unwrap();
                child_actor.configure_parent_delivery(true, None).unwrap();
                let child_ticket = child_actor.admit_turn(child_input, None, String::new()).unwrap();
                assert!(matches!(child_ticket.wait().await, TurnOutcome::Completed { .. }));
                if active { release.send(()).unwrap(); }
                let request = serde_json::to_string(&request_rx.recv_async().await.unwrap()).unwrap();
                assert!(request.contains(EXPLICIT_RESPONSE), "missing child observation: {request}");
                assert!(request.contains(&child_ticket.turn_id().to_string()), "missing child identity: {request}");
                assert!(!request.contains(CHILD_INPUT));
                let mut bubbles = 0;
                loop {
                    let envelope = events.recv_async().await.unwrap();
                    assert_eq!(envelope.run_id, RUN_ID);
                    bubbles += usize::from(matches!(envelope.event, AgentEvent::QueueItemConsumed { .. }));
                    let streamed = matches!(envelope.event, AgentEvent::TextDelta { .. });
                    app.update(crate::app::Msg::Agent(Box::new(envelope)));
                    if streamed { break; }
                }
                assert_eq!(bubbles, 0);
                assert_eq!(app.run_id, RUN_ID);
                assert_eq!(app.status, crate::components::Status::Streaming);
                release.send(()).unwrap();
                loop {
                    let envelope = events.recv_async().await.unwrap();
                    let terminal = matches!(envelope.event, AgentEvent::TurnOutcome(_));
                    app.update(crate::app::Msg::Agent(Box::new(envelope)));
                    if terminal { break; }
                }
                let expected = if active {
                    EXPLICIT_RESPONSE.repeat(2)
                } else {
                    EXPLICIT_RESPONSE.into()
                };
                crate::app::tests::assert_actor_response(&mut app, &expected);
                if let Some(parent) = parent {
                    assert!(matches!(parent.wait().await, TurnOutcome::Completed { .. }));
                }
                let mut consumed = 0;
                for event in actor_events.try_iter() {
                    if matches!(&event, maki_agent::actor::ActorEvent::Delivery(delivery)
                        if matches!(delivery.status, maki_agent::actor::DeliveryStatus::Executed { .. })) {
                        consumed += 1;
                    }
                    app.handle_actor_event(event);
                }
                assert_eq!(consumed, 1);
                manager.shutdown(TIMEOUT).await;
            }, async {
                smol::Timer::after(TIMEOUT).await;
                panic!("child completion did not reach the production TUI path");
            }).await;
        });
    }

    #[test]
    fn child_completion_mid_tool_batch_reaches_next_request_of_same_turn() {
        smol::block_on(async {
            const TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);
            futures_lite::future::or(async {
                let (mut backend, mut input, _events) = failing_backend();
                input.prompt = None;
                let (release, released) = flume::unbounded();
                let (requests, request_rx) = flume::unbounded();
                let config = maki_agent::EffectiveAgentConfig::new(
                    maki_agent::RunSettings {
                        provider: Arc::new(ToolBatchProvider { release: released, requests }),
                        model: crate::components::test_model(),
                        fast: false, workflow: false, thinking: Default::default(),
                    }, AgentMode::Build,
                ).with_mode_def(maki_agent::ModeRegistry::builtin().get(&maki_agent::ModeId::Build));
                let manager = maki_agent::AgentManagerHandle::new(Default::default()).unwrap();
                let root = manager.create_root_with_config(
                    Some(config.clone()), Vec::new(), None, |agent_id| {
                        backend.agent_id = agent_id;
                        Ok::<Box<dyn ActorBackend>, String>(Box::new(backend))
                    },
                ).unwrap();
                let actor = root.actor().unwrap();
                let actor_events = actor.subscribe_events();
                let parent = actor.admit_turn(input, None, String::new()).unwrap();
                request_rx.recv_async().await.unwrap();

                let (mut child_backend, mut child_input, _child_events) = failing_backend();
                child_input.prompt = None;
                let (child_release, child_released) = flume::unbounded();
                child_release.send(()).unwrap();
                let mut child_config = config;
                child_config.settings.provider = Arc::new(ExplicitProvider { release: child_released });
                let child = manager.spawn_child_trusted_with_config(
                    &root, child_config, Default::default(), Vec::new(), None, |agent_id| {
                        child_backend.agent_id = agent_id;
                        Ok::<Box<dyn ActorBackend>, String>(Box::new(child_backend))
                    },
                ).unwrap();
                let child_actor = child.actor().unwrap();
                child_actor.configure_parent_delivery(true, None).unwrap();
                let child_ticket = child_actor.admit_turn(child_input, None, String::new()).unwrap();
                assert!(matches!(child_ticket.wait().await, TurnOutcome::Completed { .. }));
                let mut seen = Vec::new();
                loop {
                    let event = actor_events.recv_async().await.unwrap();
                    let accepted = matches!(&event, ActorEvent::Delivery(delivery)
                        if delivery.status == DeliveryStatus::Accepted
                            && delivery.delivery.result.outcome.turn_id() == child_ticket.turn_id());
                    seen.push(event);
                    if accepted { break; }
                }
                release.send(()).unwrap();

                let request = request_rx.recv_async().await.unwrap();
                let child_turn = child_ticket.turn_id().to_string();
                let tool_result = request.iter().position(|message| message.content.iter().any(|block| {
                    matches!(block, ContentBlock::ToolResult { tool_use_id, .. } if tool_use_id == TOOL_BATCH_CALL_ID)
                })).expect("tool result missing from the follow-up request");
                let observation = request.iter().position(|message| {
                    let serialized = serde_json::to_string(message).unwrap();
                    serialized.contains(&child_turn) && serialized.contains(EXPLICIT_RESPONSE)
                }).expect("child observation missing from the follow-up request");
                assert!(observation > tool_result, "observation must follow the tool result: {request:?}");

                assert!(matches!(parent.wait().await, TurnOutcome::Completed { .. }));
                manager.shutdown(TIMEOUT).await;
                assert!(request_rx.try_recv().is_err(), "parent made an extra provider request");
                seen.extend(actor_events.try_iter());
                let started: Vec<TurnId> = seen.iter().filter_map(|event| match event {
                    ActorEvent::Start { turn_id, .. } => Some(*turn_id),
                    _ => None,
                }).collect();
                assert_eq!(started, [parent.turn_id()]);
                let executed: Vec<TurnId> = seen.iter().filter_map(|event| match event {
                    ActorEvent::Delivery(delivery) => match delivery.status {
                        DeliveryStatus::Executed { parent_turn_id } => Some(parent_turn_id),
                        _ => None,
                    },
                    _ => None,
                }).collect();
                assert_eq!(executed, [parent.turn_id()]);
            }, async {
                smol::Timer::after(TIMEOUT).await;
                panic!("child completion did not reach the parent's follow-up request");
            }).await;
        });
    }

    const TOOL_BATCH_CALL_ID: &str = "tool-batch-call";
    const TOOL_BATCH_TOOL: &str = "absent_tool";

    struct ToolBatchProvider {
        release: flume::Receiver<()>,
        requests: flume::Sender<Vec<Message>>,
    }

    impl maki_providers::provider::Provider for ToolBatchProvider {
        fn stream_message<'a>(
            &'a self,
            _: &'a Model,
            messages: &'a [Message],
            _: &'a str,
            _: &'a serde_json::Value,
            _: &'a flume::Sender<maki_providers::ProviderEvent>,
            _: maki_providers::RequestOptions,
            _: Option<&'a SessionRef>,
        ) -> maki_providers::provider::BoxFuture<
            'a,
            Result<maki_providers::StreamResponse, AgentError>,
        > {
            Box::pin(async move {
                self.requests.send(messages.to_vec()).unwrap();
                let answered = messages.iter().any(|message| {
                    message
                        .content
                        .iter()
                        .any(|block| matches!(block, ContentBlock::ToolResult { .. }))
                });
                let (content, stop_reason) = if answered {
                    (
                        ContentBlock::Text {
                            text: EXPLICIT_RESPONSE.into(),
                        },
                        maki_providers::StopReason::EndTurn,
                    )
                } else {
                    self.release.recv_async().await.unwrap();
                    (
                        ContentBlock::ToolUse {
                            id: TOOL_BATCH_CALL_ID.into(),
                            name: TOOL_BATCH_TOOL.into(),
                            input: serde_json::json!({}),
                            thought_signature: None,
                        },
                        maki_providers::StopReason::ToolUse,
                    )
                };
                Ok(maki_providers::StreamResponse {
                    message: Message {
                        role: maki_providers::Role::Assistant,
                        content: vec![content],
                        ..Default::default()
                    },
                    usage: Default::default(),
                    stop_reason: Some(stop_reason),
                })
            })
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

    struct CompletionProvider {
        inner: ExplicitProvider,
        requests: flume::Sender<Vec<Message>>,
    }

    impl maki_providers::provider::Provider for CompletionProvider {
        fn stream_message<'a>(
            &'a self,
            model: &'a Model,
            messages: &'a [Message],
            system: &'a str,
            tools: &'a serde_json::Value,
            events: &'a flume::Sender<maki_providers::ProviderEvent>,
            options: maki_providers::RequestOptions,
            session: Option<&'a SessionRef>,
        ) -> maki_providers::provider::BoxFuture<
            'a,
            Result<maki_providers::StreamResponse, AgentError>,
        > {
            self.requests.send(messages.to_vec()).unwrap();
            self.inner
                .stream_message(model, messages, system, tools, events, options, session)
        }

        fn list_models(
            &self,
        ) -> maki_providers::provider::BoxFuture<
            '_,
            Result<Vec<maki_providers::ModelInfo>, AgentError>,
        > {
            self.inner.list_models()
        }
    }

    const EXPLICIT_RESPONSE: &str = "visible explicit response";

    struct ExplicitProvider {
        release: flume::Receiver<()>,
    }

    impl maki_providers::provider::Provider for ExplicitProvider {
        fn stream_message<'a>(
            &'a self,
            _: &'a Model,
            _: &'a [Message],
            _: &'a str,
            _: &'a serde_json::Value,
            events: &'a flume::Sender<maki_providers::ProviderEvent>,
            _: maki_providers::RequestOptions,
            _: Option<&'a SessionRef>,
        ) -> maki_providers::provider::BoxFuture<
            'a,
            Result<maki_providers::StreamResponse, AgentError>,
        > {
            Box::pin(async move {
                events
                    .send(maki_providers::ProviderEvent::TextDelta {
                        text: EXPLICIT_RESPONSE.into(),
                    })
                    .unwrap();
                self.release.recv_async().await.unwrap();
                Ok(maki_providers::StreamResponse {
                    message: Message {
                        role: maki_providers::Role::Assistant,
                        content: vec![maki_providers::ContentBlock::Text {
                            text: EXPLICIT_RESPONSE.into(),
                        }],
                        ..Default::default()
                    },
                    usage: Default::default(),
                    stop_reason: Some(maki_providers::StopReason::EndTurn),
                })
            })
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
            None,
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
            None,
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
            output: Default::default(),
            provenance: Default::default(),
            cancel: CancelToken::none(),
            cancel_reason: ReasonedCancelToken::none(),
            correlation: format!("{ROOT_CORRELATION_PREFIX}0"),
            generation: 0,
            policy: policy.map(Arc::new),
            admission: None,
            interrupt: None,
            inbox: None,
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
                output: Default::default(),
                provenance: Default::default(),
                cancel,
                cancel_reason,
                correlation: format!("{ROOT_CORRELATION_PREFIX}0"),
                generation: 0,
                policy: Some(Arc::clone(&policy)),
                admission: Some(admission),
                interrupt: None,
                inbox: None,
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
                output: Default::default(),
                provenance: Default::default(),
                cancel: CancelToken::none(),
                cancel_reason: ReasonedCancelToken::none(),
                correlation: format!("{ROOT_CORRELATION_PREFIX}1"),
                generation: 1,
                policy: Some(policy),
                admission: None,
                interrupt: None,
                inbox: None,
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
