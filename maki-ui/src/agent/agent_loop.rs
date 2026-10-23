//! The TUI's root-actor backend.
//!
//! Implements [`ActorBackend`](maki_agent::actor::ActorBackend) for the root
//! agent. The actor owns history, lifecycle, queueing, cancellation, and
//! retained outcomes; the shared [`TurnSetup`](maki_agent::agent::TurnSetup)
//! admits and resolves each turn's inputs. This struct owns only the TUI's
//! startup, system prompt and `/btw` prompt, tools and the answer receiver,
//! and constructs the transient [`Agent`] for each executed turn.

use std::path::PathBuf;
use std::sync::Arc;

use arc_swap::ArcSwap;
use maki_agent::actor::{
    ActorBackend, BackendResult, ControlWork, EffectiveAgentConfig, TurnContext, WorkKind,
};
use maki_agent::mcp::config::McpServerStatus;
use maki_agent::mcp::{McpHandle, McpSession};
use maki_agent::permissions::PermissionManager;
use maki_agent::template;
use maki_agent::template::Vars;
use maki_agent::tools::offload::OffloadStore;
use maki_agent::tools::{FileReadTracker, QuestionMode, RequestTools, ToolAudience, ToolRegistry};
use maki_agent::{
    ActorWorkIdentity, Agent, AgentConfig, AgentEvent, AgentId, AgentInput, AgentParams,
    AgentRunParams, CancelMap, CancelToken, Envelope, EventSender, History, Instructions,
    McpCommand, SessionMailbox, ToolOutputLines, TurnCancellationReason, TurnId, TurnOutcome,
};
use maki_config::ModelPolicy;
use maki_lua::EventHandle;
use maki_providers::{AgentError, Message, Model};
use maki_storage::id::SessionRef;
use tracing::{info, warn};

use super::ProviderSlot;
use super::SystemPromptOverride;

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
                event_tx: EventSender::new(self.agent_tx.clone()),
                tools,
            },
        )
        .with_user_response_rx(Some(Arc::clone(&self.answer_rx)))
        .with_mcp(self.mcp.clone());
        agent.tool_context_at((**self.cwd.load()).clone())
    }

    /// Build the system prompt, honoring the CLI override and append.
    fn build_system_with(
        &self,
        mode: &maki_agent::AgentMode,
        definition: &maki_agent::ModeDef,
        prompt_slots: &maki_agent::prompt::ResolvedSlots,
    ) -> String {
        let mut system = self.system_prompt.override_text.clone().unwrap_or_else(|| {
            maki_agent::agent::build_system_prompt_with_def(
                &self.vars,
                definition,
                mode,
                &self.instructions.text,
                prompt_slots,
            )
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

    /// Per-run preparation: the admitted cwd and instructions, MCP prompt
    /// expansion into the preamble, prompt slots and system prompt, tool
    /// rebuild, and answer-receiver draining.
    async fn prepare_run(
        &mut self,
        input: &mut AgentInput,
        policy: &EffectiveAgentConfig,
        admission: Option<&maki_agent::agent::TurnAdmissionSnapshot>,
    ) -> Result<(String, RequestTools, Arc<maki_agent::prompt::ResolvedSlots>), AgentError> {
        let (Some(definition), Some(prompt)) = (
            admission.and_then(|snapshot| snapshot.mode_def.as_deref()),
            admission.and_then(|snapshot| snapshot.prompt_inputs.as_ref()),
        ) else {
            return Err(AgentError::Config {
                message: maki_agent::agent::NO_ADMITTED_CWD_ERR.into(),
            });
        };
        self.vars = template::env_vars_for(&prompt.cwd);
        self.instructions = prompt.instructions.clone();
        let prompt_slots = prompt.resolve(input, self.mcp.as_ref()).await?;
        let system = self.build_system_with(&policy.mode, definition, &prompt_slots);
        self.publish_btw_system(&prompt_slots);
        self.tools = self.build_tools(&policy.model, policy.workflow);
        let tools = self.tools.clone();

        while self.answer_rx.lock().await.try_recv().is_ok() {}

        Ok((system, tools, prompt_slots))
    }

    async fn persist_history(&self, history: &History) -> Result<(), AgentError> {
        use maki_agent::session_coordinator::{SessionCoordinatorError, SessionCoordinatorHandle};

        let Some(session_id) = &self.session_id else {
            return Ok(());
        };
        match SessionCoordinatorHandle::resolve(session_id.id()) {
            Ok(coordinator) => {
                coordinator
                    .persist_history(history.snapshot().clone())
                    .await
            }
            Err(SessionCoordinatorError::StaleSession(_)) => return Ok(()),
            Err(error) => Err(error),
        }
        .map_err(|error| AgentError::Config {
            message: error.to_string(),
        })
    }

    /// A setup failure never enters a run, and the runner drops the outcome it
    /// synthesizes when the turn is a root -- which every TUI prompt is. Emit
    /// the error here or the prompt vanishes with no feedback at all.
    fn report_setup_failure(
        &self,
        work: ActorWorkIdentity,
        turn_id: TurnId,
        context: &str,
        error: &AgentError,
    ) {
        info!(error = %error, %turn_id, "{context}");
        let _ = EventSender::with_identity(self.agent_tx.clone(), Some(work)).send(
            AgentEvent::ControlError {
                message: format!("{context}: {}", error.user_message()),
            },
        );
    }

    fn cancel_setup(
        &self,
        context: &TurnContext,
        turn_id: TurnId,
        work: ActorWorkIdentity,
    ) -> TurnOutcome {
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
        context
            .output
            .set_terminal_sender(EventSender::with_identity(
                self.agent_tx.clone(),
                Some(work),
            ));
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
        work: ActorWorkIdentity,
    ) -> Option<TurnOutcome> {
        let setup = context
            .cancel
            .race(async {
                let Some(policy) = context.policy.as_deref() else {
                    let error = AgentError::Config {
                        message: "turn was admitted without an effective agent configuration"
                            .into(),
                    };
                    self.report_setup_failure(work, turn_id, "agent turn setup failed", &error);
                    return None;
                };
                let provider = Arc::clone(&policy.settings.provider);
                let model = policy.settings.model.clone();
                let prepared = match self
                    .prepare_run(&mut input, policy, context.admission.as_ref())
                    .await
                {
                    Ok(prepared) => prepared,
                    Err(error) => {
                        self.report_setup_failure(work, turn_id, "agent turn setup failed", &error);
                        return None;
                    }
                };
                Some((provider, model, prepared))
            })
            .await;
        let (provider, model, (system, tools, prompt_slots)) = match setup {
            Ok(prepared) => prepared?,
            Err(_) => return Some(self.cancel_setup(context, turn_id, work)),
        };
        let mut agent = Agent::for_actor_turn(
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
                event_tx: EventSender::with_identity(self.agent_tx.clone(), Some(work)),
                tools,
            },
            context,
            self.instructions.loaded.clone(),
        )
        .with_user_response_rx(Some(Arc::clone(&self.answer_rx)))
        .with_mcp(self.mcp.clone());

        let outcome = agent.run(turn_id, input).await;
        drop(agent);
        if matches!(outcome, TurnOutcome::Completed { .. })
            && let Err(error) = self.persist_history(history).await
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
        let build = maki_agent::AgentMode::Build;
        let system = self.build_system_with(
            &build,
            &self.lua_handle.mode_registry().current(&build),
            prompt_slots,
        );
        self.btw_system.store(Arc::new(system));
    }
}

impl ActorBackend for TuiActorBackend {
    fn offload_store(&self) -> Option<Arc<OffloadStore>> {
        self.offload.clone()
    }

    fn prepared_readiness(&self) -> Option<maki_agent::actor::PreparedReadiness> {
        Some(maki_agent::agent::TurnSetup::readiness())
    }

    fn admission_preparation(&self) -> Option<maki_agent::actor::AdmissionPreparation> {
        let lua_handle = self.lua_handle.clone();
        Some(
            maki_agent::agent::TurnSetup {
                registry: Arc::clone(ToolRegistry::global_arc()),
                local_tools: Default::default(),
                mcp: self.mcp.clone(),
                slots: self.lua_handle.prompt_slot_source(),
                modes: lua_handle.mode_registry(),
                instructions: true,
            }
            .preparation(),
        )
    }

    fn run_turn<'a>(
        &'a mut self,
        history: &'a mut History,
        context: TurnContext,
        input: AgentInput,
        work: WorkKind,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = BackendResult> + Send + 'a>> {
        Box::pin(async move {
            let context = context;
            let turn_id = context.turn_id.unwrap_or_else(TurnId::generate);
            // The actor stamped this work at admission, so every event below
            // carries the presentation it was admitted under.
            let work_identity = context.work;
            if context.cancel.is_cancelled() {
                return BackendResult::EnteredRun(self.cancel_setup(
                    &context,
                    turn_id,
                    work_identity,
                ));
            }
            context
                .output
                .set_terminal_sender(EventSender::with_identity(
                    self.agent_tx.clone(),
                    Some(work_identity),
                ));
            if let Some(policy) = &context.policy {
                EventSender::with_identity(self.agent_tx.clone(), Some(work_identity)).try_send(
                    AgentEvent::ModelSwitched {
                        spec: policy.settings.model.spec(),
                    },
                );
            }
            let result = if matches!(&work, WorkKind::Control | WorkKind::Compact) {
                warn!(?work, "unexpected work kind in TUI run_turn");
                BackendResult::SetupFailed(None)
            } else if let Err(_) | Ok(false) = context.cancel.race(self.initialize()).await {
                if context.cancel.is_cancelled() {
                    BackendResult::EnteredRun(self.cancel_setup(&context, turn_id, work_identity))
                } else {
                    BackendResult::SetupFailed(None)
                }
            } else {
                info!(
                    agent_id = %context.agent_id,
                    %turn_id,
                    presentation = work_identity.presentation_id,
                    "tui actor turn"
                );
                match self
                    .execute_agent(history, &context, input, turn_id, work_identity)
                    .await
                {
                    Some(outcome) => BackendResult::EnteredRun(outcome),
                    None => BackendResult::SetupFailed(None),
                }
            };
            if matches!(result, BackendResult::SetupFailed(_)) {
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
            let event_tx = EventSender::with_identity(self.agent_tx.clone(), Some(context.work));
            let Some(settings) = context.policy.as_deref().map(|config| &config.settings) else {
                let _ = event_tx.send(AgentEvent::ControlError {
                    message: "compaction was admitted without an effective agent configuration"
                        .into(),
                });
                return BackendResult::ControlFailed;
            };
            let (base_provider, base_model) = compaction_source(settings);

            let (provider, model) = maki_agent::agent::resolve_compaction_model(
                &base_provider,
                &base_model,
                self.timeouts,
                &self.model_policy,
            );
            let previous = history.snapshot().clone();
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
            if result.is_ok() {
                result = self.persist_history(history).await;
            }
            match result {
                Ok(()) => BackendResult::CompactDone,
                Err(e) => {
                    history.replace_snapshot(previous);
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
    use std::time::Duration;

    use futures_lite::future::poll_once;
    use maki_agent::actor::{ActorEvent, DeliveryStatus};
    use maki_agent::agent::{PROMPT_INPUTS_CLOSED, PROMPT_SLOTS_CLOSED};
    use maki_agent::{
        AgentManagerHandle, AgentMode, AgentRef, EffectiveAgentConfig, McpPromptRef,
        ReasonedCancelToken, RunSettings, TurnAdmissionOptions,
    };
    use maki_config::PermissionsConfig;
    use maki_lua::test_support::run_command;
    use maki_providers::ContentBlock;
    use test_case::test_case;

    use super::*;
    use crate::agent::ProviderSlot;

    use crate::app::tests::work_identity;

    const TEST_TIMEOUT: Duration = Duration::from_secs(10);
    const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(1);

    /// Waits for the actor's own settlement publication and asserts it names
    /// the presentation that settled last. Settlement is the actor's, never
    /// re-derived from a frontend mirror.
    async fn await_settlement(
        events: &flume::Receiver<maki_agent::actor::ActorEvent>,
        presentation: u64,
    ) -> maki_agent::ActorWorkIdentity {
        let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
        let observed = Arc::clone(&seen);
        // Earlier settlements stay published, so skip past them rather than
        // assuming the next idle belongs to the work under test.
        futures_lite::future::or(
            async {
                loop {
                    let event = events.recv_async().await.unwrap();
                    if let maki_agent::actor::ActorEvent::Idle { settled, .. } = event {
                        observed.lock().unwrap().push(settled);
                        if let Some(settled) = settled
                            && settled.presentation_id == presentation
                        {
                            break settled;
                        }
                    }
                }
            },
            async {
                smol::Timer::after(TEST_TIMEOUT).await;
                panic!(
                    "waiting for settlement of presentation {presentation}; saw {:?}",
                    seen.lock().unwrap()
                );
            },
        )
        .await
    }

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
            // The prior UI turn already displayed RUN_ID - 1, so the explicit
            // turn after it is the one that allocates its own presentation.
            actor.observe_presentation(RUN_ID - 1);
            let mut app = crate::app::tests::actor_presentation_app(root.clone(), RUN_ID - 1);
            let make_input = |text: &str| AgentInput::new(text.into(), Vec::new());
            if deferred {
                actor.observe_presentation(RUN_ID - 2);
                // The UI already drew this deferred submission, so admission
                // keeps the id the bubble is under.
                actor
                    .submit(
                        make_input("successor"),
                        None,
                        String::new(),
                        TurnAdmissionOptions {
                            already_displayed: true,
                            displayed_presentation: Some(RUN_ID - 1),
                            ..Default::default()
                        },
                    )
                    .unwrap();
                // The deferred successor is admitted with the presentation the
                // app already drew, so its stream keeps that id.
                loop {
                    let envelope = events.recv_async().await.unwrap();
                    if matches!(envelope.event, AgentEvent::TextDelta { .. }) {
                        assert_eq!(
                            envelope.presentation(),
                            Some(RUN_ID - 1),
                            "the deferred successor streams under the id the UI drew"
                        );
                        break;
                    }
                }
                let active_policy_generation = actor.config_snapshot().unwrap().generation;
                actor
                    .submit(
                        make_input("folded user input"),
                        None,
                        String::new(),
                        TurnAdmissionOptions {
                            already_displayed: false,
                            ..Default::default()
                        },
                    )
                    .unwrap();
                assert_eq!(
                    actor.config_snapshot().unwrap().generation,
                    active_policy_generation
                );
                assert!(maki_agent::batchable(&make_input("successor")));
                assert_eq!(
                    actor
                        .pending_submissions()
                        .iter()
                        .map(|entry| entry.text.as_str())
                        .collect::<Vec<_>>(),
                    ["folded user input"]
                );
                release.send(()).unwrap();
                let mut folded_bubble = false;
                loop {
                    let envelope = events.recv_async().await.unwrap();
                    assert_eq!(envelope.presentation(), Some(RUN_ID - 1));
                    folded_bubble |= matches!(envelope.event, AgentEvent::QueueItemConsumed { .. });
                    if matches!(envelope.event, AgentEvent::TextDelta { .. }) {
                        break;
                    }
                }
                assert!(folded_bubble);
                release.send(()).unwrap();
                events.drain().for_each(drop);
            }
            let ticket = actor
                .admit_turn_with_options(
                    input,
                    None,
                    String::new(),
                    TurnAdmissionOptions {
                        provenance: maki_agent::actor::TurnProvenance {
                            origin: maki_agent::actor::TurnOrigin::Plugin,
                            ..Default::default()
                        },
                        ..Default::default()
                    },
                )
                .unwrap();
            let (successor, successor_presentation) = if deferred {
                let barrier = actor.reserve_prepared_operation(None).unwrap();
                let queue = crate::agent::shared_queue::actor_queue(Arc::new(actor.clone()));
                for text in ["earlier deferred", "final deferred"] {
                    let input = make_input(text);
                    queue.push(crate::agent::shared_queue::QueueItem {
                        input,
                        displayed: None,
                    });
                }
                (None, barrier.presentation_id() + 1)
            } else {
                (
                    Some(
                        actor
                            .admit_turn(make_input("successor"), None, String::new())
                            .unwrap(),
                    ),
                    RUN_ID + 1,
                )
            };
            // The explicit turn is admitted right after the prior UI turn, so
            // the actor stamps it with RUN_ID; the successor queued behind it
            // takes the next presentation after any barrier's reservation.
            let mut explicit_presentation = None;
            let mut visible = Vec::new();
            loop {
                let envelope = futures_lite::future::or(events.recv_async(), async {
                    smol::Timer::after(TEST_TIMEOUT).await;
                    panic!("waiting for explicit streaming: actor={:?}, outcome={:?}, events={visible:?}", actor.snapshot(), ticket.peek());
                }).await.unwrap();
                // Identity names the turn, so the deferred successor sharing
                // this stream is never mistaken for the explicit turn.
                let identity = envelope.identity.expect("admitted work is stamped");
                if identity.turn_id() == Some(ticket.turn_id()) {
                    let presentation = identity.presentation_id;
                    assert_eq!(
                        *explicit_presentation.get_or_insert(presentation),
                        presentation,
                        "one turn never spans two presentations"
                    );
                    visible.push(matches!(
                        envelope.event,
                        AgentEvent::QueueItemConsumed { .. }
                    ));
                    let streamed = matches!(
                        &envelope.event,
                        AgentEvent::TextDelta { text } if text == RESPONSE
                    );
                    app.update(crate::app::Msg::Agent(Box::new(envelope)));
                    if streamed {
                        break;
                    }
                }
            }
            assert!(visible.iter().any(|consumed| *consumed));
            let explicit_presentation =
                explicit_presentation.expect("the explicit turn emitted events");
            if cancel {
                // Cancel the explicit turn under the presentation it was
                // admitted with, not the successor queued behind it.
                actor.cancel_turn(ticket.turn_id()).unwrap();
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
                if envelope.presentation() == Some(successor_presentation)
                    && let AgentEvent::QueueItemConsumed { text, .. } = &envelope.event
                {
                    successor_bubbles.push(text.clone());
                }
                terminal |= envelope.presentation() == Some(explicit_presentation)
                    && matches!(&envelope.event, AgentEvent::TurnOutcome(_));
                let streamed = envelope.presentation() == Some(successor_presentation)
                    && matches!(&envelope.event, AgentEvent::TextDelta { .. });
                app.update(crate::app::Msg::Agent(Box::new(envelope)));
                if streamed {
                    break;
                }
            }
            assert!(terminal);
            crate::app::tests::assert_actor_presentation(
                &mut app,
                successor_presentation,
                RESPONSE,
                if deferred {
                    "final deferred"
                } else {
                    "successor"
                },
            );
            if deferred {
                assert_eq!(successor_bubbles, ["earlier deferred", "final deferred"]);
            }
            assert_eq!(explicit_presentation, RUN_ID);
            assert_eq!(
                actor.presentation_id(),
                successor_presentation,
                "the actor owns the presentation counter"
            );
            if let Some(successor) = &successor {
                assert!(successor.peek().is_none());
            }
            if deferred && cancel {
                actor
                    .cancel_turn(actor.snapshot().active_turn.unwrap())
                    .unwrap();
            } else {
                release.send(()).unwrap();
            }
            let successor_outcome = match successor {
                Some(successor) => {
                    // The successor makes its own request, so it needs its own
                    // release before it can produce an outcome.
                    release.send(()).unwrap();
                    Some(
                        futures_lite::future::or(successor.wait(), async {
                            smol::Timer::after(TEST_TIMEOUT).await;
                            panic!(
                                "waiting for successor settlement: actor={:?}",
                                actor.snapshot()
                            );
                        })
                        .await,
                    )
                }
                None => None,
            };
            // The queued successor settles into its own outcome under its own
            // presentation; the shared stream only ever carries the explicit
            // turn's terminal event.
            if let Some(outcome) = successor_outcome {
                assert!(
                    matches!(
                        outcome,
                        TurnOutcome::Cancelled {
                            reason: TurnCancellationReason::User,
                            ..
                        }
                    ) == (deferred && cancel),
                    "{outcome:?}"
                );
            }
            // Every event the shared stream carried for the explicit turn was
            // stamped with its one presentation; the outcome itself arrived on
            // the ticket the actor settles.
            // The app is only idle once every admitted turn has settled, so
            // keep folding the stream until it stops streaming.
            futures_lite::future::or(
                async {
                    while app.status == crate::components::Status::Streaming {
                        let Ok(envelope) = events.recv_async().await else {
                            break;
                        };
                        app.update(crate::app::Msg::Agent(Box::new(envelope)));
                    }
                },
                async {
                    smol::Timer::after(TEST_TIMEOUT).await;
                    panic!(
                        "app never left the streaming state: actor={:?}",
                        actor.snapshot()
                    );
                },
            )
            .await;
            assert_eq!(app.status, crate::components::Status::Idle);
            if !(deferred && cancel) {
                crate::app::tests::assert_actor_response(&mut app, RESPONSE);
            }
            actor.close();
            assert!(
                manager
                    .shutdown(SHUTDOWN_TIMEOUT)
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
        const SHARED_CORRELATION: &str = "plugin-shared-correlation";

        smol::block_on(futures_lite::future::or(
            async {
                let (mut backend, _, events) = failing_backend();
                let make_input = || AgentInput::new("explicit root".into(), Vec::new());
                let (release, released) = flume::bounded(1);
                let provider = Arc::new(ExplicitProvider {
                    release: released.clone(),
                });
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
                // Seat the actor where the app believes the display is, so the
                // first admission lands on FIRST_RUN.
                actor.observe_presentation(FIRST_RUN - 1);
                let queue = crate::agent::shared_queue::actor_queue(Arc::new(actor.clone()));
                let mut app =
                    crate::app::tests::actor_presentation_app(root.clone(), FIRST_RUN - 1);
                let first = actor.admit_turn(make_input(), None, String::new()).unwrap();
                loop {
                    let envelope = events.recv_async().await.unwrap();
                    assert_eq!(envelope.presentation(), Some(FIRST_RUN));
                    let streaming = matches!(envelope.event, AgentEvent::TextDelta { .. });
                    app.update(crate::app::Msg::Agent(Box::new(envelope)));
                    if streaming {
                        break;
                    }
                }
                let (second, second_run) = if deferred {
                    let boundary = actor.reserve_prepared_operation(None).unwrap();
                    queue.push(crate::agent::shared_queue::QueueItem {
                        input: make_input(),
                        displayed: None,
                    });
                    (None, boundary.presentation_id() + 1)
                } else {
                    (
                        Some(
                            actor
                                .admit_turn(make_input(), None, SHARED_CORRELATION.into())
                                .unwrap(),
                        ),
                        FIRST_RUN + 1,
                    )
                };
                release.send(()).unwrap();
                assert!(matches!(first.wait().await, TurnOutcome::Completed { .. }));

                let mut buffered = Vec::new();
                loop {
                    let envelope = events.recv_async().await.unwrap();
                    let second_streaming = envelope.presentation() == Some(second_run)
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
                    envelope.presentation() == Some(second_run)
                        && matches!(envelope.event, AgentEvent::QueueItemConsumed { .. })
                }));
                assert_eq!(app.run_id, FIRST_RUN);
                assert_eq!(app.status, crate::components::Status::Streaming);
                assert!(second.peek().is_none());
                // A displayed bubble pushes under the id the UI already shows,
                // so admission keeps it and the counter does not move.
                let displayed_presentation = queue.current_run_id();
                queue.push(crate::agent::shared_queue::QueueItem {
                    input: make_input(),
                    displayed: Some(displayed_presentation),
                });
                assert_eq!(queue.current_run_id(), displayed_presentation);
                assert_eq!(actor.snapshot().active_turn, Some(second.turn_id()));
                assert!(
                    actor
                        .pending_submissions()
                        .iter()
                        .any(|entry| { entry.text == "explicit root" && entry.already_displayed })
                );
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
                let cancelled_turn_id = turn_id.unwrap();
                // Cancellation fences every presentation at or below the id the
                // actor held when it cut, so the app never lands behind it.
                assert!(
                    app.run_id >= displayed_presentation,
                    "cancel must fence at or past the pending presentation"
                );
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
                assert_eq!(app.status, crate::components::Status::Idle);

                let future = if deferred {
                    queue.push(crate::agent::shared_queue::QueueItem {
                        input: make_input(),
                        displayed: None,
                    });
                    None
                } else {
                    Some(
                        actor
                            .admit_turn(make_input(), None, SHARED_CORRELATION.into())
                            .unwrap(),
                    )
                };
                let future_presentation = queue.current_run_id();
                loop {
                    let envelope = events.recv_async().await.unwrap();
                    let streaming = envelope.presentation() == Some(future_presentation)
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
                assert_eq!(app.status, crate::components::Status::Streaming);
                assert!(actor.cancel_turn(cancelled_turn_id).is_err());
                assert!(future.peek().is_none());
                release.send(()).unwrap();
                assert!(matches!(future.wait().await, TurnOutcome::Completed { .. }));
                actor.close();
                assert!(
                    manager
                        .shutdown(SHUTDOWN_TIMEOUT)
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

    #[test_case(false; "root")]
    #[test_case(true; "child")]
    fn ordinary_submission_paths_have_runtime_parity(child: bool) {
        smol::block_on(futures_lite::future::or(
            async {
                use crate::agent::shared_queue::{QueueItem, actor_queue};
                const INITIAL: &str = "initial user instruction";
                const FOLLOWUPS: [&str; 2] =
                    ["first deferred instruction", "second deferred instruction"];
                const IMAGE_BYTES: usize = 256 * 1024 + 1;
                const SHELL_CONTEXT: &str = "shell preamble output";
                const PNG: &str = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+jRZkAAAAASUVORK5CYIIA";
                let image = maki_agent::ImageSource::new(
                    maki_agent::ImageMediaType::Png,
                    Arc::from(format!("{PNG}{}", "AAAA".repeat(IMAGE_BYTES / 4))),
                );
                let (mut backend, _, events) = failing_backend();
                let (requests_tx, requests) = flume::unbounded();
                let (release, released) = flume::bounded(1);
                let manager = maki_agent::AgentManagerHandle::new(Default::default()).unwrap();
                let config = maki_agent::EffectiveAgentConfig::new(
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
                .with_mode_def(maki_agent::ModeRegistry::builtin().get(&maki_agent::ModeId::Build));
                let target = if child {
                    let (parent_backend, _, _) = failing_backend();
                    let parent = manager
                        .create_root_with_config(Some(config.clone()), Vec::new(), None, |_| {
                            Ok::<Box<dyn ActorBackend>, String>(Box::new(parent_backend))
                        })
                        .unwrap();
                    manager
                        .spawn_child_trusted_with_config(
                            &parent,
                            config,
                            Default::default(),
                            Vec::new(),
                            None,
                            |agent_id| {
                                backend.agent_id = agent_id;
                                Ok::<Box<dyn ActorBackend>, String>(Box::new(backend))
                            },
                        )
                        .unwrap()
                } else {
                    manager
                        .create_root_with_config(Some(config), Vec::new(), None, |agent_id| {
                            backend.agent_id = agent_id;
                            Ok::<Box<dyn ActorBackend>, String>(Box::new(backend))
                        })
                        .unwrap()
                };
                let actor = target.actor().unwrap();
                let queue = actor_queue(Arc::new(actor.clone()));
                let settlements = actor.subscribe_events();
                let mut app = crate::app::tests::actor_presentation_app(target.clone(), 0);
                let submission = |text: &str| {
                    let mut input = AgentInput::new(text.into(), vec![image.clone()]);
                    input.preamble = vec![Message::observation(format!("{SHELL_CONTEXT}: {text}"))];
                    QueueItem {
                        input,
                        displayed: None,
                    }
                };
                queue.push(submission(INITIAL));
                let first = requests.recv_async().await.unwrap();
                let submitted_run = actor.presentation_id();
                assert!(serde_json::to_string(&first).unwrap().contains(INITIAL));
                let ticket = actor
                    .turn_ticket(actor.snapshot().active_turn.unwrap())
                    .unwrap();
                assert_eq!(
                    ticket.provenance().origin,
                    maki_agent::actor::TurnOrigin::User
                );
                for text in FOLLOWUPS {
                    queue.push(submission(text));
                }
                assert_eq!(queue.text_messages(), FOLLOWUPS);
                release.send(()).unwrap();
                let next = requests.recv_async().await.unwrap();
                let serialized = serde_json::to_string(&next).unwrap();
                let mut previous = 0;
                for text in [INITIAL, FOLLOWUPS[0], FOLLOWUPS[1]] {
                    let preamble = format!("{SHELL_CONTEXT}: {text}");
                    assert_eq!(serialized.matches(&preamble).count(), 1);
                    let preamble_index = next
                        .iter()
                        .position(|message| {
                            message
                                .first_text_content()
                                .is_some_and(|content| content.contains(&preamble))
                        })
                        .unwrap();
                    let (index, message) = next
                        .iter()
                        .enumerate()
                        .find(|(_, message)| {
                            message
                                .content
                                .iter()
                                .any(|block| matches!(block, ContentBlock::Image { .. }))
                                && message
                                    .first_text_content()
                                    .is_some_and(|content| content.contains(text))
                        })
                        .expect("image prompt missing from the follow-up request");
                    assert_eq!(index, preamble_index + 1);
                    assert!(index > previous);
                    previous = index;
                    if text != INITIAL {
                        let wrapped = message.first_text_content().unwrap();
                        assert!(wrapped.starts_with("<user-interrupt>\n"));
                        assert!(wrapped.ends_with("\n</user-interrupt>"));
                        assert_eq!(message.display_text.as_deref(), Some(text));
                    }
                }
                assert_eq!(serialized.matches(image.data.as_ref()).count(), 3);
                assert_eq!(
                    serde_json::to_string(&first)
                        .unwrap()
                        .matches(image.data.as_ref())
                        .count(),
                    1
                );
                assert_eq!(actor.snapshot().active_turn, Some(ticket.turn_id()));
                assert!(queue.is_empty());
                release.send(()).unwrap();
                assert!(matches!(ticket.wait().await, TurnOutcome::Completed { .. }));
                // The actor itself publishes the settled identity once its queue
                // is empty; the frontend never re-derives it from a mirror.
                let settled = await_settlement(&settlements, submitted_run).await;
                assert_eq!(
                    settled.agent_id,
                    actor.agent_id(),
                    "settlement names the actor that ran the work"
                );
                for envelope in events.drain() {
                    assert_eq!(envelope.presentation(), Some(submitted_run));
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
                        .shutdown(SHUTDOWN_TIMEOUT)
                        .await
                        .timed_out
                        .is_empty()
                );
            },
            async {
                smol::Timer::after(TEST_TIMEOUT).await;
                panic!("production queue folding timed out");
            },
        ));
    }

    fn provider_target(
        manager: &AgentManagerHandle,
        mut backend: TuiActorBackend,
        provider: Arc<dyn maki_providers::provider::Provider>,
        child: bool,
    ) -> AgentRef {
        let config = EffectiveAgentConfig::new(
            RunSettings {
                provider,
                model: crate::components::test_model(),
                fast: false,
                workflow: false,
                thinking: Default::default(),
            },
            AgentMode::Build,
        )
        .with_mode_def(maki_agent::ModeRegistry::builtin().get(&maki_agent::ModeId::Build));
        if child {
            let (parent_backend, _, _) = failing_backend();
            let parent = manager
                .create_root_with_config(Some(config.clone()), Vec::new(), None, |_| {
                    Ok::<Box<dyn ActorBackend>, String>(Box::new(parent_backend))
                })
                .unwrap();
            manager
                .spawn_child_trusted_with_config(
                    &parent,
                    config,
                    Default::default(),
                    Vec::new(),
                    None,
                    |id| {
                        backend.agent_id = id;
                        Ok::<Box<dyn ActorBackend>, String>(Box::new(backend))
                    },
                )
                .unwrap()
        } else {
            manager
                .create_root_with_config(Some(config), Vec::new(), None, |id| {
                    backend.agent_id = id;
                    Ok::<Box<dyn ActorBackend>, String>(Box::new(backend))
                })
                .unwrap()
        }
    }

    #[test_case(false, false; "root_provider_error")]
    #[test_case(true, false; "child_provider_error")]
    #[test_case(false, true; "root_request_budget")]
    #[test_case(true, true; "child_request_budget")]
    fn shared_instruction_failure_and_budget_outcomes(child: bool, budget: bool) {
        const INITIAL: &str = "request before steering";
        const STEERING: &str = "accepted but omitted steering";
        const FAILURE: &str = "gated provider failure";
        smol::block_on(futures_lite::future::or(
            async {
                let (mut backend, mut input, events) = failing_backend();
                if budget {
                    backend.config.max_turns = Some(1);
                }
                input.prompt = None;
                input.message = INITIAL.into();
                let (requests_tx, requests) = flume::unbounded();
                let (release, released) = flume::bounded(1);
                let manager = maki_agent::AgentManagerHandle::new(Default::default()).unwrap();
                let target = provider_target(
                    &manager,
                    backend,
                    Arc::new(FailingQueueProvider {
                        requests: requests_tx,
                        release: released,
                        fail: !budget,
                    }),
                    child,
                );
                let actor = target.actor().unwrap();
                let first = actor
                    .submit(input, None, String::new(), Default::default())
                    .unwrap();
                let request = requests.recv_async().await.unwrap();
                assert!(serde_json::to_string(&request).unwrap().contains(INITIAL));
                let joined = actor
                    .submit(
                        AgentInput::new(STEERING.into(), Vec::new()),
                        None,
                        String::new(),
                        Default::default(),
                    )
                    .unwrap();
                assert_eq!(joined.turn_id(), first.turn_id());
                assert!(!serde_json::to_string(&request).unwrap().contains(STEERING));
                release.send(()).unwrap();
                let outcome = first.wait().await;
                assert!(matches!(&outcome, TurnOutcome::Failed { failure, .. }
                if if budget { failure.diagnostic.contains("request budget") }
                   else { failure.diagnostic.contains(FAILURE) }));
                assert_eq!(joined.wait().await, outcome);
                assert!(requests.try_recv().is_err());
                let outcomes: Vec<_> = events
                    .try_iter()
                    .filter_map(|envelope| match envelope.event {
                        AgentEvent::TurnOutcome(outcome) => Some(outcome),
                        _ => None,
                    })
                    .collect();
                assert_eq!(outcomes, [outcome]);
                assert!(manager.shutdown(TEST_TIMEOUT).await.timed_out.is_empty());
            },
            async {
                smol::Timer::after(TEST_TIMEOUT).await;
                panic!("accepted steering failure did not settle");
            },
        ));

        struct FailingQueueProvider {
            requests: flume::Sender<Vec<Message>>,
            release: flume::Receiver<()>,
            fail: bool,
        }
        impl maki_providers::provider::Provider for FailingQueueProvider {
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
                    if self.fail {
                        return Err(AgentError::Config {
                            message: FAILURE.into(),
                        });
                    }
                    Ok(maki_providers::StreamResponse {
                        message: Message {
                            role: maki_providers::Role::Assistant,
                            content: vec![ContentBlock::Text {
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
    }

    #[test_case(false; "main_does_not_join")]
    #[test_case(true; "nonmain_joins")]
    fn completion_policy_main_vs_nonmain(child: bool) {
        smol::block_on(futures_lite::future::or(
            async {
                let manager = maki_agent::AgentManagerHandle::new(maki_agent::AgentLimits {
                    max_concurrent_agent_turns: 1,
                    ..Default::default()
                })
                .unwrap();
                let (backend, mut input, _events) = failing_backend();
                input.prompt = None;
                let (release, released) = flume::bounded(1);
                let (requests_tx, requests) = flume::unbounded();
                let target = provider_target(
                    &manager,
                    backend,
                    Arc::new(CompletionProvider {
                        inner: ExplicitProvider { release: released },
                        requests: requests_tx,
                    }),
                    child,
                );
                let actor = target.actor().unwrap();
                let ticket = actor
                    .submit(input, None, String::new(), Default::default())
                    .unwrap();
                requests.recv_async().await.unwrap();
                let (mut reporter_backend, mut reporter_input, _reporter_events) =
                    failing_backend();
                reporter_input.prompt = None;
                let (report_release, report_released) = flume::bounded(1);
                let (report_requests_tx, report_requests) = flume::unbounded();
                let mut config = (*actor.effective_config().unwrap()).clone();
                config.settings.provider = Arc::new(CompletionProvider {
                    inner: ExplicitProvider {
                        release: report_released,
                    },
                    requests: report_requests_tx,
                });
                let reporter = manager
                    .spawn_child_trusted_with_config(
                        &target,
                        config,
                        Default::default(),
                        Vec::new(),
                        None,
                        |id| {
                            reporter_backend.agent_id = id;
                            Ok::<Box<dyn ActorBackend>, String>(Box::new(reporter_backend))
                        },
                    )
                    .unwrap();
                let reporter_actor = reporter.actor().unwrap();
                reporter_actor.configure_parent_delivery(true).unwrap();
                let report_ticket = reporter_actor
                    .submit(reporter_input, None, String::new(), Default::default())
                    .unwrap();
                assert!(report_requests.try_recv().is_err());
                release.send(()).unwrap();
                report_requests.recv_async().await.unwrap();
                assert!(report_ticket.peek().is_none());
                if child {
                    assert!(
                        ticket.peek().is_none(),
                        "non-main settled before its reporting child"
                    );
                } else {
                    assert!(matches!(ticket.wait().await, TurnOutcome::Completed { .. }));
                }
                report_release.send(()).unwrap();
                assert!(matches!(
                    report_ticket.wait().await,
                    TurnOutcome::Completed { .. }
                ));
                let continuation = requests.recv_async().await.unwrap();
                let serialized = serde_json::to_string(&continuation).unwrap();
                assert!(serialized.contains(&report_ticket.turn_id().to_string()));
                assert!(serialized.contains(EXPLICIT_RESPONSE));
                release.send(()).unwrap();
                assert!(matches!(ticket.wait().await, TurnOutcome::Completed { .. }));
                assert!(manager.shutdown(TEST_TIMEOUT).await.timed_out.is_empty());
                assert!(requests.try_recv().is_err());
            },
            async {
                smol::Timer::after(TEST_TIMEOUT).await;
                panic!("main/non-main completion policy did not settle");
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

    #[test]
    fn ui_submission_fires_one_turn_start() {
        smol::block_on(async {
            const PROMPT: &str = "immediate UI prompt";
            const RESPONSE: &str = "visible explicit response";
            let (mut backend, _, events) = failing_backend();
            let (release, released) = flume::bounded(1);
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
            let manager = root.manager();
            let mut app = crate::app::tests::actor_presentation_app(root, 0);
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
            app.lua_event_handle.observe_agents(&manager);
            host.wait_for_worker_barrier_for_test();
            app.update(crate::app::Msg::Paste(PROMPT.into()));
            let actions = app.update(crate::app::Msg::Key(crate::components::key(
                crossterm::event::KeyCode::Enter,
            )));
            assert!(actions.is_empty());
            let submitted_run = app.run_id;
            loop {
                let envelope = futures_lite::future::or(events.recv_async(), async {
                    smol::Timer::after(TEST_TIMEOUT).await;
                    panic!("waiting for immediate response");
                })
                .await
                .unwrap();
                assert_eq!(envelope.presentation(), Some(submitted_run));
                let streamed = matches!(&envelope.event, AgentEvent::TextDelta { .. });
                app.update(crate::app::Msg::Agent(Box::new(envelope)));
                if streamed {
                    break;
                }
            }
            release.send(()).unwrap();
            // Settlement is the actor's own publication: wait for its End
            // rather than racing a non-blocking drain.
            futures_lite::future::or(
                async {
                    loop {
                        match lifecycle.recv_async().await {
                            Ok(maki_agent::actor::ActorEvent::End(_)) => break,
                            Ok(_) => {}
                            Err(_) => break,
                        }
                    }
                },
                async {
                    smol::Timer::after(TEST_TIMEOUT).await;
                    panic!("waiting for actor settlement: actor={:?}", actor.snapshot());
                },
            )
            .await;
            for envelope in events.drain() {
                assert_eq!(envelope.presentation(), Some(submitted_run));
                app.update(crate::app::Msg::Agent(Box::new(envelope)));
            }
            assert_eq!(app.run_id, submitted_run);
            host.drain_tasks_for_test();
            assert_eq!(
                run_command(&host.command_registry(), "/assert_single_start", 0, None)
                    .recv_timeout(TEST_TIMEOUT)
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
                    .shutdown(SHUTDOWN_TIMEOUT)
                    .await
                    .timed_out
                    .is_empty()
            );
        });
    }

    #[test_case(false; "setup_failure")]
    #[test_case(true; "cancelled_admission")]
    fn submission_setup_respects_admission_cancellation(cancelled: bool) {
        smol::block_on(async {
            const ADMISSION_RUN: u64 = 1;
            let (mut backend, mut input, events) = failing_backend();
            input.prompt = None;
            backend.initialized = false;
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
                work: work_identity(ADMISSION_RUN),
                generation: 0,
                policy: None,
                admission: None,
                interrupt: None,
                inbox: None,
                managed_turn: None,
            };
            let work = WorkKind::Turn;
            let mut history = History::new(Vec::new());
            let result = backend.run_turn(&mut history, context, input, work).await;
            let BackendResult::EnteredRun(outcome) = result else {
                panic!("setup must return its terminal outcome");
            };
            if cancelled {
                assert!(matches!(
                    outcome,
                    TurnOutcome::Cancelled {
                        reason: TurnCancellationReason::User,
                        ..
                    }
                ));
            } else {
                assert!(matches!(outcome, TurnOutcome::Failed { .. }));
            }
            let emitted = events.drain().collect::<Vec<_>>();
            assert!(
                !emitted
                    .iter()
                    .any(|envelope| { matches!(envelope.event, AgentEvent::TurnOutcome(_)) }),
                "backend published before actor sealing"
            );
            assert!(
                emitted
                    .iter()
                    .all(|event| event.presentation() == Some(ADMISSION_RUN)),
                "every event must carry the admitted presentation"
            );
            assert!(
                !emitted
                    .iter()
                    .any(|event| matches!(event.event, AgentEvent::QueueItemConsumed { .. }))
            );
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
            let orchestration = Arc::new(maki_lua::orchestration::AgentOrchestration::new(
                Arc::new(arc_swap::ArcSwapOption::empty()),
            ));
            let pinned = TrustedTarget {
                target: root.clone(),
                template,
            };
            orchestration.register_root(pinned.clone(), None);
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
                            local result = assert(assert(child:send(ctx, "invoke MCP", {{ when = "idle" }})):wait(ctx))
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
                    smol::Timer::after(TEST_TIMEOUT).await;
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
                smol::Timer::after(TEST_TIMEOUT).await;
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
                    .shutdown(SHUTDOWN_TIMEOUT)
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
            futures_lite::future::or(async {
                let (mut backend, mut input, events) = failing_backend();
                input.prompt = None;
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
                actor.observe_presentation(RUN_ID - 1);
                let mut app =
                    crate::app::tests::actor_presentation_app(root.clone(), RUN_ID - 1);
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
                child_actor.configure_parent_delivery(true).unwrap();
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
                    assert_eq!(envelope.presentation(), Some(RUN_ID));
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
                manager.shutdown(TEST_TIMEOUT).await;
            }, async {
                smol::Timer::after(TEST_TIMEOUT).await;
                panic!("child completion did not reach the production TUI path");
            }).await;
        });
    }

    #[test_case(false; "root")]
    #[test_case(true; "child")]
    fn shared_consumption_wrapper_and_request_order(child: bool) {
        smol::block_on(async {
            futures_lite::future::or(async {
                let (backend, mut input, _events) = failing_backend();
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
                let root = provider_target(&manager, backend, Arc::clone(&config.settings.provider), child);
                let actor = root.actor().unwrap();
                let actor_events = actor.subscribe_events();
                let parent = actor.submit(input, None, String::new(), Default::default()).unwrap();
                request_rx.recv_async().await.unwrap();
                const STEERING: &str = "steering during tool batch";
                let steering = actor.submit(AgentInput::new(STEERING.into(), Vec::new()), None, String::new(), Default::default()).unwrap();
                assert_eq!(steering.turn_id(), parent.turn_id());

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
                child_actor.configure_parent_delivery(true).unwrap();
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
                let wrapped: Vec<_> = request.iter().enumerate().filter(|(_, message)| {
                    message.display_text.as_deref() == Some(STEERING)
                }).collect();
                let [(steering_index, steering_message)] = wrapped.as_slice() else {
                    panic!("steering was not delivered exactly once");
                };
                let text = steering_message.first_text_content().unwrap();
                assert!(text.starts_with("<user-interrupt>\n"));
                assert!(text.ends_with("\n</user-interrupt>"));
                assert!(tool_result < *steering_index);
                assert!(observation > *steering_index, "observation must follow tool results and steering: {request:?}");

                assert!(matches!(parent.wait().await, TurnOutcome::Completed { .. }));
                assert!(matches!(steering.wait().await, TurnOutcome::Completed { .. }));
                manager.shutdown(TEST_TIMEOUT).await;
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
                smol::Timer::after(TEST_TIMEOUT).await;
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
        );
        let mut input = AgentInput::new("hello".into(), Vec::new());
        let admitted = stub_policy().with_context(maki_agent::actor::AgentContextSnapshot::new(
            PathBuf::from("/tmp/admitted"),
        ));
        let mut snapshot = backend.admission_preparation().unwrap()(&input, Some(&admitted));
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
        let (system, _, used_slots) =
            smol::block_on(backend.prepare_run(&mut input, &stub_policy(), Some(&snapshot)))
                .unwrap();
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
        let mut snapshot = backend.admission_preparation().unwrap()(&input, Some(&stub_policy()));
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
            let mut snapshot =
                backend.admission_preparation().unwrap()(&input, Some(&stub_policy()));
            let (slots, slots_error) = (Arc::default(), Some(PROMPT_SLOTS_CLOSED.to_owned()));
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
                .prepare_run(&mut input, &stub_policy(), Some(&ordinary))
                .await
                .unwrap();
        });
    }

    fn stub_policy() -> EffectiveAgentConfig {
        EffectiveAgentConfig::new(
            maki_agent::RunSettings {
                provider: Arc::new(StubProvider),
                model: crate::components::test_model(),
                fast: false,
                workflow: false,
                thinking: Default::default(),
            },
            AgentMode::Build,
        )
    }

    fn failing_backend() -> (TuiActorBackend, AgentInput, flume::Receiver<Envelope>) {
        let (model_slot, _change_rx) =
            ProviderSlot::new(crate::components::test_model(), Arc::new(StubProvider));
        let (agent_tx, agent_rx) = flume::unbounded();
        let (_answer_tx, answer_rx) = flume::unbounded();
        let (_init_trigger, init_cancel) = CancelToken::new();
        let agent_id = AgentId::generate();
        let mut backend = new_backend(
            agent_id,
            model_slot,
            Arc::new(ArcSwap::from_pointee(PathBuf::from("/tmp"))),
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
        );
        backend.initialized = true;

        let input = AgentInput {
            message: "hello".into(),
            images: Vec::new(),
            preamble: Vec::new(),
            prompt: Some(Box::new(McpPromptRef {
                qualified_name: "srv.missing".into(),
                arguments: HashMap::new(),
            })),
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
            work: work_identity(0),
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
            context.work,
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
            matches!(&envelope.event, AgentEvent::ControlError { message } if message.contains(maki_agent::agent::NO_ADMITTED_CWD_ERR))
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
            let (mut backend, mut input, events) = failing_backend();
            input.prompt = None;
            let mut admission =
                backend.admission_preparation().unwrap()(&input, Some(&stub_policy()));
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
                work: work_identity(0),
                generation: 0,
                policy: Some(Arc::clone(&policy)),
                admission: Some(admission),
                interrupt: None,
                inbox: None,
                managed_turn: None,
            };
            let mut successor_input = AgentInput::new(input.message.clone(), Vec::new());
            successor_input.prompt = input.prompt.clone();
            let mut history = History::new(Vec::new());
            let outcome = {
                let mut run = pin!(backend.run_turn(&mut history, context, input, WorkKind::Turn));
                assert!(poll_once(run.as_mut()).await.is_none());
                // The model announcement is stamped from admission, so it is
                // the one event a turn emits before it settles.
                assert!(matches!(
                    events.drain().next().map(|envelope| envelope.event),
                    Some(AgentEvent::ModelSwitched { .. })
                ));
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
            assert!(events.is_empty(), "backend published before actor sealing");
            assert!(pending_prompt.is_disconnected());

            let successor = TurnContext {
                agent_id: backend.agent_id,
                turn_id: Some(TurnId::generate()),
                output: Default::default(),
                provenance: Default::default(),
                cancel: CancelToken::none(),
                cancel_reason: ReasonedCancelToken::none(),
                work: work_identity(1),
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
            assert!(matches!(
                result,
                BackendResult::EnteredRun(TurnOutcome::Failed { .. })
            ));
            let emitted = events.drain().collect::<Vec<_>>();
            assert!(
                emitted
                    .iter()
                    .any(|event| { matches!(event.event, AgentEvent::ControlError { .. }) })
            );
            assert!(
                !emitted
                    .iter()
                    .any(|event| { matches!(event.event, AgentEvent::TurnOutcome(_)) }),
                "backend published before actor sealing"
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
