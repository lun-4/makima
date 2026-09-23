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
use maki_agent::ModelSource;
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
    SessionMailbox, ToolOutputLines, TurnId, TurnOutcome,
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
    ) -> String {
        let mut system = self.system_prompt.override_text.clone().unwrap_or_else(|| {
            maki_agent::agent::build_system_prompt(
                &self.vars,
                &self.lua_handle.mode_registry(),
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

    /// Per-run preparation: cwd change detection and instruction reload, MCP
    /// prompt expansion into the preamble, prompt-slot collection and system
    /// prompt construction, tool rebuild, and answer-receiver draining.
    async fn prepare_run(
        &mut self,
        input: &mut AgentInput,
    ) -> Result<(String, RequestTools, Arc<maki_agent::prompt::ResolvedSlots>), AgentError> {
        let slot = self.model_slot.load();

        let old_cwd = self.vars.apply("{cwd}").into_owned();
        self.vars = template::env_vars_for(&self.cwd.load());
        if *self.vars.apply("{cwd}") != old_cwd {
            self.reload_instructions().await;
        }
        self.tools = self.build_tools(&slot.model, input.workflow);

        if let Some(ref prompt_ref) = input.prompt {
            let Some(ref mcp) = self.mcp else {
                return Err(AgentError::Tool {
                    tool: "mcp_prompt".into(),
                    message: "MCP not available".into(),
                });
            };
            let messages = mcp
                .get_prompt(&prompt_ref.qualified_name, &prompt_ref.arguments)
                .await
                .map_err(|e| AgentError::Tool {
                    tool: "mcp_prompt".into(),
                    message: e.to_string(),
                })?;
            for pm in messages {
                let text = pm.content.text.unwrap_or_default();
                let msg = match pm.role {
                    PromptRole::Assistant => Message {
                        role: maki_providers::Role::Assistant,
                        content: vec![maki_providers::ContentBlock::Text { text }],
                        ..Default::default()
                    },
                    PromptRole::User => Message::user(text),
                };
                input.preamble.push(msg);
            }
        }

        let prompt_slots = self.lua_handle.collect_prompt_slots_async().await;
        let system = self.build_system_with(&input.mode, &prompt_slots);
        self.publish_btw_system(&prompt_slots);
        self.tools = self.build_tools(&slot.model, input.workflow);
        let tools = self.tools.clone();

        while self.answer_rx.lock().await.try_recv().is_ok() {}

        Ok((system, tools, Arc::new(prompt_slots)))
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

    /// The one place an executed turn constructs the transient [`Agent`].
    /// Returns `Some` outcome when the run entered, `None` when setup failed
    /// before `Agent::run` (the actor then synthesizes one `Failed` delivery).
    async fn execute_agent(
        &mut self,
        history: &mut History,
        context: &TurnContext,
        mut input: AgentInput,
        turn_id: TurnId,
        run_id: u64,
    ) -> Option<TurnOutcome> {
        let lease = match self.acquire_lease().await {
            Ok(lease) => lease,
            Err(error) => {
                self.report_setup_failure(run_id, turn_id, "session lease unavailable", &error);
                return None;
            }
        };
        let (system, tools, prompt_slots) = match self.prepare_run(&mut input).await {
            Ok(prepared) => prepared,
            Err(error) => {
                self.report_setup_failure(run_id, turn_id, "agent turn setup failed", &error);
                return None;
            }
        };
        self.run_id.store(run_id, Ordering::Relaxed);
        let slot = self.model_slot.load();
        let mut agent = Agent::new(
            AgentParams {
                agent_id: self.agent_id,
                provider: Arc::clone(&slot.provider) as Arc<dyn maki_providers::provider::Provider>,
                model: slot.model.clone(),
                // The session's slot for the model, the coordinator for the
                // options, so a change while this run is in flight is picked
                // up at its next request.
                settings_source: self.session_id.as_ref().map(|session| {
                    Arc::new(maki_agent::SessionRunSettings {
                        model: Arc::clone(&self.model_slot) as Arc<dyn ModelSource>,
                        session_id: session.id(),
                    }) as Arc<dyn maki_agent::RunSettingsSource>
                }),
                // The schema is built here, so a run that can be reconfigured
                // needs the means to rebuild it. `vars` is snapshotted: a cwd
                // change defers behind the lease, so it cannot move mid-run.
                tool_builder: Some({
                    let vars = self.vars.clone();
                    let config = self.config.clone();
                    let has_mcp = self.mcp.is_some();
                    Arc::new(move |model: &Model, workflow: bool| {
                        RequestTools::build(
                            ToolRegistry::global(),
                            &vars,
                            model,
                            &config,
                            &[],
                            workflow,
                            has_mcp,
                        )
                    })
                }),
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
        .with_mcp(self.mcp.clone());

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
        let system = self.build_system_with(&maki_agent::AgentMode::Build, prompt_slots);
        self.btw_system.store(Arc::new(system));
    }

    /// Run id for control-event correlation. Controls carry no turn, so the
    /// backend stamps them with the app's current run id.
    fn current_run_id(&self) -> u64 {
        self.run_id.load(Ordering::Relaxed)
    }
}

impl ActorBackend for TuiActorBackend {
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
            } else if !self.initialize().await {
                BackendResult::SetupFailed {
                    agent_id: context.agent_id,
                    turn_id,
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
                        if !item.displayed {
                            let _ = EventSender::new(self.agent_tx.clone(), item.run_id).send(
                                AgentEvent::QueueItemConsumed {
                                    text: item.text.clone(),
                                    images: item.images.clone(),
                                },
                            );
                        }
                    }
                    if !displayed {
                        let _ = EventSender::new(self.agent_tx.clone(), run_id).send(
                            AgentEvent::QueueItemConsumed {
                                text: text.clone(),
                                images: images.clone(),
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
            let slot = self.model_slot.load();
            let current_provider =
                Arc::clone(&slot.provider) as Arc<dyn maki_providers::provider::Provider>;
            let (provider, model) = maki_agent::agent::resolve_compaction_model(
                &current_provider,
                &slot.model,
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

    use maki_agent::{AgentMode, McpPromptRef, ReasonedCancelToken};
    use maki_config::PermissionsConfig;

    use super::*;
    use crate::agent::ProviderSlot;

    /// Drives one turn through a backend whose setup cannot succeed, and
    /// returns everything the run emitted.
    fn run_failing_turn() -> (Option<TurnOutcome>, Vec<Envelope>) {
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
        let context = TurnContext {
            agent_id,
            turn_id: None,
            cancel: CancelToken::none(),
            cancel_reason: ReasonedCancelToken::none(),
            correlation: format!("{ROOT_CORRELATION_PREFIX}0"),
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
    fn a_setup_failure_reaches_the_user() {
        let (outcome, envelopes) = run_failing_turn();
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
