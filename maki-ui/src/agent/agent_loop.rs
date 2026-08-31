use std::path::PathBuf;
use std::sync::Arc;

use arc_swap::ArcSwap;
use maki_agent::agent;
use maki_agent::mcp::config::McpServerStatus;
use maki_agent::mcp::{McpHandle, McpSession};
use maki_agent::permissions::PermissionManager;
use maki_agent::template;
use maki_agent::template::Vars;
use maki_agent::tools::{
    DescriptionContext, FileReadTracker, QuestionMode, ToolAudience, ToolFilter, ToolRegistry,
};
use maki_agent::{
    Agent, AgentConfig, AgentEvent, AgentId, AgentInput, AgentParams, AgentRunParams, CancelMap,
    CancelToken, CancelTrigger, Envelope, EventSender, History, Instructions, McpCommand,
    PromptRole, SessionMailbox, SharedMessages, ToolOutputLines, TurnFailure, TurnId, TurnOutcome,
};
use maki_config::ModelPolicy;
use maki_lua::EventHandle;
use maki_providers::{AgentError, Message, Model};
use maki_storage::id::SessionRef;
use serde_json::Value;
use tracing::error;

use super::ModelSlot;
use super::SystemPromptOverride;
use super::cancel_map::RunCancelMap;
use super::shared_queue::{QueueItem, QueueReceiver};

pub(super) struct AgentLoop {
    agent_id: AgentId,
    model_slot: Arc<ArcSwap<ModelSlot>>,
    cwd: Arc<ArcSwap<PathBuf>>,
    config: AgentConfig,
    tool_output_lines: ToolOutputLines,
    vars: Vars,
    instructions: Instructions,
    tools: Value,
    mcp: Option<McpSession>,
    history: History,
    btw_system: Arc<ArcSwap<String>>,
    cancel_map: Arc<RunCancelMap>,
    init_cancel: CancelToken,
    permissions: Arc<PermissionManager>,
    file_tracker: Arc<FileReadTracker>,
    min_run_id: u64,
    agent_tx: flume::Sender<Envelope>,
    answer_rx: Arc<async_lock::Mutex<flume::Receiver<String>>>,
    queue: Arc<QueueReceiver>,
    session_id: Option<SessionRef>,
    mailbox: Option<SessionMailbox>,
    timeouts: maki_providers::Timeouts,
    lua_handle: EventHandle,
    subagent_cancels: Arc<CancelMap<String>>,
    model_policy: Arc<ModelPolicy>,
    system_prompt: SystemPromptOverride,
    file_write_locks: Arc<maki_agent::tools::FileWriteLocks>,
}

impl AgentLoop {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn new(
        model_slot: Arc<ArcSwap<ModelSlot>>,
        cwd: Arc<ArcSwap<PathBuf>>,
        config: AgentConfig,
        tool_output_lines: ToolOutputLines,
        initial_history: Vec<Message>,
        shared_history: SharedMessages,
        btw_system: Arc<ArcSwap<String>>,
        mcp_handle: Option<McpHandle>,
        permissions: Arc<PermissionManager>,
        agent_tx: flume::Sender<Envelope>,
        answer_rx: flume::Receiver<String>,
        queue: Arc<QueueReceiver>,
        cancel_map: Arc<RunCancelMap>,
        init_cancel: CancelToken,
        session_id: Option<SessionRef>,
        mailbox: Option<SessionMailbox>,
        timeouts: maki_providers::Timeouts,
        lua_handle: EventHandle,
        subagent_cancels: Arc<CancelMap<String>>,
        model_policy: Arc<ModelPolicy>,
        system_prompt: SystemPromptOverride,
        file_write_locks: Arc<maki_agent::tools::FileWriteLocks>,
    ) -> Self {
        let mcp = mcp_handle.map(|h| McpSession::new(h, &initial_history));
        Self {
            agent_id: AgentId::generate(),
            model_slot,
            cwd,
            config,
            tool_output_lines,
            vars: Vars::default(),
            instructions: Instructions::default(),
            tools: Value::Null,
            mcp,
            history: History::restored(initial_history).with_mirror(shared_history),
            btw_system,
            cancel_map,
            init_cancel,
            permissions,
            file_tracker: FileReadTracker::fresh(),
            min_run_id: 0,
            agent_tx,
            answer_rx: Arc::new(async_lock::Mutex::new(answer_rx)),
            queue,
            session_id,
            mailbox,
            timeouts,
            lua_handle,
            subagent_cancels,
            model_policy,
            system_prompt,
            file_write_locks,
        }
    }

    /// Build the system prompt, honoring the CLI override and append.
    fn build_system_with(
        &self,
        mode: &maki_agent::AgentMode,
        prompt_slots: &maki_agent::prompt::ResolvedSlots,
        model: &Model,
    ) -> String {
        let mut system = self.system_prompt.override_text.clone().unwrap_or_else(|| {
            agent::build_system_prompt(
                &self.vars,
                &self.lua_handle.mode_registry(),
                mode,
                &self.instructions.text,
                prompt_slots,
                model,
            )
        });
        if let Some(append) = &self.system_prompt.append_text {
            system.push('\n');
            system.push_str(append);
        }
        system
    }

    pub(super) async fn run(mut self) {
        if !self.initialize().await {
            return;
        }

        while let Ok(()) = self.queue.recv_notify().await {
            let mut last_run_id = None;
            while let Some(entry) = self.queue.pop() {
                if entry.run_id() < self.min_run_id {
                    continue;
                }
                last_run_id = Some(entry.run_id());
                self.process_entry(entry).await;
            }
            if let Some(run_id) = last_run_id {
                let event_tx = EventSender::new(self.agent_tx.clone(), run_id);
                self.queue
                    .publish_if_empty(|| event_tx.try_send(AgentEvent::QueueDrained));
            }
        }
    }

    async fn process_entry(&mut self, entry: QueueItem) {
        let run_id = entry.run_id();
        let event_tx = EventSender::new(self.agent_tx.clone(), run_id);
        let lease = match &self.session_id {
            Some(session_id) => {
                match maki_agent::session_coordinator::SessionCoordinatorHandle::resolve(
                    session_id.id(),
                ) {
                    Ok(coordinator) => match coordinator.acquire_lease().await {
                        Ok(lease) => Some(lease),
                        Err(error) => {
                            self.emit_error(
                                run_id,
                                AgentError::Config {
                                    message: error.to_string(),
                                },
                            );
                            return;
                        }
                    },
                    Err(
                        maki_agent::session_coordinator::SessionCoordinatorError::StaleSession(_),
                    ) => None,
                    Err(error) => {
                        self.emit_error(
                            run_id,
                            AgentError::Config {
                                message: error.to_string(),
                            },
                        );
                        return;
                    }
                }
            }
            None => None,
        };

        let result = match entry {
            QueueItem::Message {
                text,
                image_count,
                input,
                displayed,
                ..
            } => {
                if !displayed {
                    let _ = event_tx.send(AgentEvent::QueueItemConsumed { text, image_count });
                }
                let turn_id = TurnId::generate();
                if let Err(error) = self
                    .do_agent_run(input, event_tx.clone(), run_id, turn_id, lease.as_ref())
                    .await
                {
                    self.emit_turn_failure(&event_tx, turn_id, error);
                }
                return;
            }
            QueueItem::Compact { .. } => {
                let result = self.do_compact(&event_tx).await;
                match (result, lease.as_ref().and_then(|lease| lease.committer())) {
                    (Ok(()), Some(committer)) => committer
                        .commit_history(self.history.as_slice().to_vec())
                        .await
                        .map_err(|error| AgentError::Config {
                            message: error.to_string(),
                        }),
                    (result, _) => result,
                }
            }
        };

        if let Err(e) = result {
            self.emit_error(run_id, e);
        }
    }

    async fn initialize(&mut self) -> bool {
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
        !self.init_cancel.is_cancelled()
    }

    async fn do_compact(&mut self, event_tx: &EventSender) -> Result<(), AgentError> {
        let slot = self.model_slot.load();
        let (provider, model) = agent::resolve_compaction_model(
            &slot.provider,
            &slot.model,
            self.timeouts,
            &self.model_policy,
        );
        agent::compact(
            &*provider,
            &model,
            &mut self.history,
            event_tx,
            &maki_agent::cancel::CancelToken::none(),
            &self.config,
        )
        .await
    }

    async fn do_agent_run(
        &mut self,
        mut input: AgentInput,
        event_tx: EventSender,
        run_id: u64,
        turn_id: TurnId,
        lease: Option<&maki_agent::session_coordinator::SessionLease>,
    ) -> Result<(), AgentError> {
        let slot = self.model_slot.load();
        input.lease_committer = lease.and_then(|lease| lease.committer());

        let old_cwd = self.vars.apply("{cwd}").into_owned();
        self.vars = template::env_vars_for(&self.cwd.load());
        if *self.vars.apply("{cwd}") != old_cwd {
            self.reload_instructions().await;
        }
        self.rebuild_tools(&slot.model, input.workflow);

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
        let modes = self.lua_handle.mode_registry();
        let system = self.build_system_with(&input.mode, &prompt_slots, &slot.model);
        self.publish_btw_system(&prompt_slots);
        let (trigger, cancel) = CancelToken::new();
        self.set_cancel_trigger(run_id, trigger);

        while self.answer_rx.lock().await.try_recv().is_ok() {}

        let lease_committer = input.lease_committer.clone();
        let mut agent = Agent::new(
            AgentParams {
                agent_id: self.agent_id,
                provider: Arc::clone(&slot.provider),
                model: slot.model.clone(),
                config: self.config.clone(),
                tool_output_lines: self.tool_output_lines,
                permissions: Arc::clone(&self.permissions),
                session_id: self.session_id.clone(),
                mailbox: self.mailbox.clone(),
                timeouts: self.timeouts,
                file_tracker: Arc::clone(&self.file_tracker),
                prompt_slots: Arc::new(prompt_slots),
                modes: Arc::clone(&modes),
                subagent_cancels: Arc::clone(&self.subagent_cancels),
                registry: Arc::clone(maki_agent::tools::ToolRegistry::global_arc()),
                audience: ToolAudience::MAIN,
                question_mode: QuestionMode::Tui,
                model_policy: Arc::clone(&self.model_policy),
                file_write_locks: Arc::clone(&self.file_write_locks),
            },
            AgentRunParams {
                history: &mut self.history,
                system,
                event_tx,
                tools: self.tools.clone(),
            },
        )
        .with_loaded_instructions(self.instructions.loaded.clone())
        .with_user_response_rx(Arc::clone(&self.answer_rx))
        .with_interrupt_source(Arc::clone(&self.queue) as Arc<dyn maki_agent::InterruptSource>)
        .with_cancel(cancel)
        .with_mcp(self.mcp.clone());

        let outcome = agent.run(turn_id, input).await;
        drop(agent);

        self.clear_cancel_trigger(run_id);

        if matches!(outcome, TurnOutcome::Cancelled { .. }) {
            self.min_run_id = run_id + 1;
        }

        match (outcome, lease_committer) {
            (TurnOutcome::Completed { .. }, Some(committer)) => committer
                .commit_history(self.history.as_slice().to_vec())
                .await
                .map_err(|error| AgentError::Config {
                    message: error.to_string(),
                }),
            (TurnOutcome::Failed { failure, .. }, _) => Err(AgentError::Config {
                message: failure.user_message,
            }),
            (TurnOutcome::Cancelled { .. }, _) => Ok(()),
            (_, None) => Ok(()),
        }
    }

    /// Base tools only. MCP definitions are injected per request by
    /// `Agent::request_tools`; baking them here would freeze the catalog.
    fn rebuild_tools(&mut self, model: &Model, workflow: bool) {
        self.tools = self.build_tools(model, workflow);
    }

    fn build_tools(&self, model: &Model, workflow: bool) -> Value {
        let examples = model.supports_tool_examples();
        let filter = ToolFilter::from_config(&self.config, model, &[]);
        let ctx = DescriptionContext {
            filter: &filter,
            audience: ToolAudience::MAIN,
            workflow,
        };
        ToolRegistry::global().definitions(&self.vars, &ctx, examples)
    }

    async fn reload_instructions(&mut self) {
        let cwd = self.vars.apply("{cwd}").into_owned();
        self.instructions = smol::unblock(move || agent::load_instructions(&cwd)).await;
    }

    /// Always pins `Build` mode: btw runs no tools, so Plan-mode constraints would only confuse
    /// the model. Everything else matches the live prompt.
    fn publish_btw_system(&self, prompt_slots: &maki_agent::prompt::ResolvedSlots) {
        let slot = self.model_slot.load();
        let system =
            self.build_system_with(&maki_agent::AgentMode::Build, prompt_slots, &slot.model);
        self.btw_system.store(Arc::new(system));
    }

    fn set_cancel_trigger(&self, run_id: u64, trigger: CancelTrigger) {
        // One trigger per run, and `clear_cancel_trigger` drops the whole
        // key, so the slot is not worth carrying around.
        let _ = self.cancel_map.insert(run_id, trigger);
    }

    fn clear_cancel_trigger(&self, run_id: u64) {
        self.cancel_map.remove(&run_id);
    }

    fn emit_turn_failure(&self, event_tx: &EventSender, turn_id: TurnId, error: AgentError) {
        error!(error = %error, agent_id = %self.agent_id, %turn_id, "accepted turn setup failed");
        let outcome = TurnOutcome::Failed {
            agent_id: self.agent_id,
            turn_id,
            usage: Default::default(),
            num_turns: 0,
            failure: TurnFailure::from_agent_error(&error),
        };
        if let Err(send_error) = event_tx.send(AgentEvent::TurnOutcome(outcome)) {
            error!(
                %send_error,
                agent_id = %self.agent_id,
                %turn_id,
                "terminal outcome delivery failed"
            );
        }
    }

    fn emit_error(&self, run_id: u64, error: AgentError) {
        error!(error = %error, "agent error");
        let event_tx = EventSender::new(self.agent_tx.clone(), run_id);
        let _ = event_tx.send(AgentEvent::ControlError {
            message: error.user_message(),
        });
    }
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
