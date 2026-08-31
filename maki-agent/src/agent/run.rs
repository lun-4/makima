use std::borrow::Cow;
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;

use serde_json::Value;
use tracing::{error, info, warn};

use maki_providers::provider::Provider;
use maki_providers::{
    ContentBlock, Message, Model, RequestOptions, Role, StopReason, StreamResponse, TokenUsage,
};

use super::compaction;
use super::history::{History, sanitize_cancelled_history};
use super::instructions::LoadedInstructions;
use super::streaming::{StreamError, stream_with_retry};
use super::tool_dispatch::{self, RecentCalls};
use crate::cancel::{CancelMap, CancelToken};
use crate::mcp::McpSession;
use crate::permissions::PermissionManager;
use crate::tools::{Deadline, FileReadTracker, LocalTools, ToolAudience, ToolContext};
use crate::{
    AgentConfig, AgentError, AgentEvent, AgentId, AgentInput, AgentMode, DoneReason, EventSender,
    ExtractedCommand, InterruptSource, SessionMailbox, TurnCancellationReason, TurnCompleteEvent,
    TurnFailure, TurnId, TurnOutcome,
};
use maki_config::{ModelPolicy, ToolOutputLines};
use maki_storage::id::SessionRef;

const MAX_REAUTH_ATTEMPTS: u32 = 2;
const NUDGE_PROMPT: &str = "You just executed tool calls but returned an empty response. Please process the tool results above and continue with the task.";
/// A model that stalls once often stalls again on the retry, so it gets
/// plenty of chances before the turn ends empty handed.
const MAX_NUDGES: u32 = 20;
/// Counted over non-padding messages.
const RECENT_TOOL_WINDOW: usize = 5;
/// Without this note a cancelled reply replays in history as a finished
/// turn, and a model resuming its own cut-off text can wedge the session
/// (seen with llama.cpp stuck on an unterminated tool call).
const CANCELLED_TEXT_NOTE: &str = "[Response cut off by user cancel]";

pub fn resolve_compaction_model(
    provider: &Arc<dyn Provider>,
    model: &Model,
    timeouts: maki_providers::Timeouts,
    model_policy: &ModelPolicy,
) -> (Arc<dyn Provider>, Model) {
    if let Some(spec) =
        maki_providers::model_registry::spec_for_tier_any(maki_providers::ModelTier::Compaction)
        && model_policy.allows(&spec)
        && let Ok(mut m) = Model::from_spec(&spec)
        && let Ok(p) = maki_providers::provider::from_model(&mut m, timeouts)
    {
        return (Arc::from(p), m);
    }
    (Arc::clone(provider), model.clone())
}

enum TurnProgress {
    Continue,
    Done(DoneReason),
}

/// Keep only the tool definitions in `all` whose `name` is in `allowed`.
/// Used for mode-scoped toolsets (`ModeDef.tools`).
fn filter_tools(all: &Value, allowed: &[String]) -> Value {
    let Some(arr) = all.as_array() else {
        return all.clone();
    };
    let wanted: HashSet<&str> = allowed.iter().map(String::as_str).collect();
    Value::Array(
        arr.iter()
            .filter(|def| {
                def.get("name")
                    .and_then(|n| n.as_str())
                    .is_some_and(|n| wanted.contains(n))
            })
            .cloned()
            .collect(),
    )
}

#[derive(Clone)]
pub struct AgentParams {
    pub agent_id: AgentId,
    pub provider: Arc<dyn Provider>,
    pub model: Model,
    pub config: AgentConfig,
    pub tool_output_lines: ToolOutputLines,
    pub permissions: Arc<PermissionManager>,
    pub session_id: Option<SessionRef>,
    pub mailbox: Option<SessionMailbox>,
    pub timeouts: maki_providers::Timeouts,
    pub file_tracker: Arc<FileReadTracker>,
    pub prompt_slots: Arc<crate::prompt::ResolvedSlots>,
    pub modes: Arc<crate::ModeRegistry>,
    pub subagent_cancels: Arc<CancelMap<String>>,
    pub registry: Arc<crate::tools::ToolRegistry>,
    pub audience: ToolAudience,
    pub question_mode: crate::tools::QuestionMode,
    pub model_policy: Arc<ModelPolicy>,
    /// Same-process per-path mutation locks, cloned from the parent context
    /// for subagents so concurrent same-path mutations stay serialized.
    pub file_write_locks: Arc<crate::tools::FileWriteLocks>,
}

pub struct AgentRunParams<'h> {
    pub history: &'h mut History,
    pub system: String,
    pub event_tx: EventSender,
    pub tools: Value,
}

pub struct Agent<'h> {
    agent_id: AgentId,
    provider: Arc<dyn Provider>,
    model: Arc<Model>,
    history: &'h mut History,
    system: String,
    event_tx: EventSender,
    tools: Value,
    mode: AgentMode,
    user_response_rx: Option<Arc<async_lock::Mutex<flume::Receiver<String>>>>,
    interrupt_source: Option<Arc<dyn InterruptSource>>,
    cancel: CancelToken,
    total_usage: TokenUsage,
    context_size: u32,
    num_turns: u32,
    recent_calls: RecentCalls,
    auto_compact: bool,
    loaded_instructions: LoadedInstructions,
    rollback_len: usize,
    mcp: Option<McpSession>,
    config: AgentConfig,
    tool_output_lines: ToolOutputLines,
    reauth_attempts: u32,
    permissions: Arc<PermissionManager>,
    opts: RequestOptions,
    session_id: Option<SessionRef>,
    mailbox: Option<SessionMailbox>,
    timeouts: maki_providers::Timeouts,
    file_tracker: Arc<FileReadTracker>,
    prompt_slots: Arc<crate::prompt::ResolvedSlots>,
    modes: Arc<crate::ModeRegistry>,
    subagent_cancels: Arc<crate::cancel::CancelMap<String>>,
    registry: Arc<crate::tools::ToolRegistry>,
    audience: ToolAudience,
    question_mode: crate::tools::QuestionMode,
    workflow: bool,
    local_tools: LocalTools,
    model_policy: Arc<ModelPolicy>,
    file_write_locks: Arc<crate::tools::FileWriteLocks>,
}

impl<'h> Agent<'h> {
    pub fn new(params: AgentParams, run: AgentRunParams<'h>) -> Self {
        Self {
            agent_id: params.agent_id,
            provider: params.provider,
            model: Arc::new(params.model),
            config: params.config,
            tool_output_lines: params.tool_output_lines,
            permissions: params.permissions,
            timeouts: params.timeouts,
            history: run.history,
            system: run.system,
            event_tx: run.event_tx,
            tools: run.tools,
            mode: AgentMode::default(),
            user_response_rx: None,
            interrupt_source: None,
            cancel: CancelToken::none(),
            total_usage: TokenUsage::default(),
            context_size: 0,
            num_turns: 0,
            recent_calls: RecentCalls::new(),
            auto_compact: compaction::auto_compact_enabled(),
            loaded_instructions: LoadedInstructions::new(),
            rollback_len: 0,
            mcp: None,
            reauth_attempts: 0,
            opts: RequestOptions::default(),
            session_id: params.session_id,
            mailbox: params.mailbox,
            file_tracker: params.file_tracker,
            prompt_slots: params.prompt_slots,
            modes: params.modes,
            subagent_cancels: params.subagent_cancels,
            registry: params.registry,
            audience: params.audience,
            question_mode: params.question_mode,
            workflow: false,
            local_tools: LocalTools::default(),
            model_policy: params.model_policy,
            file_write_locks: params.file_write_locks,
        }
    }

    pub fn with_mcp(mut self, mcp: Option<McpSession>) -> Self {
        self.mcp = mcp;
        self
    }

    pub fn with_user_response_rx(
        mut self,
        rx: Arc<async_lock::Mutex<flume::Receiver<String>>>,
    ) -> Self {
        self.user_response_rx = Some(rx);
        self
    }

    pub fn with_interrupt_source(mut self, source: Arc<dyn InterruptSource>) -> Self {
        self.interrupt_source = Some(source);
        self
    }

    pub fn with_cancel(mut self, cancel: CancelToken) -> Self {
        self.cancel = cancel;
        self
    }

    pub fn with_local_tools(mut self, local_tools: LocalTools) -> Self {
        self.local_tools = local_tools;
        self
    }

    pub fn with_loaded_instructions(mut self, loaded: LoadedInstructions) -> Self {
        self.loaded_instructions = loaded;
        self
    }

    /// Runs one accepted turn and returns its authoritative terminal outcome.
    ///
    /// Exactly one terminal event delivery is attempted. A closed event channel
    /// does not change the returned outcome and is never retried.
    pub async fn run(&mut self, turn_id: TurnId, input: AgentInput) -> TurnOutcome {
        self.total_usage = TokenUsage::default();
        self.num_turns = 0;
        self.reauth_attempts = 0;
        self.rollback_len = self.history.len();

        let AgentInput {
            message,
            mode,
            images,
            preamble,
            thinking,
            fast,
            workflow,
            prompt: _,
            lease_committer: _,
        } = input;
        self.push_input_context(preamble);
        if !message.trim().is_empty() || !images.is_empty() {
            self.history
                .push(Message::user_with_images(message.clone(), images));
        }
        self.mode = mode;
        self.workflow = workflow;
        self.opts = RequestOptions { thinking, fast };

        info!(
            agent_id = %self.agent_id,
            %turn_id,
            model = %self.model.id,
            mode = ?self.mode,
            message_len = message.len(),
            "agent run started"
        );

        let outcome = match self.run_loop().await {
            Ok(reason) => TurnOutcome::Completed {
                agent_id: self.agent_id,
                turn_id,
                usage: self.total_usage,
                num_turns: self.num_turns,
                reason,
            },
            Err(AgentError::Cancelled) => {
                sanitize_cancelled_history(self.history, self.rollback_len);
                TurnOutcome::Cancelled {
                    agent_id: self.agent_id,
                    turn_id,
                    usage: self.total_usage,
                    num_turns: self.num_turns,
                    reason: TurnCancellationReason::User,
                }
            }
            Err(error) => TurnOutcome::Failed {
                agent_id: self.agent_id,
                turn_id,
                usage: self.total_usage,
                num_turns: self.num_turns,
                failure: TurnFailure::from_agent_error(&error),
            },
        };
        self.emit_outcome(&outcome);
        outcome
    }

    fn push_input_context(&mut self, preamble: Vec<Message>) {
        for message in preamble {
            self.history.push(message);
        }
        if let Some(mailbox) = &self.mailbox {
            for message in mailbox.drain() {
                self.history.push(message);
            }
        }
    }

    async fn run_loop(&mut self) -> Result<DoneReason, AgentError> {
        loop {
            if let Some(max) = self.config.max_turns
                && self.num_turns >= max
            {
                return Ok(DoneReason::MaxTurns);
            }
            match self.turn().await? {
                TurnProgress::Continue => {}
                TurnProgress::Done(reason) => return Ok(reason),
            }
        }
    }

    /// `self.tools` holds base tools only; the MCP part is recomputed here
    /// every turn so `tool_search` loads and late-connecting servers take
    /// effect on the next request.
    fn request_tools(&self) -> Cow<'_, Value> {
        let def = self.modes.current(&self.mode);
        let base = match &def.tools {
            Some(names) => Cow::Owned(filter_tools(&self.tools, names)),
            None => Cow::Borrowed(&self.tools),
        };
        match &self.mcp {
            Some(mcp) => {
                let mut tools = base.into_owned();
                mcp.extend_tools(&mut tools);
                Cow::Owned(tools)
            }
            None => base,
        }
    }

    async fn turn(&mut self) -> Result<TurnProgress, AgentError> {
        if self.cancel.is_cancelled() {
            return Err(AgentError::Cancelled);
        }
        let tools = self.request_tools();
        let response = match stream_with_retry(
            &*self.provider,
            &self.model,
            self.history.as_slice(),
            &self.system,
            tools.as_ref(),
            &self.event_tx,
            &self.cancel,
            self.opts,
            self.session_id.as_ref(),
        )
        .await
        {
            Ok(r) => {
                self.reauth_attempts = 0;
                r
            }
            Err(StreamError::Cancelled { streamed }) => {
                let streamed = streamed.trim_end();
                if !streamed.is_empty() {
                    self.history.push(Message {
                        role: Role::Assistant,
                        content: vec![ContentBlock::Text {
                            text: format!("{streamed}\n\n{CANCELLED_TEXT_NOTE}"),
                        }],
                        ..Default::default()
                    });
                }
                return Err(AgentError::Cancelled);
            }
            Err(StreamError::Other(e)) if e.is_auth_error() => {
                return self.wait_for_reauth(e).await;
            }
            Err(StreamError::Other(e)) => {
                error!(error = %e, model = %self.model.id, self.num_turns, "stream_message failed");
                return Err(e);
            }
        };
        self.num_turns += 1;

        let has_tools = response.message.has_tool_calls();
        let stop_reason = response.stop_reason;
        info!(
            input_tokens = response.usage.input,
            output_tokens = response.usage.output,
            cache_creation = response.usage.cache_creation,
            cache_read = response.usage.cache_read,
            has_tools,
            self.num_turns,
            model = %self.model.id,
            stop_reason = stop_reason.map_or("none", Into::into),
            "API response received"
        );

        self.emit_turn_complete(&response)?;
        let usage = response.usage;
        self.total_usage += usage;
        self.context_size = usage.total_input();

        if has_tools {
            let history_len_before = self.history.len();
            self.process_tool_calls(response).await?;
            self.context_size +=
                estimate_message_tokens(&self.history.as_slice()[history_len_before..]);
        } else {
            if response.message.first_text_content().is_some() {
                self.history.push(response.message);
            } else if self.recover_stalled_turn()? {
                return Ok(TurnProgress::Continue);
            }

            if stop_reason == Some(StopReason::MaxTokens)
                && self.num_turns <= self.config.max_continuation_turns
            {
                warn!(
                    self.num_turns,
                    "response truncated (max_tokens), re-prompting"
                );
                return Ok(TurnProgress::Continue);
            }
        }

        if self.try_auto_compact().await? || self.handle_queued_command().await? {
            return Ok(TurnProgress::Continue);
        }

        if has_tools {
            Ok(TurnProgress::Continue)
        } else {
            Ok(TurnProgress::Done(stop_reason.into()))
        }
    }

    async fn wait_for_reauth(&mut self, err: AgentError) -> Result<TurnProgress, AgentError> {
        if self.reauth_attempts >= MAX_REAUTH_ATTEMPTS {
            error!(error = %err, attempts = self.reauth_attempts, "max re-auth attempts reached");
            return Err(err);
        }
        let Some(rx) = &self.user_response_rx else {
            error!(error = %err, model = %self.model.id, self.num_turns, "stream_message failed");
            return Err(err);
        };
        self.reauth_attempts += 1;
        warn!(error = %err, attempt = self.reauth_attempts, "auth error, waiting for re-authentication");
        self.event_tx.send(AgentEvent::AuthRequired)?;
        let rx = rx.lock().await;
        match futures_lite::future::race(rx.recv_async(), async {
            self.cancel.cancelled().await;
            Err(flume::RecvError::Disconnected)
        })
        .await
        {
            Ok(_) => {
                self.provider.refresh_auth().await?;
                Ok(TurnProgress::Continue)
            }
            Err(_) => Err(AgentError::Cancelled),
        }
    }

    fn emit_turn_complete(&self, response: &StreamResponse) -> Result<(), AgentError> {
        self.event_tx
            .send(AgentEvent::TurnComplete(Box::new(TurnCompleteEvent {
                message: response.message.clone(),
                usage: response.usage,
                model: self.model.id.clone(),
                cost: self
                    .model
                    .billed_cost(&response.usage, self.opts.clamped(&self.model).fast),
                context_size: Some(response.usage.context_tokens()),
                context_window: self.model.context_window,
            })))
    }

    fn emit_outcome(&self, outcome: &TurnOutcome) {
        let retryable = match outcome {
            TurnOutcome::Failed { failure, .. } => Some(failure.retryable),
            TurnOutcome::Completed { .. } | TurnOutcome::Cancelled { .. } => None,
        };
        info!(
            agent_id = %outcome.agent_id(),
            turn_id = %outcome.turn_id(),
            num_turns = outcome.num_turns(),
            total_input = outcome.usage().input,
            total_output = outcome.usage().output,
            ?retryable,
            outcome = match outcome {
                TurnOutcome::Completed { .. } => "completed",
                TurnOutcome::Failed { .. } => "failed",
                TurnOutcome::Cancelled { .. } => "cancelled",
            },
            "agent run terminalized"
        );
        if let Err(error) = self.event_tx.send(AgentEvent::TurnOutcome(outcome.clone())) {
            error!(
                agent_id = %outcome.agent_id(),
                turn_id = %outcome.turn_id(),
                %error,
                "terminal outcome delivery failed"
            );
        }
    }

    /// The turn came back without text, so [`Message::empty_marker`] takes its
    /// place in history. Returns true when the model was nudged to try again.
    fn recover_stalled_turn(&mut self) -> Result<bool, AgentError> {
        let nudges = self.history.recent_nudges();
        let nudge = nudges < MAX_NUDGES && self.history.has_recent_tool_results(RECENT_TOOL_WINDOW);
        self.history.push(Message::empty_marker());
        if !nudge {
            return Ok(false);
        }

        warn!(
            nudges = nudges + 1,
            "empty response after tool calls, nudging model to continue"
        );
        self.event_tx.send(AgentEvent::Nudge)?;
        self.history.push(Message::synthetic(NUDGE_PROMPT.into()));
        Ok(true)
    }

    async fn process_tool_calls(&mut self, response: StreamResponse) -> Result<(), AgentError> {
        let ctx = self.tool_context();
        tool_dispatch::process_tool_calls(
            response,
            &mut self.recent_calls,
            self.mcp.as_ref(),
            self.history,
            &self.event_tx,
            &ctx,
        )
        .await
    }

    fn tool_context(&self) -> ToolContext {
        let cwd = self
            .session_id
            .as_ref()
            .and_then(|session| {
                crate::session_coordinator::SessionCoordinatorHandle::resolve(session.id()).ok()
            })
            .map(|coordinator| coordinator.read().cwd())
            .or_else(|| std::env::current_dir().ok())
            .unwrap_or_else(|| PathBuf::from("."));
        ToolContext {
            provider: Arc::clone(&self.provider),
            model: Arc::clone(&self.model),
            event_tx: self.event_tx.clone(),
            mode: self.mode.clone(),
            question_mode: self.question_mode,
            session_id: self.session_id.clone(),
            cwd,
            tool_use_id: None,
            user_response_rx: self.user_response_rx.clone(),
            loaded_instructions: self.loaded_instructions.clone(),
            cancel: self.cancel.clone(),
            mcp: self.mcp.clone(),
            deadline: Deadline::None,
            config: self.config.clone(),
            tool_output_lines: self.tool_output_lines,
            permissions: Arc::clone(&self.permissions),
            timeouts: self.timeouts,
            file_tracker: Arc::clone(&self.file_tracker),
            prompt_slots: Arc::clone(&self.prompt_slots),
            modes: Arc::clone(&self.modes),
            opts: self.opts,
            subagent_cancels: Arc::clone(&self.subagent_cancels),
            registry: Arc::clone(&self.registry),
            workflow: self.workflow,
            audience: self.audience,
            local_tools: Arc::clone(&self.local_tools),
            live_sink: None,
            model_policy: Arc::clone(&self.model_policy),
            file_write_locks: Arc::clone(&self.file_write_locks),
            write_lock_chain: Arc::new(Vec::new()),
        }
    }

    async fn try_auto_compact(&mut self) -> Result<bool, AgentError> {
        if !self.auto_compact
            || !compaction::is_overflow(
                &TokenUsage {
                    input: self.context_size,
                    ..Default::default()
                },
                &self.model,
                self.config.compaction_buffer,
            )
        {
            return Ok(false);
        }
        info!(context_size = self.context_size, "auto-compacting");
        self.event_tx.send(AgentEvent::AutoCompacting)?;
        self.do_compact().await?;
        Ok(true)
    }

    async fn do_compact(&mut self) -> Result<(), AgentError> {
        let (compact_provider, compact_model) = resolve_compaction_model(
            &self.provider,
            &self.model,
            self.timeouts,
            &self.model_policy,
        );
        self.total_usage += compaction::compact_history(
            &*compact_provider,
            &compact_model,
            self.history,
            &self.event_tx,
            &self.cancel,
            &self.config,
        )
        .await?;
        self.rollback_len = self.history.len();
        self.event_tx.send(AgentEvent::CompactionDone)?;
        self.history
            .push(Message::synthetic(compaction::continue_message(
                &self.config,
            )));
        Ok(())
    }

    async fn handle_queued_command(&mut self) -> Result<bool, AgentError> {
        let Some(ref source) = self.interrupt_source else {
            return Ok(false);
        };
        let Some(cmd) = source.poll() else {
            return Ok(false);
        };
        match cmd {
            ExtractedCommand::Interrupt(mut input, _) => {
                self.event_tx.send(AgentEvent::QueueItemConsumed {
                    text: input.message.clone(),
                    image_count: input.images.len(),
                })?;
                self.push_input_context(std::mem::take(&mut input.preamble));
                self.mode = input.mode.clone();
                let display = input.message.clone();
                let wrapped = format!(
                    "<user-interrupt>\nThe user sent a new message while you were working. Address it and continue.\n\n{display}\n</user-interrupt>"
                );
                self.history.push(Message::user_display(wrapped, display));
            }
            ExtractedCommand::Compact(_) => {
                self.do_compact().await?;
            }
        }
        Ok(true)
    }
}

const CHARS_PER_TOKEN: usize = 4;

/// Counts message content only. The system prompt and the tool schemas, a five
/// figure baseline on a full tool set, stay invisible here, so never let this
/// replace a context size the provider measured.
pub fn estimate_message_tokens(messages: &[Message]) -> u32 {
    if messages.is_empty() {
        return 0;
    }
    let total_bytes: usize = messages
        .iter()
        .flat_map(|m| &m.content)
        .filter_map(|b| match b {
            ContentBlock::Text { text } => Some(text.len()),
            ContentBlock::ToolResult { content, .. } => Some(content.len()),
            ContentBlock::ToolUse { input, .. } => Some(input.to_string().len()),
            ContentBlock::Thinking { thinking, .. } => Some(thinking.len()),
            _ => None,
        })
        .sum();
    (total_bytes.max(CHARS_PER_TOKEN) / CHARS_PER_TOKEN) as u32
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::{Arc, Mutex};

    use maki_providers::provider::{BoxFuture, Provider};
    use maki_providers::{
        ContentBlock, Message, Model, ProviderEvent, RequestOptions, Role, StopReason,
        StreamResponse, TokenUsage,
    };
    use serde_json::Value;
    use test_case::test_case;

    use super::*;
    use crate::Envelope;
    use crate::mcp::tool_names;
    use crate::permissions::PermissionManager;

    struct MockInterruptSource {
        commands: Mutex<VecDeque<ExtractedCommand>>,
    }

    impl MockInterruptSource {
        fn new(commands: Vec<ExtractedCommand>) -> Arc<Self> {
            Arc::new(Self {
                commands: Mutex::new(commands.into()),
            })
        }
    }

    impl InterruptSource for MockInterruptSource {
        fn poll(&self) -> Option<ExtractedCommand> {
            self.commands.lock().unwrap().pop_front()
        }
    }

    struct MockProvider {
        responses: Mutex<Vec<StreamResponse>>,
        captured_tools: Arc<Mutex<Vec<Value>>>,
    }

    impl MockProvider {
        fn new(responses: Vec<StreamResponse>) -> Self {
            Self {
                responses: Mutex::new(responses),
                captured_tools: Arc::default(),
            }
        }
    }

    impl Provider for MockProvider {
        fn stream_message<'a>(
            &'a self,
            _: &'a Model,
            _: &'a [Message],
            _: &'a str,
            tools: &'a Value,
            _: &'a flume::Sender<ProviderEvent>,
            _: RequestOptions,
            _: Option<&'a SessionRef>,
        ) -> BoxFuture<'a, Result<StreamResponse, AgentError>> {
            Box::pin(async {
                self.captured_tools.lock().unwrap().push(tools.clone());
                let mut responses = self.responses.lock().unwrap();
                assert!(!responses.is_empty(), "MockProvider: no more responses");
                Ok(responses.remove(0))
            })
        }

        fn list_models(&self) -> BoxFuture<'_, Result<Vec<maki_providers::ModelInfo>, AgentError>> {
            Box::pin(async { unimplemented!() })
        }
    }

    struct ScriptedProvider {
        results: Mutex<VecDeque<Result<StreamResponse, AgentError>>>,
    }

    impl Provider for ScriptedProvider {
        fn stream_message<'a>(
            &'a self,
            _: &'a Model,
            _: &'a [Message],
            _: &'a str,
            _: &'a Value,
            _: &'a flume::Sender<ProviderEvent>,
            _: RequestOptions,
            _: Option<&'a SessionRef>,
        ) -> BoxFuture<'a, Result<StreamResponse, AgentError>> {
            Box::pin(async {
                self.results
                    .lock()
                    .unwrap()
                    .pop_front()
                    .expect("ScriptedProvider: no more results")
            })
        }

        fn list_models(&self) -> BoxFuture<'_, Result<Vec<maki_providers::ModelInfo>, AgentError>> {
            Box::pin(async { unimplemented!() })
        }
    }

    /// Streams `delta` (if any), fires `cancel_after_delta` (if any),
    /// then fails with `fail_status` or hangs until cancelled.
    #[derive(Default)]
    struct StubStreamProvider {
        delta: Option<&'static str>,
        cancel_after_delta: Mutex<Option<crate::cancel::CancelTrigger>>,
        fail_status: Option<u16>,
    }

    impl Provider for StubStreamProvider {
        fn stream_message<'a>(
            &'a self,
            _: &'a Model,
            _: &'a [Message],
            _: &'a str,
            _: &'a Value,
            ptx: &'a flume::Sender<ProviderEvent>,
            _: RequestOptions,
            _: Option<&'a SessionRef>,
        ) -> BoxFuture<'a, Result<StreamResponse, AgentError>> {
            Box::pin(async move {
                if let Some(text) = self.delta {
                    ptx.send(ProviderEvent::TextDelta { text: text.into() })
                        .unwrap();
                }
                if let Some(trigger) = self.cancel_after_delta.lock().unwrap().take() {
                    trigger.cancel();
                }
                match self.fail_status {
                    Some(status) => Err(AgentError::Api {
                        status,
                        message: "stub".into(),
                    }),
                    None => futures_lite::future::pending().await,
                }
            })
        }

        fn list_models(&self) -> BoxFuture<'_, Result<Vec<maki_providers::ModelInfo>, AgentError>> {
            Box::pin(async { unimplemented!() })
        }
    }

    fn default_model() -> Model {
        Model::from_spec("anthropic/claude-sonnet-4-20250514").unwrap()
    }

    fn text_response(stop_reason: StopReason) -> StreamResponse {
        StreamResponse {
            message: Message {
                role: Role::Assistant,
                content: vec![ContentBlock::Text {
                    text: "response".into(),
                }],
                ..Default::default()
            },
            usage: TokenUsage::default(),
            stop_reason: Some(stop_reason),
        }
    }

    fn empty_response() -> StreamResponse {
        assistant_response(vec![])
    }

    fn thinking_response() -> StreamResponse {
        assistant_response(vec![ContentBlock::Thinking {
            thinking: "stalled".into(),
            signature: None,
        }])
    }

    fn assistant_response(content: Vec<ContentBlock>) -> StreamResponse {
        StreamResponse {
            message: Message {
                role: Role::Assistant,
                content,
                ..Default::default()
            },
            usage: TokenUsage::default(),
            stop_reason: Some(StopReason::EndTurn),
        }
    }

    fn make_agent(
        provider: impl Provider + 'static,
        history: &mut History,
    ) -> (Agent<'_>, flume::Receiver<Envelope>) {
        let (raw_tx, event_rx) = flume::unbounded();
        make_agent_with_sender(provider, history, raw_tx, event_rx)
    }

    fn make_agent_with_sender(
        provider: impl Provider + 'static,
        history: &mut History,
        raw_tx: flume::Sender<Envelope>,
        event_rx: flume::Receiver<Envelope>,
    ) -> (Agent<'_>, flume::Receiver<Envelope>) {
        let agent = Agent::new(
            AgentParams {
                agent_id: AgentId::generate(),
                provider: Arc::new(provider),
                model: default_model(),
                config: AgentConfig::default(),
                tool_output_lines: ToolOutputLines::default(),
                permissions: Arc::new(PermissionManager::new(
                    maki_config::PermissionsConfig {
                        default: maki_config::DefaultEffect::Allow,
                        rules: vec![],
                        ..Default::default()
                    },
                    std::path::PathBuf::from("/tmp"),
                    Arc::default(),
                )),
                session_id: None,
                mailbox: None,
                timeouts: maki_providers::Timeouts::default(),
                file_tracker: FileReadTracker::fresh(),
                prompt_slots: Arc::new(crate::prompt::ResolvedSlots::default()),
                modes: crate::ModeRegistry::builtin().into(),
                subagent_cancels: Arc::new(crate::cancel::CancelMap::new()),
                registry: Arc::new(crate::tools::ToolRegistry::new()),
                audience: ToolAudience::MAIN,
                question_mode: crate::tools::QuestionMode::Tui,
                model_policy: Arc::new(ModelPolicy::default()),
                file_write_locks: Arc::new(crate::tools::FileWriteLocks::new()),
            },
            AgentRunParams {
                history,
                system: "system".into(),
                event_tx: EventSender::new(raw_tx, 0),
                tools: serde_json::json!([]),
            },
        );
        (agent, event_rx)
    }

    fn default_input() -> AgentInput {
        AgentInput {
            message: "hello".into(),
            mode: AgentMode::Build,
            images: Vec::new(),
            preamble: Vec::new(),
            thinking: Default::default(),
            fast: false,
            workflow: false,
            prompt: None,
            lease_committer: None,
        }
    }

    #[test]
    fn run_ingests_preamble_then_mailbox_then_user_message() {
        smol::block_on(async {
            let id = maki_storage::id::MakiId::generate();
            let mailbox = SessionMailbox::new(id);
            mailbox.push("mailbox".into(), false);
            let mut history = History::new(Vec::new());
            let (mut agent, _event_rx) = make_agent(
                MockProvider::new(vec![text_response(StopReason::EndTurn)]),
                &mut history,
            );
            agent.mailbox = Some(mailbox);
            let mut input = default_input();
            input.preamble = vec![Message::observation("preamble".into())];

            agent.run(TurnId::generate(), input).await;
            drop(agent);

            assert_eq!(history.as_slice()[0].user_text(), Some("preamble"));
            assert_eq!(history.as_slice()[1].user_text(), Some("mailbox"));
            assert_eq!(history.as_slice()[2].user_text(), Some("hello"));
        });
    }

    #[test]
    fn queued_input_drains_preamble_and_mailbox() {
        smol::block_on(async {
            let id = maki_storage::id::MakiId::generate();
            let mailbox = SessionMailbox::new(id);
            mailbox.push("mailbox".into(), false);
            let mut input = default_input();
            input.preamble = vec![Message::observation("preamble".into())];
            let source = MockInterruptSource::new(vec![ExtractedCommand::Interrupt(input, 0)]);
            let mut history = History::new(Vec::new());
            let (mut agent, _event_rx) = make_agent(MockProvider::new(Vec::new()), &mut history);
            agent.mailbox = Some(mailbox);
            let mut agent = agent.with_interrupt_source(source);

            assert!(agent.handle_queued_command().await.unwrap());
            drop(agent);

            let text = history
                .as_slice()
                .iter()
                .map(Message::user_text)
                .collect::<Vec<_>>();
            assert_eq!(text, [Some("preamble"), Some("mailbox"), Some("hello")]);
            assert!(history.as_slice()[0].is_observation());
            assert!(history.as_slice()[1].is_observation());
        });
    }

    #[test]
    fn wake_only_run_does_not_insert_an_empty_user_turn() {
        smol::block_on(async {
            let id = maki_storage::id::MakiId::generate();
            let mailbox = SessionMailbox::new(id);
            mailbox.push("failed".into(), true);
            let mut history = History::new(Vec::new());
            let (mut agent, _event_rx) = make_agent(
                MockProvider::new(vec![text_response(StopReason::EndTurn)]),
                &mut history,
            );
            agent.mailbox = Some(mailbox);
            let mut input = default_input();
            input.message.clear();

            agent.run(TurnId::generate(), input).await;
            drop(agent);

            assert_eq!(history.as_slice().len(), 2);
            assert!(history.as_slice()[0].is_observation());
            assert!(matches!(history.as_slice()[1].role, Role::Assistant));
        });
    }

    fn drain_events(rx: &flume::Receiver<Envelope>) -> Vec<Envelope> {
        let mut events = Vec::new();
        while let Ok(e) = rx.try_recv() {
            events.push(e);
        }
        events
    }

    async fn run_agent(provider: MockProvider, max_turns: Option<u32>) -> (u32, DoneReason) {
        let mut history = History::new(Vec::new());
        let (mut agent, _event_rx) = make_agent(provider, &mut history);
        agent.config.max_turns = max_turns;
        let outcome = agent.run(TurnId::generate(), default_input()).await;
        match outcome {
            TurnOutcome::Completed {
                num_turns, reason, ..
            } => (num_turns, reason),
            other => panic!("expected completed outcome, got {other:?}"),
        }
    }

    fn has_event(events: &[Envelope], predicate: impl Fn(&AgentEvent) -> bool) -> bool {
        events.iter().any(|e| predicate(&e.event))
    }

    fn terminal_outcomes(events: &[Envelope]) -> Vec<&TurnOutcome> {
        events
            .iter()
            .filter_map(|envelope| match &envelope.event {
                AgentEvent::TurnOutcome(outcome) => Some(outcome),
                _ => None,
            })
            .collect()
    }

    #[test]
    fn run_emits_same_completed_outcome_once() {
        smol::block_on(async {
            let mut history = History::new(Vec::new());
            let (mut agent, event_rx) = make_agent(
                MockProvider::new(vec![text_response(StopReason::EndTurn)]),
                &mut history,
            );
            let turn_id = TurnId::generate();
            let outcome = agent.run(turn_id, default_input()).await;
            let events = drain_events(&event_rx);
            let terminal = terminal_outcomes(&events);
            assert_eq!(terminal, [&outcome]);
            assert_eq!(outcome.turn_id(), turn_id);
        });
    }

    #[test]
    fn run_emits_same_failed_outcome_once() {
        smol::block_on(async {
            let mut history = History::new(Vec::new());
            let provider = StubStreamProvider {
                fail_status: Some(400),
                ..Default::default()
            };
            let (mut agent, event_rx) = make_agent(provider, &mut history);
            let outcome = agent.run(TurnId::generate(), default_input()).await;
            assert!(matches!(outcome, TurnOutcome::Failed { .. }));
            let events = drain_events(&event_rx);
            assert_eq!(terminal_outcomes(&events), [&outcome]);
        });
    }

    #[test]
    fn reused_agent_fails_then_succeeds() {
        smol::block_on(async {
            let provider = ScriptedProvider {
                results: Mutex::new(VecDeque::from([
                    Err(AgentError::Api {
                        status: 400,
                        message: "bad request".into(),
                    }),
                    Ok(text_response(StopReason::EndTurn)),
                ])),
            };
            let mut history = History::new(Vec::new());
            let (mut agent, event_rx) = make_agent(provider, &mut history);

            let failed = agent.run(TurnId::generate(), default_input()).await;
            let completed = agent.run(TurnId::generate(), default_input()).await;

            assert!(matches!(failed, TurnOutcome::Failed { num_turns: 0, .. }));
            assert!(matches!(
                completed,
                TurnOutcome::Completed { num_turns: 1, .. }
            ));
            let events = drain_events(&event_rx);
            assert_eq!(terminal_outcomes(&events), [&failed, &completed]);
        });
    }

    #[test]
    fn reused_agent_reports_per_turn_usage_and_turn_count() {
        smol::block_on(async {
            let mut first = text_response(StopReason::EndTurn);
            first.usage.input = 11;
            let mut second = text_response(StopReason::EndTurn);
            second.usage.input = 22;
            let mut history = History::new(Vec::new());
            let (mut agent, _event_rx) =
                make_agent(MockProvider::new(vec![first, second]), &mut history);

            let first = agent.run(TurnId::generate(), default_input()).await;
            let second = agent.run(TurnId::generate(), default_input()).await;

            assert_eq!(first.usage().input, 11);
            assert_eq!(second.usage().input, 22);
            assert_eq!(first.num_turns(), 1);
            assert_eq!(second.num_turns(), 1);
        });
    }

    #[test]
    fn terminal_delivery_failure_does_not_change_outcome() {
        smol::block_on(async {
            let mut history = History::new(Vec::new());
            let (raw_tx, event_rx) = flume::bounded(1);
            let (mut agent, event_rx) = make_agent_with_sender(
                MockProvider::new(vec![text_response(StopReason::EndTurn)]),
                &mut history,
                raw_tx,
                event_rx,
            );

            let turn_id = TurnId::generate();
            let outcome = agent.run(turn_id, default_input()).await;

            assert!(matches!(outcome, TurnOutcome::Completed { .. }));
            assert_eq!(outcome.turn_id(), turn_id);
            assert_eq!(outcome.num_turns(), 1);
            assert_eq!(
                event_rx
                    .drain()
                    .filter(|envelope| matches!(envelope.event, AgentEvent::TurnOutcome(_)))
                    .count(),
                0,
                "terminal delivery must not be retried after the full channel rejects it"
            );
        });
    }

    fn has_interrupt_in_history(history: &[Message]) -> bool {
        history.iter().any(|m| {
            m.content.iter().any(
                |b| matches!(b, ContentBlock::Text { text } if text.contains("<user-interrupt>")),
            )
        })
    }

    fn tool_call_response(tool_name: &str, tool_id: &str) -> StreamResponse {
        StreamResponse {
            message: Message {
                role: Role::Assistant,
                content: vec![ContentBlock::tool_use(
                    tool_id,
                    tool_name,
                    serde_json::json!({"pattern": "*.nonexistent_test_xyz", "path": "/tmp"}),
                )],
                ..Default::default()
            },
            usage: TokenUsage::default(),
            stop_reason: Some(StopReason::ToolUse),
        }
    }

    fn tool_use_response(tool_name: &str, input: Value) -> StreamResponse {
        StreamResponse {
            message: Message {
                role: Role::Assistant,
                content: vec![ContentBlock::tool_use("t1", tool_name, input)],
                ..Default::default()
            },
            usage: TokenUsage::default(),
            stop_reason: Some(StopReason::ToolUse),
        }
    }

    #[test]
    fn mcp_definitions_refresh_per_request() {
        smol::block_on(async {
            let provider = MockProvider::new(vec![
                tool_use_response(
                    crate::mcp::TOOL_SEARCH_TOOL_NAME,
                    serde_json::json!({"query": "fetch issue"}),
                ),
                text_response(StopReason::EndTurn),
            ]);
            let captured = Arc::clone(&provider.captured_tools);
            let mut history = History::new(Vec::new());
            let (agent, _event_rx) = make_agent(provider, &mut history);
            let mut agent = agent.with_mcp(Some(crate::mcp::stub_session(&[(
                "srv.fetch_issue",
                "Fetch a GitHub issue",
            )])));
            agent.run(TurnId::generate(), default_input()).await;

            let captured = captured.lock().unwrap();
            assert_eq!(captured.len(), 2);
            let first = tool_names(&captured[0]);
            assert!(first.contains(&crate::mcp::TOOL_SEARCH_TOOL_NAME));
            assert!(!first.contains(&"srv__fetch_issue"));
            assert!(tool_names(&captured[1]).contains(&"srv__fetch_issue"));
        });
    }

    fn small_context_model(context_window: u32, max_output_tokens: u32) -> Model {
        let mut model = default_model();
        model.context_window = context_window;
        model.max_output_tokens = Some(max_output_tokens);
        model
    }

    #[track_caller]
    fn assert_ends_with_cancel_marker(history: &History) {
        let last = history.as_slice().last().unwrap();
        assert!(matches!(last.role, Role::User));
        assert!(
            matches!(&last.content[0], ContentBlock::Text { text } if text == "[Cancelled by user]")
        );
    }

    /// A truncated answer buys another turn, but only until one of the two
    /// budgets runs out: the continuation limit or the caller's `max_turns`.
    #[test_case(&[StopReason::EndTurn], None, 1, DoneReason::EndTurn ; "end_turn_completes")]
    #[test_case(&[StopReason::MaxTokens, StopReason::EndTurn], None, 2, DoneReason::EndTurn ; "max_tokens_continues")]
    #[test_case(&[StopReason::MaxTokens; 4], None, 4, DoneReason::MaxTokens ; "max_tokens_gives_up_after_limit")]
    #[test_case(&[StopReason::MaxTokens, StopReason::EndTurn], Some(1), 1, DoneReason::MaxTurns ; "turn_budget_exhausted")]
    fn turn_counting(
        stops: &[StopReason],
        max_turns: Option<u32>,
        expected_turns: u32,
        expected_reason: DoneReason,
    ) {
        smol::block_on(async {
            let responses: Vec<_> = stops.iter().map(|s| text_response(*s)).collect();
            let provider = MockProvider::new(responses);
            let (turns, reason) = run_agent(provider, max_turns).await;
            assert_eq!(turns, expected_turns);
            assert_eq!(reason, expected_reason);
        });
    }

    #[test_case(Some(true),  true,  true  ; "after_tool_use_turn")]
    #[test_case(Some(false), true,  true  ; "after_text_only_turn")]
    #[test_case(None,        false, false ; "channel_empty")]
    fn interrupt_handling(queued: Option<bool>, expect_consumed: bool, expect_injected: bool) {
        smol::block_on(async {
            let source = if queued.is_some() {
                Some(MockInterruptSource::new(vec![ExtractedCommand::Interrupt(
                    default_input(),
                    0,
                )]))
            } else {
                None
            };

            let tool_use = queued.unwrap_or(true);
            let responses = if tool_use {
                vec![
                    tool_call_response("glob", "t1"),
                    text_response(StopReason::EndTurn),
                ]
            } else {
                vec![
                    text_response(StopReason::EndTurn),
                    text_response(StopReason::EndTurn),
                ]
            };

            let mut history = History::new(Vec::new());
            let (mut agent, event_rx) = make_agent(MockProvider::new(responses), &mut history);
            if let Some(s) = source {
                agent = agent.with_interrupt_source(s);
            }
            let _ = agent.run(TurnId::generate(), default_input()).await;
            let events = drain_events(&event_rx);

            assert_eq!(
                has_event(&events, |e| matches!(
                    e,
                    AgentEvent::QueueItemConsumed { .. }
                )),
                expect_consumed,
            );
            assert_eq!(
                has_interrupt_in_history(history.as_slice()),
                expect_injected
            );
        });
    }

    #[test_case(
        (0..10).map(|i| Message::user(format!("msg {i}"))).collect(),
        vec![ExtractedCommand::Compact(0)],
        vec![tool_call_response("glob", "t1"), text_response(StopReason::EndTurn), text_response(StopReason::EndTurn)]
        ; "compaction_via_interrupt_source"
    )]
    fn compaction_through_interrupt(
        prior: Vec<Message>,
        commands: Vec<ExtractedCommand>,
        responses: Vec<StreamResponse>,
    ) {
        smol::block_on(async {
            let source = MockInterruptSource::new(commands);

            let mut history = History::new(prior);
            let (agent, _event_rx) = make_agent(MockProvider::new(responses), &mut history);
            let result = agent
                .with_interrupt_source(source)
                .run(TurnId::generate(), default_input())
                .await;

            assert!(matches!(result, TurnOutcome::Completed { .. }));
        });
    }

    #[test_case(true,  170_000, true  ; "enabled_and_over_threshold")]
    #[test_case(true,  150_000, false ; "enabled_but_below_threshold")]
    #[test_case(false, 170_000, false ; "disabled_even_over_threshold")]
    fn try_auto_compact_behavior(enabled: bool, context_size: u32, expected: bool) {
        smol::block_on(async {
            let responses = if expected {
                vec![text_response(StopReason::EndTurn)]
            } else {
                vec![]
            };
            let mut history = History::new(vec![Message::user("go".into())]);
            let (mut agent, event_rx) = make_agent(MockProvider::new(responses), &mut history);
            agent.model = Arc::new(small_context_model(200_000, 8_192));
            agent.auto_compact = enabled;
            agent.context_size = context_size;
            let result = agent.try_auto_compact().await.unwrap();

            assert_eq!(result, expected);
            drop(agent);
            assert_eq!(
                has_event(&drain_events(&event_rx), |e| matches!(
                    e,
                    AgentEvent::AutoCompacting
                )),
                expected,
            );
        });
    }

    #[test]
    fn do_compact_appends_post_instructions_to_continue_message() {
        smol::block_on(async {
            const POST: &str = "Re-read plan.md";
            let mut history = History::new(vec![Message::user("go".into())]);
            let (mut agent, _event_rx) = make_agent(
                MockProvider::new(vec![text_response(StopReason::EndTurn)]),
                &mut history,
            );
            agent.config.post_compaction_instructions = Some(POST.into());
            agent.do_compact().await.unwrap();
            drop(agent);

            let last = history.as_slice().last().unwrap();
            assert!(matches!(
                &last.content[0],
                ContentBlock::Text { text } if text.ends_with(POST) && text != POST
            ));
        });
    }

    #[test]
    fn cancel_token_aborts_during_api_call() {
        smol::block_on(async {
            let (trigger, cancel) = CancelToken::new();
            trigger.cancel();

            let mut history = History::new(Vec::new());
            let (agent, event_rx) = make_agent(StubStreamProvider::default(), &mut history);
            let mut agent = agent.with_cancel(cancel);

            let outcome = agent.run(TurnId::generate(), default_input()).await;
            assert!(matches!(outcome, TurnOutcome::Cancelled { .. }));
            drop(agent);
            assert_ends_with_cancel_marker(&history);
            let events = drain_events(&event_rx);
            assert_eq!(terminal_outcomes(&events), [&outcome]);
            assert!(matches!(
                &outcome,
                TurnOutcome::Cancelled {
                    turn_id: actual_turn_id,
                    reason: TurnCancellationReason::User,
                    ..
                } if *actual_turn_id == outcome.turn_id()
            ));
        });
    }

    #[test]
    fn cancel_mid_stream_keeps_partial_text_in_history() {
        const PARTIAL: &str = "partial answer";
        smol::block_on(async {
            let (trigger, cancel) = CancelToken::new();
            let provider = StubStreamProvider {
                delta: Some(PARTIAL),
                cancel_after_delta: Mutex::new(Some(trigger)),
                ..Default::default()
            };
            let mut history = History::new(Vec::new());
            let (agent, _event_rx) = make_agent(provider, &mut history);
            let mut agent = agent.with_cancel(cancel);

            let outcome = agent.run(TurnId::generate(), default_input()).await;
            assert!(matches!(outcome, TurnOutcome::Cancelled { .. }));
            drop(agent);
            assert_ends_with_cancel_marker(&history);
            let messages = history.as_slice();
            let partial = &messages[messages.len() - 2];
            assert!(matches!(partial.role, Role::Assistant));
            let expected = format!("{PARTIAL}\n\n{CANCELLED_TEXT_NOTE}");
            assert!(
                matches!(&partial.content[0], ContentBlock::Text { text } if *text == expected),
                "kept text must carry the truncation note so the model never resumes it"
            );
        });
    }

    /// The `Retry` event already made the view drop the failed attempt's
    /// text, so history must not resurrect it (see `StreamError`).
    #[test]
    fn cancel_during_retry_backoff_discards_failed_attempt_text() {
        const PARTIAL: &str = "doomed attempt";
        smol::block_on(async {
            let (trigger, cancel) = CancelToken::new();
            let provider = StubStreamProvider {
                delta: Some(PARTIAL),
                fail_status: Some(529),
                ..Default::default()
            };
            let mut history = History::new(Vec::new());
            let (agent, event_rx) = make_agent(provider, &mut history);
            let mut agent = agent.with_cancel(cancel);

            let mut trigger = Some(trigger);
            let pump = smol::spawn(async move {
                while let Ok(envelope) = event_rx.recv_async().await {
                    if matches!(envelope.event, AgentEvent::Retry { .. })
                        && let Some(t) = trigger.take()
                    {
                        t.cancel();
                    }
                }
            });

            let outcome = agent.run(TurnId::generate(), default_input()).await;
            assert!(matches!(outcome, TurnOutcome::Cancelled { .. }));
            drop(agent);
            pump.await;

            assert_ends_with_cancel_marker(&history);
            assert!(
                history
                    .as_slice()
                    .iter()
                    .all(|m| !m.content.iter().any(
                        |b| matches!(b, ContentBlock::Text { text } if text.contains(PARTIAL))
                    )),
                "failed attempt's text must not reach history"
            );
        });
    }

    #[test_case(
        vec![tool_call_response("nonexistent_tool_xyz", "t1"), text_response(StopReason::EndTurn)],
        "t1"
        ; "parse_error"
    )]
    #[test_case(
        vec![tool_call_response("glob", "t1"), tool_call_response("glob", "t2"), tool_call_response("glob", "t3"), text_response(StopReason::EndTurn)],
        "t3"
        ; "doom_loop"
    )]
    fn error_emits_tool_done_event(responses: Vec<StreamResponse>, expected_error_id: &str) {
        smol::block_on(async {
            let mut history = History::new(Vec::new());
            let (mut agent, event_rx) = make_agent(MockProvider::new(responses), &mut history);
            let _ = agent.run(TurnId::generate(), default_input()).await;
            drop(agent);
            let events = drain_events(&event_rx);

            assert!(has_event(&events, |e| matches!(
                e,
                AgentEvent::ToolDone(done) if done.is_error && done.id == expected_error_id
            )));
        });
    }

    #[test_case(
        vec![
            tool_call_response("glob", "t1"),
            empty_response(),
            text_response(StopReason::EndTurn),
        ],
        3, 1
        ; "nudge_on_empty_after_tools"
    )]
    #[test_case(
        [tool_call_response("glob", "t1"), thinking_response()]
            .into_iter()
            .chain((0..MAX_NUDGES).map(|_| empty_response()))
            .collect(),
        MAX_NUDGES + 2, MAX_NUDGES as usize
        ; "gives_up_after_max_nudges"
    )]
    #[test_case(
        vec![
            tool_call_response("glob", "t1"),
            text_response(StopReason::EndTurn),
        ],
        2, 0
        ; "no_nudge_when_text_after_tools"
    )]
    #[test_case(
        vec![
            empty_response(),
            text_response(StopReason::EndTurn),
        ],
        1, 0
        ; "no_nudge_without_recent_tools"
    )]
    fn nudge_behavior(responses: Vec<StreamResponse>, expected_turns: u32, expected_nudges: usize) {
        smol::block_on(async {
            let mut history = History::new(Vec::new());
            let (mut agent, event_rx) = make_agent(MockProvider::new(responses), &mut history);
            let _ = agent.run(TurnId::generate(), default_input()).await;
            drop(agent);
            let events = drain_events(&event_rx);

            let nudges = events
                .iter()
                .filter(|e| matches!(e.event, AgentEvent::Nudge))
                .count();
            assert_eq!(nudges, expected_nudges);

            let done = events
                .iter()
                .find_map(|e| match &e.event {
                    AgentEvent::TurnOutcome(outcome) => Some(outcome.num_turns()),
                    _ => None,
                })
                .expect("expected terminal outcome");
            assert_eq!(done, expected_turns);

            assert!(
                history
                    .as_slice()
                    .iter()
                    .all(|m| m.content.iter().any(|b| !b.is_thinking())),
                "history holds a message no provider will accept: {:?}",
                history.as_slice()
            );
        });
    }

    /// Pins the regression where a stale nudge counter made a follow-up
    /// "continue" end instantly: the budget lives in the history tail, and
    /// the new user message breaks the streak.
    #[test]
    fn nudge_budget_resets_on_new_run() {
        smol::block_on(async {
            let responses = [tool_call_response("glob", "t1")]
                .into_iter()
                .chain((0..=MAX_NUDGES).map(|_| empty_response()))
                .chain([empty_response(), text_response(StopReason::EndTurn)])
                .collect();
            let mut history = History::new(Vec::new());
            let (mut agent, event_rx) = make_agent(MockProvider::new(responses), &mut history);
            let _ = agent.run(TurnId::generate(), default_input()).await;
            let _ = agent.run(TurnId::generate(), default_input()).await;
            drop(agent);
            let events = drain_events(&event_rx);

            let nudges = events
                .iter()
                .filter(|e| matches!(e.event, AgentEvent::Nudge))
                .count();
            assert_eq!(nudges, MAX_NUDGES as usize + 1);
        });
    }

    /// Wiring this to `None` to make the struct literal compile would
    /// silently reintroduce the bug the field exists to fix.
    #[test]
    fn tool_context_carries_the_session() {
        let mut history = History::new(Vec::new());
        let (mut agent, _event_rx) = make_agent(MockProvider::new(Vec::new()), &mut history);
        assert_eq!(agent.tool_context().session_id, None);

        let session: SessionRef = "01965087-4c71-7f00-8000-000000000000"
            .parse()
            .expect("valid session id");
        agent.session_id = Some(session.clone());
        assert_eq!(agent.tool_context().session_id, Some(session));
    }

    #[test]
    fn filter_tools_keeps_only_allowed_names() {
        let all = serde_json::json!([
            {"name": "read", "description": "r"},
            {"name": "write", "description": "w"},
            {"name": "grep", "description": "g"},
        ]);
        let filtered = filter_tools(&all, &["read".to_owned(), "grep".to_owned()]);
        let names: Vec<&str> = filtered
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|d| d["name"].as_str())
            .collect();
        assert_eq!(names, ["read", "grep"]);
    }

    #[test]
    fn filter_tools_non_array_passes_through() {
        let all = serde_json::json!({"name": "read"});
        assert_eq!(filter_tools(&all, &["write".to_owned()]), all);
    }

    #[test]
    fn mode_tools_swaps_requested_toolset() {
        let modes = crate::ModeRegistry::builtin();
        modes
            .define(crate::ModeDefSpec {
                name: "audit".into(),
                tools: Some(vec!["read".into(), "grep".into()]),
                ..Default::default()
            })
            .unwrap();
        let all = serde_json::json!([
            {"name": "read"},
            {"name": "write"},
            {"name": "grep"},
            {"name": "task"},
        ]);
        let def = modes.current(&AgentMode::Custom(crate::ModeId::Custom("audit".into())));
        let tools = def
            .tools
            .as_ref()
            .map(|names| filter_tools(&all, names))
            .unwrap_or_else(|| all.clone());
        let names: Vec<&str> = tools
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|d| d["name"].as_str())
            .collect();
        assert_eq!(names, ["read", "grep"]);
    }

    #[test]
    fn build_mode_inherits_full_toolset() {
        let modes = crate::ModeRegistry::builtin();
        let def = modes.current(&AgentMode::Build);
        assert!(def.tools.is_none(), "build inherits the default toolset");
        assert_eq!(modes.restrict_write_to(&AgentMode::Build), None);
        assert_eq!(modes.current(&AgentMode::Build).id, crate::ModeId::Build);
    }
}
