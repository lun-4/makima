use makima_domain::LockExt;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex, OnceLock};
use std::time::Duration;

use event_listener::Event;
#[cfg(test)]
use flume::Receiver;
use futures_lite::future;
use makima_config::{ModelPolicy, ProjectConfig, SessionDefaults};
use makima_providers::Message;
use makima_providers::model::Model;
use makima_providers::provider::BoxFuture;
use makima_providers::{ProviderRegistry, RequestOptions, Timeouts};
use makima_storage::id::{MakimaId, SessionRef};
use makima_storage::session_state::StateChange;
use serde_json::Value;
use tracing::warn;

use crate::actor::{
    ActorError, ActorHistory, AdmissionPreparation, AgentActorHandle, AgentContextSnapshot,
    CompactionStatus, ConfigCommit, PreparedModel, PreparedReadiness, TerminalCheckpoint,
    TurnAdmissionOptions, TurnOrigin, TurnProvenance,
};
#[cfg(test)]
use crate::agent::History;
#[cfg(test)]
use crate::agent::isolated_turn::collect_answer;
use crate::agent::isolated_turn::{IsolatedPrompt, OPERATION_CANCELLED};
use crate::agent::{
    self, Instructions, NO_ADMITTED_CWD_ERR, PromptSlotSource, TurnSetup, resolve_compaction_model,
};
use crate::cancel::{CancelMap, CancelScope, CancelToken, CancelTrigger, SessionCancellation};
use crate::config_change::{ConfigChangeError, unresolved_model};
#[cfg(test)]
use crate::context::root_context;
use crate::journal::{JournalChange, JournalCommit, JournalIncarnation, SessionJournal};
use crate::manager::{
    ReactivationError, ResidentAgent, ResidentInitialization, ResidentInitializer, SubmitOptions,
};
use crate::permissions::{PermissionManager, PermissionSnapshot, PluginRuleStore};
use crate::prompt::ResolvedSlots;
use crate::session::{AnswerRouter, SessionEventPump};
use crate::session_checkpoint::stored_agent_config;
use crate::session_coordinator::{ModelPreparer, SessionCoordinatorHandle};
use crate::session_options::SessionOptionError;
use crate::session_writer::SessionWriter;
use crate::template;
use crate::tools::offload::{OffloadCleanup, OffloadStore};
use crate::tools::{
    Deadline, FileReadTracker, FileWriteLocks, LocalTools, QuestionMode, RequestTools,
    ToolAudience, ToolContext, ToolRegistry, TurnToolBindings,
};
use crate::{
    ActorBackend, Agent, AgentConfig, AgentEvent, AgentId, AgentInput, AgentLimits,
    AgentManagerHandle, AgentMode, AgentParams, AgentRef, AgentRunParams, BackendResult,
    ControlWork, EffectiveAgentConfig, Envelope, EventSender, HistorySnapshot, McpHandle,
    McpSession, PermissionsConfig, RunLedger, RunSettings, SessionMailbox, ToolOutputLines,
    TurnCancellationReason, TurnContext, TurnFailure, TurnFailureKind, TurnId, TurnOutcome,
    TurnTicket, WorkKind,
};

const GRAPH_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);
const INITIALIZATION_CLOSED: &str = "session ended before initialization";
const TERMINAL_CHECKPOINT_TIMEOUT: &str = "checkpoint timed out";
const NO_TURN_CONFIG: &str = "turn was admitted without an effective agent configuration";
const SETUP_FAILED: &str = "agent turn setup failed";
const MCP_INIT_CANCELLED: &str = "MCP initialization cancelled";
const HEADLESS_CONFIG_UNAVAILABLE: &str = "managed headless configuration unavailable";
const FALLBACK_CHECKPOINT_UNAVAILABLE: &str =
    "restored mode fallback requires a session checkpoint";
const COMPACTION_CANCELLED: &str = "compaction cancelled";
const RESET_REQUIRES_FRONTEND: &str = "session reset requires frontend runtime replacement";

#[derive(Clone, Default)]
pub struct RootReadiness(Arc<RootInitialization>);

#[derive(Default)]
struct RootInitialization {
    result: OnceLock<Result<AgentRef, String>>,
    changed: Event,
}

impl RootReadiness {
    pub async fn get(&self) -> Result<AgentRef, String> {
        loop {
            let listener = self.0.changed.listen();
            if let Some(result) = self.0.result.get() {
                return result.clone();
            }
            listener.await;
        }
    }

    #[cfg(test)]
    fn current(&self) -> Option<Result<AgentRef, String>> {
        self.0.result.get().cloned()
    }

    fn finish(&self, result: Result<AgentRef, String>) {
        let _ = self.0.result.set(result);
        self.0.changed.notify(usize::MAX);
    }
}

/// Called after provider preparation and root creation, before admissions can run.
pub type RootBinding = Box<
    dyn for<'a> FnOnce(
            AgentManagerHandle,
            AgentRef,
            ToolContext,
            Option<&'a ResidentInitialization>,
        ) -> BoxFuture<'a, Result<(), String>>
        + Send,
>;

#[derive(Default)]
pub enum McpLifecycle {
    #[default]
    Owned,
    Shared {
        init_cancel: CancelToken,
    },
}

pub struct FrontendProfile {
    pub question_mode: QuestionMode,
    pub interactive_reauth: bool,
    pub tool_output_lines: ToolOutputLines,
    pub hold_input_after_failure: bool,
    pub excluded_tools: Vec<&'static str>,
    pub mcp_lifecycle: McpLifecycle,
    /// Emit `ControlError` for frontends that do not display root setup outcomes.
    pub report_setup_errors: bool,
}

impl Default for FrontendProfile {
    fn default() -> Self {
        Self {
            question_mode: QuestionMode::Headless,
            interactive_reauth: false,
            tool_output_lines: ToolOutputLines::default(),
            hold_input_after_failure: false,
            excluded_tools: Vec::new(),
            mcp_lifecycle: McpLifecycle::Owned,
            report_setup_errors: false,
        }
    }
}

impl FrontendProfile {
    pub fn tui(tool_output_lines: ToolOutputLines, init_cancel: CancelToken) -> Self {
        Self {
            question_mode: QuestionMode::Tui,
            interactive_reauth: true,
            tool_output_lines,
            hold_input_after_failure: true,
            mcp_lifecycle: McpLifecycle::Shared { init_cancel },
            report_setup_errors: true,
            ..Self::default()
        }
    }
}

pub struct InteractiveParams {
    pub model: Model,
    pub config: AgentConfig,
    pub permissions_config: PermissionsConfig,
    pub timeouts: Timeouts,
    pub prompt_slots: PromptSlotSource,
    pub profile: FrontendProfile,
    pub mcp_handle: Option<McpHandle>,
    pub initial_wd: PathBuf,
    pub session_id: Option<SessionRef>,
    pub root_id: Option<AgentId>,
    pub journal: Option<Arc<SessionJournal>>,
    pub initial_history: Vec<Message>,
    pub yolo: bool,
    pub system_prompt_override: Option<String>,
    pub append_system_prompt: Option<String>,
    pub defaults: SessionDefaults,
    pub model_policy: Arc<ModelPolicy>,
    pub providers: Arc<ProviderRegistry>,
    pub modes: Arc<crate::ModeRegistry>,
    pub plugin_rules: Arc<PluginRuleStore>,
    pub project_config: ProjectConfig,
    pub local_tools: LocalTools,
    pub state_dir: Option<PathBuf>,
    /// Whether the frontend answers permission asks through the answer router. Without
    /// it every ask is denied at once instead of waiting on nobody.
    pub answers: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ManualCompactionEvent {
    Started,
    Completed,
    Cancelled,
    Failed(String),
}

pub enum InteractiveControl {
    ManualCompaction {
        instructions: Option<String>,
        output: flume::Sender<ManualCompactionEvent>,
        cancel: CancelToken,
    },
    Reset(flume::Sender<Result<(), String>>),
}

#[derive(Clone)]
pub struct InteractiveControlSender {
    root: RootReadiness,
    next: Arc<AtomicU64>,
}

/// A control the actor owns until it runs. Dropping it unrun, because it was
/// cancelled or the actor closed, reports the cancellation to its caller; once
/// the actor drops its work, the task forwarding the caller's cancel ends.
struct PendingControl {
    control: Option<InteractiveControl>,
    _done: flume::Sender<()>,
}

impl PendingControl {
    fn cancel(&mut self) {
        match self.control.take() {
            Some(InteractiveControl::Reset(reply)) => {
                let _ = reply.send(Err(OPERATION_CANCELLED.into()));
            }
            Some(InteractiveControl::ManualCompaction { output, .. }) => {
                let _ = output.send(ManualCompactionEvent::Cancelled);
            }
            None => {}
        }
    }
}

impl Drop for PendingControl {
    fn drop(&mut self) {
        self.cancel();
    }
}

impl InteractiveControlSender {
    pub async fn send(&self, control: InteractiveControl) -> Result<(), String> {
        let actor = self
            .root
            .get()
            .await?
            .actor()
            .map_err(|error| error.to_string())?;
        self.send_to(actor, control)
    }

    pub fn send_to(
        &self,
        actor: AgentActorHandle,
        control: InteractiveControl,
    ) -> Result<(), String> {
        let key = format!(
            "headless-control:{}",
            self.next.fetch_add(1, Ordering::Relaxed)
        );
        let cancel = match &control {
            InteractiveControl::ManualCompaction { cancel, .. } => Some(cancel.clone()),
            _ => None,
        };
        let (done, finished) = flume::bounded(0);
        let pending = Arc::new(StdMutex::new(PendingControl {
            control: Some(control),
            _done: done,
        }));
        let queued = Arc::downgrade(&pending);
        actor
            .push_control(key.clone(), key.clone(), Some(pending))
            .map_err(|error| error.to_string())?;
        if let Some(cancel) = cancel {
            smol::spawn(async move {
                future::or(
                    async {
                        cancel.cancelled().await;
                        actor.cancel_correlation(&key, crate::TurnCancellationReason::User);
                        if let Some(pending) = queued.upgrade() {
                            pending.lock_unpoisoned().cancel();
                        }
                    },
                    async {
                        let _ = finished.recv_async().await;
                    },
                )
                .await;
            })
            .detach();
        }
        Ok(())
    }

    pub async fn compact_to(
        &self,
        actor: AgentActorHandle,
        instructions: Option<String>,
        cancel: CancelToken,
    ) -> Result<(), String> {
        let (output, events) = flume::unbounded();
        self.send_to(
            actor,
            InteractiveControl::ManualCompaction {
                instructions,
                output,
                cancel,
            },
        )?;
        loop {
            match events.recv_async().await {
                Ok(ManualCompactionEvent::Started) => {}
                Ok(event) => return compaction_result(event),
                Err(_) => return Err(OPERATION_CANCELLED.into()),
            }
        }
    }
}

/// Admits turns on the root like the frontend's input does, without keeping
/// the session alive: plugin-run commands hold one past the frontend's input.
#[derive(Clone)]
pub struct TurnAdmitter {
    root: RootReadiness,
    events: flume::Sender<Envelope>,
    next: Arc<AtomicU64>,
}

impl TurnAdmitter {
    pub async fn admit(&self, input: AgentInput) -> Result<TurnTicket, String> {
        self.ready()
            .await?
            .submit(input)
            .map(|(ticket, _)| ticket)
            .map_err(|error| error.to_string())
    }

    /// Waits for the root, so a frontend can then submit synchronously and
    /// record the turn under its own lock before the turn's events reach it.
    pub async fn ready(&self) -> Result<RootSubmission, String> {
        let root = self.root.get().await?;
        root.wake().await.map_err(|error| error.to_string())?;
        Ok(self.for_actor(root.actor().map_err(|error| error.to_string())?))
    }

    pub fn for_actor(&self, actor: AgentActorHandle) -> RootSubmission {
        RootSubmission {
            actor,
            events: self.events.clone(),
            run: self.next.fetch_add(1, Ordering::Relaxed),
        }
    }
}

/// One submission to a ready root, made the way the TUI's input makes it.
pub struct RootSubmission {
    actor: AgentActorHandle,
    events: flume::Sender<Envelope>,
    run: u64,
}

impl RootSubmission {
    /// Joins the root's compatible turn in flight, or starts one, and returns
    /// the ticket and presentation of the turn that carries {input}.
    pub fn submit(self, input: AgentInput) -> Result<(TurnTicket, u64), ActorError> {
        SubmitOptions {
            presentation: Some(EventSender::new(self.events)),
            correlation: format!("run:{}", self.run),
            admission: TurnAdmissionOptions {
                provenance: TurnProvenance {
                    origin: TurnOrigin::User,
                    ..Default::default()
                },
                ..Default::default()
            },
            ..Default::default()
        }
        .submit_to(&self.actor, input)
    }
}

#[derive(Clone)]
pub struct InteractiveInputSender {
    admitter: TurnAdmitter,
    _lifetime: flume::Sender<()>,
}

impl InteractiveInputSender {
    pub async fn admit(&self, input: AgentInput) -> Result<TurnTicket, String> {
        self.admitter.admit(input).await
    }

    pub fn admitter(&self) -> TurnAdmitter {
        self.admitter.clone()
    }
}

pub enum WorkCancel {
    Control(CancelTrigger),
    Scope(CancelScope, SessionCancellation),
}

impl WorkCancel {
    pub fn turn_id(&self) -> Option<TurnId> {
        match self {
            Self::Scope(CancelScope::Turn(ticket), _) => Some(ticket.turn_id()),
            _ => None,
        }
    }

    pub fn cancel(self) {
        match self {
            Self::Control(trigger) => trigger.cancel(),
            Self::Scope(scope, cancellation) => {
                if let Err(error) = cancellation.cancel(scope) {
                    warn!(%error, "work cancellation failed");
                }
            }
        }
    }
}

pub struct InteractiveHandle {
    pub manager: AgentManagerHandle,
    pub cancellation: SessionCancellation,
    pub root: RootReadiness,
    pub events: Arc<SessionEventPump>,
    #[cfg(test)]
    event_rx: Receiver<Envelope>,
    pub tool_names: Vec<String>,
    pub input_tx: InteractiveInputSender,
    pub answers: AnswerRouter,
    pub control_tx: InteractiveControlSender,
    pub session_id: SessionRef,
    pub mailbox: SessionMailbox,
    pub permissions: Arc<PermissionManager>,
    pub offload_cleanup: Option<OffloadCleanup>,
    pub task: smol::Task<()>,
}

impl InteractiveHandle {
    pub async fn initialized_root(&self) -> Result<AgentRef, String> {
        self.root.get().await
    }
}

pub type ProviderPreparation =
    Box<dyn FnOnce(Model) -> BoxFuture<'static, Result<PreparedModel, crate::AgentError>> + Send>;

pub async fn checkpoint_startup_config(
    root: &AgentRef,
    coordinator: &SessionCoordinatorHandle,
) -> Result<Arc<EffectiveAgentConfig>, String> {
    let manager = root.manager();
    let idle = manager
        .prepare_idle_subtree(root.id())
        .map_err(|error| error.to_string())?;
    let source = idle.source_snapshot().map_err(|error| error.to_string())?;
    persist_startup_config(root, coordinator, source.config, source.history).await
}

/// Checkpoints startup before the runner is released, including a recovered queue.
pub async fn checkpoint_deferred_startup_config(
    root: &AgentRef,
    coordinator: &SessionCoordinatorHandle,
) -> Result<(), String> {
    let actor = root.actor().map_err(|error| error.to_string())?;
    let config = actor.config_snapshot().ok_or(HEADLESS_CONFIG_UNAVAILABLE)?;
    persist_startup_config(
        root,
        coordinator,
        config,
        actor.committed_history_snapshot(),
    )
    .await
    .map(|_| ())
}

async fn persist_startup_config(
    root: &AgentRef,
    coordinator: &SessionCoordinatorHandle,
    commit: ConfigCommit,
    history: HistorySnapshot,
) -> Result<Arc<EffectiveAgentConfig>, String> {
    let options = coordinator.read().options().config_values();
    let config = stored_agent_config(&commit.config, options).map_err(|error| error.to_string())?;
    coordinator
        .permissions()
        .map_err(|error| error.to_string())?
        .commit_startup(JournalCommit {
            changes: vec![JournalChange {
                agent: Some(root.id()),
                generation: commit.generation,
                change: StateChange::Config(config),
            }],
            histories: [(root.id(), history.clone())].into(),
            managed_histories: [root.id()].into(),
            ..Default::default()
        })
        .map_err(|error| error.to_string())?;
    coordinator
        .persist_history(history)
        .await
        .map_err(|error| error.to_string())?;
    Ok(commit.config)
}

pub fn default_preparation(providers: Arc<ProviderRegistry>) -> ProviderPreparation {
    Box::new(|model| Box::pin(async move { providers.prepare(model).await }))
}

/// Resolves and prepares the model a session coordinator commits.
pub fn default_model_preparer(providers: Arc<ProviderRegistry>) -> ModelPreparer {
    Arc::new(move |spec| {
        let providers = Arc::clone(&providers);
        Box::pin(async move {
            let model = providers.model(&spec).map_err(unresolved_model)?;
            providers
                .prepare(model)
                .await
                .map_err(|error| ConfigChangeError::ModelPreparation(error.user_message().into()))
        })
    })
}

#[derive(Default)]
pub struct HeadlessBackendOptions {
    pub persist_turn_results: bool,
    pub child: bool,
    pub system_override: Option<String>,
    pub append_system: Option<String>,
    pub prompt_slots: PromptSlotSource,
    pub local_tools: LocalTools,
    pub mcp: Option<McpSession>,
    pub answers: Option<AnswerRouter>,
}

pub struct HeadlessBackend {
    params: AgentParams,
    persist_turn_results: bool,
    excluded_tools: Vec<&'static str>,
    hold_input_after_failure: bool,
    report_setup_errors: bool,
    mcp_lifecycle: McpLifecycle,
    mcp_initialized: bool,
    prompt: IsolatedPrompt,
    setup: TurnSetup,
    answers: Option<AnswerRouter>,
    raw_tx: flume::Sender<Envelope>,
}

impl HeadlessBackend {
    pub fn new(
        mut params: AgentParams,
        profile: FrontendProfile,
        raw_tx: flume::Sender<Envelope>,
        options: HeadlessBackendOptions,
    ) -> Self {
        params.tool_output_lines = profile.tool_output_lines;
        params.question_mode = profile.question_mode;
        params.interactive_reauth = profile.interactive_reauth;
        let setup = TurnSetup {
            registry: Arc::clone(&params.registry),
            local_tools: options.local_tools,
            mcp: options.mcp,
            slots: options.prompt_slots,
            modes: Arc::clone(&params.modes),
            instructions: !options.child,
        };
        Self {
            persist_turn_results: options.persist_turn_results,
            excluded_tools: profile.excluded_tools,
            hold_input_after_failure: profile.hold_input_after_failure,
            report_setup_errors: profile.report_setup_errors,
            mcp_lifecycle: profile.mcp_lifecycle,
            mcp_initialized: false,
            prompt: IsolatedPrompt {
                override_text: options.system_override.map(Arc::from),
                append_text: options.append_system.map(Arc::from),
                modes: Arc::clone(&params.modes),
                cold_slots: (profile.question_mode != QuestionMode::Tui && !options.child)
                    .then(|| setup.slots.clone()),
                session_id: params.session_id.clone(),
            },
            answers: options.answers,
            raw_tx,
            params,
            setup,
        }
    }

    fn event_sender(&self, context: &TurnContext) -> EventSender {
        EventSender::with_identity(self.raw_tx.clone(), Some(context.work))
    }

    async fn initialize_mcp(&mut self) -> bool {
        let McpLifecycle::Shared { init_cancel } = &self.mcp_lifecycle else {
            return true;
        };
        if self.mcp_initialized {
            return true;
        }
        if init_cancel.is_cancelled() {
            return false;
        }
        if let Some(mcp) = &self.setup.mcp {
            if init_cancel.race(mcp.ready()).await.is_err() || init_cancel.is_cancelled() {
                return false;
            }
            mcp.spawn_oauth_for_needs_auth();
        }
        self.mcp_initialized = !init_cancel.is_cancelled();
        self.mcp_initialized
    }

    fn setup_failed(
        &self,
        context: &TurnContext,
        turn_id: TurnId,
        failure: TurnFailure,
    ) -> BackendResult {
        warn!(%turn_id, error = %failure.diagnostic, "{SETUP_FAILED}");
        if !self.report_setup_errors {
            return BackendResult::SetupFailed(Some(failure));
        }
        if let Some(inbox) = &context.inbox {
            inbox.close();
        }
        let event_tx = self.event_sender(context);
        context.output.set_terminal_sender(event_tx.clone());
        let _ = event_tx.send(AgentEvent::ControlError {
            message: format!("{SETUP_FAILED}: {}", failure.user_message),
        });
        BackendResult::EnteredRun(TurnOutcome::failed(
            context.agent_id,
            turn_id,
            Default::default(),
            0,
            failure,
        ))
    }

    fn cancel_setup(&self, context: &TurnContext, turn_id: TurnId) -> BackendResult {
        if let Some(inbox) = &context.inbox {
            inbox.close();
        }
        context
            .output
            .set_terminal_sender(self.event_sender(context));
        BackendResult::EnteredRun(TurnOutcome::cancelled(
            context.agent_id,
            turn_id,
            Default::default(),
            0,
            context
                .cancel_reason
                .reason()
                .unwrap_or(TurnCancellationReason::User),
        ))
    }

    fn system(
        &self,
        config: &EffectiveAgentConfig,
        instructions: &Instructions,
        slots: &ResolvedSlots,
    ) -> String {
        self.prompt.system(
            config.context.cwd(),
            &config.mode,
            config.mode_def.as_ref(),
            instructions,
            slots,
        )
    }

    fn tools(&self, config: &EffectiveAgentConfig) -> RequestTools {
        RequestTools::build(
            &self.params.registry,
            &template::env_vars_for(config.context.cwd()),
            &config.model,
            &self.params.config,
            &self.excluded_tools,
            config.workflow,
            self.setup.mcp.is_some(),
        )
    }

    pub fn template(&self, config: &EffectiveAgentConfig) -> ToolContext {
        let tools = self.tools(config);
        ToolContext {
            execution_agent_id: Some(self.params.agent_id),
            provider: Arc::clone(&config.provider),
            model: Arc::new(config.model.clone()),
            event_tx: EventSender::new(self.raw_tx.clone()),
            mode: config.mode.clone(),
            mode_def: config.mode_def.clone().map(Arc::new),
            question_mode: self.params.question_mode,
            interactive_reauth: self.params.interactive_reauth,
            session_id: self.params.session_id.clone(),
            cwd: config.context.cwd().to_path_buf(),
            tool_use_id: None,
            call: None,
            answers: self.answers.clone(),
            loaded_instructions: Default::default(),
            cancel: CancelToken::none(),
            mcp: self.setup.mcp.clone(),
            deadline: Deadline::None,
            config: self.params.config.clone(),
            tool_filter: Arc::clone(tools.filter()),
            request_tools: Some(tools),
            tool_output_lines: self.params.tool_output_lines,
            permissions: Arc::clone(&self.params.permissions),
            timeouts: self.params.timeouts,
            file_tracker: Arc::clone(&self.params.file_tracker),
            prompt_slots: Arc::clone(&self.params.prompt_slots),
            root_context: self.params.root_context.clone(),
            modes: Arc::clone(&self.params.modes),
            opts: RequestOptions {
                thinking: config.thinking,
                fast: config.fast,
            },
            subagent_cancels: Arc::clone(&self.params.subagent_cancels),
            ledger: Arc::new(RunLedger::default()),
            registry: Arc::clone(&self.params.registry),
            workflow: config.workflow,
            audience: self.params.audience,
            local_tools: Arc::clone(&self.setup.local_tools),
            local_tool_capture: None,
            turn_bindings: Arc::new(TurnToolBindings::capture(
                &self.params.registry,
                &self.setup.local_tools,
                self.setup.mcp.as_ref(),
            )),
            live_sink: None,
            model_policy: Arc::clone(&self.params.model_policy),
            providers: Arc::clone(&self.params.providers),
            file_write_locks: Arc::clone(&self.params.file_write_locks),
            offload: self.params.offload.clone(),
            pending_output_limits: None,
            write_lock_chain: Arc::new(Vec::new()),
            managed_turn: None,
        }
    }

    async fn compact(
        &self,
        mut history: ActorHistory<'_>,
        context: &TurnContext,
        instructions: Option<&str>,
        cancel: &CancelToken,
    ) -> ManualCompactionEvent {
        let Some(config) = &context.policy else {
            return ManualCompactionEvent::Failed(NO_TURN_CONFIG.into());
        };
        let previous = history.snapshot().clone();
        let session = self
            .params
            .session_id
            .as_ref()
            .filter(|_| self.persist_turn_results);
        let (provider, model) = resolve_compaction_model(
            &config.provider,
            &config.model,
            &self.params.providers,
            &self.params.model_policy,
        )
        .await;
        let event_tx = self.event_sender(context);
        event_tx.try_send(AgentEvent::ModelSwitched {
            spec: model.spec(),
            context_window: model.context_window,
        });
        let result = agent::compact(
            &provider,
            &config.provider,
            &model,
            history.reborrow(),
            &event_tx,
            cancel,
            &self.params.config,
            instructions,
            session,
        )
        .await
        .map_err(|error| error.to_string());
        settle_manual_compaction(result, history, previous, cancel, |snapshot| async move {
            match session {
                Some(session) => persist_history(session.id(), snapshot).await,
                None => Ok(()),
            }
        })
        .await
    }
}

impl ActorBackend for HeadlessBackend {
    fn isolated_prompt(&self) -> Option<IsolatedPrompt> {
        Some(self.prompt.clone())
    }

    fn hold_input_after_failure(&self) -> bool {
        self.hold_input_after_failure
    }

    fn context_event_sender(&self) -> Option<EventSender> {
        Some(EventSender::new(self.raw_tx.clone()))
    }

    fn persist_turn_results(&self) -> bool {
        self.persist_turn_results
    }

    fn terminal_checkpoint(&self) -> Option<TerminalCheckpoint> {
        if !self.persist_turn_results {
            return None;
        }
        let session_id = self.params.session_id.as_ref()?.id();
        let timeout = self.params.timeouts.low_speed;
        Some(Arc::new(move |snapshot| {
            Box::pin(async move {
                future::or(
                    async {
                        if let Some(journal) = snapshot.journal {
                            journal
                                .wait_terminal_conversion(
                                    snapshot.result.outcome.agent_id(),
                                    snapshot.result.outcome.turn_id(),
                                )
                                .await?;
                            if journal.has_writer() {
                                return journal.flush().await;
                            }
                        }
                        persist_history(session_id, snapshot.history).await
                    },
                    async {
                        smol::Timer::after(timeout).await;
                        Err(TERMINAL_CHECKPOINT_TIMEOUT.to_owned())
                    },
                )
                .await
            })
        }))
    }

    fn offload_store(&self) -> Option<Arc<OffloadStore>> {
        self.params.offload.clone()
    }

    fn admission_preparation(&self) -> Option<AdmissionPreparation> {
        Some(self.setup.clone().preparation())
    }

    fn prepared_readiness(&self) -> Option<PreparedReadiness> {
        Some(TurnSetup::readiness())
    }

    fn run_turn<'a>(
        &'a mut self,
        history: ActorHistory<'a>,
        mut context: TurnContext,
        mut input: AgentInput,
        _work: WorkKind,
    ) -> BoxFuture<'a, BackendResult> {
        Box::pin(async move {
            let turn_id = context.turn_id.unwrap_or_else(TurnId::generate);
            if context.cancel.is_cancelled() {
                return self.cancel_setup(&context, turn_id);
            }
            let event_tx = self.event_sender(&context);
            if let Some(config) = &context.policy {
                event_tx.try_send(AgentEvent::ModelSwitched {
                    spec: config.model.spec(),
                    context_window: config.model.context_window,
                });
            }
            let initialized = context.cancel.race(self.initialize_mcp()).await;
            if context.cancel.is_cancelled() {
                return self.cancel_setup(&context, turn_id);
            }
            let fail = |message: &str| TurnFailure {
                kind: TurnFailureKind::Internal,
                diagnostic: message.into(),
                user_message: message.into(),
                retryable: true,
            };
            if !matches!(initialized, Ok(true)) {
                return self.setup_failed(&context, turn_id, fail(MCP_INIT_CANCELLED));
            }
            let Some(config) = &context.policy else {
                return self.setup_failed(&context, turn_id, fail(NO_TURN_CONFIG));
            };
            // Turns admitted during startup captured an incomplete MCP catalog.
            if matches!(self.mcp_lifecycle, McpLifecycle::Shared { .. })
                && let Some(mcp) = &self.setup.mcp
                && let Some(admission) = &mut context.admission
                && admission.mcp_startup_notice.is_some()
            {
                Arc::make_mut(&mut admission.bindings).refresh_mcp(mcp);
            }
            let prompt_inputs = context
                .admission
                .as_ref()
                .and_then(|admission| admission.prompt_inputs.as_ref());
            let Some(prompt_inputs) = prompt_inputs else {
                return self.setup_failed(&context, turn_id, fail(NO_ADMITTED_CWD_ERR));
            };
            let setup = context
                .cancel
                .race(prompt_inputs.resolve(&mut input, self.setup.mcp.as_ref()))
                .await;
            let slots = match setup {
                Err(_) => return self.cancel_setup(&context, turn_id),
                Ok(Err(error)) => {
                    return self.setup_failed(
                        &context,
                        turn_id,
                        TurnFailure {
                            kind: TurnFailureKind::Internal,
                            diagnostic: error.to_string(),
                            user_message: error.user_message(),
                            retryable: false,
                        },
                    );
                }
                Ok(Ok(slots)) => slots,
            };
            let root_context = prompt_inputs.root_context();
            let instructions = root_context
                .as_ref()
                .map(|context| context.instructions.clone())
                .unwrap_or_else(|| prompt_inputs.instructions.clone());
            let slots = root_context
                .as_ref()
                .map(|context| Arc::clone(&context.slots))
                .unwrap_or(slots);
            context.output.set_terminal_sender(event_tx.clone());
            let mut params = self.params.clone();
            params.prompt_slots = Arc::clone(&slots);
            let mut agent = Agent::for_actor_turn(
                params,
                AgentRunParams {
                    history,
                    system: self.system(config, &instructions, &slots),
                    event_tx: event_tx.clone(),
                    tools: self.tools(config),
                },
                &context,
                instructions.loaded,
            )
            .with_answers(self.answers.clone())
            .with_local_tools(Arc::clone(&self.setup.local_tools))
            .with_mcp(self.setup.mcp.clone());
            let outcome = agent.run(turn_id, input).await;
            drop(agent);
            BackendResult::EnteredRun(outcome)
        })
    }

    fn run_control<'a>(
        &'a mut self,
        history: ActorHistory<'a>,
        context: TurnContext,
        control: &'a ControlWork,
    ) -> BoxFuture<'a, BackendResult> {
        Box::pin(async move {
            let pending = control
                .payload
                .clone()
                .and_then(|payload| payload.downcast::<StdMutex<PendingControl>>().ok());
            let control = pending
                .as_ref()
                .and_then(|pending| pending.lock_unpoisoned().control.take());
            match control {
                Some(InteractiveControl::ManualCompaction {
                    instructions,
                    output,
                    cancel,
                }) => {
                    let _ = output.send(ManualCompactionEvent::Started);
                    let (trigger, combined) = CancelToken::new();
                    let cancellation = smol::spawn({
                        let actor_cancel = context.cancel.clone();
                        async move {
                            future::or(cancel.cancelled(), actor_cancel.cancelled()).await;
                            trigger.cancel();
                        }
                    });
                    let result = self
                        .compact(history, &context, instructions.as_deref(), &combined)
                        .await;
                    cancellation.cancel().await;
                    let status = match result {
                        ManualCompactionEvent::Completed => CompactionStatus::Completed,
                        ManualCompactionEvent::Cancelled => CompactionStatus::Cancelled,
                        _ => CompactionStatus::Failed,
                    };
                    let _ = output.send(result);
                    BackendResult::ControlCompacted(status)
                }
                Some(InteractiveControl::Reset(reply)) => {
                    let _ = reply.send(Err(RESET_REQUIRES_FRONTEND.into()));
                    BackendResult::ControlFailed
                }
                None => BackendResult::ControlFailed,
            }
        })
    }

    fn run_compact<'a>(
        &'a mut self,
        history: ActorHistory<'a>,
        context: TurnContext,
        instructions: Option<&'a str>,
    ) -> BoxFuture<'a, BackendResult> {
        Box::pin(async move {
            let result = self
                .compact(history, &context, instructions, &context.cancel)
                .await;
            match compaction_result(result) {
                Ok(()) => BackendResult::CompactDone,
                Err(message) => {
                    let _ = self
                        .event_sender(&context)
                        .send(AgentEvent::ControlError { message });
                    BackendResult::ControlFailed
                }
            }
        })
    }
}

fn compaction_result(event: ManualCompactionEvent) -> Result<(), String> {
    match event {
        ManualCompactionEvent::Completed => Ok(()),
        ManualCompactionEvent::Failed(error) => Err(error),
        ManualCompactionEvent::Cancelled => Err(COMPACTION_CANCELLED.into()),
        ManualCompactionEvent::Started => unreachable!(),
    }
}

async fn settle_manual_compaction<P, F>(
    compacted: Result<(), String>,
    mut history: ActorHistory<'_>,
    previous: crate::agent::HistorySnapshot,
    cancel: &CancelToken,
    persist: P,
) -> ManualCompactionEvent
where
    P: FnOnce(crate::agent::HistorySnapshot) -> F,
    F: Future<Output = Result<(), String>>,
{
    let result = match compacted {
        Ok(()) => persist(history.snapshot().clone()).await,
        Err(error) => Err(error),
    };
    match result {
        Ok(()) => ManualCompactionEvent::Completed,
        Err(error) => {
            history.replace_snapshot(previous).await;
            if cancel.is_cancelled() {
                ManualCompactionEvent::Cancelled
            } else {
                ManualCompactionEvent::Failed(error)
            }
        }
    }
}

async fn persist_history(
    session_id: MakimaId,
    history: crate::agent::HistorySnapshot,
) -> Result<(), String> {
    SessionCoordinatorHandle::resolve(session_id)
        .map_err(|error| error.to_string())?
        .persist_history(history)
        .await
        .map_err(|error| error.to_string())
}

struct ManagedLifetime {
    manager: AgentManagerHandle,
    root: RootReadiness,
    cleanup: Option<OffloadCleanup>,
    children: Arc<CancelMap<String>>,
}

impl ManagedLifetime {
    fn offload_cleanup(&self) -> Option<&OffloadCleanup> {
        self.cleanup
            .as_ref()
            .filter(|_| self.manager.journal().session_id().is_none())
    }
}

impl Drop for ManagedLifetime {
    fn drop(&mut self) {
        self.root.finish(Err(INITIALIZATION_CLOSED.into()));
        if self.manager.journal().session_id().is_none() {
            self.children.cancel_all();
        }
        if let Some(cleanup) = self.offload_cleanup() {
            cleanup.request();
        }
        let manager = self.manager.clone();
        smol::spawn(async move {
            manager.suspend(GRAPH_SHUTDOWN_TIMEOUT).await;
        })
        .detach();
    }
}

#[cfg(test)]
struct RemoveOffloadOnDrop(OffloadCleanup);

#[cfg(test)]
impl Drop for RemoveOffloadOnDrop {
    fn drop(&mut self) {
        self.0.request();
    }
}

async fn drain_offload(cleanup: Option<&OffloadCleanup>) {
    if let Some(cleanup) = cleanup {
        cleanup.request();
        let _ = cleanup.wait().await;
    }
}

fn session_offload(state_dir: Option<&Path>, session: &SessionRef) -> Option<Arc<OffloadStore>> {
    state_dir.map(|root| Arc::new(OffloadStore::for_session(root, session)))
}

pub fn spawn_interactive_with_binding(
    params: InteractiveParams,
    prepare: ProviderPreparation,
    history: Option<crate::agent::HistorySnapshot>,
    binding: Option<RootBinding>,
) -> InteractiveHandle {
    spawn_interactive(params, prepare, history, binding, true, None)
}

pub fn spawn_interactive_with_checkpoint(
    params: InteractiveParams,
    prepare: ProviderPreparation,
    history: Option<crate::agent::HistorySnapshot>,
    binding: Option<RootBinding>,
    checkpoint: Arc<SessionWriter>,
) -> InteractiveHandle {
    spawn_interactive(params, prepare, history, binding, true, Some(checkpoint))
}

pub fn spawn_nonpersistent_with_binding(
    params: InteractiveParams,
    prepare: ProviderPreparation,
    binding: Option<RootBinding>,
) -> InteractiveHandle {
    spawn_interactive(params, prepare, None, binding, false, None)
}

fn spawn_interactive(
    mut params: InteractiveParams,
    prepare: ProviderPreparation,
    history: Option<crate::agent::HistorySnapshot>,
    binding: Option<RootBinding>,
    persist_turn_results: bool,
    checkpoint: Option<Arc<SessionWriter>>,
) -> InteractiveHandle {
    let session_ref = params
        .session_id
        .clone()
        .unwrap_or_else(|| SessionRef::from(MakimaId::generate()));
    let root_id = params.root_id.unwrap_or_else(AgentId::generate);
    let mailbox = SessionMailbox::new(session_ref.id());
    let limits = AgentLimits {
        max_concurrent_agent_turns: params.config.max_concurrent_agent_turns.max(1),
        max_agent_depth: params.config.max_agent_depth,
        max_children_per_agent: params.config.max_children_per_agent,
        max_active_agents: params.config.max_active_agents,
        max_retained_agents: params.config.max_retained_agents,
    };
    let journal = if persist_turn_results {
        params.journal.clone().unwrap_or_default()
    } else {
        Arc::new(SessionJournal::nonpersistent())
    };
    let snapshot = journal.snapshot();
    let restored_graph = snapshot.agent_revisions.iter().next().is_some();
    let mut restored_config = None;
    let mut restored_permissions = PermissionSnapshot::default();
    let manager = if restored_graph {
        snapshot
            .fold()
            .map_err(|error| error.to_string())
            .and_then(|view| {
                if let Some(scope) = view.scopes.get(&None) {
                    restored_permissions =
                        PermissionSnapshot::from_stored(&scope.rules, scope.yolo)
                            .map_err(|error| error.to_string())?;
                }
                restored_config = view
                    .scopes
                    .get(&Some(root_id))
                    .and_then(|scope| scope.config.clone());
                if let Some(stored) = &mut restored_config {
                    if checkpoint.is_some() {
                        stored.model = params.model.spec();
                    } else {
                        if !params.model_policy.allows(&stored.model) {
                            return Err(SessionOptionError::PolicyRejected(
                                stored.model.clone().into(),
                            )
                            .to_string());
                        }
                        params.model = params
                            .providers
                            .model(&stored.model)
                            .map_err(|error| error.to_string())?;
                    }
                    params.initial_wd = PathBuf::from(&stored.cwd);
                    params.defaults = SessionDefaults {
                        fast: stored.fast,
                        workflow: stored.workflow,
                        thinking: stored.thinking,
                    };
                }
                AgentManagerHandle::restore_graph(limits, journal, view, snapshot.histories.clone())
                    .map_err(|error| error.to_string())
            })
    } else {
        AgentManagerHandle::new_with_journal(limits, journal).map_err(|error| error.to_string())
    };
    let startup_error = manager.as_ref().err().cloned();
    let manager = manager.unwrap_or_else(|_| {
        AgentManagerHandle::new(AgentLimits::default()).expect("default agent limits are valid")
    });
    let root = RootReadiness::default();
    let (guard, events) = crate::types::event_stream();
    let raw_tx = guard.tx().clone();
    let (lifetime_tx, lifetime_rx) = flume::unbounded();
    let input_tx = InteractiveInputSender {
        admitter: TurnAdmitter {
            root: root.clone(),
            events: raw_tx.clone(),
            next: Arc::new(AtomicU64::new(0)),
        },
        _lifetime: lifetime_tx,
    };
    let control_tx = InteractiveControlSender {
        root: root.clone(),
        next: Arc::new(AtomicU64::new(0)),
    };
    let answers = AnswerRouter::default();
    let backend_answers = params.answers.then(|| answers.clone());
    let mut permission_config = params.permissions_config.clone();
    permission_config.yolo |= params.yolo;
    let permissions = Arc::new(PermissionManager::new(
        permission_config,
        params.initial_wd.clone(),
        params.project_config.clone(),
        Arc::clone(&params.plugin_rules),
    ));
    permissions.restore(restored_permissions);
    let offload = session_offload(params.state_dir.as_deref(), &session_ref);
    let cleanup = offload
        .as_ref()
        .map(|store| OffloadCleanup::new(Arc::clone(store)));
    let children = Arc::new(CancelMap::new());
    let cancellation =
        SessionCancellation::new(manager.clone(), root_id, Arc::clone(&children), None);
    let lifetime = ManagedLifetime {
        manager: manager.clone(),
        root: root.clone(),
        cleanup: cleanup.clone(),
        children: Arc::clone(&children),
    };
    let tools = RequestTools::build(
        ToolRegistry::global(),
        &template::env_vars_for(&params.initial_wd),
        &params.model,
        &params.config,
        &params.profile.excluded_tools,
        params.defaults.workflow,
        params.mcp_handle.is_some(),
    );
    let mcp = params
        .mcp_handle
        .clone()
        .map(|handle| McpSession::new(handle, &params.initial_history));
    let tool_names = advertised_tool_names(tools.definitions(), mcp.as_ref());
    let offload_cleanup = cleanup.clone();
    let task = smol::spawn({
        let manager = manager.clone();
        let root = root.clone();
        let permissions = Arc::clone(&permissions);
        let mailbox = mailbox.clone();
        let session_ref = session_ref.clone();
        async move {
            let _lifetime = lifetime;
            let owns_mcp = matches!(params.profile.mcp_lifecycle, McpLifecycle::Owned);
            let initialized = async {
                if let Some(error) = startup_error {
                    return Err(error);
                }
                let PreparedModel { model, provider } = prepare(params.model.clone())
                    .await
                    .map_err(|error| error.user_message())?;
                if persist_turn_results && let Some(store) = &offload {
                    manager
                        .bind_result_store(Arc::clone(store))
                        .map_err(|error| error.to_string())?;
                }
                let mut mode_fallback = false;
                let config = if let Some(stored) = restored_config {
                    let restored = crate::plan_state::prepare_restore_for_agent(
                        &snapshot.fold().map_err(|error| error.to_string())?,
                        root_id,
                        &stored.mode,
                        &params.modes,
                    )
                    .map_err(|error| error.to_string())?;
                    if let Some(warning) = restored.warning {
                        mode_fallback = true;
                        warn!(%warning, "saved mode restoration fell back to read-only");
                        let _ = raw_tx.send(Envelope {
                            event: AgentEvent::ControlError { message: warning },
                            subagent: None,
                            identity: None,
                        });
                    }
                    EffectiveAgentConfig::from_stored(
                        &stored,
                        PreparedModel {
                            provider: Arc::clone(&provider),
                            model: model.clone(),
                        },
                        (restored.mode, restored.definition),
                    )
                    .map_err(|error| error.to_string())?
                } else {
                    EffectiveAgentConfig::new(
                        RunSettings {
                            provider: Arc::clone(&provider),
                            model: model.clone(),
                            fast: params.defaults.fast,
                            workflow: params.defaults.workflow,
                            thinking: params.defaults.thinking.into(),
                        },
                        AgentMode::Build,
                    )
                    .with_context(AgentContextSnapshot::new(params.initial_wd.clone()))
                    .with_mode_def(Some(params.modes.current(&AgentMode::Build)))
                };
                if restored_graph && crate::modes::is_read_only_fallback(&config.mode) {
                    mode_fallback = true;
                    manager
                        .suppress_root_continuations()
                        .map_err(|error| error.to_string())?;
                }
                let backend = HeadlessBackend::new(
                    AgentParams {
                        agent_id: root_id,
                        provider,
                        model,
                        config: params.config,
                        tool_output_lines: ToolOutputLines::default(),
                        permissions,
                        session_id: Some(session_ref.clone()),
                        mailbox: Some(mailbox),
                        timeouts: params.timeouts,
                        file_tracker: FileReadTracker::fresh(),
                        prompt_slots: Arc::default(),
                        root_context: None,
                        modes: Arc::clone(&params.modes),
                        subagent_cancels: children,
                        registry: Arc::clone(ToolRegistry::global_arc()),
                        audience: ToolAudience::MAIN,
                        question_mode: QuestionMode::Headless,
                        interactive_reauth: false,
                        model_policy: params.model_policy,
                        providers: params.providers,
                        file_write_locks: Arc::new(FileWriteLocks::new()),
                        offload,
                        managed_turn: None,
                    },
                    params.profile,
                    raw_tx.clone(),
                    HeadlessBackendOptions {
                        persist_turn_results,
                        child: false,
                        system_override: params.system_prompt_override,
                        append_system: params.append_system_prompt,
                        prompt_slots: params.prompt_slots,
                        local_tools: params.local_tools,
                        mcp,
                        answers: backend_answers,
                    },
                );
                let mut template = backend.template(&config);
                if restored_graph {
                    let manager_for_binding = manager.clone();
                    let initialize: ResidentInitializer =
                        Box::new(move |installed: ResidentInitialization| {
                            template.execution_agent_id = Some(installed.agent.id());
                            Box::pin(async move {
                                manager_for_binding
                                    .validate_activation(&installed.incarnation)
                                    .map_err(|error| {
                                        ReactivationError::Initialization(error.to_string())
                                    })?;
                                let journal = manager_for_binding.journal();
                                template
                                    .permissions
                                    .bind_journal(
                                        Arc::clone(&journal),
                                        Some(JournalIncarnation {
                                            agent: installed.incarnation.agent_id,
                                            reservation: installed.incarnation.reservation,
                                        }),
                                    )
                                    .map_err(|error| {
                                        ReactivationError::Initialization(error.to_string())
                                    })?;
                                let incarnation = JournalIncarnation {
                                    agent: installed.incarnation.agent_id,
                                    reservation: installed.incarnation.reservation,
                                };
                                if let Some(binding) = binding {
                                    binding(
                                        manager_for_binding,
                                        installed.agent.clone(),
                                        template,
                                        Some(&installed),
                                    )
                                    .await
                                    .map_err(ReactivationError::Initialization)?;
                                }
                                let actor = installed.actor.clone();
                                installed
                                    .finish_with(async move {
                                        if mode_fallback {
                                            let checkpoint = checkpoint.ok_or_else(|| {
                                                ReactivationError::Initialization(
                                                    FALLBACK_CHECKPOINT_UNAVAILABLE.into(),
                                                )
                                            })?;
                                            let commit =
                                                actor.config_snapshot().ok_or_else(|| {
                                                    ReactivationError::Initialization(
                                                        HEADLESS_CONFIG_UNAVAILABLE.into(),
                                                    )
                                                })?;
                                            let stored = stored_agent_config(
                                                &commit.config,
                                                Default::default(),
                                            )
                                            .map_err(|error| {
                                                ReactivationError::Initialization(error.to_string())
                                            })?;
                                            let (_startup_trigger, startup_cancel) =
                                                CancelToken::new();
                                            let expected_incarnation = incarnation.clone();
                                            let prepared = journal
                                                .prepare_with(move |current| {
                                                    let mut stored = stored;
                                                    if let Some(latest) = current
                                                        .state
                                                        .iter()
                                                        .rev()
                                                        .find_map(|record| match &record.change {
                                                            StateChange::Config(config)
                                                                if record.agent
                                                                    == Some(incarnation.agent) =>
                                                            {
                                                                Some(config)
                                                            }
                                                            _ => None,
                                                        })
                                                    {
                                                        stored.options = latest.options.clone();
                                                    }
                                                    JournalCommit {
                                                        incarnations: vec![incarnation.clone()],
                                                        changes: vec![JournalChange {
                                                            agent: Some(incarnation.agent),
                                                            generation: commit.generation,
                                                            change: StateChange::Config(stored),
                                                        }],
                                                        ..Default::default()
                                                    }
                                                })
                                                .await
                                                .map_err(|error| {
                                                    ReactivationError::Initialization(
                                                        error.to_string(),
                                                    )
                                                })?;
                                            checkpoint
                                                .publish_prepared(
                                                    prepared,
                                                    startup_cancel,
                                                    expected_incarnation,
                                                    Arc::clone(&journal),
                                                )
                                                .await
                                                .map_err(|error| {
                                                    ReactivationError::Initialization(
                                                        error.to_string(),
                                                    )
                                                })?;
                                        }
                                        Ok(())
                                    })
                                    .await
                            })
                        });
                    let reference = manager
                        .ensure_resident_with(root_id, move |_| async move {
                            Ok(ResidentAgent {
                                backend: Box::new(backend),
                                config,
                                shared_messages: None,
                                start: None,
                                initialize: Some(initialize),
                            })
                        })
                        .await
                        .map_err(|error| error.to_string())?;
                    root.finish(Ok(reference.clone()));
                    return Ok(reference);
                }
                let mirror =
                    history.map(|snapshot| Arc::new(arc_swap::ArcSwap::from_pointee(snapshot)));
                let initial_history = mirror
                    .as_ref()
                    .map(|mirror| mirror.load().messages.as_ref().clone())
                    .unwrap_or(params.initial_history);
                let (start, started) = flume::bounded(1);
                let reference = manager
                    .create_root_deferred_with_id_and_config(
                        root_id,
                        Some(config),
                        initial_history,
                        mirror,
                        |agent_id| {
                            let mut backend = backend;
                            backend.params.agent_id = agent_id;
                            Ok::<Box<dyn ActorBackend>, String>(Box::new(backend))
                        },
                        started,
                    )
                    .map_err(|error| error.to_string())?;
                template.execution_agent_id = Some(reference.id());
                let journal = manager.journal();
                template
                    .permissions
                    .bind_journal(
                        Arc::clone(&journal),
                        Some(journal.incarnation(reference.id())),
                    )
                    .map_err(|error| error.to_string())?;
                if let Some(binding) = binding {
                    binding(manager.clone(), reference.clone(), template, None).await?;
                }
                root.finish(Ok(reference.clone()));
                start
                    .send(())
                    .map_err(|_| INITIALIZATION_CLOSED.to_owned())?;
                Ok::<_, String>(reference)
            }
            .await;
            match initialized {
                Ok(_) => {
                    let _ = lifetime_rx.recv_async().await;
                }
                Err(message) => {
                    root.finish(Err(message.clone()));
                    let _ = EventSender::new(raw_tx).send(AgentEvent::ControlError { message });
                }
            }
            manager.suspend(GRAPH_SHUTDOWN_TIMEOUT).await;
            if owns_mcp && let Some(handle) = params.mcp_handle {
                handle.shutdown().await;
            }
            drain_offload(_lifetime.offload_cleanup()).await;
            drop(guard);
        }
    });
    let raw_events = events.into_receiver();
    let events = SessionEventPump::new(raw_events.clone());
    InteractiveHandle {
        manager,
        cancellation,
        root,
        events,
        #[cfg(test)]
        event_rx: raw_events,
        tool_names,
        input_tx,
        answers,
        control_tx,
        session_id: session_ref,
        mailbox,
        permissions,
        offload_cleanup,
        task,
    }
}

fn advertised_tool_names(tools: &Value, mcp: Option<&McpSession>) -> Vec<String> {
    let mut probe = tools.clone();
    if let Some(mcp) = mcp {
        mcp.extend_tools(&mut probe);
    }
    extract_tool_names(&probe)
}

fn extract_tool_names(tools: &Value) -> Vec<String> {
    tools
        .as_array()
        .map(|array| {
            array
                .iter()
                .filter_map(|tool| tool.get("name").and_then(Value::as_str).map(String::from))
                .collect()
        })
        .unwrap_or_default()
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::actor::{ActorEvent, ConfigChange, ConfigPatch, PreparedModel};
    use crate::cancel::ReasonedCancelToken;
    use crate::mcp::{McpCommand, test_support::gated_session};
    use crate::prompt::{PromptId, Slot, SlotEntry};
    use crate::session_coordinator::tests::bind_initializing_actor;
    use crate::session_coordinator::{SessionCoordinatorParams, builtin_option_definitions};
    use crate::test_util::MockBackend;
    use crate::tools::QUESTION_TOOL_NAME;
    use crate::tools::offload::{OffloadBackend, OffloadError};
    use crate::{
        ActorWorkIdentity, ActorWorkKind, DoneReason, McpPromptRef, ModeRegistry, ToolOutput,
        TurnResult,
    };
    use makima_providers::login::{AuthStatus, InteractionService};
    use makima_providers::model_registry;
    use makima_providers::provider::Provider;
    use makima_providers::{
        ContentBlock, ConversationEnd, Invocation, InvocationPurpose, ModelInfo, ModelTier,
        PreparedProvider, ProviderBackend, ProviderEvent, RegisteredProvider, RequestDelivery,
        Role, StopReason, StreamResponse, TokenUsage,
    };
    use makima_storage::StateDir;
    use makima_storage::checkpoint::{
        CheckpointAck, CheckpointError, CheckpointFuture, CheckpointPublication, CheckpointRequest,
        CheckpointWriter, ClaimingWriter, accept_all,
    };
    use makima_storage::session_state::{
        MAX_INLINE_TURN_RESULT_BYTES, StoredDelivery, StoredDeliveryOp, StoredPayload,
    };
    use makima_storage::sessions::{SESSIONS_DIR, Session, offload_dir};
    use makima_storage::test_util::test_record;
    use std::collections::BTreeMap;
    use std::io;
    use std::sync::atomic::AtomicUsize;
    use std::sync::{Barrier, LazyLock};
    use std::thread;
    use test_case::test_case;

    const ADMITTED_CWD: &str = "/tmp/admitted";
    const ADMITTED_INSTRUCTIONS: &str = "admitted instructions";
    const ADMITTED_IDENTITY: &str = "admitted identity";
    const MCP_TOOL: &str = "stub.tool";
    const MCP_WIRE_TOOL: &str = "stub__tool";
    const OUTPUT_LINES: usize = 12;
    const CHANGED_CWD: &str = "/tmp/changed";
    const PRESENTATION: u64 = 1;
    const GATE_TIMEOUT: Duration = Duration::from_secs(10);
    const HEADLESS_PROVIDER_ERROR: &str = "headless provider failed";
    const SAVED_OUTPUT: &str = "saved output";
    const SAVE_ERROR: &str = "save failed";
    const COMPACTION_PROVIDER: &str = "test-compaction";
    const COMPACTION_MODEL: &str = "test-compaction/summary";
    const COMPACTION_WINDOW: u32 = 32_768;
    const FIRST: &str = "first provider";
    const SECOND: &str = "second provider";
    const TEST_MODEL: &str = "anthropic/claude-sonnet-4-20250514";
    static TEST_CWD: LazyLock<String> =
        LazyLock::new(|| std::env::temp_dir().to_string_lossy().into_owned());
    const ORIGINAL_RECOVERY_INPUT: &str = "interrupted input";
    const MISSING_PROMPT: &str = "srv.missing";
    const TERMINAL_CHECKPOINT_GATE: &str = "terminal checkpoint entered";
    const TERMINAL_RUNNER_DRAIN: &str = "setup-failure runner drained";
    const TERMINAL_TICKET_PUBLICATION: &str = "terminal ticket published";
    const TERMINAL_EXACT_PUBLICATION: &str = "exact terminal outcome published";
    const TERMINAL_SHUTDOWN: &str = "shutdown after terminal publication";
    const TERMINAL_SETTLEMENT: &str = "graph settled after shutdown";
    const TERMINAL_HANDLE_CANCEL: &str = "headless lifetime cancelled";
    const TERMINAL_COORDINATOR_CLOSE: &str = "terminal coordinator closed";
    type Checkpoint = Arc<dyn CheckpointWriter<crate::session_coordinator::SessionCheckpoint>>;
    type RecordedRequest = (&'static str, String, String, bool, Vec<String>);

    #[derive(Clone, Copy)]
    enum ProviderExit {
        Success,
        Pending,
    }

    struct GatedHeadlessProvider {
        entered: flume::Sender<()>,
        release: Receiver<()>,
        exit: ProviderExit,
    }

    fn response(text: &str) -> StreamResponse {
        StreamResponse {
            message: Message {
                role: Role::Assistant,
                content: vec![ContentBlock::Text { text: text.into() }],
                ..Default::default()
            },
            usage: TokenUsage::default(),
            stop_reason: Some(StopReason::EndTurn),
        }
    }

    impl Provider for GatedHeadlessProvider {
        fn stream_message<'a>(
            &'a self,
            _: &'a Model,
            _: &'a [Message],
            _: &'a str,
            _: &'a Value,
            events: &'a flume::Sender<ProviderEvent>,
            _: RequestOptions,
            _: Option<&'a SessionRef>,
            invocation: &'a Invocation,
            _delivery: RequestDelivery,
        ) -> BoxFuture<'a, Result<StreamResponse, crate::AgentError>> {
            Box::pin(async move {
                if invocation.purpose == InvocationPurpose::Isolated {
                    events
                        .send_async(ProviderEvent::TextDelta {
                            text: SAVED_OUTPUT.into(),
                        })
                        .await
                        .unwrap();
                    return Ok(response(SAVED_OUTPUT));
                }
                self.entered.send_async(()).await.unwrap();
                self.release.recv_async().await.unwrap();
                match self.exit {
                    ProviderExit::Success => Ok(response(SAVED_OUTPUT)),
                    ProviderExit::Pending => std::future::pending().await,
                }
            })
        }
    }

    struct OversizedPrintProvider;

    impl Provider for OversizedPrintProvider {
        fn stream_message<'a>(
            &'a self,
            _: &'a Model,
            _: &'a [Message],
            _: &'a str,
            _: &'a Value,
            _: &'a flume::Sender<makima_providers::ProviderEvent>,
            _: RequestOptions,
            _: Option<&'a SessionRef>,
            _: &'a Invocation,
            _: RequestDelivery,
        ) -> BoxFuture<'a, Result<StreamResponse, crate::AgentError>> {
            Box::pin(async {
                Ok(response(&SAVED_OUTPUT.repeat(
                    MAX_INLINE_TURN_RESULT_BYTES / SAVED_OUTPUT.len() + 1,
                )))
            })
        }
    }

    struct RecordingProvider {
        name: &'static str,
        requests: flume::Sender<RecordedRequest>,
    }

    struct EndRecordingProvider {
        requests: flume::Sender<Arc<str>>,
        ended: flume::Sender<(String, ConversationEnd)>,
    }

    impl Provider for EndRecordingProvider {
        fn stream_message<'a>(
            &'a self,
            _: &'a Model,
            _: &'a [Message],
            _: &'a str,
            _: &'a Value,
            _: &'a flume::Sender<makima_providers::ProviderEvent>,
            _: RequestOptions,
            _: Option<&'a SessionRef>,
            invocation: &'a Invocation,
            _delivery: RequestDelivery,
        ) -> BoxFuture<'a, Result<StreamResponse, crate::AgentError>> {
            let _ = self.requests.send(Arc::clone(&invocation.conversation_id));
            Box::pin(async { Ok(response(SAVED_OUTPUT)) })
        }

        fn end_conversation(
            &self,
            conversation_id: &str,
            reason: ConversationEnd,
        ) -> BoxFuture<'_, ()> {
            let _ = self.ended.send((conversation_id.to_owned(), reason));
            Box::pin(async {})
        }
    }

    impl Provider for RecordingProvider {
        fn stream_message<'a>(
            &'a self,
            model: &'a Model,
            _: &'a [Message],
            system: &'a str,
            tools: &'a Value,
            _: &'a flume::Sender<makima_providers::ProviderEvent>,
            options: RequestOptions,
            _: Option<&'a SessionRef>,
            _invocation: &'a Invocation,
            _delivery: RequestDelivery,
        ) -> BoxFuture<'a, Result<StreamResponse, crate::AgentError>> {
            Box::pin(async move {
                let _ = self.requests.send((
                    self.name,
                    model.spec(),
                    system.into(),
                    options.fast,
                    extract_tool_names(tools),
                ));
                Ok(response(self.name))
            })
        }
    }

    struct CompactionBackend(Arc<dyn Provider>);

    impl ProviderBackend for CompactionBackend {
        fn prepare(
            &self,
            model: Model,
            _: ModelInfo,
        ) -> BoxFuture<'static, Result<PreparedProvider, crate::AgentError>> {
            let provider = Arc::clone(&self.0);
            Box::pin(async move { Ok(PreparedProvider { model, provider }) })
        }

        fn discover(
            &self,
            _: bool,
        ) -> Option<BoxFuture<'static, Result<Vec<ModelInfo>, crate::AgentError>>> {
            None
        }

        fn login(
            &self,
            _: String,
            _: Value,
            _: Arc<dyn InteractionService>,
        ) -> BoxFuture<'static, Result<Option<String>, crate::AgentError>> {
            Box::pin(async { Ok(None) })
        }

        fn auth_status(
            &self,
            _: Value,
        ) -> Option<BoxFuture<'static, Result<AuthStatus, crate::AgentError>>> {
            None
        }

        fn logout(&self) -> Option<BoxFuture<'static, Result<(), crate::AgentError>>> {
            None
        }
    }

    #[test]
    fn isolated_controls_use_recorded_context_until_explicit_reload() {
        use crate::prompt::{PromptId, Slot, SlotEntry};

        const INSTRUCTIONS_FILE: &str = "AGENTS.md";
        const ORIGINAL_INSTRUCTIONS: &str = "Keep the original isolated control instructions.";
        const CHANGED_INSTRUCTIONS: &str = "Adopt the reloaded isolated control instructions.";
        const ORIGINAL_SLOT: &str = "Original isolated control conventions.";
        const CHANGED_SLOT: &str = "Reloaded isolated control conventions.";
        const SLOT_PLUGIN: &str = "headless-isolated-context";
        const QUESTION: &str = "Which instructions apply?";
        smol::block_on(async {
            let directory = tempfile::tempdir().unwrap();
            let instructions = directory.path().join(INSTRUCTIONS_FILE);
            smol::fs::write(&instructions, ORIGINAL_INSTRUCTIONS)
                .await
                .unwrap();
            let session_id = MakimaId::generate();
            let coordinator = coordinator(session_id, ClaimingWriter::accept_all());
            let (provider, requests) = recording();
            let slot_content = Arc::new(StdMutex::new(ORIGINAL_SLOT.to_string()));
            let mut params = interactive_params(session_id, Vec::new());
            params.initial_wd = directory.path().to_path_buf();
            params.project_config = ProjectConfig::for_project(directory.path());
            params.system_prompt_override = None;
            params.prompt_slots = PromptSlotSource::new({
                let slot_content = Arc::clone(&slot_content);
                move || {
                    let (sender, receiver) = flume::bounded(1);
                    let mut slots = ResolvedSlots::default();
                    slots.insert(
                        PromptId::System,
                        Slot::Conventions,
                        SlotEntry {
                            plugin: Arc::from(SLOT_PLUGIN),
                            name: "default".into(),
                            content: slot_content.lock().unwrap().clone(),
                        },
                    );
                    sender.send(slots).unwrap();
                    receiver
                }
            });
            let binding = coordinator_binding(coordinator.clone(), Arc::clone(&provider));
            let handle = spawn_interactive_with_binding(
                params,
                Box::new(move |model| {
                    Box::pin(async move { Ok(PreparedModel { model, provider }) })
                }),
                None,
                Some(binding),
            );
            let root = bounded(handle.root.get()).await.unwrap();
            let actor = root.actor().unwrap();
            bounded(coordinator.ensure_root_context_for_actor(&actor))
                .await
                .unwrap();
            smol::fs::write(&instructions, CHANGED_INSTRUCTIONS)
                .await
                .unwrap();
            *slot_content.lock().unwrap() = CHANGED_SLOT.into();
            for reloaded in [false, true] {
                if reloaded {
                    bounded(coordinator.reload_context()).await.unwrap();
                }
                let (_trigger, cancel) = CancelToken::new();
                bounded(collect_answer(
                    root.ask_isolated(QUESTION.into(), Vec::new(), cancel)
                        .unwrap(),
                ))
                .await
                .unwrap();
                let (_, _, system, _, _) = bounded(requests.recv_async()).await.unwrap();
                let (expected_instructions, rejected_instructions, expected_slot, rejected_slot) =
                    if reloaded {
                        (
                            CHANGED_INSTRUCTIONS,
                            ORIGINAL_INSTRUCTIONS,
                            CHANGED_SLOT,
                            ORIGINAL_SLOT,
                        )
                    } else {
                        (
                            ORIGINAL_INSTRUCTIONS,
                            CHANGED_INSTRUCTIONS,
                            ORIGINAL_SLOT,
                            CHANGED_SLOT,
                        )
                    };
                assert!(system.contains(expected_instructions));
                assert!(!system.contains(rejected_instructions));
                assert!(system.contains(expected_slot));
                assert!(!system.contains(rejected_slot));
            }
            assert!(actor.latest_ticket().is_none());
            assert!(actor.committed_history().is_empty());
            assert!(requests.is_empty());
            bounded(handle.task.cancel()).await;
            bounded(coordinator.close()).await.unwrap();
        });
    }

    #[test]
    fn isolated_control_initializes_nonpersistent_context_without_coordinator() {
        const INSTRUCTIONS_FILE: &str = "AGENTS.md";
        const INSTRUCTIONS: &str =
            "Record standalone isolated control instructions before execution.";
        const QUESTION: &str = "Which instructions apply in this standalone session?";
        smol::block_on(async {
            let directory = tempfile::tempdir().unwrap();
            smol::fs::write(directory.path().join(INSTRUCTIONS_FILE), INSTRUCTIONS)
                .await
                .unwrap();
            let (provider, requests) = recording();
            let mut params = interactive_params(MakimaId::generate(), Vec::new());
            params.initial_wd = directory.path().to_path_buf();
            params.project_config = ProjectConfig::for_project(directory.path());
            params.system_prompt_override = None;
            let handle = spawn_nonpersistent_with_binding(
                params,
                Box::new(move |model| {
                    Box::pin(async move { Ok(PreparedModel { model, provider }) })
                }),
                None,
            );
            let root = bounded(handle.root.get()).await.unwrap();
            let actor = root.actor().unwrap();
            assert!(!root.manager().journal().is_persistent());
            assert!(root.manager().journal().session_id().is_none());
            assert!(root_context(&root.state_access()).unwrap().is_none());
            let (_trigger, cancel) = CancelToken::new();
            bounded(collect_answer(
                root.ask_isolated(QUESTION.into(), Vec::new(), cancel)
                    .unwrap(),
            ))
            .await
            .unwrap();
            let (_, _, system, _, _) = bounded(requests.recv_async()).await.unwrap();
            assert!(system.contains(INSTRUCTIONS));
            assert!(
                root_context(&root.state_access())
                    .unwrap()
                    .unwrap()
                    .instructions
                    .text
                    .contains(INSTRUCTIONS)
            );
            assert!(actor.latest_ticket().is_none());
            assert!(actor.committed_history().is_empty());
            bounded(handle.task.cancel()).await;
        });
    }

    #[test]
    fn isolated_question_completes_during_primary_turn() {
        smol::block_on(async {
            let (entered, received) = flume::bounded(1);
            let (release, released) = flume::bounded(1);
            let provider = Arc::new(GatedHeadlessProvider {
                entered,
                release: released,
                exit: ProviderExit::Success,
            });
            let (handle, coordinator) = fixture(Vec::new(), checkpoint(), provider);
            let primary = handle.input_tx.admit(first_input()).await.unwrap();
            bounded(received.recv_async()).await.unwrap();
            let root = bounded(handle.root.get()).await.unwrap();
            let actor = root.actor().unwrap();
            let history = actor.committed_history();
            let events = root
                .ask_isolated(FIRST.into(), Vec::new(), CancelToken::none())
                .unwrap();
            assert_eq!(bounded(collect_answer(events)).await.unwrap(), SAVED_OUTPUT);
            assert!(primary.peek().is_none());
            assert_eq!(actor.committed_history(), history);
            release.send_async(()).await.unwrap();
            bounded(primary.wait()).await;
            bounded(handle.task.cancel()).await;
            bounded(coordinator.close()).await.unwrap();
        });
    }

    #[test]
    fn isolated_control_cancellation_precedes_context_initialization() {
        smol::block_on(async {
            let (provider, requests) = recording();
            let (handle, coordinator) = fixture(Vec::new(), checkpoint(), provider);
            let root = bounded(handle.root.get()).await.unwrap();
            let actor = root.actor().unwrap();
            let (trigger, cancel) = CancelToken::new();
            trigger.cancel();
            let result = bounded(collect_answer(
                root.ask_isolated(FIRST.into(), Vec::new(), cancel).unwrap(),
            ))
            .await;
            assert_eq!(result, Err(OPERATION_CANCELLED.into()));
            assert!(requests.is_empty());
            assert!(actor.latest_ticket().is_none());
            assert!(root_context(&root.state_access()).unwrap().is_none());
            bounded(handle.task.cancel()).await;
            bounded(coordinator.close()).await.unwrap();
        });
    }

    fn first_input() -> AgentInput {
        AgentInput::new("hello".into(), Vec::new())
    }

    /// Spawns a headless session on {provider} and admits its first turn.
    fn spawn_started(
        params: InteractiveParams,
        provider: Result<Arc<dyn Provider>, crate::AgentError>,
    ) -> InteractiveHandle {
        let handle = spawn_interactive_with_binding(
            params,
            Box::new(move |model| {
                Box::pin(async move { provider.map(|provider| PreparedModel { model, provider }) })
            }),
            None,
            None,
        );
        let _ = smol::block_on(handle.input_tx.admit(first_input()));
        handle
    }

    fn interactive_params(session_id: MakimaId, history: Vec<Message>) -> InteractiveParams {
        InteractiveParams {
            model: Model::from_spec(TEST_MODEL).unwrap(),
            providers: Arc::default(),
            config: AgentConfig::default(),
            permissions_config: PermissionsConfig::default(),
            timeouts: Timeouts::default(),
            prompt_slots: PromptSlotSource::default(),
            profile: FrontendProfile::default(),
            mcp_handle: None,
            initial_wd: PathBuf::from(TEST_CWD.as_str()),
            session_id: Some(SessionRef::from(session_id)),
            root_id: None,
            journal: None,
            initial_history: history,
            yolo: false,
            system_prompt_override: Some("admitted system".into()),
            append_system_prompt: None,
            defaults: Default::default(),
            model_policy: Arc::default(),
            modes: Arc::new(ModeRegistry::builtin()),
            plugin_rules: Arc::default(),
            project_config: ProjectConfig::for_project(Path::new(TEST_CWD.as_str())),
            local_tools: Arc::default(),
            state_dir: None,
            answers: true,
        }
    }

    pub(crate) fn command_fixture(
        model: Model,
        coordinator: SessionCoordinatorHandle,
    ) -> InteractiveHandle {
        let (provider, _) = recording();
        let mut params = interactive_params(coordinator.read().session_id(), Vec::new());
        params.model = model;
        let binding = coordinator_binding(coordinator, Arc::clone(&provider));
        spawn_interactive_with_binding(
            params,
            Box::new(move |model| Box::pin(async move { Ok(PreparedModel { model, provider }) })),
            None,
            Some(binding),
        )
    }

    fn coordinator_binding(
        coordinator: SessionCoordinatorHandle,
        provider: Arc<dyn Provider>,
    ) -> RootBinding {
        Box::new(move |_, root, template, initialization| {
            Box::pin(async move {
                coordinator
                    .bind_permissions(Arc::clone(&template.permissions))
                    .map_err(|error| error.to_string())?;
                let prepare: ModelPreparer = Arc::new(move |spec| {
                    let provider = Arc::clone(&provider);
                    let model = Model::from_spec(&spec).unwrap();
                    Box::pin(async move { Ok(PreparedModel { provider, model }) })
                });
                match initialization {
                    Some(initialization) => bind_initializing_actor(
                        &coordinator,
                        initialization,
                        prepare,
                        Arc::new(|_| {}),
                    ),
                    None => coordinator.bind_actor(root, prepare, Arc::new(|_| {})),
                }
                .map_err(|error| error.to_string())
            })
        })
    }

    fn checkpoint() -> Checkpoint {
        accept_all()
    }

    fn coordinator(session_id: MakimaId, checkpoint: Checkpoint) -> SessionCoordinatorHandle {
        SessionCoordinatorHandle::register(SessionCoordinatorParams {
            session_id,
            catalog: Default::default(),
            definitions: builtin_option_definitions(
                Arc::from(TEST_MODEL),
                [Arc::from(TEST_MODEL)],
                false,
                false,
                false,
                Default::default(),
            ),
            persisted_options: Default::default(),
            model_policy: Arc::default(),
            checkpoint,
            mailbox: SessionMailbox::new(session_id),
        })
        .unwrap()
    }

    fn fixture(
        history: Vec<Message>,
        checkpoint: Checkpoint,
        provider: Arc<dyn Provider>,
    ) -> (InteractiveHandle, SessionCoordinatorHandle) {
        let session_id = MakimaId::generate();
        let coordinator = coordinator(session_id, checkpoint);
        let binding = coordinator_binding(coordinator.clone(), Arc::clone(&provider));
        let handle = spawn_interactive_with_binding(
            interactive_params(session_id, history),
            Box::new(move |model| Box::pin(async move { Ok(PreparedModel { model, provider }) })),
            None,
            Some(binding),
        );
        (handle, coordinator)
    }

    async fn bounded<F: Future>(future: F) -> F::Output {
        future::or(future, async {
            smol::Timer::after(GATE_TIMEOUT).await;
            panic!("headless operation did not settle");
        })
        .await
    }

    async fn bounded_terminal<F: Future>(
        future: F,
        phase: &str,
        handle: &InteractiveHandle,
        ticket: &TurnTicket,
    ) -> F::Output {
        future::or(future, async {
            smol::Timer::after(GATE_TIMEOUT).await;
            panic!(
                "{phase} did not settle; graph={:?}; ticket={:?}; sealed={:?}; journal_error={:?}",
                handle.manager.snapshot(),
                ticket.peek(),
                ticket.peek_result(),
                handle.manager.journal().persistence_error()
            );
        })
        .await
    }

    fn recording() -> (Arc<dyn Provider>, Receiver<RecordedRequest>) {
        let (requests, received) = flume::unbounded();
        (
            Arc::new(RecordingProvider {
                name: FIRST,
                requests,
            }),
            received,
        )
    }

    fn tui_backend(
        profile: FrontendProfile,
        options: HeadlessBackendOptions,
    ) -> (
        HeadlessBackend,
        EffectiveAgentConfig,
        Receiver<Envelope>,
        Receiver<RecordedRequest>,
    ) {
        let cwd = PathBuf::from(ADMITTED_CWD);
        let (provider, requests) = recording();
        let model = Model::from_spec(TEST_MODEL).unwrap();
        let (events, received) = flume::unbounded();
        let params = AgentParams {
            agent_id: AgentId::generate(),
            provider: Arc::clone(&provider),
            model: model.clone(),
            config: AgentConfig::default(),
            tool_output_lines: ToolOutputLines::default(),
            permissions: Arc::new(PermissionManager::new(
                PermissionsConfig::default(),
                cwd.clone(),
                ProjectConfig::for_project(&cwd),
                Arc::default(),
            )),
            session_id: None,
            mailbox: None,
            timeouts: Timeouts::default(),
            file_tracker: FileReadTracker::fresh(),
            prompt_slots: Arc::default(),
            root_context: None,
            modes: Arc::new(ModeRegistry::builtin()),
            subagent_cancels: Arc::default(),
            registry: Arc::clone(ToolRegistry::global_arc()),
            audience: ToolAudience::MAIN,
            question_mode: QuestionMode::Headless,
            interactive_reauth: false,
            model_policy: Arc::default(),
            providers: Arc::default(),
            file_write_locks: Arc::new(FileWriteLocks::new()),
            offload: None,
            managed_turn: None,
        };
        let config = EffectiveAgentConfig::new(
            RunSettings {
                provider,
                model,
                fast: false,
                workflow: false,
                thinking: Default::default(),
            },
            AgentMode::Build,
        )
        .with_context(AgentContextSnapshot::new(cwd));
        (
            HeadlessBackend::new(params, profile, events, options),
            config,
            received,
            requests,
        )
    }

    fn turn_context(
        backend: &HeadlessBackend,
        policy: Option<EffectiveAgentConfig>,
    ) -> TurnContext {
        let agent_id = backend.params.agent_id;
        let turn_id = TurnId::generate();
        TurnContext {
            agent_id,
            turn_id: Some(turn_id),
            work: ActorWorkIdentity::new(
                agent_id,
                ActorWorkKind::Turn {
                    operation_id: PRESENTATION,
                    turn_id,
                },
                PRESENTATION,
            ),
            output: Default::default(),
            provenance: Default::default(),
            cancel: CancelToken::none(),
            cancel_reason: ReasonedCancelToken::none(),
            generation: 0,
            policy: policy.map(Arc::new),
            interrupt: None,
            inbox: None,
            managed_turn: None,
            admission: None,
        }
    }

    #[test_case(false, true; "missing_policy")]
    #[test_case(true, true; "missing_prompt_inputs")]
    #[test_case(true, false; "headless_setup_failure")]
    fn a_setup_failure_reaches_the_user(has_policy: bool, tui: bool) {
        smol::block_on(async {
            let profile = if tui {
                FrontendProfile::tui(ToolOutputLines::default(), CancelToken::none())
            } else {
                FrontendProfile::default()
            };
            let (mut backend, config, events, requests) = tui_backend(profile, Default::default());
            let context = turn_context(&backend, has_policy.then_some(config));
            let mut history = History::new(Vec::new());
            let result = backend
                .run_turn(
                    ActorHistory::new(&mut history),
                    context,
                    first_input(),
                    WorkKind::Turn,
                )
                .await;
            let expected = if has_policy {
                NO_ADMITTED_CWD_ERR
            } else {
                NO_TURN_CONFIG
            };
            let failure = match result {
                BackendResult::EnteredRun(TurnOutcome::Failed { failure, .. }) if tui => failure,
                BackendResult::SetupFailed(Some(failure)) if !tui => failure,
                result => panic!("unexpected setup result: {result:?}"),
            };
            assert_eq!(failure.user_message, expected);
            let mut emitted = events.drain();
            if has_policy {
                let model = emitted.next().unwrap();
                assert_eq!(model.presentation(), Some(PRESENTATION));
                assert!(
                    matches!(model.event, AgentEvent::ModelSwitched { spec, .. } if spec == TEST_MODEL)
                );
            }
            if tui {
                let error = emitted.next().unwrap();
                assert_eq!(error.presentation(), Some(PRESENTATION));
                assert!(
                    matches!(error.event, AgentEvent::ControlError { message } if message == format!("{SETUP_FAILED}: {expected}"))
                );
            }
            assert!(emitted.next().is_none());
            assert!(requests.is_empty());
            assert!(history.is_empty());
        });
    }

    #[test_case(TurnCancellationReason::User; "user")]
    #[test_case(TurnCancellationReason::Closed; "closed")]
    #[test_case(TurnCancellationReason::Shutdown; "shutdown")]
    fn cancelled_prompt_setup_releases_backend(reason: TurnCancellationReason) {
        smol::block_on(async {
            let profile = FrontendProfile::tui(ToolOutputLines::default(), CancelToken::none());
            let (mut backend, config, events, requests) = tui_backend(profile, Default::default());
            let input = first_input();
            let mut snapshot = backend.setup.snapshot(&input, Some(&config));
            let (pending_prompt, receiver) = flume::bounded(1);
            Arc::make_mut(snapshot.prompt_inputs.as_mut().unwrap()).resolved = Some(receiver);
            let mut context = turn_context(&backend, Some(config.clone()));
            let (trigger, cancel) = CancelToken::new();
            let (reason_trigger, cancel_reason) = ReasonedCancelToken::new();
            context.cancel = cancel;
            context.cancel_reason = cancel_reason;
            context.admission = Some(snapshot);
            let expected = TurnOutcome::cancelled(
                context.agent_id,
                context.turn_id.unwrap(),
                Default::default(),
                0,
                reason,
            );
            let mut history = History::new(Vec::new());
            {
                let mut run = backend.run_turn(
                    ActorHistory::new(&mut history),
                    context,
                    input,
                    WorkKind::Turn,
                );
                assert!(future::poll_once(&mut run).await.is_none());
                assert!(matches!(
                    events.try_recv().unwrap().event,
                    AgentEvent::ModelSwitched { .. }
                ));
                reason_trigger.cancel(reason);
                trigger.cancel();
                assert!(
                    matches!(bounded(&mut run).await, BackendResult::EnteredRun(outcome) if outcome == expected)
                );
            }
            assert!(events.is_empty());
            assert!(pending_prompt.is_disconnected());
            assert!(requests.is_empty());
            let successor = first_input();
            let mut context = turn_context(&backend, Some(config.clone()));
            context.admission = Some(backend.setup.snapshot(&successor, Some(&config)));
            assert!(matches!(
                bounded(backend.run_turn(
                    ActorHistory::new(&mut history),
                    context,
                    successor,
                    WorkKind::Turn
                ))
                .await,
                BackendResult::EnteredRun(TurnOutcome::Completed { .. })
            ));
            assert_eq!(requests.len(), 1);
        });
    }

    #[test_case(false; "cancelled_initialization")]
    #[test_case(true; "cancelled_admission")]
    fn submission_setup_respects_admission_cancellation(cancelled: bool) {
        smol::block_on(async {
            let (init_trigger, init_cancel) = CancelToken::new();
            let profile = FrontendProfile::tui(ToolOutputLines::default(), init_cancel);
            let (mcp, _publish, _) = gated_session(&[]);
            let options = HeadlessBackendOptions {
                mcp: Some(mcp),
                ..Default::default()
            };
            let (mut backend, config, events, requests) = tui_backend(profile, options);
            let mut context = turn_context(&backend, Some(config));
            let (trigger, cancel) = CancelToken::new();
            context.cancel = cancel;
            if cancelled {
                trigger.cancel();
            }
            let mut history = History::new(Vec::new());
            let mut result = backend.run_turn(
                ActorHistory::new(&mut history),
                context,
                first_input(),
                WorkKind::Turn,
            );
            if cancelled {
                assert!(matches!(
                    result.await,
                    BackendResult::EnteredRun(TurnOutcome::Cancelled {
                        reason: TurnCancellationReason::User,
                        ..
                    })
                ));
            } else {
                assert!(future::poll_once(&mut result).await.is_none());
                init_trigger.cancel();
                assert!(
                    matches!(result.await, BackendResult::EnteredRun(TurnOutcome::Failed { failure, .. }) if failure.user_message == MCP_INIT_CANCELLED)
                );
                assert!(matches!(
                    events.try_recv().unwrap().event,
                    AgentEvent::ModelSwitched { .. }
                ));
                assert!(
                    matches!(events.try_recv().unwrap().event, AgentEvent::ControlError { message } if message == format!("{SETUP_FAILED}: {MCP_INIT_CANCELLED}"))
                );
            }
            assert!(events.is_empty());
            assert!(requests.is_empty());
        });
    }

    #[test]
    fn admitted_prompt_inputs_survive_cwd_change() {
        smol::block_on(async {
            let lines = ToolOutputLines {
                bash: OUTPUT_LINES,
                ..Default::default()
            };
            let mut profile = FrontendProfile::tui(lines, CancelToken::none());
            profile.excluded_tools.push(QUESTION_TOOL_NAME);
            let mut slots = ResolvedSlots::default();
            slots.insert(
                PromptId::System,
                Slot::Identity,
                SlotEntry {
                    plugin: "test".into(),
                    name: "default".into(),
                    content: ADMITTED_IDENTITY.into(),
                },
            );
            let options = HeadlessBackendOptions {
                prompt_slots: PromptSlotSource::new(move || {
                    let (tx, rx) = flume::bounded(1);
                    tx.send(slots.clone()).unwrap();
                    rx
                }),
                ..Default::default()
            };
            let (mut backend, config, _events, requests) = tui_backend(profile, options);
            let template = backend.template(&config);
            assert_eq!(template.cwd, Path::new(ADMITTED_CWD));
            assert_eq!(template.question_mode, QuestionMode::Tui);
            assert_eq!(template.tool_output_lines, lines);
            assert!(template.interactive_reauth);
            assert!(backend.hold_input_after_failure());
            assert!(Arc::ptr_eq(
                &template.file_write_locks,
                &backend.params.file_write_locks
            ));
            assert!(!template.tool_filter.matches(QUESTION_TOOL_NAME));
            let input = first_input();
            let mut snapshot = backend.setup.snapshot(&input, Some(&config));
            assert_eq!(
                backend
                    .template(
                        &config
                            .clone()
                            .with_context(AgentContextSnapshot::new(PathBuf::from(CHANGED_CWD)))
                    )
                    .cwd,
                Path::new(CHANGED_CWD)
            );
            Arc::make_mut(snapshot.prompt_inputs.as_mut().unwrap())
                .instructions
                .text = ADMITTED_INSTRUCTIONS.into();
            let mut context = turn_context(&backend, Some(config));
            context.admission = Some(
                backend.prepared_readiness().unwrap()(snapshot)
                    .await
                    .unwrap(),
            );
            let mut history = History::new(Vec::new());
            assert!(matches!(
                bounded(backend.run_turn(
                    ActorHistory::new(&mut history),
                    context,
                    input,
                    WorkKind::Turn
                ))
                .await,
                BackendResult::EnteredRun(TurnOutcome::Completed { .. })
            ));
            let (_, _, system, _, _) = requests.try_recv().unwrap();
            for expected in [ADMITTED_CWD, ADMITTED_INSTRUCTIONS, ADMITTED_IDENTITY] {
                assert!(system.contains(expected));
            }
            assert!(!system.contains(CHANGED_CWD));
        });
    }

    #[test_case(false; "owned_shutdown")]
    #[test_case(true; "shared_waits_and_leaves_mcp_running")]
    fn session_mcp_lifecycle(shared: bool) {
        smol::block_on(async {
            let (mcp, publish, commands) = gated_session(&[(MCP_TOOL, MCP_TOOL)]);
            let (provider, requests) = recording();
            let mut params = interactive_params(MakimaId::generate(), Vec::new());
            params.mcp_handle = Some(McpHandle::clone(&mcp));
            if shared {
                params.profile =
                    FrontendProfile::tui(ToolOutputLines::default(), CancelToken::none());
            }
            let handle = spawn_nonpersistent_with_binding(
                params,
                Box::new(move |model| {
                    Box::pin(async move { Ok(PreparedModel { model, provider }) })
                }),
                None,
            );
            let ticket = bounded(handle.input_tx.admit(first_input())).await.unwrap();
            if shared {
                loop {
                    if matches!(
                        bounded(handle.event_rx.recv_async()).await.unwrap().event,
                        AgentEvent::ModelSwitched { .. }
                    ) {
                        break;
                    }
                }
                assert!(requests.is_empty());
            }
            if shared {
                publish();
            }
            assert!(matches!(
                bounded(ticket.wait()).await,
                TurnOutcome::Completed { .. }
            ));
            let (_, _, _, _, tools) = requests.try_recv().unwrap();
            assert_eq!(tools.iter().any(|tool| tool == MCP_WIRE_TOOL), shared);
            let InteractiveHandle { input_tx, task, .. } = handle;
            drop(input_tx);
            if !shared {
                let McpCommand::Shutdown { ack } = bounded(commands.recv_async()).await.unwrap()
                else {
                    panic!("expected MCP shutdown");
                };
                ack.send(()).unwrap();
            }
            bounded(task).await;
            assert!(commands.is_empty());
        });
    }

    #[test_case(false; "fallback_checkpoint_precedes_admission")]
    #[test_case(true; "binding_failure_leaves_saved_config_unchanged")]
    fn restored_plan_fallback_publication(reject_binding: bool) {
        use crate::session_recovery::{OpenModel, SessionSource, open_session};

        const STARTUP_MODEL: &str = "openai/gpt-5";
        const BINDING_ERROR: &str = "root binding failed";

        smol::block_on(async {
            let directory = tempfile::tempdir().unwrap();
            let storage = StateDir::from_path(directory.path().to_path_buf());
            let blocker = directory.path().join(SAVE_ERROR);
            std::fs::write(&blocker, SAVE_ERROR).unwrap();
            let plan_path = blocker.join(SAVED_OUTPUT);
            let mut definition = crate::ModeDef::default_for(crate::ModeId::Plan);
            definition.restrict_write_to = Some(plan_path.clone());
            let cwd = directory.path().to_string_lossy();
            let mut session = Session::<Message, TokenUsage, ToolOutput>::new(TEST_MODEL, &cwd);
            let root_id = AgentId::generate();
            session.bind_root_id(root_id).unwrap();
            session
                .resolve_legacy_config(makima_storage::session_state::StoredAgentConfig {
                    model: TEST_MODEL.into(),
                    cwd: cwd.into_owned(),
                    mode: definition.stored_snapshot(&AgentMode::Plan(plan_path)),
                    thinking: None,
                    fast: false,
                    workflow: false,
                    options: Default::default(),
                })
                .unwrap();
            session.save(&storage).unwrap();
            let id = session.id;
            let mut params = interactive_params(id, Vec::new());
            params.model = Model::from_spec(STARTUP_MODEL).unwrap();
            let opening = open_session(
                &storage,
                SessionSource::Resume(id),
                OpenModel {
                    startup: &params.model,
                    explicit: false,
                    providers: &params.providers,
                    policy: &params.model_policy,
                },
                None,
            )
            .unwrap();
            assert!(opening.warning.is_some());
            assert_eq!(opening.model.spec(), STARTUP_MODEL);
            params.model = opening.model.clone();
            let (checkpoint, _lock, recovered) =
                opening.recover(&storage, &params.modes).await.unwrap();
            let recovered_config = recovered.session().latest_state().unwrap().scopes
                [&Some(root_id)]
                .config
                .clone()
                .unwrap();
            params.root_id = Some(root_id);
            params.journal = Some(recovered.journal());
            let (provider, requests) = recording();
            let (entered, entering) = flume::bounded(1);
            let (release, released) = flume::bounded(1);
            let expected_config = recovered_config.clone();
            let handle = spawn_interactive_with_checkpoint(
                params,
                Box::new(move |model| {
                    Box::pin(async move { Ok(PreparedModel { model, provider }) })
                }),
                Some(recovered.session().history_snapshot()),
                Some(Box::new(move |manager, root, _, initialization| {
                    Box::pin(async move {
                        assert!(root.actor().is_err());
                        let config = initialization.unwrap().actor.config_snapshot().unwrap();
                        assert!(crate::modes::is_read_only_fallback(&config.config.mode));
                        assert_eq!(config.config.model.spec(), STARTUP_MODEL);
                        let view = manager.journal().snapshot().fold().unwrap();
                        let stored = view.scopes[&Some(root_id)].config.as_ref().unwrap();
                        assert_eq!(stored.model, expected_config.model);
                        assert_eq!(stored.mode.id, expected_config.mode.id);
                        entered.send_async(()).await.unwrap();
                        released.recv_async().await.unwrap();
                        if reject_binding {
                            return Err(BINDING_ERROR.into());
                        }
                        Ok(())
                    })
                })),
                checkpoint,
            );
            bounded(entering.recv_async()).await.unwrap();
            assert!(handle.root.current().is_none());
            let mut admission = Box::pin(handle.input_tx.admit(first_input()));
            assert!(future::poll_once(admission.as_mut()).await.is_none());
            drop(admission);
            release.send(()).unwrap();
            let initialized = bounded(handle.initialized_root()).await;
            let saved = Session::<Message, TokenUsage, ToolOutput>::load(id, &storage).unwrap();
            let view = saved.latest_state().unwrap();
            let config = view.scopes[&Some(root_id)].config.as_ref().unwrap();
            if reject_binding {
                assert!(initialized.unwrap_err().contains(BINDING_ERROR));
                assert_eq!(config.mode.id, recovered_config.mode.id);
                assert_eq!(config.model, TEST_MODEL);
            } else {
                let effective = initialized.unwrap().effective_config().unwrap().unwrap();
                assert!(crate::modes::is_read_only_fallback(&effective.mode));
                assert_eq!(effective.model.spec(), STARTUP_MODEL);
                assert_eq!(config.mode.id, effective.mode.id().key());
                assert_eq!(config.model, STARTUP_MODEL);
            }
            assert!(requests.is_empty());
            bounded(handle.task.cancel()).await;
        });
    }

    #[test_case(false, None; "fresh_invocation_disabled")]
    #[test_case(true, None; "invocation_yolo_is_not_stored")]
    #[test_case(false, Some(true); "stored_yolo_enabled")]
    #[test_case(true, Some(false); "stored_disable_overrides_invocation")]
    fn permissions_restore_and_bind_before_root_ready(seed_yolo: bool, stored_yolo: Option<bool>) {
        const RULE_TOOL: &str = "bash";
        const RULE_SCOPE: &str = "blocked *";
        smol::block_on(async {
            let root_id = AgentId::generate();
            let mut session =
                Session::<Message, TokenUsage, ToolOutput>::new(TEST_MODEL, &TEST_CWD);
            session.bind_root_id(root_id).unwrap();
            let rules = vec![makima_storage::sessions::StoredRule {
                tool: RULE_TOOL.into(),
                scope: Some(RULE_SCOPE.into()),
                effect: makima_storage::sessions::StoredEffect::Deny,
            }];
            let seq = session
                .state()
                .iter()
                .map(|record| record.seq)
                .chain(session.agent_revisions().iter().map(|(seq, _)| *seq))
                .max()
                .unwrap_or_default()
                + 1;
            session.push_state(makima_storage::session_state::StoredStateRecord {
                seq,
                agent: None,
                generation: 0,
                change: StateChange::Rules {
                    rules: rules.clone(),
                    yolo: stored_yolo,
                },
            });
            let mut params = interactive_params(session.id, Vec::new());
            params.root_id = Some(root_id);
            params.yolo = seed_yolo;
            params.journal = Some(Arc::new(SessionJournal::from_session(&session).unwrap()));
            let (provider, requests) = recording();
            let expected_rules = rules.clone();
            let handle = spawn_interactive_with_binding(
                params,
                Box::new(move |model| {
                    Box::pin(async move { Ok(PreparedModel { model, provider }) })
                }),
                Some(session.history_snapshot()),
                Some(Box::new(move |_, root, template, initialization| {
                    Box::pin(async move {
                        assert!(root.actor().is_err());
                        assert!(initialization.is_some());
                        assert_eq!(
                            template.permissions.snapshot().stored_rules(),
                            expected_rules
                        );
                        assert_eq!(template.permissions.persisted_yolo(), stored_yolo);
                        assert_eq!(
                            template.permissions.is_yolo(),
                            stored_yolo.unwrap_or(seed_yolo)
                        );
                        Ok(())
                    })
                })),
            );
            bounded(handle.initialized_root()).await.unwrap();
            assert_eq!(handle.permissions.persisted_yolo(), stored_yolo);
            let before = handle.manager.journal().snapshot().state;
            handle
                .permissions
                .try_set_yolo(!stored_yolo.unwrap_or(seed_yolo))
                .unwrap();
            let snapshot = handle.manager.journal().snapshot();
            assert_eq!(snapshot.state.iter().len(), before.iter().len() + 1);
            let view = snapshot.fold().unwrap();
            assert_eq!(view.scopes[&None].rules, rules);
            assert_eq!(
                view.scopes[&None].yolo,
                Some(!stored_yolo.unwrap_or(seed_yolo))
            );
            assert!(requests.is_empty());
            let permissions = Arc::clone(&handle.permissions);
            let incarnation = handle.manager.incarnation(root_id).unwrap();
            handle.manager.detach_incarnation(&incarnation).unwrap();
            assert!(permissions.try_set_yolo(seed_yolo).is_err());
            bounded(handle.task.cancel()).await;
        });
    }

    #[test_case(false; "inherited_tools")]
    #[test_case(true; "empty_tools")]
    fn folded_config_is_installed_before_recovered_root_binding(empty_tools: bool) {
        const RESTORED_MODEL: &str = "openai/gpt-5";
        const MODE_NAME: &str = "removed_mode";
        smol::block_on(async {
            let directory = tempfile::tempdir().unwrap();
            let cwd = directory.path().to_path_buf();
            let root_id = AgentId::generate();
            let mut session =
                Session::<Message, TokenUsage, ToolOutput>::new(TEST_MODEL, &TEST_CWD);
            session.bind_root_id(root_id).unwrap();
            let definition = crate::ModeDef {
                id: crate::ModeId::parse(MODE_NAME),
                label: MODE_NAME.into(),
                system_prompt: Some(SAVED_OUTPUT.into()),
                restrict_write_to: Some(cwd.join(MODE_NAME)),
                tools: empty_tools.then(Vec::new),
            };
            let mode = AgentMode::Custom(definition.id.clone());
            let stored = makima_storage::session_state::StoredAgentConfig {
                model: RESTORED_MODEL.into(),
                cwd: cwd.to_string_lossy().into_owned(),
                mode: definition.stored_snapshot(&mode),
                thinking: Some(makima_providers::ThinkingConfig::Off.into()),
                fast: false,
                workflow: true,
                options: Default::default(),
            };
            session.resolve_legacy_config(stored.clone()).unwrap();
            let mut params = interactive_params(session.id, Vec::new());
            params.root_id = Some(root_id);
            params.providers = Arc::new(makima_providers::test_util::offline_ids(&["openai"]));
            params.journal = Some(Arc::new(SessionJournal::from_session(&session).unwrap()));
            let (provider, requests) = recording();
            let (prepared, preparations) = flume::bounded(1);
            let handle = spawn_interactive_with_binding(
                params,
                Box::new(move |model| {
                    assert_eq!(model.spec(), RESTORED_MODEL);
                    prepared.send(()).unwrap();
                    Box::pin(async move { Ok(PreparedModel { model, provider }) })
                }),
                Some(session.history_snapshot()),
                Some(Box::new(move |_, root, template, initialization| {
                    Box::pin(async move {
                        assert!(root.actor().is_err());
                        let config = initialization.unwrap().actor.config_snapshot().unwrap();
                        assert_eq!(config.config.model.spec(), stored.model);
                        assert_eq!(config.config.context.cwd(), Path::new(&stored.cwd));
                        assert_eq!(config.config.mode, mode);
                        assert_eq!(config.config.mode_def.as_ref(), Some(&definition));
                        assert!(config.config.workflow);
                        assert_eq!(template.cwd, PathBuf::from(stored.cwd));
                        assert_eq!(template.mode_def.as_deref(), Some(&definition));
                        Ok(())
                    })
                })),
            );
            let root = bounded(handle.initialized_root()).await.unwrap();
            assert_eq!(root.id(), root_id);
            assert!(preparations.try_recv().is_ok());
            assert!(requests.is_empty());
            bounded(handle.task.cancel()).await;
        });
    }

    #[test_case(false; "provider_preparation")]
    #[test_case(true; "hidden_root_initializer")]
    fn cancelled_recovered_startup_settles_readiness_without_running_provider(initialize: bool) {
        smol::block_on(async {
            let mut session =
                Session::<Message, TokenUsage, ToolOutput>::new(TEST_MODEL, &TEST_CWD);
            let root_id = AgentId::generate();
            session.bind_root_id(root_id).unwrap();
            let mut params = interactive_params(session.id, Vec::new());
            params.root_id = Some(root_id);
            params.journal = Some(Arc::new(SessionJournal::from_session(&session).unwrap()));
            let (provider, requests) = recording();
            let (entered, entering) = flume::bounded(1);
            let (_release, released) = flume::bounded::<()>(1);
            let prepare_entered = entered.clone();
            let prepare_wait = released.clone();
            let binding: RootBinding = Box::new(move |manager, root, _, initialization| {
                Box::pin(async move {
                    assert_eq!(root.id(), root_id);
                    assert!(root.actor().is_err());
                    assert!(root.validate_live().is_err());
                    let initialization = initialization.unwrap();
                    manager
                        .validate_activation(&initialization.incarnation)
                        .unwrap();
                    if initialize {
                        entered.send_async(()).await.unwrap();
                        released.recv_async().await.unwrap();
                    }
                    Ok(())
                })
            });
            let handle = spawn_interactive_with_binding(
                params,
                Box::new(move |model| {
                    Box::pin(async move {
                        if !initialize {
                            prepare_entered.send_async(()).await.unwrap();
                            prepare_wait.recv_async().await.unwrap();
                        }
                        Ok(PreparedModel { model, provider })
                    })
                }),
                Some(session.history_snapshot()),
                Some(binding),
            );
            bounded(entering.recv_async()).await.unwrap();
            assert!(handle.root.current().is_none());
            assert!(handle.manager.lookup(root_id).unwrap().actor().is_err());
            let readiness = handle.root.clone();
            bounded(handle.task.cancel()).await;
            assert!(
                matches!(bounded(readiness.get()).await, Err(message) if message == INITIALIZATION_CLOSED)
            );
            assert!(requests.is_empty());
        });
    }

    #[test]
    fn rejected_recovery_publication_never_activates_root_or_prepares_provider() {
        smol::block_on(async {
            let directory = tempfile::tempdir().unwrap();
            let storage = StateDir::from_path(directory.path().to_path_buf());
            let mut session =
                Session::<Message, TokenUsage, ToolOutput>::new(TEST_MODEL, &TEST_CWD);
            let root_id = AgentId::generate();
            session.bind_root_id(root_id).unwrap();
            let mut params = interactive_params(session.id, Vec::new());
            params.root_id = Some(root_id);
            let writer = SessionWriter::open_session(
                &storage.ensure_subdir(SESSIONS_DIR).unwrap(),
                &session,
            )
            .unwrap();
            let claim = Arc::new(|_: &CheckpointPublication| Err(Arc::from(SAVE_ERROR)));
            let (prepared, preparations) = flume::unbounded();
            let startup = smol::spawn(async move {
                let recovered = writer
                    .recover(
                        session,
                        makima_storage::checkpoint::CheckpointPublication::default(),
                        claim,
                    )
                    .await?;
                params.journal = Some(recovered.journal());
                Ok::<_, CheckpointError>(spawn_interactive_with_binding(
                    params,
                    Box::new(move |model| {
                        prepared.send(()).unwrap();
                        Box::pin(async move {
                            Ok(PreparedModel {
                                model,
                                provider: Arc::new(crate::tools::NullProvider),
                            })
                        })
                    }),
                    Some(recovered.session().history_snapshot()),
                    None,
                ))
            });
            assert!(bounded(startup).await.is_err());
            assert!(preparations.is_empty());
        });
    }

    #[test_case(false; "continuation_disabled")]
    #[test_case(true; "continue_root")]
    fn recovered_startup_preserves_identity_and_resumes_work(continue_turns: bool) {
        smol::block_on(async {
            let directory = tempfile::tempdir().unwrap();
            let storage = StateDir::from_path(directory.path().to_path_buf());
            let root_id = AgentId::generate();
            let turn = TurnId::generate();
            let mut session = Session::<Message, makima_providers::TokenUsage, ToolOutput>::new(
                TEST_MODEL, &TEST_CWD,
            );
            session.bind_root_id(root_id).unwrap();
            session.push_message(Message::user(ORIGINAL_RECOVERY_INPUT.into()));
            session.push_state(makima_storage::session_state::StoredStateRecord {
                seq: 2,
                agent: Some(root_id),
                generation: 0,
                change: StateChange::Admitted {
                    turn,
                    origin: makima_storage::session_state::StoredTurnOrigin::User,
                },
            });
            let session_id = session.id;
            session.save(&storage).unwrap();
            let writer = SessionWriter::open_session(
                &storage.ensure_subdir(SESSIONS_DIR).unwrap(),
                &session,
            )
            .unwrap();
            let claim: makima_storage::checkpoint::CheckpointClaim = Arc::new(|publication| {
                assert!(publication.try_publish());
                Ok(())
            });
            let recovered = crate::session_recovery::RecoveryCandidate::prepare_with_continuation(
                session,
                continue_turns,
            )
            .unwrap()
            .publish(
                &writer,
                makima_storage::checkpoint::CheckpointPublication::default(),
                claim,
            )
            .await
            .unwrap();
            let mut params = interactive_params(session_id, Vec::new());
            params.root_id = Some(root_id);
            params.journal = Some(recovered.journal());
            let (provider, requests) = recording();
            let handle = spawn_interactive_with_binding(
                params,
                Box::new(move |model| {
                    Box::pin(async move { Ok(PreparedModel { model, provider }) })
                }),
                Some(recovered.session().history_snapshot()),
                None,
            );
            let root = handle.initialized_root().await.unwrap();
            assert_eq!(root.id(), root_id);
            let result = root.turn_ticket(turn).unwrap().wait_result().await;
            if continue_turns {
                assert!(matches!(result.outcome, TurnOutcome::Completed { .. }));
                let request = requests.try_recv().unwrap();
                assert_eq!(request.0, FIRST);
                assert_eq!(result.text, FIRST);
                let history = handle.manager.journal().snapshot().histories[&root_id].clone();
                assert_eq!(
                    history
                        .messages
                        .iter()
                        .filter(|message| message.role == Role::User)
                        .count(),
                    1
                );
                assert!(requests.try_recv().is_err());
            } else {
                assert!(matches!(
                    result.outcome,
                    crate::TurnOutcome::Cancelled {
                        reason: crate::TurnCancellationReason::Interrupted,
                        ..
                    }
                ));
                assert!(requests.try_recv().is_err());
                assert_eq!(
                    handle.manager.journal().snapshot().histories[&root_id]
                        .messages
                        .as_ref(),
                    recovered.session().messages()
                );
            }
            handle.task.cancel().await;
        });
    }

    #[test]
    fn oversized_results_survive_headless_restart_and_wake() {
        smol::block_on(async {
            let directory = tempfile::tempdir().unwrap();
            let storage = StateDir::from_path(directory.path().to_path_buf());
            let mut session =
                Session::<Message, TokenUsage, ToolOutput>::new(TEST_MODEL, &TEST_CWD);
            let session_id = session.id;
            let provider: Arc<dyn Provider> = Arc::new(OversizedPrintProvider);
            let coordinator = coordinator(session_id, checkpoint());
            let binding = coordinator_binding(coordinator.clone(), Arc::clone(&provider));
            let journal = Arc::new(SessionJournal::default());
            journal.bind_session(session_id).unwrap();
            let mut params = interactive_params(session_id, Vec::new());
            params.journal = Some(journal);
            params.state_dir = Some(directory.path().to_path_buf());
            let handle = spawn_interactive_with_binding(
                params,
                Box::new(move |model| {
                    Box::pin(async move { Ok(PreparedModel { model, provider }) })
                }),
                None,
                Some(binding),
            );
            let ticket = bounded(handle.input_tx.admit(first_input())).await.unwrap();
            let expected = bounded(ticket.wait_result()).await;
            assert!(matches!(expected.outcome, TurnOutcome::Completed { .. }));
            assert!(expected.text.len() > MAX_INLINE_TURN_RESULT_BYTES);
            let root_id = handle.manager.root_id().unwrap();
            session.bind_root_id(root_id).unwrap();
            let config = handle.manager.effective_config(root_id).unwrap().unwrap();
            let journal = handle.manager.journal();
            let snapshot = journal.checkpoint_snapshot().unwrap();
            let view = snapshot.fold().unwrap();
            let stored = &view.scopes[&Some(root_id)].results[0].1;
            assert!(matches!(stored.text, StoredPayload::Offloaded(_)));
            let child_id = AgentId::generate();
            let child_turn = TurnId::generate();
            let child_result = TurnResult {
                outcome: TurnOutcome::completed(
                    child_id,
                    child_turn,
                    TokenUsage::default(),
                    1,
                    DoneReason::EndTurn,
                ),
                ..expected.clone()
            };
            let store = session_offload(Some(directory.path()), &handle.session_id).unwrap();
            let child_stored = child_result.to_stored(session_id, Some(&store)).unwrap();
            journal
                .commit(JournalCommit {
                    agents: vec![test_record(child_id, Some(root_id))],
                    changes: vec![
                        JournalChange {
                            agent: Some(child_id),
                            generation: 0,
                            change: StateChange::Config(
                                stored_agent_config(&config, Default::default()).unwrap(),
                            ),
                        },
                        JournalChange {
                            agent: Some(child_id),
                            generation: 0,
                            change: StateChange::Turn {
                                turn: child_turn,
                                result: child_stored.clone(),
                            },
                        },
                        JournalChange {
                            agent: Some(root_id),
                            generation: 0,
                            change: StateChange::Delivery {
                                id: (child_id, child_turn),
                                op: StoredDeliveryOp::Queued(Box::new(StoredDelivery {
                                    label: None,
                                    result: child_stored,
                                    preview_bytes: None,
                                    held: true,
                                })),
                            },
                        },
                    ],
                    histories: BTreeMap::from([(
                        child_id,
                        History::new(Vec::new()).snapshot().clone(),
                    )]),
                    ..Default::default()
                })
                .unwrap();
            journal
                .checkpoint_snapshot()
                .unwrap()
                .apply_to(&mut session)
                .unwrap();
            session.save(&storage).unwrap();
            handle.task.cancel().await;
            assert!(
                bounded(handle.manager.shutdown(GATE_TIMEOUT))
                    .await
                    .timed_out
                    .is_empty()
            );
            bounded(coordinator.close()).await.unwrap();
            drop(handle.manager);
            drop(journal);

            let loaded =
                Session::<Message, TokenUsage, ToolOutput>::load(session_id, &storage).unwrap();
            let mut params = interactive_params(session_id, Vec::new());
            params.providers = Arc::new(makima_providers::test_util::offline_ids(&["anthropic"]));
            params.root_id = Some(root_id);
            params.journal = Some(Arc::new(SessionJournal::from_session(&loaded).unwrap()));
            params.state_dir = Some(directory.path().to_path_buf());
            let (provider, requests) = recording();
            let handle = spawn_interactive_with_binding(
                params,
                Box::new(move |model| {
                    Box::pin(async move { Ok(PreparedModel { model, provider }) })
                }),
                Some(loaded.history_snapshot()),
                None,
            );
            let root = bounded(handle.initialized_root()).await.unwrap();
            let actor = root.actor().unwrap();
            assert_eq!(
                actor.turn_ticket(ticket.turn_id()).unwrap().peek_result(),
                Some(expected.clone())
            );

            let manager = &handle.manager;
            assert_eq!(
                manager
                    .retained_turn_ticket(&manager.reference(child_id), child_turn)
                    .unwrap()
                    .peek_result(),
                Some(child_result.clone()),
            );
            let child = bounded(manager.ensure_resident_with(child_id, |_| async {
                Ok(ResidentAgent {
                    backend: Box::new(MockBackend::STRICT),
                    config: (*config).clone(),
                    shared_messages: None,
                    start: None,
                    initialize: None,
                })
            }))
            .await
            .unwrap();
            assert_eq!(
                child
                    .actor()
                    .unwrap()
                    .turn_ticket(child_turn)
                    .unwrap()
                    .peek_result(),
                Some(child_result.clone())
            );
            assert_eq!(
                actor.turn_ticket(ticket.turn_id()).unwrap().peek_result(),
                Some(expected)
            );
            assert!(actor.withdraw_child_delivery(child_turn));
            assert!(requests.is_empty());
            handle.task.cancel().await;
            assert!(
                bounded(manager.shutdown(GATE_TIMEOUT))
                    .await
                    .timed_out
                    .is_empty()
            );
        });
    }

    #[test_case(false ; "without_offload_store")]
    #[test_case(true ; "with_offload_store")]
    fn nonpersistent_print_preserves_oversized_result_without_durable_conversion(offload: bool) {
        smol::block_on(async {
            let state = tempfile::tempdir().unwrap();
            let session_id = MakimaId::generate();
            let coordinator = coordinator(session_id, checkpoint());
            let provider: Arc<dyn Provider> = Arc::new(OversizedPrintProvider);
            let binding = coordinator_binding(coordinator.clone(), Arc::clone(&provider));
            let mut params = interactive_params(session_id, Vec::new());
            params.answers = false;
            params.state_dir = offload.then(|| state.path().to_path_buf());
            let handle = spawn_nonpersistent_with_binding(
                params,
                Box::new(move |model| {
                    Box::pin(async move { Ok(PreparedModel { model, provider }) })
                }),
                Some(binding),
            );
            let ticket = handle.input_tx.admit(first_input()).await.unwrap();
            let result = bounded(ticket.wait_result()).await;
            assert!(matches!(result.outcome, TurnOutcome::Completed { .. }));
            assert_eq!(
                result.text,
                SAVED_OUTPUT.repeat(MAX_INLINE_TURN_RESULT_BYTES / SAVED_OUTPUT.len() + 1)
            );
            let journal = handle.manager.journal();
            assert!(journal.session_id().is_none());
            assert!(journal.persistence_error().is_none());
            assert!(journal.pending_revision().is_none());
            assert!(
                !journal
                    .snapshot()
                    .state
                    .iter()
                    .any(|record| { matches!(record.change, StateChange::Turn { .. }) })
            );
            let dir = offload_dir(&state.path().join(SESSIONS_DIR), session_id);
            if offload {
                std::fs::create_dir_all(&dir).unwrap();
                std::fs::write(dir.join("saved.txt"), SAVED_OUTPUT).unwrap();
            }
            let InteractiveHandle { input_tx, task, .. } = handle;
            drop(input_tx);
            bounded(task).await;
            assert!(!dir.exists());
            bounded(coordinator.close()).await.unwrap();
        });
    }

    #[test_case(None, true; "ambient_yolo_is_not_stored")]
    #[test_case(Some(true), false; "explicit_enabled")]
    #[test_case(Some(false), true; "explicit_disabled_overrides_seed")]
    fn startup_checkpoint_publishes_coherent_config_and_rules_once(yolo: Option<bool>, seed: bool) {
        const RULE_TOOL: &str = "bash";
        const RULE_SCOPE: &str = "startup *";
        smol::block_on(async {
            let directory = tempfile::tempdir().unwrap();
            let storage = StateDir::from_path(directory.path().to_path_buf());
            let session = Session::<Message, TokenUsage, ToolOutput>::new(TEST_MODEL, &TEST_CWD);
            let mut params = interactive_params(session.id, Vec::new());
            params.yolo = seed;
            let (provider, requests) = recording();
            let handle = spawn_interactive_with_binding(
                params,
                Box::new(move |model| {
                    Box::pin(async move { Ok(PreparedModel { model, provider }) })
                }),
                None,
                None,
            );
            let root = bounded(handle.initialized_root()).await.unwrap();
            let rules = vec![makima_storage::sessions::StoredRule {
                tool: RULE_TOOL.into(),
                scope: Some(RULE_SCOPE.into()),
                effect: makima_storage::sessions::StoredEffect::Allow,
            }];
            handle
                .permissions
                .restore(PermissionSnapshot::from_stored(&rules, yolo).unwrap());
            let journal = handle.manager.journal();
            let writer = Arc::new(
                SessionWriter::open_session(
                    &storage.ensure_subdir(SESSIONS_DIR).unwrap(),
                    &session,
                )
                .unwrap(),
            );
            writer.bind_journal(Arc::clone(&journal)).await.unwrap();
            let coordinator = coordinator(handle.session_id.id(), writer);
            coordinator
                .bind_permissions(Arc::clone(&handle.permissions))
                .unwrap();
            coordinator
                .bind_actor(
                    root.clone(),
                    Arc::new(|_| Box::pin(async { unreachable!() })),
                    Arc::new(|_| {}),
                )
                .unwrap();
            bounded(checkpoint_startup_config(&root, &coordinator))
                .await
                .unwrap();
            let saved =
                Session::<Message, TokenUsage, ToolOutput>::load(handle.session_id.id(), &storage)
                    .unwrap();
            let view = saved.latest_state().unwrap();
            assert_eq!(view.scopes[&None].rules, rules);
            assert_eq!(view.scopes[&None].yolo, yolo);
            assert_eq!(
                view.scopes[&Some(root.id())].config.as_ref().unwrap().model,
                TEST_MODEL
            );
            let before = journal.snapshot();
            bounded(checkpoint_startup_config(&root, &coordinator))
                .await
                .unwrap();
            assert_eq!(journal.snapshot().state, before.state);
            assert_eq!(journal.snapshot().revision, before.revision);
            assert_eq!(handle.permissions.is_yolo(), yolo.unwrap_or(seed));
            const GRANTS: usize = 8;
            let barrier = Barrier::new(2);
            thread::scope(|scope| {
                let permissions = &handle.permissions;
                let barrier = &barrier;
                scope.spawn(move || {
                    for index in 0..GRANTS {
                        barrier.wait();
                        permissions.add_session_rule(makima_config::PermissionRule {
                            tool: makima_config::ToolKey::native(RULE_TOOL),
                            scope: Some(format!("{RULE_SCOPE}{index}")),
                            effect: makima_config::Effect::Allow,
                        });
                    }
                });
                for _ in 0..GRANTS {
                    barrier.wait();
                    permissions
                        .commit_startup(JournalCommit::default())
                        .unwrap();
                }
            });
            let mut previous = 0;
            for record in journal.snapshot().state.iter() {
                if let StateChange::Rules {
                    rules,
                    yolo: stored,
                } = &record.change
                {
                    assert!(rules.len() >= previous);
                    previous = rules.len();
                    assert_eq!(*stored, yolo);
                }
            }
            assert_eq!(previous, GRANTS + rules.len());
            let snapshot = journal.snapshot();
            let folded = snapshot.fold().unwrap();
            assert_eq!(
                folded.scopes[&None].rules,
                handle.permissions.snapshot().stored_rules()
            );
            assert!(requests.is_empty());
            bounded(handle.task.cancel()).await;
            bounded(coordinator.close()).await.unwrap();
        });
    }

    #[test_case(false ; "fresh_session")]
    #[test_case(true ; "restored_compacted_session")]
    fn startup_checkpoint_adopts_the_actor_history_identity(restored: bool) {
        smol::block_on(async {
            let state = tempfile::tempdir().unwrap();
            let storage = StateDir::from_path(state.path().to_path_buf());
            let mut session =
                Session::<Message, TokenUsage, ToolOutput>::new(TEST_MODEL, &TEST_CWD);
            let root_id = AgentId::generate();
            let initial = restored.then(|| {
                let mut history = History::new(vec![Message::user(SAVED_OUTPUT.into())]);
                history
                    .replace_compacted(vec![Message::observation(SAVED_OUTPUT.into())])
                    .unwrap();
                history.mark_sent();
                let snapshot = history.snapshot().clone();
                session.adopt_history(&snapshot);
                session.bind_root_id(root_id).unwrap();
                snapshot
            });
            let (provider, requests) = recording();
            let mut params = interactive_params(session.id, Vec::new());
            params.root_id = Some(root_id);
            params.journal =
                restored.then(|| Arc::new(SessionJournal::from_session(&session).unwrap()));
            let handle = spawn_interactive_with_binding(
                params,
                Box::new(move |model| {
                    Box::pin(async move { Ok(PreparedModel { model, provider }) })
                }),
                initial.clone(),
                None,
            );
            let root = bounded(handle.initialized_root()).await.unwrap();
            let source = root
                .actor()
                .unwrap()
                .prepare_idle()
                .unwrap()
                .source_snapshot()
                .unwrap();
            let journal = handle.manager.journal();
            let attached = journal.snapshot();
            assert_eq!(attached.managed_histories.contains(&root_id), !restored);
            assert_eq!(attached.histories[&root_id].epoch, source.history.epoch);
            assert_eq!(
                attached.histories[&root_id].conversation,
                source.history.conversation
            );
            if let Some(initial) = initial {
                assert_eq!(source.history.epoch, initial.epoch);
                assert_eq!(source.history.conversation, initial.conversation);
                assert_eq!(source.history.compacted, initial.compacted);
            }
            let writer = Arc::new(
                SessionWriter::open_session(
                    &storage.ensure_subdir(SESSIONS_DIR).unwrap(),
                    &session,
                )
                .unwrap(),
            );
            writer.bind_journal(Arc::clone(&journal)).await.unwrap();
            let coordinator = coordinator(handle.session_id.id(), writer.clone());
            coordinator
                .bind_permissions(Arc::clone(&handle.permissions))
                .unwrap();
            let (provider, _) = recording();
            coordinator
                .bind_actor(
                    root.clone(),
                    Arc::new(move |spec| {
                        let provider = Arc::clone(&provider);
                        let model = Model::from_spec(&spec).unwrap();
                        Box::pin(async move { Ok(PreparedModel { provider, model }) })
                    }),
                    Arc::new(|_| {}),
                )
                .unwrap();
            bounded(checkpoint_startup_config(&root, &coordinator))
                .await
                .unwrap();
            let saved =
                Session::<Message, TokenUsage, ToolOutput>::load(handle.session_id.id(), &storage)
                    .unwrap();
            let saved = saved.history_snapshot();
            assert_eq!(
                journal.snapshot().histories[&root_id].epoch,
                source.history.epoch
            );
            assert_eq!(saved.compacted, source.history.compacted);
            assert_eq!(saved.conversation, source.history.conversation);
            assert_eq!(saved.messages, source.history.messages);
            assert!(journal.snapshot().managed_histories.contains(&root_id));
            assert_eq!(journal.published_revision(), journal.snapshot().revision);
            assert!(requests.is_empty());
            bounded(handle.task.cancel()).await;
            bounded(coordinator.close()).await.unwrap();
        });
    }

    #[test]
    fn terminal_waits_for_checkpoint_then_forwards() {
        smol::block_on(async {
            let (seen, received) = flume::bounded(1);
            let (release, released) = flume::bounded(1);
            let journal = Arc::new(OnceLock::<Arc<SessionJournal>>::new());
            let checkpoint_journal = Arc::clone(&journal);
            let checkpoint: Checkpoint = Arc::new(
                move |request: CheckpointRequest<crate::session_coordinator::SessionCheckpoint>| {
                    let seen = seen.clone();
                    let released = released.clone();
                    let journal = checkpoint_journal.get().unwrap().snapshot();
                    Box::pin(async move {
                        seen.send_async((request.snapshot.history.clone().unwrap(), journal))
                            .await
                            .unwrap();
                        released.recv_async().await.unwrap();
                        Ok(CheckpointAck::for_request(&request))
                    }) as CheckpointFuture
                },
            );
            let (provider, _) = recording();
            let (handle, coordinator) = fixture(Vec::new(), checkpoint, provider);
            let root = handle.initialized_root().await.unwrap();
            assert!(journal.set(handle.manager.journal()).is_ok());
            let ticket = handle.input_tx.admit(first_input()).await.unwrap();
            let (saved, journal) = bounded(received.recv_async()).await.unwrap();
            assert!(saved.messages.len() >= 2);
            let sealed = ticket.peek_result().unwrap();
            assert!(journal.state.iter().any(|record| {
                record.agent == Some(root.id())
                    && matches!(&record.change, StateChange::Turn { turn, result }
                        if *turn == ticket.turn_id()
                            && result.outcome == sealed.outcome.clone().into())
            }));
            assert_eq!(journal.histories[&root.id()].messages, saved.messages);
            assert!(
                !handle
                    .event_rx
                    .try_iter()
                    .any(|event| matches!(event.event, AgentEvent::TurnOutcome(_)))
            );
            release.send_async(()).await.unwrap();
            assert!(matches!(
                bounded(ticket.wait()).await,
                TurnOutcome::Completed { .. }
            ));
            let mut terminals = 0;
            while let Ok(event) = handle.event_rx.try_recv() {
                if let AgentEvent::TurnOutcome(outcome) = event.event {
                    assert_eq!(outcome.agent_id(), root.id());
                    terminals += 1;
                }
            }
            assert_eq!(terminals, 1);
            assert!(ticket.peek_result().is_some());
            handle.task.cancel().await;
            coordinator.close().await.unwrap();
        });
    }

    #[test_case(true, false ; "setup_failure_checkpoints_before_reporting")]
    #[test_case(true, true ; "setup_failure_checkpoint_error_does_not_strand")]
    #[test_case(false, false ; "queued_cancellation_checkpoints_before_reporting")]
    #[test_case(false, true ; "queued_cancellation_checkpoint_error_does_not_strand")]
    fn terminal_without_provider_completion_is_checkpointed(setup_failure: bool, fail: bool) {
        smol::block_on(async {
            let (seen, checkpoints) = flume::bounded(1);
            let (publish, publishing) = flume::bounded(1);
            let checkpoint_calls = Arc::new(AtomicUsize::new(0));
            let journal = Arc::new(OnceLock::<Arc<SessionJournal>>::new());
            let checkpoint_journal = Arc::clone(&journal);
            let checkpoint: Checkpoint = Arc::new(
                move |request: CheckpointRequest<crate::session_coordinator::SessionCheckpoint>| {
                    let seen = seen.clone();
                    let publishing = publishing.clone();
                    let fail = fail && checkpoint_calls.fetch_add(1, Ordering::SeqCst) == 0;
                    let journal = checkpoint_journal.get().unwrap().snapshot();
                    Box::pin(async move {
                        seen.send_async(journal).await.unwrap();
                        publishing.recv_async().await.unwrap();
                        if fail {
                            Err(CheckpointError::Save {
                                session_id: request.session_id,
                                message: Arc::from(SAVE_ERROR),
                            })
                        } else {
                            Ok(CheckpointAck::for_request(&request))
                        }
                    }) as CheckpointFuture
                },
            );
            let session_id = MakimaId::generate();
            let coordinator = coordinator(session_id, checkpoint);
            let (entered, requests) = flume::bounded(1);
            let (release, released) = flume::bounded(1);
            let provider: Arc<dyn Provider> = Arc::new(GatedHeadlessProvider {
                entered,
                release: released,
                exit: ProviderExit::Success,
            });
            let binding = coordinator_binding(coordinator.clone(), Arc::clone(&provider));
            let params = interactive_params(session_id, Vec::new());
            let handle = spawn_interactive_with_binding(
                params,
                Box::new(move |model| {
                    Box::pin(async move { Ok(PreparedModel { model, provider }) })
                }),
                None,
                Some(binding),
            );
            let root = bounded(handle.initialized_root()).await.unwrap();
            assert!(journal.set(handle.manager.journal()).is_ok());
            let mut input = first_input();
            if setup_failure {
                input.prompt = Some(Box::new(McpPromptRef {
                    qualified_name: MISSING_PROMPT.into(),
                    arguments: Default::default(),
                }));
            }
            let first = handle.input_tx.admit(input).await.unwrap();
            let ticket = if setup_failure {
                first.clone()
            } else {
                bounded(requests.recv_async()).await.unwrap();
                let queued = root
                    .actor()
                    .unwrap()
                    .admit_turn(
                        first_input(),
                        Some(EventSender::new(handle.input_tx.admitter.events.clone())),
                        SAVED_OUTPUT.into(),
                    )
                    .unwrap();
                assert_ne!(queued.turn_id(), first.turn_id());
                handle
                    .cancellation
                    .cancel(CancelScope::Turn(queued.clone()))
                    .unwrap();
                queued
            };
            let saved = bounded_terminal(
                checkpoints.recv_async(),
                TERMINAL_CHECKPOINT_GATE,
                &handle,
                &ticket,
            )
            .await
            .unwrap();
            let sealed = ticket.peek_result().unwrap();
            assert!(saved.state.iter().any(|record| {
                record.agent == Some(root.id())
                    && matches!(&record.change, StateChange::Turn { turn, result }
                        if *turn == ticket.turn_id()
                            && result.outcome == sealed.outcome.clone().into())
            }));
            if setup_failure {
                assert!(
                    matches!(&sealed.outcome, TurnOutcome::Failed { failure, .. }
                    if failure.diagnostic.contains(MISSING_PROMPT))
                );
                assert!(requests.is_empty());
            } else {
                assert!(matches!(sealed.outcome, TurnOutcome::Cancelled { .. }));
                assert!(first.peek().is_none());
            }
            assert!(future::poll_once(ticket.wait()).await.is_none());
            let actor = root.actor().unwrap();
            if setup_failure {
                let probe = bounded_terminal(
                    actor.runtime_barrier(),
                    TERMINAL_RUNNER_DRAIN,
                    &handle,
                    &ticket,
                )
                .await
                .unwrap();
                assert!(!probe.processing);
                assert_eq!(probe.pending_publications, 1);
            }
            let mut exact = Box::pin(actor.wait_outcome(ticket.turn_id()));
            assert!(future::poll_once(&mut exact).await.is_none());
            let mut settled = Box::pin(handle.manager.wait_settled());
            assert!(future::poll_once(&mut settled).await.is_none());
            let mut shutdown =
                setup_failure.then(|| Box::pin(handle.manager.shutdown(GRAPH_SHUTDOWN_TIMEOUT)));
            if let Some(shutdown) = &mut shutdown {
                assert!(future::poll_once(shutdown).await.is_none());
            }
            assert!(!handle.event_rx.try_iter().any(|event| {
                matches!(event.event, AgentEvent::TurnOutcome(outcome)
                    if outcome.turn_id() == ticket.turn_id())
            }));
            publish.send_async(()).await.unwrap();
            let outcome =
                bounded_terminal(ticket.wait(), TERMINAL_TICKET_PUBLICATION, &handle, &ticket)
                    .await;
            assert_eq!(
                bounded_terminal(exact, TERMINAL_EXACT_PUBLICATION, &handle, &ticket)
                    .await
                    .unwrap(),
                outcome
            );
            assert_eq!(ticket.wait_result().await.outcome, outcome);
            if let Some(shutdown) = shutdown {
                let report = bounded_terminal(shutdown, TERMINAL_SHUTDOWN, &handle, &ticket).await;
                assert!(report.timed_out.is_empty());
                assert!(report.joined.contains(&root.id()));
                bounded_terminal(&mut settled, TERMINAL_SETTLEMENT, &handle, &ticket).await;
            }
            if fail && setup_failure {
                assert!(matches!(&outcome, TurnOutcome::Failed { failure, .. }
                    if failure.user_message.contains(SAVE_ERROR)));
            } else {
                assert_eq!(outcome, sealed.outcome);
            }
            if fail {
                assert!(
                    journal
                        .get()
                        .unwrap()
                        .persistence_error()
                        .unwrap()
                        .contains(SAVE_ERROR)
                );
            }
            let terminals: Vec<_> = handle
                .event_rx
                .try_iter()
                .filter_map(|event| match event.event {
                    AgentEvent::TurnOutcome(outcome) if outcome.turn_id() == ticket.turn_id() => {
                        Some(outcome)
                    }
                    _ => None,
                })
                .collect();
            assert_eq!(terminals, [outcome]);
            if !setup_failure {
                release.send_async(()).await.unwrap();
                bounded(checkpoints.recv_async()).await.unwrap();
                publish.send_async(()).await.unwrap();
                let completed = bounded(first.wait()).await;
                assert!(matches!(completed, TurnOutcome::Completed { .. }));
                assert!(journal.get().unwrap().persistence_error().is_none());
            }
            let manager = handle.manager.clone();
            future::or(handle.task.cancel(), async {
                smol::Timer::after(GATE_TIMEOUT).await;
                panic!(
                    "{TERMINAL_HANDLE_CANCEL} did not settle; graph={:?}; ticket={:?}",
                    manager.snapshot(),
                    ticket.peek()
                );
            })
            .await;
            future::or(coordinator.close(), async {
                smol::Timer::after(GATE_TIMEOUT).await;
                panic!(
                    "{TERMINAL_COORDINATOR_CLOSE} did not settle; graph={:?}; ticket={:?}",
                    manager.snapshot(),
                    ticket.peek()
                );
            })
            .await
            .unwrap();
        });
    }

    #[test]
    fn terminal_conversion_timeout_does_not_strand_or_start_checkpoint() {
        smol::block_on(async {
            let (seen, checkpoints) = flume::unbounded();
            let checkpoint: Checkpoint = Arc::new(
                move |request: CheckpointRequest<crate::session_coordinator::SessionCheckpoint>| {
                    seen.send(()).unwrap();
                    Box::pin(async move { Ok(CheckpointAck::for_request(&request)) })
                        as CheckpointFuture
                },
            );
            let session_id = MakimaId::generate();
            let coordinator = coordinator(session_id, checkpoint);
            let journal = Arc::new(SessionJournal::default());
            let pending = journal.stage(JournalCommit::default(), 1).unwrap();
            let (provider, requests) = recording();
            let binding = coordinator_binding(coordinator.clone(), Arc::clone(&provider));
            let mut params = interactive_params(session_id, Vec::new());
            params.journal = Some(Arc::clone(&journal));
            params.timeouts.low_speed = Duration::ZERO;
            let handle = spawn_interactive_with_binding(
                params,
                Box::new(move |model| {
                    Box::pin(async move { Ok(PreparedModel { model, provider }) })
                }),
                None,
                Some(binding),
            );
            let ticket = handle.input_tx.admit(first_input()).await.unwrap();
            let outcome = bounded(ticket.wait()).await;
            assert!(matches!(&outcome, TurnOutcome::Failed { failure, .. }
                if failure.user_message.contains(TERMINAL_CHECKPOINT_TIMEOUT)));
            assert!(!requests.is_empty());
            assert!(checkpoints.is_empty());
            assert_eq!(ticket.peek_result().unwrap().outcome, outcome);
            journal
                .complete(
                    pending,
                    vec![JournalChange {
                        agent: None,
                        generation: 0,
                        change: StateChange::Plugin {
                            key: SAVED_OUTPUT.into(),
                            version: 1,
                            value: None,
                        },
                    }],
                )
                .unwrap();
            bounded(handle.task.cancel()).await;
            bounded(coordinator.close()).await.unwrap();
        });
    }

    #[test_case(false, SAVE_ERROR ; "checkpoint_failure_fails_the_turn_once")]
    #[test_case(true, TERMINAL_CHECKPOINT_TIMEOUT ; "hung_checkpoint_times_out_into_one_failure")]
    fn checkpoint_failure_fails_the_turn_with_its_usage(hang: bool, expected: &str) {
        smol::block_on(async {
            let checkpoint: Checkpoint = Arc::new(
                move |request: CheckpointRequest<crate::session_coordinator::SessionCheckpoint>| {
                    Box::pin(async move {
                        if hang {
                            future::pending::<()>().await;
                        }
                        Err(CheckpointError::Save {
                            session_id: request.session_id,
                            message: Arc::from(SAVE_ERROR),
                        })
                    }) as CheckpointFuture
                },
            );
            let session_id = MakimaId::generate();
            let coordinator = coordinator(session_id, checkpoint);
            let (provider, _) = recording();
            let binding = coordinator_binding(coordinator.clone(), Arc::clone(&provider));
            let mut params = interactive_params(session_id, Vec::new());
            if hang {
                params.timeouts.low_speed = Duration::ZERO;
            }
            let handle = spawn_interactive_with_binding(
                params,
                Box::new(move |model| {
                    Box::pin(async move { Ok(PreparedModel { model, provider }) })
                }),
                None,
                Some(binding),
            );
            let ticket = handle.input_tx.admit(first_input()).await.unwrap();
            let settled = bounded(ticket.wait()).await;
            assert!(
                matches!(&settled, TurnOutcome::Failed { failure, num_turns, .. }
                    if failure.user_message.contains(expected) && *num_turns > 0),
                "{settled:?}"
            );
            let terminals: Vec<_> = handle
                .event_rx
                .try_iter()
                .filter(|envelope| {
                    matches!(
                        envelope.event,
                        AgentEvent::TurnOutcome(_) | AgentEvent::ControlError { .. }
                    )
                })
                .collect();
            assert!(
                matches!(
                    terminals.as_slice(),
                    [Envelope { event: AgentEvent::TurnOutcome(outcome), .. }] if *outcome == settled
                ),
                "{terminals:?}"
            );
            handle.task.cancel().await;
            coordinator.close().await.unwrap();
        });
    }

    #[test]
    fn print_manual_compaction_without_session_id() {
        smol::block_on(async {
            let (provider, requests) = recording();
            let mut params =
                interactive_params(MakimaId::generate(), vec![Message::user(SECOND.into())]);
            params.session_id = None;
            let handle = spawn_nonpersistent_with_binding(
                params,
                Box::new(move |model| {
                    Box::pin(async move { Ok(PreparedModel { model, provider }) })
                }),
                None,
            );
            let root = bounded(handle.initialized_root()).await.unwrap();
            let actor = root.actor().unwrap();
            bounded(
                handle
                    .control_tx
                    .compact_to(actor.clone(), None, CancelToken::none()),
            )
            .await
            .unwrap();
            bounded(actor.runtime_barrier()).await.unwrap();
            assert!(requests.try_recv().is_ok());
            let history = actor.committed_history();
            assert!(
                history
                    .iter()
                    .any(|message| message.first_text_content() == Some(FIRST))
            );
            assert!(
                !history
                    .iter()
                    .any(|message| message.first_text_content() == Some(SECOND))
            );
            bounded(handle.task.cancel()).await;
        });
    }

    #[test]
    fn managed_manual_compaction_uses_model_override() {
        smol::block_on(async {
            let (base_provider, base_requests) = recording();
            let (compact_provider, compact_requests) = recording();
            let mut params =
                interactive_params(MakimaId::generate(), vec![Message::user(SECOND.into())]);
            params.providers = Arc::new(ProviderRegistry::new(vec![RegisteredProvider {
                id: COMPACTION_PROVIDER.into(),
                display_name: COMPACTION_PROVIDER.into(),
                models: vec![ModelInfo {
                    context_window: Some(COMPACTION_WINDOW),
                    ..ModelInfo::id_only(Model::parse_spec(COMPACTION_MODEL).unwrap().1.into())
                }],
                backend: Arc::new(CompactionBackend(compact_provider)),
                auth: Default::default(),
                docs: None,
            }]));
            let coordinator = coordinator(params.session_id.as_ref().unwrap().id(), checkpoint());
            let binding = coordinator_binding(coordinator.clone(), Arc::clone(&base_provider));
            let handle = spawn_interactive_with_binding(
                params,
                Box::new(move |model| {
                    Box::pin(async move {
                        Ok(PreparedModel {
                            model,
                            provider: base_provider,
                        })
                    })
                }),
                None,
                Some(binding),
            );
            let actor = bounded(handle.initialized_root())
                .await
                .unwrap()
                .actor()
                .unwrap();
            let directory = tempfile::tempdir().unwrap();
            let state = StateDir::from_path(directory.path().into());
            model_registry::set_and_persist(COMPACTION_MODEL.into(), ModelTier::Compaction, &state);
            let result = bounded(
                handle
                    .control_tx
                    .compact_to(actor, None, CancelToken::none()),
            )
            .await;
            model_registry::unset_and_persist(COMPACTION_MODEL, ModelTier::Compaction, &state);
            result.unwrap();
            assert!(base_requests.is_empty());
            assert_eq!(compact_requests.try_recv().unwrap().1, COMPACTION_MODEL);
            assert!(handle.event_rx.try_iter().any(|envelope| matches!(
                envelope.event,
                AgentEvent::ModelSwitched { spec, context_window }
                    if spec == COMPACTION_MODEL && context_window == COMPACTION_WINDOW
            )));
            bounded(handle.task.cancel()).await;
            bounded(coordinator.close()).await.unwrap();
        });
    }

    #[test_case(false ; "manual_compaction_persists_before_reporting_success")]
    #[test_case(true ; "failed_persistence_reports_failure_and_rolls_history_back")]
    fn managed_manual_compaction(fail: bool) {
        smol::block_on(async {
            let original = vec![Message::user("original".into())];
            let (seen, received) = flume::bounded(1);
            let (release, released) = flume::bounded(1);
            let checkpoint: Checkpoint = Arc::new(
                move |request: CheckpointRequest<crate::session_coordinator::SessionCheckpoint>| {
                    let seen = seen.clone();
                    let released = released.clone();
                    Box::pin(async move {
                        seen.send_async(()).await.unwrap();
                        released.recv_async().await.unwrap();
                        if fail {
                            Err(CheckpointError::Save {
                                session_id: request.session_id,
                                message: Arc::from(SAVE_ERROR),
                            })
                        } else {
                            Ok(CheckpointAck::for_request(&request))
                        }
                    }) as CheckpointFuture
                },
            );
            let (provider, _) = recording();
            let (handle, coordinator) = fixture(original.clone(), checkpoint, provider);
            let root = handle.initialized_root().await.unwrap();
            let actor = root.actor().unwrap();
            let events = actor.subscribe_events();
            let (output, results) = flume::unbounded();
            handle
                .control_tx
                .send(InteractiveControl::ManualCompaction {
                    instructions: None,
                    output,
                    cancel: CancelToken::none(),
                })
                .await
                .unwrap();
            assert_eq!(
                bounded(results.recv_async()).await.unwrap(),
                ManualCompactionEvent::Started
            );
            bounded(received.recv_async()).await.unwrap();
            assert!(results.is_empty());
            release.send_async(()).await.unwrap();
            let terminal = bounded(results.recv_async()).await.unwrap();
            if fail {
                assert!(
                    matches!(terminal, ManualCompactionEvent::Failed(error) if error.contains(SAVE_ERROR))
                );
            } else {
                assert_eq!(terminal, ManualCompactionEvent::Completed);
            }
            let ended = bounded(async {
                loop {
                    if let ActorEvent::CompactionEnd(status) = events.recv_async().await.unwrap() {
                        return status;
                    }
                }
            })
            .await;
            assert_eq!(
                ended,
                if fail {
                    CompactionStatus::Failed
                } else {
                    CompactionStatus::Completed
                }
            );
            bounded(actor.runtime_barrier()).await.unwrap();
            let transcript = actor
                .transcript(crate::actor::TranscriptRequest {
                    through_turn: None,
                    last_messages: original.len() + 4,
                    max_bytes: 4096,
                })
                .unwrap();
            assert_eq!(transcript.compacted, !fail);
            if fail {
                assert_eq!(actor.committed_message_count(), original.len());
            }
            let ticket = handle.input_tx.admit(first_input()).await.unwrap();
            bounded(received.recv_async()).await.unwrap();
            release.send_async(()).await.unwrap();
            bounded(ticket.wait()).await;
            handle.task.cancel().await;
            coordinator.close().await.unwrap();
        });
    }

    #[test]
    fn reset_dropped_on_close_replies_cancelled() {
        smol::block_on(async {
            let (seen, received) = flume::bounded(1);
            let (release, released) = flume::bounded::<()>(1);
            let checkpoint: Checkpoint = Arc::new(
                move |request: CheckpointRequest<crate::session_coordinator::SessionCheckpoint>| {
                    let seen = seen.clone();
                    let released = released.clone();
                    Box::pin(async move {
                        let _ = seen.send_async(()).await;
                        let _ = released.recv_async().await;
                        Ok(CheckpointAck::for_request(&request))
                    }) as CheckpointFuture
                },
            );
            let (provider, _) = recording();
            let (handle, coordinator) = fixture(Vec::new(), checkpoint, provider);
            let root = handle.initialized_root().await.unwrap();
            let _turn = handle.input_tx.admit(first_input()).await.unwrap();
            bounded(received.recv_async()).await.unwrap();
            let (reply, replies) = flume::bounded(1);
            handle
                .control_tx
                .send(InteractiveControl::Reset(reply))
                .await
                .unwrap();
            root.actor().unwrap().close();
            assert_eq!(
                bounded(replies.recv_async()).await.unwrap(),
                Err(OPERATION_CANCELLED.to_owned())
            );
            drop(release);
            handle.task.cancel().await;
            coordinator.close().await.unwrap();
        });
    }

    #[test_case(false ; "a_failed_compaction_is_never_persisted")]
    #[test_case(true ; "cancelled_persistence_reports_cancellation_not_failure")]
    fn unsuccessful_compaction_rolls_back(cancelled: bool) {
        smol::block_on(async {
            let previous =
                crate::agent::HistorySnapshot::new(vec![Message::user("original".into())]);
            let mut history = History::new(vec![Message::user("compacted".into())]);
            let (trigger, cancel) = CancelToken::new();
            if cancelled {
                trigger.cancel();
            }
            let compacted = if cancelled {
                Ok(())
            } else {
                Err(SAVE_ERROR.into())
            };
            let event = settle_manual_compaction(
                compacted,
                ActorHistory::new(&mut history),
                previous.clone(),
                &cancel,
                |_| async move {
                    assert!(cancelled, "failed compaction must not persist");
                    Err(SAVE_ERROR.into())
                },
            )
            .await;
            assert_eq!(
                serde_json::to_value(history.as_slice()).unwrap(),
                serde_json::to_value(previous.messages.as_ref()).unwrap()
            );
            assert_eq!(history.snapshot().epoch, previous.epoch);
            assert_eq!(
                event,
                if cancelled {
                    ManualCompactionEvent::Cancelled
                } else {
                    ManualCompactionEvent::Failed(SAVE_ERROR.into())
                }
            );
        });
    }

    #[test]
    fn headless_queued_settings_survive_config_change() {
        const FIRST_SYSTEM: &str = "admitted system\n\n# Model identity\n- Model: claude-sonnet-4-20250514\n- Provider: anthropic";
        const SECOND_SYSTEM: &str =
            "admitted system\n\n# Model identity\n- Model: claude-opus-4-8\n- Provider: anthropic";
        smol::block_on(async {
            let (requests, received) = flume::unbounded();
            let first: Arc<dyn Provider> = Arc::new(RecordingProvider {
                name: FIRST,
                requests: requests.clone(),
            });
            let (handle, coordinator) = fixture(Vec::new(), checkpoint(), first);
            let root = handle.initialized_root().await.unwrap();
            let actor = root.actor().unwrap();
            let first = handle.input_tx.admit(first_input()).await.unwrap();
            let update = actor.reserve_prepared_operation(None).unwrap();
            update
                .commit_when_ready(Some(ConfigChange::Patch(ConfigPatch {
                    model: Some(PreparedModel {
                        provider: Arc::new(RecordingProvider {
                            name: SECOND,
                            requests,
                        }),
                        model: Model::from_spec("anthropic/claude-opus-4-8").unwrap(),
                    }),
                    fast: Some(true),
                    workflow: Some(true),
                    ..Default::default()
                })))
                .await
                .unwrap();
            let second = handle.input_tx.admit(first_input()).await.unwrap();
            bounded(first.wait()).await;
            bounded(second.wait()).await;
            let before = bounded(received.recv_async()).await.unwrap();
            let after = bounded(received.recv_async()).await.unwrap();
            assert_eq!(
                (before.0, before.1, before.2, before.3),
                (FIRST, TEST_MODEL.to_owned(), FIRST_SYSTEM.into(), false)
            );
            assert_eq!(
                (after.0, after.1, after.2, after.3),
                (
                    SECOND,
                    "anthropic/claude-opus-4-8".into(),
                    SECOND_SYSTEM.into(),
                    true
                )
            );
            assert!(received.is_empty());
            handle.task.cancel().await;
            coordinator.close().await.unwrap();
        });
    }

    #[test]
    fn actor_reset_requires_frontend_and_preserves_history_and_conversation() {
        smol::block_on(async {
            let (requests_tx, requests_rx) = flume::unbounded();
            let (ended_tx, ended_rx) = flume::unbounded();
            let (handle, coordinator) = fixture(
                Vec::new(),
                checkpoint(),
                Arc::new(EndRecordingProvider {
                    requests: requests_tx,
                    ended: ended_tx,
                }),
            );
            let root = handle.initialized_root().await.unwrap();
            let turn = handle.input_tx.admit(first_input()).await.unwrap();
            bounded(turn.wait()).await;
            bounded(requests_rx.recv_async()).await.unwrap();
            let history = root.actor().unwrap().committed_history();
            let (reply, response) = flume::bounded(1);
            handle
                .control_tx
                .send(InteractiveControl::Reset(reply))
                .await
                .unwrap();
            assert_eq!(
                bounded(response.recv_async()).await.unwrap(),
                Err(RESET_REQUIRES_FRONTEND.into())
            );
            assert_eq!(root.actor().unwrap().committed_history(), history);
            assert!(ended_rx.is_empty());
            handle.task.cancel().await;
            coordinator.close().await.unwrap();
        });
    }

    #[test]
    fn reset_is_ordered_after_active_turn() {
        smol::block_on(async {
            let (entered, received) = flume::bounded(1);
            let (release, released) = flume::bounded(1);
            let provider = Arc::new(GatedHeadlessProvider {
                entered,
                release: released,
                exit: ProviderExit::Success,
            });
            let (handle, coordinator) = fixture(Vec::new(), checkpoint(), provider);
            let primary = handle.input_tx.admit(first_input()).await.unwrap();
            bounded(received.recv_async()).await.unwrap();
            let (reply, response) = flume::bounded(1);
            handle
                .control_tx
                .send(InteractiveControl::Reset(reply))
                .await
                .unwrap();
            assert!(response.is_empty());
            release.send_async(()).await.unwrap();
            bounded(primary.wait()).await;
            assert_eq!(
                bounded(response.recv_async()).await.unwrap(),
                Err(RESET_REQUIRES_FRONTEND.into())
            );
            let root = handle.initialized_root().await.unwrap();
            assert!(root.actor().unwrap().committed_message_count() > 0);
            handle.task.cancel().await;
            coordinator.close().await.unwrap();
        });
    }

    #[test]
    fn queued_compaction_cancellation_does_not_wait_for_active_turn() {
        smol::block_on(async {
            let (entered, received) = flume::bounded(1);
            let (_release, released) = flume::bounded(1);
            let provider = Arc::new(GatedHeadlessProvider {
                entered,
                release: released,
                exit: ProviderExit::Pending,
            });
            let (handle, coordinator) = fixture(Vec::new(), checkpoint(), provider);
            let primary = handle.input_tx.admit(first_input()).await.unwrap();
            bounded(received.recv_async()).await.unwrap();
            let (trigger, cancel) = CancelToken::new();
            let (output, events) = flume::unbounded();
            handle
                .control_tx
                .send(InteractiveControl::ManualCompaction {
                    instructions: None,
                    output,
                    cancel,
                })
                .await
                .unwrap();
            trigger.cancel();
            assert_eq!(
                bounded(events.recv_async()).await.unwrap(),
                ManualCompactionEvent::Cancelled
            );
            assert!(primary.peek().is_none());
            let root = handle.initialized_root().await.unwrap();
            root.cancel().unwrap();
            assert!(matches!(
                bounded(primary.wait()).await,
                TurnOutcome::Cancelled { .. }
            ));
            assert!(received.is_empty());
            handle.task.cancel().await;
            coordinator.close().await.unwrap();
        });
    }

    #[test_case(false; "fresh_root")]
    #[test_case(true; "restored_root")]
    fn binding_precedes_first_admission(restored: bool) {
        const DISPATCHED_INPUT: &str = "expanded command prompt";
        smol::block_on(async {
            let (provider, received) = recording();
            let (bound, binding_seen) = flume::bounded(1);
            let session_id = MakimaId::generate();
            let coordinator = coordinator(session_id, checkpoint());
            let bind_actor = coordinator_binding(coordinator.clone(), Arc::clone(&provider));
            let mut params = interactive_params(session_id, Vec::new());
            params.config.max_concurrent_agent_turns = 1;
            params.config.max_agent_depth = 2;
            params.config.max_children_per_agent = 3;
            params.config.max_active_agents = 5;
            params.config.max_retained_agents = 7;
            let root_id = AgentId::generate();
            params.root_id = Some(root_id);
            if restored {
                let mut session =
                    Session::<Message, TokenUsage, ToolOutput>::new(TEST_MODEL, &TEST_CWD);
                session.id = session_id;
                session.bind_root_id(root_id).unwrap();
                params.journal = Some(Arc::new(SessionJournal::from_session(&session).unwrap()));
            }
            let handle = spawn_interactive_with_binding(
                params,
                Box::new(move |model| {
                    Box::pin(async move { Ok(PreparedModel { model, provider }) })
                }),
                None,
                Some(Box::new(move |manager, root, template, initialization| {
                    Box::pin(async move {
                        assert_eq!(initialization.is_some(), restored);
                        if restored {
                            assert!(root.actor().is_err());
                        }
                        assert_eq!(manager.root_id().unwrap(), root.id());
                        assert_eq!(
                            manager.limits(),
                            AgentLimits {
                                max_concurrent_agent_turns: 1,
                                max_agent_depth: 2,
                                max_children_per_agent: 3,
                                max_active_agents: 5,
                                max_retained_agents: 7,
                            }
                        );
                        assert_eq!(template.mode, AgentMode::Build);
                        assert_eq!(template.execution_agent_id, Some(root.id()));
                        assert_eq!(template.cwd, PathBuf::from(TEST_CWD.as_str()));
                        assert!(template.managed_turn.is_none());
                        bound.send_async(root.id()).await.unwrap();
                        bind_actor(manager, root, template, initialization).await
                    })
                })),
            );
            let root = handle.initialized_root().await.unwrap();
            assert_eq!(root.id(), root_id);
            assert_eq!(binding_seen.recv_async().await.unwrap(), root_id);
            assert!(received.is_empty());
            let InteractiveHandle {
                input_tx,
                event_rx,
                manager,
                task,
                ..
            } = handle;
            let ticket = input_tx
                .admit(AgentInput::new(DISPATCHED_INPUT.into(), Vec::new()))
                .await
                .unwrap();
            assert!(matches!(
                bounded(ticket.wait()).await,
                TurnOutcome::Completed { .. }
            ));
            assert_eq!(received.len(), 1);
            drop(input_tx);
            bounded(task).await;
            let inputs = event_rx
                .try_iter()
                .filter_map(|envelope| match envelope.event {
                    AgentEvent::QueueItemConsumed { text, .. } => Some(text),
                    _ => None,
                })
                .collect::<Vec<_>>();
            assert_eq!(inputs, [DISPATCHED_INPUT]);
            assert!(
                !manager
                    .snapshot()
                    .iter()
                    .any(|node| node.graph_lifecycle == crate::GraphLifecycle::Live)
            );
            coordinator.close().await.unwrap();
        });
    }

    #[test_case(ProviderExit::Success, false ; "nonpersistent_successful_completion")]
    #[test_case(ProviderExit::Pending, false ; "nonpersistent_cancelled_task")]
    #[test_case(ProviderExit::Success, true ; "persistent_successful_completion")]
    #[test_case(ProviderExit::Pending, true ; "persistent_cancelled_task")]
    fn headless_offload_cleanup_covers_task_exit(exit: ProviderExit, persistent: bool) {
        let state = tempfile::tempdir().unwrap();
        let session_id = MakimaId::generate();
        let mut params = interactive_params(session_id, Vec::new());
        params.state_dir = Some(state.path().to_path_buf());
        let (entered, received) = flume::bounded(1);
        let (release, released) = flume::bounded(1);
        let handle = spawn_started(
            params,
            Ok(Arc::new(GatedHeadlessProvider {
                entered,
                release: released,
                exit,
            })),
        );
        let InteractiveHandle {
            manager,
            input_tx,
            event_rx,
            task,
            offload_cleanup,
            ..
        } = handle;
        received.recv_timeout(GATE_TIMEOUT).unwrap();
        let store = session_offload(Some(state.path()), &SessionRef::from(session_id)).unwrap();
        let result = TurnResult {
            outcome: TurnOutcome::completed(
                AgentId::generate(),
                TurnId::generate(),
                TokenUsage::default(),
                0,
                DoneReason::EndTurn,
            ),
            text: SAVED_OUTPUT.repeat(MAX_INLINE_TURN_RESULT_BYTES / SAVED_OUTPUT.len() + 1),
            output: Vec::new(),
            provenance: TurnProvenance::default(),
            duration_ms: 0,
        };
        let stored = result.to_stored(session_id, Some(&store)).unwrap();
        assert!(matches!(&stored.text, StoredPayload::Offloaded(_)));
        let mut session = Session::<Message, TokenUsage, ToolOutput>::new(TEST_MODEL, &TEST_CWD);
        session.id = session_id;
        if persistent {
            manager.journal().bind_session(session_id).unwrap();
            manager
                .journal()
                .commit(JournalCommit {
                    changes: vec![JournalChange {
                        agent: Some(result.outcome.agent_id()),
                        generation: 0,
                        change: StateChange::Turn {
                            turn: result.outcome.turn_id(),
                            result: stored,
                        },
                    }],
                    ..Default::default()
                })
                .unwrap();
            manager.journal().snapshot().apply_to(&mut session).unwrap();
            session.save_to(&state.path().join(SESSIONS_DIR)).unwrap();
        }
        let dir = store.dir();
        let cleanup = offload_cleanup.unwrap();
        match exit {
            ProviderExit::Pending => {
                smol::block_on(task.cancel());
            }
            ProviderExit::Success => {
                release.send(()).unwrap();
                let mut input_tx = Some(input_tx);
                while let Ok(event) = event_rx.recv() {
                    match event.event {
                        AgentEvent::TurnOutcome(_) => drop(input_tx.take()),
                        AgentEvent::StreamClosed => {
                            assert_eq!(dir.exists(), persistent);
                            break;
                        }
                        _ => {}
                    }
                }
                smol::block_on(task);
            }
        }
        smol::block_on(manager.shutdown(GRAPH_SHUTDOWN_TIMEOUT));
        if persistent {
            let loaded = Session::<Message, TokenUsage, ToolOutput>::load_from(
                session_id,
                &state.path().join(SESSIONS_DIR),
            )
            .unwrap();
            let stored = loaded
                .state()
                .iter()
                .find_map(|record| match &record.change {
                    StateChange::Turn { result, .. } => Some(result.clone()),
                    _ => None,
                })
                .unwrap();
            assert_eq!(
                TurnResult::from_stored(stored, |reference| {
                    std::fs::read(dir.join(&reference.path))
                })
                .unwrap(),
                result
            );
            Session::<Message, TokenUsage, ToolOutput>::delete_from(
                session_id,
                &state.path().join(SESSIONS_DIR),
            )
            .unwrap();
        } else {
            smol::block_on(cleanup.wait()).unwrap();
        }
        assert!(!dir.exists());
    }

    struct GatedGuardRemoval {
        entered: flume::Sender<()>,
        release: Receiver<()>,
    }

    impl OffloadBackend for GatedGuardRemoval {
        fn create_new(&self, _: &str, _: &[u8]) -> io::Result<bool> {
            Ok(true)
        }
        fn total_bytes(&self) -> io::Result<u64> {
            Ok(0)
        }
        fn remove_all(&self) -> io::Result<()> {
            self.entered.send(()).unwrap();
            self.release.recv_timeout(GATE_TIMEOUT).unwrap();
            Ok(())
        }
        fn path(&self, name: &str) -> PathBuf {
            PathBuf::from("/headless-offload-test").join(name)
        }
    }

    #[test]
    fn headless_dropped_task_requests_cleanup_without_waiting_for_removal() {
        let (entered, received) = flume::bounded(1);
        let (release, released) = flume::bounded(1);
        let store = Arc::new(OffloadStore::new(Box::new(GatedGuardRemoval {
            entered,
            release: released,
        })));
        let cleanup = OffloadCleanup::new(Arc::clone(&store));
        let guard = RemoveOffloadOnDrop(cleanup.clone());
        let task = smol::spawn(async move {
            let _guard = guard;
            std::future::pending::<()>().await;
        });
        drop(task);
        received.recv_timeout(GATE_TIMEOUT).unwrap();
        assert!(matches!(store.put(SAVED_OUTPUT), Err(OffloadError::Closed)));
        let mut wait = Box::pin(cleanup.wait());
        assert!(smol::block_on(future::poll_once(&mut wait)).is_none());
        release.send(()).unwrap();
        smol::block_on(wait).unwrap();
    }

    #[test_case(false ; "nonpersistent_initialization_error_drains_cleanup")]
    #[test_case(true ; "persistent_initialization_error_retains_offload")]
    fn headless_initialization_error_drains_cleanup_before_stream_close(persistent: bool) {
        let state = tempfile::tempdir().unwrap();
        let session_id = MakimaId::generate();
        let dir = offload_dir(&state.path().join(SESSIONS_DIR), session_id);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("saved.txt"), SAVED_OUTPUT).unwrap();
        let mut params = interactive_params(session_id, Vec::new());
        params.state_dir = Some(state.path().to_path_buf());
        if persistent {
            let journal = Arc::new(SessionJournal::default());
            journal.bind_session(session_id).unwrap();
            params.journal = Some(journal);
        }
        let handle = spawn_started(
            params,
            Err(crate::AgentError::Config {
                message: HEADLESS_PROVIDER_ERROR.into(),
            }),
        );
        let mut saw_error = false;
        let mut saw_close = false;
        while let Ok(event) = handle.event_rx.recv() {
            match event.event {
                AgentEvent::ControlError { message } => {
                    assert!(message.contains(HEADLESS_PROVIDER_ERROR));
                    saw_error = true;
                }
                AgentEvent::StreamClosed => {
                    assert_eq!(dir.exists(), persistent);
                    saw_close = true;
                    break;
                }
                _ => {}
            }
        }
        assert!(saw_error && saw_close);
        smol::block_on(handle.task);
        if persistent {
            assert_eq!(
                std::fs::read_to_string(dir.join("saved.txt")).unwrap(),
                SAVED_OUTPUT
            );
        } else {
            smol::block_on(handle.offload_cleanup.unwrap().wait()).unwrap();
        }
    }

    #[test]
    fn extract_tool_names_filters_valid_entries() {
        assert_eq!(
            extract_tool_names(
                &serde_json::json!([{"name": "read"}, {"type": "function"}, {"name": "bash"}])
            ),
            vec!["read", "bash"]
        );
    }

    #[test]
    fn advertised_names_show_tool_search_not_deferred_tools() {
        let base = serde_json::json!([{"name": "read"}]);
        let mcp =
            crate::mcp::test_support::stub_session(&[("srv.fetch_issue", "Fetch a GitHub issue")]);
        assert_eq!(
            advertised_tool_names(&base, Some(&mcp)),
            vec!["read", crate::mcp::TOOL_SEARCH_TOOL_NAME]
        );
        assert_eq!(advertised_tool_names(&base, None), vec!["read"]);
    }
}
