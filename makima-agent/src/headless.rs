use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex, OnceLock};
use std::time::Duration;

use async_lock::Mutex;
use event_listener::Event;
use flume::Receiver;
use futures_lite::future;
use makima_config::{ModelPolicy, ProjectConfig, SessionDefaults};
use makima_providers::Message;
use makima_providers::model::Model;
use makima_providers::provider::BoxFuture;
use makima_providers::{ProviderRegistry, RequestOptions, Timeouts};
use makima_storage::id::{MakimaId, SessionRef};
use makima_storage::session_state::{SessionStateView, StateChange};
use serde_json::Value;
use tracing::warn;

use crate::actor::{
    ActorError, ActorHistory, AdmissionPreparation, AgentActorHandle, AgentContextSnapshot,
    CompactionStatus, PreparedModel, PreparedReadiness, TerminalCheckpoint, TurnAdmissionOptions,
    TurnOrigin, TurnProvenance,
};
#[cfg(test)]
use crate::agent::History;
use crate::agent::isolated_turn::IsolatedTurnEvent;
use crate::agent::{self, PromptSlotSource, TurnSetup};
use crate::cancel::{CancelMap, CancelToken, CancelTrigger};
use crate::config_change::{ConfigChangeError, reserve_in_turn_for_actor, unresolved_model};
use crate::context::{ContextPolicy, RootContext, root_context};
use crate::journal::{JournalChange, JournalCommit, JournalIncarnation, SessionJournal};
use crate::manager::{
    ReactivationError, ResidentAgent, ResidentInitialization, ResidentInitializer,
};
use crate::permissions::{PermissionManager, PermissionSnapshot, PluginRuleStore};
use crate::prompt::ResolvedSlots;
use crate::session_checkpoint::{SessionLogCheckpoint, stored_agent_config};
use crate::session_coordinator::{ModelPreparer, SessionCoordinatorHandle};
use crate::session_options::SessionOptionError;
use crate::state::StateAccess;
use crate::template;
use crate::tools::offload::{OffloadCleanup, OffloadStore};
use crate::tools::{
    Deadline, FileReadTracker, LocalTools, RequestTools, ToolAudience, ToolContext, ToolRegistry,
    TurnToolBindings,
};
use crate::{
    ActorBackend, Agent, AgentConfig, AgentEvent, AgentId, AgentInput, AgentLimits,
    AgentManagerHandle, AgentMode, AgentParams, AgentRef, AgentRunParams, BackendResult,
    ControlWork, EffectiveAgentConfig, Envelope, EventSender, McpHandle, McpSession,
    PermissionsConfig, RunLedger, RunSettings, SessionMailbox, ToolOutputLines, TurnContext,
    TurnFailure, TurnFailureKind, TurnId, TurnTicket, WorkKind,
};

const GRAPH_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);
const INITIALIZATION_CLOSED: &str = "session ended before initialization";
const TERMINAL_CHECKPOINT_TIMEOUT: &str = "checkpoint timed out";
const OPERATION_CANCELLED: &str = "operation cancelled";
const ROOT_NOT_READY: &str = "session root is not initialized yet";
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
    dyn FnOnce(
            AgentManagerHandle,
            AgentRef,
            ToolContext,
            Option<ResidentInitialization>,
        ) -> BoxFuture<'static, Result<(), String>>
        + Send,
>;

pub struct InteractiveParams {
    pub model: Model,
    pub config: AgentConfig,
    pub permissions_config: PermissionsConfig,
    pub timeouts: Timeouts,
    pub prompt_slots: PromptSlotSource,
    pub excluded_tools: Vec<&'static str>,
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
    pub question_mode: crate::tools::QuestionMode,
    pub plugin_rules: Arc<PluginRuleStore>,
    pub project_config: ProjectConfig,
    pub local_tools: LocalTools,
    pub state_dir: Option<PathBuf>,
    /// Whether the frontend answers permission asks over `answer_tx`. Without
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
    Compact(flume::Sender<Result<(), String>>),
    ManualCompaction {
        instructions: Option<String>,
        output: flume::Sender<ManualCompactionEvent>,
        cancel: CancelToken,
    },
    Reset(flume::Sender<Result<(), String>>),
    IsolatedTurn {
        question: String,
        images: Vec<makima_providers::ImageSource>,
        root_context: Arc<RootContext>,
        context_access: StateAccess,
        output: flume::Sender<agent::isolated_turn::IsolatedTurnEvent>,
        cancel: CancelToken,
    },
}

#[derive(Clone)]
pub struct InteractiveControlSender {
    root: RootReadiness,
    session_id: MakimaId,
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
            Some(InteractiveControl::Compact(reply) | InteractiveControl::Reset(reply)) => {
                let _ = reply.send(Err(OPERATION_CANCELLED.into()));
            }
            Some(InteractiveControl::ManualCompaction { output, .. }) => {
                let _ = output.send(ManualCompactionEvent::Cancelled);
            }
            Some(InteractiveControl::IsolatedTurn { output, .. }) => {
                let _ = output.send(agent::isolated_turn::IsolatedTurnEvent::Cancelled);
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
            InteractiveControl::ManualCompaction { cancel, .. }
            | InteractiveControl::IsolatedTurn { cancel, .. } => Some(cancel.clone()),
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
                            pending
                                .lock()
                                .unwrap_or_else(|error| error.into_inner())
                                .cancel();
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

    /// Compacts the root's history and waits for it to settle.
    pub async fn compact(
        &self,
        instructions: Option<String>,
        cancel: CancelToken,
    ) -> Result<(), String> {
        let actor = self
            .root
            .get()
            .await?
            .actor()
            .map_err(|error| error.to_string())?;
        self.compact_to(actor, instructions, cancel).await
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

    /// Asks {question} beside the root's history, leaving it unchanged, and
    /// returns the answer.
    pub async fn ask_isolated(
        &self,
        question: String,
        images: Vec<makima_providers::ImageSource>,
        cancel: CancelToken,
    ) -> Result<String, String> {
        let actor = self
            .root
            .get()
            .await?
            .actor()
            .map_err(|error| error.to_string())?;
        self.ask_isolated_to(actor, question, images, cancel).await
    }

    pub async fn send_isolated_to(
        &self,
        actor: AgentActorHandle,
        question: String,
        images: Vec<makima_providers::ImageSource>,
        output: flume::Sender<IsolatedTurnEvent>,
        cancel: CancelToken,
    ) -> Result<(), String> {
        let (context, context_access) = cancel
            .race(async {
                let root = self.root.get().await?;
                root.validate_live().map_err(|error| error.to_string())?;
                let current = root.actor().map_err(|error| error.to_string())?;
                if !current.same_actor(&actor) {
                    return Err(OPERATION_CANCELLED.into());
                }
                let access = root.state_access().read_only();
                let session = root
                    .manager()
                    .journal()
                    .session_id()
                    .unwrap_or(self.session_id);
                let context = match SessionCoordinatorHandle::resolve(session) {
                    Ok(coordinator) => coordinator
                        .ensure_root_context_for_actor(&actor)
                        .await
                        .map_err(|error| error.to_string()),
                    Err(error) => {
                        if root.manager().journal().is_persistent() {
                            root_context(&access)
                                .map_err(|error| error.to_string())?
                                .ok_or_else(|| error.to_string())
                        } else {
                            if let Some(context) =
                                root_context(&access).map_err(|error| error.to_string())?
                            {
                                return Ok((context, access));
                            }
                            let operation = reserve_in_turn_for_actor(actor.clone(), || {
                                root.validate_live()?;
                                if !root.actor()?.same_actor(&actor) {
                                    return Err(ActorError::PolicyCancelled.into());
                                }
                                root_context(&access).map_err(|error| {
                                    ConfigChangeError::Rejected(error.to_string().into())
                                })?;
                                Ok(actor.reserve_prepared_operation(None)?)
                            })
                            .await
                            .map_err(|error| error.to_string())?;
                            operation
                                .wait_predecessor()
                                .await
                                .map_err(|error| error.to_string())?;
                            root_context(&access).map_err(|error| error.to_string())?;
                            if root_context(&access)
                                .map_err(|error| error.to_string())?
                                .is_none()
                            {
                                operation
                                    .request_context(ContextPolicy::Admission)
                                    .map_err(|error| error.to_string())?;
                                operation.resolve(None).map_err(|error| error.to_string())?;
                                operation
                                    .wait_ready()
                                    .await
                                    .map_err(|error| error.to_string())?;
                                root_context(&access).map_err(|error| error.to_string())?;
                                operation.commit().map_err(|error| error.to_string())?;
                            }
                            root_context(&access)
                                .map_err(|error| error.to_string())?
                                .ok_or_else(|| HEADLESS_CONFIG_UNAVAILABLE.into())
                        }
                    }
                }?;
                root_context(&access).map_err(|error| error.to_string())?;
                Ok::<_, String>((context, access))
            })
            .await
            .map_err(|_| OPERATION_CANCELLED.to_string())??;
        self.send_to(
            actor,
            InteractiveControl::IsolatedTurn {
                question,
                images,
                root_context: context,
                context_access,
                output,
                cancel,
            },
        )
    }

    pub async fn ask_isolated_to(
        &self,
        actor: AgentActorHandle,
        question: String,
        images: Vec<makima_providers::ImageSource>,
        cancel: CancelToken,
    ) -> Result<String, String> {
        let (output, events) = flume::unbounded();
        self.send_isolated_to(actor, question, images, output, cancel)
            .await?;
        let mut answer = String::new();
        loop {
            match events.recv_async().await {
                Ok(IsolatedTurnEvent::TextDelta(text)) => answer.push_str(&text),
                Ok(IsolatedTurnEvent::ThinkingDelta(_)) => {}
                Ok(IsolatedTurnEvent::Done) => return Ok(answer),
                Ok(IsolatedTurnEvent::Error(error)) => return Err(error),
                Ok(IsolatedTurnEvent::Cancelled) | Err(_) => {
                    return Err(OPERATION_CANCELLED.into());
                }
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
        self.actor.submit_presented(
            input,
            Some(EventSender::new(self.events)),
            format!("run:{}", self.run),
            TurnAdmissionOptions {
                provenance: TurnProvenance {
                    origin: TurnOrigin::User,
                    ..Default::default()
                },
                ..Default::default()
            },
        )
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

    /// Cancels {ticket}'s turn alone; the rest of the session keeps running.
    /// Holds no part of the sender, so it never keeps the session alive.
    pub fn turn_cancel(&self, ticket: TurnTicket) -> WorkCancel {
        WorkCancel::Turn {
            ticket,
            root: self.admitter.root.clone(),
        }
    }
}

/// What a frontend cancels when its user interrupts the work in flight: a
/// local operation's trigger, or an admitted turn on the actor.
pub enum WorkCancel {
    Control(CancelTrigger),
    Turn {
        ticket: TurnTicket,
        root: RootReadiness,
    },
}

impl WorkCancel {
    pub fn turn_id(&self) -> Option<TurnId> {
        match self {
            Self::Control(_) => None,
            Self::Turn { ticket, .. } => Some(ticket.turn_id()),
        }
    }

    pub fn cancel(self) {
        match self {
            Self::Control(trigger) => trigger.cancel(),
            Self::Turn { ticket, root } => {
                let cancelled = root
                    .current()
                    .ok_or_else(|| ROOT_NOT_READY.to_owned())
                    .flatten()
                    .and_then(|root| root.actor().map_err(|error| error.to_string()))
                    .and_then(|actor| {
                        if !actor.owns_ticket(&ticket) {
                            return Ok(());
                        }
                        actor
                            .cancel_turn(ticket.turn_id())
                            .map_err(|error| error.to_string())
                    });
                if let Err(error) = cancelled {
                    warn!(%error, turn_id = %ticket.turn_id(), "turn cancel failed");
                }
            }
        }
    }
}

pub struct InteractiveHandle {
    pub manager: AgentManagerHandle,
    pub root: RootReadiness,
    pub event_rx: Receiver<Envelope>,
    pub tool_names: Vec<String>,
    pub input_tx: InteractiveInputSender,
    pub answer_tx: flume::Sender<String>,
    pub control_tx: InteractiveControlSender,
    pub model: crate::SharedModel,
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
    let options = coordinator.read().options().config_values();
    let config =
        stored_agent_config(&source.config.config, options).map_err(|error| error.to_string())?;
    coordinator
        .permissions()
        .map_err(|error| error.to_string())?
        .commit_startup(JournalCommit {
            changes: vec![JournalChange {
                agent: Some(root.id()),
                generation: source.config.generation,
                change: StateChange::Config(config),
            }],
            histories: [(root.id(), source.history.clone())].into(),
            managed_histories: [root.id()].into(),
            ..Default::default()
        })
        .map_err(|error| error.to_string())?;
    coordinator
        .persist_history(source.history)
        .await
        .map_err(|error| error.to_string())?;
    Ok(source.config.config)
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

struct HeadlessBackend {
    params: AgentParams,
    persist_turn_results: bool,
    excluded_tools: Vec<&'static str>,
    system_override: Option<String>,
    append_system: Option<String>,
    setup: TurnSetup,
    local_tools: LocalTools,
    mcp: Option<McpSession>,
    answer_rx: Option<Arc<Mutex<Receiver<String>>>>,
    raw_tx: flume::Sender<Envelope>,
    root: RootReadiness,
}

impl HeadlessBackend {
    fn event_sender(&self, context: &TurnContext) -> EventSender {
        EventSender::with_identity(self.raw_tx.clone(), Some(context.work))
    }

    async fn config(&self, context: &TurnContext) -> Result<Arc<EffectiveAgentConfig>, String> {
        if let Some(config) = &context.policy {
            return Ok(Arc::clone(config));
        }
        self.root
            .get()
            .await?
            .effective_config()
            .map_err(|error| error.to_string())?
            .ok_or_else(|| HEADLESS_CONFIG_UNAVAILABLE.to_owned())
    }

    fn system(
        &self,
        config: &EffectiveAgentConfig,
        instructions: &agent::Instructions,
        slots: &ResolvedSlots,
    ) -> String {
        let vars = template::env_vars_for(config.context.cwd());
        let definition = config
            .mode_def
            .clone()
            .unwrap_or_else(|| self.params.modes.current(&config.mode));
        let mut system = self.system_override.clone().unwrap_or_else(|| {
            agent::build_system_prompt_with_def(
                &vars,
                &definition,
                &config.mode,
                &instructions.text,
                slots,
            )
        });
        if let Some(append) = &self.append_system {
            system.push('\n');
            system.push_str(append);
        }
        system
    }

    fn tools(&self, config: &EffectiveAgentConfig) -> RequestTools {
        RequestTools::build(
            &self.params.registry,
            &template::env_vars_for(config.context.cwd()),
            &config.model,
            &self.params.config,
            &self.excluded_tools,
            config.workflow,
            self.mcp.is_some(),
        )
    }

    fn template(&self, config: &EffectiveAgentConfig) -> ToolContext {
        let tools = self.tools(config);
        ToolContext {
            execution_agent_id: Some(self.params.agent_id),
            provider: Arc::clone(&config.provider),
            model: Arc::new(config.model.clone()),
            event_tx: EventSender::new(self.raw_tx.clone()),
            mode: config.mode.clone(),
            mode_def: config.mode_def.clone().map(Arc::new),
            question_mode: self.params.question_mode,
            session_id: self.params.session_id.clone(),
            cwd: config.context.cwd().to_path_buf(),
            tool_use_id: None,
            call: None,
            user_response_rx: self.answer_rx.clone(),
            loaded_instructions: Default::default(),
            cancel: CancelToken::none(),
            mcp: self.mcp.clone(),
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
            local_tools: Arc::clone(&self.local_tools),
            local_tool_capture: None,
            turn_bindings: Arc::new(TurnToolBindings::capture(
                &self.params.registry,
                &self.local_tools,
                self.mcp.as_ref(),
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
        let config = match self.config(context).await {
            Ok(config) => config,
            Err(error) => return ManualCompactionEvent::Failed(error),
        };
        let previous = history.snapshot().clone();
        let result = agent::compact(
            &config.provider,
            &config.provider,
            &config.model,
            history.reborrow(),
            &self.event_sender(context),
            cancel,
            &self.params.config,
            instructions,
            self.params.session_id.as_ref(),
        )
        .await
        .map_err(|error| error.to_string());
        let snapshot = history.snapshot().clone();
        settle_manual_compaction(result, history, previous, cancel, |_| {
            persist_history(self.params.session_id.as_ref().unwrap().id(), snapshot)
        })
        .await
    }
}

impl ActorBackend for HeadlessBackend {
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
                            if let Some(error) = journal.persistence_error() {
                                return Err(error);
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
        context: TurnContext,
        mut input: AgentInput,
        _work: WorkKind,
    ) -> BoxFuture<'a, BackendResult> {
        Box::pin(async move {
            let turn_id = context.turn_id.unwrap_or_else(TurnId::generate);
            let config = match self.config(&context).await {
                Ok(config) => config,
                Err(error) => {
                    warn!(%turn_id, %error, "headless turn setup failed");
                    return BackendResult::SetupFailed(Some(TurnFailure {
                        kind: TurnFailureKind::Internal,
                        diagnostic: error.clone(),
                        user_message: error,
                        retryable: true,
                    }));
                }
            };
            let prompt_inputs = context
                .admission
                .as_ref()
                .and_then(|admission| admission.prompt_inputs.clone());
            let slots = match &prompt_inputs {
                Some(inputs) => match inputs.resolve(&mut input, self.mcp.as_ref()).await {
                    Ok(slots) => slots,
                    Err(error) => {
                        warn!(%turn_id, %error, "headless turn inputs failed");
                        return BackendResult::SetupFailed(Some(TurnFailure {
                            kind: TurnFailureKind::Internal,
                            diagnostic: error.to_string(),
                            user_message: error.user_message(),
                            retryable: false,
                        }));
                    }
                },
                None => Arc::default(),
            };
            let root_context = prompt_inputs
                .as_ref()
                .and_then(|inputs| inputs.root_context());
            let instructions = root_context
                .as_ref()
                .map(|context| context.instructions.clone())
                .unwrap_or_default();
            let slots = root_context
                .as_ref()
                .map(|context| Arc::clone(&context.slots))
                .unwrap_or(slots);
            let event_tx = self.event_sender(&context);
            context.output.set_terminal_sender(event_tx.clone());
            if let Some(answer_rx) = &self.answer_rx {
                while answer_rx.lock().await.try_recv().is_ok() {}
            }
            let mut params = self.params.clone();
            params.prompt_slots = Arc::clone(&slots);
            let mut agent = Agent::for_actor_turn(
                params,
                AgentRunParams {
                    history,
                    system: self.system(&config, &instructions, &slots),
                    event_tx: event_tx.clone(),
                    tools: self.tools(&config),
                },
                &context,
                instructions.loaded,
            )
            .with_user_response_rx(self.answer_rx.clone())
            .with_local_tools(Arc::clone(&self.local_tools))
            .with_mcp(self.mcp.clone());
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
            let control = pending.as_ref().and_then(|pending| {
                pending
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .control
                    .take()
            });
            match control {
                Some(InteractiveControl::Compact(reply)) => {
                    let result = self.compact(history, &context, None, &context.cancel).await;
                    let _ = reply.send(compaction_result(result));
                }
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
                    return BackendResult::ControlCompacted(status);
                }
                Some(InteractiveControl::Reset(reply)) => {
                    let _ = reply.send(Err(RESET_REQUIRES_FRONTEND.into()));
                    return BackendResult::ControlFailed;
                }
                Some(InteractiveControl::IsolatedTurn {
                    question,
                    images,
                    root_context: pinned_context,
                    context_access,
                    output,
                    cancel,
                }) => {
                    if let Err(error) = root_context(&context_access) {
                        let _ = output.send(IsolatedTurnEvent::Error(error.to_string()));
                        return BackendResult::ControlFailed;
                    }
                    let config = match self.config(&context).await {
                        Ok(config) => config,
                        Err(error) => {
                            let _ =
                                output.send(agent::isolated_turn::IsolatedTurnEvent::Error(error));
                            return BackendResult::ControlFailed;
                        }
                    };
                    let (trigger, combined) = CancelToken::new();
                    let cancellation = smol::spawn({
                        let actor_cancel = context.cancel.clone();
                        async move {
                            future::or(cancel.cancelled(), actor_cancel.cancelled()).await;
                            trigger.cancel();
                        }
                    });
                    agent::isolated_turn::run_isolated_turn(
                        agent::isolated_turn::IsolatedTurnRequest {
                            provider: Arc::clone(&config.provider),
                            model: config.model.clone(),
                            history: history.as_slice().to_vec(),
                            previous: history.branch(history.len()),
                            system: self.system(
                                &config,
                                &pinned_context.instructions,
                                &pinned_context.slots,
                            ),
                            question,
                            images,
                            session_id: self.params.session_id.clone(),
                            cancel: combined,
                        },
                        output,
                    )
                    .await;
                    cancellation.cancel().await;
                }
                None => return BackendResult::ControlFailed,
            }
            BackendResult::ControlDone
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
    P: FnOnce(Vec<Message>) -> F,
    F: Future<Output = Result<(), String>>,
{
    let result = match compacted {
        Ok(()) => persist(history.as_slice().to_vec()).await,
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
        self.children.cancel_all();
        if let Some(cleanup) = self.offload_cleanup() {
            cleanup.request();
        }
        let manager = self.manager.clone();
        smol::spawn(async move {
            manager.shutdown(GRAPH_SHUTDOWN_TIMEOUT).await;
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
    checkpoint: Arc<SessionLogCheckpoint>,
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
    checkpoint: Option<Arc<SessionLogCheckpoint>>,
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
    let restored_graph = !snapshot.agent_revisions.is_empty();
    let mut restored_config = None;
    let mut restored_permissions = PermissionSnapshot::default();
    let manager = if restored_graph {
        SessionStateView::fold(&snapshot.state, &snapshot.agent_revisions, snapshot.seq)
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
                if let Some(stored) = &restored_config {
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
                    params.initial_wd = PathBuf::from(&stored.cwd);
                    params.defaults = SessionDefaults {
                        fast: stored.fast,
                        workflow: stored.workflow,
                        thinking: stored.thinking,
                    };
                }
                AgentManagerHandle::restore_graph(limits, journal, view, snapshot.histories)
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
    let model = crate::SharedModel::default();
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
        session_id: session_ref.id(),
        next: Arc::new(AtomicU64::new(0)),
    };
    let (answer_tx, answer_rx) = flume::unbounded();
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
        &params.excluded_tools,
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
        let model_source = model.clone();
        let permissions = Arc::clone(&permissions);
        let mailbox = mailbox.clone();
        let session_ref = session_ref.clone();
        async move {
            let _lifetime = lifetime;
            let initialized = async {
                if let Some(error) = startup_error {
                    return Err(error);
                }
                let PreparedModel { model, provider } = prepare(params.model.clone())
                    .await
                    .map_err(|error| error.user_message())?;
                model_source.install(Arc::clone(&provider), model.clone());
                let mut mode_fallback = false;
                let config = if let Some(stored) = restored_config {
                    let restored = crate::plan_state::prepare_restore_for_agent(
                        &SessionStateView::fold(
                            &snapshot.state,
                            &snapshot.agent_revisions,
                            snapshot.seq,
                        )
                        .map_err(|error| error.to_string())?,
                        root_id,
                        &stored.mode,
                        &params.modes,
                    )
                    .map_err(|error| error.to_string())?;
                    if let Some(warning) = restored.warning {
                        mode_fallback = true;
                        warn!(%warning, "saved mode restoration fell back to build");
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
                let backend = HeadlessBackend {
                    persist_turn_results,
                    params: AgentParams {
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
                        question_mode: params.question_mode,
                        model_policy: params.model_policy,
                        providers: params.providers,
                        file_write_locks: Arc::new(crate::tools::FileWriteLocks::new()),
                        offload,
                        managed_turn: None,
                    },
                    excluded_tools: params.excluded_tools,
                    system_override: params.system_prompt_override,
                    append_system: params.append_system_prompt,
                    setup: TurnSetup {
                        registry: Arc::clone(ToolRegistry::global_arc()),
                        local_tools: Arc::clone(&params.local_tools),
                        mcp: mcp.clone(),
                        slots: params.prompt_slots,
                        modes: Arc::clone(&params.modes),
                        instructions: true,
                    },
                    local_tools: params.local_tools,
                    mcp,
                    answer_rx: params.answers.then(|| Arc::new(Mutex::new(answer_rx))),
                    raw_tx: raw_tx.clone(),
                    root: root.clone(),
                };
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
                                if mode_fallback {
                                    let checkpoint = checkpoint.ok_or_else(|| {
                                        ReactivationError::Initialization(
                                            FALLBACK_CHECKPOINT_UNAVAILABLE.into(),
                                        )
                                    })?;
                                    let commit =
                                        installed.actor.config_snapshot().ok_or_else(|| {
                                            ReactivationError::Initialization(
                                                HEADLESS_CONFIG_UNAVAILABLE.into(),
                                            )
                                        })?;
                                    let stored =
                                        stored_agent_config(&commit.config, Default::default())
                                            .map_err(|error| {
                                                ReactivationError::Initialization(error.to_string())
                                            })?;
                                    let incarnation = JournalIncarnation {
                                        agent: installed.incarnation.agent_id,
                                        reservation: installed.incarnation.reservation,
                                    };
                                    let (_startup_trigger, startup_cancel) = CancelToken::new();
                                    let expected_incarnation = incarnation.clone();
                                    let prepared = journal
                                        .prepare_with(move |current| {
                                            let mut stored = stored;
                                            if let Some(latest) =
                                                current.state.iter().rev().find_map(|record| {
                                                    match &record.change {
                                                        StateChange::Config(config)
                                                            if record.agent
                                                                == Some(incarnation.agent) =>
                                                        {
                                                            Some(config)
                                                        }
                                                        _ => None,
                                                    }
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
                                            ReactivationError::Initialization(error.to_string())
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
                                            ReactivationError::Initialization(error.to_string())
                                        })?;
                                }
                                if let Some(binding) = binding {
                                    binding(
                                        manager_for_binding,
                                        installed.agent.clone(),
                                        template,
                                        Some(installed),
                                    )
                                    .await
                                    .map_err(ReactivationError::Initialization)?;
                                }
                                Ok(())
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
            manager.shutdown(GRAPH_SHUTDOWN_TIMEOUT).await;
            if let Some(handle) = params.mcp_handle {
                handle.shutdown().await;
            }
            drain_offload(_lifetime.offload_cleanup()).await;
            drop(guard);
        }
    });
    InteractiveHandle {
        manager,
        root,
        event_rx: events.into_receiver(),
        tool_names,
        input_tx,
        answer_tx,
        control_tx,
        model,
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
    use crate::session_checkpoint::SessionLogCheckpoint;
    use crate::session_coordinator::{SessionCoordinatorParams, builtin_option_definitions};
    use crate::tools::offload::{OffloadBackend, OffloadError};
    use crate::{DoneReason, McpPromptRef, ModeRegistry, ToolOutput, TurnOutcome, TurnResult};
    use makima_providers::provider::Provider;
    use makima_providers::{
        ContentBlock, ConversationEnd, Invocation, RequestDelivery, Role, StopReason,
        StreamResponse, TokenUsage,
    };
    use makima_storage::StateDir;
    use makima_storage::checkpoint::{
        CheckpointAck, CheckpointError, CheckpointFuture, CheckpointPublication, CheckpointRequest,
        CheckpointWriter, ClaimingWriter, accept_all,
    };
    use makima_storage::session_state::{MAX_INLINE_TURN_RESULT_BYTES, StoredPayload};
    use makima_storage::sessions::{SESSIONS_DIR, Session, offload_dir};
    use std::io;
    use std::sync::Barrier;
    use std::thread;
    use test_case::test_case;

    const GATE_TIMEOUT: Duration = Duration::from_secs(10);
    const HEADLESS_PROVIDER_ERROR: &str = "headless provider failed";
    const SAVED_OUTPUT: &str = "saved output";
    const SAVE_ERROR: &str = "save failed";
    const FIRST: &str = "first provider";
    const SECOND: &str = "second provider";
    const TEST_MODEL: &str = "anthropic/claude-sonnet-4-20250514";
    const TEST_CWD: &str = "/tmp";
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
    type RecordedRequest = (&'static str, String, String, bool);

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
            _: &'a flume::Sender<makima_providers::ProviderEvent>,
            _: RequestOptions,
            _: Option<&'a SessionRef>,
            _invocation: &'a Invocation,
            _delivery: RequestDelivery,
        ) -> BoxFuture<'a, Result<StreamResponse, crate::AgentError>> {
            Box::pin(async move {
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
        requests: flume::Sender<(&'static str, String, String, bool)>,
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
            _: &'a Value,
            _: &'a flume::Sender<makima_providers::ProviderEvent>,
            options: RequestOptions,
            _: Option<&'a SessionRef>,
            _invocation: &'a Invocation,
            _delivery: RequestDelivery,
        ) -> BoxFuture<'a, Result<StreamResponse, crate::AgentError>> {
            Box::pin(async move {
                let _ = self
                    .requests
                    .send((self.name, model.spec(), system.into(), options.fast));
                Ok(response(self.name))
            })
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
                            registration: 0,
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
                bounded(handle.control_tx.ask_isolated_to(
                    actor.clone(),
                    QUESTION.into(),
                    Vec::new(),
                    cancel,
                ))
                .await
                .unwrap();
                let (_, _, system, _) = bounded(requests.recv_async()).await.unwrap();
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
            bounded(handle.control_tx.ask_isolated_to(
                actor.clone(),
                QUESTION.into(),
                Vec::new(),
                cancel,
            ))
            .await
            .unwrap();
            let (_, _, system, _) = bounded(requests.recv_async()).await.unwrap();
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
    fn isolated_control_cancellation_precedes_context_initialization() {
        smol::block_on(async {
            let (provider, requests) = recording();
            let (handle, coordinator) = fixture(Vec::new(), checkpoint(), provider);
            let root = bounded(handle.root.get()).await.unwrap();
            let actor = root.actor().unwrap();
            let (trigger, cancel) = CancelToken::new();
            trigger.cancel();
            let result = bounded(handle.control_tx.ask_isolated_to(
                actor.clone(),
                FIRST.into(),
                Vec::new(),
                cancel,
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
            excluded_tools: Vec::new(),
            mcp_handle: None,
            initial_wd: PathBuf::from(TEST_CWD),
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
            question_mode: crate::tools::QuestionMode::Headless,
            plugin_rules: Arc::default(),
            project_config: ProjectConfig::for_project(Path::new(TEST_CWD)),
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
                    Some(initialization) => coordinator.bind_initializing_actor(
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

    #[test_case(false; "publishes_build_before_readiness")]
    #[test_case(true; "publication_failure_keeps_root_unavailable")]
    fn restored_plan_fallback_is_published_before_admission(reject_publication: bool) {
        smol::block_on(async {
            let directory = tempfile::tempdir().unwrap();
            let storage = StateDir::from_path(directory.path().to_path_buf());
            let blocker = directory.path().join(SAVE_ERROR);
            std::fs::write(&blocker, SAVE_ERROR).unwrap();
            let plan_path = blocker.join(SAVED_OUTPUT);
            let mut definition = crate::ModeDef::default_for(crate::ModeId::Plan);
            definition.restrict_write_to = Some(plan_path.clone());
            let mut session = Session::<Message, TokenUsage, ToolOutput>::new(TEST_MODEL, TEST_CWD);
            let root_id = AgentId::generate();
            session.bind_root_id(root_id).unwrap();
            session
                .resolve_legacy_config(makima_storage::session_state::StoredAgentConfig {
                    model: TEST_MODEL.into(),
                    cwd: TEST_CWD.into(),
                    mode: definition.stored_snapshot(&AgentMode::Plan(plan_path)),
                    thinking: None,
                    fast: false,
                    workflow: false,
                    options: Default::default(),
                })
                .unwrap();
            session.save(&storage).unwrap();
            let id = session.id;
            let checkpoint = Arc::new(
                SessionLogCheckpoint::open_with_session(storage.clone(), session, None).unwrap(),
            );
            let recovered = checkpoint
                .recover(
                    CheckpointPublication::default(),
                    Arc::new(|publication| {
                        assert!(publication.try_publish());
                        Ok(())
                    }),
                )
                .await
                .unwrap();
            let path = storage
                .ensure_subdir(SESSIONS_DIR)
                .unwrap()
                .join(format!("{id}.jsonl"));
            if reject_publication {
                std::fs::remove_file(&path).unwrap();
                std::fs::create_dir(&path).unwrap();
            }
            let mut params = interactive_params(id, Vec::new());
            params.providers = Arc::new(makima_providers::test_util::offline_ids(&["anthropic"]));
            params.root_id = Some(root_id);
            params.journal = Some(recovered.journal());
            let (provider, requests) = recording();
            let (entered, entering) = flume::bounded(1);
            let (release, released) = flume::bounded(1);
            let saved_storage = storage.clone();
            let handle = spawn_interactive_with_checkpoint(
                params,
                Box::new(move |model| {
                    Box::pin(async move { Ok(PreparedModel { model, provider }) })
                }),
                Some(recovered.session().history_snapshot()),
                Some(Box::new(move |manager, root, _, initialization| {
                    Box::pin(async move {
                        assert!(root.actor().is_err());
                        assert!(matches!(
                            initialization
                                .unwrap()
                                .actor
                                .config_snapshot()
                                .unwrap()
                                .config
                                .mode,
                            AgentMode::Build
                        ));
                        let snapshot = manager.journal().snapshot();
                        let view = SessionStateView::fold(
                            &snapshot.state,
                            &snapshot.agent_revisions,
                            snapshot.seq,
                        )
                        .unwrap();
                        assert_eq!(
                            view.scopes[&Some(root_id)].config.as_ref().unwrap().mode.id,
                            crate::ModeId::Build.key()
                        );
                        let saved =
                            Session::<Message, TokenUsage, ToolOutput>::load(id, &saved_storage)
                                .unwrap();
                        assert_eq!(
                            saved.latest_state().unwrap().scopes[&Some(root_id)]
                                .config
                                .as_ref()
                                .unwrap()
                                .mode
                                .id,
                            crate::ModeId::Build.key()
                        );
                        entered.send_async(()).await.unwrap();
                        released.recv_async().await.unwrap();
                        Ok(())
                    })
                })),
                checkpoint,
            );
            if reject_publication {
                assert!(bounded(handle.initialized_root()).await.is_err());
                assert!(handle.manager.lookup(root_id).unwrap().actor().is_err());
            } else {
                bounded(entering.recv_async()).await.unwrap();
                assert!(handle.root.current().is_none());
                let mut admission = Box::pin(handle.input_tx.admit(first_input()));
                assert!(future::poll_once(admission.as_mut()).await.is_none());
                drop(admission);
                release.send(()).unwrap();
                let root = bounded(handle.initialized_root()).await.unwrap();
                assert!(matches!(
                    root.effective_config().unwrap().unwrap().mode,
                    AgentMode::Build
                ));
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
            let mut session = Session::<Message, TokenUsage, ToolOutput>::new(TEST_MODEL, TEST_CWD);
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
            assert_eq!(snapshot.state.len(), before.len() + 1);
            let view =
                SessionStateView::fold(&snapshot.state, &snapshot.agent_revisions, snapshot.seq)
                    .unwrap();
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
            let mut session = Session::<Message, TokenUsage, ToolOutput>::new(TEST_MODEL, TEST_CWD);
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
            let mut session = Session::<Message, TokenUsage, ToolOutput>::new(TEST_MODEL, TEST_CWD);
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
            let mut session = Session::<Message, TokenUsage, ToolOutput>::new(TEST_MODEL, TEST_CWD);
            let root_id = AgentId::generate();
            session.bind_root_id(root_id).unwrap();
            let mut params = interactive_params(session.id, Vec::new());
            params.root_id = Some(root_id);
            let writer = SessionLogCheckpoint::open_with_session(storage, session, None).unwrap();
            let (entered, entering) = flume::bounded(1);
            let (release, released) = flume::bounded::<()>(1);
            let claim = Arc::new(
                move |_: &makima_storage::checkpoint::CheckpointPublication| {
                    entered.send(()).unwrap();
                    released.recv().unwrap();
                    Err(Arc::from(SAVE_ERROR))
                },
            );
            let (prepared, preparations) = flume::unbounded();
            let startup = smol::spawn(async move {
                let recovered = writer
                    .recover(
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
            bounded(entering.recv_async()).await.unwrap();
            assert!(preparations.is_empty());
            release.send(()).unwrap();
            assert!(bounded(startup).await.is_err());
            assert!(preparations.is_empty());
        });
    }

    #[test]
    fn recovered_startup_preserves_identity_and_does_not_rerun_work() {
        smol::block_on(async {
            let directory = tempfile::tempdir().unwrap();
            let storage = StateDir::from_path(directory.path().to_path_buf());
            let root_id = AgentId::generate();
            let turn = TurnId::generate();
            let mut session = Session::<Message, makima_providers::TokenUsage, ToolOutput>::new(
                TEST_MODEL, TEST_CWD,
            );
            session.bind_root_id(root_id).unwrap();
            session.push_message(Message::user(ORIGINAL_RECOVERY_INPUT.into()));
            session
                .meta
                .queued_messages
                .push(ORIGINAL_RECOVERY_INPUT.into());
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
            let writer =
                SessionLogCheckpoint::open_with_session(storage.clone(), session, None).unwrap();
            let claim: makima_storage::checkpoint::CheckpointClaim = Arc::new(|publication| {
                assert!(publication.try_publish());
                Ok(())
            });
            let recovered = writer
                .recover(
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
            handle.task.cancel().await;
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
            let session = Session::<Message, TokenUsage, ToolOutput>::new(TEST_MODEL, TEST_CWD);
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
            let mut session = session;
            journal.snapshot().apply_to(&mut session).unwrap();
            let writer = Arc::new(
                SessionLogCheckpoint::open_with_session(storage.clone(), session, None).unwrap(),
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
                        permissions
                            .try_add_session_rule(makima_config::PermissionRule {
                                tool: makima_config::ToolKey::native(RULE_TOOL),
                                scope: Some(format!("{RULE_SCOPE}{index}")),
                                effect: makima_config::Effect::Allow,
                            })
                            .unwrap();
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
            for record in &journal.snapshot().state {
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
            let folded =
                SessionStateView::fold(&snapshot.state, &snapshot.agent_revisions, snapshot.seq)
                    .unwrap();
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
            let mut session = Session::<Message, TokenUsage, ToolOutput>::new(TEST_MODEL, TEST_CWD);
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
            journal.snapshot().apply_to(&mut session).unwrap();
            let writer = Arc::new(
                SessionLogCheckpoint::open_with_session(storage.clone(), session, None).unwrap(),
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
            let journal = Arc::new(OnceLock::<Arc<SessionJournal>>::new());
            let checkpoint_journal = Arc::clone(&journal);
            let checkpoint: Checkpoint = Arc::new(
                move |request: CheckpointRequest<crate::session_coordinator::SessionCheckpoint>| {
                    let seen = seen.clone();
                    let publishing = publishing.clone();
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
                handle.input_tx.turn_cancel(queued.clone()).cancel();
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
            if fail {
                assert!(matches!(&outcome, TurnOutcome::Failed { failure, .. }
                    if failure.user_message.contains(SAVE_ERROR)));
            } else {
                assert_eq!(outcome, sealed.outcome);
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
                bounded(first.wait()).await;
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

    #[test_case(InteractiveControl::Compact ; "compact")]
    #[test_case(InteractiveControl::Reset ; "reset")]
    fn control_dropped_on_close_replies_cancelled(
        control: fn(flume::Sender<Result<(), String>>) -> InteractiveControl,
    ) {
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
            handle.control_tx.send(control(reply)).await.unwrap();
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
                before,
                (FIRST, TEST_MODEL.to_owned(), FIRST_SYSTEM.into(), false)
            );
            assert_eq!(
                after,
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
                    Session::<Message, TokenUsage, ToolOutput>::new(TEST_MODEL, TEST_CWD);
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
                        assert_eq!(template.cwd, PathBuf::from(TEST_CWD));
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
        let mut session = Session::<Message, TokenUsage, ToolOutput>::new(TEST_MODEL, TEST_CWD);
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
