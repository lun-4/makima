//! Frontend-neutral contracts for slash commands.

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::future::{Future, poll_fn};
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::task::{Poll, Waker};

use thiserror::Error;

pub type CommandFuture<T> = Pin<Box<dyn Future<Output = T> + Send + 'static>>;

pub const MAX_COMMAND_DEPTH: usize = 8;
pub const COMPACT_COMMAND_NAME: &str = "/compact";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum TargetCapability {
    AgentTurns,
    ModelSelection,
    SessionControl,
    WorkingDirectory,
    PermissionToggles,
    ConfigToggles,
    InteractiveUi,
    ApplicationLifecycle,
    Reload,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TargetCapabilities(u16);

impl TargetCapabilities {
    pub const NONE: Self = Self(0);
    pub const ALL: Self = Self((1 << 9) - 1);

    pub const fn from_capability(capability: TargetCapability) -> Self {
        Self(1 << capability as u8)
    }

    pub const fn from_slice(capabilities: &[TargetCapability]) -> Self {
        let mut bits = 0;
        let mut index = 0;
        while index < capabilities.len() {
            bits |= 1 << capabilities[index] as u8;
            index += 1;
        }
        Self(bits)
    }

    pub const fn contains(self, capability: TargetCapability) -> bool {
        self.0 & Self::from_capability(capability).0 != 0
    }

    pub const fn contains_all(self, required: Self) -> bool {
        self.0 & required.0 == required.0
    }

    pub const fn union(self, other: Self) -> Self {
        Self(self.0 | other.0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BuiltinId {
    Tasks,
    Compact,
    New,
    Help,
    Usage,
    Queue,
    Model,
    Theme,
    Mcp,
    Login,
    Cd,
    Btw,
    Yolo,
    Thinking,
    Fast,
    Workflow,
    Exit,
    Reload,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CompletionKey {
    Model,
    Theme,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BuiltinOperation {
    OpenTasks,
    Compact,
    ResetSession,
    ToggleHelp,
    ToggleUsage,
    FocusQueue,
    OpenModelPicker,
    SetModel {
        spec: Arc<str>,
    },
    OpenThemePicker,
    SetTheme {
        name: Arc<str>,
    },
    OpenMcpPicker,
    OpenLoginPicker,
    ChangeDirectory {
        path: PathBuf,
    },
    QuickQuestion {
        question: Arc<str>,
        attachments: Arc<[CommandAttachment]>,
    },
    ToggleYolo,
    SetThinking {
        config: ThinkingConfig,
    },
    ToggleFast,
    ToggleWorkflow,
    Exit,
    Reload,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ThinkingConfig {
    Off,
    Adaptive,
    Minimal,
    Low,
    Medium,
    High,
    XHigh,
    Max,
    Budget(u32),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HostContextRequest {
    ModelSpecs,
    ThemeNames,
    WorkingDirectory,
    ThinkingConfig,
    FastModeSupported,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HostContextResponse {
    Values(Arc<[Arc<str>]>),
    WorkingDirectory(PathBuf),
    ThinkingConfig(ThinkingConfig),
    FastModeSupported(bool),
    Unavailable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BuiltinDefinition {
    pub id: BuiltinId,
    pub name: &'static str,
    pub aliases: &'static [&'static str],
    pub description: &'static str,
    pub arguments: ArgumentArity,
    pub argument_hint: Option<&'static str>,
    pub required_capabilities: TargetCapabilities,
    pub completion: Option<CompletionKey>,
}

const INTERACTIVE: TargetCapabilities =
    TargetCapabilities::from_capability(TargetCapability::InteractiveUi);
const SESSION: TargetCapabilities =
    TargetCapabilities::from_capability(TargetCapability::SessionControl);
const MODEL: TargetCapabilities =
    TargetCapabilities::from_capability(TargetCapability::ModelSelection);
const CWD: TargetCapabilities =
    TargetCapabilities::from_capability(TargetCapability::WorkingDirectory);
const AGENT: TargetCapabilities = TargetCapabilities::from_capability(TargetCapability::AgentTurns);
const PERMISSIONS: TargetCapabilities =
    TargetCapabilities::from_capability(TargetCapability::PermissionToggles);
const CONFIG: TargetCapabilities =
    TargetCapabilities::from_capability(TargetCapability::ConfigToggles);
const LIFECYCLE: TargetCapabilities =
    TargetCapabilities::from_capability(TargetCapability::ApplicationLifecycle);
const RELOAD: TargetCapabilities = TargetCapabilities::from_capability(TargetCapability::Reload);

macro_rules! builtin {
    ($id:ident, $name:expr, $aliases:expr, $description:literal, $arguments:expr, $hint:expr, $caps:expr, $completion:expr $(,)?) => {
        BuiltinDefinition {
            id: BuiltinId::$id,
            name: $name,
            aliases: $aliases,
            description: $description,
            arguments: $arguments,
            argument_hint: $hint,
            required_capabilities: $caps,
            completion: $completion,
        }
    };
}

pub const BUILTIN_COMMANDS: &[BuiltinDefinition] = &[
    builtin!(
        Tasks,
        "/tasks",
        &[],
        "Browse and search tasks",
        ArgumentArity::NONE,
        None,
        INTERACTIVE,
        None,
    ),
    builtin!(
        Compact,
        COMPACT_COMMAND_NAME,
        &[],
        "Summarize and compact conversation history",
        ArgumentArity::NONE,
        None,
        SESSION,
        None,
    ),
    builtin!(
        New,
        "/new",
        &["/clear"],
        "Start a new session",
        ArgumentArity::NONE,
        None,
        SESSION,
        None,
    ),
    builtin!(
        Help,
        "/help",
        &[],
        "Show keybindings",
        ArgumentArity::NONE,
        None,
        INTERACTIVE,
        None,
    ),
    builtin!(
        Usage,
        "/usage",
        &[],
        "Show token usage breakdown",
        ArgumentArity::NONE,
        None,
        INTERACTIVE,
        None,
    ),
    builtin!(
        Queue,
        "/queue",
        &[],
        "Remove items from queue",
        ArgumentArity::NONE,
        None,
        INTERACTIVE,
        None,
    ),
    builtin!(
        Model,
        "/model",
        &[],
        "Switch model",
        ArgumentArity::OPTIONAL,
        Some("<model>"),
        MODEL,
        Some(CompletionKey::Model),
    ),
    builtin!(
        Theme,
        "/theme",
        &[],
        "Switch color theme",
        ArgumentArity::OPTIONAL,
        Some("<theme>"),
        INTERACTIVE,
        Some(CompletionKey::Theme),
    ),
    builtin!(
        Mcp,
        "/mcp",
        &[],
        "Configure MCP servers",
        ArgumentArity::NONE,
        None,
        INTERACTIVE,
        None,
    ),
    builtin!(
        Login,
        "/login",
        &[],
        "Authenticate with an LLM provider",
        ArgumentArity::NONE,
        None,
        INTERACTIVE,
        None,
    ),
    builtin!(
        Cd,
        "/cd",
        &[],
        "Change working directory",
        ArgumentArity::OPTIONAL,
        Some("<path>"),
        CWD,
        None,
    ),
    builtin!(
        Btw,
        "/btw",
        &[],
        "Ask a quick question (no tools, no history pollution)",
        ArgumentArity::ONE_OR_MORE,
        Some("<question>"),
        AGENT,
        None,
    ),
    builtin!(
        Yolo,
        "/yolo",
        &[],
        "Toggle YOLO mode (skip all permission prompts)",
        ArgumentArity::NONE,
        None,
        PERMISSIONS,
        None,
    ),
    builtin!(
        Thinking,
        "/thinking",
        &[],
        "Toggle extended thinking (off, adaptive, effort level, or budget)",
        ArgumentArity::OPTIONAL,
        Some("<mode>"),
        INTERACTIVE,
        None,
    ),
    builtin!(
        Fast,
        "/fast",
        &[],
        "Toggle Anthropic fast mode (Opus only)",
        ArgumentArity::NONE,
        None,
        CONFIG,
        None,
    ),
    builtin!(
        Workflow,
        "/workflow",
        &[],
        "Toggle workflow mode (task callable inside code_execution)",
        ArgumentArity::NONE,
        None,
        CONFIG,
        None,
    ),
    builtin!(
        Exit,
        "/exit",
        &[],
        "Exit the application",
        ArgumentArity::NONE,
        None,
        LIFECYCLE,
        None,
    ),
    builtin!(
        Reload,
        "/reload",
        &[],
        "Reload plugins and config",
        ArgumentArity::NONE,
        None,
        RELOAD,
        None,
    ),
];

static NEXT_REGISTRY_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandSpec {
    pub name: Arc<str>,
    pub aliases: Arc<[Arc<str>]>,
    pub arguments: ArgumentArity,
    pub docs: CommandDocs,
    pub required_capabilities: TargetCapabilities,
}

impl BuiltinDefinition {
    pub fn spec(&self) -> CommandSpec {
        CommandSpec {
            name: Arc::from(self.name),
            aliases: self.aliases.iter().copied().map(Arc::from).collect(),
            arguments: self.arguments,
            docs: CommandDocs {
                summary: Arc::from(self.description),
                argument_hint: self.argument_hint.map(Arc::from),
            },
            required_capabilities: self.required_capabilities,
        }
    }
}

/// Argument count bounds. Arguments are counted by splitting the raw
/// remainder on whitespace, with no shell-like quoting: `/cd "my dir"` counts
/// as two arguments.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ArgumentArity {
    pub min: usize,
    pub max: Option<usize>,
}

impl ArgumentArity {
    pub const NONE: Self = Self::exactly(0);
    pub const OPTIONAL: Self = Self::bounded(0, 1);
    pub const ONE: Self = Self::exactly(1);
    pub const ANY: Self = Self::unbounded(0);
    pub const ONE_OR_MORE: Self = Self::unbounded(1);

    pub const fn exactly(count: usize) -> Self {
        Self {
            min: count,
            max: Some(count),
        }
    }

    pub const fn bounded(min: usize, max: usize) -> Self {
        Self {
            min,
            max: Some(max),
        }
    }

    pub const fn unbounded(min: usize) -> Self {
        Self { min, max: None }
    }

    pub fn accepts(self, count: usize) -> bool {
        count >= self.min && self.max.is_none_or(|max| count <= max)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandDocs {
    pub summary: Arc<str>,
    pub argument_hint: Option<Arc<str>>,
}

pub struct Registration {
    pub spec: CommandSpec,
    pub behavior: Arc<dyn CommandBehavior>,
    pub completion: Option<Arc<dyn CommandCompletion>>,
}

impl fmt::Debug for Registration {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Registration")
            .field("spec", &self.spec)
            .field("behavior", &"dyn CommandBehavior")
            .field(
                "completion",
                &self.completion.as_ref().map(|_| "dyn CommandCompletion"),
            )
            .finish()
    }
}

macro_rules! opaque_id {
    ($name:ident) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
        pub struct $name(RegistryId, u64);

        impl $name {
            pub(crate) const fn new(registry_id: RegistryId, value: u64) -> Self {
                Self(registry_id, value)
            }
        }
    };
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct RegistryId(u64);

opaque_id!(ProducerId);
opaque_id!(CommandId);
opaque_id!(CompletionSessionId);
opaque_id!(InvocationTargetId);

struct RegistrationRecord {
    producer_id: ProducerId,
    command_id: CommandId,
    registration: Registration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum ProducerPrecedence {
    Plugin,
    Mcp,
    Application,
    Builtin,
}

#[derive(Clone)]
pub struct CommandRegistry(Arc<RegistryInner>);

struct RegistryInner {
    id: RegistryId,
    state: Mutex<RegistryState>,
}

struct RegistryState {
    next_id: u64,
    generation: u64,
    producers: Vec<ProducerSlot>,
    winners: HashMap<String, Winner>,
    projection: Arc<[ResolvedCommand]>,
    targets: HashMap<InvocationTargetId, TargetRecord>,
    subscribers: Vec<Weak<SubscriptionCore>>,
    standard_commands_registered: bool,
    completion_sessions: HashMap<CompletionSessionId, Weak<CompletionSessionCore>>,
}

struct TargetRecord {
    capabilities: TargetCapabilities,
    host: Arc<dyn CommandHost>,
}

struct TargetCore {
    id: InvocationTargetId,
    registry: Weak<RegistryInner>,
}

#[derive(Clone)]
pub struct TargetHandle(Arc<TargetCore>);

struct SubscriptionCore {
    generation: AtomicU64,
    waker: Mutex<Option<Waker>>,
}

#[derive(Clone)]
pub struct RegistrySubscription(Arc<SubscriptionCore>);

struct ProducerSlot {
    id: ProducerId,
    precedence: ProducerPrecedence,
    creation_order: u64,
    records: Vec<Arc<RegistrationRecord>>,
    generation: u64,
}

struct CompletionSessionCore {
    id: CompletionSessionId,
    producer_id: ProducerId,
    registry: Weak<RegistryInner>,
    state: Mutex<CompletionSessionState>,
}

struct CompletionSessionOwner {
    core: Arc<CompletionSessionCore>,
}

struct CompletionSessionState {
    command: ResolvedCommand,
    provider: Arc<dyn CommandCompletion>,
    target_id: InvocationTargetId,
    next_request: u64,
    current_request: Option<CurrentCompletionRequest>,
    closed: bool,
}

struct CurrentCompletionRequest {
    id: u64,
    context: CompletionContext,
    cancellation: CancellationToken,
}

struct CompletionCallback {
    provider: Arc<dyn CommandCompletion>,
    context: CompletionContext,
    event: CompletionLifecycleEvent,
    cancellation: CancellationToken,
}

struct CompletionInvalidation {
    session: Arc<CompletionSessionCore>,
}

impl CompletionCallback {
    fn call(self) -> Result<(), CompletionError> {
        self.provider
            .lifecycle(&self.context, &self.event, &self.cancellation)
    }
}

impl CompletionInvalidation {
    fn finish(self) {
        let callback = self
            .session
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .close();
        if let Some(callback) = callback {
            let _ = callback.call();
        }
    }
}

impl CompletionSessionState {
    fn close(&mut self) -> Option<CompletionCallback> {
        if self.closed {
            return None;
        }
        self.closed = true;
        let current = self.current_request.take()?;
        current.cancellation.cancel();
        Some(CompletionCallback {
            provider: Arc::clone(&self.provider),
            context: current.context,
            event: CompletionLifecycleEvent::Cancel,
            cancellation: current.cancellation,
        })
    }
}

#[derive(Clone)]
struct Winner {
    record: Arc<RegistrationRecord>,
    canonical: bool,
    precedence: ProducerPrecedence,
    creation_order: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PresentedCommand {
    pub name: Arc<str>,
    pub description: Arc<str>,
    pub argument_hint: Option<Arc<str>>,
}

impl From<&ResolvedCommand> for PresentedCommand {
    fn from(command: &ResolvedCommand) -> Self {
        Self {
            name: Arc::from(command.invoked_name()),
            description: Arc::clone(&command.spec().docs.summary),
            argument_hint: command.spec().docs.argument_hint.clone(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct RegistrySnapshot {
    generation: u64,
    commands: Arc<[ResolvedCommand]>,
}

impl RegistrySnapshot {
    pub fn generation(&self) -> u64 {
        self.generation
    }

    pub fn commands(&self) -> &[ResolvedCommand] {
        &self.commands
    }
}

impl fmt::Debug for InputDispatch {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::LiteralInput(content) => formatter
                .debug_tuple("LiteralInput")
                .field(content)
                .finish(),
            Self::Dispatched(outcome) => {
                formatter.debug_tuple("Dispatched").field(outcome).finish()
            }
        }
    }
}

pub enum InputDispatch {
    LiteralInput(CommandContent),
    Dispatched(CommandOutcome),
}

#[derive(Clone)]
pub struct Producer {
    registry: Weak<RegistryInner>,
    id: ProducerId,
}

impl CommandRegistry {
    pub fn new() -> Self {
        Self(Arc::new(RegistryInner {
            id: RegistryId(NEXT_REGISTRY_ID.fetch_add(1, Ordering::Relaxed)),
            state: Mutex::new(RegistryState {
                next_id: 1,
                generation: 0,
                producers: Vec::new(),
                winners: HashMap::new(),
                projection: Arc::from([]),
                targets: HashMap::new(),
                subscribers: Vec::new(),
                standard_commands_registered: false,
                completion_sessions: HashMap::new(),
            }),
        }))
    }

    pub fn create_producer(&self, precedence: ProducerPrecedence) -> Producer {
        let mut state = self
            .0
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let id = ProducerId::new(self.0.id, state.take_id());
        let creation_order = state.next_id;
        state.producers.push(ProducerSlot {
            id,
            precedence,
            creation_order,
            records: Vec::new(),
            generation: 0,
        });
        Producer {
            registry: Arc::downgrade(&self.0),
            id,
        }
    }

    pub fn bind_target(
        &self,
        capabilities: TargetCapabilities,
        host: Arc<dyn CommandHost>,
    ) -> TargetHandle {
        let mut state = self
            .0
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let id = InvocationTargetId::new(self.0.id, state.take_id());
        state
            .targets
            .insert(id, TargetRecord { capabilities, host });
        TargetHandle(Arc::new(TargetCore {
            id,
            registry: Arc::downgrade(&self.0),
        }))
    }

    pub fn claim_standard_commands(&self) -> bool {
        let mut state = self
            .0
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if state.standard_commands_registered {
            return false;
        }
        state.standard_commands_registered = true;
        true
    }

    pub fn release_standard_commands(&self) {
        self.0
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .standard_commands_registered = false;
    }

    pub fn subscribe(&self) -> RegistrySubscription {
        let mut state = self
            .0
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let core = Arc::new(SubscriptionCore {
            generation: AtomicU64::new(state.generation),
            waker: Mutex::new(None),
        });
        state.subscribers.push(Arc::downgrade(&core));
        RegistrySubscription(core)
    }

    pub fn open_completion(
        &self,
        command: ResolvedCommand,
        target_id: InvocationTargetId,
    ) -> Result<CompletionSession, CompletionError> {
        if command.registry_id != self.0.id || target_id.0 != self.0.id {
            return Err(CompletionError::StaleCommand);
        }
        let provider = command.completion().ok_or(CompletionError::Unavailable)?;
        let mut state = self
            .0
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        state
            .producers
            .iter()
            .find(|producer| {
                producer.id == command.producer_id()
                    && producer
                        .records
                        .iter()
                        .any(|record| record.command_id == command.command_id())
            })
            .ok_or(CompletionError::StaleCommand)?;
        let id = CompletionSessionId::new(self.0.id, state.take_id());
        let core = Arc::new(CompletionSessionCore {
            id,
            producer_id: command.producer_id(),
            registry: Arc::downgrade(&self.0),
            state: Mutex::new(CompletionSessionState {
                command: command.clone(),
                provider,
                target_id,
                next_request: 0,
                current_request: None,
                closed: false,
            }),
        });
        state.completion_sessions.insert(id, Arc::downgrade(&core));
        Ok(CompletionSession {
            command,
            target_id,
            owner: Arc::new(CompletionSessionOwner { core }),
        })
    }

    pub fn resolve_for(
        &self,
        target: &TargetHandle,
        spelling: &str,
    ) -> Result<ResolvedCommand, ResolutionError> {
        let normalized = normalize(spelling);
        let state = self
            .0
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let capabilities =
            target_capabilities(&state, self.0.id, target).ok_or(ResolutionError::StaleTarget)?;
        state
            .winners
            .get(&normalized)
            .filter(|winner| {
                capabilities.contains_all(winner.record.registration.spec.required_capabilities)
            })
            .map(|winner| ResolvedCommand {
                registry_id: self.0.id,
                record: Arc::clone(&winner.record),
                invoked_name: Arc::from(spelling),
            })
            .ok_or_else(|| ResolutionError::UnknownCommand(Arc::from(spelling)))
    }

    pub fn snapshot_for(&self, target: &TargetHandle) -> Result<RegistrySnapshot, CommandError> {
        let state = self
            .0
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let capabilities =
            target_capabilities(&state, self.0.id, target).ok_or(CommandError::StaleTarget)?;
        let commands = state
            .projection
            .iter()
            .filter(|command| capabilities.contains_all(command.spec().required_capabilities))
            .cloned()
            .collect();
        Ok(RegistrySnapshot {
            generation: state.generation,
            commands,
        })
    }

    pub fn presented_commands(
        &self,
        target: &TargetHandle,
    ) -> Result<Arc<[PresentedCommand]>, CommandError> {
        Ok(self
            .snapshot_for(target)?
            .commands()
            .iter()
            .map(PresentedCommand::from)
            .collect())
    }

    pub fn resolves_input_for(&self, target: &TargetHandle, input: &str) -> bool {
        self.resolve_input_for(target, input).is_ok()
    }

    pub fn resolve_input_for(
        &self,
        target: &TargetHandle,
        input: &str,
    ) -> Result<ResolvedInput, ResolutionError> {
        let parsed = ParsedInput::parse(input)
            .ok_or_else(|| ResolutionError::UnknownCommand(Arc::from(input.trim())))?;
        Ok(ResolvedInput {
            command: self.resolve_for(target, parsed.name)?,
            arguments: Arc::from(parsed.arguments),
        })
    }

    pub fn dispatch_input(
        &self,
        target: &TargetHandle,
        content: CommandContent,
    ) -> CommandFuture<InputDispatch> {
        self.dispatch_input_at(target.clone(), content, 0)
    }

    pub fn dispatch_input_with_depth(
        &self,
        target: &TargetHandle,
        content: CommandContent,
        depth: usize,
    ) -> CommandFuture<InputDispatch> {
        self.dispatch_input_at(target.clone(), content, depth)
    }

    fn dispatch_input_at(
        &self,
        target: TargetHandle,
        content: CommandContent,
        depth: usize,
    ) -> CommandFuture<InputDispatch> {
        let Ok(resolved) = self.resolve_input_for(&target, &content.text) else {
            return Box::pin(async move { InputDispatch::LiteralInput(content) });
        };
        let registry = self.clone();
        Box::pin(async move {
            InputDispatch::Dispatched(
                registry
                    .dispatch_resolved(resolved.command, resolved.arguments, content, target, depth)
                    .await,
            )
        })
    }

    pub fn dispatch_command(
        &self,
        target: &TargetHandle,
        command: ResolvedCommand,
        arguments: Arc<str>,
        content: CommandContent,
    ) -> CommandFuture<CommandOutcome> {
        self.dispatch_command_with_depth(target, command, arguments, content, 0)
    }

    pub fn dispatch_command_with_depth(
        &self,
        target: &TargetHandle,
        command: ResolvedCommand,
        arguments: Arc<str>,
        content: CommandContent,
        depth: usize,
    ) -> CommandFuture<CommandOutcome> {
        self.dispatch_resolved(command, arguments, content, target.clone(), depth)
    }

    fn dispatch_resolved(
        &self,
        command: ResolvedCommand,
        arguments: Arc<str>,
        content: CommandContent,
        target: TargetHandle,
        depth: usize,
    ) -> CommandFuture<CommandOutcome> {
        let registry = self.clone();
        Box::pin(async move {
            if depth > MAX_COMMAND_DEPTH {
                return CommandOutcome::Failed(CommandError::MaximumDepth);
            }
            let (capabilities, host) = {
                let state = registry
                    .0
                    .state
                    .lock()
                    .unwrap_or_else(|error| error.into_inner());
                let Some(record) = target_record(&state, registry.0.id, &target) else {
                    return CommandOutcome::Failed(CommandError::StaleTarget);
                };
                (record.capabilities, Arc::clone(&record.host))
            };
            if command.registry_id != registry.0.id {
                return CommandOutcome::Failed(CommandError::StaleCommand);
            }
            {
                let state = registry
                    .0
                    .state
                    .lock()
                    .unwrap_or_else(|error| error.into_inner());
                let Some(winner) = state.winners.get(&normalize(command.invoked_name())) else {
                    return CommandOutcome::Failed(CommandError::StaleCommand);
                };
                if winner.record.command_id != command.command_id() {
                    return CommandOutcome::Failed(CommandError::StaleCommand);
                }
            }
            if !capabilities.contains_all(command.spec().required_capabilities) {
                return CommandOutcome::Failed(CommandError::UnavailableCommand(Arc::clone(
                    &command.spec().name,
                )));
            }
            let count = arguments.split_whitespace().count();
            if !command.spec().arguments.accepts(count) {
                return CommandOutcome::Failed(CommandError::InvalidArguments {
                    command: Arc::clone(&command.spec().name),
                    expected: command.spec().arguments,
                    actual: count,
                });
            }
            let invocation = CommandInvocation {
                command_id: command.command_id(),
                canonical_name: Arc::clone(&command.spec().name),
                invoked_name: Arc::clone(&command.invoked_name),
                arguments,
                content,
                depth,
                target,
                capabilities,
                host,
                dispatcher: InvocationDispatcher { registry },
            };
            match command.behavior().execute(invocation).await {
                Ok(outcome) => outcome,
                Err(error) => CommandOutcome::Failed(error),
            }
        })
    }
}

impl Default for CommandRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl TargetHandle {
    pub fn id(&self) -> InvocationTargetId {
        self.0.id
    }
}

impl Drop for TargetCore {
    fn drop(&mut self) {
        let Some(registry) = self.registry.upgrade() else {
            return;
        };
        registry
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .targets
            .remove(&self.id);
    }
}

impl RegistrySubscription {
    pub fn generation(&self) -> u64 {
        self.0.generation.load(Ordering::Acquire)
    }

    pub fn changed(&self, generation: u64) -> CommandFuture<u64> {
        let subscription = self.clone();
        Box::pin(poll_fn(move |context| {
            let current = subscription.generation();
            if current != generation {
                return Poll::Ready(current);
            }
            *subscription
                .0
                .waker
                .lock()
                .unwrap_or_else(|error| error.into_inner()) = Some(context.waker().clone());
            let current = subscription.generation();
            if current != generation {
                Poll::Ready(current)
            } else {
                Poll::Pending
            }
        }))
    }
}

fn target_record<'a>(
    state: &'a RegistryState,
    registry_id: RegistryId,
    target: &TargetHandle,
) -> Option<&'a TargetRecord> {
    (target.0.id.0 == registry_id)
        .then(|| state.targets.get(&target.0.id))
        .flatten()
}

fn target_capabilities(
    state: &RegistryState,
    registry_id: RegistryId,
    target: &TargetHandle,
) -> Option<TargetCapabilities> {
    target_record(state, registry_id, target).map(|record| record.capabilities)
}

impl Producer {
    pub fn id(&self) -> ProducerId {
        self.id
    }

    pub fn replace(&self, registrations: Vec<Registration>) -> Result<(), RegistrationError> {
        let validated = validate_registrations(registrations)?;
        let registry = self
            .registry
            .upgrade()
            .ok_or(RegistrationError::StaleProducer)?;
        let mut state = registry
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let position = state
            .producers
            .iter()
            .position(|producer| producer.id == self.id)
            .ok_or(RegistrationError::StaleProducer)?;
        let records = validated
            .into_iter()
            .map(|registration| {
                Arc::new(RegistrationRecord {
                    producer_id: self.id,
                    command_id: CommandId::new(registry.id, state.take_id()),
                    registration,
                })
            })
            .collect();
        state.producers[position].records = records;
        state.producers[position].generation += 1;
        state.generation += 1;
        state.rebuild();
        let wakers = state.take_subscriber_wakers();
        let invalidations = state.invalidate_completion_sessions(self.id);
        drop(state);
        for waker in wakers {
            waker.wake();
        }
        for invalidation in invalidations {
            invalidation.finish();
        }
        Ok(())
    }

    pub fn remove(&self) -> bool {
        let Some(registry) = self.registry.upgrade() else {
            return false;
        };
        let mut state = registry
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let Some(position) = state
            .producers
            .iter()
            .position(|producer| producer.id == self.id)
        else {
            return false;
        };
        state.producers.remove(position);
        state.generation += 1;
        state.rebuild();
        let wakers = state.take_subscriber_wakers();
        let invalidations = state.invalidate_completion_sessions(self.id);
        drop(state);
        for waker in wakers {
            waker.wake();
        }
        for invalidation in invalidations {
            invalidation.finish();
        }
        true
    }
}

impl RegistryState {
    fn take_id(&mut self) -> u64 {
        let id = self.next_id;
        self.next_id += 1;
        id
    }

    fn invalidate_completion_sessions(
        &mut self,
        producer_id: ProducerId,
    ) -> Vec<CompletionInvalidation> {
        let sessions = self
            .completion_sessions
            .iter()
            .filter_map(|(id, session)| session.upgrade().map(|session| (*id, session)))
            .filter(|(_, session)| session.producer_id == producer_id)
            .collect::<Vec<_>>();
        sessions
            .into_iter()
            .map(|(id, session)| {
                self.completion_sessions.remove(&id);
                CompletionInvalidation { session }
            })
            .collect()
    }

    fn take_subscriber_wakers(&mut self) -> Vec<Waker> {
        let mut wakers = Vec::new();
        self.subscribers.retain(|subscriber| {
            let Some(subscriber) = subscriber.upgrade() else {
                return false;
            };
            subscriber
                .generation
                .store(self.generation, Ordering::Release);
            if let Some(waker) = subscriber
                .waker
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .take()
            {
                wakers.push(waker);
            }
            true
        });
        wakers
    }

    fn rebuild(&mut self) {
        let mut winners = HashMap::new();
        for producer in &self.producers {
            for record in &producer.records {
                let spec = &record.registration.spec;
                insert_winner(
                    &mut winners,
                    record,
                    &spec.name,
                    true,
                    producer.precedence,
                    producer.creation_order,
                );
                for alias in spec.aliases.iter() {
                    insert_winner(
                        &mut winners,
                        record,
                        alias,
                        false,
                        producer.precedence,
                        producer.creation_order,
                    );
                }
            }
        }
        let mut producers = self.producers.iter().collect::<Vec<_>>();
        producers.sort_by_key(|producer| (producer.precedence, producer.creation_order));
        let mut projection = Vec::new();
        for producer in producers {
            for record in &producer.records {
                for spelling in std::iter::once(&record.registration.spec.name)
                    .chain(record.registration.spec.aliases.iter())
                {
                    if winners
                        .get(&normalize(spelling))
                        .is_some_and(|winner| winner.record.command_id == record.command_id)
                    {
                        projection.push(ResolvedCommand {
                            registry_id: record.command_id.0,
                            record: Arc::clone(record),
                            invoked_name: Arc::clone(spelling),
                        });
                    }
                }
            }
        }
        self.winners = winners;
        self.projection = projection.into();
    }
}

fn insert_winner(
    winners: &mut HashMap<String, Winner>,
    record: &Arc<RegistrationRecord>,
    spelling: &Arc<str>,
    canonical: bool,
    precedence: ProducerPrecedence,
    creation_order: u64,
) {
    let candidate = Winner {
        record: Arc::clone(record),
        canonical,
        precedence,
        creation_order,
    };
    let key = normalize(spelling);
    match winners.get(&key) {
        Some(current) if !candidate.precedes(current) => {}
        _ => {
            winners.insert(key, candidate);
        }
    }
}

impl Winner {
    fn precedes(&self, other: &Self) -> bool {
        (self.precedence, !self.canonical, self.creation_order)
            < (other.precedence, !other.canonical, other.creation_order)
    }
}

fn validate_registrations(
    registrations: Vec<Registration>,
) -> Result<Vec<Registration>, RegistrationError> {
    let mut spellings = HashSet::new();
    for registration in &registrations {
        validate_spelling(&registration.spec.name)
            .map_err(|_| RegistrationError::InvalidName(Arc::clone(&registration.spec.name)))?;
        if registration
            .spec
            .arguments
            .max
            .is_some_and(|max| registration.spec.arguments.min > max)
        {
            return Err(RegistrationError::InvalidArgumentArity {
                min: registration.spec.arguments.min,
                max: registration.spec.arguments.max.unwrap_or_default(),
            });
        }
        for (spelling, alias) in std::iter::once((&registration.spec.name, false))
            .chain(registration.spec.aliases.iter().map(|alias| (alias, true)))
        {
            if validate_spelling(spelling).is_err() {
                return Err(if alias {
                    RegistrationError::InvalidAlias(Arc::clone(spelling))
                } else {
                    RegistrationError::InvalidName(Arc::clone(spelling))
                });
            }
            if !spellings.insert(normalize(spelling)) {
                return Err(RegistrationError::DuplicateSpelling(Arc::clone(spelling)));
            }
        }
    }
    Ok(registrations)
}

fn validate_spelling(spelling: &str) -> Result<(), ()> {
    if spelling.len() > 1 && spelling.starts_with('/') && !spelling.chars().any(char::is_whitespace)
    {
        Ok(())
    } else {
        Err(())
    }
}

fn normalize(spelling: &str) -> String {
    spelling.to_ascii_lowercase()
}

struct ParsedInput<'a> {
    name: &'a str,
    arguments: &'a str,
}

impl<'a> ParsedInput<'a> {
    fn parse(input: &'a str) -> Option<Self> {
        let trimmed = input.trim_start();
        let name_end = trimmed.find(char::is_whitespace).unwrap_or(trimmed.len());
        let name = &trimmed[..name_end];
        name.starts_with('/').then(|| Self {
            name,
            arguments: trimmed[name_end..].trim(),
        })
    }
}

#[derive(Clone)]
pub struct ResolvedInput {
    pub command: ResolvedCommand,
    pub arguments: Arc<str>,
}

#[derive(Clone)]
pub struct ResolvedCommand {
    registry_id: RegistryId,
    record: Arc<RegistrationRecord>,
    invoked_name: Arc<str>,
}

impl ResolvedCommand {
    pub fn producer_id(&self) -> ProducerId {
        self.record.producer_id
    }

    pub fn command_id(&self) -> CommandId {
        self.record.command_id
    }

    pub fn spec(&self) -> &CommandSpec {
        &self.record.registration.spec
    }

    pub fn behavior(&self) -> Arc<dyn CommandBehavior> {
        Arc::clone(&self.record.registration.behavior)
    }

    pub fn completion(&self) -> Option<Arc<dyn CommandCompletion>> {
        self.record.registration.completion.clone()
    }

    pub fn invoked_name(&self) -> &str {
        &self.invoked_name
    }
}

impl fmt::Debug for ResolvedCommand {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ResolvedCommand")
            .field("command_id", &self.record.command_id)
            .field("spec", &self.record.registration.spec)
            .field("invoked_name", &self.invoked_name)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandAttachment {
    pub media_type: Arc<str>,
    pub data: Arc<str>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CommandContent {
    pub text: Arc<str>,
    pub attachments: Arc<[CommandAttachment]>,
}

impl From<&str> for CommandContent {
    fn from(text: &str) -> Self {
        Self {
            text: Arc::from(text),
            attachments: Arc::from([]),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PromptReference {
    pub qualified_name: Arc<str>,
    pub arguments: Arc<[(Arc<str>, Arc<str>)]>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentTurn {
    pub content: CommandContent,
    pub prompt: Option<PromptReference>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommandOutcome {
    Completed,
    AgentTurn(AgentTurn),
    Failed(CommandError),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HostRequest {
    Context(HostContextRequest),
    Builtin(BuiltinOperation),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HostResponse {
    Context(HostContextResponse),
    Completed,
    AgentTurn(AgentTurn),
}

pub trait CommandHost: Send + Sync + 'static {
    fn request(&self, request: HostRequest) -> CommandFuture<Result<HostResponse, CommandError>>;
}

pub trait CommandBehavior: Send + Sync + 'static {
    fn execute(
        &self,
        invocation: CommandInvocation,
    ) -> CommandFuture<Result<CommandOutcome, CommandError>>;
}

#[derive(Clone)]
pub struct CommandInvocation {
    pub command_id: CommandId,
    pub canonical_name: Arc<str>,
    pub invoked_name: Arc<str>,
    pub arguments: Arc<str>,
    pub content: CommandContent,
    pub depth: usize,
    target: TargetHandle,
    capabilities: TargetCapabilities,
    host: Arc<dyn CommandHost>,
    dispatcher: InvocationDispatcher,
}

impl CommandInvocation {
    pub fn host_request(
        &self,
        request: HostRequest,
    ) -> CommandFuture<Result<HostResponse, CommandError>> {
        self.host.request(request)
    }

    pub fn dispatch(&self, content: CommandContent) -> CommandFuture<InputDispatch> {
        self.dispatcher
            .dispatch(self.target.clone(), content, self.depth + 1)
    }

    pub fn target_id(&self) -> InvocationTargetId {
        self.target.id()
    }

    pub fn target_supports(&self, capability: TargetCapability) -> bool {
        self.capabilities.contains(capability)
    }
}

impl fmt::Debug for CommandInvocation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CommandInvocation")
            .field("command_id", &self.command_id)
            .field("canonical_name", &self.canonical_name)
            .field("invoked_name", &self.invoked_name)
            .field("arguments", &self.arguments)
            .field("content", &self.content)
            .field("depth", &self.depth)
            .field("target_id", &self.target.id())
            .finish_non_exhaustive()
    }
}

#[derive(Clone)]
struct InvocationDispatcher {
    registry: CommandRegistry,
}

impl InvocationDispatcher {
    fn dispatch(
        &self,
        target: TargetHandle,
        content: CommandContent,
        depth: usize,
    ) -> CommandFuture<InputDispatch> {
        self.registry.dispatch_input_at(target, content, depth)
    }
}

#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub enum RegistrationError {
    #[error("the producer is no longer registered")]
    StaleProducer,
    #[error("command name is invalid: {0}")]
    InvalidName(Arc<str>),
    #[error("command alias is invalid: {0}")]
    InvalidAlias(Arc<str>),
    #[error("command argument arity is invalid")]
    InvalidArgumentArity { min: usize, max: usize },
    #[error("duplicate command spelling: {0}")]
    DuplicateSpelling(Arc<str>),
}

#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub enum ResolutionError {
    #[error("unknown command")]
    UnknownCommand(Arc<str>),
    #[error("the command target is stale")]
    StaleTarget,
}

#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub enum CommandError {
    #[error("unknown command")]
    UnknownCommand,
    #[error("invalid arguments for {command}: expected {expected}")]
    InvalidArguments {
        command: Arc<str>,
        expected: ArgumentArity,
        actual: usize,
    },
    #[error("command is unavailable: {0}")]
    UnavailableCommand(Arc<str>),
    #[error("the resolved command is stale")]
    StaleCommand,
    #[error("the command target is stale")]
    StaleTarget,
    #[error("maximum command recursion depth exceeded")]
    MaximumDepth,
    #[error("command failed: {0}")]
    Producer(Arc<str>),
}

impl fmt::Display for ArgumentArity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.max {
            Some(max) if max == self.min => write!(formatter, "{}", self.min),
            Some(max) => write!(formatter, "{}..={max}", self.min),
            None => write!(formatter, "{} or more", self.min),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompletionContext {
    pub command_id: CommandId,
    pub canonical_name: Arc<str>,
    pub invoked_name: Arc<str>,
    pub arguments: Arc<str>,
    pub argument: Arc<str>,
    pub argument_index: usize,
    pub mode: Arc<str>,
    pub target_id: InvocationTargetId,
    pub session_id: CompletionSessionId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompletionItem {
    pub label: Arc<str>,
    pub insertion: Arc<str>,
    pub description: Option<Arc<str>>,
}

pub trait CommandCompletion: Send + Sync + 'static {
    fn complete(
        &self,
        context: CompletionContext,
        cancellation: CancellationToken,
    ) -> CommandFuture<Result<Vec<CompletionItem>, CompletionError>>;

    fn lifecycle(
        &self,
        _context: &CompletionContext,
        _event: &CompletionLifecycleEvent,
        _cancellation: &CancellationToken,
    ) -> Result<(), CompletionError> {
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompletionCandidate {
    item: CompletionItem,
    session_id: CompletionSessionId,
    request_id: u64,
}

impl CompletionCandidate {
    pub fn item(&self) -> &CompletionItem {
        &self.item
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CompletionLifecycleEvent {
    Highlight(CompletionItem),
    Accept(CompletionItem),
    Cancel,
}

#[derive(Debug, Clone, Default)]
pub struct CancellationToken(Arc<AtomicBool>);

impl CancellationToken {
    pub fn is_cancelled(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }
    fn cancel(&self) {
        self.0.store(true, Ordering::Release);
    }
}

#[derive(Clone)]
pub struct CompletionSession {
    command: ResolvedCommand,
    target_id: InvocationTargetId,
    owner: Arc<CompletionSessionOwner>,
}

impl CompletionSession {
    pub fn id(&self) -> CompletionSessionId {
        self.owner.core.id
    }
    pub fn command(&self) -> &ResolvedCommand {
        &self.command
    }
    pub fn target_id(&self) -> InvocationTargetId {
        self.target_id
    }

    pub fn complete(
        &self,
        arguments: Arc<str>,
        argument: Arc<str>,
        argument_index: usize,
        mode: Arc<str>,
    ) -> CommandFuture<CompletionResult> {
        let core = Arc::clone(&self.owner.core);
        Box::pin(async move {
            let (provider, context, cancellation, request_id) = {
                let mut state = core.state.lock().unwrap_or_else(|error| error.into_inner());
                if state.closed {
                    return CompletionResult::Stale;
                }
                let request_id = state.next_request;
                state.next_request += 1;
                if let Some(current) = state.current_request.take() {
                    current.cancellation.cancel();
                }
                let cancellation = CancellationToken::default();
                let context = CompletionContext {
                    command_id: state.command.command_id(),
                    canonical_name: Arc::clone(&state.command.spec().name),
                    invoked_name: Arc::from(state.command.invoked_name()),
                    arguments,
                    argument,
                    argument_index,
                    mode,
                    target_id: state.target_id,
                    session_id: core.id,
                };
                state.current_request = Some(CurrentCompletionRequest {
                    id: request_id,
                    context: context.clone(),
                    cancellation: cancellation.clone(),
                });
                (
                    Arc::clone(&state.provider),
                    context,
                    cancellation,
                    request_id,
                )
            };
            match provider.complete(context, cancellation.clone()).await {
                Ok(items) if !cancellation.is_cancelled() => {
                    let candidates = items
                        .into_iter()
                        .map(|item| CompletionCandidate {
                            item,
                            session_id: core.id,
                            request_id,
                        })
                        .collect();
                    CompletionResult::Items(candidates)
                }
                Ok(_) => CompletionResult::Cancelled,
                Err(
                    CompletionError::StaleCommand
                    | CompletionError::StaleSession
                    | CompletionError::StaleRequest,
                ) => CompletionResult::Stale,
                Err(CompletionError::Unavailable) => CompletionResult::Failed,
            }
        })
    }

    pub fn highlight(&self, candidate: &CompletionCandidate) -> Result<(), CompletionError> {
        self.lifecycle(
            candidate,
            CompletionLifecycleEvent::Highlight(candidate.item.clone()),
        )
    }
    pub fn accept(&self, candidate: CompletionCandidate) -> Result<(), CompletionError> {
        let callback = {
            let mut state = self
                .owner
                .core
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if state.closed || candidate.session_id != self.owner.core.id {
                return Err(CompletionError::StaleSession);
            }
            let current = state
                .current_request
                .take()
                .ok_or(CompletionError::StaleRequest)?;
            if current.id != candidate.request_id {
                state.current_request = Some(current);
                return Err(CompletionError::StaleRequest);
            }
            state.closed = true;
            CompletionCallback {
                provider: Arc::clone(&state.provider),
                context: current.context,
                event: CompletionLifecycleEvent::Accept(candidate.item),
                cancellation: current.cancellation,
            }
        };
        callback.call()
    }

    pub fn cancel(&self) -> Result<(), CompletionError> {
        let callback = self
            .owner
            .core
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .close();
        callback.map_or(Ok(()), CompletionCallback::call)
    }

    fn lifecycle(
        &self,
        candidate: &CompletionCandidate,
        event: CompletionLifecycleEvent,
    ) -> Result<(), CompletionError> {
        let callback = {
            let state = self
                .owner
                .core
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if state.closed || candidate.session_id != self.owner.core.id {
                return Err(CompletionError::StaleSession);
            }
            let current = state
                .current_request
                .as_ref()
                .ok_or(CompletionError::StaleRequest)?;
            if current.id != candidate.request_id {
                return Err(CompletionError::StaleRequest);
            }
            CompletionCallback {
                provider: Arc::clone(&state.provider),
                context: current.context.clone(),
                event,
                cancellation: current.cancellation.clone(),
            }
        };
        callback.call()
    }
}

impl Drop for CompletionSessionOwner {
    fn drop(&mut self) {
        let callback = self
            .core
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .close();
        if let Some(callback) = callback {
            let _ = callback.call();
        }
    }
}

impl Drop for CompletionSessionCore {
    fn drop(&mut self) {
        if let Some(registry) = self.registry.upgrade() {
            registry
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .completion_sessions
                .remove(&self.id);
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CompletionResult {
    Items(Vec<CompletionCandidate>),
    Stale,
    Cancelled,
    Failed,
}

#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub enum CompletionError {
    #[error("completion is unavailable")]
    Unavailable,
    #[error("the resolved command is no longer registered")]
    StaleCommand,
    #[error("the completion session is closed")]
    StaleSession,
    #[error("the completion request is stale")]
    StaleRequest,
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc;
    use std::task::{Context, Wake};
    use std::thread;
    use std::time::Duration;

    use super::*;

    const LOCK_RELEASE_TIMEOUT: Duration = Duration::from_secs(1);

    struct OutcomeBehavior(CommandOutcome);
    struct CountingBehavior(Arc<AtomicU64>);

    impl CommandBehavior for OutcomeBehavior {
        fn execute(
            &self,
            _invocation: CommandInvocation,
        ) -> CommandFuture<Result<CommandOutcome, CommandError>> {
            let outcome = self.0.clone();
            Box::pin(async move { Ok(outcome) })
        }
    }

    impl CommandBehavior for CountingBehavior {
        fn execute(
            &self,
            _invocation: CommandInvocation,
        ) -> CommandFuture<Result<CommandOutcome, CommandError>> {
            self.0.fetch_add(1, Ordering::Relaxed);
            Box::pin(async { Ok(CommandOutcome::Completed) })
        }
    }

    struct Host;

    #[derive(Default)]
    struct CompletionProbe {
        completions: AtomicU64,
        events: Mutex<Vec<CompletionLifecycleEvent>>,
        reenter: Option<CommandRegistry>,
    }

    struct BlockingCompletion {
        entered: mpsc::SyncSender<()>,
        release: Mutex<mpsc::Receiver<()>>,
        events: Mutex<Vec<CompletionLifecycleEvent>>,
    }

    struct ReentrantRegistryWaker {
        registry: CommandRegistry,
        woke: mpsc::SyncSender<()>,
    }

    impl Wake for ReentrantRegistryWaker {
        fn wake(self: Arc<Self>) {
            drop(self.registry.subscribe());
            self.woke.send(()).unwrap();
        }
    }

    impl CommandCompletion for CompletionProbe {
        fn complete(
            &self,
            _context: CompletionContext,
            _cancellation: CancellationToken,
        ) -> CommandFuture<Result<Vec<CompletionItem>, CompletionError>> {
            self.completions.fetch_add(1, Ordering::Relaxed);
            Box::pin(async {
                Ok(vec![CompletionItem {
                    label: Arc::from("candidate"),
                    insertion: Arc::from("candidate"),
                    description: None,
                }])
            })
        }

        fn lifecycle(
            &self,
            _context: &CompletionContext,
            event: &CompletionLifecycleEvent,
            _cancellation: &CancellationToken,
        ) -> Result<(), CompletionError> {
            if let Some(registry) = &self.reenter {
                drop(registry.create_producer(ProducerPrecedence::Plugin));
            }
            self.events
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .push(event.clone());
            Ok(())
        }
    }

    impl CommandCompletion for BlockingCompletion {
        fn complete(
            &self,
            _context: CompletionContext,
            _cancellation: CancellationToken,
        ) -> CommandFuture<Result<Vec<CompletionItem>, CompletionError>> {
            self.entered.send(()).unwrap();
            self.release
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .recv()
                .unwrap();
            Box::pin(async {
                Ok(vec![CompletionItem {
                    label: Arc::from("candidate"),
                    insertion: Arc::from("candidate"),
                    description: None,
                }])
            })
        }

        fn lifecycle(
            &self,
            _context: &CompletionContext,
            event: &CompletionLifecycleEvent,
            _cancellation: &CancellationToken,
        ) -> Result<(), CompletionError> {
            self.events
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .push(event.clone());
            Ok(())
        }
    }

    impl CommandHost for Host {
        fn request(
            &self,
            _request: HostRequest,
        ) -> CommandFuture<Result<HostResponse, CommandError>> {
            Box::pin(async { Ok(HostResponse::Completed) })
        }
    }

    fn registration(name: &str, capabilities: TargetCapabilities) -> Registration {
        registration_with(
            name,
            &[],
            Arc::new(OutcomeBehavior(CommandOutcome::Completed)),
            capabilities,
        )
    }

    fn registration_with(
        name: &str,
        aliases: &[&str],
        behavior: Arc<dyn CommandBehavior>,
        capabilities: TargetCapabilities,
    ) -> Registration {
        Registration {
            spec: CommandSpec {
                name: Arc::from(name),
                aliases: aliases.iter().copied().map(Arc::from).collect(),
                arguments: ArgumentArity::ANY,
                docs: CommandDocs {
                    summary: Arc::from("test command"),
                    argument_hint: None,
                },
                required_capabilities: capabilities,
            },
            behavior,
            completion: None,
        }
    }

    fn completion_registration(completion: Arc<dyn CommandCompletion>) -> Registration {
        Registration {
            completion: Some(completion),
            ..registration("/complete", TargetCapabilities::NONE)
        }
    }

    fn completion_session(
        registry: &CommandRegistry,
        producer: &Producer,
        completion: Arc<dyn CommandCompletion>,
    ) -> CompletionSession {
        producer
            .replace(vec![completion_registration(completion)])
            .unwrap();
        let target = registry.bind_target(TargetCapabilities::NONE, Arc::new(Host));
        let command = registry.resolve_for(&target, "/complete").unwrap();
        registry.open_completion(command, target.id()).unwrap()
    }

    fn complete_once(session: &CompletionSession) -> CompletionCandidate {
        let CompletionResult::Items(mut items) = futures_lite::future::block_on(session.complete(
            Arc::from(""),
            Arc::from(""),
            0,
            Arc::from("insert"),
        )) else {
            panic!("completion did not return items");
        };
        items.pop().unwrap()
    }

    #[test]
    fn resolve_input_uses_shared_parser_and_preserves_arguments() {
        let registry = CommandRegistry::new();
        let producer = registry.create_producer(ProducerPrecedence::Plugin);
        producer
            .replace(vec![registration("/test", TargetCapabilities::NONE)])
            .unwrap();
        let target = registry.bind_target(TargetCapabilities::NONE, Arc::new(Host));

        let resolved = registry
            .resolve_input_for(&target, "  /TeSt alpha  beta ")
            .unwrap();

        assert_eq!(resolved.command.invoked_name(), "/TeSt");
        assert_eq!(resolved.arguments.as_ref(), "alpha  beta");
        assert!(matches!(
            registry.resolve_input_for(&target, "literal input"),
            Err(ResolutionError::UnknownCommand(name)) if name.as_ref() == "literal input"
        ));
    }

    #[test]
    fn projection_and_dispatch_share_capability_filter() {
        let registry = CommandRegistry::new();
        let producer = registry.create_producer(ProducerPrecedence::Builtin);
        let required = TargetCapabilities::from_capability(TargetCapability::InteractiveUi);
        producer
            .replace(vec![registration("/picker", required)])
            .unwrap();
        let portable = registry.bind_target(TargetCapabilities::NONE, Arc::new(Host));
        let interactive = registry.bind_target(required, Arc::new(Host));

        assert!(registry.resolve_for(&portable, "/picker").is_err());
        assert!(registry.presented_commands(&portable).unwrap().is_empty());
        assert!(matches!(
            futures_lite::future::block_on(registry.dispatch_input(&portable, "/picker".into())),
            InputDispatch::LiteralInput(_)
        ));
        assert_eq!(registry.presented_commands(&interactive).unwrap().len(), 1);
        assert!(matches!(
            futures_lite::future::block_on(registry.dispatch_input(&interactive, "/picker".into())),
            InputDispatch::Dispatched(CommandOutcome::Completed)
        ));
    }

    #[test]
    fn portable_override_wins_over_restricted_builtin() {
        let registry = CommandRegistry::new();
        let builtin = registry.create_producer(ProducerPrecedence::Builtin);
        builtin
            .replace(vec![registration(
                "/shared",
                TargetCapabilities::from_capability(TargetCapability::InteractiveUi),
            )])
            .unwrap();
        let plugin = registry.create_producer(ProducerPrecedence::Plugin);
        plugin
            .replace(vec![registration("/shared", TargetCapabilities::NONE)])
            .unwrap();
        let target = registry.bind_target(TargetCapabilities::NONE, Arc::new(Host));

        let resolved = registry.resolve_for(&target, "/shared").unwrap();
        assert_eq!(resolved.producer_id(), plugin.id());
        assert!(matches!(
            futures_lite::future::block_on(registry.dispatch_input(&target, "/shared".into())),
            InputDispatch::Dispatched(CommandOutcome::Completed)
        ));
    }

    #[test]
    fn foreign_target_is_rejected() {
        let registry = CommandRegistry::new();
        let foreign_registry = CommandRegistry::new();
        let foreign = foreign_registry.bind_target(TargetCapabilities::NONE, Arc::new(Host));

        assert!(matches!(
            registry.snapshot_for(&foreign),
            Err(CommandError::StaleTarget)
        ));
    }

    #[test]
    fn replaced_resolved_command_is_stale() {
        let registry = CommandRegistry::new();
        let producer = registry.create_producer(ProducerPrecedence::Application);
        producer
            .replace(vec![registration("/old", TargetCapabilities::NONE)])
            .unwrap();
        let target = registry.bind_target(TargetCapabilities::NONE, Arc::new(Host));
        let resolved = registry.resolve_for(&target, "/old").unwrap();
        producer
            .replace(vec![registration("/new", TargetCapabilities::NONE)])
            .unwrap();

        let outcome = futures_lite::future::block_on(registry.dispatch_command(
            &target,
            resolved,
            Arc::from(""),
            "/old".into(),
        ));

        assert!(matches!(
            outcome,
            CommandOutcome::Failed(CommandError::StaleCommand)
        ));
    }

    #[test]
    fn producer_replacement_is_atomic_on_validation_failure() {
        let registry = CommandRegistry::new();
        let producer = registry.create_producer(ProducerPrecedence::Application);
        producer
            .replace(vec![registration("/old", TargetCapabilities::NONE)])
            .unwrap();
        let target = registry.bind_target(TargetCapabilities::NONE, Arc::new(Host));
        let generation = registry.snapshot_for(&target).unwrap().generation();
        let mut invalid = registration("/invalid", TargetCapabilities::NONE);
        invalid.spec.arguments = ArgumentArity::bounded(2, 1);

        assert!(matches!(
            producer.replace(vec![
                registration("/new", TargetCapabilities::NONE),
                invalid
            ]),
            Err(RegistrationError::InvalidArgumentArity { min: 2, max: 1 })
        ));

        let snapshot = registry.snapshot_for(&target).unwrap();
        assert_eq!(snapshot.generation(), generation);
        assert_eq!(snapshot.commands().len(), 1);
        assert_eq!(snapshot.commands()[0].spec().name.as_ref(), "/old");
        assert!(registry.resolve_for(&target, "/old").is_ok());
        assert!(registry.resolve_for(&target, "/new").is_err());
    }

    #[test]
    fn winner_selection_is_deterministic_and_shared_with_projection() {
        let precedence_registry = CommandRegistry::new();
        let precedence_target =
            precedence_registry.bind_target(TargetCapabilities::NONE, Arc::new(Host));
        for precedence in [
            ProducerPrecedence::Builtin,
            ProducerPrecedence::Application,
            ProducerPrecedence::Mcp,
            ProducerPrecedence::Plugin,
        ] {
            let producer = precedence_registry.create_producer(precedence);
            producer
                .replace(vec![registration("/precedence", TargetCapabilities::NONE)])
                .unwrap();
            assert_eq!(
                precedence_registry
                    .resolve_for(&precedence_target, "/precedence")
                    .unwrap()
                    .producer_id(),
                producer.id()
            );
        }

        let registry = CommandRegistry::new();
        let alias = registry.create_producer(ProducerPrecedence::Plugin);
        alias
            .replace(vec![registration_with(
                "/alias-owner",
                &["/shared"],
                Arc::new(OutcomeBehavior(CommandOutcome::Completed)),
                TargetCapabilities::NONE,
            )])
            .unwrap();
        let first = registry.create_producer(ProducerPrecedence::Plugin);
        first
            .replace(vec![registration("/shared", TargetCapabilities::NONE)])
            .unwrap();
        let second = registry.create_producer(ProducerPrecedence::Plugin);
        second
            .replace(vec![registration("/shared", TargetCapabilities::NONE)])
            .unwrap();
        let target = registry.bind_target(TargetCapabilities::NONE, Arc::new(Host));

        let winner = registry.resolve_for(&target, "/shared").unwrap();
        assert_eq!(winner.producer_id(), first.id());
        let projected = registry
            .snapshot_for(&target)
            .unwrap()
            .commands()
            .iter()
            .find(|command| command.invoked_name() == "/shared")
            .unwrap()
            .clone();
        assert_eq!(projected.producer_id(), winner.producer_id());

        assert!(first.remove());
        assert_eq!(
            registry
                .resolve_for(&target, "/shared")
                .unwrap()
                .producer_id(),
            second.id()
        );
        assert!(second.remove());
        assert_eq!(
            registry
                .resolve_for(&target, "/shared")
                .unwrap()
                .producer_id(),
            alias.id()
        );
    }

    #[test]
    fn dispatch_rejects_foreign_command_and_target_without_execution() {
        let registry = CommandRegistry::new();
        let producer = registry.create_producer(ProducerPrecedence::Application);
        let executions = Arc::new(AtomicU64::new(0));
        producer
            .replace(vec![registration_with(
                "/local",
                &[],
                Arc::new(CountingBehavior(Arc::clone(&executions))),
                TargetCapabilities::NONE,
            )])
            .unwrap();
        let target = registry.bind_target(TargetCapabilities::NONE, Arc::new(Host));
        let command = registry.resolve_for(&target, "/local").unwrap();
        let foreign_registry = CommandRegistry::new();
        let foreign_producer = foreign_registry.create_producer(ProducerPrecedence::Application);
        foreign_producer
            .replace(vec![registration("/foreign", TargetCapabilities::NONE)])
            .unwrap();
        let foreign_target = foreign_registry.bind_target(TargetCapabilities::NONE, Arc::new(Host));
        let foreign_command = foreign_registry
            .resolve_for(&foreign_target, "/foreign")
            .unwrap();

        assert!(matches!(
            futures_lite::future::block_on(
                registry.dispatch_input(&foreign_target, "/local".into())
            ),
            InputDispatch::LiteralInput(_)
        ));
        assert!(matches!(
            futures_lite::future::block_on(registry.dispatch_command(
                &foreign_target,
                command,
                Arc::from(""),
                "/local".into(),
            )),
            CommandOutcome::Failed(CommandError::StaleTarget)
        ));
        assert!(matches!(
            futures_lite::future::block_on(registry.dispatch_command(
                &target,
                foreign_command,
                Arc::from(""),
                "/foreign".into(),
            )),
            CommandOutcome::Failed(CommandError::StaleCommand)
        ));
        assert_eq!(executions.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn subscription_reports_final_generation() {
        let registry = CommandRegistry::new();
        let subscription = registry.subscribe();
        let initial = subscription.generation();
        let producer = registry.create_producer(ProducerPrecedence::Application);
        producer
            .replace(vec![registration("/first", TargetCapabilities::NONE)])
            .unwrap();
        producer
            .replace(vec![registration("/second", TargetCapabilities::NONE)])
            .unwrap();

        let generation = futures_lite::future::block_on(subscription.changed(initial));
        assert_eq!(generation, subscription.generation());
        assert!(generation > initial);
    }

    #[test]
    fn subscriber_waker_reenters_registry_after_lock_release() {
        let registry = CommandRegistry::new();
        let subscription = registry.subscribe();
        let initial = subscription.generation();
        let producer = registry.create_producer(ProducerPrecedence::Application);
        let (woke_tx, woke_rx) = mpsc::sync_channel(1);
        let mut changed = subscription.changed(initial);
        let waker = Waker::from(Arc::new(ReentrantRegistryWaker {
            registry: registry.clone(),
            woke: woke_tx,
        }));
        let mut context = Context::from_waker(&waker);
        assert!(matches!(changed.as_mut().poll(&mut context), Poll::Pending));
        let (done_tx, done_rx) = mpsc::sync_channel(1);

        let worker = thread::spawn(move || {
            let result = producer.replace(vec![registration("/changed", TargetCapabilities::NONE)]);
            done_tx.send(result).unwrap();
        });

        woke_rx.recv_timeout(LOCK_RELEASE_TIMEOUT).unwrap();
        done_rx.recv_timeout(LOCK_RELEASE_TIMEOUT).unwrap().unwrap();
        worker.join().unwrap();
        assert!(matches!(
            changed.as_mut().poll(&mut context),
            Poll::Ready(_)
        ));
    }

    #[test]
    fn completion_invalidation_detaches_without_locking_sessions() {
        let registry = CommandRegistry::new();
        let producer = registry.create_producer(ProducerPrecedence::Application);
        let session =
            completion_session(&registry, &producer, Arc::new(CompletionProbe::default()));
        let session_lock = session
            .owner
            .core
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let registry_for_thread = registry.clone();
        let producer_id = producer.id();
        let (detached_tx, detached_rx) = mpsc::sync_channel(1);

        let worker = thread::spawn(move || {
            let invalidations = registry_for_thread
                .0
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .invalidate_completion_sessions(producer_id);
            detached_tx.send(invalidations).unwrap();
        });

        let invalidations = detached_rx.recv_timeout(LOCK_RELEASE_TIMEOUT).unwrap();
        drop(session_lock);
        worker.join().unwrap();
        for invalidation in invalidations {
            invalidation.finish();
        }
        assert!(matches!(
            futures_lite::future::block_on(session.complete(
                Arc::from(""),
                Arc::from(""),
                0,
                Arc::from("insert"),
            )),
            CompletionResult::Stale
        ));
    }

    #[test]
    fn producer_replacement_cancels_completion_once_and_rejects_reuse() {
        let registry = CommandRegistry::new();
        let producer = registry.create_producer(ProducerPrecedence::Application);
        let probe = Arc::new(CompletionProbe::default());
        let session = completion_session(&registry, &producer, probe.clone());
        let candidate = complete_once(&session);

        producer.replace(Vec::new()).unwrap();

        assert!(matches!(
            futures_lite::future::block_on(session.complete(
                Arc::from(""),
                Arc::from(""),
                0,
                Arc::from("insert"),
            )),
            CompletionResult::Stale
        ));
        assert!(matches!(
            session.highlight(&candidate),
            Err(CompletionError::StaleSession)
        ));
        assert!(matches!(
            session.accept(candidate),
            Err(CompletionError::StaleSession)
        ));
        assert_eq!(probe.completions.load(Ordering::Relaxed), 1);
        assert_eq!(
            *probe
                .events
                .lock()
                .unwrap_or_else(|error| error.into_inner()),
            [CompletionLifecycleEvent::Cancel]
        );
        session.cancel().unwrap();
    }

    #[test]
    fn producer_removal_cancels_completion_outside_registry_lock() {
        let registry = CommandRegistry::new();
        let producer = registry.create_producer(ProducerPrecedence::Application);
        let probe = Arc::new(CompletionProbe {
            reenter: Some(registry.clone()),
            ..CompletionProbe::default()
        });
        let session = completion_session(&registry, &producer, probe.clone());
        drop(complete_once(&session));

        assert!(producer.remove());

        assert_eq!(
            *probe
                .events
                .lock()
                .unwrap_or_else(|error| error.into_inner()),
            [CompletionLifecycleEvent::Cancel]
        );
        assert!(!producer.remove());
    }

    #[test]
    fn invalidating_in_flight_completion_returns_cancelled_then_stale() {
        let registry = CommandRegistry::new();
        let producer = registry.create_producer(ProducerPrecedence::Application);
        let (entered_tx, entered_rx) = mpsc::sync_channel(1);
        let (release_tx, release_rx) = mpsc::sync_channel(1);
        let completion = Arc::new(BlockingCompletion {
            entered: entered_tx,
            release: Mutex::new(release_rx),
            events: Mutex::new(Vec::new()),
        });
        let session = completion_session(&registry, &producer, completion.clone());
        let session_for_thread = session.clone();
        let worker = thread::spawn(move || {
            futures_lite::future::block_on(session_for_thread.complete(
                Arc::from(""),
                Arc::from(""),
                0,
                Arc::from("insert"),
            ))
        });
        entered_rx.recv_timeout(LOCK_RELEASE_TIMEOUT).unwrap();

        producer.replace(Vec::new()).unwrap();
        release_tx.send(()).unwrap();

        assert_eq!(worker.join().unwrap(), CompletionResult::Cancelled);
        assert!(matches!(
            futures_lite::future::block_on(session.complete(
                Arc::from(""),
                Arc::from(""),
                0,
                Arc::from("insert"),
            )),
            CompletionResult::Stale
        ));
        assert_eq!(
            *completion
                .events
                .lock()
                .unwrap_or_else(|error| error.into_inner()),
            [CompletionLifecycleEvent::Cancel]
        );
    }

    #[test]
    fn final_session_owner_drop_cancels_once() {
        let registry = CommandRegistry::new();
        let producer = registry.create_producer(ProducerPrecedence::Application);
        let probe = Arc::new(CompletionProbe::default());
        let session = completion_session(&registry, &producer, probe.clone());
        let clone = session.clone();
        complete_once(&session);

        drop(session);
        assert!(
            probe
                .events
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .is_empty()
        );
        drop(clone);

        assert_eq!(
            *probe
                .events
                .lock()
                .unwrap_or_else(|error| error.into_inner()),
            vec![CompletionLifecycleEvent::Cancel]
        );
    }

    #[test]
    fn final_owner_drop_cancels_in_flight_completion() {
        let registry = CommandRegistry::new();
        let producer = registry.create_producer(ProducerPrecedence::Application);
        let (entered_tx, entered_rx) = mpsc::sync_channel(1);
        let (release_tx, release_rx) = mpsc::sync_channel(1);
        let completion = Arc::new(BlockingCompletion {
            entered: entered_tx,
            release: Mutex::new(release_rx),
            events: Mutex::new(Vec::new()),
        });
        let session = completion_session(&registry, &producer, completion.clone());
        let future = session.complete(Arc::from(""), Arc::from(""), 0, Arc::from("insert"));
        let worker = thread::spawn(move || futures_lite::future::block_on(future));
        entered_rx.recv_timeout(LOCK_RELEASE_TIMEOUT).unwrap();

        drop(session);
        release_tx.send(()).unwrap();

        assert_eq!(worker.join().unwrap(), CompletionResult::Cancelled);
        assert_eq!(
            *completion
                .events
                .lock()
                .unwrap_or_else(|error| error.into_inner()),
            vec![CompletionLifecycleEvent::Cancel]
        );
    }

    #[test]
    fn accepting_completion_is_terminal_and_fires_once() {
        let registry = CommandRegistry::new();
        let producer = registry.create_producer(ProducerPrecedence::Application);
        let probe = Arc::new(CompletionProbe::default());
        let session = completion_session(&registry, &producer, probe.clone());
        let candidate = complete_once(&session);
        let duplicate = candidate.clone();

        session.accept(candidate).unwrap();

        assert!(matches!(
            session.accept(duplicate),
            Err(CompletionError::StaleSession)
        ));
        assert!(matches!(
            futures_lite::future::block_on(session.complete(
                Arc::from(""),
                Arc::from(""),
                0,
                Arc::from("insert"),
            )),
            CompletionResult::Stale
        ));
        assert!(matches!(
            probe
                .events
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .as_slice(),
            [CompletionLifecycleEvent::Accept(item)] if item.insertion.as_ref() == "candidate"
        ));
    }
}
