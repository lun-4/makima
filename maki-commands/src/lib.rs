//! Frontend-neutral contracts for slash commands.

use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt;
use std::future::{Future, poll_fn};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::task::{Poll, Waker};

use thiserror::Error;

pub type CommandFuture<T> = Pin<Box<dyn Future<Output = T> + Send + 'static>>;

pub const MAX_COMMAND_DEPTH: usize = 8;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BuiltinCommand {
    pub name: &'static str,
    pub description: &'static str,
    pub max_args: usize,
    pub aliases: &'static [&'static str],
}

pub const BUILTIN_COMMANDS: &[BuiltinCommand] = &[
    BuiltinCommand {
        name: "/tasks",
        description: "Browse and search tasks",
        max_args: 0,
        aliases: &[],
    },
    BuiltinCommand {
        name: "/compact",
        description: "Summarize and compact conversation history",
        max_args: 0,
        aliases: &[],
    },
    BuiltinCommand {
        name: "/new",
        description: "Start a new session",
        max_args: 0,
        aliases: &["/clear"],
    },
    BuiltinCommand {
        name: "/help",
        description: "Show keybindings",
        max_args: 0,
        aliases: &[],
    },
    BuiltinCommand {
        name: "/usage",
        description: "Show token usage breakdown",
        max_args: 0,
        aliases: &[],
    },
    BuiltinCommand {
        name: "/queue",
        description: "Remove items from queue",
        max_args: 0,
        aliases: &[],
    },
    BuiltinCommand {
        name: "/model",
        description: "Switch model",
        max_args: 1,
        aliases: &[],
    },
    BuiltinCommand {
        name: "/theme",
        description: "Switch color theme",
        max_args: 1,
        aliases: &[],
    },
    BuiltinCommand {
        name: "/mcp",
        description: "Configure MCP servers",
        max_args: 0,
        aliases: &[],
    },
    BuiltinCommand {
        name: "/login",
        description: "Authenticate with an LLM provider",
        max_args: 0,
        aliases: &[],
    },
    BuiltinCommand {
        name: "/cd",
        description: "Change working directory",
        max_args: 1,
        aliases: &[],
    },
    BuiltinCommand {
        name: "/btw",
        description: "Ask a quick question (no tools, no history pollution)",
        max_args: usize::MAX,
        aliases: &[],
    },
    BuiltinCommand {
        name: "/yolo",
        description: "Toggle YOLO mode (skip all permission prompts)",
        max_args: 0,
        aliases: &[],
    },
    BuiltinCommand {
        name: "/thinking",
        description: "Toggle extended thinking (off, adaptive, effort level, or budget)",
        max_args: 1,
        aliases: &[],
    },
    BuiltinCommand {
        name: "/fast",
        description: "Toggle Anthropic fast mode (Opus only)",
        max_args: 0,
        aliases: &[],
    },
    BuiltinCommand {
        name: "/workflow",
        description: "Toggle workflow mode (task callable inside code_execution)",
        max_args: 0,
        aliases: &[],
    },
    BuiltinCommand {
        name: "/exit",
        description: "Exit the application",
        max_args: 0,
        aliases: &[],
    },
    BuiltinCommand {
        name: "/reload",
        description: "Reload plugins and config",
        max_args: 0,
        aliases: &[],
    },
];

static NEXT_REGISTRY_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandSpec {
    pub name: Arc<str>,
    pub aliases: Arc<[Arc<str>]>,
    pub arguments: ArgumentArity,
    pub docs: CommandDocs,
}

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
    completion_sessions: HashMap<CompletionSessionId, Weak<CompletionSessionCore>>,
    #[cfg(test)]
    invalidation_gate: Option<Arc<TestRaceGate>>,
}

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
    #[cfg(test)]
    commit_gate: Mutex<Option<Arc<TestRaceGate>>>,
    #[cfg(test)]
    lifecycle_gate: Mutex<Option<Arc<TestRaceGate>>>,
}

#[cfg(test)]
struct TestRaceGate {
    reached: std::sync::Barrier,
    resume: std::sync::Barrier,
}

#[cfg(test)]
impl TestRaceGate {
    fn new() -> Self {
        Self {
            reached: std::sync::Barrier::new(2),
            resume: std::sync::Barrier::new(2),
        }
    }

    fn wait(&self) {
        self.reached.wait();
        self.resume.wait();
    }
}

struct CompletionSessionState {
    command: ResolvedCommand,
    provider: Arc<dyn CommandCompletion>,
    target_id: InvocationTargetId,
    producer_generation: u64,
    next_request: u64,
    current_request: Option<CurrentCompletionRequest>,
    callback_in_flight: bool,
    pending_callbacks: VecDeque<LifecycleCallback>,
    closed: bool,
}

struct CurrentCompletionRequest {
    id: u64,
    context: CompletionContext,
    cancellation: CancellationToken,
    items: Option<Vec<CompletionItem>>,
}

struct LifecycleCallback {
    provider: Arc<dyn CommandCompletion>,
    context: CompletionContext,
    event: CompletionLifecycleEvent,
    cancellation: CancellationToken,
}

struct InvalidatedSession {
    session: Arc<CompletionSessionCore>,
    callback: Option<LifecycleCallback>,
}

#[derive(Clone)]
struct Winner {
    record: Arc<RegistrationRecord>,
    canonical: bool,
    precedence: ProducerPrecedence,
    creation_order: u64,
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
            Self::NotCommand => formatter.write_str("NotCommand"),
            Self::UnknownCommandInput => formatter.write_str("UnknownCommandInput"),
            Self::Dispatched(_) => formatter.write_str("Dispatched(..)"),
        }
    }
}

pub enum InputDispatch {
    NotCommand,
    UnknownCommandInput,
    Dispatched(CommandDispatch),
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
                completion_sessions: HashMap::new(),
                #[cfg(test)]
                invalidation_gate: None,
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

    pub fn create_target(&self) -> InvocationTargetId {
        let mut state = self
            .0
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        InvocationTargetId::new(self.0.id, state.take_id())
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
        let producer_generation = state
            .producers
            .iter()
            .find(|producer| {
                producer.id == command.producer_id()
                    && producer
                        .records
                        .iter()
                        .any(|record| record.command_id == command.command_id())
            })
            .map(|producer| producer.generation)
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
                producer_generation,
                next_request: 0,
                current_request: None,
                callback_in_flight: false,
                pending_callbacks: VecDeque::new(),
                closed: false,
            }),
            #[cfg(test)]
            commit_gate: Mutex::new(None),
            #[cfg(test)]
            lifecycle_gate: Mutex::new(None),
        });
        state.completion_sessions.insert(id, Arc::downgrade(&core));
        Ok(CompletionSession {
            command,
            target_id,
            core,
        })
    }

    pub fn resolve(&self, spelling: &str) -> Result<ResolvedCommand, ResolutionError> {
        let normalized = normalize(spelling);
        let state = self
            .0
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        state
            .winners
            .get(&normalized)
            .map(|winner| ResolvedCommand {
                registry_id: self.0.id,
                record: Arc::clone(&winner.record),
                invoked_name: Arc::from(spelling),
            })
            .ok_or_else(|| ResolutionError::UnknownCommand(Arc::from(spelling)))
    }

    pub fn snapshot(&self) -> RegistrySnapshot {
        let state = self
            .0
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        RegistrySnapshot {
            generation: state.generation,
            commands: Arc::clone(&state.projection),
        }
    }

    /// Whether the first whitespace-delimited token of `input` resolves to a
    /// registered command, without dispatching it. Frontends use this to
    /// reject input combinations (such as images on a known command) before
    /// any behavior runs.
    pub fn resolves_input(&self, input: &str) -> bool {
        ParsedInput::parse(input).is_some_and(|parsed| self.resolve(parsed.name).is_ok())
    }

    pub fn dispatch_input(
        &self,
        input: &str,
        depth: usize,
        target_id: InvocationTargetId,
    ) -> CommandFuture<Result<InputDispatch, CommandError>> {
        let Some(parsed) = ParsedInput::parse(input) else {
            return Box::pin(async { Ok(InputDispatch::NotCommand) });
        };
        // Only resolved commands consume dispatch budget: anything else stays
        // ordinary model text regardless of depth.
        let Ok(command) = self.resolve(parsed.name) else {
            return Box::pin(async { Ok(InputDispatch::UnknownCommandInput) });
        };
        if depth > MAX_COMMAND_DEPTH {
            return Box::pin(async { Err(CommandError::MaximumDepth) });
        }
        let arguments: Arc<str> = Arc::from(parsed.arguments);
        let count = arguments.split_whitespace().count();
        if !command.spec().arguments.accepts(count) {
            return Box::pin(async move {
                Err(CommandError::InvalidArguments {
                    command: Arc::clone(&command.spec().name),
                    expected: command.spec().arguments,
                    actual: count,
                })
            });
        }

        let registry = self.clone();
        Box::pin(async move {
            let (lifecycle, classification) = classification_channel();
            let invocation = command.invocation(
                arguments,
                depth,
                target_id,
                InvocationDispatcher::new(Arc::new(registry)),
                lifecycle.clone(),
            );
            if let Err(error) = command.behavior().execute(invocation).await {
                lifecycle.transition(CommandClassification::Failed(error.clone()));
                return Err(error);
            }
            Ok(InputDispatch::Dispatched(CommandDispatch::new(
                classification,
                lifecycle,
            )))
        })
    }
}

impl Default for CommandRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl DispatchCommands for CommandRegistry {
    fn dispatch(
        &self,
        request: DispatchRequest,
    ) -> CommandFuture<Result<CommandDispatch, CommandError>> {
        let registry = self.clone();
        Box::pin(async move {
            let Some(parsed) = ParsedInput::parse(&request.input) else {
                return Err(CommandError::Producer(Arc::from(
                    "input is not a slash command",
                )));
            };
            let command = registry
                .resolve(parsed.name)
                .map_err(|_| CommandError::UnknownCommand(Arc::from(parsed.name)))?;
            registry
                .dispatch_resolved(
                    command,
                    parsed.arguments,
                    request.depth,
                    request.target_id,
                    request.lifecycle,
                )
                .await
        })
    }
}

impl CommandRegistry {
    pub fn dispatch_command(
        &self,
        command: ResolvedCommand,
        arguments: &str,
        depth: usize,
        target_id: InvocationTargetId,
    ) -> CommandFuture<Result<CommandDispatch, CommandError>> {
        let (lifecycle, classification) = classification_channel();
        let registry = self.clone();
        let arguments = arguments.to_owned();
        Box::pin(async move {
            registry
                .dispatch_resolved(command, &arguments, depth, target_id, lifecycle.clone())
                .await?;
            Ok(CommandDispatch::new(classification, lifecycle))
        })
    }

    pub fn dispatch_resolved(
        &self,
        command: ResolvedCommand,
        arguments: &str,
        depth: usize,
        target_id: InvocationTargetId,
        lifecycle: InvocationLifecycle,
    ) -> CommandFuture<Result<CommandDispatch, CommandError>> {
        let count = arguments.split_whitespace().count();
        if depth > MAX_COMMAND_DEPTH {
            lifecycle.transition(CommandClassification::Failed(CommandError::MaximumDepth));
            return Box::pin(async { Err(CommandError::MaximumDepth) });
        }
        if !command.spec().arguments.accepts(count) {
            let error = CommandError::InvalidArguments {
                command: Arc::clone(&command.spec().name),
                expected: command.spec().arguments,
                actual: count,
            };
            lifecycle.transition(CommandClassification::Failed(error.clone()));
            return Box::pin(async move { Err(error) });
        }
        let registry = self.clone();
        let arguments: Arc<str> = Arc::from(arguments);
        Box::pin(async move {
            let invocation = command.invocation(
                arguments,
                depth,
                target_id,
                InvocationDispatcher::new(Arc::new(registry)),
                lifecycle.clone(),
            );
            command
                .behavior()
                .execute(invocation)
                .await
                .inspect_err(|error| {
                    lifecycle.transition(CommandClassification::Failed(error.clone()));
                })?;
            Ok(CommandDispatch::new(lifecycle.classification(), lifecycle))
        })
    }
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
        let callbacks = state.invalidate_completion_sessions(self.id);
        drop(state);
        invoke_invalidated_sessions(callbacks);
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
        let callbacks = state.invalidate_completion_sessions(self.id);
        drop(state);
        invoke_invalidated_sessions(callbacks);
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
    ) -> Vec<InvalidatedSession> {
        let sessions = self
            .completion_sessions
            .iter()
            .filter_map(|(id, session)| session.upgrade().map(|session| (*id, session)))
            .filter(|(_, session)| session.producer_id == producer_id)
            .collect::<Vec<_>>();
        #[cfg(test)]
        if let Some(gate) = self.invalidation_gate.take() {
            gate.wait();
        }
        let mut invalidated = Vec::new();
        for (id, session) in sessions {
            self.completion_sessions.remove(&id);
            let callback = {
                let mut state = session
                    .state
                    .lock()
                    .unwrap_or_else(|error| error.into_inner());
                if state.closed {
                    None
                } else {
                    state.closed = true;
                    take_cancel_callback(&mut state)
                        .and_then(|callback| start_or_queue_callback(&mut state, callback))
                }
            };
            invalidated.push(InvalidatedSession { session, callback });
        }
        invalidated
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
pub struct ResolvedCommand {
    registry_id: RegistryId,
    record: Arc<RegistrationRecord>,
    invoked_name: Arc<str>,
}

impl ResolvedCommand {
    fn invocation(
        &self,
        arguments: Arc<str>,
        depth: usize,
        target_id: InvocationTargetId,
        dispatcher: InvocationDispatcher,
        lifecycle: InvocationLifecycle,
    ) -> CommandInvocation {
        CommandInvocation {
            command_id: self.command_id(),
            canonical_name: Arc::clone(&self.spec().name),
            invoked_name: Arc::clone(&self.invoked_name),
            arguments,
            depth,
            target_id,
            dispatcher,
            lifecycle,
        }
    }

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

pub trait CommandBehavior: Send + Sync + 'static {
    fn execute(&self, invocation: CommandInvocation) -> CommandFuture<Result<(), CommandError>>;
}

#[derive(Clone)]
pub struct CommandInvocation {
    pub command_id: CommandId,
    pub canonical_name: Arc<str>,
    pub invoked_name: Arc<str>,
    pub arguments: Arc<str>,
    pub depth: usize,
    pub target_id: InvocationTargetId,
    pub dispatcher: InvocationDispatcher,
    pub lifecycle: InvocationLifecycle,
}

impl fmt::Debug for CommandInvocation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CommandInvocation")
            .field("command_id", &self.command_id)
            .field("canonical_name", &self.canonical_name)
            .field("invoked_name", &self.invoked_name)
            .field("arguments", &self.arguments)
            .field("depth", &self.depth)
            .field("target_id", &self.target_id)
            .finish_non_exhaustive()
    }
}

#[derive(Clone)]
pub struct InvocationDispatcher(Arc<dyn DispatchCommands>);

impl InvocationDispatcher {
    pub fn new(dispatcher: Arc<dyn DispatchCommands>) -> Self {
        Self(dispatcher)
    }

    pub fn dispatch(
        &self,
        request: DispatchRequest,
    ) -> CommandFuture<Result<CommandDispatch, CommandError>> {
        self.0.dispatch(request)
    }
}

pub trait DispatchCommands: Send + Sync + 'static {
    fn dispatch(
        &self,
        request: DispatchRequest,
    ) -> CommandFuture<Result<CommandDispatch, CommandError>>;
}

#[derive(Clone)]
pub struct DispatchRequest {
    pub input: Arc<str>,
    pub depth: usize,
    pub target_id: InvocationTargetId,
    pub lifecycle: InvocationLifecycle,
}

pub struct CommandDispatch {
    classification: CommandFuture<CommandClassification>,
    lifecycle: InvocationLifecycle,
}

impl CommandDispatch {
    pub fn new(
        classification: CommandFuture<CommandClassification>,
        lifecycle: InvocationLifecycle,
    ) -> Self {
        Self {
            classification,
            lifecycle,
        }
    }

    pub fn classification(self) -> CommandFuture<CommandClassification> {
        self.classification
    }

    pub fn lifecycle(&self) -> &InvocationLifecycle {
        &self.lifecycle
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommandClassification {
    Completed,
    AgentTurnAccepted,
    Failed(CommandError),
}

#[derive(Clone)]
pub struct InvocationLifecycle(Arc<dyn ClassifyInvocation>);

impl PartialEq for InvocationLifecycle {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

struct ClassificationState {
    inner: Mutex<ClassificationInner>,
}

struct ClassificationInner {
    classification: Option<CommandClassification>,
    waker: Option<Waker>,
}

impl ClassifyInvocation for ClassificationState {
    fn transition(&self, classification: CommandClassification) -> bool {
        let waker = {
            let mut inner = self.inner.lock().unwrap_or_else(|error| error.into_inner());
            if inner.classification.is_some() {
                return false;
            }
            inner.classification = Some(classification);
            inner.waker.take()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
        true
    }

    fn poll_classification(&self, waker: &Waker) -> Poll<CommandClassification> {
        let mut inner = self.inner.lock().unwrap_or_else(|error| error.into_inner());
        match inner.classification.clone() {
            Some(classification) => Poll::Ready(classification),
            None => {
                inner.waker = Some(waker.clone());
                Poll::Pending
            }
        }
    }
}

fn classification_channel() -> (InvocationLifecycle, CommandFuture<CommandClassification>) {
    let lifecycle = InvocationLifecycle(Arc::new(ClassificationState {
        inner: Mutex::new(ClassificationInner {
            classification: None,
            waker: None,
        }),
    }));
    let classification = lifecycle.classification();
    (lifecycle, classification)
}

impl InvocationLifecycle {
    pub fn new(lifecycle: Arc<dyn ClassifyInvocation>) -> Self {
        Self(lifecycle)
    }

    pub fn detached() -> Self {
        classification_channel().0
    }

    pub fn transition(&self, classification: CommandClassification) -> bool {
        self.0.transition(classification)
    }

    pub fn classification(&self) -> CommandFuture<CommandClassification> {
        let lifecycle = self.clone();
        Box::pin(poll_fn(move |context| {
            lifecycle.0.poll_classification(context.waker())
        }))
    }
}

pub trait ClassifyInvocation: Send + Sync + 'static {
    fn transition(&self, classification: CommandClassification) -> bool;
    fn poll_classification(&self, waker: &Waker) -> Poll<CommandClassification>;
}

#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub enum RegistrationError {
    #[error("the producer is no longer registered")]
    StaleProducer,
    #[error("command name is invalid: {0}")]
    InvalidName(Arc<str>),
    #[error("command alias is invalid: {0}")]
    InvalidAlias(Arc<str>),
    #[error("command argument range {min}..={max} is invalid")]
    InvalidArgumentArity { min: usize, max: usize },
    #[error("command spelling is registered more than once: {0}")]
    DuplicateSpelling(Arc<str>),
}

#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub enum ResolutionError {
    #[error("unknown command: {0}")]
    UnknownCommand(Arc<str>),
}

#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub enum CommandError {
    #[error("unknown command: {0}")]
    UnknownCommand(Arc<str>),
    #[error("invalid arguments for {command}: expected {expected}, got {actual}")]
    InvalidArguments {
        command: Arc<str>,
        expected: ArgumentArity,
        actual: usize,
    },
    #[error("command is not supported by this frontend: {0}")]
    UnsupportedFrontend(Arc<str>),
    #[error("the command target is no longer available")]
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

    pub(crate) fn cancel(&self) {
        self.0.store(true, Ordering::Release);
    }
}

#[derive(Clone)]
pub struct CompletionSession {
    command: ResolvedCommand,
    target_id: InvocationTargetId,
    core: Arc<CompletionSessionCore>,
}

impl CompletionSession {
    pub fn id(&self) -> CompletionSessionId {
        self.core.id
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
        let (provider, context, cancellation, request_id, producer_generation) = {
            let mut state = self
                .core
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if state.closed {
                return Box::pin(async { CompletionResult::Stale });
            }
            let superseded = take_cancel_callback(&mut state);
            state.next_request += 1;
            let request_id = state.next_request;
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
                session_id: self.core.id,
            };
            state.current_request = Some(CurrentCompletionRequest {
                id: request_id,
                context: context.clone(),
                cancellation: cancellation.clone(),
                items: None,
            });
            let request = (
                Arc::clone(&state.provider),
                context,
                cancellation,
                request_id,
                state.producer_generation,
            );
            let superseded =
                superseded.and_then(|callback| start_or_queue_callback(&mut state, callback));
            drop(state);
            let _ = self.core.invoke_lifecycle(superseded);
            request
        };
        let core = Arc::clone(&self.core);
        let request = provider.complete(context, cancellation.clone());
        let guard = PendingRequestGuard {
            core: Arc::clone(&core),
            request_id,
            active: true,
        };
        Box::pin(async move {
            let mut guard = guard;
            let result = request.await;
            #[cfg(test)]
            if let Some(gate) = core.commit_gate.lock().unwrap().take() {
                gate.wait();
            }
            let Some(registry) = core.registry.upgrade() else {
                return CompletionResult::Stale;
            };
            let registry_state = registry
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            let generation_matches = registry_state.producers.iter().any(|producer| {
                producer.id == core.producer_id && producer.generation == producer_generation
            });
            let registered = registry_state
                .completion_sessions
                .get(&core.id)
                .and_then(Weak::upgrade)
                .is_some_and(|session| Arc::ptr_eq(&session, &core));
            let mut state = core.state.lock().unwrap_or_else(|error| error.into_inner());
            if state.closed || !generation_matches || !registered {
                return CompletionResult::Stale;
            }
            let Some(current) = state.current_request.as_mut() else {
                return CompletionResult::Stale;
            };
            if current.id != request_id {
                return CompletionResult::Stale;
            }
            if cancellation.is_cancelled() {
                return CompletionResult::Cancelled;
            }
            guard.active = false;
            match result {
                Ok(items) => {
                    current.items = Some(items.clone());
                    CompletionResult::Items(
                        items
                            .into_iter()
                            .map(|item| CompletionCandidate {
                                item,
                                session_id: core.id,
                                request_id,
                            })
                            .collect(),
                    )
                }
                Err(error) => CompletionResult::Failed(error),
            }
        })
    }

    pub fn highlight(&self, candidate: &CompletionCandidate) -> Result<(), CompletionError> {
        self.core.lifecycle(candidate, false)
    }

    pub fn accept(&self, candidate: CompletionCandidate) -> Result<(), CompletionError> {
        self.core.lifecycle(&candidate, true)
    }

    pub fn cancel(&self) -> Result<(), CompletionError> {
        self.core.close()
    }
}

impl CompletionSessionCore {
    fn lifecycle(
        &self,
        candidate: &CompletionCandidate,
        terminal: bool,
    ) -> Result<(), CompletionError> {
        #[cfg(test)]
        if let Some(gate) = self.lifecycle_gate.lock().unwrap().take() {
            gate.wait();
        }
        let registry = self
            .registry
            .upgrade()
            .ok_or(CompletionError::StaleSession)?;
        let mut registry_state = registry
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let registered = registry_state
            .completion_sessions
            .get(&self.id)
            .and_then(Weak::upgrade)
            .is_some_and(|session| std::ptr::eq(session.as_ref(), self));
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let generation_matches = registry_state.producers.iter().any(|producer| {
            producer.id == self.producer_id && producer.generation == state.producer_generation
        });
        if !registered || !generation_matches {
            return Err(CompletionError::StaleSession);
        }
        if state.closed || candidate.session_id != self.id {
            return Err(CompletionError::StaleSession);
        }
        let request = state
            .current_request
            .as_ref()
            .filter(|request| request.id == candidate.request_id)
            .ok_or(CompletionError::StaleRequest)?;
        let callback = LifecycleCallback {
            provider: Arc::clone(&state.provider),
            context: request.context.clone(),
            event: if terminal {
                CompletionLifecycleEvent::Accept(candidate.item.clone())
            } else {
                CompletionLifecycleEvent::Highlight(candidate.item.clone())
            },
            cancellation: request.cancellation.clone(),
        };
        if terminal {
            state.closed = true;
            state.current_request = None;
            registry_state.completion_sessions.remove(&self.id);
        }
        let callback = start_or_queue_callback(&mut state, callback);
        drop(state);
        drop(registry_state);
        self.invoke_lifecycle(callback)
    }

    fn close(&self) -> Result<(), CompletionError> {
        let callback = {
            let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            if state.closed {
                return Err(CompletionError::StaleSession);
            }
            state.closed = true;
            take_cancel_callback(&mut state)
                .and_then(|callback| start_or_queue_callback(&mut state, callback))
        };
        self.unregister();
        self.invoke_lifecycle(callback)
    }

    fn invoke_lifecycle(&self, callback: Option<LifecycleCallback>) -> Result<(), CompletionError> {
        let Some(mut callback) = callback else {
            return Ok(());
        };
        let mut result = Ok(());
        loop {
            let callback_result = callback.provider.lifecycle(
                &callback.context,
                &callback.event,
                &callback.cancellation,
            );
            if result.is_ok() {
                result = callback_result;
            }
            let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            let Some(next) = state.pending_callbacks.pop_front() else {
                state.callback_in_flight = false;
                return result;
            };
            callback = next;
        }
    }

    fn unregister(&self) {
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

impl Drop for CompletionSessionCore {
    fn drop(&mut self) {
        self.unregister();
        let callback = take_cancel_callback(
            self.state
                .get_mut()
                .unwrap_or_else(|error| error.into_inner()),
        );
        if let Some(callback) = callback {
            let _ = callback.provider.lifecycle(
                &callback.context,
                &callback.event,
                &callback.cancellation,
            );
        }
    }
}

struct PendingRequestGuard {
    core: Arc<CompletionSessionCore>,
    request_id: u64,
    active: bool,
}

impl Drop for PendingRequestGuard {
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        let callback = {
            let mut state = self
                .core
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            state
                .current_request
                .as_ref()
                .is_some_and(|request| request.id == self.request_id)
                .then(|| take_cancel_callback(&mut state))
                .flatten()
                .and_then(|callback| start_or_queue_callback(&mut state, callback))
        };
        let _ = self.core.invoke_lifecycle(callback);
    }
}

fn take_cancel_callback(state: &mut CompletionSessionState) -> Option<LifecycleCallback> {
    let request = state.current_request.take()?;
    request.cancellation.cancel();
    Some(LifecycleCallback {
        provider: Arc::clone(&state.provider),
        context: request.context,
        event: CompletionLifecycleEvent::Cancel,
        cancellation: request.cancellation,
    })
}

fn start_or_queue_callback(
    state: &mut CompletionSessionState,
    callback: LifecycleCallback,
) -> Option<LifecycleCallback> {
    if state.callback_in_flight {
        state.pending_callbacks.push_back(callback);
        None
    } else {
        state.callback_in_flight = true;
        Some(callback)
    }
}

fn invoke_invalidated_sessions(sessions: Vec<InvalidatedSession>) {
    for InvalidatedSession { session, callback } in sessions {
        let _ = session.invoke_lifecycle(callback);
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CompletionResult {
    Items(Vec<CompletionCandidate>),
    Stale,
    Cancelled,
    Failed(CompletionError),
}

#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub enum CompletionError {
    #[error("the command has no completion provider")]
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
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{Context, Poll, Wake, Waker};

    use super::{
        ArgumentArity, BUILTIN_COMMANDS, CancellationToken, CommandBehavior, CommandClassification,
        CommandCompletion, CommandDocs, CommandFuture, CommandInvocation, CommandSpec,
        CompletionContext, CompletionItem, InvocationLifecycle, Registration,
    };

    struct WakeCounter(AtomicUsize);

    impl Wake for WakeCounter {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[test]
    fn transition_wakes_registered_poller_exactly_once() {
        let lifecycle = InvocationLifecycle::detached();
        let mut classification = lifecycle.classification();
        let counter = Arc::new(WakeCounter(AtomicUsize::new(0)));
        let waker = Waker::from(counter.clone());
        let mut context = Context::from_waker(&waker);
        assert!(classification.as_mut().poll(&mut context).is_pending());

        assert!(lifecycle.transition(CommandClassification::Completed));
        assert_eq!(counter.0.load(Ordering::SeqCst), 1);
        assert!(matches!(
            classification.as_mut().poll(&mut context),
            Poll::Ready(CommandClassification::Completed)
        ));
        assert!(!lifecycle.transition(CommandClassification::Failed(
            super::CommandError::Producer(Arc::from("late")),
        )));
    }

    struct Behavior;

    impl CommandBehavior for Behavior {
        fn execute(
            &self,
            _invocation: CommandInvocation,
        ) -> CommandFuture<Result<(), super::CommandError>> {
            Box::pin(async { Ok(()) })
        }
    }

    struct Completion;

    impl CommandCompletion for Completion {
        fn complete(
            &self,
            _context: CompletionContext,
            _cancellation: CancellationToken,
        ) -> CommandFuture<Result<Vec<CompletionItem>, super::CompletionError>> {
            Box::pin(async { Ok(Vec::new()) })
        }
    }

    #[test]
    fn builtin_metadata_matches_ui_dispatch() {
        let expected = [
            ("/tasks", 0),
            ("/compact", 0),
            ("/new", 0),
            ("/help", 0),
            ("/usage", 0),
            ("/queue", 0),
            ("/model", 1),
            ("/theme", 1),
            ("/mcp", 0),
            ("/login", 0),
            ("/cd", 1),
            ("/btw", usize::MAX),
            ("/yolo", 0),
            ("/thinking", 1),
            ("/fast", 0),
            ("/workflow", 0),
            ("/exit", 0),
            ("/reload", 0),
        ];

        assert_eq!(BUILTIN_COMMANDS.len(), expected.len());
        for (command, expected) in BUILTIN_COMMANDS.iter().zip(expected) {
            assert_eq!((command.name, command.max_args), expected);
        }
        assert_eq!(BUILTIN_COMMANDS[2].aliases, ["/clear"]);
    }

    #[test]
    fn standard_arities_accept_expected_counts() {
        assert!(ArgumentArity::NONE.accepts(0));
        assert!(!ArgumentArity::NONE.accepts(1));
        assert!(ArgumentArity::OPTIONAL.accepts(0));
        assert!(ArgumentArity::OPTIONAL.accepts(1));
        assert!(!ArgumentArity::OPTIONAL.accepts(2));
        assert!(!ArgumentArity::ONE_OR_MORE.accepts(0));
        assert!(ArgumentArity::ONE_OR_MORE.accepts(2));
    }

    #[test]
    fn registration_contains_behavior_and_completion() {
        let registration = Registration {
            spec: CommandSpec {
                name: Arc::from("/test"),
                aliases: Arc::from([Arc::from("/alias")]),
                arguments: ArgumentArity::ANY,
                docs: CommandDocs {
                    summary: Arc::from("Test command"),
                    argument_hint: Some(Arc::from("[value]")),
                },
            },
            behavior: Arc::new(Behavior),
            completion: Some(Arc::new(Completion)),
        };

        assert_eq!(registration.spec.name.as_ref(), "/test");
        assert!(registration.completion.is_some());
    }

    #[test]
    fn cancellation_is_shared_and_idempotent() {
        let token = CancellationToken::default();
        let observer = token.clone();
        token.cancel();
        token.cancel();
        assert!(observer.is_cancelled());
    }

    #[test]
    fn opaque_ids_are_distinct_types() {
        let registry = super::RegistryId(1);
        let producer = super::ProducerId::new(registry, 1);
        let command = super::CommandId::new(registry, 1);
        assert_eq!(producer, super::ProducerId::new(registry, 1));
        assert_eq!(command, super::CommandId::new(registry, 1));
    }

    fn registration(name: &str, aliases: &[&str], arity: ArgumentArity) -> Registration {
        Registration {
            spec: CommandSpec {
                name: Arc::from(name),
                aliases: aliases.iter().map(|alias| Arc::from(*alias)).collect(),
                arguments: arity,
                docs: CommandDocs {
                    summary: Arc::from("test"),
                    argument_hint: None,
                },
            },
            behavior: Arc::new(Behavior),
            completion: None,
        }
    }

    #[test]
    fn alias_tie_break_survives_producer_removal() {
        let registry = super::CommandRegistry::new();
        let canonical = registry.create_producer(super::ProducerPrecedence::Builtin);
        canonical
            .replace(vec![registration("/dup", &[], ArgumentArity::ANY)])
            .unwrap();
        let alias = registry.create_producer(super::ProducerPrecedence::Builtin);
        alias
            .replace(vec![registration("/other", &["/dup"], ArgumentArity::ANY)])
            .unwrap();

        // Canonical beats alias regardless of producer age.
        let winner = registry.resolve("/DUP").unwrap();
        assert_eq!(winner.spec().name.as_ref(), "/dup");

        // After the canonical holder is removed, the older alias producer
        // wins over a same-precedence latecomer.
        canonical.remove();
        let latecomer = registry.create_producer(super::ProducerPrecedence::Builtin);
        latecomer
            .replace(vec![registration("/third", &["/dup"], ArgumentArity::ANY)])
            .unwrap();
        let winner = registry.resolve("/DUP").unwrap();
        assert_eq!(winner.spec().name.as_ref(), "/other");

        alias.remove();
        let winner = registry.resolve("/DUP").unwrap();
        assert_eq!(winner.spec().name.as_ref(), "/third");
    }

    #[test]
    fn replacement_is_atomic() {
        let registry = super::CommandRegistry::new();
        let producer = registry.create_producer(super::ProducerPrecedence::Builtin);
        producer
            .replace(vec![registration("/old", &[], ArgumentArity::ANY)])
            .unwrap();
        let generation = registry.snapshot().generation();

        producer
            .replace(vec![
                registration("/new", &[], ArgumentArity::ANY),
                registration("/other", &[], ArgumentArity::ANY),
            ])
            .unwrap();

        assert_eq!(registry.snapshot().generation(), generation + 1);
        assert!(registry.resolve("/old").is_err());
        assert!(registry.resolve("/new").is_ok());
        assert!(registry.resolve("/other").is_ok());
    }

    #[test]
    fn invalid_replacement_preserves_generation() {
        let registry = super::CommandRegistry::new();
        let producer = registry.create_producer(super::ProducerPrecedence::Builtin);
        producer
            .replace(vec![registration("/valid", &[], ArgumentArity::ANY)])
            .unwrap();
        let before = registry.snapshot();

        assert!(
            producer
                .replace(vec![registration("invalid", &[], ArgumentArity::ANY)])
                .is_err()
        );

        assert_eq!(registry.snapshot().generation(), before.generation());
        assert!(registry.resolve("/valid").is_ok());
    }

    #[test]
    fn duplicate_normalized_spelling_is_rejected() {
        let registry = super::CommandRegistry::new();
        let producer = registry.create_producer(super::ProducerPrecedence::Builtin);
        let error = producer
            .replace(vec![registration("/test", &["/TEST"], ArgumentArity::ANY)])
            .unwrap_err();
        assert!(matches!(
            error,
            super::RegistrationError::DuplicateSpelling(_)
        ));
    }

    #[test]
    fn precedence_matrix_uses_one_winner() {
        let registry = super::CommandRegistry::new();
        let builtin = registry.create_producer(super::ProducerPrecedence::Builtin);
        let application = registry.create_producer(super::ProducerPrecedence::Application);
        let mcp = registry.create_producer(super::ProducerPrecedence::Mcp);
        let plugin = registry.create_producer(super::ProducerPrecedence::Plugin);
        for producer in [&builtin, &application, &mcp, &plugin] {
            producer
                .replace(vec![registration("/same", &[], ArgumentArity::ANY)])
                .unwrap();
        }
        assert_eq!(
            registry.resolve("/same").unwrap().producer_id(),
            plugin.id()
        );
    }

    #[test]
    fn canonical_beats_alias_at_equal_precedence() {
        let registry = super::CommandRegistry::new();
        let alias = registry.create_producer(super::ProducerPrecedence::Application);
        let canonical = registry.create_producer(super::ProducerPrecedence::Application);
        alias
            .replace(vec![registration("/other", &["/same"], ArgumentArity::ANY)])
            .unwrap();
        canonical
            .replace(vec![registration("/same", &[], ArgumentArity::ANY)])
            .unwrap();
        assert_eq!(
            registry.resolve("/same").unwrap().producer_id(),
            canonical.id()
        );
    }

    #[test]
    fn creation_order_breaks_equal_ties() {
        let registry = super::CommandRegistry::new();
        let first = registry.create_producer(super::ProducerPrecedence::Application);
        let second = registry.create_producer(super::ProducerPrecedence::Application);
        first
            .replace(vec![registration("/same", &[], ArgumentArity::ANY)])
            .unwrap();
        second
            .replace(vec![registration("/same", &[], ArgumentArity::ANY)])
            .unwrap();
        assert_eq!(registry.resolve("/same").unwrap().producer_id(), first.id());
    }

    #[test]
    fn palette_and_resolve_share_winners() {
        let registry = super::CommandRegistry::new();
        let builtin = registry.create_producer(super::ProducerPrecedence::Builtin);
        let plugin = registry.create_producer(super::ProducerPrecedence::Plugin);
        builtin
            .replace(vec![registration("/same", &[], ArgumentArity::ANY)])
            .unwrap();
        plugin
            .replace(vec![registration("/same", &[], ArgumentArity::ANY)])
            .unwrap();
        let resolved = registry.resolve("/same").unwrap();
        let snapshot = registry.snapshot();
        assert_eq!(snapshot.commands().len(), 1);
        assert_eq!(snapshot.commands()[0].command_id(), resolved.command_id());
    }

    #[test]
    fn snapshot_contains_each_winning_spelling_in_deterministic_order() {
        let registry = super::CommandRegistry::new();
        let builtin = registry.create_producer(super::ProducerPrecedence::Builtin);
        let plugin = registry.create_producer(super::ProducerPrecedence::Plugin);
        builtin
            .replace(vec![registration(
                "/builtin",
                &["/shared", "/builtin-alias"],
                ArgumentArity::ANY,
            )])
            .unwrap();
        plugin
            .replace(vec![
                registration("/plugin", &["/shared", "/plugin-alias"], ArgumentArity::ANY),
                registration("/second", &["/second-alias"], ArgumentArity::ANY),
            ])
            .unwrap();

        let snapshot = registry.snapshot();
        let spellings = snapshot
            .commands()
            .iter()
            .map(|command| command.invoked_name())
            .collect::<Vec<_>>();
        assert_eq!(
            spellings,
            [
                "/plugin",
                "/shared",
                "/plugin-alias",
                "/second",
                "/second-alias",
                "/builtin",
                "/builtin-alias",
            ]
        );
        assert!(
            snapshot
                .commands()
                .iter()
                .filter(|command| command.invoked_name() == "/shared")
                .all(|command| command.producer_id() == plugin.id())
        );
    }

    #[test]
    fn removing_winner_restores_colliding_spelling() {
        let registry = super::CommandRegistry::new();
        let builtin = registry.create_producer(super::ProducerPrecedence::Builtin);
        let plugin = registry.create_producer(super::ProducerPrecedence::Plugin);
        builtin
            .replace(vec![registration("/shared", &[], ArgumentArity::ANY)])
            .unwrap();
        plugin
            .replace(vec![registration(
                "/plugin",
                &["/shared"],
                ArgumentArity::ANY,
            )])
            .unwrap();

        assert_eq!(
            registry.resolve("/shared").unwrap().producer_id(),
            plugin.id()
        );
        assert!(plugin.remove());

        let resolved = registry.resolve("/shared").unwrap();
        assert_eq!(resolved.producer_id(), builtin.id());
        assert_eq!(
            registry
                .snapshot()
                .commands()
                .iter()
                .map(|command| command.invoked_name())
                .collect::<Vec<_>>(),
            ["/shared"]
        );
    }

    #[test]
    fn owned_resolution_executes_after_replacement() {
        let registry = super::CommandRegistry::new();
        let producer = registry.create_producer(super::ProducerPrecedence::Builtin);
        producer
            .replace(vec![registration("/old", &[], ArgumentArity::ANY)])
            .unwrap();
        let resolved = registry.resolve("/old").unwrap();
        producer.replace(Vec::new()).unwrap();
        let invocation = CommandInvocation {
            command_id: resolved.command_id(),
            canonical_name: Arc::clone(&resolved.spec().name),
            invoked_name: Arc::from("/old"),
            arguments: Arc::from(""),
            depth: 0,
            target_id: registry.create_target(),
            dispatcher: super::InvocationDispatcher::new(Arc::new(registry)),
            lifecycle: super::classification_channel().0,
        };
        futures_lite::future::block_on(resolved.behavior().execute(invocation)).unwrap();
    }

    #[test]
    fn removed_command_is_not_resolved_again() {
        let registry = super::CommandRegistry::new();
        let producer = registry.create_producer(super::ProducerPrecedence::Builtin);
        producer
            .replace(vec![registration("/gone", &[], ArgumentArity::ANY)])
            .unwrap();
        assert!(producer.remove());
        assert!(registry.resolve("/gone").is_err());
        assert!(!producer.remove());
    }

    #[test]
    fn input_parsing_preserves_remainder_and_validates_arity() {
        struct Capture(Arc<std::sync::Mutex<Option<Arc<str>>>>);
        impl CommandBehavior for Capture {
            fn execute(
                &self,
                invocation: CommandInvocation,
            ) -> CommandFuture<Result<(), super::CommandError>> {
                *self.0.lock().unwrap() = Some(invocation.arguments);
                invocation
                    .lifecycle
                    .transition(super::CommandClassification::Completed);
                Box::pin(async { Ok(()) })
            }
        }
        let captured = Arc::new(std::sync::Mutex::new(None));
        let registry = super::CommandRegistry::new();
        let producer = registry.create_producer(super::ProducerPrecedence::Builtin);
        let mut command = registration("/run", &[], ArgumentArity::ONE);
        command.behavior = Arc::new(Capture(Arc::clone(&captured)));
        producer.replace(vec![command]).unwrap();
        let target = registry.create_target();

        let dispatched =
            futures_lite::future::block_on(registry.dispatch_input("  /RUN   value  ", 0, target))
                .unwrap();
        assert!(matches!(dispatched, super::InputDispatch::Dispatched(_)));
        assert_eq!(captured.lock().unwrap().as_deref(), Some("value"));
        let error =
            futures_lite::future::block_on(registry.dispatch_input("/run one two", 0, target))
                .unwrap_err();
        assert!(matches!(
            error,
            super::CommandError::InvalidArguments { actual: 2, .. }
        ));
    }

    #[test]
    fn unknown_and_non_command_inputs_are_distinct() {
        let registry = super::CommandRegistry::new();
        let target = registry.create_target();
        assert!(matches!(
            futures_lite::future::block_on(registry.dispatch_input("hello", 0, target)).unwrap(),
            super::InputDispatch::NotCommand
        ));
        assert!(matches!(
            futures_lite::future::block_on(registry.dispatch_input("/unknown text", 0, target))
                .unwrap(),
            super::InputDispatch::UnknownCommandInput
        ));
    }

    fn completion_item(label: &str) -> CompletionItem {
        CompletionItem {
            label: Arc::from(label),
            insertion: Arc::from(label),
            description: None,
        }
    }

    struct ControlledCompletion {
        requests: std::sync::mpsc::Sender<(Arc<str>, CancellationToken)>,
        responses: std::sync::Mutex<
            std::collections::HashMap<String, std::sync::mpsc::Receiver<Vec<CompletionItem>>>,
        >,
        events: std::sync::mpsc::Sender<super::CompletionLifecycleEvent>,
        on_lifecycle: Option<Arc<dyn Fn() + Send + Sync>>,
    }

    impl CommandCompletion for ControlledCompletion {
        fn complete(
            &self,
            context: CompletionContext,
            cancellation: CancellationToken,
        ) -> CommandFuture<Result<Vec<CompletionItem>, super::CompletionError>> {
            let response = self
                .responses
                .lock()
                .unwrap()
                .remove(context.argument.as_ref())
                .unwrap();
            self.requests
                .send((context.argument, cancellation))
                .unwrap();
            Box::pin(async move { Ok(response.recv().unwrap()) })
        }

        fn lifecycle(
            &self,
            _context: &CompletionContext,
            event: &super::CompletionLifecycleEvent,
            _cancellation: &CancellationToken,
        ) -> Result<(), super::CompletionError> {
            self.events.send(event.clone()).unwrap();
            if let Some(callback) = &self.on_lifecycle {
                callback();
            }
            Ok(())
        }
    }

    fn completion_registration(completion: Arc<dyn CommandCompletion>) -> Registration {
        let mut registration = registration("/complete", &[], ArgumentArity::ANY);
        registration.completion = Some(completion);
        registration
    }

    #[test]
    fn superseded_completion_is_stale() {
        let (requests_tx, requests_rx) = std::sync::mpsc::channel();
        let (events_tx, _events_rx) = std::sync::mpsc::channel();
        let (first_tx, first_rx) = std::sync::mpsc::channel();
        let (second_tx, second_rx) = std::sync::mpsc::channel();
        let provider = Arc::new(ControlledCompletion {
            requests: requests_tx,
            responses: std::sync::Mutex::new(std::collections::HashMap::from([
                ("first".to_owned(), first_rx),
                ("second".to_owned(), second_rx),
            ])),
            events: events_tx,
            on_lifecycle: None,
        });
        let registry = super::CommandRegistry::new();
        let producer = registry.create_producer(super::ProducerPrecedence::Builtin);
        producer
            .replace(vec![completion_registration(provider)])
            .unwrap();
        let session = registry
            .open_completion(
                registry.resolve("/complete").unwrap(),
                registry.create_target(),
            )
            .unwrap();
        let first_session = session.clone();
        let first = std::thread::spawn(move || {
            futures_lite::future::block_on(first_session.complete(
                Arc::from("first"),
                Arc::from("first"),
                0,
                Arc::from("test"),
            ))
        });
        let (_, first_cancellation) = requests_rx.recv().unwrap();
        let second_session = session.clone();
        let second = std::thread::spawn(move || {
            futures_lite::future::block_on(second_session.complete(
                Arc::from("second"),
                Arc::from("second"),
                0,
                Arc::from("test"),
            ))
        });
        requests_rx.recv().unwrap();
        assert!(first_cancellation.is_cancelled());
        first_tx.send(vec![completion_item("old")]).unwrap();
        second_tx.send(vec![completion_item("new")]).unwrap();
        assert_eq!(first.join().unwrap(), super::CompletionResult::Stale);
        let super::CompletionResult::Items(items) = second.join().unwrap() else {
            panic!("expected completion items");
        };
        assert_eq!(items[0].item(), &completion_item("new"));
        assert!(session.accept(items[0].clone()).is_ok());
    }

    #[test]
    fn dropping_pending_request_cancels_once() {
        let (requests_tx, requests_rx) = std::sync::mpsc::channel();
        let (events_tx, events_rx) = std::sync::mpsc::channel();
        let (_response_tx, response_rx) = std::sync::mpsc::channel();
        let provider = Arc::new(ControlledCompletion {
            requests: requests_tx,
            responses: std::sync::Mutex::new(std::collections::HashMap::from([(
                "value".to_owned(),
                response_rx,
            )])),
            events: events_tx,
            on_lifecycle: None,
        });
        let registry = super::CommandRegistry::new();
        let producer = registry.create_producer(super::ProducerPrecedence::Builtin);
        producer
            .replace(vec![completion_registration(provider)])
            .unwrap();
        let session = registry
            .open_completion(
                registry.resolve("/complete").unwrap(),
                registry.create_target(),
            )
            .unwrap();
        let request =
            session.complete(Arc::from("value"), Arc::from("value"), 0, Arc::from("test"));
        let (_, cancellation) = requests_rx.recv().unwrap();
        drop(request);
        assert!(cancellation.is_cancelled());
        assert_eq!(
            events_rx.recv().unwrap(),
            super::CompletionLifecycleEvent::Cancel
        );
        assert!(events_rx.try_recv().is_err());
    }

    #[test]
    fn superseded_candidate_cannot_be_highlighted() {
        let (requests_tx, requests_rx) = std::sync::mpsc::channel();
        let (events_tx, events_rx) = std::sync::mpsc::channel();
        let (first_tx, first_rx) = std::sync::mpsc::channel();
        let (_second_tx, second_rx) = std::sync::mpsc::channel();
        let provider = Arc::new(ControlledCompletion {
            requests: requests_tx,
            responses: std::sync::Mutex::new(std::collections::HashMap::from([
                ("first".to_owned(), first_rx),
                ("second".to_owned(), second_rx),
            ])),
            events: events_tx,
            on_lifecycle: None,
        });
        let registry = super::CommandRegistry::new();
        let producer = registry.create_producer(super::ProducerPrecedence::Builtin);
        producer
            .replace(vec![completion_registration(provider)])
            .unwrap();
        let session = registry
            .open_completion(
                registry.resolve("/complete").unwrap(),
                registry.create_target(),
            )
            .unwrap();
        let first = session.complete(Arc::from("first"), Arc::from("first"), 0, Arc::from("test"));
        requests_rx.recv().unwrap();
        first_tx.send(vec![completion_item("same")]).unwrap();
        let super::CompletionResult::Items(items) = futures_lite::future::block_on(first) else {
            panic!("expected completion items");
        };
        let second = session.complete(
            Arc::from("second"),
            Arc::from("second"),
            0,
            Arc::from("test"),
        );
        requests_rx.recv().unwrap();
        assert_eq!(
            session.highlight(&items[0]),
            Err(super::CompletionError::StaleRequest)
        );
        assert_eq!(
            events_rx.recv().unwrap(),
            super::CompletionLifecycleEvent::Cancel
        );
        drop(second);
    }

    #[test]
    fn candidate_is_bound_to_its_session() {
        let (requests_tx, requests_rx) = std::sync::mpsc::channel();
        let (events_tx, _events_rx) = std::sync::mpsc::channel();
        let (first_tx, first_rx) = std::sync::mpsc::channel();
        let provider = Arc::new(ControlledCompletion {
            requests: requests_tx,
            responses: std::sync::Mutex::new(std::collections::HashMap::from([(
                "value".to_owned(),
                first_rx,
            )])),
            events: events_tx,
            on_lifecycle: None,
        });
        let registry = super::CommandRegistry::new();
        let producer = registry.create_producer(super::ProducerPrecedence::Builtin);
        producer
            .replace(vec![completion_registration(provider)])
            .unwrap();
        let command = registry.resolve("/complete").unwrap();
        let first_session = registry
            .open_completion(command.clone(), registry.create_target())
            .unwrap();
        let second_session = registry
            .open_completion(command, registry.create_target())
            .unwrap();
        let request =
            first_session.complete(Arc::from("value"), Arc::from("value"), 0, Arc::from("test"));
        requests_rx.recv().unwrap();
        first_tx.send(vec![completion_item("same")]).unwrap();
        let super::CompletionResult::Items(items) = futures_lite::future::block_on(request) else {
            panic!("expected completion items");
        };
        assert_eq!(
            second_session.accept(items[0].clone()),
            Err(super::CompletionError::StaleSession)
        );
    }

    #[test]
    fn dropped_session_does_not_retain_provider() {
        let (requests_tx, _requests_rx) = std::sync::mpsc::channel();
        let (events_tx, _events_rx) = std::sync::mpsc::channel();
        let provider = Arc::new(ControlledCompletion {
            requests: requests_tx,
            responses: std::sync::Mutex::new(std::collections::HashMap::new()),
            events: events_tx,
            on_lifecycle: None,
        });
        let registry = super::CommandRegistry::new();
        let producer = registry.create_producer(super::ProducerPrecedence::Builtin);
        producer
            .replace(vec![completion_registration(provider.clone())])
            .unwrap();
        let session = registry
            .open_completion(
                registry.resolve("/complete").unwrap(),
                registry.create_target(),
            )
            .unwrap();
        drop(session);
        producer.replace(Vec::new()).unwrap();
        assert_eq!(Arc::strong_count(&provider), 1);
    }

    #[test]
    fn replacement_cancels_session_once() {
        let (requests_tx, requests_rx) = std::sync::mpsc::channel();
        let (events_tx, events_rx) = std::sync::mpsc::channel();
        let (response_tx, response_rx) = std::sync::mpsc::channel();
        let provider = Arc::new(ControlledCompletion {
            requests: requests_tx,
            responses: std::sync::Mutex::new(std::collections::HashMap::from([(
                "value".to_owned(),
                response_rx,
            )])),
            events: events_tx,
            on_lifecycle: None,
        });
        let registry = super::CommandRegistry::new();
        let producer = registry.create_producer(super::ProducerPrecedence::Builtin);
        producer
            .replace(vec![completion_registration(provider)])
            .unwrap();
        let session = registry
            .open_completion(
                registry.resolve("/complete").unwrap(),
                registry.create_target(),
            )
            .unwrap();
        let request =
            session.complete(Arc::from("value"), Arc::from("value"), 0, Arc::from("test"));
        let request_thread = std::thread::spawn(move || futures_lite::future::block_on(request));
        let (_, cancellation) = requests_rx.recv().unwrap();
        producer.replace(Vec::new()).unwrap();
        assert!(cancellation.is_cancelled());
        response_tx.send(Vec::new()).unwrap();
        assert_eq!(
            events_rx.recv().unwrap(),
            super::CompletionLifecycleEvent::Cancel
        );
        assert!(events_rx.try_recv().is_err());
        assert_eq!(
            request_thread.join().unwrap(),
            super::CompletionResult::Stale
        );
    }

    #[test]
    fn invalidation_retains_final_session_arc_until_registry_unlock() {
        let registry = super::CommandRegistry::new();
        let producer = registry.create_producer(super::ProducerPrecedence::Builtin);
        let (requests_tx, _requests_rx) = std::sync::mpsc::channel();
        let (events_tx, _events_rx) = std::sync::mpsc::channel();
        let provider = Arc::new(ControlledCompletion {
            requests: requests_tx,
            responses: std::sync::Mutex::new(std::collections::HashMap::new()),
            events: events_tx,
            on_lifecycle: None,
        });
        producer
            .replace(vec![completion_registration(provider)])
            .unwrap();
        let session = registry
            .open_completion(
                registry.resolve("/complete").unwrap(),
                registry.create_target(),
            )
            .unwrap();
        let gate = Arc::new(super::TestRaceGate::new());
        registry.0.state.lock().unwrap().invalidation_gate = Some(Arc::clone(&gate));
        let replacing_producer = producer.clone();
        let replacement = std::thread::spawn(move || replacing_producer.replace(Vec::new()));

        gate.reached.wait();
        drop(session);
        gate.resume.wait();

        replacement.join().unwrap().unwrap();
    }

    #[test]
    fn replacement_waits_for_started_highlight_before_cancel() {
        let (requests_tx, requests_rx) = std::sync::mpsc::channel();
        let (events_tx, events_rx) = std::sync::mpsc::channel();
        let (response_tx, response_rx) = std::sync::mpsc::channel();
        let gate = Arc::new(super::TestRaceGate::new());
        let callback_gate = Arc::clone(&gate);
        let first_callback = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let callback_first = Arc::clone(&first_callback);
        let provider = Arc::new(ControlledCompletion {
            requests: requests_tx,
            responses: std::sync::Mutex::new(std::collections::HashMap::from([(
                "value".to_owned(),
                response_rx,
            )])),
            events: events_tx,
            on_lifecycle: Some(Arc::new(move || {
                if callback_first.swap(false, std::sync::atomic::Ordering::AcqRel) {
                    callback_gate.wait();
                }
            })),
        });
        let registry = super::CommandRegistry::new();
        let producer = registry.create_producer(super::ProducerPrecedence::Builtin);
        producer
            .replace(vec![completion_registration(provider)])
            .unwrap();
        let session = registry
            .open_completion(
                registry.resolve("/complete").unwrap(),
                registry.create_target(),
            )
            .unwrap();
        let request =
            session.complete(Arc::from("value"), Arc::from("value"), 0, Arc::from("test"));
        requests_rx.recv().unwrap();
        response_tx.send(vec![completion_item("item")]).unwrap();
        let super::CompletionResult::Items(items) = futures_lite::future::block_on(request) else {
            panic!("expected completion items");
        };
        let highlighting_session = session.clone();
        let candidate = items[0].clone();
        let highlight = std::thread::spawn(move || highlighting_session.highlight(&candidate));

        gate.reached.wait();
        assert_eq!(
            events_rx.recv().unwrap(),
            super::CompletionLifecycleEvent::Highlight(completion_item("item"))
        );
        producer.replace(Vec::new()).unwrap();
        assert!(events_rx.try_recv().is_err());
        gate.resume.wait();

        highlight.join().unwrap().unwrap();
        assert_eq!(
            events_rx.recv().unwrap(),
            super::CompletionLifecycleEvent::Cancel
        );
        assert!(events_rx.try_recv().is_err());
        assert_eq!(
            session.highlight(&items[0]),
            Err(super::CompletionError::StaleSession)
        );
    }

    #[test]
    fn highlight_callback_can_replace_producer_before_queued_cancel() {
        let (requests_tx, requests_rx) = std::sync::mpsc::channel();
        let (events_tx, events_rx) = std::sync::mpsc::channel();
        let (response_tx, response_rx) = std::sync::mpsc::channel();
        let registry = super::CommandRegistry::new();
        let producer = registry.create_producer(super::ProducerPrecedence::Builtin);
        let callback_producer = producer.clone();
        let replaced = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let callback_replaced = Arc::clone(&replaced);
        let provider = Arc::new(ControlledCompletion {
            requests: requests_tx,
            responses: std::sync::Mutex::new(std::collections::HashMap::from([(
                "value".to_owned(),
                response_rx,
            )])),
            events: events_tx,
            on_lifecycle: Some(Arc::new(move || {
                if !callback_replaced.swap(true, std::sync::atomic::Ordering::AcqRel) {
                    callback_producer.replace(Vec::new()).unwrap();
                }
            })),
        });
        producer
            .replace(vec![completion_registration(provider)])
            .unwrap();
        let session = registry
            .open_completion(
                registry.resolve("/complete").unwrap(),
                registry.create_target(),
            )
            .unwrap();
        let request =
            session.complete(Arc::from("value"), Arc::from("value"), 0, Arc::from("test"));
        requests_rx.recv().unwrap();
        response_tx.send(vec![completion_item("item")]).unwrap();
        let super::CompletionResult::Items(items) = futures_lite::future::block_on(request) else {
            panic!("expected completion items");
        };

        session.highlight(&items[0]).unwrap();

        assert_eq!(
            events_rx.recv().unwrap(),
            super::CompletionLifecycleEvent::Highlight(completion_item("item"))
        );
        assert_eq!(
            events_rx.recv().unwrap(),
            super::CompletionLifecycleEvent::Cancel
        );
        assert!(events_rx.try_recv().is_err());
    }

    #[test]
    fn replacement_wins_before_result_commit() {
        let (requests_tx, requests_rx) = std::sync::mpsc::channel();
        let (events_tx, _events_rx) = std::sync::mpsc::channel();
        let (response_tx, response_rx) = std::sync::mpsc::channel();
        let provider = Arc::new(ControlledCompletion {
            requests: requests_tx,
            responses: std::sync::Mutex::new(std::collections::HashMap::from([(
                "value".to_owned(),
                response_rx,
            )])),
            events: events_tx,
            on_lifecycle: None,
        });
        let registry = super::CommandRegistry::new();
        let producer = registry.create_producer(super::ProducerPrecedence::Builtin);
        producer
            .replace(vec![completion_registration(provider)])
            .unwrap();
        let session = registry
            .open_completion(
                registry.resolve("/complete").unwrap(),
                registry.create_target(),
            )
            .unwrap();
        let gate = Arc::new(super::TestRaceGate::new());
        *session.core.commit_gate.lock().unwrap() = Some(Arc::clone(&gate));
        let request =
            session.complete(Arc::from("value"), Arc::from("value"), 0, Arc::from("test"));
        let request_thread = std::thread::spawn(move || futures_lite::future::block_on(request));
        requests_rx.recv().unwrap();
        response_tx.send(vec![completion_item("old")]).unwrap();
        gate.reached.wait();
        producer.replace(Vec::new()).unwrap();
        gate.resume.wait();

        assert_eq!(
            request_thread.join().unwrap(),
            super::CompletionResult::Stale
        );
    }

    #[test]
    fn removal_or_replacement_wins_before_candidate_acceptance() {
        for remove in [false, true] {
            let (requests_tx, requests_rx) = std::sync::mpsc::channel();
            let (events_tx, events_rx) = std::sync::mpsc::channel();
            let (response_tx, response_rx) = std::sync::mpsc::channel();
            let provider = Arc::new(ControlledCompletion {
                requests: requests_tx,
                responses: std::sync::Mutex::new(std::collections::HashMap::from([(
                    "value".to_owned(),
                    response_rx,
                )])),
                events: events_tx,
                on_lifecycle: None,
            });
            let registry = super::CommandRegistry::new();
            let producer = registry.create_producer(super::ProducerPrecedence::Builtin);
            producer
                .replace(vec![completion_registration(provider)])
                .unwrap();
            let session = registry
                .open_completion(
                    registry.resolve("/complete").unwrap(),
                    registry.create_target(),
                )
                .unwrap();
            let request =
                session.complete(Arc::from("value"), Arc::from("value"), 0, Arc::from("test"));
            requests_rx.recv().unwrap();
            response_tx.send(vec![completion_item("item")]).unwrap();
            let super::CompletionResult::Items(items) = futures_lite::future::block_on(request)
            else {
                panic!("expected completion items");
            };
            let gate = Arc::new(super::TestRaceGate::new());
            *session.core.lifecycle_gate.lock().unwrap() = Some(Arc::clone(&gate));
            let accepting_session = session.clone();
            let candidate = items[0].clone();
            let accept_thread = std::thread::spawn(move || accepting_session.accept(candidate));
            gate.reached.wait();
            if remove {
                assert!(producer.remove());
            } else {
                producer.replace(Vec::new()).unwrap();
            }
            gate.resume.wait();

            assert_eq!(
                accept_thread.join().unwrap(),
                Err(super::CompletionError::StaleSession)
            );
            assert_eq!(
                events_rx.recv().unwrap(),
                super::CompletionLifecycleEvent::Cancel
            );
            assert!(events_rx.try_recv().is_err());
        }
    }

    #[test]
    fn accept_and_cancel_are_terminal() {
        let (requests_tx, requests_rx) = std::sync::mpsc::channel();
        let (events_tx, events_rx) = std::sync::mpsc::channel();
        let (response_tx, response_rx) = std::sync::mpsc::channel();
        let provider = Arc::new(ControlledCompletion {
            requests: requests_tx,
            responses: std::sync::Mutex::new(std::collections::HashMap::from([(
                "value".to_owned(),
                response_rx,
            )])),
            events: events_tx,
            on_lifecycle: None,
        });
        let registry = super::CommandRegistry::new();
        let producer = registry.create_producer(super::ProducerPrecedence::Builtin);
        producer
            .replace(vec![completion_registration(provider)])
            .unwrap();
        let session = registry
            .open_completion(
                registry.resolve("/complete").unwrap(),
                registry.create_target(),
            )
            .unwrap();
        let request =
            session.complete(Arc::from("value"), Arc::from("value"), 0, Arc::from("test"));
        let request_thread = std::thread::spawn(move || futures_lite::future::block_on(request));
        requests_rx.recv().unwrap();
        response_tx.send(vec![completion_item("item")]).unwrap();
        let super::CompletionResult::Items(items) = request_thread.join().unwrap() else {
            panic!("expected completion items");
        };
        session.highlight(&items[0]).unwrap();
        session.accept(items[0].clone()).unwrap();
        assert_eq!(
            events_rx.recv().unwrap(),
            super::CompletionLifecycleEvent::Highlight(completion_item("item"))
        );
        assert_eq!(
            events_rx.recv().unwrap(),
            super::CompletionLifecycleEvent::Accept(completion_item("item"))
        );
        assert!(session.cancel().is_err());
        assert!(events_rx.try_recv().is_err());
    }

    #[test]
    fn supersession_callback_can_reenter_session() {
        let (requests_tx, requests_rx) = std::sync::mpsc::channel();
        let (events_tx, events_rx) = std::sync::mpsc::channel();
        let (_first_tx, first_rx) = std::sync::mpsc::channel();
        let (_second_tx, second_rx) = std::sync::mpsc::channel();
        let reentrant_session = Arc::new(std::sync::Mutex::new(None::<super::CompletionSession>));
        let callback_session = Arc::clone(&reentrant_session);
        let provider = Arc::new(ControlledCompletion {
            requests: requests_tx,
            responses: std::sync::Mutex::new(std::collections::HashMap::from([
                ("first".to_owned(), first_rx),
                ("second".to_owned(), second_rx),
            ])),
            events: events_tx,
            on_lifecycle: Some(Arc::new(move || {
                let session = callback_session.lock().unwrap().as_ref().unwrap().clone();
                let _ = session.cancel();
            })),
        });
        let registry = super::CommandRegistry::new();
        let producer = registry.create_producer(super::ProducerPrecedence::Builtin);
        producer
            .replace(vec![completion_registration(provider)])
            .unwrap();
        let session = registry
            .open_completion(
                registry.resolve("/complete").unwrap(),
                registry.create_target(),
            )
            .unwrap();
        *reentrant_session.lock().unwrap() = Some(session.clone());
        let first = session.complete(Arc::from("first"), Arc::from("first"), 0, Arc::from("test"));
        requests_rx.recv().unwrap();
        let second = session.complete(
            Arc::from("second"),
            Arc::from("second"),
            0,
            Arc::from("test"),
        );

        assert_eq!(
            events_rx.recv().unwrap(),
            super::CompletionLifecycleEvent::Cancel
        );
        let (_, second_cancellation) = requests_rx.recv().unwrap();
        assert!(second_cancellation.is_cancelled());
        drop(second);
        drop(first);
    }

    #[test]
    fn callbacks_run_outside_registry_lock() {
        let (requests_tx, requests_rx) = std::sync::mpsc::channel();
        let (events_tx, events_rx) = std::sync::mpsc::channel();
        let (response_tx, response_rx) = std::sync::mpsc::channel();
        let registry = super::CommandRegistry::new();
        let callback_registry = registry.clone();
        let provider = Arc::new(ControlledCompletion {
            requests: requests_tx,
            responses: std::sync::Mutex::new(std::collections::HashMap::from([(
                "value".to_owned(),
                response_rx,
            )])),
            events: events_tx,
            on_lifecycle: Some(Arc::new(move || {
                callback_registry.snapshot();
            })),
        });
        let producer = registry.create_producer(super::ProducerPrecedence::Builtin);
        producer
            .replace(vec![completion_registration(provider)])
            .unwrap();
        let session = registry
            .open_completion(
                registry.resolve("/complete").unwrap(),
                registry.create_target(),
            )
            .unwrap();
        let request =
            session.complete(Arc::from("value"), Arc::from("value"), 0, Arc::from("test"));
        let request_thread = std::thread::spawn(move || futures_lite::future::block_on(request));
        requests_rx.recv().unwrap();
        producer.replace(Vec::new()).unwrap();
        response_tx.send(Vec::new()).unwrap();
        assert_eq!(
            events_rx.recv().unwrap(),
            super::CompletionLifecycleEvent::Cancel
        );
        assert_eq!(
            request_thread.join().unwrap(),
            super::CompletionResult::Stale
        );
    }

    #[test]
    fn opaque_ids_do_not_collide_across_registries() {
        let first = super::CommandRegistry::new();
        let second = super::CommandRegistry::new();
        let first_producer = first.create_producer(super::ProducerPrecedence::Builtin);
        let second_producer = second.create_producer(super::ProducerPrecedence::Builtin);
        first_producer
            .replace(vec![registration("/run", &[], ArgumentArity::ANY)])
            .unwrap();
        second_producer
            .replace(vec![registration("/run", &[], ArgumentArity::ANY)])
            .unwrap();

        assert_ne!(first_producer.id(), second_producer.id());
        assert_ne!(first.create_target(), second.create_target());
        assert_ne!(
            first.resolve("/run").unwrap().command_id(),
            second.resolve("/run").unwrap().command_id()
        );
    }

    #[test]
    fn foreign_command_cannot_open_completion() {
        let (requests_tx, _requests_rx) = std::sync::mpsc::channel();
        let (events_tx, _events_rx) = std::sync::mpsc::channel();
        let provider = Arc::new(ControlledCompletion {
            requests: requests_tx,
            responses: std::sync::Mutex::new(std::collections::HashMap::new()),
            events: events_tx,
            on_lifecycle: None,
        });
        let first = super::CommandRegistry::new();
        let producer = first.create_producer(super::ProducerPrecedence::Builtin);
        producer
            .replace(vec![completion_registration(provider)])
            .unwrap();
        let second = super::CommandRegistry::new();

        assert!(matches!(
            second.open_completion(first.resolve("/complete").unwrap(), second.create_target()),
            Err(super::CompletionError::StaleCommand)
        ));
    }

    #[test]
    fn depth_limit_is_enforced_for_known_commands() {
        let registry = super::CommandRegistry::new();
        let producer = registry.create_producer(super::ProducerPrecedence::Builtin);
        producer
            .replace(vec![registration("/run", &[], ArgumentArity::ANY)])
            .unwrap();
        let target = registry.create_target();
        let error = futures_lite::future::block_on(registry.dispatch_input(
            "/run",
            super::MAX_COMMAND_DEPTH + 1,
            target,
        ))
        .unwrap_err();
        assert_eq!(error, super::CommandError::MaximumDepth);
    }

    #[test]
    fn depth_limit_boundary_allows_max_depth() {
        let registry = super::CommandRegistry::new();
        let producer = registry.create_producer(super::ProducerPrecedence::Builtin);
        producer
            .replace(vec![registration("/run", &[], ArgumentArity::ANY)])
            .unwrap();
        let target = registry.create_target();
        let dispatch = futures_lite::future::block_on(registry.dispatch_input(
            "/run",
            super::MAX_COMMAND_DEPTH,
            target,
        ))
        .unwrap();
        assert!(matches!(dispatch, super::InputDispatch::Dispatched(_)));
    }

    #[test]
    fn unknown_commands_fall_through_at_any_depth() {
        let registry = super::CommandRegistry::new();
        let target = registry.create_target();
        for depth in [0, super::MAX_COMMAND_DEPTH, super::MAX_COMMAND_DEPTH + 1] {
            let dispatch =
                futures_lite::future::block_on(registry.dispatch_input("/nope", depth, target))
                    .unwrap();
            assert!(
                matches!(dispatch, super::InputDispatch::UnknownCommandInput),
                "depth {depth} did not fall through"
            );
        }
    }
}
