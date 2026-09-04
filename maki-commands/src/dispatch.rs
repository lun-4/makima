use std::fmt;
use std::sync::Arc;

use thiserror::Error;

use crate::completion::CommandCompletion;
use crate::registry::{
    CommandRegistry, RegistrationRecord, TargetHandle, normalize, target_record,
};
use crate::spec::{
    ArgumentArity, BuiltinOperation, CommandFuture, CommandId, CommandSpec, HostContextRequest,
    HostContextResponse, InvocationTargetId, MAX_COMMAND_DEPTH, ProducerId, RegistryId,
    TargetCapabilities, TargetCapability,
};

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

impl CommandRegistry {
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

    pub(super) fn dispatch_input_at(
        &self,
        target: TargetHandle,
        content: CommandContent,
        depth: usize,
    ) -> CommandFuture<InputDispatch> {
        let class = classify_input(&content.text);
        if let SlashClass::EscapedLiteral(literal) = class {
            let attachments = content.attachments.clone();
            let text = Arc::from(literal);
            return Box::pin(async move {
                InputDispatch::LiteralInput(CommandContent { text, attachments })
            });
        }
        let Ok(resolved) = self.resolve_input_for(&target, &content.text) else {
            if matches!(class, SlashClass::Command(_)) {
                let trimmed = content.text.trim_start();
                let name =
                    Arc::from(ParsedInput::parse(trimmed).map_or(trimmed, |parsed| parsed.name));
                return Box::pin(async move {
                    InputDispatch::Dispatched(CommandOutcome::Failed(CommandError::UnknownCommand(
                        name,
                    )))
                });
            }
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
/// Lexical classification of user text: whether it is a command attempt,
/// an escaped literal, or plain prose. The only place the escape-strip value
/// is computed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SlashClass<'a> {
    /// Single-slash command attempt: `/foo` or ` /foo` (trimmed).
    Command(&'a str),
    /// `//foo` → literal `/foo`; `///foo` → literal `//foo`. Exactly one
    /// leading slash is stripped from the trimmed text.
    EscapedLiteral(&'a str),
    /// Not a command attempt.
    Plain,
}

pub fn classify_input(text: &str) -> SlashClass<'_> {
    let trimmed = text.trim_start();
    if trimmed.starts_with("//") {
        SlashClass::EscapedLiteral(&trimmed[1..])
    } else if trimmed.starts_with('/') {
        SlashClass::Command(trimmed)
    } else {
        SlashClass::Plain
    }
}

pub(super) struct ParsedInput<'a> {
    pub(super) name: &'a str,
    pub(super) arguments: &'a str,
}

impl<'a> ParsedInput<'a> {
    pub(super) fn parse(input: &'a str) -> Option<Self> {
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
    pub(super) registry_id: RegistryId,
    pub(super) record: Arc<RegistrationRecord>,
    pub(super) invoked_name: Arc<str>,
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
    #[error("unknown command {0}")]
    UnknownCommand(Arc<str>),
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
