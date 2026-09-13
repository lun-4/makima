//! Frontend-neutral contracts for slash commands.

mod arguments;
mod completion;
mod completion_providers;
mod dispatch;
mod registry;
mod spec;

pub use arguments::{
    ArgumentKind, ArgumentParseError, ArgumentValue, CommandArguments, CompletionEdit,
    CompletionPolicy, CountExpectation, LexError, MAX_EXACT_INTEGER, ParsedArgument,
    ParsedArguments, ParsedToken, PathResolutionError, PositionalArgument, QuoteStyle,
    StaticArgumentKind, StaticCommandArguments, StaticPositionalArgument, encode_completion_value,
    lex_strict, lex_tolerant, parse_completion_prefix_arguments, parse_positional, resolve_path,
};

pub use completion::{
    CancellationToken, CommandCompletion, CompletionCandidate, CompletionContext, CompletionError,
    CompletionInput, CompletionItem, CompletionItemNavigation, CompletionLifecycleEvent,
    CompletionNavigation, CompletionPublisher, CompletionResult, CompletionSession,
    CompletionSnapshot, CompletionSnapshotSink,
};
pub use completion_providers::{CompletionKind, CompletionProviders};
pub use dispatch::{
    AgentTurn, CommandAttachment, CommandBehavior, CommandContent, CommandError, CommandHost,
    CommandInvocation, CommandOutcome, HostRequest, HostResponse, InputDispatch, PromptReference,
    RegistrationError, ResolutionError, ResolvedCommand, ResolvedInput, SlashClass, classify_input,
};
pub use registry::{
    CommandRegistry, PreparedTarget, PresentedCommand, Producer, ProducerPrecedence,
    RegistrySnapshot, RegistrySubscription, TargetHandle,
};
pub use spec::{
    BUILTIN_COMMANDS, BuiltinDefinition, BuiltinId, BuiltinOperation, COMPACT_COMMAND_NAME,
    CommandDocs, CommandFuture, CommandId, CommandSpec, CompletionKey, CompletionSessionId,
    HostContextRequest, HostContextResponse, InvocationTargetId, MAX_COMMAND_DEPTH, ProducerId,
    Registration, StaticArgumentCompletion, TargetCapabilities, TargetCapability,
};

#[cfg(test)]
mod tests;
