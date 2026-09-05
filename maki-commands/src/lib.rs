//! Frontend-neutral contracts for slash commands.

mod completion;
mod dispatch;
mod registry;
mod spec;

pub use completion::{
    CancellationToken, CommandCompletion, CompletionCandidate, CompletionContext, CompletionError,
    CompletionItem, CompletionLifecycleEvent, CompletionResult, CompletionSession,
};
pub use dispatch::{
    AgentTurn, CommandAttachment, CommandBehavior, CommandContent, CommandError, CommandHost,
    CommandInvocation, CommandOutcome, HostRequest, HostResponse, InputDispatch, PromptReference,
    RegistrationError, ResolutionError, ResolvedCommand, ResolvedInput,
};
pub use registry::{
    CommandRegistry, PresentedCommand, Producer, ProducerPrecedence, RegistrySnapshot,
    RegistrySubscription, TargetHandle,
};
pub use spec::{
    ArgumentArity, BUILTIN_COMMANDS, BuiltinDefinition, BuiltinId, BuiltinOperation,
    COMPACT_COMMAND_NAME, CommandDocs, CommandFuture, CommandId, CommandSpec, CompletionKey,
    CompletionSessionId, HostContextRequest, HostContextResponse, InvocationTargetId,
    MAX_COMMAND_DEPTH, ProducerId, Registration, TargetCapabilities, TargetCapability,
};

#[cfg(test)]
mod tests;
