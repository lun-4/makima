//! Async agent loop with tools.

pub mod actor;
pub mod agent;
pub mod cancel;
pub mod child_guard;
pub use child_guard::ChildGuard;
pub mod headless;
pub mod mailbox;
pub mod manager;
pub mod mcp;
pub use mcp::config::{McpConfigError, McpConfigErrors, McpServerInfo, McpServerStatus};
pub use mcp::protocol::PromptRole;
pub mod modes;
pub use mcp::{
    McpCommand, McpHandle, McpPromptArg, McpPromptInfo, McpSession, McpSnapshot, McpSnapshotReader,
};
pub use modes::{ModeDef, ModeDefSpec, ModeError, ModeId, ModeRegistry};
pub(crate) mod task_set;
pub use actor::{
    ActorBackend, ActorError, ActorLifecycle, ActorSnapshot, ActorStatus, ActorWork,
    AgentActorHandle, BackendResult, ControlWork, InterruptQueue, QueueProjection, RootWork,
    TurnAdmission, TurnContext, TurnTicket, WorkKind,
};
pub use agent::{
    Agent, AgentParams, AgentRunParams, History, HistorySnapshot, Instructions, LoadedInstructions,
    ModelSource, RunSettings, RunSettingsSource, SessionRunSettings, SharedMessages, SharedModel,
    ToolBuilder, UNAVAILABLE_RESULT, close_dangling_tool_calls, find_subdirectory_instructions,
    is_instruction_file,
};
pub use cancel::{
    CancelMap, CancelToken, CancelTrigger, ReasonedCancelToken, ReasonedCancelTrigger,
};
pub use mailbox::{MailboxError, PreparedSessionMailbox, SessionMailbox};
pub use maki_config::{AgentConfig, PermissionsConfig, SessionDefaults, ToolOutputLines};
pub use manager::{
    AgentLimits, AgentManagerHandle, AgentMetadata, AgentNodeSnapshot, AgentRef,
    CurrentManagedTurn, GraphLifecycle, ManagedPromptWait, ManagerError, PromptWaitError,
    ShutdownReport, TurnPermitLease,
};
pub mod command;
pub mod diff;
pub mod permissions;
pub mod prompt;
pub mod session_checkpoint;
pub mod session_coordinator;
pub mod session_options;
pub mod template;
pub mod tools;
pub use tools::ToolFilter;
pub mod types;

use std::collections::HashMap;
use std::path::{Path, PathBuf};

pub use maki_providers::AgentError;
use maki_providers::Message;
pub use maki_providers::{EMPTY_RESPONSE_MARKER, ImageMediaType, ImageSource, ThinkingConfig};
pub use types::{
    AgentEvent, AgentId, BufferSnapshot, DoneReason, Envelope, EventSender, EventStreamGuard,
    GrepFileEntry, GrepLine, GrepMatchGroup, InstructionBlock, NO_FILES_FOUND, RunLedger,
    RunTotals, SessionEvents, SharedBuf, SnapshotLine, SnapshotSpan, SpanStyle, SubagentCancel,
    SubagentInfo, TextOutput, ToolDoneEvent, ToolInput, ToolOutput, ToolStartEvent,
    TurnCancellationReason, TurnCompleteEvent, TurnFailure, TurnFailureKind, TurnId, TurnOutcome,
    event_stream,
};

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub enum AgentMode {
    #[default]
    Build,
    Plan(PathBuf),
    Custom(ModeId),
}

impl AgentMode {
    pub fn plan_path(&self) -> Option<&Path> {
        match self {
            Self::Plan(p) => Some(p),
            Self::Build | Self::Custom(_) => None,
        }
    }

    pub fn id(&self) -> ModeId {
        match self {
            Self::Build => ModeId::Build,
            Self::Plan(_) => ModeId::Plan,
            Self::Custom(id) => id.clone(),
        }
    }
}

pub enum ExtractedCommand {
    /// Every message the user queued back to back, so one turn answers them
    /// all. A source must stop there and hand anything else over on its own,
    /// since a command like `/compact` rewrites the history the later messages
    /// land in.
    Interrupt(Vec<AgentInput>),
    /// Carries the guidance typed as `/compact <instructions>`, for this one
    /// summary.
    Compact(Option<String>),
}

pub trait InterruptSource: Send + Sync {
    fn poll(&self) -> Option<ExtractedCommand>;
}

/// What a message must agree on with its neighbours to share their turn.
pub type BatchKey = (AgentMode, bool);

/// The set of messages this one may share a turn with, or `None` when it
/// has to run alone: the agent resolves one MCP prompt per run. `mode` is
/// a permission boundary and `workflow` picks the tool catalog, so a
/// message queued under either may not execute under another one.
///
/// Destructured on purpose: a new `AgentInput` field then has to be
/// classified here instead of silently merging across.
pub fn batch_key(input: &AgentInput) -> Option<BatchKey> {
    let AgentInput {
        mode,
        workflow,
        prompt,
        cancel,
        lease_committer,
        message: _,
        images: _,
        preamble: _,
        thinking: _,
        fast: _,
    } = input;
    if prompt.is_some() || cancel.is_some() || lease_committer.is_some() {
        return None;
    }
    Some((mode.clone(), *workflow))
}

/// Folds a run of queued messages into one agent input. The last message
/// drives the run and the earlier ones ride in front of it as their own user
/// messages, so each keeps its images and the model answers the burst in one
/// request. The run shares one mode and one workflow by construction, so only
/// the preferences (thinking, fast) come from the last message, the user's
/// most recent intent.
pub fn merge_inputs(mut inputs: Vec<AgentInput>) -> Option<AgentInput> {
    let mut last = inputs.pop()?;
    let mut preamble = Vec::new();
    for earlier in inputs {
        preamble.extend(earlier.preamble);
        let message = maki_providers::Message::user_with_images(earlier.message, earlier.images);
        // An input with neither text nor images would become a user message
        // with no content at all, which providers reject.
        if !message.content.is_empty() {
            preamble.push(message);
        }
    }
    preamble.append(&mut last.preamble);
    last.preamble = preamble;
    Some(last)
}

#[derive(Clone)]
pub struct McpPromptRef {
    pub qualified_name: String,
    pub arguments: HashMap<String, String>,
}

pub struct AgentInput {
    pub message: String,
    pub mode: AgentMode,
    pub images: Vec<ImageSource>,
    pub preamble: Vec<Message>,
    pub thinking: ThinkingConfig,
    pub fast: bool,
    /// No `Default` on this struct so adding a field forces every call site to update.
    pub workflow: bool,
    pub prompt: Option<Box<McpPromptRef>>,
    pub cancel: Option<CancelToken>,
    pub lease_committer: Option<session_coordinator::SessionLeaseCommitter>,
}

impl AgentInput {
    /// What a host with no toggle UI sends. `-p`, the SDK and ACP know nothing
    /// about the toggles beyond what config says, so they all build their input
    /// here and a knob added to [`SessionDefaults`] reaches every one of them.
    pub fn from_defaults(
        message: String,
        mode: AgentMode,
        images: Vec<ImageSource>,
        defaults: SessionDefaults,
    ) -> Self {
        Self {
            message,
            mode,
            images,
            preamble: Vec::new(),
            thinking: defaults.thinking.into(),
            fast: defaults.fast,
            workflow: defaults.workflow,
            prompt: None,
            cancel: None,
            lease_committer: None,
        }
    }
}
