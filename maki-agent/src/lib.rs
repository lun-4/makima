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
    ActorBackend, ActorError, ActorEvent, ActorLifecycle, ActorSnapshot, ActorStatus, ActorWork,
    AgentActorHandle, AgentContextSnapshot, BackendResult, CompactionStatus, CompletionPolicy,
    ConfigChange, ConfigCommit, ConfigPatch, ControlWork, EffectiveAgentConfig, HistoryBoundary,
    HistoryChange, InterruptQueue, PendingQueueEntry, PendingSubmission, PreparedModel,
    QueueProjection, SubmissionId, TranscriptRequest, TranscriptSnapshot, TurnAdmission,
    TurnAdmissionOptions, TurnContext, TurnOrigin, TurnOutput, TurnProvenance, TurnResult,
    TurnTicket, WorkKind,
};
pub use agent::{
    Agent, AgentParams, AgentRunParams, History, HistorySnapshot, Instructions, LoadedInstructions,
    ModelSource, RunSettings, SharedMessages, SharedModel, UNAVAILABLE_RESULT,
    close_dangling_tool_calls, find_subdirectory_instructions, is_instruction_file,
};
pub use cancel::{
    CancelMap, CancelToken, CancelTrigger, ReasonedCancelToken, ReasonedCancelTrigger,
};
pub use mailbox::{MailboxError, PreparedSessionMailbox, SessionMailbox};
pub use maki_config::{AgentConfig, PermissionsConfig, SessionDefaults, ToolOutputLines};
pub use manager::{
    AgentLimits, AgentManagerHandle, AgentMetadata, AgentNodeSnapshot, AgentRef,
    CurrentManagedTurn, GraphEvents, GraphLifecycle, ManagedPromptWait, ManagerError,
    PromptWaitError, ShutdownReport, TurnPermitLease,
};
pub mod command;
pub mod config_change;
pub mod diff;
pub mod permissions;
pub mod prompt;
pub mod session_checkpoint;
pub mod session_coordinator;
pub mod session_defaults;
pub mod session_options;
pub mod template;
pub mod tools;
pub use tools::ToolFilter;
pub mod types;

use std::collections::HashMap;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;

pub use maki_domain::{POST_LEVELS, Post, PostLevel};
pub use maki_providers::AgentError;
use maki_providers::Message;
pub use maki_providers::{EMPTY_RESPONSE_MARKER, ImageMediaType, ImageSource, ThinkingConfig};
pub use types::{
    ActorWorkIdentity, ActorWorkKind, AgentEvent, AgentId, BufferSnapshot, DoneReason, Envelope,
    EventSender, EventStreamGuard, GrepFileEntry, GrepLine, GrepMatchGroup, InstructionBlock,
    NO_FILES_FOUND, RunLedger, RunTotals, SessionEvents, SharedBuf, SnapshotLine, SnapshotSpan,
    SpanStyle, SubagentCancel, SubagentInfo, TextOutput, ToolDoneEvent, ToolInput, ToolOutput,
    ToolStartEvent, TurnCancellationReason, TurnCompleteEvent, TurnFailure, TurnFailureKind,
    TurnId, TurnOutcome, event_stream,
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

pub struct PresentedInput {
    pub input: AgentInput,
    pub already_displayed: bool,
    pub mcp_startup_notice: Option<usize>,
}

pub enum ExtractedCommand {
    /// Every message the user queued back to back, so one turn answers them
    /// all. A source must stop there and hand anything else over on its own,
    /// since a command like `/compact` rewrites the history the later messages
    /// land in.
    Interrupt(Vec<AgentInput>),
    PresentedInterrupt(Vec<PresentedInput>),
    /// Carries the guidance typed as `/compact <instructions>`, for this one
    /// summary.
    Compact(Option<String>),
}

pub trait InterruptSource: Send + Sync {
    fn poll(&self) -> Option<ExtractedCommand>;

    fn poll_ready(&self) -> Pin<Box<dyn Future<Output = Option<ExtractedCommand>> + Send + '_>> {
        Box::pin(async { self.poll() })
    }
}

/// Whether this message may share a turn with its neighbours: the agent
/// resolves one MCP prompt per run, so a message carrying one runs alone.
pub fn batchable(input: &AgentInput) -> bool {
    input.prompt.is_none()
}

#[derive(Clone)]
pub struct McpPromptRef {
    pub qualified_name: String,
    pub arguments: HashMap<String, String>,
}

/// No `Default` on this struct so adding a field forces every call site to update.
pub struct AgentInput {
    pub message: String,
    pub images: Vec<ImageSource>,
    pub preamble: Vec<Message>,
    pub prompt: Option<Box<McpPromptRef>>,
}

impl AgentInput {
    pub fn new(message: String, images: Vec<ImageSource>) -> Self {
        Self {
            message,
            images,
            preamble: Vec::new(),
            prompt: None,
        }
    }
}
