use thiserror::Error;

use crate::{AgentId, TurnId};

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum ReactivationError {
    #[error("not_restorable: {reason}")]
    NotRestorable { reason: String },
    #[error("provider_unavailable: {provider}")]
    ProviderUnavailable { provider: String },
    #[error("model_unavailable: {model}")]
    ModelUnavailable { model: String },
    #[error("tool_unavailable: {tool}")]
    ToolUnavailable { tool: String },
    #[error("mcp_unavailable: {server}")]
    McpUnavailable { server: String },
    #[error("mode ceiling rejected reactivation: {0}")]
    ModeCeiling(String),
    #[error("agent initialization failed: {0}")]
    Initialization(String),
    #[error("tool_completion_pending: previous tool writes are still settling; retry reactivation")]
    ToolCompletionPending,
    #[error("agent activation was cancelled")]
    Cancelled,
    #[error("agent factory panicked")]
    Panicked,
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum ManagerError {
    #[error("agent limits must be at least one")]
    InvalidLimits,
    #[error("the agent graph already has a root")]
    DuplicateRoot,
    #[error("the agent graph has no root")]
    MissingRoot,
    #[error("agent {0} is unknown")]
    UnknownAgent(AgentId),
    #[error("agent {0} is not live")]
    NonLiveAgent(AgentId),
    #[error("agent graph is shutting down")]
    GraphShutdown,
    #[error("agent graph rewind is in progress")]
    RewindInProgress,
    #[error("agent depth {depth} exceeds the configured maximum {max}")]
    DepthExceeded { depth: usize, max: usize },
    #[error(
        "child_limit: agent {parent_id} has the configured maximum of {max} children with outstanding work"
    )]
    ChildLimit { parent_id: AgentId, max: usize },
    #[error("active_agent_limit: agent graph has the configured maximum of {max} active agents")]
    ActiveAgentLimit { max: usize },
    #[error("retention_limit: agent graph has the configured maximum of {max} retained identities")]
    RetentionLimit { max: usize },
    #[error("retired: agent {0} is retired")]
    Retired(AgentId),
    #[error("agent {0} has a newer incarnation")]
    StaleIncarnation(AgentId),
    #[error("agent {0} is not a leaf")]
    NotLeaf(AgentId),
    #[error("invalid restored agent graph: {0}")]
    InvalidGraph(String),
    #[error(transparent)]
    Reactivation(#[from] ReactivationError),
    #[error("managed turn authority belongs to another manager")]
    WrongManager,
    #[error("managed turn {turn_id} for agent {agent_id} is no longer active")]
    InactiveTurn { agent_id: AgentId, turn_id: TurnId },
    #[error("agent actor mismatch: expected {expected_id}, got {actual_id}")]
    ActorMismatch {
        expected_id: AgentId,
        actual_id: AgentId,
    },
    #[error("turn_result_unavailable: {turn_id} for agent {agent_id}")]
    TurnResultUnavailable { agent_id: AgentId, turn_id: TurnId },
    #[error("turn {turn_id} does not belong to agent {agent_id}")]
    TicketActorMismatch { agent_id: AgentId, turn_id: TurnId },
    #[error("agent {child_id} is not a descendant of agent {parent_id}")]
    NotDescendant {
        parent_id: AgentId,
        child_id: AgentId,
    },
    #[error("agent factory failed: {0}")]
    Factory(String),
    #[error("agent {0} or its descendants have pending work")]
    BusySubtree(AgentId),
    #[error("agent policy update failed: {0}")]
    Policy(String),
}
