use thiserror::Error;

use crate::{AgentId, TurnId};

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
    #[error("agent depth {depth} exceeds the configured maximum {max}")]
    DepthExceeded { depth: usize, max: usize },
    #[error("agent {parent_id} already has the configured maximum of {max} live children")]
    ChildLimit { parent_id: AgentId, max: usize },
    #[error("agent graph already has the configured maximum of {max} live agents")]
    LiveAgentLimit { max: usize },
    #[error("managed turn authority belongs to another manager")]
    WrongManager,
    #[error("managed turn {turn_id} for agent {agent_id} is no longer active")]
    InactiveTurn { agent_id: AgentId, turn_id: TurnId },
    #[error("agent actor mismatch: expected {expected_id}, got {actual_id}")]
    ActorMismatch {
        expected_id: AgentId,
        actual_id: AgentId,
    },
    #[error("turn {turn_id} does not belong to agent {agent_id}")]
    TicketActorMismatch { agent_id: AgentId, turn_id: TurnId },
    #[error("agent {child_id} is not a descendant of agent {parent_id}")]
    NotDescendant {
        parent_id: AgentId,
        child_id: AgentId,
    },
    #[error("agent factory failed: {0}")]
    Factory(String),
}
