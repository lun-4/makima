//! Domain errors for the actor scheduler.

use thiserror::Error;

use crate::types::TurnId;

#[derive(Debug, Error, Clone, PartialEq)]
pub enum ActorError {
    #[error("the actor is closed")]
    Closed,
    #[error("the actor is shutting down")]
    Shutdown,
    #[error("policy update is pending; use async admission")]
    PolicyPending,
    #[error("policy update was cancelled")]
    PolicyCancelled,
    #[error("invalid configuration: {0}")]
    InvalidConfig(String),
    #[error(transparent)]
    UnsupportedOption(#[from] crate::session_options::SessionOptionError),
    #[error("configuration preparation expired")]
    ConfigExpired,
    #[error("no such turn: {0}")]
    UnknownTurn(TurnId),
}
