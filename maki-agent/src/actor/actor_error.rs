//! Domain errors for the actor scheduler.

use thiserror::Error;

use crate::types::TurnId;

#[derive(Debug, Error, Clone, PartialEq)]
pub enum ActorError {
    #[error("the actor is closed")]
    Closed,
    #[error("the actor is shutting down")]
    Shutdown,
    #[error("no such turn: {0}")]
    UnknownTurn(TurnId),
}
