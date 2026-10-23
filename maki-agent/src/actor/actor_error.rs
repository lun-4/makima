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
    #[error("transcript caps must be 1..=1000 messages and 2..=1048576 bytes")]
    InvalidTranscriptCaps,
    #[error("turn is still pending: {0}")]
    PendingTurn(TurnId),
    #[error("exact history boundary is unavailable for turn: {0}")]
    UnavailableTurnHistory(TurnId),
    #[error("turn history has been compacted: {0}")]
    CompactedTurn(TurnId),
    #[error("could not serialize transcript: {0}")]
    TranscriptSerialization(String),
    #[error("instruction inbox capacity exceeded (64 messages or 256 KiB)")]
    InstructionOverflow,
    #[error("submission payload could not be serialized")]
    InvalidInstruction,
    #[error("the actor has pending work")]
    Busy,
    #[error("history boundary no longer belongs to this actor history")]
    StaleHistoryBoundary,
    #[error("history checkpoint failed: {0}")]
    HistoryCheckpoint(String),
    #[error("the after_turn boundary is not the latest settled turn")]
    StaleTurn,
    #[error("no such turn: {0}")]
    UnknownTurn(TurnId),
}

impl ActorError {
    /// Whether the actor refused because it closed or is shutting down,
    /// rather than because of what was asked.
    pub fn is_closed(&self) -> bool {
        matches!(self, Self::Closed | Self::Shutdown)
    }
}
