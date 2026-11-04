//! Provider error types with retry semantics.
//! Retryable: 429, 5xx, IO, stream timeouts. Non-retryable: other 4xx, context overflow,
//! JSON parse, config, channel closed, user cancel. `user_message()` returns human-readable text for each variant.

use std::{io, time::Duration};

use crate::retry::RetryKind;

const CONNECT_FAILED_MESSAGE: &str =
    "could not connect, check the server is running and the base URL is correct";

#[derive(Debug, thiserror::Error)]
pub enum AgentError {
    #[error("API error ({status}): {message}")]
    Api {
        status: u16,
        message: String,
        /// What the server's `Retry-After` header asked for, when it sent one.
        retry_after: Option<Duration>,
        /// The request does not fit the model's context, as the provider
        /// read the refusal. Compacting the session is the remedy.
        overflow: bool,
    },
    #[error("{message}")]
    Config { message: String },
    #[error("tool error in {tool}: {message}")]
    Tool { tool: String, message: String },
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
    #[error("channel send failed")]
    Channel,
    #[error("cancelled")]
    Cancelled,
    #[error("stream timed out: {message}")]
    Timeout { message: String },
    #[error("compaction returned no summary")]
    EmptySummary,
}

impl AgentError {
    /// An API error with no `Retry-After` behind it. Everything that is not a
    /// response we read the headers of lands here, SSE error frames included.
    pub fn api(status: u16, message: impl Into<String>) -> Self {
        Self::Api {
            status,
            message: message.into(),
            retry_after: None,
            overflow: false,
        }
    }

    /// Which budget trying again would draw from, or `None` when trying again
    /// is pointless. The kinds are priced differently, so the retry loop needs
    /// more than a yes or no.
    pub fn retry_kind(&self) -> Option<RetryKind> {
        if self.is_context_overflow() {
            return None;
        }
        match self {
            Self::Api { status: 429, .. } => Some(RetryKind::RateLimit),
            Self::Api { status, .. } if *status >= 500 => Some(RetryKind::Transient),
            Self::Io(e) if e.kind() == io::ErrorKind::ConnectionRefused => Some(RetryKind::Connect),
            Self::Io(_) => Some(RetryKind::Transient),
            Self::Timeout { .. } => Some(RetryKind::Timeout),
            Self::Api { .. }
            | Self::Config { .. }
            | Self::Tool { .. }
            | Self::Channel
            | Self::Json(_)
            | Self::Cancelled
            | Self::EmptySummary => None,
        }
    }

    pub fn is_retryable(&self) -> bool {
        self.retry_kind().is_some()
    }

    pub fn is_context_overflow(&self) -> bool {
        matches!(self, Self::Api { overflow: true, .. })
    }

    pub fn is_auth_error(&self) -> bool {
        matches!(self, Self::Api { status: 401, .. })
    }

    pub fn should_rotate_key(&self) -> bool {
        matches!(self, Self::Api { status, .. } if *status == 429 || *status == 401 || *status == 403)
    }

    pub fn user_message(&self) -> String {
        match self {
            Self::Config { message } => message.clone(),
            Self::Api { status: 429, .. } => "rate limited, try again in a moment".into(),
            Self::Api { status: 529, .. } => "provider is overloaded, try again later".into(),
            Self::Api { status, .. } if *status >= 500 => format!("server error ({status})"),
            Self::Api { status: 401, .. } => {
                "authentication failed, run `makima auth login` or check your API key".into()
            }
            Self::Api {
                status, message, ..
            } => format!("API error ({status}): {message}"),
            Self::Tool { tool, message } => format!("{tool}: {message}"),
            Self::Io(e) if e.kind() == io::ErrorKind::ConnectionRefused => {
                CONNECT_FAILED_MESSAGE.into()
            }
            Self::Io(e) => format!("I/O error: {e}"),
            Self::Timeout { .. } => "stream timed out, retrying".into(),
            Self::Json(_) => "received an invalid response from the API".into(),
            Self::Channel => "internal error, try again".into(),
            Self::Cancelled => "cancelled".into(),
            Self::EmptySummary => "compaction returned no summary, history kept as is".into(),
        }
    }

    /// How long the server asked us to wait, when it bothered to say. `None`
    /// leaves the caller to its own backoff.
    pub fn retry_after(&self) -> Option<Duration> {
        match self {
            Self::Api { retry_after, .. } => *retry_after,
            _ => None,
        }
    }

    pub fn retry_message(&self) -> String {
        match self {
            Self::Api { status: 429, .. } => "Rate limited".into(),
            Self::Api { status: 529, .. } => "Provider is overloaded".into(),
            Self::Api { status, .. } if *status >= 500 => format!("Server error ({status})"),
            Self::Io(_) => "Connection error".into(),
            Self::Timeout { .. } => "Stream timed out".into(),
            _ => self.to_string(),
        }
    }
}

impl<T> From<flume::SendError<T>> for AgentError {
    fn from(_: flume::SendError<T>) -> Self {
        Self::Channel
    }
}

impl From<maki_storage::StorageError> for AgentError {
    fn from(e: maki_storage::StorageError) -> Self {
        match e {
            maki_storage::StorageError::Io(io) => Self::Io(io),
            maki_storage::StorageError::Json(j) => Self::Json(j),
            other => Self::api(0, other.to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use maki_config::DEFAULT_MAX_RETRIES;
    use test_case::test_case;

    use super::*;
    use crate::retry::{RetryPolicy, RetryState};

    /// More rounds than any bounded budget allows, so a budget that survives
    /// all of them is the unbounded one.
    const ROUNDS: u32 = DEFAULT_MAX_RETRIES * 2;

    fn api(status: u16) -> AgentError {
        AgentError::api(status, "")
    }

    fn timeout() -> AgentError {
        AgentError::Timeout {
            message: String::new(),
        }
    }

    #[test_case(429, RetryKind::RateLimit ; "rate_limit")]
    #[test_case(500, RetryKind::Transient  ; "server_error")]
    #[test_case(529, RetryKind::Transient  ; "overloaded")]
    fn api_retry_kind(status: u16, expected: RetryKind) {
        assert_eq!(api(status).retry_kind(), Some(expected));
    }

    #[test_case(429, true  ; "rate_limit")]
    #[test_case(500, true  ; "server_error")]
    #[test_case(529, true  ; "overloaded")]
    #[test_case(400, false ; "bad_request")]
    #[test_case(401, false ; "unauthorized")]
    fn api_retryable(status: u16, expected: bool) {
        assert_eq!(api(status).is_retryable(), expected);
    }

    #[test]
    fn a_timeout_is_its_own_kind() {
        assert_eq!(timeout().retry_kind(), Some(RetryKind::Timeout));
    }

    /// Retries this error is granted before the loop has to give up, stopping
    /// at `ROUNDS` for a budget with no ceiling.
    fn retries_granted(error: &AgentError) -> u32 {
        let kind = error.retry_kind().expect("a transport failure retries");
        let mut state = RetryState::new(RetryPolicy::default(), 1);
        (0..ROUNDS)
            .take_while(|_| state.next_delay(kind, error.retry_after()).is_some())
            .count() as u32
    }

    #[test_case(io::ErrorKind::ConnectionRefused, DEFAULT_MAX_RETRIES ; "a_refused_socket_gives_up")]
    #[test_case(io::ErrorKind::UnexpectedEof, ROUNDS                  ; "a_truncated_read_keeps_retrying")]
    fn an_io_failure_gives_up_when_nothing_is_listening(kind: io::ErrorKind, expected: u32) {
        assert_eq!(retries_granted(&AgentError::Io(kind.into())), expected);
    }

    #[test]
    fn a_refused_connection_names_the_server() {
        let error = AgentError::Io(io::ErrorKind::ConnectionRefused.into());
        assert_eq!(error.user_message(), CONNECT_FAILED_MESSAGE);
    }

    #[test_case(401, true  ; "unauthorized")]
    #[test_case(403, false ; "forbidden")]
    fn api_auth_error(status: u16, expected: bool) {
        assert_eq!(api(status).is_auth_error(), expected);
    }

    #[test_case(429, "Rate limited"        ; "rate_limited")]
    #[test_case(529, "Provider is overloaded" ; "overloaded")]
    #[test_case(500, "Server error (500)"  ; "server_error")]
    fn retry_message_api(status: u16, expected: &str) {
        assert_eq!(api(status).retry_message(), expected);
    }

    #[test_case(429, "rate limited, try again in a moment"                              ; "user_msg_429")]
    #[test_case(529, "provider is overloaded, try again later"                           ; "user_msg_529")]
    #[test_case(500, "server error (500)"                                                 ; "user_msg_500")]
    #[test_case(401, "authentication failed, run `makima auth login` or check your API key" ; "user_msg_401")]
    #[test_case(400, "API error (400): bad input"                                         ; "user_msg_400")]
    fn user_message_api(status: u16, expected: &str) {
        let err = AgentError::api(status, "bad input");
        assert_eq!(err.user_message(), expected);
    }

    #[test]
    fn timeout_is_retryable() {
        assert!(timeout().is_retryable());
    }

    #[test]
    fn context_overflow_is_not_retryable() {
        let err = AgentError::Api {
            status: 400,
            message: String::new(),
            retry_after: None,
            overflow: true,
        };
        assert!(err.is_context_overflow());
        assert!(!err.is_retryable());
    }
}
