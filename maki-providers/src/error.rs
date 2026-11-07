//! Provider error types with retry semantics. The provider names how a
//! failure is retried; the host never derives it. `user_message()` returns
//! human-readable text for each variant.

use std::time::Duration;

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
        retry: Option<RetryKind>,
    },
    /// A provider failure with no response status.
    #[error("{message}")]
    Transport {
        message: String,
        retry: Option<RetryKind>,
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
    #[error("compaction returned no summary")]
    EmptySummary,
}

impl AgentError {
    /// An API error that is not retried and has no `Retry-After` behind it.
    pub fn api(status: u16, message: impl Into<String>) -> Self {
        Self::Api {
            status,
            message: message.into(),
            retry_after: None,
            overflow: false,
            retry: None,
        }
    }

    /// Which budget trying again would draw from, as the provider named it,
    /// or `None` when it is not retried. The kinds are priced differently, so
    /// the retry loop needs more than a yes or no.
    pub fn retry_kind(&self) -> Option<RetryKind> {
        match self {
            Self::Api { overflow: true, .. } => None,
            Self::Api { retry, .. } | Self::Transport { retry, .. } => *retry,
            Self::Config { .. }
            | Self::Tool { .. }
            | Self::Io(_)
            | Self::Json(_)
            | Self::Channel
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
            Self::Transport {
                retry: Some(RetryKind::Connect),
                ..
            } => CONNECT_FAILED_MESSAGE.into(),
            Self::Transport {
                retry: Some(RetryKind::Timeout),
                ..
            } => "stream timed out, retrying".into(),
            Self::Transport { message, .. } => format!("I/O error: {message}"),
            Self::Tool { tool, message } => format!("{tool}: {message}"),
            Self::Io(e) => format!("I/O error: {e}"),
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
            Self::Transport {
                retry: Some(RetryKind::Timeout),
                ..
            } => "Stream timed out".into(),
            Self::Transport { .. } => "Connection error".into(),
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
    use test_case::test_case;

    use super::*;

    fn api(status: u16) -> AgentError {
        AgentError::api(status, "")
    }

    fn transport(retry: Option<RetryKind>) -> AgentError {
        AgentError::Transport {
            message: String::new(),
            retry,
        }
    }

    #[test_case(Some(RetryKind::RateLimit) ; "named")]
    #[test_case(None                       ; "unnamed")]
    fn the_named_class_is_the_retry_kind(retry: Option<RetryKind>) {
        let api = AgentError::Api {
            status: 500,
            message: String::new(),
            retry_after: None,
            overflow: false,
            retry,
        };
        assert_eq!(api.retry_kind(), retry);
        assert_eq!(transport(retry).retry_kind(), retry);
    }

    #[test]
    fn a_server_error_without_a_class_is_not_retried() {
        assert!(!api(500).is_retryable());
    }

    #[test]
    fn context_overflow_is_never_retried() {
        let err = AgentError::Api {
            status: 400,
            message: String::new(),
            retry_after: None,
            overflow: true,
            retry: Some(RetryKind::Transient),
        };
        assert!(err.is_context_overflow());
        assert!(!err.is_retryable());
    }

    #[test_case(Some(RetryKind::Connect), CONNECT_FAILED_MESSAGE      ; "connect_names_the_server")]
    #[test_case(Some(RetryKind::Timeout), "stream timed out, retrying" ; "timeout")]
    fn transport_user_message(retry: Option<RetryKind>, expected: &str) {
        assert_eq!(transport(retry).user_message(), expected);
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
}
