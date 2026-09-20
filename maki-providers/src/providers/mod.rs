use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use futures_lite::StreamExt;
use futures_lite::io::AsyncBufRead;
use isahc::config::Configurable;
use isahc::http::request::Builder;
use serde::Deserialize;
use serde_json::Value;
use tracing::{debug, warn};

use maki_storage::StateDir;
use maki_storage::auth::{OAuthTokens, load_tokens, lock_tokens, save_tokens};

use crate::AgentError;

pub(crate) mod anthropic;
pub(crate) mod aperture;
pub(crate) mod catalog;
pub(crate) mod copilot;
pub mod custom;
pub(crate) mod deepseek;
pub mod dynamic;
pub(crate) mod google;
pub(crate) mod llama_cpp;
pub(crate) mod local;
pub(crate) mod mistral;
pub(crate) mod ollama;
pub(crate) mod openai;
pub(crate) mod openai_compat;
pub mod opencode;
pub(crate) mod openrouter;
pub(crate) mod regolo;
pub(crate) mod requesty;
pub(crate) mod synthetic;
pub(crate) mod tensorx;
pub(crate) mod vertex;
pub(crate) mod zai;

const LOW_SPEED_BYTES_PER_SEC: u32 = 1;
const UNMAPPED_SSE_ERROR_STATUS: u16 = 400;
const EMPTY_SSE_ERROR_MESSAGE: &str = "provider sent an error frame with no detail";
const UNAUTHORIZED_STATUS: u16 = 401;
const AUTHORIZATION_HEADER: &str = "authorization";

pub(crate) fn user_agent() -> &'static str {
    concat!(
        "maki/v",
        env!("CARGO_PKG_VERSION"),
        "-g",
        env!("GIT_SHORT_HASH")
    )
}

fn bearer_value(api_key: &str) -> String {
    format!("Bearer {api_key}")
}

#[derive(Debug, Clone, Copy)]
pub struct Timeouts {
    pub connect: Duration,
    pub low_speed: Duration,
    pub stream: Duration,
    pub retry: crate::retry::RetryPolicy,
}

impl Default for Timeouts {
    fn default() -> Self {
        Self {
            connect: Duration::from_secs(10),
            low_speed: Duration::from_secs(30),
            stream: Duration::from_secs(300),
            retry: crate::retry::RetryPolicy::default(),
        }
    }
}

impl From<&maki_config::ProviderConfig> for Timeouts {
    fn from(config: &maki_config::ProviderConfig) -> Self {
        Self {
            connect: config.connect_timeout,
            low_speed: config.low_speed_timeout,
            stream: config.stream_timeout,
            retry: crate::retry::RetryPolicy::from(config),
        }
    }
}

/// Reading, refreshing and writing tokens has to happen as one turn. Whoever
/// queued behind a peer here is holding a copy the peer already spent, and
/// replaying a rotated refresh token gets the whole family revoked, so the
/// tokens are loaded again once the lock is in hand.
pub(crate) fn refreshed_tokens(
    dir: &StateDir,
    provider: &str,
    refresh: impl FnOnce(&OAuthTokens) -> Result<OAuthTokens, AgentError>,
) -> Result<OAuthTokens, AgentError> {
    let _lock = lock_tokens(dir, provider);
    let current = load_tokens(dir, provider).ok_or_else(|| {
        AgentError::api(
            UNAUTHORIZED_STATUS,
            format!("{provider} OAuth tokens not found on disk"),
        )
    })?;
    if !current.is_expired() {
        return Ok(current);
    }
    let fresh = refresh(&current)?;
    // Not `?`: every caller reads an error here as "these credentials are
    // dead" and deletes the token file, so a full disk would log the user out
    // over a refresh that actually succeeded. The run keeps the token it just
    // got and the next start refreshes again.
    if let Err(e) = save_tokens(dir, provider, &fresh) {
        warn!(provider, error = %e, "could not persist refreshed OAuth tokens");
    }
    Ok(fresh)
}

#[derive(Clone)]
pub struct ResolvedAuth {
    pub base_url: Option<String>,
    pub headers: Vec<(String, String)>,
}

impl ResolvedAuth {
    pub fn bearer(api_key: &str) -> Self {
        Self {
            base_url: None,
            headers: vec![("authorization".into(), format!("Bearer {api_key}"))],
        }
    }

    /// Apply all auth headers to an HTTP request builder.
    pub fn configure_request(&self, builder: Builder) -> Builder {
        self.headers.iter().fold(builder, |b, (key, value)| {
            b.header(key.as_str(), value.as_str())
        })
    }

    pub(crate) fn set_header(&mut self, name: &str, value: String) {
        match self
            .headers
            .iter_mut()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
        {
            Some((_, v)) => *v = value,
            None => self.headers.push((name.to_string(), value)),
        }
    }

    fn set_key_header(&mut self, name: &str, value: String) {
        self.set_header(name, value);
    }
}

pub(crate) fn with_prefix<'a>(
    prefix: &Option<String>,
    system: &'a str,
    buf: &'a mut String,
) -> &'a str {
    match prefix {
        Some(p) => {
            *buf = format!("{p}\n\n{system}");
            buf
        }
        None => system,
    }
}

pub(crate) fn urlenc(s: &str) -> String {
    let mut out = String::with_capacity(s.len() * 2);
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            _ => {
                out.push('%');
                out.push_str(&format!("{b:02X}"));
            }
        }
    }
    out
}

#[derive(Deserialize)]
pub(crate) struct SseErrorPayload {
    pub error: SseErrorDetail,
}

/// Every field is optional because rejecting any one shape throws away the whole error, and a
/// half-filled error frame still tells us an outage happened. `code` in particular arrives as a
/// string, a number or `null` depending on the provider.
#[derive(Deserialize)]
pub(crate) struct SseErrorDetail {
    #[serde(default)]
    pub r#type: String,
    #[serde(default)]
    pub code: Value,
    #[serde(default)]
    pub message: String,
    #[serde(default)]
    pub metadata: Option<SseErrorMetadata>,
}

/// OpenRouter puts its machine-readable tag here rather than in `type`.
#[derive(Deserialize)]
pub(crate) struct SseErrorMetadata {
    #[serde(default)]
    pub error_type: String,
}

/// A streamed error rides inside a plain 200 response, so this tag is the only clue we get about
/// what went wrong and whether waiting will help.
pub(crate) fn sse_error_status(tag: &str) -> Option<u16> {
    Some(match tag {
        "overloaded_error" | "server_is_overloaded" => 529,
        "service_unavailable_error" | "provider_overloaded" => 503,
        "provider_unavailable" => 502,
        "api_error" | "server_error" => 500,
        "rate_limit_error" | "rate_limit_exceeded" | "tokens" => 429,
        "request_too_large" => 413,
        "not_found_error" => 404,
        "permission_error" => 403,
        "billing_error" | "insufficient_quota" => 402,
        "authentication_error" | "invalid_api_key" => 401,
        _ => return None,
    })
}

/// A numeric `code` is a literal HTTP status, which some routers (OpenRouter) send instead of a
/// tag. Reading it as a tag would discard the only signal about whether a retry can help.
fn code_status(code: &Value) -> Option<u16> {
    let status = match code {
        Value::Number(n) => u16::try_from(n.as_u64()?).ok()?,
        Value::String(s) => s.parse().ok()?,
        _ => return None,
    };
    (100..600).contains(&status).then_some(status)
}

impl SseErrorPayload {
    pub fn into_agent_error(self) -> AgentError {
        let status = sse_error_status(self.error.code.as_str().unwrap_or_default())
            .or_else(|| code_status(&self.error.code))
            .or_else(|| sse_error_status(&self.error.r#type))
            .or_else(|| {
                self.error
                    .metadata
                    .as_ref()
                    .and_then(|m| sse_error_status(&m.error_type))
            })
            .unwrap_or(UNMAPPED_SSE_ERROR_STATUS);
        let message = if self.error.message.trim().is_empty() {
            EMPTY_SSE_ERROR_MESSAGE.to_string()
        } else {
            self.error.message
        };
        AgentError::api(status, message)
    }
}

pub(crate) async fn next_sse_line<R: AsyncBufRead + Unpin>(
    lines: &mut futures_lite::io::Lines<R>,
    deadline: &mut Instant,
    stream_timeout: Duration,
) -> Result<Option<String>, AgentError> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    let result = futures_lite::future::or(
        async { lines.next().await.transpose().map_err(AgentError::from) },
        async {
            smol::Timer::after(remaining).await;
            Err(AgentError::Timeout {
                secs: stream_timeout.as_secs(),
            })
        },
    )
    .await;
    if let Ok(Some(_)) = &result {
        *deadline = Instant::now() + stream_timeout;
    }
    result
}

pub(crate) fn http_client(timeouts: Timeouts) -> isahc::HttpClient {
    isahc::HttpClient::builder()
        .connect_timeout(timeouts.connect)
        .low_speed_timeout(LOW_SPEED_BYTES_PER_SEC, timeouts.low_speed)
        // libcurl sends `Expect: 100-continue` by default for HTTP/1.1 POST bodies
        // over 1 KB. Edge proxies (notably Google's for Gemini/Vertex) reject this
        // preflight check on large payloads with 417 Expectation Failed.
        .expect_continue(false)
        .build()
        .expect("failed to build HTTP client")
}

#[derive(Clone, Debug)]
pub struct KeyPool {
    keys: Arc<Vec<String>>,
    index: Arc<AtomicUsize>,
}

impl KeyPool {
    pub fn from_env(env_var: &str) -> Result<Self, AgentError> {
        let raw = std::env::var(env_var).map_err(|_| AgentError::Config {
            message: format!("{env_var} not set"),
        })?;
        let keys: Vec<String> = raw
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
        if keys.is_empty() {
            return Err(AgentError::Config {
                message: format!("{env_var} is empty"),
            });
        }
        Ok(Self {
            keys: Arc::new(keys),
            index: Arc::new(AtomicUsize::new(0)),
        })
    }

    pub fn resolve(slug: &str, env_var: &str) -> Result<Self, AgentError> {
        if let Ok(pool) = Self::from_env(env_var) {
            debug!(slug, keys = pool.len(), "resolved API key from env");
            return Ok(pool);
        }
        if let Some(key) = Self::key_from_file(slug) {
            debug!(slug, "resolved API key from saved credentials");
            return Ok(Self::from_keys(vec![key]));
        }
        if let Some(key) = Self::key_from_config(slug) {
            debug!(slug, "resolved API key from providers.toml");
            return Ok(Self::from_keys(vec![key]));
        }
        Err(AgentError::Config {
            message: format!(
                "{env_var} not set and no saved credentials for '{slug}' — run `maki auth login {slug}`"
            ),
        })
    }

    fn key_from_file(slug: &str) -> Option<String> {
        let dir = maki_storage::StateDir::resolve().ok()?;
        maki_storage::auth::load_provider_credentials(&dir, slug).map(|c| c.api_key)
    }

    fn key_from_config(slug: &str) -> Option<String> {
        maki_config::providers::ProvidersConfig::load()
            .get(slug)
            .and_then(|d| d.api_key.clone())
    }

    pub fn from_keys(keys: Vec<String>) -> Self {
        Self {
            keys: Arc::new(keys),
            index: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn current(&self) -> &str {
        &self.keys[self.index.load(Ordering::Relaxed) % self.keys.len()]
    }

    pub fn rotate(&self) -> bool {
        if self.keys.len() <= 1 {
            return false;
        }
        self.index.fetch_add(1, Ordering::Relaxed);
        true
    }

    pub fn rotate_auth(
        &self,
        auth: &Mutex<ResolvedAuth>,
        build: impl FnOnce(&str) -> ResolvedAuth,
    ) -> bool {
        if !self.rotate() {
            return false;
        }
        *auth.lock().unwrap() = build(self.current());
        true
    }

    pub fn rotate_headers(
        &self,
        auth: &Mutex<ResolvedAuth>,
        build: impl FnOnce(&str) -> Vec<(String, String)>,
    ) -> bool {
        if !self.rotate() {
            return false;
        }
        auth.lock().unwrap().headers = build(self.current());
        true
    }

    pub fn len(&self) -> usize {
        self.keys.len()
    }

    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }
}

/// Where a provider's key lands in its auth headers. Two shapes cover every
/// provider we have, and an enum keeps "how a key becomes a header" in one
/// place instead of one closure per provider.
#[derive(Clone, Copy)]
pub enum KeyHeader {
    /// `Authorization: Bearer <key>`.
    Bearer,
    /// The key verbatim, in a provider specific header (`x-api-key`,
    /// `x-goog-api-key`).
    Raw(&'static str),
}

impl KeyHeader {
    fn name(self) -> &'static str {
        match self {
            Self::Bearer => AUTHORIZATION_HEADER,
            Self::Raw(name) => name,
        }
    }

    fn value(self, key: &str) -> String {
        match self {
            Self::Bearer => bearer_value(key),
            Self::Raw(_) => key.to_string(),
        }
    }
}

/// A provider's keys and the auth they are written into.
pub struct KeyRotation<'a> {
    pool: &'a KeyPool,
    auth: &'a Mutex<ResolvedAuth>,
    header: KeyHeader,
}

impl<'a> KeyRotation<'a> {
    pub fn new(pool: &'a KeyPool, auth: &'a Mutex<ResolvedAuth>, header: KeyHeader) -> Self {
        Self { pool, auth, header }
    }

    /// How many keys a walk can try before it is back where it started.
    pub fn key_count(&self) -> usize {
        self.pool.len()
    }

    /// Advance to the next key and refresh only the header carrying it, so the
    /// resolved `base_url` and any `[<slug>.headers]` survive the rotation.
    pub fn rotate(&self) -> bool {
        if !self.pool.rotate() {
            return false;
        }
        self.auth
            .lock()
            .unwrap()
            .set_key_header(self.header.name(), self.header.value(self.pool.current()));
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_lite::io::AsyncBufReadExt;
    use test_case::test_case;

    const ERROR_MESSAGE: &str = "Our servers are currently overloaded. Please try again later.";
    const PARSE_FAILED: &str = "SSE error payload should deserialize";
    const UNAVAILABLE_STATUS: u16 = 502;
    const UNAVAILABLE_TAG: &str = "provider_unavailable";

    // Codex only admits the overload in `code`, and anything we cannot place has to stay a plain
    // 400 so a user mistake is not retried forever: https://github.com/tontinton/maki/issues/777
    #[test_case(r#""type":"service_unavailable_error","code":"server_is_overloaded""#, 529, true  ; "code_beats_type")]
    #[test_case(r#""type":"service_unavailable_error""#,                               503, true  ; "absent_code")]
    #[test_case(r#""type":"service_unavailable_error","code":null"#,                   503, true  ; "null_code")]
    #[test_case(r#""type":"rate_limit_error","code":429"#,                             429, true  ; "numeric_code")]
    #[test_case(r#""code":429"#,                                                       429, true  ; "numeric_code_alone")]
    #[test_case(r#""code":502,"metadata":{"error_type":"provider_unavailable"}"#,      502, true  ; "openrouter_provider_unavailable")]
    #[test_case(r#""code":null,"metadata":{"error_type":"provider_unavailable"}"#,     502, true  ; "metadata_provider_unavailable")]
    #[test_case(r#""code":null,"metadata":{"error_type":"provider_overloaded"}"#,      503, true  ; "metadata_provider_overloaded")]
    #[test_case(r#""code":null,"metadata":{"error_type":"rate_limit_exceeded"}"#,      429, true  ; "metadata_rate_limit")]
    #[test_case(r#""code":401"#,                                                       401, false ; "numeric_auth_status")]
    #[test_case(r#""type":"invalid_request_error","code":"invalid_value""#,            400, false ; "unknown_tags")]
    fn sse_error_payload_status(tags: &str, status: u16, retryable: bool) {
        let payload: SseErrorPayload = serde_json::from_str(&format!(
            r#"{{"error":{{{tags},"message":"{ERROR_MESSAGE}"}}}}"#
        ))
        .expect(PARSE_FAILED);
        let err = payload.into_agent_error();

        assert_eq!(
            err.to_string(),
            format!("API error ({status}): {ERROR_MESSAGE}")
        );
        assert_eq!(err.is_retryable(), retryable);
    }

    // A frame that only says "the upstream is down" must survive parsing, or the turn ends with an
    // empty assistant message and no retry.
    #[test_case(Some(ERROR_MESSAGE), ERROR_MESSAGE           ; "message_present")]
    #[test_case(None,                EMPTY_SSE_ERROR_MESSAGE ; "message_key_absent")]
    #[test_case(Some(""),            EMPTY_SSE_ERROR_MESSAGE ; "message_empty")]
    #[test_case(Some("   "),         EMPTY_SSE_ERROR_MESSAGE ; "message_blank")]
    fn sse_error_payload_without_message_still_classifies(message: Option<&str>, expected: &str) {
        let mut error = serde_json::json!({
            "code": UNAVAILABLE_STATUS,
            "metadata": { "error_type": UNAVAILABLE_TAG },
        });
        if let Some(message) = message {
            error["message"] = message.into();
        }
        let payload: SseErrorPayload =
            serde_json::from_value(serde_json::json!({ "error": error })).expect(PARSE_FAILED);
        let err = payload.into_agent_error();

        assert_eq!(
            err.to_string(),
            format!("API error ({UNAVAILABLE_STATUS}): {expected}")
        );
        assert!(err.is_retryable());
    }
    #[test_case("a b", "a%20b" ; "space")]
    #[test_case("a:b", "a%3Ab" ; "colon")]
    #[test_case("abc", "abc"   ; "passthrough")]
    fn urlenc_encodes(input: &str, expected: &str) {
        assert_eq!(urlenc(input), expected);
    }

    struct NeverReader;

    impl futures_lite::io::AsyncRead for NeverReader {
        fn poll_read(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
            _buf: &mut [u8],
        ) -> std::task::Poll<std::io::Result<usize>> {
            std::task::Poll::Pending
        }
    }

    impl futures_lite::io::AsyncBufRead for NeverReader {
        fn poll_fill_buf(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::io::Result<&[u8]>> {
            std::task::Poll::Pending
        }

        fn consume(self: std::pin::Pin<&mut Self>, _amt: usize) {}
    }

    #[test]
    fn next_sse_line_expired_deadline_returns_timeout() {
        smol::block_on(async {
            let mut lines = NeverReader.lines();
            let mut past = Instant::now() - Duration::from_secs(1);
            let stream_timeout = Duration::from_secs(300);
            let err = next_sse_line(&mut lines, &mut past, stream_timeout)
                .await
                .unwrap_err();
            assert!(matches!(err, AgentError::Timeout { .. }));
        })
    }

    #[test]
    fn key_pool_single_key_current() {
        let pool = KeyPool::from_keys(vec!["sk-1".into()]);
        assert_eq!(pool.current(), "sk-1");
        assert_eq!(pool.len(), 1);
    }

    #[test]
    fn key_pool_single_key_rotate_returns_false() {
        let pool = KeyPool::from_keys(vec!["sk-1".into()]);
        assert!(!pool.rotate());
        assert_eq!(pool.current(), "sk-1");
    }

    #[test]
    fn key_pool_multi_key_rotates() {
        let pool = KeyPool::from_keys(vec!["sk-1".into(), "sk-2".into(), "sk-3".into()]);
        assert_eq!(pool.current(), "sk-1");
        assert!(pool.rotate());
        assert_eq!(pool.current(), "sk-2");
        assert!(pool.rotate());
        assert_eq!(pool.current(), "sk-3");
    }

    #[test]
    fn key_pool_wraps_around() {
        let pool = KeyPool::from_keys(vec!["a".into(), "b".into()]);
        pool.rotate();
        pool.rotate();
        assert_eq!(pool.current(), "a");
    }

    #[test]
    fn resolve_from_env() {
        let env_var = format!("MAKI_TEST_KEY_{}", fastrand::u32(..));
        unsafe { std::env::set_var(&env_var, "from-env") };
        let pool = KeyPool::resolve("test_slug", &env_var).unwrap();
        unsafe { std::env::remove_var(&env_var) };
        assert_eq!(pool.current(), "from-env");
    }

    #[test]
    fn resolve_env_supports_comma_separated() {
        let env_var = format!("MAKI_TEST_MULTI_{}", fastrand::u32(..));
        unsafe { std::env::set_var(&env_var, "sk-1, sk-2, sk-3") };
        let pool = KeyPool::resolve("test_slug", &env_var).unwrap();
        unsafe { std::env::remove_var(&env_var) };
        assert_eq!(pool.current(), "sk-1");
        assert!(pool.rotate());
        assert_eq!(pool.current(), "sk-2");
    }

    #[test]
    fn resolve_returns_error_when_nothing_found() {
        let slug = format!("test_resolve_none_{}", fastrand::u32(..));
        let env_var = format!("MAKI_TEST_KEY_NONE_{}", fastrand::u32(..));
        let result = KeyPool::resolve(&slug, &env_var);
        assert!(result.is_err());
        let msg = format!("{result:?}");
        assert!(msg.contains(&env_var) || msg.contains(&slug));
    }

    #[test]
    fn http_client_omits_expect_continue_on_large_payload() {
        use std::io::{Read, Write};
        use std::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        let client = http_client(Timeouts::default());
        let body = vec![b'x'; 2048];
        let req = isahc::Request::post(format!("http://{addr}"))
            .body(body)
            .unwrap();

        let handle = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut buf = [0u8; 1024];
            let n = stream.read(&mut buf).unwrap();
            let received = String::from_utf8_lossy(&buf[..n]).to_ascii_lowercase();
            let _ = stream.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n");
            received
        });

        smol::block_on(async {
            let _ = client.send_async(req).await;
        });

        let received = handle.join().unwrap();
        assert!(
            !received.contains("100-continue"),
            "large payload should omit Expect: 100-continue, got:\n{received}"
        );
    }
}
