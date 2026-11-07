//! `maki.net.send`: the raw streaming HTTP primitive for trusted plugin code.
//! Everything transport specific stays private to this module, so isahc can be
//! swapped out without touching the Lua API.

use std::io;
use std::path::PathBuf;
use std::sync::{Arc, LazyLock, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use futures_lite::future;
use futures_lite::io::AsyncReadExt;
#[cfg(unix)]
use isahc::config::Dialer;
use isahc::config::{Configurable, RedirectPolicy};
use isahc::error::ErrorKind;
use isahc::{AsyncBody, HttpClient, Request};
use maki_lua_macro::{lua_class, lua_fn};
use mlua::{
    Lua, Result as LuaResult, String as LuaString, Table, UserDataFields, UserDataRefMut,
    Value as LuaValue,
};
use smol::Timer;

use crate::api::base64::bytes_arg;
use crate::api::net::parse_http_url;
use crate::api::util::pair::{Pair, err_pair, try_pair};
use crate::runtime::register_defer_close;

const CHUNK_SIZE: usize = 64 * 1024;
const DEFAULT_METHOD: &str = "GET";
const DEFAULT_CONNECT_TIMEOUT_SECS: f64 = 10.0;
const DEFAULT_LOW_SPEED_TIMEOUT_SECS: f64 = 30.0;
const DEFAULT_IDLE_TIMEOUT_SECS: f64 = 300.0;
const LOW_SPEED_BYTES_PER_SEC: u32 = 1;
const USER_AGENT_HEADER: &str = "user-agent";
const USER_AGENT: &str = concat!("maki/", env!("CARGO_PKG_VERSION"));
const HEADER_VALUE_SEPARATOR: &str = ", ";
const CLOSED_ERR: &str = "response is closed";
/// Starts the error for a connection that was never established: refused, or
/// a host name that does not resolve. `http.classify` reads it back to pick
/// the retry budget.
const CONNECT_FAILED_ERR: &str = "could not connect";
/// The error for a read that got no byte within `idle_timeout`.
/// `http.classify` reads it back to pick the retry budget.
const IDLE_TIMEOUT_ERR: &str = "idle timeout waiting for response body";
#[cfg(not(unix))]
const UNIX_SOCKET_UNSUPPORTED_ERR: &str = "unix_socket is not supported on this platform";

/// One pooled client for every plugin. libcurl sends `Expect: 100-continue`
/// for HTTP/1.1 bodies over 1 KB by default, and edge proxies (notably
/// Google's) answer that preflight with 417 Expectation Failed.
static CLIENT: LazyLock<Result<HttpClient, String>> = LazyLock::new(|| {
    HttpClient::builder()
        .redirect_policy(RedirectPolicy::None)
        .expect_continue(false)
        .build()
        .map_err(|e| format!("HTTP client unavailable: {e}"))
});

struct SendParams {
    url: String,
    method: String,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
    connect_timeout: Duration,
    low_speed_timeout: Duration,
    idle_timeout: Duration,
    unix_socket: Option<PathBuf>,
}

impl SendParams {
    fn from_table(request: &Table) -> LuaResult<Self> {
        let headers = match request.get::<Option<Table>>("headers")? {
            Some(tbl) => tbl.pairs::<String, String>().collect::<LuaResult<_>>()?,
            None => Vec::new(),
        };
        let body = match request.get::<LuaValue>("body")? {
            LuaValue::Nil => Vec::new(),
            body => bytes_arg(&body, "net.send body")?,
        };
        Ok(Self {
            url: request.get("url")?,
            method: request
                .get::<Option<String>>("method")?
                .unwrap_or_else(|| DEFAULT_METHOD.to_string()),
            headers,
            body,
            connect_timeout: timeout_field(
                request,
                "connect_timeout",
                DEFAULT_CONNECT_TIMEOUT_SECS,
            )?,
            low_speed_timeout: timeout_field(
                request,
                "low_speed_timeout",
                DEFAULT_LOW_SPEED_TIMEOUT_SECS,
            )?,
            idle_timeout: timeout_field(request, "idle_timeout", DEFAULT_IDLE_TIMEOUT_SECS)?,
            unix_socket: request
                .get::<Option<String>>("unix_socket")?
                .map(PathBuf::from),
        })
    }
}

fn timeout_field(request: &Table, field: &str, default_secs: f64) -> LuaResult<Duration> {
    let secs = request.get::<Option<f64>>(field)?.unwrap_or(default_secs);
    Duration::try_from_secs_f64(secs)
        .map_err(|e| mlua::Error::runtime(format!("net.send {field}: {e}")))
}

enum BodyState {
    Open(AsyncBody),
    /// Taken out by a read in flight. A read dropped mid-flight leaves it here,
    /// which reads as closed.
    Reading,
    Finished,
    Closed,
}

type SharedBody = Arc<Mutex<BodyState>>;

fn lock_body(body: &SharedBody) -> MutexGuard<'_, BodyState> {
    body.lock().unwrap_or_else(PoisonError::into_inner)
}

pub(crate) struct LuaResponse {
    status: u16,
    headers: Vec<(String, String)>,
    /// Shared with the opening task's close hook, so the transfer ends with
    /// that task even while Lua still holds the handle.
    body: SharedBody,
    idle_timeout: Duration,
    buf: Box<[u8]>,
}

impl LuaResponse {
    /// `Ok(None)` at end of stream. A failed read closes the body, because the
    /// stream is left at an unknown position.
    async fn next_chunk(&mut self) -> Result<Option<&[u8]>, String> {
        let mut body = {
            let mut state = lock_body(&self.body);
            match std::mem::replace(&mut *state, BodyState::Reading) {
                BodyState::Open(body) => body,
                BodyState::Finished => {
                    *state = BodyState::Finished;
                    return Ok(None);
                }
                other => {
                    *state = other;
                    return Err(CLOSED_ERR.to_string());
                }
            }
        };
        let idle_timeout = self.idle_timeout;
        let idle = async move {
            Timer::after(idle_timeout).await;
            Err(io::Error::new(io::ErrorKind::TimedOut, IDLE_TIMEOUT_ERR))
        };
        let read = future::or(body.read(&mut self.buf), idle).await;
        let mut state = lock_body(&self.body);
        if !matches!(*state, BodyState::Reading) {
            return Err(CLOSED_ERR.to_string());
        }
        match read {
            Ok(0) => {
                *state = BodyState::Finished;
                Ok(None)
            }
            Ok(n) => {
                *state = BodyState::Open(body);
                Ok(Some(&self.buf[..n]))
            }
            Err(e) => {
                *state = BodyState::Closed;
                Err(format!("read failed: {e}"))
            }
        }
    }
}

async fn open(params: SendParams) -> Result<LuaResponse, String> {
    let url = parse_http_url(&params.url)?;
    let client = CLIENT.as_ref()?;
    let mut builder = Request::builder()
        .method(params.method.as_str())
        .uri(url.as_str())
        .connect_timeout(params.connect_timeout)
        .low_speed_timeout(LOW_SPEED_BYTES_PER_SEC, params.low_speed_timeout);
    if let Some(path) = params.unix_socket {
        #[cfg(unix)]
        {
            builder = builder.dial(Dialer::unix_socket(path));
        }
        #[cfg(not(unix))]
        {
            let _ = path;
            return Err(UNIX_SOCKET_UNSUPPORTED_ERR.to_string());
        }
    }
    if !params
        .headers
        .iter()
        .any(|(name, _)| name.eq_ignore_ascii_case(USER_AGENT_HEADER))
    {
        builder = builder.header(USER_AGENT_HEADER, USER_AGENT);
    }
    for (name, value) in &params.headers {
        builder = builder.header(name.as_str(), value.as_str());
    }
    let request = builder
        .body(AsyncBody::from(params.body))
        .map_err(|e| format!("invalid request: {e}"))?;
    let response = client
        .send_async(request)
        .await
        .map_err(|e| match e.kind() {
            ErrorKind::ConnectionFailed | ErrorKind::NameResolution => {
                format!("{CONNECT_FAILED_ERR}: {e}")
            }
            _ => format!("request failed: {e}"),
        })?;

    let header_map = response.headers();
    let headers = header_map
        .keys()
        .map(|name| {
            let joined = header_map
                .get_all(name)
                .iter()
                .map(|v| String::from_utf8_lossy(v.as_bytes()))
                .collect::<Vec<_>>()
                .join(HEADER_VALUE_SEPARATOR);
            (name.as_str().to_string(), joined)
        })
        .collect();
    Ok(LuaResponse {
        status: response.status().as_u16(),
        headers,
        body: Arc::new(Mutex::new(BodyState::Open(response.into_body()))),
        idle_timeout: params.idle_timeout,
        buf: vec![0; CHUNK_SIZE].into_boxed_slice(),
    })
}

/// Send one HTTP request and return a streaming response. This is the raw
/// primitive for trusted plugin code, such as LLM providers: there is no
/// private address blocking, no https upgrade, no retry, no size cap, and
/// redirects are not followed (a 3xx comes back as is). Never pass it a URL
/// that came from the model without checking it first; use
/// `maki.net.request` for that.
///
/// The response arrives with the first body byte or when the transfer ends,
/// not at the end of the headers. A server that sends headers and then stalls
/// blocks `send` until its first byte or until `low_speed_timeout` fires.
///
/// A connection that is refused or a host name that does not resolve fails
/// with an error starting `could not connect`. A `read()` that times out
/// fails with an error containing `idle timeout`. The providers plugin's
/// `http.classify` reads these to name a provider error's retry class.
///
/// {request} fields:
///   `url` (string) Required, `http://` or `https://`.
///   `method` (string) HTTP verb (default `"GET"`).
///   `headers` (table) Header name/value pairs. `User-Agent` defaults to `maki/<version>`.
///   `body` (string|buffer) Request body, binary safe.
///   `connect_timeout` (number) Seconds to connect (default 10).
///   `low_speed_timeout` (number) Seconds without any received byte before
///   the transfer fails (default 30).
///   `idle_timeout` (number) Seconds a single `read()` may wait (default 300).
///   `unix_socket` (string) Connect through this Unix domain socket instead of
///   TCP. `url` still supplies the path and the Host header. Unix only.
///
/// @param request table Request description (see above).
/// @return (maki.net.Response?, string?) Response handle, or nil plus an error string.
/// @example
/// local res, err = maki.net.send({
///   url = "https://api.example.com/v1/stream",
///   method = "POST",
///   headers = { ["content-type"] = "application/json" },
///   body = maki.json.encode({ stream = true }),
/// })
/// if not res then return print(err) end
/// local parts = {}
/// while true do
///   local chunk, read_err = res:read()
///   if read_err then print(read_err) end
///   if not chunk then break end
///   table.insert(parts, chunk)
/// end
/// res:close()
#[lua_fn(guard = Net)]
async fn send(lua: Lua, request: Table) -> LuaResult<Pair<LuaResponse>> {
    let params = SendParams::from_table(&request)?;
    let response = try_pair!(open(params).await);
    let body = Arc::clone(&response.body);
    // Outside a task scope nothing ends the handle but `close` and GC.
    let _ = register_defer_close(
        &lua,
        Box::new(move || *lock_body(&body) = BodyState::Closed),
    );
    Ok((Some(response), None))
}

/// Read the next chunk of the body, up to 64 KiB. Returns nil at the end of
/// the stream, or nil plus an error when the read fails, when no byte arrives
/// within `idle_timeout`, or after `close()`.
///
/// @return (string?, string?) Binary chunk, nil at the end, or nil plus an error string.
/// @example
/// local chunk, err = res:read()
#[lua_fn]
async fn read(lua: Lua, mut this: UserDataRefMut<LuaResponse>) -> LuaResult<Pair<LuaString>> {
    match this.next_chunk().await {
        Ok(Some(chunk)) => Ok((Some(lua.create_string(chunk)?), None)),
        Ok(None) => Ok((None, None)),
        Err(e) => Ok(err_pair(e)),
    }
}

/// Drop the body and close the connection. Safe to call more than once.
/// The socket itself closes when the next byte arrives from the server, or
/// when `low_speed_timeout` fires.
///
/// @example
/// res:close()
#[lua_fn]
fn close(_lua: &Lua, this: &mut LuaResponse) -> LuaResult<()> {
    *lock_body(&this.body) = BodyState::Closed;
    Ok(())
}

fn response_fields<F: UserDataFields<LuaResponse>>(fields: &mut F) {
    fields.add_field_method_get("status", |_, this| Ok(this.status));
    fields.add_field_method_get("headers", |lua, this| {
        lua.create_table_from(
            this.headers
                .iter()
                .map(|(name, value)| (name.as_str(), value.as_str())),
        )
    });
}

lua_class! {
    /// A streaming HTTP response from `maki.net.send`.
    ///
    /// `status` is the integer status code. `headers` is a table keyed by
    /// lowercase header name, with repeated headers joined by `", "`. Read
    /// the body with `:read()`. Closing or dropping the handle, or the end of
    /// the task that sent the request, closes the connection once the next
    /// byte arrives from the server.
    "maki.net.Response" => LuaResponse, RESPONSE_DOCS [read, close] fields response_fields
}

#[cfg(test)]
mod tests {
    use std::io::{Read, Write};
    use std::net::{TcpListener, TcpStream};
    use std::thread::{self, JoinHandle};

    use flume::{Receiver, Sender};
    use futures_lite::future::poll_once;
    use test_case::test_case;

    use super::*;
    use crate::api::net::{NON_HTTP_URL_ERR, create_net_table};
    use crate::plugin_permissions::PluginPermissions;
    use crate::runtime::TaskScope;

    const TEST_TIMEOUT: Duration = Duration::from_secs(10);
    const SHORT_IDLE_SECS: f64 = 0.2;
    const CHUNKED_HEAD: &[u8] =
        b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nX-Test: a\r\nX-Test: b\r\n\r\n";
    const CHUNK_END: &[u8] = b"0\r\n\r\n";
    const CHUNKS: [&[u8]; 3] = [b"first", b"second", b"third"];
    const LARGE_BODY_LEN: usize = 6 * 1024 * 1024;
    const LARGE_POST_LEN: usize = 2048;
    const CUSTOM_UA: &str = "custom-agent/1.0";
    const BINARY_BODY: &[u8] = b"\0\xff\x80bin\0\xfe";
    const SERVER_ERROR: &[u8] =
        b"HTTP/1.1 500 Internal Server Error\r\nContent-Length: 4\r\nConnection: close\r\n\r\nboom";
    const EXPECT_CONTINUE: &str = "100-continue";
    const FTP_URL: &str = "ftp://example.com/";

    async fn bounded<T>(fut: impl Future<Output = T>) -> T {
        future::or(fut, async {
            Timer::after(TEST_TIMEOUT).await;
            panic!("test timed out after {TEST_TIMEOUT:?}");
        })
        .await
    }

    fn recv<T>(rx: &Receiver<T>) -> T {
        rx.recv_timeout(TEST_TIMEOUT)
            .expect("server step timed out")
    }

    fn params(url: &str) -> SendParams {
        SendParams {
            url: url.to_string(),
            method: DEFAULT_METHOD.to_string(),
            headers: Vec::new(),
            body: Vec::new(),
            connect_timeout: Duration::from_secs_f64(DEFAULT_CONNECT_TIMEOUT_SECS),
            low_speed_timeout: Duration::from_secs_f64(DEFAULT_LOW_SPEED_TIMEOUT_SECS),
            idle_timeout: Duration::from_secs_f64(DEFAULT_IDLE_TIMEOUT_SECS),
            unix_socket: None,
        }
    }

    /// Reads one request (head plus `Content-Length` body), returning the
    /// lowercased head and the raw body.
    fn read_request(stream: &mut TcpStream) -> (String, Vec<u8>) {
        let mut data = Vec::new();
        let mut buf = [0u8; 4096];
        let head_end = loop {
            if let Some(pos) = data.windows(4).position(|w| w == b"\r\n\r\n") {
                break pos + 4;
            }
            let n = stream.read(&mut buf).unwrap();
            assert!(n > 0, "client closed before sending a request");
            data.extend_from_slice(&buf[..n]);
        };
        let head = String::from_utf8_lossy(&data[..head_end]).to_ascii_lowercase();
        let body_len = head
            .lines()
            .find_map(|l| l.strip_prefix("content-length:"))
            .map_or(0, |v| v.trim().parse().unwrap());
        let mut body = data.split_off(head_end);
        while body.len() < body_len {
            let n = stream.read(&mut buf).unwrap();
            body.extend_from_slice(&buf[..n]);
        }
        (head, body)
    }

    fn chunk(data: &[u8]) -> Vec<u8> {
        [format!("{:x}\r\n", data.len()).as_bytes(), data, b"\r\n"].concat()
    }

    /// Accepts one connection and hands it, with the parsed request, to
    /// `handler`. The listener comes back from the join handle.
    fn serve<T: Send + 'static>(
        handler: impl FnOnce(TcpStream, String, Vec<u8>) -> T + Send + 'static,
    ) -> (String, JoinHandle<(TcpListener, T)>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}/", listener.local_addr().unwrap());
        let handle = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let (head, body) = read_request(&mut stream);
            let out = handler(stream, head, body);
            (listener, out)
        });
        (url, handle)
    }

    fn steps() -> (Sender<()>, Receiver<()>) {
        flume::bounded(1)
    }

    #[test]
    fn response_resolves_at_first_body_byte() {
        let (headers_sent_tx, headers_sent_rx) = steps();
        let (release_tx, release_rx) = steps();
        let (url, server) = serve(move |mut stream, _, _| {
            stream.write_all(CHUNKED_HEAD).unwrap();
            headers_sent_tx.send(()).unwrap();
            recv(&release_rx);
            stream.write_all(&chunk(CHUNKS[0])).unwrap();
            stream.write_all(CHUNK_END).unwrap();
        });
        smol::block_on(bounded(async {
            let mut pending = Box::pin(open(params(&url)));
            assert!(poll_once(&mut pending).await.is_none());
            headers_sent_rx.recv_async().await.unwrap();
            assert!(
                poll_once(&mut pending).await.is_none(),
                "send resolved before any body byte"
            );
            release_tx.send(()).unwrap();
            let mut response = pending.await.unwrap();
            assert_eq!(response.status, 200);
            assert!(
                response
                    .headers
                    .contains(&("x-test".to_string(), "a, b".to_string()))
            );
            assert_eq!(response.next_chunk().await.unwrap(), Some(CHUNKS[0]));
            assert_eq!(response.next_chunk().await.unwrap(), None);
        }));
        server.join().unwrap();
    }

    #[test]
    fn chunked_body_streams_across_reads() {
        let (next_tx, next_rx) = steps();
        let (url, server) = serve(move |mut stream, _, _| {
            stream.write_all(CHUNKED_HEAD).unwrap();
            for data in CHUNKS {
                stream.write_all(&chunk(data)).unwrap();
                recv(&next_rx);
            }
            stream.write_all(CHUNK_END).unwrap();
        });
        smol::block_on(bounded(async {
            let mut response = open(params(&url)).await.unwrap();
            for data in CHUNKS {
                assert_eq!(response.next_chunk().await.unwrap(), Some(data));
                next_tx.send(()).unwrap();
            }
            assert_eq!(response.next_chunk().await.unwrap(), None);
            assert_eq!(response.next_chunk().await.unwrap(), None);
        }));
        server.join().unwrap();
    }

    #[test]
    fn large_body_streams_without_a_cap() {
        let expected: Vec<u8> = (0..LARGE_BODY_LEN).map(|i| (i % 251) as u8).collect();
        let sent = expected.clone();
        let (url, server) = serve(move |mut stream, _, _| {
            let head = format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n", sent.len());
            stream.write_all(head.as_bytes()).unwrap();
            stream.write_all(&sent).unwrap();
        });
        smol::block_on(bounded(async {
            let mut response = open(params(&url)).await.unwrap();
            let mut received = Vec::with_capacity(LARGE_BODY_LEN);
            while let Some(data) = response.next_chunk().await.unwrap() {
                assert!(data.len() <= CHUNK_SIZE);
                received.extend_from_slice(data);
            }
            assert!(received == expected, "body differs");
        }));
        server.join().unwrap();
    }

    #[test]
    fn server_error_is_returned_without_retry() {
        let (url, server) = serve(|mut stream, _, _| stream.write_all(SERVER_ERROR).unwrap());
        smol::block_on(bounded(async {
            let mut request = params(&url);
            request.method = "POST".to_string();
            request.body = BINARY_BODY.to_vec();
            let mut response = open(request).await.unwrap();
            assert_eq!(response.status, 500);
            while response.next_chunk().await.unwrap().is_some() {}
        }));
        let (listener, ()) = server.join().unwrap();
        listener.set_nonblocking(true).unwrap();
        let second = listener.accept().map(|_| ());
        assert_eq!(
            second.unwrap_err().kind(),
            io::ErrorKind::WouldBlock,
            "client sent a second request"
        );
    }

    #[test_case(None, USER_AGENT ; "default_user_agent")]
    #[test_case(Some(CUSTOM_UA), CUSTOM_UA ; "caller_user_agent_overrides")]
    fn user_agent_header(caller: Option<&str>, expected: &str) {
        let (url, server) = serve(|mut stream, head, _| {
            stream
                .write_all(b"HTTP/1.1 204 No Content\r\n\r\n")
                .unwrap();
            head
        });
        smol::block_on(bounded(async {
            let mut request = params(&url);
            request.headers = caller
                .map(|ua| vec![("User-Agent".to_string(), ua.to_string())])
                .unwrap_or_default();
            open(request).await.unwrap();
        }));
        let (_, head) = server.join().unwrap();
        let agents: Vec<&str> = head
            .lines()
            .filter_map(|l| l.strip_prefix("user-agent:"))
            .map(str::trim)
            .collect();
        assert_eq!(agents, [expected.to_ascii_lowercase().as_str()]);
    }

    #[test]
    fn large_post_omits_expect_continue() {
        let (url, server) = serve(|mut stream, head, body| {
            stream
                .write_all(b"HTTP/1.1 204 No Content\r\n\r\n")
                .unwrap();
            (head, body)
        });
        smol::block_on(bounded(async {
            let mut request = params(&url);
            request.method = "POST".to_string();
            request.body = vec![b'x'; LARGE_POST_LEN];
            open(request).await.unwrap();
        }));
        let (_, (head, body)) = server.join().unwrap();
        assert!(!head.contains(EXPECT_CONTINUE), "{head}");
        assert_eq!(body.len(), LARGE_POST_LEN);
    }

    #[test]
    fn dropping_response_mid_body_closes_connection() {
        let (dropped_tx, dropped_rx) = steps();
        let (url, server) = serve(move |mut stream, _, _| {
            stream.write_all(CHUNKED_HEAD).unwrap();
            stream.write_all(&chunk(CHUNKS[0])).unwrap();
            recv(&dropped_rx);
            stream.write_all(&chunk(CHUNKS[1])).unwrap();
            stream.set_read_timeout(Some(TEST_TIMEOUT)).unwrap();
            stream.read(&mut [0u8; 1])
        });
        smol::block_on(bounded(async {
            let mut response = open(params(&url)).await.unwrap();
            assert_eq!(response.next_chunk().await.unwrap(), Some(CHUNKS[0]));
        }));
        dropped_tx.send(()).unwrap();
        let (_, read) = server.join().unwrap();
        assert!(matches!(read, Ok(0)), "connection still open: {read:?}");
    }

    #[test]
    fn idle_timeout_fails_the_read() {
        let (release_tx, release_rx) = steps();
        let (url, server) = serve(move |mut stream, _, _| {
            stream.write_all(CHUNKED_HEAD).unwrap();
            stream.write_all(&chunk(CHUNKS[0])).unwrap();
            recv(&release_rx);
        });
        smol::block_on(bounded(async {
            let mut request = params(&url);
            request.idle_timeout = Duration::from_secs_f64(SHORT_IDLE_SECS);
            let mut response = open(request).await.unwrap();
            assert_eq!(response.next_chunk().await.unwrap(), Some(CHUNKS[0]));
            let err = response.next_chunk().await.unwrap_err();
            assert!(err.contains(IDLE_TIMEOUT_ERR), "{err}");
            assert_eq!(response.next_chunk().await.unwrap_err(), CLOSED_ERR);
        }));
        release_tx.send(()).unwrap();
        server.join().unwrap();
    }

    #[test]
    fn refused_connection_is_a_connect_failure() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}/", listener.local_addr().unwrap());
        drop(listener);
        let err = smol::block_on(bounded(open(params(&url)))).err().unwrap();
        assert!(err.starts_with(CONNECT_FAILED_ERR), "{err}");
    }

    #[test]
    fn non_http_url_is_an_error() {
        let err = smol::block_on(open(params(FTP_URL))).err().unwrap();
        assert!(err.contains(NON_HTTP_URL_ERR), "{err}");
    }

    /// Through Lua, so the body is proven to cross the boundary as bytes in
    /// both directions, and `close` is proven idempotent.
    #[test]
    fn lua_binary_roundtrip_and_close() {
        let (url, server) = serve(|mut stream, _, body| {
            let head = format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n", body.len());
            stream.write_all(head.as_bytes()).unwrap();
            stream.write_all(&body).unwrap();
        });
        let lua = Lua::new();
        let net = create_net_table(&lua, &PluginPermissions::trusted()).unwrap();
        lua.globals().set("net", net).unwrap();
        lua.globals().set("url", url).unwrap();
        lua.globals()
            .set("payload", lua.create_string(BINARY_BODY).unwrap())
            .unwrap();
        let script = r#"
            local res, err = net.send({ url = url, method = "POST", body = payload })
            assert(res, err)
            assert(res.status == 200)
            assert(res.headers["content-length"] == tostring(#payload))
            local parts = {}
            while true do
                local chunk, read_err = res:read()
                assert(not read_err, read_err)
                if not chunk then break end
                table.insert(parts, chunk)
            end
            res:close()
            res:close()
            local after, closed_err = res:read()
            return table.concat(parts), after == nil, closed_err
        "#;
        let (body, nil_after_close, closed_err): (LuaString, bool, String) =
            smol::block_on(bounded(lua.load(script).eval_async())).unwrap();
        assert_eq!(&*body.as_bytes(), BINARY_BODY);
        assert!(nil_after_close);
        assert_eq!(closed_err, CLOSED_ERR);
        server.join().unwrap();
    }

    #[test]
    fn ending_the_sending_task_closes_the_response() {
        let (release_tx, release_rx) = steps();
        let (url, server) = serve(move |mut stream, _, _| {
            stream.write_all(CHUNKED_HEAD).unwrap();
            stream.write_all(&chunk(CHUNKS[0])).unwrap();
            recv(&release_rx);
        });
        let lua = Lua::new();
        let net = create_net_table(&lua, &PluginPermissions::trusted()).unwrap();
        lua.globals().set("net", net).unwrap();
        lua.globals().set("url", url).unwrap();
        let script = lua.load("return net.send({ url = url })");
        let response: LuaValue = smol::block_on(bounded(async {
            let scope = TaskScope::detached(&lua);
            let response = scope.scope_future(script.eval_async()).await.unwrap();
            drop(scope);
            response
        }));
        lua.globals().set("res", response).unwrap();
        let (chunk, err): (Option<LuaString>, String) =
            smol::block_on(bounded(lua.load("return res:read()").eval_async())).unwrap();
        assert!(chunk.is_none());
        assert_eq!(err, CLOSED_ERR);
        release_tx.send(()).unwrap();
        server.join().unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn unix_socket_carries_the_request() {
        use std::os::unix::net::UnixListener;

        const SOCKET_URL: &str = "http://bridge/v1/status";
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bridge.sock");
        let listener = UnixListener::bind(&path).unwrap();
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut buf = [0u8; 4096];
            let mut head = Vec::new();
            while !head.windows(4).any(|w| w == b"\r\n\r\n") {
                let n = stream.read(&mut buf).unwrap();
                assert!(n > 0, "client closed before sending a request");
                head.extend_from_slice(&buf[..n]);
            }
            stream
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nfirst")
                .unwrap();
            String::from_utf8_lossy(&head).to_ascii_lowercase()
        });
        smol::block_on(bounded(async {
            let mut response = open(SendParams {
                unix_socket: Some(path),
                ..params(SOCKET_URL)
            })
            .await
            .unwrap();
            assert_eq!(response.status, 200);
            assert_eq!(response.next_chunk().await.unwrap(), Some(CHUNKS[0]));
            assert_eq!(response.next_chunk().await.unwrap(), None);
        }));
        let head = server.join().unwrap();
        assert!(head.starts_with("get /v1/status "), "{head}");
        assert!(head.contains("host: bridge"), "{head}");
    }
}
