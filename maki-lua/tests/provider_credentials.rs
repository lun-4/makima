use std::future::Future;
use std::io::{BufRead, BufReader, Write};
use std::net::TcpListener;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use futures_lite::future;
use maki_agent::tools::ToolRegistry;
use maki_lua::PluginHost;
use maki_lua::test_support::InMemoryFs;
use maki_lua::ui_sink::HostUi;
use maki_providers::{
    ContentBlock, Invocation, InvocationPurpose, PreparedProvider, RequestOptions,
};
use maki_storage::StateDir;
use maki_storage::auth::{
    OAuthTokens, ProviderCredentials, load_credentials, load_provider_credentials, load_tokens,
    lock_tokens, save_provider_credentials, save_tokens,
};
use serde_json::json;
use tempfile::TempDir;
use test_case::test_case;

const PROVIDER: &str = "fake";
const PLUGIN: &str = "fake_plugin";
const SPEC: &str = "fake/fake-1";
const BOUND: Duration = Duration::from_secs(10);
const API_KEY: &str = "sk-written-by-lua";
const OTHER_API_KEY: &str = "sk-written-by-rust";
const HOST: &str = "http://localhost:8080";
const ACCESS: &str = "access-written-by-lua";
const OTHER_ACCESS: &str = "access-written-by-rust";
const REFRESH: &str = "refresh-token";
const EXPIRES: u64 = 1_700_000_000_000;
const STALE: &str = "stale-access";
const FRESH: &str = "fresh-access";
const REFRESHED: &str = "refreshed-by-fn";
const ROTATED: &str = "rotated-access";
const REFRESH_ERR: &str = "boom";

#[derive(Clone, Copy)]
enum Shape {
    ApiKey,
    OAuth,
}

const SCRIPTS: &str = r#"
    local function token(tokens, err)
        return tokens and tokens.access or tostring(err)
    end
    local function body(res)
        local parts = {}
        while true do
            local chunk = res:read()
            if not chunk then break end
            table.insert(parts, chunk)
        end
        res:close()
        return table.concat(parts)
    end
    local scripts = {
        write_api_key = function(ctx)
            return tostring(ctx.credentials:write({ api_key = API_KEY, host = HOST }))
        end,
        write_oauth = function(ctx)
            return tostring(ctx.credentials:write({ access = ACCESS, refresh = REFRESH, expires = EXPIRES }))
        end,
        read = function(ctx)
            return maki.json.encode(ctx.credentials:read())
        end,
        delete = function(ctx)
            return tostring(ctx.credentials:delete()) .. "," .. tostring(ctx.credentials:delete())
        end,
        reread = function(ctx)
            body(assert(ctx.send({ url = SERVER })))
            return token(ctx.credentials:refresh(function(current)
                if current.access == FRESH then return nil end
                return { access = REFRESHED, refresh = REFRESH, expires = EXPIRES }
            end))
        end,
        share = function(ctx)
            local calls = 0
            local function refresh()
                return token(ctx.credentials:refresh(function()
                    calls = calls + 1
                    return { access = REFRESHED .. calls, refresh = REFRESH, expires = EXPIRES }
                end))
            end
            local results = maki.async.gather({ refresh, refresh })
            return calls .. "," .. results[1].value .. "," .. results[2].value
        end,
        rotate = function(ctx)
            return token(ctx.credentials:refresh(function()
                return maki.json.decode(body(assert(ctx.send({ url = SERVER }))))
            end))
        end,
        stored = function(ctx)
            return token(ctx.credentials:refresh(function() return nil end))
        end,
        fail = function(ctx)
            local calls = 0
            local function attempt()
                return token(ctx.credentials:refresh(function()
                    calls = calls + 1
                    return nil, REFRESH_ERR
                end))
            end
            return attempt() .. "," .. attempt() .. "," .. calls
        end,
    }
"#;

struct Fixture {
    _host: PluginHost,
    prepared: PreparedProvider,
    state: TempDir,
}

impl Fixture {
    fn new(server: &str) -> Self {
        let state = TempDir::new().unwrap();
        let host = PluginHost::with_session_provider_preparer(
            Arc::new(ToolRegistry::new()),
            Default::default(),
            true,
            Arc::new(InMemoryFs::new()),
            Some(state.path().to_path_buf()),
            None,
            HostUi::Frontend,
        )
        .unwrap();
        host.load_source(PLUGIN, &source(server)).unwrap();
        let providers = host.provider_registry().unwrap();
        let prepared =
            smol::block_on(bounded(providers.prepare(providers.model(SPEC).unwrap()))).unwrap();
        Self {
            _host: host,
            prepared,
            state,
        }
    }

    fn dir(&self) -> StateDir {
        StateDir::from_path(self.state.path().to_path_buf())
    }

    fn run(&self, script: &str) -> String {
        smol::block_on(bounded(run_script(&self.prepared, script)))
    }
}

fn source(server: &str) -> String {
    let constants = format!(
        r#"
        local API_KEY, HOST, ACCESS, REFRESH = "{API_KEY}", "{HOST}", "{ACCESS}", "{REFRESH}"
        local EXPIRES, FRESH, REFRESHED, REFRESH_ERR = {EXPIRES}, "{FRESH}", "{REFRESHED}", "{REFRESH_ERR}"
        local SERVER = "{server}"
        "#
    );
    format!(
        r#"
        {constants}
        {SCRIPTS}
        maki.api.register_provider({{
            id = "{PROVIDER}",
            display_name = "Fake",
            models = {{ {{ id = "fake-1", tier = "strong" }} }},
            prepare = function(ctx, model)
                return {{
                    stream = function(ctx, request, emit)
                        local text = scripts[request.system](ctx)
                        return {{
                            message = {{ role = "assistant", content = {{ {{ type = "text", text = text }} }} }},
                            stop_reason = "end_turn",
                        }}
                    end,
                }}
            end,
        }})
        "#
    )
}

async fn bounded<T>(fut: impl Future<Output = T>) -> T {
    future::or(fut, async {
        smol::Timer::after(BOUND).await;
        panic!("timed out after {BOUND:?}");
    })
    .await
}

async fn run_script(prepared: &PreparedProvider, script: &str) -> String {
    let (events, _events_rx) = flume::unbounded();
    let invocation = Invocation {
        conversation_id: "conv".into(),
        operation_id: "op".into(),
        attempt: 0,
        purpose: InvocationPurpose::Agent,
        agent_id: None,
    };
    let response = prepared
        .provider
        .stream_message(
            &prepared.model,
            &[],
            script,
            &json!([]),
            &events,
            RequestOptions::default(),
            None,
            &invocation,
        )
        .await
        .unwrap();
    match response.message.content.into_iter().next() {
        Some(ContentBlock::Text { text }) => text,
        other => panic!("expected text, got {other:?}"),
    }
}

/// A local HTTP server that reports each request and answers with the body
/// the test sends next.
struct Server {
    url: String,
    requests: flume::Receiver<()>,
    replies: flume::Sender<String>,
}

fn serve() -> Server {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}/", listener.local_addr().unwrap());
    let (request_tx, requests) = flume::unbounded();
    let (replies, reply_rx) = flume::unbounded::<String>();
    thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { return };
            let mut reader = BufReader::new(&stream);
            let mut line = String::new();
            while reader.read_line(&mut line).is_ok_and(|read| read > 0) && line != "\r\n" {
                line.clear();
            }
            if request_tx.send(()).is_err() {
                return;
            }
            let Ok(body) = reply_rx.recv_timeout(BOUND) else {
                return;
            };
            let _ = write!(
                stream,
                "HTTP/1.1 200 OK\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                body.len()
            );
        }
    });
    Server {
        url,
        requests,
        replies,
    }
}

fn tokens(access: &str) -> OAuthTokens {
    OAuthTokens {
        access: access.into(),
        refresh: REFRESH.into(),
        expires: EXPIRES,
        account_id: None,
    }
}

#[test_case(Shape::ApiKey ; "api_key")]
#[test_case(Shape::OAuth ; "oauth")]
fn credentials_use_existing_file_formats(shape: Shape) {
    let fixture = Fixture::new("");
    let dir = fixture.dir();
    match shape {
        Shape::ApiKey => {
            assert_eq!(fixture.run("write_api_key"), "true");
            let saved = load_provider_credentials(&dir, PROVIDER).unwrap();
            assert_eq!(saved.api_key, API_KEY);
            assert_eq!(saved.host.as_deref(), Some(HOST));
            let other = ProviderCredentials {
                api_key: OTHER_API_KEY.into(),
                host: None,
            };
            save_provider_credentials(&dir, PROVIDER, &other).unwrap();
            assert_eq!(
                serde_json::from_str::<serde_json::Value>(&fixture.run("read")).unwrap(),
                json!({ "api_key": OTHER_API_KEY })
            );
        }
        Shape::OAuth => {
            assert_eq!(fixture.run("write_oauth"), "true");
            let saved = load_tokens(&dir, PROVIDER).unwrap();
            assert_eq!(
                (saved.access.as_str(), saved.refresh.as_str(), saved.expires),
                (ACCESS, REFRESH, EXPIRES)
            );
            save_tokens(&dir, PROVIDER, &tokens(OTHER_ACCESS)).unwrap();
            assert_eq!(
                serde_json::from_str::<serde_json::Value>(&fixture.run("read")).unwrap(),
                json!({ "access": OTHER_ACCESS, "refresh": REFRESH, "expires": EXPIRES })
            );
        }
    }
    assert_eq!(fixture.run("delete"), "true,false");
    assert!(load_credentials(&dir, PROVIDER).is_none());
}

#[test]
fn refresh_skips_when_reread_under_lock_is_already_fresh() {
    let server = serve();
    let fixture = Fixture::new(&server.url);
    let dir = fixture.dir();
    save_tokens(&dir, PROVIDER, &tokens(STALE)).unwrap();
    let lock = lock_tokens(&dir, PROVIDER).unwrap();
    let result = thread::scope(|scope| {
        let refresh = scope.spawn(|| fixture.run("reread"));
        server.requests.recv_timeout(BOUND).unwrap();
        server.replies.send(String::new()).unwrap();
        save_tokens(&dir, PROVIDER, &tokens(FRESH)).unwrap();
        drop(lock);
        refresh.join().unwrap()
    });
    assert_eq!(result, FRESH);
    assert_eq!(load_tokens(&dir, PROVIDER).unwrap().access, FRESH);
}

#[test]
fn concurrent_refreshes_share_one_call() {
    let fixture = Fixture::new("");
    let expected = format!("1,{REFRESHED}1,{REFRESHED}1");
    assert_eq!(fixture.run("share"), expected);
    assert_eq!(
        load_tokens(&fixture.dir(), PROVIDER).unwrap().access,
        format!("{REFRESHED}1")
    );
}

#[test]
fn cancelled_caller_still_writes_refreshed_token() {
    let server = serve();
    let fixture = Fixture::new(&server.url);
    let rotate = async {
        run_script(&fixture.prepared, "rotate").await;
        panic!("rotate finished before the server answered");
    };
    smol::block_on(bounded(future::or(rotate, async {
        server.requests.recv_async().await.unwrap();
    })));
    let rotated = json!({ "access": ROTATED, "refresh": REFRESH, "expires": EXPIRES });
    server.replies.send(rotated.to_string()).unwrap();

    assert_eq!(fixture.run("stored"), ROTATED);
    assert_eq!(
        load_tokens(&fixture.dir(), PROVIDER).unwrap().access,
        ROTATED
    );
}

#[test]
fn refresh_error_is_returned_and_flight_cleared() {
    let fixture = Fixture::new("");
    assert_eq!(
        fixture.run("fail"),
        format!("{REFRESH_ERR},{REFRESH_ERR},2")
    );
}
