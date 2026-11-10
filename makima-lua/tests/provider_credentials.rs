use std::future::Future;
use std::io::{BufRead, BufReader, Write};
use std::net::TcpListener;
use std::path::Path;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use futures_lite::future;
use makima_agent::tools::ToolRegistry;
use makima_lua::PluginHost;
use makima_lua::test_support::InMemoryFs;
use makima_lua::ui_sink::HostUi;
use makima_providers::{
    ContentBlock, Invocation, InvocationPurpose, PreparedProvider, RequestDelivery, RequestOptions,
};
use makima_storage::StateDir;
use makima_storage::auth::{load_credentials, lock_tokens, save_credentials};
use serde_json::{Value, json};
use tempfile::TempDir;
use test_case::test_case;

const PROVIDER: &str = "fake";
const PLUGIN: &str = "fake_plugin";
const NAMESPACE: &str = "fountain";
const STORE_KEY: &str = "account";
const NAMESPACED_STORE: &str = "plugins/fake_plugin/fountain/account";
const INIT_ROOT: &str = "plugins/makima_init.global";
const BAD_NAME_ERR: &str =
    "must be non-empty ASCII letters, digits, '-', '_' or '.', and not start with '.'";
const BAD_ARGS_ERR: &str = "takes (ctx, spec) or (spec)";
const NOT_A_CTX: &str = "userdata";
const NET_TIMEOUT_ENV: &str = "MAKIMA_NET_TIMEOUT_SECS";
const NET_TIMEOUT_SECS: &str = "1";
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
const REFRESH_STATUS: u16 = 401;
const RETRY_AFTER_SECS: u32 = 7;

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
            return makima.json.encode(ctx.credentials:read())
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
            local results = makima.async.gather({ refresh, refresh })
            return calls .. "," .. results[1].value .. "," .. results[2].value
        end,
        rotate = function(ctx)
            return token(ctx.credentials:refresh(function()
                return makima.json.decode(body(assert(ctx.send({ url = SERVER }))))
            end))
        end,
        stored = function(ctx)
            return token(ctx.credentials:refresh(function() return nil end))
        end,
        empty = function(ctx)
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
        locked = function(ctx)
            body(assert(ctx.send({ url = SERVER })))
            return ctx.credentials:lock(function()
                return ctx.credentials:read().access
            end)
        end,
        plugin_store = function(ctx)
            local store = makima.credentials.open(ctx, STORE_KEY)
            store:write({ api_key = API_KEY })
            return store:read().api_key
        end,
        not_a_ctx = function()
            local _, err = pcall(makima.credentials.open, {}, STORE_KEY)
            return tostring(err)
        end,
        assign = function(ctx)
            return tostring(pcall(function() ctx.send = nil end))
        end,
        id = function(ctx)
            return ctx.id
        end,
        fail_table = function(ctx)
            local _, err = ctx.credentials:refresh(function()
                return nil, { status = REFRESH_STATUS, message = REFRESH_ERR, retry_after = RETRY_AFTER }
            end)
            return table.concat({ err.status, err.message, err.retry_after }, ",")
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
        let host = new_host(state.path());
        host.load_source(PLUGIN, &source(server)).unwrap();
        let providers = host
            .provider_registry(makima_providers::Timeouts::default())
            .unwrap();
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

fn new_host(state: &Path) -> PluginHost {
    PluginHost::with_session_provider_preparer(
        Arc::new(ToolRegistry::new()),
        Default::default(),
        true,
        Arc::new(InMemoryFs::new()),
        Some(state.to_path_buf()),
        None,
        HostUi::Frontend,
    )
    .unwrap()
}

/// Loads {source} as plugin {PLUGIN} into a fresh host on its own thread,
/// so a load that never finishes fails the test instead of hanging it.
fn load_bounded(state: &Path, source: String) -> Result<(), String> {
    let (done_tx, done) = flume::bounded(1);
    let state = state.to_path_buf();
    thread::spawn(move || {
        let host = new_host(&state);
        let _ = done_tx.send(host.load_source(PLUGIN, &source).map_err(|e| e.to_string()));
    });
    done.recv_timeout(BOUND).expect("plugin load timed out")
}

fn constants(server: &str) -> String {
    format!(
        r#"
        local root = ...
        local API_KEY, HOST, ACCESS, REFRESH = "{API_KEY}", "{HOST}", "{ACCESS}", "{REFRESH}"
        local EXPIRES, FRESH, REFRESHED, REFRESH_ERR = {EXPIRES}, "{FRESH}", "{REFRESHED}", "{REFRESH_ERR}"
        local REFRESH_STATUS, RETRY_AFTER = {REFRESH_STATUS}, {RETRY_AFTER_SECS}
        local STALE, ROTATED, STORE_KEY = "{STALE}", "{ROTATED}", "{STORE_KEY}"
        local SERVER = "{server}"
        "#
    )
}

fn source(server: &str) -> String {
    format!(
        r#"
        {constants}
        {SCRIPTS}
        makima.api.register_provider(root:with_name("{NAMESPACE}"), {{
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
        "#,
        constants = constants(server),
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
        previous: None,
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
            RequestDelivery::untracked(),
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

fn tokens(access: &str) -> Value {
    json!({ "access": access, "refresh": REFRESH, "expires": EXPIRES })
}

fn stored_access(dir: &StateDir) -> Value {
    load_credentials(dir, PROVIDER).unwrap()["access"].clone()
}

#[test_case(Shape::ApiKey ; "api_key")]
#[test_case(Shape::OAuth ; "oauth")]
fn credentials_use_existing_file_formats(shape: Shape) {
    let fixture = Fixture::new("");
    let dir = fixture.dir();
    match shape {
        Shape::ApiKey => {
            assert_eq!(fixture.run("write_api_key"), "true");
            assert_eq!(
                load_credentials(&dir, PROVIDER),
                Some(json!({ "api_key": API_KEY, "host": HOST }))
            );
            save_credentials(&dir, PROVIDER, &json!({ "api_key": OTHER_API_KEY })).unwrap();
            assert_eq!(
                serde_json::from_str::<Value>(&fixture.run("read")).unwrap(),
                json!({ "api_key": OTHER_API_KEY })
            );
        }
        Shape::OAuth => {
            assert_eq!(fixture.run("write_oauth"), "true");
            assert_eq!(load_credentials(&dir, PROVIDER), Some(tokens(ACCESS)));
            save_credentials(&dir, PROVIDER, &tokens(OTHER_ACCESS)).unwrap();
            assert_eq!(
                serde_json::from_str::<Value>(&fixture.run("read")).unwrap(),
                tokens(OTHER_ACCESS)
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
    save_credentials(&dir, PROVIDER, &tokens(STALE)).unwrap();
    let lock = lock_tokens(&dir, PROVIDER).unwrap();
    let result = thread::scope(|scope| {
        let refresh = scope.spawn(|| fixture.run("reread"));
        server.requests.recv_timeout(BOUND).unwrap();
        server.replies.send(String::new()).unwrap();
        save_credentials(&dir, PROVIDER, &tokens(FRESH)).unwrap();
        drop(lock);
        refresh.join().unwrap()
    });
    assert_eq!(result, FRESH);
    assert_eq!(stored_access(&dir), FRESH);
}

#[test]
fn lock_runs_its_function_after_another_holder_releases() {
    let server = serve();
    let fixture = Fixture::new(&server.url);
    let dir = fixture.dir();
    save_credentials(&dir, PROVIDER, &tokens(STALE)).unwrap();
    let lock = lock_tokens(&dir, PROVIDER).unwrap();
    let result = thread::scope(|scope| {
        let locked = scope.spawn(|| fixture.run("locked"));
        server.requests.recv_timeout(BOUND).unwrap();
        server.replies.send(String::new()).unwrap();
        save_credentials(&dir, PROVIDER, &tokens(FRESH)).unwrap();
        drop(lock);
        locked.join().unwrap()
    });
    assert_eq!(result, FRESH);
}

#[test]
fn concurrent_refreshes_share_one_call() {
    let fixture = Fixture::new("");
    let expected = format!("1,{REFRESHED}1,{REFRESHED}1");
    assert_eq!(fixture.run("share"), expected);
    assert_eq!(stored_access(&fixture.dir()), format!("{REFRESHED}1"));
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
    assert_eq!(stored_access(&fixture.dir()), ROTATED);
}

#[test]
fn nil_refresh_without_stored_credentials_returns_an_error() {
    let fixture = Fixture::new("");
    assert_eq!(
        fixture.run("empty"),
        "refresh returned nil with no stored credentials"
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

#[test]
fn refresh_error_table_reaches_the_caller_intact() {
    let fixture = Fixture::new("");
    assert_eq!(
        fixture.run("fail_table"),
        format!("{REFRESH_STATUS},{REFRESH_ERR},{RETRY_AFTER_SECS}")
    );
}

#[test]
fn provider_ctx_opens_the_plugin_stores_of_the_ctx_it_was_registered_under() {
    let fixture = Fixture::new("");
    assert_eq!(fixture.run("plugin_store"), API_KEY);
    assert_eq!(
        load_credentials(&fixture.dir(), NAMESPACED_STORE),
        Some(json!({ "api_key": API_KEY }))
    );
    assert!(load_credentials(&fixture.dir(), PROVIDER).is_none());
}

#[test]
fn provider_ctx_names_its_provider_and_cannot_be_assigned() {
    let fixture = Fixture::new("");
    assert_eq!(fixture.run("id"), PROVIDER);
    assert_eq!(fixture.run("assign"), "false");
}

#[test]
fn credentials_open_refuses_a_value_that_is_not_a_ctx() {
    let fixture = Fixture::new("");
    assert!(fixture.run("not_a_ctx").contains(NOT_A_CTX));
}

#[test]
fn named_children_of_the_root_ctx_open_separate_stores() {
    let state = TempDir::new().unwrap();
    let source = format!(
        r#"
        local root = ...
        local child = root:with_name("{NAMESPACE}")
        assert(makima.credentials.open(root, "{STORE_KEY}"):write({{ at = "root" }}))
        assert(makima.credentials.open(child, "{STORE_KEY}"):write({{ at = "child" }}))
        assert(makima.credentials.open(child:with_name("deeper"), "{STORE_KEY}"):write({{ at = "deeper" }}))
        "#
    );

    load_bounded(state.path(), source).unwrap();

    let dir = StateDir::from_path(state.path().to_path_buf());
    let at = |name: &str| load_credentials(&dir, name).unwrap()["at"].clone();
    assert_eq!(at(&format!("plugins/{PLUGIN}/{STORE_KEY}")), "root");
    assert_eq!(at(NAMESPACED_STORE), "child");
    assert_eq!(
        at(&format!("plugins/{PLUGIN}/{NAMESPACE}/deeper/{STORE_KEY}")),
        "deeper"
    );
}

#[test]
fn child_ctx_cannot_read_a_parent_only_store() {
    let state = TempDir::new().unwrap();
    let source = format!(
        r#"
        local root = ...
        assert(makima.credentials.open(root, "{STORE_KEY}"):write({{ at = "root" }}))
        local seen = makima.credentials.open(root:with_name("{NAMESPACE}"), "{STORE_KEY}"):read()
        assert(seen == nil, "a child ctx read its parent's store")
        "#
    );

    load_bounded(state.path(), source).unwrap();
}

#[test]
fn send_and_store_refresh_work_during_load() {
    let server = serve();
    let rotated = json!({ "access": ROTATED, "refresh": REFRESH, "expires": EXPIRES });
    server.replies.send(rotated.to_string()).unwrap();
    let state = TempDir::new().unwrap();
    let source = format!(
        r#"
        {constants}
        local ctx = root:with_name("{NAMESPACE}")
        local account = makima.credentials.open(ctx, STORE_KEY)
        assert(account:write({{ access = STALE, refresh = REFRESH, expires = EXPIRES }}))
        local refreshed, err = account:refresh(function(current)
            assert(current.access == STALE)
            local response = assert(ctx.send({{ url = SERVER }}))
            local parts = {{}}
            while true do
                local chunk = response:read()
                if not chunk then break end
                table.insert(parts, chunk)
            end
            response:close()
            return makima.json.decode(table.concat(parts))
        end)
        assert(refreshed and refreshed.access == ROTATED, tostring(err))
        "#,
        constants = constants(&server.url),
    );

    load_bounded(state.path(), source).unwrap();

    let dir = StateDir::from_path(state.path().to_path_buf());
    assert_eq!(
        load_credentials(&dir, NAMESPACED_STORE).unwrap()["access"],
        ROTATED
    );
}

#[test]
fn concurrent_refreshes_during_load_share_one_call() {
    let state = TempDir::new().unwrap();
    let source = format!(
        r#"
        {constants}
        local store = makima.credentials.open(root, STORE_KEY)
        local calls = 0
        local function refresh()
            local tokens = store:refresh(function()
                calls = calls + 1
                return {{ access = REFRESHED .. calls, refresh = REFRESH, expires = EXPIRES }}
            end)
            return tokens.access
        end
        local results = makima.async.gather({{ refresh, refresh }})
        local seen = calls .. "," .. results[1].value .. "," .. results[2].value
        assert(seen == "1," .. REFRESHED .. "1," .. REFRESHED .. "1", seen)
        "#,
        constants = constants(""),
    );

    load_bounded(state.path(), source).unwrap();

    let dir = StateDir::from_path(state.path().to_path_buf());
    assert_eq!(
        load_credentials(&dir, &format!("plugins/{PLUGIN}/{STORE_KEY}")).unwrap()["access"],
        format!("{REFRESHED}1")
    );
}

#[test]
fn plugin_ctx_send_gives_up_after_the_net_timeout() {
    // SAFETY: nextest runs each test in its own process, and the variable is
    // set before the host starts.
    unsafe { std::env::set_var(NET_TIMEOUT_ENV, NET_TIMEOUT_SECS) };
    let silent = TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}/", silent.local_addr().unwrap());
    let state = TempDir::new().unwrap();
    let source = format!(
        r#"
        local root = ...
        local response, err = root.send({{ url = "{url}" }})
        assert(response == nil and err ~= nil, "send to a silent server returned a response")
        "#
    );

    load_bounded(state.path(), source).unwrap();
}

#[test_case(r#"root:with_name("")"#, BAD_NAME_ERR ; "empty_name")]
#[test_case(r#"root:with_name(".hidden")"#, BAD_NAME_ERR ; "dot_name")]
#[test_case(r#"root:with_name("a/b")"#, BAD_NAME_ERR ; "slash_name")]
#[test_case(r#"makima.credentials.open(root, "../up")"#, BAD_NAME_ERR ; "parent_key")]
#[test_case(r#"makima.api.register_provider(root)"#, BAD_ARGS_ERR ; "ctx_without_spec")]
fn invalid_ctx_use_fails_the_load(call: &str, expected: &str) {
    let state = TempDir::new().unwrap();

    let error = load_bounded(state.path(), format!("local root = ...\n{call}")).unwrap_err();

    assert!(error.contains(expected), "{error}");
}

#[test]
fn global_and_project_init_share_one_root() {
    let state = TempDir::new().unwrap();
    let host = new_host(state.path());
    for (name, init) in [
        ("global", "global/init.lua"),
        ("project", "project/init.lua"),
    ] {
        let source = format!(
            r#"assert(makima.credentials.open((...), "{name}"):write({{ from = "{init}" }}))"#
        );
        host.send_run_init_lua(source, init.to_owned(), None)
            .unwrap();
    }

    let dir = StateDir::from_path(state.path().to_path_buf());
    for (name, init) in [
        ("global", "global/init.lua"),
        ("project", "project/init.lua"),
    ] {
        let stored = load_credentials(&dir, &format!("{INIT_ROOT}/{name}")).unwrap();
        assert_eq!(stored["from"], init);
    }
}
