use std::future::Future;
use std::io::Write;
use std::net::TcpListener;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use flume::RecvTimeoutError;
use futures_lite::future;
use maki_agent::tools::ToolRegistry;
use maki_config::providers::{ProviderDef, ProvidersConfig};
use maki_lua::PluginHost;
use maki_providers::{
    AgentError, ContentBlock, ConversationEnd, ConversationLink, ConversationRelation, Invocation,
    InvocationPurpose, Message, PreparedProvider, ProviderEvent, ProviderRegistry, RequestDelivery,
    RequestDeliveryTracker, RequestOptions, RetryKind, Role, StopReason, Timeouts, UsageLimit,
    UsageWindow,
};
use serde_json::json;
use test_case::test_case;

const PROVIDER: &str = "fake";
const PLUGIN: &str = "fake_plugin";
const OTHER_PLUGIN: &str = "other_plugin";
const MODEL: &str = "fake-1";
const SPEC: &str = "fake/fake-1";
const DISPLAY_NAME: &str = "Fake Provider";
const CONTEXT_WINDOW: u32 = 4242;
const BOUND: Duration = Duration::from_secs(10);
const CONVERSATION: &str = "conv-1";
const OPERATION: &str = "op-1";
const AGENT: &str = "agent-1";
const PREVIOUS: &str = "conv-0";
const SHARED: usize = 12;
const ATTEMPT: u32 = 2;
const USER_TEXT: &str = "hello";
const SYSTEM: &str = "be brief";
const DELTA: &str = "hi";
const API_MESSAGE: &str = "slow down";
const API_STATUS: u16 = 429;
const RETRY_AFTER_SECS: f64 = 1.5;
const TRANSPORT_MESSAGE: &str = "connection reset";
const LOOPBACK: &str = "127.0.0.1:0";
const SHORT_IDLE_SECS: f64 = 0.2;
const IDLE_TIMEOUT: &str = "idle timeout";
/// Headers and one body byte, so `send` returns and the next read stalls.
const STALLED_HEAD: &[u8] = b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nx";
const RAISED_MESSAGE: &str = "boom";
const CONFIG_MESSAGE: &str = "AWS_REGION must be set";
const INVALID_TYPE_ERR: &str = "invalid type";
const DUPLICATE_ERR: &str =
    r#"provider "fake" from plugin "other_plugin" is already registered by plugin "fake_plugin""#;
const UNKNOWN_FIELD_ERR: &str = "unknown field `colour`";
const PREPARE_REQUIRED_ERR: &str = "prepare is required";
const EXPECTED_FUNCTION_ERR: &str = "expected a function";
const BAD_ID_ERR: &str = "must be non-empty ASCII letters, digits";
const LOGIN_REQUIRED_ERR: &str = "login is required";
const UNKNOWN_METHOD_ERR: &str = "names unknown auth method";
const BAD_ALIAS_ERR: &str = "must be a valid provider id";
const DUPLICATE_METHOD_ERR: &str = "is listed twice";
const TOP_LEVEL_ERR: &str = "may only be called at the top level during plugin load";
const CALL_LOG: &str =
    "c-cancelled:cancelled,c-closed:closed,c-compacted:compacted,c-rewound:rewound,rotate";
const KEY_COUNT: usize = 3;
const CONFIGURED_URL: &str = "http://configured.invalid";
const FIVE_HOURS: u64 = 5;
const USED_PERCENT: u32 = 70;
const MARKER_ERR: &str = r#"request_delivery_marker must be "auto" or "manual""#;
const EMIT_THEN_FAIL: &str = r#"emit({ type = "text_delta", text = "hi" })
            return nil, "boom""#;
const MARK_TWICE: &str = r#"local first = ctx.mark_request_delivery()
            local second = ctx.mark_request_delivery()
            return { message = { role = "assistant",
                content = { { type = "text", text = tostring(first) .. tostring(second) } } } }"#;
const KEEP_CTX_THEN_FAIL: &str = r#"if kept_ctx then
                return { message = { role = "assistant",
                    content = { { type = "text", text = tostring(kept_ctx.mark_request_delivery()) } } } }
            end
            kept_ctx = ctx
            return nil, "boom""#;
const MARKED_TWICE: &str = "truetrue";
const RETIRED_MARK: &str = "false";
const CONFIG_SOURCE: &str = r#"
    maki.api.register_provider({
        id = "fake",
        display_name = "Fake Provider",
        discover = function(ctx, config)
            return { { id = config.base_url } }
        end,
        prepare = function(ctx, model, config)
            return { stream = function() end }, { id = model.id, context_window = #config.base_url }
        end,
    })
"#;

const REFRESH_SOURCE: &str = r#"
    maki.api.register_provider({
        id = "fake",
        display_name = "Fake Provider",
        discover = function(ctx, config, refresh)
            return { { id = tostring(refresh) } }
        end,
        prepare = function()
            return { stream = function() end }
        end,
    })
"#;

fn host() -> PluginHost {
    PluginHost::new(Arc::new(ToolRegistry::new())).unwrap()
}

fn spec_source(id: &str, extra: &str, instance: &str) -> String {
    format!(
        r#"
        maki.api.register_provider({{
            id = "{id}",
            display_name = "{DISPLAY_NAME}",
            models = {{ {{ id = "{MODEL}", context_window = {CONTEXT_WINDOW}, tier = "strong" }} }},
            {extra}
            prepare = function(ctx, model)
                {instance}
            end,
        }})
        "#
    )
}

const ECHO_INSTANCE: &str = r#"
    return {
        stream = function(ctx, request, emit)
            emit({ type = "text_delta", text = "hi" })
            emit({ type = "thinking_block_end" })
            local inv = request.invocation
            local text = table.concat({
                ctx.id, request.model.id, tostring(request.model.context_window),
                request.system, request.messages[1].content[1].text,
                inv.conversation_id, inv.operation_id, tostring(inv.attempt),
                inv.purpose, inv.agent_id, inv.previous.id, inv.previous.relation,
                tostring(inv.previous.shared), tostring(request.fast),
                tostring(request.session_id), tostring(type(ctx.send)),
            }, "|")
            return {
                message = { role = "assistant", content = { { type = "text", text = text } } },
                usage = { input_tokens = 7, output_tokens = 3 },
                stop_reason = "end_turn",
            }
        end,
    }
"#;

async fn bounded<T>(fut: impl Future<Output = T>) -> T {
    future::or(fut, async {
        smol::Timer::after(BOUND).await;
        panic!("timed out after {BOUND:?}");
    })
    .await
}

fn registry(host: &PluginHost) -> Arc<ProviderRegistry> {
    host.provider_registry(Timeouts::default()).unwrap()
}

fn prepare_fake(instance: &str) -> (PluginHost, PreparedProvider) {
    prepare_fake_with(instance, Timeouts::default())
}

fn prepare_fake_with(instance: &str, timeouts: Timeouts) -> (PluginHost, PreparedProvider) {
    let host = host();
    host.load_source(PLUGIN, &spec_source(PROVIDER, "", instance))
        .unwrap();
    let providers = host.provider_registry(timeouts).unwrap();
    let model = providers.model(SPEC).unwrap();
    let prepared = smol::block_on(bounded(providers.prepare(model))).unwrap();
    (host, prepared)
}

fn invocation() -> Invocation {
    Invocation {
        conversation_id: CONVERSATION.into(),
        operation_id: OPERATION.into(),
        attempt: ATTEMPT,
        purpose: InvocationPurpose::Agent,
        agent_id: Some(AGENT.into()),
        previous: Some(ConversationLink {
            id: PREVIOUS.into(),
            relation: ConversationRelation::Continues,
            shared: SHARED,
        }),
    }
}

fn user_message() -> Message {
    Message {
        role: Role::User,
        content: vec![ContentBlock::Text {
            text: USER_TEXT.into(),
        }],
        ..Message::default()
    }
}

fn stream(
    prepared: &PreparedProvider,
    events: &flume::Sender<ProviderEvent>,
) -> Result<maki_providers::StreamResponse, AgentError> {
    stream_tracked(prepared, events, RequestDelivery::untracked())
}

fn stream_tracked(
    prepared: &PreparedProvider,
    events: &flume::Sender<ProviderEvent>,
    delivery: RequestDelivery,
) -> Result<maki_providers::StreamResponse, AgentError> {
    smol::block_on(bounded(prepared.provider.stream_message(
        &prepared.model,
        &[user_message()],
        SYSTEM,
        &json!([]),
        events,
        RequestOptions::default(),
        None,
        &invocation(),
        delivery,
    )))
}

/// Streams once under a fresh tracker; returns the result and whether the
/// request was delivered.
fn stream_delivery(
    prepared: &PreparedProvider,
) -> (Result<maki_providers::StreamResponse, AgentError>, bool) {
    let tracker = RequestDeliveryTracker::new();
    let (events, _rx) = flume::unbounded();
    let result = stream_tracked(prepared, &events, tracker.attempt(ATTEMPT));
    (result, tracker.is_delivered())
}

fn marker_instance(marker: &str, body: &str) -> String {
    format!(
        r#"return {{ {marker} stream = function(ctx, request, emit)
            {body}
        end }}"#
    )
}

fn text(message: &Message) -> &str {
    match &message.content[0] {
        ContentBlock::Text { text } => text,
        other => panic!("expected text, got {other:?}"),
    }
}

#[test_case("" , true ; "absent_marks_on_first_event")]
#[test_case(r#"request_delivery_marker = "auto","# , true ; "auto_marks_on_first_event")]
#[test_case(r#"request_delivery_marker = "manual","# , false ; "manual_ignores_events")]
fn delivery_marker_decides_whether_an_event_delivers(marker: &str, delivered: bool) {
    let (_host, prepared) = prepare_fake(&marker_instance(marker, EMIT_THEN_FAIL));
    let (result, was_delivered) = stream_delivery(&prepared);
    result.unwrap_err();
    assert_eq!(was_delivered, delivered);
}

#[test]
fn unknown_delivery_marker_fails_preparation() {
    let host = host();
    let instance = marker_instance(r#"request_delivery_marker = "eager","#, EMIT_THEN_FAIL);
    host.load_source(PLUGIN, &spec_source(PROVIDER, "", &instance))
        .unwrap();
    let providers = registry(&host);
    let model = providers.model(SPEC).unwrap();
    let error = smol::block_on(bounded(providers.prepare(model)))
        .err()
        .unwrap();
    assert!(
        matches!(&error, AgentError::Config { message } if message.contains(MARKER_ERR)),
        "{error}"
    );
}

#[test_case("" ; "auto")]
#[test_case(r#"request_delivery_marker = "manual","# ; "manual")]
fn explicit_mark_delivers_and_repeats_as_a_no_op(marker: &str) {
    let (_host, prepared) = prepare_fake(&marker_instance(marker, MARK_TWICE));
    let (result, delivered) = stream_delivery(&prepared);
    assert_eq!(text(&result.unwrap().message), MARKED_TWICE);
    assert!(delivered);
}

#[test]
fn retained_ctx_cannot_mark_after_its_call_ended() {
    let (_host, prepared) = prepare_fake(&marker_instance(
        r#"request_delivery_marker = "manual","#,
        KEEP_CTX_THEN_FAIL,
    ));
    let first = RequestDeliveryTracker::new();
    let (events, _rx) = flume::unbounded();
    stream_tracked(&prepared, &events, first.attempt(ATTEMPT)).unwrap_err();
    let (result, _) = stream_delivery(&prepared);
    assert_eq!(text(&result.unwrap().message), RETIRED_MARK);
    assert!(!first.is_delivered());
}

#[test_case(r#"colour = "red","# , UNKNOWN_FIELD_ERR ; "unknown field")]
#[test_case(r#"discover = 42,"# , EXPECTED_FUNCTION_ERR ; "callback of wrong type")]
#[test_case(r#"auth = { { id = "k", label = "K" } },"# , LOGIN_REQUIRED_ERR ; "login missing")]
#[test_case(r#"auth = { { id = "k", label = "K", login = print }, { id = "k", label = "K", login = print } },"# , DUPLICATE_METHOD_ERR ; "duplicate method")]
#[test_case(r#"auth = { { id = "k", label = "K", login = print } }, aliases = { { id = "a", label = "A", method = "x" } },"# , UNKNOWN_METHOD_ERR ; "alias naming unknown method")]
#[test_case(r#"auth = { { id = "k", label = "K", login = print } }, aliases = { { id = "a/b", label = "A", method = "k" } },"# , BAD_ALIAS_ERR ; "alias with bad id")]
#[test_case(r#"auth = { { id = "k", label = "K", login = print } }, aliases = { { id = "fake", label = "Fake", method = "k" } },"# , BAD_ALIAS_ERR ; "alias equal to provider id")]
#[test_case(r#"docs = { env = "KEY" },"# , INVALID_TYPE_ERR ; "docs env not a list")]
#[test_case(r#"auth_status = 1,"# , EXPECTED_FUNCTION_ERR ; "auth_status of wrong type")]
fn invalid_spec_fields_are_rejected(extra: &str, expected: &str) {
    let error = host()
        .load_source(PLUGIN, &spec_source(PROVIDER, extra, "return {}"))
        .unwrap_err();
    assert!(error.to_string().contains(expected), "{error}");
}

#[test_case(r#"maki.api.register_provider({ id = "x", display_name = "X" })"#, PREPARE_REQUIRED_ERR ; "missing prepare")]
#[test_case(r#"maki.api.register_provider({ id = 1, display_name = "X", prepare = print })"#, INVALID_TYPE_ERR ; "id of wrong type")]
#[test_case(r#"maki.api.register_provider({ id = "x/y", display_name = "X", prepare = print })"#, BAD_ID_ERR ; "id with slash")]
#[test_case(r#"maki.api.register_provider({ id = "..", display_name = "X", prepare = print })"#, BAD_ID_ERR ; "id with leading dot")]
#[test_case(r#"maki.async.run(function() maki.api.register_provider({ id = "x", display_name = "X", prepare = print }) end)"#, TOP_LEVEL_ERR ; "outside plugin load")]
fn invalid_registrations_are_rejected(source: &str, expected: &str) {
    let host = host();
    let result = host.load_source(PLUGIN, source);
    host.drain_tasks_for_test();
    if expected == TOP_LEVEL_ERR {
        result.unwrap();
        assert!(registry(&host).display_name("x").is_none());
        return;
    }
    let error = result.unwrap_err();
    assert!(error.to_string().contains(expected), "{error}");
}

#[test]
fn unknown_model_descriptor_fields_are_ignored() {
    let host = host();
    host.load_source(
        PLUGIN,
        r#"maki.api.register_provider({
            id = "x", display_name = "X",
            models = { { id = "m", future_capability = true } },
            prepare = function() return { stream = function() end } end,
        })"#,
    )
    .unwrap();
    assert!(registry(&host).model("x/m").is_ok());
}

#[test]
fn duplicate_id_names_both_plugins_and_unload_frees_it() {
    let host = host();
    let source = spec_source(PROVIDER, "", ECHO_INSTANCE);
    host.load_source(PLUGIN, &source).unwrap();
    host.load_source(PLUGIN, &source).unwrap();

    let error = host.load_source(OTHER_PLUGIN, &source).unwrap_err();
    assert!(error.to_string().contains(DUPLICATE_ERR), "{error}");

    host.unload(PLUGIN).unwrap();
    assert!(registry(&host).display_name(PROVIDER).is_none());
    host.load_source(OTHER_PLUGIN, &source).unwrap();
    assert_eq!(
        registry(&host).display_name(PROVIDER).as_deref(),
        Some(DISPLAY_NAME)
    );
}

#[test]
fn duplicate_id_in_one_plugin_names_the_plugin_once() {
    let host = host();
    let source = spec_source(PROVIDER, "", ECHO_INSTANCE).repeat(2);
    let error = host.load_source(PLUGIN, &source).unwrap_err().to_string();
    assert!(
        error.contains("already registered by plugin \"fake_plugin\""),
        "{error}"
    );
    assert!(!error.contains("from plugin"), "{error}");
}

#[test]
fn aliases_cannot_collide_globally() {
    let host = host();
    let first = r#"maki.api.register_provider({
        id = "first", display_name = "First",
        auth = { { id = "key", label = "Key", login = print } },
        aliases = { { id = "shared", label = "Shared", method = "key" } },
        prepare = function() return {} end,
    })"#;
    host.load_source(PLUGIN, first).unwrap();
    let error = host
        .load_source(OTHER_PLUGIN, &spec_source("shared", "", ECHO_INSTANCE))
        .unwrap_err();
    assert!(error.to_string().contains("already registered"), "{error}");
}

#[test]
fn failed_load_registers_nothing() {
    let host = host();
    let source = format!(
        "{}\nerror('{RAISED_MESSAGE}')",
        spec_source(PROVIDER, "", ECHO_INSTANCE)
    );
    host.load_source(PLUGIN, &source).unwrap_err();
    assert!(registry(&host).display_name(PROVIDER).is_none());
}

#[test]
fn registry_resolves_plugin_models() {
    let host = host();
    host.load_source(PLUGIN, &spec_source(PROVIDER, "", ECHO_INSTANCE))
        .unwrap();
    let providers = registry(&host);
    let model = providers.model(SPEC).unwrap();
    assert_eq!(model.context_window, CONTEXT_WINDOW);
    assert_eq!(
        providers.display_name(PROVIDER).as_deref(),
        Some(DISPLAY_NAME)
    );
    assert_eq!(
        providers.spec_for_tier(PROVIDER, maki_providers::ModelTier::Strong),
        Some(SPEC.to_string())
    );
}

#[test]
fn stream_delivers_events_response_and_request_fields() {
    let (_host, prepared) = prepare_fake(ECHO_INSTANCE);
    let (events_tx, events_rx) = flume::unbounded();
    let response = stream(&prepared, &events_tx).unwrap();
    drop(events_tx);

    let events: Vec<ProviderEvent> = events_rx.iter().collect();
    assert!(matches!(&events[..], [
        ProviderEvent::TextDelta { text },
        ProviderEvent::ThinkingBlockEnd,
    ] if text == DELTA));
    let expected = [
        PROVIDER,
        MODEL,
        &CONTEXT_WINDOW.to_string(),
        SYSTEM,
        USER_TEXT,
        CONVERSATION,
        OPERATION,
        &ATTEMPT.to_string(),
        "agent",
        AGENT,
        PREVIOUS,
        "continues",
        &SHARED.to_string(),
        "false",
        "nil",
        "function",
    ]
    .join("|");
    assert_eq!(text(&response.message), expected);
    assert_eq!(response.usage.input, 7);
    assert_eq!(response.usage.output, 3);
    assert_eq!(response.stop_reason, Some(StopReason::EndTurn));
}

#[test]
fn prepare_may_adjust_the_model_but_not_its_id() {
    let adjusted = format!(
        r#"return {{ stream = function() end }}, {{ id = "{MODEL}", context_window = 99 }}"#
    );
    let (_host, prepared) = prepare_fake(&adjusted);
    assert_eq!(prepared.model.context_window, 99);

    let host = host();
    host.load_source(
        PLUGIN,
        &spec_source(
            PROVIDER,
            "",
            r#"return { stream = function() end }, { id = "other" }"#,
        ),
    )
    .unwrap();
    let providers = registry(&host);
    let model = providers.model(SPEC).unwrap();
    let error = smol::block_on(bounded(providers.prepare(model)))
        .err()
        .unwrap();
    assert!(matches!(error, AgentError::Config { .. }), "{error}");
}

#[test_case(
    "return nil, { status = 70000, message = 'bad status' }",
    |e| matches!(e, AgentError::Config { message } if message.contains("0 through 65535"))
    ; "out of range status is rejected")]
#[test_case(
    &format!("return nil, {{ status = {API_STATUS}, message = '{API_MESSAGE}', retry_after = {RETRY_AFTER_SECS} }}"),
    |e| matches!(e, AgentError::Api { status: API_STATUS, message, retry_after: Some(after), overflow: false, retry: None }
        if message == API_MESSAGE && *after == Duration::from_secs_f64(RETRY_AFTER_SECS))
    ; "error table with status is an api error")]
#[test_case(
    &format!("return nil, {{ status = {API_STATUS}, message = '{API_MESSAGE}', overflow = true, retry = 'transient' }}"),
    |e| e.is_context_overflow() && e.retry_kind().is_none()
    ; "error table marked overflow is a context overflow")]
#[test_case(
    &format!("return nil, {{ message = '{TRANSPORT_MESSAGE}' }}"),
    |e| matches!(e, AgentError::Transport { message, retry: None } if message == TRANSPORT_MESSAGE)
    ; "error table without status or class is not retried")]
#[test_case(
    &format!("return nil, '{TRANSPORT_MESSAGE}'"),
    |e| matches!(e, AgentError::Transport { message, retry: None } if message == TRANSPORT_MESSAGE)
    ; "bare string is not retried")]
#[test_case(
    &format!("return nil, {{ kind = 'config', message = '{CONFIG_MESSAGE}', retry = 'transient' }}"),
    |e| matches!(e, AgentError::Config { message } if message == CONFIG_MESSAGE) && e.retry_kind().is_none()
    ; "config kind is not retried")]
#[test_case(
    &format!("return nil, {{ kind = 'config', status = {API_STATUS}, message = '{CONFIG_MESSAGE}' }}"),
    |e| matches!(e, AgentError::Config { message } if message == CONFIG_MESSAGE)
    ; "config kind wins over status")]
#[test_case(
    &format!("error('{RAISED_MESSAGE}')"),
    |e| matches!(e, AgentError::Config { message } if message.contains(RAISED_MESSAGE)) && e.retry_kind().is_none()
    ; "raised error is not retried")]
fn stream_errors_map_to_agent_errors(body: &str, expected: fn(&AgentError) -> bool) {
    let error = stream_error(body);
    assert!(expected(&error), "{error:?}");
}

fn stream_error(body: &str) -> AgentError {
    let instance = format!("return {{ stream = function(ctx, request, emit) {body} end }}");
    let (_host, prepared) = prepare_fake(&instance);
    let (events_tx, _events_rx) = flume::unbounded();
    stream(&prepared, &events_tx).err().unwrap()
}

#[test_case("rate_limit", Some(RetryKind::RateLimit) ; "rate_limit")]
#[test_case("transient", Some(RetryKind::Transient)  ; "transient")]
#[test_case("connect", Some(RetryKind::Connect)      ; "connect")]
#[test_case("timeout", Some(RetryKind::Timeout)      ; "timeout")]
#[test_case("none", None                             ; "none")]
#[test_case("sometimes", None                        ; "unknown")]
fn the_named_retry_class_is_the_budget(class: &str, expected: Option<RetryKind>) {
    for status in ["", &format!("status = {API_STATUS},")] {
        let error = stream_error(&format!(
            "return nil, {{ {status} message = '{TRANSPORT_MESSAGE}', retry = '{class}' }}"
        ));
        assert_eq!(error.retry_kind(), expected, "{error:?}");
    }
}

/// The configured stream timeout is the default for provider requests; with
/// the 300 s send default this would run into `BOUND` instead.
#[test]
fn configured_stream_timeout_reaches_send() {
    let listener = TcpListener::bind(LOOPBACK).unwrap();
    let url = format!("http://{}/", listener.local_addr().unwrap());
    let (release_tx, release_rx) = flume::bounded::<()>(1);
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        stream.write_all(STALLED_HEAD).unwrap();
        let _ = release_rx.recv_timeout(BOUND);
    });
    let instance = format!(
        r#"return {{ stream = function(ctx)
            local res = assert(ctx.send({{ url = "{url}" }}))
            local chunk, err = res:read()
            while chunk do
                chunk, err = res:read()
            end
            return nil, err
        end }}"#
    );
    let timeouts = Timeouts {
        stream: Duration::from_secs_f64(SHORT_IDLE_SECS),
        ..Timeouts::default()
    };
    let (_host, prepared) = prepare_fake_with(&instance, timeouts);
    let (events_tx, _events_rx) = flume::unbounded();
    let error = stream(&prepared, &events_tx).err().unwrap();
    release_tx.send(()).unwrap();
    server.join().unwrap();
    assert!(
        matches!(&error, AgentError::Transport { message, .. } if message.contains(IDLE_TIMEOUT)),
        "{error:?}"
    );
}

fn wait_until(what: &str, mut done: impl FnMut() -> bool) {
    let deadline = Instant::now() + BOUND;
    while !done() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
    }
}

/// The Lua thread runs one task at a time, so `rotate_key` reporting `parked`
/// proves the stream is suspended in its second acquire, not mid-slice.
#[test]
fn dropping_the_stream_cancels_the_call_and_closes_the_emitter() {
    let (_host, prepared) = prepare_fake(
        r#"
        local parked, cancelled = false, false
        return {
            stream = function(ctx, request, emit)
                maki.async.on_cancel(function() cancelled = true end)
                emit({ type = "text_delta", text = "hi" })
                local gate = maki.async.semaphore(1)
                local held = gate:acquire()
                parked = true
                gate:acquire()
                held:release()
                error("resumed after cancel")
            end,
            rotate_key = function() return parked end,
            fetch_usage = function() return { plan = tostring(cancelled), limits = {} } end,
        }
        "#,
    );
    let provider = &prepared.provider;
    let (events_tx, events_rx) = flume::unbounded();
    let invocation = invocation();
    let tools = json!([]);
    let mut call = provider.stream_message(
        &prepared.model,
        &[],
        SYSTEM,
        &tools,
        &events_tx,
        RequestOptions::default(),
        None,
        &invocation,
        RequestDelivery::untracked(),
    );
    let first = smol::block_on(bounded(future::or(
        async {
            let _ = (&mut call).await;
            None
        },
        async { events_rx.recv_async().await.ok() },
    )));
    assert!(matches!(first, Some(ProviderEvent::TextDelta { .. })));
    wait_until("the stream to park", || {
        smol::block_on(bounded(provider.rotate_key()))
    });

    drop(call);
    drop(events_tx);
    assert!(
        matches!(
            events_rx.recv_timeout(BOUND),
            Err(RecvTimeoutError::Disconnected)
        ),
        "the emitter must not keep the event channel open"
    );
    wait_until("on_cancel", || {
        let usage = smol::block_on(bounded(provider.fetch_usage())).unwrap();
        usage.and_then(|usage| usage.plan).as_deref() == Some("true")
    });
}

#[test]
fn optional_methods_dispatch_with_their_arguments() {
    let (_host, prepared) = prepare_fake(
        r#"
        local log = {}
        return {
            stream = function() end,
            end_conversation = function(ctx, id, reason)
                table.insert(log, id .. ":" .. reason)
            end,
            rotate_key = function()
                table.insert(log, "rotate")
                return true
            end,
            fetch_usage = function()
                return { plan = table.concat(log, ","), limits = {} }
            end,
        }
        "#,
    );
    let provider = &prepared.provider;
    for reason in [
        ConversationEnd::Cancelled,
        ConversationEnd::Closed,
        ConversationEnd::Compacted,
        ConversationEnd::Rewound,
    ] {
        smol::block_on(bounded(
            provider.end_conversation(&format!("c-{reason}"), reason),
        ));
    }
    assert!(smol::block_on(bounded(provider.rotate_key())));
    let usage = smol::block_on(bounded(provider.fetch_usage()))
        .unwrap()
        .unwrap();
    assert_eq!(usage.plan.as_deref(), Some(CALL_LOG));
}

#[test]
fn provider_config_is_nonfatal_after_startup() {
    let state = tempfile::tempdir().unwrap();
    maki_storage::paths::init_at(state.path().to_path_buf()).unwrap();
    let host = host();
    host.load_source(
        PLUGIN,
        &spec_source(
            PROVIDER,
            "discover = function() local config = maki.api.provider_config('fake') or {}; return config.models or {} end,",
            "return {}",
        ),
    )
    .unwrap();
    std::fs::write(state.path().join("providers.toml"), "[broken\n").unwrap();
    let providers = registry(&host);

    let mut batches = Vec::new();
    smol::block_on(bounded(providers.fetch_all(
        &Default::default(),
        false,
        |batch| batches.extend(batch.models),
    )));
    assert!(batches.is_empty());
}

#[test]
fn prepare_and_discover_read_the_provider_config_at_call_time() {
    let state = tempfile::tempdir().unwrap();
    maki_storage::paths::init_at(state.path().to_path_buf()).unwrap();
    let host = host();
    host.load_source(PLUGIN, CONFIG_SOURCE).unwrap();
    let providers = registry(&host);
    let mut config = ProvidersConfig::default();
    config.upsert(
        PROVIDER.into(),
        ProviderDef::default().with_answers(&[("base_url".into(), CONFIGURED_URL.into())]),
    );
    config.save().unwrap();

    let mut batches = Vec::new();
    smol::block_on(bounded(providers.fetch_all(
        &Default::default(),
        false,
        |batch| batches.extend(batch.models),
    )));
    let spec = format!("{PROVIDER}/{CONFIGURED_URL}");
    assert!(batches.contains(&spec), "{batches:?}");

    let model = providers.model(&spec).unwrap();
    let prepared = smol::block_on(bounded(providers.prepare(model))).unwrap();
    assert_eq!(prepared.model.id, CONFIGURED_URL);
    assert_eq!(prepared.model.context_window as usize, CONFIGURED_URL.len());
}

#[test_case(false ; "cached")]
#[test_case(true ; "refreshed")]
fn discover_receives_the_refresh_request(refresh: bool) {
    let host = host();
    host.load_source(PLUGIN, REFRESH_SOURCE).unwrap();
    let providers = registry(&host);
    let mut batches = Vec::new();
    smol::block_on(bounded(providers.fetch_all(
        &Default::default(),
        refresh,
        |batch| batches.extend(batch.models),
    )));
    let spec = format!("{PROVIDER}/{refresh}");
    assert!(batches.contains(&spec), "{batches:?}");
}

#[test]
fn usage_limits_decode_from_lua_shapes() {
    let (_host, prepared) = prepare_fake(&format!(
        r#"return {{
            stream = function() end,
            fetch_usage = function()
                return {{ limits = {{
                    {{ kind = {{ Hours = {FIVE_HOURS} }}, percentage = {USED_PERCENT} }},
                    {{ kind = {{ Weekly = {{}} }} }},
                    {{ kind = "Credits", detail = "$1" }},
                }} }}
            end,
        }}"#
    ));
    let usage = smol::block_on(bounded(prepared.provider.fetch_usage()))
        .unwrap()
        .unwrap();
    let kinds: Vec<&UsageWindow> = usage.limits.iter().map(|limit| &limit.kind).collect();
    assert_eq!(
        kinds,
        [
            &UsageWindow::Hours(FIVE_HOURS),
            &UsageWindow::Weekly { model: None },
            &UsageWindow::Credits,
        ]
    );
    assert_eq!(
        usage.limits[0],
        UsageLimit {
            kind: UsageWindow::Hours(FIVE_HOURS),
            percentage: Some(USED_PERCENT),
            reset_at: None,
            detail: None,
        }
    );
}

#[test]
fn missing_optional_methods_are_no_ops() {
    let (_host, prepared) = prepare_fake("return { stream = function() end }");
    let provider = &prepared.provider;
    assert!(!smol::block_on(bounded(provider.rotate_key())));
    smol::block_on(bounded(
        provider.end_conversation(CONVERSATION, ConversationEnd::Closed),
    ));
    assert!(
        smol::block_on(bounded(provider.fetch_usage()))
            .unwrap()
            .is_none()
    );
    smol::block_on(bounded(provider.refresh_auth())).unwrap();
    smol::block_on(bounded(provider.reload_auth())).unwrap();
    assert_eq!(provider.key_count(), 1);
}

#[test]
fn failing_end_conversation_is_swallowed() {
    let (_host, prepared) = prepare_fake(&format!(
        "return {{ stream = function() end, end_conversation = function() error('{RAISED_MESSAGE}') end }}"
    ));
    smol::block_on(bounded(
        prepared
            .provider
            .end_conversation(CONVERSATION, ConversationEnd::Cancelled),
    ));
}

#[test]
fn key_count_is_read_at_prepare_and_after_an_auth_reload() {
    let (_host, prepared) = prepare_fake(&format!(
        r#"
        local reads, keys = 0, {KEY_COUNT}
        return {{
            stream = function() end,
            key_count = function() reads = reads + 1; return keys end,
            reload_auth = function() keys = keys + 1 end,
            fetch_usage = function() return {{ plan = tostring(reads), limits = {{}} }} end,
        }}
        "#
    ));
    let provider = &prepared.provider;
    assert_eq!(provider.key_count(), KEY_COUNT);
    assert_eq!(provider.key_count(), KEY_COUNT);
    let reads = || {
        smol::block_on(bounded(provider.fetch_usage()))
            .unwrap()
            .unwrap()
            .plan
    };
    assert_eq!(reads().as_deref(), Some("1"));

    smol::block_on(bounded(provider.reload_auth())).unwrap();
    assert_eq!(provider.key_count(), KEY_COUNT + 1);
    assert_eq!(reads().as_deref(), Some("2"));

    let (_host, prepared) = prepare_fake(&format!(
        "return {{ stream = function() end, key_count = {KEY_COUNT} }}"
    ));
    assert_eq!(prepared.provider.key_count(), KEY_COUNT);
}

#[test]
fn instance_without_stream_fails_prepare() {
    let host = host();
    host.load_source(PLUGIN, &spec_source(PROVIDER, "", "return {}"))
        .unwrap();
    let providers = registry(&host);
    let model = providers.model(SPEC).unwrap();
    let error = smol::block_on(bounded(providers.prepare(model)))
        .err()
        .unwrap();
    assert!(matches!(error, AgentError::Config { .. }), "{error}");
}
