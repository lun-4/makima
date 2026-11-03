use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures_lite::future;
use maki_agent::tools::ToolRegistry;
use maki_config::providers::{ProviderDef, ProvidersConfig};
use maki_lua::PluginHost;
use maki_providers::{
    AgentError, ContentBlock, ConversationEnd, Invocation, InvocationPurpose, Message,
    PreparedProvider, ProviderEvent, ProviderRegistry, RequestOptions, Role, StopReason, Timeouts,
    UsageLimit, UsageWindow,
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
const ATTEMPT: u32 = 2;
const USER_TEXT: &str = "hello";
const SYSTEM: &str = "be brief";
const DELTA: &str = "hi";
const API_MESSAGE: &str = "slow down";
const API_STATUS: u16 = 429;
const RETRY_AFTER_SECS: f64 = 1.5;
const TRANSPORT_MESSAGE: &str = "connection reset";
const RAISED_MESSAGE: &str = "boom";
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
const CONFIG_KEY_ERR: &str = "config key \"discover_models\"";
const TOP_LEVEL_ERR: &str = "may only be called at the top level during plugin load";
const CALL_LOG: &str =
    "c-cancelled:cancelled,c-closed:closed,c-compacted:compacted,c-rewound:rewound,rotate";
const KEY_COUNT: usize = 3;
const CONFIGURED_URL: &str = "http://configured.invalid";
const FIVE_HOURS: u64 = 5;
const USED_PERCENT: u32 = 70;
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
                inv.purpose, inv.agent_id, tostring(request.fast),
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
    let host = host();
    host.load_source(PLUGIN, &spec_source(PROVIDER, "", instance))
        .unwrap();
    let providers = registry(&host);
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
    smol::block_on(bounded(prepared.provider.stream_message(
        &prepared.model,
        &[user_message()],
        SYSTEM,
        &json!([]),
        events,
        RequestOptions::default(),
        None,
        &invocation(),
    )))
}

fn text(message: &Message) -> &str {
    match &message.content[0] {
        ContentBlock::Text { text } => text,
        other => panic!("expected text, got {other:?}"),
    }
}

#[test_case(r#"colour = "red","# , UNKNOWN_FIELD_ERR ; "unknown field")]
#[test_case(r#"discover = 42,"# , EXPECTED_FUNCTION_ERR ; "callback of wrong type")]
#[test_case(r#"auth = { { id = "k", label = "K" } },"# , LOGIN_REQUIRED_ERR ; "login missing")]
#[test_case(r#"auth = { { id = "k", label = "K", login = print }, { id = "k", label = "K", login = print } },"# , DUPLICATE_METHOD_ERR ; "duplicate method")]
#[test_case(r#"auth = { { id = "k", label = "K", login = print } }, aliases = { { id = "a", label = "A", method = "x" } },"# , UNKNOWN_METHOD_ERR ; "alias naming unknown method")]
#[test_case(r#"auth = { { id = "k", label = "K", login = print } }, aliases = { { id = "a/b", label = "A", method = "k" } },"# , BAD_ALIAS_ERR ; "alias with bad id")]
#[test_case(r#"config = { { key = "discover_models", prompt = "Discover" } },"# , CONFIG_KEY_ERR ; "non string config key")]
#[test_case(r#"auth_status = 1,"# , EXPECTED_FUNCTION_ERR ; "auth_status of wrong type")]
fn invalid_spec_fields_are_rejected(extra: &str, expected: &str) {
    let error = host()
        .load_source(PLUGIN, &spec_source(PROVIDER, extra, "return {}"))
        .unwrap_err();
    assert!(error.to_string().contains(expected), "{error}");
}

#[test_case(r#"maki.api.register_provider({ id = "x", display_name = "X" })"#, PREPARE_REQUIRED_ERR ; "missing prepare")]
#[test_case(r#"maki.api.register_provider({ id = 1, display_name = "X", prepare = print })"#, "invalid type" ; "id of wrong type")]
#[test_case(r#"maki.api.register_provider({ id = "x/y", display_name = "X", prepare = print })"#, BAD_ID_ERR ; "id with slash")]
#[test_case(r#"maki.api.register_provider({ id = "..", display_name = "X", prepare = print })"#, BAD_ID_ERR ; "id with leading dot")]
#[test_case(r#"maki.api.register_provider({ id = "x", display_name = "X", prepare = print, models = { { id = "m", colour = 1 } } })"#, UNKNOWN_FIELD_ERR ; "model descriptor field")]
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
    &format!("return nil, {{ status = {API_STATUS}, message = '{API_MESSAGE}', retry_after = {RETRY_AFTER_SECS} }}"),
    |e| matches!(e, AgentError::Api { status: API_STATUS, message, retry_after: Some(after) }
        if message == API_MESSAGE && *after == Duration::from_secs_f64(RETRY_AFTER_SECS))
    ; "error table with status is an api error")]
#[test_case(
    &format!("return nil, {{ message = '{TRANSPORT_MESSAGE}' }}"),
    |e| matches!(e, AgentError::Io(io) if io.to_string() == TRANSPORT_MESSAGE) && e.retry_kind().is_some()
    ; "error table without status is retryable")]
#[test_case(
    &format!("error('{RAISED_MESSAGE}')"),
    |e| matches!(e, AgentError::Config { message } if message.contains(RAISED_MESSAGE)) && e.retry_kind().is_none()
    ; "raised error is not retried")]
fn stream_errors_map_to_agent_errors(body: &str, expected: fn(&AgentError) -> bool) {
    let instance = format!("return {{ stream = function(ctx, request, emit) {body} end }}");
    let (_host, prepared) = prepare_fake(&instance);
    let (events_tx, _events_rx) = flume::unbounded();
    let error = stream(&prepared, &events_tx).err().unwrap();
    assert!(expected(&error), "{error:?}");
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
        events_rx.recv_timeout(BOUND).is_err(),
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
fn prepare_and_discover_read_the_provider_config_at_call_time() {
    let state = tempfile::tempdir().unwrap();
    maki_storage::paths::init_at(state.path().to_path_buf()).unwrap();
    let host = host();
    host.load_source(PLUGIN, CONFIG_SOURCE).unwrap();
    let providers = registry(&host);
    let mut config = ProvidersConfig::default();
    config.upsert(
        PROVIDER.into(),
        ProviderDef {
            base_url: Some(CONFIGURED_URL.into()),
            ..ProviderDef::default()
        },
    );
    config.save().unwrap();

    let mut batches = Vec::new();
    smol::block_on(bounded(
        providers.fetch_all(&Default::default(), |batch| batches.extend(batch.models)),
    ));
    let spec = format!("{PROVIDER}/{CONFIGURED_URL}");
    assert!(batches.contains(&spec), "{batches:?}");

    let model = providers.model(&spec).unwrap();
    let prepared = smol::block_on(bounded(providers.prepare(model))).unwrap();
    assert_eq!(prepared.model.id, CONFIGURED_URL);
    assert_eq!(prepared.model.context_window as usize, CONFIGURED_URL.len());
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
fn key_count_is_read_once_at_prepare() {
    let (_host, prepared) = prepare_fake(&format!(
        r#"
        local reads = 0
        return {{
            stream = function() end,
            key_count = function() reads = reads + 1; return {KEY_COUNT} end,
            fetch_usage = function() return {{ plan = tostring(reads), limits = {{}} }} end,
        }}
        "#
    ));
    let provider = &prepared.provider;
    assert_eq!(provider.key_count(), KEY_COUNT);
    assert_eq!(provider.key_count(), KEY_COUNT);
    let usage = smol::block_on(bounded(provider.fetch_usage()))
        .unwrap()
        .unwrap();
    assert_eq!(usage.plan.as_deref(), Some("1"));

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
