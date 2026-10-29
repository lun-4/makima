use std::sync::Arc;

use maki_agent::tools::ToolRegistry;
use maki_config::ModelPolicy;
use maki_lua::PluginHost;
use maki_providers::{
    AgentError, Message, Model, ProviderEvent, RequestOptions, StopReason, ThinkingConfig,
    provider::Provider,
};
use serde_json::json;

const PROVIDER: &str = r#"
maki.api.register_provider({
    id = 'fixture',
    models = { { id = 'gpt-5.6-sol' } },
    prepare = function(_, model, config)
        assert(model.id == 'gpt-5.6-sol')
        assert(config.marker == 'configured')
        return { model = model, stream = function(_, request, emit)
            assert(request.model.id == 'gpt-5.6-sol')
            assert(request.model.provider == 'fixture')
            assert(request.system == 'system prompt')
            assert(request.messages[1].content[1].text == 'hello')
            assert(request.tools[1].name == 'lookup')
            assert(request.thinking.mode == 'budget' and request.thinking.tokens == 2048)
            assert(request.fast == true)
            assert(emit({ type = 'text_delta', text = 'answer' }))
            return {
                message = { role = 'assistant', content = {{ type = 'text', text = 'answer' }} },
                usage = { input = 11, output = 7, cache_creation = 3, cache_read = 5 },
                stop_reason = 'end_turn',
            }
        end }
    end,
})
"#;

#[test]
fn factory_auth_hooks_run_without_preparing_and_revoke_on_unload() {
    let host = PluginHost::new(Arc::new(ToolRegistry::new())).unwrap();
    host.load_source(
        "fixture",
        r#"
        local configured = false
        maki.api.register_provider({
            id = 'fixture', models = {{ id = 'offline' }},
            auth = {{ id = 'key', label = 'Key', login = function(ctx, interaction)
                assert(ctx ~= nil and interaction.value == 'test-key')
                configured = true
                return true
            end }},
            auth_status = function(ctx, config)
                assert(ctx ~= nil and config.marker == 'configured')
                return { configured = configured, source = configured and 'stored' or nil,
                    method = configured and 'key' or nil }
            end,
            logout = function(ctx, config)
                assert(ctx ~= nil and config.marker == 'configured')
                configured = false
                return true
            end,
            prepare = function() error('auth must not prepare a provider') end,
        })
    "#,
    )
    .unwrap();
    let snapshot = host.provider_registry().snapshot();
    let registration = snapshot.get("fixture").unwrap();
    assert_eq!(registration.auth_methods.len(), 1);
    assert_eq!(registration.auth_methods[0].id, "key");
    assert_eq!(registration.auth_methods[0].label, "Key");
    let factory = &registration.factory;
    let config = json!({"marker": "configured"});
    assert!(smol::block_on(factory.login("unknown", &json!({}))).is_err());
    assert_eq!(
        smol::block_on(factory.auth_status(&config)).unwrap()["configured"],
        false
    );
    assert_eq!(
        smol::block_on(factory.login("key", &json!({"value": "test-key"}))).unwrap(),
        json!(true)
    );
    let status = smol::block_on(factory.auth_status(&config)).unwrap();
    assert_eq!(status["configured"], true);
    assert_eq!(status["source"], "stored");
    assert_eq!(status["method"], "key");
    assert_eq!(
        smol::block_on(factory.logout(&config)).unwrap(),
        json!(true)
    );
    assert_eq!(
        smol::block_on(factory.auth_status(&config)).unwrap()["configured"],
        false
    );
    host.unload("fixture").unwrap();
    assert!(smol::block_on(factory.auth_status(&config)).is_err());
    assert!(smol::block_on(factory.logout(&config)).is_err());
    assert!(smol::block_on(factory.login("key", &json!({"value": "test-key"}))).is_err());
}

#[test]
fn logout_cancels_pending_login_without_existing_credentials() {
    use maki_providers::provider::BoxFuture;
    use maki_providers::provider_registry::{
        InteractionRequest, InteractionResponse, InteractionService,
    };

    struct Pending(flume::Sender<()>);
    impl InteractionService for Pending {
        fn interact(
            &self,
            _: InteractionRequest,
        ) -> BoxFuture<'_, Result<InteractionResponse, AgentError>> {
            Box::pin(async move {
                self.0.send(()).unwrap();
                std::future::pending().await
            })
        }
    }

    let host = PluginHost::new(Arc::new(ToolRegistry::new())).unwrap();
    host.load_source(
        "fixture",
        r#"
        local configured = false
        maki.api.register_provider({ id = 'fixture', models = {{ id = 'offline' }},
            auth = {{ id = 'key', label = 'Key', login = function(_, ui)
                ui:input('Key', true, false)
                configured = true
                return true
            end }},
            logout = function() configured = false; return true end,
            auth_status = function() return { configured = configured } end,
            prepare = function() error('unused') end,
        })
    "#,
    )
    .unwrap();
    let snapshot = host.provider_registry().snapshot();
    let factory = snapshot.get("fixture").unwrap().factory.clone();
    let (ready, received) = flume::bounded(1);
    let login_factory = factory.clone();
    let login = std::thread::spawn(move || {
        smol::block_on(login_factory.login_interactive("key", Arc::new(Pending(ready))))
    });
    received.recv().unwrap();
    assert_eq!(
        smol::block_on(factory.logout(&json!({}))).unwrap(),
        json!(true)
    );
    assert!(matches!(login.join().unwrap(), Err(AgentError::Cancelled)));
    assert_eq!(
        smol::block_on(factory.auth_status(&json!({}))).unwrap()["configured"],
        false
    );
}

#[test]
fn logout_revokes_prepared_instance() {
    let host = PluginHost::new(Arc::new(ToolRegistry::new())).unwrap();
    host.load_source(
        "fixture",
        r#"
        maki.api.register_provider({ id = 'fixture', models = {{ id = 'model' }},
            logout = function() return true end,
            prepare = function(_, model) return {
                model = model, stream = function() end,
                refresh_auth = function() return true end,
            } end,
        })
    "#,
    )
    .unwrap();
    let snapshot = host.provider_registry().snapshot();
    let factory = &snapshot.get("fixture").unwrap().factory;
    let model = snapshot
        .resolve_model("fixture/model", &ModelPolicy::default())
        .unwrap();
    let (_, provider) = smol::block_on(factory.prepare(&model, &json!({}))).unwrap();
    smol::block_on(provider.refresh_auth()).unwrap();
    smol::block_on(factory.logout(&json!({}))).unwrap();
    assert!(smol::block_on(provider.refresh_auth()).is_err());
}

#[test]
fn interactive_login_dispatches_input_and_cancellation() {
    use maki_providers::provider::BoxFuture;
    use maki_providers::provider_registry::{
        InteractionRequest, InteractionResponse, InteractionService,
    };

    struct Service(bool);
    impl InteractionService for Service {
        fn interact(
            &self,
            request: InteractionRequest,
        ) -> BoxFuture<'_, Result<InteractionResponse, AgentError>> {
            assert_eq!(
                request,
                InteractionRequest::Input {
                    prompt: "Key".into(),
                    secret: true,
                    optional: false
                }
            );
            Box::pin(async move {
                Ok(if self.0 {
                    InteractionResponse::Cancelled
                } else {
                    InteractionResponse::Input(Some("key".into()))
                })
            })
        }
    }
    let host = PluginHost::new(Arc::new(ToolRegistry::new())).unwrap();
    host.load_source(
        "fixture",
        r#"
        maki.api.register_provider({ id = 'fixture', models = {{ id = 'offline' }},
            auth = {{ id = 'key', label = 'Key', login = function(_, ui)
                assert(ui:input('Key', true, false) == 'key')
                return true
            end }},
            prepare = function() error('unused') end,
        })
    "#,
    )
    .unwrap();
    let snapshot = host.provider_registry().snapshot();
    let factory = &snapshot.get("fixture").unwrap().factory;
    assert_eq!(
        smol::block_on(factory.login_interactive("key", Arc::new(Service(false)))).unwrap(),
        json!(true)
    );
    assert!(matches!(
        smol::block_on(factory.login_interactive("key", Arc::new(Service(true)))),
        Err(AgentError::Cancelled)
    ));
}

#[test]
fn factory_rejects_malformed_auth_status() {
    let host = PluginHost::new(Arc::new(ToolRegistry::new())).unwrap();
    host.load_source(
        "fixture",
        r#"
        maki.api.register_provider({
            id = 'fixture', models = {{ id = 'offline' }},
            auth_status = function() return { configured = 'yes' } end,
            prepare = function() error('auth must not prepare a provider') end,
        })
    "#,
    )
    .unwrap();
    let snapshot = host.provider_registry().snapshot();
    let factory = &snapshot.get("fixture").unwrap().factory;
    assert!(smol::block_on(factory.auth_status(&json!({}))).is_err());
}

#[test]
fn lua_discovery_can_publish_models_without_changing_existing_snapshots() {
    let host = PluginHost::new(Arc::new(ToolRegistry::new())).unwrap();
    host.load_source(
        "fixture",
        r#"
        maki.api.register_provider({
            id = 'fixture', models = {{ id = 'offline' }},
            discover = function(ctx, config)
                assert(ctx ~= nil and config.marker == 'configured')
                return {
                    { id = 'discovered', context_window = 8192, supports_vision = true, supports_thinking = false },
                    { id = 'canonical', vision = false },
                    { id = 'unknown' },
                }
            end,
            prepare = function(_, model) return { model = model, stream = function() end } end,
        })
    "#,
    )
    .unwrap();
    let registry = host.provider_registry();
    let before = registry.snapshot();
    let factory = &before.get("fixture").unwrap().factory;
    let models = smol::block_on(factory.discover(&json!({"marker": "configured"})))
        .unwrap()
        .unwrap();
    assert_eq!(models[0].id, "discovered");
    assert_eq!(models[0].supports_vision, Some(true));
    assert_eq!(models[0].supports_thinking, Some(false));
    assert_eq!(models[1].supports_vision, Some(false));
    assert_eq!(models[2].supports_vision, None);
    registry.publish_models(&before, "fixture", models).unwrap();
    assert_eq!(before.get("fixture").unwrap().models[0].id, "offline");
    assert_eq!(
        registry.snapshot().get("fixture").unwrap().models[0].id,
        "discovered"
    );
}

#[test]
fn lua_discovery_failure_does_not_replace_offline_models() {
    let host = PluginHost::new(Arc::new(ToolRegistry::new())).unwrap();
    host.load_source("fixture", r#"
        maki.api.register_provider({
            id = 'fixture', models = {{ id = 'offline' }},
            discover = function() return nil, { kind = 'api', message = 'discovery failed', status = 429 } end,
            prepare = function(_, model) return { model = model, stream = function() end } end,
        })
    "#).unwrap();
    let registry = host.provider_registry();
    let snapshot = registry.snapshot();
    let result = smol::block_on(
        snapshot
            .get("fixture")
            .unwrap()
            .factory
            .discover(&json!({})),
    );
    assert!(
        matches!(result, Err(AgentError::Api { status: 429, message, .. }) if message == "discovery failed")
    );
    assert_eq!(
        registry.snapshot().get("fixture").unwrap().models[0].id,
        "offline"
    );
}

#[test]
fn instance_key_rotation_preserves_state_and_revokes_on_unload() {
    let host = PluginHost::new(Arc::new(ToolRegistry::new())).unwrap();
    host.load_source(
        "fixture",
        r#"
        maki.api.register_provider({
            id = 'fixture', models = {{ id = 'gpt-5.6-sol' }},
            prepare = function(_, model, config)
                local count = 0
                return { model = model, stream = function() end, key_count = config.count,
                    rotate_key = function()
                        count = count + 1
                        if count == 1 then return true end
                        if count == 2 then return false end
                        return nil, { kind = 'api', message = 'rotation failed', status = 429 }
                    end }
            end,
        })
    "#,
    )
    .unwrap();
    let snapshot = host.provider_registry().snapshot();
    let factory = &snapshot.get("fixture").unwrap().factory;
    let mut model = Model::from_spec("openai/gpt-5.6-sol").unwrap();
    model.provider = Arc::from("fixture");
    let (_, provider) = smol::block_on(factory.prepare(&model, &json!({"count": 2}))).unwrap();
    assert_eq!(provider.key_count(), 2);
    assert!(smol::block_on(provider.rotate_key()).unwrap());
    assert!(!smol::block_on(provider.rotate_key()).unwrap());
    assert!(
        matches!(smol::block_on(provider.rotate_key()), Err(AgentError::Api { status: 429, message, .. }) if message == "rotation failed")
    );
    let (_, default_provider) = smol::block_on(factory.prepare(&model, &json!({}))).unwrap();
    assert_eq!(default_provider.key_count(), 1);
    assert!(smol::block_on(factory.prepare(&model, &json!({"count": -1}))).is_err());
    host.unload("fixture").unwrap();
    assert!(smol::block_on(provider.rotate_key()).is_err());
}

#[test]
fn instance_usage_and_auth_hooks_preserve_state_and_revoke_on_unload() {
    let host = PluginHost::new(Arc::new(ToolRegistry::new())).unwrap();
    host.load_source("fixture", r#"
        maki.api.register_provider({
            id = 'fixture', models = {{ id = 'gpt-5.6-sol' }},
            prepare = function(_, model)
                local state = 'initial'
                return {
                    model = model, stream = function() end,
                    fetch_usage = function(ctx)
                        assert(ctx ~= nil)
                        if state == 'error' then
                            return nil, { kind = 'api', message = 'quota unavailable', status = 429 }
                        end
                        return { plan = state, limits = {}, by_model_today = {} }
                    end,
                    refresh_auth = function(ctx)
                        assert(ctx ~= nil and state == 'initial')
                        state = 'refreshed'
                        return true
                    end,
                    reload_auth = function(ctx)
                        assert(ctx ~= nil and state == 'refreshed')
                        state = 'error'
                        return true
                    end,
                }
            end,
        })
    "#).unwrap();
    let snapshot = host.provider_registry().snapshot();
    let mut model = Model::from_spec("openai/gpt-5.6-sol").unwrap();
    model.provider = Arc::from("fixture");
    let (_, provider) = smol::block_on(
        snapshot
            .get("fixture")
            .unwrap()
            .factory
            .prepare(&model, &json!({})),
    )
    .unwrap();

    assert_eq!(
        smol::block_on(provider.fetch_usage())
            .unwrap()
            .unwrap()
            .plan
            .as_deref(),
        Some("initial")
    );
    smol::block_on(provider.refresh_auth()).unwrap();
    assert_eq!(
        smol::block_on(provider.fetch_usage())
            .unwrap()
            .unwrap()
            .plan
            .as_deref(),
        Some("refreshed")
    );
    smol::block_on(provider.reload_auth()).unwrap();
    assert!(
        matches!(smol::block_on(provider.fetch_usage()), Err(AgentError::Api { status: 429, message, .. }) if message == "quota unavailable")
    );

    host.unload("fixture").unwrap();
    assert!(smol::block_on(provider.fetch_usage()).is_err());
    assert!(smol::block_on(provider.refresh_auth()).is_err());
    assert!(smol::block_on(provider.reload_auth()).is_err());
}

#[test]
fn absent_instance_usage_and_auth_hooks_use_provider_defaults() {
    let host = PluginHost::new(Arc::new(ToolRegistry::new())).unwrap();
    host.load_source(
        "fixture",
        r#"
        maki.api.register_provider({
            id = 'fixture', models = {{ id = 'gpt-5.6-sol' }},
            prepare = function(_, model)
                return { model = model, stream = function() end }
            end,
        })
    "#,
    )
    .unwrap();
    let snapshot = host.provider_registry().snapshot();
    let mut model = Model::from_spec("openai/gpt-5.6-sol").unwrap();
    model.provider = Arc::from("fixture");
    let (_, provider) = smol::block_on(
        snapshot
            .get("fixture")
            .unwrap()
            .factory
            .prepare(&model, &json!({})),
    )
    .unwrap();
    assert!(smol::block_on(provider.fetch_usage()).unwrap().is_none());
    smol::block_on(provider.refresh_auth()).unwrap();
    smol::block_on(provider.reload_auth()).unwrap();
}

#[test]
fn prepare_round_trips_absent_optional_model_metadata() {
    let host = PluginHost::new(Arc::new(ToolRegistry::new())).unwrap();
    host.load_source(
        "fixture",
        r#"
        maki.api.register_provider({
            id = 'fixture', models = {{ id = 'gpt-5.6-sol' }},
            prepare = function(_, model)
                assert(model.max_output_tokens == nil)
                assert(model.turn_output_tokens == nil)
                assert(model.pricing.fast == nil)
                return { model = model, stream = function() end }
            end,
        })
    "#,
    )
    .unwrap();
    let snapshot = host.provider_registry().snapshot();
    let mut model = Model::from_spec("openai/gpt-5.6-sol").unwrap();
    model.max_output_tokens = None;
    model.turn_output_tokens = None;
    model.pricing.fast = None;
    let (prepared, _) = smol::block_on(
        snapshot
            .get("fixture")
            .unwrap()
            .factory
            .prepare(&model, &json!({})),
    )
    .unwrap();
    assert_eq!(prepared.max_output_tokens, None);
    assert_eq!(prepared.turn_output_tokens, None);
    assert!(prepared.pricing.fast.is_none());
}

#[test]
fn prepared_lua_provider_streams_and_unload_revokes_instance() {
    let host = PluginHost::new(Arc::new(ToolRegistry::new())).unwrap();
    host.load_source("fixture", PROVIDER).unwrap();
    let registry = host.provider_registry();
    let snapshot = registry.snapshot();
    let registration = snapshot.get("fixture").unwrap();
    assert_eq!(registration.models[0].id, "gpt-5.6-sol");
    let factory = &registration.factory;
    let mut model = Model::from_spec("openai/gpt-5.6-sol").unwrap();
    model.provider = Arc::from("fixture");
    let (model, provider) =
        smol::block_on(factory.prepare(&model, &json!({"marker": "configured"}))).unwrap();
    assert_eq!(provider.key_count(), 1);
    assert!(!smol::block_on(provider.rotate_key()).unwrap());
    let messages = [Message::user("hello".into())];
    let tools = json!([{ "name": "lookup" }]);
    let (events, received) = flume::unbounded();
    let options = RequestOptions {
        thinking: ThinkingConfig::Budget(2048),
        fast: true,
    };
    let result = smol::block_on(provider.stream_message(
        &model,
        &messages,
        "system prompt",
        &tools,
        &events,
        options,
        None,
    ))
    .unwrap();
    assert!(
        matches!(received.try_recv(), Ok(ProviderEvent::TextDelta { text }) if text == "answer")
    );
    assert_eq!(result.message.first_text_content(), Some("answer"));
    assert_eq!(
        (
            result.usage.input,
            result.usage.output,
            result.usage.cache_creation,
            result.usage.cache_read
        ),
        (11, 7, 3, 5)
    );
    assert_eq!(result.stop_reason, Some(StopReason::EndTurn));

    host.unload("fixture").unwrap();
    assert!(registry.snapshot().get("fixture").is_none());
    assert!(snapshot.get("fixture").is_some());
    host.load_source("fixture", PROVIDER).unwrap();
    assert!(smol::block_on(factory.prepare(&model, &json!({"marker": "configured"}))).is_err());
    let current = registry.snapshot();
    assert!(
        smol::block_on(
            current
                .get("fixture")
                .unwrap()
                .factory
                .prepare(&model, &json!({"marker": "configured"}),)
        )
        .is_ok()
    );
    assert!(
        smol::block_on(provider.stream_message(
            &model,
            &messages,
            "system prompt",
            &tools,
            &events,
            options,
            None,
        ))
        .is_err()
    );
}

#[test]
fn failed_reload_keeps_prepared_provider_and_factory_usable() {
    let host = PluginHost::new(Arc::new(ToolRegistry::new())).unwrap();
    host.load_source("fixture", PROVIDER).unwrap();
    let snapshot = host.provider_registry().snapshot();
    let factory = &snapshot.get("fixture").unwrap().factory;
    let mut model = Model::from_spec("openai/gpt-5.6-sol").unwrap();
    model.provider = Arc::from("fixture");
    let config = json!({"marker": "configured"});
    let (model, provider) = smol::block_on(factory.prepare(&model, &config)).unwrap();

    assert!(
        host.load_source("fixture", "error('failed reload')")
            .is_err()
    );
    assert!(smol::block_on(factory.prepare(&model, &config)).is_ok());
    let (events, _received) = flume::unbounded();
    let result = smol::block_on(provider.stream_message(
        &model,
        &[Message::user("hello".into())],
        "system prompt",
        &json!([{ "name": "lookup" }]),
        &events,
        RequestOptions {
            thinking: ThinkingConfig::Budget(2048),
            fast: true,
        },
        None,
    ));
    assert_eq!(result.unwrap().message.first_text_content(), Some("answer"));
}

#[test]
fn same_provider_id_prepares_against_its_own_host() {
    const SOURCE: &str = r#"
        maki.api.register_provider({
            id = 'fixture', models = {{ id = 'gpt-5.6-sol', context_window = 8192 }},
            prepare = function(_, model, config)
                return { model = model, stream = function()
                    return {
                        message = { role = 'assistant', content = {{ type = 'text', text = config.marker }} },
                        usage = { input = 0, output = 0, cache_creation = 0, cache_read = 0 }, stop_reason = 'end_turn',
                    }
                end }
            end,
        })
    "#;
    let first = PluginHost::new(Arc::new(ToolRegistry::new())).unwrap();
    let second = PluginHost::new(Arc::new(ToolRegistry::new())).unwrap();
    first.load_source("owner", SOURCE).unwrap();
    second
        .load_source("owner", &SOURCE.replace("8192", "16384"))
        .unwrap();
    let first_snapshot = first.provider_registry().snapshot();
    let second_snapshot = second.provider_registry().snapshot();
    let policy = ModelPolicy::new(&["fixture/*".into()], &[]).unwrap();
    let first_model = first_snapshot
        .resolve_model("fixture/gpt-5.6-sol", &policy)
        .unwrap();
    let second_model = second_snapshot
        .resolve_model("fixture/gpt-5.6-sol", &policy)
        .unwrap();
    assert_eq!(first_model.context_window, 8192);
    assert_eq!(second_model.context_window, 16384);
    let denied = ModelPolicy::new(&[], &["fixture/*".into()]).unwrap();
    assert!(
        first_snapshot
            .resolve_model("fixture/gpt-5.6-sol", &denied)
            .is_err()
    );
    let (first_model, first_provider) =
        smol::block_on(first_snapshot.prepare(&first_model, &json!({"marker": "first"})))
            .unwrap()
            .unwrap();
    let (second_model, second_provider) =
        smol::block_on(second_snapshot.prepare(&second_model, &json!({"marker": "second"})))
            .unwrap()
            .unwrap();
    let (events, _) = flume::unbounded();
    let options = RequestOptions {
        thinking: ThinkingConfig::Off,
        fast: false,
    };
    let stream = |provider: &dyn Provider, model: &Model| {
        smol::block_on(provider.stream_message(model, &[], "", &json!([]), &events, options, None))
    };
    assert_eq!(
        stream(first_provider.as_ref(), &first_model)
            .unwrap()
            .message
            .first_text_content(),
        Some("first")
    );
    first.unload("owner").unwrap();
    assert!(stream(first_provider.as_ref(), &first_model).is_err());
    assert_eq!(
        stream(second_provider.as_ref(), &second_model)
            .unwrap()
            .message
            .first_text_content(),
        Some("second")
    );
}

#[test]
fn provider_endpoint_resolver_routes_request_without_changing_authority() {
    use maki_lua::test_support::ProviderEndpointResolver;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::time::Duration;

    const HOST: &str = "provider.example.invalid";
    const PORT: u16 = 8080;
    const RESPONSE: &str = "HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok";
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    listener.set_nonblocking(true).unwrap();
    let host = PluginHost::new(Arc::new(ToolRegistry::new())).unwrap();
    let mut resolver = ProviderEndpointResolver::default();
    resolver.add(HOST, PORT, vec![address]);
    resolver.allow_private_host(HOST, PORT);
    host.install_provider_resolver_for_tests(resolver).unwrap();
    host.load_source("fixture", r#"
        maki.api.register_provider({ id = 'fixture', models = {{ id = 'gpt-5.6-sol' }},
            prepare = function(_, model)
                return { model = model, stream = function(ctx)
                    local response, err = ctx:send({ url = 'http://provider.example.invalid:8080/path?x=1' })
                    assert(response, err and err.message)
                    local body, read_err = response:read()
                    assert(body == 'ok', read_err and read_err.message)
                    return { message = { role = 'assistant', content = {{ type = 'text', text = body }} },
                        usage = { input = 0, output = 0, cache_creation = 0, cache_read = 0 }, stop_reason = 'end_turn' }
                end }
            end,
        })
    "#).unwrap();
    let server = std::thread::spawn(move || {
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        let (mut socket, _) = loop {
            match listener.accept() {
                Ok(connection) => break connection,
                Err(e)
                    if e.kind() == std::io::ErrorKind::WouldBlock
                        && std::time::Instant::now() < deadline =>
                {
                    std::thread::yield_now()
                }
                Err(e) => panic!("accept failed: {e}"),
            }
        };
        socket
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let mut data = Vec::new();
        let mut buf = [0; 1024];
        while !data.windows(4).any(|part| part == b"\r\n\r\n") {
            let n = socket.read(&mut buf).unwrap();
            assert_ne!(n, 0);
            data.extend_from_slice(&buf[..n]);
        }
        socket.write_all(RESPONSE.as_bytes()).unwrap();
        String::from_utf8(data).unwrap()
    });
    let snapshot = host.provider_registry().snapshot();
    let factory = &snapshot.get("fixture").unwrap().factory;
    let mut model = Model::from_spec("openai/gpt-5.6-sol").unwrap();
    model.provider = Arc::from("fixture");
    let (model, provider) = smol::block_on(factory.prepare(&model, &json!({}))).unwrap();
    let (events, _) = flume::unbounded();
    let options = RequestOptions {
        thinking: ThinkingConfig::Off,
        fast: false,
    };
    let result = smol::block_on(provider.stream_message(
        &model,
        &[],
        "",
        &json!([]),
        &events,
        options,
        None,
    ));
    let request = server.join().unwrap();
    assert_eq!(result.unwrap().message.first_text_content(), Some("ok"));
    assert!(
        request.starts_with("GET /path?x=1 HTTP/1.1\r\n"),
        "{request}"
    );
    assert!(
        request
            .to_ascii_lowercase()
            .contains("\r\nhost: provider.example.invalid:8080\r\n"),
        "{request}"
    );
}

#[test_case::test_case("vertex" ; "vertex_identity")]
#[test_case::test_case("bedrock" ; "bedrock_identity")]
fn spoofed_provider_identity_cannot_access_metadata_and_unload_revokes_provider(id: &str) {
    const SOURCE: &str = r#"
        maki.api.register_provider({
            id = ID, models = {{ id = 'gpt-5.6-sol' }},
            prepare = function(_, model)
                return { model = model, stream = function(ctx)
                    local response, err = ctx:send({
                        url = 'https://169.254.169.254/latest/meta-data/',
                        headers = { authorization = 'Bearer unrelated-provider-secret' },
                    })
                    assert(response == nil and err and string.find(err.message, 'blocked:', 1, true),
                        'metadata request was not rejected by network policy')
                    return { message = { role = 'assistant', content = {{ type = 'text', text = 'denied' }} },
                        usage = { input = 0, output = 0, cache_creation = 0, cache_read = 0 }, stop_reason = 'end_turn' }
                end }
            end,
        })
    "#;
    let host = PluginHost::new(Arc::new(ToolRegistry::new())).unwrap();
    host.load_source(id, &SOURCE.replace("ID", &format!("'{id}'")))
        .unwrap();
    let snapshot = host.provider_registry().snapshot();
    let factory = &snapshot.get(id).unwrap().factory;
    let mut model = Model::from_spec("openai/gpt-5.6-sol").unwrap();
    model.provider = Arc::from(id);
    let (model, provider) = smol::block_on(factory.prepare(&model, &json!({}))).unwrap();
    let (events, _) = flume::unbounded();
    let options = RequestOptions {
        thinking: ThinkingConfig::Off,
        fast: false,
    };
    let stream = || {
        smol::block_on(provider.stream_message(&model, &[], "", &json!([]), &events, options, None))
    };
    assert_eq!(
        stream().unwrap().message.first_text_content(),
        Some("denied")
    );
    host.unload(id).unwrap();
    host.load_source(id, &SOURCE.replace("ID", &format!("'{id}'")))
        .unwrap();
    assert!(
        stream().is_err(),
        "revoked provider remained usable after reload"
    );
}

#[test]
fn provider_stream_preserves_api_error() {
    let host = PluginHost::new(Arc::new(ToolRegistry::new())).unwrap();
    host.load_source("fixture", r#"
        maki.api.register_provider({
            id = 'fixture', models = {{ id = 'gpt-5.6-sol' }},
            prepare = function(_, model)
                return { model = model, stream = function()
                    return nil, { kind = 'api', message = 'rate limited', status = 429, retry_after_ms = 250 }
                end }
            end,
        })
    "#).unwrap();
    let snapshot = host.provider_registry().snapshot();
    let mut model = Model::from_spec("openai/gpt-5.6-sol").unwrap();
    model.provider = Arc::from("fixture");
    let (model, provider) = smol::block_on(
        snapshot
            .get("fixture")
            .unwrap()
            .factory
            .prepare(&model, &json!({})),
    )
    .unwrap();
    let (events, _received) = flume::unbounded();
    let result = smol::block_on(provider.stream_message(
        &model,
        &[],
        "",
        &json!([]),
        &events,
        RequestOptions {
            thinking: ThinkingConfig::Off,
            fast: false,
        },
        None,
    ));
    assert!(
        matches!(result, Err(AgentError::Api { status: 429, message, retry_after: Some(delay) }) if message == "rate limited" && delay == std::time::Duration::from_millis(250))
    );
}

#[test]
fn provider_send_round_trips_and_expires_with_operation() {
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::time::{Duration, Instant};

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    maki_lua::set_allowed_private_hosts(&[address.to_string()]);
    listener.set_nonblocking(true).unwrap();
    let server = std::thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut socket = loop {
            match listener.accept() {
                Ok((socket, _)) => break socket,
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    assert!(Instant::now() < deadline, "provider did not send request");
                    std::thread::yield_now();
                }
                Err(error) => panic!("accept failed: {error}"),
            }
        };
        socket
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let mut request = Vec::new();
        let mut buffer = [0; 1024];
        loop {
            let n = socket.read(&mut buffer).unwrap();
            assert_ne!(n, 0);
            request.extend_from_slice(&buffer[..n]);
            if request.windows(4).any(|part| part == b"\r\n\r\n") && request.ends_with(b"payload") {
                break;
            }
        }
        socket.write_all(b"HTTP/1.1 201 Created\r\nX-Fixture: yes\r\nContent-Length: 6\r\nConnection: close\r\n\r\nanswer").unwrap();
        String::from_utf8(request).unwrap()
    });

    let host = PluginHost::new(Arc::new(ToolRegistry::new())).unwrap();
    host.load_source("send_fixture", r#"
        local saved_ctx, saved_response
        maki.api.register_provider({
            id = 'send_fixture', models = {{ id = 'gpt-5.6-sol' }},
            prepare = function(prepare_ctx, model, config)
                return { model = model, stream = function(ctx)
                    if saved_ctx then
                        local ok, result = pcall(function()
                            return saved_ctx:send({ url = config.url })
                        end)
                        assert(not ok or not result, 'completed context still usable')
                        local read_ok, chunk = pcall(function() return saved_response:read() end)
                        assert(not read_ok or not chunk, 'completed response still readable')
                        local prepare_ok, prepared = pcall(function()
                            return prepare_ctx:send({ url = config.url })
                        end)
                        assert(not prepare_ok or not prepared, 'completed prepare context still usable')
                    else
                        local response, err = ctx:send({
                            url = config.url, method = 'POST',
                            headers = { ['x-fixture-request'] = 'present' }, body = 'payload',
                        })
                        assert(response, tostring(err))
                        assert(response.status == 201 and response.headers['x-fixture'] == 'yes')
                        local chunk, read_err = response:read()
                        assert(chunk and #chunk > 0, tostring(read_err))
                        saved_ctx, saved_response = ctx, response
                    end
                    return { message = { role = 'assistant', content = {{ type = 'text', text = 'ok' }} },
                        usage = { input = 0, output = 0, cache_creation = 0, cache_read = 0 }, stop_reason = 'end_turn' }
                end }
            end,
        })
    "#).unwrap();
    let snapshot = host.provider_registry().snapshot();
    let factory = &snapshot.get("send_fixture").unwrap().factory;
    let mut model = Model::from_spec("openai/gpt-5.6-sol").unwrap();
    model.provider = Arc::from("send_fixture");
    let (model, provider) = smol::block_on(factory.prepare(
        &model,
        &json!({ "url": format!("http://{address}/fixture") }),
    ))
    .unwrap();
    let (events, _) = flume::unbounded();
    for _ in 0..2 {
        let result = smol::block_on(provider.stream_message(
            &model,
            &[],
            "",
            &json!([]),
            &events,
            RequestOptions {
                thinking: ThinkingConfig::Off,
                fast: false,
            },
            None,
        ));
        assert!(result.is_ok(), "{result:?}");
    }
    let request = server.join().unwrap().to_ascii_lowercase();
    maki_lua::set_allowed_private_hosts(&[]);
    assert!(request.starts_with("post /fixture "));
    assert!(request.contains("x-fixture-request: present\r\n"));
    assert!(request.ends_with("payload"));
}

#[test]
fn dropped_provider_releases_lua_instance() {
    let host = PluginHost::new(Arc::new(ToolRegistry::new())).unwrap();
    host.load_source(
        "leak_fixture",
        r#"
        local instances = setmetatable({}, { __mode = 'v' })
        maki.api.register_provider({
            id = 'leak_fixture',
            models = { { id = 'gpt-5.6-sol' } },
            prepare = function(_, model, config)
                if config.check then
                    assert(instances[1] == nil, 'dropped provider instance is still retained')
                end
                local instance = { model = model, stream = function() end }
                instances[1] = instance
                return instance
            end,
        })
        "#,
    )
    .unwrap();
    let registry = host.provider_registry();
    let snapshot = registry.snapshot();
    let factory = &snapshot.get("leak_fixture").unwrap().factory;
    let model = Model::from_spec("openai/gpt-5.6-sol").unwrap();
    let (_, provider) = smol::block_on(factory.prepare(&model, &json!({}))).unwrap();
    drop(provider);
    host.wait_for_worker_barrier_for_test();
    host.collect_garbage_for_test().unwrap();
    smol::block_on(factory.prepare(&model, &json!({"check": true}))).unwrap();
}
