use std::sync::Arc;

use maki_agent::tools::ToolRegistry;
use maki_lua::PluginHost;
use maki_providers::{Model, RequestOptions, ThinkingConfig};
use serde_json::json;

#[test]
fn provider_auth_hooks_are_isolated_per_prepared_instance() {
    let host = PluginHost::new(Arc::new(ToolRegistry::new())).unwrap();
    host.load_source("fixture", r#"
        maki.api.register_provider({
            id = 'fixture', models = {{ id = 'gpt-5.6-sol' }},
            prepare = function(_, model, config)
                local state = config.marker
                return {
                    model = model, stream = function() end,
                    fetch_usage = function() return { plan = state, limits = {}, by_model_today = {} } end,
                    refresh_auth = function() state = state .. '-refresh'; return true end,
                    reload_auth = function() state = state .. '-reload'; return true end,
                }
            end,
        })
    "#).unwrap();
    let snapshot = host.provider_registry().snapshot();
    let factory = &snapshot.get("fixture").unwrap().factory;
    let mut model = Model::from_spec("openai/gpt-5.6-sol").unwrap();
    model.provider = Arc::from("fixture");
    let (_, first) = smol::block_on(factory.prepare(&model, &json!({"marker": "first"}))).unwrap();
    let (_, second) =
        smol::block_on(factory.prepare(&model, &json!({"marker": "second"}))).unwrap();

    smol::block_on(first.refresh_auth()).unwrap();
    smol::block_on(second.reload_auth()).unwrap();
    assert_eq!(
        smol::block_on(first.fetch_usage())
            .unwrap()
            .unwrap()
            .plan
            .as_deref(),
        Some("first-refresh")
    );
    assert_eq!(
        smol::block_on(second.fetch_usage())
            .unwrap()
            .unwrap()
            .plan
            .as_deref(),
        Some("second-reload")
    );
}

#[test_case::test_case("synthetic" ; "spoofed_synthetic")]
#[test_case::test_case("providers" ; "spoofed_providers")]
fn external_provider_cannot_access_credentials(id: &str) {
    const SOURCE: &str = r#"
        maki.api.register_provider({
            id = PROVIDER_ID, models = {{ id = 'gpt-5.6-sol' }},
            prepare = function(ctx, model)
                    local store = ctx.credentials
                    local callback_called = false
                    local operations = {
                        function() return store:read('token') end,
                        function() return store:write('token', { secret = 'fixture-secret' }) end,
                        function() return store:delete('token') end,
                        function() return store:refresh('token', function()
                            callback_called = true
                            return { secret = 'fixture-secret' }
                        end) end,
                    }
                    for _, operation in ipairs(operations) do
                        local value, err = operation()
                        assert(value == nil and err and err.kind == 'config')
                        assert(err.message == 'credential access denied')
                    end
                    assert(not callback_called)
                    return { model = model, stream = function()
                        return { message = { role = 'assistant', content = {{ type = 'text', text = 'denied' }} },
                            usage = { input = 0, output = 0, cache_creation = 0, cache_read = 0 }, stop_reason = 'end_turn' }
                    end }
            end,
        })
    "#;
    let host = PluginHost::new(Arc::new(ToolRegistry::new())).unwrap();
    host.load_source(id, &SOURCE.replace("PROVIDER_ID", &format!("'{id}'")))
        .unwrap();
    let snapshot = host.provider_registry().snapshot();
    let mut model = Model::from_spec("openai/gpt-5.6-sol").unwrap();
    model.provider = Arc::from(id);
    let (model, provider) = smol::block_on(
        snapshot
            .get(id)
            .unwrap()
            .factory
            .prepare(&model, &json!({})),
    )
    .unwrap();
    let (events, _) = flume::unbounded();
    let options = RequestOptions {
        thinking: ThinkingConfig::Off,
        fast: false,
    };
    let response = smol::block_on(provider.stream_message(
        &model,
        &[],
        "",
        &json!([]),
        &events,
        options,
        None,
    ))
    .unwrap();
    assert_eq!(response.message.first_text_content(), Some("denied"));
}
