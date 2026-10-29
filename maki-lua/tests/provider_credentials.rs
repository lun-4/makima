use std::sync::Arc;

use maki_agent::tools::ToolRegistry;
use maki_lua::PluginHost;
use maki_providers::{Model, RequestOptions, ThinkingConfig};
use serde_json::json;

#[test]
fn openai_legacy_credential_scopes_interoperate_and_preserve_other_kind() {
    use maki_storage::StateDir;
    use maki_storage::auth::{
        CredentialScope, LegacyCredentialKind, OAuthTokens, ProviderCredentials,
        load_provider_credentials, load_tokens, save_provider_credentials, save_tokens,
    };

    let temp = tempfile::tempdir().unwrap();
    let dir = StateDir::from_path(temp.path().to_path_buf());
    let oauth =
        CredentialScope::legacy(dir.clone(), "openai", LegacyCredentialKind::OAuth).unwrap();
    let key = CredentialScope::legacy(dir.clone(), "openai", LegacyCredentialKind::ApiKey).unwrap();
    save_tokens(
        &dir,
        "openai",
        &OAuthTokens {
            access: "old".into(),
            refresh: "refresh".into(),
            expires: 1234,
            account_id: Some("account".into()),
        },
    )
    .unwrap();
    assert_eq!(oauth.read().unwrap().unwrap()["access"], "old");
    assert!(key.read().unwrap().is_none());
    assert!(!smol::block_on(key.delete()).unwrap());
    assert!(smol::block_on(key.write(&json!({"api_key": "key"}))).is_err());
    assert_eq!(load_tokens(&dir, "openai").unwrap().access, "old");
    smol::block_on(oauth.write(
        &json!({"access":"new", "refresh":"rotated", "expires":4567, "account_id":"account"}),
    ))
    .unwrap();
    assert_eq!(load_tokens(&dir, "openai").unwrap().refresh, "rotated");
    assert!(smol::block_on(oauth.delete()).unwrap());
    save_provider_credentials(
        &dir,
        "openai",
        &ProviderCredentials {
            api_key: "saved-key".into(),
            host: None,
        },
    )
    .unwrap();
    assert_eq!(key.read().unwrap().unwrap()["api_key"], "saved-key");
    assert!(oauth.read().unwrap().is_none());
    assert!(!smol::block_on(oauth.delete()).unwrap());
    assert!(
        smol::block_on(
            oauth.write(&json!({"access":"other", "refresh":"refresh", "expires":1234}))
        )
        .is_err()
    );
    assert_eq!(
        load_provider_credentials(&dir, "openai").unwrap().api_key,
        "saved-key"
    );
}

#[test]
fn openai_oauth_provider_contract_through_host() {
    let host = PluginHost::new(Arc::new(ToolRegistry::new())).unwrap();
    host.load_source(
        "openai_contract",
        r#"
        local openai = require('maki.openai')
        local saved = { access = 'old', refresh = 'refresh', expires = 9999999999999, account_id = 'account' }
        local store = {
            read = function(_, key) assert(key == 'chatgpt'); return saved end,
            refresh = function(_, key, callback)
                assert(key == 'chatgpt')
                local updated, err = callback(saved)
                if err then return nil, err end
                saved = updated or saved
                return saved
            end,
        }
        local function context()
            return { credentials = store, send = function(_, request)
                local body
                if request.url:find('/models?', 1, true) then
                    assert(request.headers.Authorization == 'Bearer old')
                    assert(request.headers['chatgpt-account-id'] == 'account')
                    body = '{"models":[{"slug":"gpt-5.6-sol","visibility":"list","context_window":12345}]}'
                elseif request.url:find('/oauth/token', 1, true) then
                    assert(request.body:find('refresh_token=refresh', 1, true))
                    local payload = maki.base64.encode('{"chatgpt_account_id":"account"}'):gsub('=+$', '')
                    body = assert(maki.json.encode({ access_token = 'new', refresh_token = 'rotated',
                        id_token = 'header.' .. payload .. '.signature', expires_in = 3600 }))
                elseif request.url:find('/wham/usage', 1, true) then
                    assert(request.headers.Authorization == 'Bearer new')
                    body = '{"plan_type":"pro","rate_limit":{"primary_window":{"used_percent":20,"limit_window_seconds":18000}}}'
                else
                    assert(request.url == 'https://chatgpt.com/backend-api/codex/responses')
                    assert(request.headers.Authorization == 'Bearer old')
                    assert(request.headers['chatgpt-account-id'] == 'account')
                    body = 'event: response.output_text.delta\ndata: {"delta":"answer"}\n\nevent: response.completed\ndata: {"response":{"status":"completed","usage":{"input_tokens":8,"output_tokens":2}}}\n\n'
                end
                return { status = 200, read = function() local chunk = body; body = ''; return chunk end,
                    close = function() end }
            end }
        end
        maki.api.register_provider({
            id = 'openai_contract', models = {{ id = 'gpt-5.6-sol' }},
            discover = function(_, config) return openai.discover(context(), config) end,
            prepare = function(_, model, config)
                local provider, err = openai.prepare(context(), model, config)
                if not provider then return nil, err end
                local stream, refresh, usage = provider.stream, provider.refresh_auth, provider.fetch_usage
                provider.stream = function(_, request, emit) return stream(context(), request, emit) end
                provider.refresh_auth = function() return refresh(context()) end
                provider.fetch_usage = function() return usage(context()) end
                return provider
            end,
        })
        "#,
    )
    .unwrap();
    let snapshot = host.provider_registry().snapshot();
    let factory = &snapshot.get("openai_contract").unwrap().factory;
    let models = smol::block_on(factory.discover(&json!({})))
        .unwrap()
        .unwrap();
    assert_eq!(models[0].id, "gpt-5.6-sol");
    assert_eq!(models[0].context_window, Some(12345));
    let mut model = Model::from_spec("openai/gpt-5.6-sol").unwrap();
    model.provider = Arc::from("openai_contract");
    let (model, provider) = smol::block_on(factory.prepare(&model, &json!({}))).unwrap();
    let (events, receiver) = flume::unbounded();
    let response = smol::block_on(provider.stream_message(
        &model,
        &[],
        "system",
        &json!([]),
        &events,
        RequestOptions {
            thinking: ThinkingConfig::Off,
            fast: false,
        },
        None,
    ))
    .unwrap();
    assert_eq!(response.message.first_text_content(), Some("answer"));
    assert_eq!(response.usage.input, 8);
    assert!(!receiver.is_empty());
    smol::block_on(provider.refresh_auth()).unwrap();
    let usage = smol::block_on(provider.fetch_usage()).unwrap().unwrap();
    assert_eq!(usage.plan.as_deref(), Some("pro"));
    assert_eq!(usage.limits[0].percentage, Some(20));
    host.unload("openai_contract").unwrap();
    assert!(smol::block_on(provider.fetch_usage()).is_err());
}

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

#[test_case::test_case("synthetic", "synthetic" ; "spoofed_synthetic")]
#[test_case::test_case("requesty", "requesty" ; "spoofed_requesty")]
#[test_case::test_case("external", "providers" ; "spoofed_providers")]
#[test_case::test_case("external", "openai" ; "spoofed_openai")]
fn external_provider_cannot_access_credentials(owner: &str, id: &str) {
    const SOURCE: &str = r#"
        maki.api.register_provider({
            id = PROVIDER_ID, models = {{ id = 'gpt-5.6-sol' }},
            prepare = function(ctx, model)
                    local store = ctx.credentials
                    local callback_called = false
                    local operations = {
                        function() return store:read('token') end,
                        function() return store:read('chatgpt') end,
                        function() return store:read('api_key') end,
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
    if id == "providers" {
        let error = host.load_source(id, "return nil").unwrap_err();
        assert!(
            error
                .to_string()
                .contains("core providers plugin name is reserved")
        );
    }
    host.load_source(owner, &SOURCE.replace("PROVIDER_ID", &format!("'{id}'")))
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
