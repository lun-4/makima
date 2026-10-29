#[path = "support/http_contract.rs"]
mod http_contract;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use http_contract::{HttpContract, Response, Step};
use maki_agent::tools::ToolRegistry;
use maki_lua::PluginHost;
use maki_providers::{Message, Model, RequestOptions, ThinkingConfig};
use serde_json::json;

static PRIVATE_HOST_LOCK: Mutex<()> = Mutex::new(());

const TIMEOUT: Duration = Duration::from_secs(10);
const STREAM: &str = concat!(
    "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hello\"}}]}\n\n",
    "data: {\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":10,\"completion_tokens\":3}}\n\n",
    "data: [DONE]\n\n"
);

#[test]
fn openai_device_login_host_http_and_cancelled_challenge() {
    let _guard = PRIVATE_HOST_LOCK
        .lock()
        .unwrap_or_else(|error| error.into_inner());
    use maki_providers::AgentError;
    use maki_providers::provider::BoxFuture;
    use maki_providers::provider_registry::{
        InteractionRequest, InteractionResponse, InteractionService,
    };

    struct Interaction(bool);
    impl InteractionService for Interaction {
        fn interact(
            &self,
            request: InteractionRequest,
        ) -> BoxFuture<'_, Result<InteractionResponse, AgentError>> {
            assert!(
                matches!(request, InteractionRequest::DeviceChallenge { code, .. } if code == "user")
            );
            Box::pin(async move {
                Ok(if self.0 {
                    InteractionResponse::Acknowledged(true)
                } else {
                    InteractionResponse::Cancelled
                })
            })
        }
    }
    let response = |body: &str| Response {
        status: "200 OK".into(),
        headers: vec![],
        steps: vec![Step::Chunk(body.as_bytes().to_vec())],
    };
    let device = r#"{"device_auth_id":"device","user_code":"user","interval":"5"}"#;
    let server = HttpContract::start(vec![
        response(device),
        response(r#"{"authorization_code":"a+b","code_verifier":"x/y"}"#),
        response(r#"{"access_token":"access","refresh_token":"refresh","expires_in":3600}"#),
        response(device),
    ])
    .unwrap();
    maki_lua::set_allowed_private_hosts(&[server.addr.to_string()]);
    let host = PluginHost::new(Arc::new(ToolRegistry::new())).unwrap();
    let source = format!(
        r#"
        local auth = require('maki.openai_auth')
        local saved = nil
        maki.api.register_provider({{ id = 'device_host_contract',
          auth = {{{{ id = 'chatgpt', label = 'ChatGPT', login = function(ctx, ui)
            local proxy = {{ credentials = {{ write = function(_, key, tokens)
              assert(key == 'chatgpt'); saved = tokens; return true
            end }}, send = function(_, request)
              request.url = 'http://{address}' .. assert(request.url:match('^https://auth.openai.com(/.*)$'))
              return ctx:send(request)
            end }}
            return auth.login(proxy, ui)
          end }}}},
          auth_status = function() return {{ configured = saved ~= nil }} end,
          logout = function() saved = nil; return true end,
          prepare = function() error('unused') end,
        }})
    "#,
        address = server.addr
    );
    host.load_source("device_host_contract", &source).unwrap();
    let snapshot = host.provider_registry().snapshot();
    let factory = &snapshot.get("device_host_contract").unwrap().factory;
    assert_eq!(
        smol::block_on(factory.login_interactive("chatgpt", Arc::new(Interaction(true)))).unwrap(),
        json!(true)
    );
    assert_eq!(
        smol::block_on(factory.auth_status(&json!({}))).unwrap()["configured"],
        true
    );
    let start = server.requests.recv_timeout(TIMEOUT).unwrap().unwrap();
    let poll = server.requests.recv_timeout(TIMEOUT).unwrap().unwrap();
    let exchange = server.requests.recv_timeout(TIMEOUT).unwrap().unwrap();
    assert_eq!(start.target, "/api/accounts/deviceauth/usercode");
    assert_eq!(poll.target, "/api/accounts/deviceauth/token");
    assert_eq!(exchange.target, "/oauth/token");
    assert!(
        String::from_utf8(exchange.body)
            .unwrap()
            .contains("code=a%2Bb")
    );
    smol::block_on(factory.logout(&json!({}))).unwrap();
    assert!(
        smol::block_on(factory.login_interactive("chatgpt", Arc::new(Interaction(false)))).is_err()
    );
    assert_eq!(
        smol::block_on(factory.auth_status(&json!({}))).unwrap()["configured"],
        false
    );
    assert_eq!(
        server
            .requests
            .recv_timeout(TIMEOUT)
            .unwrap()
            .unwrap()
            .target,
        "/api/accounts/deviceauth/usercode"
    );
    maki_lua::set_allowed_private_hosts(&[]);
}

#[test]
fn openai_lua_host_discovery_and_stream_use_http_transport() {
    let _guard = PRIVATE_HOST_LOCK
        .lock()
        .unwrap_or_else(|error| error.into_inner());
    let response = |body: &str| Response {
        status: "200 OK".into(),
        headers: vec![],
        steps: vec![Step::Chunk(body.as_bytes().to_vec())],
    };
    let server = HttpContract::start(vec![
        response(r#"{"data":[{"id":"fixture-model","context_length":4096}]}"#),
        response(STREAM),
    ])
    .unwrap();
    maki_lua::set_allowed_private_hosts(&[server.addr.to_string()]);
    let host = PluginHost::new(Arc::new(ToolRegistry::new())).unwrap();
    host.load_source("openai_http_fixture", r#"
        local openai = require('maki.openai')
        maki.api.register_provider({ id = 'openai_http_fixture',
          discover = function(ctx, config)
            return openai.discover({send = function(_, request) return ctx:send(request) end}, config)
          end,
          prepare = function(ctx, model, config)
            local instance, err = openai.prepare(nil, model, config)
            if not instance then return nil, err end
            return instance
          end,
        })
    "#).unwrap();
    let snapshot = host.provider_registry().snapshot();
    let factory = &snapshot.get("openai_http_fixture").unwrap().factory;
    let config = json!({"api_key_env":"MAKI_OPENAI_HOST_CONTRACT_UNUSED", "api_key":"fixture-key", "base_url": format!("http://{}/v1", server.addr)});
    let models = smol::block_on(factory.discover(&config)).unwrap().unwrap();
    assert_eq!(models[0].id, "fixture-model");
    assert_eq!(models[0].context_window, Some(4096));
    let discovery = server.requests.recv_timeout(TIMEOUT).unwrap().unwrap();
    assert_eq!(discovery.target, "/v1/models");
    assert_eq!(
        discovery.header("authorization"),
        Some("Bearer fixture-key")
    );
    let mut model = Model::from_spec("openai/gpt-5.6-sol").unwrap();
    model.provider = Arc::from("openai_http_fixture");
    model.id = "fixture-model".into();
    let (model, provider) = smol::block_on(factory.prepare(&model, &config)).unwrap();
    let (events, received) = flume::unbounded();
    let result = smol::block_on(provider.stream_message(
        &model,
        &[Message::user("hello".into())],
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
    assert_eq!(result.message.first_text_content(), Some("hello"));
    assert!(!received.is_empty());
    let request = server.requests.recv_timeout(TIMEOUT).unwrap().unwrap();
    assert_eq!(request.target, "/v1/chat/completions");
    assert_eq!(request.header("authorization"), Some("Bearer fixture-key"));
    assert_eq!(request.json().unwrap()["model"], "fixture-model");
    maki_lua::set_allowed_private_hosts(&[]);
}
