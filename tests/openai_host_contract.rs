#[path = "support/http_contract.rs"]
mod http_contract;

use std::sync::Arc;
use std::time::Duration;

use http_contract::{HttpContract, Response, Step};
use maki_agent::tools::ToolRegistry;
use maki_lua::PluginHost;
use maki_providers::{Message, Model, RequestOptions, ThinkingConfig};
use serde_json::json;

const TIMEOUT: Duration = Duration::from_secs(10);
const STREAM: &str = concat!(
    "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hello\"}}]}\n\n",
    "data: {\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":10,\"completion_tokens\":3}}\n\n",
    "data: [DONE]\n\n"
);

#[test]
fn openai_lua_host_discovery_and_stream_use_http_transport() {
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
