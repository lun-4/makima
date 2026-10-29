#[path = "support/http_contract.rs"]
mod http_contract;

use std::sync::Arc;
use std::time::Duration;

use http_contract::{HttpContract, Response, Step};
use maki_agent::tools::ToolRegistry;
use maki_lua::PluginHost;
use maki_providers::{Message, Model, ProviderEvent, RequestOptions, ThinkingConfig};
use serde_json::{Value, json};

#[test]
fn deepseek_lua_stream_request_and_response() {
    let server = HttpContract::start(vec![Response {
        status: "200 OK".into(),
        headers: vec![("Content-Type".into(), "text/event-stream".into())],
        steps: vec![Step::Chunk(concat!(
            "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hello\"}}]}\n\n",
            "data: {\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":10,\"completion_tokens\":3}}\n\n",
            "data: [DONE]\n\n"
        ).as_bytes().to_vec())],
    }]).unwrap();
    maki_lua::set_allowed_private_hosts(&[server.addr.to_string()]);
    let host = PluginHost::new(Arc::new(ToolRegistry::new())).unwrap();
    host.load_source("deepseek", include_str!("../plugins/deepseek/init.lua"))
        .unwrap();
    let snapshot = host.provider_registry().snapshot();
    let registration = snapshot.get("deepseek").unwrap();
    let model = Model::from_spec("deepseek/deepseek-v4-flash").unwrap();
    let (model, provider) = smol::block_on(registration.factory.prepare(
        &model, &json!({"api_key_env": "MAKI_DEEPSEEK_TEST_MISSING_KEY", "api_key": "fixture-key", "base_url": format!("http://{}", server.addr)}),
    )).unwrap();
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
    assert!(
        matches!(received.try_recv(), Ok(ProviderEvent::TextDelta { text }) if text == "hello")
    );
    let request = server
        .requests
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    let body: Value = serde_json::from_slice(&request.body).unwrap();
    assert_eq!(body["thinking"]["type"], "disabled");
    assert_eq!(body["stream_options"]["include_usage"], true);
    assert_eq!(body["model"], "deepseek-v4-flash");
}
