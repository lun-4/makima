#[path = "support/http_contract.rs"]
mod http_contract;

use std::sync::Arc;
use std::time::Duration;

use http_contract::{HttpContract, Response, Step};
use maki_agent::tools::ToolRegistry;
use maki_config::PluginsConfig;
use maki_lua::PluginHost;
use maki_providers::{Message, Model, ProviderEvent, RequestOptions, ThinkingConfig};
use serde_json::{Value, json};

#[test]
fn tensorx_is_embedded_but_not_loaded_by_default() {
    let config = PluginsConfig::from_plugins(Default::default());
    assert!(!config.names.iter().any(|name| name == "tensorx"));
    let mut host = PluginHost::new(Arc::new(ToolRegistry::new())).unwrap();
    host.load_builtins(&config).unwrap();
    assert!(host.provider_registry().snapshot().get("tensorx").is_none());

    let explicit = PluginsConfig {
        enabled: true,
        names: vec!["tensorx".into()],
        ..Default::default()
    };
    host.load_builtins(&explicit).unwrap();
    assert!(host.provider_registry().snapshot().get("tensorx").is_some());
}

#[test]
fn tensorx_registration_streams_deepseek_template() {
    let server = HttpContract::start(vec![Response {
        status: "200 OK".into(),
        headers: vec![("Content-Type".into(), "text/event-stream".into())],
        steps: vec![Step::Chunk(b"data: {\"choices\":[{\"delta\":{\"content\":\"hello\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n".to_vec())],
    }]).unwrap();
    maki_lua::set_allowed_private_hosts(&[server.addr.to_string()]);
    let host = PluginHost::new(Arc::new(ToolRegistry::new())).unwrap();
    host.load_source("tensorx", include_str!("../plugins/tensorx/init.lua"))
        .unwrap();
    let snapshot = host.provider_registry().snapshot();
    let registration = snapshot.get("tensorx").unwrap();
    let model = Model::from_spec("tensorx/deepseek/deepseek-flash").unwrap();
    let (model, provider) = smol::block_on(registration.factory.prepare(
        &model,
        &json!({
            "api_key": "fixture-key", "base_url": format!("http://{}/v1", server.addr),
            "api_key_env": "TENSORX_CONTRACT_UNUSED_KEY"
        }),
    ))
    .unwrap();
    let (events, received) = flume::unbounded();
    let result = smol::block_on(provider.stream_message(
        &model,
        &[Message::user("prompt".into())],
        "system",
        &json!([]),
        &events,
        RequestOptions {
            thinking: ThinkingConfig::Adaptive,
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
    assert_eq!(request.target, "/v1/chat/completions");
    assert_eq!(request.header("authorization"), Some("Bearer fixture-key"));
    let body: Value = serde_json::from_slice(&request.body).unwrap();
    assert_eq!(body["chat_template_kwargs"]["thinking"], true);
    assert_eq!(body["stream_options"]["include_usage"], true);
    assert!(body.get("max_tokens").is_none());
}
