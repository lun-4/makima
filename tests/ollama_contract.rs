#[path = "support/http_contract.rs"]
mod http_contract;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use http_contract::{HttpContract, Response, Step};
use maki_agent::tools::ToolRegistry;
use maki_lua::PluginHost;
use maki_providers::{Message, Model, ProviderEvent, RequestOptions, ThinkingConfig};
use serde_json::{Value, json};

static TEST_LOCK: Mutex<()> = Mutex::new(());

struct PrivateHostGuard;

impl Drop for PrivateHostGuard {
    fn drop(&mut self) {
        maki_lua::set_allowed_private_hosts(&[]);
    }
}

#[test]
fn ollama_registers_from_plugin_source() {
    let host = PluginHost::new(Arc::new(ToolRegistry::new())).unwrap();
    host.load_source("ollama", include_str!("../plugins/ollama/init.lua"))
        .unwrap();
    assert!(host.provider_registry().snapshot().get("ollama").is_some());
}

#[test]
fn ollama_discovery_reads_details_and_tolerates_show_failure() {
    let _guard = TEST_LOCK.lock().unwrap();
    let response = |status: &str, body: &str| Response {
        status: status.into(),
        headers: vec![],
        steps: vec![Step::Chunk(body.as_bytes().to_vec())],
    };
    let server = HttpContract::start(vec![
        response("200 OK", r#"{"data":[{"id":"z"},{"id":"a"}]}"#),
        response(
            "200 OK",
            r#"{"model_info":{"qwen.context_length":32768},"capabilities":["vision"]}"#,
        ),
        response("404 Not Found", "missing"),
    ])
    .unwrap();
    maki_lua::set_allowed_private_hosts(&[server.addr.to_string()]);
    let _private_host_guard = PrivateHostGuard;
    let host = PluginHost::new(Arc::new(ToolRegistry::new())).unwrap();
    host.load_source("ollama", include_str!("../plugins/ollama/init.lua"))
        .unwrap();
    let snapshot = host.provider_registry().snapshot();
    let models = smol::block_on(snapshot.get("ollama").unwrap().factory.discover(&json!({
        "base_url": format!("http://{}", server.addr), "api_key_env": "OLLAMA_CONTRACT_UNUSED_KEY"
    })))
    .unwrap()
    .unwrap();
    assert_eq!(models[0].id, "a");
    assert_eq!(models[0].supports_vision, None);
    assert_eq!(models[1].context_window, Some(32768));
    assert_eq!(models[1].supports_vision, Some(true));
    for target in ["/v1/models", "/api/show", "/api/show"] {
        let request = server
            .requests
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
            .unwrap();
        assert_eq!(request.target, target);
    }
}

#[test]
fn ollama_http_errors_preserve_response_body() {
    let _guard = TEST_LOCK.lock().unwrap();
    const ERROR_BODY: &str = r#"{"error":"model unavailable"}"#;
    let response = || Response {
        status: "503 Service Unavailable".into(),
        headers: vec![],
        steps: vec![Step::Chunk(ERROR_BODY.as_bytes().to_vec())],
    };
    let server = HttpContract::start(vec![response(), response()]).unwrap();
    maki_lua::set_allowed_private_hosts(&[server.addr.to_string()]);
    let _private_host_guard = PrivateHostGuard;
    let host = PluginHost::new(Arc::new(ToolRegistry::new())).unwrap();
    host.load_source("ollama", include_str!("../plugins/ollama/init.lua"))
        .unwrap();
    let snapshot = host.provider_registry().snapshot();
    let registration = snapshot.get("ollama").unwrap();
    let config = json!({"base_url": format!("http://{}", server.addr), "api_key_env": "OLLAMA_CONTRACT_UNUSED_KEY"});
    let error = smol::block_on(registration.factory.discover(&config)).unwrap_err();
    assert!(
        matches!(error, maki_providers::AgentError::Api { status: 503, ref message, .. } if message == ERROR_BODY)
    );
    let model = Model::from_spec("ollama/qwen3").unwrap();
    let (model, provider) = smol::block_on(registration.factory.prepare(&model, &config)).unwrap();
    let (events, _receiver) = flume::unbounded();
    let error = smol::block_on(provider.stream_message(
        &model,
        &[Message::user("prompt".into())],
        "system",
        &json!([]),
        &events,
        RequestOptions {
            thinking: ThinkingConfig::Off,
            fast: false,
        },
        None,
    ))
    .unwrap_err();
    assert!(
        matches!(error, maki_providers::AgentError::Api { status: 503, ref message, .. } if message == ERROR_BODY)
    );
}

#[test]
fn ollama_registration_streams_without_credentials() {
    let _guard = TEST_LOCK.lock().unwrap();
    let server = HttpContract::start(vec![Response {
        status: "200 OK".into(),
        headers: vec![("Content-Type".into(), "text/event-stream".into())],
        steps: vec![Step::Chunk(b"data: {\"choices\":[{\"delta\":{\"content\":\"hello\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n".to_vec())],
    }]).unwrap();
    maki_lua::set_allowed_private_hosts(&[server.addr.to_string()]);
    let _private_host_guard = PrivateHostGuard;
    let host = PluginHost::new(Arc::new(ToolRegistry::new())).unwrap();
    host.load_source("ollama", include_str!("../plugins/ollama/init.lua"))
        .unwrap();
    let snapshot = host.provider_registry().snapshot();
    let registration = snapshot.get("ollama").unwrap();
    let model = Model::from_spec("ollama/qwen3").unwrap();
    let (model, provider) = smol::block_on(registration.factory.prepare(
        &model,
        &json!({"base_url": format!("http://{}", server.addr), "api_key_env": "OLLAMA_CONTRACT_UNUSED_KEY"}),
    )).unwrap();
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
    assert!(request.header("authorization").is_none());
    let body: Value = serde_json::from_slice(&request.body).unwrap();
    assert_eq!(body["stream_options"]["include_usage"], true);
    assert!(body.get("thinking").is_none());
}
