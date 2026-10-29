#[path = "support/http_contract.rs"]
mod http_contract;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use http_contract::{HttpContract, Response, Step};
use maki_agent::tools::ToolRegistry;
use maki_lua::PluginHost;
use maki_providers::{Message, Model, RequestOptions, ThinkingConfig};
use serde_json::{Value, json};

static TEST_LOCK: Mutex<()> = Mutex::new(());

struct PrivateHostGuard;

impl Drop for PrivateHostGuard {
    fn drop(&mut self) {
        maki_lua::set_allowed_private_hosts(&[]);
    }
}

#[test]
fn llama_cpp_registration_streams_without_credentials() {
    let _guard = TEST_LOCK.lock().unwrap();
    let server = HttpContract::start(vec![Response {
        status: "200 OK".into(),
        headers: vec![("Content-Type".into(), "text/event-stream".into())],
        steps: vec![Step::Chunk(b"data: {\"choices\":[{\"delta\":{\"content\":\"hello\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n".to_vec())],
    }]).unwrap();
    maki_lua::set_allowed_private_hosts(&[server.addr.to_string()]);
    let _private_host_guard = PrivateHostGuard;
    let host = PluginHost::new(Arc::new(ToolRegistry::new())).unwrap();
    host.load_source("llama-cpp", include_str!("../plugins/llama-cpp/init.lua"))
        .unwrap();
    let snapshot = host.provider_registry().snapshot();
    let registration = snapshot.get("llama-cpp").unwrap();
    let model = Model::from_spec("llama-cpp/test").unwrap();
    let (model, provider) = smol::block_on(registration.factory.prepare(&model, &json!({
        "base_url": format!("http://{}", server.addr), "api_key_env": "LLAMACPP_CONTRACT_UNUSED_KEY"
    }))).unwrap();
    let (events, event_rx) = flume::unbounded();
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
    assert!(event_rx.try_recv().is_ok());
    let request = server
        .requests
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    assert_eq!(request.target, "/v1/chat/completions");
    assert!(request.header("authorization").is_none());
    let body: Value = serde_json::from_slice(&request.body).unwrap();
    assert_eq!(body["thinking_budget_tokens"], -1);
    assert_eq!(body["stream_options"]["include_usage"], true);
}

#[test]
fn llama_cpp_http_errors_preserve_response_body() {
    let _guard = TEST_LOCK.lock().unwrap();
    const ERROR_BODY: &str = "request exceeds the available context size";
    let server = HttpContract::start(vec![Response {
        status: "400 Bad Request".into(),
        headers: vec![],
        steps: vec![Step::Chunk(ERROR_BODY.as_bytes().to_vec())],
    }])
    .unwrap();
    maki_lua::set_allowed_private_hosts(&[server.addr.to_string()]);
    let _private_host_guard = PrivateHostGuard;
    let host = PluginHost::new(Arc::new(ToolRegistry::new())).unwrap();
    host.load_source("llama-cpp", include_str!("../plugins/llama-cpp/init.lua"))
        .unwrap();
    let snapshot = host.provider_registry().snapshot();
    let registration = snapshot.get("llama-cpp").unwrap();
    let model = Model::from_spec("llama-cpp/test").unwrap();
    let (model, provider) = smol::block_on(registration.factory.prepare(&model, &json!({
        "base_url": format!("http://{}", server.addr), "api_key_env": "LLAMACPP_CONTRACT_UNUSED_KEY"
    }))).unwrap();
    let (events, _event_rx) = flume::unbounded();
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
        matches!(error, maki_providers::AgentError::Api { status: 400, ref message, .. } if message == ERROR_BODY)
    );
}
