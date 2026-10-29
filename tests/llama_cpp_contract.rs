#[path = "support/http_contract.rs"]
mod http_contract;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use http_contract::{HttpContract, Response, Step};
use maki_agent::tools::ToolRegistry;
use maki_commands::CommandRegistry;
use maki_config::PluginsConfig;
use maki_lua::{PluginHost, test_support::InMemoryFs, ui_sink::HostUi};
use maki_providers::{Effort, Message, Model, RequestOptions, ThinkingConfig};
use maki_storage::{
    StateDir,
    auth::{ProviderCredentials, save_provider_credentials},
};
use serde_json::{Value, json};
use test_case::test_case;

static TEST_LOCK: Mutex<()> = Mutex::new(());

#[test_case(r#"{"role":"router","modalities":{"vision":true}}"#, r#"{"data":[{"id":"z","status":{"args":["--fit-ctx","4096"]}},{"id":"a","meta":{"n_ctx":8192},"architecture":{"input_modalities":["text","image"],"output_modalities":["text"]}},{"id":"excluded","architecture":{"input_modalities":["image"],"output_modalities":["text"]}}]}"#, vec![("a", 8192, Some(true)), ("z", 4096, None)] ; "router")]
#[test_case(r#"{"n_ctx":9999,"modalities":{"vision":false}}"#, r#"{"data":[{"id":"single","meta":{"n_ctx":2048}}]}"#, vec![("single", 2048, Some(false))] ; "single")]
#[test_case(r#"{"n_ctx":6000,"modalities":{"vision":true}}"#, r#"{"data":[{"id":"legacy","max_model_len":0}]}"#, vec![("legacy", 6000, Some(true))] ; "legacy")]
fn discovery_modes_reach_http(props: &str, models: &str, expected: Vec<(&str, u32, Option<bool>)>) {
    let _guard = TEST_LOCK.lock().unwrap();
    let response = |body: &str| Response {
        status: "200 OK".into(),
        headers: vec![],
        steps: vec![Step::Chunk(body.as_bytes().to_vec())],
    };
    let server = HttpContract::start(vec![response(props), response(models)]).unwrap();
    maki_lua::set_allowed_private_hosts(&[server.addr.to_string()]);
    let _private_host_guard = PrivateHostGuard;
    let host = PluginHost::new(Arc::new(ToolRegistry::new())).unwrap();
    host.load_source("llama-cpp", include_str!("../plugins/llama-cpp/init.lua"))
        .unwrap();
    let snapshot = host.provider_registry().snapshot();
    let config = json!({"base_url": format!("http://{}", server.addr), "api_key_env": "LLAMACPP_CONTRACT_UNUSED_KEY", "api_key": "test-key"});
    let found = smol::block_on(snapshot.get("llama-cpp").unwrap().factory.discover(&config))
        .unwrap()
        .unwrap();
    assert_eq!(found.len(), expected.len());
    for (model, (id, context, vision)) in found.iter().zip(expected) {
        assert_eq!(model.id, id);
        assert_eq!(model.context_window, Some(context));
        assert_eq!(model.supports_vision, vision);
    }
    for target in ["/props?autoload=false", "/v1/models"] {
        let request = server
            .requests
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
            .unwrap();
        assert_eq!(request.target, target);
        assert_eq!(request.header("authorization"), Some("Bearer test-key"));
    }
}

#[test]
fn discovery_http_error_preserves_body() {
    let _guard = TEST_LOCK.lock().unwrap();
    const ERROR_BODY: &str = "discovery unavailable";
    let server = HttpContract::start(vec![Response {
        status: "503 Service Unavailable".into(),
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
    let error = smol::block_on(snapshot.get("llama-cpp").unwrap().factory.discover(&json!({"base_url": format!("http://{}", server.addr), "api_key_env": "LLAMACPP_CONTRACT_UNUSED_KEY"}))).unwrap_err();
    assert!(
        matches!(error, maki_providers::AgentError::Api { status: 503, ref message, .. } if message == ERROR_BODY)
    );
}

#[test]
fn responses_protocol_streams_progress() {
    let _guard = TEST_LOCK.lock().unwrap();
    let server = HttpContract::start(vec![Response { status: "200 OK".into(), headers: vec![], steps: vec![Step::Chunk(b"event: response.output_text.delta\ndata: {\"delta\":\"hello\"}\n\nevent: response.completed\ndata: {\"response\":{\"status\":\"completed\"}}\n\n".to_vec())] }]).unwrap();
    maki_lua::set_allowed_private_hosts(&[server.addr.to_string()]);
    let _private_host_guard = PrivateHostGuard;
    let host = PluginHost::new(Arc::new(ToolRegistry::new())).unwrap();
    host.load_source("llama-cpp", include_str!("../plugins/llama-cpp/init.lua"))
        .unwrap();
    let snapshot = host.provider_registry().snapshot();
    let model = Model::from_spec("llama-cpp/test").unwrap();
    let (model, provider) = smol::block_on(snapshot.get("llama-cpp").unwrap().factory.prepare(&model, &json!({"base_url": format!("http://{}", server.addr), "api_key_env": "LLAMACPP_CONTRACT_UNUSED_KEY", "protocol": "openai-responses"}))).unwrap();
    let (events, receiver) = flume::unbounded();
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
    assert!(receiver.try_recv().is_ok());
    let request = server
        .requests
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    assert_eq!(request.target, "/v1/responses");
    let body: Value = serde_json::from_slice(&request.body).unwrap();
    assert_eq!(body["return_progress"], true);
    assert_eq!(body["reasoning"]["summary"], "auto");
    assert!(body.get("thinking_budget_tokens").is_none());
}

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

#[test_case(ThinkingConfig::Off, json!({"off":{"reasoning_effort":"none"}}), json!({"reasoning_effort":"none"}) ; "off_fragment")]
#[test_case(ThinkingConfig::Adaptive, json!({"adaptive":{"reasoning_effort":"auto"}}), json!({"reasoning_effort":"auto"}) ; "adaptive_fragment")]
#[test_case(ThinkingConfig::Effort(Effort::High), json!({"low":{"reasoning_effort":"low"},"xhigh":{"reasoning_effort":"xhigh"}}), json!({"reasoning_effort":"low"}) ; "effort_snap")]
#[test_case(ThinkingConfig::Budget(3000), json!({"adaptive":{"reasoning_effort":"auto"}}), json!({"reasoning_effort":"auto","thinking_budget_tokens":3000}) ; "budget_with_adaptive")]
#[test_case(ThinkingConfig::Budget(3000), json!({"low":{"reasoning_effort":"low"},"xhigh":{"reasoning_effort":"xhigh"}}), json!({"reasoning_effort":"xhigh"}) ; "budget_snaps")]
#[test_case(ThinkingConfig::Adaptive, json!({"adaptive":{"stream_options":{"custom":true}}}), json!({"stream_options":{"include_usage":true,"custom":true}}) ; "nested_merge")]
#[test_case(ThinkingConfig::Off, json!({"adaptive":{"reasoning_effort":"auto"}}), json!({"thinking_budget_tokens":0}) ; "missing_fragment_fallback")]
fn llama_cpp_thinking_fields_reach_http(thinking: ThinkingConfig, fields: Value, expected: Value) {
    let _guard = TEST_LOCK.lock().unwrap();
    let server = HttpContract::start(vec![Response {
        status: "200 OK".into(),
        headers: vec![],
        steps: vec![Step::Chunk(b"data: {\"choices\":[{\"delta\":{\"content\":\"hello\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n".to_vec())],
    }]).unwrap();
    maki_lua::set_allowed_private_hosts(&[server.addr.to_string()]);
    let _private_host_guard = PrivateHostGuard;
    let host = PluginHost::new(Arc::new(ToolRegistry::new())).unwrap();
    host.load_source("llama-cpp", include_str!("../plugins/llama-cpp/init.lua"))
        .unwrap();
    let snapshot = host.provider_registry().snapshot();
    let mut model = Model::from_spec("llama-cpp/test").unwrap();
    model.max_output_tokens = Some(8192);
    model.thinking_fields = Some(Box::new(serde_json::from_value(fields).unwrap()));
    let (model, provider) = smol::block_on(snapshot.get("llama-cpp").unwrap().factory.prepare(&model, &json!({
        "base_url": format!("http://{}", server.addr), "api_key_env": "LLAMACPP_CONTRACT_UNUSED_KEY"
    }))).unwrap();
    let (events, _receiver) = flume::unbounded();
    smol::block_on(provider.stream_message(
        &model,
        &[Message::user("prompt".into())],
        "system",
        &json!([]),
        &events,
        RequestOptions {
            thinking,
            fast: false,
        },
        None,
    ))
    .unwrap();
    let request = server
        .requests
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    let mut body: Value = serde_json::from_slice(&request.body).unwrap();
    for key in ["model", "messages", "stream", "max_tokens", "tools"] {
        body.as_object_mut().unwrap().remove(key);
    }
    if expected.get("stream_options").is_none() {
        assert_eq!(body["stream_options"]["include_usage"], true);
        body.as_object_mut().unwrap().remove("stream_options");
    }
    assert_eq!(body, expected);
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

#[test]
fn saved_credentials_are_used_for_discovery_and_streaming() {
    let _guard = TEST_LOCK.lock().unwrap();
    let response = |body: &str| Response {
        status: "200 OK".into(),
        headers: vec![],
        steps: vec![Step::Chunk(body.as_bytes().to_vec())],
    };
    let server = HttpContract::start(vec![
        response("{}"), response(r#"{"data":[{"id":"test"}]}"#),
        response("data: {\"choices\":[{\"delta\":{\"content\":\"hello\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n"),
    ]).unwrap();
    maki_lua::set_allowed_private_hosts(&[server.addr.to_string()]);
    let _private_host_guard = PrivateHostGuard;
    let temp = tempfile::tempdir().unwrap();
    let state = StateDir::from_path(temp.path().to_path_buf());
    save_provider_credentials(
        &state,
        "llama-cpp",
        &ProviderCredentials {
            api_key: "saved-key".into(),
            host: None,
        },
    )
    .unwrap();
    let mut host = PluginHost::with_session_provider_preparer(
        Arc::new(ToolRegistry::new()),
        CommandRegistry::new(),
        true,
        Arc::new(InMemoryFs::new()),
        Some(temp.path().to_path_buf()),
        None,
        HostUi::Frontend,
    )
    .unwrap();
    host.load_builtins(&PluginsConfig {
        enabled: true,
        names: vec!["llama-cpp".into()],
        ..Default::default()
    })
    .unwrap();
    let snapshot = host.provider_registry().snapshot();
    let factory = &snapshot.get("llama-cpp").unwrap().factory;
    let config = json!({"api_key_env": "LLAMACPP_CONTRACT_UNUSED_KEY", "base_url": format!("http://{}", server.addr)});
    let models = smol::block_on(factory.discover(&config)).unwrap().unwrap();
    assert_eq!(models[0].id, "test");
    let model = Model::from_spec("llama-cpp/test").unwrap();
    let (model, provider) = smol::block_on(factory.prepare(&model, &config)).unwrap();
    let (events, _receiver) = flume::unbounded();
    let result = smol::block_on(provider.stream_message(
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
    .unwrap();
    assert_eq!(result.message.first_text_content(), Some("hello"));
    for target in [
        "/props?autoload=false",
        "/v1/models",
        "/v1/chat/completions",
    ] {
        let request = server
            .requests
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
            .unwrap();
        assert_eq!(request.target, target);
        assert_eq!(request.header("authorization"), Some("Bearer saved-key"));
    }
}
