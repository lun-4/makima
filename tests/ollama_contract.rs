#[path = "support/http_contract.rs"]
mod http_contract;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use http_contract::{HttpContract, Response, Step};
use maki_agent::tools::ToolRegistry;
use maki_commands::CommandRegistry;
use maki_config::PluginsConfig;
use maki_lua::{PluginHost, test_support::InMemoryFs, ui_sink::HostUi};
use maki_providers::{Effort, Message, Model, ProviderEvent, RequestOptions, ThinkingConfig};
use maki_storage::{
    StateDir,
    auth::{ProviderCredentials, save_provider_credentials},
};
use serde_json::{Value, json};
use test_case::test_case;

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

#[test]
fn ollama_discovery_sends_model_names_and_extracts_fallback_details() {
    let _guard = TEST_LOCK.lock().unwrap();
    let response = |body: &str| Response {
        status: "200 OK".into(),
        headers: vec![],
        steps: vec![Step::Chunk(body.as_bytes().to_vec())],
    };
    let server = HttpContract::start(vec![
        response(r#"{"data":[{"id":"z"},{"id":"a"}]}"#),
        response(r#"{"model_info":{"context_length":2048},"capabilities":["completion"]}"#),
        response(r#"{"parameters":"temperature 0.7\nnum_ctx 8192\n","capabilities":["vision"]}"#),
    ])
    .unwrap();
    maki_lua::set_allowed_private_hosts(&[server.addr.to_string()]);
    let _private_host_guard = PrivateHostGuard;
    let host = PluginHost::new(Arc::new(ToolRegistry::new())).unwrap();
    host.load_source("ollama", include_str!("../plugins/ollama/init.lua"))
        .unwrap();
    let snapshot = host.provider_registry().snapshot();
    let models = smol::block_on(snapshot.get("ollama").unwrap().factory.discover(&json!({
        "base_url": format!("http://{}/", server.addr),
        "api_key_env": "OLLAMA_CONTRACT_UNUSED_KEY",
        "api_key": " first, second "
    })))
    .unwrap()
    .unwrap();
    assert_eq!(
        models
            .iter()
            .map(|model| model.id.as_str())
            .collect::<Vec<_>>(),
        ["a", "z"]
    );
    assert_eq!(models[0].context_window, Some(8192));
    assert_eq!(models[0].supports_vision, Some(true));
    assert_eq!(models[1].context_window, Some(2048));
    assert_eq!(models[1].supports_vision, Some(false));
    for (method, target, model) in [
        ("GET", "/v1/models", None),
        ("POST", "/api/show", Some("z")),
        ("POST", "/api/show", Some("a")),
    ] {
        let request = server
            .requests
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
            .unwrap();
        assert_eq!(request.method, method);
        assert_eq!(request.target, target);
        assert_eq!(request.header("authorization"), Some("Bearer first"));
        if let Some(model) = model {
            assert_eq!(request.json().unwrap(), json!({"model": model}));
        }
    }
}

#[test]
fn ollama_responses_protocol_rotates_credentials() {
    let _guard = TEST_LOCK.lock().unwrap();
    let server = HttpContract::start(vec![Response {
        status: "200 OK".into(),
        headers: vec![("Content-Type".into(), "text/event-stream".into())],
        steps: vec![Step::Chunk(b"event: response.output_text.delta\ndata: {\"delta\":\"hello\"}\n\nevent: response.completed\ndata: {\"response\":{\"status\":\"completed\"}}\n\n".to_vec())],
    }]).unwrap();
    maki_lua::set_allowed_private_hosts(&[server.addr.to_string()]);
    let _private_host_guard = PrivateHostGuard;
    let host = PluginHost::new(Arc::new(ToolRegistry::new())).unwrap();
    host.load_source("ollama", include_str!("../plugins/ollama/init.lua"))
        .unwrap();
    let snapshot = host.provider_registry().snapshot();
    let model = Model::from_spec("ollama/qwen3").unwrap();
    let (model, provider) = smol::block_on(snapshot.get("ollama").unwrap().factory.prepare(
        &model,
        &json!({
            "base_url": format!("http://{}", server.addr),
            "api_key_env": "OLLAMA_CONTRACT_UNUSED_KEY",
            "api_key": " first, second ",
            "protocol": "openai-responses"
        }),
    ))
    .unwrap();
    assert_eq!(provider.key_count(), 2);
    assert!(smol::block_on(provider.rotate_key()).unwrap());
    let (events, received) = flume::unbounded();
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
    assert!(
        matches!(received.try_recv(), Ok(ProviderEvent::TextDelta { text }) if text == "hello")
    );
    let request = server
        .requests
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    assert_eq!(request.method, "POST");
    assert_eq!(request.target, "/v1/responses");
    assert_eq!(request.header("authorization"), Some("Bearer second"));
    let body = request.json().unwrap();
    assert_eq!(body["model"], "qwen3");
    assert_eq!(body["return_progress"], true);
    assert_eq!(body["stream"], true);
}

#[test]
fn ollama_discovery_missing_data_returns_empty_catalog() {
    let _guard = TEST_LOCK.lock().unwrap();
    let server = HttpContract::start(vec![Response {
        status: "200 OK".into(),
        headers: vec![],
        steps: vec![Step::Chunk(b"{}".to_vec())],
    }])
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
    assert!(models.is_empty());
    let request = server
        .requests
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    assert_eq!(request.target, "/v1/models");
}

#[test_case(r#"{"model_info":"bad","parameters":"num_ctx 8192","capabilities":["vision"]}"# ; "invalid_model_info")]
#[test_case(r#"{"model_info":{"context_length":4096},"parameters":42,"capabilities":["vision"]}"# ; "invalid_parameters")]
#[test_case(r#"{"model_info":{"context_length":4096},"capabilities":"vision"}"# ; "invalid_capabilities")]
#[test_case(r#"{"model_info":{"context_length":4096},"capabilities":["vision",42]}"# ; "invalid_capability_element")]
fn ollama_discovery_ignores_invalid_show_fields(show: &str) {
    let _guard = TEST_LOCK.lock().unwrap();
    let response = |body: &str| Response {
        status: "200 OK".into(),
        headers: vec![],
        steps: vec![Step::Chunk(body.as_bytes().to_vec())],
    };
    let server = HttpContract::start(vec![
        response(r#"{"data":[{"id":"test"}]}"#),
        response(show),
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
    assert_eq!(models.len(), 1);
    assert_eq!(models[0].id, "test");
    assert_eq!(models[0].context_window, None);
    assert_eq!(models[0].supports_vision, None);
    let _ = server
        .requests
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    let request = server
        .requests
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    assert_eq!(request.target, "/api/show");
    assert_eq!(request.json().unwrap(), json!({"model": "test"}));
}

#[test_case(ThinkingConfig::Off, None ; "off")]
#[test_case(ThinkingConfig::Adaptive, Some("medium") ; "adaptive")]
#[test_case(ThinkingConfig::Effort(Effort::Low), Some("low") ; "low")]
#[test_case(ThinkingConfig::Effort(Effort::XHigh), Some("high") ; "xhigh")]
#[test_case(ThinkingConfig::Effort(Effort::Max), Some("high") ; "max")]
#[test_case(ThinkingConfig::Budget(3000), None ; "budget")]
fn ollama_responses_reasoning_modes(thinking: ThinkingConfig, effort: Option<&str>) {
    let _guard = TEST_LOCK.lock().unwrap();
    let server = HttpContract::start(vec![Response {
        status: "200 OK".into(),
        headers: vec![("Content-Type".into(), "text/event-stream".into())],
        steps: vec![Step::Chunk(b"event: response.output_text.delta\ndata: {\"delta\":\"hello\"}\n\nevent: response.completed\ndata: {\"response\":{\"status\":\"completed\"}}\n\n".to_vec())],
    }]).unwrap();
    maki_lua::set_allowed_private_hosts(&[server.addr.to_string()]);
    let _private_host_guard = PrivateHostGuard;
    let host = PluginHost::new(Arc::new(ToolRegistry::new())).unwrap();
    host.load_source("ollama", include_str!("../plugins/ollama/init.lua"))
        .unwrap();
    let snapshot = host.provider_registry().snapshot();
    let model = Model::from_spec("ollama/qwen3").unwrap();
    let (model, provider) = smol::block_on(snapshot.get("ollama").unwrap().factory.prepare(&model, &json!({
        "base_url": format!("http://{}", server.addr), "api_key_env": "OLLAMA_CONTRACT_UNUSED_KEY", "protocol": "openai-responses"
    }))).unwrap();
    let (events, _received) = flume::unbounded();
    let result = smol::block_on(provider.stream_message(
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
    assert_eq!(result.message.first_text_content(), Some("hello"));
    let request = server
        .requests
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    assert_eq!(request.target, "/v1/responses");
    let body = request.json().unwrap();
    assert_eq!(
        body.get("reasoning"),
        effort
            .map(|effort| json!({"effort": effort, "summary": "auto"}))
            .as_ref()
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
        response(r#"{"data":[{"id":"test"}]}"#), response("{}"),
        response("data: {\"choices\":[{\"delta\":{\"content\":\"hello\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n"),
    ]).unwrap();
    maki_lua::set_allowed_private_hosts(&[server.addr.to_string()]);
    let _private_host_guard = PrivateHostGuard;
    let temp = tempfile::tempdir().unwrap();
    let state = StateDir::from_path(temp.path().to_path_buf());
    save_provider_credentials(
        &state,
        "ollama",
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
        names: vec!["ollama".into()],
        ..Default::default()
    })
    .unwrap();
    let snapshot = host.provider_registry().snapshot();
    let factory = &snapshot.get("ollama").unwrap().factory;
    let config = json!({"api_key_env": "OLLAMA_CONTRACT_UNUSED_KEY", "base_url": format!("http://{}", server.addr)});
    let models = smol::block_on(factory.discover(&config)).unwrap().unwrap();
    assert_eq!(models[0].id, "test");
    let model = Model::from_spec("ollama/test").unwrap();
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
    for target in ["/v1/models", "/api/show", "/v1/chat/completions"] {
        let request = server
            .requests
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
            .unwrap();
        assert_eq!(request.target, target);
        assert_eq!(request.header("authorization"), Some("Bearer saved-key"));
    }
}
