#[path = "support/http_contract.rs"]
mod http_contract;

use std::sync::Arc;
use std::time::Duration;

use http_contract::{HttpContract, Response, Step};
use maki_agent::tools::ToolRegistry;
use maki_commands::CommandRegistry;
use maki_config::PluginsConfig;
use maki_lua::{PluginHost, test_support::InMemoryFs, ui_sink::HostUi};
use maki_providers::{Message, Model, ProviderEvent, RequestOptions, ThinkingConfig};
use maki_storage::{
    StateDir,
    auth::{ProviderCredentials, load_provider_credentials, save_provider_credentials},
};
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
fn tensorx_legacy_credentials_are_used_for_stream_and_logout() {
    let stream = b"data: {\"choices\":[{\"delta\":{\"content\":\"hello\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n";
    let server = HttpContract::start(
        (0..2)
            .map(|_| Response {
                status: "200 OK".into(),
                headers: vec![],
                steps: vec![Step::Chunk(stream.to_vec())],
            })
            .collect(),
    )
    .unwrap();
    maki_lua::set_allowed_private_hosts(&[server.addr.to_string()]);
    let temp = tempfile::tempdir().unwrap();
    let state = StateDir::from_path(temp.path().to_path_buf());
    save_provider_credentials(
        &state,
        "tensorx",
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
        names: vec!["tensorx".into()],
        ..Default::default()
    })
    .unwrap();
    let snapshot = host.provider_registry().snapshot();
    let factory = &snapshot.get("tensorx").unwrap().factory;
    let config = json!({"api_key_env": "TENSORX_CONTRACT_UNUSED_KEY", "api_key": "config-key", "base_url": format!("http://{}", server.addr)});
    assert_eq!(
        smol::block_on(factory.auth_status(&config)).unwrap()["source"],
        "stored"
    );
    let model = Model::from_spec("tensorx/vendor/chat").unwrap();
    let (model, provider) = smol::block_on(factory.prepare(&model, &config)).unwrap();
    let (events, _received) = flume::unbounded();
    smol::block_on(provider.stream_message(
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
    let request = server
        .requests
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    assert_eq!(request.header("authorization"), Some("Bearer saved-key"));
    save_provider_credentials(
        &state,
        "tensorx",
        &ProviderCredentials {
            api_key: "refreshed-key".into(),
            host: None,
        },
    )
    .unwrap();
    smol::block_on(provider.reload_auth()).unwrap();
    smol::block_on(provider.stream_message(
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
    let request = server
        .requests
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    assert_eq!(
        request.header("authorization"),
        Some("Bearer refreshed-key")
    );
    smol::block_on(factory.logout(&config)).unwrap();
    assert!(load_provider_credentials(&state, "tensorx").is_none());
    assert_eq!(
        smol::block_on(factory.auth_status(&config)).unwrap()["source"],
        "config"
    );
}

#[test]
fn tensorx_discovery_drives_stream_parameters() {
    const ENDPOINT: &str = "https://api.tensorx.ai/v1";
    const CATALOG: &str = r#"{"data":[{"model_name":"vendor/chat","model_info":{"mode":"chat","max_tokens":100000,"max_output_tokens":4096,"supports_reasoning":true,"supports_vision":true,"input_cost_per_token":0.000002,"output_cost_per_token":0.000004,"cache_creation_input_token_cost":0.000001,"cache_read_input_token_cost":0.0000005,"supported_openai_params":["thinking","reasoning_effort",null]}},{"model_name":"vendor/embed","model_info":{"mode":"embedding"}}]}"#;
    const SSE: &str = "data: {\"choices\":[{\"delta\":{\"content\":\"hello\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n";
    let server = HttpContract::start(vec![
        Response {
            status: "200 OK".into(),
            headers: vec![],
            steps: vec![Step::Chunk(CATALOG.as_bytes().to_vec())],
        },
        Response {
            status: "200 OK".into(),
            headers: vec![],
            steps: vec![Step::Chunk(SSE.as_bytes().to_vec())],
        },
        Response {
            status: "200 OK".into(),
            headers: vec![],
            steps: vec![Step::Chunk(SSE.as_bytes().to_vec())],
        },
        Response {
            status: "200 OK".into(),
            headers: vec![],
            steps: vec![Step::Chunk(SSE.as_bytes().to_vec())],
        },
    ])
    .unwrap();
    maki_lua::set_allowed_private_hosts(&[server.addr.to_string()]);
    let host = PluginHost::new(Arc::new(ToolRegistry::new())).unwrap();
    let source = include_str!("../plugins/tensorx/init.lua");
    assert_eq!(source.matches(ENDPOINT).count(), 1);
    let source = source
        .replace(ENDPOINT, &format!("http://{}/v1", server.addr))
        .replace("if ctx and ctx.credentials then", "if false then");
    host.load_source("tensorx", &source).unwrap();
    let snapshot = host.provider_registry().snapshot();
    let registration = snapshot.get("tensorx").unwrap();
    let config = json!({"api_key": "fixture-key", "api_key_env": "TENSORX_CONTRACT_UNUSED_KEY", "base_url": "http://unused.invalid"});
    let models = smol::block_on(registration.factory.discover(&config))
        .unwrap()
        .unwrap();
    assert_eq!(models.len(), 1);
    assert_eq!(models[0].id, "vendor/chat");
    assert_eq!(models[0].context_window, Some(100000));
    assert_eq!(models[0].max_output_tokens, None);
    assert_eq!(models[0].supports_thinking, Some(true));
    assert_eq!(models[0].supports_vision, Some(true));
    let pricing = models[0].pricing.as_ref().unwrap();
    assert_eq!(pricing.input, 2.0);
    assert_eq!(pricing.output, 4.0);
    assert_eq!(pricing.cache_write, 1.0);
    assert_eq!(pricing.cache_read, 0.5);
    let discovery = server
        .requests
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    assert_eq!(discovery.method, "GET");
    assert_eq!(discovery.target, "/v1/model/info");
    assert_eq!(
        discovery.header("authorization"),
        Some("Bearer fixture-key")
    );
    let model = Model::from_spec("tensorx/vendor/chat").unwrap();
    let (model, provider) = smol::block_on(registration.factory.prepare(&model, &json!({"api_key": "fixture-key", "api_key_env": "TENSORX_CONTRACT_UNUSED_KEY", "base_url": format!("http://{}/v1", server.addr)}))).unwrap();
    let (events, _received) = flume::unbounded();
    smol::block_on(provider.stream_message(
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
    let request = server
        .requests
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    assert_eq!(request.target, "/v1/chat/completions");
    let body: Value = serde_json::from_slice(&request.body).unwrap();
    assert_eq!(body["thinking"], true);
    assert_eq!(body["reasoning_effort"], "high");
    assert!(body.get("max_tokens").is_none());
    assert!(body.get("chat_template_kwargs").is_none());
    for (thinking, expected, enabled) in [
        (ThinkingConfig::Off, "none", false),
        (ThinkingConfig::Budget(8192), "medium", true),
    ] {
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
        let body: Value = serde_json::from_slice(&request.body).unwrap();
        assert_eq!(body["thinking"], enabled);
        assert_eq!(body["reasoning_effort"], expected);
    }
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
    let source = include_str!("../plugins/tensorx/init.lua")
        .replace("if ctx and ctx.credentials then", "if false then");
    host.load_source("tensorx", &source).unwrap();
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
