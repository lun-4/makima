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
use maki_storage::id::SessionRef;
use maki_storage::{
    StateDir,
    auth::{ProviderCredentials, load_provider_credentials, save_provider_credentials},
};
use serde_json::{Value, json};

const TIMEOUT: Duration = Duration::from_secs(5);
const STREAM: &str = concat!(
    "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hello\"}}]}\n\n",
    "data: {\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":10,\"completion_tokens\":3}}\n\n",
    "data: [DONE]\n\n"
);

fn response(body: &str) -> Response {
    Response {
        status: "200 OK".into(),
        headers: vec![],
        steps: vec![Step::Chunk(body.as_bytes().to_vec())],
    }
}

fn mistral_legacy_auth_discovery_and_logout() {
    let server = HttpContract::start(vec![response(
        r#"{"data":[{"id":"chat","capabilities":{"completion_chat":true,"reasoning":true}}]}"#,
    )])
    .unwrap();
    maki_lua::set_allowed_private_hosts(&[server.addr.to_string()]);
    let temp = tempfile::tempdir().unwrap();
    let state = StateDir::from_path(temp.path().to_path_buf());
    save_provider_credentials(
        &state,
        "mistral",
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
        names: vec!["mistral".into()],
        ..Default::default()
    })
    .unwrap();
    let snapshot = host.provider_registry().snapshot();
    let factory = &snapshot.get("mistral").unwrap().factory;
    let config = json!({"api_key_env":"MAKI_MISTRAL_CONTRACT_MISSING", "api_key":"config-key", "base_url":format!("http://{}/v1", server.addr)});
    assert_eq!(
        smol::block_on(factory.auth_status(&config)).unwrap()["source"],
        "stored"
    );
    let models = smol::block_on(factory.discover(&config)).unwrap().unwrap();
    assert_eq!(models[0].supports_thinking, Some(true));
    let request = server.requests.recv_timeout(TIMEOUT).unwrap().unwrap();
    assert_eq!(request.header("authorization"), Some("Bearer saved-key"));
    smol::block_on(factory.logout(&config)).unwrap();
    assert!(load_provider_credentials(&state, "mistral").is_none());
    assert_eq!(
        smol::block_on(factory.auth_status(&config)).unwrap()["source"],
        "config"
    );
    maki_lua::set_allowed_private_hosts(&[]);
}

#[test]
fn mistral_discovery_and_rotated_stream() {
    mistral_legacy_auth_discovery_and_logout();
    let server = HttpContract::start(vec![
        response(r#"{"data":[{"id":"chat","max_context_length":12345,"capabilities":{"completion_chat":true,"reasoning":false,"vision":true}},{"id":"other","capabilities":{"completion_chat":false}},{"id":"unknown","max_context_length":0,"capabilities":{"completion_chat":true}}]}"#),
        response(STREAM),
    ]).unwrap();
    maki_lua::set_allowed_private_hosts(&[server.addr.to_string()]);
    let host = PluginHost::new(Arc::new(ToolRegistry::new())).unwrap();
    host.load_source("mistral", include_str!("../plugins/mistral/init.lua"))
        .unwrap();
    let snapshot = host.provider_registry().snapshot();
    let registration = snapshot.get("mistral").unwrap();
    let config = json!({"api_key_env": "MAKI_MISTRAL_TEST_MISSING_KEY", "api_key": "first, second", "base_url": format!("http://{}/v1/", server.addr)});
    let models = smol::block_on(registration.factory.discover(&config))
        .unwrap()
        .unwrap();
    assert_eq!(models.len(), 2);
    let chat = models.iter().find(|model| model.id == "chat").unwrap();
    assert_eq!(chat.context_window, Some(12345));
    assert_eq!(chat.supports_thinking, Some(false));
    assert_eq!(chat.supports_vision, Some(true));
    assert_eq!(
        models
            .iter()
            .find(|model| model.id == "unknown")
            .unwrap()
            .context_window,
        None
    );
    let discovery = server.requests.recv_timeout(TIMEOUT).unwrap().unwrap();
    assert_eq!(discovery.target, "/v1/models");
    assert_eq!(discovery.header("authorization"), Some("Bearer first"));
    let model = Model::from_spec("mistral/mistral-medium-latest").unwrap();
    let (model, provider) = smol::block_on(registration.factory.prepare(&model, &config)).unwrap();
    assert_eq!(provider.key_count(), 2);
    assert!(smol::block_on(provider.rotate_key()).unwrap());
    let (events, received) = flume::unbounded();
    let session = SessionRef::generate();
    let result = smol::block_on(provider.stream_message(
        &model,
        &[Message::user("hello".into())],
        "system",
        &json!([]),
        &events,
        RequestOptions {
            thinking: ThinkingConfig::Adaptive,
            fast: false,
        },
        Some(&session),
    ))
    .unwrap();
    assert_eq!(result.message.first_text_content(), Some("hello"));
    assert!(
        matches!(received.try_recv(), Ok(ProviderEvent::TextDelta { text }) if text == "hello")
    );
    let request = server.requests.recv_timeout(TIMEOUT).unwrap().unwrap();
    assert_eq!(request.target, "/v1/chat/completions");
    assert_eq!(request.header("authorization"), Some("Bearer second"));
    assert_eq!(request.header("x-affinity"), Some(session.as_str()));
    let body: Value = serde_json::from_slice(&request.body).unwrap();
    assert_eq!(body["reasoning_effort"], "high");
    assert_eq!(body["stream_options"]["include_usage"], true);
    maki_lua::set_allowed_private_hosts(&[]);
}
