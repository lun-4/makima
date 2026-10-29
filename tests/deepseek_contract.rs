#[path = "support/http_contract.rs"]
mod http_contract;

use std::sync::Arc;
use std::time::Duration;

use http_contract::{HttpContract, Response, Step};
use maki_agent::tools::ToolRegistry;
use maki_commands::CommandRegistry;
use maki_config::PluginsConfig;
use maki_lua::PluginHost;
use maki_lua::{test_support::InMemoryFs, ui_sink::HostUi};
use maki_providers::{
    ContentBlock, Message, Model, ProviderEvent, RequestOptions, Role, ThinkingConfig, UsageWindow,
};
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

fn deepseek_legacy_credentials_discovery_and_logout() {
    let server = HttpContract::start(vec![response(r#"{"data":[{"id":"discovered"}]}"#)]).unwrap();
    maki_lua::set_allowed_private_hosts(&[server.addr.to_string()]);
    let temp = tempfile::tempdir().unwrap();
    let state = StateDir::from_path(temp.path().to_path_buf());
    save_provider_credentials(
        &state,
        "deepseek",
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
        names: vec!["deepseek".into()],
        ..Default::default()
    })
    .unwrap();
    let snapshot = host.provider_registry().snapshot();
    let factory = &snapshot.get("deepseek").unwrap().factory;
    let config = json!({"api_key_env":"MISSING_DEEPSEEK_CONTRACT_KEY", "api_key":"config-key", "base_url":format!("http://{}", server.addr)});
    assert_eq!(
        smol::block_on(factory.auth_status(&config)).unwrap()["source"],
        "stored"
    );
    assert_eq!(
        smol::block_on(factory.discover(&config)).unwrap().unwrap()[0].id,
        "discovered"
    );
    let request = server.requests.recv_timeout(TIMEOUT).unwrap().unwrap();
    assert_eq!(request.header("authorization"), Some("Bearer saved-key"));
    smol::block_on(factory.logout(&config)).unwrap();
    assert!(load_provider_credentials(&state, "deepseek").is_none());
    assert_eq!(
        smol::block_on(factory.auth_status(&config)).unwrap()["source"],
        "config"
    );
    maki_lua::set_allowed_private_hosts(&[]);
}

#[test]
fn deepseek_discovery_rotation_stream_and_balance() {
    deepseek_legacy_credentials_discovery_and_logout();
    let server = HttpContract::start(vec![
        response(r#"{"data":[{"id":"z","max_model_len":12345,"max_output_length":4321,"input_modalities":["text","image"],"pricing":{"prompt":"0.5","completion":"1","cache_read":"0.1","cache_creation":"-1"}},{"id":"a"},{}]}"#),
        response(STREAM),
        response(STREAM),
        response(STREAM),
        response(r#"{"balance_infos":[{"currency":"USD","total_balance":"12","topped_up_balance":"10","granted_balance":"2"},{"currency":"CNY","total_balance":"3","topped_up_balance":"1","granted_balance":"2"},{"currency":"EUR","total_balance":"4","topped_up_balance":"3","granted_balance":"1"}]}"#),
    ]).unwrap();
    maki_lua::set_allowed_private_hosts(&[server.addr.to_string()]);
    let host = PluginHost::new(Arc::new(ToolRegistry::new())).unwrap();
    let source = include_str!("../plugins/lib/maki/deepseek.lua")
        .replace("ctx.credentials:read(\"api_key\")", "nil, nil")
        .replace(
            "https://api.deepseek.com/user/balance",
            &format!("http://{}/user/balance", server.addr),
        );
    host.load_source(
        "deepseek",
        &format!("local provider = (function()\n{source}\nend)()\nprovider.register()"),
    )
    .unwrap();
    let snapshot = host.provider_registry().snapshot();
    let registration = snapshot.get("deepseek").unwrap();
    let config = json!({"api_key_env": "MAKI_DEEPSEEK_TEST_MISSING_KEY", "api_key": " first, second ", "base_url": format!("http://{}", server.addr)});
    let discovered = smol::block_on(registration.factory.discover(&config))
        .unwrap()
        .unwrap();
    assert_eq!(
        discovered.iter().map(|m| m.id.as_str()).collect::<Vec<_>>(),
        ["a", "z"]
    );
    assert_eq!(discovered[1].context_window, Some(12345));
    assert_eq!(discovered[1].max_output_tokens, Some(4321));
    assert_eq!(discovered[1].supports_vision, Some(true));
    assert_eq!(discovered[1].pricing.as_ref().unwrap().cache_read, 0.1);
    assert_eq!(discovered[1].pricing.as_ref().unwrap().cache_write, 0.0);
    let request = server.requests.recv_timeout(TIMEOUT).unwrap().unwrap();
    assert_eq!(request.target, "/models");
    assert_eq!(request.header("authorization"), Some("Bearer first"));
    let model = Model::from_spec("deepseek/deepseek-v4-flash").unwrap();
    let (model, provider) = smol::block_on(registration.factory.prepare(&model, &config)).unwrap();
    assert_eq!(provider.key_count(), 2);
    assert!(smol::block_on(provider.rotate_key()).unwrap());
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
    let request = server.requests.recv_timeout(TIMEOUT).unwrap().unwrap();
    assert_eq!(request.header("authorization"), Some("Bearer second"));
    let body: Value = serde_json::from_slice(&request.body).unwrap();
    assert_eq!(body["thinking"]["type"], "disabled");
    assert_eq!(body["stream_options"]["include_usage"], true);
    let messages = [Message {
        role: Role::Assistant,
        content: vec![ContentBlock::Text {
            text: "earlier".into(),
        }],
        ..Default::default()
    }];
    let tools = json!([{ "name": "lookup", "description": "lookup", "input_schema": {"type":"object", "properties":{}} }]);
    let options = RequestOptions {
        thinking: ThinkingConfig::Budget(2048),
        fast: false,
    };
    smol::block_on(
        provider.stream_message(&model, &messages, "system", &tools, &events, options, None),
    )
    .unwrap();
    let request = server.requests.recv_timeout(TIMEOUT).unwrap().unwrap();
    let body: Value = serde_json::from_slice(&request.body).unwrap();
    assert_eq!(body["thinking"]["type"], "enabled");
    assert_eq!(body["reasoning_effort"], "max");
    assert_eq!(body["messages"][1]["reasoning_content"], "");
    let mut reasoner = model.clone();
    reasoner.id = "deepseek-reasoner".into();
    smol::block_on(provider.stream_message(
        &reasoner, &messages, "system", &tools, &events, options, None,
    ))
    .unwrap();
    let request = server.requests.recv_timeout(TIMEOUT).unwrap().unwrap();
    let body: Value = serde_json::from_slice(&request.body).unwrap();
    assert!(body["messages"][1].get("reasoning_content").is_none());
    let usage = smol::block_on(provider.fetch_usage()).unwrap().unwrap();
    assert_eq!(usage.limits.len(), 3);
    assert!(matches!(usage.limits[0].kind, UsageWindow::Credits));
    assert_eq!(
        usage.limits[0].detail.as_deref(),
        Some("total: $12, topped-up: $10, granted: $2")
    );
    assert_eq!(
        usage.limits[1].detail.as_deref(),
        Some("total: ¥3, topped-up: ¥1, granted: ¥2")
    );
    assert_eq!(
        usage.limits[2].detail.as_deref(),
        Some("total: 4, topped-up: 3, granted: 1")
    );
    let request = server.requests.recv_timeout(TIMEOUT).unwrap().unwrap();
    assert_eq!(request.target, "/user/balance");
    assert_eq!(request.header("authorization"), Some("Bearer second"));
    maki_lua::set_allowed_private_hosts(&[]);
}
