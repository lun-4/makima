#[path = "support/http_contract.rs"]
mod http_contract;

use std::sync::Arc;
use std::time::Duration;

use http_contract::{HttpContract, Response, Step};
use maki_agent::tools::ToolRegistry;
use maki_commands::CommandRegistry;
use maki_config::PluginsConfig;
use maki_lua::{PluginHost, test_support::InMemoryFs, ui_sink::HostUi};
use maki_providers::{
    AgentError, Message, Model, ProviderEvent, RequestOptions, StopReason, ThinkingConfig,
    UsageWindow,
};
use maki_storage::{
    StateDir,
    auth::{ProviderCredentials, load_provider_credentials, save_provider_credentials},
};
use serde_json::{Value, json};

const TIMEOUT: Duration = Duration::from_secs(5);
const QUOTA_URL: &str = "https://api.z.ai/api/monitor/usage/quota/limit";
const STREAM: &str = concat!(
    "data: {\"choices\":[{\"delta\":{\"content\":\"hello\"}}]}\n\n",
    "data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":10,\"completion_tokens\":3}}\n\n",
    "data: [DONE]\n\n"
);

fn response(status: &str, body: &str, headers: &[(&str, &str)]) -> Response {
    Response {
        status: status.into(),
        headers: headers
            .iter()
            .map(|(k, v)| ((*k).into(), (*v).into()))
            .collect(),
        steps: body
            .as_bytes()
            .chunks(7)
            .map(|chunk| Step::Chunk(chunk.to_vec()))
            .collect(),
    }
}

#[test]
fn zai_legacy_credentials_are_used_and_deleted() {
    let temp = tempfile::tempdir().unwrap();
    let state = StateDir::from_path(temp.path().to_path_buf());
    save_provider_credentials(
        &state,
        "zai",
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
        names: vec!["zai".into()],
        ..Default::default()
    })
    .unwrap();
    let snapshot = host.provider_registry().snapshot();
    let factory = &snapshot.get("zai").unwrap().factory;
    let config = json!({"api_key_env":"MAKI_ZAI_CONTRACT_UNUSED_KEY", "api_key":"config-key"});
    assert_eq!(
        smol::block_on(factory.auth_status(&config)).unwrap()["source"],
        "stored"
    );
    let model = Model::from_spec("zai/glm-5.3").unwrap();
    let (_, provider) = smol::block_on(factory.prepare(&model, &config)).unwrap();
    assert_eq!(provider.key_count(), 1);
    smol::block_on(factory.logout(&config)).unwrap();
    assert!(load_provider_credentials(&state, "zai").is_none());
    assert_eq!(
        smol::block_on(factory.auth_status(&config)).unwrap()["source"],
        "config"
    );
}

#[test]
fn zai_host_http_stream_quota_rotation_and_errors() {
    let server = HttpContract::start(vec![
        response("200 OK", STREAM, &[]),
        response("200 OK", concat!(
            "data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call_a\",\"function\":{\"name\":\"alpha\",\"arguments\":\"{\\\"value\\\":\"}}]}}]}\n\n",
            "data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"function\":{\"arguments\":\"2}\"}}]},\"finish_reason\":\"tool_calls\"}]}\n\n",
            "data: [DONE]\n\n"
        ), &[]),
        response("200 OK", r#"{"data":{"level":"Pro","limits":[{"type":"TOKENS_LIMIT","unit":3,"percentage":25,"nextResetTime":123},{"type":"TOKENS_LIMIT","unit":6,"percentage":50},{"type":"TIME_LIMIT","unit":1,"percentage":10},{"type":"X","unit":9,"percentage":5}]}}"#, &[]),
        response("429 Too Many Requests", "1113 insufficient balance", &[("Retry-After", "3")]),
        response("429 Too Many Requests", "rate limited", &[("Retry-After", "3")]),
        response("503 Service Unavailable", "fixture-secret leaked", &[]),
        response("200 OK", r#"{"data":{"limits":[{"type":"TIME_LIMIT","unit":-1,"percentage":2}]}}"#, &[]),
    ]).unwrap();
    maki_lua::set_allowed_private_hosts(&[server.addr.to_string()]);
    let host = PluginHost::new(Arc::new(ToolRegistry::new())).unwrap();
    let module = include_str!("../plugins/lib/maki/zai.lua")
        .replace(
            QUOTA_URL,
            &format!("http://{}/api/monitor/usage/quota/limit", server.addr),
        )
        .replace("if ctx and ctx.credentials then", "if false then");
    host.load_source(
        "zai",
        &format!("local provider = (function()\n{module}\nend)()\nprovider.register()"),
    )
    .unwrap();
    let snapshot = host.provider_registry().snapshot();
    let registration = snapshot.get("zai").unwrap();
    assert!(
        registration
            .models
            .iter()
            .any(|model| model.id == "glm-5.3")
    );
    let model = Model::from_spec("zai/glm-5.3").unwrap();
    let (model, provider) = smol::block_on(registration.factory.prepare(
        &model,
        &json!({
            "api_key_env": "MAKI_ZAI_CONTRACT_UNUSED_KEY", "api_key": "first, fixture-secret",
            "base_url": format!("http://{}/v4/", server.addr)
        }),
    ))
    .unwrap();
    assert_eq!(provider.key_count(), 2);
    let messages = [Message::user("prompt".into())];
    let options = RequestOptions {
        thinking: ThinkingConfig::Off,
        fast: false,
    };
    let (events, received) = flume::unbounded();
    let result = smol::block_on(provider.stream_message(
        &model,
        &messages,
        "system",
        &json!([]),
        &events,
        options,
        None,
    ))
    .unwrap();
    assert_eq!(result.message.first_text_content(), Some("hello"));
    assert_eq!(result.stop_reason, Some(StopReason::EndTurn));
    assert_eq!((result.usage.input, result.usage.output), (10, 3));
    assert!(
        matches!(received.try_recv(), Ok(ProviderEvent::TextDelta { text }) if text == "hello")
    );
    let request = server.requests.recv_timeout(TIMEOUT).unwrap().unwrap();
    assert_eq!(request.method, "POST");
    assert_eq!(request.target, "/v4/chat/completions");
    assert_eq!(request.header("authorization"), Some("Bearer first"));
    let body: Value = request.json().unwrap();
    assert_eq!(body["model"], "glm-5.3");
    assert_eq!(body["messages"][0]["content"], "system");
    assert_eq!(body["messages"][1]["content"], "prompt");
    assert_eq!(body["stream"], true);
    assert_eq!(body["reasoning_effort"], "none");
    assert!(body.get("stream_options").is_none());
    assert!(smol::block_on(provider.rotate_key()).unwrap());
    let (events, received) = flume::unbounded();
    let result = smol::block_on(provider.stream_message(
        &model,
        &messages,
        "system",
        &json!([]),
        &events,
        options,
        None,
    ))
    .unwrap();
    assert_eq!(result.stop_reason, Some(StopReason::ToolUse));
    let content = serde_json::to_value(&result.message.content).unwrap();
    assert_eq!(content[0]["id"], "call_a");
    assert_eq!(content[0]["input"], json!({"value": 2}));
    assert!(
        matches!(received.try_recv(), Ok(ProviderEvent::ToolUseStart { id, name }) if id == "call_a" && name == "alpha")
    );
    let request = server.requests.recv_timeout(TIMEOUT).unwrap().unwrap();
    assert_eq!(
        request.header("authorization"),
        Some("Bearer fixture-secret")
    );
    let usage = smol::block_on(provider.fetch_usage()).unwrap().unwrap();
    assert_eq!(usage.plan.as_deref(), Some("Pro"));
    assert_eq!(usage.limits.len(), 4);
    assert_eq!(usage.limits[0].kind, UsageWindow::Hours(5));
    assert_eq!(usage.limits[0].percentage, Some(25));
    assert_eq!(usage.limits[0].reset_at, Some(123));
    assert_eq!(usage.limits[1].kind, UsageWindow::Weekly { model: None });
    assert_eq!(usage.limits[2].kind, UsageWindow::Subscription);
    assert_eq!(usage.limits[3].kind, UsageWindow::Other("X #9".into()));
    let request = server.requests.recv_timeout(TIMEOUT).unwrap().unwrap();
    assert_eq!(request.method, "GET");
    assert_eq!(request.target, "/api/monitor/usage/quota/limit");
    assert_eq!(
        request.header("authorization"),
        Some("Bearer fixture-secret")
    );
    for (status, message, retry) in [
        (402, "1113 insufficient balance", None),
        (429, "rate limited", Some(Duration::from_secs(3))),
        (503, "Z.AI request failed", None),
    ] {
        let error = smol::block_on(provider.stream_message(
            &model,
            &messages,
            "system",
            &json!([]),
            &events,
            options,
            None,
        ))
        .unwrap_err();
        assert!(
            matches!(error, AgentError::Api { status: actual, message: ref text, retry_after, .. } if actual == status && text == message && retry_after == retry),
            "unexpected error: {error:?}"
        );
        let request = server.requests.recv_timeout(TIMEOUT).unwrap().unwrap();
        assert_eq!(request.target, "/v4/chat/completions");
        assert_eq!(
            request.header("authorization"),
            Some("Bearer fixture-secret")
        );
    }
    let error = smol::block_on(provider.fetch_usage()).unwrap_err();
    assert!(
        matches!(error, AgentError::Api { ref message, .. } if message == "Invalid Z.AI quota response"),
        "unexpected error: {error:?}"
    );
    assert_eq!(
        server
            .requests
            .recv_timeout(TIMEOUT)
            .unwrap()
            .unwrap()
            .method,
        "GET"
    );
    maki_lua::set_allowed_private_hosts(&[]);
}
