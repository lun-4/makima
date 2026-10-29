#[path = "support/http_contract.rs"]
mod http_contract;

use std::io::{Read, Seek, SeekFrom};
use std::process::{Child, Command, Output};
use std::sync::Arc;
use std::time::{Duration, Instant};

use http_contract::{HttpContract, Response, Step};
use maki_agent::tools::ToolRegistry;
use maki_lua::PluginHost;
use maki_providers::{
    AgentError, Message, Model, ProviderEvent, RequestOptions, StopReason, ThinkingConfig,
};
use serde_json::{Value, json};

struct ChildGuard(Option<Child>);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        if let Some(child) = &mut self.0 {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

fn wait_output(
    mut child: ChildGuard,
    mut stdout: std::fs::File,
    mut stderr: std::fs::File,
) -> Output {
    let deadline = Instant::now() + Duration::from_secs(30);
    let status = loop {
        if let Some(status) = child.0.as_mut().unwrap().try_wait().unwrap() {
            break status;
        }
        assert!(Instant::now() < deadline, "child timed out");
        std::thread::sleep(Duration::from_millis(10));
    };
    stdout.seek(SeekFrom::Start(0)).unwrap();
    stderr.seek(SeekFrom::Start(0)).unwrap();
    let mut out = Vec::new();
    let mut err = Vec::new();
    stdout.read_to_end(&mut out).unwrap();
    stderr.read_to_end(&mut err).unwrap();
    Output {
        status,
        stdout: out,
        stderr: err,
    }
}

#[test]
fn anthropic_direct_streams_through_plugin_host_and_http() {
    const CHILD_MARKER: &str = "ANTHROPIC_CONTRACT_CHILD";
    const ERROR_MESSAGE: &str = "rate limited";
    if std::env::var_os(CHILD_MARKER).is_none() {
        let stdout = tempfile::tempfile().unwrap();
        let stderr = tempfile::tempfile().unwrap();
        let child = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "anthropic_direct_streams_through_plugin_host_and_http",
                "--nocapture",
            ])
            .env_clear()
            .env(CHILD_MARKER, "1")
            .stdout(stdout.try_clone().unwrap())
            .stderr(stderr.try_clone().unwrap())
            .spawn()
            .unwrap();
        let output = wait_output(ChildGuard(Some(child)), stdout, stderr);
        assert!(
            output.status.success(),
            "Lua test subprocess failed: {}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(String::from_utf8_lossy(&output.stdout).contains("1 passed"));
        return;
    }

    let server = HttpContract::start(vec![
        Response {
            status: "200 OK".into(),
            headers: vec![("Content-Type".into(), "text/event-stream".into())],
            steps: concat!(
                "event: message_start\ndata: {\"message\":{\"usage\":{\"input_tokens\":10,\"output_tokens\":1,\"cache_read_input_tokens\":4,\"cache_creation_input_tokens\":2}}}\n\n",
                "event: content_block_start\ndata: {\"index\":0,\"content_block\":{\"type\":\"thinking\"}}\n\n",
                "event: content_block_delta\ndata: {\"index\":0,\"delta\":{\"type\":\"thinking_delta\",\"thinking\":\"reason\"}}\n\n",
                "event: content_block_delta\ndata: {\"index\":0,\"delta\":{\"type\":\"signature_delta\",\"signature\":\"signed\"}}\n\n",
                "event: content_block_start\ndata: {\"index\":1,\"content_block\":{\"type\":\"text\"}}\n\n",
                "event: content_block_delta\ndata: {\"index\":1,\"delta\":{\"type\":\"text_delta\",\"text\":\"hello\"}}\n\n",
                "event: content_block_start\ndata: {\"index\":2,\"content_block\":{\"type\":\"tool_use\",\"id\":\"call_1\",\"name\":\"lookup\"}}\n\n",
                "event: content_block_delta\ndata: {\"index\":2,\"delta\":{\"type\":\"input_json_delta\",\"partial_json\":\"{\\\"value\\\":\"}}\n\n",
                "event: content_block_delta\ndata: {\"index\":2,\"delta\":{\"type\":\"input_json_delta\",\"partial_json\":\"2}\"}}\n\n",
                "event: content_block_stop\ndata: {\"index\":2}\n\n",
                "event: message_delta\ndata: {\"delta\":{\"stop_reason\":\"tool_use\"},\"usage\":{\"output_tokens\":5}}\n\n",
                "event: message_stop\ndata: {}\n\n"
            ).as_bytes().chunks(7).map(|chunk| Step::Chunk(chunk.to_vec())).collect(),
        },
        Response { status: "429 Too Many Requests".into(), headers: vec![], steps: vec![Step::Chunk(ERROR_MESSAGE.as_bytes().to_vec())] },
        Response { status: "200 OK".into(), headers: vec![], steps: vec![Step::Chunk(br#"{"data":[{"id":"model-a","max_input_tokens":1000000}],"has_more":true,"last_id":"model-a"}"#.to_vec())] },
        Response { status: "200 OK".into(), headers: vec![], steps: vec![Step::Chunk(br#"{"data":[{"id":"model-b","max_input_tokens":200000}],"has_more":false}"#.to_vec())] },
        Response { status: "200 OK".into(), headers: vec![], steps: vec![Step::Chunk(br#"{"five_hour":{"utilization":42,"resets_at":"2024-01-01T00:00:00Z"}}"#.to_vec())] },
    ]).unwrap();
    maki_lua::set_allowed_private_hosts(&[server.addr.to_string()]);
    let host = PluginHost::new(Arc::new(ToolRegistry::new())).unwrap();
    host.load_source(
        "anthropic-direct",
        "require('maki.anthropic_direct').register()",
    )
    .unwrap();
    let snapshot = host.provider_registry().snapshot();
    let registration = snapshot.get("anthropic").unwrap();
    assert!(
        registration
            .models
            .iter()
            .any(|model| model.id == "claude-sonnet-4-5")
    );
    let model = Model::from_spec("anthropic/claude-sonnet-4-5").unwrap();
    let (model, provider) = smol::block_on(registration.factory.prepare(
        &model, &json!({"api_key": "fixture-key", "base_url": format!("http://{}/v1/messages", server.addr), "headers": {"x-fixture": "custom"}}),
    )).unwrap();
    let messages = [Message::user("fixture prompt".into())];
    let tools = json!([{"name": "lookup", "description": "Look up a value", "input_schema": {"type": "object"}}]);
    let options = RequestOptions {
        thinking: ThinkingConfig::Off,
        fast: false,
    };
    let (events, received) = flume::unbounded();
    let result = smol::block_on(provider.stream_message(
        &model,
        &messages,
        "system prompt",
        &tools,
        &events,
        options,
        None,
    ))
    .unwrap();
    assert_eq!(result.stop_reason, Some(StopReason::ToolUse));
    assert_eq!(
        (
            result.usage.input,
            result.usage.output,
            result.usage.cache_read,
            result.usage.cache_creation
        ),
        (10, 5, 4, 2)
    );
    let content = serde_json::to_value(&result.message.content).unwrap();
    assert_eq!(content[0]["thinking"], "reason");
    assert_eq!(content[0]["signature"], "signed");
    assert_eq!(content[1]["text"], "hello");
    assert_eq!(content[2]["id"], "call_1");
    assert_eq!(content[2]["name"], "lookup");
    assert_eq!(content[2]["input"], json!({"value": 2}));
    assert_eq!(content.as_array().unwrap().len(), 3);
    assert!(
        matches!(received.try_recv(), Ok(ProviderEvent::ThinkingDelta { text }) if text == "reason")
    );
    assert!(
        matches!(received.try_recv(), Ok(ProviderEvent::TextDelta { text }) if text == "hello")
    );
    assert!(
        matches!(received.try_recv(), Ok(ProviderEvent::ToolUseStart { id, name }) if id == "call_1" && name == "lookup")
    );
    assert!(received.try_recv().is_err());
    let request = server
        .requests
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    assert_eq!(request.method, "POST");
    assert_eq!(request.target, "/v1/messages");
    assert_eq!(request.header("x-api-key"), Some("fixture-key"));
    assert_eq!(request.header("anthropic-version"), Some("2023-06-01"));
    assert_eq!(request.header("content-type"), Some("application/json"));
    assert_eq!(request.header("x-fixture"), Some("custom"));
    let body: Value = serde_json::from_slice(&request.body).unwrap();
    assert_eq!(body["model"], "claude-sonnet-4-5");
    assert_eq!(body["stream"], true);
    assert_eq!(body["system"][0]["text"], "system prompt");
    assert_eq!(body["messages"][0]["role"], "user");
    assert_eq!(body["messages"][0]["content"][0]["text"], "fixture prompt");
    assert_eq!(body["tools"][0]["name"], "lookup");
    let error = smol::block_on(provider.stream_message(
        &model,
        &messages,
        "system prompt",
        &tools,
        &events,
        options,
        None,
    ))
    .unwrap_err();
    assert!(
        matches!(&error, AgentError::Api { status: 429, message, .. } if message == ERROR_MESSAGE),
        "unexpected error: {error:?}"
    );
    assert_eq!(
        server
            .requests
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
            .unwrap()
            .target,
        "/v1/messages"
    );
    let config = json!({"api_key": "fixture-key", "base_url": format!("http://{}", server.addr), "headers": {"authorization": "Bearer fixture-token", "x-fixture": "custom"}});
    let models = smol::block_on(registration.factory.discover(&config))
        .unwrap()
        .unwrap();
    assert_eq!(
        models.iter().map(|m| m.id.as_str()).collect::<Vec<_>>(),
        ["model-a", "model-a-1m", "model-b"]
    );
    for target in [
        "/v1/models?limit=1000",
        "/v1/models?limit=1000&after_id=model-a",
    ] {
        let request = server
            .requests
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
            .unwrap();
        assert_eq!(request.target, target);
        assert_eq!(request.header("x-api-key"), Some("fixture-key"));
        assert_eq!(request.header("x-fixture"), Some("custom"));
    }
    let (_, usage_provider) =
        smol::block_on(registration.factory.prepare(&model, &config)).unwrap();
    let usage = smol::block_on(usage_provider.fetch_usage())
        .unwrap()
        .unwrap();
    assert_eq!(usage.limits[0].percentage, Some(42));
    assert_eq!(usage.limits[0].reset_at, Some(1_704_067_200_000));
    let usage_request = server
        .requests
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    assert_eq!(usage_request.target, "/api/oauth/usage");
    assert_eq!(
        usage_request.header("authorization"),
        Some("Bearer fixture-token")
    );
    assert_eq!(
        usage_request.header("anthropic-beta"),
        Some("oauth-2025-04-20")
    );
    maki_lua::set_allowed_private_hosts(&[]);
}
