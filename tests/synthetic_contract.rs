#[path = "support/http_contract.rs"]
mod http_contract;

use std::io::{Read, Seek, SeekFrom};
use std::process::{Child, Command, Output, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use http_contract::{HttpContract, Response, Step};
use maki_agent::tools::ToolRegistry;
use maki_commands::CommandRegistry;
use maki_config::PluginsConfig;
use maki_lua::{PluginHost, test_support::InMemoryFs, ui_sink::HostUi};
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
fn lua_synthetic_streams_through_plugin_host_and_http() {
    const CHILD_MARKER: &str = "SYNTHETIC_CONTRACT_CHILD";
    if std::env::var_os(CHILD_MARKER).is_none() {
        let stdout = tempfile::tempfile().unwrap();
        let stderr = tempfile::tempfile().unwrap();
        let child = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "lua_synthetic_streams_through_plugin_host_and_http",
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

    assert!(std::env::var_os("SYNTHETIC_API_KEY").is_none());
    assert!(std::env::var_os("SYNTHETIC_BASE_URL").is_none());
    let server = HttpContract::start(vec![
        Response {
            status: "200 OK".into(),
            headers: vec![("Content-Type".into(), "text/event-stream".into())],
            steps: vec![Step::Chunk(concat!(
                "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hello\"}}]}\n\n",
                "data: {\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":10,\"completion_tokens\":3}}\n\n",
                "data: [DONE]\n\n"
            ).as_bytes().to_vec())],
        },
        Response {
            status: "200 OK".into(),
            headers: vec![("Content-Type".into(), "text/event-stream".into())],
            steps: concat!(
                "data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":1,\"id\":\"call_b\",\"function\":{\"name\":\"beta\",\"arguments\":\"{\\\"value\\\":\"}}]}}]}\n\n",
                "data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call_a\",\"function\":{\"name\":\"alpha\",\"arguments\":\"{\\\"items\\\":[\"}},{\"index\":1,\"function\":{\"arguments\":\"2}\"}}]}}]}\n\n",
                "data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"function\":{\"arguments\":\"1,null]}\"}}]},\"finish_reason\":\"tool_calls\"}]}\n\n",
                "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":12,\"completion_tokens\":5,\"prompt_tokens_details\":{\"cached_tokens\":4},\"prompt_cache_hit_tokens\":6}}\n\n",
                "data: [DONE]\n\n"
            ).as_bytes().chunks(7).map(|chunk| Step::Chunk(chunk.to_vec())).collect(),
        },
        Response {
            status: "429 Too Many Requests".into(),
            headers: vec![],
            steps: vec![Step::Chunk(b"rate limited".to_vec())],
        },
    ]).unwrap();
    maki_lua::set_allowed_private_hosts(&[server.addr.to_string()]);
    let state = tempfile::tempdir().unwrap();
    let mut host = PluginHost::with_session_provider_preparer(
        Arc::new(ToolRegistry::new()),
        CommandRegistry::new(),
        true,
        Arc::new(InMemoryFs::new()),
        Some(state.path().to_path_buf()),
        None,
        HostUi::Frontend,
    )
    .unwrap();
    host.load_builtins(&PluginsConfig {
        enabled: true,
        names: vec!["synthetic".into()],
        ..Default::default()
    })
    .unwrap();
    let snapshot = host.provider_registry().snapshot();
    let registration = snapshot.get("synthetic").unwrap();
    assert!(
        registration
            .models
            .iter()
            .any(|model| model.id == "hf:moonshotai/Kimi-K2.5")
    );
    let model = Model::from_spec("synthetic/hf:moonshotai/Kimi-K2.5").unwrap();
    let (model, provider) = smol::block_on(registration.factory.prepare(
        &model,
        &json!({"api_key": "fixture-key", "base_url": format!("http://{}/v1/", server.addr)}),
    ))
    .unwrap();
    let messages = [Message::user("fixture prompt".into())];
    let (events, received) = flume::unbounded();
    let options = RequestOptions {
        thinking: ThinkingConfig::Off,
        fast: false,
    };
    let result = smol::block_on(provider.stream_message(
        &model,
        &messages,
        "system prompt",
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
    let request = server
        .requests
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    assert_eq!(request.method, "POST");
    assert_eq!(request.target, "/v1/chat/completions");
    assert_eq!(request.header("authorization"), Some("Bearer fixture-key"));
    let body: Value = serde_json::from_slice(&request.body).unwrap();
    assert_eq!(body["model"], "hf:moonshotai/Kimi-K2.5");
    assert_eq!(body["messages"][0]["content"], "system prompt");
    assert_eq!(body["messages"][1]["content"], "fixture prompt");
    assert_eq!(body["stream"], true);
    let (tool_events, tool_received) = flume::unbounded();
    let tools_result = smol::block_on(provider.stream_message(
        &model,
        &messages,
        "system prompt",
        &json!([]),
        &tool_events,
        options,
        None,
    ))
    .unwrap();
    assert_eq!(tools_result.stop_reason, Some(StopReason::ToolUse));
    assert_eq!(
        (
            tools_result.usage.input,
            tools_result.usage.output,
            tools_result.usage.cache_read,
            tools_result.usage.cache_creation
        ),
        (6, 5, 6, 0)
    );
    let content = serde_json::to_value(&tools_result.message.content).unwrap();
    assert_eq!(content[0]["id"], "call_a");
    assert_eq!(content[0]["name"], "alpha");
    assert_eq!(content[0]["input"], json!({"items": [1, null]}));
    assert_eq!(content[1]["id"], "call_b");
    assert_eq!(content[1]["name"], "beta");
    assert_eq!(content[1]["input"], json!({"value": 2}));
    assert_eq!(content.as_array().unwrap().len(), 2);
    assert!(
        matches!(tool_received.try_recv(), Ok(ProviderEvent::ToolUseStart { id, name }) if id == "call_b" && name == "beta")
    );
    assert!(
        matches!(tool_received.try_recv(), Ok(ProviderEvent::ToolUseStart { id, name }) if id == "call_a" && name == "alpha")
    );
    assert!(tool_received.try_recv().is_err());
    assert_eq!(
        server
            .requests
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
            .unwrap()
            .target,
        "/v1/chat/completions"
    );
    let error = smol::block_on(provider.stream_message(
        &model,
        &messages,
        "system prompt",
        &json!([]),
        &events,
        options,
        None,
    ))
    .unwrap_err();
    assert!(
        matches!(&error, AgentError::Api { status: 429, message, .. } if message == "rate limited"),
        "unexpected error: {error:?}"
    );
    assert_eq!(
        server
            .requests
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
            .unwrap()
            .target,
        "/v1/chat/completions"
    );
    maki_lua::set_allowed_private_hosts(&[]);
}

#[test]
fn synthetic_chat_uses_configured_endpoint_and_bearer_key() {
    let server = HttpContract::start(vec![Response {
        status: "200 OK".into(),
        headers: vec![("Content-Type".into(), "application/json".into())],
        steps: vec![Step::Chunk(br#"{"data":[{"id":"hf:moonshotai/Kimi-K2.5"}]}"#.to_vec())],
    }, Response {
        status: "200 OK".into(),
        headers: vec![("Content-Type".into(), "text/event-stream".into())],
        steps: vec![Step::Chunk(concat!(
            "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"synthetic fixture response\"},\"finish_reason\":null}]}\n\n",
            "data: {\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n",
            "data: [DONE]\n\n"
        ).as_bytes().to_vec())],
    }]).unwrap();
    let home = tempfile::tempdir().unwrap();
    let stdout = tempfile::tempfile().unwrap();
    let stderr = tempfile::tempfile().unwrap();
    let child = Command::new(env!("CARGO_BIN_EXE_makima"))
        .args([
            "--print",
            "--yolo",
            "--max-turns",
            "1",
            "-m",
            "synthetic/hf:moonshotai/Kimi-K2.5",
            "-p",
            "fixture prompt",
        ])
        .env_clear()
        .current_dir(home.path())
        .env("PATH", std::env::var_os("PATH").unwrap_or_default())
        .env("HOME", home.path())
        .env("XDG_CONFIG_HOME", home.path().join(".config"))
        .env("XDG_STATE_HOME", home.path().join(".local/state"))
        .env("XDG_DATA_HOME", home.path().join(".local/share"))
        .env("XDG_CACHE_HOME", home.path().join(".cache"))
        .env("SYNTHETIC_API_KEY", "fixture-key")
        .env("SYNTHETIC_BASE_URL", format!("http://{}/v1", server.addr))
        .env("OLLAMA_HOST", "http://127.0.0.1:1")
        .stdin(Stdio::null())
        .stdout(stdout.try_clone().unwrap())
        .stderr(stderr.try_clone().unwrap())
        .spawn()
        .unwrap();
    let child = ChildGuard(Some(child));
    let discovery = server
        .requests
        .recv_timeout(Duration::from_secs(10))
        .unwrap()
        .unwrap();
    assert_eq!(discovery.method, "GET");
    assert_eq!(discovery.target, "/v1/models");
    assert_eq!(
        discovery.header("authorization"),
        Some("Bearer fixture-key")
    );
    let request = server
        .requests
        .recv_timeout(Duration::from_secs(10))
        .unwrap()
        .unwrap();
    let output = wait_output(child, stdout, stderr);
    let status = output.status;
    assert!(
        status.success(),
        "child failed: {status}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("synthetic fixture response"));
    assert_eq!(request.method, "POST", "unexpected request: {request:?}");
    assert_eq!(request.target, "/v1/chat/completions");
    let body: Value = serde_json::from_slice(&request.body).unwrap();
    assert_eq!(request.header("authorization"), Some("Bearer fixture-key"));
    assert_eq!(body["model"], "hf:moonshotai/Kimi-K2.5");
    assert!(
        body["messages"]
            .as_array()
            .unwrap()
            .iter()
            .any(|message| { message["role"] == "user" && message["content"] == "fixture prompt" })
    );
}
