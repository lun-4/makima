#[path = "support/http_contract.rs"]
mod http_contract;

use std::io::{Read, Seek, SeekFrom};
use std::process::{Child, Command, Output};
use std::sync::Arc;
use std::time::{Duration, Instant};

use http_contract::{HttpContract, Response, Step};
use maki_agent::tools::ToolRegistry;
use maki_lua::PluginHost;
use maki_providers::{AgentError, Message, Model, RequestOptions, ThinkingConfig};
use serde_json::json;

struct ChildGuard(Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn wait_output(
    mut child: ChildGuard,
    mut stdout: std::fs::File,
    mut stderr: std::fs::File,
) -> Output {
    let deadline = Instant::now() + Duration::from_secs(30);
    let status = loop {
        if let Some(status) = child.0.try_wait().unwrap() {
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
fn lua_synthetic_auth_and_http_failures() {
    const CHILD: &str = "SYNTHETIC_FAILURE_CONTRACT_CHILD";
    const MISSING_KEY: &str = "Synthetic API key is not configured";
    const EMPTY_ERROR: &str = "Synthetic request failed";
    if std::env::var_os(CHILD).is_none() {
        let stdout = tempfile::tempfile().unwrap();
        let stderr = tempfile::tempfile().unwrap();
        let child = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "lua_synthetic_auth_and_http_failures",
                "--nocapture",
            ])
            .env_clear()
            .env(CHILD, "1")
            .env("SYNTHETIC_API_KEY", "default-env-key")
            .env("CUSTOM_SYNTHETIC_KEY", "custom-env-key")
            .stdout(stdout.try_clone().unwrap())
            .stderr(stderr.try_clone().unwrap())
            .spawn()
            .unwrap();
        let output = wait_output(ChildGuard(child), stdout, stderr);
        assert!(
            output.status.success(),
            "{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(String::from_utf8_lossy(&output.stdout).contains("1 passed"));
        return;
    }

    assert!(std::env::var("SYNTHETIC_API_KEY").as_deref() == Ok("default-env-key"));
    assert!(std::env::var("CUSTOM_SYNTHETIC_KEY").as_deref() == Ok("custom-env-key"));
    assert!(std::env::var_os("SYNTHETIC_BASE_URL").is_none());
    assert!(std::env::var_os("UNSET_SYNTHETIC_KEY").is_none());

    let server = HttpContract::start(vec![
        Response {
            status: "401 Unauthorized".into(),
            headers: vec![],
            steps: vec![Step::Chunk(b"invalid key".to_vec())],
        },
        Response {
            status: "500 Internal Server Error".into(),
            headers: vec![],
            steps: vec![],
        },
        Response {
            status: "403 Forbidden".into(),
            headers: vec![],
            steps: vec![Step::Chunk(b"forbidden".to_vec())],
        },
        Response {
            status: "429 Too Many Requests".into(),
            headers: vec![("Retry-After".into(), " 3 ".into())],
            steps: vec![],
        },
        Response {
            status: "503 Service Unavailable".into(),
            headers: vec![("retry-after".into(), "0".into())],
            steps: vec![],
        },
        Response {
            status: "429 Too Many Requests".into(),
            headers: vec![("Retry-After".into(), "1.5".into())],
            steps: vec![],
        },
        Response {
            status: "429 Too Many Requests".into(),
            headers: vec![("Retry-After".into(), "999999999999999999999999".into())],
            steps: vec![],
        },
        Response {
            status: "429 Too Many Requests".into(),
            headers: vec![],
            steps: vec![],
        },
    ])
    .unwrap();
    maki_lua::set_allowed_private_hosts(&[server.addr.to_string()]);
    let host = PluginHost::new(Arc::new(ToolRegistry::new())).unwrap();
    host.load_source("synthetic", include_str!("../plugins/synthetic/init.lua"))
        .unwrap();
    let snapshot = host.provider_registry().snapshot();
    let registration = snapshot.get("synthetic").unwrap();
    let model = Model::from_spec("synthetic/hf:moonshotai/Kimi-K2.5").unwrap();
    let base = format!("http://{}/v1///", server.addr);
    let configs = [
        (
            json!({"api_key": "config-key", "base_url": base}),
            "default-env-key",
            401,
            "invalid key",
        ),
        (
            json!({"api_key_env": "CUSTOM_SYNTHETIC_KEY", "api_key": "config-key", "base_url": base}),
            "custom-env-key",
            500,
            EMPTY_ERROR,
        ),
        (
            json!({"api_key_env": "UNSET_SYNTHETIC_KEY", "api_key": "config-key", "base_url": base}),
            "config-key",
            403,
            "forbidden",
        ),
    ];
    for (config, key, status, message) in configs {
        let (prepared_model, provider) =
            smol::block_on(registration.factory.prepare(&model, &config)).unwrap();
        let (events, _) = flume::unbounded();
        let error = smol::block_on(provider.stream_message(
            &prepared_model,
            &[Message::user("hello".into())],
            "",
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
            matches!(&error, AgentError::Api { status: actual, message: actual_message, .. } if *actual == status && actual_message == message),
            "unexpected error: {error:?}"
        );
        let request = server
            .requests
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
            .unwrap();
        assert_eq!(request.target, "/v1/chat/completions");
        assert_eq!(
            request.header("authorization"),
            Some(format!("Bearer {key}").as_str())
        );
    }
    let (prepared_model, provider) = smol::block_on(
        registration
            .factory
            .prepare(&model, &json!({"base_url": base})),
    )
    .unwrap();
    for (status, expected) in [
        (429, Some(Duration::from_secs(3))),
        (503, None),
        (429, None),
        (429, None),
        (429, None),
    ] {
        let (events, _) = flume::unbounded();
        let error = smol::block_on(provider.stream_message(
            &prepared_model,
            &[Message::user("hello".into())],
            "",
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
            matches!(error, AgentError::Api { status: actual, retry_after, .. } if actual == status && retry_after == expected)
        );
        server
            .requests
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
            .unwrap();
    }
    let missing = smol::block_on(
        registration
            .factory
            .prepare(&model, &json!({"api_key_env": "UNSET_SYNTHETIC_KEY"})),
    );
    assert!(matches!(missing, Err(AgentError::Config { message }) if message == MISSING_KEY));
    maki_lua::set_allowed_private_hosts(&[]);
}
