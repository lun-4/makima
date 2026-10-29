#[path = "support/http_contract.rs"]
mod http_contract;

use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

use http_contract::{HttpContract, Response, Step};
use maki_agent::tools::ToolRegistry;
use maki_config::PluginsConfig;
use maki_lua::PluginHost;
use serde_json::json;

const CHILD: &str = "ENDPOINT_GRANT_CONTRACT_CHILD";
const TIMEOUT: Duration = Duration::from_secs(5);

#[test]
fn core_endpoint_grant_redirect_and_generation_revocation() {
    if std::env::var_os(CHILD).is_none() {
        let home = tempfile::tempdir().unwrap();
        let output = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "core_endpoint_grant_redirect_and_generation_revocation",
                "--nocapture",
            ])
            .env_clear()
            .env(CHILD, "1")
            .env("HOME", home.path())
            .env("XDG_CONFIG_HOME", home.path().join("config"))
            .env("XDG_STATE_HOME", home.path().join("state"))
            .env("XDG_DATA_HOME", home.path().join("data"))
            .env("XDG_CACHE_HOME", home.path().join("cache"))
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        return;
    }
    let server = HttpContract::start(vec![
        Response {
            status: "307 Temporary Redirect".into(),
            headers: vec![
                ("Location".into(), "/redirected".into()),
                ("Content-Length".into(), "0".into()),
            ],
            steps: vec![],
        },
        Response {
            status: "200 OK".into(),
            headers: vec![],
            steps: vec![Step::Chunk(br#"{"data":[{"id":"fixture"}]}"#.to_vec())],
        },
        Response {
            status: "307 Temporary Redirect".into(),
            headers: vec![
                (
                    "Location".into(),
                    "http://169.254.169.254/latest/meta-data/".into(),
                ),
                ("Content-Length".into(), "0".into()),
            ],
            steps: vec![],
        },
    ])
    .unwrap();
    unsafe { std::env::set_var("SYNTHETIC_BASE_URL", format!("http://{}/v1", server.addr)) };
    let state = tempfile::tempdir().unwrap();
    let mut host = PluginHost::with_session_provider_preparer(
        Arc::new(ToolRegistry::new()),
        maki_commands::CommandRegistry::new(),
        true,
        Arc::new(maki_lua::test_support::InMemoryFs::new()),
        Some(state.path().to_path_buf()),
        None,
        maki_lua::ui_sink::HostUi::Frontend,
    )
    .unwrap();
    host.load_builtins(&PluginsConfig {
        enabled: true,
        names: vec!["synthetic".into()],
        ..Default::default()
    })
    .unwrap();
    let snapshot = host.provider_registry().snapshot();
    let factory = &snapshot.get("synthetic").unwrap().factory;
    let config = json!({"api_key": "fixture-key", "api_key_env": "UNUSED_SYNTHETIC_GRANT_KEY", "base_url": format!("http://{}/v1", server.addr)});
    let models = smol::block_on(factory.discover(&config)).unwrap().unwrap();
    assert_eq!(models[0].id, "fixture");
    for target in ["/v1/models", "/redirected"] {
        let request = server.requests.recv_timeout(TIMEOUT).unwrap().unwrap();
        assert_eq!(request.target, target);
        assert_eq!(
            request.header("host"),
            Some(server.addr.to_string().as_str())
        );
        assert_eq!(request.header("authorization"), Some("Bearer fixture-key"));
    }
    assert!(smol::block_on(factory.discover(&config)).is_err());
    assert_eq!(
        server
            .requests
            .recv_timeout(TIMEOUT)
            .unwrap()
            .unwrap()
            .target,
        "/v1/models"
    );
    host.unload("synthetic").unwrap();
    assert!(smol::block_on(factory.discover(&config)).is_err());
}
