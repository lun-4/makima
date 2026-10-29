#[path = "support/http_contract.rs"]
mod http_contract;

use std::sync::Arc;
use std::time::Duration;

use http_contract::{HttpContract, Response, Step};
use maki_agent::tools::ToolRegistry;
use maki_commands::CommandRegistry;
use maki_config::PluginsConfig;
use maki_lua::{PluginHost, test_support::InMemoryFs, ui_sink::HostUi};
use maki_providers::{Message, Model, RequestOptions, ThinkingConfig};
use maki_storage::{
    StateDir,
    auth::{ProviderCredentials, load_provider_credentials, save_provider_credentials},
};
use serde_json::{Value, json};

fn response(status: &str, body: &str) -> Response {
    Response {
        status: status.into(),
        headers: vec![],
        steps: vec![Step::Chunk(body.as_bytes().to_vec())],
    }
}

fn requesty_host(state: &std::path::Path) -> PluginHost {
    let mut host = PluginHost::with_session_provider_preparer(
        Arc::new(ToolRegistry::new()),
        CommandRegistry::new(),
        true,
        Arc::new(InMemoryFs::new()),
        Some(state.to_path_buf()),
        None,
        HostUi::Frontend,
    )
    .unwrap();
    host.load_builtins(&PluginsConfig {
        enabled: true,
        names: vec!["requesty".into()],
        ..Default::default()
    })
    .unwrap();
    host
}

#[test]
fn requesty_saved_credentials_take_precedence_and_logout() {
    let temp = tempfile::tempdir().unwrap();
    let state = StateDir::from_path(temp.path().to_path_buf());
    save_provider_credentials(
        &state,
        "requesty",
        &ProviderCredentials {
            api_key: "saved-key".into(),
            host: None,
        },
    )
    .unwrap();
    let host = requesty_host(temp.path());
    let snapshot = host.provider_registry().snapshot();
    let factory = &snapshot.get("requesty").unwrap().factory;
    let config = json!({"api_key_env":"REQUESTY_CONTRACT_UNUSED_KEY", "api_key":"config-key"});
    assert_eq!(
        smol::block_on(factory.auth_status(&config)).unwrap()["source"],
        "stored"
    );
    smol::block_on(factory.logout(&config)).unwrap();
    assert!(load_provider_credentials(&state, "requesty").is_none());
    assert_eq!(
        smol::block_on(factory.auth_status(&config)).unwrap()["source"],
        "config"
    );
    assert_eq!(
        smol::block_on(factory.auth_status(&json!({"api_key_env":"REQUESTY_CONTRACT_UNUSED_KEY"})))
            .unwrap()["configured"],
        false
    );
}

#[test]
fn requesty_discovery_filters_and_merges_catalogs() {
    const CATALOG: &str = r#"{"data":[{"id":"vendor/chat","api":"chat","context_window":0,"max_output_tokens":4096,"input_price":0.000002,"output_price":0.000004,"cached_price":0.000001,"supports_reasoning":true,"supports_vision":true},{"id":"vendor/embed","api":"embedding"},{"id":"unknown","input_price":1}]}"#;
    let server = HttpContract::start(vec![
        response("200 OK", CATALOG),
        response("200 OK", CATALOG),
    ])
    .unwrap();
    maki_lua::set_allowed_private_hosts(&[server.addr.to_string()]);
    let state = tempfile::tempdir().unwrap();
    let host = requesty_host(state.path());
    let snapshot = host.provider_registry().snapshot();
    let models = smol::block_on(snapshot.get("requesty").unwrap().factory.discover(&json!({
        "api_key": "fixture-key", "api_key_env": "REQUESTY_CONTRACT_UNUSED_KEY",
        "base_url": format!("http://{}/v1/", server.addr)
    })))
    .unwrap()
    .unwrap();
    assert_eq!(models.len(), 2);
    let chat = models.iter().find(|m| m.id == "vendor/chat").unwrap();
    assert_eq!(chat.context_window, None);
    assert_eq!(chat.max_output_tokens, Some(4096));
    assert_eq!(chat.supports_thinking, Some(true));
    assert_eq!(chat.supports_vision, Some(true));
    let pricing = chat.pricing.as_ref().unwrap();
    assert_eq!(
        (
            pricing.input,
            pricing.output,
            pricing.cache_read,
            pricing.cache_write
        ),
        (2.0, 4.0, 1.0, 0.0)
    );
    assert!(
        models
            .iter()
            .find(|m| m.id == "unknown")
            .unwrap()
            .pricing
            .is_none()
    );
    let mut paths = Vec::new();
    for _ in 0..2 {
        let request = server
            .requests
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
            .unwrap();
        assert_eq!(request.method, "GET");
        assert_eq!(request.header("authorization"), Some("Bearer fixture-key"));
        paths.push(request.target);
    }
    paths.sort();
    assert_eq!(paths, ["/v1/models", "/v1/models/managed"]);
}

#[test]
fn requesty_discovery_tolerates_one_failure_and_rejects_two() {
    const CATALOG: &str = r#"{"data":[{"id":"available","context_window":8192}]}"#;
    for statuses in [
        ["200 OK", "503 Service Unavailable"],
        ["503 Service Unavailable", "503 Service Unavailable"],
    ] {
        let server = HttpContract::start(
            statuses
                .into_iter()
                .map(|status| response(status, CATALOG))
                .collect(),
        )
        .unwrap();
        maki_lua::set_allowed_private_hosts(&[server.addr.to_string()]);
        let state = tempfile::tempdir().unwrap();
        let host = requesty_host(state.path());
        let snapshot = host.provider_registry().snapshot();
        let result = smol::block_on(snapshot.get("requesty").unwrap().factory.discover(&json!({
            "api_key": "fixture-key", "api_key_env": "REQUESTY_CONTRACT_UNUSED_KEY",
            "base_url": format!("http://{}/v1", server.addr)
        })));
        if statuses[0] == "200 OK" {
            let models = result.unwrap().unwrap();
            assert_eq!(models.len(), 1);
            assert_eq!(models[0].id, "available");
        } else {
            assert!(result.is_err());
        }
        for _ in 0..2 {
            let request = server
                .requests
                .recv_timeout(Duration::from_secs(5))
                .unwrap()
                .unwrap();
            assert_eq!(request.header("authorization"), Some("Bearer fixture-key"));
        }
    }
}

#[test]
fn requesty_stream_sets_auto_cache_and_rotates_keys() {
    const SSE: &str = "data: {\"choices\":[{\"delta\":{\"content\":\"hello\"},\"finish_reason\":\"stop\"}]}\n\ndata: {\"choices\":[],\"usage\":{\"prompt_tokens\":12,\"completion_tokens\":5,\"prompt_tokens_details\":{\"cached_tokens\":4}}}\n\ndata: [DONE]\n\n";
    let server =
        HttpContract::start(vec![response("200 OK", SSE), response("200 OK", SSE)]).unwrap();
    maki_lua::set_allowed_private_hosts(&[server.addr.to_string()]);
    let state = tempfile::tempdir().unwrap();
    let host = requesty_host(state.path());
    let snapshot = host.provider_registry().snapshot();
    let model = Model::from_spec("requesty/openai/gpt-5.5").unwrap();
    let (model, provider) = smol::block_on(snapshot.get("requesty").unwrap().factory.prepare(
        &model,
        &json!({
            "api_key": " first, second ", "api_key_env": "REQUESTY_CONTRACT_UNUSED_KEY",
            "base_url": format!("http://{}/v1", server.addr)
        }),
    ))
    .unwrap();
    assert_eq!(provider.key_count(), 2);
    for key in ["first", "second"] {
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
        assert_eq!(
            (
                result.usage.input,
                result.usage.output,
                result.usage.cache_read
            ),
            (8, 5, 4)
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
        assert_eq!(request.header("x-title"), Some("Makima"));
        assert_eq!(request.header("http-referer"), Some("https://maki.sh"));
        let body: Value = serde_json::from_slice(&request.body).unwrap();
        assert_eq!(body["requesty"]["auto_cache"], true);
        assert_eq!(body["stream_options"]["include_usage"], true);
        if key == "first" {
            assert!(smol::block_on(provider.rotate_key()).unwrap());
        }
    }
}
