#[path = "support/http_contract.rs"]
mod http_contract;

use std::sync::Arc;
use std::time::Duration;

use http_contract::{HttpContract, Response, Step};
use maki_agent::tools::ToolRegistry;
use maki_config::PluginsConfig;
use maki_lua::PluginHost;
use maki_providers::{
    AgentError, Message, Model, ProviderEvent, RequestOptions, ThinkingConfig, UsageWindow,
};
use serde_json::json;

const TIMEOUT: Duration = Duration::from_secs(5);

fn response(status: &str, body: &str) -> Response {
    Response {
        status: status.into(),
        headers: vec![],
        steps: vec![Step::Chunk(body.as_bytes().to_vec())],
    }
}

#[test]
fn regolo_registration_discovery_usage_and_stream() {
    let server = HttpContract::start(vec![
        response("200 OK", r#"{"data":[{"id":"z"},{"id":"chat"},{"id":"embed"}]}"#),
        response("200 OK", r#"{"data":[{"model_group":"chat","mode":"chat","supports_reasoning":true,"supports_vision":true,"max_input_tokens":64000,"max_output_tokens":4096,"input_cost_per_token":0.000002,"output_cost_per_token":0.000004},{"model_group":"embed","mode":"embedding","supports_reasoning":false,"supports_vision":false}]}"#),
        response("200 OK", r#"{"data":[{"id":"z"},{"id":"a"}]}"#),
        response("503 Service Unavailable", "unavailable"),
        response("401 Unauthorized", "unauthorized"),
        response("200 OK", r#"{"info":{"spend":2.5,"max_budget":10,"budget_reset_at":"2025-01-02T03:04:05Z"}}"#),
        response("200 OK", r#"{"sum_api_requests":3,"sum_total_tokens":42}"#),
        response("200 OK", r#"{"data":[{"model_group":"chat","prompt_tokens":10,"completion_tokens":3,"total_tokens":13,"spend":0.02},{"model_group":"chat","prompt_tokens":5,"completion_tokens":2,"total_tokens":7,"spend":0.01}]}"#),
        response("200 OK", concat!(
            "data: {\"choices\":[{\"delta\":{\"content\":\"hello\"}}]}\n\n",
            "data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":12,\"completion_tokens\":5,\"prompt_tokens_details\":{\"cached_tokens\":4}}}\n\n",
            "data: [DONE]\n\n"
        )),
    ]).unwrap();
    maki_lua::set_allowed_private_hosts(&[server.addr.to_string()]);
    let mut host = PluginHost::new(Arc::new(ToolRegistry::new())).unwrap();
    let defaults = PluginsConfig::from_plugins(Default::default());
    host.load_builtins(&defaults).unwrap();
    assert!(host.provider_registry().snapshot().get("regolo").is_none());
    host.load_builtins(&PluginsConfig {
        enabled: true,
        names: vec!["regolo".into()],
        ..Default::default()
    })
    .unwrap();
    let snapshot = host.provider_registry().snapshot();
    let registration = snapshot.get("regolo").unwrap();
    assert!(
        registration
            .models
            .iter()
            .any(|model| model.id == "qwen3-coder-next")
    );
    let config = json!({"api_key_env":"MAKI_REGOLO_CONTRACT_UNUSED_KEY", "api_key":" first, second ", "base_url":format!("http://{}/v1/", server.addr)});
    let models = smol::block_on(registration.factory.discover(&config))
        .unwrap()
        .unwrap();
    assert_eq!(models.len(), 1);
    assert_eq!(models[0].id, "chat");
    assert_eq!(models[0].context_window, Some(64000));
    assert_eq!(models[0].max_output_tokens, Some(4096));
    assert_eq!(models[0].supports_thinking, Some(true));
    assert_eq!(models[0].supports_vision, Some(true));
    assert_eq!(models[0].pricing.as_ref().unwrap().input, 2.0);
    for path in ["/v1/models", "/model_group/info"] {
        let request = server.requests.recv_timeout(TIMEOUT).unwrap().unwrap();
        assert_eq!(request.method, "GET");
        assert_eq!(request.target, path);
        assert_eq!(request.header("authorization"), Some("Bearer first"));
    }
    let fallback = smol::block_on(registration.factory.discover(&config))
        .unwrap()
        .unwrap();
    assert_eq!(
        fallback.iter().map(|m| m.id.as_str()).collect::<Vec<_>>(),
        ["a", "z"]
    );
    assert_eq!(fallback[0].context_window, None);
    for path in ["/v1/models", "/model_group/info"] {
        let request = server.requests.recv_timeout(TIMEOUT).unwrap().unwrap();
        assert_eq!(request.target, path);
        assert_eq!(request.header("authorization"), Some("Bearer first"));
    }
    let error = smol::block_on(registration.factory.discover(&config)).unwrap_err();
    assert!(
        matches!(error, AgentError::Api { status: 401, .. }),
        "{error:?}"
    );
    let request = server.requests.recv_timeout(TIMEOUT).unwrap().unwrap();
    assert_eq!(request.target, "/v1/models");
    assert_eq!(request.header("authorization"), Some("Bearer first"));
    let model = Model::from_spec("regolo/qwen3-coder-next").unwrap();
    let (model, provider) = smol::block_on(registration.factory.prepare(&model, &config)).unwrap();
    assert_eq!(provider.key_count(), 2);
    assert!(smol::block_on(provider.rotate_key()).unwrap());
    let usage = smol::block_on(provider.fetch_usage()).unwrap().unwrap();
    assert_eq!(usage.limits.len(), 2);
    assert_eq!(usage.limits[0].kind, UsageWindow::Credits);
    assert_eq!(usage.limits[0].percentage, Some(25));
    assert_eq!(usage.limits[0].reset_at, Some(1_735_787_045_000));
    assert_eq!(
        usage.limits[0].detail.as_deref(),
        Some("$2.50 spent of $10.00 budget")
    );
    assert_eq!(usage.limits[1].kind, UsageWindow::Days(1));
    assert_eq!(
        usage.limits[1].detail.as_deref(),
        Some("3 requests · 42 tokens")
    );
    assert_eq!(usage.by_model_today.len(), 1);
    let row = &usage.by_model_today[0];
    assert_eq!(row.model, "chat");
    assert_eq!(
        (
            row.input_tokens,
            row.output_tokens,
            row.total_tokens,
            row.spend_microdollars
        ),
        (15, 5, 20, 30000)
    );
    for path in ["/key/info", "/global/activity", "/spend/logs/v2"] {
        let request = server.requests.recv_timeout(TIMEOUT).unwrap().unwrap();
        assert_eq!(request.method, "GET");
        assert_eq!(request.path_and_query().0, path);
        if path != "/key/info" {
            assert!(
                request
                    .path_and_query()
                    .1
                    .unwrap()
                    .starts_with("start_date=")
            );
        }
        assert_eq!(request.header("authorization"), Some("Bearer second"));
    }
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
    assert_eq!(
        (
            result.usage.input,
            result.usage.output,
            result.usage.cache_read
        ),
        (8, 5, 4)
    );
    assert!(
        matches!(received.try_recv(), Ok(ProviderEvent::TextDelta { text }) if text == "hello")
    );
    let request = server.requests.recv_timeout(TIMEOUT).unwrap().unwrap();
    assert_eq!(request.method, "POST");
    assert_eq!(request.target, "/v1/chat/completions");
    assert_eq!(request.header("authorization"), Some("Bearer second"));
    let body = request.json().unwrap();
    assert_eq!(body["model"], "qwen3-coder-next");
    assert_eq!(body["stream"], true);
    assert_eq!(body["stream_options"]["include_usage"], true);
    maki_lua::set_allowed_private_hosts(&[]);
}
