use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::Arc;
use std::time::{Duration, Instant};

use maki_agent::tools::ToolRegistry;
use maki_lua::PluginHost;
use maki_providers::{Message, Model, ProviderEvent, RequestOptions, StopReason, ThinkingConfig};
use serde_json::{Value, json};

#[test]
fn google_direct_host_stream_and_discovery_use_http_contract() {
    const CHILD: &str = "MAKI_GOOGLE_CONTRACT_CHILD";
    if std::env::var_os(CHILD).is_none() {
        let state = tempfile::tempdir().unwrap();
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "google_direct_host_stream_and_discovery_use_http_contract",
                "--nocapture",
            ])
            .env_clear()
            .env(CHILD, "1")
            .env("HOME", state.path())
            .env("XDG_STATE_HOME", state.path())
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
    maki_storage::paths::init().unwrap();
    let state = maki_storage::StateDir::resolve().unwrap();
    maki_storage::auth::save_provider_credentials(
        &state,
        "google",
        &maki_storage::auth::ProviderCredentials {
            api_key: "saved-first".into(),
            host: None,
        },
    )
    .unwrap();
    assert!(
        std::env::var("GOOGLE_BASE_URL")
            .unwrap_or_default()
            .is_empty(),
        "GOOGLE_BASE_URL must be unset for the local HTTP contract test"
    );
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    maki_lua::set_allowed_private_hosts(&[address.to_string()]);
    listener.set_nonblocking(true).unwrap();
    let server = std::thread::spawn(move || {
        let responses = [
            "data: {\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"hello\"}]},\"finishReason\":\"STOP\"}],\"usageMetadata\":{\"promptTokenCount\":10,\"cachedContentTokenCount\":3,\"candidatesTokenCount\":4}}\n\n",
            "{\"models\":[{\"name\":\"models/z\",\"supportedGenerationMethods\":[\"generateContent\"]},{\"name\":\"models/skip\",\"supportedGenerationMethods\":[\"embedContent\"]},{\"name\":\"models/a\",\"supportedGenerationMethods\":[\"generateContent\"]}]}",
            "too many tokens",
            "data: {\"candidates\":[{\"finishReason\":\"STOP\"}]}\n\n",
        ];
        let mut requests = Vec::new();
        for (index, response) in responses.into_iter().enumerate() {
            let deadline = Instant::now() + Duration::from_secs(5);
            let mut socket = loop {
                match listener.accept() {
                    Ok((socket, _)) => break socket,
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        assert!(Instant::now() < deadline, "provider did not send request");
                        std::thread::yield_now();
                    }
                    Err(error) => panic!("accept failed: {error}"),
                }
            };
            socket
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let mut bytes = Vec::new();
            let mut buffer = [0; 4096];
            loop {
                let n = socket.read(&mut buffer).unwrap();
                assert_ne!(n, 0);
                bytes.extend_from_slice(&buffer[..n]);
                if let Some(end) = bytes.windows(4).position(|part| part == b"\r\n\r\n") {
                    let headers = String::from_utf8_lossy(&bytes[..end]);
                    let length = headers
                        .lines()
                        .find_map(|line| {
                            let (name, value) = line.split_once(':')?;
                            name.eq_ignore_ascii_case("content-length")
                                .then(|| value.trim().parse::<usize>().unwrap())
                        })
                        .unwrap_or(0);
                    if bytes.len() >= end + 4 + length {
                        break;
                    }
                }
            }
            let status = if index == 2 {
                "429 Too Many Requests\r\nRetry-After: 3"
            } else {
                "200 OK"
            };
            write!(
                socket,
                "HTTP/1.1 {}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                status,
                response.len(),
                response
            )
            .unwrap();
            requests.push(String::from_utf8(bytes).unwrap());
        }
        requests
    });

    let host = PluginHost::new(Arc::new(ToolRegistry::new())).unwrap();
    host.load_source(
        "google_contract",
        include_str!("../../plugins/google/init.lua"),
    )
    .unwrap();
    let registry = host.provider_registry();
    let snapshot = registry.snapshot();
    let factory = &snapshot.get("google").unwrap().factory;
    let config = json!({"api_key_env": "MAKI_GOOGLE_CONTRACT_UNSET_KEY", "api_key": "fixture key", "base_url": format!("http://{address}/v1")});
    let model = Model::from_spec("google/gemini-2.5-pro").unwrap();
    let (model, provider) = smol::block_on(factory.prepare(&model, &config)).unwrap();
    let (events, received) = flume::unbounded();
    let result = smol::block_on(provider.stream_message(
        &model,
        &[Message::user("question".into())],
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
    assert!(
        matches!(received.try_recv(), Ok(ProviderEvent::TextDelta { text }) if text == "hello")
    );
    assert_eq!(result.message.first_text_content(), Some("hello"));
    assert_eq!(
        (
            result.usage.input,
            result.usage.output,
            result.usage.cache_read
        ),
        (7, 4, 3)
    );
    assert_eq!(result.stop_reason, Some(StopReason::EndTurn));
    let models = smol::block_on(factory.discover(&config)).unwrap().unwrap();
    assert_eq!(
        models
            .iter()
            .map(|model| model.id.as_str())
            .collect::<Vec<_>>(),
        ["a", "z"]
    );
    let error = smol::block_on(provider.stream_message(
        &model,
        &[Message::user("question".into())],
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
        matches!(error, maki_providers::AgentError::Api { status: 429, ref message, retry_after: Some(delay) } if message == "too many tokens" && delay == Duration::from_secs(3))
    );
    maki_storage::auth::save_provider_credentials(
        &state,
        "google",
        &maki_storage::auth::ProviderCredentials {
            api_key: "saved-second".into(),
            host: None,
        },
    )
    .unwrap();
    smol::block_on(provider.reload_auth()).unwrap();
    smol::block_on(provider.stream_message(
        &model,
        &[Message::user("question".into())],
        "",
        &json!([]),
        &events,
        RequestOptions {
            thinking: ThinkingConfig::Off,
            fast: false,
        },
        None,
    ))
    .unwrap();
    let requests = server.join().unwrap();
    maki_lua::set_allowed_private_hosts(&[]);
    let (headers, body) = requests[0].split_once("\r\n\r\n").unwrap();
    assert!(headers.starts_with("POST /v1/models/gemini-2.5-pro:streamGenerateContent?alt=sse "));
    assert!(
        headers
            .to_ascii_lowercase()
            .contains("x-goog-api-key: saved-first\r\n")
    );
    let body: Value = serde_json::from_str(body).unwrap();
    assert_eq!(body["systemInstruction"]["parts"][0]["text"], "system");
    assert_eq!(body["contents"][0]["parts"][0]["text"], "question");
    assert!(requests[1].starts_with("GET /v1/models?key=saved-first&pageSize=1000 "));
    assert!(
        requests[1]
            .to_ascii_lowercase()
            .contains("x-goog-api-key: saved-first\r\n")
    );
    assert!(
        requests[3]
            .to_ascii_lowercase()
            .contains("x-goog-api-key: saved-second\r\n")
    );
}
