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
        ];
        let mut requests = Vec::new();
        for response in responses {
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
            write!(
                socket,
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
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
    let requests = server.join().unwrap();
    maki_lua::set_allowed_private_hosts(&[]);
    let (headers, body) = requests[0].split_once("\r\n\r\n").unwrap();
    assert!(headers.starts_with("POST /v1/models/gemini-2.5-pro:streamGenerateContent?alt=sse "));
    assert!(
        headers
            .to_ascii_lowercase()
            .contains("x-goog-api-key: fixture key\r\n")
    );
    let body: Value = serde_json::from_str(body).unwrap();
    assert_eq!(body["systemInstruction"]["parts"][0]["text"], "system");
    assert_eq!(body["contents"][0]["parts"][0]["text"], "question");
    assert!(requests[1].starts_with("GET /v1/models?key=fixture%20key&pageSize=1000 "));
    assert!(
        requests[1]
            .to_ascii_lowercase()
            .contains("x-goog-api-key: fixture key\r\n")
    );
}
