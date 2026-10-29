#[path = "support/http_contract.rs"]
mod http_contract;

use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use flate2::Compression;
use flate2::write::GzEncoder;
use http_contract::{HttpContract, Response, Step};
use test_case::test_case;

const TIMEOUT: Duration = Duration::from_secs(5);

#[test]
fn rejects_oversized_headers_even_when_terminator_arrives_in_final_read() {
    let server = HttpContract::start(vec![Response {
        status: "200 OK".into(),
        headers: Vec::new(),
        steps: Vec::new(),
    }])
    .unwrap();
    let mut stream = TcpStream::connect(server.addr).unwrap();
    let mut request = b"GET / HTTP/1.1\r\nX-Large: ".to_vec();
    request.extend(vec![b'a'; 64 * 1024]);
    request.extend_from_slice(b"\r\n\r\n");
    let _ = stream.write_all(&request);
    let error = server.requests.recv_timeout(TIMEOUT).unwrap().unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
}

#[test_case("GZip", 1, false, None; "gzip")]
#[test_case("gzip, identity", 1, false, None; "gzip_with_identity")]
#[test_case("gzip, gzip", 2, false, None; "stacked_gzip")]
#[test_case("gzip", 1, true, Some("corrupt"); "corrupt_gzip")]
#[test_case("Zstd", 1, false, None; "zstd")]
#[test_case("gzip, zstd", 2, false, None; "mixed_layers")]
#[test_case("zstd", 1, true, Some("corrupt"); "corrupt_zstd")]
#[test_case("unknown", 1, false, Some("unsupported content encoding"); "unsupported_encoding")]
fn decodes_or_rejects_content_encoding(
    encoding: &str,
    layers: usize,
    corrupt: bool,
    expected_error: Option<&str>,
) {
    let server = HttpContract::start(vec![Response {
        status: "200 OK".into(),
        headers: Vec::new(),
        steps: Vec::new(),
    }])
    .unwrap();
    let original = b"{\"binary\":\"payload\"}\0\xff";
    let mut body = original.to_vec();
    for layer in encoding
        .split(',')
        .map(str::trim)
        .filter(|e| !e.eq_ignore_ascii_case("identity"))
        .take(layers)
    {
        if layer.eq_ignore_ascii_case("zstd") {
            body = zstd::stream::encode_all(body.as_slice(), 0).unwrap();
        } else {
            let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
            encoder.write_all(&body).unwrap();
            body = encoder.finish().unwrap();
        }
    }
    if corrupt {
        body.truncate(5);
    }
    let mut stream = TcpStream::connect(server.addr).unwrap();
    write!(
        stream,
        "POST /encoded HTTP/1.1\r\nContent-Encoding: {encoding}\r\nContent-Length: {}\r\n\r\n",
        body.len()
    )
    .unwrap();
    stream.write_all(&body).unwrap();
    let result = server.requests.recv_timeout(TIMEOUT).unwrap();
    if let Some(expected) = expected_error {
        let error = result.unwrap_err();
        if !corrupt {
            assert_eq!(error.to_string(), expected);
        }
    } else {
        let request = result.unwrap();
        assert_eq!(request.header("content-encoding"), Some(encoding));
        assert_eq!(request.body, original);
    }
}

#[test_case("gzip"; "gzip")]
#[test_case("zstd"; "zstd")]
fn rejects_oversized_decoded_body(encoding: &str) {
    let server = HttpContract::start(vec![Response {
        status: "200 OK".into(),
        headers: Vec::new(),
        steps: Vec::new(),
    }])
    .unwrap();
    let original = vec![0; 16 * 1024 * 1024 + 1];
    let body = if encoding == "zstd" {
        zstd::stream::encode_all(original.as_slice(), 0).unwrap()
    } else {
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(&original).unwrap();
        encoder.finish().unwrap()
    };
    let mut stream = TcpStream::connect(server.addr).unwrap();
    write!(
        stream,
        "POST / HTTP/1.1\r\nContent-Encoding: {encoding}\r\nContent-Length: {}\r\n\r\n",
        body.len()
    )
    .unwrap();
    stream.write_all(&body).unwrap();
    let error = server.requests.recv_timeout(TIMEOUT).unwrap().unwrap_err();
    assert_eq!(error.to_string(), "decoded body too large");
}

#[test_case("Content-Length: 1\r\nContent-Length: 1\r\n", "duplicate content length"; "duplicate_length")]
#[test_case("Content-Length: 16777217\r\n", "body too large"; "oversized_wire_body")]
fn rejects_invalid_body_framing(headers: &str, expected: &str) {
    let server = HttpContract::start(vec![Response {
        status: "200 OK".into(),
        headers: Vec::new(),
        steps: Vec::new(),
    }])
    .unwrap();
    let mut stream = TcpStream::connect(server.addr).unwrap();
    write!(stream, "POST / HTTP/1.1\r\n{headers}\r\n").unwrap();
    let error = server.requests.recv_timeout(TIMEOUT).unwrap().unwrap_err();
    assert_eq!(error.to_string(), expected);
}

#[test]
fn captures_json_semantically_and_preserves_query() {
    let server = HttpContract::start(vec![Response {
        status: "200 OK".into(),
        headers: Vec::new(),
        steps: Vec::new(),
    }])
    .unwrap();
    let body = br#"{ "items": [2, 1], "name": "test" }"#;
    let mut stream = TcpStream::connect(server.addr).unwrap();
    write!(
        stream,
        "POST /api?x=2&x=1 HTTP/1.1\r\nContent-Length: {}\r\n\r\n",
        body.len()
    )
    .unwrap();
    stream.write_all(body).unwrap();
    let request = server.requests.recv_timeout(TIMEOUT).unwrap().unwrap();
    assert_eq!(request.path_and_query(), ("/api", Some("x=2&x=1")));
    assert_eq!(
        request.json().unwrap(),
        serde_json::json!({"name": "test", "items": [2, 1]})
    );
}

#[test]
fn shutdown_interrupts_a_gated_response() {
    let (reached_tx, reached_rx) = mpsc::channel();
    let (_release, gate) = mpsc::channel();
    let server = HttpContract::start(vec![Response {
        status: "200 OK".into(),
        headers: Vec::new(),
        steps: vec![Step::Signal(reached_tx), Step::Wait(gate)],
    }])
    .unwrap();
    let mut stream = TcpStream::connect(server.addr).unwrap();
    stream.set_read_timeout(Some(TIMEOUT)).unwrap();
    stream.write_all(b"GET / HTTP/1.1\r\n\r\n").unwrap();
    server.requests.recv_timeout(TIMEOUT).unwrap().unwrap();
    reached_rx.recv_timeout(TIMEOUT).unwrap();
    let (stopped_tx, stopped_rx) = mpsc::channel();
    let thread = thread::spawn(move || {
        drop(server);
        stopped_tx.send(()).unwrap();
    });
    stopped_rx.recv_timeout(TIMEOUT).unwrap();
    thread.join().unwrap();
    let mut response = Vec::new();
    stream.read_to_end(&mut response).unwrap();
    assert_eq!(response, b"HTTP/1.1 200 OK\r\nConnection: close\r\n\r\n");
}

#[test]
fn captures_sequential_requests_and_controls_binary_responses() {
    let (release, gate) = mpsc::channel();
    let server = HttpContract::start(vec![
        Response {
            status: "201 Created".into(),
            headers: vec![
                ("Content-Length".into(), "4".into()),
                ("X-Test".into(), "yes".into()),
            ],
            steps: vec![
                Step::Chunk(vec![0, 255]),
                Step::Wait(gate),
                Step::Chunk(vec![1, 2]),
            ],
        },
        Response {
            status: "503 Unavailable".into(),
            headers: vec![("Content-Length".into(), "9".into())],
            steps: vec![Step::Chunk(vec![42]), Step::Disconnect],
        },
    ])
    .unwrap();
    let addr = server.addr;
    let (first_chunk_tx, first_chunk_rx) = mpsc::channel();
    let client = thread::spawn(move || {
        let mut first = TcpStream::connect(addr).unwrap();
        first.set_read_timeout(Some(TIMEOUT)).unwrap();
        first.write_all(b"POST /first?q=1 HTTP/1.1\r\nHost: example\r\nX-Token: value\r\nContent-Length: 3\r\n\r\n\x00\xffx").unwrap();
        let prefix = b"HTTP/1.1 201 Created\r\nContent-Length: 4\r\nX-Test: yes\r\nConnection: close\r\n\r\n\x00\xff";
        let mut received = vec![0; prefix.len()];
        first.read_exact(&mut received).unwrap();
        assert_eq!(received, prefix);
        first_chunk_tx.send(()).unwrap();
        let mut rest = Vec::new();
        first.read_to_end(&mut rest).unwrap();
        assert_eq!(rest, [1, 2]);

        let mut second = TcpStream::connect(addr).unwrap();
        second.set_read_timeout(Some(TIMEOUT)).unwrap();
        second
            .write_all(b"GET /second HTTP/1.1\r\nHost: example\r\n\r\n")
            .unwrap();
        let mut response = Vec::new();
        second.read_to_end(&mut response).unwrap();
        assert_eq!(
            response,
            b"HTTP/1.1 503 Unavailable\r\nContent-Length: 9\r\nConnection: close\r\n\r\n*"
        );
    });
    let first = server.requests.recv_timeout(TIMEOUT).unwrap().unwrap();
    assert_eq!(first.method, "POST");
    assert_eq!(first.target, "/first?q=1");
    assert_eq!(first.header("x-token"), Some("value"));
    assert_eq!(first.body, [0, 255, b'x']);
    first_chunk_rx.recv_timeout(TIMEOUT).unwrap();
    release.send(()).unwrap();
    let second = server.requests.recv_timeout(TIMEOUT).unwrap().unwrap();
    assert_eq!(second.method, "GET");
    assert_eq!(second.target, "/second");
    assert!(second.body.is_empty());
    client.join().unwrap();
}
