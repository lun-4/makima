#[path = "support/http_contract.rs"]
mod http_contract;

use std::time::Duration;

use http_contract::{HttpContract, Response, Step};
use isahc::config::{Configurable, ResolveMap};
use isahc::{HttpClient, ReadResponseExt};

const TIMEOUT: Duration = Duration::from_secs(5);
const LOGICAL_HOST: &str = "fixture.invalid";

#[test]
fn logical_endpoint_routing_preserves_request_authority() {
    let server = HttpContract::start(vec![Response {
        status: "200 OK".into(),
        headers: vec![("Content-Length".into(), "2".into())],
        steps: vec![Step::Chunk(b"ok".to_vec())],
    }])
    .unwrap();
    let client = HttpClient::builder()
        .proxy(None)
        .dns_resolve(ResolveMap::new().add(LOGICAL_HOST, server.addr.port(), server.addr.ip()))
        .timeout(TIMEOUT)
        .build()
        .unwrap();
    let url = format!("http://{LOGICAL_HOST}:{}/auth?step=1", server.addr.port());
    let mut response = client.get(&url).unwrap();
    assert_eq!(response.text().unwrap(), "ok");
    let request = server.requests.recv_timeout(TIMEOUT).unwrap().unwrap();
    assert_eq!(request.method, "GET");
    assert_eq!(request.path_and_query(), ("/auth", Some("step=1")));
    assert_eq!(
        request.header("host"),
        Some(format!("{LOGICAL_HOST}:{}", server.addr.port()).as_str())
    );
}
