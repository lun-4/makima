//! Every headless frontend (`--print`, SDK stream-json, ACP) must send the
//! `always_thinking` level configured with `maki.setup`, not the session's
//! blank default. Runs the real binary against a loopback OpenAI-compatible
//! server and inspects the request body.
//!
//! Unix only: the isolated environment points `HOME` and the XDG dirs at a
//! temp dir, which is not how Windows finds its config.
#![cfg(unix)]

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpListener;
use std::path::Path;
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::mpsc::{self, Receiver};
use std::time::Duration;

use serde_json::{Value, json};
use tempfile::TempDir;
use test_case::test_case;

const PROVIDER: &str = "mock";
const API_KEY_ENV: &str = "MOCK_API_KEY";
const MODEL: &str = "thinker";
const PROMPT: &str = "say ok";
const EFFORT: &str = "high";
const OFF_EFFORT: &str = "none";
const TIMEOUT: Duration = Duration::from_secs(60);
const NO_REQUEST: &str = "no chat request reached the mock server";
const SSE_REPLY: &str = concat!(
    "data: {\"id\":\"c\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"index\":0,",
    "\"delta\":{\"role\":\"assistant\",\"content\":\"ok\"},\"finish_reason\":null}]}\n\n",
    "data: {\"id\":\"c\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"index\":0,",
    "\"delta\":{},\"finish_reason\":\"stop\"}],",
    "\"usage\":{\"prompt_tokens\":1,\"completion_tokens\":1,\"total_tokens\":2}}\n\n",
    "data: [DONE]\n\n",
);

#[derive(Debug, Clone, Copy)]
enum Frontend {
    Print,
    Sdk,
    Acp,
}

#[test_case(Frontend::Print, Some(EFFORT) => EFFORT ; "print_sends_always_thinking")]
#[test_case(Frontend::Print, None => OFF_EFFORT ; "print_without_always_thinking_stays_off")]
#[test_case(Frontend::Sdk, Some(EFFORT) => EFFORT ; "sdk_sends_always_thinking")]
#[test_case(Frontend::Acp, Some(EFFORT) => EFFORT ; "acp_sends_always_thinking")]
fn request_carries_always_thinking(frontend: Frontend, always_thinking: Option<&str>) -> String {
    let body = first_request(PROVIDER, MODEL, frontend, always_thinking);
    if always_thinking.is_some() {
        assert_eq!(
            body["thinking"],
            Value::Null,
            "thinking must not be disabled"
        );
    }
    body["reasoning_effort"]
        .as_str()
        .unwrap_or_default()
        .to_owned()
}

/// Runs one turn through `frontend` with an isolated home and returns the
/// first chat-completions body it sent.
fn first_request(
    provider: &str,
    model: &str,
    frontend: Frontend,
    always_thinking: Option<&str>,
) -> Value {
    let (base_url, bodies) = mock_server();
    let home = TempDir::new().unwrap();
    write_config(home.path(), provider, model, &base_url, always_thinking);
    let work = home.path().join("work");
    std::fs::create_dir_all(&work).unwrap();

    let spec = format!("{provider}/{model}");
    let mut child = spawn_makima(home.path(), &work, frontend_args(frontend, &spec));
    let mut stdin = child.stdin.take().unwrap();
    match frontend {
        Frontend::Print => drop(stdin),
        Frontend::Sdk => {
            send(
                &mut stdin,
                json!({"type": "user", "message": {"role": "user", "content": PROMPT}}),
            );
            drop(stdin);
        }
        Frontend::Acp => {
            acp_prompt(&mut child, &mut stdin, &work);
        }
    }
    let body = bodies.recv_timeout(TIMEOUT);
    // SDK and ACP sessions outlive their turn, so stop the child rather than
    // wait for it.
    let _ = child.kill();
    let output = child.wait_with_output().unwrap();
    body.unwrap_or_else(|_| {
        panic!(
            "{NO_REQUEST}; makima stderr:\n{}",
            String::from_utf8_lossy(&output.stderr)
        )
    })
}

fn frontend_args(frontend: Frontend, spec: &str) -> Vec<String> {
    let args: &[&str] = match frontend {
        Frontend::Print => &[
            "--print",
            "--yolo",
            "--max-turns",
            "1",
            "-m",
            spec,
            "-p",
            PROMPT,
        ],
        Frontend::Sdk => &[
            "--print",
            "--yolo",
            "--max-turns",
            "1",
            "-m",
            spec,
            "--input-format",
            "stream-json",
            "--output-format",
            "stream-json",
        ],
        Frontend::Acp => &["acp", "--yolo", "-m", spec],
    };
    args.iter().map(|arg| arg.to_string()).collect()
}

/// `initialize`, `session/new`, then `session/prompt` on the new session id.
fn acp_prompt(child: &mut Child, stdin: &mut ChildStdin, cwd: &Path) {
    let responses = json_lines(child.stdout.take().unwrap());
    send(
        stdin,
        json!({"jsonrpc": "2.0", "id": 1, "method": "initialize",
               "params": {"protocolVersion": 1, "clientCapabilities": {}}}),
    );
    send(
        stdin,
        json!({"jsonrpc": "2.0", "id": 2, "method": "session/new",
               "params": {"cwd": cwd, "mcpServers": []}}),
    );
    let session = loop {
        let message = responses
            .recv_timeout(TIMEOUT)
            .expect("session/new response");
        if message["id"] == 2 {
            break message["result"]["sessionId"].clone();
        }
    };
    send(
        stdin,
        json!({"jsonrpc": "2.0", "id": 3, "method": "session/prompt",
               "params": {"sessionId": session, "prompt": [{"type": "text", "text": PROMPT}]}}),
    );
}

fn send(stdin: &mut ChildStdin, message: Value) {
    writeln!(stdin, "{message}").unwrap();
    stdin.flush().unwrap();
}

fn json_lines(stdout: impl Read + Send + 'static) -> Receiver<Value> {
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        for line in BufReader::new(stdout).lines().map_while(Result::ok) {
            if let Ok(message) = serde_json::from_str(&line)
                && tx.send(message).is_err()
            {
                break;
            }
        }
    });
    rx
}

fn write_config(
    home: &Path,
    provider: &str,
    model: &str,
    base_url: &str,
    always_thinking: Option<&str>,
) {
    let config = home.join(".config/makima");
    std::fs::create_dir_all(&config).unwrap();
    std::fs::write(
        config.join("providers.toml"),
        format!(
            "[{provider}]\nprotocol = \"openai\"\nbase_url = \"{base_url}\"\n\
             api_key_env = \"{API_KEY_ENV}\"\ndiscover_models = false\n\n\
             [[{provider}.models]]\nid = \"{model}\"\ntier = \"strong\"\n\
             context_window = 32768\nmax_output_tokens = 4096\nsupports_thinking = true\n"
        ),
    )
    .unwrap();
    let setup = always_thinking
        .map(|level| format!("always_thinking = \"{level}\""))
        .unwrap_or_default();
    std::fs::write(
        config.join("init.lua"),
        format!("maki.setup({{ {setup} }})\n"),
    )
    .unwrap();
}

fn spawn_makima(home: &Path, work: &Path, args: Vec<String>) -> Child {
    Command::new(env!("CARGO_BIN_EXE_makima"))
        .args(args)
        .current_dir(work)
        .env_clear()
        .env("PATH", std::env::var_os("PATH").unwrap_or_default())
        .env("HOME", home)
        .env("XDG_CONFIG_HOME", home.join(".config"))
        .env("XDG_STATE_HOME", home.join(".local/state"))
        .env("XDG_DATA_HOME", home.join(".local/share"))
        .env("XDG_CACHE_HOME", home.join(".cache"))
        .env(API_KEY_ENV, "test")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap()
}

/// Serves chat completions on loopback and forwards each request body.
fn mock_server() -> (String, Receiver<Value>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let base = format!("http://{}/v1", listener.local_addr().unwrap());
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { continue };
            let mut reader = BufReader::new(stream.try_clone().unwrap());
            let mut request_line = String::new();
            if reader.read_line(&mut request_line).is_err() {
                continue;
            }
            let mut length = 0;
            loop {
                let mut header = String::new();
                if reader.read_line(&mut header).unwrap_or(0) == 0 || header == "\r\n" {
                    break;
                }
                if let Some((name, value)) = header.split_once(':')
                    && name.eq_ignore_ascii_case("content-length")
                {
                    length = value.trim().parse().unwrap_or(0);
                }
            }
            let mut body = vec![0; length];
            let _ = reader.read_exact(&mut body);
            if request_line.contains("/chat/completions") {
                if let Ok(json) = serde_json::from_slice(&body) {
                    let _ = tx.send(json);
                }
                let _ = write!(
                    stream,
                    "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nConnection: close\r\n\r\n{SSE_REPLY}"
                );
            } else {
                let _ = write!(
                    stream,
                    "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                );
            }
        }
    });
    (base, rx)
}
