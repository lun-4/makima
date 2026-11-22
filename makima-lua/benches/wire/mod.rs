//! A loopback server and a fixture history for meters that time a provider
//! request end to end: the body built, sent and the SSE answer parsed. The
//! same file serves the native providers at the branch base, so the server
//! uses nothing but std.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::thread;

use makima_providers::{ContentBlock, Message, Role};
use serde_json::json;

pub const OPENAI: &str = "bench-openai";
pub const ANTHROPIC: &str = "bench-anthropic";
pub const MODEL: &str = "bench-1";
pub const HISTORY_LENGTHS: [usize; 3] = [100, 1_000, 5_000];
/// A tool result of a few kilobytes, the size of a typical file read.
const TOOL_OUTPUT_BYTES: usize = 4 * 1024;
const CONTENT_LENGTH: &str = "content-length:";

const OPENAI_SSE: &str = concat!(
    "data: {\"id\":\"c\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"ok\"},\"finish_reason\":\"stop\"}],",
    "\"usage\":{\"prompt_tokens\":1,\"completion_tokens\":1}}\n\n",
    "data: [DONE]\n\n",
);

const ANTHROPIC_SSE: &str = concat!(
    "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"m\",\"usage\":{\"input_tokens\":1,\"output_tokens\":0}}}\n\n",
    "event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n",
    "event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"ok\"}}\n\n",
    "event: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":0}\n\n",
    "event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":1}}\n\n",
    "event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n",
);

/// Starts the server and returns its base URL. A request whose path names
/// `messages` gets an Anthropic stream, any other a Chat Completions one.
pub fn serve() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    thread::spawn(move || {
        for stream in listener.incoming().flatten() {
            thread::spawn(move || answer(stream));
        }
    });
    url
}

fn answer(stream: TcpStream) {
    let mut reader = BufReader::new(stream);
    let mut request_line = String::new();
    if reader.read_line(&mut request_line).is_err() {
        return;
    }
    let mut length = 0;
    loop {
        let mut line = String::new();
        if reader.read_line(&mut line).unwrap_or(0) == 0 || line == "\r\n" {
            break;
        }
        if let Some(value) = line.to_ascii_lowercase().strip_prefix(CONTENT_LENGTH) {
            length = value.trim().parse().unwrap_or(0);
        }
    }
    let mut body = vec![0; length];
    if reader.read_exact(&mut body).is_err() {
        return;
    }
    let sse = if request_line.contains("messages") {
        ANTHROPIC_SSE
    } else {
        OPENAI_SSE
    };
    let response = format!(
        "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{sse}",
        sse.len()
    );
    let _ = reader.get_mut().write_all(response.as_bytes());
}

/// `providers.toml` with both bench providers pointed at {url}.
pub fn providers_toml(url: &str) -> String {
    format!(
        "[{OPENAI}]\nprotocol = \"openai\"\nbase_url = \"{url}/v1\"\nmodels = [{{ id = \"{MODEL}\" }}]\n\n\
         [{ANTHROPIC}]\nprotocol = \"anthropic\"\nbase_url = \"{url}\"\nmodels = [{{ id = \"{MODEL}\" }}]\n"
    )
}

/// Points the state and config directories at {dir}, writes the providers
/// file and sets the keys. Call before any other thread starts.
pub fn configure(dir: &std::path::Path, url: &str) {
    makima_storage::paths::init_at(dir.to_path_buf()).unwrap();
    std::fs::write(dir.join("providers.toml"), providers_toml(url)).unwrap();
    for id in [OPENAI, ANTHROPIC] {
        let env = format!("{}_API_KEY", id.to_uppercase().replace('-', "_"));
        // SAFETY: called before the bench starts any thread that reads env.
        unsafe { std::env::set_var(env, "bench-key") };
    }
}

/// Turns of a user prompt, an assistant tool call and its result.
pub fn history(length: usize) -> Vec<Message> {
    let output = "x".repeat(TOOL_OUTPUT_BYTES);
    (0..length)
        .map(|index| {
            let id = format!("call_{index}");
            let (role, content) = match index % 3 {
                0 => (
                    Role::User,
                    vec![ContentBlock::Text {
                        text: format!("step {index}: look at the next file"),
                    }],
                ),
                1 => (
                    Role::Assistant,
                    vec![
                        ContentBlock::Text {
                            text: "Reading it now.".into(),
                        },
                        ContentBlock::ToolUse {
                            id,
                            name: "read".into(),
                            input: json!({ "path": format!("src/file_{index}.rs") }),
                            thought_signature: None,
                        },
                    ],
                ),
                _ => (
                    Role::User,
                    vec![ContentBlock::ToolResult {
                        tool_use_id: id,
                        content: output.clone(),
                        is_error: false,
                    }],
                ),
            };
            Message {
                role,
                content,
                ..Message::default()
            }
        })
        .collect()
}
