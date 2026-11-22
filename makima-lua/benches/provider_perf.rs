//! What a Lua provider's request costs the host before the provider sends
//! anything: the history copied out of the agent, converted to Lua tables on
//! the shared Lua thread, and the result converted back.
//!
//!   cargo bench -p makima-lua --bench provider_perf
//!
//! `convert` returns at once, so it measures the host alone. `encode` also
//! JSON-encodes every message in Lua, which is what each codec does to build
//! its request body, so the two together show how the host's share compares
//! with the work a provider cannot avoid.

use std::sync::Arc;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use makima_agent::tools::ToolRegistry;
use makima_lua::PluginHost;
use makima_providers::{
    ContentBlock, Invocation, InvocationPurpose, Message, PreparedProvider, RequestDelivery,
    RequestOptions, Role, Timeouts,
};
use serde_json::{Value, json};

const PLUGIN: &str = "bench_provider";
const SPEC: &str = "bench/bench-1";
const HISTORY_LENGTHS: [usize; 3] = [100, 1_000, 5_000];
/// A tool result of a few kilobytes, the size of a typical file read.
const TOOL_OUTPUT_BYTES: usize = 4 * 1024;
const SYSTEM: &str = "You are a coding agent.";
/// The system prompt that makes the fake provider encode the history.
const ENCODE: &str = "encode";

const SOURCE: &str = r#"
    local DONE = {
        message = { role = "assistant", content = { { type = "text", text = "ok" } } },
        usage = { input_tokens = 1, output_tokens = 1 },
        stop_reason = "end_turn",
    }
    makima.api.register_provider({
        id = "bench",
        display_name = "Bench",
        models = { { id = "bench-1", context_window = 1000000, tier = "strong" } },
        prepare = function()
            return {
                stream = function(ctx, request, emit)
                    if request.system == "encode" then
                        for _, message in ipairs(request.messages) do
                            makima.json.encode(message)
                        end
                    end
                    return DONE
                end,
            }
        end,
    })
"#;

fn prepared() -> (PluginHost, PreparedProvider) {
    let host = PluginHost::new(Arc::new(ToolRegistry::new())).unwrap();
    host.load_source(PLUGIN, SOURCE).unwrap();
    let providers = host.provider_registry(Timeouts::default()).unwrap();
    let model = providers.model(SPEC).unwrap();
    let prepared = smol::block_on(providers.prepare(model)).unwrap();
    (host, prepared)
}

/// Turns of a user prompt, an assistant tool call and its result.
fn history(length: usize) -> Vec<Message> {
    let output = "x".repeat(TOOL_OUTPUT_BYTES);
    (0..length)
        .map(|index| {
            let id = format!("call_{index}");
            match index % 3 {
                0 => Message {
                    role: Role::User,
                    content: vec![ContentBlock::Text {
                        text: format!("step {index}: look at the next file"),
                    }],
                    ..Message::default()
                },
                1 => Message {
                    role: Role::Assistant,
                    content: vec![
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
                    ..Message::default()
                },
                _ => Message {
                    role: Role::User,
                    content: vec![ContentBlock::ToolResult {
                        tool_use_id: id,
                        content: output.clone(),
                        is_error: false,
                    }],
                    ..Message::default()
                },
            }
        })
        .collect()
}

fn invocation() -> Invocation {
    Invocation {
        conversation_id: "bench-conversation".into(),
        operation_id: "bench-operation".into(),
        attempt: 0,
        purpose: InvocationPurpose::Agent,
        agent_id: None,
        previous: None,
    }
}

fn provider_request(c: &mut Criterion) {
    let (_host, prepared) = prepared();
    let tools = Value::Array(Vec::new());
    let invocation = invocation();
    let (events_tx, _events_rx) = flume::unbounded();
    let mut group = c.benchmark_group("provider_request");
    for length in HISTORY_LENGTHS {
        let messages = history(length);
        group.throughput(Throughput::Elements(length as u64));
        for (meter, system) in [("convert", SYSTEM), (ENCODE, ENCODE)] {
            group.bench_with_input(BenchmarkId::new(meter, length), &messages, |b, messages| {
                b.iter(|| {
                    smol::block_on(prepared.provider.stream_message(
                        &prepared.model,
                        messages,
                        system,
                        &tools,
                        &events_tx,
                        RequestOptions::default(),
                        None,
                        &invocation,
                        RequestDelivery::untracked(),
                    ))
                    .unwrap()
                });
            });
        }
    }
    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(20);
    targets = provider_request
}
criterion_main!(benches);
