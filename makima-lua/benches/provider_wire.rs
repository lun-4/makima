//! A provider request end to end through the bundled providers plugin: the
//! history converted, the body built in Lua, sent to a loopback server and
//! its SSE answer parsed. A copy at the branch base times the native
//! providers on the same server and fixture, for comparison.
//!
//!   cargo bench -p makima-lua --bench provider_wire

mod wire;

use std::sync::Arc;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use makima_agent::tools::ToolRegistry;
use makima_lua::PluginHost;
use makima_providers::{
    Invocation, InvocationPurpose, PreparedProvider, RequestDelivery, RequestOptions, Timeouts,
};
use serde_json::Value;
use tempfile::TempDir;

const SYSTEM: &str = "You are a coding agent.";

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

fn provider_wire(c: &mut Criterion) {
    let dir = TempDir::new().unwrap();
    wire::configure(dir.path(), &wire::serve());
    let host = PluginHost::with_all_builtins(Arc::new(ToolRegistry::new())).unwrap();
    let providers = host.provider_registry(Timeouts::default()).unwrap();
    let prepared: Vec<(&str, PreparedProvider)> = [wire::OPENAI, wire::ANTHROPIC]
        .into_iter()
        .map(|id| {
            let model = providers.model(&format!("{id}/{}", wire::MODEL)).unwrap();
            (id, smol::block_on(providers.prepare(model)).unwrap())
        })
        .collect();
    let tools = Value::Array(Vec::new());
    let invocation = invocation();
    let (events_tx, _events_rx) = flume::unbounded();
    let mut group = c.benchmark_group("provider_wire");
    for length in wire::HISTORY_LENGTHS {
        let messages = wire::history(length);
        group.throughput(Throughput::Elements(length as u64));
        for (id, prepared) in &prepared {
            group.bench_with_input(BenchmarkId::new(*id, length), &messages, |b, messages| {
                b.iter(|| {
                    smol::block_on(prepared.provider.stream_message(
                        &prepared.model,
                        messages,
                        SYSTEM,
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
    drop(host);
}

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(20);
    targets = provider_wire
}
criterion_main!(benches);
