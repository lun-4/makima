//! Nested calls receive complete tool text; the model and its batch children
//! keep the presentation limits.

use std::sync::Arc;
use std::time::Duration;

use makima_agent::agent::tool_dispatch;
use makima_agent::tools::FILE_TRUNCATED_MARKER;
use makima_agent::tools::test_support::stub_ctx;
use makima_agent::tools::{
    CallOrigin, ToolContext, ToolOutputMode, ToolRegistry, TurnToolBindings,
};
use makima_agent::{AgentMode, ToolOutput};
use makima_lua::PluginHost;
use serde_json::{Value, json};

const READ_SRC: &str = include_str!("../../plugins/read/init.lua");
const BATCH_SRC: &str = include_str!("../../plugins/batch/init.lua");
const GLOB_SRC: &str = include_str!("../../plugins/glob/init.lua");
const FIXTURE_SRC: &str = r#"
makima.api.register_tool({
  name = "nested_read",
  description = "nested read fixture",
  schema = { type = "object", properties = { path = { type = "string" } } },
  handler = function(input, ctx)
    return makima.agent.call_tool(ctx, "read", { path = input.path, offset = 1, limit = 0 })
  end,
})

makima.api.register_tool({
  name = "nested_glob",
  description = "nested glob fixture",
  schema = { type = "object", properties = { pattern = { type = "string" }, path = { type = "string" } } },
  handler = function(input, ctx)
    return makima.agent.call_tool(ctx, "glob", { pattern = input.pattern, path = input.path })
  end,
})

makima.api.register_tool({
  name = "trailer_probe",
  description = "trailer fixture",
  schema = { type = "object", properties = {}, additionalProperties = false },
  handler = function()
    return {
      llm_output = "a\nb\nc\nd\ne",
      output_limits = { trailer = "TRAILER", max_lines = 2, max_bytes = 4096 },
    }
  end,
})

makima.api.register_tool({
  name = "mode_probe",
  description = "reports the handler's output mode",
  schema = { type = "object", properties = {}, additionalProperties = false },
  handler = function(_, ctx)
    return ctx:output_mode()
  end,
})
"#;
const FILE_LINES: usize = 8;
const MODEL_CAP_LINES: usize = 3;
const LAST_RECORD: &str = "record-7";
const TRUNCATED_MARKER: &str = "lines remaining";
const DISPATCH_ID: &str = "output-mode-1";
const TRAILER: &str = "TRAILER";
const WATCHDOG: Duration = Duration::from_secs(10);

fn host() -> (Arc<ToolRegistry>, PluginHost) {
    let registry = Arc::new(ToolRegistry::new());
    let host = PluginHost::new(Arc::clone(&registry)).unwrap();
    host.load_source("read", READ_SRC).unwrap();
    host.load_source("batch", BATCH_SRC).unwrap();
    host.load_source("glob", GLOB_SRC).unwrap();
    host.load_source("output_mode_fixture", FIXTURE_SRC)
        .unwrap();
    (registry, host)
}

fn model_ctx(registry: &Arc<ToolRegistry>) -> ToolContext {
    let mut ctx = stub_ctx(&AgentMode::Build);
    ctx.registry = Arc::clone(registry);
    ctx.turn_bindings = Arc::new(TurnToolBindings::capture(registry, &ctx.local_tools, None));
    ctx.config.max_output_lines = MODEL_CAP_LINES;
    ctx
}

fn dispatch(ctx: &ToolContext, name: &str, input: Value) -> ToolOutput {
    let ctx = ctx.clone();
    let label = name.to_owned();
    smol::block_on(async {
        futures_lite::future::race(
            async move {
                tool_dispatch::run(
                    DISPATCH_ID.to_owned(),
                    name,
                    &input,
                    &ctx,
                    CallOrigin::Model,
                )
                .await
            },
            async {
                smol::Timer::after(WATCHDOG).await;
                panic!("{label} did not finish within {WATCHDOG:?}");
            },
        )
        .await
    })
    .output
}

fn read_input(path: &str) -> Value {
    json!({ "path": path, "offset": 1, "limit": 0 })
}

fn write_records() -> (tempfile::TempDir, String) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("records.txt");
    let body: Vec<String> = (0..FILE_LINES).map(|i| format!("record-{i}")).collect();
    std::fs::write(&path, body.join("\n")).unwrap();
    let path = path.to_str().unwrap().to_owned();
    (dir, path)
}

#[test]
fn model_read_keeps_presentation_limits() {
    let (registry, _host) = host();
    let (_dir, path) = write_records();

    let out = dispatch(&model_ctx(&registry), "read", read_input(&path))
        .as_text()
        .to_owned();

    assert!(out.contains("record-0"), "{out}");
    assert!(!out.contains(LAST_RECORD), "{out}");
    assert!(out.contains(TRUNCATED_MARKER), "{out}");
}

#[test]
fn nested_call_tool_returns_full_output() {
    let (registry, _host) = host();
    let (_dir, path) = write_records();

    let out = dispatch(&model_ctx(&registry), "nested_read", read_input(&path))
        .as_text()
        .to_owned();

    for line in 0..FILE_LINES {
        assert!(out.contains(&format!("record-{line}")), "{out}");
    }
    assert!(!out.contains(TRUNCATED_MARKER), "{out}");
}

#[test]
fn batch_children_stay_compact_for_the_model() {
    let (registry, _host) = host();
    let (_dir, path) = write_records();

    let out = dispatch(
        &model_ctx(&registry),
        "batch",
        json!({ "tool_calls": [{ "tool": "read", "parameters": read_input(&path) }] }),
    )
    .as_text()
    .to_owned();

    assert!(out.contains("record-0"), "{out}");
    assert!(!out.contains(LAST_RECORD), "{out}");
    assert!(out.contains(TRUNCATED_MARKER), "{out}");
}

fn write_globs() -> (tempfile::TempDir, String) {
    let dir = tempfile::tempdir().unwrap();
    for i in 0..FILE_LINES {
        std::fs::write(dir.path().join(format!("record-{i}.txt")), "x").unwrap();
    }
    let path = dir.path().to_str().unwrap().to_owned();
    (dir, path)
}

#[test]
fn nested_call_tool_skips_host_presentation_limits() {
    let (registry, _host) = host();
    let (_dir, path) = write_globs();
    let input = json!({ "pattern": "*.txt", "path": path });

    let model = dispatch(&model_ctx(&registry), "glob", input.clone())
        .as_text()
        .to_owned();
    assert!(model.contains(FILE_TRUNCATED_MARKER), "{model}");
    assert!(
        model.matches("record-").count() <= MODEL_CAP_LINES,
        "{model}"
    );

    let nested = dispatch(&model_ctx(&registry), "nested_glob", input)
        .as_text()
        .to_owned();
    for file in 0..FILE_LINES {
        assert!(nested.contains(&format!("record-{file}.txt")), "{nested}");
    }
    assert!(!nested.contains(FILE_TRUNCATED_MARKER), "{nested}");
}

#[test]
fn empty_read_has_no_truncation_marker_in_either_mode() {
    let (registry, _host) = host();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("empty.txt");
    std::fs::write(&path, "").unwrap();
    let path = path.to_str().unwrap().to_owned();

    for tool in ["read", "nested_read"] {
        let out = dispatch(&model_ctx(&registry), tool, read_input(&path))
            .as_text()
            .to_owned();
        assert!(!out.contains(TRUNCATED_MARKER), "{tool}: {out}");
    }
}

#[test]
fn full_mode_keeps_the_trailer_without_the_limits() {
    let (registry, _host) = host();

    let model = dispatch(&model_ctx(&registry), "trailer_probe", json!({}))
        .as_text()
        .to_owned();
    assert!(model.contains(FILE_TRUNCATED_MARKER), "{model}");
    assert!(model.ends_with(TRAILER), "{model}");

    let mut ctx = model_ctx(&registry);
    ctx.output_mode = ToolOutputMode::Full;
    let full = dispatch(&ctx, "trailer_probe", json!({}))
        .as_text()
        .to_owned();
    assert!(!full.contains(FILE_TRUNCATED_MARKER), "{full}");
    assert!(full.contains("a\nb\nc\nd\ne"), "{full}");
    assert!(full.ends_with(TRAILER), "{full}");
}

#[test]
fn handlers_see_the_dispatch_output_mode() {
    let (registry, _host) = host();
    let mut ctx = model_ctx(&registry);

    assert_eq!(
        dispatch(&ctx, "mode_probe", json!({})).as_text(),
        ToolOutputMode::Compact.as_str()
    );
    ctx.output_mode = ToolOutputMode::Full;
    assert_eq!(
        dispatch(&ctx, "mode_probe", json!({})).as_text(),
        ToolOutputMode::Full.as_str()
    );
}
