//! plan_reviewer through the real driver (AC.10).
//!
//! The reviewer is spawned via the real task plugin (`subagent_type =
//! "plan_reviewer"`), which gates it to plan mode and hands it a `research_sub`
//! (read-only) tool set. These tests drive the real `maki.agent.session` (wrapped
//! only to set `inherit_provider = true`) against a canned provider, so the
//! verdict text and the tool set the reviewer actually received are observable.
//! The plan-mode UI flow on a fail verdict is UI behavior, intentionally not
//! tested here.

use std::sync::Arc;

use maki_agent::tools::{ToolContext, ToolRegistry};
use maki_agent::{AgentId, AgentMode};
use maki_lua::PluginHost;
use maki_lua::host_root::HostRootServices;
use serde_json::{Value, json};

mod common;
use common::{CannedProvider, canned_reply, ctx_with_provider, exec_tool, tool_names};

const TASK_PLUGIN_SRC: &str = include_str!("../../plugins/task/init.lua");
const PLAN_REVIEWER_ONLY_ERR: &str = "plan_reviewer is only available in plan mode";
const TASK_SPAWN_TOOL: &str = "task_spawn";
const TASK_WAIT_TOOL: &str = "task_wait";
const TASK_FAMILY: [&str; 4] = [TASK_SPAWN_TOOL, TASK_WAIT_TOOL, "task_send", "task_despawn"];

/// Wrap the real `maki.agent.session` to reuse the parent (canned) provider, and
/// stub the model/system lookups so no real model resolution or file read is
/// needed. `maki.agent.tools` is left real when `real_tools` is set (so the
/// reviewer's tool set can be inspected); otherwise it is stubbed to empty.
const SESSION_WRAPPER: &str = r#"
local real_spawn = maki.agent.spawn
maki.agent.spawn = function(ctx, opts)
  opts.inherit_provider = true
  return real_spawn(ctx, opts)
end
maki.agent.resolve_model = function(ctx, opts)
  return { spec = "anthropic/claude-sonnet-4-20250514" }
end
maki.agent.system_prompt = function(ctx, opts)
  return "sys"
end
"#;

const TOOLS_STUB: &str = "maki.agent.tools = function(ctx, opts) return nil end\n";

/// A read tool visible to `research_sub` and a write tool visible only to
/// `general_sub`, so the real `maki.agent.tools` can demonstrate the reviewer's
/// audience-based read-only filtering: the reviewer (research_sub) gets `read`
/// but not `write`.
const FAKE_READ_WRITE_TOOLS: &str = r#"
maki.api.register_tool({
  name = "read",
  description = "fake read",
  kind = "execute",
  schema = { type = "object", properties = {}, additionalProperties = false },
  audiences = { "main", "research_sub" },
  handler = function() return "read-only" end,
})
maki.api.register_tool({
  name = "write",
  description = "fake write",
  kind = "execute",
  schema = { type = "object", properties = {}, additionalProperties = false },
  audiences = { "general_sub" },
  handler = function() return "wrote" end,
})
"#;

fn load_reviewer_host(real_tools: bool) -> (Arc<ToolRegistry>, PluginHost) {
    let reg: Arc<ToolRegistry> = if real_tools {
        ToolRegistry::global_arc().clone()
    } else {
        Arc::new(ToolRegistry::new())
    };
    let host = PluginHost::new(Arc::clone(&reg)).unwrap();
    let tools = if real_tools {
        FAKE_READ_WRITE_TOOLS
    } else {
        TOOLS_STUB
    };
    let src = format!("{tools}\n{SESSION_WRAPPER}\n{TASK_PLUGIN_SRC}");
    host.load_source("plan_reviewer_policy", &src).unwrap();
    (reg, host)
}

fn bind_host_root(host: &PluginHost, ctx: &mut ToolContext, mode: AgentMode) {
    ctx.mode = mode;
    let id = AgentId::generate();
    ctx.execution_agent_id = Some(id);
    host.event_handle()
        .install_host_root_services(Arc::new(HostRootServices::fixed(
            id,
            &ctx.model,
            ctx.opts,
            ctx.model_policy.clone(),
        )));
    host.wait_for_worker_barrier_for_test();
}

fn reviewer_input(description: &str) -> Value {
    let mut input = json!({ "description": description, "prompt": "Review the plan." });
    input["subagent_type"] = json!("plan_reviewer");
    input
}

/// Spawn the reviewer and block in `task_wait` until it settles, returning the
/// reviewer's final text.
fn run_reviewer(
    reg: &ToolRegistry,
    ctx: &ToolContext,
    description: &str,
) -> Result<String, String> {
    let spawned = exec_tool(reg, ctx, TASK_SPAWN_TOOL, reviewer_input(description))?;
    let waited = exec_tool(
        reg,
        ctx,
        TASK_WAIT_TOOL,
        json!({ "task_id": spawned["task_id"], "turn_id": spawned["turn_id"] }),
    )?;
    waited["result"]["text"]
        .as_str()
        .map(str::to_owned)
        .ok_or_else(|| format!("reviewer turn has no text: {waited}"))
}

#[test]
fn plan_reviewer_audits_plan_and_returns_verdict() {
    let (reg, host) = load_reviewer_host(false);

    let pass = Arc::new(CannedProvider::new(vec![canned_reply("VERDICT: pass")]));
    let (mut ctx_pass, _rx, _trigger) = ctx_with_provider(Arc::clone(&pass));
    bind_host_root(&host, &mut ctx_pass, AgentMode::Plan("/plan.md".into()));
    let text =
        run_reviewer(&reg, &ctx_pass, "pass-review").expect("reviewer must run in plan mode");
    assert!(
        text.contains("VERDICT: pass"),
        "pass verdict must surface: {text}"
    );

    let fail = Arc::new(CannedProvider::new(vec![canned_reply(
        "VERDICT: fail\n- finding: missing AC mapping",
    )]));
    let (mut ctx_fail, _rx, _trigger) = ctx_with_provider(Arc::clone(&fail));
    bind_host_root(&host, &mut ctx_fail, AgentMode::Plan("/plan.md".into()));
    let text =
        run_reviewer(&reg, &ctx_fail, "fail-review").expect("reviewer must run in plan mode");
    assert!(
        text.contains("VERDICT: fail"),
        "fail verdict with findings must surface: {text}"
    );
}

#[test]
fn plan_reviewer_is_read_only_even_when_asked_to_write() {
    let provider = Arc::new(CannedProvider::new(vec![canned_reply("VERDICT: pass")]));
    let (mut ctx, _rx, _trigger) = ctx_with_provider(Arc::clone(&provider));
    let (reg, host) = load_reviewer_host(true);
    bind_host_root(&host, &mut ctx, AgentMode::Plan("/plan.md".into()));
    ctx.registry = Arc::clone(&reg);
    ctx.turn_bindings = Arc::new(maki_agent::tools::TurnToolBindings::capture(
        &reg,
        &ctx.local_tools,
        ctx.mcp.as_ref(),
    ));
    let parent = common::with_request_tools(&reg, &ctx);
    assert!(
        maki_agent::agent::tool_dispatch::callable(&parent)
            .iter()
            .any(|tool| tool.name == "read")
    );
    assert!(
        tool_names(parent.request_tools.as_ref().unwrap().definitions())
            .iter()
            .any(|name| name == "read")
    );

    run_reviewer(&reg, &ctx, "readonly-review").expect("reviewer must run in plan mode");

    let names = tool_names(&provider.captured_tools()[0]);
    assert!(
        names.iter().any(|n| n == "read"),
        "reviewer (research_sub) must receive the read tool: {names:?}"
    );
    assert!(
        !names.iter().any(|n| n == "write"),
        "reviewer must exclude write tools (read-only enforced): {names:?}"
    );
    for task_tool in TASK_FAMILY {
        assert!(
            !names.iter().any(|name| name == task_tool),
            "reviewer (research_sub) must exclude {task_tool}: {names:?}"
        );
    }
}

#[test]
fn plan_reviewer_only_in_plan_mode() {
    let provider = Arc::new(CannedProvider::new(vec![]));
    let (mut ctx, _rx, _trigger) = ctx_with_provider(Arc::clone(&provider));
    let (reg, host) = load_reviewer_host(false);
    bind_host_root(&host, &mut ctx, AgentMode::Build);

    let err = run_reviewer(&reg, &ctx, "gated-review")
        .expect_err("plan_reviewer must be blocked outside plan mode");
    assert!(err.contains(PLAN_REVIEWER_ONLY_ERR), "got: {err}");
    assert!(
        provider.captured_thinking().is_empty(),
        "no session must spawn outside plan mode"
    );
}
