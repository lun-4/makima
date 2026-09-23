//! Thinking reaches the model (AC.6).
//!
//! The Lua `maki.agent.session` path parses `thinking` and threads it into the
//! provider's `RequestOptions` unchanged. It does NOT consult
//! `supports_thinking`/`requires_thinking`; enforcement (clamping to Off for a
//! non-supporting model) is UI-owned. These tests drive the real session against
//! a canned provider that records `RequestOptions.thinking`, so they assert what
//! the model actually received.

use std::sync::Arc;
use std::time::Duration;

use maki_agent::tools::ToolRegistry;
use maki_lua::PluginHost;
use maki_providers::{Effort, Model, ThinkingConfig};
use serde_json::{Value, json};

mod common;
use common::{CannedProvider, canned_reply, ctx_with_provider, exec_tool};

const PROBE_SRC: &str = r#"
maki.api.register_tool({
  name = "probe_thinking",
  description = "spawn a session with a thinking setting",
  schema = {
    type = "object",
    properties = { thinking = { description = "thinking setting" } },
    additionalProperties = false,
  },
  audiences = { "main" },
  handler = function(input, ctx)
    local sess, err = maki.agent.session(ctx, {
      name = "probe",
      inherit_provider = true,
      thinking = input.thinking,
    })
    if not sess then
      return { llm_output = "spawn failed: " .. tostring(err), is_error = true }
    end
    local _, serr = sess:send("hi")
    if serr then
      return { llm_output = "send failed: " .. tostring(serr), is_error = true }
    end
    return maki.json.encode({ ok = true })
  end,
})
"#;

fn load_thinking_host() -> (Arc<ToolRegistry>, PluginHost) {
    let reg = Arc::new(ToolRegistry::new());
    let host = PluginHost::new(Arc::clone(&reg)).unwrap();
    host.load_source("thinking_policy", PROBE_SRC).unwrap();
    (reg, host)
}

/// Drive the executor until the canned provider records a request, then return
/// the thinking config it saw.
fn wait_capture(provider: &CannedProvider) -> ThinkingConfig {
    for _ in 0..400 {
        let caps = provider.captured_thinking();
        if !caps.is_empty() {
            return caps[0];
        }
        smol::block_on(async { smol::Timer::after(Duration::from_millis(5)).await });
    }
    panic!("canned provider never received a request");
}

#[test_case::test_case(json!("off"), ThinkingConfig::Off ; "off")]
#[test_case::test_case(json!("low"), ThinkingConfig::Effort(Effort::Low) ; "low")]
#[test_case::test_case(json!("medium"), ThinkingConfig::Effort(Effort::Medium) ; "medium")]
#[test_case::test_case(json!("high"), ThinkingConfig::Effort(Effort::High) ; "high")]
#[test_case::test_case(json!(4096), ThinkingConfig::Budget(4096) ; "budget")]
fn thinking_effort_reaches_the_model(thinking: Value, expected: ThinkingConfig) {
    let (reg, _host) = load_thinking_host();
    let provider = Arc::new(CannedProvider::new(vec![canned_reply("ok")]));
    let (mut ctx, _rx, _trigger) = ctx_with_provider(Arc::clone(&provider));
    ctx.opts.thinking = ThinkingConfig::Effort(Effort::High);

    exec_tool(
        &reg,
        &ctx,
        "probe_thinking",
        json!({ "thinking": thinking }),
    )
    .unwrap();
    assert_eq!(wait_capture(&provider), expected);
}

/// A non-thinking model receives `Off` at the provider regardless of the
/// requested thinking: the agent run clamps via `RequestOptions::clamped(model)`
/// before calling `stream_message`. This pins where enforcement actually lives
/// (the agent layer, reached here through the real `maki.agent.session` path),
/// so a future change that moves clamping is caught.
#[test]
fn thinking_clamped_to_off_for_non_supporting_model() {
    let (reg, _host) = load_thinking_host();
    let provider = Arc::new(CannedProvider::new(vec![canned_reply("ok")]));
    let (mut ctx, _rx, _trigger) = ctx_with_provider(Arc::clone(&provider));
    ctx.opts.thinking = ThinkingConfig::Effort(Effort::High);
    let non_thinking = Arc::new(Model::from_spec("copilot/gpt-5-mini").unwrap());
    assert!(
        !non_thinking.supports_thinking(),
        "fixture model must not support thinking"
    );
    ctx.model = non_thinking;

    exec_tool(&reg, &ctx, "probe_thinking", json!({ "thinking": "high" })).unwrap();
    assert_eq!(
        wait_capture(&provider),
        ThinkingConfig::Off,
        "a non-thinking model must clamp the requested effort to Off at the agent layer"
    );
}
