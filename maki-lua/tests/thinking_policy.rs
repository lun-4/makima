//! Thinking settings reach the provider through a managed child.
//!
//! The provider records `RequestOptions.thinking`, including the agent layer's
//! clamp to Off for models that do not support thinking.

use std::sync::Arc;
use std::time::Duration;

use maki_agent::tools::{ToolContext, ToolRegistry};
use maki_agent::{ActorBackend, AgentLimits, AgentManagerHandle};
use maki_lua::PluginHost;
use maki_lua::orchestration::TrustedTarget;
use maki_lua::test_support::run_command;
use maki_providers::{Effort, Model, ThinkingConfig};
use serde_json::{Value, json};

mod common;
use common::{CannedProvider, canned_reply, ctx_with_provider, with_request_tools};

const TURN_TIMEOUT: Duration = Duration::from_secs(5);

const PROBE_SRC: &str = r#"
maki.api.register_command({
  name = "/probe_thinking",
  tui_only = false,
  arguments = { raw = true, required = true },
  handler = function(opts, ctx)
    local input = assert(maki.json.decode(opts.args))
    local child, err = maki.agent.spawn(ctx, {
      name = "probe",
      inherit_provider = true,
      model = { thinking = input.thinking },
    })
    assert(child, err)
    local result, serr = assert(child:send(ctx, "hi", { when = "idle" })):wait(ctx)
    assert(result, serr)
    local closed, cerr = child:close(ctx)
    assert(closed, cerr)
  end,
})
"#;

fn load_thinking_host(ctx: &mut ToolContext) -> (PluginHost, TrustedTarget) {
    let reg = Arc::new(ToolRegistry::new());
    let host = PluginHost::new(Arc::clone(&reg)).unwrap();
    host.load_source("thinking_policy", PROBE_SRC).unwrap();
    let manager = AgentManagerHandle::new(AgentLimits::default()).unwrap();
    let root = manager
        .create_root_with_config(
            Some(common::effective_config(ctx)),
            Vec::new(),
            None,
            |_| Ok::<_, String>(Box::new(common::IdleRoot) as Box<dyn ActorBackend>),
        )
        .unwrap();
    ctx.execution_agent_id = Some(root.id());
    let mut template = with_request_tools(&reg, ctx);
    template.registry = Arc::clone(&reg);
    let target = common::install_root(&host, root, template);
    (host, target)
}

fn wait_capture(provider: &CannedProvider) -> ThinkingConfig {
    let captured = provider.captured_thinking();
    assert_eq!(
        captured.len(),
        1,
        "prompt must complete one provider request"
    );
    captured[0]
}

#[test_case::test_case(json!("off"), ThinkingConfig::Off ; "off")]
#[test_case::test_case(json!("low"), ThinkingConfig::Effort(Effort::Low) ; "low")]
#[test_case::test_case(json!("medium"), ThinkingConfig::Effort(Effort::Medium) ; "medium")]
#[test_case::test_case(json!("high"), ThinkingConfig::Effort(Effort::High) ; "high")]
#[test_case::test_case(json!(4096), ThinkingConfig::Budget(4096) ; "budget")]
fn thinking_effort_reaches_the_model(thinking: Value, expected: ThinkingConfig) {
    let provider = Arc::new(CannedProvider::new(vec![canned_reply("ok")]));
    let (mut ctx, _rx, _trigger) = ctx_with_provider(Arc::clone(&provider));
    ctx.opts.thinking = ThinkingConfig::Effort(Effort::High);
    let (host, target) = load_thinking_host(&mut ctx);

    let args = json!({ "thinking": thinking });
    run_command(
        &host.command_registry(),
        &format!("/probe_thinking {args}"),
        0,
        Some(target),
    )
    .recv_timeout(TURN_TIMEOUT)
    .unwrap()
    .unwrap();
    assert_eq!(wait_capture(&provider), expected);
}

#[test]
fn thinking_clamped_to_off_for_non_supporting_model() {
    let provider = Arc::new(CannedProvider::new(vec![canned_reply("ok")]));
    let (mut ctx, _rx, _trigger) = ctx_with_provider(Arc::clone(&provider));
    ctx.opts.thinking = ThinkingConfig::Effort(Effort::High);
    let non_thinking = Arc::new(Model::from_spec("copilot/gpt-5-mini").unwrap());
    assert!(
        !non_thinking.supports_thinking(),
        "fixture model must not support thinking"
    );
    ctx.model = non_thinking;
    let (host, target) = load_thinking_host(&mut ctx);

    let args = json!({ "thinking": "high" });
    run_command(
        &host.command_registry(),
        &format!("/probe_thinking {args}"),
        0,
        Some(target),
    )
    .recv_timeout(TURN_TIMEOUT)
    .unwrap()
    .unwrap();
    assert_eq!(
        wait_capture(&provider),
        ThinkingConfig::Off,
        "a non-thinking model must clamp the requested effort to Off at the agent layer"
    );
}
