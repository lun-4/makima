//! Headless-safe half of the `plan_submit` gate (AC.5).
//!
//! The plan_submit tool must refuse to run outside plan mode. The full
//! render/accept/reject UI flow is covered by the `makima-ui` app tests; here we
//! assert the plugin-level gate reads the executing agent's mode and errors
//! before touching `makima.ui.action`.

use std::sync::Arc;

use makima_agent::AgentMode;
use makima_agent::tools::ToolRegistry;
use makima_agent::tools::test_support::stub_ctx;
use makima_lua::PluginHost;
use serde_json::json;

mod common;
use common::exec_tool;

const PLAN_SUBMIT_SRC: &str = include_str!("../../plugins/plan_submit_tool/init.lua");
const GATE_ERR: &str = "plan_submit is only available in plan mode";

#[test]
fn plan_submit_rejected_outside_plan_mode() {
    let reg = Arc::new(ToolRegistry::new());
    let host = PluginHost::new(Arc::clone(&reg)).unwrap();
    host.load_source("plan_submit_policy", PLAN_SUBMIT_SRC)
        .unwrap();

    let ctx = stub_ctx(&AgentMode::Build);
    let err = exec_tool(&reg, &ctx, "plan_submit", json!({})).unwrap_err();
    assert!(err.contains(GATE_ERR), "got: {err}");
}
