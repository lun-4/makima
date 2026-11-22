use makima_agent::{ModeId, tools::ToolRegistry};
use makima_lua::{PluginHost, test_support::run_command};
use std::{path::Path, sync::Arc};
use test_case::test_case;

const MODE_OVERRIDE_SRC: &str = include_str!("../../plugins/mode_plan_override/init.lua");
const TASK_SRC: &str = include_str!("../../plugins/task/init.lua");
const PLAN_SPEC_SRC: &str = include_str!("../../plugins/lib/makima/plan_spec.lua");
const MODE_OVERRIDE_PLUGIN: &str = "mode_plan_override";
const MODE_SWITCH_FAILED: &str = "cannot switch to";
const COMMAND_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

/// pi-luna tool strings that must not survive the clone.
const PI_TOOL_STRINGS: &[&str] = &[
    "subagent_create",
    "subagent_get",
    "subagent_delete",
    "plan-reviewer",
    "{{plan_spec_path}}",
    "`ask`",
    "`explore`",
    "`general-purpose`",
];

const SPEC_HEADINGS: &[&str] = &[
    "## Goal",
    "## Implementation Summary",
    "## Implementation Plan",
    "## Acceptance Criteria",
    "## Test Strategy",
    "## Review Strategy",
    "## Documentation Strategy",
    "## Risks, Blockers, and Required Decisions",
];

fn must_contain(src: &str, needle: &str, what: &str) {
    assert!(
        src.contains(needle),
        "{what} must contain the polytoken phrase `{needle}`"
    );
}

fn must_absent(src: &str, needle: &str, what: &str) {
    assert!(
        !src.contains(needle),
        "{what} must not contain the pi-only string `{needle}`"
    );
}

#[test]
fn directive_preserves_planning_workflow() {
    for s in PI_TOOL_STRINGS {
        must_absent(MODE_OVERRIDE_SRC, s, "plan-mode directive");
    }
    // Polytoken intent-classification and artifact rules survive verbatim.
    must_contain(MODE_OVERRIDE_SRC, "plan of plans", "plan-mode directive");
    must_contain(
        MODE_OVERRIDE_SRC,
        "write it into the plan file",
        "plan-mode directive",
    );
    must_contain(MODE_OVERRIDE_SRC, "plan_submit", "plan-mode directive");
    must_contain(MODE_OVERRIDE_SRC, "{plan_path}", "plan-mode directive");
    // Tool-name substitutions land.
    must_contain(MODE_OVERRIDE_SRC, "`question`", "plan-mode directive");
    must_contain(MODE_OVERRIDE_SRC, "`task_spawn`", "plan-mode directive");
    must_contain(MODE_OVERRIDE_SRC, "`task_wait`", "plan-mode directive");
    must_contain(MODE_OVERRIDE_SRC, "`plan_reviewer`", "plan-mode directive");
    // The directive splices the shared spec.
    must_contain(
        MODE_OVERRIDE_SRC,
        "require(\"makima.plan_spec\")",
        "plan-mode directive",
    );
}

#[test]
fn plan_mode_inherits_tools_and_keeps_write_restriction() {
    let host = PluginHost::new(Arc::new(ToolRegistry::new())).unwrap();
    host.load_source(MODE_OVERRIDE_PLUGIN, MODE_OVERRIDE_SRC)
        .unwrap();
    let mode = host.mode_registry().get(&ModeId::Plan).unwrap();
    assert!(mode.tools.is_none());
    assert_eq!(
        mode.restrict_write_to.as_deref(),
        Some(Path::new("plan.md"))
    );
    let prompt = mode.system_prompt.unwrap();
    for required in [
        "Command execution through `bash`, `code_execution`",
        "allowed for read-only investigation",
        "MCP tools may also be used for read-only investigation",
        "normal permission flow",
        "Tool availability or permission approval does not authorize side effects",
        "ask the user for confirmation with `question`",
        "Everything above applies to delegated work too",
    ] {
        assert!(prompt.contains(required), "missing instruction: {required}");
    }
    assert!(!prompt.contains("There is no shell in plan mode"));
}

#[test]
fn reviewer_is_verbatim_clone() {
    for s in PI_TOOL_STRINGS {
        must_absent(TASK_SRC, s, "plan-reviewer prompt");
    }
    must_contain(TASK_SRC, "files are the truth", "plan-reviewer prompt");
    must_contain(TASK_SRC, "test infrastructure", "plan-reviewer prompt");
    must_contain(TASK_SRC, "churn", "plan-reviewer prompt");
    must_contain(
        TASK_SRC,
        "fail when any critical or high finding remains",
        "plan-reviewer prompt",
    );
    must_contain(TASK_SRC, "VERDICT: pass", "plan-reviewer prompt");
    must_contain(
        TASK_SRC,
        "embedded in your system prompt",
        "plan-reviewer prompt",
    );
    must_contain(
        TASK_SRC,
        "require(\"makima.plan_spec\")",
        "plan-reviewer prompt",
    );
}

#[test]
fn plan_spec_is_single_shared_source() {
    // The spec reference is spelled identically in both splices, so the two
    // documents cannot drift apart.
    must_contain(
        MODE_OVERRIDE_SRC,
        "require(\"makima.plan_spec\")",
        "plan-mode directive",
    );
    must_contain(
        TASK_SRC,
        "require(\"makima.plan_spec\")",
        "plan-reviewer prompt",
    );

    // The shared module keeps every one of the 8 plan artifact sections and
    // its upstream attribution.
    for h in SPEC_HEADINGS {
        must_contain(PLAN_SPEC_SRC, h, "plan spec");
    }
    must_contain(
        PLAN_SPEC_SRC,
        "Ported from polytoken's default plan specification",
        "plan spec",
    );
    for s in PI_TOOL_STRINGS {
        must_absent(PLAN_SPEC_SRC, s, "plan spec");
    }
}

/// Without an agent to switch, `/plan` and `/build` fail with the reason
/// instead of reporting success and only logging it.
#[test_case("/plan" ; "plan")]
#[test_case("/build" ; "build")]
fn mode_command_failure_reaches_the_caller(command: &str) {
    let host = PluginHost::new(Arc::new(ToolRegistry::new())).unwrap();
    host.load_source(MODE_OVERRIDE_PLUGIN, MODE_OVERRIDE_SRC)
        .unwrap();
    let result = run_command(&host.command_registry(), command, 0, None)
        .recv_timeout(COMMAND_TIMEOUT)
        .unwrap();
    let error = result.unwrap_err();
    assert!(error.contains(MODE_SWITCH_FAILED), "{error}");
}
