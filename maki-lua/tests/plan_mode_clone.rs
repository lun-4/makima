//! Verifies the plan-mode directive and plan-reviewer prompt are verbatim
//! clones of pi-luna's polytoken ports, differing only where makima's tool names
//! or mechanics require a substitution (AC.1, AC.2, AC.3).
//!
//! These run on the real plugin sources so a byte-level edit to any of the
//! three files is caught here, not just at runtime.

const MODE_OVERRIDE_SRC: &str = include_str!("../../plugins/mode_plan_override/init.lua");
const TASK_SRC: &str = include_str!("../../plugins/task/init.lua");
const PLAN_SPEC_SRC: &str = include_str!("../../plugins/lib/maki/plan_spec.lua");

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
fn directive_is_verbatim_clone() {
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
    must_contain(MODE_OVERRIDE_SRC, "`task`", "plan-mode directive");
    must_contain(MODE_OVERRIDE_SRC, "`plan_reviewer`", "plan-mode directive");
    // The directive splices the shared spec.
    must_contain(
        MODE_OVERRIDE_SRC,
        "require(\"maki.plan_spec\")",
        "plan-mode directive",
    );
}

#[test]
fn plan_toolset_includes_webfetch() {
    // Byte-pin the whole toolset line: any add or remove is caught here.
    must_contain(
        MODE_OVERRIDE_SRC,
        r#"tools = { "read", "grep", "glob", "webfetch", "write", "edit", "plan_submit", "task" }"#,
        "plan toolset",
    );
    // A phrase unique to the directive sentence (the header comment also
    // mentions webfetch, so the backticked name alone would not pin it).
    must_contain(
        MODE_OVERRIDE_SRC,
        "fetching a page is a read",
        "plan-mode directive",
    );
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
        "require(\"maki.plan_spec\")",
        "plan-reviewer prompt",
    );
}

#[test]
fn plan_spec_is_single_shared_source() {
    // The spec reference is spelled identically in both splices, so the two
    // documents cannot drift apart.
    must_contain(
        MODE_OVERRIDE_SRC,
        "require(\"maki.plan_spec\")",
        "plan-mode directive",
    );
    must_contain(
        TASK_SRC,
        "require(\"maki.plan_spec\")",
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
