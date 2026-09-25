//! Drives the real bundled `skill`, `task`, and `model` plugins through the
//! completion RPCs, so the plugin Lua is actually executed: `ctx.mode` and
//! `ctx.models` must reach the `get_items` functions, and `@` tokens must
//! expand through the real Lua expanders at submit time.

use std::sync::Arc;

use maki_agent::prompt::{PromptId, Slot};
use maki_agent::tools::ToolRegistry;
use maki_lua::test_support::{InMemoryFs, spawn_host_with_fs_for_tests};
use maki_lua::{CompletionCtx, EventHandle, ItemSpec, PluginHost};

const SKILL_SRC: &str = include_str!("../../plugins/skill/init.lua");
const TASK_SRC: &str = include_str!("../../plugins/task/init.lua");
const MODEL_SRC: &str = include_str!("../../plugins/model/init.lua");

const BUILTIN_SKILL: &str = "maki-plugin-dev";
const PROJECT_SKILL: &str = "proj-skill";
const PROJECT_SKILL_SOURCE: &str =
    "---\nname: proj-skill\ndescription: project scoped\n---\n\nDo the project thing.\n";
const PROJECT_SKILL_REL: &str = ".agents/skills/proj-skill/SKILL.md";
const MODEL_SPEC: &str = "zai/glm-5";
const PLAN_MODE: &str = "plan";
const BUILD_MODE: &str = "build";

const SUB_RESEARCH: &str = "subagent:research";
const SUB_GENERAL: &str = "subagent:general";
const SUB_PLAN_REVIEWER: &str = "subagent:plan_reviewer";

const ERR_UNKNOWN_SKILL: &str = "unknown skill";
const ERR_UNKNOWN_SUBAGENT: &str = "unknown subagent type";

fn ctx(mode: &str) -> CompletionCtx {
    CompletionCtx {
        mode: mode.into(),
        models: Vec::new(),
    }
}

fn host_with_real_plugins() -> (PluginHost, EventHandle) {
    let host =
        PluginHost::new(Arc::new(ToolRegistry::new())).unwrap_or_else(|e| panic!("host: {e}"));
    for (name, src) in [
        ("skill", SKILL_SRC),
        ("task", TASK_SRC),
        ("model", MODEL_SRC),
    ] {
        host.load_source(name, src)
            .unwrap_or_else(|e| panic!("{name} plugin failed to load: {e}"));
    }
    let eh = host.event_handle();
    (host, eh)
}

fn labels<'a>(items: &'a [ItemSpec], kind: &str) -> Vec<&'a str> {
    items
        .iter()
        .filter(|i| i.kind == kind)
        .map(|i| i.label.as_str())
        .collect()
}

#[test]
fn plan_mode_hides_general_subagent() {
    let (_host, eh) = host_with_real_plugins();
    let items = eh.collect_completion_items(&ctx(PLAN_MODE));
    let sub = labels(&items, "subagent");
    assert!(
        sub.contains(&SUB_RESEARCH),
        "research must be offered: {sub:?}"
    );
    assert!(
        sub.contains(&SUB_PLAN_REVIEWER),
        "plan_reviewer must be offered in plan mode: {sub:?}"
    );
    assert!(
        !sub.contains(&SUB_GENERAL),
        "plan mode must hide general (ctx.mode reached get_items): {sub:?}"
    );
}

#[test]
fn build_mode_hides_plan_reviewer_subagent() {
    let (_host, eh) = host_with_real_plugins();
    let items = eh.collect_completion_items(&ctx(BUILD_MODE));
    let sub = labels(&items, "subagent");
    assert!(
        sub.contains(&SUB_RESEARCH),
        "research must be offered: {sub:?}"
    );
    assert!(
        sub.contains(&SUB_GENERAL),
        "general must be offered outside plan mode: {sub:?}"
    );
    assert!(
        !sub.contains(&SUB_PLAN_REVIEWER),
        "build mode must hide plan_reviewer (ctx.mode reached get_items): {sub:?}"
    );
}

#[test]
fn ctx_models_flow_into_model_items() {
    let (_host, eh) = host_with_real_plugins();
    let c = CompletionCtx {
        mode: BUILD_MODE.into(),
        models: vec![MODEL_SPEC.into()],
    };
    let items = eh.collect_completion_items(&c);
    let models = labels(&items, "model");
    assert!(
        models.contains(&"model:zai/glm-5"),
        "ctx.models must reach the model source: {models:?}"
    );
    let empty_items = eh.collect_completion_items(&CompletionCtx::default());
    let empty = labels(&empty_items, "model");
    assert!(empty.is_empty(), "no models in ctx, no items: {empty:?}");
}

#[test]
fn skill_source_offers_builtin_skill() {
    let (_host, eh) = host_with_real_plugins();
    let items = eh.collect_completion_items(&ctx(BUILD_MODE));
    let skills = labels(&items, "skill");
    let expected = format!("skill:{BUILTIN_SKILL}");
    assert!(
        skills.contains(&expected.as_str()),
        "builtin skill must be offered: {skills:?}"
    );
}

/// The completion source and the expander have no tool ctx, so they read the
/// project root from the process cwd: a skill in the cwd's `.agents/skills`
/// must reach both the `@` popup and submit-time expansion.
#[test]
fn project_skill_from_cwd_is_offered_and_expands() {
    let fs = Arc::new(InMemoryFs::new());
    let cwd = std::env::current_dir().expect("cwd");
    fs.seed(
        &cwd.join(PROJECT_SKILL_REL),
        PROJECT_SKILL_SOURCE.as_bytes().to_vec(),
    );
    let (handle, _guard) = spawn_host_with_fs_for_tests(&["skill"], Arc::clone(&fs), None);

    let items = handle.collect_completion_items(&CompletionCtx::default());
    let expected = format!("skill:{PROJECT_SKILL}");
    let skills = labels(&items, "skill");
    assert!(
        skills.contains(&expected.as_str()),
        "project skill must be offered: {skills:?}"
    );

    let expanded = handle
        .expand_references(&format!("use @skill:{PROJECT_SKILL}"))
        .unwrap_or_else(|e| panic!("project skill must expand: {e}"));
    assert_eq!(expanded, format!("use <skill:{PROJECT_SKILL}>"));
}

#[test]
fn expands_all_three_token_kinds_in_place() {
    let (_host, eh) = host_with_real_plugins();
    let out = eh
        .expand_references("go @subagent:research @skill:maki-plugin-dev @model:zai/glm-5 now")
        .unwrap_or_else(|e| panic!("expand failed: {e}"));
    assert_eq!(
        out,
        "go <subagent:research> <skill:maki-plugin-dev> <model:zai/glm-5> now"
    );
}

#[test]
fn expands_short_form_aliases() {
    let (_host, eh) = host_with_real_plugins();
    let out = eh
        .expand_references("@a:general @s:maki-plugin-dev @m:zai/glm-5")
        .unwrap_or_else(|e| panic!("expand failed: {e}"));
    assert_eq!(
        out,
        "<subagent:general> <skill:maki-plugin-dev> <model:zai/glm-5>"
    );
}

#[test]
fn unknown_skill_rejects_submit() {
    let (_host, eh) = host_with_real_plugins();
    let err = eh
        .expand_references("use @skill:does-not-exist")
        .expect_err("unknown skill must reject");
    assert!(err.contains(ERR_UNKNOWN_SKILL), "{err}");
}

#[test]
fn unknown_subagent_type_rejects_submit() {
    let (_host, eh) = host_with_real_plugins();
    let err = eh
        .expand_references("@subagent:alien")
        .expect_err("unknown subagent type must reject");
    assert!(err.contains(ERR_UNKNOWN_SUBAGENT), "{err}");
}

#[test]
fn admitted_slot_request_precedes_plugin_replacement() {
    const FIRST: &str = "first admitted slot";
    const SECOND: &str = "replacement slot";
    let host = PluginHost::new(Arc::new(ToolRegistry::new())).unwrap();
    let source = |content| {
        format!(
            "maki.api.register_prompt_hint({{ slot = 'after_instructions', content = '{content}' }})"
        )
    };
    host.load_source("pinned_slots", &source(FIRST)).unwrap();
    let handle = host.event_handle();
    let admitted = handle.request_prompt_slots();
    host.load_source("pinned_slots", &source(SECOND)).unwrap();
    let slots = admitted.recv().unwrap();
    let contents: Vec<_> = slots
        .get(PromptId::System, Slot::AfterInstructions)
        .iter()
        .map(|entry| entry.content.as_str())
        .collect();
    assert_eq!(contents, [FIRST]);
    let latest = handle.collect_prompt_slots();
    assert_eq!(
        latest.get(PromptId::System, Slot::AfterInstructions)[0].content,
        SECOND
    );
}

#[test]
fn plugins_teach_agent_their_intent_tokens() {
    let (_host, eh) = host_with_real_plugins();
    let slots = eh.collect_prompt_slots();
    let entries: Vec<&str> = slots
        .get(PromptId::System, Slot::AfterInstructions)
        .iter()
        .map(|e| e.content.as_str())
        .collect();
    assert!(
        entries.iter().any(|c| c.contains("<model:spec>")),
        "model hint missing: {entries:?}"
    );
    assert!(
        entries.iter().any(|c| c.contains("<subagent:type>")),
        "subagent hint missing: {entries:?}"
    );
    assert!(
        entries.iter().any(|c| c.contains("<skill:name>")),
        "skill hint missing: {entries:?}"
    );
}

#[test]
fn unknown_prefix_passes_through() {
    let (_host, eh) = host_with_real_plugins();
    let src = "mail foo@bar:baz? and @nothing:whatever";
    let out = eh
        .expand_references(src)
        .unwrap_or_else(|e| panic!("expand failed: {e}"));
    assert_eq!(out, src);
}

#[test]
fn plain_text_is_untouched() {
    let (_host, eh) = host_with_real_plugins();
    let src = "no references here, just prose.";
    let out = eh
        .expand_references(src)
        .unwrap_or_else(|e| panic!("expand failed: {e}"));
    assert_eq!(out, src);
}
