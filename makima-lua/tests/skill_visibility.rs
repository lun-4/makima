//! The model discovers skills through the `skill` tool description, while the
//! `@` completion, the expander, and the tool handler share the same lookup
//! map. A skill with `disable-model-invocation` must leave the description but
//! keep the other three paths working.

use std::sync::Arc;

use makima_agent::template::Vars;
use makima_agent::tools::test_support::stub_ctx;
use makima_agent::tools::{
    DescriptionContext, ToolAudience, ToolFilter, ToolRegistry, TurnToolBindings,
};
use makima_agent::{AgentMode, ToolOutput};
use makima_config::AgentConfig;
use makima_lua::test_support::{InMemoryFs, PluginHostGuard, spawn_host_with_fs_for_tests};
use makima_lua::{CompletionCtx, EventHandle};
use makima_providers::Model;

const SKILL_PLUGIN: &str = "skill";
const SKILL_TOOL: &str = "skill";
const SKILL_FILE: &str = "SKILL.md";
const SKILLS_DIR: &str = ".agents/skills";
const MODEL_SPEC: &str = "anthropic/claude-opus-4-8";
const CWD_ERR: &str = "cwd";
const HOST_ERR: &str = "skill plugin must load";

const VISIBLE: &str = "visible-skill";
const HIDDEN: &str = "hidden-skill";
const SPELLING: &str = "spelling-skill";
const VISIBLE_DESCRIPTION: &str = "visible skill description";
const HIDDEN_DESCRIPTION: &str = "hidden skill description";
const VISIBLE_BODY: &str = "visible body marker";
const HIDDEN_BODY: &str = "hidden body marker";
const SPELLING_BODY: &str = "spelling body marker";
const UNKNOWN: &str = "nope";
const NOT_FOUND: &str = "skill not found: nope";
const SKILLS_TAG: &str = "<available_skills>";
const OUT_OF_LIST_HINT: &str = "even if it is not in the list below";

fn skill_markdown(name: &str, description: &str, invocation: &str, body: &str) -> String {
    format!("---\nname: {name}\ndescription: {description}\n{invocation}---\n\n{body}\n")
}

/// Seeds before booting: the tool description is built once at load.
fn boot() -> (EventHandle, PluginHostGuard) {
    let fs = Arc::new(InMemoryFs::new());
    let skills = std::env::current_dir().expect(CWD_ERR).join(SKILLS_DIR);
    let seed = |name: &str, source: String| {
        fs.seed(&skills.join(name).join(SKILL_FILE), source.into_bytes());
    };
    seed(
        VISIBLE,
        skill_markdown(VISIBLE, VISIBLE_DESCRIPTION, "", VISIBLE_BODY),
    );
    seed(
        HIDDEN,
        skill_markdown(
            HIDDEN,
            HIDDEN_DESCRIPTION,
            "disable-model-invocation: true\n",
            HIDDEN_BODY,
        ),
    );
    seed(
        SPELLING,
        skill_markdown(
            SPELLING,
            HIDDEN_DESCRIPTION,
            "disable-model-invocation: \"yes\"\n",
            SPELLING_BODY,
        ),
    );
    spawn_host_with_fs_for_tests(&[SKILL_PLUGIN], fs, None)
}

fn skill_description(registry: &ToolRegistry) -> String {
    let model = Model::from_spec(MODEL_SPEC).expect("model spec");
    let filter = ToolFilter::from_config(&AgentConfig::default(), &model, &[]);
    let ctx = DescriptionContext {
        filter: &filter,
        audience: ToolAudience::MAIN,
        workflow: false,
        mcp: false,
    };
    let defs = registry.definitions(&Vars::new(), &ctx, false);
    defs.as_array()
        .expect("definitions array")
        .iter()
        .find(|def| def["name"] == SKILL_TOOL)
        .and_then(|def| def["description"].as_str())
        .expect("skill tool reaches the model")
        .to_owned()
}

fn call_skill(registry: &Arc<ToolRegistry>, name: &str) -> Result<String, String> {
    let entry = registry.get(SKILL_TOOL).expect(HOST_ERR);
    let invocation = entry
        .tool
        .parse(&serde_json::json!({ "name": name }))
        .expect("parse skill input");
    let mut ctx = stub_ctx(&AgentMode::Build);
    ctx.registry = Arc::clone(registry);
    ctx.turn_bindings = Arc::new(TurnToolBindings::capture(
        &ctx.registry,
        &ctx.local_tools,
        ctx.mcp.as_ref(),
    ));
    smol::block_on(async { invocation.execute(&ctx).await })
        .output
        .map(|output| match output {
            ToolOutput::Plain(text) => text.text,
            other => panic!("unexpected skill output: {other:?}"),
        })
}

#[test]
fn description_lists_visible_skill_only() {
    let (_handle, guard) = boot();
    let registry = guard.host().registry();
    let description = skill_description(&registry);
    assert!(description.contains(VISIBLE), "got: {description}");
    assert!(
        description.contains(VISIBLE_DESCRIPTION),
        "got: {description}"
    );
    assert!(!description.contains(HIDDEN), "got: {description}");
    assert!(!description.contains(SPELLING), "got: {description}");
    assert!(
        !description.contains(HIDDEN_DESCRIPTION),
        "got: {description}"
    );
    assert!(description.contains(OUT_OF_LIST_HINT), "got: {description}");
}

#[test]
fn completion_and_expansion_keep_disabled_skill() {
    let (handle, _guard) = boot();
    let expected = format!("skill:{HIDDEN}");
    let items = handle.collect_completion_items(&CompletionCtx::default());
    assert!(
        items.iter().any(|item| item.label == expected),
        "hidden skill must stay in completion: {:?}",
        items.iter().map(|item| &item.label).collect::<Vec<_>>()
    );

    let expanded = handle
        .expand_references(&format!("use @skill:{HIDDEN}"))
        .expect("hidden skill must expand");
    assert_eq!(expanded, format!("use <skill:{HIDDEN}>"));
}

#[test]
fn skill_tool_loads_disabled_skill_by_name() {
    let (_handle, guard) = boot();
    let registry = guard.host().registry();
    let output = call_skill(&registry, HIDDEN).expect("hidden skill must load");
    assert!(output.contains(HIDDEN_BODY), "got: {output}");
}

#[test]
fn unknown_skill_error_hides_disabled_skills() {
    let (_handle, guard) = boot();
    let registry = guard.host().registry();
    let error = call_skill(&registry, UNKNOWN).expect_err("unknown skill must fail");
    assert!(error.contains(NOT_FOUND), "got: {error}");
    assert!(error.contains(VISIBLE), "got: {error}");
    assert!(error.contains(SKILLS_TAG), "got: {error}");
    assert!(!error.contains(HIDDEN), "got: {error}");
    assert!(!error.contains(SPELLING), "got: {error}");
    assert!(!error.contains(HIDDEN_DESCRIPTION), "got: {error}");
}
