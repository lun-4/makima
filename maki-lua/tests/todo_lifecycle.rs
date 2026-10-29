use std::sync::Arc;

use maki_agent::tools::{ToolContext, ToolRegistry, test_support::stub_ctx};
use maki_agent::{AgentMode, History, ToolOutput};
use maki_lua::PluginHost;
use mlua::{Lua, LuaSerdeExt, Table, Value as LuaValue};
use serde_json::{Value, json};

const TODO_SOURCE: &str = include_str!("../../plugins/todo_write/init.lua");
const FIXTURE_SOURCE: &str = include_str!("fixtures/todo_lifecycle.lua");
const NAMESPACE: &str = "todo_write.todos";
const WRITE_TOOL: &str = "todo_write";
const READ_TOOL: &str = "todo_read";
const FIRST_TASK: &str = "First task";
const SECOND_TASK: &str = "Second task";

fn fixture() -> Lua {
    let lua = Lua::new();
    let json = lua.create_table().unwrap();
    json.set(
        "decode",
        lua.create_function(|lua, text: String| {
            let value: Value = serde_json::from_str(&text).map_err(mlua::Error::external)?;
            lua.to_value(&value)
        })
        .unwrap(),
    )
    .unwrap();
    json.set(
        "encode",
        lua.create_function(|lua, value: LuaValue| {
            let value: Value = lua.from_value(value)?;
            serde_json::to_string(&value).map_err(mlua::Error::external)
        })
        .unwrap(),
    )
    .unwrap();
    lua.globals().set("fixture_json", json).unwrap();
    let harness: Table = lua.load(FIXTURE_SOURCE).eval().unwrap();
    lua.globals().set("fixture", harness).unwrap();
    lua.load(TODO_SOURCE).exec().unwrap();
    lua.load(
        r#"
        first_items = { { content = "First task", status = "pending" } }
        second_items = { { content = "Second task", status = "in_progress", priority = "high" } }
        first = fixture.agent("first-agent", first_items)
        second = fixture.agent("second-agent", second_items)
        "#,
    )
    .exec()
    .unwrap();
    lua
}

#[test]
fn ui_harness_delayed_focus_smoke() {
    fixture()
        .load(
            r#"
            first.delayed = true
            fixture.emit("AgentFocusChanged", first)
            assert(#fixture.pending == 1)
            assert(fixture.queries[1] == "first-agent")
            assert(fixture.window == nil)
            fixture.emit("AgentFocusChanged", second)
            assert(fixture.queries[2] == "second-agent")
            assert(fixture.text(fixture.window.buf) == "[•] Second task")
            fixture.release()
            assert(#fixture.pending == 0)
            assert(fixture.text(fixture.window.buf) == "[•] Second task")
            "#,
        )
        .exec()
        .unwrap();
}

#[test]
fn todo_panel_tracks_focused_history() {
    fixture()
        .load(
            r#"
            fixture.emit("AgentFocusChanged", first)
            assert(fixture.text(fixture.window.buf) == "[ ] First task")
            assert(not fixture.window.visible)
            fixture.toggle()
            assert(fixture.window.visible)
            fixture.emit("AgentFocusChanged", second)
            assert(fixture.text(fixture.window.buf) == "[•] Second task")
            assert(not fixture.window.visible)
            first.items = { { content = "Background update", status = "completed" } }
            fixture.emit("HistoryStateChanged", first, "todo_write.todos")
            assert(#fixture.queries == 2)
            assert(fixture.text(fixture.window.buf) == "[•] Second task")
            fixture.emit("HistoryStateChanged", second, "other.namespace")
            assert(#fixture.queries == 2)
            fixture.emit("AgentFocusChanged", first)
            assert(fixture.text(fixture.window.buf) == "[✓] Background update")
            assert(fixture.window.visible)
            first.items = first_items
            fixture.emit("HistoryStateChanged", first)
            assert(fixture.text(fixture.window.buf) == "[ ] First task")
            first.items = nil
            fixture.emit("HistoryStateChanged", first)
            assert(not fixture.window.visible)
            fixture.emit("AgentFocusChanged", nil)
            assert(not fixture.window.visible)
            assert(fixture.hint == nil)
            assert(fixture.commits == 0)
            "#,
        )
        .exec()
        .unwrap();
}

#[test]
fn todo_panel_windows_belong_to_focused_session() {
    fixture()
        .load(
            r#"
            first.session = "first-session"
            second.session = "second-session"
            fixture.emit("AgentFocusChanged", first)
            fixture.toggle()
            local first_window = fixture.window
            assert(first_window.session == first.session)
            assert(first_window.visible)
            fixture.emit("AgentFocusChanged", second)
            local second_window = fixture.window
            assert(first_window ~= second_window)
            assert(second_window.session == second.session)
            assert(not first_window.visible)
            fixture.toggle()
            assert(second_window.visible)
            assert(not first_window.visible)
            fixture.emit("AgentFocusChanged", first)
            assert(#fixture.windows == 2)
            assert(first_window.visible)
            assert(not second_window.visible)
            fixture.toggle()
            assert(not first_window.visible)
            assert(not second_window.visible)
            assert(fixture.text(first_window.buf) == "[ ] First task")
            assert(fixture.text(second_window.buf) == "[•] Second task")
            "#,
        )
        .exec()
        .unwrap();
}

#[test]
fn todo_ctrl_t_after_turn() {
    fixture()
        .load(
            r#"
            fixture.emit("AgentFocusChanged", first)
            fixture.emit("HistoryStateChanged", first, "todo_write.todos")
            assert(fixture.window.visible)
            fixture.emit("TurnEnd", first)
            assert(fixture.window.visible)
            fixture.toggle()
            assert(not fixture.window.visible)
            assert(fixture.hint ~= nil)
            fixture.emit("HistoryStateChanged", first, "todo_write.todos")
            assert(not fixture.window.visible)
            fixture.toggle()
            assert(fixture.window.visible)
            assert(fixture.hint == nil)
            assert(fixture.text(fixture.window.buf) == "[ ] First task")
            assert(first.items == first_items)
            assert(fixture.commits == 0)
            "#,
        )
        .exec()
        .unwrap();
}

#[test]
fn todo_historical_restore_is_pure() {
    fixture()
        .load(
            r#"
            fixture.emit("AgentFocusChanged", first)
            local old = fixture.tools.todo_write.restore({ todos = second_items })
            assert(fixture.text(old) == "[•] Second task")
            local read = fixture.tools.todo_read.restore({}, maki.json.encode(second_items))
            assert(fixture.text(read) == "[•] Second task")
            fixture.tools.todo_write.restore({ todos = maki.json.decode("[]") })
            assert(#fixture.windows == 1)
            assert(#fixture.queries == 1)
            assert(fixture.text(fixture.window.buf) == "[ ] First task")
            assert(not fixture.window.visible)
            assert(first.items == first_items)
            assert(fixture.commits == 0)
            "#,
        )
        .exec()
        .unwrap();
}

#[test]
fn todo_stale_focus_refresh_is_ignored() {
    fixture()
        .load(
            r#"
            fixture.emit("AgentFocusChanged", first)
            first.delayed = true
            first.items = { { content = "Stale response", status = "cancelled" } }
            fixture.emit("HistoryStateChanged", first, "todo_write.todos")
            assert(#fixture.pending == 1)
            fixture.emit("AgentFocusChanged", second)
            fixture.toggle()
            fixture.release()
            assert(fixture.text(fixture.window.buf) == "[•] Second task")
            assert(fixture.window.visible)
            second.delayed = true
            fixture.emit("HistoryStateChanged", second)
            fixture.emit("AgentFocusChanged", nil)
            fixture.release()
            assert(not fixture.window.visible)
            assert(fixture.hint == nil)
            "#,
        )
        .exec()
        .unwrap();
}

#[test]
fn todo_newer_state_refresh_wins() {
    fixture()
        .load(
            r#"
            first.delayed = true
            fixture.emit("AgentFocusChanged", first)
            first.delayed = false
            first.items = second_items
            fixture.emit("HistoryStateChanged", first, "todo_write.todos")
            fixture.release()
            assert(fixture.text(fixture.window.buf) == "[•] Second task")
            assert(fixture.window.visible)
            "#,
        )
        .exec()
        .unwrap();
}

fn execute(registry: &ToolRegistry, ctx: &ToolContext, tool: &str, input: Value) -> (String, bool) {
    let invocation = registry.get(tool).unwrap().tool.parse(&input).unwrap();
    match smol::block_on(invocation.execute(ctx)).output {
        Ok(ToolOutput::Plain(output) | ToolOutput::Markdown(output)) => (output.text, false),
        Err(error) => (error, true),
        output => panic!("unexpected output: {output:?}"),
    }
}

#[test]
fn todo_replace_read_clear_and_validate() {
    let registry = Arc::new(ToolRegistry::new());
    let host = PluginHost::new(Arc::clone(&registry)).unwrap();
    host.load_source(WRITE_TOOL, TODO_SOURCE).unwrap();
    let history = History::new(Vec::new());
    let mut ctx = stub_ctx(&AgentMode::Build);
    ctx.history_state = Some(history.state_handle(0, ctx.cancel.clone()));
    let first = json!([{ "content": FIRST_TASK, "status": "pending" }]);
    let second = json!([{ "content": SECOND_TASK, "status": "completed", "priority": "low" }]);

    let (output, is_error) = execute(&registry, &ctx, READ_TOOL, json!({}));
    assert!(!is_error);
    assert_eq!(serde_json::from_str::<Value>(&output).unwrap(), json!([]));
    for items in [&first, &second] {
        let (output, is_error) = execute(&registry, &ctx, WRITE_TOOL, json!({ "todos": items }));
        assert!(!is_error);
        assert_eq!(serde_json::from_str::<Value>(&output).unwrap(), *items);
        let (output, is_error) = execute(&registry, &ctx, READ_TOOL, json!({}));
        assert!(!is_error);
        assert_eq!(serde_json::from_str::<Value>(&output).unwrap(), *items);
        assert_eq!(history.snapshot().state.get(NAMESPACE), Some(items));
    }
    let (output, is_error) = execute(&registry, &ctx, WRITE_TOOL, json!({ "todos": [] }));
    assert!(!is_error);
    assert_eq!(serde_json::from_str::<Value>(&output).unwrap(), json!([]));
    assert_eq!(history.snapshot().state.get(NAMESPACE), Some(&json!([])));
    let (output, is_error) = execute(&registry, &ctx, READ_TOOL, json!({}));
    assert!(!is_error);
    assert_eq!(serde_json::from_str::<Value>(&output).unwrap(), json!([]));
    let handle = ctx.history_state.as_ref().unwrap();
    handle.set(NAMESPACE.to_owned(), first.clone()).unwrap();
    let (output, is_error) = execute(&registry, &ctx, READ_TOOL, json!({}));
    assert!(!is_error);
    assert_eq!(serde_json::from_str::<Value>(&output).unwrap(), first);
    handle.delete(NAMESPACE.to_owned()).unwrap();
    let (output, is_error) = execute(&registry, &ctx, READ_TOOL, json!({}));
    assert!(!is_error);
    assert_eq!(serde_json::from_str::<Value>(&output).unwrap(), json!([]));
    assert!(host.ui_action_rx().is_empty());
}

#[test_case::test_case(json!({}), "todos must be a dense array"; "missing_list")]
#[test_case::test_case(json!({ "todos": false }), "todos must be a dense array"; "non_list")]
#[test_case::test_case(json!({ "todos": { "entry": {} } }), "todos must be a dense array"; "object_list")]
#[test_case::test_case(json!({ "todos": [false] }), "todo 1 must be an object"; "non_item")]
#[test_case::test_case(json!({ "todos": [{ "status": "pending" }] }), "todo 1 content must be a string"; "missing_content")]
#[test_case::test_case(json!({ "todos": [{ "content": false, "status": "pending" }] }), "todo 1 content must be a string"; "invalid_content")]
#[test_case::test_case(json!({ "todos": [{ "content": FIRST_TASK }] }), "todo 1 has an invalid status"; "missing_status")]
#[test_case::test_case(json!({ "todos": [{ "content": FIRST_TASK, "status": "unknown" }] }), "todo 1 has an invalid status"; "invalid_status")]
#[test_case::test_case(json!({ "todos": [{ "content": FIRST_TASK, "status": "pending", "priority": "urgent" }] }), "todo 1 has an invalid priority"; "invalid_priority")]
fn todo_invalid_write_preserves_state(input: Value, expected_error: &str) {
    let registry = Arc::new(ToolRegistry::new());
    let host = PluginHost::new(Arc::clone(&registry)).unwrap();
    host.load_source(WRITE_TOOL, TODO_SOURCE).unwrap();
    let history = History::new(Vec::new());
    let mut ctx = stub_ctx(&AgentMode::Build);
    let handle = history.state_handle(0, ctx.cancel.clone());
    let items = json!([{ "content": FIRST_TASK, "status": "pending" }]);
    handle.set(NAMESPACE.to_owned(), items.clone()).unwrap();
    ctx.history_state = Some(handle);
    let lua = fixture();
    lua.globals()
        .set("invalid_input", lua.to_value(&input).unwrap())
        .unwrap();
    let (output, is_error, commits): (String, bool, usize) = lua
        .load(
            r#"
            fixture.tool_items = first_items
            local result = fixture.tools.todo_write.handler(invalid_input, fixture.ctx)
            assert(fixture.tool_items == first_items)
            return result.llm_output, result.is_error, fixture.commits
            "#,
        )
        .eval()
        .unwrap();
    assert!(is_error);
    assert_eq!(output, expected_error);
    assert_eq!(commits, 0);
    let tool = registry.get(WRITE_TOOL).unwrap();
    if let Ok(invocation) = tool.tool.parse(&input) {
        assert!(smol::block_on(invocation.execute(&ctx)).output.is_err());
    }
    assert_eq!(history.snapshot().state.get(NAMESPACE), Some(&items));
}

#[test]
fn todo_extra_fields_are_normalized() {
    let registry = Arc::new(ToolRegistry::new());
    let host = PluginHost::new(Arc::clone(&registry)).unwrap();
    host.load_source(WRITE_TOOL, TODO_SOURCE).unwrap();
    let history = History::new(Vec::new());
    let mut ctx = stub_ctx(&AgentMode::Build);
    let handle = history.state_handle(0, ctx.cancel.clone());
    handle
        .set(
            NAMESPACE.to_owned(),
            json!([{ "content": FIRST_TASK, "status": "pending" }]),
        )
        .unwrap();
    ctx.history_state = Some(handle);
    let normalized = json!([{ "content": SECOND_TASK, "status": "completed", "priority": "low" }]);
    let input = json!({ "todos": [{
        "content": SECOND_TASK, "status": "completed", "priority": "low", "depends_on": [FIRST_TASK]
    }] });
    let (output, is_error) = execute(&registry, &ctx, WRITE_TOOL, input.clone());
    assert!(!is_error);
    assert_eq!(serde_json::from_str::<Value>(&output).unwrap(), normalized);
    assert_eq!(history.snapshot().state.get(NAMESPACE), Some(&normalized));

    let lua = fixture();
    lua.globals()
        .set("extra_input", lua.to_value(&input).unwrap())
        .unwrap();
    let output: String = lua
        .load(
            r#"
            fixture.tool_items = first_items
            local result = fixture.tools.todo_write.handler(extra_input, fixture.ctx)
            assert(not result.is_error)
            assert(fixture.commits == 1)
            assert(fixture.tool_items[1].content == "Second task")
            assert(fixture.tool_items[1].depends_on == nil)
            return result.llm_output
            "#,
        )
        .eval()
        .unwrap();
    assert_eq!(serde_json::from_str::<Value>(&output).unwrap(), normalized);
}

#[test]
fn todo_sparse_list_validation() {
    fixture()
        .load(
            r#"
            local sparse = { [1] = first_items[1], [3] = first_items[1] }
            local result = fixture.tools.todo_write.handler({ todos = sparse }, fixture.ctx)
            assert(result.is_error)
            assert(result.llm_output == "todos must be a dense array")
            assert(fixture.commits == 0)
            "#,
        )
        .exec()
        .unwrap();
}
