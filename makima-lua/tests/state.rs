mod common;

use std::{collections::BTreeSet, sync::Arc};

use makima_agent::{
    ActorBackend, AgentId, AgentLimits, AgentManagerHandle, AgentRef,
    journal::{JournalChange, SessionJournalSnapshot},
    state::{StateScope, StateSlotSpec},
    tools::{ToolRegistry, TurnToolBindings},
};
use makima_lua::{PluginHost, test_support::InMemoryFs, ui_sink::HostUi};
use makima_storage::{
    checkpoint::CheckpointPublication,
    session_state::{SessionStateView, StateChange, StoredStateRecord},
};
use serde_json::{Value, json};
use test_case::test_case;

const PLUGIN: &str = "state_fixture";
const TOOL: &str = "state_fixture";
const FIXTURE: &str = r#"
local slot = makima.state.register("value", { scope = "agent", version = VERSION, migrate = MIGRATE })
local session = makima.state.register("shared", { scope = "session", version = 1 })
local held
makima.api.register_tool({
    name = "state_fixture", description = "state fixture",
    schema = { type = "object", properties = { action = { type = "string" }, count = { type = "number" }, error = { type = "string" } } },
    handler = function(input, ctx)
        local value, revision = slot:get(ctx)
        if input.action == "expect_error" then
            assert(value == nil and string.find(revision, input.error, 1, true), tostring(revision))
            return "rejected"
        end
        assert(type(revision) == "number", tostring(revision))
        if input.action == "write" then
            local candidate = { count = input.count, null = makima.state.null }
            local committed, err = slot:set(ctx, candidate, { expect = revision })
            assert(committed, err)
            candidate.count = -1
            local copy, read_revision = slot:get(ctx)
            assert(copy.count == input.count and copy.null == makima.state.null and read_revision == committed)
            copy.count = -2
            assert(slot:get(ctx).count == input.count)
            local agent = assert(ctx:agent())
            local retained, retained_revision = agent:state(ctx, "state_fixture/value")
            assert(retained.count == input.count and retained_revision == committed)
            return tostring(committed)
        elseif input.action == "update" then
            local calls = 0
            local updated, committed = slot:update(ctx, function(cur)
                calls = calls + 1
                cur.count = cur.count + 1
                return cur
            end)
            assert(calls == 1 and updated.count == input.count, tostring(committed))
            return tostring(committed)
        elseif input.action == "conflict" then
            local calls = 0
            local updated, err = slot:update(ctx, function(cur)
                calls = calls + 1
                assert(slot:set(ctx, { count = input.count }))
                cur.count = -1
                return cur
            end)
            assert(updated == nil and string.find(err, "state_conflict", 1, true), tostring(err))
            assert(calls == 1 and slot:get(ctx).count == input.count)
            return "conflict"
        elseif input.action == "invalid" then
            local cyclic = {}; cyclic.self = cyclic
            local invalid = { cyclic, function() end, { [1] = true, [3] = false }, { [false] = true }, 0/0, math.huge }
            for _, candidate in ipairs(invalid) do
                local committed, err = slot:set(ctx, candidate)
                assert(committed == nil and type(err) == "string")
                local unchanged, same_revision = slot:get(ctx)
                assert(unchanged.count == value.count and same_revision == revision)
            end
            local yielding, yield_error = slot:update(ctx, function() coroutine.yield(); return {} end)
            assert(yielding == nil and type(yield_error) == "string")
            local unchanged, same_revision = slot:get(ctx)
            assert(unchanged.count == value.count and same_revision == revision)
            return "invalid"
        elseif input.action == "nested" then
            local out, err = makima.agent.call_tool(ctx, "state_nested", {})
            assert(out == "nested" and err == nil, tostring(err))
            local after, committed = slot:get(ctx)
            assert(after.count == 12 and committed > revision)
            return "nested"
        elseif input.action == "delete" then
            local deleted, err = slot:delete(ctx, { expect = revision })
            assert(deleted, err)
            local absent, tombstone = slot:get(ctx)
            assert(absent == nil and tombstone == deleted)
            local stale, conflict = slot:set(ctx, {}, { expect = revision })
            assert(stale == nil and string.find(conflict, "state_conflict", 1, true))
            return tostring(deleted)
        elseif input.action == "capture" then
            held = ctx
            return "captured"
        elseif input.action == "expired" then
            local committed, err = slot:set(held, {})
            assert(committed == nil and type(err) == "string")
            return "expired"
        elseif input.action == "session" then
            local before, rev = session:get(ctx)
            local committed, err = session:set(ctx, { count = input.count }, { expect = rev })
            assert(committed, err)
            local after, current = session:get(ctx)
            assert(after.count == input.count and current == committed)
            return tostring(committed)
        else
            return makima.json.encode({ value = value, revision = revision })
        end
    end,
})
makima.api.register_tool({
    name = "state_nested", description = "nested state fixture", schema = { type = "object", properties = {} },
    handler = function(_, ctx)
        local before, revision = slot:get(ctx)
        local updated, committed = slot:update(ctx, function(cur) cur.count = 12; return cur end)
        assert(updated.count == 12 and committed > revision)
        return "nested"
    end,
})
"#;

fn source(version: u32, migrate: &str) -> String {
    FIXTURE
        .replace("VERSION", &version.to_string())
        .replace("MIGRATE", migrate)
}

#[test]
fn state_fixture_copies_cas_tombstones_and_failed_reload() {
    let registry = Arc::new(ToolRegistry::new());
    let host = PluginHost::with_session_provider_preparer(
        Arc::clone(&registry),
        Default::default(),
        true,
        Arc::new(InMemoryFs::new()),
        None,
        None,
        HostUi::Frontend,
    )
    .unwrap();
    host.load_source(PLUGIN, &source(1, "nil")).unwrap();
    let (mut ctx, _events, _cancel) = common::ctx_with_canned_provider();
    ctx.registry = Arc::clone(&registry);
    ctx.turn_bindings = Arc::new(TurnToolBindings::capture(
        &registry,
        &ctx.local_tools,
        ctx.mcp.as_ref(),
    ));
    let config = common::effective_config(&ctx);
    common::bind_parked_root(&host, &registry, &mut ctx, config, AgentLimits::default());
    let call = |input| common::exec_tool_text(&registry, &ctx, TOOL, input).unwrap();
    let first: u64 = call(json!({ "action": "write", "count": 3 }))
        .parse()
        .unwrap();
    let second: u64 = call(json!({ "action": "update", "count": 4 }))
        .parse()
        .unwrap();
    assert!(second > first);
    assert_eq!(
        call(json!({ "action": "conflict", "count": 5 })),
        "conflict"
    );
    assert_eq!(call(json!({ "action": "invalid" })), "invalid");
    assert_eq!(call(json!({ "action": "nested" })), "nested");
    assert_eq!(call(json!({ "action": "capture" })), "captured");
    assert_eq!(call(json!({ "action": "expired" })), "expired");
    assert!(
        host.load_source(PLUGIN, &(source(2, "nil") + "\nerror('candidate fails')"))
            .is_err()
    );
    let current: Value = serde_json::from_str(&call(json!({}))).unwrap();
    assert_eq!(current["value"]["count"], 12);
    host.unload(PLUGIN).unwrap();
    host.load_source(PLUGIN, &source(1, "nil")).unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(&call(json!({}))).unwrap(),
        current
    );
    let deleted: u64 = call(json!({ "action": "delete" })).parse().unwrap();
    assert!(deleted > second);
    let current: Value = serde_json::from_str(&call(json!({}))).unwrap();
    assert_eq!(current["revision"], deleted);
    assert!(current.get("value").is_none());
    call(json!({ "action": "session", "count": 7 }));
}

#[test]
fn state_fixture_migrates_once_and_preserves_failed_and_newer_values() {
    let registry = Arc::new(ToolRegistry::new());
    let host = PluginHost::with_session_provider_preparer(
        Arc::clone(&registry),
        Default::default(),
        true,
        Arc::new(InMemoryFs::new()),
        None,
        None,
        HostUi::Frontend,
    )
    .unwrap();
    host.load_source(PLUGIN, &source(1, "nil")).unwrap();
    let (mut ctx, _events, _cancel) = common::ctx_with_canned_provider();
    ctx.registry = Arc::clone(&registry);
    ctx.turn_bindings = Arc::new(TurnToolBindings::capture(
        &registry,
        &ctx.local_tools,
        ctx.mcp.as_ref(),
    ));
    let config = common::effective_config(&ctx);
    common::bind_parked_root(&host, &registry, &mut ctx, config, AgentLimits::default());
    let call = |input| common::exec_tool_text(&registry, &ctx, TOOL, input).unwrap();
    call(json!({ "action": "write", "count": 3 }));
    let before: Value = serde_json::from_str(&call(json!({}))).unwrap();
    host.load_source(
        PLUGIN,
        &source(
            2,
            "function(cur) cur.count = 99; error('migration failed') end",
        ),
    )
    .unwrap();
    assert_eq!(
        call(json!({ "action": "expect_error", "error": "migration failed" })),
        "rejected"
    );
    host.load_source(PLUGIN, &source(1, "nil")).unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(&call(json!({}))).unwrap(),
        before
    );
    host.load_source(PLUGIN, &source(2, "function(cur, version) assert(version == 1); cur.count = cur.count + 1; return cur end")).unwrap();
    let migrated: Value = serde_json::from_str(&call(json!({}))).unwrap();
    assert_eq!(migrated["value"]["count"], 4);
    assert_ne!(migrated["revision"], before["revision"]);
    assert_eq!(
        serde_json::from_str::<Value>(&call(json!({}))).unwrap(),
        migrated
    );
    host.load_source(PLUGIN, &source(1, "nil")).unwrap();
    assert_eq!(
        call(json!({ "action": "expect_error", "error": "state_newer_version" })),
        "rejected"
    );
    host.load_source(PLUGIN, &source(2, "nil")).unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(&call(json!({}))).unwrap(),
        migrated
    );
}

const NOTICE: &str = "state notification delivered";
const DORMANT_SOURCE: &str = r#"
local slot = makima.state.register("dormant", { scope = "agent", version = 1 })
slot:on_change(function(ctx, event)
    local value, revision = slot:get(ctx)
    assert(value.count == 8 and revision == event.revision)
    local agent = assert(makima.agent.get(ctx, event.agent:ref()))
    local retained, retained_revision = agent:state(ctx, "state_fixture/dormant")
    assert(retained.count == 8 and retained_revision == revision)
    assert(ctx:mode() == nil)
    assert(event.agent ~= nil and (event.reason == "commit" or event.reason == "resume" or event.reason == "rewind"))
    makima.ui.flash("state notification delivered")
end)
makima.api.register_command({
    name = "state_dormant", description = "exercise dormant state", tui_only = false, handler = function(_, ctx)
        local revision, err = slot:set(ctx, { count = 8 })
        assert(revision, err)
        local value, current = slot:get(ctx)
        assert(value.count == 8 and current == revision)
    end,
})
"#;

#[test]
fn trusted_dormant_state_and_nonveto_install_notifications() {
    use makima_agent::{ActorBackend, AgentManagerHandle, state::StateChangeReason};
    use makima_lua::{UiAction, test_support::run_command};

    let registry = Arc::new(ToolRegistry::new());
    let host = PluginHost::with_session_provider_preparer(
        Arc::clone(&registry),
        Default::default(),
        true,
        Arc::new(InMemoryFs::new()),
        None,
        None,
        HostUi::Frontend,
    )
    .unwrap();
    host.load_source(PLUGIN, DORMANT_SOURCE).unwrap();
    let (template, _events, _cancel) = common::ctx_with_canned_provider();
    let manager = AgentManagerHandle::new(AgentLimits::default()).unwrap();
    let root = manager
        .create_root_with_config(
            Some(common::effective_config(&template)),
            Vec::new(),
            None,
            |_| Ok::<_, String>(Box::new(common::IdleRoot) as Box<dyn ActorBackend>),
        )
        .unwrap();
    let target = common::install_root(&host, root.clone(), template);
    struct DormantTarget(makima_lua::orchestration::TrustedTarget);
    impl makima_lua::orchestration::OrchestrationServices for DormantTarget {
        fn resolve_target(
            &self,
            _: Option<&makima_commands::CommandInvocation>,
        ) -> Result<Option<makima_lua::orchestration::TrustedTarget>, String> {
            Ok(Some(self.0.clone()))
        }
        fn template(
            &self,
            _: &makima_agent::AgentRef,
        ) -> Result<makima_agent::tools::ToolContext, String> {
            Ok(self.0.template.clone())
        }
    }
    host.event_handle()
        .install_orchestration_services(Arc::new(DormantTarget(target.clone())));
    host.wait_for_worker_barrier_for_test();
    manager
        .detach_incarnation(&root.incarnation().unwrap())
        .unwrap();
    assert!(!manager.is_live(root.id()));
    let finished = run_command(&host.command_registry(), "/state_dormant", 0, Some(target));
    assert_eq!(
        smol::block_on(common::bounded(finished.recv_async())).unwrap(),
        Ok(())
    );
    let notices = host.ui_action_rx();
    let receive = || {
        let notice = smol::block_on(common::bounded(notices.recv_async())).unwrap();
        assert!(matches!(notice, UiAction::Flash(ref text) if text == NOTICE));
    };
    receive();
    host.event_handle().notify_state_restored(root.clone());
    receive();
    manager
        .journal()
        .notify_state_transition(StateChangeReason::Rewind);
    receive();
    assert!(!manager.is_live(root.id()));
}

#[test_case(false; "agent_scope")]
#[test_case(true; "session_scope")]
fn restored_state_observation_waits_without_resume_and_delivers_once(session_scope: bool) {
    use makima_agent::{state::StateChangeReason, tools::ToolContext};
    use makima_lua::{
        EventHandle, UiAction,
        orchestration::{OrchestrationServices, TrustedTarget},
    };

    const SOURCE: &str = r#"
local slot = makima.state.register("observed", { scope = SCOPE, version = 1 })
local calls = 0
slot:on_change(function(ctx, event)
    local value, revision = slot:get(ctx)
    assert(value.count == 8 and revision == event.revision)
    calls = calls + 1
    makima.ui.flash(event.reason .. ":" .. tostring(calls))
end)
"#;
    const REWIND: &str = "rewind:1";
    const COMMIT: &str = "commit:2";
    const HOST_STOPPED: &str = "Lua host stopped";

    struct Templates(ToolContext);
    impl OrchestrationServices for Templates {
        fn resolve_target(
            &self,
            _: Option<&makima_commands::CommandInvocation>,
        ) -> Result<Option<TrustedTarget>, String> {
            Ok(None)
        }

        fn template(&self, _: &AgentRef) -> Result<ToolContext, String> {
            Ok(self.0.clone())
        }
    }

    let host = PluginHost::with_session_provider_preparer(
        Arc::new(ToolRegistry::new()),
        Default::default(),
        true,
        Arc::new(InMemoryFs::new()),
        None,
        None,
        HostUi::Frontend,
    )
    .unwrap();
    let scope = if session_scope { "session" } else { "agent" };
    host.load_source(PLUGIN, &SOURCE.replace("SCOPE", &format!("\"{scope}\"")))
        .unwrap();
    let (template, _events, _cancel) = common::ctx_with_canned_provider();
    let manager = AgentManagerHandle::new(AgentLimits::default()).unwrap();
    let root = manager
        .create_root_with_config(
            Some(common::effective_config(&template)),
            Vec::new(),
            None,
            |_| Ok::<_, String>(Box::new(common::IdleRoot) as Box<dyn ActorBackend>),
        )
        .unwrap();
    let journal = manager.journal();
    let slot = journal
        .register_state::<Value>(StateSlotSpec {
            key: format!("{PLUGIN}/observed"),
            scope: if session_scope {
                StateScope::Session
            } else {
                StateScope::Agent
            },
            version: 1,
        })
        .unwrap();
    slot.set(&root.state_access(), json!({ "count": 8 }), None)
        .unwrap();
    host.event_handle()
        .install_orchestration_services(Arc::new(Templates(template)));
    let events = journal.subscribe_state_changes();
    let before = journal.snapshot();
    for _ in 0..2 {
        host.event_handle()
            .observe_state_restored(root.clone())
            .unwrap();
    }
    assert!(events.try_recv().is_err());
    assert_eq!(journal.snapshot().revision, before.revision);
    assert_eq!(journal.snapshot().state, before.state);
    assert_eq!(
        EventHandle::disconnected_for_test()
            .observe_state_restored(root.clone())
            .unwrap_err(),
        HOST_STOPPED
    );
    let actions = host.ui_action_rx();
    journal.notify_state_transition(StateChangeReason::Rewind);
    let notice = smol::block_on(common::bounded(actions.recv_async())).unwrap();
    assert!(matches!(notice, UiAction::Flash(ref text) if text == REWIND));
    slot.set(&root.state_access(), json!({ "count": 8 }), None)
        .unwrap();
    let notice = smol::block_on(common::bounded(actions.recv_async())).unwrap();
    assert!(matches!(notice, UiAction::Flash(ref text) if text == COMMIT));
}

const READONLY_TOOL: &str = "state_readonly";
const READONLY_SOURCE: &str = r#"
local migrations = 0
local slot = makima.state.register("readonly", {
    scope = "agent", version = VERSION,
    migrate = function(value) migrations = migrations + 1; return { count = value.count + 1 } end,
})
local function probe(input, ctx)
    local value, revision = slot:get(ctx)
    if input.error then
        assert(value == nil and string.find(revision, input.error, 1, true), tostring(revision))
    else
        assert(value.count == input.count and type(revision) == "number", tostring(revision))
    end
    local committed, err = slot:set(ctx, { count = 999 })
    assert(committed == nil and string.find(err, "state_read_only", 1, true), tostring(err))
    committed, err = slot:delete(ctx)
    assert(committed == nil and string.find(err, "state_read_only", 1, true), tostring(err))
    local updated; updated, err = slot:update(ctx, function() return { count = 999 } end)
    assert(updated == nil and string.find(err, "state_read_only", 1, true), tostring(err))
    assert(migrations == 0)
    local buf = makima.ui.buf(); buf:line("readonly verified"); return buf
end
makima.api.register_tool({
    name = "state_readonly", description = "readonly state fixture", schema = { type = "object", properties = { count = { type = "number" }, error = { type = "string" } } },
    start = function(input, ctx) ctx:live_buf(probe(input, ctx)) end,
    restore = function(input, _, _, ctx) return probe(input, ctx) end,
    handler = function() return "unused" end,
})
"#;
const READONLY_OK: &str = "readonly verified";
const RESTORE_ID: &str = "state-readonly-restore";

#[test]
fn start_and_restore_read_only_use_captured_target_without_migration() {
    use makima_agent::{
        ActorBackend, AgentEvent, AgentManagerHandle, EventSender,
        state::{StateScope, StateSlotSpec},
    };
    use makima_config::ToolOutputLines;
    use makima_lua::RestoreItem;

    let registry = Arc::new(ToolRegistry::new());
    let host = PluginHost::with_session_provider_preparer(
        Arc::clone(&registry),
        Default::default(),
        true,
        Arc::new(InMemoryFs::new()),
        None,
        None,
        HostUi::Frontend,
    )
    .unwrap();
    host.load_source(PLUGIN, &READONLY_SOURCE.replace("VERSION", "1"))
        .unwrap();
    let (mut ctx, _events, _cancel) = common::ctx_with_canned_provider();
    ctx.registry = Arc::clone(&registry);
    ctx.turn_bindings = Arc::new(TurnToolBindings::capture(
        &registry,
        &ctx.local_tools,
        ctx.mcp.as_ref(),
    ));
    let config = common::effective_config(&ctx);
    common::bind_parked_root(
        &host,
        &registry,
        &mut ctx,
        config.clone(),
        AgentLimits::default(),
    );
    let current = ctx.managed_turn.as_ref().unwrap();
    let target = current.manager().reference(current.agent_id());
    let manager = current.manager();
    let slot = manager
        .journal()
        .register_state::<Value>(StateSlotSpec {
            key: "state_fixture/readonly".into(),
            scope: StateScope::Agent,
            version: 1,
        })
        .unwrap();
    let revision = slot
        .set(&target.state_access(), json!({ "count": 31 }), None)
        .unwrap();
    let other = AgentManagerHandle::new(AgentLimits::default()).unwrap();
    let focused = other
        .create_root_with_config(Some(config), Vec::new(), None, |_| {
            Ok::<_, String>(Box::new(common::IdleRoot) as Box<dyn ActorBackend>)
        })
        .unwrap();
    common::install_root(&host, focused, ctx.clone());

    let restore = |input: Value, captured| {
        let (tx, rx) = flume::unbounded();
        host.event_handle().request_restore(
            RestoreItem {
                tool: READONLY_TOOL.into(),
                tool_use_id: RESTORE_ID.into(),
                output: String::new(),
                input,
                is_error: false,
                tool_output_lines: ToolOutputLines::default(),
                theme_gen: None,
                clicks: Vec::new(),
                state: None,
                state_target: captured,
            },
            EventSender::new(tx),
        );
        host.event_handle().wait_restore_complete_for_test();
        let texts: Vec<_> = rx
            .drain()
            .filter_map(|envelope| match envelope.event {
                AgentEvent::ToolSnapshot { snapshot, .. } => Some(
                    snapshot
                        .lines
                        .iter()
                        .flat_map(|line| line.spans.iter())
                        .map(|span| span.text.as_str())
                        .collect::<String>(),
                ),
                _ => None,
            })
            .collect();
        assert!(texts.iter().any(|text| text == READONLY_OK), "{texts:?}");
    };
    let start = |input| {
        let (tx, rx) = flume::unbounded();
        let mut invocation_ctx = ctx.clone();
        invocation_ctx.tool_use_id = Some(RESTORE_ID.into());
        invocation_ctx.event_tx = EventSender::new(tx);
        let invocation = registry
            .get(READONLY_TOOL)
            .unwrap()
            .tool
            .parse(&input)
            .unwrap();
        smol::block_on(common::bounded(invocation.start(&invocation_ctx)));
        assert!(
            rx.drain()
                .any(|envelope| matches!(envelope.event, AgentEvent::LiveToolBuf { .. })),
            "start callback failed"
        );
    };
    start(json!({ "count": 31 }));
    restore(json!({ "count": 31 }), Some(target.clone()));
    restore(json!({ "error": "state_target_unavailable" }), None);
    host.load_source(PLUGIN, &READONLY_SOURCE.replace("VERSION", "2"))
        .unwrap();
    start(json!({ "error": "state_read_only" }));
    restore(json!({ "error": "state_read_only" }), Some(target.clone()));
    let stored = slot.get_raw(&target.state_access()).unwrap();
    assert_eq!(stored.version, 1);
    assert_eq!(stored.revision, revision);
    assert_eq!(stored.value, Some(json!({ "count": 31 })));
}

const RETAINED_TOOL: &str = "state_retained";
const RETAINED_SOURCE: &str = r#"
local migrations = 0
makima.state.register("retained", {
    scope = "agent", version = VERSION,
    migrate = function(value) migrations = migrations + 1; value.count = 99; return value end,
})
makima.api.register_tool({
    name = "state_retained", description = "retained migration authority", schema = { type = "object", properties = { agent = { type = "string" }, migration = { type = "boolean" } } },
    handler = function(input, ctx)
        local child = assert(makima.agent.get(ctx, input.agent))
        local value, revision = child:state(ctx, "state_fixture/retained")
        if input.migration then
            assert(value == nil and string.find(revision, "state_migration_requires_target", 1, true), tostring(revision))
        else
            assert(value.count == 6 and type(revision) == "number", tostring(revision))
        end
        assert(migrations == 0)
        return "retained verified"
    end,
})
"#;
const RETAINED_OK: &str = "retained verified";

#[test]
fn delegated_retained_reads_do_not_gain_descendant_migration_writes() {
    use makima_agent::{
        ActorBackend, AgentMetadata,
        state::{StateScope, StateSlotSpec},
    };

    let registry = Arc::new(ToolRegistry::new());
    let host = PluginHost::with_session_provider_preparer(
        Arc::clone(&registry),
        Default::default(),
        true,
        Arc::new(InMemoryFs::new()),
        None,
        None,
        HostUi::Frontend,
    )
    .unwrap();
    host.load_source(PLUGIN, &RETAINED_SOURCE.replace("VERSION", "1"))
        .unwrap();
    let (mut ctx, _events, _cancel) = common::ctx_with_canned_provider();
    ctx.registry = Arc::clone(&registry);
    ctx.turn_bindings = Arc::new(TurnToolBindings::capture(
        &registry,
        &ctx.local_tools,
        ctx.mcp.as_ref(),
    ));
    let config = common::effective_config(&ctx);
    common::bind_parked_root(
        &host,
        &registry,
        &mut ctx,
        config.clone(),
        AgentLimits::default(),
    );
    let current = ctx.managed_turn.as_ref().unwrap();
    let manager = current.manager();
    let parent = manager.reference(current.agent_id());
    let child = manager
        .spawn_child_trusted_with_config(
            &parent,
            config,
            AgentMetadata::default(),
            Vec::new(),
            None,
            |_| Ok::<_, String>(Box::new(common::IdleRoot) as Box<dyn ActorBackend>),
        )
        .unwrap();
    let slot = manager
        .journal()
        .register_state::<Value>(StateSlotSpec {
            key: "state_fixture/retained".into(),
            scope: StateScope::Agent,
            version: 1,
        })
        .unwrap();
    let revision = slot
        .set(&child.state_access(), json!({ "count": 6 }), None)
        .unwrap();
    let input = |migration| json!({ "agent": child.id().to_string(), "migration": migration });
    assert_eq!(
        common::exec_tool_text(&registry, &ctx, RETAINED_TOOL, input(false)).unwrap(),
        RETAINED_OK
    );
    host.load_source(PLUGIN, &RETAINED_SOURCE.replace("VERSION", "2"))
        .unwrap();
    assert_eq!(
        common::exec_tool_text(&registry, &ctx, RETAINED_TOOL, input(true)).unwrap(),
        RETAINED_OK
    );
    let stored = slot.get_raw(&child.state_access()).unwrap();
    assert_eq!(stored.revision, revision);
    assert_eq!(stored.version, 1);
    assert_eq!(stored.value, Some(json!({ "count": 6 })));
}

const COMPACT_GATE: &str = "state_compact_gate";
const COMPACT_TOOL: &str = "state_compact_probe";
const COMPACT_OK: &str = "fresh invocation committed";
const COMPACT_SOURCE: &str = r#"
local slot = makima.state.register("compact", { scope = "agent", version = 1 })
makima.api.register_tool({
    name = "state_compact_probe", description = "state epoch fixture", schema = { type = "object", properties = { old = { type = "boolean" } } },
    handler = function(input, ctx)
        if input.old then
            assert(slot:set(ctx, { count = 1 }))
            local output, err = makima.agent.call_tool(ctx, "state_compact_gate", {})
            assert(output == "compacted" and err == nil, tostring(err))
            local committed; committed, err = slot:set(ctx, { count = 99 })
            assert(committed == nil and string.find(err, "state_stale_access", 1, true), tostring(err))
            local value, revision = slot:get(ctx)
            assert(value == nil and string.find(revision, "state_stale_access", 1, true), tostring(revision))
            return "old invocation fenced"
        end
        local value, revision = slot:get(ctx)
        assert(value.count == 1 and type(revision) == "number", tostring(revision))
        assert(slot:set(ctx, { count = 2 }, { expect = revision }))
        return "fresh invocation committed"
    end,
})
"#;

#[test]
fn compaction_fences_old_context_but_new_invocation_captures_current_epoch() {
    use makima_agent::{
        ToolOutput,
        journal::JournalCommit,
        tools::{
            DescriptionContext, Tool, ToolContext, ToolInvocation, ToolSource,
            registry::{ExecFuture, HeaderFuture, HeaderResult, ParseError, ToolExecResult},
        },
    };
    use makima_storage::sessions::HistorySnapshot;
    use std::borrow::Cow;

    struct CompactGate;
    impl Tool for CompactGate {
        fn name(&self) -> &str {
            COMPACT_GATE
        }
        fn description(&self, _: &DescriptionContext) -> Cow<'_, str> {
            "rotate the state epoch".into()
        }
        fn schema(&self) -> Value {
            json!({ "type": "object", "properties": {} })
        }
        fn parse(&self, _: &Value) -> Result<Box<dyn ToolInvocation>, ParseError> {
            Ok(Box::new(Self))
        }
    }
    impl ToolInvocation for CompactGate {
        fn start_header(&self) -> HeaderFuture {
            HeaderFuture::Ready(HeaderResult::plain(COMPACT_GATE.into()))
        }
        fn execute<'a>(self: Box<Self>, ctx: &'a ToolContext) -> ExecFuture<'a> {
            Box::pin(async move {
                let current = ctx.managed_turn.as_ref().unwrap();
                let journal = current.manager().journal();
                let previous = journal.snapshot().histories[&current.agent_id()].clone();
                let mut compacted = HistorySnapshot::new(previous.messages.as_ref().clone());
                compacted.compacted = true;
                journal
                    .commit(JournalCommit {
                        histories: [(current.agent_id(), compacted)].into(),
                        ..Default::default()
                    })
                    .unwrap();
                ToolExecResult::from(Ok::<_, String>(ToolOutput::Plain("compacted".into())))
            })
        }
    }
    let registry = Arc::new(ToolRegistry::new());
    registry
        .register(
            Arc::new(CompactGate),
            ToolSource::Lua {
                plugin: "state_host_gate".into(),
            },
        )
        .unwrap();
    let host = PluginHost::with_session_provider_preparer(
        Arc::clone(&registry),
        Default::default(),
        true,
        Arc::new(InMemoryFs::new()),
        None,
        None,
        HostUi::Frontend,
    )
    .unwrap();
    host.load_source(PLUGIN, COMPACT_SOURCE).unwrap();
    let (mut ctx, _events, _cancel) = common::ctx_with_canned_provider();
    ctx.registry = Arc::clone(&registry);
    ctx.turn_bindings = Arc::new(TurnToolBindings::capture(
        &registry,
        &ctx.local_tools,
        ctx.mcp.as_ref(),
    ));
    let config = common::effective_config(&ctx);
    common::bind_parked_root(&host, &registry, &mut ctx, config, AgentLimits::default());
    assert_eq!(
        common::exec_tool_text(&registry, &ctx, COMPACT_TOOL, json!({ "old": true })).unwrap(),
        "old invocation fenced"
    );
    assert_eq!(
        common::exec_tool_text(&registry, &ctx, COMPACT_TOOL, json!({})).unwrap(),
        COMPACT_OK
    );
}

const RESUME_SOURCE: &str = r#"
local migrations = 0
local last = 0
local slot = makima.state.register("resume", {
    scope = "agent", version = 2,
    migrate = function(value, version)
        assert(version == 1 and value.count == 4)
        migrations = migrations + 1
        value.count = 5
        return value
    end,
})
slot:on_change(function(ctx, event)
    local current, revision = slot:get(ctx)
    assert(type(revision) == "number" and revision >= event.revision)
    assert(event.agent ~= nil and ctx:mode() == nil)
    if event.reason == "resume" then
        assert(current.count == 5 and migrations == 1 and revision == event.revision)
        makima.ui.flash("resume migrated")
        error("notification cannot veto")
    end
    assert(event.reason == "commit" and event.revision > last)
    last = event.revision
    makima.ui.flash("host commit:" .. tostring(event.revision))
end)
"#;
const RESUME_OK: &str = "resume migrated";
const COMMIT_PREFIX: &str = "host commit:";
const TRANSITION_FAILURE: &str = "candidate migration failed";
const TRANSITION_NEWER: &str = "state_newer_version";
const TRANSITION_REQUIRED: &str = "state_migration_required";
const TRANSITION_SCOPE: &str = "state_invalid_registration";
const TRANSITION_EFFECT: &str = "state_migration_has_no_invocation_authority";
const TRANSITION_OK: &str = "candidate migrated before rewind event";
const TRANSITION_SOURCE: &str = r#"
local calls = 0
local slot
slot = makima.state.register("value", { scope = "agent", version = 2,
    migrate = function(value, version)
        assert(version == 1 and value.count == 4)
        calls = calls + 1
        local ok, err = pcall(function() slot:on_change(function() error("escaped registration") end) end)
        assert(not ok and string.find(tostring(err), "state_migration_has_no_invocation_authority", 1, true))
        ok, err = pcall(function() makima.async.run(function() error("escaped async") end) end)
        assert(not ok and string.find(tostring(err), "state_migration_has_no_invocation_authority", 1, true))
        value.count = 5
        return value
    end,
})
slot:on_change(function(ctx, event)
    assert(event.reason == "rewind" and calls == 1)
    local value, revision = slot:get(ctx)
    assert(value.count == 5 and revision == event.revision)
    makima.ui.flash("candidate migrated before rewind event")
    error("notification cannot veto")
end)
"#;

#[test]
fn resume_migrates_before_callback_and_host_write_notices_keep_commit_order() {
    use makima_agent::{
        ActorBackend, AgentManagerHandle,
        state::{StateScope, StateSlotSpec},
    };
    use makima_lua::UiAction;

    let registry = Arc::new(ToolRegistry::new());
    let host = PluginHost::with_session_provider_preparer(
        Arc::clone(&registry),
        Default::default(),
        true,
        Arc::new(InMemoryFs::new()),
        None,
        None,
        HostUi::Frontend,
    )
    .unwrap();
    host.load_source(PLUGIN, RESUME_SOURCE).unwrap();
    let (template, _events, _cancel) = common::ctx_with_canned_provider();
    let manager = AgentManagerHandle::new(AgentLimits::default()).unwrap();
    let root = manager
        .create_root_with_config(
            Some(common::effective_config(&template)),
            Vec::new(),
            None,
            |_| Ok::<_, String>(Box::new(common::IdleRoot) as Box<dyn ActorBackend>),
        )
        .unwrap();
    let journal = manager.journal();
    let old = journal
        .register_state::<Value>(StateSlotSpec {
            key: "state_fixture/resume".into(),
            scope: StateScope::Agent,
            version: 1,
        })
        .unwrap();
    old.set(&root.state_access(), json!({ "count": 4 }), None)
        .unwrap();
    common::install_root(&host, root.clone(), template);
    host.event_handle().notify_state_restored(root.clone());
    let actions = host.ui_action_rx();
    let receive = || match smol::block_on(common::bounded(actions.recv_async())).unwrap() {
        UiAction::Flash(text) => text,
        _ => panic!("unexpected notification"),
    };
    assert_eq!(receive(), RESUME_OK);
    let migrated = old.get_raw(&root.state_access()).unwrap();
    assert_eq!(migrated.version, 2);
    assert_eq!(migrated.value, Some(json!({ "count": 5 })));
    assert_eq!(receive(), format!("{COMMIT_PREFIX}{}", migrated.revision.0));
    let current = journal
        .register_state::<Value>(StateSlotSpec {
            key: "state_fixture/resume".into(),
            scope: StateScope::Agent,
            version: 2,
        })
        .unwrap();
    let first = current
        .set(&root.state_access(), json!({ "count": 6 }), None)
        .unwrap();
    let second = current
        .set(&root.state_access(), json!({ "count": 7 }), None)
        .unwrap();
    assert_eq!(receive(), format!("{COMMIT_PREFIX}{}", first.0));
    assert_eq!(receive(), format!("{COMMIT_PREFIX}{}", second.0));
    assert_eq!(
        current.get_raw(&root.state_access()).unwrap().value,
        Some(json!({ "count": 7 }))
    );
}

fn transition_fixture() -> (PluginHost, AgentRef, SessionJournalSnapshot) {
    let registry = Arc::new(ToolRegistry::new());
    let host = PluginHost::with_session_provider_preparer(
        registry,
        Default::default(),
        true,
        Arc::new(InMemoryFs::new()),
        None,
        None,
        HostUi::Frontend,
    )
    .unwrap();
    let (template, _events, _cancel) = common::ctx_with_canned_provider();
    let manager = AgentManagerHandle::new(AgentLimits::default()).unwrap();
    let root = manager
        .create_root_with_config(
            Some(common::effective_config(&template)),
            Vec::new(),
            None,
            |_| Ok::<_, String>(Box::new(common::IdleRoot) as Box<dyn ActorBackend>),
        )
        .unwrap();
    let journal = manager.journal();
    let slot = journal
        .register_state::<Value>(StateSlotSpec {
            key: format!("{PLUGIN}/value"),
            scope: StateScope::Agent,
            version: 1,
        })
        .unwrap();
    slot.set(&root.state_access(), json!({ "count": 4 }), None)
        .unwrap();
    let snapshot = journal.snapshot();
    common::install_root(&host, root.clone(), template);
    (host, root, snapshot)
}

fn prepare_transition(
    host: &PluginHost,
    snapshot: &SessionJournalSnapshot,
    affected: BTreeSet<AgentId>,
    session_scope: bool,
) -> Result<Vec<JournalChange>, String> {
    smol::block_on(common::bounded(host.event_handle().prepare_state_restore(
        snapshot.clone(),
        affected,
        session_scope,
        CheckpointPublication::default(),
    )))
    .map(|prepared| prepared.changes)
}

#[test_case("function(value) value.count = 99; error('candidate migration failed') end", TRANSITION_FAILURE; "raised_error")]
#[test_case("function(value) value.count = 99; coroutine.yield(); return value end", "yield"; "yielding_migration")]
#[test_case("function(value) value.count = 99; return function() end end", "state value does not support"; "invalid_result")]
#[test_case("function(value) value.count = 99; return { [1] = true, [3] = true } end", "state value"; "sparse_result")]
#[test_case("function(value) value.count = 99; local cycle = {}; cycle.self = cycle; return cycle end", "table cycle"; "cyclic_result")]
#[test_case("function(value) value.count = 99; return math.huge end", "must be finite"; "nonfinite_result")]
#[test_case("nil", TRANSITION_REQUIRED; "missing_migration")]
#[test_case("function() makima.state.register('escape', { scope = 'agent', version = 1 }); return {} end", TRANSITION_EFFECT; "registration_denied")]
fn failed_prepublication_migration_preserves_live_and_candidate(migrate: &str, expected: &str) {
    let (host, root, snapshot) = transition_fixture();
    host.load_source(PLUGIN, &source(2, migrate)).unwrap();
    let journal = root.manager().journal();
    let replacement = smol::block_on(common::bounded(
        journal.prepare_branch_restore(root.id(), snapshot.clone()),
    ))
    .unwrap();
    let before = replacement.snapshot().clone();
    let error = prepare_transition(
        &host,
        replacement.snapshot(),
        replacement.affected_agents().clone(),
        replacement.session_scope(),
    )
    .unwrap_err();
    assert!(error.contains(expected), "{error}");
    assert_eq!(replacement.snapshot().state, before.state);
    assert_eq!(replacement.snapshot().seq, before.seq);
    assert_eq!(journal.snapshot().state, snapshot.state);
    assert_eq!(journal.snapshot().revision, snapshot.revision);
    drop(replacement);
    host.load_source(PLUGIN, &source(1, "nil")).unwrap();
    let newer = prepare_transition(&host, &snapshot, BTreeSet::from([root.id()]), true).unwrap();
    assert!(newer.is_empty());
}

#[test]
fn prepublication_migration_installs_once_before_nonveto_notification() {
    let (host, root, snapshot) = transition_fixture();
    host.load_source(PLUGIN, TRANSITION_SOURCE).unwrap();
    let journal = root.manager().journal();
    let mut replacement = smol::block_on(common::bounded(
        journal.prepare_branch_restore(root.id(), snapshot.clone()),
    ))
    .unwrap();
    let changes = prepare_transition(
        &host,
        replacement.snapshot(),
        replacement.affected_agents().clone(),
        replacement.session_scope(),
    )
    .unwrap();
    assert_eq!(changes.len(), 1);
    assert_eq!(journal.snapshot().state, snapshot.state);
    assert!(host.ui_action_rx().try_recv().is_err());
    replacement.apply_migrations(changes).unwrap();
    assert!(
        prepare_transition(
            &host,
            replacement.snapshot(),
            replacement.affected_agents().clone(),
            replacement.session_scope(),
        )
        .unwrap()
        .is_empty()
    );
    replacement.install().unwrap();
    replacement.notify();
    let actions = host.ui_action_rx();
    let action = smol::block_on(common::bounded(actions.recv_async())).unwrap();
    assert!(matches!(action, makima_lua::UiAction::Flash(text) if text == TRANSITION_OK));
    host.drain_tasks_for_test();
    let after = journal.snapshot();
    let view = SessionStateView::fold(&after.state, &after.agent_revisions, after.seq).unwrap();
    let stored = &view.scopes[&Some(root.id())].plugins[&format!("{PLUGIN}/value")];
    assert_eq!(stored.version, 2);
    assert_eq!(stored.value, Some(json!({ "count": 5 })));
    assert!(host.ui_action_rx().try_recv().is_err());
}

#[test]
fn scoped_prepublication_migration_carries_unloaded_and_outside_values() {
    let (host, root, mut snapshot) = transition_fixture();
    let child = AgentId::generate();
    for (agent, key, version) in [
        (Some(child), format!("{PLUGIN}/value"), 1),
        (None, format!("{PLUGIN}/shared"), 3),
        (Some(child), "unloaded/opaque".into(), 9),
    ] {
        snapshot.seq += 1;
        snapshot.state.push(StoredStateRecord {
            seq: snapshot.seq,
            agent,
            generation: 7,
            change: StateChange::Plugin {
                key,
                version,
                value: Some(json!({ "count": 4 })),
            },
        });
    }
    host.load_source(
        PLUGIN,
        &source(2, "function(value) value.count = 5; return value end"),
    )
    .unwrap();
    let changes = prepare_transition(&host, &snapshot, BTreeSet::from([child]), false).unwrap();
    assert_eq!(changes.len(), 1);
    assert_eq!(changes[0].agent, Some(child));
    assert_eq!(changes[0].generation, 7);
    assert!(
        matches!(&changes[0].change, StateChange::Plugin { key, version: 2, value: Some(value) }
        if key == &format!("{PLUGIN}/value") && value == &json!({ "count": 5 }))
    );
    let error =
        prepare_transition(&host, &snapshot, BTreeSet::from([root.id(), child]), true).unwrap_err();
    assert!(error.contains(TRANSITION_NEWER), "{error}");
    host.unload(PLUGIN).unwrap();
    assert!(
        prepare_transition(&host, &snapshot, BTreeSet::from([child]), true)
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        root.manager().journal().snapshot().state.len() + 3,
        snapshot.state.len()
    );
}

#[test]
fn prepublication_migration_uses_only_current_registration_after_reload() {
    let (host, root, snapshot) = transition_fixture();
    host.load_source(
        PLUGIN,
        &source(2, "function() error('stale migration closure') end"),
    )
    .unwrap();
    host.load_source(
        PLUGIN,
        &source(2, "function(value) value.count = 6; return value end"),
    )
    .unwrap();
    let changes = prepare_transition(&host, &snapshot, BTreeSet::from([root.id()]), true).unwrap();
    assert!(
        matches!(&changes[0].change, StateChange::Plugin { value: Some(value), .. }
        if value == &json!({ "count": 6 }))
    );
    host.load_source(
        PLUGIN,
        &(source(2, "function() error('failed reload closure') end") + "\nerror('reload failed')"),
    )
    .unwrap_err();
    let changes = prepare_transition(&host, &snapshot, BTreeSet::from([root.id()]), true).unwrap();
    assert!(
        matches!(&changes[0].change, StateChange::Plugin { value: Some(value), .. }
        if value == &json!({ "count": 6 }))
    );
    host.load_source(
        PLUGIN,
        &source(2, "function() error('scope mismatch closure') end")
            .replace("scope = \"agent\"", "scope = \"session\""),
    )
    .unwrap();
    let error =
        prepare_transition(&host, &snapshot, BTreeSet::from([root.id()]), true).unwrap_err();
    assert!(error.contains(TRANSITION_SCOPE), "{error}");
    host.unload(PLUGIN).unwrap();
    assert!(
        prepare_transition(&host, &snapshot, BTreeSet::from([root.id()]), true)
            .unwrap()
            .is_empty()
    );
    assert_eq!(root.manager().journal().snapshot().state, snapshot.state);
}

#[test_case(false, false, 1; "current_version_reload_before_claim")]
#[test_case(false, true, 1; "current_version_unload_before_claim")]
#[test_case(false, false, 2; "migrated_version_reload_before_claim")]
#[test_case(false, true, 2; "migrated_version_unload_before_claim")]
#[test_case(true, false, 2; "reload_after_claim")]
#[test_case(true, true, 2; "unload_after_claim")]
fn prepublication_generation_guard_orders_reload_against_publication(
    publish_first: bool,
    unload: bool,
    version: u32,
) {
    let (host, root, snapshot) = transition_fixture();
    host.load_source(
        PLUGIN,
        &source(version, "function(value) value.count = 5; return value end"),
    )
    .unwrap();
    let journal = root.manager().journal();
    let mut replacement = smol::block_on(common::bounded(
        journal.prepare_branch_restore(root.id(), snapshot.clone()),
    ))
    .unwrap();
    let publication = CheckpointPublication::default();
    let prepared = smol::block_on(common::bounded(host.event_handle().prepare_state_restore(
        replacement.snapshot().clone(),
        replacement.affected_agents().clone(),
        replacement.session_scope(),
        publication.clone(),
    )))
    .unwrap();
    assert_eq!(prepared.changes.len(), usize::from(version > 1));
    replacement.apply_migrations(prepared.changes).unwrap();
    if publish_first {
        (prepared.claim)(&publication).unwrap();
    }
    if unload {
        host.unload(PLUGIN).unwrap();
    } else {
        host.load_source(
            PLUGIN,
            &source(version, "function() error('new generation closure') end"),
        )
        .unwrap();
    }
    if publish_first {
        assert!(!publication.is_cancelled());
        replacement.install().unwrap();
        assert!(journal.snapshot().revision > snapshot.revision);
    } else {
        assert!(publication.is_cancelled());
        assert!((prepared.claim)(&publication).is_err());
        assert_eq!(journal.snapshot().revision, snapshot.revision);
        assert_eq!(journal.snapshot().state, snapshot.state);
    }
}

#[test]
fn prepublication_claim_guards_registered_absent_slots_and_request_identity() {
    let (host, root, snapshot) = transition_fixture();
    host.load_source(PLUGIN, &source(1, "nil")).unwrap();
    host.load_source(
        "other_state",
        "makima.state.register('absent', { scope = 'session', version = 1 })",
    )
    .unwrap();
    let publication = CheckpointPublication::default();
    let prepared = smol::block_on(common::bounded(host.event_handle().prepare_state_restore(
        snapshot.clone(),
        BTreeSet::from([root.id()]),
        true,
        publication.clone(),
    )))
    .unwrap();
    assert!(prepared.changes.is_empty());
    let unrelated = CheckpointPublication::default();
    assert!((prepared.claim)(&unrelated).is_err());
    assert!(unrelated.try_publish());
    host.unload("other_state").unwrap();
    assert!(publication.is_cancelled());
    assert!((prepared.claim)(&publication).is_err());
    assert_eq!(root.manager().journal().snapshot().state, snapshot.state);
}
