use std::sync::Arc;

use crossterm::event::{Event, KeyCode, KeyEvent, KeyModifiers};
use maki_agent::actor::{
    ActorEvent, AgentActorHandle, ConfigChange, ConfigPatch, PendingQueueEntry, PreparedModel,
    PreparedTransaction,
};
use maki_agent::{AgentError, AgentId, HistorySnapshot, ThinkingConfig};
use maki_commands::{CommandError, CommandOutcome, HostContextRequest, HostRequest};
use maki_config::SessionDefaults;
use maki_lua::ui_sink::UiSink;
use maki_providers::model::Model;
use maki_providers::provider::{BoxFuture, Provider};
use maki_providers::{
    Effort, Message, ModelInfo, ProviderEvent, RequestOptions, StopReason, StreamResponse,
    TokenUsage,
};
use maki_storage::checkpoint::CheckpointPublication;
use maki_storage::id::{MakiId, SessionRef};
use maki_storage::sessions::{Effort as StoredEffort, StoredThinking};
use ratatui::Terminal;
use ratatui::backend::TestBackend;
use ratatui::layout::Rect;
use test_case::test_case;

use super::tests::{select_child_chat, with_event_loop};
use super::{
    AppSession, CLOSED_TARGET_ERR, CommandEvent, EventLoop, PreparedProvider, SESSION_OP_TIMEOUT,
    SessionRequest, UiAction,
};
use crate::app::shell::ShellEvent;
use crate::chat::Chat;
use crate::components::Action;

const RESTORED_CHAT: &str = "restored task";
const PROMPT: &str = "session prompt";
const SHELL_OUTPUT: &str = "shell output";
const EXPANDER_PLUGIN: &str = "session_prompt_expanders";
const EXPANDER_FAILURE: &str = "reference expansion rejected";
const EXPANDED_PROMPT: &str = "expanded task";
const OVERSIZED_PROMPT_BYTES: usize = 300 * 1024;
const TODO_NAMESPACE: &str = "todo_write.todos";
const TODO_TOGGLE_DONE: &str = "todo toggle complete";
const PANEL_WIDTH: u16 = 80;
const PANEL_HEIGHT: u16 = 6;
const FOCUS_BARRIER: &str = "FocusProbeBarrier";
const FOCUS_CHILD_SPAWNED: &str = "focus child spawned";
const FOCUS_PROBE: &str = r#"
    local events = {}
    maki.api.create_autocmd({ "SessionFocusChanged", "AgentFocusChanged" }, {
        callback = function(ev, ctx)
            local agent_id = ev.agent and assert(ev.agent:id()) or nil
            if ev.event == "AgentFocusChanged" then
                assert(agent_id == ev.data.agent_id)
                assert((ctx ~= nil) == (ev.agent ~= nil))
                if ctx then
                    assert(assert(ctx:agent()):id() == agent_id)
                end
            end
            events[#events + 1] = {
                event = ev.event, data = ev.data,
                agent_id = agent_id, has_context = ctx ~= nil,
            }
        end,
    })
    maki.api.create_autocmd("FocusProbeBarrier", {
        callback = function()
            maki.ui.flash(#events == 0 and "[]" or assert(maki.json.encode(events)))
            events = {}
        end,
    })
    maki.api.register_command({
        name = "/spawn-focus-child", tui_only = false,
        handler = function(_, ctx)
            assert(maki.agent.spawn(ctx, { inherit_provider = true, silent = true }))
            maki.ui.flash("focus child spawned")
        end,
    })
"#;

fn install_plugin_host(event_loop: &mut EventLoop<'_>) -> maki_lua::PluginHost {
    let host = maki_lua::PluginHost::with_session_provider_preparer(
        Arc::clone(maki_agent::tools::ToolRegistry::global_arc()),
        event_loop.ctx.command_runtime.registry.clone(),
        false,
        Arc::new(maki_lua::test_support::InMemoryFs::new()),
        Some(event_loop.ctx.storage.path().to_path_buf()),
        None,
        maki_lua::ui_sink::HostUi::Frontend,
    )
    .unwrap();
    event_loop.ctx.lua_event_handle = host.event_handle();
    event_loop.sessions[0].app.lua_event_handle = host.event_handle();
    host.event_handle()
        .install_orchestration_services(event_loop.orchestration.clone());
    host
}

pub(super) fn install_todo_plugin(event_loop: &mut EventLoop<'_>) -> maki_lua::PluginHost {
    let host = install_plugin_host(event_loop);
    let source = format!(
        r#"
        local register_keymap = maki.keymap.set
        maki.keymap.set = function(mode, key, callback, opts)
            return register_keymap(mode, key, function(...)
                callback(...)
                maki.ui.flash("{TODO_TOGGLE_DONE}")
            end, opts)
        end
        {}
        maki.keymap.set = register_keymap
        "#,
        include_str!("../../../plugins/todo_write/init.lua"),
    );
    host.load_source("todo_write", &source).unwrap();
    event_loop.ctx.keymap_reader = host.keymap_reader();
    event_loop.sessions[0].app.keymap_reader = host.keymap_reader();
    host
}

pub(super) fn seed_todos(actor: &AgentActorHandle, content: &str) -> serde_json::Value {
    let items = serde_json::json!([{ "content": content, "status": "pending" }]);
    let mut snapshot = actor
        .prepare_idle()
        .unwrap()
        .source_snapshot()
        .unwrap()
        .history;
    Arc::make_mut(&mut snapshot.state).set(
        snapshot.messages.len(),
        TODO_NAMESPACE.into(),
        items.clone(),
    );
    replace_actor_history(actor, snapshot);
    assert_eq!(actor.history_state(TODO_NAMESPACE), Some(items.clone()));
    items
}

fn replace_actor_history(actor: &AgentActorHandle, snapshot: HistorySnapshot) {
    smol::block_on(async {
        let guard = actor.prepare_idle().unwrap();
        let operation = guard.reserve_prepared_operation(None).unwrap();
        operation.resolve(None).unwrap();
        operation.wait_ready().await.unwrap();
        let transaction = PreparedTransaction {
            publication: CheckpointPublication::default(),
            publish: Box::new(|_, publication| {
                Box::pin(async move {
                    assert!(publication.try_publish());
                    Ok(())
                })
            }),
            finalize: Box::new(|_| {}),
        };
        let (held, _, _) = guard
            .commit_prepared_transaction_held(operation, Some(snapshot), transaction)
            .await
            .unwrap();
        drop(held);
    });
}

pub(super) fn settle_todo_ui(event_loop: &mut EventLoop<'_>, host: &maki_lua::PluginHost) {
    host.drain_tasks_for_test();
    for action in host.ui_action_rx().drain() {
        event_loop.render(action);
    }
    for runtime in &mut event_loop.sessions {
        let _ = runtime.app.float_mgr.tick();
    }
}

pub(super) fn toggle_todo_panel(event_loop: &mut EventLoop<'_>, host: &maki_lua::PluginHost) {
    event_loop.handle_input(Event::Key(KeyEvent::new(
        KeyCode::Char('t'),
        KeyModifiers::CONTROL,
    )));
    loop {
        let action = host
            .ui_action_rx()
            .recv_timeout(SESSION_OP_TIMEOUT)
            .unwrap();
        if matches!(&action, UiAction::Flash(text) if text == TODO_TOGGLE_DONE) {
            break;
        }
        event_loop.render(action);
    }
    settle_todo_ui(event_loop, host);
}

fn panel_text(event_loop: &mut EventLoop<'_>, session: usize) -> String {
    let app = &mut event_loop.sessions[session].app;
    let panels = app.float_mgr.panel_reqs();
    assert_eq!(panels.len(), 1);
    let mut terminal = Terminal::new(TestBackend::new(PANEL_WIDTH, PANEL_HEIGHT)).unwrap();
    terminal
        .draw(|frame| {
            app.float_mgr.view_panel(
                frame,
                panels[0].0,
                Rect::new(0, 0, PANEL_WIDTH, PANEL_HEIGHT),
            );
        })
        .unwrap();
    terminal
        .backend()
        .buffer()
        .content
        .iter()
        .map(|cell| cell.symbol())
        .collect()
}

pub(super) fn assert_todo_panel(
    event_loop: &mut EventLoop<'_>,
    session: usize,
    content: &str,
    visible: bool,
) {
    let panels = event_loop.sessions[session].app.float_mgr.panel_reqs();
    assert_eq!(
        panels.len(),
        usize::from(visible),
        "session {session}: {content}"
    );
    if visible {
        assert!(
            panel_text(event_loop, session).contains(content),
            "session {session}: {content}"
        );
    }
}

fn install_expanders(event_loop: &mut EventLoop<'_>) -> maki_lua::PluginHost {
    let host = install_plugin_host(event_loop);
    host.load_source(
        EXPANDER_PLUGIN,
        &format!(
            r#"
        maki.api.register_expander("ok", function(_) return "{EXPANDED_PROMPT}", nil end)
        maki.api.register_expander("reject", function(_) return nil, "{EXPANDER_FAILURE}" end)
    "#
        ),
    )
    .unwrap();
    host
}

fn select_restored_chat(event_loop: &mut EventLoop<'_>) {
    let runtime = Arc::clone(&event_loop.ctx.command_runtime);
    let app = &mut event_loop.sessions[0].app;
    app.command_runtime = Arc::clone(&runtime);
    app.command_target = runtime.bind_target();
    event_loop.sync_orchestration();
    assert!(
        runtime
            .target_agent(&event_loop.sessions[0].app.command_target)
            .is_some()
    );
    let app = &mut event_loop.sessions[0].app;
    let mut chat = Chat::new(
        RESTORED_CHAT.into(),
        app.ui_config.clone(),
        app.lua_event_handle.clone(),
        Arc::clone(&app.theme_provider),
    );
    chat.subagent_id = Some(RESTORED_CHAT.into());
    app.chats.push(chat);
    app.active_chat = 1;
    event_loop.sync_orchestration();
    assert!(
        runtime
            .target_agent(&event_loop.sessions[0].app.command_target)
            .is_none()
    );
    assert!(event_loop.selected_target(0).is_none());
}

fn focus_events(host: &maki_lua::PluginHost) -> Vec<serde_json::Value> {
    host.event_handle()
        .fire_autocmd(FOCUS_BARRIER, serde_json::Value::Null);
    let UiAction::Flash(text) = host
        .ui_action_rx()
        .recv_timeout(SESSION_OP_TIMEOUT)
        .unwrap()
    else {
        panic!("expected focus observations");
    };
    serde_json::from_str(&text).unwrap()
}

fn assert_agent_focus(event: &serde_json::Value, session_id: MakiId, agent_id: Option<AgentId>) {
    let agent_id = agent_id.map(|id| id.to_string());
    assert_eq!(event["event"], "AgentFocusChanged");
    assert_eq!(event["data"]["session_id"], session_id.to_string());
    assert_eq!(event["data"]["agent_id"], serde_json::json!(agent_id));
    assert_eq!(event["agent_id"], serde_json::json!(agent_id));
    assert_eq!(event["has_context"], agent_id.is_some());
}

fn focus_chat(event_loop: &mut EventLoop<'_>, session: usize, chat: usize) {
    event_loop.set_focused(session);
    event_loop.sessions[session].app.active_chat = chat;
    event_loop.sync_orchestration();
    event_loop.emit_focus_change();
}

#[test_case("live"; "pinned_live_session")]
#[test_case("missing"; "pinned_closed_session")]
#[test_case("focused"; "unpinned_legacy_focus")]
fn window_open_routes_to_its_origin_session(origin: &str) {
    with_event_loop(|event_loop| {
        let host = install_plugin_host(event_loop);
        let first_id = event_loop.sessions[0].id();
        let session = AppSession::new("test-model", &event_loop.session_cwd);
        let runtime = event_loop.ctx.spawn_runtime(session).unwrap();
        let second = event_loop.push_runtime(runtime);
        event_loop.set_focused(second);
        host.load_source("window_route_probe", "maki.ui.open_win(maki.ui.buf(), {})")
            .unwrap();
        let UiAction::OpenWin {
            buf,
            config,
            focus,
            event_tx,
            cmd_rx,
            ..
        } = host
            .ui_action_rx()
            .recv_timeout(SESSION_OP_TIMEOUT)
            .unwrap()
        else {
            panic!("expected window action");
        };
        let session_id = match origin {
            "live" => Some(first_id),
            "missing" => Some(MakiId::generate()),
            "focused" => None,
            _ => unreachable!(),
        };
        event_loop.render(UiAction::OpenWin {
            buf,
            config,
            focus,
            event_tx,
            cmd_rx,
            session_id,
        });
        assert_eq!(
            event_loop.sessions[0].app.float_mgr.is_open(),
            origin == "live"
        );
        assert_eq!(
            event_loop.sessions[second].app.float_mgr.is_open(),
            origin == "focused"
        );
        assert_eq!(event_loop.focused, second);
    });
}

#[test]
fn queued_todo_window_keeps_session_after_native_focus_moves() {
    const FIRST: &str = "queued session A task";
    const SECOND: &str = "queued session B task";
    with_event_loop(|event_loop| {
        let host = install_todo_plugin(event_loop);
        let (manager, root) = event_loop.sessions[0].handles.manager_and_root();
        seed_todos(&manager.actor(root).unwrap(), FIRST);
        let session = AppSession::new("test-model", &event_loop.session_cwd);
        let runtime = event_loop.ctx.spawn_runtime(session).unwrap();
        let (second_manager, second_root) = runtime.handles.manager_and_root();
        seed_todos(&second_manager.actor(second_root).unwrap(), SECOND);
        let second = event_loop.push_runtime(runtime);

        focus_chat(event_loop, 0, 0);
        host.drain_tasks_for_test();
        let queued: Vec<_> = host.ui_action_rx().drain().collect();
        assert!(
            queued
                .iter()
                .any(|action| matches!(action, UiAction::OpenWin { .. }))
        );
        event_loop.set_focused(second);
        event_loop.sync_orchestration();
        for action in queued {
            event_loop.render(action);
        }
        settle_todo_ui(event_loop, &host);
        assert!(event_loop.sessions[0].app.float_mgr.is_open());
        assert!(!event_loop.sessions[second].app.float_mgr.is_open());
        assert_eq!(event_loop.focused, second);
        event_loop.emit_focus_change();
        settle_todo_ui(event_loop, &host);
        toggle_todo_panel(event_loop, &host);
        assert_todo_panel(event_loop, second, SECOND, true);
        focus_chat(event_loop, 0, 0);
        settle_todo_ui(event_loop, &host);
        toggle_todo_panel(event_loop, &host);
        assert_todo_panel(event_loop, 0, FIRST, true);
        assert!(!panel_text(event_loop, 0).contains(SECOND));
    });
}

#[test]
fn todo_panel_refreshes_after_rewind_resume_and_reset() {
    const ORIGINAL: &str = "todo before rewind";
    const LATER: &str = "todo after later update";
    const BOUNDARY: &str = "rewind boundary";
    with_event_loop(|event_loop| {
        let host = install_todo_plugin(event_loop);
        let (manager, root) = event_loop.sessions[0].handles.manager_and_root();
        let actor = manager.actor(root).unwrap();
        let original = seed_todos(&actor, ORIGINAL);
        focus_chat(event_loop, 0, 0);
        settle_todo_ui(event_loop, &host);
        toggle_todo_panel(event_loop, &host);
        assert_todo_panel(event_loop, 0, ORIGINAL, true);

        let mut snapshot = actor
            .prepare_idle()
            .unwrap()
            .source_snapshot()
            .unwrap()
            .history;
        Arc::make_mut(&mut snapshot.messages).push(Message::user(BOUNDARY.into()));
        Arc::make_mut(&mut snapshot.state).set(
            snapshot.messages.len(),
            TODO_NAMESPACE.into(),
            serde_json::json!([{ "content": LATER, "status": "pending" }]),
        );
        let events = actor.subscribe_events();
        let refresh = |event_loop: &mut EventLoop<'_>| {
            loop {
                if matches!(
                    events.recv_timeout(SESSION_OP_TIMEOUT).unwrap(),
                    ActorEvent::HistoryStateChanged { namespace: None }
                ) {
                    break;
                }
            }
            host.event_handle().fire_agent_autocmd(
                "HistoryStateChanged",
                manager.lookup(root).unwrap(),
                serde_json::json!({ "namespace": null }),
            );
            settle_todo_ui(event_loop, &host);
        };
        replace_actor_history(&actor, snapshot);
        refresh(event_loop);
        assert_todo_panel(event_loop, 0, LATER, true);
        let boundary = actor
            .history_boundaries()
            .unwrap()
            .into_iter()
            .next()
            .unwrap();
        let (_, rewind) = actor.rewind_snapshot(&boundary).unwrap();
        replace_actor_history(&actor, rewind);
        refresh(event_loop);
        assert_todo_panel(event_loop, 0, ORIGINAL, true);
        assert_eq!(actor.history_state(TODO_NAMESPACE), Some(original.clone()));

        let mut session = event_loop.sessions[0].app.state.session.as_ref().clone();
        let history = actor
            .prepare_idle()
            .unwrap()
            .source_snapshot()
            .unwrap()
            .history;
        session.adopt_history(&history);
        let session_id = session.id;
        event_loop.replace_runtime(0, session).unwrap();
        focus_chat(event_loop, 0, 0);
        settle_todo_ui(event_loop, &host);
        assert_eq!(event_loop.sessions[0].id(), session_id);
        let (resumed_manager, resumed_root) = event_loop.sessions[0].handles.manager_and_root();
        assert_ne!(root, resumed_root);
        let resumed = resumed_manager.actor(resumed_root).unwrap();
        assert_eq!(resumed.history_state(TODO_NAMESPACE), Some(original));
        toggle_todo_panel(event_loop, &host);
        assert_todo_panel(event_loop, 0, ORIGINAL, true);

        replace_actor_history(&resumed, HistorySnapshot::default());
        host.event_handle().fire_agent_autocmd(
            "HistoryStateChanged",
            resumed_manager.lookup(resumed_root).unwrap(),
            serde_json::json!({ "namespace": null }),
        );
        settle_todo_ui(event_loop, &host);
        assert_todo_panel(event_loop, 0, ORIGINAL, false);
        assert_eq!(resumed.history_state(TODO_NAMESPACE), None);
    });
}

#[test]
fn todo_panel_tracks_session_and_child_window_ownership() {
    const FIRST: &str = "session A task";
    const SECOND: &str = "session B task";
    const CHILD: &str = "session A child task";
    const SPAWNED: &str = "panel child spawned";
    const SPAWN_PANEL_CHILD: &str = r#"
        maki.api.register_command({
            name = "/spawn-panel-child", tui_only = false,
            handler = function(_, ctx)
                assert(maki.agent.spawn(ctx, { inherit_provider = true, silent = true }))
                maki.ui.flash("panel child spawned")
            end,
        })
    "#;

    with_event_loop(|event_loop| {
        let host = install_todo_plugin(event_loop);
        let (manager, root) = event_loop.sessions[0].handles.manager_and_root();
        let first_actor = manager.actor(root).unwrap();
        let first_items = seed_todos(&first_actor, FIRST);
        focus_chat(event_loop, 0, 0);
        settle_todo_ui(event_loop, &host);
        assert_todo_panel(event_loop, 0, FIRST, false);
        toggle_todo_panel(event_loop, &host);
        assert_todo_panel(event_loop, 0, FIRST, true);
        assert!(!panel_text(event_loop, 0).contains(SECOND));

        let session = AppSession::new("test-model", &event_loop.session_cwd);
        let mut runtime = event_loop.ctx.spawn_runtime(session).unwrap();
        runtime.app.keymap_reader = host.keymap_reader();
        let (second_manager, second_root) = runtime.handles.manager_and_root();
        let second_actor = second_manager.actor(second_root).unwrap();
        let second_items = seed_todos(&second_actor, SECOND);
        settle_todo_ui(event_loop, &host);
        let second = event_loop.push_runtime(runtime);
        focus_chat(event_loop, second, 0);
        settle_todo_ui(event_loop, &host);
        assert_todo_panel(event_loop, second, SECOND, false);
        toggle_todo_panel(event_loop, &host);
        assert_todo_panel(event_loop, second, SECOND, true);
        assert!(!panel_text(event_loop, second).contains(FIRST));
        toggle_todo_panel(event_loop, &host);
        assert_todo_panel(event_loop, second, SECOND, false);

        focus_chat(event_loop, 0, 0);
        settle_todo_ui(event_loop, &host);
        assert_todo_panel(event_loop, 0, FIRST, true);
        assert!(!panel_text(event_loop, 0).contains(SECOND));
        toggle_todo_panel(event_loop, &host);
        assert_todo_panel(event_loop, 0, FIRST, false);
        assert_todo_panel(event_loop, second, SECOND, false);
        toggle_todo_panel(event_loop, &host);
        assert_todo_panel(event_loop, 0, FIRST, true);

        host.load_source("spawn_panel_child", SPAWN_PANEL_CHILD)
            .unwrap();
        smol::block_on(event_loop.ctx.command_runtime.registry.dispatch_input(
            &event_loop.sessions[0].app.command_target,
            "/spawn-panel-child".into(),
        ));
        assert!(matches!(
            host.ui_action_rx().recv_timeout(SESSION_OP_TIMEOUT).unwrap(),
            UiAction::Flash(text) if text == SPAWNED
        ));
        let child = manager
            .snapshot()
            .into_iter()
            .find(|node| node.parent_id == Some(root))
            .unwrap()
            .agent_id;
        let child_actor = manager.actor(child).unwrap();
        let child_items = seed_todos(&child_actor, CHILD);
        settle_todo_ui(event_loop, &host);
        select_child_chat(event_loop, child, CHILD);
        event_loop.emit_focus_change();
        settle_todo_ui(event_loop, &host);
        assert_todo_panel(event_loop, 0, CHILD, false);
        toggle_todo_panel(event_loop, &host);
        assert_todo_panel(event_loop, 0, CHILD, true);
        assert!(!panel_text(event_loop, 0).contains(FIRST));
        assert_todo_panel(event_loop, second, SECOND, false);
        focus_chat(event_loop, 0, 0);
        settle_todo_ui(event_loop, &host);
        assert_todo_panel(event_loop, 0, FIRST, true);
        assert!(!panel_text(event_loop, 0).contains(CHILD));
        assert_eq!(first_actor.history_state(TODO_NAMESPACE), Some(first_items));
        assert_eq!(
            second_actor.history_state(TODO_NAMESPACE),
            Some(second_items)
        );
        assert_eq!(child_actor.history_state(TODO_NAMESPACE), Some(child_items));
    });
}

#[test]
fn ui_harness_focus_notifications_smoke() {
    with_event_loop(|event_loop| {
        let host = install_plugin_host(event_loop);
        host.load_source("focus_probe", FOCUS_PROBE).unwrap();
        let session_id = event_loop.sessions[0].id();
        let (manager, root) = event_loop.sessions[0].handles.manager_and_root();
        focus_chat(event_loop, 0, 0);
        let events = focus_events(&host);
        assert_eq!(events.len(), 2);
        assert_eq!(events[0]["event"], "SessionFocusChanged");
        assert!(events[0]["data"]["previous_session_id"].is_null());
        assert_agent_focus(&events[1], session_id, Some(root));

        focus_chat(event_loop, 0, 0);
        assert!(focus_events(&host).is_empty());
        for _ in 0..2 {
            smol::block_on(event_loop.ctx.command_runtime.registry.dispatch_input(
                &event_loop.sessions[0].app.command_target,
                "/spawn-focus-child".into(),
            ));
            assert!(matches!(
                host.ui_action_rx().recv_timeout(SESSION_OP_TIMEOUT).unwrap(),
                UiAction::Flash(text) if text == FOCUS_CHILD_SPAWNED
            ));
        }
        let children: Vec<_> = manager
            .snapshot()
            .into_iter()
            .filter(|node| node.parent_id == Some(root))
            .map(|node| node.agent_id)
            .collect();
        assert_eq!(children.len(), 2);
        for child in &children {
            select_child_chat(event_loop, *child, RESTORED_CHAT);
            event_loop.emit_focus_change();
            let events = focus_events(&host);
            assert_eq!(events.len(), 1);
            assert_agent_focus(&events[0], session_id, Some(*child));
        }
        manager.close_subtree(children[1]).unwrap();
        event_loop.sync_orchestration();
        event_loop.emit_focus_change();
        let events = focus_events(&host);
        assert_eq!(events.len(), 1);
        assert_agent_focus(&events[0], session_id, None);

        focus_chat(event_loop, 0, 0);
        let events = focus_events(&host);
        assert_eq!(events.len(), 1);
        assert_agent_focus(&events[0], session_id, Some(root));
        event_loop.sessions[0].app.chats[1].agent_id = Some(AgentId::generate());
        focus_chat(event_loop, 0, 1);
        let events = focus_events(&host);
        assert_eq!(events.len(), 1);
        assert_agent_focus(&events[0], session_id, None);
        event_loop.sessions[0].app.chats[1].agent_id = None;
        focus_chat(event_loop, 0, 1);
        assert!(focus_events(&host).is_empty());

        let session = AppSession::new("test-model", &event_loop.session_cwd);
        let second_id = session.id;
        let runtime = event_loop.ctx.spawn_runtime(session).unwrap();
        let (_, second_root) = runtime.handles.manager_and_root();
        let second = event_loop.push_runtime(runtime);
        focus_chat(event_loop, second, 0);
        let events = focus_events(&host);
        assert_eq!(events.len(), 2);
        assert_eq!(events[0]["event"], "SessionFocusChanged");
        assert_eq!(
            events[0]["data"]["previous_session_id"],
            session_id.to_string()
        );
        assert_agent_focus(&events[1], second_id, Some(second_root));
        focus_chat(event_loop, 0, 1);
        let events = focus_events(&host);
        assert_eq!(events.len(), 2);
        assert_agent_focus(&events[1], session_id, None);
    });
}

#[test]
fn resumed_agent_focus_notifies_without_changing_session_focus() {
    with_event_loop(|event_loop| {
        let host = install_plugin_host(event_loop);
        host.load_source("focus_probe", FOCUS_PROBE).unwrap();
        focus_chat(event_loop, 0, 0);
        assert_eq!(focus_events(&host).len(), 2);
        let (_, previous) = event_loop.sessions[0].handles.manager_and_root();
        let session = event_loop.sessions[0].app.state.session.as_ref().clone();
        let session_id = session.id;
        event_loop.replace_runtime(0, session).unwrap();
        let (_, resumed) = event_loop.sessions[0].handles.manager_and_root();
        assert_ne!(previous, resumed);
        focus_chat(event_loop, 0, 0);
        let events = focus_events(&host);
        assert_eq!(events.len(), 1);
        assert_agent_focus(&events[0], session_id, Some(resumed));
    });
}

#[test]
fn event_loop_delayed_focus_query_smoke() {
    const QUERY_ENTERED: &str = "focus query held";
    const RELEASE_QUERY: &str = "ReleaseFocusQuery";
    const QUERY_PROBE: &str = r#"
        local selected, release, hold = nil, nil, true
        local function query(agent, ctx)
            local value, err = agent:history_get(ctx, "focus_probe")
            assert(err == nil)
            if hold then
                hold = false
                maki.async.await(1, function(done)
                    release = done
                    maki.ui.flash("focus query held")
                end)
            end
            return value
        end
        maki.api.create_autocmd("AgentFocusChanged", {
            callback = function(ev, ctx)
                selected = ev.data.agent_id
                if not ev.agent then return end
                local agent_id = assert(ev.agent:id())
                local value = query(ev.agent, ctx)
                maki.ui.flash(assert(maki.json.encode({
                    agent_id = agent_id, selected = selected, value = value,
                })))
            end,
        })
        maki.api.create_autocmd("ReleaseFocusQuery", {
            callback = function() assert(release)(true) end,
        })
    "#;

    with_event_loop(|event_loop| {
        let host = install_plugin_host(event_loop);
        host.load_source("delayed_focus_probe", QUERY_PROBE)
            .unwrap();
        let (_, first) = event_loop.sessions[0].handles.manager_and_root();
        focus_chat(event_loop, 0, 0);
        assert!(matches!(
            host.ui_action_rx().recv_timeout(SESSION_OP_TIMEOUT).unwrap(),
            UiAction::Flash(text) if text == QUERY_ENTERED
        ));
        let session = AppSession::new("test-model", &event_loop.session_cwd);
        let runtime = event_loop.ctx.spawn_runtime(session).unwrap();
        let (_, second) = runtime.handles.manager_and_root();
        let index = event_loop.push_runtime(runtime);
        focus_chat(event_loop, index, 0);
        let receive = || {
            let UiAction::Flash(text) = host
                .ui_action_rx()
                .recv_timeout(SESSION_OP_TIMEOUT)
                .unwrap()
            else {
                panic!("expected focus query result");
            };
            serde_json::from_str::<serde_json::Value>(&text).unwrap()
        };
        let current = receive();
        assert_eq!(current["agent_id"], second.to_string());
        assert_eq!(current["selected"], second.to_string());
        host.event_handle()
            .fire_autocmd(RELEASE_QUERY, serde_json::Value::Null);
        let delayed = receive();
        assert_eq!(delayed["agent_id"], first.to_string());
        assert_eq!(delayed["selected"], second.to_string());
    });
}

fn dispatch_slash(event_loop: &mut EventLoop<'_>, command: &str) -> CommandOutcome {
    let runtime = Arc::clone(&event_loop.ctx.command_runtime);
    runtime.dispatch_input(&event_loop.sessions[0].app.command_target, command.into());
    loop {
        let event = runtime.recv_timeout_for_test(SESSION_OP_TIMEOUT).unwrap();
        if let CommandEvent::Outcome { outcome, .. } = event {
            return outcome;
        }
        event_loop.handle_command(event);
    }
}

#[test_case("/compact"; "compact")]
#[test_case("/cd /"; "directory")]
#[test_case("/fast"; "fast")]
#[test_case("/workflow"; "workflow")]
#[test_case("/model"; "model_picker")]
#[test_case("/model anthropic/claude-sonnet-4-6"; "model_selection")]
fn closed_agent_rejects_without_root_fallback(command: &str) {
    with_event_loop(|event_loop| {
        select_restored_chat(event_loop);
        let runtime = Arc::clone(&event_loop.ctx.command_runtime);
        let (manager, root) = event_loop.sessions[0].handles.manager_and_root();
        let actor = manager.actor(root).unwrap();
        let config = actor.effective_config().unwrap();
        let history = event_loop.sessions[0].handles.history.load_full();
        let _guard = actor.prepare_idle().unwrap();
        runtime.dispatch_input(&event_loop.sessions[0].app.command_target, command.into());
        loop {
            let event = runtime.recv_timeout_for_test(SESSION_OP_TIMEOUT).unwrap();
            let finished = matches!(event, CommandEvent::Outcome { .. });
            if let CommandEvent::Outcome { outcome, .. } = &event {
                assert!(matches!(
                    outcome,
                    CommandOutcome::Failed(CommandError::StaleTarget)
                ));
            }
            event_loop.handle_command(event);
            if finished {
                break;
            }
        }
        assert_eq!(
            event_loop.sessions[0].app.status_bar.flash_text(),
            Some(CommandError::StaleTarget.to_string().as_str())
        );
        assert!(Arc::ptr_eq(&config, &actor.effective_config().unwrap()));
        assert!(Arc::ptr_eq(
            &history,
            &event_loop.sessions[0].handles.history.load_full()
        ));
        assert!(actor.pending_queue().is_empty());
    });
}

#[test]
fn live_compact_commands_preserve_guidance_and_busy_order() {
    const FIRST: &str = "retain implementation details";
    const SECOND: &str = "retain test results";
    with_event_loop(|event_loop| {
        select_restored_chat(event_loop);
        event_loop.sessions[0].app.active_chat = 0;
        event_loop.sync_orchestration();
        let (manager, root) = event_loop.sessions[0].handles.manager_and_root();
        let actor = manager.actor(root).unwrap();
        let _guard = actor.prepare_idle().unwrap();
        assert!(matches!(
            dispatch_slash(event_loop, &format!("/compact {FIRST}")),
            CommandOutcome::Completed
        ));
        assert!(matches!(
            dispatch_slash(event_loop, &format!("/compact {SECOND}")),
            CommandOutcome::Completed
        ));
        let pending = actor.pending_queue();
        assert_eq!(pending.len(), 2);
        let [
            PendingQueueEntry::Compact {
                operation_id: first,
                instructions: Some(first_text),
                ..
            },
            PendingQueueEntry::Compact {
                operation_id: second,
                instructions: Some(second_text),
                ..
            },
        ] = pending.as_slice()
        else {
            panic!("expected two queued compacts");
        };
        assert_eq!(first_text, FIRST);
        assert_eq!(second_text, SECOND);
        assert_ne!(first, second);
        assert!(actor.remove_pending_compact(*second).is_some());
        assert!(
            matches!(actor.pending_queue().as_slice(), [PendingQueueEntry::Compact { instructions: Some(text), .. }] if text == FIRST)
        );
    });
}

#[test_case("/fast"; "fast")]
#[test_case("/workflow"; "workflow")]
#[test_case("/cd /"; "directory")]
fn live_slash_controls_commit_on_the_admitted_actor(command: &str) {
    with_event_loop(|event_loop| {
        select_restored_chat(event_loop);
        event_loop.sessions[0].app.active_chat = 0;
        let (manager, root) = event_loop.sessions[0].handles.manager_and_root();
        let actor = manager.actor(root).unwrap();
        if command == "/fast" {
            let config = actor.effective_config().unwrap();
            let mut model = config.model.clone();
            model.supports_fast_override = Some(maki_providers::model::FastSupport::Supported);
            let ticket = actor.reserve_prepared_operation(None).unwrap();
            smol::block_on(
                ticket.commit_when_ready(Some(ConfigChange::Patch(ConfigPatch {
                    model: Some(PreparedModel {
                        model,
                        provider: Arc::clone(&config.settings.provider),
                    }),
                    ..Default::default()
                }))),
            )
            .unwrap();
        }
        event_loop.sync_orchestration();
        let previous = actor.effective_config().unwrap();
        let commits = actor.subscribe_config_commits();
        assert!(matches!(
            dispatch_slash(event_loop, command),
            CommandOutcome::Completed
        ));
        let commit = commits.recv_timeout(SESSION_OP_TIMEOUT).unwrap();
        match command {
            "/fast" => assert_eq!(commit.config.fast, !previous.fast),
            "/workflow" => assert_eq!(commit.config.workflow, !previous.workflow),
            _ => assert_eq!(commit.config.context.cwd(), std::path::Path::new("/")),
        }
    });
}

#[test_case(HostContextRequest::WorkingDirectory; "directory")]
#[test_case(HostContextRequest::FastModeSupported; "fast")]
#[test_case(HostContextRequest::ModelSpecs; "models")]
fn restored_context_queries_reject_missing_admission(request: HostContextRequest) {
    with_event_loop(|event_loop| {
        select_restored_chat(event_loop);
        let (reply, response) = flume::bounded(1);
        event_loop.handle_command(CommandEvent::Host {
            target: event_loop.sessions[0].app.command_target.id(),
            request: HostRequest::Context(request),
            admission: None,
            reply,
        });
        assert!(matches!(
            response.recv().unwrap(),
            Err(CommandError::StaleTarget)
        ));
    });
}

#[test_case(Action::Compact(None); "compact")]
#[test_case(Action::ChangeDirectory("/".into()); "directory")]
#[test_case(Action::ChangeMode("build".into()); "mode")]
fn restored_actions_report_closed_target(action: Action) {
    with_event_loop(|event_loop| {
        select_restored_chat(event_loop);
        let (manager, root) = event_loop.sessions[0].handles.manager_and_root();
        let actor = manager.actor(root).unwrap();
        let config = actor.effective_config().unwrap();
        let asynchronous = matches!(action, Action::ChangeMode(_));
        event_loop.handle_action(0, action);
        if asynchronous {
            let event = event_loop
                .internal_rx
                .recv_timeout(SESSION_OP_TIMEOUT)
                .unwrap();
            event_loop.handle_internal(event);
        }
        assert!(
            event_loop.sessions[0]
                .app
                .status_bar
                .flash_text()
                .unwrap()
                .contains(CLOSED_TARGET_ERR)
        );
        assert!(Arc::ptr_eq(&config, &actor.effective_config().unwrap()));
        assert!(actor.pending_queue().is_empty());
    });
}

#[test_case(false; "agent_turn")]
#[test_case(true; "isolated_turn")]
fn restored_custom_turn_commands_have_no_root_authority(isolated: bool) {
    struct CustomTurn(bool);
    impl maki_commands::CommandBehavior for CustomTurn {
        fn execute(
            &self,
            _: maki_commands::CommandInvocation,
        ) -> maki_commands::CommandFuture<Result<CommandOutcome, CommandError>> {
            let content = PROMPT.into();
            let outcome = if self.0 {
                CommandOutcome::IsolatedTurn(maki_commands::IsolatedTurn { content })
            } else {
                CommandOutcome::AgentTurn(maki_commands::AgentTurn {
                    content,
                    prompt: None,
                })
            };
            Box::pin(async move { Ok(outcome) })
        }
    }
    with_event_loop(|event_loop| {
        select_restored_chat(event_loop);
        let runtime = Arc::clone(&event_loop.ctx.command_runtime);
        let producer = runtime
            .registry
            .create_producer(maki_commands::ProducerPrecedence::Application);
        producer
            .replace(vec![maki_commands::Registration {
                spec: maki_commands::CommandSpec {
                    name: Arc::from("/restored-turn"),
                    aliases: Arc::from([]),
                    arguments: maki_commands::CommandArguments::Raw { required: false },
                    docs: maki_commands::CommandDocs {
                        summary: Arc::from(PROMPT),
                        argument_hint: None,
                    },
                    required_capabilities: maki_commands::TargetCapabilities::NONE,
                },
                behavior: Arc::new(CustomTurn(isolated)),
                argument_completions: Vec::new(),
            }])
            .unwrap();
        let history = event_loop.sessions[0].handles.history.load_full();
        let (manager, _) = event_loop.sessions[0].handles.manager_and_root();
        let actors = manager.snapshot().len();
        runtime.dispatch_input(
            &event_loop.sessions[0].app.command_target,
            "/restored-turn".into(),
        );
        event_loop.handle_command(runtime.recv_timeout_for_test(SESSION_OP_TIMEOUT).unwrap());
        assert_eq!(
            event_loop.sessions[0].app.status_bar.flash_text(),
            Some(CommandError::StaleTarget.to_string().as_str())
        );
        assert!(event_loop.sessions[0].handles.queue.is_empty());
        assert!(Arc::ptr_eq(
            &history,
            &event_loop.sessions[0].handles.history.load_full()
        ));
        assert_eq!(manager.snapshot().len(), actors);
    });
}

#[test_case(true; "restored_child")]
#[test_case(false; "live_child_pinned_across_focus_switch")]
fn registered_lua_mode_set_uses_its_invocation_agent(restored: bool) {
    const SPAWNED: &str = "mode child spawned";
    const DONE: &str = "public mode setter completed";
    with_event_loop(|event_loop| {
        select_restored_chat(event_loop);
        let host = install_expanders(event_loop);
        host.event_handle()
            .install_orchestration_services(event_loop.orchestration.clone());
        host.load_source(
            "public_mode_command",
            &format!(
                r#"
            maki.api.register_command({{
                name = "/spawn-mode-child", tui_only = false,
                handler = function(_, ctx)
                    assert(maki.agent.spawn(ctx, {{ inherit_provider = true, silent = true }}))
                    maki.ui.flash("{SPAWNED}")
                end,
            }})
            maki.api.register_command({{
                name = "/set-public-mode", tui_only = false,
                handler = function(_, ctx)
                    assert((ctx ~= nil) == {live})
                    if ctx then
                        local agent = assert(ctx:agent())
                        assert(agent:set_mode(ctx, "plan"))
                    end
                    maki.ui.flash("{DONE}")
                end,
            }})
        "#,
                live = !restored
            ),
        )
        .unwrap();
        let command_runtime = Arc::clone(&event_loop.ctx.command_runtime);
        let (manager, root) = event_loop.sessions[0].handles.manager_and_root();
        let root_actor = manager.actor(root).unwrap();
        let original = root_actor.effective_config().unwrap();
        let child = if restored {
            None
        } else {
            event_loop.sessions[0].app.active_chat = 0;
            event_loop.sync_orchestration();
            smol::block_on(command_runtime.registry.dispatch_input(
                &event_loop.sessions[0].app.command_target,
                "/spawn-mode-child".into(),
            ));
            assert!(
                matches!(host.ui_action_rx().recv_timeout(SESSION_OP_TIMEOUT).unwrap(), UiAction::Flash(text) if text == SPAWNED)
            );
            let child = manager
                .snapshot()
                .into_iter()
                .find(|node| node.parent_id == Some(root))
                .unwrap()
                .agent_id;
            select_child_chat(event_loop, child, RESTORED_CHAT);
            Some(child)
        };
        let registry = command_runtime.registry.clone();
        let target = event_loop.sessions[0].app.command_target.clone();
        let command = smol::spawn(async move {
            registry
                .dispatch_input(&target, "/set-public-mode".into())
                .await
        });
        assert!(
            matches!(host.ui_action_rx().recv_timeout(SESSION_OP_TIMEOUT).unwrap(), UiAction::Flash(text) if text == DONE)
        );
        smol::block_on(command);
        assert!(Arc::ptr_eq(
            &original,
            &root_actor.effective_config().unwrap()
        ));
        if let Some(child) = child {
            assert!(
                manager
                    .actor(child)
                    .unwrap()
                    .effective_config()
                    .unwrap()
                    .mode
                    .plan_path()
                    .is_some()
            );
        }
    });
}

#[test_case("   "; "blank")]
#[test_case("@reject:task"; "expander_failure")]
#[test_case("oversized"; "oversized")]
fn rejected_new_prompt_never_publishes_a_runtime(prompt: &str) {
    with_event_loop(|event_loop| {
        let _host = install_expanders(event_loop);
        let original = event_loop.sessions[0].id();
        let prompt = if prompt == "oversized" {
            "x".repeat(OVERSIZED_PROMPT_BYTES)
        } else {
            prompt.into()
        };
        let expected = match prompt.as_str() {
            "   " => crate::app::EMPTY_PROMPT_ERR.to_owned(),
            "@reject:task" => EXPANDER_FAILURE.to_owned(),
            _ => maki_agent::actor::ActorError::InstructionOverflow.to_string(),
        };
        let (reply, response) = flume::bounded(1);
        event_loop.handle_session_request(
            SessionRequest::New {
                prompt: Some(prompt),
                focus: true,
            },
            reply,
        );
        let error = response
            .recv_timeout(SESSION_OP_TIMEOUT)
            .unwrap()
            .unwrap_err();
        assert_eq!(error, expected);
        assert_eq!(event_loop.sessions.len(), 1);
        assert_eq!(event_loop.sessions[event_loop.focused].id(), original);
    });
}

#[test]
fn session_prompt_expands_once_and_busy_submission_is_queued() {
    with_event_loop(|event_loop| {
        let _host = install_expanders(event_loop);
        let (manager, root) = event_loop.sessions[0].handles.manager_and_root();
        let actor = manager.actor(root).unwrap();
        let _guard = actor.prepare_idle().unwrap();
        assert_eq!(
            event_loop.submit_text(0, "@ok:task".into()).unwrap(),
            serde_json::json!("started")
        );
        assert_eq!(actor.pending_submissions()[0].text, EXPANDED_PROMPT);
        assert_eq!(
            event_loop.submit_text(0, PROMPT.into()).unwrap(),
            serde_json::json!("queued")
        );
        assert_eq!(actor.pending_submissions().len(), 2);
    });
}

#[test_case(false; "expander_failure")]
#[test_case(true; "admission_failure")]
fn rejected_session_prompt_preserves_shell_context(oversized: bool) {
    with_event_loop(|event_loop| {
        let _host = install_expanders(event_loop);
        let id = event_loop.sessions[0].app.shell.reserve_for(None);
        event_loop.sessions[0]
            .app
            .handle_shell_event(ShellEvent::Done {
                id,
                command: PROMPT.into(),
                output: SHELL_OUTPUT.into(),
                is_error: false,
                visible: true,
            });
        let prompt = if oversized {
            "x".repeat(OVERSIZED_PROMPT_BYTES)
        } else {
            "@reject:task".into()
        };
        let error = event_loop.submit_text(0, prompt).unwrap_err();
        if !oversized {
            assert_eq!(error, EXPANDER_FAILURE);
        }
        assert!(
            !event_loop.sessions[0]
                .app
                .shell
                .results_for(None)
                .is_empty()
        );
        assert!(event_loop.sessions[0].handles.queue.is_empty());
    });
}

const GLM_4: &str = "ollama/glm-4";
const GLM_5: &str = "ollama/glm-5";
const OTHER_PROVIDER: &str = "zai/glm-5";
const UNREACHABLE_OLLAMA_HOST: &str = "http://127.0.0.1:1";

#[test_case(&[GLM_5, OTHER_PROVIDER], "ollama", Ok(GLM_5) ; "provider_section_switches")]
#[test_case(&[GLM_5], "glm", Ok(GLM_5) ; "unique_fragment_switches")]
#[test_case(&[GLM_4, GLM_5], "glm", Err("command failed: ambiguous model: glm") ; "ambiguous_fragment_fails")]
#[test_case(&[GLM_5], "xyz", Err("command failed: no model matches: xyz") ; "unknown_fragment_fails")]
fn model_argument_resolves_against_the_catalog(
    catalog: &[&str],
    argument: &str,
    expected: Result<&str, &str>,
) {
    // SAFETY: nextest runs each test in its own process.
    unsafe { std::env::set_var("OLLAMA_HOST", UNREACHABLE_OLLAMA_HOST) };
    with_event_loop(|event_loop| {
        event_loop.ctx.catalog.specs().store(Some(Arc::new(
            catalog.iter().map(|spec| spec.to_string()).collect(),
        )));
        event_loop.sync_orchestration();
        let (manager, root) = event_loop.sessions[0].handles.manager_and_root();
        let actor = manager.actor(root).unwrap();
        let commits = actor.subscribe_config_commits();
        let outcome = dispatch_slash(event_loop, &format!("/model {argument}"));
        match expected {
            Ok(spec) => {
                assert!(matches!(outcome, CommandOutcome::Completed), "{outcome:?}");
                let commit = commits.recv_timeout(SESSION_OP_TIMEOUT).unwrap();
                assert_eq!(commit.config.model.spec(), spec);
            }
            Err(message) => {
                let CommandOutcome::Failed(error) = outcome else {
                    panic!("expected a failure, got {outcome:?}");
                };
                assert_eq!(error.to_string(), message);
            }
        }
    });
}

const THINKING_MODEL: &str = "anthropic/claude-opus-4-6";
const PLAN_HEADING: &str = "# Plan Mode";
const PLAN_COMMAND_PLUGIN: &str = "plan_mode_command";

/// Records the system prompt and options of every request, then ends the turn.
struct RecordingProvider(flume::Sender<(String, RequestOptions)>);

impl Provider for RecordingProvider {
    fn stream_message<'a>(
        &'a self,
        _: &'a Model,
        _: &'a [Message],
        system: &'a str,
        _: &'a serde_json::Value,
        _: &'a flume::Sender<ProviderEvent>,
        options: RequestOptions,
        _: Option<&'a SessionRef>,
    ) -> BoxFuture<'a, Result<StreamResponse, AgentError>> {
        let _ = self.0.send((system.to_owned(), options));
        Box::pin(async {
            Ok(StreamResponse {
                message: Message::user(String::new()),
                usage: TokenUsage::default(),
                stop_reason: Some(StopReason::EndTurn),
            })
        })
    }

    fn list_models(&self) -> BoxFuture<'_, Result<Vec<ModelInfo>, AgentError>> {
        Box::pin(async { Ok(Vec::new()) })
    }
}

fn recording_provider() -> (Arc<dyn Provider>, flume::Receiver<(String, RequestOptions)>) {
    let (requests, recorded) = flume::unbounded();
    (Arc::new(RecordingProvider(requests)), recorded)
}

/// The TUI's half of `tests/frontend_parity.rs`: a blank session sends
/// `always_thinking` like every headless frontend does.
#[test]
fn blank_session_sends_always_thinking_to_the_provider() {
    with_event_loop(|event_loop| {
        event_loop.ctx.session_defaults = SessionDefaults {
            thinking: Some(StoredThinking::Effort {
                level: StoredEffort::High,
            }),
            ..SessionDefaults::default()
        };
        let (provider, requests) = recording_provider();
        let model = Model::from_spec(THINKING_MODEL).unwrap();
        let runtime = event_loop
            .ctx
            .spawn_runtime_with_provider(
                AppSession::new(THINKING_MODEL, &event_loop.session_cwd),
                Some(PreparedProvider {
                    model: model.clone(),
                    provider,
                }),
            )
            .unwrap();
        let index = event_loop.push_runtime(runtime);
        event_loop.submit_text(index, PROMPT.into()).unwrap();
        let (_, options) = requests.recv_timeout(SESSION_OP_TIMEOUT).unwrap();
        assert_eq!(
            options,
            RequestOptions {
                thinking: ThinkingConfig::Effort(Effort::High),
                fast: false,
            }
            .clamped(&model)
        );
    });
}

/// The TUI's half of `tests/frontend_parity.rs`: a Lua command that sets
/// plan mode changes what the next turn sends.
#[test]
fn lua_mode_command_reaches_the_provider() {
    with_event_loop(|event_loop| {
        let runtime = Arc::clone(&event_loop.ctx.command_runtime);
        let app = &mut event_loop.sessions[0].app;
        app.command_runtime = Arc::clone(&runtime);
        app.command_target = runtime.bind_target();
        let host = install_expanders(event_loop);
        host.event_handle()
            .install_orchestration_services(event_loop.orchestration.clone());
        host.load_source(
            PLAN_COMMAND_PLUGIN,
            r#"
            maki.api.register_command({
                name = "/plan-now", tui_only = false,
                handler = function(_, ctx)
                    assert(assert(ctx:agent()):set_mode(ctx, "plan"))
                end,
            })
        "#,
        )
        .unwrap();
        event_loop.sync_orchestration();
        let outcome = dispatch_slash(event_loop, "/plan-now");
        assert!(matches!(outcome, CommandOutcome::Completed), "{outcome:?}");

        let (provider, requests) = recording_provider();
        let (manager, root) = event_loop.sessions[0].handles.manager_and_root();
        let actor = manager.actor(root).unwrap();
        let model = actor.effective_config().unwrap().model.clone();
        smol::block_on(
            actor
                .reserve_prepared_operation(None)
                .unwrap()
                .commit_when_ready(Some(ConfigChange::Patch(ConfigPatch {
                    model: Some(PreparedModel { model, provider }),
                    ..ConfigPatch::default()
                }))),
        )
        .unwrap();
        event_loop.submit_text(0, PROMPT.into()).unwrap();
        let (system, _) = requests.recv_timeout(SESSION_OP_TIMEOUT).unwrap();
        assert!(system.contains(PLAN_HEADING), "{system}");
    });
}
