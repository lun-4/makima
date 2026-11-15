use std::sync::Arc;

use makima_agent::actor::ActorStatus;
use makima_agent::{
    ActorWorkIdentity, ActorWorkKind, AgentEvent, AgentInput, EventSender, Post, PostLevel,
    TurnCompleteEvent, TurnOutcome,
};
use makima_commands::{CommandOutcome, InputDispatch};
use makima_providers::{Message, TokenUsage};

use super::{
    FIRST, MODEL, SECOND, THIRD, WAIT, assert_history, boundary_for, checkpoint, finish_restore,
    pump_until, setup, target, turn, with_event_loop,
};
use crate::AppSession;
use crate::chat::Chat;
use crate::components::{Action, DisplayRole};
use crate::event_loop::InternalEvent;

const PLUGIN: &str = "rewind-outside-snapshot";
const COMMAND: &str = "/spawn-outside-snapshot-child";
const CHILD: &str = "outside snapshot child";
const POST: &str = "outside root post pending at restore snapshot";
const DRAFT: &str = "outside root draft pending at restore snapshot";
const ROOT_SCROLL: u16 = 4;
const CONTEXT_SIZE: u32 = 8192;
const CONTEXT_WINDOW: u32 = 200_000;
const USAGE: TokenUsage = TokenUsage {
    input: 137,
    output: 29,
    cache_read: 53,
    cache_creation: 11,
};

#[test]
fn child_restore_captures_outside_presentation_after_all_actors_quiesce() {
    with_event_loop(|event_loop| {
        let (index, host) = setup(event_loop);
        turn(event_loop, index, FIRST);
        host.load_source(
            PLUGIN,
            &format!(
                r#"
            makima.api.register_command({{
                name = "{COMMAND}", tui_only = false,
                handler = function(_, ctx)
                    assert(makima.agent.spawn(ctx, {{ name = "{CHILD}", system = "child",
                        tools = makima.json.decode('[]'), inherit_provider = true,
                        auto_deliver = false, silent = true }}))
                end,
            }})
        "#
            ),
        )
        .unwrap();
        let root = target(event_loop, index);
        let manager = root.manager();
        let command_target = event_loop.sessions[index].app.command_target.clone();
        let dispatched = smol::block_on(
            event_loop
                .ctx
                .command_runtime
                .registry
                .dispatch_input(&command_target, COMMAND.into()),
        );
        assert!(
            matches!(
                dispatched,
                InputDispatch::Dispatched(CommandOutcome::Completed)
            ),
            "spawn command failed: {dispatched:?}"
        );
        let child = manager
            .snapshot()
            .into_iter()
            .find(|node| node.parent_id == Some(root.id()))
            .unwrap()
            .agent_id;
        let child = manager.lookup(child).unwrap();
        let actor = child.actor().unwrap();
        let root_actor = root.actor().unwrap();
        pump_until(event_loop, |_| {
            root_actor.prepare_idle().is_ok() && actor.prepare_idle().is_ok()
        });
        for text in [SECOND, THIRD] {
            let ticket = actor
                .submit(
                    AgentInput {
                        message: text.into(),
                        images: Vec::new(),
                        preamble: Vec::new(),
                        prompt: None,
                    },
                    None,
                    text.into(),
                    Default::default(),
                )
                .unwrap();
            pump_until(event_loop, |_| {
                ticket.peek().is_some() && actor.snapshot().status == ActorStatus::Idle
            });
            assert!(matches!(ticket.peek(), Some(TurnOutcome::Completed { .. })));
        }
        let snapshot = manager.journal().snapshot();
        let key = snapshot
            .agent_revisions
            .iter()
            .rev()
            .find(|(_, record)| record.id == child.id())
            .unwrap()
            .1
            .transcript_key
            .clone();
        {
            let app = &mut event_loop.sessions[index].app;
            let mut chat = Chat::new(
                CHILD.into(),
                app.ui_config.clone(),
                app.lua_event_handle.clone(),
                Arc::clone(&app.theme_provider),
            );
            chat.agent_id = Some(child.id());
            chat.parent_agent_id = Some(root.id());
            chat.subagent_id = Some(key.clone());
            chat.push_user_message(SECOND);
            chat.push_user_message(THIRD);
            let child_index = app.chats.len();
            app.agent_chat_index.insert(child.id(), child_index);
            app.chat_index.insert(key.clone(), child_index);
            app.chats.push(chat);
            app.active_chat = 0;
        }
        event_loop.sync_orchestration();
        let original = checkpoint(event_loop, index);
        let root_history = root_actor.committed_history();
        let child_history = actor.committed_history();
        let (history, durable) = boundary_for(&child, THIRD);
        let expected_child = child_history[..history.offset].to_vec();
        let outside_gate = root_actor.prepare_idle().unwrap();
        event_loop.handle_action(
            index,
            Action::RewindAgent {
                target: child.clone(),
                boundary: history,
                durable: Some(durable),
            },
        );
        assert!(event_loop.sessions[index].app.restore_in_progress);
        drop(outside_gate);
        let snapshot_request = loop {
            let event = event_loop.internal_rx.recv_timeout(WAIT).unwrap();
            if matches!(event, InternalEvent::RestoreSnapshot { .. }) {
                break event;
            }
            assert!(!matches!(event, InternalEvent::RestoreReady { .. }));
            event_loop.handle_internal(event);
        };
        assert!(root_actor.prepare_idle().is_err());
        assert!(actor.prepare_idle().is_err());
        let runtime = &mut event_loop.sessions[index];
        let mut expected_usage = runtime.app.state.token_usage;
        expected_usage += USAGE;
        let presentation = runtime.app.run_id;
        let sender = EventSender::with_identity(
            runtime.handles.agent_tx.clone(),
            Some(ActorWorkIdentity::new(
                root.id(),
                ActorWorkKind::Control {
                    operation_id: presentation,
                },
                presentation,
            )),
        );
        sender
            .send(AgentEvent::TurnComplete(Box::new(TurnCompleteEvent {
                message: Message::default(),
                usage: USAGE,
                model: MODEL.into(),
                cost: None,
                context_size: Some(CONTEXT_SIZE),
                context_window: CONTEXT_WINDOW,
            })))
            .unwrap();
        sender
            .send(AgentEvent::Post(Post {
                text: POST.into(),
                level: PostLevel::Info,
                agent: None,
            }))
            .unwrap();
        runtime.app.chats[0].restore_scroll(ROOT_SCROLL, false);
        runtime.app.input_box.set_input(DRAFT.into());
        assert_eq!(runtime.app.state.session.token_usage, original.token_usage);
        assert_ne!(runtime.app.state.session.meta.context_size, CONTEXT_SIZE);
        assert!(!runtime.handles.agent_rx.is_empty());
        event_loop.handle_internal(snapshot_request);
        let app = &event_loop.sessions[index].app;
        assert_eq!(app.state.token_usage, expected_usage);
        assert_eq!(app.state.context_size, CONTEXT_SIZE);
        assert_eq!(app.chats[0].last_message_text(), POST);
        let outside_messages: Vec<_> = (0..app.chats[0].message_count())
            .map(|index| {
                let message = app.chats[0].message_at(index).unwrap();
                (message.role.clone(), message.text.clone())
            })
            .collect();
        finish_restore(event_loop, index);
        assert_history(event_loop, index, &root_history);
        let app = &event_loop.sessions[index].app;
        assert_eq!(app.state.token_usage, expected_usage);
        assert_eq!(app.state.context_size, CONTEXT_SIZE);
        assert_eq!(app.chats[0].context_size, CONTEXT_SIZE);
        assert_eq!(app.chats[0].scroll_top(), ROOT_SCROLL);
        assert!(!app.chats[0].auto_scroll());
        assert_eq!(app.chats[0].last_message_text(), POST);
        assert_eq!(
            app.chats[0].last_message_role(),
            Some(&DisplayRole::Post(PostLevel::Info))
        );
        assert_eq!(app.input_box.buffer.value(), DRAFT);
        assert_eq!(app.selected_agent_id(), Some(root.id()));
        assert_eq!(
            (0..app.chats[0].message_count())
                .map(|index| {
                    let message = app.chats[0].message_at(index).unwrap();
                    (message.role.clone(), message.text.clone())
                })
                .collect::<Vec<_>>(),
            outside_messages
        );
        let disk = AppSession::load(original.id, &event_loop.ctx.storage).unwrap();
        let branch_id = disk.branches().last().unwrap().id;
        let branch = disk
            .load_branch_from(branch_id, &event_loop.ctx.sessions_dir)
            .unwrap();
        for session in [app.state.session.as_ref(), &disk, &branch] {
            assert_eq!(session.token_usage, expected_usage);
            assert_eq!(session.meta.context_size, CONTEXT_SIZE);
            assert_eq!(session.meta.input_draft.as_deref(), Some(DRAFT));
            let posts: Vec<_> = session
                .posts()
                .iter()
                .filter(|stored| stored.chat.is_none() && stored.post.text == POST)
                .collect();
            assert_eq!(posts.len(), 1);
            assert_eq!(posts[0].after_message, root_history.len());
            assert_eq!(posts[0].post.level, PostLevel::Info);
            assert_eq!(
                serde_json::to_value(session.messages()).unwrap(),
                serde_json::to_value(root_history.as_ref()).unwrap()
            );
        }
        for session in [app.state.session.as_ref(), &disk] {
            assert_eq!(
                serde_json::to_value(&session.subagent_messages()[&key]).unwrap(),
                serde_json::to_value(&expected_child).unwrap()
            );
        }
        assert_eq!(
            serde_json::to_value(&branch.subagent_messages()[&key]).unwrap(),
            serde_json::to_value(child_history.as_ref()).unwrap()
        );
        assert_eq!(
            serde_json::to_value(actor.committed_history().as_ref()).unwrap(),
            serde_json::to_value(&expected_child).unwrap()
        );
    });
}
