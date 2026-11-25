use makima_agent::test_util::poll_until;
mod common;

use common::{RestartHost, TestSession};
use makima_agent::{
    AgentEvent, AgentId, AgentInput, TurnAdmissionOptions, TurnId, TurnOutcome, actor::ActorStatus,
};
use makima_lua::test_support::{InMemoryFs, run_command};
use makima_providers::{
    AgentError, ContentBlock, Invocation, Message, Model, ProviderEvent, RequestDelivery,
    RequestOptions, Role, StreamResponse,
    provider::{BoxFuture, Provider},
};
use makima_storage::{
    StateDir,
    id::SessionRef,
    session_state::{StateChange, StoredDoneReason, StoredTurnStatus},
};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use test_case::test_case;

const RESTORED_REQUEST_TIMEOUT: Duration = Duration::from_secs(5);
const PLUGIN: &str = "restart_crash";
const COMMAND: &str = "/restart_crash";
const IDS: &str = "crash_ids.json";
const RUNNING: &str = "crash running child";
const QUEUED: &str = "crash queued child";
const PARENT: &str = "crash parent";
const DESCENDANT: &str = "crash descendant";
const REPLY: &str = "completed before crash";
const SPAWN_TOOL: &str = "crash_spawn";
const PARTIAL: &str = "uncommitted streaming text before crash";
const TASK_PLUGIN: &str = "task";
const TASK_SPAWN: &str = "task_spawn";
const TASK_WAIT: &str = "task_wait";
const WAIT_ENTERED: &str = "wait_entered.json";
const NON_RESUMABLE: &str = "restart_non_resumable";
const NON_RESUMABLE_ENTERED: &str = "non_resumable_entered";
const COMPLETED_NON_RESUMABLE: &str = "restart_completed_non_resumable";
const COMPLETED_NON_RESUMABLE_ENTERED: &str = "completed_non_resumable_entered";
const COMPLETED_OUTPUT: &str = "  completed sibling\nsecond line: \"verbatim\"  \n";
const RESTART_RESULT: &str = "This tool call was interrupted when makima stopped. It may have partly run. Check the current state before retrying, then continue your task.";
const CANCEL_MARKER: &str = "[Cancelled by user]";
const TASK_PARENT: &str = "parent waiting for disk-backed task";
const TASK_CHILD: &str = "child awaited across crash";
const TASK_SOURCE: &str = include_str!("../../plugins/task/init.lua");
const SOURCE: &str = r#"
local ids_path = __IDS_PATH__
local function read()
    return assert(makima.json.decode(assert(makima.fs.read(ids_path))))
end
local function write(ids)
    assert(makima.fs.write(ids_path, makima.json.encode(ids)))
end
local function remember(ids, agent, ticket, interrupted)
    ids.turns[#ids.turns + 1] = {
        agent = agent:id(), turn = ticket:id(), interrupted = interrupted,
    }
end
makima.api.register_tool({
    name = "crash_spawn", description = "spawn a child whose result starts a continuation",
    schema = { type = "object", properties = {} },
    handler = function(_, ctx)
        local ids = read()
        ids.tool_calls = ids.tool_calls + 1
        local child = assert(makima.agent.spawn(ctx, { inherit_provider = true, tools = {}, mcp = false }))
        ids.descendant = child:id()
        write(ids)
        return "child ready"
    end,
})
makima.api.register_command({
    name = "/restart_crash", tui_only = false,
    arguments = { raw = true, required = true },
    handler = function(opts, ctx)
        local action = opts.args
        local root = assert(makima.agent.get(ctx, assert(makima.agent.root(ctx))))
        if action == "running" or action == "continuation" then
            local ids = { turns = {}, tool_calls = 0 }
            local tools = action == "continuation" and { {
                name = "crash_spawn", description = "spawn a child whose result starts a continuation",
                input_schema = { type = "object", properties = {} },
            } } or {}
            local child = assert(makima.agent.spawn(ctx, {
                inherit_provider = true, tools = tools, mcp = false, auto_deliver = false,
            }))
            ids.parent = child:id()
            local ticket = assert(child:send(ctx, action == "running" and "crash running child" or "crash parent", { when = "idle" }))
            remember(ids, child, ticket, action == "running")
            if action == "running" then
                remember(ids, child, assert(child:send(ctx, "crash queued child", { when = "queue" })), true)
            end
            write(ids)
        elseif action == "task_parent" or action == "joined_parent" then
            local ids = { turns = {}, tool_calls = 0 }
            local child = assert(makima.agent.spawn(ctx, {
                inherit_provider = true, mcp = false, auto_deliver = false,
                audience = "general_sub",
            }))
            ids.parent = child:id()
            if action == "task_parent" then
                remember(ids, child, assert(child:send(ctx, "parent waiting for disk-backed task", { when = "idle" })), true)
            end
            write(ids)
        elseif action == "wait_parent" then
            local ids = read()
            local parent = assert(makima.agent.get(ctx, ids.parent))
            assert(assert(assert(parent:turn(ctx, ids.turns[1].turn)):wait(ctx)).status == "completed")
        elseif action == "start_descendant" then
            local ids = read()
            local child = assert(makima.agent.get(ctx, ids.descendant))
            remember(ids, child, assert(child:send(ctx, "crash descendant", { when = "idle" })), false)
            write(ids)
        elseif action == "capture_continuation" then
            local ids = read()
            local parent = assert(makima.agent.get(ctx, ids.parent))
            local ticket = assert(parent:latest_turn(ctx))
            assert(ticket:id() ~= ids.turns[1].turn)
            assert(ticket:result(ctx) == nil)
            remember(ids, parent, ticket, true)
            write(ids)
        elseif action == "held_probe" then
            local ids = read()
            local parent = assert(makima.agent.get(ctx, ids.parent))
            ids.probe = assert(parent:send(ctx, "plugin input must not release restored report", { when = "idle" })):id()
            write(ids)
        elseif action == "publish" then
            assert(root:set_option(ctx, "yolo", "enabled"))
        elseif action == "wait_all" or action == "inspect" then
            for _, entry in ipairs(read().turns) do
                local agent = assert(makima.agent.get(ctx, entry.agent))
                local ticket = assert(agent:turn(ctx, entry.turn))
                assert(ticket:id() == entry.turn and ticket:agent_id() == entry.agent)
                local result = assert(ticket:wait(ctx, action == "inspect" and { timeout = 0 } or nil))
                assert(result.status == "completed" and result.text == "completed before crash", makima.json.encode(result))
                assert(assert(ticket:result(ctx)).text == result.text)
            end
        end
    end,
})
"#;

struct Request {
    messages: Vec<Message>,
    reply: flume::Sender<StreamResponse>,
}

struct GatedProvider {
    requests: flume::Sender<Request>,
    calls: AtomicUsize,
}

impl Provider for GatedProvider {
    fn stream_message<'a>(
        &'a self,
        _: &'a Model,
        messages: &'a [Message],
        _: &'a str,
        _: &'a Value,
        events: &'a flume::Sender<ProviderEvent>,
        _: RequestOptions,
        _: Option<&'a SessionRef>,
        _: &'a Invocation,
        _: RequestDelivery,
    ) -> BoxFuture<'a, Result<StreamResponse, AgentError>> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let _ = events.send(ProviderEvent::TextDelta {
                text: PARTIAL.into(),
            });
            let (reply, response) = flume::bounded(1);
            self.requests
                .send(Request {
                    messages: messages.to_vec(),
                    reply,
                })
                .unwrap();
            Ok(response.recv_async().await.unwrap())
        })
    }
}

fn command(host: &RestartHost, action: &str) {
    let finished = run_command(
        &host.host.command_registry(),
        &format!("{COMMAND} {action}"),
        0,
        Some(host.root.clone()),
    );
    assert_eq!(
        smol::block_on(common::bounded_at(
            &format!("command {action}"),
            finished.recv_async()
        ))
        .unwrap(),
        Ok(())
    );
}

fn ids(host: &RestartHost) -> Value {
    let (_, bytes) = host
        .fs
        .files()
        .into_iter()
        .find(|(path, _)| path.file_name().and_then(|name| name.to_str()) == Some(IDS))
        .unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

fn request(requests: &flume::Receiver<Request>, stage: &str) -> Request {
    smol::block_on(common::bounded_at(stage, requests.recv_async())).unwrap()
}

fn restored_request(
    host: &RestartHost,
    requests: &flume::Receiver<Request>,
    stage: &str,
) -> Request {
    smol::block_on(futures_lite::future::race(
        async { requests.recv_async().await.unwrap() },
        async {
            let timeout = smol::Timer::after(RESTORED_REQUEST_TIMEOUT);
            futures_lite::pin!(timeout);
            loop {
                let snapshot = host.manager.journal().snapshot();
                for record in snapshot.state.iter() {
                    if let StateChange::Turn { result, .. } = &record.change {
                        assert!(
                            matches!(result.outcome.status, StoredTurnStatus::Completed { .. }),
                            "{stage}: restored turn terminalized before provider request: {result:?}; graph={:?}",
                            host.manager.snapshot(),
                        );
                    }
                }
                assert!(
                    futures_lite::future::poll_once(timeout.as_mut())
                        .await
                        .is_none(),
                    "{stage}: no provider request; graph={:?}; state={:?}; persistence_error={:?}",
                    host.manager.snapshot(),
                    snapshot.state,
                    host.manager.journal().persistence_error(),
                );
                futures_lite::future::yield_now().await;
            }
        },
    ))
}

fn child_transcripts(session: &TestSession) -> BTreeMap<&str, Arc<Vec<Message>>> {
    session
        .agents()
        .values()
        .filter(|agent| agent.parent.is_some())
        .filter_map(|agent| {
            let key = agent.transcript_key.as_str();
            session
                .subagent_history_snapshot(key)
                .map(|history| (key, history.messages))
        })
        .collect()
}

fn contains(request: &Request, text: &str) -> bool {
    serde_json::to_string(&request.messages)
        .unwrap()
        .contains(text)
}

#[test_case(false; "running_and_queued_child")]
#[test_case(true; "child_result_continuation")]
fn published_crash_prefix_continues_exact_lua_tickets(continuation: bool) {
    let tmp = tempfile::tempdir().unwrap();
    let state = StateDir::from_path(tmp.path().into());
    let mut session = TestSession::new(
        &common::default_model().spec(),
        tmp.path().to_str().unwrap(),
    );
    session.save(&state).unwrap();
    let session_id = session.id;
    let fs = Arc::new(InMemoryFs::new());
    fs.seed(
        &tmp.path().join("plugins").join(format!("{PLUGIN}.lua")),
        SOURCE
            .replace("__IDS_PATH__", &json!(tmp.path().join(IDS)).to_string())
            .into_bytes(),
    );
    let (send, requests) = flume::unbounded();
    let provider = Arc::new(GatedProvider {
        requests: send,
        calls: AtomicUsize::new(0),
    });
    let host = RestartHost::open_with_provider(
        tmp.path(),
        session_id,
        fs,
        vec![PLUGIN.into()],
        provider.clone(),
    );
    command(
        &host,
        if continuation {
            "continuation"
        } else {
            "running"
        },
    );
    let first = request(&requests, "initial crash child request");
    let blocked = if continuation {
        assert!(contains(&first, PARENT));
        first
            .reply
            .send(common::canned_tool_use(SPAWN_TOOL, json!({})))
            .unwrap();
        let parent = request(&requests, "parent after spawning descendant");
        assert!(contains(&parent, PARENT));
        parent.reply.send(common::canned_reply(REPLY)).unwrap();
        command(&host, "wait_parent");
        command(&host, "start_descendant");
        let child = request(&requests, "descendant before crash");
        assert!(contains(&child, DESCENDANT));
        child.reply.send(common::canned_reply(REPLY)).unwrap();
        let continuation = request(&requests, "parent child-result continuation before crash");
        assert!(contains(&continuation, REPLY));
        assert!(continuation.messages.iter().any(|message| matches!(
            message.observation_source.as_ref(),
            Some(makima_providers::ObservationSource::ChildResult { .. })
        )));
        command(&host, "capture_continuation");
        continuation
    } else {
        assert!(contains(&first, RUNNING));
        assert!(!contains(&first, QUEUED));
        let saved = ids(&host);
        let child: AgentId = saved["parent"].as_str().unwrap().parse().unwrap();
        let actor = host.manager.actor(child).unwrap().snapshot();
        assert!(matches!(actor.status, ActorStatus::Running(_)));
        assert_eq!(actor.queued, 1);
        first
    };
    command(&host, "publish");
    let saved = ids(&host);
    assert_eq!(saved["tool_calls"], usize::from(continuation));
    assert_eq!(
        saved["turns"].as_array().unwrap().len(),
        if continuation { 3 } else { 2 }
    );
    let prefix = TestSession::load(session_id, &state).unwrap();
    for entry in saved["turns"].as_array().unwrap() {
        let agent: AgentId = entry["agent"].as_str().unwrap().parse().unwrap();
        let turn: TurnId = entry["turn"].as_str().unwrap().parse().unwrap();
        assert!(prefix.state().iter().any(|record| record.agent == Some(agent) && matches!(&record.change, StateChange::Admitted { turn: admitted, .. } if *admitted == turn)));
        assert_eq!(prefix.state().iter().any(|record| matches!(&record.change, StateChange::Turn { result, .. } if result.outcome.agent_id == agent && result.outcome.turn_id == turn)), !entry["interrupted"].as_bool().unwrap());
    }
    let calls = provider.calls.load(Ordering::SeqCst);
    let blocked_messages = serde_json::to_value(&blocked.messages).unwrap();
    let mut host = host.crash_restart_host(tmp.path());
    drop(blocked);
    let resumed = restored_request(&host, &requests, "automatic child wake after crash");
    assert_eq!(
        serde_json::to_value(&resumed.messages).unwrap(),
        blocked_messages
    );
    assert!(!contains(&resumed, PARTIAL));
    resumed.reply.send(common::canned_reply(REPLY)).unwrap();
    if !continuation {
        let queued = request(&requests, "queued child turn after crash");
        assert!(contains(&queued, QUEUED));
        assert!(!contains(&queued, PARTIAL));
        queued.reply.send(common::canned_reply(REPLY)).unwrap();
    }
    command(&host, "wait_all");
    let resumed_calls = calls + if continuation { 1 } else { 2 };
    let mut recovered_state: Option<Value> = None;
    for restart in 0..2 {
        command(&host, "inspect");
        assert_eq!(provider.calls.load(Ordering::SeqCst), resumed_calls);
        assert!(requests.is_empty());
        assert_eq!(ids(&host), saved);
        let recovered = TestSession::load(session_id, &state).unwrap();
        let children = child_transcripts(&recovered);
        assert!(!serde_json::to_string(&children).unwrap().contains(PARTIAL));
        for entry in saved["turns"].as_array().unwrap() {
            let turn: TurnId = entry["turn"].as_str().unwrap().parse().unwrap();
            let results: Vec<_> = recovered
                .state()
                .iter()
                .filter_map(|record| match &record.change {
                    StateChange::Turn { result, .. } if result.outcome.turn_id == turn => {
                        Some(result)
                    }
                    _ => None,
                })
                .collect();
            assert_eq!(results.len(), 1);
            assert_eq!(
                results[0].outcome.status,
                StoredTurnStatus::Completed {
                    reason: StoredDoneReason::EndTurn
                }
            );
        }
        let state = common::normalized_history_epoch_state(recovered.state());
        let projection = json!({
            "state": state,
            "agents": recovered.agent_revisions(),
            "messages": recovered.messages(),
            "children": children,
            "conversation": recovered.meta.conversation,
        });
        if let Some(previous) = &recovered_state {
            assert_eq!(&projection, previous);
        }
        recovered_state = Some(projection);
        if restart == 0 {
            host = host.crash_restart_host(tmp.path());
        }
    }
}

#[test_case(false; "crash")]
#[test_case(true; "graceful_suspend")]
fn published_task_wait_resumes_parent_and_child_without_respawning(graceful: bool) {
    let tmp = tempfile::tempdir().unwrap();
    let state = StateDir::from_path(tmp.path().into());
    let mut session = TestSession::new(
        &common::default_model().spec(),
        tmp.path().to_str().unwrap(),
    );
    session.save(&state).unwrap();
    let session_id = session.id;
    let fs = Arc::new(InMemoryFs::new());
    fs.seed(
        &tmp.path().join("plugins").join(format!("{PLUGIN}.lua")),
        SOURCE
            .replace("__IDS_PATH__", &json!(tmp.path().join(IDS)).to_string())
            .into_bytes(),
    );
    let marker = tmp.path().join(WAIT_ENTERED);
    let non_resumable_marker = tmp.path().join(NON_RESUMABLE_ENTERED);
    let completed_marker = tmp.path().join(COMPLETED_NON_RESUMABLE_ENTERED);
    let task_source = format!(
        r#"
local register = makima.api.register_tool
makima.api.register_tool = function(spec)
    if spec.name == "task_wait" then
        local handler = spec.handler
        spec.handler = function(input, ctx)
            local previous = makima.fs.read({marker}) or ""
            assert(makima.fs.write({marker}, previous .. "+"))
            return handler(input, ctx)
        end
    end
    return register(spec)
end
{TASK_SOURCE}
makima.api.register_tool = register
register({{
    name = "{NON_RESUMABLE}", description = "Park a non-resumable call",
    audiences = {{ "main", "general_sub" }},
    schema = {{ type = "object", properties = {{}} }},
    handler = function()
        local gate = makima.async.semaphore(1)
        local held = gate:acquire()
        local previous = makima.fs.read({non_resumable_marker}) or ""
        assert(makima.fs.write({non_resumable_marker}, previous .. "+"))
        gate:acquire()
        held:release()
        error("the non-resumable call did not park")
    end,
}})
register({{
    name = "{COMPLETED_NON_RESUMABLE}", description = "Complete a non-resumable call",
    audiences = {{ "main", "general_sub" }},
    schema = {{ type = "object", properties = {{}} }},
    handler = function()
        local previous = makima.fs.read({completed_marker}) or ""
        assert(makima.fs.write({completed_marker}, previous .. "+"))
        return {completed_output}
    end,
}})
"#,
        marker = json!(marker),
        non_resumable_marker = json!(non_resumable_marker),
        completed_marker = json!(completed_marker),
        completed_output = json!(COMPLETED_OUTPUT),
    );
    fs.seed(
        &tmp.path()
            .join("plugins")
            .join(format!("{TASK_PLUGIN}.lua")),
        task_source.into_bytes(),
    );
    let (send, requests) = flume::unbounded();
    let provider = Arc::new(GatedProvider {
        requests: send,
        calls: AtomicUsize::new(0),
    });
    let host = RestartHost::open_with_provider(
        tmp.path(),
        session_id,
        fs,
        vec![TASK_PLUGIN.into(), PLUGIN.into()],
        provider.clone(),
    );
    command(&host, "task_parent");
    let parent = request(&requests, "task parent initial request");
    assert!(contains(&parent, TASK_PARENT));
    parent
        .reply
        .send(common::canned_tool_use(
            TASK_SPAWN,
            json!({
                "description": "disk restart child",
                "prompt": TASK_CHILD,
                "subagent_type": "general",
            }),
        ))
        .unwrap();
    let first = request(&requests, "task spawn first request");
    let second = request(&requests, "task spawn second request");
    let (child, parent) = if contains(&first, TASK_PARENT) {
        (second, first)
    } else {
        (first, second)
    };
    assert!(contains(&child, TASK_CHILD));
    let task: Value = parent
        .messages
        .iter()
        .flat_map(|message| &message.content)
        .find_map(|block| match block {
            ContentBlock::ToolResult {
                content,
                is_error: false,
                ..
            } => serde_json::from_str::<Value>(content)
                .ok()
                .filter(|value| value["task_id"].is_string()),
            _ => None,
        })
        .unwrap();
    let mut wait = common::canned_tool_use(TASK_WAIT, task.clone());
    for block in &mut wait.message.content {
        if let ContentBlock::ToolUse { id, .. } = block {
            *id = TASK_WAIT.into();
        }
    }
    if graceful {
        wait.message.content.push(ContentBlock::tool_use(
            COMPLETED_NON_RESUMABLE,
            COMPLETED_NON_RESUMABLE,
            json!({}),
        ));
        wait.message.content.push(ContentBlock::tool_use(
            NON_RESUMABLE,
            NON_RESUMABLE,
            json!({}),
        ));
    }
    parent.reply.send(wait).unwrap();
    let mut markers = vec![&marker];
    if graceful {
        markers.push(&non_resumable_marker);
    }
    for marker in markers {
        smol::block_on(poll_until(RESTORED_REQUEST_TIMEOUT, || {
            host.fs
                .files()
                .iter()
                .any(|(path, bytes)| path == marker && bytes == b"+")
                .then_some(())
        }))
        .unwrap_or_else(|| panic!("tool handler did not write {}", marker.display()));
    }
    if graceful {
        let done = smol::block_on(common::bounded_at("completed sibling tool event", async {
            loop {
                let envelope = host.events.recv_async().await.unwrap();
                if let AgentEvent::ToolDone(done) = envelope.event
                    && done.id == COMPLETED_NON_RESUMABLE
                {
                    break done;
                }
            }
        }));
        assert!(!done.is_error);
        assert_eq!(done.output.as_text(), COMPLETED_OUTPUT);
    }
    command(&host, "publish");
    let saved_ids = ids(&host);
    let prefix = TestSession::load(session_id, &state).unwrap();
    assert!(
        serde_json::to_string(&child_transcripts(&prefix))
            .unwrap()
            .contains(TASK_WAIT)
    );
    let batch_ids = [TASK_WAIT, COMPLETED_NON_RESUMABLE, NON_RESUMABLE];
    let host = if graceful {
        const SUSPEND_TIMEOUT: Duration = Duration::from_secs(15);
        let report = smol::block_on(futures_lite::future::race(
            host.manager.suspend(SUSPEND_TIMEOUT),
            async {
                smol::Timer::after(SUSPEND_TIMEOUT + RESTORED_REQUEST_TIMEOUT).await;
                panic!("graceful manager suspend did not return");
            },
        ));
        assert!(report.timed_out.is_empty(), "{report:?}");
        smol::block_on(common::bounded_at(
            "flush suspended journal",
            host.manager.journal().flush(),
        ))
        .unwrap();
        let suspended = TestSession::load(session_id, &state).unwrap();
        let children = child_transcripts(&suspended);
        let messages = serde_json::to_string(&children).unwrap();
        assert!(!messages.contains(CANCEL_MARKER));
        let saved_results: Vec<_> = children
            .values()
            .flat_map(|messages| messages.iter())
            .flat_map(|message| &message.content)
            .filter_map(|block| match block {
                ContentBlock::ToolResult {
                    tool_use_id,
                    content,
                    is_error,
                } if batch_ids.contains(&tool_use_id.as_str()) => {
                    Some((tool_use_id.as_str(), content.as_str(), *is_error))
                }
                _ => None,
            })
            .collect();
        assert_eq!(
            saved_results,
            [(COMPLETED_NON_RESUMABLE, COMPLETED_OUTPUT, false)]
        );
        host.restart_host(tmp.path())
    } else {
        host.crash_restart_host(tmp.path())
    };
    drop(child);
    let resumed_child = restored_request(&host, &requests, "task child automatic wake on reopen");
    assert!(contains(&resumed_child, TASK_CHILD));
    assert!(!contains(&resumed_child, PARTIAL));
    assert!(!contains(&resumed_child, CANCEL_MARKER));
    resumed_child
        .reply
        .send(common::canned_reply(REPLY))
        .unwrap();
    let resumed_parent = restored_request(&host, &requests, "task parent after resumed task_wait");
    assert!(contains(&resumed_parent, TASK_PARENT));
    assert!(contains(&resumed_parent, REPLY));
    assert!(!contains(&resumed_parent, PARTIAL));
    assert!(!contains(&resumed_parent, CANCEL_MARKER));
    let files = host.fs.files();
    assert!(
        files
            .iter()
            .any(|(path, bytes)| path == &marker && bytes == b"++")
    );
    if graceful {
        assert!(
            files
                .iter()
                .any(|(path, bytes)| path == &non_resumable_marker && bytes == b"+")
        );
        assert!(
            files
                .iter()
                .any(|(path, bytes)| path == &completed_marker && bytes == b"+")
        );
    }
    let batch_messages: Vec<_> = resumed_parent
        .messages
        .iter()
        .filter(|message| {
            message.content.iter().any(|block| {
                matches!(block, ContentBlock::ToolResult { tool_use_id, .. }
                    if batch_ids.contains(&tool_use_id.as_str()))
            })
        })
        .collect();
    let [batch] = batch_messages.as_slice() else {
        panic!("expected one tool-result message for the batch, got {batch_messages:?}");
    };
    assert_eq!(batch.role, Role::User);
    let results: Vec<_> = batch
        .content
        .iter()
        .filter_map(|block| match block {
            ContentBlock::ToolResult {
                tool_use_id,
                content,
                is_error,
            } => Some((tool_use_id.as_str(), content.as_str(), *is_error)),
            _ => None,
        })
        .collect();
    let (&(wait_id, wait_output, wait_error), siblings) = results.split_first().unwrap();
    assert_eq!((wait_id, wait_error), (TASK_WAIT, false));
    if graceful {
        assert_eq!(
            siblings,
            &[
                (COMPLETED_NON_RESUMABLE, COMPLETED_OUTPUT, false),
                (NON_RESUMABLE, RESTART_RESULT, true),
            ]
        );
    } else {
        assert!(siblings.is_empty());
    }
    let result: Value = serde_json::from_str(wait_output).unwrap();
    assert_eq!(result["status"], "done");
    assert_eq!(result["result"]["turn_id"], task["turn_id"]);
    assert_eq!(result["result"]["text"], REPLY);
    resumed_parent
        .reply
        .send(common::canned_reply(REPLY))
        .unwrap();
    command(&host, "wait_parent");
    command(&host, "publish");
    assert_eq!(ids(&host), saved_ids);
    let recovered = TestSession::load(session_id, &state).unwrap();
    assert!(
        !serde_json::to_string(&child_transcripts(&recovered))
            .unwrap()
            .contains(PARTIAL)
    );
    assert_eq!(
        recovered.latest_state().unwrap().agents.len(),
        prefix.latest_state().unwrap().agents.len()
    );
    if graceful {
        let parent: AgentId = saved_ids["parent"].as_str().unwrap().parse().unwrap();
        let transcript_key = &recovered.latest_state().unwrap().agents[&parent].transcript_key;
        let transcript = recovered.subagent_history_snapshot(transcript_key).unwrap();
        let stored_batch: Vec<_> = transcript
            .messages
            .iter()
            .filter(|message| {
                message.content.iter().any(|block| {
                    matches!(block, ContentBlock::ToolResult { tool_use_id, .. }
                    if batch_ids.contains(&tool_use_id.as_str()))
                })
            })
            .collect();
        let [completed, resumed] = stored_batch.as_slice() else {
            panic!("expected separate saved and resumed tool-result messages");
        };
        assert_eq!(completed.content, [batch.content[1].clone()]);
        assert_eq!(resumed.content.len(), 2);
        assert!(resumed.content.contains(&batch.content[0]));
        assert!(resumed.content.contains(&batch.content[2]));
    }
    for turn in [
        saved_ids["turns"][0]["turn"].clone(),
        task["turn_id"].clone(),
    ] {
        let turn: TurnId = turn.as_str().unwrap().parse().unwrap();
        let outcomes: Vec<_> = recovered
            .state()
            .iter()
            .filter_map(|record| match &record.change {
                StateChange::Turn { result, .. } if result.outcome.turn_id == turn => {
                    Some(&result.outcome.status)
                }
                _ => None,
            })
            .collect();
        assert_eq!(
            outcomes,
            vec![&StoredTurnStatus::Completed {
                reason: StoredDoneReason::EndTurn
            }]
        );
    }
    let calls = provider.calls.load(Ordering::SeqCst);
    let host = host.crash_restart_host(tmp.path());
    command(&host, "inspect");
    assert_eq!(provider.calls.load(Ordering::SeqCst), calls);
    assert!(requests.is_empty());
}

#[test_case(false; "pending")]
#[test_case(true; "already_consumed")]
fn joined_input_survives_crash_exactly_once(consumed: bool) {
    const FIRST: &str = TASK_PARENT;
    const JOINED: &str = "joined request across restart";
    let tmp = tempfile::tempdir().unwrap();
    let state = StateDir::from_path(tmp.path().into());
    let mut session = TestSession::new(
        &common::default_model().spec(),
        tmp.path().to_str().unwrap(),
    );
    session.save(&state).unwrap();
    let (send, requests) = flume::unbounded();
    let fs = Arc::new(InMemoryFs::new());
    fs.seed(
        &tmp.path().join("plugins").join(format!("{PLUGIN}.lua")),
        SOURCE
            .replace("__IDS_PATH__", &json!(tmp.path().join(IDS)).to_string())
            .into_bytes(),
    );
    let host = RestartHost::open_with_provider(
        tmp.path(),
        session.id,
        fs,
        vec![PLUGIN.into()],
        Arc::new(GatedProvider {
            requests: send,
            calls: AtomicUsize::new(0),
        }),
    );
    command(&host, "joined_parent");
    let saved = ids(&host);
    let child: AgentId = saved["parent"].as_str().unwrap().parse().unwrap();
    let actor = host.manager.actor(child).unwrap();
    let submit = |text: &str| {
        actor
            .submit(
                AgentInput::new(text.into(), Vec::new()),
                None,
                text.into(),
                TurnAdmissionOptions::default(),
            )
            .unwrap()
    };
    let turn = submit(FIRST).turn_id();
    let parked = request(&requests, "initial request");
    assert_eq!(submit(JOINED).turn_id(), turn);
    assert!(!contains(&parked, JOINED));
    let parked = if consumed {
        parked.reply.send(common::canned_reply(REPLY)).unwrap();
        let request = request(&requests, "request with drained input");
        assert!(contains(&request, JOINED));
        request
    } else {
        parked
    };
    let host = host.crash_restart_host(tmp.path());
    drop(parked);
    let request = restored_request(&host, &requests, "resumed request with joined input");
    assert!(contains(&request, FIRST));
    assert_eq!(
        request
            .messages
            .iter()
            .filter(|message| {
                message.content.iter().any(
                    |block| matches!(block, ContentBlock::Text { text } if text.contains(JOINED)),
                )
            })
            .count(),
        1
    );
    request.reply.send(common::canned_reply(REPLY)).unwrap();
    let ticket = host
        .manager
        .actor(child)
        .unwrap()
        .turn_ticket(turn)
        .unwrap();
    assert!(matches!(
        smol::block_on(common::bounded(ticket.wait())),
        TurnOutcome::Completed { .. }
    ));
}
