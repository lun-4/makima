mod common;

use common::{RestartHost, TestSession};
use makima_agent::{AgentId, TurnId, actor::ActorStatus};
use makima_lua::test_support::{InMemoryFs, run_command};
use makima_providers::{
    AgentError, Invocation, Message, Model, ProviderEvent, RequestDelivery, RequestOptions,
    StreamResponse,
    provider::{BoxFuture, Provider},
};
use makima_storage::{
    StateDir,
    id::SessionRef,
    session_state::{StateChange, StoredCancellationReason, StoredTurnStatus},
};
use serde_json::{Value, json};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use test_case::test_case;

const PLUGIN: &str = "restart_crash";
const COMMAND: &str = "/restart_crash";
const IDS: &str = "crash_ids.json";
const RUNNING: &str = "crash running child";
const QUEUED: &str = "crash queued child";
const PARENT: &str = "crash parent";
const DESCENDANT: &str = "crash descendant";
const REPLY: &str = "completed before crash";
const SPAWN_TOOL: &str = "crash_spawn";
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
        elseif action == "inspect" then
            for _, entry in ipairs(read().turns) do
                local agent = assert(makima.agent.get(ctx, entry.agent))
                assert(not agent:is_live())
                local status = assert(agent:status(ctx))
                assert(not status.live and status.active_turn == nil and status.queued == 0)
                local ticket = assert(agent:turn(ctx, entry.turn))
                assert(ticket:id() == entry.turn and ticket:agent_id() == entry.agent)
                for _, result in ipairs({ assert(ticket:result(ctx)), assert(ticket:wait(ctx, { timeout = 0 })) }) do
                    if entry.interrupted then
                        assert(result.status == "cancelled" and result.cancellation_reason == "interrupted", makima.json.encode(result))
                    else
                        assert(result.status == "completed" and result.text == "completed before crash", makima.json.encode(result))
                    end
                end
                assert(not agent:is_live())
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
        _: &'a flume::Sender<ProviderEvent>,
        _: RequestOptions,
        _: Option<&'a SessionRef>,
        _: &'a Invocation,
        _: RequestDelivery,
    ) -> BoxFuture<'a, Result<StreamResponse, AgentError>> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
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
        smol::block_on(common::bounded(finished.recv_async())).unwrap(),
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

fn request(requests: &flume::Receiver<Request>) -> Request {
    smol::block_on(common::bounded(requests.recv_async())).unwrap()
}

fn contains(request: &Request, text: &str) -> bool {
    serde_json::to_string(&request.messages)
        .unwrap()
        .contains(text)
}

#[test_case(false; "running_and_queued_child")]
#[test_case(true; "child_result_continuation")]
fn published_crash_prefix_recovers_lua_tickets_without_reexecution(continuation: bool) {
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
    let first = request(&requests);
    let blocked = if continuation {
        assert!(contains(&first, PARENT));
        first
            .reply
            .send(common::canned_tool_use(SPAWN_TOOL, json!({})))
            .unwrap();
        let parent = request(&requests);
        assert!(contains(&parent, PARENT));
        parent.reply.send(common::canned_reply(REPLY)).unwrap();
        command(&host, "wait_parent");
        command(&host, "start_descendant");
        let child = request(&requests);
        assert!(contains(&child, DESCENDANT));
        child.reply.send(common::canned_reply(REPLY)).unwrap();
        let continuation = request(&requests);
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
    let mut host = host.crash_restart_host(tmp.path());
    drop(blocked);
    let mut recovered_state: Option<Value> = None;
    for restart in 0..2 {
        command(&host, "inspect");
        assert_eq!(provider.calls.load(Ordering::SeqCst), calls);
        assert!(requests.is_empty());
        assert_eq!(ids(&host), saved);
        let recovered = TestSession::load(session_id, &state).unwrap();
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
            if entry["interrupted"].as_bool().unwrap() {
                assert_eq!(
                    results[0].outcome.status,
                    StoredTurnStatus::Cancelled {
                        reason: StoredCancellationReason::Interrupted
                    }
                );
            }
        }
        let state = common::normalized_history_epoch_state(recovered.state());
        let projection = json!({
            "state": state,
            "agents": recovered.agent_revisions(),
            "messages": recovered.messages(),
            "children": recovered.subagent_messages(),
            "metadata": recovered.subagents(),
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
