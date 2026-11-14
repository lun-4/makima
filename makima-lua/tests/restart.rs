mod common;

use std::{
    path::Path,
    sync::{Arc, Mutex},
};

use common::{CannedProvider, RestartHost, TestSession};
use makima_agent::{
    AgentId, McpSession,
    mcp::test_support::stub_session_with_result,
    permissions::{PermissionCheck, PermissionSnapshot},
    session_coordinator::SessionCoordinatorHandle,
};
use makima_config::ToolKey;
use makima_lua::test_support::{InMemoryFs, run_command};
use makima_providers::{
    AgentError, ContentBlock, Effort, Invocation, Message, Model, ModelInfo, PreparedProvider,
    ProviderBackend, ProviderEvent, ProviderRegistry, RegisteredProvider, RequestDelivery,
    RequestOptions, StreamResponse, ThinkingConfig,
    login::{AuthSpec, AuthStatus, InteractionService},
    provider::{BoxFuture, Provider},
};
use makima_storage::session_state::{SessionStateView, StoredAgentLifecycle};
use makima_storage::{
    StateDir,
    id::SessionRef,
    sessions::{SESSIONS_DIR, StoredEffect, StoredMode, StoredRule},
};
use serde_json::{Value, json};
use test_case::test_case;

const PLUGIN: &str = "restart_probe";
const TOOL: &str = "restart_probe";
const SAVED_MESSAGE: &str = "published before restart";
const UNSAVED_MESSAGE: &str = "unpublished before restart";
const FILE_BODY: &str = "Lua file survives VM replacement";
const FILE_NAME: &str = "plugin-data.txt";
const PLUGIN_SOURCE: &str = r#"
local calls = 0
makima.api.register_tool({
    name = "restart_probe",
    description = "probe a fresh VM and its file backend",
    schema = { type = "object", properties = { path = { type = "string" } }, required = { "path" } },
    handler = function(input)
        calls = calls + 1
        local body, err = makima.fs.read(input.path)
        assert(body, err)
        assert(makima.fs.write(input.path, body))
        return tostring(calls) .. ":" .. body
    end,
})
"#;

const COMMAND: &str = "/restart_run";
const CHILD_MODEL: &str = "anthropic/claude-haiku-4-5";
const UPDATED_CHILD_MODEL: &str = "anthropic/claude-opus-4-8";
const REPLY: &str = "restart child completed";
const LOCAL_REPLY: &str = "resident closure executed";
const SAVED_IDS: &str = "saved-agent.json";
const NOT_RESTORABLE: &str = "not_restorable: agent uses plugin-local tools";
const RETIRED: &str = "retired";
const TOOL_UNAVAILABLE: &str = "tool_unavailable: restart_echo";
const MODEL_UNAVAILABLE: &str = "model_unavailable";
const PROVIDER_UNAVAILABLE: &str = "provider_unavailable";
const DISCOVERY_ERROR: &str = "model discovery failed";
const REPLACEMENT_HANDLER: &str = "replacement registry handler executed";
const CUSTOM_MODE: &str = "restart_audit";
const SAVED_DIRECTIVE: &str = "Saved audit directive, not the current registry definition.";
const CHANGED_DIRECTIVE: &str = "Changed audit directive must not replace the snapshot.";
const OPTION: &str = "restart_probe.choice";
const TRANSIENT_OPTION: &str = "restart_probe.transient";
const INITIAL_OPTION_VALUE: &str = "initial";
const SAVED_OPTION_VALUE: &str = "saved";
const PERMISSION_TOOL: &str = "restart_permissions";
const ALLOWED_SCOPE: &str = "allowed";
const DENIED_SCOPE: &str = "denied";
const MCP_TOOL: &str = "server__lookup";
const MCP_DESCRIPTION: &str = "saved MCP lookup definition";
const MCP_OLD_REPLY: &str = "MCP before restart";
const MCP_FRESH_REPLY: &str = "fresh MCP transport after restart";
const MCP_UNAVAILABLE: &str = "mcp_unavailable: server";
const CONCURRENT_MESSAGES: [&str; 2] = [
    "first concurrent restart send",
    "second concurrent restart send",
];
const OPTION_SOURCE: &str = r#"
makima.api.register_session_option({
    id = "restart_probe.choice", name = "Restart choice", description = "Persistent restart choice",
    category = "mode", persistent = true, initial_value = "initial",
    values = { { value = "initial", name = "Initial" }, { value = "saved", name = "Saved" } },
})
makima.api.register_session_option({
    id = "restart_probe.transient", name = "Transient choice", description = "Reset on restart",
    category = "mode", persistent = false, initial_value = "initial",
    values = { { value = "initial", name = "Initial" }, { value = "saved", name = "Saved" } },
})
"#;
const PERMISSION_SOURCE: &str = r#"
local function permissions(ctx)
    local function allowed(scope, force)
        return makima.agent.permission_prompt(ctx, {
            tool = "restart_permissions", scopes = { scope }, force_prompt = force,
        }) == true
    end
    return { allowed = allowed("allowed", false), denied = not allowed("denied", false) }
end
makima.api.register_tool({
    name = "restart_permissions", description = "read shared permission policy",
    schema = { type = "object", properties = {} },
    handler = function(_, ctx) return makima.json.encode(permissions(ctx)) end,
})
"#;
const ECHO_PLUGIN_SOURCE: &str = r#"
makima.api.register_tool({
    name = "restart_echo", description = "read the executing child's restored context",
    schema = { type = "object", properties = {} },
    handler = function(_, ctx)
        local selection = ctx:model()
        return makima.json.encode({ model = selection.spec, thinking = selection.thinking, fast = selection.fast,
            workflow = ctx:workflow(), mode = ctx:mode(), cwd = assert(ctx:resolve_path(".")) })
    end,
})
"#;
const AGENT_PLUGIN: &str = r#"
local child
local function saved(path)
    return assert(makima.json.decode(assert(makima.fs.read(path))))
end
local function remember(ctx, agent, ticket, path, prompt)
    local result = assert(ticket:wait(ctx))
    assert(result.status == "completed", result.error)
    assert(makima.fs.write(path, makima.json.encode({
        agent_id = agent:id(), turn_id = ticket:id(), root_id = assert(makima.agent.root(ctx)):id(),
        text = result.text, output = result.output, prompt = prompt,
    })))
end
local function inspect(ctx, input, closed)
    local ids = saved(input.path)
    local agent = assert(makima.agent.get(ctx, ids.agent_id))
    assert(agent:id() == ids.agent_id)
    assert(assert(makima.agent.root(ctx)):id() == ids.root_id)
    assert(assert(makima.agent.get(ctx, assert(agent:ref()))):id() == ids.agent_id)
    assert(not agent:is_live())
    local status = assert(agent:status(ctx))
    assert(not status.live and status.status == (closed and "closed" or "idle"))
    assert(status.active_turn == nil and status.queued == 0)
    local ticket = assert(agent:turn(ctx, ids.turn_id))
    assert(ticket:id() == ids.turn_id and ticket:agent_id() == ids.agent_id)
    for _, result in ipairs({ assert(ticket:result(ctx)), assert(ticket:wait(ctx, { timeout = 0 })) }) do
        assert(result.status == "completed" and result.text == ids.text)
        assert(makima.json.encode(result.output) == makima.json.encode(ids.output))
    end
    local through_turn, err = agent:transcript(ctx, {
        through_turn = ids.turn_id, last_messages = 100, max_bytes = 65536,
    })
    local unavailable_turn_history = "exact history boundary is unavailable for turn: "
    assert(through_turn == nil and err == unavailable_turn_history .. ids.turn_id, tostring(err))
    local transcript = assert(agent:transcript(ctx, { last_messages = 100, max_bytes = 65536 }))
    assert(transcript.agent_id == ids.agent_id and transcript.through_turn == nil)
    local history = makima.json.encode(transcript.messages)
    assert(history:find(ids.prompt, 1, true))
    assert(history:find(ids.text, 1, true))
    assert(not agent:is_live() and not assert(agent:status(ctx)).live)
    return agent
end
makima.api.register_tool({
    name = "restart_extra", description = "must not be added on reactivation",
    schema = { type = "object", properties = {} }, handler = function() return "extra" end,
})
makima.api.register_tool({
    name = "restart_inherited", description = "spawn a child inheriting its parent's closure",
    schema = { type = "object", properties = { path = { type = "string" } }, required = { "path" } },
    handler = function(input, ctx)
        local descendant = assert(makima.agent.spawn(ctx, { inherit_provider = true, silent = true, mcp = false }))
        local prompt = "inherited resident closure"
        local ticket = assert(descendant:send(ctx, prompt, { when = "idle" }))
        remember(ctx, descendant, ticket, input.path, prompt)
        return "inherited child completed"
    end,
})
makima.api.register_command({
    name = "/restart_run", tui_only = false,
    arguments = { raw = true, required = true },
    handler = function(opts, ctx)
        local input = assert(makima.json.decode(opts.args))
        if input.action == "create" then
            local options = {
                model = input.model, silent = true, mcp = input.kind == "mcp", auto_deliver = false,
                system = "Persist this exact child system prompt.",
                tools = { { name = "restart_echo", description = "frozen echo definition", input_schema = { type = "object", properties = {} } } },
            }
            if input.kind == "permissions" then
                options.tools = { { name = "restart_permissions", description = "shared permission probe", input_schema = { type = "object", properties = {} } } }
            elseif input.kind == "mcp" then
                options.tools = { { name = "server__lookup", description = "saved MCP lookup definition", input_schema = {} } }
            elseif input.kind == "structured" then
                options.output_schema = { type = "object", properties = { answer = { type = "integer" } }, required = { "answer" } }
            elseif input.kind == "local" or input.kind == "inherited" then
                options.local_tools = { resident_closure = {
                    description = "closure usable only while its owner is resident", input_schema = { type = "object", properties = {} },
                    handler = function() return "resident closure executed" end,
                } }
                if input.kind == "inherited" then
                    options.tools[#options.tools + 1] = { name = "restart_inherited", description = "inherit closure", input_schema = {
                        type = "object", properties = { path = { type = "string" } }, required = { "path" },
                    } }
                end
            end
            child = assert(makima.agent.spawn(ctx, options))
            local mode = input.mode or "plan"
            assert(child:set_mode(ctx, mode) == mode)
            local prompt = "child work before restart"
            local ticket = assert(child:send(ctx, prompt, { when = "idle" }))
            if input.kind == "inherited" then
                local result = assert(ticket:wait(ctx))
                assert(result.status == "completed", result.error)
            else
                remember(ctx, child, ticket, input.path, prompt)
            end
        elseif input.action == "configure" then
            local agent = assert(makima.agent.get(ctx, saved(input.path).agent_id))
            local selection = assert(agent:set_model(ctx, { thinking = "high", fast = true }))
            assert(selection.thinking == "high" and selection.fast)
            assert(agent:set_option(ctx, "workflow", "enabled"))
            local root = assert(makima.agent.get(ctx, assert(makima.agent.root(ctx))))
            assert(root:set_option(ctx, "restart_probe.choice", "saved"))
            assert(root:set_option(ctx, "restart_probe.transient", "saved"))
        elseif input.action == "configure_authority" then
            local root = assert(makima.agent.get(ctx, assert(makima.agent.root(ctx))))
            assert(root:set_model(ctx, { spec = input.model, thinking = "high", fast = true }))
            assert(root:set_mode(ctx, "plan") == "plan")
            assert(root:set_option(ctx, "workflow", "enabled"))
            assert(root:set_option(ctx, "yolo", input.yolo and "enabled" or "disabled"))
        elseif input.action == "set_yolo" then
            local root = assert(makima.agent.get(ctx, assert(makima.agent.root(ctx))))
            assert(root:set_option(ctx, "yolo", input.yolo and "enabled" or "disabled"))
        elseif input.action == "check_authority" then
            local root = assert(makima.agent.get(ctx, assert(makima.agent.root(ctx))))
            for _, agent in ipairs({ root, assert(makima.agent.get(ctx, saved(input.path).agent_id)) }) do
                local config = assert(agent:config(ctx))
                assert(config.model.spec == input.model and config.model.thinking == "high" and config.model.fast)
                assert(config.mode == "plan" and config.options.workflow == "enabled")
                assert(config.options.yolo == (input.yolo and "enabled" or "disabled"))
                assert(config.options["restart_probe.choice"] == "saved")
                assert(config.options["restart_probe.transient"] == "initial")
            end
        elseif input.action == "check_config" then
            local agent = assert(makima.agent.get(ctx, saved(input.path).agent_id))
            local config = assert(agent:config(ctx))
            assert(config.model.thinking == "high" and config.model.fast)
            assert(config.options.workflow == "enabled" and config.options["restart_probe.choice"] == "saved")
            assert(config.options["restart_probe.transient"] == (input.transient or "initial"))
            local root = assert(makima.agent.get(ctx, assert(makima.agent.root(ctx))))
            local root_config = assert(root:config(ctx))
            assert(root_config.options["restart_probe.choice"] == "saved")
            assert(root_config.options["restart_probe.transient"] == (input.transient or "initial"))
        elseif input.action == "concurrent" then
            local agent = inspect(ctx, input, false)
            local outcomes = makima.async.gather({
                function() return assert(agent:send(ctx, input.messages[1], { when = "queue" })) end,
                function() return assert(agent:send(ctx, input.messages[2], { when = "queue" })) end,
            })
            local tickets = {}
            for i, outcome in ipairs(outcomes) do
                assert(outcome.ok, outcome.err)
                tickets[i] = assert(outcome.value)
            end
            assert(#tickets == #input.messages)
            assert(tickets[1]:id() ~= tickets[2]:id())
            for _, ticket in ipairs(tickets) do
                assert(ticket:agent_id() == agent:id())
                local result = assert(ticket:wait(ctx))
                assert(result.status == "completed", result.error)
            end
            local history = makima.json.encode(assert(agent:transcript(ctx, { last_messages = 100, max_bytes = 65536 })).messages)
            for _, message in ipairs(input.messages) do assert(history:find(message, 1, true)) end
            assert(agent:is_live() and assert(agent:status(ctx)).queued == 0)
        elseif input.action == "set_model" then
            local agent = assert(makima.agent.get(ctx, saved(input.path).agent_id))
            local selection, err = agent:set_model(ctx, input.model)
            assert(selection, err)
            assert(selection.spec == input.model)
            assert(assert(agent:config(ctx)).model.spec == input.model)
        elseif input.action == "read" then
            inspect(ctx, input, input.closed)
        elseif input.action == "resume" then
            local agent = inspect(ctx, input, false)
            local ids = saved(input.path)
            local ticket = assert(agent:send(ctx, "child work after restart", { when = "idle" }))
            local result = assert(ticket:wait(ctx))
            assert(result.status == "completed", result.error)
            assert(agent:is_live() and assert(agent:status(ctx)).live)
            local config = assert(agent:config(ctx))
            assert(config.model.spec == input.model and config.mode == (input.mode or "plan"))
            if input.kind == "structured" then
                assert(#result.output == 1 and result.output[1].answer == 42 and result.captured.answer == 42)
                local previous = assert(assert(agent:turn(ctx, ids.turn_id)):result(ctx))
                assert(#previous.output == 1 and previous.output[1].answer == 41 and previous.captured.answer == 41)
            elseif input.kind ~= "mcp" and input.kind ~= "permissions" then
                local history = makima.json.encode(assert(agent:transcript(ctx, { last_messages = 100, max_bytes = 65536 })).messages)
                assert(history:find(input.model, 1, true) and history:find(input.cwd, 1, true) and history:find(input.mode or "plan", 1, true))
            end
        elseif input.action == "reject" then
            local agent = inspect(ctx, input, input.closed)
            local ticket, err = agent:send(ctx, "must not execute", { when = "idle" })
            assert(ticket == nil and err:find(input.error, 1, true), tostring(err))
            assert(not agent:is_live() and not assert(agent:status(ctx)).live)
        elseif input.action == "retire" then
            local agent = assert(makima.agent.get(ctx, saved(input.path).agent_id))
            assert(agent:retire(ctx))
            inspect(ctx, input, true)
        end
    end,
})
"#;

#[test]
fn restart_host_reloads_plugins_and_session() {
    let dir = tempfile::tempdir().unwrap();
    let state = StateDir::from_path(dir.path().into());
    let mut session = TestSession::new(
        &common::default_model().spec(),
        dir.path().to_str().unwrap(),
    );
    session.push_message(makima_providers::Message::user(SAVED_MESSAGE.into()));
    session.save(&state).unwrap();
    let session_id = session.id;
    let log_path = dir
        .path()
        .join(SESSIONS_DIR)
        .join(format!("{session_id}.jsonl"));
    let fs = Arc::new(InMemoryFs::new());
    let plugin_path = dir.path().join("plugins").join(format!("{PLUGIN}.lua"));
    let file_path = dir.path().join(FILE_NAME);
    fs.seed(&plugin_path, PLUGIN_SOURCE.as_bytes().to_vec());
    fs.seed(&file_path, FILE_BODY.as_bytes().to_vec());
    let mut harness = RestartHost::open(dir.path(), session_id, fs, vec![PLUGIN.into()]);
    let published = std::fs::read(&log_path).unwrap();
    let old_root = harness.root.target.clone();
    let old_registry = harness.host.registry();
    let old_fs = Arc::clone(&harness.fs);
    let probe = |host: &RestartHost| {
        common::exec_tool_text(
            &host.host.registry(),
            &host.root.template,
            TOOL,
            json!({ "path": file_path }),
        )
        .unwrap()
    };
    assert_eq!(probe(&harness), format!("1:{FILE_BODY}"));
    assert_eq!(probe(&harness), format!("2:{FILE_BODY}"));
    assert_eq!(
        harness
            .root
            .target
            .effective_config()
            .unwrap()
            .unwrap()
            .context
            .cwd(),
        dir.path()
    );
    harness
        .session
        .push_message(makima_providers::Message::user(UNSAVED_MESSAGE.into()));

    let harness = harness.restart_host(dir.path());

    assert!(!harness.root.target.same_manager(&old_root));
    assert!(!Arc::ptr_eq(&harness.host.registry(), &old_registry));
    assert!(!Arc::ptr_eq(&harness.fs, &old_fs));
    assert_eq!(probe(&harness), format!("1:{FILE_BODY}"));
    assert_eq!(
        harness
            .root
            .target
            .effective_config()
            .unwrap()
            .unwrap()
            .context
            .cwd(),
        dir.path()
    );
    assert_eq!(harness.session.id, session_id);
    assert_eq!(harness.session.messages().len(), 1);
    assert_eq!(
        harness.session.messages()[0].user_text(),
        Some(SAVED_MESSAGE)
    );
    let restored = harness.history.load();
    assert_eq!(restored.messages.len(), 1);
    assert_eq!(restored.messages[0].user_text(), Some(SAVED_MESSAGE));
    assert_eq!(std::fs::read(log_path).unwrap(), published);
    assert!(!plugin_path.exists());
    assert!(!file_path.exists());
}

struct Request {
    model: String,
    system: String,
    messages: Vec<Message>,
    tools: Value,
    opts: RequestOptions,
}

struct RestartProvider {
    canned: CannedProvider,
    requests: Mutex<Vec<Request>>,
    expected_config: Mutex<Option<(StateDir, SessionRef, AgentId, String)>>,
}

impl RestartProvider {
    fn new(replies: Vec<StreamResponse>) -> Arc<Self> {
        Arc::new(Self {
            canned: CannedProvider::new(replies),
            requests: Mutex::default(),
            expected_config: Mutex::default(),
        })
    }

    fn request_count(&self) -> usize {
        self.requests.lock().unwrap().len()
    }
}

impl Provider for RestartProvider {
    fn stream_message<'a>(
        &'a self,
        model: &'a Model,
        messages: &'a [Message],
        system: &'a str,
        tools: &'a Value,
        event_tx: &'a flume::Sender<ProviderEvent>,
        opts: RequestOptions,
        session_id: Option<&'a SessionRef>,
        invocation: &'a Invocation,
        delivery: RequestDelivery,
    ) -> BoxFuture<'a, Result<StreamResponse, AgentError>> {
        if let Some((state, session, agent, mode)) = self.expected_config.lock().unwrap().as_ref() {
            let stored = TestSession::load(session.id(), state).unwrap();
            let folded = stored.latest_state().unwrap();
            assert_eq!(
                folded.scopes[&Some(*agent)]
                    .config
                    .as_ref()
                    .unwrap()
                    .mode
                    .id,
                *mode
            );
        }
        self.requests.lock().unwrap().push(Request {
            model: model.spec(),
            system: system.into(),
            messages: messages.into(),
            tools: tools.clone(),
            opts,
        });
        self.canned.stream_message(
            model, messages, system, tools, event_tx, opts, session_id, invocation, delivery,
        )
    }
}

fn agent_host(state_dir: &Path, provider: Arc<dyn Provider>) -> RestartHost {
    agent_host_with_mcp(state_dir, provider, None)
}

fn agent_host_with_mcp(
    state_dir: &Path,
    provider: Arc<dyn Provider>,
    mcp: Option<McpSession>,
) -> RestartHost {
    let state = StateDir::from_path(state_dir.into());
    let mut session =
        TestSession::new(&common::default_model().spec(), state_dir.to_str().unwrap());
    session.save(&state).unwrap();
    let fs = Arc::new(InMemoryFs::new());
    fs.seed(
        &state_dir.join("plugins").join(format!("{PLUGIN}.lua")),
        format!("{PERMISSION_SOURCE}\n{ECHO_PLUGIN_SOURCE}\n{AGENT_PLUGIN}").into_bytes(),
    );
    RestartHost::open_with_provider_registry_and_mcp(
        state_dir,
        session.id,
        fs,
        vec![PLUGIN.into()],
        provider,
        common::test_providers(),
        mcp,
    )
}

fn command(host: &RestartHost, input: Value) {
    let finished = run_command(
        &host.host.command_registry(),
        &format!("{COMMAND} {input}"),
        0,
        Some(host.root.clone()),
    );
    assert_eq!(
        smol::block_on(common::bounded(finished.recv_async())).unwrap(),
        Ok(())
    );
}

fn saved(host: &RestartHost, path: &Path) -> Value {
    let files = host.fs.files();
    let (_, bytes) = files.iter().find(|(file, _)| file == path).unwrap();
    serde_json::from_slice(bytes).unwrap()
}

fn reject_without_mutation(
    host: &RestartHost,
    provider: &RestartProvider,
    path: &Path,
    error: &str,
    closed: bool,
) {
    let disk = host
        .root
        .template
        .cwd
        .join(SESSIONS_DIR)
        .join(format!("{}.jsonl", host.session.id));
    let published = std::fs::read(&disk).unwrap();
    let ids = saved(host, path);
    let id = ids["agent_id"].as_str().unwrap().parse().unwrap();
    let before = serde_json::to_vec(&host.manager.journal().agent(id).unwrap()).unwrap();
    let journal = host.manager.journal().checkpoint_snapshot().unwrap();
    let requests = provider.request_count();
    command(
        host,
        json!({ "action": "reject", "path": path, "error": error, "closed": closed }),
    );
    assert_eq!(provider.request_count(), requests);
    assert_eq!(std::fs::read(disk).unwrap(), published);
    let after = host.manager.journal().checkpoint_snapshot().unwrap();
    assert_eq!(after.seq, journal.seq);
    assert_eq!(after.state, journal.state);
    assert_eq!(after.agent_revisions, journal.agent_revisions);
    assert_eq!(
        serde_json::to_vec(&host.manager.journal().agent(id).unwrap()).unwrap(),
        before
    );
}

struct DiscoveredModels {
    models: Arc<Mutex<Result<Vec<ModelInfo>, String>>>,
    calls: Arc<Mutex<usize>>,
    discovery_gate: Option<(flume::Sender<()>, flume::Receiver<()>)>,
}

impl ProviderBackend for DiscoveredModels {
    fn prepare(
        &self,
        _: Model,
        _: ModelInfo,
    ) -> BoxFuture<'static, Result<PreparedProvider, AgentError>> {
        Box::pin(async {
            Err(AgentError::Config {
                message: DISCOVERY_ERROR.into(),
            })
        })
    }

    fn discover(
        &self,
        refresh: bool,
    ) -> Option<BoxFuture<'static, Result<Vec<ModelInfo>, AgentError>>> {
        let models = Arc::clone(&self.models);
        let calls = Arc::clone(&self.calls);
        let gate = self.discovery_gate.clone();
        Some(Box::pin(async move {
            assert!(refresh);
            *calls.lock().unwrap() += 1;
            if let Some((entered, release)) = gate {
                entered.send_async(()).await.unwrap();
                release.recv_async().await.unwrap();
            }
            models
                .lock()
                .unwrap()
                .clone()
                .map_err(|message| AgentError::Config { message })
        }))
    }

    fn login(
        &self,
        _: String,
        _: Value,
        _: Arc<dyn InteractionService>,
    ) -> BoxFuture<'static, Result<Option<String>, AgentError>> {
        Box::pin(async { Ok(None) })
    }

    fn auth_status(&self, _: Value) -> Option<BoxFuture<'static, Result<AuthStatus, AgentError>>> {
        None
    }

    fn logout(&self) -> Option<BoxFuture<'static, Result<(), AgentError>>> {
        None
    }
}

#[test_case("removed_provider", PROVIDER_UNAVAILABLE, CHILD_MODEL; "removed_provider")]
#[test_case("removed_model", MODEL_UNAVAILABLE, CHILD_MODEL; "removed_model")]
#[test_case("discovery_failure", PROVIDER_UNAVAILABLE, CHILD_MODEL; "discovery_failure")]
#[test_case("removed_model", MODEL_UNAVAILABLE, "anthropic/namespace/model-v1"; "full_model_id")]
fn restart_reactivation_requires_current_discovery_and_retries_without_admission(
    failure: &str,
    error: &str,
    model_spec: &str,
) {
    let dir = tempfile::tempdir().unwrap();
    let provider = RestartProvider::new(vec![
        common::canned_tool_use("restart_echo", json!({})),
        common::canned_reply(REPLY),
        common::canned_tool_use("restart_echo", json!({})),
        common::canned_reply(REPLY),
    ]);
    let host = agent_host(dir.path(), provider.clone());
    let path = dir.path().join(SAVED_IDS);
    command(
        &host,
        json!({ "action": "create", "model": model_spec, "path": path }),
    );
    let (provider_id, model_id) = Model::parse_spec(model_spec).unwrap();
    let models = Arc::new(Mutex::new(if failure == "discovery_failure" {
        Err(DISCOVERY_ERROR.to_owned())
    } else {
        Ok(vec![ModelInfo::id_only(format!("{model_id}-other"))])
    }));
    let calls = Arc::new(Mutex::new(0));
    let registry = Arc::new(ProviderRegistry::new(if failure == "removed_provider" {
        Vec::new()
    } else {
        vec![RegisteredProvider {
            id: provider_id.into(),
            display_name: provider_id.into(),
            models: vec![ModelInfo::id_only(model_id.into())],
            backend: Arc::new(DiscoveredModels {
                models: Arc::clone(&models),
                calls: Arc::clone(&calls),
                discovery_gate: None,
            }),
            auth: AuthSpec::default(),
            docs: None,
        }]
    }));
    let host = host.restart_host_with_provider_registry(dir.path(), registry);
    let disk = dir
        .path()
        .join(SESSIONS_DIR)
        .join(format!("{}.jsonl", host.session.id));
    let before = std::fs::read(&disk).unwrap();
    let unavailable = if error == MODEL_UNAVAILABLE {
        format!("{error}: {model_spec}")
    } else {
        format!("{error}: {provider_id}")
    };
    reject_without_mutation(&host, &provider, &path, &unavailable, false);
    assert_eq!(std::fs::read(&disk).unwrap(), before);
    assert_eq!(
        *calls.lock().unwrap(),
        usize::from(failure != "removed_provider")
    );
    let host = if failure == "removed_provider" {
        host.restart_host_with_provider_registry(dir.path(), common::test_providers())
    } else {
        *models.lock().unwrap() = Ok(vec![ModelInfo::id_only(model_id.into())]);
        host
    };
    command(
        &host,
        json!({ "action": "resume", "model": model_spec, "path": path, "cwd": dir.path() }),
    );
    assert_eq!(provider.request_count(), 4);
    if failure != "removed_provider" {
        assert_eq!(*calls.lock().unwrap(), 2);
    }
}

#[test]
fn runtime_shutdown_detaches_children_without_closing_durable_records() {
    let dir = tempfile::tempdir().unwrap();
    let provider = RestartProvider::new(vec![
        common::canned_tool_use("restart_echo", json!({})),
        common::canned_reply(REPLY),
    ]);
    let host = agent_host(dir.path(), provider.clone());
    let path = dir.path().join(SAVED_IDS);
    command(
        &host,
        json!({ "action": "create", "model": CHILD_MODEL, "path": path }),
    );
    let child_id = saved(&host, &path)["agent_id"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    assert!(host.manager.is_live(child_id));
    drop(host.host);
    assert!(!host.manager.is_live(child_id));
    assert_eq!(
        host.manager.journal().agent(child_id).unwrap().lifecycle,
        StoredAgentLifecycle::Open,
    );
    assert_eq!(provider.request_count(), 2);
}

#[test]
fn saved_child_and_ticket_survive_restart_and_send_restores_exact_configuration() {
    let dir = tempfile::tempdir().unwrap();
    let provider = RestartProvider::new(vec![
        common::canned_tool_use("restart_echo", json!({})),
        common::canned_reply(REPLY),
        common::canned_tool_use("restart_echo", json!({})),
        common::canned_reply(REPLY),
    ]);
    let host = agent_host(dir.path(), provider.clone());
    let path = dir.path().join(SAVED_IDS);
    command(
        &host,
        json!({ "action": "create", "model": CHILD_MODEL, "path": path }),
    );
    let ids = saved(&host, &path);
    let root = host.root.target.clone();
    assert_eq!(provider.request_count(), 2);
    let child_id = ids["agent_id"].as_str().unwrap().parse().unwrap();
    let previous_manager = host.manager.clone();
    let host = host.restart_host(dir.path());
    assert!(!previous_manager.is_live(child_id));
    assert_eq!(
        previous_manager
            .journal()
            .agent(child_id)
            .unwrap()
            .lifecycle,
        StoredAgentLifecycle::Open,
    );
    assert!(!host.root.target.same_manager(&root));
    assert_eq!(saved(&host, &path), ids);
    assert_eq!(provider.request_count(), 2);
    command(&host, json!({ "action": "read", "path": path }));
    assert_eq!(provider.request_count(), 2);
    command(
        &host,
        json!({ "action": "resume", "model": CHILD_MODEL, "path": path, "cwd": dir.path() }),
    );
    assert_eq!(provider.request_count(), 4);
    {
        let requests = provider.requests.lock().unwrap();
        let before = &requests[0];
        let after = &requests[2];
        assert_eq!(before.model, CHILD_MODEL);
        assert_eq!(after.model, before.model);
        assert_eq!(after.system, before.system);
        assert_eq!(common::tool_names(&before.tools), ["restart_echo"]);
        assert_eq!(after.tools, before.tools);
        assert_eq!(after.tools[0]["description"], "frozen echo definition");
        let history = serde_json::to_string(&after.messages).unwrap();
        assert!(history.contains(REPLY));
        let echo = requests[3]
            .messages
            .iter()
            .rev()
            .flat_map(|message| message.content.iter().rev())
            .find_map(|block| {
                if let ContentBlock::ToolResult {
                    content,
                    is_error: false,
                    ..
                } = block
                {
                    serde_json::from_str::<Value>(content).ok()
                } else {
                    None
                }
            })
            .expect("restored registry tool must return its execution context");
        assert_eq!(
            echo,
            json!({ "model": CHILD_MODEL, "thinking": "off", "fast": false,
                "workflow": false, "mode": "plan", "cwd": dir.path().join(".") })
        );
    }
    command(&host, json!({ "action": "retire", "path": path }));
    reject_without_mutation(&host, &provider, &path, RETIRED, true);
    let host = host.restart_host(dir.path());
    command(
        &host,
        json!({ "action": "read", "path": path, "closed": true }),
    );
    reject_without_mutation(&host, &provider, &path, RETIRED, true);
    assert_eq!(provider.request_count(), 4);
}

#[test]
fn durable_child_model_change_publishes_prepared_journal_without_reacquiring_it() {
    let dir = tempfile::tempdir().unwrap();
    let provider = RestartProvider::new(vec![
        common::canned_tool_use("restart_echo", json!({})),
        common::canned_reply(REPLY),
        common::canned_tool_use("restart_echo", json!({})),
        common::canned_reply(REPLY),
    ]);
    let host = agent_host(dir.path(), provider.clone());
    let path = dir.path().join(SAVED_IDS);
    command(
        &host,
        json!({ "action": "create", "model": CHILD_MODEL, "path": path }),
    );
    command(
        &host,
        json!({ "action": "set_model", "model": UPDATED_CHILD_MODEL, "path": path }),
    );
    assert_eq!(provider.request_count(), 2);
    let child_id = saved(&host, &path)["agent_id"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    let stored =
        TestSession::load(host.session.id, &StateDir::from_path(dir.path().into())).unwrap();
    let state = stored.latest_state().unwrap();
    assert_eq!(
        state.scopes[&Some(child_id)].config.as_ref().unwrap().model,
        UPDATED_CHILD_MODEL
    );
    let host = host.restart_host(dir.path());
    command(
        &host,
        json!({ "action": "resume", "model": UPDATED_CHILD_MODEL, "path": path, "cwd": dir.path() }),
    );
    assert_eq!(provider.request_count(), 4);
    assert_eq!(
        provider.requests.lock().unwrap()[2].model,
        UPDATED_CHILD_MODEL
    );
}

#[test]
fn restart_missing_tool_leaves_child_unchanged_then_reactivation_uses_current_handler() {
    let dir = tempfile::tempdir().unwrap();
    let provider = RestartProvider::new(vec![
        common::canned_tool_use("restart_echo", json!({})),
        common::canned_reply(REPLY),
        common::canned_tool_use("restart_echo", json!({})),
        common::canned_reply(REPLY),
    ]);
    let host = agent_host(dir.path(), provider.clone());
    let path = dir.path().join(SAVED_IDS);
    command(
        &host,
        json!({ "action": "create", "model": CHILD_MODEL, "path": path }),
    );
    assert_eq!(provider.request_count(), 2);
    let ids = saved(&host, &path);
    host.fs.seed(
        &dir.path().join("plugins").join(format!("{PLUGIN}.lua")),
        AGENT_PLUGIN.as_bytes().to_vec(),
    );

    let host = host.restart_host(dir.path());
    assert_eq!(saved(&host, &path), ids);
    assert_eq!(provider.request_count(), 2);
    assert!(host.host.registry().get("restart_echo").is_none());
    reject_without_mutation(&host, &provider, &path, TOOL_UNAVAILABLE, false);
    reject_without_mutation(&host, &provider, &path, TOOL_UNAVAILABLE, false);

    let replacement = ECHO_PLUGIN_SOURCE
        .replace(
            "read the executing child's restored context",
            "replacement echo definition",
        )
        .replace(
            "model = selection.spec",
            &format!("handler = {REPLACEMENT_HANDLER:?}, model = selection.spec"),
        );
    host.host
        .load_source("restart_echo_replacement", &replacement)
        .unwrap();
    assert_eq!(provider.request_count(), 2);
    command(
        &host,
        json!({ "action": "resume", "model": CHILD_MODEL, "path": path, "cwd": dir.path() }),
    );
    assert_eq!(provider.request_count(), 4);
    {
        let requests = provider.requests.lock().unwrap();
        assert_eq!(requests[2].tools, requests[0].tools);
        assert_eq!(
            requests[2].tools[0]["description"],
            "frozen echo definition"
        );
        assert_eq!(common::tool_names(&requests[2].tools), ["restart_echo"]);
        let previous = serde_json::to_string(&requests[1].messages).unwrap();
        assert!(!previous.contains(REPLACEMENT_HANDLER));
        let current = &requests[3].messages;
        assert!(current.iter().flat_map(|message| &message.content).any(|block| {
            matches!(block, ContentBlock::ToolResult { content, is_error: false, .. } if content.contains(REPLACEMENT_HANDLER))
        }));
    }
    command(&host, json!({ "action": "retire", "path": path }));
}

#[test_case(false, false; "direct_after_owner_reload")]
#[test_case(true, false; "inherited_after_owner_reload")]
#[test_case(false, true; "direct_after_restart")]
#[test_case(true, true; "inherited_after_restart")]
fn local_closures_work_while_resident_but_cannot_be_reattached(inherited: bool, restart: bool) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join(SAVED_IDS);
    let mut replies = Vec::new();
    if inherited {
        replies.push(common::canned_tool_use(
            "restart_inherited",
            json!({ "path": path }),
        ));
    }
    replies.extend([
        common::canned_tool_use("resident_closure", json!({})),
        common::canned_reply(REPLY),
    ]);
    if inherited {
        replies.push(common::canned_reply(REPLY));
    }
    let provider = RestartProvider::new(replies);
    let host = agent_host(dir.path(), provider.clone());
    command(
        &host,
        json!({
            "action": "create", "kind": if inherited { "inherited" } else { "local" },
            "model": CHILD_MODEL, "path": path,
        }),
    );
    let expected_requests = if inherited { 4 } else { 2 };
    assert_eq!(provider.request_count(), expected_requests);
    {
        let requests = provider.requests.lock().unwrap();
        let completion = &requests[if inherited { 2 } else { 1 }];
        assert!(
            common::tool_names(&completion.tools)
                .iter()
                .any(|name| name == "resident_closure")
        );
        assert!(
            serde_json::to_string(&completion.messages)
                .unwrap()
                .contains(LOCAL_REPLY)
        );
    }
    let ids = saved(&host, &path);
    let host = if restart {
        host.restart_host(dir.path())
    } else {
        host.host
            .load_source(PLUGIN, &format!("{ECHO_PLUGIN_SOURCE}\n{AGENT_PLUGIN}"))
            .unwrap();
        host
    };
    assert_eq!(saved(&host, &path), ids);
    command(&host, json!({ "action": "read", "path": path }));
    reject_without_mutation(&host, &provider, &path, NOT_RESTORABLE, false);
    assert_eq!(provider.request_count(), expected_requests);
}

#[test]
fn restart_rebuilds_one_structured_output_handler_and_keeps_previous_capture() {
    let dir = tempfile::tempdir().unwrap();
    let provider = RestartProvider::new(vec![
        common::canned_tool_use("structured_output", json!({ "answer": 41 })),
        common::canned_reply(REPLY),
        common::canned_tool_use("structured_output", json!({ "answer": 42 })),
        common::canned_reply(REPLY),
    ]);
    let host = agent_host(dir.path(), provider.clone());
    let path = dir.path().join(SAVED_IDS);
    command(
        &host,
        json!({ "action": "create", "kind": "structured", "model": CHILD_MODEL, "path": path }),
    );
    assert_eq!(saved(&host, &path)["output"], json!([{ "answer": 41 }]));
    assert_eq!(provider.request_count(), 2);
    let host = host.restart_host(dir.path());
    command(&host, json!({ "action": "read", "path": path }));
    assert_eq!(provider.request_count(), 2);
    command(
        &host,
        json!({ "action": "resume", "kind": "structured", "model": CHILD_MODEL, "path": path }),
    );
    assert_eq!(provider.request_count(), 4);
    {
        let requests = provider.requests.lock().unwrap();
        for request in requests.iter() {
            let names = common::tool_names(&request.tools);
            assert_eq!(
                names
                    .iter()
                    .filter(|name| name.as_str() == "structured_output")
                    .count(),
                1
            );
            assert_eq!(names.len(), 2);
        }
        assert_eq!(requests[2].tools, requests[0].tools);
    }
    command(&host, json!({ "action": "retire", "path": path }));
}

#[test]
fn post_spawn_options_and_root_cwd_survive_restart_without_retargeting_child() {
    let dir = tempfile::tempdir().unwrap();
    let cwd = tempfile::tempdir().unwrap();
    let provider = RestartProvider::new(vec![
        common::canned_tool_use("restart_echo", json!({})),
        common::canned_reply(REPLY),
        common::canned_tool_use("restart_echo", json!({})),
        common::canned_reply(REPLY),
    ]);
    let host = agent_host(dir.path(), provider.clone());
    let source = format!("{OPTION_SOURCE}\n{ECHO_PLUGIN_SOURCE}\n{AGENT_PLUGIN}");
    host.fs.seed(
        &dir.path().join("plugins").join(format!("{PLUGIN}.lua")),
        source.as_bytes().to_vec(),
    );
    host.host.load_source(PLUGIN, &source).unwrap();
    let path = dir.path().join(SAVED_IDS);
    command(
        &host,
        json!({ "action": "create", "model": CHILD_MODEL, "path": path }),
    );
    command(&host, json!({ "action": "configure", "path": path }));
    command(
        &host,
        json!({ "action": "check_config", "path": path, "transient": SAVED_OPTION_VALUE }),
    );
    let coordinator = SessionCoordinatorHandle::resolve(host.session.id).unwrap();
    smol::block_on(common::bounded(
        coordinator.change_directory(cwd.path().into()),
    ))
    .unwrap();
    let child_id = saved(&host, &path)["agent_id"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    let stored =
        TestSession::load(host.session.id, &StateDir::from_path(dir.path().into())).unwrap();
    let state = stored.latest_state().unwrap();
    let child = state.scopes[&Some(child_id)].config.as_ref().unwrap();
    assert_eq!(child.cwd, dir.path().to_str().unwrap());
    assert!(child.fast && child.workflow);
    assert_eq!(
        child.thinking.map(ThinkingConfig::from),
        Some(ThinkingConfig::Effort(Effort::High))
    );
    let root = state.scopes[&Some(host.root.target.id())]
        .config
        .as_ref()
        .unwrap();
    assert_eq!(root.cwd, cwd.path().to_str().unwrap());
    assert_eq!(root.options[OPTION], SAVED_OPTION_VALUE);
    assert_eq!(root.options[TRANSIENT_OPTION], SAVED_OPTION_VALUE);
    assert_eq!(provider.request_count(), 2);
    drop(coordinator);
    let host = host.restart_host(dir.path());
    assert_eq!(
        host.root
            .target
            .effective_config()
            .unwrap()
            .unwrap()
            .context
            .cwd(),
        cwd.path()
    );
    command(&host, json!({ "action": "check_config", "path": path }));
    assert!(!host.manager.is_live(child_id));
    command(
        &host,
        json!({ "action": "resume", "model": CHILD_MODEL, "path": path, "cwd": dir.path() }),
    );
    command(&host, json!({ "action": "check_config", "path": path }));
    let requests = provider.requests.lock().unwrap();
    assert_eq!(requests.len(), 4);
    assert!(!requests[0].opts.fast);
    for request in &requests[2..] {
        assert!(request.opts.fast);
        assert_eq!(request.opts.thinking, ThinkingConfig::Effort(Effort::High));
    }
    assert!(requests[3].messages.iter().flat_map(|message| &message.content).any(|block| {
        matches!(block, ContentBlock::ToolResult { content, is_error: false, .. }
            if serde_json::from_str::<Value>(content).is_ok_and(|echo| echo["workflow"] == true && echo["fast"] == true && echo["thinking"] == "high"))
    }));
}

#[test_case(false; "prompts_restored")]
#[test_case(true; "yolo_restored")]
fn restart_folded_config_and_shared_rules_override_legacy_metadata(yolo: bool) {
    let dir = tempfile::tempdir().unwrap();
    let provider = RestartProvider::new(vec![
        common::canned_tool_use(PERMISSION_TOOL, json!({})),
        common::canned_reply(REPLY),
        common::canned_tool_use(PERMISSION_TOOL, json!({})),
        common::canned_reply(REPLY),
    ]);
    let host = agent_host(dir.path(), provider.clone());
    let source =
        format!("{OPTION_SOURCE}\n{PERMISSION_SOURCE}\n{ECHO_PLUGIN_SOURCE}\n{AGENT_PLUGIN}");
    host.fs.seed(
        &dir.path().join("plugins").join(format!("{PLUGIN}.lua")),
        source.as_bytes().to_vec(),
    );
    host.host.load_source(PLUGIN, &source).unwrap();
    let rules = vec![
        StoredRule {
            tool: PERMISSION_TOOL.into(),
            scope: Some(ALLOWED_SCOPE.into()),
            effect: StoredEffect::Allow,
        },
        StoredRule {
            tool: PERMISSION_TOOL.into(),
            scope: Some(DENIED_SCOPE.into()),
            effect: StoredEffect::Deny,
        },
    ];
    let policy = PermissionSnapshot::from_stored(&rules, Some(yolo)).unwrap();
    for rule in policy.rules {
        host.root.template.permissions.add_session_rule(rule);
    }
    host.root.template.permissions.set_yolo(yolo);
    let path = dir.path().join(SAVED_IDS);
    command(
        &host,
        json!({ "action": "create", "kind": "permissions", "model": CHILD_MODEL, "path": path }),
    );
    command(&host, json!({ "action": "configure", "path": path }));
    command(
        &host,
        json!({ "action": "configure_authority", "model": CHILD_MODEL, "yolo": yolo }),
    );
    let child_id = saved(&host, &path)["agent_id"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    let session_id = host.session.id;
    let root_id = host.root.target.id();
    let coordinator = SessionCoordinatorHandle::resolve(session_id).unwrap();
    smol::block_on(common::bounded(
        coordinator.persist_history(host.history.load_full().as_ref().clone()),
    ))
    .unwrap();
    drop(coordinator);
    let state_dir = StateDir::from_path(dir.path().into());
    let mut stored = TestSession::load(session_id, &state_dir).unwrap();
    let folded = SessionStateView::fold(
        &common::normalized_history_epoch_state(stored.state()),
        stored.agent_revisions(),
        stored.sequence_high_water().unwrap(),
    )
    .unwrap();
    for id in [root_id, child_id] {
        let config = folded.scopes[&Some(id)].config.as_ref().unwrap();
        assert_eq!(config.model, CHILD_MODEL);
        assert!(config.fast && config.workflow);
        assert_eq!(config.mode.id, "plan");
        assert_eq!(
            config.thinking.map(ThinkingConfig::from),
            Some(ThinkingConfig::Effort(Effort::High))
        );
    }
    let options = &folded.scopes[&Some(root_id)]
        .config
        .as_ref()
        .unwrap()
        .options;
    assert_eq!(options[OPTION], SAVED_OPTION_VALUE);
    assert_eq!(options[TRANSIENT_OPTION], SAVED_OPTION_VALUE);
    assert_eq!(folded.scopes[&None].rules, rules);
    assert_eq!(folded.scopes[&None].yolo, Some(yolo));
    let files = host.fs.files();
    drop(host);
    stored.meta.mode = Some(StoredMode::Build);
    stored.meta.thinking = Some(ThinkingConfig::Effort(Effort::Low).into());
    stored.meta.fast = false;
    stored.meta.workflow = false;
    stored.meta.yolo = Some(!yolo);
    stored
        .meta
        .session_options
        .insert(OPTION.into(), INITIAL_OPTION_VALUE.into());
    stored
        .meta
        .session_options
        .insert(TRANSIENT_OPTION.into(), SAVED_OPTION_VALUE.into());
    stored.meta.session_rules = rules
        .iter()
        .cloned()
        .map(|mut rule| {
            rule.effect = match rule.effect {
                StoredEffect::Allow => StoredEffect::Deny,
                StoredEffect::Deny => StoredEffect::Allow,
            };
            rule
        })
        .collect();
    let mut subagents = stored.subagents().to_vec();
    for child in &mut subagents {
        child.model = Some(UPDATED_CHILD_MODEL.into());
        child.thinking = Some(ThinkingConfig::Effort(Effort::Low).into());
        child.fast = false;
    }
    stored.set_subagents(subagents);
    stored.save(&state_dir).unwrap();
    let reloaded = TestSession::load(session_id, &state_dir).unwrap();
    assert_eq!(
        SessionStateView::fold(
            &common::normalized_history_epoch_state(reloaded.state()),
            reloaded.agent_revisions(),
            reloaded.sequence_high_water().unwrap(),
        )
        .unwrap(),
        folded
    );
    let fs = Arc::new(InMemoryFs::new());
    for (path, bytes) in files {
        fs.seed(&path, bytes);
    }
    let host = RestartHost::open_with_provider(
        dir.path(),
        session_id,
        fs,
        vec![PLUGIN.into()],
        provider.clone(),
    );
    assert!(!host.manager.is_live(child_id));
    command(
        &host,
        json!({ "action": "check_authority", "path": path, "model": CHILD_MODEL, "yolo": yolo }),
    );
    assert!(!host.manager.is_live(child_id));
    assert_eq!(provider.request_count(), 2);
    assert_eq!(
        host.root.template.permissions.snapshot().stored_rules(),
        rules
    );
    assert_eq!(host.root.template.permissions.persisted_yolo(), Some(yolo));
    let tool = ToolKey::native(PERMISSION_TOOL);
    assert!(matches!(
        host.root
            .template
            .permissions
            .check(&tool, ALLOWED_SCOPE, None),
        PermissionCheck::Allowed
    ));
    assert!(matches!(
        host.root
            .template
            .permissions
            .check(&tool, DENIED_SCOPE, None),
        PermissionCheck::Denied
    ));
    for enabled in [!yolo, yolo] {
        command(&host, json!({ "action": "set_yolo", "yolo": enabled }));
        assert_eq!(
            host.root.template.permissions.persisted_yolo(),
            Some(enabled)
        );
        let snapshot = host.manager.journal().snapshot();
        let folded =
            SessionStateView::fold(&snapshot.state, &snapshot.agent_revisions, snapshot.seq)
                .unwrap();
        assert_eq!(folded.scopes[&None].yolo, Some(enabled));
    }
    command(
        &host,
        json!({ "action": "resume", "kind": "permissions", "path": path, "model": CHILD_MODEL }),
    );
    command(
        &host,
        json!({ "action": "check_authority", "path": path, "model": CHILD_MODEL, "yolo": yolo }),
    );
    let requests = provider.requests.lock().unwrap();
    assert_eq!(requests.len(), 4);
    assert!(requests[2].opts.fast);
    assert_eq!(
        requests[2].opts.thinking,
        ThinkingConfig::Effort(Effort::High)
    );
    let observed = requests[3]
        .messages
        .iter()
        .rev()
        .flat_map(|message| message.content.iter().rev())
        .find_map(|block| {
            if let ContentBlock::ToolResult {
                content,
                is_error: false,
                ..
            } = block
            {
                serde_json::from_str::<Value>(content).ok()
            } else {
                None
            }
        })
        .expect("restored permission probe must return a successful JSON tool result");
    assert_eq!(observed, json!({ "allowed": true, "denied": true }));
}

#[test]
fn restart_child_with_blocked_saved_plan_falls_back_to_build_before_provider_request() {
    let dir = tempfile::tempdir().unwrap();
    let provider = RestartProvider::new(vec![
        common::canned_reply(REPLY),
        common::canned_tool_use("restart_echo", json!({})),
        common::canned_reply(REPLY),
    ]);
    let host = agent_host(dir.path(), provider.clone());
    let path = dir.path().join(SAVED_IDS);
    command(
        &host,
        json!({ "action": "create", "model": CHILD_MODEL, "path": path }),
    );
    let child_id = saved(&host, &path)["agent_id"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    let stored =
        TestSession::load(host.session.id, &StateDir::from_path(dir.path().into())).unwrap();
    let folded = stored.latest_state().unwrap();
    let config = folded.scopes[&Some(child_id)].config.as_ref().unwrap();
    assert_eq!(config.mode.id, "plan");
    let plan_path = config.mode.restrict_write_to.as_ref().unwrap();
    if plan_path.exists() {
        std::fs::remove_file(plan_path).unwrap();
    }
    std::fs::create_dir_all(plan_path).unwrap();
    let host = host.restart_host(dir.path());
    assert!(!host.manager.is_live(child_id));
    assert_eq!(provider.request_count(), 1);
    *provider.expected_config.lock().unwrap() = Some((
        StateDir::from_path(dir.path().into()),
        host.session.id.into(),
        child_id,
        "build".into(),
    ));
    command(
        &host,
        json!({ "action": "resume", "model": CHILD_MODEL, "mode": "build", "path": path, "cwd": dir.path() }),
    );
    let config = host
        .manager
        .lookup(child_id)
        .unwrap()
        .effective_config()
        .unwrap()
        .unwrap();
    assert_eq!(config.mode.id().key(), "build");
    let snapshot = host.manager.journal().snapshot();
    let folded =
        SessionStateView::fold(&snapshot.state, &snapshot.agent_revisions, snapshot.seq).unwrap();
    assert_eq!(
        folded.scopes[&Some(child_id)]
            .config
            .as_ref()
            .unwrap()
            .mode
            .id,
        "build"
    );
    assert!(
        config
            .mode_def
            .as_ref()
            .unwrap()
            .restrict_write_to
            .is_none()
    );
    let requests = provider.requests.lock().unwrap();
    assert_eq!(requests.len(), 3);
    assert!(
        requests[2]
            .messages
            .iter()
            .flat_map(|message| &message.content)
            .any(|block| {
                matches!(block, ContentBlock::ToolResult { content, is_error: false, .. }
            if serde_json::from_str::<Value>(content).is_ok_and(|echo| echo["mode"] == "build"))
            })
    );
}

#[test_case(false; "deleted_definition")]
#[test_case(true; "changed_definition")]
fn restart_custom_mode_keeps_saved_definition_instead_of_current_registry(changed: bool) {
    let dir = tempfile::tempdir().unwrap();
    let provider = RestartProvider::new(vec![
        common::canned_tool_use("restart_echo", json!({})),
        common::canned_reply(REPLY),
        common::canned_tool_use("restart_echo", json!({})),
        common::canned_reply(REPLY),
    ]);
    let host = agent_host(dir.path(), provider.clone());
    host.host.load_source("restart_mode", &format!(
        "assert(makima.api.mode.define({{ name = {CUSTOM_MODE:?}, label = '[SAVED AUDIT]', system_prompt = {SAVED_DIRECTIVE:?}, tools = {{ 'restart_echo' }} }}))"
    )).unwrap();
    let path = dir.path().join(SAVED_IDS);
    command(
        &host,
        json!({ "action": "create", "model": CHILD_MODEL, "mode": CUSTOM_MODE, "path": path }),
    );
    let child_id = saved(&host, &path)["agent_id"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    let before = host
        .manager
        .lookup(child_id)
        .unwrap()
        .effective_config()
        .unwrap()
        .unwrap()
        .mode_def
        .clone()
        .unwrap();
    let host = host.restart_host(dir.path());
    if changed {
        host.host.load_source("restart_mode", &format!(
            "assert(makima.api.mode.define({{ name = {CUSTOM_MODE:?}, label = '[CHANGED AUDIT]', system_prompt = {CHANGED_DIRECTIVE:?}, tools = {{ 'restart_extra' }} }}))"
        )).unwrap();
    }
    command(
        &host,
        json!({ "action": "resume", "model": CHILD_MODEL, "mode": CUSTOM_MODE, "path": path, "cwd": dir.path() }),
    );
    let after = host
        .manager
        .lookup(child_id)
        .unwrap()
        .effective_config()
        .unwrap()
        .unwrap()
        .mode_def
        .clone()
        .unwrap();
    assert_eq!(after.label, before.label);
    assert_eq!(after.system_prompt, before.system_prompt);
    assert_eq!(after.tools, before.tools);
    assert_eq!(after.restrict_write_to, before.restrict_write_to);
    let requests = provider.requests.lock().unwrap();
    assert_eq!(requests.len(), 4);
    assert!(requests[0].system.contains(SAVED_DIRECTIVE));
    assert_eq!(requests[2].system, requests[0].system);
    assert!(!requests[2].system.contains(CHANGED_DIRECTIVE));
    assert_eq!(requests[2].tools, requests[0].tools);
    assert_eq!(common::tool_names(&requests[2].tools), ["restart_echo"]);
}

#[test]
fn concurrent_public_lua_sends_reactivate_one_resident_and_keep_both_turns() {
    let dir = tempfile::tempdir().unwrap();
    let provider = RestartProvider::new(vec![
        common::canned_reply(REPLY),
        common::canned_reply(REPLY),
        common::canned_reply(REPLY),
    ]);
    let host = agent_host(dir.path(), provider.clone());
    let path = dir.path().join(SAVED_IDS);
    command(
        &host,
        json!({ "action": "create", "model": CHILD_MODEL, "path": path }),
    );
    let child_id = saved(&host, &path)["agent_id"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    let (entered, discovery) = flume::bounded(1);
    let (release, released) = flume::bounded(1);
    let calls = Arc::new(Mutex::new(0));
    let (provider_id, model_id) = Model::parse_spec(CHILD_MODEL).unwrap();
    let registry = Arc::new(ProviderRegistry::new(vec![RegisteredProvider {
        id: provider_id.into(),
        display_name: provider_id.into(),
        models: vec![ModelInfo::id_only(model_id.into())],
        backend: Arc::new(DiscoveredModels {
            models: Arc::new(Mutex::new(Ok(vec![ModelInfo::id_only(model_id.into())]))),
            calls: Arc::clone(&calls),
            discovery_gate: Some((entered, released)),
        }),
        auth: AuthSpec::default(),
        docs: None,
    }]));
    let host = host.restart_host_with_provider_registry(dir.path(), registry);
    assert!(!host.manager.is_live(child_id));
    let finished = run_command(
        &host.host.command_registry(),
        &format!(
            "{COMMAND} {}",
            json!({ "action": "concurrent", "path": path, "messages": CONCURRENT_MESSAGES })
        ),
        0,
        Some(host.root.clone()),
    );
    smol::block_on(common::bounded(discovery.recv_async())).unwrap();
    host.host.wait_for_worker_barrier_for_test();
    assert_eq!(*calls.lock().unwrap(), 1);
    assert_eq!(provider.request_count(), 1);
    release.send(()).unwrap();
    assert_eq!(
        smol::block_on(common::bounded(finished.recv_async())).unwrap(),
        Ok(())
    );
    assert_eq!(*calls.lock().unwrap(), 1);
    assert!(host.manager.is_live(child_id));
    assert_eq!(provider.request_count(), 3);
    let latest_turn = host
        .manager
        .actor(child_id)
        .unwrap()
        .latest_ticket()
        .unwrap()
        .turn_id();
    smol::block_on(common::bounded(
        host.manager
            .journal()
            .wait_terminal_conversion(child_id, latest_turn),
    ))
    .unwrap();
    let coordinator = SessionCoordinatorHandle::resolve(host.session.id).unwrap();
    smol::block_on(common::bounded(
        coordinator.persist_history(host.history.load_full().as_ref().clone()),
    ))
    .unwrap();
    let stored =
        TestSession::load(host.session.id, &StateDir::from_path(dir.path().into())).unwrap();
    assert_eq!(
        stored.latest_state().unwrap().scopes[&Some(child_id)]
            .results
            .len(),
        3
    );
    let requests = provider.requests.lock().unwrap();
    let history = serde_json::to_string(&requests[2].messages).unwrap();
    for message in CONCURRENT_MESSAGES {
        assert!(history.contains(message));
    }
}

#[test]
fn missing_mcp_restart_is_read_only_then_fresh_transport_executes_saved_tool() {
    let dir = tempfile::tempdir().unwrap();
    let provider = RestartProvider::new(vec![
        common::canned_tool_use(MCP_TOOL, json!({})),
        common::canned_reply(REPLY),
        common::canned_tool_use(MCP_TOOL, json!({})),
        common::canned_reply(REPLY),
    ]);
    let host = agent_host_with_mcp(
        dir.path(),
        provider.clone(),
        Some(stub_session_with_result(
            &[("server.lookup", MCP_DESCRIPTION)],
            MCP_OLD_REPLY,
        )),
    );
    let path = dir.path().join(SAVED_IDS);
    command(
        &host,
        json!({ "action": "create", "kind": "mcp", "model": CHILD_MODEL, "path": path }),
    );
    let host = host.restart_host_with_mcp(dir.path(), None);
    reject_without_mutation(&host, &provider, &path, MCP_UNAVAILABLE, false);
    reject_without_mutation(&host, &provider, &path, MCP_UNAVAILABLE, false);
    let host = host.restart_host_with_mcp(
        dir.path(),
        Some(stub_session_with_result(
            &[("server.lookup", "current MCP description")],
            MCP_FRESH_REPLY,
        )),
    );
    command(
        &host,
        json!({ "action": "resume", "kind": "mcp", "model": CHILD_MODEL, "path": path }),
    );
    let requests = provider.requests.lock().unwrap();
    assert_eq!(requests.len(), 4);
    assert_eq!(requests[2].tools, requests[0].tools);
    assert_eq!(requests[2].tools[0]["description"], MCP_DESCRIPTION);
    for (request, expected) in [
        (&requests[1], MCP_OLD_REPLY),
        (&requests[3], MCP_FRESH_REPLY),
    ] {
        assert!(
            request
                .messages
                .iter()
                .rev()
                .flat_map(|message| message.content.iter().rev())
                .find_map(|block| {
                    if let ContentBlock::ToolResult {
                        content,
                        is_error: false,
                        ..
                    } = block
                    {
                        Some(content)
                    } else {
                        None
                    }
                })
                .unwrap()
                .contains(expected)
        );
    }
}
