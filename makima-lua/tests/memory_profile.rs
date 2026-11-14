#![cfg(all(feature = "memory-profile", target_os = "linux"))]

mod common;

use makima_agent::actor::{
    ActorHistory, AdmissionPreparation, AgentContextSnapshot, graph_quiescence_barrier,
};
use makima_agent::agent::{PromptSlotSource, TurnSetup};
use makima_agent::manager::AgentResidency;
use makima_agent::session_checkpoint::SessionLogCheckpoint;
use makima_agent::session_coordinator::{SessionCheckpoint, SessionCheckpointKind};
use makima_agent::session_options::SessionOptions;
use makima_agent::tools::offload::OffloadStore;
use makima_agent::tools::{FileReadTracker, RequestTools, ToolContext, ToolRegistry};
use makima_agent::{
    ActorBackend, Agent, AgentEvent, AgentInput, AgentLimits, AgentManagerHandle, AgentMetadata,
    AgentParams, AgentRunParams, BackendResult, CancelMap, ControlWork, Envelope, EventSender,
    McpSession, ToolOutput, TurnContext, WorkKind,
};
use makima_interpreter::allocation_snapshot;
use makima_lua::orchestration::{AgentOrchestration, TrustedTarget};
use makima_lua::test_support::run_command;
use makima_lua::{PluginHost, ui_sink::HostUi};
use makima_providers::provider::{BoxFuture, Provider};
use makima_providers::{
    AgentError, ContentBlock, Invocation, Message, Model, ProviderEvent, RequestDelivery,
    RequestOptions, StreamResponse, TokenUsage,
};
use makima_storage::StateDir;
use makima_storage::checkpoint::{CheckpointRequest, CheckpointVersion, CheckpointWriter};
use makima_storage::id::{MakimaId, SessionRef};
use makima_storage::sessions::{SESSIONS_DIR, Session};
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::fs;
use std::path::Path;
use std::process::Command;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;
use test_case::test_case;

const MAX_CHILDREN: usize = 128;
const FIRST_SAMPLE: usize = 33;
const LARGE_BYTES: usize = 1024 * 1024;
const SMALL_BYTES: usize = 64;
const PROFILE_PLUGIN: &str = "resident_memory_profile";
const PROFILE_COMMAND: &str = "/resident_memory_profile";
const PROFILE_CHILD: &str = "MAKIMA_MEMORY_PROFILE_CHILD";
const MCP_TOOL: &str = "stub.profile";
const MCP_WIRE_TOOL: &str = "stub__profile";
const MCP_RESULT: &str = "deterministic profile transport response";
const PROFILE_SOURCE: &str = r#"
local sequence = 0
makima.api.register_command({
    name = "/resident_memory_profile", tui_only = false,
    description = "Run one retained resident child",
    handler = function(_, ctx)
        sequence = sequence + 1
        local child = assert(makima.agent.spawn(ctx, {
            inherit_provider = true, tools = PROFILE_MCP and {
                { name = "stub__profile", description = "Deterministic resident memory profile tool",
                  input_schema = { type = "object", properties = {} } },
            } or {}, mcp = PROFILE_MCP,
            auto_deliver = false, system = "memory profile", name = tostring(sequence),
        }))
        local ticket = assert(child:send(ctx, tostring(sequence), { when = "idle" }))
        local result = assert(ticket:wait(ctx))
        if result.status ~= "completed" then error(makima.json.encode(result)) end
    end,
})
"#;

struct GeneratedProvider {
    bytes: usize,
    sequence: AtomicUsize,
    mcp: bool,
    calls: AtomicUsize,
}

impl Provider for GeneratedProvider {
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
            self.calls.fetch_add(1, Ordering::Relaxed);
            let tool_result = messages
                .iter()
                .rev()
                .flat_map(|message| &message.content)
                .any(|block| matches!(block, ContentBlock::ToolResult { .. }));
            if self.mcp && tool_result {
                assert!(
                    messages
                        .iter()
                        .flat_map(|message| &message.content)
                        .any(|block| {
                            matches!(block, ContentBlock::ToolResult { content, is_error, .. }
                        if !is_error && content.contains(MCP_RESULT))
                        })
                );
            }
            if self.mcp && !tool_result {
                return Ok(common::canned_tool_use(MCP_WIRE_TOOL, json!({})));
            }
            let sequence = self.sequence.fetch_add(1, Ordering::Relaxed);
            let mut text = format!("{sequence:016x}:");
            text.extend(std::iter::repeat_n(
                char::from(b'a' + (sequence % 26) as u8),
                self.bytes - text.len(),
            ));
            Ok(common::canned_reply(&text))
        })
    }
}

struct Scenario {
    bytes: usize,
    retain: bool,
    mcp: bool,
    actor_only: bool,
}

struct ActorOnlyBackend {
    params: AgentParams,
    tools: RequestTools,
    mcp: Option<McpSession>,
    events: EventSender,
}

impl ActorOnlyBackend {
    fn new(context: &ToolContext, registry: &Arc<ToolRegistry>) -> Self {
        let mcp = context.mcp.as_ref().map(McpSession::fresh);
        let mut definitions = json!([]);
        if let Some(mcp) = &mcp {
            mcp.extend_tools(&mut definitions);
        }
        Self {
            tools: RequestTools::assembled(definitions, &context.config, &context.model),
            mcp,
            events: context.event_tx.clone(),
            params: AgentParams {
                agent_id: context.execution_agent_id.unwrap(),
                provider: Arc::clone(&context.provider),
                model: (*context.model).clone(),
                config: context.config.clone(),
                tool_output_lines: context.tool_output_lines,
                permissions: Arc::clone(&context.permissions),
                session_id: context.session_id.clone(),
                mailbox: None,
                timeouts: context.timeouts,
                file_tracker: FileReadTracker::fresh(),
                prompt_slots: Arc::clone(&context.prompt_slots),
                modes: Arc::clone(&context.modes),
                subagent_cancels: Arc::new(CancelMap::new()),
                registry: Arc::clone(registry),
                audience: context.audience,
                question_mode: context.question_mode,
                model_policy: Arc::clone(&context.model_policy),
                providers: Arc::clone(&context.providers),
                file_write_locks: Arc::clone(&context.file_write_locks),
                offload: context.offload.clone(),
                managed_turn: None,
            },
        }
    }
}

impl ActorBackend for ActorOnlyBackend {
    fn admission_preparation(&self) -> Option<AdmissionPreparation> {
        Some(
            TurnSetup {
                registry: Arc::clone(&self.params.registry),
                local_tools: Arc::default(),
                mcp: self.mcp.clone(),
                slots: PromptSlotSource::default(),
                modes: Arc::clone(&self.params.modes),
                instructions: false,
            }
            .preparation(),
        )
    }

    fn offload_store(&self) -> Option<Arc<OffloadStore>> {
        self.params.offload.clone()
    }

    fn run_turn<'a>(
        &'a mut self,
        mut history: ActorHistory<'a>,
        context: TurnContext,
        input: AgentInput,
        _: WorkKind,
    ) -> BoxFuture<'a, BackendResult> {
        Box::pin(async move {
            let events = self.events.clone();
            let mut agent = Agent::for_actor_turn(
                self.params.clone(),
                AgentRunParams {
                    history: history.reborrow(),
                    system: "memory profile".into(),
                    event_tx: events.clone(),
                    tools: self.tools.clone(),
                },
                &context,
                Default::default(),
            )
            .with_mcp(self.mcp.clone());
            let outcome = agent.run(context.turn_id.unwrap(), input).await;
            drop(agent);
            {
                let snapshot = history.snapshot();
                let _ = events.send(AgentEvent::SubagentHistory {
                    tool_use_id: context.agent_id.to_string(),
                    history_epoch: snapshot.epoch,
                    conversation_id: snapshot
                        .conversation
                        .as_ref()
                        .map(|conversation| Arc::clone(&conversation.id)),
                    messages: snapshot.messages.as_ref().clone(),
                });
            }
            BackendResult::EnteredRun(outcome)
        })
    }

    fn run_control<'a>(
        &'a mut self,
        _: ActorHistory<'a>,
        _: TurnContext,
        _: &'a ControlWork,
    ) -> BoxFuture<'a, BackendResult> {
        Box::pin(async { BackendResult::ControlDone })
    }

    fn run_compact<'a>(
        &'a mut self,
        _: ActorHistory<'a>,
        _: TurnContext,
        _: Option<&'a str>,
    ) -> BoxFuture<'a, BackendResult> {
        Box::pin(async { BackendResult::CompactDone })
    }
}

fn disk_bytes(path: &Path) -> u64 {
    fs::read_dir(path)
        .map(|entries| {
            entries
                .map(|entry| {
                    let path = entry.unwrap().path();
                    if path.is_dir() {
                        disk_bytes(&path)
                    } else {
                        path.metadata().unwrap().len()
                    }
                })
                .sum()
        })
        .unwrap_or(0)
}

fn sample(
    host: Option<&PluginHost>,
    path: &Path,
    children: usize,
    scenario: &Scenario,
    collect: bool,
) -> usize {
    let lua = host.map(|host| host.profile_memory(collect).unwrap());
    let allocation = allocation_snapshot();
    let smaps = fs::read_to_string("/proc/self/smaps_rollup").unwrap();
    let kb = |key: &str| {
        smaps
            .lines()
            .find_map(|line| {
                let mut fields = line.split_whitespace();
                (fields.next()? == key).then(|| fields.next().unwrap().parse::<u64>().unwrap())
            })
            .unwrap()
    };
    println!(
        "{}",
        json!({
            "children": children, "output_bytes": scenario.bytes, "projection_retained": scenario.retain,
            "full_gc": collect, "live_bytes": allocation.live_bytes,
            "peak_bytes_process_lifetime": allocation.peak_bytes, "lua_used_bytes_overlapping": lua,
            "rss_bytes": kb("Rss:") * 1024, "pss_bytes": kb("Pss:") * 1024,
            "disk_bytes": disk_bytes(path), "mcp": scenario.mcp,
            "mcp_transport": "deterministic_inprocess_stub_shared_warmed",
            "actor_only": scenario.actor_only, "delivery": "disabled_auto_deliver_false", "jit": host.is_some(),
            "history_assistant_payload_bytes_logical": children * scenario.bytes,
            "sealed_text_payload_bytes_logical": children * scenario.bytes,
            "payload_counts_are_overlapping_not_exclusive_allocation": true,
            "limits": "configured_129_active_128_children_129_retained",
            "revision": option_env!("MAKIMA_PROFILE_REVISION").unwrap_or("not_supplied"),
            "debug_assertions": cfg!(debug_assertions),
        })
    );
    allocation.live_bytes
}

fn retain_event(
    event: Envelope,
    projection: &mut BTreeMap<String, Vec<Message>>,
    retain: bool,
) -> bool {
    match event.event {
        AgentEvent::SubagentHistory {
            tool_use_id,
            messages,
            ..
        } if retain => {
            projection.insert(tool_use_id, messages);
            false
        }
        AgentEvent::TurnOutcome(_) => true,
        _ => false,
    }
}

#[test_case(SMALL_BYTES, false, false, false; "small_discard_projection")]
#[test_case(SMALL_BYTES, true, false, false; "small_retain_projection")]
#[test_case(LARGE_BYTES, false, false, false; "large_discard_projection")]
#[test_case(LARGE_BYTES, true, false, false; "large_retain_projection")]
#[test_case(SMALL_BYTES, false, true, false; "small_mcp_discard_projection")]
#[test_case(SMALL_BYTES, true, true, false; "small_mcp_retain_projection")]
#[test_case(LARGE_BYTES, false, true, false; "large_mcp_discard_projection")]
#[test_case(LARGE_BYTES, true, true, false; "large_mcp_retain_projection")]
#[test_case(SMALL_BYTES, false, false, true; "small_actor_only")]
#[test_case(LARGE_BYTES, false, false, true; "large_actor_only")]
#[test_case(SMALL_BYTES, false, true, true; "small_actor_only_mcp")]
#[test_case(LARGE_BYTES, false, true, true; "large_actor_only_mcp")]
#[test_case(SMALL_BYTES, true, false, true; "small_actor_only_retain_projection")]
#[test_case(LARGE_BYTES, true, false, true; "large_actor_only_retain_projection")]
#[test_case(SMALL_BYTES, true, true, true; "small_actor_only_mcp_retain_projection")]
#[test_case(LARGE_BYTES, true, true, true; "large_actor_only_mcp_retain_projection")]
#[ignore = "process-isolated resident memory measurements"]
fn resident_agents_memory_profile(bytes: usize, retain: bool, mcp: bool, actor_only: bool) {
    let scenario = Scenario {
        bytes,
        retain,
        mcp,
        actor_only,
    };
    let process_key = format!("{bytes}:{retain}:{mcp}:{actor_only}");
    if std::env::var(PROFILE_CHILD).ok().as_deref() != Some(process_key.as_str()) {
        let name = std::thread::current().name().unwrap().to_owned();
        let status = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                &name,
                "--ignored",
                "--nocapture",
                "--test-threads=1",
            ])
            .env(PROFILE_CHILD, process_key)
            .status()
            .unwrap();
        assert!(status.success(), "profiling child process failed: {status}");
        return;
    }
    smol::block_on(async {
        let directory = tempfile::tempdir().unwrap();
        let storage = StateDir::from_path(directory.path().into());
        let registry = Arc::new(ToolRegistry::new());
        let mut host = (!actor_only).then(|| {
            let host = PluginHost::with_command_registry(
                Arc::clone(&registry),
                Default::default(),
                true,
                HostUi::Frontend,
            )
            .unwrap();
            host.load_source(
                PROFILE_PLUGIN,
                &format!("local PROFILE_MCP = {mcp}\n{PROFILE_SOURCE}"),
            )
            .unwrap();
            host
        });
        let provider = Arc::new(GeneratedProvider {
            bytes,
            sequence: AtomicUsize::new(0),
            mcp,
            calls: AtomicUsize::new(0),
        });
        let (mut context, events, _cancel) = common::ctx_with_provider(Arc::clone(&provider));
        if mcp {
            let session = makima_agent::mcp::test_support::stub_session_with_result(
                &[(MCP_TOOL, "Deterministic resident memory profile tool")],
                MCP_RESULT,
            );
            assert!(
                session
                    .call_tool(MCP_TOOL, &json!({}))
                    .await
                    .unwrap()
                    .contains(MCP_RESULT)
            );
            context.mcp = Some(session);
        }
        let session_id = MakimaId::generate();
        context.session_id = Some(session_id.into());
        context.cwd = directory.path().into();
        context.offload = Some(Arc::new(OffloadStore::for_session(
            directory.path(),
            &SessionRef::from(session_id),
        )));
        let manager = AgentManagerHandle::new(AgentLimits {
            max_children_per_agent: MAX_CHILDREN,
            max_active_agents: MAX_CHILDREN + 1,
            max_retained_agents: MAX_CHILDREN + 1,
            ..Default::default()
        })
        .unwrap();
        let root = manager
            .create_root_with_config(
                Some(
                    common::effective_config(&context)
                        .with_context(AgentContextSnapshot::new(context.cwd.clone())),
                ),
                Vec::new(),
                None,
                |_| Ok::<_, String>(Box::new(common::IdleRoot) as Box<dyn ActorBackend>),
            )
            .unwrap();
        context.execution_agent_id = Some(root.id());
        let mut template = common::with_request_tools(&registry, &context);
        if let Some(mcp) = &template.mcp {
            let mut definitions = json!([]);
            mcp.extend_tools(&mut definitions);
            let request = RequestTools::assembled(definitions, &template.config, &template.model);
            template.tool_filter = Arc::clone(request.filter());
            template.request_tools = Some(request);
        }
        let target = TrustedTarget {
            target: root.clone(),
            template,
        };
        if let Some(host) = &host {
            host.event_handle().install_orchestration_services(Arc::new(
                AgentOrchestration::bind_root(Arc::default(), target.clone()),
            ));
            host.wait_for_worker_barrier_for_test();
        }
        let mut seed = Session::<Message, TokenUsage, ToolOutput>::new(
            &context.model.spec(),
            directory.path().to_str().unwrap(),
        );
        seed.id = session_id;
        manager.journal().snapshot().apply_to(&mut seed).unwrap();
        let writer = SessionLogCheckpoint::open_with_session(storage.clone(), seed, None).unwrap();
        writer.bind_journal(manager.journal()).await.unwrap();
        let mut projection = BTreeMap::new();
        let mut full_gc_live = Vec::new();
        for children in 0..=MAX_CHILDREN {
            if children != 0 {
                if let Some(host) = &host {
                    let command = run_command(
                        &host.command_registry(),
                        PROFILE_COMMAND,
                        0,
                        Some(target.clone()),
                    );
                    common::bounded(command.recv_async())
                        .await
                        .unwrap()
                        .unwrap();
                } else {
                    let child = manager
                        .spawn_child_trusted_with_config(
                            &root,
                            common::effective_config(&context)
                                .with_context(AgentContextSnapshot::new(context.cwd.clone())),
                            AgentMetadata {
                                label: Some(children.to_string()),
                                transcript_key: Some(format!("profile-{children}")),
                                ..Default::default()
                            },
                            Vec::new(),
                            None,
                            |_| {
                                Ok::<_, String>(
                                    Box::new(ActorOnlyBackend::new(&context, &registry))
                                        as Box<dyn ActorBackend>,
                                )
                            },
                        )
                        .unwrap();
                    let ticket = child
                        .actor()
                        .unwrap()
                        .admit_turn(
                            AgentInput::new(children.to_string(), Vec::new()),
                            Some(context.event_tx.clone()),
                            PROFILE_PLUGIN.into(),
                        )
                        .unwrap();
                    assert!(matches!(
                        common::bounded(ticket.wait()).await,
                        makima_agent::TurnOutcome::Completed { .. }
                    ));
                }
                let mut terminal = false;
                let mut history = false;
                while !terminal || !history {
                    let event = common::bounded(events.recv_async()).await.unwrap();
                    history |= matches!(event.event, AgentEvent::SubagentHistory { .. });
                    terminal |= retain_event(event, &mut projection, retain);
                }
                common::bounded(manager.wait_settled()).await;
                let actors = manager
                    .list()
                    .iter()
                    .map(|agent| agent.actor().unwrap())
                    .collect::<Vec<_>>();
                common::bounded(graph_quiescence_barrier(&actors))
                    .await
                    .unwrap();
                for agent in manager.list() {
                    if agent.id() == root.id() {
                        continue;
                    }
                    let ticket = agent.actor().unwrap().latest_ticket().unwrap();
                    common::bounded(
                        manager
                            .journal()
                            .wait_terminal_conversion(agent.id(), ticket.turn_id()),
                    )
                    .await
                    .unwrap();
                }
                if let Some(host) = &host {
                    host.drain_tasks_for_test();
                }
                while let Ok(event) = events.try_recv() {
                    retain_event(event, &mut projection, retain);
                }
            }
            if [0, FIRST_SAMPLE, MAX_CHILDREN].contains(&children) {
                let options = SessionOptions::new(Vec::new(), &Default::default()).unwrap();
                writer
                    .checkpoint(CheckpointRequest {
                        session_id,
                        version: CheckpointVersion {
                            revision: children as u64 + 1,
                            epoch: 0,
                        },
                        snapshot: Arc::new(SessionCheckpoint {
                            kind: SessionCheckpointKind::Options,
                            history_revision: None,
                            history: None,
                            options: options.snapshot(),
                            config: None,
                            journal: None,
                        }),
                    })
                    .await
                    .unwrap();
                writer.drain().await.unwrap();
                assert_eq!(manager.list().len(), children + 1);
                assert!(manager.snapshot().iter().all(|node| {
                    node.residency == AgentResidency::Resident
                        && node.actor.as_ref().is_some_and(|actor| actor.queued == 0)
                }));
                assert_eq!(
                    manager.journal().published_revision(),
                    manager.journal().snapshot().revision
                );
                sample(
                    host.as_ref(),
                    &directory.path().join(SESSIONS_DIR),
                    children,
                    &scenario,
                    false,
                );
                full_gc_live.push(sample(
                    host.as_ref(),
                    &directory.path().join(SESSIONS_DIR),
                    children,
                    &scenario,
                    true,
                ));
            }
        }
        println!(
            "{}",
            json!({
                "output_bytes": bytes, "projection_retained": retain, "mcp": mcp, "actor_only": actor_only,
                "delta_33_bytes": full_gc_live[1] as i128 - full_gc_live[0] as i128,
                "delta_128_bytes": full_gc_live[2] as i128 - full_gc_live[0] as i128,
                "slope_bytes_per_child": (full_gc_live[2] as f64 - full_gc_live[1] as f64) / (MAX_CHILDREN - FIRST_SAMPLE) as f64,
            })
        );
        let saved = Session::<Message, TokenUsage, ToolOutput>::load(session_id, &storage).unwrap();
        assert_eq!(saved.latest_state().unwrap().agents.len(), MAX_CHILDREN + 1);
        drop(saved);
        assert_eq!(provider.sequence.load(Ordering::Relaxed), MAX_CHILDREN);
        assert_eq!(
            provider.calls.load(Ordering::Relaxed),
            MAX_CHILDREN * if mcp { 2 } else { 1 }
        );
        if let Some(host) = &mut host {
            host.begin_shutdown();
        }
        assert!(
            common::bounded(manager.shutdown(Duration::from_secs(10)))
                .await
                .timed_out
                .is_empty()
        );
    });
}
