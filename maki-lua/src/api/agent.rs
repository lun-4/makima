//! `maki.agent` exposes subagent primitives to Lua plugins. Policy (retries,
//! validation, concurrency) lives in the task plugin, not here.

use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::pin::pin;
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::{Duration, Instant};

mod handles;
mod structured_output;
use structured_output::StructuredOutput;

pub(crate) use handles::{
    AGENT_DOCS, LuaAgent, LuaAgentRef, REF_DOCS, SUBSCRIPTION_DOCS, TURN_DOCS, agent_defer_close,
    current, focused_agent_ref,
};
use handles::{
    current__doc, current__register, get__doc, get__register, list__doc, list__register, root__doc,
    root__register, spawn__doc, spawn__register,
};

use async_lock::Mutex as AsyncMutex;
use futures::future::{Either, select};
use maki_agent::actor::{
    ActorBackend, ActorLifecycle, ActorStatus, AdmissionPolicyConstraint, BackendResult,
    EffectiveAgentConfig, TurnContext, WorkKind,
};
use maki_agent::agent::{
    Instructions, ResolvedPromptInputs, TurnPromptInputs, build_mode_system_prompt, tool_dispatch,
};
use maki_agent::cancel::{CancelMap, CancelSlot, CancelToken};
use maki_agent::template::env_vars_for;
use maki_agent::tools::interpreter_bridge;
use maki_agent::tools::offload::OffloadStore;
use maki_agent::tools::registry::ToolRegistry;
use maki_agent::tools::schema::sanitize_tool_input_schema;
use maki_agent::tools::{
    CallOrigin, Deadline, DescriptionContext, FileReadTracker, LocalTool, LocalTools,
    PermissionScopes, RequestTools, ToolAudience, ToolContext, ToolFilter, ToolLive,
    TurnToolBindings,
};
use maki_agent::{
    Agent, AgentActorHandle, AgentEvent, AgentId, AgentInput, AgentMode, AgentParams,
    AgentRunParams, CancelTrigger, CurrentManagedTurn, Envelope, EventSender, History, McpSession,
    ModeDef, Post, RunLedger, RunSettings, SubagentCancel, SubagentInfo, ToolDoneEvent,
    TurnAdmissionOptions, TurnCancellationReason, TurnFailure, TurnFailureKind, TurnId, TurnOrigin,
    TurnOutcome, TurnProvenance, TurnResult,
};
use maki_config::{ModelPolicy, ToolKey};
use maki_lua_macro::{lua_class, lua_fn, lua_table};
use maki_providers::model::ModelTier;
use maki_providers::provider;
use maki_providers::{
    Message, Model, ModelError, ModelSelection, RequestOptions, TokenUsage, add_cost,
};
use maki_storage::id::MakiId;
use mlua::{Function, IntoLuaMulti, Lua, Result as LuaResult, Table, Value as LuaValue};
use serde_json::Value as JsonValue;
use tracing::{info, warn};

use crate::agent_errors::{
    BLANK_POST, INVOCATION_EXPIRED, MANAGED_AUTHORITY_INACTIVE, NO_PLUGIN_AUTHORITY,
    NO_TASK_AUTHORITY, ORCHESTRATION_SERVICES_EXPIRED, POST_TARGET_NOT_STARTED, POST_TARGET_SILENT,
    SHOW_SILENT_AGENT, SHOW_TOOL_AGENT, SHOW_UNTRUSTED_PARENT, STRUCTURED_OUTPUT_CONFLICT,
};
use crate::api::r#async::LuaSemaphore;
use crate::api::tool::{audiences_to_lua, parse_audience};
use crate::api::ui::buf::BufHandle;
use crate::api::util::convert::{json_to_lua, lua_to_json, lua_tool_result};
use crate::api::util::ctx::{AgentContext, LuaCtx};
use crate::api::util::pair::{Pair, err_pair, try_pair};
use crate::host_root::HostRootServices;
use crate::orchestration::TrustedContext;
use crate::runtime::CANCELLED_MSG;

const MANAGED_POLICY_MISSING_ERR: &str = "managed agent turn is missing its admitted configuration";
const SESSION_CLOSED_ERR: &str = "session closed";
const SESSION_POLICY_CHANGED_ERR: &str = "session creation policy changed";
const DEFAULT_SESSION_AUDIENCE: ToolAudience = ToolAudience::GENERAL_SUB;
const UNNAMED_AGENT_LABEL: &str = "agent";
const SPAWN_OPTIONS: &[&str] = &[
    "audience",
    "auto_deliver",
    "inherit_provider",
    "local_tools",
    "mcp",
    "model",
    "name",
    "output_schema",
    "semaphore",
    "show",
    "silent",
    "system",
    "tools",
];

pub type SessionProviderPreparer =
    Arc<dyn Fn(Model) -> Result<(Model, Arc<dyn provider::Provider>), String> + Send + Sync>;

/// Build the `(model, provider)` pair that backs a `maki.agent.session` spawn.
/// `inherit_provider` (or an absent `model.spec`) reuses the parent model and
/// provider so a test-side wrapper can drive a real spawned session through a
/// caller-provided provider without network. Otherwise build a provider for the
/// given `model.spec`.
async fn build_session_provider(
    model_spec: &Option<String>,
    inherit_provider: bool,
    agent_ctx: &AgentContext,
    prepare: Option<SessionProviderPreparer>,
) -> Result<(Model, Arc<dyn provider::Provider>), String> {
    if inherit_provider || model_spec.is_none() {
        Ok((
            Model::clone(&agent_ctx.model),
            Arc::clone(&agent_ctx.provider),
        ))
    } else {
        let spec = model_spec.as_deref().expect("model_spec present");
        let mut m = Model::from_spec_with_policy(spec, &agent_ctx.model_policy)
            .map_err(|e| e.to_string())?;
        if let Some(prepare) = prepare {
            return prepare(m);
        }
        let p = provider::from_model_async(&mut m, agent_ctx.timeouts)
            .await
            .map_err(|e| e.to_string())?;
        Ok((m, Arc::from(p)))
    }
}

#[derive(Default)]
struct StandaloneChildren {
    closed: bool,
    actors: Vec<(Weak<AgentActorHandle>, Weak<LuaActorState>)>,
}

type StandaloneOwner = (Weak<CancelMap<String>>, Arc<Mutex<StandaloneChildren>>);

#[derive(Default)]
struct StandaloneActors(Vec<StandaloneOwner>);

/// Shared backend state, owned by the adapter and cloned into the actor's
/// backend. Runs are transient: the [`actor::Runner`](maki_agent::actor) owns
/// the [`History`], while every per-session capability that must outlive a
/// run (MCP, answer channels, UI identity, cancel wiring) lives here.
///
/// Writes happen inside [the `ActorBackend::run_turn` future]
/// (`LuaActorBackend`), which can only borrow `&mut self`, so every field is
/// a `Mutex`/`Sender`/`OnceLock` the borrowed shell can touch. `prompt` and
/// `status` never read through this struct; they speak to the actor handle.
struct LuaActorState {
    children: Arc<Mutex<StandaloneChildren>>,
    template: OnceLock<ToolContext>,
    params: OnceLock<AgentParams>,
    custom_system: String,
    tools: RequestTools,
    opts: RequestOptions,
    mode: AgentMode,
    mode_def: Option<Arc<maki_agent::ModeDef>>,
    mcp: Option<McpSession>,
    chip_event_tx: EventSender,
    child_cancel: CancelToken,
    answer_rx: Arc<AsyncMutex<flume::Receiver<String>>>,
    answer_tx: Option<flume::Sender<String>>,
    ui_id: String,
    parent_event_tx: EventSender,
    input_tx: flume::Sender<String>,
    cancel: SubagentCancel,
    parent_agent_id: Option<AgentId>,
    parent_is_root: bool,
    auto_deliver: bool,
    silent: bool,
    subagent_info: Arc<OnceLock<SubagentInfo>>,
    local_tools: LocalTools,
    structured: Option<Arc<StructuredOutput>>,
    name: String,
    commit: Arc<Mutex<Option<JsonValue>>>,
    semaphore: Option<Arc<async_lock::Semaphore>>,
    /// The `semaphore` permit held by the running turn, released for the
    /// length of a blocking wait from that turn.
    turn_permit: Mutex<Option<async_lock::SemaphoreGuardArc>>,
    start: Instant,
    /// Set only after the adapter backend has actually entered a turn. A
    /// queued admission never sets it, so close cannot mistake queued work for
    /// an executed transcript.
    execution_started: std::sync::atomic::AtomicBool,
    /// Prevents close/drop from duplicating the latest terminal relay.
    history_relayed: std::sync::atomic::AtomicBool,
    close_notified: std::sync::atomic::AtomicBool,
    relay_snapshot: Mutex<Vec<Message>>,
}

impl LuaActorState {
    /// Releases the running turn's permit, so the children a blocking wait
    /// observes can take it. Returns whether the turn held one.
    fn release_turn_permit(&self) -> bool {
        self.turn_permit.lock().unwrap().take().is_some()
    }

    async fn reacquire_turn_permit(&self, cancel: &CancelToken) -> Result<(), String> {
        let Some(semaphore) = &self.semaphore else {
            return Ok(());
        };
        let permit = cancel.race(Arc::clone(semaphore).acquire_arc()).await?;
        *self.turn_permit.lock().unwrap() = Some(permit);
        Ok(())
    }

    fn init_subagent_info(&self, first_message: &str) {
        if self.subagent_info.get().is_none() {
            let _ = self.subagent_info.set(SubagentInfo {
                agent_id: self
                    .params
                    .get()
                    .expect("session parameters must be initialized before admission")
                    .agent_id,
                parent_agent_id: self.parent_agent_id,
                parent_is_root: self.parent_is_root,
                auto_deliver: self.auto_deliver
                    && self
                        .template
                        .get()
                        .is_some_and(|template| template.managed_turn.is_none())
                    && self.parent_agent_id.is_none(),
                parent_tool_use_id: self.ui_id.clone(),
                name: self.name.clone(),
                prompt: Some(first_message.to_owned()),
                model: Some(
                    self.params
                        .get()
                        .expect("session parameters must be initialized before admission")
                        .model
                        .spec(),
                ),
                opts: Some(self.opts),
                answer_tx: self.answer_tx.clone(),
                input_tx: Some(self.input_tx.clone()),
                cancel: Some(self.cancel.clone()),
            });
        }
    }

    fn relay_history(&self, history: &History) {
        self.execution_started
            .store(true, std::sync::atomic::Ordering::Release);
        self.history_relayed
            .store(false, std::sync::atomic::Ordering::Release);
        *self.relay_snapshot.lock().unwrap() = history.as_slice().to_vec();
        self.relay_snapshot_if_pending();
    }

    /// Close-time fallback: emit an existing transcript exactly once, but
    /// never an empty snapshot or one belonging to queued-only work.
    fn relay_snapshot_if_pending(&self) {
        if self.silent {
            return;
        }
        if self
            .history_relayed
            .swap(true, std::sync::atomic::Ordering::SeqCst)
        {
            return;
        }
        let messages = self.relay_snapshot.lock().unwrap().clone();
        if messages.is_empty() {
            self.history_relayed
                .store(false, std::sync::atomic::Ordering::SeqCst);
            return;
        }
        let event = AgentEvent::SubagentHistory {
            tool_use_id: self.ui_id.clone(),
            messages,
        };
        let _ = match self.subagent_info.get() {
            Some(subagent) => self.parent_event_tx.send_envelope(Envelope {
                event,
                subagent: Some(subagent.clone()),
                run_id: self.parent_event_tx.run_id(),
            }),
            None => self.parent_event_tx.send(event),
        };
    }

    fn take_close_notification(&self) -> Option<Envelope> {
        let subagent = self.subagent_info.get()?;
        if self.silent
            || self
                .close_notified
                .swap(true, std::sync::atomic::Ordering::AcqRel)
        {
            return None;
        }
        Some(Envelope {
            event: AgentEvent::SubagentClosed,
            subagent: Some(subagent.clone()),
            run_id: self.parent_event_tx.run_id(),
        })
    }

    fn notify_closed_direct(&self) {
        if let Some(envelope) = self.take_close_notification() {
            let _ = self.parent_event_tx.send_envelope(envelope);
        }
    }

    fn notify_closed_ordered(&self) {
        if let Some(envelope) = self.take_close_notification() {
            let _ = self.chip_event_tx.send_envelope(envelope);
        }
    }

    fn cancel_subtree(&self, actor: &AgentActorHandle) {
        let children = self.children.lock().unwrap();
        actor.cancel_existing();
        for (actor, state) in &children.actors {
            if let (Some(actor), Some(state)) = (actor.upgrade(), state.upgrade()) {
                state.cancel_subtree(&actor);
            }
        }
    }

    /// Idempotent cancellation close: notify the UI immediately, retire the
    /// subtree, and resolve queued/parked waiters as Closed.
    fn close_with(&self, actor: &AgentActorHandle) {
        self.notify_closed_direct();
        let mut children = self.children.lock().unwrap();
        children.closed = true;
        actor.close();
        for (actor, state) in children.actors.drain(..) {
            if let (Some(actor), Some(state)) = (actor.upgrade(), state.upgrade()) {
                state.close_with(&actor);
            }
        }
        drop(children);
        if !self
            .execution_started
            .load(std::sync::atomic::Ordering::Acquire)
        {
            self.relay_snapshot_if_pending();
        }
    }
}

/// The actor backend for one Lua session. Thin `&mut self` shell over the
/// shared [`LuaActorState`] so the boxed `run_turn` future stays `Send`.
struct LuaActorBackend {
    state: Arc<LuaActorState>,
    offload: Option<Arc<OffloadStore>>,
}

impl LuaActorBackend {
    fn new(state: Arc<LuaActorState>) -> Self {
        Self {
            state,
            offload: None,
        }
    }
}

impl Drop for LuaActorBackend {
    fn drop(&mut self) {
        self.state.notify_closed_direct();
    }
}

impl ActorBackend for LuaActorBackend {
    fn offload_store(&self) -> Option<Arc<OffloadStore>> {
        self.offload.clone().or_else(|| {
            self.state
                .params
                .get()
                .and_then(|params| params.offload.clone())
        })
    }

    fn admission_preparation(&self) -> Option<maki_agent::actor::AdmissionPreparation> {
        let state = Arc::clone(&self.state);
        Some(Arc::new(move |_input, mode, config| {
            let params = state
                .params
                .get()
                .expect("session parameters initialized before admission");
            let mode_def = if let Some(config) = config {
                config.mode_def.clone().map(Arc::new)
            } else if *mode == state.mode {
                state.mode_def.clone().or_else(|| match mode {
                    AgentMode::Custom(id) => params.modes.get(id).map(Arc::new),
                    _ => Some(Arc::new(params.modes.current(mode))),
                })
            } else {
                None
            };
            maki_agent::agent::TurnAdmissionSnapshot {
                mode_def,
                prompt_inputs: Some(Arc::new(TurnPromptInputs {
                    cwd: state
                        .template
                        .get()
                        .map(|template| template.cwd.clone())
                        .unwrap_or_else(|| ".".into()),
                    instructions: Instructions::default(),
                    mcp_prompt: None,
                    resolved: None,
                    ready: Some(ResolvedPromptInputs {
                        slots: Arc::clone(&params.prompt_slots),
                        slots_error: None,
                        mcp_messages: None,
                    }),
                })),
                bindings: Arc::new(TurnToolBindings::capture(
                    &params.registry,
                    &state.local_tools,
                    state.mcp.as_ref(),
                )),
                mcp_startup_notice: None,
            }
        }))
    }

    fn run_turn<'a>(
        &'a mut self,
        history: &'a mut History,
        context: TurnContext,
        mut input: AgentInput,
        _work: WorkKind,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = BackendResult> + Send + 'a>> {
        Box::pin(async move {
            let state = &*self.state;
            state
                .execution_started
                .store(true, std::sync::atomic::Ordering::Release);
            // The chip sender drives the parent relay: it is the only handle
            // keeping `sub_tx` alive, and `Agent::run` emits through it.
            let event_tx = state
                .chip_event_tx
                .clone()
                .with_output(context.output.clone());
            let turn_id = context.turn_id.unwrap_or_else(TurnId::generate);
            if context.managed_turn.is_some() && context.policy.is_none() {
                return BackendResult::EnteredRun(TurnOutcome::failed(
                    context.agent_id,
                    turn_id,
                    TokenUsage::default(),
                    0,
                    TurnFailure {
                        kind: TurnFailureKind::Internal,
                        diagnostic: MANAGED_POLICY_MISSING_ERR.into(),
                        user_message: MANAGED_POLICY_MISSING_ERR.into(),
                        retryable: false,
                    },
                ));
            }
            state.commit.lock().unwrap().take();
            if let Some(structured) = &state.structured {
                structured.begin_turn(context.output.clone());
            }
            state.init_subagent_info(&input.message);
            info!(
                agent_id = %context.agent_id,
                %turn_id,
                "lua actor turn"
            );
            // The semaphore gates the turn, racing close/shutdown so a blocked
            // acquire cannot strand after the actor closes. The permit is
            // dropped on every path; a race loss yields an EnteredRun Cancelled
            // (the outcome truth), never a dead actor.
            let permit = match &state.semaphore {
                Some(semaphore) => {
                    let semaphore = Arc::clone(semaphore);
                    let cancel = context.cancel.clone();
                    match cancel.race(semaphore.acquire_arc()).await {
                        Ok(permit) => Some(permit),
                        Err(_) => {
                            let cancelled = TurnOutcome::cancelled(
                                context.agent_id,
                                turn_id,
                                TokenUsage::default(),
                                0,
                                context
                                    .cancel_reason
                                    .reason()
                                    .unwrap_or(TurnCancellationReason::User),
                            );
                            state.relay_history(history);
                            return BackendResult::EnteredRun(cancelled);
                        }
                    }
                }
                None => None,
            };
            *state.turn_permit.lock().unwrap() = permit;

            let mut params = state
                .params
                .get()
                .expect("session parameters must be initialized before admission")
                .clone();
            if let Some(config) = &context.policy {
                params.provider = Arc::clone(&config.provider);
                params.model = config.model.clone();
                input.mode = config.mode.clone();
                input.thinking = config.thinking;
                input.fast = config.fast;
                input.workflow = config.workflow;
            }
            params.agent_id = context.agent_id;
            params.managed_turn = context.managed_turn.clone();
            let mut system = context
                .admission
                .as_ref()
                .and_then(|admission| {
                    let definition = admission.mode_def.as_deref()?;
                    let inputs = admission.prompt_inputs.as_ref()?;
                    Some(build_mode_system_prompt(
                        &env_vars_for(&inputs.cwd),
                        definition,
                        &input.mode,
                    ))
                })
                .unwrap_or_default();
            if !state.custom_system.is_empty() {
                if !system.is_empty() {
                    system.push_str("\n\n");
                }
                system.push_str(&state.custom_system);
            }
            let mut agent = Agent::new(
                params,
                AgentRunParams {
                    history,
                    system,
                    event_tx,
                    tools: state.tools.clone(),
                },
            )
            .with_user_response_rx(Arc::clone(&state.answer_rx))
            .with_cancel(context.cancel.clone())
            .with_turn_inbox(context.inbox)
            .with_cancel_reason_source(context.cancel_reason.clone())
            .with_mcp(state.mcp.clone())
            .with_local_tools(Arc::clone(&state.local_tools))
            .with_local_tool_capture(Arc::clone(&state.commit))
            .with_admission(context.admission.clone());
            if let Some(interrupt) = context.interrupt {
                agent = agent.with_interrupt_source(interrupt);
            }
            let commit = Arc::clone(&state.commit);
            let structured = state.structured.clone();
            agent = agent.with_instruction_consumed_callback(Arc::new(move || {
                commit.lock().unwrap().take();
                if let Some(structured) = &structured {
                    structured.invalidate();
                }
            }));
            if let Some(structured) = &state.structured {
                let check = Arc::clone(structured);
                let exhausted = Arc::clone(structured);
                agent = agent
                    .with_completion_check(Arc::new(move || check.completion_check()))
                    .with_completion_exhausted(Arc::new(move || exhausted.completion_exhausted()));
            }
            let outcome = agent.run(turn_id, input).await;
            drop(agent);
            state.turn_permit.lock().unwrap().take();

            if let Some(structured) = &state.structured {
                structured.finish_turn();
            }
            let captured = state.commit.lock().unwrap().take();
            if state.structured.is_none()
                && let Some(value) = &captured
            {
                context.output.append_output(value.clone());
            }
            state.relay_history(history);
            BackendResult::EnteredRun(outcome)
        })
    }

    fn run_control<'a>(
        &'a mut self,
        _history: &'a mut History,
        _context: TurnContext,
        control: &'a maki_agent::ControlWork,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = BackendResult> + Send + 'a>> {
        // Lua sessions admit turns only; standalone controls never reach here.
        Box::pin(async move {
            warn!(control = %control.name, "unexpected control for Lua session backend");
            BackendResult::ControlFailed
        })
    }

    fn run_compact<'a>(
        &'a mut self,
        _history: &'a mut History,
        _context: TurnContext,
        _instructions: Option<&'a str>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = BackendResult> + Send + 'a>> {
        // Lua sessions admit turns only; compacts are a TUI-run concern.
        Box::pin(async move {
            warn!("unexpected compact for Lua session backend");
            BackendResult::CompactDone
        })
    }
}

fn resolve_model_from_ctx(ctx: &AgentContext, tier: Option<&str>) -> Result<Model, String> {
    let Some(tier_str) = tier else {
        return Ok(Model::clone(&ctx.model));
    };
    let requested: ModelTier = tier_str.parse().map_err(|e: ModelError| e.to_string())?;
    let effective = requested.capped_at(ctx.model.tier);
    if effective == ctx.model.tier {
        return Ok(Model::clone(&ctx.model));
    }
    let slug = &ctx.model.provider;
    maki_providers::model_registry::spec_for_tier(slug, effective)
        .or_else(|| maki_providers::model_registry::spec_for_tier_any(effective))
        .filter(|spec| ctx.model_policy.allows(spec))
        .and_then(|s| Model::from_spec(&s).ok())
        .map(Ok)
        .unwrap_or_else(|| {
            Model::from_tier_with_policy(slug, effective, &ctx.model_policy)
                .map_err(|e| e.to_string())
        })
}

fn model_to_lua_table(lua: &Lua, model: &Model) -> LuaResult<Table> {
    let tbl = lua.create_table()?;
    tbl.set("id", model.id.clone())?;
    tbl.set("tier", model.tier.to_string())?;
    tbl.set("provider", model.provider.to_string())?;
    tbl.set("spec", model.spec())?;
    Ok(tbl)
}

fn dispatch_ctx<'a>(lua: &Lua, ctx: &'a LuaCtx, method: &str) -> Result<&'a AgentContext, String> {
    ctx.validate_origin(lua)?;
    ctx.agent()
        .ok_or_else(|| ctx.cap_err(&format!("maki.agent.{method}")))
}

/// Forwards subagent events to the parent, stamped with the subagent identity.
async fn relay_session_events(
    sub_rx: flume::Receiver<Envelope>,
    parent_tx: EventSender,
    subagent_info: Arc<OnceLock<SubagentInfo>>,
    live_sink: Option<flume::Sender<ToolLive>>,
    silent: bool,
    relay_gate: Option<flume::Receiver<()>>,
) {
    if let Some(gate) = relay_gate {
        let _ = gate.recv_async().await;
    }
    let mut cost = None;
    while let Ok(mut envelope) = sub_rx.recv_async().await {
        if silent {
            continue;
        }
        match &envelope.event {
            AgentEvent::TurnComplete(turn) => {
                add_cost(&mut cost, turn.cost);
                if let Some(sink) = &live_sink {
                    let _ = sink.send(ToolLive::Usage(turn.usage.format_sum_cost(cost)));
                }
            }
            AgentEvent::ToolOutput { .. }
            | AgentEvent::ToolPending { .. }
            | AgentEvent::ToolExecutionStart { .. } => continue,
            AgentEvent::SubagentHistory { .. } if envelope.subagent.is_none() => continue,
            _ => {}
        }
        if envelope.subagent.is_none() {
            envelope.subagent = subagent_info.get().cloned();
        }
        let _ = parent_tx.send_envelope(envelope);
    }
}

/// Look up the model that the current agent is using, or pick a cheaper one.
/// You might want a cheaper model for simple subtasks (summaries, classification)
/// without hard-coding a model name.
///
/// The returned table has fields: `id` (string), `tier` (string),
/// `provider` (string), `spec` (string).
///
/// @param ctx LuaCtx Agent context.
/// @param opts table? Optional fields:
///   `tier` (string?) - target tier, e.g. `"fast"`, `"mid"`, `"best"`. Clamped to
///     the parent tier so you cannot escalate.
///   `spec` (string?) - exact model spec string, e.g. `"claude-3-5-haiku-20241022"`.
///     Takes precedence over `tier`.
/// @return (table?, string?) Model table on success, or `(nil, err)` on failure.
/// @example
/// local model, err = maki.agent.resolve_model(ctx, { tier = "fast" })
/// if err then error(err) end
/// print(model.spec, model.tier)
#[lua_fn]
async fn resolve_model(
    lua: Lua,
    ctx: mlua::UserDataRef<LuaCtx>,
    opts: Option<Table>,
) -> LuaResult<Pair<Table>> {
    let agent = try_pair!(dispatch_ctx(&lua, &ctx, "resolve_model"));
    let tier_str = opts
        .as_ref()
        .and_then(|t| t.get::<Option<String>>("tier").ok().flatten());
    let spec_str = opts
        .as_ref()
        .and_then(|t| t.get::<Option<String>>("spec").ok().flatten());

    let model = match spec_str {
        Some(ref spec) => try_pair!(Model::from_spec_with_policy(spec, &agent.model_policy)),
        None => try_pair!(resolve_model_from_ctx(agent, tier_str.as_deref())),
    };
    Ok((Some(model_to_lua_table(&lua, &model)?), None))
}

/// Build a system prompt from a built-in template. Environment variables like
/// `{cwd}` are substituted automatically. Use this when you need a ready-made
/// prompt for a subagent session.
///
/// @param ctx LuaCtx Agent context.
/// @param opts table Required fields:
///   `prompt_id` (string) - one of `"research"`, `"general"`, `"system"`.
/// Optional fields:
///   `instructions` (string|boolean?) - extra text appended to the prompt.
///     `true` loads instructions from the project `.makima/instructions` file.
///     `false` or nil omits them.
/// @return (string?, string?) The assembled prompt string, or `(nil, err)` on failure.
/// @example
/// local prompt, err = maki.agent.system_prompt(ctx, {
///   prompt_id = "research",
///   instructions = true,
/// })
/// if err then error(err) end
#[lua_fn]
async fn system_prompt(
    lua: Lua,
    ctx: mlua::UserDataRef<LuaCtx>,
    opts: Table,
) -> LuaResult<Pair<String>> {
    let slots = Arc::clone(&try_pair!(dispatch_ctx(&lua, &ctx, "system_prompt")).prompt_slots);
    let invocation = try_pair!(InvocationGuard::capture(&lua));
    // Nothing may hold the ctx borrow across the wait: a cancel hook firing
    // meanwhile needs `ctx:finish`, which takes it mutably.
    drop(ctx);
    let prompt_id_str: String = opts.get("prompt_id")?;
    let prompt_id = match prompt_id_str.as_str() {
        "research" => maki_agent::prompt::PromptId::Research,
        "general" => maki_agent::prompt::PromptId::General,
        "system" => maki_agent::prompt::PromptId::System,
        other => return Ok(err_pair(format!("unknown prompt_id: {other}"))),
    };

    let vars = maki_agent::template::env_vars();
    let instructions_val: LuaValue = opts.get("instructions")?;
    let instructions = match instructions_val {
        LuaValue::Boolean(true) => {
            let cwd = vars.apply("{cwd}").into_owned();
            smol::unblock(move || maki_agent::agent::load_instruction_text(&cwd)).await
        }
        LuaValue::Boolean(false) | LuaValue::Nil => String::new(),
        LuaValue::String(s) => s.to_str()?.to_owned(),
        _ => return Err(mlua::Error::runtime("instructions must be bool or string")),
    };

    try_pair!(invocation.validate(&lua));
    let assembled = maki_agent::prompt::assemble(prompt_id, &slots, &instructions);
    Ok((Some(vars.apply(&assembled).into_owned()), None))
}

/// Get the list of tool definitions for a given audience. Pass the result
/// straight into `maki.agent.session()` or use it to inspect what tools are
/// available.
///
/// @param ctx LuaCtx Agent context.
/// @param opts table Required fields:
///   `audience` (string) - tool audience filter, e.g. `"general"`, `"subagent"`,
///     `"general_sub"`.
/// Optional fields:
///   `only` (string[]?) - include only these tool names.
///   `except` (string[]?) - exclude these tool names.
///   `workflow` (boolean?) - use workflow-mode descriptions. Default: `false`.
///   `spec` (string?) - evaluate capability exclusions against this model spec.
/// @return (table?, string?) Array of tool definition tables, or `(nil, err)` on failure.
/// @example
/// local defs, err = maki.agent.tools(ctx, {
///   audience = "general_sub",
///   except = { "bash", "write" },
/// })
/// if err then error(err) end
/// print(#defs .. " tools available")
#[lua_fn]
async fn tools(lua: Lua, ctx: mlua::UserDataRef<LuaCtx>, opts: Table) -> LuaResult<Pair<LuaValue>> {
    let agent = try_pair!(dispatch_ctx(&lua, &ctx, "tools"));
    let audience_str: String = opts.get("audience")?;
    let audience = try_pair!(
        ToolAudience::parse_name(&audience_str)
            .ok_or_else(|| format!("unknown audience: {audience_str}"))
    );

    let only: Option<Vec<String>> = opts.get("only")?;
    let except: Option<Vec<String>> = opts.get("except")?;
    let workflow: bool = opts.get::<Option<bool>>("workflow")?.unwrap_or(false);
    let spec_str: Option<String> = opts.get("spec")?;
    let mcp_enabled: bool = opts.get::<Option<bool>>("mcp")?.unwrap_or(true);

    let parsed = spec_str
        .as_deref()
        .and_then(|spec| Model::from_spec_with_policy(spec, &agent.model_policy).ok());
    let model = parsed.as_ref().unwrap_or(&agent.model);

    let base = match (only, except) {
        (Some(o), _) => ToolFilter::Only(o),
        (_, Some(e)) => ToolFilter::AllExcept(e),
        _ => ToolFilter::All,
    };
    let disabled: Vec<&str> = agent
        .config
        .disabled_tools
        .iter()
        .map(String::as_str)
        .collect();
    let filter = base
        .excluding(&disabled)
        .excluding(maki_agent::tools::capability_exclusions(model));

    let vars = maki_agent::template::env_vars();
    let ctx_desc = DescriptionContext {
        filter: &filter,
        audience,
        workflow,
        mcp: mcp_enabled && agent.mcp.is_some(),
    };
    // Base definitions only: the session injects MCP definitions per
    // request, so baking them into a tools array would freeze the catalog.
    let defs = ToolRegistry::global().definitions(&vars, &ctx_desc, model.supports_tool_examples());

    Ok((Some(json_to_lua(&lua, &defs)?), None))
}

/// Every tool name this context can dispatch: registry tools, MCP tools
/// (deferred ones included), host tools (ACP client tools, a subagent's
/// `structured_output`) and `tool_search`. Reach for it when you expose tools
/// inside a sandbox and need the names to bind. `maki.api.get_tools()` covers
/// the registry alone and has no view of the session.
///
/// The list already accounts for this session's audience, the config's
/// `disabled_tools` and the model's capabilities. Read `audiences` to layer
/// your own policy on top. A sandbox wants `interpreter`.
///
/// Each name shows up once, described by the tool a call would really reach, so
/// a host tool that shadows a registry name reports its own audience rather
/// than the shadowed one's.
///
/// @param ctx LuaCtx Agent context.
/// @return (table?, string?) Array of `{ name, alias?, source, audiences, schema? }`,
///   or `(nil, err)` on failure. `source` is one of `"native"`, `"local"`,
///   `"mcp"`. `alias` is a safe identifier to bind, set only when `name` is not
///   one (say `srv__get-docs`). Dispatch `name` in every case. `schema` comes
///   with registry tools only.
/// @example
/// local tools, err = maki.agent.callable_tools(ctx)
/// if err then error(err) end
/// for _, t in ipairs(tools) do
///   print(t.source, t.alias or t.name)
/// end
#[lua_fn]
async fn callable_tools(lua: Lua, ctx: mlua::UserDataRef<LuaCtx>) -> LuaResult<Pair<Table>> {
    let agent = try_pair!(dispatch_ctx(&lua, &ctx, "callable_tools"));
    let out = lua.create_table()?;
    for (i, tool) in tool_dispatch::callable(&agent.to_tool_context())
        .into_iter()
        .enumerate()
    {
        let t = lua.create_table()?;
        t.set("name", tool.name)?;
        if let Some(alias) = tool.alias {
            t.set("alias", alias)?;
        }
        t.set("source", tool.source)?;
        t.set("audiences", audiences_to_lua(&lua, tool.audience)?)?;
        if let Some(schema) = tool.schema {
            t.set("schema", json_to_lua(&lua, &schema)?)?;
        }
        out.set(i + 1, t)?;
    }
    Ok((Some(out), None))
}

/// Run a tool by name and wait for the result. This is how you call built-in
/// tools (like `read`, `bash`, `glob`) from Lua without going through the LLM.
///
/// Live events (streaming output, annotations, cumulative usage) are delivered
/// through optional callbacks while the tool runs.
///
/// @param ctx LuaCtx Agent context.
/// @param name string Tool name, e.g. `"bash"`, `"read"`.
/// @param input table|any Tool input (JSON-serializable). Must match the tool's `input_schema`.
/// @param opts table? Optional fields:
///   `timeout` (integer?) - deadline in seconds.
///   `on_live_buf` (function?) - called with a `BufHandle` for each live buffer
///     the tool publishes. Must not yield.
///   `on_annotation` (function?) - called with an annotation string for each
///     annotation event. Must not yield.
///   `on_usage` (function?) - called with a formatted cumulative token usage
///     string. Must not yield.
/// @return (string?, string?, any) Tool output text, or `(nil, err)` on
///   failure. The third value is the tool's `state` (see `register_tool`),
///   for callers that hand it back to the tool's `restore` later.
/// @example
/// local out, err = maki.agent.call_tool(ctx, "bash", {
///   command = "ls -la",
///   timeout = 10,
/// })
/// if err then error(err) end
/// print(out)
#[lua_fn]
async fn call_tool(
    lua: Lua,
    ctx: mlua::UserDataRef<LuaCtx>,
    name: String,
    input: LuaValue,
    opts: Option<Table>,
) -> LuaResult<(Option<String>, Option<String>, LuaValue)> {
    if let Err(error) = ctx.validate_origin(&lua) {
        return Ok((None, Some(error), LuaValue::Nil));
    }
    let invocation = match InvocationGuard::capture(&lua) {
        Ok(invocation) => invocation,
        Err(error) => return Ok((None, Some(error), LuaValue::Nil)),
    };
    let input_json = lua_to_json(&lua, &input)?;
    let agent = match dispatch_ctx(&lua, &ctx, "call_tool") {
        Ok(a) => a,
        Err(e) => return Ok((None, Some(e.to_string()), LuaValue::Nil)),
    };
    let mut tctx = agent.to_tool_context();
    let (mut on_buf, mut on_ann, mut on_usage, mut rx) = (None, None, None, None);
    if let Some(o) = opts {
        if let Some(secs) = o.get::<Option<u64>>("timeout")? {
            tctx.deadline = tctx
                .deadline
                .min(Deadline::after(Duration::from_secs(secs)));
        }
        on_buf = o.get::<Option<Function>>("on_live_buf")?;
        on_ann = o.get::<Option<Function>>("on_annotation")?;
        on_usage = o.get::<Option<Function>>("on_usage")?;
        if on_buf.is_some() || on_ann.is_some() || on_usage.is_some() {
            let (tx, r) = flume::unbounded();
            tctx.live_sink = Some(tx);
            rx = Some(r);
        }
    }
    drop(ctx);
    if let Err(e) = tctx.deadline.check() {
        return Ok((None, Some(e.to_string()), LuaValue::Nil));
    }
    let cbs = LiveCallbacks {
        tool: &name,
        authority: Some((&lua, &invocation)),
        on_buf,
        on_ann,
        on_usage,
    };
    let done = dispatch_racing_live(&tctx, &name, &input_json, rx, &cbs).await;
    if let Err(error) = invocation.validate(&lua) {
        return Ok((None, Some(error), LuaValue::Nil));
    }
    // Same fallback the UI applies on tool completion, so a batch child's
    // header carries the annotation its standalone run would get.
    let annotation = done
        .annotation
        .clone()
        .or_else(|| (!done.is_error).then(|| done.output.annotation()).flatten());
    if let Some(a) = annotation {
        cbs.deliver(ToolLive::Annotation(a)).await;
    }
    let state = match done.output.state() {
        Some(v) => json_to_lua(&lua, v)?,
        None => LuaValue::Nil,
    };
    match interpreter_bridge::flatten(&done) {
        Ok(text) => Ok((Some(text), None, state)),
        Err(err) => Ok((None, Some(err), state)),
    }
}

/// Run the normal interactive permission prompt for a tool call, without
/// dispatching the tool. This is how a plugin escalates a classifier deny or
/// failure to the same allow/deny/remember choices a user would see with
/// automode off.
/// Allow and deny rules apply exactly as they do for a plain tool call.
///
/// @param ctx LuaCtx Agent context.
/// @param opts table Required fields:
///   `tool` (string) - tool key to prompt for, e.g. `"bash"`.
///   `scopes` (string[]) - permission scopes to prompt for.
/// Optional fields:
///   `force_prompt` (boolean?) - prompt even for scopes an allow rule claims.
/// @return (boolean?, string?) `true` when the call is allowed, or
///   `(nil, err)` with the permission-denied message when denied.
/// @example
/// local ok, err = maki.agent.permission_prompt(ctx, {
///   tool = "bash",
///   scopes = { "echo hello" },
///   force_prompt = false,
/// })
/// if not ok then return { llm_output = err, is_error = true } end
#[lua_fn]
async fn permission_prompt(
    lua: Lua,
    ctx: mlua::UserDataRef<LuaCtx>,
    opts: Table,
) -> LuaResult<Pair<bool>> {
    let agent = try_pair!(dispatch_ctx(&lua, &ctx, "permission_prompt"));
    let invocation = try_pair!(InvocationGuard::capture(&lua));
    let tool: String = opts.get("tool")?;
    if tool.is_empty() || tool.contains('.') {
        return Err(mlua::Error::runtime(format!(
            "permission_prompt: tool must be a native tool name, got {tool:?}"
        )));
    }
    let scopes: Vec<String> = opts.get("scopes")?;
    let force_prompt: bool = opts.get::<Option<bool>>("force_prompt")?.unwrap_or(false);
    // Clone the ToolContext and drop the ctx borrow before awaiting: the
    // prompt wait must not hold the LuaCtx userdata's shared lock, or a
    // cancel hook firing meanwhile would deadlock on `ctx:finish`. A plain
    // clone (not `to_tool_context`, which clears it) keeps `tool_use_id` so
    // the TUI/ACP binds the prompt to the outer tool call.
    let tctx: ToolContext = (**agent).clone();
    drop(ctx);
    let tool_key = ToolKey::native(&tool);
    let perm_scopes = PermissionScopes {
        scopes,
        force_prompt,
    };
    let result = tctx
        .permissions
        .enforce(
            &tool_key,
            &perm_scopes,
            &tctx.event_tx,
            tctx.user_response_rx.as_deref(),
            tctx.tool_use_id.as_deref().unwrap_or(""),
            &tctx.cancel,
            tctx.restrict_write_to().as_deref(),
        )
        .await;
    try_pair!(invocation.validate(&lua));
    match result {
        Ok(()) => Ok((Some(true), None)),
        Err(e) => Ok((None, Some(e.to_string()))),
    }
}

/// Whether this session is in YOLO mode, where permission prompts are skipped
/// and the bash automode classifier's deny or failure is final (no escalation).
///
/// YOLO toggles at runtime (`/yolo`, `--yolo`), so query it per call rather
/// than caching it.
///
/// @param ctx LuaCtx Agent context.
/// @return (boolean) `true` when YOLO mode is active.
#[lua_fn]
async fn is_yolo(lua: Lua, ctx: mlua::UserDataRef<LuaCtx>) -> LuaResult<Pair<bool>> {
    let agent = try_pair!(dispatch_ctx(&lua, &ctx, "is_yolo"));
    Ok((Some(agent.permissions.is_yolo()), None))
}

/// Create a new subagent session. The session inherits the parent model and
/// MCP handle unless you override them. You get back a `Session` object that
/// you can send messages to with `:prompt()`.
///
/// Trusted sessions remain usable by same-plugin handlers until host/plugin
/// revocation. Tool-created sessions expire when their creating invocation ends.
/// Passing a Session to another coroutine does not transfer authority.
///
/// This is the main way to spin up a sub-conversation with its own history
/// and tool set.
///
/// @param ctx LuaCtx Agent context.
/// @param opts table Optional fields:
///   `model` (string|table?) - a spec string or a `{spec, thinking, fast}`
///     selection, validated like `Agent:set_model`. Omitted fields inherit the
///     parent's; thinking is capped at the parent's level. A query result from
///     `Agent:model` can be passed as is.
///   `system` (string?) - custom text appended after the admitted mode's prompt contribution. Defaults to empty; does not inherit the parent's full system prompt.
///   `tools` (table?) - tool definitions array (from `maki.agent.tools()`).
///   `local_tools` (table?) - map of `name -> spec` for Lua-backed tools. Each spec
///     requires `description` (string), `input_schema` (table), and
///     `handler` (function). The handler receives the input table and must return
///     `(string)` or `(nil, err)`. Set `capture_input` to surface a successful
///     call's input as `captured` in the turn result.
///   `name` (string?) - display name for logs and UI.
///   `audience` (string?) - tool audience for capability gating. Default: `"general_sub"`.
///   `mcp` (boolean?) - give the session access to MCP tools. Their
///     definitions are injected automatically each turn (deferred behind
///     `tool_search`), so don't put MCP definitions in `tools`. The session
///     starts with no loaded tools of its own. Default: `true`.
///   `inherit_provider` (boolean?) - reuse the parent model and provider instead
///     of building one from `model.spec`. Used by tests (and tools that want to
///     thread a caller-provided provider) to drive a real spawned session without
///     hitting the network. `model.spec` is ignored when this is `true`. Default: `false`.
///   `silent` (boolean?) - do not relay the session's turns, annotations, or
///     usage into the parent session's UI or event stream. The session still
///     completes and `:prompt()` still returns its result (including a commit
///     set via a `local_tools` handler). Use for hidden one-shot classification.
///   `show` (boolean|string?) - post a row for the session in its parent's
///     chat; a string adds a description. Default: `true` for trusted parents,
///     `false` otherwise. Silent sessions and sessions opened by a tool call,
///     which already have its row, cannot be shown.
///   `auto_deliver` (boolean?) - for managed children, queue a structured
///     lifecycle observation per settled turn to the recorded parent, independently
///     of `prompt()` or ticket waits. Nested children notify their own parent,
///     and a managed child's turn does not settle while a child reporting to it
///     is still running, unless its request budget runs out. Silent children
///     disable delivery. Rejection and discard are observable in
///     host logs; delivery is in-memory, without retry. Default: `true`.
///   `semaphore` (maki.async.Semaphore?) - concurrency limit acquired by the
///     driver immediately before each unmanaged turn and released when that turn
///     ends. Managed sessions ignore it and use the parent manager's limit.
/// Any other option key is an error.
/// @return (Session?, string?) Session handle, or `(nil, err)` on failure.
/// @example
/// local tools = maki.agent.tools(ctx, { audience = "general_sub" })
/// local sess, err = maki.agent.session(ctx, {
///   system = "You are a research assistant.",
///   tools = tools,
///   name = "researcher",
/// })
/// if err then error(err) end
/// local result = sess:prompt("Summarize this file.")
/// sess:close()
#[lua_fn]
async fn session(
    lua: Lua,
    ctx: mlua::UserDataRef<LuaCtx>,
    opts: Table,
) -> LuaResult<Pair<mlua::AnyUserData>> {
    let (session, error) = build_session(lua.clone(), ctx, opts, false).await?;
    match session {
        Some(session) => Ok((Some(lua.create_userdata(session)?), error)),
        None => Ok((None, error)),
    }
}

fn request_tools_json(lua: &Lua, value: &LuaValue) -> LuaResult<JsonValue> {
    if let LuaValue::Table(table) = value
        && table.is_empty()
    {
        return Ok(JsonValue::Array(Vec::new()));
    }
    let definitions = lua_to_json(lua, value)?;
    if !definitions.is_array() {
        return Err(mlua::Error::runtime("tools must be an array"));
    }
    Ok(definitions)
}

/// Who a new session answers to: the parent agent context it inherits from and
/// the authority that must still hold after every suspension point.
struct SessionParent {
    context: AgentContext,
    trusted: Option<TrustedContext>,
    managed_turn: Option<CurrentManagedTurn>,
    authority: SessionAuthority,
    agent_id: Option<AgentId>,
    is_root: bool,
}

impl SessionParent {
    fn resolve(lua: &Lua, ctx: &LuaCtx) -> Result<Self, String> {
        ctx.validate_origin(lua)?;
        let trusted = ctx.trusted(lua).ok().cloned();
        let context = Self::agent_context(lua, ctx, trusted.as_ref())?;
        let authority = SessionAuthority::capture(lua, trusted.as_ref(), &context)?;
        let managed_turn = match (
            &context.managed_turn,
            crate::runtime::current_managed_turn(lua),
        ) {
            (Some(expected), Some(active)) if expected.same_authority(&active) => Some(active),
            (None, None) => None,
            _ => return Err(MANAGED_AUTHORITY_INACTIVE.into()),
        };
        let agent_id = managed_turn
            .as_ref()
            .map(|current| current.agent_id())
            .or_else(|| trusted.as_ref().map(|trusted| trusted.target.id()));
        let is_root = managed_turn
            .as_ref()
            .and_then(|current| current.node_snapshot().ok())
            .is_some_and(|node| node.parent_id.is_none());
        Ok(Self {
            context,
            trusted,
            managed_turn,
            authority,
            agent_id,
            is_root,
        })
    }

    fn agent_context(
        lua: &Lua,
        ctx: &LuaCtx,
        trusted: Option<&TrustedContext>,
    ) -> Result<AgentContext, String> {
        if let Some(trusted) = trusted {
            let mut template = trusted.template.clone();
            if let Some(config) = trusted
                .target
                .effective_config()
                .map_err(|error| error.to_string())?
            {
                template.apply_effective_config(&config);
            }
            return Ok(AgentContext::from(&template));
        }
        if let Ok(host) = ctx.trusted_host_root(lua) {
            let template = host
                .template
                .as_ref()
                .ok_or("host root spawn template is unavailable")?;
            return Ok(AgentContext::from(template));
        }
        dispatch_ctx(lua, ctx, "session").cloned()
    }

    fn restricts_writes(&self) -> bool {
        self.context.restrict_write_to().is_some()
    }

    fn revalidate(&self, lua: &Lua, ctx: &LuaCtx) -> Result<(), String> {
        ctx.validate_origin(lua)?;
        if let Some(current) = &self.managed_turn {
            current
                .validate_active()
                .map_err(|error| error.to_string())?;
        }
        if let Some(trusted) = &self.trusted {
            trusted.validate_target(lua, &trusted.target)?;
        }
        self.authority.validate(lua, None)
    }

    /// The parent's callable tool context, with turn bindings captured fresh
    /// for trusted parents, and the names it may call.
    fn callable_tools(&self) -> (ToolContext, HashSet<String>) {
        let mut context = self.context.to_tool_context();
        if self.trusted.is_some() {
            context.turn_bindings = Arc::new(TurnToolBindings::capture(
                &context.registry,
                &context.local_tools,
                context.mcp.as_ref(),
            ));
        }
        let allowed = tool_dispatch::callable(&context)
            .into_iter()
            .map(|tool| tool.name)
            .collect();
        (context, allowed)
    }
}

struct SessionOptions {
    inherit_defaults: bool,
    selection: ModelSelection,
    system: Option<String>,
    tools: Option<LuaValue>,
    local_tools: Option<Table>,
    name: String,
    audience: ToolAudience,
    inherit_provider: bool,
    fast: bool,
    mcp: bool,
    silent: bool,
    auto_deliver: bool,
    semaphore: Option<Arc<async_lock::Semaphore>>,
    /// `Some` posts a row for the child in its parent's chat, with an
    /// optional description.
    show: Option<Option<String>>,
}

impl SessionOptions {
    fn parse(
        lua: &Lua,
        opts: &Table,
        parent: &SessionParent,
        inherit_defaults: bool,
    ) -> LuaResult<Result<Self, String>> {
        for pair in opts.pairs::<LuaValue, LuaValue>() {
            let (key, _) = pair?;
            let known =
                matches!(&key, LuaValue::String(name) if SPAWN_OPTIONS.contains(&&*name.to_str()?));
            if !known {
                return Ok(Err(format!(
                    "{}: {}",
                    crate::agent_errors::UNKNOWN_SPAWN_OPTION,
                    key.to_string()?
                )));
            }
        }
        let selection = match opts.get::<Option<LuaValue>>("model")? {
            Some(value) => match crate::api::util::selection::selection_from_lua(lua, value) {
                Ok(selection) => selection,
                Err(error) => return Ok(Err(error)),
            },
            None => Default::default(),
        };
        let system = opts.get("system")?;
        let tools = opts.get("tools")?;
        let local_tools = opts.get("local_tools")?;
        let name: Option<String> = opts.get("name")?;
        let audience = match opts.get::<Option<String>>("audience")? {
            Some(s) => match ToolAudience::parse_name(&s) {
                Some(audience) => audience,
                None => return Ok(Err(format!("unknown audience: {s}"))),
            },
            None if inherit_defaults => parent.context.audience,
            None => DEFAULT_SESSION_AUDIENCE,
        };
        let inherit_provider = opts
            .get::<Option<bool>>("inherit_provider")?
            .unwrap_or(false);
        let fast = selection.fast.unwrap_or(parent.context.opts.fast);
        let mcp = opts.get::<Option<bool>>("mcp")?.unwrap_or(true) && !parent.restricts_writes();
        let silent = opts.get::<Option<bool>>("silent")?.unwrap_or(false);
        let auto_deliver = opts.get::<Option<bool>>("auto_deliver")?.unwrap_or(true);
        let showable = if silent {
            Err(SHOW_SILENT_AGENT)
        } else if parent.context.tool_use_id.is_some() {
            Err(SHOW_TOOL_AGENT)
        } else if parent.trusted.is_none() {
            Err(SHOW_UNTRUSTED_PARENT)
        } else {
            Ok(())
        };
        let show = match opts.get::<LuaValue>("show")? {
            LuaValue::Nil => showable.is_ok().then_some(None),
            LuaValue::Boolean(false) => None,
            LuaValue::Boolean(true) => match showable {
                Ok(()) => Some(None),
                Err(error) => return Ok(Err(error.into())),
            },
            LuaValue::String(description) => {
                if let Err(error) = showable {
                    return Ok(Err(error.into()));
                }
                let description = description.to_str()?.to_owned();
                if description.trim().is_empty() {
                    return Ok(Err(BLANK_POST.into()));
                }
                Some(Some(description))
            }
            other => {
                return Err(mlua::Error::runtime(format!(
                    "show must be a boolean or string, got {}",
                    other.type_name()
                )));
            }
        };
        let semaphore = if parent.managed_turn.is_some() {
            None
        } else {
            opts.get::<Option<mlua::AnyUserData>>("semaphore")?
                .map(|value| {
                    value
                        .borrow::<LuaSemaphore>()
                        .map(|semaphore| Arc::clone(&semaphore.sem))
                })
                .transpose()?
        };
        Ok(Ok(Self {
            inherit_defaults,
            selection,
            system,
            tools,
            local_tools,
            name: name.unwrap_or_default(),
            audience,
            inherit_provider,
            fast,
            mcp,
            silent,
            auto_deliver,
            semaphore,
            show,
        }))
    }

    fn workflow(&self, parent: &SessionParent) -> bool {
        self.inherit_defaults && parent.context.workflow
    }

    fn inherits_parent_mode(&self, parent: &SessionParent) -> bool {
        self.inherit_defaults
            || parent.restricts_writes()
            || matches!(&parent.context.mode, AgentMode::Custom(_))
    }
}

fn validate_required_output_mode(
    required: bool,
    definition: Option<&ModeDef>,
) -> Result<(), String> {
    if required
        && definition
            .and_then(|definition| definition.tools.as_ref())
            .is_some_and(|tools| {
                !tools
                    .iter()
                    .any(|name| name == structured_output::TOOL_NAME)
            })
    {
        return Err(format!(
            "output_schema requires tool '{}' but the selected mode excludes it",
            structured_output::TOOL_NAME,
        ));
    }
    Ok(())
}

struct SessionTools {
    request: RequestTools,
    local: HashMap<String, LocalTool>,
    structured: Option<Arc<StructuredOutput>>,
}

impl SessionTools {
    fn assemble(
        lua: &Lua,
        opts: &Table,
        parent: &SessionParent,
        options: &SessionOptions,
        model: &Model,
    ) -> LuaResult<Result<Self, String>> {
        let (parent_tools, parent_allowed) = parent.callable_tools();
        let mut definitions = match &options.tools {
            Some(value) => request_tools_json(lua, value)?,
            None if options.inherit_defaults => {
                match inherited_tool_definitions(&parent.context, model, &parent_allowed) {
                    Ok(definitions) => definitions,
                    Err(error) => return Ok(Err(error)),
                }
            }
            None => JsonValue::Array(vec![]),
        };
        let structured = match opts.get::<Option<LuaValue>>("output_schema")? {
            Some(value) => match StructuredOutput::compile(lua_to_json(lua, &value)?) {
                Ok(structured) => Some(structured),
                Err(error) => return Ok(Err(error)),
            },
            None => None,
        };
        let mut local = if options.inherit_defaults && options.tools.is_none() {
            inherited_local_tools(&parent.context, &definitions)
        } else {
            HashMap::new()
        };
        if let Some(table) = &options.local_tools
            && let Err(error) = add_lua_local_tools(lua, table, &mut definitions, &mut local)?
        {
            return Ok(Err(error));
        }
        if let Some(structured) = &structured {
            if local.contains_key(structured_output::TOOL_NAME)
                || definition_names(&definitions).any(|name| name == structured_output::TOOL_NAME)
            {
                return Ok(Err(STRUCTURED_OUTPUT_CONFLICT.into()));
            }
            definitions
                .as_array_mut()
                .expect("tools checked")
                .push(structured.definition());
            local.insert(structured_output::TOOL_NAME.into(), structured.local_tool());
        }
        // The array is the caller's, and the filter comes out of it, so whatever
        // the caller left out is also a name this session cannot dispatch or bind
        // inside its sandbox.
        if options.inherits_parent_mode(parent) {
            let initial =
                RequestTools::assembled(definitions.clone(), &parent.context.config, model);
            let mut child_tools = parent_tools;
            child_tools.audience = options.audience;
            child_tools.tool_filter = Arc::clone(initial.filter());
            let within_parent = options.inherit_defaults || parent.restricts_writes();
            let mut allowed: HashSet<_> = tool_dispatch::callable(&child_tools)
                .into_iter()
                .map(|tool| tool.name)
                .filter(|name| !within_parent || parent_allowed.contains(name))
                .collect();
            allowed.extend(local.keys().cloned());
            definitions
                .as_array_mut()
                .expect("request tools are an array")
                .retain(|definition| {
                    definition_name(definition).is_some_and(|name| allowed.contains(name))
                });
        }
        Ok(Ok(Self {
            request: RequestTools::assembled(definitions, &parent.context.config, model),
            local,
            structured,
        }))
    }
}

fn definition_name(definition: &JsonValue) -> Option<&str> {
    definition.get("name").and_then(JsonValue::as_str)
}

fn definition_names(definitions: &JsonValue) -> impl Iterator<Item = &str> {
    definitions
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(definition_name)
}

fn inherited_tool_definitions(
    context: &AgentContext,
    model: &Model,
    parent_allowed: &HashSet<String>,
) -> Result<JsonValue, String> {
    let filter = context
        .tool_filter
        .as_ref()
        .clone()
        .excluding(maki_agent::tools::capability_exclusions(model));
    let mut definitions = context
        .request_tools
        .as_ref()
        .ok_or("parent request tools snapshot is unavailable")?
        .definitions()
        .clone();
    if let Some(definitions) = definitions.as_array_mut() {
        definitions.retain(|definition| {
            definition_name(definition).is_some_and(|name| {
                name != structured_output::TOOL_NAME
                    && parent_allowed.contains(name)
                    && filter.matches(name)
            })
        });
    }
    Ok(definitions)
}

fn inherited_local_tools(
    context: &AgentContext,
    definitions: &JsonValue,
) -> HashMap<String, LocalTool> {
    let published: HashSet<_> = definition_names(definitions).collect();
    context
        .local_tools
        .iter()
        .filter(|(name, _)| {
            name.as_str() != structured_output::TOOL_NAME && published.contains(name.as_str())
        })
        .map(|(name, tool)| (name.clone(), tool.clone()))
        .collect()
}

fn add_lua_local_tools(
    lua: &Lua,
    table: &Table,
    definitions: &mut JsonValue,
    local: &mut HashMap<String, LocalTool>,
) -> LuaResult<Result<(), String>> {
    let definitions = definitions.as_array_mut().expect("checked above");
    for pair in table.pairs::<String, Table>() {
        let (name, spec) = pair?;
        let Ok(description) = spec.get::<String>("description") else {
            return Ok(Err(format!(
                "local_tools.{name}: 'description' is required"
            )));
        };
        let input_schema = lua_to_json(lua, &spec.get::<LuaValue>("input_schema")?)?;
        let sanitized_schema = sanitize_tool_input_schema(input_schema);
        let Ok(handler) = spec.get::<Function>("handler") else {
            return Ok(Err(format!("local_tools.{name}: 'handler' is required")));
        };
        let capture_input = spec.get::<Option<bool>>("capture_input")?.unwrap_or(false);
        definitions.push(serde_json::json!({
            "name": name,
            "description": description,
            "input_schema": sanitized_schema,
        }));
        let audience =
            parse_audience(spec.get::<Option<Table>>("audiences")?, ToolAudience::MODEL)?;
        let weak = lua.weak();
        local.insert(
            name,
            maki_agent::tools::local_tool(audience, move |input, ctx| {
                let result = call_local_tool(
                    &weak,
                    &handler,
                    &input,
                    ctx.local_tool_capture.as_ref(),
                    capture_input,
                );
                Box::pin(async move { result })
            }),
        );
    }
    Ok(Ok(()))
}

/// Model, provider, and mode the child runs with, resolved once the provider
/// is built.
struct ChildSettings {
    model: Model,
    provider: Arc<dyn provider::Provider>,
    opts: RequestOptions,
    mode: AgentMode,
    inherits_parent_mode: bool,
}

impl ChildSettings {
    fn new(
        parent: &SessionParent,
        options: &SessionOptions,
        model: Model,
        provider: Arc<dyn provider::Provider>,
    ) -> Self {
        let parent_thinking = parent.context.opts.thinking;
        let thinking = options
            .selection
            .thinking
            .map_or(parent_thinking, |t| t.clamp_to(parent_thinking));
        let opts = RequestOptions {
            thinking,
            fast: options.fast,
        }
        .clamped(&model);
        let inherits_parent_mode = options.inherits_parent_mode(parent);
        let mode = if inherits_parent_mode {
            parent.context.mode.clone()
        } else {
            AgentMode::Build
        };
        Self {
            model,
            provider,
            opts,
            mode,
            inherits_parent_mode,
        }
    }

    fn mode_definition(&self, parent: &SessionParent) -> ModeDef {
        if self.inherits_parent_mode {
            parent
                .context
                .mode_def
                .as_deref()
                .cloned()
                .unwrap_or_else(|| parent.context.modes.current(&self.mode))
        } else {
            parent.context.modes.current(&self.mode)
        }
    }

    fn tool_template(
        &self,
        parent: &SessionParent,
        options: &SessionOptions,
        tools: &SessionTools,
        commit: &Arc<Mutex<Option<JsonValue>>>,
    ) -> ToolContext {
        let context = &parent.context;
        let mut template = context.to_tool_context();
        template.model = Arc::new(self.model.clone());
        template.provider = Arc::clone(&self.provider);
        template.opts = self.opts;
        template.audience = options.audience;
        template.workflow = options.workflow(parent);
        template.mode = self.mode.clone();
        template.mode_def = Some(Arc::new(self.mode_definition(parent)));
        template.tool_filter = Arc::clone(tools.request.filter());
        template.request_tools = Some(tools.request.clone());
        template.managed_turn = None;
        template.local_tools = Arc::new(tools.local.clone());
        template.local_tool_capture = Some(Arc::clone(commit));
        template.mcp = context
            .mcp
            .as_ref()
            .filter(|_| options.mcp)
            .map(McpSession::fresh);
        template
    }

    fn initial_config(
        &self,
        parent: &SessionParent,
        options: &SessionOptions,
    ) -> EffectiveAgentConfig {
        EffectiveAgentConfig::new(
            RunSettings {
                provider: Arc::clone(&self.provider),
                model: self.model.clone(),
                thinking: self.opts.thinking,
                fast: self.opts.fast,
                workflow: options.workflow(parent),
            },
            self.mode.clone(),
        )
        .with_mode_def(Some(self.mode_definition(parent)))
    }
}

fn agent_params(
    context: &AgentContext,
    agent_id: AgentId,
    model: Model,
    provider: Arc<dyn provider::Provider>,
    options: &SessionOptions,
) -> AgentParams {
    AgentParams {
        agent_id,
        provider,
        model,
        config: context.config.clone(),
        tool_output_lines: maki_config::ToolOutputLines::default(),
        permissions: Arc::clone(&context.permissions),
        session_id: context.session_id.clone(),
        mailbox: None,
        timeouts: context.timeouts,
        file_tracker: FileReadTracker::fresh(),
        prompt_slots: Arc::clone(&context.prompt_slots),
        modes: Arc::clone(&context.modes),
        subagent_cancels: Arc::new(CancelMap::new()),
        ledger: RunLedger::child(&context.ledger),
        registry: if options.inherit_defaults {
            Arc::clone(&context.registry)
        } else {
            Arc::clone(ToolRegistry::global_arc())
        },
        audience: options.audience,
        question_mode: context.question_mode,
        model_policy: Arc::clone(&context.model_policy),
        file_write_locks: Arc::clone(&context.file_write_locks),
        offload: context.offload.clone(),
        managed_turn: None,
    }
}

/// Spawns the child under the managed turn or trusted target when there is
/// one, otherwise as a free actor keyed into the parent's `subagent_cancels`.
fn spawn_session_actor(
    lua: &Lua,
    parent: &SessionParent,
    state: &Arc<LuaActorState>,
    initial_config: EffectiveAgentConfig,
    params: impl FnOnce(AgentId) -> AgentParams,
    child_trigger: CancelTrigger,
) -> Result<(AgentActorHandle, SessionControl), String> {
    let backend = || -> Box<dyn ActorBackend> {
        let mut backend = LuaActorBackend::new(Arc::clone(state));
        backend.offload = parent.context.offload.clone();
        Box::new(backend)
    };
    let init_params = |agent_id| {
        assert!(
            state.params.set(params(agent_id)).is_ok(),
            "session parameters must only be initialized once"
        );
    };
    if parent.managed_turn.is_none() && parent.trusted.is_none() {
        let agent_id = AgentId::generate();
        init_params(agent_id);
        let cancel_slot = parent
            .context
            .subagent_cancels
            .insert(state.ui_id.clone(), child_trigger);
        let (actor, task) = AgentActorHandle::spawn(agent_id, Vec::new(), None, backend());
        actor
            .initialize_config(initial_config)
            .map_err(|error| error.to_string())?;
        task.detach();
        return Ok((
            actor,
            SessionControl::Unmanaged {
                parent_cancels: Arc::clone(&parent.context.subagent_cancels),
                cancel_slot,
            },
        ));
    }
    let metadata = maki_agent::AgentMetadata {
        label: (!state.name.is_empty()).then_some(state.name.clone()),
        spawned_by_tool_use_id: parent.context.tool_use_id.clone(),
    };
    let child = if let Some(current) = &parent.managed_turn {
        current.spawn_child_with_config(initial_config, metadata, Vec::new(), None, backend())
    } else {
        let trusted = parent.trusted.as_ref().expect("trusted spawn checked");
        trusted.validate_target(lua, &trusted.target)?;
        trusted.target.manager().spawn_child_trusted_with_config(
            &trusted.target,
            initial_config,
            metadata,
            Vec::new(),
            None,
            |_| Ok::<_, String>(backend()),
        )
    }
    .map_err(|error| error.to_string())?;
    init_params(child.id());
    let actor = child.actor().map_err(|error| error.to_string())?;
    if let Err(error) = actor.configure_parent_delivery(
        state.auto_deliver && !state.silent,
        Some(state.ui_id.clone()),
    ) {
        let _ = child.close_subtree();
        return Err(error.to_string());
    }
    Ok((
        actor,
        SessionControl::Managed {
            agent: child,
            _child_trigger: child_trigger,
        },
    ))
}

/// `task_despawn` and global cancellation fire the shared child token.
/// Permanent closure aborts running turns and terminalizes queued turns;
/// managed sessions close their subtree. A normal close stops the relay first,
/// through the returned sender.
fn spawn_close_relay(
    actor: &AgentActorHandle,
    control: &SessionControl,
    state: &Arc<LuaActorState>,
) -> flume::Sender<()> {
    let (relay_stop_tx, relay_stop_rx) = flume::bounded::<()>(1);
    let actor = actor.clone();
    let managed_agent = match control {
        SessionControl::Managed { agent, .. } => Some(agent.clone()),
        SessionControl::Unmanaged { .. } => None,
    };
    let child_cancel = state.child_cancel.clone();
    let close_state = Arc::clone(state);
    smol::spawn(async move {
        select(
            Box::pin(async move { child_cancel.cancelled().await }),
            Box::pin(async move {
                let _ = relay_stop_rx.recv_async().await;
            }),
        )
        .await;
        close_state.notify_closed_direct();
        if let Some(agent) = managed_agent {
            let _ = agent.close_subtree();
        } else {
            close_state.close_with(&actor);
        }
    })
    .detach();
    relay_stop_tx
}

/// UI input relay: tab submits from the parent dispatch into this session.
/// Instructions steer open ordinary work through the same admission path as Lua submit.
fn spawn_ui_input_relay(
    actor: &AgentActorHandle,
    state: &LuaActorState,
    ui_input_rx: flume::Receiver<String>,
) {
    let actor = actor.clone();
    let mode = state.mode.clone();
    let opts = state.opts;
    smol::spawn(async move {
        while let Ok(message) = ui_input_rx.recv_async().await {
            let _ = actor.submit(
                AgentInput {
                    message,
                    mode: mode.clone(),
                    images: Vec::new(),
                    preamble: Vec::new(),
                    thinking: opts.thinking,
                    fast: opts.fast,
                    workflow: false,
                    prompt: None,
                    cancel: None,
                    lease_committer: None,
                },
                None,
                String::new(),
                TurnAdmissionOptions {
                    provenance: TurnProvenance {
                        origin: TurnOrigin::User,
                        ..Default::default()
                    },
                    ..Default::default()
                },
            );
        }
    })
    .detach();
}

/// Publishes a managed child as a trusted target, ties a delegated unmanaged
/// session to its invocation's lifetime, and records its owner. A failed step
/// closes the session.
fn register_session(
    lua: &Lua,
    sess: LuaSession,
    child_template: ToolContext,
    inherit_defaults: bool,
) -> Result<LuaSession, String> {
    let _ = sess.state.template.set(child_template.clone());
    if let SessionControl::Unmanaged { parent_cancels, .. } = &sess.control {
        if lua.app_data_ref::<StandaloneActors>().is_none() {
            lua.set_app_data(StandaloneActors::default());
        }
        let mut actors = lua.app_data_mut::<StandaloneActors>().unwrap();
        actors.0.retain(|(map, _)| map.strong_count() > 0);
        let parent = actors
            .0
            .iter()
            .find(|(map, _)| map.ptr_eq(&Arc::downgrade(parent_cancels)));
        if let Some((_, parent)) = parent {
            let mut children = parent.lock().unwrap();
            children
                .actors
                .retain(|(actor, _)| actor.strong_count() > 0);
            if children.closed {
                sess.state.close_with(&sess.actor);
            } else {
                children
                    .actors
                    .push((Arc::downgrade(&sess.actor), Arc::downgrade(&sess.state)));
            }
        }
        if let Some(params) = sess.state.params.get() {
            actors.0.push((
                Arc::downgrade(&params.subagent_cancels),
                Arc::clone(&sess.state.children),
            ));
        }
    }
    if let SessionControl::Managed { agent, .. } = &sess.control
        && let Some(service) = crate::orchestration::services(lua)
        && let Err(error) = service.register_target(crate::orchestration::TrustedTarget {
            target: agent.clone(),
            template: child_template,
        })
    {
        sess.close_controlled();
        return Err(error.to_string());
    }
    if !inherit_defaults
        && matches!(sess.authority, SessionAuthority::Delegated { .. })
        && matches!(sess.control, SessionControl::Unmanaged { .. })
    {
        let actor = Arc::clone(&sess.actor);
        let state = Arc::clone(&sess.state);
        if let Err(error) = crate::runtime::register_defer_close(
            lua,
            Box::new(move || {
                state.close_with(&actor);
            }),
        ) {
            sess.close_controlled();
            return Err(error.to_string());
        }
    }
    register_session_owner(lua, &sess);
    Ok(sess)
}

async fn build_session(
    lua: Lua,
    ctx: mlua::UserDataRef<LuaCtx>,
    opts: Table,
    inherit_defaults: bool,
) -> LuaResult<Pair<LuaSession>> {
    let parent = try_pair!(SessionParent::resolve(&lua, &ctx));
    let mut options = try_pair!(SessionOptions::parse(
        &lua,
        &opts,
        &parent,
        inherit_defaults
    )?);

    let required_output = opts.get::<Option<LuaValue>>("output_schema")?.is_some();
    let initial_mode_def = if options.inherits_parent_mode(&parent) {
        parent
            .context
            .mode_def
            .as_deref()
            .cloned()
            .unwrap_or_else(|| parent.context.modes.current(&parent.context.mode))
    } else {
        parent.context.modes.current(&AgentMode::Build)
    };
    try_pair!(validate_required_output_mode(
        required_output,
        Some(&initial_mode_def)
    ));
    let prepare = lua
        .app_data_ref::<SessionProviderPreparer>()
        .map(|p| Arc::clone(&p));
    let model_spec = options.selection.spec.as_deref().map(str::to_owned);
    let (model, provider) = try_pair!(
        build_session_provider(
            &model_spec,
            options.inherit_provider,
            &parent.context,
            prepare
        )
        .await
    );
    try_pair!(parent.revalidate(&lua, &ctx));
    let model_annotation = model.spec();
    let model_id = model.id.clone();

    let tools = try_pair!(SessionTools::assemble(
        &lua, &opts, &parent, &options, &model
    )?);
    let child = ChildSettings::new(&parent, &options, model, provider);
    let commit = Arc::new(Mutex::new(None));
    let child_template = child.tool_template(&parent, &options, &tools, &commit);
    let initial_config = child.initial_config(&parent, &options);
    try_pair!(validate_required_output_mode(
        tools.structured.is_some(),
        initial_config.mode_def.as_ref(),
    ));

    let (sub_tx, sub_rx) = flume::unbounded::<Envelope>();
    let (answer_tx, answer_rx) = flume::unbounded::<String>();
    let (ui_input_tx, ui_input_rx) = flume::unbounded::<String>();
    let subagent_info: Arc<OnceLock<SubagentInfo>> = Arc::new(OnceLock::new());
    // Register a cancel trigger so the child token does not fire on drop
    // and kill the subagent at birth. The fallback key gets its own id:
    // it keys `subagent_cancels`, so sharing the session id would make two
    // subagents running at once collide.
    let ui_id = parent
        .context
        .tool_use_id
        .clone()
        .unwrap_or_else(|| format!("session-{}", MakiId::generate()));
    // The subagent's permanent close is independent of the parent run's cancel:
    // it is triggered only through the shared `subagent_cancels` map
    // (`task_despawn` and global cancellation). Deriving it from `parent.context.cancel`
    // (the run's token, which the UI fires at normal run end by dropping the
    // run's CancelTrigger) would close every in-flight subagent as soon as the
    // spawning run finishes.
    let (child_trigger, child_cancel) = CancelToken::new();
    let cancel_actor = Arc::new(Mutex::new(None::<AgentActorHandle>));
    let cancel = SubagentCancel::new({
        let cancel_actor = Arc::clone(&cancel_actor);
        move || {
            if let Some(actor) = cancel_actor.lock().unwrap().as_ref() {
                actor.cancel_all();
            }
        }
    });
    let state = Arc::new(LuaActorState {
        template: OnceLock::new(),
        children: Arc::default(),
        params: OnceLock::new(),
        custom_system: options.system.take().unwrap_or_default(),
        tools: tools.request,
        opts: child.opts,
        mode: child.mode,
        mode_def: child
            .inherits_parent_mode
            .then(|| parent.context.mode_def.clone())
            .flatten(),
        mcp: parent
            .context
            .mcp
            .as_ref()
            .filter(|_| options.mcp)
            .map(McpSession::fresh),
        chip_event_tx: EventSender::new(sub_tx, parent.context.event_tx.run_id()),
        child_cancel,
        answer_rx: Arc::new(AsyncMutex::new(answer_rx)),
        answer_tx: Some(answer_tx),
        ui_id: ui_id.clone(),
        parent_event_tx: parent.context.event_tx.clone(),
        input_tx: ui_input_tx,
        cancel,
        parent_agent_id: parent.agent_id,
        parent_is_root: parent.is_root,
        auto_deliver: options.auto_deliver,
        silent: options.silent,
        subagent_info: Arc::clone(&subagent_info),
        local_tools: Arc::new(tools.local),
        structured: tools.structured,
        name: options.name.clone(),
        commit,
        semaphore: options.semaphore.take(),
        turn_permit: Mutex::default(),
        start: Instant::now(),
        execution_started: std::sync::atomic::AtomicBool::new(false),
        history_relayed: std::sync::atomic::AtomicBool::new(false),
        close_notified: std::sync::atomic::AtomicBool::new(false),
        relay_snapshot: Mutex::new(Vec::new()),
    });
    let (actor, control) = try_pair!(spawn_session_actor(
        &lua,
        &parent,
        &state,
        initial_config,
        |agent_id| agent_params(
            &parent.context,
            agent_id,
            child.model,
            child.provider,
            &options
        ),
        child_trigger,
    ));
    // A standalone task shows its model via SubagentInfo on the header;
    // a dispatching caller (batch) gets the same thing as a live annotation.
    if !options.silent
        && let Some(sink) = &parent.context.live_sink
    {
        let _ = sink.send(ToolLive::Annotation(model_annotation));
    }
    info!(name = %options.name, model = %model_id, "subagent session opened");
    smol::spawn(relay_session_events(
        sub_rx,
        parent.context.event_tx.clone(),
        subagent_info,
        parent.context.live_sink.clone(),
        options.silent,
        None,
    ))
    .detach();

    *cancel_actor.lock().unwrap() = Some(actor.clone());
    let relay_stop_tx = spawn_close_relay(&actor, &control, &state);
    spawn_ui_input_relay(&actor, &state, ui_input_rx);

    let sess = LuaSession {
        id: ui_id,
        agent_id: actor.agent_id(),
        policy_constraint: actor.effective_config().map(|ceiling| {
            session_policy_constraint(Arc::clone(&child_template.model_policy), ceiling)
        }),
        actor: Arc::new(actor),
        state,
        control,
        relay_stop_tx,
        authority: parent.authority,
    };
    let session = try_pair!(register_session(
        &lua,
        sess,
        child_template,
        inherit_defaults
    ));
    if let (Some(description), Some(trusted)) = (options.show, &parent.trusted) {
        let label = if options.name.is_empty() {
            UNNAMED_AGENT_LABEL
        } else {
            &options.name
        };
        let post = Post {
            text: description.map_or_else(|| label.to_owned(), |text| format!("{label}: {text}")),
            level: Default::default(),
            agent: Some(session.id.clone()),
        };
        if let Err(error) = send_post(&lua, trusted, post) {
            warn!(name = %options.name, %error, "could not show spawned agent");
        }
    }
    Ok((Some(session), None))
}

/// Puts {post} in the chat of {target}. A Lua child shares its parent's event
/// stream, so its posts carry its identity to land in its own chat.
pub(crate) fn send_post(lua: &Lua, target: &TrustedContext, post: Post) -> Result<(), String> {
    let event = AgentEvent::Post(post);
    let Some(state) = handles::session_state(lua, target.target.id()) else {
        return target
            .template
            .event_tx
            .send(event)
            .map_err(|error| error.to_string());
    };
    if state.silent {
        return Err(POST_TARGET_SILENT.into());
    }
    let subagent = state.subagent_info.get().ok_or(POST_TARGET_NOT_STARTED)?;
    state
        .parent_event_tx
        .send_envelope(Envelope {
            event,
            subagent: Some(subagent.clone()),
            run_id: state.parent_event_tx.run_id(),
        })
        .map_err(|error| error.to_string())
}

lua_table! {
    /// Subagent primitives for plugins that need to talk to an LLM.
    ///
    /// This module gives you the building blocks: resolve which model to use,
    /// build a system prompt, list available tools, call a tool directly, or
    /// open a full session with its own conversation history.
    ///
    /// Policy like retries, validation, and concurrency lives in the calling
    /// plugin, not here.
    ///
    /// ```lua
    /// local tools = maki.agent.tools(ctx, { audience = "general_sub" })
    /// local sess = maki.agent.session(ctx, {
    ///   system = "You are a helpful assistant.",
    ///   tools = tools,
    /// })
    /// local r = sess:prompt("Hello!")
    /// print(r.text)
    /// sess:close()
    /// ```
    "maki.agent" => pub(crate) fn create_agent_table(), DOCS [
        resolve_model, system_prompt, tools, callable_tools, call_tool, permission_prompt, is_yolo, session,
        report_task_result, current, root, list, get, spawn,
    ]
}

/// Must use `call_async`, not `call`: callbacks that yield (highlight,
/// markdown) hit the C-call boundary otherwise.
struct LiveCallbacks<'a> {
    tool: &'a str,
    authority: Option<(&'a Lua, &'a InvocationGuard)>,
    on_buf: Option<Function>,
    on_ann: Option<Function>,
    on_usage: Option<Function>,
}

impl LiveCallbacks<'_> {
    async fn deliver(&self, ev: ToolLive) {
        if let Some((lua, authority)) = self.authority
            && authority.validate(lua).is_err()
        {
            return;
        }
        let res = match ev {
            ToolLive::Buf(buf) => call_opt(&self.on_buf, BufHandle::foreign(buf)).await,
            ToolLive::Annotation(ann) => call_opt(&self.on_ann, ann).await,
            ToolLive::Usage(usage) => call_opt(&self.on_usage, usage).await,
        };
        if let Some(Err(e)) = res {
            tracing::warn!(tool = self.tool, error = %e, "call_tool callback failed");
        }
    }
}

async fn call_opt(f: &Option<Function>, arg: impl IntoLuaMulti) -> Option<LuaResult<()>> {
    match f {
        Some(f) => Some(f.call_async::<()>(arg).await),
        None => None,
    }
}

/// Like `interpreter_bridge::dispatch`, but keeps the full `ToolDoneEvent`
/// (the annotation lives there) and feeds live events to `cbs` while the
/// child runs.
async fn dispatch_racing_live(
    tctx: &ToolContext,
    name: &str,
    input: &JsonValue,
    rx: Option<flume::Receiver<ToolLive>>,
    cbs: &LiveCallbacks<'_>,
) -> ToolDoneEvent {
    let run = tool_dispatch::run(String::new(), name, input, tctx, CallOrigin::Nested);
    let Some(rx) = rx else {
        return run.await;
    };
    let mut run = pin!(run);
    loop {
        match select(run.as_mut(), pin!(rx.recv_async())).await {
            Either::Left((done, _)) => {
                while let Ok(ev) = rx.try_recv() {
                    cbs.deliver(ev).await;
                }
                return done;
            }
            Either::Right((Ok(ev), _)) => cbs.deliver(ev).await,
            // The sender is gone but no result arrived: just wait for the run.
            Either::Right((Err(_), _)) => return run.await,
        }
    }
}

#[derive(Clone)]
struct InvocationGuard {
    task: Weak<Mutex<crate::runtime::TaskCell>>,
    plugin: Option<(Arc<str>, Arc<crate::orchestration::PluginAuthority>)>,
}

impl InvocationGuard {
    fn capture(lua: &Lua) -> Result<Self, String> {
        let task = lua
            .app_data_ref::<crate::runtime::TaskHandle>()
            .map(|task| Arc::clone(&task))
            .ok_or(NO_TASK_AUTHORITY)?;
        let plugin = crate::runtime::current_plugin(lua).map(|(plugin, _)| {
            let owner = crate::orchestration::plugin_authority(lua, plugin.clone());
            (plugin, owner)
        });
        let guard = Self {
            task: Arc::downgrade(&task),
            plugin,
        };
        guard.validate(lua)?;
        Ok(guard)
    }

    fn validate(&self, lua: &Lua) -> Result<(), String> {
        let task = self.task.upgrade().ok_or(INVOCATION_EXPIRED)?;
        let current = lua
            .app_data_ref::<crate::runtime::TaskHandle>()
            .map(|task| Arc::clone(&task))
            .ok_or(NO_TASK_AUTHORITY)?;
        let cell = crate::runtime::lock_cell(&task);
        if !cell.is_live() {
            return Err(INVOCATION_EXPIRED.into());
        }
        if !Arc::ptr_eq(&task, &current) {
            return Err("context belongs to another invocation".into());
        }
        drop(cell);
        if let Some((plugin, owner)) = &self.plugin
            && !Arc::ptr_eq(
                owner,
                &crate::orchestration::plugin_authority(lua, plugin.clone()),
            )
        {
            return Err("session plugin generation expired".into());
        }
        Ok(())
    }
}

#[derive(Clone)]
enum SessionAuthority {
    Trusted {
        plugin: Arc<str>,
        generation: u64,
        owner: Arc<crate::orchestration::PluginAuthority>,
        services: Arc<dyn crate::orchestration::OrchestrationServices>,
    },
    Delegated {
        invocation: InvocationGuard,
        turn: Option<Box<CurrentManagedTurn>>,
        parent: Option<Weak<CancelMap<String>>>,
        generation: Option<u64>,
    },
}

impl SessionAuthority {
    fn capture(
        lua: &Lua,
        trusted: Option<&TrustedContext>,
        context: &AgentContext,
    ) -> Result<Self, String> {
        if let Some(trusted) = trusted {
            return Ok(Self::Trusted {
                plugin: trusted.plugin.clone(),
                generation: trusted.generation,
                owner: crate::orchestration::plugin_authority(lua, trusted.plugin.clone()),
                services: trusted.services(lua)?.clone(),
            });
        }
        Ok(Self::Delegated {
            invocation: InvocationGuard::capture(lua)?,
            turn: context.managed_turn.clone().map(Box::new),
            generation: crate::runtime::current_plugin(lua).map(|(_, generation)| generation),
            parent: crate::runtime::lock_cell(&crate::runtime::active_task(lua))
                .agent_parent
                .as_ref()
                .map(Arc::downgrade),
        })
    }

    fn validate(&self, lua: &Lua, child: Option<AgentId>) -> Result<(), String> {
        match self {
            Self::Trusted {
                plugin,
                generation,
                owner,
                services,
            } => {
                InvocationGuard::capture(lua)?.validate(lua)?;
                crate::runtime::current_plugin(lua).ok_or(NO_PLUGIN_AUTHORITY)?;
                if !crate::orchestration::holds_plugin_grant(lua, plugin, *generation, owner) {
                    return Err("session belongs to another plugin generation".into());
                }
                let current =
                    crate::orchestration::services(lua).ok_or(ORCHESTRATION_SERVICES_EXPIRED)?;
                if !Arc::ptr_eq(services, &current) {
                    return Err("session belongs to another host scope".into());
                }
                if crate::runtime::lock_cell(&crate::runtime::active_task(lua))
                    .delegated_tool
                    .is_some()
                {
                    return Err("trusted session requires trusted coroutine authority".into());
                }
            }
            Self::Delegated {
                invocation,
                turn,
                parent,
                generation,
            } => {
                let active = InvocationGuard::capture(lua)?;
                if *generation
                    != crate::runtime::current_plugin(lua).map(|(_, generation)| generation)
                {
                    return Err("session belongs to another plugin generation".into());
                }
                match (&invocation.plugin, &active.plugin) {
                    (Some((plugin, owner)), Some((current, current_owner)))
                        if plugin == current && Arc::ptr_eq(owner, current_owner) => {}
                    (None, None) => {}
                    _ => return Err("session belongs to another plugin generation".into()),
                }
                match (turn, crate::runtime::current_managed_turn(lua)) {
                    (Some(expected), Some(current)) if expected.same_authority(&current) => {
                        current
                            .validate_active()
                            .map_err(|error| error.to_string())?;
                        if let Some(child) = child {
                            current
                                .validate_descendant(child)
                                .map_err(|error| error.to_string())?;
                        }
                    }
                    (None, None) => {
                        if let Some(parent) = parent {
                            let expected =
                                parent.upgrade().ok_or("session parent scope expired")?;
                            let task = crate::runtime::active_task(lua);
                            let cell = crate::runtime::lock_cell(&task);
                            if !cell
                                .agent_parent
                                .as_ref()
                                .is_some_and(|current| Arc::ptr_eq(&expected, current))
                            {
                                return Err("session belongs to another parent scope".into());
                            }
                        } else {
                            invocation.validate(lua)?;
                        }
                    }
                    _ => {
                        return Err(MANAGED_AUTHORITY_INACTIVE.into());
                    }
                }
            }
        }
        Ok(())
    }
}

struct SessionOwner {
    plugin: Arc<str>,
    actor: Weak<AgentActorHandle>,
    state: Weak<LuaActorState>,
    agent: Option<maki_agent::AgentRef>,
}

#[derive(Default)]
struct SessionOwners(Vec<SessionOwner>);

fn register_session_owner(lua: &Lua, session: &LuaSession) {
    let plugin = match &session.authority {
        SessionAuthority::Trusted { plugin, .. } => Some(plugin.clone()),
        SessionAuthority::Delegated { invocation, .. } => {
            invocation.plugin.as_ref().map(|(plugin, _)| plugin.clone())
        }
    };
    let Some(plugin) = plugin else { return };
    if lua.app_data_ref::<SessionOwners>().is_none() {
        lua.set_app_data(SessionOwners::default());
    }
    if let Some(mut owners) = lua.app_data_mut::<SessionOwners>() {
        owners.0.retain(|owner| owner.actor.strong_count() > 0);
        owners.0.push(SessionOwner {
            plugin,
            actor: Arc::downgrade(&session.actor),
            state: Arc::downgrade(&session.state),
            agent: match &session.control {
                SessionControl::Managed { agent, .. } => Some(agent.clone()),
                SessionControl::Unmanaged { .. } => None,
            },
        });
    }
}

pub(crate) fn close_plugin_agents(lua: &Lua, plugin: &str) {
    handles::close_plugin_agents(lua, plugin);
    let retired = lua
        .app_data_mut::<SessionOwners>()
        .map(|mut owners| {
            let (retired, retained): (Vec<_>, Vec<_>) = owners
                .0
                .drain(..)
                .partition(|owner| owner.plugin.as_ref() == plugin);
            owners.0 = retained;
            retired
        })
        .unwrap_or_default();
    for owner in retired {
        if let Some(agent) = owner.agent {
            let _ = agent.close_subtree();
        } else if let Some(actor) = owner.actor.upgrade() {
            if let Some(state) = owner.state.upgrade() {
                state.close_with(&actor);
            } else {
                actor.close();
            }
        }
    }
}

enum SessionControl {
    Managed {
        agent: maki_agent::AgentRef,
        _child_trigger: CancelTrigger,
    },
    Unmanaged {
        parent_cancels: Arc<CancelMap<String>>,
        cancel_slot: CancelSlot,
    },
}

/// Compatibility child capability. Effects require current same-plugin authority.
/// Trusted creation survives handler return within one host generation; delegated
/// creation expires with its invocation. Reload/unload closes owned children.
struct LuaSession {
    id: String,
    agent_id: AgentId,
    /// Agent actor data; used for `agent_id`.
    ///
    /// This is a separate Arc so prompt/send/status can keep the session
    /// alive while the actor is being torn down (the actor owns its own
    /// backend Arc).
    actor: Arc<AgentActorHandle>,
    state: Arc<LuaActorState>,
    control: SessionControl,
    /// Stops the CancelSubagent->close relay so a normal close (not a
    /// cancellation) does not double-close.
    relay_stop_tx: flume::Sender<()>,
    authority: SessionAuthority,
    policy_constraint: Option<AdmissionPolicyConstraint>,
}

fn session_policy_matches(
    model_policy: &ModelPolicy,
    ceiling: &EffectiveAgentConfig,
    current: &EffectiveAgentConfig,
) -> bool {
    model_policy.allows(&current.model.spec()) && mode_policy_is_within(ceiling, current)
}

fn session_policy_constraint(
    model_policy: Arc<ModelPolicy>,
    ceiling: Arc<EffectiveAgentConfig>,
) -> AdmissionPolicyConstraint {
    Arc::new(move |current| {
        if session_policy_matches(&model_policy, &ceiling, current) {
            Ok(())
        } else {
            Err(SESSION_POLICY_CHANGED_ERR.to_owned())
        }
    })
}

fn mode_policy_is_within(ceiling: &EffectiveAgentConfig, current: &EffectiveAgentConfig) -> bool {
    let ceiling_path = ceiling
        .mode_def
        .as_ref()
        .and_then(|definition| definition.restrict_write_to.as_deref())
        .or_else(|| ceiling.mode.plan_path());
    let current_path = current
        .mode_def
        .as_ref()
        .and_then(|definition| definition.restrict_write_to.as_deref())
        .or_else(|| current.mode.plan_path());
    if ceiling_path.is_some() && ceiling_path != current_path {
        return false;
    }
    match ceiling
        .mode_def
        .as_ref()
        .and_then(|definition| definition.tools.as_ref())
    {
        Some(allowed) => current
            .mode_def
            .as_ref()
            .and_then(|definition| definition.tools.as_ref())
            .is_some_and(|tools| tools.iter().all(|tool| allowed.contains(tool))),
        None => true,
    }
}

impl LuaSession {
    fn validate(&self, lua: &Lua) -> Result<(), String> {
        self.authority.validate(lua, Some(self.agent_id))
    }

    fn close_controlled(&self) {
        self.state.notify_closed_ordered();
        let _ = self.relay_stop_tx.try_send(());
        match &self.control {
            SessionControl::Managed { agent, .. } => {
                let _ = agent.close_subtree();
            }
            SessionControl::Unmanaged { .. } => self.state.close_with(&self.actor),
        }
        self.retire_fallback();
    }

    fn retire_fallback(&self) {
        if let SessionControl::Unmanaged {
            parent_cancels,
            cancel_slot,
        } = &self.control
        {
            parent_cancels.retire(&self.id, *cancel_slot);
        }
    }
}

impl Drop for LuaSession {
    fn drop(&mut self) {
        // The actor retains every terminal outcome; closing it first would
        // terminalize queued turns. The history fallback fires once, only if
        // no executed turn relayed already.
        self.close_controlled();
        let snapshot = self.actor.snapshot();
        info!(
            name = %self.state.name,
            agent_id = %self.agent_id,
            duration_ms = self.state.start.elapsed().as_millis() as u64,
            input_tokens = snapshot.cumulative_usage.total_input(),
            output_tokens = snapshot.cumulative_usage.output,
            "subagent session dropped",
        );
    }
}

/// Send a message to the subagent and wait for its full response. The agent
/// loop runs to completion, calling tools as needed. Conversation history is
/// kept across calls, so you can have a multi-turn conversation. This is a
/// blocking compatibility wrapper over `send` + the completion notifier; the
/// async `send`/`status` pair is preferred for background work.
///
/// In a managed invocation, `prompt` validates the current turn and temporarily
/// yields its manager permit while the child runs. Without a current managed
/// invocation, the same managed session uses an ordinary exact-turn wait. The
/// child still uses manager capacity, but no parent permit is yielded. A timeout
/// closes the child's managed subtree.
///
/// The returned table has fields: `text` (string), `captured` (optional value),
/// `input_tokens` (integer), `output_tokens` (integer). Counters are cumulative
/// for the session, unlike AgentTurn's per-turn counters. `text` is an empty
/// string when the subagent produced no text block (e.g. it only called
/// tools).
///
/// @param message string User message to send.
/// @param opts table? Optional fields:
///   `timeout` (integer?) - maximum wait in seconds. A timeout closes the session and returns no partial streamed text.
/// @return (table?, string?) Result table on success, or `(nil, err)` on
/// failure. A run cut short after streaming some text hands you both: the
/// error and a `{ text = <what it streamed> }` table.
/// @example
/// local r, err = sess:prompt("What files are in this project?")
/// if err then error(err) end
/// print(r.text)
/// print(r.input_tokens .. " input, " .. r.output_tokens .. " output tokens")
#[lua_fn]
async fn prompt(
    lua: Lua,
    this: mlua::UserDataRef<LuaSession>,
    message: String,
    opts: Option<Table>,
) -> LuaResult<Pair<Table>> {
    try_pair!(this.validate(&lua));
    let authority = this.authority.clone();
    let policy_constraint = this.policy_constraint.clone();
    let provenance = session_provenance(&lua);
    let child_id = this.agent_id;
    let actor = Arc::clone(&this.actor);
    let state = Arc::clone(&this.state);
    let (managed_child, fallback_cancel) = match &this.control {
        SessionControl::Managed { agent, .. } => (Some(agent.clone()), None),
        SessionControl::Unmanaged {
            parent_cancels,
            cancel_slot,
        } => (
            None,
            Some((Arc::clone(parent_cancels), this.id.clone(), *cancel_slot)),
        ),
    };
    let timeout = opts
        .map(|opts| opts.get::<Option<u64>>("timeout"))
        .transpose()?
        .flatten();
    let managed_wait = match (
        managed_child.as_ref().map(maki_agent::AgentRef::id),
        crate::runtime::current_managed_turn(&lua),
    ) {
        (Some(child_id), Some(current)) => {
            try_pair!(current.validate_descendant(child_id));
            Some((current, child_id))
        }
        _ => None,
    };
    if timeout == Some(0) {
        return Ok(err_pair("timeout must be greater than zero"));
    }
    drop(this);
    let input = AgentInput {
        message,
        mode: state.mode.clone(),
        images: Vec::new(),
        preamble: Vec::new(),
        thinking: state.opts.thinking,
        fast: state.opts.fast,
        workflow: false,
        prompt: None,
        cancel: None,
        lease_committer: None,
    };
    let result = if let Some((current, child_id)) = managed_wait {
        let admission = maki_agent::manager::PromptAdmission {
            input,
            event_sender: None,
            correlation: String::new(),
        };
        let admitted = match current.lease().admit_and_wait_for_descendant_with_options(
            &current,
            child_id,
            &actor,
            admission,
            timeout.map(Duration::from_secs),
            maki_agent::TurnAdmissionOptions {
                provenance,
                policy_constraint,
                ..Default::default()
            },
        ) {
            Ok(Some(admitted)) => admitted,
            Ok(None) => return Ok((None, Some(SESSION_CLOSED_ERR.to_owned()))),
            Err(error) => return Ok(err_pair(error.to_string())),
        };
        let (_, wait) = admitted;
        match wait.wait_result().await {
            Ok(outcome) => outcome,
            Err(maki_agent::PromptWaitError::Timeout) => {
                let seconds = timeout.expect("managed timeout requires a configured duration");
                return Ok(err_pair(format!(
                    "session prompt timed out after {seconds}s"
                )));
            }
            Err(maki_agent::PromptWaitError::Cancelled) => {
                return Ok(err_pair(CANCELLED_MSG));
            }
        }
    } else {
        let Ok(ticket) = actor.admit_turn_with_options(
            input,
            None,
            String::new(),
            maki_agent::TurnAdmissionOptions {
                provenance,
                policy_constraint,
                ..Default::default()
            },
        ) else {
            return Ok((None, Some(SESSION_CLOSED_ERR.to_owned())));
        };
        match timeout {
            Some(seconds) => {
                let outcome =
                    futures_lite::future::race(async { Some(ticket.wait_result().await) }, async {
                        smol::Timer::after(Duration::from_secs(seconds)).await;
                        None
                    })
                    .await;
                let Some(outcome) = outcome else {
                    if let Some(agent) = &managed_child {
                        let _ = agent.close_subtree();
                    } else {
                        state.close_with(&actor);
                    }
                    if let Some((parent_cancels, id, cancel_slot)) = &fallback_cancel {
                        parent_cancels.retire(id, *cancel_slot);
                    }
                    return Ok(err_pair(format!(
                        "session prompt timed out after {seconds}s"
                    )));
                };
                outcome
            }
            None => ticket.wait_result().await,
        }
    };
    try_pair!(authority.validate(&lua, Some(child_id)));
    let cumulative = actor.snapshot().cumulative_usage;
    build_prompt_result(&lua, result, cumulative)
}

fn session_result_table(lua: &Lua, result: &TurnResult, usage: TokenUsage) -> LuaResult<Table> {
    let tbl = lua.create_table()?;
    tbl.set("text", result.text.clone())?;
    tbl.set("input_tokens", usage.total_input())?;
    tbl.set("output_tokens", usage.output)?;
    if let Some(captured) = result.output.last() {
        tbl.set("captured", json_to_lua(lua, captured)?)?;
    }
    Ok(tbl)
}

fn session_result_error(outcome: &TurnOutcome) -> Option<String> {
    match outcome {
        TurnOutcome::Completed { .. } => None,
        TurnOutcome::Failed { failure, .. } => Some(failure.diagnostic.clone()),
        TurnOutcome::Cancelled { .. } => Some(CANCELLED_MSG.to_owned()),
    }
}

fn build_prompt_result(lua: &Lua, result: TurnResult, usage: TokenUsage) -> LuaResult<Pair<Table>> {
    let error = session_result_error(&result.outcome);
    let table = if error.is_some() && result.text.is_empty() && result.output.is_empty() {
        None
    } else {
        Some(session_result_table(lua, &result, usage)?)
    };
    Ok((table, error))
}

fn session_provenance(lua: &Lua) -> maki_agent::TurnProvenance {
    let plugin = crate::runtime::current_plugin(lua);
    let turn = crate::runtime::current_managed_turn(lua);
    maki_agent::TurnProvenance {
        origin: maki_agent::TurnOrigin::Plugin,
        plugin: plugin.as_ref().map(|(name, _)| name.to_string()),
        plugin_generation: plugin.map(|(_, generation)| generation),
        source_agent: turn.as_ref().map(CurrentManagedTurn::agent_id),
        source_turn: turn.as_ref().map(CurrentManagedTurn::turn_id),
    }
}

fn actor_is_busy(status: ActorStatus, queued: usize) -> bool {
    matches!(status, ActorStatus::Running(_)) || queued > 0
}

/// Non-blocking: enqueue a message to the subagent's driver and return
/// immediately. Poll `status()` for the result.
///
/// @param message string User message to send.
/// @return (boolean?, string?) `true` on success, or `(nil, err)` if the session is closed.
#[lua_fn]
async fn send(
    lua: Lua,
    this: mlua::UserDataRef<LuaSession>,
    message: String,
) -> LuaResult<Pair<bool>> {
    try_pair!(this.validate(&lua));
    let actor = Arc::clone(&this.actor);
    let state = Arc::clone(&this.state);
    let policy_constraint = this.policy_constraint.clone();
    drop(this);
    match actor.admit_turn_with_options(
        AgentInput {
            message,
            mode: state.mode.clone(),
            images: Vec::new(),
            preamble: Vec::new(),
            thinking: state.opts.thinking,
            fast: state.opts.fast,
            workflow: false,
            prompt: None,
            cancel: None,
            lease_committer: None,
        },
        None,
        String::new(),
        maki_agent::TurnAdmissionOptions {
            provenance: session_provenance(&lua),
            policy_constraint,
            ..Default::default()
        },
    ) {
        Ok(_) => Ok((Some(true), None)),
        Err(_) => Ok((None, Some(SESSION_CLOSED_ERR.to_owned()))),
    }
}

/// Read the subagent's current status without blocking.
///
/// @return (table?, string?) Table with status ("running", "done", or "closed").
///   When done, result contains text, input_tokens, output_tokens (cumulative
///   session counters), and optional captured. error is present on failure or
///   cancellation; retryable is present on failure. Authority failure returns nil
///   and an error instead of a status table.
#[lua_fn]
async fn status(lua: Lua, this: mlua::UserDataRef<LuaSession>) -> LuaResult<Pair<Table>> {
    try_pair!(this.authority.validate(&lua, Some(this.agent_id)));
    let actor = Arc::clone(&this.actor);
    drop(this);
    let snapshot = actor.snapshot();
    let tbl = lua.create_table()?;
    match snapshot.lifecycle {
        ActorLifecycle::Open => {
            // A newer turn admitted behind the latest outcome keeps the session
            // running: the old SessionState reported Running while pending > 0,
            // so only an idle queue with a retained latest outcome is "done".
            let busy = actor_is_busy(snapshot.status, snapshot.queued);
            if busy {
                tbl.set("status", "running")?;
            } else if let Some(outcome) = snapshot.latest {
                tbl.set("status", "done")?;
                if let Some(result) = actor
                    .turn_ticket(outcome.turn_id())
                    .and_then(|ticket| ticket.peek_result())
                {
                    tbl.set(
                        "result",
                        session_result_table(&lua, &result, snapshot.cumulative_usage)?,
                    )?;
                }
                if let Some(error) = session_result_error(&outcome) {
                    tbl.set("error", error)?;
                }
                if let TurnOutcome::Failed { failure, .. } = &outcome {
                    tbl.set("retryable", failure.retryable)?;
                }
            } else {
                tbl.set("status", "running")?;
            }
        }
        ActorLifecycle::Closed | ActorLifecycle::Shutdown => {
            tbl.set("status", "closed")?;
        }
    }
    Ok((Some(tbl), None))
}

/// Close the session and flush its history back to the parent agent. You can
/// call this multiple times safely. If you forget, it runs automatically when
/// the session is garbage collected. Firing the cancel token wakes the driver
/// so it flushes, even if it is parked on the input queue.
///
/// @return
#[lua_fn]
async fn close(lua: Lua, this: mlua::UserDataRef<LuaSession>) -> LuaResult<Pair<bool>> {
    try_pair!(this.authority.validate(&lua, Some(this.agent_id)));
    this.close_controlled();
    Ok((Some(true), None))
}

/// Return this session's stable id, used as a `task_id` by the task plugin.
///
/// @return (string) The session's id.
#[lua_fn]
fn session_id(lua: &Lua, this: &LuaSession) -> LuaResult<String> {
    let _ = lua;
    Ok(this.id.clone())
}

enum TaskOwnerAuthority {
    Session(Box<SessionAuthority>),
    StandaloneAgent {
        plugin: Arc<str>,
        generation: u64,
        owner: Arc<crate::orchestration::PluginAuthority>,
        parent: Arc<CancelMap<String>>,
        host_root: Option<Arc<HostRootServices>>,
    },
    Agent {
        plugin: Arc<str>,
        generation: u64,
        owner: Arc<crate::orchestration::PluginAuthority>,
        target: maki_agent::AgentRef,
    },
}

struct LuaTaskOwner {
    owner_id: Option<AgentId>,
    authority: TaskOwnerAuthority,
    child_id: AgentId,
}

impl mlua::UserData for LuaTaskOwner {
    fn add_methods<M: mlua::UserDataMethods<Self>>(methods: &mut M) {
        // A managed task whose child has closed, for instance because its owner
        // was closed, can be dropped without anyone's authority.
        methods.add_method("_maki_retired", |_, this, ()| {
            Ok(matches!(
                &this.authority,
                TaskOwnerAuthority::Agent { target, .. } if target.validate_live().is_err()
            ))
        });
        methods.add_method("_maki_can_control", |lua, this, ()| {
            match &this.authority {
                TaskOwnerAuthority::Session(authority) => {
                    if authority.validate(lua, Some(this.child_id)).is_err() {
                        return Ok(false);
                    }
                }
                TaskOwnerAuthority::StandaloneAgent {
                    plugin,
                    generation,
                    owner,
                    parent,
                    host_root,
                } => {
                    if host_root
                        .as_ref()
                        .is_some_and(|services| services.validate(&services.reference()).is_err())
                    {
                        return Ok(false);
                    }
                    if InvocationGuard::capture(lua).is_err()
                        || !crate::orchestration::holds_plugin_grant(
                            lua,
                            plugin,
                            *generation,
                            owner,
                        )
                        || crate::runtime::current_managed_turn(lua).is_some()
                    {
                        return Ok(false);
                    }
                    let scope = crate::runtime::active_task(lua);
                    let cell = crate::runtime::lock_cell(&scope);
                    if cell.delegated_tool.is_none() {
                        return Ok(true);
                    }
                    drop(cell);
                    return Ok(crate::runtime::lock_cell(&scope)
                        .agent_parent
                        .as_ref()
                        .is_some_and(|current| Arc::ptr_eq(parent, current)));
                }
                TaskOwnerAuthority::Agent {
                    plugin,
                    generation,
                    owner,
                    target,
                } => {
                    if InvocationGuard::capture(lua).is_err()
                        || !crate::orchestration::holds_plugin_grant(
                            lua,
                            plugin,
                            *generation,
                            owner,
                        )
                    {
                        return Ok(false);
                    }
                    let Some(current) = crate::runtime::current_managed_turn(lua) else {
                        return Ok(false);
                    };
                    if !current.manager().same_manager(&target.manager())
                        || current.validate_active().is_err()
                        || (current.agent_id() != target.id()
                            && current.validate_descendant(target.id()).is_err())
                    {
                        return Ok(false);
                    }
                }
            }
            let Some(owner_id) = this.owner_id else {
                return Ok(true);
            };
            let Some(current) = crate::runtime::current_managed_turn(lua) else {
                return Ok(false);
            };
            if current.agent_id() == owner_id {
                return Ok(current.validate_active().is_ok());
            }
            Ok(current.validate_descendant(owner_id).is_ok())
        });
    }
}

fn session_internal_methods<M: mlua::UserDataMethods<LuaSession>>(methods: &mut M) {
    methods.add_method("_maki_managed", |_, this, ()| {
        Ok(matches!(this.control, SessionControl::Managed { .. }))
    });
    methods.add_method("_maki_task_owner", |_, this, ()| {
        Ok(LuaTaskOwner {
            authority: TaskOwnerAuthority::Session(Box::new(this.authority.clone())),
            child_id: this.agent_id,
            owner_id: matches!(this.control, SessionControl::Managed { .. })
                .then_some(this.state.parent_agent_id)
                .flatten(),
        })
    });
}

lua_class! {
    /// A compatibility subagent session with its own conversation history.
    /// Context-free methods validate current host-assigned coroutine authority.
    /// Trusted-created sessions require the same plugin and host generation;
    /// delegated-created sessions expire with their creating invocation.
    /// Operations cannot widen the immutable creation policy ceiling. Plugin
    /// revocation cancels owned work; passing userdata does not transfer authority.
    ///
    /// Create one with `maki.agent.session()`, then send messages with
    /// `:prompt()` or `:send()`. The session remembers previous turns, so you
    /// can have a multi-step conversation. Call `:close()` when you are done,
    /// or let garbage collection handle it.
    "maki.agent.Session" => LuaSession, SESSION_DOCS [prompt, send, status, close, session_id]
    extra session_internal_methods
}

/// Commit a result to the session whose local tool is currently executing.
///
/// @param value table The result to commit.
/// @return (boolean?, string?) `true` while a session-local tool is active.
#[lua_fn]
fn report_task_result(lua: &Lua, value: LuaValue) -> LuaResult<Pair<bool>> {
    ACTIVE_COMMIT.with_borrow(|active| {
        let Some(slot) = active else {
            return Ok((None, Some(SESSION_CLOSED_ERR.to_owned())));
        };
        let json = lua_to_json(lua, &value).map_err(|e| mlua::Error::runtime(e.to_string()))?;
        *slot.lock().unwrap() = Some(json);
        Ok((Some(true), None))
    })
}

thread_local! {
    static ACTIVE_COMMIT: RefCell<Option<Arc<Mutex<Option<JsonValue>>>>> = const { RefCell::new(None) };
}

/// Weak Lua ref avoids a reference cycle when the session is stored in userdata.
fn call_local_tool(
    weak: &mlua::WeakLua,
    f: &Function,
    input: &JsonValue,
    commit: Option<&Arc<Mutex<Option<JsonValue>>>>,
    capture_input: bool,
) -> Result<String, String> {
    let lua = weak.try_upgrade().ok_or("Lua runtime shut down")?;
    let arg = json_to_lua(&lua, input).map_err(|e| e.to_string())?;
    let previous = ACTIVE_COMMIT.replace(commit.cloned());
    let values = f.call::<mlua::MultiValue>(arg);
    ACTIVE_COMMIT.replace(previous);
    let result = lua_tool_result(values.map_err(|e| e.to_string())?);
    if result.is_ok()
        && capture_input
        && let Some(commit) = commit
    {
        *commit.lock().unwrap() = Some(input.clone());
    }
    result
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use maki_agent::cancel::CancelToken;
    use maki_agent::tools::test_support::stub_ctx;
    use maki_agent::{
        AgentId, AgentParams, DoneReason, TurnCompleteEvent, TurnId, TurnOutcome, TurnTicket,
    };
    use maki_providers::provider::{BoxFuture, Provider};
    use maki_providers::{
        AgentError, ContentBlock, Message, Model, ModelInfo, ProviderEvent, RequestOptions, Role,
        StopReason, StreamResponse, ThinkingConfig, TokenUsage,
    };
    use maki_storage::id::SessionRef;
    use serde_json::{Value, json};

    use super::*;

    #[test_case::test_case("{}", Some(json!([])); "empty_table")]
    #[test_case::test_case("{{ name = 'read' }}", Some(json!([{ "name": "read" }])); "sequence")]
    #[test_case::test_case("{ name = 'read' }", None; "map_rejected")]
    #[test_case::test_case("'read'", None; "scalar_rejected")]
    fn request_tools_normalizes_only_sequence_tables(source: &str, expected: Option<Value>) {
        let lua = Lua::new();
        let value: LuaValue = lua.load(format!("return {source}")).eval().unwrap();
        let result = request_tools_json(&lua, &value);
        match expected {
            Some(expected) => assert_eq!(result.unwrap(), expected),
            None => assert!(result.is_err()),
        }
    }

    fn canned_reply(text: &str) -> StreamResponse {
        canned_reply_with_usage(text, TokenUsage::default())
    }

    pub(super) fn canned_reply_with_usage(text: &str, usage: TokenUsage) -> StreamResponse {
        StreamResponse {
            message: Message {
                role: Role::Assistant,
                content: vec![ContentBlock::Text { text: text.into() }],
                ..Default::default()
            },
            usage,
            stop_reason: Some(StopReason::EndTurn),
        }
    }

    /// `inherit_provider` must select the reuse branch even when `model_spec`
    /// is present, so a test-side wrapper can drive a real spawned session
    /// through the parent provider instead of building one from the spec
    /// (which would hit the network).
    #[test]
    fn inherit_provider_selects_reuse_branch_over_model_spec() {
        let ctx = stub_ctx(&AgentMode::Build);
        let parent_model = ctx.model.spec();
        let parent_provider = Arc::clone(&ctx.provider);
        let agent = AgentContext::from(&ctx);

        let (model, provider) = smol::block_on(build_session_provider(
            &Some("anthropic/claude-opus-4-20250514".into()),
            true,
            &agent,
            None,
        ))
        .unwrap();

        assert!(
            Arc::ptr_eq(&provider, &parent_provider),
            "must reuse parent provider"
        );
        assert_eq!(model.spec(), parent_model, "must reuse parent model");
    }

    #[test]
    fn absent_model_spec_reuses_parent_provider() {
        let ctx = stub_ctx(&AgentMode::Build);
        let parent_provider = Arc::clone(&ctx.provider);
        let agent = AgentContext::from(&ctx);

        let (_, provider) =
            smol::block_on(build_session_provider(&None, false, &agent, None)).unwrap();
        assert!(Arc::ptr_eq(&provider, &parent_provider));
    }

    fn call(src: &str, input: JsonValue) -> Result<String, String> {
        let lua = Lua::new();
        let f: Function = lua.load(src).eval().unwrap();
        let commit = Arc::new(Mutex::new(None));
        call_local_tool(&lua.weak(), &f, &input, Some(&commit), false)
    }

    #[test]
    fn local_tool_handler_result_conventions() {
        let input = json!({"x": "1"});
        assert_eq!(
            call("function(v) return 'ok:' .. v.x end", input.clone()),
            Ok("ok:1".into())
        );
        assert_eq!(
            call("function() return nil, 'bad' end", input.clone()),
            Err("bad".into())
        );
        assert_eq!(
            call("function() end", input.clone()),
            Err(crate::api::util::convert::NIL_TOOL_RESULT_ERR.into())
        );
        let raised = call("function() error('boom') end", input.clone()).unwrap_err();
        assert!(raised.contains("boom"), "got: {raised}");
        let wrong = call("function() return 42 end", input).unwrap_err();
        assert!(wrong.contains("expected string"), "got: {wrong}");
    }

    const RUN_ID: u64 = 7;
    const PARENT_ID: &str = "task-1";
    const FIRST_USAGE: TokenUsage = tokens(100, 20);
    const SECOND_USAGE: TokenUsage = tokens(50, 10);
    const CUMULATIVE_USAGE: TokenUsage = tokens(150, 30);
    const DONE_USAGE: TokenUsage = CUMULATIVE_USAGE;

    const fn tokens(input: u32, output: u32) -> TokenUsage {
        TokenUsage {
            input,
            output,
            cache_creation: 0,
            cache_read: 0,
        }
    }

    fn envelope(event: AgentEvent) -> Envelope {
        Envelope {
            event,
            subagent: None,
            run_id: RUN_ID,
        }
    }

    fn turn(usage: TokenUsage, cost: f64) -> AgentEvent {
        AgentEvent::TurnComplete(Box::new(TurnCompleteEvent {
            message: Message::default(),
            usage,
            model: "test-model".into(),
            cost: Some(cost),
            context_size: None,
            context_window: 0,
        }))
    }

    /// A provider that never resolves, so a running turn parks until its
    /// cancel fires. Deterministic for close/cancel tests.
    struct HangingProvider;

    impl Provider for HangingProvider {
        fn stream_message<'a>(
            &'a self,
            _model: &'a Model,
            _messages: &'a [Message],
            _system: &'a str,
            _tools: &'a Value,
            _event_tx: &'a flume::Sender<ProviderEvent>,
            _opts: RequestOptions,
            _session_id: Option<&'a SessionRef>,
        ) -> BoxFuture<'a, Result<StreamResponse, AgentError>> {
            Box::pin(async move {
                std::future::pending::<()>().await;
                unreachable!()
            })
        }

        fn list_models(&self) -> BoxFuture<'_, Result<Vec<ModelInfo>, AgentError>> {
            Box::pin(async { unimplemented!() })
        }
    }

    /// A canned `LuaActorState` + actor harness. Runs park on the hanging
    /// provider until their cancel fires, so close/cancel tests are
    /// deterministic.
    fn canned_state(
        semaphore: Option<Arc<async_lock::Semaphore>>,
    ) -> (
        Arc<AgentActorHandle>,
        Arc<LuaActorState>,
        LuaSession,
        flume::Receiver<Envelope>,
    ) {
        session_with_provider(Arc::new(HangingProvider), semaphore, None)
    }

    /// A provider whose turns answer with one canned reply.
    pub(super) struct StreamOnceProvider {
        replies: Mutex<Vec<StreamResponse>>,
    }

    impl StreamOnceProvider {
        pub(super) fn new_replies(replies: Vec<StreamResponse>) -> Self {
            Self {
                replies: Mutex::new(replies),
            }
        }
    }

    impl Provider for StreamOnceProvider {
        fn stream_message<'a>(
            &'a self,
            _model: &'a Model,
            _messages: &'a [Message],
            _system: &'a str,
            _tools: &'a Value,
            _event_tx: &'a flume::Sender<ProviderEvent>,
            _opts: RequestOptions,
            _session_id: Option<&'a SessionRef>,
        ) -> BoxFuture<'a, Result<StreamResponse, AgentError>> {
            Box::pin(async move {
                let mut replies = self.replies.lock().unwrap();
                assert!(!replies.is_empty(), "no more canned replies");
                Ok(replies.remove(0))
            })
        }

        fn list_models(&self) -> BoxFuture<'_, Result<Vec<ModelInfo>, AgentError>> {
            Box::pin(async { unimplemented!() })
        }
    }

    /// Rebuild a canned state with a working provider, so an admitted turn
    /// actually executes.
    pub(super) fn session_with_provider(
        provider: Arc<dyn Provider>,
        semaphore: Option<Arc<async_lock::Semaphore>>,
        relay_gate: Option<flume::Receiver<()>>,
    ) -> (
        Arc<AgentActorHandle>,
        Arc<LuaActorState>,
        LuaSession,
        flume::Receiver<Envelope>,
    ) {
        session_with_provider_and_parent(provider, semaphore, relay_gate, None)
    }

    pub(super) fn session_with_provider_for_agent(
        provider: Arc<dyn Provider>,
        agent_id: AgentId,
    ) -> (
        Arc<AgentActorHandle>,
        Arc<LuaActorState>,
        LuaSession,
        flume::Receiver<Envelope>,
    ) {
        session_fixture(provider, None, None, None, agent_id)
    }

    fn session_with_provider_and_parent(
        provider: Arc<dyn Provider>,
        semaphore: Option<Arc<async_lock::Semaphore>>,
        relay_gate: Option<flume::Receiver<()>>,
        parent_raw_tx: Option<flume::Sender<Envelope>>,
    ) -> (
        Arc<AgentActorHandle>,
        Arc<LuaActorState>,
        LuaSession,
        flume::Receiver<Envelope>,
    ) {
        session_fixture(
            provider,
            semaphore,
            relay_gate,
            parent_raw_tx,
            AgentId::generate(),
        )
    }

    fn session_fixture(
        provider: Arc<dyn Provider>,
        semaphore: Option<Arc<async_lock::Semaphore>>,
        relay_gate: Option<flume::Receiver<()>>,
        parent_raw_tx: Option<flume::Sender<Envelope>>,
        agent_id: AgentId,
    ) -> (
        Arc<AgentActorHandle>,
        Arc<LuaActorState>,
        LuaSession,
        flume::Receiver<Envelope>,
    ) {
        let (fallback_tx, parent_rx) = flume::unbounded();
        let parent_raw_tx = parent_raw_tx.unwrap_or(fallback_tx);
        let (chip_raw_tx, relay) = match relay_gate {
            Some(gate) => {
                let (sub_tx, sub_rx) = flume::unbounded();
                (sub_tx, Some((sub_rx, gate)))
            }
            None => (parent_raw_tx.clone(), None),
        };
        let (answer_tx, answer_rx) = flume::unbounded();
        let (input_tx, _input_rx) = flume::unbounded::<String>();
        let (relay_stop_tx, _relay_stop_rx) = flume::bounded::<()>(1);
        let ui_id = "task-1".to_owned();
        let (child_trigger, child_cancel) = CancelToken::new();
        let ctx = AgentContext::from(&stub_ctx(&AgentMode::Build));
        let params = AgentParams {
            agent_id,
            provider,
            model: ctx.model.as_ref().clone(),
            config: ctx.config.clone(),
            tool_output_lines: maki_config::ToolOutputLines::default(),
            permissions: Arc::clone(&ctx.permissions),
            session_id: ctx.session_id.clone(),
            mailbox: None,
            timeouts: ctx.timeouts,
            file_tracker: FileReadTracker::fresh(),
            prompt_slots: Arc::clone(&ctx.prompt_slots),
            modes: Arc::clone(&ctx.modes),
            subagent_cancels: Arc::new(CancelMap::new()),
            ledger: Arc::new(RunLedger::default()),
            registry: Arc::clone(maki_agent::tools::ToolRegistry::global_arc()),
            audience: DEFAULT_SESSION_AUDIENCE,
            question_mode: ctx.question_mode,
            model_policy: Arc::clone(&ctx.model_policy),
            file_write_locks: Arc::clone(&ctx.file_write_locks),
            offload: ctx.offload.clone(),
            managed_turn: None,
        };
        let (parent_cancels, cancel_slot) = {
            let map = Arc::new(CancelMap::new());
            (Arc::clone(&map), map.insert(ui_id.clone(), child_trigger))
        };
        let state = Arc::new(LuaActorState {
            template: OnceLock::new(),
            children: Arc::default(),
            params: OnceLock::from(params),
            custom_system: String::new(),
            tools: RequestTools::default(),
            opts: maki_providers::RequestOptions {
                thinking: ThinkingConfig::Off,
                fast: false,
            },
            mode: AgentMode::Build,
            mode_def: None,
            mcp: None,
            chip_event_tx: EventSender::new(chip_raw_tx, RUN_ID),
            child_cancel,
            answer_rx: Arc::new(AsyncMutex::new(answer_rx)),
            answer_tx: Some(answer_tx),
            ui_id: ui_id.clone(),
            parent_event_tx: EventSender::new(parent_raw_tx.clone(), RUN_ID),
            input_tx,
            cancel: SubagentCancel::new(|| {}),
            parent_agent_id: None,
            parent_is_root: false,
            auto_deliver: true,
            silent: false,
            subagent_info: Arc::new(OnceLock::new()),
            local_tools: LocalTools::default(),
            structured: None,
            name: "probe".to_owned(),
            commit: Arc::new(Mutex::new(None)),
            semaphore,
            turn_permit: Mutex::default(),
            start: Instant::now(),
            execution_started: std::sync::atomic::AtomicBool::new(false),
            history_relayed: std::sync::atomic::AtomicBool::new(false),
            close_notified: std::sync::atomic::AtomicBool::new(false),
            relay_snapshot: Mutex::new(Vec::new()),
        });
        if let Some((sub_rx, gate)) = relay {
            smol::spawn(relay_session_events(
                sub_rx,
                EventSender::new(parent_raw_tx.clone(), RUN_ID),
                Arc::clone(&state.subagent_info),
                None,
                false,
                Some(gate),
            ))
            .detach();
        }
        let (actor, task) = AgentActorHandle::spawn(
            agent_id,
            Vec::new(),
            None,
            Box::new(LuaActorBackend::new(Arc::clone(&state))),
        );
        task.detach();
        let actor = Arc::new(actor);
        let sess = LuaSession {
            id: ui_id,
            agent_id,
            actor: Arc::clone(&actor),
            policy_constraint: actor
                .effective_config()
                .map(|ceiling| session_policy_constraint(Arc::clone(&ctx.model_policy), ceiling)),
            state: Arc::clone(&state),
            control: SessionControl::Unmanaged {
                parent_cancels,
                cancel_slot,
            },
            relay_stop_tx,
            authority: SessionAuthority::Delegated {
                invocation: InvocationGuard {
                    task: Weak::new(),
                    plugin: None,
                },
                turn: None,
                parent: None,
                generation: None,
            },
        };
        (actor, state, sess, parent_rx)
    }

    fn admit(state: &LuaActorState, actor: &AgentActorHandle, message: &str) -> TurnTicket {
        actor
            .admit_turn(
                AgentInput {
                    message: message.to_owned(),
                    mode: AgentMode::Build,
                    images: Vec::new(),
                    preamble: Vec::new(),
                    thinking: state.opts.thinking,
                    fast: state.opts.fast,
                    workflow: false,
                    prompt: None,
                    cancel: None,
                    lease_committer: None,
                },
                None,
                String::new(),
            )
            .unwrap()
    }

    struct MissingPolicyBackend(LuaActorBackend);

    impl ActorBackend for MissingPolicyBackend {
        fn run_turn<'a>(
            &'a mut self,
            history: &'a mut History,
            mut context: TurnContext,
            input: AgentInput,
            work: WorkKind,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = BackendResult> + Send + 'a>>
        {
            assert!(context.managed_turn.is_some());
            context.policy = None;
            self.0.run_turn(history, context, input, work)
        }

        fn run_control<'a>(
            &'a mut self,
            history: &'a mut History,
            context: TurnContext,
            control: &'a maki_agent::ControlWork,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = BackendResult> + Send + 'a>>
        {
            self.0.run_control(history, context, control)
        }

        fn run_compact<'a>(
            &'a mut self,
            history: &'a mut History,
            context: TurnContext,
            instructions: Option<&'a str>,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = BackendResult> + Send + 'a>>
        {
            self.0.run_compact(history, context, instructions)
        }
    }

    #[test]
    fn managed_missing_policy_returns_visible_failure_without_provider_request() {
        smol::block_on(async {
            let provider: Arc<dyn Provider> = Arc::new(HangingProvider);
            let (_unused_actor, state, _session, _events) =
                session_with_provider(provider, None, None);
            let params = state.params.get().unwrap();
            let config = EffectiveAgentConfig::new(
                RunSettings {
                    provider: Arc::clone(&params.provider),
                    model: params.model.clone(),
                    thinking: ThinkingConfig::Off,
                    fast: false,
                    workflow: false,
                },
                AgentMode::Build,
            );
            let manager = maki_agent::AgentManagerHandle::new(Default::default()).unwrap();
            let root = manager
                .create_root_with_config(Some(config), Vec::new(), None, |_| {
                    Ok::<_, String>(Box::new(MissingPolicyBackend(LuaActorBackend::new(
                        Arc::clone(&state),
                    ))) as Box<dyn ActorBackend>)
                })
                .unwrap();
            let ticket = admit(&state, &root.actor().unwrap(), "missing policy");
            let outcome = ticket.wait().await;
            assert!(matches!(outcome, TurnOutcome::Failed { .. }));
            let result = ticket.peek_result().unwrap();
            assert_eq!(
                session_result_error(&result.outcome).as_deref(),
                Some(MANAGED_POLICY_MISSING_ERR)
            );
            manager.shutdown(Duration::from_secs(1)).await;
        });
    }

    #[test]
    fn invocation_guard_rejects_transfer_and_retirement() {
        const WRONG_INVOCATION: &str = "context belongs to another invocation";
        const EXPIRED_INVOCATION: &str = INVOCATION_EXPIRED;
        let lua = Lua::new();
        let scope = crate::runtime::TaskScope::detached(&lua);
        let guard = InvocationGuard::capture(&lua).unwrap();
        {
            let _other = crate::runtime::TaskScope::detached(&lua);
            assert_eq!(guard.validate(&lua).unwrap_err(), WRONG_INVOCATION);
        }
        assert!(guard.validate(&lua).is_ok());
        drop(scope);
        assert_eq!(guard.validate(&lua).unwrap_err(), EXPIRED_INVOCATION);
    }

    #[test]
    fn unmanaged_session_authority_follows_parent_scope() {
        const WRONG_PARENT: &str = "session belongs to another parent scope";
        const NO_ACTIVE_AUTHORITY: &str = NO_TASK_AUTHORITY;
        let lua = Lua::new();
        let parent = Arc::new(CancelMap::new());
        let creator = crate::runtime::TaskScope::detached(&lua);
        crate::runtime::lock_cell(creator.handle()).agent_parent = Some(Arc::clone(&parent));
        let authority = SessionAuthority::capture(
            &lua,
            None,
            &AgentContext::from(&stub_ctx(&AgentMode::Build)),
        )
        .unwrap();
        drop(creator);
        let sibling = crate::runtime::TaskScope::detached(&lua);
        crate::runtime::lock_cell(sibling.handle()).agent_parent = Some(Arc::clone(&parent));
        assert!(authority.validate(&lua, None).is_ok());
        crate::runtime::lock_cell(sibling.handle()).agent_parent = Some(Arc::new(CancelMap::new()));
        assert_eq!(authority.validate(&lua, None).unwrap_err(), WRONG_PARENT);
        drop(sibling);
        assert_eq!(
            authority.validate(&lua, None).unwrap_err(),
            NO_ACTIVE_AUTHORITY
        );
    }

    #[test]
    fn session_policy_allows_refresh_but_rejects_changed_restrictions() {
        let ctx = AgentContext::from(&stub_ctx(&AgentMode::Build));
        let ceiling = EffectiveAgentConfig::new(
            RunSettings {
                provider: Arc::clone(&ctx.provider),
                model: ctx.model.as_ref().clone(),
                thinking: ThinkingConfig::Off,
                fast: false,
                workflow: false,
            },
            AgentMode::Build,
        );
        let policy = ModelPolicy::new(&[ceiling.model.spec()], &[]).unwrap();
        let mut current = ceiling.clone();
        assert!(session_policy_matches(&policy, &ceiling, &current));
        current.settings = RunSettings {
            fast: true,
            ..current.settings.clone()
        };
        assert!(session_policy_matches(&policy, &ceiling, &current));
        current = ceiling.clone();
        current.mode = AgentMode::Plan(std::path::PathBuf::from("/project/plan.md"));
        assert!(session_policy_matches(&policy, &ceiling, &current));
        current = ceiling.clone();
        let mut model = current.settings.model.clone();
        model.id.push_str("-different");
        current.settings = RunSettings {
            model,
            ..current.settings.clone()
        };
        assert!(!session_policy_matches(&policy, &ceiling, &current));
    }

    #[test]
    fn send_returns_during_policy_update() {
        smol::block_on(async {
            const MESSAGE: &str = "queued";
            const REPLY: &str = "accepted";
            let provider: Arc<dyn Provider> =
                Arc::new(StreamOnceProvider::new_replies(vec![canned_reply(REPLY)]));
            let (actor, _state, sess, _rx) = session_with_provider(provider, None, None);
            let reservation = actor.reserve_config_update().unwrap();
            let lua = Lua::new();
            let _scope = crate::runtime::TaskScope::detached(&lua);
            let mut sess = sess;
            sess.authority = SessionAuthority::Delegated {
                invocation: InvocationGuard::capture(&lua).unwrap(),
                turn: None,
                parent: None,
                generation: None,
            };
            let userdata = lua.create_userdata(sess).unwrap();
            assert_eq!(
                send(lua.clone(), userdata.borrow().unwrap(), MESSAGE.into())
                    .await
                    .unwrap(),
                (Some(true), None)
            );
            drop(reservation);
            while actor.snapshot().latest.is_none() {
                smol::future::yield_now().await;
            }
            assert!(matches!(
                actor.snapshot().latest,
                Some(TurnOutcome::Completed { .. })
            ));
        });
    }

    #[test]
    fn lua_prompt_runs_consecutive_turns_through_actor_backend() {
        smol::block_on(async {
            const FIRST_REPLY: &str = "first answer";
            const SECOND_REPLY: &str = "second answer";
            let provider: Arc<dyn Provider> = Arc::new(StreamOnceProvider::new_replies(vec![
                canned_reply_with_usage(FIRST_REPLY, FIRST_USAGE),
                canned_reply_with_usage(SECOND_REPLY, SECOND_USAGE),
            ]));
            let (actor, _state, sess, _events) = session_with_provider(provider, None, None);
            let lua = Lua::new();
            let scope = crate::runtime::TaskScope::detached(&lua);
            let mut sess = sess;
            sess.authority = SessionAuthority::Delegated {
                invocation: InvocationGuard::capture(&lua).unwrap(),
                turn: None,
                parent: None,
                generation: None,
            };
            let userdata = lua.create_userdata(sess).unwrap();

            scope
                .scope_future(async {
                    let (first, first_error) = prompt(
                        lua.clone(),
                        userdata.borrow().unwrap(),
                        "first request".into(),
                        None,
                    )
                    .await
                    .unwrap();
                    assert_eq!(first_error, None);
                    assert_eq!(first.unwrap().get::<String>("text").unwrap(), FIRST_REPLY);

                    let (second, second_error) = prompt(
                        lua.clone(),
                        userdata.borrow().unwrap(),
                        "follow-up request".into(),
                        None,
                    )
                    .await
                    .unwrap();
                    assert_eq!(second_error, None);
                    let second = second.unwrap();
                    assert_eq!(second.get::<String>("text").unwrap(), SECOND_REPLY);
                    assert_eq!(second.get::<u32>("input_tokens").unwrap(), 150);
                    assert_eq!(second.get::<u32>("output_tokens").unwrap(), 30);
                    let Some(TurnOutcome::Completed { usage, .. }) = actor.snapshot().latest else {
                        panic!("expected completed second turn");
                    };
                    assert_eq!(usage, SECOND_USAGE);
                })
                .await;
        });
    }

    #[test]
    fn prompt_survives_policy_update() {
        smol::block_on(async {
            const MESSAGE: &str = "queued prompt";
            const REPLY: &str = "accepted";
            let provider: Arc<dyn Provider> =
                Arc::new(StreamOnceProvider::new_replies(vec![canned_reply(REPLY)]));
            let (actor, _state, sess, _rx) = session_with_provider(provider, None, None);
            let reservation = actor.reserve_config_update().unwrap();
            let lua = Lua::new();
            let scope = crate::runtime::TaskScope::detached(&lua);
            let mut sess = sess;
            sess.authority = SessionAuthority::Delegated {
                invocation: InvocationGuard::capture(&lua).unwrap(),
                turn: None,
                parent: None,
                generation: None,
            };
            let userdata = lua.create_userdata(sess).unwrap();
            let mut pending = Box::pin(scope.scope_future(prompt(
                lua.clone(),
                userdata.borrow().unwrap(),
                MESSAGE.into(),
                None,
            )));
            assert!(
                futures_lite::future::poll_once(&mut pending)
                    .await
                    .is_none()
            );
            drop(reservation);
            let (result, error) = pending.await.unwrap();
            assert_eq!(error, None);
            assert_eq!(result.unwrap().get::<String>("text").unwrap(), REPLY);
        });
    }

    #[test]
    fn close_rejects_new_admission_and_closes_actor() {
        let (actor, state, _sess, _rx) = canned_state(None);
        actor.close();
        assert!(matches!(actor.snapshot().lifecycle, ActorLifecycle::Closed));
        assert!(
            actor
                .admit_turn(
                    AgentInput {
                        message: "rejected".into(),
                        mode: AgentMode::Build,
                        images: Vec::new(),
                        preamble: Vec::new(),
                        thinking: state.opts.thinking,
                        fast: state.opts.fast,
                        workflow: false,
                        prompt: None,
                        cancel: None,
                        lease_committer: None,
                    },
                    None,
                    String::new(),
                )
                .is_err()
        );
    }

    #[test]
    fn queued_only_close_does_not_relay_history() {
        let (actor, state, _sess, parent_rx) = canned_state(None);
        let _ticket = admit(&state, &actor, "queued");
        state.close_with(&actor);
        assert!(
            parent_rx
                .drain()
                .all(|e| !matches!(e.event, AgentEvent::SubagentHistory { .. }))
        );
    }

    #[test]
    fn admitted_and_closed_turn_resolves_as_cancelled() {
        let (actor, state, _sess, _rx) = canned_state(None);
        let ticket = admit(&state, &actor, "waiting");
        let turn_id = ticket.turn_id();
        actor.close();
        let outcome = smol::block_on(ticket.wait());
        assert!(matches!(
            outcome,
            TurnOutcome::Cancelled { turn_id: id, reason: TurnCancellationReason::Closed, .. }
                if id == turn_id
        ));
    }

    #[test]
    fn close_resolves_semaphore_blocked_turn_as_cancelled() {
        let semaphore = Arc::new(async_lock::Semaphore::new(0));
        let (actor, state, _sess, _rx) = canned_state(Some(semaphore));
        let ticket = admit(&state, &actor, "blocked on semaphore");
        let turn_id = ticket.turn_id();
        // The acquire is parked; closing the actor fires the per-turn cancel
        // and the run must settle as Cancelled rather than strand.
        actor.close();
        let outcome = smol::block_on(ticket.wait());
        assert!(matches!(
            outcome,
            TurnOutcome::Cancelled { turn_id: id, reason, .. }
                if id == turn_id && reason == TurnCancellationReason::Closed
        ));
    }

    #[test]
    fn executed_cancelled_turn_relays_history_once() {
        struct StartedProvider(flume::Sender<()>);
        impl Provider for StartedProvider {
            fn stream_message<'a>(
                &'a self,
                _model: &'a Model,
                _messages: &'a [Message],
                _system: &'a str,
                _tools: &'a Value,
                _event_tx: &'a flume::Sender<ProviderEvent>,
                _opts: RequestOptions,
                _session_id: Option<&'a SessionRef>,
            ) -> BoxFuture<'a, Result<StreamResponse, AgentError>> {
                Box::pin(async move {
                    self.0.send(()).unwrap();
                    std::future::pending().await
                })
            }

            fn list_models(&self) -> BoxFuture<'_, Result<Vec<ModelInfo>, AgentError>> {
                Box::pin(async { Ok(Vec::new()) })
            }
        }
        let (started_tx, started_rx) = flume::bounded(1);
        let (actor, state, _sess, parent_rx) =
            session_with_provider(Arc::new(StartedProvider(started_tx)), None, None);
        let ticket = admit(&state, &actor, "cancel me");
        smol::block_on(started_rx.recv_async()).unwrap();
        state.close_with(&actor);
        assert!(matches!(
            smol::block_on(ticket.wait()),
            TurnOutcome::Cancelled {
                reason: TurnCancellationReason::Closed,
                ..
            }
        ));
        smol::block_on(async {
            loop {
                let envelope = parent_rx.recv_async().await.unwrap();
                if matches!(envelope.event, AgentEvent::SubagentHistory { .. }) {
                    break;
                }
            }
        });
        assert!(
            parent_rx
                .drain()
                .all(|envelope| !matches!(envelope.event, AgentEvent::SubagentHistory { .. }))
        );
        state.close_with(&actor);
        assert!(
            parent_rx
                .drain()
                .all(|e| !matches!(e.event, AgentEvent::SubagentHistory { .. }))
        );
    }

    #[test]
    fn repeated_close_and_drop_emit_subagent_closed_once() {
        let provider: Arc<dyn Provider> = Arc::new(StreamOnceProvider::new_replies(vec![
            canned_reply_with_usage("done", FIRST_USAGE),
        ]));
        let (actor, state, sess, parent_rx) = session_with_provider(provider, None, None);
        let ticket = admit(&state, &actor, "run me");
        assert!(matches!(
            smol::block_on(ticket.wait()),
            TurnOutcome::Completed { .. }
        ));
        let _ = parent_rx.drain().count();

        sess.close_controlled();
        sess.close_controlled();
        drop(sess);

        assert_eq!(
            parent_rx
                .drain()
                .filter(|envelope| matches!(envelope.event, AgentEvent::SubagentClosed))
                .count(),
            1,
            "close notification must be emitted once"
        );
    }

    #[test]
    fn normal_close_follows_completed_child_events_through_relay() {
        const REPLY: &str = "ordered reply";

        smol::block_on(async {
            let (release_tx, release_rx) = flume::bounded(1);
            let provider: Arc<dyn Provider> = Arc::new(StreamOnceProvider::new_replies(vec![
                canned_reply_with_usage(REPLY, FIRST_USAGE),
            ]));
            let (actor, state, sess, parent_rx) =
                session_with_provider(provider, None, Some(release_rx));
            let ticket = admit(&state, &actor, "run me");

            assert!(matches!(ticket.wait().await, TurnOutcome::Completed { .. }));
            let before_close = parent_rx.drain().collect::<Vec<_>>();
            assert!(
                before_close
                    .iter()
                    .all(|envelope| matches!(envelope.event, AgentEvent::SubagentHistory { .. }))
            );

            sess.close_controlled();
            sess.close_controlled();
            assert!(matches!(actor.snapshot().lifecycle, ActorLifecycle::Closed));
            assert!(
                actor
                    .admit_turn(
                        AgentInput {
                            message: "rejected".into(),
                            mode: AgentMode::Build,
                            images: Vec::new(),
                            preamble: Vec::new(),
                            thinking: state.opts.thinking,
                            fast: state.opts.fast,
                            workflow: false,
                            prompt: None,
                            cancel: None,
                            lease_committer: None,
                        },
                        None,
                        String::new()
                    )
                    .is_err()
            );
            assert!(
                parent_rx.is_empty(),
                "normal close must not bypass the relay"
            );

            release_tx.send(()).unwrap();
            let mut forwarded = Vec::new();
            loop {
                let envelope = parent_rx.recv_async().await.unwrap();
                let closed = matches!(envelope.event, AgentEvent::SubagentClosed);
                forwarded.push(envelope);
                if closed {
                    break;
                }
            }

            let text = forwarded.iter().find_map(|envelope| match &envelope.event {
                AgentEvent::TextDelta { text } => Some(text.as_str()),
                AgentEvent::TurnComplete(turn) => turn.message.content.iter().find_map(|block| {
                    if let ContentBlock::Text { text } = block {
                        Some(text.as_str())
                    } else {
                        None
                    }
                }),
                _ => None,
            });
            assert_eq!(text, Some(REPLY));
            assert!(forwarded.iter().any(|envelope| matches!(
                envelope.event,
                AgentEvent::TurnOutcome(TurnOutcome::Completed { .. })
            )));
            assert!(matches!(
                forwarded.last().map(|envelope| &envelope.event),
                Some(AgentEvent::SubagentClosed)
            ));
            assert_eq!(
                forwarded
                    .iter()
                    .filter(|envelope| matches!(envelope.event, AgentEvent::SubagentClosed))
                    .count(),
                1
            );

            drop(sess);
            smol::future::yield_now().await;
            assert!(parent_rx.is_empty(), "drop must not publish a second close");
        });
    }

    #[test]
    fn close_retires_only_its_own_cancel_slot() {
        // Two sessions share one `ui_id`; retiring ours must leave the sibling
        // registration (and its entry) untouched.
        let map = Arc::new(CancelMap::new());
        let (own_trigger, _own_token) = CancelToken::new();
        let own_slot = map.insert("task-1".to_owned(), own_trigger);
        let (sibling_trigger, sibling_token) = CancelToken::new();
        let sibling_slot = map.insert("task-1".to_owned(), sibling_trigger);

        map.retire(&"task-1".to_owned(), own_slot);
        assert!(
            !sibling_token.is_cancelled(),
            "sibling must not be cancelled by our retire"
        );
        // A later CancelSubagent under the same id must still reach the sibling.
        map.cancel_or_precancel("task-1".to_owned());
        assert!(
            sibling_token.is_cancelled(),
            "sibling must still be cancellable after our retire"
        );
        map.retire(&"task-1".to_owned(), sibling_slot);
        map.remove(&"task-1".to_owned());
    }

    #[test]
    fn reusable_actor_retains_parent_turn_event_sender_after_end_turn() {
        let provider: Arc<dyn Provider> =
            Arc::new(StreamOnceProvider::new_replies(vec![canned_reply(
                "child done",
            )]));
        let (turn_tx, turn_rx) = flume::unbounded();
        let (actor, state, session, _relayed) =
            session_with_provider_and_parent(provider, None, None, Some(turn_tx.clone()));
        let ticket = admit(&state, &actor, "run me");
        let outcome = smol::block_on(ticket.wait());
        assert!(matches!(outcome, TurnOutcome::Completed { .. }));
        assert!(matches!(actor.snapshot().lifecycle, ActorLifecycle::Open));

        let root_outcome = TurnOutcome::completed(
            AgentId::generate(),
            TurnId::generate(),
            TokenUsage::default(),
            1,
            DoneReason::EndTurn,
        );
        turn_tx
            .send(Envelope {
                event: AgentEvent::TextDelta {
                    text: "visible completion".into(),
                },
                subagent: None,
                run_id: RUN_ID,
            })
            .unwrap();
        turn_tx
            .send(Envelope {
                event: AgentEvent::TurnOutcome(root_outcome),
                subagent: None,
                run_id: RUN_ID,
            })
            .unwrap();
        drop(turn_tx);

        let (visible_tx, visible_rx) = flume::unbounded();
        let mut terminal_task = smol::spawn(async move {
            let mut terminal = None;
            while let Ok(envelope) = turn_rx.recv_async().await {
                if matches!(envelope.event, AgentEvent::TurnOutcome(_)) {
                    terminal = Some(envelope);
                } else {
                    visible_tx.send(envelope).unwrap();
                }
            }
            terminal
        });
        smol::block_on(async {
            while !matches!(
                visible_rx.recv_async().await.unwrap(),
                Envelope {
                    event: AgentEvent::TextDelta { ref text },
                    subagent: None,
                    run_id: RUN_ID,
                } if text == "visible completion"
            ) {}
            assert!(
                futures_lite::future::poll_once(&mut terminal_task)
                    .await
                    .is_none(),
                "sender retention must reproduce the old disconnect wait"
            );
        });
        drop(terminal_task);
        drop(session);
    }

    #[test]
    fn executed_completed_turns_accumulate_usage() {
        let provider: Arc<dyn Provider> = Arc::new(StreamOnceProvider::new_replies(vec![
            canned_reply_with_usage("the answer", FIRST_USAGE),
            canned_reply_with_usage("the follow-up", SECOND_USAGE),
        ]));
        let (actor, state, sess, parent_rx) = session_with_provider(provider, None, None);
        let first = admit(&state, &actor, "run me");
        let first_id = first.turn_id();
        let first_outcome = smol::block_on(first.wait());
        assert!(matches!(
            first_outcome,
            TurnOutcome::Completed { turn_id, .. } if turn_id == first_id
        ));
        let first_result = first.peek_result().unwrap();
        assert_eq!(first_result.text, "the answer");
        assert_eq!(session_result_error(&first_result.outcome), None);

        let second = admit(&state, &actor, "follow up");
        let second_id = second.turn_id();
        let second_outcome = smol::block_on(second.wait());
        assert!(matches!(
            second_outcome,
            TurnOutcome::Completed { turn_id, .. } if turn_id == second_id
        ));
        assert_eq!(actor.snapshot().cumulative_usage, CUMULATIVE_USAGE);
        drop(sess);
        assert!(
            parent_rx
                .drain()
                .any(|e| matches!(e.event, AgentEvent::SubagentHistory { .. })),
            "completed run must surface history"
        );
    }

    #[test]
    fn status_stays_running_while_turn_is_active_or_queued() {
        assert!(actor_is_busy(ActorStatus::Running(TurnId::generate()), 0));
        assert!(actor_is_busy(ActorStatus::Idle, 1));
        assert!(!actor_is_busy(ActorStatus::Idle, 0));
    }

    #[test]
    fn lua_backend_prepares_mode_at_admission() {
        let provider: Arc<dyn Provider> = Arc::new(StreamOnceProvider::new_replies(vec![]));
        let (actor, state, session, _events) = session_with_provider(provider, None, None);
        let modes = &state.params.get().unwrap().modes;
        modes
            .define(maki_agent::ModeDefSpec {
                name: "build".into(),
                system_prompt: Some("before".into()),
                ..Default::default()
            })
            .unwrap();
        let backend = LuaActorBackend::new(Arc::clone(&state));
        let prepare = backend.admission_preparation().unwrap();
        let input = AgentInput::from_defaults(
            "queued".into(),
            AgentMode::Build,
            Vec::new(),
            Default::default(),
        );
        let admitted = prepare(&input, &input.mode, None);
        let params = state.params.get().unwrap();
        let config = EffectiveAgentConfig::new(
            RunSettings {
                provider: Arc::clone(&params.provider),
                model: params.model.clone(),
                thinking: ThinkingConfig::Off,
                fast: false,
                workflow: false,
            },
            AgentMode::Build,
        )
        .with_mode_def(admitted.mode_def.as_deref().cloned());
        modes
            .define(maki_agent::ModeDefSpec {
                name: "build".into(),
                system_prompt: Some("after".into()),
                ..Default::default()
            })
            .unwrap();
        assert_eq!(
            prepare(&input, &input.mode, Some(&config))
                .mode_def
                .as_deref(),
            admitted.mode_def.as_deref(),
        );
        assert_eq!(
            admitted.mode_def.as_ref().unwrap().system_prompt.as_deref(),
            Some("before")
        );
        assert_eq!(
            prepare(&input, &input.mode, None)
                .mode_def
                .as_ref()
                .unwrap()
                .system_prompt
                .as_deref(),
            Some("after")
        );
        drop(session);
        actor.shutdown();
    }

    #[test]
    fn failed_turn_keeps_session_open_for_later_turn() {
        let fail = Arc::new(FailOnceProvider::default());
        let provider: Arc<dyn Provider> = fail.clone();
        let (actor, state, sess, parent_rx) = session_with_provider(provider, None, None);

        let ticket = admit(&state, &actor, "fail this turn");
        let turn_id = ticket.turn_id();
        let failed = smol::block_on(ticket.wait());
        assert!(matches!(
            failed,
            TurnOutcome::Failed { turn_id: id, .. } if id == turn_id
        ));
        assert!(
            session_result_error(&ticket.peek_result().unwrap().outcome).is_some(),
            "failed turn must present an error"
        );

        let recovered = admit(&state, &actor, "reuse the same session");
        let recovered_id = recovered.turn_id();
        let outcome = smol::block_on(recovered.wait());
        assert!(matches!(
            outcome,
            TurnOutcome::Completed { turn_id: id, .. } if id == recovered_id
        ));
        assert!(matches!(actor.snapshot().lifecycle, ActorLifecycle::Open));
        drop(sess);
        assert_eq!(
            parent_rx
                .drain()
                .filter(|e| matches!(e.event, AgentEvent::SubagentHistory { .. }))
                .count(),
            2,
            "each executed turn relays history once"
        );
        state.close_with(&actor);
        assert!(
            parent_rx
                .drain()
                .all(|e| !matches!(e.event, AgentEvent::SubagentHistory { .. }))
        );
    }

    #[test]
    fn status_done_result_keeps_cumulative_compatibility_usage() {
        let lua = Lua::new();
        let cumulative = tokens(150, 30);
        let (result, error) = build_prompt_result(
            &lua,
            TurnResult {
                outcome: TurnOutcome::completed(
                    AgentId::generate(),
                    TurnId::generate(),
                    tokens(10, 2),
                    1,
                    maki_agent::DoneReason::EndTurn,
                ),
                text: "done".into(),
                output: Vec::new(),
                provenance: Default::default(),
                duration_ms: 0,
            },
            cumulative,
        )
        .unwrap();

        assert!(error.is_none());
        let result = result.unwrap();
        assert_eq!(result.get::<u32>("input_tokens").unwrap(), 150);
        assert_eq!(result.get::<u32>("output_tokens").unwrap(), 30);
    }

    #[test]
    fn prompt_result_preserves_partial_text_and_captured_output_with_error() {
        const FAILURE: &str = "provider failed";
        const PARTIAL: &str = "partial output";
        let lua = Lua::new();
        let (result, error) = build_prompt_result(
            &lua,
            TurnResult {
                outcome: TurnOutcome::failed(
                    AgentId::generate(),
                    TurnId::generate(),
                    TokenUsage::default(),
                    1,
                    maki_agent::TurnFailure {
                        kind: maki_agent::TurnFailureKind::Provider,
                        diagnostic: FAILURE.into(),
                        user_message: FAILURE.into(),
                        retryable: false,
                    },
                ),
                text: PARTIAL.into(),
                output: vec![json!({ "answer": 42 })],
                provenance: Default::default(),
                duration_ms: 0,
            },
            TokenUsage::default(),
        )
        .unwrap();

        let result = result.unwrap();
        assert_eq!(result.get::<String>("text").unwrap(), PARTIAL);
        assert_eq!(
            result
                .get::<Table>("captured")
                .unwrap()
                .get::<u32>("answer")
                .unwrap(),
            42
        );
        assert_eq!(error.as_deref(), Some(FAILURE));
    }

    #[test]
    fn prompt_empty_closed_result_returns_only_error() {
        let lua = Lua::new();
        let agent_id = AgentId::generate();
        let turn_id = TurnId::generate();
        let outcome = TurnOutcome::cancelled(
            agent_id,
            turn_id,
            TokenUsage::default(),
            0,
            TurnCancellationReason::Closed,
        );
        let usage = outcome.usage();
        let (result, error) = build_prompt_result(
            &lua,
            TurnResult {
                outcome,
                text: String::new(),
                output: Vec::new(),
                provenance: Default::default(),
                duration_ms: 0,
            },
            usage,
        )
        .unwrap();
        assert!(result.is_none());
        assert_eq!(error.as_deref(), Some(CANCELLED_MSG));
    }

    struct FailOnceProvider {
        calls: AtomicUsize,
    }

    impl Default for FailOnceProvider {
        fn default() -> Self {
            Self {
                calls: AtomicUsize::new(0),
            }
        }
    }

    impl Provider for FailOnceProvider {
        fn stream_message<'a>(
            &'a self,
            _model: &'a Model,
            _messages: &'a [Message],
            _system: &'a str,
            _tools: &'a Value,
            _event_tx: &'a flume::Sender<ProviderEvent>,
            _opts: RequestOptions,
            _session_id: Option<&'a SessionRef>,
        ) -> BoxFuture<'a, Result<StreamResponse, AgentError>> {
            Box::pin(async move {
                if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
                    Err(AgentError::Config {
                        message: "provider failed".into(),
                    })
                } else {
                    Ok(canned_reply("recovered on the same session"))
                }
            })
        }

        fn list_models(&self) -> BoxFuture<'_, Result<Vec<ModelInfo>, AgentError>> {
            Box::pin(async { unimplemented!() })
        }
    }

    #[test]
    fn relay_session_events_reports_live_usage_and_done_total() {
        let (sub_tx, sub_rx) = flume::unbounded();
        let (parent_raw_tx, parent_rx) = flume::unbounded();
        let subagent_info = Arc::new(OnceLock::new());
        subagent_info
            .set(SubagentInfo {
                agent_id: AgentId::generate(),
                parent_agent_id: None,
                parent_is_root: true,
                auto_deliver: true,
                parent_tool_use_id: PARENT_ID.into(),
                name: "research".into(),
                prompt: None,
                model: None,
                opts: None,
                answer_tx: None,
                input_tx: None,
                cancel: None,
            })
            .unwrap();
        let (live_tx, live_rx) = flume::unbounded();
        let outcome = TurnOutcome::completed(
            AgentId::generate(),
            TurnId::generate(),
            DONE_USAGE,
            2,
            DoneReason::EndTurn,
        );

        for event in [
            turn(tokens(100, 20), 0.25),
            turn(tokens(50, 10), 0.5),
            AgentEvent::TurnOutcome(outcome.clone()),
        ] {
            sub_tx.send(envelope(event)).unwrap();
        }
        drop(sub_tx);

        smol::block_on(relay_session_events(
            sub_rx,
            EventSender::new(parent_raw_tx, RUN_ID),
            subagent_info,
            Some(live_tx),
            false,
            None,
        ));

        let live = live_rx
            .drain()
            .map(|event| match event {
                ToolLive::Usage(usage) => usage,
                _ => panic!("relay must only publish usage"),
            })
            .collect::<Vec<_>>();
        let expected = [
            tokens(100, 20).format_sum_cost(Some(0.25)),
            tokens(50, 10).format_sum_cost(Some(0.75)),
        ];
        assert_eq!(live, expected);

        let forwarded = parent_rx.drain().collect::<Vec<_>>();
        assert_eq!(forwarded.len(), expected.len() + 1);
        assert!(forwarded.iter().all(|envelope| {
            envelope
                .subagent
                .as_ref()
                .is_some_and(|info| info.parent_tool_use_id == PARENT_ID)
        }));
        assert!(matches!(
            &forwarded.last().unwrap().event,
            AgentEvent::TurnOutcome(forwarded_outcome) if forwarded_outcome == &outcome
        ));
    }

    #[test]
    fn relay_session_events_preserves_nested_subagent_identity_and_history() {
        let (sub_tx, sub_rx) = flume::unbounded();
        let (parent_raw_tx, parent_rx) = flume::unbounded();
        let parent_info = Arc::new(OnceLock::new());
        parent_info
            .set(SubagentInfo {
                agent_id: AgentId::generate(),
                parent_agent_id: None,
                parent_is_root: true,
                auto_deliver: true,
                parent_tool_use_id: "parent".into(),
                name: "parent".into(),
                prompt: None,
                model: None,
                opts: None,
                answer_tx: None,
                input_tx: None,
                cancel: None,
            })
            .unwrap();
        let grandchild_id = AgentId::generate();
        let grandchild_info = SubagentInfo {
            agent_id: grandchild_id,
            parent_agent_id: parent_info.get().map(|info| info.agent_id),
            parent_is_root: false,
            auto_deliver: true,
            parent_tool_use_id: "grandchild".into(),
            name: "grandchild".into(),
            prompt: None,
            model: None,
            opts: None,
            answer_tx: None,
            input_tx: None,
            cancel: None,
        };
        sub_tx
            .send(Envelope {
                event: AgentEvent::SubagentHistory {
                    tool_use_id: "grandchild".into(),
                    messages: Vec::new(),
                },
                subagent: Some(grandchild_info),
                run_id: RUN_ID,
            })
            .unwrap();
        drop(sub_tx);

        smol::block_on(relay_session_events(
            sub_rx,
            EventSender::new(parent_raw_tx, RUN_ID),
            parent_info,
            None,
            false,
            None,
        ));

        let forwarded = parent_rx.recv().unwrap();
        assert!(matches!(
            forwarded.event,
            AgentEvent::SubagentHistory { .. }
        ));
        assert_eq!(forwarded.subagent.unwrap().agent_id, grandchild_id);
    }
}
