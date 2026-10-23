//! `maki.agent` exposes subagent primitives to Lua plugins. Policy (retries,
//! validation, concurrency) lives in the task plugin, not here.

use std::collections::{HashMap, HashSet};
use std::pin::pin;
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::{Duration, Instant};

mod handles;
mod structured_output;
use structured_output::StructuredOutput;

pub(crate) use handles::{
    AGENT_DOCS, LuaAgent, LuaAgentRef, REF_DOCS, SUBSCRIPTION_DOCS, TURN_DOCS, current,
    focused_agent_ref, lookup_agent, origin_json, result_json, selection_json,
};
use handles::{
    get__doc, get__register, list__doc, list__register, root__doc, root__register, spawn__doc,
    spawn__register,
};

use async_lock::Mutex as AsyncMutex;
use futures::future::{Either, select};
use maki_agent::actor::{
    ActorBackend, AdmissionPreparation, AgentContextSnapshot, BackendResult, EffectiveAgentConfig,
    ModeCeiling, PreparedModel, PreparedReadiness, TurnContext, WorkKind,
};
use maki_agent::agent::{PromptSlotSource, TurnSetup, build_mode_system_prompt, tool_dispatch};
use maki_agent::cancel::CancelMap;
use maki_agent::config_change::prepare_model;
use maki_agent::headless::default_model_preparer;
use maki_agent::session_coordinator::ModelPreparer;
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
    Agent, AgentActorHandle, AgentEvent, AgentId, AgentInput, AgentMode, AgentParams, AgentRef,
    AgentRunParams, CurrentManagedTurn, Envelope, EventSender, History, HistorySnapshot,
    McpSession, ModeDef, Post, RunSettings, SubagentCancel, SubagentInfo, ToolDoneEvent,
    TurnCancellationReason, TurnFailure, TurnFailureKind, TurnId, TurnOutcome,
};
use maki_config::ToolKey;
use maki_lua_macro::{lua_fn, lua_table};
use maki_providers::model::ModelTier;
use maki_providers::provider;
use maki_providers::{Model, ModelError, ModelSelection, RequestOptions, TokenUsage, add_cost};
use maki_storage::id::MakiId;
use mlua::{Function, IntoLuaMulti, Lua, Result as LuaResult, Table, Value as LuaValue};
use serde_json::Value as JsonValue;
use tracing::{info, warn};

use crate::agent_errors::{
    BLANK_POST, INVOCATION_EXPIRED, MANAGED_AUTHORITY_INACTIVE, NO_TASK_AUTHORITY,
    POST_TARGET_NOT_STARTED, POST_TARGET_SILENT, SHOW_SILENT_AGENT, SHOW_TOOL_AGENT,
    SHOW_UNTRUSTED_PARENT, STRUCTURED_OUTPUT_CONFLICT,
};
use crate::api::tool::{audiences_to_lua, parse_audience};
use crate::api::ui::buf::BufHandle;
use crate::api::util::convert::{json_to_lua, lua_to_json, lua_tool_result};
use crate::api::util::ctx::{AgentContext, LuaCtx};
use crate::api::util::pair::{Pair, err_pair, try_pair};
use crate::orchestration::{OrchestrationServices, TrustedContext};
use crate::runtime::Request;

const MANAGED_POLICY_MISSING_ERR: &str = "managed agent turn is missing its admitted configuration";
const COMPACT_PARAMS_MISSING_ERR: &str = "compaction requires initialized agent parameters";
const COMPACT_CONFIG_MISSING_ERR: &str =
    "compaction was admitted without an effective agent configuration";
const INVALID_SHOW_OPTION: &str = "show must be a boolean or string";
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
    "show",
    "silent",
    "system",
    "tools",
];

pub type SessionProviderPreparer =
    Arc<dyn Fn(Model) -> Result<(Model, Arc<dyn provider::Provider>), String> + Send + Sync>;

/// Resolve a child's model and provider, optionally reusing the parent's provider.
async fn build_session_provider(
    model_spec: &Option<String>,
    inherit_provider: bool,
    agent_ctx: &AgentContext,
    prepare: Option<SessionProviderPreparer>,
) -> Result<(Model, Arc<dyn provider::Provider>), String> {
    let Some(spec) = model_spec.as_deref().filter(|_| !inherit_provider) else {
        return Ok((
            Model::clone(&agent_ctx.model),
            Arc::clone(&agent_ctx.provider),
        ));
    };
    let preparer: ModelPreparer = match prepare {
        Some(prepare) => Arc::new(move |model| {
            let prepared = prepare(model)
                .map(|(model, provider)| PreparedModel { model, provider })
                .map_err(Arc::from);
            Box::pin(async move { prepared })
        }),
        None => default_model_preparer(agent_ctx.timeouts),
    };
    let prepared = prepare_model(spec, &agent_ctx.model_policy, &preparer)
        .await
        .map_err(|error| error.to_string())?;
    Ok((prepared.model, prepared.provider))
}

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
    template: OnceLock<ToolContext>,
    params: OnceLock<AgentParams>,
    prompt_slots: PromptSlotSource,
    custom_system: String,
    tools: RequestTools,
    opts: RequestOptions,
    mcp: Option<McpSession>,
    chip_event_tx: EventSender,
    /// `None` when the parent has no answer channel either (print), so the
    /// child's asks are refused at once like its parent's.
    answer_rx: Option<Arc<AsyncMutex<flume::Receiver<String>>>>,
    answer_tx: Option<flume::Sender<String>>,
    ui_id: String,
    parent_event_tx: EventSender,
    cancel: SubagentCancel,
    parent_agent_id: Option<AgentId>,
    auto_deliver: bool,
    silent: bool,
    subagent_info: Arc<OnceLock<SubagentInfo>>,
    local_tools: LocalTools,
    structured: Option<Arc<StructuredOutput>>,
    name: String,
    commit: Arc<Mutex<Option<JsonValue>>>,
    start: Instant,
    /// Prevents close/drop from duplicating the latest terminal relay.
    history_relayed: std::sync::atomic::AtomicBool,
    close_notified: std::sync::atomic::AtomicBool,
    relay_snapshot: Mutex<HistorySnapshot>,
}

impl LuaActorState {
    fn init_subagent_info(&self, first_message: &str) {
        if self.subagent_info.get().is_none() {
            let _ = self.subagent_info.set(SubagentInfo {
                agent_id: self
                    .params
                    .get()
                    .expect("session parameters must be initialized before admission")
                    .agent_id,
                parent_agent_id: self.parent_agent_id,
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
                cancel: Some(self.cancel.clone()),
            });
        }
    }

    fn relay_history(&self, history: &History) {
        self.history_relayed
            .store(false, std::sync::atomic::Ordering::Release);
        *self.relay_snapshot.lock().unwrap() = history.snapshot().clone();
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
        let snapshot = self.relay_snapshot.lock().unwrap().clone();
        if snapshot.messages.is_empty() {
            self.history_relayed
                .store(false, std::sync::atomic::Ordering::SeqCst);
            return;
        }
        let event = AgentEvent::SubagentHistory {
            tool_use_id: self.ui_id.clone(),
            history_epoch: snapshot.epoch,
            messages: snapshot.messages.as_ref().clone(),
        };
        let _ = match self.subagent_info.get() {
            Some(subagent) => self.parent_event_tx.send_envelope(Envelope {
                event,
                subagent: Some(subagent.clone()),
                identity: self.parent_event_tx.identity(),
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
            identity: self.parent_event_tx.identity(),
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
}

/// The actor backend for one Lua session. Thin `&mut self` shell over the
/// shared [`LuaActorState`] so the boxed `run_turn` future stays `Send`.
struct LuaActorBackend {
    state: Arc<LuaActorState>,
    offload: Option<Arc<OffloadStore>>,
    completion: Option<Arc<dyn OrchestrationServices>>,
}

impl LuaActorBackend {
    fn new(state: Arc<LuaActorState>) -> Self {
        Self {
            state,
            offload: None,
            completion: None,
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

    fn admission_preparation(&self) -> Option<AdmissionPreparation> {
        let state = Arc::clone(&self.state);
        Some(Arc::new(move |input, config| {
            let params = state
                .params
                .get()
                .expect("session parameters initialized before admission");
            TurnSetup {
                registry: Arc::clone(&params.registry),
                local_tools: Arc::clone(&state.local_tools),
                mcp: state.mcp.clone(),
                slots: state.prompt_slots.clone(),
                modes: Arc::clone(&params.modes),
                instructions: false,
            }
            .snapshot(input, config)
        }))
    }

    fn prepared_readiness(&self) -> Option<PreparedReadiness> {
        Some(TurnSetup::readiness())
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
            // The finalizer uses the same relay sink after sealing the result.
            let event_tx = state
                .chip_event_tx
                .clone()
                .with_output(context.output.clone());
            context.output.set_terminal_sender(event_tx.clone());
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
            let mut params = state
                .params
                .get()
                .expect("session parameters must be initialized before admission")
                .clone();
            let prompt_inputs = context
                .admission
                .as_ref()
                .and_then(|admission| admission.prompt_inputs.as_ref());
            if let Some(inputs) = prompt_inputs {
                match context
                    .cancel
                    .race(inputs.resolve(&mut input, state.mcp.as_ref()))
                    .await
                {
                    Ok(Ok(slots)) => params.prompt_slots = slots,
                    Ok(Err(error)) => {
                        return BackendResult::EnteredRun(TurnOutcome::failed(
                            context.agent_id,
                            turn_id,
                            TokenUsage::default(),
                            0,
                            TurnFailure {
                                kind: TurnFailureKind::Internal,
                                diagnostic: error.to_string(),
                                user_message: error.user_message(),
                                retryable: false,
                            },
                        ));
                    }
                    Err(_) => {
                        return BackendResult::EnteredRun(TurnOutcome::cancelled(
                            context.agent_id,
                            turn_id,
                            TokenUsage::default(),
                            0,
                            context
                                .cancel_reason
                                .reason()
                                .unwrap_or(TurnCancellationReason::User),
                        ));
                    }
                }
            }
            let mut system = context
                .admission
                .as_ref()
                .and_then(|admission| {
                    let inputs = prompt_inputs?;
                    let definition = admission
                        .mode_def
                        .as_deref()
                        .cloned()
                        .unwrap_or_else(|| ModeDef::default_for(maki_agent::ModeId::Build));
                    Some(build_mode_system_prompt(
                        &env_vars_for(&inputs.cwd),
                        &definition,
                        context
                            .policy
                            .as_ref()
                            .map_or(&AgentMode::Build, |policy| &policy.mode),
                    ))
                })
                .unwrap_or_default();
            if !state.custom_system.is_empty() {
                if !system.is_empty() {
                    system.push_str("\n\n");
                }
                system.push_str(&state.custom_system);
            }
            let loaded = prompt_inputs
                .map(|inputs| inputs.instructions.loaded.clone())
                .unwrap_or_default();
            let mut agent = Agent::for_actor_turn(
                params,
                AgentRunParams {
                    history,
                    system,
                    event_tx,
                    tools: state.tools.clone(),
                },
                &context,
                loaded,
            )
            .with_user_response_rx(state.answer_rx.clone())
            .with_mcp(state.mcp.clone())
            .with_local_tools(Arc::clone(&state.local_tools))
            .with_local_tool_capture(Arc::clone(&state.commit));
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
            let mut outcome = agent.run(turn_id, input).await;
            drop(agent);

            if let Some(structured) = &state.structured {
                structured.finish_turn();
            }
            let captured = state.commit.lock().unwrap().take();
            if state.structured.is_none()
                && let Some(value) = &captured
            {
                context.output.append_output(value.clone());
            }
            if let Some(completion) = &self.completion
                && let Some(current) = &context.managed_turn
            {
                let result = match current.agent_ref() {
                    Ok(target) => {
                        completion
                            .complete_turn(&target, history.snapshot().clone())
                            .await
                    }
                    Err(error) => Err(error.to_string()),
                };
                if let Err(error) = result {
                    warn!(%error, %turn_id, "child history was not checkpointed");
                    if matches!(outcome, TurnOutcome::Completed { .. }) {
                        outcome = outcome.checkpoint_failed(error);
                    }
                }
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
        history: &'a mut History,
        context: TurnContext,
        instructions: Option<&'a str>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = BackendResult> + Send + 'a>> {
        Box::pin(async move {
            let state = &*self.state;
            let event_tx = state
                .chip_event_tx
                .clone()
                .with_output(context.output.clone());
            let Some(params) = state.params.get() else {
                let _ = event_tx.send(AgentEvent::ControlError {
                    message: COMPACT_PARAMS_MISSING_ERR.into(),
                });
                return BackendResult::ControlFailed;
            };
            let Some(settings) = context.policy.as_deref().map(|config| &config.settings) else {
                let _ = event_tx.send(AgentEvent::ControlError {
                    message: COMPACT_CONFIG_MISSING_ERR.into(),
                });
                return BackendResult::ControlFailed;
            };
            let (provider, model) = maki_agent::agent::resolve_compaction_model(
                &settings.provider,
                &settings.model,
                params.timeouts,
                &params.model_policy,
            );
            match maki_agent::agent::compact(
                &*provider,
                &model,
                history,
                &event_tx,
                &context.cancel,
                &params.config,
                instructions,
                params.session_id.as_ref(),
            )
            .await
            {
                Ok(()) => {
                    state.relay_history(history);
                    BackendResult::CompactDone
                }
                Err(error) => {
                    warn!(error = %error, "Lua agent compaction failed");
                    let _ = event_tx.send(AgentEvent::ControlError {
                        message: error.user_message(),
                    });
                    BackendResult::ControlFailed
                }
            }
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

fn dispatch_ctx<'a>(lua: &Lua, ctx: &'a LuaCtx, method: &str) -> Result<&'a AgentContext, String> {
    ctx.validate_origin(lua)?;
    ctx.agent()
        .ok_or_else(|| ctx.cap_err(&format!("maki.agent.{method}")))
}

/// Runtime resolution touches no actor state, so a trusted invocation may
/// resolve against its pinned target's template where dispatch would not.
fn resolve_ctx<'a>(lua: &Lua, ctx: &'a LuaCtx) -> Result<&'a AgentContext, String> {
    ctx.validate_origin(lua)?;
    ctx.model_context()
        .ok_or_else(|| ctx.cap_err("maki.agent.resolve_model"))
}

/// Forwards subagent events to the parent, stamped with the subagent identity.
async fn relay_session_events(
    sub_rx: flume::Receiver<Envelope>,
    parent_tx: EventSender,
    subagent_info: Arc<OnceLock<SubagentInfo>>,
    live_sink: Option<flume::Sender<ToolLive>>,
    silent: bool,
) {
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
/// The returned table has the same fields as `maki.model.info`.
///
/// @param ctx LuaCtx Agent context.
/// @param opts table? Optional fields:
///   `tier` (string?) - target tier, one of `"weak"`, `"medium"`, `"strong"`.
///     Clamped to the parent tier so you cannot escalate.
///   `spec` (string?) - exact model spec string, e.g. `"claude-3-5-haiku-20241022"`.
///     Takes precedence over `tier`.
/// @return (table?, string?) Model table on success, or `(nil, err)` on failure.
/// @example
/// local model, err = maki.agent.resolve_model(ctx, { tier = "weak" })
/// if err then error(err) end
/// print(model.spec, model.tier)
#[lua_fn]
async fn resolve_model(
    lua: Lua,
    ctx: mlua::UserDataRef<LuaCtx>,
    opts: Option<Table>,
) -> LuaResult<Pair<Table>> {
    let agent = try_pair!(resolve_ctx(&lua, &ctx));
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
    Ok((Some(crate::api::model::model_table(&lua, &model)?), None))
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
    let agent = try_pair!(dispatch_ctx(&lua, &ctx, "system_prompt"));
    let slots = Arc::clone(&agent.prompt_slots);
    let vars = env_vars_for(&agent.cwd);
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
/// straight into `maki.agent.spawn()` or use it to inspect what tools are
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

    let vars = env_vars_for(&agent.cwd);
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
    agent_id: Option<AgentId>,
}

impl SessionParent {
    fn resolve(lua: &Lua, ctx: &LuaCtx) -> Result<Self, String> {
        ctx.validate_origin(lua)?;
        let trusted = ctx.trusted(lua).ok().cloned();
        let context = Self::agent_context(lua, ctx, trusted.as_ref())?;
        let managed_turn = match (
            &context.managed_turn,
            crate::runtime::current_managed_turn(lua),
        ) {
            (Some(expected), Some(active)) if expected.same_authority(&active) => Some(active),
            (None, None) => None,
            _ => return Err(MANAGED_AUTHORITY_INACTIVE.into()),
        };
        if managed_turn.is_none() && trusted.is_none() {
            return Err(MANAGED_AUTHORITY_INACTIVE.into());
        }
        let agent_id = managed_turn
            .as_ref()
            .map(|current| current.agent_id())
            .or_else(|| trusted.as_ref().map(|trusted| trusted.target.id()));
        Ok(Self {
            context,
            trusted,
            managed_turn,
            agent_id,
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
                template.cwd = config.context.cwd().to_path_buf();
            }
            return Ok(AgentContext::from(&template));
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
        Ok(())
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
    /// `Some` posts a row for the child in its parent's chat, with an
    /// optional description.
    show: Option<Option<String>>,
}

impl SessionOptions {
    fn parse(lua: &Lua, opts: &Table, parent: &SessionParent) -> LuaResult<Result<Self, String>> {
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
            None => parent.context.audience,
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
                    "{INVALID_SHOW_OPTION}, got {}",
                    other.type_name()
                )));
            }
        };
        Ok(Ok(Self {
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
            show,
        }))
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
            None => match inherited_tool_definitions(&parent.context, model, &parent_allowed) {
                Ok(definitions) => definitions,
                Err(error) => return Ok(Err(error)),
            },
        };
        let structured = match opts.get::<Option<LuaValue>>("output_schema")? {
            Some(value) => match StructuredOutput::compile(lua_to_json(lua, &value)?) {
                Ok(structured) => Some(structured),
                Err(error) => return Ok(Err(error)),
            },
            None => None,
        };
        let mut local = if options.tools.is_none() {
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
        {
            let initial =
                RequestTools::assembled(definitions.clone(), &parent.context.config, model);
            let mut child_tools = parent_tools;
            child_tools.audience = options.audience;
            child_tools.tool_filter = Arc::clone(initial.filter());
            let mut allowed: HashSet<_> = tool_dispatch::callable(&child_tools)
                .into_iter()
                .map(|tool| tool.name)
                .filter(|name| parent_allowed.contains(name))
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
        let mode = parent.context.mode.clone();
        Self {
            model,
            provider,
            opts,
            mode,
        }
    }

    fn mode_definition(&self, parent: &SessionParent) -> ModeDef {
        parent
            .context
            .mode_def
            .as_deref()
            .cloned()
            .unwrap_or_else(|| parent.context.modes.current(&self.mode))
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
        template.workflow = parent.context.workflow;
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

    fn initial_config(&self, parent: &SessionParent) -> EffectiveAgentConfig {
        EffectiveAgentConfig::new(
            RunSettings {
                provider: Arc::clone(&self.provider),
                model: self.model.clone(),
                thinking: self.opts.thinking,
                fast: self.opts.fast,
                workflow: parent.context.workflow,
            },
            self.mode.clone(),
        )
        .with_mode_def(Some(self.mode_definition(parent)))
        .with_mode_authority(if let Some(trusted) = &parent.trusted {
            trusted
                .target
                .effective_config()
                .ok()
                .flatten()
                .and_then(|config| config.mode_authority())
                .or_else(|| {
                    trusted
                        .target
                        .snapshot()
                        .ok()
                        .filter(|node| node.parent_id.is_none())
                        .map(|_| ModeCeiling::default())
                })
                .unwrap_or_else(|| {
                    ModeCeiling::from_mode(&parent.context.mode, parent.context.mode_def.as_deref())
                })
        } else {
            ModeCeiling::from_mode(&self.mode, Some(&self.mode_definition(parent)))
        })
        .with_context(
            parent
                .trusted
                .as_ref()
                .and_then(|trusted| trusted.target.effective_config().ok().flatten())
                .map(|config| config.context.clone())
                .unwrap_or_else(|| AgentContextSnapshot::new(parent.context.cwd.clone())),
        )
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
        registry: Arc::clone(&context.registry),
        audience: options.audience,
        question_mode: context.question_mode,
        model_policy: Arc::clone(&context.model_policy),
        file_write_locks: Arc::clone(&context.file_write_locks),
        offload: context.offload.clone(),
        managed_turn: None,
    }
}

/// Spawn under the active managed turn or trusted target.
fn spawn_session_actor(
    lua: &Lua,
    parent: &SessionParent,
    state: &Arc<LuaActorState>,
    initial_config: EffectiveAgentConfig,
    params: impl FnOnce(AgentId) -> AgentParams,
) -> Result<(AgentActorHandle, AgentRef), String> {
    let backend = || -> Box<dyn ActorBackend> {
        let mut backend = LuaActorBackend::new(Arc::clone(state));
        backend.offload = parent.context.offload.clone();
        backend.completion = crate::orchestration::services(lua);
        Box::new(backend)
    };
    let init_params = |agent_id| {
        assert!(
            state.params.set(params(agent_id)).is_ok(),
            "session parameters must only be initialized once"
        );
    };
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
    if let Err(error) = actor.configure_parent_delivery(state.auto_deliver && !state.silent) {
        let _ = child.close_subtree();
        return Err(error.to_string());
    }
    Ok((actor, child))
}

fn register_session(
    lua: &Lua,
    sess: LuaSession,
    child_template: ToolContext,
) -> Result<LuaSession, String> {
    let _ = sess.state.template.set(child_template.clone());
    if let Some(service) = crate::orchestration::services(lua)
        && let Err(error) = service.register_child_target(
            crate::orchestration::TrustedTarget {
                target: sess.agent.clone(),
                template: child_template,
            },
            sess.state.ui_id.clone(),
        )
    {
        sess.close_controlled();
        return Err(error.to_string());
    }
    Ok(sess)
}

async fn build_session(
    lua: Lua,
    ctx: mlua::UserDataRef<LuaCtx>,
    opts: Table,
) -> LuaResult<Pair<LuaSession>> {
    let parent = try_pair!(SessionParent::resolve(&lua, &ctx));
    let mut options = try_pair!(SessionOptions::parse(&lua, &opts, &parent)?);

    let required_output = opts.get::<Option<LuaValue>>("output_schema")?.is_some();
    let initial_mode_def = parent
        .context
        .mode_def
        .as_deref()
        .cloned()
        .unwrap_or_else(|| parent.context.modes.current(&parent.context.mode));
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
    let initial_config = child.initial_config(&parent);
    try_pair!(validate_required_output_mode(
        tools.structured.is_some(),
        initial_config.mode_def.as_ref(),
    ));

    let (sub_tx, sub_rx) = flume::unbounded::<Envelope>();
    let (answer_tx, answer_rx) = match parent.context.user_response_rx {
        Some(_) => {
            let (answer_tx, answer_rx) = flume::unbounded::<String>();
            (Some(answer_tx), Some(Arc::new(AsyncMutex::new(answer_rx))))
        }
        None => (None, None),
    };
    let subagent_info: Arc<OnceLock<SubagentInfo>> = Arc::new(OnceLock::new());
    let ui_id = parent
        .context
        .tool_use_id
        .clone()
        .unwrap_or_else(|| format!("session-{}", MakiId::generate()));
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
        params: OnceLock::new(),
        prompt_slots: lua
            .app_data_ref::<crate::runtime::RequestTx>()
            .map(|requests| {
                let requests = requests.0.clone();
                PromptSlotSource::new(move || {
                    let (reply, slots) = flume::bounded(1);
                    let _ = requests.send(Request::CollectPromptSlots { reply });
                    slots
                })
            })
            .unwrap_or_default(),
        custom_system: options.system.take().unwrap_or_default(),
        tools: tools.request,
        opts: child.opts,
        mcp: parent
            .context
            .mcp
            .as_ref()
            .filter(|_| options.mcp)
            .map(McpSession::fresh),
        chip_event_tx: EventSender::with_identity(sub_tx, parent.context.event_tx.identity()),
        answer_rx,
        answer_tx,
        ui_id,
        parent_event_tx: parent.context.event_tx.clone(),
        cancel,
        parent_agent_id: parent.agent_id,
        auto_deliver: options.auto_deliver,
        silent: options.silent,
        subagent_info: Arc::clone(&subagent_info),
        local_tools: Arc::new(tools.local),
        structured: tools.structured,
        name: options.name.clone(),
        commit,
        start: Instant::now(),
        history_relayed: std::sync::atomic::AtomicBool::new(false),
        close_notified: std::sync::atomic::AtomicBool::new(false),
        relay_snapshot: Mutex::default(),
    });
    let (actor, agent) = try_pair!(spawn_session_actor(
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
    ))
    .detach();

    *cancel_actor.lock().unwrap() = Some(actor.clone());

    let sess = LuaSession {
        agent_id: actor.agent_id(),
        actor: Arc::new(actor),
        state,
        agent,
    };
    let session = try_pair!(register_session(&lua, sess, child_template));
    if let (Some(description), Some(trusted)) = (options.show, &parent.trusted) {
        let label = if options.name.is_empty() {
            UNNAMED_AGENT_LABEL
        } else {
            &options.name
        };
        let post = Post {
            text: description.map_or_else(|| label.to_owned(), |text| format!("{label}: {text}")),
            level: Default::default(),
            agent: Some(session.state.ui_id.clone()),
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
            identity: state.parent_event_tx.identity(),
        })
        .map_err(|error| error.to_string())
}

lua_table! {
    /// Subagent primitives for plugins that need to talk to an LLM.
    ///
    /// This module gives you the building blocks: resolve which model to use,
    /// build a system prompt, list available tools, call a tool directly, or
    /// spawn an agent with its own conversation history.
    ///
    /// Policy like retries, validation, and concurrency lives in the calling
    /// plugin, not here.
    ///
    /// ```lua
    /// local tools = maki.agent.tools(ctx, { audience = "general_sub" })
    /// local agent = assert(maki.agent.spawn(ctx, {
    ///   system = "You are a helpful assistant.",
    ///   tools = tools,
    /// }))
    /// local r = assert(assert(agent:send(ctx, "Hello!", { when = "idle" })):wait(ctx))
    /// print(r.text)
    /// agent:close(ctx)
    /// ```
    "maki.agent" => pub(crate) fn create_agent_table(), DOCS [
        resolve_model, system_prompt, tools, callable_tools, call_tool, permission_prompt,
        root, list, get, spawn,
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
        {
            let cell = crate::runtime::lock_cell(&task);
            if !cell.is_live() {
                return Err(INVOCATION_EXPIRED.into());
            }
        }
        if !crate::runtime::same_invocation(lua, &task) {
            return Err("context belongs to another invocation".into());
        }
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

pub(crate) fn close_plugin_agents(lua: &Lua, plugin: &str) {
    handles::close_plugin_agents(lua, plugin);
}

struct LuaSession {
    agent_id: AgentId,
    actor: Arc<AgentActorHandle>,
    state: Arc<LuaActorState>,
    agent: AgentRef,
}

impl LuaSession {
    fn close_controlled(&self) {
        self.state.notify_closed_ordered();
        let _ = self.agent.close_subtree();
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

struct LuaTaskOwner {
    owner_id: Option<AgentId>,
    plugin: Arc<str>,
    generation: u64,
    owner: Arc<crate::orchestration::PluginAuthority>,
    target: AgentRef,
}

impl mlua::UserData for LuaTaskOwner {
    fn add_methods<M: mlua::UserDataMethods<Self>>(methods: &mut M) {
        // A managed task whose child has closed, for instance because its owner
        // was closed, can be dropped without anyone's authority.
        methods.add_method("_maki_retired", |_, this, ()| {
            Ok(this.target.validate_live().is_err())
        });
        methods.add_method("_maki_can_control", |lua, this, ()| {
            if InvocationGuard::capture(lua).is_err()
                || !crate::orchestration::holds_plugin_grant(
                    lua,
                    &this.plugin,
                    this.generation,
                    &this.owner,
                )
            {
                return Ok(false);
            }
            let Some(current) = crate::runtime::current_managed_turn(lua) else {
                return Ok(false);
            };
            if !current.manager().same_manager(&this.target.manager())
                || current.validate_active().is_err()
                || (current.agent_id() != this.target.id()
                    && current.validate_descendant(this.target.id()).is_err())
            {
                return Ok(false);
            }
            Ok(match this.owner_id {
                None => true,
                Some(owner_id) if owner_id == current.agent_id() => true,
                Some(owner_id) => current.validate_descendant(owner_id).is_ok(),
            })
        });
    }
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
    let values = f.call::<mlua::MultiValue>(arg);
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

    use maki_agent::actor::ActorLifecycle;
    use maki_agent::cancel::CancelToken;
    use maki_agent::tools::test_support::stub_ctx;
    use maki_agent::{
        AgentId, AgentParams, DoneReason, TurnCompleteEvent, TurnId, TurnOutcome, TurnResult,
        TurnTicket,
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
            identity: None,
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

    #[test_case::test_case(false; "missing_policy_rejected")]
    #[test_case::test_case(true; "child_history_compacted")]
    fn lua_compaction_uses_common_backend(configured: bool) {
        smol::block_on(async {
            const SUMMARY: &str = "compacted child history";
            let provider = Arc::new(StreamOnceProvider::new_replies(vec![canned_reply(SUMMARY)]));
            let (actor, state, session, events) = session_with_provider(provider, None);
            let params = state.params.get().unwrap();
            let policy = configured.then(|| {
                Arc::new(EffectiveAgentConfig::new(
                    RunSettings {
                        provider: Arc::clone(&params.provider),
                        model: params.model.clone(),
                        fast: false,
                        workflow: false,
                        thinking: Default::default(),
                    },
                    AgentMode::Build,
                ))
            });
            let (_, cancel_reason) = maki_agent::ReasonedCancelToken::new();
            let context = TurnContext {
                agent_id: params.agent_id,
                turn_id: None,
                output: Default::default(),
                provenance: Default::default(),
                cancel: CancelToken::none(),
                cancel_reason,
                work: maki_agent::ActorWorkIdentity::new(
                    params.agent_id,
                    maki_agent::ActorWorkKind::Turn {
                        operation_id: 0,
                        turn_id: maki_agent::TurnId::generate(),
                    },
                    0,
                ),
                generation: 0,
                policy,
                interrupt: None,
                inbox: None,
                managed_turn: None,
                admission: None,
            };
            let mut history = History::new(vec![Message::user("original child prompt".into())]);
            let original = serde_json::to_value(history.as_slice()).unwrap();
            let result = LuaActorBackend::new(Arc::clone(&state))
                .run_compact(&mut history, context, None)
                .await;
            if configured {
                assert!(matches!(result, BackendResult::CompactDone));
                assert!(history.as_slice().iter().any(|message| message.content.iter().any(|block| matches!(block, ContentBlock::Text { text } if text.contains(SUMMARY)))));
                assert!(
                    events.drain().any(|envelope| matches!(
                        envelope.event,
                        AgentEvent::ControlComplete { .. }
                    ))
                );
            } else {
                assert!(matches!(result, BackendResult::ControlFailed));
                assert_eq!(serde_json::to_value(history.as_slice()).unwrap(), original);
                assert!(
                    events
                        .drain()
                        .any(|envelope| matches!(envelope.event, AgentEvent::ControlError { .. }))
                );
            }
            actor.close();
            drop(session);
        });
    }

    /// A provider that never resolves, so a running turn parks until its
    /// cancel fires. Deterministic for close/cancel tests.
    pub(super) struct HangingProvider;

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

    pub(super) struct BackendFixture {
        pub(super) actor: Arc<AgentActorHandle>,
        pub(super) state: Arc<LuaActorState>,
    }

    impl BackendFixture {
        pub(super) fn close_controlled(&self) {
            self.state.notify_closed_ordered();
            self.actor.close();
        }
    }

    impl Drop for BackendFixture {
        fn drop(&mut self) {
            self.close_controlled();
        }
    }

    /// A backend fixture whose provider parks until cancellation.
    fn canned_state() -> (
        Arc<AgentActorHandle>,
        Arc<LuaActorState>,
        BackendFixture,
        flume::Receiver<Envelope>,
    ) {
        session_with_provider(Arc::new(HangingProvider), None)
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
        relay_gate: Option<flume::Receiver<()>>,
    ) -> (
        Arc<AgentActorHandle>,
        Arc<LuaActorState>,
        BackendFixture,
        flume::Receiver<Envelope>,
    ) {
        session_with_provider_and_parent(provider, relay_gate, None)
    }

    pub(super) fn session_with_provider_for_agent(
        provider: Arc<dyn Provider>,
        agent_id: AgentId,
    ) -> (
        Arc<AgentActorHandle>,
        Arc<LuaActorState>,
        BackendFixture,
        flume::Receiver<Envelope>,
    ) {
        session_fixture(provider, None, None, agent_id)
    }

    fn session_with_provider_and_parent(
        provider: Arc<dyn Provider>,
        relay_gate: Option<flume::Receiver<()>>,
        parent_raw_tx: Option<flume::Sender<Envelope>>,
    ) -> (
        Arc<AgentActorHandle>,
        Arc<LuaActorState>,
        BackendFixture,
        flume::Receiver<Envelope>,
    ) {
        session_fixture(provider, relay_gate, parent_raw_tx, AgentId::generate())
    }

    fn session_fixture(
        provider: Arc<dyn Provider>,
        relay_gate: Option<flume::Receiver<()>>,
        parent_raw_tx: Option<flume::Sender<Envelope>>,
        agent_id: AgentId,
    ) -> (
        Arc<AgentActorHandle>,
        Arc<LuaActorState>,
        BackendFixture,
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
        let ui_id = "task-1".to_owned();
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
            registry: Arc::clone(maki_agent::tools::ToolRegistry::global_arc()),
            audience: ToolAudience::GENERAL_SUB,
            question_mode: ctx.question_mode,
            model_policy: Arc::clone(&ctx.model_policy),
            file_write_locks: Arc::clone(&ctx.file_write_locks),
            offload: ctx.offload.clone(),
            managed_turn: None,
        };
        let state = Arc::new(LuaActorState {
            template: OnceLock::from(stub_ctx(&AgentMode::Build)),
            params: OnceLock::from(params),
            prompt_slots: PromptSlotSource::default(),
            custom_system: String::new(),
            tools: RequestTools::default(),
            opts: maki_providers::RequestOptions {
                thinking: ThinkingConfig::Off,
                fast: false,
            },
            mcp: None,
            chip_event_tx: EventSender::new(chip_raw_tx),
            answer_rx: Some(Arc::new(AsyncMutex::new(answer_rx))),
            answer_tx: Some(answer_tx),
            ui_id: ui_id.clone(),
            parent_event_tx: EventSender::with_identity(
                parent_raw_tx.clone(),
                Some(maki_agent::ActorWorkIdentity::new(
                    agent_id,
                    maki_agent::ActorWorkKind::Turn {
                        operation_id: 0,
                        turn_id: maki_agent::TurnId::generate(),
                    },
                    RUN_ID,
                )),
            ),
            cancel: SubagentCancel::new(|| {}),
            parent_agent_id: None,
            auto_deliver: true,
            silent: false,
            subagent_info: Arc::new(OnceLock::new()),
            local_tools: LocalTools::default(),
            structured: None,
            name: "probe".to_owned(),
            commit: Arc::new(Mutex::new(None)),
            start: Instant::now(),
            history_relayed: std::sync::atomic::AtomicBool::new(false),
            close_notified: std::sync::atomic::AtomicBool::new(false),
            relay_snapshot: Mutex::default(),
        });
        if let Some((sub_rx, gate)) = relay {
            let relay = relay_session_events(
                sub_rx,
                EventSender::with_identity(
                    parent_raw_tx.clone(),
                    Some(maki_agent::ActorWorkIdentity::new(
                        agent_id,
                        maki_agent::ActorWorkKind::Turn {
                            operation_id: 0,
                            turn_id: maki_agent::TurnId::generate(),
                        },
                        RUN_ID,
                    )),
                ),
                Arc::clone(&state.subagent_info),
                None,
                false,
            );
            smol::spawn(async move {
                let _ = gate.recv_async().await;
                relay.await;
            })
            .detach();
        }
        let settings = {
            let params = state.params.get().unwrap();
            RunSettings {
                provider: Arc::clone(&params.provider),
                model: params.model.clone(),
                fast: false,
                workflow: false,
                thinking: Default::default(),
            }
        };
        let (actor, task) = AgentActorHandle::spawn(
            agent_id,
            Vec::new(),
            None,
            Box::new(LuaActorBackend::new(Arc::clone(&state))),
        );
        task.detach();
        actor
            .initialize_config(
                EffectiveAgentConfig::new(settings, AgentMode::Build)
                    .with_context(AgentContextSnapshot::new(ctx.cwd.clone())),
            )
            .unwrap();
        let actor = Arc::new(actor);
        let sess = BackendFixture {
            actor: Arc::clone(&actor),
            state: Arc::clone(&state),
        };
        (actor, state, sess, parent_rx)
    }

    fn admit(actor: &AgentActorHandle, message: &str) -> TurnTicket {
        actor
            .admit_turn(
                AgentInput {
                    message: message.to_owned(),
                    images: Vec::new(),
                    preamble: Vec::new(),
                    prompt: None,
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
            let (_unused_actor, state, _session, _events) = session_with_provider(provider, None);
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
            let ticket = admit(&root.actor().unwrap(), "missing policy");
            let outcome = ticket.wait().await;
            assert!(matches!(outcome, TurnOutcome::Failed { .. }));
            let result = ticket.peek_result().unwrap();
            assert!(
                matches!(&result.outcome, TurnOutcome::Failed { failure, .. }
                if failure.diagnostic == MANAGED_POLICY_MISSING_ERR)
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
    fn send_returns_during_policy_update() {
        smol::block_on(async {
            const MESSAGE: &str = "queued";
            const REPLY: &str = "accepted";
            let provider: Arc<dyn Provider> =
                Arc::new(StreamOnceProvider::new_replies(vec![canned_reply(REPLY)]));
            let (actor, _, _fixture, _rx) = session_with_provider(provider, None);
            let reservation = actor.reserve_prepared_operation(None).unwrap();
            let ticket = admit(&actor, MESSAGE);
            assert!(ticket.peek().is_none());
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
            let (actor, _, _fixture, _events) = session_with_provider(provider, None);
            let first = admit(&actor, "first request").wait_result().await;
            assert_eq!(first.text, FIRST_REPLY);
            assert_eq!(first.outcome.usage(), FIRST_USAGE);
            let second = admit(&actor, "follow-up request").wait_result().await;
            assert_eq!(second.text, SECOND_REPLY);
            assert_eq!(second.outcome.usage(), SECOND_USAGE);
            assert_eq!(actor.snapshot().cumulative_usage, CUMULATIVE_USAGE);
        });
    }

    #[test]
    fn prompt_survives_policy_update() {
        smol::block_on(async {
            const MESSAGE: &str = "queued prompt";
            const REPLY: &str = "accepted";
            let provider: Arc<dyn Provider> =
                Arc::new(StreamOnceProvider::new_replies(vec![canned_reply(REPLY)]));
            let (actor, _, _fixture, _rx) = session_with_provider(provider, None);
            let reservation = actor.reserve_prepared_operation(None).unwrap();
            let ticket = admit(&actor, MESSAGE);
            let mut pending = Box::pin(ticket.wait_result());
            assert!(
                futures_lite::future::poll_once(&mut pending)
                    .await
                    .is_none()
            );
            drop(reservation);
            let result = pending.await;
            assert_eq!(result.text, REPLY);
            assert!(matches!(result.outcome, TurnOutcome::Completed { .. }));
        });
    }

    #[test]
    fn close_rejects_new_admission_and_closes_actor() {
        let (actor, _, _sess, _rx) = canned_state();
        actor.close();
        assert!(matches!(actor.snapshot().lifecycle, ActorLifecycle::Closed));
        assert!(
            actor
                .admit_turn(
                    AgentInput {
                        message: "rejected".into(),
                        images: Vec::new(),
                        preamble: Vec::new(),
                        prompt: None,
                    },
                    None,
                    String::new(),
                )
                .is_err()
        );
    }

    #[test]
    fn queued_only_close_does_not_relay_history() {
        let (actor, _, sess, parent_rx) = canned_state();
        let _ticket = admit(&actor, "queued");
        sess.close_controlled();
        assert!(
            parent_rx
                .drain()
                .all(|e| !matches!(e.event, AgentEvent::SubagentHistory { .. }))
        );
    }

    #[test]
    fn admitted_and_closed_turn_resolves_as_cancelled() {
        let (actor, _, _sess, _rx) = canned_state();
        let ticket = admit(&actor, "waiting");
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
        let (actor, state, sess, parent_rx) =
            session_with_provider(Arc::new(StartedProvider(started_tx)), None);
        let ticket = admit(&actor, "cancel me");
        smol::block_on(started_rx.recv_async()).unwrap();
        sess.close_controlled();
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
                if let AgentEvent::SubagentHistory { history_epoch, .. } = envelope.event {
                    assert_eq!(history_epoch, state.relay_snapshot.lock().unwrap().epoch);
                    break;
                }
            }
        });
        assert!(
            parent_rx
                .drain()
                .all(|envelope| !matches!(envelope.event, AgentEvent::SubagentHistory { .. }))
        );
        sess.close_controlled();
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
        let (actor, _, sess, parent_rx) = session_with_provider(provider, None);
        let ticket = admit(&actor, "run me");
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
            let (actor, _, sess, parent_rx) = session_with_provider(provider, Some(release_rx));
            let ticket = admit(&actor, "run me");

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
                            images: Vec::new(),
                            preamble: Vec::new(),
                            prompt: None,
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
        let (actor, _, session, _relayed) =
            session_with_provider_and_parent(provider, None, Some(turn_tx.clone()));
        let ticket = admit(&actor, "run me");
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
                identity: None,
            })
            .unwrap();
        turn_tx
            .send(Envelope {
                event: AgentEvent::TurnOutcome(root_outcome),
                subagent: None,
                identity: None,
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
                    identity: None,
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
        let (actor, _, sess, parent_rx) = session_with_provider(provider, None);
        let first = admit(&actor, "run me");
        let first_id = first.turn_id();
        let first_outcome = smol::block_on(first.wait());
        assert!(matches!(
            first_outcome,
            TurnOutcome::Completed { turn_id, .. } if turn_id == first_id
        ));
        let first_result = first.peek_result().unwrap();
        assert_eq!(first_result.text, "the answer");
        assert!(matches!(
            first_result.outcome,
            TurnOutcome::Completed { .. }
        ));

        let second = admit(&actor, "follow up");
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
        let (actor, _, sess, _rx) = canned_state();
        assert!(actor.snapshot().active_turn.is_none());
        assert_eq!(actor.snapshot().queued, 0);

        let _active = admit(&actor, "active");
        smol::block_on(async {
            while actor.snapshot().active_turn.is_none() {
                smol::future::yield_now().await;
            }
        });
        assert_eq!(actor.snapshot().queued, 0);

        let _queued = admit(&actor, "queued");
        assert!(actor.snapshot().active_turn.is_some());
        assert_eq!(
            actor.snapshot().queued,
            1,
            "a second admission waits behind the running turn"
        );
        drop(sess);
    }

    #[test]
    fn lua_backend_prepares_mode_at_admission() {
        let provider: Arc<dyn Provider> = Arc::new(StreamOnceProvider::new_replies(vec![]));
        let (actor, state, session, _events) = session_with_provider(provider, None);
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
        let input = AgentInput::new("queued".into(), Vec::new());
        let admitted = prepare(&input, None);
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
            prepare(&input, Some(&config)).mode_def.as_deref(),
            admitted.mode_def.as_deref(),
        );
        assert_eq!(
            admitted.mode_def.as_ref().unwrap().system_prompt.as_deref(),
            Some("before")
        );
        assert_eq!(
            prepare(&input, None)
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
    fn lua_admission_pins_cwd_and_tool_inheritance() {
        let directory = tempfile::tempdir().unwrap();
        let provider: Arc<dyn Provider> = Arc::new(StreamOnceProvider::new_replies(vec![]));
        let (actor, state, session, _events) = session_with_provider(provider, None);
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
        .with_context(AgentContextSnapshot::new(directory.path().into()));
        let input = AgentInput::new("queued".into(), Vec::new());
        let prepare = LuaActorBackend::new(Arc::clone(&state))
            .admission_preparation()
            .unwrap();
        let admitted = prepare(&input, Some(&config));
        let inputs = admitted.prompt_inputs.as_ref().unwrap();
        assert_eq!(inputs.cwd, directory.path());
        let mut history = History::new(Vec::new());
        let agent = Agent::new(
            params.clone(),
            AgentRunParams {
                history: &mut history,
                system: String::new(),
                event_tx: state.chip_event_tx.clone(),
                tools: state.tools.clone(),
            },
        )
        .with_admission(Some(admitted.clone()));
        let inherited = AgentContext::from(&agent.tool_context().unwrap());
        assert_eq!(inherited.cwd, directory.path());
        drop(session);
        actor.shutdown();
    }

    #[test]
    fn failed_turn_keeps_session_open_for_later_turn() {
        let fail = Arc::new(FailOnceProvider::default());
        let provider: Arc<dyn Provider> = fail.clone();
        let (actor, _, sess, parent_rx) = session_with_provider(provider, None);

        let ticket = admit(&actor, "fail this turn");
        let turn_id = ticket.turn_id();
        let failed = smol::block_on(ticket.wait());
        assert!(matches!(
            failed,
            TurnOutcome::Failed { turn_id: id, .. } if id == turn_id
        ));
        let TurnOutcome::Failed { failure, .. } = ticket.peek_result().unwrap().outcome else {
            panic!("failed turn must retain a failure outcome");
        };
        assert!(
            !failure.diagnostic.is_empty(),
            "failed turn must present an error"
        );

        let recovered = admit(&actor, "reuse the same session");
        let recovered_id = recovered.turn_id();
        let outcome = smol::block_on(recovered.wait());
        assert!(matches!(
            outcome,
            TurnOutcome::Completed { turn_id: id, .. } if id == recovered_id
        ));
        assert!(matches!(actor.snapshot().lifecycle, ActorLifecycle::Open));
        sess.close_controlled();
        assert_eq!(
            parent_rx
                .drain()
                .filter(|e| matches!(e.event, AgentEvent::SubagentHistory { .. }))
                .count(),
            2,
            "each executed turn relays history once"
        );
        sess.close_controlled();
        assert!(
            parent_rx
                .drain()
                .all(|e| !matches!(e.event, AgentEvent::SubagentHistory { .. }))
        );
    }

    #[test]
    fn prompt_result_preserves_partial_text_and_captured_output_with_error() {
        const FAILURE: &str = "provider failed";
        const PARTIAL: &str = "partial output";
        let lua = Lua::new();
        let result = handles::projection::result_table(
            &lua,
            &TurnResult {
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
        )
        .unwrap();
        assert_eq!(result.get::<String>("text").unwrap(), PARTIAL);
        assert_eq!(
            result
                .get::<Table>("captured")
                .unwrap()
                .get::<u32>("answer")
                .unwrap(),
            42
        );
        assert_eq!(result.get::<String>("status").unwrap(), "failed");
        assert_eq!(result.get::<String>("error").unwrap(), FAILURE);
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
                parent_tool_use_id: PARENT_ID.into(),
                name: "research".into(),
                prompt: None,
                model: None,
                opts: None,
                answer_tx: None,
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
            EventSender::new(parent_raw_tx),
            subagent_info,
            Some(live_tx),
            false,
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
                parent_tool_use_id: "parent".into(),
                name: "parent".into(),
                prompt: None,
                model: None,
                opts: None,
                answer_tx: None,
                cancel: None,
            })
            .unwrap();
        let grandchild_id = AgentId::generate();
        let grandchild_info = SubagentInfo {
            agent_id: grandchild_id,
            parent_agent_id: parent_info.get().map(|info| info.agent_id),
            parent_tool_use_id: "grandchild".into(),
            name: "grandchild".into(),
            prompt: None,
            model: None,
            opts: None,
            answer_tx: None,
            cancel: None,
        };
        sub_tx
            .send(Envelope {
                event: AgentEvent::SubagentHistory {
                    tool_use_id: "grandchild".into(),
                    history_epoch: 0,
                    messages: Vec::new(),
                },
                subagent: Some(grandchild_info),
                identity: None,
            })
            .unwrap();
        drop(sub_tx);

        smol::block_on(relay_session_events(
            sub_rx,
            EventSender::new(parent_raw_tx),
            parent_info,
            None,
            false,
        ));

        let forwarded = parent_rx.recv().unwrap();
        assert!(matches!(
            forwarded.event,
            AgentEvent::SubagentHistory { .. }
        ));
        assert_eq!(forwarded.subagent.unwrap().agent_id, grandchild_id);
    }
}
