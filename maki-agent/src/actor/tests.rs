//! Deterministic actor-core tests. No sleeps: every test drives the actor
//! through a scripted backend and waits on exact outcome tickets, which
//! resolve the moment the actor retains the outcome.

#[path = "config_tests.rs"]
mod config_tests;

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};

use maki_providers::provider::{BoxFuture, Provider};
use maki_providers::{
    ContentBlock, ImageMediaType, ImageSource, Message, Model, ProviderEvent, RequestOptions, Role,
    StreamResponse, TokenUsage,
};
use maki_storage::id::SessionRef;
use serde_json::Value;

fn test_image() -> ImageSource {
    ImageSource::new(ImageMediaType::Png, Arc::from("dGVzdA=="))
}

use super::queue::{ActorQueue, QueueProjection};
use super::types::{
    ActorLifecycle, ActorStatus, BackendResult, ControlWork, RootWork, TurnContext, WorkKind,
};
use super::{
    ActorBackend, ActorError, ActorEvent, ActorWork, AgentActorHandle, ChildDelivery,
    DeliveryStatus, TurnAdmission,
};
use crate::tools::DescriptionContext;
use crate::tools::{
    ExecFuture, HeaderFuture, HeaderResult, ParseError, Tool, ToolAudience, ToolInvocation,
    ToolSource,
};
use crate::types::{AgentId, DoneReason, EventSender, TurnCancellationReason, TurnId, TurnOutcome};
use crate::{AgentEvent, AgentInput, AgentMode, ExtractedCommand, SharedMessages};
use std::borrow::Cow;

const COMPLETION_TIMEOUT: Duration = Duration::from_secs(5);
const CONDITION_TIMEOUT: &str = "condition never became true";
const BILLED_RESPONSE: &str = "billed response";
const BILLED_COST: f64 = 0.007;
const BILLED_LIST_COST: f64 = 0.009;

struct PolicyTestProvider;

impl Provider for PolicyTestProvider {
    fn stream_message<'a>(
        &'a self,
        _: &'a Model,
        _: &'a [Message],
        _: &'a str,
        _: &'a Value,
        _: &'a flume::Sender<ProviderEvent>,
        _: RequestOptions,
        _: Option<&'a SessionRef>,
    ) -> BoxFuture<'a, Result<StreamResponse, maki_providers::AgentError>> {
        Box::pin(async { unreachable!() })
    }

    fn list_models(
        &self,
    ) -> BoxFuture<'_, Result<Vec<maki_providers::ModelInfo>, maki_providers::AgentError>> {
        Box::pin(async { Ok(Vec::new()) })
    }
}

fn fast_change(fast: bool) -> super::ConfigChange {
    super::ConfigChange::Patch(super::ConfigPatch {
        fast: Some(fast),
        ..Default::default()
    })
}

pub(super) fn policy(fast: bool) -> crate::RunSettings {
    let mut model = Model::from_spec("anthropic/claude-sonnet-4-20250514").unwrap();
    model.supports_fast_override = Some(maki_providers::model::FastSupport::Supported);
    crate::RunSettings {
        provider: Arc::new(PolicyTestProvider),
        model,
        fast,
        workflow: false,
        thinking: Default::default(),
    }
}

/// Shared observations the scripted backend records for the test to assert.
#[derive(Default)]
struct ScriptedState {
    runs: Mutex<Vec<(WorkKind, String)>>,
    root_metadata: Mutex<Vec<(u64, bool, String, usize)>>,
    folds: Mutex<Vec<String>>,
    controls: Mutex<Vec<String>>,
    compacts: AtomicU32,
    compact_policies: Mutex<Vec<(Option<bool>, u64)>>,
    compact_modes: Mutex<Vec<Option<AgentMode>>>,
    entered: AtomicU32,
    policies: Mutex<Vec<(String, Option<bool>, u64)>>,
    modes: Mutex<Vec<(String, Option<AgentMode>)>>,
    admissions: Mutex<Vec<(String, Option<String>, bool)>>,
}

/// Scripted backend with an optional gate and a scripted outcome sequence.
/// `run_turn` always polls the interrupt source exactly once (before the
/// gate), which makes the root-fold and interrupt-extraction tests
/// deterministic.
struct ScriptedBackend {
    state: Arc<ScriptedState>,
    gate: Option<Arc<Gate>>,
    outcomes: Mutex<Vec<BackendResult>>,
    preparation: Option<super::AdmissionPreparation>,
    readiness: Option<super::PreparedReadiness>,
    root_preparation_error: Option<Arc<dyn Fn(u64, String) + Send + Sync>>,
}

struct Gate {
    opened: std::sync::atomic::AtomicBool,
    event: event_listener::Event,
}

impl Gate {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            opened: std::sync::atomic::AtomicBool::new(false),
            event: event_listener::Event::new(),
        })
    }

    async fn wait(&self) {
        loop {
            if self.opened.load(Ordering::Acquire) {
                return;
            }
            let listener = self.event.listen();
            if self.opened.load(Ordering::Acquire) {
                return;
            }
            listener.await;
        }
    }

    fn open(&self) {
        self.opened.store(true, Ordering::Release);
        self.event.notify(usize::MAX);
    }
}

impl ScriptedBackend {
    fn new() -> Self {
        Self {
            state: Arc::new(ScriptedState::default()),
            gate: None,
            outcomes: Mutex::new(Vec::new()),
            preparation: None,
            readiness: None,
            root_preparation_error: None,
        }
    }

    fn gated(gate: Arc<Gate>) -> Self {
        Self {
            gate: Some(gate),
            ..Self::new()
        }
    }

    fn failing_setup() -> Self {
        let backend = Self::new();
        *backend.outcomes.lock().unwrap() = vec![BackendResult::SetupFailed {
            agent_id: AgentId::generate(),
            turn_id: crate::types::TurnId::generate(),
        }];
        backend
    }

    fn completed(context: &TurnContext, usage: TokenUsage) -> BackendResult {
        BackendResult::EnteredRun(TurnOutcome::completed(
            context.agent_id,
            context.turn_id.unwrap(),
            usage,
            1,
            DoneReason::EndTurn,
        ))
    }
}

fn default_completed(context: &TurnContext) -> BackendResult {
    ScriptedBackend::completed(
        context,
        TokenUsage {
            input: 10,
            output: 5,
            cache_creation: 0,
            cache_read: 0,
        },
    )
}

impl ActorBackend for ScriptedBackend {
    fn root_preparation_error_handler(&self) -> Option<Arc<dyn Fn(u64, String) + Send + Sync>> {
        self.root_preparation_error.clone()
    }

    fn prepared_readiness(&self) -> Option<super::PreparedReadiness> {
        self.readiness.clone()
    }

    fn admission_preparation(&self) -> Option<super::AdmissionPreparation> {
        self.preparation.clone()
    }

    fn run_turn<'a>(
        &'a mut self,
        history: &'a mut crate::History,
        context: TurnContext,
        input: crate::AgentInput,
        work: WorkKind,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = BackendResult> + Send + 'a>> {
        Box::pin(async move {
            self.state
                .runs
                .lock()
                .unwrap()
                .push((work.clone(), input.message.clone()));
            if let WorkKind::Root {
                run_id,
                displayed,
                text,
                images,
                ..
            } = work
            {
                self.state.root_metadata.lock().unwrap().push((
                    run_id,
                    displayed,
                    text,
                    images.len(),
                ));
            }
            self.state.policies.lock().unwrap().push((
                input.message.clone(),
                context.policy.as_ref().map(|policy| policy.fast),
                context.generation,
            ));
            self.state.modes.lock().unwrap().push((
                input.message.clone(),
                context.policy.as_ref().map(|config| config.mode.clone()),
            ));
            self.state.admissions.lock().unwrap().push((
                input.message.clone(),
                context
                    .admission
                    .as_ref()
                    .and_then(|snapshot| snapshot.mode_def.as_ref())
                    .and_then(|def| def.system_prompt.clone()),
                context
                    .admission
                    .as_ref()
                    .is_some_and(|snapshot| snapshot.bindings.get("queued_tool").is_some()),
            ));
            self.state.entered.fetch_add(1, Ordering::SeqCst);
            for message in input.preamble {
                history.push(message);
            }
            if !input.message.is_empty() {
                history.push(Message::user(input.message));
            }

            // Poll the interrupt source exactly once per turn, after the
            // gate, so a test can queue a root mid-run and the fold is
            // deterministic. An extracted root folds into this turn (no new
            // TurnId, no outcome of its own).
            if let Some(gate) = &self.gate {
                gate.wait().await;
            }
            if let Some(ExtractedCommand::PresentedInterrupt(folded)) =
                context.interrupt.as_ref().and_then(|source| source.poll())
            {
                for msg in folded {
                    self.state
                        .folds
                        .lock()
                        .unwrap()
                        .push(msg.input.message.clone());
                    history.push(Message::user(msg.input.message));
                }
            }

            if let Some(inbox) = &context.inbox {
                for input in inbox.drain_instructions() {
                    history.push(Message::user(input.message));
                }
                for message in inbox.drain_completions() {
                    history.push(message);
                }
                inbox.instructions_submitted();
                assert!(inbox.finish());
            }
            self.outcomes
                .lock()
                .unwrap()
                .pop()
                .unwrap_or_else(|| default_completed(&context))
        })
    }

    fn run_control<'a>(
        &'a mut self,
        history: &'a mut crate::History,
        _context: TurnContext,
        control: &'a ControlWork,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = BackendResult> + Send + 'a>> {
        Box::pin(async move {
            self.state
                .controls
                .lock()
                .unwrap()
                .push(control.name.clone());
            history.push(Message::user(format!("control:{}", control.name)));
            BackendResult::ControlDone
        })
    }

    fn run_compact<'a>(
        &'a mut self,
        history: &'a mut crate::History,
        context: TurnContext,
        _instructions: Option<&'a str>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = BackendResult> + Send + 'a>> {
        Box::pin(async move {
            self.state.compacts.fetch_add(1, Ordering::SeqCst);
            self.state.compact_policies.lock().unwrap().push((
                context.policy.as_ref().map(|policy| policy.fast),
                context.generation,
            ));
            self.state
                .compact_modes
                .lock()
                .unwrap()
                .push(context.policy.as_ref().map(|policy| policy.mode.clone()));
            history.push(Message::user("compact".to_owned()));
            BackendResult::CompactDone
        })
    }
}

/// Backend whose first turn blocks until cancelled and then returns the
/// cancellation (the reason `Agent::run` would read) as its outcome. Later
/// turns complete normally so the actor's reuse is testable.
struct CancellableBackend {
    state: Arc<ScriptedState>,
    block_first: std::sync::atomic::AtomicU32,
    after_cancel: Option<Arc<Gate>>,
    billed_request: Option<(TokenUsage, EventSender)>,
}

impl CancellableBackend {
    fn new() -> Self {
        Self {
            state: Arc::new(ScriptedState::default()),
            block_first: std::sync::atomic::AtomicU32::new(0),
            after_cancel: None,
            billed_request: None,
        }
    }
}

impl ActorBackend for CancellableBackend {
    fn run_turn<'a>(
        &'a mut self,
        history: &'a mut crate::History,
        context: TurnContext,
        input: crate::AgentInput,
        _work: WorkKind,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = BackendResult> + Send + 'a>> {
        Box::pin(async move {
            history.push(Message::user(input.message));
            if self.billed_request.is_some() {
                context.output.append_text(BILLED_RESPONSE);
                history.push(Message {
                    role: Role::Assistant,
                    content: vec![ContentBlock::Text {
                        text: BILLED_RESPONSE.into(),
                    }],
                    ..Default::default()
                });
            }
            self.state.entered.fetch_add(1, Ordering::SeqCst);
            if self.block_first.fetch_add(1, Ordering::SeqCst) == 0 {
                let reason = context.cancel_reason.cancelled().await;
                if let Some(gate) = &self.after_cancel {
                    gate.wait().await;
                }
                let mut outcome = TurnOutcome::cancelled(
                    context.agent_id,
                    context.turn_id.unwrap(),
                    self.billed_request
                        .as_ref()
                        .map_or(TokenUsage::default(), |(usage, _)| *usage),
                    u32::from(self.billed_request.is_some()),
                    reason,
                );
                if let Some((_, sender)) = &self.billed_request {
                    if let TurnOutcome::Cancelled {
                        cost, list_cost, ..
                    } = &mut outcome
                    {
                        *cost = Some(BILLED_COST);
                        *list_cost = Some(BILLED_LIST_COST);
                    }
                    sender
                        .send(AgentEvent::TurnOutcome(outcome.clone()))
                        .unwrap();
                }
                return BackendResult::EnteredRun(outcome);
            }
            default_completed(&context)
        })
    }

    fn run_control<'a>(
        &'a mut self,
        history: &'a mut crate::History,
        _context: TurnContext,
        control: &'a ControlWork,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = BackendResult> + Send + 'a>> {
        Box::pin(async move {
            history.push(Message::user(format!("control:{}", control.name)));
            BackendResult::ControlDone
        })
    }

    fn run_compact<'a>(
        &'a mut self,
        history: &'a mut crate::History,
        _context: TurnContext,
        _instructions: Option<&'a str>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = BackendResult> + Send + 'a>> {
        Box::pin(async move {
            history.push(Message::user("compact".to_owned()));
            BackendResult::CompactDone
        })
    }
}

fn input(message: &str) -> AgentInput {
    AgentInput {
        message: message.to_owned(),
        mode: AgentMode::Build,
        images: Vec::new(),
        preamble: Vec::new(),
        thinking: maki_providers::ThinkingConfig::Off,
        fast: false,
        workflow: false,
        prompt: None,
        cancel: None,
        lease_committer: None,
    }
}

struct QueuedTool;
struct QueuedInvocation;

impl ToolInvocation for QueuedInvocation {
    fn start_header(&self) -> HeaderFuture {
        HeaderFuture::Ready(HeaderResult::plain("queued".into()))
    }

    fn execute<'a>(self: Box<Self>, _ctx: &'a crate::tools::ToolContext) -> ExecFuture<'a> {
        Box::pin(async {
            crate::tools::ToolExecResult {
                output: Ok(crate::ToolOutput::Plain("queued".into())),
                annotation: None,
                written_path: None,
            }
        })
    }
}

impl Tool for QueuedTool {
    fn name(&self) -> &str {
        "queued_tool"
    }
    fn description(&self, _ctx: &DescriptionContext) -> Cow<'_, str> {
        "queued".into()
    }
    fn schema(&self) -> Value {
        serde_json::json!({"type": "object"})
    }
    fn audience(&self) -> ToolAudience {
        ToolAudience::MAIN
    }
    fn parse(&self, _input: &Value) -> Result<Box<dyn ToolInvocation>, ParseError> {
        Ok(Box::new(QueuedInvocation))
    }
}

#[test_case::test_case(false; "queued_cancel_during_active_history")]
#[test_case::test_case(true; "close_queued_during_active_history")]
fn synthetic_settlement_has_unavailable_history_boundary(close: bool) {
    smol::block_on(async {
        let gate = Gate::new();
        let backend = ScriptedBackend::gated(Arc::clone(&gate));
        let state = Arc::clone(&backend.state);
        let (actor, task) = spawn(backend);
        let active = actor
            .admit_turn(input("active"), None, "active".into())
            .unwrap();
        until(|| state.entered.load(Ordering::SeqCst) == 1).await;
        active.output().append_text("partial");
        let settled = actor
            .admit_turn(input("queued"), None, "queued".into())
            .unwrap();
        if close {
            actor.close();
        } else {
            actor.cancel_turn(settled.turn_id()).unwrap();
        }
        settled.wait().await;
        let request = super::TranscriptRequest {
            through_turn: Some(settled.turn_id()),
            last_messages: 10,
            max_bytes: 1024,
        };
        assert!(
            matches!(actor.transcript(request.clone()), Err(ActorError::UnavailableTurnHistory(id)) if id == settled.turn_id())
        );
        gate.open();
        active.wait().await;
        actor.close();
        task.await;
        assert!(matches!(
            actor.transcript(request),
            Err(ActorError::UnavailableTurnHistory(_))
        ));
    });
}

#[test]
fn root_setup_failure_resolves_started_ticket_and_emits_end() {
    smol::block_on(async {
        let gate = Gate::new();
        let mut backend = ScriptedBackend::failing_setup();
        backend.gate = Some(Arc::clone(&gate));
        let (actor, task) = spawn(backend);
        let events = actor.subscribe_events();
        actor
            .rush(RootWork::new(
                input("root"),
                1,
                false,
                "root".into(),
                Vec::new(),
                "root".into(),
            ))
            .unwrap();
        let super::ActorEvent::Start { turn_id, .. } = events.recv_async().await.unwrap() else {
            panic!("expected root start")
        };
        let ticket = actor.turn_ticket(turn_id).unwrap();
        gate.open();
        assert!(matches!(ticket.wait().await, TurnOutcome::Failed { .. }));
        assert!(
            matches!(events.recv_async().await.unwrap(), super::ActorEvent::End(result) if result.outcome.turn_id() == turn_id)
        );
        actor.close();
        task.await;
    });
}

#[test]
fn typed_actor_events_are_ordered_future_only_and_broadcast() {
    smol::block_on(async {
        let gate = Gate::new();
        let (actor, task) = spawn(ScriptedBackend::gated(Arc::clone(&gate)));
        let first = actor.subscribe_events();
        actor
            .initialize_config(super::EffectiveAgentConfig::new(
                policy(false),
                AgentMode::Build,
            ))
            .unwrap();
        assert!(
            matches!(first.recv_async().await.unwrap(), super::ActorEvent::Config(commit) if !commit.config.fast)
        );
        let second = actor.subscribe_events();
        assert!(second.is_empty());
        let update = actor.reserve_config_update().unwrap();
        update.resolve(Ok(fast_change(true))).unwrap();
        update.wait().await.unwrap();
        for events in [&first, &second] {
            assert!(
                matches!(events.recv_async().await.unwrap(), super::ActorEvent::Config(commit) if commit.config.fast)
            );
        }
        let ticket = actor
            .admit_turn(input("events"), None, "events".into())
            .unwrap();
        for events in [&first, &second] {
            assert!(
                matches!(events.recv_async().await.unwrap(), super::ActorEvent::Start { turn_id, config: Some(config), .. } if turn_id == ticket.turn_id() && config.config.fast)
            );
        }
        let late = actor.subscribe_events();
        assert!(late.is_empty());
        gate.open();
        ticket.wait().await;
        for events in [&first, &second, &late] {
            assert!(
                matches!(events.recv_async().await.unwrap(), super::ActorEvent::End(result) if result.outcome.turn_id() == ticket.turn_id())
            );
            assert!(
                matches!(events.recv_async().await.unwrap(), super::ActorEvent::Idle { agent_id } if agent_id == actor.agent_id())
            );
            assert!(events.is_empty());
        }
        actor.close();
        actor.close();
        for events in [&first, &second, &late] {
            assert!(matches!(
                events.recv_async().await.unwrap(),
                super::ActorEvent::Close {
                    lifecycle: ActorLifecycle::Closed,
                    ..
                }
            ));
            assert!(events.is_empty());
        }
        let closed_subscription = actor.subscribe_events();
        assert!(closed_subscription.is_empty());
        task.await;
    });
}

#[test_case::test_case(true; "streamed_response")]
#[test_case::test_case(false; "response_without_deltas")]
fn text_attempt_retry_and_reconcile_returns_last_response(streamed: bool) {
    const PRIOR: &str = "prior";
    const FAILED: &str = "failed";
    const FINAL: &str = "final";
    const LATE: &str = "late";
    let ticket = super::TurnTicket::new(crate::TurnId::generate(), Arc::new(()));
    let output = ticket.output();
    output.append_text(PRIOR);
    output.begin_text_attempt();
    output.append_text(FAILED);
    output.reset_text_attempt();
    if streamed {
        output.append_text(FINAL);
    }
    output.reconcile_text_attempt(FINAL);
    let outcome = super::cancelled_outcome(
        AgentId::generate(),
        ticket.turn_id(),
        TurnCancellationReason::Closed,
    );
    ticket.resolve(outcome.clone());
    output.reset_text_attempt();
    output.begin_text_attempt();
    output.reconcile_text_attempt(LATE);
    output.append_text(LATE);
    ticket.resolve(outcome);
    assert_eq!(ticket.peek_result().unwrap().text, FINAL);
}

#[test_case::test_case(false; "complete")]
#[test_case::test_case(true; "close")]
fn idle_admission_frozen_result_and_future_events(close: bool) {
    smol::block_on(async {
        const PARTIAL: &str = "partial";
        const LATE: &str = "late";
        let gate = Gate::new();
        let (actor, task) = spawn(ScriptedBackend::gated(Arc::clone(&gate)));
        let events = actor.subscribe_events();
        let other_events = actor.subscribe_events();
        let ticket = actor
            .admit_turn_if_idle(input("first"), None, "first".into(), None)
            .unwrap();
        let output = ticket.output();
        output.append_text(PARTIAL);
        assert!(matches!(
            actor.admit_turn_if_idle(input("second"), None, "second".into(), None),
            Err(ActorError::Busy)
        ));
        assert!(
            matches!(events.recv_async().await.unwrap(), super::ActorEvent::Start { turn_id, .. } if turn_id == ticket.turn_id())
        );
        assert!(
            matches!(other_events.recv_async().await.unwrap(), super::ActorEvent::Start { turn_id, .. } if turn_id == ticket.turn_id())
        );
        if close {
            actor.close();
            assert!(ticket.peek_result().is_none());
        }
        gate.open();
        let first = ticket.wait_result().await;
        output.append_text(LATE);
        assert_eq!(ticket.wait_result().await.text, PARTIAL);
        assert_eq!(ticket.peek_result().unwrap().outcome, first.outcome);
        assert_eq!(
            actor
                .turn_ticket(ticket.turn_id())
                .unwrap()
                .peek_result()
                .unwrap()
                .text,
            PARTIAL
        );
        if !close {
            until(|| !actor.has_pending_work()).await;
            assert!(matches!(
                actor.admit_turn_if_idle(
                    input("stale"),
                    None,
                    "stale".into(),
                    Some(crate::TurnId::generate())
                ),
                Err(ActorError::StaleTurn)
            ));
            let second = actor
                .admit_turn_if_idle(input("next"), None, "next".into(), Some(ticket.turn_id()))
                .unwrap();
            second.wait().await;
            actor.close();
        }
        gate.open();
        task.await;
    });
}

#[test_case::test_case(0, 1024; "zero_messages")]
#[test_case::test_case(1001, 1024; "message_cap")]
#[test_case::test_case(10, 0; "zero_bytes")]
#[test_case::test_case(10, 1; "cannot_encode_empty_array")]
#[test_case::test_case(10, 1048577; "byte_cap")]
fn transcript_rejects_invalid_caps(last_messages: usize, max_bytes: usize) {
    let (actor, task) = spawn(ScriptedBackend::new());
    assert!(matches!(
        actor.transcript(super::TranscriptRequest {
            through_turn: None,
            last_messages,
            max_bytes
        }),
        Err(ActorError::InvalidTranscriptCaps)
    ));
    actor.close();
    smol::block_on(task);
}

#[test_case::test_case(1, 1024, 0; "count_cannot_split_group")]
#[test_case::test_case(2, 1024, 2; "whole_group")]
#[test_case::test_case(2, 2, 0; "byte_cannot_split_group")]
fn transcript_keeps_tool_groups(last_messages: usize, max_bytes: usize, expected: usize) {
    const TOOL_ID: &str = "tool-id";
    const TOOL_NAME: &str = "test-tool";
    let messages = vec![
        Message {
            content: vec![ContentBlock::ToolUse {
                id: TOOL_ID.into(),
                name: TOOL_NAME.into(),
                input: serde_json::json!({}),
                thought_signature: None,
            }],
            ..Default::default()
        },
        Message {
            content: vec![ContentBlock::ToolResult {
                tool_use_id: TOOL_ID.into(),
                content: String::new(),
                is_error: false,
            }],
            ..Default::default()
        },
    ];
    let (actor, task) = AgentActorHandle::spawn(
        AgentId::generate(),
        Vec::new(),
        None,
        Box::new(ScriptedBackend::new()),
    );
    actor
        .inner
        .history
        .store(Arc::new(crate::HistorySnapshot::new(messages)));
    let turn_id = crate::TurnId::generate();
    let outcome = super::cancelled_outcome(actor.agent_id(), turn_id, TurnCancellationReason::User);
    super::retire_and_publish(&actor.inner, turn_id, outcome, None, false, false, true);
    let snapshot = actor
        .transcript(super::TranscriptRequest {
            through_turn: None,
            last_messages,
            max_bytes,
        })
        .unwrap();
    assert_eq!(snapshot.messages.as_array().unwrap().len(), expected);
    assert!(snapshot.bytes <= max_bytes);
    assert_eq!(
        snapshot.bytes,
        serde_json::to_string(&snapshot.messages).unwrap().len()
    );
    actor.close();
    smol::block_on(task);
}

#[test]
fn default_transcript_excludes_first_active_and_later_active_history() {
    smol::block_on(async {
        let gate = Gate::new();
        let backend = ScriptedBackend::gated(Arc::clone(&gate));
        let state = Arc::clone(&backend.state);
        let (actor, task) = spawn(backend);
        let request = super::TranscriptRequest {
            through_turn: None,
            last_messages: 10,
            max_bytes: 1024,
        };
        let first = actor
            .admit_turn(input("first"), None, "first".into())
            .unwrap();
        until(|| state.entered.load(Ordering::SeqCst) == 1).await;
        let empty = actor.transcript(request.clone()).unwrap();
        assert_eq!(empty.through_turn, None);
        assert_eq!(empty.total_messages, 0);
        assert_eq!(empty.messages, serde_json::json!([]));
        gate.open();
        first.wait().await;
        until(|| !actor.has_pending_work()).await;
        gate.opened.store(false, Ordering::Release);
        let second = actor
            .admit_turn(input("second"), None, "second".into())
            .unwrap();
        until(|| state.entered.load(Ordering::SeqCst) == 2).await;
        let bounded = actor.transcript(request).unwrap();
        assert_eq!(bounded.through_turn, Some(first.turn_id()));
        assert_eq!(bounded.total_messages, 1);
        assert_eq!(bounded.messages[0]["content"][0]["text"], "first");
        gate.open();
        second.wait().await;
        actor.close();
        task.await;
    });
}

#[test]
fn transcript_releases_locks_before_encoding_while_turn_settles() {
    const FIRST: &str = "first";
    const SECOND: &str = "second";
    const MAX_BYTES: usize = 1024;
    const LAST_MESSAGES: usize = 10;

    std::thread::scope(|scope| {
        smol::block_on(async {
            let gate = Gate::new();
            let backend = ScriptedBackend::gated(Arc::clone(&gate));
            let state = Arc::clone(&backend.state);
            let (actor, task) = spawn(backend);
            let first = actor.admit_turn(input(FIRST), None, FIRST.into()).unwrap();
            gate.open();
            first.wait().await;
            until(|| !actor.has_pending_work()).await;
            gate.opened.store(false, Ordering::Release);
            let second = actor
                .admit_turn(input(SECOND), None, SECOND.into())
                .unwrap();
            until(|| state.entered.load(Ordering::SeqCst) == 2).await;
            let epoch = actor.inner.history.load().epoch;
            let request = super::TranscriptRequest {
                through_turn: None,
                last_messages: LAST_MESSAGES,
                max_bytes: MAX_BYTES,
            };
            let (captured_tx, captured_rx) = flume::bounded(1);
            let (release_tx, release_rx) = flume::bounded(1);
            let reader = actor.clone();
            let reader_request = request.clone();
            let transcript = scope.spawn(move || {
                reader
                    .transcript_inner(reader_request, || {
                        assert!(reader.inner.outcomes.try_lock().is_ok());
                        assert!(reader.inner.state.try_lock().is_ok());
                        assert!(reader.inner.coverage.try_lock().is_ok());
                        assert!(reader.inner.tickets.try_lock().is_ok());
                        captured_tx.send(()).unwrap();
                        release_rx.recv().unwrap();
                    })
                    .unwrap()
            });
            captured_rx.recv().unwrap();
            gate.open();
            second.wait().await;
            let latest = actor.transcript(request.clone()).unwrap();
            assert_eq!(latest.through_turn, Some(second.turn_id()));
            assert_eq!(latest.total_messages, 2);
            actor
                .inner
                .history
                .store(Arc::new(crate::HistorySnapshot::new(Vec::new())));
            release_tx.send(()).unwrap();
            let snapshot = transcript.join().unwrap();
            assert_eq!(snapshot.through_turn, Some(first.turn_id()));
            assert_eq!(snapshot.epoch, epoch);
            assert_eq!(snapshot.total_messages, 1);
            assert_eq!(snapshot.omitted_messages, 0);
            assert!(!snapshot.truncated);
            assert_eq!(snapshot.messages[0]["content"][0]["text"], FIRST);
            assert_eq!(
                snapshot.bytes,
                serde_json::to_string(&snapshot.messages).unwrap().len()
            );
            assert!(snapshot.bytes <= MAX_BYTES);
            assert!(matches!(
                actor.transcript(request),
                Err(ActorError::CompactedTurn(id)) if id == second.turn_id()
            ));
            actor.close();
            task.await;
        });
    });
}

#[test]
fn terminal_runner_exits_without_requiring_another_wake() {
    smol::block_on(async {
        const CORRELATION: &str = "terminal runner";
        let (actor, task) = spawn(ScriptedBackend::new());
        let (popped, release) = actor.pause_after_next_pop();
        let ticket = actor
            .admit_turn(input(CORRELATION), None, CORRELATION.into())
            .unwrap();
        popped.recv_async().await.unwrap();
        actor.close();
        actor.wake.consume();
        release.send(()).unwrap();
        ticket.wait().await;
        futures_lite::future::or(task, async {
            smol::Timer::after(COMPLETION_TIMEOUT).await;
            panic!("terminal runner waited for another wake");
        })
        .await;
    });
}

#[test_case::test_case(false; "queued_cancel_zero")]
#[test_case::test_case(true; "started_duration_frozen")]
fn ticket_duration_is_measured_once_at_settlement(started: bool) {
    let ticket = super::TurnTicket::new(crate::TurnId::generate(), Arc::new(()));
    if started {
        ticket.mark_started();
    }
    ticket.resolve(super::cancelled_outcome(
        AgentId::generate(),
        ticket.turn_id(),
        TurnCancellationReason::User,
    ));
    let duration = ticket.peek_result().unwrap().duration_ms;
    if !started {
        assert_eq!(duration, 0);
    }
    ticket.mark_started();
    assert_eq!(ticket.peek_result().unwrap().duration_ms, duration);
}

#[test_case::test_case(false; "constraint_rejection")]
#[test_case::test_case(true; "preparation_panic")]
fn failed_admission_delivers_fifo_predecessor_before_backend(preparation_panic: bool) {
    smol::block_on(async {
        const DENIED: &str = "admitted configuration exceeds captured ceiling";
        const PANIC_DIAGNOSTIC: &str = "admission preparation panicked";
        const CORRELATION: &str = "constrained turn";
        const MODEL: &str = "anthropic/claude-opus-4-20250514";
        const MODE: &str = "review";
        let (_parent_start, parent_rx) = flume::bounded(1);
        let (parent, parent_task) = AgentActorHandle::spawn_inner(
            AgentId::generate(),
            Vec::new(),
            None,
            Box::new(ScriptedBackend::new()),
            None,
            None,
            Some(parent_rx),
        );
        let mut managed = super::types::ManagedTurnAdmission::new(Weak::new(), AgentId::generate());
        managed.parent_delivery = Some(super::delivery::ParentDelivery {
            parent_id: parent.agent_id(),
            parent: Arc::downgrade(&parent.inner),
            metadata: Default::default(),
        });
        let mut backend = ScriptedBackend::new();
        if preparation_panic {
            backend.preparation = Some(Arc::new(|_, _, _| panic!("{PANIC_DIAGNOSTIC}")));
        }
        let state = Arc::clone(&backend.state);
        let (_start, rx) = flume::bounded(1);
        let (actor, task) = AgentActorHandle::spawn_inner(
            managed.agent_id,
            Vec::new(),
            None,
            Box::new(backend),
            Some(managed),
            Some(Arc::new(crate::EffectiveAgentConfig::new(
                policy(false),
                AgentMode::Build,
            ))),
            Some(rx),
        );
        actor.configure_parent_delivery(true, None).unwrap();
        let events = parent.subscribe_events();
        let barrier = actor.reserve_config_update().unwrap();
        let constraint_actor = actor.clone();
        let ticket = actor
            .admit_turn_with_options(
                input(CORRELATION),
                None,
                CORRELATION.into(),
                super::TurnAdmissionOptions {
                    policy_constraint: Some(Arc::new(move |config| {
                        let deadline = Instant::now() + COMPLETION_TIMEOUT;
                        while constraint_actor.inner.state.try_lock().is_err() {
                            assert!(Instant::now() < deadline, "{CONDITION_TIMEOUT}");
                            std::thread::yield_now();
                        }
                        assert_eq!(config.model.spec(), MODEL);
                        assert_eq!(config.mode.id().key(), MODE);
                        assert!(config.fast && config.workflow);
                        if preparation_panic {
                            Ok(())
                        } else {
                            Err(DENIED.into())
                        }
                    })),
                    ..Default::default()
                },
            )
            .unwrap();
        assert!(ticket.peek().is_none());
        let mut model = Model::from_spec(MODEL).unwrap();
        model.supports_fast_override = Some(maki_providers::model::FastSupport::Supported);
        barrier
            .resolve(Ok(super::ConfigChange::PatchAndMode {
                patch: super::ConfigPatch {
                    model: Some(super::PreparedModel {
                        provider: Arc::new(PolicyTestProvider),
                        model,
                    }),
                    fast: Some(true),
                    workflow: Some(true),
                    ..Default::default()
                },
                mode: AgentMode::Custom(crate::modes::ModeId::parse(MODE)),
                mode_def: crate::ModeDef::default_for(crate::modes::ModeId::parse(MODE)),
            }))
            .unwrap();
        let outcome = ticket.wait().await;
        let diagnostic = if preparation_panic {
            PANIC_DIAGNOSTIC
        } else {
            DENIED
        };
        assert!(matches!(outcome, TurnOutcome::Failed { failure, .. }
            if failure.diagnostic == diagnostic && failure.retryable == preparation_panic));
        assert_eq!(state.entered.load(Ordering::SeqCst), 0);
        assert!(actor.effective_config().unwrap().fast);
        let envelope = loop {
            if let super::ActorEvent::Delivery(event) = events.recv_async().await.unwrap() {
                break event.delivery;
            }
        };
        assert_eq!(envelope.result.outcome.turn_id(), ticket.turn_id());
        assert_eq!(envelope.correlation, CORRELATION);
        let configuration = envelope.to_json()["configuration"].clone();
        assert_eq!(configuration["generation"], 1);
        assert_eq!(configuration["selection"]["spec"], MODEL);
        assert_eq!(configuration["selection"]["thinking"], "off");
        assert_eq!(configuration["selection"]["fast"], true);
        assert_eq!(configuration["mode"], MODE);
        assert_eq!(configuration["workflow"], true);
        assert!(actor.inner.delivery_records.lock().unwrap().is_empty());
        actor.close();
        parent.close();
        task.await;
        parent_task.await;
    });
}

#[test]
fn presentation_counter_is_actor_owned_and_cancel_fences_late_allocation() {
    smol::block_on(async {
        const CORRELATION: &str = "presentation";
        let gate = Gate::new();
        let (actor, task) = spawn(ScriptedBackend::gated(Arc::clone(&gate)));
        assert_eq!(actor.reserve_presentation(4), 5);
        actor.observe_presentation(9);
        assert_eq!(actor.presentation_id(), 9);
        let ticket = actor
            .admit_turn(input(CORRELATION), None, CORRELATION.into())
            .unwrap();
        until(|| actor.processing_turn_id() == Some(ticket.turn_id())).await;
        assert_eq!(
            actor
                .allocate_processing_presentation(ticket.turn_id(), Some(9))
                .unwrap(),
            9
        );
        let (turn, fence) = actor.capture_processing_presentation_cut(9);
        assert_eq!(turn, Some(ticket.turn_id()));
        assert_eq!(fence, 10);
        // The cut cancels the run, which may settle before this call; either
        // way the late allocation is refused.
        let refused = actor.allocate_processing_presentation(ticket.turn_id(), None);
        assert!(
            matches!(refused, Err(ActorError::PolicyCancelled))
                || matches!(refused, Err(ActorError::UnknownTurn(id)) if id == ticket.turn_id()),
            "{refused:?}"
        );
        gate.open();
        ticket.wait().await;
        actor.close();
        task.await;
    });
}

#[test]
fn retained_results_evict_without_invalidating_held_tickets() {
    smol::block_on(async {
        const TEXT: &str = "retained";
        let (actor, task) = spawn(ScriptedBackend::new());
        let first = actor.admit_turn(input(TEXT), None, TEXT.into()).unwrap();
        first.wait().await;
        for _ in 0..super::MAX_RETAINED_TURN_RESULTS {
            actor
                .admit_turn(input(TEXT), None, TEXT.into())
                .unwrap()
                .wait()
                .await;
        }
        assert!(actor.turn_ticket(first.turn_id()).is_none());
        assert!(actor.outcome(first.turn_id()).is_none());
        assert_eq!(
            actor.inner.outcomes.lock().unwrap().len(),
            super::MAX_RETAINED_TURN_RESULTS
        );
        assert_eq!(
            actor.inner.retained_tickets.lock().unwrap().len(),
            super::MAX_RETAINED_TURN_RESULTS
        );
        assert_eq!(
            actor.inner.coverage.lock().unwrap().len(),
            super::MAX_RETAINED_TURN_RESULTS
        );
        assert!(first.peek_result().is_some());
        actor.close();
        task.await;
        assert!(first.peek_result().is_some());
    });
}

#[test]
fn default_transcript_ignores_cancelled_queued_turn() {
    smol::block_on(async {
        const TEXT: &str = "terminal history";
        let (actor, task) = spawn(ScriptedBackend::new());
        let first = actor.admit_turn(input(TEXT), None, TEXT.into()).unwrap();
        first.wait().await;
        let barrier = actor.reserve_config_update().unwrap();
        for _ in 0..=super::MAX_RETAINED_TURN_RESULTS {
            let queued = actor.admit_turn(input(TEXT), None, TEXT.into()).unwrap();
            actor.cancel_turn(queued.turn_id()).unwrap();
            queued.wait().await;
        }
        assert!(actor.turn_ticket(first.turn_id()).is_none());
        assert!(
            !actor
                .inner
                .coverage
                .lock()
                .unwrap()
                .contains_key(&first.turn_id())
        );
        let snapshot = actor
            .transcript(super::TranscriptRequest {
                through_turn: None,
                last_messages: 10,
                max_bytes: 1024,
            })
            .unwrap();
        assert_eq!(snapshot.through_turn, Some(first.turn_id()));
        assert_eq!(snapshot.messages[0]["content"][0]["text"], TEXT);
        drop(barrier);
        actor.close();
        task.await;
    });
}

#[test]
fn failed_turn_keeps_partial_last_assistant_message() {
    const NARRATION: &str = "earlier narration";
    const PARTIAL: &str = "partial final answer";
    let ticket = super::TurnTicket::new(crate::TurnId::generate(), Arc::new(()));
    let output = ticket.output();
    output.reconcile_text_attempt(NARRATION);
    output.begin_text_attempt();
    output.append_text(PARTIAL);
    ticket.resolve(super::cancelled_outcome(
        AgentId::generate(),
        ticket.turn_id(),
        TurnCancellationReason::User,
    ));
    assert_eq!(ticket.peek_result().unwrap().text, PARTIAL);
}

#[test]
fn transcript_exact_boundary_pending_unknown_and_rewrite() {
    smol::block_on(async {
        let gate = Gate::new();
        let (actor, task) = spawn(ScriptedBackend::gated(Arc::clone(&gate)));
        let ticket = actor
            .admit_turn(input("first"), None, "first".into())
            .unwrap();
        let request = super::TranscriptRequest {
            through_turn: Some(ticket.turn_id()),
            last_messages: 10,
            max_bytes: 1024,
        };
        assert!(matches!(
            actor.transcript(request.clone()),
            Err(ActorError::PendingTurn(_))
        ));
        assert!(matches!(
            actor.transcript(super::TranscriptRequest {
                through_turn: Some(crate::TurnId::generate()),
                ..request.clone()
            }),
            Err(ActorError::UnknownTurn(_))
        ));
        gate.open();
        ticket.wait().await;
        let snapshot = actor.transcript(request.clone()).unwrap();
        assert_eq!(snapshot.total_messages, 1);
        assert_eq!(snapshot.messages[0]["content"][0]["text"], "first");
        let second = actor
            .admit_turn(input("second"), None, "second".into())
            .unwrap();
        second.wait().await;
        assert_eq!(actor.transcript(request.clone()).unwrap().total_messages, 1);
        let current = actor.inner.history.load_full();
        actor
            .inner
            .history
            .store(Arc::new(crate::HistorySnapshot::new(
                (*current.messages).clone(),
            )));
        assert!(matches!(
            actor.transcript(request),
            Err(ActorError::CompactedTurn(_))
        ));
        actor.close();
        task.await;
    });
}

#[test]
fn busy_parent_consumes_delivery_without_reordering_config_fifo() {
    smol::block_on(async {
        const ACTIVE: &str = "active parent";
        const BEFORE: &str = "queued before config";
        const AFTER: &str = "queued after delivery";
        const CHILD: &str = "child response";
        let gate = Gate::new();
        let backend = ScriptedBackend::gated(Arc::clone(&gate));
        let state = Arc::clone(&backend.state);
        let (parent, task) = spawn(backend);
        parent
            .initialize_config(super::EffectiveAgentConfig::new(
                policy(false),
                AgentMode::Build,
            ))
            .unwrap();
        let events = parent.subscribe_events();
        let active = parent
            .admit_turn(input(ACTIVE), None, ACTIVE.into())
            .unwrap();
        until(|| state.entered.load(Ordering::SeqCst) == 1).await;
        let before = parent
            .admit_turn(input(BEFORE), None, BEFORE.into())
            .unwrap();
        let config = parent.reserve_config_update().unwrap();
        let source_agent = AgentId::generate();
        let source_turn = crate::TurnId::generate();
        let envelope = Arc::new(super::ChildDelivery {
            parent_id: parent.agent_id(),
            compatibility_id: Some(CHILD.into()),
            label: None,
            child_config: None,
            correlation: CHILD.into(),
            result: super::TurnResult {
                outcome: TurnOutcome::cancelled(
                    source_agent,
                    source_turn,
                    TokenUsage::default(),
                    0,
                    TurnCancellationReason::User,
                ),
                text: CHILD.into(),
                output: Vec::new(),
                provenance: super::TurnProvenance::default(),
                duration_ms: 0,
            },
        });
        super::delivery::admit_delivery(&parent.inner, envelope).unwrap();
        let after = parent.admit_turn(input(AFTER), None, AFTER.into()).unwrap();
        config.resolve(Ok(fast_change(true))).unwrap();
        assert_eq!(state.entered.load(Ordering::SeqCst), 1);
        gate.open();
        active.wait().await;
        before.wait().await;
        config.wait().await.unwrap();
        after.wait().await;
        assert_eq!(
            *state.policies.lock().unwrap(),
            vec![
                (ACTIVE.into(), Some(false), 0),
                (BEFORE.into(), Some(false), 0),
                (AFTER.into(), Some(true), 1),
            ]
        );
        let history = parent.inner.history.load_full();
        let observation = history
            .messages
            .iter()
            .find(|message| message.is_observation())
            .unwrap();
        assert_eq!(
            observation.observation_source,
            Some(maki_providers::ObservationSource::ChildResult {
                agent_id: source_agent.to_string(),
                turn_id: source_turn.to_string(),
            })
        );
        let restored: Message =
            serde_json::from_value(serde_json::to_value(observation).unwrap()).unwrap();
        assert_eq!(restored.observation_source, observation.observation_source);
        let statuses: Vec<_> = events
            .try_iter()
            .filter_map(|event| match event {
                super::ActorEvent::Delivery(event) => Some(event.status),
                _ => None,
            })
            .collect();
        assert_eq!(
            statuses,
            vec![
                super::DeliveryStatus::Accepted,
                super::DeliveryStatus::Executed {
                    parent_turn_id: active.turn_id()
                },
            ]
        );
        parent.close();
        task.await;
    });
}

#[test]
fn parent_delivery_policy_is_creation_time_only() {
    smol::block_on(async {
        let (actor, task) = spawn(ScriptedBackend::new());
        actor
            .configure_parent_delivery(true, Some("compatibility".into()))
            .unwrap();
        assert!(matches!(
            actor.configure_parent_delivery(false, None),
            Err(ActorError::Busy)
        ));
        let ticket = actor
            .admit_turn(input("first"), None, "first".into())
            .unwrap();
        ticket.wait().await;
        assert!(matches!(
            actor.configure_parent_delivery(false, None),
            Err(ActorError::Busy)
        ));
        actor.close();
        assert!(matches!(
            actor.configure_parent_delivery(false, None),
            Err(ActorError::Closed)
        ));
        task.await;
    });
}

fn spawn(backend: impl ActorBackend + 'static) -> (AgentActorHandle, smol::Task<()>) {
    AgentActorHandle::spawn(AgentId::generate(), Vec::new(), None, Box::new(backend))
}

async fn until(cond: impl Fn() -> bool) {
    futures_lite::future::or(
        async {
            while !cond() {
                smol::future::yield_now().await;
            }
        },
        async {
            smol::Timer::after(COMPLETION_TIMEOUT).await;
            panic!("{CONDITION_TIMEOUT}");
        },
    )
    .await;
}

#[test]
fn admission_preparation_does_not_hold_actor_state_lock() {
    let (entered_tx, entered_rx) = flume::bounded(1);
    let (release_tx, release_rx) = flume::bounded(1);
    let mut backend = ScriptedBackend::new();
    backend.preparation = Some(Arc::new(move |_, _, _| {
        entered_tx.send(()).unwrap();
        release_rx.recv().unwrap();
        crate::agent::TurnAdmissionSnapshot {
            mode_def: None,
            prompt_inputs: None,
            bindings: Arc::default(),
            mcp_startup_notice: None,
        }
    }));
    let (handle, task) = spawn(backend);
    let actor = handle.clone();
    let admission =
        std::thread::spawn(move || actor.admit_turn(input("work"), None, "work".into()));
    entered_rx.recv().unwrap();
    handle.close();
    release_tx.send(()).unwrap();
    let ticket = admission.join().unwrap().unwrap();
    assert!(matches!(
        smol::block_on(ticket.wait()),
        TurnOutcome::Cancelled { .. }
    ));
    smol::block_on(task);
}

#[test_case::test_case(false, false, false; "cancel_pending")]
#[test_case::test_case(true, false, false; "clear_pending_batch")]
#[test_case::test_case(false, true, false; "cancel_pending_then_close")]
#[test_case::test_case(true, true, false; "clear_pending_batch_then_close")]
#[test_case::test_case(false, false, true; "remove_pending_at_index")]
#[test_case::test_case(false, true, true; "remove_pending_at_index_then_close")]
fn pending_cancellation_publishes_end_before_single_idle(
    clear: bool,
    close: bool,
    remove_index: bool,
) {
    smol::block_on(async {
        let (entered_tx, entered_rx) = flume::bounded(1);
        let (release_tx, release_rx) = flume::bounded(1);
        let mut backend = ScriptedBackend::new();
        let observations = Arc::clone(&backend.state);
        backend.preparation = Some(Arc::new(move |_, _, _| {
            entered_tx.send(()).unwrap();
            release_rx.recv().unwrap();
            crate::agent::TurnAdmissionSnapshot {
                mode_def: None,
                prompt_inputs: None,
                bindings: Arc::default(),
                mcp_startup_notice: None,
            }
        }));
        let (handle, task) = spawn(backend);
        let events = handle.subscribe_events();
        let (presentation_tx, presentation) = flume::unbounded();
        let ticket = handle
            .admit_turn(
                input("pending"),
                Some(EventSender::new(presentation_tx, 0)),
                "pending".into(),
            )
            .unwrap();
        entered_rx.recv_async().await.unwrap();
        let second = clear.then(|| {
            handle
                .admit_turn(input("second"), None, "second".into())
                .unwrap()
        });
        let (stale_tx, stale_rx) = flume::bounded(1);
        *handle.inner.stale_preparation.lock().unwrap() = Some(stale_tx);
        let (retired, publish) = handle.pause_after_next_finalization_retire();
        let actor = handle.clone();
        let turn_id = ticket.turn_id();
        let cancellation = std::thread::spawn(move || {
            if clear {
                assert_eq!(actor.clear(), 2);
            } else if remove_index {
                assert!(actor.remove_at(0).is_some());
            } else {
                actor.cancel_turn(turn_id).unwrap();
            }
        });
        retired.recv_async().await.unwrap();
        assert!(ticket.peek_result().is_none());
        assert!(presentation.is_empty());
        assert!(events.is_empty());
        if close {
            handle.close();
        } else {
            assert_eq!(handle.clear(), 0);
        }
        assert!(events.is_empty());
        publish.send(()).unwrap();
        cancellation.join().unwrap();
        let outcome = ticket.wait().await;
        assert!(matches!(
            outcome,
            TurnOutcome::Cancelled {
                reason: TurnCancellationReason::User,
                ..
            }
        ));
        assert!(matches!(
            presentation.try_recv().unwrap().event,
            AgentEvent::TurnOutcome(delivered) if delivered == outcome
        ));
        assert!(presentation.is_empty());
        assert!(matches!(
            events.try_recv().unwrap(),
            super::ActorEvent::End(result) if result.outcome == outcome
        ));
        if let Some(second) = second {
            let second_outcome = second.wait().await;
            assert!(matches!(
                second_outcome,
                TurnOutcome::Cancelled {
                    reason: TurnCancellationReason::User,
                    ..
                }
            ));
            assert!(
                matches!(events.try_recv().unwrap(), super::ActorEvent::End(result)
                if result.outcome == second_outcome)
            );
        }
        let transition = events.try_recv().unwrap();
        if close {
            assert!(matches!(transition, super::ActorEvent::Close { .. }));
        } else {
            assert!(
                matches!(transition, super::ActorEvent::Idle { agent_id } if agent_id == handle.agent_id())
            );
        }
        assert!(events.is_empty());
        release_tx.send(()).unwrap();
        stale_rx.recv_async().await.unwrap();
        assert_eq!(observations.entered.load(Ordering::SeqCst), 0);
        assert!(events.is_empty());
        if !close {
            handle.cancel_all();
            assert_eq!(handle.clear(), 0);
            assert!(events.is_empty());
            handle.close();
            assert!(matches!(
                events.try_recv().unwrap(),
                super::ActorEvent::Close { .. }
            ));
        }
        task.await;
        assert!(events.is_empty());
    });
}

#[test]
fn clearing_pending_work_waits_for_processing_before_idle() {
    smol::block_on(async {
        let gate = Gate::new();
        let backend = ScriptedBackend::gated(Arc::clone(&gate));
        let observations = Arc::clone(&backend.state);
        let (handle, task) = spawn(backend);
        let events = handle.subscribe_events();
        let (popped, release) = handle.pause_after_next_pop();
        assert_eq!(handle.processing_turn_id(), None);
        let active = handle
            .admit_turn(input("active"), None, "active".into())
            .unwrap();
        popped.recv_async().await.unwrap();
        assert_eq!(handle.processing_turn_id(), Some(active.turn_id()));
        let pending = handle
            .admit_turn(input("pending"), None, "pending".into())
            .unwrap();
        assert_eq!(handle.clear(), 1);
        assert!(matches!(
            pending.wait().await,
            TurnOutcome::Cancelled { .. }
        ));
        assert!(
            matches!(events.try_recv().unwrap(), super::ActorEvent::End(result) if result.outcome.turn_id() == pending.turn_id())
        );
        assert!(events.is_empty());
        release.send(()).unwrap();
        until(|| observations.entered.load(Ordering::SeqCst) == 1).await;
        handle
            .bind_active_correlation(active.turn_id(), "presentation".into())
            .unwrap();
        assert_eq!(handle.processing_turn_id(), Some(active.turn_id()));
        assert!(matches!(
            events.try_recv().unwrap(),
            super::ActorEvent::Start { .. }
        ));
        assert_eq!(handle.clear(), 0);
        assert!(events.is_empty());
        gate.open();
        assert!(matches!(active.wait().await, TurnOutcome::Completed { .. }));
        assert!(matches!(
            events.recv_async().await.unwrap(),
            super::ActorEvent::End(_)
        ));
        assert!(matches!(
            events.recv_async().await.unwrap(),
            super::ActorEvent::Idle { .. }
        ));
        assert_eq!(handle.processing_turn_id(), None);
        handle.close();
        task.await;
        assert!(matches!(
            events.try_recv().unwrap(),
            super::ActorEvent::Close { .. }
        ));
        assert!(events.is_empty());
    });
}

#[test]
fn queued_turn_keeps_admitted_mode_and_tool_binding() {
    smol::block_on(async {
        let modes = Arc::new(crate::modes::ModeRegistry::builtin());
        let registry = Arc::new(crate::tools::ToolRegistry::new());
        modes
            .define(crate::modes::ModeDefSpec {
                name: "build".into(),
                system_prompt: Some("before".into()),
                ..Default::default()
            })
            .unwrap();
        registry
            .register(
                Arc::new(QueuedTool),
                ToolSource::Lua {
                    plugin: "queued".into(),
                },
            )
            .unwrap();
        let gate = Gate::new();
        let mut backend = ScriptedBackend::gated(Arc::clone(&gate));
        let observations = Arc::clone(&backend.state);
        backend.preparation = Some(Arc::new({
            let modes = Arc::clone(&modes);
            let registry = Arc::clone(&registry);
            move |_, mode, _| crate::agent::TurnAdmissionSnapshot {
                mode_def: Some(Arc::new(modes.current(mode))),
                prompt_inputs: None,
                bindings: Arc::new(crate::tools::TurnToolBindings::capture(
                    &registry,
                    &crate::tools::LocalTools::default(),
                    None,
                )),
                mcp_startup_notice: None,
            }
        }));
        let (handle, task) = spawn(backend);
        let first = handle
            .admit_turn(input("first"), None, "first".into())
            .unwrap();
        until(|| observations.entered.load(Ordering::SeqCst) == 1).await;
        let second = handle
            .admit_turn(input("second"), None, "second".into())
            .unwrap();
        let barrier = handle.reserve_config_update().unwrap();
        barrier.resolve(Err(ActorError::PolicyCancelled)).unwrap();
        assert!(matches!(
            barrier.wait().await,
            Err(ActorError::PolicyCancelled)
        ));
        modes
            .define(crate::modes::ModeDefSpec {
                name: "build".into(),
                system_prompt: Some("after".into()),
                ..Default::default()
            })
            .unwrap();
        registry.clear_plugin("queued");
        gate.open();
        assert!(matches!(first.wait().await, TurnOutcome::Completed { .. }));
        assert!(matches!(second.wait().await, TurnOutcome::Completed { .. }));
        assert_eq!(
            observations.admissions.lock().unwrap().as_slice(),
            &[
                ("first".into(), Some("before".into()), true),
                ("second".into(), Some("before".into()), true),
            ]
        );
        handle.shutdown();
        task.await;
    });
}

#[test]
fn fifo_order_is_strict() {
    smol::block_on(async {
        let backend = ScriptedBackend::new();
        let state = Arc::clone(&backend.state);
        let (handle, task) = spawn(backend);
        let a = handle.admit_turn(input("first"), None, "a".into()).unwrap();
        let b = handle
            .admit_turn(input("second"), None, "b".into())
            .unwrap();
        let c = handle.admit_turn(input("third"), None, "c".into()).unwrap();
        assert_eq!(a.wait().await.turn_id(), a.turn_id());
        assert_eq!(b.wait().await.turn_id(), b.turn_id());
        assert_eq!(c.wait().await.turn_id(), c.turn_id());
        let messages: Vec<String> = state
            .runs
            .lock()
            .unwrap()
            .iter()
            .map(|(_, m)| m.clone())
            .collect();
        assert_eq!(messages, ["first", "second", "third"]);
        handle.close();
        task.await;
    });
}

#[test]
fn exact_wait_and_lookup() {
    smol::block_on(async {
        let backend = ScriptedBackend::new();
        let (handle, task) = spawn(backend);
        let ticket = handle.admit_turn(input("work"), None, "w".into()).unwrap();
        let outcome = ticket.wait().await;
        assert_eq!(outcome.turn_id(), ticket.turn_id());
        assert!(matches!(outcome, TurnOutcome::Completed { .. }));

        // Lookup by id and exact wait by id agree.
        assert_eq!(handle.outcome(ticket.turn_id()), Some(outcome.clone()));
        assert_eq!(
            handle.wait_outcome(ticket.turn_id()).await.unwrap(),
            outcome
        );

        // Waiting a turn that never existed errors instead of stranding.
        assert!(matches!(
            handle.wait_outcome(crate::types::TurnId::generate()).await,
            Err(ActorError::UnknownTurn(_))
        ));
        handle.close();
        task.await;
    });
}

#[test]
fn subscriptions_after_close_are_disconnected() {
    smol::block_on(async {
        let (handle, task) = spawn(ScriptedBackend::new());
        handle.close();
        task.await;
        assert!(handle.subscribe_events().recv_async().await.is_err());
        assert!(handle.subscribe_leased_events().recv_async().await.is_err());
        assert!(
            handle
                .subscribe_config_commits()
                .recv_async()
                .await
                .is_err()
        );
    });
}

#[test]
fn close_releases_leases_stranded_in_unread_subscriptions() {
    smol::block_on(async {
        let (handle, task) = spawn(ScriptedBackend::new());
        let events = handle.subscribe_leased_events();
        let ticket = handle.admit_turn(input("work"), None, "w".into()).unwrap();
        ticket.wait().await;
        handle.close();
        task.await;
        assert!(!handle.callbacks_drained());
        drop(events);
        assert!(handle.callbacks_drained());
    });
}

#[test]
fn failed_actor_is_reusable() {
    smol::block_on(async {
        let backend = ScriptedBackend::failing_setup();
        let state = Arc::clone(&backend.state);
        let (handle, task) = spawn(backend);
        let a = handle.admit_turn(input("one"), None, "a".into()).unwrap();
        // The actor synthesizes and delivers exactly one Failed outcome.
        let outcome = a.wait().await;
        assert!(matches!(outcome, TurnOutcome::Failed { num_turns: 0, .. }));

        // Reuse: the second turn succeeds.
        let b = handle.admit_turn(input("two"), None, "b".into()).unwrap();
        assert!(matches!(b.wait().await, TurnOutcome::Completed { .. }));
        assert_eq!(state.runs.lock().unwrap().len(), 2);
        handle.close();
        task.await;
    });
}

#[test]
fn close_drains_queued_turns_and_cancels_active_with_closed_reason() {
    smol::block_on(async {
        let backend = CancellableBackend::new();
        let (handle, task) = spawn(backend);
        let t1 = handle
            .admit_turn(input("running"), None, "t1".into())
            .unwrap();
        let t2 = handle
            .admit_turn(input("queued"), None, "t2".into())
            .unwrap();
        let (tx, rx) = flume::unbounded();
        let t3 = handle
            .admit_turn(
                input("queued-with-sink"),
                Some(EventSender::new(tx, 0)),
                "t3".into(),
            )
            .unwrap();

        until(|| handle.snapshot().status != ActorStatus::Idle).await;
        handle.close();

        // Queued turns terminalize with the Closed reason and their tickets
        // resolve; the one with a sink gets exactly one delivery.
        assert!(matches!(
            t2.wait().await,
            TurnOutcome::Cancelled {
                reason: TurnCancellationReason::Closed,
                ..
            }
        ));
        assert!(matches!(
            t3.wait().await,
            TurnOutcome::Cancelled {
                reason: TurnCancellationReason::Closed,
                ..
            }
        ));
        let events: Vec<_> = rx.drain().collect();
        assert_eq!(
            events.len(),
            1,
            "each queued turn delivers its outcome exactly once"
        );

        // The active run was cancelled with Closed (reason-before-abort).
        assert!(matches!(
            t1.wait().await,
            TurnOutcome::Cancelled {
                reason: TurnCancellationReason::Closed,
                ..
            }
        ));
        assert_eq!(handle.snapshot().queued, 0);
        task.await;
    });
}

#[test]
fn cancel_all_is_reusable() {
    smol::block_on(async {
        let backend = CancellableBackend::new();
        let state = Arc::clone(&backend.state);
        let (handle, task) = spawn(backend);
        let t1 = handle
            .admit_turn(input("running"), None, "t1".into())
            .unwrap();
        // Status leaves Idle at pop; only the backend entering guarantees
        // t1, not t3, is the run that blocks until cancelled.
        until(|| state.entered.load(Ordering::SeqCst) == 1).await;

        handle
            .rush(RootWork::new(
                input("root"),
                1,
                false,
                "root".into(),
                Vec::new(),
                "r1".into(),
            ))
            .unwrap();
        let t2 = handle
            .admit_turn(input("queued"), None, "t2".into())
            .unwrap();

        handle.cancel_all();

        // Queued admitted turn terminalized with User; root dropped silently.
        assert!(matches!(
            t2.wait().await,
            TurnOutcome::Cancelled {
                reason: TurnCancellationReason::User,
                ..
            }
        ));
        assert_eq!(handle.snapshot().queued, 0);

        // The active run reads the User reason from its reasoned token.
        assert!(matches!(
            t1.wait().await,
            TurnOutcome::Cancelled {
                reason: TurnCancellationReason::User,
                ..
            }
        ));
        assert_eq!(
            handle.snapshot().lifecycle,
            ActorLifecycle::Open,
            "cancel_all keeps the actor open"
        );

        // The actor stayed open: a new turn runs normally.
        let t3 = handle
            .admit_turn(input("after"), None, "t3".into())
            .unwrap();
        assert!(matches!(t3.wait().await, TurnOutcome::Completed { .. }));
        handle.close();
        task.await;
    });
}

#[test]
fn cancel_existing_catches_turn_between_pop_and_active_install() {
    smol::block_on(async {
        let backend = ScriptedBackend::new();
        let state = Arc::clone(&backend.state);
        let (handle, task) = spawn(backend);
        let (popped, release) = handle.pause_after_next_pop();
        let cancelled = handle
            .admit_turn(input("cancelled"), None, "cancelled".into())
            .unwrap();

        popped.recv_async().await.unwrap();
        handle.cancel_existing();
        release.send(()).unwrap();

        assert!(matches!(
            cancelled.wait().await,
            TurnOutcome::Cancelled {
                reason: TurnCancellationReason::User,
                ..
            }
        ));
        assert_eq!(state.entered.load(Ordering::SeqCst), 0);

        let surviving = handle
            .admit_turn(input("surviving"), None, "surviving".into())
            .unwrap();
        assert!(matches!(
            surviving.wait().await,
            TurnOutcome::Completed { .. }
        ));
        assert_eq!(state.entered.load(Ordering::SeqCst), 1);
        handle.close();
        task.await;
    });
}

#[test]
fn cancel_existing_catches_control_between_pop_and_backend_entry() {
    smol::block_on(async {
        let backend = ScriptedBackend::new();
        let state = Arc::clone(&backend.state);
        let (handle, task) = spawn(backend);
        let (popped, release) = handle.pause_after_next_pop();
        handle
            .push_control(ControlWork {
                name: "cancelled".into(),
                correlation: "cancelled".into(),
            })
            .unwrap();

        popped.recv_async().await.unwrap();
        handle.cancel_existing();
        release.send(()).unwrap();
        until(|| handle.snapshot().queued == 0).await;
        assert!(state.controls.lock().unwrap().is_empty());

        handle
            .push_control(ControlWork {
                name: "surviving".into(),
                correlation: "surviving".into(),
            })
            .unwrap();
        until(|| !state.controls.lock().unwrap().is_empty()).await;
        assert_eq!(state.controls.lock().unwrap().as_slice(), ["surviving"]);
        handle.close();
        task.await;
    });
}

#[test]
fn compact_keeps_admitted_policy() {
    smol::block_on(async {
        let backend = ScriptedBackend::new();
        let state = Arc::clone(&backend.state);
        let (handle, task) = spawn(backend);
        handle
            .initialize_config(super::EffectiveAgentConfig::new(
                policy(false),
                AgentMode::Build,
            ))
            .unwrap();
        handle.push_compact(1, None).unwrap();
        let change = handle.reserve_config_update().unwrap();
        change.resolve(Ok(fast_change(true))).unwrap();
        change.wait().await.unwrap();
        handle
            .push_control(ControlWork {
                name: "fence".into(),
                correlation: "fence".into(),
            })
            .unwrap();
        until(|| state.controls.lock().unwrap().len() == 1).await;
        assert_eq!(
            state.compact_policies.lock().unwrap().as_slice(),
            &[(Some(false), 0)]
        );
        handle.shutdown();
        task.await;
    });
}

#[test]
fn cancel_existing_catches_compact_between_pop_and_backend_entry() {
    smol::block_on(async {
        let backend = ScriptedBackend::new();
        let state = Arc::clone(&backend.state);
        let (handle, task) = spawn(backend);
        let (popped, release) = handle.pause_after_next_pop();
        handle.push_compact(1, None).unwrap();

        popped.recv_async().await.unwrap();
        handle.cancel_existing();
        release.send(()).unwrap();
        until(|| handle.snapshot().queued == 0).await;
        assert_eq!(state.compacts.load(Ordering::SeqCst), 0);

        handle.push_compact(2, None).unwrap();
        handle
            .push_control(ControlWork {
                name: "fence".into(),
                correlation: "fence".into(),
            })
            .unwrap();
        until(|| !state.controls.lock().unwrap().is_empty()).await;
        assert_eq!(state.compacts.load(Ordering::SeqCst), 1);
        handle.close();
        task.await;
    });
}

#[test]
fn cancel_turn_catches_exact_turn_between_pop_and_active_install() {
    smol::block_on(async {
        let backend = ScriptedBackend::new();
        let state = Arc::clone(&backend.state);
        let (handle, task) = spawn(backend);
        assert!(matches!(
            handle.cancel_turn(crate::types::TurnId::generate()),
            Err(ActorError::UnknownTurn(_))
        ));

        let (popped, release) = handle.pause_after_next_pop();
        let cancelled = handle
            .admit_turn(input("cancelled"), None, "same-correlation".into())
            .unwrap();
        let cancelled_id = cancelled.turn_id();
        popped.recv_async().await.unwrap();
        handle.cancel_turn(cancelled_id).unwrap();
        release.send(()).unwrap();

        assert!(matches!(
            cancelled.wait().await,
            TurnOutcome::Cancelled {
                reason: TurnCancellationReason::User,
                ..
            }
        ));
        assert_eq!(state.entered.load(Ordering::SeqCst), 0);
        assert!(matches!(
            handle.cancel_turn(cancelled_id),
            Err(ActorError::UnknownTurn(id)) if id == cancelled_id
        ));

        let surviving = handle
            .admit_turn(input("surviving"), None, "same-correlation".into())
            .unwrap();
        assert!(matches!(
            surviving.wait().await,
            TurnOutcome::Completed { .. }
        ));
        assert_eq!(state.entered.load(Ordering::SeqCst), 1);
        handle.close();
        task.await;
    });
}

#[test]
fn cancel_turn_terminalizes_only_the_queued_exact_ticket() {
    smol::block_on(async {
        let gate = Gate::new();
        let backend = ScriptedBackend::gated(Arc::clone(&gate));
        let state = Arc::clone(&backend.state);
        let (handle, task) = spawn(backend);
        let first = handle
            .admit_turn(input("first"), None, "shared".into())
            .unwrap();
        until(|| state.entered.load(Ordering::SeqCst) == 1).await;
        let second = handle
            .admit_turn(input("second"), None, "shared".into())
            .unwrap();

        handle.cancel_turn(second.turn_id()).unwrap();
        assert!(matches!(
            second.wait().await,
            TurnOutcome::Cancelled {
                reason: TurnCancellationReason::User,
                ..
            }
        ));
        assert_eq!(state.entered.load(Ordering::SeqCst), 1);
        gate.open();
        assert!(matches!(first.wait().await, TurnOutcome::Completed { .. }));

        handle.close();
        task.await;
    });
}

#[test_case::test_case(false, true; "entered_run_closed")]
#[test_case::test_case(true, true; "setup_failure_closed")]
#[test_case::test_case(false, false; "entered_run_idle")]
#[test_case::test_case(true, false; "setup_failure_idle")]
fn terminal_ticket_is_retired_before_notification(setup_failure: bool, close_before_publish: bool) {
    smol::block_on(async {
        let backend = if setup_failure {
            ScriptedBackend::failing_setup()
        } else {
            ScriptedBackend::new()
        };
        let (handle, task) = spawn(backend);
        let (retired, release) = handle.pause_after_next_finalization_retire();
        let (tx, rx) = flume::unbounded();
        let ticket = handle
            .admit_turn(input("work"), Some(EventSender::new(tx, 0)), "work".into())
            .unwrap();
        let turn_id = ticket.turn_id();

        let cancel_handle = handle.clone();
        let pending_ticket = ticket.clone();
        let events = handle.subscribe_events();
        let cancel = std::thread::spawn(move || {
            retired.recv().unwrap();
            let result = cancel_handle.cancel_turn(turn_id);
            let pending = pending_ticket.peek_result().is_none();
            let retained = cancel_handle.outcome(turn_id).is_some();
            let presentation_pending = rx.is_empty();
            let idle = cancel_handle.snapshot().status == ActorStatus::Idle;
            if close_before_publish {
                cancel_handle.close();
            }
            let close_pending = !events
                .drain()
                .any(|event| matches!(event, super::ActorEvent::Close { .. }));
            release.send(()).unwrap();
            assert!(pending && retained && presentation_pending && close_pending && idle);
            (result, rx, events)
        });

        let outcome = ticket.wait().await;
        if setup_failure {
            assert!(matches!(outcome, TurnOutcome::Failed { .. }));
        } else {
            assert!(matches!(outcome, TurnOutcome::Completed { .. }));
        }
        let (cancel_result, presentation, events) = cancel.join().unwrap();
        assert!(matches!(cancel_result, Err(ActorError::UnknownTurn(id)) if id == turn_id));
        if setup_failure {
            assert!(
                matches!(presentation.recv_async().await.unwrap().event, AgentEvent::TurnOutcome(delivered) if delivered == outcome)
            );
        }
        assert!(presentation.is_empty());
        assert!(
            matches!(events.recv_async().await.unwrap(), super::ActorEvent::End(result) if result.outcome == outcome)
        );
        if close_before_publish {
            task.await;
            assert!(matches!(
                events.try_recv().unwrap(),
                super::ActorEvent::Close { .. }
            ));
            assert!(events.is_empty());
        } else {
            assert_eq!(handle.snapshot().status, ActorStatus::Idle);
            assert!(matches!(
                events.recv_async().await.unwrap(),
                super::ActorEvent::Idle { .. }
            ));
            const NEXT: &str = "next idle turn";
            handle
                .admit_turn_if_idle(input(NEXT), None, NEXT.into(), None)
                .unwrap()
                .wait()
                .await;
            assert_eq!(ticket.peek().unwrap(), outcome);
            handle.close();
            task.await;
        }
    });
}

#[test]
fn entered_run_is_never_emitted_twice() {
    smol::block_on(async {
        let backend = ScriptedBackend::new();
        let (handle, task) = spawn(backend);
        let (tx, rx) = flume::unbounded();
        let ticket = handle
            .admit_turn(input("work"), Some(EventSender::new(tx, 0)), "w".into())
            .unwrap();
        ticket.wait().await;
        // Agent::run already emitted the authoritative outcome; the actor
        // retains but never re-delivers it.
        assert!(rx.is_empty());
        assert!(handle.outcome(ticket.turn_id()).is_some());

        // Close after completion does not re-emit either.
        handle.close();
        assert_eq!(rx.drain().count(), 0);
        task.await;
    });
}

#[test]
fn setup_failure_delivered_exactly_once() {
    smol::block_on(async {
        let backend = ScriptedBackend::failing_setup();
        let (handle, task) = spawn(backend);
        let (tx, rx) = flume::unbounded();
        let ticket = handle
            .admit_turn(input("work"), Some(EventSender::new(tx, 0)), "w".into())
            .unwrap();
        let outcome = ticket.wait().await;
        let event = rx
            .try_recv()
            .expect("ticket resolved before outcome delivery");
        assert!(matches!(
            event.event,
            AgentEvent::TurnOutcome(TurnOutcome::Failed { .. })
        ));
        assert!(
            rx.is_empty(),
            "synthesized outcome delivered more than once"
        );
        assert!(matches!(outcome, TurnOutcome::Failed { .. }));
        assert_eq!(handle.snapshot().status, ActorStatus::Idle);
        handle.close();
        task.await;
    });
}

#[test]
fn event_sink_failure_still_retains() {
    smol::block_on(async {
        let backend = ScriptedBackend::failing_setup();
        let (handle, task) = spawn(backend);
        let (tx, rx) = flume::unbounded();
        drop(rx); // receiver dropped before admission so delivery fails
        let ticket = handle
            .admit_turn(input("work"), Some(EventSender::new(tx, 0)), "w".into())
            .unwrap();
        let outcome = ticket.wait().await;
        assert!(matches!(outcome, TurnOutcome::Failed { .. }));
        assert!(
            handle.outcome(ticket.turn_id()).is_some(),
            "outcome retained despite sink failure"
        );
        handle.close();
        task.await;
    });
}

#[test_case::test_case(false, false; "matching_plugin")]
#[test_case::test_case(true, false; "different_origin")]
#[test_case::test_case(false, true; "reloaded_plugin")]
fn active_turn_interrupt_respects_provenance(origin_boundary: bool, generation_boundary: bool) {
    smol::block_on(async {
        let gate = Gate::new();
        let backend = ScriptedBackend::gated(Arc::clone(&gate));
        let state = Arc::clone(&backend.state);
        let (actor, task) = spawn(backend);
        let provenance = super::TurnProvenance {
            origin: super::TurnOrigin::Plugin,
            plugin: Some("plugin".into()),
            plugin_generation: Some(1),
            ..Default::default()
        };
        let ticket = actor
            .admit_turn_with_options(
                input("active"),
                None,
                "active".into(),
                super::TurnAdmissionOptions {
                    provenance: provenance.clone(),
                    ..Default::default()
                },
            )
            .unwrap();
        until(|| state.entered.load(Ordering::SeqCst) == 1).await;
        let mut queued = provenance;
        if origin_boundary {
            queued.origin = super::TurnOrigin::User;
        }
        if generation_boundary {
            queued.plugin_generation = Some(2);
        }
        actor
            .rush(
                RootWork::new(
                    input("queued"),
                    1,
                    false,
                    "queued".into(),
                    Vec::new(),
                    "queued".into(),
                )
                .with_provenance(queued.clone()),
            )
            .unwrap();
        gate.open();
        ticket.wait().await;
        until(|| !actor.has_pending_work()).await;
        let boundary = origin_boundary || generation_boundary;
        assert_eq!(
            state.runs.lock().unwrap().len(),
            if boundary { 2 } else { 1 }
        );
        assert_eq!(state.folds.lock().unwrap().len(), usize::from(!boundary));
        if boundary {
            let latest = actor.snapshot().latest.unwrap();
            assert_eq!(
                actor.turn_ticket(latest.turn_id()).unwrap().provenance(),
                &queued
            );
        }
        actor.close();
        task.await;
    });
}

#[test]
fn root_folds_into_active_turn_with_no_orphan() {
    smol::block_on(async {
        let gate = Gate::new();
        let backend = ScriptedBackend::gated(Arc::clone(&gate));
        let state = Arc::clone(&backend.state);
        let (handle, task) = spawn(backend);
        let main = handle
            .admit_turn_with_options(
                input("main"),
                None,
                "m".into(),
                super::TurnAdmissionOptions {
                    provenance: super::TurnProvenance {
                        origin: super::TurnOrigin::User,
                        ..Default::default()
                    },
                    ..Default::default()
                },
            )
            .unwrap();
        // Wait until the main turn has entered the backend, then queue a
        // root. The gated backend polls the interrupt source only after the
        // gate opens, so ordering is deterministic.
        until(|| state.entered.load(Ordering::SeqCst) > 0).await;
        handle
            .rush(RootWork::new(
                input("fold-me"),
                1,
                false,
                "fold-me".into(),
                Vec::new(),
                "r1".into(),
            ))
            .unwrap();
        gate.open();
        main.wait().await;

        let runs: Vec<_> = state.runs.lock().unwrap().clone();
        assert_eq!(runs.len(), 1, "root folded, never started as its own turn");
        assert_eq!(runs[0].0, WorkKind::Turn);
        let folds = state.folds.lock().unwrap().clone();
        assert_eq!(folds, ["fold-me"]);

        let snapshot = handle.snapshot();
        assert_eq!(snapshot.queued, 0);
        assert_eq!(
            snapshot.latest.as_ref().map(|o| o.turn_id()),
            Some(main.turn_id())
        );
        assert_eq!(snapshot.status, ActorStatus::Idle);
        handle.close();
        task.await;
    });
}

#[test]
fn permitted_idle_turn_folds_interrupts_like_any_turn() {
    smol::block_on(async {
        let gate = Gate::new();
        let backend = ScriptedBackend::gated(Arc::clone(&gate));
        let state = Arc::clone(&backend.state);
        let (handle, task) = spawn(backend);
        let guard = handle.prepare_idle().unwrap();
        let approved = handle
            .admit_turn_with_options(
                input("approved"),
                None,
                "approved".into(),
                super::TurnAdmissionOptions {
                    provenance: super::TurnProvenance {
                        origin: super::TurnOrigin::User,
                        ..Default::default()
                    },
                    ..Default::default()
                },
            )
            .unwrap();
        guard.allow_turn(&approved).unwrap();
        until(|| state.entered.load(Ordering::SeqCst) > 0).await;
        handle
            .rush(RootWork::new(
                input("fold-me"),
                1,
                false,
                "fold-me".into(),
                Vec::new(),
                "r1".into(),
            ))
            .unwrap();
        gate.open();
        approved.wait().await;
        assert_eq!(state.folds.lock().unwrap().as_slice(), ["fold-me"]);
        assert_eq!(state.runs.lock().unwrap().len(), 1);
        drop(guard);
        handle.close();
        task.await;
    });
}

#[test]
fn permitted_idle_turn_with_invalid_backend_result_settles_and_releases_actor() {
    smol::block_on(async {
        let backend = ScriptedBackend::new();
        *backend.outcomes.lock().unwrap() = vec![BackendResult::ControlDone];
        let state = Arc::clone(&backend.state);
        let (handle, task) = spawn(backend);
        let events = handle.subscribe_events();
        let guard = handle.prepare_idle().unwrap();
        let approved = handle
            .admit_turn_with_options(
                input("approved"),
                None,
                "approved".into(),
                super::TurnAdmissionOptions {
                    provenance: super::TurnProvenance {
                        origin: super::TurnOrigin::User,
                        ..Default::default()
                    },
                    ..Default::default()
                },
            )
            .unwrap();
        let later = handle
            .admit_turn(input("later"), None, "later".into())
            .unwrap();
        guard.allow_turn(&approved).unwrap();
        assert!(matches!(later.wait().await, TurnOutcome::Completed { .. }));
        let result = approved.wait_result().await;
        assert!(matches!(result.outcome, TurnOutcome::Failed { .. }));
        assert_eq!(handle.outcome(approved.turn_id()), Some(result.outcome));
        assert_eq!(events.drain().filter(|event| matches!(event, super::ActorEvent::End(result) if result.outcome.turn_id() == approved.turn_id())).count(), 1);
        assert_eq!(state.entered.load(Ordering::SeqCst), 2);
        drop(guard);
        handle.close();
        task.await;
    });
}

#[test]
fn permitted_idle_turn_runs_ahead_of_earlier_queued_turn() {
    smol::block_on(async {
        const EARLIER: &str = "earlier";
        const APPROVED: &str = "approved";
        let backend = ScriptedBackend::new();
        let state = Arc::clone(&backend.state);
        let (handle, task) = spawn(backend);
        let guard = handle.prepare_idle().unwrap();
        let earlier = handle
            .admit_turn(input(EARLIER), None, EARLIER.into())
            .unwrap();
        let approved = handle
            .admit_turn(input(APPROVED), None, APPROVED.into())
            .unwrap();
        guard.allow_turn(&approved).unwrap();
        earlier.wait().await;
        let order: Vec<_> = state
            .policies
            .lock()
            .unwrap()
            .iter()
            .map(|(message, _, _)| message.clone())
            .collect();
        assert_eq!(order, [APPROVED, EARLIER]);
        drop(guard);
        handle.close();
        task.await;
    });
}

#[test]
fn idle_root_start_preserves_metadata_to_backend() {
    smol::block_on(async {
        let backend = ScriptedBackend::new();
        let state = Arc::clone(&backend.state);
        let (handle, task) = spawn(backend);
        // Queue the root while idle: the runner pops it and starts it as its
        // own turn, carrying the neutral display metadata.
        handle
            .rush(RootWork::new(
                input("deferred"),
                7,
                false,
                "deferred bubble".into(),
                vec![test_image(), test_image(), test_image()],
                "r7".into(),
            ))
            .unwrap();
        // The runner starts the root (entered) and settles to Idle; both are
        // observed before asserting, so the backend's run completed.
        until(|| {
            state.entered.load(Ordering::SeqCst) > 0
                && handle.snapshot().status == ActorStatus::Idle
        })
        .await;

        // Backend observed the started root with its exact metadata.
        let runs: Vec<_> = state.runs.lock().unwrap().clone();
        assert_eq!(runs.len(), 1);
        assert_eq!(
            runs[0].0,
            WorkKind::Root {
                run_id: 7,
                displayed: false,
                text: "deferred bubble".into(),
                images: vec![test_image(), test_image(), test_image()],
                earlier: Vec::new(),
                mcp_startup_notice: None,
                already_displayed: false,
            }
        );
        assert_eq!(runs[0].1, "deferred");
        let metadata = state.root_metadata.lock().unwrap().clone();
        assert_eq!(metadata, vec![(7, false, "deferred bubble".into(), 3)]);

        let snapshot = handle.snapshot();
        assert_eq!(snapshot.queued, 0);
        assert_eq!(snapshot.status, ActorStatus::Idle);
        handle.close();
        task.await;
    });
}

#[test]
fn snapshot_queued_excludes_controls_from_pending_work_count() {
    smol::block_on(async {
        let gate = Gate::new();
        let (handle, task) = spawn(ScriptedBackend::gated(Arc::clone(&gate)));
        let running = handle
            .admit_turn(input("running"), None, "running".into())
            .unwrap();
        until(|| handle.snapshot().status != ActorStatus::Idle).await;
        handle
            .push_control(ControlWork {
                name: "pin".into(),
                correlation: "control".into(),
            })
            .unwrap();
        handle
            .rush(RootWork::new(
                input("queued"),
                1,
                false,
                "queued".into(),
                Vec::new(),
                "queued".into(),
            ))
            .unwrap();

        let snapshot = handle.snapshot();
        assert_eq!(snapshot.queued, 1);
        assert_eq!(snapshot.queue.len(), 2);

        gate.open();
        running.wait().await;
        handle.close();
        task.await;
    });
}

#[test]
fn control_produces_no_outcome() {
    smol::block_on(async {
        let backend = ScriptedBackend::new();
        let state = Arc::clone(&backend.state);
        let (handle, task) = spawn(backend);
        handle
            .push_control(ControlWork {
                name: "pin".into(),
                correlation: "c1".into(),
            })
            .unwrap();
        until(|| !state.controls.lock().unwrap().is_empty()).await;
        assert_eq!(state.controls.lock().unwrap().as_slice(), ["pin"]);
        let snapshot = handle.snapshot();
        assert_eq!(snapshot.latest, None, "control must not produce an outcome");
        assert_eq!(snapshot.queued, 0);
        handle.close();
        task.await;
    });
}

#[test]
fn first_cancellation_reason_wins() {
    smol::block_on(async {
        let backend = CancellableBackend::new();
        let state = Arc::clone(&backend.state);
        let (handle, task) = spawn(backend);
        let ticket = handle.admit_turn(input("work"), None, "w".into()).unwrap();
        until(|| state.entered.load(Ordering::SeqCst) > 0).await;
        handle.close();
        let outcome = ticket.wait().await;
        assert!(matches!(
            outcome,
            TurnOutcome::Cancelled {
                reason: TurnCancellationReason::Closed,
                ..
            }
        ));
        task.await;
    });
}

#[test_case::test_case(false; "cancel_all_before_close")]
#[test_case::test_case(true; "cancel_turn_before_close")]
fn first_reason_wins_across_sources(cancel_one: bool) {
    smol::block_on(async {
        let gate = Gate::new();
        let mut backend = CancellableBackend::new();
        backend.after_cancel = Some(Arc::clone(&gate));
        let state = Arc::clone(&backend.state);
        let (handle, task) = spawn(backend);
        let ticket = handle.admit_turn(input("work"), None, "w".into()).unwrap();
        until(|| state.entered.load(Ordering::SeqCst) > 0).await;
        if cancel_one {
            handle.cancel_turn(ticket.turn_id()).unwrap();
        } else {
            handle.cancel_all();
        }
        handle.close();
        assert!(ticket.peek_result().is_none());
        gate.open();
        let outcome = ticket.wait().await;
        assert!(matches!(
            outcome,
            TurnOutcome::Cancelled {
                reason: TurnCancellationReason::User,
                ..
            }
        ));
        task.await;
    });
}

#[test_case::test_case(false, false; "close")]
#[test_case::test_case(true, false; "shutdown")]
#[test_case::test_case(false, true; "user_cancel_then_close")]
#[test_case::test_case(true, true; "user_cancel_then_shutdown")]
fn close_retains_billed_request_after_active_backend_unwinds(shutdown: bool, user_cancel: bool) {
    smol::block_on(async {
        const USAGE: TokenUsage = TokenUsage {
            input: 23,
            output: 11,
            cache_creation: 7,
            cache_read: 5,
        };
        let gate = Gate::new();
        let (tx, presentation) = flume::unbounded();
        let sender = EventSender::new(tx, 1);
        let mut backend = CancellableBackend::new();
        backend.after_cancel = Some(Arc::clone(&gate));
        backend.billed_request = Some((USAGE, sender.clone()));
        let state = Arc::clone(&backend.state);
        let (actor, task) = spawn(backend);
        let events = actor.subscribe_events();
        let ticket = actor
            .admit_turn(input("active"), Some(sender), "active".into())
            .unwrap();
        until(|| state.entered.load(Ordering::SeqCst) == 1).await;
        assert!(matches!(
            events.try_recv().unwrap(),
            super::ActorEvent::Start { .. }
        ));
        let queued = actor
            .admit_turn(input("queued"), None, "queued".into())
            .unwrap();
        if user_cancel {
            actor.cancel_turn(ticket.turn_id()).unwrap();
        }
        let (lifecycle, reason, error) = if shutdown {
            actor.shutdown();
            (
                ActorLifecycle::Shutdown,
                TurnCancellationReason::Shutdown,
                ActorError::Shutdown,
            )
        } else {
            actor.close();
            (
                ActorLifecycle::Closed,
                TurnCancellationReason::Closed,
                ActorError::Closed,
            )
        };
        actor.close();
        actor.shutdown();
        assert_eq!(actor.snapshot().lifecycle, lifecycle);
        assert_eq!(actor.snapshot().active_turn, Some(ticket.turn_id()));
        assert_eq!(
            actor.admit_turn(input("late"), None, "late".into()).err(),
            Some(error)
        );
        assert!(ticket.peek_result().is_none());
        assert!(
            actor
                .turn_ticket(ticket.turn_id())
                .unwrap()
                .peek_result()
                .is_none()
        );
        assert!(actor.outcome(ticket.turn_id()).is_none());
        assert!(presentation.is_empty());
        let queued_outcome = queued.peek_result().unwrap().outcome;
        assert_eq!(queued_outcome.usage(), TokenUsage::default());
        assert_eq!(queued_outcome.num_turns(), 0);
        assert!(
            matches!(queued_outcome, TurnOutcome::Cancelled { reason: actual, .. } if actual == reason)
        );
        assert!(
            matches!(events.try_recv().unwrap(), super::ActorEvent::End(result) if result.outcome.turn_id() == queued.turn_id())
        );
        let closing_events = actor.subscribe_events();
        assert!(closing_events.is_empty());
        assert!(events.is_empty());
        gate.open();
        let result = ticket.wait_result().await;
        task.await;
        let expected_reason = if user_cancel {
            TurnCancellationReason::User
        } else {
            reason
        };
        assert!(
            matches!(result.outcome, TurnOutcome::Cancelled { reason: actual, .. } if actual == expected_reason)
        );
        assert_eq!(result.outcome.usage(), USAGE);
        assert_eq!(result.outcome.num_turns(), 1);
        assert_eq!(result.outcome.cost(), Some(BILLED_COST));
        assert_eq!(result.outcome.list_cost(), Some(BILLED_LIST_COST));
        assert_eq!(result.text, BILLED_RESPONSE);
        assert_eq!(
            actor.wait_outcome(ticket.turn_id()).await.unwrap(),
            result.outcome
        );
        assert_eq!(
            actor
                .turn_ticket(ticket.turn_id())
                .unwrap()
                .wait_result()
                .await
                .outcome,
            result.outcome
        );
        let snapshot = actor.snapshot();
        assert_eq!(snapshot.lifecycle, lifecycle);
        assert_eq!(snapshot.status, ActorStatus::Idle);
        assert_eq!(snapshot.cumulative_usage, USAGE);
        assert_eq!(snapshot.latest, Some(result.outcome.clone()));
        assert!(
            matches!(presentation.try_recv().unwrap().event, AgentEvent::TurnOutcome(outcome) if outcome == result.outcome)
        );
        assert!(presentation.is_empty());
        for observer in [&events, &closing_events] {
            assert!(
                matches!(observer.try_recv().unwrap(), super::ActorEvent::End(end) if end.outcome == result.outcome && end.text == result.text)
            );
            assert!(
                matches!(observer.try_recv().unwrap(), super::ActorEvent::Close { lifecycle: actual, .. } if actual == lifecycle)
            );
            assert!(observer.is_empty());
        }
        assert_eq!(state.entered.load(Ordering::SeqCst), 1);
        assert!(actor.inner.tickets.lock().unwrap().is_empty());
        assert_eq!(actor.inner.outcomes.lock().unwrap().len(), 2);
        assert_eq!(
            actor
                .transcript(super::TranscriptRequest {
                    through_turn: Some(ticket.turn_id()),
                    last_messages: 10,
                    max_bytes: 1024,
                })
                .unwrap()
                .total_messages,
            2
        );
    });
}

#[test]
fn close_rejects_new_admissions() {
    smol::block_on(async {
        let backend = ScriptedBackend::new();
        let (handle, task) = spawn(backend);
        handle.close();
        assert!(matches!(
            handle.admit_turn(input("late"), None, "late".into()),
            Err(ActorError::Closed)
        ));
        assert_eq!(handle.snapshot().lifecycle, ActorLifecycle::Closed);
        task.await;
    });
}

#[test]
fn queue_pop_interrupt_keeps_incompatible_entries() {
    let queue = ActorQueue::new();
    queue.push(ActorWork::Compact {
        run_id: 1,
        instructions: None,
        generation: 0,
        policy: None,
    });
    queue.push(ActorWork::Control(ControlWork {
        name: "c".into(),
        correlation: "c".into(),
    }));
    assert!(matches!(
        queue.pop_interrupt(
            0,
            &crate::batch_key(&input("t")),
            &super::TurnProvenance {
                origin: super::TurnOrigin::User,
                ..Default::default()
            }
        ),
        Some(ExtractedCommand::Compact(None))
    ));
    // A control at the front is incompatible: poll must not consume it.
    assert!(
        queue
            .pop_interrupt(
                0,
                &crate::batch_key(&input("t")),
                &super::TurnProvenance {
                    origin: super::TurnOrigin::User,
                    ..Default::default()
                }
            )
            .is_none()
    );
    assert_eq!(queue.len(), 1);
    // A turn at the front shields a root behind it.
    let admission = TurnAdmission {
        turn_id: crate::types::TurnId::generate(),
        input: Some(input("t")),
        event_sender: None,
        correlation: "t".into(),
        generation: 0,
        policy: None,
        admission: None,
        ticket: super::TurnTicket::new(crate::types::TurnId::generate(), Arc::new(())),
    };
    queue.push(ActorWork::Turn(admission));
    queue.push(ActorWork::Root(RootWork::new(
        input("r"),
        2,
        false,
        "r".into(),
        Vec::new(),
        "r2".into(),
    )));
    assert!(
        queue
            .pop_interrupt(
                0,
                &crate::batch_key(&input("t")),
                &super::TurnProvenance {
                    origin: super::TurnOrigin::User,
                    ..Default::default()
                }
            )
            .is_none()
    );
    assert_eq!(
        queue.len(),
        3,
        "incompatible FIFO entries must not be discarded"
    );
}

#[test]
fn no_op_policy_update_keeps_generation() {
    smol::block_on(async {
        let backend = ScriptedBackend::new();
        let (handle, task) = spawn(backend);
        let unchanged = policy(false);
        handle
            .initialize_config(super::EffectiveAgentConfig::new(
                unchanged,
                AgentMode::Build,
            ))
            .unwrap();
        let before = handle.inner.state.lock().unwrap().policy_generation;
        let reservation = handle.reserve_config_update().unwrap();
        reservation
            .resolve(Ok(
                super::ConfigChange::Patch(super::ConfigPatch::default()),
            ))
            .unwrap();
        reservation.wait().await.unwrap();
        assert_eq!(handle.inner.state.lock().unwrap().policy_generation, before);
        handle.close();
        task.await;
    });
}

#[test]
fn reserved_policy_orders_admissions_and_failed_updates() {
    smol::block_on(async {
        let gate = Gate::new();
        let backend = ScriptedBackend::gated(Arc::clone(&gate));
        let state = Arc::clone(&backend.state);
        let (handle, task) = spawn(backend);
        handle
            .initialize_config(super::EffectiveAgentConfig::new(
                policy(false),
                AgentMode::Build,
            ))
            .unwrap();
        let a = handle.admit_turn(input("a"), None, "a".into()).unwrap();
        until(|| backend_reached(&handle)).await;
        let options = handle.reserve_config_update().unwrap();
        let reservation = handle.reserve_config_update().unwrap();
        let next = handle.clone();
        let b = next.admit_turn(input("b"), None, "b".into()).unwrap();
        assert!(handle.policy_snapshot().is_some_and(|p| !p.fast));
        options.resolve(Ok(fast_change(true))).unwrap();
        reservation
            .resolve(Ok(super::ConfigChange::Mode {
                mode: AgentMode::Plan("plan.md".into()),
                mode_def: Some(
                    crate::modes::ModeRegistry::builtin()
                        .current(&AgentMode::Plan("plan.md".into())),
                ),
            }))
            .unwrap();
        gate.open();
        a.wait().await;
        b.wait().await;
        assert_eq!(
            *state.policies.lock().unwrap(),
            vec![("a".into(), Some(false), 0), ("b".into(), Some(true), 2)]
        );
        assert_eq!(
            handle.effective_config().unwrap().mode,
            AgentMode::Plan("plan.md".into())
        );
        assert_eq!(
            state.modes.lock().unwrap().as_slice(),
            &[
                ("a".into(), Some(AgentMode::Build)),
                ("b".into(), Some(AgentMode::Plan("plan.md".into())))
            ]
        );

        let failed = handle.reserve_config_update().unwrap();
        failed.resolve(Err(ActorError::PolicyCancelled)).unwrap();
        assert!(matches!(
            failed.wait().await,
            Err(ActorError::PolicyCancelled)
        ));
        let c = handle.admit_turn(input("c"), None, "c".into()).unwrap();
        c.wait().await;
        assert_eq!(
            state.policies.lock().unwrap()[2],
            ("c".into(), Some(true), 2)
        );
        handle.close();
        task.await;
    });
}

#[test]
fn compact_uses_fifo_config() {
    smol::block_on(async {
        let backend = ScriptedBackend::new();
        let observed = Arc::clone(&backend.state);
        let (handle, task) = spawn(backend);
        handle
            .initialize_config(super::EffectiveAgentConfig::new(
                policy(true),
                AgentMode::Build,
            ))
            .unwrap();
        let reservation = handle.reserve_config_update().unwrap();
        handle.push_compact(7, Some("compact".into())).unwrap();
        let snapshot = handle.snapshot();
        assert_eq!(snapshot.queued, 1);
        assert_eq!(
            snapshot.queue,
            [QueueProjection::Compact(Some("compact".into()))]
        );
        reservation
            .resolve(Ok(super::ConfigChange::Mode {
                mode: AgentMode::Plan("pinned-plan.md".into()),
                mode_def: Some(
                    crate::modes::ModeRegistry::builtin()
                        .current(&AgentMode::Plan("pinned-plan.md".into())),
                ),
            }))
            .unwrap();
        reservation.wait().await.unwrap();
        until(|| observed.compacts.load(Ordering::SeqCst) == 1).await;
        assert_eq!(
            observed.compact_policies.lock().unwrap().as_slice(),
            &[(Some(true), 1)]
        );
        assert_eq!(
            observed.compact_modes.lock().unwrap().as_slice(),
            &[Some(AgentMode::Plan("pinned-plan.md".into()))]
        );
        handle.close();
        task.await;
    });
}

#[test]
fn overlapping_reservations_commit_fifo_and_cancel_preserves_pending_policy() {
    smol::block_on(async {
        let backend = ScriptedBackend::new();
        let state = Arc::clone(&backend.state);
        let (handle, task) = spawn(backend);
        handle
            .initialize_config(super::EffectiveAgentConfig::new(
                policy(false),
                AgentMode::Build,
            ))
            .unwrap();
        let first = handle.reserve_config_update().unwrap();
        let second = handle.reserve_config_update().unwrap();
        second
            .resolve(Ok(super::ConfigChange::Mode {
                mode: AgentMode::Plan("plan.md".into()),
                mode_def: Some(
                    crate::modes::ModeRegistry::builtin()
                        .current(&AgentMode::Plan("plan.md".into())),
                ),
            }))
            .unwrap();
        assert!(!handle.policy_snapshot().unwrap().fast);
        let after = handle
            .admit_turn(input("after"), None, "after".into())
            .unwrap();
        first.resolve(Ok(fast_change(true))).unwrap();
        after.wait().await;
        assert_eq!(
            state.policies.lock().unwrap()[0],
            ("after".into(), Some(true), 2)
        );
        assert_eq!(
            handle.effective_config().unwrap().mode,
            AgentMode::Plan("plan.md".into())
        );
        let pending = handle.reserve_config_update().unwrap();
        let cancelled = handle
            .admit_turn(input("cancelled"), None, "cancelled".into())
            .unwrap();
        handle.cancel_existing();
        pending.resolve(Ok(fast_change(true))).unwrap();
        pending.wait().await.unwrap();
        assert!(matches!(
            cancelled.wait().await,
            TurnOutcome::Cancelled { .. }
        ));
        let after_cancel = handle
            .admit_turn(input("cancelled"), None, "cancelled".into())
            .unwrap();
        assert_eq!(after_cancel.wait().await.agent_id(), handle.agent_id());
        pending.resolve(Ok(fast_change(false))).unwrap();
        assert!(pending.wait().await.unwrap().config.fast);
        handle.close();
        task.await;
    });
}

#[test]
fn cancel_all_preserves_config_changes() {
    smol::block_on(async {
        let backend = ScriptedBackend::new();
        let state = Arc::clone(&backend.state);
        let (handle, task) = spawn(backend);
        handle
            .initialize_config(super::EffectiveAgentConfig::new(
                policy(false),
                AgentMode::Build,
            ))
            .unwrap();
        let pending = handle.reserve_config_update().unwrap();
        let gate = Gate::new();
        let setter_gate = Arc::clone(&gate);
        let setter = smol::spawn(async move {
            setter_gate.wait().await;
            pending.resolve(Ok(fast_change(true)))
        });
        let deferred = handle
            .admit_turn(input("deferred"), None, "deferred".into())
            .unwrap();

        handle.cancel_all();
        assert!(matches!(
            deferred.wait().await,
            TurnOutcome::Cancelled { .. }
        ));
        assert!(handle.policy_snapshot().is_some_and(|policy| !policy.fast));

        gate.open();
        setter.await.unwrap();
        handle.wait_policy_updates().await.unwrap();
        assert!(handle.policy_snapshot().is_some_and(|policy| policy.fast));

        let next = handle
            .admit_turn(input("next"), None, "next".into())
            .unwrap();
        next.wait().await;
        assert_eq!(
            state.policies.lock().unwrap()[0],
            ("next".into(), Some(true), 1)
        );
        handle.close();
        task.await;
    });
}

#[test]
fn admissions_between_setters_keep_their_fifo_policy() {
    smol::block_on(async {
        let backend = ScriptedBackend::new();
        let observed = Arc::clone(&backend.state);
        let (handle, task) = spawn(backend);
        handle
            .initialize_config(super::EffectiveAgentConfig::new(
                policy(false),
                AgentMode::Build,
            ))
            .unwrap();
        let first = handle.reserve_config_update().unwrap();
        let b = handle.admit_turn(input("b"), None, "b".into()).unwrap();
        let second = handle.reserve_config_update().unwrap();
        let c = handle.admit_turn(input("c"), None, "c".into()).unwrap();
        second.resolve(Ok(fast_change(false))).unwrap();
        assert!(b.peek().is_none());
        first.resolve(Ok(fast_change(true))).unwrap();
        b.wait().await;
        c.wait().await;
        assert_eq!(
            *observed.policies.lock().unwrap(),
            vec![("b".into(), Some(true), 1), ("c".into(), Some(false), 2)]
        );
        handle.close();
        task.await;
    });
}

#[test]
fn first_setter_releases_b_while_second_still_blocks_c() {
    smol::block_on(async {
        let backend = ScriptedBackend::new();
        let observed = Arc::clone(&backend.state);
        let (handle, task) = spawn(backend);
        handle
            .initialize_config(super::EffectiveAgentConfig::new(
                policy(false),
                AgentMode::Build,
            ))
            .unwrap();
        let first = handle.reserve_config_update().unwrap();
        let b = handle.admit_turn(input("b"), None, "b".into()).unwrap();
        let second = handle.reserve_config_update().unwrap();
        let c = handle.admit_turn(input("c"), None, "c".into()).unwrap();
        first.resolve(Ok(fast_change(true))).unwrap();
        b.wait().await;
        assert!(c.peek().is_none());
        assert_eq!(
            *observed.policies.lock().unwrap(),
            vec![("b".into(), Some(true), 1)]
        );
        second.resolve(Ok(fast_change(false))).unwrap();
        c.wait().await;
        assert_eq!(
            *observed.policies.lock().unwrap(),
            vec![("b".into(), Some(true), 1), ("c".into(), Some(false), 2)]
        );
        handle.close();
        task.await;
    });
}

#[test]
fn deferred_work_projects_and_removes_without_orphan_tickets() {
    smol::block_on(async {
        let (handle, task) = spawn(ScriptedBackend::new());
        let reservation = handle.reserve_config_update().unwrap();
        let removed = handle
            .admit_turn(input("removed"), None, "removed".into())
            .unwrap();
        let cleared = handle
            .admit_turn(input("cleared"), None, "cleared".into())
            .unwrap();
        handle
            .rush(RootWork::new(
                input("root"),
                1,
                false,
                "root".into(),
                Vec::new(),
                "root".into(),
            ))
            .unwrap();
        let snapshot = handle.snapshot();
        assert_eq!(snapshot.queued, snapshot.queue.len());
        assert!(
            snapshot
                .queue
                .contains(&QueueProjection::Turn("removed".into()))
        );
        assert!(snapshot.queue.contains(&QueueProjection::Message {
            text: "root".into(),
            image_count: 0,
            displayed: false,
        }));
        let index = snapshot
            .queue
            .iter()
            .position(|work| *work == QueueProjection::Turn("removed".into()))
            .unwrap();
        assert_eq!(
            handle.remove_at(index),
            Some(QueueProjection::Turn("removed".into()))
        );
        assert!(matches!(
            removed.wait().await,
            TurnOutcome::Cancelled { .. }
        ));
        assert_eq!(
            handle.remove_visible_at(0),
            Some(QueueProjection::Message {
                text: "root".into(),
                image_count: 0,
                displayed: false,
            })
        );
        assert_eq!(handle.clear(), 1);
        assert!(matches!(
            cleared.wait().await,
            TurnOutcome::Cancelled { .. }
        ));
        assert!(handle.inner.tickets.lock().unwrap().is_empty());
        drop(reservation);
        handle.close();
        task.await;
    });
}

#[test]
fn admissions_reserve_between_setters() {
    smol::block_on(async {
        let backend = ScriptedBackend::new();
        let observed = Arc::clone(&backend.state);
        let (handle, task) = spawn(backend);
        handle
            .initialize_config(super::EffectiveAgentConfig::new(
                policy(false),
                AgentMode::Build,
            ))
            .unwrap();
        let first = handle.reserve_config_update().unwrap();
        let b = handle.admit_turn(input("b"), None, "b".into()).unwrap();
        let second = handle.reserve_config_update().unwrap();
        let c = handle.admit_turn(input("c"), None, "c".into()).unwrap();
        second.resolve(Ok(fast_change(false))).unwrap();
        first.resolve(Ok(fast_change(true))).unwrap();
        b.wait().await;
        c.wait().await;
        assert_eq!(
            *observed.policies.lock().unwrap(),
            vec![("b".into(), Some(true), 1), ("c".into(), Some(false), 2)]
        );
        handle.close();
        task.await;
    });
}

#[test]
fn root_batch_and_interrupt_respect_policy_generation() {
    smol::block_on(async {
        let gate = Gate::new();
        let backend = ScriptedBackend::gated(Arc::clone(&gate));
        let observed = Arc::clone(&backend.state);
        let (handle, task) = spawn(backend);
        handle
            .initialize_config(super::EffectiveAgentConfig::new(
                policy(false),
                AgentMode::Build,
            ))
            .unwrap();
        let active = handle
            .admit_turn_with_options(
                input("active"),
                None,
                "active".into(),
                super::TurnAdmissionOptions {
                    provenance: super::TurnProvenance {
                        origin: super::TurnOrigin::User,
                        ..Default::default()
                    },
                    ..Default::default()
                },
            )
            .unwrap();
        until(|| observed.entered.load(Ordering::SeqCst) == 1).await;
        for (run_id, message) in [(1, "old"), (2, "old too")] {
            handle
                .rush(RootWork::new(
                    input(message),
                    run_id,
                    false,
                    message.into(),
                    Vec::new(),
                    message.into(),
                ))
                .unwrap();
        }
        let reservation = handle.reserve_config_update().unwrap();
        reservation.resolve(Ok(fast_change(true))).unwrap();
        handle
            .rush(RootWork::new(
                input("new"),
                3,
                false,
                "new".into(),
                Vec::new(),
                "new".into(),
            ))
            .unwrap();
        gate.open();
        active.wait().await;
        until(|| observed.entered.load(Ordering::SeqCst) == 2).await;
        assert_eq!(
            observed.folds.lock().unwrap().as_slice(),
            &["old", "old too"]
        );
        assert_eq!(observed.policies.lock().unwrap()[0].2, 0);
        assert_eq!(observed.policies.lock().unwrap()[1].2, 1);
        assert_eq!(observed.root_metadata.lock().unwrap()[0].2, "new");
        handle.close();
        task.await;
    });
}

#[test]
fn deferred_roots_keep_setter_order() {
    smol::block_on(async {
        let backend = ScriptedBackend::new();
        let observed = Arc::clone(&backend.state);
        let (handle, task) = spawn(backend);
        handle
            .initialize_config(super::EffectiveAgentConfig::new(
                policy(false),
                AgentMode::Build,
            ))
            .unwrap();
        let first = handle.reserve_config_update().unwrap();
        handle
            .rush(RootWork::new(
                input("b"),
                1,
                false,
                "b".into(),
                Vec::new(),
                "b".into(),
            ))
            .unwrap();
        let second = handle.reserve_config_update().unwrap();
        handle
            .rush(RootWork::new(
                input("c"),
                2,
                false,
                "c".into(),
                Vec::new(),
                "c".into(),
            ))
            .unwrap();
        second.resolve(Ok(fast_change(false))).unwrap();
        first.resolve(Ok(fast_change(true))).unwrap();
        until(|| observed.entered.load(Ordering::SeqCst) == 2).await;
        assert_eq!(
            *observed.policies.lock().unwrap(),
            vec![("b".into(), Some(true), 1), ("c".into(), Some(false), 2)]
        );
        handle.close();
        task.await;
    });
}

#[test]
fn roots_reserve_between_setters() {
    smol::block_on(async {
        let backend = ScriptedBackend::new();
        let observed = Arc::clone(&backend.state);
        let (handle, task) = spawn(backend);
        handle
            .initialize_config(super::EffectiveAgentConfig::new(
                policy(false),
                AgentMode::Build,
            ))
            .unwrap();
        let first = handle.reserve_config_update().unwrap();
        handle
            .rush(RootWork::new(
                input("b"),
                1,
                false,
                "b".into(),
                Vec::new(),
                "b".into(),
            ))
            .unwrap();
        let second = handle.reserve_config_update().unwrap();
        handle
            .rush(RootWork::new(
                input("c"),
                2,
                false,
                "c".into(),
                Vec::new(),
                "c".into(),
            ))
            .unwrap();
        second.resolve(Ok(fast_change(false))).unwrap();
        first.resolve(Ok(fast_change(true))).unwrap();
        until(|| observed.entered.load(Ordering::SeqCst) == 2).await;
        assert_eq!(
            *observed.policies.lock().unwrap(),
            vec![("b".into(), Some(true), 1), ("c".into(), Some(false), 2)]
        );
        handle.close();
        task.await;
    });
}

#[test]
fn targeted_cancel_settles_deferred_turn_before_policy_commit() {
    smol::block_on(async {
        let backend = ScriptedBackend::new();
        let observed = Arc::clone(&backend.state);
        let (handle, task) = spawn(backend);
        handle
            .initialize_config(super::EffectiveAgentConfig::new(
                policy(false),
                AgentMode::Build,
            ))
            .unwrap();
        let reservation = handle.reserve_config_update().unwrap();
        let cancelled = handle
            .admit_turn(input("cancelled"), None, "cancelled".into())
            .unwrap();
        handle.cancel_turn(cancelled.turn_id()).unwrap();
        assert!(matches!(
            cancelled.wait().await,
            TurnOutcome::Cancelled { .. }
        ));
        reservation.resolve(Ok(fast_change(true))).unwrap();
        assert!(observed.policies.lock().unwrap().is_empty());
        handle.close();
        task.await;
    });
}

#[test]
fn precancelled_deferred_turn_is_cancelled_after_policy_commit() {
    smol::block_on(async {
        let backend = ScriptedBackend::new();
        let observed = Arc::clone(&backend.state);
        let (handle, task) = spawn(backend);
        handle
            .initialize_config(super::EffectiveAgentConfig::new(
                policy(false),
                AgentMode::Build,
            ))
            .unwrap();
        let reservation = handle.reserve_config_update().unwrap();
        handle.cancel_correlation("deferred", TurnCancellationReason::User);
        let ticket = handle
            .admit_turn(input("cancelled"), None, "deferred".into())
            .unwrap();
        reservation.resolve(Ok(fast_change(true))).unwrap();
        assert!(matches!(ticket.wait().await, TurnOutcome::Cancelled { .. }));
        assert!(observed.policies.lock().unwrap().is_empty());
        handle.close();
        task.await;
    });
}

#[test]
fn dropped_reservation_releases_async_admission() {
    smol::block_on(async {
        let backend = ScriptedBackend::new();
        let observed = Arc::clone(&backend.state);
        let (handle, task) = spawn(backend);
        handle
            .initialize_config(super::EffectiveAgentConfig::new(
                policy(false),
                AgentMode::Build,
            ))
            .unwrap();
        let reservation = handle.reserve_config_update().unwrap();
        let ticket = handle
            .admit_turn(input("after"), None, "after".into())
            .unwrap();
        assert!(ticket.peek().is_none());
        drop(reservation);
        ticket.wait().await;
        assert_eq!(
            observed.policies.lock().unwrap()[0],
            ("after".into(), Some(false), 0)
        );
        handle.close();
        task.await;
    });
}

#[test]
fn close_wakes_policy_waiter() {
    smol::block_on(async {
        let (handle, task) = spawn(ScriptedBackend::new());
        let reservation = handle.reserve_config_update().unwrap();
        let ticket = handle
            .admit_turn(input("after"), None, "after".into())
            .unwrap();
        handle.close();
        let outcome = ticket.wait().await;
        assert!(matches!(
            outcome,
            TurnOutcome::Cancelled {
                reason: TurnCancellationReason::Closed,
                ..
            }
        ));
        assert_eq!(handle.outcome(ticket.turn_id()), Some(outcome));
        drop(reservation);
        assert!(handle.inner.state.lock().unwrap().operations.is_empty());
        assert!(handle.policy_snapshot().is_none());
        assert!(matches!(
            handle.admit_turn(input("after close"), None, "after close".into()),
            Err(ActorError::Closed)
        ));
        task.await;
    });
}

#[test]
fn cancelled_running_turn_does_not_fold_next_generation() {
    smol::block_on(async {
        let gate = Gate::new();
        let backend = ScriptedBackend::gated(Arc::clone(&gate));
        let state = Arc::clone(&backend.state);
        let (handle, task) = spawn(backend);
        let running = handle
            .admit_turn(input("running"), None, "running".into())
            .unwrap();
        until(|| backend_reached(&handle)).await;
        handle.cancel_existing();
        handle
            .rush(RootWork::new(
                input("new"),
                2,
                false,
                "new".into(),
                Vec::new(),
                "new".into(),
            ))
            .unwrap();
        gate.open();
        running.wait().await;
        until(|| state.entered.load(Ordering::SeqCst) == 2).await;
        assert!(state.folds.lock().unwrap().is_empty());
        assert_eq!(state.runs.lock().unwrap().len(), 2);
        handle.close();
        task.await;
    });
}

#[test]
fn queue_drain_publication_is_ordered() {
    let queue = ActorQueue::new();
    let published = Arc::new(Mutex::new(0));
    queue.publish_if_empty(|| *published.lock().unwrap() += 1);
    assert_eq!(*published.lock().unwrap(), 1, "empty queue publishes");
    queue.push(ActorWork::Compact {
        run_id: 1,
        instructions: None,
        generation: 0,
        policy: None,
    });
    queue.publish_if_empty(|| *published.lock().unwrap() += 1);
    assert_eq!(
        *published.lock().unwrap(),
        1,
        "non-empty queue must not publish"
    );
    // After a drain the queue is empty again and publishes.
    assert_eq!(queue.drain_all().len(), 1);
    queue.publish_if_empty(|| *published.lock().unwrap() += 1);
    assert_eq!(*published.lock().unwrap(), 2);
}

#[test]
fn restored_mirror_is_synchronous() {
    smol::block_on(async {
        let mirror = SharedMessages::default();
        let backend = ScriptedBackend::new();
        let (handle, task) = AgentActorHandle::spawn(
            AgentId::generate(),
            vec![Message::user("restored".to_owned())],
            Some(mirror.clone()),
            Box::new(backend),
        );
        // The mirror already contains the restored history before the handle
        // escapes: no yield needed.
        let snapshot = mirror.load_full();
        assert_eq!(snapshot.messages.len(), 1);
        assert!(matches!(
            snapshot.messages[0].content[0],
            ContentBlock::Text { .. }
        ));
        handle.close();
        task.await;
    });
}

#[test]
fn remove_terminalizes_instead_of_stranding() {
    smol::block_on(async {
        let gate = Gate::new();
        let backend = ScriptedBackend::gated(Arc::clone(&gate));
        let (handle, task) = spawn(backend);
        let t1 = handle
            .admit_turn(input("running"), None, "t1".into())
            .unwrap();
        until(|| backend_reached(&handle)).await;
        let t2 = handle
            .admit_turn(input("queued"), None, "t2".into())
            .unwrap();
        let t3 = handle
            .admit_turn(input("also-queued"), None, "t3".into())
            .unwrap();

        let removed = handle.remove(t2.turn_id()).unwrap();
        assert!(matches!(
            removed,
            TurnOutcome::Cancelled {
                reason: TurnCancellationReason::User,
                ..
            }
        ));
        assert!(matches!(
            t2.wait().await,
            TurnOutcome::Cancelled {
                reason: TurnCancellationReason::User,
                ..
            }
        ));
        assert_eq!(handle.snapshot().queued, 1, "only t3 remains");

        gate.open();
        t1.wait().await;
        t3.wait().await;
        handle.close();
        task.await;
    });
}

/// One of the running/queued turns reached the backend.
fn backend_reached(handle: &AgentActorHandle) -> bool {
    handle.snapshot().status != ActorStatus::Idle
}

#[test]
fn targeted_cancel_matching_queued_correlation_and_reuse() {
    smol::block_on(async {
        let gate = Gate::new();
        let backend = ScriptedBackend::gated(Arc::clone(&gate));
        let (handle, task) = spawn(backend);
        let t1 = handle
            .admit_turn(input("one"), None, "keep".into())
            .unwrap();
        until(|| backend_reached(&handle)).await;
        let t2 = handle
            .admit_turn(input("two"), None, "drop".into())
            .unwrap();
        let t3 = handle
            .admit_turn(input("three"), None, "keep".into())
            .unwrap();
        // Queue root work sharing the "drop" correlation, behind the turns.
        handle
            .rush(RootWork::new(
                input("root-drop"),
                9,
                false,
                "root-drop".into(),
                Vec::new(),
                "drop".into(),
            ))
            .unwrap();

        handle.cancel_correlation("drop", TurnCancellationReason::User);

        // The matching queued turn is terminalized exactly once with the
        // reason; unrelated turns run normally.
        assert!(matches!(
            t2.wait().await,
            TurnOutcome::Cancelled {
                reason: TurnCancellationReason::User,
                ..
            }
        ));
        // The matching root was dropped; t1 is active and only t3 remains queued.
        assert_eq!(handle.snapshot().queued, 1);
        gate.open();
        assert!(matches!(t1.wait().await, TurnOutcome::Completed { .. }));
        assert!(matches!(t3.wait().await, TurnOutcome::Completed { .. }));
        // The actor stays reusable.
        let t4 = handle
            .admit_turn(input("four"), None, "new".into())
            .unwrap();
        assert!(matches!(t4.wait().await, TurnOutcome::Completed { .. }));
        handle.close();
        task.await;
    });
}

#[test]
fn targeted_cancel_matching_active_run() {
    smol::block_on(async {
        let backend = CancellableBackend::new();
        let state = Arc::clone(&backend.state);
        let (handle, task) = spawn(backend);
        let running = handle
            .admit_turn(input("running"), None, "run-a".into())
            .unwrap();
        until(|| state.entered.load(Ordering::SeqCst) > 0).await;
        handle.cancel_correlation("run-a", TurnCancellationReason::User);
        assert!(matches!(
            running.wait().await,
            TurnOutcome::Cancelled {
                reason: TurnCancellationReason::User,
                ..
            }
        ));
        // Unrelated correlation left the actor reusable.
        let later = handle
            .admit_turn(input("later"), None, "run-b".into())
            .unwrap();
        assert!(matches!(later.wait().await, TurnOutcome::Completed { .. }));
        handle.close();
        task.await;
    });
}

#[test]
fn started_run_retires_superseded_precancel_marks() {
    smol::block_on(async {
        let backend = ScriptedBackend::new();
        let (handle, task) = spawn(backend);
        // Cancel r2 while nothing with that correlation exists, so its mark
        // only shields later pushes with r2.
        handle.cancel_correlation("r2", TurnCancellationReason::User);
        let first = handle
            .admit_turn(input("first"), None, "r1".into())
            .unwrap();
        assert!(matches!(first.wait().await, TurnOutcome::Completed { .. }));
        // Starting r1 retires the r2 mark: run-id pushes always stamp the
        // latest run, so the mark could never match again.
        let retried = handle
            .admit_turn(input("retried"), None, "r2".into())
            .unwrap();
        assert!(matches!(
            retried.wait().await,
            TurnOutcome::Completed { .. }
        ));
        handle.close();
        task.await;
    });
}

#[test]
fn precancel_marks_match_and_do_not_poison() {
    smol::block_on(async {
        let backend = ScriptedBackend::new();
        let (handle, task) = spawn(backend);
        // Cancel before any work with this correlation exists.
        handle.cancel_correlation("run-5", TurnCancellationReason::User);
        // Precancelled turn admission is terminalized immediately and exactly
        // once, without executing or stranding.
        let (tx, rx) = flume::unbounded();
        let ticket = handle
            .admit_turn(input("late"), Some(EventSender::new(tx, 0)), "run-5".into())
            .unwrap();
        assert!(matches!(
            ticket.wait().await,
            TurnOutcome::Cancelled {
                reason: TurnCancellationReason::User,
                ..
            }
        ));
        let events: Vec<_> = rx.drain().collect();
        assert_eq!(events.len(), 1, "precancelled admission delivered once");
        // Precancelled root is dropped without executing.
        handle
            .rush(RootWork::new(
                input("late-root"),
                5,
                false,
                "late-root".into(),
                Vec::new(),
                "run-5".into(),
            ))
            .unwrap();
        assert_eq!(handle.snapshot().queued, 0);
        // The mark is retired: a later unrelated correlation runs fine.
        let ok = handle
            .admit_turn(input("other"), None, "other".into())
            .unwrap();
        assert!(matches!(ok.wait().await, TurnOutcome::Completed { .. }));
        handle.close();
        task.await;
    });
}

#[test]
fn targeted_cancel_first_reason_wins_vs_close() {
    smol::block_on(async {
        let backend = CancellableBackend::new();
        let state = Arc::clone(&backend.state);
        let (handle, task) = spawn(backend);
        let running = handle
            .admit_turn(input("running"), None, "race-a".into())
            .unwrap();
        until(|| state.entered.load(Ordering::SeqCst) > 0).await;
        // Targeted cancel fires User; the later close cannot override.
        handle.cancel_correlation("race-a", TurnCancellationReason::User);
        handle.close();
        assert!(matches!(
            running.wait().await,
            TurnOutcome::Cancelled {
                reason: TurnCancellationReason::User,
                ..
            }
        ));
        task.await;
    });
}

#[test]
fn remove_visible_at_skips_hidden_and_removes_deferred() {
    smol::block_on(async {
        let gate = Gate::new();
        let backend = ScriptedBackend::gated(Arc::clone(&gate));
        let (handle, task) = spawn(backend);
        // The runner is parked in the gated backend on the admitted turn, so
        // everything pushed below stays queued and the queue order is stable.
        let running = handle
            .admit_turn(input("running"), None, "r0".into())
            .unwrap();
        until(|| backend_reached(&handle)).await;
        // Hidden rows: an already-displayed root.
        handle
            .rush(RootWork::new(
                input("displayed-root"),
                1,
                true,
                "displayed-root".into(),
                Vec::new(),
                "h2".into(),
            ))
            .unwrap();
        // Visible rows: a deferred root and a compact.
        handle
            .rush(RootWork::new(
                input("deferred-root"),
                2,
                false,
                "deferred-root".into(),
                Vec::new(),
                "v1".into(),
            ))
            .unwrap();
        handle.push_compact(3, None).unwrap();

        let queue = handle.snapshot().queue;
        assert_eq!(queue.len(), 3);

        // Removing visible index 0 removes the deferred root, not the hidden
        // displayed root.
        let removed = handle.remove_visible_at(0).unwrap();
        assert!(matches!(
            removed,
            QueueProjection::Message { ref text, .. } if text == "deferred-root"
        ));
        assert_eq!(handle.snapshot().queued, 2);
        // The running turn finishes normally once the gate opens.
        gate.open();
        assert!(matches!(
            running.wait().await,
            TurnOutcome::Completed { .. }
        ));
        handle.close();
        task.await;
    });
}

#[test]
fn remove_at_raw_index_terminalizes_real_turn() {
    smol::block_on(async {
        let gate = Gate::new();
        let backend = ScriptedBackend::gated(Arc::clone(&gate));
        let (handle, task) = spawn(backend);
        let t1 = handle
            .admit_turn(input("running"), None, "r1".into())
            .unwrap();
        until(|| backend_reached(&handle)).await;

        let t2 = handle
            .admit_turn(input("queued"), None, "r2".into())
            .unwrap();
        let t3 = handle
            .admit_turn(input("queued2"), None, "r3".into())
            .unwrap();

        let removed = handle.remove_at(0).unwrap();
        assert!(matches!(removed, QueueProjection::Turn(_)));
        assert!(matches!(
            t2.wait().await,
            TurnOutcome::Cancelled {
                reason: TurnCancellationReason::User,
                ..
            }
        ));
        let snapshot = handle.snapshot();
        assert_eq!(snapshot.queued, 1);
        assert_eq!(snapshot.queue, vec![QueueProjection::Turn("r3".into())]);
        gate.open();
        t1.wait().await;
        t3.wait().await;
        handle.close();
        task.await;
    });
}

#[test]
fn remove_at_out_of_bounds_is_none() {
    smol::block_on(async {
        let backend = ScriptedBackend::new();
        let (handle, task) = spawn(backend);
        assert_eq!(handle.remove_at(0), None);
        assert_eq!(handle.remove_visible_at(0), None);
        handle.close();
        task.await;
    });
}

#[test]
fn cancel_r7_precancels_and_drops_compact_7_but_not_8() {
    smol::block_on(async {
        let backend = ScriptedBackend::new();
        let state = Arc::clone(&backend.state);
        let (handle, task) = spawn(backend);
        task.detach();

        // Precancel r7 before any work with that correlation exists. Compact 7
        // queued afterwards is dropped; compact 8 is unrelated and runs.
        handle.cancel_correlation("r7", TurnCancellationReason::User);
        handle.push_compact(7, None).unwrap();
        handle.push_compact(8, None).unwrap();
        until(|| state.compacts.load(Ordering::SeqCst) >= 1).await;
        assert_eq!(
            state.compacts.load(Ordering::SeqCst),
            1,
            "compact 7 dropped, only compact 8 ran"
        );

        // Targeted queued cancellation removes compact 7 while compact 8
        // survives: park the runner in a gated turn so the compacts stay
        // queued, then cancel r7.
        let gate = Gate::new();
        let backend = ScriptedBackend::gated(Arc::clone(&gate));
        let (handle, task) = spawn(backend);
        let running = handle
            .admit_turn(input("running"), None, "run-1".into())
            .unwrap();
        until(|| backend_reached(&handle)).await;
        handle.push_compact(7, None).unwrap();
        handle.push_compact(8, None).unwrap();
        assert_eq!(handle.snapshot().queued, 2);

        handle.cancel_correlation("r7", TurnCancellationReason::User);
        let queue = handle.snapshot().queue;
        assert_eq!(queue.len(), 1, "compact 7 removed, compact 8 survives");
        assert!(matches!(queue[0], QueueProjection::Compact(_)));

        gate.open();
        assert!(matches!(
            running.wait().await,
            TurnOutcome::Completed { .. }
        ));
        handle.close();
        task.await;
    });
}

#[test]
fn task_submit_busy_and_idle_turn_ids() {
    smol::block_on(async {
        const FIRST: &str = "initial task";
        const NEXT: &str = "steering instruction";
        let gate = Gate::new();
        let backend = ScriptedBackend::gated(Arc::clone(&gate));
        let state = Arc::clone(&backend.state);
        let (actor, task) = spawn(backend);
        let first = actor
            .submit(input(FIRST), None, FIRST.into(), Default::default())
            .unwrap();
        until(|| state.entered.load(Ordering::SeqCst) == 1).await;
        let provenance = super::TurnProvenance {
            source_turn: Some(crate::TurnId::generate()),
            ..Default::default()
        };
        let joined = actor
            .submit(
                input(NEXT),
                None,
                NEXT.into(),
                super::TurnAdmissionOptions {
                    provenance: provenance.clone(),
                    ..Default::default()
                },
            )
            .unwrap();
        assert_eq!(first.turn_id(), joined.turn_id());
        assert_ne!(joined.provenance(), &provenance);
        let exact = actor.admit_turn(input(NEXT), None, NEXT.into()).unwrap();
        assert_ne!(exact.turn_id(), first.turn_id());
        gate.open();
        first.wait().await;
        exact.wait().await;
        let history = actor.inner.history.load_full();
        let messages: Vec<_> = history
            .messages
            .iter()
            .filter_map(Message::first_text_content)
            .collect();
        assert_eq!(messages, vec![FIRST, NEXT, NEXT]);
        let successor = actor
            .submit(input(FIRST), None, FIRST.into(), Default::default())
            .unwrap();
        assert_ne!(first.turn_id(), successor.turn_id());
        successor.wait().await;
        actor.close();
        task.await;
    });
}

#[test]
fn submit_races_finish_on_both_sides() {
    smol::block_on(async {
        const FIRST: &str = "first instruction";
        const NEXT: &str = "next instruction";
        let (_start, rx) = flume::bounded(1);
        let (actor, task) = AgentActorHandle::spawn_inner(
            AgentId::generate(),
            Vec::new(),
            None,
            Box::new(ScriptedBackend::new()),
            None,
            None,
            Some(rx),
        );
        let first = actor
            .submit(input(FIRST), None, FIRST.into(), Default::default())
            .unwrap();
        let source = super::inbox::ActorTurnInbox::source(&actor.inner, first.turn_id(), None);
        let joined = actor
            .submit(input(NEXT), None, NEXT.into(), Default::default())
            .unwrap();
        assert_eq!(first.turn_id(), joined.turn_id());
        assert!(!source.finish());
        assert_eq!(source.drain_instructions()[0].message, NEXT);
        assert!(!source.finish());
        assert!(source.has_unsubmitted_instructions());
        source.instructions_submitted();
        assert!(source.finish());
        let successor = actor
            .submit(input(NEXT), None, NEXT.into(), Default::default())
            .unwrap();
        assert_ne!(first.turn_id(), successor.turn_id());
        assert!(!source.has_unsubmitted_instructions());
        actor.close();
        task.await;
    });
}

#[test]
fn existing_only_submit_rejects_successor_without_admitting_work() {
    smol::block_on(async {
        const TEXT: &str = "instruction";
        let (_start, rx) = flume::bounded(1);
        let (actor, task) = AgentActorHandle::spawn_inner(
            AgentId::generate(),
            Vec::new(),
            None,
            Box::new(ScriptedBackend::new()),
            None,
            None,
            Some(rx),
        );
        let first = actor
            .submit(input(TEXT), None, TEXT.into(), Default::default())
            .unwrap();
        let existing_only = || super::TurnAdmissionOptions {
            existing_only: true,
            ..Default::default()
        };
        assert_eq!(
            actor
                .submit(input(TEXT), None, TEXT.into(), existing_only())
                .unwrap()
                .turn_id(),
            first.turn_id()
        );
        let source = super::inbox::ActorTurnInbox::source(&actor.inner, first.turn_id(), None);
        assert_eq!(source.drain_instructions().len(), 1);
        source.instructions_submitted();
        assert!(source.finish());
        let admission_state = || {
            let state = actor.inner.state.lock().unwrap();
            (
                state.next_operation_id,
                state.instructions.len(),
                state.operations.len(),
                actor.inner.tickets.lock().unwrap().len(),
            )
        };
        let before = admission_state();
        assert!(matches!(
            actor.submit(input(TEXT), None, TEXT.into(), existing_only()),
            Err(ActorError::Busy)
        ));
        assert_eq!(admission_state(), before);
        assert_eq!(actor.latest_ticket().unwrap().turn_id(), first.turn_id());
        assert!(source.drain_instructions().is_empty());
        let successor = actor
            .submit(input(TEXT), None, TEXT.into(), Default::default())
            .unwrap();
        assert_ne!(first.turn_id(), successor.turn_id());
        actor.close();
        task.await;
    });
}

#[test_case::test_case(false; "count")]
#[test_case::test_case(true; "utf8_bytes")]
fn instruction_inbox_limits_are_nonfatal(bytes: bool) {
    smol::block_on(async {
        const TEXT: &str = "instruction";
        let (_start, rx) = flume::bounded(1);
        let (actor, task) = AgentActorHandle::spawn_inner(
            AgentId::generate(),
            Vec::new(),
            None,
            Box::new(ScriptedBackend::new()),
            None,
            None,
            Some(rx),
        );
        let first = actor
            .submit(input(TEXT), None, TEXT.into(), Default::default())
            .unwrap();
        if bytes {
            let text = "é".repeat((super::MAX_PENDING_INSTRUCTION_BYTES - TEXT.len()) / "é".len());
            assert_eq!(
                actor
                    .submit(input(&text), None, TEXT.into(), Default::default())
                    .unwrap()
                    .turn_id(),
                first.turn_id()
            );
        } else {
            for _ in 1..super::MAX_PENDING_INSTRUCTIONS {
                assert_eq!(
                    actor
                        .submit(input(TEXT), None, TEXT.into(), Default::default())
                        .unwrap()
                        .turn_id(),
                    first.turn_id()
                );
            }
        }
        assert!(matches!(
            actor.submit(input(TEXT), None, TEXT.into(), Default::default()),
            Err(ActorError::InstructionOverflow)
        ));
        assert_eq!(actor.snapshot().lifecycle, ActorLifecycle::Open);
        let source = super::inbox::ActorTurnInbox::source(&actor.inner, first.turn_id(), None);
        assert!(!source.drain_instructions().is_empty());
        source.instructions_submitted();
        assert_eq!(
            actor
                .submit(input(TEXT), None, TEXT.into(), Default::default())
                .unwrap()
                .turn_id(),
            first.turn_id()
        );
        actor.cancel_turn(first.turn_id()).unwrap();
        assert!(source.drain_instructions().is_empty());
        let successor = actor
            .submit(input(TEXT), None, TEXT.into(), Default::default())
            .unwrap();
        assert_ne!(first.turn_id(), successor.turn_id());
        actor.close();
        task.await;
    });
}

#[test]
fn submit_pending_work_and_configuration_boundaries() {
    smol::block_on(async {
        const TEXT: &str = "instruction";
        let (_start, rx) = flume::bounded(1);
        let (actor, task) = AgentActorHandle::spawn_inner(
            AgentId::generate(),
            Vec::new(),
            None,
            Box::new(ScriptedBackend::new()),
            None,
            None,
            Some(rx),
        );
        let first = actor
            .submit(input(TEXT), None, TEXT.into(), Default::default())
            .unwrap();
        let exact = actor.admit_turn(input(TEXT), None, TEXT.into()).unwrap();
        let next = actor
            .submit(input(TEXT), None, TEXT.into(), Default::default())
            .unwrap();
        assert_ne!(first.turn_id(), next.turn_id());
        assert_ne!(exact.turn_id(), next.turn_id());
        assert_eq!(
            next.turn_id(),
            actor
                .submit(input(TEXT), None, TEXT.into(), Default::default())
                .unwrap()
                .turn_id()
        );
        let config = actor.reserve_config_update().unwrap();
        let after = actor
            .submit(input(TEXT), None, TEXT.into(), Default::default())
            .unwrap();
        assert_ne!(next.turn_id(), after.turn_id());
        drop(config);
        actor.close();
        task.await;
    });
}

#[test]
fn cancel_active_compact_cancels_cleanly_without_panic() {
    smol::block_on(async {
        let (started_tx, started_rx) = flume::bounded(1);
        struct BlockingCompactBackend {
            state: Arc<ScriptedState>,
            started_tx: flume::Sender<()>,
        }
        impl ActorBackend for BlockingCompactBackend {
            fn run_turn<'a>(
                &'a mut self,
                _history: &'a mut crate::History,
                context: TurnContext,
                _input: crate::AgentInput,
                _work: WorkKind,
            ) -> std::pin::Pin<Box<dyn std::future::Future<Output = BackendResult> + Send + 'a>>
            {
                Box::pin(async move { default_completed(&context) })
            }
            fn run_control<'a>(
                &'a mut self,
                _history: &'a mut crate::History,
                _context: TurnContext,
                _control: &'a ControlWork,
            ) -> std::pin::Pin<Box<dyn std::future::Future<Output = BackendResult> + Send + 'a>>
            {
                Box::pin(async { BackendResult::ControlDone })
            }
            fn run_compact<'a>(
                &'a mut self,
                _history: &'a mut crate::History,
                context: TurnContext,
                _instructions: Option<&'a str>,
            ) -> std::pin::Pin<Box<dyn std::future::Future<Output = BackendResult> + Send + 'a>>
            {
                Box::pin(async move {
                    self.state.compacts.fetch_add(1, Ordering::SeqCst);
                    let _ = self.started_tx.send(());
                    context.cancel.cancelled().await;
                    BackendResult::CompactDone
                })
            }
        }
        let state = Arc::new(ScriptedState::default());
        let backend = BlockingCompactBackend {
            state: Arc::clone(&state),
            started_tx,
        };
        let (handle, task) = spawn(backend);
        handle.push_compact(7, None).unwrap();
        started_rx.recv_async().await.unwrap();
        handle.cancel_correlation("r7", TurnCancellationReason::User);
        handle.close();
        task.await;
        assert_eq!(state.compacts.load(Ordering::SeqCst), 1);
    });
}

fn delivery_statuses(events: &flume::Receiver<ActorEvent>) -> Vec<DeliveryStatus> {
    events
        .try_iter()
        .filter_map(|event| match event {
            ActorEvent::Delivery(event) => Some(event.status),
            _ => None,
        })
        .collect()
}

#[derive(Clone, Copy)]
enum Claim {
    WaitFirst,
    AutomaticFirst,
}

#[test_case::test_case(Claim::WaitFirst; "wait_first")]
#[test_case::test_case(Claim::AutomaticFirst; "automatic_first")]
fn wait_and_automatic_delivery_claim_race(first: Claim) {
    smol::block_on(async {
        const CHILD: &str = "child work";
        let backend = ScriptedBackend::new();
        let scripted = Arc::clone(&backend.state);
        let (parent_start, parent_rx) = flume::bounded(1);
        let (parent, parent_task) = AgentActorHandle::spawn_inner(
            AgentId::generate(),
            Vec::new(),
            None,
            Box::new(backend),
            None,
            None,
            Some(parent_rx),
        );
        let mut managed = super::ManagedTurnAdmission::new(Weak::new(), AgentId::generate());
        managed.parent_delivery = Some(super::delivery::ParentDelivery {
            parent_id: parent.agent_id(),
            parent: Arc::downgrade(&parent.inner),
            metadata: Default::default(),
        });
        let (_child_start, child_rx) = flume::bounded(1);
        let (child, child_task) = AgentActorHandle::spawn_inner(
            managed.agent_id,
            Vec::new(),
            None,
            Box::new(ScriptedBackend::new()),
            Some(managed),
            None,
            Some(child_rx),
        );
        child.configure_parent_delivery(true, None).unwrap();
        let events = parent.subscribe_events();
        let child_turn = child
            .admit_turn(input(CHILD), None, CHILD.into())
            .unwrap()
            .turn_id();
        child.cancel_turn(child_turn).unwrap();
        let continuation = {
            let mut state = parent.inner.state.lock().unwrap();
            super::delivery::schedule(&parent.inner, &mut state);
            let (&continuation, batch) = state.delivery_runs.iter().next().unwrap();
            assert_eq!(batch, &vec![child_turn]);
            continuation
        };
        let continuation_ticket = parent.latest_ticket().unwrap();
        assert_eq!(continuation_ticket.turn_id(), continuation);
        assert!(continuation_ticket.peek_result().is_none());
        let waiter_ticket = continuation_ticket.clone();
        let continuation_waiter = smol::spawn(async move { waiter_ticket.wait_result().await });
        let claimed = match first {
            Claim::WaitFirst => {
                assert!(parent.withdraw_child_delivery(child_turn));
                assert!(super::delivery::drain(&parent.inner, continuation).is_empty());
                DeliveryStatus::Discarded(TurnCancellationReason::Withdrawn)
            }
            Claim::AutomaticFirst => {
                assert_eq!(super::delivery::drain(&parent.inner, continuation).len(), 1);
                assert!(!parent.withdraw_child_delivery(child_turn));
                assert!(super::delivery::drain(&parent.inner, continuation).is_empty());
                DeliveryStatus::Executed {
                    parent_turn_id: continuation,
                }
            }
        };
        assert_eq!(
            delivery_statuses(&events),
            vec![DeliveryStatus::Accepted, claimed]
        );
        let retained = child
            .turn_ticket(child_turn)
            .and_then(|ticket| ticket.peek_result())
            .unwrap();
        assert_eq!(retained.outcome.turn_id(), child_turn);
        assert_eq!(child.outcome(child_turn), Some(retained.outcome));

        parent_start.send(()).unwrap();
        let result = futures_lite::future::or(continuation_waiter, async {
            smol::Timer::after(COMPLETION_TIMEOUT).await;
            panic!("{CONDITION_TIMEOUT}");
        })
        .await;
        assert_eq!(
            result.outcome,
            super::cancelled_outcome(
                parent.agent_id(),
                continuation,
                TurnCancellationReason::Withdrawn,
            )
        );
        assert_eq!(result.duration_ms, 0);
        assert!(parent.inner.state.lock().unwrap().delivery_runs.is_empty());
        assert_eq!(scripted.entered.load(Ordering::SeqCst), 0);
        assert!(parent.turn_ticket(continuation).is_none());
        assert!(
            events
                .try_iter()
                .all(|event| !matches!(event, ActorEvent::Start { .. }))
        );
        parent.close();
        child.close();
        parent_task.await;
        child_task.await;
        let retained_result = continuation_ticket.wait_result().await;
        assert_eq!(retained_result.outcome, result.outcome);
        assert_eq!(retained_result.duration_ms, result.duration_ms);
    });
}

#[test]
fn delivery_arrival_during_finalization() {
    smol::block_on(async {
        const ACTIVE: &str = "finishing parent turn";
        const LATER: &str = "later parent turn";
        const CHILD: &str = "late child result";
        let backend = ScriptedBackend::new();
        let scripted = Arc::clone(&backend.state);
        let (parent, task) = spawn(backend);
        let events = parent.subscribe_events();
        let (retired, release_retire) = parent.pause_after_next_finalization_retire();
        let finished = parent
            .admit_turn(input(ACTIVE), None, ACTIVE.into())
            .unwrap();
        retired.recv_async().await.unwrap();
        assert!(finished.peek_result().is_none());
        assert!(
            !parent
                .inner
                .state
                .lock()
                .unwrap()
                .instructions
                .contains_key(&finished.turn_id())
        );

        let child_turn = TurnId::generate();
        let envelope = Arc::new(ChildDelivery {
            parent_id: parent.agent_id(),
            compatibility_id: None,
            label: None,
            child_config: None,
            correlation: CHILD.into(),
            result: super::TurnResult {
                outcome: TurnOutcome::cancelled(
                    AgentId::generate(),
                    child_turn,
                    TokenUsage::default(),
                    0,
                    TurnCancellationReason::User,
                ),
                text: CHILD.into(),
                output: Vec::new(),
                provenance: super::TurnProvenance::default(),
                duration_ms: 0,
            },
        });
        super::delivery::admit_delivery(&parent.inner, envelope).unwrap();
        {
            let state = parent.inner.state.lock().unwrap();
            assert!(state.deliveries.contains_key(&child_turn));
            assert!(state.delivery_runs.is_empty());
        }

        let (popped, release_pop) = parent.pause_after_next_pop();
        release_retire.send(()).unwrap();
        assert!(matches!(
            finished.wait().await,
            TurnOutcome::Completed { .. }
        ));
        until(|| !popped.is_empty()).await;
        let continuation = {
            let state = parent.inner.state.lock().unwrap();
            assert_eq!(state.delivery_runs.len(), 1);
            let (&continuation, batch) = state.delivery_runs.iter().next().unwrap();
            assert_eq!(batch, &vec![child_turn]);
            assert!(state.deliveries.contains_key(&child_turn));
            continuation
        };
        assert_ne!(continuation, finished.turn_id());
        release_pop.send(()).unwrap();
        let continuation_ticket = parent.turn_ticket(continuation).unwrap();
        until(|| continuation_ticket.peek().is_some()).await;
        assert!(matches!(
            continuation_ticket.peek(),
            Some(TurnOutcome::Completed { .. })
        ));

        let later = parent.admit_turn(input(LATER), None, LATER.into()).unwrap();
        until(|| later.peek().is_some()).await;
        let mut started = Vec::new();
        let mut statuses = Vec::new();
        for event in events.try_iter() {
            match event {
                ActorEvent::Start { turn_id, .. } => started.push(turn_id),
                ActorEvent::Delivery(event) => statuses.push(event.status),
                _ => {}
            }
        }
        assert_eq!(
            started,
            vec![finished.turn_id(), continuation, later.turn_id()]
        );
        assert_eq!(
            statuses,
            vec![
                DeliveryStatus::Accepted,
                DeliveryStatus::Executed {
                    parent_turn_id: continuation
                },
            ]
        );
        assert_eq!(scripted.entered.load(Ordering::SeqCst), 3);
        {
            let state = parent.inner.state.lock().unwrap();
            assert!(state.deliveries.is_empty());
            assert!(state.delivery_runs.is_empty());
        }
        parent.close();
        task.await;
    });
}
