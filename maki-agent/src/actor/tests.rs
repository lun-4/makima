//! Deterministic actor-core tests. No sleeps: every test drives the actor
//! through a scripted backend and waits on exact outcome tickets, which
//! resolve the moment the actor retains the outcome.

#[path = "config_tests.rs"]
mod config_tests;

use std::slice;
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
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

use super::queue::QueueProjection;
use super::types::{
    ActorLifecycle, ActorStatus, BackendResult, ControlWork, TurnContext, WorkKind,
};
use super::{
    ActorBackend, ActorError, ActorEvent, ActorWork, AgentActorHandle, ChildDelivery,
    CompactionStatus, DeliveryStatus,
};
use crate::tools::DescriptionContext;
use crate::tools::{
    ExecFuture, HeaderFuture, HeaderResult, ParseError, Tool, ToolAudience, ToolInvocation,
    ToolSource,
};
use crate::types::{
    AgentId, DoneReason, Envelope, EventSender, TurnCancellationReason, TurnFailure,
    TurnFailureKind, TurnId, TurnOutcome,
};
use crate::{AgentEvent, AgentInput, AgentMode, SharedMessages};
use std::borrow::Cow;

const COMPLETION_TIMEOUT: Duration = Duration::from_secs(5);
const CONDITION_TIMEOUT: &str = "condition never became true";
const BILLED_RESPONSE: &str = "billed response";
const SETUP_ERROR: &str = "configuration unavailable";
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
    folds: Mutex<Vec<String>>,
    controls: Mutex<Vec<String>>,
    compacts: AtomicU32,
    compact_fails: std::sync::atomic::AtomicBool,
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
pub(super) struct ScriptedBackend {
    state: Arc<ScriptedState>,
    gate: Option<Arc<Gate>>,
    outcomes: Mutex<Vec<BackendResult>>,
    preparation: Option<super::AdmissionPreparation>,
    readiness: Option<super::PreparedReadiness>,
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
    pub(super) fn new() -> Self {
        Self {
            state: Arc::new(ScriptedState::default()),
            gate: None,
            outcomes: Mutex::new(Vec::new()),
            preparation: None,
            readiness: None,
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
        *backend.outcomes.lock().unwrap() = vec![BackendResult::SetupFailed(None)];
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

            if let Some(gate) = &self.gate {
                gate.wait().await;
            }
            if let Some(inbox) = &context.inbox {
                inbox.take_initial_submission();
                for input in inbox.drain_instructions() {
                    self.state.folds.lock().unwrap().push(input.message.clone());
                    for message in input.preamble {
                        history.push(message);
                    }
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
            if self.state.compact_fails.load(Ordering::SeqCst) {
                BackendResult::ControlFailed
            } else {
                BackendResult::CompactDone
            }
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
                    context.output.set_terminal_sender(sender.clone());
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

pub(super) fn input(message: &str) -> AgentInput {
    AgentInput {
        message: message.to_owned(),
        images: Vec::new(),
        preamble: Vec::new(),
        prompt: None,
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

#[test_case::test_case(None ; "generic_failure")]
#[test_case::test_case(Some(SETUP_ERROR) ; "backend_failure")]
fn setup_failure_settles_without_turn_history(error: Option<&str>) {
    smol::block_on(async {
        let backend = ScriptedBackend::new();
        *backend.outcomes.lock().unwrap() =
            vec![BackendResult::SetupFailed(error.map(|error| TurnFailure {
                kind: TurnFailureKind::Internal,
                diagnostic: error.into(),
                user_message: error.into(),
                retryable: true,
            }))];
        let (actor, task) = spawn(backend);
        let ticket = actor
            .admit_turn(input("setup"), None, "setup".into())
            .unwrap();
        let TurnOutcome::Failed { failure, .. } = ticket.wait().await else {
            panic!("setup failure must settle as Failed");
        };
        if let Some(error) = error {
            assert_eq!(failure.user_message, error);
        }
        let request = super::TranscriptRequest {
            through_turn: Some(ticket.turn_id()),
            last_messages: 10,
            max_bytes: 1024,
        };
        assert!(
            matches!(actor.transcript(request), Err(ActorError::UnavailableTurnHistory(id)) if id == ticket.turn_id())
        );
        actor.close();
        task.await;
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
            .submit(
                input("root"),
                None,
                "root".into(),
                super::TurnAdmissionOptions::default(),
            )
            .unwrap();
        let turn_id = loop {
            if let ActorEvent::Start { turn_id, .. } = events.recv_async().await.unwrap() {
                break turn_id;
            }
        };
        let ticket = actor.turn_ticket(turn_id).unwrap();
        gate.open();
        assert!(matches!(ticket.wait().await, TurnOutcome::Failed { .. }));
        until(|| events.drain().any(|event| matches!(event, ActorEvent::End(result) if result.outcome.turn_id() == turn_id))).await;
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
        let update = actor.reserve_prepared_operation(None).unwrap();
        update
            .commit_when_ready(Some(fast_change(true)))
            .await
            .unwrap();
        for events in [&first, &second] {
            assert!(
                matches!(events.recv_async().await.unwrap(), super::ActorEvent::Config(commit) if commit.config.fast)
            );
            assert!(matches!(
                events.recv_async().await.unwrap(),
                super::ActorEvent::Idle { .. }
            ));
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
            assert!(matches!(
                events.recv_async().await.unwrap(),
                super::ActorEvent::SubmissionConsumed(_)
            ));
            assert!(
                matches!(events.recv_async().await.unwrap(), super::ActorEvent::End(result) if result.outcome.turn_id() == ticket.turn_id())
            );
            assert!(
                matches!(events.recv_async().await.unwrap(), super::ActorEvent::Idle { agent_id, .. } if agent_id == actor.agent_id())
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
    super::retire_and_publish(
        &actor.inner,
        turn_id,
        outcome,
        &bare_admission(&actor, turn_id, 0),
        false,
        false,
        true,
    );
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
            parent: Arc::downgrade(&parent.inner),
            metadata: Default::default(),
        });
        let mut backend = ScriptedBackend::new();
        if preparation_panic {
            backend.preparation = Some(Arc::new(|_, _| panic!("{PANIC_DIAGNOSTIC}")));
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
        actor.configure_parent_delivery(true).unwrap();
        let events = parent.subscribe_events();
        let barrier = actor.reserve_prepared_operation(None).unwrap();
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
            .commit_when_ready(Some(super::ConfigChange::PatchAndMode {
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
            .await
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
        assert_eq!(envelope.result.outcome.agent_id(), actor.agent_id());
        let result = ticket.wait_result().await;
        assert_eq!(envelope.result.outcome, result.outcome);
        assert_eq!(envelope.result.text, result.text);
        assert_eq!(envelope.result.output, result.output);
        assert_eq!(envelope.result.provenance, result.provenance);
        assert_eq!(envelope.result.duration_ms, result.duration_ms);
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
        // Admission stamps the work with its own presentation, so a later
        // allocation only moves past it rather than reusing the displayed id.
        let admitted = actor.presentation_id();
        assert!(admitted > 9, "{admitted}");
        until(|| actor.processing_turn_id() == Some(ticket.turn_id())).await;
        assert_eq!(
            actor
                .allocate_processing_presentation(ticket.turn_id(), Some(admitted))
                .unwrap(),
            admitted,
            "a caller already displaying the current presentation keeps it"
        );
        let (turn, fence) = actor.cancel_local(admitted);
        assert_eq!(turn, Some(ticket.turn_id()));
        assert_eq!(fence, admitted + 1);
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
        let barrier = actor.reserve_prepared_operation(None).unwrap();
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
        let config = parent.reserve_prepared_operation(None).unwrap();
        let source_agent = AgentId::generate();
        let source_turn = crate::TurnId::generate();
        let envelope = Arc::new(super::ChildDelivery {
            label: None,
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
        let config = smol::spawn(config.commit_when_ready(Some(fast_change(true))));
        assert_eq!(state.entered.load(Ordering::SeqCst), 1);
        gate.open();
        active.wait().await;
        before.wait().await;
        config.await.unwrap();
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
        actor.configure_parent_delivery(true).unwrap();
        assert!(matches!(
            actor.configure_parent_delivery(false),
            Err(ActorError::Busy)
        ));
        let ticket = actor
            .admit_turn(input("first"), None, "first".into())
            .unwrap();
        ticket.wait().await;
        assert!(matches!(
            actor.configure_parent_delivery(false),
            Err(ActorError::Busy)
        ));
        actor.close();
        assert!(matches!(
            actor.configure_parent_delivery(false),
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
    backend.preparation = Some(Arc::new(move |_, _| {
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
        backend.preparation = Some(Arc::new(move |_, _| {
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
                Some(EventSender::new(presentation_tx)),
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
                actor.remove(turn_id).unwrap();
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
                matches!(transition, super::ActorEvent::Idle { agent_id, .. } if agent_id == handle.agent_id())
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
            super::ActorEvent::SubmissionConsumed(_)
        ));
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
            move |_, _| crate::agent::TurnAdmissionSnapshot {
                mode_def: Some(Arc::new(modes.current(&AgentMode::Build))),
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
        let barrier = handle.reserve_prepared_operation(None).unwrap();
        assert!(barrier.commit_when_ready(None).await.is_err());
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
fn failed_actor_is_reusable() {
    smol::block_on(async {
        let backend = ScriptedBackend::failing_setup();
        let state = Arc::clone(&backend.state);
        let (handle, task) = spawn(backend);
        let (sink, delivered) = flume::unbounded();
        let a = handle
            .admit_turn(input("one"), Some(EventSender::new(sink)), "a".into())
            .unwrap();
        // The actor synthesizes and delivers exactly one Failed outcome,
        // stamped with the admitted work so frontends can fence on it.
        let outcome = a.wait().await;
        assert!(matches!(outcome, TurnOutcome::Failed { num_turns: 0, .. }));
        let terminals: Vec<_> = delivered.try_iter().collect();
        assert!(matches!(
            terminals.as_slice(),
            [Envelope { event: AgentEvent::TurnOutcome(TurnOutcome::Failed { .. }), identity: Some(identity), .. }]
                if identity.turn_id() == Some(a.turn_id())
        ));

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
        let state = Arc::clone(&backend.state);
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
                Some(EventSender::new(tx)),
                "t3".into(),
            )
            .unwrap();

        until(|| state.entered.load(Ordering::SeqCst) == 1).await;
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
            .submit(
                input("root"),
                None,
                "r1".into(),
                super::TurnAdmissionOptions::default(),
            )
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
            .push_control("cancelled".into(), "cancelled".into(), None)
            .unwrap();

        popped.recv_async().await.unwrap();
        handle.cancel_existing();
        release.send(()).unwrap();
        until(|| handle.snapshot().queued == 0).await;
        assert!(state.controls.lock().unwrap().is_empty());

        handle
            .push_control("surviving".into(), "surviving".into(), None)
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
        let change = handle.reserve_prepared_operation(None).unwrap();
        change
            .commit_when_ready(Some(fast_change(true)))
            .await
            .unwrap();
        handle
            .push_control("fence".into(), "fence".into(), None)
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

/// An idle compaction is no turn, so it reports how it ended on its own,
/// a failure included, for every frontend's plugins to hear.
#[test_case::test_case(false, CompactionStatus::Completed; "completed")]
#[test_case::test_case(true, CompactionStatus::Failed; "failed")]
fn idle_compaction_reports_how_it_ended(fails: bool, expected: CompactionStatus) {
    smol::block_on(async {
        let backend = ScriptedBackend::new();
        backend.state.compact_fails.store(fails, Ordering::SeqCst);
        let (handle, task) = spawn(backend);
        let events = handle.subscribe_events();
        handle.push_compact(1, None).unwrap();
        let status = loop {
            if let ActorEvent::CompactionEnd(status) = events.recv_async().await.unwrap() {
                break status;
            }
        };
        assert_eq!(status, expected);
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
        let events = handle.subscribe_events();
        let (popped, release) = handle.pause_after_next_pop();
        handle.push_compact(1, None).unwrap();

        popped.recv_async().await.unwrap();
        handle.cancel_existing();
        release.send(()).unwrap();
        until(|| handle.snapshot().queued == 0).await;
        assert_eq!(state.compacts.load(Ordering::SeqCst), 0);
        let ended = loop {
            if let ActorEvent::CompactionEnd(status) = events.recv_async().await.unwrap() {
                break status;
            }
        };
        assert_eq!(ended, CompactionStatus::Cancelled);

        handle.push_compact(2, None).unwrap();
        handle
            .push_control("fence".into(), "fence".into(), None)
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
        let events = handle.subscribe_events();
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
        // The queued turn never popped. Settlement must name the work that was
        // finalized, not the turn still running.
        let settled = handle
            .inner
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .settled
            .expect("the cancelled turn settled as work");
        assert_eq!(
            settled.turn_id(),
            Some(second.turn_id()),
            "settlement names the cancelled queued turn"
        );
        gate.open();
        assert!(matches!(first.wait().await, TurnOutcome::Completed { .. }));

        handle.close();
        task.await;
        drop(events);
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
            .admit_turn(input("work"), Some(EventSender::new(tx)), "work".into())
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
        assert!(
            matches!(presentation.recv_async().await.unwrap().event, AgentEvent::TurnOutcome(delivered) if delivered == outcome)
        );
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

#[test_case::test_case(false, false; "completed")]
#[test_case::test_case(true, false; "setup_failure")]
#[test_case::test_case(false, true; "queued_cancel")]
fn ui_terminal_observes_sealed_immutable_result(setup_failure: bool, cancel: bool) {
    smol::block_on(async {
        let backend = if setup_failure {
            ScriptedBackend::failing_setup()
        } else {
            ScriptedBackend::new()
        };
        let (start, ready) = flume::bounded(1);
        let (handle, task) = AgentActorHandle::spawn_inner(
            AgentId::generate(),
            Vec::new(),
            None,
            Box::new(backend),
            None,
            None,
            Some(ready),
        );
        let observed_ticket = Arc::new(Mutex::new(None::<super::TurnTicket>));
        let ticket_slot = Arc::clone(&observed_ticket);
        let observed = Arc::new(AtomicUsize::new(0));
        let observed_count = Arc::clone(&observed);
        let (tx, rx) = flume::unbounded();
        let sender = EventSender::new(tx).with_observer(move |event| {
            if let AgentEvent::TurnOutcome(outcome) = event {
                let ticket = ticket_slot.lock().unwrap().clone().unwrap();
                let sealed = ticket.peek_result().expect("UI terminal preceded sealing");
                assert_eq!(&sealed.outcome, outcome);
                ticket.output().append_text("late text");
                ticket
                    .output()
                    .append_output(Value::String("late output".into()));
                let unchanged = ticket.peek_result().unwrap();
                assert_eq!(unchanged.text, sealed.text);
                assert_eq!(unchanged.output, sealed.output);
                observed_count.fetch_add(1, Ordering::SeqCst);
            }
        });
        let ticket = handle
            .admit_turn(input("work"), Some(sender), "work".into())
            .unwrap();
        *observed_ticket.lock().unwrap() = Some(ticket.clone());
        if cancel {
            handle.cancel_turn(ticket.turn_id()).unwrap();
        }
        start.send(()).unwrap();
        ticket.wait().await;
        assert_eq!(observed.load(Ordering::SeqCst), 1);
        assert_eq!(rx.drain().count(), 1);
        handle.close();
        task.await;
    });
}

#[test]
fn entered_run_is_never_emitted_twice() {
    smol::block_on(async {
        let backend = ScriptedBackend::new();
        let (handle, task) = spawn(backend);
        let (tx, rx) = flume::unbounded();
        let ticket = handle
            .admit_turn(input("work"), Some(EventSender::new(tx)), "w".into())
            .unwrap();
        ticket.wait().await;
        assert!(matches!(
            rx.try_recv().unwrap().event,
            AgentEvent::TurnOutcome(_)
        ));
        assert!(rx.is_empty());
        assert!(handle.outcome(ticket.turn_id()).is_some());

        // Close after completion does not re-emit either.
        handle.close();
        assert_eq!(rx.drain().count(), 0);
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
            .admit_turn(input("work"), Some(EventSender::new(tx)), "w".into())
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
fn shared_submission_retains_execution_provenance(
    origin_boundary: bool,
    generation_boundary: bool,
) {
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
            .submit(
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
        let mut queued = provenance.clone();
        if origin_boundary {
            queued.origin = super::TurnOrigin::User;
        }
        if generation_boundary {
            queued.plugin_generation = Some(2);
        }
        actor
            .submit(
                input("queued"),
                None,
                "queued".into(),
                super::TurnAdmissionOptions {
                    provenance: queued.clone(),
                    ..Default::default()
                },
            )
            .unwrap();
        gate.open();
        ticket.wait().await;
        until(|| !actor.has_pending_work()).await;
        assert_eq!(state.runs.lock().unwrap().len(), 1);
        assert_eq!(state.folds.lock().unwrap().as_slice(), ["queued"]);
        assert_eq!(ticket.provenance(), &provenance);
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
            .submit(
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
        // The backend drains accepted instructions only after the gate opens.
        until(|| state.entered.load(Ordering::SeqCst) > 0).await;
        handle
            .submit(
                input("fold-me"),
                None,
                "r1".into(),
                super::TurnAdmissionOptions::default(),
            )
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
            .submit(
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
            .submit(
                input("fold-me"),
                None,
                "r1".into(),
                super::TurnAdmissionOptions::default(),
            )
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
fn snapshot_queued_excludes_controls_from_pending_work_count() {
    smol::block_on(async {
        let gate = Gate::new();
        let (handle, task) = spawn(ScriptedBackend::gated(Arc::clone(&gate)));
        let running = handle
            .admit_turn(input("running"), None, "running".into())
            .unwrap();
        until(|| handle.snapshot().status != ActorStatus::Idle).await;
        handle
            .push_control("pin".into(), "control".into(), None)
            .unwrap();
        handle
            .submit(
                input("queued"),
                None,
                "queued".into(),
                super::TurnAdmissionOptions::default(),
            )
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
        let events = handle.subscribe_events();
        handle
            .push_control("pin".into(), "c1".into(), None)
            .unwrap();
        until(|| !state.controls.lock().unwrap().is_empty()).await;
        assert_eq!(state.controls.lock().unwrap().as_slice(), ["pin"]);
        let snapshot = handle.snapshot();
        assert_eq!(snapshot.latest, None, "control must not produce an outcome");
        assert_eq!(snapshot.queued, 0);
        // A control retires no turn, so its settlement identity is recorded
        // when it finishes rather than when an outcome is finalized.
        loop {
            match events.recv_async().await.unwrap() {
                super::ActorEvent::Idle {
                    settled: Some(settled),
                    ..
                } => {
                    assert!(
                        matches!(settled.kind, super::ActorWorkKind::Control { .. }),
                        "the control settlement names the control"
                    );
                    break;
                }
                super::ActorEvent::Idle { settled: None, .. } => {
                    panic!("the control settlement must carry its identity");
                }
                _ => {}
            }
        }
        handle.close();
        task.await;
    });
}

#[test]
fn control_settles_after_a_turn_cancelled_while_it_ran() {
    smol::block_on(async {
        let (handle, task) = spawn(ScriptedBackend::new());
        let events = handle.subscribe_events();
        let (popped, release) = handle.pause_after_next_pop();
        handle
            .push_control("pin".into(), "c1".into(), None)
            .unwrap();
        popped.recv_async().await.unwrap();
        let queued = handle
            .admit_turn(input("queued"), None, "q".into())
            .unwrap();
        handle.cancel_turn(queued.turn_id()).unwrap();
        assert!(matches!(queued.wait().await, TurnOutcome::Cancelled { .. }));
        release.send(()).unwrap();
        let settled = loop {
            if let super::ActorEvent::Idle { settled, .. } = events.recv_async().await.unwrap() {
                break settled;
            }
        };
        assert!(
            settled.is_some_and(|settled| matches!(
                settled.kind,
                super::ActorWorkKind::Control { .. }
            )),
            "the last work to settle was the control: {settled:?}"
        );
        handle.close();
        task.await;
    });
}

fn bare_admission(
    actor: &AgentActorHandle,
    turn_id: TurnId,
    presentation: u64,
) -> super::TurnAdmission {
    super::TurnAdmission {
        turn_id,
        input: None,
        event_sender: None,
        correlation: String::new(),
        work: crate::ActorWorkIdentity::new(
            actor.agent_id(),
            crate::ActorWorkKind::Turn {
                operation_id: presentation,
                turn_id,
            },
            presentation,
        ),
        generation: 0,
        policy: None,
        ticket: crate::TurnTicket::new(turn_id, actor.identity()),
        admission: None,
    }
}

#[test]
fn duplicate_finalize_keeps_the_first_settled_identity() {
    let (actor, task) = AgentActorHandle::spawn(
        AgentId::generate(),
        Vec::new(),
        None,
        Box::new(ScriptedBackend::new()),
    );
    let settle = |turn_id: TurnId, presentation: u64| {
        super::retire_and_publish(
            &actor.inner,
            turn_id,
            super::cancelled_outcome(actor.agent_id(), turn_id, TurnCancellationReason::User),
            &bare_admission(&actor, turn_id, presentation),
            false,
            false,
            true,
        );
    };
    let reconciled = TurnId::generate();
    settle(reconciled, 1);
    // A repeated finalize of already-retired work publishes nothing, so it must
    // not move the settled identity either.
    settle(reconciled, 2);
    assert_eq!(
        actor
            .inner
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .settled
            .map(|settled| settled.presentation_id),
        Some(1),
        "a duplicate finalize keeps the identity that actually settled"
    );
    actor.close();
    smol::block_on(task);
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
        let sender = EventSender::new(tx);
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
        let reservation = handle.reserve_prepared_operation(None).unwrap();
        reservation
            .commit_when_ready(Some(super::ConfigChange::Patch(
                super::ConfigPatch::default(),
            )))
            .await
            .unwrap();
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
        until(|| turn_popped(&handle)).await;
        let options = handle.reserve_prepared_operation(None).unwrap();
        let reservation = handle.reserve_prepared_operation(None).unwrap();
        let next = handle.clone();
        let b = next.admit_turn(input("b"), None, "b".into()).unwrap();
        assert!(handle.policy_snapshot().is_some_and(|p| !p.fast));
        let options = smol::spawn(options.commit_when_ready(Some(fast_change(true))));
        let reservation = smol::spawn(
            reservation.commit_when_ready(Some(super::ConfigChange::Mode {
                mode: AgentMode::Plan("plan.md".into()),
                mode_def: Some(
                    crate::modes::ModeRegistry::builtin()
                        .current(&AgentMode::Plan("plan.md".into())),
                ),
            })),
        );
        options.await.unwrap();
        reservation.await.unwrap();
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

        let failed = handle.reserve_prepared_operation(None).unwrap();
        let c = handle.admit_turn(input("c"), None, "c".into()).unwrap();
        assert!(matches!(
            failed
                .commit_when_ready(Some(super::ConfigChange::Patch(super::ConfigPatch {
                    thinking: Some(maki_providers::ThinkingConfig::Off),
                    toggle_thinking: true,
                    ..Default::default()
                })))
                .await,
            Err(ActorError::InvalidConfig(_))
        ));
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
        let reservation = handle.reserve_prepared_operation(None).unwrap();
        handle.push_compact(7, Some("compact".into())).unwrap();
        let snapshot = handle.snapshot();
        assert_eq!(snapshot.queued, 1);
        assert_eq!(
            snapshot.queue,
            [QueueProjection::Compact(Some("compact".into()))]
        );
        reservation
            .commit_when_ready(Some(super::ConfigChange::Mode {
                mode: AgentMode::Plan("pinned-plan.md".into()),
                mode_def: Some(
                    crate::modes::ModeRegistry::builtin()
                        .current(&AgentMode::Plan("pinned-plan.md".into())),
                ),
            }))
            .await
            .unwrap();
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
        let first = handle.reserve_prepared_operation(None).unwrap();
        let second = handle.reserve_prepared_operation(None).unwrap();
        let second =
            smol::spawn(
                second.commit_when_ready(Some(super::ConfigChange::Mode {
                    mode: AgentMode::Plan("plan.md".into()),
                    mode_def: Some(
                        crate::modes::ModeRegistry::builtin()
                            .current(&AgentMode::Plan("plan.md".into())),
                    ),
                })),
            );
        assert!(!handle.policy_snapshot().unwrap().fast);
        let after = handle
            .admit_turn(input("after"), None, "after".into())
            .unwrap();
        first
            .commit_when_ready(Some(fast_change(true)))
            .await
            .unwrap();
        second.await.unwrap();
        after.wait().await;
        assert_eq!(
            state.policies.lock().unwrap()[0],
            ("after".into(), Some(true), 2)
        );
        assert_eq!(
            handle.effective_config().unwrap().mode,
            AgentMode::Plan("plan.md".into())
        );
        let pending = handle.reserve_prepared_operation(None).unwrap();
        let cancelled = handle
            .admit_turn(input("cancelled"), None, "cancelled".into())
            .unwrap();
        handle.cancel_existing();
        pending
            .commit_when_ready(Some(fast_change(true)))
            .await
            .unwrap();
        assert!(matches!(
            cancelled.wait().await,
            TurnOutcome::Cancelled { .. }
        ));
        let after_cancel = handle
            .admit_turn(input("cancelled"), None, "cancelled".into())
            .unwrap();
        assert_eq!(after_cancel.wait().await.agent_id(), handle.agent_id());
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
        let pending = handle.reserve_prepared_operation(None).unwrap();
        let gate = Gate::new();
        let setter_gate = Arc::clone(&gate);
        let setter = smol::spawn(async move {
            setter_gate.wait().await;
            pending.commit_when_ready(Some(fast_change(true))).await
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
        let first = handle.reserve_prepared_operation(None).unwrap();
        let b = handle.admit_turn(input("b"), None, "b".into()).unwrap();
        let second = handle.reserve_prepared_operation(None).unwrap();
        let c = handle.admit_turn(input("c"), None, "c".into()).unwrap();
        let second = smol::spawn(second.commit_when_ready(Some(fast_change(false))));
        assert!(b.peek().is_none());
        first
            .commit_when_ready(Some(fast_change(true)))
            .await
            .unwrap();
        b.wait().await;
        second.await.unwrap();
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
        let first = handle.reserve_prepared_operation(None).unwrap();
        let b = handle.admit_turn(input("b"), None, "b".into()).unwrap();
        let second = handle.reserve_prepared_operation(None).unwrap();
        let c = handle.admit_turn(input("c"), None, "c".into()).unwrap();
        first
            .commit_when_ready(Some(fast_change(true)))
            .await
            .unwrap();
        b.wait().await;
        assert!(c.peek().is_none());
        assert_eq!(
            *observed.policies.lock().unwrap(),
            vec![("b".into(), Some(true), 1)]
        );
        second
            .commit_when_ready(Some(fast_change(false)))
            .await
            .unwrap();
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
        let reservation = handle.reserve_prepared_operation(None).unwrap();
        let removed = handle
            .admit_turn(input("removed"), None, "removed".into())
            .unwrap();
        let cleared = handle
            .admit_turn(input("cleared"), None, "cleared".into())
            .unwrap();
        handle
            .submit(
                input("root"),
                None,
                "root".into(),
                super::TurnAdmissionOptions::default(),
            )
            .unwrap();
        let snapshot = handle.snapshot();
        assert_eq!(snapshot.queued, snapshot.queue.len());
        assert!(
            snapshot
                .queue
                .contains(&QueueProjection::Turn("removed".into()))
        );
        assert_eq!(
            handle
                .pending_submissions()
                .iter()
                .map(|entry| entry.text.as_str())
                .collect::<Vec<_>>(),
            ["removed", "cleared", "root"]
        );
        handle.remove(removed.turn_id()).unwrap();
        assert!(matches!(
            removed.wait().await,
            TurnOutcome::Cancelled { .. }
        ));
        let submission = handle.pending_submissions().pop().unwrap();
        assert_eq!(
            handle.remove_submission(submission.id).unwrap().text,
            "root"
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
            .submit(
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
        for message in ["old", "old too"] {
            handle
                .submit(
                    input(message),
                    None,
                    message.into(),
                    super::TurnAdmissionOptions::default(),
                )
                .unwrap();
        }
        let reservation = handle.reserve_prepared_operation(None).unwrap();
        let reservation = smol::spawn(reservation.commit_when_ready(Some(fast_change(true))));
        handle
            .submit(
                input("new"),
                None,
                "new".into(),
                super::TurnAdmissionOptions::default(),
            )
            .unwrap();
        gate.open();
        active.wait().await;
        reservation.await.unwrap();
        until(|| observed.entered.load(Ordering::SeqCst) == 2).await;
        assert_eq!(
            observed.folds.lock().unwrap().as_slice(),
            &["old", "old too"]
        );
        assert_eq!(observed.policies.lock().unwrap()[0].2, 0);
        assert_eq!(observed.policies.lock().unwrap()[1].2, 1);
        assert_eq!(observed.runs.lock().unwrap()[1].1, "new");
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
        let first = handle.reserve_prepared_operation(None).unwrap();
        handle
            .submit(
                input("b"),
                None,
                "b".into(),
                super::TurnAdmissionOptions::default(),
            )
            .unwrap();
        let second = handle.reserve_prepared_operation(None).unwrap();
        handle
            .submit(
                input("c"),
                None,
                "c".into(),
                super::TurnAdmissionOptions::default(),
            )
            .unwrap();
        let second = smol::spawn(second.commit_when_ready(Some(fast_change(false))));
        first
            .commit_when_ready(Some(fast_change(true)))
            .await
            .unwrap();
        second.await.unwrap();
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
        let reservation = handle.reserve_prepared_operation(None).unwrap();
        let cancelled = handle
            .admit_turn(input("cancelled"), None, "cancelled".into())
            .unwrap();
        handle.cancel_turn(cancelled.turn_id()).unwrap();
        assert!(matches!(
            cancelled.wait().await,
            TurnOutcome::Cancelled { .. }
        ));
        reservation
            .commit_when_ready(Some(fast_change(true)))
            .await
            .unwrap();
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
        let reservation = handle.reserve_prepared_operation(None).unwrap();
        handle.cancel_correlation("deferred", TurnCancellationReason::User);
        let ticket = handle
            .admit_turn(input("cancelled"), None, "deferred".into())
            .unwrap();
        reservation
            .commit_when_ready(Some(fast_change(true)))
            .await
            .unwrap();
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
        let reservation = handle.reserve_prepared_operation(None).unwrap();
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
        let reservation = handle.reserve_prepared_operation(None).unwrap();
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
        until(|| state.entered.load(Ordering::SeqCst) == 1).await;
        handle.cancel_existing();
        handle
            .submit(
                input("new"),
                None,
                "new".into(),
                super::TurnAdmissionOptions::default(),
            )
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
        until(|| turn_popped(&handle)).await;
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

/// The runner popped a turn. Its backend may not have entered yet, so a
/// cancel issued now can settle the turn before it runs.
fn turn_popped(handle: &AgentActorHandle) -> bool {
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
        until(|| turn_popped(&handle)).await;
        let t2 = handle
            .admit_turn(input("two"), None, "drop".into())
            .unwrap();
        let t3 = handle
            .admit_turn(input("three"), None, "keep".into())
            .unwrap();
        // Queue root work sharing the "drop" correlation, behind the turns.
        handle
            .submit(
                input("root-drop"),
                None,
                "drop".into(),
                super::TurnAdmissionOptions::default(),
            )
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
            .admit_turn(input("late"), Some(EventSender::new(tx)), "run-5".into())
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
            .submit(
                input("late-root"),
                None,
                "run-5".into(),
                super::TurnAdmissionOptions::default(),
            )
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
fn remove_turn_terminalizes_exact_queued_turn() {
    smol::block_on(async {
        let gate = Gate::new();
        let backend = ScriptedBackend::gated(Arc::clone(&gate));
        let (handle, task) = spawn(backend);
        let t1 = handle
            .admit_turn(input("running"), None, "r1".into())
            .unwrap();
        until(|| turn_popped(&handle)).await;

        let t2 = handle
            .admit_turn(input("queued"), None, "r2".into())
            .unwrap();
        let t3 = handle
            .admit_turn(input("queued2"), None, "r3".into())
            .unwrap();

        assert!(matches!(
            handle.remove(t2.turn_id()).unwrap(),
            TurnOutcome::Cancelled { .. }
        ));
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
fn remove_unknown_turn_is_rejected() {
    smol::block_on(async {
        let backend = ScriptedBackend::new();
        let (handle, task) = spawn(backend);
        let turn_id = TurnId::generate();
        assert_eq!(
            handle.remove(turn_id),
            Err(ActorError::UnknownTurn(turn_id))
        );
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
        until(|| turn_popped(&handle)).await;
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

#[test_case::test_case(false; "close")]
#[test_case::test_case(true; "shutdown")]
fn close_cut_wins_over_completed_backend_with_joined_input(shutdown: bool) {
    smol::block_on(async {
        const FIRST: &str = "initial task";
        const JOINED: &str = "joined instruction";
        let gate = Gate::new();
        let backend = ScriptedBackend::gated(Arc::clone(&gate));
        let state = Arc::clone(&backend.state);
        let (actor, task) = spawn(backend);
        let first = actor
            .submit(input(FIRST), None, FIRST.into(), Default::default())
            .unwrap();
        until(|| state.entered.load(Ordering::SeqCst) == 1).await;
        let joined = actor
            .submit(input(JOINED), None, JOINED.into(), Default::default())
            .unwrap();
        assert_eq!(first.turn_id(), joined.turn_id());
        let (captured, cut) = flume::bounded(1);
        let (release, released) = flume::bounded(1);
        *actor.inner.after_close_cut.lock().unwrap() = Some((captured, released));
        let closing = actor.clone();
        let thread = std::thread::spawn(move || {
            if shutdown {
                closing.shutdown()
            } else {
                closing.close()
            }
        });
        cut.recv_async().await.unwrap();
        gate.open();
        let outcome = first.wait().await;
        release.send(()).unwrap();
        thread.join().unwrap();
        task.await;
        let reason = if shutdown {
            TurnCancellationReason::Shutdown
        } else {
            TurnCancellationReason::Closed
        };
        assert!(state.folds.lock().unwrap().is_empty());
        assert!(
            matches!(outcome, TurnOutcome::Cancelled { reason: actual, .. } if actual == reason)
        );
        assert!(
            matches!(joined.wait().await, TurnOutcome::Cancelled { reason: actual, .. } if actual == reason)
        );
    });
}

#[test]
fn local_cancel_cut_wins_over_completed_backend_with_joined_input() {
    smol::block_on(async {
        const FIRST: &str = "initial task";
        const JOINED: &str = "joined instruction";
        let gate = Gate::new();
        let backend = ScriptedBackend::gated(Arc::clone(&gate));
        let state = Arc::clone(&backend.state);
        let (actor, task) = spawn(backend);
        let first = actor
            .submit(input(FIRST), None, FIRST.into(), Default::default())
            .unwrap();
        until(|| state.entered.load(Ordering::SeqCst) == 1).await;
        let joined = actor
            .submit(input(JOINED), None, JOINED.into(), Default::default())
            .unwrap();
        assert_eq!(first.turn_id(), joined.turn_id());
        let cut = {
            let mut state = actor.inner.state.lock().unwrap();
            actor.capture_local_cancel(&mut state, 0)
        };
        gate.open();
        let outcome = first.wait().await;
        actor.settle_local_cancel(cut);
        actor.close();
        task.await;
        assert!(state.folds.lock().unwrap().is_empty());
        assert!(matches!(
            outcome,
            TurnOutcome::Cancelled {
                reason: TurnCancellationReason::User,
                ..
            }
        ));
        assert!(matches!(joined.wait().await, TurnOutcome::Cancelled { .. }));
    });
}

#[test_case::test_case(false; "unresolved")]
#[test_case::test_case(true; "ready")]
fn local_cancel_discards_reserved_prepared_turn(ready: bool) {
    smol::block_on(async {
        const TEXT: &str = "implementation";
        let (actor, task) = spawn(ScriptedBackend::new());
        actor
            .initialize_config(super::EffectiveAgentConfig::new(
                policy(false),
                AgentMode::Build,
            ))
            .unwrap();
        let operation = actor
            .reserve_prepared_operation(Some(super::PreparedTurn {
                input: input(TEXT),
                event_sender: None,
                correlation: TEXT.into(),
            }))
            .unwrap();
        if ready {
            operation.resolve(None).unwrap();
            operation.wait_ready().await.unwrap();
        }
        actor.cancel_local(0);
        let result = operation.resolve(None);
        actor.close();
        task.await;
        assert!(matches!(result, Err(ActorError::PolicyCancelled)));
    });
}

#[test_case::test_case(false; "cancelled")]
#[test_case::test_case(true; "equivalent_commit")]
fn steering_resumes_after_reserved_config_disappears(commit: bool) {
    smol::block_on(async {
        const TEXT: &str = "instruction";
        let gate = Gate::new();
        let backend = ScriptedBackend::gated(Arc::clone(&gate));
        let state = Arc::clone(&backend.state);
        let (actor, task) = spawn(backend);
        actor
            .initialize_config(super::EffectiveAgentConfig::new(
                policy(false),
                AgentMode::Build,
            ))
            .unwrap();
        let first = actor
            .submit(input(TEXT), None, TEXT.into(), Default::default())
            .unwrap();
        until(|| state.entered.load(Ordering::SeqCst) == 1).await;
        let config = actor.reserve_prepared_operation(None).unwrap();
        if commit {
            config
                .commit_when_ready(Some(fast_change(false)))
                .await
                .unwrap();
        } else {
            drop(config);
        }
        let joined = actor
            .submit(input(TEXT), None, TEXT.into(), Default::default())
            .unwrap();
        gate.open();
        first.wait().await;
        joined.wait().await;
        actor.close();
        task.await;
        assert_eq!(first.turn_id(), joined.turn_id());
    });
}

#[test]
fn shared_submission_preserves_large_images_and_preamble() {
    smol::block_on(async {
        const TEXT: &str = "image instruction";
        const PREAMBLE: &str = "shell output";
        let (start, rx) = flume::bounded(1);
        let (actor, task) = AgentActorHandle::spawn_inner(
            AgentId::generate(),
            Vec::new(),
            None,
            Box::new(ScriptedBackend::new()),
            None,
            None,
            Some(rx),
        );
        let mut request = input(TEXT);
        request.images = vec![ImageSource::new(
            ImageMediaType::Png,
            Arc::from("a".repeat(super::MAX_PENDING_INSTRUCTION_BYTES + 1)),
        )];
        request.preamble = vec![Message::user(PREAMBLE.into())];
        let images = request.images.clone();
        let ticket = actor.submit(request, None, TEXT.into(), Default::default());
        if let Ok(ticket) = &ticket {
            let state = actor.inner.state.lock().unwrap();
            let inbox = &state.instructions[&ticket.turn_id()];
            assert_eq!(inbox.initial.as_ref().unwrap().images, images);
        }
        start.send(()).unwrap();
        ticket.unwrap().wait().await;
        let history = actor.inner.history.load_full();
        assert_eq!(
            history
                .messages
                .iter()
                .filter_map(Message::first_text_content)
                .collect::<Vec<_>>(),
            vec![PREAMBLE, TEXT]
        );
        actor.close();
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
            let initial = input(TEXT);
            let overhead = serde_json::to_vec(&initial.preamble).unwrap().len();
            let text = "é".repeat(
                (super::MAX_PENDING_INSTRUCTION_BYTES - TEXT.len() - 2 * overhead) / "é".len(),
            );
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
fn graph_quiescence_probe_detects_extra_execution() {
    smol::block_on(async {
        const TEXT: &str = "probe";
        let (actor, task) = spawn(ScriptedBackend::new());
        let first = super::graph_quiescence_barrier(slice::from_ref(&actor))
            .await
            .unwrap();
        let ticket = actor
            .submit(input(TEXT), None, TEXT.into(), Default::default())
            .unwrap();
        ticket.wait().await;
        let second = super::graph_quiescence_barrier(slice::from_ref(&actor))
            .await
            .unwrap();
        assert!(second.iter().all(super::ActorRuntimeProbe::is_quiescent));
        assert_eq!(second[0].retained_results, first[0].retained_results + 1);
        actor.close();
        task.await;
    });
}

#[test_case::test_case(false; "materialized")]
#[test_case::test_case(true; "reserved")]
fn compact_removal_uses_unique_operation_id(reserved: bool) {
    smol::block_on(async {
        const RUN: u64 = 19;
        const FIRST: &str = "first compact";
        const SECOND: &str = "second compact";
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
        let reservation = reserved.then(|| actor.reserve_prepared_operation(None).unwrap());
        actor.push_compact(RUN, Some(FIRST.into())).unwrap();
        actor.push_compact(RUN, Some(SECOND.into())).unwrap();
        let entries = actor.pending_queue();
        let ids: Vec<_> = entries
            .iter()
            .filter_map(|entry| match entry {
                super::PendingQueueEntry::Compact { operation_id, .. } => Some(*operation_id),
                _ => None,
            })
            .collect();
        assert_eq!(ids.len(), 2);
        assert_ne!(ids[0], ids[1]);
        assert!(
            matches!(actor.remove_pending_compact(ids[1]), Some(super::PendingQueueEntry::Compact { instructions: Some(text), .. }) if text == SECOND)
        );
        assert!(actor.remove_pending_compact(ids[1]).is_none());
        assert!(
            matches!(&actor.pending_queue()[..], [super::PendingQueueEntry::Compact { instructions: Some(text), .. }] if text == FIRST)
        );
        drop(reservation);
        actor.close();
        task.await;
    });
}

#[test]
fn committed_message_count_tracks_live_history() {
    smol::block_on(async {
        const TEXT: &str = "initial task";
        let gate = Gate::new();
        let backend = ScriptedBackend::gated(Arc::clone(&gate));
        let (actor, task) = spawn(backend);
        assert_eq!(actor.committed_message_count(), 0);
        let ticket = actor
            .submit(input(TEXT), None, TEXT.into(), Default::default())
            .unwrap();
        until(|| {
            actor
                .inner
                .history
                .load()
                .messages
                .first()
                .and_then(Message::first_text_content)
                == Some(TEXT)
        })
        .await;
        assert!(ticket.peek_result().is_none());
        assert_eq!(actor.committed_message_count(), 1);
        gate.open();
        ticket.wait().await;
        assert_eq!(actor.committed_message_count(), 1);
        actor.close();
        task.await;
    });
}

#[test]
fn pending_queue_preserves_compaction_boundary_order() {
    smol::block_on(async {
        const FIRST: &str = "first";
        const NEXT: &str = "next";
        const RUN: u64 = 19;
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
        actor
            .submit(input(FIRST), None, FIRST.into(), Default::default())
            .unwrap();
        actor.push_compact(RUN, None).unwrap();
        actor
            .submit(input(NEXT), None, NEXT.into(), Default::default())
            .unwrap();
        let entries = actor.pending_queue();
        assert!(
            matches!(&entries[..], [super::PendingQueueEntry::Message(first), super::PendingQueueEntry::Compact {run_id: RUN, ..}, super::PendingQueueEntry::Message(next)] if first.text == FIRST && next.text == NEXT)
        );
        let super::PendingQueueEntry::Compact { operation_id, .. } = entries[1] else {
            unreachable!()
        };
        assert!(actor.remove_pending_compact(operation_id).is_some());
        assert!(actor.remove_pending_compact(operation_id).is_none());
        actor.close();
        task.await;
    });
}

#[test_case::test_case(false; "accepted")]
#[test_case::test_case(true; "rejected")]
fn constrained_join_waits_for_ordered_policy(reject: bool) {
    smol::block_on(async {
        const FIRST: &str = "pending first";
        const NEXT: &str = "pending constrained next";
        const DENIED: &str = "ordered policy rejected";
        let (_start, rx) = flume::bounded(1);
        let backend = ScriptedBackend::new();
        let observations = Arc::clone(&backend.state);
        let (actor, task) = AgentActorHandle::spawn_inner(
            AgentId::generate(),
            Vec::new(),
            None,
            Box::new(backend),
            None,
            Some(Arc::new(super::EffectiveAgentConfig::new(
                policy(false),
                AgentMode::Build,
            ))),
            Some(rx),
        );
        let boundary = actor.reserve_prepared_operation(None).unwrap();
        let first = actor
            .submit(input(FIRST), None, FIRST.into(), Default::default())
            .unwrap();
        let constraint_actor = actor.clone();
        let calls = Arc::new(AtomicU32::new(0));
        let constraint_calls = Arc::clone(&calls);
        let joined = actor
            .submit(
                input(NEXT),
                None,
                NEXT.into(),
                super::TurnAdmissionOptions {
                    policy_constraint: Some(Arc::new(move |config| {
                        let _state = constraint_actor.inner.state.lock().unwrap();
                        assert!(config.fast);
                        constraint_calls.fetch_add(1, Ordering::SeqCst);
                        if reject { Err(DENIED.into()) } else { Ok(()) }
                    })),
                    ..Default::default()
                },
            )
            .unwrap();
        assert_eq!(joined.turn_id(), first.turn_id());
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert_eq!(actor.pending_submissions().len(), 2);
        boundary
            .commit_when_ready(Some(fast_change(true)))
            .await
            .unwrap();
        if reject {
            assert!(
                matches!(joined.wait().await, TurnOutcome::Failed { failure, .. }
                if failure.diagnostic == DENIED)
            );
            assert!(matches!(first.wait().await, TurnOutcome::Failed { .. }));
            assert!(actor.pending_submissions().is_empty());
            assert!(observations.runs.lock().unwrap().is_empty());
        } else {
            until(|| !actor.inner.queue.is_empty()).await;
            let pending = actor.pending_submissions();
            assert_eq!(pending.len(), 2);
            assert_eq!(pending[0].text, FIRST);
            assert_eq!(pending[1].text, NEXT);
            assert!(joined.peek().is_none());
        }
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        actor.close();
        task.await;
    });
}

#[test]
fn prepared_initial_submission_cannot_be_removed_and_constrained_join_uses_reserved_policy() {
    smol::block_on(async {
        const FIRST: &str = "prepared first";
        const NEXT: &str = "constrained next";
        let (entered_tx, entered_rx) = flume::bounded(1);
        let (release_tx, release_rx) = flume::bounded(1);
        let mut backend = ScriptedBackend::new();
        backend.preparation = Some(Arc::new(move |_, _| {
            entered_tx.send(()).unwrap();
            release_rx.recv().unwrap();
            crate::agent::TurnAdmissionSnapshot {
                mode_def: None,
                prompt_inputs: None,
                bindings: Arc::default(),
                mcp_startup_notice: None,
            }
        }));
        let (_start, rx) = flume::bounded(1);
        let (actor, task) = AgentActorHandle::spawn_inner(
            AgentId::generate(),
            Vec::new(),
            None,
            Box::new(backend),
            None,
            Some(Arc::new(super::EffectiveAgentConfig::new(
                policy(false),
                AgentMode::Build,
            ))),
            Some(rx),
        );
        let first = actor
            .submit(input(FIRST), None, FIRST.into(), Default::default())
            .unwrap();
        entered_rx.recv_async().await.unwrap();
        let constraint_actor = actor.clone();
        let joined = actor
            .submit(
                input(NEXT),
                None,
                NEXT.into(),
                super::TurnAdmissionOptions {
                    policy_constraint: Some(Arc::new(move |config| {
                        let _state = constraint_actor.inner.state.lock().unwrap();
                        assert_eq!(config.mode, AgentMode::Build);
                        Ok(())
                    })),
                    ..Default::default()
                },
            )
            .unwrap();
        assert_eq!(joined.turn_id(), first.turn_id());
        let initial = actor.pending_submissions()[0].clone();
        assert!(actor.remove_submission(initial.id).is_none());
        release_tx.send(()).unwrap();
        until(|| actor.inner.queue.has_prepared_turn(first.turn_id())).await;
        assert!(actor.remove_submission(initial.id).is_none());
        let pending = actor.pending_submissions();
        assert_eq!(pending.len(), 2);
        assert_eq!(pending[0].id, initial.id);
        assert_eq!(pending[0].text, FIRST);
        assert_eq!(pending[1].text, NEXT);
        assert!(actor.remove_submission(pending[1].id).is_some());
        actor.close();
        task.await;
    });
}

#[test]
fn stable_submission_removal_preserves_promoted_input_and_metadata() {
    smol::block_on(async {
        const FIRST: &str = "first";
        const NEXT: &str = "next";
        const LAST: &str = "last";
        const PREAMBLE: &str = "context";
        const NOTICE: usize = 3;
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
        let mut next = input(NEXT);
        next.images = vec![test_image()];
        next.preamble = vec![Message::user(PREAMBLE.into())];
        let joined = actor
            .submit(
                next,
                None,
                NEXT.into(),
                super::TurnAdmissionOptions {
                    already_displayed: true,
                    mcp_startup_notice: Some(NOTICE),
                    ..Default::default()
                },
            )
            .unwrap();
        actor
            .submit(input(LAST), None, LAST.into(), Default::default())
            .unwrap();
        assert_eq!(first.turn_id(), joined.turn_id());
        let pending = actor.pending_submissions();
        assert_eq!(pending.len(), 3);
        assert_eq!(actor.remove_submission(pending[0].id).unwrap().text, FIRST);
        assert!(actor.remove_submission(pending[0].id).is_none());
        assert_eq!(actor.remove_submission(pending[2].id).unwrap().text, LAST);
        let remaining = actor.pending_submissions().pop().unwrap();
        assert_eq!(remaining.id, pending[1].id);
        assert_eq!(remaining.turn_id, first.turn_id());
        assert_eq!(remaining.text, NEXT);
        assert_eq!(remaining.images, vec![test_image()]);
        assert!(remaining.already_displayed);
        assert_eq!(remaining.mcp_startup_notice, Some(NOTICE));
        let source = super::inbox::ActorTurnInbox::source(&actor.inner, first.turn_id(), None);
        let events = actor.subscribe_events();
        let consumed = source.take_initial_submission().unwrap();
        assert_eq!(consumed.id, remaining.id);
        assert!(events.drain().any(|event| matches!(event,
            ActorEvent::SubmissionConsumed(entry) if entry.id == remaining.id)));
        assert!(source.take_initial_submission().is_none());
        let promoted = match actor.inner.queue.pop().unwrap() {
            ActorWork::Turn(mut admission) => admission.input.take().unwrap(),
            _ => panic!("expected promoted turn"),
        };
        assert_eq!(promoted.message, NEXT);
        assert_eq!(promoted.images, vec![test_image()]);
        assert_eq!(
            serde_json::to_value(promoted.preamble).unwrap(),
            serde_json::to_value(vec![Message::user(PREAMBLE.into())]).unwrap()
        );
        actor.close();
        task.await;
    });
}

#[test]
fn queued_submissions_do_not_cross_control_boundary() {
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
        actor.push_control(TEXT.into(), TEXT.into(), None).unwrap();
        let next = actor
            .submit(input(TEXT), None, TEXT.into(), Default::default())
            .unwrap();
        let joined = actor
            .submit(input(TEXT), None, TEXT.into(), Default::default())
            .unwrap();
        assert_ne!(first.turn_id(), next.turn_id());
        assert_eq!(next.turn_id(), joined.turn_id());
        let pending = actor.pending_submissions();
        assert_eq!(
            pending
                .iter()
                .map(|entry| entry.turn_id)
                .collect::<Vec<_>>(),
            vec![first.turn_id(), next.turn_id(), next.turn_id()]
        );
        actor.close();
        task.await;
    });
}

#[test]
fn local_cancellation_preserves_pending_configuration_and_advances_fence() {
    smol::block_on(async {
        const TEXT: &str = "instruction";
        const DISPLAYED: u64 = 7;
        let (actor, task) = spawn(ScriptedBackend::new());
        actor
            .initialize_config(super::EffectiveAgentConfig::new(
                policy(false),
                AgentMode::Build,
            ))
            .unwrap();
        let config = actor.reserve_prepared_operation(None).unwrap();
        let ticket = actor
            .submit(input(TEXT), None, TEXT.into(), Default::default())
            .unwrap();
        let (active, fence) = actor.cancel_local(DISPLAYED);
        assert_eq!(active, None);
        assert_eq!(fence, DISPLAYED + 1);
        assert!(actor.pending_submissions().is_empty());
        assert!(matches!(
            ticket.wait().await,
            TurnOutcome::Cancelled {
                reason: TurnCancellationReason::User,
                ..
            }
        ));
        config
            .commit_when_ready(Some(fast_change(true)))
            .await
            .unwrap();
        assert!(actor.policy_snapshot().unwrap().fast);
        let next = actor
            .submit(input(TEXT), None, TEXT.into(), Default::default())
            .unwrap();
        assert!(matches!(next.wait().await, TurnOutcome::Completed { .. }));
        assert!(actor.policy_snapshot().unwrap().fast);
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
        let config = actor.reserve_prepared_operation(None).unwrap();
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
        child.configure_parent_delivery(true).unwrap();
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
            label: None,
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
