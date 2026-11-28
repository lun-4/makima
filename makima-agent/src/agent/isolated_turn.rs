use std::path::Path;
use std::sync::Arc;

use flume::Receiver;
use futures_lite::future;

use makima_providers::provider::Provider;
use makima_providers::{
    ConversationEnd, ConversationLink, ConversationRelation, ImageSource, Invocation,
    InvocationPurpose, Message, Model, ProviderEvent, RequestDeliveryTracker, RequestOptions,
};
use makima_storage::id::SessionRef;
use serde_json::Value;
use thiserror::Error;

use crate::actor::{ActorError, AgentActorHandle, EffectiveAgentConfig};
use crate::cancel::CancelToken;
use crate::context::{ContextBinding, ContextPolicy, RootContext, root_context};
use crate::journal::{JournalCommit, JournalError, JournalIncarnation};
use crate::prompt::ResolvedSlots;
use crate::state::{StateAccess, StateError};
use crate::template::env_vars_for;
use crate::{AgentMode, AgentRef, HistorySnapshot, ManagerError, ModeDef, ModeRegistry};

use super::streaming::{end_conversation_detached, fresh_id};
use super::{
    Instructions, PromptSlotSource, UNAVAILABLE_RESULT, build_system_prompt_with_def,
    close_dangling_tool_calls, load_instructions,
};

pub(crate) const OPERATION_CANCELLED: &str = "operation cancelled";
const FALLBACK_SYSTEM: &str =
    "You are a helpful coding assistant. Answer concisely from the conversation context.";

const BTW_REMINDER: &str = "<system-reminder>\nThis is a side question. Answer it directly in a \
single response.\n- You have NO tools: you cannot read files, run commands, or take any action.\n\
- One-off response: there are no follow-up turns.\n- Answer ONLY from the existing conversation \
context.\n- Never say \"Let me...\", \"I'll now...\", or promise any action.\n- If you don't know, \
say so; do not offer to look it up.\n</system-reminder>";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IsolatedTurnEvent {
    TextDelta(String),
    ThinkingDelta(String),
    Done,
    Cancelled,
    Error(String),
}

#[derive(Debug, Error)]
pub enum IsolatedTurnError {
    #[error(transparent)]
    Manager(#[from] ManagerError),
    #[error(transparent)]
    Actor(#[from] ActorError),
    #[error(transparent)]
    State(#[from] StateError),
    #[error(transparent)]
    Journal(#[from] JournalError),
    #[error("agent configuration is not initialized")]
    NoConfig,
}

#[derive(Clone)]
pub struct IsolatedPrompt {
    pub(crate) override_text: Option<Arc<str>>,
    pub(crate) append_text: Option<Arc<str>>,
    pub(crate) modes: Arc<ModeRegistry>,
    pub(crate) cold_slots: Option<PromptSlotSource>,
    pub(crate) session_id: Option<SessionRef>,
}

impl IsolatedPrompt {
    pub(crate) fn system(
        &self,
        cwd: &Path,
        mode: &AgentMode,
        definition: Option<&ModeDef>,
        instructions: &Instructions,
        slots: &ResolvedSlots,
    ) -> String {
        self.compose(|| {
            build_system_prompt_with_def(
                &env_vars_for(cwd),
                definition.unwrap_or(&self.modes.current(mode)),
                mode,
                &instructions.text,
                slots,
            )
        })
    }

    fn compose(&self, default: impl FnOnce() -> String) -> String {
        let mut system = self
            .override_text
            .as_deref()
            .map_or_else(default, str::to_owned);
        if let Some(append) = &self.append_text {
            system.push('\n');
            system.push_str(append);
        }
        system
    }
}

pub(crate) struct IsolatedSnapshot {
    pub(crate) config: Arc<EffectiveAgentConfig>,
    pub(crate) history: Arc<HistorySnapshot>,
    pub(crate) prompt: Option<IsolatedPrompt>,
}

impl AgentRef {
    pub fn ask_isolated(
        &self,
        question: String,
        images: Vec<ImageSource>,
        cancel: CancelToken,
    ) -> Result<Receiver<IsolatedTurnEvent>, IsolatedTurnError> {
        let (output, events) = flume::bounded(64);
        if cancel.is_cancelled() {
            let _ = output.send(IsolatedTurnEvent::Cancelled);
            return Ok(events);
        }
        self.validate_live()?;
        let incarnation = self.incarnation()?;
        let actor = self.manager().actor_for_incarnation(&incarnation)?;
        let IsolatedSnapshot {
            config,
            history,
            prompt,
        } = actor.isolated_snapshot()?;
        let recorded = root_context(&self.state_access())?;
        let definition = prompt
            .as_ref()
            .map(|prompt| prompt.modes.current(&AgentMode::Build));
        let cold_slots = prompt.as_ref().and_then(|prompt| prompt.cold_slots.clone());
        let journal = self.manager().journal();
        let session_id = prompt
            .as_ref()
            .and_then(|prompt| prompt.session_id.clone())
            .or_else(|| journal.session_id().map(SessionRef::from));
        let access = self.state_access();
        let (stop, cancel) = cancel.child();
        smol::spawn(async move {
            let closed = smol::spawn({
                let actor = actor.clone();
                async move {
                    actor.wait_closed().await;
                    stop.cancel();
                }
            });
            let recorded = if let (None, Some(slots)) = (&recorded, cold_slots) {
                cancel
                    .race(async {
                        let slots = slots.current().await;
                        initialize_context(
                            &actor,
                            access,
                            config.context.cwd(),
                            &slots,
                            incarnation.reservation,
                        )
                        .map(Some)
                    })
                    .await
            } else {
                Ok(Ok(recorded))
            };
            match recorded {
                Ok(Ok(recorded)) => {
                    let system = match (&prompt, recorded) {
                        (Some(prompt), Some(context)) => prompt.system(
                            config.context.cwd(),
                            &AgentMode::Build,
                            definition.as_ref(),
                            &context.instructions,
                            &context.slots,
                        ),
                        (Some(prompt), None) => prompt.compose(|| FALLBACK_SYSTEM.into()),
                        (None, _) => FALLBACK_SYSTEM.into(),
                    };
                    let previous = history.conversation.as_ref().and_then(|conversation| {
                        conversation.link(ConversationRelation::Branches, history.messages.len())
                    });
                    run_isolated_turn(
                        IsolatedTurnRequest {
                            provider: Arc::clone(&config.provider),
                            model: config.model.clone(),
                            history: history.messages.as_ref().clone(),
                            previous,
                            system,
                            question,
                            images,
                            session_id,
                            cancel,
                        },
                        output,
                    )
                    .await;
                }
                Ok(Err(error)) => {
                    let _ = output
                        .send_async(IsolatedTurnEvent::Error(error.to_string()))
                        .await;
                }
                Err(_) => {
                    let _ = output.send_async(IsolatedTurnEvent::Cancelled).await;
                }
            }
            closed.cancel().await;
        })
        .detach();
        Ok(events)
    }
}

fn initialize_context(
    actor: &AgentActorHandle,
    access: StateAccess,
    cwd: &Path,
    slots: &ResolvedSlots,
    reservation: u64,
) -> Result<Arc<RootContext>, IsolatedTurnError> {
    let idle = actor.prepare_idle().ok();
    if let Some(context) = root_context(&access)? {
        return Ok(context);
    }
    let journal = Arc::clone(&access.journal);
    let instructions = load_instructions(&cwd.to_string_lossy());
    let (context, update) = ContextBinding::capture(access)?.prepare(
        &instructions,
        slots,
        true,
        ContextPolicy::Admission,
    )?;
    if idle.is_some() {
        journal.commit_for(
            JournalIncarnation {
                agent: actor.agent_id(),
                reservation,
            },
            JournalCommit {
                context_updates: vec![update],
                ..Default::default()
            },
        )?;
    }
    Ok(context)
}

pub async fn collect_answer(events: Receiver<IsolatedTurnEvent>) -> Result<String, String> {
    let mut answer = String::new();
    while let Ok(event) = events.recv_async().await {
        match event {
            IsolatedTurnEvent::TextDelta(text) => answer.push_str(&text),
            IsolatedTurnEvent::ThinkingDelta(_) => {}
            IsolatedTurnEvent::Done => return Ok(answer),
            IsolatedTurnEvent::Error(error) => return Err(error),
            IsolatedTurnEvent::Cancelled => break,
        }
    }
    Err(OPERATION_CANCELLED.into())
}

struct IsolatedTurnRequest {
    provider: Arc<dyn Provider>,
    model: Model,
    history: Vec<Message>,
    /// The link of the conversation `history` was copied from.
    previous: Option<ConversationLink>,
    system: String,
    question: String,
    images: Vec<ImageSource>,
    session_id: Option<SessionRef>,
    cancel: CancelToken,
}

async fn run_isolated_turn(request: IsolatedTurnRequest, output: flume::Sender<IsolatedTurnEvent>) {
    let IsolatedTurnRequest {
        provider,
        model,
        mut history,
        previous,
        mut system,
        question,
        images,
        session_id,
        cancel,
    } = request;
    crate::prompt::append_model_identity(&mut system, &model);
    close_dangling_tool_calls(&mut history, UNAVAILABLE_RESULT);
    history.push(Message::user_with_images(
        format!("{BTW_REMINDER}\n\n{question}"),
        images,
    ));
    let messages = makima_providers::adapt_images_for_model(&model, &history).await;
    let tools = Value::Array(Vec::new());
    let (provider_tx, provider_rx) = flume::unbounded();
    let forwarder = smol::spawn({
        let output = output.clone();
        async move {
            while let Ok(event) = provider_rx.recv_async().await {
                let event = match event {
                    ProviderEvent::TextDelta { text } => IsolatedTurnEvent::TextDelta(text),
                    ProviderEvent::ThinkingDelta { text } => IsolatedTurnEvent::ThinkingDelta(text),
                    ProviderEvent::ThinkingBlockEnd
                    | ProviderEvent::ToolUseStart { .. }
                    | ProviderEvent::PromptProgress { .. } => continue,
                };
                if output.send_async(event).await.is_err() {
                    break;
                }
            }
        }
    });
    let invocation = Invocation {
        conversation_id: fresh_id(),
        operation_id: fresh_id(),
        attempt: 0,
        purpose: InvocationPurpose::Isolated,
        agent_id: None,
        previous,
    };
    let delivery = RequestDeliveryTracker::new();
    let result = future::race(
        provider.stream_message(
            &model,
            &messages,
            &system,
            &tools,
            &provider_tx,
            RequestOptions::default(),
            session_id.as_ref(),
            &invocation,
            delivery.attempt(invocation.attempt),
        ),
        async {
            cancel.cancelled().await;
            Err(crate::AgentError::Cancelled)
        },
    )
    .await;
    delivery.end_attempt(result.is_ok());
    drop(provider_tx);
    forwarder.await;
    if delivery.is_delivered() {
        end_conversation_detached(
            &provider,
            invocation.conversation_id,
            ConversationEnd::Closed,
        );
    }
    let terminal = match result {
        Ok(_) => IsolatedTurnEvent::Done,
        Err(crate::AgentError::Cancelled) => IsolatedTurnEvent::Cancelled,
        Err(error) => IsolatedTurnEvent::Error(error.to_string()),
    };
    let _ = output.send_async(terminal).await;
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use makima_providers::provider::{BoxFuture, Provider};
    use makima_providers::{
        ContentBlock, ConversationRelation, ProviderEvent, RequestDelivery, Role, StopReason,
        StreamResponse, TokenUsage,
    };
    use test_case::test_case;

    use super::*;

    const SOURCE_CONVERSATION: &str = "source";
    const SOURCE_SHARED: usize = 4;

    #[derive(Default)]
    struct Captured {
        messages: Vec<Message>,
        tools: Value,
        system: String,
        invocation: Option<Invocation>,
    }

    enum ProviderOutcome {
        Success,
        Failure,
        Pending,
        DeliveredThenPending,
    }

    struct CapturingProvider {
        captured: Arc<Mutex<Captured>>,
        outcome: ProviderOutcome,
        ended: Option<flume::Sender<(Arc<str>, ConversationEnd)>>,
    }

    impl Provider for CapturingProvider {
        fn stream_message<'a>(
            &'a self,
            _model: &'a Model,
            messages: &'a [Message],
            system: &'a str,
            tools: &'a Value,
            events: &'a flume::Sender<ProviderEvent>,
            _opts: RequestOptions,
            _session_id: Option<&'a SessionRef>,
            invocation: &'a Invocation,
            delivery: RequestDelivery,
        ) -> BoxFuture<'a, Result<StreamResponse, crate::AgentError>> {
            let mut captured = self.captured.lock().unwrap();
            captured.invocation = Some(invocation.clone());
            captured.messages = messages.to_vec();
            captured.tools = tools.clone();
            captured.system = system.to_owned();
            drop(captured);
            match self.outcome {
                ProviderOutcome::Success => {
                    let _ = events.send(ProviderEvent::TextDelta {
                        text: "answer".into(),
                    });
                    Box::pin(async {
                        Ok(StreamResponse {
                            message: Message {
                                role: Role::Assistant,
                                content: vec![ContentBlock::Text {
                                    text: "answer".into(),
                                }],
                                ..Default::default()
                            },
                            usage: TokenUsage::default(),
                            stop_reason: Some(StopReason::EndTurn),
                        })
                    })
                }
                ProviderOutcome::Failure => Box::pin(async {
                    Err(crate::AgentError::Config {
                        message: "provider failed".into(),
                    })
                }),
                ProviderOutcome::Pending => Box::pin(std::future::pending()),
                ProviderOutcome::DeliveredThenPending => {
                    assert!(delivery.mark());
                    Box::pin(std::future::pending())
                }
            }
        }

        fn end_conversation(
            &self,
            conversation_id: &str,
            reason: ConversationEnd,
        ) -> BoxFuture<'_, ()> {
            if let Some(ended) = &self.ended {
                let _ = ended.send((conversation_id.into(), reason));
            }
            Box::pin(async {})
        }
    }

    /// The request owns its history (`IsolatedTurnRequest.history: Vec<Message>`),
    /// so an isolated turn cannot reach the caller's primary history at all --
    /// that guarantee is enforced by the signature, not by an assertion here.
    /// What these tests pin is the content of the copy the provider is handed.
    async fn run_with_outcome(
        outcome: ProviderOutcome,
        cancel: CancelToken,
    ) -> Vec<IsolatedTurnEvent> {
        let original = vec![Message::user("primary".into())];
        let provider = Arc::new(CapturingProvider {
            captured: Arc::new(Mutex::new(Captured::default())),
            outcome,
            ended: None,
        });
        let (tx, rx) = flume::unbounded();
        run_isolated_turn(
            IsolatedTurnRequest {
                provider,
                model: Model::from_spec("openai/gpt-4o").unwrap(),
                history: original.clone(),
                previous: None,
                system: "system".into(),
                question: "why?".into(),
                images: Vec::new(),
                session_id: None,
                cancel,
            },
            tx,
        )
        .await;
        rx.iter().collect()
    }

    fn link() -> ConversationLink {
        ConversationLink {
            id: SOURCE_CONVERSATION.into(),
            relation: ConversationRelation::Branches,
            shared: SOURCE_SHARED,
            cancelled: false,
        }
    }

    #[test_case(ProviderOutcome::Success, false, true ; "finished")]
    #[test_case(ProviderOutcome::DeliveredThenPending, true, true ; "cancelled_after_delivery")]
    #[test_case(ProviderOutcome::Pending, true, false ; "cancelled_before_delivery")]
    #[test_case(ProviderOutcome::Failure, false, false ; "failed_before_delivery")]
    fn isolated_turn_ends_only_a_delivered_conversation(
        outcome: ProviderOutcome,
        cancelled: bool,
        ends: bool,
    ) {
        smol::block_on(async {
            let (trigger, cancel) = CancelToken::new();
            if cancelled {
                trigger.cancel();
            }
            let captured = Arc::new(Mutex::new(Captured::default()));
            let (ended_tx, ended_rx) = flume::unbounded();
            let provider = Arc::new(CapturingProvider {
                captured: Arc::clone(&captured),
                outcome,
                ended: Some(ended_tx),
            });
            let (tx, _rx) = flume::unbounded();
            run_isolated_turn(
                IsolatedTurnRequest {
                    provider,
                    model: Model::from_spec("openai/gpt-4o").unwrap(),
                    history: Vec::new(),
                    previous: Some(link()),
                    system: "system".into(),
                    question: "why?".into(),
                    images: Vec::new(),
                    session_id: None,
                    cancel,
                },
                tx,
            )
            .await;

            let invocation = captured.lock().unwrap().invocation.clone().unwrap();
            assert_eq!(invocation.purpose, InvocationPurpose::Isolated);
            assert_eq!(invocation.attempt, 0);
            assert_eq!(invocation.previous, Some(link()));
            // The provider, and with it the sender, outlives the turn only
            // inside a detached end call.
            assert_eq!(
                ended_rx.recv_async().await.ok(),
                ends.then_some((invocation.conversation_id, ConversationEnd::Closed))
            );
        });
    }

    #[test]
    fn isolated_turn_preserves_primary_history_on_failure() {
        smol::block_on(async {
            let events = run_with_outcome(ProviderOutcome::Failure, CancelToken::none()).await;
            assert!(matches!(events.last(), Some(IsolatedTurnEvent::Error(_))));
        });
    }

    #[test]
    fn isolated_turn_preserves_primary_history_on_cancel() {
        smol::block_on(async {
            let (trigger, cancel) = CancelToken::new();
            trigger.cancel();
            let events = run_with_outcome(ProviderOutcome::Pending, cancel).await;
            assert_eq!(events.last(), Some(&IsolatedTurnEvent::Cancelled));
        });
    }

    /// A history that ends on an unanswered tool call is not a valid request.
    /// The isolated turn closes those in its own copy; the primary history keeps
    /// the call open, because the real turn may still be answering it.
    #[test]
    fn isolated_turn_closes_dangling_tool_calls_in_its_copy() {
        smol::block_on(async {
            let dangling = Message {
                role: Role::Assistant,
                content: vec![ContentBlock::ToolUse {
                    id: "call-1".into(),
                    name: "read".into(),
                    input: serde_json::json!({}),
                    thought_signature: None,
                }],
                ..Default::default()
            };
            let original = vec![Message::user("primary".into()), dangling];
            let captured = Arc::new(Mutex::new(Captured::default()));
            let provider = Arc::new(CapturingProvider {
                captured: Arc::clone(&captured),
                outcome: ProviderOutcome::Success,
                ended: None,
            });
            let (tx, _rx) = flume::unbounded();
            run_isolated_turn(
                IsolatedTurnRequest {
                    provider,
                    model: Model::from_spec("openai/gpt-4o").unwrap(),
                    history: original.clone(),
                    previous: None,
                    system: "system".into(),
                    question: "why?".into(),
                    images: Vec::new(),
                    session_id: None,
                    cancel: CancelToken::none(),
                },
                tx,
            )
            .await;

            let captured = captured.lock().unwrap();
            let closed = captured.messages.iter().any(|message| {
                message.content.iter().any(|block| {
                    matches!(
                        block,
                        ContentBlock::ToolResult { tool_use_id, content, .. }
                            if tool_use_id == "call-1" && content == UNAVAILABLE_RESULT
                    )
                })
            });
            assert!(
                closed,
                "the dangling call must be closed before the copy reaches the provider"
            );
        });
    }

    #[test]
    fn isolated_turn_uses_copy_and_empty_tools() {
        const EXPECTED_SYSTEM: &str =
            "system\n\n# Model identity\n- Model: gpt-4o\n- Provider: openai";
        smol::block_on(async {
            let original = vec![Message::user("primary".into())];
            let captured = Arc::new(Mutex::new(Captured::default()));
            let provider = Arc::new(CapturingProvider {
                captured: Arc::clone(&captured),
                outcome: ProviderOutcome::Success,
                ended: None,
            });
            let (tx, rx) = flume::unbounded();
            run_isolated_turn(
                IsolatedTurnRequest {
                    provider,
                    model: Model::from_spec("openai/gpt-4o").unwrap(),
                    history: original.clone(),
                    previous: None,
                    system: "system".into(),
                    question: "why?".into(),
                    images: Vec::new(),
                    session_id: None,
                    cancel: CancelToken::none(),
                },
                tx,
            )
            .await;

            let captured = captured.lock().unwrap();
            assert_eq!(captured.tools, Value::Array(Vec::new()));
            assert_eq!(captured.messages.len(), original.len() + 1);
            assert_eq!(
                captured.system, EXPECTED_SYSTEM,
                "the isolated turn must preserve the caller's system text and identify its model"
            );
            assert!(rx.iter().any(|event| event == IsolatedTurnEvent::Done));
        });
    }
}
