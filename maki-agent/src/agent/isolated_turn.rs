use std::sync::Arc;

use maki_providers::provider::Provider;
use maki_providers::{ImageSource, Message, Model, ProviderEvent, RequestOptions};
use maki_storage::id::SessionRef;
use serde_json::Value;

use crate::cancel::CancelToken;

use super::{UNAVAILABLE_RESULT, close_dangling_tool_calls};

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

pub struct IsolatedTurnRequest {
    pub provider: Arc<dyn Provider>,
    pub model: Model,
    pub history: Vec<Message>,
    pub system: String,
    pub question: String,
    pub images: Vec<ImageSource>,
    pub session_id: Option<SessionRef>,
    pub cancel: CancelToken,
}

pub async fn run_isolated_turn(
    request: IsolatedTurnRequest,
    output: flume::Sender<IsolatedTurnEvent>,
) {
    let IsolatedTurnRequest {
        provider,
        model,
        mut history,
        system,
        question,
        images,
        session_id,
        cancel,
    } = request;
    close_dangling_tool_calls(&mut history, UNAVAILABLE_RESULT);
    history.push(Message::user_with_images(
        format!("{BTW_REMINDER}\n\n{question}"),
        images,
    ));
    let messages = maki_providers::adapt_images_for_model(&model, &history);
    let tools = Value::Array(Vec::new());
    let (provider_tx, provider_rx) = flume::unbounded();
    let forwarder = smol::spawn({
        let output = output.clone();
        async move {
            while let Ok(event) = provider_rx.recv_async().await {
                let event = match event {
                    ProviderEvent::TextDelta { text } => IsolatedTurnEvent::TextDelta(text),
                    ProviderEvent::ThinkingDelta { text } => IsolatedTurnEvent::ThinkingDelta(text),
                    ProviderEvent::ToolUseStart { .. } | ProviderEvent::PromptProgress { .. } => {
                        continue;
                    }
                };
                if output.send_async(event).await.is_err() {
                    break;
                }
            }
        }
    });
    let result = futures_lite::future::race(
        provider.stream_message(
            &model,
            &messages,
            &system,
            &tools,
            &provider_tx,
            RequestOptions::default(),
            session_id.as_ref(),
        ),
        async {
            cancel.cancelled().await;
            Err(crate::AgentError::Cancelled)
        },
    )
    .await;
    drop(provider_tx);
    forwarder.await;
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

    use maki_providers::provider::{BoxFuture, Provider};
    use maki_providers::{
        ContentBlock, ProviderEvent, Role, StopReason, StreamResponse, TokenUsage,
    };

    use super::*;

    #[derive(Default)]
    struct Captured {
        messages: Vec<Message>,
        tools: Value,
    }

    enum ProviderOutcome {
        Success,
        Failure,
        Pending,
    }

    struct CapturingProvider {
        captured: Arc<Mutex<Captured>>,
        outcome: ProviderOutcome,
    }

    impl Provider for CapturingProvider {
        fn stream_message<'a>(
            &'a self,
            _model: &'a Model,
            messages: &'a [Message],
            _system: &'a str,
            tools: &'a Value,
            events: &'a flume::Sender<ProviderEvent>,
            _opts: RequestOptions,
            _session_id: Option<&'a SessionRef>,
        ) -> BoxFuture<'a, Result<StreamResponse, crate::AgentError>> {
            let mut captured = self.captured.lock().unwrap();
            captured.messages = messages.to_vec();
            captured.tools = tools.clone();
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
            }
        }

        fn list_models(
            &self,
        ) -> BoxFuture<'_, Result<Vec<maki_providers::ModelInfo>, crate::AgentError>> {
            Box::pin(async { unimplemented!() })
        }
    }

    async fn run_with_outcome(
        outcome: ProviderOutcome,
        cancel: CancelToken,
    ) -> Vec<IsolatedTurnEvent> {
        let original = vec![Message::user("primary".into())];
        let before = serde_json::to_value(&original).unwrap();
        let provider = Arc::new(CapturingProvider {
            captured: Arc::new(Mutex::new(Captured::default())),
            outcome,
        });
        let (tx, rx) = flume::unbounded();
        run_isolated_turn(
            IsolatedTurnRequest {
                provider,
                model: Model::from_spec("openai/gpt-4o").unwrap(),
                history: original.clone(),
                system: "system".into(),
                question: "why?".into(),
                images: Vec::new(),
                session_id: None,
                cancel,
            },
            tx,
        )
        .await;
        assert_eq!(serde_json::to_value(&original).unwrap(), before);
        rx.iter().collect()
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

    #[test]
    fn isolated_turn_uses_copy_and_empty_tools() {
        smol::block_on(async {
            let original = vec![Message::user("primary".into())];
            let before = serde_json::to_value(&original).unwrap();
            let captured = Arc::new(Mutex::new(Captured::default()));
            let provider = Arc::new(CapturingProvider {
                captured: Arc::clone(&captured),
                outcome: ProviderOutcome::Success,
            });
            let (tx, rx) = flume::unbounded();
            run_isolated_turn(
                IsolatedTurnRequest {
                    provider,
                    model: Model::from_spec("openai/gpt-4o").unwrap(),
                    history: original.clone(),
                    system: "system".into(),
                    question: "why?".into(),
                    images: Vec::new(),
                    session_id: None,
                    cancel: CancelToken::none(),
                },
                tx,
            )
            .await;

            assert_eq!(serde_json::to_value(&original).unwrap(), before);
            let captured = captured.lock().unwrap();
            assert_eq!(captured.tools, Value::Array(Vec::new()));
            assert_eq!(captured.messages.len(), original.len() + 1);
            assert!(rx.iter().any(|event| event == IsolatedTurnEvent::Done));
        });
    }
}
