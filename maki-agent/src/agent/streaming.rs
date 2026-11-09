use std::sync::Arc;
use std::time::Duration;

use maki_providers::provider::Provider;
use maki_providers::retry::{RetryPolicy, RetryState};
use maki_providers::{
    ContentBlock, ConversationEnd, ConversationLink, Invocation, InvocationPurpose, Message, Model,
    ProviderEvent, RequestDeliveryTracker, RequestOptions, StreamResponse,
};
use maki_storage::id::{MakiId, SessionRef};
use serde_json::Value;
use tracing::warn;

use crate::cancel::CancelToken;
use crate::{AgentError, AgentEvent, EventSender};

const FUNCTIONS_PREFIX: &str = "functions.";
const END_CONVERSATION_TIMEOUT: Duration = Duration::from_secs(10);

pub(crate) fn fresh_id() -> Arc<str> {
    MakiId::generate().to_string().into()
}

/// The conversation a request belongs to; [`stream_with_retry`] adds the
/// operation and attempt.
pub(crate) struct Conversation {
    pub id: Arc<str>,
    pub purpose: InvocationPurpose,
    pub agent_id: Option<Arc<str>>,
    pub previous: Option<ConversationLink>,
}

impl Conversation {
    fn invocation(self) -> Invocation {
        Invocation {
            conversation_id: self.id,
            operation_id: fresh_id(),
            attempt: 0,
            purpose: self.purpose,
            agent_id: self.agent_id,
            previous: self.previous,
        }
    }
}

/// Best effort: never delays the caller, never retried, bounded by
/// [`END_CONVERSATION_TIMEOUT`].
pub fn end_conversation_detached(
    provider: &Arc<dyn Provider>,
    conversation_id: Arc<str>,
    reason: ConversationEnd,
) {
    let provider = Arc::clone(provider);
    smol::spawn(async move {
        let ended = futures_lite::future::or(
            async {
                provider.end_conversation(&conversation_id, reason).await;
                true
            },
            async {
                smol::Timer::after(END_CONVERSATION_TIMEOUT).await;
                false
            },
        )
        .await;
        if !ended {
            warn!(
                %conversation_id,
                %reason,
                timeout_ms = END_CONVERSATION_TIMEOUT.as_millis() as u64,
                "end_conversation timed out"
            );
        }
    })
    .detach();
}

/// GPT models sometimes emit `functions.<name>`, a Codex training habit.
/// Stripped here at the provider boundary so no raw name enters the agent;
/// the batch plugin mirrors the rule in Lua.
pub(crate) fn canonical_tool_name(name: &str) -> &str {
    name.strip_prefix(FUNCTIONS_PREFIX).unwrap_or(name)
}

fn canonicalize_tool_names(message: &mut Message) {
    for block in &mut message.content {
        if let ContentBlock::ToolUse { name, .. } = block {
            *name = canonical_tool_name(name).to_owned();
        }
    }
}

async fn forward_provider_events(
    prx: flume::Receiver<ProviderEvent>,
    event_tx: &EventSender,
) -> String {
    let mut streamed = String::new();
    while let Ok(pe) = prx.recv_async().await {
        let ae = match pe {
            ProviderEvent::TextDelta { text } => {
                streamed.push_str(&text);
                AgentEvent::TextDelta { text }
            }
            ProviderEvent::ThinkingDelta { text } => AgentEvent::ThinkingDelta { text },
            ProviderEvent::ThinkingBlockEnd => AgentEvent::ThinkingBlockEnd,
            ProviderEvent::ToolUseStart { id, name } => AgentEvent::ToolPending {
                id,
                name: canonical_tool_name(&name).to_owned(),
            },
            ProviderEvent::PromptProgress {
                processed,
                total,
                cache,
            } => AgentEvent::PromptProgress {
                processed,
                total,
                cache,
            },
        };
        if event_tx.send(ae).is_err() && event_tx.output().is_none() {
            break;
        }
    }
    streamed
}

/// Cancelling mid-stream carries the text the user still sees on screen,
/// so the caller can keep it in history. A cancel during the retry backoff
/// carries nothing: the `Retry` event already made the view drop the failed
/// attempt's text (`stream_reset`), and history must agree with the view.
#[derive(Debug)]
pub(crate) enum StreamError {
    Cancelled { streamed: String },
    Other(AgentError),
}

impl From<AgentError> for StreamError {
    fn from(e: AgentError) -> Self {
        Self::Other(e)
    }
}

impl From<StreamError> for AgentError {
    fn from(e: StreamError) -> Self {
        match e {
            StreamError::Cancelled { .. } => Self::Cancelled,
            StreamError::Other(e) => e,
        }
    }
}

/// {delivery} learns, while the stream still runs, the first time any
/// attempt reaches the provider's server.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn stream_with_retry(
    provider: &dyn Provider,
    conversation: Conversation,
    model: &Model,
    messages: &[Message],
    system: &str,
    tools: &Value,
    event_tx: &EventSender,
    cancel: &CancelToken,
    opts: RequestOptions,
    session_id: Option<&SessionRef>,
    retry_policy: RetryPolicy,
    delivery: &RequestDeliveryTracker,
) -> Result<StreamResponse, StreamError> {
    let opts = opts.clamped(model);
    let messages = maki_providers::adapt_images_for_model(model, messages).await;
    let messages = &*messages;
    let mut retry = RetryState::new(retry_policy, provider.key_count());
    let mut invocation = conversation.invocation();
    if let Some(output) = event_tx.output() {
        output.begin_text_attempt();
    }
    loop {
        let (ptx, prx) = flume::unbounded();
        let forwarder = smol::spawn({
            let event_tx = event_tx.clone();
            async move { forward_provider_events(prx, &event_tx).await }
        });
        let result = futures_lite::future::race(
            provider.stream_message(
                model,
                messages,
                system,
                tools,
                &ptx,
                opts,
                session_id,
                &invocation,
                delivery.attempt(invocation.attempt),
            ),
            async {
                cancel.cancelled().await;
                Err(AgentError::Cancelled)
            },
        )
        .await;
        delivery.end_attempt(result.is_ok());
        drop(ptx);
        let streamed = forwarder.await;
        match result {
            Ok(mut r) => {
                canonicalize_tool_names(&mut r.message);
                if let Some(output) = event_tx.output() {
                    let text = r
                        .message
                        .content
                        .iter()
                        .filter_map(|block| match block {
                            ContentBlock::Text { text } => Some(text.as_str()),
                            _ => None,
                        })
                        .collect::<String>();
                    output.reconcile_text_attempt(&text);
                }
                return Ok(r);
            }
            Err(AgentError::Cancelled) => return Err(StreamError::Cancelled { streamed }),
            Err(e) => {
                invocation.attempt += 1;
                let attempt = invocation.attempt;
                let rotated =
                    e.should_rotate_key() && retry.book_rotation() && provider.rotate_key().await;
                let (message, delay) = if rotated {
                    (e.retry_message(), Duration::ZERO)
                } else if let Some(kind) = e.retry_kind() {
                    let Some(delay) = retry.next_delay(kind, e.retry_after()) else {
                        return Err(e.into());
                    };
                    (e.retry_message(), delay)
                } else {
                    return Err(e.into());
                };
                let delay_ms = delay.as_millis() as u64;
                warn!(attempt, delay_ms, rotated, error = %e, "retryable, will retry");
                event_tx.send(AgentEvent::Retry {
                    attempt,
                    message,
                    delay_ms,
                })?;
                futures_lite::future::race(
                    async {
                        smol::Timer::after(delay).await;
                    },
                    cancel.cancelled(),
                )
                .await;
                if cancel.is_cancelled() {
                    return Err(StreamError::Cancelled {
                        streamed: String::new(),
                    });
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use maki_providers::{RequestDelivery, RetryKind, Role};
    use serde_json::json;
    use test_case::test_case;

    use super::*;

    #[test]
    fn tool_use_names_canonicalized() {
        let mut message = Message {
            role: Role::Assistant,
            content: vec![
                ContentBlock::Text { text: "hi".into() },
                ContentBlock::tool_use("t1", "functions.bash", json!({})),
                ContentBlock::tool_use("t2", "read", json!({})),
                ContentBlock::tool_use("t3", "my_functions.x", json!({})),
            ],
            ..Default::default()
        };
        canonicalize_tool_names(&mut message);
        let names: Vec<&str> = message.tool_uses().map(|(_, name, _)| name).collect();
        assert_eq!(names, ["bash", "read", "my_functions.x"]);
    }

    #[test]
    fn forward_provider_events_maps_thinking_block_end() {
        smol::block_on(async {
            let (prx_tx, prx_rx) = flume::unbounded();
            let (event_tx, event_rx) = flume::unbounded();
            prx_tx
                .send(ProviderEvent::ThinkingDelta { text: "a".into() })
                .unwrap();
            prx_tx.send(ProviderEvent::ThinkingBlockEnd).unwrap();
            prx_tx
                .send(ProviderEvent::ThinkingDelta { text: "b".into() })
                .unwrap();
            drop(prx_tx);

            let sender = EventSender::new(event_tx);
            forward_provider_events(prx_rx, &sender).await;

            let events: Vec<AgentEvent> = event_rx.drain().map(|e| e.event).collect();
            assert_eq!(events.len(), 3);
            assert!(matches!(&events[0], AgentEvent::ThinkingDelta { text } if text == "a"));
            assert!(matches!(&events[1], AgentEvent::ThinkingBlockEnd));
            assert!(matches!(&events[2], AgentEvent::ThinkingDelta { text } if text == "b"));
        });
    }

    #[test_case(false; "disconnected_sink")]
    #[test_case(true; "full_sink")]
    fn output_capture_drains_all_deltas_when_delivery_fails(full: bool) {
        smol::block_on(async {
            const DELTAS: [&str; 3] = ["first", "second", "third"];
            const EXPECTED: &str = "firstsecondthird";
            let ticket = crate::TurnTicket::new(crate::TurnId::generate(), std::sync::Arc::new(()));
            let (event_tx, event_rx) = flume::bounded(1);
            if full {
                event_tx
                    .send(crate::Envelope {
                        event: AgentEvent::Nudge,
                        subagent: None,
                        identity: None,
                    })
                    .unwrap();
            } else {
                drop(event_rx);
            }
            let sender = EventSender::new(event_tx).with_output(ticket.output());
            let (provider_tx, provider_rx) = flume::unbounded();
            for text in DELTAS {
                provider_tx
                    .send(ProviderEvent::TextDelta { text: text.into() })
                    .unwrap();
            }
            drop(provider_tx);
            let streamed = forward_provider_events(provider_rx, &sender).await;
            ticket.resolve(crate::TurnOutcome::cancelled(
                crate::AgentId::generate(),
                ticket.turn_id(),
                maki_providers::TokenUsage::default(),
                0,
                crate::TurnCancellationReason::User,
            ));
            assert_eq!(streamed, EXPECTED);
            assert_eq!(ticket.peek_result().unwrap().text, EXPECTED);
        });
    }

    const POOL_KEYS: [&str; 3] = ["sk-first", "sk-second", "sk-third"];
    const FULL_POOL: usize = POOL_KEYS.len();
    const SINGLE_KEY: usize = 1;
    const RATE_LIMITED: u16 = 429;
    const UNAUTHORIZED: u16 = 401;
    const FORBIDDEN: u16 = 403;
    const REJECTED_BODY: &str = "this key is done";

    const CONVERSATION: &str = "conversation";

    fn rejected(status: u16, retry: Option<RetryKind>) -> AgentError {
        AgentError::Api {
            status,
            message: REJECTED_BODY.into(),
            retry_after: None,
            overflow: false,
            retry,
        }
    }

    fn test_conversation() -> Conversation {
        Conversation {
            id: CONVERSATION.into(),
            purpose: InvocationPurpose::Agent,
            agent_id: None,
            previous: None,
        }
    }

    fn instant_retry() -> RetryPolicy {
        RetryPolicy {
            base_delay: Duration::ZERO,
            max_delay: Duration::ZERO,
            max_retries: 1,
            max_timeout_retries: 0,
        }
    }

    fn no_budget() -> RetryPolicy {
        RetryPolicy {
            max_retries: 0,
            max_timeout_retries: 0,
            ..RetryPolicy::default()
        }
    }

    #[derive(Default)]
    struct CapturedProvider {
        attempt: std::sync::atomic::AtomicUsize,
        invocations: std::sync::Mutex<Vec<Invocation>>,
    }

    impl Provider for CapturedProvider {
        fn stream_message<'a>(
            &'a self,
            _: &'a Model,
            _: &'a [Message],
            _: &'a str,
            _: &'a Value,
            events: &'a flume::Sender<ProviderEvent>,
            _: RequestOptions,
            _: Option<&'a SessionRef>,
            invocation: &'a Invocation,
            _delivery: RequestDelivery,
        ) -> maki_providers::provider::BoxFuture<'a, Result<StreamResponse, AgentError>> {
            self.invocations.lock().unwrap().push(invocation.clone());
            Box::pin(async move {
                let attempt = self
                    .attempt
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                events
                    .send(ProviderEvent::TextDelta {
                        text: REJECTED_BODY.into(),
                    })
                    .unwrap();
                if attempt == 0 {
                    return Err(rejected(RATE_LIMITED, Some(RetryKind::RateLimit)));
                }
                Ok(StreamResponse {
                    message: Message {
                        role: Role::Assistant,
                        content: vec![
                            ContentBlock::Text {
                                text: "accepted".into(),
                            },
                            ContentBlock::Text {
                                text: " response".into(),
                            },
                        ],
                        ..Default::default()
                    },
                    usage: maki_providers::TokenUsage::default(),
                    stop_reason: Some(maki_providers::StopReason::EndTurn),
                })
            })
        }
    }

    #[test]
    fn retry_reconciliation_returns_last_message_with_all_final_text_blocks() {
        smol::block_on(async {
            const PRIOR: &str = "prior message";
            const EXPECTED: &str = "accepted response";
            let provider = CapturedProvider::default();
            let ticket = crate::TurnTicket::new(crate::TurnId::generate(), std::sync::Arc::new(()));
            let (tx, _rx) = flume::unbounded();
            let sender = EventSender::new(tx).with_output(ticket.output());
            sender
                .send(AgentEvent::TextDelta { text: PRIOR.into() })
                .unwrap();
            let policy = instant_retry();
            let model = Model::from_spec("ollama/qwen3").unwrap();
            for _ in 0..2 {
                stream_with_retry(
                    &provider,
                    test_conversation(),
                    &model,
                    &[],
                    "",
                    &json!([]),
                    &sender,
                    &CancelToken::none(),
                    RequestOptions::default(),
                    None,
                    policy,
                    &RequestDeliveryTracker::new(),
                )
                .await
                .unwrap();
            }
            ticket.resolve(crate::TurnOutcome::completed(
                crate::AgentId::generate(),
                ticket.turn_id(),
                maki_providers::TokenUsage::default(),
                2,
                crate::DoneReason::EndTurn,
            ));
            assert_eq!(ticket.peek_result().unwrap().text, EXPECTED);
        });
    }

    #[test]
    fn operation_id_is_stable_across_retries_and_fresh_per_call() {
        smol::block_on(async {
            let provider = CapturedProvider::default();
            let (tx, _rx) = flume::unbounded();
            let sender = EventSender::new(tx);
            let model = Model::from_spec("ollama/qwen3").unwrap();
            for _ in 0..2 {
                stream_with_retry(
                    &provider,
                    test_conversation(),
                    &model,
                    &[],
                    "",
                    &json!([]),
                    &sender,
                    &CancelToken::none(),
                    RequestOptions::default(),
                    None,
                    instant_retry(),
                    &RequestDeliveryTracker::new(),
                )
                .await
                .unwrap();
            }
            let invocations = provider.invocations.lock().unwrap();
            let attempts: Vec<_> = invocations.iter().map(|i| i.attempt).collect();
            assert_eq!(attempts, [0, 1, 0]);
            assert_eq!(invocations[0].operation_id, invocations[1].operation_id);
            assert_ne!(invocations[1].operation_id, invocations[2].operation_id);
            assert!(
                invocations
                    .iter()
                    .all(|i| &*i.conversation_id == CONVERSATION)
            );
        });
    }

    struct PooledServer {
        keys: &'static [&'static str],
        current: std::sync::atomic::AtomicUsize,
        status: u16,
        relents_for: Option<&'static str>,
        seen: std::sync::Mutex<Vec<String>>,
    }

    impl PooledServer {
        fn new(keys: usize, status: u16, relents_for: Option<&'static str>) -> Self {
            Self {
                keys: &POOL_KEYS[..keys],
                current: std::sync::atomic::AtomicUsize::new(0),
                status,
                relents_for,
                seen: std::sync::Mutex::default(),
            }
        }

        fn keys_seen(&self) -> Vec<String> {
            self.seen.lock().unwrap().clone()
        }
    }

    impl Provider for PooledServer {
        fn stream_message<'a>(
            &'a self,
            _: &'a Model,
            _: &'a [Message],
            _: &'a str,
            _: &'a Value,
            _: &'a flume::Sender<ProviderEvent>,
            _: RequestOptions,
            _: Option<&'a SessionRef>,
            _invocation: &'a Invocation,
            _delivery: RequestDelivery,
        ) -> maki_providers::provider::BoxFuture<'a, Result<StreamResponse, AgentError>> {
            Box::pin(async move {
                let index = self.current.load(std::sync::atomic::Ordering::Relaxed);
                let key = self.keys[index % self.keys.len()].to_owned();
                let relents = self.relents_for == Some(key.as_str());
                self.seen.lock().unwrap().push(key);
                if !relents {
                    let retry = (self.status == RATE_LIMITED).then_some(RetryKind::RateLimit);
                    return Err(rejected(self.status, retry));
                }
                Ok(StreamResponse {
                    message: Message::user("ok".into()),
                    usage: maki_providers::TokenUsage::default(),
                    stop_reason: Some(maki_providers::StopReason::EndTurn),
                })
            })
        }

        fn key_count(&self) -> usize {
            self.keys.len()
        }

        fn rotate_key(&self) -> maki_providers::provider::BoxFuture<'_, bool> {
            let rotated = self.keys.len() > 1;
            if rotated {
                self.current
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
            Box::pin(async move { rotated })
        }
    }

    async fn send_pooled(server: &PooledServer) -> Result<StreamResponse, StreamError> {
        let (tx, _rx) = flume::unbounded();
        let model = Model::from_spec("ollama/qwen3").unwrap();
        stream_with_retry(
            server,
            test_conversation(),
            &model,
            &[Message::user("hi".into())],
            "",
            &json!([]),
            &EventSender::new(tx),
            &CancelToken::none(),
            RequestOptions::default(),
            None,
            no_budget(),
            &RequestDeliveryTracker::new(),
        )
        .await
    }

    #[test]
    fn a_fresh_key_is_tried_without_spending_the_retry_budget() {
        smol::block_on(async {
            let server = PooledServer::new(FULL_POOL, RATE_LIMITED, Some(POOL_KEYS[FULL_POOL - 1]));

            send_pooled(&server)
                .await
                .expect("the last key in the pool answers");

            assert_eq!(
                server.keys_seen(),
                POOL_KEYS,
                "the walk tries each key once, in the pool's order"
            );
        });
    }

    #[test_case(RATE_LIMITED, FULL_POOL  ; "a_rate_limit_walks_the_whole_pool")]
    #[test_case(UNAUTHORIZED, FULL_POOL  ; "unauthorized_walks_the_whole_pool")]
    #[test_case(FORBIDDEN, FULL_POOL     ; "forbidden_walks_the_whole_pool")]
    #[test_case(UNAUTHORIZED, SINGLE_KEY ; "a_lone_key_is_tried_once")]
    fn a_spent_pool_is_walked_once_and_then_gives_up(status: u16, keys: usize) {
        smol::block_on(async {
            let server = PooledServer::new(keys, status, None);

            let result = send_pooled(&server).await;

            assert!(result.is_err(), "no key in the pool is accepted");
            assert_eq!(server.keys_seen().len(), keys, "and no key is tried twice");
        });
    }

    enum AttemptEnd {
        Retryable,
        Terminal,
        Success,
    }

    /// Plays one `(marks, end)` per attempt and keeps every handle it got.
    struct DeliveryScript {
        attempts: std::sync::Mutex<std::vec::IntoIter<(bool, AttemptEnd)>>,
        handles: std::sync::Mutex<Vec<RequestDelivery>>,
    }

    impl Provider for DeliveryScript {
        fn stream_message<'a>(
            &'a self,
            _: &'a Model,
            _: &'a [Message],
            _: &'a str,
            _: &'a Value,
            _: &'a flume::Sender<ProviderEvent>,
            _: RequestOptions,
            _: Option<&'a SessionRef>,
            _: &'a Invocation,
            delivery: RequestDelivery,
        ) -> maki_providers::provider::BoxFuture<'a, Result<StreamResponse, AgentError>> {
            let (marks, end) = self.attempts.lock().unwrap().next().unwrap();
            if marks {
                assert!(delivery.mark());
            }
            self.handles.lock().unwrap().push(delivery);
            Box::pin(async move {
                match end {
                    AttemptEnd::Retryable => {
                        Err(rejected(RATE_LIMITED, Some(RetryKind::RateLimit)))
                    }
                    AttemptEnd::Terminal => Err(rejected(UNAUTHORIZED, None)),
                    AttemptEnd::Success => Ok(StreamResponse {
                        message: Message::user("ok".into()),
                        usage: maki_providers::TokenUsage::default(),
                        stop_reason: Some(maki_providers::StopReason::EndTurn),
                    }),
                }
            })
        }
    }

    #[test_case(vec![(false, AttemptEnd::Retryable), (false, AttemptEnd::Success)], true ; "success_delivers_without_a_mark")]
    #[test_case(vec![(true, AttemptEnd::Retryable), (false, AttemptEnd::Terminal)], true ; "delivered_attempt_stays_delivered_after_a_failed_retry")]
    #[test_case(vec![(false, AttemptEnd::Retryable), (true, AttemptEnd::Terminal)], true ; "a_later_attempt_delivers")]
    #[test_case(vec![(false, AttemptEnd::Retryable), (false, AttemptEnd::Terminal)], false ; "failures_without_a_mark_stay_undelivered")]
    fn delivery_spans_the_logical_request(attempts: Vec<(bool, AttemptEnd)>, delivered: bool) {
        smol::block_on(async {
            let provider = DeliveryScript {
                attempts: std::sync::Mutex::new(attempts.into_iter()),
                handles: std::sync::Mutex::default(),
            };
            let tracker = RequestDeliveryTracker::new();
            let (tx, _rx) = flume::unbounded();
            let _ = stream_with_retry(
                &provider,
                test_conversation(),
                &Model::from_spec("ollama/qwen3").unwrap(),
                &[Message::user("hi".into())],
                "",
                &json!([]),
                &EventSender::new(tx),
                &CancelToken::none(),
                RequestOptions::default(),
                None,
                instant_retry(),
                &tracker,
            )
            .await;
            assert_eq!(tracker.is_delivered(), delivered);
            for handle in provider.handles.lock().unwrap().iter() {
                assert!(!handle.mark(), "a retired attempt cannot mark");
            }
            assert_eq!(tracker.is_delivered(), delivered);
        });
    }
}
