use std::time::Duration;

use maki_providers::provider::Provider;
use maki_providers::retry::{RetryPolicy, RetryState};
use maki_providers::{ContentBlock, Message, Model, ProviderEvent, RequestOptions, StreamResponse};
use maki_storage::id::SessionRef;
use serde_json::Value;
use tracing::warn;

use crate::cancel::CancelToken;
use crate::{AgentError, AgentEvent, EventSender};

const FUNCTIONS_PREFIX: &str = "functions.";

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
        if event_tx.send(ae).is_err() {
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

#[allow(clippy::too_many_arguments)]
pub(crate) async fn stream_with_retry(
    provider: &dyn Provider,
    model: &Model,
    messages: &[Message],
    system: &str,
    tools: &Value,
    event_tx: &EventSender,
    cancel: &CancelToken,
    opts: RequestOptions,
    session_id: Option<&SessionRef>,
    retry_policy: RetryPolicy,
) -> Result<StreamResponse, StreamError> {
    let opts = opts.clamped(model);
    let messages = maki_providers::adapt_images_for_model(model, messages).await;
    let messages = &*messages;
    let mut retry = RetryState::new(
        retry_policy,
        provider.keys().map_or(1, |keys| keys.key_count()),
    );
    let mut attempt = 0;
    loop {
        let (ptx, prx) = flume::unbounded();
        let forwarder = smol::spawn({
            let event_tx = event_tx.clone();
            async move { forward_provider_events(prx, &event_tx).await }
        });
        let result = futures_lite::future::race(
            provider.stream_message(model, messages, system, tools, &ptx, opts, session_id),
            async {
                cancel.cancelled().await;
                Err(AgentError::Cancelled)
            },
        )
        .await;
        drop(ptx);
        let streamed = forwarder.await;
        match result {
            Ok(mut r) => {
                canonicalize_tool_names(&mut r.message);
                return Ok(r);
            }
            Err(AgentError::Cancelled) => return Err(StreamError::Cancelled { streamed }),
            Err(e) => {
                attempt += 1;
                let rotated = e.should_rotate_key()
                    && retry.book_rotation()
                    && provider.keys().is_some_and(|keys| keys.rotate());
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
    use maki_providers::Role;
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

            let sender = EventSender::new(event_tx, 0);
            forward_provider_events(prx_rx, &sender).await;

            let events: Vec<AgentEvent> = event_rx.drain().map(|e| e.event).collect();
            assert_eq!(events.len(), 3);
            assert!(matches!(&events[0], AgentEvent::ThinkingDelta { text } if text == "a"));
            assert!(matches!(&events[1], AgentEvent::ThinkingBlockEnd));
            assert!(matches!(&events[2], AgentEvent::ThinkingDelta { text } if text == "b"));
        });
    }

    const POOL_KEYS: [&str; 3] = ["sk-first", "sk-second", "sk-third"];
    const FULL_POOL: usize = POOL_KEYS.len();
    const SINGLE_KEY: usize = 1;
    const RATE_LIMITED: u16 = 429;
    const UNAUTHORIZED: u16 = 401;
    const FORBIDDEN: u16 = 403;
    const REJECTED_BODY: &str = "this key is done";

    fn no_budget() -> RetryPolicy {
        RetryPolicy {
            max_retries: 0,
            max_timeout_retries: 0,
            ..RetryPolicy::default()
        }
    }

    struct PooledServer {
        pool: maki_providers::KeyPool,
        auth: std::sync::Mutex<maki_providers::ResolvedAuth>,
        status: u16,
        relents_for: Option<&'static str>,
        seen: std::sync::Mutex<Vec<String>>,
    }

    impl PooledServer {
        fn new(keys: usize, status: u16, relents_for: Option<&'static str>) -> Self {
            let pool = maki_providers::KeyPool::from_keys(
                POOL_KEYS[..keys].iter().map(|k| (*k).into()).collect(),
            );
            let auth = maki_providers::ResolvedAuth::bearer(pool.current());
            Self {
                pool,
                auth: std::sync::Mutex::new(auth),
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
        ) -> maki_providers::provider::BoxFuture<'a, Result<StreamResponse, AgentError>> {
            Box::pin(async move {
                let key = self.pool.current().to_owned();
                let relents = self.relents_for == Some(key.as_str());
                self.seen.lock().unwrap().push(key);
                if !relents {
                    return Err(AgentError::api(self.status, REJECTED_BODY));
                }
                Ok(StreamResponse {
                    message: Message::user("ok".into()),
                    usage: maki_providers::TokenUsage::default(),
                    stop_reason: Some(maki_providers::StopReason::EndTurn),
                })
            })
        }

        fn list_models(
            &self,
        ) -> maki_providers::provider::BoxFuture<
            '_,
            Result<Vec<maki_providers::ModelInfo>, AgentError>,
        > {
            Box::pin(async { unimplemented!() })
        }

        fn keys(&self) -> Option<maki_providers::KeyRotation<'_>> {
            Some(maki_providers::KeyRotation::new(
                &self.pool,
                &self.auth,
                maki_providers::KeyHeader::Bearer,
            ))
        }
    }

    async fn send_pooled(server: &PooledServer) -> Result<StreamResponse, StreamError> {
        let (tx, _rx) = flume::unbounded();
        let model = Model::from_spec("ollama/qwen3").unwrap();
        stream_with_retry(
            server,
            &model,
            &[Message::user("hi".into())],
            "",
            &json!([]),
            &EventSender::new(tx, 0),
            &CancelToken::none(),
            RequestOptions::default(),
            None,
            no_budget(),
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
}
