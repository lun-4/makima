use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use maki_agent::cancel::CancelToken;
use maki_providers::{
    AgentError, ContentBlock, Message, Overflow, ProviderEvent, Role, StopReason, StreamResponse,
    TokenUsage,
};
use mlua::{Function, Lua, MultiValue, Table, Value};
use serde::Deserialize;
use serde_json::Value as JsonValue;

use crate::api::util::convert::{json_to_lua_with_nulls, lua_to_json};
use crate::plugin_permissions::PluginPermissions;
use crate::runtime::{TaskCell, TaskScope};

const CLOSED: &str = "provider stream is closed";

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum Event {
    TextDelta {
        text: String,
    },
    ThinkingDelta {
        text: String,
    },
    ThinkingBlockEnd,
    ToolUseStart {
        id: String,
        name: String,
    },
    PromptProgress {
        processed: u32,
        total: u32,
        cache: u32,
    },
}

impl From<Event> for ProviderEvent {
    fn from(event: Event) -> Self {
        match event {
            Event::TextDelta { text } => Self::TextDelta { text },
            Event::ThinkingDelta { text } => Self::ThinkingDelta { text },
            Event::ThinkingBlockEnd => Self::ThinkingBlockEnd,
            Event::ToolUseStart { id, name } => Self::ToolUseStart { id, name },
            Event::PromptProgress {
                processed,
                total,
                cache,
            } => Self::PromptProgress {
                processed,
                total,
                cache,
            },
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Usage {
    input: u32,
    output: u32,
    cache_creation: u32,
    cache_read: u32,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Response {
    message: Message,
    usage: Usage,
    stop_reason: Option<String>,
}

fn response(value: JsonValue) -> Result<StreamResponse, String> {
    let response: Response =
        serde_json::from_value(value).map_err(|e| format!("invalid provider response: {e}"))?;
    if !matches!(response.message.role, Role::Assistant) {
        return Err("invalid provider response: message role must be assistant".into());
    }
    if response.message.content.iter().any(|block| {
        matches!(
            block,
            ContentBlock::ToolResult { .. } | ContentBlock::Image { .. }
        )
    }) {
        return Err("invalid provider response: invalid assistant content block".into());
    }
    let stop_reason = match response.stop_reason.as_deref() {
        None => None,
        Some("end_turn") => Some(StopReason::EndTurn),
        Some("tool_use") => Some(StopReason::ToolUse),
        Some("max_tokens") => Some(StopReason::MaxTokens),
        Some(_) => return Err("invalid provider response: unknown stop reason".into()),
    };
    Ok(StreamResponse {
        message: response.message,
        usage: TokenUsage {
            input: response.usage.input,
            output: response.usage.output,
            cache_creation: response.usage.cache_creation,
            cache_read: response.usage.cache_read,
        },
        stop_reason,
    })
}

struct Gate {
    sender: Option<flume::Sender<ProviderEvent>>,
}

struct CloseGate(Arc<Mutex<Gate>>);

impl Drop for CloseGate {
    fn drop(&mut self) {
        self.0.lock().unwrap_or_else(|e| e.into_inner()).sender = None;
    }
}

fn emit(lua: &Lua, gate: Arc<Mutex<Gate>>, cancel: CancelToken) -> mlua::Result<Function> {
    lua.create_function(move |lua, value: Value| {
        let gate = gate.lock().unwrap_or_else(|e| e.into_inner());
        let Some(sender) = gate.sender.as_ref().filter(|_| !cancel.is_cancelled()) else {
            return Ok((
                Value::Nil,
                Value::Table(error_table(lua, "closed", CLOSED)?),
            ));
        };
        let event = lua_to_json(lua, &value)
            .map_err(|e| format!("invalid provider event: {e}"))
            .and_then(|value| {
                serde_json::from_value::<Event>(value)
                    .map_err(|e| format!("invalid provider event: {e}"))
            });
        match event {
            Ok(event) => match sender.try_send(event.into()) {
                Ok(()) => Ok((Value::Boolean(true), Value::Nil)),
                Err(flume::TrySendError::Full(_)) => Ok((
                    Value::Nil,
                    Value::Table(error_table(
                        lua,
                        "backpressure",
                        "provider event channel is full",
                    )?),
                )),
                Err(flume::TrySendError::Disconnected(_)) => Ok((
                    Value::Nil,
                    Value::Table(error_table(lua, "closed", CLOSED)?),
                )),
            },
            Err(error) => Ok((
                Value::Nil,
                Value::Table(error_table(lua, "config", &error)?),
            )),
        }
    })
}

fn error_table(lua: &Lua, kind: &str, message: &str) -> mlua::Result<Table> {
    let table = lua.create_table()?;
    table.set("kind", kind)?;
    table.set("message", message)?;
    Ok(table)
}

pub(crate) fn provider_error(error: Table) -> AgentError {
    let parse = || -> Result<AgentError, String> {
        let kind: String = error
            .raw_get("kind")
            .map_err(|_| "invalid provider error: missing kind")?;
        let message: String = error
            .raw_get("message")
            .map_err(|_| "invalid provider error: missing message")?;
        let retry_after = || -> Result<Option<Duration>, String> {
            error
                .raw_get::<Option<u64>>("retry_after_ms")
                .map(|value| value.map(Duration::from_millis))
                .map_err(|_| "invalid provider error: invalid retry_after_ms".into())
        };
        match kind.as_str() {
            "api" => {
                let status = error
                    .raw_get::<Option<u16>>("status")
                    .map_err(|_| "invalid provider error: invalid status")?
                    .unwrap_or(500);
                let source = AgentError::Api {
                    status,
                    message,
                    retry_after: retry_after()?.filter(|delay| !delay.is_zero()),
                };
                let overflow = error
                    .raw_get::<Option<Table>>("overflow")
                    .map_err(|_| "invalid provider error: invalid overflow")?;
                match overflow {
                    None => Ok(source),
                    Some(overflow) => {
                        let kind = overflow
                            .raw_get::<String>("kind")
                            .map_err(|_| "invalid provider error: invalid overflow kind")?;
                        let overflow = match kind.as_str() {
                            "prompt" => Overflow::Prompt,
                            "budget" => Overflow::Budget {
                                prompt: overflow.raw_get::<Option<u32>>("prompt").map_err(
                                    |_| "invalid provider error: invalid overflow prompt",
                                )?,
                                limit: overflow.raw_get::<Option<u32>>("limit").map_err(
                                    |_| "invalid provider error: invalid overflow limit",
                                )?,
                            },
                            _ => return Err("invalid provider error: invalid overflow kind".into()),
                        };
                        Ok(AgentError::StructuredOverflow {
                            source: Box::new(source),
                            overflow,
                        })
                    }
                }
            }
            "network" => Ok(AgentError::Network {
                message,
                retry_after: retry_after()?.filter(|delay| !delay.is_zero()),
            }),
            "config" => Ok(AgentError::Config { message }),
            "cancelled" => Ok(AgentError::Cancelled),
            "closed" | "backpressure" => Ok(AgentError::Channel),
            _ => Err("invalid provider error: unknown kind".into()),
        }
    };
    parse().unwrap_or_else(|message| AgentError::Config { message })
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn run_hook(
    lua: &Lua,
    instance: &Table,
    hook: &str,
    cancel: CancelToken,
    generation: CancelToken,
    permissions: &PluginPermissions,
    source: crate::loader::SourceIdentity,
    provider: String,
    endpoint: Option<crate::api::net::ConfiguredEndpoint>,
) -> Result<JsonValue, AgentError> {
    let config_error = |message: String| AgentError::Config { message };
    if generation.is_cancelled() {
        return Err(config_error("provider generation revoked".into()));
    }
    if cancel.is_cancelled() {
        return Err(AgentError::Cancelled);
    }
    let function: Option<Function> = instance
        .raw_get(hook)
        .map_err(|e| config_error(format!("provider {hook}: {e}")))?;
    let Some(function) = function else {
        return Ok(JsonValue::Null);
    };
    let scope = TaskScope::new(lua, TaskCell::new(cancel.clone(), None, None));
    let (context, _net_scope) =
        crate::api::net::provider_context(lua, scope.handle().clone(), permissions, endpoint)
            .map_err(|e| config_error(e.to_string()))?;
    crate::provider_credentials::attach(
        lua,
        &context,
        scope.handle().clone(),
        generation.clone(),
        source,
        provider,
    )
    .map_err(|e| config_error(e.to_string()))?;
    let result = generation
        .race(cancel.race(scope.scope_future(function.call_async::<MultiValue>(context))))
        .await;
    let values = match result {
        Err(_) => {
            scope.fire_cancel_hooks();
            return Err(config_error("provider generation revoked".into()));
        }
        Ok(Err(_)) => {
            scope.fire_cancel_hooks();
            return Err(AgentError::Cancelled);
        }
        Ok(Ok(result)) => {
            result.map_err(|e| config_error(format!("provider {hook} failed: {e}")))?
        }
    };
    if generation.is_cancelled() {
        scope.fire_cancel_hooks();
        return Err(config_error("provider generation revoked".into()));
    }
    if cancel.is_cancelled() {
        scope.fire_cancel_hooks();
        return Err(AgentError::Cancelled);
    }
    let mut values = values.into_iter();
    match (
        values.next().unwrap_or(Value::Nil),
        values.next().unwrap_or(Value::Nil),
    ) {
        (Value::Nil, Value::Table(error)) => Err(provider_error(error)),
        (value, Value::Nil) if hook == "fetch_usage" => lua_to_json(lua, &value)
            .map_err(|e| config_error(format!("invalid provider usage: {e}"))),
        (Value::Boolean(rotated), Value::Nil) if hook == "rotate_key" => {
            Ok(JsonValue::Bool(rotated))
        }
        (Value::Boolean(true), Value::Nil) => Ok(JsonValue::Null),
        _ => Err(config_error(format!("invalid provider {hook} result"))),
    }
}

/// Runs a stream callback on the Lua executor. Dropping this future closes its
/// emitter immediately; cancellation also interrupts a suspended callback.
pub(crate) async fn run_stream(
    lua: &Lua,
    stream: Function,
    request: JsonValue,
    sender: flume::Sender<ProviderEvent>,
    cancel: CancelToken,
    generation: CancelToken,
    network: (
        &PluginPermissions,
        Option<crate::api::net::ConfiguredEndpoint>,
    ),
) -> Result<StreamResponse, AgentError> {
    let (permissions, endpoint) = network;
    let config_error = |message: String| AgentError::Config { message };
    let gate = Arc::new(Mutex::new(Gate {
        sender: Some(sender),
    }));
    let _close = CloseGate(Arc::clone(&gate));
    let emit = emit(lua, gate, cancel.clone()).map_err(|e| config_error(e.to_string()))?;
    let request =
        json_to_lua_with_nulls(lua, &request, true).map_err(|e| config_error(e.to_string()))?;
    let scope = TaskScope::new(lua, TaskCell::new(cancel.clone(), None, None));
    let (context, _net_scope) =
        crate::api::net::provider_context(lua, scope.handle().clone(), permissions, endpoint)
            .map_err(|e| config_error(e.to_string()))?;
    let call = scope.scope_future(stream.call_async::<MultiValue>((context, request, emit)));
    let result = generation.race(cancel.race(call)).await;
    let values = match result {
        Err(_) => {
            scope.fire_cancel_hooks();
            return Err(config_error("provider generation revoked".to_owned()));
        }
        Ok(Err(_)) => {
            scope.fire_cancel_hooks();
            return Err(AgentError::Cancelled);
        }
        Ok(Ok(values)) => {
            values.map_err(|e| config_error(format!("provider stream failed: {e}")))?
        }
    };
    if generation.is_cancelled() {
        scope.fire_cancel_hooks();
        return Err(config_error("provider generation revoked".to_owned()));
    }
    if cancel.is_cancelled() {
        scope.fire_cancel_hooks();
        return Err(AgentError::Cancelled);
    }
    let mut values = values.into_iter();
    let value = values.next().unwrap_or(Value::Nil);
    let error = values.next().unwrap_or(Value::Nil);
    match (value, error) {
        (Value::Nil, Value::Table(error)) => Err(provider_error(error)),
        (Value::Nil, _) => Err(config_error(
            "provider stream returned no response or error".into(),
        )),
        (_, error) if !matches!(error, Value::Nil) => Err(config_error(
            "provider stream returned both response and error".into(),
        )),
        (value, _) => response(
            lua_to_json(lua, &value)
                .map_err(|e| config_error(format!("invalid provider response: {e}")))?,
        )
        .map_err(config_error),
    }
}

#[cfg(test)]
mod tests {
    use super::{CloseGate, Gate, PluginPermissions, emit, response};
    use maki_agent::cancel::CancelToken;
    use mlua::{Lua, Value};
    use serde_json::json;
    use std::sync::{Arc, Mutex};

    #[test]
    fn rejects_invalid_response_role_and_usage() {
        let mut value = json!({"message": {"role": "user", "content": []}, "usage": {"input": 1, "output": 2, "cache_creation": 0, "cache_read": 0}});
        assert!(response(value.clone()).is_err());
        value["message"]["role"] = json!("assistant");
        value["usage"]["input"] = json!(-1);
        assert!(response(value).is_err());
    }

    #[test]
    fn lua_stream_emits_and_returns_response() -> mlua::Result<()> {
        let lua = Lua::new();
        let stream = lua.load(r#"return function(ctx, request, emit)
            local ok, err = emit({ type = 'text_delta', text = request.text })
            if not ok then return nil, err end
            return { message = { role = 'assistant', content = {{ type = 'text', text = request.text }} },
                usage = { input = 1, output = 2, cache_creation = 0, cache_read = 0 }, stop_reason = 'end_turn' }
        end"#).eval()?;
        let (sender, receiver) = flume::unbounded();
        let (_trigger, cancel) = CancelToken::new();
        let result = smol::block_on(super::run_stream(
            &lua,
            stream,
            json!({"text": "hello"}),
            sender,
            cancel,
            CancelToken::none(),
            (&PluginPermissions::denied(), None),
        ));
        assert!(result.is_ok(), "{result:?}");
        assert!(
            matches!(receiver.try_recv(), Ok(maki_providers::ProviderEvent::TextDelta { text }) if text == "hello")
        );
        Ok(())
    }

    #[test]
    fn stream_errors_keep_retry_and_overflow_classification() -> mlua::Result<()> {
        let lua = Lua::new();
        for (error, retryable, overflow) in [
            (
                "{kind='api', message='rate limited', status=429, retry_after_ms=1500}",
                true,
                false,
            ),
            (
                "{kind='api', message='prompt too long', status=413}",
                false,
                true,
            ),
            (
                "{kind='api', message='bad request', status=400}",
                false,
                false,
            ),
            ("{kind='network', message='connection lost'}", true, false),
            ("{kind='config', message='bad config'}", false, false),
        ] {
            let stream = lua
                .load(format!("return function() return nil, {error} end"))
                .eval()?;
            let (sender, _receiver) = flume::unbounded();
            let result = smol::block_on(super::run_stream(
                &lua,
                stream,
                json!({}),
                sender,
                CancelToken::none(),
                CancelToken::none(),
                (&PluginPermissions::denied(), None),
            ))
            .unwrap_err();
            assert_eq!(result.is_retryable(), retryable, "{error}");
            assert_eq!(result.is_context_overflow(), overflow, "{error}");
            if let maki_providers::AgentError::Api {
                status: 429,
                retry_after,
                ..
            } = result
            {
                assert_eq!(retry_after, Some(std::time::Duration::from_millis(1500)));
            }
        }
        Ok(())
    }

    #[test]
    fn zero_retry_hint_uses_bounded_rate_limit_budget() -> mlua::Result<()> {
        use maki_providers::retry::{RetryKind, RetryPolicy, RetryState};

        let lua = Lua::new();
        let error = super::provider_error(
            lua.load("{kind='api', message='limited', status=429, retry_after_ms=0}")
                .eval()?,
        );
        assert_eq!(error.retry_kind(), Some(RetryKind::RateLimit));
        assert_eq!(error.retry_after(), None);
        let mut state = RetryState::new(
            RetryPolicy {
                max_retries: 0,
                ..RetryPolicy::default()
            },
            1,
        );
        assert_eq!(
            state.next_delay(RetryKind::RateLimit, error.retry_after()),
            None
        );
        Ok(())
    }

    #[test]
    fn network_retry_hint_is_preserved_and_validated() -> mlua::Result<()> {
        use maki_providers::retry::{RetryKind, RetryPolicy, RetryState};
        use std::time::Duration;

        let lua = Lua::new();
        for (hint, expected) in [
            ("1500", Some(Duration::from_millis(1500))),
            ("0", None),
            ("nil", None),
        ] {
            let error = super::provider_error(
                lua.load(format!(
                    "{{kind='network', message='lost', retry_after_ms={hint}}}"
                ))
                .eval()?,
            );
            assert_eq!(error.retry_kind(), Some(RetryKind::Transient));
            assert_eq!(error.retry_after(), expected);
            let mut state = RetryState::new(
                RetryPolicy {
                    base_delay: Duration::from_millis(100),
                    ..RetryPolicy::default()
                },
                1,
            );
            if let Some(delay) = expected {
                assert_eq!(
                    state.next_delay(RetryKind::Transient, error.retry_after()),
                    Some(delay)
                );
            }
        }
        let error = super::provider_error(
            lua.load("{kind='network', message='lost', retry_after_ms=-1}")
                .eval()?,
        );
        assert!(matches!(error, maki_providers::AgentError::Config { .. }));
        Ok(())
    }

    #[test]
    fn stream_preserves_structured_overflow() -> mlua::Result<()> {
        let lua = Lua::new();
        for (overflow, expected) in [
            ("{kind='prompt'}", Some(maki_providers::Overflow::Prompt)),
            (
                "{kind='budget', prompt=10, limit=20}",
                Some(maki_providers::Overflow::Budget {
                    prompt: Some(10),
                    limit: Some(20),
                }),
            ),
            (
                "{kind='budget'}",
                Some(maki_providers::Overflow::Budget {
                    prompt: None,
                    limit: None,
                }),
            ),
            ("{kind='budget', prompt=-1}", None),
            ("{kind='unknown'}", None),
        ] {
            let stream = lua.load(format!("return function() return nil, {{kind='api', message='opaque', status=500, overflow={overflow}}} end")).eval()?;
            let (sender, _) = flume::unbounded();
            let error = smol::block_on(super::run_stream(
                &lua,
                stream,
                json!({}),
                sender,
                CancelToken::none(),
                CancelToken::none(),
                (&PluginPermissions::denied(), None),
            ))
            .unwrap_err();
            assert_eq!(error.overflow(), expected, "{overflow}");
            assert!(!error.is_retryable(), "{overflow}");
            if expected.is_none() {
                assert!(matches!(error, maki_providers::AgentError::Config { .. }));
            }
        }
        Ok(())
    }

    #[test]
    fn rejects_tool_results_in_assistant_response() {
        let value = json!({"message": {"role": "assistant", "content": [{"type": "tool_result", "tool_use_id": "id", "content": "result"}]}, "usage": {"input": 1, "output": 2, "cache_creation": 0, "cache_read": 0}});
        assert!(
            response(value)
                .unwrap_err()
                .contains("invalid assistant content")
        );
    }

    #[test]
    fn emitter_reports_backpressure_and_disconnection() -> mlua::Result<()> {
        let lua = Lua::new();
        let (sender, receiver) = flume::bounded(1);
        let gate = Arc::new(Mutex::new(Gate {
            sender: Some(sender),
        }));
        let emit = emit(&lua, gate, CancelToken::none())?;
        let event: Value = lua
            .load("return { type = 'text_delta', text = 'hello' }")
            .eval()?;
        let (ok, _): (Value, Value) = emit.call(event.clone())?;
        assert!(matches!(ok, Value::Boolean(true)));
        let (ok, error): (Value, mlua::Table) = emit.call(event.clone())?;
        assert!(matches!(ok, Value::Nil));
        assert_eq!(error.get::<String>("kind")?, "backpressure");
        assert!(receiver.try_recv().is_ok());
        drop(receiver);
        let (ok, error): (Value, mlua::Table) = emit.call(event)?;
        assert!(matches!(ok, Value::Nil));
        assert_eq!(error.get::<String>("kind")?, "closed");
        Ok(())
    }

    #[test]
    fn cancelled_callback_cannot_return_success() -> mlua::Result<()> {
        let lua = Lua::new();
        let (trigger, cancel) = CancelToken::new();
        let trigger = Mutex::new(Some(trigger));
        lua.globals().set(
            "cancel",
            lua.create_function(move |_, ()| {
                if let Some(trigger) = trigger.lock().unwrap().take() {
                    trigger.cancel();
                }
                Ok(())
            })?,
        )?;
        let stream = lua.load("return function() cancel(); return {message={role='assistant',content={}},usage={input=0,output=0,cache_creation=0,cache_read=0}} end").eval()?;
        let (sender, _receiver) = flume::unbounded();
        let result = smol::block_on(super::run_stream(
            &lua,
            stream,
            json!({}),
            sender,
            cancel,
            CancelToken::none(),
            (&PluginPermissions::denied(), None),
        ));
        assert!(matches!(
            result.unwrap_err(),
            maki_providers::AgentError::Cancelled
        ));
        Ok(())
    }

    #[test]
    fn generation_revocation_fires_stream_cancel_hooks() -> mlua::Result<()> {
        let lua = Lua::new();
        let (trigger, generation) = CancelToken::new();
        lua.globals().set(
            "arm",
            lua.create_function(|lua, hook: mlua::Function| {
                crate::runtime::register_cancel_hook(lua, hook)
            })?,
        )?;
        lua.globals().set(
            "park",
            lua.create_async_function(|_, ()| async {
                std::future::pending::<mlua::Result<()>>().await
            })?,
        )?;
        let stream = lua.load("return function() arm(function(reason) fired = reason end); parked = true; park() end").eval()?;
        let (sender, _) = flume::unbounded();
        let permissions = PluginPermissions::denied();
        let future = super::run_stream(
            &lua,
            stream,
            json!({}),
            sender,
            CancelToken::none(),
            generation,
            (&permissions, None),
        );
        let mut future = std::pin::pin!(future);
        assert!(smol::block_on(futures_lite::future::poll_once(future.as_mut())).is_none());
        assert!(lua.globals().get::<bool>("parked")?);
        trigger.cancel();
        let result = smol::block_on(future);
        assert!(matches!(
            result,
            Err(maki_providers::AgentError::Config { .. })
        ));
        assert_eq!(lua.globals().get::<String>("fired")?, "cancelled");
        Ok(())
    }

    #[test]
    fn emitter_rejects_invalid_and_revoked_events() -> mlua::Result<()> {
        let lua = Lua::new();
        let (trigger, cancel) = CancelToken::new();
        let (sender, receiver) = flume::unbounded();
        let gate = Arc::new(Mutex::new(Gate {
            sender: Some(sender),
        }));
        let close = CloseGate(Arc::clone(&gate));
        let emit = emit(&lua, gate, cancel)?;
        let malformed: Value = lua
            .load("return { type = 'prompt_progress', total = -1 }")
            .eval()?;
        let (ok, _): (Value, Value) = emit.call(malformed)?;
        assert!(matches!(ok, Value::Nil));
        let valid: Value = lua
            .load("return { type = 'text_delta', text = 'hello' }")
            .eval()?;
        let (ok, _): (Value, Value) = emit.call(valid.clone())?;
        assert!(matches!(ok, Value::Boolean(true)));
        assert!(receiver.try_recv().is_ok());
        drop(trigger);
        let (ok, _): (Value, Value) = emit.call(valid.clone())?;
        assert!(matches!(ok, Value::Nil));
        drop(close);
        let (ok, _): (Value, Value) = emit.call(valid)?;
        assert!(matches!(ok, Value::Nil));
        Ok(())
    }
}
