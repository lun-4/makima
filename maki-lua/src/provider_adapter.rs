use std::sync::Arc;

use flume::Sender;
use maki_agent::cancel::CancelToken;
use maki_providers::{
    AgentError, Message, Model, ModelInfo, ProviderEvent, ProviderUsage, RequestOptions,
    StreamResponse, ThinkingConfig, ThinkingConfigExt,
    provider::{BoxFuture, Provider},
    provider_registry::{InteractionService, PreparedProvider, ProviderFactory},
};
use maki_storage::id::SessionRef;
use serde_json::{Value, json};

/// Thread-safe dispatch boundary implemented by the Lua runtime. Implementations
/// must bind instance IDs to their originating plugin generation.
pub trait ProviderDispatch: Send + Sync {
    fn auth<'a>(
        &'a self,
        _provider: &'a str,
        _hook: &'static str,
        _input: &'a Value,
        _cancel: CancelToken,
    ) -> BoxFuture<'a, Result<Value, AgentError>> {
        Box::pin(async { Ok(Value::Null) })
    }

    fn login_interactive<'a>(
        &'a self,
        _provider: &'a str,
        _method: &'a str,
        _interaction: Arc<dyn InteractionService>,
        _cancel: CancelToken,
    ) -> BoxFuture<'a, Result<Value, AgentError>> {
        Box::pin(async {
            Err(AgentError::Config {
                message: "interactive login unavailable".into(),
            })
        })
    }

    fn discover<'a>(
        &'a self,
        _provider: &'a str,
        _config: &'a Value,
        _cancel: CancelToken,
    ) -> BoxFuture<'a, Result<Option<Vec<ModelInfo>>, AgentError>> {
        Box::pin(async { Ok(None) })
    }

    fn prepare<'a>(
        &'a self,
        provider: &'a str,
        model: &'a Model,
        config: &'a Value,
        cancel: CancelToken,
    ) -> BoxFuture<'a, Result<(String, Model, usize), AgentError>>;

    fn release(&self, instance: &str);

    fn hook<'a>(
        &'a self,
        _instance: &'a str,
        _hook: &'static str,
        _cancel: CancelToken,
    ) -> BoxFuture<'a, Result<Value, AgentError>> {
        Box::pin(async { Ok(Value::Null) })
    }

    fn stream<'a>(
        &'a self,
        instance: &'a str,
        request: Value,
        events: Sender<ProviderEvent>,
        cancel: CancelToken,
    ) -> BoxFuture<'a, Result<StreamResponse, AgentError>>;
}

pub struct LuaProviderFactory {
    pub provider: String,
    pub models: Vec<ModelInfo>,
    pub dispatch: Arc<dyn ProviderDispatch>,
}

impl ProviderFactory for LuaProviderFactory {
    fn login_interactive<'a>(
        &'a self,
        method: &'a str,
        interaction: Arc<dyn InteractionService>,
    ) -> BoxFuture<'a, Result<Value, AgentError>> {
        Box::pin(async move {
            let (_trigger, cancel) = CancelToken::new();
            self.dispatch
                .login_interactive(&self.provider, method, interaction, cancel)
                .await
        })
    }

    fn auth_status<'a>(&'a self, config: &'a Value) -> BoxFuture<'a, Result<Value, AgentError>> {
        Box::pin(async move {
            let (_trigger, cancel) = CancelToken::new();
            self.dispatch
                .auth(&self.provider, "auth_status", config, cancel)
                .await
        })
    }

    fn logout<'a>(&'a self, config: &'a Value) -> BoxFuture<'a, Result<Value, AgentError>> {
        Box::pin(async move {
            let (_trigger, cancel) = CancelToken::new();
            self.dispatch
                .auth(&self.provider, "logout", config, cancel)
                .await
        })
    }

    fn login<'a>(
        &'a self,
        method: &'a str,
        interaction: &'a Value,
    ) -> BoxFuture<'a, Result<Value, AgentError>> {
        Box::pin(async move {
            let (_trigger, cancel) = CancelToken::new();
            let input = json!({ "method": method, "interaction": interaction });
            self.dispatch
                .auth(&self.provider, "login", &input, cancel)
                .await
        })
    }

    fn discover<'a>(
        &'a self,
        config: &'a Value,
    ) -> BoxFuture<'a, Result<Option<Vec<ModelInfo>>, AgentError>> {
        Box::pin(async move {
            let (_trigger, cancel) = CancelToken::new();
            self.dispatch.discover(&self.provider, config, cancel).await
        })
    }

    fn prepare<'a>(
        &'a self,
        model: &'a Model,
        config: &'a Value,
    ) -> BoxFuture<'a, PreparedProvider> {
        Box::pin(async move {
            let (_trigger, cancel) = CancelToken::new();
            let (instance, model, key_count) = self
                .dispatch
                .prepare(&self.provider, model, config, cancel)
                .await?;
            let provider = LuaProvider {
                instance,
                key_count,
                models: self.models.clone(),
                dispatch: Arc::clone(&self.dispatch),
            };
            Ok((model, Box::new(provider) as Box<dyn Provider>))
        })
    }
}

struct LuaProvider {
    instance: String,
    key_count: usize,
    models: Vec<ModelInfo>,
    dispatch: Arc<dyn ProviderDispatch>,
}

impl Drop for LuaProvider {
    fn drop(&mut self) {
        self.dispatch.release(&self.instance);
    }
}

impl Provider for LuaProvider {
    fn stream_message<'a>(
        &'a self,
        model: &'a Model,
        messages: &'a [Message],
        system: &'a str,
        tools: &'a Value,
        event_tx: &'a Sender<ProviderEvent>,
        opts: RequestOptions,
        session_id: Option<&'a SessionRef>,
    ) -> BoxFuture<'a, Result<StreamResponse, AgentError>> {
        Box::pin(async move {
            let (trigger, cancel) = CancelToken::new();
            let request = request(model, messages, system, tools, opts, session_id)?;
            let result = self
                .dispatch
                .stream(&self.instance, request, event_tx.clone(), cancel)
                .await;
            drop(trigger);
            result
        })
    }

    fn key_count(&self) -> usize {
        self.key_count
    }

    fn rotate_key(&self) -> BoxFuture<'_, Result<bool, AgentError>> {
        Box::pin(async {
            let (_trigger, cancel) = CancelToken::new();
            match self
                .dispatch
                .hook(&self.instance, "rotate_key", cancel)
                .await?
            {
                Value::Null => Ok(false),
                Value::Bool(rotated) => Ok(rotated),
                _ => Err(AgentError::Config {
                    message: "invalid provider rotate_key result".into(),
                }),
            }
        })
    }

    fn list_models(&self) -> BoxFuture<'_, Result<Vec<ModelInfo>, AgentError>> {
        Box::pin(async { Ok(self.models.clone()) })
    }

    fn fetch_usage(&self) -> BoxFuture<'_, Result<Option<ProviderUsage>, AgentError>> {
        Box::pin(async {
            let (_trigger, cancel) = CancelToken::new();
            let mut value = self
                .dispatch
                .hook(&self.instance, "fetch_usage", cancel)
                .await?;
            if value.is_null() {
                return Ok(None);
            }
            if let Some(object) = value.as_object_mut() {
                for field in ["limits", "by_model_today"] {
                    if let Some(value) = object.get_mut(field)
                        && value.as_object().is_some_and(|object| object.is_empty())
                    {
                        *value = Value::Array(Vec::new());
                    }
                }
            }
            let usage: Usage = serde_json::from_value(value).map_err(|e| AgentError::Config {
                message: format!("invalid provider usage: {e}"),
            })?;
            Ok(Some(usage.into()))
        })
    }

    fn refresh_auth(&self) -> BoxFuture<'_, Result<(), AgentError>> {
        Box::pin(async {
            let (_trigger, cancel) = CancelToken::new();
            self.dispatch
                .hook(&self.instance, "refresh_auth", cancel)
                .await
                .map(|_| ())
        })
    }

    fn reload_auth(&self) -> BoxFuture<'_, Result<(), AgentError>> {
        Box::pin(async {
            let (_trigger, cancel) = CancelToken::new();
            self.dispatch
                .hook(&self.instance, "reload_auth", cancel)
                .await
                .map(|_| ())
        })
    }
}

#[derive(serde::Deserialize)]
struct Usage {
    plan: Option<String>,
    limits: Vec<UsageLimit>,
    #[serde(default)]
    by_model_today: Vec<maki_providers::ModelUsageRow>,
}

#[derive(serde::Deserialize)]
struct UsageLimit {
    kind: UsageWindow,
    percentage: Option<u32>,
    reset_at: Option<u64>,
    detail: Option<String>,
}

#[derive(serde::Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum UsageWindow {
    Hours { hours: u64 },
    Days { days: u64 },
    Monthly,
    Weekly { model: Option<String> },
    Credits,
    Subscription,
    Other { label: String },
}

impl From<Usage> for ProviderUsage {
    fn from(value: Usage) -> Self {
        use maki_providers::{UsageLimit as Limit, UsageWindow as Window};
        Self {
            plan: value.plan,
            limits: value
                .limits
                .into_iter()
                .map(|limit| Limit {
                    kind: match limit.kind {
                        UsageWindow::Hours { hours } => Window::Hours(hours),
                        UsageWindow::Days { days } => Window::Days(days),
                        UsageWindow::Monthly => Window::Monthly,
                        UsageWindow::Weekly { model } => Window::Weekly { model },
                        UsageWindow::Credits => Window::Credits,
                        UsageWindow::Subscription => Window::Subscription,
                        UsageWindow::Other { label } => Window::Other(label),
                    },
                    percentage: limit.percentage,
                    reset_at: limit.reset_at,
                    detail: limit.detail,
                })
                .collect(),
            by_model_today: value.by_model_today,
        }
    }
}

pub(crate) fn model_metadata(model: &Model) -> Value {
    let mut metadata = json!({
        "id": model.id,
        "provider": model.provider.as_ref(),
        "context_window": model.context_window,
        "max_output_tokens": model.max_output_tokens,
        "turn_output_tokens": model.turn_output_tokens,
        "vision": model.supports_vision(),
        "fast": model.supports_fast(),
        "tool_examples": model.supports_tool_examples(),
        "thinking": if model.supports_thinking() {
            json!({"mode": "adaptive", "levels": [], "required": model.requires_thinking()})
        } else {
            json!({"mode": "none"})
        },
        "pricing": {
            "input": model.pricing.input,
            "output": model.pricing.output,
            "cache_write": model.pricing.cache_write,
            "cache_read": model.pricing.cache_read,
            "fast": model.pricing.fast.as_ref().map(|fast| json!({"input": fast.input, "output": fast.output})),
        },
    });
    if let Value::Object(fields) = &mut metadata {
        fields.retain(|_, value| !value.is_null());
        if let Some(Value::Object(pricing)) = fields.get_mut("pricing") {
            pricing.retain(|_, value| !value.is_null());
        }
    }
    metadata
}

pub(crate) fn apply_prepared_metadata(
    lua: &mlua::Lua,
    prepared: &mlua::Table,
    model: &mut Model,
) -> Result<(), String> {
    use maki_providers::model::{FastSupport, ThinkingSupport};
    use mlua::Value as LuaValue;

    let wrapper = lua.create_table().map_err(|e| e.to_string())?;
    let models = lua.create_table().map_err(|e| e.to_string())?;
    models
        .raw_set(1, prepared.clone())
        .map_err(|e| e.to_string())?;
    wrapper
        .raw_set("models", models)
        .map_err(|e| e.to_string())?;
    let descriptor = crate::provider_staging::model_descriptors(&wrapper)?.remove(0);
    if let Some(context) = descriptor.context_window {
        model.context_window = context;
    }
    if let Some(max) = descriptor.max_output_tokens {
        model.max_output_tokens = Some(max);
    }
    match prepared
        .raw_get::<LuaValue>("turn_output_tokens")
        .map_err(|e| e.to_string())?
    {
        LuaValue::Nil => {}
        LuaValue::Integer(n) if n > 0 && n <= u32::MAX as i64 => {
            model.turn_output_tokens = Some(n as u32)
        }
        LuaValue::Number(n)
            if n.is_finite() && n > 0.0 && n <= u32::MAX as f64 && n.fract() == 0.0 =>
        {
            model.turn_output_tokens = Some(n as u32)
        }
        _ => {
            return Err(
                "prepared model turn_output_tokens: expected a positive integer within u32 range"
                    .into(),
            );
        }
    }
    if let Some(pricing) = descriptor.pricing {
        model.pricing = pricing;
    }
    for field in ["vision", "fast", "tool_examples"] {
        match prepared
            .raw_get::<LuaValue>(field)
            .map_err(|e| e.to_string())?
        {
            LuaValue::Nil => {}
            LuaValue::Boolean(value) => match field {
                "vision" => model.supports_vision_override = Some(value),
                "fast" => {
                    model.supports_fast_override = Some(if value {
                        FastSupport::Supported
                    } else {
                        FastSupport::Unsupported
                    })
                }
                "tool_examples" => model.supports_tool_examples_override = Some(value),
                _ => unreachable!(),
            },
            _ => return Err(format!("prepared model {field}: expected a boolean")),
        }
    }
    let thinking: LuaValue = prepared.raw_get("thinking").map_err(|e| e.to_string())?;
    if let LuaValue::Table(thinking) = thinking {
        let mode: String = thinking
            .raw_get("mode")
            .map_err(|e| format!("prepared model thinking.mode: {e}"))?;
        let required: Option<bool> = thinking
            .raw_get("required")
            .map_err(|e| format!("prepared model thinking.required: {e}"))?;
        model.thinking_override = Some(match mode.as_str() {
            "none" if required != Some(true) => ThinkingSupport::No,
            "adaptive" | "effort" | "budget" => {
                if required == Some(true) {
                    ThinkingSupport::Required
                } else {
                    ThinkingSupport::Yes
                }
            }
            _ => return Err("prepared model thinking.mode: invalid mode".into()),
        });
    } else if !matches!(thinking, LuaValue::Nil) {
        return Err("prepared model thinking: expected a table".into());
    }
    Ok(())
}

fn request(
    model: &Model,
    messages: &[Message],
    system: &str,
    tools: &Value,
    opts: RequestOptions,
    session_id: Option<&SessionRef>,
) -> Result<Value, AgentError> {
    let thinking = match opts.thinking {
        ThinkingConfig::Off => json!({"mode": "off"}),
        ThinkingConfig::Adaptive => json!({"mode": "adaptive"}),
        ThinkingConfig::Effort(level) => json!({"mode": "effort", "level": level.as_str()}),
        ThinkingConfig::Budget(tokens) => json!({"mode": "budget", "tokens": tokens}),
    };
    let mut local_thinking = json!({});
    opts.thinking
        .apply_local_thinking(&mut local_thinking, model);
    Ok(json!({
        "local_thinking": local_thinking,
        "model": model_metadata(model),
        "system": system,
        "messages": serde_json::to_value(messages)?,
        "tools": tools,
        "session_id": session_id.map(SessionRef::as_str),
        "thinking": thinking,
        "fast": opts.fast,
    }))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use maki_providers::{
        ContentBlock, Effort, ImageMediaType, ImageSource, Message, Model, RequestOptions,
        ThinkingConfig, adapt_images_for_model,
    };
    use serde_json::json;
    use test_case::test_case;

    #[test]
    fn prepared_metadata_updates_model_and_request() {
        let lua = mlua::Lua::new();
        let prepared = lua.load(r#"return { id = 'gpt-5.6-sol', context_window = 8192, max_output_tokens = 1024, vision = false, fast = true, tool_examples = true, thinking = { mode = 'budget', required = true }, pricing = { input = 1, output = 2, cache_write = 3, cache_read = 4, fast = { input = 5, output = 6 } } }"#).eval().unwrap();
        let mut model = Model::from_spec("openai/gpt-5.6-sol").unwrap();
        super::apply_prepared_metadata(&lua, &prepared, &mut model).unwrap();
        assert_eq!(model.context_window, 8192);
        assert_eq!(model.max_output_tokens, Some(1024));
        assert!(!model.supports_vision());
        assert!(
            model.supports_fast() && model.supports_tool_examples() && model.requires_thinking()
        );
        let metadata = super::model_metadata(&model);
        assert_eq!(metadata["pricing"]["fast"]["output"], 6.0);
        assert_eq!(metadata["thinking"]["required"], true);
        let invalid = lua
            .load("return { id = 'model', context_window = 0 }")
            .eval()
            .unwrap();
        assert!(super::apply_prepared_metadata(&lua, &invalid, &mut model).is_err());
    }

    #[test]
    fn request_preserves_adapted_images_without_mutating_history() {
        let mut model = Model::from_spec("openai/gpt-5.6-sol").unwrap();
        model.supports_vision_override = Some(false);
        let history = vec![Message::user_with_images(
            "look".into(),
            vec![ImageSource::new(ImageMediaType::Png, Arc::from("invalid"))],
        )];
        let adapted = smol::block_on(adapt_images_for_model(&model, &history));
        let request = super::request(
            &model,
            &adapted,
            "",
            &json!([]),
            RequestOptions::default(),
            None,
        )
        .unwrap();
        assert_eq!(request["messages"][0]["content"][0]["type"], "text");
        assert_eq!(
            request["messages"][0]["content"][0]["text"],
            maki_providers::IMAGE_OMITTED_NOTE
        );
        assert!(matches!(history[0].content[0], ContentBlock::Image { .. }));
    }

    #[test_case(ThinkingConfig::Off, json!({"mode": "off"}); "off")]
    #[test_case(ThinkingConfig::Adaptive, json!({"mode": "adaptive"}); "adaptive")]
    #[test_case(ThinkingConfig::Effort(Effort::High), json!({"mode": "effort", "level": "high"}); "effort")]
    #[test_case(ThinkingConfig::Budget(2048), json!({"mode": "budget", "tokens": 2048}); "budget")]
    fn request_preserves_thinking(thinking: ThinkingConfig, expected: serde_json::Value) {
        let model = Model::from_spec("openai/gpt-5.6-sol").unwrap();
        let request = super::request(
            &model,
            &[],
            "system",
            &json!([]),
            RequestOptions {
                thinking,
                fast: true,
            },
            None,
        )
        .unwrap();
        assert_eq!(request["thinking"], expected);
        assert_eq!(request["model"]["id"], model.id);
        assert_eq!(request["fast"], true);
        assert_eq!(request["tools"], json!([]));
    }
}
