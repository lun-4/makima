//! Providers registered from Lua, behind `ProviderBackend` and `Provider`.
//! Every call runs as a job on the Lua thread; nothing here touches Lua from
//! any other thread.

use std::future::Future;
use std::io;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use flume::Sender;
use futures::future::LocalBoxFuture;
use maki_agent::cancel::CancelToken;
use maki_config::providers::ProvidersConfig;
use maki_providers::login::{AuthStatus, InteractionService};
use maki_providers::provider::{BoxFuture, Provider};
use maki_providers::{
    AgentError, ConversationEnd, Invocation, Message, Model, ModelInfo, PreparedProvider,
    ProviderBackend, ProviderEvent, ProviderRegistry, ProviderUsage, RegisteredProvider,
    RequestOptions, StopReason, StreamResponse, Timeouts, TokenUsage,
};
use maki_storage::id::SessionRef;
use mlua::{
    Function, IntoLuaMulti, Lua, LuaSerdeExt, MultiValue, RegistryKey, SerializeOptions, Table,
    Value as LuaValue,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tracing::warn;

use crate::api::provider::{ProviderSpec, ProviderStore};
use crate::credentials::Credentials;
use crate::login_ui::LoginUi;
use crate::runtime::{Request, strip_traceback};

pub(crate) const HOST_STOPPED: &str = "plugin host stopped";
const NO_STREAM: &str = "provider instance has no stream function";
const DEFAULT_KEY_COUNT: usize = 1;
const LOGIN_INCOMPLETE: &str = "login did not complete";

pub(crate) type ProviderJob = Box<dyn FnOnce(Lua) -> LocalBoxFuture<'static, ()> + Send>;
/// The outer error is raised Lua; the inner one is what the provider returned.
type JobResult<T> = mlua::Result<Result<T, AgentError>>;

pub(crate) fn build_registry(
    lua: &Lua,
    requests: &Sender<Request>,
    timeouts: Timeouts,
) -> ProviderRegistry {
    let providers = lua
        .app_data_ref::<ProviderStore>()
        .map(|store| {
            store
                .specs()
                .iter()
                .filter_map(|spec| match registered(lua, requests, spec) {
                    Ok(provider) => Some(provider),
                    Err(error) => {
                        warn!(provider = %spec.id, plugin = %spec.plugin, %error, "provider left out of registry");
                        None
                    }
                })
                .collect()
        })
        .unwrap_or_default();
    ProviderRegistry::new(providers, timeouts)
}

fn registered(
    lua: &Lua,
    requests: &Sender<Request>,
    spec: &ProviderSpec,
) -> mlua::Result<RegisteredProvider> {
    let callbacks = Callbacks {
        id: Arc::clone(&spec.id),
        prepare: lua.create_registry_value(spec.prepare.clone())?,
        discover: optional_key(lua, &spec.discover)?,
        logins: spec
            .auth
            .methods
            .iter()
            .zip(&spec.logins)
            .map(|(method, login)| {
                Ok((method.id.clone(), lua.create_registry_value(login.clone())?))
            })
            .collect::<mlua::Result<_>>()?,
        auth_status: optional_key(lua, &spec.auth_status)?,
        logout: optional_key(lua, &spec.logout)?,
        send: lua.create_registry_value(spec.send.clone())?,
    };
    Ok(RegisteredProvider {
        id: Arc::clone(&spec.id),
        display_name: Arc::clone(&spec.display_name),
        models: spec.models.clone(),
        auth: spec.auth.clone(),
        backend: Arc::new(LuaBackend {
            requests: requests.clone(),
            callbacks: Arc::new(callbacks),
        }),
    })
}

fn optional_key(lua: &Lua, function: &Option<Function>) -> mlua::Result<Option<RegistryKey>> {
    function
        .clone()
        .map(|function| lua.create_registry_value(function))
        .transpose()
}

/// Runs {call} on the Lua thread. Dropping the returned future cancels the
/// call, which ends the Lua side and its scope's resources.
async fn invoke<T: Send + 'static>(
    requests: &Sender<Request>,
    call: impl FnOnce(Lua) -> LocalBoxFuture<'static, JobResult<T>> + Send + 'static,
) -> Result<T, AgentError> {
    let (_cancel_on_drop, cancel) = CancelToken::new();
    let (reply_tx, reply_rx) = flume::bounded(1);
    let job: ProviderJob = Box::new(move |lua| {
        Box::pin(async move {
            let result = call(lua).await.unwrap_or_else(|error| Err(raised(&error)));
            let _ = reply_tx.send(result);
        })
    });
    requests
        .send(Request::Provider { job, cancel })
        .map_err(|_| config_error(HOST_STOPPED))?;
    reply_rx
        .recv_async()
        .await
        .map_err(|_| config_error(HOST_STOPPED))?
}

fn config_error(message: impl Into<String>) -> AgentError {
    AgentError::Config {
        message: message.into(),
    }
}

fn raised(error: &mlua::Error) -> AgentError {
    config_error(strip_traceback(error))
}

/// `nil, err` is a returned failure; anything else is the result.
fn returned(values: MultiValue) -> Result<MultiValue, AgentError> {
    match (values.front(), values.get(1)) {
        (None | Some(LuaValue::Nil), Some(error)) if !error.is_nil() => Err(returned_error(error)),
        _ => Ok(values),
    }
}

fn returned_error(error: &LuaValue) -> AgentError {
    let (status, message, retry_after) = match error {
        LuaValue::Table(fields) => (
            fields.get::<Option<u16>>("status").ok().flatten(),
            fields
                .get::<Option<String>>("message")
                .ok()
                .flatten()
                .unwrap_or_default(),
            fields.get::<Option<f64>>("retry_after").ok().flatten(),
        ),
        LuaValue::String(message) => (None, message.to_string_lossy(), None),
        other => (None, other.type_name().to_owned(), None),
    };
    match status {
        Some(status) => AgentError::Api {
            status,
            message,
            retry_after: retry_after.and_then(|secs| Duration::try_from_secs_f64(secs).ok()),
        },
        None => AgentError::Io(io::Error::other(message)),
    }
}

fn first(values: MultiValue) -> LuaValue {
    values.into_iter().next().unwrap_or(LuaValue::Nil)
}

/// Calls {function} with ctx and {args}. {decode} gets the result, or `None`
/// when there is no function to call.
async fn call_with_ctx<T>(
    lua: &Lua,
    callbacks: &Callbacks,
    function: Option<Function>,
    args: impl FnOnce(&Lua) -> mlua::Result<MultiValue>,
    decode: impl FnOnce(&Lua, Option<LuaValue>) -> JobResult<T>,
) -> JobResult<T> {
    let Some(function) = function else {
        return decode(lua, None);
    };
    let mut call_args = args(lua)?;
    call_args.push_front(LuaValue::Table(callbacks.ctx(lua)?));
    let values = function.call_async::<MultiValue>(call_args).await?;
    match returned(values) {
        Ok(values) => decode(lua, Some(first(values))),
        Err(error) => Ok(Err(error)),
    }
}

pub(crate) fn serialize_options() -> SerializeOptions {
    SerializeOptions::new().serialize_none_to_null(false)
}

struct Callbacks {
    id: Arc<str>,
    prepare: RegistryKey,
    discover: Option<RegistryKey>,
    logins: Vec<(String, RegistryKey)>,
    auth_status: Option<RegistryKey>,
    logout: Option<RegistryKey>,
    send: RegistryKey,
}

impl Callbacks {
    fn ctx(&self, lua: &Lua) -> mlua::Result<Table> {
        let ctx = lua.create_table()?;
        ctx.set("id", &*self.id)?;
        ctx.set("send", lua.registry_value::<Function>(&self.send)?)?;
        ctx.set(
            "credentials",
            Credentials {
                id: Arc::clone(&self.id),
            },
        )?;
        Ok(ctx)
    }
}

struct LuaBackend {
    requests: Sender<Request>,
    callbacks: Arc<Callbacks>,
}

impl LuaBackend {
    /// Calls the callback {key} picks with ctx and {args}. {decode} gets the
    /// result, or `None` when the provider has no such callback.
    fn call_callback<T: Send + 'static>(
        &self,
        key: impl FnOnce(&Callbacks) -> Option<&RegistryKey> + Send + 'static,
        args: impl FnOnce(&Lua) -> mlua::Result<MultiValue> + Send + 'static,
        decode: impl FnOnce(&Lua, Option<LuaValue>) -> JobResult<T> + Send + 'static,
    ) -> BoxFuture<'static, Result<T, AgentError>> {
        let requests = self.requests.clone();
        let callbacks = Arc::clone(&self.callbacks);
        Box::pin(async move {
            invoke(&requests, move |lua| {
                Box::pin(async move {
                    let function = key(&callbacks)
                        .map(|key| lua.registry_value::<Function>(key))
                        .transpose()?;
                    call_with_ctx(&lua, &callbacks, function, args, decode).await
                })
            })
            .await
        })
    }
}

impl ProviderBackend for LuaBackend {
    fn prepare(
        &self,
        model: Model,
        info: ModelInfo,
    ) -> BoxFuture<'static, Result<PreparedProvider, AgentError>> {
        let requests = self.requests.clone();
        let callbacks = Arc::clone(&self.callbacks);
        Box::pin(async move {
            let job_callbacks = Arc::clone(&callbacks);
            let config = provider_config(&callbacks.id);
            let (instance, adjusted, key_count) = invoke(&requests, move |lua| {
                Box::pin(
                    async move { prepare_instance(&lua, &job_callbacks, &info, &config).await },
                )
            })
            .await?;
            let model = match adjusted {
                Some(info) if info.id != model.id => {
                    return Err(config_error(format!(
                        "prepare returned model {:?} for {:?}",
                        info.id, model.id
                    )));
                }
                Some(info) => Model::from_info(&model.provider, &info),
                None => model,
            };
            Ok(PreparedProvider {
                model,
                provider: Arc::new(LuaProvider {
                    requests,
                    callbacks,
                    instance: Arc::new(instance),
                    key_count,
                }),
            })
        })
    }

    fn discover(&self) -> Option<BoxFuture<'static, Result<Vec<ModelInfo>, AgentError>>> {
        self.callbacks.discover.as_ref()?;
        let config = provider_config(&self.callbacks.id);
        Some(self.call_callback(
            |callbacks| callbacks.discover.as_ref(),
            move |lua| config_arg(lua, &config),
            |lua, models| Ok(Ok(lua.from_value(models.unwrap_or(LuaValue::Nil))?)),
        ))
    }

    fn login(
        &self,
        method: String,
        config: Value,
        ui: Arc<dyn InteractionService>,
    ) -> BoxFuture<'static, Result<Option<String>, AgentError>> {
        let cancelled = Arc::new(AtomicBool::new(false));
        let login_ui = LoginUi {
            service: ui,
            cancelled: Arc::clone(&cancelled),
        };
        let unknown = format!("unknown login method {method:?}");
        let call = self.call_callback(
            move |callbacks| {
                let (_, login) = callbacks.logins.iter().find(|(id, _)| *id == method)?;
                Some(login)
            },
            move |lua| {
                let config = lua.to_value_with(&config, serialize_options())?;
                (login_ui, config).into_lua_multi(lua)
            },
            |_, result| {
                Ok(match result {
                    None => Err(config_error(unknown)),
                    Some(LuaValue::Nil | LuaValue::Boolean(false)) => {
                        Err(config_error(LOGIN_INCOMPLETE))
                    }
                    Some(LuaValue::Table(result)) => Ok(result.get("model")?),
                    Some(_) => Ok(None),
                })
            },
        );
        Box::pin(async move {
            let result = call.await;
            // A login that swallowed the cancel must not report a failure.
            if cancelled.load(Ordering::SeqCst) {
                return Err(AgentError::Cancelled);
            }
            result
        })
    }

    fn auth_status(
        &self,
        config: Value,
    ) -> Option<BoxFuture<'static, Result<AuthStatus, AgentError>>> {
        self.callbacks.auth_status.as_ref()?;
        Some(self.call_callback(
            |callbacks| callbacks.auth_status.as_ref(),
            move |lua| config_arg(lua, &config),
            |lua, status| Ok(Ok(lua.from_value(status.unwrap_or(LuaValue::Nil))?)),
        ))
    }

    fn logout(&self) -> Option<BoxFuture<'static, Result<(), AgentError>>> {
        self.callbacks.logout.as_ref()?;
        Some(self.call_callback(
            |callbacks| callbacks.logout.as_ref(),
            no_args,
            |_, _| Ok(Ok(())),
        ))
    }
}

/// The provider's `providers.toml` section, read at call time so a login
/// earlier in the session is seen. Serializing a parsed section cannot fail.
fn provider_config(id: &str) -> Value {
    let section = ProvidersConfig::load_or_default()
        .get(id)
        .cloned()
        .unwrap_or_default();
    serde_json::to_value(section).unwrap_or_default()
}

fn config_arg(lua: &Lua, config: &Value) -> mlua::Result<MultiValue> {
    lua.to_value_with(config, serialize_options())?
        .into_lua_multi(lua)
}

async fn prepare_instance(
    lua: &Lua,
    callbacks: &Callbacks,
    info: &ModelInfo,
    config: &Value,
) -> JobResult<(RegistryKey, Option<ModelInfo>, usize)> {
    let prepare: Function = lua.registry_value(&callbacks.prepare)?;
    let values = prepare
        .call_async::<MultiValue>((
            callbacks.ctx(lua)?,
            lua.to_value_with(info, serialize_options())?,
            lua.to_value_with(config, serialize_options())?,
        ))
        .await?;
    let mut values = match returned(values) {
        Ok(values) => values.into_iter(),
        Err(error) => return Ok(Err(error)),
    };
    let Some(LuaValue::Table(instance)) = values.next() else {
        return Ok(Err(config_error("prepare must return an instance table")));
    };
    if instance.get::<Option<Function>>("stream")?.is_none() {
        return Ok(Err(config_error(NO_STREAM)));
    }
    let adjusted = match values.next() {
        None | Some(LuaValue::Nil) => None,
        Some(model) => Some(lua.from_value(model)?),
    };
    let key_count = match instance.get::<LuaValue>("key_count")? {
        LuaValue::Nil => DEFAULT_KEY_COUNT,
        LuaValue::Function(key_count) => key_count.call_async(callbacks.ctx(lua)?).await?,
        key_count => lua.from_value(key_count)?,
    };
    Ok(Ok((
        lua.create_registry_value(instance)?,
        adjusted,
        key_count,
    )))
}

#[derive(Serialize)]
struct RequestModel {
    id: String,
    output_tokens: Option<u32>,
    thinking_ceiling: Option<u32>,
    context_window: u32,
}

#[derive(Serialize)]
struct StreamRequest {
    model: RequestModel,
    system: String,
    messages: Vec<Message>,
    tools: Value,
    #[serde(flatten)]
    options: RequestOptions,
    session_id: Option<SessionRef>,
    invocation: Invocation,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StreamResult {
    message: Message,
    #[serde(default)]
    usage: TokenUsage,
    #[serde(default)]
    stop_reason: Option<StopReason>,
}

/// The `emit` a Lua stream holds. The Rust side closes it when the call
/// ends, so a closure Lua keeps around cannot hold the event channel open.
#[derive(Clone)]
struct Emitter(Arc<Mutex<Option<Sender<ProviderEvent>>>>);

impl Emitter {
    fn send(&self, event: ProviderEvent) -> bool {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .as_ref()
            .is_some_and(|tx| tx.send(event).is_ok())
    }
}

struct CloseOnDrop(Emitter);

impl Drop for CloseOnDrop {
    fn drop(&mut self) {
        self.0
            .0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
    }
}

struct LuaProvider {
    requests: Sender<Request>,
    callbacks: Arc<Callbacks>,
    instance: Arc<RegistryKey>,
    key_count: usize,
}

impl LuaProvider {
    /// Calls instance function {name} with ctx and {args}. {decode} gets the
    /// result, or `None` when the instance has no such function.
    fn call<T: Send + 'static>(
        &self,
        name: &'static str,
        args: impl FnOnce(&Lua) -> mlua::Result<MultiValue> + Send + 'static,
        decode: impl FnOnce(&Lua, Option<LuaValue>) -> JobResult<T> + Send + 'static,
    ) -> impl Future<Output = Result<T, AgentError>> + Send + 'static {
        let requests = self.requests.clone();
        let callbacks = Arc::clone(&self.callbacks);
        let instance = Arc::clone(&self.instance);
        async move {
            invoke(&requests, move |lua| {
                Box::pin(async move {
                    let instance: Table = lua.registry_value(&instance)?;
                    let function = instance.get::<Option<Function>>(name)?;
                    call_with_ctx(&lua, &callbacks, function, args, decode).await
                })
            })
            .await
        }
    }

    fn call_unit(&self, name: &'static str) -> BoxFuture<'_, Result<(), AgentError>> {
        Box::pin(self.call(name, no_args, |_, _| Ok(Ok(()))))
    }
}

fn no_args(_: &Lua) -> mlua::Result<MultiValue> {
    Ok(MultiValue::new())
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
        invocation: &'a Invocation,
    ) -> BoxFuture<'a, Result<StreamResponse, AgentError>> {
        let request = StreamRequest {
            model: RequestModel {
                id: model.id.clone(),
                output_tokens: model.output_tokens(),
                thinking_ceiling: model.thinking_ceiling(),
                context_window: model.context_window,
            },
            system: system.to_owned(),
            messages: messages.to_vec(),
            tools: tools.clone(),
            options: opts,
            session_id: session_id.cloned(),
            invocation: invocation.clone(),
        };
        let emitter = Emitter(Arc::new(Mutex::new(Some(event_tx.clone()))));
        let close = CloseOnDrop(emitter.clone());
        let call = self.call(
            "stream",
            move |lua| {
                let emit = lua.create_function(move |lua, event: LuaValue| {
                    Ok(emitter.send(lua.from_value(event)?))
                })?;
                (lua.to_value_with(&request, serialize_options())?, emit).into_lua_multi(lua)
            },
            |lua, result| {
                let Some(result) = result else {
                    return Ok(Err(config_error(NO_STREAM)));
                };
                let result: StreamResult = lua.from_value(result)?;
                Ok(Ok(StreamResponse {
                    message: result.message,
                    usage: result.usage,
                    stop_reason: result.stop_reason,
                }))
            },
        );
        Box::pin(async move {
            let _close = close;
            call.await
        })
    }

    fn fetch_usage(&self) -> BoxFuture<'_, Result<Option<ProviderUsage>, AgentError>> {
        Box::pin(self.call("fetch_usage", no_args, |lua, usage| match usage {
            None | Some(LuaValue::Nil) => Ok(Ok(None)),
            Some(usage) => Ok(Ok(Some(lua.from_value(usage)?))),
        }))
    }

    fn refresh_auth(&self) -> BoxFuture<'_, Result<(), AgentError>> {
        self.call_unit("refresh_auth")
    }

    fn reload_auth(&self) -> BoxFuture<'_, Result<(), AgentError>> {
        self.call_unit("reload_auth")
    }

    fn key_count(&self) -> usize {
        self.key_count
    }

    fn rotate_key(&self) -> BoxFuture<'_, bool> {
        let call = self.call("rotate_key", no_args, |_, rotated| {
            Ok(Ok(matches!(rotated, Some(LuaValue::Boolean(true)))))
        });
        Box::pin(async move {
            call.await.unwrap_or_else(|error| {
                warn!(provider = %self.callbacks.id, %error, "rotate_key failed");
                false
            })
        })
    }

    fn end_conversation(
        &self,
        conversation_id: &str,
        reason: ConversationEnd,
    ) -> BoxFuture<'_, ()> {
        let conversation_id = conversation_id.to_owned();
        let args_id = conversation_id.clone();
        let call = self.call(
            "end_conversation",
            move |lua| (args_id, reason.to_string()).into_lua_multi(lua),
            |_, _| Ok(Ok(())),
        );
        Box::pin(async move {
            if let Err(error) = call.await {
                warn!(provider = %self.callbacks.id, conversation_id, %error, "end_conversation failed");
            }
        })
    }
}
