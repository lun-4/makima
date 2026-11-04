//! Providers registered from Lua, behind `ProviderBackend` and `Provider`.
//! Every call runs as a job on the Lua thread; nothing here touches Lua from
//! any other thread.

use std::future::Future;
use std::io;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, SystemTime};

use flume::Sender;
use futures::future::LocalBoxFuture;
use maki_agent::cancel::CancelToken;
use maki_config::providers::{self, ProvidersConfig};
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

use crate::api::net::send::{CONNECT_FAILED_ERR, IDLE_TIMEOUT_ERR};
use crate::api::provider::{ProviderSpec, ProviderStore};
use crate::api::util::memory::unlimited;
use crate::credentials::Credentials;
use crate::login_ui::LoginUi;
use crate::runtime::{Request, strip_traceback};

pub(crate) const HOST_STOPPED: &str = "plugin host stopped";
const NO_STREAM: &str = "provider instance has no stream function";
const DEFAULT_KEY_COUNT: usize = 1;
const LOGIN_INCOMPLETE: &str = "login did not complete";
const CONFIG_KIND: &str = "config";
const SEND_WITH_DEFAULTS: &str = r#"
    local send, defaults = ...
    return function(request)
        local filled = table.clone(request)
        for field, value in defaults do
            if filled[field] == nil then
                filled[field] = value
            end
        end
        return send(filled)
    end
"#;

pub(crate) type ProviderJob = Box<dyn FnOnce(Lua) -> LocalBoxFuture<'static, ()> + Send>;
/// The outer error is raised Lua; the inner one is what the provider returned.
type JobResult<T> = mlua::Result<Result<T, AgentError>>;

pub(crate) fn build_registry(
    lua: &Lua,
    requests: &Sender<Request>,
    timeouts: Timeouts,
) -> ProviderRegistry {
    let configs = Arc::new(ProviderConfigs::default());
    let providers = lua
        .app_data_ref::<ProviderStore>()
        .map(|store| {
            store
                .specs()
                .iter()
                .filter_map(|spec| match registered(lua, requests, spec, timeouts, &configs) {
                    Ok(provider) => Some(provider),
                    Err(error) => {
                        warn!(provider = %spec.id, plugin = %spec.plugin, %error, "provider left out of registry");
                        None
                    }
                })
                .collect()
        })
        .unwrap_or_default();
    ProviderRegistry::new(providers)
}

fn registered(
    lua: &Lua,
    requests: &Sender<Request>,
    spec: &ProviderSpec,
    timeouts: Timeouts,
    configs: &Arc<ProviderConfigs>,
) -> mlua::Result<RegisteredProvider> {
    let callbacks = Callbacks {
        id: Arc::clone(&spec.id),
        configs: Arc::clone(configs),
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
        send: lua.create_registry_value(send_with_defaults(lua, &spec.send, timeouts)?)?,
    };
    Ok(RegisteredProvider {
        id: Arc::clone(&spec.id),
        display_name: Arc::clone(&spec.display_name),
        models: spec.models.clone(),
        auth: spec.auth.clone(),
        docs: spec.docs.clone(),
        backend: Arc::new(LuaBackend {
            requests: requests.clone(),
            callbacks: Arc::new(callbacks),
        }),
    })
}

/// {send} with the `[provider]` timeouts filled in where a request leaves
/// them out.
fn send_with_defaults(lua: &Lua, send: &Function, timeouts: Timeouts) -> mlua::Result<Function> {
    let defaults = lua.create_table()?;
    defaults.set("connect_timeout", timeouts.connect.as_secs_f64())?;
    defaults.set("low_speed_timeout", timeouts.low_speed.as_secs_f64())?;
    defaults.set("idle_timeout", timeouts.stream.as_secs_f64())?;
    lua.load(SEND_WITH_DEFAULTS)
        .set_name("=provider send")
        .call((send.clone(), defaults))
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
    let fields = match error {
        LuaValue::Table(fields) => fields,
        LuaValue::String(message) => return transport_error(message.to_string_lossy()),
        other => return AgentError::Io(io::Error::other(other.type_name())),
    };
    let message = fields
        .get::<Option<String>>("message")
        .ok()
        .flatten()
        .unwrap_or_default();
    let kind = fields.get::<Option<String>>("kind").ok().flatten();
    if kind.as_deref() == Some(CONFIG_KIND) {
        return config_error(message);
    }
    match fields.get::<Option<u16>>("status").ok().flatten() {
        Some(status) => AgentError::Api {
            status,
            message,
            retry_after: fields
                .get::<Option<f64>>("retry_after")
                .ok()
                .flatten()
                .and_then(|secs| Duration::try_from_secs_f64(secs).ok()),
            overflow: fields
                .get::<Option<bool>>("overflow")
                .ok()
                .flatten()
                .unwrap_or_default(),
        },
        None => transport_error(message),
    }
}

/// A failure with no status, classified by the `maki.net.send` error it
/// carries, so a refused connection and a stalled stream draw from their
/// bounded retry budgets. Wrapping the error in more text keeps its class.
fn transport_error(message: String) -> AgentError {
    if message.contains(CONNECT_FAILED_ERR) {
        AgentError::Io(io::Error::new(io::ErrorKind::ConnectionRefused, message))
    } else if message.contains(IDLE_TIMEOUT_ERR) {
        AgentError::Timeout { message }
    } else {
        AgentError::Io(io::Error::other(message))
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
    configs: Arc<ProviderConfigs>,
    prepare: RegistryKey,
    discover: Option<RegistryKey>,
    logins: Vec<(String, RegistryKey)>,
    auth_status: Option<RegistryKey>,
    logout: Option<RegistryKey>,
    send: RegistryKey,
}

impl Callbacks {
    fn config(&self) -> Value {
        self.configs.section(&self.id)
    }

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

#[derive(Clone)]
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
            let config = callbacks.config();
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
                    key_count: AtomicUsize::new(key_count),
                }),
            })
        })
    }

    fn discover(
        &self,
        refresh: bool,
    ) -> Option<BoxFuture<'static, Result<Vec<ModelInfo>, AgentError>>> {
        self.callbacks.discover.as_ref()?;
        let backend = self.clone();
        Some(Box::pin(async move {
            let config = backend.callbacks.config();
            backend
                .call_callback(
                    |callbacks| callbacks.discover.as_ref(),
                    move |lua| {
                        (lua.to_value_with(&config, serialize_options())?, refresh)
                            .into_lua_multi(lua)
                    },
                    |lua, models| Ok(Ok(lua.from_value(models.unwrap_or(LuaValue::Nil))?)),
                )
                .await
        }))
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

type ConfigStamp = Option<(SystemTime, u64)>;

/// `providers.toml` as of its last change, shared by a registry's providers
/// so a discovery round across all of them parses it once. A login or a hand
/// edit changes the file, and the next read sees it.
#[derive(Default)]
pub(crate) struct ProviderConfigs(Mutex<Option<(ConfigStamp, Arc<ProvidersConfig>)>>);

impl ProviderConfigs {
    fn current(&self) -> Arc<ProvidersConfig> {
        let stamp = providers::file_stamp();
        let mut cached = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some((cached_stamp, config)) = &*cached
            && *cached_stamp == stamp
        {
            return Arc::clone(config);
        }
        let config = Arc::new(ProvidersConfig::load_or_default());
        *cached = Some((stamp, Arc::clone(&config)));
        config
    }

    /// {id}'s section. Serializing a parsed section cannot fail.
    fn section(&self, id: &str) -> Value {
        let section = self.current().get(id).cloned().unwrap_or_default();
        serde_json::to_value(section).unwrap_or_default()
    }
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
    let key_count = instance_key_count(lua, callbacks, &instance).await?;
    Ok(Ok((
        lua.create_registry_value(instance)?,
        adjusted,
        key_count,
    )))
}

async fn instance_key_count(
    lua: &Lua,
    callbacks: &Callbacks,
    instance: &Table,
) -> mlua::Result<usize> {
    match instance.get::<LuaValue>("key_count")? {
        LuaValue::Nil => Ok(DEFAULT_KEY_COUNT),
        LuaValue::Function(key_count) => key_count.call_async(callbacks.ctx(lua)?).await,
        key_count => lua.from_value(key_count),
    }
}

#[derive(Serialize)]
struct RequestModel {
    id: String,
    max_output_tokens: Option<u32>,
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
    /// Read at prepare and again after each auth reload or refresh, which
    /// may change the key pool.
    key_count: AtomicUsize,
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

    /// Calls auth function {name}, then rereads the key count it may have
    /// changed.
    fn call_auth(&self, name: &'static str) -> BoxFuture<'_, Result<(), AgentError>> {
        Box::pin(async move {
            self.call(name, no_args, |_, _| Ok(Ok(()))).await?;
            let callbacks = Arc::clone(&self.callbacks);
            let instance = Arc::clone(&self.instance);
            let key_count = invoke(&self.requests, move |lua| {
                Box::pin(async move {
                    let instance: Table = lua.registry_value(&instance)?;
                    Ok(Ok(instance_key_count(&lua, &callbacks, &instance).await?))
                })
            })
            .await?;
            self.key_count.store(key_count, Ordering::Relaxed);
            Ok(())
        })
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
                max_output_tokens: model.max_output_tokens,
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
                let request = unlimited(lua, || lua.to_value_with(&request, serialize_options()))?;
                (request, emit).into_lua_multi(lua)
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
        self.call_auth("refresh_auth")
    }

    fn reload_auth(&self) -> BoxFuture<'_, Result<(), AgentError>> {
        self.call_auth("reload_auth")
    }

    fn key_count(&self) -> usize {
        self.key_count.load(Ordering::Relaxed)
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

#[cfg(test)]
mod tests {
    use std::fs;

    use serde_json::json;
    use tempfile::TempDir;

    use super::*;

    const PROVIDER: &str = "local";
    const FIRST_URL: &str = "http://first.test";
    const EDITED_URL: &str = "http://edited.example.test";

    fn write_section(dir: &TempDir, base_url: &str) {
        let file = format!("[{PROVIDER}]\nbase_url = \"{base_url}\"\n");
        fs::write(dir.path().join("providers.toml"), file).unwrap();
    }

    #[test]
    fn provider_configs_parse_again_only_when_the_file_changes() {
        let dir = TempDir::new().unwrap();
        maki_storage::paths::init_at(dir.path().to_path_buf()).unwrap();
        let configs = ProviderConfigs::default();
        assert_eq!(configs.section(PROVIDER), json!({}));

        write_section(&dir, FIRST_URL);
        assert_eq!(configs.section(PROVIDER), json!({ "base_url": FIRST_URL }));
        assert!(Arc::ptr_eq(&configs.current(), &configs.current()));

        write_section(&dir, EDITED_URL);
        assert_eq!(configs.section(PROVIDER), json!({ "base_url": EDITED_URL }));
    }
}
