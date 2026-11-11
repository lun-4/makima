//! `makima.Ctx`: a plugin's authority. Each entry chunk receives its root ctx
//! as `...` and passes it on by reference; plugins narrow it with
//! `with_name`, and only the host extends it, as when it derives a provider
//! ctx for a provider's callbacks.

use std::env;
use std::sync::Arc;
use std::time::Duration;

use makima_lua_macro::{lua_class, lua_fn};
use makima_providers::Timeouts;
use makima_storage::auth::PLUGIN_STORES_DIR;
use mlua::{Function, Lua, Result as LuaResult, UserDataFields};
use tracing::warn;

use crate::api::provider::valid_id;
use crate::credentials::Store;

pub(crate) const NET_TIMEOUT_ENV: &str = "MAKIMA_NET_TIMEOUT_SECS";
const DEFAULT_NET_TIMEOUT_SECS: u64 = 30;
const BAD_NAME_ERR: &str =
    "must be non-empty ASCII letters, digits, '-', '_' or '.', and not start with '.'";
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

/// The part of a ctx every variant has: where its plugin stores live and
/// how it reaches the network.
#[derive(Clone)]
pub(crate) struct PluginCtx {
    namespace: Arc<[Arc<str>]>,
    /// The owning plugin's `makima.net.send`, which the host wraps with the
    /// defaults each derived `send` needs.
    net_send: Function,
    send: Function,
}

impl PluginCtx {
    /// The root ctx of a plugin whose stores live under {name}. Its `send` is
    /// {net_send} with `MAKIMA_NET_TIMEOUT_SECS` defaults, because config
    /// does not exist yet while the plugin loads.
    pub(crate) fn root(lua: &Lua, name: Arc<str>, net_send: Function) -> LuaResult<Self> {
        let timeout = net_timeout(env::var(NET_TIMEOUT_ENV).ok().as_deref());
        let send = with_timeouts(lua, &net_send, timeout, timeout, timeout)?;
        Ok(Self {
            namespace: Arc::from([name]),
            net_send,
            send,
        })
    }

    fn child(&self, name: &str) -> Result<Self, String> {
        if !valid_id(name) {
            return Err(format!("with_name: name {name:?} {BAD_NAME_ERR}"));
        }
        let namespace = self
            .namespace
            .iter()
            .cloned()
            .chain([Arc::from(name)])
            .collect();
        Ok(Self {
            namespace,
            ..self.clone()
        })
    }

    /// The name under `auth/` of this ctx's store {key}.
    pub(crate) fn store_name(&self, key: &str) -> Result<Arc<str>, String> {
        if !valid_id(key) {
            return Err(format!("store key {key:?} {BAD_NAME_ERR}"));
        }
        let segments: Vec<&str> = [PLUGIN_STORES_DIR]
            .into_iter()
            .chain(self.namespace.iter().map(|name| &**name))
            .chain([key])
            .collect();
        Ok(segments.join("/").into())
    }

    /// `send` for a provider registered under this ctx: the `[provider]`
    /// {timeouts} as defaults.
    pub(crate) fn provider_send(&self, lua: &Lua, timeouts: &Timeouts) -> LuaResult<Function> {
        with_timeouts(
            lua,
            &self.net_send,
            timeouts.connect,
            timeouts.low_speed,
            timeouts.stream,
        )
    }
}

pub(crate) enum Ctx {
    Plugin(PluginCtx),
    Provider {
        plugin: PluginCtx,
        id: Arc<str>,
        send: Function,
        /// Only on a `stream` call's ctx: the host extends that one ctx so the
        /// provider can deliver its request.
        mark_request_delivery: Option<Function>,
    },
}

impl Ctx {
    pub(crate) fn plugin(&self) -> &PluginCtx {
        match self {
            Self::Plugin(plugin) | Self::Provider { plugin, .. } => plugin,
        }
    }
}

/// The timeout {raw}, the value of `MAKIMA_NET_TIMEOUT_SECS`, names, or the
/// default when it is unset or not a positive whole number of seconds.
fn net_timeout(raw: Option<&str>) -> Duration {
    let Some(raw) = raw else {
        return Duration::from_secs(DEFAULT_NET_TIMEOUT_SECS);
    };
    match raw.trim().parse::<u64>() {
        Ok(secs) if secs > 0 => Duration::from_secs(secs),
        _ => {
            warn!(
                env = NET_TIMEOUT_ENV,
                value = %raw,
                default_secs = DEFAULT_NET_TIMEOUT_SECS,
                "invalid network timeout, using the default"
            );
            Duration::from_secs(DEFAULT_NET_TIMEOUT_SECS)
        }
    }
}

fn with_timeouts(
    lua: &Lua,
    send: &Function,
    connect: Duration,
    low_speed: Duration,
    idle: Duration,
) -> LuaResult<Function> {
    let defaults = lua.create_table()?;
    defaults.set("connect_timeout", connect.as_secs_f64())?;
    defaults.set("low_speed_timeout", low_speed.as_secs_f64())?;
    defaults.set("idle_timeout", idle.as_secs_f64())?;
    lua.load(SEND_WITH_DEFAULTS)
        .set_name("=ctx send")
        .call((send.clone(), defaults))
}

/// A child ctx whose plugin stores live under {name}, inside this ctx's.
/// It reaches neither this ctx's stores nor a sibling's. A provider ctx's
/// child is a plain plugin ctx.
///
/// @param name string ASCII letters, digits, `-`, `_` or `.`, not starting with `.`.
/// @return (makima.Ctx) The child ctx.
/// @example
/// local ctx = ...
/// require("fountain.provider").setup(ctx:with_name("fountain"))
#[lua_fn]
fn with_name(_lua: &Lua, this: &Ctx, name: String) -> LuaResult<Ctx> {
    this.plugin()
        .child(&name)
        .map(Ctx::Plugin)
        .map_err(mlua::Error::runtime)
}

fn ctx_fields<F: UserDataFields<Ctx>>(fields: &mut F) {
    fields.add_field_method_get("send", |_, this| {
        Ok(match this {
            Ctx::Plugin(plugin) => plugin.send.clone(),
            Ctx::Provider { send, .. } => send.clone(),
        })
    });
    fields.add_field_method_get("id", |_, this| {
        Ok(match this {
            Ctx::Plugin(_) => None,
            Ctx::Provider { id, .. } => Some(id.to_string()),
        })
    });
    fields.add_field_method_get("mark_request_delivery", |_, this| {
        Ok(match this {
            Ctx::Plugin(_) => None,
            Ctx::Provider {
                mark_request_delivery,
                ..
            } => mark_request_delivery.clone(),
        })
    });
    fields.add_field_method_get("credentials", |_, this| {
        Ok(match this {
            Ctx::Plugin(_) => None,
            Ctx::Provider { id, .. } => Some(Store::new(Arc::clone(id))),
        })
    });
}

lua_class! {
    /// A plugin's authority. An entry chunk (a bundled plugin, or `init.lua`)
    /// receives its root ctx as `...`; pass it on by reference, such as to
    /// a module's `setup(ctx)`, and narrow it with `with_name`. APIs that
    /// act for the plugin take it, such as `makima.credentials.open` and
    /// `makima.api.register_provider`. Its fields cannot be assigned.
    ///
    /// `send` is `makima.net.send` with default timeouts: from
    /// `MAKIMA_NET_TIMEOUT_SECS` (default 30 seconds) on a plugin ctx, from
    /// `[provider]` on a provider ctx. A provider ctx, which a provider's
    /// callbacks receive, also has `id` (the provider id) and `credentials`
    /// (the provider's `makima.credentials.Store`); a `stream` call's ctx also
    /// has `mark_request_delivery`, described on `makima.api.register_provider`.
    "makima.Ctx" => Ctx, CTX_DOCS [with_name] fields ctx_fields
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use test_case::test_case;

    use super::{DEFAULT_NET_TIMEOUT_SECS, net_timeout};

    #[test_case(None, DEFAULT_NET_TIMEOUT_SECS ; "unset")]
    #[test_case(Some("5"), 5 ; "whole_seconds")]
    #[test_case(Some(" 12 "), 12 ; "padded")]
    #[test_case(Some("0"), DEFAULT_NET_TIMEOUT_SECS ; "zero")]
    #[test_case(Some("1.5"), DEFAULT_NET_TIMEOUT_SECS ; "fraction")]
    #[test_case(Some("soon"), DEFAULT_NET_TIMEOUT_SECS ; "not_a_number")]
    fn net_timeout_reads_whole_positive_seconds(raw: Option<&str>, expected_secs: u64) {
        assert_eq!(net_timeout(raw), Duration::from_secs(expected_secs));
    }
}
