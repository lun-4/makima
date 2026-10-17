use crate::agent_errors::{
    NO_ORCHESTRATION_SERVICES, ORCHESTRATION_CONTEXT_EXPIRED, ORCHESTRATION_SERVICES_EXPIRED,
};
use crate::host_root::HostRootServices;
use crate::runtime::{TaskHandle, active_task_id, lock_cell};
use maki_agent::cancel::{CancelToken, CancelTrigger};
use maki_agent::{AgentRef, tools::ToolContext};
use maki_commands::CommandInvocation;
use mlua::{Function, Lua, RegistryKey};
use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};

const WRONG_TASK: &str = "orchestration context belongs to another task";
static NEXT_CALLBACK_ID: AtomicU64 = AtomicU64::new(1);
static NEXT_PLUGIN_GENERATION: AtomicU64 = AtomicU64::new(1);

pub(crate) struct PluginAuthority {
    alive: AtomicBool,
    generation: u64,
}

impl PluginAuthority {
    pub(crate) fn generation(&self) -> u64 {
        self.generation
    }

    pub(crate) fn is_alive(&self) -> bool {
        self.alive.load(Ordering::Acquire)
    }
}

pub struct AgentEventRequest {
    pub callback_id: u64,
    pub payload: serde_json::Value,
    pub target: TrustedTarget,
    pub lease: maki_agent::actor::ActorEventLease,
}

struct EventCallback {
    function: RegistryKey,
    plugin: Arc<str>,
    owner: Arc<PluginAuthority>,
    services: Option<Arc<dyn OrchestrationServices>>,
    cancel: CancelToken,
    _trigger: CancelTrigger,
}

#[derive(Default)]
struct EventCallbacks(HashMap<u64, EventCallback>);

pub(crate) fn register_event_callback(
    lua: &Lua,
    plugin: Arc<str>,
    function: Function,
) -> mlua::Result<u64> {
    crate::runtime::validate_plugin_effect(lua).map_err(mlua::Error::runtime)?;
    let plugin = crate::runtime::current_plugin(lua).map_or(plugin, |(plugin, _)| plugin);
    let owner = lua
        .app_data_ref::<TaskHandle>()
        .and_then(|task| lock_cell(&task).plugin_owner.clone())
        .unwrap_or_else(|| plugin_authority(lua, plugin.clone()));
    let services = services(lua);
    if lua.app_data_ref::<EventCallbacks>().is_none() {
        lua.set_app_data(EventCallbacks::default());
    }
    let id = NEXT_CALLBACK_ID.fetch_add(1, Ordering::Relaxed);
    let function = lua.create_registry_value(function)?;
    let (trigger, cancel) = CancelToken::new();
    lua.app_data_mut::<EventCallbacks>()
        .expect("callbacks installed")
        .0
        .insert(
            id,
            EventCallback {
                function,
                plugin,
                owner,
                services,
                cancel,
                _trigger: trigger,
            },
        );
    Ok(id)
}

pub(crate) fn close_event_callback(lua: &Lua, id: u64) {
    let callback = lua
        .app_data_mut::<EventCallbacks>()
        .and_then(|mut callbacks| callbacks.0.remove(&id));
    if let Some(callback) = callback {
        lua.remove_registry_value(callback.function).ok();
    }
}

pub(crate) fn event_callback(lua: &Lua, id: u64) -> Option<(Function, Arc<str>, CancelToken)> {
    let callbacks = lua.app_data_ref::<EventCallbacks>()?;
    let callback = callbacks.0.get(&id)?;
    let current = services(lua);
    let same_host = match (&callback.services, &current) {
        (Some(captured), Some(current)) => Arc::ptr_eq(captured, current),
        (None, None) => true,
        _ => false,
    };
    if !same_host || callback.cancel.is_cancelled() || !callback.owner.alive.load(Ordering::Acquire)
    {
        return None;
    }
    Some((
        lua.registry_value(&callback.function).ok()?,
        callback.plugin.clone(),
        callback.cancel.clone(),
    ))
}

#[derive(Clone)]
pub struct TrustedTarget {
    pub target: AgentRef,
    pub template: ToolContext,
}

pub trait OrchestrationServices: Send + Sync + 'static {
    fn resolve_target(
        &self,
        invocation: Option<&CommandInvocation>,
    ) -> Result<Option<TrustedTarget>, String>;

    fn validate_target(&self, target: &AgentRef) -> Result<(), String> {
        self.template(target).map(|_| ())
    }

    fn register_target(&self, _target: TrustedTarget) -> Result<(), String> {
        Err("host target registration is unavailable".into())
    }

    fn template(&self, _target: &AgentRef) -> Result<ToolContext, String> {
        Err("host target template is unavailable".into())
    }

    fn available_models(&self, _target: &AgentRef) -> Result<Vec<String>, String> {
        Err("host model catalog is unavailable".into())
    }
}

#[derive(Clone, Default)]
pub struct OrchestrationServicesSlot(pub Option<Arc<dyn OrchestrationServices>>);

/// The orchestration services the host installed, if any.
pub(crate) fn services(lua: &Lua) -> Option<Arc<dyn OrchestrationServices>> {
    lua.app_data_ref::<OrchestrationServicesSlot>()
        .and_then(|slot| slot.0.clone())
}

#[derive(Clone, Default)]
pub struct HostRootServicesSlot {
    pub services: Option<Arc<HostRootServices>>,
    pub template: Option<ToolContext>,
}

#[derive(Clone)]
pub(crate) struct TrustedHostRootContext {
    pub(crate) services: Arc<HostRootServices>,
    pub(crate) template: Option<ToolContext>,
    pub(crate) plugin: Arc<str>,
    pub(crate) generation: u64,
    authority: ScopeAuthority,
}

impl TrustedHostRootContext {
    pub(crate) fn new(
        lua: &Lua,
        task: &TaskHandle,
        plugin: Arc<str>,
        owner: Arc<PluginAuthority>,
    ) -> Result<Option<Self>, String> {
        let Some(slot) = lua.app_data_ref::<HostRootServicesSlot>() else {
            return Ok(None);
        };
        let Some(services) = slot.services.clone() else {
            return Ok(None);
        };
        services
            .validate(&services.reference())
            .map_err(|error| error.to_string())?;
        Ok(Some(Self {
            services,
            template: slot.template.clone(),
            plugin,
            generation: owner.generation,
            authority: ScopeAuthority {
                task: Arc::downgrade(task),
                owner,
            },
        }))
    }

    pub(crate) fn validate(&self, lua: &Lua) -> Result<(), String> {
        self.authority.validate(lua)?;
        let current = lua
            .app_data_ref::<HostRootServicesSlot>()
            .and_then(|slot| slot.services.clone())
            .ok_or("host root services expired")?;
        if !Arc::ptr_eq(&self.services, &current) {
            return Err("host root context does not belong to this host scope".into());
        }
        self.services
            .validate(&self.services.reference())
            .map_err(|error| error.to_string())
    }
}

#[derive(Default)]
struct PluginAuthorities(HashMap<Arc<str>, Arc<PluginAuthority>>);

pub(crate) fn revoke_plugin(lua: &Lua, plugin: &str) {
    if let Some(mut owners) = lua.app_data_mut::<PluginAuthorities>()
        && let Some(alive) = owners.0.remove(plugin)
    {
        alive.alive.store(false, Ordering::Release);
    }
    crate::api::agent::close_plugin_agents(lua, plugin);
    let removed = if let Some(mut callbacks) = lua.app_data_mut::<EventCallbacks>() {
        let ids: Vec<_> = callbacks
            .0
            .iter()
            .filter(|(_, callback)| callback.plugin.as_ref() == plugin)
            .map(|(id, _)| *id)
            .collect();
        ids.into_iter()
            .filter_map(|id| callbacks.0.remove(&id))
            .collect::<Vec<_>>()
    } else {
        Vec::new()
    };
    for callback in removed {
        lua.remove_registry_value(callback.function).ok();
    }
}

/// The plugin's current authority. A plugin with no committed load, such as
/// one being unloaded, gets an authority that is already revoked, so nothing
/// minted or validated against it can act.
pub(crate) fn plugin_authority(lua: &Lua, plugin: Arc<str>) -> Arc<PluginAuthority> {
    lua.app_data_ref::<PluginAuthorities>()
        .and_then(|owners| owners.0.get(&plugin).cloned())
        .unwrap_or_else(|| {
            Arc::new(PluginAuthority {
                alive: AtomicBool::new(false),
                generation: 0,
            })
        })
}

/// Whether the running code belongs to {plugin} at {generation} and the
/// plugin's live authority is still {owner}.
pub(crate) fn holds_plugin_grant(
    lua: &Lua,
    plugin: &Arc<str>,
    generation: u64,
    owner: &Arc<PluginAuthority>,
) -> bool {
    crate::runtime::current_plugin(lua).is_some_and(|(current, current_generation)| {
        current == *plugin && current_generation == generation
    }) && Arc::ptr_eq(owner, &plugin_authority(lua, plugin.clone()))
}

/// Grants a fresh authority when a plugin load commits; the previous one
/// must already be revoked.
pub(crate) fn install_plugin_authority(lua: &Lua, plugin: Arc<str>) -> Arc<PluginAuthority> {
    if lua.app_data_ref::<PluginAuthorities>().is_none() {
        lua.set_app_data(PluginAuthorities::default());
    }
    let authority = Arc::new(PluginAuthority {
        alive: AtomicBool::new(true),
        generation: NEXT_PLUGIN_GENERATION.fetch_add(1, Ordering::Relaxed),
    });
    lua.app_data_mut::<PluginAuthorities>()
        .expect("authority map installed")
        .0
        .insert(plugin, Arc::clone(&authority));
    authority
}

#[derive(Clone)]
pub(crate) struct TrustedContext {
    pub(crate) target: AgentRef,
    pub(crate) template: ToolContext,
    pub(crate) plugin: Arc<str>,
    pub(crate) generation: u64,
    services: Option<Arc<dyn OrchestrationServices>>,
    authority: ScopeAuthority,
}

#[derive(Clone)]
struct ScopeAuthority {
    task: Weak<Mutex<crate::runtime::TaskCell>>,
    owner: Arc<PluginAuthority>,
}

impl TrustedContext {
    pub(crate) fn new(
        lua: &Lua,
        target: TrustedTarget,
        task: &TaskHandle,
        plugin: Arc<str>,
        owner: Arc<PluginAuthority>,
    ) -> Self {
        Self {
            target: target.target,
            template: target.template,
            plugin,
            generation: owner.generation,
            services: services(lua),
            authority: ScopeAuthority {
                task: Arc::downgrade(task),
                owner,
            },
        }
    }

    /// Checks plugin and host authority only. The pinned target may have
    /// closed since; operations that address it check it themselves.
    pub(crate) fn validate(&self, lua: &Lua) -> Result<(), String> {
        self.authority.validate(lua)?;
        let captured = self.services.as_ref().ok_or(NO_ORCHESTRATION_SERVICES)?;
        let current = services(lua).ok_or(ORCHESTRATION_SERVICES_EXPIRED)?;
        if !Arc::ptr_eq(captured, &current) {
            return Err("agent target does not belong to this host scope".into());
        }
        Ok(())
    }

    pub(crate) fn validate_target(&self, lua: &Lua, target: &AgentRef) -> Result<(), String> {
        self.validate(lua)?;
        self.services
            .as_ref()
            .ok_or(NO_ORCHESTRATION_SERVICES)?
            .validate_target(target)
    }

    pub(crate) fn services(&self, lua: &Lua) -> Result<&Arc<dyn OrchestrationServices>, String> {
        self.validate(lua)?;
        self.services
            .as_ref()
            .ok_or_else(|| NO_ORCHESTRATION_SERVICES.into())
    }
}

impl ScopeAuthority {
    fn validate(&self, lua: &Lua) -> Result<(), String> {
        let task = self.task.upgrade().ok_or(ORCHESTRATION_CONTEXT_EXPIRED)?;
        let current_id = active_task_id(lua);
        let cell = lock_cell(&task);
        if !cell.is_live() || !self.owner.alive.load(Ordering::Acquire) {
            return Err(ORCHESTRATION_CONTEXT_EXPIRED.into());
        }
        if current_id != Some(cell.id) {
            return Err(WRONG_TASK.into());
        }
        Ok(())
    }
}

pub(crate) fn resolve_target(
    lua: &Lua,
    invocation: Option<&CommandInvocation>,
) -> Result<Option<TrustedTarget>, String> {
    let services = services(lua);
    let Some(services) = services else {
        return Ok(None);
    };
    let target = services.resolve_target(invocation)?;
    if let Some(target) = &target {
        services.validate_target(&target.target)?;
    }
    Ok(target)
}

/// Revalidates a target the host pinned before the request reached Lua; a
/// target that closed in between resolves to none rather than to focus.
pub(crate) fn validate_pinned_target(
    lua: &Lua,
    target: Option<TrustedTarget>,
) -> Result<Option<TrustedTarget>, String> {
    let Some(target) = target.filter(|target| target.target.validate_live().is_ok()) else {
        return Ok(None);
    };
    services(lua)
        .ok_or(NO_ORCHESTRATION_SERVICES)?
        .validate_target(&target.target)?;
    Ok(Some(target))
}

#[cfg(test)]
mod tests {
    use super::{
        ScopeAuthority, WRONG_TASK, close_event_callback, event_callback, install_plugin_authority,
        plugin_authority, register_event_callback, revoke_plugin,
    };
    use crate::agent_errors::ORCHESTRATION_CONTEXT_EXPIRED;
    use crate::runtime::{TaskCell, TaskScope};
    use maki_agent::cancel::CancelToken;
    use mlua::Lua;
    use std::sync::Arc;

    #[test]
    fn authority_rejects_sibling_scope_reload_and_scope_exit() {
        let lua = Lua::new();
        let first = TaskScope::detached(&lua);
        let mut authority = ScopeAuthority {
            task: Arc::downgrade(first.handle()),
            owner: install_plugin_authority(&lua, Arc::from("owner")),
        };
        assert!(authority.validate(&lua).is_ok());
        let sibling = TaskScope::detached(&lua);
        assert_eq!(authority.validate(&lua).unwrap_err(), WRONG_TASK);
        drop(sibling);
        assert!(authority.validate(&lua).is_ok());
        revoke_plugin(&lua, "owner");
        assert_eq!(
            authority.validate(&lua).unwrap_err(),
            ORCHESTRATION_CONTEXT_EXPIRED
        );
        assert!(!plugin_authority(&lua, Arc::from("owner")).is_alive());
        authority.owner = install_plugin_authority(&lua, Arc::from("owner"));
        assert!(authority.validate(&lua).is_ok());
        let retained = first.handle().clone();
        drop(first);
        assert_eq!(
            authority.validate(&lua).unwrap_err(),
            ORCHESTRATION_CONTEXT_EXPIRED
        );
        drop(retained);
    }

    #[test]
    fn callback_cannot_cross_services_installation() {
        struct Services;
        impl super::OrchestrationServices for Services {
            fn resolve_target(
                &self,
                _: Option<&maki_commands::CommandInvocation>,
            ) -> Result<Option<super::TrustedTarget>, String> {
                Ok(None)
            }
        }
        let lua = Lua::new();
        let callback = lua.create_function(|_, ()| Ok(())).unwrap();
        lua.set_app_data(super::OrchestrationServicesSlot(Some(Arc::new(Services))));
        install_plugin_authority(&lua, Arc::from("owner"));
        let id = register_event_callback(&lua, Arc::from("owner"), callback).unwrap();
        assert!(event_callback(&lua, id).is_some());
        lua.set_app_data(super::OrchestrationServicesSlot(Some(Arc::new(Services))));
        assert!(event_callback(&lua, id).is_none());
        close_event_callback(&lua, id);
    }

    #[test]
    fn cancellation_expires_scope_and_subscription_close_cancels_all_calls() {
        let lua = Lua::new();
        let (trigger, cancel) = CancelToken::new();
        let scope = TaskScope::new(&lua, TaskCell::new(cancel, None, None));
        let authority = ScopeAuthority {
            task: Arc::downgrade(scope.handle()),
            owner: install_plugin_authority(&lua, Arc::from("owner")),
        };
        drop(trigger);
        assert_eq!(
            authority.validate(&lua).unwrap_err(),
            ORCHESTRATION_CONTEXT_EXPIRED
        );
        drop(scope);
        let callback = lua.create_function(|_, ()| Ok(())).unwrap();
        let id = register_event_callback(&lua, Arc::from("owner"), callback).unwrap();
        let (_, _, first) = event_callback(&lua, id).unwrap();
        let (_, _, second) = event_callback(&lua, id).unwrap();
        close_event_callback(&lua, id);
        assert!(first.is_cancelled() && second.is_cancelled());
        assert!(event_callback(&lua, id).is_none());
    }
}
