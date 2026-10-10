use crate::runtime::{TaskHandle, active_task_id, lock_cell};
use maki_agent::cancel::{CancelToken, CancelTrigger};
use maki_agent::{AgentRef, tools::ToolContext};
use maki_commands::CommandInvocation;
use mlua::{Function, Lua, RegistryKey};
use std::time::Instant;
use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};

const EXPIRED_CONTEXT: &str = "orchestration context expired";
const WRONG_TASK: &str = "orchestration context belongs to another task";
static NEXT_CALLBACK_ID: AtomicU64 = AtomicU64::new(1);
static NEXT_PLUGIN_GENERATION: AtomicU64 = AtomicU64::new(1);

pub(crate) struct PluginAuthority {
    alive: AtomicBool,
    generation: u64,
}

pub struct AgentEventRequest {
    pub callback_id: u64,
    pub payload: serde_json::Value,
    pub target: TrustedTarget,
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
    let plugin = crate::runtime::current_plugin(lua).map_or(plugin, |(plugin, _)| plugin);
    let owner = plugin_authority(lua, plugin.clone());
    let services = lua
        .app_data_ref::<OrchestrationServicesSlot>()
        .and_then(|slot| slot.0.clone());
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
    let current = lua
        .app_data_ref::<OrchestrationServicesSlot>()
        .and_then(|slot| slot.0.clone());
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

    fn plan_path_preparer(&self) -> Option<crate::api::agent::PlanPathPreparer> {
        None
    }
}

#[derive(Clone, Default)]
pub struct OrchestrationServicesSlot(pub Option<Arc<dyn OrchestrationServices>>);

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

pub(crate) fn plugin_authority(lua: &Lua, plugin: Arc<str>) -> Arc<PluginAuthority> {
    if lua.app_data_ref::<PluginAuthorities>().is_none() {
        lua.set_app_data(PluginAuthorities::default());
    }
    lua.app_data_mut::<PluginAuthorities>()
        .expect("authority map installed")
        .0
        .entry(plugin)
        .or_insert_with(|| {
            Arc::new(PluginAuthority {
                alive: AtomicBool::new(true),
                generation: NEXT_PLUGIN_GENERATION.fetch_add(1, Ordering::Relaxed),
            })
        })
        .clone()
}

#[derive(Clone)]
pub(crate) struct TrustedContext {
    pub(crate) target: AgentRef,
    pub(crate) template: ToolContext,
    pub(crate) plugin: Arc<str>,
    pub(crate) generation: u64,
    services: Option<Arc<dyn OrchestrationServices>>,
    event_scope: bool,
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
            event_scope: false,
            services: lua
                .app_data_ref::<OrchestrationServicesSlot>()
                .and_then(|slot| slot.0.clone()),
            authority: ScopeAuthority {
                task: Arc::downgrade(task),
                owner,
            },
        }
    }

    pub(crate) fn for_event(mut self) -> Self {
        self.event_scope = true;
        self
    }

    pub(crate) fn validate(&self, lua: &Lua) -> Result<(), String> {
        self.validate_host(lua)?;
        if self.event_scope {
            return Ok(());
        }
        self.services
            .as_ref()
            .ok_or("no host orchestration services")?
            .validate_target(&self.target)
    }

    pub(crate) fn validate_host(&self, lua: &Lua) -> Result<(), String> {
        self.authority.validate(lua)?;
        let captured = self
            .services
            .as_ref()
            .ok_or("no host orchestration services")?;
        let current = lua
            .app_data_ref::<OrchestrationServicesSlot>()
            .and_then(|slot| slot.0.clone())
            .ok_or("host orchestration services expired")?;
        if !Arc::ptr_eq(captured, &current) {
            return Err("agent target does not belong to this host scope".into());
        }
        Ok(())
    }

    pub(crate) fn validate_target(&self, lua: &Lua, target: &AgentRef) -> Result<(), String> {
        self.validate_host(lua)?;
        self.services
            .as_ref()
            .ok_or("no host orchestration services")?
            .validate_target(target)
    }

    pub(crate) fn services(&self, lua: &Lua) -> Result<&Arc<dyn OrchestrationServices>, String> {
        self.validate(lua)?;
        self.services
            .as_ref()
            .ok_or_else(|| "no host orchestration services".into())
    }
}

impl ScopeAuthority {
    fn validate(&self, lua: &Lua) -> Result<(), String> {
        let task = self.task.upgrade().ok_or(EXPIRED_CONTEXT)?;
        let current_id = active_task_id(lua);
        let cell = lock_cell(&task);
        if !cell.scope_alive
            || cell.cancel.is_cancelled()
            || cell
                .deadline
                .get()
                .is_some_and(|deadline| Instant::now() > deadline)
            || !self.owner.alive.load(Ordering::Acquire)
        {
            return Err(EXPIRED_CONTEXT.into());
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
    let services = lua
        .app_data_ref::<OrchestrationServicesSlot>()
        .and_then(|slot| slot.0.clone());
    let Some(services) = services else {
        return Ok(None);
    };
    let target = services.resolve_target(invocation)?;
    if let Some(target) = &target {
        services.validate_target(&target.target)?;
    }
    Ok(target)
}

#[cfg(test)]
mod tests {
    use super::{
        EXPIRED_CONTEXT, ScopeAuthority, WRONG_TASK, close_event_callback, event_callback,
        plugin_authority, register_event_callback, revoke_plugin,
    };
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
            owner: plugin_authority(&lua, Arc::from("owner")),
        };
        assert!(authority.validate(&lua).is_ok());
        let sibling = TaskScope::detached(&lua);
        assert_eq!(authority.validate(&lua).unwrap_err(), WRONG_TASK);
        drop(sibling);
        assert!(authority.validate(&lua).is_ok());
        revoke_plugin(&lua, "owner");
        assert_eq!(authority.validate(&lua).unwrap_err(), EXPIRED_CONTEXT);
        authority.owner = plugin_authority(&lua, Arc::from("owner"));
        assert!(authority.validate(&lua).is_ok());
        let retained = first.handle().clone();
        drop(first);
        assert_eq!(authority.validate(&lua).unwrap_err(), EXPIRED_CONTEXT);
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
            owner: plugin_authority(&lua, Arc::from("owner")),
        };
        drop(trigger);
        assert_eq!(authority.validate(&lua).unwrap_err(), EXPIRED_CONTEXT);
        let callback = lua.create_function(|_, ()| Ok(())).unwrap();
        let id = register_event_callback(&lua, Arc::from("owner"), callback).unwrap();
        let (_, _, first) = event_callback(&lua, id).unwrap();
        let (_, _, second) = event_callback(&lua, id).unwrap();
        close_event_callback(&lua, id);
        assert!(first.is_cancelled() && second.is_cancelled());
        assert!(event_callback(&lua, id).is_none());
    }
}
