use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex, Weak},
};

use makima_agent::{
    AgentId, AgentRef,
    journal::{JournalChange, SessionJournal, SessionJournalSnapshot},
    state::{
        StateAccess, StateChangeEvent, StateChangeReason, StateRevision, StateScope, StateSlot,
        StateSlotSpec,
    },
};
use makima_storage::{
    checkpoint::CheckpointPublication,
    session_state::{SessionStateView, StateChange, StoredStateRecord},
};
use mlua::{Function, Lua, MultiValue, Table, UserData, UserDataMethods, UserDataRef, Value};
use serde_json::Value as JsonValue;

use crate::{
    api::util::{
        convert::{state_value_from_lua, state_value_to_lua},
        ctx::LuaCtx,
        pair::{Pair, err_pair, pair},
    },
    loader::PreparedStateRestore,
    runtime::{deny_delegated_registration, require_plugin_load},
};

const MAX_LUA_REVISION: f64 = 9_007_199_254_740_991.0;
const CALLBACK_WRAPPER: &str = "local callback, event, slot = ...; return function(ctx) if event.reason ~= 'commit' then local _, revision = slot:get(ctx); assert(type(revision) == 'number', revision); event.revision = revision end; return callback(ctx, event) end";

#[derive(Clone, Copy, PartialEq, Eq)]
enum Scope {
    Agent,
    Session,
}

struct Definition {
    plugin: Arc<str>,
    key: String,
    scope: Scope,
    version: u32,
    migrate: Option<Function>,
    callbacks: Mutex<Vec<Function>>,
}

#[derive(Clone)]
struct LuaStateSlot(Arc<Definition>);

#[derive(Clone, Default)]
pub(crate) struct PendingState(Arc<Mutex<BTreeMap<String, Arc<Definition>>>>);

#[derive(Default)]
pub(crate) struct StateStore(BTreeMap<String, Arc<Definition>>);

pub(crate) fn activate(lua: &Lua, plugin: &str, pending: &PendingState) {
    let definitions = pending
        .0
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .clone();
    if let Some(mut store) = lua.app_data_mut::<StateStore>() {
        store
            .0
            .retain(|_, definition| definition.plugin.as_ref() != plugin);
        store.0.extend(definitions);
    }
}

pub(crate) fn unload(lua: &Lua, plugin: &str) {
    if let Some(mut store) = lua.app_data_mut::<StateStore>() {
        store
            .0
            .retain(|_, definition| definition.plugin.as_ref() != plugin);
    }
}

fn validate_slot(lua: &Lua, slot: &LuaStateSlot) -> Result<(), String> {
    let active = lua
        .app_data_ref::<StateStore>()
        .and_then(|store| store.0.get(&slot.0.key).cloned());
    if active.is_some_and(|active| Arc::ptr_eq(&active, &slot.0)) {
        Ok(())
    } else {
        Err("state_slot_expired".into())
    }
}

pub(crate) fn create_state_table(
    lua: &Lua,
    pending: PendingState,
    plugin: Arc<str>,
) -> mlua::Result<Table> {
    let table = lua.create_table()?;
    table.set("null", Value::NULL)?;
    table.set(
        "register",
        lua.create_function(move |lua, (name, spec): (String, Table)| {
            deny_delegated_registration(lua)?;
            require_plugin_load(lua, &plugin, "makima.state.register")?;
            if name.is_empty() || name.contains('/') {
                return Err(mlua::Error::runtime(
                    "state slot name must be nonempty and contain no slash",
                ));
            }
            let key = format!("{plugin}/{name}");
            if key == "plan/state" {
                return Err(mlua::Error::runtime(
                    "state slot plan/state is reserved for the host",
                ));
            }
            for key in spec.clone().pairs::<String, Value>() {
                if !matches!(key?.0.as_str(), "scope" | "version" | "migrate") {
                    return Err(mlua::Error::runtime(
                        "unknown state slot registration field",
                    ));
                }
            }
            let scope = match spec.get::<String>("scope")?.as_str() {
                "agent" => Scope::Agent,
                "session" => Scope::Session,
                _ => return Err(mlua::Error::runtime("state scope must be agent or session")),
            };
            let version = spec.get::<u32>("version")?;
            if version == 0 {
                return Err(mlua::Error::runtime("state slot version must be positive"));
            }
            let definition = Arc::new(Definition {
                plugin: plugin.clone(),
                key: key.clone(),
                scope,
                version,
                migrate: spec.get("migrate")?,
                callbacks: Mutex::default(),
            });
            let mut definitions = pending.0.lock().unwrap_or_else(|error| error.into_inner());
            if definitions.contains_key(&key) {
                return Err(mlua::Error::runtime("state slot already registered"));
            }
            definitions.insert(key, definition.clone());
            Ok(LuaStateSlot(definition))
        })?,
    )?;
    Ok(table)
}

pub(crate) fn prepare_restore(
    lua: &Lua,
    snapshot: &SessionJournalSnapshot,
    affected: &BTreeSet<AgentId>,
    session_scope: bool,
    publication: CheckpointPublication,
) -> Result<PreparedStateRestore, String> {
    let view = SessionStateView::fold(&snapshot.state, &snapshot.agent_revisions, snapshot.seq)
        .map_err(|error| error.to_string())?;
    let definitions = lua
        .app_data_ref::<StateStore>()
        .map(|store| store.0.clone())
        .unwrap_or_default();
    let owners = definitions
        .values()
        .filter(|definition| match definition.scope {
            Scope::Agent => !affected.is_empty(),
            Scope::Session => session_scope,
        })
        .map(|definition| crate::orchestration::plugin_authority(lua, definition.plugin.clone()))
        .collect();
    let mut changes = Vec::new();
    for (agent, scope) in view.scopes {
        if agent.map_or(!session_scope, |agent| !affected.contains(&agent)) {
            continue;
        }
        for (key, stored) in scope.plugins {
            let Some(definition) = definitions.get(&key) else {
                continue;
            };
            if agent.is_some() != (definition.scope == Scope::Agent) {
                return Err(format!(
                    "state_invalid_registration: scope changed for {key}"
                ));
            }
            if stored.version > definition.version {
                return Err(format!(
                    "state_newer_version: stored {}, registered {}",
                    stored.version, definition.version
                ));
            }
            if stored.version == definition.version {
                continue;
            }
            let Some(value) = stored.value else {
                continue;
            };
            let migrate = definition
                .migrate
                .as_ref()
                .ok_or("state_migration_required")?;
            let copied = state_value_to_lua(lua, &value).map_err(|error| error.to_string())?;
            let migrated: Value = migrate
                .call((copied, stored.version))
                .map_err(|error| error.to_string())?;
            let value = state_value_from_lua(lua, &migrated).map_err(|error| error.to_string())?;
            validate_slot(lua, &LuaStateSlot(definition.clone()))?;
            let record = StoredStateRecord {
                seq: snapshot
                    .seq
                    .checked_add(1)
                    .ok_or("state sequence exhausted")?,
                agent,
                generation: scope.generation,
                change: StateChange::Plugin {
                    key,
                    version: definition.version,
                    value: Some(value),
                },
            };
            let encoded = serde_json::to_vec(&record).map_err(|error| error.to_string())?;
            let decoded: StoredStateRecord =
                serde_json::from_slice(&encoded).map_err(|error| error.to_string())?;
            if decoded != record {
                return Err("state value storage round trip is lossy".into());
            }
            SessionStateView::fold(&[decoded], &[], record.seq)
                .map_err(|error| error.to_string())?;
            changes.push(JournalChange {
                agent: record.agent,
                generation: record.generation,
                change: record.change,
            });
        }
    }
    Ok(PreparedStateRestore::new(changes, owners, publication))
}

fn host_slot(
    access: &StateAccess,
    definition: &Definition,
) -> Result<StateSlot<JsonValue>, String> {
    access
        .journal()
        .register_state(StateSlotSpec {
            key: definition.key.clone(),
            scope: match definition.scope {
                Scope::Agent => StateScope::Agent,
                Scope::Session => StateScope::Session,
            },
            version: definition.version,
        })
        .map_err(|error| error.to_string())
}

fn read_value(
    lua: &Lua,
    ctx: &LuaCtx,
    definition: &Definition,
    access: &StateAccess,
) -> Result<(Option<JsonValue>, StateRevision), String> {
    let slot = host_slot(access, definition)?;
    let raw = slot.get_raw(access).map_err(|error| error.to_string())?;
    if raw.version > definition.version {
        return Err(format!(
            "state_newer_version: stored {}, registered {}",
            raw.version, definition.version
        ));
    }
    if raw.version < definition.version
        && let Some(value) = raw.value.as_ref()
    {
        ctx.validate_state_migration(lua, access.target())?;
        let migrate = definition
            .migrate
            .as_ref()
            .ok_or("state_migration_required")?;
        let copied = state_value_to_lua(lua, value).map_err(|error| error.to_string())?;
        let migrated: Value = migrate
            .call((copied, raw.version))
            .map_err(|error| error.to_string())?;
        let value = state_value_from_lua(lua, &migrated).map_err(|error| error.to_string())?;
        let revision = ctx.claim_state_effect(lua, || {
            slot.set(access, value.clone(), Some(raw.revision))
                .map_err(|error| error.to_string())
        })?;
        return Ok((Some(value), revision));
    }
    Ok((raw.value, raw.revision))
}

fn value_result(
    lua: &Lua,
    result: Result<(Option<JsonValue>, StateRevision), String>,
) -> mlua::Result<MultiValue> {
    let converted = result.and_then(|(value, revision)| {
        let value = value
            .as_ref()
            .map(|value| state_value_to_lua(lua, value))
            .transpose()
            .map_err(|error| error.to_string())?;
        Ok((value, revision))
    });
    match converted {
        Ok((value, revision)) => Ok(MultiValue::from_vec(vec![
            value.unwrap_or(Value::Nil),
            Value::Number(revision.0 as f64),
        ])),
        Err(error) => Ok(MultiValue::from_vec(vec![
            Value::Nil,
            Value::String(lua.create_string(error)?),
        ])),
    }
}

fn expected_revision(opts: Option<Table>) -> Result<Option<StateRevision>, String> {
    let Some(opts) = opts else {
        return Ok(None);
    };
    let value = opts
        .raw_get::<Option<f64>>("expect")
        .map_err(|error| error.to_string())?;
    value
        .map(|value| {
            if !value.is_finite() || value < 0.0 || value.fract() != 0.0 || value > MAX_LUA_REVISION
            {
                Err("state revision must be a nonnegative lossless integer".into())
            } else {
                Ok(StateRevision(value as u64))
            }
        })
        .transpose()
}

fn mutate(
    lua: &Lua,
    this: &LuaStateSlot,
    ctx: &LuaCtx,
    value: Option<Value>,
    opts: Option<Table>,
) -> Result<u64, String> {
    validate_slot(lua, this)?;
    ctx.validate_state_write(lua)?;
    let access = ctx.state_access(lua)?;
    observe(lua, &access.journal(), &ctx.state_target(lua)?);
    let slot = host_slot(&access, &this.0)?;
    let value = value
        .as_ref()
        .map(|value| state_value_from_lua(lua, value))
        .transpose()
        .map_err(|error| error.to_string())?;
    let expect = expected_revision(opts)?;
    ctx.claim_state_effect(lua, || {
        match value {
            Some(value) => slot.set(&access, value, expect),
            None => slot.delete(&access, expect),
        }
        .map(|revision| revision.0)
        .map_err(|error| error.to_string())
    })
}

impl UserData for LuaStateSlot {
    fn add_methods<M: UserDataMethods<Self>>(methods: &mut M) {
        methods.add_method("get", |lua, this, ctx: UserDataRef<LuaCtx>| {
            let context = (*ctx).clone();
            drop(ctx);
            let ctx = context;
            let result = (|| {
                validate_slot(lua, this)?;
                let access = ctx.state_access(lua)?;
                observe(lua, &access.journal(), &ctx.state_target(lua)?);
                read_value(lua, &ctx, &this.0, &access)
            })();
            value_result(lua, result)
        });
        methods.add_method(
            "set",
            |lua, this, (ctx, value, opts): (UserDataRef<LuaCtx>, Value, Option<Table>)| {
                Ok(pair(mutate(lua, this, &ctx, Some(value), opts)))
            },
        );
        methods.add_method(
            "delete",
            |lua, this, (ctx, opts): (UserDataRef<LuaCtx>, Option<Table>)| {
                Ok(pair(mutate(lua, this, &ctx, None, opts)))
            },
        );
        methods.add_method(
            "update",
            |lua, this, (ctx, callback): (UserDataRef<LuaCtx>, Function)| {
                let context = (*ctx).clone();
                drop(ctx);
                let ctx = context;
                let result = (|| {
                    validate_slot(lua, this)?;
                    ctx.validate_state_write(lua)?;
                    let access = ctx.state_access(lua)?;
                    observe(lua, &access.journal(), &ctx.state_target(lua)?);
                    let (current, revision) = read_value(lua, &ctx, &this.0, &access)?;
                    let copied = current
                        .as_ref()
                        .map(|value| state_value_to_lua(lua, value))
                        .transpose()
                        .map_err(|error| error.to_string())?
                        .unwrap_or(Value::Nil);
                    let value: Value = callback.call(copied).map_err(|error| error.to_string())?;
                    let value =
                        state_value_from_lua(lua, &value).map_err(|error| error.to_string())?;
                    let slot = host_slot(&access, &this.0)?;
                    let revision = ctx.claim_state_effect(lua, || {
                        slot.set(&access, value.clone(), Some(revision))
                            .map_err(|error| error.to_string())
                    })?;
                    Ok((Some(value), revision))
                })();
                value_result(lua, result)
            },
        );
        methods.add_method(
            "on_change",
            |lua, this, callback: Function| -> mlua::Result<Pair<bool>> {
                deny_delegated_registration(lua)?;
                if !crate::runtime::loading_plugin_is(lua, &this.0.plugin) {
                    if let Err(error) = validate_slot(lua, this) {
                        return Ok(err_pair(error));
                    }
                    crate::runtime::validate_plugin_effect(lua).map_err(mlua::Error::runtime)?;
                    if crate::runtime::current_plugin(lua)
                        .is_none_or(|(plugin, _)| plugin != this.0.plugin)
                    {
                        return Ok(err_pair("state slot belongs to another plugin"));
                    }
                }
                this.0
                    .callbacks
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .push(callback);
                Ok((Some(true), None))
            },
        );
    }
}

#[derive(Default)]
struct ObservedJournals(Vec<(Weak<SessionJournal>, smol::Task<()>)>);

pub(crate) fn observe(lua: &Lua, journal: &Arc<SessionJournal>, target: &AgentRef) {
    if lua.app_data_ref::<ObservedJournals>().is_none() {
        lua.set_app_data(ObservedJournals::default());
    }
    let Some(tx) = lua
        .app_data_ref::<crate::runtime::RequestTx>()
        .map(|tx| tx.0.clone())
    else {
        return;
    };
    {
        let mut observed = lua
            .app_data_mut::<ObservedJournals>()
            .expect("state observers installed");
        observed.0.retain(|(journal, _)| journal.strong_count() > 0);
        if observed
            .0
            .iter()
            .filter_map(|(journal, _)| journal.upgrade())
            .any(|observed| Arc::ptr_eq(&observed, journal))
        {
            return;
        }
    }
    let events = journal.subscribe_state_changes();
    let manager = target.manager().downgrade();
    let agent = target.id();
    let task = smol::spawn(async move {
        while let Ok(event) = events.recv_async().await {
            let Some(manager) = manager.upgrade() else {
                break;
            };
            let request = crate::runtime::Request::StateChanged {
                target: manager.reference(agent),
                event,
            };
            drop(manager);
            if tx.send(request).is_err() {
                break;
            }
        }
    });
    if let Some(mut observed) = lua.app_data_mut::<ObservedJournals>() {
        observed.0.push((Arc::downgrade(journal), task));
    }
}

pub(crate) fn dispatch(lua: &Lua, target: AgentRef, event: StateChangeEvent) -> mlua::Result<()> {
    let definition = lua
        .app_data_ref::<StateStore>()
        .and_then(|store| store.0.get(&event.key).cloned());
    let Some(definition) = definition else {
        return Ok(());
    };
    let manager = target.manager();
    let agent = match event.agent {
        Some(id) => manager.reference(id),
        None => manager.root().map_err(mlua::Error::external)?,
    };
    let services = crate::orchestration::services(lua)
        .ok_or_else(|| mlua::Error::runtime("state notification target unavailable"))?;
    let template = services
        .template(&agent)
        .or_else(|_| {
            manager
                .root()
                .map_err(|error| error.to_string())
                .and_then(|root| services.template(&root))
        })
        .map_err(mlua::Error::runtime)?;
    let target = crate::orchestration::TrustedTarget {
        target: agent.clone(),
        template,
    };
    let data = lua.create_table()?;
    if event.agent.is_some() {
        data.set(
            "agent",
            crate::api::agent::lookup_agent(manager, agent.id(), lua)
                .map_err(mlua::Error::runtime)?,
        )?;
    }
    data.set("revision", event.revision.0)?;
    data.set(
        "reason",
        match event.reason {
            StateChangeReason::Commit => "commit",
            StateChangeReason::Resume => "resume",
            StateChangeReason::Rewind => "rewind",
        },
    )?;
    let callbacks = definition
        .callbacks
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .clone();
    for callback in callbacks {
        let work = lua
            .load(CALLBACK_WRAPPER)
            .set_name("=state_on_change")
            .call::<Function>((callback, data.clone(), LuaStateSlot(definition.clone())))?;
        crate::runtime::enqueue_callback(
            lua,
            work,
            definition.plugin.clone(),
            "StateChanged",
            makima_agent::cancel::CancelToken::none(),
            Some(target.clone()),
        )?;
    }
    Ok(())
}

pub(crate) fn retained(
    lua: &Lua,
    ctx: &LuaCtx,
    access: StateAccess,
    key: &str,
) -> mlua::Result<MultiValue> {
    let definition = lua
        .app_data_ref::<StateStore>()
        .and_then(|store| store.0.get(key).cloned());
    let result = (|| {
        let definition = definition.ok_or("state_slot_unavailable")?;
        if definition.scope != Scope::Agent {
            return Err("state slot is not agent-scoped".into());
        }
        read_value(lua, ctx, &definition, &access)
    })();
    value_result(lua, result)
}

pub(crate) const DOCS: crate::docs::ModuleDoc = crate::docs::ModuleDoc {
    name: "makima.state",
    kind: crate::docs::DocKind::Table,
    desc: "Journaled plugin slots, scoped to an agent or session. Values are copied strict JSON data. Agent-scoped values follow the durable agent ID across session restart. Unloaded plugin values remain stored without running plugin code; old slot handles return state_slot_expired after unload or replacement. StateSlot methods are get(ctx), set(ctx, value, opts?), delete(ctx, opts?), update(ctx, callback), and on_change(callback). get returns value, revision on success, including nil, revision for an absent value; update returns the updated value and revision. Failures return nil, error. set and delete return revision or nil, error. opts.expect requires the observed revision. State writes require a live invocation context; retaining a slot or context does not extend its authority.",
    fns: &[
        crate::docs::FnDoc {
            name: "register",
            args: "{name}, {spec}",
            desc: "Register a plugin-qualified slot during load. spec supplies scope (agent or session), positive version, and optional synchronous migrate(value, old_version). Registration is transactional with plugin load. Names are local to the plugin and contain no slash; plan/state is reserved for the host.",
            params: &[],
            returns: "(StateSlot) Registered slot.",
            example: "",
            guard: None,
        },
        crate::docs::FnDoc {
            name: "null",
            args: "",
            desc: "JSON null sentinel, used as makima.state.null without a call. Unlike nil it remains present in arrays and objects.",
            params: &[],
            returns: "",
            example: "",
            guard: None,
        },
    ],
};

#[cfg(test)]
mod tests {
    use super::observe;
    use crate::runtime::RequestTx;
    use makima_agent::{AgentId, AgentLimits, AgentManagerHandle};
    use mlua::Lua;
    use std::sync::Arc;

    #[test]
    fn state_observer_does_not_keep_manager_or_journal_alive() {
        let lua = Lua::new();
        let (tx, _rx) = flume::unbounded();
        lua.set_app_data(RequestTx(tx));
        let manager = AgentManagerHandle::new(AgentLimits::default()).unwrap();
        let journal = manager.journal();
        let weak_manager = manager.downgrade();
        let weak_journal = Arc::downgrade(&journal);
        let target = manager.reference(AgentId::generate());
        observe(&lua, &journal, &target);
        observe(&lua, &journal, &target);
        drop(target);
        drop(journal);
        drop(manager);
        assert!(weak_manager.upgrade().is_none());
        assert!(weak_journal.upgrade().is_none());
    }
}
