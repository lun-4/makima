use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};

use maki_agent::cancel::{CancelToken, CancelTrigger};
use maki_agent::{AgentId, AgentManagerHandle, AgentRef};
use maki_lua_macro::{lua_fn, lua_table};
use mlua::{Function, Lua, Result as LuaResult, Table, UserData, UserDataRef, Value};

use crate::agent_autocmd::AGENT_CLOSED_EVENT;
use crate::agent_errors::DELEGATED_REGISTRATION;
use crate::api::agent::{LuaAgent, lookup_agent};
use crate::api::util::dispatch::MAX_HOOK_DEPTH;
use crate::api::util::pair::{Pair, err_pair, try_pair};
use crate::orchestration::{self, TrustedTarget};
use crate::runtime::{enqueue_callback, event_depth};

static NEXT_AUTOCMD_ID: AtomicU64 = AtomicU64::new(1);

const WILDCARD_PATTERN: &str = "*";
const CALLBACK_WRAPPER: &str = r#"
local callback, ev, keep = ...
return function(ctx)
    local _ = keep
    return callback(ev, ctx)
end
"#;

struct RevocationKeepalive {
    _revocation: Revocation,
}

impl UserData for RevocationKeepalive {}

/// The one agent an autocmd listens to, named by its graph's generation so
/// the filter does not keep that graph alive.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) struct AgentFilter {
    manager: u64,
    agent: AgentId,
}

impl AgentFilter {
    pub(crate) fn new(manager: &AgentManagerHandle, agent: AgentId) -> Self {
        Self {
            manager: manager.generation(),
            agent,
        }
    }

    fn of(agent: &AgentRef) -> Self {
        Self::new(&agent.manager(), agent.id())
    }
}

#[derive(Clone)]
pub(crate) struct AutocmdEntry {
    pub id: u64,
    pub callback: Function,
    pub plugin: Arc<str>,
    pub once: bool,
    pub patterns: Option<Vec<String>>,
    pub agent: Option<AgentFilter>,
    cancel: CancelToken,
    revocation: Revocation,
}

/// Taken by `del_autocmd` and plugin unload to cancel running callbacks.
/// Each callback holds a clone, so an entry leaving the store on its own
/// (`once`, or its agent closing) never cuts short a callback it started.
type Revocation = Arc<RevocationSlot>;
type RevocationSlot = Mutex<Option<CancelTrigger>>;

fn revoke_now(revocation: &Revocation) {
    revocation
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .take();
}

impl AutocmdEntry {
    pub(crate) fn new(
        id: u64,
        callback: Function,
        plugin: Arc<str>,
        once: bool,
        patterns: Option<Vec<String>>,
        agent: Option<AgentFilter>,
    ) -> Self {
        let (trigger, cancel) = CancelToken::new();
        Self {
            id,
            callback,
            plugin,
            once,
            patterns,
            agent,
            cancel,
            revocation: Arc::new(Mutex::new(Some(trigger))),
        }
    }
}

pub(crate) fn next_autocmd_id() -> u64 {
    NEXT_AUTOCMD_ID.fetch_add(1, Ordering::Relaxed)
}

#[derive(Clone, Default)]
pub(crate) struct AutocmdStore {
    pub(crate) listeners: HashMap<String, Vec<AutocmdEntry>>,
    /// Entries that left on their own while callbacks they started may still
    /// run, by plugin, so unloading the plugin still cancels those callbacks.
    retired: Vec<(Arc<str>, Weak<RevocationSlot>)>,
}

pub(crate) type PendingAutocmdStore = Arc<Mutex<AutocmdStore>>;

impl AutocmdStore {
    pub fn register(&mut self, event: String, entry: AutocmdEntry) {
        self.listeners.entry(event).or_default().push(entry);
    }

    pub fn remove(&mut self, id: u64) {
        self.revoke(|e| e.id == id);
    }

    /// Unload or replacement: also cancels the plugin's running callbacks,
    /// those of entries that already left included.
    pub fn revoke_plugin(&mut self, plugin: &str) {
        self.revoke(|e| e.plugin.as_ref() == plugin);
        self.retired.retain(|(owner, revocation)| {
            if owner.as_ref() != plugin {
                return true;
            }
            if let Some(revocation) = revocation.upgrade() {
                revoke_now(&revocation);
            }
            false
        });
    }

    fn retire(&mut self, entry: &AutocmdEntry) {
        self.retired
            .retain(|(_, revocation)| revocation.strong_count() > 0);
        self.retired
            .push((entry.plugin.clone(), Arc::downgrade(&entry.revocation)));
    }

    /// {agent} closed, so the entries listening to it leave.
    fn retire_agent(&mut self, agent: AgentFilter) {
        let mut retired = Vec::new();
        self.retain(|entry| {
            let keep = entry.agent != Some(agent);
            if !keep {
                retired.push(entry.clone());
            }
            keep
        });
        for entry in &retired {
            self.retire(entry);
        }
    }

    pub fn clear_plugin(&mut self, plugin: &str) {
        self.retain(|e| e.plugin.as_ref() != plugin);
    }

    fn revoke(&mut self, revoked: impl Fn(&AutocmdEntry) -> bool) {
        self.retain(|entry| {
            let keep = !revoked(entry);
            if !keep {
                revoke_now(&entry.revocation);
            }
            keep
        });
    }

    fn retain(&mut self, mut keep: impl FnMut(&AutocmdEntry) -> bool) {
        for entries in self.listeners.values_mut() {
            entries.retain(&mut keep);
        }
        self.listeners.retain(|_, entries| !entries.is_empty());
    }

    pub fn replace_plugin(&mut self, plugin: &str, mut candidate: Self) {
        self.clear_plugin(plugin);
        for (event, entries) in candidate.listeners.drain() {
            self.listeners.entry(event).or_default().extend(
                entries
                    .into_iter()
                    .filter(|entry| entry.plugin.as_ref() == plugin),
            );
        }
    }
}

fn pattern_matches(patterns: Option<&[String]>, fired: Option<&str>) -> bool {
    match patterns {
        None => true,
        Some(ps) => {
            ps.iter().any(|p| p == WILDCARD_PATTERN)
                || fired.is_some_and(|f| ps.iter().any(|p| p == f))
        }
    }
}

/// One dispatch path for host-fired and plugin-fired events. Never throws.
/// Each matching callback runs as its own task, so none of them blocks the
/// event that fired it or the callbacks after it.
///
/// `data` is shared across callbacks (nvim does the same), but each callback
/// gets its own `ev` table, so one plugin's mutation cannot leak into the
/// next.
pub(crate) fn dispatch(
    lua: &Lua,
    event: &str,
    pattern: Option<&str>,
    data: Value,
    agent: Option<&AgentRef>,
) {
    let snapshot = {
        let Some(mut store) = lua.app_data_mut::<AutocmdStore>() else {
            return;
        };
        let snapshot = snapshot(&mut store, event, pattern, agent);
        if let Some(agent) = agent
            && event == AGENT_CLOSED_EVENT
        {
            store.retire_agent(AgentFilter::of(agent));
        }
        snapshot
    };
    if let Err(error) = dispatch_snapshot(lua, event, pattern, data, agent, snapshot) {
        tracing::warn!(event, %error, "autocmd dispatch failed");
    }
}

fn snapshot(
    store: &mut AutocmdStore,
    event: &str,
    pattern: Option<&str>,
    agent: Option<&AgentRef>,
) -> Vec<AutocmdEntry> {
    let Some(entries) = store.listeners.get_mut(event) else {
        return Vec::new();
    };
    let fired_agent = agent.map(AgentFilter::of);
    let mut snapshot = Vec::new();
    // Drop `once` entries now, at snapshot time: if a callback refires
    // the same event they are already gone, so they stay exactly-once.
    entries.retain(|e| {
        let fires = pattern_matches(e.patterns.as_deref(), pattern)
            && e.agent.is_none_or(|filter| Some(filter) == fired_agent);
        if fires {
            snapshot.push(e.clone());
        }
        !(fires && e.once)
    });
    for entry in snapshot.iter().filter(|entry| entry.once) {
        store.retire(entry);
    }
    snapshot
}

/// The trusted target an agent event's callbacks act from: the agent itself
/// while it is live, else pinned to it with its root's template, so a
/// callback for a closed agent can still reach the rest of the graph.
fn event_target(lua: &Lua, agent: &AgentRef) -> Option<TrustedTarget> {
    let services = orchestration::services(lua)?;
    let template = services.template(agent).ok().or_else(|| {
        let root = agent.manager().root().ok()?;
        services.template(&root).ok()
    })?;
    Some(TrustedTarget {
        target: agent.clone(),
        template,
    })
}

fn dispatch_snapshot(
    lua: &Lua,
    event: &str,
    pattern: Option<&str>,
    data: Value,
    agent: Option<&AgentRef>,
    snapshot: Vec<AutocmdEntry>,
) -> LuaResult<()> {
    if snapshot.is_empty() {
        return Ok(());
    }
    if event_depth(lua) >= MAX_HOOK_DEPTH {
        tracing::warn!(
            event,
            "autocmd callbacks fired each other past the depth bound, skipping"
        );
        return Ok(());
    }
    let target = agent.and_then(|agent| event_target(lua, agent));
    let handle = match agent {
        Some(agent) => match lookup_agent(agent.manager(), agent.id(), lua) {
            Ok(handle) => Value::UserData(lua.create_userdata(handle)?),
            Err(_) => Value::Nil,
        },
        None => Value::Nil,
    };
    if let (Some(agent), Value::Table(data)) = (agent, &data) {
        data.set("agent_id", agent.id().to_string())?;
        data.set(
            "parent_id",
            agent
                .snapshot()
                .ok()
                .and_then(|node| node.parent_id)
                .map(|id| id.to_string()),
        )?;
        data.set(
            "session_id",
            target
                .as_ref()
                .and_then(|target| target.template.session_id.as_ref())
                .map(ToString::to_string),
        )?;
    }
    for entry in snapshot {
        let ev = lua.create_table()?;
        ev.set("id", entry.id)?;
        ev.set("event", event)?;
        ev.set("match", pattern)?;
        ev.set("data", data.clone())?;
        ev.set("agent", handle.clone())?;
        let keep = RevocationKeepalive {
            _revocation: entry.revocation,
        };
        let work = lua
            .load(CALLBACK_WRAPPER)
            .set_name("=autocmd_callback")
            .call::<Function>((entry.callback, ev, keep))?;
        enqueue_callback(lua, work, entry.plugin, event, entry.cancel, target.clone())?;
    }
    Ok(())
}

fn parse_string_or_seq(value: Value, what: &str) -> LuaResult<Vec<String>> {
    match value {
        Value::String(s) => Ok(vec![s.to_str()?.to_owned()]),
        Value::Table(t) => t.sequence_values::<String>().collect(),
        _ => Err(mlua::Error::runtime(format!(
            "{what} must be a string or string[]"
        ))),
    }
}

/// Listen for one or more events. Returns an id you can pass to
/// `del_autocmd` later to remove the listener.
///
/// Every callback runs as its own task, called as `callback(ev, ctx)`: it
/// can await, and it never blocks the event that fired it. `ev` is
/// `{ id, event, match, data, agent }`.
///
/// Agent events fire for every agent, children included: `"TurnStart"`,
/// `"TurnEnd"`, `"AgentIdle"`, `"ConfigChanged"`, `"AgentClosed"`,
/// `"ToolStart"`, `"ToolDone"`, `"AutoCompacting"` and `"CompactionDone"`.
/// Their `ev.agent` is the agent's handle (nil once it has left the graph),
/// `ctx` is a trusted context pinned to it, and `ev.data` carries `agent_id`,
/// `session_id` and `parent_id` (nil for a root, so `if ev.data.parent_id then
/// return end` keeps a listener on roots). `"TurnStart"` adds `turn_id`,
/// `origin` and `selection`; `"TurnEnd"` carries the same fields as
/// `AgentTurn:result` plus `reason`, `cost`, `list_cost`, `context_size`,
/// `context_window` and `num_turns`; `"AgentIdle"` fires once work (a turn,
/// a compaction) settled, not after a config change alone, and adds `settled`;
/// `"ConfigChanged"` adds `generation`, `selection` and `mode`;
/// `"AgentClosed"` adds `lifecycle`. Tool events add `tool_id` and `tool`.
/// `"AutoCompacting"` adds `context_size` and `context_window`.
/// `"CompactionDone"` adds `status` (`"completed"`, `"failed"` or
/// `"cancelled"`), and after a compaction inside a turn also
/// `context_size_before`, `context_size_after` and `context_window`; a
/// compaction inside a turn that fails ends the turn, so it reports as a
/// failed `"TurnEnd"`.
///
/// Host events with no agent, where `ctx` is nil: `"SessionReset"`,
/// `"SessionFocusChanged"`, `"ProviderChanged"`, `"SessionPickerRequested"`,
/// `"SplashShown"`, `"SplashHidden"` and `"StoreChanged"`. `"SessionReset"`
/// and `"SessionFocusChanged"` carry `data.session_id`: the session left
/// behind, and the one now focused, which also has `data.previous_session_id`
/// except on initial startup. `"StoreChanged"` carries `data.registry` and,
/// for registrations, `data.key`. Plugins can fire their own events with
/// `exec_autocmds`.
///
/// @param event string|string[] Event name or list of names.
/// @param opts table Options:
///   `callback` (function) called as `callback(ev, ctx)`.
///   `agent` (Agent) only fire for this agent's events.
///   `once` (boolean) remove the handler after it fires once (default false).
///   `pattern` (string|string[]) only fire when the pattern matches. `"*"` matches everything. Omit to match all.
/// @return (integer|nil, string|nil) Autocmd id. Returns nil and an error when called from a delegated tool handler.
/// @example
/// maki.api.create_autocmd("TurnEnd", {
///   callback = function(ev, ctx)
///     if ev.data.parent_id then
///       return
///     end
///     maki.log.info("root turn ended: " .. ev.data.status)
///   end,
/// })
#[lua_fn]
fn create_autocmd(
    lua: &Lua,
    #[ctx] pending: PendingAutocmdStore,
    #[ctx] plugin: Arc<str>,
    event: Value,
    opts: Table,
) -> LuaResult<Pair<u64>> {
    if crate::runtime::in_delegated_tool(lua) {
        return Ok(err_pair(DELEGATED_REGISTRATION));
    }
    try_pair!(crate::runtime::validate_plugin_effect(lua));
    let events = parse_string_or_seq(event, "event")?;
    let patterns = match opts.get::<Value>("pattern")? {
        Value::Nil => None,
        v => Some(parse_string_or_seq(v, "pattern")?),
    };
    let agent = opts
        .get::<Option<UserDataRef<LuaAgent>>>("agent")?
        .map(|agent| agent.listen(lua));
    let entry = AutocmdEntry::new(
        next_autocmd_id(),
        opts.get("callback")?,
        Arc::clone(&plugin),
        opts.get::<Option<bool>>("once")?.unwrap_or(false),
        patterns,
        agent,
    );
    let id = entry.id;
    let register = |store: &mut AutocmdStore| {
        for event in events {
            store.register(event, entry.clone());
        }
    };
    if crate::runtime::loading_plugin_is(lua, &plugin) {
        register(&mut pending.lock().unwrap_or_else(|error| error.into_inner()));
    } else {
        register(
            &mut *lua
                .app_data_mut::<AutocmdStore>()
                .ok_or_else(|| mlua::Error::runtime("autocmd store not initialized"))?,
        );
    }
    Ok((Some(id), None))
}

/// Remove a previously registered autocmd and cancel its running callbacks.
/// Does nothing if the {id} does not exist.
///
/// @param id integer Id returned by `create_autocmd`.
/// @return
/// @example
/// maki.api.del_autocmd(id)
#[lua_fn]
fn del_autocmd(
    lua: &Lua,
    #[ctx] pending: PendingAutocmdStore,
    #[ctx] plugin: Arc<str>,
    id: u64,
) -> LuaResult<()> {
    if crate::runtime::loading_plugin_is(lua, &plugin) {
        pending
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .remove(id);
    } else if let Some(mut store) = lua.app_data_mut::<AutocmdStore>() {
        store.remove(id);
    }
    Ok(())
}

/// Fire one or more events manually. Each matching callback starts as its
/// own task; none has run yet when this function returns.
///
/// @param event string|string[] Event name or list of names to fire.
/// @param opts table? Options:
///   `pattern` (string) passed to callbacks as `ev.match`.
///   `data` (any) arbitrary value passed as `ev.data`.
/// @return
/// @example
/// maki.api.exec_autocmds("MyEvent", {
///   pattern = "init",
///   data = { msg = "hello" },
/// })
#[lua_fn]
fn exec_autocmds(
    lua: &Lua,
    #[ctx] pending: PendingAutocmdStore,
    #[ctx] plugin: Arc<str>,
    event: Value,
    opts: Option<Table>,
) -> LuaResult<()> {
    let events = parse_string_or_seq(event, "event")?;
    let (pattern, data) = match opts {
        Some(opts) => {
            let pattern = match opts.get::<Value>("pattern")? {
                Value::Nil => None,
                Value::String(s) => Some(s.to_str()?.to_owned()),
                _ => return Err(mlua::Error::runtime("pattern must be a string")),
            };
            (pattern, opts.get::<Value>("data")?)
        }
        None => (None, Value::Nil),
    };
    for event in events {
        if crate::runtime::loading_plugin_is(lua, &plugin) {
            let snapshot = {
                let mut store = pending.lock().unwrap_or_else(|error| error.into_inner());
                snapshot(&mut store, &event, pattern.as_deref(), None)
            };
            dispatch_snapshot(
                lua,
                &event,
                pattern.as_deref(),
                data.clone(),
                None,
                snapshot,
            )?;
        } else {
            dispatch(lua, &event, pattern.as_deref(), data.clone(), None);
        }
    }
    Ok(())
}

lua_table! {
    extend "maki.api" => pub(crate) fn add_autocmd_methods(pending: PendingAutocmdStore, plugin: Arc<str>), DOCS [
        create_autocmd(pending, plugin), del_autocmd(pending, plugin), exec_autocmds(pending, plugin),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_case::test_case;

    const PLUGIN: &str = "owner";
    const EVENT: &str = "TurnEnd";

    /// A `once` entry fired, or an agent-filtered one whose agent closed,
    /// leaves the store while its callback still runs; unloading its plugin
    /// still cancels that callback.
    #[test_case(true ; "once_entry")]
    #[test_case(false ; "closed_agent_entry")]
    fn unload_cancels_callbacks_of_entries_that_left(once: bool) {
        let lua = Lua::new();
        let agent = AgentFilter {
            manager: 0,
            agent: AgentId::generate(),
        };
        let entry = AutocmdEntry::new(
            next_autocmd_id(),
            lua.create_function(|_, ()| Ok(())).unwrap(),
            Arc::from(PLUGIN),
            once,
            None,
            (!once).then_some(agent),
        );
        let running = entry.cancel.clone();
        let mut store = AutocmdStore::default();
        store.register(EVENT.into(), entry);
        let started = if once {
            snapshot(&mut store, EVENT, None, None)
        } else {
            let started = store.listeners[EVENT].clone();
            store.retire_agent(agent);
            started
        };
        assert!(store.listeners.values().all(Vec::is_empty));
        store.revoke_plugin(PLUGIN);
        assert!(running.is_cancelled());
        drop(started);
    }

    #[test_case(None, None => true ; "no_patterns_no_fired")]
    #[test_case(None, Some("x") => true ; "no_patterns_with_fired")]
    #[test_case(Some(&["*"]), None => true ; "wildcard_no_fired")]
    #[test_case(Some(&["a", "*"]), Some("z") => true ; "wildcard_among_others")]
    #[test_case(Some(&["a", "b"]), Some("b") => true ; "fired_in_patterns")]
    #[test_case(Some(&["a", "b"]), Some("c") => false ; "fired_not_in_patterns")]
    #[test_case(Some(&["a"]), None => false ; "patterns_but_no_fired")]
    fn match_rule(patterns: Option<&[&str]>, fired: Option<&str>) -> bool {
        let owned = patterns.map(|ps| ps.iter().map(|s| (*s).to_owned()).collect::<Vec<_>>());
        pattern_matches(owned.as_deref(), fired)
    }
}
