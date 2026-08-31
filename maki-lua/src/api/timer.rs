use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use flume::{Receiver, Sender};
use maki_agent::cancel::CancelToken;
use maki_lua_macro::{lua_fn, lua_table};
use mlua::{Function, Lua, RegistryKey, Result as LuaResult};

use crate::runtime::{ASYNC_RUN_DEFAULT_DEADLINE, MAX_LUA_SECONDS, PendingAsyncTask};

const ERR_SECONDS_RANGE: &str = "seconds must be a finite number > 0 and < 1e9";

/// One recurring schedule. The callback is pinned once at registration
/// (`key`) and referenced by `callback`; each fire re-pins it under a fresh
/// key that the fire task owns and removes on completion.
pub(crate) struct TimerEntry {
    pub(crate) plugin: Arc<str>,
    pub(crate) key: RegistryKey,
    pub(crate) callback: Function,
    pub(crate) interval: Duration,
    pub(crate) next_fire: Instant,
}

/// Recurring schedules owned by the dispatcher loop's timer arm.
pub(crate) struct TimerStore {
    pub(crate) entries: HashMap<u64, TimerEntry>,
    next_id: u64,
    /// `set` pokes this so the pump wakes early when a newly registered
    /// deadline is earlier than the one it is already sleeping on.
    wake_tx: Sender<()>,
    pub(crate) wake_rx: Receiver<()>,
}

pub(crate) type PendingTimerStore = Arc<Mutex<TimerStore>>;

impl TimerStore {
    pub fn new() -> Self {
        let (wake_tx, wake_rx) = flume::bounded(1);
        Self {
            entries: HashMap::new(),
            next_id: 0,
            wake_tx,
            wake_rx,
        }
    }

    pub fn candidate(lua: &Lua, plugin: &str) -> LuaResult<Self> {
        let live = lua
            .app_data_ref::<TimerStore>()
            .ok_or_else(|| mlua::Error::runtime("timer store not initialized"))?;
        let (wake_tx, wake_rx) = flume::bounded(1);
        let mut candidate = Self {
            entries: HashMap::new(),
            next_id: live.next_id,
            wake_tx,
            wake_rx,
        };
        for (&id, entry) in &live.entries {
            if entry.plugin.as_ref() == plugin {
                continue;
            }
            candidate.entries.insert(
                id,
                TimerEntry {
                    plugin: Arc::clone(&entry.plugin),
                    key: lua.create_registry_value(entry.callback.clone())?,
                    callback: entry.callback.clone(),
                    interval: entry.interval,
                    next_fire: entry.next_fire,
                },
            );
        }
        Ok(candidate)
    }

    pub fn discard_candidate(&mut self) -> Vec<RegistryKey> {
        self.entries.drain().map(|(_, entry)| entry.key).collect()
    }

    pub fn replace_entries(&mut self, mut candidate: TimerStore) -> Vec<RegistryKey> {
        let old = self.entries.drain().map(|(_, entry)| entry.key).collect();
        self.entries = std::mem::take(&mut candidate.entries);
        self.next_id = candidate.next_id;
        let _ = self.wake_tx.try_send(());
        old
    }

    pub fn add(
        &mut self,
        plugin: Arc<str>,
        key: RegistryKey,
        callback: Function,
        interval: Duration,
    ) -> u64 {
        let id = self.next_id;
        self.next_id += 1;
        self.entries.insert(
            id,
            TimerEntry {
                plugin,
                key,
                callback,
                interval,
                next_fire: Instant::now() + interval,
            },
        );
        id
    }

    pub fn del(&mut self, id: u64) -> Option<RegistryKey> {
        self.entries.remove(&id).map(|e| e.key)
    }

    pub fn clear_plugin(&mut self, plugin: &str) -> Vec<RegistryKey> {
        let ids: Vec<u64> = self
            .entries
            .iter()
            .filter(|(_, e)| e.plugin.as_ref() == plugin)
            .map(|(id, _)| *id)
            .collect();
        ids.into_iter()
            .map(|id| self.entries.remove(&id).expect("collected id present").key)
            .collect()
    }

    pub fn next_deadline(&self) -> Option<Instant> {
        self.entries.values().map(|e| e.next_fire).min()
    }

    /// Callbacks whose deadline has come, each with its id and entry advanced
    /// to the next slot strictly after `now`. Deadlines missed while the
    /// runtime was busy coalesce into one fire: no backlog catch-up.
    pub fn due(&mut self, now: Instant) -> Vec<(u64, Function)> {
        let mut out = Vec::new();
        for (id, e) in self.entries.iter_mut() {
            if e.next_fire <= now {
                // Next slot strictly after `now`. The interval cap keeps the
                // nanosecond count within u64.
                let rem = (now - e.next_fire).as_nanos() % e.interval.as_nanos();
                e.next_fire = now + (e.interval - Duration::from_nanos(rem as u64));
                out.push((*id, e.callback.clone()));
            }
        }
        out
    }
}

/// The dispatcher loop's wake receiver. `set` pokes it so a newly registered
/// earlier deadline is not shadowed by a deadline the loop already sleeps on.
pub(crate) fn wake_rx(lua: &Lua) -> Receiver<()> {
    lua.app_data_ref::<TimerStore>()
        .expect("timer store installed at init")
        .wake_rx
        .clone()
}

/// Earliest pending fire, for the dispatcher loop's timer arm.
pub(crate) fn next_deadline(lua: &Lua) -> Option<Instant> {
    lua.app_data_ref::<TimerStore>()?.next_deadline()
}

/// Spawn-ready tasks for every timer due at this instant. Each fire runs as
/// a fresh short task: a fresh cancel token (the caller's context may be
/// stale by the time the deadline lands), a fresh async deadline, and a
/// fresh registry pin the task removes on completion. The timer id is passed
/// as the first callback argument, so self-stopping callbacks need no
/// upvalue capture.
pub(crate) fn due_tasks(lua: &Lua) -> Vec<PendingAsyncTask> {
    let now = Instant::now();
    let fires = match lua.app_data_mut::<TimerStore>() {
        Some(mut store) => store.due(now),
        None => return Vec::new(),
    };
    fires
        .into_iter()
        .filter_map(
            |(timer_id, callback)| match lua.create_registry_value(callback) {
                Ok(work_fn) => Some(PendingAsyncTask {
                    work_fn,
                    cancel: CancelToken::none(),
                    deadline: Some(now + ASYNC_RUN_DEFAULT_DEADLINE),
                    live_ctx: None,
                    owner: None,
                    command_depth: 0,
                    command_invocation: None,
                    timer_id: Some(timer_id),
                }),
                Err(e) => {
                    tracing::warn!(error = %e, "failed to pin timer callback");
                    None
                }
            },
        )
        .collect()
}

/// Schedule {callback} to run every {seconds} on the runtime's timer pump.
///
/// Each fire runs as a fresh task, so the callback may be async (`sleep`,
/// fs, ...) and fires exactly when due: no per-frame polling. The callback
/// receives the timer's id as its first argument - use it with
/// `maki.timer.del` to stop the timer. Do not capture the returned id in the
/// callback instead: `local id = maki.timer.set(5, function()
/// maki.timer.del(id) end)` captures nil (a Luau value-capture quirk), which
/// is why the id is passed as an argument. A fire still running when the
/// next deadline arrives runs alongside it; every fire task carries the
/// standard 60s async deadline. Deadlines missed while the runtime was busy
/// coalesce into a single fire.
///
/// Registration is synchronous - nothing runs until the first deadline - so
/// calling this from `init.lua` or a command is safe.
///
/// @param seconds number Interval between fires, in seconds. Must be a finite number > 0 and < 1e9.
/// @param callback function Called on each fire with the timer id. May be async.
/// @return integer Id. Pass to `maki.timer.del` to stop the timer.
/// @example
/// local runs = 0
/// maki.timer.set(1, function(id)
///   runs = runs + 1
///   if runs == 10 then
///     maki.timer.del(id)
///   end
/// end)
#[lua_fn]
fn set(
    lua: &Lua,
    #[ctx] pending: PendingTimerStore,
    #[ctx] plugin: Arc<str>,
    seconds: f64,
    callback: Function,
) -> LuaResult<u64> {
    if !seconds.is_finite() || seconds <= 0.0 || seconds >= MAX_LUA_SECONDS {
        return Err(mlua::Error::runtime(ERR_SECONDS_RANGE));
    }
    let key = lua.create_registry_value(callback.clone())?;
    let interval = Duration::from_secs_f64(seconds);
    if crate::runtime::loading_plugin(lua).is_some() {
        Ok(pending
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .add(plugin, key, callback, interval))
    } else {
        let mut store = lua
            .app_data_mut::<TimerStore>()
            .ok_or_else(|| mlua::Error::runtime("timer store not initialized"))?;
        let id = store.add(plugin, key, callback, interval);
        // One pending wake is enough: the pump re-picks the earliest deadline
        // on its next pass.
        let _ = store.wake_tx.try_send(());
        Ok(id)
    }
}

/// Stop the timer {id}. Does nothing for an unknown id. A fire that already
/// started keeps running out.
///
/// @param id integer Id returned by `maki.timer.set`.
/// @example
/// local id = maki.timer.set(5, on_tick)
/// -- later
/// maki.timer.del(id)
#[lua_fn]
fn del(lua: &Lua, #[ctx] pending: PendingTimerStore, id: u64) -> LuaResult<()> {
    let key = if crate::runtime::loading_plugin(lua).is_some() {
        pending
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .del(id)
    } else {
        lua.app_data_mut::<TimerStore>()
            .and_then(|mut store| store.del(id))
    };
    if let Some(key) = key {
        lua.remove_registry_value(key).ok();
    }
    Ok(())
}

lua_table! {
    /// Recurring callbacks on the runtime's timer pump.
    ///
    /// Use `set` for anything that must happen every N seconds: demo loops,
    /// periodic refreshes, watchdogs. Each fire runs as a fresh task, so
    /// callbacks may sleep or do I/O, and fires land exactly on schedule
    /// instead of being polled each frame. Timers registered by a plugin are
    /// dropped when the plugin is unloaded.
    ///
    /// ```lua
    /// local id = maki.timer.set(5, function()
    ///   print("five seconds gone")
    /// end)
    /// ```
    "maki.timer" => pub(crate) fn create_timer_table(pending: PendingTimerStore, plugin: Arc<str>), DOCS [
        set(pending, plugin), del(pending),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    use mlua::Table;
    use test_case::test_case;

    fn setup() -> (Lua, Table) {
        let lua = Lua::new();
        lua.set_app_data(TimerStore::new());
        let tbl = create_timer_table(
            &lua,
            Arc::new(Mutex::new(TimerStore::new())),
            Arc::from("test"),
        )
        .unwrap();
        lua.globals().set("timer_tbl", tbl.clone()).unwrap();
        (lua, tbl)
    }

    #[test_case("timer_tbl.set(0, function() end)" ; "zero")]
    #[test_case("timer_tbl.set(-1, function() end)" ; "negative")]
    #[test_case("timer_tbl.set(math.huge, function() end)" ; "infinity")]
    #[test_case("timer_tbl.set(0 / 0, function() end)" ; "nan")]
    #[test_case("timer_tbl.set(1e10, function() end)" ; "too_large")]
    fn set_rejects_bad_seconds(code: &str) {
        let (lua, _tbl) = setup();
        let err = lua.load(code).eval::<u64>().unwrap_err();
        assert!(
            err.to_string().contains(ERR_SECONDS_RANGE),
            "expected error containing {ERR_SECONDS_RANGE:?}, got: {err}"
        );
    }

    #[test]
    fn set_returns_sequential_ids_and_earliest_deadline_wins() {
        let (lua, _tbl) = setup();
        let ids: (u64, u64) = lua
            .load(
                r#"
                local a = timer_tbl.set(2, function() end)
                local b = timer_tbl.set(0.5, function() end)
                return a, b
                "#,
            )
            .eval()
            .unwrap();
        assert_eq!(ids, (0, 1));
        let store = lua.app_data_ref::<TimerStore>().unwrap();
        let deadline = store.next_deadline().unwrap();
        assert!(
            deadline <= Instant::now() + Duration::from_secs_f64(0.6),
            "earliest interval wins the deadline: {deadline:?}"
        );
    }

    #[test]
    fn del_stops_the_schedule() {
        let (lua, _tbl) = setup();
        lua.load(
            r#"
            local a = timer_tbl.set(1, function() end)
            timer_tbl.del(a)
            return true
            "#,
        )
        .eval::<bool>()
        .unwrap();
        assert!(
            lua.app_data_ref::<TimerStore>()
                .unwrap()
                .next_deadline()
                .is_none()
        );
        // Unknown ids are a no-op, not an error.
        lua.load("timer_tbl.del(99)").eval::<()>().unwrap();
    }

    #[test]
    fn set_pokes_the_pump_wake_channel() {
        let (lua, _tbl) = setup();
        let wake = wake_rx(&lua);
        assert!(wake.try_recv().is_err(), "no wake before set");
        lua.load("timer_tbl.set(1, function() end)")
            .eval::<u64>()
            .unwrap();
        assert!(wake.try_recv().is_ok(), "set must wake the pump");
    }

    #[test]
    fn due_coalesces_missed_intervals_without_double_firing() {
        let (lua, _tbl) = setup();
        let callback = lua
            .create_function(|_lua, _args: mlua::MultiValue| Ok::<(), mlua::Error>(()))
            .unwrap();
        let key = lua.create_registry_value(callback.clone()).unwrap();
        let mut store = lua.app_data_mut::<TimerStore>().unwrap();
        let t0 = Instant::now();
        store.add(Arc::from("p"), key, callback, Duration::from_millis(50));
        let now = t0 + Duration::from_millis(230);
        assert_eq!(store.due(now).len(), 1, "one fire per missed window");
        let next = store.next_deadline().unwrap();
        assert!(
            next > now && next <= now + Duration::from_millis(51),
            "missed intervals coalesce to the next slot: {next:?} vs {now:?}"
        );
        assert!(
            store.due(now).is_empty(),
            "no double fire at the same instant"
        );
    }

    #[test]
    fn clear_plugin_returns_only_own_keys() {
        let (lua, _tbl) = setup();
        let fn_a = lua
            .create_function(|_lua, _args: mlua::MultiValue| Ok::<(), mlua::Error>(()))
            .unwrap();
        let fn_b = lua
            .create_function(|_lua, _args: mlua::MultiValue| Ok::<(), mlua::Error>(()))
            .unwrap();
        let key_a = lua.create_registry_value(fn_a.clone()).unwrap();
        let key_a_id = key_a.id();
        let key_b = lua.create_registry_value(fn_b.clone()).unwrap();
        let mut store = lua.app_data_mut::<TimerStore>().unwrap();
        store.add(Arc::from("p1"), key_a, fn_a, Duration::from_secs(1));
        store.add(Arc::from("p2"), key_b, fn_b, Duration::from_secs(2));
        let cleared = store.clear_plugin("p1");
        assert_eq!(cleared.len(), 1);
        assert_eq!(cleared[0].id(), key_a_id, "p1's pinned key is returned");
        assert!(store.next_deadline().is_some(), "p2's schedule survives");
        assert!(
            store.clear_plugin("p1").is_empty(),
            "unknown plugin is a no-op"
        );
    }
}
