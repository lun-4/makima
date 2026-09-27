use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::mem;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use arc_swap::ArcSwap;
use maki_agent::tools::HookStage;
use maki_lua_macro::{lua_fn, lua_table};
use mlua::{Function, Lua, MultiValue, Result as LuaResult, Table, Value};

use crate::api::util::dispatch::{DepthGuard, Reentry, call_swallowing};

/// Slot names the host fires itself. A plugin declaring one would shadow a
/// point whose firing order dispatch guarantees, so the namespace is closed.
pub(crate) const HOST_PREFIX: &str = "tool.";

const SEAM: &str = "slot";

#[derive(Clone)]
pub(crate) struct SlotLayer {
    pub plugin: Arc<str>,
    pub func: Function,
}

/// `owner: None` means orphan fillers: `set_slot` ran before the owner's
/// `declare_slot`. They wait here and attach once the owner declares.
#[derive(Clone, Default)]
pub(crate) struct SlotEntry {
    pub owner: Option<Arc<str>>,
    pub default: Option<Function>,
    pub layers: Vec<SlotLayer>,
}

#[derive(Clone)]
pub(crate) struct SlotStore {
    pub slots: HashMap<String, SlotEntry>,
    /// The published view of `slots`, for the one reader that cannot take the
    /// Lua state: a tool call on the agent's thread.
    layered: Arc<LayeredTools>,
}

pub(crate) type PendingSlotStore = Arc<Mutex<SlotStore>>;

impl SlotStore {
    pub fn new(layered: Arc<LayeredTools>) -> Self {
        Self {
            slots: HashMap::new(),
            layered,
        }
    }

    pub fn clear_plugin(&mut self, plugin: &str) {
        for entry in self.slots.values_mut() {
            entry.layers.retain(|l| l.plugin.as_ref() != plugin);
            if entry.owner.as_deref() == Some(plugin) {
                entry.owner = None;
                entry.default = None;
            }
        }
        self.slots
            .retain(|_, e| e.owner.is_some() || !e.layers.is_empty());
        self.publish();
    }

    pub fn replace_plugin(&mut self, plugin: &str, candidate: Self) {
        self.clear_plugin(plugin);
        for (name, entry) in candidate.slots {
            let default = (entry.owner.as_deref() == Some(plugin))
                .then_some(entry.default)
                .flatten();
            let layers: Vec<_> = entry
                .layers
                .into_iter()
                .filter(|layer| layer.plugin.as_ref() == plugin)
                .collect();
            if default.is_some() || !layers.is_empty() {
                let live = self.slots.entry(name).or_default();
                if let Some(default) = default {
                    live.owner = Some(Arc::from(plugin));
                    live.default = Some(default);
                }
                live.layers.extend(layers);
            }
        }
        self.publish();
    }

    /// Rebuilds the published index from `slots`, which stays the only thing
    /// anyone writes. Every mutation ends here, so a tool call never reads a
    /// layer a reload took away, or misses one it just added.
    pub fn publish(&self) {
        let mut stages = StageSets::default();
        for (name, entry) in &self.slots {
            if entry.layers.is_empty() {
                continue;
            }
            if let Some((tool, stage)) = host_slot_target(name) {
                stages[stage as usize].insert(Arc::from(tool));
            }
        }
        self.layered.0.store(Arc::new(stages));
    }
}

impl Default for SlotStore {
    fn default() -> Self {
        Self::new(Arc::default())
    }
}

type StageSets = [HashSet<Arc<str>>; HookStage::ALL.len()];

/// Which tools have a layer on which stage, keyed by tool rather than by slot
/// name so the check every tool call makes costs one atomic load and one
/// lookup, with nothing formatted and nothing allocated.
///
/// Owned by the runtime that created the [`SlotStore`], so two plugin hosts in
/// one process each answer for their own layers.
#[derive(Default)]
pub struct LayeredTools(ArcSwap<StageSets>);

impl LayeredTools {
    pub fn wraps(&self, tool: &str, stage: HookStage) -> bool {
        self.0.load()[stage as usize].contains(tool)
    }
}

/// Each layer gets a fresh single-shot `prev`. Calling it twice, or after
/// the layer already returned, throws instead of running the rest of the
/// chain again: the states make double execution impossible by shape.
enum PrevState {
    Armed,
    Running,
    Done(LuaResult<MultiValue>),
    Expired,
}

type PrevCell = Arc<Mutex<PrevState>>;

fn take_state(cell: &PrevCell, next: PrevState) -> PrevState {
    mem::replace(&mut cell.lock().expect("prev state poisoned"), next)
}

fn set_state(cell: &PrevCell, state: PrevState) {
    *cell.lock().expect("prev state poisoned") = state;
}

fn slot_snapshot(
    lua: &Lua,
    plugin: Option<&str>,
    name: &str,
) -> Option<(Option<Function>, Arc<[SlotLayer]>)> {
    let snapshot =
        |entry: &SlotEntry| Some((entry.default.clone(), entry.layers.as_slice().into()));
    if plugin.is_some_and(|plugin| crate::runtime::loading_plugin_is(lua, plugin))
        && let Some(pending) = lua.app_data_ref::<PendingSlotStore>()
    {
        let pending = pending.lock().unwrap_or_else(|error| error.into_inner());
        return pending.slots.get(name).and_then(snapshot);
    }
    let store = lua.app_data_ref::<SlotStore>()?;
    store.slots.get(name).and_then(snapshot)
}

fn with_slot_store<T>(
    lua: &Lua,
    plugin: &str,
    f: impl FnOnce(&mut SlotStore) -> LuaResult<T>,
) -> LuaResult<T> {
    if crate::runtime::loading_plugin_is(lua, plugin)
        && let Some(pending) = lua.app_data_ref::<PendingSlotStore>()
    {
        return f(&mut pending.lock().unwrap_or_else(|e| e.into_inner()));
    }
    let mut store = lua
        .app_data_mut::<SlotStore>()
        .ok_or_else(|| mlua::Error::runtime("slot store not initialized"))?;
    let res = f(&mut store)?;
    store.publish();
    Ok(res)
}

/// The innermost call of a host-fired chain: no plugin owns those slots, so
/// the arguments fall through unchanged when every layer defers.
fn identity_default(lua: &Lua) -> LuaResult<Function> {
    lua.create_function(|_, args: MultiValue| Ok(args))
}

/// Everything a chain needs except its position in it. Bundled because
/// `create_async_function` wants an owned copy per call, and the alternative is
/// cloning four captures by hand at every hop.
#[derive(Clone)]
struct Chain {
    lua: Lua,
    name: Arc<str>,
    default: Function,
    layers: Arc<[SlotLayer]>,
}

fn make_prev(chain: &Chain, rest: usize, state: &PrevCell) -> LuaResult<Function> {
    let owned = chain.clone();
    let state = Arc::clone(state);
    chain.lua.create_async_function(move |_, args: MultiValue| {
        let chain = owned.clone();
        let state = Arc::clone(&state);
        async move {
            match take_state(&state, PrevState::Running) {
                PrevState::Armed => {
                    let r = invoke_chain(chain, rest, args).await;
                    set_state(&state, PrevState::Done(r.clone()));
                    r
                }
                prior => {
                    let what = match prior {
                        PrevState::Expired => "expired",
                        _ => "already consumed",
                    };
                    set_state(&state, prior);
                    Err(mlua::Error::runtime(format!(
                        "prev for slot '{}' {what}",
                        chain.name
                    )))
                }
            }
        }
    })
}

/// Runs the chain so everything below a layer executes exactly once.
///
/// `idx` is the number of layers left; layer `idx - 1` runs with a fresh
/// single-shot `prev` that continues the chain. The `(default, layers)`
/// snapshot cannot race an unload: all Lua runs on the runtime thread and
/// unloads arrive through the request channel.
///
/// Layers may park, which is what lets one shell out or read a file before it
/// decides. They run in the caller's task ([`call_swallowing`]), so the
/// caller's cancellation and deadline reach the layers producing its answer.
///
/// When a layer errors, its `prev` state tells us how far it got:
/// - never called `prev`: skip the broken layer, run the rest with the
///   layer's own input
/// - called `prev`: the rest already ran, so return the stored outcome
///   rather than re-running it
///
/// Errors from the default propagate unwrapped: the default is the owner's
/// own function, same as any local call.
fn invoke_chain(
    chain: Chain,
    idx: usize,
    args: MultiValue,
) -> Pin<Box<dyn Future<Output = LuaResult<MultiValue>> + Send>> {
    Box::pin(async move {
        let Some(layer) = idx.checked_sub(1).map(|i| chain.layers[i].clone()) else {
            return chain.default.call_async(args).await;
        };
        let state: PrevCell = Arc::new(Mutex::new(PrevState::Armed));
        let prev = make_prev(&chain, idx - 1, &state)?;
        let mut layer_args = args.clone();
        layer_args.push_front(Value::Function(prev));
        let result =
            call_swallowing::<MultiValue>(&layer.func, layer_args, &chain.name, &layer.plugin)
                .await;
        match (result, take_state(&state, PrevState::Expired)) {
            (Some(r), _) => Ok(r),
            (None, PrevState::Done(r)) => r,
            (None, PrevState::Armed) => invoke_chain(chain, idx - 1, args).await,
            (None, PrevState::Running | PrevState::Expired) => Err(mlua::Error::runtime(format!(
                "prev for slot '{}' left in inconsistent state",
                chain.name
            ))),
        }
    })
}

/// The one way into [`invoke_chain`], so no caller can start a chain without
/// the depth bound that stops a layer from re-entering its own seam forever.
async fn run_chain(
    lua: &Lua,
    name: Arc<str>,
    default: Function,
    layers: Arc<[SlotLayer]>,
    args: MultiValue,
) -> LuaResult<MultiValue> {
    let _guard = DepthGuard::enter(lua, SEAM, &name, Reentry::Task).map_err(|_| {
        mlua::Error::runtime(format!(
            "slot '{name}' exceeded max depth (recursive filler? call prev instead)"
        ))
    })?;
    let depth = layers.len();
    let chain = Chain {
        lua: lua.clone(),
        name,
        default,
        layers,
    };
    invoke_chain(chain, depth, args).await
}

/// The slot a stage of a tool call fires: `("bash", Input)` -> `tool.bash.input`.
pub(crate) fn host_slot_name(tool: &str, stage: HookStage) -> String {
    format!("{HOST_PREFIX}{tool}.{}", stage.as_str())
}

/// The inverse of [`host_slot_name`]. `None` for any other name, including a
/// `tool.` name whose suffix names no stage.
pub(crate) fn host_slot_target(slot: &str) -> Option<(&str, HookStage)> {
    let (tool, suffix) = slot.strip_prefix(HOST_PREFIX)?.rsplit_once('.')?;
    let stage = HookStage::ALL.into_iter().find(|s| s.as_str() == suffix)?;
    Some((tool, stage))
}

/// Fires a host-owned slot: same layer contract as a declared one, with an
/// identity default nobody can replace. `allow_layer` says which plugins'
/// layers may see it, and living in the caller keeps slots ignorant of tools
/// and permissions.
///
/// `None` means nothing ran, which the identity default handing back `args`
/// would not say: the caller has to leave the value alone rather than report a
/// rewrite.
pub(crate) async fn run_host_chain(
    lua: &Lua,
    name: &str,
    args: MultiValue,
    allow_layer: &dyn Fn(&str) -> bool,
) -> LuaResult<Option<MultiValue>> {
    let Some((_, layers)) = slot_snapshot(lua, None, name) else {
        return Ok(None);
    };
    let layers: Arc<[SlotLayer]> = layers
        .iter()
        .filter(|layer| allow_layer(&layer.plugin))
        .cloned()
        .collect();
    if layers.is_empty() {
        return Ok(None);
    }
    run_chain(lua, Arc::from(name), identity_default(lua)?, layers, args)
        .await
        .map(Some)
}

/// The callable closes over `name` only and reads the store on every call,
/// so a handle given out before a reload keeps working after it.
fn make_callable(lua: &Lua, plugin: Option<Arc<str>>, name: String) -> LuaResult<Function> {
    let name: Arc<str> = Arc::from(name.as_str());
    lua.create_async_function(move |lua, args: MultiValue| {
        let name = Arc::clone(&name);
        let plugin = plugin.clone();
        async move {
            let (default, layers) = slot_snapshot(&lua, plugin.as_deref(), &name)
                .and_then(|(default, layers)| Some((default?, layers)))
                .ok_or_else(|| mlua::Error::runtime(format!("slot '{name}' is not declared")))?;
            run_chain(&lua, name, default, layers, args).await
        }
    })
}

pub(crate) async fn invoke_slot_from_host(
    lua: &Lua,
    name: &str,
    args: MultiValue,
) -> LuaResult<MultiValue> {
    let func = make_callable(lua, None, name.to_owned())?;
    func.call_async(args).await
}

/// Create a named extension point owned by your plugin. You provide a
/// {default} function, and other plugins can wrap it with layers using
/// `set_slot`. The returned callable runs the full chain: outermost
/// layer first, then inward, ending at {default}.
///
/// Throws if another plugin already owns a slot with the same {name}, or
/// if {name} starts with `"tool."`, which the host fires itself.
///
/// The chain is async: the default and every layer may park (`maki.fs.*`,
/// `maki.fn.jobwait`, `maki.agent.call_tool`, ...), and so does the
/// returned callable. Call it from a tool handler, a command, or an
/// autocmd, rather than from a `header` or `restore` function, which
/// cannot wait. The chain runs in your task, so cancelling the caller
/// cancels the layers it is waiting on.
///
/// @param name string Unique slot name, e.g. `"myplugin.render"`.
/// @param default function Default implementation, called when no layers wrap it.
/// @return (function) Callable that dispatches through all layers.
/// @example
/// local render = maki.api.declare_slot("myplugin.render", function(text)
///   return text:upper()
/// end)
/// print(render("hello")) -- HELLO
#[lua_fn]
fn declare_slot(
    lua: &Lua,
    #[ctx] plugin: Arc<str>,
    name: String,
    default: Function,
) -> LuaResult<Function> {
    if name.starts_with(HOST_PREFIX) {
        return Err(mlua::Error::runtime(format!(
            "slot '{name}' is host owned: '{HOST_PREFIX}' names are fired by maki itself, \
             use set_slot to wrap one"
        )));
    }
    with_slot_store(lua, &plugin, |store| {
        let entry = store.slots.entry(name.clone()).or_default();
        if let Some(owner) = &entry.owner {
            return Err(mlua::Error::runtime(format!(
                "slot '{name}' already declared by '{owner}'"
            )));
        }
        entry.owner = Some(Arc::clone(&plugin));
        entry.default = Some(default);
        Ok(())
    })?;
    make_callable(lua, Some(plugin), name)
}

/// Add a layer around an existing (or future) slot. Layers wrap the
/// default from the outside in. Each layer receives `prev` as its
/// first argument. Call `prev(...)` to continue down the chain.
/// Calling `prev` more than once throws.
///
/// You can call this before the owner runs `declare_slot`. The layer
/// is queued and attached when the slot is declared.
///
/// A layer may park, and one that throws is skipped: the chain continues
/// as if it had returned `prev(...)` untouched, so a broken layer never
/// takes the seam down with it.
///
/// Layers wrap in registration order, so the last one registered runs
/// first and sees the value before the others do.
///
/// Maki fires two slots per tool itself: `tool.<name>.input` before
/// permissions look at the call, and `tool.<name>.output` on the text it
/// produced. Both take `function(prev, value, ctx)` and answer with a
/// table to replace the value, nothing to leave it alone, or
/// `nil, reason` to stop the call. Wrapping one costs the capability the
/// tool declares, and a tool declaring none costs every permission. See
/// [Hooks](/docs/hooks/).
///
/// @param name string Slot name to wrap.
/// @param wrapper function Layer: `function(prev, ...)`. Call `prev(...)` to continue.
/// @return
/// @example
/// maki.api.set_slot("myplugin.render", function(prev, text)
///   return prev("[" .. text .. "]")
/// end)
#[lua_fn]
fn set_slot(lua: &Lua, #[ctx] plugin: Arc<str>, name: String, wrapper: Function) -> LuaResult<()> {
    with_slot_store(lua, &plugin, |store| {
        store.slots.entry(name).or_default().layers.push(SlotLayer {
            plugin: Arc::clone(&plugin),
            func: wrapper,
        });
        Ok(())
    })
}

/// List all known slots and their current state. Useful for debugging
/// which plugins own or wrap each slot.
///
/// @return table<string, { owner: string?, has_default: boolean, layers: { plugin: string }[] }>
#[lua_fn]
fn get_slots(lua: &Lua) -> LuaResult<Table> {
    let out = lua.create_table()?;
    let pending = lua.app_data_ref::<PendingSlotStore>();
    let store_guard = lua.app_data_ref::<SlotStore>();
    let pending_guard = pending
        .as_ref()
        .map(|p| p.lock().unwrap_or_else(|e| e.into_inner()));
    let slots_map: &HashMap<String, SlotEntry> = if let Some(p) = pending_guard.as_ref() {
        &p.slots
    } else if let Some(s) = store_guard.as_ref() {
        &s.slots
    } else {
        return Ok(out);
    };
    for (name, entry) in slots_map {
        let info = lua.create_table()?;
        info.set("owner", entry.owner.as_deref())?;
        info.set("declared", entry.default.is_some())?;
        let fillers = lua.create_table()?;
        for layer in &entry.layers {
            fillers.push(layer.plugin.as_ref())?;
        }
        info.set("fillers", fillers)?;
        out.set(name.as_str(), info)?;
    }
    Ok(out)
}

lua_table! {
    extend "maki.api" => pub(crate) fn add_slot_methods(plugin: Arc<str>), DOCS [
        declare_slot(plugin), set_slot(plugin), get_slots,
    ]
}

#[cfg(test)]
mod tests {
    use test_case::test_case;

    use super::*;

    fn noop(lua: &Lua) -> Function {
        lua.create_function(|_, ()| Ok(())).unwrap()
    }

    fn entry(lua: &Lua, owner: &str, layers: &[&str]) -> SlotEntry {
        SlotEntry {
            owner: Some(Arc::from(owner)),
            default: Some(noop(lua)),
            layers: layers
                .iter()
                .map(|p| SlotLayer {
                    plugin: Arc::from(*p),
                    func: noop(lua),
                })
                .collect(),
        }
    }

    #[test_case("tool.bash.input", Some(("bash", HookStage::Input)); "input")]
    #[test_case("tool.bash.output", Some(("bash", HookStage::Output)); "output")]
    #[test_case("tool.mcp__srv__do.input", Some(("mcp__srv__do", HookStage::Input)); "underscored_name")]
    #[test_case("tool.srv.do.input", Some(("srv.do", HookStage::Input)); "dotted_name")]
    #[test_case("tool.bash.header", None; "unknown_suffix")]
    #[test_case("tool.bash", None; "no_suffix")]
    #[test_case("myplugin.render", None; "not_host_owned")]
    fn host_slot_target_reads_the_wrapped_tool(slot: &str, target: Option<(&str, HookStage)>) {
        assert_eq!(host_slot_target(slot), target);
    }

    /// The two directions have to stay each other's inverse, since dispatch
    /// builds the name it fires and `publish` parses the names it was given.
    #[test_case("bash", HookStage::Input ; "input")]
    #[test_case("srv.do", HookStage::Output ; "dotted_output")]
    fn host_slot_names_round_trip(tool: &str, stage: HookStage) {
        let name = host_slot_name(tool, stage);
        assert_eq!(host_slot_target(&name), Some((tool, stage)));
    }

    #[test]
    fn clear_plugin_semantics() {
        let lua = Lua::new();
        let mut store = SlotStore::default();
        store
            .slots
            .insert("s".into(), entry(&lua, "owner", &["a", "b"]));
        store.clear_plugin("a");
        assert_eq!(store.slots["s"].layers.len(), 1);
        assert_eq!(store.slots["s"].layers[0].plugin.as_ref(), "b");

        store.clear_plugin("owner");
        assert!(store.slots["s"].owner.is_none());
        assert!(store.slots["s"].default.is_none());
        assert_eq!(
            store.slots["s"].layers.len(),
            1,
            "layers survive owner clear"
        );

        store.clear_plugin("b");
        assert!(
            !store.slots.contains_key("s"),
            "fully-cleared entry is dropped"
        );
    }

    /// The published index is derived, never written to directly, so an unload
    /// can only narrow it.
    #[test]
    fn publishing_follows_the_layers() {
        let lua = Lua::new();
        let layered: Arc<LayeredTools> = Arc::default();
        let mut store = SlotStore::new(Arc::clone(&layered));
        store.slots.insert(
            host_slot_name("bash", HookStage::Input),
            entry(&lua, "owner", &["a"]),
        );
        store
            .slots
            .insert("myplugin.render".into(), entry(&lua, "owner", &["a"]));
        store.publish();
        assert!(layered.wraps("bash", HookStage::Input));
        assert!(!layered.wraps("bash", HookStage::Output));
        assert!(!layered.wraps("myplugin.render", HookStage::Input));

        store.clear_plugin("a");
        assert!(
            !layered.wraps("bash", HookStage::Input),
            "the index drops with the plugin that registered the layer"
        );
    }
}
