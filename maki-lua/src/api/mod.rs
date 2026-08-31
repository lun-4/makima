pub(crate) mod agent;
pub(crate) mod r#async;
pub(crate) mod autocmd;
pub(crate) mod base64;
pub(crate) mod completion;
pub(crate) mod env;
pub(crate) mod r#fn;
pub(crate) mod fs;
pub(crate) mod image;
pub(crate) mod interpreter;
pub(crate) mod json;
pub(crate) mod keymap;
pub(crate) mod log;
pub(crate) mod r#match;
pub(crate) mod mode;
pub(crate) mod model;
pub(crate) mod net;
pub(crate) mod options;
pub(crate) mod perf;
pub(crate) mod session;
pub(crate) mod session_option;
pub(crate) mod slot;
pub(crate) mod split;
pub(crate) mod store;
pub(crate) mod text;
pub(crate) mod time;
pub(crate) mod timer;
pub(crate) mod tool;
pub(crate) mod treesitter;
pub(crate) mod ui;
pub(crate) mod util;
pub(crate) mod uv;
pub(crate) mod yaml;

use std::sync::{Arc, Mutex};

use mlua::{Lua, Result as LuaResult, Table};

use crate::api::keymap::PendingKeymapStore;
use crate::api::options::PluginOpts;
use crate::api::session_option::PendingSessionOptions;
use crate::api::store::PendingStore;
use crate::api::tool::{PendingRules, PendingTools};
use crate::api::util::command::{PendingCommandMap, UiAction};
use crate::plugin_permissions::PluginPermissions;

#[derive(Clone)]
pub(crate) struct PluginLoadContext {
    pub(crate) pending: PendingTools,
    pub(crate) pending_rules: PendingRules,
    pub(crate) pending_autocmds: crate::api::autocmd::PendingAutocmdStore,
    pub(crate) pending_timers: crate::api::timer::PendingTimerStore,
    pub(crate) pending_session_options: PendingSessionOptions,
    pub(crate) pending_commands: PendingCommandMap,
    pub(crate) pending_keymaps: PendingKeymapStore,
    pub(crate) pending_store: PendingStore,
    pub(crate) pending_options: crate::api::options::PendingPluginOptionSpecs,
    pub(crate) pending_sources: Arc<Mutex<crate::api::completion::PendingCompletionStore>>,
    pub(crate) pending_expanders: Arc<Mutex<crate::api::completion::PendingExpanderStore>>,
    pub(crate) pending_slots: crate::api::slot::PendingSlotStore,
    pub(crate) pending_prompts: crate::runtime::PendingPromptHintCallbacks,
    pub(crate) pending_hint: crate::api::ui::PendingHintStore,
}

pub(crate) fn create_maki_global(
    lua: &Lua,
    context: PluginLoadContext,
    plugin: Arc<str>,
    ui_action_tx: Option<flume::Sender<UiAction>>,
    permissions: &PluginPermissions,
    opts: PluginOpts,
) -> LuaResult<Table> {
    let maki = lua.create_table()?;
    lua.set_app_data(context.pending_prompts.clone());
    lua.set_app_data(context.pending_hint.clone());

    let api = tool::create_api_table(
        lua,
        context.clone(),
        Arc::clone(&plugin),
        opts,
        ui_action_tx.clone(),
    )?;
    autocmd::add_autocmd_methods(
        &api,
        lua,
        context.pending_autocmds.clone(),
        Arc::clone(&plugin),
    )?;
    lua.set_app_data(context.pending_slots.clone());
    slot::add_slot_methods(&api, lua, Arc::clone(&plugin))?;
    let mode_table = mode::create_mode_table(lua, ui_action_tx.clone())?;
    api.set("mode", mode_table)?;
    maki.set("api", api)?;
    maki.set("env", env::create_env_table(lua, permissions)?)?;
    maki.set("fs", fs::create_fs_table(lua, permissions)?)?;
    maki.set("log", log::create_log_table(lua, Arc::clone(&plugin))?)?;
    maki.set("treesitter", treesitter::create_treesitter_table(lua)?)?;
    maki.set("uv", uv::create_uv_table(lua, permissions)?)?;
    maki.set("base64", base64::create_base64_table(lua)?)?;
    maki.set("image", image::create_image_table(lua)?)?;
    maki.set("json", json::create_json_table(lua)?)?;
    maki.set("yaml", yaml::create_yaml_table(lua)?)?;
    maki.set("net", net::create_net_table(lua, permissions)?)?;
    maki.set("text", text::create_text_table(lua)?)?;
    maki.set("match", r#match::create_match_table(lua)?)?;
    maki.set(
        "store",
        store::create_store_table(lua, context.pending_store.clone(), Arc::clone(&plugin))?,
    )?;
    maki.set(
        "session",
        session::create_session_table(lua, ui_action_tx.clone())?,
    )?;
    maki.set(
        "model",
        model::create_model_table(lua, ui_action_tx.clone())?,
    )?;
    maki.set(
        "ui",
        ui::create_ui_table(
            lua,
            ui_action_tx.clone(),
            Arc::clone(&plugin),
            Some(context.pending_hint.clone()),
        )?,
    )?;
    maki.set(
        "fn",
        r#fn::create_fn_table(lua, Arc::clone(&plugin), permissions, ui_action_tx.clone())?,
    )?;
    split::split__register(&maki, lua)?;
    maki.set("async", r#async::create_async_table(lua)?)?;
    maki.set(
        "interpreter",
        interpreter::create_interpreter_table(lua, permissions)?,
    )?;
    maki.set("agent", agent::create_agent_table(lua)?)?;
    maki.set(
        "keymap",
        keymap::create_keymap_table(lua, context.pending_keymaps.clone(), Arc::clone(&plugin))?,
    )?;
    maki.set(
        "timer",
        timer::create_timer_table(lua, context.pending_timers.clone(), Arc::clone(&plugin))?,
    )?;
    maki.set("time", time::create_time_table(lua)?)?;
    maki.set("perf", perf::create_perf_table(lua)?)?;
    crate::splash::register_version_api(lua, &maki)?;

    Ok(maki)
}
