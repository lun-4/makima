use std::path::PathBuf;

use maki_lua_macro::{lua_fn, lua_table};
use maki_storage::StateDir;
use mlua::Lua;

use crate::plugin_permissions::PluginPermissions;

pub(crate) struct StateDirOverride(pub(crate) PathBuf);

/// Present when maki was started with `--no-project`.
pub(crate) struct NoProject;

/// The state directory this host writes to: the override a test or embedder
/// installed, or the user's.
pub(crate) fn host_state_dir(lua: &Lua) -> Result<StateDir, String> {
    match lua.app_data_ref::<StateDirOverride>() {
        Some(dir) => Ok(StateDir::from_path(dir.0.clone())),
        None => StateDir::resolve().map_err(|error| error.to_string()),
    }
}

fn utf8(p: PathBuf) -> Option<String> {
    p.into_os_string().into_string().ok()
}

/// Return the directory where makima stores runtime state (sessions, auth tokens, etc.).
/// Typically something like `~/.local/state/makima`.
///
/// @return (string?) State directory path, or nil if it cannot be determined.
/// @example
/// local dir = maki.env.state_dir()
#[lua_fn(guard = Env)]
fn state_dir(lua: &Lua) -> mlua::Result<Option<String>> {
    if let Some(path) = lua.app_data_ref::<StateDirOverride>() {
        return Ok(utf8(path.0.clone()));
    }
    Ok(maki_storage::paths::state_dir().ok().and_then(utf8))
}

/// Return the directory where makima looks for user configuration files.
/// Typically something like `~/.config/makima`.
///
/// @return (string?) Config directory path, or nil if it cannot be determined.
/// @example
/// local dir = maki.env.config_dir()
#[lua_fn(guard = Env)]
fn config_dir(_lua: &Lua) -> mlua::Result<Option<String>> {
    Ok(maki_storage::paths::config_dir().ok().and_then(utf8))
}

/// Return the directory where maki writes its log files (`maki.log`).
/// Typically something like `~/.local/logs/makima`.
///
/// @return (string?) Logs directory path, or nil if it cannot be determined.
/// @example
/// local dir = maki.env.logs_dir()
#[lua_fn(guard = Env)]
fn logs_dir(_lua: &Lua) -> mlua::Result<Option<String>> {
    Ok(maki_storage::paths::logs_dir().ok().and_then(utf8))
}

/// Whether maki was started with `--no-project`. A plugin that reads files
/// from the project's `.makima` directory should skip them when this is true.
///
/// @return (boolean) `true` under `--no-project`.
/// @example
/// if not maki.env.no_project() then
///   scan(".makima/skills")
/// end
#[lua_fn(guard = Env)]
fn no_project(lua: &Lua) -> mlua::Result<bool> {
    Ok(lua.app_data_ref::<NoProject>().is_some())
}

lua_table! {
    /// Paths to makima's own directories (config, state, logs), and whether
    /// the project's `.makima` directory is off limits.
    ///
    /// Use these to locate config files or persistent state without hard-coding paths.
    ///
    /// ```lua
    /// local cfg = maki.env.config_dir()
    /// ```
    "maki.env" => pub(crate) fn create_env_table(perms: &PluginPermissions), DOCS [
        state_dir(perms), config_dir(perms), logs_dir(perms), no_project(perms),
    ]
}
