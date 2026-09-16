use maki_agent::session_coordinator::SessionCoordinatorError;
use std::io;
use std::path::PathBuf;

#[derive(Debug, thiserror::Error)]
pub enum PluginError {
    #[error("lua error in {plugin}: {source}")]
    Lua {
        plugin: String,
        #[source]
        source: mlua::Error,
    },
    #[error("plugin {plugin} attempted to shadow existing tool '{tool}'")]
    NameConflict { plugin: String, tool: String },
    #[error("io error loading plugin {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error(
        "plugins.{plugin} sets options ({keys}), but there is no bundled plugin named \"{plugin}\""
    )]
    UnknownPluginOptions { plugin: String, keys: String },
    #[error("no bundled plugin named \"{plugin}\" (enabled via plugins.{plugin})")]
    UnknownPlugin { plugin: String },
    #[error("failed to unload plugin {plugin}: {error}")]
    Unload { plugin: String, error: String },
    #[error("plugin host is not running")]
    HostDead,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SessionOptionMutationError {
    #[error("plugin host is not running")]
    HostDead,
    #[error(transparent)]
    Coordinator(#[from] SessionCoordinatorError),
}
