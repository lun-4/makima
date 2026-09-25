use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::Duration;

use include_dir::{Dir, include_dir};
use maki_agent::permissions::{PluginRuleStore, carries_builtin_defaults};
use maki_agent::session_coordinator::SessionOptionCatalog;
use maki_agent::tools::{ToolRegistry, ToolSource};
use maki_commands::CommandRegistry;
use maki_config::{GatedFile, PluginsConfig, ProjectConfig, RawConfig};

use crate::api::completion::{CompletionCtx, ItemSpec};
use crate::api::fs::{FsBackend, RealFs};
use crate::api::keymap::KeymapReader;
use crate::api::options::{PluginOptionSpecs, PluginOpts};
use crate::api::util::command::{CommandGenerationMap, HintReader, StatusContentReader, UiAction};
use crate::api::util::picker::PickerEvent;
use crate::coalesced_latest::CoalescedLatest;
use crate::error::PluginError;
use crate::plugin_permissions::{PluginPermissions, load_plugin_permissions};
use crate::runtime::{
    self, ClickFallback, CommandArgumentContext, CommandArgumentLifecycle,
    CommandArgumentLifecycleRequest, CommandArgumentRequest, LuaThread, Request, RestoreItem,
    SplashFrameRequest, lifecycle_superseded,
};
use crate::splash::{SPLASH_PULL_TIMEOUT, SplashFrame, SplashPull};
use maki_agent::prompt::ResolvedSlots;

const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(2);
const GLOBAL_INIT_OWNER: &str = "maki_init.global";
const PROJECT_INIT_OWNER: &str = "maki_init.project";
#[allow(dead_code)]
pub const SKIPPED_PLUGIN_WARNING: &str = "skipping plugin lua";
/// Tests assert on this exact text, so a wording tweak here updates them too.
pub const PERMISSION_NAME_WARNING: &str = "inherits maki's permission rules for the builtin \
     tool of the same name, together with any \"always allow\" you saved";
pub const TRUST_SCOPE_WARNING: &str =
    "trust is only read from the global init.lua; ignoring the trust table in";

/// How far user `init.lua` may reach. `--no-plugins` turns it off, and a
/// project folder nobody vouched for stops at the global file.
///
/// The project variant carries the path instead of a trust verdict, so the only
/// way to build one is a `Some` out of [`ProjectConfig::gated_path`] and
/// "trusted" cannot disagree with "which file".
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum InitFiles {
    Disabled,
    Global,
    GlobalAndProject(PathBuf),
}

impl InitFiles {
    pub fn resolve(project_config: &ProjectConfig, no_plugins: bool) -> Self {
        if no_plugins {
            return InitFiles::Disabled;
        }
        match project_config.gated_path(GatedFile::InitLua) {
            Some(path) => InitFiles::GlobalAndProject(path),
            None => InitFiles::Global,
        }
    }
}

struct BundledPlugin {
    name: &'static str,
    dir: Dir<'static>,
}

/// `lib` is not a default builtin; it exists so plugins can
/// `require()` shared modules across boundaries.
static BUNDLED_PLUGINS: &[BundledPlugin] = &[
    BundledPlugin {
        name: "sessions",
        dir: include_dir!("$CARGO_MANIFEST_DIR/../plugins/sessions"),
    },
    BundledPlugin {
        name: "usage",
        dir: include_dir!("$CARGO_MANIFEST_DIR/../plugins/usage"),
    },
    BundledPlugin {
        name: "index",
        dir: include_dir!("$CARGO_MANIFEST_DIR/../plugins/index"),
    },
    BundledPlugin {
        name: "webfetch",
        dir: include_dir!("$CARGO_MANIFEST_DIR/../plugins/webfetch"),
    },
    BundledPlugin {
        name: "websearch",
        dir: include_dir!("$CARGO_MANIFEST_DIR/../plugins/websearch"),
    },
    BundledPlugin {
        name: "bash",
        dir: include_dir!("$CARGO_MANIFEST_DIR/../plugins/bash"),
    },
    BundledPlugin {
        name: "batch",
        dir: include_dir!("$CARGO_MANIFEST_DIR/../plugins/batch"),
    },
    BundledPlugin {
        name: "grep",
        dir: include_dir!("$CARGO_MANIFEST_DIR/../plugins/grep"),
    },
    BundledPlugin {
        name: "glob",
        dir: include_dir!("$CARGO_MANIFEST_DIR/../plugins/glob"),
    },
    BundledPlugin {
        name: "skill",
        dir: include_dir!("$CARGO_MANIFEST_DIR/../plugins/skill"),
    },
    BundledPlugin {
        name: "memory",
        dir: include_dir!("$CARGO_MANIFEST_DIR/../plugins/memory"),
    },
    BundledPlugin {
        name: "question",
        dir: include_dir!("$CARGO_MANIFEST_DIR/../plugins/question"),
    },
    BundledPlugin {
        name: "todo_write",
        dir: include_dir!("$CARGO_MANIFEST_DIR/../plugins/todo_write"),
    },
    BundledPlugin {
        name: "read",
        dir: include_dir!("$CARGO_MANIFEST_DIR/../plugins/read"),
    },
    BundledPlugin {
        name: "write",
        dir: include_dir!("$CARGO_MANIFEST_DIR/../plugins/write"),
    },
    BundledPlugin {
        name: "edit",
        dir: include_dir!("$CARGO_MANIFEST_DIR/../plugins/edit"),
    },
    BundledPlugin {
        name: "task",
        dir: include_dir!("$CARGO_MANIFEST_DIR/../plugins/task"),
    },
    BundledPlugin {
        name: "model",
        dir: include_dir!("$CARGO_MANIFEST_DIR/../plugins/model"),
    },
    BundledPlugin {
        name: "thinking",
        dir: include_dir!("$CARGO_MANIFEST_DIR/../plugins/thinking"),
    },
    BundledPlugin {
        name: "mode_plan_override",
        dir: include_dir!("$CARGO_MANIFEST_DIR/../plugins/mode_plan_override"),
    },
    BundledPlugin {
        name: "options",
        dir: include_dir!("$CARGO_MANIFEST_DIR/../plugins/options"),
    },
    BundledPlugin {
        name: "perf",
        dir: include_dir!("$CARGO_MANIFEST_DIR/../plugins/perf"),
    },
    BundledPlugin {
        name: "plan_submit_tool",
        dir: include_dir!("$CARGO_MANIFEST_DIR/../plugins/plan_submit_tool"),
    },
    BundledPlugin {
        name: "code_execution",
        dir: include_dir!("$CARGO_MANIFEST_DIR/../plugins/code_execution"),
    },
    BundledPlugin {
        name: "view_image",
        dir: include_dir!("$CARGO_MANIFEST_DIR/../plugins/view_image"),
    },
    BundledPlugin {
        name: "lib",
        dir: include_dir!("$CARGO_MANIFEST_DIR/../plugins/lib"),
    },
    BundledPlugin {
        name: "list",
        dir: include_dir!("$CARGO_MANIFEST_DIR/../plugins/list"),
    },
    // Bundled splashes: the default starfield plus the named picker entries.
    BundledPlugin {
        name: "splashes_default",
        dir: include_dir!("$CARGO_MANIFEST_DIR/../plugins/splashes_default"),
    },
    // Splash picker: presents the `splash` registry as a switchable set.
    BundledPlugin {
        name: "splashes",
        dir: include_dir!("$CARGO_MANIFEST_DIR/../plugins/splashes"),
    },
];

pub(crate) fn lib_dir() -> &'static Dir<'static> {
    &BUNDLED_PLUGINS
        .iter()
        .find(|p| p.name == "lib")
        .expect("lib plugin bundled")
        .dir
}

static BUNDLED_DIRS: LazyLock<&'static [&'static Dir<'static>]> = LazyLock::new(|| {
    let dirs: Vec<&'static Dir<'static>> = BUNDLED_PLUGINS.iter().map(|p| &p.dir).collect();
    Vec::leak(dirs)
});

pub struct PluginHost {
    inner: LuaThread,
    plugin_rules: Arc<PluginRuleStore>,
    registry: Arc<ToolRegistry>,
    command_registry: CommandRegistry,
}

impl Drop for PluginHost {
    fn drop(&mut self) {
        let Some(handle) = self.inner.join.take() else {
            return;
        };
        // Start the shutdown first, or the join below waits for all
        // queued bulk work to drain.
        self.begin_shutdown();
        let (done_tx, done_rx) = flume::bounded(1);
        std::thread::spawn(move || {
            let _ = done_tx.send(handle.join().is_err());
        });
        match done_rx.recv_timeout(SHUTDOWN_TIMEOUT) {
            Ok(true) => tracing::warn!("lua thread panicked on shutdown"),
            Err(_) => tracing::warn!("lua thread did not stop within timeout, detaching"),
            Ok(false) => {}
        }
    }
}

impl PluginHost {
    pub fn new(registry: Arc<ToolRegistry>) -> Result<Self, PluginError> {
        Self::with_jit(registry, true)
    }

    pub fn with_command_registry(
        registry: Arc<ToolRegistry>,
        command_registry: CommandRegistry,
        jit: bool,
    ) -> Result<Self, PluginError> {
        Self::with_jit_and_state_dir(registry, command_registry, jit, Arc::new(RealFs), None)
    }

    /// `jit: false` (the `--no-jit` flag) runs plugin Lua on the O1
    /// interpreter with full debug info. Applied at VM creation, so
    /// every chunk gets it, init.lua files included.
    pub fn with_jit(registry: Arc<ToolRegistry>, jit: bool) -> Result<Self, PluginError> {
        Self::with_jit_and_state_dir(
            registry,
            CommandRegistry::new(),
            jit,
            Arc::new(RealFs),
            None,
        )
    }

    #[cfg(feature = "test-support")]
    pub(crate) fn with_fs_for_tests(
        registry: Arc<ToolRegistry>,
        fs: Arc<dyn FsBackend>,
        state_dir: PathBuf,
    ) -> Result<Self, PluginError> {
        Self::with_jit_and_state_dir(registry, CommandRegistry::new(), true, fs, Some(state_dir))
    }

    fn with_jit_and_state_dir(
        registry: Arc<ToolRegistry>,
        command_registry: CommandRegistry,
        jit: bool,
        fs: Arc<dyn FsBackend>,
        state_dir: Option<PathBuf>,
    ) -> Result<Self, PluginError> {
        let modes = Arc::new(maki_agent::ModeRegistry::builtin());
        let plugin_rules = Arc::new(PluginRuleStore::default());
        let session_options = SessionOptionCatalog::default();
        let lua = runtime::spawn(
            Arc::clone(&registry),
            runtime::SpawnConfig {
                command_registry: command_registry.clone(),
                modes,
                bundled_dirs: *BUNDLED_DIRS,
                jit,
                plugin_rules: Arc::clone(&plugin_rules),
                session_options: session_options.clone(),
                state_dir,
                fs,
            },
        )?;
        Ok(Self {
            inner: lua,
            plugin_rules,
            registry,
            command_registry,
        })
    }

    /// The tool registry the host booted with, so tests can execute the
    /// plugins' tools outside the Lua thread.
    pub fn registry(&self) -> Arc<ToolRegistry> {
        Arc::clone(&self.registry)
    }

    /// The store that `maki.api.register_permission_rule` writes into. Hand
    /// it to every [`maki_agent::permissions::PermissionManager`] so plugin
    /// rules apply to all sessions.
    pub fn plugin_rules(&self) -> Arc<PluginRuleStore> {
        Arc::clone(&self.plugin_rules)
    }

    /// Stop the Lua thread from taking new work without joining it, so the
    /// caller can rebuild shared state (like the tool registry) while the
    /// old VM winds down on its own. The flag makes the watchdog abort
    /// in-flight callbacks, `Shutdown` on the priority lane skips ahead of
    /// queued bulk work, and swapping the senders for disconnected ones
    /// makes every later host call fail right at the send; `&mut self`
    /// rules out a call racing the swap. `Drop` still joins the thread.
    pub fn begin_shutdown(&mut self) {
        self.inner.shutdown.store(true, Ordering::Release);
        let _ = self.inner.prio_tx.send(Request::Shutdown);
        self.inner.tx = flume::unbounded().0;
        self.inner.prio_tx = flume::unbounded().0;
    }

    /// Boots the runtime and loads every default bundled plugin into `registry`.
    /// For callers like tests and docgen that want the full builtin set
    /// without building a config.
    pub fn with_all_builtins(registry: Arc<ToolRegistry>) -> Result<Self, PluginError> {
        let mut host = Self::new(registry)?;
        host.load_builtins(&PluginsConfig::from_plugins(HashMap::new()))?;
        Ok(host)
    }

    /// `warnings` collects non-fatal startup problems for the caller to surface.
    pub fn load_init_files(
        &self,
        init_files: InitFiles,
        warnings: &mut Vec<String>,
    ) -> Result<Option<RawConfig>, PluginError> {
        self.load_init_files_from_dirs(
            init_files,
            maki_storage::paths::config_search_dirs(),
            warnings,
        )
    }

    fn load_init_files_from_dirs(
        &self,
        init_files: InitFiles,
        global_dirs: impl IntoIterator<Item = PathBuf>,
        warnings: &mut Vec<String>,
    ) -> Result<Option<RawConfig>, PluginError> {
        if init_files == InitFiles::Disabled {
            return Ok(None);
        }

        let mut merged: Option<RawConfig> = None;

        for global_dir in global_dirs {
            self.run_init_file(
                &global_dir.join("init.lua"),
                "global/init.lua",
                GLOBAL_INIT_OWNER,
                true,
                &mut merged,
                warnings,
            )?;
            if merged.is_some() {
                break;
            }
        }
        if let InitFiles::GlobalAndProject(path) = &init_files {
            self.run_init_file(
                path,
                "project/init.lua",
                PROJECT_INIT_OWNER,
                false,
                &mut merged,
                warnings,
            )?;
        }

        Ok(merged)
    }

    /// `--no-plugins` recovery path: skip every user `init.lua` while the
    /// host and builtin plugins stay live. Centralized so every entry point
    /// (TUI, index, acp, prompt) honors the flag identically.
    pub fn load_init_files_or_skip(
        &self,
        no_plugins: bool,
        project_config: &ProjectConfig,
        warnings: &mut Vec<String>,
    ) -> Result<Option<RawConfig>, PluginError> {
        self.load_init_files(InitFiles::resolve(project_config, no_plugins), warnings)
    }

    fn run_init_file(
        &self,
        path: &Path,
        source_name: &str,
        owner: &str,
        global: bool,
        merged: &mut Option<RawConfig>,
        warnings: &mut Vec<String>,
    ) -> Result<(), PluginError> {
        if !path.is_file() {
            return Ok(());
        }
        let source = fs::read_to_string(path).map_err(|e| PluginError::Io {
            path: path.to_path_buf(),
            source: e,
        })?;
        let plugin_dir = path.parent().map(Path::to_path_buf);
        if let Some(mut raw) =
            self.send_run_init_lua_as(source, source_name.to_owned(), Arc::from(owner), plugin_dir)?
        {
            if !global && std::mem::take(&mut raw.trust).is_set() {
                warnings.push(format!("{TRUST_SCOPE_WARNING} {owner}"));
            }
            match merged {
                Some(existing) => existing.merge(raw),
                None => *merged = Some(raw),
            }
        }
        if let Some(warning) = self.permission_name_warning(owner) {
            warnings.push(warning);
        }
        Ok(())
    }

    pub fn load_builtins(&mut self, config: &PluginsConfig) -> Result<(), PluginError> {
        let result = self.send_builtin_loads(config);
        // Armed even when a load failed, so a caller that only warns about the
        // error is not left interpreting for the rest of the session.
        let _ = self.inner.tx.send(Request::WarmJit);
        result
    }

    fn send_builtin_loads(&self, config: &PluginsConfig) -> Result<(), PluginError> {
        for (plugin, opts) in &config.opts {
            let keys: Vec<&str> = opts.keys().map(String::as_str).collect();
            if !BUNDLED_PLUGINS.iter().any(|p| p.name == plugin.as_str()) {
                return Err(PluginError::UnknownPluginOptions {
                    plugin: plugin.clone(),
                    keys: keys.join(", "),
                });
            }
            if !config.names.contains(plugin) {
                tracing::warn!(
                    plugin = plugin.as_str(),
                    keys = keys.join(", "),
                    "plugin is disabled; its plugins.{} options are ignored until re-enabled",
                    plugin
                );
            }
        }
        for builtin in &config.names {
            let dir = match BUNDLED_PLUGINS.iter().find(|p| p.name == builtin.as_str()) {
                Some(p) => &p.dir,
                None => {
                    return Err(PluginError::UnknownPlugin {
                        plugin: builtin.clone(),
                    });
                }
            };
            let init = dir
                .get_file("init.lua")
                .and_then(|f| f.contents_utf8())
                .ok_or_else(|| PluginError::Lua {
                    plugin: builtin.clone(),
                    source: mlua::Error::runtime("bundled plugin missing init.lua"),
                })?;
            let name: Arc<str> = Arc::from(builtin.as_str());
            let opts = config
                .opts
                .get(builtin.as_str())
                .cloned()
                .map(Arc::new)
                .unwrap_or_default();
            self.send_load(
                name,
                init.to_owned(),
                None,
                PluginPermissions::trusted(),
                opts,
                matches!(
                    builtin.as_str(),
                    "read" | "glob" | "grep" | "write" | "edit"
                ),
            )?;
        }
        Ok(())
    }

    fn send_load(
        &self,
        name: Arc<str>,
        source: String,
        plugin_dir: Option<PathBuf>,
        permissions: PluginPermissions,
        opts: PluginOpts,
        bundled: bool,
    ) -> Result<(), PluginError> {
        let (reply_tx, reply_rx) = flume::bounded(1);
        self.inner
            .tx
            .send(Request::LoadSource {
                name,
                source,
                plugin_dir,
                permissions,
                opts,
                bundled,
                reply: reply_tx,
            })
            .map_err(|_| PluginError::HostDead)?;
        reply_rx.recv().map_err(|_| PluginError::HostDead)?
    }

    #[cfg(feature = "test-support")]
    pub fn pause_worker_for_test(&self) -> flume::Sender<()> {
        let (ready_tx, ready_rx) = flume::bounded(1);
        let (release_tx, release_rx) = flume::bounded(1);
        self.inner
            .prio_tx
            .send(Request::TestPause {
                ready: ready_tx,
                release: release_rx,
            })
            .expect("Lua worker is alive");
        ready_rx.recv().expect("Lua worker reached test pause");
        release_tx
    }

    #[cfg(feature = "test-support")]
    pub fn queue_load_source_for_test(
        &self,
        name: &str,
        source: &str,
    ) -> Result<flume::Receiver<Result<(), PluginError>>, PluginError> {
        let (reply_tx, reply_rx) = flume::bounded(1);
        self.inner
            .prio_tx
            .send(Request::LoadSource {
                name: Arc::from(name),
                source: source.to_owned(),
                plugin_dir: None,
                permissions: PluginPermissions::trusted(),
                opts: PluginOpts::default(),
                bundled: false,
                reply: reply_tx,
            })
            .map_err(|_| PluginError::HostDead)?;
        Ok(reply_rx)
    }

    #[cfg(feature = "test-support")]
    pub fn wait_for_worker_barrier_for_test(&self) {
        let (reply_tx, reply_rx) = flume::bounded(1);
        self.inner
            .tx
            .send(Request::TestBarrier { reply: reply_tx })
            .expect("Lua worker is alive");
        reply_rx.recv().expect("Lua worker reached test barrier");
    }

    /// Option specs declared by loaded plugins via `maki.api.register_options`,
    /// keyed by plugin name. Used by docgen.
    pub fn plugin_options(&self) -> Result<PluginOptionSpecs, PluginError> {
        let (reply_tx, reply_rx) = flume::bounded(1);
        self.inner
            .tx
            .send(Request::CollectPluginOptions { reply: reply_tx })
            .map_err(|_| PluginError::HostDead)?;
        reply_rx.recv().map_err(|_| PluginError::HostDead)
    }

    pub fn send_run_init_lua(
        &self,
        source: String,
        source_name: String,
        plugin_dir: Option<PathBuf>,
    ) -> Result<Option<RawConfig>, PluginError> {
        let owner = Arc::from(source_name.as_str());
        self.send_run_init_lua_as(source, source_name, owner, plugin_dir)
    }

    fn send_run_init_lua_as(
        &self,
        source: String,
        source_name: String,
        owner: Arc<str>,
        plugin_dir: Option<PathBuf>,
    ) -> Result<Option<RawConfig>, PluginError> {
        let (reply_tx, reply_rx) = flume::bounded(1);
        self.inner
            .tx
            .send(Request::RunInitLua {
                source,
                source_name,
                owner,
                plugin_dir,
                reply: reply_tx,
            })
            .map_err(|_| PluginError::HostDead)?;
        reply_rx.recv().map_err(|_| PluginError::HostDead)?
    }

    pub fn unload(&self, plugin: &str) -> Result<(), PluginError> {
        let (reply_tx, reply_rx) = flume::bounded(1);
        self.inner
            .tx
            .send(Request::ClearPlugin {
                plugin: Arc::from(plugin),
                reply: reply_tx,
            })
            .map_err(|_| PluginError::HostDead)?;
        reply_rx.recv().map_err(|_| PluginError::HostDead)?
    }

    pub fn load_source(&self, name: &str, source: &str) -> Result<(), PluginError> {
        self.load_source_with_opts(name, source, serde_json::Map::new())
    }

    pub fn load_source_with_opts(
        &self,
        name: &str,
        source: &str,
        opts: serde_json::Map<String, serde_json::Value>,
    ) -> Result<(), PluginError> {
        self.send_load(
            Arc::from(name),
            source.to_owned(),
            None,
            PluginPermissions::trusted(),
            Arc::new(opts),
            false,
        )
    }

    pub fn load_source_with_permissions(
        &self,
        name: &str,
        source: &str,
        permissions: PluginPermissions,
    ) -> Result<(), PluginError> {
        self.send_load(
            Arc::from(name),
            source.to_owned(),
            None,
            permissions,
            PluginOpts::default(),
            false,
        )
    }

    pub fn load_plugin_file(&self, path: &Path) -> Result<(), PluginError> {
        let source = fs::read_to_string(path).map_err(|e| PluginError::Io {
            path: path.to_path_buf(),
            source: e,
        })?;
        let plugin_dir = path.parent().map(Path::to_path_buf);
        let permissions = load_plugin_permissions(plugin_dir.as_deref());
        // Test-only path today. Once user plugin dirs exist: derive a real
        // plugin name, since the hardcoded "user" would collide across files,
        // pass the `plugins.<name>` opts through, and teach the
        // unknown-plugin guards about user plugin names.
        self.send_load(
            Arc::from("user"),
            source,
            plugin_dir,
            permissions,
            PluginOpts::default(),
            false,
        )
    }

    /// The names the plugin registered that maki's own permission defaults are
    /// keyed on. Taking such a name is allowed, and a drop-in replacement may
    /// want the builtin's rules, but the user has to be told which rules the
    /// plugin just inherited. One warning lists them all, because the TUI
    /// flashes a single warning and a per-tool one would drop the rest.
    pub fn permission_name_warning(&self, plugin: &str) -> Option<String> {
        let snapshot = self.registry.iter();
        let names: Vec<String> = snapshot
            .iter()
            .filter(|t| matches!(&t.source, ToolSource::Lua { plugin: p } if p.as_ref() == plugin))
            .filter(|t| carries_builtin_defaults(t.name()))
            .map(|t| format!("`{}`", t.name()))
            .collect();
        if names.is_empty() {
            return None;
        }
        Some(format!(
            "{plugin}: registered {}, so it {PERMISSION_NAME_WARNING}",
            names.join(", ")
        ))
    }
    pub fn event_handle(&self) -> EventHandle {
        EventHandle {
            tx: self.inner.tx.clone(),
            shutdown: Arc::clone(&self.inner.shutdown),
            prio_tx: self.inner.prio_tx.clone(),
            modes: Arc::clone(&self.inner.modes),
            session_options: self.inner.session_options.clone(),
            completion: None,
            command_generations: Some(Arc::clone(&self.inner.command_generations)),
            command_arguments: self.inner.command_arguments.clone(),
            command_argument_lifecycle: self.inner.command_argument_lifecycle.clone(),
            splash_frames: self.inner.splash_frames.clone(),
        }
    }

    /// Shared mode registry (built-ins plus whatever plugins defined).
    pub fn mode_registry(&self) -> Arc<maki_agent::ModeRegistry> {
        Arc::clone(&self.inner.modes)
    }

    pub fn command_registry(&self) -> CommandRegistry {
        self.command_registry.clone()
    }

    pub fn keymap_reader(&self) -> KeymapReader {
        self.inner.keymap_reader.clone()
    }

    pub fn hint_reader(&self) -> HintReader {
        self.inner.hint_reader.clone()
    }

    pub fn status_content_reader(&self) -> StatusContentReader {
        self.inner.status_content_reader.clone()
    }

    pub fn ui_action_rx(&self) -> flume::Receiver<UiAction> {
        self.inner.ui_action_rx.clone()
    }
}

#[derive(Clone)]
pub struct EventHandle {
    tx: flume::Sender<Request>,
    /// Set once the host starts shutting down. A handle outlives
    /// `begin_shutdown` -- it holds its own sender clone -- so without this it
    /// would race the dispatch loop for whether a request still gets served.
    shutdown: Arc<AtomicBool>,
    /// User-initiated requests bypass queued bulk work (session restores).
    prio_tx: flume::Sender<Request>,
    /// Shared mode registry; `None`-less so plugins and the Rust agent see
    /// the same definitions. Test handles use an empty builtin set.
    modes: Arc<maki_agent::ModeRegistry>,
    session_options: SessionOptionCatalog,
    /// In-memory stand-in for the Lua completion/expander stores, used only by
    /// tests that build an `App` without a running plugin host. `None` in
    /// production, where the two RPC methods below talk to the Lua thread.
    completion: Option<Arc<TestCompletionBackend>>,
    command_generations: Option<Arc<Mutex<CommandGenerationMap>>>,
    command_arguments: CoalescedLatest<CommandArgumentRequest>,
    command_argument_lifecycle: CoalescedLatest<CommandArgumentLifecycleRequest>,
    splash_frames: CoalescedLatest<SplashFrameRequest>,
}

/// In-memory completion/expander store for tests with no running Lua thread.
/// Mirrors what the Lua-side stores offer, so `App` code is identical between
/// production (RPC) and tests (direct lookup).
#[derive(Default)]
pub struct TestCompletionBackend {
    sources: std::sync::Mutex<HashMap<String, Vec<ItemSpec>>>,
    expanders: std::sync::Mutex<ExpanderMap>,
}

type ExpanderFn = Box<dyn Fn(&str) -> Result<String, String> + Send + Sync>;
type ExpanderMap = HashMap<String, ExpanderFn>;

impl TestCompletionBackend {
    pub fn new() -> Self {
        Self {
            sources: std::sync::Mutex::new(HashMap::new()),
            expanders: std::sync::Mutex::new(HashMap::new()),
        }
    }

    pub fn register_source(&self, prefix: &str, items: Vec<ItemSpec>) {
        self.sources
            .lock()
            .unwrap()
            .insert(prefix.to_string(), items);
    }

    pub fn register_expander<F>(&self, prefix: &str, f: F)
    where
        F: Fn(&str) -> Result<String, String> + Send + Sync + 'static,
    {
        self.expanders
            .lock()
            .unwrap()
            .insert(prefix.to_string(), Box::new(f));
    }

    fn collect(&self, _ctx: &CompletionCtx) -> Vec<ItemSpec> {
        self.sources
            .lock()
            .unwrap()
            .values()
            .flatten()
            .cloned()
            .collect()
    }

    fn expand(&self, text: &str) -> Result<String, String> {
        let tokens = crate::api::completion::parse_at_tokens(text);
        if tokens.is_empty() {
            return Ok(text.to_string());
        }
        let expanders = self.expanders.lock().unwrap();
        let mut out = String::with_capacity(text.len());
        let mut last_end = 0;
        for tok in &tokens {
            out.push_str(&text[last_end..tok.range.start]);
            match expanders.get(&tok.prefix) {
                Some(f) => out.push_str(&f(&tok.value)?),
                None => out.push_str(&text[tok.range.start..tok.range.end]),
            }
            last_end = tok.range.end;
        }
        out.push_str(&text[last_end..]);
        Ok(out)
    }
}

impl EventHandle {
    pub(crate) fn from_tx(tx: flume::Sender<Request>) -> Self {
        Self {
            tx: tx.clone(),
            shutdown: Arc::default(),
            prio_tx: flume::unbounded().0,
            modes: Arc::new(maki_agent::ModeRegistry::builtin()),
            session_options: SessionOptionCatalog::default(),
            completion: None,
            command_generations: None,
            command_arguments: CoalescedLatest::new({
                let tx = tx.clone();
                move |work| tx.send(Request::CollectCommandArgumentItems(work)).is_ok()
            }),
            command_argument_lifecycle: CoalescedLatest::with_supersede(
                {
                    let tx = tx.clone();
                    move |work| tx.send(Request::CommandArgumentLifecycle(work)).is_ok()
                },
                lifecycle_superseded,
            ),
            splash_frames: CoalescedLatest::new(move |work| {
                tx.send(Request::SplashFrame(work)).is_ok()
            }),
        }
    }

    pub fn mode_registry(&self) -> Arc<maki_agent::ModeRegistry> {
        Arc::clone(&self.modes)
    }

    pub fn session_option_catalog(&self) -> SessionOptionCatalog {
        self.session_options.clone()
    }

    pub async fn set_session_option(
        &self,
        coordinator: maki_agent::session_coordinator::SessionCoordinatorHandle,
        id: impl Into<Arc<str>>,
        value: impl Into<Arc<str>>,
    ) -> Result<
        maki_agent::session_options::SessionOptionsSnapshot,
        crate::SessionOptionMutationError,
    > {
        let id = id.into();
        let value = value.into();
        let snapshot = coordinator.read().options();
        if self.is_disconnected()
            && snapshot.options.iter().any(|option| {
                option.definition.id == id
                    && matches!(
                        option.definition.owner,
                        maki_agent::session_options::SessionOptionOwner::Builtin
                    )
            })
        {
            return coordinator
                .set_option_if_version(id, value, Some(snapshot.version))
                .await
                .map_err(Into::into);
        }
        let (reply, response) = flume::bounded(1);
        self.tx
            .send_async(Request::SetSessionOption {
                coordinator,
                id,
                value,
                reply,
            })
            .await
            .map_err(|_| crate::SessionOptionMutationError::HostDead)?;
        response
            .recv_async()
            .await
            .map_err(|_| crate::SessionOptionMutationError::HostDead)?
    }

    fn command_generation(&self, plugin: &Arc<str>, command: &Arc<str>) -> u64 {
        self.command_generations
            .as_ref()
            .and_then(|generations| {
                generations
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .get(&(Arc::clone(plugin), Arc::clone(command)))
                    .copied()
            })
            .unwrap_or_default()
    }

    #[doc(hidden)]
    pub fn command_generation_for_test(&self, plugin: &str, command: &str) -> u64 {
        self.command_generation(&Arc::from(plugin), &Arc::from(command))
    }

    #[doc(hidden)]
    pub fn disconnected_for_test() -> Self {
        Self::from_tx(flume::unbounded().0)
    }

    /// Test sibling of `disconnected_for_test` that carries a specific mode
    /// registry, for exercising mode-gated plan behavior.
    #[doc(hidden)]
    pub fn disconnected_for_test_with_modes(modes: Arc<maki_agent::ModeRegistry>) -> Self {
        Self {
            tx: flume::unbounded().0,
            shutdown: Arc::default(),
            prio_tx: flume::unbounded().0,
            modes,
            session_options: SessionOptionCatalog::default(),
            completion: None,
            command_generations: None,
            command_arguments: CoalescedLatest::new(|_| false),
            command_argument_lifecycle: CoalescedLatest::new(|_| false),
            splash_frames: CoalescedLatest::new(|_| false),
        }
    }

    /// Test handle backed by an in-memory completion/expander store, so `@`
    /// completion and submit expansion work without a running plugin host.
    #[doc(hidden)]
    pub fn with_completion_for_test(backend: Arc<TestCompletionBackend>) -> Self {
        Self {
            shutdown: Arc::default(),
            tx: flume::unbounded().0,
            prio_tx: flume::unbounded().0,
            modes: Arc::new(maki_agent::ModeRegistry::builtin()),
            session_options: SessionOptionCatalog::default(),
            completion: Some(backend),
            command_generations: None,
            command_arguments: CoalescedLatest::new(|_| false),
            command_argument_lifecycle: CoalescedLatest::new(|_| false),
            splash_frames: CoalescedLatest::new(|_| false),
        }
    }

    /// True when no runtime is draining requests. Production handles stay
    /// connected for the host's lifetime; the disconnected-for-test handle
    /// and a host whose thread has shut down both report true. Callers use
    /// this to skip async side effects (e.g. a restore-complete flip) that
    /// no live consumer would ever observe.
    pub fn is_disconnected(&self) -> bool {
        self.tx.is_disconnected() && self.prio_tx.is_disconnected()
    }

    /// Test probe sibling of `from_tx`: collapses both senders onto one
    /// channel so a `RequestProbe` sees every request, including the
    /// `prio_tx`-routed commands and keybind callbacks that `from_tx`
    /// would route to a disconnected channel.
    #[cfg(feature = "test-support")]
    pub(crate) fn probed_for_test(shared: flume::Sender<Request>) -> Self {
        Self {
            shutdown: Arc::default(),
            tx: shared.clone(),
            prio_tx: shared.clone(),
            modes: Arc::new(maki_agent::ModeRegistry::builtin()),
            session_options: SessionOptionCatalog::default(),
            completion: None,
            command_generations: None,
            command_arguments: CoalescedLatest::new({
                let shared = shared.clone();
                move |work| {
                    shared
                        .send(Request::CollectCommandArgumentItems(work))
                        .is_ok()
                }
            }),
            command_argument_lifecycle: CoalescedLatest::with_supersede(
                {
                    let shared = shared.clone();
                    move |work| shared.send(Request::CommandArgumentLifecycle(work)).is_ok()
                },
                lifecycle_superseded,
            ),
            splash_frames: CoalescedLatest::new(move |work| {
                shared.send(Request::SplashFrame(work)).is_ok()
            }),
        }
    }

    pub fn run_command(&self, plugin: Arc<str>, command: Arc<str>, args: String, depth: u8) {
        let generation = self.command_generation(&plugin, &command);
        let _ = self.prio_tx.try_send(Request::RunCommand {
            plugin,
            command,
            generation,
            args,
            depth,
            completion: None,
        });
    }

    #[doc(hidden)]
    pub fn run_command_for_test(
        &self,
        plugin: Arc<str>,
        command: Arc<str>,
        args: String,
        depth: u8,
    ) -> flume::Receiver<Result<(), String>> {
        let (completion, rx) = flume::bounded(1);
        let generation = self.command_generation(&plugin, &command);
        let _ = self.prio_tx.try_send(Request::RunCommand {
            plugin,
            command,
            generation,
            args,
            depth,
            completion: Some(completion),
        });
        rx
    }

    /// Headless drivers install their own provider so `maki.session.read` has
    /// something to answer with instead of "no interactive UI attached". The UI
    /// leaves the slot empty and answers through its event loop, which owns the
    /// live session runtimes.
    pub fn install_session_snapshot(&self, provider: crate::api::session::SessionSnapshotFn) {
        let _ = self
            .tx
            .try_send(Request::InstallSessionSnapshot { provider });
    }

    pub fn collect_prompt_slots(&self) -> ResolvedSlots {
        self.request_prompt_slots().recv().unwrap_or_default()
    }

    /// Gather `@`-completion candidates from every registered source, for the
    /// popup opened with `ctx` (mode + available models). Returns empty when no
    /// host is connected.
    pub fn collect_command_argument_items(
        &self,
        context: CommandArgumentContext,
        cancel: maki_agent::CancelToken,
    ) -> Option<flume::Receiver<Vec<crate::CommandArgumentItem>>> {
        let (reply, rx) = flume::bounded(1);
        self.command_arguments
            .submit(CommandArgumentRequest {
                context,
                callbacks: None,
                cancel,
                reply,
            })
            .then_some(rx)
    }

    pub fn command_argument_lifecycle(
        &self,
        context: CommandArgumentContext,
        event: CommandArgumentLifecycle,
        item: Option<crate::CommandArgumentItem>,
        cancel: maki_agent::CancelToken,
    ) {
        self.command_argument_lifecycle
            .submit(CommandArgumentLifecycleRequest {
                context,
                callbacks: None,
                event,
                item,
                cancel,
                _lifecycle_owner: None,
            });
    }

    pub fn collect_completion_items(&self, ctx: &CompletionCtx) -> Vec<ItemSpec> {
        if let Some(backend) = &self.completion {
            return backend.collect(ctx);
        }
        let (tx, rx) = flume::bounded(1);
        if self
            .tx
            .send(Request::CollectCompletionItems {
                ctx: ctx.clone(),
                reply: tx,
            })
            .is_err()
        {
            return Vec::new();
        }
        rx.recv().unwrap_or_default()
    }

    /// Rewrite a finished prompt by dispatching each `@prefix:value` token to
    /// its registered expander. A disconnected handle (no host) passes the text
    /// through unchanged so plain prompts still submit.
    pub fn expand_references(&self, text: &str) -> Result<String, String> {
        if let Some(backend) = &self.completion {
            return backend.expand(text);
        }
        let (tx, rx) = flume::bounded(1);
        if self
            .tx
            .send(Request::ExpandReferences {
                text: text.to_string(),
                reply: tx,
            })
            .is_err()
        {
            return Ok(text.to_string());
        }
        rx.recv().unwrap_or_else(|_| Ok(text.to_string()))
    }

    pub fn request_prompt_slots(&self) -> flume::Receiver<ResolvedSlots> {
        let (tx, rx) = flume::bounded(1);
        if !self.shutdown.load(Ordering::Acquire) {
            let _ = self.tx.send(Request::CollectPromptSlots { reply: tx });
        }
        rx
    }

    pub async fn collect_prompt_slots_async(&self) -> ResolvedSlots {
        self.request_prompt_slots()
            .recv_async()
            .await
            .unwrap_or_default()
    }

    pub fn request_restore(&self, item: RestoreItem, event_tx: maki_agent::EventSender) {
        let _ = self.tx.send(Request::RestoreToolAsync { item, event_tx });
    }

    /// `row` is the 1-based line in the tool's live buffer, 0 for clicks
    /// outside it (header line etc.).
    pub fn request_click(&self, tool_use_id: String, row: usize) {
        let _ = self.tx.send(Request::ClickTool {
            tool_use_id,
            row,
            fallback: None,
        });
    }

    /// Like [`Self::request_click`], but when the runtime no longer holds
    /// a live or warm handle for the tool it restores from `item` (whose
    /// `clicks` must already include `row`) and emits fresh snapshots on
    /// `event_tx`. Callers need no knowledge of the runtime's warm cache.
    pub fn request_click_with_fallback(
        &self,
        tool_use_id: String,
        row: usize,
        item: RestoreItem,
        event_tx: maki_agent::EventSender,
    ) {
        let _ = self.tx.send(Request::ClickTool {
            tool_use_id,
            row,
            fallback: Some(Box::new(ClickFallback { item, event_tx })),
        });
    }

    pub fn send_restore_complete(&self, flag: Arc<AtomicBool>) {
        let _ = self.tx.send(Request::RestoreComplete { flag });
    }

    /// Blocks until every restore item queued so far has finished; restores
    /// run as spawned tasks, and the `RestoreComplete` flag flips only once
    /// the whole batch has landed, making it the batch barrier.
    #[doc(hidden)]
    pub fn wait_restore_complete_for_test(&self) {
        const DEADLINE: Duration = Duration::from_secs(30);
        let flag = Arc::new(AtomicBool::new(true));
        self.send_restore_complete(Arc::clone(&flag));
        let start = std::time::Instant::now();
        while flag.load(Ordering::Relaxed) {
            assert!(start.elapsed() < DEADLINE, "restore batch never completed");
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    pub fn fire_autocmd(&self, event: &str, data: serde_json::Value) {
        let _ = self.tx.try_send(Request::FireAutocmd {
            event: event.to_owned(),
            data,
        });
    }

    /// Queues one `splash.render` frame without waiting for the Lua host.
    /// Requests share the coalesced priority lane, so only the latest queued
    /// frame survives while another render is active.
    pub fn request_splash_frame(
        &self,
        width: u16,
        height: u16,
        elapsed_secs: f32,
        fade: f32,
    ) -> Option<flume::Receiver<Option<SplashFrame>>> {
        if self.tx.is_disconnected() && self.prio_tx.is_disconnected() {
            return None;
        }
        let (reply_tx, reply_rx) = flume::bounded(1);
        self.splash_frames
            .submit(SplashFrameRequest {
                width,
                height,
                elapsed_secs,
                fade,
                reply: reply_tx,
            })
            .then_some(reply_rx)
    }

    /// Blocking compatibility wrapper around [`Self::request_splash_frame`].
    pub fn splash_pull(&self, width: u16, height: u16, elapsed_secs: f32, fade: f32) -> SplashPull {
        let Some(reply_rx) = self.request_splash_frame(width, height, elapsed_secs, fade) else {
            return SplashPull::Unknown;
        };
        match reply_rx.recv_timeout(SPLASH_PULL_TIMEOUT) {
            Ok(Some(frame)) => SplashPull::Frame(frame),
            Ok(None) => SplashPull::Missing,
            Err(_) => SplashPull::Unknown,
        }
    }

    /// Convenience: frame-only view of [`Self::splash_pull`].
    pub fn splash_frame(
        &self,
        width: u16,
        height: u16,
        elapsed_secs: f32,
        fade: f32,
    ) -> Option<SplashFrame> {
        self.splash_pull(width, height, elapsed_secs, fade).frame()
    }

    /// Push fresh version/update info into the Lua-side `VersionStore` via the
    /// priority lane so a frame pull queued right after sees it in channel
    /// order. Only called when the reported version actually changes.
    pub fn set_version(&self, current: &str, latest: Option<&str>) {
        let _ = self.prio_tx.try_send(Request::SetVersion {
            current: current.to_owned(),
            latest: latest.map(str::to_owned),
        });
    }

    pub fn set_clock_format(&self, format: maki_config::ClockFormat) {
        let _ = self.prio_tx.try_send(Request::SetClockFormat(format));
    }

    pub fn provider_usage_changed(
        &self,
        snapshot: crate::ProviderUsageSnapshot,
        invalidation: Option<crate::ProviderUsageInvalidation>,
    ) -> bool {
        self.prio_tx
            .send(Request::ProviderUsageChanged {
                snapshot,
                invalidation,
            })
            .is_ok()
    }

    pub fn run_keybind_callback(&self, id: u64) -> bool {
        self.prio_tx
            .try_send(Request::RunKeybindCallback { id })
            .is_ok()
    }

    /// Reports a host list-picker dialog event (selection change, idle
    /// timeout, done) to the Lua thread so the callbacks stored with the
    /// dialog fire; `Done` drains the store entry.
    pub fn picker_event(&self, id: u64, ev: PickerEvent) {
        let _ = self.prio_tx.try_send(Request::PickerEvent { id, ev });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use maki_agent::SessionMailbox;
    use maki_agent::prompt::{PromptId, ResolvedSlots, Slot};
    use maki_agent::session_coordinator::{
        DirectoryAdoptionFuture, ModelAdoptionFuture, SessionCheckpoint, SessionCoordinatorHandle,
        SessionCoordinatorParams, builtin_option_definitions,
    };
    use maki_agent::tools::ToolRegistry;
    use maki_providers::Model;
    use maki_storage::checkpoint::{
        CheckpointAck, CheckpointError, CheckpointFuture, CheckpointRequest, CheckpointWriter,
    };
    use maki_storage::id::MakiId;
    use std::collections::BTreeMap;
    use std::thread;
    use std::time::Instant;
    use test_case::test_case;

    struct FakeCommandHost;

    impl maki_commands::CommandHost for FakeCommandHost {
        fn request(
            &self,
            _request: maki_commands::HostRequest,
        ) -> maki_commands::CommandFuture<
            Result<maki_commands::HostResponse, maki_commands::CommandError>,
        > {
            Box::pin(async { Ok(maki_commands::HostResponse::Completed) })
        }
    }

    fn command_snapshot(host: &PluginHost) -> maki_commands::RegistrySnapshot {
        let registry = host.command_registry();
        let target = registry.bind_target(
            maki_commands::TargetCapabilities::ALL,
            Arc::new(FakeCommandHost),
        );
        registry.snapshot_for(&target).unwrap()
    }

    fn test_coordinator(catalog: SessionOptionCatalog) -> SessionCoordinatorHandle {
        test_coordinator_with_options(catalog, Default::default())
    }

    fn test_coordinator_with_options(
        catalog: SessionOptionCatalog,
        persisted_options: BTreeMap<String, String>,
    ) -> SessionCoordinatorHandle {
        let checkpoint: Arc<dyn CheckpointWriter<SessionCheckpoint>> =
            Arc::new(|request: CheckpointRequest<SessionCheckpoint>| {
                Box::pin(async move {
                    Ok(CheckpointAck {
                        session_id: request.session_id,
                        version: request.version,
                    })
                }) as CheckpointFuture
            });
        test_coordinator_with_checkpoint(catalog, persisted_options, checkpoint)
    }

    fn test_coordinator_with_checkpoint(
        catalog: SessionOptionCatalog,
        persisted_options: BTreeMap<String, String>,
        checkpoint: Arc<dyn CheckpointWriter<SessionCheckpoint>>,
    ) -> SessionCoordinatorHandle {
        let id = MakiId::generate();
        SessionCoordinatorHandle::register(SessionCoordinatorParams {
            session_id: id,
            catalog,
            definitions: builtin_option_definitions(
                "test/model",
                [Arc::from("test/model")],
                false,
                false,
                false,
                maki_agent::ThinkingConfig::Off,
            ),
            persisted_options,
            history: Vec::new(),
            model: Arc::from("test/model"),
            cwd: PathBuf::from("/project"),
            model_policy: Arc::default(),
            model_adopter: Arc::new(|_: Model| Box::pin(async { Ok(()) }) as ModelAdoptionFuture),
            directory_adopter: Arc::new(|path: PathBuf| {
                Box::pin(async move { Ok(path) }) as DirectoryAdoptionFuture
            }),
            checkpoint,
            mailbox: SessionMailbox::new(id),
        })
        .unwrap()
    }

    /// jit=true is exercised by the whole integration suite
    /// (`tests/plugin_host.rs` boots hosts via `new`); only the O1
    /// interpreter path needs its own coverage.
    #[test]
    fn with_jit_off_loads_builtins_and_registers_tools() {
        let reg = Arc::new(ToolRegistry::new());
        let mut host = PluginHost::with_jit(Arc::clone(&reg), false).unwrap();
        host.load_builtins(&PluginsConfig::from_plugins(HashMap::new()))
            .unwrap();
        assert!(reg.has("glob"));
    }

    #[test]
    fn global_and_project_init_session_options_coexist_with_stable_owners() {
        const GLOBAL_SOURCE: &str = r#"
            maki.api.register_session_option({
                id = "maki_init.global.choice",
                name = "Global choice",
                description = "Global init choice",
                category = "mode",
                values = { { value = "on", name = "On" } },
                initial_value = "on",
            })
        "#;
        const PROJECT_SOURCE: &str = r#"
            maki.api.register_session_option({
                id = "maki_init.project.choice",
                name = "Project choice",
                description = "Project init choice",
                category = "mode",
                values = { { value = "on", name = "On" } },
                initial_value = "on",
            })
        "#;

        smol::block_on(async {
            let host = PluginHost::new(Arc::new(ToolRegistry::new())).unwrap();
            host.send_run_init_lua_as(
                GLOBAL_SOURCE.to_owned(),
                "global/init.lua".to_owned(),
                Arc::from(GLOBAL_INIT_OWNER),
                None,
            )
            .unwrap();
            host.send_run_init_lua_as(
                PROJECT_SOURCE.to_owned(),
                "project/init.lua".to_owned(),
                Arc::from(PROJECT_INIT_OWNER),
                None,
            )
            .unwrap();
            let error = host
                .send_run_init_lua_as(
                    "error('broken')".to_owned(),
                    "global/init.lua".to_owned(),
                    Arc::from(GLOBAL_INIT_OWNER),
                    None,
                )
                .unwrap_err();
            assert!(matches!(
                error,
                PluginError::Lua { plugin, .. } if plugin == "global/init.lua"
            ));
            let coordinator = test_coordinator(host.event_handle().session_option_catalog());
            let snapshot = coordinator.read().options();

            for (id, expected_owner) in [
                ("maki_init.global.choice", GLOBAL_INIT_OWNER),
                ("maki_init.project.choice", PROJECT_INIT_OWNER),
            ] {
                let option = snapshot
                    .options
                    .iter()
                    .find(|option| option.definition.id.as_ref() == id)
                    .unwrap();
                assert!(matches!(
                    &option.definition.owner,
                    maki_agent::session_options::SessionOptionOwner::Plugin { plugin, .. }
                        if plugin.as_ref() == expected_owner
                ));
            }

            coordinator.close().await.unwrap();
        });
    }

    /// The second call sends `Shutdown` on a sender that is already
    /// disconnected; it must swallow that error and keep rejecting work.
    #[test]
    fn session_option_load_reload_failure_and_unload_are_transactional() {
        const SOURCE: &str = r#"
            maki.api.register_session_option({
                id = "choice.value",
                name = "Choice",
                description = "Test choice",
                category = "mode",
                values = {
                    { value = "a", name = "A" },
                    { value = "b", name = "B" },
                },
                initial_value = "a",
                persistent = true,
            })
        "#;
        smol::block_on(async {
            let host = PluginHost::new(Arc::new(ToolRegistry::new())).unwrap();
            host.load_source("choice", SOURCE).unwrap();
            let coordinator = test_coordinator(host.event_handle().session_option_catalog());
            coordinator.set_option("choice.value", "b").await.unwrap();

            host.load_source("choice", SOURCE).unwrap();
            let current = || {
                coordinator
                    .read()
                    .options()
                    .options
                    .iter()
                    .find(|option| option.definition.id.as_ref() == "choice.value")
                    .unwrap()
                    .current_value
                    .to_string()
            };
            let generation = || {
                coordinator
                    .read()
                    .options()
                    .options
                    .iter()
                    .find_map(|option| match &option.definition.owner {
                        maki_agent::session_options::SessionOptionOwner::Plugin {
                            plugin,
                            generation,
                        } if plugin.as_ref() == "choice" => Some(*generation),
                        _ => None,
                    })
                    .unwrap()
            };
            assert_eq!(current(), "b");
            let before_unload = generation();

            assert!(
                host.load_source("choice", "maki.api.register_session_option({ id = 'bad' })")
                    .is_err()
            );
            assert_eq!(current(), "b");

            host.unload("choice").unwrap();
            assert!(
                coordinator
                    .read()
                    .options()
                    .options
                    .iter()
                    .all(|option| option.definition.id.as_ref() != "choice.value")
            );
            host.load_source("choice", SOURCE).unwrap();
            assert!(generation() > before_unload);
            host.unload("choice").unwrap();
            coordinator.close().await.unwrap();
        });
    }

    #[test]
    fn tool_conflict_preserves_plugin_catalog_and_commands() {
        const ORIGINAL: &str = r#"
            maki.api.register_tool({
                name = "original_tool",
                description = "Original tool",
                schema = { type = "object", properties = {}, additionalProperties = false },
                handler = function() return "ok" end,
            })
            maki.api.register_command({
                name = "/original",
                description = "Original command",
                tui_only = false,
                handler = function() end,
            })
            maki.api.register_session_option({
                id = "choice.original",
                name = "Original option",
                description = "Original option",
                category = "mode",
                values = {{ value = "on", name = "On" }},
                initial_value = "on",
            })
        "#;
        const CANDIDATE: &str = r#"
            maki.api.register_tool({
                name = "shared_tool",
                description = "Conflicting tool",
                schema = { type = "object", properties = {}, additionalProperties = false },
                handler = function() return "ok" end,
            })
            maki.api.register_command({
                name = "/candidate",
                description = "Candidate command",
                tui_only = false,
                handler = function() end,
            })
            maki.api.register_session_option({
                id = "choice.candidate",
                name = "Candidate option",
                description = "Candidate option",
                category = "mode",
                values = {{ value = "on", name = "On" }},
                initial_value = "on",
            })
        "#;
        smol::block_on(async {
            let registry = Arc::new(ToolRegistry::new());
            let host = PluginHost::new(Arc::clone(&registry)).unwrap();
            host.load_source("choice", ORIGINAL).unwrap();
            host.load_source(
                "other",
                r#"
                maki.api.register_tool({
                    name = "shared_tool",
                    description = "Shared tool",
                    schema = { type = "object", properties = {}, additionalProperties = false },
                    handler = function() return "ok" end,
                })
                "#,
            )
            .unwrap();
            let coordinator = test_coordinator(host.event_handle().session_option_catalog());

            assert!(matches!(
                host.load_source("choice", CANDIDATE),
                Err(PluginError::NameConflict { .. })
            ));
            assert!(registry.has("original_tool"));
            assert!(registry.has("shared_tool"));
            let commands = command_snapshot(&host);
            assert!(
                commands
                    .commands()
                    .iter()
                    .any(|command| command.spec().name.as_ref() == "/original")
            );
            assert!(
                commands
                    .commands()
                    .iter()
                    .all(|command| command.spec().name.as_ref() != "/candidate")
            );
            assert!(
                coordinator
                    .read()
                    .options()
                    .options
                    .iter()
                    .any(|option| option.definition.id.as_ref() == "choice.original")
            );
            assert!(
                coordinator
                    .read()
                    .options()
                    .options
                    .iter()
                    .all(|option| option.definition.id.as_ref() != "choice.candidate")
            );
            coordinator.close().await.unwrap();
        });
    }

    #[test]
    fn event_handle_validates_and_commits_session_option() {
        const SOURCE: &str = r#"
            maki.api.register_session_option({
                id = "choice.value",
                name = "Choice",
                description = "Test choice",
                category = "mode",
                values = {
                    { value = "a", name = "A" },
                    { value = "b", name = "B" },
                },
                initial_value = "a",
                validate = function(value) return value == "b", "expected b" end,
            })
        "#;

        smol::block_on(async {
            let host = PluginHost::new(Arc::new(ToolRegistry::new())).unwrap();
            host.load_source("choice", SOURCE).unwrap();
            let handle = host.event_handle();
            let coordinator = test_coordinator(handle.session_option_catalog());

            let snapshot = handle
                .set_session_option(coordinator.clone(), "choice.value", "b")
                .await
                .unwrap();

            assert_eq!(
                snapshot
                    .options
                    .iter()
                    .find(|option| option.definition.id.as_ref() == "choice.value")
                    .unwrap()
                    .current_value
                    .as_ref(),
                "b"
            );
            coordinator.close().await.unwrap();
        });
    }

    #[test]
    fn unload_failure_clears_generic_state_and_can_retry_session_options() {
        const SOURCE: &str = r#"
            maki.api.register_tool({
                name = "choice_tool",
                description = "Test tool",
                schema = { type = "object", properties = {}, additionalProperties = false },
                handler = function() return "ok" end,
            })
            maki.api.register_session_option({
                id = "choice.value",
                name = "Choice",
                description = "Test choice",
                category = "mode",
                values = {
                    { value = "a", name = "A" },
                    { value = "b", name = "B" },
                },
                initial_value = "a",
                validate = function(value)
                    if value == "b" then return false, "validator remains active" end
                    return true
                end,
            })
        "#;
        smol::block_on(async {
            let registry = Arc::new(ToolRegistry::new());
            let host = PluginHost::new(Arc::clone(&registry)).unwrap();
            host.load_source("choice", SOURCE).unwrap();
            let fail = Arc::new(AtomicBool::new(true));
            let checkpoint: Arc<dyn CheckpointWriter<SessionCheckpoint>> = {
                let fail = Arc::clone(&fail);
                Arc::new(move |request: CheckpointRequest<SessionCheckpoint>| {
                    let fail = Arc::clone(&fail);
                    Box::pin(async move {
                        if fail.load(Ordering::Relaxed) {
                            Err(CheckpointError::Save {
                                session_id: request.session_id,
                                message: Arc::from("deterministic unload failure"),
                            })
                        } else {
                            Ok(CheckpointAck {
                                session_id: request.session_id,
                                version: request.version,
                            })
                        }
                    }) as CheckpointFuture
                })
            };
            let coordinator = test_coordinator_with_checkpoint(
                host.event_handle().session_option_catalog(),
                Default::default(),
                checkpoint,
            );
            let error = host.unload("choice").unwrap_err();
            assert!(matches!(error, PluginError::Unload { .. }));
            assert!(!registry.has("choice_tool"));
            assert!(
                coordinator
                    .read()
                    .options()
                    .options
                    .iter()
                    .any(|option| option.definition.id.as_ref() == "choice.value")
            );
            fail.store(false, Ordering::Relaxed);
            host.unload("choice").unwrap();
            assert!(
                coordinator
                    .read()
                    .options()
                    .options
                    .iter()
                    .all(|option| option.definition.id.as_ref() != "choice.value")
            );
            coordinator.close().await.unwrap();
        });
    }

    #[test]
    fn independent_plugin_hosts_do_not_share_session_option_catalogs() {
        const FIRST: &str = r#"
            maki.api.register_session_option({
                id = "choice.value",
                name = "Choice",
                description = "First choice",
                category = "mode",
                values = {{ value = "a", name = "A" }},
                initial_value = "a",
            })
        "#;
        const SECOND: &str = r#"
            maki.api.register_session_option({
                id = "choice.value",
                name = "Choice",
                description = "Second choice",
                category = "mode",
                values = {{ value = "b", name = "B" }},
                initial_value = "b",
            })
        "#;
        smol::block_on(async {
            let first_host = PluginHost::new(Arc::new(ToolRegistry::new())).unwrap();
            let second_host = PluginHost::new(Arc::new(ToolRegistry::new())).unwrap();
            first_host.load_source("choice", FIRST).unwrap();
            second_host.load_source("choice", SECOND).unwrap();

            let first = test_coordinator(first_host.event_handle().session_option_catalog());
            let second = test_coordinator(second_host.event_handle().session_option_catalog());
            let option = |coordinator: &SessionCoordinatorHandle| {
                coordinator
                    .read()
                    .options()
                    .options
                    .iter()
                    .find(|option| option.definition.id.as_ref() == "choice.value")
                    .cloned()
                    .unwrap()
            };
            assert_eq!(option(&first).current_value.as_ref(), "a");
            assert_eq!(
                option(&first).definition.description.as_ref(),
                "First choice"
            );
            assert_eq!(option(&second).current_value.as_ref(), "b");
            assert_eq!(
                option(&second).definition.description.as_ref(),
                "Second choice"
            );

            first.close().await.unwrap();
            second.close().await.unwrap();
        });
    }

    #[test]
    fn bash_auto_mode_persisted_value_wins_over_plugin_default() {
        smol::block_on(async {
            let host = PluginHost::new(Arc::new(ToolRegistry::new())).unwrap();
            let source = BUNDLED_PLUGINS
                .iter()
                .find(|plugin| plugin.name == "bash")
                .and_then(|plugin| plugin.dir.get_file("init.lua"))
                .and_then(|file| file.contents_utf8())
                .unwrap();
            let mut opts = serde_json::Map::new();
            opts.insert("auto_mode".to_string(), serde_json::json!(true));
            host.load_source_with_opts("bash", source, opts).unwrap();

            let coordinator = test_coordinator_with_options(
                host.event_handle().session_option_catalog(),
                BTreeMap::from([("bash.auto_mode".to_string(), "disabled".to_string())]),
            );
            let option = coordinator
                .read()
                .options()
                .options
                .iter()
                .find(|option| option.definition.id.as_ref() == "bash.auto_mode")
                .cloned()
                .unwrap();
            assert_eq!(option.current_value.as_ref(), "disabled");
            coordinator.close().await.unwrap();
        });
    }

    #[test]
    fn begin_shutdown_rejects_later_loads_and_is_idempotent() {
        let mut host = PluginHost::new(Arc::new(ToolRegistry::new())).unwrap();
        host.begin_shutdown();
        assert!(host.load_source("late", "return {}").is_err());
        host.begin_shutdown();
        assert!(host.load_source("later", "return {}").is_err());
    }

    /// Regression for the exit drain in `runtime::spawn`. An `EventHandle`
    /// clone keeps queued requests alive after the Lua thread exits, and
    /// dispatch prefers the priority lane, so a bulk request queued behind
    /// `Shutdown` is never served. Without the drain its reply sender lives
    /// forever and `collect_prompt_slots` blocks; with it, the call falls
    /// back to defaults right away.
    #[test]
    fn live_event_handle_does_not_hang_after_begin_shutdown() {
        let mut host = PluginHost::new(Arc::new(ToolRegistry::new())).unwrap();
        host.load_source(
            "hinted",
            r#"maki.api.register_prompt_hint({ slot = "tool_usage", content = "live" })"#,
        )
        .unwrap();
        let handle = host.event_handle();
        host.begin_shutdown();

        let slots = handle.collect_prompt_slots();
        assert!(
            contents(&slots, PromptId::System, Slot::ToolUsage).is_empty(),
            "dead host must yield defaults, not real slots"
        );

        drop(host);
        let slots = handle.collect_prompt_slots();
        assert!(contents(&slots, PromptId::System, Slot::ToolUsage).is_empty());
    }

    /// Load `src` as one plugin, collect resolved slots.
    /// Panics on failure; use `load_err` to inspect errors.
    fn slots_from(plugin: &str, src: &str) -> (PluginHost, ResolvedSlots) {
        let host = PluginHost::new(Arc::new(ToolRegistry::new())).unwrap();
        host.load_source(plugin, src).unwrap();
        let slots = host.event_handle().collect_prompt_slots();
        (host, slots)
    }

    fn contents(slots: &ResolvedSlots, prompt: PromptId, slot: Slot) -> Vec<&str> {
        slots
            .get(prompt, slot)
            .iter()
            .map(|e| e.content.as_str())
            .collect()
    }

    #[test]
    fn memory_builtin_registers_command() {
        let reg = Arc::new(ToolRegistry::new());
        let host = PluginHost::with_all_builtins(Arc::clone(&reg)).unwrap();
        let snap = command_snapshot(&host);
        let found = snap
            .commands()
            .iter()
            .any(|c| c.spec().name.as_ref() == "/memory");
        assert!(
            found,
            "Expected /memory command, found: {:?}",
            snap.commands()
                .iter()
                .map(|c| c.spec().name.as_ref())
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn bundled_read_only_identity_follows_loader_and_replacement() {
        let registry = Arc::new(ToolRegistry::new());
        let mut host = PluginHost::new(Arc::clone(&registry)).unwrap();
        host.load_builtins(&PluginsConfig {
            enabled: true,
            names: vec!["read".into(), "glob".into(), "grep".into()],
            opts: HashMap::new(),
        })
        .unwrap();
        for name in ["read", "glob", "grep"] {
            assert!(registry.get(name).unwrap().is_bundled_read_only());
        }
        host.load_source(
            "read",
            r#"maki.api.register_tool({name = "read", description = "probe", schema = {type = "object", properties = {}}, handler = function() return "ok" end})"#,
        )
        .unwrap();
        assert!(!registry.get("read").unwrap().is_bundled_read_only());
        for name in ["glob", "grep"] {
            assert!(registry.get(name).unwrap().is_bundled_read_only());
        }
    }

    #[test]
    fn usage_builtin_registers_command() {
        let reg = Arc::new(ToolRegistry::new());
        let mut host = PluginHost::new(Arc::clone(&reg)).unwrap();
        host.load_builtins(&PluginsConfig {
            enabled: true,
            names: vec!["usage".into()],
            opts: HashMap::new(),
        })
        .unwrap();
        assert!(
            command_snapshot(&host)
                .commands()
                .iter()
                .any(|command| command.spec().name.as_ref() == "/usage")
        );
    }

    #[test]
    fn run_command_sends_correct_request() {
        let (prio_tx, prio_rx) = flume::bounded(8);
        let (tx, _rx) = flume::bounded(8);
        let handle = EventHandle {
            tx,
            shutdown: Arc::default(),
            prio_tx: prio_tx.clone(),
            modes: Arc::new(maki_agent::ModeRegistry::builtin()),
            session_options: SessionOptionCatalog::default(),
            completion: None,
            command_generations: None,
            command_arguments: CoalescedLatest::new(|_| false),
            command_argument_lifecycle: CoalescedLatest::new(|_| false),
            splash_frames: CoalescedLatest::new(move |work| {
                prio_tx.send(Request::SplashFrame(work)).is_ok()
            }),
        };
        handle.run_command(
            Arc::from("myplugin"),
            Arc::from("/greet"),
            "world".into(),
            2,
        );
        let req = prio_rx.try_recv().unwrap();
        match req {
            Request::RunCommand {
                plugin,
                command,
                generation,
                args,
                depth,
                completion,
            } => {
                assert_eq!(plugin.as_ref(), "myplugin");
                assert_eq!(generation, 0);
                assert_eq!(command.as_ref(), "/greet");
                assert_eq!(args, "world");
                assert_eq!(depth, 2);
                assert!(completion.is_none());
            }
            _ => panic!("expected RunCommand"),
        }
    }

    #[test]
    fn command_argument_requests_keep_only_latest_pending() {
        let (tx, rx) = flume::unbounded();
        let handle = EventHandle::probed_for_test(tx);
        let request = |arg: &str| {
            handle.collect_command_argument_items(
                CommandArgumentContext {
                    command: Arc::from("/deploy"),
                    plugin: Arc::from("deploy"),
                    args: format!("/deploy {arg}"),
                    arg: arg.to_string(),
                    index: 0,
                    mode: "build".to_string(),
                    session: 1,
                    generation: 1,
                    command_generation: 0,
                    argument_name: None,
                    argument_kind: None,
                    preceding_arguments: Arc::from([]),
                },
                maki_agent::CancelToken::none(),
            )
        };

        let first_reply = request("a").unwrap();
        let stale_reply = request("ab").unwrap();
        let latest_reply = request("abc").unwrap();
        let active = match rx.recv().unwrap() {
            Request::CollectCommandArgumentItems(work) => work,
            _ => panic!("expected command argument request"),
        };
        active.finish(|request| {
            let _ = request.reply.send(Vec::new());
        });
        let pending = match rx.recv().unwrap() {
            Request::CollectCommandArgumentItems(work) => work,
            _ => panic!("expected command argument request"),
        };
        assert_eq!(pending.value().context.arg, "abc");
        pending.finish(|request| {
            let _ = request.reply.send(Vec::new());
        });

        assert!(first_reply.recv().is_err());
        assert!(stale_reply.recv().is_err());
        assert_eq!(latest_reply.recv().unwrap(), Vec::new());
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn splash_requests_coalesce_across_handle_clones() {
        let (tx, rx) = flume::unbounded();
        let handle = EventHandle::probed_for_test(tx);
        let clone = handle.clone();
        let request = |width| {
            let (reply, wake) = flume::bounded(1);
            (
                SplashFrameRequest {
                    width,
                    height: 5,
                    elapsed_secs: 1.0,
                    fade: 0.0,
                    reply,
                },
                wake,
            )
        };
        let (first, first_wake) = request(10);
        let (stale, stale_wake) = request(20);
        let (latest, latest_wake) = request(30);

        assert!(handle.splash_frames.submit(first));
        assert!(clone.splash_frames.submit(stale));
        assert!(handle.splash_frames.submit(latest));
        let active = match rx.recv().unwrap() {
            Request::SplashFrame(work) => work,
            _ => panic!("expected splash frame"),
        };
        active.finish(|request| {
            let _ = request.reply.send(None);
        });
        let pending = match rx.recv().unwrap() {
            Request::SplashFrame(work) => work,
            _ => panic!("expected splash frame"),
        };
        assert_eq!(pending.value().width, 30);
        pending.finish(|request| {
            let _ = request.reply.send(None);
        });

        assert!(first_wake.recv().is_err());
        assert!(stale_wake.recv().is_err());
        assert!(latest_wake.recv().unwrap().is_none());
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn splash_pull_wakes_unknown_when_transport_closes() {
        let (tx, rx) = flume::unbounded();
        let handle = EventHandle::probed_for_test(tx);
        let pull = thread::spawn(move || handle.splash_pull(10, 5, 1.0, 0.0));
        let work = match rx.recv().unwrap() {
            Request::SplashFrame(work) => work,
            _ => panic!("expected splash frame"),
        };

        drop(work);
        assert!(matches!(pull.join().unwrap(), SplashPull::Unknown));
    }

    #[test]
    fn multiple_plugins_register_independent_commands() {
        let reg = Arc::new(ToolRegistry::new());
        let host = PluginHost::new(Arc::clone(&reg)).unwrap();
        host.load_source(
            "plugin_a",
            r#"
            maki.api.register_command({
                name = "/alpha",
                description = "from a",
                tui_only = false,
                handler = function() end,
            })
            "#,
        )
        .unwrap();
        host.load_source(
            "plugin_b",
            r#"
            maki.api.register_command({
                name = "/beta",
                description = "from b",
                tui_only = false,
                handler = function() end,
            })
            "#,
        )
        .unwrap();

        let snap = command_snapshot(&host);
        assert_eq!(snap.commands().len(), 2);
        let names: Vec<&str> = snap
            .commands()
            .iter()
            .map(|c| c.spec().name.as_ref())
            .collect();
        assert!(names.contains(&"/alpha"));
        assert!(names.contains(&"/beta"));
    }

    #[test]
    fn register_command_adds_missing_leading_slash() {
        let host = PluginHost::new(Arc::new(ToolRegistry::new())).unwrap();
        host.load_source(
            "noslash",
            r#"
            maki.api.register_command({
                name = "hello",
                description = "no slash",
                tui_only = false,
                handler = function() end,
            })
            "#,
        )
        .unwrap();

        let snap = command_snapshot(&host);
        assert_eq!(snap.commands().len(), 1);
        assert_eq!(snap.commands()[0].spec().name.as_ref(), "/hello");
    }

    #[test]
    fn provider_usage_publication_updates_mirror_before_callback_and_unload_cleans_it() {
        use crate::{ProviderUsageSnapshot, UiAction};

        let host = PluginHost::new(Arc::new(ToolRegistry::new())).unwrap();
        host.load_source(
            "usage_observer",
            r#"
            maki.usage.on_change(function(snapshot)
                local current = maki.usage.get()
                if current.provider_id == snapshot.provider_id
                    and current.status == snapshot.status then
                    maki.ui.flash("usage:" .. current.status)
                end
            end)
            "#,
        )
        .unwrap();
        let handle = host.event_handle();
        let actions = host.ui_action_rx();
        let snapshot = ProviderUsageSnapshot {
            provider_id: "anthropic".into(),
            provider: "Anthropic".into(),
            model: "claude-sonnet-4-5".into(),
            status: "unsupported".into(),
            limits: Vec::new(),
            plan: None,
            error: None,
        };

        assert!(
            handle.provider_usage_changed(
                snapshot.clone(),
                Some(crate::ProviderUsageInvalidation(1)),
            )
        );
        assert!(matches!(
            actions.recv_timeout(Duration::from_secs(1)),
            Ok(UiAction::Flash(message)) if message == "usage:unsupported"
        ));
        assert!(matches!(
            actions.recv_timeout(Duration::from_secs(1)),
            Ok(UiAction::ProviderUsageAck(crate::ProviderUsageAck {
                invalidation: crate::ProviderUsageInvalidation(1),
                ..
            }))
        ));

        host.unload("usage_observer").unwrap();
        assert!(
            handle.provider_usage_changed(snapshot, Some(crate::ProviderUsageInvalidation(2)),)
        );
        assert!(
            matches!(
                actions.recv_timeout(Duration::from_secs(1)),
                Ok(UiAction::ProviderUsageAck(crate::ProviderUsageAck {
                    invalidation: crate::ProviderUsageInvalidation(2),
                    ..
                }))
            ),
            "post-unload invalidation must ack without a Flash from the unloaded callback"
        );
    }

    /// End-to-end: a plugin registers a keymap override, the override is published
    /// to the snapshot, EventHandle::run_keybind_callback dispatches the request,
    /// the runtime resolves the Function by id from the registry, and the callback
    /// executes with an observable side effect. This is the load-bearing path the
    /// dispatch reorder and the dead-host fallback rest on; unit tests only cover
    /// the layers in isolation.
    #[test]
    fn keybind_callback_runs_end_to_end() {
        let host = PluginHost::new(Arc::new(ToolRegistry::new())).unwrap();
        host.load_source(
            "kb",
            r#"
            maki.keymap.set("n", "<C-g>", function()
                maki.api.register_command({
                    name = "/fired",
                    description = "callback ran",
                    tui_only = false,
                    handler = function() end,
                })
            end, { desc = "test override" })
            "#,
        )
        .unwrap();

        let snap = host.keymap_reader().load();
        assert_eq!(snap.entries.len(), 1, "override published to snapshot");
        let entry = &snap.entries[0];
        assert_eq!(entry.desc, "test override");
        assert!(
            command_snapshot(&host).commands().is_empty(),
            "callback has not fired yet"
        );

        let handle = host.event_handle();
        handle.run_keybind_callback(entry.id);

        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            let snapshot = command_snapshot(&host);
            if snapshot
                .commands()
                .iter()
                .any(|c| c.spec().name.as_ref() == "/fired")
            {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "keybind callback did not register /fired within 2s"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    #[test]
    fn slots_prompts_and_status_hints_replace_transactionally() {
        const OLD: &str = r#"
            maki.api.declare_slot("transaction.slot", function() return "old slot" end)
            maki.api.set_prompt({ slot = "identity", content = function() return "old prompt" end })
            maki.ui.set_status_hint({ { "o", "old hint" } })
        "#;
        const FAILED: &str = r#"
            maki.api.declare_slot("transaction.slot", function() return "candidate slot" end)
            maki.api.set_prompt({ slot = "identity", content = function() return "candidate prompt" end })
            maki.ui.set_status_hint({ { "c", "candidate hint" } })
            error("reject candidate")
        "#;

        let host = PluginHost::new(Arc::new(ToolRegistry::new())).unwrap();
        host.load_source("transaction", OLD).unwrap();
        assert!(host.load_source("transaction", FAILED).is_err());

        let slots = host.event_handle().collect_prompt_slots();
        assert_eq!(
            contents(&slots, PromptId::System, Slot::Identity),
            ["old prompt"]
        );
        let hints = host.hint_reader().load_full();
        assert_eq!(hints.entries.len(), 1);
        assert_eq!(hints.entries[0].1, vec![("o".into(), "old hint".into())]);

        host.load_source("transaction", "return true").unwrap();
        let slots = host.event_handle().collect_prompt_slots();
        assert!(contents(&slots, PromptId::System, Slot::Identity).is_empty());
        assert!(host.hint_reader().load_full().entries.is_empty());
    }

    #[test]
    fn keymap_deletion_is_transactional_across_owners() {
        const BINDING: &str = r#"
            maki.keymap.set("n", "<C-p>", function() end, { desc = "bundled mapping" })
        "#;
        const FAILED_DELETE: &str = r#"
            maki.keymap.del("n", "<C-p>")
            error("reject deletion")
        "#;

        let host = PluginHost::new(Arc::new(ToolRegistry::new())).unwrap();
        host.load_source("sessions", BINDING).unwrap();
        assert_eq!(
            host.keymap_reader().load().entries[0].plugin.as_ref(),
            "sessions"
        );

        assert!(host.load_source("user", FAILED_DELETE).is_err());
        let keymaps = host.keymap_reader().load();
        assert_eq!(keymaps.entries.len(), 1);
        assert_eq!(keymaps.entries[0].plugin.as_ref(), "sessions");
        drop(keymaps);

        host.load_source("user", r#"maki.keymap.del("n", "<C-p>")"#)
            .unwrap();
        assert!(host.keymap_reader().load().entries.is_empty());
    }

    #[test]
    fn command_and_keymap_replacement_is_transactional() {
        const OLD: &str = r#"
            maki.api.register_command({
                name = "/choice",
                description = "old command",
                tui_only = false,
                handler = function() end,
            })
            maki.keymap.set("n", "<C-g>", function()
                maki.api.register_command({
                    name = "/old_callback",
                    description = "old callback",
                    tui_only = false,
                    handler = function() end,
                })
            end, { desc = "old keymap" })
        "#;
        const FAILED: &str = r#"
            maki.api.register_command({
                name = "/choice",
                description = "candidate command",
                tui_only = false,
                handler = function() end,
            })
            maki.keymap.set("n", "<C-g>", function() error("candidate callback") end,
                { desc = "candidate keymap" })
            error("reject candidate")
        "#;

        let host = PluginHost::new(Arc::new(ToolRegistry::new())).unwrap();
        host.load_source("transaction", OLD).unwrap();
        assert!(host.load_source("transaction", FAILED).is_err());

        let commands = command_snapshot(&host);
        assert!(
            commands
                .commands()
                .iter()
                .any(|command| command.spec().name.as_ref() == "/choice")
        );
        let keymaps = host.keymap_reader().load();
        assert_eq!(keymaps.entries.len(), 1);
        assert_eq!(keymaps.entries[0].desc, "old keymap");
        host.event_handle()
            .run_keybind_callback(keymaps.entries[0].id);

        let deadline = Instant::now() + Duration::from_secs(2);
        while !command_snapshot(&host)
            .commands()
            .iter()
            .any(|command| command.spec().name.as_ref() == "/old_callback")
        {
            assert!(Instant::now() < deadline, "old keymap callback was lost");
            std::thread::sleep(Duration::from_millis(10));
        }

        host.load_source("transaction", "return true").unwrap();
        assert!(
            command_snapshot(&host)
                .commands()
                .iter()
                .all(|command| command.spec().name.as_ref() != "/choice")
        );
        assert!(host.keymap_reader().load().entries.is_empty());
    }

    #[test]
    fn autocmd_failed_replacement_keeps_old_listener() {
        const OLD: &str = r#"
            maki.api.create_autocmd("Replacement", { callback = function()
                maki.api.register_command({
                    name = "/old-fired",
                    description = "old",
                    tui_only = false,
                    handler = function() end,
                })
            end })
        "#;
        const FAILED: &str = r#"
            maki.api.create_autocmd("Replacement", { callback = function()
                maki.api.register_command({
                    name = "/new-fired",
                    description = "new",
                    tui_only = false,
                    handler = function() end,
                })
            end })
            error("reject replacement")
        "#;

        let host = PluginHost::new(Arc::new(ToolRegistry::new())).unwrap();
        host.load_source("replacement", OLD).unwrap();
        assert!(host.load_source("replacement", FAILED).is_err());
        host.event_handle()
            .fire_autocmd("Replacement", serde_json::json!({}));

        let deadline = Instant::now() + Duration::from_secs(2);
        while !command_snapshot(&host)
            .commands()
            .iter()
            .any(|command| command.spec().name.as_ref() == "/old-fired")
        {
            assert!(Instant::now() < deadline, "old autocmd listener was lost");
            thread::sleep(Duration::from_millis(10));
        }
        assert!(
            command_snapshot(&host)
                .commands()
                .iter()
                .all(|command| command.spec().name.as_ref() != "/new-fired")
        );
    }

    #[test]
    fn autocmd_successful_replacement_omits_old_listener_and_keeps_others() {
        const LISTENER: &str = r#"
            maki.api.create_autocmd("Replacement", { callback = function()
                maki.api.register_command({
                    name = "/retained-fired",
                    description = "retained",
                    tui_only = false,
                    handler = function() end,
                })
            end })
        "#;
        const OLD: &str = r#"
            maki.api.create_autocmd("Replacement", { callback = function()
                maki.api.register_command({
                    name = "/omitted-fired",
                    description = "omitted",
                    tui_only = false,
                    handler = function() end,
                })
            end })
        "#;

        let host = PluginHost::new(Arc::new(ToolRegistry::new())).unwrap();
        host.load_source("retained", LISTENER).unwrap();
        host.load_source("replacement", OLD).unwrap();
        host.load_source("replacement", "return true").unwrap();
        host.event_handle()
            .fire_autocmd("Replacement", serde_json::json!({}));

        let deadline = Instant::now() + Duration::from_secs(2);
        while !command_snapshot(&host)
            .commands()
            .iter()
            .any(|command| command.spec().name.as_ref() == "/retained-fired")
        {
            assert!(
                Instant::now() < deadline,
                "retained autocmd listener was lost"
            );
            thread::sleep(Duration::from_millis(10));
        }
        assert!(
            command_snapshot(&host)
                .commands()
                .iter()
                .all(|command| command.spec().name.as_ref() != "/omitted-fired")
        );
    }

    #[test]
    fn timer_failed_load_never_fires_candidate_callback() {
        let host = PluginHost::new(Arc::new(ToolRegistry::new())).unwrap();
        assert!(
            host.load_source(
                "timer_replacement",
                r#"
                    maki.timer.set(0.001, function()
                        maki.api.register_command({
                            name = "/candidate-timer-fired",
                            description = "candidate",
                            tui_only = false,
                            handler = function() end,
                        })
                    end)
                    error("reject timer")
                "#,
            )
            .is_err()
        );
        thread::sleep(Duration::from_millis(50));
        assert!(
            command_snapshot(&host)
                .commands()
                .iter()
                .all(|command| command.spec().name.as_ref() != "/candidate-timer-fired")
        );
    }

    #[test]
    fn committed_timer_keeps_id_and_successful_omission_stops_it() {
        let host = PluginHost::new(Arc::new(ToolRegistry::new())).unwrap();
        host.load_source(
            "timer_replacement",
            r#"
                local count = 0
                maki.timer.set(0.001, function(id)
                    count = count + 1
                    maki.api.register_command({
                        name = "/committed-timer-" .. tostring(count),
                        description = "committed timer",
                        tui_only = false,
                        handler = function() end,
                    })
                    maki.timer.del(id)
                end)
            "#,
        )
        .unwrap();
        let deadline = Instant::now() + Duration::from_secs(2);
        while command_snapshot(&host)
            .commands()
            .iter()
            .all(|command| command.spec().name.as_ref() != "/committed-timer-1")
        {
            assert!(Instant::now() < deadline, "committed timer did not fire");
            thread::sleep(Duration::from_millis(10));
        }
        thread::sleep(Duration::from_millis(20));
        assert!(
            command_snapshot(&host)
                .commands()
                .iter()
                .all(|command| command.spec().name.as_ref() != "/committed-timer-2"),
            "callback timer id did not stop the committed timer"
        );

        host.load_source("timer_replacement", "return true")
            .unwrap();
        assert!(
            command_snapshot(&host)
                .commands()
                .iter()
                .all(|command| !command.spec().name.starts_with("/committed-timer-"))
        );
    }

    #[test_case("register_prompt_hint", r#"{ slot = "tool_usage", content = "late" }"#)]
    #[test_case("set_prompt", r#"{ slot = "identity", content = "late" }"#)]
    #[test_case(
        "register_options",
        r#"{ enabled = { default = true, desc = "Enabled" } }"#
    )]
    #[test_case(
        "register_session_option",
        r#"{ id = "late.value", name = "Late", description = "Late", category = "mode", values = {{ value = "a", name = "A" }}, initial_value = "a" }"#
    )]
    fn load_only_registration_apis_reject_runtime_calls(api: &str, spec: &str) {
        let host = PluginHost::new(Arc::new(ToolRegistry::new())).unwrap();
        host.load_source(
            "late",
            &format!(
                r#"maki.api.register_command({{
                    name = "/late",
                    description = "late registration",
                    tui_only = false,
                    handler = function() maki.api.{api}({spec}) end,
                }})"#
            ),
        )
        .unwrap();

        let result = host
            .event_handle()
            .run_command_for_test(Arc::from("late"), Arc::from("/late"), String::new(), 0)
            .recv()
            .unwrap();
        let error = result.unwrap_err();
        assert!(error.contains("may only be called at the top level during plugin load"));
    }

    #[test]
    fn complete_plugin_replacement_is_transactional() {
        const OLD: &str = r#"
            maki.api.register_command({ name = "/old", description = "old", tui_only = false, handler = function() end })
            maki.keymap.set("n", "<C-o>", function() maki.api.register_command({ name = "/old-key", description = "old", tui_only = false, handler = function() end }) end, { desc = "old key" })
            maki.store.register("transaction", "old", { value = "old" })
            maki.api.register_options({ old = { type = "string", desc = "old" } })
            maki.api.register_completion_source("old", { get_items = function() return {{ label = "old", kind = "old", insertion = "@old:x" }} end })
            maki.api.register_expander("old", function(ref) return "<old:" .. ref.value .. ">", nil end)
            maki.api.declare_slot("transaction.slot", function() return "old slot" end)
            maki.api.set_prompt({ slot = "identity", content = function() return "old prompt" end })
            maki.ui.set_status_hint({ { "o", "old hint" } })
            maki.api.create_autocmd("TransactionProbe", { callback = function()
                local entries = maki.store.collect("transaction")
                if entries.old then maki.api.register_command({ name = "/old-store", description = "old", tui_only = false, handler = function() end }) end
                if entries.candidate then maki.api.register_command({ name = "/candidate-store", description = "candidate", tui_only = false, handler = function() end }) end
            end })
            maki.timer.set(0.001, function(id)
                maki.api.register_command({ name = "/old-timer", description = "old", tui_only = false, handler = function() end })
                maki.timer.del(id)
            end)
            maki.api.register_session_option({ id = "transaction.old", name = "Old", description = "old", category = "mode", values = {{ value = "yes", name = "Yes" }}, initial_value = "yes", persistent = true })
        "#;
        const FAILED: &str = r#"
            maki.api.register_command({ name = "/candidate", description = "candidate", tui_only = false, handler = function() end })
            maki.keymap.set("n", "<C-o>", function() maki.api.register_command({ name = "/candidate-key", description = "candidate", tui_only = false, handler = function() end }) end, { desc = "candidate key" })
            maki.store.register("transaction", "candidate", { value = "candidate" })
            maki.api.register_options({ candidate = { type = "string", desc = "candidate" } })
            maki.api.register_completion_source("candidate", { get_items = function() return {{ label = "candidate", kind = "candidate", insertion = "@candidate:x" }} end })
            maki.api.register_expander("candidate", function(ref) return "<candidate:" .. ref.value .. ">", nil end)
            maki.api.declare_slot("transaction.slot", function() return "candidate slot" end)
            maki.api.set_prompt({ slot = "identity", content = function() return "candidate prompt" end })
            maki.ui.set_status_hint({ { "c", "candidate hint" } })
            maki.api.create_autocmd("TransactionProbe", { callback = function() maki.api.register_command({ name = "/candidate-autocmd", description = "candidate", tui_only = false, handler = function() end }) end })
            maki.timer.set(0.001, function() maki.api.register_command({ name = "/candidate-timer", description = "candidate", tui_only = false, handler = function() end }) end)
            maki.api.register_session_option({ id = "transaction.candidate", name = "Candidate", description = "candidate", category = "mode", values = {{ value = "no", name = "No" }}, initial_value = "no", persistent = true })
            error("reject candidate")
        "#;

        smol::block_on(async {
            let host = PluginHost::new(Arc::new(ToolRegistry::new())).unwrap();
            host.load_source("transaction", OLD).unwrap();
            assert!(host.load_source("transaction", FAILED).is_err());

            let commands = command_snapshot(&host);
            assert!(
                commands
                    .commands()
                    .iter()
                    .any(|c| c.spec().name.as_ref() == "/old")
            );
            assert!(
                commands
                    .commands()
                    .iter()
                    .all(|c| c.spec().name.as_ref() != "/candidate")
            );
            let keymaps = host.keymap_reader().load();
            assert_eq!(keymaps.entries[0].desc, "old key");
            let slots = host.event_handle().collect_prompt_slots();
            assert_eq!(
                contents(&slots, PromptId::System, Slot::Identity),
                ["old prompt"]
            );
            assert_eq!(
                host.hint_reader().load_full().entries[0].1,
                vec![("o".into(), "old hint".into())]
            );
            let handle = host.event_handle();
            assert_eq!(
                handle.collect_completion_items(&CompletionCtx::default())[0].label,
                "old"
            );
            assert_eq!(handle.expand_references("@old:x").unwrap(), "<old:x>");
            handle.fire_autocmd("TransactionProbe", serde_json::json!({}));
            let deadline = Instant::now() + Duration::from_secs(2);
            while !command_snapshot(&host).commands().iter().any(|c| {
                c.spec().name.as_ref() == "/old-store" || c.spec().name.as_ref() == "/old-timer"
            }) {
                assert!(Instant::now() < deadline, "old callbacks did not fire");
                thread::sleep(Duration::from_millis(10));
            }
            let commands = command_snapshot(&host);
            assert!(
                commands
                    .commands()
                    .iter()
                    .all(|c| !c.spec().name.starts_with("/candidate-"))
            );
            assert!(
                commands
                    .commands()
                    .iter()
                    .all(|c| c.spec().name.as_ref() != "/candidate-store")
            );
            let options = host.plugin_options().unwrap();
            assert!(
                options[&Arc::from("transaction")]
                    .iter()
                    .any(|s| s.name == "old")
            );
            let coordinator = test_coordinator(host.event_handle().session_option_catalog());
            assert!(
                coordinator
                    .read()
                    .options()
                    .options
                    .iter()
                    .any(|o| o.definition.id.as_ref() == "transaction.old")
            );
            coordinator.close().await.unwrap();

            host.load_source("transaction", "return true").unwrap();
            assert!(
                command_snapshot(&host)
                    .commands()
                    .iter()
                    .all(|c| c.spec().name.as_ref() != "/old")
            );
            assert!(host.keymap_reader().load().entries.is_empty());
            assert!(
                contents(
                    &host.event_handle().collect_prompt_slots(),
                    PromptId::System,
                    Slot::Identity
                )
                .is_empty()
            );
            assert!(host.hint_reader().load_full().entries.is_empty());
            assert!(
                host.event_handle()
                    .collect_completion_items(&CompletionCtx::default())
                    .iter()
                    .all(|i| i.label != "old")
            );
            assert_eq!(
                host.event_handle().expand_references("@old:x").unwrap(),
                "@old:x"
            );
            assert!(
                !host
                    .plugin_options()
                    .unwrap()
                    .contains_key(&Arc::from("transaction"))
            );
        });
    }

    #[test]
    fn options_and_completion_replacement_is_transactional() {
        let host = PluginHost::new(Arc::new(ToolRegistry::new())).unwrap();
        host.load_source(
            "transaction",
            r#"
            maki.api.register_options({ old = { type = "string", desc = "old" } })
            maki.api.register_completion_source("old", { get_items = function() return {} end })
            maki.api.register_expander("old", function() return "old", nil end)
        "#,
        )
        .unwrap();
        assert!(
            host.load_source(
                "transaction",
                r#"
            maki.api.register_options({ bad = { type = "string", desc = "bad" } })
            maki.api.register_completion_source("bad", { get_items = function() return {} end })
            error("reject")
        "#
            )
            .is_err()
        );
        assert!(
            host.plugin_options().unwrap()[&Arc::from("transaction")]
                .iter()
                .any(|s| s.name == "old")
        );
        assert!(host.event_handle().expand_references("@old:x").is_ok());
        host.load_source(
            "transaction",
            r#"
            maki.api.register_options({ new = { type = "string", desc = "new" } })
            maki.api.register_completion_source("new", { get_items = function() return {} end })
        "#,
        )
        .unwrap();
        let options = host.plugin_options().unwrap();
        assert!(
            options[&Arc::from("transaction")]
                .iter()
                .all(|s| s.name == "new")
        );
        assert!(
            host.event_handle()
                .expand_references("@old:x")
                .unwrap()
                .contains("@old:x")
        );
    }

    /// `load_init_files_or_skip` is the single seam every entry point
    /// (TUI, index, acp, prompt) uses to honor `--no-plugins`. Verify both
    /// halves: the flag skips a broken init.lua, and absence runs it (so
    /// the skip path is not a tautology that hides a regression in the
    /// unconditional loader).
    #[test]
    fn load_init_files_or_skip_respects_flag() {
        let dir = tempfile::tempdir().unwrap();
        fs::create_dir_all(dir.path().join(".makima")).unwrap();
        fs::write(
            dir.path().join(".makima/init.lua"),
            "error('broken init lua must not run')",
        )
        .unwrap();

        let host = PluginHost::new(Arc::new(ToolRegistry::new())).unwrap();
        let mut warnings = Vec::new();
        let project_config = ProjectConfig::for_project(dir.path());

        let skipped = host
            .load_init_files_or_skip(true, &project_config, &mut warnings)
            .expect("no-plugins skips broken init.lua");
        assert!(
            skipped.is_none(),
            "--no-plugins must skip user init.lua entirely"
        );

        let ran = host.load_init_files_or_skip(false, &project_config, &mut warnings);
        assert!(
            ran.is_err(),
            "without --no-plugins the broken init.lua must surface as an error"
        );
    }

    #[test]
    fn callback_string_lands_in_targeted_prompt_only() {
        let (_host, slots) = slots_from(
            "cb",
            r#"
            maki.api.register_prompt_hint({
                slot = "tool_usage",
                prompt = "general",
                content = function() return "from_cb" end,
            })
            "#,
        );
        assert_eq!(
            contents(&slots, PromptId::General, Slot::ToolUsage),
            ["from_cb"]
        );
        assert!(contents(&slots, PromptId::System, Slot::ToolUsage).is_empty());
    }

    #[test]
    fn callback_returning_nil_contributes_nothing() {
        let (_host, slots) = slots_from(
            "nil_cb",
            r#"
            maki.api.register_prompt_hint({
                slot = "tool_usage",
                content = function() return nil end,
            })
            "#,
        );
        assert!(contents(&slots, PromptId::System, Slot::ToolUsage).is_empty());
    }

    /// A hint with no `prompt` is a default: it lands on every prompt that has the slot.
    #[test]
    fn static_no_prompt_lands_on_all_prompts_with_slot() {
        let (_host, slots) = slots_from(
            "static_hint",
            r#"
            maki.api.register_prompt_hint({
                slot = "efficient_tools",
                content = "index",
            })
            "#,
        );
        for &pid in PromptId::ALL {
            assert_eq!(contents(&slots, pid, Slot::EfficientTools), ["index"]);
        }
    }

    /// `conventions` lives on system and general but not research, so a default
    /// hint follows the slot and skips research.
    #[test]
    fn default_hint_skips_prompts_lacking_the_slot() {
        let (_host, slots) = slots_from(
            "conv",
            r#"
            maki.api.register_prompt_hint({
                slot = "conventions",
                content = "follow conventions",
            })
            "#,
        );
        for pid in [PromptId::System, PromptId::General] {
            assert_eq!(
                contents(&slots, pid, Slot::Conventions),
                ["follow conventions"]
            );
        }
        assert!(contents(&slots, PromptId::Research, Slot::Conventions).is_empty());
    }

    /// Targeting a prompt that does not have the slot quietly drops the hint.
    #[test]
    fn register_prompt_hint_rejects_incompatible_slot_prompt() {
        let host = PluginHost::new(Arc::new(ToolRegistry::new())).unwrap();
        let r = host.load_source(
            "drop",
            r#"
            maki.api.register_prompt_hint({
                slot = "after_instructions",
                prompt = "research",
                content = "never lands",
            })
            "#,
        );
        assert!(r.is_err());
        assert!(r.unwrap_err().to_string().contains("not available"));
    }

    #[test]
    fn prompt_list_targets_each_listed_prompt() {
        const CONTENT: &str = "shared";
        let (_host, slots) = slots_from(
            "list",
            r#"
            maki.api.register_prompt_hint({
                slot = "tool_usage",
                prompt = { "system", "research" },
                content = "shared",
            })
            "#,
        );
        assert_eq!(
            contents(&slots, PromptId::System, Slot::ToolUsage),
            [CONTENT]
        );
        assert_eq!(
            contents(&slots, PromptId::Research, Slot::ToolUsage),
            [CONTENT]
        );
        assert!(contents(&slots, PromptId::General, Slot::ToolUsage).is_empty());
    }

    #[test]
    fn multiple_plugins_sorted_by_plugin_name() {
        let host = PluginHost::new(Arc::new(ToolRegistry::new())).unwrap();
        for plugin in ["zzz", "aaa"] {
            host.load_source(
                plugin,
                r#"
                maki.api.register_prompt_hint({ slot = "tool_usage", content = "from_PLUGIN" })
                "#
                .replace("PLUGIN", plugin)
                .as_str(),
            )
            .unwrap();
        }
        let slots = host.event_handle().collect_prompt_slots();
        assert_eq!(
            contents(&slots, PromptId::System, Slot::ToolUsage),
            ["from_aaa", "from_zzz"],
            "entries must be ordered by plugin name"
        );
    }

    /// One plugin can register several hints; unloading it clears all of them.
    #[test]
    fn unload_clears_all_hints_from_plugin() {
        let host = PluginHost::new(Arc::new(ToolRegistry::new())).unwrap();
        host.load_source(
            "multi",
            r#"
            maki.api.register_prompt_hint({ slot = "tool_usage", prompt = "system", content = "usage" })
            maki.api.register_prompt_hint({ slot = "conventions", prompt = "system", content = "conv" })
            "#,
        )
        .unwrap();
        let handle = host.event_handle();

        let slots = handle.collect_prompt_slots();
        assert_eq!(
            contents(&slots, PromptId::System, Slot::ToolUsage),
            ["usage"]
        );
        assert_eq!(
            contents(&slots, PromptId::System, Slot::Conventions),
            ["conv"]
        );

        host.unload("multi").unwrap();
        let slots = handle.collect_prompt_slots();
        assert!(contents(&slots, PromptId::System, Slot::ToolUsage).is_empty());
        assert!(contents(&slots, PromptId::System, Slot::Conventions).is_empty());
    }

    #[test_case(r#"{ slot = "nonexistent", content = "x" }"# ; "invalid_slot")]
    #[test_case(r#"{ slot = "tool_usage", content = "x", prompt = "nope" }"# ; "invalid_prompt")]
    #[test_case(r#"{ slot = "tool_usage", content = "x", prompt = { "system", "bogus" } }"# ; "invalid_prompt_in_list")]
    #[test_case(r#"{ slot = "tool_usage" }"# ; "missing_content")]
    #[test_case(r#"{ content = "x" }"# ; "missing_slot")]
    #[test_case(r#"{ slot = "tool_usage", content = 42 }"# ; "content_wrong_type")]
    #[test_case(r#"{ slot = "tool_usage", content = "x", prompt = 42 }"# ; "prompt_wrong_type")]
    fn invalid_hint_spec_is_rejected(spec: &str) {
        let host = PluginHost::new(Arc::new(ToolRegistry::new())).unwrap();
        let src = format!("maki.api.register_prompt_hint({spec})");
        assert!(host.load_source("bad", &src).is_err());
    }

    #[test]
    fn identity_slot_lands_on_system_only() {
        let (_host, slots) = slots_from(
            "id",
            r#"
            maki.api.set_prompt({
                slot = "identity",
                content = "Custom identity",
            })
            "#,
        );
        assert_eq!(
            contents(&slots, PromptId::System, Slot::Identity),
            ["Custom identity"]
        );
        assert!(contents(&slots, PromptId::Research, Slot::Identity).is_empty());
        assert!(contents(&slots, PromptId::General, Slot::Identity).is_empty());
    }

    #[test]
    fn tone_slot_lands_on_system_only() {
        let (_host, slots) = slots_from(
            "tone",
            r#"
            maki.api.set_prompt({
                slot = "tone",
                content = "Custom tone",
            })
            "#,
        );
        assert_eq!(
            contents(&slots, PromptId::System, Slot::Tone),
            ["Custom tone"]
        );
        assert!(contents(&slots, PromptId::Research, Slot::Tone).is_empty());
        assert!(contents(&slots, PromptId::General, Slot::Tone).is_empty());
    }

    #[test]
    fn singleton_last_wins_across_plugins() {
        let host = PluginHost::new(Arc::new(ToolRegistry::new())).unwrap();
        host.load_source(
            "aaa",
            r#"maki.api.set_prompt({ slot = "identity", content = "AAA" })"#,
        )
        .unwrap();
        host.load_source(
            "zzz",
            r#"maki.api.set_prompt({ slot = "identity", content = "ZZZ" })"#,
        )
        .unwrap();
        let slots = host.event_handle().collect_prompt_slots();
        let entries = slots.get(PromptId::System, Slot::Identity);
        assert_eq!(entries.len(), 2);
        assert_eq!(entries.last().unwrap().content, "ZZZ");
    }

    #[test]
    fn content_required() {
        let host = PluginHost::new(Arc::new(ToolRegistry::new())).unwrap();
        let r = host.load_source("bad", r#"maki.api.set_prompt({ slot = "identity" })"#);
        assert!(r.is_err());
        assert!(r.unwrap_err().to_string().contains("'content' is required"));
    }

    #[test]
    fn set_prompt_sets_identity() {
        let (_host, slots) = slots_from(
            "setter",
            r#"
            maki.api.set_prompt({
                slot = "identity",
                content = "New identity",
            })
            "#,
        );
        assert_eq!(
            contents(&slots, PromptId::System, Slot::Identity),
            ["New identity"]
        );
    }

    #[test]
    fn set_prompt_explicit_system_prompt() {
        let (_host, slots) = slots_from(
            "setter",
            r#"
            maki.api.set_prompt({
                slot = "identity",
                prompt = "system",
                content = "Explicit identity",
            })
            "#,
        );
        assert_eq!(
            contents(&slots, PromptId::System, Slot::Identity),
            ["Explicit identity"]
        );
    }

    #[test]
    fn prompt_field_targets_specific_prompt() {
        let (_host, slots) = slots_from(
            "targeter",
            r#"
            maki.api.register_prompt_hint({
                slot = "tool_usage",
                prompt = "general",
                content = "General hint",
            })
            "#,
        );
        assert_eq!(
            contents(&slots, PromptId::General, Slot::ToolUsage),
            ["General hint"]
        );
        assert!(contents(&slots, PromptId::System, Slot::ToolUsage).is_empty());
    }

    #[test]
    fn set_prompt_invalid_prompt_rejected() {
        let host = PluginHost::new(Arc::new(ToolRegistry::new())).unwrap();
        let r = host.load_source(
            "bad",
            r#"maki.api.set_prompt({ slot = "identity", prompt = "nope", content = "x" })"#,
        );
        assert!(r.is_err());
    }

    #[test]
    fn set_prompt_and_register_prompt_hint_coexist() {
        let host = PluginHost::new(Arc::new(ToolRegistry::new())).unwrap();
        host.load_source(
            "hint",
            r#"maki.api.register_prompt_hint({ slot = "tool_usage", content = "HINT" })"#,
        )
        .unwrap();
        host.load_source(
            "setter",
            r#"maki.api.set_prompt({ slot = "identity", content = "SET" })"#,
        )
        .unwrap();
        let slots = host.event_handle().collect_prompt_slots();
        assert_eq!(
            contents(&slots, PromptId::System, Slot::ToolUsage),
            ["HINT"]
        );
        assert_eq!(contents(&slots, PromptId::System, Slot::Identity), ["SET"]);
    }

    #[test]
    fn set_prompt_rejects_aggregate_slot() {
        let host = PluginHost::new(Arc::new(ToolRegistry::new())).unwrap();
        let r = host.load_source(
            "bad",
            r#"maki.api.set_prompt({ slot = "tool_usage", content = "nope" })"#,
        );
        assert!(r.is_err());
    }

    #[test]
    fn set_prompt_rejects_incompatible_slot_prompt() {
        let host = PluginHost::new(Arc::new(ToolRegistry::new())).unwrap();
        let r = host.load_source(
            "bad",
            r#"maki.api.set_prompt({ slot = "identity", prompt = "research", content = "x" })"#,
        );
        assert!(r.is_err());
        assert!(r.unwrap_err().to_string().contains("not available"));
    }

    #[test]
    fn empty_prompt_table_is_rejected() {
        let host = PluginHost::new(Arc::new(ToolRegistry::new())).unwrap();
        let r = host.load_source(
            "bad",
            r#"maki.api.set_prompt({ slot = "identity", prompt = {}, content = "x" })"#,
        );
        assert!(r.is_err());
        assert!(r.unwrap_err().to_string().contains("no sequence entries"));
    }

    #[test]
    fn content_must_not_be_empty() {
        let host = PluginHost::new(Arc::new(ToolRegistry::new())).unwrap();
        let r = host.load_source(
            "bad",
            r#"maki.api.set_prompt({ slot = "identity", content = "" })"#,
        );
        assert!(r.is_err());
        assert!(r.unwrap_err().to_string().contains("empty"));
    }

    #[test]
    fn set_prompt_with_callback() {
        let (_host, slots) = slots_from(
            "setter_cb",
            r#"
            maki.api.set_prompt({
                slot = "identity",
                content = function() return "Dyn identity" end,
            })
            "#,
        );
        assert_eq!(
            contents(&slots, PromptId::System, Slot::Identity),
            ["Dyn identity"]
        );
    }
}
