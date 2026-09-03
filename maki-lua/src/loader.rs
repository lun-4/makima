use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::Duration;

use include_dir::{Dir, include_dir};
use maki_agent::permissions::PluginRuleStore;
use maki_agent::tools::ToolRegistry;
use maki_commands::CommandRegistry;
use maki_config::{PluginsConfig, RawConfig};

use crate::api::completion::{CompletionCtx, ItemSpec};
use crate::api::fs::{FsBackend, RealFs};
use crate::api::keymap::KeymapReader;
use crate::api::options::{PluginOptionSpecs, PluginOpts};
use crate::api::util::command::{HintReader, StatusContentReader, UiAction};
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
        let lua = runtime::spawn(
            Arc::clone(&registry),
            runtime::SpawnConfig {
                command_registry: command_registry.clone(),
                modes,
                bundled_dirs: *BUNDLED_DIRS,
                jit,
                plugin_rules: Arc::clone(&plugin_rules),
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

    pub fn load_init_files(&self, cwd: &Path) -> Result<Option<RawConfig>, PluginError> {
        let mut merged: Option<RawConfig> = None;

        for global_dir in maki_config::global_config_dirs() {
            self.run_init_file(&global_dir.join("init.lua"), "global/init.lua", &mut merged)?;
            if merged.is_some() {
                break;
            }
        }
        self.run_init_file(
            &cwd.join(".makima/init.lua"),
            "project/init.lua",
            &mut merged,
        )?;

        Ok(merged)
    }

    /// `--no-plugins` recovery path: skip every user `init.lua` while the
    /// host and builtin plugins stay live. Centralized so every entry point
    /// (TUI, index, acp, prompt) honors the flag identically.
    pub fn load_init_files_or_skip(
        &self,
        no_plugins: bool,
        cwd: &Path,
    ) -> Result<Option<RawConfig>, PluginError> {
        if no_plugins {
            return Ok(None);
        }
        self.load_init_files(cwd)
    }

    fn run_init_file(
        &self,
        path: &Path,
        label: &str,
        merged: &mut Option<RawConfig>,
    ) -> Result<(), PluginError> {
        if !path.is_file() {
            return Ok(());
        }
        let source = fs::read_to_string(path).map_err(|e| PluginError::Io {
            path: path.to_path_buf(),
            source: e,
        })?;
        let plugin_dir = path.parent().map(Path::to_path_buf);
        if let Some(raw) = self.send_run_init_lua(source, label.to_owned(), plugin_dir)? {
            match merged {
                Some(existing) => existing.merge(raw),
                None => *merged = Some(raw),
            }
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
                reply: reply_tx,
            })
            .map_err(|_| PluginError::HostDead)?;
        reply_rx.recv().map_err(|_| PluginError::HostDead)?
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
        let (reply_tx, reply_rx) = flume::bounded(1);
        self.inner
            .tx
            .send(Request::RunInitLua {
                source,
                source_name,
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
        reply_rx.recv().map_err(|_| PluginError::HostDead)?;
        Ok(())
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
        )
    }

    pub fn event_handle(&self) -> EventHandle {
        EventHandle {
            tx: self.inner.tx.clone(),
            prio_tx: self.inner.prio_tx.clone(),
            modes: Arc::clone(&self.inner.modes),
            completion: None,
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
    /// User-initiated requests bypass queued bulk work (session restores).
    prio_tx: flume::Sender<Request>,
    /// Shared mode registry; `None`-less so plugins and the Rust agent see
    /// the same definitions. Test handles use an empty builtin set.
    modes: Arc<maki_agent::ModeRegistry>,
    /// In-memory stand-in for the Lua completion/expander stores, used only by
    /// tests that build an `App` without a running plugin host. `None` in
    /// production, where the two RPC methods below talk to the Lua thread.
    completion: Option<Arc<TestCompletionBackend>>,
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
            prio_tx: flume::unbounded().0,
            modes: Arc::new(maki_agent::ModeRegistry::builtin()),
            completion: None,
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
            prio_tx: flume::unbounded().0,
            modes,
            completion: None,
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
            tx: flume::unbounded().0,
            prio_tx: flume::unbounded().0,
            modes: Arc::new(maki_agent::ModeRegistry::builtin()),
            completion: Some(backend),
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
            tx: shared.clone(),
            prio_tx: shared.clone(),
            modes: Arc::new(maki_agent::ModeRegistry::builtin()),
            completion: None,
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
        let _ = self.prio_tx.try_send(Request::RunCommand {
            plugin,
            command,
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
    ) -> flume::Receiver<()> {
        let (completion, rx) = flume::bounded(1);
        let _ = self.prio_tx.try_send(Request::RunCommand {
            plugin,
            command,
            args,
            depth,
            completion: Some(completion),
        });
        rx
    }

    pub fn collect_prompt_slots(&self) -> ResolvedSlots {
        let (tx, rx) = flume::bounded(1);
        let _ = self.tx.send(Request::CollectPromptSlots { reply: tx });
        rx.recv().unwrap_or_default()
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

    pub async fn collect_prompt_slots_async(&self) -> ResolvedSlots {
        let (tx, rx) = flume::bounded(1);
        let _ = self.tx.send(Request::CollectPromptSlots { reply: tx });
        rx.recv_async().await.unwrap_or_default()
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
    use maki_agent::prompt::{PromptId, ResolvedSlots, Slot};
    use maki_agent::tools::ToolRegistry;
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

    /// The second call sends `Shutdown` on a sender that is already
    /// disconnected; it must swallow that error and keep rejecting work.
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
            prio_tx: prio_tx.clone(),
            modes: Arc::new(maki_agent::ModeRegistry::builtin()),
            completion: None,
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
                args,
                depth,
                completion,
            } => {
                assert_eq!(plugin.as_ref(), "myplugin");
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

        let skipped = host
            .load_init_files_or_skip(true, dir.path())
            .expect("no-plugins skips broken init.lua");
        assert!(
            skipped.is_none(),
            "--no-plugins must skip user init.lua entirely"
        );

        let ran = host.load_init_files_or_skip(false, dir.path());
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
