mod api;
mod coalesced_latest;
pub mod docs;
pub mod docs_render;
mod error;
pub mod language;
mod loader;
pub(crate) mod plugin_permissions;
mod runtime;
mod splash;

pub use api::keymap::{KeymapEntry, KeymapReader, KeymapSnapshot};
pub use api::options::{OptionSpec, OptionType, PluginOptionSpecs};
pub use api::time::format_ago;
pub use api::util::command::{
    Anchor, Axis, Border, BuiltinAction, CommandArgumentItem, Dimension, Edge, FloatConfig,
    FloatConfigPatch, HintReader, HintSnapshot, ModelRequest, SessionReply, SessionRequest, Split,
    TitlePos, UiAction, UiReply, WinCommand, WinEvent, WinView,
};
pub use api::util::picker::{PickerConfig, PickerEvent, PickerItemSpec, PickerResult};
pub use docs::{DocKind, FnDoc, ModuleDoc, ParamDoc, api_docs};
pub use error::PluginError;
pub use loader::{EventHandle, PluginHost, TestCompletionBackend};
pub use plugin_permissions::{Permission, PluginPermissions};
pub use runtime::{CommandArgumentContext, CommandArgumentLifecycle};
pub use runtime::{KILL_GRACE, RestoreItem, WARM_TOOL_CAP};
pub use splash::{
    SPLASH_PULL_TIMEOUT, SplashFrame, SplashPull, SplashRow, SplashStyle, VersionInfo,
};

pub use api::completion::{AtToken, CompletionCtx, ItemSpec, at_is_token_start, parse_at_tokens};

#[cfg(feature = "test-support")]
pub mod test_support {
    use crate::api::keymap::{KeymapEntry, KeymapWriter};
    use crate::api::util::command::{HintEntries, HintReader, HintWriter};
    use crate::{EventHandle, KeymapReader, PluginHost, TestCompletionBackend};

    /// Stands in for the Lua thread publishing a plugin's status hints.
    pub struct HintWriterHandle(HintWriter);

    impl HintWriterHandle {
        pub fn publish(&self, entries: HintEntries) {
            self.0.publish(entries);
        }
    }

    pub fn hint_writer_pair() -> (HintWriterHandle, HintReader) {
        let (writer, reader) = HintWriter::new();
        (HintWriterHandle(writer), reader)
    }

    /// Observes which requests an [`crate::EventHandle`] sends, without a
    /// running plugin host.
    pub struct RequestProbe(flume::Receiver<crate::runtime::Request>);

    impl RequestProbe {
        /// Next request as `(kind, clicks)`: `"click"` carries no clicks,
        /// `"click_fallback"` and `"restore"` carry their restore item's.
        pub fn try_recv(&self) -> Option<(&'static str, Vec<usize>)> {
            use crate::runtime::Request;
            Some(match self.0.try_recv().ok()? {
                Request::ClickTool { fallback: None, .. } => ("click", Vec::new()),
                Request::ClickTool {
                    fallback: Some(fb), ..
                } => ("click_fallback", fb.item.clicks),
                Request::RestoreToolAsync { item, .. } => ("restore", item.clicks),
                _ => ("other", Vec::new()),
            })
        }

        /// Next dispatched slash command as `(command, args, depth)`, skipping
        /// other requests.
        pub fn try_recv_command(&self) -> Option<(String, String, u8)> {
            use crate::runtime::Request;
            while let Ok(req) = self.0.try_recv() {
                if let Request::RunCommand {
                    command,
                    args,
                    depth,
                    ..
                } = req
                {
                    return Some((command.to_string(), args, depth));
                }
            }
            None
        }

        /// Next fired autocmd as `(event, data)`, skipping other requests.
        pub fn try_recv_autocmd(&self) -> Option<(String, serde_json::Value)> {
            use crate::runtime::Request;
            while let Ok(req) = self.0.try_recv() {
                if let Request::FireAutocmd { event, data } = req {
                    return Some((event, data));
                }
            }
            None
        }

        /// Next picker dialog event as `(id, event)`, skipping other requests.
        pub fn try_recv_picker_event(&self) -> Option<(u64, crate::PickerEvent)> {
            use crate::runtime::Request;
            while let Ok(req) = self.0.try_recv() {
                if let Request::PickerEvent { id, ev } = req {
                    return Some((id, ev));
                }
            }
            None
        }

        pub fn try_finish_command_arguments(
            &self,
            items: Vec<crate::CommandArgumentItem>,
        ) -> Option<(u64, u64)> {
            use crate::runtime::Request;
            while let Ok(req) = self.0.try_recv() {
                if let Request::CollectCommandArgumentItems(work) = req {
                    let stamp = (
                        work.value().context.session,
                        work.value().context.generation,
                    );
                    work.finish(|request| {
                        let _ = request.reply.send(items);
                    });
                    return Some(stamp);
                }
            }
            None
        }

        pub fn try_finish_command_argument_lifecycle(
            &self,
        ) -> Option<(&'static str, Option<String>, bool)> {
            use crate::runtime::{CommandArgumentLifecycle, Request};
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(1);
            loop {
                let remaining = deadline.checked_duration_since(std::time::Instant::now())?;
                let req = self.0.recv_timeout(remaining).ok()?;
                if let Request::CommandArgumentLifecycle(work) = req {
                    let latest = !work.is_superseded();
                    let event = match work.value().event {
                        CommandArgumentLifecycle::Highlight => "highlight",
                        CommandArgumentLifecycle::Accept => "accept",
                        CommandArgumentLifecycle::Cancel => "cancel",
                    };
                    let insertion = work
                        .value()
                        .item
                        .as_ref()
                        .map(|item| item.insertion.clone());
                    work.finish(drop);
                    return Some((event, insertion, latest));
                }
            }
        }

        pub fn try_finish_splash_frame(
            &self,
            frame: Option<crate::SplashFrame>,
        ) -> Option<(u16, u16)> {
            use crate::runtime::Request;
            while let Ok(req) = self.0.try_recv() {
                if let Request::SplashFrame(work) = req {
                    let size = (work.value().width, work.value().height);
                    work.finish(|request| {
                        let _ = request.reply.send(frame);
                    });
                    return Some(size);
                }
            }
            None
        }
    }

    pub fn probed_event_handle() -> (crate::EventHandle, RequestProbe) {
        let (tx, rx) = flume::unbounded();
        (crate::EventHandle::probed_for_test(tx), RequestProbe(rx))
    }

    /// Synthetic state dir for hosts booted by [`spawn_host_for_tests`].
    /// Never created on the real disk; hosts run on the in-memory backend.
    pub const TEST_STATE_DIR: &str = "/maki-test-state";

    pub use crate::api::fs::InMemoryFs;

    /// Boots a real `PluginHost` (background Lua thread) pre-loading the given
    /// builtins on an in-memory FS backend with a synthetic state dir, so the
    /// whole host runs disk-free. Returns a live `EventHandle` plus a guard
    /// that keeps the host alive until it drops. Real-host tests use this
    /// instead of the disconnected/probed handles so end-to-end Lua behavior
    /// (e.g. pulling a `splash.render` frame) can be exercised.
    pub fn spawn_host_for_tests(plugins: &[&str]) -> (crate::EventHandle, PluginHostGuard) {
        spawn_host_with_fs_for_tests(
            plugins,
            std::sync::Arc::new(crate::api::fs::InMemoryFs::new()),
            None,
        )
    }

    /// Same as [`spawn_host_for_tests`], but on a caller-provided backend so
    /// tests can pre-seed state files, with `pre_init` loaded before the
    /// builtins — mirroring the real boot order where user `init.lua` runs
    /// first and its `set_slot` layers sit below the builtins'.
    pub fn spawn_host_with_fs_for_tests(
        plugins: &[&str],
        fs: std::sync::Arc<crate::api::fs::InMemoryFs>,
        pre_init: Option<&str>,
    ) -> (crate::EventHandle, PluginHostGuard) {
        spawn_host_with_fs_and_opts_for_tests(
            plugins,
            fs,
            pre_init,
            std::collections::HashMap::new(),
        )
    }

    /// Same as [`spawn_host_with_fs_for_tests`], with per-builtin options
    /// passed through to `maki.api.register_options`.
    pub fn spawn_host_with_fs_and_opts_for_tests(
        plugins: &[&str],
        fs: std::sync::Arc<crate::api::fs::InMemoryFs>,
        pre_init: Option<&str>,
        opts: std::collections::HashMap<String, serde_json::Map<String, serde_json::Value>>,
    ) -> (crate::EventHandle, PluginHostGuard) {
        use maki_config::PluginsConfig;
        use std::sync::Arc;

        let registry = Arc::new(maki_agent::tools::ToolRegistry::new());
        let backend: std::sync::Arc<dyn crate::api::fs::FsBackend> = Arc::clone(&fs) as _;
        let mut host = crate::PluginHost::with_fs_for_tests(
            registry,
            backend,
            std::path::PathBuf::from(TEST_STATE_DIR),
        )
        .unwrap();
        if let Some(source) = pre_init {
            host.load_source("user_init", source).unwrap();
        }
        let config = PluginsConfig {
            enabled: true,
            names: plugins.iter().map(|s| s.to_string()).collect(),
            opts,
        };
        host.load_builtins(&config).unwrap();
        let handle = host.event_handle();
        (handle, PluginHostGuard { host, backend: fs })
    }

    /// Keeps a booted [`PluginHost`] alive (its `Drop` joins the Lua thread)
    /// and exposes the in-memory backend the host ran on.
    pub struct PluginHostGuard {
        host: PluginHost,
        backend: std::sync::Arc<crate::api::fs::InMemoryFs>,
    }

    impl PluginHostGuard {
        pub fn host(&self) -> &PluginHost {
            &self.host
        }

        pub fn backend(&self) -> &std::sync::Arc<crate::api::fs::InMemoryFs> {
            &self.backend
        }
    }

    impl Drop for PluginHostGuard {
        fn drop(&mut self) {}
    }

    pub fn keymap_reader_with(entries: Vec<KeymapEntry>) -> KeymapReader {
        let (writer, reader) = KeymapWriter::new();
        writer.publish(entries);
        reader
    }

    /// An `EventHandle` backed by an in-memory completion/expander store, plus
    /// the store handle so tests can seed sources and expanders. Use this for
    /// `@`-completion and submit-expansion tests that run without a plugin host.
    pub fn event_handle_with_completion() -> (EventHandle, std::sync::Arc<TestCompletionBackend>) {
        let backend = std::sync::Arc::new(TestCompletionBackend::new());
        let handle = EventHandle::with_completion_for_test(std::sync::Arc::clone(&backend));
        (handle, backend)
    }
}
