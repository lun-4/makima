use std::env;
use std::io::{self, IsTerminal, Read};
use std::path::Path;
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Instant;

use color_eyre::Result;
use color_eyre::eyre::{Context, bail};
use crossterm::style::Stylize;

use maki_agent::command::{self, CustomCommand};
use maki_agent::tools::ToolRegistry;
use maki_config::{Config, load_env_files, load_permissions};
use maki_lua::PluginHost;
use maki_providers::model::Model;
use maki_storage::StateDir;
use maki_storage::id::MakiId;
use maki_storage::session_lock;
use maki_storage::sessions::{SESSIONS_DIR, StoredThinking};
use maki_ui::{AppSession, RunOutcome};

use crate::cli::{Cli, normalize_tool_name};
use crate::setup;

const FALLBACK_MODEL_SPEC: &str = "anthropic/claude-sonnet-4-20250514";
const CONFIG_FALLBACK_WARNING: &str = "config reload failed, using previous config";
const MODEL_FALLBACK_WARNING: &str = "model resolution failed, keeping previous model";
const PICKER_NEEDS_TUI_ERR: &str = "continuing without a session ID opens the session picker, which needs the TUI; run `makima sessions --json` to list session IDs";

/// One generation of the app: everything torn down and rebuilt on `/reload`.
/// Dropping it joins the Lua thread via `PluginHost::drop`.
struct Stack {
    plugin_host: PluginHost,
    config: Config,
    commands: Vec<CustomCommand>,
    model: Model,
    needs_login: bool,
}

impl Stack {
    fn timeouts(&self) -> maki_providers::Timeouts {
        maki_providers::Timeouts {
            connect: self.config.provider.connect_timeout,
            low_speed: self.config.provider.low_speed_timeout,
            stream: self.config.provider.stream_timeout,
        }
    }
}

/// Background teardown of the previous generation. `defer` keeps the slow
/// drop (a Lua thread join, capped at 2s in `PluginHost::drop`) off the
/// `/reload` hot path. Joining on replace and on drop covers every exit
/// path, including `?` unwinds, so no VM is abandoned mid-shutdown and at
/// most one teardown is ever in flight.
#[derive(Default)]
struct Teardown(Option<JoinHandle<()>>);

impl Teardown {
    fn defer(&mut self, work: impl FnOnce() + Send + 'static) {
        self.join();
        self.0 = Some(thread::spawn(work));
    }

    fn join(&mut self) {
        if let Some(handle) = self.0.take()
            && handle.join().is_err()
        {
            tracing::warn!("background teardown panicked");
        }
    }
}

impl Drop for Teardown {
    fn drop(&mut self) {
        self.join();
    }
}

fn discover_commands(disable: bool) -> Vec<CustomCommand> {
    if disable {
        return Vec::new();
    }
    let cwd = env::current_dir().unwrap_or_else(|_| ".".into());
    command::discover_commands(&cwd)
}

fn load_config(plugin_host: &PluginHost, cli: &Cli, cwd: &Path) -> Result<Config> {
    let raw_config = plugin_host
        .load_init_files_or_skip(cli.no_plugins, cwd)
        .context("load init.lua files")?;

    let mut config = raw_config
        .unwrap_or_default()
        .into_config(cli.no_rtk)
        .context("invalid config")?;
    config.permissions = load_permissions(cwd);

    if cli.yolo || config.always_yolo {
        config.permissions.yolo = true;
    }
    let automode_on = cli.automode || config.always_automode;
    super::seed_automode(&mut config, automode_on);
    if !cli.allowed_tools.is_empty() {
        config.agent.allowed_tools = cli
            .allowed_tools
            .iter()
            .map(|t| normalize_tool_name(t))
            .collect::<Result<Vec<_>>>()?;
    }
    if !cli.disallowed_tools.is_empty() {
        config.agent.disabled_tools.extend(
            cli.disallowed_tools
                .iter()
                .filter_map(|t| normalize_tool_name(t).ok()),
        );
    }
    config.validate()?;
    Ok(config)
}

fn config_or_fallback(
    loaded: Result<Config>,
    fallback: Option<Config>,
    warnings: &mut Vec<String>,
) -> Result<Config> {
    match (loaded, fallback) {
        (Ok(config), _) => Ok(config),
        (Err(e), Some(last_good)) => {
            warnings.push(format!("{CONFIG_FALLBACK_WARNING}: {e:#}"));
            Ok(last_good)
        }
        (Err(e), None) => Err(e),
    }
}

/// The one construction path for a generation: first startup passes
/// `fallback: None` (fail-fast); `/reload` passes the last-good config and
/// model so a broken config reopens the UI with a warning instead of exiting.
fn build_stack(
    cli: &Cli,
    cwd: &Path,
    storage: &StateDir,
    fallback: Option<(Config, Model)>,
) -> Result<(Stack, Vec<String>)> {
    let mut warnings = Vec::new();

    let command_registry = maki_commands::CommandRegistry::new();
    let mut plugin_host = PluginHost::with_command_registry(
        Arc::clone(ToolRegistry::global_arc()),
        command_registry,
        !cli.no_jit,
    )
    .context("initialize lua plugin host")?;

    let (fallback_config, fallback_model) = fallback.unzip();
    let reloading = fallback_model.is_some();
    let config = config_or_fallback(
        load_config(&plugin_host, cli, cwd),
        fallback_config,
        &mut warnings,
    )?;

    if let Err(e) = plugin_host.load_builtins(&config.plugins) {
        let e = color_eyre::eyre::Report::from(e).wrap_err("load builtin plugins");
        if reloading {
            warnings.push(format!("{e:#}"));
        } else {
            return Err(e);
        }
    }

    let commands = discover_commands(cli.no_commands);

    let model_result = setup::resolve_model(cli.model.as_deref(), &config.provider, storage);
    let (model, needs_login) = match (model_result, fallback_model) {
        (Ok(m), _) => (m, false),
        (Err(e), Some(last_model)) => {
            warnings.push(format!("{MODEL_FALLBACK_WARNING}: {e:#}"));
            (last_model, false)
        }
        (Err(_), None) if !cli.print => {
            let placeholder = Model::from_spec(FALLBACK_MODEL_SPEC).expect("fallback model");
            (placeholder, true)
        }
        (Err(e), None) => return Err(e),
    };

    Ok((
        Stack {
            plugin_host,
            config,
            commands,
            model,
            needs_login,
        },
        warnings,
    ))
}

fn resolve_session(
    last_session: bool,
    session_id: Option<&str>,
    model: &str,
    cwd: &str,
    storage: &StateDir,
) -> Result<AppSession> {
    let sessions_dir = storage.ensure_subdir(SESSIONS_DIR)?;
    if let Some(raw) = session_id {
        let id: MakiId = raw
            .parse()
            .map_err(|e| color_eyre::eyre::eyre!("invalid session id {raw:?}: {e}"))?;
        let session = AppSession::load(id, storage).map_err(|e| color_eyre::eyre::eyre!("{e}"))?;
        let locked = session_lock::open_elsewhere(&sessions_dir, &session.id);
        if let Some(block) = session_lock::resume_block(&session.cwd, cwd, locked) {
            return Err(color_eyre::eyre::eyre!("session {id}: {block}"));
        }
        return Ok(session);
    }
    if last_session {
        match AppSession::latest(cwd, storage) {
            Ok(Some(session)) => {
                let locked = session_lock::open_elsewhere(&sessions_dir, &session.id);
                if let Some(block) = session_lock::resume_block(&session.cwd, cwd, locked) {
                    return Err(color_eyre::eyre::eyre!("session {}: {block}", session.id));
                }
                return Ok(session);
            }
            Ok(None) => {
                tracing::info!("no previous session found for this directory, starting new");
            }
            Err(e) => {
                tracing::warn!(error = %e, "failed to load latest session, starting new");
            }
        }
    }
    Ok(AppSession::new(model, cwd))
}

fn read_initial_prompt(cli_prompt: Option<String>) -> Result<Option<String>> {
    match cli_prompt {
        Some(p) => Ok(Some(p)),
        None if !io::stdin().is_terminal() => {
            let mut buf = String::new();
            io::stdin().read_to_string(&mut buf).context("read stdin")?;
            Ok(Some(buf))
        }
        None => Ok(None),
    }
}

/// A bare `-c` (no ID) asks for the session picker; a valued flag
/// continues by ID and an absent flag starts fresh.
fn session_picker_requested(cli: &Cli) -> bool {
    matches!(cli.continue_session, Some(None))
}

/// Exit hint offering both resume options; the flags and the session ID
/// are cyan-highlighted only when stderr is a TTY, so piped output stays
/// plain text.
fn exit_resume_hint(id: &str, colored: bool) -> String {
    if !colored {
        return format!("Resume session:\n\n  makima -c {id}\n  makima -l");
    }
    format!(
        "Resume session:\n\n  makima {} {}\n  makima {}",
        "-c".cyan(),
        id.cyan(),
        "-l".cyan()
    )
}

pub fn run(mut cli: Cli) -> Result<()> {
    if cli.print && session_picker_requested(&cli) {
        bail!(PICKER_NEEDS_TUI_ERR);
    }
    let storage = StateDir::resolve().context("resolve data directory")?;
    maki_providers::model_registry::load_from_storage(&storage);

    let cwd = env::current_dir().unwrap_or_else(|_| ".".into());

    load_env_files(&cwd);
    warn_stale_config_toml(&cwd);

    let (mut stack, _) = build_stack(&cli, &cwd, &storage, None)?;

    setup::init_logging(&stack.config.storage);
    setup::install_panic_log_hook();
    setup::warn_ignored_provider_fields();

    if cli.is_sdk_mode() {
        let fast = stack.config.always_fast && stack.model.supports_fast();
        let prompt_slots = stack.plugin_host.event_handle().collect_prompt_slots();
        let timeouts = stack.timeouts();
        crate::sdk_mode::run(crate::sdk_mode::SdkParams {
            cli,
            model: stack.model,
            config: stack.config.agent,
            permissions_config: stack.config.permissions,
            timeouts,
            prompt_slots,
            fast,
            workflow: stack.config.always_workflow,
            model_policy: Arc::new(stack.config.provider.model_policy.clone()),
            plugin_rules: stack.plugin_host.plugin_rules(),
            commands: stack.commands,
            command_registry: stack.plugin_host.command_registry(),
        })
        .context("run sdk mode")?;
        return Ok(());
    }
    if cli.print {
        let fast = stack.config.always_fast && stack.model.supports_fast();
        let timeouts = stack.timeouts();
        crate::print::run(
            &stack.model,
            cli.initial_prompt,
            cli.images,
            cli.output_format,
            cli.verbose,
            stack.config.agent,
            stack.config.permissions,
            timeouts,
            stack.plugin_host.event_handle(),
            fast,
            stack.config.always_workflow,
            Arc::new(stack.config.provider.model_policy.clone()),
            cli.system_prompt,
            cli.append_system_prompt,
            stack.plugin_host.plugin_rules(),
            &stack.commands,
            stack.plugin_host.command_registry(),
            stack.plugin_host.mode_registry(),
        )
        .context("run print mode")?;
        return Ok(());
    }

    let cwd_str = cwd.to_string_lossy().into_owned();
    let session_id = cli.continue_session.as_ref().and_then(|o| o.as_deref());
    let mut session_picker = session_picker_requested(&cli);
    let mut tabs = vec![resolve_session(
        cli.last_session && !session_picker,
        session_id,
        &stack.model.spec(),
        &cwd_str,
        &storage,
    )?];
    let mut focused = 0;
    let mut warnings: Vec<String> = Vec::new();
    let mut initial_prompt = read_initial_prompt(cli.initial_prompt.take())?;
    let mut teardown = Teardown::default();
    let default_thinking: Option<StoredThinking> = maki_storage::sessions::read_prefs(&storage)
        .default_thinking
        .or(stack.config.always_thinking);

    loop {
        for session in &mut tabs {
            if session.messages().is_empty() {
                session.meta.fast |= stack.config.always_fast;
                session.meta.workflow |= stack.config.always_workflow;
                if let Some(thinking) = default_thinking {
                    session.meta.thinking = Some(thinking);
                }
            }
        }
        let focused_tab = &tabs[focused];
        let model = if focused_tab.messages().is_empty()
            || !stack
                .config
                .provider
                .model_policy
                .allows(&focused_tab.model)
        {
            stack.model.clone()
        } else {
            Model::from_spec(&focused_tab.model).unwrap_or_else(|_| stack.model.clone())
        };

        let outcome = maki_ui::run(
            maki_ui::EventLoopParams {
                model,
                needs_login: stack.needs_login,
                commands: std::mem::take(&mut stack.commands),
                sessions: std::mem::take(&mut tabs),
                focused,
                startup_warnings: std::mem::take(&mut warnings),
                storage: storage.clone(),
                config: stack.config.agent.clone(),
                ui_config: stack.config.ui.clone(),
                input_history_size: stack.config.storage.input_history_size,
                permissions: Arc::new(maki_agent::permissions::PermissionManager::new(
                    stack.config.permissions.clone(),
                    cwd.clone(),
                    stack.plugin_host.plugin_rules(),
                )),
                timeouts: stack.timeouts(),
                exit_on_done: cli.exit_on_done,
                command_registry: stack.plugin_host.command_registry(),
                session_picker,
                keymap_reader: stack.plugin_host.keymap_reader(),
                hint_reader: stack.plugin_host.hint_reader(),
                status_content_reader: stack.plugin_host.status_content_reader(),
                ui_action_rx: stack.plugin_host.ui_action_rx(),
                lua_event_handle: stack.plugin_host.event_handle(),
                model_policy: Arc::new(stack.config.provider.model_policy.clone()),
                system_prompt_override: cli.system_prompt.clone(),
                append_system_prompt: cli.append_system_prompt.clone(),
            },
            initial_prompt.take(),
        )
        .context("run UI")?;

        match outcome {
            RunOutcome::Exit { session_id, code } => {
                if let Some(session_id) = session_id {
                    let colored = io::stderr().is_terminal();
                    eprintln!("{}", exit_resume_hint(&session_id.to_string(), colored));
                }
                let started = Instant::now();
                drop(stack);
                let stack_ms = started.elapsed().as_millis() as u64;
                teardown.join();
                tracing::info!(
                    stack_ms,
                    teardown_ms = started.elapsed().as_millis() as u64 - stack_ms,
                    "plugin host and teardown joined"
                );
                if code != 0 {
                    std::process::exit(code);
                }
                return Ok(());
            }
            RunOutcome::Reload {
                tabs: reloaded,
                focused: f,
            } => {
                // The picker is a one-shot startup request; a later
                // `/reload` must reopen a fresh tab instead of re-prompting.
                session_picker = false;
                let started = Instant::now();
                let last_good = (stack.config.clone(), stack.model.clone());
                // Shut the old host down first so nothing can repopulate
                // the registry after the clear: its senders disconnect, the
                // watchdog aborts in-flight callbacks, and only this thread
                // issues loads. The old VM then shares nothing with the new
                // stack, so its slow join (up to 2s) can run on a
                // background thread.
                stack.plugin_host.begin_shutdown();
                ToolRegistry::global().clear_lua();
                teardown.defer(move || drop(stack));
                let (new_stack, new_warnings) = build_stack(&cli, &cwd, &storage, Some(last_good))?;
                tabs = reloaded;
                if tabs.is_empty() {
                    tabs.push(AppSession::new(&new_stack.model.spec(), &cwd_str));
                }
                stack = new_stack;
                warnings = new_warnings;
                focused = f.min(tabs.len() - 1);
                tracing::info!(
                    elapsed_ms = started.elapsed().as_millis() as u64,
                    tabs = tabs.len(),
                    "reload: rebuilt plugins and config"
                );
            }
        }
    }
}

fn warn_stale_config_toml(cwd: &std::path::Path) {
    let stale_paths = [
        maki_config::global_config_dir().map(|d| d.join("config.toml")),
        Some(cwd.join(".makima/config.toml")),
    ];
    for path in stale_paths.into_iter().flatten() {
        if path.is_file() {
            tracing::warn!(
                path = %path.display(),
                "config.toml found but no longer used. Migrate to init.lua. See https://makima.ln4.net/docs/configuration/"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use color_eyre::eyre::eyre;
    use maki_config::RawConfig;
    use std::fs;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicBool, Ordering};
    use test_case::test_case;

    /// `second_saw_first` requires both joins: `defer` joining the first
    /// closure before spawning the second, and `Drop` joining the second
    /// before the assert reads the flag.
    #[test]
    fn teardown_defer_joins_previous_and_drop_joins_last() {
        let first_done = Arc::new(AtomicBool::new(false));
        let second_saw_first = Arc::new(AtomicBool::new(false));
        let mut teardown = Teardown::default();

        let set = Arc::clone(&first_done);
        teardown.defer(move || set.store(true, Ordering::Release));

        let read = Arc::clone(&first_done);
        let record = Arc::clone(&second_saw_first);
        teardown.defer(move || record.store(read.load(Ordering::Acquire), Ordering::Release));

        drop(teardown);
        assert!(second_saw_first.load(Ordering::Acquire));
    }

    #[test]
    fn teardown_swallows_panic_and_keeps_working() {
        let prev_hook = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| {}));

        let after_panic_ran = Arc::new(AtomicBool::new(false));
        let mut teardown = Teardown::default();
        teardown.defer(|| panic!("intentional"));
        let set = Arc::clone(&after_panic_ran);
        teardown.defer(move || set.store(true, Ordering::Release));
        drop(teardown);

        std::panic::set_hook(prev_hook);
        assert!(after_panic_ran.load(Ordering::Acquire));
    }

    fn test_config() -> Config {
        RawConfig::default()
            .into_config(false)
            .expect("default config")
    }

    #[test]
    fn seed_automode_wires_bash_opt_when_requested() {
        let mut on = test_config();
        crate::cmd::seed_automode(&mut on, true);
        assert_eq!(
            on.plugins.opts["bash"]["auto_mode"],
            serde_json::json!(true),
            "--automode seeds config.plugins.opts[\"bash\"][\"auto_mode\"]"
        );

        let mut off = test_config();
        crate::cmd::seed_automode(&mut off, false);
        assert!(
            !off.plugins.opts.contains_key("bash"),
            "auto_mode absent when --automode is off"
        );
    }

    #[test]
    fn broken_config_with_fallback_uses_last_good_and_warns() {
        let mut last_good = test_config();
        last_good.always_fast = true;
        let mut warnings = Vec::new();

        let config = config_or_fallback(Err(eyre!("boom")), Some(last_good), &mut warnings)
            .expect("fallback config");

        assert!(config.always_fast);
        assert_eq!(warnings.len(), 1);
        assert!(
            warnings[0].starts_with(CONFIG_FALLBACK_WARNING),
            "{warnings:?}"
        );
        assert!(warnings[0].contains("boom"), "{warnings:?}");
    }

    #[test]
    fn broken_config_without_fallback_is_fatal() {
        let mut warnings = Vec::new();
        let err = match config_or_fallback(Err(eyre!("boom")), None, &mut warnings) {
            Err(e) => e,
            Ok(_) => panic!("expected error without fallback"),
        };
        assert!(err.to_string().contains("boom"));
        assert!(warnings.is_empty());
    }

    /// `--no-plugins` keeps the Lua host live (tools + default keymap
    /// still load) but skips user `init.lua`, so a broken project
    /// `init.lua` must not be executed in that mode.
    #[test]
    fn no_plugins_skips_broken_init_lua_but_keeps_host_alive() {
        use clap::Parser;
        use maki_agent::tools::ToolRegistry;
        use tempfile::tempdir;

        let dir = tempdir().expect("tempdir");
        let maki_dir: PathBuf = dir.path().join(".makima");
        fs::create_dir_all(&maki_dir).expect("mkdir .makima");
        fs::write(
            maki_dir.join("init.lua"),
            "error('broken init lua must not run')",
        )
        .expect("write init.lua");

        let cli = Cli::parse_from(["makima", "--no-plugins"]);
        assert!(cli.no_plugins);

        let mut plugin_host = PluginHost::with_jit(Arc::new(ToolRegistry::new()), true)
            .expect("live host boots under --no-plugins");

        let config = load_config(&plugin_host, &cli, dir.path())
            .expect("no-plugins must skip the broken init.lua and still load defaults");
        assert!(
            !config.plugins.names.is_empty(),
            "default builtin plugins must still be enabled under --no-plugins"
        );

        plugin_host
            .load_builtins(&config.plugins)
            .expect("builtins load on the live host under --no-plugins");

        plugin_host.begin_shutdown();
    }

    /// Bare `-c` (no ID) is the only shape that opens the picker;
    /// an absent flag must never trip it.
    #[test]
    fn session_picker_requested_requires_bare_resume() {
        use clap::Parser;

        assert!(!session_picker_requested(&Cli::parse_from(["makima"])));
        assert!(session_picker_requested(&Cli::parse_from(["makima", "-c"])));
        assert!(session_picker_requested(&Cli::parse_from([
            "makima",
            "--continue"
        ])));
        assert!(!session_picker_requested(&Cli::parse_from([
            "makima", "-c", "abc"
        ])));
    }

    #[test]
    fn print_mode_bare_continue_errors() {
        use clap::Parser;

        let err = run(Cli::parse_from(["makima", "--print", "-c"]))
            .expect_err("bare -c under --print fails before any stack work");
        assert!(err.to_string().contains(PICKER_NEEDS_TUI_ERR));
    }

    const OTHER_CWD: &str = "/elsewhere";
    const THIS_CWD: &str = "/here";
    /// A pid no live process on this machine has.
    const FAKE_PID: u32 = u32::MAX - 1;

    #[test]
    fn resolve_session_rejects_session_from_other_cwd() {
        use tempfile::tempdir;

        let tmp = tempdir().unwrap();
        let storage = StateDir::from_path(tmp.path().to_path_buf());
        let mut session = AppSession::new("test-model", OTHER_CWD);
        session.save(&storage).unwrap();
        let err = resolve_session(
            false,
            Some(&session.id.to_string()),
            "test-model",
            THIS_CWD,
            &storage,
        )
        .expect_err("a session stored under another cwd must be rejected");
        assert!(err.to_string().contains(OTHER_CWD));
    }

    #[test_case(true; "valued_continue")]
    #[test_case(false; "last")]
    fn resolve_session_rejects_session_open_elsewhere(valued: bool) {
        use tempfile::tempdir;

        let tmp = tempdir().unwrap();
        let storage = StateDir::from_path(tmp.path().to_path_buf());
        let mut session = AppSession::new("test-model", THIS_CWD);
        session.save(&storage).unwrap();
        let sessions_dir = storage.ensure_subdir(SESSIONS_DIR).unwrap();
        let (last, id_arg) = match valued {
            true => (false, Some(session.id.to_string())),
            false => (true, None),
        };

        let loaded = resolve_session(last, id_arg.as_deref(), "test-model", THIS_CWD, &storage)
            .expect("an unlocked session loads");
        assert_eq!(loaded.id, session.id);

        fs::write(
            session_lock::lock_path(&sessions_dir, &session.id),
            FAKE_PID.to_string(),
        )
        .unwrap();
        let err = resolve_session(last, id_arg.as_deref(), "test-model", THIS_CWD, &storage)
            .expect_err("a session locked by another instance must be rejected");
        assert!(err.to_string().contains(session_lock::OPEN_ELSEWHERE_MSG));
    }

    #[test]
    fn exit_resume_hint_plain_mentions_both_flags() {
        let hint = exit_resume_hint("abc", false);
        assert!(hint.contains("makima -c abc"));
        assert!(hint.contains("makima -l"));
        assert!(!hint.contains("\u{1b}["));
    }

    #[test]
    fn exit_resume_hint_colored_highlights_flags_and_id() {
        let hint = exit_resume_hint("abc", true);
        assert!(hint.contains("\u{1b}["));
        assert_eq!(strip_ansi(&hint), exit_resume_hint("abc", false));
    }

    fn strip_ansi(s: &str) -> String {
        let mut out = String::new();
        let mut chars = s.chars();
        while let Some(c) = chars.next() {
            if c == '\u{1b}' {
                for c in chars.by_ref() {
                    if c == 'm' {
                        break;
                    }
                }
            } else {
                out.push(c);
            }
        }
        out
    }

    /// Negative control for the test above: without `--no-plugins`, the
    /// same broken `init.lua` must surface as an error so the skip path
    /// cannot silently regress into a tautology.
    #[test]
    fn broken_init_lua_errors_without_no_plugins() {
        use clap::Parser;
        use maki_agent::tools::ToolRegistry;
        use tempfile::tempdir;

        let dir = tempdir().expect("tempdir");
        let maki_dir: PathBuf = dir.path().join(".makima");
        fs::create_dir_all(&maki_dir).expect("mkdir .makima");
        fs::write(
            maki_dir.join("init.lua"),
            "error('broken init lua must not run')",
        )
        .expect("write init.lua");

        let cli = Cli::parse_from(["makima"]);
        assert!(!cli.no_plugins);

        let mut plugin_host =
            PluginHost::with_jit(Arc::new(ToolRegistry::new()), true).expect("live host boots");

        match load_config(&plugin_host, &cli, dir.path()) {
            Err(_) => {}
            Ok(_) => panic!("broken init.lua must error without --no-plugins"),
        }

        plugin_host.begin_shutdown();
    }
}
