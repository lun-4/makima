mod acp;
mod subcmd;
mod tui;

use std::env;
use std::path::PathBuf;

use color_eyre::Result;
use color_eyre::eyre::Context;

use makima_agent::tools::ToolRegistry;
use makima_config::project::ProjectConfig;
use makima_config::{Config, PermissionsConfig, load_permissions};
use makima_lua::PluginHost;
use makima_storage::StateDir;
use tracing::warn;

use crate::cli::{AuthAction, Cli, Command, McpAction, TrustAction};
use crate::project_trust;
use crate::update;

const CWD_ERR: &str = "cannot read the current directory (was it deleted?)";
const UNMATCHED_RULE_WARNING: &str = "permission rule matches no tool in this project";

fn startup_cwd() -> Result<PathBuf> {
    env::current_dir().context(CWD_ERR)
}

/// Seed `plugins.bash.auto_mode` so the loaded bash plugin classifies every
/// command. Call after `into_config`, before `load_builtins`.
pub fn seed_automode(config: &mut makima_config::Config, automode: bool) {
    if automode {
        config
            .plugins
            .opts
            .entry("bash".into())
            .or_default()
            .insert("auto_mode".into(), serde_json::Value::Bool(true));
    }
}

/// The CLI overrides every mode applies on top of the loaded config, so a flag
/// cannot hold in one run path and be ignored in another. The CLI wins over the
/// config file, and `validate` runs after the overrides.
fn apply_cli_overrides(cli: &Cli, config: &mut Config) -> Result<()> {
    if cli.yolo || config.always_yolo {
        config.permissions.yolo = true;
    }
    seed_automode(config, cli.automode || config.always_automode);
    cli.apply_tool_flags(&mut config.agent)?;
    // A turn cap would cut an interactive session short, so only headless runs take it.
    if cli.print {
        config.agent.max_turns = cli.max_turns;
    }
    config.validate()?;
    Ok(())
}

/// The one config load every mode goes through: `init.lua`, `permissions.toml`,
/// then the CLI flags and the checks that follow them.
pub fn load_config(
    plugin_host: &PluginHost,
    cli: &Cli,
    project_config: &ProjectConfig,
    warnings: &mut Vec<String>,
) -> Result<Config> {
    let mut config = plugin_host
        .load_init_files_or_skip(cli.no_plugins, project_config, warnings)
        .context("load init.lua files")?
        .unwrap_or_default()
        .into_config()
        .context("invalid config")?;
    makima_lua::set_allowed_private_hosts(&config.net.allowed_private_hosts);
    config.permissions = load_permissions(project_config)?;
    apply_cli_overrides(cli, &mut config)?;
    Ok(config)
}

/// Logs each permission rule whose tool nothing registered, and returns it as
/// a startup warning, so a typo'd section such as `[bassh]` shows up.
/// Call once logging is up and every plugin has registered its tools.
fn unmatched_permission_rules(permissions: &PermissionsConfig) -> Vec<String> {
    let registry = ToolRegistry::global();
    permissions
        .unmatched_rules(|tool| registry.get(tool).is_some())
        .into_iter()
        .map(|rule| {
            warn!(rule, "{UNMATCHED_RULE_WARNING}");
            format!("{UNMATCHED_RULE_WARNING}: {rule}")
        })
        .collect()
}

pub fn dispatch(mut cli: Cli) -> Result<()> {
    // `--trust` and `--no-project` hold for this process, so every entry
    // point under them reads the same shared project config the TUI would.
    let trust_mode = cli.trust_mode();
    match cli.command.take() {
        Some(Command::Auth { action }) => {
            let storage = StateDir::resolve().context("resolve data directory")?;
            makima_providers::model_registry::load_from_storage(&storage);
            let (_host, providers, config) =
                subcmd::provider_host(&startup_cwd()?, cli.no_plugins, cli.no_jit, trust_mode)?;
            match action {
                AuthAction::Login { provider } => subcmd::auth_login(
                    provider.as_deref(),
                    &storage,
                    &providers,
                    &config.provider.model_policy,
                )?,
                AuthAction::Logout { provider } => {
                    subcmd::auth_logout(&provider, &storage, &providers)?
                }
                AuthAction::Status => subcmd::auth_status(&storage, &providers)?,
            }
        }
        Some(Command::Index { path }) => {
            subcmd::index(
                &startup_cwd()?,
                &path,
                cli.no_plugins,
                cli.no_jit,
                trust_mode,
            )?;
        }
        Some(Command::Models { refresh }) => subcmd::models(
            &startup_cwd()?,
            cli.no_plugins,
            cli.no_jit,
            refresh,
            trust_mode,
        )?,
        Some(Command::Sessions { json }) => {
            let storage = StateDir::resolve().context("resolve data directory")?;
            subcmd::sessions(&storage, json)?;
        }
        Some(Command::Mcp { action }) => {
            let storage = StateDir::resolve().context("resolve data directory")?;
            match action {
                McpAction::Auth { server } => {
                    subcmd::mcp_auth(&startup_cwd()?, &server, &storage, trust_mode)?
                }
                McpAction::Logout { server } => subcmd::mcp_logout(&server, &storage)?,
            }
        }
        Some(Command::Update { yes, no_color }) => {
            update::update(yes, no_color).map_err(|e| color_eyre::eyre::eyre!("{e}"))?;
        }
        Some(Command::Rollback) => {
            update::rollback().map_err(|e| color_eyre::eyre::eyre!("{e}"))?;
        }
        Some(Command::Acp {
            model,
            yolo,
            automode,
        }) => {
            // The subcommand's copies fold into the global flags so every mode
            // reads them from one place.
            cli.yolo |= yolo;
            cli.automode |= automode;
            acp::run(&cli, startup_cwd()?, model, trust_mode)?;
        }
        Some(Command::Trust { action }) => {
            let storage = StateDir::resolve().context("resolve state directory")?;
            match action {
                TrustAction::Add { path, yes } => {
                    project_trust::add(&storage, path.as_deref(), yes)?
                }
                TrustAction::Remove { path } => project_trust::remove(&storage, path.as_deref())?,
                TrustAction::List => {
                    for decision in project_trust::list(&storage)? {
                        println!("{decision}");
                    }
                }
            }
        }
        Some(Command::Prompt {
            variant,
            plan,
            tools,
            names,
        }) => {
            subcmd::prompt(
                &startup_cwd()?,
                &variant,
                plan,
                tools,
                names,
                cli.no_plugins,
                cli.no_jit,
                trust_mode,
            )?;
        }
        None => {
            tui::run(cli, startup_cwd()?)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use clap::Parser;
    use test_case::test_case;

    use makima_config::RawConfig;

    const ALLOWED: &str = "Read";
    const ALLOWED_NORMALIZED: &str = "read";
    const DISALLOWED: &str = "Bash";
    const DISALLOWED_NORMALIZED: &str = "bash";
    const EXISTING_ALLOWED: &str = "grep";
    const EXISTING_DISALLOWED: &str = "read";
    const UNKNOWN_TOOL: &str = "Bogus";
    const MAX_TURNS: &str = "3";
    const MAX_TURNS_VALUE: u32 = 3;

    /// One entry per run path, as `(global flags, subcommand)`: ACP is the only
    /// mode behind a subcommand, so its global flags must come before it.
    const MODES: [(&[&str], &[&str]); 4] = [
        (&[], &[]),
        (&["--print"], &[]),
        (&["--print", "--input-format", "stream-json"], &[]),
        (&[], &["acp"]),
    ];

    fn cli_args(global: &[&str], subcommand: &[&str], flags: &[&str]) -> Vec<String> {
        std::iter::once("makima")
            .chain(global.iter().copied())
            .chain(flags.iter().copied())
            .chain(subcommand.iter().copied())
            .map(str::to_owned)
            .collect()
    }

    fn default_config() -> Config {
        RawConfig::default().into_config().expect("default config")
    }

    fn overridden(global: &[&str], subcommand: &[&str], flags: &[&str]) -> Config {
        let args = cli_args(global, subcommand, flags);
        let mut config = default_config();
        apply_cli_overrides(&Cli::parse_from(&args), &mut config).expect("overrides apply");
        config
    }

    #[test]
    fn tool_flags_apply_in_every_mode() {
        for (global, subcommand) in MODES {
            let config = overridden(
                global,
                subcommand,
                &["--allowed-tools", ALLOWED, "--disallowed-tools", DISALLOWED],
            );
            assert_eq!(
                config.agent.allowed_tools,
                [ALLOWED_NORMALIZED],
                "{global:?}"
            );
            assert_eq!(
                config.agent.disabled_tools,
                [DISALLOWED_NORMALIZED],
                "{global:?}"
            );
        }
    }

    #[test]
    fn tool_flags_replace_the_allowlist_and_extend_the_denylist() {
        let mut config = default_config();
        config.agent.allowed_tools = vec![EXISTING_ALLOWED.into()];
        config.agent.disabled_tools = vec![EXISTING_DISALLOWED.into()];

        apply_cli_overrides(
            &Cli::parse_from(cli_args(
                &[],
                &[],
                &["--allowed-tools", ALLOWED, "--disallowed-tools", DISALLOWED],
            )),
            &mut config,
        )
        .unwrap();

        assert_eq!(config.agent.allowed_tools, [ALLOWED_NORMALIZED]);
        assert_eq!(
            config.agent.disabled_tools,
            [EXISTING_DISALLOWED, DISALLOWED_NORMALIZED]
        );
    }

    #[test_case(&[], &[], None ; "tui")]
    #[test_case(&["--print"], &[], Some(MAX_TURNS_VALUE) ; "print")]
    #[test_case(&["--print", "--input-format", "stream-json"], &[], Some(MAX_TURNS_VALUE) ; "sdk")]
    #[test_case(&[], &["acp"], None ; "acp")]
    fn max_turns_reaches_only_headless_modes(
        global: &[&str],
        subcommand: &[&str],
        expected: Option<u32>,
    ) {
        let config = overridden(global, subcommand, &["--max-turns", MAX_TURNS]);
        assert_eq!(config.agent.max_turns, expected, "{global:?}");
    }

    #[test]
    fn cli_flags_apply_on_top_of_config_defaults() {
        let mut config = default_config();
        config.always_yolo = false;
        config.always_automode = false;

        apply_cli_overrides(
            &Cli::parse_from(cli_args(&[], &[], &["--yolo", "--automode"])),
            &mut config,
        )
        .unwrap();

        assert!(config.permissions.yolo);
        assert_eq!(
            config.plugins.opts["bash"]["auto_mode"],
            serde_json::json!(true)
        );
    }

    #[test]
    fn config_defaults_apply_without_cli_flags() {
        let mut config = default_config();
        config.always_yolo = true;
        config.always_automode = true;

        apply_cli_overrides(&Cli::parse_from(cli_args(&[], &[], &[])), &mut config).unwrap();

        assert!(config.permissions.yolo);
        assert_eq!(
            config.plugins.opts["bash"]["auto_mode"],
            serde_json::json!(true)
        );
    }

    #[test]
    fn unknown_allowed_tool_is_rejected_in_every_mode() {
        for (global, subcommand) in MODES {
            let args = cli_args(global, subcommand, &["--allowed-tools", UNKNOWN_TOOL]);
            let mut config = default_config();
            assert!(
                apply_cli_overrides(&Cli::parse_from(&args), &mut config).is_err(),
                "{args:?}"
            );
        }
    }

    #[test]
    fn unknown_disallowed_tool_is_skipped_in_every_mode() {
        for (global, subcommand) in MODES {
            let config = overridden(global, subcommand, &["--disallowed-tools", UNKNOWN_TOOL]);
            assert!(config.agent.disabled_tools.is_empty(), "{global:?}");
        }
    }

    #[test]
    fn validation_runs_in_every_mode() {
        for (global, subcommand) in MODES {
            let mut config = default_config();
            config.agent.max_line_bytes = makima_config::MIN_LINE_BYTES - 1;
            let args = cli_args(global, subcommand, &[]);
            assert!(
                apply_cli_overrides(&Cli::parse_from(&args), &mut config).is_err(),
                "{args:?}"
            );
        }
    }

    /// The flags have to survive the real loader, not just the override step.
    #[test]
    fn load_config_applies_the_cli_overrides() {
        use makima_agent::tools::ToolRegistry;
        use makima_lua::ui_sink::{HeadlessUi, HostUi};
        use tempfile::tempdir;

        let dir = tempdir().expect("tempdir");
        let mut plugin_host = PluginHost::with_jit(
            Arc::new(ToolRegistry::new()),
            true,
            HostUi::Sink(Box::new(HeadlessUi)),
        )
        .expect("live host boots");

        let cli = Cli::parse_from(cli_args(
            &["--print"],
            &[],
            &["--allowed-tools", ALLOWED, "--max-turns", MAX_TURNS],
        ));
        let config = load_config(
            &plugin_host,
            &cli,
            &ProjectConfig::for_project(dir.path()),
            &mut Vec::new(),
        )
        .expect("config loads");

        assert_eq!(config.agent.allowed_tools, [ALLOWED_NORMALIZED]);
        assert_eq!(config.agent.max_turns, Some(MAX_TURNS_VALUE));
        plugin_host.begin_shutdown();
    }
}
