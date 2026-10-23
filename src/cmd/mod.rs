mod acp;
mod subcmd;
mod tui;

use std::env;
use std::path::PathBuf;

use color_eyre::Result;
use color_eyre::eyre::Context;

use maki_agent::tools::ToolRegistry;
use maki_config::PermissionsConfig;
use maki_storage::StateDir;
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
pub fn seed_automode(config: &mut maki_config::Config, automode: bool) {
    if automode {
        config
            .plugins
            .opts
            .entry("bash".into())
            .or_default()
            .insert("auto_mode".into(), serde_json::Value::Bool(true));
    }
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

pub fn dispatch(cli: Cli) -> Result<()> {
    // `--trust` and `--no-project` hold for this process, so every entry
    // point under them reads the same shared project config the TUI would.
    let trust_mode = cli.trust_mode();
    match cli.command {
        Some(Command::Auth { action }) => {
            let storage = StateDir::resolve().context("resolve data directory")?;
            match action {
                AuthAction::Login { provider } => {
                    subcmd::auth_login(provider.as_deref(), &storage)?
                }
                AuthAction::Logout { provider } => subcmd::auth_logout(&provider, &storage)?,
                AuthAction::Status => subcmd::auth_status(&storage)?,
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
            acp::run(
                startup_cwd()?,
                model,
                yolo,
                automode,
                cli.no_plugins,
                cli.no_jit,
                cli.system_prompt,
                cli.append_system_prompt,
                trust_mode,
            )?;
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
