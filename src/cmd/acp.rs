use std::env;
use std::sync::Arc;

use color_eyre::Result;
use color_eyre::eyre::Context;

use maki_agent::{command, tools::ToolRegistry};
use maki_config::project::{self, TrustMode};
use maki_config::{load_env_files, load_permissions};
use maki_lua::PluginHost;
use maki_storage::StateDir;

use crate::setup;

#[allow(clippy::too_many_arguments)]
pub fn run(
    model_arg: Option<String>,
    yolo: bool,
    automode: bool,
    no_plugins: bool,
    no_jit: bool,
    system_prompt_override: Option<String>,
    append_system_prompt: Option<String>,
    trust_mode: TrustMode,
) -> Result<()> {
    let storage = StateDir::resolve().context("resolve data directory")?;
    maki_providers::model_registry::load_from_storage(&storage);

    let cwd = env::current_dir().unwrap_or_else(|_| ".".into());
    let trust = project::resolve(&storage, &cwd, trust_mode);
    load_env_files(&trust.project_config);

    let command_registry = maki_commands::CommandRegistry::new();
    let standard_commands = command::StandardCommands::register(
        &command_registry,
        &command::discover_commands(&cwd),
        command::StandardCompletions::default(),
    )
    .context("register standard commands")?;
    let mut plugin_host = PluginHost::with_command_registry(
        Arc::clone(ToolRegistry::global_arc()),
        command_registry.clone(),
        !no_jit,
    )
    .context("initialize lua plugin host")?;

    let mut warnings: Vec<String> = trust.warning.clone().into_iter().collect();
    let raw_config = plugin_host
        .load_init_files_or_skip(no_plugins, &trust.project_config, &mut warnings)
        .context("load init.lua files")?;

    let mut config = raw_config
        .unwrap_or_default()
        .into_config(false)
        .context("invalid config")?;
    maki_lua::set_allowed_private_hosts(&config.net.allowed_private_hosts);
    config.permissions = load_permissions(&trust.project_config);

    if yolo || config.always_yolo {
        config.permissions.yolo = true;
    }
    let automode_on = automode || config.always_automode;
    super::seed_automode(&mut config, automode_on);
    config.validate()?;

    plugin_host
        .load_builtins(&config.plugins)
        .context("load builtin plugins")?;

    let timeouts = maki_providers::Timeouts::from(&config.provider);

    let model = setup::resolve_model(model_arg.as_deref(), &config.provider, &storage)?;

    setup::init_logging(&config.storage);
    setup::install_panic_log_hook();
    setup::warn_ignored_provider_fields();

    let lua_event_handle = plugin_host.event_handle();
    let prompt_slots = lua_event_handle.collect_prompt_slots();
    let modes = lua_event_handle.mode_registry();
    let yolo = config.permissions.yolo;

    let result = maki_acp::run(maki_acp::AcpParams {
        model,
        config: config.agent,
        permissions_config: config.permissions,
        timeouts,
        initial_wd: cwd,
        storage,
        prompt_slots: Arc::new(prompt_slots),
        modes,
        yolo,
        system_prompt_override,
        append_system_prompt,
        model_policy: Arc::new(config.provider.model_policy.clone()),
        plugin_rules: plugin_host.plugin_rules(),
        lua_event_handle,
        command_registry,
        trust_mode,
        trust_policy: Arc::new(config.trust),
    });
    drop(standard_commands);
    result
}
