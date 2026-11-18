use std::path::PathBuf;
use std::sync::Arc;

use color_eyre::Result;
use color_eyre::eyre::Context;

use makima_agent::{command, tools::ToolRegistry};
use makima_config::load_env_files;
use makima_config::project::{self, TrustMode};
use makima_lua::PluginHost;
use makima_lua::ui_sink::{HeadlessUi, HostUi};
use makima_storage::StateDir;

use crate::cli::Cli;
use crate::setup;

pub fn run(
    cli: &Cli,
    cwd: PathBuf,
    model_arg: Option<String>,
    trust_mode: TrustMode,
) -> Result<()> {
    let storage = StateDir::resolve().context("resolve data directory")?;
    makima_providers::model_registry::load_from_storage(&storage);

    let trust = project::resolve(&storage, &cwd, trust_mode);
    load_env_files(&trust.project_config);

    let command_registry = makima_commands::CommandRegistry::new();
    let standard_commands = command::StandardCommands::register(
        &command_registry,
        &command::discover_commands(&cwd, &trust.project_config),
        command::StandardCompletions::default(),
    )
    .context("register standard commands")?;
    let mut plugin_host = PluginHost::with_command_registry(
        Arc::clone(ToolRegistry::global_arc()),
        command_registry.clone(),
        !cli.no_jit,
        HostUi::Sink(Box::new(HeadlessUi)),
    )
    .context("initialize lua plugin host")?;

    let mut warnings: Vec<String> = trust.warning.clone().into_iter().collect();
    let config = super::load_config(&plugin_host, cli, &trust.project_config, &mut warnings)?;
    for warning in &warnings {
        eprintln!("warning: {warning}");
    }

    plugin_host
        .load_builtins(&config.plugins)
        .context("load builtin plugins")?;

    let timeouts = makima_providers::Timeouts::from(&config.provider);
    let providers = plugin_host
        .provider_registry(timeouts)
        .context("build provider registry")?;

    let model = setup::resolve_model(&providers, model_arg.as_deref(), &config.provider, &storage)?;

    setup::init_logging(&config.storage);
    setup::install_panic_log_hook();
    for warning in super::unmatched_permission_rules(&config.permissions) {
        eprintln!("warning: {warning}");
    }

    let lua_event_handle = plugin_host.event_handle();
    let yolo = config.permissions.yolo;

    let result = makima_acp::run(makima_acp::AcpParams {
        model,
        config: config.agent,
        permissions_config: config.permissions,
        timeouts,
        initial_wd: cwd,
        storage,
        prompt_slots: lua_event_handle.prompt_slot_source(),
        yolo,
        defaults: config.session_defaults,
        system_prompt_override: cli.system_prompt.clone(),
        append_system_prompt: cli.append_system_prompt.clone(),
        model_policy: Arc::new(config.provider.model_policy.clone()),
        providers,
        plugin_rules: plugin_host.plugin_rules(),
        lua_event_handle,
        command_registry,
        trust_mode,
        trust_policy: Arc::new(config.trust),
    });
    drop(standard_commands);
    result
}
