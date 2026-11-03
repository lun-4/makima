use std::env;
use std::io::{self, BufRead, Write};
use std::path::Path;
use std::sync::Arc;

use color_eyre::Result;
use color_eyre::eyre::{Context, bail};

use maki_agent::mcp::{config as mcp_config, oauth as mcp_oauth};
use maki_agent::tools::ToolRegistry;
use maki_config::project::{self, ProjectConfig, TrustMode};
use maki_config::providers::{
    ProviderDef, ProvidersConfig, VERTEX_LOGIN_INSTRUCTIONS, all_builtins, builtin_provider,
    resolve_api_key_env, resolve_base_url, resolve_default_model, resolve_display_name,
    resolve_login_url, slugify,
};
use maki_config::{Config, load_env_files, load_permissions};
use maki_lua::PluginHost;
use maki_lua::ui_sink::{HeadlessUi, HostUi};
use maki_providers::login::{
    self, AuthState, AuthStatus, InteractionRequest, InteractionService, LoginOutcome,
    LoginRequest, ProviderStatus, drive_login,
};
use maki_providers::provider::BoxFuture;
use maki_providers::{AgentError, ProviderData, ProviderRegistry, Timeouts, catalog_providers};
use maki_providers::{copilot_auth, dynamic, openai_auth};
use maki_storage::StateDir;
use maki_storage::auth::ProviderCredentials;
use maki_storage::auth::{
    delete_provider_credentials, load_provider_credentials, load_tokens, save_provider_credentials,
};
use maki_storage::model::persist_model;
use maki_ui::{AppSession, CUSTOM_SLUG, OPENAI_CODEX_SLUG, native_login_choices};

const SESSIONS_JSON_ONLY_ERR: &str =
    "the sessions command only supports --json; use `makima sessions --json`";
const SAVED_MARK: &str = "\x1b[32m✓\x1b[0m";
const ENV_MARK: &str = "\x1b[33m~\x1b[0m";
const CONFIGURED_MARK: &str = "\x1b[34m●\x1b[0m";
const MISSING_MARK: &str = "\x1b[31m✗\x1b[0m";
const FAILED_MARK: &str = "\x1b[31m!\x1b[0m";

/// Subcommands have no UI, so their hosts print notifications to stderr.
fn headless() -> HostUi {
    HostUi::Sink(Box::new(HeadlessUi))
}

pub fn auth_login(
    provider: Option<&str>,
    storage: &StateDir,
    registry: &ProviderRegistry,
) -> Result<()> {
    let native = match provider {
        Some(_) => Vec::new(),
        None => native_login_choices(storage, registry, Some(catalog_providers())),
    };
    let request = LoginRequest {
        provider: provider.map(str::to_owned),
        native,
    };
    let ui = Arc::new(TerminalInteraction);
    match smol::block_on(drive_login(registry, ui, storage, request))? {
        LoginOutcome::LoggedIn {
            display_name,
            model,
            ..
        } => {
            println!();
            println!("  {SAVED_MARK} Authenticated: {display_name}");
            if let Some(model) = model {
                println!("  Default model: {model}");
            }
        }
        LoginOutcome::Native(name) => login_native(&slugify(&name), storage, provider.is_none())?,
    }
    Ok(())
}

/// {picked} from the list rather than named: the listed OpenAI row asks for
/// an API key, while `auth login openai` runs the ChatGPT device login.
fn login_native(slug: &str, storage: &StateDir, picked: bool) -> Result<()> {
    match slug {
        "openai" if picked => login_provider(slug, storage)?,
        "openai" | OPENAI_CODEX_SLUG => openai_auth::login(storage)?,
        "copilot" => copilot_auth::login(storage)?,
        "vertex" => println!("{VERTEX_LOGIN_INSTRUCTIONS}"),
        CUSTOM_SLUG => login_custom(storage)?,
        slug => {
            if builtin_provider(slug).is_none()
                && dynamic::display_name(slug).is_none()
                && ProvidersConfig::load().get(slug).is_none()
                && let Some(provider_data) = maki_providers::catalog_provider(slug)
            {
                login_catalog_provider(&provider_data, storage)?;
            } else {
                login_provider(slug, storage)?;
            }
        }
    }
    Ok(())
}

/// Answers the login driver on the terminal, off the plugin host's thread.
struct TerminalInteraction;

impl InteractionService for TerminalInteraction {
    fn interact(&self, request: InteractionRequest) -> BoxFuture<'_, Result<String, AgentError>> {
        Box::pin(smol::unblock(move || {
            terminal_interact(request, &mut io::stdin().lock(), &mut io::stdout())
        }))
    }
}

/// End of input counts as backing out.
fn terminal_interact(
    request: InteractionRequest,
    input: &mut impl BufRead,
    out: &mut impl Write,
) -> Result<String, AgentError> {
    match request {
        InteractionRequest::Input {
            prompt, default, ..
        } => {
            match &default {
                Some(default) => write!(out, "  {prompt} [{default}]: ")?,
                None => write!(out, "  {prompt}: ")?,
            }
            out.flush()?;
            let answer = read_answer(input)?;
            Ok(match answer.is_empty() {
                true => default.unwrap_or_default(),
                false => answer,
            })
        }
        InteractionRequest::Choice { prompt, options } => {
            writeln!(out)?;
            let mut section = None;
            for (i, option) in options.iter().enumerate() {
                if let Some(heading) = &option.section
                    && section != Some(heading)
                {
                    writeln!(out, "  {heading}:")?;
                    section = Some(heading);
                }
                let detail = option.detail.as_deref().map(|d| format!(" ({d})"));
                let detail = detail.unwrap_or_default();
                writeln!(out, "  {:>3}. {}{detail}", i + 1, option.label)?;
            }
            loop {
                write!(out, "  {prompt} [1-{}]: ", options.len())?;
                out.flush()?;
                let chosen = read_answer(input)?
                    .parse::<usize>()
                    .ok()
                    .and_then(|number| options.get(number.checked_sub(1)?));
                if let Some(option) = chosen {
                    return Ok(option.id.clone());
                }
            }
        }
        InteractionRequest::Instructions(text) => {
            writeln!(out, "  {text}")?;
            Ok(String::new())
        }
        InteractionRequest::DeviceCode { url, code } => {
            writeln!(out, "  Open {url} and enter code {code}")?;
            Ok(String::new())
        }
        InteractionRequest::OpenBrowser(url) => {
            writeln!(out, "  Opening {url} in your browser")?;
            Ok(String::new())
        }
    }
}

fn read_answer(input: &mut impl BufRead) -> Result<String, AgentError> {
    let mut line = String::new();
    if input.read_line(&mut line)? == 0 {
        return Err(AgentError::Cancelled);
    }
    Ok(line.trim().to_owned())
}

fn login_provider(slug: &str, storage: &StateDir) -> Result<()> {
    let builtin = builtin_provider(slug);
    let is_custom = ProvidersConfig::load().get(slug).is_some();
    if builtin.is_none() && dynamic::display_name(slug).is_none() && !is_custom {
        bail!("unknown provider '{slug}'");
    }

    if builtin.is_none() && dynamic::auth_providers().iter().any(|(s, _)| *s == slug) {
        dynamic::login(slug)?;
        return Ok(());
    }

    let mut config = ProvidersConfig::load();
    let def = config.get(slug).cloned();

    let plan = select_plan(slug, builtin, def.as_ref())?;

    let needs_url = builtin.is_some_and(|b| b.needs_url);
    let host_url = if needs_url {
        Some(prompt_host_url(
            slug,
            &resolve_display_name(slug, def.as_ref()),
            def.as_ref(),
        )?)
    } else {
        None
    };

    let api_key_optional = needs_url;
    let login_url = resolve_login_url(slug, plan.as_deref());
    let api_key = prompt_api_key(
        login_url.as_deref(),
        &resolve_display_name(slug, def.as_ref()),
        api_key_optional,
    )?;

    let mut provider_def = def.unwrap_or_default();
    if let Some(plan_name) = &plan {
        provider_def.plan = Some(plan_name.clone());
    }
    if let Some(url) = &host_url {
        provider_def.base_url = Some(url.clone());
    }

    let has_key = !api_key.is_empty();
    if has_key {
        let creds = ProviderCredentials {
            api_key,
            host: None,
        };
        save_provider_credentials(storage, slug, &creds).context("save credentials")?;
    }

    if plan.is_some() || needs_url || host_url.is_some() || builtin.is_none() {
        config.upsert(slug.to_string(), provider_def);
        config.save().context("save providers.toml")?;
    }

    let default_model = if needs_url {
        None
    } else {
        resolve_default_model(slug, config.get(slug))
    };
    if let Some(model) = &default_model {
        persist_model(storage, model);
    }

    println!();
    let display = resolve_display_name(slug, config.get(slug));
    println!("  \x1b[32m✓\x1b[0m Configured: {}", display);
    if let Some(url) = resolve_base_url(slug, config.get(slug)) {
        println!("  Endpoint: {}", url);
    }
    if let Some(model) = &default_model {
        println!("  Default model: {}", model);
    }
    if has_key {
        println!("  Credentials: ~/.local/state/makima/auth/{}.json", slug);
    } else {
        let env_var = resolve_api_key_env(slug, config.get(slug));
        println!(
            "  Set API key via: {} or run: makima auth login {}",
            env_var, slug
        );
    }

    Ok(())
}

fn login_catalog_provider(provider: &ProviderData, storage: &StateDir) -> Result<()> {
    println!();
    if let Some(ref var) = provider.env_keys.first() {
        println!("  Provider: {} (env: {var})", provider.slug);
    } else {
        println!("  Provider: {}", provider.slug);
    }
    print!("  API key: ");
    io::stdout().flush()?;
    let mut key = String::new();
    io::stdin().read_line(&mut key)?;
    let key = key.trim().to_string();
    if key.is_empty() {
        println!("  Skipped (no key entered)");
        return Ok(());
    }
    let creds = ProviderCredentials {
        api_key: key,
        host: None,
    };
    save_provider_credentials(storage, &provider.slug, &creds).context("save credentials")?;
    println!("  \x1b[32m✓\x1b[0m Saved credentials for {}", provider.slug);
    println!(
        "  Credentials: ~/.local/state/makima/auth/{}.json",
        provider.slug
    );
    println!(
        "  You can also set via: {}",
        provider
            .env_keys
            .first()
            .cloned()
            .unwrap_or_else(|| "API key environment variable".to_string())
    );
    Ok(())
}

fn login_custom(storage: &StateDir) -> Result<()> {
    print!("  Provider name: ");
    io::stdout().flush()?;
    let mut name = String::new();
    io::stdin().read_line(&mut name)?;
    let slug = slugify(&name);
    if slug.is_empty() {
        bail!("provider name cannot be empty");
    }

    println!("  Protocol:");
    println!("    1. openai   (OpenAI-compatible chat completions)");
    println!("    2. anthropic (Anthropic messages API)");
    println!("    3. google   (Google Gemini API)");
    print!("  Select [1-3]: ");
    io::stdout().flush()?;
    let mut proto_input = String::new();
    io::stdin().read_line(&mut proto_input)?;
    let protocol = match proto_input.trim() {
        "1" | "openai" => "openai",
        "2" | "anthropic" => "anthropic",
        "3" | "google" => "google",
        _ => bail!("invalid protocol selection"),
    };

    print!("  Base URL: ");
    io::stdout().flush()?;
    let mut url_input = String::new();
    io::stdin().read_line(&mut url_input)?;
    let base_url = url_input.trim().to_string();
    if base_url.is_empty() {
        bail!("base URL cannot be empty");
    }

    let display_name = format!("Custom ({slug})");
    let api_key_env = format!("{}_API_KEY", slug.to_uppercase().replace('-', "_"));

    print!("  API key (or Enter to skip): ");
    io::stdout().flush()?;
    let mut key_input = String::new();
    io::stdin().read_line(&mut key_input)?;
    let api_key = key_input.trim().to_string();

    let mut config = ProvidersConfig::load();
    let provider_def = ProviderDef {
        display_name: Some(display_name),
        protocol: Some(
            protocol
                .parse()
                .map_err(|e: String| color_eyre::eyre::eyre!("{e}"))?,
        ),
        base_url: Some(base_url.clone()),
        api_key_env: Some(api_key_env.clone()),
        discover_models: true,
        ..Default::default()
    };

    let has_key = !api_key.is_empty();
    if has_key {
        let creds = ProviderCredentials {
            api_key,
            host: None,
        };
        save_provider_credentials(storage, &slug, &creds).context("save credentials")?;
    }

    config.upsert(slug.clone(), provider_def);
    config.save().context("save providers.toml")?;

    println!();
    println!("  \x1b[32m✓\x1b[0m Configured: {}", slug);
    println!("  Endpoint: {}", base_url);
    if has_key {
        println!("  Credentials: ~/.local/state/makima/auth/{}.json", slug);
    } else {
        println!(
            "  Set API key via: {} or run: makima auth login {}",
            api_key_env, slug
        );
    }
    println!("  Use with: makima -m {}/<model>", slug);

    Ok(())
}

fn select_plan(
    slug: &str,
    builtin: Option<&'static maki_config::providers::BuiltInProvider>,
    def: Option<&ProviderDef>,
) -> Result<Option<String>> {
    let plans = builtin.and_then(|b| b.plans);
    if plans.is_none_or(|p| p.len() <= 1) {
        if let Some(d) = def {
            return Ok(d.plan.clone());
        }
        return Ok(None);
    }
    let plans = plans.unwrap();

    if let Some(d) = def
        && d.plan.is_some()
    {
        return Ok(d.plan.clone());
    }

    println!();
    println!("  {} plan:", resolve_display_name(slug, def));
    for (i, (_key, plan)) in plans.iter().enumerate() {
        println!("    {}. {} ({})", i + 1, plan.display_name, plan.base_url);
    }
    println!();
    print!("  Select [1-{}]: ", plans.len());
    io::stdout().flush()?;
    let mut input = String::new();
    io::stdin().read_line(&mut input)?;
    let choice: usize = input.trim().parse().context("enter a number")?;
    if choice == 0 || choice > plans.len() {
        bail!("invalid plan selection");
    }
    Ok(Some(plans[choice - 1].0.to_string()))
}

fn prompt_host_url(slug: &str, display_name: &str, def: Option<&ProviderDef>) -> Result<String> {
    let default = resolve_base_url(slug, def).unwrap_or_default();
    print!("  {} host URL [{}]: ", display_name, default);
    io::stdout().flush()?;

    let mut url = String::new();
    io::stdin().read_line(&mut url)?;
    let url = url.trim().to_string();

    Ok(if url.is_empty() { default } else { url })
}

fn prompt_api_key(url: Option<&str>, display_name: &str, optional: bool) -> Result<String> {
    if let Some(url) = url {
        if let Err(e) = open::that(url) {
            tracing::warn!(error = %e, "failed to open browser");
        }
        println!("  Opened {} in your browser.", url);
    }
    if optional {
        print!("  {} API key (or Enter to skip): ", display_name);
    } else {
        print!("  {} API key: ", display_name);
    }
    io::stdout().flush()?;

    let mut api_key = String::new();
    io::stdin().read_line(&mut api_key)?;
    let api_key = api_key.trim().to_string();

    Ok(api_key)
}

pub fn auth_logout(provider: &str, storage: &StateDir, registry: &ProviderRegistry) -> Result<()> {
    if let Some(name) = smol::block_on(login::logout(registry, storage, provider))? {
        println!("Logged out of {name}.");
        return Ok(());
    }
    let slug = slugify(provider);
    match provider {
        "openai" => openai_auth::logout(storage)?,
        "copilot" => copilot_auth::logout(storage)?,
        _ => {
            let mut config = ProvidersConfig::load();
            let deleted =
                delete_provider_credentials(storage, &slug).context("delete credentials")?;
            if deleted {
                println!("Removed credentials for '{}'.", slug);
            }
            if config.remove(&slug) {
                config.save().context("save providers.toml")?;
            }
            if !deleted && builtin_provider(&slug).is_none() {
                dynamic::logout(&slug)?;
            }
        }
    }
    Ok(())
}

pub fn auth_status(storage: &StateDir, registry: &ProviderRegistry) -> Result<()> {
    let config = ProvidersConfig::load();
    let builtins = all_builtins();

    println!();
    for provider in smol::block_on(login::auth_status(registry, storage)) {
        println!("{}", status_line(&provider));
    }
    for b in builtins.iter().filter(|b| !registry.contains(b.slug)) {
        let def = config.get(b.slug);
        let display = resolve_display_name(b.slug, def);

        if load_tokens(storage, b.slug).is_some() {
            println!("  \x1b[32m✓\x1b[0m {:<14} {} (oauth)", b.slug, display);
        } else if let Some(creds) = load_provider_credentials(storage, b.slug) {
            let plan_info = def
                .and_then(|d| d.plan.as_deref())
                .map(|p| format!(" ({})", p))
                .unwrap_or_default();
            let masked = if creds.api_key.len() > 8 {
                format!(
                    "{}...{}",
                    &creds.api_key[..4],
                    &creds.api_key[creds.api_key.len() - 4..]
                )
            } else {
                "****".to_string()
            };
            println!(
                "  \x1b[32m✓\x1b[0m {:<14} {} (key: {}){}",
                b.slug, display, masked, plan_info
            );
        } else if env::var(b.default_api_key_env).is_ok() {
            println!(
                "  \x1b[33m~\x1b[0m {:<14} {} (via {})",
                b.slug, display, b.default_api_key_env
            );
        } else if def.is_some_and(|d| d.base_url.is_some()) {
            println!("  \x1b[34m●\x1b[0m {:<14} {} (configured)", b.slug, display);
        } else {
            println!(
                "  \x1b[31m✗\x1b[0m {:<14} {} (run: makima auth login {})",
                b.slug, display, b.slug
            );
        }
    }

    for (slug, def) in &config.providers {
        // 'opencode' could show up here, when the user configured free models on that provider.
        if builtin_provider(slug).is_some()
            || registry.contains(slug)
            || (slug == "opencode" && def.enable_free_models.is_some())
        {
            continue;
        }
        let display = def.display_name.as_deref().unwrap_or(slug);
        if let Some(creds) = load_provider_credentials(storage, slug) {
            println!(
                "  \x1b[32m✓\x1b[0m {:<14} {} (key: {})",
                slug,
                display,
                creds.masked_api_key()
            );
        } else {
            let default_env = format!("{}_API_KEY", slug.to_uppercase().replace('-', "_"));
            let env_var = def.api_key_env.as_deref().unwrap_or(&default_env);
            if env::var(env_var).is_ok() {
                println!(
                    "  \x1b[33m~\x1b[0m {:<14} {} (via {})",
                    slug, display, env_var
                );
            } else {
                println!(
                    "  \x1b[31m✗\x1b[0m {:<14} {} (run: makima auth login {})",
                    slug, display, slug
                );
            }
        }
    }
    // Catalog providers from models.dev
    let catalog_entries: Vec<_> = catalog_providers()
        .into_iter()
        .filter(|entry| !registry.contains(&entry.slug))
        .collect();
    if !catalog_entries.is_empty() {
        println!("  \x1b[1mCatalog Providers (models.dev):\x1b[0m");
        for entry in &catalog_entries {
            if let Some(creds) = load_provider_credentials(storage, &entry.slug) {
                println!(
                    "  \x1b[32m✓\x1b[0m {:<14} {} (key: {})",
                    entry.slug,
                    entry.display_name,
                    creds.masked_api_key()
                );
            } else if let Some(env) = entry.env_key_set() {
                println!(
                    "  \x1b[33m~\x1b[0m {:<14} {} (via {})",
                    entry.slug, entry.display_name, env
                );
            } else {
                println!(
                    "  \x1b[31m✗\x1b[0m {:<14} {} (run: makima auth login {})",
                    entry.slug, entry.display_name, entry.slug
                );
            }
        }
        println!();
    }

    Ok(())
}

fn status_line(provider: &ProviderStatus) -> String {
    let AuthStatus { state, detail } = &provider.status;
    let mark = match state {
        AuthState::Saved => SAVED_MARK,
        AuthState::Env => ENV_MARK,
        AuthState::Configured => CONFIGURED_MARK,
        AuthState::Missing => MISSING_MARK,
        AuthState::Failed => FAILED_MARK,
    };
    let detail = match state {
        AuthState::Missing => Some(format!("run: makima auth login {}", provider.id)),
        _ => detail.clone(),
    };
    let detail = detail.map(|d| format!(" ({d})")).unwrap_or_default();
    format!(
        "  {mark} {:<14} {}{detail}",
        provider.id, provider.display_name
    )
}

/// Starts the plugin host with the builtins loaded and builds the provider
/// registry. The registry only works while the host is alive.
pub fn provider_host(
    cwd: &Path,
    no_plugins: bool,
    no_jit: bool,
    trust_mode: TrustMode,
) -> Result<(PluginHost, Arc<ProviderRegistry>, Config)> {
    let trust = project::resolve_noninteractive(cwd, trust_mode);
    load_env_files(&trust.project_config);

    let mut host =
        PluginHost::with_jit(Arc::clone(ToolRegistry::global_arc()), !no_jit, headless())
            .context("initialize lua plugin host")?;
    let mut warnings: Vec<String> = trust.warning.clone().into_iter().collect();
    let config = load_effective_config(&host, no_plugins, &trust.project_config, &mut warnings)?;
    host.load_builtins(&config.plugins)
        .context("load builtin plugins")?;
    let providers = host
        .provider_registry(Timeouts::from(&config.provider))
        .context("build provider registry")?;
    Ok((host, providers, config))
}

pub fn models(
    cwd: &Path,
    no_plugins: bool,
    no_jit: bool,
    refresh: bool,
    trust_mode: TrustMode,
) -> Result<()> {
    let (_host, providers, config) = provider_host(cwd, no_plugins, no_jit, trust_mode)?;

    let mut refresh_failure = None;
    if refresh {
        match maki_providers::refresh_catalog() {
            Ok(()) => eprintln!("models.dev catalog has been refreshed"),
            Err(e) => refresh_failure = Some(e),
        }
    }

    smol::block_on(providers.fetch_all(&config.provider.model_policy, |batch| {
        for model in batch.models {
            println!("{model}");
        }
        for warning in batch.warnings {
            eprintln!("warning: {warning}");
        }
    }));

    if let Some(e) = refresh_failure {
        bail!("catalog refresh failed, keeping existing cache: {e}");
    }
    Ok(())
}

fn load_effective_config(
    host: &PluginHost,
    no_plugins: bool,
    project_config: &ProjectConfig,
    warnings: &mut Vec<String>,
) -> Result<Config> {
    let config = host
        .load_init_files_or_skip(no_plugins, project_config, warnings)
        .context("load init.lua files")?
        .unwrap_or_default()
        .into_config()
        .context("invalid config")?;
    maki_lua::set_allowed_private_hosts(&config.net.allowed_private_hosts);
    Ok(config)
}

pub fn sessions(storage: &StateDir, json: bool) -> Result<()> {
    if !json {
        bail!(SESSIONS_JSON_ONLY_ERR);
    }
    let list = AppSession::list_all(storage).context("list sessions")?;
    println!("{}", serde_json::to_string_pretty(&list)?);
    Ok(())
}

pub fn index(
    cwd: &Path,
    path: &str,
    no_plugins: bool,
    no_jit: bool,
    trust_mode: TrustMode,
) -> Result<()> {
    let trust = project::resolve_noninteractive(cwd, trust_mode);
    load_env_files(&trust.project_config);

    let mut host =
        PluginHost::with_jit(Arc::clone(ToolRegistry::global_arc()), !no_jit, headless())
            .context("initialize lua plugin host")?;

    let mut warnings: Vec<String> = trust.warning.clone().into_iter().collect();
    let raw_config = host
        .load_init_files_or_skip(no_plugins, &trust.project_config, &mut warnings)
        .context("load init.lua files")?;

    let mut config = raw_config
        .unwrap_or_default()
        .into_config()
        .context("invalid config")?;
    maki_lua::set_allowed_private_hosts(&config.net.allowed_private_hosts);
    config.permissions = load_permissions(&trust.project_config)?;

    host.load_builtins(&config.plugins)
        .context("load builtin plugins")?;

    let abs_path = Path::new(path)
        .canonicalize()
        .unwrap_or_else(|_| Path::new(path).to_path_buf());
    let input = serde_json::json!({"path": abs_path.to_str().unwrap_or(path)});
    let reg = ToolRegistry::global_arc();
    let entry = reg
        .get("index")
        .ok_or_else(|| color_eyre::eyre::eyre!("index tool not registered"))?;
    let inv = entry
        .tool
        .parse(&input)
        .map_err(|e| color_eyre::eyre::eyre!("parse index input: {e}"))?;
    let ctx = maki_agent::tools::cli_tool_ctx(cwd.to_path_buf());
    let result = smol::block_on(async { inv.execute(&ctx).await });
    match result.output {
        Ok(output) => print!("{}", output.as_text()),
        Err(e) => bail!("index failed: {e}"),
    }
    Ok(())
}

pub fn mcp_auth(cwd: &Path, server: &str, storage: &StateDir, trust_mode: TrustMode) -> Result<()> {
    smol::block_on(async {
        let trust = project::resolve(storage, cwd, trust_mode);
        let (config, _) = mcp_config::load_config(cwd, trust.project_config);
        let raw = config
            .mcp
            .get(server)
            .ok_or_else(|| color_eyre::eyre::eyre!("unknown MCP server: {server}"))?;
        let (url, oauth) = match mcp_config::parse_server(server.to_owned(), raw.clone())?.transport
        {
            mcp_config::Transport::Http { url, oauth, .. } => (url, oauth),
            _ => color_eyre::eyre::bail!("server '{server}' is not an HTTP transport"),
        };
        mcp_oauth::authenticate(
            server,
            &url,
            None,
            storage,
            mcp_oauth::Interaction::Cli,
            oauth,
        )
        .await?;
        eprintln!("Successfully authenticated with MCP server '{server}'");
        Ok(())
    })
}

pub fn mcp_logout(server: &str, storage: &StateDir) -> Result<()> {
    let deleted = maki_storage::auth::delete_mcp_auth(storage, server)?;
    if deleted {
        eprintln!("Removed OAuth credentials for MCP server '{server}'");
    } else {
        eprintln!("No stored credentials for MCP server '{server}'");
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub fn prompt(
    cwd: &Path,
    variant: &crate::cli::PromptVariant,
    plan: bool,
    tools: bool,
    names: bool,
    no_plugins: bool,
    no_jit: bool,
    trust_mode: TrustMode,
) -> Result<()> {
    use crate::cli::PromptVariant;
    use maki_agent::agent::{build_system_prompt, load_instruction_text};
    use maki_agent::prompt::{PromptId, assemble};
    use maki_agent::template;
    use maki_agent::tools::{DescriptionContext, ToolAudience, ToolFilter, ToolRegistry};

    if plan && !matches!(variant, PromptVariant::System) {
        bail!("--plan can only be used with the 'system' prompt variant");
    }

    let trust = project::resolve_noninteractive(cwd, trust_mode);
    load_env_files(&trust.project_config);

    let vars = template::env_vars_for(cwd);
    let reg = ToolRegistry::global_arc();
    let mut host = PluginHost::with_jit(Arc::clone(reg), !no_jit, headless())
        .context("initialize lua plugin host")?;
    let mut warnings: Vec<String> = trust.warning.clone().into_iter().collect();
    let raw_config = host
        .load_init_files_or_skip(no_plugins, &trust.project_config, &mut warnings)
        .context("load init.lua files")?;
    let config = raw_config
        .unwrap_or_default()
        .into_config()
        .context("invalid config")?;
    maki_lua::set_allowed_private_hosts(&config.net.allowed_private_hosts);
    host.load_builtins(&config.plugins)
        .context("load builtin plugins")?;

    if tools {
        let ctx = DescriptionContext {
            filter: &ToolFilter::All,
            audience: ToolAudience::MAIN,
            workflow: false,
            mcp: false,
        };
        let defs = reg.definitions(&vars, &ctx, true);
        if names {
            for name in defs
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(|d| d["name"].as_str())
            {
                println!("{name}");
            }
        } else {
            println!("{}", serde_json::to_string_pretty(&defs)?);
        }
        return Ok(());
    }

    let cwd_str = cwd.to_string_lossy();
    let instructions = load_instruction_text(&cwd_str);
    let slots = host.event_handle().collect_prompt_slots();

    let output = match variant {
        PromptVariant::System => {
            let mode = if plan {
                maki_agent::AgentMode::Plan(std::path::PathBuf::from("plan.md"))
            } else {
                maki_agent::AgentMode::Build
            };
            let modes = host.event_handle().mode_registry();
            build_system_prompt(&vars, &modes, &mode, &instructions, &slots)
        }
        PromptVariant::Research => assemble(PromptId::Research, &slots, &instructions),
        PromptVariant::General => assemble(PromptId::General, &slots, &instructions),
    };

    print!("{output}");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use maki_lua::test_support::InMemoryFs;
    use maki_providers::login::ChoiceOption;
    use maki_storage::sessions::SessionSummary;
    use std::collections::BTreeSet;
    use std::io::Cursor;
    use tempfile::TempDir;
    use test_case::test_case;

    const SESSION_ID_A: &str = "BWBeN28Vb7cMEx7Ym8AUzs";
    const SESSION_ID_B: &str = "BWBeN28Vb7cMEx7Ym8AUzt";
    const PROVIDER: &str = "fake";
    const DISPLAY_NAME: &str = "Fake";
    const KEY: &str = "sk-fake";
    const PROMPT: &str = "Pick";
    const DEFAULT: &str = "http://localhost:8080";
    const SOURCE: &str = r#"
        maki.api.register_provider({
            id = "fake",
            display_name = "Fake",
            prepare = function() return {} end,
            auth = { { id = "api_key", label = "API key", login = function() return true end } },
        })
    "#;

    fn choice(id: &str) -> ChoiceOption {
        ChoiceOption {
            id: id.into(),
            label: id.into(),
            ..ChoiceOption::default()
        }
    }

    fn answer(request: InteractionRequest, typed: &str) -> Option<String> {
        terminal_interact(request, &mut Cursor::new(typed), &mut Vec::new()).ok()
    }

    #[test]
    fn terminal_choice_reasks_on_bad_number() {
        let request = InteractionRequest::Choice {
            prompt: PROMPT.into(),
            options: vec![choice("a"), choice("b")],
        };
        let mut out = Vec::new();

        let chosen = terminal_interact(request, &mut Cursor::new("0\nx\n3\n2\n"), &mut out);

        assert_eq!(chosen.unwrap(), "b");
        assert_eq!(String::from_utf8(out).unwrap().matches(PROMPT).count(), 4);
    }

    #[test_case("\n", Some(DEFAULT) ; "empty_takes_default")]
    #[test_case("typed\n", Some("typed") ; "typed_wins")]
    #[test_case("", None ; "end_of_input_cancels")]
    fn terminal_input(typed: &str, expected: Option<&str>) {
        let request = InteractionRequest::Input {
            prompt: PROMPT.into(),
            secret: false,
            default: Some(DEFAULT.into()),
        };
        assert_eq!(answer(request, typed).as_deref(), expected);
    }

    #[test]
    fn registered_status_and_logout_use_the_shared_functions() {
        let state = TempDir::new().unwrap();
        maki_storage::paths::init_at(state.path().to_path_buf()).unwrap();
        let storage = StateDir::from_path(state.path().to_path_buf());
        let host = PluginHost::with_session_provider_preparer(
            Arc::new(ToolRegistry::new()),
            Default::default(),
            true,
            Arc::new(InMemoryFs::new()),
            Some(state.path().to_path_buf()),
            None,
            HostUi::Frontend,
        )
        .unwrap();
        host.load_source("fake_plugin", SOURCE).unwrap();
        let registry = host.provider_registry(Timeouts::default()).unwrap();
        let credentials = ProviderCredentials {
            api_key: KEY.into(),
            host: None,
        };
        save_provider_credentials(&storage, PROVIDER, &credentials).unwrap();
        let status = || {
            smol::block_on(login::auth_status(&registry, &storage))
                .iter()
                .map(status_line)
                .find(|line| line.contains(DISPLAY_NAME))
                .unwrap()
        };
        assert!(status().contains(SAVED_MARK));

        auth_logout(PROVIDER, &storage, &registry).unwrap();

        assert!(load_provider_credentials(&storage, PROVIDER).is_none());
        assert!(status().contains(MISSING_MARK));
    }

    fn summary(id: &str, title: &str, updated_at: u64, cwd: &str) -> SessionSummary {
        SessionSummary {
            id: id.parse().unwrap(),
            title: title.into(),
            updated_at,
            cwd: cwd.into(),
            message_count: 0,
            open_elsewhere: false,
        }
    }

    #[test]
    fn sessions_plain_text_is_an_error() {
        let dir = tempfile::tempdir().expect("tempdir");
        let storage = StateDir::from_path(dir.path().to_path_buf());
        let err = sessions(&storage, false).expect_err("plain text output is an error");
        assert_eq!(err.to_string(), SESSIONS_JSON_ONLY_ERR);
    }

    #[test]
    fn sessions_json_pins_the_serialized_fields() {
        let list = vec![
            summary(SESSION_ID_A, "title a", 100, "/a"),
            summary(SESSION_ID_B, "title b", 200, "/b"),
        ];
        let value = serde_json::to_value(&list).unwrap();
        let arr = value.as_array().unwrap();
        assert_eq!(arr.len(), 2);
        for entry in arr {
            let obj = entry.as_object().unwrap();
            let keys: BTreeSet<&str> = obj.keys().map(String::as_str).collect();
            let expected: BTreeSet<_> = [
                "cwd",
                "id",
                "message_count",
                "open_elsewhere",
                "title",
                "updated_at",
            ]
            .into_iter()
            .collect();
            assert_eq!(keys, expected);
            assert!(obj["id"].is_string());
            assert!(obj["title"].is_string());
            assert!(obj["updated_at"].is_u64());
            assert!(obj["cwd"].is_string());
            assert!(!obj["open_elsewhere"].as_bool().unwrap());
        }
    }
}
