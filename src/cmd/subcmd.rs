use std::io::{self, BufRead, IsTerminal, Write};
use std::path::Path;
use std::process::Command;
use std::sync::Arc;

use color_eyre::Result;
use color_eyre::eyre::{Context, bail};
use crossterm::event::{Event, KeyCode, KeyEventKind, KeyModifiers};

use maki_agent::mcp::{config as mcp_config, oauth as mcp_oauth};
use maki_agent::tools::ToolRegistry;
use maki_config::project::{self, ProjectConfig, TrustMode};
use maki_config::{Config, ModelPolicy, load_env_files, load_permissions};
use maki_lua::PluginHost;
use maki_lua::ui_sink::{HeadlessUi, HostUi};
use maki_providers::login::{
    self, AuthState, AuthStatus, InteractionRequest, InteractionService, LoginOutcome,
    LoginRequest, ProviderStatus, drive_login,
};
use maki_providers::provider::BoxFuture;
use maki_providers::{AgentError, ProviderRegistry, Timeouts};
use maki_storage::StateDir;
use maki_ui::AppSession;

const SESSIONS_JSON_ONLY_ERR: &str =
    "the sessions command only supports --json; use `makima sessions --json`";
const SAVED_MARK: &str = "\x1b[32m✓\x1b[0m";
const ENV_MARK: &str = "\x1b[33m~\x1b[0m";
const CONFIGURED_MARK: &str = "\x1b[34m●\x1b[0m";
const MISSING_MARK: &str = "\x1b[31m✗\x1b[0m";
const FAILED_MARK: &str = "\x1b[31m!\x1b[0m";
const EMPTY_COMMAND_ERR: &str = "no program to run";

/// Subcommands have no UI, so their hosts print notifications to stderr.
fn headless() -> HostUi {
    HostUi::Sink(Box::new(HeadlessUi))
}

pub fn auth_login(
    provider: Option<&str>,
    storage: &StateDir,
    registry: &ProviderRegistry,
    model_policy: &ModelPolicy,
) -> Result<()> {
    let request = LoginRequest {
        provider: provider.map(str::to_owned),
    };
    let ui = Arc::new(TerminalInteraction);
    let LoginOutcome {
        display_name,
        model,
        ..
    } = smol::block_on(drive_login(registry, ui, storage, model_policy, request))?;
    println!();
    println!("  {SAVED_MARK} Authenticated: {display_name}");
    if let Some(model) = model {
        println!("  Default model: {model}");
    }
    Ok(())
}

/// Answers the login driver on the terminal, off the plugin host's thread.
struct TerminalInteraction;

impl InteractionService for TerminalInteraction {
    fn interact(&self, request: InteractionRequest) -> BoxFuture<'_, Result<String, AgentError>> {
        Box::pin(smol::unblock(move || {
            if let InteractionRequest::OpenBrowser(url) = &request
                && let Err(error) = open::that_detached(url)
            {
                tracing::warn!(%error, url, "failed to open browser");
            }
            if let InteractionRequest::Input {
                prompt,
                secret: true,
                default,
            } = &request
                && io::stdin().is_terminal()
            {
                return terminal_secret_input(prompt, default.clone());
            }
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
            prompt,
            secret,
            default,
        } => {
            match (&default, secret) {
                (Some(default), false) => write!(out, "  {prompt} [{default}]: ")?,
                _ => write!(out, "  {prompt}: ")?,
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
        InteractionRequest::Terminal(argv) => {
            run_on_terminal(&argv)?;
            Ok(String::new())
        }
    }
}

/// Hands the terminal to {argv} until it exits.
fn run_on_terminal(argv: &[String]) -> Result<(), AgentError> {
    let (program, args) = argv.split_first().ok_or_else(|| AgentError::Config {
        message: EMPTY_COMMAND_ERR.into(),
    })?;
    let status = Command::new(program)
        .args(args)
        .status()
        .map_err(|error| AgentError::Config {
            message: format!("failed to run {program}: {error}"),
        })?;
    match status.success() {
        true => Ok(()),
        false => Err(AgentError::Config {
            message: format!("{program} exited with {status}"),
        }),
    }
}

fn terminal_secret_input(prompt: &str, default: Option<String>) -> Result<String, AgentError> {
    struct RawMode;

    impl Drop for RawMode {
        fn drop(&mut self) {
            let _ = crossterm::terminal::disable_raw_mode();
            let _ = writeln!(io::stdout());
        }
    }

    print!("  {prompt}: ");
    io::stdout().flush()?;
    crossterm::terminal::enable_raw_mode()?;
    let _raw_mode = RawMode;
    let mut answer = String::new();
    loop {
        let Event::Key(key) = crossterm::event::read()? else {
            continue;
        };
        if !matches!(key.kind, KeyEventKind::Press | KeyEventKind::Repeat) {
            continue;
        }
        match key.code {
            KeyCode::Enter => {
                return Ok(if answer.is_empty() {
                    default.unwrap_or_default()
                } else {
                    answer
                });
            }
            KeyCode::Backspace => {
                answer.pop();
            }
            KeyCode::Esc => return Err(AgentError::Cancelled),
            KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                return Err(AgentError::Cancelled);
            }
            KeyCode::Char(c) => answer.push(c),
            _ => {}
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

pub fn auth_logout(provider: &str, storage: &StateDir, registry: &ProviderRegistry) -> Result<()> {
    match smol::block_on(login::logout(registry, storage, provider))? {
        Some(name) => println!("Logged out of {name}."),
        None => bail!("unknown provider '{provider}'"),
    }
    Ok(())
}

pub fn auth_status(storage: &StateDir, registry: &ProviderRegistry) -> Result<()> {
    println!();
    let mut section = None;
    let statuses = smol::block_on(login::auth_status(registry, storage));
    for provider in statuses.iter().filter(|provider| provider.offers_models) {
        if let Some(heading) = provider.section.as_deref()
            && section != Some(heading.to_owned())
        {
            println!("  \x1b[1m{heading}:\x1b[0m");
            section = provider.section.clone();
        }
        println!("{}", status_line(provider));
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

    smol::block_on(
        providers.fetch_all(&config.provider.model_policy, refresh, |batch| {
            for model in batch.models {
                println!("{model}");
            }
            for warning in batch.warnings {
                eprintln!("warning: {warning}");
            }
        }),
    );
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
    use maki_storage::auth::{load_credentials, save_credentials};
    use maki_storage::sessions::SessionSummary;
    use serde_json::json;
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
    fn terminal_secret_input_does_not_show_saved_value() {
        let request = InteractionRequest::Input {
            prompt: PROMPT.into(),
            secret: true,
            default: Some(KEY.into()),
        };
        let mut out = Vec::new();

        let answer = terminal_interact(request, &mut Cursor::new("\n"), &mut out).unwrap();
        let shown = String::from_utf8(out).unwrap();

        assert_eq!(answer, KEY);
        assert!(shown.contains(PROMPT));
        assert!(!shown.contains(KEY));
    }

    #[test]
    fn piped_secret_input_uses_line_input_without_echoing_default() {
        let request = InteractionRequest::Input {
            prompt: PROMPT.into(),
            secret: true,
            default: Some(KEY.into()),
        };
        let mut out = Vec::new();

        let answer = terminal_interact(request, &mut Cursor::new("piped-key\n"), &mut out).unwrap();
        let shown = String::from_utf8(out).unwrap();

        assert_eq!(answer, "piped-key");
        assert!(shown.contains(PROMPT));
        assert!(!shown.contains(KEY));
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
        save_credentials(&storage, PROVIDER, &json!({ "api_key": KEY })).unwrap();
        let status = || {
            smol::block_on(login::auth_status(&registry, &storage))
                .iter()
                .map(status_line)
                .find(|line| line.contains(DISPLAY_NAME))
                .unwrap()
        };
        assert!(status().contains(SAVED_MARK));

        auth_logout(PROVIDER, &storage, &registry).unwrap();

        assert!(load_credentials(&storage, PROVIDER).is_none());
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
