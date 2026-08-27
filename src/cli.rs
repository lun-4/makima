use std::path::PathBuf;

use clap::{Parser, Subcommand, ValueEnum};
use color_eyre::Result;
use color_eyre::eyre::bail;

use maki_agent::tools::{all_builtin_tool_names, is_builtin_tool};

use crate::print::OutputFormat;

#[derive(Clone, ValueEnum, Default)]
pub enum PromptVariant {
    #[default]
    System,
    Research,
    General,
}

#[derive(Clone, ValueEnum, Default)]
pub enum InputFormat {
    #[default]
    Text,
    StreamJson,
}

#[derive(Parser)]
#[command(name = "makima", version, about = "AI coding agent for the terminal")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Option<Command>,

    /// Non-interactive mode. Runs the prompt and exits. Compatible with Claude Code's --print flag
    #[arg(short, long)]
    pub print: bool,

    /// Attach an image to the prompt in --print mode as vision content (repeatable)
    #[arg(long = "image", value_name = "PATH")]
    pub images: Vec<PathBuf>,

    /// Model spec (provider/model-id). Defaults to last used model, or claude-opus-4-6
    #[arg(short, long)]
    pub model: Option<String>,

    /// Include full turn-by-turn messages in --print output
    #[arg(long)]
    pub verbose: bool,

    /// Continue the most recent session in this directory
    #[arg(short = 'l', long = "last")]
    pub last_session: bool,

    /// Continue a session by ID. With no ID (TUI only), open the session picker for this directory
    #[arg(short = 'c', long = "continue", num_args = 0..=1, value_name = "ID")]
    pub continue_session: Option<Option<String>>,

    /// Output format for --print mode
    #[arg(long, value_enum, default_value_t = OutputFormat::Text)]
    pub output_format: OutputFormat,

    /// Input format (text or stream-json for SDK mode)
    #[arg(long, value_enum, default_value_t = InputFormat::Text)]
    pub input_format: InputFormat,

    /// Skip loading custom commands from .makima/commands, .claude/commands, etc.
    #[arg(long)]
    pub no_commands: bool,

    /// Disable rtk command rewriting
    #[arg(long)]
    pub no_rtk: bool,

    /// Skip user `init.lua` files (global and project) but keep the Lua
    /// host and every builtin plugin running, so tools and the default
    /// keymap still load. Use this to recover from a broken `init.lua`
    /// or keymap override. Only Lua `init.lua` files are affected;
    /// `permissions.toml`, custom commands, and env files load as usual.
    #[arg(long)]
    pub no_plugins: bool,

    /// Run plugin Lua on the interpreter with full debug info (no native codegen)
    #[arg(long)]
    pub no_jit: bool,

    /// Skip all permission prompts (allow everything)
    #[arg(long, alias = "dangerously-skip-permissions")]
    pub yolo: bool,

    /// Gate every bash command through the auto-mode classifier
    #[arg(long)]
    pub automode: bool,

    /// Exit after the agent completes (for automation workflows)
    #[arg(long)]
    pub exit_on_done: bool,

    /// Pre-approve tools (comma-separated). Accepts PascalCase (Claude Code) or snake_case.
    #[arg(long, value_delimiter = ',', visible_alias = "allowedTools")]
    pub allowed_tools: Vec<String>,

    /// Disallowed tools (comma-separated).
    #[arg(long, value_delimiter = ',', visible_alias = "disallowedTools")]
    pub disallowed_tools: Vec<String>,

    /// Session ID for SDK mode
    #[arg(long)]
    pub session_id: Option<String>,

    /// Fork the loaded session under a new ID
    #[arg(long)]
    pub fork_session: bool,

    /// Maximum number of agent turns
    #[arg(long)]
    pub max_turns: Option<u32>,

    /// System prompt override
    #[arg(long)]
    pub system_prompt: Option<String>,

    /// Append to system prompt
    #[arg(long)]
    pub append_system_prompt: Option<String>,

    /// Permission mode for SDK
    #[arg(long)]
    pub permission_mode: Option<String>,

    /// Include partial streaming messages in SDK output
    #[arg(long)]
    pub include_partial_messages: bool,

    /// Permission prompt tool (accepted for compat, used in SDK mode)
    #[arg(long, hide = true)]
    pub permission_prompt_tool: Option<String>,

    // Accepted but ignored, so Claude Code SDK callers don't break.
    #[arg(long, hide = true)]
    pub fallback_model: Option<String>,
    #[arg(long, hide = true)]
    pub settings: Option<String>,
    #[arg(long, hide = true)]
    pub setting_sources: Option<String>,
    #[arg(long, hide = true)]
    pub add_dir: Option<String>,
    #[arg(long, hide = true)]
    pub strict_mcp_config: bool,
    #[arg(long, hide = true)]
    pub include_hook_events: bool,
    #[arg(long, hide = true)]
    pub mcp_config: Option<String>,
    #[arg(long, hide = true)]
    pub tools: Option<String>,
    #[arg(long, hide = true)]
    pub betas: Option<String>,
    #[arg(long, hide = true)]
    pub max_thinking_tokens: Option<String>,
    #[arg(long, hide = true)]
    pub effort: Option<String>,
    #[arg(long, hide = true)]
    pub json_schema: Option<String>,
    #[arg(long, hide = true)]
    pub max_budget_usd: Option<String>,
    #[arg(long, hide = true)]
    pub thinking: Option<String>,
    #[arg(long, hide = true)]
    pub thinking_display: Option<String>,

    /// Initial prompt (reads stdin if piped)
    #[arg(value_name = "PROMPT")]
    pub initial_prompt: Option<String>,
}

impl Cli {
    pub fn warn_ignored_flags(&self) {
        let ignored = [
            ("fallback-model", self.fallback_model.is_some()),
            ("settings", self.settings.is_some()),
            ("setting-sources", self.setting_sources.is_some()),
            ("add-dir", self.add_dir.is_some()),
            ("strict-mcp-config", self.strict_mcp_config),
            ("include-hook-events", self.include_hook_events),
            ("mcp-config", self.mcp_config.is_some()),
            ("tools", self.tools.is_some()),
            ("betas", self.betas.is_some()),
            ("max-thinking-tokens", self.max_thinking_tokens.is_some()),
            ("effort", self.effort.is_some()),
            ("json-schema", self.json_schema.is_some()),
            ("max-budget-usd", self.max_budget_usd.is_some()),
            ("thinking", self.thinking.is_some()),
            ("thinking-display", self.thinking_display.is_some()),
        ];
        for (flag, set) in &ignored {
            if *set {
                eprintln!("warning: --{flag} is accepted but ignored");
            }
        }
    }

    pub fn is_sdk_mode(&self) -> bool {
        self.print && matches!(self.input_format, InputFormat::StreamJson)
    }
}

#[derive(Subcommand)]
pub enum Command {
    /// Manage API authentication
    Auth {
        #[command(subcommand)]
        action: AuthAction,
    },
    /// List all available models
    Models,
    /// List stored sessions as JSON
    Sessions {
        #[arg(long)]
        json: bool,
    },
    /// Run the index tool on a file to see how it looks like
    Index { path: String },
    /// Manage MCP server authentication
    Mcp {
        #[command(subcommand)]
        action: McpAction,
    },
    /// Update makima to the latest version
    Update {
        /// Skip confirmation prompt
        #[arg(short = 'y', long)]
        yes: bool,
        /// Disable syntax highlighting
        #[arg(long)]
        no_color: bool,
    },
    /// Rollback to the previous version
    Rollback,
    /// Run as an ACP (Agent Client Protocol) server over stdio
    Acp {
        /// Model spec (provider/model-id)
        #[arg(short, long)]
        model: Option<String>,
        /// Skip all permission prompts
        #[arg(long)]
        yolo: bool,
        /// Gate every bash command through the auto-mode classifier
        #[arg(long)]
        automode: bool,
    },
    /// Show the rendered system prompt or tool definitions
    Prompt {
        /// Prompt variant: system (default), research, general
        #[arg(value_enum, default_value_t = PromptVariant::System)]
        variant: PromptVariant,
        /// Append the plan mode reminder to the system prompt
        #[arg(long)]
        plan: bool,
        /// Show tool definitions (JSON) instead of prompt text
        #[arg(long)]
        tools: bool,
        /// With --tools: show only tool names, one per line
        #[arg(long, requires = "tools")]
        names: bool,
    },
    /// Data migration utilities
    Migrate {
        #[command(subcommand)]
        action: MigrateAction,
    },
}

#[derive(Subcommand)]
pub enum MigrateAction {
    /// Migrate files from ~/.makima/ to XDG directories
    Xdg,
}

#[derive(Subcommand)]
pub enum McpAction {
    /// Authenticate with an MCP server
    Auth {
        /// Server name from config
        server: String,
    },
    /// Remove stored OAuth credentials for an MCP server
    Logout {
        /// Server name from config
        server: String,
    },
}

#[derive(Subcommand)]
pub enum AuthAction {
    /// Authenticate with a provider (interactive if no provider specified)
    Login {
        /// Provider slug (e.g. zai, openai, xai). Omit for interactive selection.
        provider: Option<String>,
    },
    /// Remove stored credentials for a provider
    Logout {
        /// Provider slug (e.g. openai, xai)
        provider: String,
    },
    /// Show authentication status for all providers
    Status,
}

pub fn normalize_tool_name(name: &str) -> Result<String> {
    let mut result = String::with_capacity(name.len() + 4);
    for (i, c) in name.chars().enumerate() {
        if c.is_ascii_uppercase() {
            if i > 0 {
                result.push('_');
            }
            result.push(c.to_ascii_lowercase());
        } else {
            result.push(c);
        }
    }
    if !is_builtin_tool(&result) {
        bail!(
            "unknown tool '{}'. Valid tools: {}",
            name,
            all_builtin_tool_names().join(", ")
        );
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_case::test_case;

    #[test_case("Read", "read")]
    #[test_case("Bash", "bash")]
    #[test_case("CodeExecution", "code_execution")]
    #[test_case("code_execution", "code_execution"; "snake_passthrough")]
    fn normalize_tool_name_valid_inputs(input: &str, expected: &str) {
        assert_eq!(normalize_tool_name(input).unwrap(), expected);
    }

    #[test]
    fn normalize_tool_name_rejects_unknown() {
        let result = normalize_tool_name("NonExistentTool");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("unknown tool"));
    }

    #[test]
    fn normalize_tool_name_multi_edit_rejects_snake_variant() {
        assert!(normalize_tool_name("MultiEdit").is_err());
    }

    #[test]
    fn no_continue_flags_parse_as_absent() {
        let cli = Cli::parse_from(["makima"]);
        assert_eq!(cli.continue_session, None);
        assert!(!cli.last_session);
    }

    #[test_case(&["makima", "-c"]; "short_bare")]
    #[test_case(&["makima", "--continue"]; "long_bare")]
    fn bare_continue_parses_as_picker_request(args: &[&str]) {
        let cli = Cli::parse_from(args);
        assert_eq!(cli.continue_session, Some(None));
        assert!(!cli.last_session);
    }

    #[test_case(&["makima", "-c", "abc"]; "short")]
    #[test_case(&["makima", "--continue", "abc"]; "long")]
    fn valued_continue_parses_the_id(args: &[&str]) {
        let cli = Cli::parse_from(args);
        assert_eq!(cli.continue_session, Some(Some("abc".into())));
        assert!(!cli.last_session);
    }

    #[test_case(&["makima", "-l"]; "short")]
    #[test_case(&["makima", "--last"]; "long")]
    fn last_flag_parsing(args: &[&str]) {
        let cli = Cli::parse_from(args);
        assert!(cli.last_session);
        assert_eq!(cli.continue_session, None);
    }

    #[test]
    fn bare_continue_does_not_swallow_a_following_flag() {
        let cli = Cli::parse_from(["makima", "-c", "--yolo"]);
        assert_eq!(cli.continue_session, Some(None));
        assert!(cli.yolo);
    }

    #[test_case(&["makima", "--resume"]; "resume")]
    #[test_case(&["makima", "--session"]; "session")]
    #[test_case(&["makima", "--continue-last"]; "continue_last")]
    #[test_case(&["makima", "-s"]; "short_s")]
    fn legacy_resume_flags_are_rejected(args: &[&str]) {
        assert!(
            Cli::try_parse_from(args).is_err(),
            "{args:?} must be rejected"
        );
    }
}
