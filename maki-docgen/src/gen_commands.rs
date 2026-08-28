use std::fmt::Write;

use maki_commands::BUILTIN_COMMANDS;

use crate::lua_util;

const ALIASING: &str = r#"## Aliasing commands

Prefer a different name for a command? `maki.api.run_command` runs any slash command exactly as typing it would, so an alias is a one-line handler in your `init.lua` instead of a reimplementation.

```lua
-- ~/.config/makima/init.lua
local aliases = {
    { name = "/clear", target = "/new", description = "Alias for /new" },
    { name = "/resume", target = "/sessions", description = "Alias for /sessions" },
}

for _, alias in ipairs(aliases) do
    maki.api.register_command({
        name = alias.name,
        description = alias.description,
        handler = function()
            local ok, err = maki.api.run_command(alias.target)
            if not ok then
                maki.ui.flash("could not run " .. alias.target .. ": " .. err)
            end
        end,
    })
end
```

Both names stay in the palette: aliasing adds a name, it does not rename or hide the original. It works for any command listed above, plus plugin commands and MCP prompts. See [`maki.api.run_command`](/docs/lua-api/#maki-api-run_command) for matching and error handling, or [`maki.ui.action`](/docs/lua-api/#maki-ui-action) to bind a key instead of a name."#;

fn write_row(
    out: &mut String,
    name: &str,
    description: &str,
    argument_hint: Option<&str>,
    tui_only: bool,
) {
    writeln!(
        out,
        "| `{name}` | {} | {} | {} |",
        description.replace('|', "\\|"),
        argument_hint.unwrap_or(""),
        if tui_only { "yes" } else { "no" }
    )
    .unwrap();
}

pub fn generate() -> String {
    let mut out = String::new();
    writeln!(out, "+++").unwrap();
    writeln!(out, "title = \"Commands\"").unwrap();
    writeln!(out, "weight = 8").unwrap();
    writeln!(out, "[extra]").unwrap();
    writeln!(out, "group = \"Reference\"").unwrap();
    writeln!(out, "+++").unwrap();
    writeln!(out).unwrap();
    writeln!(out, "# Commands").unwrap();
    writeln!(out).unwrap();
    writeln!(
        out,
        "Type `/` in the TUI input box to open the command palette. A leading slash command is also recognized in `--print`, SDK stream mode, and ACP. Command names use exact ASCII-insensitive matching. Unknown and unavailable slash-prefixed text remains a model prompt. A known available command with invalid arguments returns an error instead of becoming a prompt."
    )
    .unwrap();
    writeln!(out).unwrap();
    writeln!(
        out,
        "The active registry combines built-ins, custom Markdown commands, MCP prompts, and Lua commands. Lua commands have the highest collision priority, followed by MCP prompts, custom commands, and built-ins. Each frontend advertises its capabilities, and the registry omits commands that require unavailable capabilities. The Lua `tui_only` field maps to the interactive-TUI capability. Registrations can change when plugins reload or MCP servers reconnect. The palette and protocol command lists show the current target-scoped winners. Root CLI subcommands such as `maki auth` are separate from slash commands."
    )
    .unwrap();
    writeln!(out).unwrap();

    writeln!(out, "## Built-in commands").unwrap();
    writeln!(out).unwrap();
    writeln!(out, "| Command | Description | Arguments | TUI-only |").unwrap();
    writeln!(out, "|---------|-------------|-----------|----------|").unwrap();
    for cmd in BUILTIN_COMMANDS {
        let spec = cmd.spec();
        write_row(
            &mut out,
            cmd.name,
            cmd.description,
            spec.docs.argument_hint.as_deref(),
            cmd.required_capabilities
                .contains(maki_commands::TargetCapability::InteractiveUi),
        );
        for alias in cmd.aliases {
            write_row(
                &mut out,
                alias,
                &format!("Alias for `{}`", cmd.name),
                spec.docs.argument_hint.as_deref(),
                cmd.required_capabilities
                    .contains(maki_commands::TargetCapability::InteractiveUi),
            );
        }
    }
    for cmd in &lua_util::load_builtin_plugin_commands() {
        write_row(&mut out, &cmd.name, &cmd.description, None, cmd.tui_only);
    }

    writeln!(out).unwrap();
    writeln!(
        out,
        "The portable built-ins are `/compact`, `/new` (and `/clear`), `/model`, `/cd`, `/btw`, `/yolo`, `/fast`, and `/workflow`. ACP advertises these built-ins plus custom, MCP, and portable Lua commands. Commands that require TUI capabilities are omitted from ACP. Invoking an unavailable command sends the complete input as ordinary model text."
    )
    .unwrap();

    writeln!(out).unwrap();
    writeln!(out, "### Command arguments").unwrap();
    writeln!(out).unwrap();
    writeln!(
        out,
        "`/model` and `/theme` also accept an argument. While you type it, the palette lists the possible values (model specs, theme names), and submitting resolves the argument without opening the picker:"
    )
    .unwrap();
    writeln!(out).unwrap();
    writeln!(
        out,
        "- **`/model <spec>`**: a full `provider/id` spec is used as-is, even if it is not in the discovered list. A fragment is fuzzy-matched against the discovered specs, and a unique match switches to it. Zero or multiple matches flash a note and keep the current model."
    )
    .unwrap();
    writeln!(
        out,
        "- **`/theme <name>`**: the exact name is applied and persisted. A fragment that matches one theme name (like `toky` for `tokyonight`) is resolved the same way; unknown or ambiguous names flash a note and leave the current theme. With no argument, the picker opens and previews each theme as you navigate."
    )
    .unwrap();

    writeln!(out).unwrap();
    writeln!(out, "## Sessions").unwrap();
    writeln!(out).unwrap();
    writeln!(
        out,
        "Sessions run concurrently. `/new` starts a fresh session while the old one keeps working in the background, and `/sessions` shows the live status of each (working, needs input, idle) so you can jump between them. The picker lists this directory's sessions; a session open in another terminal is greyed out and cannot be opened from here. When a background session finishes or needs input, Makima flashes a note in the status bar. `/rename` renames the current session; in the session picker, `Ctrl+N` / `Ctrl+R` / `Ctrl+D` create, rename, and delete."
    )
    .unwrap();

    writeln!(out).unwrap();
    writeln!(out, "## Modes and toggles").unwrap();
    writeln!(out).unwrap();
    writeln!(
        out,
        "- **`/yolo`**: skip permission prompts for this session (deny rules still apply). Config: `always_yolo = true`."
    )
    .unwrap();
    writeln!(
        out,
        "- **`/thinking`**: extended thinking. Optional arg: `off`, `adaptive`, an effort level (`minimal` … `max`), or a token budget number. Config: `always_thinking`."
    )
    .unwrap();
    writeln!(
        out,
        "- **`/fast`**: Anthropic fast mode (Opus only; ignored on other models). Config: `always_fast = true`."
    )
    .unwrap();
    writeln!(
        out,
        "- **`/workflow`**: let `code_execution` call the `task` tool (and other workflow-only tools) from inside the Python sandbox. Config: `always_workflow = true`."
    )
    .unwrap();
    writeln!(
        out,
        "- **Plan / build**: not a slash command. Press `Tab` in the input to toggle plan mode (plan-file writes only)."
    )
    .unwrap();
    writeln!(
        out,
        "- **`/reload`**: rebuild plugins and config without leaving the app."
    )
    .unwrap();
    writeln!(
        out,
        "- **`/btw`**: one-shot side question with no tools and no history pollution."
    )
    .unwrap();
    writeln!(
        out,
        "- **`/memory`**: open the memory file picker (view / edit / delete). See the `memory` tool under [Tools](/docs/tools/)."
    )
    .unwrap();

    writeln!(out).unwrap();
    writeln!(out, "## Custom commands").unwrap();
    writeln!(out).unwrap();
    writeln!(
        out,
        "You can define your own slash commands as Markdown files. Empty files are skipped."
    )
    .unwrap();
    writeln!(out).unwrap();

    writeln!(out, "### Discovery and priority").unwrap();
    writeln!(out).unwrap();
    writeln!(
        out,
        "Later sources override earlier ones when the command **name** matches (the stem of the file, or `name` in frontmatter):"
    )
    .unwrap();
    writeln!(out).unwrap();
    writeln!(
        out,
        "1. User config: `~/.config/makima/commands/` (and legacy `~/.makima/commands/` if present)"
    )
    .unwrap();
    writeln!(out, "2. User third-party: `~/.claude/commands/`").unwrap();
    writeln!(
        out,
        "3. Project dirs, walking from the current working directory up to the nearest `.git` root. At each level: `.makima/commands/`, then `.claude/commands/`"
    )
    .unwrap();
    writeln!(out).unwrap();
    writeln!(
        out,
        "Because the walk goes cwd → … → git root, a command at the **repository root overrides** the same name found only under a nested cwd. Project commands override user commands. Palette names are `/project:<name>` or `/user:<name>` depending on which scope won."
    )
    .unwrap();
    writeln!(out).unwrap();
    writeln!(
        out,
        "Skip all of the above with `--no-commands` (see [CLI](/docs/cli/))."
    )
    .unwrap();
    writeln!(out).unwrap();

    writeln!(out, "### Metadata").unwrap();
    writeln!(out).unwrap();
    writeln!(
        out,
        "You can add optional metadata at the top of the file between `---` lines to set `name`, `description`, and `argument-hint`:"
    )
    .unwrap();
    writeln!(out).unwrap();
    writeln!(out, "```markdown").unwrap();
    writeln!(out, "---").unwrap();
    writeln!(out, "description: Review code for issues").unwrap();
    writeln!(out, "argument-hint: <file>").unwrap();
    writeln!(out, "---").unwrap();
    writeln!(out, "Review $ARGUMENTS and suggest improvements.").unwrap();
    writeln!(out, "```").unwrap();
    writeln!(out).unwrap();

    writeln!(out, "### Arguments").unwrap();
    writeln!(out).unwrap();
    writeln!(
        out,
        "Use `$ARGUMENTS` in the command body. It gets replaced with whatever you type after the command name. The command is treated as accepting args if the body contains `$ARGUMENTS` or you set `argument-hint`."
    )
    .unwrap();
    writeln!(out).unwrap();
    writeln!(
        out,
        "For example, `/project:review main.rs` replaces `$ARGUMENTS` with `main.rs`."
    )
    .unwrap();
    writeln!(out).unwrap();
    writeln!(out, "{ALIASING}").unwrap();

    writeln!(out).unwrap();
    writeln!(
        out,
        "Related: [CLI](/docs/cli/) for shell flags and subcommands, [Skills](/docs/skills/) for on-demand playbooks."
    )
    .unwrap();

    if out.ends_with('\n') {
        out.pop();
    }
    out
}

#[cfg(test)]
mod tests {
    use maki_commands::BUILTIN_COMMANDS;

    use super::generate;

    #[test]
    fn doc_projection_matches_builtin_specs() {
        let generated = generate();
        let mut previous = 0;
        for command in BUILTIN_COMMANDS {
            let row = format!("| `{}` | {} |", command.name, command.description);
            let position = generated[previous..]
                .find(&row)
                .map(|position| previous + position)
                .unwrap_or_else(|| panic!("missing command row: {row}"));
            assert!(position >= previous);
            previous = position + row.len();
            for alias in command.aliases {
                assert!(
                    generated.contains(&format!("| `{alias}` | Alias for `{}` |", command.name))
                );
            }
        }
    }
}
