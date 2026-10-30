use maki_ui::keybindings::{ALT_SEP, KEYBINDS, KeyLabel, KeybindContext, Platform, all_contexts};

const FRONTMATTER: &str = "\
+++
title = \"Keybindings\"
weight = 9
[extra]
group = \"Reference\"
+++";

const LUA_CONTEXT_BINDS: &[(&str, &str, &str)] = &[
    ("Session Picker", "`Ctrl+N`", "New session"),
    ("Session Picker", "`Ctrl+R`", "Rename session"),
    ("Session Picker", "`Ctrl+D`", "Delete session (press twice)"),
];

// Built-in plugins own these globally, so they never reach `KEYBINDS`.
const PLUGIN_BINDS: &[(&str, &str)] = &[("`Ctrl+P`", "Browse sessions")];

const MAIN_CONTEXTS: &[KeybindContext] = &[
    KeybindContext::General,
    KeybindContext::Editing,
    KeybindContext::Streaming,
    KeybindContext::FormInput,
    KeybindContext::Picker,
];

fn label_str(label: KeyLabel) -> String {
    match label {
        KeyLabel::Single(s) => format!("`{s}`"),
        KeyLabel::Alt(a, b) => format!("`{a}`{ALT_SEP}`{b}`"),
        KeyLabel::MacAlt(a, _) => format!("`{a}`"),
        KeyLabel::MacMulti(normal, _) => normal
            .iter()
            .map(|s| format!("`{s}`"))
            .collect::<Vec<_>>()
            .join(ALT_SEP),
    }
}

fn write_table_2col(out: &mut String, rows: &[(String, &str)]) {
    out.push_str("| Key | Action |\n|-----|--------|\n");
    for (key, desc) in rows {
        out.push_str(&format!("| {key} | {desc} |\n"));
    }
}

fn write_section(out: &mut String, ctx: KeybindContext) {
    out.push_str(&format!("\n## {}\n\n", ctx.label()));

    let all_rows: Vec<_> = KEYBINDS.iter().filter(|kb| kb.context == ctx).collect();

    let normal: Vec<_> = all_rows
        .iter()
        .filter(|kb| kb.platform == Platform::All)
        .map(|kb| (label_str(kb.label), kb.description))
        .collect();

    if !normal.is_empty() {
        write_table_2col(out, &normal);
    }

    let mac_only: Vec<_> = all_rows
        .iter()
        .filter(|kb| kb.platform == Platform::MacOnly)
        .map(|kb| (label_str(kb.label), kb.description))
        .collect();

    if !mac_only.is_empty() {
        out.push_str("\n### macOS-specific\n\n");
        write_table_2col(out, &mac_only);
    }
}

fn write_context_specific(out: &mut String) {
    let child_binds: Vec<_> = KEYBINDS
        .iter()
        .filter(|kb| kb.context.parent().is_some())
        .collect();

    if child_binds.is_empty() {
        return;
    }

    out.push_str("\n## Context-Specific\n\n");
    out.push_str("Some pickers add extra bindings on top of the defaults:\n\n");
    out.push_str("| Context | Key | Action |\n|---------|-----|--------|\n");

    for kb in &child_binds {
        let key = label_str(kb.label);
        out.push_str(&format!(
            "| {} | {key} | {} |\n",
            kb.context.label(),
            kb.description
        ));
    }

    for (ctx, key, desc) in LUA_CONTEXT_BINDS {
        out.push_str(&format!("| {ctx} | {key} | {desc} |\n"));
    }
}

fn write_model_picker(out: &mut String) {
    out.push_str("\n## Model option controls\n\n");
    out.push_str("The [model picker](/docs/providers/#model-picker) uses these controls for ordinary model changes and plan approval. Only available options receive controls.\n\n");
    write_table_2col(
        out,
        &[
            (
                "`Tab` / `Shift+Tab`".into(),
                "Move between the model list and available option controls",
            ),
            (
                "`Up` / `Down`".into(),
                "Select a model with the list focused; cycle presets and Use current in free-input controls",
            ),
            (
                "`Space` / `Left` / `Right`".into(),
                "Cycle choices and Use current in choice controls",
            ),
            (
                "`Ctrl+R`".into(),
                "Clear only retained overrides that are unavailable or pending",
            ),
            (
                "`Enter`".into(),
                "Validate and confirm the model and option draft together",
            ),
            (
                "`Esc` / `Ctrl+C`".into(),
                "Cancel the whole draft without changing the prior selection",
            ),
        ],
    );
    out.push_str("\nTyping and paste edit only the focused control. Tier shortcuts apply only while the model list is focused. Conflicting overrides remain visible and block confirmation until cleared or supported by the highlighted model.\n");
}

fn write_plugin_binds(out: &mut String) {
    out.push_str("\n## Plugins\n\n");
    out.push_str("Built-in plugins register these themselves, and your own plugins can add more with `maki.keymap.set`:\n\n");
    out.push_str("| Key | Action |\n|-----|--------|\n");
    for (key, desc) in PLUGIN_BINDS {
        out.push_str(&format!("| {key} | {desc} |\n"));
    }
}

fn write_inheritance(out: &mut String) {
    let children: Vec<_> = all_contexts()
        .filter(|ctx| ctx.parent().is_some())
        .collect();

    if children.is_empty() {
        return;
    }

    out.push_str("\n## Context Inheritance\n\n");
    out.push_str("Child contexts inherit their parent's bindings and add their own.\n\n");

    let mut by_parent: Vec<(KeybindContext, Vec<&str>)> = Vec::new();
    for child in &children {
        let parent = child.parent().unwrap();
        if let Some(entry) = by_parent.iter_mut().find(|(p, _)| *p == parent) {
            entry.1.push(child.label());
        } else {
            by_parent.push((parent, vec![child.label()]));
        }
    }

    for (parent, kids) in &by_parent {
        let list = kids.join(", ");
        out.push_str(&format!(
            "- **{}** is the base for: {list}\n",
            parent.label()
        ));
    }
}

pub fn generate() -> String {
    let mut out = String::from(FRONTMATTER);
    out.push_str("\n\n# Keybindings\n\n");
    out.push_str("On macOS, some bindings use Option or Fn keys instead (run `/help` for exact keybindings).\n");

    for &ctx in MAIN_CONTEXTS {
        write_section(&mut out, ctx);
    }

    write_context_specific(&mut out);
    write_model_picker(&mut out);
    write_plugin_binds(&mut out);
    write_inheritance(&mut out);
    write_overrides(&mut out);

    out
}

fn write_overrides(out: &mut String) {
    out.push_str("\n## Overriding Keybindings\n\n");
    out.push_str(
        "Plugins and `init.lua` can rebind keys at runtime with \
         `maki.keymap.set` and `maki.keymap.del`. The tables above are the \
         built-in defaults. An override on the same key wins, unless a \
         modal or overlay is open (help, plan form, permission prompt).\n\n",
    );
    out.push_str("Precedence, high to low:\n\n");
    out.push_str(
        "1. **Suspend** (`Ctrl+Z`, Unix). Always wins, non-remappable.\n\
         2. **Modal and overlay keys.** An open modal or picker consumes \
         its keys first, so they cannot be shadowed while open.\n\
         3. **Lua overrides** from `maki.keymap.set`. Last set wins; \
         binding the same key twice warns.\n\
         4. **Built-in defaults.** An override on the same key shadows \
         them; `maki.keymap.del` lifts the override so the default returns. \
         Suspend is the only binding outside this layer, so every key is \
         remappable except `Ctrl+Z`.\n\n",
    );
    out.push_str(
        "Only single-key bindings can be overridden. Multi-key combinations \
         and non-key rows (like `Type` to filter) cannot.\n\n",
    );
    out.push_str(
        "The `/help` modal and the splash show default labels, not live \
         overrides, but pressing the key still runs the override.\n\n",
    );
    out.push_str("### Recovering from a bad keymap\n\n");
    out.push_str(
        "If an override leaves Makima stuck (a rebound `Ctrl+C`, a modal \
         that won't close, a plugin that throws on load), boot without \
         user `init.lua`:\n\n",
    );
    out.push_str("```bash\nmakima --no-plugins\n```\n\n");
    out.push_str(
        "Skips user `init.lua` files (global and project) but keeps the \
         Lua host and builtin plugins running, so tools still work. \
         `permissions.toml`, custom commands, and env files load as \
         usual.\n\n",
    );
    out.push_str(
        "The default keymap lives in Rust, not Lua, so `--no-plugins` \
         never drops it.\n\n",
    );
    out.push_str("## Shell and images\n\n");
    out.push_str(
        "These are input conventions, not remappable key rows:\n\n\
         - Prefix a line with `!` to run a shell command yourself (5 minute \
         timeout). Use `!!` to hide the command and its output from the agent.\n\
         - `Ctrl+V` pastes an image from the clipboard into the prompt when the \
         model supports vision. You can also paste image file paths.\n",
    );
}

#[cfg(test)]
mod tests {
    #[test]
    fn generated_model_picker_keybindings_include_option_controls() {
        let generated = super::generate();
        let controls = generated.split("## Model option controls").nth(1).unwrap();
        for expected in [
            "`Tab` / `Shift+Tab`",
            "`Up` / `Down`",
            "`Space` / `Left` / `Right`",
            "`Ctrl+R`",
            "`Enter`",
            "`Esc` / `Ctrl+C`",
            "unavailable or pending",
        ] {
            assert!(controls.contains(expected), "{expected}");
        }
    }
}
