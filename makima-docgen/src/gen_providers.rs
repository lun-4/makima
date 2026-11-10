use std::fmt::Write;
use std::sync::Arc;

use makima_agent::tools::ToolRegistry;
use makima_lua::PluginHost;
use makima_providers::login::AuthSpec;
use makima_providers::{ModelInfo, ModelTier, ProviderDocs, ProviderListing, ProviderRegistry};

const FRONT_MATTER: &str = r#"+++
title = "Providers"
weight = 5
[extra]
group = "Reference"
+++"#;

const INTRO: &str = "Makima talks to LLM providers over their HTTP APIs. Every provider is registered by a Lua plugin; the built-in ones come from the bundled `providers` plugin. Models are split into three tiers: **weak** (cheap and fast), **medium** (balanced), and **strong** (highest capability, highest cost). There is also a **compaction** tier for choosing a dedicated model to summarize context when the conversation grows long.";

const TIER_PICKER_NOTE: &str = r#"Press `!`, `@`, `#`, or `$` on any row of the model list to assign it to strong, medium, weak, or compaction. Press the same key again to remove the assignment. Your overrides are saved to `~/.local/state/makima/model-tiers` and apply across sessions."#;

const MODEL_PICKER: &str = r#"## Model picker

`/model` without arguments and Ctrl+M open the model picker. The plan form's `Implementation model` row opens the same picker.

Type to filter the list and use Up and Down to move. Tab moves to the options the highlighted model supports, such as thinking and fast mode. Each option starts at `Use current`, which keeps the agent's value. Type a value, or cycle through the choices with Up and Down for thinking or Space, Left and Right for fast. Enter applies the model and its options together, and Esc cancels everything.

Options the highlighted model does not support are hidden. If you set one and then move to a model without it, it is left out, and it comes back when you move to a model that has it.
"#;

const AUTH_RELOADING: &str = r#"## Auth Reloading

Makima re-reads auth from storage and environment variables each time a new agent spawns (`/new`, retry, session load). If you run `makima auth login` in another terminal or change an env var, the next session picks it up without a restart.

An API key variable can hold several keys separated by commas (`sk-1,sk-2,sk-3`). They rotate on rate-limit and auth errors."#;

const SETTINGS: &str = r#"## providers.toml

`providers.toml` lives in the config directory (`~/.config/makima/providers.toml` on Linux and macOS, `%APPDATA%\makima\providers.toml` on Windows). Each section is named after a provider id and holds that provider's settings. `makima auth login` and `/login` write the answers they ask for there once the login works, and `makima auth logout` removes the section.

Most hosted providers read these fields:

| Field | Notes |
|-------|-------|
| `base_url` | Origin of a proxy or compatible endpoint; Makima appends the API paths |
| `api_key_env` | Env var that holds the key, instead of the one listed below |
| `api_key` | Inline key (prefer the env var or `makima auth login`) |
| `headers` | Extra headers sent with every request; values expand `${VAR}` from the environment, and a same-name header replaces the built-in auth header |

A provider that lists a `<ID>_BASE_URL` variable below honors it over `base_url`, so a proxy can be set for one run:

```sh
<ID>_BASE_URL=https://my-proxy.internal makima
```

Local servers list a host variable instead, and the `base_url` saved by `makima auth login` wins over it. Provider-specific fields are described in each provider's section."#;

const MODEL_IDENTIFIERS: &str = r#"## Model Identifiers

Models are referenced as `provider/model_id`:"#;

const NEWER_MODELS: &str = r#"### Models newer than your Makima version

The tables above list the models each provider curates. Any other id a provider accepts works too: type it into `/model` or pass it to `--model`. The picker also lists what the provider's own model endpoint reports, so same-day releases are selectable there.

A model's rates, limits and capabilities come from its provider: what the provider's model endpoint reported this session, else its curated row. Each provider decides how a row matches an id; most match dated snapshots of a row by prefix. An id nothing describes gets a 128K context window, no cost estimate, and no thinking, vision or fast mode.

New models start at the **medium** tier until you assign one in the picker."#;

const NO_CATALOG: &str = "No curated models. The provider lists them once it is configured.";
const EXAMPLE_COUNT: usize = 3;
const ELLIPSIS: &str = "...";
const THOUSAND: u32 = 1_000;
const AGENT_TIERS: [ModelTier; 3] = [ModelTier::Weak, ModelTier::Medium, ModelTier::Strong];

/// The registry the bundled plugins build with no user configuration,
/// credentials or network.
pub(crate) fn builtin_registry() -> Arc<ProviderRegistry> {
    let host =
        PluginHost::with_all_builtins(Arc::new(ToolRegistry::new())).expect("loading builtins");
    host.provider_registry(makima_providers::Timeouts::default())
        .expect("building provider registry")
}

/// The medium-tier default of the first {count} documented providers that
/// have one.
pub(crate) fn example_specs(registry: &ProviderRegistry, count: usize) -> Vec<String> {
    registry
        .listings()
        .filter(|listing| listing.docs.is_some())
        .filter_map(|listing| registry.spec_for_tier(listing.id, ModelTier::Medium))
        .take(count)
        .collect()
}

fn format_k(tokens: u32) -> String {
    format!("{}K", tokens / THOUSAND)
}

fn format_pricing(model: &ModelInfo) -> String {
    model.pricing.as_ref().map_or_else(String::new, |pricing| {
        format!("${:.2} / ${:.2}", pricing.input, pricing.output)
    })
}

fn format_limits(model: &ModelInfo) -> String {
    let context = model
        .context_window
        .map(|window| format!("{} ctx", format_k(window)));
    let output = model
        .max_output_tokens
        .map(|tokens| format!("{} out", format_k(tokens)));
    [context, output]
        .into_iter()
        .flatten()
        .collect::<Vec<_>>()
        .join(" / ")
}

fn code_list<'a>(items: impl IntoIterator<Item = &'a str>) -> String {
    items
        .into_iter()
        .map(|item| format!("`{item}`"))
        .collect::<Vec<_>>()
        .join(", ")
}

fn write_model_table(
    out: &mut String,
    registry: &ProviderRegistry,
    id: &str,
    models: &[ModelInfo],
) {
    let defaults: Vec<String> = AGENT_TIERS
        .iter()
        .filter_map(|tier| registry.spec_for_tier(id, *tier))
        .collect();
    let _ = writeln!(
        out,
        "| Tier | Model | Pricing (in/out per 1M tokens) | Limits |"
    );
    let _ = writeln!(
        out,
        "|------|-------|-------------------------------|--------|"
    );
    // A row per model: prices and limits differ inside a tier.
    for tier in AGENT_TIERS {
        for model in models.iter().filter(|model| model.tier == Some(tier)) {
            let name = if defaults.contains(&format!("{id}/{}", model.id)) {
                format!("**{}** (default)", model.id)
            } else {
                model.id.clone()
            };
            let _ = writeln!(
                out,
                "| {tier} | {name} | {} | {} |",
                format_pricing(model),
                format_limits(model)
            );
        }
    }
    let _ = writeln!(out);
}

fn write_auth(out: &mut String, id: &str, auth: &AuthSpec) {
    if !auth.methods.is_empty() {
        let methods: Vec<&str> = auth.methods.iter().map(|m| m.label.as_str()).collect();
        let _ = writeln!(
            out,
            "- **Login**: `makima auth login {id}` ({})",
            methods.join(", ")
        );
    }
    for alias in &auth.aliases {
        let _ = writeln!(
            out,
            "- **Login alias**: `makima auth login {}`, {}",
            alias.id, alias.label
        );
    }
    for field in &auth.config {
        let choices = code_list(field.choices.iter().map(|choice| choice.id.as_str()));
        let suffix = if choices.is_empty() {
            String::new()
        } else {
            format!(": {choices}")
        };
        let _ = writeln!(
            out,
            "- **`{}`** in `providers.toml`: {}{suffix}",
            field.key, field.prompt
        );
    }
}

fn write_section(
    out: &mut String,
    registry: &ProviderRegistry,
    listing: &ProviderListing<'_>,
    docs: &ProviderDocs,
) {
    // Picker labels such as "Custom provider..." end in an ellipsis.
    let heading = listing.display_name.trim_end_matches(ELLIPSIS);
    let _ = writeln!(out, "### {heading}\n");
    let _ = writeln!(out, "- **Id**: `{}`", listing.id);
    if !docs.env.is_empty() {
        let _ = writeln!(
            out,
            "- **Env vars**: {}",
            code_list(docs.env.iter().map(String::as_str))
        );
    }
    if let Some(endpoint) = &docs.endpoint {
        let _ = writeln!(out, "- **API**: `{endpoint}`");
    }
    write_auth(out, listing.id, listing.auth);
    // Rendered from the schedule, so the docs cannot drift from what is billed.
    if let Some(schedule) = listing
        .models
        .iter()
        .find_map(|model| model.pricing_schedule.as_ref())
    {
        let _ = writeln!(
            out,
            "- **Peak pricing**: the prices below are off-peak; each turn is billed as it happens, at {schedule}"
        );
    }
    let _ = writeln!(out);
    if listing.models.iter().any(|model| model.tier.is_some()) {
        write_model_table(out, registry, listing.id, &listing.models);
    } else if !docs.env.is_empty() {
        let _ = writeln!(out, "{NO_CATALOG}\n");
    }
    if let Some(notes) = &docs.notes {
        let _ = writeln!(out, "{notes}\n");
    }
}

pub fn generate() -> String {
    let registry = builtin_registry();
    let mut out = String::with_capacity(16 * 1024);

    let _ = writeln!(out, "{FRONT_MATTER}\n");
    let _ = writeln!(out, "# Providers\n");
    let _ = writeln!(out, "{INTRO}\n");
    let _ = writeln!(out, "{MODEL_PICKER}\n{TIER_PICKER_NOTE}\n");
    let _ = writeln!(out, "{AUTH_RELOADING}\n");
    let _ = writeln!(out, "{SETTINGS}\n");
    let _ = writeln!(out, "## Built-in Providers\n");
    for listing in registry.listings() {
        if let Some(docs) = listing.docs {
            write_section(&mut out, &registry, &listing, docs);
        }
    }
    let _ = writeln!(out, "{MODEL_IDENTIFIERS}\n");
    let _ = writeln!(out, "```");
    for spec in example_specs(&registry, EXAMPLE_COUNT) {
        let _ = writeln!(out, "{spec}");
    }
    let _ = writeln!(out, "```\n");
    let _ = writeln!(out, "{NEWER_MODELS}");

    out
}
