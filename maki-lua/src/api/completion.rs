//! `maki.api` completion sources and submit-time expanders.
//!
//! Plugins own the non-file `@`-reference story end to end. A *completion
//! source* feeds the popup (`@`-token candidates shown as the user types); an
//! *expander* rewrites a recognized `@prefix:value` token in-place at submit.
//! Both are keyed by prefix and live in `Lua::app_data` stores, so they share
//! `Lua`'s lifetime and are dropped with the owning plugin on unload — the same
//! pattern as commands and keymaps. The UI reads them through synchronous
//! `Request` RPCs on `EventHandle` (see `collect_completion_items` /
//! `expand_references`), never touching `Lua` directly.

use std::collections::HashMap;
use std::sync::Arc;

use maki_lua_macro::{lua_fn, lua_table};
use mlua::{Function, Lua, MultiValue, Result as LuaResult, Table, Value};
use serde::Deserialize;

use crate::api::util::command::{CommandArgumentItem, CommandHandlerMap};
use crate::api::util::convert::{json_to_lua, lua_to_json};
use crate::api::util::pair::Pair;
use crate::runtime::{
    CommandArgumentContext, CommandArgumentLifecycle, CommandArgumentLifecycleRequest,
};

/// An `@` at `at_byte` begins a token only if nothing but whitespace precedes
/// it (or it starts the text). Shared by the popup (via `maki-ui`) and the
/// expander parser so both agree on what counts as a reference.
pub fn at_is_token_start(text: &str, at_byte: usize) -> bool {
    text[..at_byte]
        .chars()
        .next_back()
        .is_none_or(char::is_whitespace)
}

/// One candidate offered by a completion source. `label` is the fuzzy-match
/// target and display text; `kind` drives rendering via `Theme::completion_kinds`;
/// `insertion` replaces the whole `@`-token (including its leading `@`); the
/// optional `description` is shown beside the label.
#[derive(Debug, Clone, Deserialize)]
pub struct ItemSpec {
    pub label: String,
    pub kind: String,
    pub insertion: String,
    #[serde(default)]
    pub description: Option<String>,
}

/// Context handed to a source's `get_items` at popup-open time. `mode` is the
/// active mode id ("build"/"plan"/custom); `models` is the available-models
/// list the caller already loaded. Both are passed in by value so sources stay
/// synchronous and never need the async `mode.get()` round-trip.
#[derive(Debug, Clone, Default)]
pub struct CompletionCtx {
    pub mode: String,
    pub models: Vec<String>,
}

/// `plugin -> prefix -> get_items fn`. One source per prefix per plugin;
/// re-registering a prefix drops the previous function. A newtype so its
/// `app_data` slot is distinct from `ExpanderStore` (both hold the same shape).
#[derive(Default)]
pub(crate) struct CompletionStore(pub(crate) HashMap<Arc<str>, HashMap<String, Function>>);

/// `plugin -> prefix -> expander fn`. Aliases are separate prefix entries.
#[derive(Default)]
pub(crate) struct ExpanderStore(pub(crate) HashMap<Arc<str>, HashMap<String, Function>>);

fn install_store<T: Default + Send + Sync + 'static>(lua: &Lua) {
    if lua.app_data_ref::<T>().is_none() {
        lua.set_app_data(T::default());
    }
}

/// Register a completion source for `prefix`. The source's `get_items(ctx)` is
/// called once when the `@` popup opens; `ctx` is `{ mode = "...", models = {...} }`.
/// Returns its candidates as an array of `{ label, kind, insertion, description? }`.
///
/// @param prefix string The prefix this source owns (e.g. "skill"); labels typically carry the prefix (`skill:review`) so the fuzzy filter narrows by kind as the user types.
/// @param spec table `{ get_items = function(ctx) -> { {label, kind, insertion, description?} } }`.
/// @return
/// @example
/// maki.api.register_completion_source("skill", {
///   get_items = function(ctx)
///     return { { label = "skill:review", kind = "skill", insertion = "@skill:review" } }
///   end,
/// })
#[lua_fn]
fn register_completion_source(
    lua: &Lua,
    #[ctx] plugin: Arc<str>,
    prefix: String,
    spec: Table,
) -> LuaResult<()> {
    let get_items: Function = spec.get("get_items")?;
    let mut store = lua
        .app_data_mut::<CompletionStore>()
        .ok_or_else(|| mlua::Error::runtime("completion store not available"))?;
    store.0.entry(plugin).or_default().insert(prefix, get_items);
    Ok(())
}

/// Register a submit-time expander for `prefix`. Called with `{ value = "..." }`
/// (the part after `prefix:`) for each `@prefix:value` token; returns
/// `(string, nil)` to splice that string in-place of the token, or
/// `(nil, err)` to flash `err` and abort the run. Unknown prefixes pass through
/// verbatim, so register an expander under every alias you accept (e.g. both
/// `"skill"` and `"s"`).
///
/// @param prefix string The prefix this expander owns (e.g. "skill" or "s").
/// @param f function `{ value = string } -> (string, string|nil)`.
/// @return
/// @example
/// maki.api.register_expander("skill", function(ref)
///   return "<skill:" .. ref.value .. ">", nil
/// end)
/// maki.api.register_expander("s", function(ref)
///   return "<skill:" .. ref.value .. ">", nil
/// end)
#[lua_fn]
fn register_expander(
    lua: &Lua,
    #[ctx] plugin: Arc<str>,
    prefix: String,
    f: Function,
) -> LuaResult<()> {
    let mut store = lua
        .app_data_mut::<ExpanderStore>()
        .ok_or_else(|| mlua::Error::runtime("expander store not available"))?;
    store.0.entry(plugin).or_default().insert(prefix, f);
    Ok(())
}

lua_table! {
    /// `@`-reference completion. Plugins register sources (popup candidates)
    /// and expanders (submit-time token replacement) keyed by prefix. The
    /// built-in `skill`, `task`, and `model` plugins provide the defaults; you
    /// can add your own kinds the same way.
    extend "maki.api" => pub(crate) fn add_completion_fns(plugin: Arc<str>), DOCS [
        register_completion_source(plugin), register_expander(plugin),
    ]
}

/// A parsed `@` reference token: the lowercase prefix (empty for file
/// references like `@src/main.rs`), the value, and its byte range in the
/// source text (including the leading `@`).
pub struct AtToken {
    pub prefix: String,
    pub value: String,
    pub range: std::ops::Range<usize>,
}

/// Scan `text` for `@`-tokens at token boundaries. A token is `@prefix:value`
/// running to the next whitespace; a token without a `:` is a file reference
/// (empty prefix). An empty value (e.g. `@skill:`) is not a reference yet and
/// is skipped.
pub fn parse_at_tokens(text: &str) -> Vec<AtToken> {
    let mut out = Vec::new();
    let mut i = 0;
    while i < text.len() {
        let c = text[i..].chars().next().unwrap();
        if c == '@' && at_is_token_start(text, i) {
            let start = i;
            let mut j = i + c.len_utf8();
            while j < text.len() {
                let cc = text[j..].chars().next().unwrap();
                if cc.is_whitespace() {
                    break;
                }
                j += cc.len_utf8();
            }
            let content = &text[start + 1..j];
            if let Some((prefix, value)) = content.split_once(':')
                && !value.is_empty()
            {
                out.push(AtToken {
                    prefix: prefix.to_ascii_lowercase(),
                    value: value.to_string(),
                    range: start..j,
                });
            } else if !content.contains(':') {
                out.push(AtToken {
                    prefix: String::new(),
                    value: content.to_string(),
                    range: start..j,
                });
            }
            i = j;
        } else {
            i += c.len_utf8();
        }
    }
    out
}

/// Look up the expander `Function` for `prefix` across all plugins.
/// Re-registration by the same plugin replaces in place; a prefix owned by
/// two different plugins resolves nondeterministically (map iteration order).
fn expander_for<'a>(store: &'a ExpanderStore, prefix: &str) -> Option<&'a Function> {
    store.0.values().flat_map(|m| m.get(prefix)).next()
}

/// Coerce a Lua `(value, err)` multi-return into the project pair shape.
fn pair_from_multivalue(mv: MultiValue) -> Pair<String> {
    let mut iter = mv.into_iter();
    let value = match iter.next() {
        Some(Value::String(s)) => Some(s.to_string_lossy()),
        Some(Value::Nil) | None => None,
        _ => None,
    };
    let err = match iter.next() {
        Some(Value::String(s)) => Some(s.to_string_lossy()),
        _ => None,
    };
    (value, err)
}

/// Rewrite `text` by dispatching each `@` reference token to the expander
/// registered for its prefix. Recognized tokens are replaced in place with the
/// returned string; unknown prefixes and file references (empty prefix) pass
/// through verbatim. The first expander `Err` aborts and is surfaced to the
/// user as a flash. Async because expanders may call yield-bridged FFI (e.g.
/// `maki.fs`); callers must be a Lua task scope.
pub(crate) async fn expand_references(lua: &Lua, text: &str) -> Result<String, String> {
    let tokens = parse_at_tokens(text);
    if tokens.is_empty() {
        return Ok(text.to_string());
    }
    let mut out = String::with_capacity(text.len());
    let mut last_end = 0;
    for tok in &tokens {
        out.push_str(&text[last_end..tok.range.start]);
        // Resolve (clone) the expander before the await: the app_data borrow
        // must not outlive the call, or the task-scope TaskHandle swap below
        // cannot mutably borrow the container.
        let f: Option<Function> = lua
            .app_data_ref::<ExpanderStore>()
            .as_ref()
            .and_then(|s| expander_for(s, &tok.prefix))
            .cloned();
        match f {
            Some(f) => {
                let ctx = lua
                    .create_table()
                    .map_err(|e| format!("expander ctx failed: {e}"))?;
                ctx.set("value", tok.value.as_str())
                    .map_err(|e| format!("expander ctx failed: {e}"))?;
                let mv: MultiValue = f.call_async(ctx).await.map_err(|e| {
                    format!("expander for '@{}:{}' failed: {e}", tok.prefix, tok.value)
                })?;
                match pair_from_multivalue(mv) {
                    (Some(replacement), None) => out.push_str(&replacement),
                    (_, Some(err)) => return Err(err),
                    (None, None) => {
                        return Err(format!(
                            "expander for '@{}:{}' returned no replacement string",
                            tok.prefix, tok.value
                        ));
                    }
                }
            }
            None => out.push_str(&text[tok.range.start..tok.range.end]),
        }
        last_end = tok.range.end;
    }
    out.push_str(&text[last_end..]);
    Ok(out)
}

/// Gather candidates from every registered source, passing `ctx` to each
/// `get_items`. A source that errors is skipped with a warning rather than
/// aborting the popup. Async because sources may call yield-bridged FFI
/// (e.g. `maki.fs`); callers must be a Lua task scope.
pub(crate) async fn collect_completion_items(lua: &Lua, ctx: &CompletionCtx) -> Vec<ItemSpec> {
    // Resolve (clone) all sources up front: the app_data borrow must not
    // outlive the awaits, or the task-scope TaskHandle swap cannot mutably
    // borrow the container.
    let fns: Vec<Function> = lua
        .app_data_ref::<CompletionStore>()
        .map(|store| store.0.values().flat_map(|m| m.values().cloned()).collect())
        .unwrap_or_default();
    if fns.is_empty() {
        return Vec::new();
    }
    let lua_ctx = match lua.create_table() {
        Ok(t) => t,
        Err(e) => {
            tracing::warn!(error = %e, "completion ctx build failed");
            return Vec::new();
        }
    };
    if let Err(e) = lua_ctx.set("mode", ctx.mode.as_str()) {
        tracing::warn!(error = %e, "completion ctx mode failed");
        return Vec::new();
    }
    match lua.create_sequence_from(ctx.models.iter().map(|s| s.as_str())) {
        Ok(models) => {
            if let Err(e) = lua_ctx.set("models", models) {
                tracing::warn!(error = %e, "completion ctx models failed");
                return Vec::new();
            }
        }
        Err(e) => {
            tracing::warn!(error = %e, "completion ctx models build failed");
            return Vec::new();
        }
    }
    let mut out = Vec::new();
    for f in fns {
        let result = match f.call_async::<Value>(lua_ctx.clone()).await {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(error = %e, "completion source get_items failed");
                continue;
            }
        };
        match lua_to_json(lua, &result) {
            Ok(json) => match serde_json::from_value::<Vec<ItemSpec>>(json) {
                Ok(items) => out.extend(items),
                Err(e) => tracing::warn!(error = %e, "completion source items malformed"),
            },
            Err(e) => tracing::warn!(error = %e, "completion source items convert failed"),
        }
    }
    out
}

fn command_argument_ctx(lua: &Lua, context: &CommandArgumentContext) -> LuaResult<Table> {
    let ctx = lua.create_table()?;
    ctx.set("command", context.command.as_ref())?;
    ctx.set("args", context.args.as_str())?;
    ctx.set("arg", context.arg.as_str())?;
    ctx.set("index", context.index)?;
    ctx.set("mode", context.mode.as_str())?;
    ctx.set("session", context.session)?;
    ctx.set("generation", context.generation)?;
    Ok(ctx)
}

pub(crate) async fn collect_command_argument_items(
    lua: &Lua,
    context: &CommandArgumentContext,
) -> Vec<CommandArgumentItem> {
    let value = lua.app_data_ref::<CommandHandlerMap>().and_then(|map| {
        map.get(&context.plugin).and_then(|commands| {
            commands.get(&context.command).and_then(|entry| {
                entry
                    .argument_completion
                    .as_ref()
                    .and_then(|key| lua.registry_value::<Value>(key).ok())
            })
        })
    });
    let Some(value) = value else {
        return Vec::new();
    };
    let result = match value {
        Value::Table(items) => Ok(items),
        Value::Function(function) => match command_argument_ctx(lua, context) {
            Ok(ctx) => function
                .call_async::<Value>(ctx)
                .await
                .and_then(|value| match value {
                    Value::Table(items) => Ok(items),
                    _ => Err(mlua::Error::runtime(
                        "argument completion must return a table",
                    )),
                }),
            Err(error) => Err(error),
        },
        _ => Err(mlua::Error::runtime(
            "argument completion must be a table or function",
        )),
    };
    let items = match result {
        Ok(items) => items,
        Err(error) => {
            tracing::warn!(plugin = %context.plugin, command = %context.command, error = %error, "command completion get_items failed");
            return Vec::new();
        }
    };
    let Ok(json) = lua_to_json(lua, &Value::Table(items)) else {
        return Vec::new();
    };
    let Ok(raw) = serde_json::from_value::<Vec<serde_json::Value>>(json) else {
        return Vec::new();
    };
    raw.into_iter()
        .filter_map(|item| {
            let label = item.get("label")?.as_str()?.to_owned();
            let insertion = item.get("insertion")?.as_str()?.to_owned();
            let description = item
                .get("description")
                .and_then(|value| value.as_str())
                .map(str::to_owned);
            Some(CommandArgumentItem {
                label,
                insertion,
                description,
            })
        })
        .collect()
}

fn lifecycle_function(
    lua: &Lua,
    commands: Option<&HashMap<Arc<str>, crate::api::util::command::CommandEntry>>,
    request: &CommandArgumentLifecycleRequest,
) -> Option<Function> {
    let entry = commands?.get(&request.context.command)?;
    let key = match request.event {
        CommandArgumentLifecycle::Highlight => &entry.completion_on_highlight,
        CommandArgumentLifecycle::Accept => &entry.completion_on_accept,
        CommandArgumentLifecycle::Cancel => &entry.completion_on_cancel,
    };
    key.as_ref()
        .and_then(|key| lua.registry_value::<Function>(key).ok())
}

pub(crate) async fn run_command_argument_lifecycle(
    lua: &Lua,
    request: &CommandArgumentLifecycleRequest,
) {
    let function = lua
        .app_data_ref::<CommandHandlerMap>()
        .and_then(|map| lifecycle_function(lua, map.get(&request.context.plugin), request))
        .or_else(|| {
            lua.app_data_ref::<crate::api::util::command::RetiredCommandHandlerMap>()
                .and_then(|retired| {
                    retired
                        .iter()
                        .rev()
                        .find(|(plugin, _)| plugin == &request.context.plugin)
                        .and_then(|(_, commands)| lifecycle_function(lua, Some(commands), request))
                })
        });
    let Some(function) = function else { return };
    let ctx = match command_argument_ctx(lua, &request.context) {
        Ok(ctx) => ctx,
        Err(error) => {
            tracing::warn!(plugin = %request.context.plugin, command = %request.context.command, hook = ?request.event, error = %error, "command completion hook context failed");
            return;
        }
    };
    let result = if let Some(item) = &request.item {
        let item = match serde_json::to_value(item)
            .map_err(mlua::Error::external)
            .and_then(|value| json_to_lua(lua, &value))
        {
            Ok(item) => item,
            Err(error) => {
                tracing::warn!(plugin = %request.context.plugin, command = %request.context.command, hook = ?request.event, error = %error, "command completion hook item failed");
                return;
            }
        };
        function.call_async::<()>((ctx, item)).await
    } else {
        function.call_async::<()>(ctx).await
    };
    if let Err(error) = result {
        tracing::warn!(plugin = %request.context.plugin, command = %request.context.command, hook = ?request.event, error = %error, "command completion hook failed");
    }
}

/// Drop everything `plugin` registered. Called from `clear_plugin` so an
/// unloaded plugin's sources and expanders go away with it.
pub(crate) fn clear_plugin(lua: &Lua, plugin: &str) {
    let plugin: Arc<str> = Arc::from(plugin);
    if let Some(mut store) = lua.app_data_mut::<CompletionStore>() {
        store.0.remove(&plugin);
    }
    if let Some(mut store) = lua.app_data_mut::<ExpanderStore>() {
        store.0.remove(&plugin);
    }
}

/// Ensure the stores exist before plugins load. Idempotent.
pub(crate) fn install(lua: &Lua) {
    install_store::<CompletionStore>(lua);
    install_store::<ExpanderStore>(lua);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::util::pair::Pair;

    fn lua_with_stores() -> Lua {
        let lua = Lua::new();
        install(&lua);
        lua
    }

    fn register_source(lua: &Lua, prefix: &str, items: &[(&str, &str, &str)]) {
        let items_lua = lua.create_table().unwrap();
        for (i, (label, kind, insertion)) in items.iter().enumerate() {
            let row = lua.create_table().unwrap();
            row.set("label", *label).unwrap();
            row.set("kind", *kind).unwrap();
            row.set("insertion", *insertion).unwrap();
            items_lua.set(i + 1, row).unwrap();
        }
        let get_items = lua
            .create_function(move |_lua, _ctx: mlua::Value| Ok(items_lua.clone()))
            .unwrap();
        lua.app_data_mut::<CompletionStore>()
            .unwrap()
            .0
            .entry(Arc::from("test"))
            .or_default()
            .insert(prefix.to_string(), get_items);
    }

    fn register_expander(
        lua: &Lua,
        prefix: &str,
        f: impl Fn(&str) -> Pair<String> + Send + 'static,
    ) {
        let f = lua
            .create_function(move |_lua, ctx: Table| {
                let value: String = ctx.get("value")?;
                Ok(f(&value))
            })
            .unwrap();
        lua.app_data_mut::<ExpanderStore>()
            .unwrap()
            .0
            .entry(Arc::from("test"))
            .or_default()
            .insert(prefix.to_string(), f);
    }

    #[test]
    fn at_is_token_start_basics() {
        assert!(at_is_token_start("@x", 0));
        assert!(at_is_token_start(" @x", 1));
        assert!(!at_is_token_start("a@x", 1));
    }

    #[test]
    fn parse_skips_unknown_and_mid_word_at() {
        let tokens = parse_at_tokens("foo@bar @skill:rev @nothing:x @skill:");
        assert_eq!(tokens.len(), 2);
        assert_eq!(tokens[0].prefix, "skill");
        assert_eq!(tokens[0].value, "rev");
        assert_eq!(tokens[1].prefix, "nothing");
        assert_eq!(tokens[1].value, "x");
    }

    #[test]
    fn parse_prefix_is_case_insensitive() {
        let tokens = parse_at_tokens("@SKILL:Rev");
        assert_eq!(tokens[0].prefix, "skill");
        assert_eq!(tokens[0].value, "Rev");
    }

    #[test]
    fn parse_file_reference_has_empty_prefix() {
        let tokens = parse_at_tokens("read @src/main.rs and @/abs/path ok");
        assert_eq!(tokens.len(), 2);
        assert_eq!(tokens[0].prefix, "");
        assert_eq!(tokens[0].value, "src/main.rs");
        assert_eq!(tokens[0].range, 5..17);
        assert_eq!(tokens[1].prefix, "");
        assert_eq!(tokens[1].value, "/abs/path");
    }

    #[test]
    fn expand_in_place_replacement() {
        let lua = lua_with_stores();
        register_expander(&lua, "subagent", |v| {
            (Some(format!("<subagent:{v}>")), None)
        });
        register_expander(&lua, "a", |v| (Some(format!("<subagent:{v}>")), None));
        assert_eq!(
            smol::block_on(expand_references(&lua, "@subagent:research do x")).unwrap(),
            "<subagent:research> do x"
        );
        assert_eq!(
            smol::block_on(expand_references(&lua, "@a:general run")).unwrap(),
            "<subagent:general> run"
        );
    }

    #[test]
    fn expand_unknown_prefix_passes_through() {
        let lua = lua_with_stores();
        register_expander(&lua, "skill", |v| (Some(format!("<skill:{v}>")), None));
        assert_eq!(
            smol::block_on(expand_references(&lua, "foo@bar @nothing:whatever fix it")).unwrap(),
            "foo@bar @nothing:whatever fix it"
        );
    }

    #[test]
    fn expand_file_reference_passes_through() {
        let lua = lua_with_stores();
        register_expander(&lua, "skill", |v| (Some(format!("<skill:{v}>")), None));
        assert_eq!(
            smol::block_on(expand_references(&lua, "@src/main.rs @skill:rev")).unwrap(),
            "@src/main.rs <skill:rev>"
        );
    }

    #[test]
    fn expand_err_propagates() {
        let lua = lua_with_stores();
        register_expander(&lua, "skill", |_| (None, Some("unknown skill".to_string())));
        assert_eq!(
            smol::block_on(expand_references(&lua, "@skill:nope do x")),
            Err("unknown skill".to_string())
        );
    }

    #[test]
    fn expand_no_tokens_returns_text_unchanged() {
        let lua = lua_with_stores();
        assert_eq!(
            smol::block_on(expand_references(&lua, "just a plain prompt")).unwrap(),
            "just a plain prompt"
        );
    }

    #[test]
    fn expand_model_only_no_special_case() {
        let lua = lua_with_stores();
        register_expander(&lua, "model", |v| (Some(format!("<model:{v}>")), None));
        register_expander(&lua, "m", |v| (Some(format!("<model:{v}>")), None));
        assert_eq!(
            smol::block_on(expand_references(&lua, "@model:zai/glm-5 fix it")).unwrap(),
            "<model:zai/glm-5> fix it"
        );
    }

    #[test]
    fn collect_gathers_from_sources() {
        let lua = lua_with_stores();
        register_source(&lua, "skill", &[("skill:rev", "skill", "@skill:rev")]);
        register_source(
            &lua,
            "subagent",
            &[("subagent:research", "subagent", "@subagent:research ")],
        );
        let items = smol::block_on(collect_completion_items(
            &lua,
            &CompletionCtx {
                mode: "build".into(),
                models: vec![],
            },
        ));
        let labels: Vec<&str> = items.iter().map(|i| i.label.as_str()).collect();
        assert!(labels.contains(&"skill:rev"));
        assert!(labels.contains(&"subagent:research"));
    }

    #[test]
    fn command_argument_completion_receives_raw_args_context() {
        let lua = lua_with_stores();
        let observed = Arc::new(std::sync::Mutex::new(None));
        let observed_ctx = Arc::clone(&observed);
        let completion = lua
            .create_function(move |lua, ctx: Table| {
                *observed_ctx.lock().unwrap() = Some((
                    ctx.get::<String>("command")?,
                    ctx.get::<String>("args")?,
                    ctx.get::<String>("arg")?,
                    ctx.get::<usize>("index")?,
                    ctx.get::<String>("mode")?,
                ));
                lua.create_table()
            })
            .unwrap();
        let key = lua.create_registry_value(completion).unwrap();
        lua.set_app_data(crate::api::util::command::CommandHandlerMap::from([(
            Arc::from("test"),
            HashMap::from([(
                Arc::from("/deploy"),
                crate::api::util::command::CommandEntry {
                    handler: lua
                        .create_registry_value(lua.create_function(|_, ()| Ok(())).unwrap())
                        .unwrap(),
                    description: Arc::from("deploy"),
                    argument_hint: None,
                    arguments: maki_commands::ArgumentArity::ONE,
                    tui_only: false,
                    argument_completion: Some(key),
                    completion_on_highlight: None,
                    completion_on_accept: None,
                    completion_on_cancel: None,
                },
            )]),
        )]));

        let items = smol::block_on(collect_command_argument_items(
            &lua,
            &CommandArgumentContext {
                command: Arc::from("/deploy"),
                plugin: Arc::from("test"),
                args: " staging ".into(),
                arg: "staging".into(),
                index: 0,
                mode: "build".into(),
                session: 7,
                generation: 9,
            },
        ));

        assert!(items.is_empty());
        assert_eq!(
            observed.lock().unwrap().as_ref(),
            Some(&(
                "/deploy".to_string(),
                " staging ".to_string(),
                "staging".to_string(),
                0,
                "build".to_string(),
            ))
        );
    }

    #[test]
    fn clear_plugin_drops_registrations() {
        let lua = lua_with_stores();
        register_source(&lua, "skill", &[("skill:rev", "skill", "@skill:rev")]);
        register_expander(&lua, "skill", |v| (Some(format!("<skill:{v}>")), None));
        assert!(
            !smol::block_on(collect_completion_items(&lua, &CompletionCtx::default())).is_empty()
        );
        clear_plugin(&lua, "test");
        assert!(
            smol::block_on(collect_completion_items(&lua, &CompletionCtx::default())).is_empty()
        );
        assert_eq!(
            smol::block_on(expand_references(&lua, "@skill:rev x")).unwrap(),
            "@skill:rev x"
        );
    }
}
