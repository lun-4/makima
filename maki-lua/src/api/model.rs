//! `maki.model`. Context-free calls address the focused session and need a
//! UI; explicit tool contexts read their own admitted model anywhere.

use maki_lua_macro::{lua_fn, lua_table};
use maki_providers::{
    CompleteModelSelection, MODEL_THINKING_KEY, Model, RequestOptions, ThinkingConfig,
};
use mlua::{Error as LuaError, Lua, LuaSerdeExt, Result as LuaResult, Table, UserDataRef, Value};

use crate::agent_errors::{
    DELEGATED_FOCUS, EXPLICIT_MODEL_TARGET_REQUIRED, TRUSTED_MODEL_MUTATION_REQUIRED,
};
use crate::api::util::command::{ModelRequest, UiAction, ui_json_roundtrip};
use crate::api::util::ctx::LuaCtx;
use crate::api::util::pair::{Pair, err_pair, try_pair};
use crate::api::util::selection::selection_from_lua;

const SET_ARG_ERR: &str = "expected a model spec string or an options table";

async fn roundtrip(
    lua: Lua,
    tx: Option<flume::Sender<UiAction>>,
    req: ModelRequest,
) -> LuaResult<Pair<Value>> {
    ui_json_roundtrip(&lua, tx.as_ref(), |reply_tx| UiAction::Model {
        req,
        reply_tx,
    })
    .await
}

/// Reads the model for an explicit tool context, or the focused session when
/// a UI is attached. Tool handlers must pass their `ctx`, because they cannot
/// read UI focus. `thinking` uses the spelling `set` accepts.
///
/// @param ctx userdata|nil Optional tool handler context.
/// @return (table|nil, string|nil) `{spec, thinking, fast}`, or nil and an error.
/// @example
/// local m = maki.model.get(ctx)
/// if m.spec ~= "anthropic/claude-opus-4-6" then ... end
#[lua_fn]
async fn get(
    lua: Lua,
    #[ctx] tx: Option<flume::Sender<UiAction>>,
    ctx: Option<UserDataRef<LuaCtx>>,
) -> LuaResult<Pair<Value>> {
    if let Some(ctx) = ctx {
        if let Err(error) = ctx.validate_origin(&lua) {
            return Ok(err_pair(error));
        }
        let Some(agent) = ctx.agent() else {
            return Ok(err_pair(ctx.cap_err("maki.model.get")));
        };
        return model_value(&lua, &agent.model, &agent.opts);
    }
    if tx.is_none() {
        return Ok(err_pair(EXPLICIT_MODEL_TARGET_REQUIRED));
    }
    if crate::runtime::in_delegated_tool(&lua) {
        return Ok(err_pair(DELEGATED_FOCUS));
    }
    roundtrip(lua, tx, ModelRequest::Get).await
}

fn model_value(lua: &Lua, model: &Model, opts: &RequestOptions) -> LuaResult<Pair<Value>> {
    let selection = CompleteModelSelection::from_model(model, *opts);
    Ok((Some(lua.to_value(&selection)?), None))
}

/// Lists the model specs you can switch to: what the providers you are logged
/// into offer, minus what your model policy blocks. The list fills in the
/// background at startup, so right after launch it can still be empty.
///
/// @return (table|nil, string|nil) Array of `"provider/id"` specs, or nil and an error.
/// @example
/// local specs = maki.model.available()
#[lua_fn]
async fn available(lua: Lua, #[ctx] tx: Option<flume::Sender<UiAction>>) -> LuaResult<Pair<Value>> {
    if tx.is_none() {
        return Ok(err_pair(EXPLICIT_MODEL_TARGET_REQUIRED));
    }
    roundtrip(lua, tx, ModelRequest::Available).await
}

/// Switches the focused session's model, thinking level, or fast mode. Fields
/// you leave out stay as they are, so this doubles as a thinking-only switch.
/// Answers with the committed state, in the same shape `get` returns. Changes
/// apply to subsequent admissions. UI actors save asynchronously after commit;
/// SDK and ACP roots retain checkpoint-before-commit rollback semantics. Fixed
/// print roots reject all patches. Delegated tool handlers cannot call it,
/// because it addresses no explicit agent. Prefer `Agent:set_model(ctx, patch)`
/// for an explicit target.
///
/// @param opts string|table A model spec, or a table with any of:
///   `spec` (string) `"provider/id"`, as listed by `available()`;
///   `thinking` (string|number) `"off"`, `"adaptive"`, an effort level
///   (`"minimal"` to `"max"`), a token budget, or `""` to toggle it on and off;
///   `fast` (boolean) Anthropic fast mode.
/// @return (table|nil, string|nil) The new state, or nil and an error.
/// @example
/// maki.model.set("anthropic/claude-opus-4-6")
/// maki.model.set({ spec = "zai/glm-5", thinking = "high" })
/// maki.model.set({ thinking = "" })
#[lua_fn]
async fn set(
    lua: Lua,
    #[ctx] tx: Option<flume::Sender<UiAction>>,
    opts: Value,
) -> LuaResult<Pair<Value>> {
    if crate::runtime::in_delegated_tool(&lua) {
        return Ok(err_pair(TRUSTED_MODEL_MUTATION_REQUIRED));
    }
    try_pair!(crate::runtime::validate_plugin_effect(&lua));
    let (opts, toggle_thinking) = match opts {
        Value::Table(table) => {
            // `thinking = ""` is the documented toggle, not a selection value.
            let toggle = matches!(table.get::<Value>(MODEL_THINKING_KEY)?, Value::String(value) if value.to_str()?.is_empty());
            if toggle {
                let rest = lua.create_table()?;
                for pair in table.pairs::<Value, Value>() {
                    let (key, value) = pair?;
                    if !matches!(&key, Value::String(name) if name.to_str()?.as_ref() == MODEL_THINKING_KEY)
                    {
                        rest.set(key, value)?;
                    }
                }
                (Value::Table(rest), true)
            } else {
                (Value::Table(table), false)
            }
        }
        Value::String(spec) => (Value::String(spec), false),
        other => {
            return Err(LuaError::runtime(format!(
                "{SET_ARG_ERR}, got {}",
                other.type_name()
            )));
        }
    };
    let selection = match selection_from_lua(&lua, opts) {
        Ok(selection) => selection,
        Err(error) => return Ok(err_pair(error)),
    };
    if tx.is_none() {
        return Ok(err_pair(EXPLICIT_MODEL_TARGET_REQUIRED));
    }
    let req = ModelRequest::Set {
        spec: selection.spec.map(|spec| spec.to_string()),
        thinking: selection.thinking,
        toggle_thinking,
        fast: selection.fast,
    };
    roundtrip(lua, tx, req).await
}

/// Describes a model by spec without addressing any agent. Unknown specs
/// return an error.
///
/// @param spec string Model spec, e.g. `"anthropic/claude-sonnet-4-20250514"`.
/// @return (table|nil, string|nil) `{spec, provider, id, tier, supports_thinking,
///   supports_fast, context_window, max_output_tokens, pricing}`, or nil and an
///   error. `max_output_tokens` is nil when unknown. `pricing` is
///   `{input, output, cache_write, cache_read}` in USD per million tokens.
/// @example
/// local info = assert(maki.model.info("anthropic/claude-sonnet-4-20250514"))
#[lua_fn]
fn info(lua: &Lua, spec: String) -> LuaResult<Pair<Table>> {
    let model = match Model::from_spec(&spec) {
        Ok(model) => model,
        Err(error) => return Ok(err_pair(error.to_string())),
    };
    let table = lua.create_table()?;
    table.set("spec", model.spec())?;
    table.set("provider", model.provider.to_string())?;
    table.set("id", model.id.clone())?;
    table.set("tier", model.tier.to_string())?;
    table.set("supports_thinking", model.supports_thinking())?;
    table.set("supports_fast", model.supports_fast())?;
    table.set("context_window", model.context_window)?;
    table.set("max_output_tokens", model.max_output_tokens)?;
    let pricing = lua.create_table()?;
    pricing.set("input", model.pricing.input)?;
    pricing.set("output", model.pricing.output)?;
    pricing.set("cache_write", model.pricing.cache_write)?;
    pricing.set("cache_read", model.pricing.cache_read)?;
    table.set("pricing", pricing)?;
    Ok((Some(table), None))
}

/// Lists the thinking levels a selection accepts, from `off` to `max`. A
/// token budget is also accepted.
///
/// @return table Array of thinking level names.
/// @example
/// local levels = maki.model.thinking_levels()
#[lua_fn]
fn thinking_levels(lua: &Lua) -> LuaResult<Table> {
    lua.create_sequence_from(ThinkingConfig::options().iter().copied())
}

lua_table! {
    /// Compatibility model access. With a ctx, `get` reads that invocation's
    /// admitted model. Without a ctx, calls address the focused session and
    /// need an interactive UI; other hosts use the Agent API with an explicit
    /// ctx instead.
    "maki.model" => pub(crate) fn create_model_table(tx: Option<flume::Sender<UiAction>>),
    DOCS [get(tx), available(tx), set(tx), info, thinking_levels]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::util::command::{NO_UI_ERR, UI_DROPPED_ERR, UiReply};
    use crate::api::util::convert::lua_to_json;
    use maki_agent::{AgentMode, tools::test_support::stub_ctx};
    use maki_providers::ThinkingConfig;
    use serde_json::{Value as Json, json};
    use test_case::test_case;

    const SPEC: &str = "anthropic/claude-opus-4-6";
    const THINKING: &str = "high";
    const UI_FAILURE: &str = "Model is not allowed by policy: anthropic/claude-opus-4-6";

    fn lua_with_model(tx: Option<flume::Sender<UiAction>>) -> Lua {
        let lua = Lua::new();
        let t = create_model_table(&lua, tx).unwrap();
        lua.globals().set("model", t).unwrap();
        lua
    }

    /// The receiver is dropped up front, so the request cannot even leave.
    fn closed_ui() -> Lua {
        let (tx, rx) = flume::unbounded::<UiAction>();
        drop(rx);
        lua_with_model(Some(tx))
    }

    /// Answering `None` drops the reply channel, like a UI that took the
    /// request and then vanished.
    fn stub_ui(answer: fn(ModelRequest) -> Option<UiReply>) -> Lua {
        let (tx, rx) = flume::unbounded::<UiAction>();
        std::thread::spawn(move || {
            while let Ok(UiAction::Model { req, reply_tx }) = rx.recv() {
                if let Some(reply) = answer(req) {
                    let _ = reply_tx.send(reply);
                }
            }
        });
        lua_with_model(Some(tx))
    }

    /// Echoes the request back, so a test can assert on what the UI would have
    /// acted on. Fields left out stay `nil` on the way back.
    fn echo(req: ModelRequest) -> Option<UiReply> {
        Some(Ok(match req {
            ModelRequest::Get => json!({ "spec": SPEC, "thinking": THINKING, "fast": true }),
            ModelRequest::Available => json!([SPEC]),
            ModelRequest::Set {
                spec,
                thinking,
                toggle_thinking,
                fast,
            } => json!({
                "spec": spec,
                "thinking": if toggle_thinking {
                    Some(String::new())
                } else {
                    thinking.map(|thinking| thinking.to_string())
                },
                "fast": fast,
            }),
        }))
    }

    /// The value comes back as JSON so assertions outlive the Lua state.
    fn eval(lua: &Lua, script: &str) -> (Json, Option<String>) {
        let (val, err): (Value, Option<String>) =
            smol::block_on(lua.load(script).eval_async()).unwrap();
        (lua_to_json(lua, &val).unwrap(), err)
    }

    /// Omitted fields stay unchanged; the legacy empty-string toggle is preserved.
    #[test_case("return model.get()", json!({ "spec": SPEC, "thinking": THINKING, "fast": true }) ; "get")]
    #[test_case("return model.available()", json!([SPEC]) ; "available")]
    #[test_case("return model.set('anthropic/claude-opus-4-6')", json!({ "spec": SPEC }) ; "set_bare_spec_string")]
    #[test_case("return model.set({ thinking = 8192, fast = true })", json!({ "thinking": "8192", "fast": true }) ; "set_table_without_spec")]
    #[test_case("return model.set({ thinking = '', fast = false })", json!({ "thinking": "", "fast": false }) ; "set_empty_thinking_and_false_fast")]
    #[test_case("local m = model.get() return model.set(m)", json!({ "spec": SPEC, "thinking": THINKING, "fast": true }) ; "set_fed_by_get")]
    fn requests_cross_the_channel_and_answer_with_the_new_state(script: &str, expected: Json) {
        assert_eq!(eval(&stub_ui(echo), script), (expected, None));
    }

    #[test]
    fn delegated_tool_cannot_use_context_free_setter() {
        let (tx, rx) = flume::unbounded::<UiAction>();
        let lua = lua_with_model(Some(tx));
        let scope = crate::runtime::TaskScope::detached(&lua);
        crate::runtime::lock_cell(scope.handle()).delegated_tool = Some(Default::default());
        assert_eq!(
            eval(&lua, "return model.set({ thinking = 'low' })"),
            (Json::Null, Some(TRUSTED_MODEL_MUTATION_REQUIRED.into()))
        );
        assert!(rx.is_empty());
    }

    #[test_case("return model.set({ unknown = true })" ; "unknown_key")]
    #[test_case("return model.set({ thinking = 'not-a-setting' })" ; "invalid_thinking")]
    #[test_case("return model.set({ fast = 'true' })" ; "non_boolean_fast")]
    fn invalid_selection_is_rejected_before_ui_request(script: &str) {
        let (value, error) = eval(&lua_with_model(None), script);
        assert_eq!(value, Json::Null);
        assert!(error.is_some_and(|error| error != NO_UI_ERR));
    }

    #[test]
    fn test_lua_model_get_without_ui() {
        let lua = lua_with_model(None);
        let mut ctx = stub_ctx(&AgentMode::Build);
        ctx.opts.thinking = ThinkingConfig::Off;
        ctx.opts.fast = false;
        let model = &ctx.model;
        let expected = json!({
            "spec": model.spec(), "thinking": "off", "fast": false,
        });
        lua.globals()
            .set("ctx", lua.create_userdata(LuaCtx::handler(&ctx)).unwrap())
            .unwrap();
        let (value, error) = model_value(&lua, &ctx.model, &ctx.opts).unwrap();
        assert_eq!(error, None);
        assert_eq!(lua_to_json(&lua, &value.unwrap()).unwrap(), expected);
        assert_eq!(
            eval(&lua, "return model.get(ctx)"),
            (
                Json::Null,
                Some("context is not bound to an invocation".into())
            )
        );
        for script in [
            "return model.get()",
            "return model.available()",
            "return model.set({ fast = false })",
        ] {
            assert_eq!(
                eval(&lua, script),
                (Json::Null, Some(EXPLICIT_MODEL_TARGET_REQUIRED.to_owned()))
            );
        }
    }

    /// Every way of not getting an answer lands in the error slot, instead of
    /// throwing or parking forever.
    #[test_case(lua_with_model(None), EXPLICIT_MODEL_TARGET_REQUIRED ; "no_ui_attached")]
    #[test_case(closed_ui(), NO_UI_ERR ; "event_loop_closed")]
    #[test_case(stub_ui(|_| None), UI_DROPPED_ERR ; "reply_channel_dropped")]
    #[test_case(stub_ui(|_| Some(Err(UI_FAILURE.to_owned()))), UI_FAILURE ; "ui_refused")]
    fn unanswered_request_returns_an_error_pair(lua: Lua, expected: &str) {
        assert_eq!(
            eval(&lua, "return model.get()"),
            (Json::Null, Some(expected.to_owned()))
        );
    }

    /// A non-spec argument is a programmer error, so it throws instead of
    /// answering with a pair.
    #[test_case("return model.set(42)" ; "number")]
    #[test_case("return model.set()" ; "no_argument")]
    #[test_case("return model.set(nil)" ; "explicit_nil")]
    fn set_throws_on_a_non_spec_argument(script: &str) {
        let lua = lua_with_model(None);
        let err = smol::block_on(lua.load(script).eval_async::<Value>()).unwrap_err();
        assert!(err.to_string().contains(SET_ARG_ERR));
    }

    #[test]
    fn info_describes_the_model_registry_entry() {
        let model = Model::from_spec(SPEC).unwrap();
        let (info, err) = eval(
            &lua_with_model(None),
            &format!("return model.info({SPEC:?})"),
        );
        assert_eq!(err, None);
        assert_eq!(info["supports_fast"], model.supports_fast());
        assert_eq!(info["context_window"], model.context_window);
        assert_eq!(info["pricing"]["input"], model.pricing.input);
        assert_eq!(info["pricing"]["cache_read"], model.pricing.cache_read);
    }
}
