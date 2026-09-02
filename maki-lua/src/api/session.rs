//! `maki.session`: host session primitives. Session management round-trips to
//! the UI event loop, which owns live runtimes and storage. `notify` posts
//! directly to the agent mailbox so synchronous callbacks can use it.

use std::sync::Arc;

use maki_agent::SessionMailbox;
use maki_lua_macro::{lua_fn, lua_table};
use maki_storage::id::MakiId;
use mlua::{Lua, Result as LuaResult, Table, Value};

use crate::api::util::command::{SessionRequest, UiAction, ui_json_roundtrip};
use crate::api::util::convert::{json_to_lua, lua_to_json};
use crate::api::util::pair::{Pair, err_pair};

const BLANK_NOTIFY_ERR: &str = "text must not be blank";
const SESSION_REQUIRED_ERR: &str = "session is required";

async fn roundtrip(
    lua: Lua,
    tx: Option<flume::Sender<UiAction>>,
    req: SessionRequest,
) -> LuaResult<Pair<Value>> {
    ui_json_roundtrip(&lua, tx.as_ref(), |reply_tx| UiAction::Session {
        req,
        reply_tx,
    })
    .await
}

/// Lists sessions stored for the current project. Answered from a
/// background scan, so a slow disk never blocks the UI. `open_elsewhere` is
/// true while another makima instance has the session open.
///
/// @return (table|nil, string|nil) Array of `{id, title, updated_at, cwd, open_elsewhere}`, or nil and an error.
/// @example
/// local stored, err = maki.session.list()
#[lua_fn]
async fn list(lua: Lua, #[ctx] tx: Option<flume::Sender<UiAction>>) -> LuaResult<Pair<Value>> {
    roundtrip(lua, tx, SessionRequest::List).await
}

/// Lists stored sessions across every project directory, most recently
/// updated first. Answered from a background scan, so a slow disk never
/// blocks the UI. `open_elsewhere` is true while another makima instance has
/// the session open.
///
/// @return (table|nil, string|nil) Array of `{id, title, updated_at, cwd, open_elsewhere}`, or nil and an error.
/// @example
/// local stored, err = maki.session.list_all()
#[lua_fn]
async fn list_all(lua: Lua, #[ctx] tx: Option<flume::Sender<UiAction>>) -> LuaResult<Pair<Value>> {
    roundtrip(lua, tx, SessionRequest::ListAll).await
}

/// Lists the sessions currently running in this UI. Status is "working",
/// "needs_input", or "idle". A mailbox follow-up stays "working" without an
/// intermediate "idle" status.
///
/// @return (table|nil, string|nil) Array of `{id, title, status, updated_at, focused}`, or nil and an error.
/// @example
/// local live, err = maki.session.live()
#[lua_fn]
async fn live(lua: Lua, #[ctx] tx: Option<flume::Sender<UiAction>>) -> LuaResult<Pair<Value>> {
    roundtrip(lua, tx, SessionRequest::Live).await
}

/// Returns the id of the currently focused session.
///
/// @return (string|nil, string|nil) Session id, or nil and an error.
/// @example
/// local id = maki.session.current()
#[lua_fn]
async fn current(lua: Lua, #[ctx] tx: Option<flume::Sender<UiAction>>) -> LuaResult<Pair<Value>> {
    roundtrip(lua, tx, SessionRequest::Current).await
}

/// Returns the focused session's token usage without making a network request.
/// Costs are the values recorded when each turn was billed and remain nil when
/// no turn for that total or model was priced. Models are sorted by total tokens
/// descending, then model spec ascending.
///
/// @return (table|nil, string|nil) `{total={input, output, cache_creation,
///   cache_read, cost}, models}`, where each model has `{model, input, output,
///   cache_creation, cache_read, cost}`, or nil and an error.
/// @example
/// local usage, err = maki.session.usage()
#[lua_fn]
async fn usage(lua: Lua, #[ctx] tx: Option<flume::Sender<UiAction>>) -> LuaResult<Pair<Value>> {
    roundtrip(lua, tx, SessionRequest::Usage).await
}

/// Switches the UI to the session with {id}. The session must belong to
/// the current directory and must not be open in another terminal.
///
/// @param id string Session id, as returned by `list()` or `live()`.
/// @return (boolean|nil, string|nil) true on success, or nil and an error.
/// @example
/// local _, err = maki.session.focus(id)
#[lua_fn]
async fn focus(
    lua: Lua,
    #[ctx] tx: Option<flume::Sender<UiAction>>,
    id: String,
) -> LuaResult<Pair<Value>> {
    roundtrip(lua, tx, SessionRequest::Focus { id }).await
}

/// Changes the active mode of a live session without focusing it.
///
/// @param id string Live session id.
/// @param mode string Mode id ("build", "plan", or a custom name).
/// @return (boolean|nil, string|nil) true on success, or nil and an error.
/// @example
/// local _, err = maki.session.set_mode(id, "build")
#[lua_fn]
async fn set_mode(
    lua: Lua,
    #[ctx] tx: Option<flume::Sender<UiAction>>,
    id: String,
    mode: String,
) -> LuaResult<Pair<Value>> {
    let mode = match crate::api::mode::resolve_mode(&lua, mode)? {
        Ok(mode) => mode,
        Err(error) => return Ok(err_pair(error)),
    };
    roundtrip(lua, tx, SessionRequest::SetMode { id, mode }).await
}

/// Deletes a session and its stored history, cancelling it first if it
/// is running. The focused session cannot be deleted.
///
/// @param id string Session id to delete.
/// @return (boolean|nil, string|nil) true on success, or nil and an error.
/// @example
/// local _, err = maki.session.delete(id)
#[lua_fn]
async fn delete(
    lua: Lua,
    #[ctx] tx: Option<flume::Sender<UiAction>>,
    id: String,
) -> LuaResult<Pair<Value>> {
    roundtrip(lua, tx, SessionRequest::Delete { id }).await
}

/// Starts a new session in the current project.
///
/// @param opts table? Optional fields: prompt (string) first user message
///   to submit right away; focus (boolean) switch the UI to the new session.
/// @return (string|nil, string|nil) New session id, or nil and an error.
/// @example
/// local id, err = maki.session.new({ prompt = "fix the tests", focus = true })
#[lua_fn]
async fn new(
    lua: Lua,
    #[ctx] tx: Option<flume::Sender<UiAction>>,
    opts: Option<Table>,
) -> LuaResult<Pair<Value>> {
    let (prompt, focus) = match opts {
        Some(opts) => (opts.get("prompt")?, opts.get("focus").unwrap_or(false)),
        None => (None, false),
    };
    roundtrip(lua, tx, SessionRequest::New { prompt, focus }).await
}

/// Sends {text} as a regular user prompt to a live session. The text is
/// never interpreted: slash commands, `exit`, and `!` shell prefixes are
/// all sent to the model verbatim. If the session is currently streaming,
/// the prompt is queued and picked up when the agent reaches it.
///
/// @param text string The prompt to send. Must not be blank.
/// @param opts table? Optional fields: session (string) id of a live
///   session; defaults to the focused one.
/// @return (string|nil, string|nil) "started" or "queued", or nil and an error.
/// @example
/// local state, err = maki.session.prompt("run the tests", { session = id })
#[lua_fn]
async fn prompt(
    lua: Lua,
    #[ctx] tx: Option<flume::Sender<UiAction>>,
    text: String,
    opts: Option<Table>,
) -> LuaResult<Pair<Value>> {
    let id = match opts {
        Some(opts) => opts.get("session")?,
        None => None,
    };
    roundtrip(lua, tx, SessionRequest::Prompt { id, text }).await
}

/// Reports {text} to a live session without creating a user turn. The
/// observation waits for the session's next agent run.
///
/// @param text string What to report. Must not be blank.
/// @param opts table Options:
///   `session` (string) id of a live session.
///   `wake` (boolean) start a TUI turn when it next becomes idle (default false).
/// @return (boolean|nil, string|nil) true, or nil and an error.
/// @example
/// maki.session.notify("[monitor] deploy failed", { session = id, wake = true })
#[lua_fn]
fn notify(_lua: &Lua, text: String, opts: Option<Table>) -> LuaResult<Pair<bool>> {
    if text.trim().is_empty() {
        return Ok(err_pair(BLANK_NOTIFY_ERR));
    }
    let Some(opts) = opts else {
        return Ok(err_pair(SESSION_REQUIRED_ERR));
    };
    let Some(raw_id) = opts.get::<Option<String>>("session")? else {
        return Ok(err_pair(SESSION_REQUIRED_ERR));
    };
    let session_id: MakiId = match raw_id.parse() {
        Ok(id) => id,
        Err(error) => return Ok(err_pair(error)),
    };
    let wake = opts.get("wake").unwrap_or(false);
    if let Err(error) = SessionMailbox::notify(session_id, text, wake) {
        return Ok(err_pair(error));
    }
    Ok((Some(true), None))
}

/// Returns this plugin's persisted JSON-compatible data for a live session.
/// Plugins cannot read another plugin's data.
///
/// @param session string Live session id.
/// @return (any|nil, string|nil) Stored value, nil if unset, or nil and an error.
/// @example
/// local state, err = maki.session.get_data(session_id)
#[lua_fn]
fn get_data(lua: &Lua, #[ctx] plugin: Arc<str>, session: String) -> LuaResult<Pair<Value>> {
    let session_id: MakiId = match session.parse() {
        Ok(id) => id,
        Err(error) => return Ok(err_pair(error)),
    };
    match SessionMailbox::plugin_data(session_id, &plugin) {
        Ok(Some(value)) => Ok((Some(json_to_lua(lua, &value)?), None)),
        Ok(None) => Ok((Some(Value::Nil), None)),
        Err(error) => Ok(err_pair(error)),
    }
}

/// Replaces this plugin's persisted data for a live session. Values must be
/// JSON-compatible. Passing nil clears the data.
///
/// @param session string Live session id.
/// @param value any JSON-compatible value, or nil to clear.
/// @return (boolean|nil, string|nil) true, or nil and an error.
/// @example
/// maki.session.set_data(session_id, { run = 3 })
#[lua_fn]
fn set_data(
    lua: &Lua,
    #[ctx] plugin: Arc<str>,
    session: String,
    value: Value,
) -> LuaResult<Pair<bool>> {
    let session_id: MakiId = match session.parse() {
        Ok(id) => id,
        Err(error) => return Ok(err_pair(error)),
    };
    let value = if value.is_nil() {
        None
    } else {
        Some(lua_to_json(lua, &value)?)
    };
    match SessionMailbox::set_plugin_data(session_id, plugin.to_string(), value) {
        Ok(()) => Ok((Some(true), None)),
        Err(error) => Ok(err_pair(error)),
    }
}

/// Renames a session, live or stored.
///
/// @param opts table Required fields: id (string) session to rename;
///   title (string) the new title.
/// @return (boolean|nil, string|nil) true on success, or nil and an error.
/// @example
/// local _, err = maki.session.set_title({ id = id, title = "refactor" })
#[lua_fn]
async fn set_title(
    lua: Lua,
    #[ctx] tx: Option<flume::Sender<UiAction>>,
    opts: Table,
) -> LuaResult<Pair<Value>> {
    let req = SessionRequest::SetTitle {
        id: opts.get("id")?,
        title: opts.get("title")?,
    };
    roundtrip(lua, tx, req).await
}

/// Returns the current thinking mode of the focused session and whether its
/// model supports thinking at all (for hiding/graving the selector).
///
/// @return (table|nil, string|nil) `{mode, supports_thinking}`, or nil and an error.
/// @example
/// local info = maki.session.thinking()
#[lua_fn]
async fn thinking(lua: Lua, #[ctx] tx: Option<flume::Sender<UiAction>>) -> LuaResult<Pair<Value>> {
    roundtrip(lua, tx, SessionRequest::GetThinking).await
}

/// Sets the focused session's thinking mode. `mode` accepts any value
/// `StoredThinking::parse_setting` understands: `off`, `adaptive`, an effort
/// level (`minimal` .. `max`), or a token budget. When `set_default` is true,
/// the choice is also persisted as the global default for new sessions.
///
/// @param opts table Required fields: mode (string) the thinking setting;
///   set_default (boolean) also persist as the default for new sessions.
/// @return (table|nil, string|nil) `{mode}`, or nil and an error.
/// @example
/// maki.session.set_thinking({ mode = "medium", set_default = true })
#[lua_fn]
async fn set_thinking(
    lua: Lua,
    #[ctx] tx: Option<flume::Sender<UiAction>>,
    opts: Table,
) -> LuaResult<Pair<Value>> {
    let req = SessionRequest::SetThinking {
        set_default: opts.get("set_default").unwrap_or(false),
        thinking: opts.get("mode")?,
    };
    roundtrip(lua, tx, req).await
}

lua_table! {
    /// Host session primitives. The interactive UI can run several sessions
    /// at once; these functions let plugins list, create, focus, configure,
    /// rename, and delete them. Session management returns `nil, "no interactive UI
    /// attached"` without a UI. `notify` instead targets a live agent mailbox
    /// directly, so it also works under ACP and SDK frontends.
    "maki.session" => pub(crate) fn create_session_table(plugin: Arc<str>, tx: Option<flume::Sender<UiAction>>),
    DOCS [list(tx), list_all(tx), live(tx), current(tx), usage(tx), focus(tx), set_mode(tx), delete(tx), new(tx), prompt(tx), notify(), get_data(plugin), set_data(plugin), set_title(tx), thinking(tx), set_thinking(tx)]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::util::command::NO_UI_ERR;
    use mlua::Value;
    use serde_json::json;
    use test_case::test_case;

    fn lua_with_session(tx: Option<flume::Sender<UiAction>>) -> Lua {
        let lua = Lua::new();
        lua.set_app_data(Arc::new(maki_agent::ModeRegistry::builtin()));
        let t = create_session_table(&lua, Arc::from("test"), tx).unwrap();
        lua.globals().set("session", t).unwrap();
        lua
    }

    #[test]
    fn live_without_ui_returns_error_pair() {
        let lua = lua_with_session(None);
        let (val, err): (Value, Option<String>) =
            smol::block_on(lua.load("return session.live()").eval_async()).unwrap();
        assert!(val.is_nil());
        assert_eq!(err.as_deref(), Some(NO_UI_ERR));
    }

    #[test]
    fn plugin_data_roundtrips_with_ownership() {
        let id = MakiId::generate();
        let _mailbox = SessionMailbox::register(id);
        let lua = lua_with_session(None);
        lua.globals().set("sid", id.to_string()).unwrap();

        let (saved, err): (bool, Option<String>) = lua
            .load("return session.set_data(sid, { run = 3 })")
            .eval()
            .unwrap();
        assert!(saved);
        assert_eq!(err, None);
        let (state, err): (Table, Option<String>) =
            lua.load("return session.get_data(sid)").eval().unwrap();
        assert_eq!(state.get::<u32>("run").unwrap(), 3);
        assert_eq!(err, None);

        let other = create_session_table(&lua, Arc::from("other"), None).unwrap();
        lua.globals().set("other", other).unwrap();
        let (value, err): (Value, Option<String>) =
            lua.load("return other.get_data(sid)").eval().unwrap();
        assert!(value.is_nil());
        assert_eq!(err, None);
    }

    #[test]
    fn usage_roundtrips_through_ui_channel() {
        let (tx, rx) = flume::unbounded::<UiAction>();
        let lua = lua_with_session(Some(tx));
        let checker = std::thread::spawn(move || {
            let Ok(UiAction::Session {
                req: SessionRequest::Usage,
                reply_tx,
            }) = rx.recv()
            else {
                panic!("expected usage request");
            };
            reply_tx
                .send(Ok(json!({
                    "total": {
                        "input": 1,
                        "output": 2,
                        "cache_creation": 3,
                        "cache_read": 4,
                        "cost": null,
                    },
                    "models": [],
                })))
                .unwrap();
        });
        let (val, err): (Table, Option<String>) =
            smol::block_on(lua.load("return session.usage()").eval_async()).unwrap();
        checker.join().unwrap();
        assert_eq!(err, None);
        let total: Table = val.get("total").unwrap();
        assert_eq!(total.get::<u32>("input").unwrap(), 1);
        assert!(total.get::<Value>("cost").unwrap().is_nil());
        assert_eq!(val.get::<Table>("models").unwrap().raw_len(), 0);
    }

    #[test]
    fn focus_roundtrips_through_ui_channel() {
        let (tx, rx) = flume::unbounded::<UiAction>();
        let lua = lua_with_session(Some(tx));
        std::thread::spawn(move || {
            let Ok(UiAction::Session {
                req: SessionRequest::Focus { id },
                reply_tx,
            }) = rx.recv()
            else {
                panic!("expected focus request");
            };
            reply_tx.send(Ok(json!({ "focused": id }))).unwrap();
        });
        let (val, err): (Table, Option<String>) =
            smol::block_on(lua.load("return session.focus('abc')").eval_async()).unwrap();
        assert_eq!(err, None);
        assert_eq!(val.get::<String>("focused").unwrap(), "abc");
    }

    #[test]
    fn set_mode_targets_the_requested_session() {
        let (tx, rx) = flume::unbounded::<UiAction>();
        let lua = lua_with_session(Some(tx));
        let checker = std::thread::spawn(move || {
            let Ok(UiAction::Session {
                req: SessionRequest::SetMode { id, mode },
                reply_tx,
            }) = rx.recv()
            else {
                panic!("expected set mode request");
            };
            assert_eq!(id, "background");
            assert_eq!(mode, "build");
            reply_tx.send(Ok(json!(true))).unwrap();
        });
        let (value, error): (bool, Option<String>) = smol::block_on(
            lua.load("return session.set_mode('background', 'build')")
                .eval_async(),
        )
        .unwrap();
        checker.join().unwrap();
        assert!(value);
        assert_eq!(error, None);
    }

    #[test_case("return session.prompt('hi', { session = 'abc' })", Some("abc") ; "explicit_session_id")]
    #[test_case("return session.prompt('hi')", None ; "defaults_to_focused")]
    fn prompt_forwards_text_and_session_id(code: &str, expected_id: Option<&str>) {
        let (tx, rx) = flume::unbounded::<UiAction>();
        let lua = lua_with_session(Some(tx));
        let expected_id = expected_id.map(str::to_owned);
        let checker = std::thread::spawn(move || {
            let Ok(UiAction::Session {
                req: SessionRequest::Prompt { id, text },
                reply_tx,
            }) = rx.recv()
            else {
                panic!("expected prompt request");
            };
            assert_eq!(id, expected_id);
            assert_eq!(text, "hi");
            reply_tx.send(Ok(json!("queued"))).unwrap();
        });
        let (val, err): (String, Option<String>) =
            smol::block_on(lua.load(code).eval_async()).unwrap();
        checker.join().unwrap();
        assert_eq!(err, None);
        assert_eq!(val, "queued");
    }

    #[test]
    fn notify_is_synchronous_and_queues_an_observation() {
        let id = MakiId::generate();
        let mailbox = SessionMailbox::register(id);
        let (tx, rx) = flume::unbounded::<UiAction>();
        let lua = lua_with_session(Some(tx));
        lua.globals().set("session_id", id.to_string()).unwrap();

        let (value, error): (bool, Option<String>) = lua
            .load("return session.notify('built', { session = session_id })")
            .eval()
            .unwrap();

        assert!(value);
        assert_eq!(error, None);
        let messages = mailbox.drain();
        assert_eq!(messages.len(), 1);
        assert!(messages[0].is_observation());
        assert_eq!(messages[0].user_text(), Some("built"));
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn waking_notify_sets_the_mailbox_wake_flag() {
        let id = MakiId::generate();
        let mailbox = SessionMailbox::register(id);
        let lua = lua_with_session(None);
        lua.globals().set("session_id", id.to_string()).unwrap();

        let (value, error): (bool, Option<String>) = lua
            .load("return session.notify('failed', { session = session_id, wake = true })")
            .eval()
            .unwrap();

        assert!(value);
        assert_eq!(error, None);
        assert_eq!(mailbox.claim_wake().len(), 1);
    }

    #[test]
    fn notify_rejects_missing_and_non_live_sessions() {
        let lua = lua_with_session(None);
        let (_, missing): (Value, Option<String>) =
            lua.load("return session.notify('built')").eval().unwrap();
        assert_eq!(missing.as_deref(), Some(SESSION_REQUIRED_ERR));

        let id = MakiId::generate();
        lua.globals().set("session_id", id.to_string()).unwrap();
        let (_, not_live): (Value, Option<String>) = lua
            .load("return session.notify('built', { session = session_id })")
            .eval()
            .unwrap();
        assert_eq!(not_live, Some(format!("session not live: {id}")));
    }

    #[test]
    fn notify_rejects_blank_text_and_invalid_session_ids() {
        let lua = lua_with_session(None);
        let (_, blank): (Value, Option<String>) = lua
            .load("return session.notify(' ', { session = 'invalid' })")
            .eval()
            .unwrap();
        assert_eq!(blank.as_deref(), Some(BLANK_NOTIFY_ERR));

        let (_, invalid): (Value, Option<String>) = lua
            .load("return session.notify('built', { session = 'invalid' })")
            .eval()
            .unwrap();
        assert!(invalid.is_some_and(|error| error.contains("invalid base58")));
    }

    #[test]
    fn set_title_with_wrong_type_throws() {
        let lua = lua_with_session(None);
        let result: LuaResult<Value> =
            smol::block_on(lua.load("return session.set_title('oops')").eval_async());
        assert!(result.unwrap_err().to_string().contains("table"));
    }

    #[test]
    fn set_thinking_forwards_mode_and_set_default_flag() {
        let (tx, rx) = flume::unbounded::<UiAction>();
        let lua = lua_with_session(Some(tx));
        let checker = std::thread::spawn(move || {
            let Ok(UiAction::Session {
                req:
                    SessionRequest::SetThinking {
                        set_default,
                        thinking,
                    },
                reply_tx,
            }) = rx.recv()
            else {
                panic!("expected set_thinking request");
            };
            assert!(set_default);
            assert_eq!(thinking, "medium");
            reply_tx.send(Ok(json!({ "mode": "medium" }))).unwrap();
        });
        let (val, err): (Table, Option<String>) = smol::block_on(
            lua.load("return session.set_thinking({ mode = 'medium', set_default = true })")
                .eval_async(),
        )
        .unwrap();
        assert_eq!(err, None);
        assert_eq!(val.get::<String>("mode").unwrap(), "medium");
        checker.join().unwrap();
    }

    #[test]
    fn thinking_roundtrips_mode_and_support_flag() {
        let (tx, rx) = flume::unbounded::<UiAction>();
        let lua = lua_with_session(Some(tx));
        let checker = std::thread::spawn(move || {
            let Ok(UiAction::Session {
                req: SessionRequest::GetThinking,
                reply_tx,
            }) = rx.recv()
            else {
                panic!("expected get_thinking request");
            };
            reply_tx
                .send(Ok(json!({ "mode": "high", "supports_thinking": true })))
                .unwrap();
        });
        let (val, err): (Table, Option<String>) =
            smol::block_on(lua.load("return session.thinking()").eval_async()).unwrap();
        assert_eq!(err, None);
        assert_eq!(val.get::<String>("mode").unwrap(), "high");
        assert!(val.get::<bool>("supports_thinking").unwrap());
        checker.join().unwrap();
    }
}
