//! `maki.session`: host session primitives. Session management round-trips to
//! the UI event loop, which owns live runtimes and storage. `notify` posts
//! directly to the agent mailbox so synchronous callbacks can use it.

use maki_agent::SessionMailbox;
use maki_lua_macro::{lua_fn, lua_table};
use maki_providers::parse_thinking;
use maki_storage::id::MakiId;
use maki_storage::sessions::{Prefs, write_prefs};
use mlua::{Lua, Result as LuaResult, Table, Value};

use crate::agent_errors::{
    DELEGATED_FOREIGN_SESSION, DELEGATED_HOST_MUTATION, DELEGATED_SESSION_LIFECYCLE,
    SESSION_REQUIRED_ERR,
};
use crate::api::env::host_state_dir;
use crate::api::util::command::{SessionRequest, UiAction, ui_json_roundtrip};
use crate::api::util::convert::json_to_lua;
use crate::api::util::pair::{Pair, err_pair, try_pair};

/// Answers `maki.session.read` for a driver that has no UI to ask. Takes the
/// session id from Lua and returns a serialized
/// [`crate::session_snapshot::SessionSnapshot`].
pub type SessionSnapshotFn =
    Box<dyn Fn(MakiId) -> Result<serde_json::Value, String> + Send + Sync + 'static>;

pub struct SessionSnapshotSlot(pub SessionSnapshotFn);

/// Present in print, which saves nothing.
pub(crate) struct NoPreferences;

const BLANK_NOTIFY_ERR: &str = "text must not be blank";
const NO_PREFERENCES_ERR: &str = "print saves no preferences";

async fn roundtrip(
    lua: Lua,
    tx: Option<flume::Sender<UiAction>>,
    req: SessionRequest,
) -> LuaResult<Pair<Value>> {
    try_pair!(crate::runtime::validate_plugin_effect(&lua));
    ui_json_roundtrip(&lua, tx.as_ref(), |reply_tx| UiAction::Session {
        req,
        reply_tx,
    })
    .await
}

/// The `session` id {opts} must name; the inner error is a missing or
/// malformed id.
fn required_session(opts: Option<&Table>) -> LuaResult<Result<MakiId, String>> {
    let Some(raw) = opts
        .map(|opts| opts.get::<Option<String>>("session"))
        .transpose()?
        .flatten()
    else {
        return Ok(Err(SESSION_REQUIRED_ERR.to_owned()));
    };
    Ok(raw.parse::<MakiId>().map_err(|error| error.to_string()))
}

/// Lists sessions stored for the current project. Answered from a
/// background scan, so a slow disk never blocks the UI. `open_elsewhere` is
/// true while another makima instance has the session open. `message_count`
/// counts main transcript messages only and excludes subagent histories.
///
/// @return (table|nil, string|nil) Array of `{id, title, updated_at, cwd, message_count, open_elsewhere}`, or nil and an error.
/// @example
/// local stored, err = maki.session.list()
#[lua_fn]
async fn list(lua: Lua, #[ctx] tx: Option<flume::Sender<UiAction>>) -> LuaResult<Pair<Value>> {
    roundtrip(lua, tx, SessionRequest::List).await
}

/// Lists stored sessions across every project directory, most recently
/// updated first. Answered from a background scan, so a slow disk never
/// blocks the UI. `open_elsewhere` is true while another makima instance has
/// the session open. `message_count` counts main transcript messages only
/// and excludes subagent histories.
///
/// @return (table|nil, string|nil) Array of `{id, title, updated_at, cwd, message_count, open_elsewhere}`, or nil and an error.
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
/// `message_count` counts main transcript messages only and excludes subagent histories.
///
/// @return (table|nil, string|nil) Array of `{id, title, status, updated_at, message_count, focused}`, or nil and an error.
/// @example
/// local live, err = maki.session.live()
#[lua_fn]
async fn live(lua: Lua, #[ctx] tx: Option<flume::Sender<UiAction>>) -> LuaResult<Pair<Value>> {
    roundtrip(lua, tx, SessionRequest::Live).await
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
    if crate::runtime::in_delegated_tool(&lua) {
        return Ok(err_pair(DELEGATED_SESSION_LIFECYCLE));
    }
    roundtrip(lua, tx, SessionRequest::Focus { id }).await
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
    if crate::runtime::in_delegated_tool(&lua) {
        return Ok(err_pair(DELEGATED_SESSION_LIFECYCLE));
    }
    roundtrip(lua, tx, SessionRequest::Delete { id }).await
}

/// Snapshot of the session you name in `session`.
///
/// The returned table:
/// ```lua
/// {
///   id,
///   title,             -- nil under headless drivers
///   status = "idle" | "working" | "needs_input",
///   updated_at,        -- unix seconds
///   usage = {
///     total = { input, output, cache_creation, cache_read, cost },
///     models = { { model, input, output, cache_creation, cache_read, cost }, ... },
///   },
/// }
/// ```
///
/// `usage` includes subagent spend. Costs are the values recorded when each
/// turn was billed and stay nil when unpriced. `models` is sorted by total
/// tokens, descending, then spec; headless drivers do not split usage by
/// model and leave it empty. Model, mode and options are on the agent:
/// `Agent:config(ctx)`; per-turn context size and list price are on the
/// `TurnEnd` autocmd.
///
/// @param opts table Required `session` (string) id.
/// @return (table|nil, string|nil) Snapshot table, or nil and an error.
/// @example
/// local s = maki.session.read({ session = ctx:session_id() })
/// maki.ui.notify("spent " .. tostring(s.usage.total.cost))
#[lua_fn]
async fn read(
    lua: Lua,
    #[ctx] tx: Option<flume::Sender<UiAction>>,
    opts: Table,
) -> LuaResult<Pair<Value>> {
    let id = try_pair!(required_session(Some(&opts))?);
    if let Some(slot) = lua.app_data_ref::<SessionSnapshotSlot>() {
        return match (slot.0)(id) {
            Ok(value) => Ok((Some(json_to_lua(&lua, &value)?), None)),
            Err(msg) => Ok(err_pair(msg)),
        };
    }
    ui_json_roundtrip(&lua, tx.as_ref(), |reply_tx| UiAction::Session {
        req: SessionRequest::Read { id },
        reply_tx,
    })
    .await
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
    if crate::runtime::in_delegated_tool(&lua) {
        return Ok(err_pair(DELEGATED_SESSION_LIFECYCLE));
    }
    let (prompt, focus) = match opts {
        Some(opts) => (opts.get("prompt")?, opts.get("focus").unwrap_or(false)),
        None => (None, false),
    };
    roundtrip(lua, tx, SessionRequest::New { prompt, focus }).await
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
fn notify(_lua: &Lua, text: String, opts: Table) -> LuaResult<Pair<bool>> {
    if text.trim().is_empty() {
        return Ok(err_pair(BLANK_NOTIFY_ERR));
    }
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
    let id: String = opts.get("id")?;
    let parsed = try_pair!(id.parse::<MakiId>());
    if let Some(denial) = delegated_tool_denial(&lua, parsed) {
        return Ok(err_pair(denial));
    }
    let req = SessionRequest::SetTitle {
        id,
        title: opts.get("title")?,
    };
    roundtrip(lua, tx, req).await
}

/// Persists the default thinking setting for new sessions without changing
/// any live session. Every frontend starts a blank session on it, ahead of
/// `always_thinking`. `mode` is a thinking setting as `Agent:set_model`
/// accepts it. Print saves nothing, so there it returns an error and leaves
/// the saved default alone.
///
/// @param mode string The thinking setting.
/// @return (table|nil, string|nil) `{mode}` with the normalized setting, or nil and an error.
/// @example
/// maki.session.set_default_thinking("medium")
#[lua_fn]
fn set_default_thinking(lua: &Lua, mode: String) -> LuaResult<Pair<Table>> {
    if crate::runtime::in_delegated_tool(lua) {
        return Ok(err_pair(DELEGATED_HOST_MUTATION));
    }
    try_pair!(crate::runtime::validate_plugin_effect(lua));
    if lua.app_data_ref::<NoPreferences>().is_some() {
        return Ok(err_pair(NO_PREFERENCES_ERR));
    }
    let thinking = try_pair!(parse_thinking(mode.trim()));
    let state_dir = try_pair!(host_state_dir(lua));
    try_pair!(write_prefs(
        &state_dir,
        &Prefs {
            default_thinking: Some(thinking.into()),
        },
    ));
    let reply = lua.create_table()?;
    reply.set("mode", thinking.to_string())?;
    Ok((Some(reply), None))
}

/// Delegated tools may change only the session their turn belongs to.
fn delegated_tool_denial(lua: &Lua, session: MakiId) -> Option<&'static str> {
    let tool = crate::runtime::delegated_tool(lua)?;
    (tool.session != Some(session)).then_some(DELEGATED_FOREIGN_SESSION)
}

lua_table! {
    /// Host session primitives. The interactive UI can run several sessions
    /// at once; these functions let plugins list, create, focus, rename, and
    /// delete them. Session management returns `nil, "no interactive UI
    /// attached"` without a UI. `notify` instead targets a live agent mailbox
    /// directly, so it also works under ACP and SDK frontends. A tool handler
    /// cannot create, focus or delete sessions, and the functions that change
    /// a session (`set_title`) accept
    /// only the session its own turn belongs to. Every function that reads or
    /// changes one session takes it explicitly; `ctx:session_id()` names the
    /// invocation's session.
    "maki.session" => pub(crate) fn create_session_table(tx: Option<flume::Sender<UiAction>>),
    DOCS [list(tx), list_all(tx), live(tx), read(tx), focus(tx), delete(tx), new(tx), notify(), set_title(tx), set_default_thinking]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::util::command::NO_UI_ERR;
    use std::sync::Arc;

    use maki_agent::session_coordinator::{
        SessionCheckpoint, SessionCoordinatorHandle, SessionCoordinatorParams,
        builtin_option_definitions,
    };
    use maki_storage::checkpoint::{CheckpointWriter, accept_all};
    use mlua::Value;
    use serde_json::json;
    use test_case::test_case;

    fn lua_with_session(tx: Option<flume::Sender<UiAction>>) -> Lua {
        let lua = Lua::new();
        let t = create_session_table(&lua, tx).unwrap();
        lua.globals().set("session", t).unwrap();
        lua
    }

    fn live_mailbox(id: MakiId) -> (SessionMailbox, SessionCoordinatorHandle) {
        let mailbox = SessionMailbox::new(id);
        let checkpoint: Arc<dyn CheckpointWriter<SessionCheckpoint>> = accept_all();
        let coordinator = SessionCoordinatorHandle::register(SessionCoordinatorParams {
            session_id: id,
            catalog: Default::default(),
            definitions: builtin_option_definitions(
                "test/model",
                [Arc::from("test/model")],
                false,
                false,
                false,
                maki_agent::ThinkingConfig::Off,
            ),
            persisted_options: Default::default(),
            model_policy: Arc::default(),
            checkpoint,
            mailbox: mailbox.clone(),
        })
        .unwrap();
        (mailbox, coordinator)
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

    #[test_case("return session.read({})" ; "read")]
    fn session_target_is_required(code: &str) {
        let (tx, rx) = flume::unbounded::<UiAction>();
        let lua = lua_with_session(Some(tx));
        let (value, error): (Value, Option<String>) =
            smol::block_on(lua.load(code).eval_async()).unwrap();
        assert!(value.is_nil());
        assert_eq!(error.as_deref(), Some(SESSION_REQUIRED_ERR));
        assert!(rx.is_empty());
    }

    #[test]
    fn notify_is_synchronous_and_queues_an_observation() {
        let id = MakiId::generate();
        let (mailbox, coordinator) = live_mailbox(id);
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
        smol::block_on(coordinator.close()).unwrap();
    }

    #[test]
    fn waking_notify_sets_the_mailbox_wake_flag() {
        let id = MakiId::generate();
        let (mailbox, coordinator) = live_mailbox(id);
        let lua = lua_with_session(None);
        lua.globals().set("session_id", id.to_string()).unwrap();

        let (value, error): (bool, Option<String>) = lua
            .load("return session.notify('failed', { session = session_id, wake = true })")
            .eval()
            .unwrap();

        assert!(value);
        assert_eq!(error, None);
        assert_eq!(mailbox.claim_wake().len(), 1);
        smol::block_on(coordinator.close()).unwrap();
    }

    #[test]
    fn notify_rejects_missing_and_non_live_sessions() {
        let lua = lua_with_session(None);
        let (_, missing): (Value, Option<String>) = lua
            .load("return session.notify('built', {})")
            .eval()
            .unwrap();
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
}
