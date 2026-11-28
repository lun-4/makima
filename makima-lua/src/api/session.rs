//! `makima.session`: host session primitives. Session management round-trips to
//! the UI event loop, which owns live runtimes and storage. `notify` posts
//! directly to the agent mailbox so synchronous callbacks can use it.

use makima_agent::SessionMailbox;
use makima_agent::live_history;
use makima_lua_macro::{lua_fn, lua_table};
use makima_providers::{ContentBlock, Message, MessageKind, Role, parse_thinking};
use makima_storage::id::MakimaId;
use makima_storage::sessions::{Prefs, write_prefs};
use mlua::{Lua, Result as LuaResult, Table, Value};
use serde_json::json;

use crate::agent_errors::{
    DELEGATED_FOREIGN_SESSION, DELEGATED_HOST_MUTATION, DELEGATED_SESSION_LIFECYCLE,
    SESSION_REQUIRED_ERR,
};
use crate::api::env::host_state_dir;
use crate::api::util::command::{SessionRequest, UiAction, ui_json_roundtrip};
use crate::api::util::convert::json_to_lua;
use crate::api::util::pair::{Pair, err_pair, try_pair};

/// Answers `makima.session.read` for a driver that has no UI to ask. Takes the
/// session id from Lua and returns a serialized
/// [`crate::session_snapshot::SessionSnapshot`].
pub type SessionSnapshotFn =
    Box<dyn Fn(MakimaId) -> Result<serde_json::Value, String> + Send + Sync + 'static>;

pub struct SessionSnapshotSlot(pub SessionSnapshotFn);

/// Present in print, which saves nothing.
pub(crate) struct NoPreferences;

const BLANK_NOTIFY_ERR: &str = "text must not be blank";
const NO_PREFERENCES_ERR: &str = "print saves no preferences";
const NOT_LIVE_ERR: &str = "session not live";
const NO_FOCUSED_SESSION_ERR: &str = "no focused session";

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
fn required_session(opts: Option<&Table>) -> LuaResult<Result<MakimaId, String>> {
    let Some(raw) = opts
        .map(|opts| opts.get::<Option<String>>("session"))
        .transpose()?
        .flatten()
    else {
        return Ok(Err(SESSION_REQUIRED_ERR.to_owned()));
    };
    Ok(raw.parse::<MakimaId>().map_err(|error| error.to_string()))
}

/// Lists sessions stored for the current project. Answered from a
/// background scan, so a slow disk never blocks the UI. `open_elsewhere` is
/// true while another makima instance has the session open. `message_count`
/// counts main transcript messages only and excludes subagent histories.
///
/// @return (table|nil, string|nil) Array of `{id, title, updated_at, cwd, message_count, open_elsewhere}`, or nil and an error.
/// @example
/// local stored, err = makima.session.list()
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
/// local stored, err = makima.session.list_all()
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
/// local live, err = makima.session.live()
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
/// local _, err = makima.session.focus(id)
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
/// local _, err = makima.session.delete(id)
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
/// local s = makima.session.read({ session = ctx:session_id() })
/// makima.ui.notify("spent " .. tostring(s.usage.total.cost))
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

/// The focused tab's id. A headless driver has no focused session, so a
/// plugin running there has to name one.
async fn focused_session(
    lua: Lua,
    tx: Option<flume::Sender<UiAction>>,
) -> Result<MakimaId, String> {
    let (raw, error) = roundtrip(lua, tx, SessionRequest::Current)
        .await
        .map_err(|error| error.to_string())?;
    if let Some(error) = error {
        return Err(error);
    }
    let raw = raw
        .as_ref()
        .and_then(|value| value.as_string())
        .and_then(|text| text.to_str().ok())
        .ok_or_else(|| NO_FOCUSED_SESSION_ERR.to_owned())?;
    raw.parse::<MakimaId>().map_err(|error| error.to_string())
}

fn block_json(block: &ContentBlock) -> Option<serde_json::Value> {
    Some(match block {
        ContentBlock::Text { text } => json!({ "type": "text", "text": text }),
        ContentBlock::Thinking { thinking, .. } => json!({ "type": "thinking", "text": thinking }),
        ContentBlock::RedactedThinking { .. } => return None,
        ContentBlock::ToolUse {
            id, name, input, ..
        } => {
            json!({ "type": "tool_use", "id": id, "name": name, "input": input })
        }
        ContentBlock::ToolResult {
            tool_use_id,
            content,
            is_error,
        } => json!({
            "type": "tool_result",
            "tool_use_id": tool_use_id,
            "content": content,
            "is_error": is_error,
        }),
        ContentBlock::Image { source } => {
            json!({ "type": "image", "media_type": source.media_type.mime() })
        }
    })
}

/// An image keeps only its media type. A plugin reading history wants to know
/// one was there, and the base64 payload would cost megabytes per call.
fn message_json(message: &Message) -> serde_json::Value {
    let content: Vec<_> = message.content.iter().filter_map(block_json).collect();
    json!({
        "role": match message.role {
            Role::User => "user",
            Role::Assistant => "assistant",
        },
        "kind": match message.kind {
            MessageKind::Turn => "turn",
            MessageKind::Observation => "observation",
        },
        "hidden": message.display_text.as_deref() == Some(""),
        "content": content,
    })
}

/// Reads a live session's transcript, oldest first: everything the model has
/// been sent so far, tool calls and results included. Read only.
///
/// Each message is `{ role, kind, hidden, content }`. `role` is `"user"` or
/// `"assistant"`. `kind` is `"turn"` for something the user or the model said
/// and `"observation"` for a report sent to the model as a user message, like
/// `makima.session.notify`. `hidden` marks a message only the model sees, such
/// as a nudge or a compaction note. `content` lists blocks:
///
/// ```text
/// { type = "text", text }
/// { type = "thinking", text }
/// { type = "tool_use", id, name, input }
/// { type = "tool_result", tool_use_id, content, is_error }
/// { type = "image", media_type }
/// ```
///
/// Works in the TUI and in the drivers that keep a live session, `makima -p`
/// and SDK included. A headless driver has no focused session, so pass
/// `session` there; hook and event payloads carry the `session_id` to pass.
///
/// @param opts table? Options:
///   `session` (string?) id of a live session, defaults to the focused one.
///   `last` (integer?) only the newest `last` messages.
/// @return (table|nil, string|nil) Array of messages, or nil and an error.
/// @example
/// local msgs = makima.session.messages({ last = 1 })
/// local last = msgs and msgs[1]
/// if last and last.role == "assistant" then
///   makima.ui.notify(last.content[1].text)
/// end
#[lua_fn]
async fn messages(
    lua: Lua,
    #[ctx] tx: Option<flume::Sender<UiAction>>,
    opts: Option<Table>,
) -> LuaResult<Pair<Table>> {
    let (id, last) = match &opts {
        Some(opts) => (
            opts.get::<Option<String>>("session")?,
            opts.get::<Option<usize>>("last")?,
        ),
        None => (None, None),
    };
    let session: MakimaId = match id {
        Some(id) => try_pair!(id.parse::<MakimaId>()),
        None => try_pair!(focused_session(lua.clone(), tx).await),
    };
    let Some(history) = live_history(session) else {
        return Ok(err_pair(format!("{NOT_LIVE_ERR}: {session}")));
    };
    let skip = last.map_or(0, |count| history.len().saturating_sub(count));
    let out = lua.create_table()?;
    for message in &history[skip..] {
        out.push(json_to_lua(&lua, &message_json(message))?)?;
    }
    Ok((Some(out), None))
}

/// Starts a new session in the current project.
///
/// @param opts table? Optional fields: prompt (string) first user message
///   to submit right away; focus (boolean) switch the UI to the new session.
/// @return (string|nil, string|nil) New session id, or nil and an error.
/// @example
/// local id, err = makima.session.new({ prompt = "fix the tests", focus = true })
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
/// makima.session.notify("[monitor] deploy failed", { session = id, wake = true })
#[lua_fn]
fn notify(_lua: &Lua, text: String, opts: Table) -> LuaResult<Pair<bool>> {
    if text.trim().is_empty() {
        return Ok(err_pair(BLANK_NOTIFY_ERR));
    }
    let Some(raw_id) = opts.get::<Option<String>>("session")? else {
        return Ok(err_pair(SESSION_REQUIRED_ERR));
    };
    let session_id: MakimaId = match raw_id.parse() {
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
/// local _, err = makima.session.set_title({ id = id, title = "refactor" })
#[lua_fn]
async fn set_title(
    lua: Lua,
    #[ctx] tx: Option<flume::Sender<UiAction>>,
    opts: Table,
) -> LuaResult<Pair<Value>> {
    let id: String = opts.get("id")?;
    let parsed = try_pair!(id.parse::<MakimaId>());
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
/// makima.session.set_default_thinking("medium")
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
fn delegated_tool_denial(lua: &Lua, session: MakimaId) -> Option<&'static str> {
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
    "makima.session" => pub(crate) fn create_session_table(tx: Option<flume::Sender<UiAction>>),
    DOCS [list(tx), list_all(tx), live(tx), read(tx), messages(tx), focus(tx), delete(tx), new(tx), notify(), set_title(tx), set_default_thinking]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::util::command::NO_UI_ERR;
    use std::sync::Arc;

    use arc_swap::ArcSwap;
    use makima_agent::{HistorySnapshot, SharedMessages, publish_live_history};

    use makima_agent::session_coordinator::{
        SessionCheckpoint, SessionCoordinatorHandle, SessionCoordinatorParams,
        builtin_option_definitions,
    };
    use makima_storage::checkpoint::{CheckpointWriter, accept_all};
    use mlua::Value;
    use serde_json::json;
    use test_case::test_case;

    fn lua_with_session(tx: Option<flume::Sender<UiAction>>) -> Lua {
        let lua = Lua::new();
        let t = create_session_table(&lua, tx).unwrap();
        lua.globals().set("session", t).unwrap();
        lua
    }

    fn live_mailbox(id: MakimaId) -> (SessionMailbox, SessionCoordinatorHandle) {
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
                makima_agent::ThinkingConfig::Off,
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
        let id = MakimaId::generate();
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
        let id = MakimaId::generate();
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

        let id = MakimaId::generate();
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

    const FOCUSED_TEXT: &str = "focused";
    const OTHER_TEXT: &str = "other";
    const FIRST: &str = "first";

    fn published(id: MakimaId, messages: Vec<Message>) -> SharedMessages {
        let mirror: SharedMessages =
            Arc::new(ArcSwap::from_pointee(HistorySnapshot::new(messages)));
        publish_live_history(id, &mirror);
        mirror
    }

    fn first_text(messages: &Table) -> String {
        messages
            .get::<Table>(1)
            .unwrap()
            .get::<Table>("content")
            .unwrap()
            .get::<Table>(1)
            .unwrap()
            .get::<String>("text")
            .unwrap()
    }

    /// Answers `Current` with {id}, the way the UI does.
    fn ui_focused_on(id: MakimaId) -> Lua {
        let (tx, rx) = flume::unbounded::<UiAction>();
        std::thread::spawn(move || {
            let Ok(UiAction::Session {
                req: SessionRequest::Current,
                reply_tx,
            }) = rx.recv()
            else {
                panic!("expected current request");
            };
            reply_tx.send(Ok(json!(id.to_string()))).unwrap();
        });
        lua_with_session(Some(tx))
    }

    /// Two live sessions, one focused: the transcript read is the focused
    /// one's alone.
    #[test]
    fn messages_reads_only_the_focused_session() {
        let focused = MakimaId::generate();
        let other = MakimaId::generate();
        let _focused = published(focused, vec![Message::user(FOCUSED_TEXT.into())]);
        let _other = published(other, vec![Message::user(OTHER_TEXT.into())]);
        let lua = ui_focused_on(focused);

        let (messages, error): (Table, Option<String>) =
            smol::block_on(lua.load("return session.messages()").eval_async()).unwrap();
        assert_eq!(error, None);
        assert_eq!(messages.raw_len(), 1);
        assert_eq!(first_text(&messages), FOCUSED_TEXT);
    }

    #[test]
    fn messages_takes_an_explicit_session_and_keeps_the_newest() {
        let id = MakimaId::generate();
        let _mirror = published(
            id,
            vec![
                Message::user(FIRST.into()),
                Message::user(OTHER_TEXT.into()),
            ],
        );
        let lua = lua_with_session(None);

        let code = format!("return session.messages({{ session = '{id}', last = 1 }})");
        let (messages, error): (Table, Option<String>) =
            smol::block_on(lua.load(&code).eval_async()).unwrap();
        assert_eq!(error, None);
        assert_eq!(messages.raw_len(), 1);
        assert_eq!(first_text(&messages), OTHER_TEXT);
    }

    #[test]
    fn messages_of_a_session_that_is_not_live_is_an_error_pair() {
        let id = MakimaId::generate();
        let lua = lua_with_session(None);
        let code = format!("return session.messages({{ session = '{id}' }})");
        let (value, error): (Value, Option<String>) =
            smol::block_on(lua.load(&code).eval_async()).unwrap();
        assert!(value.is_nil());
        assert_eq!(error, Some(format!("{NOT_LIVE_ERR}: {id}")));
    }

    #[test]
    fn messages_without_a_focused_session_is_an_error_pair() {
        let (tx, rx) = flume::unbounded::<UiAction>();
        let lua = lua_with_session(Some(tx));
        std::thread::spawn(move || {
            let Ok(UiAction::Session { reply_tx, .. }) = rx.recv() else {
                panic!("expected session request");
            };
            reply_tx.send(Ok(serde_json::Value::Null)).unwrap();
        });
        let (_, error): (Value, Option<String>) =
            smol::block_on(lua.load("return session.messages()").eval_async()).unwrap();
        assert_eq!(error.as_deref(), Some(NO_FOCUSED_SESSION_ERR));
    }

    #[test]
    fn set_title_with_wrong_type_throws() {
        let lua = lua_with_session(None);
        let result: LuaResult<Value> =
            smol::block_on(lua.load("return session.set_title('oops')").eval_async());
        assert!(result.unwrap_err().to_string().contains("table"));
    }
}
