//! `maki.session`: host session primitives. Session management round-trips to
//! the UI event loop, which owns live runtimes and storage. `notify` posts
//! directly to the agent mailbox so synchronous callbacks can use it.

use maki_agent::SessionMailbox;
use maki_agent::session_coordinator::SessionCoordinatorHandle;
use maki_agent::session_options::{
    SessionOptionCategory, SessionOptionOwner, SessionOptionsSnapshot,
};
use maki_lua_macro::{lua_fn, lua_table};
use maki_storage::id::MakiId;
use mlua::{Lua, Result as LuaResult, Table, Value};

use crate::api::session_option::{ensure_validation_not_in_progress, validate_option_value};
use crate::api::util::command::{SessionRequest, UiAction, ui_json_roundtrip};
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

pub(crate) async fn resolve_coordinator(
    lua: &Lua,
    tx: Option<&flume::Sender<UiAction>>,
    opts: Option<&Table>,
) -> Result<SessionCoordinatorHandle, String> {
    let raw_id = match opts {
        Some(opts) => opts
            .get::<Option<String>>("session")
            .map_err(|e| e.to_string())?,
        None => None,
    };
    let raw_id = match raw_id {
        Some(id) => id,
        None => {
            let (value, error) = ui_json_roundtrip(lua, tx, |reply_tx| UiAction::Session {
                req: SessionRequest::Current,
                reply_tx,
            })
            .await
            .map_err(|e| e.to_string())?;
            if let Some(error) = error {
                return Err(error);
            }
            value
                .and_then(|value| {
                    value
                        .as_string()
                        .and_then(|value| value.to_str().ok().map(|value| value.to_string()))
                })
                .ok_or_else(|| SESSION_REQUIRED_ERR.to_string())?
        }
    };
    let id = raw_id.parse::<MakiId>().map_err(|e| e.to_string())?;
    SessionCoordinatorHandle::resolve(id).map_err(|e| e.to_string())
}

fn snapshot_table(lua: &Lua, snapshot: &SessionOptionsSnapshot) -> LuaResult<Table> {
    let result = lua.create_table()?;
    result.set("version", snapshot.version)?;
    let options = lua.create_table_with_capacity(snapshot.options.len(), 0)?;
    for (index, state) in snapshot.options.iter().enumerate() {
        let option = lua.create_table()?;
        let definition = &state.definition;
        option.set("id", definition.id.as_ref())?;
        option.set("name", definition.name.as_ref())?;
        option.set("description", definition.description.as_ref())?;
        option.set(
            "category",
            match definition.category {
                SessionOptionCategory::Model => "model",
                SessionOptionCategory::Mode => "mode",
            },
        )?;
        option.set("current_value", state.current_value.as_ref())?;
        option.set("persistent", definition.persistent)?;
        option.set(
            "owner",
            match &definition.owner {
                SessionOptionOwner::Builtin => "builtin",
                SessionOptionOwner::Plugin { plugin, .. } => plugin.as_ref(),
            },
        )?;
        let values = lua.create_table_with_capacity(definition.values.len(), 0)?;
        for (value_index, value) in definition.values.iter().enumerate() {
            let item = lua.create_table()?;
            item.set("value", value.value.as_ref())?;
            item.set("name", value.name.as_ref())?;
            values.set(value_index + 1, item)?;
        }
        option.set("values", values)?;
        options.set(index + 1, option)?;
    }
    result.set("options", options)?;
    Ok(result)
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

/// Returns the complete ordered option snapshot for a live session.
///
/// @param opts table? Optional `session` id; defaults to the focused session.
/// @return (table|nil, string|nil) `{version, options}`, or nil and an error.
/// @example
/// local snapshot, err = maki.session.options({ session = id })
#[lua_fn]
async fn options(
    lua: Lua,
    #[ctx] tx: Option<flume::Sender<UiAction>>,
    opts: Option<Table>,
) -> LuaResult<Pair<Table>> {
    let coordinator = match resolve_coordinator(&lua, tx.as_ref(), opts.as_ref()).await {
        Ok(coordinator) => coordinator,
        Err(error) => return Ok(err_pair(error)),
    };
    Ok((
        Some(snapshot_table(&lua, &coordinator.read().options())?),
        None,
    ))
}

/// Sets one option explicitly for a live session. Validation, runtime adoption,
/// and persistence complete before success is returned.
///
/// @param id string Stable option id.
/// @param value string Selectable value id.
/// @param opts table? Optional `session` id; defaults to the focused session.
/// @return (boolean|nil, string|nil) true, or nil and an error.
/// @example
/// local ok, err = maki.session.set_option("fast", "enabled", { session = id })
#[lua_fn]
async fn set_option(
    lua: Lua,
    #[ctx] tx: Option<flume::Sender<UiAction>>,
    id: String,
    value: String,
    opts: Option<Table>,
) -> LuaResult<Pair<bool>> {
    if let Err(error) = ensure_validation_not_in_progress(&lua) {
        return Ok(err_pair(error));
    }
    let coordinator = match resolve_coordinator(&lua, tx.as_ref(), opts.as_ref()).await {
        Ok(coordinator) => coordinator,
        Err(error) => return Ok(err_pair(error)),
    };
    let snapshot = coordinator.read().options();
    if let Some(option) = snapshot
        .options
        .iter()
        .find(|option| option.definition.id.as_ref() == id)
        && let Err(error) = validate_option_value(&lua, option, &value)
    {
        return Ok(err_pair(error));
    }
    match coordinator
        .set_option_if_version(id, value, Some(snapshot.version))
        .await
    {
        Ok(_) => Ok((Some(true), None)),
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
    /// at once; these functions let plugins list, create, focus, rename, and
    /// delete them. Session management returns `nil, "no interactive UI
    /// attached"` without a UI. `notify` instead targets a live agent mailbox
    /// directly, so it also works under ACP and SDK frontends.
    "maki.session" => pub(crate) fn create_session_table(tx: Option<flume::Sender<UiAction>>),
    DOCS [list(tx), list_all(tx), live(tx), current(tx), focus(tx), delete(tx), new(tx), prompt(tx), notify(), options(tx), set_option(tx), set_title(tx), thinking(tx), set_thinking(tx)]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::util::command::NO_UI_ERR;
    use std::path::PathBuf;
    use std::sync::Arc;

    use maki_agent::session_coordinator::{
        DirectoryAdoptionFuture, ModelAdoptionFuture, SessionCheckpoint, SessionCoordinatorHandle,
        SessionCoordinatorParams, builtin_option_definitions,
    };
    use maki_providers::Model;
    use maki_storage::checkpoint::{
        CheckpointAck, CheckpointFuture, CheckpointRequest, CheckpointWriter,
    };
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
        let checkpoint: Arc<dyn CheckpointWriter<SessionCheckpoint>> =
            Arc::new(|request: CheckpointRequest<SessionCheckpoint>| {
                Box::pin(async move {
                    Ok(CheckpointAck {
                        session_id: request.session_id,
                        version: request.version,
                    })
                }) as CheckpointFuture
            });
        let coordinator = SessionCoordinatorHandle::register(SessionCoordinatorParams {
            session_id: id,
            catalog: Default::default(),
            definitions: builtin_option_definitions(
                "test/model",
                [Arc::from("test/model")],
                false,
                false,
                false,
            ),
            persisted_options: Default::default(),
            history: Vec::new(),
            model: Arc::from("test/model"),
            cwd: PathBuf::from("/project"),
            model_policy: Arc::default(),
            model_adopter: Arc::new(|_: Model| Box::pin(async { Ok(()) }) as ModelAdoptionFuture),
            directory_adopter: Arc::new(|path: PathBuf| {
                Box::pin(async move { Ok(path) }) as DirectoryAdoptionFuture
            }),
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
    fn options_explicit_session_returns_complete_ordered_snapshot() {
        let id = MakiId::generate();
        let (_, coordinator) = live_mailbox(id);
        let lua = lua_with_session(None);
        lua.globals().set("session_id", id.to_string()).unwrap();

        let (snapshot, error): (Table, Option<String>) = smol::block_on(
            lua.load("return session.options({ session = session_id })")
                .eval_async(),
        )
        .unwrap();

        assert_eq!(error, None);
        assert_eq!(snapshot.get::<u64>("version").unwrap(), 1);
        let options = snapshot.get::<Table>("options").unwrap();
        let ids = options
            .sequence_values::<Table>()
            .map(|option| option.unwrap().get::<String>("id").unwrap())
            .collect::<Vec<_>>();
        assert_eq!(ids, ["model", "yolo", "fast", "workflow"]);
        let model = options.get::<Table>(1).unwrap();
        assert_eq!(model.get::<String>("category").unwrap(), "model");
        assert_eq!(model.get::<String>("current_value").unwrap(), "test/model");
        assert_eq!(model.get::<Table>("values").unwrap().raw_len(), 1);
        smol::block_on(coordinator.close()).unwrap();
    }

    #[test]
    fn focused_session_options_and_setter_use_coordinator() {
        let id = MakiId::generate();
        let (_, coordinator) = live_mailbox(id);
        let (tx, rx) = flume::unbounded::<UiAction>();
        let lua = lua_with_session(Some(tx));
        let session_id = id.to_string();
        let responder = std::thread::spawn(move || {
            for _ in 0..2 {
                let Ok(UiAction::Session {
                    req: SessionRequest::Current,
                    reply_tx,
                }) = rx.recv()
                else {
                    panic!("expected current-session request");
                };
                reply_tx.send(Ok(json!(session_id))).unwrap();
            }
        });

        let (ok, error): (bool, Option<String>) = smol::block_on(
            lua.load("return session.set_option('yolo', 'enabled')")
                .eval_async(),
        )
        .unwrap();
        assert!(ok);
        assert_eq!(error, None);

        let (snapshot, error): (Table, Option<String>) =
            smol::block_on(lua.load("return session.options()").eval_async()).unwrap();
        assert_eq!(error, None);
        let yolo = snapshot
            .get::<Table>("options")
            .unwrap()
            .get::<Table>(2)
            .unwrap();
        assert_eq!(yolo.get::<String>("current_value").unwrap(), "enabled");
        responder.join().unwrap();
        smol::block_on(coordinator.close()).unwrap();
    }

    #[test]
    fn options_rejects_closed_explicit_session() {
        let id = MakiId::generate();
        let (_, coordinator) = live_mailbox(id);
        smol::block_on(coordinator.close()).unwrap();
        let lua = lua_with_session(None);
        lua.globals().set("session_id", id.to_string()).unwrap();

        let (value, error): (Value, Option<String>) = smol::block_on(
            lua.load("return session.options({ session = session_id })")
                .eval_async(),
        )
        .unwrap();

        assert!(value.is_nil());
        assert_eq!(error, Some(format!("session not live: {id}")));
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
