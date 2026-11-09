use std::collections::HashMap;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicU64, Ordering},
};
use std::time::Duration;

use crossterm::event::KeyEvent;
use mlua::{Function, Result as LuaResult, Table, Value};

use crate::api::keymap::parse_event_key;
use crate::api::time::duration_from_table;
use crate::api::ui::parse_footer;

const ITEMS_MUST_BE_TABLE: &str = "open_list_picker: items must be a table";
const ITEM_MUST_BE_STRING_OR_TABLE: &str = "picker items must be strings or {label, ...} tables";
const ITEM_MISSING_LABEL: &str = "picker item missing `label`";
const ITEM_LABEL_MUST_BE_STRING: &str = "picker item `label` must be a string";
const ITEM_FIELD_MUST_BE_STRING: &str = "picker item field must be a string or nil";
const ITEM_AGE_MUST_BE_TIMESTAMP: &str =
    "picker item `age` must be a timestamp from maki.time.now or maki.time.at";
const TITLE_MUST_BE_STRING: &str = "open_list_picker: title must be a string";
const CURSOR_MUST_BE_POSITIVE: &str = "open_list_picker: cursor must be >= 1";
const TIMEOUT_MUST_BE_NON_NEGATIVE: &str = "open_list_picker: timeout_ms must be >= 0";

static NEXT_PICKER_ID: AtomicU64 = AtomicU64::new(1);

/// One entry of `maki.ui.open_list_picker`, decoded from the Lua items array.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PickerItemSpec {
    pub label: String,
    pub detail: Option<String>,
    pub section: Option<String>,
    pub section_detail: Option<String>,
    /// Unix-epoch instant, despite the user-facing `age` name. The host
    /// formats it against the current time, so it stays live after open.
    pub age: Option<Duration>,
}

/// Decoded `maki.ui.open_list_picker` options. Indices are 0-based; the
/// delete key is fixed by the host dialog (ctrl+d, old-Lua parity).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PickerConfig {
    pub title: Option<String>,
    pub footer: Option<Vec<(String, String)>>,
    pub cursor: Option<usize>,
    pub submit_keys: Vec<KeyEvent>,
    pub notify_initial: bool,
    pub timeout: Option<Duration>,
}

/// The dialog's outcome, as the waiting Lua call receives it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PickerResult {
    /// The selected item, 0-based index into the original items array.
    Choice(usize),
    /// The item the delete key fired on, 0-based original index.
    Delete(usize),
    /// Dismissed without a choice.
    Close,
}

/// Dialog events the host reports back to the Lua thread.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PickerEvent {
    /// The selection moved to `index` (0-based original item index).
    Change { index: usize },
    /// The idle window elapsed.
    Timeout,
    /// The dialog closed; the callback store entry for this id may be dropped.
    Done,
}

/// Original Lua item passed to `on_change`, looked up by 0-based original
/// index into the stored items table.
#[derive(Debug)]
pub struct PickerCallbackEntry {
    pub items: Table,
    pub on_change: Option<Function>,
    pub on_timeout: Option<Function>,
}

/// Lua-side callbacks per open dialog, keyed by dialog id. Lives in Lua
/// app-data because mlua `Function`s never cross `UiAction`: the host dialog
/// talks to this thread by id, and the dispatch loop invokes the stored
/// callbacks via `run_detached` (the job-callback pattern).
#[derive(Default)]
pub struct PickerCallbacks(Arc<Mutex<HashMap<u64, PickerCallbackEntry>>>);

impl PickerCallbacks {
    pub fn new() -> Self {
        Self::default()
    }

    /// Inserts the callbacks for a new dialog; `id` comes from
    /// [`alloc_picker_id`].
    pub fn insert(&self, id: u64, entry: PickerCallbackEntry) {
        self.0.lock().unwrap().insert(id, entry);
    }

    /// Drops the entry when the dialog is done.
    pub fn remove(&self, id: u64) {
        self.0.lock().unwrap().remove(&id);
    }

    /// The `on_change` callback plus the original item the selection moved
    /// to, for dialog `id`; `index` is 0-based into the original items array
    /// and the returned index is the 1-based Lua form.
    pub fn change_payload(&self, id: u64, index: usize) -> Option<(Function, Value, usize)> {
        let mut map = self.0.lock().unwrap();
        let entry = map.get_mut(&id)?;
        let item = entry.items.get::<Value>(index + 1).ok()?;
        entry.on_change.clone().map(|func| (func, item, index + 1))
    }

    /// The `on_timeout` callback for dialog `id`.
    pub fn timeout_callback(&self, id: u64) -> Option<Function> {
        let mut map = self.0.lock().unwrap();
        map.get_mut(&id)?.on_timeout.clone()
    }
}

/// Next dialog id. Starts at 1 so a bare `0` can mean "no dialog".
pub fn alloc_picker_id() -> u64 {
    NEXT_PICKER_ID.fetch_add(1, Ordering::Relaxed)
}

/// Decodes the Lua `items` argument: a sequence of plain strings or
/// `{label, detail?, section?, section_detail?}` tables. Returns the original
/// table (the `on_change` callback is handed entries straight from it) plus
/// the decoded specs the host dialog renders.
pub(crate) fn decode_picker_items(
    value: &Value,
) -> Result<(Table, Vec<PickerItemSpec>), mlua::Error> {
    let table = match value {
        Value::Table(t) => t.clone(),
        _ => return Err(mlua::Error::runtime(ITEMS_MUST_BE_TABLE)),
    };
    let len: i64 = table.len()?;
    let mut specs = Vec::with_capacity(len.max(0) as usize);
    for i in 1..=len {
        specs.push(decode_picker_item(&table.get::<Value>(i)?)?);
    }
    Ok((table, specs))
}

/// A `nil`-or-string field; mlua `String` would coerce numbers, so the
/// value is matched by variant.
fn optional_string(t: &Table, field: &str) -> LuaResult<Option<String>> {
    match t.get::<Option<Value>>(field)? {
        None => Ok(None),
        Some(Value::String(s)) => Ok(Some(s.to_str()?.to_owned())),
        Some(_) => Err(mlua::Error::runtime(format!(
            "{ITEM_FIELD_MUST_BE_STRING}: {field}"
        ))),
    }
}

fn decode_picker_item(value: &Value) -> Result<PickerItemSpec, mlua::Error> {
    match value {
        Value::String(s) => Ok(PickerItemSpec {
            label: s.to_str()?.to_owned(),
            ..Default::default()
        }),
        Value::Table(t) => {
            let label: String = match t.get::<Option<Value>>("label")? {
                Some(Value::String(s)) => s.to_str()?.to_owned(),
                Some(_) => return Err(mlua::Error::runtime(ITEM_LABEL_MUST_BE_STRING)),
                None => return Err(mlua::Error::runtime(ITEM_MISSING_LABEL)),
            };
            let age = match t.get::<Option<Value>>("age")? {
                None => None,
                Some(Value::Table(ts)) => Some(duration_from_table(&ts)?),
                Some(_) => return Err(mlua::Error::runtime(ITEM_AGE_MUST_BE_TIMESTAMP)),
            };
            Ok(PickerItemSpec {
                label,
                detail: optional_string(t, "detail")?,
                section: optional_string(t, "section")?,
                section_detail: optional_string(t, "section_detail")?,
                age,
            })
        }
        _ => Err(mlua::Error::runtime(ITEM_MUST_BE_STRING_OR_TABLE)),
    }
}

/// Decodes the Lua `opts` argument. Key names are validated up front via
/// [`parse_event_key`] so a bad config fails the call, not the dialog.
pub(crate) fn decode_picker_opts(opts: Option<&Table>) -> Result<PickerConfig, mlua::Error> {
    let Some(opts) = opts else {
        return Ok(PickerConfig::default());
    };
    let title = match opts.get::<Option<Value>>("title")? {
        Some(Value::String(s)) => Some(s.to_str()?.to_owned()),
        Some(_) => return Err(mlua::Error::runtime(TITLE_MUST_BE_STRING)),
        None => None,
    };
    let footer = parse_footer(opts)?;
    let cursor = match opts.get::<Option<i64>>("cursor").ok().flatten() {
        Some(c) if c >= 1 => Some(c as usize - 1),
        Some(_) => return Err(mlua::Error::runtime(CURSOR_MUST_BE_POSITIVE)),
        None => None,
    };
    let mut submit_keys = Vec::new();
    if let Some(keys) = opts.get::<Option<Table>>("submit_keys").ok().flatten() {
        for key in keys.sequence_values::<String>() {
            let key = key?;
            submit_keys.push(parse_event_key(&key).map_err(mlua::Error::runtime)?);
        }
    }
    let notify_initial = opts
        .get::<Option<bool>>("notify_initial")
        .ok()
        .flatten()
        .unwrap_or(false);
    let timeout = match opts.get::<Option<i64>>("timeout_ms").ok().flatten() {
        Some(ms) if ms > 0 => Some(Duration::from_millis(ms as u64)),
        // Zero means no timeout: a zero-length idle window would re-fire
        // on_timeout on every event-loop turn.
        Some(0) => None,
        Some(_) => return Err(mlua::Error::runtime(TIMEOUT_MUST_BE_NON_NEGATIVE)),
        None => None,
    };
    Ok(PickerConfig {
        title,
        footer: (!footer.is_empty()).then_some(footer),
        cursor,
        submit_keys,
        notify_initial,
        timeout,
    })
}

/// The `on_change`/`on_timeout` callbacks for a dialog.
pub(crate) fn decode_picker_callbacks(
    opts: Option<&Table>,
) -> (Option<Function>, Option<Function>) {
    let (on_change, on_timeout) = (
        opts.and_then(|o| o.get::<Option<Function>>("on_change").ok().flatten()),
        opts.and_then(|o| o.get::<Option<Function>>("on_timeout").ok().flatten()),
    );
    (on_change, on_timeout)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crossterm::event::{KeyCode, KeyModifiers};
    use mlua::Lua;
    use test_case::test_case;

    fn eval(lua: &Lua, source: &str) -> Value {
        lua.load(source).eval().unwrap()
    }

    fn eval_table(lua: &Lua, source: &str) -> Table {
        match eval(lua, source) {
            Value::Table(t) => t,
            other => panic!("expected a table, got {other:?}"),
        }
    }

    #[test]
    fn decode_picker_items_string_form() {
        let lua = Lua::new();
        let value = eval(&lua, r#"{ "alpha", "beta" }"#);
        let (table, specs) = decode_picker_items(&value).unwrap();
        assert_eq!(table.len().unwrap(), 2);
        assert_eq!(
            specs,
            vec![
                PickerItemSpec {
                    label: "alpha".into(),
                    ..Default::default()
                },
                PickerItemSpec {
                    label: "beta".into(),
                    ..Default::default()
                },
            ]
        );
    }

    #[test]
    fn decode_picker_items_table_form() {
        let lua = Lua::new();
        let value = eval(
            &lua,
            r#"{ { label = "x", detail = "d", section = "s", section_detail = "sd", age = { secs = 1700000000, nanosecs = 400000000 } }, { label = "y" } }"#,
        );
        let (_, specs) = decode_picker_items(&value).unwrap();
        assert_eq!(
            specs,
            vec![
                PickerItemSpec {
                    label: "x".into(),
                    detail: Some("d".into()),
                    section: Some("s".into()),
                    section_detail: Some("sd".into()),
                    age: Some(Duration::new(1_700_000_000, 400_000_000)),
                },
                PickerItemSpec {
                    label: "y".into(),
                    ..Default::default()
                },
            ]
        );
    }

    #[test_case(r#"{ { label = "x", age = "soon" } }"# ; "non_table_age")]
    #[test_case(r#"{ { label = "x", age = { secs = -1, nanosecs = 0 } } }"# ; "negative_secs")]
    #[test_case(r#"{ { label = "x", age = { secs = 1.5, nanosecs = 0 } } }"# ; "fractional_secs")]
    #[test_case(r#"{ { label = "x", age = { secs = 1, nanosecs = 1000000000 } } }"# ; "nanosecs_out_of_range")]
    #[test_case(r#"{ { label = "x", age = { secs = 1 } } }"# ; "missing_nanosecs")]
    fn picker_rejects_malformed_age(source: &str) {
        let lua = Lua::new();
        assert!(decode_picker_items(&eval(&lua, source)).is_err());
    }

    #[test_case(r#"42"# ; "non_table_items")]
    #[test_case(r#"{ { detail = "d" } }"# ; "missing_label")]
    #[test_case(r#"{ 42 }"# ; "non_string_item")]
    #[test_case(r#"{ { label = 42 } }"# ; "non_string_label")]
    #[test_case(r#"{ { label = "x", detail = 42 } }"# ; "non_string_detail")]
    #[test_case(r#"{ { label = "x", section = 42 } }"# ; "non_string_section")]
    #[test_case(r#"{ { label = "x", section_detail = 42 } }"# ; "non_string_section_detail")]
    fn decode_picker_items_bad_shape_errors(source: &str) {
        let lua = Lua::new();
        let value = eval(&lua, source);
        assert!(decode_picker_items(&value).is_err());
    }

    #[test]
    fn decode_picker_opts_footer_key_label_pairs() {
        let lua = Lua::new();
        let opts = eval_table(
            &lua,
            r#"{ footer = { { "Enter", "open" }, { "Ctrl+O", "edit" }, { "Ctrl+D", "delete" } } }"#,
        );
        let config = decode_picker_opts(Some(&opts)).unwrap();
        assert_eq!(
            config.footer,
            Some(vec![
                ("Enter".into(), "open".into()),
                ("Ctrl+O".into(), "edit".into()),
                ("Ctrl+D".into(), "delete".into()),
            ])
        );

        let none = eval_table(&lua, "{}");
        assert_eq!(decode_picker_opts(Some(&none)).unwrap().footer, None);
    }

    #[test_case(1usize, 0usize ; "cursor_one_is_zero_based")]
    #[test_case(3usize, 2usize ; "cursor_three_is_two_based")]
    fn decode_picker_opts_cursor(cursor: usize, expected: usize) {
        let lua = Lua::new();
        let opts = eval_table(&lua, &format!(r#"{{ cursor = {cursor} }}"#));
        assert_eq!(
            decode_picker_opts(Some(&opts)).unwrap().cursor,
            Some(expected)
        );
    }

    #[test_case(0i64 ; "zero_rejected")]
    #[test_case(-1i64 ; "negative_rejected")]
    fn decode_picker_opts_bad_cursor(cursor: i64) {
        let lua = Lua::new();
        let opts = eval_table(&lua, &format!(r#"{{ cursor = {cursor} }}"#));
        assert!(decode_picker_opts(Some(&opts)).is_err());
    }

    #[test]
    fn decode_picker_opts_submit_keys() {
        let lua = Lua::new();
        let opts = eval_table(&lua, r#"{ submit_keys = { "ctrl+o", "enter" } }"#);
        let config = decode_picker_opts(Some(&opts)).unwrap();
        assert_eq!(
            config.submit_keys,
            vec![
                KeyEvent::new(KeyCode::Char('o'), KeyModifiers::CONTROL),
                KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE),
            ]
        );
    }

    #[test]
    fn decode_picker_opts_bad_submit_key_errors() {
        let lua = Lua::new();
        let opts = eval_table(&lua, r#"{ submit_keys = { "ctrl+o", "bogus" } }"#);
        assert!(decode_picker_opts(Some(&opts)).is_err());
    }

    #[test_case(1234i64, 1234u64 ; "timeout_set")]
    #[test_case(0i64, 0 ; "zero_timeout_disables_the_window")]
    fn decode_picker_opts_timeout(ms: i64, expected_ms: u64) {
        let lua = Lua::new();
        let opts = eval_table(&lua, &format!(r#"{{ timeout_ms = {ms} }}"#));
        let expected = (expected_ms > 0).then(|| Duration::from_millis(expected_ms));
        assert_eq!(decode_picker_opts(Some(&opts)).unwrap().timeout, expected);
    }

    #[test]
    fn decode_picker_opts_negative_timeout_errors() {
        let lua = Lua::new();
        let opts = eval_table(&lua, r#"{ timeout_ms = -1 }"#);
        assert!(decode_picker_opts(Some(&opts)).is_err());
    }

    #[test]
    fn decode_picker_opts_notify_initial_and_title() {
        let lua = Lua::new();
        let opts = eval_table(&lua, r#"{ title = "Pick", notify_initial = true }"#);
        let config = decode_picker_opts(Some(&opts)).unwrap();
        assert_eq!(config.title.as_deref(), Some("Pick"));
        assert!(config.notify_initial);
        assert!(!decode_picker_opts(None).unwrap().notify_initial);
    }

    #[test_case(r#"{ title = 42 }"# ; "number_title")]
    #[test_case(r#"{ title = {} }"# ; "table_title")]
    fn decode_picker_opts_bad_title_errors(source: &str) {
        let lua = Lua::new();
        let opts = eval_table(&lua, source);
        assert!(
            decode_picker_opts(Some(&opts)).is_err(),
            "{source} must not decode"
        );
    }

    #[test]
    fn decode_picker_callbacks_only_from_opts() {
        let lua = Lua::new();
        let opts = eval_table(
            &lua,
            r#"{ on_change = function(item, index) end, on_timeout = function() end }"#,
        );
        let (on_change, on_timeout) = decode_picker_callbacks(Some(&opts));
        assert!(on_change.is_some());
        assert!(on_timeout.is_some());
        assert_eq!(decode_picker_callbacks(None), (None, None));
    }
}
