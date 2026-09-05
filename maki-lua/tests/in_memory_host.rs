//! Hermetic real-host tests: a full `PluginHost` (background Lua thread,
//! real builtins) booted on the in-memory FS backend. The `TEST_STATE_DIR`
//! assertions prove the whole host ran disk-free: nothing may create that
//! path on the real filesystem.

use std::sync::Arc;
use std::time::{Duration, Instant};

use maki_agent::AgentMode;
use maki_agent::cancel::CancelToken;
use maki_agent::tools::ToolRegistry;
use maki_agent::tools::test_support::stub_ctx;
use maki_lua::test_support::InMemoryFs;
use maki_lua::{CommandArgumentContext, CommandArgumentItem, CommandArgumentLifecycle};

const NOTE_PATH: &str = "note.md";
const NOTE_BODY: &str = "hello-maki";
const WROTE_PREFIX: &str = "wrote note.md";
const PROJECTS_PREFIX: &str = "/maki-test-state/projects/";
const SPLASH_DEADLINE: Duration = Duration::from_secs(30);
const SPLASH_BACKOFF: Duration = Duration::from_millis(100);

fn exec(reg: &Arc<ToolRegistry>, tool: &str, input: serde_json::Value) -> String {
    let mut ctx = stub_ctx(&AgentMode::Build);
    ctx.registry = Arc::clone(reg);
    let inv = reg
        .get(tool)
        .unwrap_or_else(|| panic!("tool {tool} not registered"))
        .tool
        .parse(&input)
        .expect("parse failed");
    let result = smol::block_on(inv.execute(&ctx));
    match result.output.expect("tool output") {
        maki_agent::ToolOutput::Plain(s) | maki_agent::ToolOutput::Markdown(s) => s.text,
        other => panic!("unexpected output: {other:?}"),
    }
}

#[test]
fn memory_write_read_roundtrip_stays_in_memory() {
    let state = std::path::Path::new(maki_lua::test_support::TEST_STATE_DIR);
    assert!(
        !state.exists(),
        "precondition: TEST_STATE_DIR must not exist on disk"
    );

    let (_handle, guard) = maki_lua::test_support::spawn_host_for_tests(&["memory"]);
    let reg = guard.host().registry();

    let wrote = exec(
        &reg,
        "memory",
        serde_json::json!({
            "command": "write",
            "path": NOTE_PATH,
            "content": NOTE_BODY,
            "tags": ["t"],
        }),
    );
    assert!(wrote.contains(WROTE_PREFIX), "got: {wrote}");

    let read = exec(
        &reg,
        "memory",
        serde_json::json!({ "command": "read", "path": NOTE_PATH }),
    );
    assert!(read.contains(NOTE_BODY), "got: {read}");

    // The note landed in the in-memory backend, not on disk.
    let notes: Vec<_> = guard
        .backend()
        .files()
        .into_iter()
        .filter(|(p, _)| p.to_string_lossy().starts_with(PROJECTS_PREFIX))
        .collect();
    assert_eq!(notes.len(), 1, "exactly one note file in the backend");
    let (path, bytes) = &notes[0];
    let content = String::from_utf8(bytes.clone()).unwrap();
    assert!(
        path.file_name().is_some_and(|n| n == NOTE_PATH),
        "unexpected note path: {path:?}"
    );
    assert!(
        content.starts_with("---\n"),
        "frontmatter missing: {content}"
    );
    assert!(content.contains(NOTE_BODY), "body missing: {content}");

    assert!(
        !state.exists(),
        "the round-trip must not touch the real disk"
    );
}

#[test]
fn splash_host_boots_disk_free() {
    let state = std::path::Path::new(maki_lua::test_support::TEST_STATE_DIR);
    assert!(
        !state.exists(),
        "precondition: TEST_STATE_DIR must not exist on disk"
    );

    let (handle, _guard) = maki_lua::test_support::spawn_host_for_tests(&["splashes_default"]);
    let start = Instant::now();
    let frame = loop {
        if let Some(frame) = handle.splash_frame(80, 20, 10.0, 1.0) {
            break frame;
        }
        assert!(
            start.elapsed() < SPLASH_DEADLINE,
            "splash frame never arrived"
        );
        std::thread::sleep(SPLASH_BACKOFF);
    };
    assert!(!frame.is_empty(), "rows must be non-empty");

    assert!(
        !state.exists(),
        "the frame pull must not touch the real disk"
    );
}

// ---- splashes picker lifecycle (in-memory state) ----

const SELECTION_REL: &str = "splashes/selection.json";
const SELECTION_STALE: &str = r#"{"name":"vortex"}"#;
const SELECTION_MATRIX_JSON: &str = r#"{"name":"matrix"}"#;
const SELECTION_INVALID: &[u8] = &[0xff, 0xfe, 0x00];
const SELECTION_DEFAULT: &str = "default";
const SELECTION_MATRIX: &str = "matrix";
const SELECTION_VORTEX: &str = "vortex";

const WRAPPER_SOURCE: &str = r##"
maki.api.set_slot("splash.render", function(prev, w, h, t, fade)
  local rows = {}
  for i = 1, h do
    rows[i] = { { glyphs = string.rep("w", w), style = "#00ff41" } }
  end
  return rows
end)
"##;

const CONTRIBUTION_SOURCE: &str = r##"
local M = {}
function M.render(w, h, t, fade)
  local rows = {}
  for i = 1, h do
    rows[i] = { { glyphs = string.rep("s", w), style = "#00ff41" } }
  end
  return rows
end

maki.store.register("splash", "vortex", {
  label = "vortex",
  description = "test splash",
  renderer = M.render,
})
"##;

fn selection_path() -> std::path::PathBuf {
    std::path::Path::new(maki_lua::test_support::TEST_STATE_DIR).join(SELECTION_REL)
}

fn selection_content(guard: &maki_lua::test_support::PluginHostGuard) -> Option<String> {
    let path = selection_path();
    guard
        .backend()
        .files()
        .into_iter()
        .find(|(p, _)| p.as_path() == path.as_path())
        .map(|(_, bytes)| String::from_utf8(bytes).unwrap())
}

fn wait_for_selection(guard: &maki_lua::test_support::PluginHostGuard, needle: &str) -> String {
    let deadline = Instant::now() + SPLASH_DEADLINE;
    loop {
        if let Some(content) = selection_content(guard)
            && content.contains(needle)
        {
            return content;
        }
        assert!(
            Instant::now() < deadline,
            "selection file never contained '{needle}'"
        );
        std::thread::sleep(SPLASH_BACKOFF);
    }
}

fn pull_frame(handle: &maki_lua::EventHandle, needle: Option<&str>) -> maki_lua::SplashFrame {
    let deadline = Instant::now() + SPLASH_DEADLINE;
    loop {
        if let Some(frame) = handle.splash_frame(80, 20, 10.0, 1.0) {
            let all: String = frame.rows.iter().map(|r| r.glyphs.as_str()).collect();
            if needle.is_none_or(|n| all.contains(n)) {
                return frame;
            }
        }
        assert!(
            Instant::now() < deadline,
            "splash frame never matched {needle:?}"
        );
        std::thread::sleep(SPLASH_BACKOFF);
    }
}

fn frame_text(frame: &maki_lua::SplashFrame) -> String {
    frame.rows.iter().map(|r| r.glyphs.as_str()).collect()
}

fn lifecycle_ctx() -> CommandArgumentContext {
    CommandArgumentContext {
        command: Arc::from("/splash"),
        plugin: Arc::from("splashes"),
        args: SELECTION_VORTEX.into(),
        arg: SELECTION_VORTEX.into(),
        index: 1,
        mode: "build".into(),
        session: 1,
        generation: 1,
    }
}

fn lifecycle_item() -> CommandArgumentItem {
    CommandArgumentItem {
        label: SELECTION_VORTEX.into(),
        insertion: SELECTION_VORTEX.into(),
        description: None,
    }
}

#[test]
fn splash_picker_repairs_unknown_selection() {
    let state = std::path::Path::new(maki_lua::test_support::TEST_STATE_DIR);
    assert!(
        !state.exists(),
        "precondition: TEST_STATE_DIR must not exist on disk"
    );

    let fs = Arc::new(InMemoryFs::new());
    fs.seed(&selection_path(), SELECTION_STALE.as_bytes().to_vec());
    let (handle, guard) = maki_lua::test_support::spawn_host_with_fs_for_tests(
        &["splashes", "splashes_default"],
        Arc::clone(&fs),
        None,
    );

    // The first frame rolls the stale selection back to the fallback and
    // serves it, then the repair task rewrites the file.
    let frame = pull_frame(&handle, Some("makima"));
    assert!(!frame.rows.is_empty(), "fallback frame has rows");
    let content = wait_for_selection(&guard, SELECTION_DEFAULT);
    assert!(content.contains("name"), "repaired file: {content}");
    assert!(!state.exists(), "the repair must not touch the real disk");
}

#[test]
fn splash_picker_repairs_malformed_selection() {
    let fs = Arc::new(InMemoryFs::new());
    // Non-UTF8: the load-time read throws and must be treated as invalid.
    fs.seed(&selection_path(), SELECTION_INVALID.to_vec());
    let (handle, guard) = maki_lua::test_support::spawn_host_with_fs_for_tests(
        &["splashes", "splashes_default"],
        Arc::clone(&fs),
        None,
    );

    let frame = pull_frame(&handle, Some("makima"));
    assert!(!frame.rows.is_empty());
    let content = wait_for_selection(&guard, SELECTION_DEFAULT);
    assert!(content.contains("name"), "repaired file: {content}");
}

#[test]
fn splash_picker_default_runs_chain_below() {
    // The user layer loads before the builtins, so its slot layer sits below
    // the picker's: the picker must delegate the committed default to the
    // chain, or the frame would show the starfield instead.
    let fs = Arc::new(InMemoryFs::new());
    let (handle, _guard) = maki_lua::test_support::spawn_host_with_fs_for_tests(
        &["splashes", "splashes_default"],
        fs,
        Some(WRAPPER_SOURCE),
    );

    let frame = pull_frame(&handle, Some("www"));
    assert!(
        !frame_text(&frame).contains("makima"),
        "picker must not bypass the chain below it"
    );
}

#[test]
fn splash_picker_renders_user_contribution() {
    let fs = Arc::new(InMemoryFs::new());
    fs.seed(&selection_path(), SELECTION_STALE.as_bytes().to_vec());
    let (handle, guard) = maki_lua::test_support::spawn_host_with_fs_for_tests(
        &["splashes", "splashes_default"],
        Arc::clone(&fs),
        Some(CONTRIBUTION_SOURCE),
    );

    let frame = pull_frame(&handle, Some("sss"));
    assert!(
        !frame_text(&frame).contains("makima"),
        "the contributed splash draws the screen"
    );
    assert!(
        selection_content(&guard).unwrap().contains("vortex"),
        "a valid contribution must not roll back"
    );
}

#[test]
fn splash_picker_command_persists_selection() {
    let fs = Arc::new(InMemoryFs::new());
    let (handle, guard) = maki_lua::test_support::spawn_host_with_fs_for_tests(
        &["splashes", "splashes_default"],
        Arc::clone(&fs),
        None,
    );

    handle.run_command(
        Arc::from("splashes"),
        Arc::from("/splash"),
        "matrix".into(),
        0,
    );
    let content = wait_for_selection(&guard, SELECTION_MATRIX);
    assert!(content.contains("name"), "persisted file: {content}");

    let frame = pull_frame(&handle, None);
    assert!(!frame.rows.is_empty(), "committed splash serves frames");
    assert!(
        selection_content(&guard)
            .unwrap()
            .contains(SELECTION_MATRIX),
        "selection must survive the first frame"
    );
}

#[test]
fn splash_picker_accept_survives_late_highlight() {
    let state = std::path::Path::new(maki_lua::test_support::TEST_STATE_DIR);
    assert!(
        !state.exists(),
        "precondition: TEST_STATE_DIR must not exist on disk"
    );

    let fs = Arc::new(InMemoryFs::new());
    fs.seed(&selection_path(), SELECTION_MATRIX_JSON.as_bytes().to_vec());
    let (handle, guard) = maki_lua::test_support::spawn_host_with_fs_for_tests(
        &["splashes", "splashes_default"],
        Arc::clone(&fs),
        Some(CONTRIBUTION_SOURCE),
    );

    // The seeded matrix serves the first frame, so a frame pull can be in
    // flight when the lifecycle events arrive.
    let frame = pull_frame(&handle, None);
    assert!(
        !frame.rows.is_empty(),
        "seeded splash serves the first frame"
    );

    // Enter in the popup submits an Accept; the next keystroke re-triggers
    // completion and submits a Highlight for the new popup session while the
    // Accept is still in flight. The late Highlight must not coalesce the
    // Accept away, or the selection save is silently lost.
    handle.command_argument_lifecycle(
        lifecycle_ctx(),
        CommandArgumentLifecycle::Accept,
        Some(lifecycle_item()),
        CancelToken::none(),
    );
    handle.command_argument_lifecycle(
        lifecycle_ctx(),
        CommandArgumentLifecycle::Highlight,
        Some(lifecycle_item()),
        CancelToken::none(),
    );

    let content = wait_for_selection(&guard, SELECTION_VORTEX);
    assert!(content.contains("name"), "persisted file: {content}");
    assert!(!state.exists(), "the save must not touch the real disk");
}

#[test]
fn splash_picker_command_switches_to_default() {
    let fs = Arc::new(InMemoryFs::new());
    fs.seed(&selection_path(), SELECTION_MATRIX_JSON.as_bytes().to_vec());
    let (handle, guard) = maki_lua::test_support::spawn_host_with_fs_for_tests(
        &["splashes", "splashes_default"],
        Arc::clone(&fs),
        None,
    );

    // Negative marker: the rain alphabet (A-Z, 0-9) and the corner version
    // text cannot spell the starfield logo.
    let frame = pull_frame(&handle, None);
    assert!(
        !frame_text(&frame).contains("makima"),
        "the committed matrix splash serves frames before the switch"
    );

    handle.run_command(
        Arc::from("splashes"),
        Arc::from("/splash"),
        "default".into(),
        0,
    );

    // The fallback path must serve the starfield through the chain below the
    // picker; a failing delegation rolls the selection back to matrix.
    let frame = pull_frame(&handle, Some("makima"));
    assert!(!frame.rows.is_empty(), "the default splash serves frames");
    let content = wait_for_selection(&guard, SELECTION_DEFAULT);
    assert!(
        !content.contains(SELECTION_MATRIX),
        "the switch must not roll the file back to matrix: {content}"
    );
}

const SECOND_SOURCE: &str = r##"
local M = {}
function M.render(w, h, t, fade)
  local rows = {}
  for i = 1, h do
    rows[i] = { { glyphs = string.rep("o", w), style = "#00ff41" } }
  end
  return rows
end

maki.store.register("splash", "other", {
  label = "other",
  description = "test splash",
  renderer = M.render,
})
"##;

const RELOAD_SOURCE: &str = r##"
local M = {}
function M.render(w, h, t, fade)
  local rows = {}
  for i = 1, h do
    rows[i] = { { glyphs = string.rep("n", w), style = "#00ff41" } }
  end
  return rows
end

maki.store.register("splash", "vortex", {
  label = "vortex",
  description = "test splash",
  renderer = M.render,
})
"##;

#[test]
fn splash_picker_rollback_serves_reloaded_contribution() {
    let fs = Arc::new(InMemoryFs::new());
    fs.seed(&selection_path(), SELECTION_STALE.as_bytes().to_vec());
    let (handle, guard) = maki_lua::test_support::spawn_host_with_fs_for_tests(
        &["splashes", "splashes_default"],
        Arc::clone(&fs),
        Some(CONTRIBUTION_SOURCE),
    );

    // Load the second plugin before the first frame so the commit runs
    // undirty: the previous selection must keep its cached renderer until
    // the registry changes again.
    guard.host().load_source("second", SECOND_SOURCE).unwrap();
    let frame = pull_frame(&handle, Some("sss"));
    assert!(
        !frame.rows.is_empty(),
        "the committed vortex splash serves before the switch"
    );
    handle.run_command(
        Arc::from("splashes"),
        Arc::from("/splash"),
        "other".into(),
        0,
    );
    let content = wait_for_selection(&guard, "other");
    assert!(content.contains("name"), "persisted file: {content}");

    // Reload vortex with a fresh closure ("n") and drop the committed
    // splash: the rollback must re-resolve the previous selection, not serve
    // the first load's cached renderer.
    guard.host().unload("user_init").unwrap();
    guard
        .host()
        .load_source("user_init", RELOAD_SOURCE)
        .unwrap();
    guard.host().unload("second").unwrap();

    let frame = pull_frame(&handle, Some("nnn"));
    assert!(!frame.rows.is_empty(), "the reloaded vortex splash serves");
    let content = wait_for_selection(&guard, SELECTION_STALE);
    assert!(content.contains("name"), "repaired file: {content}");
}

const LATE_SOURCE: &str = r##"
local M = {}
function M.render(w, h, t, fade)
  local rows = {}
  for i = 1, h do
    rows[i] = { { glyphs = string.rep("l", w), style = "#00ff41" } }
  end
  return rows
end

maki.store.register("splash", "late", {
  label = "late",
  description = "test splash",
  renderer = M.render,
})
"##;

#[test]
fn splash_picker_sees_contribution_loaded_after_picker() {
    let fs = Arc::new(InMemoryFs::new());
    let (handle, guard) = maki_lua::test_support::spawn_host_with_fs_for_tests(
        &["splashes", "splashes_default"],
        fs,
        None,
    );

    // Loaded after the picker booted: the picker learns about it from the
    // StoreChanged event, not from polling.
    guard.host().load_source("late", LATE_SOURCE).unwrap();

    handle.run_command(
        Arc::from("splashes"),
        Arc::from("/splash"),
        "late".into(),
        0,
    );
    let content = wait_for_selection(&guard, "late");
    assert!(content.contains("name"), "persisted file: {content}");
    let frame = pull_frame(&handle, Some("lll"));
    assert!(
        !frame.rows.is_empty(),
        "the late contribution serves frames"
    );
}

#[test]
fn splash_picker_re_resolves_after_contribution_unloads() {
    let fs = Arc::new(InMemoryFs::new());
    fs.seed(&selection_path(), SELECTION_STALE.as_bytes().to_vec());
    let (handle, guard) = maki_lua::test_support::spawn_host_with_fs_for_tests(
        &["splashes", "splashes_default"],
        Arc::clone(&fs),
        Some(CONTRIBUTION_SOURCE),
    );

    // The seeded selection is the user contribution; the first frame serves it.
    let frame = pull_frame(&handle, Some("sss"));
    assert!(
        !frame_text(&frame).contains("makima"),
        "the committed contribution draws"
    );

    guard.host().unload("user_init").unwrap();

    // The picker must drop the stale renderer, find the name unknown, roll
    // back to the fallback, and repair the persisted selection.
    let frame = pull_frame(&handle, Some("makima"));
    assert!(!frame.rows.is_empty(), "fallback frame after unload");
    let content = wait_for_selection(&guard, SELECTION_DEFAULT);
    assert!(content.contains("name"), "repaired file: {content}");
}

fn splash_opts(
    splash: &str,
) -> std::collections::HashMap<String, serde_json::Map<String, serde_json::Value>> {
    let mut opts = std::collections::HashMap::new();
    opts.insert(
        "splashes".to_string(),
        serde_json::json!({ "splash": splash })
            .as_object()
            .unwrap()
            .clone(),
    );
    opts
}

#[test]
fn splash_picker_startup_option_selects_load_time_contribution() {
    let fs = Arc::new(InMemoryFs::new());
    let (handle, guard) = maki_lua::test_support::spawn_host_with_fs_and_opts_for_tests(
        &["splashes", "splashes_default"],
        Arc::clone(&fs),
        Some(CONTRIBUTION_SOURCE),
        splash_opts("vortex"),
    );

    let frame = pull_frame(&handle, Some("sss"));
    assert!(
        !frame.rows.is_empty(),
        "the load-time contribution serves before the first frame"
    );
    let content = wait_for_selection(&guard, "vortex");
    assert!(content.contains("name"), "persisted file: {content}");
}

#[test]
fn splash_picker_startup_option_typo_keeps_fallback() {
    let fs = Arc::new(InMemoryFs::new());
    let (handle, guard) = maki_lua::test_support::spawn_host_with_fs_and_opts_for_tests(
        &["splashes", "splashes_default"],
        Arc::clone(&fs),
        None,
        splash_opts("nope"),
    );

    let frame = pull_frame(&handle, Some("makima"));
    assert!(!frame.rows.is_empty(), "the fallback splash serves frames");
    assert!(
        selection_content(&guard).is_none(),
        "an unresolvable startup selection must not persist a file"
    );
}

#[test]
fn splash_picker_splash_select_commits_and_persists() {
    let fs = Arc::new(InMemoryFs::new());
    let (handle, guard) = maki_lua::test_support::spawn_host_with_fs_for_tests(
        &["splashes", "splashes_default"],
        Arc::clone(&fs),
        None,
    );

    handle.fire_autocmd("SplashSelect", serde_json::json!({ "name": "matrix" }));
    let content = wait_for_selection(&guard, SELECTION_MATRIX);
    assert!(content.contains("name"), "persisted file: {content}");
    let frame = pull_frame(&handle, None);
    assert!(
        !frame_text(&frame).contains("makima"),
        "the selected splash serves frames"
    );
}

#[test]
fn splash_picker_splash_select_transient_keeps_file() {
    let fs = Arc::new(InMemoryFs::new());
    fs.seed(&selection_path(), SELECTION_MATRIX_JSON.as_bytes().to_vec());
    let (handle, guard) = maki_lua::test_support::spawn_host_with_fs_for_tests(
        &["splashes", "splashes_default"],
        Arc::clone(&fs),
        None,
    );

    // matrix (persisted) is committed from the seeded file.
    let frame = pull_frame(&handle, None);
    assert!(
        !frame_text(&frame).contains("makima"),
        "the seeded selection draws before the switch"
    );

    handle.fire_autocmd(
        "SplashSelect",
        serde_json::json!({ "name": "voronoi", "persist": false }),
    );
    let frame = pull_frame(&handle, None);
    assert!(
        !frame_text(&frame).contains("makima"),
        "the transient selection serves frames"
    );
    let content = wait_for_selection(&guard, SELECTION_MATRIX);
    assert!(
        content.contains(SELECTION_MATRIX),
        "the transient switch must not rewrite the file: {content}"
    );
}

#[test]
fn splash_picker_splash_select_rejects_bad_data() {
    let fs = Arc::new(InMemoryFs::new());
    let (handle, guard) = maki_lua::test_support::spawn_host_with_fs_for_tests(
        &["splashes", "splashes_default"],
        Arc::clone(&fs),
        None,
    );

    handle.fire_autocmd("SplashSelect", serde_json::json!({}));
    let frame = pull_frame(&handle, Some("makima"));
    assert!(!frame.rows.is_empty(), "the fallback splash serves frames");
    assert!(
        selection_content(&guard).is_none(),
        "bad data must not persist a selection"
    );
}

const TIMER_TICKS_REL: &str = "timer_ticks";
const TIMER_FOREVER_REL: &str = "timer_forever";
const TIMER_QUIESCENCE: Duration = Duration::from_millis(300);

const TIMER_SELF_STOP_SOURCE: &str = r##"
local base = maki.env.state_dir()
assert(base, "state dir missing in test host")
local ok, err = maki.fs.mkdir(base, { parents = true })
assert(ok, "mkdir failed: " .. tostring(err))
local path = maki.fs.joinpath(base, "timer_ticks")
local n = 0
maki.timer.set(0.05, function(id)
  n = n + 1
  if n >= 3 then
    maki.timer.del(id)
  end
  local ok, err = maki.fs.atomic_write(path, tostring(n))
  assert(ok, "timer tick write failed: " .. tostring(err))
end)
"##;

const TIMER_FOREVER_SOURCE: &str = r##"
local base = maki.env.state_dir()
local ok, err = maki.fs.mkdir(base, { parents = true })
assert(ok, "mkdir failed: " .. tostring(err))
local path = maki.fs.joinpath(base, "timer_forever")
local n = 0
maki.timer.set(0.05, function()
  n = n + 1
  local ok, err = maki.fs.atomic_write(path, tostring(n))
  assert(ok, "timer tick write failed: " .. tostring(err))
end)
"##;

const TIMER_CONFIG_SOURCE: &str = r##"
local base = maki.env.state_dir()
local ok, err = maki.fs.mkdir(base, { parents = true })
assert(ok, "mkdir failed: " .. tostring(err))
local path = maki.fs.joinpath(base, "timer_ticks")
local n = 0
maki.timer.set(0.05, function()
  n = n + 1
  local ok, err = maki.fs.atomic_write(path, tostring(n))
  assert(ok, "timer tick write failed: " .. tostring(err))
end)
"##;

fn ticks_file(guard: &maki_lua::test_support::PluginHostGuard, rel: &str) -> Option<u64> {
    let path = std::path::Path::new(maki_lua::test_support::TEST_STATE_DIR).join(rel);
    guard
        .backend()
        .files()
        .into_iter()
        .find(|(p, _)| p.as_path() == path.as_path())
        .and_then(|(_, bytes)| String::from_utf8(bytes).ok()?.parse().ok())
}

fn wait_for_ticks(guard: &maki_lua::test_support::PluginHostGuard, rel: &str, n: u64) {
    let deadline = Instant::now() + SPLASH_DEADLINE;
    loop {
        if ticks_file(guard, rel).is_some_and(|v| v >= n) {
            return;
        }
        assert!(Instant::now() < deadline, "timer never reached tick {n}");
        std::thread::sleep(SPLASH_BACKOFF);
    }
}

#[test]
fn timer_fires_repeatedly_until_deleted() {
    let fs = Arc::new(InMemoryFs::new());
    let (_handle, guard) =
        maki_lua::test_support::spawn_host_with_fs_for_tests(&[], Arc::clone(&fs), None);

    guard
        .host()
        .load_source("tick", TIMER_SELF_STOP_SOURCE)
        .unwrap();
    wait_for_ticks(&guard, TIMER_TICKS_REL, 3);
    std::thread::sleep(TIMER_QUIESCENCE);
    assert_eq!(
        ticks_file(&guard, TIMER_TICKS_REL),
        Some(3),
        "no fires after del"
    );
}

#[test]
fn timer_unload_drops_schedules() {
    let fs = Arc::new(InMemoryFs::new());
    let (_handle, guard) =
        maki_lua::test_support::spawn_host_with_fs_for_tests(&[], Arc::clone(&fs), None);

    guard
        .host()
        .load_source("tick", TIMER_FOREVER_SOURCE)
        .unwrap();
    wait_for_ticks(&guard, TIMER_FOREVER_REL, 2);
    guard.host().unload("tick").unwrap();

    let frozen = ticks_file(&guard, TIMER_FOREVER_REL).unwrap();
    std::thread::sleep(TIMER_QUIESCENCE);
    assert_eq!(
        ticks_file(&guard, TIMER_FOREVER_REL),
        Some(frozen),
        "unload must drop the schedule"
    );
}

#[test]
fn timer_registered_from_config_does_not_block_boot() {
    let fs = Arc::new(InMemoryFs::new());
    let (_handle, guard) = maki_lua::test_support::spawn_host_with_fs_for_tests(
        &[],
        Arc::clone(&fs),
        Some(TIMER_CONFIG_SOURCE),
    );

    // Boot (config + every builtin) has already returned: a timer registered
    // during config load must not stall the load barrier, and it must fire
    // on the pump once the loop is running.
    wait_for_ticks(&guard, TIMER_TICKS_REL, 1);
}

const SPLASH_FPS_PREFIX: &str = "splash ";

fn hint_text(reader: &maki_lua::HintReader) -> Option<String> {
    reader
        .load_full()
        .entries
        .iter()
        .flat_map(|(_, pairs)| pairs.iter().map(|(text, _)| text.as_str()))
        .find(|text| text.starts_with(SPLASH_FPS_PREFIX))
        .map(str::to_owned)
}

fn wait_for_hint(reader: &maki_lua::HintReader, want: bool) -> Option<String> {
    let start = Instant::now();
    loop {
        let text = hint_text(reader);
        if text.is_some() == want {
            return text;
        }
        assert!(
            start.elapsed() < SPLASH_DEADLINE,
            "fps readout never {}",
            if want { "appeared" } else { "cleared" }
        );
        std::thread::sleep(SPLASH_BACKOFF);
    }
}

#[test]
fn splash_fps_overlay_toggles_through_store_and_readout() {
    let (handle, guard) =
        maki_lua::test_support::spawn_host_for_tests(&["perf", "splashes_default"]);
    let hints = guard.host().hint_reader();
    assert_eq!(hint_text(&hints), None, "overlay starts off");

    // Drive real renders through the host before toggling: timings must see
    // them, or the readout would show a dead pipeline (regression: PerfInfo
    // was never seeded, so fps stayed 0 no matter what the splash did).
    for _ in 0..3 {
        let start = Instant::now();
        loop {
            if handle.splash_frame(80, 20, 10.0, 1.0).is_some() {
                break;
            }
            assert!(
                start.elapsed() < SPLASH_DEADLINE,
                "blocking pull must render"
            );
            std::thread::sleep(SPLASH_BACKOFF);
        }
    }
    guard
        .host()
        .load_source(
            "perf-probe",
            r#"local t = maki.perf.timings()
               assert(t.fps >= 1, "host renders land in the fps window")"#,
        )
        .expect("timings reflect real renders");

    let toggle = || {
        handle.run_command(
            Arc::from("perf"),
            Arc::from("/splash-fps"),
            String::new(),
            0,
        );
    };

    toggle();
    let text = wait_for_hint(&hints, true).expect("readout after enable");
    assert!(text.contains("fps"), "readout text: {text}");

    let store_state = guard.host().load_source(
        "perf-probe-flag",
        r#"assert(maki.store.collect("perf").fps_overlay == true, "store flag not set")"#,
    );
    store_state.expect("enable flag lands in the store");

    toggle();
    wait_for_hint(&hints, false);
    guard
        .host()
        .load_source(
            "perf-probe-clear",
            r#"assert(maki.store.collect("perf").fps_overlay == false, "store flag not cleared")"#,
        )
        .expect("disable flag lands in the store");
}
