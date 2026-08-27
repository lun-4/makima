//! Real-VM splash meters for the bundled splashes at 200x79 (the tall-terminal
//! case that drags fps), as criterion benches. Replaces the libtest
//! `tests/splash_bench.rs` budget loop; criterion adds warmup, sample
//! statistics, baselines (`--save-baseline`/`--load-baseline`) and HTML
//! reports, and does not run under `just ci` (nextest ignores bench targets),
//! matching the old `#[ignore]`d status of the host meter.
//!
//!   cargo bench -p maki-lua --bench splash_perf
//
// Findings baked into this file:
// - `raw_render_200x79`: luau native codegen makes code ~5x faster than the
//   interpreter (aurora 32.5 -> 5.8 ms).
// - `codegen_watchdog_200x79`: maki's 10ms watchdog interrupt does not disable
//   codegen.
// - `env_codegen_200x79`: codegen does NOT apply to chunks whose env table was
//   never marked safe (`lua_setsafeenv`). maki's build_env now calls
//   `Table::set_safeenv(true)`; env'd + safeenv runs at native speed (4.1ms),
//   the same as a global-env chunk (4.0ms).
// - `pull_roundtrip_200x79`: full `EventHandle::splash_frame` meter (render +
//   mlua conversion + Rust SplashFrame parse). The render runs compiled (the
//   splash pull spends a small codegen budget per frame); remaining cost is
//   the per-segment parse, so segment count is the thing to watch (see
//   MERGE_TOL in the splashes).

mod common;

use std::cell::Cell;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use common::spawn_watchdog;
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use maki_lua::test_support::{InMemoryFs, spawn_host_with_fs_and_opts_for_tests};
use mlua::{Compiler, Function, Lua, MultiValue, Table};

const W: u16 = 200;
const H: u16 = 79;
const SPLASHES: [&str; 5] = ["aurora", "caustics", "kaleidoscope", "metaballs", "voronoi"];
const FRAME_STEP: f64 = 0.033;
const MEMORY_LIMIT: usize = 512 * 1024 * 1024;

fn install_maki_stub(lua: &Lua) {
    let maki = lua.create_table().unwrap();
    let ui = lua.create_table().unwrap();
    ui.set(
        "theme_color",
        lua.create_function(|_, _: ()| Ok(None::<String>)).unwrap(),
    )
    .unwrap();
    maki.set("ui", ui).unwrap();
    let version = lua.create_table().unwrap();
    version.set("current", "0.0.0-test").unwrap();
    maki.set(
        "version",
        lua.create_function(move |_, _: ()| Ok(version.clone()))
            .unwrap(),
    )
    .unwrap();
    let api = lua.create_table().unwrap();
    api.set(
        "set_slot",
        lua.create_function(|_, _: MultiValue| Ok(())).unwrap(),
    )
    .unwrap();
    api.set(
        "create_autocmd",
        lua.create_function(|_, _: MultiValue| Ok(())).unwrap(),
    )
    .unwrap();
    maki.set("api", api).unwrap();
    lua.globals().set("maki", maki).unwrap();
}

fn runtime_lua() -> Lua {
    let lua = Lua::new();
    lua.set_compiler(Compiler::new().set_optimization_level(2));
    lua.set_memory_limit(MEMORY_LIMIT).unwrap();
    lua.sandbox(true).unwrap();
    lua.enable_jit(false);
    install_maki_stub(&lua);
    lua
}

fn splash_src(name: &str) -> String {
    std::fs::read_to_string(format!(
        "{}/../plugins/splashes_default/splash/{name}.lua",
        env!("CARGO_MANIFEST_DIR")
    ))
    .unwrap_or_else(|_| panic!("read {name}"))
}

fn codegen_compile(lua: &Lua, chunk: &Function) {
    unsafe {
        lua.exec_raw::<()>(chunk.clone(), |state| {
            mlua::ffi::luau_codegen_compile(state, -1)
        })
        .expect("codegen");
    }
}

/// Module-style env: a fresh table chaining to the real globals, like maki's
/// build_env. The safeenv-marked twin is what maki now produces (runtime.rs
/// build_env); proving it unlocks codegen for env'd chunks.
fn env_table(lua: &Lua, safe: bool) -> Table {
    let env = lua.create_table().unwrap();
    let mt = lua.create_table().unwrap();
    mt.set("__index", lua.globals()).unwrap();
    env.set_metatable(Some(mt)).unwrap();
    let maki: Table = lua.globals().get("maki").unwrap();
    env.set("maki", maki).unwrap();
    if safe {
        env.set_safeenv(true);
    }
    env
}

struct Prepared {
    render: Function,
    t: Cell<f64>,
}

impl Prepared {
    /// Loads the chunk, runs one warmup frame (needed before `late` codegen),
    /// and returns a meter that advances time per frame like the runtime.
    fn new(lua: &Lua, name: &str, env: Option<&Table>, early: bool) -> Self {
        let chunk = lua
            .load(splash_src(name))
            .set_name(format!("splash.{name}"));
        let chunk = match env {
            Some(e) => chunk.set_environment(e.clone()),
            None => chunk,
        };
        let chunk = chunk.into_function().unwrap();
        if early {
            codegen_compile(lua, &chunk);
        }
        let m: Table = chunk.call(()).unwrap();
        let render: Function = m.get("render").unwrap();
        render
            .call::<Table>((W as i64, H as i64, 1.0f64, 1.0f64))
            .unwrap();
        if !early {
            codegen_compile(lua, &chunk);
        }
        Self {
            render,
            t: Cell::new(1.0),
        }
    }

    fn next_frame(&self) {
        let t = self.t.get() + FRAME_STEP;
        self.t.set(t);
        self.render
            .call::<Table>((W as i64, H as i64, t, 1.0f64))
            .unwrap();
    }
}

fn raw_render(c: &mut Criterion) {
    let mut g = c.benchmark_group("raw_render_200x79");
    for name in SPLASHES {
        for (label, codegen) in [("interp", false), ("codegen", true)] {
            let lua = runtime_lua();
            let prepared = Prepared::new(&lua, name, None, codegen);
            g.bench_with_input(BenchmarkId::new(label, name), &prepared, |b, p| {
                b.iter(|| p.next_frame())
            });
        }
    }
    g.finish();
}

fn codegen_under_watchdog(c: &mut Criterion) {
    let mut g = c.benchmark_group("codegen_watchdog_200x79");
    let lua = runtime_lua();
    let prepared = Prepared::new(&lua, "aurora", None, true);
    let _wd = spawn_watchdog(&lua);
    g.bench_with_input(BenchmarkId::from_parameter("aurora"), &prepared, |b, p| {
        b.iter(|| p.next_frame())
    });
    g.finish();
}

fn env_d_late_codegen(c: &mut Criterion) {
    let mut g = c.benchmark_group("env_codegen_200x79");
    for (label, safe, early) in [
        ("global", false, true),
        ("global_late", false, false),
        ("envd", false, true),
        ("envd_safeenv", true, true),
        ("envd_late", false, false),
        ("envd_safeenv_late", true, false),
    ] {
        let lua = runtime_lua();
        let env = (!label.starts_with("global")).then(|| env_table(&lua, safe));
        let prepared = Prepared::new(&lua, "aurora", env.as_ref(), early);
        g.bench_with_input(BenchmarkId::from_parameter(label), &prepared, |b, p| {
            b.iter(|| p.next_frame())
        });
    }
    g.finish();
}

struct HostPrepared {
    handle: maki_lua::EventHandle,
    t: Cell<f64>,
}

impl HostPrepared {
    fn next_frame(&self) {
        let t = self.t.get() + FRAME_STEP;
        self.t.set(t);
        self.handle
            .splash_frame(W, H, t as f32, 1.0)
            .expect("missed splash deadline");
    }
}

fn pull_roundtrip(c: &mut Criterion) {
    let mut g = c.benchmark_group("pull_roundtrip_200x79");
    for name in SPLASHES {
        let mut opts = HashMap::new();
        let mut plugin_opts = serde_json::Map::new();
        plugin_opts.insert(
            "splash".to_string(),
            serde_json::Value::String(name.to_string()),
        );
        opts.insert("splashes".to_string(), plugin_opts);
        let (handle, _guard) = spawn_host_with_fs_and_opts_for_tests(
            &["splashes_default", "splashes"],
            Arc::new(InMemoryFs::new()),
            None,
            opts,
        );
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if handle.splash_frame(W, H, 1.0, 1.0).is_some() {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "{name}: host never served a frame"
            );
        }
        let prepared = HostPrepared {
            handle,
            t: Cell::new(1.0),
        };
        g.bench_with_input(BenchmarkId::from_parameter(name), &prepared, |b, p| {
            b.iter(|| p.next_frame())
        });
    }
    g.finish();
}

fn benches(c: &mut Criterion) {
    raw_render(c);
    codegen_under_watchdog(c);
    env_d_late_codegen(c);
    pull_roundtrip(c);
}

criterion_group! {
    name = splash_perf;
    config = Criterion::default()
        .sample_size(10)
        .warm_up_time(Duration::from_millis(500))
        .measurement_time(Duration::from_secs(1));
    targets = benches
}
criterion_main!(splash_perf);
