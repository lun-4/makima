// Real-VM splash bench for the bundled splashes at 200x79 (the tall-terminal
// case that drags fps).
//
//   cargo test -p maki-lua --test splash_bench -- --nocapture          (fast)
//   cargo test -p maki-lua --test splash_bench pull_roundtrip -- --ignored --nocapture
//
// Findings baked into this file:
// - `raw_luau_render_200x79`: luau native codegen makes code ~5x faster than
//   the interpreter (aurora 32.5 -> 5.8 ms).
// - `codegen_under_watchdog`: maki's 50ms watchdog interrupt does not
//   disable codegen.
// - `env_d_late_codegen`: codegen does NOT apply to chunks whose env table
//   was never marked safe (`lua_setsafeenv`). maki's build_env now calls
//   `Table::set_safeenv(true)`; env'd + safeenv runs at native speed (4.1ms),
//   the same as a global-env chunk (4.0ms).
// - `pull_roundtrip_200x79`: full EventHandle::splash_frame pull meter
//   (render + mlua conversion + Rust SplashFrame parse) at 200x79. The
//   render runs compiled (the splash pull spends a small codegen budget per
//   frame); remaining cost is the per-segment parse, so segment count is the
//   thing to watch (see MERGE_TOL in the splashes).

use std::collections::HashMap;
use std::ffi::c_int;
use std::ptr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicPtr, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use maki_lua::test_support::{InMemoryFs, spawn_host_with_fs_and_opts_for_tests};
use mlua::{Compiler, Lua, Table, ffi};

const W: u16 = 200;
const H: u16 = 79;
const SPLASHES: [&str; 5] = ["aurora", "caustics", "kaleidoscope", "metaballs", "voronoi"];

type InterruptFn = unsafe extern "C-unwind" fn(*mut ffi::lua_State, c_int);

fn store_interrupt(state: *mut ffi::lua_State, cb: Option<InterruptFn>) {
    let raw = cb.map_or(ptr::null_mut(), |f| f as *mut ());
    unsafe {
        let slot = &raw mut (*ffi::lua_callbacks(state)).interrupt;
        AtomicPtr::from_ptr(slot.cast::<*mut ()>()).store(raw, Ordering::Release);
    }
}

unsafe extern "C-unwind" fn one_shot_interrupt(state: *mut ffi::lua_State, gc: c_int) {
    if gc >= 0 {
        return;
    }
    store_interrupt(state, None);
}

struct WatchdogGuard {
    stop: Arc<AtomicBool>,
    thread: Option<thread::JoinHandle<()>>,
}

impl Drop for WatchdogGuard {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(t) = self.thread.take() {
            t.thread().unpark();
            let _ = t.join();
        }
    }
}

fn spawn_watchdog(lua: &Lua) -> WatchdogGuard {
    let main_state = lua.exec_raw_lua(|raw| unsafe { ffi::lua_mainthread(raw.state()) }) as usize;
    let stop = Arc::new(AtomicBool::new(false));
    let thread = thread::spawn({
        let stop = Arc::clone(&stop);
        let keep_alive = lua.clone();
        move || {
            let _keep_alive = keep_alive;
            loop {
                thread::park_timeout(Duration::from_millis(10));
                if stop.load(Ordering::Relaxed) {
                    return;
                }
                store_interrupt(main_state as *mut ffi::lua_State, Some(one_shot_interrupt));
            }
        }
    });
    WatchdogGuard {
        stop,
        thread: Some(thread),
    }
}

fn raw_bench_one(name: &str, lua: &Lua, codegen: bool) -> f64 {
    let src = std::fs::read_to_string(format!(
        "{}/../plugins/splashes_default/splash/{name}.lua",
        env!("CARGO_MANIFEST_DIR")
    ))
    .unwrap_or_else(|_| panic!("read {name}"));
    let chunk = lua
        .load(&src)
        .set_name(format!("splash.{name}"))
        .into_function()
        .unwrap();
    if codegen {
        unsafe {
            lua.exec_raw::<()>(chunk.clone(), |state| {
                mlua::ffi::luau_codegen_compile(state, -1)
            })
            .expect("codegen");
        }
    }
    let m: mlua::Table = chunk.call(()).unwrap();
    let render: mlua::Function = m.get("render").unwrap();
    render
        .call::<mlua::Table>((W as i64, H as i64, 1.0f64, 1.0f64))
        .unwrap();
    let budget = std::env::var("SPLASH_BENCH_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0.5);
    let mut frames = 0u32;
    let mut t = 1.0;
    let t0 = Instant::now();
    while t0.elapsed().as_secs_f64() < budget {
        t += 0.033;
        render
            .call::<mlua::Table>((W as i64, H as i64, t, 1.0f64))
            .unwrap();
        frames += 1;
    }
    t0.elapsed().as_secs_f64() * 1000.0 / frames as f64
}

#[test]
fn raw_luau_render_200x79() {
    let (interp, _g1) = (Lua::new(), ());
    let (jit, _g2) = (Lua::new(), ());
    for lua in [&interp, &jit] {
        lua.set_compiler(Compiler::new().set_optimization_level(2));
        lua.set_memory_limit(512 * 1024 * 1024).unwrap();
        lua.sandbox(true).unwrap();
        lua.enable_jit(false);
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
            lua.create_function(|_, _: mlua::MultiValue| Ok(()))
                .unwrap(),
        )
        .unwrap();
        api.set(
            "create_autocmd",
            lua.create_function(|_, _: mlua::MultiValue| Ok(()))
                .unwrap(),
        )
        .unwrap();
        maki.set("api", api).unwrap();
        lua.globals().set("maki", maki).unwrap();
    }
    for name in SPLASHES {
        let interp_ms = raw_bench_one(name, &interp, false);
        let jit_ms = raw_bench_one(name, &jit, true);
        eprintln!(
            "{name:<15} interp {interp_ms:6.2} ms  codegen {jit_ms:6.2} ms  speedup {speedup:4.2}x",
            speedup = interp_ms / jit_ms
        );
    }
}

fn bench_one(name: &str, budget_secs: f64) {
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
    let deadline = Instant::now() + std::time::Duration::from_secs(5);
    loop {
        if handle.splash_frame(W, H, 1.0, 1.0).is_some() {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "{name}: host never served a frame"
        );
    }
    let mut t = 1.0;
    let mut frames = 0u32;
    let mut misses = 0u32;
    let t0 = Instant::now();
    while t0.elapsed().as_secs_f64() < budget_secs {
        t += 0.033;
        match handle.splash_frame(W, H, t as f32, 1.0) {
            Some(_) => frames += 1,
            None => misses += 1,
        }
    }
    eprintln!(
        "{name:<15} {fps:6.1} fps  ({frames} frames, {misses} missed)",
        fps = frames as f64 / budget_secs
    );
}

#[test]
fn codegen_under_watchdog() {
    let lua = Lua::new();
    lua.set_compiler(Compiler::new().set_optimization_level(2));
    lua.set_memory_limit(512 * 1024 * 1024).unwrap();
    lua.sandbox(true).unwrap();
    lua.enable_jit(false);
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
        lua.create_function(|_, _: mlua::MultiValue| Ok(()))
            .unwrap(),
    )
    .unwrap();
    api.set(
        "create_autocmd",
        lua.create_function(|_, _: mlua::MultiValue| Ok(()))
            .unwrap(),
    )
    .unwrap();
    maki.set("api", api).unwrap();
    lua.globals().set("maki", maki).unwrap();

    let src = std::fs::read_to_string(format!(
        "{}/../plugins/splashes_default/splash/aurora.lua",
        env!("CARGO_MANIFEST_DIR")
    ))
    .unwrap();
    let chunk = lua
        .load(&src)
        .set_name("splash.aurora")
        .into_function()
        .unwrap();
    unsafe {
        lua.exec_raw::<()>(chunk.clone(), |state| {
            mlua::ffi::luau_codegen_compile(state, -1)
        })
        .expect("codegen");
    }
    let m: mlua::Table = chunk.call(()).unwrap();
    let render: mlua::Function = m.get("render").unwrap();
    render
        .call::<mlua::Table>((W as i64, H as i64, 1.0f64, 1.0f64))
        .unwrap();

    let _wd = spawn_watchdog(&lua);
    let budget = 1.0;
    let mut frames = 0u32;
    let mut t = 1.0;
    let t0 = Instant::now();
    while t0.elapsed().as_secs_f64() < budget {
        t += 0.033;
        render
            .call::<mlua::Table>((W as i64, H as i64, t, 1.0f64))
            .unwrap();
        frames += 1;
    }
    eprintln!(
        "aurora codegen+watchdog: {:.2} ms/frame",
        t0.elapsed().as_secs_f64() * 1000.0 / frames as f64
    );
}

#[test]
fn env_d_late_codegen() {
    let lua = Lua::new();
    lua.set_compiler(Compiler::new().set_optimization_level(2));
    lua.set_memory_limit(512 * 1024 * 1024).unwrap();
    lua.sandbox(true).unwrap();
    lua.enable_jit(false);
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
        lua.create_function(|_, _: mlua::MultiValue| Ok(()))
            .unwrap(),
    )
    .unwrap();
    api.set(
        "create_autocmd",
        lua.create_function(|_, _: mlua::MultiValue| Ok(()))
            .unwrap(),
    )
    .unwrap();
    maki.set("api", api).unwrap();
    // Module-style env: a fresh table chaining to the real globals, like
    // maki's build_env. The safeenv-marked twin is what maki now produces
    // (runtime.rs build_env); proving it unlocks codegen for env'd chunks.
    let env = lua.create_table().unwrap();
    let mt = lua.create_table().unwrap();
    mt.set("__index", lua.globals()).unwrap();
    env.set_metatable(Some(mt)).unwrap();
    env.set("maki", maki.clone()).unwrap();
    lua.globals().set("maki", maki).unwrap();
    let safe_env = lua.create_table().unwrap();
    let mt = lua.create_table().unwrap();
    mt.set("__index", lua.globals()).unwrap();
    safe_env.set_metatable(Some(mt)).unwrap();
    safe_env
        .set("maki", env.get::<mlua::Table>("maki").unwrap())
        .unwrap();
    safe_env.set_safeenv(true);

    let src = std::fs::read_to_string(format!(
        "{}/../plugins/splashes_default/splash/aurora.lua",
        env!("CARGO_MANIFEST_DIR")
    ))
    .unwrap();

    for (label, env_opt, early) in [
        ("global", None, true),
        ("global_late", None, false),
        ("envd", Some(&env), true),
        ("envd_safeenv", Some(&safe_env), true),
        ("envd_late", Some(&env), false),
        ("envd_safeenv_late", Some(&safe_env), false),
    ] {
        let chunk = {
            let c = lua.load(&src).set_name("splash.aurora");
            let c = match env_opt {
                Some(e) => c.set_environment(e.clone()),
                None => c,
            };
            c.into_function().unwrap()
        };
        if early {
            unsafe {
                lua.exec_raw::<()>(chunk.clone(), |state| {
                    mlua::ffi::luau_codegen_compile(state, -1)
                })
                .expect("codegen");
            }
        }
        let m: Table = chunk.call(()).unwrap();
        let render: mlua::Function = m.get("render").unwrap();
        render
            .call::<mlua::Table>((W as i64, H as i64, 1.0f64, 1.0f64))
            .unwrap();
        if !early {
            unsafe {
                lua.exec_raw::<()>(chunk, |state| mlua::ffi::luau_codegen_compile(state, -1))
                    .expect("codegen");
            }
        }
        let mut frames = 0u32;
        let mut t = 1.0;
        let t0 = Instant::now();
        while t0.elapsed().as_secs_f64() < 0.6 {
            t += 0.033;
            render
                .call::<mlua::Table>((W as i64, H as i64, t, 1.0f64))
                .unwrap();
            frames += 1;
        }
        eprintln!(
            "aurora {label}: {:.2} ms/frame",
            t0.elapsed().as_secs_f64() * 1000.0 / frames as f64
        );
    }
}

#[test]
fn pull_roundtrip_200x79() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_test_writer()
        .try_init();
    let budget: f64 = std::env::var("SPLASH_BENCH_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0.5);
    for name in SPLASHES {
        bench_one(name, budget);
    }
}
