// Real-VM splash bench: same CLI as scripts/bench.lua (lua5.1) but runs on
// Luau through mlua with native codegen, configured like the plugin runtime:
// O2 compiler, sandbox, memory limit, and safeenv-marked envs (runtime.rs
// build_env), so the reported numbers are the ones maki actually renders with.
// The Rust-side per-segment parse is not included; see
// maki-lua/tests/splash_bench.rs pull_roundtrip_200x79 for the full host.
//
//   cargo run -p maki-lua --example splash_bench -- [--dir DIR] [--sizes WxH[,WxH...]] [--budget SECS] name...

use std::path::Path;
use std::process::ExitCode;
use std::time::Instant;

use mlua::{Compiler, Function, Lua, Table, ffi};

const DEFAULT_SIZES: [(i64, i64); 2] = [(80, 24), (200, 79)];
const DEFAULT_BUDGET: f64 = 0.5;
const MEMORY_LIMIT: usize = 512 * 1024 * 1024;
const MAX_REQUIRE_DEPTH: u32 = 16;

struct Args {
    dir: String,
    sizes: Vec<(i64, i64)>,
    budget: f64,
    names: Vec<String>,
}

fn parse_args(argv: Vec<String>) -> Result<Args, String> {
    let mut args = Args {
        dir: ".".to_string(),
        sizes: DEFAULT_SIZES.to_vec(),
        budget: DEFAULT_BUDGET,
        names: vec![],
    };
    let mut i = 0;
    while i < argv.len() {
        match argv[i].as_str() {
            "--dir" => {
                i += 1;
                args.dir = argv.get(i).ok_or("--dir needs a value")?.clone();
            }
            "--sizes" => {
                i += 1;
                let list = argv.get(i).ok_or("--sizes needs a value")?;
                args.sizes = list
                    .split(',')
                    .map(|s| {
                        let (w, h) = s
                            .split_once('x')
                            .ok_or_else(|| format!("bad size '{s}', want WxH"))?;
                        Ok((
                            w.parse().map_err(|_| format!("bad width '{w}'"))?,
                            h.parse().map_err(|_| format!("bad height '{h}'"))?,
                        ))
                    })
                    .collect::<Result<_, String>>()?;
            }
            "--budget" => {
                i += 1;
                args.budget = argv
                    .get(i)
                    .ok_or("--budget needs a value")?
                    .parse()
                    .map_err(|_| "--budget needs a number of seconds")?;
            }
            name => args.names.push(name.to_string()),
        }
        i += 1;
    }
    if args.names.is_empty() {
        return Err(
            "usage: splash_bench [--dir DIR] [--sizes WxH[,WxH...]] [--budget SECS] name...".into(),
        );
    }
    Ok(args)
}

fn make_maki_stub(lua: &Lua) -> mlua::Result<Table> {
    let maki = lua.create_table()?;
    let ui = lua.create_table()?;
    ui.set(
        "theme_color",
        lua.create_function(|_, _: ()| Ok(None::<String>))?,
    )?;
    maki.set("ui", ui)?;
    let version = lua.create_table()?;
    version.set("current", "0.0.0-bench")?;
    maki.set(
        "version",
        lua.create_function(move |_, _: ()| Ok(version.clone()))?,
    )?;
    let api = lua.create_table()?;
    api.set(
        "set_slot",
        lua.create_function(|_, _: mlua::MultiValue| Ok(()))?,
    )?;
    api.set(
        "create_autocmd",
        lua.create_function(|_, _: mlua::MultiValue| Ok(()))?,
    )?;
    maki.set("api", api)?;
    Ok(maki)
}

/// Mirrors runtime.rs build_env: fresh env table, safeenv set (unlocks native
/// codegen), maki bound, require present, everything else via globals.
fn make_env(
    lua: &Lua,
    maki: &Table,
    dir: &str,
    with_require: bool,
    depth: u32,
) -> mlua::Result<Table> {
    let env = lua.create_table()?;
    env.set_safeenv(true);
    env.set("maki", maki.clone())?;
    if with_require {
        let dir = dir.to_string();
        let maki = maki.clone();
        let require = lua.create_function(move |lua, modname: String| {
            if depth >= MAX_REQUIRE_DEPTH {
                return Err(mlua::Error::RuntimeError(format!(
                    "require depth exceeded loading '{modname}'"
                )));
            }
            for cand in [
                format!("{dir}/{modname}.lua"),
                format!("{dir}/splash/{modname}.lua"),
            ] {
                if let Ok(src) = std::fs::read_to_string(&cand) {
                    let env = make_env(lua, &maki, &dir, true, depth + 1)?;
                    return load_module(lua, &env, &src, &cand);
                }
            }
            Err(mlua::Error::RuntimeError(format!(
                "module '{modname}' not found under '{dir}'"
            )))
        })?;
        env.set("require", require)?;
    }
    let meta = lua.create_table()?;
    meta.set("__index", lua.globals())?;
    env.set_metatable(Some(meta))?;
    Ok(env)
}

fn load_module(lua: &Lua, env: &Table, src: &str, name: &str) -> mlua::Result<Table> {
    let chunk = lua
        .load(src)
        .set_name(name)
        .set_environment(env.clone())
        .into_function()?;
    unsafe {
        lua.exec_raw::<()>(chunk.clone(), |state| ffi::luau_codegen_compile(state, -1))?;
    }
    chunk.call(())
}

fn resolve_splash(dir: &str, name: &str) -> String {
    if name.ends_with(".lua") {
        return name.to_string();
    }
    for cand in [
        format!("{dir}/{name}.lua"),
        format!("{dir}/splash/{name}.lua"),
    ] {
        if Path::new(&cand).exists() {
            return cand;
        }
    }
    panic!("{name}: no {dir}/{name}.lua or {dir}/splash/{name}.lua")
}

fn measure(render: &Function, w: i64, h: i64, budget: f64) -> mlua::Result<f64> {
    render.call::<Table>((w, h, 1.0f64, 1.0f64))?;
    let mut frames = 0u32;
    let mut t = 1.0;
    let t0 = Instant::now();
    while t0.elapsed().as_secs_f64() < budget {
        t += 0.033;
        render.call::<Table>((w, h, t, 1.0f64))?;
        frames += 1;
    }
    Ok(t0.elapsed().as_secs_f64() * 1000.0 / f64::from(frames))
}

fn main() -> ExitCode {
    let args = match parse_args(std::env::args().skip(1).collect()) {
        Ok(a) => a,
        Err(e) => {
            eprintln!("{e}");
            return ExitCode::from(2);
        }
    };

    let lua = Lua::new();
    lua.set_compiler(Compiler::new().set_optimization_level(2));
    lua.set_memory_limit(MEMORY_LIMIT).expect("memory limit");
    lua.sandbox(true).expect("sandbox");
    lua.enable_jit(false);

    let maki = make_maki_stub(&lua).expect("maki stub");
    lua.globals().set("maki", maki.clone()).expect("globals");
    let env = make_env(&lua, &maki, &args.dir, true, 0).expect("env");

    let header = format!(
        "{:<16} {:>8} {:>10} {:>10} {:>8}",
        "splash", "size", "ms", "fps", "fps@60"
    );
    println!("{header}");
    println!("{}", "-".repeat(header.len()));
    for name in &args.names {
        let src = std::fs::read_to_string(resolve_splash(&args.dir, name))
            .unwrap_or_else(|e| panic!("read {name}: {e}"));
        let m = load_module(&lua, &env, &src, &format!("splash.{name}"))
            .unwrap_or_else(|e| panic!("load {name}: {e}"));
        let render: Function = m
            .get("render")
            .unwrap_or_else(|_| panic!("{name}: no M.render"));
        for (w, h) in &args.sizes {
            let ms = measure(&render, *w, *h, args.budget)
                .unwrap_or_else(|e| panic!("render {name} {w}x{h}: {e}"));
            println!(
                "{:<16} {:>4}x{:<3} {:>9.2} {:>9.1}  {}",
                name,
                w,
                h,
                ms,
                1000.0 / ms,
                if ms <= 16.7 { "ok" } else { "SLOW" }
            );
        }
    }
    println!(
        "measured on Luau with native codegen (safeenv envs, matches the plugin runtime); the Rust-side per-segment parse is excluded (see maki-lua/tests/splash_bench.rs pull_roundtrip_200x79). fps@60 means the frame fits in a 60hz tick."
    );
    ExitCode::SUCCESS
}
