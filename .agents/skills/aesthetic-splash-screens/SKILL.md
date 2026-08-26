---
name: aesthetic-splash-screens
description: Design and build visually striking maki splash screens (the animated home screen behind the prompt) as self-contained Lua splashes. Covers the splash.render slot contract, the shared splash template, glyph-ramp shading, WGSL/GLSL shader porting, state management, perf budgets, and smoke testing without building maki. Use whenever someone wants a new splash, wants to polish an existing splash, mentions the maki home screen animation, asks to port a shader to a splash, wants to promote/bundle a splash into the maki distribution (the bundled splashes), or is exploring terminal eye-candy for maki, even if they don't say the word "splash".
---

# Aesthetic splash screens for maki

Maki's home screen is a pull-driven canvas of terminal cells. A splash is
a single Lua file that, given `(w, h, t, fade)`, returns `h` rows of styled
glyph segments covering exactly `w` cells each. This skill is about making
those frames beautiful, not just correct.

## What makes a splash look good

These come from building 17 splashes and watching what worked:

- **One clear idea per splash.** A spinning pentagram, rain with ripples, a Doom
  fire. The moment a splash has two ideas it reads as noise. Pick one effect and
  spend the whole budget on it.
- **Restraint.** Most cells should be background most of the time. Negative
  space is what makes the bright parts read. Full-field splashes (plasma, fire,
  caustics) are the exception, and they earn it by being smooth gradients, not
  clutter.
- **Motion with memory.** Trails, afterimages, and fades (`* -> + -> : -> .`)
  make motion legible at a glance. A thing that teleports between frames looks
  broken; a thing that leaves a decaying wake looks alive.
- **Depth cues.** Dim far things, brighten near things, cluster detail toward
  the horizon. Non-linear spacing (tunnel's `r^0.55` rings) reads as 3D almost
  for free.
- **Theme-aware color.** Pull colors from `maki.ui.theme_color` so the splash
  belongs to the user's theme. Reserve hardcoded palettes (fire, caustics
  deep-water blues) for effects that ARE their palette. When a concept
  mandates a fixed palette (synthwave purple-orange), the intended split is:
  hardcoded colors for the scene, theme colors for the furniture (background,
  label, version). The scene is the art; the furniture should still match
  the theme.
- **Calm timing.** Splash effects run at 0.1-0.6 Hz in feel. If it pulses
  faster than ~2 Hz it competes with the user's attention instead of
  decorating the screen.
- **A bottom-center label and top-right version** give every splash the same
  furniture, so the cycle feels like one gallery rather than 17 apps.

## The contract (short version)

Full reference: `site/docs/content/splash/_index.md`. The essentials:

```lua
maki.api.set_slot("splash.render", function(prev, w, h, t, fade)
  -- return h rows; each row is a list of { glyphs = string, style = ... };
  -- per row, all seg.glyphs concatenated are exactly w terminal cells wide.
end)
```

- `w`, `h`: cells. `t`: seconds since the splash appeared. `fade`: 0 -> 1
  entry fade; multiply color intensity by it.
- Style is `"field"` (transparent spaces), a hex string, or
  `{ fg = "#rrggbb", bg = "#rrggbb", bold = bool }`. Cache style tables and
  reuse them by identity: the renderer coalesces runs of the *same table* into
  one segment, so fresh tables per cell explode the segment count.
- Rendering is pull-driven: be pure in `t`, never block, never sleep.
  `maki.ui.theme_color` and `maki.version()` are fine to call.
- Stateful effects (cellular automata) keep per-size buffers, step by
  accumulated `dt` from `t`, and reset on
  `maki.api.create_autocmd("SplashShown", { callback = ... })`. See
  `fire.lua`/`life.lua` in the live config for the pattern.
- Every file also returns `M` with `M.render(w, h, t, fade)`, so a cycler in
  `init.lua` can call splashes directly. Keep the `set_slot` call too; the
  cycler's later `set_slot` wins.

## Workflow

1. **Copy the template.** `references/template.lua` has all the boilerplate.
   It ships every common helper (atan2, smoothstep, hash01, ...); delete the
   ones your effect doesn't use — dead helpers in a copied file mislead the
   next reader about what the splash actually does.

   Details:
   theme colors, style cache, grid builder, segment coalescing, tiny-area
   guard, label + version lines, and the shading helpers (quantized style,
   luminance glyph ramp, smoothstep, atan2). Fill in `M.shade` or the draw
   loop. Do not rewrite the boilerplate; it is the way it is because of hard
   lessons (style identity, UTF-8, fade handling).
2. **Pick the idea and the genre** (see the sparks list below). Decide early
   whether the splash is *sparse* (glyphs on background, most splashes) or *full*
   (every cell shaded, shader ports).
3. **Smoke test after every change** with the bundled script (no maki build
   needed; it runs under stock lua5.1 with a stubbed `maki`):

   ```bash
   lua5.1 scripts/smoke_test.lua --dir ~/.config/maki/lua kaleidoscope voronoi
   ```

   It asserts `rows == h`, exact per-row cell width (UTF-8 aware), no blank
   frames across sizes and time steps, and reports ms/frame.
4. **Bench it before and after perf work** with the bundled script (same
   lua5.1 stub): `lua5.1 scripts/bench.lua --dir <dir> <name>...`. It reports
   ms/frame at 80x24 and 200x79 (a tall terminal). Real maki renders compile
   to native (plugin envs are marked safe via `lua_setsafeenv`, so luau
   codegen applies), so expect ~lua5.1/5 in the app, plus a Rust-side parse
   that scales with segment count. The per-cell output path (quantize + ramp
   + segment coalescing) is the common floor; sampled-grid bilinear rendering
   (caustics-style, `SAMPLE_STEP`) cuts shade cost ~4x for smooth fields but
   smears steep gradients (metaballs kept full-res for this reason). Run
   merging within one 5-bit color step (`MERGE_TOL`) can cut segment count
   several-fold — the parse pays per segment — but keep it at 0 for
   high-frequency fields (kaleidoscope: tolerance merging flattened the
   fractal's color grain and read as bad quality).
5. **Look at it as text** before running maki:

   ```bash
   lua5.1 scripts/dump_frame.lua ~/.config/maki/lua/voronoi.lua 60 20 2.0
   ```

   Glyph-only dumps catch structural problems (asymmetry, clipping, label
   collisions) that unit checks miss. Background-color effects (fire) look
   blank in text dumps; inspect `seg.style.bg` values instead.
5. **Run maki and watch the cycle.** Color balance, quantization banding, and
   pacing are only judgeable live. Iterate on the one effect until it's
   unmistakable.

## Shipping a splash in the distribution

Once a splash earns its place, promote it from config to the bundled splashes so
every user gets it via `require("splash.<name>")` and sees it in the
`/splash` picker. Promotion ships to every install, so it has a higher bar
than the config playground.

Checklist (using `mysplash` as the example):

1. Copy the file to `plugins/splashes_default/splash/mysplash.lua`. Strip the
   `set_slot` call from the config version and add `M.description` (one line
   for the picker blurb); the module returns `M` with `M.description` and
   `M.render` and does not activate itself. Copy an existing splash's header.
2. Add `"mysplash"` to the `SPLASHES` list in `plugins/splashes_default/init.lua`.
   **No Rust change needed for the splash itself.** `splashes_default` is
   already a `BundledPlugin` entry in `maki-lua/src/loader.rs`; any `.lua`
   dropped under its `splash/` subdir becomes `require("splash.<name>")` on
   the next build. The namespace is reserved (`splash.*` resolves
   bundled-first), so never give a config splash a `splash.` name.
3. Add the splash to the table in `site/docs/content/splash/_index.md`.
4. Verify in order:
   - `lua5.1 .agents/skills/aesthetic-splash-screens/scripts/smoke_test.lua plugins/splashes_default/splash/mysplash.lua`
   - `cargo test -p maki-lua --test plugin_host` (runs the real VM: boots the
     bundled renderers, checks the slot default, overrides, and error paths).
   `just ci` covers all of it on a full run.

Build-env gotchas (homura VM): export `OPENSSL_NO_VENDOR=1` (system
libssl-dev exists; the vendored build fails), and if `include_dir!` claims a
file that exists does not, the 9p dentry cache is stale —
`sync && echo 3 > /proc/sys/vm/drop_caches` and retry.

## Making frame tests non-flaky

Frame-asserting tests (the splash tests in `maki-lua/tests/plugin_host.rs`)
assert things about frames, and stateful or RNG-driven splashes make naive
thresholds flaky. Rules learned the expensive way:

- **Drive warm-up time.** A stateful splash starts far from steady state
  (matrix heads spawn up to 40 rows above the screen). Pull frames across
  many simulated seconds before asserting anything; pure splashes don't care,
  so drive all splashes uniformly.
- **Sample several frames.** Rain/CA density oscillates as drops respawn.
  Assert on the max of a handful of frames, not one arbitrary instant.
- **Set thresholds from the expectation, not from what passed once.**
  Compute the steady-state mean and its sigma (binomial over columns, or
  just reason about it), put the floor several sigma below the mean — and
  confirm it stays far above what a *broken* splash would produce (a splash
  that draws only its label and version shows ~15 cells, so any floor above
  that distinguishes working from dead). A threshold set at 1.8 sigma from
  the mean failed CI on the second machine that ran it.

## Techniques that carry most of the weight

- **Aspect correction.** Terminal cells are ~2x taller than wide. For
  isotropic coordinates, advance x at half the y rate:
  `nx = (px - w/2) / h`, `ny = (2*py - h) / h` (y down). Circles stay round.
- **Luminance glyph ramp.** Map brightness to `" .:-=+*#%@"` and put the
  actual color in the fg style. The glyph carries shape, the color carries
  mood. Quantize RGB to ~5 bits/channel before building hex styles, or the
  style cache fills with thousands of one-frame colors.
- **Deterministic hashing for texture.** Starfields, rain columns, comet
  schedules: hash `(x, y)` or the index with an integer hash
  (`(x * 2654435761 + salt) % 4294967296`) instead of `math.random`. The frame
  stays a pure function of `t`, which keeps re-renders stable and testable.
- **Drift instead of jumps.** Parameters that morph over time (lissajous
  frequency ratio, wavebanner phase) should move with slow sines of `t`,
  period >= ~8 s, so the viewer sees evolution, not flicker.
- **Performance budget.** Established full-cell splashes run 2-3 ms/frame at
  80x24 on stock lua5.1 (slower than maki's luau-jit by ~5x). Up to ~20 ms on
  lua5.1 is acceptable. If a shader port is too slow, hoist terms that depend
  on only one axis into per-column or per-row precomputes (aurora went from
  33 ms to 9 ms this way), and memoize hashes.
- **Small terminal guard.** Below the guard size return `flat_rows` (solid
  background). Every existing splash guards; follow their thresholds.

## Porting shaders (WGSL / GLSL / shadertoy)

Read `references/shader-porting.md` when porting from shader-gallery.html,
shadertoy, or any fragment shader. It covers the coordinate mapping table,
hash/noise functions in float-safe Lua, uniforms without a host (`u_mouse`
becomes a slow sine orbit), and the quantization + hoisting playbook.

## Idea sparks

For scenes with regions, horizons, or perspective floors, read
`references/recipes.md` — it has the horizon split, perspective grid, and
striped-sun math ready to adapt, plus palette guidance for lit scenes.

Genres already shipped, for contrast — orbital geometry (pentagram,
lissajous), weather (rain, comets), cellular (fire, life), full-field shader
ports (plasma, metaballs), typography (wavebanner, printer), perspective
(tunnel, warp), nature (flowers). Six of the best are promoted to the
bundled splashes (`plugins/splashes_default/splash/`): kaleidoscope, voronoi,
caustics, metaballs, aurora (shader ports) and matrix (falling-code rain).
Treat the gallery sources as canonical worked examples — they are the
current bar for what ships.

Directions still open, roughly from safe to spicy:

- **Fractal zoom**: Mandelbrot/Julia with smooth iteration count -> ramp.
- **Reaction-diffusion-ish**: cheap Gray-Scott on a coarse grid, stateful.
- **Raymarched primitive**: one SDF sphere/torus with a moving light, shaded
  by ramp. Very few rays, huge payoff.
- **Terrain flyover**: value-noise heightfield rows scrolled toward the
  viewer, height -> glyph, distance fog.
- **Clock/calendar art**: the time rendered as geometry (orbital clock,
  binary tree of seconds).
- **CRT/glitch**: take any existing splash and add scanline dimming + rare
  horizontal slice offsets on a slow random schedule.

Whatever you pick: one idea, mostly background, memory in the motion, theme in
the colors. Then watch it for a full minute before calling it done.
