# Porting fragment shaders to splashes

Field guide for turning WGSL / GLSL / shadertoy fragment shaders into makima
splashes. The five ports in the live config (kaleidoscope, voronoi,
caustics, metaballs, aurora, all from `shader-gallery.html`) follow exactly
this playbook.

## Coordinate mapping

Terminal cells are ~2x taller than wide. A shader written for square pixels
looks stretched unless x advances at half the y rate. Let `px = x - 0.5`,
`py = y - 0.5` be the cell-center coordinates, 1-based.

| Shader expression | Lua equivalent |
|---|---|
| `(p.xy * 2.0 - res) / res.y` | `nx = (px - w / 2) / h`, `ny = (2 * py - h) / h` |
| `p.xy / res.y` | `ux = px / (2 * h)`, `uy = py / h` |
| `p.xy / res` | `ux = px / w`, `uy = py / h` (no aspect math needed) |
| `length(uv)` | `math.sqrt(nx * nx + ny * ny)` |
| `atan2(y, x)` | the local `atan2` in the template (no `math.atan2` on luau) |
| `fract(x)` | `x - math.floor(x)` |
| `mix(a, b, u)` | `a + (b - a) * u` |
| `clamp(x, 0, 1)` | explicit min/max (no builtin on 5.1) |
| `smoothstep` | template helper (works with reversed edges too) |
| `pow(x, y)` | `x ^ y` (watch negative bases with fractional y) |
| vector swizzles / vec ops | expand to scalar locals; write `qx, qy` not tables in hot loops |

Cell-aspect rule of thumb: if the shader normalizes by `res.y` (isotropic
space), halve the x rate. If it normalizes per-axis (`p/res`), use as is.

## Uniforms without a host

- `u_time` -> `t` (seconds, scaled as the shader likes).
- `u_mouse` -> slow sine orbits: `mx = 0.5 + 0.3 * math.sin(t * 0.33 + phase)`.
  Mouse-driven interactivity does not exist on a splash; a lazy autonomous
  driver keeps the motion organic without input.
- `u_res` -> `w`, `h` in cells.

## Hashes and noise in double-precision Lua

GLSL hashes use `fract(sin(dot(p, c)) * 43758.5453)`. Lua doubles keep this
stable for the coordinate ranges splashes use (|p| up to a few thousand). Port
them literally:

```lua
local function h21(px, py)
  local s = math.sin(px * 127.1 + py * 311.7) * 43758.5453
  return s - math.floor(s)
end

-- smooth value noise (bilinear with hermite smoothing)
local function n2(px, py)
  local ix, iy = math.floor(px), math.floor(py)
  local fx, fy = px - ix, py - iy
  local ux = fx * fx * (3.0 - 2.0 * fx)
  local uy = fy * fy * (3.0 - 2.0 * fy)
  local a = h21(ix, iy)
  local b = h21(ix + 1, iy)
  local c = h21(ix, iy + 1)
  local d = h21(ix + 1, iy + 1)
  return a + (b - a) * ux + (c - a) * uy + (a - b - c + d) * ux * uy
end
```

## Order-of-evaluation traps

GPUs evaluate both branches/operands from the *old* state; a line like
`q = q + vec2(n2(q * f + k), n2(q * f - k)) * 0.7` reads the old `q` twice.
In scalar Lua, compute both terms into locals first, then update, or you
silently get a different (usually uglier) dynamic.

## Color and glyph output

Full-color effects map each cell to a quantized hex fg style plus a glyph from
the luminance ramp (`" .:-=+*#%@"`). Use the template's `shade_style` (5-bit
quantization) and `ramp_glyph`. Effects that ARE light (fire) use
theme-background-colored spaces via a small `pixel_style` helper instead; they
look blank in text dumps, judge them by `seg.style.fg` values.

## Performance playbook

Budget reference: 2-3 ms/frame at 80x24 on stock lua5.1 is the established
norm (tunnel, plasma); up to ~20 ms is tolerable. Makima's luau-jit is roughly
5x faster than lua5.1, so measure with the bundled smoke script and use the
lua5.1 number as a pessimistic bound.

1. **Hoist single-axis terms.** If part of the fragment depends only on `x`
   or only on `y` (aurora's band centers, shimmer, hues), compute it once per
   column/row outside the inner loop. This took aurora from 33 to 9 ms.
2. **Memoize hashes per frame** when the same lattice points repeat across
   cells (voronoi neighbors shared between adjacent fragments).
3. **Count your transcendentals.** `sin/cos/exp/sqrt` dominate. If a frame
   needs > ~100k of them on the big screen, restructure: coarser sampling of
   smooth fields (evaluate the field at half resolution and it still reads),
   shorter loop ranges, or early-outs for cells that end up background.
4. **Scalar locals in hot loops.** No table allocation per cell
   (`vec2 {x, y}` per fragment is the classic mistake). The grid cell table is
   the only per-cell allocation you need.
