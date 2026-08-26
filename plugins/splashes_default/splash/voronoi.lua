-- Bundled splash, part of the maki distribution.
-- Require from init.lua with:   local splash = require("splash.voronoi")
-- The module returns M with M.description and M.render(w, h, t, fade) and does not activate itself.
--
-- Voronoi cells splash. Software port of the WGSL "Voronoi Cells" shader

local TAU = 2.0 * math.pi
local SCALE = 6.0
local RAMP = " .:-=+*#%@"
local SAMPLE_STEP = 2
local M = {}
M.description = "Animated Voronoi cells with warm glowing borders."

local function theme_or(name, fallback)
  local c = maki.ui.theme_color(name)
  if c then
    return {
      tonumber(string.sub(c, 2, 3), 16),
      tonumber(string.sub(c, 4, 5), 16),
      tonumber(string.sub(c, 6, 7), 16),
    }
  end
  return fallback
end

local BG, FG, ACCENT, BG_HEX
local style_cache = {}

local function refresh_colors()
  BG = theme_or("background", { 40, 42, 54 })
  FG = theme_or("foreground", { 248, 248, 242 })
  ACCENT = theme_or("accent", { 255, 184, 108 })
  BG_HEX = string.format("#%02x%02x%02x", BG[1], BG[2], BG[3])
  style_cache = {}
end

local function rgb_to_hex(c, a)
  return string.format(
    "#%02x%02x%02x",
    math.floor(c[1] * a + 0.5),
    math.floor(c[2] * a + 0.5),
    math.floor(c[3] * a + 0.5)
  )
end

local function color(hex)
  local s = style_cache[hex]
  if not s then
    s = { fg = hex, bg = BG_HEX, bold = false }
    style_cache[hex] = s
  end
  return s
end

local function flat_rows(w, h, st)
  local rows = {}
  for y = 1, h do
    rows[y] = { { glyphs = string.rep(" ", w), style = st } }
  end
  return rows
end

local function smoothstep(e0, e1, x)
  local u = (x - e0) / (e1 - e0)
  if u < 0 then
    u = 0
  elseif u > 1 then
    u = 1
  end
  return u * u * (3.0 - 2.0 * u)
end

-- 5-bit keyed style: quantize to 0..31 per channel (so a `31`-step grid maps
-- back to 8-bit hex on cache miss) and key on the components directly. This
-- drops the per-cell *255/31 round-trip, and `MERGE_TOL` (one step ~= 8/255)
-- lets the row builder merge runs whose colors drift by a step; that cuts
-- segment count (and Rust-side parse cost) several-fold with no visible
-- banding.
local MERGE_TOL = 1

local function quantize5(v)
  if v < 0 then
    v = 0
  elseif v > 1 then
    v = 1
  end
  return math.floor(v * 31 + 0.5)
end

local function cell_style(r, g, b, f)
  local qr = quantize5(r * f)
  local qg = quantize5(g * f)
  local qb = quantize5(b * f)
  local key = qr * 1024 + qg * 32 + qb
  local st = style_cache[key]
  if not st then
    st = {
      fg = string.format(
        "#%02x%02x%02x",
        math.floor(qr * 255 / 31 + 0.5),
        math.floor(qg * 255 / 31 + 0.5),
        math.floor(qb * 255 / 31 + 0.5)
      ),
      bg = BG_HEX,
      bold = false,
    }
    style_cache[key] = st
  end
  return qr, qg, qb, st
end

local function ramp_glyph(lum)
  if lum < 0 then
    lum = 0
  elseif lum > 1 then
    lum = 1
  end
  local gi = math.floor(lum * (#RAMP - 1) + 0.5) + 1
  return string.sub(RAMP, gi, gi)
end

local function h22(px, py)
  local s1 = math.sin(px * 127.1 + py * 311.7) * 43758.5453
  local s2 = math.sin(px * 269.5 + py * 183.3) * 43758.5453
  return s1 - math.floor(s1), s2 - math.floor(s2)
end

-- Fragment shade for pattern coords (ux, uy); returns r, g, b in [0, 1].
-- point_cache rows hold { bx, by, rnd, cr2, cg2, cb2 }: the squared mesh
-- color terms are precomputed per point per frame in render.
function M.shade(ux, uy, t, point_cache)
  local ipx = math.floor(ux)
  local ipy = math.floor(uy)
  local fpx = ux - ipx
  local fpy = uy - ipy
  local f1 = 8.0
  local f2 = 8.0
  local colors = point_cache[0][0]
  for gy = -1, 1 do
    local point_row = point_cache[ipy + gy]
    for gx = -1, 1 do
      local point = point_row[ipx + gx]
      local dx = gx + point[1] - fpx
      local dy = gy + point[2] - fpy
      local d = dx * dx + dy * dy
      if d < f1 then
        f2 = f1
        f1 = d
        colors = point
      elseif d < f2 then
        f2 = d
      end
    end
  end
  f1 = math.sqrt(f1)
  f2 = math.sqrt(f2)
  local lines = smoothstep(0.0, 0.08, f2 - f1)
  local bright = 0.4 + 0.6 * f1
  local rr = (1.0 + (colors[4] - 1.0) * lines) * bright
  local gg = (0.95 + (colors[5] - 0.95) * lines) * bright
  local bb = (0.7 + (colors[6] - 0.7) * lines) * bright
  return rr, gg, bb
end

function M.render(w, h, t, fade)
  refresh_colors()
  local f = fade or 1.0
  if w < 8 or h < 6 then
    return flat_rows(w, h, color(BG_HEX))
  end
  local version = "v" .. maki.version().current
  local version_x = w - #version + 1
  local version_style = color(rgb_to_hex(FG, 0.4 * f))
  local rows = {}
  local point_cache = {}
  local x_scale = SCALE / (2 * h)
  local sin_t = math.sin(t)
  local cos_t = math.cos(t)
  for py = -1, math.ceil(SCALE + t * 0.1) + 1 do
    local point_row = {}
    point_cache[py] = point_row
    for px = -1, math.ceil(w * x_scale + t * 0.2) + 1 do
      local ox, oy = h22(px, py)
      local sin_ox, cos_ox = math.sin(TAU * ox), math.cos(TAU * ox)
      local sin_oy, cos_oy = math.sin(TAU * oy), math.cos(TAU * oy)
      local bx = 0.5 + 0.5 * (sin_t * cos_ox + cos_t * sin_ox)
      local by = 0.5 + 0.5 * (sin_t * cos_oy + cos_t * sin_oy)
      local ph = ox * TAU + t * 0.5
      local cr = 0.5 + 0.5 * math.cos(ph + 0.0)
      local cg = 0.5 + 0.5 * math.cos(ph + 2.0)
      local cb = 0.5 + 0.5 * math.cos(ph + 4.0)
      point_row[px] = { bx, by, ox, cr * cr, cg * cg, cb * cb }
    end
  end
  local sample_r, sample_g, sample_b
  for y = 1, h do
    if (y - 1) % SAMPLE_STEP == 0 then
      sample_r, sample_g, sample_b = {}, {}, {}
      local uy = ((y - 0.5) / h) * SCALE + t * 0.1
      for x = 1, w, SAMPLE_STEP do
        local r, g, b = M.shade((x - 0.5) * x_scale + t * 0.2, uy, t, point_cache)
        sample_r[x], sample_g[x], sample_b[x] = r, g, b
      end
    end
    local glyphs = {}
    local segs = {}
    local current_style
    local run_qr, run_qg, run_qb
    local run_start = 1
    for x = 1, w do
      local glyph
      local qr, qg, qb
      local style
      if y == 1 and x >= version_x then
        glyph = string.sub(version, x - version_x + 1, x - version_x + 1)
        style = version_style
      else
        local sample_x = x - (x - 1) % SAMPLE_STEP
        local r, g, b = sample_r[sample_x], sample_g[sample_x], sample_b[sample_x]
        glyph = ramp_glyph((0.2126 * r + 0.7152 * g + 0.0722 * b) * f)
        qr, qg, qb, style = cell_style(r, g, b, f)
      end
      local near = style == current_style
        or (
          qr ~= nil
          and run_qr ~= nil
          and math.abs(qr - run_qr) <= MERGE_TOL
          and math.abs(qg - run_qg) <= MERGE_TOL
          and math.abs(qb - run_qb) <= MERGE_TOL
        )
      if not near then
        if current_style then
          segs[#segs + 1] = { glyphs = table.concat(glyphs, "", run_start, x - 1), style = current_style }
        end
        current_style = style
        run_qr, run_qg, run_qb = qr, qg, qb
        run_start = x
      end
      glyphs[x] = glyph
    end
    segs[#segs + 1] = { glyphs = table.concat(glyphs, "", run_start, w), style = current_style }
    rows[y] = segs
  end
  return rows
end

return M
