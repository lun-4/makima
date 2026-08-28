-- Bundled splash, part of the makima distribution.
-- Require from init.lua with:   local splash = require("splash.kaleidoscope")
-- The module returns M with M.description and M.render(w, h, t, fade) and does not activate itself.
--
-- Kaleidoscope splash. Software port of the WGSL "Kaleidoscope" shader from

local TAU = 2.0 * math.pi
local SEGMENTS = 10.0
local RAMP = " .:-=+*#%@"
local SAMPLE_STEP = 2
local M = {}
M.description = "A mirrored fractal kaleidoscope with tenfold symmetry."

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

local function atan2(y, x)
  if x > 0 then
    return math.atan(y / x)
  elseif x < 0 and y >= 0 then
    return math.atan(y / x) + math.pi
  elseif x < 0 then
    return math.atan(y / x) - math.pi
  elseif y > 0 then
    return math.pi / 2
  elseif y < 0 then
    return -math.pi / 2
  end
  return 0
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
-- back to 8-bit hex on cache miss) and key on the components directly. Runs
-- coalesce on the exact key (MERGE_TOL = 0): the fractal's fine color grain
-- is per-bucket, and tolerance merging read as flat bands on the kaleidoscope.
local MERGE_TOL = 0

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

-- Per-frame hoisted constants for shade: 1/brightness steadied with the drift
-- term, plus the five per-iteration sin phases.
local function shade_terms(t)
  local tt = t * 0.25
  local drift = 0.3 + 0.2 * math.sin(t * 0.3)
  local phase = { math.sin(tt + 0), math.sin(tt + 1), math.sin(tt + 2), math.sin(tt + 3), math.sin(tt + 4) }
  return tt, drift, phase
end

-- Fragment shade: returns r, g, b in [0, 1] for isotropic coords (nx, ny).
local function shade_core(nx, ny, t, tt, drift, phase)
  local a = atan2(ny, nx)
  local r = math.sqrt(nx * nx + ny * ny)
  local seg = TAU / SEGMENTS
  local sa = math.abs((a % (2.0 * seg)) - seg) + tt * 0.4
  local ox = math.cos(sa) * r - drift
  local oy = math.sin(sa) * r
  local qx, qy = ox * 3.0, oy * 3.0
  local ar, ag, ab = 0.0, 0.0, 0.0
  for i = 0, 4 do
    local d = qx * qx + qy * qy
    if d < 0.15 then
      d = 0.15
    end
    qx = math.abs(qx) / d
    qy = math.abs(qy) / d
    qx = qx * 1.9 - (0.9 + 0.3 * phase[i + 1])
    qy = qy * 1.9 - 0.7
    local s = qx * 0.4 + qy * 0.8 + t + i
    ar = (ar + 0.5 + 0.5 * math.cos(s + 0.0)) * 0.85
    ag = (ag + 0.5 + 0.5 * math.cos(s + 2.1)) * 0.85
    ab = (ab + 0.5 + 0.5 * math.cos(s + 4.3)) * 0.85
  end
  ar = ar / 5.0
  ag = ag / 5.0
  ab = ab / 5.0
  local edge = smoothstep(1.6, 0.2, r * 0.5)
  return ar ^ 1.6 * edge, ag ^ 1.6 * edge, ab ^ 1.6 * edge
end

function M.shade(nx, ny, t)
  local tt, drift, phase = shade_terms(t)
  return shade_core(nx, ny, t, tt, drift, phase)
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
  local tt, drift, phase = shade_terms(t)
  local inv_h = 1 / h
  local sample_columns = math.floor((w - 1) / SAMPLE_STEP) + 2
  local sample_rows = math.floor((h - 1) / SAMPLE_STEP) + 2
  local sample_r, sample_g, sample_b = {}, {}, {}
  for sample_y = 1, sample_rows do
    local y = 1 + (sample_y - 1) * SAMPLE_STEP
    local ny = (2 * (y - 0.5) - h) * inv_h
    local row_r, row_g, row_b = {}, {}, {}
    for sample_x = 1, sample_columns do
      local x = 1 + (sample_x - 1) * SAMPLE_STEP
      local nx = ((x - 0.5) - w / 2) * inv_h
      local r, g, b = shade_core(nx, ny, t, tt, drift, phase)
      row_r[sample_x], row_g[sample_x], row_b[sample_x] = r, g, b
    end
    sample_r[sample_y], sample_g[sample_y], sample_b[sample_y] = row_r, row_g, row_b
  end
  local rows = {}
  for y = 1, h do
    local grid_y = (y - 1) / SAMPLE_STEP
    local sample_y = math.floor(grid_y) + 1
    local fy = grid_y - math.floor(grid_y)
    -- Vertical interpolation hoisted per row: one lerp per sample column.
    local row_r, row_g, row_b = {}, {}, {}
    for sx = 1, sample_columns do
      row_r[sx] = sample_r[sample_y][sx] * (1 - fy) + sample_r[sample_y + 1][sx] * fy
      row_g[sx] = sample_g[sample_y][sx] * (1 - fy) + sample_g[sample_y + 1][sx] * fy
      row_b[sx] = sample_b[sample_y][sx] * (1 - fy) + sample_b[sample_y + 1][sx] * fy
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
        local grid_x = (x - 1) / SAMPLE_STEP
        local sample_x = math.floor(grid_x) + 1
        local fx = grid_x - math.floor(grid_x)
        local r = row_r[sample_x] * (1 - fx) + row_r[sample_x + 1] * fx
        local g = row_g[sample_x] * (1 - fx) + row_g[sample_x + 1] * fx
        local b = row_b[sample_x] * (1 - fx) + row_b[sample_x + 1] * fx
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
