-- Bundled splash, part of the makima distribution.
-- Require from init.lua with:   local splash = require("splash.aurora")
-- The module returns M with M.description and M.render(w, h, t, fade) and does not activate itself.
--
-- Aurora splash. Software port of the WGSL "Aurora" shader from

local RAMP = " .:-=+*#%@"
local SAMPLE_STEP = 4
local M = {}
M.description = "Drifting northern lights over a dark gradient."

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

-- 5-bit-per-channel quantized style keyed by number, so string.format only
-- runs on cache miss; keeps the style cache bounded.
local MERGE_TOL = 1

-- 5-bit keyed style: quantize to 0..31 per channel (so a `31`-step grid maps
-- back to 8-bit hex on cache miss) and key on the components directly. This
-- drops the per-cell *255/31 round-trip, and `MERGE_TOL` (one step ~= 8/255)
-- lets the row builder merge runs whose colors drift by a step; that cuts
-- segment count (and Rust-side parse cost) several-fold with no visible
-- banding.
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

local function h21(px, py)
  local s = math.sin(px * 127.1 + py * 311.7) * 43758.5453
  return s - math.floor(s)
end

-- Smooth value noise.
function M.n2(px, py)
  local ix = math.floor(px)
  local iy = math.floor(py)
  local fx = px - ix
  local fy = py - iy
  local ux = fx * fx * (3.0 - 2.0 * fx)
  local uy = fy * fy * (3.0 - 2.0 * fy)
  local a = h21(ix, iy)
  local b = h21(ix + 1, iy)
  local c = h21(ix, iy + 1)
  local d = h21(ix + 1, iy + 1)
  return a + (b - a) * ux + (c - a) * uy + (a - b - c + d) * ux * uy
end

-- Per-column band parameters (center, spread, color, shimmer).
function M.column_bands(ux, t)
  local tt = t * 0.5
  local yc = {}
  local spread = {}
  local cr = {}
  local cg = {}
  local cb = {}
  local shimmer = {}
  for i = 0, 4 do
    local fi = tonumber(i)
    local sx = ux * 3.0 + fi * 1.7
    local wave = M.n2(sx * 1.2 + tt * 0.7, fi * 3.1) * 0.5 + M.n2(sx * 0.4 - tt * 0.3, fi * 7.7) * 0.5
    yc[i + 1] = 0.2 + wave * 0.45 + fi * 0.07
    spread[i + 1] = 10.0 + fi * 6.0
    local hue = 0.45 + 0.25 * math.sin(fi * 1.3 + tt * 0.2)
    cr[i + 1] = hue ^ 2 * 0.9
    cg[i + 1] = 0.9
    cb[i + 1] = (1.0 - hue) ^ 1.5
    shimmer[i + 1] = 0.25 + 0.25 * M.n2(sx * 5.0, tt)
  end
  return { yc = yc, spread = spread, cr = cr, cg = cg, cb = cb, shimmer = shimmer }
end

-- Fragment shade for normalized screen coords (ux, uy in [0, 1], y down).
function M.shade(ux, uy, t, cols)
  if not cols then
    cols = M.column_bands(ux, t)
  end
  local r, g, b = 0.0, 0.0, 0.0
  for i = 1, 5 do
    local band = math.exp(-math.abs(uy - cols.yc[i]) * cols.spread[i]) * cols.shimmer[i]
    r = r + cols.cr[i] * band
    g = g + cols.cg[i] * band
    b = b + cols.cb[i] * band
  end
  local sky = 1.0 - uy
  return r + 0.03 * sky, g + 0.03 * sky, b + 0.07 * sky
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
  local sample_columns = math.floor((w - 1) / SAMPLE_STEP) + 2
  local sample_rows = math.floor((h - 1) / SAMPLE_STEP) + 2
  local band_cache = {}
  for sample_x = 1, sample_columns do
    local x = 1 + (sample_x - 1) * SAMPLE_STEP
    band_cache[sample_x] = M.column_bands((x - 0.5) / w, t)
  end
  local sample_r, sample_g, sample_b = {}, {}, {}
  for sample_y = 1, sample_rows do
    local y = 1 + (sample_y - 1) * SAMPLE_STEP
    local uy = (y - 0.5) / h
    local row_r, row_g, row_b = {}, {}, {}
    for sample_x = 1, sample_columns do
      local x = 1 + (sample_x - 1) * SAMPLE_STEP
      local r, g, b = M.shade((x - 0.5) / w, uy, t, band_cache[sample_x])
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
