-- Bundled splash, part of the maki distribution.
-- Require from init.lua with:   local splash = require("splash.caustics")
-- The module returns M with M.description and M.render(w, h, t, fade) and does not activate itself.
--
-- Caustics splash. Software port of the WGSL "Caustics" shader from

local RAMP = " .:-=+*#%@"
local SAMPLE_STEP = 4
local M = {}
M.description = "Deep-water light patterns shimmering across the screen."

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

-- Smooth value noise; the four lattice hashes are inlined so a single
-- call replaces five (h21 x4 + n2).
function M.n2(px, py)
  local ix = math.floor(px)
  local iy = math.floor(py)
  local fx = px - ix
  local fy = py - iy
  local ux = fx * fx * (3.0 - 2.0 * fx)
  local uy = fy * fy * (3.0 - 2.0 * fy)
  local base_x = ix * 127.1
  local base_y = iy * 311.7
  local s1 = math.sin(base_x + base_y) * 43758.5453
  local a = s1 - math.floor(s1)
  local s2 = math.sin(base_x + 127.1 + base_y) * 43758.5453
  local b = s2 - math.floor(s2)
  local s3 = math.sin(base_x + base_y + 311.7) * 43758.5453
  local c = s3 - math.floor(s3)
  local s4 = math.sin(base_x + 127.1 + base_y + 311.7) * 43758.5453
  local d = s4 - math.floor(s4)
  return a + (b - a) * ux + (c - a) * uy + (a - b - c + d) * ux * uy
end

-- Fragment shade for isotropic coords (nx, ny); returns r, g, b in [0, 1].
function M.shade(nx, ny, t)
  local ux = nx * 2.0
  local uy = ny * 2.0
  local tt = t * 0.6
  local qx, qy = ux, uy
  local c = 0.0
  for i = 1, 4 do
    local fi = i
    local wx = M.n2(qx * fi + tt * 0.3, qy * fi + tt * 0.3)
    local wy = M.n2(qx * fi - tt * 0.2, qy * fi - tt * 0.2)
    qx = qx + wx * 0.7
    qy = qy + wy * 0.7
    local wv = 1.0 - math.abs(math.sin((qx + qy) * (2.0 + fi) - tt))
    wv = wv * wv
    wv = wv * wv
    c = c + wv * wv / fi
  end
  local deep_r, deep_g, deep_b = 0.0, 0.08, 0.18
  local lite_r, lite_g, lite_b = 0.4, 0.95, 1.1
  local vig = 1.0 - 0.35 * math.sqrt(ux * ux + uy * uy) * 0.5
  return (deep_r + lite_r * c * 0.9) * vig, (deep_g + lite_g * c * 0.9) * vig, (deep_b + lite_b * c * 0.9) * vig
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
  local x_scale = 1 / h
  local sample_r, sample_g, sample_b = {}, {}, {}
  local sample_columns = math.floor((w - 1) / SAMPLE_STEP) + 2
  local sample_rows = math.floor((h - 1) / SAMPLE_STEP) + 2
  for sample_y = 1, sample_rows do
    sample_r[sample_y], sample_g[sample_y], sample_b[sample_y] = {}, {}, {}
    local y = 1 + (sample_y - 1) * SAMPLE_STEP
    local ny = (2 * (y - 0.5) - h) / h
    for sample_x = 1, sample_columns do
      local x = 1 + (sample_x - 1) * SAMPLE_STEP
      local r, g, b = M.shade(((x - 0.5) - w / 2) * x_scale, ny, t)
      sample_r[sample_y][sample_x] = r
      sample_g[sample_y][sample_x] = g
      sample_b[sample_y][sample_x] = b
    end
  end
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
