-- Bundled splash, part of the maki distribution.
-- Require from init.lua with:   local splash = require("splash.metaballs")
-- The module returns M with M.description and M.render(w, h, t, fade) and does not activate itself.
--
-- Metaballs splash. Software port of the WGSL "Metaballs" shader from

local RAMP = " .:-=+*#%@"
local M = {}
M.description = "Glowing metaballs that merge and flow together."

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
-- drops the per-cell *255/31 round-trip. Metaballs keeps exact-key runs because
-- its thin gridlines are high-frequency details that tolerance merging drops.
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

local function fract(x)
  return x - math.floor(x)
end

local function ball(nx, ny, bx, by, k)
  local dx = nx - bx
  local dy = ny - by
  return k / math.sqrt(dx * dx + dy * dy + 1e-4)
end

-- Smooth metaball field without the gridline overlay; blobs are the four
-- { bx, by, k } centers hoisted per frame.
local function field_shade(nx, ny, blobs)
  local v = 0.0
  for i = 1, 4 do
    local b = blobs[i]
    v = v + ball(nx, ny, b[1], b[2], b[3])
  end
  local edge = smoothstep(1.15, 1.25, v)
  local core = smoothstep(1.25, 2.4, v)
  local r = 0.03 + (0.1 - 0.03) * edge
  local g = 0.02 + (0.4 - 0.02) * edge
  local b = 0.06 + (0.9 - 0.06) * edge
  r = r + (0.6 - r) * core
  g = g + (0.9 - g) * core
  b = b + (1.0 - b) * core
  local glow = math.exp(-math.abs(v - 1.2) * 3.0) * 0.8
  return r + 0.3 * glow, g + 0.7 * glow, b + 1.0 * glow
end

local function gridline(nx, ny)
  local gx = math.abs(fract(nx * 8.0) - 0.5)
  local gy = math.abs(fract(ny * 8.0) - 0.5)
  return 0.02 * smoothstep(0.48, 0.5, gx > gy and gx or gy)
end

local function blob_positions(t)
  return {
    { math.sin(t * 0.7) * 0.7, math.cos(t * 0.9) * 0.7, 0.35 },
    { math.cos(t * 1.1) * 0.8, math.sin(t * 0.6) * 0.8, 0.30 },
    { math.sin(t * 0.5 + 2.0) * 0.5, math.cos(t * 0.8 + 1.0) * 0.5, 0.25 },
    { math.sin(t * 0.33 + 4.0) * 0.9, math.cos(t * 0.41 + 2.0) * 0.6, 0.22 },
  }
end

-- Fragment shade for isotropic coords (nx, ny); returns r, g, b in [0, 1].
function M.shade(nx, ny, t)
  local r, g, b = field_shade(nx, ny, blob_positions(t))
  local line = gridline(nx, ny)
  return r + line, g + line, b + line
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
  local blobs = blob_positions(t)
  local inv_h = 1 / h
  local rows = {}
  for y = 1, h do
    local ny = (2 * (y - 0.5) - h) * inv_h
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
        local nx = ((x - 0.5) - w / 2) * inv_h
        local r, g, b = field_shade(nx, ny, blobs)
        local line = gridline(nx, ny)
        glyph = ramp_glyph((0.2126 * (r + line) + 0.7152 * (g + line) + 0.0722 * (b + line)) * f)
        qr, qg, qb, style = cell_style(r + line, g + line, b + line, f)
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
