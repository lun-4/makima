-- Splash template. Copy this file to your new splash name, set LABEL,
-- implement M.shade (full-cell splashes) or draw into the grid (sparse splashes),
-- and adjust the tiny-area guard. Delete the helpers you don't use (this
-- file ships the full toolbox: atan2, smoothstep, hash01, ...). Conventions
-- live in .agents/skills/aesthetic-splash-screens/SKILL.md; scene composition
-- building blocks live in references/recipes.md.

local LABEL = "mysplash"
local RAMP = " .:-=+*#%@"
local M = {}

local function theme_rgb(name)
  local c = makima.ui.theme_color(name)
  if not c then
    return nil
  end
  return {
    tonumber(string.sub(c, 2, 3), 16),
    tonumber(string.sub(c, 4, 5), 16),
    tonumber(string.sub(c, 6, 7), 16),
  }
end

local BG_HEX, FG
local style_cache = {}

local function refresh_colors()
  BG_HEX = makima.ui.theme_color("background")
  FG = theme_rgb("foreground")
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

-- Style cache: always fetch styles through here and reuse the returned table.
-- The renderer coalesces runs of identical tables, so identity matters.
local function color(hex)
  local s = style_cache[hex]
  if not s then
    s = { fg = hex, bold = false }
    style_cache[hex] = s
  end
  return s
end

local W, H

local function new_grid()
  local cell_style = BG_HEX and color(BG_HEX) or { bold = false }
  local grid = {}
  for y = 1, H do
    local row = {}
    for x = 1, W do
      row[x] = { glyph = " ", style = cell_style }
    end
    grid[y] = row
  end
  return grid
end

local function place_text(grid, row, x, text, st)
  if row < 1 or row > H then
    return
  end
  local r = grid[row]
  for i = 1, #text do
    local xx = x + i - 1
    if xx >= 1 and xx <= W then
      r[xx] = { glyph = string.sub(text, i, i), style = st }
    end
  end
end

local function build_rows(grid)
  local rows = {}
  for y = 1, H do
    local segs = {}
    local buf = {}
    local cur
    local function flush()
      if #buf > 0 then
        segs[#segs + 1] = { glyphs = table.concat(buf), style = cur }
        buf = {}
      end
    end
    for x = 1, W do
      local cell = grid[y][x]
      if cell.style ~= cur then
        flush()
        cur = cell.style
      end
      buf[#buf + 1] = cell.glyph
    end
    flush()
    rows[y] = segs
  end
  return rows
end

local function flat_rows(w, h, st)
  local rows = {}
  for y = 1, h do
    rows[y] = { { glyphs = string.rep(" ", w), style = st } }
  end
  return rows
end

-- Isotropic coordinates: terminal cells are ~2x taller than wide, so x
-- advances at half the y rate. nx, ny are in screen-half-height units, y down.
local function isotropic(px, py)
  return (px - W / 2) / H, (2 * py - H) / H
end

-- 4-quadrant atan2 without math.atan2 (works on lua5.1 and luau).
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

-- Quantized full-color style (5 bits/channel keeps the style cache bounded).
local function shade_style(r, g, b, f)
  local function q(v)
    if v < 0 then
      v = 0
    elseif v > 1 then
      v = 1
    end
    return math.floor(v * 31 + 0.5) * 255 / 31
  end
  return color(string.format("#%02x%02x%02x", math.floor(q(r * f) + 0.5), math.floor(q(g * f) + 0.5), math.floor(q(b * f) + 0.5)))
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

-- Deterministic hash -> [0, 1). Use instead of math.random for anything that
-- must be a pure function of position/index + t (starfields, schedules).
local function hash01(i, salt)
  return ((i * 2654435761 + salt * 40503) % 4294967296) / 4294967296
end

-- Full-cell splashes: return r, g, b in [0, 1] for isotropic coords (nx, ny).
-- Sparse splashes: skip this and draw into the grid directly in M.render.
function M.shade(nx, ny, t)
  local r = math.sqrt(nx * nx + ny * ny)
  local v = 0.5 + 0.5 * math.sin(r * 12.0 - t * 2.0)
  return v * 0.3, v * 0.7, v
end

function M.render(w, h, t, fade)
  refresh_colors()
  W, H = w, h
  local f = fade or 1.0
  if w < 8 or h < 6 then
    return flat_rows(w, h, { bold = false })
  end
  local grid = new_grid()
  for y = 1, h do
    local row = grid[y]
    for x = 1, w do
      local nx, ny = isotropic(x - 0.5, y - 0.5)
      local r, g, b = M.shade(nx, ny, t)
      row[x] = {
        glyph = ramp_glyph(0.2126 * r * f + 0.7152 * g * f + 0.0722 * b * f),
        style = shade_style(r, g, b, f),
      }
    end
  end
  place_text(grid, H - 1, math.floor((W - #LABEL) / 2) + 1, LABEL, FG and color(rgb_to_hex(FG, 0.5 * f)) or { bold = false })
  place_text(grid, 1, W - #("v" .. makima.version().current) + 1, "v" .. makima.version().current, FG and color(rgb_to_hex(FG, 0.4 * f)) or { bold = false })
  return build_rows(grid)
end

makima.api.set_slot("splash.render", function(prev, w, h, t, fade)
  return M.render(w, h, t, fade)
end)

return M
