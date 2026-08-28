-- Bundled splash, part of the makima distribution.
-- Require from init.lua with:   local splash = require("splash.matrix")
-- The module returns M with M.description and M.render(w, h, t, fade) and does not activate itself.
--
-- Matrix rain splash. Green falling code with per-column state stepped by
-- frame dt; resets when the home screen shows again (SplashShown). The
-- renderer is pull-driven: Rust pulls a frame each tick while the splash
-- animates, so it must not call blocking maki APIs.

local GLYPHS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
local TRAIL = 6
local M = {}
M.description = "Green falling-code rain with persistent animated trails."

local state = { cols = {}, last_t = nil }

local function reset(w)
  local cols = {}
  for x = 1, w do
    cols[x] = {
      y = math.random(-40, 0),
      speed = 2.5 + math.random() * 4,
    }
  end
  state.cols[w] = cols
end

maki.api.create_autocmd("SplashShown", {
  callback = function()
    state.cols = {}
    state.last_t = nil
  end,
})

local function glyph()
  local i = math.random(1, #GLYPHS)
  return string.sub(GLYPHS, i, i)
end

-- Blend a "#rrggbb" color toward black by `amount` (0..1). The plugin owns
-- fade, so during the entry fade the whole scene ramps in with it.
local function shade(hex, amount)
  local r = tonumber(string.sub(hex, 2, 3), 16)
  local g = tonumber(string.sub(hex, 4, 5), 16)
  local b = tonumber(string.sub(hex, 6, 7), 16)
  return string.format(
    "#%02x%02x%02x",
    math.floor(r * amount + 0.5),
    math.floor(g * amount + 0.5),
    math.floor(b * amount + 0.5)
  )
end

-- grid[row][col] = { ch, fg } for every trail cell, then coalesced to segments.
function M.render(w, h, t, fade)
  if w == 0 or h == 0 then
    return {}
  end
  if not state.cols[w] then
    reset(w)
  end
  local dt = state.last_t and math.max(0, t - state.last_t) or 0
  state.last_t = t
  local cols = state.cols[w]

  for x = 1, w do
    cols[x].y = cols[x].y + cols[x].speed * dt
    -- once the head and its trail have cleared the bottom, start a fresh
    -- drop near the top so the column never drains empty
    if cols[x].y > h + TRAIL then
      cols[x].y = -math.random(1, 10)
    end
  end

  local grid = {}
  for _ = 1, h do
    grid[#grid + 1] = {}
  end
  for x = 1, w do
    local head = math.floor(cols[x].y)
    for k = 0, TRAIL do
      local yy = head - k
      if yy >= 1 and yy <= h then
        local base = "#00ff41"
        if k == 0 then
          base = "#c8ffd0"
        elseif k > 1 then
          base = "#005c17"
        end
        grid[yy][x] = { ch = glyph(), fg = shade(base, fade) }
      end
    end
  end

  local ver = "v" .. maki.version().current
  if h >= 1 and w - #ver >= 1 then
    local vx = w - #ver + 1
    for i = 1, #ver do
      grid[1][vx + i - 1] = { ch = string.sub(ver, i, i), fg = shade("#00ff41", fade) }
    end
  end

  local rows = {}
  for y = 1, h do
    local segs = {}
    local buf = {}
    local fg = nil
    local function flush()
      if #buf > 0 then
        segs[#segs + 1] = { glyphs = table.concat(buf), style = { fg = fg or "#000000", bg = "#000000", bold = false } }
        buf = {}
      end
    end
    for x = 1, w do
      local cell = grid[y][x]
      local next_fg = (cell and cell.fg) or "#000000"
      if next_fg ~= fg then
        flush()
        fg = next_fg
      end
      buf[#buf + 1] = (cell and cell.ch) or " "
    end
    flush()
    rows[y] = segs
  end
  return rows
end

return M
