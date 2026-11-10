#!/usr/bin/env lua5.1
-- Render one splash frame as plain text (glyphs only) to eyeball composition
-- without starting makima. Colors are invisible here; for background-color
-- effects (fire) this looks blank by design.
--
-- Usage: lua5.1 dump_frame.lua <splash.lua|name> [w] [h] [t] [fade]
--   name (no .lua suffix) is resolved in ~/.config/makima/lua.

local target = arg[1]
if not target then
  io.stderr:write("usage: lua5.1 dump_frame.lua <splash.lua|name> [w] [h] [t] [fade]\n")
  os.exit(2)
end
local w = tonumber(arg[2]) or 80
local h = tonumber(arg[3]) or 24
local t = tonumber(arg[4]) or 2.0
local fade = tonumber(arg[5]) or 1.0

package.path = os.getenv("HOME") .. "/.config/makima/lua/?.lua;" .. package.path

_G.makima = {
  ui = { theme_color = function() return nil end },
  version = function() return { current = "0.0.0-test" } end,
  api = {
    set_slot = function() end,
    create_autocmd = function() end,
  },
}

local m
if target:match("%.lua$") then
  m = assert(loadfile(target))()
else
  m = require(target)
end

local rows = m.render(w, h, t, fade)
for y = 1, #rows do
  local parts = {}
  for _, seg in ipairs(rows[y]) do
    parts[#parts + 1] = seg.glyphs
  end
  io.write(table.concat(parts), "|\n")
end
