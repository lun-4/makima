#!/usr/bin/env lua5.1
-- Contract + perf smoke test for makima splashes, no makima build needed.
--
-- Usage:
--   lua5.1 smoke_test.lua [--dir DIR] [--sizes WxH[,WxH...]] name [name...]
--
-- Each name is a splash module name (kaleidoscope) resolved in DIR
-- (default ~/.config/makima/lua), or a path to a .lua file.
--
-- Checks per splash, per size, per time step:
--   * render returns exactly h rows
--   * each row's seg.glyphs concatenate to exactly w terminal cells
--     (UTF-8 aware: multi-byte glyphs count once)
--   * the frame is not entirely blank
-- Then prints ms/frame at 80x24 (lua5.1, no jit: makima's luau-jit is ~5x
-- faster, so treat this as a pessimistic bound; 2-3 ms is the norm for
-- full-cell splashes, up to ~20 ms is tolerable).

local DIR = os.getenv("HOME") .. "/.config/makima/lua"
local SIZES = { { 80, 24 }, { 40, 12 }, { 120, 40 } }

local names = {}
local i = 1
while i <= #arg do
  if arg[i] == "--dir" then
    i = i + 1
    DIR = arg[i]
  elseif arg[i] == "--sizes" then
    i = i + 1
    SIZES = {}
    for s in string.gmatch(arg[i], "[^,]+") do
      local w, h = s:match("(%d+)x(%d+)")
      SIZES[#SIZES + 1] = { tonumber(w), tonumber(h) }
    end
  else
    names[#names + 1] = arg[i]
  end
  i = i + 1
end

if #names == 0 then
  io.stderr:write("usage: lua5.1 smoke_test.lua [--dir DIR] name [name...]\n")
  os.exit(2)
end

package.path = DIR .. "/?.lua;" .. package.path

_G.makima = {
  ui = { theme_color = function() return nil end },
  version = function() return { current = "0.0.0-test" } end,
  api = {
    set_slot = function() end,
    create_autocmd = function() end,
  },
}

local function cell_count(s)
  local _, conts = s:gsub("[\128-\191]", "")
  return #s - conts
end

local function check_rows(name, rows, w, h, t)
  assert(type(rows) == "table", name .. ": rows not a table")
  assert(#rows == h, string.format("%s: %d rows, expected %d (t=%s)", name, #rows, h, t))
  local any = false
  for y = 1, h do
    local total = 0
    local glyphs = {}
    for _, seg in ipairs(rows[y]) do
      assert(type(seg.glyphs) == "string", name .. ": seg.glyphs not a string")
      assert(seg.style ~= nil, name .. ": seg.style is nil")
      total = total + cell_count(seg.glyphs)
      glyphs[#glyphs + 1] = seg.glyphs
    end
    assert(total == w, string.format("%s: row %d is %d cells, expected %d (t=%s)", name, y, total, w, t))
    if table.concat(glyphs):match("%S") then
      any = true
    end
  end
  assert(any, string.format("%s: frame entirely blank (t=%s)", name, t))
end

local function load_splash(name)
  if name:match("%.lua$") then
    local chunk = assert(loadfile(name))
    return chunk()
  end
  return require(name)
end

local mods = {}
for _, name in ipairs(names) do
  local m = load_splash(name)
  assert(type(m) == "table" and type(m.render) == "function", name .. ": no M.render")
  mods[name] = m
  for _, s in ipairs(SIZES) do
    local w, h = s[1], s[2]
    for step = 0, 30 do
      local t = 0.1 + step * 0.17
      local ok, err = pcall(check_rows, name, m.render(w, h, t, 1.0), w, h, t)
      if not ok then
        io.write("FAIL ", err, "\n")
        os.exit(1)
      end
    end
    -- also exercise the fade range entry
    local ok, err = pcall(check_rows, name, m.render(w, h, 0.05, 0.3), w, h, 0.05)
    if not ok then
      io.write("FAIL ", err, "\n")
      os.exit(1)
    end
  end
  io.write("ok ", name, "\n")
end

io.write("\n-- perf: 80x24 single frame, lua5.1 (pessimistic bound) --\n")
for _, name in ipairs(names) do
  local m = mods[name]
  m.render(80, 24, 1.0, 1.0)
  local t0 = os.clock()
  local n = 5
  for step = 1, n do
    m.render(80, 24, 1.0 + step * 0.033, 1.0)
  end
  io.write(string.format("%-20s %7.1f ms/frame\n", name, (os.clock() - t0) / n * 1000))
end
io.write("ALL OK\n")
