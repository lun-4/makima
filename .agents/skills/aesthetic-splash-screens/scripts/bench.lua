#!/usr/bin/env lua5.1
-- Perf bench for maki splashes, no maki build needed.
--
-- Usage:
--   lua5.1 bench.lua [--dir DIR] [--sizes WxH[,WxH...]] [--budget SECS] [name ...]
--
-- Each name is a splash module name (splash.kaleidoscope) resolved in DIR or a
-- path to a .lua file. Default sizes are 80x24 (reference, matches smoke_test)
-- and 200x79 (a tall terminal).
--
-- Reports average ms/frame per splash per size measured under lua5.1 (no
-- JIT). maki marks plugin envs safe (set_safeenv), so luau's native codegen
-- applies to real renders: expect roughly lua5.1/5 ms/frame there, plus the
-- Rust-side per-segment parse the lua5.1 bench does not see. The script also
-- prints that codegen estimate.
--
-- For real-VM numbers (same CLI, Luau + codegen, safeenv envs), run:
--   cargo run -p maki-lua --example splash_bench -- --dir DIR name...

local DIR = "."
local SIZES = { { 80, 24 }, { 200, 79 } }
local BUDGET = 0.5

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
  elseif arg[i] == "--budget" then
    i = i + 1
    BUDGET = tonumber(arg[i])
  else
    names[#names + 1] = arg[i]
  end
  i = i + 1
end

if #names == 0 then
  io.stderr:write("usage: lua5.1 bench.lua [--dir DIR] name [name...]\n")
  os.exit(2)
end

package.path = DIR .. "/?.lua;" .. package.path

_G.maki = {
  ui = { theme_color = function() return nil end },
  version = function() return { current = "0.0.0-test" } end,
  api = {
    set_slot = function() end,
    create_autocmd = function() end,
  },
}

local function load_splash(name)
  local m
  if name:match("%.lua$") then
    m = assert(loadfile(name))()
  else
    local ok, mod = pcall(require, name)
    if not ok then
      mod = require("splash." .. name)
    end
    m = mod
  end
  assert(type(m) == "table" and type(m.render) == "function", name .. ": no M.render")
  return m
end

local mods = {}
for _, name in ipairs(names) do
  mods[name] = load_splash(name)
end

local function measure(m, w, h)
  m.render(w, h, 1.0, 1.0)
  local frames = 0
  local t0 = os.clock()
  local t1 = t0 + BUDGET
  local t = 1.0
  while os.clock() < t1 do
    t = t + 0.033
    m.render(w, h, t, 1.0)
    frames = frames + 1
  end
  local elapsed = os.clock() - t0
  return elapsed / frames * 1000
end

local header = string.format("%-16s %8s %12s %10s %10s %8s", "splash", "size", "lua5.1 ms", "luau ms*", "est fps", "fps@60")
io.write(header, "\n")
io.write(string.rep("-", #header), "\n")
for _, name in ipairs(names) do
  local m = mods[name]
  for _, s in ipairs(SIZES) do
    local w, h = s[1], s[2]
    local ms = measure(m, w, h)
    local luau_ms = ms / 5
    io.write(
      string.format(
        "%-16s %4dx%-3d %11.2f %10.2f %9.1f  %s",
        name,
        w,
        h,
        ms,
        luau_ms,
        1000 / luau_ms,
        luau_ms <= 16.7 and "ok" or "SLOW"
      ),
      "\n"
    )
  end
end
io.write("* luau-codegen estimate = lua5.1 / 5 (maki marks plugin envs safe, so native codegen applies); real frames also pay a Rust-side per-segment parse; fps@60 means the frame fits in a 60hz tick.\n")