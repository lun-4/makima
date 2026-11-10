-- Indexer plugin spec orchestrator.
--
-- Per-language cases live in tests/lang/<lang>.lua and run as side effects of
-- require. Add a new language by creating tests/lang/<lang>.lua (use any
-- existing file as a template) and adding a require line below — keep them
-- alphabetized.

local th = require("makima.test_helpers")
local case = th.case
local eq = th.eq
local mktmpdir = function()
  return th.mktmpdir("index_spec")
end
local rmtree = th.rmtree

local dir_listing = require("makima.dir_listing")

local function mock_ctx(path)
  return {
    is_instruction_file = function(self, name)
      local set = { ["AGENTS.md"] = true, ["CLAUDE.md"] = true, ["COPILOT.md"] = true }
      return set[name] or false
    end,
    find_instructions = function()
      return {}
    end,
  }
end

-- integration: directory listing via real filesystem

case("dir_listing_sort_and_filter", function()
  local tmpdir = mktmpdir()
  makima.fs.write(makima.fs.joinpath(tmpdir, "c.txt"), "")
  makima.fs.write(makima.fs.joinpath(tmpdir, "a.txt"), "")
  makima.fs.write(makima.fs.joinpath(tmpdir, "AGENTS.md"), "instructions")
  makima.fs.write(makima.fs.joinpath(tmpdir, "b.txt"), "")
  makima.fs.write(makima.fs.joinpath(tmpdir, "m.txt"), "")
  makima.fs.mkdir(makima.fs.joinpath(tmpdir, "zdir"))
  makima.fs.mkdir(makima.fs.joinpath(tmpdir, "adir"))
  makima.fs.mkdir(makima.fs.joinpath(tmpdir, "idir"))

  local listing, err = dir_listing.list(tmpdir, mock_ctx(tmpdir))
  assert(err == nil, "dir listing should succeed: " .. tostring(err))
  eq(#listing.names, 7)
  eq(listing.names[1], "adir/")
  eq(listing.names[2], "idir/")
  eq(listing.names[3], "zdir/")
  eq(listing.names[4], "a.txt")
  eq(listing.names[5], "b.txt")
  eq(listing.names[6], "c.txt")
  eq(listing.names[7], "m.txt")
  rmtree(tmpdir)
end)
require("tests.indexer_core")
require("tests.lang.bash")
require("tests.lang.bazel")
require("tests.lang.c")
require("tests.lang.c_sharp")
require("tests.lang.containerfile")
require("tests.lang.cpp")
require("tests.lang.css")
require("tests.lang.dart")
require("tests.lang.elixir")
require("tests.lang.gleam")
require("tests.lang.go")
require("tests.lang.hcl")
require("tests.lang.html")
require("tests.lang.java")
require("tests.lang.json")
require("tests.lang.kotlin")
require("tests.lang.lua_lang")
require("tests.lang.make")
require("tests.lang.markdown")
require("tests.lang.nix")
require("tests.lang.php")
require("tests.lang.python")
require("tests.lang.ruby")
require("tests.lang.rust")
require("tests.lang.scala")
require("tests.lang.sql")
require("tests.lang.swift")
require("tests.lang.toml")
require("tests.lang.typescript")
require("tests.lang.v")
require("tests.lang.yaml")
require("tests.lang.zig")

th.report()
