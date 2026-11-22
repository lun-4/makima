local list_helpers = require("list_helpers")
local th = require("makima.test_helpers")

local case = th.case
local eq = th.eq
local mktmpdir = function()
  return th.mktmpdir("list_spec")
end
local rmtree = th.rmtree

local PATH_REQUIRED_MSG = "error: path is required"

local function mock_ctx(instructions)
  return {
    resolve_path = function(_self, path)
      return makima.fs.abspath(path), nil
    end,
    is_instruction_file = function(_self, name)
      local set = { ["AGENTS.md"] = true, ["CLAUDE.md"] = true, ["COPILOT.md"] = true }
      return set[name] or false
    end,
    find_instructions = function()
      return instructions or {}
    end,
  }
end

case("handler_requires_path", function()
  local result = list_helpers.handler({}, mock_ctx())
  eq(result.is_error, true)
  eq(result.llm_output, PATH_REQUIRED_MSG)
end)

case("handler_errors_on_missing_path", function()
  local tmpdir = mktmpdir()
  local missing = makima.fs.joinpath(tmpdir, "missing")
  local result = list_helpers.handler({ path = missing }, mock_ctx())
  eq(result.is_error, true)
  eq(result.llm_output, "error: path not found: " .. missing)
  rmtree(tmpdir)
end)

case("handler_errors_on_file_path", function()
  local tmpdir = mktmpdir()
  local file = makima.fs.joinpath(tmpdir, "file.txt")
  makima.fs.write(file, "content")
  local result = list_helpers.handler({ path = file }, mock_ctx())
  eq(result.is_error, true)
  eq(result.llm_output, "error: path is not a directory: " .. file)
  rmtree(tmpdir)
end)

case("handler_lists_sorted_and_filtered", function()
  local tmpdir = mktmpdir()
  makima.fs.write(makima.fs.joinpath(tmpdir, "b.txt"), "")
  makima.fs.write(makima.fs.joinpath(tmpdir, "a.txt"), "")
  makima.fs.write(makima.fs.joinpath(tmpdir, "AGENTS.md"), "instructions")
  makima.fs.mkdir(makima.fs.joinpath(tmpdir, "zdir"))
  makima.fs.mkdir(makima.fs.joinpath(tmpdir, "adir"))

  local result = list_helpers.handler({ path = tmpdir }, mock_ctx())
  eq(result.is_error, nil)
  eq(result.llm_output, "adir/\nzdir/\na.txt\nb.txt")
  eq(result.annotation, "4 entries")
  eq(result.instructions, nil)
  rmtree(tmpdir)
end)

case("handler_propagates_instructions", function()
  local tmpdir = mktmpdir()
  makima.fs.write(makima.fs.joinpath(tmpdir, "a.txt"), "")
  local instructions = { makima.fs.joinpath(tmpdir, "AGENTS.md") }
  local result = list_helpers.handler({ path = tmpdir }, mock_ctx(instructions))
  eq(result.is_error, nil)
  eq(result.instructions, instructions)
  rmtree(tmpdir)
end)

th.report()
