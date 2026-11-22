local helpers = require("read_helpers")

local truncate_bytes = helpers.truncate_bytes
local split_lines = helpers.split_lines

local th = require("makima.test_helpers")

local case = th.case
local eq = th.eq

case("split_lines", function()
  local vectors = {
    { "", 0, {} },
    { "hello", 1, { "hello" } },
    { "a\nb", 2, { "a", "b" } },
    { "a\nb\n", 2, { "a", "b" } },
    { "\n\n\n", 3, { "", "", "" } },
    { "a\r\nb\r\n", 2, { "a", "b" } },
  }
  for _, v in ipairs(vectors) do
    local lines = split_lines(v[1])
    eq(#lines, v[2], "count for " .. ("%q"):format(v[1]))
    for i, expected in ipairs(v[3]) do
      eq(lines[i], expected, "line " .. i .. " for " .. ("%q"):format(v[1]))
    end
  end
end)

th.report()
