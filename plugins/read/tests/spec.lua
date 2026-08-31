local helpers = require("read_helpers")

local source_lines = helpers.source_lines
local split_lines = helpers.split_lines
local utf8_prefix = helpers.utf8_prefix

local th = require("maki.test_helpers")

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

case("source_lines_coordinates", function()
  local vectors = {
    { "", {} },
    { "a", { { "a", 0, 1, 1, false } } },
    { "a\n", { { "a", 0, 1, 2, true } } },
    { "\n\n", { { "", 0, 0, 1, true }, { "", 1, 1, 2, true } } },
    { "a\r\nb", { { "a", 0, 1, 3, true }, { "b", 3, 4, 4, false } } },
  }
  for _, vector in ipairs(vectors) do
    local lines = source_lines(vector[1])
    eq(#lines, #vector[2], "coordinate line count")
    for i, expected in ipairs(vector[2]) do
      local line = lines[i]
      eq(line.text, expected[1], "text " .. i)
      eq(line.start_byte, expected[2], "start " .. i)
      eq(line.text_end_byte, expected[3], "text end " .. i)
      eq(line.terminator_end_byte, expected[4], "terminator end " .. i)
      eq(line.terminated, expected[5], "terminated " .. i)
    end
  end
end)

case("utf8_prefix_ends_at_character_boundary", function()
  local prefix, bytes = utf8_prefix("aé日", 4)
  eq(prefix, "aé")
  eq(bytes, 3)
end)

th.report()
