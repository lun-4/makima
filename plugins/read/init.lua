local ToolView = require("maki.tool_view")
local shorten_path = require("maki.shorten_path")
local output_limits = require("maki.output_limits")
local helpers = require("read_helpers")

local split_lines = helpers.split_lines

local DESCRIPTION = [[Read a file. Returns contents with line numbers (1-indexed).

- Supports absolute, relative, and ~/ paths.
- **offset** and **limit** are required. Use offset=1 to read from the first line.
- Use limit=0 to read until the end of file (capped at 2000 lines).
- Use the **index** tool or **grep** tool first to find the offset and limit.
- Only read the sections you actually need.
- Use `wc -l` to check total number of lines before reading to decide a reasonable limit.
- Use truncation hints (e.g. "truncated lines X-Y") to continue with the correct offset.
- Do not reread the same range (same file and same offset).
- Prefer grep to locate content instead of scanning full files.
- Call in parallel when reading multiple files.
- Avoid tiny repeated slices - read a larger window if you need more context.]]

local DEFAULT_MAX_OUTPUT_LINES = 2000

local opts = maki.api.register_options(output_limits.extend({
  max_line_bytes = {
    default = output_limits.DEFAULT_MAX_LINE_BYTES,
    min = 80,
    desc = "Truncate lines longer than this many bytes.",
  },
}))

local function read_view_opts(ctx)
  local tol = ctx:tool_output_lines()
  return { max_lines = (tol and tol.read) or 10, keep = "head" }
end

local function apply_highlights(view, lines, ext, prefix)
  local opts = prefix and { prefix = prefix } or nil
  local highlighted = maki.ui.highlight(table.concat(lines, "\n"), ext, opts)
  if not highlighted then
    return
  end
  for i, hl_spans in ipairs(highlighted) do
    local plain = view.all_lines[i]
    if not plain then
      break
    end
    view:update_line(i, { plain[1], table.unpack(hl_spans) })
  end
  view:flush()
end

local function build_file_view(lines, start_line, total_lines, path, ctx, prefix)
  local buf = maki.ui.buf()
  local view = ToolView.new(buf, read_view_opts(ctx))
  local nr_fmt = ToolView.line_nr_fmt(start_line + #lines - 1) .. " "

  for i, line in ipairs(lines) do
    view:append({ { string.format(nr_fmt, start_line + i - 1), "line_nr" }, { line } })
  end

  local trunc_start = start_line + #lines
  if trunc_start <= total_lines then
    view:append({
      {
        string.format(
          "... Truncated %d lines. Use offset=%d to read further.",
          total_lines - trunc_start + 1,
          trunc_start
        ),
        "dim",
      },
    })
  end

  view:finish()

  local ext = path:match("%.([^%.]+)$") or ""
  maki.async.run(function()
    apply_highlights(view, lines, ext, prefix)
  end)

  buf:on("click", function()
    view:toggle()
  end)
  return buf
end

local function read_file(path, offset, limit, ctx)
  local content, err = maki.fs.read(path)
  if not content then
    return { llm_output = "read error: " .. tostring(err), is_error = true }
  end

  local all_lines = split_lines(content)
  local total_lines = #all_lines

  local start = math.max(math.floor(offset), 1)
  local max_lines, max_bytes = output_limits.resolve(opts, ctx)
  max_lines = limit == 0 and max_lines or math.min(limit, max_lines)
  local max_line_bytes = opts.max_line_bytes

  local lines = {}
  for i = start, math.min(start + max_lines - 1, total_lines) do
    lines[#lines + 1] = maki.text.truncate_line(all_lines[i], max_line_bytes)
  end

  ctx:record_read(path)

  local parts = {}
  local nr_fmt = ToolView.line_nr_fmt(start + #lines - 1) .. ": %s"
  for i, line in ipairs(lines) do
    parts[#parts + 1] = string.format(nr_fmt, start + i - 1, line)
  end
  local trunc_start = start + #lines
  local remaining_lines = trunc_start <= total_lines and total_lines - trunc_start + 1 or 0
  local llm_output = maki.text.truncate_file(table.concat(parts, "\n"), max_lines, max_bytes, remaining_lines)

  local shown = #lines
  local annotation = shown < total_lines and string.format("%d of %d lines", shown, total_lines)
    or string.format("%d lines", shown)

  local basename = path:match("([^/]+)$")
  if not ctx:is_instruction_file(basename) then
    local parent = maki.fs.dirname(path)
    if parent then
      local instructions = ctx:find_instructions(parent)
      if #instructions > 0 then
        return {
          llm_output = llm_output,
          body = ToolView.restore(llm_output, read_view_opts(ctx)),
          annotation = annotation,
          instructions = instructions,
        }
      end
    end
  end

  return {
    llm_output = llm_output,
    body = build_file_view(lines, start, total_lines, path, ctx, prefix),
    annotation = annotation,
  }
end

maki.api.register_prompt_hint({
  slot = "tool_usage",
  content = [[
- When using the **read** tool, only read the sections you actually need.
- Use `wc -l` to check total number of lines before reading to decide a reasonable **read** tool limit.]],
})

maki.api.register_tool({
  name = "read",
  kind = "read",
  description = DESCRIPTION,

  schema = {
    type = "object",
    properties = {
      path = {
        type = "string",
        description = "Absolute path to the file",
        required = true,
        alias = "file_path",
      },
      offset = {
        type = "integer",
        description = "Line number to start from (1-indexed). Use 1 for the first line.",
        required = true,
      },
      limit = {
        type = "integer",
        description = "Max number of lines to read. Use 0 to read until end of file (capped at 2000 lines).",
        required = true,
      },
    },
  },

  header = function(input)
    local buf = maki.ui.buf()
    local s = shorten_path(input.path or "")
    local start = input.offset or 1
    if input.limit and input.limit > 0 then
      s = s .. ":" .. start .. "-" .. (start + input.limit - 1)
    else
      s = s .. ":" .. start
    end
    buf:line({ { s, "path" } })
    return buf
  end,

  restore = function(input, output, _is_error, ctx)
    return ToolView.restore(output, read_view_opts(ctx))
  end,

  handler = function(input, ctx)
    local raw = input.path
    if not raw then
      return { llm_output = "error: path is required", is_error = true }
    end
    local path, path_err = ctx:resolve_path(raw)
    if not path then
      return { llm_output = "error: " .. tostring(path_err), is_error = true }
    end
    local meta = maki.fs.metadata(path)
    if not meta then
      return { llm_output = "error: path not found: " .. path, is_error = true }
    end
    if meta.is_dir then
      return { llm_output = "error: path is a directory, use the list tool instead", is_error = true }
    end
    return read_file(path, input.offset, input.limit, ctx)
  end,
})
