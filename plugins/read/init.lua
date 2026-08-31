local ToolView = require("maki.tool_view")
local shorten_path = require("maki.shorten_path")
local output_limits = require("maki.output_limits")
local helpers = require("read_helpers")

local source_lines = helpers.source_lines
local utf8_prefix = helpers.utf8_prefix

local LINE_MARKER = "[line truncated]"
local FILE_MARKER = "[file truncated]"

local DESCRIPTION = [[Read a file with bounded output.

Use exactly one complete mode pair:
- Line mode: **offset** and **limit**. Offset is 1-indexed; limit=0 reads to the bounded end.
- Byte mode: **byte_offset** and **byte_limit**. Offsets are zero-based UTF-8 boundaries.

Line mode returns numbered lines and preserves existing truncation markers. Byte mode returns exact source bytes with their source range and total file size, and can recover omitted bytes reported by a mutation error. Supports absolute, relative, and ~/ paths. Prefer index or grep to locate relevant sections, and avoid rereading the same range.]]

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

local function read_content(path, ctx)
  local lease, lease_err = ctx:begin_read(path)
  if lease_err then
    return nil, nil, lease_err
  end
  local content, err = maki.fs.read(path)
  if not content then
    return nil, nil, "read error: " .. tostring(err)
  end
  return content, lease
end

local function line_marker(remaining)
  return string.format("[file truncated, %d lines remaining]", remaining)
end

local function read_lines(path, offset, limit, ctx)
  local content, lease, err = read_content(path, ctx)
  if not content then
    return { llm_output = err, is_error = true }
  end

  local all_lines = source_lines(content)
  local total_lines = #all_lines
  local start = math.max(math.floor(offset), 1)
  local max_lines, max_bytes = output_limits.resolve(opts, ctx)
  local requested = limit == 0 and max_lines or math.min(limit, max_lines)
  local last = math.min(start + requested - 1, total_lines)
  local width = #(tostring(math.max(last, start)))
  local parts, lines, intervals = {}, {}, {}
  local output_bytes = 0

  for line_nr = start, last do
    local source = all_lines[line_nr]
    local rendered, covered_end
    if #source.text > opts.max_line_bytes then
      local prefix, prefix_bytes = utf8_prefix(source.text, math.max(opts.max_line_bytes - #LINE_MARKER, 0))
      rendered = prefix .. LINE_MARKER
      covered_end = source.start_byte + prefix_bytes
    else
      rendered = source.text
      covered_end = source.terminator_end_byte
    end
    local fragment = string.format("%" .. width .. "d: %s", line_nr, rendered)
    local added = #fragment + (#parts > 0 and 1 or 0)
    if #parts >= max_lines or output_bytes + added > max_bytes then
      break
    end
    parts[#parts + 1] = fragment
    lines[#lines + 1] = rendered
    output_bytes = output_bytes + added
    if covered_end > source.start_byte then
      intervals[#intervals + 1] = { source.start_byte, covered_end }
    end
  end

  local next_line = start + #parts
  local remaining = next_line <= total_lines and total_lines - next_line + 1 or 0
  local llm_output = table.concat(parts, "\n")
  if remaining > 0 then
    if llm_output ~= "" then
      llm_output = llm_output .. "\n\n"
    end
    llm_output = llm_output .. line_marker(remaining)
  end

  local _, record_err = ctx:record_read_ranges(path, content, intervals, lease)
  if record_err then
    return { llm_output = "read provenance error: " .. tostring(record_err), is_error = true }
  end

  local shown = #lines
  local annotation = shown < total_lines and string.format("%d of %d lines", shown, total_lines)
    or string.format("%d lines", shown)
  local result = {
    llm_output = llm_output,
    body = build_file_view(lines, start, total_lines, path, ctx),
    annotation = annotation,
  }

  local basename = path:match("([^/]+)$")
  if not ctx:is_instruction_file(basename) then
    local parent = maki.fs.dirname(path)
    if parent then
      local instructions = ctx:find_instructions(parent)
      if #instructions > 0 then
        result.instructions = instructions
      end
    end
  end
  return result
end

local function read_bytes(path, byte_offset, byte_limit, ctx)
  if byte_offset < 0 or byte_limit < 0 then
    return { llm_output = "error: byte_offset and byte_limit must be non-negative", is_error = true }
  end
  local content, lease, err = read_content(path, ctx)
  if not content then
    return { llm_output = err, is_error = true }
  end
  if byte_offset > #content then
    return {
      llm_output = string.format("error: byte_offset %d is beyond file size %d", byte_offset, #content),
      is_error = true,
    }
  end
  local first = content:byte(byte_offset + 1)
  if first and first >= 0x80 and first < 0xC0 then
    return {
      llm_output = string.format("error: byte_offset %d is not a UTF-8 character boundary", byte_offset),
      is_error = true,
    }
  end

  local _, max_bytes = output_limits.resolve(opts, ctx)
  local source = content:sub(byte_offset + 1)
  local requested = math.min(byte_limit, #content - byte_offset)
  local chunk, chunk_bytes = utf8_prefix(source, requested)
  local header
  while true do
    local byte_end = byte_offset + chunk_bytes
    header = string.format("[bytes %d-%d of %d]\n", byte_offset, byte_end, #content)
    local available = math.max(max_bytes - #header, 0)
    if #chunk <= available then
      break
    end
    chunk, chunk_bytes = utf8_prefix(source, math.min(requested, available))
  end
  local byte_end = byte_offset + chunk_bytes

  local intervals = chunk_bytes > 0 and { { byte_offset, byte_end } } or {}
  local _, record_err = ctx:record_read_ranges(path, content, intervals, lease)
  if record_err then
    return { llm_output = "read provenance error: " .. tostring(record_err), is_error = true }
  end
  return {
    llm_output = header .. chunk,
    body = ToolView.restore(header .. chunk, read_view_opts(ctx)),
    annotation = string.format("bytes %d-%d of %d", byte_offset, byte_end, #content),
  }
end

maki.api.register_prompt_hint({
  slot = "tool_usage",
  content = [[
- When using the **read** tool, only read the sections you need. Use line mode with `offset` and `limit`.
- If a mutation reports unseen source bytes, recover that exact range with byte mode using `byte_offset` and `byte_limit`.]],
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
        description = "Line-mode start line (1-indexed). Requires limit and cannot be mixed with byte mode.",
      },
      limit = {
        type = "integer",
        description = "Line-mode maximum lines. Use 0 for the bounded end. Requires offset.",
      },
      byte_offset = {
        type = "integer",
        description = "Byte-mode start offset (zero-based UTF-8 boundary). Requires byte_limit.",
      },
      byte_limit = {
        type = "integer",
        description = "Byte-mode maximum source bytes. Requires byte_offset.",
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
    local has_line = input.offset ~= nil or input.limit ~= nil
    local has_byte = input.byte_offset ~= nil or input.byte_limit ~= nil
    if has_line == has_byte then
      return {
        llm_output = "error: provide exactly one complete pair: offset with limit, or byte_offset with byte_limit",
        is_error = true,
      }
    end
    if has_line and (input.offset == nil or input.limit == nil) then
      return { llm_output = "error: line mode requires both offset and limit", is_error = true }
    end
    if has_byte and (input.byte_offset == nil or input.byte_limit == nil) then
      return { llm_output = "error: byte mode requires both byte_offset and byte_limit", is_error = true }
    end

    local path = maki.fs.abspath(raw)
    local meta = maki.fs.metadata(path)
    if not meta then
      return { llm_output = "error: path not found: " .. path, is_error = true }
    end
    if meta.is_dir then
      return { llm_output = "error: path is a directory, use the list tool instead", is_error = true }
    end
    if has_line then
      return read_lines(path, input.offset, input.limit, ctx)
    end
    return read_bytes(path, input.byte_offset, input.byte_limit, ctx)
  end,
})
