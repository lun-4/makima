local shorten_path = require("maki.shorten_path")
local ToolView = require("maki.tool_view")
local output_limits = require("maki.output_limits")
local long_lines = require("maki.long_lines")

local DESCRIPTION = [[Write content to a file, replacing existing content.

- Creates parent directories if needed.
- Always read the file first before writing.
- Refuses to drop or alter lines longer than `agent.max_line_bytes`, since read may have shown them cut. Change text inside such a line with **edit**, or delete it with **edit_lines** and an empty new_string.
- Refuses to add lines ending in a `[line truncated, +N bytes]` marker; that marker is read output, not file content.
- NEVER create files unless absolutely necessary - prefer editing existing files.
- NEVER proactively create documentation files (*.md) or README files. Only create documentation files if explicitly requested by the User.]]

-- The current text for the guard to compare against: "" for a new file, and
-- false for non-UTF-8 content, which read can never have shown as lines.
local function existing_text(path)
  local meta, meta_err = maki.fs.metadata(path)
  if meta_err then
    return nil, meta_err
  end
  if not meta then
    return ""
  end
  local bytes, read_err = maki.fs.read_bytes(path)
  if not bytes then
    return nil, read_err
  end
  local text = buffer.tostring(bytes)
  return utf8.len(text) ~= nil and text
end

local function write_view_opts(ctx)
  local tol = ctx:tool_output_lines()
  return { max_lines = (tol and tol.write) or 10, keep = "head" }
end

local function build_view(content, path, ctx)
  local buf = maki.ui.buf()
  local view = ToolView.new(buf, write_view_opts(ctx))
  view:set_highlight(content, path:match("%.([^%.]+)$") or "")
  view:finish()
  buf:on("click", function()
    view:toggle()
  end)
  return buf
end

maki.api.register_tool({
  name = "write",
  kind = "edit",
  mutable_path = "path",
  permission = "fs_write",
  permission_scopes = "path",
  audiences = { "main", "general_sub", "interpreter" },
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
      content = {
        type = "string",
        description = "The complete file content to write",
        required = true,
      },
    },
  },

  header = function(input)
    local buf = maki.ui.buf()
    buf:line({ { shorten_path(input.path or ""), "path" } })
    return buf
  end,

  restore = function(input, output, _is_error, ctx)
    local content = input.content or ""
    if content == "" then
      return ToolView.restore(output, write_view_opts(ctx))
    end
    return build_view(content, input.path or "", ctx)
  end,

  handler = function(input, ctx)
    local raw = input.path
    if not raw then
      return { llm_output = "error: path is required", is_error = true }
    end
    local content = input.content
    if not content then
      return { llm_output = "error: content is required", is_error = true }
    end

    local path, path_err = ctx:resolve_path(raw)
    if not path then
      return { llm_output = "error: " .. tostring(path_err), is_error = true }
    end

    local ok, err = ctx:check_before_edit(path)
    if not ok then
      return { llm_output = err, is_error = true }
    end

    local before, read_err = existing_text(path)
    if before == nil then
      return { llm_output = "read error: " .. tostring(read_err), is_error = true }
    end
    local guard_err = before and long_lines.check_write(before, content, output_limits.line_bytes(ctx))
      or long_lines.check_markers(before or "", content)
    if guard_err then
      return { llm_output = guard_err, is_error = true }
    end

    local parent = maki.fs.dirname(path)
    if parent then
      maki.fs.mkdir(parent, { parents = true })
    end

    local _, write_err = maki.fs.atomic_write(path, content)
    if write_err then
      return { llm_output = "write error: " .. tostring(write_err), is_error = true }
    end

    ctx:record_read(path)

    local byte_count = #content
    local rel = shorten_path(path)
    local llm_output = string.format("wrote %d bytes to %s", byte_count, rel)
    local annotation = string.format("%d bytes", byte_count)

    return {
      llm_output = llm_output,
      body = build_view(content, path, ctx),
      annotation = annotation,
      written_path = path,
    }
  end,
})
