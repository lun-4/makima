local M = {}

function M.find(snapshot, id)
  for _, option in ipairs(snapshot.options or {}) do
    if option.id == id then
      return option
    end
  end
end

function M.option_items(snapshot)
  local items = {}
  for _, option in ipairs(snapshot.options or {}) do
    items[#items + 1] = {
      label = option.name .. ": " .. option.current_value,
      detail = option.description .. " (" .. option.category .. ")",
      id = option.id,
    }
  end
  return items
end

function M.value_items(option)
  local items = {}
  local cursor = 1
  for index, value in ipairs(option.values or {}) do
    items[#items + 1] = {
      label = value.name,
      detail = value.value,
      value = value.value,
    }
    if value.value == option.current_value then
      cursor = index
    end
  end
  return items, cursor
end

return M
