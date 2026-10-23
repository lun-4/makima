local M = {}

-- The definitions this session has a value for, each with its current value.
function M.options(definitions, values)
  local options = {}
  for _, definition in ipairs(definitions) do
    local current = values[definition.id]
    if current ~= nil then
      options[#options + 1] = {
        id = definition.id,
        name = definition.name,
        description = definition.description,
        category = definition.category,
        values = definition.values,
        current_value = current,
      }
    end
  end
  return options
end

function M.find(options, id)
  for _, option in ipairs(options) do
    if option.id == id then
      return option
    end
  end
end

function M.option_items(options)
  local items = {}
  for _, option in ipairs(options) do
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
