-- Spans: styled slices of a label for fuzzy-match highlighting.
--
-- `match_spans` takes 1-based CODEPOINT indices (as returned by
-- `makima.match.fuzzy`) and slices the label by bytes, so multi-byte titles
-- (CJK, emoji) get whole-codepoint spans. Adjacent indices merge into one
-- span.

local Spans = {}

-- indices: 1-based codepoint offsets, ascending. No indices yields a single
-- {text, base_style} span.
function Spans.match_spans(text, indices, base_style, match_style)
  if #indices == 0 then
    return { { text, base_style } }
  end
  local spans, pos = {}, 1
  local i = 1
  while i <= #indices do
    local j = i
    while j < #indices and indices[j + 1] == indices[j] + 1 do
      j = j + 1
    end
    local b = utf8.offset(text, indices[i])
    local e = utf8.offset(text, indices[j] + 1) or #text + 1
    if b > pos then
      spans[#spans + 1] = { text:sub(pos, b - 1), base_style }
    end
    spans[#spans + 1] = { text:sub(b, e - 1), match_style }
    pos = e
    i = j + 1
  end
  if pos <= #text then
    spans[#spans + 1] = { text:sub(pos), base_style }
  end
  return spans
end

return Spans
