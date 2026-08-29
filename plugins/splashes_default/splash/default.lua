-- Bundled default splash renderer. Rust owns the frame clock, the cadence,
-- and the update check; this module owns every visible element (starfield,
-- logo, tagline, tip, help, and the top-right version/update notice). The
-- splashes_default plugin wires it into the `splash.render` slot and the
-- `splash` registry. Replace or wrap it with maki.api.set_slot.

local INTENSITY_SCALE = 0.3
local VIGNETTE_SCALE = 0.25
local FIELD_CHAR_MAX = 4
local WAVE_LAYERS = 3
local TAU = 2.0 * math.pi
local PI = math.pi
local FRAC_PI_2 = PI / 2.0
local BHASKARA_B = 4.0 / (PI * PI)

local FSYM = { [1] = ".", [2] = ":", [3] = "+", [4] = "*" }

local LOGO = "makima"
local TAGLINE = "less context, more control"
local LOGO_DELAY = 0.2
local LOGO_RAMP = 0.8

local HELP = {
  { "Ctrl+H", true },
  { " help", false },
  { " · ", false },
  { "/help", true },
  { " in chat", false },
}
local TIPS = {
  { "Ctrl+S", "to grab file paths with fuzzy search" },
  { "Ctrl+X", "to see what your subagents are up to" },
  { "Ctrl+F", "to find things in the conversation" },
  { "/btw", "to ask something without interrupting the session" },
  { "/memory", "to view, edit, and delete persistent notes" },
  { "/cd", "to switch to a different directory" },
}

local function theme_or(name, fallback)
  local c = maki.ui.theme_color(name)
  if c then
    return {
      tonumber(string.sub(c, 2, 3), 16),
      tonumber(string.sub(c, 4, 5), 16),
      tonumber(string.sub(c, 6, 7), 16),
    }
  end
  return fallback
end

-- theme_color resolves these from the seeded UI palette; the fallback is the
-- dracula default (kept in sync with the old Rust splash) for a host that has
-- not seeded them yet. Resolved per frame (not cached at load) so the splash
-- tracks the active theme and never bakes a pre-seed fallback.
local BG, FG, ACCENT, TIP, BG_HEX
local function refresh_colors()
  BG = theme_or("background", { 40, 42, 54 })
  FG = theme_or("foreground", { 248, 248, 242 })
  ACCENT = theme_or("accent", { 255, 184, 108 })
  TIP = theme_or("todo_in_progress", { 241, 250, 140 })
  BG_HEX = string.format("#%02x%02x%02x", BG[1], BG[2], BG[3])
end

local function charlen(s)
  local n = 0
  for i = 1, #s do
    local b = string.byte(s, i)
    if b < 128 or b >= 192 then
      n = n + 1
    end
  end
  return n
end

local function fast_sin(x)
  x = x - math.floor(x / TAU) * TAU
  local sign = 1.0
  if x > PI then
    x = x - PI
    sign = -1.0
  end
  local raw = BHASKARA_B * x * (PI - x)
  return sign * (4.0 * raw) / (5.0 - raw)
end

local function fast_sincos(x)
  return fast_sin(x), fast_sin(x + FRAC_PI_2)
end

local function ease_out_cubic(t)
  t = math.max(0, math.min(1, t))
  return 1.0 - (1.0 - t) * (1.0 - t) * (1.0 - t)
end

local function lerp_u8(a, b, t)
  return math.floor(a + (b - a) * t)
end

local function hex(r, g, b)
  return string.format("#%02x%02x%02x", r, g, b)
end

local function text_color(target, alpha, bold)
  return {
    fg = hex(lerp_u8(BG[1], target[1], alpha), lerp_u8(BG[2], target[2], alpha), lerp_u8(BG[3], target[3], alpha)),
    bg = BG_HEX,
    bold = bold,
  }
end

local function field_sym(idx)
  if idx <= 0 then
    return " "
  end
  local i = idx
  if i > FIELD_CHAR_MAX then
    i = FIELD_CHAR_MAX
  end
  return FSYM[i]
end

-- Per-column wave tables depend only on width (fx/fy/weight are constants and
-- each frame only varies the row phase), so cache them per width.
local col_cache = {}
local function column_data(w)
  local cached = col_cache[w]
  if cached then
    return cached
  end
  local inv_w = 1.0 / w
  local layers = {}
  for i = 0, WAVE_LAYERS - 1 do
    layers[i + 1] = { 2.0 + i * 1.8, 1.5 + i * 1.2, 1.0 / (1.5 + i * 0.5) }
  end
  local vx = {}
  local col_sin = {}
  local col_cos = {}
  for col = 0, w - 1 do
    local nx = col * inv_w
    local d = (nx - 0.5) * 2.0
    vx[col + 1] = d * d
    local cs, cc = {}, {}
    for i = 0, WAVE_LAYERS - 1 do
      local s, c = fast_sincos(nx * layers[i + 1][1])
      cs[i + 1] = s * layers[i + 1][3]
      cc[i + 1] = c * layers[i + 1][3]
    end
    col_sin[col + 1] = cs
    col_cos[col + 1] = cc
  end
  local data = { vx = vx, col_sin = col_sin, col_cos = col_cos, layers = layers }
  col_cache[w] = data
  return data
end

local function field_row(data, w, h, row, val_scale, vignette_inv, inv_h, t)
  local layers = data.layers
  local ny = row * inv_h
  local d = (ny - 0.5) * 2.0
  local vy = d * d
  if vignette_inv - vy <= 0 then
    return string.rep(" ", w)
  end
  local half_weight = 0.0
  for i = 0, WAVE_LAYERS - 1 do
    half_weight = half_weight + layers[i + 1][3] * 0.5
  end
  local rs, rc = {}, {}
  for i = 0, WAVE_LAYERS - 1 do
    local ph = t * (0.3 + i * 0.15) + i * 2.094
    local s, c = fast_sincos(ny * layers[i + 1][2] + ph)
    rs[i + 1] = s
    rc[i + 1] = c
  end
  local sb = {}
  for col = 0, w - 1 do
    local s = 0.0
    for i = 0, WAVE_LAYERS - 1 do
      s = s + data.col_sin[col + 1][i + 1] * rc[i + 1] + data.col_cos[col + 1][i + 1] * rs[i + 1]
    end
    s = (s + half_weight) * (1.0 - (data.vx[col + 1] + vy) * VIGNETTE_SCALE) * val_scale
    sb[col + 1] = field_sym(math.floor(s * FIELD_CHAR_MAX + 0.5))
  end
  return table.concat(sb)
end

local function place_text(rows, rowidx, x1, glyphs, style)
  local field = rows[rowidx][1].glyphs
  local lead = string.sub(field, 1, x1 - 1)
  local tail = string.sub(field, x1 + charlen(glyphs))
  rows[rowidx] = {
    { glyphs = lead, style = "field" },
    { glyphs = glyphs, style = style },
    { glyphs = tail, style = "field" },
  }
end

local function place_seqs(rows, rowidx, x1, items)
  local field = rows[rowidx][1].glyphs
  local segs = {}
  local pos = x1 - 1
  if pos > 0 then
    segs[#segs + 1] = { glyphs = string.sub(field, 1, pos), style = "field" }
  end
  for _, it in ipairs(items) do
    segs[#segs + 1] = { glyphs = it[1], style = it[2] }
    pos = pos + charlen(it[1])
  end
  local tail = string.sub(field, pos + 1)
  if #tail > 0 then
    segs[#segs + 1] = { glyphs = tail, style = "field" }
  end
  rows[rowidx] = segs
end

local tip_idx = 1
maki.api.create_autocmd("SplashShown", {
  callback = function()
    tip_idx = math.random(1, #TIPS)
  end,
})

local function render_splash(w, h, t, fade)
  refresh_colors()
  if w == 0 or h == 0 then
    return {}
  end
  local data = column_data(w)
  local layers = data.layers
  local half_weight = 0.0
  for i = 0, WAVE_LAYERS - 1 do
    half_weight = half_weight + layers[i + 1][3] * 0.5
  end
  local val_scale = (fade * INTENSITY_SCALE) / half_weight
  local vignette_inv = 1.0 / VIGNETTE_SCALE
  local inv_h = 1.0 / h

  local rows = {}
  for row = 0, h - 1 do
    rows[row + 1] = { { glyphs = field_row(data, w, h, row, val_scale, vignette_inv, inv_h, t), style = "field" } }
  end

  local block_height = 8
  local top_y = math.floor((h - block_height) / 2)
  local tag_y = top_y + 1
  local help_y = tag_y + 2
  local tip_y = help_y + 2

  if top_y >= 0 and top_y < h then
    place_text(
      rows,
      top_y + 1,
      math.floor((w - #LOGO) / 2) + 1,
      LOGO,
      text_color(ACCENT, 0.85 * ease_out_cubic(math.max(0, math.min(1, (t - LOGO_DELAY) / LOGO_RAMP))) * fade, true)
    )
  end
  if tag_y >= 0 and tag_y < h then
    place_text(rows, tag_y + 1, math.floor((w - charlen(TAGLINE)) / 2) + 1, TAGLINE, text_color(FG, 0.75 * fade, false))
  end
  if help_y >= 0 and help_y < h then
    local total = 0
    for _, seg in ipairs(HELP) do
      total = total + #seg[1]
    end
    local items = {}
    for _, seg in ipairs(HELP) do
      local text, hi = seg[1], seg[2]
      local target, alpha = FG, 0.5
      if hi then
        target, alpha = ACCENT, 0.75
      end
      items[#items + 1] = { text, text_color(target, alpha * fade, false) }
    end
    place_seqs(rows, help_y + 1, math.floor((w - total) / 2) + 1, items)
  end
  if tip_y >= 0 and tip_y < h then
    local label, desc = TIPS[tip_idx][1], TIPS[tip_idx][2]
    local total = 5 + #label + 1 + charlen(desc)
    local items = {
      { "tip: ", text_color(TIP, 0.75 * fade, true) },
      { label, text_color(ACCENT, 0.75 * fade, false) },
      { " ", { fg = BG_HEX, bg = BG_HEX, bold = false } },
      { desc, text_color(FG, 0.5 * fade, false) },
    }
    place_seqs(rows, tip_y + 1, math.floor((w - total) / 2) + 1, items)
  end

  local version = maki.version()
  local version_text = "v" .. version.current
  if version.update_available then
    version_text = version_text .. " run makima update to get v" .. (version.latest or "")
  end
  place_text(rows, 1, w - charlen(version_text), version_text, text_color(FG, 0.4 * fade, false))

  return rows
end

return {
  description = "The standard Makima splash screen.",
  render = render_splash,
}
