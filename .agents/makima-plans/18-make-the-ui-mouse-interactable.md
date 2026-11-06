# Make the UI mouse-interactable

## Goal

Every surface named by issue #18 and the operator's follow-up becomes mouse-operable:
draggable (and click-to-jump) scrollbars on every scrollable surface, clickable
status/top bar chips with in-place dropup/dropdown menus for mode and thinking, clickable
rows and terminal actions in every picker and modal dialog, and the same click contract
exposed to Lua plugins so plugin-drawn UI behaves like built-in UI.

"Terminal actions" here means every way to leave a dialog (confirm, cancel, dismiss). It
does not mean full keyboard parity: tier assign/unassign (`Shift+1..4` in the model
picker), `Ctrl+D` delete, `Ctrl+Q` pop, and palette dismissal stay keyboard-only, as
listed under scope boundaries.

## Implementation Summary

Today the mouse only does: middle-click autoscroll, wheel scroll, click-to-place-cursor
in the input, click-to-expand a tool row, and drag-select (`maki-ui/src/app/mouse.rs:117`).
Everything else is inert. The chokepoint is that nothing publishes screen rects: the
status bar draws `Span`s into a `Paragraph` (`components/status_bar.rs:158`), the
scrollbar widget is decorative (`components/scrollbar.rs:16`), and every modal knows its
popup rect only transiently inside `view`.

The change is one shared layer plus per-surface wiring:

- **`maki-ui/src/click.rs` (new)**: `ClickTarget`, `HitTarget`, `HitRegistry`. Components
  record their hits while they render (`last_hits: Vec<HitTarget>` / `Option<HitTarget>`
  fields, the same pattern the components already use for `last_total_lines` and
  `viewport_height`), and `App::view` appends them to `self.hits` in render order. Views
  keep their signatures; only the status bar returns its hits because it has no state to
  keep them in. `HitRegistry::target_at` walks in reverse, so later renders win, exactly
  like `ZoneRegistry` (`maki-ui/src/selection.rs:121`). The registry is cleared at the top
  of `App::view` and lives until the next paint.
- **Mouse arbitration in `app/mouse.rs`**: `handle_mouse` is restructured into named
  steps (`left_down`, `left_up`, `left_drag`, `hover_update`, wheel stays put) with one
  explicit gate order, instead of growing the existing nested match. A left press on a
  scrollbar starts drag-scroll and never starts a selection; a left press on any other
  target records `pending_click` *and still starts a selection*. On release, an empty
  selection over the same target dispatches the click; a non-empty selection is a normal
  copy. Drag-select therefore keeps working across chips and over form text.
- **One modal gate, first in the order**: while `has_modal_overlay()`, only targets
  registered by the open modal (and the permission prompt, which draws topmost and takes
  precedence in `dispatch_overlay`) are eligible. A press that matches none of them is an
  "outside click": on release with an empty selection it may synthesize `Esc`, closing the
  topmost modal exactly like the keyboard. Because chrome and background targets are not
  registered at all in that state, a chip click while a modal is open closes the modal and
  does not activate the chip. This replaces the current early return at
  `app/mouse.rs:162-165`; drag-select inside a modal keeps working because outside-click
  only fires on an empty selection.
- **Outside-click `Esc` only applies to dismissible modals.** `FloatManager` is in
  `overlays()` and `is_modal()` defaults to true (`components/mod.rs:57`), so a focused
  float (including the question form in the below split) counts as a modal, but its
  `Esc` path is plugin-defined and can *discard answers* (`question_form.lua:275`). So
  `Esc` synthesis targets only overlays whose `Esc` means close: help, btw, search, file
  picker, and the `ListPicker` family. A focused float swallows the click (as today).
- **Clicks resolve to the same dispatch as keys.** Row clicks set the component's
  selection and then feed `Enter` through `App::handle_key` (`app/mod.rs:1630`), which
  consults `dispatch_overlay` (`app/mod.rs:1244`) first and falls through to
  `handle_chat_key`, so every row surface including the command palette
  (`app/mod.rs:1700`) and file completion (`:1761`) shares one accept path. Chrome clicks
  map to `run_builtin` (`app/mod.rs:1581`) and the existing actions (`Action::ChangeMode`,
  `Action::SelectModel`).
- **Scrollbar geometry is computed once** in `components/scrollbar.rs` from the same
  inputs ratatui uses (`content_length = max_scroll + 1`, `viewport = area.height`), and
  every caller stores the returned `ScrollbarHit`. Drag maps a row back to a scroll line
  through the thumb travel, so the ends are exactly reachable. `ListPicker` scrolls in
  visual-row space while `scroll_offset` is a filtered item index, so it needs an explicit
  inverse (`item_at_visual_row`) and cannot assign the row directly.
- **Lua gets two new click surfaces** with the existing shapes: a `{type="click", row, col}`
  event from `win:recv()` for float windows, and an optional click callback on
  `maki.ui.set_status_content` entries. The float payload uses the same 1-based content
  row as `buf:on("click")` (`row = 0` means the click landed on chrome or outside the
  content, matching `plugins/lib/maki/tool_view.lua:3`), and adds `col` as a superset.

Scope boundaries:

- No right-click menus, no double-click semantics, no keyboard focus ring, no mouse
  support in the splash or in ACP/headless mode.
- Keyboard-only by design: model picker tier assign/unassign (`Shift+1..4`),
  `Ctrl+D` delete in pickers, `Ctrl+Q` pop, and palette/file-completion dismissal keys.
  Clicking rows and confirming is in scope; these modifiers are not.
- `fast`, `workflow`, `yolo`, and `restricted` stay indicators; they are not clickable in
  this pass (a mis-click on YOLO must not turn permissions off).
- Plugin-drawn splashes get no input events.

Touch points: `maki-ui/src/app/{mouse,mod,view}.rs`, `maki-ui/src/click.rs` (new),
`maki-ui/src/components/*` (status bar, menu (new), scrollbar, list_picker, dialogs,
lua_float), `maki-lua/src/api/util/command.rs`, `maki-lua/src/api/ui/{mod,win,buf}.rs`,
`maki-lua/src/{loader,runtime,lib}.rs`, `maki-config/src/lib.rs`,
`maki-docgen/src/gen_keybindings.rs`, `plugins/question/question_form.lua`,
`plugins/lib/maki/list_picker.lua`, docs under `site/docs/content/`.

## Implementation Plan

### Branch and commit workflow

- Before Phase 0, create a branch off the current `main`: `git switch -c feat/mouse-interactable`.
  The working tree is clean today; do not carry unrelated changes onto the branch.
- Commit as each coherent feature lands, not once at the end. Each commit must leave
  `cargo check -p maki-ui --tests` green and the tests for the crates it touches passing
  (`cargo nextest run -p maki-ui` / `-p maki-lua`). `just lint`, `just fmt-check`, and
  `just gen-docs-check` only have to pass on the last commit of each phase.
- The expected commit sequence (split a line further if one change grows large):

  | After | Commit |
  |---|---|
  | Phase 0 | `feat(ui): add click target registry and mouse arbitration` |
  | Phase 1 | `feat(ui): draggable and click-to-jump scrollbars` |
  | Phase 2 | `feat(ui): clickable status and top bar chips with in-place menus` |
  | Phase 3a | `feat(ui): clickable picker, popup, and panel rows` |
  | Phase 3b | `feat(ui): mouse-reachable permission prompt actions` |
  | Phase 4 floats | `feat(lua): deliver clicks to float windows` |
  | Phase 4 status | `feat(lua): clickable plugin status content` |
  | Phase 4 plugins | `feat(plugins): mouse support in question and list pickers` |
  | Phase 5 | `docs: document mouse interaction and the Lua click contract` |

- Follow the repository's existing commit style (`feat(ui):`, `feat(lua):`, `fix(ui):`,
  `docs:`), keep messages imperative and scoped, and never force-push, skip hooks, or amend
  commits you did not create. Do not push unless the operator asks.

### Phase 0 - Hit-testing foundation

Files: `maki-ui/src/click.rs` (new), `maki-ui/src/lib.rs`, `maki-ui/src/app/mod.rs`,
`maki-ui/src/app/mouse.rs`, `maki-ui/src/app/view.rs`, `maki-ui/src/app/tests.rs`.

- Define `ClickTarget` (`Copy + Debug + PartialEq + Eq`) covering the whole plan:
  `ModeChip`, `ThinkingChip`, `ModelChip`, `AgentChip`, `TasksHint`, `AutoScrollChip`,
  `StatusEntry(u32)`, `MenuRow(u16)`, `PickerRow { picker: PickerId, index: usize }`,
  `PermissionHint { row: u16, cell: u16 }` (a hint row packs up to four cells, so one index
  is not enough), `PermissionGuidance`, `PlanRow(u16)`, `QueueRow(usize)`,
  `CompletionRow { grid: CompletionGrid, index: usize }` (with `CompletionGrid =
  PaletteCommands | PaletteArguments | PaletteGrid | FileCompletion`, the palette's three
  selection domains plus the completion grid), `WindowContent(u32)`,
  `WindowChrome(u32)`, `Scrollbar(ScrollOwner)`.
  `PickerId` is declared once, here: `Model | Task | Theme | Rewind | Login | Mcp | Lua |
  Search | File`. `ScrollOwner` = `Messages | Input | Picker(PickerId) | Help | Btw |
  Float(u32)`. Every surface has exactly one name (search and file pickers are
  `Picker(Search)`/`Picker(File)`, not separate scroll owners).
  Floats register three hits, not per-cell ones: content (`WindowContent`), chrome border
  (`WindowChrome`), and the scrollbar column.
  Unused variants in Phase 0 are fine: `dispatch_click` arms for surfaces that land in later
  phases start as `unreachable!()`/`Vec::new()` stubs, and `FloatManager::click_at` gets a
  stub signature whose body Phase 4 fills in, so `cargo check -p maki-ui --tests` stays
  green between phases; `just lint` is deferred to Phase 5.
- `HitRegistry` owns a reused `Vec<HitTarget>`: `clear`, `push(rect, target)`,
  `target_at(Position)`, `rect_of(ClickTarget)`, `set_modal_rects`/`modal_rects`.
  Skip zero-sized rects. `rect_of` exists for tests and for hover.
- `App` gains `hits: HitRegistry`, `pending_click: Option<(ClickTarget, Position)>`,
  `scroll_drag: Option<ScrollDrag>`, `hover: Option<ClickTarget>`. `App::view` clears the
  registry first and registers in render order; `App::hit_rect` is `#[cfg(test)]`-visible.
- Zones keep up with the new surfaces: `register_zones` (`app/view.rs:488`) has no zone
  for `top_bar_area`, and `ZoneRegistry` silently drops pushes past `ZONE_CAP = 12`
  (`selection.rs:85-112`), which a maximally decorated layout already reaches. Raise the
  cap to 20 (the array is tiny and all `Copy`), make `push` return whether it dropped and
  expose `ZoneRegistry::overflowed()` for tests (production keeps a `tracing::warn!`), and
  register a top-bar overlay zone plus a menu overlay zone while the menu is open, so every
  click target lies inside a registered zone.
- Change `App::handle_mouse` to return `(Dirty, Vec<Action>)`; `Msg::Mouse` in
  `App::update` (`app/mod.rs:999`) propagates the actions. Restructure the handler into
  `left_down`, `left_up`, `left_drag`, `hover_update`, with wheel/middle-scroll arms kept
  as their own early-returning steps, so the order below is one readable sequence instead
  of deeper nesting inside the existing match.
- Arbitration rules, in this exact order:
  - `Down(Left)`, modal gate first: if `has_modal_overlay()`, only modal-registered hits
    are eligible. A press on a modal-owned hit is handled per below. A press anywhere else
    records `pending_click = None` and **returns immediately**, so the outside click only
    queues the Esc on release and never reaches the input caret path
    (`app/mouse.rs:166-179`); drag-selection starts only when the pointer is inside one of
    the `modal_rect`s. This is a deliberate tightening: today's gate
    (`app/mouse.rs:163`) still starts a selection for status bar/plan form/queue/split
    zones while a picker is open.
  - `Down(Left)`, menu open and the hit is not a menu row or the owning chip: close the
    menu and swallow the press (checked before the scrollbar rule, so a press anywhere while
    the menu is open closes it rather than starting a drag).
  - `Down(Left)`, `Scrollbar(owner)` hit: start
    `ScrollDrag { owner, area, content_len, grab }`, apply the initial jump (thumb press
    grabs without jumping, track press centers the thumb), return without starting a
    selection. Unreachable while a modal is open except for a modal's own scrollbar.
  - `Down(Left)`, any other registered hit: set `pending_click`, continue into the
    existing selection start.
  - `Down(Left)`, no hit: `pending_click = None`, existing behaviour.
  - `Up(Left)`: end `scroll_drag` if any; else when `pending_click` is set, the selection
    is missing or empty (popup rows may land inside no zone, so `selection_state` can be
    `None`), and the release still hits the same target, dispatch the click and clear the
    selection; else if a dismissible modal is open and the release is outside every
    `modal_rect`, synthesize `Esc`; else existing copy / transcript toggle. Feeding `Enter`
    through `handle_key` also runs its normal side effects
    (`clear_selection_unless_pending_copy`, middle-scroll cancel), which is what the
    keyboard would do.
  - `Drag(Left)`: `scroll_drag` updates the scroll position; otherwise existing edge
    selection.
  - `Moved`: existing middle-scroll tick, then hover update; return `Dirty::YES` only when
    the hovered target changes.
  - Wheel (`Msg::Scroll`) unchanged.
- `App::view` registers chrome chips and the menu only when `!self.has_modal_overlay()`;
  modal-owned targets are always registered, and the permission prompt (which draws topmost)
  registers its hint cells even when another modal is open, matching its precedence in
  `dispatch_overlay`. `modal_rects` is sourced from each overlay's own popup rect returned
  by its `view`, plus a new `FloatManager::focused_rect() -> Option<Rect>` accessor
  (the field exists at `lua_float.rs:149` and covers split and panel floats, unlike the
  union returned by `FloatManager::view`).
- Menu self-heal has one site (Phase 2): in `App::update`, next to
  `validate_middle_scroll()` (`app/mod.rs:1015`), `if self.has_modal_overlay() ||
  self.permission_active() { self.menu.close() }`; `App::view` additionally neither paints
  nor registers a menu that was left open.
- `App::dispatch_click(&mut self, target: ClickTarget, pos: Position) -> Vec<Action>`
  matches the variant: chips/menu rows map to `run_builtin`/menu actions; rows call the
  owner then feed `Enter` through `App::handle_key`; `WindowContent` passes `pos` to
  `FloatManager::click_at` so the content row and column are computed from the pointer;
  `StatusEntry` is Phase 4. `pending_click` stores the position alongside the target.

### Phase 1 - Scrollbars on every scrollable surface

Files: `maki-ui/src/components/scrollbar.rs`, `messages/mod.rs`, `input.rs`,
`list_picker.rs`, `help_modal.rs`, `btw_modal.rs`, `search_modal.rs`, `file_picker.rs`,
`lua_float.rs`, `maki-ui/src/app/{mouse,mod,view}.rs`, `maki-ui/src/click.rs`.

- `render_vertical_scrollbar` returns `Option<ScrollbarHit { area, content_len }>` (the
  1-column rect it actually drew and the total content length it was given) and returns
  `None` when the scrollbar is disabled by config, so an invisible bar is never draggable.
  Pure helpers reproduce ratatui's `part_lengths`
  (`ratatui-widgets-0.3.2/src/scrollbar.rs:557-599`) exactly. With `h = area.height`,
  `total = content_len` (the total content lines passed in), `max_scroll = total - h`,
  ratatui is given `content_length = max_scroll + 1` and uses `viewport = h`, so:
  - `thumb_len = round(h*h / total).clamp(1, h)`;
  - `thumb_start = round(position*h / total).clamp(0, h - thumb_len)`;
  - `travel = h - thumb_len`, and a drag maps
    `position = round((y - grab) / travel * max_scroll)` clamped to `[0, max_scroll]`.
  Track press sets `grab = thumb_len / 2`; thumb press sets
  `grab = press_y - thumb_start`, so both ends are exactly reachable and mid-drag is within
  one row of exact (the thumb is quantised to cells). Helpers are defensive because tests
  call them directly: `total.max(1)`, integer rounding via `(n + d/2)/d` (never `f64`), and
  `travel == 0` leaves the position unchanged.
  `thumb_matches_ratatui_render` asserts the drawn thumb cells equal `thumb()` across
  several `(h, total, position)` triples, so the helper cannot drift from the widget.
- Every surface stores the hit and exposes `scroll_to_line(u16)`:
  `MessagesPanel` (`set_scroll_top` at `messages/mod.rs:618` unpins auto-scroll and the
  next `view` re-pins at the bottom in `resolve_scroll`, `messages/mod.rs:1585`),
  `InputBox`, `ListPicker`, `help_modal`/`btw_modal` (`ModalScroll`), `search_modal`,
  `file_picker`, and each `FloatWindow` (`scroll_offset`).
-   `ListPicker` draws its scrollbar in visual-row space (`visual_rows_in_range` includes
  section header rows) while `scroll_offset` and `select()` are filtered-item indices.
  Add an explicit inverse, `item_at_visual_row(visual) -> usize`, and have its
  `scroll_to_line` convert before storing; assigning a visual row into `scroll_offset`
  would desynchronise the thumb from the content exactly where AC.7/AC.20 claim
  consistency. `ListPicker` also gains its `picker: Option<PickerId>` here, because the
  scrollbar hit must carry `Scrollbar(Picker(PickerId))`.
- `App::scroll_owner_to(owner, line)` dispatches the enum to those setters.
  `validate_scroll_drag` mirrors `validate_middle_scroll` (`app/mouse.rs:74`): cancel when
  the owner closes, the chat changes, or the hit rect moves.
- The scrollbar column is excluded from selection start and from the transcript
  click-to-toggle, for surfaces where it is reserved (messages already reserves it,
  `app/mouse.rs:398`); fix the `file_picker` content-length mismatch (the truncation hint
  at `components/file_picker.rs:511` drops a row while `content_length = match_count` at
  `:378`), so the thumb and the drag agree.
- Extend `App::scroll_at` (`app/mod.rs:1048`) with the pickers it currently misses
  (theme, login, mcp, lua, search) so the wheel and the drag cover the same surfaces.

### Phase 2 - Chips and in-place menus

Files: `maki-ui/src/components/menu.rs` (new), `status_bar.rs`, `maki-ui/src/app/view.rs`,
`maki-ui/src/app/mod.rs`, `maki-ui/src/components/mod.rs`.

- `Menu { kind: Option<MenuKind>, selected: usize, anchor: Rect, area: Rect }` with
  `MenuKind { Mode, Thinking }`. `MenuItem { label, current: bool, action: MenuAction }`,
  `MenuAction { Mode(String), Thinking(ThinkingConfig) }`. Items are built from app state
  on open (mode registry via `lua_event_handle.mode_registry()`, thinking via
  `ThinkingConfig::options()` from `maki-domain/src/maki_thinking.rs:124`, each option
  parsed into a `ThinkingConfig` when the item is built).
- Rendering: bordered popup, last of the chrome passes, `Clear` first; anchored to the
  chip and flipped above it when the anchor sits in the bottom half of the frame,
  clamped to the frame. Current value marked. Rows push `ClickTarget::MenuRow`.
- Keyboard: the menu branch in `dispatch_overlay` is conditioned on
  `menu.is_open() && !has_modal_overlay() && !permission_active()` and sits after the Alt+M
  defer branch and the `permission_active()` branch (`app/mod.rs:1244-1263`), so a pending
  permission demand always wins the keyboard. The self-heal gate is the same predicate: a
  permission demand arriving while a menu is open closes it (`App::update`), and `App::view`
  neither opens, paints, nor registers a menu while `permission_active()`. While the menu is
  open, and no permission is pending, it consumes navigation keys
  (`Up`/`Down`/`Enter`/`Esc`).
- Mode menu order follows `ModeRegistry::list()` (built-ins first); custom modes are sorted
  by label when building items so the rows are stable across runs.
- Status bar (`StatusBarContext` gains `hover: Option<ClickTarget>` and
  `thinking_supported: bool`; `view` returns its `Vec<HitTarget>` because the bar keeps no
  state): hits for the mode chip (left), the thinking and model chips (right), and the
  `auto-scroll paused` indicator (registered only when that indicator is drawn), computed
  from the spans after truncation so the rects match what is drawn. The two new context
  fields mean updating the five `StatusBarContext` constructors in `status_bar.rs` tests
  (511, 619, 656, 691, 879) and the real one in `app/view.rs:439`. Show a
  `[thinking: off]` chip when the model supports thinking but the level is off (today
  `status_label()` returns `None`, `maki-providers/src/types.rs:828`), otherwise the chip
  cannot be used to turn thinking back on. List every `ThinkingConfig::options()` entry,
  parsing each option into a `ThinkingConfig` when the item is built (dropping an
  unparsable one); unsupported levels are reconciled by `RequestOptions::clamped`, exactly
  as `/thinking` does.
- Top bar (`app/view.rs:195`): hits for the agent badge and the tasks hint, clamped to
  `left_area` (the left half is `Constraint::Min(0)`, so a narrow terminal can clip the
  badge; a clipped chip must not keep a hit rect); both open the task picker through
  `run_builtin(BuiltinAction::Tasks)`.
- Click semantics: mode chip toggles the menu open/closed, menu row emits
  `Action::ChangeMode(key)`; thinking chip toggles its menu, row emits
  `Action::SelectModel(ModelSelection { spec: None, thinking: Some(parsed_level), fast:
  None })` (the same action `/model thinking=high` produces, `app/tests.rs:3483`); model chip
  calls `run_builtin(BuiltinAction::ModelPicker)`; `auto-scroll paused` calls
  `enable_auto_scroll()`.
- Hover: chips render underlined when `hover` matches their target; only a hover change
  marks the frame dirty.

### Phase 3a - Pickers, panels, and popups

Files: `list_picker.rs`, `lua_picker.rs`, `model_picker.rs`, `task_picker` users in
`app/mod.rs`, `rewind_picker.rs`, `theme_picker.rs`, `login_picker.rs`, `mcp_picker.rs`,
`search_modal.rs`, `file_picker.rs`, `command.rs`, `file_completion.rs`,
`plan_form.rs`, `queue_panel.rs`, `help_modal.rs`, `btw_modal.rs`,
`maki-ui/src/app/{mouse,mod,view}.rs`, `maki-ui/src/app/queue.rs`.

- `ListPicker` records `row_hits: Vec<(u16, usize)>` (screen row -> filtered index) while
  rendering the list, so section headers and gaps are naturally skipped. It gains a
  `picker: Option<PickerId>` (declared in Phase 1, since the scrollbar hit needs it) set
  with `.with_picker(PickerId)` at the eleven production construction sites
  (`app/mod.rs:585`, `model_picker.rs:112`, `rewind_picker.rs:57`,
  `mcp_picker.rs:85`, `theme_picker.rs:29`, `lua_picker.rs:80` and `:107`, and four inside
  `login_picker.rs`'s step machine), leaving the ~20 test constructors on plain `new()`.
  A row click calls `select(index)` and then feeds `Enter` through `App::handle_key`, which
  routes through each picker's existing branch (model/theme/mcp toggles, task/rewind
  selects, lua picker choices). `scroll_owner_to` and `dispatch_click` each match on
  `PickerId` and address the nine concrete picker fields. Login exposes two non-generic
  methods, `click_row(&mut self, index) -> bool` and `scroll_to_line(&mut self, line)`,
  which match its three differently-typed step pickers internally; there is no
  `active_picker_mut()`.
- `search_modal` and `file_picker` store their list rect and per-row mapping (they are not
  `ListPicker`); a row click selects and feeds `Enter` through their existing handlers
  (`SearchAction::Select`, `FilePickerModalAction::Select`). `file_picker` also gets a click
  on its search row that focuses it (a no-op today, kept for symmetry).
- Dispatch note: `ClickTarget::ModelChip` must expect `Action::RefreshModels`, because
  `run_builtin(BuiltinAction::ModelPicker)` returns it (`app/mod.rs:1622-1625`).
- `help_modal` / `btw_modal`: scrollbar drag; an empty click outside the popup closes them
  (Esc parity).
- Modal outside-click close applies to every dismissible modal in `App::overlays()`
  (`app/mod.rs:2968`): help, btw, search, file picker, and the `ListPicker` family. The
  permission prompt is non-modal and is exempt; a focused float is modal for blocking but
  swallows outside clicks instead of synthesizing `Esc`, because its `Esc` belongs to the
  plugin (the question form dismisses answers on it).
- Command palette (`components/command.rs:1467`) and file completion
  (`file_completion.rs:956`) need production selection setters, because `Enter` accepts the
  component's current selection, not the clicked row (`command.rs:1410-1429`,
  `file_completion.rs:851`), and today every setter is `#[cfg(test)]`
  (`command.rs:492-533`) or behind private fields. Add `select_command(index)`,
  `select_argument(index)`, `select_grid(index)` on the palette and `select_grid(index)` on
  the completion menu (or a single `click_row(domain, index)`), then feed `Enter` through
  `App::handle_key` so `handle_chat_key` (`app/mod.rs:1700`, `:1761`) accepts the clicked
  entry. Row indices are a hit-testing snapshot only: dispatch resolves the *current* list at
  click time and clamps/drops an out-of-range index, because the palette refilters on every
  poll and the completion grid re-publishes.
- `PlanForm`: rows wrap (`render_form` applies `Wrap { trim: false }`,
  `components/form.rs:28`, and `wrapped_form_keeps_hint_and_cursor_visible` exists because
  of it), so accumulate row offsets with the existing `crate::wrap::Measure`
  (`wrap.rs:37-54`) instead of writing a second wrapper. A click selects the row and feeds
  `Enter` (`MODEL_ROW` opens the model picker, `USE_CURRENT_ROW` clears the choice). `Esc`
  parity on outside click is not applied because the form is a panel, not a modal.
- Queue panel: a row click focuses it (`MessageQueue::set_focus_at`) and does **not** feed
  `Enter`; a click on the already-focused row synthesizes `Enter` (remove, keyboard parity).
  This is the one deliberate divergence from the generic select-then-`Enter` rule. Keep
  `Ctrl+Q` as the only pop path.
- Rewind rows feed `Enter` like every other picker, which requests `Action::RewindAgent` and
  truncates the conversation; that is the accepted single-click-confirm semantics.

### Phase 3b - Permission prompt hint cells

Files: `permission_prompt.rs`, `components/mod.rs`, `maki-ui/src/app/{mouse,mod,view}.rs`.

This is separated from 3a because it reworks the panel's own height and is the highest-risk
UI change in the plan; a failure here must not block the picker work.

- `PermissionPrompt` is split into `body_lines()` (the tool/scope/allow block plus its
  separator blank, i.e. everything `build_lines` emits before the hint lines) and
  `hint_rows()` (structured `HintCell`s). The outer block is drawn once over `area` (not via
  `render_form`, which draws block and paragraph together); the body `Paragraph` goes into
  `Rect { x+1, y+1, w-2, body_h }` where `body_h = form_height(body_lines(), width) - 2`,
  and `render_hints` starts at `area.y + 1 + body_h`.
  `height(width) == 2 + body_h + hint_rows().len() + 1` (borders, body, hints, trailing
  separator), which is what `compute_layout`'s `permission_prompt.height(inner.width)`
  (`app/view.rs:121-123`) sizes the panel with. `compute_layout` clamps the whole prompt to
  `max_bottom`, so the body sub-rect is clamped to `area.height`, and `render_hints` drops
  rows that do not fit vertically, registering nothing for them.
  `render_hints(frame, area, rows) -> Vec<(Rect, HintAction)>` paints each `(key, desc)`
  cell as its own widget at a computed rect (same aligned columns) and registers that exact
  rect; a cell that does not fit the width is clipped and not clickable (a documented
  change from today's wrapping hints). `HintAction` covers `y`, `n`, `a`, `A`, `s`, `d`,
  `D`, `Enter` in confirm sub-states, the "any key cancels" cell in confirm sub-states
  (`permission_prompt.rs:233-239`), and the `Esc` cancel cell in deny-editing. The `Alt+M`
  defer cell (`HINT_DEFER_ROW`, `permission_prompt.rs:30`) stays keyboard-only and is
  rendered as a non-clickable cell, like the other keyboard-only actions listed under scope
  boundaries. Actions go
  through `permission_prompt.handle_key` plus a new `App::answer_permission(answer)`
  extracted from the existing branch (`app/mod.rs:1254`). In deny-editing the guidance row
  is `ClickTarget::PermissionGuidance` and places the deny caret at the clicked column,
  mapped through `crate::wrap::Measure` so wrapped guidance rows land correctly. A
  permission hint stays clickable when another modal is behind it, matching its precedence
  in `dispatch_overlay`.

### Phase 4 - Lua surfaces and the shared click contract

Files: `maki-lua/src/api/util/command.rs`, `maki-lua/src/api/ui/{mod,win,buf}.rs`,
`maki-lua/src/{loader,runtime}.rs`, `maki-ui/src/components/lua_float.rs`,
`maki-ui/src/components/status_bar.rs`, `maki-ui/src/app/{mouse,mod}.rs`,
`plugins/question/question_form.lua`, `plugins/lib/maki/list_picker.lua`,
`maki-lua/tests/spec.rs`, `maki-lua/tests/ui.rs`, plugin specs.

- **Float windows**: add `WinEvent::Click { row, col }` and map it in `event_table`
  (`api/ui/win.rs:71`) to `{type="click", row, col}`. Extend the `recv` doc block
  (`win.rs:94`) and the `Buf:on` doc to state that floats deliver clicks from `recv`
  while transcript buffers deliver them through `buf:on("click")`.
  `FloatWindow` stores the three content bands it drew (`pinned_top`, `scroll_area`,
  `pinned_bot`) through a shared `bands(popup, content_area, layout)` helper used by both
  `render_window` and `click_at`, so geometry cannot drift. (It already stores
  `last_content` = `block.inner(popup)`, `lua_float.rs:486-496`; the popup rect itself is
  `focused_rect`, `:576`.) `FloatManager::click_at(id, pos)` validates that `id` is the
  focused window and sends `WinEvent::Click` with the real 1-based content row:
  - scrolling band: `row = 1 + reserved_top + scroll_offset + (y - scroll_area.y)`;
  - pinned top band: `row = 1 + (y - pinned_top.y)`;
  - pinned bottom band: `row = 1 + reserved_top + scrollable + (y - pinned_bot.y)`.
  The pinned bands carry real content (`lua_float.rs:551-565` renders `cached_lines[..top]`
  and `cached_lines[top + scrollable..]` there), and the question form's tab bar lives in
  `reserved_top` (`question_form.lua:464-468`), so collapsing them to "chrome" would make a
  multi-select question impossible to finish with the mouse.
  `row = 0` is reserved for clicks on the border/title/footer or outside the content,
  matching `plugins/lib/maki/tool_view.lua:3`; a `WindowChrome` click sends
  `{row = 0, col = 1}` so plugins see the click without inventing a row. Rows are clamped
  to `1..=drawn_line_count` so a plugin never receives a row past its buffer, and
  `col = 1 + (x - content_area.x)` is clamped to the content width. The App sends it from
  `handle_mouse` on a `WindowContent(id)` target, and each float's scrollbar gets a
  `Scrollbar(Float(id))` hit pushed after the content hit so the column wins.
- **`maki.ui.open_list_picker`** needs no Lua change: it renders the Rust `ListPicker`
  (`components/lua_picker.rs`), so Phase 3 gives it click rows.
- **Float focus rule**: only the focused float registers click targets. A float released by
  Alt+M (`FloatManager::release_focus`, `lua_float.rs:305`) stays drawn but is inert, so a
  stray click cannot steal focus back from the input or deliver a click to a plugin parked
  in `recv`. There is no `float_click_focuses_unfocused_window`; activating an unfocused
  float is still a keyboard action.
- **Status content clicks**: change `StatusContent` from `Vec<(String, String)>` to
  `Vec<StatusEntry { text, style, click: Option<u32> }>`
  (`maki-lua/src/api/util/command.rs:18`). `set_status_content` accepts an optional third
  element (a function) and registers it in a new `ClickRegions` store in Lua app data
  (`HashMap<u32, RegistryKey>`, one id allocated once per (plugin, entry index) and reused
  on re-register so an unchanged `set_status_content` still publishes nothing, with only
  the registry value replaced). Replaced keys and the whole set on plugin unload must call
  `lua.remove_registry_value`, like every other callback slot in the runtime
  (`runtime.rs:2944-3013`), because plugins re-call `set_status_content` on every update
  and a leaked key per update would grow without bound. `maki-ui` renders those entries as
  `StatusEntry(id)` click targets; a clipped entry (status bar truncates plugin entries with
  `truncate_head`, `status_bar.rs:353`) registers only the cells actually drawn, and a
  fully clipped entry registers nothing. On click the App calls
  `EventHandle::click_region(id)`; the runtime handles a new
  `Request::ClickRegion { id }` by fetching the registry function and calling it under the
  normal task gate, mirroring `Request::ClickTool` (`runtime.rs:5374`).
- Extend the test-only `RequestProbe::try_recv` (`maki-lua/src/lib.rs:159-169`) to report
  the new request kind; today everything unknown collapses into `"other"` and the request
  would be invisible to tests.
- **Built-in plugins**: `plugins/question/question_form.lua` records a row -> action map
  during render and handles `ev.type == "click"` (options, custom row, confirm, dismiss,
  tab bar). `plugins/lib/maki/list_picker.lua` handles click rows the same way it handles
  `up`/`down`/`enter` so thinking, usage, todo_write, and sessions panels gain mouse
  support.
- Keep the two Lua click contracts consistent: same event name (`click`), same 1-based
  `row` convention with `row = 0` meaning chrome/outside (`api/ui/buf.rs:244`,
  `plugins/lib/maki/tool_view.lua:3`). Floats also carry `col` and arrive from
  `win:recv()`; transcript buffers carry only `row` and arrive through
  `buf:on("click")`.

### Phase 5 - Docs, guidance, and polish

The user-facing docs are generated, so the sources change, then `just gen-docs`
regenerates the pages. Hand-editing `site/docs/content/**` fails `gen-docs-check`.

- `maki-docgen/src/gen_keybindings.rs`: add a static "Mouse" row table (it already has
  static-row precedent in `LUA_CONTEXT_BINDS`/`PLUGIN_BINDS`) covering scrollbar
  drag/jump, chips and menus, picker rows, outside click, and that dragging still selects
  text; emit it from `generate()`. `site/docs/content/keybindings/_index.md` is generated
  from this file.
- `maki-config/src/lib.rs:1299`: extend the `scrollbar` field description
  ("Show vertical scrollbar in scrollable areas" -> mention it is draggable).
  `site/docs/content/configuration/_index.md` is generated from `ConfigField` desc
  strings.
- `site/docs/content/lua-api/_index.md`: regenerated from the `#[lua_fn]`/`manual` doc
  blocks for `win:recv` (new click event), `Buf:on` (float vs transcript delivery) and
  `set_status_content` (optional click callback).
- Root `AGENTS.md`: extend the maki-ui bullet with the click registry, and
  `maki-lua/src/api/AGENTS.md` with the click contract (floats via `recv`, transcript via
  `buf:on`, status entries via the third element).
- Run `just fmt`, `just lint`, `just test`, `just fmt-check`, `just gen-docs-check`
  (`just ci` unions these with `pylint` on `scripts/` and `machete`, neither of which
  this change touches).

## Acceptance Criteria

- AC.1 Clicking the mode chip opens the mode menu; clicking another mode's row emits
  `Action::ChangeMode` with that mode's key.
- AC.2 Clicking the thinking chip opens a menu of `ThinkingConfig::options()`; choosing a
  row emits `Action::SelectModel` with that thinking level and no model spec. The chip is
  visible as `[thinking: off]` on a thinking-capable model.
- AC.3 Clicking the model chip opens the model picker (identical to `Ctrl+M`).
- AC.4 Clicking the agent badge or the tasks hint opens the task picker (identical to
  `Ctrl+X`).
- AC.5 Dragging the transcript scrollbar scrolls continuously, unpins auto-scroll, and
  releases exactly at `max_scroll` when dragged to the bottom, re-pinning auto-scroll.
- AC.6 Clicking the transcript scrollbar track jumps the transcript so `scroll_top` equals
  the value computed by `position_at`, and the content line rendered at the clicked row
  equals `scroll_top + (y - area.y)` (the thumb centres on the pointer, so the clicked line
  is not expected to stay under the cursor).
- AC.7 Scrollbar drag and track jump work on the input box, every `ListPicker`-backed
  picker (including each login step), the search modal, the file picker, help, btw, and Lua
  float windows.
- AC.8 A single click on a picker row selects that row and confirms it through the same
  handler as `Enter` (model changes the model, task switches chat, theme applies, mcp
  toggles, lua picker returns the choice, search jumps to the match, file picker inserts
  the path, login advances its step, rewind requests the rewind).
- AC.9 A click on a permission prompt hint cell produces the same result as typing its
  key: confirm sub-states accept or cancel, `n` enters deny-editing, and in deny-editing a
  click on the guidance line places the deny caret at the clicked column. Every registered
  hint cell rect contains the cell text actually drawn there. At a narrow width, and with
  enough height that `compute_layout`'s `max_bottom` clamp does not bite, every
  `body_lines()` string is still present in the rendered buffer and `height(width)` equals
  the rows actually drawn, while hint cells that do not fit are neither drawn nor clickable.
- AC.10 A click on a plan form row selects it and confirms through `Enter` (implement,
  clear-and-implement, refine, open model picker, use current model), including at a
  narrow width where rows wrap.
- AC.11 An empty click outside an open modal closes it exactly as `Esc` does (for
  dismissible modals only; a focused float swallows it), moves no input caret, and leaves
  the permission prompt alone; a drag that starts outside a modal selects nothing.
- AC.12 While a modal is open, a click on a status/top bar chip closes the modal and does
  not activate the chip (it is an outside click), and background scrollbars cannot be
  dragged.
- AC.13 Hovering a chip underlines exactly that chip's cells (asserted from the rendered
  buffer's modifiers) and marks the frame dirty only when the hovered target changes.
- AC.14 Dragging from a chip still selects text; dragging a scrollbar never starts a text
  selection and never copies.
- AC.15 A click inside a Lua float delivers `{type="click", row, col}` from `win:recv`
  with the real 1-based content row, including rows in the pinned top/bottom bands (the
  question form's tab bar), a clamped row for clicks past the last drawn line, `row = 0`
  for border/title/footer clicks, and the click column. A click on a float released by
  Alt+M does nothing.
- AC.16 A `maki.ui.open_list_picker` selection can be confirmed with the mouse.
- AC.17 `maki.ui.set_status_content` entries accept an optional click callback, and a
  click on the entry runs it.
- AC.18 The question form is operable end to end with the mouse: select options, use the
  custom row, click the tab bar and the Review cell, confirm, and dismiss.
- AC.19 The queue panel focuses a row on click and removes the focused row when it is
  clicked again.
- AC.20 Scrollbar geometry is consistent with what is drawn: the drawn thumb equals
  `thumb()` output, drag position and the reverse mapping are exact at both ends and within
  one row in the middle, and a `ListPicker` drag lands on the item under the cursor across
  section headers.
- AC.21 `just lint`, `just test`, `just fmt-check`, and `just gen-docs-check` pass (a
  command gate, not a named test).
- AC.22 Hits never outlive the surface that registered them: closing a picker, a chat
  switch, or a layout change leaves no clickable rect behind and cancels an in-flight
  scroll drag.
- AC.23 Hit registration and zone registration agree in every paint: a picker row, a
  top-bar chip, and a menu row each lie inside a `SelectionZone` registered in the same
  paint, and the zone registry reports rather than silently swallowing overflow.
- AC.24 The generated docs and the two `AGENTS.md` files describe the new mouse behavior
  and the Lua click contract; verified by `just gen-docs-check` plus a human read of the
  rendered keybindings page, the hand-written permissions page, and the two guidance files.
- AC.25 A click on a command palette row (commands, arguments, and the typed-path grid) and
  on a file completion cell accepts the entry at the clicked row index resolved against the
  list current at click time; when the list has not republished this is exactly the clicked
  entry, and an out-of-range index is dropped and emits nothing.
- AC.26 The `auto-scroll paused` indicator is clickable and re-pins the transcript to the
  bottom.
- AC.27 The menu is keyboard-operable: `Up`/`Down` move the highlighted row, `Enter` applies
  it, `Esc` closes without emitting an action, when the menu is closed those keys pass
  through untouched, a modal opening from any other path closes the menu, and a pending
  permission demand closes it (permission keys are never swallowed by an open menu).
- AC.28 The mouse behaviours that predate this work survive the handler restructure: a
  click in the input places the caret, a click on a collapsed tool row still expands it, and
  wheel scrolling is unchanged.
- AC.29 The work lands on `feat/mouse-interactable` as one branch off `main`, with at least
  one commit per phase (Phase 3a and 3b, and the three Phase 4 slices, count separately), and
  every commit's tree compiles with `cargo check -p maki-ui --tests`.

## Test Strategy

Rust tests live in the same file as the code (`#[cfg(test)]`) or in
`maki-ui/src/app/tests.rs` for App-level mouse flows, using the existing
`mouse_event` helper (`app/tests.rs:705`), `TestBackend` rendering (`rendered_rows`,
`app/tests.rs:2725`, which already draws the app), and `app.update(...)` return values.
New helpers, derived from `rendered_rows` to avoid a second draw path:
`render_app(&mut App, w, h)`, `hit_rect(&App, ClickTarget)`, and
`click(&mut App, ClickTarget) -> Vec<Action>` (render, down, up).

Lua behavior is tested through `maki-lua/tests/spec.rs` with `include_str!` specs, using
mock windows and buffers (`plugins/question/tests/spec.lua:19`, `:660`). The question spec's
`with_mock_ui` must be extended to script `maki.ui.open_win`/`win:recv` rows (only
`maki.ui.buf` is mocked there today; the `open_win` mock at `:661` covers one shape), so
the click cases can drive a full receive loop.

Style assertions (hover underline) need a buffer-modifier reader; `rendered_rows` only
returns symbols. Add a helper next to it that returns `(symbol, Modifier)` per cell by
reading `cell.modifier.contains(Modifier::UNDERLINED)` from the `TestBackend` buffer.

| AC | Test |
|----|------|
| AC.1 | `click_mode_chip_opens_menu`, `menu_mode_row_emits_change_mode` (app/tests.rs) |
| AC.2 | `click_thinking_chip_opens_levels`, `thinking_row_emits_select_model`, `thinking_chip_shows_off_when_supported` |
| AC.3 | `click_model_chip_opens_model_picker` |
| AC.4 | `click_agent_badge_opens_tasks`, `click_tasks_hint_opens_tasks` |
| AC.5 | `drag_transcript_scrollbar_reaches_bottom_and_repins`, `drag_transcript_scrollbar_midpoint` |
| AC.6 | `click_transcript_scrollbar_track_jumps` (asserts the rendered row too) |
| AC.7 | `scrollbar_drag_scrolls_input`, `scrollbar_drag_scrolls_picker`, `scroll_owner_covers_every_picker` (table over all `PickerId` values), `picker_drag_scrolls_login_step_picker`, `scrollbar_drag_scrolls_search_modal`, `scrollbar_drag_scrolls_file_picker`, `scrollbar_drag_scrolls_help_and_btw`, `scrollbar_drag_scrolls_float`, `scrollbar_track_jump_input`, `scrollbar_track_jump_picker`, `scrollbar_track_jump_modal`, `scrollbar_track_jump_float`, `wheel_scrolls_every_picker`, `file_picker_thumb_and_drag_agree_with_truncation_hint` |
| AC.8 | `click_picker_row_confirms_model`, `click_picker_row_selects_task`, `click_theme_row_applies`, `click_mcp_row_toggles`, `click_lua_picker_row_returns_choice`, `click_login_row_advances_step`, `click_rewind_row_requests_rewind`, `click_search_match_jumps`, `click_file_picker_row_inserts_path` |
| AC.9 | `permission_hints_land_on_their_drawn_cells`, `permission_prompt_fits_at_narrow_width`, `permission_hints_clipped_at_short_height`, `click_permission_deny_cell_enters_editing`, `click_permission_confirm_accepts`, `click_permission_any_key_cancels`, `click_permission_guidance_places_caret`, `click_permission_guidance_wrapped_places_caret`, `permission_hint_clickable_over_open_modal` |
| AC.10 | `click_plan_row_implements`, `click_plan_row_clear_and_implement`, `click_plan_model_row_opens_picker`, `click_plan_row_implements_at_narrow_width` |
| AC.11 | `outside_click_closes_model_picker`, `outside_click_closes_help`, `outside_click_does_not_answer_permission`, `outside_click_does_not_dismiss_question_form`, `outside_click_does_not_move_input_caret`, `drag_outside_modal_selects_nothing` |
| AC.12 | `chip_click_closes_modal_and_does_not_activate_chip`, `scrollbar_drag_blocked_by_modal` |
| AC.13 | `hover_chip_underline_only_on_hovered_chip`, `hover_chip_repaints_once_per_target_change`, `hover_clears_when_pointer_leaves` |
| AC.14 | `drag_from_chip_still_selects_text`, `scrollbar_drag_does_not_start_selection`, `drag_inside_split_float_selects_text` |
| AC.15 | `float_click_sends_win_event_click` (lua_float.rs: scrolling band, both pinned bands, clamped tail, chrome), `click_event_table_maps_row_and_col` (win.rs), `app_click_float_delivers_click_event` (app/tests.rs: real `Msg::Mouse` through `App`), `deferred_float_click_is_inert`, `question_form_win_click_event` (spec) |
| AC.16 | `lua_picker_click_confirms_choice` (lua_picker.rs) plus a `plugins/lib/tests/spec.lua` case for the shared Lua list picker |
| AC.17 | `status_content_accepts_click_callback`, `status_content_accepts_legacy_entries`, `status_click_region_keys_freed_on_replace_and_unload`, `click_status_entry_sends_click_region` (probe), `click_status_entry_calls_click_region` (app/tests.rs: publish a snapshot, render, `hit_rect(StatusEntry(id))`, click, assert the probe request) |
| AC.18 | `question_form_click_selects_option`, `question_form_click_custom_row`, `question_form_click_tab_bar`, `question_form_click_review_cell`, `question_form_click_confirms`, `question_form_click_dismisses` (plugins/question/tests/spec.lua) |
| AC.19 | `click_queue_row_focuses`, `click_focused_queue_row_removes` |
| AC.20 | `thumb_matches_ratatui_render`, `thumb_and_position_round_trip`, `position_at_reaches_both_ends`, `picker_drag_lands_on_item_across_sections`, `disabled_scrollbar_registers_no_hit` (restoring the global scrollbar flag) |
| AC.21 | `just lint`, `just test`, `just fmt-check`, `just gen-docs-check` |
| AC.22 | `stale_hits_cleared_when_overlay_closes`, `scroll_drag_cancelled_on_chat_switch`, `scroll_drag_cancelled_on_layout_change` |
| AC.23 | `picker_row_hit_is_inside_overlay_zone`, `top_bar_chip_hit_is_inside_zone`, `menu_row_hit_is_inside_zone`, `zone_registry_reports_overflow` |
| AC.24 | `just gen-docs-check` plus human read of `site/docs/content/keybindings/_index.md`, `site/docs/content/permissions/_index.md`, `AGENTS.md`, `maki-lua/src/api/AGENTS.md` |
| AC.25 | `click_palette_command_row_accepts`, `click_palette_argument_row_accepts`, `click_palette_grid_row_accepts`, `click_palette_row_after_republish_accepts_current_row`, `click_file_completion_cell_accepts` |
| AC.26 | `click_auto_scroll_chip_repins` |
| AC.27 | `menu_keyboard_up_down_changes_selection`, `menu_enter_applies`, `menu_esc_closes_without_action`, `menu_closed_passes_keys_through`, `menu_closes_when_a_modal_opens`, `menu_closes_when_permission_demand_arrives` |
| AC.28 | `click_input_places_caret`, `click_tool_row_still_expands`, `wheel_scroll_unchanged` |
| AC.29 | `git log --oneline main..feat/mouse-interactable` shows the phase commits; each is checked out and `cargo check -p maki-ui --tests` is run at that revision |

Test-infrastructure note: the only new seams are `FloatManager::click_at`,
`EventHandle::click_region`, and the `RequestProbe` new-kind mapping, all directly
unit-testable without a full Lua host. The end-to-end Lua status-click path lives in
`maki-lua/tests/ui.rs` (real host via `spawn_host_for_tests` and `ui_action_rx`): register
the callback, consume the published status snapshot, call `EventHandle::click_region(id)`,
and assert the callback ran; the UI half is the App-level click test above. The
App-to-float path is covered by `app_click_float_delivers_click_event`, which uses the
existing channel plumbing. No test harness is missing.

## Review Strategy

Plan-mode review with `plan_reviewer` before `plan_submit`; all findings fixed in this
file or rebutted. After implementation, dispatch a `general` subagent to review the work
against this plan and the repository guidance (root `AGENTS.md`, `maki-lua/src/api/AGENTS.md`),
reviewing `git diff main...feat/mouse-interactable` rather than the working tree, and
fix or rebut every finding, and re-run until no critical findings remain.

## Documentation Strategy

Eight pages under `site/docs/content/` are generated by `maki-docgen` (`tools`, `plugins`,
`providers`, `configuration`, `lua-api`, `keybindings`, `commands`, `folder-trust`) and must
be regenerated, not hand-edited; the rest, including `permissions`, are hand-written.

- `maki-docgen/src/gen_keybindings.rs`: a static "Mouse" table (scrollbar drag/jump, chips
  and menus, picker rows, outside click, and that dragging still selects text), emitted by
  `generate()`; this produces `site/docs/content/keybindings/_index.md`.
- `maki-docgen/src/gen_config.rs` source, `maki-config/src/lib.rs:1299`: mention drag in the
  `scrollbar` field description; this produces the row in
  `site/docs/content/configuration/_index.md`.
- `site/docs/content/permissions/_index.md` (hand-written): note that the permission
  actions are clickable, and that at very narrow widths a hint cell that does not fit is
  clipped and therefore not clickable (the `s`/`D` affordances stay discoverable here).
- `site/docs/content/lua-api/_index.md`: regenerated from the `#[lua_fn]`/`manual` doc
  blocks for `win:recv` (new click event), `Buf:on` (float vs transcript delivery) and
  `set_status_content` (optional click callback).
- Root `AGENTS.md`: extend the maki-ui bullet with the click registry, and
  `maki-lua/src/api/AGENTS.md` with the click contract, so the next agent finds the seam.

## Risks, Blockers, and Required Decisions

- Single-click confirm in pickers is destructive by design (a click on a model row switches
  the model, a rewind row truncates the conversation, a second click on a focused queue row
  removes it). This is the operator's decision; `Ctrl+D` delete still needs its own two-step
  arming, so no click path can delete a stored item.
- Row-to-screen mapping depends on wrapping: the plan form must reuse `crate::wrap::Measure`
  (narrow-width test required), and the permission prompt must not reintroduce packed hint
  lines if it wants column-accurate cells.
- Structured permission hint cells change narrow-width behavior: today the hint lines wrap,
  after this change a cell that does not fit is clipped and unclickable. This is deliberate
  (a clipped cell cannot be hit accurately) and is documented on the permissions page.
- Outside-click `Esc` must never be synthesized for a focused float: a float's `Esc` is
  plugin-defined and the question form discards answers on it. The dismissible set is
  explicit and tested.
- `HitRegistry` reuse is a correctness hazard: forgetting to clear (or clearing after
  registration) routes clicks to stale rects. Cover with
  `stale_hits_cleared_when_overlay_closes`.
- `ZoneRegistry` silently drops pushes past its cap (`selection.rs:85-112`); Phase 0 raises
  the cap and makes overflow visible, otherwise the new top-bar and menu zones could be
  the ones dropped.
- Scroll drag must be cancelled on chat switch, overlay close, or layout change,
  mirroring `validate_middle_scroll`; a stale drag would scroll the wrong surface.
- A queued mouse event can be matched against the previous paint's registry (the event
  loop drains mouse events before the next `draw`). This is pre-existing and identical for
  `ZoneRegistry`, so behaviour is not regressed, but do not "optimise" the registry to be
  rebuilt only on layout changes without revisiting that ordering.
- Hover repaints on `Moved` cost one frame per target change only. If a terminal floods
  `Moved`, the comparison keeps repaints bounded; no cadence is claimed.
- Changing `StatusContent` from tuples to a struct touches maki-ui's renderer, the Lua
  parser, and the docs; land it in Phase 4 behind the existing snapshot types and update
  generated docs in the same change.
- `set_status_content`'s third element is an API addition; existing two-element calls
  must keep working (backward compatible), covered by `status_content_accepts_legacy_entries`.
- Blocker: none. The decisions already taken by the operator are single-click confirm,
  modals block chrome (a chip click while a modal is open is an outside click that closes
  it), both scrollbar behaviors, all surfaces in one effort, Lua included, and in-place
  menus for mode/thinking.
