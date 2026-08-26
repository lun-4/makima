# Defer + z-order input-requiring tool-call UI

## Goal

Stop input-requiring tool-call UI (permission prompts and the `question`/ask tool
window) from (a) painting *behind* other overlays like the `/model` picker and
(b) silently hijacking the keyboard mid-typing. When such a demand arrives while
the user is busy, hold it pending; show it on top and take focus only after 2s of
input idleness or an explicit yield (the blocking modal closes). Give the user a
*manual* escape too (`Alt+M`): hide whatever input surface is currently active,
keep typing, and bring it back either by submitting the focused input box or by
pressing `Alt+M` again. While a demand is held, a left-aligned hint pinned on
top of the status bar (`(Alt+M Undefer pending model input)`) advertises the
restore.

## Implementation Summary

Two coordinated changes in `maki-ui`, both contained to the app layer plus one
small `FloatManager` addition. No change to the agent, Lua API, or the question
plugin's `split="below"` layout.

The three input-demanding surfaces today:
- `PermissionPrompt` (`maki-ui/src/components/permission_prompt.rs`) — opened at
  `app/mod.rs:1641`, drawn in `render_bottom_panel` (`view.rs:226`), given first
  key precedence in `dispatch_overlay` (`app/mod.rs:782`).
- The question/ask float — opened via `UiAction::OpenWin` (`event_loop.rs:855`)
  with `focus=true, needs_input=true, split="below"` (see
  `plugins/question/question_form.lua:603`); drawn in `render_splits`
  (`view.rs:309`); steals keys via `float_mgr.handle_key` (`app/mod.rs:831`,
  returns true when `focused_id` is set — `lua_float.rs:276`).
- `PendingInput::AuthRetry` (`app/mod.rs:272`) — has no UI; **out of scope** for
  deferral (no modal to defer).

Both bug symptoms stem from draw order and input precedence being hardcoded
independently:
- Draw: `render_bottom_panel` (permission) and `render_splits` (question) run
  *before* `render_picker_overlays` (`view.rs:53`) — so the `/model` picker
  paints over them.
- Input: `dispatch_overlay` routes to `permission_prompt` first and to
  `float_mgr` before the model picker (`app/mod.rs:782`, `:831`, `:942`), so a
  freshly-arrived demand steals keys instantly with no visual cue.

**Approach:** add an input-arbitration queue on `App` (`input_queue` +
`active_input` marker) plus a `last_input` activity timestamp. A demand arriving
while the user is busy (last keystroke < 2s ago) or while another demand is
already active/queued is **enqueued** (FIFO) rather than shown: it is not drawn,
does not reserve layout space, and does not take keyboard focus — keys keep going
to whatever the user is doing (the `/model` picker, the input box). The queue head
promotes to active on 2s of input idleness (checked in `tick`, with a precise
`Cadence::after` wake) or when the blocking modal closes (yield). When active,
the surface is rendered in a new **topmost** pass at the end of `App::view`, on
top of all pickers/modals. The queue (not a single slot) is what makes a second
demand arriving mid-deferral wait its turn instead of becoming an invisible
zombie.

Alongside the automatic queue, a *manual* path reuses the same queue: `Alt+M`
(`key::DEFER_INPUT`) on an active surface snapshots it back into the queue marked
`hold_until_submit`, and the queue head with that marker waits for the user's
next submit instead of the 2s idle timer. The affordance is advertised on both
input surfaces.

A follow-up pass (Phase 10) rounds the manual path out: `Alt+M` becomes a toggle
(pressing it again while a demand is held promotes the held head immediately,
no submit needed), and a pending hold pins a left-aligned
`(Alt+M Undefer pending model input)` hint to the row directly above the status
bar so the deferred demand is visible in the chrome.

**Draw/input-switch invariant:** rendering and input routing share a single
source of truth — `active_input` / the `*_active()` predicates. A queued demand
(`active_input == None`) is never drawn (the topmost pass and `render_splits`
both gate on `*_active()` / `below_is_input()`; the permission prompt is not
even opened until activation). Promotion sets `active_input` and returns
`Dirty::YES`, so the *same frame* that first draws the surface on top is also the
first frame `dispatch_overlay` routes keys to it. The 2s timer only governs *when
promotion fires*; it never creates a "drawn on top but not yet input-active"
window. So there is no draw-first-then-switch-input-later gap.

This is a targeted fix (Option A), not a full overlay-z-stack rewrite. It reuses
the existing `Overlay`/`has_modal_overlay` and `Cadence`/`tick` machinery. The
"stack of windows" is realized conceptually: the active input surface is the
single topmost layer; pending ones wait. See Risks for the multi-demand caveat.

## Implementation Plan

### Phase 1 — Activity tracking and arbitration state (`app/mod.rs`)

1. Add const near the other app consts (after line ~129):
   `const INPUT_DEFER: Duration = Duration::from_secs(2);`
2. Add fields to `App` (near `last_esc`, `app/mod.rs:326`), all `pub(super)` so
   `app::tests` can mutate them (L-NEW-2 — mark the helpers `pub(crate)` too):
   - `last_input: Option<Instant>` — `None` until the first user keystroke.
   - `active_input: Option<InputKind>` — the surface currently shown/focused
     (`None` when nothing is live). This is the fix for H-NEW-1: it is the
     explicit marker that distinguishes "staged but deferred" from "active," so
     a second demand arriving during a deferral **enqueues** instead of becoming
     an invisible zombie.
   - `input_queue: std::collections::VecDeque<InputDemand>` — pending demands in
     arrival order (FIFO). Replaces the single `input_deferral` slot. The
     permission **payload** (`id`, `tool`, `scopes`, `subagent_id`) lives in the
     queue entry, so `permission_prompt` is *not* opened (staged) until
     activation — this keeps `any_overlay_open()`/`permission_prompt.is_open()`
     false while queued, so a queued permission does not hide the input cursor,
     suppress `@`-completion, or steal mouse click-focus (M-1, fixed at the
     source). The question float, by contrast, must be opened at arrival (the Lua
     `win:recv()` loop needs a window), but opening it `focus=false` keeps its
     `focused_id == None` so `FloatManager::is_open()` (Overlay) stays false
     (see M.3 correction — no `release_focus` call is needed).
   - `pending_bell: bool` — set by activation when a bell is due; drained by the
     event loop after `tick`/`update` (Phase 4) since `tick` returns `Dirty`, not
     `Vec<Action>`.
   ```rust
   #[derive(Debug, Clone, Copy, PartialEq, Eq)]
   pub(crate) enum InputKind { Permission, Question }

   #[derive(Debug, Clone)]
   struct PermissionPayload {
        id: String,
        tool: maki_config::ToolKey,
        scopes: Vec<String>,
        subagent_id: Option<String>,
   }

   #[derive(Debug, Clone)]
   struct InputDemand {
        kind: InputKind,
        blocked_by_modal: bool,
        perm: Option<PermissionPayload>, // Some for Permission, None for Question
   }
   ```
   Init all to `None`/empty/`false` in the constructor (alongside `last_esc`,
   `app/mod.rs:426`).
3. Record **typing** activity in `App::update` (`app/mod.rs:595`): in the
   `Msg::Key` and `Msg::Paste` arms only, set
   `self.last_input = Some(Instant::now());` **before** dispatching. `Msg::Mouse`,
   `Msg::Scroll`, and `Msg::Agent` do **not** count — the trigger is "the user is
   currently typing" (M.7), and agent events must never look like user activity.
4. Add helpers (all `pub(crate)` so `app::tests` can drive them — L-NEW-2):
   - `fn is_busy(&self) -> bool { self.last_input.is_some_and(|t| t.elapsed() < INPUT_DEFER) }`
   - `fn has_blocking_modal(&self) -> bool { self.has_modal_overlay() }`
     — safe because `PermissionPrompt::is_modal()` is `false`
     (`permission_prompt.rs:113`) and a queued question float is opened
     `focus=false` (Phase 6) so its `focused_id` stays `None` and
     `FloatManager::is_open()` (`lua_float.rs:650`,
     = `focused_id.is_some()`) is `false`; neither self-counts.
   - `fn reconcile_active(&mut self)` — clears a stale `active_input`: if
     `active_input == Some(Permission) && !self.permission_prompt.is_open()` →
     `None`; if `active_input == Some(Question) && !self.float_mgr.below_is_input()`
     → `None`. (Handles active-surface resolution. Called at the top of `tick`
     and `promote_deferred_if_ready`.)
   - `fn permission_active(&self) -> bool { self.active_input == Some(InputKind::Permission) && self.permission_prompt.is_open() }`
   - `fn question_active(&self) -> bool { self.active_input == Some(InputKind::Question) && self.float_mgr.below_is_input() }`
   - `fn any_input_active(&self) -> bool { self.permission_active() || self.question_active() }`
   - `fn begin_input_demand(&mut self, demand: InputDemand) -> bool` — the single
     arbitration entry point. If `self.is_busy() || self.any_input_active() ||
     !self.input_queue.is_empty()`: `self.input_queue.push_back(demand)` and
     return `true` (queued — caller does NOT open the component, ring, or
     transition). Otherwise set `self.active_input = Some(demand.kind)` and, for a
     Permission, **open** `permission_prompt` now from `demand.perm` (the active
     path opens immediately); return `false` (active immediately). The caller
     (Phase 2) supplies the `InputDemand` (with `perm` for Permission, `None` for
     Question). This replaces the single-slot guard and fixes H-NEW-1: a second
     demand while one is queued enqueues behind it; the active surface is never
     displaced mid-typing (C.1 holds). Because the permission payload lives in
     the queue, `permission_prompt` is not opened while queued — fixing M-1 at
     the source (`any_overlay_open()`/`permission_prompt.is_open()` stay false
     while queued, so the input cursor, `@`-completion, and mouse click-focus
     are untouched).
   - `fn promote_deferred_if_ready(&mut self) -> Dirty` —
     `self.reconcile_active();` then, while the queue is non-empty and nothing is
     active: peek `d = self.input_queue.front()`. **M-NEW-1 stale discard:** a
     queued *Question* whose float already closed (`!self.float_mgr.below_is_input()`)
     is popped and skipped (try the next). A queued *Permission* is never stale
     while queued (it is not opened until activation, and cancellation clears the
     queue via `handle_cancel`, Phase 2 step 7). Then promote when
     `!self.is_busy()` (2s idle) **or** `d.blocked_by_modal &&
     !self.has_blocking_modal()` (blocking modal closed → yield); else stop
     (return `Dirty::NO`). On promote: `let d = self.input_queue.pop_front().unwrap();`,
     `self.activate_deferred_input(d)`, return `Dirty::YES`. If the queue is
     empty or the head isn't ready, `Dirty::NO`. Idempotent.
   - `fn activate_deferred_input(&mut self, d: InputDemand) -> Dirty` —
     `self.active_input = Some(d.kind); match d.kind { Permission => { self.permission_prompt.open(d.perm.id, d.perm.tool, d.perm.scopes, d.perm.subagent_id); } Question => { self.float_mgr.focus_input_window(); self.transition_plan(PlanTrigger::InteractivePrompt); } } let bell = match d.kind { Permission => self.ui_config.bell.permission, Question => self.ui_config.bell.ask }; if bell { self.pending_bell = true; } Dirty::YES`.
     (Opening `permission_prompt` here, from the queued payload, is what keeps
     it closed while queued.) The bell is recorded in `pending_bell` (not
     returned as `Action::Bell`) because promotion can fire from `tick`, which
     returns `Dirty`, not actions.
   - `pub fn take_pending_bell(&mut self) -> bool { std::mem::take(&mut self.pending_bell) }`
   - **No `reassess_input_demand`, no every-tick re-defer.** The lifecycle is:
     `begin_input_demand` (enqueue-or-activate-and-open) → `promote_deferred_if_ready`
     (pop-when-ready, open permission at activation) → `reconcile_active`
     (clear-on-close). The active surface is never re-deferred; queued ones wait
     FIFO. (C.1 fix.)

### Phase 2 — Defer at arrival sites

5. Permission arrival (`app/mod.rs:1640`, `ChatEventResult::PermissionRequest`):
   build the demand with the payload and let `begin_input_demand` open-or-enqueue:
   ```rust
   let demand = InputDemand {
       kind: InputKind::Permission,
       blocked_by_modal: self.has_blocking_modal(),
       perm: Some(PermissionPayload { id, tool: tool.clone(), scopes: scopes.clone(), subagent_id: subagent_id.clone() }),
   };
   let defer = self.begin_input_demand(demand);
   ```
   `begin_input_demand` opens `permission_prompt` from the payload only on the
   active path (`!defer`); on the queued path the payload stays in the queue and
   `permission_prompt` stays closed (M-1). Bell change: today `handle_agent_event`
   returns `Action::Bell` on arrival when `bell.permission`
   (`app/mod.rs:1643-1645`). Replace with: if `!defer && self.ui_config.bell.permission
   { self.pending_bell = true; }` and return `vec![]` instead of `vec![Action::Bell]`.
   (The event loop drains `pending_bell` → `ring_bell`, Phase 4.) If queued, no
   bell now; promotion rings. This unifies the permission and ask bell paths
   (H.1/H.2) and stops the arrival bell for a queued demand.
6. **Factor the `OpenWin` arrival into a testable `App` method (H-NEW-2).**
   `UiAction::OpenWin` (`event_loop.rs:855`) is the shared path for input
   below-splits, non-input below-splits (tool output), panels, and centered
   popups (H.3). Move the gating out of the event loop into a new
   `pub(crate) fn handle_open_win(&mut self, buf, config, focus, event_tx, cmd_rx) -> bool`
   on `App`, returning whether the window went active (true) or was queued
   (false). The event loop's `UiAction::OpenWin` arm becomes a thin caller with
   **no bell logic** (M-2: drop the existing direct `ring_bell()` in this arm;
   rely solely on `pending_bell`, drained in Phase 4 — this avoids a
   double-bell where the arm rings and the drain rings):
   `let _active = app.handle_open_win(buf, config, focus, event_tx, cmd_rx);`
   `handle_open_win` body:
   ```rust
   let is_input_demand = config.needs_input && config.split == Split::Below;
   let defer = is_input_demand && self.begin_input_demand(InputDemand {
       kind: InputKind::Question, blocked_by_modal: self.has_blocking_modal(), perm: None,
   });
   let open_focus = if is_input_demand { !defer } else { focus };
   self.float_mgr.open(buf, config, open_focus, event_tx, cmd_rx);
   // No release_focus here — see "M.3 correction" below. A deferred question
   // is opened focus=false; `remove_windows` runs before the new window is
   // pushed, so it cannot auto-focus the not-yet-active question.
   if is_input_demand && !defer {
       self.transition_plan(PlanTrigger::InteractivePrompt);
       if self.ui_config.bell.ask { self.pending_bell = true; }
   }
   !defer
   ```
   Non-input windows keep their original `focus` and never trigger
   `transition_plan`/bell/deferral. When queued, promotion handles focus +
   plan transition + bell. Because this is an `App` method, `app::tests` can
   drive it directly (H-NEW-2). The event-loop arm calls `handle_open_win` and
   then drains `pending_bell` (see Phase 4 step 11b) — no gating logic in the
   loop. (Cleanup: the old `App::bell_on_ask` helper and its `bell_on_ask_predicate`
   test become dead after this refactor — the ask bell now flows through
   `pending_bell` — so remove both.)
7. Resolution and cancellation (no reassess): the active demand is closed by its
   own path (permission answered at `dispatch_overlay` `app/mod.rs:786`; question
   float closed via `WinCommand::Close` consumed in `float_mgr.tick`).
   `reconcile_active` (Phase 1) clears `active_input` when the active surface
   closes, so the queue head becomes promotable on the next
   `promote_deferred_if_ready`. No explicit resolution call is needed; nothing
   re-defers. **Cancellation:** `handle_cancel` (`app/mod.rs:1422`) already calls
   `self.close_all_overlays()` and `self.permission_prompt = Closed`-equivalent;
   add `self.input_queue.clear(); self.active_input = None;` there so a cancel
   drops all queued demands (this is also what makes queued Permissions never go
   stale — they are cleared on cancel, not orphaned).

### Phase 3 — Input routing gate (`app/mod.rs`)

8. `dispatch_overlay` (`app/mod.rs:781`): change the permission branch guard from
   `self.permission_prompt.is_open()` to `self.permission_active()`. When
   deferred, keys fall through to the current focus (model picker / input box).
   The `float_mgr.handle_key` branch (`app/mod.rs:831`): a deferred question
   float is opened `focus=false`, so `focused_id == None` and `handle_key`
   already returns `false` (`lua_float.rs:276-289`) and needs no change for the
   common case. (See "M.3 correction" below for why no `release_focus` is
   needed.) **L-NEW-1 hardening:** guard the branch with
   `!self.permission_active()` (in addition to `float_mgr.handle_key`'s own
   `focused_id.is_some()`) so that, in the rare two-active-surfaces state, an
   undrawn question float whose `focused_id` is set does not steal keys while the
   permission prompt is the visible topmost surface. `!permission_active()` (not
   `question_active()`) is the right predicate: it suppresses key-stealing by a
   zombie question while a permission is topmost, without narrowing the route for
   general focused non-input floats (panels, popups) that should still receive
   keys. (When the question is the active surface, `permission_active()` is false
   and routing proceeds normally.)
9. `handle_submit` (`app/mod.rs:1389`): no special-casing — submitting is not a
   yield (it starts a new action, not a hand-off to the pending prompt). A
   deferred demand stays deferred until 2s idle or modal close.

### Phase 4 — Promotion drive: tick + cadence (`app/mod.rs`, `event_loop.rs`)

10. `App::tick` (`app/mod.rs:2174`): add `self.reconcile_active()` at the top and
    `| self.promote_deferred_if_ready()` to the poller chain. (`reconcile_active`
    clears `active_input` when the active surface closed during `float_mgr.tick`
    — e.g. a question float closed by `WinCommand::Close` — so the queue head can
    promote. No `reassess` call — removed per C.1.) When promotion fires it sets
    `pending_bell`; the loop drains it (step 11b).
11. `App::update` (`app/mod.rs:595`): after the `match msg` returns its actions,
    call `self.promote_deferred_if_ready();` (which calls `reconcile_active()`
    internally first) so a modal-closing key (or the active permission being
    answered) yields immediately (not waiting for the next 100ms tick). Folding
    the `reconcile_active` call inside `promote_deferred_if_ready` keeps one
    reconciliation site instead of two. Promotion records the bell in
    `pending_bell` (not as an `Action::Bell` on the returned vec) — uniform
    with the tick path, since `update` can't tell whether promotion came from
    `tick` or `update`.
11b. Event loop drain (`event_loop.rs`): after each `self.tick()` (`:629`),
    after `app.update(..)` (`handle_input :1250` and `handle_agent :780`), and
    in the `UiAction::OpenWin` arm, check `app.take_pending_bell()`; if true,
    call the existing `ring_bell()` (`event_loop.rs:868`). This closes H.1/H.2:
    both idle promotion (via tick) and modal-close promotion (via update) route
    through the same `pending_bell` → `ring_bell` path, and the ask surface uses
    `ring_bell` just like the existing arrival path. The `OpenWin`-arm drain
    (deviation from the original M-2, which said the arm would have no bell
    logic) keeps the active-path ask bell immediate instead of ~1 frame late;
    a deferred ask sets `pending_bell` only at promotion, so the arm drain is
    a no-op for it. `ring_bell` is the existing mechanism
    (`event_loop.rs:868-870`), so no new side-effect channel is added.
12. `App::cadence` (`app/mod.rs:2226`): add a term so the loop wakes precisely at
    the 2s mark:
    `Cadence::when(!self.input_queue.is_empty() && !self.any_input_active(), Cadence::after(self.defer_remaining()))`
    where `defer_remaining()` = `self.last_input.map(|t| INPUT_DEFER.saturating_sub(t.elapsed())).unwrap_or(Duration::ZERO)`.
    (`Instant` has no `saturating_add`; `saturating_sub(elapsed)` is the correct
    spelling. `Duration::ZERO` when there was no keystroke wakes immediately,
    which is right: a demand queued only because another was active should not
    wait 2s once the queue empties.) Wake only when there is a queued demand
    awaiting promotion and nothing is active. Even without this, `IDLE_POLL`
    =100ms bounds staleness (`repaint.rs:37`), but the precise wake avoids up
    to 100ms jitter.

### Phase 5 — Draw on top + hide-while-deferred (`app/view.rs`)

13. `compute_layout` (`view.rs:60`):
    - `permission_open` (`view.rs:61`) → `self.permission_active()`.
    - `form_visible` (`view.rs:44`) → `self.permission_active() || self.plan_form_active()`.
    - Below-split filter (`view.rs:79-84`): the existing predicate is
      `.filter(|r| !(permission_open && r.split == Split::Below))`. Extend the
      **drop** condition (inside the negation, not appended — M.2) so a deferred
      (queued, not active) input below-split also drops:
      `.filter(|r| !((permission_open && r.split == Split::Below) || (r.split == Split::Below && self.below_input_hidden())))`,
      where `below_input_hidden()` = `self.float_mgr.below_is_input() && !self.question_active()`
      (a below+needs_input window that is *not* the active surface — i.e. queued).
      (When queued, the question split reserves no space; when active it does.)
    - **H.4:** update the two other `form_visible` sites that mirror line 44 —
      `layout_geometry`/`top_bar_rect` (`view.rs:492`, `:505`) compute
      `form_visible = self.permission_prompt.is_open() || self.plan_form_active()`.
      Change both to `self.permission_active() || self.plan_form_active()` so
      `compute_layout`'s `form_visible` *parameter* (used for `bottom_takeover`
      and the `else if form_visible { plan_form.height() }` branch at
      `view.rs:89,95`) matches `view()`. Without this, a queued permission
      would reserve `plan_form.height()` (=0) and shrink the input area to 0 in
      `layout_geometry`, contradicting the on-screen render.
14. `render_bottom_panel` (`view.rs:225`): the `if self.permission_prompt.is_open()`
    branch becomes `if self.permission_active() { /* paint nothing here; the
    topmost pass owns the active permission form */ }` — i.e. when active, leave
    the reserved `bottom_area` for `render_active_input`; when queued,
    `permission_active()` is false so this branch is skipped and the input box /
    panels render normally (full height, since layout didn't reserve for it).
15. `render_splits` (`view.rs:306`): skip `Split::Below` whenever
    `self.float_mgr.below_is_input()` — both when active (the topmost pass owns
    it, so no redundant first draw — L-3) and when queued (it is hidden). Plain
    below splits (non-input) draw normally. (The topmost pass is the sole drawer
    of the active question split; the layout still reserved its space because
    `below_input_hidden()` is false when active, so `compute_layout` carved the
    rect.)
16. New `render_active_input(&mut self, frame, layout)` called as the **last**
    step of `App::view` (`view.rs:43`, after `apply_selection`):
    - `if self.permission_active()` → first `frame.render_widget(Clear, layout.bottom_area)` then `frame.render_widget(Block::default().style(bg), layout.bottom_area)` then `self.permission_prompt.view(frame, layout.bottom_area)`. The `Clear`+bg fill is required: `permission_prompt.view` → `render_form` (`permission_prompt.rs:355-362`) draws only a bordered `Paragraph` with no background, so without the fill the picker beneath bleeds through and AC.5's negative assertion (picker label absent from the overlap) fails. `view_split` already clears for the question path, so no fill is needed there.
    - `else if self.question_active()` → `if let Some(rect) = layout.splits.rect(Split::Below) { self.float_mgr.view_split(frame, Split::Below, rect); }`
    (Re-rendering the same rect on top of the pickers is what makes it "on top";
    `view_split` does `Clear` + paint — `lua_float.rs:342-350`.)
17. `register_zones` (`view.rs:412`): **gate the guard** on active-ness (M.1) and
    push the active-input zone exactly once, topmost (M-NEW-2).
    - Change the guard at `view.rs:436`
      (`if self.permission_prompt.is_open() || self.plan_form_active()`) to
      `if self.plan_form_active()` — **drop the permission case entirely** here
      (M-NEW-2). The active-permission `bottom_area` is pushed by the new
      topmost step below, not at `:437`; a queued permission pushes nothing (no
      zone, no shadow on Input — M.1).
    - In the `Split::ALL` loop (`view.rs:452-456`), skip `Split::Below` when
      `self.float_mgr.below_is_input()` (the active question split is pushed
      topmost below; a queued one pushes nothing).
    - After the `overlay_rect` push (`view.rs:460`, the current topmost), push
      the active-input zone as the **last** entry so it wins the reverse walk:
      `if self.permission_active() { self.zones.push_overlay(layout.bottom_area); } else if self.question_active() { if let Some(rect) = layout.splits.rect(Split::Below) { self.zones.push_overlay(selection::inset_border(rect)); } }`.
      This is the single topmost push (no double-push).

### Phase 6 — FloatManager additions (`components/lua_float.rs`)

18. Add:
    - `pub fn below_is_input(&self) -> bool` →
      `self.windows.iter().any(|w| w.visible && w.config.split == Split::Below && w.config.needs_input)`
      (include `w.visible` so a hidden window does not count).
    - `pub fn focus_input_window(&mut self)` → find the window with
      `visible && config.split == Split::Below && config.needs_input` and set
      `self.focused_id = Some(win.id)` (for promotion). No-op if none.
    - `pub fn is_focused(&self) -> bool` → `self.focused_id.is_some()` (used by
      the mouse scroll gate, Phase 7 step 23).
19. **M.3 correction (no `release_focus`).** The original plan added a
    `release_focus()` call on the deferred-open path to clear a stale focus.
    That was wrong: `FloatManager` is modal by default (`Overlay::is_modal()`
    defaults `true`), so a focused popup/panel makes `has_blocking_modal()` true.
    A question arriving behind it is queued with `blocked_by_modal: true`;
    `release_focus()` would then clear that unrelated popup's focus, flipping
    `has_blocking_modal()` to false, and the very next
    `promote_deferred_if_ready` would promote immediately via the
    `blocked_by_modal && !has_blocking_modal()` clause despite the user being
    busy — defeating deferral and defocusing the in-use popup (violates AC.7 in
    that scenario). So **do not add `release_focus`**. It is unnecessary anyway:
    `FloatManager::open` calls `remove_windows` (same-split eviction) *before*
    pushing the new window, so a deferred question opened `focus=false` is never
    auto-focused by `remove_windows`'s `focused_id` reassignment. The deferred
    question therefore keeps `focused_id == None`, so `handle_key` returns false,
    `Overlay::is_open == false`, and `has_blocking_modal` does not self-count —
    all the properties the original M.3 wanted, without the side effect.
    (Promotion restores focus via `focus_input_window`.)
20. `needs_input()` (`lua_float.rs:270`) is **removed**, not left unchanged.
    After Phase 7 switches `awaiting_input`/`attention` to the `*_active()`
    predicates, `needs_input()` has no production caller, and `clippy --all
    -D dead-code` rejects it. Its two unit tests convert to `below_is_input`.

### Phase 7 — Status / attention / mouse gating (`app/mod.rs`)

21. `awaiting_input` (`app/mod.rs:2147`): gate on active-ness so a queued
    demand does not report "needs input" (no nag while the user is typing):
    `self.permission_active() || self.pending_input != PendingInput::None || self.question_active()`.
    (Replaces the old `float_mgr.needs_input() && input_deferral.is_none()`
    term with `question_active()`, which already requires the float to be the
    active surface.)
22. `attention` (`app/mod.rs:528`): gate the `PermissionRequested` arm on
    `permission_active()` and the `QuestionRequested` arm on `question_active()`,
    so notifications reflect active state, not arrival. **M.5 delivery
    guarantee:** `attention()` is polled by `notifications.reconcile` in the
    event loop (`event_loop.rs:952`), which runs every loop iteration after
    `tick`/`update` and after promotion (Phase 4 step 11b's drain site is
    adjacent). Because promotion sets `active_input = Some(kind)` (making
    `*_active()` true) on the same tick, the next `reconcile` sees the active
    state and fires the notification. Confirm `reconcile` is called on the
    post-promotion repaint (it is — the loop calls it each iteration); no extra
    hook needed.
23. **M.6 (mouse routing):** the mouse handler uses
    `float_mgr.is_open()` (`app/mod.rs:663`, the inherent `!windows.is_empty()`
    at `lua_float.rs:519`) and `float_mgr.contains()` (hit-tests `focused_rect`,
    `lua_float.rs:505-507`). A deferred question float has windows present but
    `focused_id == None` so `focused_rect` is stale/`None` and `contains()`
    returns false — mouse clicks already miss it. But the `app/mod.rs:663`
    `float_mgr.is_open()` branch still routes scroll to it. Gate that branch on
    `self.float_mgr.is_focused()` (the new helper from Phase 6 step 18; the
    `focused_id.is_some()` option) so a deferred, undrawn question float does not
    capture mouse scroll. (This mirrors the keyboard gate: only the *active*
    input surface takes input.)

### Phase 8 — Test audit (`app/tests.rs`)

24. Audit existing tests that open a permission prompt or a needs-input float and
    then assert behavior. Because `last_input` starts `None` (idle), demands
    opened without prior keystrokes activate immediately — the common case is
    unaffected. Fix any test that sends a `Msg::Key` *then* triggers a demand
    (it would now defer) by resetting `app.last_input = None;` before the demand,
    or by asserting the new deferred behavior intentionally. Known relevant:
    `ctrl_c_denies_permission_prompt`, `permission_prompt_takes_bottom_precedence_over_below_split`,
    `attention_float_marks_app_as_awaiting_input_until_close`,
    `cancel_clears_pending_input` (`app/tests.rs:1793`).

### Phase 9 — Manual `Alt+M` hold-and-queue deferral (extension)

The queue from Phase 1 is the single slot; `Alt+M` moves an *active* surface
back into it with a marker that changes *when* it promotes. No new timers, no new
arbitration entry point — just a manual demotion and a submit-driven release.

25. **Keybinding** (`components/keybindings.rs`): add `key::DEFER_INPUT = Alt+M`
    next to `EDIT_INPUT` (modifiers `ALT` is already the `Alt+O` pattern), plus a
    `KEYBINDS` help entry in the `General` context so it shows in the in-app
    keybindings help and the generated keybindings docs.
26. **Demand marker** (`app/mod.rs`): `InputDemand` gains `hold_until_submit:
    bool`, default `false` on both Phase-2 arrival constructors (the permission
    request and the `UiAction::OpenWin` question) so auto-deferral is unchanged.
27. **Release flag** (`app/mod.rs`): `App` gains `submit_released: bool` (init
    `false`), armed by any keyboard submit — at the top of `handle_submit`
    (`app/mod.rs:1439`, covers the main input box, `exit`, and `!`-shell also)
    and in `handle_subagent_chat_key`'s `InputAction::Submit` arm. It is consumed
    once by the next promotion pass (`std::mem::take`, so it cannot leak into a
    later promotion after a normal submit).
28. **`defer_active_input(&mut self) -> bool`** (`app/mod.rs`): the manual
    demotion. If `permission_active()`, snapshot the open prompt's
    `id/tool/scopes/subagent_id` via `active_permission_payload()`, close the
    prompt, clear `active_input`, and enqueue a `hold_until_submit` Permission
    demand. If `question_active()`, call `float_mgr.release_focus()` (the window
    stays open) and enqueue a `hold_until_submit` Question demand. Otherwise a
    no-op returning `false`. Called from `dispatch_overlay` via
    `toggle_defer_input` (Phase 10 step 34), so `Alt+M` is consumed even as a
    no-op — a reserved hotkey that never types a char — and it works for both
    surfaces without either of their existing branches eating the key.
29. **Promotion change** (`promote_deferred_if_ready`, `app/mod.rs:2300`): after
    `reconcile_active`, take `submit_released` once; for a front demand with
    `hold_until_submit`, `ready = submit_released` (ignore `is_busy()` and the
    `blocked_by_modal` clause). Non-held demands keep the Phase-1 readiness rule.
    A manual hold therefore *never* promotes on the 2s idle timer or a modal
    close — only after the focused input box submits, which re-appears the
    surface (and rings its bell via the existing `activate_deferred_input`).
30. **Why submitted text is queued, not misdelivered** (`app/mod.rs`): deferring
    a permission leaves the agent parked on its answer guard with the run still
    `Status::Streaming`, so a main-box submit already routes through
    `submit_or_queue` → `queue_and_notify` and lands in the shared queue as a new
    user message. The deferral only *releases* the hold; it never redirects the
    typed text into the tool's answer channel.
31. **Float manager** (`components/lua_float.rs`): add `release_focus()` (clears
    `focused_id`/`focused_rect`, the inverse of `focus_input_window`). This is the
    one deliberate focus release; Phase 6 step 19's M.3 avoidance of
    `release_focus` does not apply because the float being released *is* the
    active surface rather than a pre-existing popup, and the enqueued demand is
    `blocked_by_modal: false` + `hold_until_submit: true`, so it cannot be
    promoted prematurely through the modal-close clause.
32. **Affordance on the ask window** (`lua_float.rs:render_window`): when the
    window is `Split::Below` + `needs_input` (the active ask), add a
    left-aligned `Alt+M defer` bottom title next to the existing right-aligned
    footer. Only the active input window is ever rendered (Phase 5), so the hint
    never appears on a queued/hidden window.
33. **Affordance on the permission prompt** (`permission_prompt.rs`): append
    `HINT_DEFER_ROW = (key::DEFER_INPUT.label, "defer, keep typing")` to the
    Normal-state hint rows.

### Phase 10 — Undefer toggle + status-bar hint (follow-up)

As originally specified, a manual hold released only via the next submit, and a
deferring user lost all sight of the demand. This follow-up makes `Alt+M` a true
toggle and surfaces the held state in the chrome.

34. **Toggle** (`app/mod.rs`): add `toggle_defer_input(&mut self) -> bool`
    ahead of `defer_active_input`. It first tries `defer_active_input()` (the
    demotion path). If that returns `false` — nothing was active — and the queue
    head is `hold_until_submit`, it arms `submit_released = true` and runs
    `promote_deferred_if_ready()` on the spot, so the held head re-promotes on
    the same keystroke (focus and bell via the normal activation path).
    FIFO safety falls out of inspecting the *head* only: an auto-deferred demand
    sitting in front is never reached past (it keeps its own idle/modal release),
    and with an empty queue the key stays a consumed no-op (AC.17 unchanged).
    `dispatch_overlay`'s `DEFER_INPUT` branch now calls this instead of
    `defer_active_input` directly.
35. **Pending query** (`app/mod.rs`): add
    `held_input_pending(&self) -> bool` —
    `self.input_queue.iter().any(|d| d.hold_until_submit)` — the view layer's
    only new dependency on the queue.
36. **Hint row** (`app/view.rs`): when `held_input_pending()`,
    `compute_layout` carves one extra full-width row between the content region
    and the status bar (`Constraint::Length(defer_hint_h)` added to the opening
    vertical split; `ViewLayout` gains `defer_hint_area`). A zero-length
    constraint makes the extra carve a no-op when nothing is held.
    `render_defer_hint(frame, layout.defer_hint_area)` draws the row flush-left
    as `(key::DEFER_INPUT.label Undefer pending model input)` — paren and
    description in `tool_dim`, key label in `keybind_key` — right after
    `render_status_bar`, bailing out on `area.height == 0`. The hint vanishes
    automatically once promotion drains the hold.
37. **Help copy** (`components/keybindings.rs`): the `DEFER_INPUT` `KEYBINDS`
    entry's description becomes "Defer an input prompt, press again to restore",
    flowing into `site/docs/content/keybindings/_index.md` via
    `cargo run -p maki-docgen`.

## Acceptance Criteria

- **AC.1** A permission prompt arriving while the user is typing (last keystroke
  < 2s ago) is *not* answered by keystrokes and does not take focus: keys keep
  going to the current focus. Verified by `permission_deferred_while_typing`.
- **AC.2** A deferred permission prompt becomes active (focus + drawable) once 2s
  elapse since the last keystroke, with no real-time sleep. Verified by
  `permission_promotes_after_idle`.
- **AC.3** A deferred demand promotes immediately when the blocking modal closes
  (e.g. Esc closes the `/model` picker). Verified by
  `permission_promotes_on_modal_close`.
- **AC.4** When idle (no recent input), a permission prompt activates
  immediately (no regression vs. today). Verified by
  `permission_immediate_when_idle`.
- **AC.5** An *active* permission prompt paints on top of an open `/model`
  picker: in the screen region where the two overlap, the rendered cells show the
  permission prompt's content (e.g. its title), not the picker's. Verified by
  `permission_drawn_on_top_of_model_picker`.
- **AC.6** A *deferred* permission prompt is not drawn and reserves no bottom
  area (input box keeps full height). Verified by
  `permission_hidden_while_deferred`.
- **AC.7** A `question`/ask float arriving while busy does not steal focus
  (`focused_id` stays `None`, keys reach the model picker); after promotion it
  takes focus and receives keys. Verified by `question_float_deferred_then_promoted`.
  The behind-a-focused-popup case (the deferred question must not defocus the
  popup nor promote immediately via the modal-close clause) is verified by
  `question_deferred_behind_focused_popup_waits_for_idle` (M.3 regression guard).
- **AC.8** An active question float paints on top of an open picker: in the
  overlapping region the rendered cells show the question window's content, not
  the picker's. Verified by `question_drawn_on_top_when_active`.
- **AC.9** Bell/attention for a permission or ask demand fires on promotion, not
  on arrival while deferred. Verified by `bell_deferred_until_promotion`.
- **AC.10** (M-NEW-4, C.1 regression guard) An *active* question/permission
  surface stays active across `tick`s while the user keeps typing — it is never
  re-deferred or vanished. Verified by `active_surface_survives_typing_ticks`.
- **AC.11** (H-NEW-2) A non-input window opened via the `OpenWin` path (tool-output
  below split, panel, centered popup) while the user is busy is *not* deferred:
  it keeps its original `focus`, does not trigger `transition_plan`, and is drawn
  normally. Verified by `non_input_window_not_deferred`.
- **AC.12** (H-NEW-1) A second input demand arriving while the first is *queued*
  (not yet active) enqueues behind it (FIFO) rather than becoming an invisible
  zombie: neither is active until the head promotes, and the second does not steal
  focus or ring a bell on arrival. Verified by `second_demand_enqueues_not_zombie`.
- **AC.13** (M-1) A *queued* permission prompt does not contaminate
  `any_overlay_open()`: while it is queued (user typing, no picker open), the
  input cursor stays visible, `@`-file-completion still renders, and a mouse
  click in the input area focuses it. Verified by `queued_permission_no_overlay_side_effects`.
- **AC.14** An active permission prompt or ask float shows the `Alt+M` defer
  affordance. Verified by render assertions in `permission_drawn_on_top_of_model_picker`
  and `question_drawn_on_top_when_active`.
- **AC.15** `Alt+M` hides the active surface, clears its focus, returns focus to
  the input box, and (re)queues the demand marked `hold_until_submit`. Verified by
  `alt_m_defers_active_permission_until_submit` and
  `alt_m_defers_active_question_until_submit`.
- **AC.16** A manually-held demand does **not** promote on 2s idle or on modal
  close; it promotes after the focused input box submits **or** on a second
  `Alt+M` press (AC.18), re-appearing and ringing its bell. The idle-immunity
  half is asserted in the AC.15 tests by tuning `last_input` to idle and ticking
  with the hold still queued.
- **AC.17** Typing/submitting at the main input box while a demand is held never
  misdelivers the text as the tool answer; it flows through the normal
  submit/queue path. `Alt+M` with no active surface is a harmless no-op. Verified
  by `alt_m_when_nothing_active_is_noop`.
- **AC.18** (Phase 10) Pressing `Alt+M` again while a demand is held promotes
  the held head immediately — queue drains, focus restored, no submit needed.
  Verified by `alt_m_toggles_back_to_the_held_permission`.
- **AC.19** (Phase 10) The toggle respects FIFO order: an auto-deferred head
  (typing window) is not force-promoted by `Alt+M` and keeps releasing via its
  own idle rule. Verified by `alt_m_toggle_ignores_auto_deferred_head`.
- **AC.20** (Phase 10) While a demand is held, a left-aligned
  `(Alt+M Undefer pending model input)` hint occupies the row directly above the
  status bar; it is absent while the surface is active and clears once the hold
  is restored. Verified by `defer_hint_pins_above_status_bar_until_restored`.

## Test Strategy

All tests are unit tests in `maki-ui/src/app/tests.rs` (and one or two in
`lua_float.rs`'s test module), using `test_app()`, direct field mutation
(`app.last_input`, `app.active_input`, `app.input_queue`, `app.pending_bell` are
`pub(super)`; the helpers `begin_input_demand`/`promote_deferred_if_ready`/
`handle_open_win` are `pub(crate)` — accessible from the `app::tests` module),
`app.update(Msg::Key(..))`, `app.tick()`, `app.layout_geometry(TEST_AREA)`, and
the `rendered(app)`/`buffer_text` render harness (`tests.rs:2338`,
`components/mod.rs:414`). Time is controlled by setting `last_input` to
`Instant::now() - Duration::from_secs(3)` (idle) or `Some(Instant::now())` (busy)
— **no sleeps, no flakiness**.

The `OpenWin` arrival path is exercised via the new `App::handle_open_win`
method (Phase 2 step 6), so AC.7/AC.11/AC.12 are drivable from `app::tests`
without an event-loop harness (H-NEW-2 fix). **Test-layer boundary (M-NEW-3):**
the event loop's `pending_bell` → `ring_bell` drain itself is not unit-tested
(`ring_bell` writes `\x07` to stdout); tests assert the `pending_bell` flag at
the App boundary, which is the testable seam. The "no bell on arrival while
queued" half of AC.9 is therefore verified by `pending_bell == false` after
`handle_open_win`/`begin_input_demand` returns deferred — this proves the App
suppressed the arrival bell; the loop's drain is a thin passthrough covered by
code reading.

| AC | Test | Layer |
|----|------|-------|
| AC.1 | `permission_deferred_while_typing` | unit |
| AC.2 | `permission_promotes_after_idle` | unit |
| AC.3 | `permission_promotes_on_modal_close` | unit |
| AC.4 | `permission_immediate_when_idle` | unit |
| AC.5 | `permission_drawn_on_top_of_model_picker` | render |
| AC.6 | `permission_hidden_while_deferred` | render + layout |
| AC.7 | `question_float_deferred_then_promoted` | unit |
| AC.7 | `question_deferred_behind_focused_popup_waits_for_idle` | unit |
| AC.8 | `question_drawn_on_top_when_active` | render |
| AC.9 | `bell_deferred_until_promotion` | unit |
| AC.10 | `active_surface_survives_typing_ticks` | unit |
| AC.11 | `non_input_window_not_deferred` | unit |
| AC.12 | `second_demand_enqueues_not_zombie` | unit |
| AC.13 | `queued_permission_no_overlay_side_effects` | unit + render |
| AC.14 | `permission_drawn_on_top_of_model_picker` + `question_drawn_on_top_when_active` (hint asserts) | render |
| AC.15 | `alt_m_defers_active_permission_until_submit` + `alt_m_defers_active_question_until_submit` | unit |
| AC.16 | same as AC.15 (idle-immunity half) | unit |
| AC.17 | `alt_m_when_nothing_active_is_noop` | unit |
| AC.18 | `alt_m_toggles_back_to_the_held_permission` | unit |
| AC.19 | `alt_m_toggle_ignores_auto_deferred_head` | unit |
| AC.20 | `defer_hint_pins_above_status_bar_until_restored` | render |

The `Alt+M` tests use an `alt_m()` key helper (`KeyCode::Char('m')` +
`KeyModifiers::ALT`) and drive the release via `app.handle_submit(Submission::
empty())`, which arms `submit_released` without starting a run, then
`promote_deferred_if_ready()`. The Phase-10 toggle tests (AC.18/AC.19) drive the
release with a second `alt_m()` press instead.

Test sketches (a test-local `perm_demand(id, tool, scopes)` helper builds
`InputDemand { kind: Permission, blocked_by_modal: false, perm: Some(PermissionPayload{..}) }`;
`question_demand()` builds the `Question` variant with `perm: None`):
- `permission_deferred_while_typing`: `app.last_input = Some(Instant::now())`;
  call `begin_input_demand(perm_demand(..))` (returns true, queued); assert
  `app.input_queue.len() == 1`, `!app.permission_active()`, and
  `!app.permission_prompt.is_open()` (queued → not staged, M-1); send
  `Msg::Key('y')`; assert no answer sent to a spy `answer_tx`/agent channel and
  `permission_prompt` still closed.
- `permission_promotes_after_idle`: queue as above; set
  `app.last_input = Some(Instant::now() - Duration::from_secs(3))`; call
  `app.tick()`; assert `app.input_queue.is_empty()`, `permission_active()`
  (which requires `permission_prompt.is_open()` — opened at activation), and
  that a `Msg::Key('y')` now answers.
- `permission_promotes_on_modal_close`: open model picker (`run_builtin(ModelPicker)`),
  set `last_input = Some(now)`, queue a permission with `blocked_by_modal: true`,
  send `Esc` via `update` to close the picker, then assert
  `app.input_queue.is_empty()` and `permission_active()` (yield fired in
  `update`'s post-dispatch `promote_deferred_if_ready`).
- `permission_immediate_when_idle`: `app.last_input = None`; call
  `begin_input_demand(perm_demand(..))` (returns false, active); assert
  `app.input_queue.is_empty()`, `permission_active()`, and `'y'` answers
  (guards regression).
- `permission_drawn_on_top_of_model_picker`: open model picker + active
  permission; render via the `rendered`/`buffer_text` harness. The model picker
  is a centered modal; the active permission paints `bottom_area` last. Assert
  occlusion in the overlap: compute the picker's centered rect (or use
  `layout_geometry`), and within the cells where both would draw, assert the
  permission title (`"Permission Required"`) is present **and** a known picker
  label that would otherwise occupy those cells is absent. (Mere `contains`
  passes against the buggy draw order too; the negative half — picker label gone
  from the overlap — is what proves the prompt won.)
- `permission_hidden_while_deferred`: queue a permission; `rendered` does **not**
  contain `"Permission Required"`; `layout_geometry` (with `form_visible` now
  gated on `permission_active()`, Phase 5 H.4) shows the input box at full
  height (no reserved bottom form).
- `question_float_deferred_then_promoted`: with `last_input=Some(now)`, call
  `app.handle_open_win(buf, below_needs_input_config(focus=true), true, event_tx, cmd_rx)`;
  assert it returned `false` (queued), `float_mgr` `focused_id` is `None`
  (opened `focus=false`; no `release_focus` — see M.3 correction),
  `app.input_queue` has one Question entry, and a key reaches the model
  picker; then `app.last_input = Some(now-3s)`; `app.tick()`; assert
  `focus_input_window` ran (`focused_id.is_some()`) and the next key is
  received on the window's `event_rx`.
- `question_deferred_behind_focused_popup_waits_for_idle` (AC.7, M.3 guard):
  open a focused centered popup (`split=None, focus=true`); assert
  `has_blocking_modal()` is true. With `last_input=Some(now)`, call
  `handle_open_win` for a question; assert it returned `false` (queued),
  the popup keeps its focus (`is_focused()`), `has_blocking_modal()` still
  true, `!question_active()`. `tick()` while still busy: assert
  `!question_active()` (no immediate promotion via the modal-close clause).
  Set `last_input=Some(now-3s)`, `tick()`: assert `question_active()`
  (promotes only after idle, popup still open).
- `question_drawn_on_top_when_active`: open model picker + active question
  float; render and assert occlusion in the overlap — the question window's
  title/content is present and a competing picker label that would occupy those
  cells is absent (same negative-assertion technique as AC.5).
- `bell_deferred_until_promotion`: with `bell.permission`/`bell.ask` enabled,
  queue a demand (via `handle_open_win`/`begin_input_demand`); assert
  `app.pending_bell == false` (arrival bell suppressed); promote (idle or
  modal-close) via `tick`/`update`; assert `app.take_pending_bell()` returns
  true. (The event loop translates `pending_bell` → `ring_bell`; the unit test
  asserts the `pending_bell` flag, which is the testable boundary, since
  `ring_bell` writes to stdout and is not unit-testable.) Cover both promotion
  paths: idle (`tick`) and modal-close (`update`). Note (L-2): the active
  permission arrival sets `pending_bell` during `handle_agent`'s `app.update`
  (`event_loop.rs:778`); it is drained on the next `tick` (`:629`), so no bell is
  lost, just ~1 frame later than today's immediate `Action::Bell`.
- `active_surface_survives_typing_ticks` (AC.10, C.1 guard): promote a question
  float (idle → `tick`); set `app.last_input = Some(Instant::now())` (user
  typing the answer); call `app.tick()` several times; assert `question_active()`
  stays true, `focused_id` stays `Some`, and the surface is still drawn
  (`rendered` still contains the question content). This catches a regression of
  the original C.1 bug (every-tick re-deferral vanishing the active surface).
- `non_input_window_not_deferred` (AC.11, H-NEW-2): with `last_input=Some(now)`
  (busy), call `app.handle_open_win(buf, non_input_config(focus=true), true, ...)`
  for each of: a tool-output below split (`needs_input=false, split=Below`), a
  panel (`split=Panel`), and a centered popup (`split=None`). For each, assert
  the return is `true` (not deferred), `float_mgr.focused_id` reflects the
  passed `focus`, `app.input_queue` is empty, `transition_plan` was not invoked
  (mode unchanged), and `pending_bell == false`.
- `second_demand_enqueues_not_zombie` (AC.12, H-NEW-1): with `last_input=Some(now)`,
  queue a permission (`begin_input_demand(perm_demand(..))`); then queue a
  question via `handle_open_win` (also busy). Assert `app.input_queue` has two
  entries [Permission, Question] in arrival order, `!permission_active()`,
  `!question_active()`, the question's `focused_id` is `None`,
  `!permission_prompt.is_open()`, and `pending_bell == false` (no zombie bell).
  Then set idle and `tick()`: the Permission (head) promotes first
  (`permission_active()`, queue length 1); answer it (closes the prompt,
  `reconcile_active` clears `active_input`); `tick()` again → the Question
  promotes (`question_active()`, queue empty). At no point are zero surfaces
  visible while a demand is staged.
- `queued_permission_no_overlay_side_effects` (AC.13, M-1): with no picker open
  and `last_input=Some(now)`, queue a permission; assert `!app.any_overlay_open()`,
  `!app.permission_prompt.is_open()`; render and assert the input box cursor is
  visible and (if completion is active) `file_completion` renders; assert
  `!app.has_modal_overlay()` so a mouse click in the input area would focus it
  (verify via `register_zones`/`layout_geometry` that the Input zone is not
  shadowed by an Overlay zone at the input rect).
- `alt_m_when_nothing_active_is_noop` (AC.17): with `last_input=None` and no
  surface open, send `alt_m()` via `update`; assert empty actions, empty queue,
  and `!permission_active()`/`!question_active()`.
- `alt_m_defers_active_permission_until_submit` (AC.15/AC.16): idle-activate a
  permission (`begin_input_demand` returns false); `update(alt_m())`; assert
  `!permission_active()`, `!permission_prompt.is_open()`, queue length 1 with
  `hold_until_submit`. Set `last_input` idle and `tick()` → assert the hold still
  queued (idle-immunity); a typed `'h'` does not open/misanswer the prompt; then
  `handle_submit(Submission::empty())` + `promote_deferred_if_ready()` → prompt
  re-opens and the queue drains.
- `alt_m_defers_active_question_until_submit` (AC.15/AC.16): idle-open a
  question (`open_question_win(..)` active, float focused); `update(alt_m())`;
  assert `!question_active()`, `!float_mgr.is_focused()` (focus released),
  queue length 1 with `hold_until_submit`; idle `tick()` keeps it held; submit +
  promote → `question_active()` and float focused again.

Add a `lua_float.rs` unit test for `below_is_input()` and `focus_input_window()`
(toggle `focused_id` to the below+needs_input window), plus
`release_focus_drops_only_focus_the_window_stays` (Phase 9 step 31): open a
below+needs_input window focused, `release_focus()` clears focus while the
window stays open (`below_is_input()` still true).

Scope to the crate while iterating: `cargo check -p maki-ui --tests`,
`cargo clippy -p maki-ui --tests -- -D warnings`, `cargo nextest run -p maki-ui`.
Final: `just ci` on the remote build box (`.ssh/remote-ci.sh`).

## Review Strategy

- **Plan-mode:** a `plan_reviewer` pass before `plan_submit`; fix/rebut all
  critical/high findings, re-review if any remain.
- **Implementation:** after tests pass, dispatch a `general` subagent to review
  the completed diff against this plan and the codebase conventions in
  `AGENTS.md` (no trivial comments, KISS/DRY, explicit error handling, test
  placement). Fix or rebut all findings; re-run review if critical findings
  remain.

## Documentation Strategy

No hand-written user-facing docs change is required: the behavior is a
correctness fix to existing UI (permission prompts, the ask tool) and is covered
by existing docs on permissions/ask. The `Alt+M` affordance is surfaced through
the generated keybindings docs: registering `key::DEFER_INPUT` in `KEYBINDS`
(Phase 9 step 25) flows into `site/docs/content/keybindings/_index.md` via
`just gen-docs`, so no prose edit is needed. `AGENTS.md` needs no update
(architecture unchanged). The Phase-10 follow-up changes only the generated
`Alt+M` description text (step 37), regenerated the same way.

## Risks, Blockers, and Required Decisions

- **Ordered queue, not a single slot (H-NEW-1 resolved).** `input_queue` +
  `active_input` replace the single deferral slot. A second demand arriving while
  the first is queued enqueues behind it (FIFO) instead of becoming an invisible
  zombie. Only the queue head can be active; the active surface is never
  displaced or re-deferred (C.1). When the active demand resolves,
  `reconcile_active` clears `active_input` and the head promotes on the next
  `promote_deferred_if_ready`. `render_active_input` draws permission with
  precedence over question (the only two input kinds). The queue is bounded in
  practice (rare to have >2 staged), so no cap is needed; if a queued surface
  closes before promotion, `promote_deferred_if_ready` discards the stale entry
  (M-NEW-1).
- **Same-kind concurrent demands (M-3, non-goal).** The queue holds N
  `InputDemand`s, but the underlying surfaces are single-slot: `PermissionPrompt`
  is one `Open/Closed` enum and `FloatManager::open` evicts same-split windows
  (`lua_float.rs:195-198`). For **Permission** this is *not* a problem: the
  payload lives in the queue and `permission_prompt` is opened only at
  activation, so a second queued Permission just waits — no clobbering. For
  **Question**, a second ask arriving while one is active/queued calls
  `float_mgr.open(split=Below)` which evicts the first's float; the first's
  `win:recv()` loop then errors/closes. Two simultaneous ask-tool calls (e.g.
  concurrent subagents both asking) is pathological and out of scope for this
  plan; if it occurs the second evicts the first and the first's Lua loop should
  treat the close as a dismiss. AC.12 covers the common different-kind case
  (Permission + Question). Flagged, not blocking.
- **`blocked_by_modal` is stored at enqueue (L-4).** A demand deferred purely by
  typing (`blocked_by_modal: false`) promotes only on 2s idle; it will not yield
  on a modal that opens *after* it was queued. This is intended: yield-on-close
  applies only to the modal that was open at arrival (the thing the user was
  interacting with). A modal opened later is itself a user action and resets
  `last_input`, so the 2s idle timer governs. Internally consistent.
- **Pre-focused float (M.3, corrected).** The original plan's `release_focus`
  call was removed (see Phase 6 step 19): it defocused an in-use popup and
  triggered immediate promotion via the modal-close clause. A deferred
  question opened `focus=false` never gets auto-focused by `remove_windows`
  (it runs before the new window is pushed), so it keeps `focused_id == None`
  without any explicit release. Promotion restores focus via
  `focus_input_window`. Covered by `question_deferred_behind_focused_popup_waits_for_idle`.
- **Mouse routing (M.6).** Phase 7 step 23 gates the mouse `float_mgr` branch on
  `question_active()`/`focused_id.is_some()` so a queued, undrawn question
  float does not capture mouse scroll. Clicks already miss it (`contains` hits
  `focused_rect`, which is `None` when queued).
- **Backgrounded sessions (L-NEW-4).** `EventLoop::tick` calls `app.tick()` only
  for the focused session (`event_loop.rs:734-735`), and `cadence` is computed
  for the focused app (`event_loop.rs:657`). So a backgrounded session's queued
  demand never promotes and never wakes until that session is focused again.
  This is the intended behavior (background demands should not pop over the
  foreground), but worth noting: switching to a backgrounded session with a
  queued demand will promote it on the next idle tick.
- **Layout shift on promotion.** A queued question float reserves no space; on
  promotion the below split appears and the chat shrinks. This is the intended
  "now I need your attention" effect, but it is a visible jump. Acceptable per
  the requested behavior; if it feels abrupt, a future tweak could reserve space
  with a dimmed placeholder.
- **Draw-on-top via re-render of the same rect.** Where the picker and the input
  surface overlap, the topmost paint wins; non-overlapping cells keep the picker.
  This matches "drawn on top" for the overlapping region. No full-pixel
  occlusion is guaranteed for non-overlapping cells — acceptable. AC.5/AC.8
  verify the overlap via negative assertion (picker label absent from the
  overlap), not mere presence (M.4).
- **Test audit.** Tests that send a key then trigger a demand will now queue
  unless `last_input` is reset (or activity is typing-only now, M.7, so a mouse
  scroll in a test no longer marks busy). Phase 8 covers the audit; risk is
  bounded to `maki-ui` tests and is mechanical.
- **AuthRetry is intentionally not deferred** (no UI/modal to defer). Its
  "Enter retries" semantics are unchanged. Out of scope by design.
- **Not a full overlay-z-stack rewrite.** This plan deliberately does not add
  `zindex`/`view` to the `Overlay` trait or replace `view.rs`'s hardcoded draw
  sequence. If the operator wants the full architectural generalization (Option
  B), that is a separate, larger effort; this plan delivers the requested
  behavior with far less regression risk.
