# Sync makima with Upstream maki v0.5.5

## Status: READY FOR IMPLEMENTATION
**Tracking Issue:** [#138](https://github.com/lun-4/makima/issues/138)  
**Target Upstream:** `tontinton/maki` tagged `v0.5.5` (`3a0c8de5`)  
**Merge Base:** `4d84a321` (perf(ui): reflow only the viewport on resize)  
**Total Upstream Commits:** 219 non-merge commits  
**Execution Branch:** `luna.138-sync-upstream-055` (consolidated monolithic sync branch)

---

## Goal

Synchronize `makima` with upstream `maki` v0.5.5 (`3a0c8de5`) in a single consolidated sync branch (`luna.138-sync-upstream-055`), strictly adhering to the golden rule from `<skill:lunamaki-sync-with-upstream>`: **additive only. Never drop a lunamaki fork feature; bring in every upstream improvement.**

Reconcile all 219 upstream commits between the merge base (`4d84a321`) and `v0.5.5` (`3a0c8de5`), accounting for fork-specific innovations (persistent agent actor, manager graph, async subagents, mode plugins, Pi-effort thinking mapping, automode, custom splashes, and vertex integration) and explicit decisions from the team discussion (Steve Yegge & Don't Drink the Checksum).

---

## Implementation Summary

### Architectural Approach
The upstream divergence spans 219 commits since August 2026. A mechanical git merge or rebase would corrupt the fork's heaviest crates (`maki-ui` with +21k/-6.5k churn, `maki-agent` with actor/manager graph, `maki-lua` with async subagents, and `maki-commands`). 

Instead, this plan executes a targeted, additive synchronization on the monolithic branch `luna.138-sync-upstream-055`:
1. **Tier 1 (24 commits)**: Clean cherry-picks applied in sequence (e.g. `33b31d9b` off-pool syntect, `06af8b2f` reflow window, `495a9a23` V indexing, `2865809d` OpenRouter retries).
2. **Tier 2 (57 commits)**: Small, self-contained correctness fixes hand-merged into lightly diverged areas (SSRF/permission guards, agent compaction bounds, Codex dynamic model discovery `b14d444e` replacing static `PLAN_MODELS`, adaptive subagent thinking clamp `4d445a81`, model tier strength `612fc54f`, and mlua truthiness bugfix `77b21c63`).
3. **Tier 3 (112 commits)**: Major feature series synthesized additively:
   - **Terminal Inline Images**: Integrated via `ratatui-image` across Kitty/iTerm2/Sixel, resolving **Issue #45**.
   - **Folder Trust System**: Gating project-local `.maki/` configs, `.env`, and project MCP servers, with CLI `--trust` and interactive TUI card. Prompt text (`AGENTS.md`, skills, commands) and security deny rules explicitly stay ungated. Port `maki-storage/src/lock.rs` and add `fs4 = "1"` workspace dependency. Test fixtures default to trusted policy.
   - **Queue Message Condensing**: Drains consecutively-queued plain user messages into one turn in Makima's persistent actor, resolving **Issue #35**.
   - **File Mutation & Staleness**: Adopts upstream `canonical_key` and dispatcher-level staleness/registration enforcement while retaining Makima's `FileWriteLocks` (reentrancy error `SAME_PATH_MUTATION_IN_PROGRESS`, cancel races, timeouts) and `atomic_write`. `ctx:record_read` actively updates the tracker.
   - **Yolo Persistence**: Tri-state `Option<bool>` in `SessionMeta` and `[yolo]` status bar badge.
   - **Theme & Terminal Colors**: 16/256 palette indices (`eb70f92e`) and diff styling (`86c5c387`, `0da492df`) ported to `ThemesProvider`.
   - **Lua Supervision Hooks**: `TurnEnd` autocmd mapped to actor `TurnOutcome` (with cost/context fields populated), `maki-lua/src/agent_autocmd.rs`, `maki-lua/src/session_snapshot.rs`, tool layering in `tool_dispatch.rs` and `maki-lua/src/hook.rs`, and MCP tools in python `code_execution`.
   - **Thinking Picker & Dialect**: Retains Makima's `plugins/thinking/init.lua` (preserving typed `maki-commands` autocomplete), binds `<M-t>`, merges Rust-side clamping and status bar badge formatting.
   - **ACP Server Hardening**: Ignores upstream's monolithic ask refactor `8819365a` (which breaks `CommandRegistry`); ports file diff generation (`5d79739e`), request ID tagging (`ae5fd640` with `TaggedAnswer`), and auth error recovery (`928f99e5`).
   - **UI Transcript Performance**: Reimplemented in Makima (`wrap.rs` ASCII fast-path, syntect memoization in `maki-highlight`, line-walking copy, and dropping search text cache).
   - **Deferred Series**: Tracked via issues (Lua jobs blocked by God Issue #24; Makima package manager design session; subagent `todo_write` panel isolation; OTel denied/skipped).
4. **Tier 4 (26 commits)**: Skipped/dropped (xAI removal, deleted usage modal, upstream site redesign, version bumps).

### Non-Negotiable Fork Invariants
Every commit, cherry-pick, and manual merge must preserve:
1. **Persistent Agent Actor & Graph Manager** (`maki-agent/src/actor/`, `maki-agent/src/manager/` from God Issue #24 PR 1, 2, 3).
2. **Async Subagents & Task Lifecycle** (independent cancellation tokens, `task_spawn`, `task_send`, `task_get`, `task_despawn`, child delivery on turn completion with `replied` guard).
3. **Plan Mode Architecture** (`mode_plan_override`, `plan_reviewer`, `plan_submit`, guest-defined facets).
4. **Pi-style `/thinking` Effort Mapping** (`luna.thinking-mapping`, `effort_dialect` in `maki-providers/src/providers/openai/responses.rs`, typed command schema in `plugins/thinking/init.lua`).
5. **Contextual Automode** (`maki-agent` classifier and permission bypass).
6. **TUI & CLI Features**: `--append-system-prompt`, OpenAI Codex `/login`, custom splash gallery & shaders.
7. **Provider Enhancements**: Vertex AI Gemini (`luna.gcloud-integration` PR #141), OpenRouter inventory, `warm-catalog-always`.
8. **Deleted Upstream Systems**: `xai` remains completely excised; do not re-introduce xai code, auth, or dependencies.
9. **Must-Use `Dirty` Discipline**: Poll methods returning `#[must_use] Dirty` (e.g. `tick_edge_scroll()`, `tick()`, `poll_live_bufs()`) must never be ignored with `let _ =`. Always bitwise OR (`dirty |= ...`) into the returned `Dirty` state to prevent `unused_must_use` compile errors under `-D warnings` and avoid missed repaints.
10. **Identity Deferral**:
    - `Cargo.toml`: `version = "0.5.5-makima"`.
    - `install.sh` / `install.ps1`: `REPO="lun-4/makima"`, `BINARY="makima"`.
    - `maki-storage/src/version.rs`: `RELEASES_URL` points to `lun-4/makima`.
    - Branding: `LOGO = "luna-maki"`, banner, and documentation identity stay on Makima.

### Replace-vs-Edit Architecture Guidance
To avoid bugs, high churn, or regressions during implementation:
- **Clean Replacement / New Module Import**:
  - `maki-storage/src/lock.rs`: Direct port from upstream `075600f1`. Requires `fs4 = "1"` in workspace root and `maki-storage/Cargo.toml`.
  - `maki-storage/src/trusted_folders.rs`: Direct port from upstream `075600f1`.
  - `maki-config/src/project.rs`: Direct port from upstream `075600f1`.
  - `maki-ui/src/wrap.rs`: Direct port from upstream `898ac171`.
  - `maki-ui/src/terminal_image.rs`: Direct port from upstream `5a98cf16`.
  - `maki-lua/src/agent_autocmd.rs` & `maki-lua/src/session_snapshot.rs`: Direct port from upstream `13f7f396`.
  - `plugins/lib/maki/toast.lua`: Direct port from upstream `384d22ef`.
- **Targeted Additive Edits (DO NOT Replace Whole Files)**:
  - `maki-agent/src/tools/file_locks.rs`: Surgical enhancement. Add `canonical_key` normalization; keep reentrancy detection (`SAME_PATH_MUTATION_IN_PROGRESS`), cancellation race handling, acquisition timeouts, and `atomic_write`.
  - `maki-commands/src/spec.rs`: Surgical enhancement. Update `COMPACT_COMMAND_NAME` to accept optional trailing instructions string; update `BuiltinOperation::Compact` to `Compact(Option<String>)`.
  - `plugins/thinking/init.lua`: Surgical enhancement. Retain Makima's typed argument metadata and popup ladder (`render_ladder`); add upstream's `<M-t>` keymap.
  - `maki-acp/src/server.rs`: Surgical enhancement. Add diff generation and tagged request IDs (`ae5fd640`). Do NOT adopt upstream's monolithic ask refactor `8819365a`.
  - `maki-agent/src/types.rs`: Surgical addition of `cost`/`context` fields to `TurnOutcome` and `SessionEvents` RAII guard.

---

## Upstream Commit Inventory & Disposition

Between `4d84a321` and `v0.5.5` (`3a0c8de5`), exactly 219 upstream commits are accounted for:

```
Total: 219 commits
├── Tier 1: Verified clean picks (take as-is)                    24 commits
├── Tier 2: Targeted small conflict & correctness fixes          57 commits
├── Tier 3: Coherent feature series & major capabilities        112 commits
│   ├── 3A: UI Transcript Rework (reimplement perf wins)          (4 commits)
│   ├── 3B: Yolo Persistence & Status Bar                        (1 commit)
│   ├── 3C: Theme Terminal Colors & Diff Styling                 (3 commits)
│   ├── 3D: Lua Supervision Hooks (TurnEnd, Tool Layering)        (3 commits)
│   ├── 3E: Lua UI Library (Toast notifications, Action Keys)    (1 commit)
│   ├── 3F: Queued Messages & Compaction Text (Issue #35)        (2 commits)
│   ├── 3G: Session Event Stream End (stream-json leak fix)      (1 commit)
│   ├── 3H: File Mutation Gates & Canonical Keys                (1 commit)
│   ├── 3I: Config Surface (${VAR} expansion, RTK, Allowed Hosts)(3 commits)
│   ├── 3J: Window Title (OSC 2 sanitized title)                 (1 commit)
│   ├── 3K: Deferred Series (Lua Jobs, Maki-Pack, OTel)          (53 commits: 17 jobs + 16 pack + 4 otel + 7 yolo iter + 5 UI refactor + 4 churn)
│   ├── 3L: Folder Trust System (v0.5.4 capability)              (7 commits)
│   ├── 3M: Terminal Inline Images (Issue #45 / ratatui-image)   (5 commits)
│   ├── 3N: Provider Additions & Resilience (Requesty, Codex)   (18 commits)
│   ├── 3O: ACP Rework & Hardening (diffs, routing, ask refactor)(5 commits)
│   └── 3P: Agent & Subagent Core Fixes (output budget, schemas) (4 commits)
└── Tier 4: Skip / Dropped / Fork-Preempted                      26 commits
    (19 code/version commits + 7 website commits)
```

Sum: `24 + 57 + 112 + 26 = 219 commits`. Every single upstream commit is accounted for.

---

### Tier 1: Verified Clean Picks (24 Commits)
These cherry-pick cleanly onto the fork or require only minor 1-line context fixups:

| Commit | Summary | Action |
|---|---|---|
| `e0d87e35` | style(ui): fix import ordering in model picker tests | Pick as-is |
| `06af8b2f` | fix(ui): aim reflow window at visible rows, not anchor segment | Pick (1-line test fixup for fork's 3-arg `MessagesPanel::new`) |
| `06a777b1` | fix(edit): stop sinking replacement that adds nesting level | Pick as-is |
| `51eb048f` | fix(providers): keep DeepSeek's peak surcharge off the weekend | Pick as-is |
| `295947e4` | fix(ui): clip selection to buffer before reading cells | Pick as-is |
| `b0830cda` | fix(edit): pick reindent base in stable order | Pick as-is |
| `c96f2ae6` | fix(edit): write file back with line endings it came with | Pick as-is |
| `2f570190` | README.me: fix spelling mistake | Pick as-is |
| `65a2113b` | fix(permissions): don't collapse redirected bash chain into one scope | Pick as-is |
| `33b31d9b` | perf(highlight): move syntect off render and blocking pools | **Standout win**: pick as-is (massive TUI perf boost) |
| `4ae3a720` | test: add memory probe that waits for plateau | Pick as-is |
| `d3840bfd` | docs(highlight): correct what syntect cache actually costs | Pick as-is |
| `eac7ee56` | fix(providers): stop bad output cap from eating transcript | Pick as-is |
| `09bb50d3` | docs(highlight): correct what caps regex cache | Pick as-is |
| `451b44a8` | fix(providers): detect vision support for Ollama models | Pick as-is |
| `21ea1ab3` | fix(providers): ask warm models.dev metadata before family guess | Pick as-is |
| `75ae8b26` | fix(highlight): stop JavaScript line from pinning core forever | Pick as-is |
| `495a9a23` | feat(index): add V language tree-sitter support | Pick as-is |
| `d4d35ecf` | fix(storage): keep logging when another process rotates log | Pick as-is |
| `58260dfb` | fix(providers): keep tensorx thinking on after deepseek rename | Pick as-is |
| `2865809d` | fix(providers): retry OpenRouter mid-stream errors | Pick as-is |
| `84909121` | fix(providers): read published zero limit as no limit | Pick as-is |
| `14d14cd0` | fix(providers): keep minted tool call id unique across runs | Pick as-is |
| `cdfc415f` | fix(providers): keep SSE error frame that carries no message | Pick as-is |

*(Note: `ce73793e` "feat(lua): add jobinfo and joblist" was moved from Tier 1 to Tier 3K Deferred Jobs series per the user directive to defer the entire Lua jobs API).*

---

### Tier 2: Targeted Small Conflict & Correctness Fixes (57 Commits)

#### 1. Permissions & Sandboxing (12 commits)
- `a50b6f31`: `fix(permissions): stop "allow always" handing out a blank cheque`
- `33f04f6b`: `fix(agent): read a universal scope the way the matcher reads it`
- `8639da9a`: `fix(agent): rank approvals by how squarely they name the tool`
- `8e608b2d`: `fix(lua): close gaps SSRF guard says it covers`
- `06d7fbaf`: `fix(net): don't call a DNS hiccup an SSRF block`
- `5db9a257`: `feat(agent): allow MCP tool calls in plan mode` (retires `mcp_tool_blocked_in_plan_mode` test)
- `94c540a6`: `fix(agent): keep plan mode ahead of yolo for MCP tools`
- `b1468936`: `fix(agent): give sandbox request's own tool filter`
- `346f7a44`: `fix(agent): read subagent's filter off array it published`
- `db22d1c2`: `fix(lua): name tool and fix when permission pair is incomplete`
- `4d847507`: `fix(permissions): add permission guard to register_permission_rule` (standalone security fix from jobs series)
- `1ac265ff`: `fix(config): let a disabled plugin hand its tool name over` (prevents disabled plugin copying into agent disabled_tools and warns on reserved names)

#### 2. Agent Streaming & Compaction Correctness (10 commits)
- `f4e31320`: `keep clamped output cap above thinking floor`
- `c41031f0`: `clamp output cap against measured prompt`
- `471ccff8`: `stop full context from wedging session`
- `4a282efb`: `send session id on compaction requests`
- `774b18e1`: `guard first request of a resumed session`
- `61b9bdc9`: `perf(agent): share one tool output between session and UI`
- `8e406fd1`: `fix(agent): stop sizing output budget off prompt estimate`
- `2815f517`: `fix(agent): cap what compaction holds back at half the window`
- `61a9bba0`: `fix(agent): announce every retry, including shrunk output budget`
- `a40a3597`: `fix(agent): make esc reach every kind of run`

#### 3. Provider Resilience & Discovery (21 commits)
- `bc9c8e94`: `retry Codex overloads instead of failing the run`
- `667a9c0d`: `don't kill session over a providers.toml typo`
- `6dfbf1fc`: `don't log user out when token file won't write`
- `89e66918`: `intern image payloads while decoding them` (perf)
- `e8b0a4af`: `gate Zen streaming on enable_free_models too`
- `6312bf39`: `move OpenCode policy out of models.dev catalog`
- `bdd5dfef`: `bill from cost response reports (OpenRouter)`
- `b14d444e`: `discover Coding Plan models from Codex backend` (**promoted**: replaces hand-written `PLAN_MODELS` table in `platform.rs`)
- `db5975bb`: `add Regolo provider` (clean additive provider)
- `7a298c8c`: `feat(providers): honor server Retry-After on retryable API errors`
- `6c81b711`: `feat(providers): split retry budgets by what the error costs`
- `f19dfb15`: `fix(providers): rotate to a fresh key without spending a retry`
- `2ed84543`: `fix(providers): cap whole loopback request head, not each read`
- `b5c2bdd7`: `fix(providers): preserve OpenCode usage limit details`
- `067f4a5c`: `fix(providers): mint unique ids for tool calls without one`
- `79a837de`: `fix(providers): price zai glm-5.1 and glm-5.2 at real rates`
- `b952bb9e`: `fix(providers): size reset gauge the way seeded one is sized`
- `4d445a81`: `fix(providers): stop an adaptive subagent from outrunning its parent` (**critical recovered bugfix**: `ThinkingConfig::clamp_to` clamps `Adaptive` to parent's concrete budget)
- `612fc54f`: `fix(providers): write down the order of model tiers` (**critical recovered bugfix**: explicit `ModelTier::strength()` ordering `Compaction < Weak < Medium < Strong`)
- `dd0bb152`: `make a 401 refresh actually refresh` (OpenAI parts only; xai stripped)
- `c5622f4f`: `stop oauth refreshes racing` (OpenAI parts only; xai stripped)

#### 4. UI & Lua Correctness (14 commits)
- `e4efbb6d`: `let code block's last line survive closing fence`
- `8750d22d`: `count input box's rows the way it draws them`
- `57d1f900`: `put terminal cursor where input box drew it` (reconcile with makima's `input.rs`)
- `0fff9468`: `bind PageUp/PageDown to page scrolling in chat`
- `9d00e16b`: `settle picker on matcher, not quiet tick`
- `eed23c20`: `add ModelChanged autocmd`
- `01d60931`: `ring bell for prompts even when terminal has focus`
- `9cf1d878`: `make btw model use same thinking styling`
- `d636772c`: `fix(ui): keep agent errors in the chat transcript`
- `18472ae9`: `fix(lua): load subdirectory AGENTS.md once, on model call`
- `77b21c63`: `fix(lua): stop every window from opening invisible` (**critical recovered bugfix**: mlua truthiness fix for `opt_bool` in `maki-lua/src/api/ui/mod.rs:551`)
- `0da492df`: `fix(theme): style edit diff parts separately` (companion to diff signs styling for `plugins/edit/init.lua`)
- `a1ae3624`: `Fix for missing global AGENTS.md when ~/.maki exists` (normalizes path discovery)
- `a268c3c1`: `fix(cli): confirm before maki session delete`

---

## Feature-by-Feature Deep Dive: Merge Classifications & Architecture

Every feature area is classified into:
* **Clean-Merge**: Self-contained upstream commit that applies directly without architectural collision.
* **Ignore / Skip**: Preempted by fork design, already implemented independently, or explicitly rejected (e.g., `maki-pack`, `maki-otel`, `xai`).
* **Additive-Merge**: Areas where both upstream and Makima evolved independently; requires careful synthesis preserving Makima's architectural invariants.

---

### Feature 1: Folder Trust System
* **Commits**: `075600f1`, `9dc3945e`, `50b323eb`, `a82702b1`, `f4cb482b`, `1c5b4999`, `a26a849b` (7 commits).
* **Makima Current State**: Makima loads project `.env`, `.maki/init.lua`, and MCP servers directly from `cwd` without trust gating.
* **Classification**: **Additive-Merge**.
* **Synthesis & Architecture**:
  1. Add `fs4 = "1"` to root `Cargo.toml` and `maki-storage/Cargo.toml`.
  2. Port `maki-storage/src/lock.rs`: Advisory kernel lock implementation (`fs4::FileExt`) used to coordinate trusted folders across concurrent processes.
  3. Port `maki-storage/src/trusted_folders.rs`: Manages JSON database of trusted project roots (`trusted-folders.json`) and snapshot records of gated files under advisory lock (`trusted-folders.lock`).
  4. Port `maki-config/src/project.rs`: Encapsulates `ProjectEnvironment`, `TrustStatus`, and `TrustGatedFiles`. Gating strictly applies to `.maki/init.lua`, `.env`, and project-defined MCP servers. Prompt text (`AGENTS.md`, system instructions, skills, commands) and security `deny` rules explicitly stay **ungated** (because trust gates code execution and environment mutation, not what the model reads).
  5. Add `src/project_trust.rs` and CLI subcommands: `maki trust add|list|remove`, plus `--trust` flag for one-shot automation.
  6. Global config: `trust` policy table in `~/.config/maki/init.lua` (`f4cb482b`).
  7. Interactive TUI trust card: Rendered in `maki-ui` before opening the first thread in an untrusted directory (`50b323eb`).
  8. **Test Fixture Exemption (Crucial)**: In tests (e.g. `test_support::spawn_host_for_tests`), construct `ProjectEnvironment` with `TrustPolicy::TrustAll` or pre-trust the temporary directory so test runs never block waiting for interactive input.

---

### Feature 2: Terminal Inline Image Rendering (Issue #45)
* **Commits**: `5a98cf16`, `3b9d0ccc`, `e36213ba`, `65e3a66b`, `0257881a` (5 commits).
* **Makima Current State**: Makima formats images as plain text summaries (`[1 image]`, `[2 images]`) via `format_with_images` in `chat.rs`.
* **Classification**: **Additive-Merge**.
* **Synthesis & Architecture**:
  1. Add `ratatui-image = { version = "11.0.8", default-features = false, features = ["crossterm"] }` to root `Cargo.toml`.
  2. Update `maki-ui/Cargo.toml` with `image = { workspace = true, features = ["jpeg", "gif", "webp"] }` and `ratatui-image = { workspace = true }`.
  3. Port `maki-ui/src/terminal_image.rs`: Picker initialization, protocol detection (Kitty, iTerm2, Sixel), and cache management.
  4. Wire into `MessagesPanel`: Render inline thumbnails for user attached images and assistant image blocks.
  5. Include decode optimizations: Prefix dimension reading (`e36213ba`), selective message rebuilding (`65e3a66b`), and bad image isolation (`0257881a`).
  6. Fallback: Automatically fall back to `[image]` text line when inline images are disabled (`ui.inline_images = false`), during headless runs, or on terminals without graphics protocol support.

---

### Feature 3: Queue Message Condensing & Compaction Text (Issue #35)
* **Commits**: `48342883`, `a8e80019`, `2815f517` (3 commits).
* **Makima Current State**:
  * Makima's execution is driven by the persistent agent actor (`maki-agent/src/actor/`) and `maki-ui/src/agent/shared_queue.rs`.
  * Typing multiple messages while the agent is busy creates multiple individual runs, one at a time.
  * `COMPACT_COMMAND_NAME` in `maki-commands/src/spec.rs:265` takes 0 arguments, and `maki-commands::BuiltinOperation::Compact` carries no payload.
* **Classification**: **Additive-Merge**.
* **Synthesis & Architecture**:
  1. **Actor Queue Condensing (`48342883`)**:
     * In `maki-agent/src/actor/queue.rs` and `runner.rs`, when the scheduler consumes work to start a turn, if consecutive items in the queue are plain user messages (with same mode and no workflow/compaction boundary), coalesce them into a single `TurnAdmission`.
     * Merge earlier messages into the preamble of the resulting `AgentInput`, and accumulate all image attachments across the burst so the entire burst executes in a single LLM request.
     * Update `maki-ui/src/agent/shared_queue.rs` presentation to reflect condensed items.
  2. **Compaction Custom Instructions (`a8e80019`)**:
     * In `maki-commands/src/spec.rs`, update `COMPACT_COMMAND_NAME` signature to accept optional trailing text.
     * Update `maki-commands::BuiltinOperation::Compact` to `Compact(Option<String>)`.
     * Update `maki-agent/src/command.rs` to dispatch `HostResponse::ManualCompaction(Option<String>)`.
     * In `maki-agent/src/agent/compaction.rs`, append custom instructions under "Additional instructions".
     * Apply `2815f517`: Cap compaction buffer at half window size.

---

### Feature 4: File Mutation & Staleness Harmonization
* **Commits**: `b31e4679` (1 commit).
* **Makima Current State**:
  * Makima implemented `FileWriteLocks` (`maki-agent/src/tools/file_locks.rs`, +305 lines) from Issue #16 with:
    - Reentrancy detection returning `SAME_PATH_MUTATION_IN_PROGRESS`.
    - Cancellation race integration with `CancelToken`.
    - Acquisition timeouts (`acquire_timeout`).
    - Whole-file mutations using `maki.fs.atomic_write`.
* **Upstream Design**:
  * Introduced `canonical_key` in `maki-storage/src/paths.rs`.
  * Moved `check_before_edit` and mtime recording directly into `tool_dispatch.rs` under the file lock.
  * Enforced `mutable_path` when `permission = "fs_write"`.
* **Classification**: **Additive-Merge**.
* **Synthesis & Architecture**:
  1. Do **NOT** replace `FileWriteLocks` with upstream's `FileAccess` (upstream lacks reentrancy detection, cancel races, and timeouts).
  2. Adopt `maki_storage::paths::canonical_key` and use it inside `FileWriteLocks::lock_key` (replacing raw `incremental_canonicalize`).
  3. In `maki-agent/src/agent/tool_dispatch.rs`: Execute `check_before_edit` automatically inside the acquired `FileWriteLocks` guard before invoking handler, and record mtime after successful writes.
  4. In `maki-lua/src/api/tool.rs`: Enforce that tools declaring `permission = "fs_write"` must declare `mutable_path`.
  5. In `maki-lua/src/api/util/ctx.rs`:
     - `ctx:record_read(path)` MUST actively record the read timestamp in `FileReadTracker` (since `plugins/read/init.lua` and `plugins/grep/init.lua` call it).
     - `ctx:check_before_edit` stays as a delegating/no-op shim returning `(Some(true), None)` so external plugins do not break.

---

### Feature 5: Yolo Persistence & Status Bar
* **Commits**: `b63a23aa` (1 commit adopted; 7 intermediate dim-factor commits dropped).
* **Makima Current State**:
  * Makima already has `pub yolo: bool` in `SessionMeta` (`maki-storage/src/sessions.rs:159`), mirrored in `storage_writer.rs` and `event_loop.rs`.
  * However, as a plain `bool`, it cannot distinguish "never set" from "explicitly set to false", which causes `--yolo` / `always_yolo` CLI flags to stamp or erase intent.
  * Makima does not display `[yolo]` in `status_bar.rs`.
* **Classification**: **Additive-Merge**.
* **Synthesis & Architecture**:
  1. Change `SessionMeta.yolo` to `Option<bool>`, where `None` = unconfigured (inherits CLI/config default), `Some(true/false)` = explicit session toggle.
  2. Add `[yolo]` badge in `maki-ui/src/components/status_bar.rs` when active.
  3. Collapse dim factor to final value `0.15`.
  4. Drop the 7 intermediate tweak commits (`c39efd1f`, `9fd4ab53`, `9352f8d9`, `d6d3a9c9`, `c56fb36c`, `504dd859`, `a26770ec`).

---

### Feature 6: Theme Terminal Colors & Diff Styling
* **Commits**: `eb70f92e`, `86c5c387`, `97f94e28`, `0da492df` (4 commits).
* **Makima Current State**:
  * Makima uses `Arc<dyn ThemesProvider>` (`maki-ui/src/theme.rs`).
  * Only hex colors are supported. Terminals in nested tmux/SSH sessions mangle truecolor.
* **Classification**: **Additive-Merge**.
* **Synthesis & Architecture**:
  1. Adopt 16-color named colors (`blue`, `light-gray`) and 0-255 palette indices in `maki-highlight` and `maki-theme` (`eb70f92e`). Syntect encodes the index in the alpha byte.
  2. Add separate styling tokens for diff signs (`diff_old_sign`, `diff_new_sign`) and line numbers (`diff_old_line_nr`, `diff_new_line_nr`) into `ThemeData` and `ThemesProvider` (`86c5c387`).
  3. Update `plugins/edit/init.lua` with `0da492df` to render diff blocks with the new tokens.
  4. Port diff styling unit tests to `ThemesProvider` (`97f94e28`).

---

### Feature 7: Lua Supervision Hooks & Tool Layering
* **Commits**: `13f7f396`, `0e68892e`, `982f1082` (3 commits).
* **Makima Current State**:
  * No `TurnEnd` autocmd; `TurnOutcome` in `maki-agent/src/types.rs` lacks cost and context size metadata.
  * No `maki-lua/src/agent_autocmd.rs` or `maki-lua/src/session_snapshot.rs`.
  * Tools cannot be layered before schema/permission checks.
  * `code_execution` Python scripts cannot call MCP tools.
* **Classification**: **Additive-Merge**.
* **Synthesis & Architecture**:
  1. **`TurnEnd` Autocmd Architecture (`13f7f396`)**:
     * Augment `TurnOutcome` in `maki-agent/src/types.rs` to include `cost: Option<f64>`, `list_cost: Option<f64>`, `context_size: Option<usize>`, `context_window: Option<usize>`.
     * Port `maki-lua/src/agent_autocmd.rs` and `maki-lua/src/session_snapshot.rs`.
     * Introduce autocmds `TurnEnd`, `AutoCompacting`, `CompactionDone`, `PlanReady`, and Lua methods `maki.session.queue()` and `maki.session.read()`.
     * Wire into `maki-agent/src/actor/` when `TurnOutcome` is generated; explicitly filter out subagent turns (`is_subagent == false`).
     * Provides foundation for `/goal` (Issue #42).
  2. **Tool Layering (`0e68892e`)**:
     * Add `maki-agent/src/tools/hook.rs`, `maki-lua/src/hook.rs`, and `maki-lua/src/api/slot.rs` supporting `tool.<name>.input` and `tool.<name>.output`.
     * In `tool_dispatch.rs`: Execute input layers *before* schema validation and permission checks. Input layers cannot bypass plan mode `fs_write` gating.
  3. **MCP in Python (`982f1082`)**:
     * Update `maki-agent/src/tools/interpreter_bridge.rs` and `plugins/code_execution/init.lua` to expose MCP tools, ACP client tools, and `tool_search` to the Python sandbox.

---

### Feature 8: Lua UI Library
* **Commits**: `384d22ef`, `77b21c63` (2 commits).
* **Makima Current State**:
  * Makima has `maki.ui.flash`, but no stacking toast notifications.
  * Makima lacks `plugins/lib/maki/list_picker.lua` and `toast.lua`.
  * `maki-lua/src/api/ui/mod.rs:551` suffers from the mlua truthiness bug where `opts.get("visible").unwrap_or(true)` evaluates `nil` as `false`.
* **Classification**: **Additive-Merge**.
* **Synthesis & Architecture**:
  1. Add `plugins/lib/maki/toast.lua`: Stacking corner toasts anchored NE that auto-dismiss or restack.
  2. Add `plugins/lib/maki/list_picker.lua` with `action_keys` support.
  3. Expose host primitives: `maki.ui.buf({ scratch = true })`, `maki.defer_fn`, `maki.notify`.
  4. Fix mlua truthiness bug via `opt_bool` across `maki-lua/src/api/ui/mod.rs`, `fs.rs`, and `win.rs` (`77b21c63`).

---

### Feature 9: Window Title
* **Commits**: `054fac6f` (1 commit).
* **Makima Current State**: Terminal title is left untouched.
* **Classification**: **Clean-Merge**.
* **Synthesis & Architecture**:
  * Port `maki.ui.set_window_title` in `maki-lua/src/api/ui/mod.rs` and `maki-ui/src/terminal.rs`.
  * Strip control characters on Rust side before emitting OSC 2 sequences.

---

### Feature 10: Session Event Stream End
* **Commits**: `2cf5e1da` (1 commit).
* **Makima Current State**: In `maki --input-format stream-json`, tool executions leave `EventSender` clones parked in idle `LuaCtx`, preventing channel close on stdin EOF.
* **Classification**: **Additive-Merge**.
* **Synthesis & Architecture**:
  1. In `maki-agent/src/types.rs`, introduce `SessionEvents` and `EventStreamGuard` emitting `StreamClosed` on drop.
  2. In `plugins/task/init.lua`, move `sess:close()` outside of `pcall` in the handler epilogue so subagent event relays are always closed on every exit path. Validate with `luac -p`.
  3. Update `sdk_mode.rs` and `print.rs` pumps to exit cleanly on `StreamClosed`.

---

### Feature 11: Config Surface
* **Commits**: `6d9393a7`, `e745f057`, `d83e10e5`, `6c4cc67d`, `e65fd48a` (5 commits).
* **Makima Current State**: Different frontends handle defaults slightly differently; no `${VAR}` expansion in headers.
* **Classification**: **Additive-Merge**.
* **Synthesis & Architecture**:
  1. `e745f057`: `${VAR}` environment variable expansion in MCP server and provider HTTP headers (strip `xai/auth.rs` half!).
  2. `6d9393a7`: `net.allowed_private_hosts` config in `maki-config/src/lib.rs` and manual redirect following in SSRF guard.
  3. `d83e10e5`: Move `--no-rtk` into `agent.rtk` config option.
  4. `6c4cc67d`: `SessionDefaults` unifying `always_*` settings across Headless, Sdk, Acp, and Print frontends.
  5. `e65fd48a`: Default `edit_lines = true`.

---

### Feature 12: ACP Server Synchronization
* **Commits**: `38271fc5`, `112b4014`, `d8d55237`, `8819365a`, `5d79739e`, `ae5fd640`, `928f99e5`, `4646fb97` (8 commits).
* **Makima Current State**:
  * Recently refactored around `CommandRegistry` (PR #88) and persistent `AgentActor` / `AgentManager`.
  * Already implements pipelined prompt rejection (`admit_prompt`), session restore pricing (`settle_session`), and subagent permission routing (`server.rs:1928-1939`).
* **Classification**:
  * `38271fc5`, `112b4014`, `d8d55237`: **Ignore / Skip** (already solved in Makima).
  * `8819365a`: **Ignore / Skip** (incompatible synchronous ask refactor that breaks `CommandRegistry`).
  * `5d79739e`, `ae5fd640`, `928f99e5`, `4646fb97`: **Additive-Merge** (targeted correctness fixes).
* **Synthesis & Architecture**:
  1. `5d79739e`: Update `maki-acp/src/translate.rs:permission_request` to accept raw tool inputs and generate unified diffs for `write`/`edit`.
  2. `ae5fd640`: Tag outgoing permission requests with request IDs using `TaggedAnswer` in `maki-agent/src/permissions.rs`, `maki-ui/src/components/permission_prompt.rs`, `src/sdk_mode.rs`, and `maki-acp/src/server.rs`. Stale responses from previous turns are safely discarded.
  3. `928f99e5`: End ACP turns with authentication errors on 401 `AuthRequired`; accept string-encoded request IDs (e.g. `"1001"`).
  4. `4646fb97`: Use `WeakSender` to ensure writer task exits cleanly on EOF.

---

### Feature 13: Thinking Picker & Pi-Effort Dialect
* **Commits**: `59463e3d`, `0be4ebd7`, `4765e776` (3 commits).
* **Makima Current State**:
  * Makima already has `plugins/thinking/init.lua` with typed `maki-commands` argument metadata (`arguments = { { name = "effort", ... } }`), interactive ladder popup (`render_ladder`), and Pi-style effort mapping (`effort_dialect`).
* **Classification**: **Additive-Merge**.
* **Synthesis & Architecture**:
  1. Do **NOT** overwrite Makima's `plugins/thinking/init.lua` with upstream's `thinking_picker.lua` / `thinking_window.lua` (which uses `nargs = "?"` and would destroy typed autocompletion).
  2. Add keymap to Makima's `plugins/thinking/init.lua`: `maki.keymap.set("n", "<M-t>", open_selector, { desc = "Thinking effort" })`.
  3. Adopt upstream Rust-side improvements: Clamp stored thinking in `ThinkingConfig` so required thinking models don't report `[off]`, and format status bar badge as `[high]` / `[8192 tokens]`.
  4. `4765e776`: Support per-task thinking override capped at parent budget.

---

### Feature 14: Provider Additions, Models.dev & Resilience
* **Commits**: `b14d444e`, `7c8e1355`, `6dd4922d`, `14929920`, `e9c7d023`, `1df4d830`, `db5975bb`, `f1c235e9`, `8ac293e0`, `7acf9cd8`, `7a298c8c`, `6c81b711`, `f19dfb15`, `2865809d`, `4d445a81`, `612fc54f`, `ee6d7a09`, `3b497b4c`, `c1e723be`, `bc9c8e94`, `667a9c0d`, `6dfbf1fc` (22 commits).
* **Classification**:
  * Clean-Merge: `7c8e1355`/`6dd4922d` (Codex fast), `14929920` (Requesty), `db5975bb` (Regolo), `f1c235e9` (GLM-5.3), `8ac293e0` (You.com), `7a298c8c` (Retry-After), `6c81b711` (error cost retry split), `f19dfb15` (auth key rotation), `2865809d` (OpenRouter retries), `4d445a81` (adaptive subagent clamp), `612fc54f` (model tier strength), `bc9c8e94` (Codex overload retries), `667a9c0d` (providers.toml typo resilience).
  * Additive-Merge:
    - `b14d444e`: Dynamic Coding Plan model discovery from ChatGPT backend, augmenting Makima's static fallbacks (`gpt-6-astra`).
    - `ee6d7a09`/`3b497b4c`/`c1e723be`: Models.dev integration & refresh, preserving Makima's `warm-catalog-always` and vendored offline catalog (`maki-storage/src/model_catalog.json`).

---

### Feature 15: UI Transcript Performance & Benchmarks
* **Commits**: `898ac171`, `5c2f3915`, `a6305a20`, `f9c24df5`, `33b31d9b` (5 commits).
* **Makima Current State**:
  * `maki-highlight` and `maki-markdown` are completely untouched by the fork.
  * `maki-ui` diverged (+21k / -6.5k). Makima does not have `wrap.rs`.
* **Classification**: **Additive-Merge**.
* **Synthesis & Architecture**:
  1. `33b31d9b`: **Clean-Merge** into untouched `maki-highlight` (moves syntect off render pool).
  2. `5c2f3915`: **Clean-Merge** into untouched `maki-highlight` and `maki-markdown` (memoizes syntect highlighting by `(lang, code_hash)`). Adapt `maki-ui/src/markdown.rs` call sites. Adopt `maki-ui/benches/reflow.rs`.
  3. `898ac171`: Introduce `maki-ui/src/wrap.rs` with ASCII fast-path wrapping (`WordWrapper` bypass for pure-ASCII spans) and integrate into `MessagesPanel`. Adopt `maki-ui/benches/wrap.rs`.
  4. `a6305a20`: Optimize selection copy via forward line walking (`Segment::rows_from`) mapped to Makima's document-row selection model.
  5. `f9c24df5`: Drop transcript-wide `search_text` memory cache; rebuild on search modal open.

---

### Feature 16: Explicitly Deferred / Skipped Systems
* **Lua Jobs Resumable State API** (17 commits: `cb7b8b4d`..`df045ed5`, including `ce73793e`):
  * **Classification**: **Ignore / Defer**.
  * File GitHub issue marking God Issue #24 (Append-Only Log / persistent state API) as blocking it.
* **`maki-pack` Package Manager** (16 commits: `ade51171`..`2a2c06b8`):
  * **Classification**: **Ignore / Defer**.
  * Collides with `maki-commands`. File GitHub issue for custom Cordis-style package manager design session.
* **OpenTelemetry (`maki-otel`)** (4 commits: `df0a069e`, `64d0b33c`, `dd8c1046`, `ed1f05e9`):
  * **Classification**: **Ignore / Skip**.
  * Rejected to avoid pulling protobuf/grpc runtime into TUI.
* **`todo_write` Subagent Panel Isolation** (`fcb8040b`, `589edfa5`):
  * **Classification**: **Ignore / Defer**.
  * File GitHub issue blocked by God Issue #24 agent unification.
* **xAI Provider Remnants**:
  * **Classification**: **Ignore / Skip**.
  * `xai` remains deleted from Makima.
* **Website Redesign** (7 commits: `a18ec395`..`ce0dd7cb`):
  * **Classification**: **Ignore / Skip**.
  * Makima maintains its own documentation and branding under `site/`.

---

## Implementation Plan

All work executes on the consolidated sync branch `luna.138-sync-upstream-055` based on `mistress`.

### Phase 1: Branch Setup & Workspace Grounding
1. Ensure working tree is on `luna.138-sync-upstream-055`.
2. Verify git remotes: `upstream` points to `tontinton/maki`, `origin` points to `lun-4/maki`.
3. Check and verify `install.sh` and `install.ps1`: confirm `REPO="lun-4/makima"` and `BINARY="makima"`.

### Phase 2: Cherry-Pick Tier 1 Clean Commits
1. Cherry-pick the 24 verified clean commits in chronological order:
   - `e0d87e35`, `06af8b2f` (fixup test for fork's 3-arg `MessagesPanel::new`), `06a777b1`, `51eb048f`, `295947e4`, `b0830cda`, `c96f2ae6`, `2f570190`, `65a2113b`, `33b31d9b`, `4ae3a720`, `d3840bfd`, `eac7ee56`, `09bb50d3`, `451b44a8`, `21ea1ab3`, `75ae8b26`, `495a9a23`, `d4d35ecf`, `58260dfb`, `2865809d`, `84909121`, `14d14cd0`, `cdfc415f`.
2. Run local check: `cargo check -p maki-highlight -p maki-providers`.

### Phase 3: Permissions, Sandboxing & Security Hardening
1. Port permissions fixes:
   - `a50b6f31` ("allow always" blank cheque fix), `33f04f6b` (universal scope matching), `8639da9a` (approval ranking).
   - `8e608b2d` & `06d7fbaf` (SSRF guard closing private IP/DNS bypasses in `maki-lua/src/api/net.rs`).
   - `4d847507` (add permission guard to `register_permission_rule`).
   - `1ac265ff` (disabled plugins handing tool names over and warning on reserved names).
2. Plan mode MCP access:
   - Apply `5db9a257` (allow MCP tools in plan mode) and `94c540a6` (keep plan mode ahead of yolo for MCP).
   - Update `mcp_tool_blocked_in_plan_mode` test in `maki-agent/src/agent/tool_dispatch.rs`.
3. Tool filters:
   - Apply `b1468936` (sandbox request filter) and `346f7a44` (subagent array filter).
   - Apply `db22d1c2` (incomplete permission pair naming).

### Phase 4: File Mutation & Staleness Harmonization
1. Add `maki_storage::paths::expand_tilde` and `maki_storage::paths::canonical_key`.
2. Synthesize with makima's `FileWriteLocks`:
   - Enforce in `maki-lua/src/api/tool.rs`: tools declaring `permission = "fs_write"` must declare `mutable_path`.
   - Update `FileWriteLocks::lock_key` (`maki-agent/src/tools/file_locks.rs:28`) to use `canonical_key`.
   - In `maki-agent/src/agent/tool_dispatch.rs`: check stale-read (`check_before_edit`) automatically inside the acquired `FileWriteLocks` guard before executing handler, and record mtime after a successful write.
   - Retain makima's `maki.fs.atomic_write`, reentrancy detection (`SAME_PATH_MUTATION_IN_PROGRESS`), cancellation races, and timeouts.
   - In `maki-lua/src/api/util/ctx.rs`:
     - Update `ctx:record_read(path)` to record read timestamps in `FileReadTracker`.
     - Retain `ctx:check_before_edit` as a delegating compatibility shim returning `(Some(true), None)`.
   - Apply `a1ae3624` (global AGENTS.md discovery fix when `~/.maki` exists).

### Phase 5: Provider Enhancements & Resilience
1. Port OpenAI Codex enhancements:
   - `b14d444e`: Dynamic Coding Plan model discovery from ChatGPT backend; fallback to static table.
   - `7c8e1355` & `6dd4922d`: Codex fast mode subscription support scoped to logged-in user.
2. Add Requesty provider (`14929920`, `e9c7d023`, `1df4d830`).
3. Add Z.AI GLM-5.3 models with 1M context (`f1c235e9`).
4. Port retry and rate-limiting resilience:
   - `7a298c8c`: Honor `Retry-After` headers on retryable HTTP errors.
   - `6c81b711`: Split retry budgets by error cost.
   - `f19dfb15`: Key rotation on auth failure without burning retry count.
   - `bc9c8e94`: Retry Codex overloads.
   - `667a9c0d`: Tolerate `providers.toml` parse typos without crashing session.
   - `4d445a81`: Prevent adaptive subagents from outrunning parent thinking budget (`clamp_to`).
   - `612fc54f`: Enforce model tier strength ordering (`Compaction < Weak < Medium < Strong`).
5. Add You.com websearch provider (`8ac293e0`, `7acf9cd8`).
6. Port models.dev fallback & refresh (`ee6d7a09`, `3b497b4c`, `c1e723be`), ensuring `warm-catalog-always` and offline fallback to `maki-storage/src/model_catalog.json` remain intact.
7. Add Regolo provider (`db5975bb`).

### Phase 6: Agent Lifecycle, Queue Condensing & Compaction
1. Issue #35 queue condensing (`48342883`):
   - In `maki-agent/src/actor/queue.rs` and `runner.rs`, drain consecutively-queued plain user messages into a single turn when consuming next turn work.
   - Preserve image attachments across condensed messages.
   - Stop drain on non-plain messages or mode/workflow boundaries.
2. Compaction custom instructions (`a8e80019`):
   - In `maki-commands/src/spec.rs`, update `COMPACT_COMMAND_NAME` to accept optional trailing text argument.
   - Update `maki-commands::BuiltinOperation::Compact` to `Compact(Option<String>)`.
   - Update `maki-agent/src/command.rs` to pass instructions through `HostResponse::ManualCompaction(Option<String>)`.
   - In `maki-agent/src/agent/compaction.rs`, append instructions under "Additional instructions".
   - Apply `4a282efb` (send session id on compaction) and `2815f517` (cap compaction buffer at half window).
3. Output budget fixes:
   - `8e406fd1`: Decouple output budget from prompt estimate.
   - `f4e31320`: Clamped output cap stays above thinking floor.
   - `61a9bba0`: Announce retries and shrunk budgets in transcript.
   - `a40a3597`: Ensure Esc cancellation reaches every run type.
4. Schema validation:
   - `95f5bce5`: Enforce object root on tool input schemas. Audit fork-specific tools (`mode_plan_override`, `plan_reviewer`, `plan_submit`, `task_spawn`, `task_send`, `task_get`) for object root compliance.
5. Subagent options unification (`d5b6d840`):
   - Unify request options into one struct, ensuring child subagents get independent `CancelToken`s registered only in `subagent_cancels`.
6. Stream-JSON EOF leak fix (`2cf5e1da`):
   - Add `SessionEvents` RAII guard emitting `StreamClosed` on drop in `maki-agent/src/types.rs`.
   - In `plugins/task/init.lua`, move `sess:close()` to handler epilogue outside `pcall`. Validate with `luac -p`.

### Phase 7: ACP Server Synchronization
1. Reconcile ACP changes against Makima's recently refactored `server.rs` (`command-registry-acp-revamp` PR #88 and Agent Actor / Manager integration):
   - **Already Solved in Makima**:
     - `38271fc5` (reject second prompt): Enforced by `admit_prompt(&session.pending)` (`server.rs:1565`), returning `-32600 ACTIVE_OPERATION_MESSAGE`.
     - `112b4014` (restore session pricing): Solved via `settle_session` returning `restored_cost` (`server.rs:540`).
     - `d8d55237` (subagent permission routing): Implemented in Makima's ACP pump (`server.rs:1928-1939`), routing subagent permission asks to `subagent.answer_tx`.
   - **Do NOT Cherry-Pick / Drop**:
     - `8819365a` (outstanding ask refactor): Drops `CommandRegistry` dispatch. Reject.
   - **Targeted Correctness Fixes to Port**:
     - `5d79739e`: Include file paths and diffs in ACP permission requests by updating `maki-acp/src/translate.rs:permission_request` to accept raw input and generate diffs for `write`/`edit`.
     - `ae5fd640`: Implement `TaggedAnswer` in `maki-agent/src/permissions.rs`, `maki-ui/src/components/permission_prompt.rs`, `src/sdk_mode.rs`, and `maki-acp/src/server.rs`. Outgoing permission requests carry request IDs so stale responses from previous turns are dropped.
     - `928f99e5`: End ACP turns with an authentication error on 401 `AuthRequired` instead of hanging; parse client response IDs as both `i64` and string.
     - `4646fb97`: Use `WeakSender` where appropriate to ensure ACP writer task exits cleanly on EOF.

### Phase 8: Folder Trust System Integration
1. Dependencies & advisory locking:
   - Add `fs4 = "1"` to root `Cargo.toml` and `maki-storage/Cargo.toml`.
   - Port `maki-storage/src/lock.rs` (+107 lines from upstream `075600f1`).
2. Storage & config implementation:
   - Port `maki-storage/src/trusted_folders.rs` (+1254 lines).
   - Port `maki-config/src/project.rs` (+972 lines).
   - Gating rules: Strictly gate `.maki/init.lua`, `.env`, and project MCP servers.
   - Ungated exemption: Prompt text (`AGENTS.md`, system prompts, skills, commands) and security deny rules explicitly stay **ungated** (`075600f1`).
3. CLI & configuration:
   - Add `src/project_trust.rs` supporting `maki trust add|list|remove` and `--trust` flag (`9dc3945e`).
   - Global config support: `trust` policy table in global `init.lua` (`f4cb482b`).
   - Grandfather snapshot resilience (`a26a849b`).
4. UI & Test support:
   - Render interactive trust card in `maki-ui` (`50b323eb`).
   - **Test Fixture Exemption**: In `test_support::spawn_host_for_tests`, construct `ProjectEnvironment` with `TrustPolicy::TrustAll` so automated tests do not block on trust prompts.
5. Documentation generation for trust page (`1c5b4999`).

### Phase 9: Terminal Inline Image Rendering (Issue #45)
1. Add `ratatui-image = { version = "11.0.8", default-features = false, features = ["crossterm"] }` to root `Cargo.toml`.
2. Add `image = { workspace = true, features = ["jpeg", "gif", "webp"] }` to `maki-ui/Cargo.toml`.
3. Wire `ratatui-image` into `maki-ui`:
   - Port `maki-ui/src/terminal_image.rs` and picker setup (`5a98cf16`).
   - Support Kitty, iTerm2, and Sixel protocols with automatic detection.
   - Implement `refresh_images` on terminal font resize or resume.
4. Transcript rendering:
   - Render inline thumbnails in `MessagesPanel` for attached user images and assistant image blocks.
   - Fall back to `[image]` placeholder line when inline images are disabled (`ui.inline_images = false`), during headless runs, or when the terminal lacks graphics support.
   - Apply image decode optimizations: prefix dimension reading (`e36213ba`), message rebuild targeting (`65e3a66b`), and bad image isolation (`0257881a`).
   - Clean up placeholder formatting (`3b9d0ccc`).

### Phase 10: UI, Theming & Performance Enhancements
1. Transcript performance reimplementation:
   - Port `maki-ui/src/wrap.rs` with ASCII fast-path wrapping (`898ac171`) and integrate into `MessagesPanel`. Adopt `maki-ui/benches/wrap.rs`.
   - Memoize syntect syntax highlighting by `(lang, code_hash)` (`5c2f3915`). Adopt `maki-ui/benches/reflow.rs`.
   - Optimize selection copy via forward line walking (`a6305a20`), mapped to makima's document-row selection model.
   - Drop transcript-wide `search_text` memory cache (`f9c24df5`).
   - Ensure all poll/tick methods returning `#[must_use] Dirty` (e.g. `tick_edge_scroll()`, `tick_float_render()`, `poll_live_bufs()`) use `dirty |= ...` so repaints are never missed and `-D warnings` does not flag unused must-use values.
2. Yolo persistence & status bar:
   - Add `yolo: Option<bool>` to `SessionMeta` (`b63a23aa`).
   - Display Yolo indicator in status bar with dim factor `0.15`.
3. Theming & Terminal Colors:
   - Apply `eb70f92e` (named 16-color & 256-color palette indices).
   - Apply `86c5c387`, `97f94e28`, and `0da492df` (separate diff sign, line number, and edit-diff styling).
   - Add diff tokens (`diff_old_sign`, `diff_new_sign`, `diff_old_line_nr`, `diff_new_line_nr`) to `ThemeData` and `ThemesProvider`.
4. Lua UI Library:
   - Port `plugins/lib/maki/toast.lua` stacking corner notifications (`384d22ef`).
   - Port `action_keys` on `plugins/lib/maki/list_picker.lua`.
   - Expose `maki.ui.defer_fn`, `maki.ui.notify`, and `maki.ui.set_window_title` (`054fac6f`).
   - Fix `opt_bool` truthiness bug in `maki-lua/src/api/ui/mod.rs:551` (`77b21c63`).
5. Config additions:
   - Env var expansion `${VAR}` in headers (`e745f057`).
   - `net.allowed_private_hosts` (`6d9393a7`).
   - `agent.rtk` setting (`d83e10e5`).
   - `SessionDefaults` unifying `always_*` settings across frontends (`6c4cc67d`).
   - Default `edit_lines = true` (`e65fd48a`).
   - Add CLI session delete confirmation prompt (`a268c3c1`).

### Phase 11: Lua Supervision Hooks & Subagent Thinking
1. Autocmds & Tool Layering:
   - Add `TurnEnd` autocmd firing on turn completion with reason, usage, and cost (`13f7f396`). Augment `TurnOutcome` in `maki-agent/src/types.rs` with `cost`, `list_cost`, `context_size`, `context_window`.
   - Port `maki-lua/src/agent_autocmd.rs` and `maki-lua/src/session_snapshot.rs`. Filter out subagent turns (`is_subagent == false`).
   - Port `maki-agent/src/tools/hook.rs`, `maki-lua/src/hook.rs`, and `maki-lua/src/api/slot.rs` supporting `tool.<name>.input` and `tool.<name>.output` (`0e68892e`). Input layers run before schema/permissions, but cannot bypass plan mode `fs_write` gating.
   - Expose MCP tools in `code_execution` Python environment via `maki-agent/src/tools/interpreter_bridge.rs` and `plugins/code_execution/init.lua` (`982f1082`).
2. Thinking UI & Subagent Thinking:
   - **Additive Thinking Merge Strategy**:
     - Retain Makima's `plugins/thinking/init.lua` with typed argument metadata (`arguments = { { name = "effort", ... } }`) and interactive popup ladder (`render_ladder`).
     - Add `<M-t>` keybinding to Makima's `plugins/thinking/init.lua`: `maki.keymap.set("n", "<M-t>", open_selector, { desc = "Thinking effort" })`.
     - Adopt upstream Rust-side improvements: `supports_thinking()` check fixes, clamping stored thinking in `ThinkingConfig` so a model requiring thinking does not erroneously report `[off]`, and status bar badge formatting (`[high]` / `[8192 tokens]`).
     - Allow per-task thinking override capped at parent (`4765e776`), integrated with fork's `/thinking` Pi mapping.

### Phase 12: Fork Identity, Nix & Issue Tracking
1. Set `Cargo.toml` `[workspace.package] version = "0.5.5-makima"`.
2. Verify all identity markers:
   - `install.sh` / `install.ps1`: `REPO="lun-4/makima"`, `BINARY="makima"`.
   - `maki-storage/src/version.rs`: `RELEASES_URL` points to `lun-4/makima`.
   - `README.md`, `banner.png`, `splash.rs`: `LOGO="luna-maki"`.
3. Update `flake.lock` if dependencies changed; run `nix flake check`.
4. File GitHub issues for deferred systems:
   - *Lua Jobs & Supervisor API*: Blocked by God Issue #24 / Append-Only Log.
   - *Makima Package Manager*: Cordis-style package manager design session.
   - *Subagent `todo_write` Isolation*: Blocked by #24 full agent unification.
5. Record `3a0c8de5` as upstream synchronization point.

---

## Acceptance Criteria

List of explicit, verifiable acceptance criteria:

- **AC.1**: Upstream syntect highlighting execution is decoupled from render pools, OpenRouter mid-stream retries succeed without session termination, and output cap clamping respects the thinking floor.
- **AC.2**: SSRF guard in `maki-lua/src/api/net.rs` rejects loopback, RFC1918, and metadata endpoints while passing valid URLs; DNS resolution errors return network errors instead of SSRF security violations.
- **AC.3**: Plugin tools declaring `permission = "fs_write"` without declaring `mutable_path` fail registration with a descriptive error.
- **AC.4**: Concurrent mutations to the same file path serialize on `FileWriteLocks`; recursive mutations within the same execution return `SAME_PATH_MUTATION_IN_PROGRESS`; edits to stale files are aborted before mutating content.
- **AC.5**: Consecutive plain user messages queued while the agent is busy are coalesced into a single turn admission with image attachments preserved; messages with mode changes or workflow breaks remain separate turns.
- **AC.6**: Invoking `/compact <custom instructions>` parses without command validation error and passes the custom instructions into the compaction prompt context under "Additional instructions".
- **AC.7**: Project configurations (`.maki/init.lua`), `.env`, and project MCP servers require folder trust approval before loading; prompt text (`AGENTS.md`) and security `deny` rules load unconditionally in untrusted folders; `--trust` grants one-run trust; test fixtures bypass prompt.
- **AC.8**: Terminal image rendering displays thumbnails on Kitty, iTerm2, and Sixel protocols using `ratatui-image`, and cleanly falls back to `[image]` text representation in unsupported terminals or when `ui.inline_images = false`.
- **AC.9**: Coding Plan model choices for Codex are dynamically discovered from the ChatGPT backend without falling back to a hardcoded table unless the backend query fails.
- **AC.10**: `TurnEnd` autocmd fires on whole-turn completion with `reason`, `usage` (including cache counts), and `cost`; subagent turns do NOT trigger `TurnEnd`.
- **AC.11**: Stacking corner toast notifications in `plugins/lib/maki/toast.lua` render without breaking TUI layout and auto-dismiss after their duration.
- **AC.12**: Dropping `SessionEvents` emits `StreamClosed` in-band, causing SDK `stream-json` mode to terminate cleanly on stdin EOF even when Lua tools leave idle contexts open.
- **AC.13**: Subagents spawned with `Adaptive` thinking have their effort clamped to the parent agent's concrete budget via `ThinkingConfig::clamp_to`.
- **AC.14**: `ModelTier::strength` ordering ranks Compaction as weakest (`Compaction < Weak < Medium < Strong`).
- **AC.15**: Lua windows opened via `maki.ui.open_win` with omitted `visible` parameter default to visible rather than hidden (mlua truthiness fix).
- **AC.16**: `SessionDefaults` correctly applies unified `always_*` configuration across Headless, Sdk, Acp, and Print frontends.
- **AC.17**: All Makima fork invariants remain operational with zero regression: persistent agent actor, manager graph, async subagents (`task_spawn`/`task_send`/`task_get`), plan mode override, Pi-effort `/thinking` mapping (`effort_dialect`), and custom splashes.
- **AC.18**: Workspace version in root `Cargo.toml` is `0.5.5-makima`; workspace compiles cleanly and passes all clippy lints under `-D warnings` on remote CI.

---

## Test Strategy

Testing follows the three-layer testing methodology defined in `AGENTS.md` and `<skill:lunas-testing-pillars>`:

### 1. Test Layers
- **Pure Logic & Unit Tests**:
  - `maki-highlight`: Syntect off-render thread pool tests and memoization tests.
  - `maki-providers`: Model tier strength ordering, adaptive subagent thinking clamping, and Retry-After header parsing.
  - `maki-storage`: Advisory lock acquisition/release and trusted folders JSON database persistence.
  - `maki-commands`: Command spec argument parsing for `/compact <instructions>`.
  - `maki-agent`: Queue condensing logic in `actor/queue.rs`, output budget clamping in `compaction.rs`.
- **Integration Tests (Subsystem Boundaries & Protocols)**:
  - `maki-lua`: Net SSRF guard tests (`check_ssrf_cases`), plugin registration rejection (`registration_validation_rejects`), and window truthiness (`opt_bool`).
  - `maki-agent`: File write locking serialized mutation and reentrancy tests in `write_lock_regression.rs`.
  - `maki-acp`: ACP event translation and permission request diff generation tests in `translate.rs` and `server.rs`.
- **Interface & Visual Verification**:
  - `maki-ui`: Terminal image protocol fallback tests, `MessagesPanel` reflow and wrap benchmarks (`wrap.rs`, `reflow.rs`), and status bar `[yolo]` badge rendering.

### 2. Test Infrastructure Additions
To prevent automated tests from deadlocking or prompting interactively:
- **Trust Test Environment**: Ensure `test_support::spawn_host_for_tests` sets up a pre-trusted `ProjectEnvironment` (`TrustPolicy::TrustAll`) so tests creating temporary git roots run without trust interruption.
- **ACP Diff Fixtures**: Add sample diff generation assertions in `maki-acp/tests` validating that `write` and `edit` generate unified diff strings.

### 3. Named Test Mapping Table

| Acceptance Criterion | Verification / Named Test Case | File Location |
|---|---|---|
| **AC.1** | `test_syntect_off_render_pool`<br>`test_openrouter_retry_stream`<br>`test_clamped_output_cap_above_thinking_floor` | `maki-highlight/src/lib.rs`<br>`maki-providers/src/providers/openrouter.rs`<br>`maki-agent/src/agent/streaming.rs` |
| **AC.2** | `check_ssrf_cases`<br>`an_unresolvable_host_reads_as_a_network_failure` | `maki-lua/src/api/net.rs` |
| **AC.3** | `registration_rejects_fs_write_without_mutable_path` | `maki-lua/tests/plugin_host.rs` |
| **AC.4** | `mutable_tools_share_path_lock`<br>`dispatched_handlers_serialize_on_the_lock`<br>`test_file_write_locks_canonical_key_resolution`<br>`test_same_path_reentry_returns_error` | `maki-lua/src/write_lock_regression.rs`<br>`maki-agent/src/tools/file_locks.rs` |
| **AC.5** | `test_actor_condenses_consecutive_plain_user_messages`<br>`test_actor_preserves_images_in_condensed_burst`<br>`test_actor_does_not_condense_tool_results_or_slash_commands` | `maki-agent/src/actor/queue.rs` |
| **AC.6** | `test_compact_command_parses_optional_instructions`<br>`test_compact_with_custom_instructions` | `maki-commands/src/spec.rs`<br>`maki-agent/src/agent/compaction.rs` |
| **AC.7** | `test_trusted_folder_persistence`<br>`test_untrusted_folder_detection`<br>`test_untrusted_folder_still_loads_agents_md`<br>`test_project_environment_trust_gating` | `maki-storage/src/trusted_folders.rs`<br>`maki-config/src/project.rs` |
| **AC.8** | `test_protocol_detection_fallback`<br>`test_dimension_prefix_decoding`<br>`test_image_rendering_fallback_to_text` | `maki-ui/src/terminal_image.rs`<br>`maki-ui/src/chat.rs` |
| **AC.9** | `test_dynamic_coding_plan_discovery` | `maki-providers/src/providers/openai/codex.rs` |
| **AC.10** | `test_turn_end_autocmd_fires_with_metadata`<br>`test_turn_end_ignores_subagents`<br>`plan_ready_fires_once_per_draft` | `maki-lua/src/agent_autocmd.rs`<br>`maki-ui/src/app/tests.rs` |
| **AC.11** | `test_toast_stacking_and_dismissal` | `maki-lua/tests/toast.rs` |
| **AC.12** | `test_session_events_guard_emits_stream_closed_on_drop` | `maki-agent/src/types.rs` |
| **AC.13** | `test_adaptive_subagent_clamped_to_parent_budget` | `maki-providers/src/types.rs` |
| **AC.14** | `test_model_tier_strength_ordering` | `maki-providers/src/model.rs` |
| **AC.15** | `test_opt_bool_truthiness_visible_window` | `maki-lua/tests/ui.rs` |
| **AC.16** | `test_session_defaults_unification` | `maki-config/src/defaults.rs` |
| **AC.17** | `test_subagent_run_end_lifecycle`<br>`test_mode_plan_override_isolated`<br>`test_splash_render_contract`<br>`test_vertex_provider_headers`<br>`test_pi_effort_dialect_mapping` | `maki-agent/tests/subagent_run_end.rs`<br>`maki-agent/src/modes.rs`<br>`maki-ui/src/splash.rs`<br>`maki-providers/src/providers/gemini.rs`<br>`plugins/thinking/init.lua` |
| **AC.18** | Full workspace build, clippy, test suite, and doc check: `just check && just lint && just test && just gen-docs-check` | Remote CI: `.ssh/remote-ci.sh` |

### 4. Verification Protocol
Because Rust builds are compute-intensive, strictly follow the local/remote split defined in `AGENTS.local.md`:
1. **Local Lua Checks**:
   ```bash
   for f in $(find plugins -name '*.lua'); do luac -p "$f" || echo "BROKE $f"; done
   ```
2. **Local Formatting**:
   ```bash
   just fmt-check
   ```
3. **Full Remote CI (Build Box at `100.122.23.69`)**:
   ```bash
   .ssh/remote-ci.sh
   ```

---

## Review Strategy

Review occurs in two distinct phases:

### 1. Plan Review Phase (Pre-Implementation)
- Conducted via an adversarial research subagent running the `plan_reviewer` rubric.
- Every acceptance criterion must map to concrete named test cases.
- Any critical or high findings must be resolved and updated in the plan before code execution begins.
- Loop repeats until a clean audit verdict (`VERDICT: pass`) is achieved.

### 2. Implementation Review Phase (Post-Implementation)
- Following the completion of Phases 1 through 12 and a green remote CI run (`.ssh/remote-ci.sh`), dispatch an implementation review subagent.
- The review subagent will audit git diff (`git diff mistress...HEAD`) against:
  - Additive-only guarantee: verify zero Makima fork features dropped.
  - Security audit: verify folder trust gating and SSRF guard protections.
  - Concurrency audit: verify `Dirty` bitwise OR discipline and `FileWriteLocks` mutual exclusion.
- All implementation review findings must be addressed before final PR submission.

---

## Documentation Strategy

Documentation updates follow the Diátaxis structure governed by `AGENTS.md`:

1. **Repository & Architecture Documentation**:
   - Update `maki-agent/README.md` or memory notes documenting `FileWriteLocks` canonicalization with `maki_storage::paths::canonical_key`.
   - Update `.agents/makima-issues/138.md` with implementation closure details.
2. **User-Facing Documentation (`site/docs/`)**:
   - Port upstream documentation for Folder Trust (`site/docs/content/folder-trust/`).
   - Document `/compact <instructions>` capability in commands reference.
   - Document terminal inline image support and configuration flag (`ui.inline_images`) in configuration reference.
   - Document `maki trust add|list|remove` and `--trust` flag in CLI reference.
   - Run `just gen-docs` to regenerate tool, command, and Lua API documentation via `maki-docgen`. Verify with `just gen-docs-check`.

---

## Risks, Blockers, and Required Decisions

### 1. Risk Matrix

| Risk | Severity | Mitigation Strategy |
|---|---|---|
| **Folder Trust Deadlock in Tests** | High | Default all unit and integration test hosts (`test_support::spawn_host_for_tests`) to `TrustPolicy::TrustAll` to prevent test processes from hanging on interactive stdin prompts. |
| **`Dirty` Bitwise OR Regression** | High | Strictly audit all TUI tick and poll call sites (`tick_edge_scroll()`, `tick()`, `poll_live_bufs()`) during Phase 10 to ensure `dirty |= ...` is used everywhere; `-D warnings` in remote CI catches unused must-use values. |
| **ACP CommandRegistry Breakage** | High | Explicitly reject upstream commit `8819365a` (monolithic ask refactor). Port only diff generation (`5d79739e`) and tagged request IDs (`ae5fd640`) onto Makima's existing `CommandRegistry` dispatch. |
| **Staleness Tracking Bypass** | Medium | Keep `ctx:record_read` actively recording into `FileReadTracker` so tools like `read` and `grep` continue updating read timestamps required for pre-mutation checks. |
| **Terminal Graphics Hangs** | Medium | Isolate image decodes behind prefix dimension checks (`e36213ba`) and wrap terminal queries with timeouts; provide instant text fallback if terminal does not report graphics capabilities within deadline. |
| **Remote CI Latency** | Low | Batch independent phase implementations locally, test Lua syntax with `luac -p`, and run formatting with `just fmt-check` before streaming runs to `.ssh/remote-ci.sh`. |

### 2. Blockers & Decisions Log
- **Decision: Lua Jobs API**: Deferred to post-God Issue #24 design session. Standalone security fix `4d847507` is cherry-picked; all other jobs commits (`cb7b8b4d`..`df045ed5`, `ce73793e`) are deferred.
- **Decision: Package Manager (`maki-pack`)**: Defer upstream `maki-pack` commits (`ade51171`..`2a2c06b8`) to a dedicated custom package manager design session to avoid colliding with `maki-commands`. Port only `maki-storage/src/lock.rs` and `fs4` dependency required by Folder Trust.
- **Decision: OpenTelemetry**: Denied and dropped (`df0a069e`..`ed1f05e9`) to protect binary size and TUI responsiveness.
- **Decision: Thinking Picker**: Retain Makima's `plugins/thinking/init.lua` with typed argument metadata for `maki-commands` autocomplete and `render_ladder`; bind upstream's `<M-t>` keymap; port Rust-side clamping.
- **Decision: xAI Excised**: All upstream xAI references and commits (`xai` provider) remain permanently deleted from Makima.
