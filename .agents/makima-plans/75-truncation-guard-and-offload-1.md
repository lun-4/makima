# Truncated output: write-time guard and offload store (#75)

### Goal

Close lun-4/makima#75 in a single PR with two parts. First, a stateless write-time guard (plus fuzzy-matcher hardening) that rejects the ways a line shown truncated to the model can be written back lossily: altering it in place, matching it fuzzily, or pasting the marked prefix anywhere, including into a new file. The residual risk is listed under Risks. Second, a host-owned, session-scoped offload store so that tool output past the limits is saved to a file instead of discarded.

### Implementation Summary

Background: `read` cuts physical lines at 1000 bytes and appends `[line truncated]` (`maki-agent/src/tools/mod.rs:646` `truncate_line`). The model can then write that text back with `write` or `edit_lines`, persisting the marker and losing the suffix. This has already happened in this repo: `.agents/makima-plans/50-command-registry-acp-unification.md` was committed in `f3fb2f83` with two persisted markers, and the original text is not in git history. PR #85 (closed as overengineered) tracked byte-interval provenance. The maintainers agreed on #85 to follow Claude Code: offload output that would be truncated.

The design came out of a discussion plus an adversarial review. Its key conclusions:

- The stateful read tracker is not a sound basis for the guard. The dispatcher calls `record_read` after every successful mutation (`maki-agent/src/agent/tool_dispatch.rs:705-708`), and grep calls it on every matched file (`plugins/grep/init.lua:286-288`). The tracker is also in memory and private to each agent: subagents (`maki-lua/src/api/agent.rs:1210`), resume and headless all start empty. So the guard is stateless: it checks the content at write time.
- `edit` is not safe by construction. `block_anchor` matches a block on its trimmed first and last lines alone, with a single-candidate threshold of `0.0` (`plugins/lib/maki/fuzzy_replace.lua:7, 434-475`). `context_aware` accepts a block when 50% of its middle lines match. Either can match a block whose middle line the model saw truncated and replace it.
- `read` never offloads. Its source is already a file and its output is paged, so it cannot loop. Offload files are ordinary files that read/grep/glob can open without prompting (none of them declares a permission). Bodies are normalised by stripping trailing `\n` before hashing and storing. With that, a `cat` of an unedited offload file through bash (which joins lines without a trailing newline) hashes to the same name. Its bytes match, so it gets a pointer back instead of a new file. Files capped at 8 MiB are the exception, and so are `cat` commands that rtk rewrites (`agent.rtk`). Neither can loop, because each `cat` is itself capped, but neither is deduplicated.
- Partial lines stay. The #85 discussion considered pi's approach: omit an oversized line entirely instead of showing a prefix, to avoid a read, failed write, retry cycle. That would not remove any of the checks below. A whole-file `write` can still drop a line the model never saw, `edit_lines` can still replace it, the fuzzy matchers can still swallow it, and a placeholder can still be pasted. So omitting lines only takes away the useful prefix, for example of a long string literal. The checks only fire on lossy writes, so normal edits never enter a retry cycle.
- Line cap: one agent-wide `agent.max_line_bytes` (default 1000) is used by read, grep and the guard (operator decision). The per-plugin `max_line_bytes` options of read and grep become deprecated no-ops. They can't simply be removed, because an unknown plugin option fails the plugin load (`maki-lua/src/api/options.rs:204-216`).

Touch points:
- maki-config: `AgentConfig` (`maki-config/src/lib.rs:1367`).
- maki-agent: `tools/mod.rs` truncation helpers, a new `tools/offload.rs` (`OffloadBackend`, `OffloadStore`, `DiskBackend` and `limit_output`), `ToolContext` (`tools/mod.rs:444`), `AgentParams`, `agent/run.rs:704` `Agent::tool_context`, `agent/tool_dispatch.rs` (MCP results), `headless.rs` (print mode).
- maki-storage: `sessions.rs`, adding `OFFLOAD_DIR` and cleanup in `Session::delete_from`.
- maki-lua: `api/util/ctx.rs` (new `ctx:limit_output`), `api/agent.rs` (child agents inherit the store), `api/fs/in_memory.rs` (an `OffloadStore` over the in-memory map, for test hosts), and `api/text.rs` (marker constants and docs).
- Plugins: `lib/maki/output_limits.lua`, a new `lib/maki/long_lines.lua`, `lib/maki/fuzzy_replace.lua`, `edit`, `write`, `read`, `grep`, `bash`, `code_execution`, `webfetch`, `websearch`, `glob`.
- Docs: site/docs.

Shipping: one PR containing all six phases, closing #75 ("Closes #75"). Phases 1-3 (guard) land before phases 4-6 (offload) and pass `just test` on their own, so commits can be split along that line within the PR. The executing agent must not commit or open the PR without the operator's explicit, per-batch consent.

Non-goals:
- Excluding the state dir from grep/glob walks. Walking from `$HOME` already reaches `sessions/*.jsonl` today; this is unrelated to this change.
- Copying offload files on `--fork-session`. The dangling path is accepted and documented.
- Offloading `read`, `index`, `memory`, task subagent output or the user `!` shell (`maki-ui/src/app/shell.rs`).
- Limiting host-supplied local tools (`run_local_tool`, used by SDK/ACP clients and `maki.agent.session` tools). Their output is the client's responsibility and stays unchanged.
- Introducing truncation where none exists today. When MCP results have no offload store, they pass through unchanged, as they do now.
- Any change to the `FileReadTracker` mtime freshness check.

### Implementation review fixes (2026-10-03)

The read-only review of `7e478a9a..dafa7cb5` found four medium-severity defects and one low-severity restore inconsistency. The operator approved fixing all five without committing.

1. TUI session replacement must reuse the existing `Arc<OffloadStore>` when the session ID is unchanged. The outgoing runtime may still have child or MCP writes in flight when the replacement starts. Both runtimes must share the same quota mutex. A new session ID gets a new store. Regression coverage must assert Arc identity through the production replacement path and gate concurrent additions near the quota.
2. TUI offload paths must use the supplied `StateDir`, not resolve the global state directory inside the actor constructor. A non-default-root regression must verify the artifact path and removal through session deletion.
3. Per-plugin zero output-line and output-byte overrides remain supported. Zero requests metadata-only output under the existing metadata exception; it must not throw from a completed bash job or cancellation hook. Tests cover synchronous tool output and bash exit/cancel callbacks for both overrides.
4. The mid-line note must take O(content bytes + matches), including exact global edits on short lines. Advance one cursor through line boundaries without copying or rescanning the prefix for every match. Specs cover many short-line matches, matches sharing a line, and short matches preceding the first qualifying long-line match. Verify scaling with a focused probe where available.
5. grep restore must recognize an offload notice anywhere in the parsed trailer. A cut path-header preview can precede the footer with no parsed entries. Live and restored views must match for that shape; no-match and error output still decline specialized restoration.

The accepted implementation differences remain: escape-normalized candidates need no additional long-line guard because the full-block branch requires equality after unescaping and the substring branch replaces only that substring, preserving its suffix; preview cuts use `[line cut: first/last X of Y bytes]`; print cleanup uses a Drop guard; MCP permission-scope compatibility is tested through `truncate_scope_cases`; the bash in-memory-store test uses a RealFs host because its coordinator must be registered. Generated docs are refreshed after API documentation changes.

The original phase requirements below include these corrections.

Implementation completed:

- `SpawnCtx::prepare_runtime_with_config` constructs TUI stores from its supplied `StateDir`. `AgentHandles` retains the store, clones it into the actor backend, and exposes it to same-ID replacement preparation. New session IDs allocate distinct stores. Production rewind entry points and the test-only respawn path retain store identity.
- `ctx:limit_output` forwards zero per-call limits to the existing limiter. The limiter returns metadata only, preserving footer/pointer/trailer text and the success pair. API source documentation and generated Lua API docs describe zero limits.
- `mid_line_end` advances through LF boundaries with one cursor and no prefix allocations. Regression inputs include matches ending inside short lines, not only matches at line ends.
- grep restore scans all parsed trailer lines for an offload notice. A cut path header before the footer restores to the same spans as the live view.

Verification completed:

- `cargo check -p maki-lua --tests`, `cargo check -p maki-ui --tests`, scoped Clippy for both crates, all 1,524 Lua tests and all 1,993 UI tests passed. The four new UI regression cases passed.
- The new zero-limit API matrix and cut-header grep regression failed for the expected defects before their production fixes, then passed after the fixes.
- A temporary embedded-Lua probe replaced `X` in `string.rep("Xb\n", count)` with the line cap enabled. Measured times were 15.4 ms at 20,000 matches, 26.1 ms at 40,000, 48.0 ms at 80,000 and 93.9 ms at 160,000. These timings are consistent with linear scaling; they are observations, not timing assertions. The probe was removed after measurement. All 18 permanent Lua specs passed afterward.
- `just check`, `just lint`, `just test` (6,758 tests, none skipped), `just fmt-check`, `just gen-docs-check` and `git diff --check` passed on the combined fixes. Generated docs were refreshed.
- `nat-code-reviewer:offload-fix-review` approved the fixes with no findings after static inspection. It could not independently run tests or inspect the exact git diff; the parent verified both.

The first UI regression run failed because its replacement fixtures used the pre-activation test-model string. The fixtures now use the active runtime model, and the rerun passed. An initial plan edit missed its indentation and was reapplied successfully. No tool, build or test error remains unresolved. No changes were staged or committed.

### Implementation Plan

#### Phase 1: one line cap, honest truncation markers, read fixes

1. **The cap in config.** In `maki-config/src/lib.rs`, add `pub const DEFAULT_MAX_LINE_BYTES: usize = 1000;` and `pub const MIN_LINE_BYTES: usize = 80;` next to `DEFAULT_MAX_OUTPUT_BYTES` and `MIN_OUTPUT_BYTES`. Add the field `max_line_bytes: usize` to `AgentConfig` with `#[config(default = DEFAULT_MAX_LINE_BYTES, min = MIN_LINE_BYTES, desc = "Max bytes of one line shown by read and grep; longer lines are cut, and write/edit_lines refuse to alter them")]`. Add the matching `Option<usize>` to `AgentFileConfig`, to `from_file`, and to merge and validation. Copy the patterns used for `max_output_lines`, including the min-validation `test_case` rows near `lib.rs:2831` and `:2904`.
2. **A shared accessor.** In `plugins/lib/maki/output_limits.lua`, add `M.line_bytes(ctx)` returning `ctx:config("max_line_bytes", DEFAULT_MAX_LINE_BYTES)`. Read, grep and the guard all use this one accessor, so the cap is defined in one place. Keep `M.DEFAULT_MAX_LINE_BYTES` as it is, because bash and task use it for their UI views (`plugins/bash/init.lua:168`, `plugins/task/init.lua:555`). Those caps are display-only and never reach the model.
3. **Deprecate the per-plugin options.** In `plugins/read/init.lua` and `plugins/grep/init.lua`, replace the `max_line_bytes` option spec with `{ type = "integer", desc = "Deprecated and ignored; use agent.max_line_bytes." }`, with no default and no min. If `opts.max_line_bytes ~= nil`, call `maki.log.warn` once at load. Both plugins take the cap from `output_limits.line_bytes(ctx)`. grep passes it as `max_line_bytes` to `maki.fs.grep`.
4. **The marker states what was cut.** Change `truncate_line` in `maki-agent/src/tools/mod.rs` so that a cut line ends with `[line truncated, +N bytes]`, where N is the number of bytes hidden. Rename `LINE_TRUNCATED_MARKER` to `LINE_TRUNCATED_PREFIX` (`"[line truncated"`) and add `fn format_line_truncated_marker(hidden: usize) -> String`. The result must stay within `max_bytes`. Reserve the marker length for `hidden = line.len()`, which gives an upper bound on the digit count, then compute the real `hidden` from the UTF-8 boundary. Update the existing test at `mod.rs:1084` and the `maki.text.truncate_line` doc comment in `maki-lua/src/api/text.rs:24`. MCP permission scopes (`agent/tool_dispatch.rs:902`) also call `truncate_line`, and persisted allow/deny rules match on that string. They switch to a separate `truncate_scope(input, max_bytes)` that keeps the legacy fixed `[line truncated]` suffix, so existing rules keep matching and a scope doesn't encode the input's length. Test: `mcp_perm_scope_format_unchanged`. `plugins/lib/maki/tool_view.lua:44` is a UI-only renderer and keeps its own marker.
5. **Correct the remaining-lines count.** In `truncate_file`, when `remaining_lines` is `Some(r)` and the cap cuts lines, report `r + (lines not emitted)`. Today it undercounts by the lines the byte cap removed (`plugins/read/init.lua:114-116`). When it is `None`, behaviour is unchanged.
6. **Stat before reading.** In `plugins/read/init.lua` `read_file`, call `ctx:record_read(path)` before `maki.fs.read(path)`. A file that changes between the two calls then leaves an older mtime, so the next edit fails as stale (the safe direction). A side effect: a read that then fails (for example on non-UTF-8 content) still counts as a read for the staleness check. That is accepted and listed in Risks, because the long-line guard doesn't depend on the tracker. Test infrastructure: the existing `ReadBarrierFs` (`maki-lua/src/write_lock_regression.rs:451`) wraps `InMemoryFs`, but the tracker stats the real disk. Add a `RealReadBarrierFs` test backend next to it that wraps `RealFs` and blocks `read` until released. Give it its own test, `real_read_barrier_blocks_until_released`.
7. **Read description.** Replace the "truncation hints (e.g. \"truncated lines X-Y\")" bullet with the actual hint, `[file truncated, N lines remaining]`. Add one bullet: "Lines longer than `agent.max_line_bytes` end with `[line truncated, +N bytes]`. That marker is not file content. Change such a line with `edit` and an exact `old_string`; `write` and `edit_lines` refuse to alter it."
8. **Audit.** Repair `.agents/makima-plans/50-command-registry-acp-unification.md`.
   - Line 16: replace the marker with ` (remainder lost to a truncated read, see #75)`.
   - Line 56: split the AC.5 bullet back onto its own line, and replace its trailing marker the same way.
   - Then run `git grep -n "\[line truncated"` once and paste its output into the PR description. This is a one-time audit, not a permanent test, because any future doc or plan discussing the feature would trip a permanent check.

#### Phase 2: stateless write-time guard

1. **The guard module.** Create `plugins/lib/maki/long_lines.lua` (required as `maki.long_lines`). In this module, lines are split on `\n` with any trailing `\r` stripped, and "long" means `#line > max_line_bytes`.
   - Both checks below count occurrences rather than test set membership. Each long line of the old text consumes one occurrence of an identical line in the new text, from a count table built once per check. So two identical long lines can't silently become one, while moving lines stays allowed. Lines are compared exactly, including indentation.
   - `M.check_write(before, after, max_line_bytes)` returns `nil` or an error string. Every long line of `before` must be matched by its own occurrence in `after`.
   - `M.check_replace_lines(content, start_line, end_line, new_string, max_line_bytes)` returns `nil` when `new_string == ""` (an explicit deletion; the model knows which lines it is removing). Otherwise every long line in `[start_line, end_line]` must be matched by its own occurrence in `new_string`.
   - The error is a module constant format string, `M.LONG_LINE_CHANGED`: `"line %d is %d bytes, longer than agent.max_line_bytes (%d), so it may have been shown truncated; this change would drop or alter it. Use edit with an exact old_string to change text inside that line."` It reports the first offending line, numbered in `before`.
   - `M.check_markers(before, after)` closes copy and move bypasses: deleting the long line and pasting a truncated copy elsewhere, writing a truncated read into a new file, or removing the file and writing it again.
     - It rejects any line of `after` that ends with a truncation marker, unless an identical line is still available in `before`. This uses the same occurrence counting, so duplicating an existing marker line is also rejected.
     - Marker patterns come from one list exported by Rust through `maki.text.truncation_marker_patterns()`, so Lua and Rust can't drift. The list holds the new `[line truncated, +N bytes]`, the legacy `[line truncated]` (which still appears in resumed histories and `.agents/` plans), and phase 4's `[line cut: first/last X of Y bytes]`.
     - The error is `M.TRUNCATED_MARKER_ADDED`, a format naming the line number and the marker category matched, each with its own recovery text:
       - read or grep marker (new or legacy): `"... ends with %s, a marker from truncated read/grep output; the text after it was never shown. Change the original line with edit instead."`
       - preview cut marker: `"... ends with %s, a marker from a cut tool-output preview; read the saved output file for the full line."`
       - Every variant ends with: `"If this line really is content that ends with that text, write it with a shell command."`
     - What counts as "added": a line of `after` ending in a marker that has no remaining identical line in `before`. Removing a marker, or deleting the line, never triggers this check. Duplicating a marker line, or changing it into a different line that still ends in a marker, does.
     - Scope of the exemption: a line ending in marker text can be kept unchanged, and every other line in that file stays freely editable. Adding a new line that ends in marker text, or changing an existing one, is rejected and needs a shell write. That is narrower than #75's "never reject legitimate files", and it is accepted: it only affects lines whose last bytes are exactly a marker, and the error text says how to proceed. Lines with marker text in the middle are unaffected.
     - The marker-agnostic checks above stay the primary guard, so a model that strips the marker is still caught when it alters the original line.
2. **`write`.** In `plugins/write/init.lua`, after `check_before_edit`:
   - Classify the target without broad assumptions, so the guard never fails open:
     - `local meta, meta_err = maki.fs.metadata(path)`. `(nil, nil)` means the file is absent: `before = ""`. `(nil, err)` returns `err` as an error without writing (`maki-lua/src/api/fs/mod.rs:195-212` returns `(nil, err)` for failures other than NotFound).
     - Otherwise, `maki.fs.read_bytes(path)`. It has no UTF-8 decoding to throw on, so it returns `(nil, err)` for IO failures, which are returned as errors without writing. Permission errors raised by the guard wrapper (`maki-lua/src/plugin_permissions.rs:70-97`) propagate as errors, as they do today.
     - Convert with `buffer.tostring` and check `utf8.len(text)`. Only a `nil` result, which positively identifies invalid UTF-8, skips `check_write`, since read can't have shown such a file as text lines. `check_markers` still runs against `""`. Valid text becomes `before`.
   - Run `check_write(before, content, output_limits.line_bytes(ctx))`, then `check_markers(before, content)`. On an error, return `{ llm_output = err, is_error = true }` without writing.
3. **The edit tools.** In `plugins/edit/init.lua`:
   - The `edit_lines` transform runs `check_replace_lines` on the LF-normalised content (inside `preserve_line_endings`) before `replace_lines`, and returns `nil, err` on failure.
   - `apply_edit` runs `check_markers(before, after)` for every edit tool (`edit`, `multiedit`, `edit_lines`, `insert_lines`) before `atomic_write`.
   - `insert_lines` gets no long-line check, since an insertion cannot lose bytes.
   - The guard does not depend on `agent.stale_read_check`.

4. **Tool descriptions.** `EDIT_LINES_DESCRIPTION` and the `write` description say they refuse to drop or alter lines longer than `agent.max_line_bytes`, and to reject added `[line truncated, +N bytes]` markers. Both point to `edit` for changes inside such lines, and say that a long line can be deleted with `edit_lines` and an empty `new_string`.

#### Phase 3: fuzzy-matcher hardening and the mid-line note

1. **New parameter.** `fuzzy_replace.replace(content, old_string, new_string, replace_all, max_line_bytes)` takes a fifth argument. When it is `nil`, there is no long-line protection, which keeps any other caller unchanged. `edit` and `multiedit` pass `output_limits.line_bytes(ctx)`.
2. **Long lines in fuzzy candidates.** In `try_match`, a candidate `matched` from any replacer other than `exact` is skipped unless each of its long lines can be matched to its own trimmed-equal line in `find`. This uses occurrence counting: a count table of `find`'s trimmed lines, rebuilt for each candidate, with each long candidate line consuming one occurrence. Without counting, a candidate whose long line `L` appears twice could match against an `old_string` holding a single `L`, and the replacement would silently drop the second copy. `exact` is exempt because `matched == find`. A skipped candidate does not set `any_found`, so the user gets `NO_MATCH` rather than `MULTIPLE_MATCHES`. Apply the rule to the main `REPLACERS`/`LATE_REPLACERS` loops. The `escape_normalized` path remains exempt: its full-block branch requires equality after unescaping, and its substring branch changes only the exact unescaped substring while preserving unmatched bytes.
3. **The mid-line note.** `replace` returns a third value: `nil`, or `{ line = n, rest = k }` when a replacement's match ends strictly inside a line (the next byte is not `\n` and not end-of-content) whose full length exceeds `max_line_bytes`. `n` is the 1-based line number in the original content, and `k` is the number of bytes from the match end to the end of that line. For `replace_all`, report the first such match. Advance a single line-boundary and line-number cursor over the ordered match offsets, keeping total work O(content bytes + matches). Do not materialize the prefix or rescan a line for every occurrence.
   - `edit` and `multiedit` append `"\nnote: the match ends inside line %d, which continues for %d more bytes that old_string did not cover"` to the `llm_output` summary. This catches the case where the model pasted a visible prefix to replace a whole line.
   - multiedit reports it for the first edit that triggers it, as `edits[i]`. Its line number refers to the content that edit ran on, which is stated in the note text: `"(line numbers as of edits[i])"`.
   - `EDIT_DESCRIPTION` and `MULTIEDIT_DESCRIPTION` gain one line explaining the note.
   - `preserve_line_endings` and `apply_edit` only pass `(result, err)` through. The handlers therefore capture the third value in a handler-local upvalue set inside the transform closure, and read it after `apply_edit` returns.

#### Phase 4: offload store core

1. **Storage layout.** In `maki-storage/src/sessions.rs`, add `pub const OFFLOAD_DIR: &str = "offload";` and `pub fn offload_dir(sessions_dir: &Path, id: MakiId) -> PathBuf` (returning `sessions_dir/offload/<id>`). In `Session::delete_from`, remove that directory best-effort, exactly like the `ARCHIVE_DIR` sweep at `:1697`: ignore NotFound and `warn!` on other errors. `is_session_file` already ignores directories.
2. **The store.** Create `maki-agent/src/tools/offload.rs`. Constants `MAX_OFFLOAD_FILE_BYTES` (8 MiB) and `MAX_OFFLOAD_SESSION_BYTES` (256 MiB) go at the top. Move `tempfile = { workspace = true }` from maki-agent's `[dev-dependencies]` to `[dependencies]`.
   - The store logic is written once and sits over a minimal backend:
     - `pub trait OffloadBackend: Send + Sync` has raw operations only: `fn read(&self, name) -> io::Result<Option<Vec<u8>>>`, `fn create_new(&self, name, bytes) -> io::Result<bool>` (false if the name is taken), `fn list_slots(&self, hash_prefix) -> io::Result<Vec<String>>` (the existing names matching `<h>.txt` / `<h>-N.txt`), `fn total_bytes(&self) -> io::Result<u64>`, `fn remove_all(&self) -> io::Result<()>`, and `fn path(&self, name) -> PathBuf`.
     - `pub struct OffloadStore { backend: Box<dyn OffloadBackend>, state: Mutex<StoreState> }`, where `StoreState { closed: bool }`. It provides the single storage operation `pub fn put(&self, body: &str) -> io::Result<Saved>`, plus `pub fn path_of(&self, saved: &Saved) -> PathBuf` and `pub fn close_and_remove(&self) -> io::Result<()>`.
     - `Saved { name: String, outcome: Created | Existing, original_bytes: u64, stored_bytes: u64 }`.
   - `put` performs these steps in order, holding the `state` mutex throughout. If the store is closed, it fails with a `closed` error, and the caller falls back to plain truncation.
     1. Compute the stored form: `body`, capped at `MAX_OFFLOAD_FILE_BYTES` on a UTF-8 boundary. When capped, append the note `\n[offload capped: first X of Y bytes saved]`. The note is excluded from the 8 MiB.
     2. Let `h` be the first 16 hex chars of sha256(full body). Compare every existing slot from `list_slots(h)` (`<h>.txt`, `<h>-2.txt`, ...) against the stored form. If any matches byte-for-byte, return `Existing`. Editing and deleting are allowed (the model may edit a result and paste it back), so neither the name nor the lowest free slot proves anything. Only if none matches is the lowest free name chosen as the write target. Example: `h.txt` created, then edited, then `h-2.txt` created, then `h.txt` deleted. A repeat of the output still finds `h-2.txt`.
     3. Duplicates never reach the quota check, because they need no extra storage. For a new file, `backend.total_bytes()` is recomputed under the lock, and `total + stored_form.len() > MAX_OFFLOAD_SESSION_BYTES` fails with a quota error. Recomputing means edits that grow, shrink or delete files are reflected in the next decision; there is no cached count to drift.
        - The guarantee covers additions made by `put`. Shell or other external writes into the directory can't be bounded by this lock. They are counted at the next `put`, but they can push the directory past the quota in between.
     4. Call `create_new`. If it returns false (an outside writer took the name), go back to step 2 under the lock.
   - One `Arc<OffloadStore>` is shared by every tool in the session, on parent and child contexts, so the mutex serialises parallel tools and subagents. Cross-process writers can't occur, because `session_lock` already keeps a session open in a single process.
   - `close_and_remove` takes the lock, which waits for any in-flight `put`. It then sets `closed`, calls `backend.remove_all()`, and releases the lock. Later `put`s, for example from a child agent that outlives the root turn, fail with `closed` and can't recreate the directory.
   - `pub struct DiskBackend { dir }` (maki-agent):
     - Create the directory with mode 0700 on unix.
     - `create_new` writes a `tempfile::NamedTempFile` (0600 on unix) in `dir`, then `persist_noclobber`, mapping `AlreadyExists` to `false`.
     - `total_bytes` sums regular files.
   - `InMemoryOffloadBackend` (`maki-lua/src/api/fs/in_memory.rs`) implements the same raw operations over the `InMemoryFs` file map. Test hosts use it so that Lua `read` can open offloaded files without touching disk.
3. **`limit_output`.** `pub fn limit_output(body: &str, opts: &LimitOpts, store: Option<&OffloadStore>) -> String`, where `pub struct LimitOpts<'a> { pub trailer: Option<&'a str>, pub shape: PreviewShape, pub label: &'a str, pub limits: OutputLimits }`. `label` is `"output"` by default and `"search results"` for grep, and it is used in the footer and the pointer.
   - `pub enum PreviewShape { Head, HeadTail }`.
   - `pub struct OutputLimits { pub max_lines: usize, pub max_bytes: usize, pub max_line_bytes: usize }`.
   - It first strips trailing `\n` from `body`; that normalised body is what gets hashed, stored and previewed.
   - **What the limits cover.** `max_lines` and `max_bytes` bound the whole returned text: preview, separators, omission line, footer and trailer. The one exception covers metadata, which is never cut: the footer, the pointer, the fallback `FILE_TRUNCATED_MARKER` line with any "not saved" reason, and the trailer. When metadata alone exceeds the limits, the result is the metadata with no preview. This holds in every branch, the no-store and failed-`put` fallbacks included. A preview segment whose cut marker can't fit in its own budget is omitted entirely, never rendered partially. The preview budget is computed from the actual rendered footer and trailer, not from a fixed reserve.
   - **Within limits** (the body plus trailer fits): return `body`, plus `"\n" + trailer`. This is the same assembly bash does today.
   - **No store, or `put` fails** (IO or quota): return `truncate_file(body, ...)` plus the trailer, within the same total budget. If `put` failed, `" (full output not saved: <reason>)"` is added after the `FILE_TRUNCATED_MARKER` line, and a `warn!` is logged with `error`, `dir` and `bytes`.
   - **Footer** (`Created`), honest about fidelity:
     - Uncapped: `[output truncated: L lines, S; all of it saved to P; inspect it with grep, or read with offset and limit]`.
     - Capped: `[output truncated: L lines, S; first S' saved to P, the remaining R discarded; ...]`.
     - "All of it" means all of the tool's rendered output. For grep, that is the search results with lines already clipped at `max_line_bytes` (`maki-agent/src/tools/grep.rs:183`), so clipped lines keep their markers in the file. The marker check therefore still applies to text copied from it. The grep footer says `search results` instead of `output`, from a per-call label passed in `opts`. It also states, to the model: `lines longer than agent.max_line_bytes are clipped in the saved file too`.
     - If any stored line exceeds `max_line_bytes`, the advice becomes `inspect it with bash (e.g. jq, or cut -c) since some lines exceed agent.max_line_bytes`.
   - **Pointer** (`Existing`): `[output identical to a result saved earlier in this session (possibly by another agent): L lines, S (first S' saved, R discarded, when capped), at P; read the file if that result is not in this conversation]`. No preview is included.
   - **Paths.** `P` is the absolute path, unabbreviated, so both the tools and the shell can use it. When the path contains characters that need shell quoting, the bash advice adds a POSIX single-quoted copy. Don't abbreviate to `~`, because quoting would block tilde expansion.
   - **Trailer and parsing.** The trailer, if any, follows on its own line. S is a human size (`"1.2 MB"`). The footer ends with `]`, never `:`, so grep's parser can't mistake it for a path line (`plugins/grep/init.lua:172`).
   - **Previews:**
     - `Head` keeps whole lines from the start.
     - `HeadTail` gives the omission line `[... N lines omitted ...]` its own line and byte allowance first. It then splits the remaining line budget `ceil/floor` between head and tail, and the byte budget the same way, so an odd budget never yields an extra line.
     - A line that alone exceeds its segment's byte budget is cut on a UTF-8 boundary, and `[line cut: first X of Y bytes]` or `[line cut: last X of Y bytes]` is appended within that budget. A head-cut line keeps its first bytes, and a tail-cut line keeps its last bytes. In both cases the cut marker goes at the end of the line, so `check_markers` (which matches line endings) catches either one if pasted. An AC.4.3 matrix row asserts the exact rendering of each.
     - The cut marker is one of the shared patterns from Phase 2.1, so `check_markers` rejects pasting a cut line.
   - Footer formats are consts or format functions shared with the tests.
4. **Wiring the store into contexts.**
   - `ToolContext` gains `pub offload: Option<Arc<OffloadStore>>`, and `AgentParams` gains the same field.
   - Ownership: the TUI and interactive headless build `AgentParams` once per turn. Build the store once per session, not once per turn or actor. Each TUI runtime retains the Arc in `AgentHandles` and passes clones into `TuiActorBackend`; same-ID runtime replacements reuse that Arc while outgoing root, child and MCP operations retire. A different session ID gets a new store. Interactive headless owns its store outside the turn loop. Headless and print receive the state dir through `state_dir: Option<PathBuf>` on `HeadlessParams` / `InteractiveParams`, because print mode generates its session id inside `spawn_initialized`. SDK mode persists its sessions (`SessionLogCheckpoint`, `src/sdk_mode.rs:809`), so it keeps its dir like the TUI and ACP do.
   - Top-level construction sites build one `OffloadStore` over a `DiskBackend` per session from `offload_dir_for(state_dir, session)` and pass it into `AgentParams`.
     - `pub fn offload_dir_for(state_dir: &Path, session: Option<&SessionRef>) -> Option<PathBuf>` returns `state_dir/sessions/offload/<session.id()>`, or `None`.
     - Use the frontend's supplied `StateDir` where the session is set up. Do not independently resolve `paths::state_dir()` inside the actor constructor or `tool_context`. Persistence, session deletion and offload files use the same root.
   - `Agent::tool_context` (`agent/run.rs:704`) copies `offload` into the `ToolContext`.
   - Child agents: extract the `build_params` closure in `maki-lua/src/api/agent.rs:1200-1221` into a named `fn child_agent_params(agent_ctx: &AgentContext, agent_id, provider, model, audience, ..) -> AgentParams`, which production calls. It sets `offload: agent_ctx.offload.clone()`, so children share the parent's store, lock and quota.
   - `interpreter_ctx` (`tools/mod.rs:731`) sets `None`.
   - Update every `ToolContext { .. }` literal: `tool_dispatch.rs:812`, `:983` and `:3564`, plus `ctx.rs:69` `to_tool_context`, which clones.
   - `tools::test_support` gains `set_offload(&mut ToolContext, Arc<OffloadStore>)`.
5. **Print mode.** In `maki-agent/src/headless.rs`, print mode never saves its session (`headless.rs:128-216`).
   - When the run ends, print mode calls `store.close_and_remove()` on the shared store. It is best-effort and logs `warn!` on any error other than NotFound. Closing under the lock (Phase 4.2) means an in-flight child `put` finishes first, and a late one can't recreate the directory.
   - Interactive headless and ACP keep their dirs, since their sessions persist.
6. **MCP results.**
   - **Test infrastructure first.** `mcp::test_support::stub_session` installs a `StubTransport` that fails every call with `UnknownTool` (`maki-agent/src/mcp/mod.rs:1596-1609`), so no test can reach `execute_mcp_tool`'s `Ok` branch today. Add `stub_session_with_result(tools, text)` to `mcp::test_support`: a stub transport whose `tools/call` returns a text content result, published through the same path as `stub_session`. Give it its own test, `stubbed_tool_call_returns_text`, showing that dispatch returns that text unchanged.
   - **Then** in `agent/tool_dispatch.rs`:
     - When `ctx.offload` is `Some`, pass the `Ok(text)` of `execute_mcp_tool` (`:933`) through `limit_output` with `PreviewShape::Head`, inside `smol::unblock`. Clone the `Arc` into the `'static` closure. `limits` come from `ctx.config`.
     - When it is `None`, the text passes through unchanged (today's behaviour).
     - MCP and Lua tools use the same store instance, so an in-memory test host's MCP output can be read back by its Lua `read`.
     - MCP results are often JSON, so they get a head preview.
   - `run_local_tool` is untouched (non-goal).

#### Phase 5: Lua API and plugin migration

1. **`ctx:limit_output`.** Add `ctx:limit_output(body, opts)` in `maki-lua/src/api/util/ctx.rs`. `opts` is `{ trailer = string?, preview = "head" | "head_tail", label = string?, max_lines = int?, max_bytes = int? }`.
   - It is a synchronous `add_method` and returns `(string, nil)`, following the pair convention in `maki-lua/src/api/AGENTS.md`. It must be synchronous because it is called from job `on_exit` callbacks and `on_cancel` hooks, which cannot yield (`api/fn.rs:615`, `api/async.rs:134`).
   - It is available in Handler caps only. Other caps get `cap_err_pair("limit_output")`.
   - `max_lines` and `max_bytes` default to the agent config. Plugins pass `output_limits.resolve(opts, ctx)` so the per-tool overrides keep working, including zero: a zero limit returns metadata only, under the metadata exception, without throwing from job exit or cancellation callbacks. `max_line_bytes` comes from the config.
   - The store is the handler `ToolContext`'s `offload`. It needs no `FsBackend` change, since the host chose the store when it built the agent.
   - Take `body` as `mlua::String` to avoid a second copy of large output. Invalid UTF-8 is converted lossily.
   - Add a line for `ctx:limit_output` to the `register_tool` doc comment (`maki-lua/src/api/tool.rs:748`), so it appears in the generated lua-api docs.
2. **Migrating the plugins.** Replace each `maki.text.truncate_file` call except read's.
   - **bash `finish`** (`plugins/bash/init.lua:510-541`):
     - `body = table.concat(output_parts)`. For a non-zero exit, `trailer = "Exit code: N"`; for exit 0 there is no trailer. Use `preview = "head_tail"`.
     - Empty-output handling stays as it is (`"Exit code: N"` / `"Exit code: 0"`).
     - The result still ends in `\nExit code: N`, so the restore regex at `:409-433` keeps working.
   - **bash `on_cancel`** (`:577`): `partial.cut(view, ctx:limit_output(body, { preview = "head_tail" }), reason, timeout_secs)`. The `partial.cut` marker stays last; its length is outside the limit, as today.
   - **code_execution** (`:287`, `:356`): `head_tail`, no trailer. Keep its `"\n"` join.
   - **webfetch** (`:155`), **websearch** (`:128`) and **glob** (`:81`): `head`.
   - **grep** (`:291`): `head`, with `label = "search results"`. The footer lands in `parse_llm_output`'s trailer and renders dim. Live and restored views stay equal because grep parses its own limited `llm_output` (`:293-295`).
     - grep's `restore` currently returns nil when there are no entries (`:244-248`). Change it to also build the view when any trailer line holds the offload footer or the "identical" pointer, recognized by `maki.text.is_offload_notice`. A cut path-header preview before the footer and a trailer-only pointer both restore the same as they render live. `NO_MATCHES` and `error: ...` outputs keep returning nil, as today.
   - Update bash's description, "Output truncated beyond 2000 lines or 50KB." (`:266`), to say that the output is saved to a file whose path is given.
3. **Third-party plugins.** `maki.text.truncate_file` stays for them, unchanged.

#### Phase 6: docs

- In `site/docs/content/token-economy/_index.md` (`:45`, "Truncation everywhere"), document:
  - `agent.max_line_bytes`, the `[line truncated, +N bytes]` marker, and why `write`/`edit_lines` refuse to alter long lines;
  - offloading: where files live, deduplication, caps, the MCP behaviour change, retention (deleted with the session; print mode deletes at exit), and that `--fork-session` histories point into the source session's store.
- Regenerate the configuration, lua-api and tools docs with `just gen-docs`.
- Update the maki-storage line in `AGENTS.md` to mention the offload store.

### Acceptance Criteria

Guard (phases 1-3):
- **AC.1.1** `agent.max_line_bytes` exists with default 1000 and min 80, and read and grep cut lines at it. Setting `plugins.read.max_line_bytes` or `plugins.grep.max_line_bytes` still loads the plugin and has no effect on the cut.
- **AC.1.2** A cut line ends with `[line truncated, +N bytes]` where N equals the hidden byte count, and the whole result stays within `max_bytes` for ASCII and multibyte input.
- **AC.1.2b** MCP permission scope strings for long inputs are byte-identical to today's.
- **AC.1.3** When read's byte cap cuts lines, the marker's remaining count equals the true number of lines after the last one shown.
- **AC.1.4** The PR description includes `git grep -n "\[line truncated"` output in which nothing under `.agents/` appears, and the two lines in `.agents/makima-plans/50-command-registry-acp-unification.md` read as described in Phase 1.8. This is a one-time audit check, verified by the reviewer against the PR.
- **AC.1.5** A file modified on disk while `read` is blocked between its stat and its read leaves the tracker with the older mtime, so a following `edit` is rejected as stale.
- **AC.2.1** After reading a file whose line 2 exceeds the cap, a `write` that reproduces the file from the read output fails with the `LONG_LINE_CHANGED` error and leaves the file byte-identical.
- **AC.2.2** An `edit_lines` replacing a range that includes the long line with non-empty text lacking that line fails the same way. So do a `write` or `edit_lines` that turns two identical long lines into one, and one that replaces either of them with its marker-stripped prefix. An `edit_lines` with empty `new_string` deleting it succeeds. `insert_lines` next to it, without a marker, succeeds.
- **AC.2.3** These succeed:
  - a `write` that keeps every long line unchanged (even moved);
  - a `write` to a new file without markers;
  - writes and edits to other lines of a file that already contains a line ending in a marker (new or legacy format), provided that line is kept unchanged;
  - a `write` over an existing non-UTF-8 file.
- **AC.2.3b** A `write` whose target can't be stat'ed or read for any reason other than absence fails with that error, and the target is unchanged.
- **AC.2.4** These are rejected with `TRUNCATED_MARKER_ADDED`, leaving the target unchanged or absent:
  - writing a truncated read's content to a new file;
  - an `edit` or `multiedit` whose `new_string` adds a marked line;
  - an `insert_lines` whose `new_string` adds a marked line;
  - a new file containing a line ending in the legacy `[line truncated]` marker, as pasted from a resumed history;
  - duplicating an existing marker line, or changing it into a different line that still ends in a marker. Removing the marker from such a line is allowed.
  - deleting the long line with `edit_lines` and then inserting its marked copy.
- **AC.2.5** The guard holds with `agent.stale_read_check = false`, and with a ctx that never read the file. The latter is the resume, subagent and grep-only case.
- **AC.3.1** These no longer match through any fuzzy replacer, and return `NO_MATCH` with the file unchanged:
  - an `edit` whose `old_string` is a 3+ line block with the truncated long line (prefix plus marker) in the middle;
  - an `edit` whose fuzzy candidate holds more copies of a long line than `old_string` does.
- **AC.3.2** Existing fuzzy behaviour for short lines is unchanged: all current `plugins/edit/tests/spec.lua` cases pass, plus a new short-line case run with the cap set.
- **AC.3.3** On the `escape_normalized` path, a truncated long line is rejected, while a legitimate match on a long line containing backslash escapes still succeeds.
- **AC.3.4** An exact `edit` whose `old_string` ends inside a long line succeeds, and its tool output contains the mid-line note with the correct line number and remaining byte count. An edit ending at a line end produces no note.

Offload (phases 4-6):
- **AC.4.1** With a store, output over the limits produces a file named `<sha256 prefix>.txt` of the trailing-newline-stripped body, containing exactly that body. The returned text holds the preview and the uncapped footer with the absolute path, and the trailer comes last.
- **AC.4.2** Calling `limit_output` twice with the same body (also when only trailing newlines differ) returns the pointer the second time, with no preview, and the store holds one file. If the stored file was edited in between, the second call stores `<h>-2.txt` and returns a fresh footer, and the edited file is untouched.
- **AC.4.3** The whole returned text stays within `max_lines` and `max_bytes`, except that the footer and trailer are kept intact when they alone exceed the limits. This is checked over a `test_case` matrix:
  - `Head` and `HeadTail`;
  - odd, even and tiny budgets;
  - long paths and trailers;
  - a single oversized line at the start and at the end, cut on a UTF-8 boundary with the cut marker inside the budget.
- **AC.4.4** With no store, or a failing `put` (IO, quota or closed), the result is the head of `truncate_file`-style output plus the marker line and the trailer. When `put` failed, the "not saved" reason is included. This stays within the total budget except under the metadata exception, which is checked with tiny budgets in every branch.
- **AC.4.5** A body over 8 MiB is stored as the first 8 MiB plus the cap note. Both the footer and a later pointer state the original size, the saved size and the discarded remainder.
- **AC.4.6** Quota and concurrency:
  - Concurrent `put`s of distinct bodies never push stored bytes past the session quota.
  - A duplicate `put` at full quota still returns the pointer.
  - Quota accounting uses stored (capped) sizes, and reflects files grown, shrunk or deleted since the last `put`.
  - An identical result is found even when it sits in a higher slot than a deleted lower one.
- **AC.4.7** `Session::delete_from` removes `sessions/offload/<id>/`.
- **AC.4.8** `DiskBackend` creates files with mode 0600 inside a 0700 directory (unix).
- **AC.4.9** `offload_dir_for` maps a session to `<state>/sessions/offload/<id>` and no session to `None`. A child agent built through `build_params` shares the parent's store instance.
- **AC.4.10** An MCP tool result over the limits is offloaded when the ctx has a store, and passes through unchanged when it doesn't. The stub transport this needs is itself tested. In an in-memory host, a file offloaded through the shared store can be opened by the Lua `read` tool.
- **AC.4.11** Print-mode cleanup removes the run's offload dir. A `put` in flight during close finishes before the removal. A `put` after close, for example from a late or cancelled child, fails with `closed` and leaves no directory behind.
- **AC.4.12** When a stored line exceeds `max_line_bytes`, the footer gives the bash-based advice. When the path needs shell quoting, the advice includes a single-quoted copy.
- **AC.5.1** Running real bash through the plugin host with output over the limits and a non-zero exit gives `llm_output` with the preview, the footer, then `Exit code: N`. bash's restore still shows the exit code. The file exists in the in-memory store and nothing touches the disk.
- **AC.5.2** A cancelled bash run with oversized output ends with the `partial.cut` marker after the footer.
- **AC.5.3** grep output over the limits shows a head preview plus a footer labelled "search results", and its live and restored views are equal. The same holds for the pointer. "No matches" still restores as before.
- **AC.5.4** `ctx:limit_output` is unavailable outside handler caps. With no store it falls back to truncation.
- **AC.5.5** Loop check: running `cat` on an unedited offload file through real bash returns the pointer, and the store still holds exactly one file.
- **AC.5.6** Zero per-plugin output-line or output-byte limits return metadata-only output without throwing. A synchronous migrated tool, bash exit and bash cancellation preserve their normal result/status handling for both overrides.
- **AC.3.5** Cap-enabled global edits calculate the first mid-line note in O(content bytes + matches), including many matches on short lines and repeated matches in one line.
- **AC.4.13** Same-ID TUI replacement reuses the exact store Arc for outgoing and incoming actors. Concurrent additions across replacement remain serialized and cannot exceed the session quota. Different-ID replacement uses a distinct store and directory.
- **AC.4.14** TUI offload files use the supplied StateDir. Session deletion under a non-default root removes the artifact directory.
- **AC.5.3b** grep live and restored views are equal when a cut path-header preview precedes the offload footer and no entries are parsed.
- **AC.6.1** `just gen-docs-check` passes.

### Test Strategy

Test names in the original phase table are planned names; the implementation may use equivalent matrix/helper names. The implementation-review rows identify the actual added regressions. The earlier MCP permission-scope case is covered by `truncate_scope_cases`, and the bash in-memory-store test uses a RealFs host with a registered coordinator while the offload backend remains in memory.

While iterating on a phase, run scoped checks: `cargo check -p <crate> --tests`, plus `cargo nextest run -p <crate>` for the crates touched. At the two milestones, after phase 3 and after phase 6, run the full `just check`, `just lint`, `just test` and `just gen-docs-check`. Every test that runs real bash sets `ctx.config.rtk = false` (as `plugin_host.rs:4278` does) so results don't depend on rtk being installed. Tests that use `insert_lines` boot with `plugins.edit.insert_lines = true` through `builtins_host_with`, since it is opt-in. The Lua specs run through `maki-lua/tests/spec.rs`. Handler-level behaviour runs through the Rust integration tests in `maki-lua/tests/plugin_host.rs`, which use `builtins_host()`, `exec_with_ctx` and `stub_ctx_with_session`, with tempdir files written by the test. All errors and notes are asserted against shared constants or format functions, never retyped strings.

| AC | Test (layer) |
|---|---|
| AC.1.1 | `max_line_bytes_below_min` / `agent_line_bytes_overlay` test_case rows in `maki-config/src/lib.rs` (unit). In `maki-lua/tests/plugin_host.rs` (integration): `read_cuts_lines_at_agent_max_line_bytes`; `grep_cuts_lines_at_agent_max_line_bytes` (RealFs host plus tempdir, since InMemoryFs grep skips long lines); and `deprecated_plugin_max_line_bytes_loads_and_is_ignored`, a `test_case` over read and grep using `builtins_host_with` with the plugin opts set |
| AC.1.2 | `truncate_line_cases` extended with `ascii_reports_hidden_bytes` and `multibyte_reports_hidden_bytes` cases (`maki-agent/src/tools/mod.rs`, unit) |
| AC.1.2b | `mcp_perm_scope_format_unchanged` (`tool_dispatch.rs` tests, unit) |
| AC.1.3 | `truncate_file_counts_byte_cut_lines_as_remaining` (`tools/mod.rs`, unit) |
| AC.1.4 | One-time audit recorded in the PR description (see Phase 1.8) |
| AC.1.5 | `real_read_barrier_blocks_until_released` (infrastructure test); `read_racing_modification_leaves_stale_mtime` in `maki-lua/src/write_lock_regression.rs` (integration: block the read, rewrite the file and `File::set_modified` it to a clearly later time (as `file_tracker.rs:78` does), release, then dispatch `edit` and assert the stale error) |
| AC.2.1 | `write_after_truncated_read_is_rejected` (`plugin_host.rs`, integration: read, then write the reconstructed content, assert the error constant and that the file is unchanged) |
| AC.2.2 | `check_write_duplicate_long_line_collapse_rejected`, `check_write_stripped_prefix_of_duplicate_rejected`, `check_replace_lines_duplicate_collapse_rejected` (unit); `edit_lines_altering_long_line_is_rejected`, `edit_lines_deleting_long_line_succeeds`, `insert_lines_next_to_long_line_succeeds` (integration); `check_replace_lines_*` cases in `plugins/lib/tests/spec.lua` (unit) |
| AC.2.3 | `check_write_*` and `check_markers_*` cases (moved long line, new file, pre-existing marker line kept, CRLF, multibyte) in `plugins/lib/tests/spec.lua` (unit); `write_preserving_long_lines_succeeds`, `write_over_non_utf8_file_succeeds` (integration); `write_metadata_error_leaves_target_unchanged` and `write_read_error_leaves_target_unchanged`, through a wrapping test `FsBackend` (as in `maki-lua/src/write_lock_regression.rs`) that fails `stat` or `read_bytes` with a non-NotFound IO error (integration) |
| AC.2.4 | `write_new_file_with_preview_cut_marker_is_rejected` (phase 5, ctx with a store set, since the no-store fallback never emits a cut marker; oversized single-line bash output, then write of the cut line); `write_new_file_with_truncated_marker_is_rejected`, `edit_new_string_with_truncated_marker_is_rejected`, `insert_lines_with_truncated_marker_is_rejected`, `edit_lines_delete_then_insert_truncated_copy_is_rejected`, `write_new_file_with_legacy_marker_is_rejected` (integration); `check_markers_duplicate_rejected`, `check_markers_changed_marker_line_rejected`, `check_markers_removing_marker_allowed`, `check_markers_error_names_category` (unit) |
| AC.2.5 | `guard_holds_through_dispatcher_with_stale_check_off`, through the real dispatcher (`dispatch` / `shared_ctx` helpers in `maki-lua/src/write_lock_regression.rs`) with `stale_read_check = false` and a fresh tracker; `guard_holds_with_fresh_ctx` in `plugin_host.rs` for the subagent/resume case (integration) |
| AC.3.1 | `block_anchor_rejects_truncated_long_middle_line`, `context_aware_rejects_truncated_long_middle_line`, `block_anchor_rejects_duplicate_long_line_collapse` (file `BEGIN / L / L / END`, old_string `BEGIN / L / END`) and `escape_normalized_rejects_duplicate_long_line_collapse` in `plugins/edit/tests/spec.lua` (unit) |
| AC.3.2 | Existing `plugins/edit/tests/spec.lua` suite plus `short_line_fuzzy_unchanged_with_cap` (unit) |
| AC.3.3 | `escape_normalized_rejects_truncated_long_line`, `escape_normalized_keeps_long_line_with_backslashes` (unit) |
| AC.3.4 | `exact_match_ending_mid_long_line_returns_note`, `match_ending_at_line_end_has_no_note` (unit); `edit_output_includes_mid_line_note`, `multiedit_output_includes_mid_line_note` (integration) |
| AC.4.1-4.5, 4.12 | `offload.rs` unit tests over a small in-crate map `OffloadBackend` (maki-agent cannot depend on maki-lua; only the raw operations are faked, the store logic is real), using `test_case`: `offloads_over_limit_with_footer_and_trailer`, `identical_body_returns_pointer_only`, `trailing_newline_variants_dedupe`, `edited_artifact_gets_new_slot`, `dedup_finds_higher_slot_after_lower_deleted` (also at full quota), `total_output_within_limits` (matrix: shape x budget x path/trailer length x oversized first/last line), `metadata_kept_when_over_limits`, `no_store_falls_back_to_truncate_file`, `put_error_falls_back_with_reason`, `fallback_tiny_budget_keeps_metadata` (test_case over the no-store, IO-error, quota-error and closed branches), `segment_omitted_when_cut_marker_cannot_fit`, `capped_body_footer_and_pointer_disclose_sizes`, `long_line_body_gets_bash_advice`, `shell_quoted_path_in_advice` |
| AC.4.6 | `concurrent_puts_respect_quota` (threads with a `Barrier` started together, assert total stored bytes ≤ quota; deterministic in outcome), `duplicate_at_full_quota_returns_pointer`, `quota_counts_stored_capped_size`, `quota_reflects_grow_shrink_delete` (grow, shrink and delete files on disk between `put`s on the same live store instance) (`offload.rs`, unit, `DiskBackend` on a tempdir) |
| AC.4.7 | `delete_removes_offload_dir` in `maki-storage/src/sessions.rs`, modelled on `delete_removes_archive_dir` (unit) |
| AC.4.8 | `disk_backend_creates_private_files` (unit, unix-gated, tempdir) |
| AC.4.9 | `offload_dir_for` test_case rows `some_session_maps_to_sessions_offload_id` and `no_session_is_none` (unit); `child_agent_shares_parent_offload_store`: call the extracted production `child_agent_params` and assert `Arc::ptr_eq` with the parent's store (unit). Not the hand-built fixture at `agent.rs:2207`, which would pass even if production dropped the field. |
| AC.4.10 | `stubbed_tool_call_returns_text` (infrastructure test, `mcp/mod.rs` test_support); `mcp_result_over_limit_is_offloaded` and `mcp_result_without_store_is_unchanged` in `tool_dispatch.rs` tests (integration); `mcp_offload_readable_by_lua_read`, in `maki-lua/src/write_lock_regression.rs` tests, end to end. It dispatches an MCP tool through the real dispatcher (the `dispatch` helper) with a ctx carrying `stub_session_with_result` (`mcp::test_support` is public and always compiled) and an `OffloadStore` over the host's `InMemoryOffloadBackend`. It parses the path from the footer, execs the Lua `read` tool on it, and asserts the content (integration) |
| AC.4.11 | `close_and_remove_removes_dir`, `close_waits_for_in_flight_put` (backend whose `create_new` blocks on a gate; close from another thread; assert ordering), `put_after_close_fails_without_recreating_dir`, `cancelled_child_put_after_close_leaves_no_dir` (unit); `print_run_removes_offload_dir` (integration, stub provider per `src/print.rs:661-690`, tempdir state dir) |
| AC.5.1 | `bash_large_output_offloads_with_exit_code_last` in `maki-lua/tests/in_memory_host.rs` (real bash, `OffloadStore` over `InMemoryOffloadBackend`, assert the stored file and the restore view) |
| AC.5.2 | `cancelled_bash_large_output_keeps_partial_marker_last`, modelled on `cancelled_bash_keeps_streamed_output_as_partial` (`plugin_host.rs:4264`) |
| AC.5.3 | `grep_offload_footer_survives_restore` (asserts the clipping note), `grep_identical_pointer_live_equals_restored`, `grep_no_matches_restore_unchanged` in `maki-lua/tests/real_plugins_restore.rs`; update `grep_restore_keeps_truncation_marker` for the no-store fallback |
| AC.5.4 | `limit_output_is_handler_only`, modelled on `session_id_reaches_handler_and_start_but_not_restore` (`ctx.rs` tests); `limit_output_without_store_truncates` (unit) |
| AC.5.5 | `cat_offload_file_returns_pointer` in `plugin_host.rs` (integration, `RealFs` host with a `DiskBackend` store on a tempdir, since real bash's `cat` needs the file on disk: one oversized bash run, then `cat` of the reported path, then assert the pointer and one file in the dir) |
| AC.3.5 | `many_short_line_matches_have_no_note`, `multiple_matches_on_one_long_line_report_first_note`, `short_line_matches_before_long_line_report_correct_note` in `plugins/edit/tests/spec.lua`; embedded Lua scaling probe, recorded below and removed after measurement |
| AC.4.13 | `replacement_offload_store_identity` (same/different session IDs), `same_id_replacement_serializes_in_flight_offload_put` in `maki-ui/src/event_loop.rs`; production preparation and replacement with a gated near-quota backend |
| AC.4.14 | `offload_uses_supplied_storage_root_and_session_deletion` in `maki-ui/src/event_loop.rs` |
| AC.5.3b | `grep_cut_path_header_live_equals_restored` in `maki-lua/tests/real_plugins_restore.rs`; no-match test also covers ordinary errors |
| AC.5.6 | `limit_output_zero_keeps_metadata_and_success_pair` API matrix with/without store; `glob_zero_output_override_returns_metadata_only`, `bash_zero_output_override_finishes_on_exit`, `bash_zero_output_override_finishes_on_cancel` in `maki-lua/tests/plugin_host.rs` |
| AC.6.1 | `just gen-docs-check` |


### Review Strategy

Planning review: `plan-reviewer`, looping until no critical or high findings remain.

Implementation review uses `nat-code-reviewer` for the guard and integration scopes, then another read-only pass over the approved review fixes. Each pass reviews the changes against this plan and `AGENTS.md`'s code guidelines: no trivial comments, consts at the top, imports, `test_case`, and no flaky sleeps. Review guard bypasses, offload loops, store ownership through replacement, and callback completion. Every finding is fixed or rebutted, and critical findings trigger another pass. Do not override the configured subagent model without operator direction.

### Documentation Strategy

- Hand-written: `site/docs/content/token-economy/_index.md`, in the tone `AGENTS.md` sets for docs.
- Generated, via `just gen-docs`:
  - configuration: the `agent.max_line_bytes` row and the deprecated plugin options;
  - lua-api: `maki.text.truncate_line`, `register_tool`'s `ctx:limit_output` line, and `output_limits.lua`;
  - tools: the read, bash and edit descriptions.
- `AGENTS.md`: the maki-storage architecture line mentions the offload store.

### Risks, Blockers, and Required Decisions

Decided:
- The guard is stateless, not a tracker.
- One agent-wide line cap, default 1000.
- Everything ships as one PR closing #75 (operator decision).
- No separate GitHub issue.

Risks:
- **False positive:** a model rewriting, with `write`, a file it authored earlier that has lines over 1000 bytes (e.g. long Markdown paragraphs) is rejected and must use `edit`. This is accepted. The error says what to do.
- **grep match lines** grow from 500 to 1000 bytes, which costs tokens on minified matches. Accepted with the cap decision.
- **Deduplication** returns only a pointer when the same command produces the same output twice (e.g. a repeated failing test). If compaction dropped the earlier preview, the model has to read the file. The same applies when a subagent or a parallel sibling call created the file first. To cover both, the pointer text reads `[output identical to a result saved earlier in this session (possibly by another agent): ...; read the file if that result is not in this conversation]`. Accepted.
- **Sync writes** of up to 8 MiB (plus a byte comparison when the content was saved before) happen on the Lua thread at tool finish, under the store lock. This is acceptable; the alternative of restructuring bash's callbacks is larger.
- **Directory scan per new file:** `put` lists the slots and sums the directory under the lock for every new file. The byte quota doesn't bound the file count, so many small outputs could mean thousands of files. Measure the scan cost during implementation with a store of a few thousand small files. If it is significant, maintain the size total incrementally and reconcile it with a scan only when a `put` would exceed the quota. That keeps decisions correct after edits without rescanning on every write.
- **Non-UTF-8 targets and grep:** grep decodes lossily, so a Latin-1 file's long lines can reach the model cut. `write` skips `check_write` for non-UTF-8 targets, but such a write already loses bytes through the lossy decode, so the guard would add little.
- **Two-step loss inside one multiedit:** an exact edit can delete a long line's visible prefix (with a note), leaving a remainder under the cap that a later fuzzy edit in the same call can replace. This is the residual risk of fuzzy matching on short lines.
- **write reads its target first:** every overwrite now reads the existing file for the guard, which costs time and memory on very large files.
- **Whitespace drift on long lines:** fuzzy candidates compare long lines with `trim` only, so an `old_string` holding a whole long line with internal whitespace drift no longer matches. The model rarely holds a whole long line, so this is accepted.
- **Implementation note:** the `escape_normalized` path is left unguarded. Its full-block branch requires equality after unescaping, while its substring branch replaces only the exact unescaped substring and preserves the suffix. Neither branch can forgive an unseen middle line or suffix.
- **Marker-ending lines:** a legitimate new line ending exactly in marker text can't be added through the edit tools (Phase 2.1 scope). Accepted.
- **Residual guard bypass:** a model that strips the marker and writes the visible prefix into a new location (a new file, or after deleting the original) is not caught, because no original line is altered and no marker is present. This needs a deliberate workaround of two error messages. It is accepted, since closing it would require the stateful provenance tracking rejected in #85.
- **Shell writes:** writes through bash (heredoc, `sed -i`, python) are not guarded. The guard covers `write`, `edit`, `multiedit`, `edit_lines` and `insert_lines`.
- **Catting a capped file:** a file over 8 MiB is stored with a cap note, so `cat` of it hashes differently and saves a second capped copy. Each round is bounded by the per-file cap and stops at the session quota.
- **Print-mode cleanup on SIGKILL:** print mode removes its store through a drop guard, which covers completion, a dropped task and a panic, but not a killed process. Such a directory stays until removed by hand.
- **rtk:** with `agent.rtk` on, a `cat` of an offload file may be rewritten and produce a new, smaller offload file instead of the pointer. This is bounded, but it is not deduplicated.
- **Behaviour change for MCP:** with an offload dir, large MCP results now reach the model as a preview plus path instead of in full. This is intended, and the docs describe it. Without an offload dir they are unchanged.
- **Retention:** offload dirs live until their session is deleted, and there is no global retention or GC across sessions that are never deleted. Print mode deletes its dir at exit, so a final answer that cites an offload path points at a deleted file. Both points go into the token-economy docs.
- **Failed reads:** with the reordering in Phase 1.6, a failed read still records an mtime, which only relaxes the staleness check for that file.
- **Offload of single-line content** (minified JSON) can only be inspected through bash. Where bash is denied, only the preview is reachable. The footer says so explicitly (AC.4.12).
- **Repairing the plan doc:** the lost text in `.agents/makima-plans/50-...md` can't be recovered, so the repair marks it as lost rather than inventing content.
