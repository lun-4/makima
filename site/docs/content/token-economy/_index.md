+++
title = "Token Economy"
weight = 30
[extra]
group = "Concepts"
+++

# Token Economy

Makima's whole design falls out of one fact about agent loops: the conversation is re-sent to the model on every turn.

```
turn 1   [system + prompt]                      ─► model ─► tool call
turn 2   [system + prompt + result 1]           ─► model ─► tool call
turn 3   [system + prompt + result 1 + 2]       ─► model ─► ...
```

A tool result does not cost its tokens once. It costs them again on every turn until the session ends or history is compacted. `cat` a 2000-line file on turn 2 of a 40-turn session and you pay for it 38 more times. Prompt caching softens the price, not the principle: cache reads still cost, and a bloated context also makes models measurably dumber.

So Makima attacks the two multipliers: how much each step adds to context, and how many steps there are.

## Smaller results

**index instead of read.** The `index` tool returns a tree-sitter skeleton of a source file: imports, types, signatures, line numbers. Usually 70-90% smaller than the file itself. The agent indexes first, then reads only the ranges it needs.

```
read main.rs                 index main.rs
────────────                 ─────────────────────────────
1400 lines in context        60 lines of signatures
                             + read offset=812 limit=40
```

**Subagents as garbage collectors.** A `task` subagent gets its own throwaway context. It can grep, read, and hit dead ends as much as it wants; only its final summary returns to your conversation. The mess is collected when it exits. Model tiers make this cheap too: delegate a search to a weak model at a fraction of the cost, keep the strong model for judgment.

```
main context                subagent context (discarded)
────────────                ────────────────────────────
task("find auth") ───────►  glob, grep ×6, read ×9, ...
                  ◄───────  "JWT middleware, auth.rs:120"
one line stays              ~20k tokens never seen
```

**Deferred MCP tools.** An MCP server with 100 tools would ship 100 definitions in every request. Makima loads a single `tool_search` tool instead; the model searches when it actually needs something and only the matches load. See [MCP](/docs/mcp/#tool-search).

**Cut, but not lost.** Tool output is capped (`agent.max_output_bytes`, `agent.max_output_lines`). When bash, grep, glob, webfetch, websearch, `code_execution` or an MCP tool exceeds a cap, its full output is saved to a file. The model gets a preview and a footer with this format:

```
[output truncated: N bytes saved to PATH[, M bytes discarded]; inspect with read or grep; use bash for long lines[; saved search results also contain clipped lines]]
```

The model can inspect the saved file with `read` or `grep`. Each oversized result gets a unique artifact. Artifacts are limited to 8 MiB each, with a 256 MiB quota per session. The quota starts with the directory's existing usage and increases only after successful writes. Editing or deleting artifacts does not change the quota until the session is reopened. Artifacts remain until the session is deleted. A forked session can still refer to artifacts in its original session.

Print mode (`--print`) stops new saves and waits up to five seconds for in-flight saves and cleanup. Saved paths may no longer exist after cleanup. Removal failures are logged. Abrupt process termination can leave artifacts behind.

MCP results go through the same limit only when a session has a place to save them; without one they pass through unchanged. [Output hooks](/docs/hooks/#trimming-output) receive the complete MCP result before limiting or saving. A successful hook replacement is saved instead of the original text. A layer that throws is skipped and its error is logged. Cancellation or timeout during an output hook replaces the output with an error and prevents saving.

**Long lines stay whole on disk.** `read` and `grep` cut any line longer than `agent.max_line_bytes` (default 1000) and end it with `[line truncated, +N bytes]`. That prefix is all the model saw, so writing it back would lose the rest. `write` and `edit_lines` refuse to drop or change such a line, and no edit tool accepts a new line that ends in a recognized truncation marker, including one followed by Unicode whitespace. Existing literal marker lines can remain unchanged or be removed, but cannot gain extra occurrences. To change text inside a long line, the model uses `edit` with an exact `old_string`.

Fuzzy `edit` and `multi_edit` matches require full coverage of long lines. Nonblank long lines tolerate indentation changes. A long whitespace-only line requires a byte-identical line in `old_string` for each occurrence. An empty, shorter, or differently spaced blank line does not cover it. Exact substring edits remain available.

**Interrupted work is not wasted.** Press Esc on a long tool, or let its deadline hit, and whatever it printed so far still reaches the model, tagged as partial: bash keeps its streamed lines, `code_execution` the script output, a `task` subagent its half transcript. Otherwise the next turn starts from nothing and you pay to run it all again.

Lua tool handlers return raw `llm_output` and `output_limits` in a result table or through `ctx:finish`. Output limits apply after the terminal reply leaves Lua. See the [tool API](/docs/lua-api/#maki-api-register_tool).

## Fewer round-trips

Every round-trip re-sends the context, so round-trips are the other half of the bill.

**batch** runs independent tool calls in one turn: one request, N results.

**code_execution** goes further: a Python sandbox where tools are async functions. Chained calls, loops, and filtering happen inside the sandbox; only what the script prints enters context.

```
without                          with code_execution
─────────────────────            ─────────────────────────────
glob        → 300 paths          results = gather(read × 300)
read × 300  → 300 files          filter in python
300 turns, every file            print("3 files call foo_v1")
in context forever               1 turn, 1 line in context
```

**Compaction** resets the multiplier when a session runs long: older turns are summarized and dropped. [Context](/docs/context/#when-the-window-fills) has the details.

## Watching it work

`/usage` shows the token breakdown of the current session, and `--output-format json` in [Headless Mode](/docs/headless/) reports `total_cost_usd` per run. Cheap is a feature you can measure.

When a provider exposes a quota endpoint (Synthetic, OpenAI, Z.AI, DeepSeek), a compact readout like `5h30% w50%` lines up next to the input box so you see remaining quota at a glance, with each lane colored blue to red as it fills. `/usage` still shows the full detail.

Each turn is priced when it happens and that number is stored with the session. Prices move (DeepSeek, for one, doubles every rate during peak UTC hours), so a total re-priced later would be a guess. What you see is what you were billed.
