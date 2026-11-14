+++
title = "Context"
weight = 31
[extra]
group = "Concepts"
+++

# Context

Everything the model knows about your project passes through one context window, and every token in it costs money and attention. This page covers what Makima puts there, when, and where you should put things so they land well.

## What loads when

```
session start (paid every request)   on demand (paid when used)
──────────────────────────────────   ─────────────────────────────────
system prompt                        file contents   read / index / grep
tool definitions                     skill bodies    skill tool
instruction files (AGENTS.md, ...)   memory notes    memory tool
memory tag names                     subdir rules    first read there
skill names + descriptions           MCP tool defs   tool_search
```

The left column is the fixed overhead of every single request, so Makima keeps it small on purpose: a skill contributes one description line, memories one list of tags, a big MCP server one search tool. The bodies stay on disk until the agent asks.

## Instruction files

At session start Makima walks from the project git root down to the working directory (no `.git` root, only the cwd). In each directory it loads **one** project instruction file, first match wins:

| Order | File |
|------|------|
| 1 | `AGENTS.md` |
| 2 | `CLAUDE.md` |
| 3 | `.github/copilot-instructions.md` |
| 4 | `COPILOT.md` |
| 5 | `.cursorrules` |
| 6 | `.windsurfrules` |
| 7 | `.clinerules` |
| 8 | `CONVENTIONS.md` |
| 9 | `GEMINI.md` |
| 10 | `CODING_AGENT.md` |

After the match it always loads `AGENTS.local.md` from the same directory if present: that one is yours, keep it gitignored. Closer directories win on conflicts. Finally one global `~/.config/makima/AGENTS.md` for preferences that follow you across projects.

```
~/repo/AGENTS.md           loaded (root)
~/repo/AGENTS.local.md     loaded (yours, gitignored)
~/repo/api/CLAUDE.md       loaded when cwd is ~/repo/api, wins over root
~/repo/web/AGENTS.md       not loaded yet...
~/.config/makima/AGENTS.md   loaded (global)
```

That `web/AGENTS.md` is not dead weight. The first time the agent `read`s a file under a subdirectory whose instruction file was never loaded, Makima pulls it in. Monorepo rules live next to the code they govern and cost nothing until someone works there.

Put coding conventions, repo quirks, and off-limits directories in these files. Keep them short; [Four places to put knowledge](#four-places-to-put-knowledge) compares their cost with skills and memory.

## Recorded context

Saved sessions record the instruction text, source order, and resolved system-prompt contributions used by the main agent. Resume uses those recorded values. Editing, removing, or reordering an already recorded source does not silently change the next request. Makima reports `instructions changed on disk, run /reload-context` when it observes such a change. Repeated observations of the same change do not repeat the notice.

Newly encountered instruction sources are still added when first loaded, including subdirectory rules. Recording context does not load every instruction file in the project in advance.

Run `/reload-context` on the main agent to adopt the current instruction sources and available prompt contributions. It records the new context for subsequent requests. If prompt contributions are unavailable during reload, their recorded values remain in use. The command is unavailable on subagents; descendants use the recorded root context, with each child's own prompt fixed at spawn.

Context reload is part of recorded session state. A conversation rewind can restore an earlier recorded context without changing the instruction files on disk. See [Persistence and resume](/docs/agent-work/#persistence-and-resume) and [Rewind and branches](/docs/agent-work/#rewind-and-branches).

## Four places to put knowledge

All four end up in context, but at different times and prices:

| | Loaded | Costs | Good for |
|---|--------|-------|----------|
| `AGENTS.md` | every session | every request | short rules: conventions, build commands, no-go areas |
| [Skills](/docs/skills/) | when the agent picks one | a description line until then | long playbooks: release process, plugin authoring |
| Memory | when the agent recalls a tag | tag names until then | gotchas the agent learns while working |
| [Commands](/docs/commands/) | when you type `/name` | nothing until invoked | prompts you keep retyping |

Rule of thumb: when `AGENTS.md` grows past a screen, the new material probably wants to be a skill. `AGENTS.md` is a tax on every request; a skill is a tax only on the sessions that need it.

## When the window fills

Long sessions eventually approach the model's context limit. Makima reserves a slice of the window (`agent.compaction_buffer`, default 20%) and before running out it summarizes the older turns and continues from the summary. `/compact` triggers it early, `/usage` shows where the tokens went, and `agent.compaction_instructions` steers what the summary keeps.

Compaction replaces the older turns in the session's on-disk log with the summary. The dropped turns are not lost: before the rewrite, Makima parks the previous log at `sessions/archive/<session-id>/<n>.jsonl` in the [state directory](/docs/configuration/#directory-layout). It keeps the newest three per session, and at most 32 MB of them. The names count up, so the highest number is the newest.

Archives can be inspected with `jq` or an editor. Replacing the live session log by hand bypasses the checks that keep agent state, branches, and file-restore records consistent. Use [rewind](/docs/agent-work/#rewind-and-branches) for supported restoration points instead; an archive is not a file-restore backup.

`MAKIMA_DISABLE_AUTOCOMPACT=1` turns off the automatic compaction. A manual `/compact` still compacts.

Related: [Token Economy](/docs/token-economy/) for why all this frugality exists, [Configuration](/docs/configuration/) for the knobs.
