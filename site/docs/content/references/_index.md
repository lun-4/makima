+++
title = "References"
weight = 12
[extra]
group = "Reference"
+++

# `@` References

Type `@` in the input box and Makima opens a completion popup. File completions are built in, but skills, subagents, and models come from plugins via the completion-source API. The built-in `skill`, `task`, and `model` plugins provide the defaults, and you are free to implement your own completion sources and expanders (see the [Lua API](/docs/lua-api/#maki-api-register_completion_source)).

Each reference has a long prefix, a one-letter short form, and a fixed meaning at submit time. At submit, a recognized `@prefix:value` token is replaced in place with an intent token like `<skill:name>`, `<subagent:type>`, or `<model:spec>`; the agent acts on it per its instructions. Unknown prefixes pass through verbatim.

## Syntax

| Reference | Long form | Short form | What it does at submit |
|---|---|---|---|
| File | `@path/to/file` | - | Stays in the message as plain text. The agent reads the file lazily with its tools when it needs it. |
| Skill | `@skill:name` | `@s:name` | Becomes `<skill:name>` in place. The agent loads that skill with the `skill` tool before answering. |
| Subagent | `@subagent:type` | `@a:type` | Becomes `<subagent:type>` in place. The agent delegates the request to a `task` subagent of the given type. |
| Model | `@model:spec` | `@m:spec` | Becomes `<model:spec>` in place. Paired with a subagent, sets that subagent's model. |

Prefixes are case-insensitive, so `@SKILL:pdf` and `@s:pdf` are the same. A bare `@skill:` with nothing after it is not a reference yet. It keeps the popup open while the user types.

An unquoted reference ends at whitespace. Sentence punctuation at the right edge is not part of its value. This applies to `, . ! ? ) ] } " '` and the equivalent full-width characters. For example, `use @skill:pdf,` expands to `use <skill:pdf>,`. Punctuation in the middle of a value remains part of the value.

Single or double quotes preserve spaces and trailing punctuation in a value:

```text
@"docs/release notes.md"
@'docs/what next?.md'
@skill:"release review"
```

The opening and closing quote must match. Completion adds quotes when an insertion needs them. Unquoted whitespace closes the popup. An unfinished quoted reference stays open across whitespace until completion inserts the matching closing quote.

File references are the odd one out. They are never expanded or stripped. Makima does not inject file contents at submit time. The agent gets the path as text and decides when to read it. This keeps big files out of context until they are actually needed.

Project-relative file completion uses the project index. A file value that starts with `~`, `/`, or `.` instead reads one directory at a time. For example, `@~/`, `@/var/`, `@./`, and `@../` list the immediate children of those directories. Selecting a directory keeps the popup open at the next level. Selecting a file completes the reference and closes the popup. Missing or unreadable directories produce no candidates and remain editable.

Project-index completion and the file picker use Nucleo for coarse retrieval and the shared matcher for final admission, highlighting, and ranking. Each surface materializes at most 640 coarse results for final matching. Additional coarse results are not ranked and are not guaranteed to appear.

## Subagents

`@subagent:` (short `@a:`) lists the subagent types the `task` plugin's [completion source](/docs/lua-api/#maki-api-register_completion_source) offers:

- `research` - read-only search and summarize.
- `general` - can modify files.
- `plan_reviewer` - read-only plan audit, plan mode only.

The list is filtered by your current mode, which the source reads from the context it is given. In plan mode, `general` is hidden. Outside plan mode, `plan_reviewer` is hidden. You can never pick a type the plugin would reject.

Submitting `@subagent:research review this package` becomes `<subagent:research> review this package`. The agent sees the `<subagent:research>` intent token and delegates to a `task` subagent of that type, relaying its result.

More than one `@subagent:` token each expands in place, so a single message can ask for several subagents at once; the agent launches them as it sees fit (the task tool is built for parallel launches).

## Models

`@model:` (short `@m:`) lists model specs from the available-models slot, the same list the `/model` picker uses. The list comes from the `model` plugin's completion source.

A `@model:spec` next to a subagent reference sets that subagent's model; the `<model:spec>` token sits beside the `<subagent:type>` token and the agent applies it when spawning the subagent. If the requested model is unavailable (the `task` tool's `allow_model` option is off, so its schema has no `model` parameter), the agent asks you how to proceed via the `question` tool if it is loaded, or rejects the request and asks you to clarify. It does not silently pick another model.

A `<model:spec>` reference without a subagent has no effect. Use the `/model` command to switch the session model.

## Skills

`@skill:` (short `@s:`) lists skills from the `skill` plugin's completion source. Submitting `@skill:pdf @skill:csv summarize this` becomes `<skill:pdf> <skill:csv> summarize this`, telling the agent to load `pdf` and `csv` via the `skill` tool before answering. Skills named next to a subagent go into the subagent's task. See [Skills](/docs/skills/) for how skills work.

## What passes through

Not every `@` is a reference. Makima only treats `@` as a reference start when it begins a token, meaning nothing but whitespace comes before it on the line. So `foo@bar`, email addresses, and unknown prefixes like `@nothing:whatever` are left alone and sent to the agent verbatim.

## Custom completion sources

The `@` popup is extensible. A plugin can call `maki.api.register_completion_source` to add its own candidate kind (with its own `kind` color and insertion behavior) and `maki.api.register_expander` to control what its `@prefix:` token becomes at submit. See the [Lua API reference](/docs/lua-api/#maki-api-register_completion_source) for both. A one-line sketch:

```lua
maki.api.register_completion_source("ticket", {
  get_items = function(ctx)
    return { { label = "ticket:1234", kind = "ticket", insertion = "@ticket:1234" } }
  end,
})
maki.api.register_expander("ticket", function(ref)
  return "<ticket:" .. ref.value .. ">", nil
end)
```
