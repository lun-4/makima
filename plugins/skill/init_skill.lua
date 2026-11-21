-- Builtin `init` skill. Hidden from the model's skill list and summoned by the
-- user with @skill:init, so it costs no prompt tokens.
local CONTENT = [=[# Init: generate AGENTS.md

The job is to produce an `AGENTS.md` for the current project that gives coding
agents the general instructions about how the project works:

- what the project is and what it provides
- how to build and test it
- a small map of which top-level folders contain which components

## Step 1 — explore with a subagent

Do **not** explore the codebase yourself. Dispatch a `research` subagent with
`task_spawn` to do the exploration and return a rundown. Keeping the
exploration in the subagent keeps your own context window clean for writing.

Give the subagent a prompt along these lines:

> Explore this codebase (read-only). Report back:
> 1. What the project is, what it does, and what it provides (from README,
>    manifests, docs, and entry points).
> 2. An extremely short description of the architecture.
> 3. The exact commands to build and test the project for development — verify
>    them against package manifests, build files, CI configuration, and existing
>    docs. Include a one-line description per command.
> 4. A list of the primary top-level directories from the repo root and what
>    each contains. For monorepos, list the folders for each project. Do not
>    enumerate source files, nested structures, or subfolders that don't
>    represent separate services.

Read the subagent's report, spot-check anything that looks off, and only then
move to step 2.

## Step 2 — write AGENTS.md

As the main agent, write the final `AGENTS.md` file yourself, using the
subagent's rundown as your source material. Follow this structure exactly:

````markdown
# AGENTS.md

These are guidelines and context for coding agents to follow on this codebase:

## what it is

<high level details on what the project is, what it does, what it provides>

<extremely short description of the architecture>

## how to build & test it

<details on commands to run to build and test the project for development, like this:>

```sh
# <command description>
<command>
```

## initial directories

<a list of primary directories coming from the repo root that detail the key
directories, if it's a monorepo then list the folders for the projects here.
of note is to not actually link source or nested structures that don't
represent separate services, this serves only as a guideline map. listing of
the subfolders or specific components is discouraged>
````
]=]

return {
  name = "init",
  description = "Initialize a project's AGENTS.md: explore the codebase with a research subagent, then write the file yourself.",
  disable_model_invocation = true,
  content = CONTENT,
}
