-- Builtin `skill-creator` skill. Visible by default so "make me a skill" can
-- trigger it; plugins.skill.skill_creator = false turns it off.
local CONTENT = [=[# Creating makima skills

A skill is a Markdown how-to the agent loads only when it needs it. This skill
is about writing one that actually triggers and actually helps.

## What a skill is in makima

- A directory with a `SKILL.md`. The frontmatter gives `name` and
  `description`; the body is the instructions.
- Only `name` and `description` sit in the system prompt. The body loads when
  the skill tool runs, so the description carries the whole triggering burden.
- `disable-model-invocation: true` hides the skill from the model's list but
  keeps it reachable with `@skill:<name>`.
- Paths into the skill's own files are relative to its directory, so
  `references/schema.md` resolves next to `SKILL.md`. Project paths such as
  `docs/meetings/` are not; state the working directory for them.

Discovery order matters when names collide: builtins, then the global
`~/.config/makima/skills/`, the Claude/OpenCode/Agents dirs, then the project
dirs walked up to the `.git` root. Later wins.

## Workflow

### 1. Capture the job

If the conversation already contains the workflow, extract it from the tools
used, the steps that worked, the corrections the user made, and the input and
output formats. Confirm the gaps instead of interviewing from zero.

Ask only what changes what you write:

- What should the agent do that it cannot do well now?
- When should it trigger? Ask for two or three real user phrasings.
- What does good output look like? Ask for one example when the shape matters.
- Project or personal? Model-picked or user-summoned?
- Is the result objectively checkable? If yes, plan one or two tests.

Use the question tool when answers would change the draft. Do not ask for
things you can infer.

### 2. Choose one coherent scope

A skill is a reusable method, not a story about one task. Decide what class of
work it covers and stop there. Too narrow and the agent loads three skills for
one job; too broad and it cannot tell when to load this one.

If the skill has genuinely distinct modes, keep the shared workflow and the
selection rule in `SKILL.md` and put each mode in `references/`.

If the behavior is enforceable by a validator or a script, automate it instead
of writing advice.

### 3. Pick the location

- Shareable with the repo and other agents: `<repo>/.agents/skills/<name>/SKILL.md`
- Project, makima only: `<repo>/.makima/skills/<name>/SKILL.md`
- Personal, every project: `~/.config/makima/skills/<name>/SKILL.md`

Name the directory exactly after the frontmatter `name` for portability.

### 4. Write the frontmatter

```yaml
---
name: release-notes
description: Draft release notes from merged PRs and git history. Use whenever the user asks for a changelog, release notes, a version bump summary, or what shipped since the last tag, even if they do not say "release notes".
---
```

Rules:

- Lowercase letters, digits, and hyphens. At most 64 characters.
- The description says what the skill does and when to use it, in imperative
  voice. List concrete triggers and the boundary where it should not apply.
- Be a little pushy. Models tend to skip skills that would help. Say "use
  this whenever ..." and name the user's likely phrasings.
- Keep the description under 1024 characters. Do not summarize the workflow
  there; the agent may follow the summary and skip the body.
- Add `disable-model-invocation: true` only when the user should summon it
  explicitly, like a deploy or destructive migration skill. Tell the user to
  use `@skill:<name>`.

### 5. Write the body

- Assume the agent is already capable. Add only the project-specific facts,
  non-obvious steps, and decision criteria it would otherwise miss.
- Prefer procedures over answers. "Read the schema at
  `references/schema.yaml`, then ..." beats one hard-coded query.
- Give a default and mention the escape hatch, not a menu of equal options.
- Explain why a rule exists when the reason helps the agent handle a new case.
- Match specificity to fragility: prose for open choices, parameterized
  scripts for preferred shapes, exact commands only when a wrong move is
  expensive.
- Keep `SKILL.md` under about 500 lines. Move long schemas, API docs, and
  variant details into `references/`, and say exactly when to read each file.
- Use `scripts/` only for deterministic or repeated operations.
- Use `assets/` for files that go into the result, not instructions.
- No README, changelog, install guide, or other scaffolding.

Say where commands run. A skill can be loaded from anywhere, but project files
live somewhere specific. Pick one base, usually the repo root, and use it in
every command block. Do not mix a skill-relative script path with a
project-relative data path in the same block unless the sentence above the
block states both bases.

Script contract:

- Resolve project inputs from the stated base, or accept them as arguments.
  Never depend on the process cwd without saying so.
- Fail loudly when an expected input directory or file is missing. An empty
  result is not a missing input.
- Create the output directories the skill promises, without deleting existing
  files.
- Exit non-zero on validation failure and print one short `file:line: problem`
  line per issue. Keep normal output compact.
- Reject conflicting flags instead of silently preferring one.

Useful shape:

```markdown
## <task>

<what to do and why>

If <observable condition>, read [references/x.md](references/x.md).
```

### 6. Validate by running it

Run the skill's own commands, not just your eyes. Use a throwaway directory
with realistic fixture inputs.

- [ ] Directory name and `name` agree; the name is lowercase, digits, and
      hyphens, at most 64 characters.
- [ ] `description` is present, specific, and under 1024 characters.
- [ ] The body is non-empty and has no unfinished TODOs or placeholders.
- [ ] Every relative file mentioned in the body exists.
- [ ] Every command block names its working directory, and the paths in it
      are valid from that directory.
- [ ] Every fenced command was run verbatim from the working directory the
      body states and succeeded.
- [ ] The body's own output template passes the body's own validator.
- [ ] Every code example in the body parses or runs (YAML, JSON, shell,
      snippets).
- [ ] Empty sections, heading variants, and every behavior promised in the
      description or body each have a fixture that exercises them.
- [ ] At least one negative case per documented check fails with the expected
      message and exit code.
- [ ] Fixtures cover an empty result, a missing input, and an item that hits
      each extraction rule.
- [ ] A missing input directory fails loudly instead of returning an empty
      result.
- [ ] The skill is one coherent unit and does not restate what the model or
      `AGENTS.md` already knows.
- [ ] Hidden skills are documented for the user as `@skill:<name>`.
- [ ] `SKILL.md` is under about 500 lines; over 5k tokens is a smell.
- [ ] After the mechanical checks pass, read one generated artifact against
      the body and list every promise no validator checks.

Do not duplicate another skill or `AGENTS.md`. Link to the canonical home
instead.

### 7. Test it

Trigger test, worth doing for any skill the model should pick on its own:

1. Write three prompts that should trigger it and three near misses that
   should not. Near misses share keywords but need a different job.
2. If subagents are available, send each prompt to a research subagent with
   realistic context and ask which skill, if any, it would load first, and
   why. Do not reveal the expected answer.
3. If should-trigger prompts miss, tighten the description with the user's
   real words. If near misses trigger, add a boundary.
4. Run the explicit path yourself: `@skill:<name>` followed by a real task.
   Confirm the body is enough to finish it.

Output test, only for skills with objective results:

- Run one subagent with the skill and one without on the same task, then
  compare the artifacts.
- Keep it to one or two cases. This is a smoke test, not a benchmark.

### Updating an existing skill

- Preserve the name and directory.
- Read the current body first, then edit the smallest part that addresses the
  problem.
- Re-read the description after any scope change. Retest triggering if you
  changed it.
- If the skill grew past its budget, move detail into `references/` instead of
  deleting useful constraints.

## Anti-patterns

- Narrative: "In one session we found ...". Skills are reusable methods.
- Generic advice: "handle errors appropriately", "follow best practices".
- A menu of options with no default.
- Restating what `AGENTS.md` or the system prompt already says.
- A description that summarizes the workflow.
- Cramming unrelated jobs into one skill.
- A hidden skill the user has no way to discover. Tell them the token.

## Makima notes

- Skills are not slash commands. `/name` does nothing unless a command exists.
- Completion offers `@skill:` for every discovered skill, including hidden
  ones.
- Skill bodies are shown with line numbers when loaded. References are read
  with the read tool.
- The builtins `makima-plugin-dev` and `init` are examples of generated and
  hidden skills, but a normal skill is just files.

For deeper reference, the Agent Skills specification and best-practices pages
at agentskills.io cover the portable format, description testing, and script
guidance.
]=]

return {
  name = "skill-creator",
  description = 'Create, update, or review makima skills. Use whenever the user wants to turn a workflow into a skill, write a new skill from scratch, improve or slim an existing skill, make a skill trigger more reliably, or test whether a skill works, even if they do not say "skill".',
  content = CONTENT,
}
