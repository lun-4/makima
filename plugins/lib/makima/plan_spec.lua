-- Shared plan specification, spliced verbatim into the plan-mode directive and
-- the review prompt so both always see the exact same document.
--
-- Ported from polytoken's default plan specification (plan_spec_default.md);
-- keep in sync with upstream.

return [[
## Plan artifact specification

This default plan specification is optimized for software projects. Non-software projects should follow it as closely as is useful, adapting section content to the domain.

A handoff plan is written for the next execution agent, not for the transcript. It should be proportional to the requested work: small tasks can have short sections, but the plan should still provide enough detail to execute and validate without rediscovering the same context.

Use the following headings in the plan you write to the plan file. For small tasks, keep sections brief. If a section has no substantive content, write a short "Not needed because..." rather than expanding boilerplate.

## Goal

Write one or two sentences describing the concrete outcome.

## Implementation Summary

Summarize the approach and why it fits.

For software projects, name the relevant packages, crates, modules, services, commands, APIs, or contracts. For non-software projects, use well-understood document names, concepts, stakeholders, systems, or artifacts to orient the next agent.

Include affected touch points, important files, public contracts, and scope boundaries or non-goals when they affect execution.

## Implementation Plan

Write phased implementation steps with detailed requirements.

Include relevant research and solutioning here when the user requested in-depth planning or when investigation produced important findings. Cite research with project-relative file paths, symbols, documentation URLs, or other stable references where available.

Resolve implementation unknowns before writing the plan. Use repository inspection, available research tools, or a `research` subagent when the environment provides one. Do not leave ordinary implementation questions for the executing session.

## Acceptance Criteria

List explicit acceptance criteria named `AC.1`, `AC.2`, and so on.

Each criterion must say how completion can be determined, such as a concrete runnable verification passing, generated output matching expectations, or a human validating behavior, copy, visuals, or workflow. Acceptance criteria should describe observable completion, not merely restate implementation steps.

Each criterion must be independently verifiable by an executable test or a concrete observable check. "Verified by code inspection" is not sufficient — if a criterion cannot be tested with current infrastructure, say so explicitly in the Test Strategy and flag the gap in Risks rather than silently weakening the criterion.

## Test Strategy

Describe the tests or checks that should validate the acceptance criteria.

**Every acceptance criterion must have at least one named test** that would fail if the criterion's behavior regressed. Map tests to acceptance criteria explicitly — either inline per criterion (e.g. "AC.3 → `test_isolation_guarantee`") or in a lightweight table. The plan_reviewer will check this mapping and flag any criterion without coverage.

Identify the appropriate test layer for each behavior:

- Pure logic (parsers, state machines, transforms, validation) → unit tests.
- Full-stack behavior (routes, persistence, hooks, process boundaries, provider flows) → integration tests with whatever test harness the project provides.
- Observable interface changes (UI, TUI, CLI output) → render or scenario tests that assert visible behavior.

Do not mark acceptance criteria as "implicitly verified," "covered by existing tests," or "code inspection only." These are not tests. If a criterion genuinely cannot be tested with current infrastructure, call out the gap explicitly in this section and in Risks — do not hide it behind a euphemism. Pre-existing tests that pass regardless of whether the new behavior exists do not validate the new work.

If the project appears not to have a meaningful test mechanism, or if adequate testing requires infrastructure that does not yet exist (e.g., no integration harness, no UI test framework, no mock provider), flag the gap explicitly and surface it to the operator before handoff. Proceeding without test infrastructure means the resulting work will be less reliable and regressions will be harder to catch. If automated testing is impractical, include a manual validation approach and note its limitations.

## Review Strategy

Review happens twice: first in plan mode before `plan_submit`, and later after implementation and after all automatable testing is complete.

The plan-mode review should use the `plan_reviewer` subagent; the operator can choose to skip it at the `plan_submit` approval step. All plan_reviewer findings should be fixed in the plan with `edit` or explicitly rebutted to the operator. If a review returns any critical or high findings, fix or rebut all findings and run another `plan_reviewer` pass before `plan_submit`.

For implementation review, if the repository has its own review guidance, the executing session should follow it. Otherwise, the executing session should dispatch a `general` subagent tasked with reviewing the completed work.

All implementation review findings must be fixed or explicitly rebutted by the executing session. If critical findings are returned, fix or rebut all findings, then run another review pass. Repeat until no critical findings remain, or until progress is blocked and the operator must decide.

## Documentation Strategy

Describe documentation work needed for the change.

Consider both repository-facing documentation, such as `AGENTS.md`, architecture notes, or similar project-local guidance, and user-facing documentation, if the repository has it or the behavior is user-visible.

If documentation appears unnecessary, say why. If the repository has no documentation pattern but the change would normally warrant docs, call that out.

## Risks, Blockers, and Required Decisions

List known risks the executing session should watch.

Resolve blockers in plan mode whenever possible. If a blocker remains after reasonable investigation and required user questions, flag the blocker to the operator before finalizing the plan or calling `plan_submit`. Do not quietly bury blockers inside the plan.

Use `question` for decisions only the operator can make, such as product intent, scope tradeoffs, approval to change behavior, or choosing between acceptable alternatives with different user-facing consequences. If the operator's answer opens new research avenues or changes the implementation approach, do that follow-up investigation before writing the plan.

In the final plan, include only blockers the operator has already seen and accepted, and decisions that have already been answered or must be made before execution can safely proceed.
]]
