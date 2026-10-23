# Internal architecture documentation

This directory documents makima's current implementation for maintainers. It is separate from the generated and user-facing site in [`site/docs/`](../site/docs/).

## Agent system

- [`agent-system.md`](agent-system.md): the current agent runtime, actor, manager, graph, APIs, and frontend integration
- [`agent-invariants.md`](agent-invariants.md): correctness rules that changes to the agent system must preserve

These files describe the implementation on the current branch. They are not a specification for later Issue 24 work: check the deferred-work section in the agent-system document before assuming a proposed API or persistence behavior exists.
