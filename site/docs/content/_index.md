+++
title = "Makima Docs"
sort_by = "weight"
+++

# Makima Docs

Makima is a terminal coding agent written in Rust, built bottom up to spend as few tokens as possible without getting dumber. Point it at a repo, pick a provider, and it reads, searches, edits, and runs code for you.

The docs are sorted by what you came here to do:

<div class="doc-group">
  <div class="doc-group-head">
    <span class="eyebrow">Getting Started</span>
    <span class="tagline">new to makima</span>
  </div>
  <div class="card-grid">
    <a class="card" href="/docs/quick-start/"><span class="card-title">Quick Start</span><span class="card-desc">Install, connect a provider, first session.</span></a>
    <a class="card" href="/docs/configuration/"><span class="card-title">Configuration</span><span class="card-desc">init.lua, the small Lua script where all settings live.</span></a>
  </div>
</div>

<div class="doc-group">
  <div class="doc-group-head">
    <span class="eyebrow">Guides</span>
    <span class="tagline">getting things done</span>
  </div>
  <div class="card-grid">
    <a class="card" href="/docs/skills/"><span class="card-title">Skills</span><span class="card-desc">Write Markdown playbooks the agent loads on demand.</span></a>
    <a class="card" href="/docs/plugins/"><span class="card-title">Plugins</span><span class="card-desc">Add your own tools and commands in Lua, or let the agent write them.</span></a>
    <a class="card" href="/docs/headless/"><span class="card-title">Headless Mode</span><span class="card-desc">--print for scripts and CI. Drop-in Claude Code compatible.</span></a>
    <a class="card" href="/docs/acp/"><span class="card-title">ACP</span><span class="card-desc">Drive Makima from your editor, like Zed, over the Agent Client Protocol.</span></a>
  </div>
</div>

<div class="doc-group">
  <div class="doc-group-head">
    <span class="eyebrow">Concepts</span>
    <span class="tagline">wondering why</span>
  </div>
  <div class="card-grid">
    <a class="card" href="/docs/token-economy/"><span class="card-title">Token Economy</span><span class="card-desc">Where tokens go in an agent loop, and every trick Makima uses to spend fewer of them.</span></a>
    <a class="card" href="/docs/modes/"><span class="card-title">Modes</span><span class="card-desc">Build, plan, and plugin-defined modes you can switch between or override.</span></a>
    <a class="card" href="/docs/context/"><span class="card-title">Context</span><span class="card-desc">What enters the model's context and when, and where to put project knowledge.</span></a>
    <a class="card" href="/docs/splash/"><span class="card-title">Home-screen splash</span><span class="card-desc">The default screen is a Lua plugin; override it with matrix rain and more.</span></a>
  </div>
</div>

<div class="doc-group">
  <div class="doc-group-head">
    <span class="eyebrow">Reference</span>
    <span class="tagline">looking something up</span>
  </div>
  <div class="card-grid">
    <a class="card" href="/docs/tools/"><span class="card-title">Tools</span><span class="card-desc">Every built-in tool and its parameters.</span></a>
    <a class="card" href="/docs/providers/"><span class="card-title">Providers</span><span class="card-desc">Model catalogs, env vars, providers.toml, model tiers.</span></a>
    <a class="card" href="/docs/permissions/"><span class="card-title">Permissions</span><span class="card-desc">What runs freely, what asks first, TOML rules.</span></a>
    <a class="card" href="/docs/notifications/"><span class="card-title">Notifications</span><span class="card-desc">Know when a session finishes or needs your input.</span></a>
    <a class="card" href="/docs/mcp/"><span class="card-title">MCP</span><span class="card-desc">External tool servers over stdio or HTTP.</span></a>
    <a class="card" href="/docs/commands/"><span class="card-title">Commands</span><span class="card-desc">The / palette, sessions, toggles, custom commands.</span></a>
    <a class="card" href="/docs/keybindings/"><span class="card-title">Keybindings</span><span class="card-desc">Defaults, precedence, rebinding from Lua.</span></a>
    <a class="card" href="/docs/lua-api/"><span class="card-title">Lua API</span><span class="card-desc">The plugin surface, mirrored from Neovim.</span></a>
    <a class="card" href="/docs/hooks/"><span class="card-title">Hooks</span><span class="card-desc">Rewrite, block, or trim a tool call from Lua.</span></a>
    <a class="card" href="/docs/cli/"><span class="card-title">CLI</span><span class="card-desc">Flags and subcommands (auth, models, acp, prompt, ...).</span></a>
    <a class="card" href="/docs/references/"><span class="card-title">References</span><span class="card-desc">The @ popup: files, skills, subagents, models, and what each does at submit.</span></a>
  </div>
</div>

Something missing or wrong? Open an issue on [GitHub](https://github.com/lun-4/makima).
