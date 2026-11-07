Makima is an AI coding agent (like Claude Code and opencode), that is built bottom up to optimize costs and number of tokens used, while not sacrificing performance too much.

## Code guidelines

- No trivial comments
- Minimal bloat (KISS, DRY, SRP)
- No unnecessary state (variables, fields, arguments)
- Each line of code should justify its existence
- Follow Rust idioms and best practices
- Latest Rust features can be used
- Descriptive variable and function names
- No wildcard imports
- Import types at top of file and use short names everywhere (e.g. `use std::sync::Arc;` then `Arc<T>`, never `std::sync::Arc<T>` inline)
- Keep consts at top of file, right after imports
- Explicit error handling with `Result<T, E>` over panics
- Use `color_eyre` when the specific error is not as important
- Use custom error types using `thiserror` for domain-specific errors
- Place unit tests in the same file using `#[cfg(test)]` modules
- Add dependencies to global `Cargo.toml`, and then set workspace=true in specific package
- Try solving with existing dependencies before adding new ones
- Prefer well-maintained crates from crates.io
- Be mindful of allocations in hot paths
- Prefer structured logging (wide logs with a bunch of useful fields)
- Provide helpful error messages
- Use #[test_case] when writing tests, and use snake_case for naming the tests
- No need for bullshit tests (e.g. tautology)
- Make sure tests are not flaky (no weird sleeps)
- No inline magic numbers or strings
- In tests const error/status messages and assert against the shared constant
- Add #[derive(Copy)] only on structs with 1 primitive field
- NO TRIVIAL COMMENTS

## Testing

Cheapest first, and scope to the crate you touched while iterating:

- `just check` (or `cargo check -p <crate> --tests`)
- `just lint` - `cargo clippy --all --tests -- -D warnings`
- `just test` - `cargo nextest run --workspace`

Read `justfile` for more.

Dev builds skip debug info for deps and vendored C (our crates keep it); to debug into a dep set `[profile.dev.package.<name>] debug = true`.

## Nix

`nix develop` (dev shell; exports `OPENSSL_NO_VENDOR`, saves ~80s cold build - export it yourself outside the shell if you have libssl-dev), `nix build`, `nix fmt` (nixfmt), `nix flake check` (includes git-dep-hashes drift).

## Architecture

Rust workspace, key crates in root dir:

- maki-ui: Uses ratatui for an interactive UI (elm like architecture). `maki-ui/src/click.rs` holds the per-frame hit registry: components record `HitTarget`s while they render, `App::view` appends them in render order, and `app/mouse.rs` arbitrates clicks, scrollbar drags, hover, and the modal outside-click gate.
- maki-providers: Provider-neutral types (messages, events, `ModelInfo`, `ThinkingConfig`, errors), the `Provider` trait, `ProviderRegistry` and the login driver (`login.rs`: `drive_login`, `auth_status`, `logout`, `InteractionService`). It knows no provider by name
- maki-agent: Owns the smol-based agent loop and the persistent actor shared by TUI roots and Lua agent sessions
- maki-interpreter: code_execution tool implementation using pydantic/monty (a minimal python sandbox)
- maki-storage: Persistent state across runs (e.g. sessions, auth); cross-process session locks (`session_lock`) keep a session that is open in another terminal from being resumed here; per-session offload dirs (`sessions/offload/<id>/`) hold tool output past the limits, written through `maki-agent`'s `tools::offload` store
- maki-config: User config
- maki-lua: Lua plugin system (API mirrored from neovim for plugin compatibility), built-in plugins in ./plugins dir. Provider host: `maki.api.register_provider` (`api/provider.rs`), `LuaProvider` (`provider.rs`), `ctx.credentials` (`credentials.rs`), the login `ui` (`login_ui.rs`) and streaming `maki.net.send` (`api/net/send.rs`)
- maki-match: shared fuzzy matching (nucleo): `fuzzy_match`/`fuzzy_resolve` used by command-argument resolution and the `maki.match` Lua API
- maki-acp: ACP ndjson stdio server
- claude-agent-bridge: TypeScript bridge that serves the `claude-agent` provider through the Claude Agent SDK over a Unix socket (`just bridge-check`)

Built-in lua plugins in ./plugins: index (return a compact skeleton of a source file using tree-sitter), bash, glob, question, skill, memory, webfetch, websearch, todo_write, read, write, edit, task, code_execution (python sandbox), batch, splash (the home-screen renderer; it owns the version UI via `maki.version()`), providers (every built-in provider, the models.dev catalog providers, and custom and executable providers from `providers.toml` and `providers/`).

## Providers

Every provider lives in Lua. Rust never names a provider id or endpoint; `src/architecture_tests.rs` enforces this. `plugins/providers/init.lua` registers them all. Modules under `plugins/providers/providers/` hold the providers and what they share: codecs (`chat_completions`, `responses`, `anthropic`, `gemini`, `sse`, `bedrock_eventstream`), `http` (API keys and rotation, `${VAR}` headers, error responses, `config_error`), `openai_compat` (the factory for Chat Completions providers), `local_server`, `thinking`, `catalog` and `sigv4`. `types.lua` holds Luau annotations for authors.

Adding a provider:

- An OpenAI-compatible API is a `def` table in `plugins/providers/providers/<id>.lua`, added to the `openai_compat` list in `init.lua`. Read the `def` fields at the top of `openai_compat.lua`.
- Anything else returns a `register_provider` spec from its module. The fields (`id`, `display_name`, `models`, `prepare`, `discover`, `config`, `auth`, `aliases`, `section`, `auth_status`, `logout`, `docs`) and the instance and error contract are documented on `register_provider` in `maki-lua/src/api/provider.rs`. Reuse a codec and `http` rather than parsing streams or reading keys yourself.
- Send requests with `ctx.send` (the plugin's `maki.net.send`, which has no private-address guard, so never give it a URL the model chose), store secrets with `ctx.credentials`, and return `http.config_error(message)` for what only the user can fix (missing key, setting or login), so the host does not retry it.
- The host retries an error only when it names `retry`. `init.lua` passes every provider it registers through `http.classify_stream`, which names the class from the status or the `maki.net.send` failure; a provider that knows better (a step that must not repeat) names `retry` itself.
- Fill `docs` (`env`, `endpoint`, `notes` in Markdown): docgen renders the providers page from the registry, and a provider without `docs` is left off it. Run `just gen-docs`.
- Write a spec in `plugins/providers/tests/<id>.lua` with `maki.test_helpers` (`t.provider_ctx` scripts responses and credentials) and add it to `maki-lua/tests/spec.rs`. Test what is specific to the provider, not the shared codecs again.

## Docs

Homepage: ./site/. User docs: ./site/docs/.

Generated by `maki-docgen` (`just gen-docs` / `just gen-docs-check`): tools, providers, configuration, lua-api, plugins, keybindings, commands. Bundled Lua command docs use static extraction of direct `maki.api.register_command` calls with one inline table and literal metadata fields. Keep that contract within the Lua grammar supported by `tree-sitter-lua`; unsupported expressions and Luau-only syntax fail generation instead of being omitted.

Hand-written: quick-start, permissions, skills, mcp, cli, headless, acp, token-economy, context, and the docs index.

Docs follow [Diátaxis](https://diataxis.fr/) (guides for goals, reference for lookup, concepts for understanding); keep one canonical home per topic, link instead of duplicating.

Tone: warm, simple, concise, easy for non-native English, story telling. No em-dashes, no emojis, no AI tone. Audience is competent devs: no hand-holding, no fluff, every line earns its place. ASCII diagrams (in ``` blocks) where they beat prose.
