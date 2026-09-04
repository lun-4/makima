Mirror Neovim's Lua API namespaces (maki.uv = vim.uv, maki.fs = vim.fs, maki.treesitter = vim.treesitter).
Keep function signatures identical so plugins can be copy-pasted between Neovim and maki.
Only exception is the UI API, neovim's has baggage.

## Design

Our goal is to let plugin authors have as much freedom as possible, that's why desiging the APIs should be looked at as simple primitives you combine together.

## Error convention

Fallible runtime operations return the pair (value, err) and never throw.
Throwing is reserved for programmer errors, like passing a number where a string belongs.
`util/pair.rs` is the single home for that shape: use `Pair<T>`, `err_pair`, `pair`, and `try_pair!` instead of writing a new helper.
A call with nothing to return still answers `(true, nil)` on success, so `if not ok` always means failure.

Tool handlers fail with `{ llm_output = msg, is_error = true }`; a plain string is always success (only `is_error` flags the result as an error to the provider).

## maki.fs backends

All `maki.fs` I/O dispatches through the `FsBackend` trait (`api/fs/mod.rs`), async and object-safe via boxed futures (`BoxFuture<'_, T>`). Backends that perform blocking I/O hop to the smol thread pool themselves (`RealFs` wraps each call in `smol::unblock`); unit tests drive the methods with `smol::block_on`. Two backends:

- `RealFs` (`api/fs/real.rs`): production. The exact semantics `maki.fs` always had — symlinks, gitignore, permissions, real mtimes. Keep it behavior-preserving; the `api::fs::tests` suite in `mod.rs` is the regression proof.
- `InMemoryFs` (`api/fs/in_memory.rs`): hermetic tests. Deliberately dumb — no symlinks, no gitignore, no permissions, file mtime is an insertion counter. Behavior tests must not rely on real-filesystem vagaries (project roots derived from `.git`, gitignore filtering); those belong in the real-backend suite.
- The test-only surface (`InMemoryFs`, `test_support`, the `*_for_test` helpers) is behind the `test-support` cargo feature, so production builds drop it and its `globset` dependency. A self dev-dep in maki-lua and a dev-dep in maki-ui enable the feature for all test builds, so scoped `cargo nextest run -p maki-lua` needs no flags.

The backend is resolved per Lua state via app_data (`FsBackendHandle`); a state that carries none falls back to a static `RealFs`, so bare `Lua::new()` unit tests still hit the disk. Test hosts boot disk-free: `test_support::spawn_host_for_tests` runs on `InMemoryFs` with the synthetic `TEST_STATE_DIR` (never created on the real disk), and `PluginHostGuard::backend()` exposes the backend for assertions (see `tests/in_memory_host.rs`).

## Callback seams

Autocmd callbacks run synchronously on the Lua thread and must never suspend (UI roundtrips, `win:recv`); defer suspending work via `maki.async.run` or a command/keymap handler instead, since command handlers run as deadline-free coroutines. See the `SessionPickerRequested` autocmd in `plugins/sessions/init.lua`.
