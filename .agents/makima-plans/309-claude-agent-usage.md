## Goal

Show Claude subscription usage (plan, 5-hour and weekly rate-limit utilization, reset times, extra-usage credits) for the `claude-agent` provider through Makima's existing provider usage view, using the Claude Agent SDK's `usage_EXPERIMENTAL_MAY_CHANGE_DO_NOT_RELY_ON_THIS_API_YET` call.

## Implementation Summary

Makima already has the whole consumer side: `Provider::fetch_usage` returning `ProviderUsage { plan, limits }` (`makima-providers/src/types.rs:592`), the Lua instance hook `fetch_usage(ctx)` (`makima-lua/src/api/provider.rs:226`, `makima-lua/src/provider.rs:781`), and the UI fetch/render path (`makima-ui/src/event_loop.rs:2862`, status bar usage view). OpenAI (`plugins/providers/providers/openai.lua:504,661`) and Z.ai (`zai.lua:175`) are the reference implementations. No Rust change is needed.

The work is two pieces:

1. **Bridge**: a new `GET /v1/usage` route that starts an idle Claude Code query (like `probe()`), calls the SDK usage method with `skipBehaviors: true`, trims the answer to known fields, and converts ISO reset times to epoch milliseconds. Cached briefly, because each call starts a `claude` process (measured about 375 ms init plus 470 ms for the request).
2. **Lua provider**: `instance.fetch_usage(ctx)` in `plugins/providers/providers/claude_agent.lua` that GETs the route and maps it to `ProviderUsage` with a pure `M.parse_usage(body)`.

Verified manually against Claude Code 2.1.322 and SDK 0.3.318: the call works with a Pro login, returns `subscription_type`, `rate_limits_available`, `rate_limits.five_hour` / `seven_day` as `{ utilization, resets_at }`, and `extra_usage` with `used_credits` in minor units plus `decimal_places` and `currency`. `rate_limits` also carries about 25 undocumented codename keys (all null), so the bridge must whitelist fields rather than pass the object through.

## Implementation Plan

1. **Bridge: fetch usage from Claude Code** (`claude-agent-bridge/src/session.ts`)
   - Factor the idle-query lifecycle out of `probe()` into a private helper that starts `query({ prompt: idle(), options: { ...options, persistSession: false } })`, awaits `initializationResult()` under the timeout, runs a callback with the SDK handle and init result, and always resolves the idle prompt and closes the SDK in `finally`. `probe()` keeps its exact behavior on top of it.
   - Add `export async function usage(options, timeoutMs): Promise<Usage | undefined>` built on the same helper. When the SDK handle has no `usage_EXPERIMENTAL_MAY_CHANGE_DO_NOT_RELY_ON_THIS_API_YET` function, return `undefined`. Otherwise call it with `{ skipBehaviors: true }` under the timeout and return `trimUsage(raw)`.
   - Keep the experimental method name in one const so the rename on stabilization is a one-line change.

2. **Bridge: protocol shape** (`claude-agent-bridge/src/protocol.ts`)
   - Add the wire types:
     ```ts
     interface UsageWindow { utilization?: number; resets_at?: number }   // resets_at: epoch ms
     interface Usage {
       plan?: string
       available: boolean
       five_hour?: UsageWindow
       seven_day?: UsageWindow
       seven_day_opus?: UsageWindow
       seven_day_sonnet?: UsageWindow
       model_scoped: ({ name: string } & UsageWindow)[]
       extra_usage?: { enabled: boolean; used?: number; limit?: number; currency?: string }   // major units
     }
     ```
   - Add pure `trimUsage(raw)` here (or in `convert.ts`, next to the other SDK-to-wire mapping):
     - Read only the fields above. Drop windows that are null or have neither a numeric utilization nor a parseable `resets_at`.
     - `resets_at` goes through `Date.parse`; NaN drops the field.
     - `extra_usage` money is divided by `10 ** (decimal_places ?? 2)`.
     - When `rate_limits_available` is false or `rate_limits` is null, return `{ plan, available: false, model_scoped: [] }`.
   - `PROTOCOL_VERSION` stays 1: the route is additive, and an old bridge answers 404, which the Lua side treats as unsupported.

3. **Bridge: route** (`claude-agent-bridge/src/server.ts`, `src/main.ts`)
   - Extend `handler` with a third dependency, `fetchUsage: () => Promise<Usage | undefined>`, wrapped in the existing `cachedProbe` pattern with its own TTL const (`USAGE_TTL_MS = 40_000`, just over the usage view's 30 s poll, so every other poll hits the cache). Generalize `cachedProbe` to `cached<T>(ttl, load)` rather than duplicating it.
   - `GET /v1/usage` answers `200 { usage }`. `usage` is omitted when the SDK lacks the method. Failures go through the existing catch and `toErrorTable`.
   - Wire it in `main.ts` with `usage(base(environment), PROBE_TIMEOUT_MS)`. Update test call sites of `handler(...)` for the new argument.
   - Mention the route in `claude-agent-bridge/README.md` if it lists routes.

4. **Lua provider** (`plugins/providers/providers/claude_agent.lua`)
   - Add `USAGE_PATH = "/v1/usage"` with the other path consts.
   - Add pure `M.parse_usage(body)` returning `ProviderUsage` or `nil`:
     - `nil` when `body.usage` is missing or `available` is false (API key, Bedrock or Vertex sessions, or an SDK without the method). The UI shows that as unsupported.
     - `five_hour` → `{ kind = { Hours = 5 } }`
     - `seven_day` → `{ kind = { Weekly = {} } }`
     - `seven_day_opus` → `{ kind = { Weekly = { model = "Opus" } } }`, `seven_day_sonnet` → `{ Weekly = { model = "Sonnet" } }`
     - each `model_scoped[i]` → `{ kind = { Weekly = { model = name } } }`
     - `extra_usage` when `enabled` → `{ kind = { Credits = {} }, detail = "<used> / <limit> <currency>" }`, or `"<used> <currency> used"` without a limit, formatted with two decimals. Check the exact unit-variant encoding against `openai.lua`'s `{ Weekly = {} }` and the serde `UsageWindow` derive while implementing.
     - `percentage` = `math.max(0, math.floor(utilization + 0.5))` when numeric, with no upper clamp: claude.ai reports over 100 once a window is exceeded, and that is worth showing. `reset_at` is passed through (already ms).
     - `plan` = `body.usage.plan`.
     - A window that maps to neither percentage nor reset is skipped.
   - In `prepare`, add `instance.fetch_usage(ctx)`: GET `USAGE_PATH` with the existing `get_json`/`send` helpers. A bridge that cannot be reached returns `unreachable(socket, err)`, as `stream` does. A 404 returns `nil` (old bridge). Any other non-200 returns `refused(response)`. Otherwise return `M.parse_usage(body)`.
   - Use named consts for the labels and the 5-hour window. No inline magic values.

5. **Docs**
   - Extend the provider `docs.notes` in `claude_agent.lua` with one sentence: plan usage shows for Claude subscription logins, and API-key logins have none. Run `just gen-docs`.

## Acceptance Criteria

- **AC.1** `GET /v1/usage` returns the trimmed `Usage` with epoch-ms resets, whitelisted fields only, and extra-usage money in major units.
- **AC.2** A session without plan limits (`rate_limits_available: false`) yields `available: false`, and the Lua side returns `nil` (unsupported).
- **AC.3** An SDK without the experimental method yields a 200 without `usage`. An old bridge (404) also maps to `nil`, not an error.
- **AC.4** Repeated `/v1/usage` calls within the TTL start Claude Code once. A failed fetch is not cached.
- **AC.5** `M.parse_usage` maps every window kind to the documented `UsageWindow`, rounds percentages and floors them at 0 (over 100 is kept), skips empty windows, and formats credits.
- **AC.6** `instance.fetch_usage` reaches the bridge over the socket. An unreachable bridge is a config error (no retry), matching `stream`.
- **AC.7** `probe()` behavior and the existing routes are unchanged.

## Test Strategy

| AC | Test |
|---|---|
| AC.1 | `claude-agent-bridge/test/convert.test.ts` (or a new `usage.test.ts`): `trimUsage keeps known windows and converts resets to epoch ms`, `trimUsage drops codename keys and null windows`, `trimUsage converts extra usage credits to major units` (fixture modeled on the real response captured during investigation) |
| AC.2 | bridge: `trimUsage marks sessions without plan limits unavailable`. Lua: `parse_usage_without_plan_limits_is_unsupported` |
| AC.3 | `server.test.ts`: `usage without the SDK method answers without usage`. Lua: `fetch_usage_on_an_old_bridge_is_unsupported` (scripted 404) |
| AC.4 | `server.test.ts`: `usage is cached for its ttl`, `a failed usage fetch is not cached` (counting fake `fetchUsage`) |
| AC.5 | `plugins/providers/tests/claude_agent.lua`: `parse_usage_maps_windows` (test_case table over five_hour, seven_day, opus, sonnet, model_scoped), `parse_usage_skips_disabled_credits_and_empty_model_windows` (also keeps a percentage over 100), `parse_usage_skips_empty_windows`, `parse_usage_formats_credits` |
| AC.6 | Lua: `fetch_usage_reads_the_bridge`, `fetch_usage_with_bridge_down_is_a_config_error` |
| AC.7 | Existing `server.test.ts` (`status reports the login`, step tests) and existing `claude_agent.lua` cases pass unchanged |

Checks: `just bridge-check`, `just fmt-check`, `cargo nextest run -p makima-lua` (runs the `providers_claude_agent_spec`), `just gen-docs-check`, then `.ssh/remote-ci.sh` for full CI. Manual: with the bridge running and a subscription login, open the usage view in the TUI and compare against `claude` `/usage`.

## Review Strategy

After implementation, review the diff against AC.1 to AC.7, with special attention to process cleanup in the shared idle-query helper (no leaked `claude` processes on timeout or error), the unknown-field whitelist, and unit conversions (ISO to ms, minor to major currency units).

## Risks and Decisions

- **Experimental API.** The method name and response shape may change in any SDK release. Mitigations: the method check, the field whitelist, and a single const for the method name. When it is stabilized, rename the const and revisit `trimUsage`.
- **Cost per fetch.** Each uncached fetch starts a `claude` process (about 0.8 s). That is acceptable for a user-opened view, and the 30 s cache bounds repeats. Reusing a live conversation's SDK handle is a possible later optimization, but out of scope.
- **Not chosen: passive `rate_limit_event` capture** (`convert.ts:189`). It is free, but covers only the window that fired and has no plan or credits. It could become a fallback later.
- **Decision: extra usage shows only when enabled.** A disabled pool (for example `out_of_credits`) is omitted to keep the view focused on limits that apply.
- **Decision: map to Makima's `UsageWindow` in Lua, not in the bridge.** The bridge protocol stays free of Makima's serde enum encoding, and the mapping is spec-tested next to the other providers.

## Implementation Notes (deviation from the plan)

`anthropic_provider.lua` already parses claude.ai's OAuth usage body (`five_hour`, `seven_day*`, `extra_usage` in minor units with `decimal_places`), which is the same shape the SDK returns. So the bridge forwards the whitelisted fields as they are (`planUsage` in `convert.ts`: RFC 3339 resets and minor-unit credits are kept), and `claude_agent.parse_usage` reuses `anthropic.parse_usage`. It adds `model_scoped` rows through the new shared `anthropic.window_limit` and lists them before the credits row. Credits are formatted like the anthropic provider (`253.84 BRL spent`), shown only when extra usage is enabled.
