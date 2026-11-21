## Goal

Implement [#333](https://github.com/lun-4/makima/issues/333): report the time bounds of an observed prompt-cache prefix and make the latest report available to Lua plugins during a turn. Use the existing direct ChatGPT OAuth path for Codex subscriptions as the reference integration and implement all defensible bundled-provider mappings in the same branch.

## Implementation Summary

Add an optional `PromptCache` report to `makima-providers::StreamResponse` and the Lua provider `stream` result. A provider reports one prefix and owns its deadline. `makima-agent` publishes the latest main-request report to the live actor snapshot before tool dispatch. `makima-lua` exposes it as `Agent:status(ctx).cache` under the existing read authority.

Keep all provider mappings in the same branch and keep provider identities, auth routing, and retention policy outside Rust core. Reuse Makima’s direct Responses HTTP/SSE transport and its tool loop. No `codex app-server`, subprocess, patched binary, WebSocket transport, or new OAuth flow is required. Do not change the outbound requests or their routing.

V1 observes the existing policy. It does not request a higher TTL, change cache controls or pricing, add UI or events, or schedule compaction. Cache-preserving and timed compaction remain in [#334](https://github.com/lun-4/makima/issues/334). V1 keeps reports in memory. Add persistence soon after the ongoing storage rework; store the original absolute deadline and enough identity to reject a stale report on resume. Do not start a new TTL clock on resume.

## Implementation Plan

### Phase 1: Provider and Lua contract

Add the following provider-neutral shape in `makima-providers/src/types.rs:583`. Keep `Provider::stream_message` and `RequestOptions` unchanged. Update response constructors and mocks to use `None` unless they have a report.

```rust
pub enum CacheBound {
    Minimum { ttl_secs: NonZeroU32, bound_at: u64 },
    UpTo { ttl_secs: NonZeroU32, bound_at: u64 },
    NoTimeLimit,
}

pub struct PromptCache {
    pub bound: CacheBound,
    pub prefix_tokens: Option<NonZeroU32>,
}
```

Expose the same flat table in the Lua provider result and agent snapshot:

```lua
cache = { bound = "minimum", ttl_secs = 300, bound_at = 1763049900, prefix_tokens = 182000 }
cache = { bound = "up_to", ttl_secs = 86400, bound_at = 1763136000 }
cache = { bound = "no_time_limit", prefix_tokens = 80000 }
```

- `minimum`: the provider's coverage window extends at least to `bound_at`. Passing the deadline does not establish a miss. `up_to`: this observation supports no later deadline, and the prefix may be gone earlier. A typical lifetime is neither bound.
- `no_time_limit`: elapsed time alone does not invalidate this cached prefix under the identified policy. Omit `ttl_secs` and `bound_at`. This does not guarantee a hit when the prefix, account, routing, model, or other cache inputs change.
- A finite report requires a positive whole-second `ttl_secs` and a nonnegative whole-second Unix `bound_at`. `ttl_secs` describes the original duration; `bound_at` is the provider's defensible absolute deadline. Response time is the default anchor where the provider's policy permits it. Use a provider-specific anchor or conservative margin where needed. For example, Anthropic measures from request start. Use the attempt that produced the report, not an earlier retry. A deadline already in the past is valid. The host does not rebase, add a margin, or infer a deadline.
- `prefix_tokens` is optional and counts the prefix covered by the report. Do not substitute aggregate read/write totals when they cannot establish that coverage. Report at most one prefix; prefer the deepest prefix whose policy can be established. `nil` means there is no defensible report for this response. Cache usage remains independent of TTL metadata.

In `makima-lua/src/provider.rs:755`, decode the optional cache table separately from the required response fields. Validate it without Lua coercion. For a malformed table, log a structured warning naming the provider and invalid field, omit the report, and keep the message, usage, and stop reason. Use no prompt, token, or credential data in the warning. Update `plugins/providers/types.lua` and the Lua registration contract in `makima-lua/src/api/provider.rs:244`.

### Phase 2: Publish the live agent snapshot

Store the report in `ActorState`. `Runner::run_turn` (`makima-agent/src/actor/runner.rs:427`) supplies an optional synchronous cache sink through `TurnContext` to `Agent::for_actor_turn` (`makima-agent/src/agent/run.rs:258`). The sink captures `Weak<ActorInner>`, the admitted `TurnId`, the cancellation generation, and the admitted model/provider revision. Capture the admitted revision with the frozen policy when materializing `TurnAdmission` in `actor/mod.rs:706`, not when a queued turn begins. Each request separately captures the history revision before it is sent. Under the actor state mutex, require an open actor, `Running(turn_id)`, a matching processing turn with no cancellation reason, an installed active cancellation handle, and unchanged cancellation generation, admitted model/provider revision, and request history revision. Advance the model/provider revision on committed model/provider changes, even if the user switches away and back. An old turn cannot adopt that revision on a later request. Advance the separate history revision on in-turn history replacement; the next request may capture it if the turn still owns the admitted model/provider revision. Do not advance either revision for unrelated config changes. Publish the successful response at `run.rs:762` and release the lock before `TurnComplete` or tool dispatch. An obsolete sink must not write, even to clear. Do not go through an asynchronous frontend event. Project through `ActorSnapshot` (`makima-agent/src/actor/types.rs:203`) and `agent_status` (`makima-lua/src/api/agent/handles/agent_methods.rs:62`) using the existing observation authority.

- Replace the report on every successful main-model response, including responses that request tools. A missing or invalid report clears the previous one. A failed attempt does not publish a new report or move its deadline. Do not clear solely because time passed.
- Clear at the model/provider commit in `actor/prepared.rs:500`, after applying a rewind in `actor/history_edit.rs:232`, on successful automatic compaction in `agent/run.rs:1053` and queued compaction in `actor/runner.rs:610`, and when an entered turn sanitizes history on cancellation in `agent/run.rs:537`. The cancellation clear must use the matching processing owner and history mutation rather than the success sink, which rejects cancelled runs. For both headless control variants, clear at the successful history commit in `HeadlessBackend::compact` before `run_control` sends its `InteractiveControl::Compact` reply or `ManualCompactionEvent::Completed` (`makima-agent/src/headless.rs:719`). Do not wait for the runner to settle the control. Preserve the report on failed or rolled-back commits. Apply the same clear at a successful headless reset (`makima-agent/src/headless.rs:759`). A model switch can leave the same conversation ID. Ordinary appends are not an invalidation.
- An isolated request or compaction summary request does not replace the main report. A successful compaction clears it because that operation replaces the main history, not because of the summary request's usage. A rejected or queued cancellation that leaves history unchanged does not clear it.
- An idle agent has no fingerprint for its next resolved prompt. Document this field as an observation of the last applicable report, not a prediction of a future hit.

Plugins can read it from a tool handler or from an existing hook. No TTL event, historical `TurnEnd` payload, or timer API is needed.

```lua
makima.api.create_autocmd("ToolStart", {
  callback = function(ev, ctx)
    if not ev.agent then return end
    local status = ev.agent:status(ctx)
    local cache = status and status.cache
    if not cache or cache.bound ~= "minimum" then return end
    local seconds_left = cache.bound_at - makima.time.now().secs
    if seconds_left <= 0 then
      makima.log.info("Prompt-cache minimum window has ended")
    end
  end,
})
```

`AgentIdle` offers the same read after work drains. Hooks read current state, not a snapshot attributed to the event's turn. An event context cannot be reused from a later timer. A display-only plugin can compare the captured timestamp later; acting on the agent requires fresh authority and a fresh check.

### Phase 3: Direct Codex subscription reference

Makima posts logged-in Codex requests directly to `https://chatgpt.com/backend-api/codex/responses` (`plugins/providers/providers/openai.lua:540`). Pi, Oh My Pi, and OpenCode use the same direct endpoint on their Codex login paths. The clients may prefer WebSocket, but none needs app-server for inference or tool execution. [Pi transport](https://github.com/earendil-works/pi/blob/780bf31fcaeb252fa9b68df3964c735eae75bde3/packages/ai/src/api/openai-codex-responses.ts#L319-L430) · [Oh My Pi transport](https://github.com/can1357/oh-my-pi/blob/efd92300eec7e1d20e4d07393632f9b9ffcf031f/packages/ai/src/providers/openai-codex-responses.ts#L1479-L1530) · [OpenCode Codex route](https://github.com/anomalyco/opencode/blob/09bfa6041dfe4d3cc194588facd91633d93eaaf2/packages/core/src/plugin/provider/openai.ts#L243-L280).

The [Codex announcement](https://x.com/thsottiaux/status/2112712984644784534) distinguishes logged-in Codex and Work from the public API's finite retention. This plan applies the announced elapsed-time policy to Makima's official direct Codex route. [Oh My Pi applies that same interpretation](https://github.com/can1357/oh-my-pi/blob/efd92300eec7e1d20e4d07393632f9b9ffcf031f/packages/catalog/src/compat/rules/providers/openai-codex.kdl#L24-L31) and [excludes proxies](https://github.com/can1357/oh-my-pi/blob/efd92300eec7e1d20e4d07393632f9b9ffcf031f/packages/catalog/src/compat/prompt-cache-persistent.ts#L5-L12). This is a product-policy inference, not a TTL field in Responses or proof that KV tensors stay resident indefinitely.

1. In the `authorized` callback in `openai.lua`, select the request's policy after the live auth and URL are resolved. Evaluate it anew on each invocation, including after refresh or API-key fallback. Require a stored ChatGPT OAuth credential selected for this request, the exact official Codex Responses URL, and effective auth headers built from that credential. Suppress the subscription policy if configured headers override `Authorization` or `chatgpt-account-id`, matching names case-insensitively. Leave model discovery, login alias, API-key fallback, and session affinity unchanged. `openai-codex`, a prompt cache key, or a hostname alone is not evidence of auth.
2. Keep terminal event and usage decoding in `responses.lua:295`. Pass an optional provider-local observer through `responses.post_stream` to the decoder for this request, including its existing reasoning-summary compatibility retry. Invoke it only on the terminal event from the attempt whose result the decoder returns. It receives the event type and raw usage from that event; do not cache or reuse those fields across attempts. If the decoder returns an error, do not attach a report. The shared codec does not name a provider or pick a policy.
3. On the eligible Codex path, require a `response.completed` with no contradictory response status and a fresh positive integer `usage.input_tokens_details.cached_tokens` that fits the supported token count. Return `{ bound = "no_time_limit", prefix_tokens = cached_tokens }`. Zero, absent, malformed, or incomplete usage yields no report; cached-write counts cannot prove a read. Do not use deltas, aggregated usage, a prior response, or restored session data as evidence. The normal main-response boundary publishes the report before tools run. On resume, only a new inference response can recreate it.
4. Do not send `prompt_cache_options` or change `store`, affinity headers, prefix encoding, or tool dispatch. Keep usage accounting independent. The separate OpenAI API-key mapping uses its own policy, even when a ChatGPT refresh fails and the provider falls back to an API key.

### Phase 4: Managed Codex test fixture

Add a per-host scripted streaming send backend under the existing `makima-lua` `test-support` feature. Inject it into the real plugin host before loading builtins, using the in-memory host setup in `makima-lua/src/lib.rs:392` as the entry point. Branch in `makima-lua/src/api/net/send.rs:293` after the permission guard and `SendParams::from_table`, before calling `open`. Build the normal response userdata with a fixture-backed `AsyncBody` so its read and close behavior still run through the production Lua boundary. The fixture captures the original request and dequeues status, headers, and SSE bytes; fail on unmatched requests rather than contacting the network. Do not alter URL resolution, credentials, permission checks, or HTTP behavior in production. Do not add a runtime setting or a provider-specific test hook to Rust core.

Use the managed host and agent setup in `makima-lua/tests/runtime_alignment.rs:270`, replacing its fake Rust provider with the actual `providers` builtin through `PluginHost::provider_registry` and the normal provider preparation. Extend the public test-support helper in `makima-lua/src/lib.rs:419` to accept a caller-supplied state path and pass it to `PluginHost::with_fs_for_tests`. Use an isolated on-disk `StateDir` from a temporary directory for both hosts, while keeping the in-memory backend for plugin filesystem calls. Before loading the builtin, seed synthetic unexpired access and refresh tokens and an account ID with `makima_storage::auth::save_credentials` under `openai`, the registered provider ID. Use the same temporary state root on reopen. `ctx.credentials` bypasses the plugin filesystem (`makima-lua/src/credentials.rs:52`); do not write credentials into `InMemoryFs` or touch the operator's store. Register a real Lua tool that captures `Agent:status(ctx).cache` on entry. Recreate the host and managed actor from committed session history with the same session ID for the resume case, but do not copy actor cache state. Assert that the reopened actor has no report before inference and that a fresh positive SSE result reaches the tool. Capture the exact URL and auth headers to verify that the fixture exercises the official route without weakening its eligibility check.

### Phase 5: Bundled provider mappings

Gate reporting on the actual model, auth mode, endpoint or route, applied cache policy, and fresh usage. Put wire decoding in shared codecs where useful, but do not assign one service's retention policy to every caller of a codec. If a response only shows cache activity, keep the usage and omit `cache`.

| Provider or route | Reporting rule |
| --- | --- |
| OpenAI public API, modern models | In `openai.lua`, select only API-key requests actually sent to `api.openai.com/v1/responses` with the exact modern IDs in the bundled list: `gpt-5.6-{luna,terra,sol}`, `gpt-6-{astra,sol,luna}`, `gpt-6.1-{sol,terra}`, and `gpt-6.2-{astra,sol}`. Under the unchanged implicit policy, a positive `input_tokens_details.cached_tokens` or `cache_write_tokens_details.ttl_30m_tokens` yields a 1,800-second `minimum` from attempt start. Use the read count for prefix length on a read-only response; omit the length when write totals do not locate the covered prefix. Do not infer 12-hour coverage or refresh it on a 30-minute read. [Policy](https://developers.openai.com/api/docs/guides/prompt-caching#cache-lifetime). |
| OpenAI public API, older models | For exact `gpt-5.5` or `gpt-5.5-pro` on the official API-key path, a positive read in the protocol's `cached_tokens` field yields `up_to`, 86,400 seconds from response receipt, with the read count as prefix length. The guide documents only the `24h` policy for those IDs. Other older models are `nil`: the request does not establish which policy applied. |
| Direct ChatGPT OAuth | The exact official Codex Responses route uses Phase 3. Other OAuth requests and custom bases are `nil` on current evidence. Apply API-key rules only to an actual API-key request on the documented public endpoint. |
| Direct Anthropic | In `anthropic_provider.lua`, require the official `api.anthropic.com/v1/messages` path, a Claude wire model after suffix normalization, only the bundled untiered `ephemeral` controls, and fresh positive `cache_read_input_tokens` or `cache_creation_input_tokens`. Use `minimum`, 300 seconds from the successful request start. With a pure read use the read prefix length; otherwise leave it absent unless coverage is explicit. Omit the report for unexpected mixed-tier evidence or a custom base. [Policy](https://platform.claude.com/docs/en/build-with-claude/prompt-caching). |
| Bedrock | In `bedrock.lua`, require the official AWS runtime and the actual native Invoke model ID or one of its documented regional inference profiles for Claude Sonnet 4.6, after environment overrides. Start with the IDs in the [Sonnet 4.6 model card](https://docs.aws.amazon.com/bedrock/latest/userguide/model-card-anthropic-claude-sonnet-4-6.html); unknown IDs or custom runtimes are `nil`. With the bundled untiered `ephemeral` controls and a positive native Messages read or write counter from EventStream, report `minimum`, 300 seconds from successful invoke start. Apply the same prefix-length rule as direct Anthropic. Do not use Converse counters. [Policy](https://docs.aws.amazon.com/bedrock/latest/userguide/prompt-caching.html). |
| Vertex and Gemini implicit | `nil`. `vertex.lua` uses Google's `streamGenerateContent`, not Claude on Vertex. Implicit cache usage is not evidence of a lifetime; explicit cached-content objects are not used here. [Policy](https://ai.google.dev/gemini-api/docs/caching). |
| Claude Agent SDK | `nil` on the current bridge. Its aggregate usage has neither effective tier nor request-start evidence. [Claude Code's policy](https://code.claude.com/docs/en/prompt-caching) can change with request purpose or usage credits. Do not infer the tier from plan or model labels. |
| OpenRouter | `nil` on the current route. `openrouter.lua` supplies a request slug and a generic ephemeral marker, and the decoder does not establish the serving backend and policy for the reported count. Do not treat the requested vendor as the serving vendor. [Route-specific rules](https://openrouter.ai/docs/guides/best-practices/prompt-caching). |
| DeepSeek, Synthetic, remaining bundled providers and generic compatible endpoints | `nil` without an established bound. DeepSeek's [best-effort cleanup window](https://api-docs.deepseek.com/guides/kv_cache/) and Synthetic's [cached-input pricing](https://synthetic.new/pricing/models) do not establish one. Preserve observed token usage. |

## Acceptance Criteria

- AC.1: All three report variants cross the Lua provider boundary with the documented fields. Missing metadata remains compatible. An invalid field drops only the report, logs the field and provider, and does not change output, usage, or stop reason.
- AC.2: A tool handler can read the preceding model response's report from `Agent:status(ctx)` while its turn is still active. A later main response replaces or clears it. Elapsed deadlines remain visible and failed retries do not rebase them.
- AC.3: A committed model switch on the same conversation ID, a rewind, an applied compaction, and active cancellation that replaces history clear the report. Isolated requests and unapplied compactions do not overwrite it. An actor restored in a fresh runtime has no report in v1. A cancelled or superseded response cannot overwrite a cleared report or a successor's report. A later request from the same turn on a superseded model cannot publish. After an in-turn compaction on the admitted model, the next request can publish against the new history revision. Both headless control replies observe cleared state on success.
- AC.4: A fresh positive cached-input count on a completed direct Codex subscription response reaches a managed Lua tool as `no_time_limit` before dispatch. Zero, missing, malformed, incomplete, or stale usage produces no report. Report selection is isolated across refresh and fallback; API-key, custom-endpoint, and auth-header override paths cannot produce the subscription report. A resumed session starts without a report and can acquire one on a fresh hit.
- AC.5: Provider fixtures report documented bounds only when policy and usage evidence support them. Unknown routes and best-effort caches preserve usage without a deadline. Captured requests show no new TTL opt-in, cache controls, or change to existing direct OpenAI routing.
- AC.6: Generated Lua and provider docs match the contract, including mid-turn reads and the distinction between a bound and a cache-hit prediction.
- AC.7: The managed fixture loads the bundled provider and captures its unchanged request URL and OAuth headers, streams through the real Lua response interface, and rejects unplanned requests without using the network.

## Test Strategy

| Criterion | Named verification |
| --- | --- |
| AC.1 | `provider_cache_report_round_trips` and `invalid_provider_cache_does_not_drop_response` in `makima-lua/tests/provider_api.rs`; exercise absent metadata, past deadlines, malformed values, nonfinite number input, and unexpected deadlines on `no_time_limit`. Capture the structured warning. |
| AC.2 | `tool_reads_current_model_cache_report` using a registered Lua provider and a real managed tool handler. `cache_report_replacement_preserves_provider_deadline` uses controlled response times and a retry; a subsequent successful response without metadata clears the state. |
| AC.3 | `cache_report_invalidates_with_main_history` covers the named actor transitions and unrelated requests, including successful control and queued compaction independently and preserving the report on unapplied compaction. `restored_actor_does_not_restore_cache_report` exercises a new actor with restored history. `retired_cache_response_cannot_publish` gates an old response on a channel until cancellation or an identity commit wins, then releases it before and after a successor publishes. `old_model_turn_cannot_publish_after_model_commit` starts another request from the same admitted turn after a model switch and asserts it cannot publish. `same_turn_can_publish_after_history_replacement` asserts an in-turn compaction clears the old report and a subsequent request on the same admitted model publishes. `control_compaction_reply_observes_cleared_cache` reads status immediately after each successful headless control reply; a failed or rolled-back control retains the report. |
| AC.4 | `direct_codex_cache_report_requires_completed_oauth_response` in `plugins/providers/tests/openai.lua` runs the registered provider against scripted SSE. Cover a positive read, zero or absent reads, malformed counts, `response.incomplete`, contradictory status on `response.completed`, custom endpoint, header overrides, and OAuth 401 with both successful refresh and rejected refresh followed by API-key fallback. Use a positive count on the fallback fixture to catch attribution to the wrong auth. `responses_summary_retry_observes_returned_attempt_only` in `plugins/providers/tests/responses.lua` scripts the summary-refusal retry with and without eligible terminal usage. Assert one observer call for the accepted attempt and no retained report when retry usage is absent. `direct_codex_tool_reads_fresh_cache_report` uses the Phase 4 fixture to assert the tool sees the report on entry and that a later response without one clears it. `restored_direct_codex_tool_reads_only_fresh_hit` recreates the host and actor, checks the initial absence, then reads a new report inside the real tool after fresh scripted SSE. `live_direct_codex_cache_smoke` checks fresh and resumed requests on the existing OAuth path with authorized credentials. |
| AC.5 | `openai_public_modern_cache_minimum`, `openai_public_gpt55_cache_upper_bound`, `openai_cache_bounds_follow_live_route`, `anthropic_cache_bound_from_attempt_start`, `bedrock_invoke_cache_bound_for_verified_model`, and `unknown_cache_ttl_preserves_usage` in provider specs. Capture requests and use a fake clock. Negative cases include non-Codex OAuth and custom routes, mixed writes, unsupported IDs, search/routed backends, unobservable tiers, and zero activity. |
| AC.6 | `generated_prompt_cache_contract_includes_provider_and_plugin_docs` checks the generated Lua references and bundled provider docs for the report variants, deadline semantics, Codex mode distinction, and a mid-turn example that treats the bound as an observation. Run `just gen-docs-check` for drift. |
| AC.7 | `managed_send_fixture_preserves_route_and_rejects_unscripted_requests` loads the bundled provider, checks the exact Codex URL and synthetic bearer/account headers, verifies fixture SSE reaches the decoder, then issues an unexpected request and checks the fixture error. |

Run scoped checks first, then use the repository's remote build workflow for full `just ci`. The automated tests use no live credentials.

For `live_direct_codex_cache_smoke`, use `makima auth login openai-codex` and an eligible `openai/` model. Keep the model, prompt, tool set, and Makima session unchanged. Establish an eligible stable prefix, repeat a request that calls a diagnostic Lua tool, and verify the tool reads `no_time_limit` and a positive `prefix_tokens` during the turn. Close Makima, reopen the same session, confirm the report has not been restored, and verify a new matching request can produce a fresh hit and report. If practical, repeat after more than 30 minutes of inactivity; record the delay and hit or miss, without treating one result as proof of retention or expiry. Disclose if authorized credentials or a positive hit were unavailable. Do not log credentials or request content.

## Review Strategy

Use the canonical plan reviewer on the final handoff. Resolve or explicitly rebut findings. After any critical or high finding, run another plan review and require `VERDICT: pass` before handoff or `plan_submit` when in plan mode.

After automated checks, have a separate agent review the implementation. Review the provider contract and actor publication independently from the adapters. Check validation, owner fencing, live-route selection, response usage attribution, cancellation, and provider policy sources. Fix or rebut findings and repeat independent review after critical findings until none remain or an operator decision blocks progress.

## Documentation Strategy

Update the generated `makima.api.register_provider` and `Agent:status(ctx)` references and the bundled provider docs. Include a mid-turn plugin example and explain `nil`, expired finite deadlines, and `no_time_limit`. Document which direct Codex requests qualify and that no local Codex executable is required. Distinguish the subscription policy from the public API-key TTL and from unsupported custom endpoints. Run `just gen-docs` and `just gen-docs-check`. Do not add a second reference for the same API fields.

## Risks, Blockers, and Required Decisions

- OpenAI does not return a TTL for direct Codex usage. The `no_time_limit` mapping relies on the logged-in product announcement and corroborating client behavior. It applies only to the matched official route with observed fresh cache reads. The smoke check requires an authorized ChatGPT login and validates interoperability, not indefinite retention. If the direct route cannot be verified live, record that limitation instead of claiming it passed.
- Provider support can change by model, billing path, and route. The API permits absent reports; do not fill coverage gaps with typical durations or undocumented defaults. The actor cannot verify the next request's resolved prefix while idle.
- Persistence is intentionally deferred only until the storage rework can carry this data. Preserve the original absolute deadline and identity when implementing it. Longer-TTL configuration, TTL-dependent pricing changes, and timed compaction remain out of scope.
