# Provider request delivery marker API extension

## Goal

Track whether a conversation request was delivered to the provider's server before the stream ended. Conversation lineage and cleanup must refer only to conversation IDs whose requests were delivered.

The current host calls `History::mark_sent()` before `Provider::stream_message()`. A configuration, preparation, connection, or authentication failure can therefore record a request that never reached the model endpoint. A successor can then name an unknown conversation in `invocation.previous`, and cleanup calls `end_conversation` for an ID the server never saw.

Completion of `stream_message()` is too late as the boundary. A provider can deliver a request and then lose the stream, return a malformed response, or receive cancellation. The host needs a signal during the stream call.

## Non-goals

- Exactly-once delivery across a network failure. The host cannot determine whether a server received a request when the connection fails before the server returns evidence.
- A generic event visible to frontends or users.
- Automatic delivery on every `ctx.send()` call. Provider callbacks can send authentication, metadata, discovery, and usage requests before the model request.
- Changes to retry classification or retry budgets.
- Provider-specific interpretation of SSE frames by the host.

## Terms

A request is delivered when the provider's server holds it, or when the provider decides to treat it that way. The provider chooses the boundary in manual mode. The host chooses it in auto mode.

A conversation is delivered after any request under its `conversation_id` is delivered. Delivery is monotonic for that conversation.

An attempt is one `Provider::stream_message()` call identified by `(operation_id, attempt)`. Retries share an `operation_id` and `conversation_id`. A logical request is one `stream_with_retry()` call and all of its attempts.

Naming: the concept is always "request delivery". Rust types and functions use `RequestDelivery` / `request_delivery` / `request_delivered`, never bare `Delivery`. Bare "delivery" already means message delivery to agents (`maki-agent/src/actor/delivery.rs`, `DeliveryStatus`, inbox, the `maki.agent` Lua API).

## Lua API

A provider instance can set `request_delivery_marker`:

```lua
return {
  request_delivery_marker = "manual", -- absent or "auto": the default
  stream = function(ctx, request, emit)
    local token, err = refresh_token(ctx)
    if not token then
      return nil, err -- not delivered
    end
    if not ctx.mark_request_delivery() then
      return nil, CANCELLED -- the host retired this call: do not send
    end
    local response, send_err = ctx.send(model_request(token, request))
    if not response then
      return nil, send_err -- delivered: the server may hold it
    end
    return codec.stream(response, emit)
  end,
}
```

The accepted values are:

- Absent or `"auto"`: the request is delivered at the earliest of `ctx.mark_request_delivery()`, the first accepted `emit(...)` call, or a successful `stream` return.
- `"manual"`: the request is delivered at the earliest of `ctx.mark_request_delivery()` or a successful `stream` return. The host never marks delivery on an `emit`, before `stream`, or on a failed or cancelled return.

Any other value fails provider preparation with a configuration error.

This is an intended behavior change for every provider that does not set the field. Today the host marks the request sent before it calls `stream`. In auto mode a request that fails before the first event (a connection failure, or an error response with no events) is no longer delivered.

`ctx.mark_request_delivery()`:

- is available in both modes, so an auto provider can mark earlier than its first event;
- returns `true` when the request is now delivered or already was;
- returns `false` and records nothing once the host has retired the call (cancelled or dropped). The provider must not start the model request after a `false`;
- is idempotent: repeated calls are no-ops that return `true` while the call is alive;
- does not enter the provider event channel and never reaches a frontend;
- exists only on the `ctx` passed to `stream`, not in other provider callbacks.

A manual provider owns its boundary. It can mark before `ctx.send`, on a protocol frame of its own, after N events, or never. If it never marks, a successful return still delivers the request, which keeps a provider that is written wrong from completing a turn whose request stays unsent in persisted lineage. The host logs that fallback at `warn` with the provider id, conversation id and operation id, because a manual provider is expected to mark. A failed or cancelled return without a mark stays undelivered.

A manual provider that marks late accepts the risk: if the call is cancelled or fails between the server receiving the request and the mark, the server may hold state that the host never ends. The provider docs state this.

Auto mode is safe without code for every bundled provider except `claude_agent`. All of them emit only after the transport returns a response, so the first emit never comes before contact. A provider that emits locally made events before contacting its server must use manual mode or mark explicitly.

## Rust API

The provider boundary carries an owned, cloneable `RequestDelivery` handle that is separate from `ProviderEvent`. It is internal protocol state and must not be forwarded to frontends.

```rust
pub enum RequestDeliveryMarker {
    Auto,
    Manual,
}

#[derive(Clone)]
pub struct RequestDelivery(/* shared state */);

impl RequestDelivery {
    /// Returns false once the attempt has been retired.
    pub fn mark(&self) -> bool;
}
```

`stream_message()` takes the handle as a new argument, by value. The Lua adapter moves it into a `'static` job on the Lua thread (`maki-lua/src/provider.rs`, `invoke`), so a borrowed `&dyn` reporter does not work.

The shape must provide these properties:

- exactly one observable transition per logical request;
- no mutable borrow of `History` across the provider future: the handle only signals, and the history owner applies the mark;
- safe use from the Lua runtime thread;
- no frontend event;
- no delivery after the attempt has been retired. The retire check and the mark share one lock with the emitter's close (`CloseOnDrop`), so a mark is accepted only while the emitter is open, and `ctx.mark_request_delivery()` returns `false` after it closes;
- no cost on the text-delta path beyond one flag check for the first emit in auto mode.

`LuaProvider` reads `request_delivery_marker` at preparation. In auto mode the emitter marks delivery on its first accepted event. `ctx.mark_request_delivery()` maps to `RequestDelivery::mark`. The successful-return fallback lives on the host side, in `RequestDeliveryTracker::end_attempt(succeeded)`, so it covers every provider; the Lua adapter only logs the `warn` when a manual provider succeeds without a mark.

The owner side is `RequestDeliveryTracker`: it opens an attempt (`attempt(n)` returns the handle), ends it (`end_attempt(succeeded)` retires the handle and applies the success fallback), reports `is_delivered()`, and resolves `delivered()` once on the first delivery.

Every real provider is Lua (`src/architecture_tests.rs`). Rust `Provider` implementations are test providers. They can mark through the handle to cover either mode.

## Agent behavior

`Agent::turn` no longer calls `History::mark_sent()` before `stream_with_retry()`.

`stream_with_retry()` owns delivery across the logical request:

1. It creates one shared delivery state for the logical request.
2. Every attempt receives a `RequestDelivery` connected to that state and retired when the attempt ends.
3. The caller owns the `RequestDeliveryTracker` and passes it in, so it learns of the first delivery while the stream is still running (see below). Callers without a `History` (compaction summaries, isolated turns) read `is_delivered()` after the call.
4. Delivery stays true for all later retries and terminal outcomes.
5. A terminal failure or cancellation before delivery leaves `conversation.sent` unchanged. For the first request of a conversation that means `None`. For a later request it keeps the previous request's length.

`Agent::turn` marks sent live, at the moment of delivery:

1. Before the call it clones the `Arc` of `HistorySnapshot.messages`. The stream gets the cloned `Arc`, so the `History` is not borrowed during the stream.
2. `RequestDelivery::mark()` wakes a channel that `turn()` owns.
3. `turn()` races the stream future against that channel. On the first signal it calls the existing `History::mark_sent()`, which publishes to the mirror, and keeps polling the stream.
4. If the stream ends with delivery reported in its result but no signal handled yet (for example a successful-return fallback), `turn()` calls `mark_sent()` then.

`mark_sent()` records `self.len()`. That equals the request length because nothing changes the history between the request start and the end of the stream: `turn()` owns it and consumes the inbox before the call. A pending mark must therefore be applied before `turn()` pushes anything after the stream ends (the cancelled partial text or the response).

If a delivered attempt fails and the host retries, the conversation stays delivered. The next attempt uses the same conversation ID and a higher attempt number. The host does not retract delivery. A 401 that triggers key rotation after delivery follows the same rule.

Authentication recovery follows the same rule. A failure before delivery does not mark the request sent. An authentication error after delivery does. `wait_for_reauth` then starts a new logical request under the same conversation ID.

## Persistence timing

`sent` is published to the history mirror at the moment of delivery, during the stream. The UI checkpoints the mirror every frame and writes agent changes without delay (`maki-ui/src/app/session.rs`, `checkpoint_with`), so a delivered request reaches disk as fast as today's mark before the stream. A crash after delivery keeps `sent`. Headless mode persists at its existing points, which is the same durability as today.

## Conversation lineage

This behavior already exists and is kept. The change is only when `sent` is set.

`ConversationState::link` (`maki-storage/src/conversation.rs`) names the conversation only when `sent` is `Some`. An undelivered conversation passes its own `previous` link on, subject to the shared-prefix cap and relation composition.

Delivery state and the delivered request length persist through the existing `ConversationState.sent` field. No second persisted flag is needed.

## Conversation cleanup

The host calls `end_conversation` only for conversation IDs that were delivered.

`History::continue_conversation`, `History::restart_conversation` and `History::replace_compacted` return `Option<Arc<str>>`: the ended ID when `sent` was `Some`, else `None`. Every `end_conversation_detached` call site takes that option, so the check is in the type:

- cancellation (`maki-agent/src/agent/run.rs`, after `sanitize_cancelled_history`);
- rewind and clear (`maki-agent/src/actor/history_edit.rs`);
- headless reset (`maki-agent/src/headless.rs`);
- compaction of the agent history (`maki-agent/src/agent/compaction.rs`, `finish_compact`);
- session replacement (`maki-ui/src/event_loop.rs`, which already filters on `sent.is_some()`).

Compaction summaries and isolated turns do not use `History`. They use the delivery result of their own request:

- `summarize` gets the delivered flag from `stream_with_retry()` for each attempt and ends `summary_id` only when it is true;
- `run_isolated_turn` passes a `RequestDelivery` to its single `stream_message()` call and ends its conversation only when it was delivered.

The provider contract stays defensive: `end_conversation` must stay idempotent and accept unknown IDs. Races, old persisted sessions, and other hosts can still produce such calls.

## Bundled providers

- Every provider built on `http.send`, `http.post_stream` and the shared codecs stays in auto mode with no source change.
- `claude_agent` sets `request_delivery_marker = "manual"` and calls `ctx.mark_request_delivery()` before its `POST` to the steps path. The boundary follows the bridge (`claude-agent-bridge/src/conversations.ts`, `step` and `adopt`): as soon as a step arrives, the bridge registers the conversation under the new ID and can take over the predecessor's Claude session, removing the predecessor's record. That happens before the bridge answers 200, so a step it then refuses (for example a bad continuation in `plan`) still left state under the new ID. Only the two `conflict` checks refuse before any state changes. They are rare, and a mark for them is harmless because `end` accepts unknown IDs.

## Documentation

- The `register_provider` docs in `maki-lua/src/api/provider.rs` describe `request_delivery_marker`, `ctx.mark_request_delivery()`, the auto and manual rules, the successful-return fallback, retirement, and retry behavior. They state that a provider with server-side conversation state should mark before its model request.
- `plugins/providers/types.lua` annotates the field and the `ctx` function.
- Run `just gen-docs`.

## Error and cancellation cases

| Outcome | Auto: delivered | Manual, marked before send: delivered | Manual, never marked: delivered |
|---|---:|---:|---:|
| Configuration failure before model request | No | No | No |
| Failure before the model transport call | No | No | No |
| DNS or connection failure after the transport starts | No | Yes | No |
| HTTP auth or rate-limit error response, no events | No | Yes | No |
| Cancel before the provider reaches its boundary | No | No | No |
| Cancel while waiting for the first event | No | Yes | No |
| First event, then cancellation | Yes | Yes | No |
| First event, then dropped or malformed stream | Yes | Yes | No |
| Successful stream result | Yes | Yes | Yes |
| Retry fails before delivery, later retry delivers | Yes | Yes | Yes, if it succeeds |
| Retry delivers, a later retry marks again | Yes, once | Yes, once | n/a |
| Mark after the attempt was retired | Ignored, returns `false` | Ignored, returns `false` | n/a |

## Tests

### Host tests

- Nothing is marked sent before the provider's stream body runs.
- A failure before delivery leaves `conversation.sent` unchanged: `None` on the first request, the previous length on a later request.
- A cancellation before delivery leaves `conversation.sent` unchanged.
- A request that is delivered and then fails records the request length.
- A request that is delivered and then cancelled records the request length.
- After a mark and before the stream ends, the published mirror snapshot already carries `sent` for the request length.
- A successful stream without a mark records delivery and logs a `warn`.
- Repeated marks produce one state transition.
- A failed attempt before delivery followed by a delivered retry records one request.
- A delivered failed attempt followed by another retry does not change the recorded request length.
- A mark after the attempt was retired returns `false` and records nothing.
- A successor names a delivered cancelled or rewound conversation with the correct `shared` count.
- Cancellation, rewind, clear, headless reset, compaction and session replacement do not call `end_conversation` for an undelivered ID.
- The same operations call `end_conversation` once for a delivered ID.
- Compaction-summary and isolated-turn cleanup follow their own delivery state.

### Lua adapter tests

- `request_delivery_marker = "manual"` and `"auto"` decode, and absence means auto.
- Unknown values fail preparation.
- Auto mode: the first accepted `emit` marks delivery.
- Manual mode: `emit` does not mark delivery.
- `ctx.mark_request_delivery()` marks delivery in both modes, returns `true`, and is idempotent.
- `ctx.mark_request_delivery()` returns `false` after the Rust call has ended, including from a retained `ctx`.
- A successful return marks delivery in both modes.
- A failed return without a mark stays undelivered in manual mode.
- Marks never reach the provider event channel.

### Bundled provider tests

- `claude_agent`: delivery is marked before the steps request, and an unreachable bridge or a refused step after the mark still counts as delivered. A missing socket config does not.

## Implementation sequence

1. Make cleanup conditional on `sent`: `continue_conversation`, `restart_conversation` and `replace_compacted` return `Option<Arc<str>>`, and all `end_conversation_detached` call sites use it. Everything in this plan lands in one branch.
2. Add `RequestDelivery` and `RequestDeliveryMarker` to `maki-providers` and pass the handle through `Provider::stream_message()`. Update test providers.
3. Add `request_delivery_marker`, auto first-emit marking, `ctx.mark_request_delivery()`, and the successful-return fallback to `LuaProvider`, sharing the emitter's close lock.
4. Remove the `mark_sent` before `stream_with_retry()` in `Agent::turn`. Pass the request messages as a cloned `Arc`, race the stream against the delivery channel, and call `mark_sent()` on the first signal, before any push after the stream ends. Return delivery from `stream_with_retry()` and use it in `summarize` and `run_isolated_turn` cleanup.
5. Add retry, cancellation, lineage, and persistence tests.
6. Switch `claude_agent` to manual mode with an explicit mark.
7. Add Lua docs and `types.lua` annotations, and run `just gen-docs`.
8. Run `just bench -- <provider_perf filter>` (`maki-lua/benches/provider_perf.rs`) to confirm that the first-emit check adds no material per-chunk overhead.

## Acceptance criteria

- A failure before delivery cannot create a `previous` link to the failed conversation ID.
- A cancellation or stream failure after delivery preserves the sent boundary and lineage.
- Providers without `request_delivery_marker` use auto mode.
- A manual provider controls delivery fully, except that a successful return always delivers.
- `ctx.mark_request_delivery()` never records delivery after the attempt was retired, and returns `false` then.
- Marks never produce a frontend event.
- Retries keep one monotonic delivery state for the logical request.
- The host does not intentionally call `end_conversation` for an undelivered conversation.
- The provider contract still requires unknown-ID cleanup calls to be harmless.
- No Rust identifier for this feature uses bare `Delivery`.
- Generated Lua API documentation and `types.lua` describe the extension.
- `just ci` passes.
