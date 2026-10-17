use std::sync::Arc;
use std::time::Duration;

use futures_lite::future::race;
use maki_agent::{CurrentManagedTurn, PromptWaitError, TurnCancellationReason, TurnId, TurnTicket};
use maki_lua_macro::{lua_class, lua_fn};
use mlua::{Lua, Table, UserDataRef};
use smol::Timer;

use super::authority::{authorize, observe};
use super::projection::result_table;
use super::{AgentRuntime, LuaAgentTurn, session_state};
use crate::agent_errors::{SELF_TURN_WAIT, TURN_WAIT_TIMEOUT};
use crate::api::agent::LuaActorState;
use crate::api::util::ctx::LuaCtx;
use crate::api::util::pair::{Pair, err_pair, try_pair};
use crate::host_root::UNSUPPORTED_OPERATION_ERROR;
use crate::runtime::CANCELLED_MSG;

const UNSETTLED_TURN_RESULT: &str = "turn has no settled result";

/// Read the stable turn ID.
/// @return (string?, string?) Turn ID.
#[lua_fn(name = "id")]
fn turn_id(_lua: &Lua, this: &LuaAgentTurn) -> mlua::Result<Pair<String>> {
    Ok((Some(this.ticket.turn_id().to_string()), None))
}

/// Read the target agent ID.
/// @return (string?, string?) Agent ID.
#[lua_fn(name = "agent_id")]
fn turn_agent_id(_lua: &Lua, this: &LuaAgentTurn) -> mlua::Result<Pair<String>> {
    Ok((Some(this.agent.identity()), None))
}

fn turn_result(lua: &Lua, ticket: &TurnTicket) -> mlua::Result<Pair<Table>> {
    let result = try_pair!(ticket.peek_result().ok_or(UNSETTLED_TURN_RESULT));
    Ok((Some(result_table(lua, &result)?), None))
}

/// Read a retained result without waiting or consuming it.
/// @param ctx LuaCtx Invocation context.
/// @return (table?, string?) Nil without error while pending.
#[lua_fn(name = "result")]
fn ticket_result(
    lua: &Lua,
    this: &LuaAgentTurn,
    ctx: UserDataRef<LuaCtx>,
) -> mlua::Result<Pair<Table>> {
    try_pair!(observe(lua, &ctx, &this.agent));
    if this.ticket.peek().is_none() {
        return Ok((None, None));
    }
    turn_result(lua, &this.ticket)
}

/// A wait in the parent's own turn hands the result to its model, which makes
/// the queued delivery redundant. Plain result reads leave the delivery alone.
fn withdraw_delivery(active: Option<&CurrentManagedTurn>, turn_id: TurnId) {
    if let Some(parent) = active.and_then(|active| active.manager().actor(active.agent_id()).ok()) {
        parent.withdraw_child_delivery(turn_id);
    }
}

/// Wait for this exact turn. Timeout stops waiting, not the agent. A wait that
/// returns a result in the recorded parent's own turn withdraws the queued
/// child-result delivery for that turn.
/// @param ctx LuaCtx Invocation context.
/// @param opts table? timeout is a nonnegative integer number of seconds. Zero returns nil, nil while pending; positive timeout returns nil and "agent turn wait timed out" without cancelling work.
/// @return (table?, string?) Per-turn result or error.
#[lua_fn(name = "wait")]
pub(super) async fn ticket_wait(
    lua: Lua,
    this: UserDataRef<LuaAgentTurn>,
    ctx: UserDataRef<LuaCtx>,
    opts: Option<Table>,
) -> mlua::Result<Pair<Table>> {
    let active = try_pair!(observe(&lua, &ctx, &this.agent));
    let agent = this.agent.clone();
    let ticket = this.ticket.clone();
    let retained_ticket = ticket.clone();
    let timeout = opts
        .map(|opts| opts.get::<Option<u64>>("timeout"))
        .transpose()?
        .flatten();
    if ticket.peek().is_some() {
        withdraw_delivery(active.as_ref(), ticket.turn_id());
        return turn_result(&lua, &ticket);
    }
    if timeout == Some(0) {
        return Ok((None, None));
    }
    drop(this);
    if let Some(active) = &active {
        if Some(active.agent_id()) == agent.id {
            return Ok(err_pair(SELF_TURN_WAIT));
        }
        let actor = try_pair!(agent.manager.actor(agent.id));
        let wait = try_pair!(active.lease().observe_descendant(
            active,
            try_pair!(agent.id.ok_or(UNSUPPORTED_OPERATION_ERROR)),
            &actor,
            ticket,
            timeout.map(Duration::from_secs)
        ));
        try_pair!(wait.wait().await.map_err(|error| match error {
            PromptWaitError::Timeout => TURN_WAIT_TIMEOUT,
            PromptWaitError::Cancelled => CANCELLED_MSG,
        }));
    } else {
        let released = release_caller_permit(&lua, &ctx);
        let waited = wait_unmanaged(ticket, timeout).await;
        if let Some(state) = released {
            try_pair!(state.reacquire_turn_permit(&ctx.cancel).await);
        }
        try_pair!(waited);
    }
    try_pair!(ctx.validate_origin(&lua));
    withdraw_delivery(active.as_ref(), retained_ticket.turn_id());
    turn_result(&lua, &retained_ticket)
}

/// A turn on the plugin semaphore keeps its permit while it runs. Waiting on
/// a child with the permit held could leave every permit with a waiter and
/// none for the children, so the wait releases it until the child settles.
fn release_caller_permit(lua: &Lua, ctx: &LuaCtx) -> Option<Arc<LuaActorState>> {
    let caller = ctx.agent()?.execution_agent_id?;
    let state = session_state(lua, caller)?;
    state.release_turn_permit().then_some(state)
}

async fn wait_unmanaged(ticket: TurnTicket, timeout: Option<u64>) -> Result<(), &'static str> {
    let Some(seconds) = timeout else {
        ticket.wait().await;
        return Ok(());
    };
    let settled = async {
        ticket.wait().await;
        Ok(())
    };
    let expired = async {
        Timer::after(Duration::from_secs(seconds)).await;
        Err(TURN_WAIT_TIMEOUT)
    };
    race(settled, expired).await
}

/// Cancel only this admitted turn.
/// @param ctx LuaCtx Invocation context.
/// @return (boolean?, string?) Success or error.
#[lua_fn(name = "cancel")]
fn ticket_cancel(
    lua: &Lua,
    this: &LuaAgentTurn,
    ctx: UserDataRef<LuaCtx>,
) -> mlua::Result<Pair<bool>> {
    try_pair!(authorize(lua, &ctx, &this.agent));
    match (&this.agent.manager, this.agent.id) {
        (AgentRuntime::Managed(manager), Some(id)) => {
            try_pair!(manager.cancel_turn(id, this.ticket.turn_id(), TurnCancellationReason::User))
        }
        _ => {
            let actor = try_pair!(this.agent.manager.actor(this.agent.id));
            try_pair!(actor.cancel_turn(this.ticket.turn_id()));
        }
    }
    Ok((Some(true), None))
}

lua_class! {
    /// An exact-turn ticket with repeatable retained result reads. Dropping it does not cancel work.
    /// Settled results remain readable with current valid authority after lookup eviction
    /// or agent close. Result fields: agent_id, turn_id, status ("completed", "failed",
    /// or "cancelled"), text (last assistant message, including partial failure text),
    /// duration_ms (immutable elapsed milliseconds for this turn), usage,
    /// input_tokens, output_tokens (per-turn counters), output (captured array),
    /// origin. usage contains input_tokens, output_tokens, cache_creation_input_tokens
    /// and cache_read_input_tokens; top-level input_tokens includes cached tokens.
    /// captured is the last output when present. Failed results include error,
    /// user_message and retryable; cancelled results include cancellation_reason.
    /// origin contains kind ("user", "plugin", or "internal"), plugin?,
    /// plugin_generation?, source_agent?, source_turn?. It conveys provenance only.
    "maki.agent.AgentTurn" => LuaAgentTurn, TURN_DOCS [
        turn_id, turn_agent_id, ticket_result, ticket_wait, ticket_cancel,
    ]
}
