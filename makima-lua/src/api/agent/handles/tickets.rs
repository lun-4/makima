use std::{
    io::{self, ErrorKind},
    time::Duration,
};

use futures_lite::future::race;
use makima_agent::{
    CurrentManagedTurn, PromptWaitError, TurnCancellationReason, TurnId, TurnTicket,
};
use makima_lua_macro::{lua_class, lua_fn};
use mlua::{Lua, Table, UserDataRef};
use smol::Timer;

use super::authority::{authorize, observe};
use super::projection::result_table;
use super::{LuaAgent, LuaAgentTurn};
use crate::agent_errors::{SELF_TURN_WAIT, TURN_WAIT_TIMEOUT};
use crate::api::util::ctx::LuaCtx;
use crate::api::util::pair::{Pair, err_pair, try_pair};
use crate::orchestration;
use crate::runtime::CANCELLED_MSG;

const UNSETTLED_TURN_RESULT: &str = "turn has no settled result";

pub(super) fn retained_ticket(
    lua: &Lua,
    agent: &LuaAgent,
    turn: TurnId,
) -> Result<TurnTicket, String> {
    agent
        .manager
        .retained_turn_ticket_with(&agent.manager.reference(agent.id), turn, |reference| {
            let denied = || {
                io::Error::new(
                    ErrorKind::PermissionDenied,
                    "stored result is outside the authorized session",
                )
            };
            let root = agent.manager.root().map_err(|_| denied())?;
            let services = orchestration::services(lua).ok_or_else(denied)?;
            let template = services.template(&root).map_err(|_| denied())?;
            if template.session_id.as_ref().map(|session| session.id()) != Some(reference.session) {
                return Err(denied());
            }
            let name = reference.path.to_str().ok_or_else(denied)?;
            let store = template.offload.as_ref().ok_or_else(denied)?;
            store.read(name, reference.bytes)
        })
        .map_err(|error| error.to_string())
}

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
async fn ticket_wait(
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
        if active.agent_id() == agent.id {
            return Ok(err_pair(SELF_TURN_WAIT));
        }
        let actor = try_pair!(agent.manager.actor(agent.id));
        let wait = try_pair!(active.lease().observe_descendant(
            active,
            agent.id,
            &actor,
            ticket,
            timeout.map(Duration::from_secs)
        ));
        try_pair!(wait.wait().await.map_err(|error| match error {
            PromptWaitError::Timeout => TURN_WAIT_TIMEOUT,
            PromptWaitError::Cancelled => CANCELLED_MSG,
        }));
    } else {
        try_pair!(wait_ticket(ticket, timeout).await);
    }
    try_pair!(ctx.validate_origin(&lua));
    withdraw_delivery(active.as_ref(), retained_ticket.turn_id());
    turn_result(&lua, &retained_ticket)
}

async fn wait_ticket(ticket: TurnTicket, timeout: Option<u64>) -> Result<(), &'static str> {
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

/// Cancel this admitted turn. For a managed turn, also close the child
/// subtrees it delegated. UI cancellation leaves those child subtrees open.
/// @param ctx LuaCtx Invocation context.
/// @return (boolean?, string?) Success or error.
#[lua_fn(name = "cancel")]
fn ticket_cancel(
    lua: &Lua,
    this: &LuaAgentTurn,
    ctx: UserDataRef<LuaCtx>,
) -> mlua::Result<Pair<bool>> {
    try_pair!(authorize(lua, &ctx, &this.agent));
    try_pair!(this.agent.manager.cancel_turn(
        this.agent.id,
        this.ticket.turn_id(),
        TurnCancellationReason::User
    ));
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
    "makima.agent.AgentTurn" => LuaAgentTurn, TURN_DOCS [
        turn_id, turn_agent_id, ticket_result, ticket_wait, ticket_cancel,
    ]
}

#[cfg(all(test, unix))]
mod tests {
    use std::{fs, os::unix::fs::symlink, sync::Arc};

    use makima_agent::{
        AgentId, AgentLimits, AgentManagerHandle, AgentMode, AgentRef, DoneReason, HistorySnapshot,
        TurnId, TurnOutcome, TurnProvenance, TurnResult,
        journal::{JournalChange, JournalCommit, SessionJournal},
        tools::{ToolContext, offload::OffloadStore, test_support::stub_ctx},
    };
    use makima_storage::{
        id::{MakimaId, SessionRef},
        session_state::{
            MAX_INLINE_TURN_RESULT_BYTES, SessionStateView, StateChange, StoredAgentLifecycle,
            StoredAgentRecord, StoredPayload,
        },
    };
    use mlua::Lua;
    use serde_json::json;
    use test_case::test_case;

    use crate::{
        api::{
            agent::create_agent_table,
            fs::{FsBackendHandle, InMemoryFs},
            util::ctx::LuaCtx,
        },
        orchestration::{
            OrchestrationServices, OrchestrationServicesSlot, TrustedTarget,
            install_plugin_authority,
        },
        runtime::TaskScope,
    };

    const PLUGIN: &str = "retained_result_security";
    const UNAVAILABLE: &str = "turn_result_unavailable";
    const REPLACEMENT: &str = "replacement";
    const BODY: &str = "retained result";

    struct ResultServices(TrustedTarget);

    impl OrchestrationServices for ResultServices {
        fn resolve_target(
            &self,
            _: Option<&makima_commands::CommandInvocation>,
        ) -> Result<Option<TrustedTarget>, String> {
            Ok(Some(self.0.clone()))
        }

        fn template(&self, target: &AgentRef) -> Result<ToolContext, String> {
            if self.0.target.same_manager(target) {
                Ok(self.0.template.clone())
            } else {
                Err(UNAVAILABLE.into())
            }
        }
    }

    #[test_case(false, false, false; "text_artifact_without_fs_backend")]
    #[test_case(false, true, true; "output_artifact_with_fs_backend")]
    #[test_case(true, false, false; "directory_without_fs_backend")]
    #[test_case(true, true, true; "directory_with_fs_backend")]
    fn lua_retained_turn_rejects_symlink_substitution(
        directory: bool,
        output: bool,
        fs_backend: bool,
    ) {
        let dir = tempfile::tempdir().unwrap();
        let session = MakimaId::generate();
        let store = Arc::new(OffloadStore::for_session(
            dir.path(),
            &SessionRef::from(session),
        ));
        let agent = AgentId::generate();
        let turn = TurnId::generate();
        let text = BODY.repeat(MAX_INLINE_TURN_RESULT_BYTES);
        let result = TurnResult {
            outcome: TurnOutcome::completed(
                agent,
                turn,
                Default::default(),
                1,
                DoneReason::EndTurn,
            ),
            text: text.clone(),
            output: vec![json!({ "answer": text })],
            provenance: TurnProvenance::default(),
            duration_ms: 0,
        };
        let stored = result.to_stored(session, Some(&store)).unwrap();
        let reference = if output {
            match &stored.output {
                StoredPayload::Offloaded(reference) => reference,
                StoredPayload::Inline(_) => panic!("output must be offloaded"),
            }
        } else {
            match &stored.text {
                StoredPayload::Offloaded(reference) => reference,
                StoredPayload::Inline(_) => panic!("text must be offloaded"),
            }
        };
        let artifact = store.dir().join(&reference.path);
        let mut artifacts = Vec::new();
        if let StoredPayload::Offloaded(reference) = &stored.text {
            let path = store.dir().join(&reference.path);
            artifacts.push((path.clone(), fs::read(path).unwrap()));
        }
        if let StoredPayload::Offloaded(reference) = &stored.output {
            let path = store.dir().join(&reference.path);
            artifacts.push((path.clone(), fs::read(path).unwrap()));
        }
        let replacement = dir.path().join(REPLACEMENT);
        let (original, moved) = if directory {
            (store.dir().to_path_buf(), replacement)
        } else {
            (artifact.clone(), replacement)
        };
        fs::rename(&original, &moved).unwrap();
        symlink(&moved, &original).unwrap();
        let journal = Arc::new(SessionJournal::default());
        journal.bind_session(session).unwrap();
        journal
            .commit(JournalCommit {
                agents: vec![StoredAgentRecord {
                    id: agent,
                    parent: None,
                    depth: 0,
                    transcript_key: String::new(),
                    lifecycle: StoredAgentLifecycle::Open,
                    ancestry_unknown: false,
                    label: None,
                    spawned_by_tool_use_id: None,
                    spec: None,
                    restorable: Ok(()),
                    usage: Default::default(),
                    created_seq: 0,
                    updated_seq: 0,
                    legacy: false,
                }],
                changes: vec![JournalChange {
                    agent: Some(agent),
                    generation: 0,
                    change: StateChange::Turn {
                        turn,
                        result: stored,
                    },
                }],
                histories: [(agent, HistorySnapshot::new(Vec::new()))].into(),
                ..Default::default()
            })
            .unwrap();
        let before = journal.checkpoint_snapshot().unwrap();
        let view =
            SessionStateView::fold(&before.state, &before.agent_revisions, before.seq).unwrap();
        let manager = AgentManagerHandle::restore_graph(
            AgentLimits::default(),
            Arc::clone(&journal),
            view,
            before.histories.clone(),
        )
        .unwrap();
        let lua = Lua::new();
        if fs_backend {
            let fs = Arc::new(InMemoryFs::new());
            for (path, bytes) in artifacts {
                fs.seed(&path, bytes);
            }
            lua.set_app_data(FsBackendHandle(fs));
        }
        let mut template = stub_ctx(&AgentMode::Build);
        template.session_id = Some(SessionRef::from(session));
        template.offload = Some(Arc::clone(&store));
        let target = TrustedTarget {
            target: manager.reference(agent),
            template,
        };
        lua.set_app_data(OrchestrationServicesSlot(Some(Arc::new(ResultServices(
            target.clone(),
        )))));
        install_plugin_authority(&lua, Arc::from(PLUGIN));
        let _scope = TaskScope::detached(&lua);
        lua.globals()
            .set(
                "ctx",
                LuaCtx::trusted_context(&lua, target, Arc::from(PLUGIN)),
            )
            .unwrap();
        lua.globals()
            .set("agent_api", create_agent_table(&lua).unwrap())
            .unwrap();
        lua.globals().set("agent_id", agent.to_string()).unwrap();
        lua.globals().set("turn_id", turn.to_string()).unwrap();
        lua.globals().set("expected_text", result.text).unwrap();
        lua.load(format!(
            r#"
            local agent = assert(agent_api.get(ctx, agent_id))
            local ticket, err = agent:turn(ctx, turn_id)
            assert(ticket == nil and err:find({UNAVAILABLE:?}, 1, true), tostring(err))
            assert(not agent:is_live())
        "#
        ))
        .exec()
        .unwrap();
        assert_eq!(journal.checkpoint_snapshot().unwrap().state, before.state);
        assert_eq!(
            journal.checkpoint_snapshot().unwrap().agent_revisions,
            before.agent_revisions
        );
        fs::remove_file(&original).unwrap();
        fs::rename(&moved, &original).unwrap();
        lua.load("local agent = assert(agent_api.get(ctx, agent_id)); local ticket = assert(agent:turn(ctx, turn_id)); assert(assert(ticket:result(ctx)).text == expected_text)")
            .exec().unwrap();
    }
}
