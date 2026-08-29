use std::collections::VecDeque;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::time::Instant;

use serde_json::Value;
use tracing::{debug, error, warn};

use crate::mcp::{McpSession, TOOL_SEARCH_TOOL_NAME, UNKNOWN_MCP};
use crate::task_set::TaskSet;
use crate::tools::registry::{ToolInvocation, ToolRegistry};
use crate::tools::{LocalToolFn, ToolContext, truncate_line};
use crate::{AgentError, AgentEvent, ToolDoneEvent, ToolOutput, ToolStartEvent};
use maki_config::ToolKey;

#[derive(Clone, Copy)]
pub enum Emit {
    Notify,
    Silent,
}

const DOOM_LOOP_THRESHOLD: usize = 3;
const DOOM_LOOP_MESSAGE: &str = "You have called this tool with identical input 3 times in a row. You are stuck in a loop. Break out and try a different approach.";
const MCP_BLOCKED_IN_PLAN: &str = "MCP tools are not available in plan mode";
const UNKNOWN_TOOL_PREFIX: &str = "unknown tool";
const MCP_PERM_SCOPE_MAX_BYTES: usize = 200;

pub(super) struct RecentCalls(VecDeque<(String, u64)>);

impl RecentCalls {
    pub(super) fn new() -> Self {
        Self(VecDeque::new())
    }

    fn hash_input(input: &Value) -> u64 {
        let mut h = DefaultHasher::new();
        input.to_string().hash(&mut h);
        h.finish()
    }

    fn is_doom_loop(&self, name: &str, input: &Value) -> bool {
        let hash = Self::hash_input(input);
        self.0.len() >= DOOM_LOOP_THRESHOLD - 1
            && self
                .0
                .iter()
                .rev()
                .take(DOOM_LOOP_THRESHOLD - 1)
                .all(|(n, h)| n == name && *h == hash)
    }

    fn record(&mut self, name: String, input: &Value) {
        self.0.push_back((name, Self::hash_input(input)));
        if self.0.len() > DOOM_LOOP_THRESHOLD {
            self.0.pop_front();
        }
    }
}

/// Parse errors and unknown tools skip the start event so the UI never
/// shows a phantom spinner.
pub async fn run(
    registry: &ToolRegistry,
    mcp: Option<&McpSession>,
    id: String,
    name: &str,
    input: &Value,
    ctx: &ToolContext,
    emit: Emit,
) -> ToolDoneEvent {
    // Covers names re-entering from model JSON (batch children, `call_tool`,
    // the interpreter bridge); streamed names are canonicalized in streaming.rs.
    let name = super::streaming::canonical_tool_name(name);
    if let Some(local) = ctx.local_tools.get(name) {
        return run_local_tool(local, id, name, input, ctx, emit).await;
    }
    let entry = registry.get(name);
    // LLM providers send tool names in wire format (server__tool) but our
    // internal index uses server.tool. Only convert if the name isn't a
    // native tool — avoids mangling native names that happen to contain __.
    let mcp_name;
    let mcp_lookup = if entry.is_none() && name.contains("__") && mcp.is_some() {
        mcp_name = crate::mcp::internal_tool_name(name);
        mcp_name.as_str()
    } else {
        name
    };
    let tool_id: Arc<str> = entry
        .as_ref()
        .map(|e| Arc::from(e.tool.name()))
        .or_else(|| mcp.map(|m| m.interned_name(mcp_lookup)))
        .unwrap_or_else(|| Arc::from(UNKNOWN_MCP));
    let started = Instant::now();

    let done_error = |msg: String| ToolDoneEvent {
        id: id.clone(),
        tool: Arc::clone(&tool_id),
        output: ToolOutput::Plain(msg.into()),
        is_error: true,
        annotation: None,
        written_path: None,
    };

    if let Some(entry) = entry {
        let invocation = match entry.tool.parse(input) {
            Ok(inv) => inv,
            Err(e) => {
                warn!(
                    tool = %name,
                    source = %entry.source.as_log_field(),
                    input_preview = %crate::tools::schema::preview(&input.to_string()),
                    error = %e,
                    "tool input parse failed"
                );
                return done_error(e.to_string());
            }
        };

        if let Some(target) = invocation.mutable_path() {
            let restrict = ctx.restrict_write_to();
            let is_plan_target = restrict.as_deref().is_some_and(|pp| target == pp);
            if !is_plan_target {
                if restrict.is_some() {
                    warn!(
                        tool = %name,
                        target = %target.display(),
                        "blocked write in restricted mode"
                    );
                    return done_error(crate::tools::PLAN_WRITE_RESTRICTED.into());
                }
                if let Some(reason) = ctx.permissions.boundary_block_reason(target) {
                    return done_error(reason);
                }
            }
        }

        let header_result = invocation.start_header().await;
        let start = ToolStartEvent {
            id: id.clone(),
            tool: Arc::clone(&tool_id),
            summary: header_result.text(),
            render_header: header_result.snapshot(),
            annotation: invocation.start_annotation(),
            input: None,
            raw_input: Some(input.clone()),
            output: invocation.start_output(ctx),
        };
        if matches!(emit, Emit::Notify) {
            let _ = ctx.event_tx.send(AgentEvent::ToolStart(Box::new(start)));
        }

        invocation.start(ctx).await;

        if let Err(e) = enforce_permission(invocation.as_ref(), name, ctx, &id).await {
            return done_error(e);
        }

        // Serialize mutable-path mutations per normalized key: acquire the
        // gate immediately before execution so the whole read-modify-write
        // handler (including the Lua apply_edit read-record-write) is the
        // critical section, without serializing headers or permission
        // prompts. The execution context carries this dispatch's owner
        // appended to the inherited chain, so recursive same-path calls
        // from inside a locked handler are rejected instead of deadlocking.
        let locked = match invocation.mutable_path() {
            Some(target) => {
                let key = match crate::tools::file_locks::FileWriteLocks::lock_key(
                    &target.to_string_lossy(),
                ) {
                    Ok(key) => key,
                    Err(e) => return done_error(e),
                };
                match ctx
                    .file_write_locks
                    .acquire(key, &ctx.write_lock_chain, &ctx.cancel, ctx.deadline)
                    .await
                {
                    Ok(guard) => {
                        let mut chain = (*ctx.write_lock_chain).clone();
                        chain.push(guard.owner());
                        let mut exec_ctx = ctx.clone();
                        exec_ctx.write_lock_chain = Arc::new(chain);
                        Some((exec_ctx, guard))
                    }
                    Err(msg) => return done_error(msg),
                }
            }
            None => None,
        };

        let result = match locked {
            Some((exec_ctx, guard)) => {
                let result = invocation.execute(&exec_ctx).await;
                drop(guard);
                result
            }
            None => invocation.execute(ctx).await,
        };

        let elapsed = started.elapsed();
        match result.output {
            Ok(output) => {
                debug!(
                    tool = %name,
                    source = %entry.source.as_log_field(),
                    elapsed_ms = elapsed.as_millis() as u64,
                    "tool ok"
                );
                ToolDoneEvent {
                    id,
                    tool: tool_id,
                    output,
                    is_error: false,
                    annotation: result.annotation,
                    written_path: result.written_path,
                }
            }
            Err(message) => {
                warn!(
                    tool = %name,
                    source = %entry.source.as_log_field(),
                    elapsed_ms = elapsed.as_millis() as u64,
                    error = %message,
                    "tool failed"
                );
                done_error(message)
            }
        }
    } else if let Some(mcp) = mcp.filter(|_| name == TOOL_SEARCH_TOOL_NAME) {
        run_tool_search(mcp, id, input, ctx, emit)
    } else if mcp.is_some_and(|m| m.has_tool(mcp_lookup)) {
        emit_raw_start(
            ctx,
            emit,
            &id,
            &tool_id,
            format!("mcp: {mcp_lookup}"),
            input,
        );
        execute_mcp_tool(ctx, &id, tool_id, mcp_lookup, input).await
    } else {
        let msg = format!("{UNKNOWN_TOOL_PREFIX}: {mcp_lookup}");
        warn!(tool = %mcp_lookup, "unknown tool");
        done_error(msg)
    }
}

/// MCP, local, and search tools never go through invocation parsing,
/// so there is no parsed input to show; the UI gets the raw JSON instead.
fn emit_raw_start(
    ctx: &ToolContext,
    emit: Emit,
    id: &str,
    tool: &Arc<str>,
    summary: String,
    input: &Value,
) {
    if !matches!(emit, Emit::Notify) {
        return;
    }
    let start = ToolStartEvent {
        id: id.to_owned(),
        tool: Arc::clone(tool),
        summary,
        render_header: None,
        annotation: None,
        input: None,
        raw_input: Some(input.clone()),
        output: None,
    };
    let _ = ctx.event_tx.send(AgentEvent::ToolStart(Box::new(start)));
}

/// Runs without a permission gate: search only reveals names the deferred
/// catalog already showed the model.
fn run_tool_search(
    mcp: &McpSession,
    id: String,
    input: &Value,
    ctx: &ToolContext,
    emit: Emit,
) -> ToolDoneEvent {
    let tool_id: Arc<str> = Arc::from(TOOL_SEARCH_TOOL_NAME);
    let query = input["query"].as_str().unwrap_or_default();
    emit_raw_start(ctx, emit, &id, &tool_id, query.to_owned(), input);
    let (output, is_error) = match mcp.search_tools(query) {
        Ok(out) => (out, false),
        Err(e) => (e, true),
    };
    ToolDoneEvent {
        id,
        tool: tool_id,
        output: ToolOutput::Markdown(output.into()),
        is_error,
        annotation: None,
        written_path: None,
    }
}

async fn run_local_tool(
    local: &LocalToolFn,
    id: String,
    name: &str,
    input: &Value,
    ctx: &ToolContext,
    emit: Emit,
) -> ToolDoneEvent {
    let tool_id: Arc<str> = Arc::from(name);
    emit_raw_start(ctx, emit, &id, &tool_id, name.to_owned(), input);
    let tool_ctx = ToolContext {
        tool_use_id: Some(id.clone()),
        ..ctx.clone()
    };
    let (output, is_error) = match local(input.clone(), tool_ctx).await {
        Ok(output) => (output, false),
        Err(e) => {
            warn!(tool = %name, error = %e, "local tool failed");
            (e, true)
        }
    };
    ToolDoneEvent {
        id,
        tool: tool_id,
        output: ToolOutput::Plain(output.into()),
        is_error,
        annotation: None,
        written_path: None,
    }
}

/// Enforce permission for a native tool. MCP tools bypass this — they go
/// through `execute_mcp_tool` which handles permission checking internally.
///
/// Returns an error if `name` contains dots (not a valid native tool name).
async fn enforce_permission(
    inv: &dyn ToolInvocation,
    name: &str,
    ctx: &ToolContext,
    id: &str,
) -> Result<(), String> {
    if name.contains('.') {
        return Err(format!(
            "enforce_permission called with dotted name: {name}"
        ));
    }
    if let Some(scopes) = inv.permission_scopes().await {
        let tool_key = ToolKey::native(name);
        ctx.permissions
            .enforce(
                &tool_key,
                &scopes,
                &ctx.event_tx,
                ctx.user_response_rx.as_deref(),
                id,
                &ctx.cancel,
                ctx.restrict_write_to().as_deref(),
            )
            .await
            .map_err(|e| e.to_string())?;
    }
    Ok(())
}

async fn execute_mcp_tool(
    ctx: &ToolContext,
    id: &str,
    tool_id: Arc<str>,
    tool_name: &str,
    input: &Value,
) -> ToolDoneEvent {
    let done = |output: String, is_error: bool| ToolDoneEvent {
        id: id.to_owned(),
        tool: Arc::clone(&tool_id),
        output: ToolOutput::Plain(output.into()),
        is_error,
        annotation: None,
        written_path: None,
    };

    if ctx.mode.plan_path().is_some() {
        return done(MCP_BLOCKED_IN_PLAN.into(), true);
    }

    let perm_tool = match ToolKey::parse(tool_name) {
        Ok(k) => k,
        Err(e) => {
            return done(format!("invalid MCP tool key '{tool_name}': {e}"), true);
        }
    };
    let perm_scope = truncate_line(&input.to_string(), MCP_PERM_SCOPE_MAX_BYTES);
    let perm_scopes = crate::tools::PermissionScopes::single(perm_scope);

    if let Err(e) = ctx
        .permissions
        .enforce(
            &perm_tool,
            &perm_scopes,
            &ctx.event_tx,
            ctx.user_response_rx.as_deref(),
            id,
            &ctx.cancel,
            ctx.restrict_write_to().as_deref(),
        )
        .await
    {
        return done(e.to_string(), true);
    }

    let Some(mcp) = &ctx.mcp else {
        return done(format!("MCP manager not available for {tool_name}"), true);
    };

    // A permitted call to a deferred tool counts as loading it, so its full
    // definition joins the next request; a denied call must not load anything.
    mcp.mark_loaded(tool_name);
    match mcp.call_tool(tool_name, input).await {
        Ok(text) => done(text, false),
        Err(e) => done(e.to_string(), true),
    }
}

/// Deduplicates doom-loop repeats, then runs remaining calls in parallel.
pub(super) async fn process_tool_calls(
    response: maki_providers::StreamResponse,
    recent_calls: &mut RecentCalls,
    mcp: Option<&McpSession>,
    history: &mut super::history::History,
    event_tx: &crate::EventSender,
    ctx: &ToolContext,
) -> Result<(), AgentError> {
    let tool_uses: Vec<(String, String, Value)> = response
        .message
        .tool_uses()
        .map(|(id, name, input)| (id.to_owned(), name.to_owned(), input.clone()))
        .collect();

    history.push(response.message);

    let mut immediate_errors: Vec<ToolDoneEvent> = Vec::new();
    let mut runnable: Vec<(String, String, Value)> = Vec::new();

    for (id, name, input) in tool_uses {
        debug!(
            tool = %name,
            id = %id,
            input_preview = %crate::tools::schema::preview(&input.to_string()),
            "parsing tool call"
        );
        if recent_calls.is_doom_loop(&name, &input) {
            warn!(tool = %name, "doom loop detected, skipping execution");
            immediate_errors.push(ToolDoneEvent::error(id.clone(), DOOM_LOOP_MESSAGE));
        } else {
            runnable.push((id, name.clone(), input.clone()));
        }
        recent_calls.record(name, &input);
    }

    for err in &immediate_errors {
        event_tx.try_send(AgentEvent::ToolDone(Box::new(err.clone())));
    }

    let mut set = TaskSet::new();
    let mut spawned_ids: Vec<String> = Vec::new();
    for (id, name, input) in runnable {
        spawned_ids.push(id.clone());
        let event_tx_clone = ctx.event_tx.clone();
        let tool_ctx = ToolContext {
            tool_use_id: Some(id.clone()),
            ..ctx.clone()
        };
        let mcp_owned = mcp.cloned();
        set.spawn(async move {
            let done = run(
                &tool_ctx.registry,
                mcp_owned.as_ref(),
                id,
                &name,
                &input,
                &tool_ctx,
                Emit::Notify,
            )
            .await;
            event_tx_clone.try_send(AgentEvent::ToolDone(Box::new(done.clone())));
            done
        });
    }

    let results: Vec<ToolDoneEvent> = set
        .join_all()
        .await
        .into_iter()
        .zip(spawned_ids)
        .map(|(r, id)| match r {
            Ok(out) => out,
            Err(e) => {
                error!(error = %e, "tool task panicked");
                ToolDoneEvent::error(id, format!("internal error: tool panicked: {e}"))
            }
        })
        .collect();

    let mut all_results = results;
    all_results.extend(immediate_errors);
    let tool_msg = crate::types::tool_results(all_results);
    event_tx.send(AgentEvent::ToolResultsSubmitted {
        message: Box::new(tool_msg.clone()),
    })?;
    history.push(tool_msg);
    Ok(())
}

/// Test-only entry that skips native lookup, letting plan-mode and MCP tests
/// exercise the dispatch path without registering a fake native tool.
#[cfg(test)]
async fn dispatch_mcp(
    ctx: &ToolContext,
    id: &str,
    tool_name: &str,
    input: &Value,
) -> ToolDoneEvent {
    let tool_id = ctx
        .mcp
        .as_ref()
        .map(|m| m.interned_name(tool_name))
        .unwrap_or_else(|| Arc::from(UNKNOWN_MCP));
    execute_mcp_tool(ctx, id, tool_id, tool_name, input).await
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::Arc;

    use maki_config::{Effect, PermissionRule, PermissionsConfig, ToolKey};
    use tempfile::TempDir;
    use test_case::test_case;

    use super::*;
    use crate::AgentMode;
    use crate::permissions::{PERMISSION_DENIED_PREFIX, PermissionManager};
    use crate::tools::registry::ToolSource;
    use crate::tools::test_support::{GUARDED_TOOL_NAME, GuardedMock};

    fn recent_calls(entries: &[(&str, Value)]) -> RecentCalls {
        let mut rc = RecentCalls::new();
        for (n, v) in entries {
            rc.record(n.to_string(), v);
        }
        rc
    }

    #[test_case("read", &[("read", "/a"), ("read", "/a")], true  ; "triggers_at_threshold")]
    #[test_case("read", &[("read", "/a")],                 false ; "below_threshold")]
    #[test_case("read", &[("read", "/a"), ("read", "/b")], false ; "different_input_breaks_chain")]
    #[test_case("grep", &[("glob", "/a"), ("glob", "/a")], false ; "different_tool_name")]
    #[test_case("bash", &[("bash", "/a"), ("bash", "/b"), ("bash", "/a")], false ; "interrupted_chain")]
    fn doom_loop_detection(name: &str, history: &[(&str, &str)], expected: bool) {
        let entries: Vec<_> = history
            .iter()
            .map(|(n, p)| (*n, serde_json::json!({"path": p})))
            .collect();
        let input = serde_json::json!({"path": "/a"});
        assert_eq!(recent_calls(&entries).is_doom_loop(name, &input), expected);
    }

    fn local_ctx(
        name: &str,
        f: impl Fn(&Value) -> Result<String, String> + Send + Sync + 'static,
    ) -> ToolContext {
        let mut ctx = crate::tools::test_support::stub_ctx(&AgentMode::Build);
        let mut map = std::collections::HashMap::new();
        map.insert(
            name.to_owned(),
            crate::tools::local_tool(move |input, _ctx| {
                let result = f(&input);
                Box::pin(async move { result })
            }),
        );
        ctx.local_tools = Arc::new(map);
        ctx
    }

    #[test]
    fn local_tool_shadows_registry_and_maps_errors() {
        smol::block_on(async {
            let ctx = local_ctx("batch", |input| Ok(format!("local:{}", input["path"])));
            let done = run(
                ToolRegistry::global(),
                None,
                "t1".into(),
                "batch",
                &serde_json::json!({"path": "/a"}),
                &ctx,
                Emit::Silent,
            )
            .await;
            assert!(!done.is_error);
            assert_eq!(done.output.as_text(), r#"local:"/a""#);

            let ctx = local_ctx("boom", |_| Err("nope".into()));
            let done = run(
                ToolRegistry::global(),
                None,
                "t2".into(),
                "boom",
                &serde_json::json!({}),
                &ctx,
                Emit::Silent,
            )
            .await;
            assert!(done.is_error);
            assert_eq!(done.output.as_text(), "nope");
        });
    }

    #[test]
    fn functions_prefixed_name_dispatches_to_canonical_tool() {
        smol::block_on(async {
            let ctx = local_ctx("ok", |_| Ok("ran".into()));
            let done = run(
                ToolRegistry::global(),
                None,
                "t1".into(),
                "functions.ok",
                &serde_json::json!({}),
                &ctx,
                Emit::Silent,
            )
            .await;
            assert!(!done.is_error);
            assert_eq!(done.output.as_text(), "ran");
        });
    }

    #[test]
    fn local_tool_notify_emits_tool_start_with_raw_input() {
        smol::block_on(async {
            let (tx, rx) = flume::unbounded::<crate::Envelope>();
            let event_tx = crate::EventSender::new(tx, 0);
            let mut ctx =
                crate::tools::test_support::stub_ctx_with(&AgentMode::Build, Some(&event_tx), None);
            let mut map = std::collections::HashMap::new();
            map.insert(
                "local_echo".to_owned(),
                crate::tools::local_tool(|input, _ctx| {
                    let out = input.to_string();
                    Box::pin(async move { Ok(out) })
                }),
            );
            ctx.local_tools = Arc::new(map);

            let input = serde_json::json!({"path": "/a"});
            let done = run(
                ToolRegistry::global(),
                None,
                "t1".into(),
                "local_echo",
                &input,
                &ctx,
                Emit::Notify,
            )
            .await;
            assert!(!done.is_error);

            let envelope = rx
                .try_recv()
                .expect("ToolStart must be emitted before the tool completes");
            let AgentEvent::ToolStart(start) = envelope.event else {
                panic!("expected ToolStart, got {:?}", envelope.event);
            };
            assert_eq!(start.tool.as_ref(), "local_echo");
            assert_eq!(start.summary, "local_echo");
            assert_eq!(start.raw_input, Some(input));
        });
    }

    #[test]
    fn tool_search_routes_and_loads_matches() {
        smol::block_on(async {
            let mcp = crate::mcp::stub_session(&[("srv.fetch_issue", "Fetch a GitHub issue")]);
            let ctx = crate::tools::test_support::stub_ctx(&AgentMode::Build);
            let done = run(
                ToolRegistry::global(),
                Some(&mcp),
                "t1".into(),
                TOOL_SEARCH_TOOL_NAME,
                &serde_json::json!({"query": "issue"}),
                &ctx,
                Emit::Silent,
            )
            .await;
            assert!(!done.is_error, "got: {}", done.output.as_text());
            assert_eq!(done.tool.as_ref(), TOOL_SEARCH_TOOL_NAME);
            assert!(done.output.as_text().contains("srv__fetch_issue"));

            let mut tools = serde_json::json!([]);
            mcp.extend_tools(&mut tools);
            assert!(
                crate::mcp::tool_names(&tools).contains(&"srv__fetch_issue"),
                "searched tool must join the next request"
            );
        });
    }

    #[test_case(serde_json::json!({"query": "  "}) ; "blank_query")]
    #[test_case(serde_json::json!({}) ; "missing_query")]
    fn tool_search_bad_query_is_error_event(input: Value) {
        smol::block_on(async {
            let mcp = crate::mcp::stub_session(&[("srv.tool", "")]);
            let ctx = crate::tools::test_support::stub_ctx(&AgentMode::Build);
            let done = run(
                ToolRegistry::global(),
                Some(&mcp),
                "t1".into(),
                TOOL_SEARCH_TOOL_NAME,
                &input,
                &ctx,
                Emit::Silent,
            )
            .await;
            assert!(done.is_error);
            assert_eq!(done.output.as_text(), crate::mcp::SEARCH_EMPTY_QUERY);
        });
    }

    #[test]
    fn calling_deferred_mcp_tool_marks_it_loaded() {
        smol::block_on(async {
            let mcp = crate::mcp::stub_session(&[("srv.fetch_issue", "")]);
            let mut ctx = crate::tools::test_support::stub_ctx(&AgentMode::Build);
            ctx.mcp = Some(mcp.clone());
            let done = run(
                ToolRegistry::global(),
                Some(&mcp),
                "t1".into(),
                "srv__fetch_issue",
                &serde_json::json!({}),
                &ctx,
                Emit::Silent,
            )
            .await;
            assert_eq!(done.tool.as_ref(), "srv.fetch_issue", "must route to MCP");

            let mut tools = serde_json::json!([]);
            mcp.extend_tools(&mut tools);
            assert_eq!(
                crate::mcp::tool_names(&tools),
                vec!["srv__fetch_issue"],
                "called tool must join the next request"
            );
        });
    }

    #[test]
    fn denied_mcp_call_does_not_load_definition() {
        smol::block_on(async {
            let mcp = crate::mcp::stub_session(&[("srv.fetch_issue", "")]);
            let deny_cfg = PermissionsConfig {
                rules: vec![PermissionRule {
                    tool: ToolKey::parse("srv.fetch_issue").unwrap(),
                    scope: None,
                    effect: Effect::Deny,
                }],
                ..Default::default()
            };
            let dir = TempDir::new().unwrap();
            let permissions = Arc::new(PermissionManager::new(
                deny_cfg,
                dir.path().to_path_buf(),
                Arc::default(),
            ));
            let mut ctx = crate::tools::test_support::stub_ctx_with_permissions(
                &AgentMode::Build,
                permissions,
            );
            ctx.mcp = Some(mcp.clone());
            let done = run(
                ToolRegistry::global(),
                Some(&mcp),
                "t1".into(),
                "srv__fetch_issue",
                &serde_json::json!({}),
                &ctx,
                Emit::Silent,
            )
            .await;
            assert!(done.is_error);
            assert!(
                done.output.as_text().starts_with(PERMISSION_DENIED_PREFIX),
                "got: {}",
                done.output.as_text()
            );

            let mut tools = serde_json::json!([]);
            mcp.extend_tools(&mut tools);
            assert_eq!(
                crate::mcp::tool_names(&tools),
                vec![TOOL_SEARCH_TOOL_NAME],
                "denied call must not load the definition"
            );
        });
    }

    #[test]
    fn local_tool_named_tool_search_shadows_mcp_search() {
        smol::block_on(async {
            let mcp = crate::mcp::stub_session(&[("srv.tool", "")]);
            let ctx = local_ctx(TOOL_SEARCH_TOOL_NAME, |_| Ok("local wins".into()));
            let done = run(
                ToolRegistry::global(),
                Some(&mcp),
                "t1".into(),
                TOOL_SEARCH_TOOL_NAME,
                &serde_json::json!({"query": "tool"}),
                &ctx,
                Emit::Silent,
            )
            .await;
            assert_eq!(done.output.as_text(), "local wins");
        });
    }

    #[test]
    fn unknown_tool_returns_error_event() {
        smol::block_on(async {
            let ctx = crate::tools::test_support::stub_ctx(&AgentMode::Build);
            let done = run(
                &ctx.registry,
                None,
                "t1".into(),
                "nonexistent.tool",
                &serde_json::json!({}),
                &ctx,
                Emit::Silent,
            )
            .await;
            assert!(done.is_error);
            assert_eq!(done.tool.as_ref(), UNKNOWN_MCP);
            let text = done.output.as_text();
            assert!(text.starts_with(UNKNOWN_TOOL_PREFIX));
            assert!(text.contains("nonexistent.tool"));
        });
    }

    #[test]
    fn mcp_tool_blocked_in_plan_mode() {
        smol::block_on(async {
            let result = dispatch_mcp(
                &crate::tools::test_support::stub_ctx(&AgentMode::Plan(PathBuf::from(
                    "/tmp/plan.md",
                ))),
                "t1",
                "myserver.mytool",
                &serde_json::json!({}),
            )
            .await;
            assert!(result.is_error);
            assert_eq!(result.output.as_text(), MCP_BLOCKED_IN_PLAN);
        });
    }

    #[test]
    fn mcp_tool_errors_without_mcp_manager() {
        smol::block_on(async {
            let result = dispatch_mcp(
                &crate::tools::test_support::stub_ctx(&AgentMode::Build),
                "t1",
                "myserver.mytool",
                &serde_json::json!({}),
            )
            .await;
            assert!(result.is_error);
            assert!(result.output.as_text().contains("not available"));
        });
    }

    #[test]
    fn permission_denial_short_circuits_execute() {
        smol::block_on(async {
            let deny_cfg = PermissionsConfig {
                rules: vec![PermissionRule {
                    tool: ToolKey::native(GUARDED_TOOL_NAME),
                    scope: None,
                    effect: Effect::Deny,
                }],
                ..Default::default()
            };
            let dir = TempDir::new().unwrap();
            let permissions = Arc::new(PermissionManager::new(
                deny_cfg,
                dir.path().to_path_buf(),
                Arc::default(),
            ));
            let ctx = crate::tools::test_support::stub_ctx_with_permissions(
                &AgentMode::Build,
                permissions,
            );

            let registry = ToolRegistry::new();
            registry
                .register(
                    Arc::new(GuardedMock),
                    ToolSource::Lua {
                        plugin: "test".into(),
                    },
                )
                .unwrap();

            let done = run(
                &registry,
                None,
                "t1".into(),
                GUARDED_TOOL_NAME,
                &serde_json::json!({}),
                &ctx,
                Emit::Silent,
            )
            .await;

            assert!(done.is_error, "permission denial must produce error event");
            assert!(
                done.output.as_text().starts_with(PERMISSION_DENIED_PREFIX),
                "error should be the permission-denied message, got: {}",
                done.output.as_text()
            );
        });
    }

    const START_PROBE_NAME: &str = "start_probe";

    use std::sync::atomic::{AtomicBool, Ordering};

    use crate::tools::{
        BoxFuture, DescriptionContext, ExecFuture, HeaderFuture, HeaderResult, ParseError,
        PermissionScopes, Tool, ToolExecResult,
    };

    #[derive(Default)]
    struct StartProbe {
        started: Arc<AtomicBool>,
        executed: Arc<AtomicBool>,
    }

    struct StartProbeInvocation {
        started: Arc<AtomicBool>,
        executed: Arc<AtomicBool>,
    }

    impl ToolInvocation for StartProbeInvocation {
        fn start_header(&self) -> HeaderFuture {
            HeaderFuture::Ready(HeaderResult::plain("probe".into()))
        }
        fn start<'a>(&'a self, _ctx: &'a ToolContext) -> BoxFuture<'a, ()> {
            self.started.store(true, Ordering::SeqCst);
            Box::pin(std::future::ready(()))
        }
        fn permission_scopes(&self) -> BoxFuture<'_, Option<PermissionScopes>> {
            Box::pin(std::future::ready(Some(PermissionScopes::single(
                "probe".into(),
            ))))
        }
        fn execute<'a>(self: Box<Self>, _ctx: &'a ToolContext) -> ExecFuture<'a> {
            self.executed.store(true, Ordering::SeqCst);
            Box::pin(async {
                ToolExecResult::from(Ok::<_, String>(ToolOutput::Plain("ok".into())))
            })
        }
    }

    impl Tool for StartProbe {
        fn name(&self) -> &str {
            START_PROBE_NAME
        }
        fn description(&self, _ctx: &DescriptionContext) -> std::borrow::Cow<'_, str> {
            "start probe".into()
        }
        fn schema(&self) -> Value {
            serde_json::json!({"type": "object", "properties": {}, "additionalProperties": false})
        }
        fn parse(&self, _input: &Value) -> Result<Box<dyn ToolInvocation>, ParseError> {
            Ok(Box::new(StartProbeInvocation {
                started: Arc::clone(&self.started),
                executed: Arc::clone(&self.executed),
            }))
        }
    }

    /// A denied tool should still get its preview, but never its `execute`.
    #[test]
    fn start_runs_before_permission_denial_blocks_execute() {
        smol::block_on(async {
            let deny_cfg = PermissionsConfig {
                rules: vec![PermissionRule {
                    tool: ToolKey::native(START_PROBE_NAME),
                    scope: None,
                    effect: Effect::Deny,
                }],
                ..Default::default()
            };
            let dir = TempDir::new().unwrap();
            let permissions = Arc::new(PermissionManager::new(
                deny_cfg,
                dir.path().to_path_buf(),
                Arc::default(),
            ));
            let ctx = crate::tools::test_support::stub_ctx_with_permissions(
                &AgentMode::Build,
                permissions,
            );

            let probe = StartProbe::default();
            let (started, executed) = (Arc::clone(&probe.started), Arc::clone(&probe.executed));
            let registry = ToolRegistry::new();
            registry
                .register(
                    Arc::new(probe),
                    ToolSource::Lua {
                        plugin: "test".into(),
                    },
                )
                .unwrap();

            let done = run(
                &registry,
                None,
                "t1".into(),
                START_PROBE_NAME,
                &serde_json::json!({}),
                &ctx,
                Emit::Silent,
            )
            .await;

            assert!(done.is_error, "denial must error");
            assert!(
                started.load(Ordering::SeqCst),
                "start must run before permission enforcement"
            );
            assert!(
                !executed.load(Ordering::SeqCst),
                "execute must not run after denial"
            );
        });
    }

    // ---- write-lock dispatch tests ----------------------------------------

    use std::path::Path;
    use std::time::Duration;

    use crate::cancel::CancelToken;
    use crate::tools::file_locks::SAME_PATH_MUTATION_IN_PROGRESS;
    use crate::tools::{DEADLINE_EXCEEDED, Deadline};

    struct Gate {
        entered: flume::Sender<()>,
        entered_rx: flume::Receiver<()>,
        release: flume::Sender<()>,
        release_rx: flume::Receiver<()>,
        exited: flume::Sender<()>,
        exited_rx: flume::Receiver<()>,
    }

    impl Gate {
        fn new() -> Arc<Self> {
            let (entered, entered_rx) = flume::unbounded();
            let (release, release_rx) = flume::unbounded();
            let (exited, exited_rx) = flume::unbounded();
            Arc::new(Self {
                entered,
                entered_rx,
                release,
                release_rx,
                exited,
                exited_rx,
            })
        }

        async fn entered(&self) {
            let _ = self.entered_rx.recv_async().await;
        }

        fn try_entered(&self) -> bool {
            self.entered_rx.try_recv().is_ok()
        }

        async fn exited(&self) {
            let _ = self.exited_rx.recv_async().await;
        }

        fn release(&self) {
            self.release.send(()).ok();
        }
    }

    /// Mutable-path tool that parks inside its handler on a `Gate` until the
    /// test releases it, recording entry/exit. `fail` makes the handler
    /// return an error after release, exercising guard release on error.
    struct GatedWriteNamed {
        name: String,
        gate: Arc<Gate>,
        fail: bool,
    }

    struct GatedWriteInvocation {
        gate: Arc<Gate>,
        path: String,
        fail: bool,
    }

    impl ToolInvocation for GatedWriteInvocation {
        fn start_header(&self) -> HeaderFuture {
            HeaderFuture::Ready(HeaderResult::plain("gated".into()))
        }
        fn mutable_path(&self) -> Option<&Path> {
            Some(Path::new(&self.path))
        }
        fn execute<'a>(self: Box<Self>, _ctx: &'a ToolContext) -> ExecFuture<'a> {
            Box::pin(async move {
                self.gate.entered.send(()).ok();
                let _ = self.gate.release_rx.recv_async().await;
                self.gate.exited.send(()).ok();
                let output: Result<ToolOutput, String> = if self.fail {
                    Err("boom".into())
                } else {
                    Ok(ToolOutput::Plain("ok".into()))
                };
                ToolExecResult::from(output)
            })
        }
    }

    impl Tool for GatedWriteNamed {
        fn name(&self) -> &str {
            &self.name
        }
        fn description(&self, _ctx: &DescriptionContext) -> std::borrow::Cow<'_, str> {
            "gated write".into()
        }
        fn schema(&self) -> Value {
            serde_json::json!({
                "type": "object",
                "properties": { "path": { "type": "string" } },
                "additionalProperties": false
            })
        }
        fn parse(&self, input: &Value) -> Result<Box<dyn ToolInvocation>, ParseError> {
            Ok(Box::new(GatedWriteInvocation {
                gate: Arc::clone(&self.gate),
                path: input["path"].as_str().unwrap_or_default().to_owned(),
                fail: self.fail,
            }))
        }
    }

    fn register_gated_with(registry: &ToolRegistry, name: &str, gate: Arc<Gate>, fail: bool) {
        registry
            .register(
                Arc::new(GatedWriteNamed {
                    name: name.to_owned(),
                    gate,
                    fail,
                }),
                ToolSource::Lua {
                    plugin: "test".into(),
                },
            )
            .unwrap();
    }

    fn register_gated(registry: &ToolRegistry, name: &str, gate: Arc<Gate>) {
        register_gated_with(registry, name, gate, false);
    }

    fn register_failing_gated(registry: &ToolRegistry, name: &str, gate: Arc<Gate>) {
        register_gated_with(registry, name, gate, true);
    }

    async fn dispatch_gated(
        registry: Arc<ToolRegistry>,
        ctx: ToolContext,
        id: String,
        name: String,
        path: String,
    ) -> ToolDoneEvent {
        run(
            &registry,
            None,
            id,
            &name,
            &serde_json::json!({ "path": path }),
            &ctx,
            Emit::Silent,
        )
        .await
    }

    #[test]
    fn cloned_tool_contexts_share_write_locks() {
        let ctx = crate::tools::test_support::stub_ctx(&AgentMode::Build);
        let cloned = ctx.clone();
        assert!(
            Arc::ptr_eq(&ctx.file_write_locks, &cloned.file_write_locks),
            "cloned contexts must share one lock registry"
        );
        assert_eq!(
            (*ctx.write_lock_chain).len(),
            0,
            "root contexts start with an empty owner chain"
        );
    }

    #[test]
    fn same_path_mutations_are_serialized() {
        smol::block_on(async {
            let ctx = crate::tools::test_support::stub_ctx(&AgentMode::Build);
            let registry = Arc::new(ToolRegistry::new());
            let gate_a = Gate::new();
            let gate_b = Gate::new();
            register_gated(&registry, "gated_a", Arc::clone(&gate_a));
            register_gated(&registry, "gated_b", Arc::clone(&gate_b));

            let a = smol::spawn(dispatch_gated(
                Arc::clone(&registry),
                ctx.clone(),
                "a".into(),
                "gated_a".into(),
                "/shared".into(),
            ));
            gate_a.entered().await;

            let b = smol::spawn(dispatch_gated(
                Arc::clone(&registry),
                ctx.clone(),
                "b".into(),
                "gated_b".into(),
                "/shared".into(),
            ));
            for _ in 0..10 {
                smol::future::yield_now().await;
            }
            assert!(
                !gate_b.try_entered(),
                "second same-path call entered while the first holds the lock"
            );

            gate_a.release();
            let done_a = a.await;
            gate_a.exited().await;
            assert!(!done_a.is_error, "first call: {}", done_a.output.as_text());

            gate_b.entered().await;
            gate_b.release();
            let done_b = b.await;
            gate_b.exited().await;
            assert!(!done_b.is_error, "second call: {}", done_b.output.as_text());
        });
    }

    #[test]
    fn different_paths_do_not_share_a_lock() {
        smol::block_on(async {
            let ctx = crate::tools::test_support::stub_ctx(&AgentMode::Build);
            let registry = Arc::new(ToolRegistry::new());
            let gate_a = Gate::new();
            let gate_b = Gate::new();
            register_gated(&registry, "gated_a", Arc::clone(&gate_a));
            register_gated(&registry, "gated_b", Arc::clone(&gate_b));

            let a = smol::spawn(dispatch_gated(
                Arc::clone(&registry),
                ctx.clone(),
                "a".into(),
                "gated_a".into(),
                "/a".into(),
            ));
            let b = smol::spawn(dispatch_gated(
                Arc::clone(&registry),
                ctx.clone(),
                "b".into(),
                "gated_b".into(),
                "/b".into(),
            ));
            gate_a.entered().await;
            gate_b.entered().await;

            gate_a.release();
            gate_b.release();
            let done_a = a.await;
            let done_b = b.await;
            gate_a.exited().await;
            gate_b.exited().await;
            assert!(!done_a.is_error);
            assert!(!done_b.is_error);
        });
    }

    #[test]
    fn path_aliases_share_write_lock() {
        smol::block_on(async {
            let dir = tempfile::tempdir().unwrap();
            let target = dir.path().join("target.txt");
            std::fs::write(&target, "payload").unwrap();
            let target_s = target.to_string_lossy().into_owned();
            let dot_alias = dir
                .path()
                .join(".")
                .join("target.txt")
                .to_string_lossy()
                .into_owned();
            let dotdot_alias = dir
                .path()
                .join("missing")
                .join("..")
                .join("target.txt")
                .to_string_lossy()
                .into_owned();

            let ctx = crate::tools::test_support::stub_ctx(&AgentMode::Build);
            let registry = Arc::new(ToolRegistry::new());
            for (i, alias) in [target_s.clone(), dot_alias, dotdot_alias]
                .iter()
                .enumerate()
            {
                let gate_a = Gate::new();
                let gate_b = Gate::new();
                register_gated(
                    &registry,
                    &format!("gated_alias_{i}_a"),
                    Arc::clone(&gate_a),
                );
                register_gated(
                    &registry,
                    &format!("gated_alias_{i}_b"),
                    Arc::clone(&gate_b),
                );

                let a = smol::spawn(dispatch_gated(
                    Arc::clone(&registry),
                    ctx.clone(),
                    format!("a{i}"),
                    format!("gated_alias_{i}_a"),
                    alias.to_owned(),
                ));
                gate_a.entered().await;
                let b = smol::spawn(dispatch_gated(
                    Arc::clone(&registry),
                    ctx.clone(),
                    format!("b{i}"),
                    format!("gated_alias_{i}_b"),
                    target_s.clone(),
                ));
                for _ in 0..10 {
                    smol::future::yield_now().await;
                }
                assert!(
                    !gate_b.try_entered(),
                    "alias {alias:?} must share the lock with {target_s:?}"
                );
                gate_a.release();
                let done_a = a.await;
                gate_a.exited().await;
                assert!(!done_a.is_error);
                gate_b.entered().await;
                gate_b.release();
                let done_b = b.await;
                gate_b.exited().await;
                assert!(!done_b.is_error);
            }

            #[cfg(unix)]
            {
                let alias = dir.path().join("link.txt");
                if std::os::unix::fs::symlink(&target, &alias).is_ok() {
                    let alias_s = alias.to_string_lossy().into_owned();
                    let gate_a = Gate::new();
                    let gate_b = Gate::new();
                    register_gated(&registry, "gated_sym_a", Arc::clone(&gate_a));
                    register_gated(&registry, "gated_sym_b", Arc::clone(&gate_b));

                    let a = smol::spawn(dispatch_gated(
                        Arc::clone(&registry),
                        ctx.clone(),
                        "sym_a".into(),
                        "gated_sym_a".into(),
                        alias_s.clone(),
                    ));
                    gate_a.entered().await;
                    let b = smol::spawn(dispatch_gated(
                        Arc::clone(&registry),
                        ctx.clone(),
                        "sym_b".into(),
                        "gated_sym_b".into(),
                        target_s.clone(),
                    ));
                    for _ in 0..10 {
                        smol::future::yield_now().await;
                    }
                    assert!(
                        !gate_b.try_entered(),
                        "symlink alias {alias_s:?} must share the lock with {target_s:?}"
                    );
                    gate_a.release();
                    let done_a = a.await;
                    gate_a.exited().await;
                    assert!(!done_a.is_error);
                    gate_b.entered().await;
                    gate_b.release();
                    let done_b = b.await;
                    gate_b.exited().await;
                    assert!(!done_b.is_error);
                } else {
                    eprintln!("skipping symlink alias case: symlink creation unavailable");
                }
            }
        });
    }

    #[test]
    fn write_lock_reusable_after_waiter_cancellation() {
        smol::block_on(async {
            let base = crate::tools::test_support::stub_ctx(&AgentMode::Build);
            let registry = Arc::new(ToolRegistry::new());
            let gate_a = Gate::new();
            let gate_b = Gate::new();
            let gate_c = Gate::new();
            register_gated(&registry, "gated_a", Arc::clone(&gate_a));
            register_gated(&registry, "gated_b", Arc::clone(&gate_b));
            register_gated(&registry, "gated_c", Arc::clone(&gate_c));

            let a = smol::spawn(dispatch_gated(
                Arc::clone(&registry),
                base.clone(),
                "a".into(),
                "gated_a".into(),
                "/same".into(),
            ));
            gate_a.entered().await;

            let (trigger_b, token_b) = CancelToken::new();
            let mut ctx_b = base.clone();
            ctx_b.cancel = token_b;
            let b = smol::spawn(dispatch_gated(
                Arc::clone(&registry),
                ctx_b.clone(),
                "b".into(),
                "gated_b".into(),
                "/same".into(),
            ));
            for _ in 0..10 {
                smol::future::yield_now().await;
            }
            trigger_b.cancel();
            let done_b = b.await;
            assert!(done_b.is_error);
            assert_eq!(done_b.output.as_text(), "cancelled");
            assert!(!gate_b.try_entered(), "cancelled waiter must not enter");

            gate_a.release();
            let done_a = a.await;
            gate_a.exited().await;
            assert!(!done_a.is_error);

            let c = smol::spawn(dispatch_gated(
                Arc::clone(&registry),
                base.clone(),
                "c".into(),
                "gated_c".into(),
                "/same".into(),
            ));
            gate_c.entered().await;
            gate_c.release();
            let done_c = c.await;
            gate_c.exited().await;
            assert!(!done_c.is_error, "registry must be reusable after cancel");
        });
    }

    #[test]
    fn write_lock_reusable_after_waiter_timeout() {
        smol::block_on(async {
            let base = crate::tools::test_support::stub_ctx(&AgentMode::Build);
            let registry = Arc::new(ToolRegistry::new());
            let gate_a = Gate::new();
            let gate_b = Gate::new();
            let gate_c = Gate::new();
            register_gated(&registry, "gated_a", Arc::clone(&gate_a));
            register_gated(&registry, "gated_b", Arc::clone(&gate_b));
            register_gated(&registry, "gated_c", Arc::clone(&gate_c));

            let a = smol::spawn(dispatch_gated(
                Arc::clone(&registry),
                base.clone(),
                "a".into(),
                "gated_a".into(),
                "/same".into(),
            ));
            gate_a.entered().await;

            let mut ctx_b = base.clone();
            ctx_b.deadline = Deadline::after(Duration::from_millis(40));
            let b = smol::spawn(dispatch_gated(
                Arc::clone(&registry),
                ctx_b.clone(),
                "b".into(),
                "gated_b".into(),
                "/same".into(),
            ));
            let done_b = b.await;
            assert!(done_b.is_error);
            assert_eq!(done_b.output.as_text(), DEADLINE_EXCEEDED);
            assert!(!gate_b.try_entered(), "timed-out waiter must not enter");

            gate_a.release();
            let done_a = a.await;
            gate_a.exited().await;
            assert!(!done_a.is_error);

            let c = smol::spawn(dispatch_gated(
                Arc::clone(&registry),
                base.clone(),
                "c".into(),
                "gated_c".into(),
                "/same".into(),
            ));
            gate_c.entered().await;
            gate_c.release();
            let done_c = c.await;
            gate_c.exited().await;
            assert!(!done_c.is_error, "registry must be reusable after timeout");
        });
    }

    #[test]
    fn write_lock_reusable_after_holder_error_or_existing_execution_cancel() {
        smol::block_on(async {
            let base = crate::tools::test_support::stub_ctx(&AgentMode::Build);
            let registry = Arc::new(ToolRegistry::new());
            let gate_a = Gate::new();
            let gate_b = Gate::new();
            let gate_c = Gate::new();
            register_gated(&registry, "gated_a", Arc::clone(&gate_a));
            register_gated(&registry, "gated_b", Arc::clone(&gate_b));
            register_gated(&registry, "gated_c", Arc::clone(&gate_c));

            // Holder errors: the guard must release on every return path.
            let gate_fail = Gate::new();
            register_failing_gated(&registry, "gated_fail", Arc::clone(&gate_fail));
            let a = smol::spawn(dispatch_gated(
                Arc::clone(&registry),
                base.clone(),
                "a".into(),
                "gated_fail".into(),
                "/same".into(),
            ));
            gate_fail.entered().await;
            gate_fail.release();
            let done_a = a.await;
            gate_fail.exited().await;
            assert!(done_a.is_error, "expected the holder to fail");
            assert_eq!(done_a.output.as_text(), "boom");

            let b = smol::spawn(dispatch_gated(
                Arc::clone(&registry),
                base.clone(),
                "b".into(),
                "gated_b".into(),
                "/same".into(),
            ));
            gate_b.entered().await;
            gate_b.release();
            let done_b = b.await;
            gate_b.exited().await;
            assert!(!done_b.is_error);

            // An execution that returns due to cancellation releases too.
            let (trigger_c, token_c) = CancelToken::new();
            let mut ctx_c = base.clone();
            ctx_c.cancel = token_c;
            let c = smol::spawn(dispatch_gated(
                Arc::clone(&registry),
                ctx_c.clone(),
                "c".into(),
                "gated_c".into(),
                "/same".into(),
            ));
            gate_c.entered().await;
            trigger_c.cancel();
            gate_c.release();
            let done_c = c.await;
            gate_c.exited().await;
            assert!(
                !done_c.is_error,
                "cancel during execution is execution-level"
            );

            let (_trigger_d, token_d) = CancelToken::new();
            let mut ctx_d = base.clone();
            ctx_d.cancel = token_d;
            let d = smol::spawn(dispatch_gated(
                Arc::clone(&registry),
                ctx_d.clone(),
                "d".into(),
                "gated_c".into(),
                "/same".into(),
            ));
            gate_c.entered().await;
            gate_c.release();
            let done_d = d.await;
            gate_c.exited().await;
            assert!(
                !done_d.is_error,
                "registry reusable after the previous holder"
            );
        });
    }

    const RECURSIVE_WRITE_NAME: &str = "recursive_write";
    const INNER_WRITE_NAME: &str = "inner_write";

    struct RecursiveWrite;

    struct RecursiveWriteInvocation {
        input: Value,
    }

    impl ToolInvocation for RecursiveWriteInvocation {
        fn start_header(&self) -> HeaderFuture {
            HeaderFuture::Ready(HeaderResult::plain("recursive".into()))
        }
        fn mutable_path(&self) -> Option<&Path> {
            self.input["path"].as_str().map(Path::new)
        }
        fn execute<'a>(self: Box<Self>, ctx: &'a ToolContext) -> ExecFuture<'a> {
            Box::pin(async move {
                let inner = run(
                    &ctx.registry,
                    None,
                    "inner".into(),
                    INNER_WRITE_NAME,
                    &self.input,
                    ctx,
                    Emit::Silent,
                )
                .await;
                let out = if inner.is_error {
                    Err(inner.output.as_text())
                } else {
                    Ok(inner.output)
                };
                ToolExecResult::from(out)
            })
        }
    }

    impl Tool for RecursiveWrite {
        fn name(&self) -> &str {
            RECURSIVE_WRITE_NAME
        }
        fn description(&self, _ctx: &DescriptionContext) -> std::borrow::Cow<'_, str> {
            "recursive write".into()
        }
        fn schema(&self) -> Value {
            serde_json::json!({
                "type": "object",
                "properties": { "path": { "type": "string" } },
                "additionalProperties": false
            })
        }
        fn parse(&self, input: &Value) -> Result<Box<dyn ToolInvocation>, ParseError> {
            Ok(Box::new(RecursiveWriteInvocation {
                input: input.clone(),
            }))
        }
    }

    /// The inner tool of the reentry probe: a plain mutable-path tool that
    /// acquires the same key only if the outer lock was released.
    struct InnerWrite;

    struct SimpleWriteInvocation {
        path: String,
    }

    impl ToolInvocation for SimpleWriteInvocation {
        fn start_header(&self) -> HeaderFuture {
            HeaderFuture::Ready(HeaderResult::plain("inner".into()))
        }
        fn mutable_path(&self) -> Option<&Path> {
            Some(Path::new(&self.path))
        }
        fn execute<'a>(self: Box<Self>, _ctx: &'a ToolContext) -> ExecFuture<'a> {
            Box::pin(async {
                ToolExecResult::from(Ok::<_, String>(ToolOutput::Plain("ok".into())))
            })
        }
    }

    impl Tool for InnerWrite {
        fn name(&self) -> &str {
            INNER_WRITE_NAME
        }
        fn description(&self, _ctx: &DescriptionContext) -> std::borrow::Cow<'_, str> {
            "inner write".into()
        }
        fn schema(&self) -> Value {
            serde_json::json!({
                "type": "object",
                "properties": { "path": { "type": "string" } },
                "additionalProperties": false
            })
        }
        fn parse(&self, input: &Value) -> Result<Box<dyn ToolInvocation>, ParseError> {
            Ok(Box::new(SimpleWriteInvocation {
                path: input["path"].as_str().unwrap_or_default().to_owned(),
            }))
        }
    }

    /// Two independent root contexts (fresh owner chains) that share one
    /// registry lock the same way a parent and a subagent do: they must
    /// serialize, never error.
    #[test]
    fn independent_root_contexts_share_write_locks_serialize() {
        smol::block_on(async {
            let base = crate::tools::test_support::stub_ctx(&AgentMode::Build);
            let registry = Arc::new(ToolRegistry::new());
            let gate_a = Gate::new();
            let gate_b = Gate::new();
            register_gated(&registry, "gated_a", Arc::clone(&gate_a));
            register_gated(&registry, "gated_b", Arc::clone(&gate_b));
            let mut ctx_a = base.clone();
            ctx_a.registry = Arc::clone(&registry);
            let ctx_b = ToolContext {
                registry: Arc::clone(&registry),
                file_write_locks: Arc::clone(&ctx_a.file_write_locks),
                write_lock_chain: Arc::new(Vec::new()),
                ..ctx_a.clone()
            };

            let a = smol::spawn(dispatch_gated(
                Arc::clone(&registry),
                ctx_a,
                "a".into(),
                "gated_a".into(),
                "/shared".into(),
            ));
            gate_a.entered().await;

            let b = smol::spawn(dispatch_gated(
                Arc::clone(&registry),
                ctx_b,
                "b".into(),
                "gated_b".into(),
                "/shared".into(),
            ));
            for _ in 0..10 {
                smol::future::yield_now().await;
            }
            assert!(
                !gate_b.try_entered(),
                "fresh root context must queue behind the other root"
            );

            gate_a.release();
            let done_a = a.await;
            gate_a.exited().await;
            assert!(!done_a.is_error);

            gate_b.entered().await;
            gate_b.release();
            let done_b = b.await;
            gate_b.exited().await;
            assert!(!done_b.is_error);
        });
    }

    #[test]
    fn same_path_reentry_returns_error() {
        smol::block_on(async {
            let mut ctx = crate::tools::test_support::stub_ctx(&AgentMode::Build);
            let registry = Arc::new(ToolRegistry::new());
            ctx.registry = Arc::clone(&registry);
            registry
                .register(
                    Arc::new(RecursiveWrite),
                    ToolSource::Lua {
                        plugin: "test".into(),
                    },
                )
                .unwrap();
            registry
                .register(
                    Arc::new(InnerWrite),
                    ToolSource::Lua {
                        plugin: "test".into(),
                    },
                )
                .unwrap();

            let done = run(
                &registry,
                None,
                "outer".into(),
                RECURSIVE_WRITE_NAME,
                &serde_json::json!({ "path": "/reentrant" }),
                &ctx,
                Emit::Silent,
            )
            .await;

            assert!(done.is_error, "reentry must surface as an error");
            assert!(
                done.output
                    .as_text()
                    .contains(SAME_PATH_MUTATION_IN_PROGRESS),
                "got: {}",
                done.output.as_text()
            );
        });
    }
}
