use std::borrow::Cow;
use std::collections::hash_map::DefaultHasher;
use std::collections::{HashMap, HashSet, VecDeque};
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::{Value, json};
use tracing::{debug, error, warn};

use crate::mcp::{McpSession, TOOL_SEARCH_TOOL_NAME, UNKNOWN_MCP};
use crate::task_set::TaskSet;
use crate::tools::hook::{Authority, HookCall, HookStage, OUTPUT_IS_ERROR, OUTPUT_TEXT, Verdict};
use crate::tools::registry::{InstalledHook, RegisteredTool, ToolInvocation};
use crate::tools::{
    CallOrigin, Deadline, LocalTool, LocalToolFn, ToolAudience, ToolContext, TurnToolRoute,
    truncate_line,
};
use crate::{AgentError, AgentEvent, ToolDoneEvent, ToolOutput, ToolStartEvent};
use maki_config::{FILE_WRITE_TOOLS, ToolKey};
use maki_storage::id::SessionRef;

const DOOM_LOOP_THRESHOLD: usize = 3;
const DOOM_LOOP_MESSAGE: &str = "You have called this tool with identical input 3 times in a row. You are stuck in a loop. Break out and try a different approach.";
const UNKNOWN_TOOL_PREFIX: &str = "unknown tool";
const UNAVAILABLE_TOOL_PREFIX: &str = "tool not available for this agent";
const MCP_PERM_SCOPE_MAX_BYTES: usize = 200;

const SOURCE_NATIVE: &str = "native";
const SOURCE_LOCAL: &str = "local";
const SOURCE_MCP: &str = "mcp";
#[cfg(test)]
const SOURCE_UNKNOWN: &str = "unknown";

const ERROR_CANCELLED: &str = "cancelled";

/// The window a chain gets when the call carries no deadline of its own.
/// Generous, because a layer may shell out before it decides, but a layer that
/// parks and never comes back has to end somewhere short of "when the user
/// gives up".
const HOOK_CHAIN_MAX: Duration = Duration::from_secs(60);

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

/// Every tool call in maki lands here (native, Lua, MCP, subagents, batch
/// children), which makes it the one place telemetry has to wrap and the one
/// place [hooks] fire.
///
/// [hooks]: crate::tools::hook
pub async fn run(
    id: String,
    name: &str,
    input: &Value,
    ctx: &ToolContext,
    origin: CallOrigin,
) -> ToolDoneEvent {
    let resolved = resolve(ctx, name);
    let name = resolved.name;
    if matches!(resolved.route, Route::Unknown) {
        return run_inner(resolved, id, input, ctx, origin).await;
    }
    if let Err(reason) = authorize_mode(&resolved, input, ctx) {
        warn!(tool = %name, reason = %reason, "tool blocked by mode");
        return mode_denied(id, name, reason);
    }
    let hook = Hook::of(ctx, &resolved, origin);

    let verdict = match &hook {
        Some(hook) => hook.filter_input(&id, input).await,
        None => Verdict::Unchanged,
    };
    let input = match verdict {
        Verdict::Unchanged => Cow::Borrowed(input),
        Verdict::Replaced(value) => {
            debug!(tool = %name, "input hook rewrote the call");
            Cow::Owned(value)
        }
        Verdict::Denied(reason) => {
            warn!(tool = %name, reason = %reason, "input hook stopped the call");
            return ToolDoneEvent {
                id,
                tool: Arc::from(name),
                output: ToolOutput::Plain(reason.into()),
                is_error: true,
                annotation: None,
                written_path: None,
            };
        }
    };

    if let Err(reason) = authorize_mode(&resolved, &input, ctx) {
        return mode_denied(id, name, reason);
    }
    let mut done = run_inner(resolved, id, &input, ctx, origin).await;
    if let Some(hook) = &hook {
        hook.filter_output(&mut done).await;
    }
    done
}

/// The hook installed on this registry, bound to one call. `None` when nobody
/// installed one, or when the name routes nowhere to run.
///
/// The call id is handed to each stage instead of held, because `run_inner`
/// owns it in between.
struct Hook<'a> {
    installed: InstalledHook,
    ctx: &'a ToolContext,
    tool: &'a str,
    origin: CallOrigin,
    authority: Authority,
}

impl<'a> Hook<'a> {
    fn of(ctx: &'a ToolContext, resolved: &Resolved<'a>, origin: CallOrigin) -> Option<Self> {
        Some(Self {
            installed: ctx.registry.hook()?,
            ctx,
            tool: resolved.name,
            origin,
            authority: resolved.route.authority()?,
        })
    }

    async fn filter_input(&self, tool_id: &str, input: &Value) -> Verdict {
        if !self.installed.wraps(self.tool, HookStage::Input) {
            return Verdict::Unchanged;
        }
        let cancelled = Verdict::Denied(ERROR_CANCELLED.to_owned());
        self.fire(HookStage::Input, tool_id, input.clone(), cancelled)
            .await
    }

    /// Rewrites the finished event in place. Text and error flag move together,
    /// so a hook that cannot reach the text cannot flip the flag either.
    async fn filter_output(&self, done: &mut ToolDoneEvent) {
        if !self.installed.wraps(self.tool, HookStage::Output) {
            return;
        }
        let was_error = done.is_error;
        let Some(text) = done.output.filterable_text_mut() else {
            debug!(
                tool = %self.tool,
                "output hook skipped: this output renders from fields, not prose"
            );
            return;
        };
        let value = json!({ OUTPUT_TEXT: &*text, OUTPUT_IS_ERROR: was_error });
        let (rewritten, is_error) = match self
            .fire(HookStage::Output, &done.id, value, Verdict::Unchanged)
            .await
        {
            Verdict::Unchanged => return,
            // Nothing left to stop, so the reason becomes what the model reads.
            Verdict::Denied(reason) => (reason, true),
            Verdict::Replaced(value) => match value.get(OUTPUT_TEXT).and_then(Value::as_str) {
                Some(replaced) => (
                    replaced.to_owned(),
                    value
                        .get(OUTPUT_IS_ERROR)
                        .and_then(Value::as_bool)
                        .unwrap_or(was_error),
                ),
                None => {
                    warn!(
                        tool = %self.tool,
                        field = OUTPUT_TEXT,
                        "output hook replaced the output without a text field, leaving it alone"
                    );
                    return;
                }
            },
        };
        *text = rewritten;
        done.is_error = is_error;
        debug!(tool = %self.tool, "output hook rewrote the output");
    }

    /// Cancellation outranks a hook: nobody is left to read the verdict, so
    /// the wait ends with `on_cancel`.
    async fn fire(
        &self,
        stage: HookStage,
        tool_id: &str,
        value: Value,
        on_cancel: Verdict,
    ) -> Verdict {
        let call = HookCall {
            tool: self.tool,
            tool_id,
            session_id: self.ctx.session_id.as_ref().map(SessionRef::as_str),
            origin: self.origin,
            authority: self.authority,
            cancel: &self.ctx.cancel,
            deadline: self.window(),
        };
        self.ctx
            .cancel
            .race(self.installed.run(stage, value, &call))
            .await
            .unwrap_or(on_cancel)
    }

    /// Read when a stage fires, not once per call: the input chain and the tool
    /// spend from the same budget, so an output chain handed the entry-time
    /// answer would inherit a window the call already used up.
    fn window(&self) -> Instant {
        let cap = Instant::now() + HOOK_CHAIN_MAX;
        match self.ctx.deadline {
            Deadline::At(at) => at.min(cap),
            Deadline::None => cap,
        }
    }
}

/// Where a name goes for one context. Dispatch and telemetry read this one
/// answer, so a name can never be reported as one thing and run as another.
enum Route<'a> {
    Local(&'a LocalTool),
    Native(RegisteredTool),
    ToolSearch(&'a McpSession),
    Mcp(&'a McpSession, Arc<str>),
    Unknown,
}

impl Route<'_> {
    /// Registry tools report the plugin behind them, because "native" alone
    /// tells whoever reads the metric nothing.
    #[cfg(test)]
    fn source(&self) -> Cow<'static, str> {
        match self {
            Self::Native(entry) => entry.source.as_log_field(),
            Self::Local(_) => Cow::Borrowed(SOURCE_LOCAL),
            Self::Mcp(..) => Cow::Borrowed(SOURCE_MCP),
            Self::ToolSearch(_) => Cow::Borrowed(TOOL_SEARCH_TOOL_NAME),
            Self::Unknown => Cow::Borrowed(SOURCE_UNKNOWN),
        }
    }

    /// What hooking this call would lend. Every route answers here, so a new
    /// one cannot reach a hook without naming its price. Only a declared
    /// capability narrows it: reading undeclared as free is what would let an
    /// unprivileged plugin steer `batch` or `code_execution` into any tool it
    /// likes.
    fn authority(&self) -> Option<Authority> {
        match self {
            Self::Native(entry) => Some(
                entry
                    .tool
                    .required_permission()
                    .map_or(Authority::Unbounded, Authority::Capability),
            ),
            Self::ToolSearch(_) | Self::Local(_) | Self::Mcp(..) => Some(Authority::Unbounded),
            // A rewrite cannot make the name exist, so firing here would hand
            // out a tool's authority without the tool.
            Self::Unknown => None,
        }
    }
}

/// A canonical name paired with where it goes. Only [`resolve`] builds one, so
/// no caller can act on a name it forgot to canonicalize.
struct Resolved<'a> {
    name: &'a str,
    route: Route<'a>,
}

/// Precedence, highest first: client (ACP) tools, the registry, then MCP.
/// The context is the only input, because resolving against one session and
/// executing against another is how a tool escapes its audience.
fn resolve<'a>(ctx: &'a ToolContext, name: &'a str) -> Resolved<'a> {
    // Names coming back from model JSON (batch children, `call_tool`, the
    // interpreter bridge) never passed through streaming.rs, so clean up here.
    let name = super::streaming::canonical_tool_name(name);
    let route = if let Some(local) = ctx.local_tools.get(name) {
        Route::Local(local)
    } else if let Some(entry) = ctx.registry.get(name) {
        Route::Native(entry)
    } else if let Some(mcp) = ctx.mcp.as_ref() {
        match mcp.resolve(name) {
            Some(qualified) => Route::Mcp(mcp, qualified),
            None if name == TOOL_SEARCH_TOOL_NAME => Route::ToolSearch(mcp),
            None => Route::Unknown,
        }
    } else {
        Route::Unknown
    };
    Resolved { name, route }
}

const MODE_DENIED: &str = "tool not allowed in restricted mode";

fn mode_denied(id: String, name: &str, reason: String) -> ToolDoneEvent {
    ToolDoneEvent {
        id,
        tool: Arc::from(name),
        output: ToolOutput::Plain(reason.into()),
        is_error: true,
        annotation: None,
        written_path: None,
    }
}

fn binding_matches(resolved: &Resolved<'_>, ctx: &ToolContext) -> bool {
    let Some(pinned) = ctx.turn_bindings.get(resolved.name) else {
        return false;
    };
    let same_route = match (pinned, &resolved.route) {
        (TurnToolRoute::Local(expected), Route::Local(current)) => {
            Arc::ptr_eq(&expected.handler, &current.handler)
                && expected.audience == current.audience
        }
        (TurnToolRoute::Native(expected), Route::Native(current)) => {
            Arc::ptr_eq(&expected.tool, &current.tool)
        }
        (TurnToolRoute::Mcp(expected), Route::Mcp(mcp, qualified)) => {
            expected.qualified_name().as_ref() == qualified.as_ref()
                && mcp.binding_is_current(expected)
        }
        (TurnToolRoute::ToolSearch, Route::ToolSearch(_)) => true,
        _ => false,
    };
    same_route && ctx.resolve_turn_route(resolved.name).is_some()
}

fn mode_offers(ctx: &ToolContext) -> bool {
    if ctx.restrict_write_to().is_none() {
        return true;
    }
    // Lua registrations do not expose a host-verified bundled handler identity.
    // Names, kinds, permissions, and plugin owners are supplied by plugins.
    false
}

pub(crate) fn authorize_advertised(ctx: &ToolContext, name: &str) -> bool {
    let resolved = resolve(ctx, name);
    let Some(mode) = ctx.mode_def.as_ref() else {
        return false;
    };
    if mode
        .tools
        .as_ref()
        .is_some_and(|names| !names.iter().any(|allowed| allowed == resolved.name))
    {
        return false;
    }
    let available = match &resolved.route {
        Route::Native(entry) => {
            entry.tool.audience().contains(ctx.audience) && ctx.tool_filter.matches(resolved.name)
        }
        Route::Local(local) => local.audience.contains(ctx.audience),
        Route::Mcp(..) | Route::ToolSearch(_) => true,
        Route::Unknown => false,
    };
    available && binding_matches(&resolved, ctx) && mode_offers(ctx)
}

fn authorize_mode(
    resolved: &Resolved<'_>,
    _input: &Value,
    ctx: &ToolContext,
) -> Result<(), String> {
    let name = resolved.name;
    if !authorize_advertised(ctx, name) {
        return Err(if ctx.restrict_write_to().is_some() && !mode_offers(ctx) {
            format!("{MODE_DENIED}: {name}")
        } else {
            format!("{UNAVAILABLE_TOOL_PREFIX}: {name}")
        });
    }
    Ok(())
}

/// One callable name, as [`resolve`] would route it.
pub struct Callable {
    /// The name to dispatch. Always what `resolve` was asked, never an alias.
    pub name: String,
    /// A name a host that binds tools as identifiers can use, set only when
    /// `name` is not one already (MCP servers publish `srv__get-docs`). Call
    /// `name`, bind `alias`.
    pub alias: Option<String>,
    pub source: &'static str,
    /// The audience of whatever will run, not of whatever shares its name.
    pub audience: ToolAudience,
    /// Registry tools only. MCP and host tools publish their schema to the
    /// model in the request's tool array, so repeating it here would buy an
    /// allocation per call and nothing else.
    pub schema: Option<Value>,
}

/// Every name this context can dispatch, deduplicated in [`resolve`]'s
/// precedence, so an entry always describes the tool that a call to that name
/// would actually reach. It lives beside `resolve` so the two cannot drift into
/// answering differently.
///
/// Filtered by the same filter that built the request's tool array, so a name
/// the model never saw is not one a script can reach either. What is left is
/// the caller's own policy, read off `audience` (a sandbox wants
/// `INTERPRETER`).
///
/// Recompute per call: MCP republishes its index whenever a server comes or goes.
pub fn callable(ctx: &ToolContext) -> Vec<Callable> {
    let mut out: Vec<Callable> = Vec::new();
    let mut claimed: HashSet<String> = HashSet::new();
    // A name belongs to the first source dispatch would reach, claimed before
    // any filter runs: a registry tool this audience may not call still owns its
    // name, or MCP would publish a way around it.
    let mut claim = |name: &str, audience: ToolAudience| {
        let first = claimed.insert(name.to_owned());
        first && audience.contains(ctx.audience)
    };
    let entry_of = |name: &str, source, audience, schema| Callable {
        name: name.to_owned(),
        alias: None,
        source,
        audience,
        schema,
    };

    let mut local: Vec<(&String, &LocalTool)> = ctx.local_tools.iter().collect();
    local.sort_by(|a, b| a.0.cmp(b.0));
    for (name, tool) in local {
        if claim(name, tool.audience) && authorize_advertised(ctx, name) {
            out.push(entry_of(name, SOURCE_LOCAL, tool.audience, None));
        }
    }
    for entry in ctx.registry.iter().iter() {
        let audience = entry.tool.audience();
        if !claim(entry.name(), audience) || !authorize_advertised(ctx, entry.name()) {
            continue;
        }
        out.push(entry_of(
            entry.name(),
            SOURCE_NATIVE,
            audience,
            Some(entry.tool.schema()),
        ));
    }
    if let Some(mcp) = ctx.mcp.as_ref() {
        let mut names = mcp.wire_names();
        names.push(TOOL_SEARCH_TOOL_NAME.to_owned());
        names.sort();
        for name in names {
            // MCP has no audience system: a server is reachable or it is not,
            // and a session holding one already offers its tools to the model.
            if claim(&name, ToolAudience::all()) && authorize_advertised(ctx, &name) {
                out.push(entry_of(&name, SOURCE_MCP, ToolAudience::all(), None));
            }
        }
    }
    assign_aliases(&mut out);
    out
}

/// Fills in `alias` for names an identifier cannot hold. A collision (a server
/// publishing both `get-docs` and `get_docs`) leaves both aliases unset rather
/// than pointing one name at the other's tool.
fn assign_aliases(tools: &mut [Callable]) {
    let aliases: Vec<Option<String>> = tools.iter().map(|t| identifier_alias(&t.name)).collect();
    let mut claims: HashMap<String, usize> = HashMap::new();
    for claimant in tools
        .iter()
        .map(|t| t.name.clone())
        .chain(aliases.iter().flatten().cloned())
    {
        *claims.entry(claimant).or_default() += 1;
    }
    // An alias always claims itself once, and never its own name, or
    // `identifier_alias` would have declined it. A second claim is therefore
    // another tool's name or alias, and then neither of them may have it.
    for (tool, alias) in tools.iter_mut().zip(aliases) {
        if alias.as_deref().is_some_and(|a| claims[a] == 1) {
            tool.alias = alias;
        }
    }
}

fn identifier_alias(name: &str) -> Option<String> {
    let is_body = |c: char| c.is_ascii_alphanumeric() || c == '_';
    // A leading digit is not something substitution can fix without inventing a
    // character the model never saw.
    if name.is_empty() || name.starts_with(|c: char| c.is_ascii_digit()) {
        return None;
    }
    name.chars().any(|c| !is_body(c)).then(|| {
        name.chars()
            .map(|c| if is_body(c) { c } else { '_' })
            .collect()
    })
}

/// Pure router: every arm owns its own start event, permission gate and
/// telemetry, so adding a source never means editing another one's path.
async fn run_inner(
    resolved: Resolved<'_>,
    id: String,
    input: &Value,
    ctx: &ToolContext,
    origin: CallOrigin,
) -> ToolDoneEvent {
    let name = resolved.name;
    match resolved.route {
        Route::Local(local) => run_local_tool(&local.handler, id, name, input, ctx, origin).await,
        Route::Native(entry) => run_native_tool(entry, id, name, input, ctx, origin).await,
        Route::ToolSearch(mcp) => run_tool_search(mcp, id, input, ctx, origin),
        Route::Mcp(mcp, qualified) => {
            execute_mcp_tool(ctx, mcp, &id, qualified, input, origin).await
        }
        Route::Unknown => {
            warn!(tool = %name, "unknown tool");
            ToolDoneEvent {
                id,
                tool: Arc::from(UNKNOWN_MCP),
                output: ToolOutput::Plain(format!("{UNKNOWN_TOOL_PREFIX}: {name}").into()),
                is_error: true,
                annotation: None,
                written_path: None,
            }
        }
    }
}

/// Parse errors skip the start event so the UI never shows a phantom spinner.
async fn run_native_tool(
    entry: RegisteredTool,
    id: String,
    name: &str,
    input: &Value,
    ctx: &ToolContext,
    origin: CallOrigin,
) -> ToolDoneEvent {
    let tool_id: Arc<str> = Arc::from(entry.tool.name());
    let started = Instant::now();

    let done_error = |msg: String| ToolDoneEvent {
        id: id.clone(),
        tool: Arc::clone(&tool_id),
        output: ToolOutput::Plain(msg.into()),
        is_error: true,
        annotation: None,
        written_path: None,
    };

    if !entry.tool.audience().contains(ctx.audience) {
        warn!(tool = %name, audience = ?ctx.audience, "tool blocked by audience");
        return done_error(format!("{UNAVAILABLE_TOOL_PREFIX}: {name}"));
    }

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

    if let Some(target) = invocation.mutable_path(ctx) {
        let restrict = ctx.restrict_write_to();
        if restrict.is_some() {
            warn!(tool = %name, target = %target.display(), "blocked write in restricted mode");
            return done_error(crate::tools::PLAN_WRITE_RESTRICTED.into());
        }
        if let Some(reason) = ctx.permissions.boundary_block_reason(&target) {
            return done_error(reason);
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
    if origin.is_model() {
        let _ = ctx.event_tx.send(AgentEvent::ToolStart(Box::new(start)));
    }

    invocation.start(ctx).await;

    if !binding_matches(&resolve(ctx, name), ctx) {
        return done_error(format!("{UNAVAILABLE_TOOL_PREFIX}: {name}"));
    }
    if let Err(e) = enforce_permission(invocation.as_ref(), name, ctx, &id).await {
        return done_error(e);
    }

    if !binding_matches(&resolve(ctx, name), ctx) {
        return done_error(format!("{UNAVAILABLE_TOOL_PREFIX}: {name}"));
    }
    let locked = match invocation.mutable_path(ctx) {
        Some(target) => {
            let key = match crate::tools::file_locks::FileWriteLocks::lock_key(
                &target.to_string_lossy(),
                &ctx.cwd,
            ) {
                Ok(key) => key,
                Err(e) => return done_error(e),
            };
            match ctx
                .file_write_locks
                .acquire(
                    key.clone(),
                    &ctx.write_lock_chain,
                    &ctx.cancel,
                    ctx.deadline,
                )
                .await
            {
                Ok(guard) => {
                    if ctx.config.stale_read_check
                        && let Err(message) = ctx.file_tracker.check_before_edit(&key)
                    {
                        return done_error(message);
                    }
                    let mut chain = (*ctx.write_lock_chain).clone();
                    chain.push(guard.owner());
                    let mut exec_ctx = ctx.clone();
                    exec_ctx.write_lock_chain = Arc::new(chain);
                    Some((exec_ctx, guard, key))
                }
                Err(msg) => return done_error(msg),
            }
        }
        None => None,
    };

    if origin.is_model() {
        let _ = ctx
            .event_tx
            .send(AgentEvent::ToolExecutionStart { id: id.clone() });
    }

    if !binding_matches(&resolve(ctx, name), ctx) {
        return done_error(format!("{UNAVAILABLE_TOOL_PREFIX}: {name}"));
    }
    let result = match locked {
        Some((exec_ctx, guard, key)) => {
            let result = invocation.execute(&exec_ctx).await;
            if result.output.is_ok() {
                ctx.file_tracker.record_read(&key);
            }
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
}

/// MCP, local, and search tools never go through invocation parsing,
/// so there is no parsed input to show; the UI gets the raw JSON instead.
fn emit_raw_start(
    ctx: &ToolContext,
    origin: CallOrigin,
    id: &str,
    tool: &Arc<str>,
    summary: String,
    input: &Value,
) {
    if !origin.is_model() {
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
    origin: CallOrigin,
) -> ToolDoneEvent {
    let tool_id: Arc<str> = Arc::from(TOOL_SEARCH_TOOL_NAME);
    let query = input["query"].as_str().unwrap_or_default();
    emit_raw_start(ctx, origin, &id, &tool_id, query.to_owned(), input);
    let (output, is_error) = match ctx.turn_bindings.search_mcp_tools(mcp, query, origin) {
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
    origin: CallOrigin,
) -> ToolDoneEvent {
    let tool_id: Arc<str> = Arc::from(name);
    emit_raw_start(ctx, origin, &id, &tool_id, name.to_owned(), input);
    if origin.is_model() {
        let _ = ctx
            .event_tx
            .send(AgentEvent::ToolExecutionStart { id: id.clone() });
    }
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
    if let Some(scopes) = inv.permission_scopes(ctx.session_id.as_ref()).await {
        let scopes = if FILE_WRITE_TOOLS.contains(&name) {
            crate::tools::PermissionScopes {
                scopes: scopes
                    .scopes
                    .into_iter()
                    .map(|scope| ctx.resolve_path(&scope).unwrap_or(scope))
                    .collect(),
                force_prompt: scopes.force_prompt,
            }
        } else {
            scopes
        };
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
    mcp: &McpSession,
    id: &str,
    tool: Arc<str>,
    input: &Value,
    origin: CallOrigin,
) -> ToolDoneEvent {
    emit_raw_start(ctx, origin, id, &tool, format!("mcp: {tool}"), input);
    let done = |output: String, is_error: bool| ToolDoneEvent {
        id: id.to_owned(),
        tool: Arc::clone(&tool),
        output: ToolOutput::Plain(output.into()),
        is_error,
        annotation: None,
        written_path: None,
    };

    let perm_tool = match ToolKey::parse(&tool) {
        Ok(k) => k,
        Err(e) => {
            return done(format!("invalid MCP tool key '{tool}': {e}"), true);
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
            ctx.mode.plan_path(),
        )
        .await
    {
        return done(e.to_string(), true);
    }

    if ctx.resolve_turn_route(&tool).is_none() {
        return done(format!("{UNAVAILABLE_TOOL_PREFIX}: {tool}"), true);
    }
    if origin.is_model() {
        let _ = ctx
            .event_tx
            .send(AgentEvent::ToolExecutionStart { id: id.to_owned() });
    }

    // A permitted call counts as loading the tool, so its definition joins the
    // next request; a denied one must not load anything.
    let Some(TurnToolRoute::Mcp(binding)) = ctx.resolve_turn_route(&tool) else {
        return done(format!("{UNAVAILABLE_TOOL_PREFIX}: {tool}"), true);
    };
    mcp.mark_loaded(&tool, origin);
    match mcp.call_bound_tool(binding, input).await {
        Ok(text) => done(text, false),
        Err(e) => done(e.to_string(), true),
    }
}

/// Deduplicates doom-loop repeats, then runs remaining calls in parallel.
pub(super) async fn process_tool_calls(
    response: maki_providers::StreamResponse,
    recent_calls: &mut RecentCalls,
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
        set.spawn(async move {
            let done = run(id, &name, &input, &tool_ctx, CallOrigin::Model).await;
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

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicBool, Ordering};

    use maki_config::{Effect, Permission, PermissionRule, PermissionsConfig, ToolKey};
    use test_case::test_case;

    use super::*;
    use crate::AgentMode;
    use crate::cancel::CancelToken;
    use crate::mcp::test_support::stub_session;
    use crate::mcp::tool_names;
    use crate::permissions::{PERMISSION_DENIED_PREFIX, PermissionManager};
    use crate::template::Vars;
    use crate::tools::registry::{ToolRegistry, ToolSource};
    use crate::tools::schema::{JsonPath, ToolInputErrorKind};
    use crate::tools::test_support::{
        GUARDED_TOOL_NAME, GuardedMock, mock_tool, stub_ctx, stub_ctx_with_permissions,
    };
    use crate::tools::{
        BoxFuture, DescriptionContext, ExecFuture, HeaderFuture, HeaderResult, ParseError,
        PermissionScopes, RequestTools, TOOL_NAME_FIELD, Tool, ToolAudience, ToolExecResult,
        ToolHook, local_tool,
    };

    const TEST_ID: &str = "t1";
    const PROBE_WIRE: &str = "srv__probe";
    const PROBE_QUALIFIED: &str = "srv.probe";
    const OTHER_WIRE: &str = "srv__other";
    const OTHER_QUALIFIED: &str = "srv.other";
    const SHADOWED_NAME: &str = "batch";
    const CLIENT_NAME: &str = "client_probe";
    const TEST_PLUGIN: &str = "test";
    const HOOK_TOOL_NAME: &str = "hook_probe";
    const HOOK_FIELD: &str = "command";
    const HOOK_PLAIN: &str = "ls";
    const HOOK_REWRITTEN_FROM: &str = "grep -r x .";
    const HOOK_REWRITTEN_TO: &str = "rg x";
    const HOOK_DENIED: &str = "sudo rm -rf /";
    const HOOK_DENY_REASON: &str = "not on my watch";
    const HOOK_OUTPUT_TEXT: &str = "trimmed";
    const HOOK_PERMISSION: Permission = Permission::Run;
    const HOOK_FIELD_TYPE: &str = "string";
    const HOOK_DIFF_COMMAND: &str = "apply";
    const HOOK_DIFF_PATH: &str = "/tmp/diffed.txt";
    const HOOK_DIFF_SUMMARY: &str = "1 file changed";
    const HOOK_ESCAPED_PATH: &str = "/tmp/not-the-plan.md";
    const TEST_PLUGIN_SOURCE: &str = "lua:test";
    const START_PROBE_NAME: &str = "start_probe";
    /// Allowed by the shared stub permissions; nothing is ever written here.
    const TEST_ROOT: &str = "/tmp";
    const PLAN_PATH: &str = "/tmp/plan.md";
    const HOOK_CALL_DEADLINE: Duration = Duration::from_secs(7);
    const HOOK_SLOW_COMMAND: &str = "slow";
    /// Real elapsed time inside the call, so the gap between the two stages'
    /// windows is a measurement rather than a race.
    const HOOK_SLOW_RUN: Duration = Duration::from_millis(20);

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
        let mut ctx = stub_ctx(&AgentMode::Build);
        ctx.local_tools = Arc::new(HashMap::from([(
            name.to_owned(),
            local_tool(ToolAudience::all(), move |input, _ctx| {
                let result = f(&input);
                Box::pin(async move { result })
            }),
        )]));
        pin(&mut ctx);
        ctx
    }

    async fn dispatch(ctx: &ToolContext, name: &str, input: &Value) -> ToolDoneEvent {
        dispatch_rebound(ctx, name, input).await
    }

    async fn dispatch_nested(ctx: &ToolContext, name: &str, input: &Value) -> ToolDoneEvent {
        let mut ctx = ctx.clone();
        pin(&mut ctx);
        run(TEST_ID.into(), name, input, &ctx, CallOrigin::Nested).await
    }

    fn with_mcp(mut ctx: ToolContext, mcp: &McpSession) -> ToolContext {
        ctx.mcp = Some(mcp.clone());
        pin(&mut ctx);
        ctx
    }

    /// Publishes the tools with empty descriptions: search matches on the name.
    fn stub_mcp(qualified: &[&str]) -> McpSession {
        let tools: Vec<_> = qualified.iter().map(|name| (*name, "")).collect();
        stub_session(&tools)
    }

    fn mcp_ctx(mcp: &McpSession) -> ToolContext {
        with_mcp(stub_ctx(&AgentMode::Build), mcp)
    }

    fn pin(ctx: &mut ToolContext) {
        ctx.turn_bindings = Arc::new(crate::tools::TurnToolBindings::capture(
            &ctx.registry,
            &ctx.local_tools,
            ctx.mcp.as_ref(),
        ));
    }

    async fn dispatch_pinned(ctx: &ToolContext, name: &str, input: &Value) -> ToolDoneEvent {
        run(TEST_ID.into(), name, input, ctx, CallOrigin::Model).await
    }

    async fn dispatch_rebound(ctx: &ToolContext, name: &str, input: &Value) -> ToolDoneEvent {
        let mut ctx = ctx.clone();
        pin(&mut ctx);
        dispatch_pinned(&ctx, name, input).await
    }

    fn registered(tool: Arc<dyn Tool>) -> Arc<ToolRegistry> {
        let registry = ToolRegistry::new();
        register(&registry, tool);
        Arc::new(registry)
    }

    fn register(registry: &ToolRegistry, tool: Arc<dyn Tool>) {
        registry
            .register(
                tool,
                ToolSource::Lua {
                    plugin: TEST_PLUGIN.into(),
                },
            )
            .unwrap();
    }

    fn registry_with(names: &[&str]) -> Arc<ToolRegistry> {
        let registry = ToolRegistry::new();
        for name in names {
            register(&registry, mock_tool(name, ToolAudience::all()));
        }
        Arc::new(registry)
    }

    fn ruled_ctx(mode: &AgentMode, tool: ToolKey, effect: Effect) -> ToolContext {
        let config = PermissionsConfig {
            rules: vec![PermissionRule {
                tool,
                scope: None,
                effect,
            }],
            ..Default::default()
        };
        let permissions = Arc::new(PermissionManager::new(
            config,
            PathBuf::from(TEST_ROOT),
            maki_config::ProjectConfig::discover(Path::new(TEST_ROOT)),
            Arc::default(),
        ));
        stub_ctx_with_permissions(mode, permissions)
    }

    fn denying_ctx(tool: ToolKey) -> ToolContext {
        ruled_ctx(&AgentMode::Build, tool, Effect::Deny)
    }

    fn build_ctx() -> ToolContext {
        stub_ctx(&AgentMode::Build)
    }

    /// Its permission scope, its write target and its output are all its
    /// input, which is how a test sees the input each stage of dispatch got.
    struct HookMock(Option<Permission>);

    struct HookMockInvocation(String);

    impl ToolInvocation for HookMockInvocation {
        fn start_header(&self) -> HeaderFuture {
            HeaderFuture::Ready(HeaderResult::plain(HOOK_TOOL_NAME.into()))
        }
        fn permission_scopes(
            &self,
            _session_id: Option<&SessionRef>,
        ) -> BoxFuture<'_, Option<PermissionScopes>> {
            Box::pin(std::future::ready(Some(PermissionScopes::single(
                self.0.clone(),
            ))))
        }
        fn mutable_path(&self, _ctx: &ToolContext) -> Option<PathBuf> {
            self.0.starts_with('/').then(|| PathBuf::from(&self.0))
        }
        fn execute<'a>(self: Box<Self>, _ctx: &'a ToolContext) -> ExecFuture<'a> {
            Box::pin(async move {
                if self.0 == HOOK_SLOW_COMMAND {
                    smol::Timer::after(HOOK_SLOW_RUN).await;
                }
                Ok(output_of(&self.0)).into()
            })
        }
    }

    fn ran(command: &str) -> String {
        format!("ran {command}")
    }

    /// One command answers with a shape the UI renders from fields, the one
    /// kind of output a hook may not touch.
    fn output_of(command: &str) -> ToolOutput {
        if command == HOOK_DIFF_COMMAND {
            return ToolOutput::Diff {
                path: HOOK_DIFF_PATH.to_owned(),
                before: String::new(),
                after: String::new(),
                summary: HOOK_DIFF_SUMMARY.to_owned(),
            };
        }
        ToolOutput::Plain(ran(command).into())
    }

    /// Shared by the mock and the assertion, so the test cannot pass against
    /// some other error.
    fn missing_command() -> ParseError {
        ParseError {
            path: JsonPath::default(),
            kind: ToolInputErrorKind::Missing {
                expected: HOOK_FIELD_TYPE,
            },
        }
    }

    impl Tool for HookMock {
        fn name(&self) -> &str {
            HOOK_TOOL_NAME
        }
        fn description(&self, _ctx: &DescriptionContext) -> Cow<'_, str> {
            "hook mock".into()
        }
        fn schema(&self) -> Value {
            serde_json::json!({"type": "object", "properties": {"command": {"type": "string"}}})
        }
        fn tool_kind(&self) -> Option<&str> {
            Some("edit")
        }
        fn required_permission(&self) -> Option<Permission> {
            self.0
        }
        fn parse(&self, input: &Value) -> Result<Box<dyn ToolInvocation>, ParseError> {
            match input[HOOK_FIELD].as_str() {
                Some(command) => Ok(Box::new(HookMockInvocation(command.to_owned()))),
                None => Err(missing_command()),
            }
        }
    }

    /// One firing as the hook saw it.
    #[derive(Clone, Debug)]
    struct Seen {
        stage: HookStage,
        authority: Authority,
        tool: String,
        tool_id: String,
        session_id: Option<String>,
        origin: CallOrigin,
        value: Value,
        cancelled: bool,
        deadline: Instant,
    }

    #[derive(Clone, Copy)]
    enum Reply {
        Answers(fn(HookStage, &Value) -> Verdict),
        /// Never resolves, so only cancellation can end the wait.
        Pending,
    }

    /// Stands in for the Lua slot chain: records every firing and answers with
    /// whatever the test scripted.
    #[derive(Clone)]
    struct RecordingHook {
        seen: Arc<Mutex<Vec<Seen>>>,
        wrapped: &'static [HookStage],
        reply: Reply,
    }

    impl Default for RecordingHook {
        fn default() -> Self {
            Self {
                seen: Arc::default(),
                wrapped: &HookStage::ALL,
                reply: Reply::Answers(steer_the_call),
            }
        }
    }

    /// Rewrites, denies or defers on the way in, the way a plugin steering the
    /// model off one command onto another would.
    fn steer_the_call(stage: HookStage, value: &Value) -> Verdict {
        match stage {
            HookStage::Input => match value[HOOK_FIELD].as_str() {
                Some(HOOK_DENIED) => Verdict::Denied(HOOK_DENY_REASON.into()),
                Some(HOOK_REWRITTEN_FROM) => Verdict::Replaced(call_input(HOOK_REWRITTEN_TO)),
                _ => Verdict::Unchanged,
            },
            HookStage::Output => Verdict::Unchanged,
        }
    }

    impl RecordingHook {
        fn wrapping(wrapped: &'static [HookStage]) -> Self {
            Self {
                wrapped,
                ..Self::default()
            }
        }

        fn answering(answer: fn(HookStage, &Value) -> Verdict) -> Self {
            Self {
                reply: Reply::Answers(answer),
                ..Self::default()
            }
        }

        fn never_answering(wrapped: &'static [HookStage]) -> Self {
            Self {
                wrapped,
                reply: Reply::Pending,
                ..Self::default()
            }
        }

        fn seen(&self) -> Vec<Seen> {
            self.seen.lock().unwrap().clone()
        }

        fn stages(&self) -> Vec<(HookStage, Authority)> {
            self.seen().iter().map(|s| (s.stage, s.authority)).collect()
        }

        fn at(&self, stage: HookStage) -> Option<Seen> {
            self.seen().into_iter().find(|s| s.stage == stage)
        }
    }

    impl ToolHook for RecordingHook {
        fn wraps(&self, _tool: &str, stage: HookStage) -> bool {
            self.wrapped.contains(&stage)
        }

        fn run<'a>(
            &'a self,
            stage: HookStage,
            value: Value,
            call: &'a HookCall<'a>,
        ) -> BoxFuture<'a, Verdict> {
            self.seen.lock().unwrap().push(Seen {
                stage,
                authority: call.authority,
                tool: call.tool.to_owned(),
                tool_id: call.tool_id.to_owned(),
                session_id: call.session_id.map(str::to_owned),
                origin: call.origin,
                value: value.clone(),
                cancelled: call.cancel.is_cancelled(),
                deadline: call.deadline,
            });
            match self.reply {
                Reply::Answers(answer) => Box::pin(std::future::ready(answer(stage, &value))),
                Reply::Pending => Box::pin(std::future::pending()),
            }
        }
    }

    fn hooked_ctx(ctx: ToolContext) -> (ToolContext, RecordingHook) {
        hooked_with(ctx, None, RecordingHook::default())
    }

    fn plain_hooked_ctx(hook: RecordingHook) -> (ToolContext, RecordingHook) {
        hooked_with(build_ctx(), None, hook)
    }

    fn hooked_with(
        mut ctx: ToolContext,
        permission: Option<Permission>,
        hook: RecordingHook,
    ) -> (ToolContext, RecordingHook) {
        ctx.registry = registered(Arc::new(HookMock(permission)));
        ctx.registry.set_hook(hook.clone());
        pin(&mut ctx);
        (ctx, hook)
    }

    fn cancelled_token() -> CancelToken {
        let (trigger, token) = CancelToken::new();
        trigger.cancel();
        token
    }

    fn call_input(command: &str) -> Value {
        serde_json::json!({ HOOK_FIELD: command })
    }

    fn both_stages(authority: Authority) -> Vec<(HookStage, Authority)> {
        HookStage::ALL.map(|stage| (stage, authority)).into()
    }

    /// The rewritten call is the one that runs and the one the rules judge.
    /// Were it the other way round, an `allow bash: git status` rule would be a
    /// way to run anything.
    #[test_case(build_ctx                                       , false ; "reaches_execute")]
    #[test_case(|| denying_ctx(ToolKey::native(HOOK_TOOL_NAME))  , true  ; "reaches_the_permission_prompt")]
    fn an_input_rewrite_is_the_call_that_runs(build: fn() -> ToolContext, is_error: bool) {
        smol::block_on(async {
            let (ctx, _hook) = hooked_ctx(build());
            let done = dispatch(&ctx, HOOK_TOOL_NAME, &call_input(HOOK_REWRITTEN_FROM)).await;

            let text = done.output.as_text();
            assert_eq!(done.is_error, is_error, "{text}");
            assert!(
                text.contains(HOOK_REWRITTEN_TO) && !text.contains(HOOK_REWRITTEN_FROM),
                "everything downstream names the rewritten command: {text}"
            );
        });
    }

    #[test]
    fn input_hook_denial_never_runs_the_tool() {
        smol::block_on(async {
            let (ctx, hook) = hooked_ctx(stub_ctx(&AgentMode::Build));
            let done = dispatch(&ctx, HOOK_TOOL_NAME, &call_input(HOOK_DENIED)).await;

            assert!(done.is_error);
            assert_eq!(done.output.as_text(), HOOK_DENY_REASON);
            assert_eq!(
                hook.stages(),
                vec![(HookStage::Input, Authority::Unbounded)],
                "a stopped call has no output to hook"
            );
        });
    }

    /// Everything the model reads passes the output stage, so a hook that
    /// redacts or trims cannot be walked around by failing the call.
    #[test]
    fn a_refused_call_still_reaches_the_output_stage() {
        smol::block_on(async {
            let (ctx, hook) = hooked_ctx(denying_ctx(ToolKey::native(HOOK_TOOL_NAME)));
            let done = dispatch(&ctx, HOOK_TOOL_NAME, &call_input(HOOK_PLAIN)).await;

            assert!(done.is_error);
            assert!(done.output.as_text().contains(PERMISSION_DENIED_PREFIX));
            assert_eq!(hook.stages(), both_stages(Authority::Unbounded));

            let firing = hook.at(HookStage::Output).expect("the output stage fired");
            assert_eq!(firing.value[OUTPUT_IS_ERROR], Value::Bool(true));
            let text = firing.value[OUTPUT_TEXT].as_str().unwrap_or_default();
            assert!(text.contains(PERMISSION_DENIED_PREFIX), "got: {text}");
        });
    }

    /// A name that routes nowhere lends no authority, so nothing fires.
    #[test]
    fn unknown_names_are_not_hooked() {
        smol::block_on(async {
            let (ctx, hook) = hooked_ctx(stub_ctx(&AgentMode::Build));
            let done = dispatch(&ctx, "nope", &call_input(HOOK_DENIED)).await;

            assert!(done.is_error);
            assert!(done.output.as_text().contains(UNKNOWN_TOOL_PREFIX));
            assert!(hook.stages().is_empty());
        });
    }

    fn mcp_route_ctx() -> ToolContext {
        mcp_ctx(&stub_mcp(&[PROBE_QUALIFIED]))
    }

    fn host_route_ctx() -> ToolContext {
        local_ctx(CLIENT_NAME, |_| Ok(String::new()))
    }

    /// Hooking in dispatch is what reaches a route maki did not write the code
    /// behind, and each one has to answer for what that lends. Only a declared
    /// capability narrows the price; everything else prices at the maximum.
    /// The name stays the one the model called, not whatever dispatch routes
    /// it to.
    #[test_case(build_ctx,      HOOK_TOOL_NAME,        Some(HOOK_PERMISSION), Authority::Capability(HOOK_PERMISSION) ; "a_checked_tool_lends_its_capability")]
    #[test_case(build_ctx,      HOOK_TOOL_NAME,        None,                  Authority::Unbounded                   ; "a_tool_declaring_nothing_declares_no_limit")]
    #[test_case(mcp_route_ctx,  TOOL_SEARCH_TOOL_NAME, None,                  Authority::Unbounded                   ; "search_declares_nothing_either")]
    #[test_case(mcp_route_ctx,  PROBE_WIRE,            None,                  Authority::Unbounded                   ; "an_mcp_tool_is_code_maki_does_not_own")]
    #[test_case(host_route_ctx, CLIENT_NAME,           None,                  Authority::Unbounded                   ; "a_host_tool_is_code_maki_does_not_own")]
    fn a_route_lends_the_authority_it_declares(
        build: fn() -> ToolContext,
        name: &str,
        permission: Option<Permission>,
        expected: Authority,
    ) {
        smol::block_on(async {
            let (ctx, hook) = hooked_with(build(), permission, RecordingHook::default());
            dispatch(&ctx, name, &call_input(HOOK_PLAIN)).await;

            let firing = hook.at(HookStage::Input).expect("the input stage fired");
            assert_eq!(firing.authority, expected);
            assert_eq!(firing.tool, name, "hooked under the name the model called");
        });
    }

    /// `wraps` is why an unwrapped slot costs nothing: a stage the hook
    /// declines never reaches `run` at all.
    #[test_case(&[HookStage::Input]  ; "input_only")]
    #[test_case(&[HookStage::Output] ; "output_only")]
    fn a_stage_the_hook_declines_never_fires(wrapped: &'static [HookStage]) {
        smol::block_on(async {
            let (ctx, hook) = plain_hooked_ctx(RecordingHook::wrapping(wrapped));
            let done = dispatch(&ctx, HOOK_TOOL_NAME, &call_input(HOOK_PLAIN)).await;

            assert!(!done.is_error);
            assert_eq!(done.output.as_text(), ran(HOOK_PLAIN));
            let fired: Vec<HookStage> = hook.seen().iter().map(|s| s.stage).collect();
            assert_eq!(fired, wrapped);
        });
    }

    /// Nobody is left reading the answer, so waiting on a verdict that never
    /// comes would only keep the call alive. Each stage keeps what it has: no
    /// input was judged, and an output already produced stands.
    #[test_case(&[HookStage::Input],  true,  ERROR_CANCELLED.to_owned() ; "input")]
    #[test_case(&[HookStage::Output], false, ran(HOOK_PLAIN)            ; "output")]
    fn a_cancelled_call_does_not_wait_for_a_verdict(
        wrapped: &'static [HookStage],
        is_error: bool,
        expected: String,
    ) {
        smol::block_on(async {
            let mut ctx = build_ctx();
            ctx.cancel = cancelled_token();
            let (ctx, _hook) = hooked_with(ctx, None, RecordingHook::never_answering(wrapped));

            let done = dispatch(&ctx, HOOK_TOOL_NAME, &call_input(HOOK_PLAIN)).await;

            assert_eq!(done.is_error, is_error);
            assert_eq!(done.output.as_text(), expected);
        });
    }

    /// A chain runs off this thread, so it only dies with the call it filters
    /// when it is handed that call's own token and an instant to be killed at.
    #[test]
    fn a_firing_carries_the_calls_cancellation_and_deadline() {
        smol::block_on(async {
            let at = Instant::now() + HOOK_CALL_DEADLINE;
            let mut ctx = build_ctx();
            ctx.deadline = Deadline::At(at);
            ctx.cancel = cancelled_token();
            let (ctx, hook) = hooked_ctx(ctx);

            dispatch(&ctx, HOOK_TOOL_NAME, &call_input(HOOK_PLAIN)).await;

            let firing = hook.at(HookStage::Input).expect("the input stage fired");
            assert!(firing.cancelled, "the call's own token, not a fresh one");
            assert_eq!(firing.deadline, at, "and no later than the call itself");
        });
    }

    /// A call with no deadline of its own still bounds each chain, or a layer
    /// that hangs hangs the call with it. Bounded from where the stage starts,
    /// too: the input chain and the tool spend from the same budget, and an
    /// output chain handed the entry-time answer would get whatever they left,
    /// which for a slow tool is nothing.
    #[test]
    fn a_call_without_a_deadline_bounds_each_stage_from_where_it_starts() {
        smol::block_on(async {
            let (ctx, hook) = hooked_ctx(build_ctx());
            let before = Instant::now();

            dispatch(&ctx, HOOK_TOOL_NAME, &call_input(HOOK_SLOW_COMMAND)).await;

            let input = hook.at(HookStage::Input).expect("the input stage fired");
            let output = hook.at(HookStage::Output).expect("the output stage fired");
            assert!(
                input.deadline >= before && input.deadline <= Instant::now() + HOOK_CHAIN_MAX,
                "a deadline already past kills every chain"
            );
            assert!(
                output.deadline - input.deadline >= HOOK_SLOW_RUN,
                "the output chain inherited a window the call had already spent"
            );
        });
    }

    fn replace_the_output(stage: HookStage, _value: &Value) -> Verdict {
        match stage {
            HookStage::Input => Verdict::Unchanged,
            HookStage::Output => Verdict::Replaced(
                serde_json::json!({OUTPUT_TEXT: HOOK_OUTPUT_TEXT, OUTPUT_IS_ERROR: true}),
            ),
        }
    }

    fn replace_the_output_without_text(stage: HookStage, _value: &Value) -> Verdict {
        match stage {
            HookStage::Input => Verdict::Unchanged,
            HookStage::Output => Verdict::Replaced(serde_json::json!({OUTPUT_IS_ERROR: true})),
        }
    }

    fn deny_the_output(stage: HookStage, _value: &Value) -> Verdict {
        match stage {
            HookStage::Input => Verdict::Unchanged,
            HookStage::Output => Verdict::Denied(HOOK_DENY_REASON.into()),
        }
    }

    /// Text and error flag move together, and a hook that has run out of call
    /// to stop has only the text left to say so with.
    #[test_case(replace_the_output,              true,  HOOK_OUTPUT_TEXT.to_owned() ; "a_replacement_moves_text_and_flag")]
    #[test_case(replace_the_output_without_text, false, ran(HOOK_PLAIN)             ; "a_replacement_without_text_changes_neither")]
    #[test_case(deny_the_output,                 true,  HOOK_DENY_REASON.to_owned() ; "a_denial_becomes_the_result")]
    fn the_output_stage_decides_what_the_model_reads(
        answer: fn(HookStage, &Value) -> Verdict,
        is_error: bool,
        expected: String,
    ) {
        smol::block_on(async {
            let (ctx, _hook) = plain_hooked_ctx(RecordingHook::answering(answer));
            let done = dispatch(&ctx, HOOK_TOOL_NAME, &call_input(HOOK_PLAIN)).await;

            assert_eq!(done.is_error, is_error);
            assert_eq!(done.output.as_text(), expected);
        });
    }

    /// An output the UI renders from fields carries no prose to lend, and
    /// editing it would desync the fields from the text.
    #[test]
    fn a_rendered_output_skips_the_output_stage() {
        smol::block_on(async {
            let (ctx, hook) = plain_hooked_ctx(RecordingHook::answering(deny_the_output));
            let done = dispatch(&ctx, HOOK_TOOL_NAME, &call_input(HOOK_DIFF_COMMAND)).await;

            assert!(!done.is_error);
            assert_eq!(done.output.as_text(), HOOK_DIFF_SUMMARY);
            assert_eq!(
                hook.stages(),
                vec![(HookStage::Input, Authority::Unbounded)]
            );
        });
    }

    fn drop_the_field(stage: HookStage, _value: &Value) -> Verdict {
        match stage {
            HookStage::Input => Verdict::Replaced(serde_json::json!({})),
            HookStage::Output => Verdict::Unchanged,
        }
    }

    /// The rewrite lands before the schema check, so a shape the tool cannot
    /// parse is an ordinary parse error rather than something dispatch has to
    /// survive.
    #[test]
    fn a_rewrite_the_tool_cannot_parse_is_a_parse_error() {
        smol::block_on(async {
            let (ctx, _hook) = plain_hooked_ctx(RecordingHook::answering(drop_the_field));
            let done = dispatch(&ctx, HOOK_TOOL_NAME, &call_input(HOOK_PLAIN)).await;

            assert!(done.is_error);
            assert_eq!(done.output.as_text(), missing_command().to_string());
        });
    }

    fn rewrite_the_target(stage: HookStage, _value: &Value) -> Verdict {
        match stage {
            HookStage::Input => Verdict::Replaced(call_input(HOOK_ESCAPED_PATH)),
            HookStage::Output => Verdict::Unchanged,
        }
    }

    #[test]
    fn restricted_write_never_reaches_input_hooks_even_for_plan_path() {
        smol::block_on(async {
            let plan = AgentMode::Plan(PathBuf::from(PLAN_PATH));
            let (ctx, hook) = hooked_with(
                stub_ctx(&plan),
                None,
                RecordingHook::answering(rewrite_the_target),
            );
            let done = dispatch(&ctx, HOOK_TOOL_NAME, &call_input(PLAN_PATH)).await;
            assert!(done.is_error);
            assert_eq!(
                done.output.as_text(),
                format!("{MODE_DENIED}: {HOOK_TOOL_NAME}")
            );
            assert!(hook.seen.lock().unwrap().is_empty());
        });
    }

    /// A plugin keys its state on these, so a stage firing under another
    /// call's identity would write onto that other call.
    #[test]
    fn both_stages_carry_the_call_id_the_session_and_the_origin() {
        smol::block_on(async {
            let session = SessionRef::generate();
            let mut ctx = build_ctx();
            ctx.session_id = Some(session.clone());
            let (ctx, hook) = hooked_ctx(ctx);

            dispatch_nested(&ctx, HOOK_TOOL_NAME, &call_input(HOOK_PLAIN)).await;

            let seen = hook.seen();
            assert_eq!(seen.len(), HookStage::ALL.len(), "both stages fire");
            for firing in seen {
                assert_eq!(firing.tool_id, TEST_ID);
                assert_eq!(firing.session_id.as_deref(), Some(session.as_str()));
                assert_eq!(firing.origin, CallOrigin::Nested);
            }
        });
    }

    #[test]
    fn local_tool_shadows_registry_and_maps_errors() {
        smol::block_on(async {
            let mut ctx = local_ctx(SHADOWED_NAME, |input| {
                Ok(format!("local:{}", input["path"]))
            });
            ctx.registry = registry_with(&[SHADOWED_NAME]);
            let done = dispatch(&ctx, SHADOWED_NAME, &serde_json::json!({"path": "/a"})).await;
            assert!(!done.is_error);
            assert_eq!(done.output.as_text(), r#"local:"/a""#);

            let ctx = local_ctx("boom", |_| Err("nope".into()));
            let done = dispatch(&ctx, "boom", &serde_json::json!({})).await;
            assert!(done.is_error);
            assert_eq!(done.output.as_text(), "nope");
        });
    }

    #[test]
    fn functions_prefixed_name_dispatches_to_canonical_tool() {
        smol::block_on(async {
            let ctx = local_ctx("ok", |_| Ok("ran".into()));
            let done = dispatch(&ctx, "functions.ok", &serde_json::json!({})).await;
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
            ctx.local_tools = Arc::new(HashMap::from([(
                "local_echo".to_owned(),
                local_tool(ToolAudience::all(), |input: Value, _ctx| {
                    let out = input.to_string();
                    Box::pin(async move { Ok(out) })
                }),
            )]));

            let input = serde_json::json!({"path": "/a"});
            let done = dispatch(&ctx, "local_echo", &input).await;
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
            let mcp = stub_mcp(&[PROBE_QUALIFIED]);
            let done = dispatch(
                &mcp_ctx(&mcp),
                TOOL_SEARCH_TOOL_NAME,
                &serde_json::json!({"query": "probe"}),
            )
            .await;
            assert!(!done.is_error, "got: {}", done.output.as_text());
            assert_eq!(done.tool.as_ref(), TOOL_SEARCH_TOOL_NAME);
            assert!(done.output.as_text().contains(PROBE_WIRE));

            let mut tools = serde_json::json!([]);
            mcp.extend_tools(&mut tools);
            assert!(
                tool_names(&tools).contains(&PROBE_WIRE),
                "searched tool must join the next request"
            );
        });
    }

    #[test]
    fn stale_mcp_catalog_rejects_tool_search() {
        smol::block_on(async {
            let original = stub_mcp(&[PROBE_QUALIFIED]);
            let replacement = stub_mcp(&[PROBE_QUALIFIED]);
            let mut ctx = mcp_ctx(&original);
            ctx.mcp = Some(replacement);
            let done = run_tool_search(
                ctx.mcp.as_ref().unwrap(),
                TEST_ID.into(),
                &serde_json::json!({"query": "probe"}),
                &ctx,
                CallOrigin::Model,
            );
            assert!(done.is_error);
            assert_eq!(
                done.output.as_text(),
                "MCP tool catalog changed during this turn"
            );
        });
    }

    #[test]
    fn plan_mode_denies_tool_search_before_hooks_and_loading() {
        smol::block_on(async {
            let plan = AgentMode::Plan(PathBuf::from(PLAN_PATH));
            let mcp = stub_mcp(&[PROBE_QUALIFIED]);
            let ctx = with_mcp(stub_ctx(&plan), &mcp);
            let (ctx, hook) = hooked_with(ctx, None, RecordingHook::default());
            let done = dispatch(
                &ctx,
                TOOL_SEARCH_TOOL_NAME,
                &serde_json::json!({"query": "probe"}),
            )
            .await;
            assert!(done.is_error);
            assert_eq!(
                done.output.as_text(),
                format!("{MODE_DENIED}: {TOOL_SEARCH_TOOL_NAME}")
            );
            assert!(hook.seen.lock().unwrap().is_empty());
            let mut tools = serde_json::json!([]);
            mcp.extend_tools(&mut tools);
            assert!(!tool_names(&tools).contains(&PROBE_WIRE));
        });
    }

    #[test_case(serde_json::json!({"query": "  "}) ; "blank_query")]
    #[test_case(serde_json::json!({}) ; "missing_query")]
    fn tool_search_bad_query_is_error_event(input: Value) {
        smol::block_on(async {
            let done = dispatch(
                &mcp_ctx(&stub_mcp(&[PROBE_QUALIFIED])),
                TOOL_SEARCH_TOOL_NAME,
                &input,
            )
            .await;
            assert!(done.is_error);
            assert_eq!(done.output.as_text(), crate::mcp::SEARCH_EMPTY_QUERY);
        });
    }

    #[test]
    fn calling_deferred_mcp_tool_marks_it_loaded() {
        smol::block_on(async {
            let mcp = stub_mcp(&[PROBE_QUALIFIED]);
            let done = dispatch(&mcp_ctx(&mcp), PROBE_WIRE, &serde_json::json!({})).await;
            assert_eq!(done.tool.as_ref(), PROBE_QUALIFIED, "must route to MCP");

            let mut tools = serde_json::json!([]);
            mcp.extend_tools(&mut tools);
            assert_eq!(
                tool_names(&tools),
                vec![PROBE_WIRE],
                "called tool must join the next request"
            );
        });
    }

    /// `McpSession::new` rebuilds the loaded set from the `ToolUse` blocks in
    /// history, which hold no nested call, so loading one here would make the
    /// live tool array differ from the resumed one.
    #[test_case(PROBE_WIRE, serde_json::json!({}), PROBE_QUALIFIED ; "tool_call")]
    #[test_case(TOOL_SEARCH_TOOL_NAME, serde_json::json!({"query": "probe"}), TOOL_SEARCH_TOOL_NAME ; "tool_search")]
    fn nested_call_reaches_mcp_without_loading_anything(name: &str, input: Value, routed: &str) {
        smol::block_on(async {
            let mcp = stub_mcp(&[PROBE_QUALIFIED]);
            let done = dispatch_nested(&mcp_ctx(&mcp), name, &input).await;
            assert_eq!(done.tool.as_ref(), routed, "must route to MCP");

            let mut tools = serde_json::json!([]);
            mcp.extend_tools(&mut tools);
            assert_eq!(
                tool_names(&tools),
                vec![TOOL_SEARCH_TOOL_NAME],
                "a nested call must not change the next request"
            );
        });
    }

    #[test]
    fn denied_mcp_call_does_not_load_definition() {
        smol::block_on(async {
            let mcp = stub_mcp(&[PROBE_QUALIFIED]);
            let ctx = with_mcp(denying_ctx(ToolKey::parse(PROBE_QUALIFIED).unwrap()), &mcp);
            let done = dispatch(&ctx, PROBE_WIRE, &serde_json::json!({})).await;
            assert!(done.is_error);
            assert!(
                done.output.as_text().starts_with(PERMISSION_DENIED_PREFIX),
                "got: {}",
                done.output.as_text()
            );

            let mut tools = serde_json::json!([]);
            mcp.extend_tools(&mut tools);
            assert_eq!(
                tool_names(&tools),
                vec![TOOL_SEARCH_TOOL_NAME],
                "denied call must not load the definition"
            );
        });
    }

    #[test]
    fn local_tool_named_tool_search_shadows_mcp_search() {
        smol::block_on(async {
            let mcp = stub_mcp(&[PROBE_QUALIFIED]);
            let ctx = with_mcp(
                local_ctx(TOOL_SEARCH_TOOL_NAME, |_| Ok("local wins".into())),
                &mcp,
            );
            let done = dispatch(
                &ctx,
                TOOL_SEARCH_TOOL_NAME,
                &serde_json::json!({"query": "probe"}),
            )
            .await;
            assert_eq!(done.output.as_text(), "local wins");
        });
    }

    #[test]
    fn telemetry_source_names_the_plugin_for_registry_tools() {
        let mut ctx = mcp_ctx(&stub_mcp(&[PROBE_QUALIFIED, OTHER_QUALIFIED]));
        ctx.registry = registry_with(&[PROBE_WIRE]);
        assert_eq!(resolve(&ctx, PROBE_WIRE).route.source(), TEST_PLUGIN_SOURCE);
        assert_eq!(resolve(&ctx, OTHER_WIRE).route.source(), SOURCE_MCP);
    }

    #[test]
    fn resolve_prefers_local_over_registry_and_registry_over_mcp() {
        let mcp = stub_mcp(&[PROBE_QUALIFIED]);
        let mut ctx = with_mcp(local_ctx(PROBE_WIRE, |_| Ok(String::new())), &mcp);
        ctx.registry = registry_with(&[PROBE_WIRE]);

        assert!(matches!(resolve(&ctx, PROBE_WIRE).route, Route::Local(_)));

        ctx.local_tools = Arc::default();
        assert!(matches!(resolve(&ctx, PROBE_WIRE).route, Route::Native(_)));

        ctx.registry = Arc::new(ToolRegistry::new());
        assert!(matches!(resolve(&ctx, PROBE_WIRE).route, Route::Mcp(..)));
    }

    fn callable_names(ctx: &ToolContext) -> Vec<String> {
        callable(ctx).into_iter().map(|c| c.name).collect()
    }

    /// A deferred MCP tool is missing from the request's tool array and is still
    /// a name the sandbox may bind.
    #[test]
    fn callable_lists_host_and_deferred_mcp_names() {
        let mcp = stub_mcp(&[PROBE_QUALIFIED, OTHER_QUALIFIED]);
        let ctx = with_mcp(local_ctx(CLIENT_NAME, |_| Ok(String::new())), &mcp);
        assert_eq!(
            callable_names(&ctx),
            [CLIENT_NAME, OTHER_WIRE, PROBE_WIRE, TOOL_SEARCH_TOOL_NAME]
        );
    }

    #[test]
    fn pinned_native_replacement_is_not_dispatchable() {
        smol::block_on(async {
            let mut ctx = stub_ctx(&AgentMode::Build);
            ctx.registry = registry_with(&[PROBE_WIRE]);
            pin(&mut ctx);
            ctx.registry = registry_with(&[PROBE_WIRE]);
            assert!(!authorize_advertised(&ctx, PROBE_WIRE));
            let done = dispatch_pinned(&ctx, PROBE_WIRE, &json!({})).await;
            assert!(done.is_error);
            assert_eq!(
                done.output.as_text(),
                format!("{UNAVAILABLE_TOOL_PREFIX}: {PROBE_WIRE}")
            );
        });
    }

    #[test]
    fn stale_binding_denied_before_hooks_and_permission_prompt() {
        smol::block_on(async {
            let mut ctx = stub_ctx(&AgentMode::Build);
            ctx.permissions = Arc::new(PermissionManager::new(
                PermissionsConfig::default(),
                PathBuf::from(TEST_ROOT),
                maki_config::ProjectConfig::discover(Path::new(TEST_ROOT)),
                Arc::default(),
            ));
            let (mut ctx, hook) = hooked_with(ctx, Some(Permission::Run), RecordingHook::default());
            ctx.registry = registered(Arc::new(HookMock(Some(Permission::Run))));
            let done = dispatch_pinned(&ctx, HOOK_TOOL_NAME, &call_input(HOOK_PLAIN)).await;
            assert!(done.is_error);
            assert_eq!(
                done.output.as_text(),
                format!("{UNAVAILABLE_TOOL_PREFIX}: {HOOK_TOOL_NAME}")
            );
            assert!(hook.seen().is_empty());
        });
    }

    #[test]
    fn pinned_route_rejects_local_shadowing() {
        smol::block_on(async {
            let mut ctx = stub_ctx(&AgentMode::Build);
            ctx.registry = registry_with(&[PROBE_WIRE]);
            pin(&mut ctx);
            ctx.local_tools = Arc::new(HashMap::from([(
                PROBE_WIRE.into(),
                local_tool(ToolAudience::all(), |_, _| {
                    Box::pin(async { Ok("shadow".into()) })
                }),
            )]));
            assert!(!authorize_advertised(&ctx, PROBE_WIRE));
            assert!(dispatch_pinned(&ctx, PROBE_WIRE, &json!({})).await.is_error);
        });
    }

    #[test]
    fn unpinned_native_name_is_not_dispatchable() {
        smol::block_on(async {
            let mut ctx = stub_ctx(&AgentMode::Build);
            ctx.registry = registry_with(&[PROBE_WIRE]);
            assert!(dispatch_pinned(&ctx, PROBE_WIRE, &json!({})).await.is_error);
        });
    }

    #[test]
    fn plan_callable_excludes_unknown_effect_and_shell() {
        let plan = AgentMode::Plan(PathBuf::from(PLAN_PATH));
        let mut ctx = with_mcp(stub_ctx(&plan), &stub_mcp(&[PROBE_QUALIFIED]));
        ctx.registry = registered(Arc::new(HookMock(Some(Permission::Run))));
        assert!(callable_names(&ctx).is_empty());
        assert!(!authorize_advertised(&ctx, TOOL_SEARCH_TOOL_NAME));
    }

    struct SpoofedReadInvocation(Arc<AtomicBool>);

    impl ToolInvocation for SpoofedReadInvocation {
        fn start_header(&self) -> HeaderFuture {
            HeaderFuture::Ready(HeaderResult::plain("read probe".into()))
        }
        fn execute<'a>(self: Box<Self>, _ctx: &'a ToolContext) -> ExecFuture<'a> {
            self.0.store(true, Ordering::SeqCst);
            Box::pin(async { Ok(ToolOutput::Plain("ran".into())).into() })
        }
    }

    struct SpoofedReadTool {
        name: &'static str,
        kind: &'static str,
        executed: Arc<AtomicBool>,
    }

    impl Tool for SpoofedReadTool {
        fn name(&self) -> &str {
            self.name
        }
        fn description(&self, _ctx: &DescriptionContext) -> Cow<'_, str> {
            "read probe".into()
        }
        fn schema(&self) -> Value {
            json!({"type": "object"})
        }
        fn tool_kind(&self) -> Option<&str> {
            Some(self.kind)
        }
        fn parse(&self, _input: &Value) -> Result<Box<dyn ToolInvocation>, ParseError> {
            Ok(Box::new(SpoofedReadInvocation(Arc::clone(&self.executed))))
        }
    }

    #[test_case("read", "read" ; "read")]
    #[test_case("glob", "search" ; "glob")]
    #[test_case("grep", "search" ; "grep")]
    fn restricted_mode_denies_spoofed_lua_read_or_search(name: &'static str, kind: &'static str) {
        smol::block_on(async {
            let mut ctx = stub_ctx(&AgentMode::Plan(PathBuf::from(PLAN_PATH)));
            let registry = ToolRegistry::new();
            let executed = Arc::new(AtomicBool::new(false));
            registry
                .register(
                    Arc::new(SpoofedReadTool {
                        name,
                        kind,
                        executed: Arc::clone(&executed),
                    }),
                    ToolSource::Lua {
                        plugin: name.into(),
                    },
                )
                .unwrap();
            ctx.registry = Arc::new(registry);
            pin(&mut ctx);
            assert!(callable_names(&ctx).is_empty());
            let done = dispatch_pinned(&ctx, name, &json!({})).await;
            assert!(done.is_error);
            assert_eq!(done.output.as_text(), format!("{MODE_DENIED}: {name}"));
            assert!(!executed.load(Ordering::SeqCst));
        });
    }

    #[test]
    fn plan_denies_unknown_effect_even_if_pinned_in_mode_toolset() {
        smol::block_on(async {
            let mut ctx = stub_ctx(&AgentMode::Plan(PathBuf::from(PLAN_PATH)));
            ctx.registry = registry_with(&[PROBE_WIRE, OTHER_WIRE]);
            let mut mode = ctx.mode_def.as_ref().unwrap().as_ref().clone();
            mode.tools = Some(vec![OTHER_WIRE.into()]);
            ctx.mode_def = Some(Arc::new(mode));
            pin(&mut ctx);
            assert!(callable_names(&ctx).is_empty());
            let done = dispatch_pinned(&ctx, OTHER_WIRE, &json!({})).await;
            assert!(done.is_error);
            assert_eq!(
                done.output.as_text(),
                format!("{MODE_DENIED}: {OTHER_WIRE}")
            );
        });
    }

    #[test]
    fn mode_toolset_blocks_dispatch_and_catalog() {
        smol::block_on(async {
            let mut ctx = stub_ctx(&AgentMode::Build);
            ctx.registry = registry_with(&[PROBE_WIRE, OTHER_WIRE]);
            let mut pinned = (*ctx.mode_def.as_ref().unwrap()).as_ref().clone();
            pinned.tools = Some(vec![OTHER_WIRE.into()]);
            ctx.mode_def = Some(Arc::new(pinned));
            pin(&mut ctx);
            assert!(!authorize_advertised(&ctx, PROBE_WIRE));
            assert!(authorize_advertised(&ctx, OTHER_WIRE));
            assert_eq!(callable_names(&ctx), [OTHER_WIRE]);
            let done = dispatch(&ctx, PROBE_WIRE, &serde_json::json!({})).await;
            assert_eq!(
                done.output.as_text(),
                format!("{UNAVAILABLE_TOOL_PREFIX}: {PROBE_WIRE}")
            );
        });
    }

    #[test]
    fn dispatch_uses_pinned_mode_toolset_after_registry_changes() {
        smol::block_on(async {
            let mut ctx = stub_ctx(&AgentMode::Build);
            ctx.registry = registry_with(&[PROBE_WIRE, OTHER_WIRE]);
            let mut pinned = (*ctx.mode_def.as_ref().unwrap()).as_ref().clone();
            pinned.tools = Some(vec![OTHER_WIRE.into()]);
            ctx.mode_def = Some(Arc::new(pinned));
            pin(&mut ctx);
            ctx.modes
                .define(crate::ModeDefSpec {
                    name: "build".into(),
                    tools: Some(vec![PROBE_WIRE.into()]),
                    ..Default::default()
                })
                .unwrap();
            assert_eq!(callable_names(&ctx), [OTHER_WIRE]);
            assert!(dispatch(&ctx, PROBE_WIRE, &json!({})).await.is_error);
            assert!(!dispatch(&ctx, OTHER_WIRE, &json!({})).await.is_error);
        });
    }

    #[test]
    fn missing_pinned_mode_def_denies_dispatch() {
        smol::block_on(async {
            let mut ctx = stub_ctx(&AgentMode::Build);
            ctx.registry = registry_with(&[PROBE_WIRE]);
            ctx.mode_def = None;
            assert!(callable_names(&ctx).is_empty());
            let done = dispatch(&ctx, PROBE_WIRE, &json!({})).await;
            assert!(done.is_error);
            assert_eq!(
                done.output.as_text(),
                format!("{UNAVAILABLE_TOOL_PREFIX}: {PROBE_WIRE}")
            );
        });
    }

    /// A shadowed name appears once, described by whatever `resolve` picks:
    /// listing it under the loser's audience is how a script gets handed a tool
    /// its own audience was denied.
    #[test]
    fn callable_describes_a_shadowed_name_by_what_dispatch_runs() {
        let mcp = stub_mcp(&[PROBE_QUALIFIED]);
        let mut ctx = with_mcp(local_ctx(PROBE_WIRE, |_| Ok(String::new())), &mcp);
        ctx.registry = registry_with(&[PROBE_WIRE]);
        pin(&mut ctx);

        let probe = |ctx: &ToolContext| {
            let all = callable(ctx);
            assert_eq!(all.iter().filter(|c| c.name == PROBE_WIRE).count(), 1);
            all.into_iter()
                .find(|c| c.name == PROBE_WIRE)
                .expect("the name is dispatchable")
        };
        assert_eq!(probe(&ctx).source, SOURCE_LOCAL);

        ctx.local_tools = Arc::default();
        pin(&mut ctx);
        let native = probe(&ctx);
        assert_eq!(native.source, SOURCE_NATIVE);
        assert!(native.schema.is_some(), "registry tools carry their schema");
    }

    /// The sandbox gets the same tools the request's array does. Otherwise a
    /// tool the user disabled, or one the host cannot service (ACP without
    /// form elicitation drops `question`), comes back through a script.
    #[test_case(&[PROBE_WIRE], &[]           ; "config_disabled")]
    #[test_case(&[],           &[PROBE_WIRE] ; "host_excluded")]
    fn callable_drops_what_the_requests_filter_dropped(disabled: &[&str], excluded: &[&str]) {
        let mut ctx = stub_ctx(&AgentMode::Build);
        ctx.registry = registry_with(&[PROBE_WIRE, OTHER_WIRE]);
        ctx.config.disabled_tools = disabled.iter().map(|n| (*n).to_owned()).collect();
        let tools = RequestTools::build(
            &ctx.registry,
            &Vars::new(),
            &ctx.model,
            &ctx.config,
            excluded,
            false,
            false,
        );
        ctx.tool_filter = Arc::clone(tools.filter());
        pin(&mut ctx);

        assert_eq!(tool_names(tools.definitions()), [OTHER_WIRE]);
        assert_eq!(callable_names(&ctx), [OTHER_WIRE]);
    }

    #[test]
    fn disabled_native_call_is_blocked_before_hooks() {
        smol::block_on(async {
            let (mut ctx, hook) = hooked_ctx(build_ctx());
            ctx.tool_filter = Arc::new(crate::tools::ToolFilter::Only(vec![]));
            let done = dispatch(&ctx, HOOK_TOOL_NAME, &call_input(HOOK_PLAIN)).await;
            assert_eq!(
                done.output.as_text(),
                format!("{UNAVAILABLE_TOOL_PREFIX}: {HOOK_TOOL_NAME}")
            );
            assert!(hook.seen.lock().unwrap().is_empty());
        });
    }

    /// A host that trims the array it publishes (a Lua caller passing `except`)
    /// has answered for the sandbox too, because the filter comes off that same
    /// array.
    #[test]
    fn callable_drops_a_name_the_published_array_left_out() {
        let mut ctx = stub_ctx(&AgentMode::Build);
        ctx.registry = registry_with(&[PROBE_WIRE, OTHER_WIRE]);
        let tools = RequestTools::assembled(
            serde_json::json!([{ TOOL_NAME_FIELD: OTHER_WIRE }]),
            &ctx.config,
            &ctx.model,
        );
        ctx.tool_filter = Arc::clone(tools.filter());
        pin(&mut ctx);

        assert_eq!(callable_names(&ctx), [OTHER_WIRE]);
    }

    /// Losing on audience is not the same as freeing the name: MCP publishing
    /// the wire name of a gated registry tool must not become a way around it.
    #[test]
    fn mcp_cannot_republish_a_name_the_registry_gated() {
        let mut ctx = mcp_ctx(&stub_mcp(&[PROBE_QUALIFIED]));
        ctx.registry = registered(mock_tool(PROBE_WIRE, ToolAudience::MAIN));
        ctx.audience = ToolAudience::GENERAL_SUB;
        assert!(!callable_names(&ctx).contains(&PROBE_WIRE.to_owned()));
    }

    /// A host tool this session's audience excludes is not a callable name, even
    /// though `resolve` would route to it.
    #[test]
    fn callable_drops_names_this_audience_cannot_reach() {
        let mut ctx = stub_ctx(&AgentMode::Build);
        ctx.local_tools = Arc::new(HashMap::from([(
            CLIENT_NAME.to_owned(),
            local_tool(ToolAudience::MAIN, |_, _| {
                Box::pin(async { Ok(String::new()) })
            }),
        )]));
        pin(&mut ctx);
        assert_eq!(callable_names(&ctx), [CLIENT_NAME]);

        ctx.audience = ToolAudience::GENERAL_SUB;
        assert!(callable_names(&ctx).is_empty());
    }

    #[test_case("srv.get_docs", "srv__get_docs", None                  ; "identifier_needs_no_alias")]
    #[test_case("srv.get-docs", "srv__get-docs", Some("srv__get_docs") ; "hyphen_becomes_underscore")]
    fn alias_is_set_only_when_the_name_is_not_an_identifier(
        qualified: &str,
        wire: &str,
        expected: Option<&str>,
    ) {
        let ctx = mcp_ctx(&stub_mcp(&[qualified]));
        let entry = callable(&ctx)
            .into_iter()
            .find(|c| c.name == wire)
            .expect("the published tool is callable");
        assert_eq!(entry.alias.as_deref(), expected);
    }

    /// Two names collapsing onto one alias would silently point a caller at the
    /// wrong tool, so neither gets one.
    #[test]
    fn colliding_aliases_are_dropped() {
        let ctx = mcp_ctx(&stub_mcp(&["srv.get-docs", "srv.get_docs"]));
        assert!(callable(&ctx).iter().all(|c| c.alias.is_none()));
    }

    /// The model only fixes names it recognizes, so it hears back what it sent.
    #[test_case(None, "nonexistent.tool" ; "without_mcp")]
    #[test_case(Some(PROBE_QUALIFIED), OTHER_WIRE ; "unpublished_wire_name")]
    fn unknown_tool_errors_and_echoes_the_name_verbatim(published: Option<&str>, name: &str) {
        smol::block_on(async {
            let mcp = published.map(|tool| stub_mcp(&[tool]));
            let ctx = match &mcp {
                Some(mcp) => mcp_ctx(mcp),
                None => stub_ctx(&AgentMode::Build),
            };
            let done = dispatch(&ctx, name, &serde_json::json!({})).await;
            assert!(done.is_error);
            assert_eq!(done.tool.as_ref(), UNKNOWN_MCP);
            let text = done.output.as_text();
            assert!(text.starts_with(UNKNOWN_TOOL_PREFIX), "got: {text}");
            assert!(text.contains(name), "got: {text}");
        });
    }

    #[test]
    fn restricted_mode_blocks_unknown_effect_before_hooks() {
        smol::block_on(async {
            let plan = AgentMode::Plan(PathBuf::from(PLAN_PATH));
            let (ctx, hook) = hooked_with(stub_ctx(&plan), None, RecordingHook::default());
            let done = dispatch(&ctx, HOOK_TOOL_NAME, &call_input(HOOK_PLAIN)).await;
            assert_eq!(
                done.output.as_text(),
                format!("{MODE_DENIED}: {HOOK_TOOL_NAME}")
            );
            assert!(hook.seen.lock().unwrap().is_empty());
        });
    }

    #[test]
    fn plan_mode_blocks_write_even_with_yolo() {
        smol::block_on(async {
            let plan = AgentMode::Plan(PathBuf::from(PLAN_PATH));
            let (ctx, hook) = hooked_with(stub_ctx(&plan), None, RecordingHook::default());
            ctx.permissions.set_yolo(true);
            let done = dispatch(&ctx, HOOK_TOOL_NAME, &call_input(PLAN_PATH)).await;
            assert!(done.is_error);
            assert_eq!(
                done.output.as_text(),
                format!("{MODE_DENIED}: {HOOK_TOOL_NAME}")
            );
            assert!(hook.seen.lock().unwrap().is_empty());
        });
    }

    #[test]
    fn plan_mode_blocks_shell_before_hooks_even_with_allow_rule() {
        smol::block_on(async {
            let plan = AgentMode::Plan(PathBuf::from(PLAN_PATH));
            let (ctx, hook) = hooked_with(
                ruled_ctx(&plan, ToolKey::native(HOOK_TOOL_NAME), Effect::Allow),
                Some(Permission::Run),
                RecordingHook::default(),
            );
            let done = dispatch(&ctx, HOOK_TOOL_NAME, &call_input(PLAN_PATH)).await;
            assert!(done.is_error);
            assert_eq!(
                done.output.as_text(),
                format!("{MODE_DENIED}: {HOOK_TOOL_NAME}")
            );
            assert!(hook.seen.lock().unwrap().is_empty());
        });
    }

    #[test]
    fn mcp_tool_denied_even_with_allow_rule_in_plan_mode() {
        smol::block_on(async {
            let plan = AgentMode::Plan(PathBuf::from(PLAN_PATH));
            let ctx = with_mcp(
                ruled_ctx(
                    &plan,
                    ToolKey::parse(PROBE_QUALIFIED).unwrap(),
                    Effect::Allow,
                ),
                &stub_mcp(&[PROBE_QUALIFIED]),
            );
            let done = dispatch(&ctx, PROBE_WIRE, &serde_json::json!({})).await;
            assert!(done.is_error);
            assert_eq!(
                done.output.as_text(),
                format!("{MODE_DENIED}: {PROBE_WIRE}")
            );
            let mut tools = serde_json::json!([]);
            ctx.mcp.as_ref().unwrap().extend_tools(&mut tools);
            assert!(
                !tool_names(&tools).contains(&&PROBE_WIRE.to_owned()[..]),
                "a restricted call must not load the definition"
            );
        });
    }

    /// An MCP server can write without announcing it, so a restricted mode
    /// rejects it even if ordinary permissions would allow it.
    #[test]
    fn mcp_tool_in_plan_mode_is_never_auto_approved() {
        smol::block_on(async {
            let plan = AgentMode::Plan(PathBuf::from(PLAN_PATH));
            let ctx = with_mcp(stub_ctx(&plan), &stub_mcp(&[PROBE_QUALIFIED]));
            let done = dispatch(&ctx, PROBE_WIRE, &serde_json::json!({})).await;
            assert!(done.is_error);
            let text = done.output.as_text();
            assert_eq!(text, format!("{MODE_DENIED}: {PROBE_WIRE}"));
            let mut tools = serde_json::json!([]);
            ctx.mcp.as_ref().unwrap().extend_tools(&mut tools);
            assert!(
                !tool_names(&tools).contains(&&PROBE_WIRE.to_owned()[..]),
                "an unapproved call must not load the definition"
            );
        });
    }

    #[test]
    fn mcp_tool_denied_by_rule_in_plan_mode() {
        smol::block_on(async {
            let plan = AgentMode::Plan(PathBuf::from(PLAN_PATH));
            let ctx = with_mcp(
                ruled_ctx(
                    &plan,
                    ToolKey::parse(PROBE_QUALIFIED).unwrap(),
                    Effect::Deny,
                ),
                &stub_mcp(&[PROBE_QUALIFIED]),
            );
            let done = dispatch(&ctx, PROBE_WIRE, &serde_json::json!({})).await;
            assert!(done.is_error, "plan mode must not bypass deny rules");
            assert!(
                done.output.as_text() == format!("{MODE_DENIED}: {PROBE_WIRE}"),
                "got: {}",
                done.output.as_text()
            );
        });
    }

    #[test]
    fn permission_denial_short_circuits_execute() {
        smol::block_on(async {
            let mut ctx = denying_ctx(ToolKey::native(GUARDED_TOOL_NAME));
            ctx.registry = registered(Arc::new(GuardedMock));

            let done = dispatch(&ctx, GUARDED_TOOL_NAME, &serde_json::json!({})).await;

            assert!(done.is_error, "permission denial must produce error event");
            assert!(
                done.output.as_text().starts_with(PERMISSION_DENIED_PREFIX),
                "error should be the permission-denied message, got: {}",
                done.output.as_text()
            );
        });
    }

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
        fn permission_scopes(
            &self,
            _session_id: Option<&SessionRef>,
        ) -> BoxFuture<'_, Option<PermissionScopes>> {
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
            let mut ctx = denying_ctx(ToolKey::native(START_PROBE_NAME));
            let probe = StartProbe::default();
            let (started, executed) = (Arc::clone(&probe.started), Arc::clone(&probe.executed));
            ctx.registry = registered(Arc::new(probe));

            let done = dispatch(&ctx, START_PROBE_NAME, &serde_json::json!({})).await;

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

    use std::time::Duration;

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
        fn mutable_path(&self, _ctx: &ToolContext) -> Option<PathBuf> {
            Some(PathBuf::from(&self.path))
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
        mut ctx: ToolContext,
        id: String,
        name: String,
        path: String,
    ) -> ToolDoneEvent {
        ctx.registry = registry;
        pin(&mut ctx);
        run(
            id,
            &name,
            &serde_json::json!({ "path": path }),
            &ctx,
            CallOrigin::Nested,
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
        fn mutable_path(&self, _ctx: &ToolContext) -> Option<PathBuf> {
            self.input["path"].as_str().map(PathBuf::from)
        }
        fn execute<'a>(self: Box<Self>, ctx: &'a ToolContext) -> ExecFuture<'a> {
            Box::pin(async move {
                let inner = run(
                    "inner".into(),
                    INNER_WRITE_NAME,
                    &self.input,
                    ctx,
                    CallOrigin::Nested,
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
        fn mutable_path(&self, _ctx: &ToolContext) -> Option<PathBuf> {
            Some(PathBuf::from(&self.path))
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
            pin(&mut ctx);

            let done = run(
                "outer".into(),
                RECURSIVE_WRITE_NAME,
                &serde_json::json!({ "path": "/reentrant" }),
                &ctx,
                CallOrigin::Nested,
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
