# Frontends as clients of one session interface

Status: approved by the user as a large refactor on top of PR 6 (`luna.24-pr-6-persistent-agents.md`).

## Goal

Every frontend (TUI, SDK, ACP, print, Lua session API) is a client of one session interface in `makima-agent`. The agent and storage layer owns every session fact and writes it once. Frontends translate: the TUI draws chats, SDK writes its wire format, ACP sends `session/update`, print prints. UI state (tabs, scroll, selection, pickers, the text in the input box, flashes) stays in UI code.

The interface covers the union of verbs the frontends perform today. The TUI has the most; SDK alone has fork.

## Related issues

- #24 PR 9 "Complete frontend migration and cleanup" (items 13 and 14 of the #24 plan): this work.
- #159 "Agent driving": one shared backend in `makima-agent` for every frontend, replacing `TuiActorBackend` and the headless split (step 9). The rest of #159 (widget protocol) is out of scope.
- #21 / #24 PR 8B: the same promptbox for every agent. Wake on submit already landed; the interface keeps every verb agent-addressed.
- #120 polymakima: a future out-of-process client. The interface is data-only at its edges (commands, projection, events) so it can be served over a protocol later.
- #43 SQLite: frontends depend on the interface, never on the `.jsonl` format.
- #168 subagent cost accounting: step 2 makes usage one fold in the agent layer and fixes the frozen session cost in the TUI status bar.
- #80: ACP lost feedback because state changes were not projected back. The shared event and projection side prevents that.
- #24 open question 11: frontends that cannot show the graph project a selected agent; the interface stays fully agent-addressable.

## What frontends do today (summary)

- Only the TUI writes `Title`, `Draft`, `Queued`, `Out` and `Post` records (`makima-ui/src/app/session.rs` `append_ui_record`). SDK, ACP and print sessions keep the default title and have no tool outputs or posts.
- Usage is folded twice: the TUI (`account_turn_complete`) and `HeadlessObserver::persist_usage` (`makima-lua/src/session_snapshot.rs`). The TUI session cost (`state.cost`) never grows after load.
- Opening and composing a session is written four times (SDK, ACP, print, TUI), and so are the event pumps. Submit has five call paths, cancel three, compaction two, isolated questions two, reset two, command hosts two, actor backends two (`TuiActorBackend`, `HeadlessBackend`).
- TUI-only verbs: shell `!cmd`, rewind, queue editing, move to background, cancel by tool id, plan approval with a linked session, back to planning, delete, list, title, draft, posts, MCP toggle, and all of `makima.session.*`. SDK-only: fork.
- The TUI keeps a whole `AppSession` copy and writes into it by hand.

## Interface

New module `makima-agent/src/session/`. `makima-agent` cannot depend on `makima-lua`, so Lua wiring comes in through a trait.

```rust
pub trait FrontendServices: Send + Sync {
    fn bind_root(&self, session: &SessionHandle) -> Result<Arc<dyn CommandHost>, String>;
    fn observe(&self, envelope: &Envelope);
}

pub enum SessionSource {
    Existing(MakimaId),
    New(SessionSeed),
    ExistingOrNew(SessionSeed),
    Fork { from: MakimaId },
    Linked { parent: MakimaId, seed: SessionSeed },
}
pub enum Durability { Persistent, Ephemeral }

pub struct FrontendProfile {
    pub question_mode: QuestionMode,
    pub interactive_reauth: bool,
    pub tool_output_lines: ToolOutputLines,
    pub excluded_tools: Vec<&'static str>,
    pub mcp: Option<McpHandle>,
}

pub struct OpenRequest<'a> {
    pub source: SessionSource,
    pub model: OpenModel<'a>,
    pub cwd: Option<&'a str>,
    pub durability: Durability,
    pub profile: FrontendProfile,
    pub continue_turns: bool,
}

impl SessionHost {
    pub async fn open(&self, request: OpenRequest<'_>) -> Result<SessionHandle, OpenSessionError>;
    pub fn list(&self, scope: ListScope) -> Result<Vec<SessionRow>, SessionError>;
    pub fn read(&self, id: MakimaId) -> Result<Arc<SessionProjection>, SessionError>;
    pub fn set_title(&self, id: MakimaId, title: String) -> Result<(), SessionError>;
    pub fn delete(&self, id: MakimaId) -> Result<(), SessionError>;
}

impl SessionHandle {
    pub fn id(&self) -> MakimaId;
    pub fn root(&self) -> AgentRef;
    pub fn agent(&self, id: AgentId) -> Result<AgentRef, ManagerError>;

    pub fn submit(&self, target: AgentId, input: AgentInput, options: SubmitOptions) -> Result<(TurnTicket, u64), SubmitError>;
    pub fn withdraw(&self, target: AgentId, pending: PendingSubmissionId) -> Result<(), ActorError>;
    pub fn cancel(&self, scope: CancelScope) -> Result<(), ManagerError>;
    pub fn answer(&self, agent: AgentId, answer: TaggedAnswer) -> Result<(), String>;
    pub fn ask_isolated(&self, target: AgentId, question: String, images: Vec<ImageSource>, cancel: CancelToken) -> Result<flume::Receiver<IsolatedTurnEvent>, String>;
    pub fn move_to_background(&self, target: AgentId) -> Result<String, String>;

    pub async fn wake(&self, target: AgentId) -> Result<(), WakeError>;
    pub fn close_agent(&self, target: AgentId) -> Result<(), ManagerError>;

    pub fn begin_rewind(&self, target: AgentId) -> Result<RewindGuard, RewindError>;
    pub fn compact(&self, target: AgentId, instructions: Option<String>, cancel: CancelToken) -> Result<flume::Receiver<ManualCompactionEvent>, String>;

    pub async fn set_option(&self, target: AgentId, id: &str, value: Option<Arc<str>>) -> Result<SessionOptionsSnapshot, ConfigChangeError>;
    pub async fn select_model(&self, target: AgentId, selection: ModelSelection) -> Result<ConfigCommit, ConfigChangeError>;
    pub async fn set_mode(&self, target: AgentId, key: &str) -> Result<ConfigCommit, ConfigChangeError>;
    pub async fn change_directory(&self, target: AgentId, path: PathBuf) -> Result<PathBuf, ConfigChangeError>;
    pub async fn reload_context(&self, target: AgentId) -> Result<(), ConfigChangeError>;
    pub async fn approve_plan(&self, target: AgentId, approval: PlanApproval) -> Result<Option<SessionSource>, ConfigChangeError>;
    pub fn command_host(&self) -> Arc<dyn CommandHost>;

    pub fn set_title(&self, title: String);
    pub fn set_draft(&self, draft: Option<String>);
    pub fn post(&self, target: Option<AgentId>, post: Post);

    pub fn projection(&self) -> Arc<SessionProjection>;
    pub fn subscribe(&self) -> SessionEvents;
    pub fn options(&self) -> SessionOptionsSnapshot;
    pub fn subscribe_options(&self) -> SessionOptionsSubscription;
    pub fn writer_status(&self) -> WriterStatus;

    pub async fn reset(self) -> Result<SessionHandle, SessionError>;
    pub async fn close(self, timeout: Duration) -> ShutdownReport;
}

pub enum CancelScope { Turn(TurnTicket), Subtree(AgentId), ToolUse(String) }

pub struct SessionProjection {
    pub id: MakimaId,
    pub revision: u64,
    pub title: String,
    pub cwd: PathBuf,
    pub model: Arc<str>,
    pub parent: Option<ParentLink>,
    pub updated_at: u64,
    pub root: AgentId,
    pub agents: BTreeMap<AgentId, AgentProjection>,
    pub tool_outputs: HashMap<String, Arc<ToolOutput>>,
    pub posts: Vec<StoredPost>,
    pub usage: SessionUsage,
    pub plan: PlanSlot,
    pub draft: Option<String>,
    pub state: SessionStateView,
}

pub enum SessionEvent {
    Agent(Envelope),
    Actor { agent: AgentId, event: ActorEvent },
    Projection(u64),
    Options(SessionOptionsSnapshot),
    Writer(WriterStatus),
    Closed,
}
```

One event pump per session: raw events go to `FrontendServices::observe` (Lua autocmds), then subscribers. The journal derives the title when it commits root history. The agent records usage before `TurnComplete` and records tool outputs and posts before emitting their events.

These become internal: the coordinator handle types, `SessionWriter` and `Record`, `open_session` and the recovery types, the headless spawn functions and `InteractiveHandle`, `TurnAdmitter`/`RootSubmission`, `project_session_journal`, and `apply_session_checkpoint` (deleted). Names drop "resume": `SessionSource::Resume` becomes `Existing`, `ResumeOrCreate` becomes `ExistingOrNew`.

The signatures are the target shape. Steps may adjust them when the code shows a smaller correct form; the plan is updated when they do.

## Steps

One commit per step. Each step is verified on the build box (check, workspace nextest, clippy, gen-docs) before the next starts.

0. SDK, ACP and print share `SessionEventPump` in `makima-agent/src/session/`. The journal records facts at their producers. The event pump does not record facts.
1. Title: the journal derives the title on the first root-history commit with user text while the title is the default, and appends `Title` in the same flush. Until the handle and host exist, explicit titles go through `SessionJournal::set_title` for live sessions and `makima_agent::session::set_title` for stored sessions. Delete the TUI title diff and append, `update_title_if_default` in `AppSession::checkpoint`, and `apply_session_checkpoint`.
2. Usage: the agent records each completed provider response in `SessionJournal` before emitting `TurnComplete`, including tool rounds and successful compaction. The journal folds `SessionUsage` and appends cumulative `Usage` through its writer. It restores the fold from stored totals, not event replay. Delete `HeadlessObserver::persist_usage` and the TUI's persistence in `account_turn_complete`, plus `SessionState.cost`/`token_usage`. The status bar reads `SessionJournal::cost` until the session projection is available (#168).
3. Drop `terminal_subagents` and `subagent_channels.closed`; read agent lifecycle from the agent layer.
4. `Out`: shared tool dispatch records the final output after offload, before the foreground and background completion paths diverge. The journal writes each tool-use ID once and projects the outputs. Delete the TUI writes.
5. `Post`: Lua post dispatch, context notices, and permission warnings record posts directly in the journal. The journal anchors each post to the target agent's last committed message ID. `StoredPost.chat` (tool-use ID) becomes `agent: Option<AgentId>`, with no agent for root posts. The v2 loader maps legacy chat keys to agent IDs. Delete TUI persistence and keep live display.
6. Work held after a failed turn: the TUI backend opts in at actor creation. SDK, ACP, and print continue draining the queue after failure. A failed turn or compaction records `StateChange::InputHold` for queued user admissions. Recovery keeps held admissions without continuing them. The next accepted user submit releases the hold and queues a distinct turn behind held work, even for Join. Local cancel discards held admissions. The TUI displays the actor queue and reads its hold state before a mailbox wake. Remove `recoverable_queue` and the v3 `Queued` record. The v2 reader still folds legacy queued text into the draft.
7. Shell `!cmd`: `AgentRef::run_shell` takes the shell settings, cancellation token and output callback. It runs in the selected actor's cwd. Both forms display output. Only `!` records a `StateChange::Preamble` for the agent. The next accepted user submission admits a distinct turn; one `TurnInput` record admits it, consumes the preamble IDs and releases any input hold. Previously held turns and mailbox wakes do not take the preamble. Delete the TUI buckets and all reads and drains. The previous `admit_user_turn_on` already drained after a successful submit; `SendMessage` only served mailbox wakes on this branch.
8. `SessionJournalSnapshot` also serves as the read projection. `SessionJournal::projection()` shares the committed snapshot by `Arc`. The TUI refreshes its reference without copying the histories and keeps the actor's shared history for in-flight display. `SessionJournal::set_draft` writes the change-checked `Draft` record. Model and cwd come from committed config. Remove the TUI mirror writes, `App::checkpoint`, `checkpoint_all`, `AppSession::checkpoint`, and `project_session_journal`. `AgentRef::begin_rewind()` reads the journal directly. Reload carries tab IDs and reopens the logs under their locks, including tabs whose cwd changed during the previous UI run.
9. One shared actor backend and session host:
   - 9a: Share the startup config checkpoint and compaction model resolution. Remove `InteractiveControl::Compact`, the `ModelSource` implementation for `ProviderSlot`, and the unused `InteractiveHandle.model` projection and `SharedModel`.
   - 9b: Configure `HeadlessBackend` through `FrontendProfile`: `question_mode`, `interactive_reauth`, `tool_output_lines`, `hold_input_after_failure`, and `excluded_tools`. Support shared or owned MCP, with MCP-ready gating, cancellable initialization and OAuth. Share `file_write_locks`, check `cancel_setup` early, emit `ControlError` on setup failure, and emit `ModelSwitched` per turn. The system prompt override and append belong to host config. The TUI remains on its backend.
   - 9c: Run the TUI root and children on the shared backend and delete `TuiActorBackend`. Adopt terminal checkpoint semantics explicitly.
   - 9d: Replace `command_router`, `AgentCommand` and `WorkCancel::Turn` with `CancelScope { Turn(TurnTicket), Subtree(AgentId), ToolUse(String) }`. Use one answer map. Run isolated questions (`btw`) outside the actor queue for every frontend, using pinned context and a history snapshot so they can run during an active turn, as in the TUI today. Delete `btw_system` and the TUI's separate provider path for `btw`.
   - 9e-1: Use one `SessionHost::open` for SDK, ACP and print.
   - 9e-2: Move the TUI to `SessionHost::open`. Define reset once. The TUI carries root config and permissions into the new session. Match that behavior in SDK or document why it differs. ACP and print may omit reset.

Log format changes (v3 is unreleased, no migration): `StateChange::InputHold` and no `Queued` (6), `StateChange::Preamble` (7), `StoredPost.agent` (5).

## Done when

- `grep` finds no `append_ui_record`, no `SessionWriter` use and no `AppSession` mutation in `makima-ui`, `src/sdk_mode.rs`, `src/print.rs` or `makima-acp` outside tests.
- An SDK-created session opened in the TUI shows its title, tool outputs, posts and cost.
- The TUI session cost grows during a run.
- Full verify passes twice, the bridge check passes, and a hands-on release run against the fake server covers spawn, rewind, restart, dormant wake, shell, btw and cost.
- Docs: `docs/agent-system.md`, `docs/agent-invariants.md`, `AGENTS.md` architecture lines, and the headless/acp/cli docs describe the interface where they mention frontends.
