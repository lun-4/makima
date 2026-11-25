use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use crate::chat::{Chat, history_to_display_with_notes, history_to_display_with_target};
use crate::components::DisplayRole;
use crate::components::{Action, SessionReplacementKind, SessionReplacementRequest};
use crate::repaint::Dirty;
use makima_agent::actor::HistoryChange;
use makima_agent::agent::estimate_message_tokens;
use makima_agent::journal::{SessionJournal, SessionJournalSnapshot};
use makima_agent::{AgentId, AgentRef, HistorySnapshot, ModeId};
#[cfg(test)]
use makima_providers::Model;
use makima_storage::id::MakimaId;
use makima_storage::session_state::StoredAgentLifecycle;
use makima_storage::sessions::{ContextEntry, StoredMode, StoredPost, context_entries};

use crate::AppSession;
use makima_providers::RequestOptions;

use super::App;
#[cfg(test)]
use super::PendingInput;
#[cfg(test)]
use super::session_state::SessionState;
#[cfg(test)]
use makima_agent::permissions::PermissionSnapshot;

const DORMANT_AGENT_TEXT: &str = "Dormant";
const CLOSED_AGENT_TEXT: &str = "Closed";
const RETIRED_AGENT_TEXT: &str = "Retired";

pub(crate) fn session_has_content(session: &SessionJournalSnapshot) -> bool {
    session
        .root_history()
        .is_some_and(|history| !history.messages.is_empty())
        || !session.posts.is_empty()
        || session.draft().is_some()
        || session
            .histories
            .values()
            .any(|history| !history.messages.is_empty())
        || session.fold().is_ok_and(|view| {
            view.agents.values().any(|record| record.parent.is_some())
                || view
                    .scopes
                    .values()
                    .any(|scope| scope.plugins.values().any(|stored| stored.value.is_some()))
                || session
                    .root_agent_id()
                    .and_then(|root| view.scopes.get(&Some(root)))
                    .and_then(|scope| scope.config.as_ref())
                    .map_or_else(
                        || {
                            matches!(
                                session.seed_meta().mode,
                                Some(StoredMode::Plan | StoredMode::Custom(_))
                            )
                        },
                        |config| config.mode.id != ModeId::Build.key(),
                    )
        })
}

impl App {
    pub(crate) fn journal(&self) -> Option<Arc<SessionJournal>> {
        let journal = self
            .root_agent
            .as_ref()
            .map(|root| root.manager().journal());
        #[cfg(test)]
        let journal = journal
            .filter(|journal| journal.session_id() == Some(self.state.projection.id))
            .or_else(|| self.fixture_journal.clone());
        journal
    }

    pub(crate) fn session_projection(&self) -> Arc<SessionJournalSnapshot> {
        Arc::clone(&self.state.projection)
    }

    pub(crate) fn refresh_session_projection(&mut self) -> bool {
        let Some(journal) = self.journal() else {
            return false;
        };
        let projection = journal.projection();
        if Arc::ptr_eq(&self.state.projection, &projection) {
            return false;
        }
        self.state.projection = projection;
        true
    }

    pub(crate) fn has_content(&self) -> bool {
        session_has_content(&self.state.projection)
    }

    pub(crate) fn sync_writer_warning(&mut self) -> crate::repaint::Dirty {
        let warning = self
            .journal()
            .and_then(|journal| journal.persistence_error());
        self.status_bar.set_writer_warning(warning)
    }

    pub(crate) fn persist_input(&mut self) {
        if self.restore_in_progress || self.rewind_pending {
            return;
        }
        let lines = self.input_box.buffer.lines();
        let draft = self.state.projection.draft().unwrap_or("");
        if !draft.split('\n').eq(lines.iter().map(String::as_str))
            && let Some(journal) = self.journal()
        {
            let draft = lines.join("\n");
            journal.set_draft((!draft.is_empty()).then_some(draft));
        }
    }

    pub(super) fn session_cost(&self) -> Option<f64> {
        self.state.projection.usage().cost
    }

    pub(crate) fn title(&self) -> String {
        self.state.projection.title().to_owned()
    }

    pub(crate) fn set_title(&mut self, title: String) {
        if let Some(journal) = self.journal() {
            journal.set_title(title);
            self.refresh_session_projection();
        }
    }

    pub(super) fn save_input_history(&self) {
        if let Err(e) = self.input_box.history().save(&self.storage) {
            tracing::warn!(error = %e, "input history save failed");
        }
    }

    #[cfg(test)]
    pub(super) fn reset_ui_chrome(&mut self) {
        let _ = self.cancel_middle_scroll();
        let splash_frame = self.chats.first().and_then(Chat::splash_frame).cloned();
        let root = self.chats.first().and_then(|chat| chat.agent_id);
        self.chats.clear();
        let mut main = Chat::new(
            "Main".into(),
            self.ui_config.clone(),
            self.lua_event_handle.clone(),
            Arc::clone(&self.theme_provider),
        );
        main.set_restore_channel(self.restore_event_tx.clone());
        main.set_splash_frame(splash_frame);
        main.agent_id = root;
        self.chats.push(main);
        self.active_chat = 0;
        self.chat_index.clear();
        self.agent_chat_index.clear();
        self.status = super::Status::Idle;
        self.clear_exit_request();
        self.queue.clear_root_user_work();
        self.close_all_overlays();
        self.pending_input = PendingInput::None;
        self.status_bar.clear_flash();
        self.task_picker_original = None;
        self.last_esc = None;
        self.restoring = Arc::new(AtomicBool::new(false));
        self.plan_form.reset();
        self.plan_approval_pending = false;
    }

    fn fork_notes(&self, agent: Option<AgentId>) -> HashMap<MakimaId, String> {
        agent
            .and_then(|agent| context_entries(self.state.projection.context_events(), agent).ok())
            .unwrap_or_default()
            .into_iter()
            .filter_map(|entry| match entry {
                ContextEntry::Note { id, note } => Some((id, note)),
                ContextEntry::Message { .. } => None,
            })
            .collect()
    }

    fn root_fork_notes(&self) -> HashMap<MakimaId, String> {
        self.fork_notes(
            self.state
                .projection
                .root_agent_id()
                .or_else(|| self.state.projection.id.to_string().parse().ok()),
        )
    }

    pub(crate) fn finish_child_rewind(
        &mut self,
        agent: AgentRef,
        history: HistorySnapshot,
        draft: String,
    ) {
        self.refresh_session_projection();
        self.finish_rewind_input(draft);
        let id = agent.id();
        let Some(&index) = self.agent_chat_index.get(&id) else {
            return;
        };
        let notes = self.fork_notes(Some(id));
        let (display, items) = history_to_display_with_notes(
            &history.messages,
            &self.state.projection.tool_outputs,
            &self.ui_config.tool_output_lines,
            &posts_for(&self.state.projection.posts, Some(id)),
            Some(agent),
            (&history.ids, &notes),
        );
        self.chats[index].load_messages(display);
        self.chats[index].retry_info = None;
        self.fire_restore_items(items);
    }

    pub(crate) fn finish_root_rewind(&mut self, draft: String) {
        self.refresh_session_projection();
        self.queue.unfocus_root();
        let notes = self.root_fork_notes();
        let (messages, ids) = history_display_parts(self.state.projection.root_history());
        let (display, items) = history_to_display_with_notes(
            messages,
            &self.state.projection.tool_outputs,
            &self.ui_config.tool_output_lines,
            &posts_for(&self.state.projection.posts, None),
            self.root_agent.clone(),
            (ids, &notes),
        );
        self.chats[0].load_messages(display);
        self.fire_restore_items(items);
        self.finish_rewind_input(draft);
    }

    fn finish_rewind_input(&mut self, draft: String) {
        self.refresh_at_ref_labels(&draft);
        self.input_box.set_input(draft);
        self.input_box.buffer.move_to_end();
        self.pending_dirty |= Dirty::YES;
    }

    pub(crate) fn restore_display(&mut self) {
        self.refresh_session_projection();
        let restoring = Arc::new(AtomicBool::new(true));
        self.restoring = restoring.clone();

        let view = self.state.projection.fold().unwrap_or_else(|error| {
            tracing::warn!(%error, "could not fold restored child configuration");
            Default::default()
        });
        let cost = self.session_cost();
        let root_id = self.state.projection.root_agent_id();
        let context_size = root_id
            .and_then(|root| view.scopes.get(&Some(root)))
            .and_then(|scope| scope.results.back())
            .map_or(
                self.state.projection.seed_meta().context_size,
                |(_, result)| result.outcome.context_size,
            );
        let main = self.main_chat();
        main.agent_id = root_id.or(main.agent_id);
        main.cost = cost;
        main.context_size = context_size;
        if let Some(id) = main.agent_id {
            self.agent_chat_index.insert(id, 0);
        }
        if let Some(draft) = self.state.projection.draft().map(str::to_owned) {
            self.refresh_at_ref_labels(&draft);
            self.input_box.set_input(draft);
            self.input_box.buffer.move_to_end();
        }

        let mut children: Vec<_> = view
            .agents
            .values()
            .filter(|record| record.parent.is_some())
            .collect();
        children.sort_by_key(|record| record.created_seq);
        for record in children {
            let idx = self.chats.len();
            self.chat_index.insert(record.transcript_key.clone(), idx);
            self.agent_chat_index.insert(record.id, idx);
            let mut chat = Chat::new(
                record
                    .label
                    .clone()
                    .unwrap_or_else(|| record.id.to_string()),
                self.ui_config.clone(),
                self.lua_event_handle.clone(),
                Arc::clone(&self.theme_provider),
            );
            chat.set_restore_channel(self.restore_event_tx.clone());
            chat.subagent_id = Some(record.transcript_key.clone());
            chat.agent_id = Some(record.id);
            chat.parent_agent_id = record.parent;
            let scope = view.scopes.get(&Some(record.id));
            let last_turn = scope.and_then(|scope| {
                scope
                    .admitted
                    .keys()
                    .chain(scope.results.iter().map(|(turn, _)| turn))
                    .max()
                    .copied()
            });
            let started_at = last_turn
                .and_then(|turn| turn.created_at())
                .or_else(|| record.id.created_at());
            chat.restore_turn(last_turn, started_at);
            chat.context_size = scope
                .and_then(|scope| scope.results.back())
                .map_or(0, |(_, result)| result.outcome.context_size);
            if let Some(config) = scope.and_then(|scope| scope.config.as_ref()) {
                chat.model_id = Some(config.model.clone());
                chat.opts = Some(RequestOptions {
                    thinking: config.thinking.map(Into::into).unwrap_or_default(),
                    fast: config.fast,
                });
            } else if let Some(spec) = &record.spec {
                chat.model_id = spec.model.spec.as_ref().map(ToString::to_string);
                chat.opts = spec.model.thinking.map(|thinking| RequestOptions {
                    thinking,
                    fast: spec.model.fast.unwrap_or_default(),
                });
            }
            self.chats.push(chat);
        }

        for chat in &mut self.chats {
            let target = chat.agent_id.and_then(|id| {
                self.root_agent
                    .as_ref()
                    .map(|root| root.manager().reference(id))
            });
            chat.set_restore_target(target);
        }

        let projection = self.session_projection();
        let posts = &projection.posts;
        let notes = self.root_fork_notes();
        let (messages, ids) = history_display_parts(self.state.projection.root_history());
        let (display, items) = history_to_display_with_notes(
            messages,
            &self.state.projection.tool_outputs,
            &self.ui_config.tool_output_lines,
            &posts_for(posts, None),
            self.chats[0].agent_id.and_then(|id| {
                self.root_agent
                    .as_ref()
                    .map(|root| root.manager().reference(id))
            }),
            (ids, &notes),
        );
        self.chats[0].load_messages(display);
        self.fire_restore_items(items);

        for index in 1..self.chats.len() {
            let history = self.chats[index]
                .agent_id
                .and_then(|id| self.state.projection.histories.get(&id));
            let (messages, ids) = history_display_parts(history);
            let notes = self.fork_notes(self.chats[index].agent_id);
            let (display, items) = history_to_display_with_notes(
                messages,
                &self.state.projection.tool_outputs,
                &self.ui_config.tool_output_lines,
                &posts_for(posts, self.chats[index].agent_id),
                self.chats[index].agent_id.and_then(|id| {
                    self.root_agent
                        .as_ref()
                        .map(|root| root.manager().reference(id))
                }),
                (ids, &notes),
            );
            self.chats[index].load_messages(display);
            self.fire_restore_items(items);
            let status = self.chats[index]
                .agent_id
                .and_then(|id| view.agents.get(&id))
                .map_or(CLOSED_AGENT_TEXT, |record| match record.lifecycle {
                    StoredAgentLifecycle::Open => DORMANT_AGENT_TEXT,
                    StoredAgentLifecycle::Closed => CLOSED_AGENT_TEXT,
                    StoredAgentLifecycle::Retired => RETIRED_AGENT_TEXT,
                });
            self.chats[index].mark_finished(DisplayRole::Done, status);
        }

        let eh = &self.lua_event_handle;
        if eh.is_disconnected() {
            self.restoring
                .store(false, std::sync::atomic::Ordering::Relaxed);
        } else {
            eh.send_restore_complete(restoring);
        }
    }

    fn fire_restore_items(&self, items: Vec<makima_lua::RestoreItem>) {
        let Some(tx) = &self.restore_event_tx else {
            return;
        };
        let eh = &self.lua_event_handle;
        let theme_gen = self.theme_provider.generation();
        for mut item in items {
            item.theme_gen = Some(theme_gen);
            eh.request_restore(item, tx.clone());
        }
    }

    pub(crate) fn restore_resumed_session(&mut self) {
        self.restore_display();
        for w in self.state.warnings.drain(..) {
            self.status_bar.flash(w);
        }
    }

    pub(super) fn reset_session(&mut self) -> Vec<Action> {
        let ended_id = self.state.projection.id;
        let config = self.root_config();
        let session = AppSession::new(
            &config.model.spec(),
            &config.context.cwd().to_string_lossy(),
        );
        vec![Action::ReplaceSession(Box::new(
            SessionReplacementRequest {
                session,
                kind: SessionReplacementKind::Reset { ended_id },
                provider: None,
            },
        ))]
    }

    pub(crate) fn apply_history_change(&mut self, change: HistoryChange) {
        let Some(index) = self
            .chats
            .iter()
            .position(|chat| chat.agent_id == Some(change.agent_id))
        else {
            return;
        };
        let draft = change.draft.user_text().unwrap_or("").to_owned();
        let previous_history = self.state.projection.histories.get(&change.agent_id);
        let previous_tokens =
            previous_history.map_or(0, |history| estimate_message_tokens(&history.messages));
        let context_size = self.chats[index]
            .context_size
            .saturating_sub(previous_tokens)
            .saturating_add(estimate_message_tokens(&change.messages));
        self.refresh_session_projection();
        let target = self
            .root_agent
            .as_ref()
            .map(|root| root.manager().reference(change.agent_id));
        let projection = self.session_projection();
        let ids = projection
            .histories
            .get(&change.agent_id)
            .filter(|history| history.epoch == change.epoch)
            .map_or(&[][..], |history| history.ids.as_slice());
        let (display, items) = history_to_display_with_target(
            &change.messages,
            &projection.tool_outputs,
            &self.ui_config.tool_output_lines,
            &posts_for(&projection.posts, (index != 0).then_some(change.agent_id)),
            target,
            ids,
        );
        self.chats[index].load_messages(display);
        self.chats[index].context_size = context_size;
        self.fire_restore_items(items);
        self.refresh_at_ref_labels(&draft);
        self.input_box.set_input(draft);
        self.input_box.buffer.move_to_end();
    }

    #[cfg(test)]
    pub(crate) fn set_session_fixture(&mut self, session: &AppSession) {
        let journal = Arc::new(SessionJournal::from_session(session).unwrap());
        self.state.projection = journal.projection();
        self.fixture_journal = Some(journal);
    }

    #[cfg(test)]
    pub(crate) fn session_fixture(&self) -> AppSession {
        let snapshot = self.journal().unwrap().projection();
        let mut session = AppSession::new(snapshot.model(), snapshot.cwd());
        session.id = snapshot.id;
        session.updated_at = snapshot.updated_at;
        session.meta = snapshot.seed_meta().clone();
        session.set_parent(snapshot.parent().cloned());
        session.replace_agent_revisions(snapshot.agent_revisions.to_vec());
        session.replace_state(snapshot.state.to_vec());
        snapshot.apply_to(&mut session).unwrap();
        if let Some(history) = snapshot.root_history() {
            session.adopt_history(history);
        }
        for record in snapshot.fold().unwrap().agents.into_values() {
            if record.parent.is_some()
                && let Some(history) = snapshot.histories.get(&record.id)
            {
                session.adopt_subagent_history(record.transcript_key, history);
            }
        }
        session.replace_context_events(snapshot.context_events().to_vec());
        session.meta.input_draft = snapshot.draft().map(str::to_owned);
        session
    }

    #[cfg(test)]
    pub(crate) fn edit_session_fixture<T>(&mut self, edit: impl FnOnce(&mut AppSession) -> T) -> T {
        let mut session = self.session_fixture();
        let result = edit(&mut session);
        self.set_session_fixture(&session);
        result
    }

    #[cfg(test)]
    pub(crate) fn apply_loaded_session(&mut self, session: AppSession, _fallback_model: &Model) {
        self.rotate_command_target();
        let view = session.latest_state().unwrap();
        let snapshot = view
            .scopes
            .get(&None)
            .map_or_else(
                || PermissionSnapshot::from_stored(&session.meta.session_rules, session.meta.yolo),
                |scope| PermissionSnapshot::from_stored(&scope.rules, scope.yolo),
            )
            .unwrap();
        self.permissions.restore(snapshot);
        self.fixture_journal = Some(Arc::new(SessionJournal::from_session(&session).unwrap()));
        self.state = SessionState::from_session(&session).unwrap();
        for w in self.state.warnings.drain(..) {
            self.status_bar.flash(w);
        }
        self.reset_ui_chrome();
        self.restore_display();
        self.shared_history = None;
    }

    /// Applies an already-loaded session, for callers that loaded it
    /// themselves (e.g. to run their own checks first).
    #[cfg(test)]
    pub(crate) fn load_loaded_session(&mut self, session: AppSession) -> Vec<Action> {
        vec![Action::ReplaceSession(Box::new(
            SessionReplacementRequest {
                session,
                kind: SessionReplacementKind::Load,
                provider: None,
            },
        ))]
    }
}

fn history_display_parts(
    history: Option<&HistorySnapshot>,
) -> (&[makima_providers::Message], &[MakimaId]) {
    history.map_or((&[], &[]), |history| (&history.messages, &history.ids))
}

fn posts_for(posts: &[StoredPost], agent: Option<AgentId>) -> Vec<&StoredPost> {
    posts
        .iter()
        .filter(|stored| stored.agent == agent)
        .collect()
}

#[cfg(test)]
mod tests {
    use makima_agent::actor::{ConfigChange, ConfigPatch, HistoryChange};
    use makima_agent::journal::SessionJournal;
    use makima_agent::permissions::PermissionSnapshot;
    use makima_agent::{AgentId, HistorySnapshot, SubagentInfo, ToolOutput};
    use makima_domain::{Post, PostLevel};
    use makima_providers::{
        ContentBlock, Effort, Message, ModelSelection, RequestOptions, Role, ThinkingConfig,
    };
    use std::sync::Arc;

    use makima_storage::session_state::{
        StateChange, StoredAgentConfig, StoredAgentLifecycle, StoredAgentSpec, StoredModeCeiling,
        StoredModeSnapshot, StoredStateRecord,
    };
    use makima_storage::sessions::{StoredEffect, StoredRule};

    use super::{
        CLOSED_AGENT_TEXT, DORMANT_AGENT_TEXT, DisplayRole, RETIRED_AGENT_TEXT, session_has_content,
    };

    const TRANSCRIPT: &str = "configured-child";
    const CONFIG_MODEL: &str = "provider/committed";
    const SPAWN_MODEL: &str = "provider/spawn";

    #[test_case::test_case(false; "root")]
    #[test_case::test_case(true; "child")]
    fn journal_post_anchor_survives_display_restore_and_history_change(child: bool) {
        const PROMPT: &str = "original prompt";
        const POST: &str = "review result";
        let mut app = super::super::tests::test_app();
        let root = AgentId::generate();
        let child_id = AgentId::generate();
        let mut session = app.session_fixture();
        session.bind_root_id(root).unwrap();
        if child {
            let mut record = session.agents()[&root].clone();
            record.id = child_id;
            record.parent = Some(root);
            record.depth = 1;
            record.transcript_key = TRANSCRIPT.into();
            let seq = session.next_state_seq();
            record.created_seq = seq;
            record.updated_seq = seq;
            session.push_agent_revision(seq, record);
            session.replace_journal_histories(
                &[(
                    child_id,
                    HistorySnapshot::new(vec![Message::user(PROMPT.into())]),
                )]
                .into(),
            );
        } else {
            session.push_message(Message::user(PROMPT.into()));
        }
        let agent = if child { child_id } else { root };
        let journal = SessionJournal::from_session(&session).unwrap();
        journal.record_post(
            agent,
            Post {
                text: POST.into(),
                level: PostLevel::Info,
                agent: None,
            },
        );
        journal.snapshot().apply_to(&mut session).unwrap();
        let history = if child {
            session.subagent_history_snapshot(TRANSCRIPT).unwrap()
        } else {
            session.history_snapshot()
        };
        app.set_session_fixture(&session);
        app.restore_display();
        let index = app.agent_chat_index[&agent];
        assert_eq!(app.chats[index].message_at(0).unwrap().text, PROMPT);
        assert_eq!(app.chats[index].message_at(1).unwrap().text, POST);
        app.apply_history_change(HistoryChange {
            agent_id: agent,
            epoch: history.epoch,
            messages: history.messages,
            draft: Message::user(PROMPT.into()),
        });
        assert_eq!(app.chats[index].message_at(1).unwrap().text, POST);
    }

    #[test]
    fn restored_tool_renders_the_projected_output() {
        const TOOL_ID: &str = "restore-tool";
        const OUTPUT: &str = "complete output from the journal";
        let mut app = super::super::tests::test_app();
        let mut session = app.session_fixture();
        session.push_message(Message {
            role: Role::Assistant,
            content: vec![ContentBlock::tool_use(
                TOOL_ID,
                "bash",
                serde_json::json!({}),
            )],
            ..Default::default()
        });
        session.push_message(Message {
            role: Role::User,
            content: vec![ContentBlock::ToolResult {
                tool_use_id: TOOL_ID.into(),
                content: "provider preview".into(),
                is_error: false,
            }],
            ..Default::default()
        });
        let mut snapshot = SessionJournal::from_session(&session).unwrap().snapshot();
        Arc::make_mut(&mut snapshot.tool_outputs)
            .insert(TOOL_ID.into(), Arc::new(ToolOutput::Plain(OUTPUT.into())));
        snapshot.apply_to(&mut session).unwrap();
        app.set_session_fixture(&session);
        app.restore_display();
        let tool = app.chats[0].message_at(0).unwrap();
        assert!(matches!(&tool.role, DisplayRole::Tool(role) if role.id == TOOL_ID));
        assert_eq!(tool.tool_output.as_deref().unwrap().as_text(), OUTPUT);
    }

    fn stored_config() -> StoredAgentConfig {
        StoredAgentConfig {
            cwd: "/tmp".into(),
            model: CONFIG_MODEL.into(),
            thinking: Some(ThinkingConfig::Effort(Effort::High).into()),
            fast: true,
            workflow: true,
            mode: StoredModeSnapshot {
                id: "build".into(),
                label: "Build".into(),
                system_prompt: None,
                restrict_write_to: None,
                tools: None,
            },
            options: Default::default(),
        }
    }

    #[test]
    fn persisting_draft_does_not_change_live_config_or_permissions() {
        let mut app = super::super::tests::test_app();
        app.state.plan = super::super::mode::PlanState::Drafting(TRANSCRIPT.into());
        let actor = app.queue.root_actor().unwrap();
        smol::block_on(
            actor
                .reserve_prepared_operation(None)
                .unwrap()
                .commit_when_ready(Some(ConfigChange::Patch(ConfigPatch {
                    thinking: Some(ThinkingConfig::Effort(Effort::High)),
                    workflow: Some(true),
                    ..Default::default()
                }))),
        )
        .unwrap();
        let rules = vec![StoredRule {
            tool: "bash".into(),
            scope: None,
            effect: StoredEffect::Allow,
        }];
        app.permissions
            .restore(PermissionSnapshot::from_stored(&rules, Some(true)).unwrap());
        app.input_box.set_input(TRANSCRIPT.into());
        let before = app.session_projection().state.to_vec();
        app.persist_input();
        assert!(app.refresh_session_projection());
        assert_eq!(app.session_projection().draft(), Some(TRANSCRIPT));
        assert_eq!(app.session_projection().state.to_vec(), before);
        assert_eq!(
            app.root_config().thinking,
            ThinkingConfig::Effort(Effort::High)
        );
        assert!(app.root_config().workflow);
        assert_eq!(app.permissions.snapshot().stored_rules(), rules);
        assert_eq!(app.permissions.snapshot().yolo, Some(true));
    }

    #[test_case::test_case(false; "folded_config")]
    #[test_case::test_case(true; "folded_default_thinking")]
    fn restored_child_config_beats_spawn(default_thinking: bool) {
        let mut app = super::super::tests::test_app();
        let root = app
            .session_projection()
            .root_agent_id()
            .unwrap_or_else(AgentId::generate);
        let child = AgentId::generate();
        let mut session = app.session_fixture();
        session.bind_root_id(root).unwrap();
        let mut record = session.agents()[&root].clone();
        record.id = child;
        record.parent = Some(root);
        record.depth = 1;
        record.transcript_key = TRANSCRIPT.into();
        record.spec = Some(StoredAgentSpec {
            model: ModelSelection {
                spec: Some(SPAWN_MODEL.into()),
                ..Default::default()
            },
            system: String::new(),
            tools: Vec::new(),
            audience: "all".into(),
            mcp: false,
            output_schema: None,
            name: None,
            silent: false,
            auto_deliver: false,
            inherit_provider: false,
            mode_ceiling: StoredModeCeiling {
                restrict_write_to: None,
                tools: None,
            },
            plugin: None,
        });
        let seq = session.agent_revisions().last().unwrap().0 + 1;
        record.created_seq = seq;
        record.updated_seq = seq;
        session.push_agent_revision(seq, record);
        let mut config = stored_config();
        if default_thinking {
            config.thinking = None;
        }
        session.push_state(StoredStateRecord {
            seq: seq + 1,
            agent: Some(child),
            generation: 0,
            change: StateChange::Config(config.clone()),
        });
        app.set_session_fixture(&session);
        app.restore_display();
        let chat = &app.chats[1];
        assert_eq!(chat.agent_id, Some(child));
        assert_eq!(chat.model_id.as_deref(), Some(CONFIG_MODEL));
        assert_eq!(
            chat.opts,
            Some(RequestOptions {
                thinking: if default_thinking {
                    ThinkingConfig::Off
                } else {
                    ThinkingConfig::Effort(Effort::High)
                },
                fast: true,
            })
        );
    }

    #[test]
    fn content_check_ignores_blank_root_but_keeps_folded_mode_and_children() {
        let mut session = crate::AppSession::new(CONFIG_MODEL, "/tmp");
        session.meta.mode = None;
        assert!(!session_has_content(
            &SessionJournal::from_session(&session).unwrap().projection()
        ));
        let root = AgentId::generate();
        session.bind_root_id(root).unwrap();
        let mut config = stored_config();
        session.push_state(StoredStateRecord {
            seq: 1,
            agent: Some(root),
            generation: 0,
            change: StateChange::Config(config.clone()),
        });
        assert!(!session_has_content(
            &SessionJournal::from_session(&session).unwrap().projection()
        ));
        config.mode.id = "plan".into();
        session.push_state(StoredStateRecord {
            seq: 2,
            agent: Some(root),
            generation: 0,
            change: StateChange::Config(config.clone()),
        });
        assert!(session_has_content(
            &SessionJournal::from_session(&session).unwrap().projection()
        ));
        config.mode.id = "build".into();
        session.push_state(StoredStateRecord {
            seq: 3,
            agent: Some(root),
            generation: 0,
            change: StateChange::Config(config),
        });
        session.meta.mode = Some(super::StoredMode::Plan);
        assert!(!session_has_content(
            &SessionJournal::from_session(&session).unwrap().projection()
        ));
        let seq = session.next_state_seq();
        let mut child = session.agents()[&root].clone();
        child.id = AgentId::generate();
        child.parent = Some(root);
        child.depth = 1;
        child.transcript_key = TRANSCRIPT.into();
        child.created_seq = seq;
        child.updated_seq = seq;
        session.push_agent_revision(seq, child);
        assert!(session_has_content(
            &SessionJournal::from_session(&session).unwrap().projection()
        ));
    }

    #[test]
    fn reactivated_agent_reuses_restored_transcript_tab() {
        const TRANSCRIPT: &str = "reactivated-child";
        let mut app = super::super::tests::test_app();
        let root = app
            .session_projection()
            .root_agent_id()
            .unwrap_or_else(AgentId::generate);
        let child = AgentId::generate();
        let mut session = app.session_fixture();
        session.bind_root_id(root).unwrap();
        let mut record = session.agents()[&root].clone();
        record.id = child;
        record.parent = Some(root);
        record.depth = 1;
        record.transcript_key = TRANSCRIPT.into();
        record.label = Some(TRANSCRIPT.into());
        let seq = session.agent_revisions().last().unwrap().0 + 1;
        record.created_seq = seq;
        record.updated_seq = seq;
        session.push_agent_revision(seq, record);
        app.set_session_fixture(&session);
        app.restore_display();
        let index = app.agent_chat_index[&child];
        let chat_count = app.chats.len();
        let info = SubagentInfo {
            agent_id: child,
            parent_agent_id: Some(root),
            parent_tool_use_id: TRANSCRIPT.into(),
            name: TRANSCRIPT.into(),
            prompt: None,
            model: None,
            opts: None,
            answer_tx: None,
            cancel: None,
        };
        assert_eq!(app.resolve_or_create_chat(&info), index);
        assert_eq!(app.chats.len(), chat_count);
        assert_eq!(app.agent_chat_index[&child], index);
        assert!(!app.chats[index].is_finished());
    }

    #[test_case::test_case(StoredAgentLifecycle::Open, DORMANT_AGENT_TEXT; "dormant")]
    #[test_case::test_case(StoredAgentLifecycle::Closed, CLOSED_AGENT_TEXT; "closed")]
    #[test_case::test_case(StoredAgentLifecycle::Retired, RETIRED_AGENT_TEXT; "retired")]
    fn restored_graph_tabs_use_durable_identity_and_parent(
        lifecycle: StoredAgentLifecycle,
        expected: &str,
    ) {
        const TRANSCRIPT: &str = "restored-child";
        const CHILD_TEXT: &str = "stored child history";
        let mut app = super::super::tests::test_app();
        let root = app
            .session_projection()
            .root_agent_id()
            .unwrap_or_else(AgentId::generate);
        let child = AgentId::generate();
        let mut session = app.session_fixture();
        session.bind_root_id(root).unwrap();
        let mut record = session.agents()[&root].clone();
        record.id = child;
        record.parent = Some(root);
        record.depth = 1;
        record.transcript_key = TRANSCRIPT.into();
        record.lifecycle = lifecycle;
        record.label = Some(TRANSCRIPT.into());
        let seq = session.agent_revisions().last().unwrap().0 + 1;
        record.created_seq = seq;
        record.updated_seq = seq;
        session.push_agent_revision(seq, record);
        session.replace_journal_histories(
            &[(
                child,
                HistorySnapshot::new(vec![Message::user(CHILD_TEXT.into())]),
            )]
            .into(),
        );
        app.set_session_fixture(&session);
        app.restore_display();
        assert_eq!(app.chats[0].agent_id, Some(root));
        let index = app.agent_chat_index[&child];
        assert_eq!(app.chats[index].agent_id, Some(child));
        assert_eq!(app.chats[index].parent_agent_id, Some(root));
        assert_eq!(app.chats[index].last_message_text(), expected);
        let info = SubagentInfo {
            agent_id: child,
            parent_agent_id: Some(root),
            parent_tool_use_id: TRANSCRIPT.into(),
            name: TRANSCRIPT.into(),
            prompt: None,
            model: None,
            opts: None,
            answer_tx: None,
            cancel: None,
        };
        app.display_post(
            Some(&info),
            &Post {
                text: CHILD_TEXT.into(),
                level: Default::default(),
                agent: None,
            },
        );
        assert!(app.chats[index].is_finished());
        assert_eq!(app.chats[index].last_message_text(), CHILD_TEXT);
        assert!(app.session_projection().posts.is_empty());
        assert_eq!(app.agent_chat_index[&root], 0);
        assert!(app.queue.is_empty());
    }
}
