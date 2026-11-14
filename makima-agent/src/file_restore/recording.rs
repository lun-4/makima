use super::secure::AnchoredPath;
use crate::{
    journal::{JournalChange, JournalCommit, JournalIncarnation, SessionJournal},
    tools::ToolContext,
};
use makima_storage::{
    StateDir,
    blobs::{BlobStore, FileState},
    id::{AgentId, MakimaId},
    session_state::StateChange,
    sessions::{SESSIONS_DIR, offload_dir},
};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

pub(crate) const SHELL_KIND: &str = "execute";

pub(crate) struct ToolRecording {
    journal: Arc<SessionJournal>,
    incarnation: JournalIncarnation,
    agent: AgentId,
    blobs: BlobStore,
    session: MakimaId,
}

pub(crate) struct FileCapture {
    path: PathBuf,
    anchor: AnchoredPath,
    before: FileState,
}

impl ToolRecording {
    pub(crate) fn from_context(ctx: &ToolContext) -> Result<Option<Self>, String> {
        let Some(turn) = &ctx.managed_turn else {
            return Ok(None);
        };
        let journal = turn.state_access().journal();
        if !journal.is_persistent() {
            return Ok(None);
        }
        let Some(session) = journal.session_id() else {
            return Ok(None);
        };
        let offload = ctx
            .offload
            .as_ref()
            .ok_or("File recording requires session storage")?;
        let dir = offload.dir();
        let state_root = dir
            .ancestors()
            .nth(3)
            .ok_or("Invalid session storage path")?;
        if offload_dir(&state_root.join(SESSIONS_DIR), session) != dir {
            return Err("File recording storage does not belong to the active session".into());
        }
        let agent = turn.agent_id();
        Ok(Some(Self {
            incarnation: journal.incarnation(agent),
            journal,
            agent,
            blobs: BlobStore::new(StateDir::from_path(state_root.to_path_buf())),
            session,
        }))
    }

    pub(crate) fn capture(&self, path: &Path) -> Result<FileCapture, String> {
        let anchor = AnchoredPath::capture(path).map_err(|error| error.to_string())?;
        let bytes = anchor.read().map_err(|error| error.to_string())?;
        Ok(FileCapture {
            path: path.to_path_buf(),
            anchor,
            before: self.store(bytes.as_deref())?,
        })
    }

    pub(crate) fn finish(&self, capture: FileCapture) -> Result<(), String> {
        let after = capture
            .anchor
            .read_after_mutation()
            .map_err(|error| error.to_string())?;
        let after = self.store(after.as_deref())?;
        if capture.before != after {
            self.record(StateChange::File {
                path: capture.path,
                before: capture.before,
                after,
            })?;
        }
        Ok(())
    }

    pub(crate) fn shell_execution(&self) -> Result<(), String> {
        self.record(StateChange::ToolExecution {
            kind: SHELL_KIND.into(),
        })
    }

    fn store(&self, bytes: Option<&[u8]>) -> Result<FileState, String> {
        bytes.map_or(Ok(FileState::Absent), |bytes| {
            self.blobs
                .put(self.session, bytes)
                .map(FileState::Blob)
                .map_err(|error| error.to_string())
        })
    }

    fn record(&self, change: StateChange) -> Result<(), String> {
        self.journal
            .commit_with_for(self.incarnation.clone(), |snapshot| {
                let generation = snapshot
                    .state
                    .iter()
                    .filter(|record| record.agent == Some(self.agent))
                    .max_by_key(|record| record.seq)
                    .map_or(0, |record| record.generation);
                JournalCommit {
                    changes: vec![JournalChange {
                        agent: Some(self.agent),
                        generation,
                        change,
                    }],
                    ..Default::default()
                }
            })
            .map(|_| ())
            .map_err(|error| error.to_string())
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::{SHELL_KIND, ToolRecording};
    use crate::{
        HistorySnapshot,
        file_restore::StoredSession,
        journal::{JournalChange, JournalCommit, SessionJournal},
    };
    use makima_storage::{
        StateDir,
        blobs::{BlobHash, BlobRef, BlobStore, FileState},
        id::AgentId,
        session_state::{
            SessionStateView, StateChange, StoredAgentConfig, StoredModeSnapshot, StoredStateRecord,
        },
    };
    use std::{fs, os::unix::fs::symlink, path::Path, sync::Arc};
    use tempfile::TempDir;
    use test_case::test_case;

    const MODEL: &str = "recording-test";
    const GENERATION: u64 = 7;
    const HISTORY_EPOCH: u64 = 29;
    const BEFORE: &[u8] = b"original\0\xffbytes";
    const AFTER: &[u8] = b"replacement bytes\n";
    const PARTIAL: &[u8] = b"partial write";
    const TOOL_ERROR: &str = "tool failed after writing";
    const DIRECTORY_CHANGED: &str = "Restore parent directory was replaced";

    fn fixture() -> (TempDir, ToolRecording) {
        let temporary = tempfile::tempdir().unwrap();
        let state = StateDir::from_path(temporary.path().join("state"));
        let mut session = StoredSession::new(MODEL, "/");
        session.save(&state).unwrap();
        let journal = Arc::new(SessionJournal::default());
        journal.bind_session(session.id).unwrap();
        let agent = AgentId::generate();
        let recording = ToolRecording {
            incarnation: journal.incarnation(agent),
            journal,
            agent,
            blobs: BlobStore::new(state),
            session: session.id,
        };
        (temporary, recording)
    }

    fn file_state(recording: &ToolRecording, bytes: Option<&[u8]>) -> FileState {
        bytes.map_or(FileState::Absent, |bytes| {
            FileState::Blob(BlobRef {
                session: recording.session,
                hash: BlobHash::digest(bytes),
            })
        })
    }

    fn assert_blob(recording: &ToolRecording, state: &FileState, bytes: Option<&[u8]>) {
        assert_eq!(*state, file_state(recording, bytes));
        if let FileState::Blob(reference) = state {
            assert_eq!(recording.blobs.read(reference).unwrap(), bytes.unwrap());
        }
    }

    fn assert_file_change(
        recording: &ToolRecording,
        path: &Path,
        before: Option<&[u8]>,
        after: Option<&[u8]>,
    ) {
        let snapshot = recording.journal.snapshot();
        assert_eq!(snapshot.revision, 1);
        assert_eq!(snapshot.seq, 1);
        assert_eq!(
            snapshot.state,
            [StoredStateRecord {
                seq: 1,
                agent: Some(recording.agent),
                generation: 0,
                change: StateChange::File {
                    path: path.to_path_buf(),
                    before: file_state(recording, before),
                    after: file_state(recording, after),
                },
            }]
        );
        let StateChange::File {
            before: saved_before,
            after: saved_after,
            ..
        } = &snapshot.state[0].change
        else {
            unreachable!()
        };
        assert_blob(recording, saved_before, before);
        assert_blob(recording, saved_after, after);
    }

    #[test_case(None, Some(b"".as_slice()); "create_empty_file")]
    #[test_case(None, Some(AFTER); "create_nonempty_file")]
    #[test_case(Some(BEFORE), Some(AFTER); "overwrite")]
    #[test_case(Some(BEFORE), Some(b"".as_slice()); "truncate")]
    #[test_case(Some(BEFORE), None; "delete_nonempty_file")]
    #[test_case(Some(b"".as_slice()), None; "delete_empty_file")]
    fn records_actual_file_transition(before: Option<&[u8]>, after: Option<&[u8]>) {
        let (temporary, recording) = fixture();
        let path = temporary.path().canonicalize().unwrap().join("file");
        if let Some(bytes) = before {
            fs::write(&path, bytes).unwrap();
        }
        let capture = recording.capture(&path).unwrap();
        assert_blob(&recording, &capture.before, before);
        assert!(recording.journal.snapshot().state.is_empty());
        match after {
            Some(bytes) => fs::write(&path, bytes).unwrap(),
            None => fs::remove_file(&path).unwrap(),
        }
        recording.finish(capture).unwrap();
        assert_file_change(&recording, &path, before, after);
        assert_eq!(fs::read(&path).ok().as_deref(), after);
    }

    #[test]
    fn records_partial_write_even_when_tool_returns_error() {
        let (temporary, recording) = fixture();
        let path = temporary.path().canonicalize().unwrap().join("file");
        fs::write(&path, BEFORE).unwrap();
        let capture = recording.capture(&path).unwrap();
        let execute = || {
            fs::write(&path, PARTIAL).unwrap();
            Err::<(), _>(TOOL_ERROR)
        };
        let result = execute();
        recording.finish(capture).unwrap();
        assert_eq!(result, Err(TOOL_ERROR));
        assert_file_change(&recording, &path, Some(BEFORE), Some(PARTIAL));
    }

    #[test]
    fn records_file_under_tool_created_parent_directories() {
        let (temporary, recording) = fixture();
        let path = temporary
            .path()
            .canonicalize()
            .unwrap()
            .join("new/nested/file");
        let capture = recording.capture(&path).unwrap();
        assert_eq!(capture.before, FileState::Absent);
        assert!(!path.parent().unwrap().exists());
        assert!(recording.journal.snapshot().state.is_empty());
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(&path, AFTER).unwrap();
        recording.finish(capture).unwrap();
        assert_file_change(&recording, &path, None, Some(AFTER));
    }

    #[test_case(false; "destination_symlink")]
    #[test_case(true; "parent_symlink")]
    fn rejects_unsafe_precapture_symlink(parent_symlink: bool) {
        let (temporary, recording) = fixture();
        let root = temporary.path().canonicalize().unwrap();
        let outside = root.join("outside");
        fs::create_dir(&outside).unwrap();
        fs::write(outside.join("file"), BEFORE).unwrap();
        let path = if parent_symlink {
            symlink(&outside, root.join("link")).unwrap();
            root.join("link/file")
        } else {
            symlink(outside.join("file"), root.join("link")).unwrap();
            root.join("link")
        };
        let before = recording.journal.snapshot();
        assert!(recording.capture(&path).is_err());
        let after = recording.journal.snapshot();
        assert_eq!(after.state, before.state);
        assert_eq!(after.seq, before.seq);
        assert_eq!(after.revision, before.revision);
        assert_eq!(fs::read(outside.join("file")).unwrap(), BEFORE);
    }

    #[test_case(false; "replacement_directory")]
    #[test_case(true; "replacement_symlink")]
    fn rejects_postcapture_parent_substitution(symlink_replacement: bool) {
        let (temporary, recording) = fixture();
        let root = temporary.path().canonicalize().unwrap();
        let parent = root.join("parent");
        fs::create_dir(&parent).unwrap();
        let path = parent.join("file");
        fs::write(&path, BEFORE).unwrap();
        let capture = recording.capture(&path).unwrap();
        let before = recording.journal.snapshot();
        let moved = root.join("moved");
        fs::rename(&parent, &moved).unwrap();
        let replacement = if symlink_replacement {
            let outside = root.join("outside");
            fs::create_dir(&outside).unwrap();
            symlink(&outside, &parent).unwrap();
            outside
        } else {
            fs::create_dir(&parent).unwrap();
            parent
        };
        fs::write(replacement.join("file"), AFTER).unwrap();
        let error = recording.finish(capture).unwrap_err();
        if !symlink_replacement {
            assert_eq!(error, DIRECTORY_CHANGED);
        }
        let after = recording.journal.snapshot();
        assert_eq!(after.state, before.state);
        assert_eq!(after.seq, before.seq);
        assert_eq!(after.revision, before.revision);
        assert_eq!(fs::read(moved.join("file")).unwrap(), BEFORE);
        assert_eq!(fs::read(replacement.join("file")).unwrap(), AFTER);
        assert_blob(
            &recording,
            &file_state(&recording, Some(BEFORE)),
            Some(BEFORE),
        );
    }

    #[test_case(false; "policy_generation_differs_from_history_epoch")]
    #[test_case(true; "configuration_changes_between_capture_and_finish")]
    fn file_record_uses_policy_generation_at_atomic_commit(reconfigure: bool) {
        let (temporary, recording) = fixture();
        let config = StoredAgentConfig {
            cwd: temporary.path().to_str().unwrap().into(),
            model: MODEL.into(),
            thinking: None,
            fast: false,
            workflow: false,
            mode: StoredModeSnapshot {
                id: "build".into(),
                label: "Build".into(),
                system_prompt: None,
                restrict_write_to: None,
                tools: None,
            },
            options: Default::default(),
        };
        let history = HistorySnapshot {
            epoch: HISTORY_EPOCH,
            ..Default::default()
        };
        recording
            .journal
            .commit(JournalCommit {
                histories: [(recording.agent, history)].into(),
                changes: vec![JournalChange {
                    agent: Some(recording.agent),
                    generation: GENERATION,
                    change: StateChange::Config(config.clone()),
                }],
                ..Default::default()
            })
            .unwrap();
        let path = temporary.path().canonicalize().unwrap().join("file");
        let capture = recording.capture(&path).unwrap();
        if reconfigure {
            recording
                .journal
                .commit(JournalCommit {
                    changes: vec![JournalChange {
                        agent: Some(recording.agent),
                        generation: GENERATION + 1,
                        change: StateChange::Config(StoredAgentConfig {
                            fast: true,
                            ..config
                        }),
                    }],
                    ..Default::default()
                })
                .unwrap();
        }
        fs::write(&path, AFTER).unwrap();
        recording.finish(capture).unwrap();
        let snapshot = recording.journal.snapshot();
        let record = snapshot.state.last().unwrap();
        assert!(matches!(&record.change, StateChange::File { .. }));
        assert_eq!(record.generation, GENERATION + u64::from(reconfigure));
        assert_eq!(snapshot.histories[&recording.agent].epoch, HISTORY_EPOCH);
        let view = SessionStateView::fold(&snapshot.state, &snapshot.agent_revisions, snapshot.seq)
            .unwrap();
        assert_eq!(
            view.scopes[&Some(recording.agent)].generation,
            record.generation
        );
    }

    #[test_case(0; "initial_generation")]
    #[test_case(GENERATION; "later_generation")]
    fn shell_execution_records_each_marker_with_agent_and_generation(generation: u64) {
        let (_temporary, recording) = fixture();
        recording
            .journal
            .commit(JournalCommit {
                changes: vec![JournalChange {
                    agent: Some(recording.agent),
                    generation,
                    change: StateChange::ToolExecution {
                        kind: SHELL_KIND.into(),
                    },
                }],
                ..Default::default()
            })
            .unwrap();
        recording.shell_execution().unwrap();
        recording.shell_execution().unwrap();
        let snapshot = recording.journal.snapshot();
        assert_eq!(snapshot.seq, 3);
        assert_eq!(snapshot.revision, 3);
        assert_eq!(
            snapshot.state,
            [1, 2, 3].map(|seq| StoredStateRecord {
                seq,
                agent: Some(recording.agent),
                generation,
                change: StateChange::ToolExecution {
                    kind: SHELL_KIND.into(),
                },
            })
        );
    }
}
