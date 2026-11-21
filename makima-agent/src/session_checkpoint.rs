use crate::{
    actor::EffectiveAgentConfig,
    cancel::CancelToken,
    config_change::ConfigChangeError,
    journal::{JournalIncarnation, JournalPrepared, SessionJournal},
    session_coordinator::{SessionCheckpoint, SessionCheckpointKind},
    session_writer::{SessionWriter, StoredSession, WriterError},
};
use makima_storage::{
    checkpoint::{
        CheckpointAck, CheckpointClaim, CheckpointError, CheckpointFuture, CheckpointPublication,
        CheckpointRequest, CheckpointWriter,
    },
    session_state::StoredAgentConfig,
};
use std::{collections::BTreeMap, sync::Arc};

const CONFIG_MODE_UNRESOLVED: &str = "agent mode definition is not resolved";
const CHECKPOINT_CANCELLED: &str = "checkpoint publication was cancelled";
const SESSION_MISMATCH: &str = "checkpoint belongs to a different session";
const HISTORY_MISSING_REVISION: &str = "history checkpoint is missing its revision";
const HISTORY_MISSING_SNAPSHOT: &str = "history checkpoint is missing its snapshot";

impl From<WriterError> for CheckpointError {
    fn from(error: WriterError) -> Self {
        Self::Save {
            session_id: error.session_id,
            message: error.message,
        }
    }
}

impl SessionWriter {
    pub async fn bind_journal(&self, journal: Arc<SessionJournal>) -> Result<(), CheckpointError> {
        journal
            .bind_session(self.session_id())
            .map_err(|error| CheckpointError::Save {
                session_id: self.session_id(),
                message: error.to_string().into(),
            })?;
        journal
            .bind_writer(self.clone())
            .map_err(|error| CheckpointError::Save {
                session_id: self.session_id(),
                message: error.to_string().into(),
            })?;
        self.attach_journal(&journal);
        self.drain().await
    }

    pub async fn drain(&self) -> Result<(), CheckpointError> {
        self.append(Vec::new()).await?;
        Ok(())
    }

    async fn flush_journals(&self) -> Result<(), CheckpointError> {
        for journal in self.journals() {
            journal
                .flush()
                .await
                .map_err(|message| CheckpointError::Save {
                    session_id: self.session_id(),
                    message: message.into(),
                })?;
        }

        self.drain().await
    }

    pub async fn publish_prepared(
        &self,
        mut prepared: JournalPrepared,
        cancel: CancelToken,
        incarnation: JournalIncarnation,
        journal: Arc<SessionJournal>,
    ) -> Result<(), CheckpointError> {
        let publication = CheckpointPublication::default();
        let _registration = cancel.register_publication(&publication);
        if !cancel.claim_publication_if(&publication, || {
            journal.incarnation(incarnation.agent) == incarnation
        }) {
            return Err(CheckpointError::Save {
                session_id: self.session_id(),
                message: CHECKPOINT_CANCELLED.into(),
            });
        }
        prepared.install().map_err(|error| CheckpointError::Save {
            session_id: self.session_id(),
            message: error.to_string().into(),
        })?;
        journal
            .flush()
            .await
            .map_err(|message| CheckpointError::Save {
                session_id: self.session_id(),
                message: message.into(),
            })?;
        self.drain().await
    }
}

impl CheckpointWriter<SessionCheckpoint> for SessionWriter {
    fn checkpoint(&self, request: CheckpointRequest<SessionCheckpoint>) -> CheckpointFuture {
        let writer = self.clone();
        Box::pin(async move {
            validate_session(&writer, &request)?;
            writer.flush_journals().await?;
            Ok(CheckpointAck::for_request(&request))
        })
    }

    fn checkpoint_guarded(
        &self,
        request: CheckpointRequest<SessionCheckpoint>,
        publication: CheckpointPublication,
        claim: CheckpointClaim,
    ) -> CheckpointFuture {
        let writer = self.clone();
        Box::pin(async move {
            validate_session(&writer, &request)?;

            claim(&publication).map_err(|message| CheckpointError::Save {
                session_id: request.session_id,
                message,
            })?;
            // The claim installs the prepared changes before the flush captures its target.
            writer.flush_journals().await?;
            Ok(CheckpointAck::for_request(&request))
        })
    }
}

fn validate_session(
    writer: &SessionWriter,
    request: &CheckpointRequest<SessionCheckpoint>,
) -> Result<(), CheckpointError> {
    if writer.session_id() != request.session_id {
        return Err(CheckpointError::Save {
            session_id: request.session_id,
            message: SESSION_MISMATCH.into(),
        });
    }
    Ok(())
}

pub fn validate_checkpoint_history(
    checkpoint: &SessionCheckpoint,
    has_history: bool,
) -> Result<(), &'static str> {
    match checkpoint.kind {
        SessionCheckpointKind::Options => Ok(()),
        SessionCheckpointKind::History if !has_history => Err(HISTORY_MISSING_SNAPSHOT),
        _ if has_history && checkpoint.history_revision.is_none() => Err(HISTORY_MISSING_REVISION),
        _ => Ok(()),
    }
}

pub fn stored_agent_config(
    config: &EffectiveAgentConfig,
    options: BTreeMap<String, String>,
) -> Result<StoredAgentConfig, ConfigChangeError> {
    let definition = config
        .mode_def
        .as_ref()
        .ok_or_else(|| ConfigChangeError::Rejected(Arc::from(CONFIG_MODE_UNRESOLVED)))?;
    Ok(StoredAgentConfig {
        cwd: config.context.cwd().to_string_lossy().into_owned(),
        model: config.model.spec(),
        thinking: Some(config.thinking.into()),
        fast: config.fast,
        workflow: config.workflow,
        mode: definition.stored_snapshot(&config.mode),
        options,
    })
}

pub fn apply_session_checkpoint(session: &mut StoredSession, checkpoint: &SessionCheckpoint) {
    if checkpoint.kind != SessionCheckpointKind::Options
        && let Some(history) = &checkpoint.history
    {
        session.adopt_history(history);
        session.update_title_if_default();
    }
}

#[cfg(test)]
mod tests {
    use super::{
        CHECKPOINT_CANCELLED, SessionCheckpoint, SessionCheckpointKind, SessionWriter,
        StoredSession,
    };
    use crate::{
        cancel::CancelToken,
        journal::{JournalChange, JournalCommit, SessionJournal},
        session_options::SessionOptions,
    };
    use makima_storage::{
        checkpoint::{
            CheckpointAck, CheckpointClaim, CheckpointPublication, CheckpointRequest,
            CheckpointVersion, CheckpointWriter,
        },
        id::{AgentId, MakimaId},
        session_state::{StateChange, StoredStateRecord},
    };
    use serde_json::Value;
    use std::{
        collections::BTreeMap,
        fs::read,
        io,
        sync::{Arc, Mutex},
    };

    const MODEL: &str = "checkpoint-test";
    const CWD: &str = "/checkpoint-test";
    const SLOT: &str = "test:checkpoint";

    fn request(session_id: MakimaId) -> CheckpointRequest<SessionCheckpoint> {
        CheckpointRequest {
            session_id,
            version: CheckpointVersion {
                revision: 1,
                epoch: 1,
            },
            snapshot: Arc::new(SessionCheckpoint {
                kind: SessionCheckpointKind::Options,
                history_revision: None,
                history: None,
                options: SessionOptions::new(Vec::new(), &BTreeMap::new())
                    .unwrap()
                    .snapshot(),
                config: None,
                journal: None,
            }),
        }
    }

    #[test_case::test_case(false; "cancelled")]
    #[test_case::test_case(true; "published")]
    fn guarded_receipt_persists_only_installed_journal_changes(publish: bool) {
        smol::block_on(async {
            let directory = tempfile::tempdir().unwrap();
            let session = StoredSession::new(MODEL, CWD);
            let writer = SessionWriter::open_session(directory.path(), &session).unwrap();
            let journal = Arc::new(SessionJournal::from_session(&session).unwrap());
            writer.bind_journal(Arc::clone(&journal)).await.unwrap();
            let prepared = journal
                .prepare(JournalCommit {
                    changes: vec![JournalChange {
                        agent: None,
                        generation: 0,
                        change: StateChange::Plugin {
                            key: SLOT.into(),
                            version: 1,
                            value: None,
                        },
                    }],
                    ..Default::default()
                })
                .await
                .unwrap();
            let prepared = Mutex::new(prepared);
            let claim: CheckpointClaim = Arc::new(move |publication| {
                if !publication.try_publish() {
                    return Err(CHECKPOINT_CANCELLED.into());
                }
                prepared
                    .lock()
                    .unwrap()
                    .install()
                    .map(|_| ())
                    .map_err(|error| error.to_string().into())
            });
            let publication = CheckpointPublication::default();
            if !publish {
                publication.cancel();
            }
            let result = CheckpointWriter::checkpoint_guarded(
                &writer,
                request(session.id),
                publication,
                claim,
            )
            .await;
            assert_eq!(result.is_ok(), publish);
            if publish {
                assert_eq!(
                    result.unwrap(),
                    CheckpointAck::for_request(&request(session.id))
                );
            }
            let bytes = read(directory.path().join(format!("{}.jsonl", session.id))).unwrap();
            let states: Vec<StoredStateRecord> = bytes
                .split(|byte| *byte == b'\n')
                .filter(|line| !line.is_empty())
                .map(|line| serde_json::from_slice::<Value>(line).unwrap())
                .filter(|record| record["t"] == "state")
                .map(|record| serde_json::from_value(record["d"].clone()).unwrap())
                .collect();
            assert_eq!(states.len(), usize::from(publish));
            if publish {
                assert!(
                    matches!(&states[0].change, StateChange::Plugin { key, .. } if key == SLOT)
                );
            }
        });
    }

    #[test]
    fn checkpoint_ignores_retired_journal_pending_changes() {
        smol::block_on(async {
            let directory = tempfile::tempdir().unwrap();
            let session = StoredSession::new(MODEL, CWD);
            let writer = SessionWriter::open_session(directory.path(), &session).unwrap();
            let retired = Arc::new(SessionJournal::from_session(&session).unwrap());
            writer.bind_journal(Arc::clone(&retired)).await.unwrap();
            let pending = retired.stage(JournalCommit::default(), 1).unwrap();
            let current = Arc::new(SessionJournal::from_session(&session).unwrap());
            writer.bind_journal(Arc::clone(&current)).await.unwrap();
            assert_eq!(writer.journals().len(), 1);
            assert!(Arc::ptr_eq(&writer.journals()[0], &current));
            CheckpointWriter::checkpoint(&writer, request(session.id))
                .await
                .unwrap();
            retired
                .complete(
                    pending,
                    vec![JournalChange {
                        agent: None,
                        generation: 0,
                        change: StateChange::Plugin {
                            key: SLOT.into(),
                            version: 1,
                            value: None,
                        },
                    }],
                )
                .unwrap();
        });
    }

    #[test_case::test_case(false; "ordinary")]
    #[test_case::test_case(true; "guarded")]
    fn checkpoint_waits_for_pending_journal_records_before_acknowledgment(guarded: bool) {
        smol::block_on(async {
            let directory = tempfile::tempdir().unwrap();
            let session = StoredSession::new(MODEL, CWD);
            let writer = SessionWriter::open_session(directory.path(), &session).unwrap();
            let journal = Arc::new(SessionJournal::from_session(&session).unwrap());
            writer.bind_journal(Arc::clone(&journal)).await.unwrap();
            let pending = journal.stage(JournalCommit::default(), 1).unwrap();
            let checkpoint = if guarded {
                CheckpointWriter::checkpoint_guarded(
                    &writer,
                    request(session.id),
                    CheckpointPublication::default(),
                    Arc::new(|publication| {
                        publication
                            .try_publish()
                            .then_some(())
                            .ok_or_else(|| CHECKPOINT_CANCELLED.into())
                    }),
                )
            } else {
                CheckpointWriter::checkpoint(&writer, request(session.id))
            };
            futures_lite::pin!(checkpoint);
            assert!(
                futures_lite::future::poll_once(&mut checkpoint)
                    .await
                    .is_none()
            );
            journal
                .complete(
                    pending,
                    vec![JournalChange {
                        agent: None,
                        generation: 0,
                        change: StateChange::Plugin {
                            key: SLOT.into(),
                            version: 1,
                            value: None,
                        },
                    }],
                )
                .unwrap();
            checkpoint.await.unwrap();
            let bytes = read(directory.path().join(format!("{}.jsonl", session.id))).unwrap();
            let records: Vec<Value> = bytes
                .split(|byte| *byte == b'\n')
                .filter(|line| !line.is_empty())
                .map(|line| serde_json::from_slice(line).unwrap())
                .collect();
            assert!(
                records
                    .iter()
                    .any(|record| record["t"] == "state" && record["d"]["change"]["key"] == SLOT)
            );
        });
    }

    struct GatedSync {
        entered: flume::Sender<()>,
        release: Option<flume::Receiver<()>>,
    }

    impl crate::session_writer::SessionIo for GatedSync {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            Ok(bytes.len())
        }

        fn sync(&mut self) -> io::Result<()> {
            if let Some(release) = self.release.take() {
                self.entered.send(()).map_err(io::Error::other)?;
                release.recv().map_err(io::Error::other)?;
            }
            Ok(())
        }
    }

    #[test]
    fn prepared_publication_waits_for_fsync_before_success() {
        smol::block_on(async {
            let session = StoredSession::new(MODEL, CWD);
            let (entered, syncing) = flume::bounded(1);
            let (release, gate) = flume::bounded(1);
            let writer = SessionWriter::with_io(
                session.id,
                GatedSync {
                    entered,
                    release: Some(gate),
                },
                0,
            );
            let journal = Arc::new(SessionJournal::from_session(&session).unwrap());
            journal.bind_writer(writer.clone()).unwrap();
            let agent = AgentId::generate();
            let incarnation = journal.incarnation(agent);
            let prepared = journal
                .prepare(JournalCommit {
                    changes: vec![JournalChange {
                        agent: None,
                        generation: 0,
                        change: StateChange::Plugin {
                            key: SLOT.into(),
                            version: 1,
                            value: None,
                        },
                    }],
                    ..Default::default()
                })
                .await
                .unwrap();
            let (_trigger, cancel) = CancelToken::new();
            let (finished, result) = flume::bounded(1);
            let task = smol::spawn(async move {
                let result = writer
                    .publish_prepared(prepared, cancel, incarnation, journal)
                    .await;
                finished.send(result).unwrap();
            });
            syncing.recv_async().await.unwrap();
            assert!(result.try_recv().is_err());
            release.send(()).unwrap();
            result.recv_async().await.unwrap().unwrap();
            task.await;
        });
    }

    struct FailingIo;

    impl crate::session_writer::SessionIo for FailingIo {
        fn write(&mut self, _: &[u8]) -> io::Result<usize> {
            Err(io::Error::from(io::ErrorKind::StorageFull))
        }

        fn sync(&mut self) -> io::Result<()> {
            Err(io::Error::from(io::ErrorKind::StorageFull))
        }
    }

    #[test]
    fn checkpoint_receipt_reports_writer_failure() {
        smol::block_on(async {
            let session = StoredSession::new(MODEL, CWD);
            let writer = SessionWriter::with_io(session.id, FailingIo, 0);
            let journal = Arc::new(SessionJournal::from_session(&session).unwrap());
            journal.bind_writer(writer.clone()).unwrap();
            journal
                .commit(JournalCommit {
                    changes: vec![JournalChange {
                        agent: None,
                        generation: 0,
                        change: StateChange::Plugin {
                            key: SLOT.into(),
                            version: 1,
                            value: None,
                        },
                    }],
                    ..Default::default()
                })
                .unwrap();
            assert!(
                CheckpointWriter::checkpoint(&writer, request(session.id))
                    .await
                    .is_err()
            );
            assert!(writer.status().unsaved_records > 0);
            writer.close().await.unwrap();
        });
    }
}
