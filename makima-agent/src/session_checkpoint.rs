use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use futures_lite::future;
use makima_providers::{Message, TokenUsage};
use makima_storage::checkpoint::{
    CheckpointAck, CheckpointClaim, CheckpointError, CheckpointFuture, CheckpointPublication,
    CheckpointRequest, CheckpointWriter,
};
use makima_storage::id::{AgentId, MakimaId};
use makima_storage::session_state::{ParentLink, StoredAgentConfig};
use makima_storage::sessions::{
    HistorySnapshot, SESSIONS_DIR, Session, SessionError, SessionLog, StoredSubagent,
};
use makima_storage::{StateDir, StorageError, session_lock::SessionPublicationGuard};
use tracing::warn;

use crate::ToolOutput;
use crate::actor::EffectiveAgentConfig;
use crate::cancel::{CancelToken, PublicationRegistration};
use crate::config_change::ConfigChangeError;
use crate::journal::{JournalIncarnation, JournalPrepared, SessionJournal, SessionJournalSnapshot};
use crate::session_coordinator::{SessionCheckpoint, SessionCheckpointKind, publication_claim};
use crate::session_recovery::{RecoveredSession, RecoveryCandidate};

type StoredSession = Session<Message, TokenUsage, ToolOutput>;

const JOURNAL_POLL_INTERVAL: Duration = Duration::from_secs(1);
const CONFIG_MODE_UNRESOLVED: &str = "configuration requires a resolved mode definition";
const CHECKPOINT_CANCELLED: &str = "checkpoint publication cancelled";
const HISTORY_SUPERSEDED: &str = "history checkpoint superseded by a newer revision";
const HISTORY_NOT_READY: &str = "requested history is not covered by the ready journal checkpoint";
const HISTORY_MISSING_REVISION: &str = "history checkpoint has no history revision";
const HISTORY_MISSING_SNAPSHOT: &str = "history checkpoint has no exact history snapshot";
const JOURNAL_PREFIX_MISMATCH: &str = "session journal does not match the checkpoint prefix";
const JOURNAL_ALREADY_BOUND: &str = "session checkpoint already has a journal";
const JOURNAL_SUPERSEDED: &str = "journal checkpoint superseded by a newer published revision";

#[derive(Clone)]
struct RetainedSession {
    session: StoredSession,
    history_version: Option<(u64, u64)>,
}

impl RetainedSession {
    fn validate_history(
        &self,
        checkpoint: &SessionCheckpoint,
    ) -> Result<Option<(u64, u64)>, &'static str> {
        validate_checkpoint_history(checkpoint, checkpoint.history.is_some())?;
        let (Some(history), Some(revision), false) = (
            &checkpoint.history,
            checkpoint.history_revision,
            checkpoint.kind == SessionCheckpointKind::Options,
        ) else {
            return Ok(self.history_version);
        };
        let incoming = (history.epoch, revision);
        if self
            .history_version
            .is_some_and(|current| incoming < current)
        {
            return Err(HISTORY_SUPERSEDED);
        }
        Ok(Some(incoming))
    }
}

type SaveJob =
    Box<dyn FnOnce(&StateDir, &Mutex<RetainedSession>, Option<&SessionPublicationGuard>) + Send>;
type SaveFuture<R> = Pin<Box<dyn Future<Output = Result<R, CheckpointError>> + Send>>;

#[derive(Clone)]
pub struct SessionLogCheckpoint {
    session_id: MakimaId,
    save_tx: flume::Sender<SaveJob>,
    journal: Arc<Mutex<Option<Arc<SessionJournal>>>>,
    #[cfg(test)]
    session: Arc<Mutex<RetainedSession>>,
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

impl SessionLogCheckpoint {
    pub fn open(
        dir: StateDir,
        session_id: MakimaId,
        model: &str,
        cwd: &str,
        publication_guard: Option<SessionPublicationGuard>,
    ) -> Result<Self, CheckpointError> {
        Self::open_bound(dir, session_id, model, cwd, publication_guard, None)
    }

    pub fn open_with_root(
        dir: StateDir,
        session_id: MakimaId,
        model: &str,
        cwd: &str,
        publication_guard: Option<SessionPublicationGuard>,
        root: AgentId,
        parent: Option<ParentLink>,
    ) -> Result<Self, CheckpointError> {
        Self::open_bound(
            dir,
            session_id,
            model,
            cwd,
            publication_guard,
            Some((root, parent)),
        )
    }

    fn open_bound(
        dir: StateDir,
        session_id: MakimaId,
        model: &str,
        cwd: &str,
        publication_guard: Option<SessionPublicationGuard>,
        root: Option<(AgentId, Option<ParentLink>)>,
    ) -> Result<Self, CheckpointError> {
        let mut session = match StoredSession::load(session_id, &dir) {
            Ok(session) => session,
            Err(SessionError::Storage(StorageError::NotFound(_))) => {
                let mut session = StoredSession::new(model, cwd);
                session.id = session_id;
                session
            }
            Err(error) => {
                return Err(CheckpointError::Save {
                    session_id,
                    message: Arc::from(error.to_string()),
                });
            }
        };
        if let Some((root, parent)) = root {
            bind_root(&mut session, root, parent).map_err(|error| CheckpointError::Save {
                session_id,
                message: Arc::from(error.to_string()),
            })?;
        }
        Self::open_with_session(dir, session, publication_guard)
    }

    pub fn open_with_session(
        dir: StateDir,
        session: StoredSession,
        publication_guard: Option<SessionPublicationGuard>,
    ) -> Result<Self, CheckpointError> {
        let session_id = session.id;
        session
            .latest_state()
            .map_err(|error| CheckpointError::Save {
                session_id,
                message: Arc::from(error.to_string()),
            })?;
        let session = Arc::new(Mutex::new(RetainedSession {
            session,
            history_version: None,
        }));
        let (save_tx, save_rx) = flume::unbounded::<SaveJob>();
        let worker_session = Arc::clone(&session);
        let journal = Arc::new(Mutex::new(None::<Arc<SessionJournal>>));
        let worker_journal = Arc::clone(&journal);
        smol::spawn(async move {
            loop {
                let job = future::race(async { save_rx.recv_async().await.map(Some) }, async {
                    smol::Timer::after(JOURNAL_POLL_INTERVAL).await;
                    Ok(None)
                })
                .await;
                let Ok(job) = job else {
                    break;
                };
                let dir = dir.clone();
                let session = Arc::clone(&worker_session);
                let publication_guard = publication_guard.clone();
                let journal = lock(&worker_journal).clone();
                smol::unblock(move || {
                    if let Some(job) = job {
                        job(&dir, &session, publication_guard.as_ref());
                    } else if let Some(journal) = journal
                        && let Some(Err(error)) = Self::save_pending_journal(
                            &dir,
                            &session,
                            publication_guard.as_ref(),
                            &journal,
                        )
                    {
                        warn!(%error, %session_id, "session state applied but not saved");
                    }
                })
                .await;
            }
        })
        .detach();
        Ok(Self {
            session_id,
            save_tx,
            journal,
            #[cfg(test)]
            session,
        })
    }

    fn save_pending_journal(
        dir: &StateDir,
        session: &Mutex<RetainedSession>,
        guard: Option<&SessionPublicationGuard>,
        journal: &SessionJournal,
    ) -> Option<Result<(), CheckpointError>> {
        let _publication = journal.try_publication_read()?;
        Some(Self::save_journal(dir, session, guard, journal))
    }

    fn save_journal(
        dir: &StateDir,
        session: &Mutex<RetainedSession>,
        guard: Option<&SessionPublicationGuard>,
        journal: &SessionJournal,
    ) -> Result<(), CheckpointError> {
        let result = Self::save_journal_snapshot(dir, session, guard, journal);
        journal.set_checkpoint_error(result.as_ref().err().map(ToString::to_string));
        result
    }

    fn save_journal_snapshot(
        dir: &StateDir,
        session: &Mutex<RetainedSession>,
        guard: Option<&SessionPublicationGuard>,
        journal: &SessionJournal,
    ) -> Result<(), CheckpointError> {
        let failure = |message: String| CheckpointError::Save {
            session_id: lock(session).session.id,
            message: message.into(),
        };
        let snapshot = journal.checkpoint_snapshot().map_err(failure)?;
        if snapshot.revision <= journal.published_revision() {
            return Ok(());
        }
        let mut candidate = lock(session).session.clone();
        snapshot
            .apply_to(&mut candidate)
            .map_err(|error| failure(error.to_string()))?;
        Self::publish_candidate(
            dir,
            &candidate,
            guard,
            &CheckpointPublication::default(),
            &publication_claim(),
        )?;
        lock(session).session = candidate;
        journal
            .acknowledge(&snapshot)
            .map_err(|error| failure(error.to_string()))
    }

    /// Runs `job` on the blocking pool after every earlier job has finished.
    fn submit<R: Send + 'static>(
        &self,
        job: impl FnOnce(
            &StateDir,
            &Mutex<RetainedSession>,
            Option<&SessionPublicationGuard>,
        ) -> Result<R, CheckpointError>
        + Send
        + 'static,
    ) -> SaveFuture<R> {
        let session_id = self.session_id;
        let (reply, response) = flume::bounded(1);
        let sent = self.save_tx.send(Box::new(move |dir, session, guard| {
            let _ = reply.send(job(dir, session, guard));
        }));
        Box::pin(async move {
            sent.map_err(|_| CheckpointError::Closed(session_id))?;
            response
                .recv_async()
                .await
                .unwrap_or(Err(CheckpointError::Closed(session_id)))
        })
    }

    fn submit_checkpoint<R: Send + 'static>(
        &self,
        job: impl FnOnce(
            &StateDir,
            &Mutex<RetainedSession>,
            Option<&SessionPublicationGuard>,
            Option<&SessionJournalSnapshot>,
        ) -> Result<R, CheckpointError>
        + Send
        + 'static,
    ) -> SaveFuture<R> {
        self.submit_checkpoint_snapshot(None, job)
    }

    fn submit_checkpoint_snapshot<R: Send + 'static>(
        &self,
        prepared: Option<SessionJournalSnapshot>,
        job: impl FnOnce(
            &StateDir,
            &Mutex<RetainedSession>,
            Option<&SessionPublicationGuard>,
            Option<&SessionJournalSnapshot>,
        ) -> Result<R, CheckpointError>
        + Send
        + 'static,
    ) -> SaveFuture<R> {
        let writer = self.clone();
        let journal = lock(&self.journal).clone();
        let session_id = self.session_id;
        let acknowledge = prepared.is_none();
        let eager = if acknowledge {
            journal.as_ref().and_then(|journal| {
                journal
                    .try_publication_read()
                    .map(|publication| (publication, journal.checkpoint_snapshot()))
            })
        } else {
            None
        };
        Box::pin(async move {
            let (publication, snapshot) = if let Some(prepared) = prepared {
                (None, Some(prepared))
            } else if let Some((publication, snapshot)) = eager {
                let snapshot = snapshot.map_err(|message| CheckpointError::Save {
                    session_id,
                    message: Arc::from(message),
                })?;
                (Some(publication), Some(snapshot))
            } else if let Some(journal) = &journal {
                let publication = journal.publication_read().await;
                let snapshot =
                    journal
                        .checkpoint_snapshot()
                        .map_err(|message| CheckpointError::Save {
                            session_id,
                            message: Arc::from(message),
                        })?;
                (Some(publication), Some(snapshot))
            } else {
                (None, None)
            };
            writer
                .submit(move |dir, session, guard| {
                    let _publication = publication;
                    let Some(mut snapshot) = snapshot else {
                        return job(dir, session, guard, None);
                    };
                    if acknowledge
                        && let Some(journal) = &journal
                        && snapshot.revision < journal.published_revision()
                    {
                        let current = journal.checkpoint_snapshot().map_err(|message| {
                            CheckpointError::Save {
                                session_id,
                                message: message.into(),
                            }
                        })?;
                        if current.epoch != snapshot.epoch {
                            return Err(CheckpointError::Save {
                                session_id,
                                message: Arc::from(JOURNAL_SUPERSEDED),
                            });
                        }
                        snapshot = current;
                    }
                    let mut candidate = lock(session).clone();
                    if !acknowledge {
                        candidate.session.replace_state(snapshot.state.clone());
                        candidate
                            .session
                            .replace_agent_revisions(snapshot.agent_revisions.clone());
                    }
                    snapshot.apply_to(&mut candidate.session).map_err(|error| {
                        CheckpointError::Save {
                            session_id,
                            message: Arc::from(error.to_string()),
                        }
                    })?;
                    let candidate = Mutex::new(candidate);
                    let result = job(dir, &candidate, guard, Some(&snapshot))?;
                    *lock(session) = lock(&candidate).clone();
                    if acknowledge && let Some(journal) = journal {
                        journal
                            .acknowledge(&snapshot)
                            .map_err(|error| CheckpointError::Save {
                                session_id,
                                message: Arc::from(error.to_string()),
                            })?;
                    }
                    Ok(result)
                })
                .await
        })
    }

    pub async fn recover(
        &self,
        publication: CheckpointPublication,
        claim: CheckpointClaim,
    ) -> Result<RecoveredSession, CheckpointError> {
        let binding = Arc::clone(&self.journal);
        let session_id = self.session_id;
        self.submit(move |dir, retained, guard| {
            let mut binding = lock(&binding);
            if binding.is_some() {
                return Err(CheckpointError::Save {
                    session_id,
                    message: Arc::from(JOURNAL_ALREADY_BOUND),
                });
            }
            let candidate = RecoveryCandidate::prepare(lock(retained).session.clone())?;
            Self::publish_candidate(dir, candidate.session(), guard, &publication, &claim)?;
            let recovered = candidate.finalize_after_publication();
            *lock(retained) = RetainedSession {
                session: recovered.session().clone(),
                history_version: None,
            };
            *binding = Some(recovered.journal());
            Ok(recovered)
        })
        .await
    }

    pub async fn create_journal(&self) -> Result<Arc<SessionJournal>, CheckpointError> {
        let binding = Arc::clone(&self.journal);
        let session_id = self.session_id;
        self.submit(move |_, session, _| {
            let mut binding = lock(&binding);
            if let Some(journal) = binding.as_ref() {
                journal
                    .bind_session(session_id)
                    .map_err(|error| CheckpointError::Save {
                        session_id,
                        message: Arc::from(error.to_string()),
                    })?;
                return Ok(Arc::clone(journal));
            }
            let journal = Arc::new(
                SessionJournal::from_session(&lock(session).session).map_err(|error| {
                    CheckpointError::Save {
                        session_id,
                        message: Arc::from(error.to_string()),
                    }
                })?,
            );
            journal
                .bind_session(session_id)
                .map_err(|error| CheckpointError::Save {
                    session_id,
                    message: Arc::from(error.to_string()),
                })?;
            *binding = Some(Arc::clone(&journal));
            Ok(journal)
        })
        .await
    }

    pub async fn bind_journal(&self, journal: Arc<SessionJournal>) -> Result<(), CheckpointError> {
        let binding = Arc::clone(&self.journal);
        let session_id = self.session_id;
        self.submit(move |_, session, _| {
            let mut binding = lock(&binding);
            if let Some(current) = binding.as_ref() {
                return if Arc::ptr_eq(current, &journal) {
                    journal
                        .bind_session(session_id)
                        .map_err(|error| CheckpointError::Save {
                            session_id,
                            message: Arc::from(error.to_string()),
                        })
                } else {
                    Err(CheckpointError::Save {
                        session_id,
                        message: Arc::from(JOURNAL_ALREADY_BOUND),
                    })
                };
            }
            let mut retained = lock(session);
            let snapshot =
                journal
                    .checkpoint_snapshot()
                    .map_err(|message| CheckpointError::Save {
                        session_id,
                        message: Arc::from(message),
                    })?;
            let state = retained.session.state();
            let agents = retained.session.agent_revisions();
            let last_seq = state
                .iter()
                .map(|record| record.seq)
                .chain(agents.iter().map(|(seq, _)| *seq))
                .max();
            if !snapshot.state.starts_with(state)
                || !snapshot.agent_revisions.starts_with(agents)
                || snapshot.state[state.len()..]
                    .iter()
                    .map(|record| record.seq)
                    .chain(
                        snapshot.agent_revisions[agents.len()..]
                            .iter()
                            .map(|(seq, _)| *seq),
                    )
                    .any(|seq| last_seq.is_some_and(|last| seq <= last))
            {
                return Err(CheckpointError::Save {
                    session_id,
                    message: Arc::from(JOURNAL_PREFIX_MISMATCH),
                });
            }
            let mut candidate = retained.session.clone();
            snapshot
                .apply_to(&mut candidate)
                .map_err(|error| CheckpointError::Save {
                    session_id,
                    message: Arc::from(error.to_string()),
                })?;
            journal
                .bind_session(session_id)
                .map_err(|error| CheckpointError::Save {
                    session_id,
                    message: Arc::from(error.to_string()),
                })?;
            retained.session = candidate;
            *binding = Some(journal);
            Ok(())
        })
        .await
    }

    /// `child.tool_use_id` is the canonical history key supplied by the host.
    /// Returns after publication is attempted, without waiting on the child runner.
    pub async fn checkpoint_child(
        &self,
        child: StoredSubagent,
        history: HistorySnapshot<Message>,
    ) -> Result<(), CheckpointError> {
        let session_id = self.session_id;
        self.submit_checkpoint(move |dir, session, guard, journal_snapshot| {
            Self::save_child(
                dir,
                session,
                session_id,
                child,
                &history,
                guard,
                journal_snapshot,
            )
        })
        .await
    }

    /// Publishes metadata for the host-authorized canonical child key.
    pub async fn checkpoint_child_config(
        &self,
        child: StoredSubagent,
        snapshot: Option<SessionJournalSnapshot>,
        publication: CheckpointPublication,
        claim: CheckpointClaim,
    ) -> Result<(), CheckpointError> {
        self.submit_checkpoint_snapshot(snapshot, move |dir, session, guard, _| {
            Self::save_child_config(dir, session, child, guard, &publication, &claim)
        })
        .await
    }

    pub async fn publish_prepared(
        &self,
        prepared: JournalPrepared,
        cancel: CancelToken,
        incarnation: JournalIncarnation,
        journal: Arc<SessionJournal>,
    ) -> Result<(), CheckpointError> {
        let publication = CheckpointPublication::default();
        let registration = cancel.register_publication(&publication);
        let claim: CheckpointClaim = Arc::new(move |publication| {
            cancel
                .claim_publication_if(publication, || {
                    journal.incarnation(incarnation.agent) == incarnation
                })
                .then_some(())
                .ok_or_else(|| Arc::from(CHECKPOINT_CANCELLED))
        });
        self.publish_prepared_with_claim(prepared, publication, registration, claim)
            .await
    }

    async fn publish_prepared_with_claim(
        &self,
        prepared: JournalPrepared,
        publication: CheckpointPublication,
        registration: PublicationRegistration,
        claim: CheckpointClaim,
    ) -> Result<(), CheckpointError> {
        self.submit_checkpoint_snapshot(
            Some(prepared.snapshot().clone()),
            move |dir, retained, guard, _| {
                let _registration = registration;
                let candidate = lock(retained).session.clone();
                Self::publish_candidate(dir, &candidate, guard, &publication, &claim)?;
                prepared.commit().map_err(|error| CheckpointError::Save {
                    session_id: candidate.id,
                    message: Arc::from(error.to_string()),
                })?;
                Ok(())
            },
        )
        .await
    }

    pub async fn bind_root_id(
        &self,
        id: AgentId,
        parent: Option<ParentLink>,
    ) -> Result<(), CheckpointError> {
        let session_id = self.session_id;
        self.submit(move |_, session, _| {
            bind_root(&mut lock(session).session, id, parent).map_err(|error| {
                CheckpointError::Save {
                    session_id,
                    message: Arc::from(error.to_string()),
                }
            })
        })
        .await
    }

    pub async fn resolve_mode(&self, config: &EffectiveAgentConfig) -> Result<(), CheckpointError> {
        let config = config.clone();
        let session_id = self.session_id;
        self.submit(move |_, session, _| {
            let mut retained = lock(session);
            let options = retained.session.meta.session_options.clone();
            let config =
                stored_agent_config(&config, options).map_err(|error| CheckpointError::Save {
                    session_id,
                    message: Arc::from(error.to_string()),
                })?;
            retained
                .session
                .resolve_legacy_config(config)
                .map_err(|error| CheckpointError::Save {
                    session_id,
                    message: Arc::from(error.to_string()),
                })?;
            Ok(())
        })
        .await
    }

    pub async fn drain(&self) -> Result<(), CheckpointError> {
        let journal = lock(&self.journal).clone();
        let publication = if let Some(journal) = &journal {
            Some(journal.publication_read().await)
        } else {
            None
        };
        self.submit(move |dir, session, guard| {
            let _publication = publication;
            if let Some(journal) = journal {
                Self::save_journal(dir, session, guard, &journal)
            } else {
                Ok(())
            }
        })
        .await
    }

    fn save(
        dir: &StateDir,
        session: &Mutex<RetainedSession>,
        request: CheckpointRequest<SessionCheckpoint>,
        publication_guard: Option<&SessionPublicationGuard>,
        journal_snapshot: Option<&SessionJournalSnapshot>,
    ) -> Result<CheckpointAck, CheckpointError> {
        let (mut candidate, history_version) = {
            let retained = lock(session);
            let revision = retained
                .validate_history(&request.snapshot)
                .map_err(|message| CheckpointError::Save {
                    session_id: request.session_id,
                    message: Arc::from(message),
                })?;
            if let Some(snapshot) = journal_snapshot
                && request.snapshot.kind != SessionCheckpointKind::Options
                && let Some(history) = &request.snapshot.history
                && (!journal_covers_history(snapshot, &retained.session, "", history)
                    || !history_matches(&retained.session.history_snapshot(), history))
            {
                return Err(CheckpointError::Save {
                    session_id: request.session_id,
                    message: Arc::from(HISTORY_NOT_READY),
                });
            }
            (retained.session.clone(), revision)
        };
        apply_session_checkpoint_with_history(
            &mut candidate,
            &request.snapshot,
            journal_snapshot.is_none(),
        )
        .map_err(|error| CheckpointError::Save {
            session_id: request.session_id,
            message: Arc::from(error.to_string()),
        })?;
        let saved = match publication_guard {
            Some(guard) => guard
                .publish(|| candidate.save(dir))
                .map_err(|error| CheckpointError::Save {
                    session_id: request.session_id,
                    message: Arc::from(error.to_string()),
                })?
                .ok_or(CheckpointError::Closed(request.session_id))?,
            None => candidate.save(dir),
        };
        if let Err(error) = saved {
            return Err(CheckpointError::Save {
                session_id: request.session_id,
                message: Arc::from(error.to_string()),
            });
        }
        *lock(session) = RetainedSession {
            session: candidate,
            history_version,
        };
        Ok(CheckpointAck::for_request(&request))
    }

    fn save_child(
        dir: &StateDir,
        session: &Mutex<RetainedSession>,
        session_id: MakimaId,
        mut child: StoredSubagent,
        history: &HistorySnapshot<Message>,
        guard: Option<&SessionPublicationGuard>,
        journal_snapshot: Option<&SessionJournalSnapshot>,
    ) -> Result<(), CheckpointError> {
        let failure = |message| CheckpointError::Save {
            session_id,
            message,
        };
        let mut candidate = lock(session).session.clone();
        let key = child.tool_use_id.clone();
        mark_duplicate_agent_ids(&mut candidate, &mut child);
        mark_unidentified_history(&candidate, &mut child);
        let selected = candidate.subagent_history_snapshot(&key);
        if selected.as_ref().is_some_and(|selected| {
            selected.epoch > history.epoch
                || (selected.epoch == history.epoch
                    && selected.messages.len() > history.messages.len())
        }) {
            return Err(failure(Arc::from(HISTORY_SUPERSEDED)));
        }
        if let Some(snapshot) = journal_snapshot {
            if !journal_covers_history(snapshot, &candidate, &key, history)
                || !selected
                    .as_ref()
                    .is_some_and(|selected| child_history_matches(selected, history))
            {
                return Err(failure(Arc::from(HISTORY_NOT_READY)));
            }
        } else if !candidate.adopt_subagent_history(key.clone(), history) {
            return Err(failure(Arc::from(HISTORY_SUPERSEDED)));
        }
        if let Some(history) = candidate.subagent_history_snapshot(&key) {
            candidate.prune_posts(Some(&key), history.messages.len());
        }
        if let Some(current) = candidate
            .subagents()
            .iter()
            .find(|current| current.tool_use_id == key)
        {
            let mut metadata = current.clone();
            merge_child_identity(&mut metadata, &child);
            upsert_child(&mut candidate, metadata);
        } else {
            upsert_child(&mut candidate, child);
        }
        let directory = dir
            .ensure_subdir(SESSIONS_DIR)
            .map_err(|error| failure(Arc::from(error.to_string())))?;
        let staged = SessionLog::stage_checkpoint(&directory, &candidate)
            .map_err(|error| failure(Arc::from(error.to_string())))?;
        let published = match guard {
            Some(guard) => staged.publish(guard, || true),
            None => staged.publish_unowned(|| true),
        }
        .map_err(|error| failure(Arc::from(error.to_string())))?;
        if !published {
            return Err(CheckpointError::Closed(session_id));
        }
        lock(session).session = candidate;
        Ok(())
    }

    fn save_child_config(
        dir: &StateDir,
        session: &Mutex<RetainedSession>,
        mut child: StoredSubagent,
        guard: Option<&SessionPublicationGuard>,
        publication: &CheckpointPublication,
        claim: &CheckpointClaim,
    ) -> Result<(), CheckpointError> {
        let mut candidate = lock(session).session.clone();
        mark_duplicate_agent_ids(&mut candidate, &mut child);
        mark_unidentified_history(&candidate, &mut child);
        upsert_child(&mut candidate, child);
        Self::publish_candidate(dir, &candidate, guard, publication, claim)?;
        lock(session).session = candidate;
        Ok(())
    }

    fn save_guarded(
        dir: &StateDir,
        session: &Mutex<RetainedSession>,
        request: CheckpointRequest<SessionCheckpoint>,
        guard: Option<&SessionPublicationGuard>,
        publication: &CheckpointPublication,
        claim: &CheckpointClaim,
        journal_snapshot: Option<&SessionJournalSnapshot>,
    ) -> Result<CheckpointAck, CheckpointError> {
        let failure = |message| CheckpointError::Save {
            session_id: request.session_id,
            message,
        };
        if publication.is_cancelled() {
            return Err(failure(Arc::from(CHECKPOINT_CANCELLED)));
        }
        let (mut candidate, history_version) = {
            let retained = lock(session);
            let revision = retained
                .validate_history(&request.snapshot)
                .map_err(|message| failure(Arc::from(message)))?;
            if let Some(snapshot) = journal_snapshot
                && request.snapshot.kind != SessionCheckpointKind::Options
                && let Some(history) = &request.snapshot.history
                && (!journal_covers_history(snapshot, &retained.session, "", history)
                    || !history_matches(&retained.session.history_snapshot(), history))
            {
                return Err(failure(Arc::from(HISTORY_NOT_READY)));
            }
            (retained.session.clone(), revision)
        };
        apply_session_checkpoint_with_history(
            &mut candidate,
            &request.snapshot,
            journal_snapshot.is_none(),
        )
        .map_err(|error| CheckpointError::Save {
            session_id: request.session_id,
            message: Arc::from(error.to_string()),
        })?;
        Self::publish_candidate(dir, &candidate, guard, publication, claim)?;
        *lock(session) = RetainedSession {
            session: candidate,
            history_version,
        };
        Ok(CheckpointAck::for_request(&request))
    }

    fn publish_candidate(
        dir: &StateDir,
        candidate: &StoredSession,
        guard: Option<&SessionPublicationGuard>,
        publication: &CheckpointPublication,
        claim: &CheckpointClaim,
    ) -> Result<(), CheckpointError> {
        let failure = |message| CheckpointError::Save {
            session_id: candidate.id,
            message,
        };
        if publication.is_cancelled() {
            return Err(failure(Arc::from(CHECKPOINT_CANCELLED)));
        }
        let directory = dir
            .ensure_subdir(SESSIONS_DIR)
            .map_err(|error| failure(Arc::from(error.to_string())))?;
        let staged = SessionLog::stage_checkpoint(&directory, candidate)
            .map_err(|error| failure(Arc::from(error.to_string())))?;
        let mut claim_error = None;
        let mut authorized = || match claim(publication) {
            Ok(()) => true,
            Err(error) => {
                publication.cancel();
                claim_error = Some(error);
                false
            }
        };
        let published = match guard {
            Some(guard) => staged.publish(guard, authorized),
            None => staged.publish_unowned(&mut authorized),
        }
        .map_err(|error| failure(Arc::from(error.to_string())))?;
        if !published {
            publication.cancel();
            return Err(failure(
                claim_error.unwrap_or_else(|| Arc::from(CHECKPOINT_CANCELLED)),
            ));
        }
        Ok(())
    }
}

impl CheckpointWriter<SessionCheckpoint> for SessionLogCheckpoint {
    fn checkpoint_guarded(
        &self,
        request: CheckpointRequest<SessionCheckpoint>,
        publication: CheckpointPublication,
        claim: CheckpointClaim,
    ) -> CheckpointFuture {
        self.submit_checkpoint_snapshot(
            request.snapshot.journal.clone(),
            move |dir, session, guard, snapshot| {
                Self::save_guarded(dir, session, request, guard, &publication, &claim, snapshot)
            },
        )
    }

    fn checkpoint(&self, request: CheckpointRequest<SessionCheckpoint>) -> CheckpointFuture {
        self.submit_checkpoint_snapshot(
            request.snapshot.journal.clone(),
            move |dir, session, guard, journal_snapshot| {
                Self::save(dir, session, request, guard, journal_snapshot)
            },
        )
    }
}

fn journal_covers_history(
    snapshot: &SessionJournalSnapshot,
    session: &StoredSession,
    key: &str,
    history: &HistorySnapshot<Message>,
) -> bool {
    session.agents().values().any(|agent| {
        agent.transcript_key == key
            && snapshot
                .histories
                .get(&agent.id)
                .is_some_and(|selected| history_matches(selected, history))
    })
}

fn history_matches(
    selected: &HistorySnapshot<Message>,
    requested: &HistorySnapshot<Message>,
) -> bool {
    selected.epoch == requested.epoch
        && selected.compacted == requested.compacted
        && selected.conversation == requested.conversation
        && (Arc::ptr_eq(&selected.messages, &requested.messages)
            || selected.messages == requested.messages)
}

fn child_history_matches(
    selected: &HistorySnapshot<Message>,
    requested: &HistorySnapshot<Message>,
) -> bool {
    selected.epoch == requested.epoch
        && selected
            .conversation
            .as_ref()
            .map(|conversation| &conversation.id)
            == requested
                .conversation
                .as_ref()
                .map(|conversation| &conversation.id)
        && (Arc::ptr_eq(&selected.messages, &requested.messages)
            || selected.messages == requested.messages)
}

fn bind_root(
    session: &mut StoredSession,
    id: AgentId,
    parent: Option<ParentLink>,
) -> Result<(), SessionError> {
    session.bind_root_id(id)?;
    if parent.is_some() {
        session.set_parent(parent);
    }
    Ok(())
}

fn mark_unidentified_history(candidate: &StoredSession, child: &mut StoredSubagent) {
    if child.agent_id.is_some()
        && candidate
            .subagent_messages()
            .contains_key(&child.tool_use_id)
        && !candidate
            .subagents()
            .iter()
            .any(|current| current.tool_use_id == child.tool_use_id && current.agent_id.is_some())
        && !child.agent_id.as_ref().is_some_and(|id| {
            id.parse::<AgentId>().ok().is_some_and(|id| {
                candidate.agents().get(&id).is_some_and(|record| {
                    !record.legacy && record.transcript_key == child.tool_use_id
                })
            })
        })
    {
        child.agent_id = None;
        child.ambiguous = true;
    }
}

fn mark_duplicate_agent_ids(candidate: &mut StoredSession, child: &mut StoredSubagent) {
    let Some(agent_id) = &child.agent_id else {
        return;
    };
    let mut children = candidate.subagents().to_vec();
    let mut collision = false;
    for current in &mut children {
        if current.tool_use_id != child.tool_use_id && current.agent_id.as_ref() == Some(agent_id) {
            current.agent_id = None;
            current.ambiguous = true;
            collision = true;
        }
    }
    if collision {
        child.agent_id = None;
        child.ambiguous = true;
        candidate.set_subagents(children);
    }
}

fn merge_child_identity(current: &mut StoredSubagent, incoming: &StoredSubagent) {
    current.ambiguous |= incoming.ambiguous
        || matches!((&current.agent_id, &incoming.agent_id), (Some(a), Some(b)) if a != b);
    if current.ambiguous {
        current.agent_id = None;
    } else if incoming.agent_id.is_some() {
        current.agent_id.clone_from(&incoming.agent_id);
    }
}

fn upsert_child(candidate: &mut StoredSession, mut child: StoredSubagent) {
    let mut children = candidate.subagents().to_vec();
    let key = child.tool_use_id.clone();
    let mut matching = children
        .iter_mut()
        .filter(|current| current.tool_use_id == key);
    if let Some(current) = matching.next() {
        merge_child_identity(current, &child);
        child.agent_id.clone_from(&current.agent_id);
        child.ambiguous = current.ambiguous;
        child.model.clone_from(&current.model);
        child.thinking = current.thinking;
        child.fast = current.fast;
        *current = child;
        for duplicate in matching {
            duplicate.agent_id = None;
            duplicate.ambiguous = true;
            current.agent_id = None;
            current.ambiguous = true;
        }
    } else {
        child.model = None;
        child.thinking = None;
        child.fast = false;
        if child.ambiguous {
            child.agent_id = None;
        }
        children.push(child);
    }
    candidate.set_subagents(children);
}

/// Rejects a checkpoint whose history fields cannot be applied exactly.
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

pub fn apply_session_checkpoint(
    session: &mut StoredSession,
    checkpoint: &SessionCheckpoint,
) -> Result<(), SessionError> {
    apply_session_checkpoint_with_history(session, checkpoint, true)
}

fn apply_session_checkpoint_with_history(
    session: &mut StoredSession,
    checkpoint: &SessionCheckpoint,
    apply_history: bool,
) -> Result<(), SessionError> {
    if apply_history
        && checkpoint.kind != SessionCheckpointKind::Options
        && let Some(history) = &checkpoint.history
    {
        session.adopt_history(history);
        session.update_title_if_default();
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::path::PathBuf;
    use std::task::{Context, Poll, Waker};
    use std::time::Duration;

    use makima_storage::checkpoint::{CheckpointRequest, CheckpointVersion};
    use makima_storage::conversation::ConversationState;
    use makima_storage::id::{MakimaId, TurnId};
    use makima_storage::session_lock;
    use makima_storage::session_state::{MAX_INLINE_TURN_RESULT_BYTES, StateChange};
    use makima_storage::sessions::StoredPost;
    use tempfile::TempDir;

    use super::*;
    use crate::RunSettings;
    use crate::actor::{
        ActorHistory, AgentContextSnapshot, BackendResult, ControlWork, TurnContext,
        TurnProvenance, TurnResult, WorkKind,
    };
    use crate::journal::{JournalChange, JournalCommit};
    use crate::session_coordinator::{CommittedConfigProjection, builtin_option_definitions};
    use crate::session_options::{
        DISABLED_VALUE, ENABLED_VALUE, FAST_OPTION_ID, SessionOptionOwner, SessionOptions,
        WORKFLOW_OPTION_ID,
    };
    use crate::state::{StateScope, StateSlotSpec};
    use crate::tools::NullProvider;
    use crate::{
        ActorBackend, AgentInput, AgentLimits, AgentManagerHandle, AgentMode, DoneReason, ModeDef,
        ModeId, Post, PostLevel, TurnOutcome,
    };
    use makima_providers::{Model, ThinkingConfig};

    const CHILD_PREPARED_TIMEOUT: Duration = Duration::from_secs(10);
    const CHILD_PREPARED_DEADLOCK: &str =
        "prepared child checkpoint waited on its own publication lane";
    const MODEL: &str = "ollama/model";
    const COMMITTED_MODEL: &str = "ollama/committed";
    const CWD: &str = "/project";
    const COMMITTED_CWD: &str = "/committed-project";
    const ORIGINAL: &str = "original";
    const TERMINAL: &str = "terminal";
    const REJECTED: &str = "rejected";
    const CLAIM_ERROR: &str = "checkpoint authority revoked";
    const CATALOG_ID: &str = "test.selection";
    const CATALOG_PLUGIN: &str = "test";
    const CATALOG_VALUE: &str = "updated";
    const STATE_KEY: &str = "fixture/state";
    const SESSIONS_BACKUP: &str = "sessions-backup";
    const CHILD_KEY: &str = "task/child";
    const SIBLING_KEY: &str = "task/sibling";
    const CHILD_NAME: &str = "child";
    const POST_TEXT: &str = "child observation";

    fn child(key: &str) -> StoredSubagent {
        StoredSubagent {
            tool_use_id: key.into(),
            agent_id: None,
            ambiguous: false,
            name: CHILD_NAME.into(),
            model: Some(MODEL.into()),
            thinking: Some(ThinkingConfig::Off.into()),
            fast: false,
        }
    }

    fn stored_value(session: &StoredSession) -> serde_json::Value {
        serde_json::json!({
            "id": session.id,
            "title": session.title,
            "cwd": session.cwd,
            "model": session.model,
            "messages": session.messages(),
            "token_usage": session.token_usage,
            "tool_outputs": session.tool_outputs(),
            "subagent_messages": session.subagent_messages(),
            "subagents": session.subagents(),
            "posts": session.posts(),
            "usage_by_model": session.usage_by_model(),
            "meta": session.meta,
            "created_at": session.created_at,
            "updated_at": session.updated_at,
        })
    }

    #[test_case::test_case(false, false, None; "unguarded")]
    #[test_case::test_case(true, false, None; "guarded")]
    #[test_case::test_case(true, true, None; "revoked_claim")]
    #[test_case::test_case(true, false, Some(false); "changed_history")]
    #[test_case::test_case(true, false, Some(true); "identical_content_new_epoch")]
    fn captured_checkpoint_rebases_after_state_only_poll(
        guarded: bool,
        rejected: bool,
        replaced_history: Option<bool>,
    ) {
        smol::block_on(async {
            let temporary = TempDir::new().unwrap();
            let dir = StateDir::from_path(temporary.path().to_path_buf());
            let mut session = StoredSession::new(MODEL, CWD);
            let root = AgentId::generate();
            session.bind_root_id(root).unwrap();
            let id = session.id;
            let writer =
                SessionLogCheckpoint::open_with_session(dir.clone(), session, None).unwrap();
            let journal = writer.create_journal().await.unwrap();
            let history = HistorySnapshot::new(vec![Message::user(ORIGINAL.into())]);
            journal
                .commit(JournalCommit {
                    histories: [(root, history.clone())].into(),
                    managed_histories: [root].into(),
                    ..Default::default()
                })
                .unwrap();
            let slot = journal
                .register_state::<String>(StateSlotSpec {
                    key: STATE_KEY.into(),
                    scope: StateScope::Agent,
                    version: 1,
                })
                .unwrap();
            let access = journal.state_access(root, None);
            slot.set(&access, ORIGINAL.into(), None).unwrap();
            writer.drain().await.unwrap();
            let captured_revision = journal.snapshot().revision;
            let options = SessionOptions::new(Vec::new(), &BTreeMap::new()).unwrap();
            let mut change = request(id, 1, MODEL, &options);
            let checkpoint = Arc::make_mut(&mut change.snapshot);
            checkpoint.kind = SessionCheckpointKind::History;
            checkpoint.history_revision = Some(1);
            checkpoint.history = Some(history.clone());
            let (claimed, claims) = flume::unbounded();
            let saving = if guarded {
                let claim: CheckpointClaim = Arc::new(move |publication| {
                    claimed.send(()).unwrap();
                    if rejected {
                        Err(Arc::from(CLAIM_ERROR))
                    } else {
                        publication_claim()(publication)
                    }
                });
                writer.checkpoint_guarded(change, CheckpointPublication::default(), claim)
            } else {
                writer.checkpoint(change)
            };
            slot.set(&access, TERMINAL.into(), None).unwrap();
            if let Some(identical) = replaced_history {
                let replacement = if identical {
                    HistorySnapshot {
                        epoch: history.epoch + 1,
                        ..history.clone()
                    }
                } else {
                    HistorySnapshot {
                        messages: Arc::new(vec![Message::user(TERMINAL.into())]),
                        ..history.clone()
                    }
                };
                journal
                    .commit(JournalCommit {
                        histories: [(root, replacement)].into(),
                        ..Default::default()
                    })
                    .unwrap();
            }
            let polling_journal = Arc::clone(&journal);
            writer
                .submit(move |dir, session, guard| {
                    SessionLogCheckpoint::save_pending_journal(
                        dir,
                        session,
                        guard,
                        &polling_journal,
                    )
                    .unwrap()
                })
                .await
                .unwrap();
            assert!(journal.published_revision() > captured_revision);
            let before = StoredSession::load(id, &dir).unwrap();
            let retained_history = lock(&writer.session).session.history_snapshot();
            let result = saving.await;
            if replaced_history.is_some() {
                assert!(matches!(result, Err(CheckpointError::Save { message, .. })
                    if message.as_ref() == HISTORY_NOT_READY));
            } else if rejected {
                assert!(matches!(result, Err(CheckpointError::Save { message, .. })
                    if message.as_ref() == CLAIM_ERROR));
            } else {
                result.unwrap();
            }
            assert_eq!(
                claims.len(),
                usize::from(guarded && replaced_history.is_none())
            );
            let saved = StoredSession::load(id, &dir).unwrap();
            assert_eq!(saved.state(), before.state());
            assert_eq!(saved.agent_revisions(), before.agent_revisions());
            assert_eq!(saved.messages(), before.messages());
            assert_eq!(saved.meta.compacted, before.meta.compacted);
            assert_eq!(saved.meta.conversation, before.meta.conversation);
            assert!(history_matches(
                &lock(&writer.session).session.history_snapshot(),
                &retained_history
            ));
            assert_eq!(
                saved.latest_state().unwrap().scopes[&Some(root)].plugins[STATE_KEY].value,
                Some(serde_json::json!(TERMINAL))
            );
        });
    }

    #[test_case::test_case(false; "prepared_committed")]
    #[test_case::test_case(true; "prepared_rejected")]
    fn drain_waits_outside_worker_for_prepared_publication_and_preserves_skipped_error(
        rejected: bool,
    ) {
        smol::block_on(async {
            let temporary = TempDir::new().unwrap();
            let dir = StateDir::from_path(temporary.path().to_path_buf());
            let mut session = StoredSession::new(MODEL, CWD);
            let root = AgentId::generate();
            session.bind_root_id(root).unwrap();
            session.save(&dir).unwrap();
            let id = session.id;
            let writer =
                SessionLogCheckpoint::open_with_session(dir.clone(), session, None).unwrap();
            let journal = writer.create_journal().await.unwrap();
            let slot = journal
                .register_state::<String>(StateSlotSpec {
                    key: STATE_KEY.into(),
                    scope: StateScope::Agent,
                    version: 1,
                })
                .unwrap();
            slot.set(&journal.state_access(root, None), ORIGINAL.into(), None)
                .unwrap();
            writer.drain().await.unwrap();
            let (entered, entering) = flume::bounded(1);
            let (release, released) = flume::bounded(1);
            let gate = writer.submit(move |_, _, _| {
                entered.send(()).unwrap();
                released.recv().unwrap();
                Ok(())
            });
            entering.recv_async().await.unwrap();
            slot.set(&journal.state_access(root, None), TERMINAL.into(), None)
                .unwrap();
            let prepared = journal
                .prepare(JournalCommit {
                    changes: vec![JournalChange {
                        agent: Some(root),
                        generation: 0,
                        change: StateChange::Config(
                            stored_agent_config(
                                &projection(COMMITTED_MODEL, CWD).config,
                                BTreeMap::new(),
                            )
                            .unwrap(),
                        ),
                    }],
                    ..Default::default()
                })
                .await
                .unwrap();
            journal.set_checkpoint_error(Some(CLAIM_ERROR.into()));
            let polling_journal = Arc::clone(&journal);
            let polling = writer.submit(move |dir, session, guard| {
                assert!(
                    SessionLogCheckpoint::save_pending_journal(
                        dir,
                        session,
                        guard,
                        &polling_journal
                    )
                    .is_none()
                );
                assert_eq!(
                    polling_journal.persistence_error().as_deref(),
                    Some(CLAIM_ERROR)
                );
                Ok(())
            });
            let mut draining = Box::pin(writer.drain());
            assert!(future::poll_once(draining.as_mut()).await.is_none());
            let (claimed, claiming) = flume::bounded(1);
            let (publish, publishing) = flume::bounded(1);
            let publication = CheckpointPublication::default();
            let (_trigger, cancel) = CancelToken::new();
            let registration = cancel.register_publication(&publication);
            let claim: CheckpointClaim = Arc::new(move |publication| {
                claimed.send(()).unwrap();
                publishing.recv().unwrap();
                if rejected {
                    Err(Arc::from(CLAIM_ERROR))
                } else {
                    publication_claim()(publication)
                }
            });
            let mut saving = Box::pin(writer.publish_prepared_with_claim(
                prepared,
                publication,
                registration,
                claim,
            ));
            assert!(future::poll_once(saving.as_mut()).await.is_none());
            release.send(()).unwrap();
            gate.await.unwrap();
            polling.await.unwrap();
            future::race(async { claiming.recv_async().await.unwrap() }, async {
                smol::Timer::after(CHILD_PREPARED_TIMEOUT).await;
                panic!("{CHILD_PREPARED_DEADLOCK}");
            })
            .await;
            let premature = future::poll_once(draining.as_mut()).await.is_some();
            let error = journal.persistence_error();
            let before = StoredSession::load(id, &dir).unwrap();
            publish.send(()).unwrap();
            assert!(!premature);
            assert_eq!(error.as_deref(), Some(CLAIM_ERROR));
            assert_eq!(
                before.latest_state().unwrap().scopes[&Some(root)].plugins[STATE_KEY].value,
                Some(serde_json::json!(ORIGINAL))
            );
            future::race(
                async {
                    assert_eq!(saving.await.is_err(), rejected);
                    draining.await.unwrap();
                },
                async {
                    smol::Timer::after(CHILD_PREPARED_TIMEOUT).await;
                    panic!("{CHILD_PREPARED_DEADLOCK}");
                },
            )
            .await;
            assert!(journal.persistence_error().is_none());
            let saved = StoredSession::load(id, &dir).unwrap();
            assert_eq!(
                saved.latest_state().unwrap().scopes[&Some(root)].plugins[STATE_KEY].value,
                Some(serde_json::json!(TERMINAL))
            );
            assert_eq!(journal.published_revision(), journal.snapshot().revision);
        });
    }

    #[test_case::test_case(false; "root")]
    #[test_case::test_case(true; "child")]
    fn state_only_checkpoint_round_trips_and_retries_without_new_messages(child: bool) {
        smol::block_on(async {
            let temporary = TempDir::new().unwrap();
            let dir = StateDir::from_path(temporary.path().to_path_buf());
            let mut session = StoredSession::new(MODEL, CWD);
            let root = AgentId::generate();
            session.bind_root_id(root).unwrap();
            session.save(&dir).unwrap();
            let id = session.id;
            let writer =
                SessionLogCheckpoint::open_with_session(dir.clone(), session, None).unwrap();
            let journal = writer.create_journal().await.unwrap();
            let agent = if child {
                let snapshot = journal.snapshot();
                let mut record = snapshot.agent_revisions.last().unwrap().1.clone();
                record.id = AgentId::generate();
                record.parent = Some(root);
                record.depth = 1;
                record.transcript_key = CHILD_KEY.into();
                let id = record.id;
                journal
                    .commit(JournalCommit {
                        agents: vec![record],
                        ..Default::default()
                    })
                    .unwrap();
                id
            } else {
                root
            };
            let slot = journal
                .register_state::<String>(StateSlotSpec {
                    key: STATE_KEY.into(),
                    scope: StateScope::Agent,
                    version: 1,
                })
                .unwrap();
            let access = journal.state_access(agent, None);
            slot.set(&access, ORIGINAL.into(), None).unwrap();
            writer.drain().await.unwrap();
            let saved = StoredSession::load(id, &dir).unwrap();
            assert!(saved.messages().is_empty());
            assert_eq!(
                saved.latest_state().unwrap().scopes[&Some(agent)].plugins[STATE_KEY].value,
                Some(serde_json::json!(ORIGINAL))
            );
            slot.set(&access, TERMINAL.into(), None).unwrap();
            let sessions = dir.path().join(SESSIONS_DIR);
            let backup = temporary.path().join(SESSIONS_BACKUP);
            std::fs::rename(&sessions, &backup).unwrap();
            std::fs::write(&sessions, REJECTED).unwrap();
            assert!(writer.drain().await.is_err());
            assert!(journal.persistence_error().is_some());
            std::fs::remove_file(&sessions).unwrap();
            std::fs::rename(&backup, &sessions).unwrap();
            writer.drain().await.unwrap();
            assert!(journal.persistence_error().is_none());
            let saved = StoredSession::load(id, &dir).unwrap();
            assert!(saved.messages().is_empty());
            assert_eq!(
                saved.latest_state().unwrap().scopes[&Some(agent)].plugins[STATE_KEY].value,
                Some(serde_json::json!(TERMINAL))
            );
            assert_eq!(journal.published_revision(), journal.snapshot().revision);
        });
    }

    #[test_case::test_case(false; "collision_in_history")]
    #[test_case::test_case(true; "collision_in_config")]
    fn child_identity_collision_is_persistent(config_collision: bool) {
        smol::block_on(async {
            let tmp = TempDir::new().unwrap();
            let dir = StateDir::from_path(tmp.path().to_path_buf());
            let id = MakimaId::generate();
            let mut first = child(CHILD_KEY);
            first.agent_id = Some(MakimaId::generate().to_string());
            let mut session = child_session(id);
            session.set_subagents(vec![first.clone(), child(SIBLING_KEY)]);
            session.save(&dir).unwrap();
            let writer = SessionLogCheckpoint::open(dir.clone(), id, MODEL, CWD, None).unwrap();
            let history = HistorySnapshot::new(vec![Message::user(TERMINAL.into())]);
            writer
                .checkpoint_child(first.clone(), history.clone())
                .await
                .unwrap();
            assert_eq!(
                StoredSession::load(id, &dir).unwrap().subagents()[0].agent_id,
                first.agent_id
            );

            let mut second = first.clone();
            second.agent_id = Some(MakimaId::generate().to_string());
            if config_collision {
                writer
                    .checkpoint_child_config(
                        second,
                        None,
                        CheckpointPublication::default(),
                        Arc::new(|publication| {
                            assert!(publication.try_publish());
                            Ok(())
                        }),
                    )
                    .await
                    .unwrap();
            } else {
                writer
                    .checkpoint_child(second, history.clone())
                    .await
                    .unwrap();
            }
            drop(writer);
            let writer = SessionLogCheckpoint::open(dir.clone(), id, MODEL, CWD, None).unwrap();
            writer.checkpoint_child(first, history).await.unwrap();
            let loaded = StoredSession::load(id, &dir).unwrap();
            assert!(loaded.subagents()[0].ambiguous);
            assert_eq!(loaded.subagents()[0].agent_id, None);
        });
    }

    #[test_case::test_case(false; "history")]
    #[test_case::test_case(true; "config")]
    fn duplicate_tool_keys_stay_ambiguous(config: bool) {
        smol::block_on(async {
            let tmp = TempDir::new().unwrap();
            let dir = StateDir::from_path(tmp.path().to_path_buf());
            let id = MakimaId::generate();
            let mut first = child(CHILD_KEY);
            first.agent_id = Some(MakimaId::generate().to_string());
            let mut session = child_session(id);
            session.set_subagents(vec![first.clone(), first.clone(), child(SIBLING_KEY)]);
            session.save(&dir).unwrap();
            let writer = SessionLogCheckpoint::open(dir.clone(), id, MODEL, CWD, None).unwrap();
            if config {
                writer
                    .checkpoint_child_config(
                        first.clone(),
                        None,
                        CheckpointPublication::default(),
                        Arc::new(|publication| {
                            assert!(publication.try_publish());
                            Ok(())
                        }),
                    )
                    .await
                    .unwrap();
            } else {
                writer
                    .checkpoint_child(
                        first,
                        HistorySnapshot::new(vec![Message::user(TERMINAL.into())]),
                    )
                    .await
                    .unwrap();
            }
            let loaded = StoredSession::load(id, &dir).unwrap();
            for saved in loaded
                .subagents()
                .iter()
                .filter(|saved| saved.tool_use_id == CHILD_KEY)
            {
                assert!(saved.ambiguous);
                assert_eq!(saved.agent_id, None);
            }
        });
    }

    #[test_case::test_case(false, false; "history_conflicting_identity")]
    #[test_case::test_case(false, true; "history_unidentified")]
    #[test_case::test_case(true, false; "config_conflicting_identity")]
    #[test_case::test_case(true, true; "config_unidentified")]
    fn cross_key_identity_collision_is_persistent(config: bool, unidentified: bool) {
        smol::block_on(async {
            let tmp = TempDir::new().unwrap();
            let dir = StateDir::from_path(tmp.path().to_path_buf());
            let id = MakimaId::generate();
            let mut session = child_session(id);
            let mut first = child(CHILD_KEY);
            if !unidentified {
                first.agent_id = Some(MakimaId::generate().to_string());
            }
            let mut sibling = child(SIBLING_KEY);
            sibling.agent_id = Some(MakimaId::generate().to_string());
            session.set_subagents(vec![first.clone(), sibling.clone()]);
            session.save(&dir).unwrap();
            let writer = SessionLogCheckpoint::open(dir.clone(), id, MODEL, CWD, None).unwrap();
            first.agent_id.clone_from(&sibling.agent_id);
            if config {
                writer
                    .checkpoint_child_config(
                        first.clone(),
                        None,
                        CheckpointPublication::default(),
                        Arc::new(|publication| {
                            assert!(publication.try_publish());
                            Ok(())
                        }),
                    )
                    .await
                    .unwrap();
            } else {
                writer
                    .checkpoint_child(
                        first.clone(),
                        HistorySnapshot::new(vec![Message::user(TERMINAL.into())]),
                    )
                    .await
                    .unwrap();
            }
            drop(writer);
            let writer = SessionLogCheckpoint::open(dir.clone(), id, MODEL, CWD, None).unwrap();
            writer
                .checkpoint_child_config(
                    first,
                    None,
                    CheckpointPublication::default(),
                    Arc::new(|publication| {
                        assert!(publication.try_publish());
                        Ok(())
                    }),
                )
                .await
                .unwrap();
            let loaded = StoredSession::load(id, &dir).unwrap();
            for saved in loaded.subagents() {
                assert!(saved.ambiguous);
                assert_eq!(saved.agent_id, None);
            }
        });
    }

    #[test_case::test_case(false, false; "history_with_metadata")]
    #[test_case::test_case(false, true; "history_without_metadata")]
    #[test_case::test_case(true, false; "config_with_metadata")]
    #[test_case::test_case(true, true; "config_without_metadata")]
    fn unidentified_history_does_not_acquire_identity(config: bool, missing_metadata: bool) {
        smol::block_on(async {
            let tmp = TempDir::new().unwrap();
            let dir = StateDir::from_path(tmp.path().to_path_buf());
            let id = MakimaId::generate();
            let mut session = child_session(id);
            if missing_metadata {
                session.set_subagents(vec![child(SIBLING_KEY)]);
            }
            session.save(&dir).unwrap();
            let writer = SessionLogCheckpoint::open(dir.clone(), id, MODEL, CWD, None).unwrap();
            let mut incoming = child(CHILD_KEY);
            incoming.agent_id = Some(MakimaId::generate().to_string());
            if config {
                writer
                    .checkpoint_child_config(
                        incoming.clone(),
                        None,
                        CheckpointPublication::default(),
                        Arc::new(|publication| {
                            assert!(publication.try_publish());
                            Ok(())
                        }),
                    )
                    .await
                    .unwrap();
            } else {
                writer
                    .checkpoint_child(
                        incoming.clone(),
                        HistorySnapshot::new(vec![Message::user(TERMINAL.into())]),
                    )
                    .await
                    .unwrap();
            }
            drop(writer);
            let writer = SessionLogCheckpoint::open(dir.clone(), id, MODEL, CWD, None).unwrap();
            writer
                .checkpoint_child(
                    incoming,
                    HistorySnapshot::new(vec![Message::user(TERMINAL.into())]),
                )
                .await
                .unwrap();
            let loaded = StoredSession::load(id, &dir).unwrap();
            let saved = loaded
                .subagents()
                .iter()
                .find(|child| child.tool_use_id == CHILD_KEY)
                .unwrap();
            assert!(saved.ambiguous);
            assert_eq!(saved.agent_id, None);
        });
    }

    fn child_session(id: MakimaId) -> StoredSession {
        let mut session = StoredSession::new(MODEL, CWD);
        session.id = id;
        session.push_message(Message::user(ORIGINAL.into()));
        session.set_subagents(vec![child(CHILD_KEY), child(SIBLING_KEY)]);
        for key in [CHILD_KEY, SIBLING_KEY] {
            session.set_subagent_messages(
                key.into(),
                vec![
                    Message::user(ORIGINAL.into()),
                    Message::user(ORIGINAL.into()),
                ],
            );
        }
        for chat in [None, Some(CHILD_KEY), Some(SIBLING_KEY)] {
            session.push_post(StoredPost {
                chat: chat.map(Into::into),
                after_message: 2,
                post: Post {
                    text: POST_TEXT.into(),
                    level: PostLevel::Info,
                    agent: None,
                },
            });
        }
        session
    }

    fn request(
        id: MakimaId,
        revision: u64,
        model: &'static str,
        options: &SessionOptions,
    ) -> CheckpointRequest<SessionCheckpoint> {
        CheckpointRequest {
            session_id: id,
            version: CheckpointVersion { revision, epoch: 1 },
            snapshot: Arc::new(SessionCheckpoint {
                kind: SessionCheckpointKind::Config,
                history_revision: None,
                history: None,
                options: options.snapshot(),
                config: Some(projection(model, CWD)),
                journal: None,
            }),
        }
    }

    fn projection(model: &str, cwd: &str) -> CommittedConfigProjection {
        let mut config = EffectiveAgentConfig::new(
            RunSettings {
                provider: Arc::new(NullProvider),
                model: Model::from_spec(model).unwrap(),
                fast: false,
                workflow: false,
                thinking: ThinkingConfig::Off,
            },
            AgentMode::Build,
        );
        config.context = AgentContextSnapshot::new(PathBuf::from(cwd));
        config.mode_def = Some(ModeDef::default_for(ModeId::Build));
        CommittedConfigProjection {
            runtime: Arc::new(()),
            generation: 1,
            runtime_epoch: 1,
            config: Arc::new(config),
        }
    }

    #[test_case::test_case(false; "cancel_queued_publication")]
    #[test_case::test_case(true; "drop_waiter_after_claim")]
    fn prepared_publication_retains_token_and_fences_cancelled_waiter(claimed: bool) {
        smol::block_on(async {
            let tmp = TempDir::new().unwrap();
            let dir = StateDir::from_path(tmp.path().to_path_buf());
            let mut session = StoredSession::new(MODEL, CWD);
            let root = AgentId::generate();
            session.bind_root_id(root).unwrap();
            session.save(&dir).unwrap();
            let id = session.id;
            let writer =
                SessionLogCheckpoint::open_with_session(dir.clone(), session, None).unwrap();
            let journal = writer.create_journal().await.unwrap();
            let before = journal.snapshot();
            let prepared = journal
                .prepare(JournalCommit {
                    incarnations: vec![journal.incarnation(root)],
                    changes: vec![JournalChange {
                        agent: Some(root),
                        generation: 0,
                        change: StateChange::Config(
                            stored_agent_config(
                                &projection(COMMITTED_MODEL, CWD).config,
                                BTreeMap::new(),
                            )
                            .unwrap(),
                        ),
                    }],
                    ..Default::default()
                })
                .await
                .unwrap();
            let (trigger, cancel) = CancelToken::new();
            let (entered, entering) = flume::bounded(1);
            let (release, released) = flume::bounded(1);
            if claimed {
                let publication = CheckpointPublication::default();
                let registration = cancel.register_publication(&publication);
                let bound_journal = Arc::clone(&journal);
                let incarnation = journal.incarnation(root);
                let claim: CheckpointClaim = Arc::new(move |publication| {
                    assert!(
                        cancel
                            .claim_publication_if(publication, || bound_journal.incarnation(root)
                                == incarnation)
                    );
                    entered.send(()).unwrap();
                    released.recv().unwrap();
                    Ok(())
                });
                let saving =
                    writer.publish_prepared_with_claim(prepared, publication, registration, claim);
                let mut saving = Box::pin(saving);
                assert!(
                    futures_lite::future::poll_once(saving.as_mut())
                        .await
                        .is_none()
                );
                entering.recv_async().await.unwrap();
                drop(trigger);
                drop(saving);
                assert!(journal.try_publication_read().is_none());
                release.send(()).unwrap();
            } else {
                let gate = writer.submit(move |_, _, _| {
                    entered.send(()).unwrap();
                    released.recv().unwrap();
                    Ok(())
                });
                entering.recv_async().await.unwrap();
                let mut saving = Box::pin(writer.publish_prepared(
                    prepared,
                    cancel,
                    journal.incarnation(root),
                    Arc::clone(&journal),
                ));
                assert!(
                    futures_lite::future::poll_once(saving.as_mut())
                        .await
                        .is_none()
                );
                drop(trigger);
                drop(saving);
                release.send(()).unwrap();
                gate.await.unwrap();
            }
            writer.drain().await.unwrap();
            let loaded = StoredSession::load(id, &dir).unwrap();
            if claimed {
                assert_eq!(
                    loaded.latest_state().unwrap().scopes[&Some(root)]
                        .config
                        .as_ref()
                        .unwrap()
                        .model,
                    COMMITTED_MODEL
                );
                assert_eq!(loaded.state(), journal.snapshot().state);
                assert_eq!(journal.published_revision(), journal.snapshot().revision);
            } else {
                assert_eq!(loaded.state(), before.state);
                assert_eq!(journal.snapshot().state, before.state);
                assert_eq!(journal.published_revision(), before.revision);
            }
        });
    }

    #[test]
    fn ordinary_checkpoint_holds_publication_lane_through_disk_completion() {
        smol::block_on(async {
            let tmp = TempDir::new().unwrap();
            let dir = StateDir::from_path(tmp.path().to_path_buf());
            let root = AgentId::generate();
            let mut session = StoredSession::new(MODEL, CWD);
            session.bind_root_id(root).unwrap();
            let id = session.id;
            let writer =
                SessionLogCheckpoint::open_with_session(dir.clone(), session, None).unwrap();
            let journal = writer.create_journal().await.unwrap();
            let (entered, entering) = flume::bounded(1);
            let (release, released) = flume::bounded(1);
            let blocking = writer.submit(move |_, _, _| {
                entered.send(()).unwrap();
                released.recv().unwrap();
                Ok(())
            });
            entering.recv_async().await.unwrap();
            let options = SessionOptions::new(
                builtin_option_definitions(
                    MODEL,
                    [Arc::from(MODEL)],
                    false,
                    false,
                    false,
                    ThinkingConfig::Off,
                ),
                &BTreeMap::new(),
            )
            .unwrap();
            let mut saving = Box::pin(writer.checkpoint(request(id, 1, MODEL, &options)));
            assert!(
                futures_lite::future::poll_once(saving.as_mut())
                    .await
                    .is_none()
            );
            let mut preparing = Box::pin(journal.prepare(JournalCommit {
                changes: vec![JournalChange {
                    agent: Some(root),
                    generation: 0,
                    change: StateChange::Plugin {
                        key: CATALOG_ID.into(),
                        version: 1,
                        value: None,
                    },
                }],
                ..Default::default()
            }));
            assert!(
                futures_lite::future::poll_once(preparing.as_mut())
                    .await
                    .is_none()
            );
            release.send(()).unwrap();
            blocking.await.unwrap();
            saving.await.unwrap();
            let prepared = preparing.await.unwrap();
            let loaded = StoredSession::load(id, &dir).unwrap();
            assert!(
                loaded
                    .latest_state()
                    .unwrap()
                    .scopes
                    .values()
                    .all(|scope| scope.plugins.is_empty())
            );
            drop(prepared);
        });
    }

    #[test_case::test_case(false; "prepared_aborted")]
    #[test_case::test_case(true; "prepared_committed")]
    fn ordinary_checkpoint_waits_for_prepared_publication(committed: bool) {
        smol::block_on(async {
            let tmp = TempDir::new().unwrap();
            let dir = StateDir::from_path(tmp.path().to_path_buf());
            let root = AgentId::generate();
            let mut session = StoredSession::new(MODEL, CWD);
            session.bind_root_id(root).unwrap();
            let original = HistorySnapshot::new(vec![Message::user(ORIGINAL.into())]);
            session.adopt_history(&original);
            let id = session.id;
            let writer =
                SessionLogCheckpoint::open_with_session(dir.clone(), session, None).unwrap();
            let journal = writer.create_journal().await.unwrap();
            let history = HistorySnapshot::new(vec![Message::user(TERMINAL.into())]);
            let prepared = journal
                .prepare(JournalCommit {
                    histories: [(root, history.clone())].into(),
                    managed_histories: [root].into(),
                    ..Default::default()
                })
                .await
                .unwrap();
            let options = SessionOptions::new(Vec::new(), &BTreeMap::new()).unwrap();
            let mut change = request(id, 1, MODEL, &options);
            Arc::make_mut(&mut change.snapshot).kind = SessionCheckpointKind::Options;
            let mut saving = writer.checkpoint(change);
            assert!(
                futures_lite::future::poll_once(saving.as_mut())
                    .await
                    .is_none()
            );
            assert!(lock(&writer.session).session.state().is_empty());
            if committed {
                prepared.commit().unwrap();
            } else {
                drop(prepared);
            }
            saving.await.unwrap();
            let expected = if committed { history } else { original };
            assert_eq!(
                StoredSession::load(id, &dir).unwrap().messages(),
                expected.messages.as_ref()
            );
            assert_eq!(journal.published_revision(), journal.snapshot().revision);
        });
    }

    #[test_case::test_case(false; "publish_then_install")]
    #[test_case::test_case(true; "rejected_candidate")]
    fn prepared_checkpoint_publishes_exact_candidate_without_early_ack(rejected: bool) {
        smol::block_on(async {
            let tmp = TempDir::new().unwrap();
            let dir = StateDir::from_path(tmp.path().to_path_buf());
            let root = AgentId::generate();
            let mut session = StoredSession::new(MODEL, CWD);
            session.bind_root_id(root).unwrap();
            session.save(&dir).unwrap();
            let id = session.id;
            let writer =
                SessionLogCheckpoint::open_with_session(dir.clone(), session, None).unwrap();
            let journal = writer.create_journal().await.unwrap();
            let initial = journal.snapshot();
            let history = HistorySnapshot::new(vec![Message::user(COMMITTED_MODEL.into())]);
            let prepared = journal
                .prepare(JournalCommit {
                    changes: vec![JournalChange {
                        agent: Some(root),
                        generation: 1,
                        change: StateChange::Plugin {
                            key: CATALOG_ID.into(),
                            version: 1,
                            value: Some(serde_json::Value::Bool(true)),
                        },
                    }],
                    histories: [(root, history.clone())].into(),
                    managed_histories: [root].into(),
                    ..Default::default()
                })
                .await
                .unwrap();
            let options = SessionOptions::new(
                builtin_option_definitions(
                    MODEL,
                    [Arc::from(MODEL)],
                    false,
                    false,
                    false,
                    ThinkingConfig::Off,
                ),
                &BTreeMap::new(),
            )
            .unwrap();
            let mut request = request(id, 1, MODEL, &options);
            let checkpoint = Arc::make_mut(&mut request.snapshot);
            checkpoint.journal = Some(prepared.snapshot().clone());
            checkpoint.history_revision = Some(1);
            checkpoint.history = Some(history.clone());
            let claim: CheckpointClaim = Arc::new(move |publication| {
                if rejected {
                    return Err(Arc::from(CLAIM_ERROR));
                }
                assert!(publication.try_publish());
                Ok(())
            });
            let result = writer
                .checkpoint_guarded(request, CheckpointPublication::default(), claim)
                .await;
            assert_eq!(result.is_err(), rejected);
            assert_eq!(journal.published_revision(), 0);
            assert_eq!(journal.checkpoint_snapshot().unwrap().state, initial.state);
            let loaded = StoredSession::load(id, &dir).unwrap();
            if rejected {
                assert_eq!(loaded.state(), initial.state);
                assert!(loaded.messages().is_empty());
                drop(prepared);
            } else {
                assert_eq!(loaded.state(), prepared.snapshot().state);
                assert_eq!(loaded.messages(), history.messages.as_ref());
                let candidate = prepared.snapshot().clone();
                prepared.commit().unwrap();
                assert_eq!(journal.published_revision(), candidate.revision);
                assert_eq!(
                    journal.checkpoint_snapshot().unwrap().state,
                    candidate.state
                );
            }
        });
    }

    #[test_case::test_case(false; "history")]
    #[test_case::test_case(true; "config")]
    fn journal_child_projection_preserves_registered_identity_and_exact_cut(config: bool) {
        smol::block_on(async {
            let tmp = TempDir::new().unwrap();
            let dir = StateDir::from_path(tmp.path().to_path_buf());
            let root = AgentId::generate();
            let child_id = AgentId::generate();
            let mut session = StoredSession::new(MODEL, CWD);
            session.bind_root_id(root).unwrap();
            let mut record = session.agents()[&root].clone();
            record.id = child_id;
            record.parent = Some(root);
            record.depth = 1;
            record.transcript_key = CHILD_KEY.into();
            record.created_seq = 2;
            record.updated_seq = 2;
            session.push_agent_revision(2, record);
            let id = session.id;
            session.save(&dir).unwrap();
            let writer =
                SessionLogCheckpoint::open_with_session(dir.clone(), session, None).unwrap();
            let journal = writer.create_journal().await.unwrap();
            let mut history = HistorySnapshot::new(vec![Message::user(TERMINAL.into())]);
            history.compacted = true;
            let mut conversation = ConversationState::fresh(None);
            conversation.sent = Some(history.messages.len());
            history.conversation = Some(conversation);
            journal
                .commit(JournalCommit {
                    histories: [(child_id, history.clone())].into(),
                    managed_histories: [child_id].into(),
                    ..Default::default()
                })
                .unwrap();
            let mut metadata = child(CHILD_KEY);
            metadata.agent_id = Some(child_id.to_string());
            if config {
                writer
                    .checkpoint_child_config(
                        metadata.clone(),
                        None,
                        CheckpointPublication::default(),
                        Arc::new(|publication| {
                            assert!(publication.try_publish());
                            Ok(())
                        }),
                    )
                    .await
                    .unwrap();
            } else {
                let mut wrong_cut = history.clone();
                wrong_cut.conversation.as_mut().unwrap().sent = None;
                assert!(matches!(
                    writer.checkpoint_child(metadata.clone(), wrong_cut).await,
                    Err(CheckpointError::Save { message, .. }) if message.as_ref() == HISTORY_NOT_READY
                ));
                writer
                    .checkpoint_child(metadata.clone(), history.clone())
                    .await
                    .unwrap();
            }
            let loaded = StoredSession::load(id, &dir).unwrap();
            metadata.model = None;
            metadata.thinking = None;
            metadata.fast = false;
            assert_eq!(loaded.subagents(), &[metadata]);
            assert_eq!(loaded.subagent_messages()[CHILD_KEY], history.messages);
            assert_eq!(loaded.agents()[&child_id].transcript_key, CHILD_KEY);
            assert_eq!(journal.published_revision(), journal.snapshot().revision);
        });
    }

    #[test_case::test_case(false; "publish_then_install")]
    #[test_case::test_case(true; "rejected_candidate")]
    fn prepared_child_config_uses_candidate_without_read_lane_or_early_ack(rejected: bool) {
        smol::block_on(async {
            let tmp = TempDir::new().unwrap();
            let dir = StateDir::from_path(tmp.path().to_path_buf());
            let root = AgentId::generate();
            let child_id = AgentId::generate();
            let mut session = StoredSession::new(MODEL, CWD);
            session.bind_root_id(root).unwrap();
            let mut record = session.agents()[&root].clone();
            record.id = child_id;
            record.parent = Some(root);
            record.depth = 1;
            record.transcript_key = CHILD_KEY.into();
            record.created_seq = 2;
            record.updated_seq = 2;
            session.push_agent_revision(2, record);
            let mut metadata = child(CHILD_KEY);
            metadata.agent_id = Some(child_id.to_string());
            session.set_subagents(vec![metadata.clone()]);
            session.adopt_subagent_history(
                CHILD_KEY.into(),
                &HistorySnapshot::new(vec![Message::user(ORIGINAL.into())]),
            );
            session.save(&dir).unwrap();
            let id = session.id;
            let writer =
                SessionLogCheckpoint::open_with_session(dir.clone(), session, None).unwrap();
            let journal = writer.create_journal().await.unwrap();
            let initial = journal.snapshot();
            let changed = HistorySnapshot::new(vec![Message::user(TERMINAL.into())]);
            let mut config = projection(COMMITTED_MODEL, COMMITTED_CWD)
                .config
                .as_ref()
                .clone();
            config.mode_def = Some(ModeDef::default_for(ModeId::Build));
            let stored = stored_agent_config(&config, BTreeMap::new()).unwrap();
            let prepared = journal
                .prepare(JournalCommit {
                    changes: vec![JournalChange {
                        agent: Some(child_id),
                        generation: 1,
                        change: StateChange::Config(stored.clone()),
                    }],
                    histories: [(child_id, changed.clone())].into(),
                    managed_histories: [child_id].into(),
                    ..Default::default()
                })
                .await
                .unwrap();
            metadata.model = Some(COMMITTED_MODEL.into());
            let publication = CheckpointPublication::default();
            let (entered, entering) = flume::bounded(1);
            let (release, released) = flume::bounded(1);
            let claim: CheckpointClaim = Arc::new(move |publication| {
                entered.send(()).unwrap();
                released.recv().unwrap();
                if rejected {
                    return Err(Arc::from(CLAIM_ERROR));
                }
                assert!(publication.try_publish());
                Ok(())
            });
            let writer_task = writer.clone();
            let candidate = prepared.snapshot().clone();
            let writing = smol::spawn(async move {
                writer_task
                    .checkpoint_child_config(metadata, Some(candidate), publication, claim)
                    .await
            });
            let claimed = futures_lite::future::or(
                async {
                    entering.recv_async().await.unwrap();
                    true
                },
                async {
                    smol::Timer::after(CHILD_PREPARED_TIMEOUT).await;
                    false
                },
            )
            .await;
            release.send(()).unwrap();
            assert!(claimed, "{CHILD_PREPARED_DEADLOCK}");
            assert_eq!(journal.published_revision(), 0);
            assert_eq!(journal.checkpoint_snapshot().unwrap().state, initial.state);
            assert_eq!(writing.await.is_err(), rejected);
            assert_eq!(journal.published_revision(), 0);
            let loaded = StoredSession::load(id, &dir).unwrap();
            if rejected {
                assert_eq!(loaded.state(), initial.state);
                assert_eq!(
                    loaded.subagent_messages()[CHILD_KEY],
                    initial.histories[&child_id].messages
                );
            } else {
                assert_eq!(loaded.state(), prepared.snapshot().state);
                assert_eq!(loaded.subagent_messages()[CHILD_KEY], changed.messages);
                assert_eq!(
                    loaded.latest_state().unwrap().scopes[&Some(child_id)]
                        .config
                        .as_ref(),
                    Some(&stored)
                );
                let revision = prepared.snapshot().revision;
                prepared.commit().unwrap();
                assert_eq!(journal.published_revision(), revision);
            }
        });
    }

    #[test_case::test_case(false; "ordinary")]
    #[test_case::test_case(true; "guarded")]
    fn journal_conversion_error_rejects_publication(guarded: bool) {
        smol::block_on(async {
            let tmp = TempDir::new().unwrap();
            let dir = StateDir::from_path(tmp.path().to_path_buf());
            let mut session = StoredSession::new(MODEL, CWD);
            let id = session.id;
            let root = AgentId::generate();
            session.bind_root_id(root).unwrap();
            session.save(&dir).unwrap();
            let writer =
                SessionLogCheckpoint::open_with_session(dir.clone(), session, None).unwrap();
            let journal = writer.create_journal().await.unwrap();
            let turn = TurnId::generate();
            let result = TurnResult {
                outcome: TurnOutcome::completed(
                    root,
                    turn,
                    TokenUsage::default(),
                    1,
                    DoneReason::EndTurn,
                ),
                text: ORIGINAL.repeat(MAX_INLINE_TURN_RESULT_BYTES),
                output: Vec::new(),
                provenance: TurnProvenance::default(),
                duration_ms: 0,
            };
            let expected = result.to_stored(id, None).unwrap_err().to_string();
            journal.record_terminal(result, None).unwrap();
            journal.release_terminal(root, turn);
            assert_eq!(
                journal.wait_terminal_conversion(root, turn).await,
                Err(expected.clone())
            );
            let before = stored_value(&StoredSession::load(id, &dir).unwrap());
            let options = SessionOptions::new(Vec::new(), &BTreeMap::new()).unwrap();
            let change = request(id, 1, COMMITTED_MODEL, &options);
            let result = if guarded {
                writer
                    .checkpoint_guarded(
                        change,
                        CheckpointPublication::default(),
                        Arc::new(|_| Err(Arc::from(CLAIM_ERROR))),
                    )
                    .await
            } else {
                writer.checkpoint(change).await
            };
            assert!(matches!(
                result,
                Err(CheckpointError::Save { message, .. }) if message.as_ref() == expected
            ));
            assert_eq!(
                stored_value(&StoredSession::load(id, &dir).unwrap()),
                before
            );
            assert_eq!(stored_value(&lock(&writer.session).session), before);
            assert_eq!(journal.published_revision(), 0);
        });
    }

    #[test_case::test_case(false; "unbound")]
    #[test_case::test_case(true; "different_session")]
    fn checkpoint_journal_binding_enforces_session_identity(different_session: bool) {
        smol::block_on(async {
            let tmp = TempDir::new().unwrap();
            let dir = StateDir::from_path(tmp.path().to_path_buf());
            let session = StoredSession::new(MODEL, CWD);
            let id = session.id;
            let writer = SessionLogCheckpoint::open_with_session(dir, session, None).unwrap();
            let journal = Arc::new(SessionJournal::default());
            let owner = if different_session {
                MakimaId::generate()
            } else {
                id
            };
            if different_session {
                journal.bind_session(owner).unwrap();
            }
            journal
                .commit(JournalCommit {
                    changes: vec![JournalChange {
                        agent: None,
                        generation: 1,
                        change: StateChange::Plugin {
                            key: CATALOG_ID.into(),
                            version: 1,
                            value: Some(serde_json::json!(ORIGINAL)),
                        },
                    }],
                    ..Default::default()
                })
                .unwrap();
            let before = stored_value(&lock(&writer.session).session);
            let result = writer.bind_journal(Arc::clone(&journal)).await;
            assert_eq!(journal.session_id(), Some(owner));
            if different_session {
                assert!(matches!(result, Err(CheckpointError::Save { .. })));
                assert!(lock(&writer.journal).is_none());
                assert_eq!(stored_value(&lock(&writer.session).session), before);
            } else {
                result.unwrap();
                assert_eq!(
                    lock(&writer.session).session.state(),
                    journal.snapshot().state
                );
                writer.bind_journal(Arc::clone(&journal)).await.unwrap();
            }
            assert_eq!(
                writer.create_journal().await.unwrap().session_id(),
                Some(id)
            );
        });
    }

    #[test_case::test_case(Some(SessionCheckpointKind::History); "root_history")]
    #[test_case::test_case(Some(SessionCheckpointKind::Config); "root_config")]
    #[test_case::test_case(None; "child_history")]
    fn ordinary_journal_save_cannot_cross_pending_conversion(kind: Option<SessionCheckpointKind>) {
        smol::block_on(async {
            let tmp = TempDir::new().unwrap();
            let dir = StateDir::from_path(tmp.path().to_path_buf());
            let root = AgentId::generate();
            let child_id = AgentId::generate();
            let mut session = StoredSession::new(MODEL, CWD);
            session.bind_root_id(root).unwrap();
            let id = session.id;
            let mut metadata = child(CHILD_KEY);
            metadata.agent_id = Some(child_id.to_string());
            session.set_subagents(vec![metadata.clone()]);
            let mut record = session.agents()[&root].clone();
            record.id = child_id;
            record.parent = Some(root);
            record.depth = 1;
            record.transcript_key = CHILD_KEY.into();
            let writer =
                SessionLogCheckpoint::open_with_session(dir.clone(), session, None).unwrap();
            let journal = writer.create_journal().await.unwrap();
            let original = HistorySnapshot::new(vec![Message::user(ORIGINAL.into())]);
            journal
                .commit(JournalCommit {
                    agents: vec![record],
                    histories: [(root, original.clone()), (child_id, original.clone())].into(),
                    managed_histories: [root, child_id].into(),
                    ..Default::default()
                })
                .unwrap();
            let before = journal.snapshot();
            let options = SessionOptions::new(Vec::new(), &BTreeMap::new()).unwrap();
            let mut baseline = request(id, 0, MODEL, &options);
            Arc::make_mut(&mut baseline.snapshot).kind = SessionCheckpointKind::Options;
            writer.checkpoint(baseline).await.unwrap();
            let terminal = HistorySnapshot::new(vec![Message::user(TERMINAL.into())]);
            let pending = journal
                .stage(
                    JournalCommit {
                        histories: [(root, terminal.clone()), (child_id, terminal.clone())].into(),
                        ..Default::default()
                    },
                    1,
                )
                .unwrap();
            let (entered, started) = flume::bounded(1);
            let (release, released) = flume::bounded(1);
            let gate = writer.submit(move |_, _, _| {
                entered.send(()).unwrap();
                released.recv().unwrap();
                Ok(())
            });
            started.recv_async().await.unwrap();
            let save = |history: HistorySnapshot<Message>, revision: u64| -> SaveFuture<()> {
                if let Some(kind) = &kind {
                    let mut change = request(id, 1, MODEL, &options);
                    let checkpoint = Arc::make_mut(&mut change.snapshot);
                    checkpoint.kind = kind.clone();
                    checkpoint.history_revision = Some(revision);
                    checkpoint.history = Some(history);
                    let future = writer.checkpoint(change);
                    Box::pin(async move { future.await.map(|_| ()) })
                } else {
                    let writer = writer.clone();
                    let metadata = metadata.clone();
                    Box::pin(async move { writer.checkpoint_child(metadata, history).await })
                }
            };
            let mut checkpoint = save(terminal.clone(), 2);
            assert!(
                checkpoint
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop()))
                    .is_pending()
            );
            let agent = if kind.is_some() { root } else { child_id };
            let turn = TurnId::generate();
            let result = TurnResult {
                outcome: TurnOutcome::completed(
                    agent,
                    turn,
                    TokenUsage::default(),
                    1,
                    DoneReason::EndTurn,
                ),
                text: TERMINAL.into(),
                output: Vec::new(),
                provenance: TurnProvenance::default(),
                duration_ms: 0,
            }
            .to_stored(id, None)
            .unwrap();
            journal
                .complete(
                    pending,
                    vec![JournalChange {
                        agent: Some(agent),
                        generation: 1,
                        change: StateChange::Turn {
                            turn,
                            result: result.clone(),
                        },
                    }],
                )
                .unwrap();
            release.send(()).unwrap();
            gate.await.unwrap();
            assert!(matches!(
                checkpoint.await,
                Err(CheckpointError::Save { message, .. }) if message.as_ref() == HISTORY_NOT_READY
            ));
            assert_eq!(lock(&writer.session).history_version, None);
            let loaded = StoredSession::load(id, &dir).unwrap();
            assert_eq!(loaded.state(), before.state);
            assert_eq!(loaded.messages(), original.messages.as_ref());
            assert_eq!(
                loaded.subagent_messages()[CHILD_KEY].as_ref(),
                original.messages.as_ref()
            );
            assert_eq!(journal.published_revision(), before.revision);
            save(terminal.clone(), 2).await.unwrap();
            assert_eq!(
                lock(&writer.session).history_version,
                kind.as_ref().map(|_| (terminal.epoch, 2))
            );
            let loaded = StoredSession::load(id, &dir).unwrap();
            assert_eq!(loaded.messages(), terminal.messages.as_ref());
            assert_eq!(
                loaded.subagent_messages()[CHILD_KEY].as_ref(),
                terminal.messages.as_ref()
            );
            assert_eq!(
                loaded.latest_state().unwrap().scopes[&Some(agent)].results[0],
                (turn, result)
            );
            assert_eq!(journal.published_revision(), journal.snapshot().revision);
            assert!(matches!(
                save(original, 3).await,
                Err(CheckpointError::Save { message, .. }) if message.as_ref() == HISTORY_SUPERSEDED
            ));
            if let Some(kind) = &kind {
                let mut stale = request(id, 3, COMMITTED_MODEL, &options);
                let checkpoint = Arc::make_mut(&mut stale.snapshot);
                checkpoint.kind = kind.clone();
                checkpoint.history_revision = Some(1);
                checkpoint.history = Some(terminal.clone());
                assert!(matches!(
                    writer.checkpoint(stale).await,
                    Err(CheckpointError::Save { message, .. }) if message.as_ref() == HISTORY_SUPERSEDED
                ));
                assert_eq!(
                    lock(&writer.session).history_version,
                    Some((terminal.epoch, 2))
                );
            }
            let mismatched = HistorySnapshot {
                messages: Arc::new(vec![Message::user(REJECTED.into())]),
                ..terminal.clone()
            };
            assert!(matches!(
                save(mismatched, 3).await,
                Err(CheckpointError::Save { message, .. }) if message.as_ref() == HISTORY_NOT_READY
            ));
            assert_eq!(
                lock(&writer.session).history_version,
                kind.as_ref().map(|_| (terminal.epoch, 2))
            );
            let loaded = StoredSession::load(id, &dir).unwrap();
            assert_eq!(loaded.state(), journal.snapshot().state);
            assert_eq!(loaded.model, MODEL);
            assert_eq!(loaded.messages(), terminal.messages.as_ref());
            assert_eq!(
                loaded.subagent_messages()[CHILD_KEY].as_ref(),
                terminal.messages.as_ref()
            );
        });
    }

    #[test_case::test_case(false; "rejected")]
    #[test_case::test_case(true; "authorized")]
    fn guarded_journal_save_preserves_prepared_history_overlay(authorized: bool) {
        smol::block_on(async {
            let tmp = TempDir::new().unwrap();
            let dir = StateDir::from_path(tmp.path().to_path_buf());
            let root = AgentId::generate();
            let mut session = StoredSession::new(MODEL, CWD);
            session.bind_root_id(root).unwrap();
            let id = session.id;
            let original = HistorySnapshot::new(vec![Message::user(ORIGINAL.into())]);
            session.adopt_history(&original);
            session.save(&dir).unwrap();
            let writer =
                SessionLogCheckpoint::open_with_session(dir.clone(), session, None).unwrap();
            let journal = writer.create_journal().await.unwrap();
            journal
                .commit(JournalCommit {
                    histories: [(root, original.clone())].into(),
                    managed_histories: [root].into(),
                    ..Default::default()
                })
                .unwrap();
            let options = SessionOptions::new(Vec::new(), &BTreeMap::new()).unwrap();
            let overlay = HistorySnapshot::new(vec![Message::user(TERMINAL.into())]);
            let prepared = journal
                .prepare(JournalCommit {
                    histories: [(root, overlay.clone())].into(),
                    managed_histories: [root].into(),
                    ..Default::default()
                })
                .await
                .unwrap();
            let mut change = request(id, 1, MODEL, &options);
            let checkpoint = Arc::make_mut(&mut change.snapshot);
            checkpoint.kind = SessionCheckpointKind::History;
            checkpoint.history_revision = Some(1);
            checkpoint.history = Some(overlay.clone());
            checkpoint.journal = Some(prepared.snapshot().clone());
            let result = writer
                .checkpoint_guarded(
                    change,
                    CheckpointPublication::default(),
                    Arc::new(move |publication| {
                        if !authorized {
                            return Err(Arc::from(CLAIM_ERROR));
                        }
                        assert!(publication.try_publish());
                        Ok(())
                    }),
                )
                .await;
            assert_eq!(journal.published_revision(), 0);
            assert_eq!(
                journal.snapshot().histories[&root].messages,
                original.messages
            );
            if authorized {
                result.unwrap();
                prepared.commit().unwrap();
                assert_eq!(journal.published_revision(), journal.snapshot().revision);
            } else {
                assert!(
                    matches!(result, Err(CheckpointError::Save { message, .. }) if message.as_ref() == CLAIM_ERROR)
                );
                drop(prepared);
            }
            let expected = if authorized { overlay } else { original };
            assert_eq!(
                StoredSession::load(id, &dir).unwrap().messages(),
                expected.messages.as_ref()
            );
            assert_eq!(
                lock(&writer.session).session.messages(),
                expected.messages.as_ref()
            );
        });
    }

    #[test_case::test_case(false; "unguarded")]
    #[test_case::test_case(true; "guarded")]
    fn options_checkpoint_rebases_older_cut_after_newer_publication(guarded: bool) {
        smol::block_on(async {
            let tmp = TempDir::new().unwrap();
            let dir = StateDir::from_path(tmp.path().to_path_buf());
            let root = AgentId::generate();
            let mut session = StoredSession::new(MODEL, CWD);
            session.bind_root_id(root).unwrap();
            let id = session.id;
            let writer =
                SessionLogCheckpoint::open_with_session(dir.clone(), session, None).unwrap();
            let journal = writer.create_journal().await.unwrap();
            let options = SessionOptions::new(Vec::new(), &BTreeMap::new()).unwrap();
            let (claimed, claims) = flume::unbounded();
            let save = |revision| {
                let mut change = request(id, revision, MODEL, &options);
                Arc::make_mut(&mut change.snapshot).kind = SessionCheckpointKind::Options;
                if guarded {
                    let claimed = claimed.clone();
                    writer.checkpoint_guarded(
                        change,
                        CheckpointPublication::default(),
                        Arc::new(move |publication| {
                            claimed.send(()).unwrap();
                            assert!(publication.try_publish());
                            Ok(())
                        }),
                    )
                } else {
                    writer.checkpoint(change)
                }
            };
            let commit = |text: &str| {
                journal
                    .commit(JournalCommit {
                        changes: vec![JournalChange {
                            agent: Some(root),
                            generation: 1,
                            change: StateChange::Plugin {
                                key: CATALOG_ID.into(),
                                version: 1,
                                value: Some(serde_json::json!(text)),
                            },
                        }],
                        histories: [(root, HistorySnapshot::new(vec![Message::user(text.into())]))]
                            .into(),
                        managed_histories: [root].into(),
                        ..Default::default()
                    })
                    .unwrap();
            };
            commit(ORIGINAL);
            let older = save(1);
            commit(TERMINAL);
            let newer = save(2);
            newer.await.unwrap();
            let published_revision = journal.published_revision();
            let before = lock(&writer.session).clone();
            let published = StoredSession::load(id, &dir).unwrap();
            let ack = older.await.unwrap();
            assert_eq!(ack.version.revision, 1);
            assert_eq!(claims.len(), if guarded { 2 } else { 0 });
            {
                let retained = lock(&writer.session);
                assert!(history_matches(
                    &retained.session.history_snapshot(),
                    &before.session.history_snapshot()
                ));
                assert_eq!(retained.session.state(), before.session.state());
                assert_eq!(
                    retained.session.agent_revisions(),
                    before.session.agent_revisions()
                );
                assert_eq!(retained.session.meta, before.session.meta);
                assert_eq!(retained.history_version, before.history_version);
            }
            assert_eq!(journal.published_revision(), published_revision);
            let loaded = StoredSession::load(id, &dir).unwrap();
            assert_eq!(loaded.state(), published.state());
            assert_eq!(loaded.agent_revisions(), published.agent_revisions());
            assert_eq!(loaded.meta, published.meta);
            assert_eq!(
                loaded.messages(),
                HistorySnapshot::new(vec![Message::user(TERMINAL.into())])
                    .messages
                    .as_ref()
            );
            assert_eq!(
                loaded.latest_state().unwrap().scopes[&Some(root)].plugins[CATALOG_ID].value,
                Some(serde_json::json!(TERMINAL)),
            );
        });
    }

    #[test_case::test_case(false; "unguarded")]
    #[test_case::test_case(true; "guarded")]
    fn journal_publication_uses_submission_snapshot_and_acknowledges_success(guarded: bool) {
        smol::block_on(async {
            let tmp = TempDir::new().unwrap();
            let dir = StateDir::from_path(tmp.path().to_path_buf());
            let id = MakimaId::generate();
            let root = AgentId::generate();
            let writer =
                SessionLogCheckpoint::open_with_root(dir.clone(), id, MODEL, CWD, None, root, None)
                    .unwrap();
            let journal = writer.create_journal().await.unwrap();
            writer.bind_journal(Arc::clone(&journal)).await.unwrap();
            assert!(Arc::ptr_eq(
                &journal,
                &writer.create_journal().await.unwrap()
            ));
            journal
                .commit(JournalCommit {
                    changes: vec![JournalChange {
                        agent: Some(root),
                        generation: 1,
                        change: StateChange::Plugin {
                            key: CATALOG_ID.into(),
                            version: 1,
                            value: Some(serde_json::json!(ORIGINAL)),
                        },
                    }],
                    ..Default::default()
                })
                .unwrap();
            let options = SessionOptions::new(
                builtin_option_definitions(
                    MODEL,
                    [Arc::from(MODEL)],
                    false,
                    false,
                    false,
                    ThinkingConfig::Off,
                ),
                &BTreeMap::new(),
            )
            .unwrap();
            let change = || {
                let mut change = request(id, 1, MODEL, &options);
                Arc::make_mut(&mut change.snapshot).kind = SessionCheckpointKind::Options;
                change
            };
            let rejected = writer
                .checkpoint_guarded(
                    change(),
                    CheckpointPublication::default(),
                    Arc::new(|_| Err(Arc::from(CLAIM_ERROR))),
                )
                .await
                .unwrap_err();
            assert!(
                matches!(rejected, CheckpointError::Save { message, .. } if message.as_ref() == CLAIM_ERROR)
            );
            assert_eq!(journal.published_revision(), 0);
            assert!(
                !dir.ensure_subdir(SESSIONS_DIR)
                    .unwrap()
                    .join(format!("{id}.jsonl"))
                    .exists()
            );
            assert_eq!(lock(&writer.session).session.state().len(), 0);
            let checkpoint = if guarded {
                writer.checkpoint_guarded(
                    change(),
                    CheckpointPublication::default(),
                    Arc::new(|publication| {
                        assert!(publication.try_publish());
                        Ok(())
                    }),
                )
            } else {
                writer.checkpoint(change())
            };
            journal
                .commit(JournalCommit {
                    changes: vec![JournalChange {
                        agent: Some(root),
                        generation: 1,
                        change: StateChange::Plugin {
                            key: CATALOG_ID.into(),
                            version: 1,
                            value: Some(serde_json::json!(TERMINAL)),
                        },
                    }],
                    ..Default::default()
                })
                .unwrap();
            checkpoint.await.unwrap();
            assert_eq!(journal.published_revision(), 1);
            let loaded = StoredSession::load(id, &dir).unwrap();
            assert_eq!(loaded.root_agent_id(), Some(root));
            assert_eq!(
                loaded.latest_state().unwrap().scopes[&Some(root)].plugins[CATALOG_ID].value,
                Some(serde_json::json!(ORIGINAL)),
            );
            writer.checkpoint(change()).await.unwrap();
            assert_eq!(journal.published_revision(), 2);
            let loaded = StoredSession::load(id, &dir).unwrap();
            assert_eq!(
                loaded.latest_state().unwrap().scopes[&Some(root)].plugins[CATALOG_ID].value,
                Some(serde_json::json!(TERMINAL)),
            );
        });
    }

    struct IdleBackend;

    impl ActorBackend for IdleBackend {
        fn run_turn<'a>(
            &'a mut self,
            _: ActorHistory<'a>,
            _: TurnContext,
            _: AgentInput,
            _: WorkKind,
        ) -> Pin<Box<dyn Future<Output = BackendResult> + Send + 'a>> {
            Box::pin(async { BackendResult::SetupFailed(None) })
        }

        fn run_control<'a>(
            &'a mut self,
            _: ActorHistory<'a>,
            _: TurnContext,
            _: &'a ControlWork,
        ) -> Pin<Box<dyn Future<Output = BackendResult> + Send + 'a>> {
            Box::pin(async { BackendResult::ControlDone })
        }

        fn run_compact<'a>(
            &'a mut self,
            _: ActorHistory<'a>,
            _: TurnContext,
            _: Option<&'a str>,
        ) -> Pin<Box<dyn Future<Output = BackendResult> + Send + 'a>> {
            Box::pin(async { BackendResult::CompactDone })
        }
    }

    #[test_case::test_case(false; "empty_session")]
    #[test_case::test_case(true; "bound_root")]
    fn journal_binding_accepts_manager_root_and_config_extension(bound_root: bool) {
        smol::block_on(async {
            let tmp = TempDir::new().unwrap();
            let dir = StateDir::from_path(tmp.path().to_path_buf());
            let mut session = StoredSession::new(MODEL, CWD);
            let id = session.id;
            let root = AgentId::generate();
            if bound_root {
                session.bind_root_id(root).unwrap();
            }
            let original_state = session.state().to_vec();
            let original_agents = session.agent_revisions().to_vec();
            let journal = Arc::new(SessionJournal::from_session(&session).unwrap());
            let writer =
                SessionLogCheckpoint::open_with_session(dir.clone(), session, None).unwrap();
            let manager =
                AgentManagerHandle::new_with_journal(AgentLimits::default(), Arc::clone(&journal))
                    .unwrap();
            let mut config = (*projection(MODEL, CWD).config).clone();
            config.mode_def = Some(ModeDef::default_for(ModeId::parse("build")));
            let (start, wait) = flume::bounded(1);
            manager
                .create_root_deferred_with_id_and_config(
                    root,
                    Some(config),
                    Vec::new(),
                    None,
                    |_| Ok::<Box<dyn ActorBackend>, String>(Box::new(IdleBackend)),
                    wait,
                )
                .unwrap();
            let snapshot = journal.snapshot();
            assert!(snapshot.state.starts_with(&original_state));
            assert!(snapshot.agent_revisions.starts_with(&original_agents));
            assert!(snapshot.agent_revisions.len() > original_agents.len());
            assert!(!snapshot.state.is_empty());
            writer.bind_journal(Arc::clone(&journal)).await.unwrap();
            {
                let retained = lock(&writer.session);
                assert_eq!(retained.session.state(), snapshot.state);
                assert_eq!(retained.session.agent_revisions(), snapshot.agent_revisions);
            }
            let path = dir
                .ensure_subdir(SESSIONS_DIR)
                .unwrap()
                .join(format!("{id}.jsonl"));
            assert!(!path.exists());
            assert_eq!(journal.published_revision(), 0);
            let options = SessionOptions::new(
                builtin_option_definitions(
                    MODEL,
                    [Arc::from(MODEL)],
                    false,
                    false,
                    false,
                    ThinkingConfig::Off,
                ),
                &BTreeMap::new(),
            )
            .unwrap();
            let mut change = request(id, 1, MODEL, &options);
            Arc::make_mut(&mut change.snapshot).kind = SessionCheckpointKind::Options;
            writer.checkpoint(change).await.unwrap();
            let published = StoredSession::load(id, &dir).unwrap();
            assert_eq!(published.root_agent_id(), Some(root));
            assert_eq!(published.state(), snapshot.state);
            assert_eq!(published.agent_revisions(), snapshot.agent_revisions);
            assert_eq!(journal.published_revision(), snapshot.revision);
            start.send(()).unwrap();
            assert!(
                manager
                    .shutdown(std::time::Duration::from_secs(1))
                    .await
                    .timed_out
                    .is_empty()
            );
        });
    }

    #[test]
    fn journal_binding_rejects_a_different_migration_seed() {
        smol::block_on(async {
            let tmp = TempDir::new().unwrap();
            let dir = StateDir::from_path(tmp.path().to_path_buf());
            let id = MakimaId::generate();
            let writer = SessionLogCheckpoint::open_with_root(
                dir,
                id,
                MODEL,
                CWD,
                None,
                AgentId::generate(),
                None,
            )
            .unwrap();
            let unrelated = Arc::new(SessionJournal::default());
            assert!(matches!(
                writer.bind_journal(unrelated).await,
                Err(CheckpointError::Save { message, .. }) if message.as_ref() == JOURNAL_PREFIX_MISMATCH
            ));
            let journal =
                Arc::new(SessionJournal::from_session(&lock(&writer.session).session).unwrap());
            writer.bind_journal(Arc::clone(&journal)).await.unwrap();
            assert!(matches!(
                writer.bind_journal(Arc::new(SessionJournal::default())).await,
                Err(CheckpointError::Save { message, .. }) if message.as_ref() == JOURNAL_ALREADY_BOUND
            ));
            assert!(Arc::ptr_eq(
                &journal,
                &writer.create_journal().await.unwrap()
            ));
        });
    }

    #[test]
    fn loaded_legacy_session_shares_child_identity_with_journal() {
        smol::block_on(async {
            let tmp = TempDir::new().unwrap();
            let dir = StateDir::from_path(tmp.path().to_path_buf());
            let id = MakimaId::generate();
            let root = AgentId::generate();
            let header = serde_json::json!({
                "t": "header", "v": 2, "id": id, "model": MODEL,
                "cwd": CWD, "created_at": 1
            });
            let meta = serde_json::json!({
                "t": "meta", "title": ORIGINAL,
                "token_usage": TokenUsage::default(), "updated_at": 1,
                "subagents": [child(CHILD_KEY)]
            });
            let path = dir
                .ensure_subdir(SESSIONS_DIR)
                .unwrap()
                .join(format!("{id}.jsonl"));
            let legacy = format!("{header}\n{meta}\n");
            std::fs::write(&path, &legacy).unwrap();
            let mut loaded = StoredSession::load(id, &dir).unwrap();
            loaded.bind_root_id(root).unwrap();
            let child_id = loaded
                .agents()
                .values()
                .find(|agent| agent.transcript_key == CHILD_KEY)
                .unwrap()
                .id;
            let journal = Arc::new(SessionJournal::from_session(&loaded).unwrap());
            let independently_loaded =
                SessionLogCheckpoint::open_with_root(dir.clone(), id, MODEL, CWD, None, root, None)
                    .unwrap();
            assert!(matches!(
                independently_loaded.bind_journal(Arc::clone(&journal)).await,
                Err(CheckpointError::Save { message, .. }) if message.as_ref() == JOURNAL_PREFIX_MISMATCH
            ));
            let writer =
                SessionLogCheckpoint::open_with_session(dir.clone(), loaded, None).unwrap();
            writer.bind_journal(Arc::clone(&journal)).await.unwrap();
            assert_eq!(std::fs::read_to_string(&path).unwrap(), legacy);
            let options = SessionOptions::new(
                builtin_option_definitions(
                    MODEL,
                    [Arc::from(MODEL)],
                    false,
                    false,
                    false,
                    ThinkingConfig::Off,
                ),
                &BTreeMap::new(),
            )
            .unwrap();
            let mut change = request(id, 1, MODEL, &options);
            Arc::make_mut(&mut change.snapshot).kind = SessionCheckpointKind::Options;
            writer.checkpoint(change).await.unwrap();
            let published = StoredSession::load(id, &dir).unwrap();
            assert_eq!(published.root_agent_id(), Some(root));
            assert_eq!(published.agents()[&child_id].transcript_key, CHILD_KEY);
            assert_eq!(published.state(), journal.snapshot().state);
            assert_eq!(
                published.agent_revisions(),
                journal.snapshot().agent_revisions
            );
        });
    }

    #[test_case::test_case(false, false; "fresh_unguarded")]
    #[test_case::test_case(false, true; "fresh_guarded")]
    #[test_case::test_case(true, false; "legacy_unguarded")]
    #[test_case::test_case(true, true; "legacy_guarded")]
    fn root_is_bound_before_first_config_publication(legacy: bool, guarded: bool) {
        smol::block_on(async {
            let tmp = TempDir::new().unwrap();
            let dir = StateDir::from_path(tmp.path().to_path_buf());
            let id = MakimaId::generate();
            let root = AgentId::generate();
            let parent = ParentLink {
                session: MakimaId::generate(),
                message_count: 1,
                seq: 2,
            };
            let path = dir
                .ensure_subdir(SESSIONS_DIR)
                .unwrap()
                .join(format!("{id}.jsonl"));
            let original = legacy.then(|| {
                let header = serde_json::json!({
                    "t": "header", "v": 2, "id": id, "model": MODEL,
                    "cwd": CWD, "created_at": 1
                });
                let meta = serde_json::json!({
                    "t": "meta", "title": ORIGINAL,
                    "token_usage": TokenUsage::default(), "updated_at": 1
                });
                let contents = format!("{header}\n{meta}\n");
                std::fs::write(&path, &contents).unwrap();
                StoredSession::load(id, &dir).unwrap();
                contents
            });
            let writer = SessionLogCheckpoint::open_with_root(
                dir.clone(),
                id,
                MODEL,
                CWD,
                None,
                root,
                Some(parent.clone()),
            )
            .unwrap();
            assert_eq!(lock(&writer.session).session.root_agent_id(), Some(root));
            if let Some(original) = original {
                assert_eq!(std::fs::read_to_string(&path).unwrap(), original);
            } else {
                assert!(!path.exists());
            }
            let options = SessionOptions::new(
                builtin_option_definitions(
                    MODEL,
                    [Arc::from(MODEL)],
                    false,
                    false,
                    false,
                    ThinkingConfig::Off,
                ),
                &BTreeMap::new(),
            )
            .unwrap();
            let mut change = request(id, 1, COMMITTED_MODEL, &options);
            Arc::make_mut(
                &mut Arc::make_mut(&mut change.snapshot)
                    .config
                    .as_mut()
                    .unwrap()
                    .config,
            )
            .mode_def = Some(ModeDef::default_for(ModeId::parse("build")));
            writer
                .resolve_mode(&change.snapshot.config.as_ref().unwrap().config)
                .await
                .unwrap();
            if guarded {
                writer
                    .checkpoint_guarded(
                        change,
                        CheckpointPublication::default(),
                        Arc::new(|publication| {
                            assert!(publication.try_publish());
                            Ok(())
                        }),
                    )
                    .await
                    .unwrap();
            } else {
                writer.checkpoint(change).await.unwrap();
            }
            let loaded = StoredSession::load(id, &dir).unwrap();
            assert_eq!(loaded.root_agent_id(), Some(root));
            assert_eq!(loaded.parent(), Some(&parent));
            assert_eq!(
                loaded.latest_state().unwrap().scopes[&Some(root)]
                    .config
                    .as_ref()
                    .unwrap()
                    .model,
                COMMITTED_MODEL,
            );
            writer.bind_root_id(root, None).await.unwrap();
            assert_eq!(lock(&writer.session).session.parent(), Some(&parent));
            let other = AgentId::generate();
            assert!(matches!(
                writer.bind_root_id(other, None).await,
                Err(CheckpointError::Save { .. })
            ));
            assert!(matches!(
                SessionLogCheckpoint::open_with_root(
                    dir.clone(),
                    id,
                    MODEL,
                    CWD,
                    None,
                    other,
                    None
                ),
                Err(CheckpointError::Save { .. })
            ));
            assert_eq!(
                StoredSession::load(id, &dir).unwrap().root_agent_id(),
                Some(root)
            );
        });
    }

    #[test_case::test_case(true, true; "plan_guarded")]
    #[test_case::test_case(true, false; "plan_unguarded")]
    #[test_case::test_case(false, true; "custom_guarded")]
    #[test_case::test_case(false, false; "custom_unguarded")]
    fn first_config_checkpoint_migrates_the_resolved_mode(plan: bool, guarded: bool) {
        smol::block_on(async {
            let tmp = TempDir::new().unwrap();
            let dir = StateDir::from_path(tmp.path().to_path_buf());
            let id = MakimaId::generate();
            let mode = if plan {
                AgentMode::Plan(tmp.path().join("plan.md"))
            } else {
                AgentMode::Custom(ModeId::parse("review"))
            };
            let mut definition = ModeDef::default_for(mode.id());
            definition.system_prompt = Some(ORIGINAL.into());
            definition.tools = Some(vec!["read".into()]);
            let expected_mode = definition.stored_snapshot(&mode);
            let header = serde_json::json!({
                "t": "header", "v": 2, "id": id, "model": MODEL,
                "cwd": CWD, "created_at": 1
            });
            let meta = serde_json::json!({
                "t": "meta", "title": ORIGINAL, "token_usage": TokenUsage::default(), "updated_at": 1,
                "mode": if plan { serde_json::json!("plan") } else { serde_json::json!({"custom": "review"}) },
                "plan_path": mode.plan_path()
            });
            let path = dir
                .ensure_subdir(SESSIONS_DIR)
                .unwrap()
                .join(format!("{id}.jsonl"));
            let legacy = format!("{header}\n{meta}\n");
            std::fs::write(&path, &legacy).unwrap();
            let writer = SessionLogCheckpoint::open(dir.clone(), id, MODEL, CWD, None).unwrap();
            assert_eq!(std::fs::read_to_string(&path).unwrap(), legacy);
            assert!(
                lock(&writer.session)
                    .session
                    .latest_state()
                    .unwrap()
                    .scopes
                    .values()
                    .all(|scope| scope.config.is_none())
            );
            let options = SessionOptions::new(
                builtin_option_definitions(
                    MODEL,
                    [Arc::from(MODEL)],
                    false,
                    true,
                    true,
                    ThinkingConfig::Off,
                ),
                &BTreeMap::new(),
            )
            .unwrap();
            let mut change = request(id, 1, COMMITTED_MODEL, &options);
            let mut committed = projection(COMMITTED_MODEL, COMMITTED_CWD);
            let config = Arc::make_mut(&mut committed.config);
            config.mode = mode;
            config.mode_def = Some(definition);
            config.settings.fast = true;
            config.settings.workflow = true;
            Arc::make_mut(&mut change.snapshot).config = Some(committed.clone());
            writer.resolve_mode(&committed.config).await.unwrap();
            if guarded {
                writer
                    .checkpoint_guarded(
                        change,
                        CheckpointPublication::default(),
                        Arc::new(|publication| {
                            assert!(publication.try_publish());
                            Ok(())
                        }),
                    )
                    .await
                    .unwrap();
            } else {
                writer.checkpoint(change).await.unwrap();
            }
            let loaded = StoredSession::load(id, &dir).unwrap();
            let root = loaded.root_agent_id().unwrap();
            let view = loaded.latest_state().unwrap();
            let stored = view.scopes[&Some(root)].config.as_ref().unwrap();
            assert_eq!(stored.mode, expected_mode);
            assert_eq!(stored.model, COMMITTED_MODEL);
            assert_eq!(stored.cwd, COMMITTED_CWD);
            assert!(stored.fast);
            assert!(stored.workflow);
            let state = loaded.state().to_vec();
            writer.resolve_mode(&committed.config).await.unwrap();
            assert_eq!(lock(&writer.session).session.state(), state);
            let published = std::fs::read_to_string(path).unwrap();
            let header: serde_json::Value =
                serde_json::from_str(published.lines().next().unwrap()).unwrap();
            assert_eq!(header["v"], 3);
        });
    }

    #[test_case::test_case(false; "builtin")]
    #[test_case::test_case(true; "overridden")]
    fn prepared_mode_seeds_fresh_history_publication(overridden: bool) {
        smol::block_on(async {
            let tmp = TempDir::new().unwrap();
            let dir = StateDir::from_path(tmp.path().to_path_buf());
            let id = MakimaId::generate();
            let writer = SessionLogCheckpoint::open(dir.clone(), id, MODEL, CWD, None).unwrap();
            let mut committed = projection(COMMITTED_MODEL, COMMITTED_CWD);
            let mut definition = ModeDef::default_for(AgentMode::Build.id());
            if overridden {
                definition.system_prompt = Some(ORIGINAL.into());
                definition.restrict_write_to = Some(tmp.path().join("allowed"));
            }
            let expected = definition.stored_snapshot(&AgentMode::Build);
            let config = Arc::make_mut(&mut committed.config);
            config.mode_def = Some(definition);
            config.settings.fast = true;
            config.settings.workflow = true;
            config.settings.thinking = ThinkingConfig::Adaptive;
            let persistent_options = BTreeMap::from([(CATALOG_ID.into(), CATALOG_VALUE.into())]);
            lock(&writer.session).session.meta.session_options = persistent_options.clone();
            let expected_config =
                stored_agent_config(&committed.config, persistent_options).unwrap();
            writer.resolve_mode(&committed.config).await.unwrap();
            assert!(
                std::fs::read_dir(dir.path().join(SESSIONS_DIR))
                    .unwrap()
                    .next()
                    .is_none()
            );
            let options = SessionOptions::new(
                builtin_option_definitions(
                    MODEL,
                    [Arc::from(MODEL)],
                    false,
                    false,
                    false,
                    ThinkingConfig::Off,
                ),
                &BTreeMap::new(),
            )
            .unwrap();
            let mut history = request(id, 1, MODEL, &options);
            let snapshot = Arc::make_mut(&mut history.snapshot);
            snapshot.kind = SessionCheckpointKind::History;
            snapshot.history_revision = Some(1);
            snapshot.history = Some(HistorySnapshot::new(vec![Message::user(ORIGINAL.into())]));
            snapshot.config = None;
            writer.checkpoint(history).await.unwrap();
            let loaded = StoredSession::load(id, &dir).unwrap();
            let root = loaded.root_agent_id().unwrap();
            let state = loaded.latest_state().unwrap();
            let stored = state.scopes[&Some(root)].config.as_ref().unwrap();
            assert_eq!(stored.mode, expected);
            assert_eq!(stored, &expected_config);
            assert_eq!(loaded.messages().len(), 1);
        });
    }

    #[test]
    fn checkpoint_call_does_not_run_disk_save_on_the_executor() {
        smol::block_on(async {
            let tmp = TempDir::new().unwrap();
            let dir = StateDir::from_path(tmp.path().to_path_buf());
            let id = MakimaId::generate();
            let options = SessionOptions::new(
                builtin_option_definitions(
                    "test/model",
                    [Arc::from("test/model")],
                    false,
                    false,
                    false,
                    ThinkingConfig::Off,
                ),
                &BTreeMap::new(),
            )
            .unwrap();
            let writer = Arc::new(SessionLogCheckpoint::open(dir, id, MODEL, CWD, None).unwrap());
            let session_guard = lock(&writer.session);
            let (checkpoint_tx, checkpoint_rx) = flume::bounded(1);
            let checkpoint_writer = Arc::clone(&writer);
            let checkpoint_request = request(id, 1, MODEL, &options);
            let call = std::thread::spawn(move || {
                checkpoint_tx
                    .send(checkpoint_writer.checkpoint(checkpoint_request))
                    .unwrap();
            });

            let checkpoint = checkpoint_rx
                .recv_timeout(std::time::Duration::from_secs(1))
                .expect("checkpoint construction must not perform the save");
            drop(session_guard);
            call.join().unwrap();
            checkpoint.await.unwrap();
        });
    }

    #[test_case::test_case(false; "existing_child_metadata")]
    #[test_case::test_case(true; "new_child_metadata")]
    fn child_completion_waits_for_serialized_publication(new_child: bool) {
        smol::block_on(async {
            let tmp = TempDir::new().unwrap();
            let dir = StateDir::from_path(tmp.path().to_path_buf());
            let id = MakimaId::generate();
            let mut initial = child_session(id);
            if new_child {
                initial.set_subagents(vec![child(SIBLING_KEY)]);
            }
            initial.save(&dir).unwrap();
            let writer = SessionLogCheckpoint::open(dir.clone(), id, MODEL, CWD, None).unwrap();
            let sibling_writer = writer.clone();
            let history = HistorySnapshot::new(vec![Message::user(TERMINAL.into())]);
            let sibling_history = HistorySnapshot::new(vec![Message::user(COMMITTED_MODEL.into())]);
            let publication = CheckpointPublication::default();
            let mut configured_child = child(CHILD_KEY);
            configured_child.name = TERMINAL.into();
            configured_child.model = Some(COMMITTED_MODEL.into());
            configured_child.thinking = Some(ThinkingConfig::Adaptive.into());
            configured_child.fast = true;
            let (root_history, child_history, sibling_before, posts, history_version) = {
                let retained = lock(&writer.session);
                (
                    retained.session.history_snapshot(),
                    retained
                        .session
                        .subagent_history_snapshot(CHILD_KEY)
                        .unwrap(),
                    retained
                        .session
                        .subagent_history_snapshot(SIBLING_KEY)
                        .unwrap(),
                    retained.session.posts().to_vec(),
                    retained.history_version,
                )
            };
            writer
                .checkpoint_child_config(
                    configured_child.clone(),
                    None,
                    CheckpointPublication::default(),
                    Arc::new(|publication| {
                        assert!(publication.try_publish());
                        Ok(())
                    }),
                )
                .await
                .unwrap();
            {
                let retained = lock(&writer.session);
                assert_eq!(retained.history_version, history_version);
                assert_eq!(retained.session.posts(), posts);
                for (before, after) in [
                    (root_history, retained.session.history_snapshot()),
                    (
                        child_history,
                        retained
                            .session
                            .subagent_history_snapshot(CHILD_KEY)
                            .unwrap(),
                    ),
                    (
                        sibling_before,
                        retained
                            .session
                            .subagent_history_snapshot(SIBLING_KEY)
                            .unwrap(),
                    ),
                ] {
                    assert_eq!(before.epoch, after.epoch);
                    assert!(Arc::ptr_eq(&before.messages, &after.messages));
                }
                let mut metadata_only = initial.clone();
                upsert_child(&mut metadata_only, configured_child.clone());
                metadata_only.updated_at = retained.session.updated_at;
                assert_eq!(
                    stored_value(&StoredSession::load(id, &dir).unwrap()),
                    stored_value(&metadata_only)
                );
            }
            let (completed, sibling) = {
                let session_guard = lock(&writer.session);
                let mut config = Box::pin(writer.checkpoint_child_config(
                    configured_child.clone(),
                    None,
                    publication.clone(),
                    Arc::new(|publication| {
                        assert!(publication.try_publish());
                        Ok(())
                    }),
                ));
                let mut completed =
                    Box::pin(writer.checkpoint_child(child(CHILD_KEY), history.clone()));
                let mut sibling = Box::pin(
                    sibling_writer.checkpoint_child(child(SIBLING_KEY), sibling_history.clone()),
                );
                let mut context = Context::from_waker(Waker::noop());
                assert!(config.as_mut().poll(&mut context).is_pending());
                assert!(completed.as_mut().poll(&mut context).is_pending());
                assert!(sibling.as_mut().poll(&mut context).is_pending());
                assert_eq!(
                    StoredSession::load(id, &dir).unwrap().subagent_messages()[CHILD_KEY][0]
                        .first_text_content(),
                    Some(ORIGINAL)
                );
                let mut expected = configured_child.clone();
                expected.model = (!new_child).then(|| MODEL.into());
                expected.thinking = (!new_child).then(|| ThinkingConfig::Off.into());
                expected.fast = false;
                assert!(session_guard.session.subagents().contains(&expected));
                drop(config);
                (completed, sibling)
            };
            let (completed, sibling) = futures_lite::future::zip(completed, sibling).await;
            assert!(!publication.cancel());
            completed.unwrap();
            sibling.unwrap();
            configured_child.model = (!new_child).then(|| MODEL.into());
            configured_child.thinking = (!new_child).then(|| ThinkingConfig::Off.into());
            configured_child.fast = false;
            for stale in [
                HistorySnapshot {
                    epoch: history.epoch - 1,
                    messages: Arc::new(vec![Message::user(REJECTED.into())]),
                    ..history.clone()
                },
                HistorySnapshot {
                    messages: Arc::new(Vec::new()),
                    ..history.clone()
                },
            ] {
                assert!(
                    matches!(writer.checkpoint_child(child(CHILD_KEY), stale).await, Err(CheckpointError::Save { message, .. }) if message.as_ref() == HISTORY_SUPERSEDED)
                );
            }
            let loaded = StoredSession::load(id, &dir).unwrap();
            {
                let retained = lock(&writer.session);
                for saved in [&loaded, &retained.session] {
                    assert_eq!(saved.messages()[0].first_text_content(), Some(ORIGINAL));
                    assert_eq!(saved.model, MODEL);
                    assert_eq!(saved.cwd, CWD);
                    let expected = if new_child {
                        vec![child(SIBLING_KEY), configured_child.clone()]
                    } else {
                        vec![configured_child.clone(), child(SIBLING_KEY)]
                    };
                    assert_eq!(saved.subagents(), expected);
                    assert_eq!(
                        saved.subagent_messages()[CHILD_KEY][0].first_text_content(),
                        Some(TERMINAL)
                    );
                    assert_eq!(
                        saved.subagent_messages()[SIBLING_KEY][0].first_text_content(),
                        Some(COMMITTED_MODEL)
                    );
                    assert_eq!(saved.posts().len(), 1);
                    assert!(saved.posts()[0].chat.is_none());
                }
                assert!(retained.history_version.is_none());
                assert!(Arc::ptr_eq(
                    &retained
                        .session
                        .subagent_history_snapshot(CHILD_KEY)
                        .unwrap()
                        .messages,
                    &history.messages
                ));
                assert!(Arc::ptr_eq(
                    &retained
                        .session
                        .subagent_history_snapshot(SIBLING_KEY)
                        .unwrap()
                        .messages,
                    &sibling_history.messages
                ));
            }
        });
    }

    #[test_case::test_case(false, false; "ordinary_history_then_config")]
    #[test_case::test_case(false, true; "ordinary_config_then_history")]
    #[test_case::test_case(true, false; "guarded_history_then_config")]
    #[test_case::test_case(true, true; "guarded_config_then_history")]
    fn concurrent_checkpoints_preserve_submission_order(guarded: bool, config_first: bool) {
        smol::block_on(async {
            let tmp = TempDir::new().unwrap();
            let dir = StateDir::from_path(tmp.path().to_path_buf());
            let id = MakimaId::generate();
            let options = SessionOptions::new(
                builtin_option_definitions(
                    "test/model",
                    [Arc::from("test/model")],
                    false,
                    false,
                    false,
                    ThinkingConfig::Off,
                ),
                &BTreeMap::new(),
            )
            .unwrap();
            child_session(id).save(&dir).unwrap();
            let owner = config_first.then(|| {
                session_lock::claim(&dir.ensure_subdir(SESSIONS_DIR).unwrap(), &id)
                    .unwrap()
                    .unwrap()
            });
            let writer = SessionLogCheckpoint::open(
                dir.clone(),
                id,
                MODEL,
                CWD,
                owner.as_ref().map(|owner| owner.publication_guard()),
            )
            .unwrap();
            let publication = CheckpointPublication::default();
            let claim: CheckpointClaim = Arc::new(|publication| {
                publication
                    .try_publish()
                    .then_some(())
                    .ok_or_else(|| Arc::from(CHECKPOINT_CANCELLED))
            });
            let child_history = HistorySnapshot::new(vec![Message::user(TERMINAL.into())]);
            let mut completed_child = child(CHILD_KEY);
            completed_child.model = Some(COMMITTED_MODEL.into());
            completed_child.thinking = Some(ThinkingConfig::Adaptive.into());
            completed_child.fast = true;
            writer
                .checkpoint_child(completed_child.clone(), child_history.clone())
                .await
                .unwrap();
            completed_child.name = TERMINAL.into();
            writer
                .checkpoint_child_config(
                    completed_child.clone(),
                    None,
                    CheckpointPublication::default(),
                    Arc::clone(&claim),
                )
                .await
                .unwrap();
            writer
                .checkpoint_child(child(CHILD_KEY), child_history)
                .await
                .unwrap();
            completed_child.model = Some(MODEL.into());
            completed_child.thinking = Some(ThinkingConfig::Off.into());
            completed_child.fast = false;
            assert_eq!(
                StoredSession::load(id, &dir).unwrap().subagents(),
                &[completed_child.clone(), child(SIBLING_KEY)]
            );
            let mut history = HistorySnapshot::new(vec![Message::user(TERMINAL.into())]);
            history.compacted = true;
            let mut config = request(id, u64::from(!config_first) + 1, COMMITTED_MODEL, &options);
            let candidate = Arc::make_mut(&mut config.snapshot);
            candidate.config = Some(projection(COMMITTED_MODEL, COMMITTED_CWD));
            for option in Arc::make_mut(&mut candidate.options.options) {
                if matches!(
                    option.definition.id.as_ref(),
                    FAST_OPTION_ID | WORKFLOW_OPTION_ID
                ) {
                    option.current_value = Arc::from(ENABLED_VALUE);
                }
            }
            let mut terminal = request(id, u64::from(config_first) + 1, MODEL, &options);
            let candidate = Arc::make_mut(&mut terminal.snapshot);
            candidate.kind = SessionCheckpointKind::History;
            candidate.history_revision = Some(1);
            candidate.history = Some(history.clone());
            let mut config = Some(config);
            let mut submit_config = || {
                let request = config.take().unwrap();
                if guarded {
                    writer.checkpoint_guarded(request, publication.clone(), Arc::clone(&claim))
                } else {
                    writer.checkpoint(request)
                }
            };
            let mut terminal = Some(terminal);
            let first = if config_first {
                submit_config()
            } else {
                writer.checkpoint(terminal.take().unwrap())
            };
            let mut child_config = Box::pin(writer.checkpoint_child_config(
                completed_child.clone(),
                None,
                CheckpointPublication::default(),
                Arc::clone(&claim),
            ));
            let child_config_result = child_config
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()));
            let second = if config_first {
                writer.checkpoint(terminal.take().unwrap())
            } else {
                submit_config()
            };
            let (first, second) = futures_lite::future::zip(first, second).await;
            match child_config_result {
                Poll::Ready(result) => result,
                Poll::Pending => child_config.await,
            }
            .unwrap();
            for (ack, revision) in [(first.unwrap(), 1), (second.unwrap(), 2)] {
                assert_eq!(ack.session_id, id);
                assert_eq!(ack.version, CheckpointVersion { revision, epoch: 1 });
            }
            if guarded {
                assert!(!publication.cancel());
                assert_eq!(
                    writer
                        .checkpoint_guarded(request(id, 3, MODEL, &options), publication, claim)
                        .await
                        .unwrap_err(),
                    CheckpointError::Save {
                        session_id: id,
                        message: Arc::from(CHECKPOINT_CANCELLED)
                    }
                );
            }
            writer.drain().await.unwrap();
            let loaded: StoredSession = StoredSession::load(id, &dir).unwrap();
            {
                let retained = lock(&writer.session);
                for saved in [&loaded, &retained.session] {
                    assert_eq!(saved.model, MODEL);
                    assert_eq!(saved.cwd, CWD);
                    assert!(!saved.meta.fast);
                    assert!(!saved.meta.workflow);
                    assert!(saved.meta.compacted);
                    assert_eq!(saved.messages()[0].first_text_content(), Some(TERMINAL));
                    assert_eq!(
                        saved.subagents(),
                        &[completed_child.clone(), child(SIBLING_KEY)]
                    );
                    assert_eq!(
                        saved.subagent_messages()[CHILD_KEY][0].first_text_content(),
                        Some(TERMINAL)
                    );
                    assert_eq!(saved.subagent_messages()[SIBLING_KEY].len(), 2);
                    assert_eq!(
                        saved
                            .posts()
                            .iter()
                            .map(|post| post.chat.as_deref())
                            .collect::<Vec<_>>(),
                        [None, Some(SIBLING_KEY)]
                    );
                }
                assert_eq!(retained.session.history_snapshot().epoch, history.epoch);
                assert!(Arc::ptr_eq(
                    &retained.session.history_snapshot().messages,
                    &history.messages
                ));
                assert_eq!(retained.history_version, Some((history.epoch, 1)));
            }
            let mut stale = request(id, 3, MODEL, &options);
            let candidate = Arc::make_mut(&mut stale.snapshot);
            candidate.kind = SessionCheckpointKind::History;
            candidate.history_revision = Some(0);
            candidate.history = Some(HistorySnapshot {
                messages: Arc::new(vec![Message::user(REJECTED.into())]),
                ..history
            });
            assert!(
                matches!(writer.checkpoint(stale).await, Err(CheckpointError::Save { message, .. }) if message.as_ref() == HISTORY_SUPERSEDED)
            );
            assert_eq!(
                StoredSession::load(id, &dir).unwrap().messages()[0].first_text_content(),
                Some(TERMINAL)
            );
            if let Some(owner) = owner {
                owner.release().unwrap();
            }
        });
    }

    #[test]
    fn failed_post_rename_save_preserves_live_history_namespace_for_retry() {
        const CWD_INDEX: &str = "cwd_latest.json";
        let temporary = TempDir::new().unwrap();
        let dir = StateDir::from_path(temporary.path().to_path_buf());
        let root = AgentId::generate();
        let mut session = child_session(MakimaId::generate());
        session.bind_root_id(root).unwrap();
        let history = session.history_snapshot();
        let child_history = session.subagent_history_snapshot(CHILD_KEY).unwrap();
        let boundary_seq = session.next_state_seq();
        session.push_state(makima_storage::session_state::StoredStateRecord {
            seq: boundary_seq,
            agent: Some(root),
            generation: 0,
            change: StateChange::Boundary {
                offset: history.messages.len(),
                lens: [(root, (history.epoch, history.messages.len()))].into(),
            },
        });
        session.save(&dir).unwrap();
        let id = session.id;
        let journal = SessionJournal::from_session(&session).unwrap();
        let retained = Mutex::new(RetainedSession {
            session,
            history_version: Some((history.epoch, 1)),
        });
        let before = lock(&retained).clone();
        let index = dir.path().join(SESSIONS_DIR).join(CWD_INDEX);
        std::fs::remove_file(&index).unwrap();
        std::fs::create_dir(&index).unwrap();
        let options = SessionOptions::new(Vec::new(), &BTreeMap::new()).unwrap();
        let failed = request(id, 2, MODEL, &options);
        assert!(matches!(
            SessionLogCheckpoint::save(&dir, &retained, failed, None, None),
            Err(CheckpointError::Save { .. })
        ));
        assert!(StoredSession::load(id, &dir).is_ok());
        {
            let retained = lock(&retained);
            assert!(history_matches(
                &retained.session.history_snapshot(),
                &history
            ));
            assert!(history_matches(
                &retained
                    .session
                    .subagent_history_snapshot(CHILD_KEY)
                    .unwrap(),
                &child_history,
            ));
            assert_eq!(retained.session.state(), before.session.state());
            assert_eq!(retained.history_version, before.history_version);
        }
        std::fs::remove_dir(&index).unwrap();
        let mut retry = request(id, 3, MODEL, &options);
        let checkpoint = Arc::make_mut(&mut retry.snapshot);
        checkpoint.kind = SessionCheckpointKind::History;
        checkpoint.history = Some(history.clone());
        checkpoint.history_revision = Some(2);
        SessionLogCheckpoint::save(&dir, &retained, retry, None, Some(&journal.snapshot()))
            .unwrap();
        let retained = lock(&retained);
        assert!(history_matches(
            &retained.session.history_snapshot(),
            &history
        ));
        assert!(history_matches(
            &retained
                .session
                .subagent_history_snapshot(CHILD_KEY)
                .unwrap(),
            &child_history,
        ));
        assert_eq!(retained.history_version, Some((history.epoch, 2)));
        assert_eq!(
            StoredSession::load(id, &dir).unwrap().messages(),
            history.messages.as_ref()
        );
        assert!(
            journal
                .preview_rewind(crate::rewind::RewindBoundary {
                    agent: root,
                    seq: boundary_seq,
                    epoch: journal.snapshot().epoch,
                })
                .is_ok()
        );
    }

    #[test_case::test_case(false, false, false; "ordinary_io_error")]
    #[test_case::test_case(true, false, false; "guarded_io_error")]
    #[test_case::test_case(true, true, false; "rejected_claim")]
    #[test_case::test_case(true, true, true; "claimed_error")]
    fn failed_candidate_is_not_retained_for_later_checkpoint(
        guarded: bool,
        claim_error: bool,
        claimed: bool,
    ) {
        let tmp = TempDir::new().unwrap();
        let dir = StateDir::from_path(tmp.path().to_path_buf());
        let id = MakimaId::generate();
        let options = SessionOptions::new(
            builtin_option_definitions(
                "test/model",
                [Arc::from("test/model")],
                false,
                false,
                false,
                ThinkingConfig::Off,
            ),
            &BTreeMap::new(),
        )
        .unwrap();
        child_session(id).save(&dir).unwrap();
        let writer = SessionLogCheckpoint::open(dir.clone(), id, MODEL, CWD, None).unwrap();
        let original = Message::user(ORIGINAL.into());
        let history = HistorySnapshot::new(vec![original.clone()]);
        let mut accepted = request(id, 1, MODEL, &options);
        let candidate = Arc::make_mut(&mut accepted.snapshot);
        candidate.kind = SessionCheckpointKind::History;
        candidate.history = Some(history.clone());
        candidate.history_revision = Some(1);
        smol::block_on(writer.checkpoint(accepted)).unwrap();
        let sessions = dir.ensure_subdir(SESSIONS_DIR).unwrap();
        let path = sessions.join(format!("{id}.jsonl"));
        if !claim_error {
            std::fs::remove_file(&path).unwrap();
            std::fs::create_dir(&path).unwrap();
        }
        let mut failed = request(id, 2, COMMITTED_MODEL, &options);
        let rejected = Arc::make_mut(&mut failed.snapshot);
        rejected.history = Some(HistorySnapshot {
            messages: Arc::new(vec![Message::user(REJECTED.into())]),
            ..history.clone()
        });
        rejected.history_revision = Some(2);

        let publication = CheckpointPublication::default();
        let failed = if guarded {
            writer.checkpoint_guarded(
                failed,
                publication.clone(),
                Arc::new(move |publication| {
                    if !claim_error || claimed {
                        assert!(publication.try_publish());
                    }
                    if claim_error {
                        Err(Arc::from(CLAIM_ERROR))
                    } else {
                        Ok(())
                    }
                }),
            )
        } else {
            writer.checkpoint(failed)
        };
        let error = smol::block_on(failed).unwrap_err();
        let mut rejected_child = child(CHILD_KEY);
        rejected_child.name = REJECTED.into();
        rejected_child.model = Some(COMMITTED_MODEL.into());
        rejected_child.thinking = Some(ThinkingConfig::Adaptive.into());
        rejected_child.fast = true;
        let child_publication = CheckpointPublication::default();
        let child_error = smol::block_on(writer.checkpoint_child_config(
            rejected_child.clone(),
            None,
            child_publication.clone(),
            Arc::new(move |publication| {
                if !claim_error || claimed {
                    assert!(publication.try_publish());
                }
                if claim_error {
                    Err(Arc::from(CLAIM_ERROR))
                } else {
                    Ok(())
                }
            }),
        ))
        .unwrap_err();
        if claim_error {
            assert_eq!(
                child_error,
                CheckpointError::Save {
                    session_id: id,
                    message: Arc::from(CLAIM_ERROR),
                }
            );
        }
        assert_eq!(child_publication.is_cancelled(), claim_error && !claimed);
        if claim_error {
            assert_eq!(
                error,
                CheckpointError::Save {
                    session_id: id,
                    message: Arc::from(CLAIM_ERROR)
                }
            );
            assert_eq!(publication.is_cancelled(), !claimed);
            assert_eq!(
                StoredSession::load(id, &dir).unwrap().messages()[0].first_text_content(),
                Some(ORIGINAL)
            );
            std::fs::remove_file(&path).unwrap();
            std::fs::create_dir(&path).unwrap();
        }
        assert!(matches!(
            smol::block_on(writer.checkpoint_child(
                rejected_child,
                HistorySnapshot::new(vec![Message::user(REJECTED.into())]),
            )),
            Err(CheckpointError::Save { session_id, .. }) if session_id == id
        ));
        {
            let retained = lock(&writer.session);
            assert_eq!(
                retained.session.subagents(),
                &[child(CHILD_KEY), child(SIBLING_KEY)]
            );
            assert_eq!(retained.session.subagent_messages()[CHILD_KEY].len(), 2);
            assert_eq!(retained.session.posts().len(), 3);
            assert_eq!(retained.session.model, MODEL);
            assert_eq!(
                retained.session.messages()[0].first_text_content(),
                Some(ORIGINAL)
            );
            assert_eq!(retained.history_version, Some((history.epoch, 1)));
        }
        std::fs::remove_dir_all(&path).unwrap();
        smol::block_on(writer.checkpoint(request(id, 3, COMMITTED_MODEL, &options))).unwrap();

        let loaded: StoredSession = StoredSession::load(id, &dir).unwrap();
        assert_eq!(loaded.model, MODEL);
        assert_eq!(loaded.subagents(), &[child(CHILD_KEY), child(SIBLING_KEY)]);
        assert_eq!(loaded.subagent_messages()[CHILD_KEY].len(), 2);
        assert_eq!(loaded.subagent_messages()[SIBLING_KEY].len(), 2);
        assert_eq!(loaded.posts().len(), 3);
        assert_eq!(
            serde_json::to_value(loaded.messages()).unwrap(),
            serde_json::to_value([original]).unwrap()
        );
    }

    #[test_case::test_case(false; "stale_owner")]
    #[test_case::test_case(true; "cancelled_before_claim")]
    fn stale_owner_cannot_publish_after_reclaim(cancelled: bool) {
        let tmp = TempDir::new().unwrap();
        let dir = StateDir::from_path(tmp.path().to_path_buf());
        let sessions = dir
            .ensure_subdir(makima_storage::sessions::SESSIONS_DIR)
            .unwrap();
        let id = MakimaId::generate();
        let old = session_lock::claim(&sessions, &id).unwrap().unwrap();
        let writer =
            SessionLogCheckpoint::open(dir.clone(), id, MODEL, CWD, Some(old.publication_guard()))
                .unwrap();
        let owner = if cancelled {
            old
        } else {
            old.release().unwrap();
            session_lock::claim(&sessions, &id).unwrap().unwrap()
        };
        let options = SessionOptions::new(
            builtin_option_definitions(
                MODEL,
                [Arc::from(MODEL)],
                false,
                false,
                false,
                ThinkingConfig::Off,
            ),
            &BTreeMap::new(),
        )
        .unwrap();
        let publication = CheckpointPublication::default();
        if cancelled {
            assert!(publication.cancel());
        }
        let checkpoint = writer.checkpoint_guarded(
            request(id, 1, COMMITTED_MODEL, &options),
            publication.clone(),
            Arc::new(|_| panic!("stale publication claim ran")),
        );
        assert!(
            matches!(smol::block_on(checkpoint), Err(CheckpointError::Save { message, .. }) if message.as_ref() == CHECKPOINT_CANCELLED)
        );
        assert!(publication.is_cancelled());
        let child_publication = CheckpointPublication::default();
        if cancelled {
            assert!(child_publication.cancel());
        }
        assert!(matches!(
            smol::block_on(writer.checkpoint_child_config(
                child(CHILD_KEY),
                None,
                child_publication.clone(),
                Arc::new(|_| panic!("stale child configuration claim ran")),
            )),
            Err(CheckpointError::Save { message, .. }) if message.as_ref() == CHECKPOINT_CANCELLED
        ));
        assert!(child_publication.is_cancelled());
        assert_eq!(lock(&writer.session).session.model, MODEL);
        if !cancelled {
            assert!(matches!(
                smol::block_on(writer.checkpoint_child(
                    child(CHILD_KEY),
                    HistorySnapshot::new(vec![Message::user(REJECTED.into())]),
                )),
                Err(CheckpointError::Closed(session_id)) if session_id == id
            ));
        }
        assert!(lock(&writer.session).session.subagents().is_empty());
        assert!(matches!(
            StoredSession::load(id, &dir),
            Err(SessionError::Storage(StorageError::NotFound(_)))
        ));
        owner.release().unwrap();
    }

    #[test]
    fn checkpoint_errors_survive_unblock() {
        smol::block_on(async {
            let tmp = TempDir::new().unwrap();
            let state_path = tmp.path().join("not-a-directory");
            std::fs::write(&state_path, "file").unwrap();
            let dir = StateDir::from_path(state_path);
            let id = MakimaId::generate();
            assert!(matches!(
                SessionLogCheckpoint::open(dir, id, MODEL, CWD, None),
                Err(CheckpointError::Save { session_id, .. }) if session_id == id
            ));
        });
    }

    #[test]
    fn corrupt_session_is_rejected_without_overwrite() {
        let tmp = TempDir::new().unwrap();
        let dir = StateDir::from_path(tmp.path().to_path_buf());
        let id = MakimaId::generate();
        let mut session = StoredSession::new("test/model", "/project");
        session.id = id;
        session.save(&dir).unwrap();
        let sessions = dir
            .ensure_subdir(makima_storage::sessions::SESSIONS_DIR)
            .unwrap();
        let path = std::fs::read_dir(sessions)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .find(|path| {
                path.extension()
                    .is_some_and(|extension| extension == "jsonl")
            })
            .unwrap();
        const CORRUPT: &[u8] = b"not json\n";
        std::fs::write(&path, CORRUPT).unwrap();

        assert!(matches!(
            SessionLogCheckpoint::open(dir, id, MODEL, CWD, None),
            Err(CheckpointError::Save { session_id, .. }) if session_id == id
        ));
        assert_eq!(std::fs::read(path).unwrap(), CORRUPT);
    }

    #[test_case::test_case(false; "existing_child")]
    #[test_case::test_case(true; "new_child")]
    fn child_checkpoint_payload_does_not_author_configuration(new_child: bool) {
        let mut session = child_session(MakimaId::generate());
        if new_child {
            session.set_subagents(vec![child(SIBLING_KEY)]);
        }
        let mut incoming = child(CHILD_KEY);
        incoming.model = Some(COMMITTED_MODEL.into());
        incoming.thinking = Some(ThinkingConfig::Adaptive.into());
        incoming.fast = true;
        incoming.name = TERMINAL.into();
        upsert_child(&mut session, incoming);
        let saved = session
            .subagents()
            .iter()
            .find(|child| child.tool_use_id == CHILD_KEY)
            .unwrap();
        assert_eq!(saved.name, TERMINAL);
        assert_eq!(saved.model.as_deref(), (!new_child).then_some(MODEL));
        assert_eq!(
            saved.thinking,
            (!new_child).then(|| ThinkingConfig::Off.into())
        );
        assert!(!saved.fast);
    }

    #[test]
    fn config_conversion_requires_resolved_mode() {
        let mut config = projection(MODEL, CWD).config.as_ref().clone();
        config.mode_def = None;
        assert!(matches!(
            stored_agent_config(&config, BTreeMap::new()),
            Err(ConfigChangeError::Rejected(message)) if message.as_ref() == CONFIG_MODE_UNRESOLVED
        ));
    }

    #[test_case::test_case(false; "ordinary")]
    #[test_case::test_case(true; "guarded")]
    fn journal_config_is_authoritative_over_checkpoint_payload(guarded: bool) {
        smol::block_on(async {
            let tmp = TempDir::new().unwrap();
            let dir = StateDir::from_path(tmp.path().to_path_buf());
            let mut session = StoredSession::new(MODEL, CWD);
            let root = AgentId::generate();
            session.bind_root_id(root).unwrap();
            let id = session.id;
            let writer =
                SessionLogCheckpoint::open_with_session(dir.clone(), session, None).unwrap();
            let journal = writer.create_journal().await.unwrap();
            let mut config = projection(COMMITTED_MODEL, COMMITTED_CWD)
                .config
                .as_ref()
                .clone();
            config.settings.fast = true;
            config.settings.workflow = true;
            config.settings.thinking = ThinkingConfig::Adaptive;
            let stored = stored_agent_config(
                &config,
                BTreeMap::from([(CATALOG_ID.into(), CATALOG_VALUE.into())]),
            )
            .unwrap();
            journal
                .commit(JournalCommit {
                    changes: vec![JournalChange {
                        agent: Some(root),
                        generation: 1,
                        change: StateChange::Config(stored.clone()),
                    }],
                    ..Default::default()
                })
                .unwrap();
            let options = SessionOptions::new(Vec::new(), &BTreeMap::new()).unwrap();
            let change = request(id, 1, MODEL, &options);
            if guarded {
                writer
                    .checkpoint_guarded(
                        change,
                        CheckpointPublication::default(),
                        Arc::new(|publication| {
                            assert!(publication.try_publish());
                            Ok(())
                        }),
                    )
                    .await
                    .unwrap();
            } else {
                writer.checkpoint(change).await.unwrap();
            }
            let loaded = StoredSession::load(id, &dir).unwrap();
            assert_eq!(
                loaded.latest_state().unwrap().scopes[&Some(root)]
                    .config
                    .as_ref(),
                Some(&stored)
            );
            assert_eq!(journal.published_revision(), journal.snapshot().revision);
            assert_eq!(
                lock(&writer.session).session.latest_state().unwrap().scopes[&Some(root)]
                    .config
                    .as_ref(),
                Some(&stored)
            );
        });
    }

    #[test]
    fn checkpoint_payload_does_not_author_config_metadata() {
        smol::block_on(async {
            let tmp = TempDir::new().unwrap();
            let dir = StateDir::from_path(tmp.path().to_path_buf());
            let id = MakimaId::generate();
            let options = SessionOptions::new(
                builtin_option_definitions(
                    "test/model",
                    [Arc::from("test/model")],
                    true,
                    true,
                    true,
                    ThinkingConfig::Effort(makima_providers::Effort::High),
                ),
                &BTreeMap::new(),
            )
            .unwrap();
            let writer = SessionLogCheckpoint::open(dir.clone(), id, MODEL, CWD, None).unwrap();

            writer
                .checkpoint(CheckpointRequest {
                    session_id: id,
                    version: CheckpointVersion {
                        revision: 1,
                        epoch: 1,
                    },
                    snapshot: Arc::new(SessionCheckpoint {
                        kind: SessionCheckpointKind::Config,
                        history_revision: Some(1),
                        history: Some(HistorySnapshot::new(vec![Message::user("hello".into())])),
                        options: options.snapshot(),
                        config: Some(projection(MODEL, CWD)),
                        journal: None,
                    }),
                })
                .await
                .unwrap();

            let loaded: StoredSession = StoredSession::load(id, &dir).unwrap();
            assert_eq!(loaded.meta.yolo, None);
            assert!(!loaded.meta.fast);
            assert!(!loaded.meta.workflow);
            assert_eq!(loaded.meta.thinking, None);
            assert_eq!(loaded.messages().len(), 1);
            assert_eq!(loaded.model, MODEL);
            assert_eq!(loaded.cwd, CWD);
            let acknowledged = lock(&writer.session).session.history_snapshot();
            let mut catalog = request(id, 2, COMMITTED_MODEL, &options);
            let candidate = Arc::make_mut(&mut catalog.snapshot);
            candidate.kind = SessionCheckpointKind::Options;
            candidate.config = Some(projection(COMMITTED_MODEL, COMMITTED_CWD));
            candidate.history = Some(HistorySnapshot::new(vec![Message::user(REJECTED.into())]));
            for option in Arc::make_mut(&mut candidate.options.options) {
                option.current_value = Arc::from(DISABLED_VALUE);
            }
            let mut entries = candidate.options.options.to_vec();
            let mut persistent = entries[0].clone();
            persistent.definition.id = Arc::from(CATALOG_ID);
            persistent.definition.owner = SessionOptionOwner::Plugin {
                plugin: Arc::from(CATALOG_PLUGIN),
                generation: 1,
            };
            persistent.definition.persistent = true;
            persistent.current_value = Arc::from(CATALOG_VALUE);
            entries.push(persistent);
            candidate.options.options = entries.into();
            writer.checkpoint(catalog).await.unwrap();
            let catalog = StoredSession::load(id, &dir).unwrap();
            assert_eq!(catalog.model, loaded.model);
            assert_eq!(catalog.cwd, loaded.cwd);
            assert_eq!(catalog.meta.fast, loaded.meta.fast);
            assert_eq!(catalog.meta.workflow, loaded.meta.workflow);
            assert_eq!(catalog.meta.thinking, loaded.meta.thinking);
            assert_eq!(catalog.meta.yolo, None);
            assert!(catalog.meta.session_options.is_empty());
            assert!(
                catalog
                    .latest_state()
                    .unwrap()
                    .scopes
                    .values()
                    .all(|scope| scope.config.is_none())
            );
            assert_eq!(
                serde_json::to_value(catalog.messages()).unwrap(),
                serde_json::to_value(loaded.messages()).unwrap()
            );
            let retained = lock(&writer.session);
            assert_eq!(
                retained.session.history_snapshot().epoch,
                acknowledged.epoch
            );
            assert!(Arc::ptr_eq(
                &retained.session.history_snapshot().messages,
                &acknowledged.messages
            ));
        });
    }
}
