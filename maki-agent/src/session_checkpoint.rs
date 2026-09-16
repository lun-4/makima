use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use maki_providers::{Message, ThinkingConfig, TokenUsage};
use maki_storage::StateDir;
use maki_storage::checkpoint::{
    CheckpointAck, CheckpointError, CheckpointFuture, CheckpointRequest, CheckpointWriter,
};
use maki_storage::sessions::{Session, SessionMeta};

use crate::ToolOutput;
use crate::session_coordinator::SessionCheckpoint;
use crate::session_options::{
    ENABLED_VALUE, FAST_OPTION_ID, THINKING_OPTION_ID, WORKFLOW_OPTION_ID, YOLO_OPTION_ID,
};

type StoredSession = Session<Message, TokenUsage, ToolOutput>;

struct SaveJob {
    request: CheckpointRequest<SessionCheckpoint>,
    reply: flume::Sender<Result<CheckpointAck, CheckpointError>>,
}

pub struct SessionLogCheckpoint {
    save_tx: flume::Sender<SaveJob>,
    #[cfg(test)]
    session: Arc<Mutex<StoredSession>>,
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

impl SessionLogCheckpoint {
    pub fn open(
        dir: StateDir,
        session_id: maki_storage::id::MakiId,
        model: &str,
        cwd: &str,
    ) -> Self {
        let session = StoredSession::load(session_id, &dir).unwrap_or_else(|_| {
            let mut session = StoredSession::new(model, cwd);
            session.id = session_id;
            session
        });
        let session = Arc::new(Mutex::new(session));
        let (save_tx, save_rx) = flume::unbounded::<SaveJob>();
        let worker_session = Arc::clone(&session);
        smol::spawn(async move {
            while let Ok(job) = save_rx.recv_async().await {
                let dir = dir.clone();
                let session = Arc::clone(&worker_session);
                let result = smol::unblock(move || Self::save(&dir, &session, job.request)).await;
                let _ = job.reply.send(result);
            }
        })
        .detach();
        Self {
            save_tx,
            #[cfg(test)]
            session,
        }
    }

    pub fn resolve(
        session_id: maki_storage::id::MakiId,
        model: &str,
        cwd: &str,
    ) -> Result<Self, CheckpointError> {
        let dir = StateDir::resolve().map_err(|error| CheckpointError::Save {
            session_id,
            message: Arc::from(error.to_string()),
        })?;
        Ok(Self::open(dir, session_id, model, cwd))
    }

    fn save(
        dir: &StateDir,
        session: &Mutex<StoredSession>,
        request: CheckpointRequest<SessionCheckpoint>,
    ) -> Result<CheckpointAck, CheckpointError> {
        let mut session = lock(session);
        let checkpoint = &request.snapshot;
        // Only a history replacement carries messages; an option or model
        // change leaves the stored ones alone rather than rewinding them to
        // the coordinator's pre-turn copy.
        if let Some(history) = &checkpoint.history {
            session.replace_messages(history.as_ref().clone());
        }
        session.set_model(checkpoint.model.to_string());
        session.set_cwd(checkpoint.cwd.to_string_lossy().into_owned());
        session.update_title_if_default();
        session.meta = checkpoint_meta(&session.meta, checkpoint);
        session.save(dir).map_err(|error| CheckpointError::Save {
            session_id: request.session_id,
            message: Arc::from(error.to_string()),
        })?;
        Ok(CheckpointAck {
            session_id: request.session_id,
            version: request.version,
        })
    }
}

impl CheckpointWriter<SessionCheckpoint> for SessionLogCheckpoint {
    fn checkpoint(&self, request: CheckpointRequest<SessionCheckpoint>) -> CheckpointFuture {
        let session_id = request.session_id;
        let (reply, response) = flume::bounded(1);
        if self.save_tx.send(SaveJob { request, reply }).is_err() {
            return Box::pin(async move { Err(CheckpointError::Closed(session_id)) });
        }
        Box::pin(async move {
            response
                .recv_async()
                .await
                .unwrap_or(Err(CheckpointError::Closed(session_id)))
        })
    }
}

fn checkpoint_meta(current: &SessionMeta, checkpoint: &SessionCheckpoint) -> SessionMeta {
    let mut meta = current.clone();
    meta.session_options.clear();
    for option in checkpoint.options.options.iter() {
        let id = option.definition.id.as_ref();
        let enabled = option.current_value.as_ref() == ENABLED_VALUE;
        match id {
            YOLO_OPTION_ID => meta.yolo = enabled,
            FAST_OPTION_ID => meta.fast = enabled,
            WORKFLOW_OPTION_ID => meta.workflow = enabled,
            // Thinking predates session options and keeps its own field, so
            // the projection writes there rather than into the generic map.
            THINKING_OPTION_ID => {
                meta.thinking = option
                    .current_value
                    .parse::<ThinkingConfig>()
                    .ok()
                    .map(Into::into);
            }
            _ if option.definition.persistent && id.contains('.') => {
                meta.session_options
                    .insert(id.to_string(), option.current_value.to_string());
            }
            _ => {}
        }
    }
    meta
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::path::PathBuf;

    use maki_storage::checkpoint::{CheckpointRequest, CheckpointVersion};
    use maki_storage::id::MakiId;
    use tempfile::TempDir;

    use super::*;
    use crate::session_coordinator::builtin_option_definitions;
    use crate::session_options::SessionOptions;

    fn request(
        id: MakiId,
        revision: u64,
        model: &'static str,
        options: &SessionOptions,
    ) -> CheckpointRequest<SessionCheckpoint> {
        CheckpointRequest {
            session_id: id,
            version: CheckpointVersion { revision, epoch: 1 },
            snapshot: Arc::new(SessionCheckpoint {
                history: None,
                model: Arc::from(model),
                cwd: PathBuf::from("/project"),
                options: options.snapshot(),
            }),
        }
    }

    #[test]
    fn checkpoint_call_does_not_run_disk_save_on_the_executor() {
        smol::block_on(async {
            let tmp = TempDir::new().unwrap();
            let dir = StateDir::from_path(tmp.path().to_path_buf());
            let id = MakiId::generate();
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
            let writer = Arc::new(SessionLogCheckpoint::open(
                dir,
                id,
                "test/model",
                "/project",
            ));
            let session_guard = lock(&writer.session);
            let (checkpoint_tx, checkpoint_rx) = flume::bounded(1);
            let checkpoint_writer = Arc::clone(&writer);
            let checkpoint_request = request(id, 1, "test/model", &options);
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

    #[test]
    fn concurrent_checkpoints_preserve_submission_order() {
        smol::block_on(async {
            let tmp = TempDir::new().unwrap();
            let dir = StateDir::from_path(tmp.path().to_path_buf());
            let id = MakiId::generate();
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
            let writer = SessionLogCheckpoint::open(dir.clone(), id, "test/model", "/project");
            let first = writer.checkpoint(request(id, 1, "test/first", &options));
            let second = writer.checkpoint(request(id, 2, "test/second", &options));

            let (first, second) = futures_lite::future::zip(first, second).await;
            first.unwrap();
            second.unwrap();

            let loaded: StoredSession = StoredSession::load(id, &dir).unwrap();
            assert_eq!(loaded.model, "test/second");
        });
    }

    #[test]
    fn checkpoint_errors_survive_unblock() {
        smol::block_on(async {
            let tmp = TempDir::new().unwrap();
            let state_path = tmp.path().join("not-a-directory");
            std::fs::write(&state_path, "file").unwrap();
            let dir = StateDir::from_path(state_path);
            let id = MakiId::generate();
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
            let writer = SessionLogCheckpoint::open(dir, id, "test/model", "/project");

            assert!(matches!(
                writer
                    .checkpoint(request(id, 1, "test/model", &options))
                    .await,
                Err(CheckpointError::Save { session_id, .. }) if session_id == id
            ));
        });
    }

    #[test]
    fn projected_options_round_trip_session_storage() {
        smol::block_on(async {
            let tmp = TempDir::new().unwrap();
            let dir = StateDir::from_path(tmp.path().to_path_buf());
            let id = MakiId::generate();
            let options = SessionOptions::new(
                builtin_option_definitions(
                    "test/model",
                    [Arc::from("test/model")],
                    true,
                    true,
                    true,
                    ThinkingConfig::Effort(maki_providers::Effort::High),
                ),
                &BTreeMap::new(),
            )
            .unwrap();
            let writer = SessionLogCheckpoint::open(dir.clone(), id, "test/model", "/project");

            writer
                .checkpoint(CheckpointRequest {
                    session_id: id,
                    version: CheckpointVersion {
                        revision: 1,
                        epoch: 1,
                    },
                    snapshot: Arc::new(SessionCheckpoint {
                        history: Some(Arc::new(vec![Message::user("hello".into())])),
                        model: Arc::from("test/model"),
                        cwd: PathBuf::from("/project"),
                        options: options.snapshot(),
                    }),
                })
                .await
                .unwrap();

            let loaded: StoredSession = StoredSession::load(id, &dir).unwrap();
            assert!(loaded.meta.yolo);
            assert!(loaded.meta.fast);
            assert!(loaded.meta.workflow);
            assert_eq!(
                loaded.meta.thinking,
                Some(maki_storage::sessions::StoredThinking::Effort {
                    level: maki_providers::Effort::High
                }),
                "thinking is projected into the field it has always been saved in"
            );
            assert_eq!(loaded.messages().len(), 1);
            assert_eq!(loaded.model, "test/model");
            assert_eq!(loaded.cwd, "/project");
        });
    }
}
