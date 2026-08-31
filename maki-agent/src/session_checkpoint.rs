use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use maki_providers::{Message, TokenUsage};
use maki_storage::StateDir;
use maki_storage::checkpoint::{
    CheckpointAck, CheckpointError, CheckpointFuture, CheckpointRequest, CheckpointWriter,
};
use maki_storage::sessions::{Session, SessionMeta};

use crate::ToolOutput;
use crate::session_coordinator::SessionCheckpoint;
use crate::session_options::{ENABLED_VALUE, FAST_OPTION_ID, WORKFLOW_OPTION_ID, YOLO_OPTION_ID};

type StoredSession = Session<Message, TokenUsage, ToolOutput>;

pub struct SessionLogCheckpoint {
    dir: StateDir,
    session: Mutex<StoredSession>,
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
        Self {
            dir,
            session: Mutex::new(session),
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
        &self,
        request: CheckpointRequest<SessionCheckpoint>,
    ) -> Result<CheckpointAck, CheckpointError> {
        let mut session = lock(&self.session);
        let checkpoint = &request.snapshot;
        session.replace_messages(checkpoint.history.as_ref().clone());
        session.set_model(checkpoint.model.to_string());
        session.set_cwd(checkpoint.cwd.to_string_lossy().into_owned());
        session.update_title_if_default();
        session.meta = checkpoint_meta(&session.meta, checkpoint);
        session
            .save(&self.dir)
            .map_err(|error| CheckpointError::Save {
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
        let result = self.save(request);
        Box::pin(async move { result })
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
                        history: Arc::new(vec![Message::user("hello".into())]),
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
            assert_eq!(loaded.messages().len(), 1);
            assert_eq!(loaded.model, "test/model");
            assert_eq!(loaded.cwd, "/project");
        });
    }
}
