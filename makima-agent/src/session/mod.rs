use std::{path::Path, sync::Arc};

use makima_storage::{
    id::MakimaId,
    session_lock,
    sessions::{SessionRecord, normalize_title},
};

use crate::permissions::{PermissionManager, PermissionSnapshot};
use crate::session_writer::{SessionWriter, StoredSession};
use crate::{AgentRef, EffectiveAgentConfig, ManagerError};

mod answers;
mod events;
mod open;

pub use crate::session_recovery::{OpenSessionError, SessionSource};
pub use answers::{AnswerError, AnswerKey, AnswerRouter, PendingAnswer};
pub use events::{SessionEvent, SessionEventPump, SessionSubscription};
pub use open::{
    Durability, FrontendObserver, FrontendServices, OpenRequest, PrepareOptions, PreparedSession,
    SessionHandle, SessionPersistence, SessionReuse, open, prepare,
};

/// A fresh session and optional runtime configuration carried over on reset.
pub struct ResetSeed {
    pub(super) session: StoredSession,
    pub(super) config: Option<(Arc<EffectiveAgentConfig>, PermissionSnapshot)>,
}

impl ResetSeed {
    pub fn new(model_spec: &str, cwd: &Path) -> Self {
        Self {
            session: StoredSession::new(model_spec, &cwd.to_string_lossy()),
            config: None,
        }
    }

    pub fn from_root(
        root: &AgentRef,
        permissions: &PermissionManager,
    ) -> Result<Self, ManagerError> {
        let config = root
            .effective_config()?
            .ok_or_else(|| ManagerError::NonLiveAgent(root.id()))?;
        let mut seed = Self::new(&config.model.spec(), config.context.cwd());
        seed.config = Some((config, permissions.snapshot()));
        Ok(seed)
    }

    pub fn with_session(mut self, session: StoredSession) -> Self {
        self.session = session;
        self
    }

    pub fn into_session(self) -> StoredSession {
        self.session
    }

    pub fn into_source(self) -> SessionSource {
        SessionSource::New(self.into_session())
    }
}

pub async fn set_title(
    directory: &Path,
    id: MakimaId,
    title: String,
) -> Result<(), OpenSessionError> {
    let mut lease = session_lock::claim(directory, &id)?.ok_or(OpenSessionError::Busy(id))?;
    let session = StoredSession::load_from(id, directory)?;
    let writer = SessionWriter::open_leased(directory, &session, &mut lease)?;
    writer
        .append(vec![SessionRecord::Title {
            title: normalize_title(&title),
        }])
        .await?;
    Ok(())
}
