use std::path::Path;

use makima_storage::{
    id::MakimaId,
    session_lock,
    sessions::{SessionRecord, normalize_title},
};

use crate::{
    session_recovery::OpenSessionError,
    session_writer::{SessionWriter, StoredSession},
};

mod answers;
mod events;

pub use answers::{AnswerError, AnswerKey, AnswerRouter, PendingAnswer};
pub use events::{SessionEvent, SessionEventPump, SessionSubscription};

pub async fn set_title(
    directory: &Path,
    id: MakimaId,
    title: String,
) -> Result<(), OpenSessionError> {
    let lease = session_lock::claim(directory, &id)?.ok_or(OpenSessionError::Busy(id))?;
    let session = StoredSession::load_from(id, directory)?;
    let writer = SessionWriter::open_session(directory, &session)?;
    writer
        .append_owned(
            vec![SessionRecord::Title {
                title: normalize_title(&title),
            }],
            lease.publication_guard(),
        )
        .await?;
    Ok(())
}
