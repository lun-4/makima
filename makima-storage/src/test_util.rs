use crate::{
    StateDir,
    blobs::{BlobStore, FileState},
    id::{AgentId, MakimaId},
    restore::{
        PathRestore, RestoreChoice, RestoreEndpoint, RestoreStore, RestoreTransition, TransitionRef,
    },
    restore_digest::session_digest,
    session_state::{StoredAgentLifecycle, StoredAgentRecord},
    sessions::{Session, TitleSource},
};
use serde::{Serialize, de::DeserializeOwned};
use std::path::Path;

pub fn pending_restore<M, U, T>(
    storage: &StateDir,
    session: &Session<M, U, T>,
    path: &Path,
    before: &[u8],
    after: &[u8],
) -> TransitionRef
where
    M: Serialize + DeserializeOwned + TitleSource + Clone,
    U: Serialize + DeserializeOwned + Default,
    T: Serialize + DeserializeOwned,
{
    let blobs = BlobStore::new(storage.clone());
    let endpoint = RestoreEndpoint {
        session: session.id,
        branch: None,
        seq: session.sequence_high_water().unwrap_or_default(),
        digest: session_digest(session).unwrap(),
    };
    RestoreStore::new(storage.clone())
        .prepare(
            session.id,
            RestoreTransition {
                id: MakimaId::generate(),
                choice: RestoreChoice::FilesOnly,
                source: endpoint.clone(),
                target: endpoint,
                inverse_of: None,
                paths: vec![PathRestore {
                    path: path.to_path_buf(),
                    before: FileState::Blob(blobs.put(session.id, before).unwrap()),
                    after: FileState::Blob(blobs.put(session.id, after).unwrap()),
                }],
                skipped: Vec::new(),
            },
        )
        .unwrap()
}

pub fn test_record(id: AgentId, parent: Option<AgentId>) -> StoredAgentRecord {
    StoredAgentRecord {
        id,
        parent,
        depth: u32::from(parent.is_some()),
        transcript_key: parent.map_or_else(String::new, |_| id.to_string()),
        lifecycle: StoredAgentLifecycle::Open,
        ancestry_unknown: false,
        label: None,
        spawned_by_tool_use_id: None,
        spec: None,
        restorable: Ok(()),
        usage: Default::default(),
        created_seq: 0,
        updated_seq: 0,
        legacy: false,
    }
}
