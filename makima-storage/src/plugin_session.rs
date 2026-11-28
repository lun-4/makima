//! Bounded sidecars owned by a specific live session publication lease.
use std::fs::{self, File};
use std::io::{self, Read, Write};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use serde_json::Map;
use serde_json::Value;
use tempfile::NamedTempFile;

use crate::id::MakimaId;
use crate::session_lock::SessionPublicationGuard;

pub(crate) const PLUGIN_STATE_DIR: &str = "plugin-state";
const MAX_RECORD_BYTES: usize = 32 * 1024;

pub type PluginRecord = Map<String, Value>;

/// A host binding, not a discoverable session ID. Clones keep the original
/// lease and runtime validity even when a replacement opens the same ID.
#[derive(Clone)]
pub struct PluginSession {
    id: MakimaId,
    dir: PathBuf,
    lease: SessionPublicationGuard,
    live: Arc<Mutex<bool>>,
}

impl PluginSession {
    pub fn new(id: MakimaId, lease: SessionPublicationGuard) -> Option<Self> {
        let dir = lease.directory_for(id)?.to_path_buf();
        Some(Self {
            id,
            dir,
            lease,
            live: Arc::new(Mutex::new(true)),
        })
    }

    pub fn id(&self) -> MakimaId {
        self.id
    }

    pub fn retire(&self) {
        *self.live.lock().unwrap_or_else(|e| e.into_inner()) = false;
    }

    fn path(&self, plugin: &str, key: &str) -> Result<PathBuf, String> {
        if [plugin, key].iter().any(|name| {
            name.is_empty()
                || name.len() > 128
                || !name
                    .bytes()
                    .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'-' | b'_' | b'.'))
                || matches!(*name, "." | "..")
        }) {
            return Err("invalid plugin session sidecar key".into());
        }
        Ok(self
            .dir
            .join(PLUGIN_STATE_DIR)
            .join(self.id.to_string())
            .join(plugin)
            .join(format!("{key}.json")))
    }

    fn with_lease<T>(&self, f: impl FnOnce() -> io::Result<T>) -> Result<T, String> {
        let live = self.live.lock().unwrap_or_else(|e| e.into_inner());
        if !*live {
            return Err("plugin session runtime retired".into());
        }
        self.lease
            .publish(f)
            .map_err(|e| e.to_string())?
            .ok_or_else(|| "plugin session lease lost".to_owned())?
            .map_err(|e| e.to_string())
    }

    pub fn read(&self, plugin: &str, key: &str) -> Result<Option<PluginRecord>, String> {
        let path = self.path(plugin, key)?;
        self.with_lease(|| {
            let metadata = match fs::symlink_metadata(&path) {
                Ok(metadata) => metadata,
                Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
                Err(e) => return Err(e),
            };
            if !metadata.is_file() || metadata.len() > MAX_RECORD_BYTES as u64 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "invalid plugin session record file",
                ));
            }
            let file = File::open(path)?;
            let mut bytes = Vec::new();
            file.take((MAX_RECORD_BYTES + 1) as u64)
                .read_to_end(&mut bytes)?;
            if bytes.len() > MAX_RECORD_BYTES {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "plugin session record exceeds size limit",
                ));
            }
            serde_json::from_slice(&bytes)
                .map(Some)
                .map_err(io::Error::other)
        })
    }

    pub fn write(&self, plugin: &str, key: &str, record: &PluginRecord) -> Result<(), String> {
        let path = self.path(plugin, key)?;
        let bytes = serde_json::to_vec(record).map_err(|e| e.to_string())?;
        if bytes.len() > MAX_RECORD_BYTES {
            return Err("plugin session record exceeds size limit".into());
        }
        // The same binding guards staging and replacement. Use the host's
        // atomic rename primitive without following a sidecar file symlink.
        self.with_lease(|| {
            let parent = path.parent().expect("sidecar has a parent");
            fs::create_dir_all(parent)?;
            let mut tmp = NamedTempFile::new_in(parent)?;
            tmp.write_all(&bytes)?;
            tmp.as_file().sync_data()?;
            crate::persist(tmp, &path).map_err(io::Error::other)
        })
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::session_lock::claim;

    #[test]
    fn stale_lease_and_retired_runtime_cannot_replace_plugin_record() {
        let dir = tempfile::tempdir().unwrap();
        let id = MakimaId::generate();
        let first = claim(dir.path(), &id).unwrap().unwrap();
        let stale = PluginSession::new(id, first.publication_guard()).unwrap();
        assert!(PluginSession::new(MakimaId::generate(), first.publication_guard()).is_none());
        assert!(stale.read("owner", "record").unwrap().is_none());
        let old = json!({"version": 1, "content": "old"});
        stale
            .write("owner", "record", old.as_object().unwrap())
            .unwrap();
        first.release().unwrap();
        let second = claim(dir.path(), &id).unwrap().unwrap();
        let current = PluginSession::new(id, second.publication_guard()).unwrap();
        let new = json!({"version": 1, "content": "new"});
        current
            .write("owner", "record", new.as_object().unwrap())
            .unwrap();
        assert!(
            stale
                .write("owner", "record", old.as_object().unwrap())
                .is_err()
        );
        assert_eq!(
            current.read("owner", "record").unwrap().as_ref(),
            new.as_object()
        );
        current.retire();
        assert!(
            current
                .write("owner", "record", old.as_object().unwrap())
                .is_err()
        );
        let reopened = PluginSession::new(id, second.publication_guard()).unwrap();
        assert_eq!(
            reopened.read("owner", "record").unwrap().as_ref(),
            new.as_object()
        );
    }
}
