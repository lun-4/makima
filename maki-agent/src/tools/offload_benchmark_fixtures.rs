use std::fs;
#[cfg(test)]
use std::io;

use tempfile::TempDir;

use super::{OffloadError, OffloadStore, PutOutcome};

pub struct Fixture {
    _temp_dir: TempDir,
    store: OffloadStore,
    seeded_bodies: Vec<String>,
    new_body: String,
    sequence_bodies: Vec<String>,
}

impl Fixture {
    pub fn new(artifact_count: usize) -> Result<Self, OffloadError> {
        let temp_dir = TempDir::new()?;
        let store = OffloadStore::on_disk(temp_dir.path().to_owned());
        let seeded_bodies: Vec<String> = (0..artifact_count).map(body).collect();
        let new_body = body(artifact_count);
        let sequence_bodies: Vec<String> =
            (artifact_count + 1..=artifact_count * 2).map(body).collect();

        for seeded_body in &seeded_bodies {
            let saved = store.put(seeded_body)?;
            debug_assert_eq!(saved.outcome, PutOutcome::Created);
        }

        Ok(Self {
            _temp_dir: temp_dir,
            store,
            seeded_bodies,
            new_body,
            sequence_bodies,
        })
    }

    pub fn put_new(&self) -> Result<(), OffloadError> {
        let saved = self.store.put(&self.new_body)?;
        debug_assert_eq!(saved.outcome, PutOutcome::Created);
        Ok(())
    }

    pub fn put_duplicate(&self) -> Result<(), OffloadError> {
        let saved = self.store.put(&self.seeded_bodies[0])?;
        debug_assert_eq!(saved.outcome, PutOutcome::Existing);
        Ok(())
    }

    pub fn put_sequence(&self) -> Result<(), OffloadError> {
        for body in &self.sequence_bodies {
            let saved = self.store.put(body)?;
            debug_assert_eq!(saved.outcome, PutOutcome::Created);
        }
        Ok(())
    }

    #[cfg(test)]
    #[allow(dead_code)]
    pub fn file_count(&self) -> io::Result<usize> {
        Ok(fs::read_dir(self.store.dir())?.count())
    }
}

fn body(index: usize) -> String {
    format!("benchmark artifact {index:04}: small deterministic disk payload\n")
}
