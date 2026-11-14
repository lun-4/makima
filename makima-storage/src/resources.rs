use crate::StateDir;
use std::{
    any::{Any, TypeId},
    collections::HashMap,
    fs, io,
    ops::Deref,
    path::PathBuf,
    sync::{Arc, Mutex, OnceLock, Weak},
};

type RootRegistry = Mutex<HashMap<PathBuf, Weak<RootResources>>>;

static ROOTS: OnceLock<RootRegistry> = OnceLock::new();

#[derive(Default)]
struct RootResources {
    values: Mutex<HashMap<TypeId, Arc<dyn Any + Send + Sync>>>,
}

/// A typed resource and a keepalive for its canonical state root.
pub struct RootResource<T> {
    value: Arc<T>,
    root: Arc<RootResources>,
}

impl<T> Clone for RootResource<T> {
    fn clone(&self) -> Self {
        Self {
            value: Arc::clone(&self.value),
            root: Arc::clone(&self.root),
        }
    }
}

impl<T> Deref for RootResource<T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.value
    }
}

impl StateDir {
    /// Shares a resource across handles for the same existing canonical root.
    /// Resources expire when the last root keepalive is released.
    pub fn resource<T: Default + Send + Sync + 'static>(&self) -> io::Result<RootResource<T>> {
        let path = fs::canonicalize(self.path())?;
        let root = {
            let mut roots = ROOTS
                .get_or_init(Mutex::default)
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            roots.retain(|_, root| root.strong_count() > 0);
            match roots.get(&path).and_then(Weak::upgrade) {
                Some(root) => root,
                None => {
                    let root = Arc::new(RootResources::default());
                    roots.insert(path, Arc::downgrade(&root));
                    root
                }
            }
        };
        let existing = {
            let values = root
                .values
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            values.get(&TypeId::of::<T>()).cloned()
        };
        let value = if let Some(existing) = existing {
            existing
        } else {
            let candidate = Arc::new(T::default());
            let value = {
                let mut values = root
                    .values
                    .lock()
                    .unwrap_or_else(|error| error.into_inner());
                Arc::clone(
                    values
                        .entry(TypeId::of::<T>())
                        .or_insert_with(|| candidate.clone()),
                )
            };
            drop(candidate);
            value
        };
        let value = value
            .downcast::<T>()
            .map_err(|_| io::Error::other("root resource type mismatch"))?;
        Ok(RootResource { value, root })
    }
}

#[cfg(test)]
mod tests {
    use crate::StateDir;
    use std::{
        fs, io,
        sync::{Arc, Mutex},
    };
    use test_case::test_case;

    #[derive(Default)]
    struct Resource;

    #[test_case(false; "independent_handles")]
    #[test_case(true; "symlink_alias")]
    fn resources_share_only_the_same_canonical_root(alias: bool) {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().join("root");
        fs::create_dir(&root).unwrap();
        let other = tempfile::tempdir().unwrap();
        let alias_path = directory.path().join("alias");
        if alias {
            #[cfg(unix)]
            std::os::unix::fs::symlink(&root, &alias_path).unwrap();
            #[cfg(not(unix))]
            return;
        }
        let first = StateDir::from_path(root.clone())
            .resource::<Resource>()
            .unwrap();
        let second = StateDir::from_path(if alias { alias_path } else { root })
            .resource::<Resource>()
            .unwrap();
        let separate = StateDir::from_path(other.path().to_path_buf())
            .resource::<Resource>()
            .unwrap();
        assert!(Arc::ptr_eq(&first.value, &second.value));
        assert!(!Arc::ptr_eq(&first.value, &separate.value));
        let weak = Arc::downgrade(&first.root);
        drop(first);
        drop(second);
        assert!(weak.upgrade().is_none());
    }

    #[test]
    fn missing_root_does_not_create_or_intern_a_resource() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("missing");
        let error = StateDir::from_path(path.clone())
            .resource::<Resource>()
            .err()
            .unwrap();
        assert_eq!(error.kind(), io::ErrorKind::NotFound);
        assert!(!path.exists());
    }

    #[derive(Default)]
    struct ReentrantDrop(Mutex<Option<StateDir>>);

    impl Drop for ReentrantDrop {
        fn drop(&mut self) {
            if let Some(state) = self.0.get_mut().unwrap().take() {
                state.resource::<Resource>().unwrap();
            }
        }
    }

    #[test]
    fn resource_destruction_can_reenter_the_registry() {
        let directory = tempfile::tempdir().unwrap();
        let state = StateDir::from_path(directory.path().to_path_buf());
        let resource = state.resource::<ReentrantDrop>().unwrap();
        *resource.0.lock().unwrap() = Some(state);
        drop(resource);
    }
}
