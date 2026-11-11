//! Panic-catching concurrent task set for tool execution.
//!
//! Tool panics must not crash the agent; every spawned task is wrapped in `catch_unwind` and returns `Err(String)` instead.

use std::backtrace::Backtrace;
use std::future::Future;
use std::panic::AssertUnwindSafe;

use futures_lite::FutureExt;

pub(crate) struct TaskSet<T> {
    tasks: Vec<smol::Task<Result<T, String>>>,
}

impl<T: Send + 'static> TaskSet<T> {
    pub fn new() -> Self {
        Self { tasks: Vec::new() }
    }

    pub fn spawn<F>(&mut self, future: F)
    where
        F: Future<Output = T> + Send + 'static,
    {
        self.tasks.push(smol::spawn(async move {
            AssertUnwindSafe(future)
                .catch_unwind()
                .await
                .map_err(panic_to_string)
        }));
    }

    pub async fn join_all(self) -> Vec<Result<T, String>> {
        let mut results = Vec::with_capacity(self.tasks.len());
        for task in self.tasks {
            results.push(task.await);
        }
        results
    }
}

fn panic_to_string(payload: Box<dyn std::any::Any + Send>) -> String {
    let msg = if let Some(s) = payload.downcast_ref::<&str>() {
        (*s).to_owned()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "unknown panic".into()
    };
    let bt = Backtrace::force_capture();
    format!("{msg}\n\nBacktrace:\n{bt}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mixed_panic_and_ok() {
        smol::block_on(async {
            let mut set: TaskSet<i32> = TaskSet::new();
            set.spawn(async { 42 });
            set.spawn(async { panic!("oops") });
            set.spawn(async { 7 });
            let results = set.join_all().await;
            assert_eq!(results.len(), 3);
            assert_eq!(results[0].as_ref().unwrap(), &42);
            let err = results[1].as_ref().unwrap_err();
            assert!(err.starts_with("oops"), "unexpected error: {err}");
            assert!(err.contains("Backtrace:"), "missing backtrace in: {err}");
            assert_eq!(results[2].as_ref().unwrap(), &7);
        });
    }
}
