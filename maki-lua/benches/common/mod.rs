//! Shared helpers for the maki-lua criterion benches: the one-shot watchdog
//! interrupt the plugin runtime uses (see `luau_perf` for the cancellation
//! strategies comparison).

use std::ffi::c_int;
use std::ptr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicPtr, Ordering};
use std::thread;
use std::time::Duration;

use mlua::{Lua, ffi};

pub const WATCHDOG_POLL_INTERVAL: Duration = Duration::from_millis(10);

pub type InterruptFn = unsafe extern "C-unwind" fn(*mut ffi::lua_State, c_int);

/// Same atomic store the runtime's watchdog uses: the poker thread and the
/// VM thread race on this field, so plain writes would be a data race.
pub fn store_interrupt(state: *mut ffi::lua_State, cb: Option<InterruptFn>) {
    let raw = cb.map_or(ptr::null_mut(), |f| f as *mut ());
    unsafe {
        let slot = &raw mut (*ffi::lua_callbacks(state)).interrupt;
        AtomicPtr::from_ptr(slot.cast::<*mut ()>()).store(raw, Ordering::Release);
    }
}

unsafe extern "C-unwind" fn one_shot_interrupt(state: *mut ffi::lua_State, gc: c_int) {
    if gc >= 0 {
        return;
    }
    store_interrupt(state, None);
}

pub struct WatchdogGuard {
    stop: Arc<AtomicBool>,
    thread: Option<thread::JoinHandle<()>>,
}

impl Drop for WatchdogGuard {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(t) = self.thread.take() {
            t.thread().unpark();
            let _ = t.join();
        }
    }
}

/// A resident thread arms a one-shot native interrupt every 10ms, like the
/// runtime's watchdog; `keep_alive` keeps the VM alive while the thread spins.
pub fn spawn_watchdog(lua: &Lua) -> WatchdogGuard {
    let main_state = lua.exec_raw_lua(|raw| unsafe { ffi::lua_mainthread(raw.state()) }) as usize;
    let stop = Arc::new(AtomicBool::new(false));
    let thread = thread::spawn({
        let stop = Arc::clone(&stop);
        let keep_alive = lua.clone();
        move || {
            let _keep_alive = keep_alive;
            loop {
                thread::park_timeout(WATCHDOG_POLL_INTERVAL);
                if stop.load(Ordering::Relaxed) {
                    return;
                }
                store_interrupt(main_state as *mut ffi::lua_State, Some(one_shot_interrupt));
            }
        }
    });
    WatchdogGuard {
        stop,
        thread: Some(thread),
    }
}
