use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, MutexGuard};

use klipper_transport::{ReliableSerialSession, SerialSettings, SessionEvent};
use thiserror::Error;

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum McuRegistryError {
    #[error("MCU name cannot be empty")]
    EmptyName,
    #[error("MCU '{name}' is already registered")]
    DuplicateName { name: String },
    #[error("failed to open MCU '{name}': {message}")]
    OpenFailed { name: String, message: String },
    #[error("opening MCU '{name}' was cancelled by registry cleanup")]
    OpenCancelled { name: String },
    #[error("MCU '{name}' session is already disconnected")]
    DisconnectedSession { name: String },
}

#[derive(Debug, Clone, PartialEq)]
pub struct McuSessionEvent {
    pub mcu: String,
    pub event: SessionEvent,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McuPollError {
    pub mcu: String,
    pub message: String,
    pub disconnected: bool,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct McuPollBatch {
    pub events: Vec<McuSessionEvent>,
    pub errors: Vec<McuPollError>,
}

/// Owns the serial sessions for all MCUs attached to this printer.
///
/// Session handles are independently locked so one MCU's serial read does not
/// hold the registry lock or block access to another MCU's session.
#[derive(Default)]
struct McuRegistryState {
    sessions: HashMap<String, Arc<Mutex<ReliableSerialSession>>>,
    opening: HashSet<String>,
    generation: u64,
}

#[derive(Default)]
pub struct McuRegistry {
    // Opening a serial device can block in the OS. Keep that work outside the
    // registry lock, while reserving the name and generation transactionally.
    state: Mutex<McuRegistryState>,
}

fn poll_generation_is_current(snapshot: u64, current: u64) -> bool {
    snapshot == current
}

fn poll_error_is_reportable(terminal_handle_detached: bool, handle_is_current: bool) -> bool {
    terminal_handle_detached || handle_is_current
}

fn session_handle_is_current<T>(
    current: Option<&Arc<Mutex<T>>>,
    snapshot: Option<&Arc<Mutex<T>>>,
) -> bool {
    matches!((current, snapshot), (Some(current), Some(snapshot)) if Arc::ptr_eq(current, snapshot))
}

fn disconnect_session_handle(session: &Arc<Mutex<ReliableSerialSession>>) {
    match session.lock() {
        Ok(mut guard) => guard.mark_disconnected(),
        Err(poisoned) => poisoned.into_inner().mark_disconnected(),
    }
}

fn remove_session_if_same<T>(
    sessions: &mut HashMap<String, Arc<Mutex<T>>>,
    name: &str,
    session: &Arc<Mutex<T>>,
) -> bool {
    if sessions.get(name).is_some_and(|current| Arc::ptr_eq(current, session)) {
        sessions.remove(name);
        true
    } else {
        false
    }
}

impl McuRegistry {
    // Registry critical sections contain only internal bookkeeping; recover
    // the state after poisoning so one panic cannot permanently disable MCU
    // management. Individual session locks still have terminal-state handling.
    fn lock_state(&self) -> MutexGuard<'_, McuRegistryState> {
        self.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    pub fn new() -> Self {
        Self::default()
    }

    pub fn open_serial(
        &self,
        name: impl Into<String>,
        settings: &SerialSettings,
    ) -> Result<(), McuRegistryError> {
        let name = normalize_mcu_name(name.into())?;
        let generation = {
            let mut state = self.lock_state();
            if state.sessions.contains_key(&name) || !state.opening.insert(name.clone()) {
                return Err(McuRegistryError::DuplicateName { name });
            }
            state.generation
        };

        let opened = ReliableSerialSession::open(settings);
        let mut state = self.lock_state();
        // A clear/disconnect may have happened while the OS was opening the
        // device. Never publish a session into a newer printer lifecycle.
        if !poll_generation_is_current(generation, state.generation) {
            // The operation belongs to a printer lifecycle that no longer
            // exists. Its OS-level result is stale too: drop a successfully
            // opened transport and report cancellation consistently.
            drop(opened);
            return Err(McuRegistryError::OpenCancelled { name });
        }
        state.opening.remove(&name);
        let session = opened.map_err(|error| McuRegistryError::OpenFailed {
            name: name.clone(),
            message: error.to_string(),
        })?;
        state.sessions.insert(name, Arc::new(Mutex::new(session)));
        Ok(())
    }

    pub fn register_session(
        &self,
        name: impl Into<String>,
        session: ReliableSerialSession,
    ) -> Result<(), McuRegistryError> {
        let name = normalize_mcu_name(name.into())?;
        if session.is_disconnected() {
            return Err(McuRegistryError::DisconnectedSession { name });
        }
        let mut state = self.lock_state();
        if state.sessions.contains_key(&name) || state.opening.contains(&name) {
            return Err(McuRegistryError::DuplicateName { name });
        }
        state.sessions.insert(name, Arc::new(Mutex::new(session)));
        Ok(())
    }

    pub fn session_handle(
        &self,
        name: &str,
    ) -> Option<Arc<Mutex<ReliableSerialSession>>> {
        self.lock_state()
            .sessions
            .get(name)
            .cloned()
    }

    pub fn with_session<R>(
        &self,
        name: &str,
        callback: impl FnOnce(&mut ReliableSerialSession) -> R,
    ) -> Option<R> {
        let session = self.session_handle(name)?;
        match session.lock() {
            Ok(mut guard) => {
                // Catch callback panics while the guard is still available so
                // the session can be invalidated and detached immediately.
                // Resuming the panic preserves the caller's existing behavior.
                match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    callback(&mut guard)
                })) {
                    Ok(result) => Some(result),
                    Err(payload) => {
                        guard.mark_disconnected();
                        drop(guard);
                        let mut state = self.lock_state();
                        remove_session_if_same(&mut state.sessions, name, &session);
                        std::panic::resume_unwind(payload)
                    }
                }
            }
            Err(poisoned) => {
                // A prior panic may have left the session's protocol state
                // partially mutated. Treat it as terminal and remove this
                // exact handle from the registry before returning.
                let mut guard = poisoned.into_inner();
                guard.mark_disconnected();
                drop(guard);
                let mut state = self.lock_state();
                remove_session_if_same(&mut state.sessions, name, &session);
                None
            }
        }
    }

    pub fn remove_session(
        &self,
        name: &str,
    ) -> Option<Arc<Mutex<ReliableSerialSession>>> {
        // Detach under the registry lock, then invalidate outside it. This
        // lock order avoids deadlocking against a poll that owns a session
        // lock and needs the registry lock for panic cleanup.
        let session = {
            let mut state = self.lock_state();
            // Removing one MCU must not invalidate unrelated sessions or
            // cancel serial opens reserved by other MCU names. poll_all()
            // filters this exact handle out before publishing its batch.
            state.sessions.remove(name)?
        };
        disconnect_session_handle(&session);
        Some(session)
    }

    pub fn clear(&self) -> Vec<String> {
        let (names, sessions) = {
            let mut state = self.lock_state();
            let mut names: Vec<_> = state.sessions.keys().cloned().collect();
            names.sort();
            let sessions = state.sessions.drain().map(|(_, session)| session).collect::<Vec<_>>();
            state.opening.clear();
            state.generation = state.generation.wrapping_add(1);
            (names, sessions)
        };

        // Invalidate detached sessions even if another caller retained a
        // session_handle() clone. Never hold the registry lock while waiting
        // for an individual session lock.
        for session in &sessions {
            disconnect_session_handle(session);
        }
        names
    }

    pub fn session_names(&self) -> Vec<String> {
        let mut names: Vec<_> = self
            .lock_state()
            .sessions
            .keys()
            .cloned()
            .collect();
        names.sort();
        names
    }

    pub fn is_disconnected(&self, name: &str) -> Option<bool> {
        self.with_session(name, |session| session.is_disconnected())
    }

    /// Return the current startup phase for one registered MCU.
    ///
    /// A missing or concurrently detached MCU returns None. Session locking
    /// and poisoned-session cleanup follow the same rules as other registry
    /// inspection methods.
    pub fn startup_stage(&self, name: &str) -> Option<klipper_transport::McuStartupStage> {
        self.with_session(name, |session| session.startup_stage())
    }

    /// Snapshot startup phases for all currently registered MCUs in name order.
    ///
    /// Each phase is read under its own session lock, so a slow or detached
    /// MCU cannot hold the registry lock or block inspection of other sessions.
    pub fn startup_stages(&self) -> Vec<(String, klipper_transport::McuStartupStage)> {
        let mut names = self.session_names();
        names.sort();
        names.into_iter().filter_map(|name| {
            self.startup_stage(&name).map(|stage| (name, stage))
        }).collect()
    }

    /// Poll every registered MCU once, preserving each session's event order.
    /// A failed MCU is reported independently; other sessions are still polled.
    pub fn poll_all(&self) -> McuPollBatch {
        // Take the generation and session snapshot under the same lock. If
        // clear() runs while any serial session is being polled, the complete
        // batch belongs to an obsolete printer lifecycle and must be dropped.
        let (generation, mut sessions): (u64, Vec<_>) = {
            let state = self.lock_state();
            (
                state.generation,
                state.sessions
                    .iter()
                    .map(|(name, session)| (name.clone(), session.clone()))
                    .collect(),
            )
        };
        sessions.sort_by(|left, right| left.0.cmp(&right.0));
        let snapshot_handles: HashMap<_, _> = sessions
            .iter()
            .map(|(name, session)| (name.clone(), session.clone()))
            .collect();

        let mut batch = McuPollBatch::default();
        let mut terminal_sessions = Vec::new();
        for (name, session) in sessions {
            let session_handle = session.clone();
            let mut session = match session.lock() {
                Ok(guard) => guard,
                Err(poisoned) => {
                    // Do not let one panicked session take down polling for
                    // every other MCU. Its state is no longer trustworthy.
                    let mut guard = poisoned.into_inner();
                    guard.mark_disconnected();
                    drop(guard);
                    terminal_sessions.push((name.clone(), session_handle));
                    batch.errors.push(McuPollError {
                        mcu: name,
                        message: "MCU session lock poisoned after a panic".to_owned(),
                        disconnected: true,
                    });
                    continue;
                }
            };
            let poll_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                session.poll()
            }));
            match poll_result {
                Ok(Ok(events)) => batch.events.extend(events.into_iter().map(|event| {
                    McuSessionEvent {
                        mcu: name.clone(),
                        event,
                    }
                })),
                Ok(Err(error)) => {
                    let disconnected = session.is_disconnected();
                    batch.errors.push(McuPollError {
                        mcu: name.clone(),
                        message: error.to_string(),
                        disconnected,
                    });
                    if disconnected {
                        terminal_sessions.push((name, session_handle));
                    }
                },
                Err(_) => {
                    // A panic during protocol processing may leave scheduler
                    // and response-router state inconsistent. Isolate it to
                    // this MCU and require a fresh serial session.
                    session.mark_disconnected();
                    drop(session);
                    terminal_sessions.push((name.clone(), session_handle));
                    batch.errors.push(McuPollError {
                        mcu: name,
                        message: "MCU session poll panicked".to_owned(),
                        disconnected: true,
                    });
                }
            }
        }

        // A clear/disconnect invalidates the whole printer lifecycle. A
        // single-session removal, however, only filters that handle: events
        // and errors from unrelated MCUs remain valid for this poll cycle.
        let mut state = self.lock_state();
        if !poll_generation_is_current(generation, state.generation) {
            return McuPollBatch::default();
        }
        batch.events.retain(|event| {
            session_handle_is_current(
                state.sessions.get(&event.mcu),
                snapshot_handles.get(&event.mcu),
            )
        });

        // A terminally disconnected session cannot be polled or reopened
        // under the same name while it remains registered. Detach only the
        // exact failed handle, preserving its final error for the caller.
        let mut terminal_names = HashSet::new();
        for (name, session) in terminal_sessions {
            if remove_session_if_same(&mut state.sessions, &name, &session) {
                terminal_names.insert(name);
            }
        }
        batch.errors.retain(|error| {
            poll_error_is_reportable(
                terminal_names.contains(&error.mcu),
                session_handle_is_current(
                    state.sessions.get(&error.mcu),
                    snapshot_handles.get(&error.mcu),
                ),
            )
        });
        batch
    }

}

fn normalize_mcu_name(name: String) -> Result<String, McuRegistryError> {
    let name = name.trim();
    if name.is_empty() {
        Err(McuRegistryError::EmptyName)
    } else {
        Ok(name.to_owned())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn poisoned_session_cleanup_does_not_remove_a_replacement_handle() {
        let old = Arc::new(Mutex::new(1_u8));
        let replacement = Arc::new(Mutex::new(2_u8));
        let mut sessions = HashMap::from([("mcu".to_owned(), replacement.clone())]);

        assert!(!remove_session_if_same(&mut sessions, "mcu", &old));
        assert!(Arc::ptr_eq(sessions.get("mcu").unwrap(), &replacement));
        assert!(remove_session_if_same(&mut sessions, "mcu", &replacement));
        assert!(!sessions.contains_key("mcu"));
    }

    #[test]
    fn registry_operations_recover_poisoned_bookkeeping_lock() {
        let registry = Arc::new(McuRegistry::new());
        let poisoned_registry = registry.clone();
        let panic_result = std::thread::spawn(move || {
            let _guard = poisoned_registry.state.lock().unwrap();
            panic!("poison MCU registry mutex");
        }).join();
        assert!(panic_result.is_err());

        assert!(registry.session_names().is_empty());
        assert!(registry.session_handle("mcu").is_none());
        assert_eq!(registry.clear(), Vec::<String>::new());
        assert_eq!(registry.poll_all(), McuPollBatch::default());
    }

    #[test]
    fn startup_stage_queries_are_empty_without_registered_sessions() {
        let registry = McuRegistry::new();
        assert_eq!(registry.startup_stage("mcu"), None);
        assert!(registry.startup_stages().is_empty());
    }

    #[test]
    fn registry_starts_empty_and_polls_without_errors() {
        let registry = McuRegistry::new();
        assert!(registry.session_names().is_empty());
        assert!(registry.session_handle("mcu").is_none());
        assert!(registry.remove_session("mcu").is_none());
        assert!(registry.is_disconnected("mcu").is_none());
        assert_eq!(registry.poll_all(), McuPollBatch::default());
    }

    #[test]
    fn clear_returns_sorted_session_names_and_empties_registry() {
        let registry = McuRegistry::new();
        assert!(registry.clear().is_empty());
        assert!(registry.session_names().is_empty());
    }

    #[test]
    fn poll_batch_generation_check_rejects_a_cleared_registry() {
        assert!(poll_generation_is_current(7, 7));
        assert!(!poll_generation_is_current(7, 8));
    }

    #[test]
    fn terminal_mcu_errors_survive_detach_but_stale_errors_do_not() {
        assert!(poll_error_is_reportable(true, false));
        assert!(poll_error_is_reportable(false, true));
        assert!(!poll_error_is_reportable(false, false));
    }

    #[test]
    fn removing_one_session_does_not_match_a_replacement_handle() {
        let old = Arc::new(Mutex::new(1_u8));
        let replacement = Arc::new(Mutex::new(2_u8));
        assert!(session_handle_is_current(Some(&old), Some(&old)));
        assert!(!session_handle_is_current(Some(&replacement), Some(&old)));
        assert!(!session_handle_is_current(None, Some(&old)));
        assert!(!session_handle_is_current(Some(&old), None));
    }

    #[test]
    fn clear_invalidates_an_in_progress_open_reservation() {
        let registry = McuRegistry::new();
        let generation = {
            let mut state = registry.state.lock().unwrap();
            state.opening.insert("mcu".to_owned());
            state.generation
        };

        registry.clear();

        let state = registry.state.lock().unwrap();
        assert!(!state.opening.contains("mcu"));
        assert_ne!(state.generation, generation);
    }

    #[test]
    fn mcu_names_are_trimmed_before_registry_lookup_and_storage() {
        assert_eq!(normalize_mcu_name("  mcu  ".to_owned()).unwrap(), "mcu");
        assert_eq!(normalize_mcu_name(" \t ".to_owned()), Err(McuRegistryError::EmptyName));
    }

    #[test]
    fn serial_open_uses_normalized_name_in_errors_and_reservations() {
        let registry = McuRegistry::new();
        let settings = SerialSettings::new("", 0);
        assert!(matches!(
            registry.open_serial("  mcu  ", &settings),
            Err(McuRegistryError::OpenFailed { name, .. }) if name == "mcu"
        ));
        assert!(registry.session_names().is_empty());
        assert!(registry.lock_state().opening.is_empty());
    }

    #[test]
    fn serial_open_rejects_empty_name_before_opening_port() {
        let registry = McuRegistry::new();
        let settings = SerialSettings::new("", 250_000);
        assert_eq!(
            registry.open_serial("  ", &settings),
            Err(McuRegistryError::EmptyName)
        );
        assert!(registry.session_names().is_empty());
    }

    #[test]
    fn serial_open_reports_invalid_baud_rate_without_hardware() {
        let registry = McuRegistry::new();
        let settings = SerialSettings::new("", 0);
        assert!(matches!(
            registry.open_serial("mcu", &settings),
            Err(McuRegistryError::OpenFailed { name, .. }) if name == "mcu"
        ));
        assert!(registry.session_names().is_empty());
        assert!(registry.state.lock().unwrap().opening.is_empty());
    }
}
