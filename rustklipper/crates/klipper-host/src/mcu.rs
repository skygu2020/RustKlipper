use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

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

fn remove_session_if_same(
    state: &Mutex<McuRegistryState>,
    name: &str,
    session: &Arc<Mutex<ReliableSerialSession>>,
) {
    let mut state = state.lock().expect("MCU registry lock poisoned");
    if state.sessions.get(name).is_some_and(|current| Arc::ptr_eq(current, session)) {
        state.sessions.remove(name);
    }
}

impl McuRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn open_serial(
        &self,
        name: impl Into<String>,
        settings: &SerialSettings,
    ) -> Result<(), McuRegistryError> {
        let name = name.into();
        self.validate_name(&name)?;
        let generation = {
            let mut state = self.state.lock().expect("MCU registry lock poisoned");
            if state.sessions.contains_key(&name) || !state.opening.insert(name.clone()) {
                return Err(McuRegistryError::DuplicateName { name });
            }
            state.generation
        };

        let opened = ReliableSerialSession::open(settings);
        let mut state = self.state.lock().expect("MCU registry lock poisoned");
        // A clear/disconnect may have happened while the OS was opening the
        // device. Never publish a session into a newer printer lifecycle.
        if state.generation != generation {
            return match opened {
                Ok(_) => Err(McuRegistryError::OpenCancelled { name }),
                Err(error) => Err(McuRegistryError::OpenFailed {
                    name,
                    message: error.to_string(),
                }),
            };
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
        let name = name.into();
        self.validate_name(&name)?;
        let mut state = self.state.lock().expect("MCU registry lock poisoned");
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
        self.state
            .lock()
            .expect("MCU registry lock poisoned")
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
            Ok(mut guard) => Some(callback(&mut guard)),
            Err(poisoned) => {
                // A panicking callback may have left the session's protocol
                // state partially mutated. Treat it as terminal and remove
                // this exact handle from the registry before returning.
                let mut guard = poisoned.into_inner();
                guard.mark_disconnected();
                drop(guard);
                remove_session_if_same(&self.state, name, &session);
                None
            }
        }
    }

    pub fn remove_session(
        &self,
        name: &str,
    ) -> Option<Arc<Mutex<ReliableSerialSession>>> {
        self.state
            .lock()
            .expect("MCU registry lock poisoned")
            .sessions
            .remove(name)
    }

    pub fn clear(&self) -> Vec<String> {
        let mut state = self.state.lock().expect("MCU registry lock poisoned");
        let mut names: Vec<_> = state.sessions.keys().cloned().collect();
        names.sort();
        state.sessions.clear();
        state.opening.clear();
        state.generation = state.generation.wrapping_add(1);
        names
    }

    pub fn session_names(&self) -> Vec<String> {
        let mut names: Vec<_> = self
            .state
            .lock()
            .expect("MCU registry lock poisoned")
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

    /// Poll every registered MCU once, preserving each session's event order.
    /// A failed MCU is reported independently; other sessions are still polled.
    pub fn poll_all(&self) -> McuPollBatch {
        let mut sessions: Vec<_> = self
            .state
            .lock()
            .expect("MCU registry lock poisoned")
            .sessions
            .iter()
            .map(|(name, session)| (name.clone(), session.clone()))
            .collect();
        sessions.sort_by(|left, right| left.0.cmp(&right.0));

        let mut batch = McuPollBatch::default();
        for (name, session) in sessions {
            let mut session = match session.lock() {
                Ok(guard) => guard,
                Err(poisoned) => {
                    // Do not let one panicked session take down polling for
                    // every other MCU. Its state is no longer trustworthy.
                    let mut guard = poisoned.into_inner();
                    guard.mark_disconnected();
                    drop(guard);
                    remove_session_if_same(&self.state, &name, &session);
                    batch.errors.push(McuPollError {
                        mcu: name,
                        message: "MCU session lock poisoned after a panic".to_owned(),
                        disconnected: true,
                    });
                    continue;
                }
            };
            match session.poll() {
                Ok(events) => batch.events.extend(events.into_iter().map(|event| {
                    McuSessionEvent {
                        mcu: name.clone(),
                        event,
                    }
                })),
                Err(error) => batch.errors.push(McuPollError {
                    mcu: name,
                    message: error.to_string(),
                    disconnected: session.is_disconnected(),
                }),
            }
        }
        batch
    }

    fn validate_name(&self, name: &str) -> Result<(), McuRegistryError> {
        if name.trim().is_empty() {
            Err(McuRegistryError::EmptyName)
        } else {
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stale_poison_cleanup_does_not_remove_a_replacement_session() {
        let mut sessions = HashMap::new();
        let old = Arc::new(Mutex::new(1_u8));
        let replacement = Arc::new(Mutex::new(2_u8));
        sessions.insert("mcu".to_owned(), replacement.clone());

        if sessions.get("mcu").is_some_and(|current| Arc::ptr_eq(current, &old)) {
            sessions.remove("mcu");
        }

        assert!(Arc::ptr_eq(sessions.get("mcu").unwrap(), &replacement));
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
