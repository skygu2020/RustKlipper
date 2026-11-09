use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};

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
pub struct McuRegistry {
    sessions: RwLock<HashMap<String, Arc<Mutex<ReliableSerialSession>>>>,
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
        if self
            .sessions
            .read()
            .expect("MCU registry lock poisoned")
            .contains_key(&name)
        {
            return Err(McuRegistryError::DuplicateName { name });
        }

        let session = ReliableSerialSession::open(settings).map_err(|error| {
            McuRegistryError::OpenFailed {
                name: name.clone(),
                message: error.to_string(),
            }
        })?;
        self.register_session(name, session)
    }

    pub fn register_session(
        &self,
        name: impl Into<String>,
        session: ReliableSerialSession,
    ) -> Result<(), McuRegistryError> {
        let name = name.into();
        self.validate_name(&name)?;
        let mut sessions = self
            .sessions
            .write()
            .expect("MCU registry lock poisoned");
        if sessions.contains_key(&name) {
            return Err(McuRegistryError::DuplicateName { name });
        }
        sessions.insert(name, Arc::new(Mutex::new(session)));
        Ok(())
    }

    pub fn session_handle(
        &self,
        name: &str,
    ) -> Option<Arc<Mutex<ReliableSerialSession>>> {
        self.sessions
            .read()
            .expect("MCU registry lock poisoned")
            .get(name)
            .cloned()
    }

    pub fn with_session<R>(
        &self,
        name: &str,
        callback: impl FnOnce(&mut ReliableSerialSession) -> R,
    ) -> Option<R> {
        let session = self.session_handle(name)?;
        let mut session = session.lock().ok()?;
        Some(callback(&mut session))
    }

    pub fn remove_session(
        &self,
        name: &str,
    ) -> Option<Arc<Mutex<ReliableSerialSession>>> {
        self.sessions
            .write()
            .expect("MCU registry lock poisoned")
            .remove(name)
    }

    pub fn clear(&self) -> Vec<String> {
        let mut sessions = self
            .sessions
            .write()
            .expect("MCU registry lock poisoned");
        let mut names: Vec<_> = sessions.keys().cloned().collect();
        names.sort();
        sessions.clear();
        names
    }

    pub fn session_names(&self) -> Vec<String> {
        let mut names: Vec<_> = self
            .sessions
            .read()
            .expect("MCU registry lock poisoned")
            .keys()
            .cloned()
            .collect();
        names.sort();
        names
    }

    pub fn is_disconnected(&self, name: &str) -> Option<bool> {
        self.with_session(name, ReliableSerialSession::is_disconnected)
    }

    /// Poll every registered MCU once, preserving each session's event order.
    /// A failed MCU is reported independently; other sessions are still polled.
    pub fn poll_all(&self) -> McuPollBatch {
        let mut sessions: Vec<_> = self
            .sessions
            .read()
            .expect("MCU registry lock poisoned")
            .iter()
            .map(|(name, session)| (name.clone(), session.clone()))
            .collect();
        sessions.sort_by(|left, right| left.0.cmp(&right.0));

        let mut batch = McuPollBatch::default();
        for (name, session) in sessions {
            let mut session = session.lock().expect("MCU session lock poisoned");
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
    }
}
