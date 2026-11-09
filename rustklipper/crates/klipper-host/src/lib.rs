//! Core Rust Klipper host runtime.
//!
//! This crate owns printer lifecycle, typed object registration, configuration
//! ownership, and the host-side G-code dispatcher. MCU firmware is outside the
//! scope of this crate; communication is delegated to the protocol/transport
//! layers.

use std::any::Any;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard, RwLock};

use klipper_config::{ConfigDocument, ConfigFileLoader, ConfigLoadError};
use klipper_gcode::{DispatchOutcome, GCodeDispatcher};
use thiserror::Error;

type EventHandler = Arc<dyn Fn(&PrinterEvent) + Send + Sync>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PrinterState {
    Startup,
    Configuring,
    Ready,
    Shutdown,
    Disconnected,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PrinterEventKind {
    ConfigLoaded,
    Ready,
    Shutdown,
    Disconnect,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PrinterEvent {
    ConfigLoaded,
    Ready,
    Shutdown { reason: String },
    Disconnect,
}

impl PrinterEvent {
    pub fn kind(&self) -> PrinterEventKind {
        match self {
            Self::ConfigLoaded => PrinterEventKind::ConfigLoaded,
            Self::Ready => PrinterEventKind::Ready,
            Self::Shutdown { .. } => PrinterEventKind::Shutdown,
            Self::Disconnect => PrinterEventKind::Disconnect,
        }
    }
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum PrinterError {
    #[error("printer object '{name}' is already registered")]
    DuplicateObject { name: String },
    #[error("printer object '{name}' was not found")]
    ObjectNotFound { name: String },
    #[error("printer object '{name}' has a different concrete type")]
    ObjectTypeMismatch { name: String },
}

pub struct Printer {
    state: RwLock<PrinterState>,
    state_message: RwLock<String>,
    config: RwLock<Option<Arc<ConfigDocument>>>,
    objects: RwLock<HashMap<String, Arc<dyn Any + Send + Sync>>>,
    event_handlers: Mutex<HashMap<PrinterEventKind, Vec<EventHandler>>>,
    gcode: Mutex<GCodeDispatcher>,
}

impl Default for Printer {
    fn default() -> Self {
        Self::new()
    }
}

impl Printer {
    pub fn new() -> Self {
        Self {
            state: RwLock::new(PrinterState::Startup),
            state_message: RwLock::new("Printer is not ready".to_owned()),
            config: RwLock::new(None),
            objects: RwLock::new(HashMap::new()),
            event_handlers: Mutex::new(HashMap::new()),
            gcode: Mutex::new(GCodeDispatcher::new()),
        }
    }

    pub fn state(&self) -> PrinterState {
        *self.state.read().expect("printer state lock poisoned")
    }

    pub fn state_message(&self) -> String {
        self.state_message
            .read()
            .expect("printer state message lock poisoned")
            .clone()
    }

    /// Load a Klipper config file, recursively expand its includes, and
    /// install the resulting host configuration.
    pub fn load_config_file(
        &self,
        path: impl AsRef<std::path::Path>,
    ) -> Result<(), ConfigLoadError> {
        let config = ConfigFileLoader::load(path)?;
        self.install_config(config);
        Ok(())
    }

    /// Install a parsed host configuration and enter the configuring state.
    pub fn install_config(&self, config: ConfigDocument) {
        *self.config.write().expect("printer config lock poisoned") = Some(Arc::new(config));
        self.set_configuring();
        self.send_event(PrinterEvent::ConfigLoaded);
    }

    pub fn config(&self) -> Option<Arc<ConfigDocument>> {
        self.config
            .read()
            .expect("printer config lock poisoned")
            .clone()
    }

    pub fn set_configuring(&self) {
        *self.state.write().expect("printer state lock poisoned") = PrinterState::Configuring;
        *self.state_message.write().expect("printer state message lock poisoned") =
            "Printer is configuring".to_owned();
        self.gcode
            .lock()
            .expect("gcode dispatcher lock poisoned")
            .set_not_ready("Printer is not ready");
    }

    pub fn set_ready(&self) {
        *self.state.write().expect("printer state lock poisoned") = PrinterState::Ready;
        *self.state_message.write().expect("printer state message lock poisoned") =
            "Printer is ready".to_owned();
        self.gcode
            .lock()
            .expect("gcode dispatcher lock poisoned")
            .set_ready();
        self.send_event(PrinterEvent::Ready);
    }

    /// Enter shutdown. Klipper's shutdown event is emitted only if the printer
    /// had reached the ready state.
    pub fn invoke_shutdown(&self, reason: impl Into<String>) {
        let was_ready = self.state() == PrinterState::Ready;
        let reason = reason.into();
        *self.state.write().expect("printer state lock poisoned") = PrinterState::Shutdown;
        *self.state_message.write().expect("printer state message lock poisoned") =
            reason.clone();
        self.gcode
            .lock()
            .expect("gcode dispatcher lock poisoned")
            .set_shutdown(reason.clone());
        if was_ready {
            self.send_event(PrinterEvent::Shutdown { reason });
        }
    }

    pub fn disconnect(&self) {
        *self.state.write().expect("printer state lock poisoned") =
            PrinterState::Disconnected;
        *self.state_message.write().expect("printer state message lock poisoned") =
            "Printer is disconnected".to_owned();
        self.gcode
            .lock()
            .expect("gcode dispatcher lock poisoned")
            .set_disconnected();
        self.send_event(PrinterEvent::Disconnect);
    }

    /// Register a named host object. The registry retains shared ownership.
    pub fn register_object<T>(&self, name: impl Into<String>, object: T) -> Result<(), PrinterError>
    where
        T: Any + Send + Sync,
    {
        let name = name.into();
        let mut objects = self.objects.write().expect("printer object lock poisoned");
        if objects.contains_key(&name) {
            return Err(PrinterError::DuplicateObject { name });
        }
        objects.insert(name, Arc::new(object));
        Ok(())
    }

    pub fn lookup_object<T>(&self, name: &str) -> Result<Arc<T>, PrinterError>
    where
        T: Any + Send + Sync,
    {
        let object = self
            .objects
            .read()
            .expect("printer object lock poisoned")
            .get(name)
            .cloned()
            .ok_or_else(|| PrinterError::ObjectNotFound {
                name: name.to_owned(),
            })?;
        Arc::downcast::<T>(object).map_err(|_| PrinterError::ObjectTypeMismatch {
            name: name.to_owned(),
        })
    }

    pub fn remove_object(&self, name: &str) -> bool {
        self.objects
            .write()
            .expect("printer object lock poisoned")
            .remove(name)
            .is_some()
    }

    pub fn object_names(&self) -> Vec<String> {
        let mut names: Vec<_> = self
            .objects
            .read()
            .expect("printer object lock poisoned")
            .keys()
            .cloned()
            .collect();
        names.sort();
        names
    }

    pub fn register_event_handler<F>(&self, event: PrinterEventKind, handler: F)
    where
        F: Fn(&PrinterEvent) + Send + Sync + 'static,
    {
        self.event_handlers
            .lock()
            .expect("printer event handler lock poisoned")
            .entry(event)
            .or_default()
            .push(Arc::new(handler));
    }

    /// Dispatch an event without holding the handler registry lock while user
    /// callbacks execute, allowing handlers to register other callbacks.
    pub fn send_event(&self, event: PrinterEvent) {
        let handlers = self
            .event_handlers
            .lock()
            .expect("printer event handler lock poisoned")
            .get(&event.kind())
            .cloned()
            .unwrap_or_default();
        for handler in handlers {
            handler(&event);
        }
    }

    pub fn gcode_dispatcher(&self) -> MutexGuard<'_, GCodeDispatcher> {
        self.gcode.lock().expect("gcode dispatcher lock poisoned")
    }

    pub fn process_gcode_line(
        &self,
        line: &str,
        need_ack: bool,
    ) -> Option<DispatchOutcome> {
        self.gcode_dispatcher().process_line(line, need_ack)
    }

    pub fn process_gcode_script(&self, script: &str) -> Vec<DispatchOutcome> {
        self.gcode_dispatcher().process_script(script)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use klipper_gcode::DispatchEvent;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn lifecycle_controls_gcode_readiness() {
        let printer = Printer::new();
        printer
            .gcode_dispatcher()
            .register_command("STATUS", false, None, |_| {
                Ok(vec![DispatchEvent::Info("ready".to_owned())])
            })
            .unwrap();

        let before_ready = printer.process_gcode_line("STATUS", true).unwrap();
        assert!(matches!(before_ready.events.first(), Some(DispatchEvent::Error(_))));

        printer.set_ready();
        assert_eq!(printer.state(), PrinterState::Ready);
        let ready = printer.process_gcode_line("STATUS", true).unwrap();
        assert_eq!(ready.events[0], DispatchEvent::Info("ready".to_owned()));

        printer.invoke_shutdown("test shutdown");
        assert_eq!(printer.state(), PrinterState::Shutdown);
        let shutdown = printer.process_gcode_line("STATUS", true).unwrap();
        assert!(matches!(shutdown.events.first(), Some(DispatchEvent::Error(_))));
    }

    #[test]
    fn object_registry_is_typed_and_rejects_duplicates() {
        let printer = Printer::new();
        printer.register_object("counter", 42usize).unwrap();
        assert_eq!(*printer.lookup_object::<usize>("counter").unwrap(), 42);
        assert!(matches!(
            printer.lookup_object::<String>("counter"),
            Err(PrinterError::ObjectTypeMismatch { .. })
        ));
        assert!(matches!(
            printer.register_object("counter", 7usize),
            Err(PrinterError::DuplicateObject { .. })
        ));
        assert_eq!(printer.object_names(), vec!["counter".to_owned()]);
        assert!(printer.remove_object("counter"));
        assert!(matches!(
            printer.lookup_object::<usize>("counter"),
            Err(PrinterError::ObjectNotFound { .. })
        ));
    }

    #[test]
    fn config_install_emits_event_and_retains_document() {
        let printer = Printer::new();
        let events = Arc::new(AtomicUsize::new(0));
        let observed = events.clone();
        printer.register_event_handler(PrinterEventKind::ConfigLoaded, move |_| {
            observed.fetch_add(1, Ordering::SeqCst);
        });

        printer.install_config(ConfigDocument::parse("[printer]\nkinematics: cartesian\n").unwrap());
        assert_eq!(printer.state(), PrinterState::Configuring);
        assert_eq!(events.load(Ordering::SeqCst), 1);
        assert_eq!(
            printer.config().unwrap().section("printer").unwrap().get("kinematics"),
            Some("cartesian")
        );
    }


    #[test]
    fn config_file_loading_uses_recursive_config_loader() {
        let path = std::env::temp_dir().join(format!(
            "klipper-host-config-{}.cfg",
            std::process::id()
        ));
        std::fs::write(&path, "[printer]\\nkinematics: cartesian\\n").unwrap();

        let printer = Printer::new();
        printer.load_config_file(&path).unwrap();
        assert_eq!(printer.state(), PrinterState::Configuring);
        assert_eq!(
            printer.config().unwrap().section("printer").unwrap().get("kinematics"),
            Some("cartesian")
        );

        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn shutdown_event_only_fires_after_ready() {
        let printer = Printer::new();
        let events = Arc::new(AtomicUsize::new(0));
        let observed = events.clone();
        printer.register_event_handler(PrinterEventKind::Shutdown, move |_| {
            observed.fetch_add(1, Ordering::SeqCst);
        });

        printer.invoke_shutdown("startup failed");
        assert_eq!(events.load(Ordering::SeqCst), 0);
        printer.set_ready();
        printer.invoke_shutdown("runtime failure");
        assert_eq!(events.load(Ordering::SeqCst), 1);
    }
}
