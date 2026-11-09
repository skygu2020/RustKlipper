//! Core Rust Klipper host runtime.
//!
//! This crate owns printer lifecycle, typed object registration, configuration
//! ownership, and the host-side G-code dispatcher. MCU firmware is outside the
//! scope of this crate; communication is delegated to the protocol/transport
//! layers.

pub mod reactor;
pub use reactor::{Completion, CompletionCancelled, Reactor, ReactorError, TaskId};

use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex, MutexGuard, RwLock};

use klipper_config::{
    ConfigDocument, ConfigFileLoader, ConfigLoadError, ConfigUsageTracker,
    ConfigValidationError, ConfigValueError, ConfigWarning, UnusedConfigOption,
};
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PrinterStateCategory {
    Startup,
    Ready,
    Shutdown,
    Error,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrinterStatus {
    pub state: PrinterState,
    pub category: PrinterStateCategory,
    pub message: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PrinterEventKind {
    ConfigLoaded,
    McuIdentify,
    Connect,
    Ready,
    Shutdown,
    Disconnect,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PrinterEvent {
    ConfigLoaded,
    McuIdentify,
    Connect,
    Ready,
    Shutdown { reason: String },
    Disconnect,
}

impl PrinterEvent {
    pub fn kind(&self) -> PrinterEventKind {
        match self {
            Self::ConfigLoaded => PrinterEventKind::ConfigLoaded,
            Self::McuIdentify => PrinterEventKind::McuIdentify,
            Self::Connect => PrinterEventKind::Connect,
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

type ComponentFactory = Arc<
    dyn for<'a> Fn(
            &Printer,
            &klipper_config::TrackedConfigSection<'a>,
        ) -> Result<(), String>
        + Send
        + Sync,
>;

/// Registry of Rust host components keyed by their Klipper config section.
struct ComponentFactoryEntry {
    dependencies: Vec<String>,
    factory: ComponentFactory,
}

#[derive(Default)]
pub struct ComponentRegistry {
    factories: HashMap<String, ComponentFactoryEntry>,
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum ComponentError {
    #[error("configuration must be installed before loading components")]
    ConfigNotLoaded,
    #[error("component factory for section '{section}' is already registered")]
    DuplicateFactory { section: String },
    #[error("component section '{section}' requires missing dependency '{dependency}'")]
    MissingDependency { section: String, dependency: String },
    #[error("component dependency cycle detected at section '{section}'")]
    DependencyCycle { section: String },
    #[error("failed to load component section '{section}': {message}")]
    LoadFailed { section: String, message: String },
    #[error(transparent)]
    ConfigValidation(#[from] ConfigValidationError),
    #[error("{count} handler(s) panicked while dispatching {event:?}")]
    EventHandlerPanicked {
        event: PrinterEventKind,
        count: usize,
    },
}

impl ComponentRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register<F>(
        &mut self,
        section: impl Into<String>,
        factory: F,
    ) -> Result<(), ComponentError>
    where
        F: for<'a> Fn(
                &Printer,
                &klipper_config::TrackedConfigSection<'a>,
            ) -> Result<(), String>
            + Send
            + Sync
            + 'static,
    {
        self.register_with_dependencies(section, Vec::new(), factory)
    }

    pub fn register_with_dependencies<F>(
        &mut self,
        section: impl Into<String>,
        dependencies: Vec<String>,
        factory: F,
    ) -> Result<(), ComponentError>
    where
        F: for<'a> Fn(
                &Printer,
                &klipper_config::TrackedConfigSection<'a>,
            ) -> Result<(), String>
            + Send
            + Sync
            + 'static,
    {
        let section = section.into();
        if section.trim().is_empty() {
            return Err(ComponentError::LoadFailed {
                section,
                message: "section name cannot be empty".to_owned(),
            });
        }
        if self.factories.contains_key(&section) {
            return Err(ComponentError::DuplicateFactory { section });
        }
        if dependencies.iter().any(|dependency| dependency.trim().is_empty()) {
            return Err(ComponentError::LoadFailed {
                section,
                message: "dependency names cannot be empty".to_owned(),
            });
        }
        self.factories.insert(
            section,
            ComponentFactoryEntry {
                dependencies,
                factory: Arc::new(factory),
            },
        );
        Ok(())
    }

    fn load_section(
        &self,
        name: &str,
        config: &ConfigDocument,
        printer: &Printer,
        loaded: &mut Vec<String>,
        loaded_set: &mut HashSet<String>,
        loading: &mut HashSet<String>,
        requested_by: Option<&str>,
    ) -> Result<(), ComponentError> {
        if loaded_set.contains(name)
            || printer.is_component_loaded(name)
            || printer.has_object(name)
        {
            return Ok(());
        }
        let Some(entry) = self.factories.get(name) else {
            return Err(ComponentError::MissingDependency {
                section: requested_by.unwrap_or(name).to_owned(),
                dependency: name.to_owned(),
            });
        };
        if !loading.insert(name.to_owned()) {
            return Err(ComponentError::DependencyCycle {
                section: name.to_owned(),
            });
        }
        let Some(section) = config.section(name) else {
            loading.remove(name);
            return Err(ComponentError::MissingDependency {
                section: requested_by.unwrap_or(name).to_owned(),
                dependency: name.to_owned(),
            });
        };

        for dependency in &entry.dependencies {
            self.load_section(
                dependency,
                config,
                printer,
                loaded,
                loaded_set,
                loading,
                Some(name),
            )?;
        }

        let tracked = printer.config_usage.track_section(section);
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            (entry.factory)(printer, &tracked)
        }))
        .map_err(|_| ComponentError::LoadFailed {
            section: section.name.clone(),
            message: "component factory panicked".to_owned(),
        })?;
        result.map_err(|message| ComponentError::LoadFailed {
            section: section.name.clone(),
            message,
        })?;
        loading.remove(name);
        loaded_set.insert(name.to_owned());
        printer.mark_component_loaded(name);
        loaded.push(name.to_owned());
        Ok(())
    }

    /// Instantiate registered components in configuration-file order, loading
    /// declared dependencies first. Unregistered sections are left for later
    /// validation/component crates.
    pub fn load_registered(&self, printer: &Printer) -> Result<Vec<String>, ComponentError> {
        let config = printer.config().ok_or(ComponentError::ConfigNotLoaded)?;
        let mut loaded = Vec::new();
        let mut loaded_set = HashSet::new();
        let mut loading = HashSet::new();
        for section in config.sections() {
            if self.factories.contains_key(&section.name) {
                self.load_section(
                    &section.name,
                    &config,
                    printer,
                    &mut loaded,
                    &mut loaded_set,
                    &mut loading,
                    None,
                )?;
            }
        }
        Ok(loaded)
    }
}

pub struct Printer {
    state: RwLock<PrinterState>,
    state_message: RwLock<String>,
    config: RwLock<Option<Arc<ConfigDocument>>>,
    config_usage: ConfigUsageTracker,
    loaded_components: Mutex<HashSet<String>>,
    connection_initialized: AtomicBool,
    objects: RwLock<HashMap<String, Arc<dyn Any + Send + Sync>>>,
    event_handlers: Mutex<HashMap<PrinterEventKind, Vec<EventHandler>>>,
    gcode: Mutex<GCodeDispatcher>,
    reactor: Reactor,
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
            config_usage: ConfigUsageTracker::new(),
            loaded_components: Mutex::new(HashSet::new()),
            connection_initialized: AtomicBool::new(false),
            objects: RwLock::new(HashMap::new()),
            event_handlers: Mutex::new(HashMap::new()),
            gcode: Mutex::new(GCodeDispatcher::new()),
            reactor: Reactor::new(),
        }
    }

    pub fn state(&self) -> PrinterState {
        *self.state.read().expect("printer state lock poisoned")
    }

    pub fn is_shutdown(&self) -> bool {
        self.state() == PrinterState::Shutdown
    }

    pub fn is_ready(&self) -> bool {
        self.state() == PrinterState::Ready
    }

    pub fn state_message(&self) -> String {
        // Acquire locks in the same order as lifecycle transitions so callers
        // never observe a new state with the previous state's message.
        let _state = self.state.read().expect("printer state lock poisoned");
        self.state_message
            .read()
            .expect("printer state message lock poisoned")
            .clone()
    }

    /// Return the UI-facing state category alongside the current message.
    pub fn state_category(&self) -> PrinterStateCategory {
        match self.state() {
            PrinterState::Startup | PrinterState::Configuring => {
                PrinterStateCategory::Startup
            }
            PrinterState::Ready => PrinterStateCategory::Ready,
            PrinterState::Shutdown => PrinterStateCategory::Shutdown,
            PrinterState::Disconnected => PrinterStateCategory::Error,
        }
    }

    /// Read state, UI category, and message under one consistent lock snapshot.
    pub fn status(&self) -> PrinterStatus {
        let state = self.state.read().expect("printer state lock poisoned");
        let message = self
            .state_message
            .read()
            .expect("printer state message lock poisoned");
        let category = match *state {
            PrinterState::Startup | PrinterState::Configuring => {
                PrinterStateCategory::Startup
            }
            PrinterState::Ready => PrinterStateCategory::Ready,
            PrinterState::Shutdown => PrinterStateCategory::Shutdown,
            PrinterState::Disconnected => PrinterStateCategory::Error,
        };
        PrinterStatus {
            state: *state,
            category,
            message: message.clone(),
        }
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
        self.config_usage.clear();
        self.connection_initialized.store(false, std::sync::atomic::Ordering::SeqCst);
        self.loaded_components
            .lock()
            .expect("loaded component lock poisoned")
            .clear();
        self.objects
            .write()
            .expect("printer object lock poisoned")
            .clear();
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

    /// Read a config option and record that the host consumed it.
    pub fn config_value(&self, section: &str, option: &str) -> Option<String> {
        let config = self.config()?;
        self.config_usage
            .get(&config, section, option)
            .map(str::to_owned)
    }

    /// Read a config option with an optional default and explicit tracking policy.
    pub fn config_value_or(
        &self,
        section: &str,
        option: &str,
        default: Option<&str>,
        note_valid: bool,
    ) -> Option<String> {
        let config = self.config()?;
        self.config_usage
            .get_with_default(&config, section, option, default, note_valid)
            .map(str::to_owned)
    }

    pub fn config_required(
        &self,
        section: &str,
        option: &str,
    ) -> Result<String, ConfigValueError> {
        let config = self.config();
        let value = config
            .as_deref()
            .ok_or_else(|| ConfigValueError::Missing {
                section: section.to_owned(),
                option: option.to_owned(),
            })?;
        self.config_usage
            .get_required(value, section, option)
            .map(str::to_owned)
    }

    pub fn config_i64(
        &self,
        section: &str,
        option: &str,
    ) -> Result<i64, ConfigValueError> {
        let config = self.config();
        let config = config.as_deref().ok_or_else(|| ConfigValueError::Missing {
            section: section.to_owned(),
            option: option.to_owned(),
        })?;
        self.config_usage.get_i64(config, section, option)
    }

    pub fn config_f64(
        &self,
        section: &str,
        option: &str,
    ) -> Result<f64, ConfigValueError> {
        let config = self.config();
        let config = config.as_deref().ok_or_else(|| ConfigValueError::Missing {
            section: section.to_owned(),
            option: option.to_owned(),
        })?;
        self.config_usage.get_f64(config, section, option)
    }

    pub fn config_choice<T: Clone>(
        &self,
        section: &str,
        option: &str,
        choices: &[(String, T)],
        default: Option<&str>,
    ) -> Result<T, ConfigValueError> {
        let config = self.config();
        let config = config.as_deref().ok_or_else(|| ConfigValueError::Missing {
            section: section.to_owned(),
            option: option.to_owned(),
        })?;
        self.config_usage
            .get_choice(config, section, option, choices, default)
    }

    pub fn config_i64_range(
        &self,
        section: &str,
        option: &str,
        min: Option<i64>,
        max: Option<i64>,
    ) -> Result<i64, ConfigValueError> {
        let config = self.config();
        let config = config.as_deref().ok_or_else(|| ConfigValueError::Missing {
            section: section.to_owned(),
            option: option.to_owned(),
        })?;
        self.config_usage
            .get_i64_range(config, section, option, min, max)
    }

    pub fn config_f64_range(
        &self,
        section: &str,
        option: &str,
        min: Option<f64>,
        max: Option<f64>,
        above: Option<f64>,
        below: Option<f64>,
    ) -> Result<f64, ConfigValueError> {
        let config = self.config();
        let config = config.as_deref().ok_or_else(|| ConfigValueError::Missing {
            section: section.to_owned(),
            option: option.to_owned(),
        })?;
        self.config_usage
            .get_f64_range(config, section, option, min, max, above, below)
    }

    pub fn config_bool(
        &self,
        section: &str,
        option: &str,
    ) -> Result<bool, ConfigValueError> {
        let config = self.config();
        let config = config.as_deref().ok_or_else(|| ConfigValueError::Missing {
            section: section.to_owned(),
            option: option.to_owned(),
        })?;
        self.config_usage.get_bool(config, section, option)
    }

    pub fn config_list(
        &self,
        section: &str,
        option: &str,
        separator: char,
        count: Option<usize>,
    ) -> Result<Vec<String>, ConfigValueError> {
        let config = self.config();
        let config = config.as_deref().ok_or_else(|| ConfigValueError::Missing {
            section: section.to_owned(),
            option: option.to_owned(),
        })?;
        self.config_usage
            .get_list(config, section, option, separator, count)
    }

    pub fn config_i64_list(
        &self,
        section: &str,
        option: &str,
        separator: char,
        count: Option<usize>,
    ) -> Result<Vec<i64>, ConfigValueError> {
        let config = self.config();
        let config = config.as_deref().ok_or_else(|| ConfigValueError::Missing {
            section: section.to_owned(),
            option: option.to_owned(),
        })?;
        self.config_usage
            .get_i64_list(config, section, option, separator, count)
    }

    pub fn config_f64_list(
        &self,
        section: &str,
        option: &str,
        separator: char,
        count: Option<usize>,
    ) -> Result<Vec<f64>, ConfigValueError> {
        let config = self.config();
        let config = config.as_deref().ok_or_else(|| ConfigValueError::Missing {
            section: section.to_owned(),
            option: option.to_owned(),
        })?;
        self.config_usage
            .get_f64_list(config, section, option, separator, count)
    }

    /// Return options that have not yet been consumed by host components.
    pub fn unused_config_options(&self) -> Vec<UnusedConfigOption> {
        let Some(config) = self.config() else {
            return Vec::new();
        };
        self.config_usage.unused_options(&config)
    }

    /// Return deduplicated config deprecation warnings collected by components.
    pub fn config_warnings(&self) -> Vec<ConfigWarning> {
        self.config_usage.warnings()
    }

    /// Validate config sections and options after all registered components load.
    pub fn validate_config(&self) -> Result<(), ConfigValidationError> {
        let Some(config) = self.config() else {
            return Ok(());
        };
        self.config_usage
            .validate(&config, &self.object_names())
    }

    pub fn load_components(
        &self,
        registry: &ComponentRegistry,
    ) -> Result<Vec<String>, ComponentError> {
        registry.load_registered(self)
    }

    /// Load registered components and emit the ordered connection events.
    ///
    /// MCU identify and connect handlers run after configuration components
    /// have been constructed; the caller transitions to Ready after hardware
    /// initialization has completed.
    pub fn initialize_components(
        &self,
        registry: &ComponentRegistry,
    ) -> Result<Vec<String>, ComponentError> {
        if self.state() != PrinterState::Configuring
            || self
                .connection_initialized
                .compare_exchange(false, true, std::sync::atomic::Ordering::SeqCst, std::sync::atomic::Ordering::SeqCst)
                .is_err()
        {
            return Ok(Vec::new());
        }

        let result = (|| {
            let loaded = self.load_components(registry)?;
            self.validate_config()?;
            if self.state() != PrinterState::Configuring {
                return Ok(loaded);
            }
            let identify_panics = self.send_event(PrinterEvent::McuIdentify);
            if identify_panics > 0 {
                return Err(ComponentError::EventHandlerPanicked {
                    event: PrinterEventKind::McuIdentify,
                    count: identify_panics,
                });
            }
            if self.state() != PrinterState::Configuring {
                return Ok(loaded);
            }
            let connect_panics = self.send_event(PrinterEvent::Connect);
            if connect_panics > 0 {
                return Err(ComponentError::EventHandlerPanicked {
                    event: PrinterEventKind::Connect,
                    count: connect_panics,
                });
            }
            Ok(loaded)
        })();
        if result.is_err() {
            self.connection_initialized.store(false, std::sync::atomic::Ordering::SeqCst);
        }
        result
    }

    pub fn set_configuring(&self) {
        {
            let mut state = self.state.write().expect("printer state lock poisoned");
            if matches!(*state, PrinterState::Shutdown | PrinterState::Disconnected) {
                return;
            }
            *state = PrinterState::Configuring;
            *self
                .state_message
                .write()
                .expect("printer state message lock poisoned") =
                "Printer is configuring".to_owned();
        }
        self.gcode
            .lock()
            .expect("gcode dispatcher lock poisoned")
            .set_not_ready("Printer is not ready");
    }

    pub fn set_ready(&self) {
        {
            let mut state = self.state.write().expect("printer state lock poisoned");
            if matches!(
                *state,
                PrinterState::Ready | PrinterState::Shutdown | PrinterState::Disconnected
            ) {
                return;
            }
            *state = PrinterState::Ready;
            *self
                .state_message
                .write()
                .expect("printer state message lock poisoned") =
                "Printer is ready".to_owned();
        }
        self.gcode
            .lock()
            .expect("gcode dispatcher lock poisoned")
            .set_ready();
        self.send_event(PrinterEvent::Ready);
    }

    /// Enter shutdown. Klipper's shutdown event is emitted only if the printer
    /// had reached the ready state.
    pub fn invoke_shutdown(&self, reason: impl Into<String>) {
        let reason = reason.into();
        let was_ready = {
            let mut state = self.state.write().expect("printer state lock poisoned");
            if matches!(*state, PrinterState::Shutdown | PrinterState::Disconnected) {
                return;
            }
            let was_ready = *state == PrinterState::Ready;
            *state = PrinterState::Shutdown;
            *self
                .state_message
                .write()
                .expect("printer state message lock poisoned") = reason.clone();
            was_ready
        };
        self.gcode
            .lock()
            .expect("gcode dispatcher lock poisoned")
            .set_shutdown(reason.clone());
        if was_ready {
            self.send_event(PrinterEvent::Shutdown { reason });
        }
    }

    /// Request shutdown from any thread; the transition runs on the reactor.
    pub fn invoke_async_shutdown(
        self: &Arc<Self>,
        reason: impl Into<String>,
    ) -> TaskId {
        let printer = Arc::clone(self);
        let reason = reason.into();
        self.reactor.register_async_callback(move |_| {
            printer.invoke_shutdown(reason);
        })
    }

    pub fn disconnect(&self) {
        {
            let mut state = self.state.write().expect("printer state lock poisoned");
            if *state == PrinterState::Disconnected {
                return;
            }
            *state = PrinterState::Disconnected;
            *self
                .state_message
                .write()
                .expect("printer state message lock poisoned") =
                "Printer is disconnected".to_owned();
        }
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

    /// Look up an optional typed object without treating absence as an error.
    pub fn lookup_object_optional<T>(&self, name: &str) -> Result<Option<Arc<T>>, PrinterError>
    where
        T: Any + Send + Sync,
    {
        let object = self
            .objects
            .read()
            .expect("printer object lock poisoned")
            .get(name)
            .cloned();
        object
            .map(|object| {
                Arc::downcast::<T>(object).map_err(|_| PrinterError::ObjectTypeMismatch {
                    name: name.to_owned(),
                })
            })
            .transpose()
    }

    /// Return all objects, or the named object and its space-delimited children.
    ///
    /// The exact module object is returned first, followed by child objects in
    /// stable name order, matching Klipper's lookup_objects() traversal.
    pub fn lookup_objects(
        &self,
        module: Option<&str>,
    ) -> Vec<(String, Arc<dyn Any + Send + Sync>)> {
        let objects = self.objects.read().expect("printer object lock poisoned");
        let mut matches: Vec<_> = objects
            .iter()
            .filter(|(name, _)| {
                module.is_none_or(|module| {
                    name.as_str() == module
                        || name
                            .strip_prefix(module)
                            .is_some_and(|suffix| suffix.starts_with(' '))
                })
            })
            .map(|(name, object)| (name.clone(), Arc::clone(object)))
            .collect();
        matches.sort_by(|left, right| {
            let left_exact = module.is_some_and(|module| left.0 == module);
            let right_exact = module.is_some_and(|module| right.0 == module);
            right_exact.cmp(&left_exact).then_with(|| left.0.cmp(&right.0))
        });
        matches
    }

    fn is_component_loaded(&self, section: &str) -> bool {
        self.loaded_components
            .lock()
            .expect("loaded component lock poisoned")
            .contains(section)
    }

    fn mark_component_loaded(&self, section: &str) {
        self.loaded_components
            .lock()
            .expect("loaded component lock poisoned")
            .insert(section.to_owned());
    }

    pub fn has_object(&self, name: &str) -> bool {
        self.objects
            .read()
            .expect("printer object lock poisoned")
            .contains_key(name)
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
    ///
    /// A panicking handler is isolated from the remaining handlers. The return
    /// value is the number of handlers that panicked.
    pub fn send_event(&self, event: PrinterEvent) -> usize {
        let handlers = self
            .event_handlers
            .lock()
            .expect("printer event handler lock poisoned")
            .get(&event.kind())
            .cloned()
            .unwrap_or_default();
        handlers
            .into_iter()
            .filter(|handler| {
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| handler(&event)))
                    .is_err()
            })
            .count()
    }

    pub fn gcode_dispatcher(&self) -> MutexGuard<'_, GCodeDispatcher> {
        self.gcode.lock().expect("gcode dispatcher lock poisoned")
    }

    /// Access the cooperative host event scheduler.
    pub fn reactor(&self) -> &Reactor {
        &self.reactor
    }

    /// Run callbacks and timers whose deadlines have elapsed without waiting.
    pub fn run_reactor_once(&self) -> usize {
        self.reactor.run_due(std::time::Instant::now())
    }

    /// Wait for the next host callback or timer, bounded by max_wait.
    pub fn run_reactor(&self, max_wait: std::time::Duration) -> usize {
        self.reactor.run_once(max_wait)
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
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    #[test]
    fn configuring_transition_cannot_clear_shutdown() {
        let printer = Printer::new();
        printer.invoke_shutdown("startup failure");
        printer.set_configuring();
        assert_eq!(printer.state(), PrinterState::Shutdown);
        assert_eq!(printer.state_message(), "startup failure");
    }

    #[test]
    fn concurrent_status_reads_observe_matching_state_messages() {
        let printer = Arc::new(Printer::new());
        let start = Arc::new(std::sync::Barrier::new(2));
        let writer_printer = printer.clone();
        let writer_start = start.clone();
        let writer = std::thread::spawn(move || {
            writer_start.wait();
            for _ in 0..500 {
                writer_printer.set_configuring();
                writer_printer.set_ready();
                std::thread::yield_now();
            }
        });
        start.wait();
        for _ in 0..500 {
            let status = printer.status();
            std::thread::yield_now();
            match status.state {
                PrinterState::Startup => {
                    assert_eq!(status.category, PrinterStateCategory::Startup);
                    assert_eq!(status.message, "Printer is not ready");
                }
                PrinterState::Configuring => {
                    assert_eq!(status.category, PrinterStateCategory::Startup);
                    assert_eq!(status.message, "Printer is configuring");
                }
                PrinterState::Ready => {
                    assert_eq!(status.category, PrinterStateCategory::Ready);
                    assert_eq!(status.message, "Printer is ready");
                }
                PrinterState::Shutdown | PrinterState::Disconnected => {
                    panic!("unexpected terminal state in status snapshot test");
                }
            }
        }
        writer.join().unwrap();
    }

    #[test]
    fn printer_status_returns_a_consistent_snapshot() {
        let printer = Printer::new();
        printer.set_configuring();
        assert_eq!(
            printer.status(),
            PrinterStatus {
                state: PrinterState::Configuring,
                category: PrinterStateCategory::Startup,
                message: "Printer is configuring".to_owned(),
            }
        );
        printer.set_ready();
        assert_eq!(
            printer.status(),
            PrinterStatus {
                state: PrinterState::Ready,
                category: PrinterStateCategory::Ready,
                message: "Printer is ready".to_owned(),
            }
        );
    }

    #[test]
    fn printer_state_category_tracks_lifecycle() {
        let printer = Printer::new();
        assert_eq!(printer.state_category(), PrinterStateCategory::Startup);
        printer.set_configuring();
        assert_eq!(printer.state_category(), PrinterStateCategory::Startup);
        printer.set_ready();
        assert_eq!(printer.state_category(), PrinterStateCategory::Ready);
        assert!(printer.is_ready());
        printer.invoke_shutdown("failure");
        assert!(!printer.is_ready());
        assert_eq!(printer.state_category(), PrinterStateCategory::Shutdown);
        printer.disconnect();
        assert_eq!(printer.state_category(), PrinterStateCategory::Error);
        printer.set_ready();
        assert_eq!(printer.state(), PrinterState::Disconnected);
        assert!(!printer.is_ready());
    }

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
    fn object_registry_supports_optional_and_module_lookups() {
        let printer = Printer::new();
        printer.register_object("fan", 1usize).unwrap();
        printer.register_object("fan generic", 2usize).unwrap();
        printer.register_object("fan extra", 3usize).unwrap();
        printer.register_object("heater", 4usize).unwrap();

        assert!(printer.lookup_object_optional::<usize>("missing").unwrap().is_none());
        assert_eq!(
            *printer.lookup_object_optional::<usize>("fan").unwrap().unwrap(),
            1
        );
        let names: Vec<_> = printer
            .lookup_objects(Some("fan"))
            .into_iter()
            .map(|(name, _)| name)
            .collect();
        assert_eq!(
            names,
            vec![
                "fan".to_owned(),
                "fan extra".to_owned(),
                "fan generic".to_owned()
            ]
        );
        assert_eq!(printer.lookup_objects(None).len(), 4);
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
    fn printer_exposes_config_deprecation_warnings() {
        let printer = Printer::new();
        printer.install_config(
            ConfigDocument::parse("[printer]\nkinematics=cartesian\n").unwrap(),
        );
        let config = printer.config().unwrap();
        let section = printer
            .config_usage
            .track_section(config.section("printer").unwrap());
        section.deprecate("kinematics", None, None);
        assert_eq!(printer.config_warnings().len(), 1);
        printer.install_config(
            ConfigDocument::parse("[printer]\nkinematics=corexy\n").unwrap(),
        );
        assert!(printer.config_warnings().is_empty());
    }

    #[test]
    fn printer_validation_rejects_unknown_config_sections() {
        let printer = Printer::new();
        printer.install_config(
            ConfigDocument::parse("[unknown_section]\nvalue=1\n").unwrap(),
        );
        assert_eq!(
            printer.validate_config(),
            Err(ConfigValidationError::UnknownSection {
                section: "unknown_section".to_owned(),
            })
        );
    }

    #[test]
    fn printer_config_defaults_can_control_validation_tracking() {
        let tracked = Printer::new();
        tracked.install_config(ConfigDocument::parse("[printer]\n").unwrap());
        assert_eq!(
            tracked.config_value_or("printer", "optional", Some("fallback"), true),
            Some("fallback".to_owned())
        );
        assert!(tracked.validate_config().is_ok());

        let untracked = Printer::new();
        untracked.install_config(ConfigDocument::parse("[printer]\n").unwrap());
        assert_eq!(
            untracked.config_value_or("printer", "optional", Some("fallback"), false),
            Some("fallback".to_owned())
        );
        assert!(matches!(
            untracked.validate_config(),
            Err(ConfigValidationError::UnknownSection { .. })
        ));
    }

    #[test]
    fn printer_typed_config_getters_record_access() {
        let printer = Printer::new();
        printer.install_config(
            ConfigDocument::parse(
                "[printer]\nmax_velocity=300\nmax_accel=5000.5\nuse_pin=true\n",
            )
            .unwrap(),
        );
        assert_eq!(printer.config_i64("printer", "max_velocity").unwrap(), 300);
        assert_eq!(printer.config_f64("printer", "max_accel").unwrap(), 5000.5);
        assert!(printer.config_bool("printer", "use_pin").unwrap());
        assert!(printer.unused_config_options().is_empty());
    }

    #[test]
    fn printer_bounded_config_getters_record_access() {
        let printer = Printer::new();
        printer.install_config(
            ConfigDocument::parse(
                "[printer]\nmax_velocity=300\nmax_accel=5000.5\n",
            )
            .unwrap(),
        );
        assert_eq!(
            printer
                .config_i64_range("printer", "max_velocity", Some(100), Some(300))
                .unwrap(),
            300
        );
        assert_eq!(
            printer
                .config_f64_range("printer", "max_accel", Some(1000.0), None, Some(5000.0), None)
                .unwrap(),
            5000.5
        );
        assert!(printer.unused_config_options().is_empty());
    }

    #[test]
    fn printer_choice_config_getter_records_access() {
        let printer = Printer::new();
        printer.install_config(
            ConfigDocument::parse("[heater]\ncontrol=pid\n").unwrap(),
        );
        let choices = vec![
            ("pid".to_owned(), 1u8),
            ("watermark".to_owned(), 2u8),
        ];
        assert_eq!(printer.config_choice("heater", "control", &choices, None).unwrap(), 1);
        assert!(printer.unused_config_options().is_empty());
    }

    #[test]
    fn printer_list_config_getters_record_access() {
        let printer = Printer::new();
        printer.install_config(
            ConfigDocument::parse(
                "[stepper_x]\nposition=10,20,30\n[bed_mesh]\nmesh_min=0.5,1.25\n",
            )
            .unwrap(),
        );
        assert_eq!(
            printer
                .config_i64_list("stepper_x", "position", ',', Some(3))
                .unwrap(),
            vec![10, 20, 30]
        );
        assert_eq!(
            printer
                .config_f64_list("bed_mesh", "mesh_min", ',', Some(2))
                .unwrap(),
            vec![0.5, 1.25]
        );
        assert!(printer.unused_config_options().is_empty());
    }

    #[test]
    fn printer_tracks_consumed_config_options() {
        let printer = Printer::new();
        printer.install_config(
            ConfigDocument::parse("[printer]\nkinematics=cartesian\nmax_velocity=300\n")
                .unwrap(),
        );
        assert_eq!(
            printer.config_value("printer", "KINEMATICS").as_deref(),
            Some("cartesian")
        );
        let unused = printer.unused_config_options();
        assert_eq!(unused.len(), 1);
        assert_eq!(unused[0].option, "max_velocity");

        printer.install_config(
            ConfigDocument::parse("[printer]\nkinematics=corexy\n").unwrap(),
        );
        let unused_after_reload = printer.unused_config_options();
        assert_eq!(unused_after_reload.len(), 1);
        assert_eq!(unused_after_reload[0].option, "kinematics");
    }

    #[test]
    fn config_file_loading_uses_recursive_config_loader() {
        let path = std::env::temp_dir().join(format!(
            "klipper-host-config-{}.cfg",
            std::process::id()
        ));
        std::fs::write(&path, "[printer]\nkinematics: cartesian\n").unwrap();

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
    fn component_registry_loads_registered_sections_in_config_order() {
        let printer = Printer::new();
        printer.install_config(
            ConfigDocument::parse(
                "[fan]\npin: PA0\n[heater_generic hotend]\nmax_temp: 250\n",
            )
            .unwrap(),
        );

        let mut registry = ComponentRegistry::new();
        registry
            .register("fan", |printer, section| {
                let pin = section.get("pin").ok_or("missing pin")?.to_owned();
                printer.register_object("fan_pin", pin).map_err(|error| error.to_string())
            })
            .unwrap();
        registry
            .register("heater_generic hotend", |printer, section| {
                let max_temp = section.get("max_temp").ok_or("missing max_temp")?.to_owned();
                printer
                    .register_object("hotend_max_temp", max_temp)
                    .map_err(|error| error.to_string())
            })
            .unwrap();

        let loaded = printer.load_components(&registry).unwrap();
        assert_eq!(loaded, vec!["fan".to_owned(), "heater_generic hotend".to_owned()]);
        assert_eq!(&*printer.lookup_object::<String>("fan_pin").unwrap(), "PA0");
        assert_eq!(
            &*printer.lookup_object::<String>("hotend_max_temp").unwrap(),
            "250"
        );
        assert!(printer.unused_config_options().is_empty());
        assert!(printer.validate_config().is_ok());
    }

    #[test]
    fn connect_handler_panic_is_reported_after_other_handlers_run() {
        let printer = Printer::new();
        printer.install_config(ConfigDocument::parse("[fan]\npin=PA0\n").unwrap());
        let called = Arc::new(AtomicUsize::new(0));
        let observed = called.clone();
        printer.register_event_handler(PrinterEventKind::Connect, |_| {
            panic!("connect handler panic");
        });
        printer.register_event_handler(PrinterEventKind::Connect, move |_| {
            observed.fetch_add(1, Ordering::SeqCst);
        });
        let mut registry = ComponentRegistry::new();
        registry
            .register("fan", |printer, section| {
                printer
                    .register_object("fan", section.get("pin").unwrap().to_owned())
                    .map_err(|error| error.to_string())
            })
            .unwrap();

        assert!(matches!(
            printer.initialize_components(&registry),
            Err(ComponentError::EventHandlerPanicked {
                event: PrinterEventKind::Connect,
                count: 1,
            })
        ));
        assert_eq!(called.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn shutdown_during_identify_prevents_connect_event() {
        let printer = Arc::new(Printer::new());
        printer.install_config(ConfigDocument::parse("[fan]\npin=PA0\n").unwrap());
        let shutdown_printer = printer.clone();
        printer.register_event_handler(PrinterEventKind::McuIdentify, move |_| {
            shutdown_printer.invoke_shutdown("identify failed");
        });
        let connects = Arc::new(AtomicUsize::new(0));
        let observed = connects.clone();
        printer.register_event_handler(PrinterEventKind::Connect, move |_| {
            observed.fetch_add(1, Ordering::SeqCst);
        });
        let mut registry = ComponentRegistry::new();
        registry
            .register("fan", |printer, section| {
                printer
                    .register_object("fan", section.get("pin").unwrap().to_owned())
                    .map_err(|error| error.to_string())
            })
            .unwrap();

        assert_eq!(
            printer.initialize_components(&registry).unwrap(),
            vec!["fan".to_owned()]
        );
        assert_eq!(printer.state(), PrinterState::Shutdown);
        assert_eq!(connects.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn identify_handler_panic_prevents_connect_event() {
        let printer = Printer::new();
        printer.install_config(ConfigDocument::parse("[fan]\npin=PA0\n").unwrap());
        let connects = Arc::new(AtomicUsize::new(0));
        let observed = connects.clone();
        printer.register_event_handler(PrinterEventKind::McuIdentify, |_| {
            panic!("identify handler panic");
        });
        printer.register_event_handler(PrinterEventKind::Connect, move |_| {
            observed.fetch_add(1, Ordering::SeqCst);
        });
        let mut registry = ComponentRegistry::new();
        registry
            .register("fan", |printer, section| {
                printer
                    .register_object("fan", section.get("pin").unwrap().to_owned())
                    .map_err(|error| error.to_string())
            })
            .unwrap();

        assert!(matches!(
            printer.initialize_components(&registry),
            Err(ComponentError::EventHandlerPanicked {
                event: PrinterEventKind::McuIdentify,
                count: 1,
            })
        ));
        assert_eq!(connects.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn component_factory_panic_is_reported_as_load_failure() {
        let printer = Printer::new();
        printer.install_config(ConfigDocument::parse("[fan]\npin=PA0\n").unwrap());
        let mut registry = ComponentRegistry::new();
        registry
            .register("fan", |_, _| -> Result<(), String> {
                panic!("factory panic");
            })
            .unwrap();
        assert!(matches!(
            printer.load_components(&registry),
            Err(ComponentError::LoadFailed {
                message,
                ..
            }) if message == "component factory panicked"
        ));
    }

    #[test]
    fn component_factory_failure_prevents_connection_events() {
        let printer = Printer::new();
        printer.install_config(ConfigDocument::parse("[fan]\npin=PA0\n").unwrap());
        let events = Arc::new(AtomicUsize::new(0));
        let observed = events.clone();
        printer.register_event_handler(PrinterEventKind::Connect, move |_| {
            observed.fetch_add(1, Ordering::SeqCst);
        });
        let mut registry = ComponentRegistry::new();
        registry
            .register("fan", |_, _| Err("factory failed".to_owned()))
            .unwrap();

        assert!(matches!(
            printer.initialize_components(&registry),
            Err(ComponentError::LoadFailed { .. })
        ));
        assert_eq!(events.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn component_initialization_validates_before_connect_events() {
        let printer = Printer::new();
        printer.install_config(
            ConfigDocument::parse("[fan]\npin=PA0\nunknown=1\n").unwrap(),
        );
        let events = Arc::new(AtomicUsize::new(0));
        let observed = events.clone();
        printer.register_event_handler(PrinterEventKind::Connect, move |_| {
            observed.fetch_add(1, Ordering::SeqCst);
        });
        let mut registry = ComponentRegistry::new();
        registry
            .register("fan", |printer, section| {
                printer
                    .register_object("fan", section.get("pin").unwrap().to_owned())
                    .map_err(|error| error.to_string())
            })
            .unwrap();

        assert!(matches!(
            printer.initialize_components(&registry),
            Err(ComponentError::ConfigValidation(
                ConfigValidationError::UnknownOption { .. }
            ))
        ));
        assert_eq!(events.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn component_dependencies_load_before_config_ordered_consumers() {
        let printer = Printer::new();
        printer.install_config(
            ConfigDocument::parse(
                "[consumer]\nsource=provider\n[provider]\nvalue=ready\n",
            )
            .unwrap(),
        );
        let mut registry = ComponentRegistry::new();
        registry
            .register_with_dependencies(
                "consumer",
                vec!["provider".to_owned()],
                |printer, section| {
                    let provider = printer
                        .lookup_object::<String>("provider")
                        .map_err(|error| error.to_string())?;
                    printer
                        .register_object(
                            "consumer",
                            format!("{}:{}", provider.as_str(), section.get("source").unwrap()),
                        )
                        .map_err(|error| error.to_string())
                },
            )
            .unwrap();
        registry
            .register("provider", |printer, section| {
                printer
                    .register_object("provider", section.get("value").unwrap().to_owned())
                    .map_err(|error| error.to_string())
            })
            .unwrap();

        assert_eq!(
            printer.load_components(&registry).unwrap(),
            vec!["provider".to_owned(), "consumer".to_owned()]
        );
        assert_eq!(
            &*printer.lookup_object::<String>("consumer").unwrap(),
            "ready:provider"
        );
    }

    #[test]
    fn pre_registered_component_object_satisfies_dependency() {
        let printer = Printer::new();
        printer.install_config(
            ConfigDocument::parse("[consumer]\nsource=provider\n").unwrap(),
        );
        printer.register_object("provider", "preloaded".to_owned()).unwrap();
        let mut registry = ComponentRegistry::new();
        registry
            .register_with_dependencies(
                "consumer",
                vec!["provider".to_owned()],
                |printer, section| {
                    let provider = printer
                        .lookup_object::<String>("provider")
                        .map_err(|error| error.to_string())?;
                    printer
                        .register_object(
                            "consumer",
                            format!("{}:{}", provider.as_str(), section.get("source").unwrap()),
                        )
                        .map_err(|error| error.to_string())
                },
            )
            .unwrap();
        assert_eq!(
            printer.load_components(&registry).unwrap(),
            vec!["consumer".to_owned()]
        );
        assert_eq!(
            &*printer.lookup_object::<String>("consumer").unwrap(),
            "preloaded:provider"
        );
    }

    #[test]
    fn missing_component_dependency_is_reported_with_requester() {
        let printer = Printer::new();
        printer.install_config(
            ConfigDocument::parse("[consumer]\nsource=missing\n").unwrap(),
        );
        let mut registry = ComponentRegistry::new();
        registry
            .register_with_dependencies(
                "consumer",
                vec!["missing".to_owned()],
                |_, _| Ok(()),
            )
            .unwrap();
        assert!(matches!(
            printer.load_components(&registry),
            Err(ComponentError::MissingDependency {
                section,
                dependency,
            }) if section == "consumer" && dependency == "missing"
        ));
    }

    #[test]
    fn component_loading_is_cached_until_config_reinstall() {
        let printer = Printer::new();
        printer.install_config(ConfigDocument::parse("[fan]\npin=PA0\n").unwrap());
        let calls = Arc::new(AtomicUsize::new(0));
        let observed = calls.clone();
        let mut registry = ComponentRegistry::new();
        registry
            .register("fan", move |printer, section| {
                observed.fetch_add(1, Ordering::SeqCst);
                let pin = section.get("pin").unwrap().to_owned();
                printer
                    .register_object("fan", pin)
                    .map_err(|error| error.to_string())
            })
            .unwrap();

        assert_eq!(printer.load_components(&registry).unwrap(), vec!["fan"]);
        assert_eq!(&*printer.lookup_object::<String>("fan").unwrap(), "PA0");
        assert!(printer.load_components(&registry).unwrap().is_empty());
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        printer.install_config(ConfigDocument::parse("[fan]\npin=PB0\n").unwrap());
        assert!(!printer.has_object("fan"));
        assert_eq!(printer.load_components(&registry).unwrap(), vec!["fan"]);
        assert_eq!(&*printer.lookup_object::<String>("fan").unwrap(), "PB0");
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn shared_component_dependency_is_loaded_once() {
        let printer = Printer::new();
        printer.install_config(
            ConfigDocument::parse(
                "[first]\nsource=provider\n[second]\nsource=provider\n[provider]\nvalue=ready\n",
            )
            .unwrap(),
        );
        let provider_loads = Arc::new(AtomicUsize::new(0));
        let observed = provider_loads.clone();
        let mut registry = ComponentRegistry::new();
        for consumer in ["first", "second"] {
            registry
                .register_with_dependencies(
                    consumer,
                    vec!["provider".to_owned()],
                    |printer, section| {
                        let value = printer
                            .lookup_object::<String>("provider")
                            .map_err(|error| error.to_string())?;
                        printer
                            .register_object(
                                section.name(),
                                format!("{}:{}", value.as_str(), section.get("source").unwrap()),
                            )
                            .map_err(|error| error.to_string())
                    },
                )
                .unwrap();
        }
        registry
            .register("provider", move |printer, section| {
                observed.fetch_add(1, Ordering::SeqCst);
                printer
                    .register_object("provider", section.get("value").unwrap().to_owned())
                    .map_err(|error| error.to_string())
            })
            .unwrap();

        assert_eq!(
            printer.load_components(&registry).unwrap(),
            vec!["provider".to_owned(), "first".to_owned(), "second".to_owned()]
        );
        assert_eq!(provider_loads.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn component_registration_rejects_empty_dependency_names() {
        let mut registry = ComponentRegistry::new();
        assert!(matches!(
            registry.register_with_dependencies("fan", vec![" ".to_owned()], |_, _| Ok(())),
            Err(ComponentError::LoadFailed { message, .. })
                if message == "dependency names cannot be empty"
        ));
    }

    #[test]
    fn component_dependency_cycles_are_rejected() {
        let printer = Printer::new();
        printer.install_config(
            ConfigDocument::parse("[first]\nvalue=1\n[second]\nvalue=2\n").unwrap(),
        );
        let mut registry = ComponentRegistry::new();
        registry
            .register_with_dependencies(
                "first",
                vec!["second".to_owned()],
                |_, _| Ok(()),
            )
            .unwrap();
        registry
            .register_with_dependencies(
                "second",
                vec!["first".to_owned()],
                |_, _| Ok(()),
            )
            .unwrap();
        assert!(matches!(
            printer.load_components(&registry),
            Err(ComponentError::DependencyCycle { .. })
        ));
    }

    #[test]
    fn concurrent_component_initialization_runs_once() {
        let printer = Arc::new(Printer::new());
        printer.install_config(ConfigDocument::parse("[fan]\npin=PA0\n").unwrap());
        let calls = Arc::new(AtomicUsize::new(0));
        let observed_calls = calls.clone();
        let mut owned_registry = ComponentRegistry::new();
        owned_registry
            .register("fan", move |printer, section| {
                observed_calls.fetch_add(1, Ordering::SeqCst);
                printer
                    .register_object("fan", section.get("pin").unwrap().to_owned())
                    .map_err(|error| error.to_string())
            })
            .unwrap();
        let registry = Arc::new(owned_registry);
        let events = Arc::new(AtomicUsize::new(0));
        let observed_events = events.clone();
        printer.register_event_handler(PrinterEventKind::Connect, move |_| {
            observed_events.fetch_add(1, Ordering::SeqCst);
        });
        let start = Arc::new(std::sync::Barrier::new(3));
        let mut workers = Vec::new();
        for _ in 0..2 {
            let printer = printer.clone();
            let registry = registry.clone();
            let start = start.clone();
            workers.push(std::thread::spawn(move || {
                start.wait();
                printer.initialize_components(&registry).unwrap().len()
            }));
        }
        start.wait();
        let loaded: usize = workers.into_iter().map(|worker| worker.join().unwrap()).sum();
        assert_eq!(loaded, 1);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(events.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn failed_component_factory_is_retried_on_next_initialization() {
        let printer = Printer::new();
        printer.install_config(ConfigDocument::parse("[fan]\npin=PA0\n").unwrap());
        let calls = Arc::new(AtomicUsize::new(0));
        let observed_calls = calls.clone();
        let mut registry = ComponentRegistry::new();
        registry
            .register("fan", move |printer, section| {
                let call = observed_calls.fetch_add(1, Ordering::SeqCst);
                if call == 0 {
                    return Err("temporary factory failure".to_owned());
                }
                printer
                    .register_object("fan", section.get("pin").unwrap().to_owned())
                    .map_err(|error| error.to_string())
            })
            .unwrap();

        assert!(matches!(
            printer.initialize_components(&registry),
            Err(ComponentError::LoadFailed { .. })
        ));
        assert_eq!(printer.initialize_components(&registry).unwrap(), vec!["fan"]);
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert!(printer.has_object("fan"));
    }

    #[test]
    fn initialization_can_retry_after_connect_handler_panic() {
        let printer = Printer::new();
        printer.install_config(ConfigDocument::parse("[fan]\npin=PA0\n").unwrap());
        let factory_calls = Arc::new(AtomicUsize::new(0));
        let observed_factory_calls = factory_calls.clone();
        let mut registry = ComponentRegistry::new();
        registry
            .register("fan", move |printer, section| {
                observed_factory_calls.fetch_add(1, Ordering::SeqCst);
                printer
                    .register_object("fan", section.get("pin").unwrap().to_owned())
                    .map_err(|error| error.to_string())
            })
            .unwrap();

        let should_panic = Arc::new(AtomicBool::new(true));
        let panic_flag = should_panic.clone();
        let connect_calls = Arc::new(AtomicUsize::new(0));
        let observed_connect_calls = connect_calls.clone();
        printer.register_event_handler(PrinterEventKind::Connect, move |_| {
            if panic_flag.swap(false, Ordering::SeqCst) {
                panic!("first connect attempt fails");
            }
            observed_connect_calls.fetch_add(1, Ordering::SeqCst);
        });

        assert!(matches!(
            printer.initialize_components(&registry),
            Err(ComponentError::EventHandlerPanicked {
                event: PrinterEventKind::Connect,
                count: 1,
            })
        ));
        assert_eq!(printer.initialize_components(&registry).unwrap(), Vec::<String>::new());
        assert_eq!(factory_calls.load(Ordering::SeqCst), 1);
        assert_eq!(connect_calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn component_initialization_events_are_emitted_once() {
        let printer = Printer::new();
        printer.install_config(ConfigDocument::parse("[fan]\npin=PA0\n").unwrap());
        let identifies = Arc::new(AtomicUsize::new(0));
        let connects = Arc::new(AtomicUsize::new(0));
        let observed_identifies = identifies.clone();
        let observed_connects = connects.clone();
        printer.register_event_handler(PrinterEventKind::McuIdentify, move |_| {
            observed_identifies.fetch_add(1, Ordering::SeqCst);
        });
        printer.register_event_handler(PrinterEventKind::Connect, move |_| {
            observed_connects.fetch_add(1, Ordering::SeqCst);
        });
        let mut registry = ComponentRegistry::new();
        registry
            .register("fan", |printer, section| {
                printer
                    .register_object("fan", section.get("pin").unwrap().to_owned())
                    .map_err(|error| error.to_string())
            })
            .unwrap();

        assert_eq!(printer.initialize_components(&registry).unwrap(), vec!["fan"]);
        assert!(printer.initialize_components(&registry).unwrap().is_empty());
        assert_eq!(identifies.load(Ordering::SeqCst), 1);
        assert_eq!(connects.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn component_initialization_emits_identify_then_connect_events() {
        let printer = Printer::new();
        printer.install_config(ConfigDocument::parse("[fan]\npin=PA0\n").unwrap());
        let order = Arc::new(Mutex::new(Vec::new()));
        for (kind, value) in [
            (PrinterEventKind::McuIdentify, "identify"),
            (PrinterEventKind::Connect, "connect"),
        ] {
            let order = order.clone();
            printer.register_event_handler(kind, move |_| {
                order.lock().unwrap().push(value);
            });
        }
        let mut registry = ComponentRegistry::new();
        registry
            .register("fan", |printer, section| {
                printer
                    .register_object("fan", section.get("pin").unwrap().to_owned())
                    .map_err(|error| error.to_string())
            })
            .unwrap();

        assert_eq!(
            printer.initialize_components(&registry).unwrap(),
            vec!["fan".to_owned()]
        );
        assert_eq!(*order.lock().unwrap(), vec!["identify", "connect"]);
        assert_eq!(&*printer.lookup_object::<String>("fan").unwrap(), "PA0");
    }

    #[test]
    fn component_registry_requires_config_and_rejects_duplicate_factories() {
        let printer = Printer::new();
        let mut registry = ComponentRegistry::new();
        assert!(matches!(
            printer.load_components(&registry),
            Err(ComponentError::ConfigNotLoaded)
        ));
        registry.register("fan", |_, _| Ok(())).unwrap();
        assert!(matches!(
            registry.register("fan", |_, _| Ok(())),
            Err(ComponentError::DuplicateFactory { .. })
        ));
    }


    #[test]
    fn printer_blocking_reactor_dispatch_accepts_cross_thread_callbacks() {
        let printer = Arc::new(Printer::new());
        let dispatcher_printer = printer.clone();
        let dispatcher = std::thread::spawn(move || {
            dispatcher_printer.run_reactor(std::time::Duration::from_secs(2))
        });
        std::thread::sleep(std::time::Duration::from_millis(10));
        let called = Arc::new(AtomicUsize::new(0));
        let observed = called.clone();
        printer.reactor().register_async_callback(move |_| {
            observed.fetch_add(1, Ordering::SeqCst);
        });
        assert_eq!(dispatcher.join().unwrap(), 1);
        assert_eq!(called.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn printer_exposes_reactor_for_host_callbacks() {
        let printer = Printer::new();
        let called = Arc::new(AtomicUsize::new(0));
        let observed = called.clone();
        printer.reactor().register_callback(move || {
            observed.fetch_add(1, Ordering::SeqCst);
        });

        assert_eq!(printer.run_reactor_once(), 1);
        assert_eq!(called.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn event_handler_panic_does_not_prevent_later_handlers() {
        let printer = Printer::new();
        let called = Arc::new(AtomicUsize::new(0));
        printer.register_event_handler(PrinterEventKind::Ready, |_| {
            panic!("handler failure");
        });
        let observed = called.clone();
        printer.register_event_handler(PrinterEventKind::Ready, move |_| {
            observed.fetch_add(1, Ordering::SeqCst);
        });

        assert_eq!(printer.send_event(PrinterEvent::Ready), 1);
        assert_eq!(called.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn async_shutdown_is_dispatched_by_the_reactor() {
        let printer = Arc::new(Printer::new());
        let events = Arc::new(AtomicUsize::new(0));
        let observed = events.clone();
        printer.register_event_handler(PrinterEventKind::Shutdown, move |_| {
            observed.fetch_add(1, Ordering::SeqCst);
        });
        printer.set_ready();
        printer.invoke_async_shutdown("async failure");
        assert_eq!(printer.state(), PrinterState::Ready);
        assert_eq!(printer.run_reactor_once(), 1);
        assert_eq!(printer.state(), PrinterState::Shutdown);
        assert_eq!(printer.state_message(), "async failure");
        assert_eq!(events.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn shutdown_then_disconnect_emits_both_lifecycle_events() {
        let printer = Printer::new();
        let shutdowns = Arc::new(AtomicUsize::new(0));
        let disconnects = Arc::new(AtomicUsize::new(0));
        let observed_shutdowns = shutdowns.clone();
        let observed_disconnects = disconnects.clone();
        printer.register_event_handler(PrinterEventKind::Shutdown, move |_| {
            observed_shutdowns.fetch_add(1, Ordering::SeqCst);
        });
        printer.register_event_handler(PrinterEventKind::Disconnect, move |_| {
            observed_disconnects.fetch_add(1, Ordering::SeqCst);
        });

        printer.set_ready();
        printer.invoke_shutdown("runtime failure");
        printer.disconnect();
        printer.disconnect();

        assert_eq!(shutdowns.load(Ordering::SeqCst), 1);
        assert_eq!(disconnects.load(Ordering::SeqCst), 1);
        assert_eq!(printer.state(), PrinterState::Disconnected);
    }

    #[test]
    fn concurrent_disconnect_requests_emit_one_event() {
        let printer = Arc::new(Printer::new());
        let events = Arc::new(AtomicUsize::new(0));
        let observed = events.clone();
        printer.register_event_handler(PrinterEventKind::Disconnect, move |_| {
            observed.fetch_add(1, Ordering::SeqCst);
        });
        let workers: Vec<_> = (0..8)
            .map(|_| {
                let printer = printer.clone();
                std::thread::spawn(move || printer.disconnect())
            })
            .collect();
        for worker in workers {
            worker.join().unwrap();
        }
        assert_eq!(events.load(Ordering::SeqCst), 1);
        assert_eq!(printer.state(), PrinterState::Disconnected);
    }

    #[test]
    fn repeated_ready_transition_emits_one_event() {
        let printer = Printer::new();
        let events = Arc::new(AtomicUsize::new(0));
        let observed = events.clone();
        printer.register_event_handler(PrinterEventKind::Ready, move |_| {
            observed.fetch_add(1, Ordering::SeqCst);
        });
        printer.set_ready();
        printer.set_ready();
        assert_eq!(events.load(Ordering::SeqCst), 1);
        assert!(printer.is_ready());
    }

    #[test]
    fn repeated_disconnect_emits_one_event() {
        let printer = Printer::new();
        let events = Arc::new(AtomicUsize::new(0));
        let observed = events.clone();
        printer.register_event_handler(PrinterEventKind::Disconnect, move |_| {
            observed.fetch_add(1, Ordering::SeqCst);
        });
        printer.disconnect();
        printer.disconnect();
        assert_eq!(printer.state(), PrinterState::Disconnected);
        assert_eq!(events.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn shutdown_query_reflects_terminal_shutdown_state() {
        let printer = Printer::new();
        assert!(!printer.is_shutdown());
        printer.invoke_shutdown("failure");
        assert!(printer.is_shutdown());
        printer.disconnect();
        printer.invoke_shutdown("late shutdown");
        assert_eq!(printer.state(), PrinterState::Disconnected);
        assert!(!printer.is_shutdown());
    }

    #[test]
    fn ready_transition_cannot_race_shutdown_back_to_ready() {
        let printer = Arc::new(Printer::new());
        printer.set_configuring();
        let ready_printer = printer.clone();
        let shutdown_printer = printer.clone();
        let ready = std::thread::spawn(move || ready_printer.set_ready());
        let shutdown = std::thread::spawn(move || {
            shutdown_printer.invoke_shutdown("concurrent failure");
        });
        ready.join().unwrap();
        shutdown.join().unwrap();
        assert_eq!(printer.state(), PrinterState::Shutdown);
        assert!(printer.is_shutdown());
    }

    #[test]
    fn concurrent_shutdown_requests_emit_one_event() {
        let printer = Arc::new(Printer::new());
        printer.set_ready();
        let events = Arc::new(AtomicUsize::new(0));
        let observed = events.clone();
        printer.register_event_handler(PrinterEventKind::Shutdown, move |_| {
            observed.fetch_add(1, Ordering::SeqCst);
        });
        let workers: Vec<_> = (0..8)
            .map(|index| {
                let printer = printer.clone();
                std::thread::spawn(move || {
                    printer.invoke_shutdown(format!("failure {index}"));
                })
            })
            .collect();
        for worker in workers {
            worker.join().unwrap();
        }
        assert_eq!(events.load(Ordering::SeqCst), 1);
        assert_eq!(printer.state(), PrinterState::Shutdown);
    }

    #[test]
    fn repeated_shutdown_preserves_the_first_reason() {
        let printer = Printer::new();
        printer.invoke_shutdown("initial failure");
        printer.invoke_shutdown("secondary failure");
        assert_eq!(printer.state(), PrinterState::Shutdown);
        assert_eq!(printer.state_message(), "initial failure");
    }

    #[test]
    fn shutdown_event_only_fires_after_ready() {
        let startup = Printer::new();
        let events = Arc::new(AtomicUsize::new(0));
        let observed = events.clone();
        startup.register_event_handler(PrinterEventKind::Shutdown, move |_| {
            observed.fetch_add(1, Ordering::SeqCst);
        });

        startup.invoke_shutdown("startup failed");
        assert_eq!(events.load(Ordering::SeqCst), 0);
        startup.set_ready();
        assert_eq!(startup.state(), PrinterState::Shutdown);

        let runtime = Printer::new();
        let observed = events.clone();
        runtime.register_event_handler(PrinterEventKind::Shutdown, move |_| {
            observed.fetch_add(1, Ordering::SeqCst);
        });
        runtime.set_ready();
        runtime.invoke_shutdown("runtime failure");
        assert_eq!(events.load(Ordering::SeqCst), 1);
    }
}
