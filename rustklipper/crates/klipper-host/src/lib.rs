//! Core Rust Klipper host runtime.
//!
//! This crate owns printer lifecycle, typed object registration, configuration
//! ownership, and the host-side G-code dispatcher. MCU firmware is outside the
//! scope of this crate; communication is delegated to the protocol/transport
//! layers.

pub mod klippy_api;
pub mod mcu;
pub mod mcu_config;
pub use mcu_config::HostMcuConfigBuilder;
pub use mcu::{McuPollBatch, McuPollError, McuRegistry, McuRegistryError, McuSessionEvent};
pub use klipper_transport::McuStartupStage;
mod reactor;
pub use reactor::{Completion, CompletionCancelled, Reactor, ReactorError, TaskId};

use std::any::Any;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, RwLock};

use klipper_config::{
    ConfigDocument, ConfigFileLoader, ConfigLoadError, ConfigUsageTracker,
    ConfigValidationError, ConfigValueError, ConfigWarning, UnusedConfigOption,
};
use klipper_gcode::{DispatchEvent, DispatchOutcome, GCodeCommand, GCodeDispatcher, GCodeError};
use thiserror::Error;

type EventHandler = Arc<dyn Fn(&PrinterEvent) + Send + Sync>;
type McuEventHandler = Arc<dyn Fn(&McuSessionEvent) + Send + Sync>;
type McuMessageHandler = Arc<dyn Fn(&McuDecodedMessage) + Send + Sync>;

#[derive(Debug, Clone, PartialEq)]
pub struct McuDecodedMessage {
    pub mcu: String,
    pub name: String,
    pub params: std::collections::BTreeMap<String, serde_json::Value>,
    pub receive_time: f64,
}

struct McuMessageHandlerEntry {
    mcu: Option<String>,
    name: String,
    handler: McuMessageHandler,
}

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
    /// Startup phase snapshot for each currently registered MCU, sorted by name.
    pub mcu_startup_stages: Vec<(String, McuStartupStage)>,
}

/**
 * One cooperative Host runtime iteration.
 *
 * MCU polling is deliberately bounded by a short reactor wait so serial
 * sessions continue to make progress even when no reactor timer is pending.
 */
#[derive(Debug, Clone, Default, PartialEq)]
pub struct PrinterRunBatch {
    pub reactor_callbacks: usize,
    pub mcus: McuPollBatch,
    /// MCUs that completed the Host-provided configuration handshake.
    pub mcu_configured: Vec<McuConfigReady>,
    /// Startup coordinator failures isolated from the Host polling loop.
    pub mcu_startup_errors: Vec<McuStartupError>,
    /// Number of MCU event callbacks isolated after panicking.
    pub mcu_handler_panics: usize,
    /// Number of decoded-message callbacks isolated after panicking.
    pub mcu_message_handler_panics: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McuConfigReady {
    pub mcu: String,
    pub usable_move_count: u16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McuStartupError {
    pub mcu: String,
    pub message: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum McuClockWaitKind {
    Uptime,
    Clock,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct McuStartupQueueConfig {
    uptime_queue_id: u64,
    periodic_clock_queue_id: u64,
}

struct PendingMcuClockStartup {
    queue_id: u64,
    kind: McuClockWaitKind,
    waiter: klipper_transport::ResponseWaiter,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum McuConfigWaitKind {
    Initial,
    Final,
}

struct PendingMcuConfigStartup {
    queue_id: u64,
    firmware_restart: bool,
    kind: McuConfigWaitKind,
    handshake: klipper_transport::McuConfigHandshake,
    waiter: klipper_transport::ResponseWaiter,
}

const MCU_POLL_INTERVAL: std::time::Duration = std::time::Duration::from_millis(20);

fn bounded_host_wait(max_wait: std::time::Duration) -> std::time::Duration {
    max_wait.min(MCU_POLL_INTERVAL)
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
    #[error("component name cannot be empty")]
    EmptyComponentName,
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
    #[error("component initialization was cancelled by a lifecycle transition")]
    InitializationCancelled,
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

    /// Register a component with config-section or pre-registered-object dependencies.
    /// Dependencies are loaded recursively before this section's factory runs.
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
        let section = section.trim().to_owned();
        if self.factories.contains_key(&section) {
            return Err(ComponentError::DuplicateFactory { section });
        }
        if dependencies.iter().any(|dependency| dependency.trim().is_empty()) {
            return Err(ComponentError::LoadFailed {
                section,
                message: "dependency names cannot be empty".to_owned(),
            });
        }
        let dependencies = dependencies
            .into_iter()
            .map(|dependency| dependency.trim().to_owned())
            .collect();
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
        generation: Option<u64>,
    ) -> Result<(), ComponentError> {
        if generation.is_some_and(|generation| !printer.is_current_connection_generation(generation)) {
            return Err(ComponentError::InitializationCancelled);
        }
        if loaded_set.contains(name)
            || printer.is_component_loaded(name)
            // A pre-registered object may satisfy a dependency, but it must
            // not suppress the factory when this section is loaded directly.
            || (requested_by.is_some() && printer.has_object(name))
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
                generation,
            )?;
        }

        if generation.is_some_and(|generation| !printer.is_current_connection_generation(generation)) {
            loading.remove(name);
            return Err(ComponentError::InitializationCancelled);
        }

        let usage_transaction = printer.config_usage.begin_transaction();
        let tracked = printer.config_usage.track_section(section);
        let object_transaction = printer.begin_object_registration_transaction();
        let mcu_config_transaction = printer.begin_mcu_config_registration_transaction();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            (entry.factory)(printer, &tracked)
        }));
        if generation.is_some_and(|generation| !printer.is_current_connection_generation(generation)) {
            drop(usage_transaction);
            drop(object_transaction);
            drop(mcu_config_transaction);
            printer.remove_component_gcode_commands(name);
            printer.remove_component_mcu_message_handlers(name);
            loading.remove(name);
            return Err(ComponentError::InitializationCancelled);
        }
        let failure = match result {
            Ok(Ok(())) => None,
            Ok(Err(message)) => Some(message),
            Err(_) => Some("component factory panicked".to_owned()),
        };
        if let Some(message) = failure {
            drop(usage_transaction);
            drop(object_transaction);
            drop(mcu_config_transaction);
            printer.remove_component_gcode_commands(name);
            printer.remove_component_mcu_message_handlers(name);
            return Err(ComponentError::LoadFailed {
                section: section.name.clone(),
                message,
            });
        }
        usage_transaction.commit();
        object_transaction.commit();
        mcu_config_transaction.commit();
        loading.remove(name);
        loaded_set.insert(name.to_owned());
        printer.mark_component_loaded(name);
        loaded.push(name.to_owned());
        Ok(())
    }

    pub fn registered_sections(&self) -> Vec<String> {
        let mut names: Vec<_> = self.factories.keys().cloned().collect();
        names.sort();
        names
    }

    /// Lazily load one component and its dependencies.
    pub fn load_component(
        &self,
        printer: &Printer,
        section: &str,
    ) -> Result<Vec<String>, ComponentError> {
        let section = section.trim();
        let _load_guard = printer.lock_component_load();
        let config = printer.config().ok_or(ComponentError::ConfigNotLoaded)?;
        let mut loaded = Vec::new();
        let mut loaded_set = HashSet::new();
        let mut loading = HashSet::new();
        self.load_section(
            section,
            &config,
            printer,
            &mut loaded,
            &mut loaded_set,
            &mut loading,
            None,
            None,
        )?;
        Ok(loaded)
    }

    /// Instantiate registered components in configuration-file order, loading
    /// declared dependencies first. Unregistered sections are left for later
    /// validation/component crates.
    pub fn load_registered(&self, printer: &Printer) -> Result<Vec<String>, ComponentError> {
        self.load_registered_for_generation(printer, None)
    }

    fn load_registered_for_generation(
        &self,
        printer: &Printer,
        generation: Option<u64>,
    ) -> Result<Vec<String>, ComponentError> {
        let _load_guard = printer.lock_component_load();
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
                    generation,
                )?;
            }
        }
        Ok(loaded)
    }
}

struct McuConfigRegistrationTransaction<'a> {
    printer: &'a Printer,
    snapshot: BTreeMap<String, HostMcuConfigBuilder>,
    completed: bool,
}

impl McuConfigRegistrationTransaction<'_> {
    fn commit(mut self) {
        self.completed = true;
    }
}

impl Drop for McuConfigRegistrationTransaction<'_> {
    fn drop(&mut self) {
        if !self.completed {
            *self.printer.mcu_config_builders
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = self.snapshot.clone();
        }
    }
}

struct ObjectRegistrationTransaction<'a> {
    printer: &'a Printer,
    thread_id: std::thread::ThreadId,
    completed: bool,
}

impl ObjectRegistrationTransaction<'_> {
    fn commit(mut self) {
        self.printer.finish_object_registration_transaction(&self.thread_id, true);
        self.completed = true;
    }
}

impl Drop for ObjectRegistrationTransaction<'_> {
    fn drop(&mut self) {
        if !self.completed {
            self.printer.finish_object_registration_transaction(&self.thread_id, false);
        }
    }
}

pub struct Printer {
    // Monotonic origin used for Klipper-compatible eventtime values.
    start_time: std::time::Instant,
    // Serializes lifecycle transitions with G-code dispatcher snapshots.
    // Handlers are always invoked after this lock is released.
    lifecycle_lock: Mutex<()>,
    state: RwLock<PrinterState>,
    state_message: RwLock<String>,
    config: RwLock<Option<Arc<ConfigDocument>>>,
    config_usage: ConfigUsageTracker,
    loaded_components: Mutex<HashSet<String>>,
    component_load_lock: Mutex<()>,
    connection_initialized: AtomicBool,
    connection_generation: AtomicU64,
    mcus: McuRegistry,
    mcu_config_builders: Mutex<BTreeMap<String, HostMcuConfigBuilder>>,
    mcu_clock_startups: Mutex<HashMap<String, PendingMcuClockStartup>>,
    mcu_identify_startups: Mutex<HashMap<String, McuStartupQueueConfig>>,
    mcu_config_startups: Mutex<HashMap<String, PendingMcuConfigStartup>>,
    objects: RwLock<HashMap<String, Arc<dyn Any + Send + Sync>>>,
    object_registration_transactions: Mutex<
        HashMap<std::thread::ThreadId, Vec<Vec<(String, Arc<dyn Any + Send + Sync>)>>>,
    >,
    event_handlers: Mutex<HashMap<PrinterEventKind, Vec<EventHandler>>>,
    mcu_event_handlers: Mutex<Vec<McuEventHandler>>,
    next_mcu_message_handler_id: AtomicU64,
    mcu_message_handlers: Mutex<HashMap<u64, McuMessageHandlerEntry>>,
    component_mcu_message_handlers: Mutex<HashMap<String, HashSet<u64>>>,
    component_gcode: Mutex<HashMap<String, HashSet<String>>>,
    gcode: Mutex<GCodeDispatcher>,
    reactor: Reactor,
    status_updates: tokio::sync::broadcast::Sender<String>,
}

impl Default for Printer {
    fn default() -> Self {
        Self::new()
    }
}

impl Printer {
    pub fn new() -> Self {
        Self {
            start_time: std::time::Instant::now(),
            lifecycle_lock: Mutex::new(()),
            state: RwLock::new(PrinterState::Startup),
            state_message: RwLock::new("Printer is not ready".to_owned()),
            config: RwLock::new(None),
            config_usage: ConfigUsageTracker::new(),
            loaded_components: Mutex::new(HashSet::new()),
            component_load_lock: Mutex::new(()),
            connection_initialized: AtomicBool::new(false),
            connection_generation: AtomicU64::new(0),
            mcus: McuRegistry::new(),
            mcu_config_builders: Mutex::new(BTreeMap::new()),
            mcu_clock_startups: Mutex::new(HashMap::new()),
            mcu_identify_startups: Mutex::new(HashMap::new()),
            mcu_config_startups: Mutex::new(HashMap::new()),
            objects: RwLock::new(HashMap::new()),
            object_registration_transactions: Mutex::new(HashMap::new()),
            event_handlers: Mutex::new(HashMap::new()),
            mcu_event_handlers: Mutex::new(Vec::new()),
            next_mcu_message_handler_id: AtomicU64::new(1),
            mcu_message_handlers: Mutex::new(HashMap::new()),
            component_mcu_message_handlers: Mutex::new(HashMap::new()),
            component_gcode: Mutex::new(HashMap::new()),
            gcode: Mutex::new(GCodeDispatcher::new()),
            reactor: Reactor::new(),
            status_updates: tokio::sync::broadcast::channel(128).0,
        }
    }

    fn begin_mcu_config_registration_transaction(&self) -> McuConfigRegistrationTransaction<'_> {
        let snapshot = self.mcu_config_builders
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        McuConfigRegistrationTransaction {
            printer: self,
            snapshot,
            completed: false,
        }
    }

    /// Let a component contribute declarations to the shared builder for one MCU.
    /// Component factories run under the component-load lock, so contributions
    /// are serialized and receive globally unique OIDs within that MCU.
    pub fn with_mcu_config_builder<R>(
        &self,
        mcu: &str,
        contribute: impl FnOnce(&mut HostMcuConfigBuilder) -> R,
    ) -> R {
        let mut builders = self.mcu_config_builders
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        contribute(builders.entry(mcu.to_owned()).or_default())
    }

    /// Return an immutable snapshot of the declarations accumulated for an MCU.
    pub fn mcu_config_builder_snapshot(&self, mcu: &str) -> Option<HostMcuConfigBuilder> {
        self.mcu_config_builders
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(mcu)
            .cloned()
    }

    /// Discard all component-provided MCU declarations for a new config.
    fn clear_mcu_config_builders(&self) {
        self.mcu_config_builders
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clear();
    }

    fn lock_component_load(&self) -> MutexGuard<'_, ()> {
        self.component_load_lock
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn lock_object_registration_transactions(
        &self,
    ) -> MutexGuard<'_, HashMap<std::thread::ThreadId, Vec<Vec<(String, Arc<dyn Any + Send + Sync>)>>>> {
        self.object_registration_transactions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Seconds elapsed since this printer instance was created, using a
    /// monotonic clock suitable for Klipper status timestamps.
    pub fn eventtime(&self) -> f64 {
        self.start_time.elapsed().as_secs_f64()
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
        // Read MCU phases independently before taking printer-state locks; the
        // registry/session lock hierarchy must not be nested with lifecycle locks.
        let mcu_startup_stages = self.mcus.startup_stages();
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
            mcu_startup_stages,
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
        // Serialize configuration replacement with component factories so a
        // factory cannot register handlers or objects into a discarded config.
        let _load_guard = self.lock_component_load();
        // Invalidate any in-flight connection-stage callbacks before clearing
        // state owned by the previous configuration. Component initialization
        // may start concurrently, but it cannot pass the load lock until this
        // replacement (including MCU cleanup) is complete.
        let _lifecycle_guard = self.lifecycle_lock.lock()
            .expect("printer lifecycle lock poisoned");
        self.connection_generation.fetch_add(1, Ordering::SeqCst);
        self.connection_initialized.store(false, Ordering::SeqCst);
        self.config_usage.clear();
        self.clear_mcu_config_builders();
        self.clear_component_mcu_message_handlers();
        let mut component_gcode = self
            .component_gcode
            .lock()
            .expect("component G-code ownership lock poisoned");
        component_gcode.clear();
        self.gcode
            .lock()
            .expect("gcode dispatcher lock poisoned")
            .reset();
        self.loaded_components
            .lock()
            .expect("loaded component lock poisoned")
            .clear();
        self.objects
            .write()
            .expect("printer object lock poisoned")
            .clear();
        // Drop every internal registry guard before invoking event handlers.
        // ConfigLoaded callbacks may register component-owned G-code commands.
        drop(component_gcode);
        *self.config.write().expect("printer config lock poisoned") = Some(Arc::new(config));
        self.set_configuring_locked();
        drop(_lifecycle_guard);
        // MCU cleanup follows generation invalidation and state replacement,
        // but stays inside the component-load critical section so a new
        // initialization cannot publish sessions before the cleanup finishes.
        self.mcus.clear();
        // Event handlers may initialize or load components; release the
        // component load lock before invoking user callbacks.
        drop(_load_guard);
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
        let mut valid_sections = self.object_names();
        valid_sections.extend(self.loaded_component_names());
        self.config_usage.validate(&config, &valid_sections)
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
        let generation = {
            let _lifecycle_guard = self
                .lifecycle_lock
                .lock()
                .expect("printer lifecycle lock poisoned");
            if *self.state.read().expect("printer state lock poisoned")
                != PrinterState::Configuring
                || self
                    .connection_initialized
                    .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                    .is_err()
            {
                return Ok(Vec::new());
            }
            self.connection_generation.load(Ordering::SeqCst)
        };

        let result = (|| {
            let loaded = match registry.load_registered_for_generation(self, Some(generation)) {
                Ok(loaded) => loaded,
                Err(ComponentError::InitializationCancelled) => return Ok(Vec::new()),
                Err(error) => return Err(error),
            };
            if !self.is_current_connection_generation(generation) {
                // Component loading may itself trigger disconnect or config
                // replacement. The initialization attempt was cancelled, so
                // do not report its partial load as a completed connection.
                return Ok(Vec::new());
            }
            self.validate_config()?;
            let Some(identify_panics) =
                self.send_event_while_configuring(PrinterEvent::McuIdentify, generation)
            else {
                return Ok(loaded);
            };
            if identify_panics > 0 {
                return Err(ComponentError::EventHandlerPanicked {
                    event: PrinterEventKind::McuIdentify,
                    count: identify_panics,
                });
            }
            let Some(connect_panics) =
                self.send_event_while_configuring(PrinterEvent::Connect, generation)
            else {
                return Ok(loaded);
            };
            if connect_panics > 0 {
                return Err(ComponentError::EventHandlerPanicked {
                    event: PrinterEventKind::Connect,
                    count: connect_panics,
                });
            }
            Ok(loaded)
        })();
        if result.is_err()
            && self.connection_generation.load(Ordering::SeqCst) == generation
        {
            self.connection_initialized.store(false, Ordering::SeqCst);
        }
        result
    }

    pub fn set_configuring(&self) {
        let _lifecycle_guard = self.lifecycle_lock.lock()
            .expect("printer lifecycle lock poisoned");
        self.set_configuring_locked();
    }

    fn set_configuring_locked(&self) {
        {
            let mut state = self.state.write().expect("printer state lock poisoned");
            if matches!(*state, PrinterState::Shutdown | PrinterState::Disconnected) {
                return;
            }
            if *state != PrinterState::Configuring {
                // A new configuring phase is a new connection attempt. Invalidate
                // callbacks from the previous phase and allow initialization to run
                // again (for example after a Ready -> Configuring reconnect).
                self.connection_generation.fetch_add(1, Ordering::SeqCst);
                self.connection_initialized.store(false, Ordering::SeqCst);
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
        let _lifecycle_guard = self.lifecycle_lock.lock()
            .expect("printer lifecycle lock poisoned");
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
        drop(_lifecycle_guard);
        self.send_event(PrinterEvent::Ready);
    }

    /// Enter shutdown. Klipper's shutdown event is emitted only if the printer
    /// had reached the ready state.
    pub fn invoke_shutdown(&self, reason: impl Into<String>) {
        let reason = reason.into();
        let _lifecycle_guard = self.lifecycle_lock.lock()
            .expect("printer lifecycle lock poisoned");
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
        drop(_lifecycle_guard);
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
        let _lifecycle_guard = self.lifecycle_lock.lock()
            .expect("printer lifecycle lock poisoned");
        {
            let mut state = self.state.write().expect("printer state lock poisoned");
            if *state == PrinterState::Disconnected {
                return;
            }
            *state = PrinterState::Disconnected;
            // Invalidate every in-flight connection-stage operation before
            // releasing the lifecycle lock. A later transition back to a
            // configuring state must never authorize callbacks from this
            // disconnected connection attempt.
            self.connection_generation.fetch_add(1, Ordering::SeqCst);
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
        drop(_lifecycle_guard);
        self.mcus.clear();
        self.mcu_identify_startups.lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clear();
        self.mcu_clock_startups.lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clear();
        self.mcu_config_startups.lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clear();
        self.send_event(PrinterEvent::Disconnect);
    }

    fn begin_object_registration_transaction(&self) -> ObjectRegistrationTransaction<'_> {
        let thread_id = std::thread::current().id();
        self.lock_object_registration_transactions()
            .entry(thread_id)
            .or_default()
            .push(Vec::new());
        ObjectRegistrationTransaction {
            printer: self,
            thread_id,
            completed: false,
        }
    }

    fn finish_object_registration_transaction(
        &self,
        thread_id: &std::thread::ThreadId,
        commit: bool,
    ) {
        let registered = {
            let mut transactions = self.lock_object_registration_transactions();
            let Some(stack) = transactions.get_mut(thread_id) else {
                return;
            };
            let Some(registered) = stack.pop() else {
                return;
            };
            if commit {
                if let Some(parent) = stack.last_mut() {
                    parent.extend(registered);
                    return;
                }
            }
            if stack.is_empty() {
                transactions.remove(thread_id);
            }
            registered
        };

        if !commit {
            let mut objects = self.objects.write().expect("printer object lock poisoned");
            for (name, registered_object) in registered {
                if objects
                    .get(&name)
                    .is_some_and(|current| Arc::ptr_eq(current, &registered_object))
                {
                    objects.remove(&name);
                }
            }
        }
    }

    /// Register a named host object. The registry retains shared ownership.
    /// Objects registered synchronously by a component factory are tracked for
    /// rollback if that factory fails.
    pub fn register_object<T>(&self, name: impl Into<String>, object: T) -> Result<(), PrinterError>
    where
        T: Any + Send + Sync,
    {
        let name = name.into();
        let object: Arc<dyn Any + Send + Sync> = Arc::new(object);
        {
            let mut objects = self.objects.write().expect("printer object lock poisoned");
            if objects.contains_key(&name) {
                return Err(PrinterError::DuplicateObject { name });
            }
            objects.insert(name.clone(), object.clone());
        }
        let thread_id = std::thread::current().id();
        if let Some(transaction) = self
            .lock_object_registration_transactions()
            .get_mut(&thread_id)
            .and_then(|stack| stack.last_mut())
        {
            transaction.push((name, object));
        }
        Ok(())
    }

    /// Subscribe to names of status objects whose published fields change.
    ///
    /// Receivers are independent; a slow receiver may lag and should refresh
    /// its snapshot through `objects/query` after a lag notification.
    pub fn subscribe_status_updates(&self) -> tokio::sync::broadcast::Receiver<String> {
        self.status_updates.subscribe()
    }

    /// Register a JSON-backed status object for Klippy API queries.
    ///
    /// Components should expose externally visible fields through this API
    /// while keeping implementation objects registered with `register_object`.
    pub fn register_status_object(
        &self,
        name: impl Into<String>,
        fields: serde_json::Value,
    ) -> Result<(), PrinterError> {
        let name = name.into();
        if !fields.is_object() {
            return Err(PrinterError::ObjectTypeMismatch { name });
        }
        self.register_object(name.clone(), fields)?;
        let _ = self.status_updates.send(name);
        Ok(())
    }

    /// Atomically replace the fields exposed by a registered status object.
    pub fn update_status_object(
        &self,
        name: &str,
        fields: serde_json::Value,
    ) -> Result<(), PrinterError> {
        if !fields.is_object() {
            return Err(PrinterError::ObjectTypeMismatch { name: name.to_owned() });
        }
        let mut objects = self.objects.write().expect("printer object lock poisoned");
        let current = objects.get(name).cloned().ok_or_else(|| {
            PrinterError::ObjectNotFound { name: name.to_owned() }
        })?;
        if Arc::downcast::<serde_json::Value>(current).is_err() {
            return Err(PrinterError::ObjectTypeMismatch { name: name.to_owned() });
        }
        objects.insert(name.to_owned(), Arc::new(fields));
        let _ = self.status_updates.send(name.to_owned());
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

    pub fn is_component_loaded(&self, section: &str) -> bool {
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

    pub fn loaded_component_names(&self) -> Vec<String> {
        let mut names: Vec<_> = self
            .loaded_components
            .lock()
            .expect("loaded component lock poisoned")
            .iter()
            .cloned()
            .collect();
        names.sort();
        names
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

    /// Remove a JSON-backed status object and notify active Klippy subscribers.
    ///
    /// Internal Rust component objects are deliberately left to
    /// `remove_object`; this method only removes objects exposed by the
    /// status-object API. Subscribers querying explicit fields will observe
    /// those fields as null on their next update.
    pub fn remove_status_object(&self, name: &str) -> bool {
        let removed = {
            let mut objects = self.objects.write().expect("printer object lock poisoned");
            let Some(current) = objects.get(name).cloned() else {
                return false;
            };
            if Arc::downcast::<serde_json::Value>(current)
                .map(|value| value.is_object())
                .unwrap_or(false)
            {
                objects.remove(name).is_some()
            } else {
                false
            }
        };

        if removed {
            let _ = self.status_updates.send(name.to_owned());
        }
        removed
    }

    /// Unload a component's owned G-code commands and invalidate its load cache.
    /// Callers remain responsible for removing objects and dependent components.
    pub fn unload_component(&self, section: &str) -> bool {
        // Serialize unload with component factories and configuration
        // replacement so a concurrent load cannot publish a partially
        // unloaded component as successfully loaded.
        let section = section.trim();
        let _load_guard = self.lock_component_load();
        let removed_commands = self.remove_component_gcode_commands(section);
        let removed_mcu_handlers = self.remove_component_mcu_message_handlers(section);
        let removed_component = self
            .loaded_components
            .lock()
            .expect("loaded component lock poisoned")
            .remove(section);
        removed_commands || removed_mcu_handlers || removed_component
    }

    fn remove_component_gcode_commands(&self, component: &str) -> bool {
        let mut ownership = self
            .component_gcode
            .lock()
            .expect("component G-code ownership lock poisoned");
        let Some(commands) = ownership.remove(component) else {
            return false;
        };
        if commands.is_empty() {
            return false;
        }
        let mut dispatcher = self.gcode.lock().expect("gcode dispatcher lock poisoned");
        for command in commands {
            dispatcher.unregister_command(&command);
        }
        true
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

    /// Return only objects backed by JSON status data for the Klippy API.
    ///
    /// The general object registry also contains internal Rust component
    /// instances. Klipper's objects/list endpoint must not expose those
    /// implementation-only objects.
    pub fn status_object_names(&self) -> Vec<String> {
        let objects = self.objects.read().expect("printer object lock poisoned");
        let mut names: Vec<_> = objects
            .iter()
            .filter_map(|(name, object)| {
                Arc::downcast::<serde_json::Value>(object.clone())
                    .ok()
                    .filter(|value| value.is_object())
                    .map(|_| name.clone())
            })
            .collect();
        names.sort();
        names
    }

    fn lock_event_handlers(
        &self,
    ) -> MutexGuard<'_, HashMap<PrinterEventKind, Vec<EventHandler>>> {
        self.event_handlers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    pub fn register_event_handler<F>(&self, event: PrinterEventKind, handler: F)
    where
        F: Fn(&PrinterEvent) + Send + Sync + 'static,
    {
        self.lock_event_handlers()
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
        let handlers = self.event_handlers_for(event.kind());
        Self::dispatch_event_handlers(&event, handlers)
    }

    /// Atomically authorize a connection-stage event against the configuring
    /// state, then invoke its callbacks without holding the lifecycle lock.
    ///
    /// Taking the handler snapshot while holding the lifecycle lock gives this
    /// stage a clear ordering relative to shutdown/disconnect transitions:
    /// either the stage is authorized first, or the transition prevents it.
    /// Callbacks remain lock-free so they may themselves change printer state.
    fn is_current_connection_generation(&self, generation: u64) -> bool {
        let _lifecycle_guard = self
            .lifecycle_lock
            .lock()
            .expect("printer lifecycle lock poisoned");
        self.connection_generation.load(Ordering::SeqCst) == generation
            && *self.state.read().expect("printer state lock poisoned")
                == PrinterState::Configuring
    }

    fn send_event_while_configuring(
        &self,
        event: PrinterEvent,
        generation: u64,
    ) -> Option<usize> {
        let handlers = {
            let _lifecycle_guard = self
                .lifecycle_lock
                .lock()
                .expect("printer lifecycle lock poisoned");
            if self.connection_generation.load(Ordering::SeqCst) != generation
                || *self.state.read().expect("printer state lock poisoned")
                    != PrinterState::Configuring
            {
                return None;
            }
            self.event_handlers_for(event.kind())
        };
        Some(Self::dispatch_event_handlers(&event, handlers))
    }

    fn event_handlers_for(&self, kind: PrinterEventKind) -> Vec<EventHandler> {
        self.lock_event_handlers()
            .get(&kind)
            .cloned()
            .unwrap_or_default()
    }

    fn dispatch_event_handlers(
        event: &PrinterEvent,
        handlers: Vec<Arc<dyn Fn(&PrinterEvent) + Send + Sync>>,
    ) -> usize {
        handlers
            .into_iter()
            .filter(|handler| {
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| handler(event)))
                    .is_err()
            })
            .count()
    }

    /// Access the dispatcher for registration and inspection.
    ///
    /// Use process_gcode_line / process_gcode_script for execution so
    /// callbacks run after the printer's dispatcher lock has been released.
    pub fn gcode_dispatcher(&self) -> MutexGuard<'_, GCodeDispatcher> {
        self.gcode.lock().expect("gcode dispatcher lock poisoned")
    }

    pub fn register_gcode_command<F>(
        &self,
        command: &str,
        available_before_ready: bool,
        help: Option<&str>,
        handler: F,
    ) -> Result<(), GCodeError>
    where
        F: Fn(&GCodeCommand) -> Result<Vec<DispatchEvent>, GCodeError>
            + Send
            + Sync
            + 'static,
    {
        self.gcode
            .lock()
            .expect("gcode dispatcher lock poisoned")
            .register_command(command, available_before_ready, help, handler)
    }

    pub fn register_mux_gcode_command<F>(
        &self,
        command: &str,
        key: &str,
        value: Option<&str>,
        help: Option<&str>,
        handler: F,
    ) -> Result<(), GCodeError>
    where
        F: Fn(&GCodeCommand) -> Result<Vec<DispatchEvent>, GCodeError>
            + Send
            + Sync
            + 'static,
    {
        self.gcode
            .lock()
            .expect("gcode dispatcher lock poisoned")
            .register_mux_command(command, key, value, help, handler)
    }

    /// Register a command owned by one component and remove it automatically
    /// when that component is unloaded or configuration is reinstalled.
    /// Register a command owned by a component so failed loads and unloads
    /// can roll back the command together with the component.
    pub fn register_component_gcode_command<F>(
        &self,
        component: &str,
        command: &str,
        available_before_ready: bool,
        help: Option<&str>,
        handler: F,
    ) -> Result<(), GCodeError>
    where
        F: Fn(&GCodeCommand) -> Result<Vec<DispatchEvent>, GCodeError>
            + Send
            + Sync
            + 'static,
    {
        self.register_owned_gcode_name(component, command, |dispatcher| {
            dispatcher.register_command(command, available_before_ready, help, handler)
        })
    }

    /// Register one mux value for a component-owned command. A command name
    /// may belong to only one component, while that component may add values.
    pub fn register_component_mux_gcode_command<F>(
        &self,
        component: &str,
        command: &str,
        key: &str,
        value: Option<&str>,
        help: Option<&str>,
        handler: F,
    ) -> Result<(), GCodeError>
    where
        F: Fn(&GCodeCommand) -> Result<Vec<DispatchEvent>, GCodeError>
            + Send
            + Sync
            + 'static,
    {
        self.register_owned_gcode_name(component, command, |dispatcher| {
            dispatcher.register_mux_command(command, key, value, help, handler)
        })
    }

    fn register_owned_gcode_name<F>(
        &self,
        component: &str,
        command: &str,
        register: F,
    ) -> Result<(), GCodeError>
    where
        F: FnOnce(&mut GCodeDispatcher) -> Result<(), GCodeError>,
    {
        let component = component.trim();
        let command = command.trim();
        if component.is_empty() {
            return Err(GCodeError::MalformedCommand {
                line: "component name cannot be empty".to_owned(),
            });
        }
        let mut ownership = self
            .component_gcode
            .lock()
            .expect("component G-code ownership lock poisoned");
        if ownership
            .iter()
            .any(|(owner, commands)| owner != component && commands.contains(command))
        {
            return Err(GCodeError::MalformedCommand {
                line: format!("G-code command {command} is already owned by another component"),
            });
        }
        {
            let mut dispatcher = self.gcode.lock().expect("gcode dispatcher lock poisoned");
            register(&mut dispatcher)?;
        }
        ownership
            .entry(component.to_owned())
            .or_default()
            .insert(command.to_owned());
        Ok(())
    }

    pub fn unregister_gcode_command(&self, command: &str) -> bool {
        let command = command.trim();
        let mut ownership = self
            .component_gcode
            .lock()
            .expect("component G-code ownership lock poisoned");
        let removed = self
            .gcode
            .lock()
            .expect("gcode dispatcher lock poisoned")
            .unregister_command(command);
        for commands in ownership.values_mut() {
            commands.remove(command);
        }
        ownership.retain(|_, commands| !commands.is_empty());
        removed
    }

    /// Return the sorted command names currently active for this printer state.
    pub fn registered_gcode_commands(&self) -> Vec<String> {
        self.gcode_dispatcher().registered_commands()
    }

    /// Return help text for commands currently active for this printer state.
    pub fn gcode_help(&self) -> HashMap<String, String> {
        self.gcode_dispatcher().command_help()
    }

    /// Begin MCU dictionary discovery on one registered serial session.
    /// All subsequent identify chunks are requested by the transport session.
    pub fn start_mcu_identify(
        &self,
        name: &str,
        queue_id: u64,
        chunk_size: u8,
    ) -> Result<(), McuRegistryError> {
        self.mcus.start_identify(name, queue_id, chunk_size)
    }

    /// Start an automatically coordinated Identify -> uptime -> clock startup.
    ///
    /// The Host remembers the caller-selected queues and chunk size. Once the
    /// transport reports IdentifyProgress::Complete, run_host_once() starts
    /// uptime sampling and then advances the eight initial clock samples.
    pub fn start_managed_mcu_identify(
        &self,
        name: &str,
        identify_queue_id: u64,
        identify_chunk_size: u8,
        uptime_queue_id: u64,
        periodic_clock_queue_id: u64,
    ) -> Result<(), McuRegistryError> {
        let normalized = name.trim();
        if normalized.is_empty() {
            return Err(McuRegistryError::EmptyName);
        }
        {
            let mut pending = self.mcu_identify_startups.lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if pending.contains_key(normalized) {
                return Err(McuRegistryError::SessionOperation {
                    name: normalized.to_owned(),
                    message: "identify startup is already being coordinated".to_owned(),
                });
            }
            pending.insert(normalized.to_owned(), McuStartupQueueConfig {
                uptime_queue_id,
                periodic_clock_queue_id,
            });
        }
        if let Err(error) = self.mcus.start_identify(normalized, identify_queue_id, identify_chunk_size) {
            self.mcu_identify_startups.lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .remove(normalized);
            return Err(error);
        }
        Ok(())
    }

    /// Start MCU uptime sampling after Identify has completed.
    pub fn start_mcu_clock_calibration(
        &self,
        name: &str,
        queue_id: u64,
    ) -> Result<klipper_transport::ResponseWaiter, McuRegistryError> {
        self.mcus.start_clock_calibration(name, queue_id)
    }

    /// Begin sequential clock calibration from the MCU uptime response.
    pub fn begin_mcu_clock_calibration(
        &self,
        name: &str,
        queue_id: u64,
        uptime: klipper_transport::Response,
    ) -> Result<klipper_transport::ResponseWaiter, McuRegistryError> {
        self.mcus.begin_clock_calibration(name, queue_id, uptime)
    }

    /// Select the dedicated queue used for periodic MCU clock synchronization.
    pub fn set_mcu_periodic_clock_queue_id(
        &self,
        name: &str,
        queue_id: u64,
    ) -> Result<(), McuRegistryError> {
        self.mcus.set_periodic_clock_queue_id(name, queue_id)
    }

    /// Advance one of the eight startup clock samples for an MCU.
    /// The returned waiter represents the next sample, or None when calibrated.
    pub fn handle_mcu_clock_calibration_response(
        &self,
        name: &str,
        queue_id: u64,
        response: klipper_transport::Response,
    ) -> Result<Option<klipper_transport::ResponseWaiter>, McuRegistryError> {
        self.mcus.handle_clock_calibration_response(name, queue_id, response)
    }

    /// Start a Host-managed MCU clock bootstrap.
    ///
    /// The Host owns the uptime waiter and advances all eight get_clock samples
    /// from run_host_once(), so callers do not need to manually poll waiters.
    /// The periodic clock queue must be distinct from application/config queues.
    pub fn start_managed_mcu_clock_calibration(
        &self,
        name: &str,
        uptime_queue_id: u64,
        periodic_clock_queue_id: u64,
    ) -> Result<(), McuRegistryError> {
        let normalized = name.trim();
        if normalized.is_empty() {
            return Err(McuRegistryError::EmptyName);
        }
        let mut pending = self.mcu_clock_startups
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if pending.contains_key(normalized) {
            return Err(McuRegistryError::SessionOperation {
                name: normalized.to_owned(),
                message: "clock startup is already being coordinated".to_owned(),
            });
        }
        self.mcus.set_periodic_clock_queue_id(normalized, periodic_clock_queue_id)?;
        let waiter = self.mcus.start_clock_calibration(normalized, uptime_queue_id)?;
        pending.insert(normalized.to_owned(), PendingMcuClockStartup {
            queue_id: uptime_queue_id,
            kind: McuClockWaitKind::Uptime,
            waiter,
        });
        Ok(())
    }

    /// Start the two-query MCU config handshake using a plan built by the
    /// Host's configuration/component graph. This API deliberately accepts
    /// the completed handshake plan instead of inventing firmware commands.
    /// Call only after clock calibration has reached ClockReady.
    pub fn start_managed_mcu_config_handshake(
        &self,
        name: &str,
        queue_id: u64,
        handshake: klipper_transport::McuConfigHandshake,
        firmware_restart: bool,
    ) -> Result<(), McuRegistryError> {
        let normalized = name.trim();
        if normalized.is_empty() {
            return Err(McuRegistryError::EmptyName);
        }
        if handshake.stage() != klipper_transport::ConfigHandshakeStage::AwaitingInitialResponse {
            return Err(McuRegistryError::SessionOperation {
                name: normalized.to_owned(),
                message: "config handshake must be at AwaitingInitialResponse".to_owned(),
            });
        }
        let mut pending = self.mcu_config_startups.lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if pending.contains_key(normalized) {
            return Err(McuRegistryError::SessionOperation {
                name: normalized.to_owned(),
                message: "config handshake is already being coordinated".to_owned(),
            });
        }
        let waiter = self.mcus.with_session(normalized, |session| {
            session.begin_config_handshake(queue_id)
        });
        let waiter = match waiter {
            Some(Ok(waiter)) => waiter,
            Some(Err(error)) => return Err(McuRegistryError::SessionOperation {
                name: normalized.to_owned(), message: error.to_string(),
            }),
            None => return Err(McuRegistryError::SessionNotFound {
                name: normalized.to_owned(),
            }),
        };
        pending.insert(normalized.to_owned(), PendingMcuConfigStartup {
            queue_id,
            firmware_restart,
            kind: McuConfigWaitKind::Initial,
            handshake,
            waiter,
        });
        Ok(())
    }

    fn advance_mcu_config_startups(&self) -> (Vec<McuStartupError>, Vec<McuConfigReady>) {
        let names = {
            let pending = self.mcu_config_startups.lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let mut names: Vec<_> = pending.keys().cloned().collect();
            names.sort();
            names
        };
        let mut errors = Vec::new();
        let mut configured = Vec::new();
        for name in names {
            let Some(mut current) = self.mcu_config_startups.lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .remove(&name) else { continue; };
            let response = match current.waiter.try_recv() {
                Ok(Some(response)) => response,
                Ok(None) => {
                    self.mcu_config_startups.lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .insert(name, current);
                    continue;
                }
                Err(error) => {
                    errors.push(McuStartupError {
                        mcu: name,
                        message: format!("config response waiter failed: {error:?}"),
                    });
                    continue;
                }
            };
            match current.kind {
                McuConfigWaitKind::Initial => {
                    let result = self.mcus.with_session(&name, |session| {
                        session.advance_config_handshake(
                            current.queue_id,
                            &mut current.handshake,
                            response,
                            current.firmware_restart,
                        )
                    });
                    match result {
                        Some(Ok(waiter)) => {
                            current.kind = McuConfigWaitKind::Final;
                            current.waiter = waiter;
                            self.mcu_config_startups.lock()
                                .unwrap_or_else(std::sync::PoisonError::into_inner)
                                .insert(name, current);
                        }
                        Some(Err(error)) => errors.push(McuStartupError {
                            mcu: name, message: error.to_string(),
                        }),
                        None => errors.push(McuStartupError {
                            mcu: name.clone(), message: "MCU session disappeared during config handshake".to_owned(),
                        }),
                    }
                }
                McuConfigWaitKind::Final => {
                    let result = self.mcus.with_session(&name, |session| {
                        session.complete_config_handshake(&mut current.handshake, response)
                    });
                    match result {
                        Some(Ok(usable_move_count)) => configured.push(McuConfigReady {
                            mcu: name, usable_move_count,
                        }),
                        Some(Err(error)) => errors.push(McuStartupError {
                            mcu: name, message: error.to_string(),
                        }),
                        None => errors.push(McuStartupError {
                            mcu: name, message: "MCU session disappeared during config handshake".to_owned(),
                        }),
                    }
                }
            }
        }
        (errors, configured)
    }

    fn clear_mcu_startup_for(&self, name: &str) {
        self.mcu_identify_startups.lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(name);
        self.mcu_clock_startups.lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(name);
        self.mcu_config_startups.lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(name);
    }

    fn start_clock_calibration_for_identified_mcus(
        &self,
        events: &[McuSessionEvent],
    ) -> Vec<McuStartupError> {
        let mut errors = Vec::new();
        for event in events {
            if !matches!(
                &event.event,
                klipper_transport::SessionEvent::IdentifyProgress {
                    progress: klipper_transport::IdentifyProgress::Complete { .. }
                }
            ) {
                continue;
            }
            let config = self.mcu_identify_startups.lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .remove(&event.mcu);
            let Some(config) = config else { continue; };
            if let Err(error) = self.start_managed_mcu_clock_calibration(
                &event.mcu,
                config.uptime_queue_id,
                config.periodic_clock_queue_id,
            ) {
                errors.push(McuStartupError {
                    mcu: event.mcu.clone(),
                    message: error.to_string(),
                });
            }
        }
        errors
    }

    fn advance_mcu_clock_startups(&self) -> Vec<McuStartupError> {
        let names = {
            let pending = self.mcu_clock_startups
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let mut names: Vec<_> = pending.keys().cloned().collect();
            names.sort();
            names
        };
        let mut errors = Vec::new();
        for name in names {
            let Some(current) = self.mcu_clock_startups
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .remove(&name) else { continue; };
            let response = match current.waiter.try_recv() {
                Ok(Some(response)) => response,
                Ok(None) => {
                    self.mcu_clock_startups.lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .insert(name, current);
                    continue;
                }
                Err(error) => {
                    errors.push(McuStartupError { mcu: name, message: format!("clock response waiter failed: {error:?}") });
                    continue;
                }
            };
            let next = match current.kind {
                McuClockWaitKind::Uptime => self.mcus.begin_clock_calibration(
                    &name, current.queue_id, response,
                ).map(|waiter| Some(PendingMcuClockStartup {
                    queue_id: current.queue_id,
                    kind: McuClockWaitKind::Clock,
                    waiter,
                })),
                McuClockWaitKind::Clock => self.mcus.handle_clock_calibration_response(
                    &name, current.queue_id, response,
                ).map(|waiter| waiter.map(|waiter| PendingMcuClockStartup {
                    queue_id: current.queue_id,
                    kind: McuClockWaitKind::Clock,
                    waiter,
                })),
            };
            match next {
                Ok(Some(next)) => {
                    self.mcu_clock_startups.lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .insert(name, next);
                }
                Ok(None) => {}
                Err(error) => errors.push(McuStartupError { mcu: name, message: error.to_string() }),
            }
        }
        errors
    }

    /// Access the cooperative host event scheduler.
    pub fn mcus(&self) -> &McuRegistry {
        &self.mcus
    }

    pub fn poll_mcus(&self) -> McuPollBatch {
        self.mcus.poll_all()
    }

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

    /// Register a callback for every event emitted by any MCU session.
    ///
    /// Callbacks run after the session and registry locks have been released.
    /// Consumers can filter by MCU name and event variant.
    pub fn register_mcu_event_handler<F>(&self, handler: F)
    where
        F: Fn(&McuSessionEvent) + Send + Sync + 'static,
    {
        self.mcu_event_handlers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(Arc::new(handler));
    }

    /// Register a handler for one decoded MCU protocol message.
    ///
    /// Set mcu to None to receive the named message from every MCU. The
    /// returned ID can be passed to unregister_mcu_message_handler().
    pub fn register_mcu_message_handler<F>(
        &self,
        mcu: Option<&str>,
        message: impl Into<String>,
        handler: F,
    ) -> u64
    where
        F: Fn(&McuDecodedMessage) + Send + Sync + 'static,
    {
        let id = self.next_mcu_message_handler_id.fetch_add(1, Ordering::Relaxed);
        let entry = McuMessageHandlerEntry {
            mcu: mcu.map(str::trim).filter(|name| !name.is_empty()).map(str::to_owned),
            name: message.into(),
            handler: Arc::new(handler),
        };
        self.mcu_message_handlers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(id, entry);
        id
    }

    /// Register a decoded MCU message handler owned by a component.
    ///
    /// The handler is removed automatically if component initialization fails,
    /// when the component is unloaded, or when a new config is installed.
    pub fn register_component_mcu_message_handler<F>(
        &self,
        component: &str,
        mcu: Option<&str>,
        message: impl Into<String>,
        handler: F,
    ) -> Result<u64, PrinterError>
    where
        F: Fn(&McuDecodedMessage) + Send + Sync + 'static,
    {
        let component = component.trim();
        if component.is_empty() {
            return Err(PrinterError::EmptyComponentName);
        }
        let id = self.register_mcu_message_handler(mcu, message, handler);
        self.component_mcu_message_handlers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .entry(component.to_owned())
            .or_default()
            .insert(id);
        Ok(id)
    }

    fn remove_component_mcu_message_handlers(&self, component: &str) -> bool {
        let ids = self.component_mcu_message_handlers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(component);
        let Some(ids) = ids else {
            return false;
        };
        let mut handlers = self.mcu_message_handlers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for id in &ids {
            handlers.remove(id);
        }
        !ids.is_empty()
    }

    fn clear_component_mcu_message_handlers(&self) {
        let owned = std::mem::take(
            &mut *self.component_mcu_message_handlers
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
        );
        if owned.is_empty() {
            return;
        }
        let mut handlers = self.mcu_message_handlers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for id in owned.values().flatten() {
            handlers.remove(id);
        }
    }

    /// Remove a previously registered decoded-message handler.
    pub fn unregister_mcu_message_handler(&self, id: u64) -> bool {
        self.mcu_message_handlers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&id)
            .is_some()
    }

    fn dispatch_mcu_messages(&self, events: &[McuSessionEvent]) -> usize {
        let mut handlers: Vec<_> = self.mcu_message_handlers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .map(|(id, entry)| (*id, entry.mcu.clone(), entry.name.clone(), entry.handler.clone()))
            .collect();
        handlers.sort_by_key(|(id, _, _, _)| *id);
        let mut panics = 0;
        for event in events {
            let klipper_transport::SessionEvent::DecodedMessage {
                name, params, receive_time,
            } = &event.event else {
                continue;
            };
            let message = McuDecodedMessage {
                mcu: event.mcu.clone(),
                name: name.clone(),
                params: params.clone(),
                receive_time: *receive_time,
            };
            for (_, mcu, expected_name, handler) in &handlers {
                if expected_name == name && mcu.as_deref().map_or(true, |expected| expected == event.mcu.as_str()) {
                    if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| handler(&message))).is_err() {
                        panics += 1;
                    }
                }
            }
        }
        panics
    }

    fn dispatch_mcu_events(&self, events: &[McuSessionEvent]) -> usize {
        let handlers = self.mcu_event_handlers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        events.iter().map(|event| {
            handlers.iter().filter(|handler| {
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| handler(event)))
                    .is_err()
            }).count()
        }).sum()
    }

    /// Run one cooperative Host iteration.
    ///
    /// The reactor wait is capped so MCU serial sessions are polled regularly.
    /// All resulting MCU events are delivered to registered callbacks before
    /// the batch is returned. A panicking callback is isolated from the others.
    pub fn run_host_once(&self, max_wait: std::time::Duration) -> PrinterRunBatch {
        let reactor_wait = bounded_host_wait(max_wait);
        let reactor_callbacks = self.reactor.run_once(reactor_wait);
        let mcus = self.poll_mcus();
        for error in mcus.errors.iter().filter(|error| error.disconnected) {
            self.clear_mcu_startup_for(&error.mcu);
        }
        let mut mcu_startup_errors = self.start_clock_calibration_for_identified_mcus(&mcus.events);
        mcu_startup_errors.extend(self.advance_mcu_clock_startups());
        let (config_errors, mcu_configured) = self.advance_mcu_config_startups();
        mcu_startup_errors.extend(config_errors);
        let mcu_handler_panics = self.dispatch_mcu_events(&mcus.events);
        let mcu_message_handler_panics = self.dispatch_mcu_messages(&mcus.events);
        PrinterRunBatch {
            reactor_callbacks,
            mcus,
            mcu_configured,
            mcu_startup_errors,
            mcu_handler_panics,
            mcu_message_handler_panics,
        }
    }

    pub fn process_gcode_line(
        &self,
        line: &str,
        need_ack: bool,
    ) -> Option<DispatchOutcome> {
        // Snapshot state and dispatcher atomically with respect to lifecycle
        // transitions, then release the lock before executing user handlers.
        let dispatcher = {
            let _lifecycle_guard = self.lifecycle_lock.lock()
                .expect("printer lifecycle lock poisoned");
            self.gcode_dispatcher().clone()
        };
        let outcome = dispatcher.process_line(line, need_ack)?;
        if outcome.command == "M112" && outcome.handler_found {
            self.invoke_shutdown("Shutdown due to M112 command");
        }
        Some(outcome)
    }

    pub fn process_gcode_script(&self, script: &str) -> Vec<DispatchOutcome> {
        script
            .split('\n')
            .filter_map(|line| self.process_gcode_line(line, false))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use klipper_gcode::DispatchEvent;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    #[test]
    fn managed_mcu_clock_startup_reports_missing_session() {
        let printer = Printer::new();
        assert!(matches!(
            printer.start_managed_mcu_clock_calibration(" mcu ", 4, 5),
            Err(McuRegistryError::SessionNotFound { name }) if name == "mcu"
        ));
        assert!(printer.mcu_clock_startups.lock().unwrap().is_empty());
    }

    #[test]
    fn managed_identify_startup_rolls_back_configuration_without_session() {
        let printer = Printer::new();
        assert!(matches!(
            printer.start_managed_mcu_identify(" mcu ", 1, 64, 2, 3),
            Err(McuRegistryError::SessionNotFound { name }) if name == "mcu"
        ));
        assert!(printer.mcu_identify_startups.lock().unwrap().is_empty());
    }

    #[test]
    fn identify_completion_consumes_configuration_even_when_clock_start_fails() {
        let printer = Printer::new();
        printer.mcu_identify_startups.lock().unwrap().insert(
            "mcu".to_owned(),
            McuStartupQueueConfig { uptime_queue_id: 2, periodic_clock_queue_id: 3 },
        );
        let events = [McuSessionEvent {
            mcu: "mcu".to_owned(),
            event: klipper_transport::SessionEvent::IdentifyProgress {
                progress: klipper_transport::IdentifyProgress::Complete {
                    version: "test".to_owned(),
                },
            },
        }];
        let errors = printer.start_clock_calibration_for_identified_mcus(&events);
        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0].mcu, "mcu");
        assert!(printer.mcu_identify_startups.lock().unwrap().is_empty());
    }

    #[test]
    fn identify_completion_without_managed_configuration_is_ignored() {
        let printer = Printer::new();
        let events = [McuSessionEvent {
            mcu: "mcu".to_owned(),
            event: klipper_transport::SessionEvent::IdentifyProgress {
                progress: klipper_transport::IdentifyProgress::Complete {
                    version: "test".to_owned(),
                },
            },
        }];
        assert!(printer.start_clock_calibration_for_identified_mcus(&events).is_empty());
        assert!(printer.mcu_clock_startups.lock().unwrap().is_empty());
    }

    #[test]
    fn host_iteration_reports_no_errors_without_pending_mcu_startups() {
        let printer = Printer::new();
        let batch = printer.run_host_once(std::time::Duration::ZERO);
        assert!(batch.mcu_startup_errors.is_empty());
        assert!(batch.mcu_configured.is_empty());
    }

    #[test]
    fn failed_component_factory_rolls_back_mcu_config_contributions() {
        let printer = Printer::new();
        printer.install_config(ConfigDocument::parse("[heater_component]\n").unwrap());
        let mut registry = ComponentRegistry::new();
        registry.register("heater_component", |printer, _| {
            printer.with_mcu_config_builder("mcu", |builder| {
                assert_eq!(builder.allocate_oid("heater").unwrap(), 0);
                builder.add_config_command("config_heater oid=0");
            });
            Err("component initialization failed".to_owned())
        }).unwrap();

        assert!(matches!(
            printer.load_components(&registry),
            Err(ComponentError::LoadFailed { .. })
        ));
        assert!(printer.mcu_config_builder_snapshot("mcu").is_none());
    }

    #[test]
    fn successful_components_share_one_mcu_oid_allocator() {
        let printer = Printer::new();
        printer.install_config(
            ConfigDocument::parse("[heater_component]\n\n[fan_component]\n").unwrap(),
        );
        let mut registry = ComponentRegistry::new();
        registry.register("heater_component", |printer, _| {
            printer.with_mcu_config_builder("mcu", |builder| {
                assert_eq!(builder.allocate_oid("heater").unwrap(), 0);
                builder.add_config_command("config_heater oid=0");
            });
            Ok(())
        }).unwrap();
        registry.register("fan_component", |printer, _| {
            printer.with_mcu_config_builder("mcu", |builder| {
                assert_eq!(builder.allocate_oid("fan").unwrap(), 1);
                builder.add_config_command("config_fan oid=1");
            });
            Ok(())
        }).unwrap();

        printer.load_components(&registry).unwrap();
        let builder = printer.mcu_config_builder_snapshot("mcu").unwrap();
        assert_eq!(builder.oid_count(), 2);
        assert_eq!(builder.oid("heater"), Some(0));
        assert_eq!(builder.oid("fan"), Some(1));
        assert_eq!(
            builder.config_commands(),
            &["config_heater oid=0".to_owned(), "config_fan oid=1".to_owned()]
        );
    }

    #[test]
    fn failed_component_rolls_back_only_its_mcu_config_contributions() {
        let printer = Printer::new();
        printer.install_config(
            ConfigDocument::parse(
                "[heater_component]\n\n[fan_component]\n",
            )
            .unwrap(),
        );
        let mut registry = ComponentRegistry::new();
        registry
            .register("heater_component", |printer, _| {
                printer.with_mcu_config_builder("mcu", |builder| {
                    assert_eq!(builder.allocate_oid("heater").unwrap(), 0);
                    builder.add_config_command("config_heater oid=0");
                });
                Ok(())
            })
            .unwrap();
        registry
            .register("fan_component", |printer, _| {
                printer.with_mcu_config_builder("mcu", |builder| {
                    assert_eq!(builder.allocate_oid("fan").unwrap(), 1);
                    builder.add_config_command("config_fan oid=1");
                });
                Err("fan initialization failed".to_owned())
            })
            .unwrap();

        assert!(matches!(
            printer.load_components(&registry),
            Err(ComponentError::LoadFailed { .. })
        ));

        let builder = printer.mcu_config_builder_snapshot("mcu").unwrap();
        assert_eq!(builder.oid_count(), 1);
        assert_eq!(builder.oid("heater"), Some(0));
        assert_eq!(builder.oid("fan"), None);
        assert_eq!(
            builder.config_commands(),
            &["config_heater oid=0".to_owned()]
        );
    }

    #[test]
    fn panicking_component_rolls_back_only_its_mcu_config_contributions() {
        let printer = Printer::new();
        printer.install_config(
            ConfigDocument::parse(
                "[heater_component]\n\n[fan_component]\n",
            )
            .unwrap(),
        );
        let mut registry = ComponentRegistry::new();
        registry
            .register("heater_component", |printer, _| {
                printer.with_mcu_config_builder("mcu", |builder| {
                    assert_eq!(builder.allocate_oid("heater").unwrap(), 0);
                    builder.add_config_command("config_heater oid=0");
                });
                Ok(())
            })
            .unwrap();
        registry
            .register("fan_component", |printer, _| {
                printer.with_mcu_config_builder("mcu", |builder| {
                    assert_eq!(builder.allocate_oid("fan").unwrap(), 1);
                    builder.add_config_command("config_fan oid=1");
                });
                panic!("fan initialization panicked");
            })
            .unwrap();

        assert!(matches!(
            printer.load_components(&registry),
            Err(ComponentError::LoadFailed { .. })
        ));

        let builder = printer.mcu_config_builder_snapshot("mcu").unwrap();
        assert_eq!(builder.oid_count(), 1);
        assert_eq!(builder.oid("heater"), Some(0));
        assert_eq!(builder.oid("fan"), None);
        assert_eq!(
            builder.config_commands(),
            &["config_heater oid=0".to_owned()]
        );
    }

    #[test]
    fn installing_new_config_clears_component_mcu_config_contributions() {
        let printer = Printer::new();
        printer.with_mcu_config_builder("mcu", |builder| {
            builder.allocate_oid("heater").unwrap();
        });
        printer.install_config(ConfigDocument::parse("[printer]\n").unwrap());
        assert!(printer.mcu_config_builder_snapshot("mcu").is_none());
    }

    #[test]
    fn object_registration_transactions_recover_after_poisoning() {
        let printer = Printer::new();
        let poisoned = printer.object_registration_transactions.lock().unwrap();
        let panic_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = poisoned;
            panic!("poison object registration transaction lock");
        }));
        assert!(panic_result.is_err());

        let transaction = printer.begin_object_registration_transaction();
        printer.register_object("temporary", 7_u8).unwrap();
        drop(transaction);
        assert!(matches!(
            printer.lookup_object::<u8>("temporary"),
            Err(PrinterError::ObjectNotFound { .. })
        ));

        printer.register_object("permanent", 9_u8).unwrap();
        assert_eq!(*printer.lookup_object::<u8>("permanent").unwrap(), 9);
    }

    #[test]
    fn component_load_lock_recovers_after_poisoning() {
        use std::panic::{catch_unwind, AssertUnwindSafe};

        let printer = Printer::new();
        let _ = catch_unwind(AssertUnwindSafe(|| {
            let _guard = printer.component_load_lock.lock().unwrap();
            panic!("poison component load lock");
        }));

        let _guard = printer.lock_component_load();
        assert!(printer.config().is_none());
    }

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
                mcu_startup_stages: Vec::new(),
            }
        );
        printer.set_ready();
        assert_eq!(
            printer.status(),
            PrinterStatus {
                state: PrinterState::Ready,
                category: PrinterStateCategory::Ready,
                message: "Printer is ready".to_owned(),
                mcu_startup_stages: Vec::new(),
            }
        );
    }

    #[test]
    fn printer_owns_an_empty_mcu_registry_at_startup() {
        let printer = Printer::new();
        assert!(printer.mcus().session_names().is_empty());
        assert_eq!(printer.mcus().poll_all(), McuPollBatch::default());
        assert_eq!(printer.poll_mcus(), McuPollBatch::default());
    }

    #[test]
    fn host_iteration_runs_due_reactor_callbacks_and_polls_mcus() {
        let printer = Printer::new();
        let called = Arc::new(AtomicBool::new(false));
        let observed = called.clone();
        printer.reactor().register_callback(move || {
            observed.store(true, Ordering::SeqCst);
        });

        let batch = printer.run_host_once(std::time::Duration::ZERO);

        assert_eq!(batch.reactor_callbacks, 1);
        assert!(called.load(Ordering::SeqCst));
        assert_eq!(batch.mcus, McuPollBatch::default());
    }

    #[test]
    fn host_iteration_bounds_wait_to_keep_mcu_polling_regular() {
        assert_eq!(
            bounded_host_wait(std::time::Duration::from_millis(100)),
            std::time::Duration::from_millis(20)
        );
        assert_eq!(
            bounded_host_wait(std::time::Duration::from_millis(5)),
            std::time::Duration::from_millis(5)
        );
        assert_eq!(
            bounded_host_wait(std::time::Duration::ZERO),
            std::time::Duration::ZERO
        );
    }


    #[test]
    fn unloading_component_removes_its_mcu_message_handlers() {
        let printer = Printer::new();
        let called = Arc::new(AtomicBool::new(false));
        let observed = called.clone();
        printer.register_component_mcu_message_handler("heater", None, "sensor_update", move |_| {
            observed.store(true, Ordering::SeqCst);
        }).unwrap();

        assert!(printer.unload_component("heater"));
        let events = vec![McuSessionEvent {
            mcu: "mcu".into(),
            event: klipper_transport::SessionEvent::DecodedMessage {
                name: "sensor_update".into(),
                params: std::collections::BTreeMap::new(),
                receive_time: 1.0,
            },
        }];
        assert_eq!(printer.dispatch_mcu_messages(&events), 0);
        assert!(!called.load(Ordering::SeqCst));
    }

    #[test]
    fn failed_component_factory_rolls_back_mcu_message_handlers() {
        let printer = Printer::new();
        printer.install_config(ConfigDocument::parse("[sensor_component]\n").unwrap());
        let called = Arc::new(AtomicBool::new(false));
        let observed = called.clone();
        let mut registry = ComponentRegistry::new();
        registry.register("sensor_component", move |printer, _| {
            printer.register_component_mcu_message_handler(
                "sensor_component", None, "sensor_update", {
                    let observed = observed.clone();
                    move |_| observed.store(true, Ordering::SeqCst)
                },
            ).map_err(|error| error.to_string())?;
            Err("component initialization failed".to_owned())
        }).unwrap();

        assert!(matches!(printer.load_components(&registry), Err(ComponentError::LoadFailed { .. })));
        let events = vec![McuSessionEvent {
            mcu: "mcu".into(),
            event: klipper_transport::SessionEvent::DecodedMessage {
                name: "sensor_update".into(),
                params: std::collections::BTreeMap::new(),
                receive_time: 1.0,
            },
        }];
        assert_eq!(printer.dispatch_mcu_messages(&events), 0);
        assert!(!called.load(Ordering::SeqCst));
    }

    #[test]
    fn config_reinstall_clears_component_handlers_but_keeps_global_handlers() {
        let printer = Printer::new();
        let component_called = Arc::new(AtomicBool::new(false));
        let component_observed = component_called.clone();
        printer.register_component_mcu_message_handler("sensor", None, "sensor_update", move |_| {
            component_observed.store(true, Ordering::SeqCst);
        }).unwrap();
        let global_called = Arc::new(AtomicBool::new(false));
        let global_observed = global_called.clone();
        printer.register_mcu_message_handler(None, "sensor_update", move |_| {
            global_observed.store(true, Ordering::SeqCst);
        });

        printer.install_config(ConfigDocument::parse("[sensor]\n").unwrap());
        let events = vec![McuSessionEvent {
            mcu: "mcu".into(),
            event: klipper_transport::SessionEvent::DecodedMessage {
                name: "sensor_update".into(),
                params: std::collections::BTreeMap::new(),
                receive_time: 1.0,
            },
        }];
        assert_eq!(printer.dispatch_mcu_messages(&events), 0);
        assert!(!component_called.load(Ordering::SeqCst));
        assert!(global_called.load(Ordering::SeqCst));
    }

    #[test]
    fn decoded_messages_route_by_mcu_and_protocol_name() {
        let printer = Printer::new();
        let received = Arc::new(Mutex::new(Vec::new()));
        let specific = received.clone();
        printer.register_mcu_message_handler(Some("mcu0"), "status", move |message| {
            specific.lock().unwrap().push(format!("{}:{}", message.mcu, message.name));
        });
        let wildcard = received.clone();
        printer.register_mcu_message_handler(None, "status", move |message| {
            wildcard.lock().unwrap().push(format!("all:{}", message.mcu));
        });
        let events = vec![
            McuSessionEvent {
                mcu: "mcu0".into(),
                event: klipper_transport::SessionEvent::DecodedMessage {
                    name: "status".into(),
                    params: std::collections::BTreeMap::new(),
                    receive_time: 1.0,
                },
            },
            McuSessionEvent {
                mcu: "mcu1".into(),
                event: klipper_transport::SessionEvent::DecodedMessage {
                    name: "status".into(),
                    params: std::collections::BTreeMap::new(),
                    receive_time: 2.0,
                },
            },
            McuSessionEvent {
                mcu: "mcu0".into(),
                event: klipper_transport::SessionEvent::DecodedMessage {
                    name: "clock".into(),
                    params: std::collections::BTreeMap::new(),
                    receive_time: 3.0,
                },
            },
        ];

        assert_eq!(printer.dispatch_mcu_messages(&events), 0);
        assert_eq!(
            *received.lock().unwrap(),
            vec!["mcu0:status".to_owned(), "all:mcu0".to_owned(), "all:mcu1".to_owned()]
        );
    }

    #[test]
    fn decoded_message_handler_can_be_unregistered() {
        let printer = Printer::new();
        let called = Arc::new(AtomicBool::new(false));
        let observed = called.clone();
        let id = printer.register_mcu_message_handler(None, "status", move |_| {
            observed.store(true, Ordering::SeqCst);
        });

        assert!(printer.unregister_mcu_message_handler(id));
        assert!(!printer.unregister_mcu_message_handler(id));
        let events = vec![McuSessionEvent {
            mcu: "mcu0".into(),
            event: klipper_transport::SessionEvent::DecodedMessage {
                name: "status".into(),
                params: std::collections::BTreeMap::new(),
                receive_time: 1.0,
            },
        }];
        assert_eq!(printer.dispatch_mcu_messages(&events), 0);
        assert!(!called.load(Ordering::SeqCst));
    }

    #[test]
    fn panicking_decoded_message_handler_does_not_block_other_handlers() {
        let printer = Printer::new();
        let called = Arc::new(AtomicBool::new(false));
        printer.register_mcu_message_handler(None, "status", |_| panic!("handler panic"));
        let observed = called.clone();
        printer.register_mcu_message_handler(None, "status", move |_| {
            observed.store(true, Ordering::SeqCst);
        });
        let events = vec![McuSessionEvent {
            mcu: "mcu0".into(),
            event: klipper_transport::SessionEvent::DecodedMessage {
                name: "status".into(),
                params: std::collections::BTreeMap::new(),
                receive_time: 1.0,
            },
        }];

        assert_eq!(printer.dispatch_mcu_messages(&events), 1);
        assert!(called.load(Ordering::SeqCst));
    }

    #[test]
    fn mcu_events_are_dispatched_to_all_registered_handlers() {
        let printer = Printer::new();
        let received = Arc::new(Mutex::new(Vec::new()));
        let first = received.clone();
        printer.register_mcu_event_handler(move |event| {
            first.lock().unwrap().push(event.mcu.clone());
        });
        let second = received.clone();
        printer.register_mcu_event_handler(move |event| {
            second.lock().unwrap().push(format!("{}:second", event.mcu));
        });

        let events = vec![McuSessionEvent {
            mcu: "mcu0".to_owned(),
            event: klipper_transport::SessionEvent::Sent {
                sequence: 4,
                bytes: 12,
            },
        }];
        assert_eq!(printer.dispatch_mcu_events(&events), 0);
        assert_eq!(
            *received.lock().unwrap(),
            vec!["mcu0".to_owned(), "mcu0:second".to_owned()]
        );
    }

    #[test]
    fn panicking_mcu_event_handler_does_not_block_other_handlers() {
        let printer = Printer::new();
        let called = Arc::new(AtomicBool::new(false));
        printer.register_mcu_event_handler(|_| panic!("isolated MCU callback panic"));
        let observed = called.clone();
        printer.register_mcu_event_handler(move |_| {
            observed.store(true, Ordering::SeqCst);
        });

        let events = vec![McuSessionEvent {
            mcu: "mcu0".to_owned(),
            event: klipper_transport::SessionEvent::Sent {
                sequence: 1,
                bytes: 3,
            },
        }];
        assert_eq!(printer.dispatch_mcu_events(&events), 1);
        assert!(called.load(Ordering::SeqCst));
    }

    #[test]
    fn mcu_event_callback_can_register_another_callback() {
        let printer = Arc::new(Printer::new());
        let nested = printer.clone();
        printer.register_mcu_event_handler(move |_| {
            nested.register_mcu_event_handler(|_| {});
        });
        let events = vec![McuSessionEvent {
            mcu: "mcu0".to_owned(),
            event: klipper_transport::SessionEvent::Sent {
                sequence: 1,
                bytes: 1,
            },
        }];

        assert_eq!(printer.dispatch_mcu_events(&events), 0);
        assert_eq!(
            printer.mcu_event_handlers.lock().unwrap().len(),
            2
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
    fn gcode_handler_can_transition_printer_lifecycle() {
        let printer = Arc::new(Printer::new());
        let handler_printer = printer.clone();
        printer
            .register_gcode_command("TRIGGER_SHUTDOWN", false, None, move |_| {
                handler_printer.invoke_shutdown("command requested shutdown");
                Ok(Vec::new())
            })
            .unwrap();
        printer.set_ready();

        let outcome = printer
            .process_gcode_line("TRIGGER_SHUTDOWN", true)
            .expect("command should be parsed");

        assert!(outcome.handler_found);
        assert_eq!(printer.state(), PrinterState::Shutdown);
        assert_eq!(printer.state_message(), "command requested shutdown");
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
    fn unloading_component_removes_its_section_from_validation_allowlist() {
        let printer = Printer::new();
        printer.install_config(ConfigDocument::parse("[component]\n").unwrap());
        let mut registry = ComponentRegistry::new();
        registry
            .register("component", |_, _| Ok(()))
            .unwrap();
        printer.load_components(&registry).unwrap();
        assert!(printer.validate_config().is_ok());
        assert!(printer.unload_component("component"));
        assert!(matches!(
            printer.validate_config(),
            Err(ConfigValidationError::UnknownSection { .. })
        ));
    }

    #[test]
    fn loaded_component_without_registered_object_validates_its_section() {
        let printer = Printer::new();
        printer.install_config(ConfigDocument::parse("[component]\n").unwrap());
        let mut registry = ComponentRegistry::new();
        registry
            .register("component", |_, _| Ok(()))
            .unwrap();
        printer.load_components(&registry).unwrap();
        assert!(printer.validate_config().is_ok());
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
    fn base_gcode_handlers_remain_available_after_shutdown() {
        let printer = Printer::new();
        printer.invoke_shutdown("runtime fault");
        assert!(printer.process_gcode_line("M112", true).unwrap().handler_found);
    }

    #[test]
    fn m112_requests_printer_shutdown() {
        let printer = Printer::new();
        printer.set_ready();

        let outcome = printer.process_gcode_line("M112", true).unwrap();
        assert!(outcome.handler_found);
        assert!(outcome.acknowledged);
        assert_eq!(printer.state(), PrinterState::Shutdown);
    }

    #[test]
    fn script_observes_commands_unregistered_by_prior_handlers() {
        let printer = Arc::new(Printer::new());
        printer
            .register_gcode_command("M998", true, None, |_| Ok(Vec::new()))
            .unwrap();
        let unregistering_printer = printer.clone();
        printer
            .register_gcode_command("M999", true, None, move |_| {
                unregistering_printer.unregister_gcode_command("M998");
                Ok(Vec::new())
            })
            .unwrap();

        let outcomes = printer.process_gcode_script("M999\nM998");
        assert_eq!(outcomes.len(), 2);
        assert!(outcomes[0].handler_found);
        assert!(!outcomes[1].handler_found);
    }

    #[test]
    fn script_observes_state_changes_between_command_handlers() {
        let printer = Arc::new(Printer::new());
        let shutdown_printer = printer.clone();
        printer
            .register_gcode_command("M999", false, None, move |_| {
                shutdown_printer.invoke_shutdown("script shutdown");
                Ok(Vec::new())
            })
            .unwrap();
        printer
            .register_gcode_command("M998", false, None, |_| Ok(Vec::new()))
            .unwrap();
        printer.set_ready();

        let outcomes = printer.process_gcode_script("M999\nM998");
        assert_eq!(outcomes.len(), 2);
        assert!(outcomes[0].handler_found);
        assert!(!outcomes[1].handler_found);
        assert_eq!(printer.state(), PrinterState::Shutdown);
    }

    #[test]
    fn gcode_script_continues_after_a_panicking_handler() {
        let printer = Printer::new();
        printer.set_ready();
        printer
            .register_gcode_command("PANIC_NOW", false, None, |_| {
                panic!("script handler panic");
            })
            .unwrap();
        printer
            .register_gcode_command("AFTER_PANIC", false, None, |_| {
                Ok(vec![DispatchEvent::Info("continued".to_owned())])
            })
            .unwrap();

        let outcomes = printer.process_gcode_script("PANIC_NOW\nAFTER_PANIC");
        assert_eq!(outcomes.len(), 2);
        assert!(matches!(
            outcomes[0].events.first(),
            Some(DispatchEvent::Error(message)) if message.contains("panicked")
        ));
        assert_eq!(
            outcomes[1].events[0],
            DispatchEvent::Info("continued".to_owned())
        );
    }

    #[test]
    fn gcode_script_continues_after_a_handler_panic() {
        let printer = Printer::new();
        printer
            .register_gcode_command("M999", true, None, |_| {
                panic!("first command panics");
            })
            .unwrap();
        printer
            .register_gcode_command("M998", true, None, |_| Ok(Vec::new()))
            .unwrap();

        let outcomes = printer.process_gcode_script("M999\nM998");
        assert_eq!(outcomes.len(), 2);
        assert!(matches!(outcomes[0].events.first(), Some(DispatchEvent::Error(_))));
        assert!(outcomes[1].handler_found);
    }

    #[test]
    fn gcode_script_accepts_crlf_line_endings() {
        let printer = Printer::new();
        printer
            .register_gcode_command("M999", true, None, |_| Ok(Vec::new()))
            .unwrap();
        let outcomes = printer.process_gcode_script("M999\r\nM999\r\n");
        assert_eq!(outcomes.len(), 2);
        assert!(outcomes.iter().all(|outcome| outcome.handler_found));
    }

    #[test]
    fn gcode_script_dispatches_without_per_line_acknowledgements() {
        let printer = Printer::new();
        printer
            .register_gcode_command("M999", true, None, |_| Ok(Vec::new()))
            .unwrap();
        let outcomes = printer.process_gcode_script("M999\nM999");
        assert_eq!(outcomes.len(), 2);
        assert!(outcomes.iter().all(|outcome| !outcome.acknowledged));
    }

    #[test]
    fn script_observes_commands_registered_by_prior_handlers() {
        let printer = Arc::new(Printer::new());
        let registering_printer = printer.clone();
        printer
            .register_gcode_command("M999", true, None, move |_| {
                registering_printer
                    .register_gcode_command("M998", true, None, |_| Ok(Vec::new()))
                    .map_err(|error| GCodeError::MalformedCommand {
                        line: error.to_string(),
                    })?;
                Ok(Vec::new())
            })
            .unwrap();

        let outcomes = printer.process_gcode_script("M999\nM998");
        assert_eq!(outcomes.len(), 2);
        assert!(outcomes.iter().all(|outcome| outcome.handler_found));
    }

    #[test]
    fn gcode_handler_can_register_another_command_without_deadlock() {
        let printer = Arc::new(Printer::new());
        let handler_printer = printer.clone();
        printer
            .register_gcode_command("M105", true, None, move |_| {
                handler_printer
                    .register_gcode_command("M999", true, None, |_| Ok(Vec::new()))
                    .map_err(|error| GCodeError::MalformedCommand {
                        line: error.to_string(),
                    })?;
                Ok(Vec::new())
            })
            .unwrap();

        assert!(printer.process_gcode_line("M105", true).unwrap().handler_found);
        assert!(printer.process_gcode_line("M999", true).unwrap().handler_found);
    }

    #[test]
    fn gcode_handler_can_unregister_itself_without_deadlock() {
        let printer = Arc::new(Printer::new());
        let handler_printer = printer.clone();
        printer
            .register_gcode_command("M999", true, None, move |_| {
                handler_printer.unregister_gcode_command("M999");
                Ok(Vec::new())
            })
            .unwrap();

        assert!(printer.process_gcode_line("M999", true).unwrap().handler_found);
        assert!(!printer.process_gcode_line("M999", true).unwrap().handler_found);
    }

    #[test]
    fn ready_only_gcode_handlers_are_disabled_after_shutdown() {
        let printer = Printer::new();
        printer
            .register_gcode_command("M999", false, None, |_| Ok(Vec::new()))
            .unwrap();
        printer.set_ready();
        assert!(printer.process_gcode_line("M999", true).unwrap().handler_found);
        printer.invoke_shutdown("runtime fault");
        assert!(!printer.process_gcode_line("M999", true).unwrap().handler_found);
        printer.disconnect();
        assert!(!printer.process_gcode_line("M999", true).unwrap().handler_found);
    }

    #[test]
    fn printer_gcode_introspection_tracks_active_state_and_help() {
        let printer = Printer::new();
        printer
            .register_gcode_command("STATUS", true, Some("Report status"), |_| {
                Ok(Vec::new())
            })
            .unwrap();
        printer
            .register_gcode_command("MOVE", false, Some("Move toolhead"), |_| {
                Ok(Vec::new())
            })
            .unwrap();

        assert_eq!(
            printer.registered_gcode_commands(),
            vec!["M110".to_owned(), "M112".to_owned(), "M115".to_owned(), "STATUS".to_owned()]
        );
        assert_eq!(
            printer.gcode_help(),
            HashMap::from([("STATUS".to_owned(), "Report status".to_owned())])
        );

        printer.set_ready();
        assert_eq!(
            printer.registered_gcode_commands(),
            vec!["M110".to_owned(), "M112".to_owned(), "M115".to_owned(), "MOVE".to_owned(), "STATUS".to_owned()]
        );
        assert_eq!(printer.gcode_help().len(), 2);
    }

    #[test]
    fn gcode_handler_can_register_another_command_without_reentrant_locking() {
        let printer = Arc::new(Printer::new());
        let callback_printer = printer.clone();
        printer
            .register_gcode_command("REGISTER_NEXT", true, None, move |_| {
                callback_printer
                    .register_gcode_command("NEXT_COMMAND", false, None, |_| {
                        Ok(vec![DispatchEvent::Info("next".to_owned())])
                    })
                    .map_err(|error| error)?;
                Ok(vec![DispatchEvent::Info("registered".to_owned())])
            })
            .unwrap();

        printer.set_ready();
        let first = printer.process_gcode_line("REGISTER_NEXT", true).unwrap();
        assert_eq!(first.events[0], DispatchEvent::Info("registered".to_owned()));

        let second = printer.process_gcode_line("NEXT_COMMAND", true).unwrap();
        assert_eq!(second.events[0], DispatchEvent::Info("next".to_owned()));
    }

    #[test]
    fn gcode_handler_can_replace_itself_during_dispatch() {
        let printer = Arc::new(Printer::new());
        let callback_printer = printer.clone();
        printer
            .register_gcode_command("REPLACE_ME", true, None, move |_| {
                assert!(callback_printer.unregister_gcode_command("REPLACE_ME"));
                callback_printer
                    .register_gcode_command("REPLACE_ME", true, None, |_| {
                        Ok(vec![DispatchEvent::Info("replacement".to_owned())])
                    })
                    .map_err(|error| error)?;
                Ok(vec![DispatchEvent::Info("original".to_owned())])
            })
            .unwrap();

        printer.set_ready();
        let original = printer.process_gcode_line("REPLACE_ME", true).unwrap();
        assert_eq!(original.events[0], DispatchEvent::Info("original".to_owned()));

        let replacement = printer.process_gcode_line("REPLACE_ME", true).unwrap();
        assert_eq!(
            replacement.events[0],
            DispatchEvent::Info("replacement".to_owned())
        );
    }

    #[test]
    fn config_reinstall_waits_for_in_progress_component_factory() {
        let printer = Arc::new(Printer::new());
        printer.install_config(ConfigDocument::parse("[macro]\n").unwrap());

        let factory_entered = Arc::new(std::sync::Barrier::new(2));
        let release_factory = Arc::new(std::sync::Barrier::new(2));
        let mut registry = ComponentRegistry::new();
        let entered = factory_entered.clone();
        let release = release_factory.clone();
        registry
            .register("macro", move |printer, _| {
                entered.wait();
                release.wait();
                printer
                    .register_component_gcode_command("macro", "M999", true, None, |_| {
                        Ok(Vec::new())
                    })
                    .map_err(|error| error.to_string())
            })
            .unwrap();

        let loading_printer = printer.clone();
        let loading_registry = Arc::new(registry);
        let loading = std::thread::spawn(move || {
            loading_printer.load_components(&loading_registry).unwrap();
        });
        factory_entered.wait();

        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let (finished_tx, finished_rx) = std::sync::mpsc::channel();
        let installing_printer = printer.clone();
        let installing = std::thread::spawn(move || {
            started_tx.send(()).unwrap();
            installing_printer.install_config(
                ConfigDocument::parse("[macro]\nname=reloaded\n").unwrap(),
            );
            finished_tx.send(()).unwrap();
        });
        started_rx.recv().unwrap();
        assert!(finished_rx.recv_timeout(std::time::Duration::from_millis(10)).is_err());

        release_factory.wait();
        loading.join().unwrap();
        finished_rx.recv_timeout(std::time::Duration::from_secs(1)).unwrap();
        installing.join().unwrap();

        assert_eq!(
            printer.config().unwrap().get("macro", "name"),
            Some("reloaded")
        );
        assert!(!printer.registered_gcode_commands().contains(&"M999".to_owned()));
        assert!(printer.loaded_component_names().is_empty());
    }

    #[test]
    fn component_gcode_command_cannot_be_owned_by_two_components() {
        let printer = Printer::new();
        printer
            .register_component_gcode_command("first", "M999", true, None, |_| {
                Ok(Vec::new())
            })
            .unwrap();
        assert!(printer
            .register_component_mux_gcode_command(
                "second", "M999", "MODE", Some("fast"), None, |_| Ok(Vec::new()),
            )
            .is_err());
        assert!(printer.registered_gcode_commands().contains(&"M999".to_owned()));
    }

    #[test]
    fn config_reinstall_clears_component_gcode_ownership_and_handlers() {
        let printer = Printer::new();
        printer
            .register_component_gcode_command("macro", "M999", true, None, |_| {
                Ok(Vec::new())
            })
            .unwrap();
        printer.install_config(ConfigDocument::parse("[macro]\n").unwrap());
        assert!(!printer.registered_gcode_commands().contains(&"M999".to_owned()));

        printer
            .register_component_gcode_command("macro", "M999", true, None, |_| {
                Ok(vec![DispatchEvent::Info("reloaded".to_owned())])
            })
            .unwrap();
        printer.set_ready();
        assert!(printer.process_gcode_line("M999", true).unwrap().handler_found);
    }

    #[test]
    fn panicking_component_factory_rolls_back_owned_gcode_commands() {
        let printer = Printer::new();
        printer.install_config(ConfigDocument::parse("[macro]\n").unwrap());
        let mut registry = ComponentRegistry::new();
        registry
            .register("macro", |printer, _| {
                printer
                    .register_component_gcode_command("macro", "M999", true, None, |_| {
                        Ok(Vec::new())
                    })
                    .map_err(|error| error.to_string())?;
                panic!("factory panic after registering command");
            })
            .unwrap();

        assert!(matches!(
            printer.load_components(&registry),
            Err(ComponentError::LoadFailed { section, .. }) if section == "macro"
        ));
        assert!(!printer.registered_gcode_commands().contains(&"M999".to_owned()));
    }

    #[test]
    fn lazy_component_reload_reinstalls_owned_gcode_commands() {
        let printer = Printer::new();
        printer.install_config(ConfigDocument::parse("[macro]\n").unwrap());
        let mut registry = ComponentRegistry::new();
        registry
            .register("macro", |printer, _| {
                printer
                    .register_component_gcode_command("macro", "M999", true, None, |_| {
                        Ok(vec![DispatchEvent::Info("loaded".to_owned())])
                    })
                    .map_err(|error| error.to_string())
            })
            .unwrap();

        assert_eq!(registry.load_component(&printer, "macro").unwrap(), vec!["macro"]);
        printer.set_ready();
        assert!(printer.process_gcode_line("M999", true).unwrap().handler_found);

        assert!(printer.unload_component("macro"));
        assert!(!printer.process_gcode_line("M999", true).unwrap().handler_found);
        assert_eq!(registry.load_component(&printer, "macro").unwrap(), vec!["macro"]);
        assert_eq!(
            printer.process_gcode_line("M999", true).unwrap().events[0],
            DispatchEvent::Info("loaded".to_owned())
        );
    }

    #[test]
    fn manual_gcode_unregistration_releases_component_ownership() {
        let printer = Printer::new();
        printer
            .register_component_gcode_command("first", "M999", true, None, |_| {
                Ok(vec![DispatchEvent::Info("first".to_owned())])
            })
            .unwrap();
        assert!(printer.unregister_gcode_command("M999"));

        printer
            .register_component_gcode_command("second", "M999", true, None, |_| {
                Ok(vec![DispatchEvent::Info("second".to_owned())])
            })
            .unwrap();
        assert!(!printer.unload_component("first"));
        printer.set_ready();
        let outcome = printer.process_gcode_line("M999", true).unwrap();
        assert_eq!(outcome.events[0], DispatchEvent::Info("second".to_owned()));
    }

    #[test]
    fn unloading_component_unregisters_owned_gcode_commands() {
        let printer = Printer::new();
        printer
            .register_component_gcode_command("macro", "M999", true, None, |_| {
                Ok(vec![DispatchEvent::Info("owned".to_owned())])
            })
            .unwrap();
        printer.set_ready();
        assert!(printer.process_gcode_line("M999", true).unwrap().handler_found);

        assert!(printer.unload_component("macro"));
        assert!(!printer.registered_gcode_commands().contains(&"M999".to_owned()));
        assert!(!printer.process_gcode_line("M999", true).unwrap().handler_found);
    }

    #[test]
    fn unloading_component_removes_its_mux_command_values() {
        let printer = Printer::new();
        for value in ["quiet", "fast"] {
            printer
                .register_component_mux_gcode_command(
                    "heater", "SET_MODE", "MODE", Some(value), None, |_| Ok(Vec::new()),
                )
                .unwrap();
        }
        printer.set_ready();
        assert!(printer.process_gcode_line("SET_MODE MODE=quiet", true).unwrap().handler_found);
        assert!(printer.unload_component("heater"));
        assert!(!printer.process_gcode_line("SET_MODE MODE=quiet", true).unwrap().handler_found);
    }

    #[test]
    fn failed_component_factory_rolls_back_owned_gcode_commands() {
        let printer = Printer::new();
        printer.install_config(ConfigDocument::parse("[macro]\n").unwrap());
        let mut registry = ComponentRegistry::new();
        registry
            .register("macro", |printer, _| {
                printer
                    .register_component_gcode_command("macro", "M999", true, None, |_| {
                        Ok(Vec::new())
                    })
                    .map_err(|error| error.to_string())?;
                Err("factory failed after registration".to_owned())
            })
            .unwrap();

        assert!(matches!(
            printer.load_components(&registry),
            Err(ComponentError::LoadFailed { section, .. }) if section == "macro"
        ));
        assert!(!printer.registered_gcode_commands().contains(&"M999".to_owned()));
    }

    #[test]
    fn gcode_help_tracks_printer_readiness() {
        let printer = Printer::new();
        printer
            .register_gcode_command("M999", false, Some("Ready-only command"), |_| {
                Ok(Vec::new())
            })
            .unwrap();
        printer
            .register_gcode_command("M998", true, Some("Base command"), |_| {
                Ok(Vec::new())
            })
            .unwrap();

        let before_ready = printer.gcode_help();
        assert!(!before_ready.contains_key("M999"));
        assert!(before_ready.contains_key("M998"));
        printer.set_ready();
        let ready = printer.gcode_help();
        assert!(ready.contains_key("M999"));
        assert!(ready.contains_key("M998"));
    }

    #[test]
    fn printer_exposes_registered_gcode_commands_and_help() {
        let printer = Printer::new();
        printer
            .register_gcode_command("M999", true, Some("Report host status"), |_| {
                Ok(Vec::new())
            })
            .unwrap();
        assert_eq!(
            printer.registered_gcode_commands(),
            vec!["M110".to_owned(), "M112".to_owned(), "M115".to_owned(), "M999".to_owned()]
        );
        assert_eq!(
            printer.gcode_help().get("M999").map(String::as_str),
            Some("Report host status")
        );
    }

    #[test]
    fn printer_gcode_registration_helpers_manage_commands() {
        let printer = Printer::new();
        printer
            .register_gcode_command("M105", true, Some("Report temperatures"), |_| {
                Ok(Vec::new())
            })
            .unwrap();
        assert!(printer
            .register_gcode_command("M105", true, None, |_| Ok(Vec::new()))
            .is_err());
        printer
            .register_mux_gcode_command(
                "SET_HEATER",
                "HEATER",
                Some("hotend"),
                None,
                |_| Ok(Vec::new()),
            )
            .unwrap();
        assert!(printer
            .gcode_dispatcher()
            .registered_commands()
            .contains(&"M105".to_owned()));
        assert!(printer.unregister_gcode_command("M105"));
        assert!(!printer.unregister_gcode_command("M105"));
    }

    #[test]
    fn unloading_component_removes_its_mux_gcode_command() {
        let printer = Printer::new();
        printer.install_config(ConfigDocument::parse("[heater]\nname=hotend\n").unwrap());
        let mut registry = ComponentRegistry::new();
        registry
            .register("heater", |printer, section| {
                let value = section.get("name").unwrap().to_owned();
                printer
                    .register_component_mux_gcode_command(
                        "heater",
                        "SET_HEATER",
                        "HEATER",
                        Some(&value),
                        None,
                        |_| Ok(Vec::new()),
                    )
                    .map_err(|error| error.to_string())?;
                printer
                    .register_component_mux_gcode_command(
                        "heater",
                        "SET_HEATER",
                        "HEATER",
                        Some("bed"),
                        None,
                        |_| Ok(Vec::new()),
                    )
                    .map_err(|error| error.to_string())
            })
            .unwrap();
        printer
            .register_gcode_command("M998", true, None, |_| Ok(Vec::new()))
            .unwrap();
        printer.load_components(&registry).unwrap();
        printer.set_ready();
        for value in ["hotend", "bed"] {
            assert!(printer
                .process_gcode_line(&format!("SET_HEATER HEATER={value}"), true)
                .unwrap()
                .handler_found);
        }
        assert!(printer.unload_component("heater"));
        for value in ["hotend", "bed"] {
            assert!(!printer
                .process_gcode_line(&format!("SET_HEATER HEATER={value}"), true)
                .unwrap()
                .handler_found);
        }
        assert!(printer.process_gcode_line("M998", true).unwrap().handler_found);
    }

    #[test]
    fn component_gcode_name_conflicts_are_reported_to_the_second_factory() {
        let printer = Printer::new();
        printer.install_config(
            ConfigDocument::parse("[first]\nvalue=1\n[second]\nvalue=2\n").unwrap(),
        );
        let mut registry = ComponentRegistry::new();
        for section_name in ["first", "second"] {
            registry
                .register(section_name, |printer, section| {
                    let value = section.get("value").unwrap().to_owned();
                    printer
                        .register_gcode_command("M999", true, None, move |_| {
                            Ok(vec![DispatchEvent::Info(value.clone())])
                        })
                        .map_err(|error| error.to_string())
                })
                .unwrap();
        }

        assert!(matches!(
            printer.load_components(&registry),
            Err(ComponentError::LoadFailed { section, .. }) if section == "second"
        ));
        let outcome = printer.process_gcode_line("M999", true).unwrap();
        assert_eq!(
            outcome.events.first(),
            Some(&DispatchEvent::Info("1".to_owned()))
        );
    }

    #[test]
    fn component_registered_gcode_commands_follow_config_lifecycle() {
        let printer = Printer::new();
        printer.install_config(ConfigDocument::parse("[macro]\nname=first\n").unwrap());
        let calls = Arc::new(AtomicUsize::new(0));
        let observed = calls.clone();
        let mut registry = ComponentRegistry::new();
        registry
            .register("macro", move |printer, section| {
                let value = section.get("name").unwrap().to_owned();
                let observed = observed.clone();
                printer
                    .register_gcode_command("M999", true, None, move |_| {
                        observed.fetch_add(1, Ordering::SeqCst);
                        Ok(vec![DispatchEvent::Info(value.clone())])
                    })
                    .map_err(|error| error.to_string())
            })
            .unwrap();

        printer.initialize_components(&registry).unwrap();
        assert!(printer.process_gcode_line("M999", true).unwrap().handler_found);
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        printer.install_config(ConfigDocument::parse("[macro]\nname=second\n").unwrap());
        printer.initialize_components(&registry).unwrap();
        let outcome = printer.process_gcode_line("M999", true).unwrap();
        assert!(outcome.handler_found);
        assert!(outcome.events.contains(&DispatchEvent::Info("second".to_owned())));
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn config_reinstall_clears_component_gcode_handlers() {
        let printer = Printer::new();
        {
            let mut dispatcher = printer.gcode_dispatcher();
            dispatcher
                .register_command("M105", true, None, |_| Ok(Vec::new()))
                .unwrap();
            dispatcher
                .register_mux_command("SET_HEATER", "HEATER", Some("hotend"), None, |_| {
                    Ok(Vec::new())
                })
                .unwrap();
        }
        assert!(printer
            .gcode_dispatcher()
            .registered_commands()
            .contains(&"M105".to_owned()));

        printer.install_config(ConfigDocument::parse("[printer]\n").unwrap());
        let dispatcher = printer.gcode_dispatcher();
        assert_eq!(dispatcher.registered_commands(), vec!["M110".to_owned(), "M112".to_owned(), "M115".to_owned()]);
        assert!(!dispatcher.is_ready());
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
    fn config_loaded_handler_can_register_component_gcode() {
        let printer = Arc::new(Printer::new());
        let callback_printer = printer.clone();
        printer.register_event_handler(PrinterEventKind::ConfigLoaded, move |_| {
            callback_printer
                .register_component_gcode_command(
                    "probe",
                    "M999",
                    true,
                    None,
                    |_| Ok(Vec::new()),
                )
                .unwrap();
        });

        printer.install_config(ConfigDocument::parse("[probe]\nvalue=1\n").unwrap());
        assert!(printer.registered_gcode_commands().contains(&"M999".to_owned()));
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
    fn component_registry_normalizes_section_and_dependency_names() {
        let printer = Printer::new();
        printer.install_config(
            ConfigDocument::parse("[base]\nvalue=ready\n[fan]\npin=PA0\n").unwrap(),
        );
        let mut registry = ComponentRegistry::new();
        registry
            .register(" base ", |printer, section| {
                printer
                    .register_object("base_value", section.get("value").unwrap().to_owned())
                    .map_err(|error| error.to_string())
            })
            .unwrap();
        registry
            .register_with_dependencies(" fan ", vec![" base ".to_owned()], |printer, section| {
                let base = printer
                    .lookup_object::<String>("base_value")
                    .map_err(|error| error.to_string())?;
                let pin = section.get("pin").unwrap();
                printer
                    .register_object("fan_config", format!("{}:{pin}", base.as_str()))
                    .map_err(|error| error.to_string())
            })
            .unwrap();

        assert_eq!(
            printer.load_components(&registry).unwrap(),
            vec!["base".to_owned(), "fan".to_owned()]
        );
        assert_eq!(
            &*printer.lookup_object::<String>("fan_config").unwrap(),
            "ready:PA0"
        );
        assert!(matches!(
            registry.register("base ", |_, _| Ok(())),
            Err(ComponentError::DuplicateFactory { section }) if section == "base"
        ));
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
    fn disconnect_during_component_loading_prevents_connection_events() {
        let printer = Arc::new(Printer::new());
        printer.install_config(
            ConfigDocument::parse(
                "[fan]\npin=PA0\n[heater_generic hotend]\nmax_temp=250\n",
            )
            .unwrap(),
        );
        let identifies = Arc::new(AtomicUsize::new(0));
        let connects = Arc::new(AtomicUsize::new(0));
        let later_factories = Arc::new(AtomicUsize::new(0));
        let seen_identifies = identifies.clone();
        let seen_connects = connects.clone();
        let seen_later_factories = later_factories.clone();
        printer.register_event_handler(PrinterEventKind::McuIdentify, move |_| {
            seen_identifies.fetch_add(1, Ordering::SeqCst);
        });
        printer.register_event_handler(PrinterEventKind::Connect, move |_| {
            seen_connects.fetch_add(1, Ordering::SeqCst);
        });
        let disconnect_printer = printer.clone();
        let mut registry = ComponentRegistry::new();
        registry
            .register("fan", move |printer, _| {
                printer
                    .register_object("partial_fan", "PA0".to_owned())
                    .map_err(|error| error.to_string())?;
                disconnect_printer.disconnect();
                Ok(())
            })
            .unwrap();
        registry
            .register("heater_generic hotend", move |_, _| {
                seen_later_factories.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })
            .unwrap();

        assert!(printer.initialize_components(&registry).unwrap().is_empty());
        assert_eq!(printer.state(), PrinterState::Disconnected);
        assert_eq!(identifies.load(Ordering::SeqCst), 0);
        assert_eq!(connects.load(Ordering::SeqCst), 0);
        assert_eq!(later_factories.load(Ordering::SeqCst), 0);
        assert!(!printer.has_object("partial_fan"));
        assert!(printer.loaded_component_names().is_empty());
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
    fn disconnect_during_identify_prevents_connect_event() {
        let printer = Arc::new(Printer::new());
        printer.install_config(ConfigDocument::parse("[fan]\npin=PA0\n").unwrap());
        let disconnect_printer = printer.clone();
        printer.register_event_handler(PrinterEventKind::McuIdentify, move |_| {
            disconnect_printer.disconnect();
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
        assert_eq!(printer.state(), PrinterState::Disconnected);
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
    fn component_factory_panic_rolls_back_owned_mux_gcode_commands() {
        let printer = Printer::new();
        printer.install_config(ConfigDocument::parse("[heater]\nname=hotend\n").unwrap());
        let mut registry = ComponentRegistry::new();
        registry
            .register("heater", |printer, section| {
                printer
                    .register_component_mux_gcode_command(
                        "heater",
                        "SET_HEATER",
                        "HEATER",
                        section.get("name"),
                        None,
                        |_| Ok(Vec::new()),
                    )
                    .map_err(|error| error.to_string())?;
                panic!("factory panic after mux registration");
            })
            .unwrap();

        assert!(matches!(
            printer.load_components(&registry),
            Err(ComponentError::LoadFailed { .. })
        ));
        printer.set_ready();
        assert!(!printer
            .process_gcode_line("SET_HEATER HEATER=hotend", true)
            .unwrap()
            .handler_found);
    }

    #[test]
    fn component_factory_panic_rolls_back_owned_gcode_commands() {
        let printer = Printer::new();
        printer.install_config(ConfigDocument::parse("[component]\nvalue=1\n").unwrap());
        let mut registry = ComponentRegistry::new();
        registry
            .register("component", |printer, _| {
                printer
                    .register_component_gcode_command(
                        "component",
                        "M999",
                        true,
                        None,
                        |_| Ok(Vec::new()),
                    )
                    .map_err(|error| error.to_string())?;
                panic!("factory panics after command registration");
            })
            .unwrap();

        assert!(matches!(
            printer.load_components(&registry),
            Err(ComponentError::LoadFailed { .. })
        ));
        assert!(!printer.registered_gcode_commands().contains(&"M999".to_owned()));
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
    fn full_load_skips_components_already_loaded_lazily() {
        let printer = Printer::new();
        printer.install_config(ConfigDocument::parse("[fan]\npin=PA0\n").unwrap());
        let calls = Arc::new(AtomicUsize::new(0));
        let observed = calls.clone();
        let mut registry = ComponentRegistry::new();
        registry
            .register("fan", move |printer, section| {
                observed.fetch_add(1, Ordering::SeqCst);
                printer
                    .register_object("fan", section.get("pin").unwrap().to_owned())
                    .map_err(|error| error.to_string())
            })
            .unwrap();

        assert_eq!(
            registry.load_component(&printer, "fan").unwrap(),
            vec!["fan".to_owned()]
        );
        assert!(registry.load_registered(&printer).unwrap().is_empty());
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn concurrent_lazy_component_loads_run_factory_once() {
        let printer = Arc::new(Printer::new());
        printer.install_config(ConfigDocument::parse("[fan]\npin=PA0\n").unwrap());
        let calls = Arc::new(AtomicUsize::new(0));
        let observed = calls.clone();
        let mut owned_registry = ComponentRegistry::new();
        owned_registry
            .register("fan", move |printer, section| {
                observed.fetch_add(1, Ordering::SeqCst);
                std::thread::sleep(std::time::Duration::from_millis(10));
                printer
                    .register_object("fan", section.get("pin").unwrap().to_owned())
                    .map_err(|error| error.to_string())
            })
            .unwrap();
        let registry = Arc::new(owned_registry);
        let start = Arc::new(std::sync::Barrier::new(3));
        let mut workers = Vec::new();
        for _ in 0..2 {
            let printer = printer.clone();
            let registry = registry.clone();
            let start = start.clone();
            workers.push(std::thread::spawn(move || {
                start.wait();
                registry.load_component(&printer, "fan").unwrap().len()
            }));
        }
        start.wait();
        let loaded: usize = workers.into_iter().map(|worker| worker.join().unwrap()).sum();
        assert_eq!(loaded, 1);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn lazy_component_load_reports_unknown_component() {
        let printer = Printer::new();
        printer.install_config(ConfigDocument::parse("[printer]\nkinematics=cartesian\n").unwrap());
        let registry = ComponentRegistry::new();
        assert!(matches!(
            registry.load_component(&printer, "missing"),
            Err(ComponentError::MissingDependency {
                section,
                dependency,
            }) if section == "missing" && dependency == "missing"
        ));
    }

    #[test]
    fn lazy_component_load_normalizes_section_name() {
        let printer = Printer::new();
        printer.install_config(ConfigDocument::parse("[fan]\npin=PA0\n").unwrap());
        let mut registry = ComponentRegistry::new();
        registry
            .register(" fan ", |printer, section| {
                printer
                    .register_object("fan", section.get("pin").unwrap().to_owned())
                    .map_err(|error| error.to_string())
            })
            .unwrap();

        assert_eq!(registry.load_component(&printer, " fan ").unwrap(), vec!["fan"]);
        assert!(printer.has_object("fan"));
        assert!(printer.unload_component(" fan "));
        assert!(!printer.is_component_loaded("fan"));
    }

    #[test]
    fn lazy_component_load_requires_installed_config() {
        let printer = Printer::new();
        let registry = ComponentRegistry::new();
        assert_eq!(
            registry.load_component(&printer, "fan"),
            Err(ComponentError::ConfigNotLoaded)
        );
    }

    #[test]
    fn lazy_component_load_skips_unrelated_sections() {
        let printer = Printer::new();
        printer.install_config(
            ConfigDocument::parse(
                "[unused]\nvalue=ignored\n[heater]\nsource=fan\n[fan]\npin=PA0\n",
            )
            .unwrap(),
        );
        let mut registry = ComponentRegistry::new();
        registry
            .register("unused", |_, _| panic!("unrelated component should not load"))
            .unwrap();
        registry
            .register_with_dependencies(
                "heater",
                vec!["fan".to_owned()],
                |printer, section| {
                    let fan = printer
                        .lookup_object::<String>("fan")
                        .map_err(|error| error.to_string())?;
                    printer
                        .register_object(
                            "heater",
                            format!("{}:{}", fan.as_str(), section.get("source").unwrap()),
                        )
                        .map_err(|error| error.to_string())
                },
            )
            .unwrap();
        registry
            .register("fan", |printer, section| {
                printer
                    .register_object("fan", section.get("pin").unwrap().to_owned())
                    .map_err(|error| error.to_string())
            })
            .unwrap();

        assert_eq!(
            registry.load_component(&printer, "heater").unwrap(),
            vec!["fan".to_owned(), "heater".to_owned()]
        );
        assert!(!printer.has_object("unused"));
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
    fn pre_registered_object_does_not_skip_its_own_component_factory() {
        let printer = Printer::new();
        printer.install_config(
            ConfigDocument::parse("[provider]\nvalue=initialized\n").unwrap(),
        );
        printer
            .register_object("provider", "preloaded".to_owned())
            .unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let observed = calls.clone();
        let mut registry = ComponentRegistry::new();
        registry
            .register("provider", move |printer, section| {
                observed.fetch_add(1, Ordering::SeqCst);
                printer
                    .register_object(
                        "provider child",
                        section.get("value").unwrap().to_owned(),
                    )
                    .map_err(|error| error.to_string())
            })
            .unwrap();

        assert_eq!(
            printer.load_components(&registry).unwrap(),
            vec!["provider".to_owned()]
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            &*printer.lookup_object::<String>("provider").unwrap(),
            "preloaded"
        );
        assert_eq!(
            &*printer.lookup_object::<String>("provider child").unwrap(),
            "initialized"
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
    fn unloading_unknown_component_returns_false() {
        let printer = Printer::new();
        assert!(!printer.unload_component("missing"));
    }

    #[test]
    fn removing_component_object_allows_component_reload() {
        let printer = Printer::new();
        printer.install_config(ConfigDocument::parse("[fan]\npin=PA0\n").unwrap());
        let calls = Arc::new(AtomicUsize::new(0));
        let observed = calls.clone();
        let mut registry = ComponentRegistry::new();
        registry
            .register("fan", move |printer, section| {
                observed.fetch_add(1, Ordering::SeqCst);
                printer
                    .register_object("fan", section.get("pin").unwrap().to_owned())
                    .map_err(|error| error.to_string())
            })
            .unwrap();
        printer.load_components(&registry).unwrap();
        assert!(printer.remove_object("fan"));
        assert!(printer.load_components(&registry).unwrap().is_empty());
        assert!(printer.unload_component("fan"));
        assert!(!printer.is_component_loaded("fan"));
        assert!(printer.loaded_component_names().is_empty());
        assert_eq!(printer.load_components(&registry).unwrap(), vec!["fan"]);
        assert_eq!(calls.load(Ordering::SeqCst), 2);
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
        assert!(printer.is_component_loaded("fan"));
        assert_eq!(printer.loaded_component_names(), vec!["fan"]);
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
    fn failed_factory_usage_rollback_preserves_concurrent_config_reads() {
        let printer = Arc::new(Printer::new());
        printer.install_config(
            ConfigDocument::parse("[fan]\npin=PA0\nspeed=100\n").unwrap(),
        );
        let barrier = Arc::new(std::sync::Barrier::new(2));
        let factory_barrier = barrier.clone();
        let mut registry = ComponentRegistry::new();
        registry
            .register("fan", move |_, section| {
                assert_eq!(section.get("pin"), Some("PA0"));
                section.deprecate("pin", None, None);
                factory_barrier.wait();
                factory_barrier.wait();
                Err("factory failure".to_owned())
            })
            .unwrap();

        let loading_printer = printer.clone();
        let loader = std::thread::spawn(move || loading_printer.load_components(&registry));
        barrier.wait();
        let config = printer.config().unwrap();
        let external_section = printer.config_usage.track_section(config.section("fan").unwrap());
        assert_eq!(external_section.get("speed"), Some("100"));
        barrier.wait();
        assert!(matches!(
            loader.join().unwrap(),
            Err(ComponentError::LoadFailed { .. })
        ));

        let unused = printer.unused_config_options();
        assert_eq!(unused.len(), 1);
        assert_eq!(unused[0].option, "pin");
        assert!(printer.config_warnings().is_empty());
    }

    #[test]
    fn panicking_component_factory_rolls_back_objects_and_config_usage() {
        let printer = Printer::new();
        printer.install_config(ConfigDocument::parse("[fan]\npin=PA0\n").unwrap());
        let mut registry = ComponentRegistry::new();
        registry
            .register("fan", |printer, section| {
                assert_eq!(section.get("pin"), Some("PA0"));
                section.deprecate("pin", None, None);
                printer
                    .register_object("fan", "partially initialized")
                    .map_err(|error| error.to_string())?;
                panic!("factory panic after partial initialization");
            })
            .unwrap();

        assert!(matches!(
            printer.load_components(&registry),
            Err(ComponentError::LoadFailed { message, .. })
                if message == "component factory panicked"
        ));
        assert!(!printer.has_object("fan"));
        assert_eq!(printer.unused_config_options().len(), 1);
        assert!(printer.config_warnings().is_empty());
    }

    #[test]
    fn failed_component_factory_rolls_back_config_usage_and_warnings() {
        let printer = Printer::new();
        printer.install_config(ConfigDocument::parse("[fan]\npin=PA0\n").unwrap());
        let mut registry = ComponentRegistry::new();
        registry
            .register("fan", |_, section| {
                assert_eq!(section.get("pin"), Some("PA0"));
                section.deprecate("pin", None, None);
                Err("temporary factory failure".to_owned())
            })
            .unwrap();

        assert!(matches!(
            printer.load_components(&registry),
            Err(ComponentError::LoadFailed { .. })
        ));
        let unused = printer.unused_config_options();
        assert_eq!(unused.len(), 1);
        assert_eq!(unused[0].section, "fan");
        assert_eq!(unused[0].option, "pin");
        assert!(printer.config_warnings().is_empty());
    }

    #[test]
    fn failed_component_factory_does_not_remove_concurrent_external_objects() {
        let printer = Arc::new(Printer::new());
        printer.install_config(ConfigDocument::parse("[fan]\npin=PA0\n").unwrap());
        let barrier = Arc::new(std::sync::Barrier::new(2));
        let factory_barrier = barrier.clone();
        let mut registry = ComponentRegistry::new();
        registry
            .register("fan", move |printer, section| {
                printer
                    .register_object("fan", section.get("pin").unwrap().to_owned())
                    .map_err(|error| error.to_string())?;
                factory_barrier.wait();
                factory_barrier.wait();
                Err("factory fails after registering its object".to_owned())
            })
            .unwrap();

        let loading_printer = printer.clone();
        let loader = std::thread::spawn(move || {
            loading_printer.load_components(&registry)
        });
        barrier.wait();
        printer
            .register_object("external", "owned by another thread".to_owned())
            .unwrap();
        barrier.wait();
        assert!(matches!(
            loader.join().unwrap(),
            Err(ComponentError::LoadFailed { .. })
        ));

        assert!(!printer.has_object("fan"));
        assert_eq!(
            &*printer.lookup_object::<String>("external").unwrap(),
            "owned by another thread"
        );
    }

    #[test]
    fn failed_component_factory_removes_objects_before_retry() {
        let printer = Printer::new();
        printer.install_config(ConfigDocument::parse("[fan]\npin=PA0\n").unwrap());
        let calls = Arc::new(AtomicUsize::new(0));
        let observed = calls.clone();
        let mut registry = ComponentRegistry::new();
        registry
            .register("fan", move |printer, section| {
                let pin = section.get("pin").unwrap().to_owned();
                printer
                    .register_object("fan", pin)
                    .map_err(|error| error.to_string())?;
                if observed.fetch_add(1, Ordering::SeqCst) == 0 {
                    return Err("fail after object registration".to_owned());
                }
                Ok(())
            })
            .unwrap();

        assert!(matches!(
            printer.load_components(&registry),
            Err(ComponentError::LoadFailed { .. })
        ));
        assert!(!printer.has_object("fan"));
        assert!(!printer.is_component_loaded("fan"));

        assert_eq!(printer.load_components(&registry).unwrap(), vec!["fan"]);
        assert_eq!(&*printer.lookup_object::<String>("fan").unwrap(), "PA0");
        assert_eq!(calls.load(Ordering::SeqCst), 2);
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
    fn config_reinstall_allows_a_fresh_component_connection_cycle() {
        let printer = Printer::new();
        printer.install_config(ConfigDocument::parse("[fan]\npin=PA0\n").unwrap());
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

        printer.initialize_components(&registry).unwrap();
        printer.set_ready();
        printer.install_config(ConfigDocument::parse("[fan]\npin=PB0\n").unwrap());
        assert_eq!(printer.state(), PrinterState::Configuring);
        printer.initialize_components(&registry).unwrap();
        assert_eq!(connects.load(Ordering::SeqCst), 2);
        assert_eq!(&*printer.lookup_object::<String>("fan").unwrap(), "PB0");
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
    fn component_registry_lists_registered_sections_in_sorted_order() {
        let mut registry = ComponentRegistry::new();
        registry.register("z_stepper", |_, _| Ok(())).unwrap();
        registry.register("fan", |_, _| Ok(())).unwrap();
        registry.register("heater", |_, _| Ok(())).unwrap();
        assert_eq!(
            registry.registered_sections(),
            vec![
                "fan".to_owned(),
                "heater".to_owned(),
                "z_stepper".to_owned()
            ]
        );
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
    fn repeated_disconnect_emits_one_event_and_invalidates_connection_generation_once() {
        let printer = Printer::new();
        let events = Arc::new(AtomicUsize::new(0));
        let observed = events.clone();
        printer.register_event_handler(PrinterEventKind::Disconnect, move |_| {
            observed.fetch_add(1, Ordering::SeqCst);
        });
        let generation = printer.connection_generation.load(Ordering::SeqCst);
        printer.disconnect();
        let disconnected_generation = printer.connection_generation.load(Ordering::SeqCst);
        printer.disconnect();
        assert_eq!(printer.state(), PrinterState::Disconnected);
        assert_eq!(events.load(Ordering::SeqCst), 1);
        assert_eq!(disconnected_generation, generation.wrapping_add(1));
        assert_eq!(
            printer.connection_generation.load(Ordering::SeqCst),
            disconnected_generation
        );
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
    fn event_handler_registry_recovers_after_lock_poisoning() {
        let printer = Arc::new(Printer::new());
        let poisoned_printer = printer.clone();
        let panic_result = std::thread::spawn(move || {
            let _guard = poisoned_printer.event_handlers.lock().unwrap();
            panic!("poison printer event handler registry");
        })
        .join();
        assert!(panic_result.is_err());

        let called = Arc::new(AtomicUsize::new(0));
        let observed = called.clone();
        printer.register_event_handler(PrinterEventKind::Ready, move |_| {
            observed.fetch_add(1, Ordering::SeqCst);
        });

        assert_eq!(printer.send_event(PrinterEvent::Ready), 0);
        assert_eq!(called.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn returning_to_configuring_starts_a_new_connection_initialization() {
        let printer = Printer::new();
        let identify_events = Arc::new(AtomicUsize::new(0));
        let observed = identify_events.clone();
        printer.register_event_handler(PrinterEventKind::McuIdentify, move |_| {
            observed.fetch_add(1, Ordering::SeqCst);
        });
        printer.install_config(ConfigDocument::parse("[fan]\npin=PA0\n").unwrap());
        let mut registry = ComponentRegistry::new();
        registry
            .register("fan", |printer, section| {
                printer
                    .register_object("fan", section.get("pin").unwrap().to_owned())
                    .map_err(|error| error.to_string())
            })
            .unwrap();

        // Initialization is only valid after entering the configuring state.
        printer.set_configuring();
        assert_eq!(printer.initialize_components(&registry).unwrap(), vec!["fan"]);
        printer.set_ready();
        assert_eq!(identify_events.load(Ordering::SeqCst), 1);

        let previous_generation = printer.connection_generation.load(Ordering::SeqCst);
        printer.set_configuring();
        assert_eq!(
            printer.connection_generation.load(Ordering::SeqCst),
            previous_generation.wrapping_add(1)
        );
        printer.initialize_components(&registry).unwrap();
        assert_eq!(identify_events.load(Ordering::SeqCst), 2);
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
    #[test]
    fn status_objects_are_explicitly_registered_and_atomically_updated() {
        let printer = Printer::new();
        printer.register_status_object("toolhead", serde_json::json!({
            "position": [0.0, 0.0, 0.0],
            "homed_axes": ""
        })).unwrap();
        printer.update_status_object("toolhead", serde_json::json!({
            "position": [10.0, 20.0, 30.0],
            "homed_axes": "xyz"
        })).unwrap();
        let status = printer.lookup_object::<serde_json::Value>("toolhead").unwrap();
        assert_eq!(status["position"], serde_json::json!([10.0, 20.0, 30.0]));
        assert_eq!(status["homed_axes"], "xyz");
    }

    #[test]
    fn removing_status_objects_only_removes_published_json_objects() {
        let printer = Printer::new();
        printer.register_status_object("toolhead", serde_json::json!({
            "position": [0.0, 0.0, 0.0]
        })).unwrap();
        printer.register_object("internal_component", "private").unwrap();

        assert!(!printer.remove_status_object("missing"));
        assert!(!printer.remove_status_object("internal_component"));
        assert!(printer.has_object("internal_component"));
        assert!(printer.remove_status_object("toolhead"));
        assert!(!printer.has_object("toolhead"));
        assert!(printer.status_object_names().is_empty());
        assert!(!printer.remove_status_object("toolhead"));
    }

    #[tokio::test]
    async fn removing_status_objects_broadcasts_the_removed_name() {
        let printer = Printer::new();
        printer.register_status_object("toolhead", serde_json::json!({
            "position": [0.0, 0.0, 0.0]
        })).unwrap();
        let mut updates = printer.subscribe_status_updates();

        assert!(printer.remove_status_object("toolhead"));
        assert_eq!(updates.recv().await.unwrap(), "toolhead");
    }

    #[test]
    fn status_object_registration_requires_an_object_value() {
        let printer = Printer::new();
        assert!(matches!(
            printer.register_status_object("toolhead", serde_json::json!([1, 2, 3])),
            Err(PrinterError::ObjectTypeMismatch { .. })
        ));
    }

    #[test]
    fn printer_eventtime_is_non_negative_and_monotonic() {
        let printer = Printer::new();
        let first = printer.eventtime();
        assert!(first >= 0.0);

        std::thread::sleep(std::time::Duration::from_millis(2));
        let second = printer.eventtime();
        assert!(second > first);
    }

    #[tokio::test]
    async fn status_object_updates_are_broadcast_to_subscribers() {
        let printer = Printer::new();
        printer.register_status_object("toolhead", serde_json::json!({
            "position": [0.0, 0.0, 0.0]
        })).unwrap();
        let mut updates = printer.subscribe_status_updates();
        printer.update_status_object("toolhead", serde_json::json!({
            "position": [1.0, 2.0, 3.0]
        })).unwrap();
        assert_eq!(updates.recv().await.unwrap(), "toolhead");
    }


}
