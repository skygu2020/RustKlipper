//! Host-side G-code command registration and dispatch.
//!
//! This is the command-routing layer corresponding to the core of
//! `GCodeDispatch` in Klipper's `klippy/gcode.py`. It deliberately does not
//! implement printer objects, reactor integration, or macro evaluation.

use std::collections::HashMap;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::Arc;

use crate::{is_traditional_command, parse_line, GCodeCommand, GCodeError};

/// Responses emitted by a command handler.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DispatchEvent {
    /// Send a protocol line without adding a prefix.
    Raw(String),
    /// Send informational output using Klipper's `// ` convention.
    Info(String),
    /// Report a command error using Klipper's `!! ` convention.
    Error(String),
    /// Explicitly acknowledge a command. This suppresses automatic ACK.
    Ack(Option<String>),
}

type Handler = Arc<dyn Fn(&GCodeCommand) -> Result<Vec<DispatchEvent>, GCodeError> + Send + Sync>;

#[derive(Clone)]
struct Registration {
    handler: Handler,
    help: Option<String>,
}

#[derive(Clone)]
struct MuxRegistration {
    key: String,
    values: HashMap<Option<String>, Handler>,
}

/// Result of processing one input line.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DispatchOutcome {
    pub command: String,
    pub events: Vec<DispatchEvent>,
    pub handler_found: bool,
    pub acknowledged: bool,
}

/// Routes parsed G-code to registered host-side command handlers.
///
/// Commands are registered in the ready table. A command marked
/// `available_before_ready` is also copied into the base table, which remains
/// active while the printer is not ready or has shut down.
#[derive(Clone, Default)]
pub struct GCodeDispatcher {
    ready_handlers: HashMap<String, Registration>,
    base_handlers: HashMap<String, Registration>,
    mux_commands: HashMap<String, MuxRegistration>,
    ready: bool,
    state_message: String,
}

impl GCodeDispatcher {
    pub fn new() -> Self {
        Self {
            state_message: "Printer is not ready".to_owned(),
            ..Self::default()
        }
    }

    pub fn is_ready(&self) -> bool {
        self.ready
    }

    /// Clear all registered handlers and return to the initial dispatcher state.
    pub fn reset(&mut self) {
        *self = Self::new();
    }

    pub fn set_ready(&mut self) {
        self.ready = true;
    }

    pub fn set_shutdown(&mut self, message: impl Into<String>) {
        self.ready = false;
        self.state_message = message.into();
    }

    pub fn set_disconnected(&mut self) {
        self.ready = false;
        self.state_message = "Printer is disconnected".to_owned();
    }

    pub fn set_not_ready(&mut self, message: impl Into<String>) {
        self.ready = false;
        self.state_message = message.into();
    }

    /// Register or replace a command handler.
    ///
    /// Traditional commands include names such as `G1` and `M105`.
    /// Extended command names must be uppercase alphanumeric/underscore names
    /// and may not begin with a digit or have a digit as their second byte.
    pub fn register_command<F>(
        &mut self,
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
        let name = command.trim();
        if !valid_command_name(name) {
            return Err(GCodeError::MalformedCommand {
                line: format!("invalid command registration name '{name}'"),
            });
        }
        if self.ready_handlers.contains_key(name) {
            return Err(GCodeError::MalformedCommand {
                line: format!("gcode command {name} already registered"),
            });
        }

        let registration = Registration {
            handler: Arc::new(handler),
            help: help.map(str::to_owned),
        };
        self.ready_handlers.insert(name.to_owned(), registration.clone());
        if available_before_ready {
            self.base_handlers.insert(name.to_owned(), registration);
        }
        Ok(())
    }

    /// Register one value handler for a Klipper-style mux command.
    ///
    /// Multiple registrations for the same command must use the same key and
    /// distinct values. A `None` value registers the optional/default branch.
    pub fn register_mux_command<F>(
        &mut self,
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
        let name = command.trim();
        if let Some(existing) = self.mux_commands.get(name) {
            if existing.key != key {
                return Err(GCodeError::MalformedCommand {
                    line: format!(
                        "mux command {name} may have only one key ({}), not {key}",
                        existing.key
                    ),
                });
            }
        } else {
            self.register_command(name, false, help, |_| Ok(Vec::new()))?;
            self.mux_commands.insert(
                name.to_owned(),
                MuxRegistration {
                    key: key.to_owned(),
                    values: HashMap::new(),
                },
            );
        }

        let mux = self.mux_commands.get_mut(name).expect("mux inserted above");
        let value = value.map(str::to_owned);
        if mux.values.contains_key(&value) {
            return Err(GCodeError::MalformedCommand {
                line: format!("mux command {name} {key} {value:?} already registered"),
            });
        }
        mux.values.insert(value, Arc::new(handler));
        Ok(())
    }

    /// Remove a command from both readiness tables.
    pub fn unregister_command(&mut self, command: &str) -> bool {
        let name = command.trim();
        let removed = self.ready_handlers.remove(name).is_some();
        self.base_handlers.remove(name);
        self.mux_commands.remove(name);
        removed
    }

    pub fn command_help(&self) -> HashMap<String, String> {
        self.active_handlers()
            .iter()
            .filter_map(|(name, registration)| {
                registration.help.as_ref().map(|help| (name.clone(), help.clone()))
            })
            .collect()
    }

    pub fn registered_commands(&self) -> Vec<String> {
        let mut commands: Vec<_> = self.active_handlers().keys().cloned().collect();
        commands.sort();
        commands
    }

    /// Parse and dispatch one line. Blank/comment-only lines are ignored.
    pub fn process_line(&self, input: &str, need_ack: bool) -> Option<DispatchOutcome> {
        let parsed = parse_line(input)?;
        Some(self.dispatch(parsed, need_ack))
    }

    /// Execute a multi-line script in order without per-line ACKs, matching
    /// Klipper's `run_script` / `run_script_from_command` behavior.
    pub fn process_script(&self, script: &str) -> Vec<DispatchOutcome> {
        script
            .split('\n')
            .filter_map(|line| self.process_line(line, false))
            .collect()
    }

    pub fn dispatch(
        &self,
        parsed: crate::ParsedLine,
        need_ack: bool,
    ) -> DispatchOutcome {
        let command_name = parsed.command.clone();
        let command = match GCodeCommand::new_for_dispatch(parsed) {
            Ok(command) => command,
            Err(error) => {
                let mut events = vec![DispatchEvent::Error(error.to_string())];
                let acknowledged = need_ack;
                if need_ack {
                    events.push(DispatchEvent::Ack(None));
                }
                return DispatchOutcome {
                    command: command_name,
                    events,
                    handler_found: false,
                    acknowledged,
                };
            }
        };

        let registration = self.active_handlers().get(command.command()).cloned();
        let mut handler_found = registration.is_some();
        let mut events = Vec::new();

        match (registration, self.mux_commands.get(command.command())) {
            (Some(_), Some(mux)) => {
                let result = catch_unwind(AssertUnwindSafe(|| dispatch_mux(mux, &command)))
                    .unwrap_or_else(|_| Err(GCodeError::HandlerPanicked {
                        command: command.command().to_owned(),
                    }));
                match result {
                    Ok(mut responses) => events.append(&mut responses),
                    Err(error) => events.push(DispatchEvent::Error(error.to_string())),
                }
            }
            (Some(registration), None) => {
                let result = catch_unwind(AssertUnwindSafe(|| (registration.handler)(&command)))
                    .unwrap_or_else(|_| Err(GCodeError::HandlerPanicked {
                        command: command.command().to_owned(),
                    }));
                match result {
                    Ok(mut responses) => events.append(&mut responses),
                    Err(error) => events.push(DispatchEvent::Error(error.to_string())),
                }
            },
            (None, _) => match command.command() {
                // Klipper intentionally handles these two requests before the
                // printer is ready; M105 includes its ACK payload.
                "M105" => {
                    if need_ack {
                        events.push(DispatchEvent::Ack(Some("T:0".to_owned())));
                    }
                    handler_found = true;
                }
                "M21" => handler_found = true,
                _ if !self.ready => {
                    events.push(DispatchEvent::Error(self.state_message.clone()));
                }
                "" => {}
                _ => events.push(DispatchEvent::Info(format!(
                    "Unknown command:\"{}\"", command.command()
                ))),
            },
        }

        if !need_ack {
            events.retain(|event| !matches!(event, DispatchEvent::Ack(_)));
        }
        let explicit_ack = events.iter().any(|event| matches!(event, DispatchEvent::Ack(_)));
        if need_ack && !explicit_ack {
            events.push(DispatchEvent::Ack(None));
        }
        DispatchOutcome {
            command: command_name,
            events,
            handler_found,
            acknowledged: need_ack || explicit_ack,
        }
    }

    fn active_handlers(&self) -> &HashMap<String, Registration> {
        if self.ready {
            &self.ready_handlers
        } else {
            &self.base_handlers
        }
    }
}


fn dispatch_mux(
    mux: &MuxRegistration,
    command: &GCodeCommand,
) -> Result<Vec<DispatchEvent>, GCodeError> {
    let value = match command.get(&mux.key) {
        Some(value) => Some(value.to_owned()),
        None if mux.values.contains_key(&None) => None,
        None => {
            return Err(GCodeError::MissingParameter {
                line: command.commandline().to_owned(),
                parameter: mux.key.clone(),
            });
        }
    };
    if let Some(handler) = mux.values.get(&value) {
        return handler(command);
    }

    let value_text = value.as_deref().unwrap_or("");
    let mut hint = String::new();
    let suggestion = mux
        .values
        .keys()
        .filter_map(|value| value.as_deref())
        .find(|candidate| !value_text.is_empty() && candidate.contains(value_text));
    if let Some(suggestion) = suggestion {
        hint = format!(". Did you mean '{suggestion}'?");
    } else {
        let options: Vec<_> = mux
            .values
            .keys()
            .filter_map(|value| value.as_deref())
            .map(|option| format!("'{option}'"))
            .collect();
        if !options.is_empty() {
            hint = format!(". Options: {}", options.join(", "));
        }
    }
    Err(GCodeError::InvalidMuxValue {
        key: mux.key.clone(),
        value: value_text.to_owned(),
        hint,
    })
}

fn valid_command_name(name: &str) -> bool {
    if name.is_empty() || name != name.to_ascii_uppercase() {
        return false;
    }
    if is_traditional_command(name) {
        return true;
    }
    let bytes = name.as_bytes();
    if !bytes[0].is_ascii_alphabetic() || bytes.get(1).is_some_and(u8::is_ascii_digit) {
        return false;
    }
    bytes.iter().all(|byte| byte.is_ascii_alphanumeric() || *byte == b'_')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn command_handler_panics_are_reported_as_dispatch_errors() {
        let mut dispatcher = GCodeDispatcher::new();
        dispatcher
            .register_command("M999", true, None, |_| -> Result<Vec<DispatchEvent>, GCodeError> {
                panic!("handler panic");
            })
            .unwrap();
        let outcome = dispatcher.process_line("M999", true).unwrap();
        assert!(outcome.handler_found);
        assert!(matches!(outcome.events.first(), Some(DispatchEvent::Error(_))));
        assert!(outcome.acknowledged);
    }

    #[test]
    fn mux_handler_panics_are_reported_as_dispatch_errors() {
        let mut dispatcher = GCodeDispatcher::new();
        dispatcher
            .register_mux_command("SET_HEATER", "HEATER", Some("hotend"), None, |_| {
                panic!("mux handler panic");
            })
            .unwrap();
        let outcome = dispatcher
            .process_line("SET_HEATER HEATER=hotend", true)
            .unwrap();
        assert!(outcome.handler_found);
        assert!(matches!(outcome.events.first(), Some(DispatchEvent::Error(_))));
    }

    #[test]
    fn dispatcher_snapshot_keeps_handlers_alive_after_live_unregister() {
        let mut dispatcher = GCodeDispatcher::new();
        dispatcher
            .register_command("M999", true, None, |_| Ok(Vec::new()))
            .unwrap();
        let snapshot = dispatcher.clone();
        assert!(dispatcher.unregister_command("M999"));

        assert!(snapshot.process_line("M999", true).unwrap().handler_found);
        assert!(!dispatcher.process_line("M999", true).unwrap().handler_found);
    }

    #[test]
    fn in_flight_dispatcher_snapshot_survives_reset() {
        let mut dispatcher = GCodeDispatcher::new();
        dispatcher
            .register_command("M999", true, None, |_| Ok(Vec::new()))
            .unwrap();
        let snapshot = dispatcher.clone();
        dispatcher.reset();

        assert!(snapshot.process_line("M999", true).unwrap().handler_found);
        assert!(!dispatcher.process_line("M999", true).unwrap().handler_found);
    }

    #[test]
    fn reset_clears_handlers_muxes_and_ready_state() {
        let mut dispatcher = GCodeDispatcher::new();
        dispatcher
            .register_command("M999", true, None, |_| Ok(Vec::new()))
            .unwrap();
        dispatcher
            .register_mux_command("SET_HEATER", "HEATER", Some("hotend"), None, |_| {
                Ok(Vec::new())
            })
            .unwrap();
        dispatcher.set_ready();
        dispatcher.reset();

        assert!(!dispatcher.is_ready());
        assert!(dispatcher.registered_commands().is_empty());
        assert!(dispatcher.command_help().is_empty());
        let unknown = dispatcher.process_line("M999", true).unwrap();
        assert!(!unknown.handler_found);
        assert!(unknown
            .events
            .contains(&DispatchEvent::Error("Printer is not ready".to_owned())));
        assert!(dispatcher.process_line("M105", true).unwrap().handler_found);
    }

    #[test]
    fn only_base_commands_are_available_before_ready() {
        let mut dispatcher = GCodeDispatcher::new();
        dispatcher
            .register_command("M115", true, Some("Firmware version"), |_| {
                Ok(vec![DispatchEvent::Info("Klipper".to_owned())])
            })
            .unwrap();
        dispatcher
            .register_command("G1", false, None, |_| Ok(Vec::new()))
            .unwrap();

        assert!(dispatcher.process_line("M115", true).unwrap().handler_found);
        let blocked = dispatcher.process_line("G1 X1", true).unwrap();
        assert!(!blocked.handler_found);
        assert!(matches!(blocked.events.first(), Some(DispatchEvent::Error(_))));

        dispatcher.set_ready();
        assert!(dispatcher.process_line("G1 X1", true).unwrap().handler_found);
    }

    #[test]
    fn extended_commands_receive_shell_parsed_parameters() {
        let mut dispatcher = GCodeDispatcher::new();
        dispatcher.set_ready();
        dispatcher
            .register_command("RESPOND", false, None, |command| {
                assert_eq!(command.get("MSG"), Some("hello world"));
                Ok(vec![DispatchEvent::Info("received".to_owned())])
            })
            .unwrap();

        let result = dispatcher
            .process_line("RESPOND MSG=\"hello world\"", true)
            .unwrap();
        assert!(result.handler_found);
        assert_eq!(result.events.last(), Some(&DispatchEvent::Ack(None)));
    }

    #[test]
    fn handler_can_ack_with_a_payload_without_duplicate_ack() {
        let mut dispatcher = GCodeDispatcher::new();
        dispatcher.set_ready();
        dispatcher
            .register_command("M115", false, None, |_| {
                Ok(vec![DispatchEvent::Ack(Some("FIRMWARE_NAME:Klipper".to_owned()))])
            })
            .unwrap();

        let result = dispatcher.process_line("M115", true).unwrap();
        assert_eq!(
            result.events,
            vec![DispatchEvent::Ack(Some("FIRMWARE_NAME:Klipper".to_owned()))]
        );
    }


    #[test]
    fn pre_ready_temperature_query_only_acks_when_ack_is_requested() {
        let dispatcher = GCodeDispatcher::new();
        let interactive = dispatcher.process_line("M105", true).unwrap();
        assert_eq!(
            interactive.events,
            vec![DispatchEvent::Ack(Some("T:0".to_owned()))]
        );

        let script = dispatcher.process_line("M105", false).unwrap();
        assert!(script.events.is_empty());
        assert!(!script.acknowledged);
    }

    #[test]
    fn rejects_invalid_and_duplicate_registration_names() {
        let mut dispatcher = GCodeDispatcher::new();
        assert!(dispatcher
            .register_command("bad_name", false, None, |_| Ok(Vec::new()))
            .is_err());
        dispatcher
            .register_command("STATUS", true, None, |_| Ok(Vec::new()))
            .unwrap();
        assert!(dispatcher
            .register_command("STATUS", false, None, |_| Ok(Vec::new()))
            .is_err());
    }

    #[test]
    fn script_dispatch_does_not_emit_automatic_ack() {
        let mut dispatcher = GCodeDispatcher::new();
        dispatcher.set_ready();
        dispatcher
            .register_command("G1", false, None, |_| {
                Ok(vec![DispatchEvent::Info("move queued".to_owned())])
            })
            .unwrap();

        let result = dispatcher.process_line("G1 X10", false).unwrap();
        assert_eq!(result.events, vec![DispatchEvent::Info("move queued".to_owned())]);
        assert!(!result.acknowledged);
    }


    #[test]
    fn script_processes_each_nonempty_line_in_order_without_ack() {
        let mut dispatcher = GCodeDispatcher::new();
        dispatcher.set_ready();
        dispatcher
            .register_command("G1", false, None, |command| {
                Ok(vec![DispatchEvent::Info(command.commandline().to_owned())])
            })
            .unwrap();
        dispatcher
            .register_command("M400", false, None, |command| {
                Ok(vec![DispatchEvent::Info(command.commandline().to_owned())])
            })
            .unwrap();

        let results = dispatcher.process_script("G1 X1\n\nM400 ; wait\n");
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].command, "G1");
        assert_eq!(results[1].command, "M400");
        assert!(results.iter().all(|result| !result.acknowledged));
    }


    #[test]
    fn panicking_command_handler_becomes_error_event_and_dispatcher_recovers() {
        let mut dispatcher = GCodeDispatcher::new();
        dispatcher.set_ready();
        dispatcher
            .register_command("PANIC_NOW", false, None, |_| {
                panic!("handler panic");
            })
            .unwrap();
        dispatcher
            .register_command("AFTER_PANIC", false, None, |_| {
                Ok(vec![DispatchEvent::Info("still alive".to_owned())])
            })
            .unwrap();

        let panicked = dispatcher.process_line("PANIC_NOW", true).unwrap();
        assert!(matches!(
            panicked.events.as_slice(),
            [DispatchEvent::Error(message), DispatchEvent::Ack(None)]
                if message.contains("panicked")
        ));
        assert!(panicked.handler_found);
        assert!(panicked.acknowledged);

        let recovered = dispatcher.process_line("AFTER_PANIC", true).unwrap();
        assert_eq!(
            recovered.events,
            vec![
                DispatchEvent::Info("still alive".to_owned()),
                DispatchEvent::Ack(None),
            ]
        );
    }

    #[test]
    fn panicking_mux_handler_becomes_error_event() {
        let mut dispatcher = GCodeDispatcher::new();
        dispatcher.set_ready();
        dispatcher
            .register_mux_command("SET_MODE", "MODE", Some("panic"), None, |_| {
                panic!("mux handler panic");
            })
            .unwrap();

        let result = dispatcher
            .process_line("SET_MODE MODE=panic", true)
            .unwrap();
        assert!(matches!(
            result.events.as_slice(),
            [DispatchEvent::Error(message), DispatchEvent::Ack(None)]
                if message.contains("panicked")
        ));
    }

    #[test]
    fn mux_dispatch_selects_registered_value_handlers() {
        let mut dispatcher = GCodeDispatcher::new();
        dispatcher.set_ready();
        dispatcher
            .register_mux_command("SET_MODE", "MODE", Some("quiet"), None, |_| {
                Ok(vec![DispatchEvent::Info("quiet mode".to_owned())])
            })
            .unwrap();
        dispatcher
            .register_mux_command("SET_MODE", "MODE", Some("fast"), None, |_| {
                Ok(vec![DispatchEvent::Info("fast mode".to_owned())])
            })
            .unwrap();

        let result = dispatcher
            .process_line("SET_MODE MODE=fast", true)
            .unwrap();
        assert_eq!(result.events[0], DispatchEvent::Info("fast mode".to_owned()));
        assert!(result.handler_found);
    }

    #[test]
    fn mux_dispatch_supports_default_and_reports_invalid_values() {
        let mut dispatcher = GCodeDispatcher::new();
        dispatcher.set_ready();
        dispatcher
            .register_mux_command("SET_MODE", "MODE", None, None, |_| {
                Ok(vec![DispatchEvent::Info("default mode".to_owned())])
            })
            .unwrap();
        dispatcher
            .register_mux_command("SET_MODE", "MODE", Some("quiet"), None, |_| {
                Ok(vec![DispatchEvent::Info("quiet mode".to_owned())])
            })
            .unwrap();

        let default = dispatcher.process_line("SET_MODE", true).unwrap();
        assert_eq!(default.events[0], DispatchEvent::Info("default mode".to_owned()));

        let invalid = dispatcher
            .process_line("SET_MODE MODE=qui", true)
            .unwrap();
        assert!(matches!(invalid.events.first(), Some(DispatchEvent::Error(message)) if message.contains("Did you mean 'quiet'?")));
    }


    #[test]
    fn mux_without_default_requires_its_key_parameter() {
        let mut dispatcher = GCodeDispatcher::new();
        dispatcher.set_ready();
        dispatcher
            .register_mux_command("SET_MODE", "MODE", Some("quiet"), None, |_| {
                Ok(Vec::new())
            })
            .unwrap();

        let result = dispatcher.process_line("SET_MODE", true).unwrap();
        assert!(matches!(
            result.events.first(),
            Some(DispatchEvent::Error(message)) if message.contains("missing MODE")
        ));
    }

    #[test]
    fn mux_command_rejects_mixed_keys_and_duplicate_values() {
        let mut dispatcher = GCodeDispatcher::new();
        dispatcher
            .register_mux_command("SET_MODE", "MODE", Some("quiet"), None, |_| {
                Ok(Vec::new())
            })
            .unwrap();
        assert!(dispatcher
            .register_mux_command("SET_MODE", "PROFILE", Some("fast"), None, |_| {
                Ok(Vec::new())
            })
            .is_err());
        assert!(dispatcher
            .register_mux_command("SET_MODE", "MODE", Some("quiet"), None, |_| {
                Ok(Vec::new())
            })
            .is_err());
    }

    #[test]
    fn unregister_removes_command_from_both_tables() {
        let mut dispatcher = GCodeDispatcher::new();
        dispatcher
            .register_command("STATUS", true, None, |_| Ok(Vec::new()))
            .unwrap();
        assert!(dispatcher.unregister_command("STATUS"));
        assert!(!dispatcher.process_line("STATUS", true).unwrap().handler_found);
    }
}
