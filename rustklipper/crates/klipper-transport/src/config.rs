//! MCU configuration handshake planning, matching Klipper's get_config flow.
use std::collections::BTreeMap;

use klipper_protocol::IdentifyDictionary;
use crate::{PinResolver, PinResolverError};
use serde_json::Value;
use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConfigResponse {
    pub is_config: bool,
    pub crc: u32,
    pub is_shutdown: bool,
    pub move_count: u16,
}

impl ConfigResponse {
    pub fn from_params(params: &BTreeMap<String, Value>) -> Result<Self, ConfigError> {
        fn number(params: &BTreeMap<String, Value>, name: &'static str) -> Result<u64, ConfigError> {
            params.get(name)
                .and_then(|v| v.as_u64().or_else(|| v.as_i64().and_then(|n| u64::try_from(n).ok())))
                .ok_or(ConfigError::MissingField(name))
        }
        let is_config = number(params, "is_config")?;
        let is_shutdown = number(params, "is_shutdown")?;
        if is_config > 1 || is_shutdown > 1 {
            return Err(ConfigError::InvalidBoolean);
        }
        Ok(Self {
            is_config: is_config != 0,
            crc: u32::try_from(number(params, "crc")?).map_err(|_| ConfigError::IntegerRange("crc"))?,
            is_shutdown: is_shutdown != 0,
            move_count: u16::try_from(number(params, "move_count")?).map_err(|_| ConfigError::IntegerRange("move_count"))?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfigAction {
    SendFullConfig { commands: Vec<Vec<u8>>, crc: u32 },
    SendRestartAndInit { commands: Vec<Vec<u8>>, crc: u32 },
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("missing config response field: {0}")]
    MissingField(&'static str),
    #[error("config response boolean field is not 0 or 1")]
    InvalidBoolean,
    #[error("config response field outside supported range: {0}")]
    IntegerRange(&'static str),
    #[error("MCU is shutdown; configuration cannot proceed")]
    Shutdown,
    #[error("MCU config CRC mismatch: firmware={firmware:#010x}, host={host:#010x}")]
    CrcMismatch { firmware: u32, host: u32 },
    #[error("firmware restart did not reset MCU configuration")]
    RestartFailed,
    #[error("MCU did not enter configured state after configuration")]
    NotConfigured,
    #[error("config command text and encoded command counts differ")]
    CommandCountMismatch,
    #[error("MCU reports {reported} move slots, fewer than {reserved} reserved slots")]
    TooFewMoves { reported: u16, reserved: u16 },
    #[error("MCU OID space exhausted (maximum 255 allocated OIDs)")]
    OidExhausted,
    #[error("OID name already allocated: {0}")]
    DuplicateOidName(String),
    #[error(transparent)]
    Pin(#[from] PinResolverError),
    #[error("invalid MCU configuration handshake stage: expected {expected}, got {actual}")]
    UnexpectedHandshakeStage { expected: &'static str, actual: &'static str },
    #[error(transparent)]
    Dictionary(#[from] klipper_protocol::DictionaryError),
}

/// Immutable command plan for one MCU configuration attempt.
///
/// config_lines must be the exact pin-resolved textual config commands, in
/// transmission order. config_commands are their dictionary-encoded wire forms.
#[derive(Debug, Clone)]
pub struct McuConfigPlan {
    crc: u32,
    allocate_oids: Vec<u8>,
    finalize_config: Vec<u8>,
    config_commands: Vec<Vec<u8>>,
    restart_commands: Vec<Vec<u8>>,
    init_commands: Vec<Vec<u8>>,
}

impl McuConfigPlan {
    pub fn new(
        dictionary: &IdentifyDictionary,
        oid_count: u8,
        config_lines: &[String],
        config_commands: Vec<Vec<u8>>,
        restart_commands: Vec<Vec<u8>>,
        init_commands: Vec<Vec<u8>>,
    ) -> Result<Self, ConfigError> {
        if config_lines.len() != config_commands.len() {
            return Err(ConfigError::CommandCountMismatch);
        }
        let allocate_line = format!("allocate_oids count={oid_count}");
        let mut crc_input = allocate_line.into_bytes();
        for line in config_lines {
            crc_input.push(b'\n');
            crc_input.extend_from_slice(line.as_bytes());
        }
        let crc = crc32fast::hash(&crc_input);

        let mut allocate_values = BTreeMap::new();
        allocate_values.insert("count".into(), Value::from(oid_count));
        let allocate_oids = dictionary.encode_command("allocate_oids", &allocate_values)?;

        let mut finalize_values = BTreeMap::new();
        finalize_values.insert("crc".into(), Value::from(crc));
        let finalize_config = dictionary.encode_command("finalize_config", &finalize_values)?;

        Ok(Self {
            crc, allocate_oids, finalize_config,
            config_commands, restart_commands, init_commands,
        })
    }


    /// Build a complete plan directly from Klipper-style textual command lines.
    /// The config lines must already be pin-resolved and in firmware send order.
    pub fn from_command_lines(
        dictionary: &IdentifyDictionary,
        oid_count: u8,
        config_lines: &[String],
        restart_lines: &[String],
        init_lines: &[String],
    ) -> Result<Self, ConfigError> {
        let encode_all = |lines: &[String]| -> Result<Vec<Vec<u8>>, ConfigError> {
            lines.iter()
                .map(|line| dictionary.encode_command_line(line).map_err(ConfigError::from))
                .collect()
        };
        Self::new(
            dictionary,
            oid_count,
            config_lines,
            encode_all(config_lines)?,
            encode_all(restart_lines)?,
            encode_all(init_lines)?,
        )
    }


    /// Resolve board aliases/reservations in all command groups before encoding
    /// and CRC calculation, matching Klipper's MCU finalize-config ordering.
    pub fn from_unresolved_command_lines(
        dictionary: &IdentifyDictionary,
        oid_count: u8,
        config_lines: &[String],
        restart_lines: &[String],
        init_lines: &[String],
        resolver: &mut PinResolver,
    ) -> Result<Self, ConfigError> {
        let resolve = |lines: &[String], resolver: &mut PinResolver|
            -> Result<Vec<String>, ConfigError> {
            lines.iter()
                .map(|line| resolver.update_command(line).map_err(ConfigError::from))
                .collect()
        };
        // Resolve against a working copy so a failed plan does not leave
        // partially activated pins in the caller's resolver. A caller may
        // correct the invalid command and retry with the same resolver.
        let mut candidate = resolver.clone();
        let resolved_config = resolve(config_lines, &mut candidate)?;
        let resolved_restart = resolve(restart_lines, &mut candidate)?;
        let resolved_init = resolve(init_lines, &mut candidate)?;
        let plan = Self::from_command_lines(
            dictionary,
            oid_count,
            &resolved_config,
            &resolved_restart,
            &resolved_init,
        )?;
        *resolver = candidate;
        Ok(plan)
    }

    pub fn crc(&self) -> u32 { self.crc }

    /// Decide what to send after the initial get_config response.
    pub fn initial_action(
        &self,
        response: ConfigResponse,
        firmware_restart: bool,
    ) -> Result<ConfigAction, ConfigError> {
        if response.is_shutdown { return Err(ConfigError::Shutdown); }
        if !response.is_config {
            let mut commands = Vec::with_capacity(
                2 + self.config_commands.len() + self.init_commands.len()
            );
            commands.push(self.allocate_oids.clone());
            commands.extend(self.config_commands.iter().cloned());
            commands.push(self.finalize_config.clone());
            commands.extend(self.init_commands.iter().cloned());
            return Ok(ConfigAction::SendFullConfig { commands, crc: self.crc });
        }
        if firmware_restart { return Err(ConfigError::RestartFailed); }
        if response.crc != self.crc {
            return Err(ConfigError::CrcMismatch { firmware: response.crc, host: self.crc });
        }
        let mut commands = Vec::with_capacity(self.restart_commands.len() + self.init_commands.len());
        commands.extend(self.restart_commands.iter().cloned());
        commands.extend(self.init_commands.iter().cloned());
        Ok(ConfigAction::SendRestartAndInit { commands, crc: self.crc })
    }

    /// Validate the second get_config response and return the MCU move capacity.
    pub fn validate_final_response(&self, response: ConfigResponse) -> Result<u16, ConfigError> {
        if response.is_shutdown { return Err(ConfigError::Shutdown); }
        if !response.is_config { return Err(ConfigError::NotConfigured); }
        Ok(response.move_count)
    }

    /// Validate the final get_config response and subtract host-reserved move slots.
    pub fn usable_move_count(
        &self,
        response: ConfigResponse,
        reserved_slots: u16,
    ) -> Result<u16, ConfigError> {
        let reported = self.validate_final_response(response)?;
        reported.checked_sub(reserved_slots).ok_or(ConfigError::TooFewMoves {
            reported, reserved: reserved_slots,
        })
    }
}


#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigHandshakeStage {
    AwaitingInitialResponse,
    AwaitingFinalResponse,
    Complete { usable_move_count: u16 },
}

/// Stateful two-query MCU configuration handshake.
///
/// The caller sends get_config, passes its decoded response to
/// handle_initial_response, enqueues the returned action, then sends get_config
/// again and passes that response to handle_final_response.
#[derive(Debug, Clone)]
pub struct McuConfigHandshake {
    plan: McuConfigPlan,
    reserved_move_slots: u16,
    stage: ConfigHandshakeStage,
}

impl McuConfigHandshake {
    pub fn new(plan: McuConfigPlan, reserved_move_slots: u16) -> Self {
        Self {
            plan,
            reserved_move_slots,
            stage: ConfigHandshakeStage::AwaitingInitialResponse,
        }
    }

    pub fn stage(&self) -> ConfigHandshakeStage { self.stage }

    pub fn handle_initial_response(
        &mut self,
        response: ConfigResponse,
        firmware_restart: bool,
    ) -> Result<ConfigAction, ConfigError> {
        if self.stage != ConfigHandshakeStage::AwaitingInitialResponse {
            return Err(ConfigError::UnexpectedHandshakeStage {
                expected: "awaiting initial get_config response",
                actual: self.stage_name(),
            });
        }
        let action = self.plan.initial_action(response, firmware_restart)?;
        self.stage = ConfigHandshakeStage::AwaitingFinalResponse;
        Ok(action)
    }

    pub fn handle_final_response(
        &mut self,
        response: ConfigResponse,
    ) -> Result<u16, ConfigError> {
        if self.stage != ConfigHandshakeStage::AwaitingFinalResponse {
            return Err(ConfigError::UnexpectedHandshakeStage {
                expected: "awaiting final get_config response",
                actual: self.stage_name(),
            });
        }
        let usable = self.plan.usable_move_count(response, self.reserved_move_slots)?;
        self.stage = ConfigHandshakeStage::Complete { usable_move_count: usable };
        Ok(usable)
    }

    fn stage_name(&self) -> &'static str {
        match self.stage {
            ConfigHandshakeStage::AwaitingInitialResponse => "awaiting initial response",
            ConfigHandshakeStage::AwaitingFinalResponse => "awaiting final response",
            ConfigHandshakeStage::Complete { .. } => "complete",
        }
    }
}

/// Sequential MCU object-id allocator. Firmware OIDs are one-byte values and
/// allocate_oids count is also a byte, so at most 255 IDs can be reserved.
#[derive(Debug, Clone, Default)]
pub struct OidAllocator {
    next: u16,
    names: BTreeMap<String, u8>,
}

impl OidAllocator {
    pub fn new() -> Self { Self::default() }

    pub fn allocate(&mut self) -> Result<u8, ConfigError> {
        if self.next >= u16::from(u8::MAX) {
            return Err(ConfigError::OidExhausted);
        }
        let oid = self.next as u8;
        self.next += 1;
        Ok(oid)
    }

    pub fn allocate_named(&mut self, name: impl Into<String>) -> Result<u8, ConfigError> {
        let name = name.into();
        if self.names.contains_key(&name) {
            return Err(ConfigError::DuplicateOidName(name));
        }
        let oid = self.allocate()?;
        self.names.insert(name, oid);
        Ok(oid)
    }

    pub fn get(&self, name: &str) -> Option<u8> { self.names.get(name).copied() }

    pub fn count(&self) -> u8 { self.next as u8 }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dictionary() -> IdentifyDictionary {
        IdentifyDictionary::from_json(br#"{
            "enumerations": {"pin": {"PA0": 1, "PA1": 2}},
            "commands": {
                "allocate_oids count=%c": 1,
                "finalize_config crc=%u": 2,
                "config_pin oid=%c pin=%u value=%u": 3,
                "restart_pin oid=%c value=%u": 4,
                "init_pin oid=%c value=%u": 5
            },
            "responses": {}
        }"#).unwrap()
    }

    fn response(is_config: u64, crc: u64, is_shutdown: u64, move_count: u64) -> ConfigResponse {
        let params = BTreeMap::from([
            ("is_config".into(), Value::from(is_config)),
            ("crc".into(), Value::from(crc)),
            ("is_shutdown".into(), Value::from(is_shutdown)),
            ("move_count".into(), Value::from(move_count)),
        ]);
        ConfigResponse::from_params(&params).unwrap()
    }

    #[test]
    fn unconfigured_mcu_gets_allocate_config_finalize_then_init() {
        let plan = McuConfigPlan::new(
            &dictionary(), 2, &["config_a oid=0".into()],
            vec![vec![10]], vec![vec![20]], vec![vec![30]],
        ).unwrap();
        let action = plan.initial_action(response(0, 0, 0, 100), false).unwrap();
        let ConfigAction::SendFullConfig { commands, crc } = action else { panic!("expected full config") };
        assert_eq!(crc, plan.crc());
        assert_eq!(commands, vec![
            dictionary().encode_command("allocate_oids", &BTreeMap::from([("count".into(), Value::from(2))])).unwrap(),
            vec![10],
            dictionary().encode_command("finalize_config", &BTreeMap::from([("crc".into(), Value::from(crc))])).unwrap(),
            vec![30],
        ]);
    }


    #[test]
    fn textual_plan_encodes_all_command_groups_and_uses_exact_config_text_crc() {
        let dictionary = dictionary();
        let config_lines = vec!["config_pin oid=0 pin=PA1 value=1".to_owned()];
        let restart_lines = vec!["restart_pin oid=0 value=0".to_owned()];
        let init_lines = vec!["init_pin oid=0 value=1".to_owned()];
        let plan = McuConfigPlan::from_command_lines(
            &dictionary, 1, &config_lines, &restart_lines, &init_lines,
        ).unwrap();

        let mut crc_input = b"allocate_oids count=1".to_vec();
        crc_input.extend_from_slice(b"\nconfig_pin oid=0 pin=PA1 value=1");
        assert_eq!(plan.crc(), crc32fast::hash(&crc_input));

        let action = plan.initial_action(response(0, 0, 0, 32), false).unwrap();
        let ConfigAction::SendFullConfig { commands, .. } = action else {
            panic!("expected full configuration");
        };
        assert_eq!(commands[1], dictionary.encode_command_line(&config_lines[0]).unwrap());
        assert_eq!(commands[3], dictionary.encode_command_line(&init_lines[0]).unwrap());

        let existing = plan.initial_action(response(1, u64::from(plan.crc()), 0, 32), false).unwrap();
        let ConfigAction::SendRestartAndInit { commands, .. } = existing else {
            panic!("expected restart and init commands");
        };
        assert_eq!(commands, vec![
            dictionary.encode_command_line(&restart_lines[0]).unwrap(),
            dictionary.encode_command_line(&init_lines[0]).unwrap(),
        ]);
    }


    #[test]
    fn failed_unresolved_plan_does_not_mutate_pin_resolver() {
        let dictionary = dictionary();
        let mut resolver = PinResolver::new(true);
        resolver.alias_pin("LED", "PA1").unwrap();
        resolver.reserve_pin("PA2", "serial").unwrap();

        let result = McuConfigPlan::from_unresolved_command_lines(
            &dictionary,
            1,
            &[
                "config_pin oid=0 pin=LED value=1".to_owned(),
                "config_pin oid=0 pin=PA2 value=0".to_owned(),
            ],
            &[],
            &[],
            &mut resolver,
        );
        assert!(matches!(result, Err(ConfigError::Pin(PinResolverError::ReservedPin { .. }))));

        // The failed attempt must not leave LED active as PA1; retrying with
        // the physical pin is valid when the earlier attempt was rolled back.
        let retry = McuConfigPlan::from_unresolved_command_lines(
            &dictionary,
            1,
            &["config_pin oid=0 pin=PA1 value=1".to_owned()],
            &[],
            &[],
            &mut resolver,
        );
        assert!(retry.is_ok());
    }

    #[test]
    fn unresolved_plan_resolves_aliases_before_crc_and_encoding() {
        let dictionary = dictionary();
        let config_lines = vec!["config_pin oid=0 pin=LED value=1".to_owned()];
        let restart_lines = vec!["restart_pin oid=0 value=0".to_owned()];
        let init_lines = vec!["init_pin oid=0 value=1".to_owned()];
        let mut resolver = PinResolver::new(true);
        resolver.alias_pin("LED", "PA1").unwrap();
        let plan = McuConfigPlan::from_unresolved_command_lines(
            &dictionary, 1, &config_lines, &restart_lines, &init_lines, &mut resolver,
        ).unwrap();

        let mut expected_crc = b"allocate_oids count=1".to_vec();
        expected_crc.extend_from_slice(b"\nconfig_pin oid=0 pin=PA1 value=1");
        assert_eq!(plan.crc(), crc32fast::hash(&expected_crc));
        let action = plan.initial_action(response(0, 0, 0, 32), false).unwrap();
        let ConfigAction::SendFullConfig { commands, .. } = action else {
            panic!("expected full configuration");
        };
        assert_eq!(commands[1], dictionary.encode_command_line(
            "config_pin oid=0 pin=PA1 value=1"
        ).unwrap());
    }

    #[test]
    fn configured_mcu_gets_only_restart_and_init_commands() {
        let plan = McuConfigPlan::new(
            &dictionary(), 0, &[], vec![], vec![vec![20]], vec![vec![30]],
        ).unwrap();
        let action = plan.initial_action(response(1, u64::from(plan.crc()), 0, 64), false).unwrap();
        assert_eq!(action, ConfigAction::SendRestartAndInit {
            commands: vec![vec![20], vec![30]], crc: plan.crc()
        });
        assert_eq!(plan.validate_final_response(response(1, u64::from(plan.crc()), 0, 64)).unwrap(), 64);
    }


    #[test]
    fn move_slots_subtract_reserved_capacity_and_reject_underflow() {
        let plan = McuConfigPlan::new(&dictionary(), 0, &[], vec![], vec![], vec![]).unwrap();
        assert_eq!(
            plan.usable_move_count(response(1, 123, 0, 64), 8).unwrap(),
            56,
        );
        assert!(matches!(
            plan.usable_move_count(response(1, 123, 0, 4), 8),
            Err(ConfigError::TooFewMoves { reported: 4, reserved: 8 })
        ));
    }


    #[test]
    fn config_handshake_requires_initial_then_final_query_responses() {
        let plan = McuConfigPlan::new(
            &dictionary(), 0, &[], vec![], vec![], vec![],
        ).unwrap();
        let mut handshake = McuConfigHandshake::new(plan.clone(), 8);
        assert_eq!(handshake.stage(), ConfigHandshakeStage::AwaitingInitialResponse);
        let action = handshake.handle_initial_response(
            response(0, 0, 0, 64), false,
        ).unwrap();
        assert!(matches!(action, ConfigAction::SendFullConfig { .. }));
        assert_eq!(handshake.stage(), ConfigHandshakeStage::AwaitingFinalResponse);
        assert!(matches!(
            handshake.handle_initial_response(response(0, 0, 0, 64), false),
            Err(ConfigError::UnexpectedHandshakeStage { .. })
        ));
        assert_eq!(handshake.handle_final_response(response(1, 0, 0, 64)).unwrap(), 56);
        assert_eq!(
            handshake.stage(),
            ConfigHandshakeStage::Complete { usable_move_count: 56 },
        );
        assert!(matches!(
            handshake.handle_final_response(response(1, 0, 0, 64)),
            Err(ConfigError::UnexpectedHandshakeStage { .. })
        ));
    }

    #[test]
    fn failed_handshake_responses_leave_each_stage_retryable() {
        let plan = McuConfigPlan::new(
            &dictionary(), 0, &[], vec![], vec![], vec![],
        ).unwrap();
        let mut handshake = McuConfigHandshake::new(plan.clone(), 8);

        assert!(matches!(
            handshake.handle_initial_response(response(1, 0, 0, 64), false),
            Err(ConfigError::CrcMismatch { .. })
        ));
        assert_eq!(handshake.stage(), ConfigHandshakeStage::AwaitingInitialResponse);

        let action = handshake.handle_initial_response(response(0, 0, 0, 64), false).unwrap();
        assert!(matches!(action, ConfigAction::SendFullConfig { .. }));
        assert_eq!(handshake.stage(), ConfigHandshakeStage::AwaitingFinalResponse);

        assert!(matches!(
            handshake.handle_final_response(response(1, 0, 0, 4)),
            Err(ConfigError::TooFewMoves { reported: 4, reserved: 8 })
        ));
        assert_eq!(handshake.stage(), ConfigHandshakeStage::AwaitingFinalResponse);

        assert_eq!(handshake.handle_final_response(response(1, 0, 0, 64)).unwrap(), 56);
        assert_eq!(
            handshake.stage(),
            ConfigHandshakeStage::Complete { usable_move_count: 56 },
        );
    }

    #[test]
    fn oid_allocator_assigns_stable_named_ids_and_enforces_limit() {
        let mut ids = OidAllocator::new();
        assert_eq!(ids.allocate_named("stepper_x").unwrap(), 0);
        assert_eq!(ids.allocate_named("heater").unwrap(), 1);
        assert_eq!(ids.get("stepper_x"), Some(0));
        assert!(matches!(ids.allocate_named("stepper_x"), Err(ConfigError::DuplicateOidName(_))));
        for _ in 2..u8::MAX { ids.allocate().unwrap(); }
        assert_eq!(ids.count(), u8::MAX);
        assert!(matches!(ids.allocate(), Err(ConfigError::OidExhausted)));
    }

    #[test]
    fn rejects_shutdown_crc_mismatch_and_failed_finalize() {
        let plan = McuConfigPlan::new(&dictionary(), 0, &[], vec![], vec![], vec![]).unwrap();
        assert!(matches!(plan.initial_action(response(0, 0, 1, 10), false), Err(ConfigError::Shutdown)));
        assert!(matches!(plan.initial_action(response(1, 0, 0, 10), false), Err(ConfigError::CrcMismatch { .. })));
        assert!(matches!(plan.validate_final_response(response(0, 0, 0, 10)), Err(ConfigError::NotConfigured)));
    }
}
