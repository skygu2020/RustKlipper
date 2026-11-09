//! MCU configuration handshake planning, matching Klipper's get_config flow.
use std::collections::BTreeMap;

use klipper_protocol::IdentifyDictionary;
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
        if response.crc != self.crc {
            return Err(ConfigError::CrcMismatch { firmware: response.crc, host: self.crc });
        }
        Ok(response.move_count)
    }
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
    fn rejects_shutdown_crc_mismatch_and_failed_finalize() {
        let plan = McuConfigPlan::new(&dictionary(), 0, &[], vec![], vec![], vec![]).unwrap();
        assert!(matches!(plan.initial_action(response(0, 0, 1, 10), false), Err(ConfigError::Shutdown)));
        assert!(matches!(plan.initial_action(response(1, 0, 0, 10), false), Err(ConfigError::CrcMismatch { .. })));
        assert!(matches!(plan.validate_final_response(response(0, 0, 0, 10)), Err(ConfigError::NotConfigured)));
    }
}
