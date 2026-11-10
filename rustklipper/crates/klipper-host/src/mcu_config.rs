//! Host-side assembly of component-provided MCU configuration commands.
//!
//! Components allocate OIDs and append the exact Klipper command lines they
//! require. The builder preserves declaration order; pin aliases/reservations
//! are resolved before command encoding and config CRC calculation.
use klipper_protocol::IdentifyDictionary;
use klipper_transport::{ConfigError, McuConfigPlan, OidAllocator, PinResolver, PinResolverError};

/// Collects one MCU's host-side configuration declarations.
///
/// This type does not infer firmware commands from generic printer config.
/// Each component must contribute the commands corresponding to its own
/// registered hardware objects.
#[derive(Debug, Clone)]
pub struct HostMcuConfigBuilder {
    oids: OidAllocator,
    pins: PinResolver,
    config_lines: Vec<String>,
    restart_lines: Vec<String>,
    init_lines: Vec<String>,
}

impl Default for HostMcuConfigBuilder {
    fn default() -> Self {
        Self::new(true)
    }
}

impl HostMcuConfigBuilder {
    /// Create a builder. Alias validation should normally remain enabled.
    pub fn new(validate_pin_aliases: bool) -> Self {
        Self {
            oids: OidAllocator::new(),
            pins: PinResolver::new(validate_pin_aliases),
            config_lines: Vec::new(),
            restart_lines: Vec::new(),
            init_lines: Vec::new(),
        }
    }

    /// Allocate a stable one-byte firmware OID for a named host object.
    pub fn allocate_oid(&mut self, name: impl Into<String>) -> Result<u8, ConfigError> {
        self.oids.allocate_named(name)
    }

    pub fn oid(&self, name: &str) -> Option<u8> {
        self.oids.get(name)
    }

    pub fn oid_count(&self) -> u8 {
        self.oids.count()
    }

    pub fn alias_pin(
        &mut self,
        alias: impl Into<String>,
        physical_pin: impl Into<String>,
    ) -> Result<(), PinResolverError> {
        self.pins.alias_pin(alias, physical_pin)
    }

    pub fn reserve_pin(
        &mut self,
        pin: impl Into<String>,
        owner: impl Into<String>,
    ) -> Result<(), PinResolverError> {
        self.pins.reserve_pin(pin, owner)
    }

    /// Append a command included in the config CRC and full-config sequence.
    pub fn add_config_command(&mut self, command: impl Into<String>) {
        self.config_lines.push(command.into());
    }

    /// Append a command sent when the MCU already has the matching config.
    pub fn add_restart_command(&mut self, command: impl Into<String>) {
        self.restart_lines.push(command.into());
    }

    /// Append a command sent after either full config or restart handling.
    pub fn add_init_command(&mut self, command: impl Into<String>) {
        self.init_lines.push(command.into());
    }

    pub fn config_commands(&self) -> &[String] {
        &self.config_lines
    }

    pub fn restart_commands(&self) -> &[String] {
        &self.restart_lines
    }

    pub fn init_commands(&self) -> &[String] {
        &self.init_lines
    }

    /// Resolve pins, encode the declared commands with the MCU identify
    /// dictionary, and calculate the exact Klipper config CRC.
    pub fn build(
        &self,
        dictionary: &IdentifyDictionary,
    ) -> Result<McuConfigPlan, ConfigError> {
        McuConfigPlan::from_unresolved_command_lines(
            dictionary,
            self.oids.count(),
            &self.config_lines,
            &self.restart_lines,
            &self.init_lines,
            &mut self.pins.clone(),
        )
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

    #[test]
    fn builder_allocates_oids_and_preserves_component_command_order() {
        let mut builder = HostMcuConfigBuilder::default();
        let first = builder.allocate_oid("heater").unwrap();
        let second = builder.allocate_oid("fan").unwrap();
        assert_eq!((first, second), (0, 1));
        assert_eq!(builder.oid("heater"), Some(0));
        assert_eq!(builder.oid_count(), 2);

        builder.add_config_command(format!(
            "config_pin oid={first} pin=LED value=1"
        ));
        builder.add_config_command(format!(
            "config_pin oid={second} pin=PA0 value=0"
        ));
        builder.add_restart_command(format!("restart_pin oid={first} value=0"));
        builder.add_init_command(format!("init_pin oid={second} value=1"));

        // The builder's own resolver is configured through its public API.
        let mut builder = builder;
        builder.alias_pin("LED", "PA1").unwrap();
        let plan = builder.build(&dictionary()).unwrap();
        let expected = McuConfigPlan::from_command_lines(
            &dictionary(),
            2,
            &[
                "config_pin oid=0 pin=PA1 value=1".to_owned(),
                "config_pin oid=1 pin=PA0 value=0".to_owned(),
            ],
            &["restart_pin oid=0 value=0".to_owned()],
            &["init_pin oid=1 value=1".to_owned()],
        ).unwrap();
        assert_eq!(plan.crc(), expected.crc());
    }

    #[test]
    fn duplicate_oid_names_are_rejected_without_consuming_another_oid() {
        let mut builder = HostMcuConfigBuilder::default();
        assert_eq!(builder.allocate_oid("stepper").unwrap(), 0);
        assert!(matches!(
            builder.allocate_oid("stepper"),
            Err(ConfigError::DuplicateOidName(name)) if name == "stepper"
        ));
        assert_eq!(builder.allocate_oid("endstop").unwrap(), 1);
        assert_eq!(builder.oid_count(), 2);
    }

    #[test]
    fn reserved_pin_conflicts_are_reported_before_plan_creation() {
        let mut builder = HostMcuConfigBuilder::default();
        builder.reserve_pin("PA1", "serial").unwrap();
        builder.add_config_command("config_pin oid=0 pin=PA1 value=1");
        assert!(matches!(
            builder.build(&dictionary()),
            Err(ConfigError::Pin(PinResolverError::ReservedPin { .. }))
        ));
    }
}
