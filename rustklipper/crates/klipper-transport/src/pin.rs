//! MCU pin aliases and command-line pin resolution, matching Klipper pins.py.
use std::collections::BTreeMap;
use thiserror::Error;

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum PinResolverError {
    #[error("pin {pin} is reserved for {owner}; cannot reserve it for {requested}")]
    ReservationConflict { pin: String, owner: String, requested: String },
    #[error("alias {alias} already maps to {existing}; cannot remap to {requested}")]
    AliasConflict { alias: String, existing: String, requested: String },
    #[error("invalid pin alias target: {0}")]
    InvalidAliasTarget(String),
    #[error("pin {pin} is an alias for {active}")]
    AliasUsedAlongsideTarget { pin: String, active: String },
    #[error("pin {pin} is reserved for {owner}")]
    ReservedPin { pin: String, owner: String },
}

/// Resolves board pin aliases and validates alias use in MCU config commands.
#[derive(Debug, Clone)]
pub struct PinResolver {
    validate_aliases: bool,
    reserved: BTreeMap<String, String>,
    aliases: BTreeMap<String, String>,
    active_pins: BTreeMap<String, String>,
}

impl Default for PinResolver {
    fn default() -> Self { Self::new(true) }
}

impl PinResolver {
    pub fn new(validate_aliases: bool) -> Self {
        Self {
            validate_aliases,
            reserved: BTreeMap::new(),
            aliases: BTreeMap::new(),
            active_pins: BTreeMap::new(),
        }
    }

    pub fn reserve_pin(
        &mut self,
        pin: impl Into<String>,
        reserve_name: impl Into<String>,
    ) -> Result<(), PinResolverError> {
        let pin = pin.into();
        let reserve_name = reserve_name.into();
        if let Some(existing) = self.reserved.get(&pin) {
            if existing != &reserve_name {
                return Err(PinResolverError::ReservationConflict {
                    pin, owner: existing.clone(), requested: reserve_name,
                });
            }
        }
        self.reserved.insert(pin, reserve_name);
        Ok(())
    }

    pub fn alias_pin(
        &mut self,
        alias: impl Into<String>,
        pin: impl Into<String>,
    ) -> Result<(), PinResolverError> {
        let alias = alias.into();
        let mut pin = pin.into();
        if let Some(existing) = self.aliases.get(&alias) {
            if existing != &pin {
                return Err(PinResolverError::AliasConflict {
                    alias, existing: existing.clone(), requested: pin,
                });
            }
        }
        if pin.chars().any(|c| matches!(c, '^' | '~' | '!' | ':'))
            || pin.chars().any(char::is_whitespace)
        {
            return Err(PinResolverError::InvalidAliasTarget(pin));
        }
        if let Some(target) = self.aliases.get(&pin) {
            pin = target.clone();
        }
        self.aliases.insert(alias.clone(), pin.clone());
        for target in self.aliases.values_mut() {
            if target.as_str() == alias.as_str() { *target = pin.clone(); }
        }
        Ok(())
    }

    /// Rewrite pin fields preceded by a space or underscore, as Klipper's
    /// re_pin expression does (for example "pin=PA0" in a config command).
    pub fn update_command(&mut self, command: &str) -> Result<String, PinResolverError> {
        let bytes = command.as_bytes();
        let mut output = String::with_capacity(command.len());
        let mut cursor = 0usize;
        let mut pos = 0usize;

        while pos + 5 <= bytes.len() {
            let has_prefix = bytes[pos] == b' ' || bytes[pos] == b'_';
            if has_prefix && bytes.get(pos + 1..pos + 5) == Some(&b"pin="[..]) {
                let name_start = pos + 5;
                let end = bytes[name_start..].iter()
                    .position(|b| *b == b' ')
                    .map_or(bytes.len(), |offset| name_start + offset);
                let name = &command[name_start..end];
                let pin_id = self.aliases.get(name).cloned().unwrap_or_else(|| name.to_owned());

                // Validate reservations before recording this pin as active.
                // A rejected command must not poison subsequent attempts with
                // an alias-vs-physical-pin conflict.
                if let Some(owner) = self.reserved.get(&pin_id) {
                    return Err(PinResolverError::ReservedPin {
                        pin: name.to_owned(), owner: owner.clone(),
                    });
                }

                if self.validate_aliases {
                    if let Some(active) = self.active_pins.get(&pin_id) {
                        if name != active {
                            return Err(PinResolverError::AliasUsedAlongsideTarget {
                                pin: name.to_owned(), active: active.clone(),
                            });
                        }
                    }
                }
                self.active_pins.entry(pin_id.clone()).or_insert_with(|| name.to_owned());

                output.push_str(&command[cursor..name_start]);
                output.push_str(&pin_id);
                cursor = end;
                pos = end;
            } else {
                pos += 1;
            }
        }
        output.push_str(&command[cursor..]);
        Ok(output)
    }

    pub fn aliases(&self) -> &BTreeMap<String, String> { &self.aliases }
    pub fn reserved(&self) -> &BTreeMap<String, String> { &self.reserved }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn aliases_rewrite_pin_fields_and_keep_prefixes() {
        let mut resolver = PinResolver::new(true);
        resolver.alias_pin("LED", "PA1").unwrap();
        assert_eq!(
            resolver.update_command("config_digital_out oid=1 pin=LED value=0").unwrap(),
            "config_digital_out oid=1 pin=PA1 value=0",
        );
        assert_eq!(
            resolver.update_command("config_spi oid=2 cs_pin=LED").unwrap(),
            "config_spi oid=2 cs_pin=PA1",
        );
    }


    #[test]
    fn pin_rewrite_matches_only_prefixed_pin_fields() {
        let mut resolver = PinResolver::new(true);
        resolver.alias_pin("LED", "PA1").unwrap();
        assert_eq!(resolver.update_command("pin=LED").unwrap(), "pin=LED");
        assert_eq!(resolver.update_command("config_a pin=LED").unwrap(), "config_a pin=PA1");
        assert_eq!(resolver.update_command("config_b cs_pin=LED").unwrap(), "config_b cs_pin=PA1");
    }

    #[test]
    fn alias_and_physical_pin_cannot_both_be_active() {
        let mut resolver = PinResolver::new(true);
        resolver.alias_pin("LED", "PA1").unwrap();
        resolver.update_command("config_a pin=PA1").unwrap();
        assert!(matches!(
            resolver.update_command("config_b pin=LED"),
            Err(PinResolverError::AliasUsedAlongsideTarget { .. })
        ));
    }

    #[test]
    fn reserved_pins_are_rejected_after_alias_resolution() {
        let mut resolver = PinResolver::new(true);
        resolver.alias_pin("LED", "PA1").unwrap();
        resolver.reserve_pin("PA1", "serial").unwrap();
        assert!(matches!(
            resolver.update_command("config_a pin=LED"),
            Err(PinResolverError::ReservedPin { .. })
        ));
        assert!(matches!(
            resolver.reserve_pin("PA1", "spi"),
            Err(PinResolverError::ReservationConflict { .. })
        ));
    }

    #[test]
    fn alias_chains_are_flattened_and_bad_targets_rejected() {
        let mut resolver = PinResolver::new(true);
        resolver.alias_pin("LED", "PA1").unwrap();
        resolver.alias_pin("STATUS", "LED").unwrap();
        assert_eq!(resolver.aliases().get("STATUS").map(String::as_str), Some("PA1"));
        assert!(matches!(
            resolver.alias_pin("BAD", "!PA1"),
            Err(PinResolverError::InvalidAliasTarget(_))
        ));
    }

    #[test]
    fn rejected_reserved_pin_does_not_activate_alias_state() {
        let mut resolver = PinResolver::new(true);
        resolver.alias_pin("LED", "PA1").unwrap();
        resolver.reserve_pin("PA1", "serial").unwrap();

        assert!(matches!(
            resolver.update_command("config_a pin=LED"),
            Err(PinResolverError::ReservedPin { .. })
        ));

        // The rejected alias use must not make the physical pin appear to
        // have been activated by a prior successful command.
        resolver.reserved.clear();
        assert_eq!(
            resolver.update_command("config_b pin=PA1").unwrap(),
            "config_b pin=PA1",
        );
    }

    #[test]
    fn validate_aliases_can_be_disabled() {
        let mut resolver = PinResolver::new(false);
        resolver.alias_pin("LED", "PA1").unwrap();
        resolver.update_command("config_a pin=PA1").unwrap();
        assert_eq!(resolver.update_command("config_b pin=LED").unwrap(), "config_b pin=PA1");
    }
}
