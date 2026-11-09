# klipper-host

Core Rust implementation of the Klipper **host** runtime.

Implemented:
- Printer lifecycle states: startup, configuring, ready, shutdown, disconnected.
- Lifecycle events for config loaded, ready, shutdown, and disconnect.
- Typed named-object registry with duplicate and type-mismatch checks.
- Parsed configuration ownership and recursive config-file loading.\n- Configuration-ordered component factory registry.
- Integration with the host G-code dispatcher and its readiness gating.

This crate does not compile, configure, flash, or emulate MCU firmware. MCU
communication remains the responsibility of the host protocol and transport
crates. Reactor scheduling, component loading, and the full printer object
graph are subsequent host-runtime work.
