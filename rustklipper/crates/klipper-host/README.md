# klipper-host

Core Rust implementation of the Klipper **host** runtime.

Implemented:
- Printer lifecycle states: startup, configuring, ready, shutdown, disconnected.
- Lifecycle events for config loaded, ready, shutdown, and disconnect.
- Typed named-object registry with duplicate and type-mismatch checks.
- Parsed configuration ownership and recursive config-file loading.
- Configuration-ordered component factory registry.
- Integration with the host G-code dispatcher and its readiness gating.
- Component-scoped G-code and mux-command registration, with ownership checks,
  automatic handler removal on component unload, rollback after factory failure,
  and reset on configuration reinstall.
- G-code callbacks run from dispatcher snapshots outside the printer mutex;
  scripts refresh the active command snapshot for each line, and handler panics
  are converted into command error events without aborting the remaining script.
- Cooperative Reactor for immediate callbacks, delayed/periodic timers,
  cancellation, and next-deadline polling, driven by the host runtime.

This crate does not compile, configure, flash, or emulate MCU firmware. MCU
communication remains the responsibility of the host protocol and transport
crates. The Reactor is a cooperative scheduler; the outer application remains
responsible for polling it. Full asynchronous I/O integration and the complete
printer object graph are subsequent host-runtime work.

The Rust workspace CI validates this host crate on Linux and Windows with
`cargo test --workspace --all-targets`; it does not build MCU firmware.
