# RustKlipper Host

This directory contains the independent Rust implementation of the Klipper
host-side runtime. The upstream Klipper source at the repository root remains
the compatibility reference and is not modified by the Rust implementation.

## Current scope

- Target platforms: Linux and Windows.
- MCU transport: USB serial only.
- CAN transport and CAN discovery are intentionally out of scope.
- Multiple MCUs remain supported when each MCU is attached through its own
  serial port.
- Compatibility target: stock Klipper MCU firmware, printer configuration,
  and G-code behavior.

## Workspace crates

- `klipper-protocol`: MCU framing, CRC-16/CCITT, base-128 integers, identify
  dictionary parsing, and message encoding/decoding.
- `klipper-transport`: USB serial transport, command scheduling, ACK tracking,
  retransmission, response routing, MCU clock synchronization, and config
  handshake primitives.
- `klipper-config`: ordered Klipper-style section/option parsing, typed
  configuration access, and recursive `[include ...]` expansion with relative
  paths, sorted glob matches, recursive-include detection, tracked option
  access, unknown section/option validation, and deduplicated deprecation
  warnings. Autosave blocks and full printer-object validation remain future work.
- `klipper-gcode`: Klipper-style traditional/extended G-code parsing, typed
  parameter access, readiness-aware command dispatch, mux commands, script
  execution, and ACK/response events.
- `klipper-host`: printer lifecycle and events, typed object registry and
  module-group lookup, tracked configuration reads, reactor-driven callbacks,
  configuration ownership/loading, and integration with G-code readiness.

The upstream Python implementation remains the behavioral oracle during the
staged port. Run the Rust workspace tests from this directory with:

```sh
cargo test --workspace
```
