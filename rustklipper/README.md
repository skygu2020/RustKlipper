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

## Workspace

The initial workspace contains `klipper-protocol`, which implements the
MCU frame format, CRC-16/CCITT, and Klipper's base-128 integer encoding.
Run tests from this directory with:

```sh
cargo test --workspace
```

The workspace is deliberately separate from the upstream Python host. The
Python implementation remains the behavioral oracle during the staged port.
