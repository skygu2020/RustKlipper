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
- `klipper-config`: ordered Klipper-style section/option parsing and typed
  configuration access. Include-file expansion and printer-object validation
  are the next layers to be added on top of this parser.

The upstream Python implementation remains the behavioral oracle during the
staged port. Run the Rust workspace tests from this directory with:

```sh
cargo test --workspace
```
