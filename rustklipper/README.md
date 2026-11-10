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
  access, typed/list/choice/range getters, unknown section/option validation,
  deduplicated deprecation warnings, and Klipper `SAVE_CONFIG` autosave-block
  loading with source-line preservation. Full printer-object validation remains
  future work.
- `klipper-gcode`: Klipper-style traditional/extended G-code parsing, typed
  parameter access, readiness-aware command dispatch, mux commands, script
  execution, and ACK/response events.
- `moonraker`: the initial in-process JSON-RPC 2.0 dispatcher is retained as a small protocol/test layer. The full Moonraker API-server implementation is now vendored as the pinned `vendor/moonruster` Git submodule (upstream `Rusti3D/moonruster`); it is a standalone executable, not a Cargo library dependency or a workspace member.
- `klipper-host`: guarded printer lifecycle and ordered identify/connect
  events, typed object registry and module-group lookup, multi-MCU USB serial
  session registry, dependency-aware/lazy component loading, tracked config
  reads, pre-connect validation,
  reactor callbacks, G-code handler reset on configuration reload, and
  callback dispatch outside the printer lock with per-line script state refresh,
  panic-contained handlers, component-owned G-code command lifecycle, and
  normal/mux command cleanup when a component unloads.

## Full Moonraker API server

The project uses [Moonruster](https://github.com/Rusti3D/moonruster) as the existing Rust port of Moonraker instead of reimplementing its 37 server components. It is pinned as a Git submodule at `vendor/moonruster`; `.gitmodules` records the upstream URL. Fetch it when cloning or updating this repository:

```sh
git clone --recurse-submodules https://github.com/skygu2020/RustKlipper.git
# or, in an existing checkout:
git submodule update --init --recursive
```

Build and run the standalone API server from the submodule directory using its own documented configuration and toolchain requirements (currently Rust 1.89):

```sh
cd rustklipper/vendor/moonruster
cargo build --release
```

Moonruster communicates with Klipper through the Klippy Unix-domain-socket JSON-RPC interface. The Rust Host integration must preserve that protocol boundary; the MCU serial transport is not a Moonraker interface. The submodule is deliberately not a workspace member, so `cargo test --workspace` continues to test only this repository's Rust crates and does not build Moonruster or Klipper firmware. The pinned upstream source retains its own license and update history; update the submodule pointer explicitly when adopting a newer upstream revision.

The upstream Python implementation remains the behavioral oracle during the
staged port. Run the Rust workspace tests from this directory with:

```sh
cargo test --workspace
```
