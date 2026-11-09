# Klipper

Klipper is a 3D printer firmware project.

The original Python host and MCU firmware sources remain at the repository
root. The staged Rust host implementation is maintained independently in
[`rustklipper/`](rustklipper/README.md), using the upstream source as its
compatibility reference.

The Rust workspace currently contains MCU protocol, USB serial transport,
and configuration parsing crates. To run its tests:

```sh
cd rustklipper
cargo test --workspace
```

The Rust implementation targets Linux and Windows with USB-serial MCUs;
CAN transport is intentionally outside the current scope.
