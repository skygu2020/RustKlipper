# RustKlipper implementation baseline

- Upstream: https://github.com/Klipper3d/klipper
- Fork: https://github.com/skygu2020/RustKlipper
- Baseline branch: `master`
- Baseline commit: `282db764dc568841837f68af3964762189d2baee`
- Commit date: 2026-11-08T01:21:42Z
- License: GPL-3.0

The Rust implementation is isolated under `rustklipper/`. Do not rewrite or
delete upstream Python/C sources during the compatibility phase. Changes to
upstream reference files should be avoided; Rust modules should cite the
corresponding upstream path and be validated against its behavior.

## Product constraints

- Linux + Windows.
- USB serial only; no SocketCAN, PCAN, Kvaser, or CAN UUID workflows.
- Multiple USB-serial MCUs are in scope.
- Preserve stock MCU firmware protocol compatibility.
- Preserve `printer.cfg` and G-code compatibility as the long-term target.
- Moonraker compatibility is preferred; UI is not part of the first host core.
