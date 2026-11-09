# Upstream-to-Rust porting map

Baseline: `282db764dc568841837f68af3964762189d2baee`.

| Rust component | Upstream reference | Responsibility |
|---|---|---|
| `klipper-protocol` | `klippy/msgproto.py` | MCU wire framing, CRC, integer and message dictionary codec (initial implementation committed) |
| `klipper-transport` (planned) | `klippy/serialhdl.py` | USB serial lifecycle, reader/writer queues, ACK and response routing |
| `klipper-reactor` (planned) | `klippy/reactor.py` | Timers, file readiness, async completions and event dispatch |
| `klipper-clock` (planned) | `klippy/clocksync.py` | MCU clock extension, RTT tracking and host/MCU clock regression |
| `klipper-mcu` (planned) | `klippy/mcu.py`, `klippy/pins.py` | MCU identify/configuration, commands, pins and scheduling |
| `klipper-config` (planned) | `klippy/configfile.py` | Includes, sections, options, validation and printer object setup |
| `klipper-gcode` (planned) | `klippy/gcode.py` | G-code parsing, command dispatch, state and responses |
| `klipper-motion` (planned) | `klippy/toolhead.py`, `klippy/kinematics/`, `klippy/chelper/` | Motion planning, kinematics and step generation |
| `klipper-runtime` (planned) | `klippy/klippy.py` | Startup, object graph, lifecycle and shutdown |

## Port order

1. Protocol codec and golden vectors against the Python implementation.
2. Cross-platform USB serial transport and reconnect behavior.
3. Event loop and asynchronous command/response coordination.
4. MCU identify, configuration, clock synchronization and multi-MCU startup.
5. G-code/config compatibility and printer object lifecycle.
6. Motion planning and hardware integration.
7. Extras, diagnostics and Moonraker-facing behavior.

CAN-specific transports and discovery are intentionally excluded. Do not
remove CAN-related upstream code: it remains part of the reference snapshot,
but the Rust host will not implement those paths.
