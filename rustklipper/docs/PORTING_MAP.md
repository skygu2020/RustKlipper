# Upstream-to-Rust porting map

Baseline: `282db764dc568841837f68af3964762189d2baee`.

| Rust component | Upstream reference | Responsibility |
|---|---|---|
| `klipper-protocol` | `klippy/msgproto.py` | Frame encode/check/stream decode, CRC, integer codec, identify bootstrap chunks, zlib JSON dictionary, enums, typed command/response codec, and firmware `output` format decoder |
| `klipper-transport` | `klippy/serialhdl.py` | Initial cross-platform USB serial open/read/write and incremental frame ingestion; scheduling, ACKs, callbacks and reconnect still pending |
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


## Current implementation notes

- `klipper-protocol` has an incremental `FrameDecoder` that accepts fragmented
  and concatenated serial reads, validates CRC/sync/sequence, and attempts
  recovery after malformed frames.
- Identify bootstrap message IDs 0 and 1 are available before loading the
  dynamic firmware dictionary.
- `IdentifyDictionary` currently handles command and response definitions,
  integer and dynamic-buffer fields, enum lookup, and config constants.
- Firmware `output` format definitions are parsed and decoded into `#output` debug strings.
- `klipper-transport` currently wraps `serialport` with bounded read timeouts.
  It is not yet a drop-in replacement for Klipper's C `serialqueue`.
- Linux and Windows GitHub Actions tests are configured under
  `.github/workflows/rustklipper.yml`.


### Serial transaction primitives (initial implementation)

- `response::ResponseRouter`: FIFO waiters keyed by message name and optional
  object id; response dispatch preserves sent/receive timestamps and decoded
  parameter values.
- `ack::AckTracker`: notification IDs, extended four-bit sequence handling,
  and the serialqueue wake-sequence rule (empty ACK: receive sequence minus
  one; data response: minus two).
- `retry::RetryPolicy`: the upstream `SerialRetryCommand` retry schedule:
  five retries after the initial attempt, with exponential delays beginning
  at 10ms.
- These are tested primitives, not yet connected to a live serial background
  reader or a clock-aware scheduler. In particular, the transport does not
  yet implement retransmission of lost wire frames, RTT/RTO estimation,
  receive-window limits, command batching, or minclock/reqclock scheduling.


### Clock-aware command scheduler (first pass)

- `scheduler::CommandScheduler` keeps per-command-queue FIFO order, holds
  upcoming commands until `min_clock`, and promotes eligible commands.
- Ready queue heads are selected by lowest `req_clock`, while preserving FIFO
  within each queue; payloads are packed up to the MCU frame size.
- Tracks extended send/receive sequence numbers, outstanding frame bytes,
  cumulative ACK retirement, receive-window backpressure, and notify IDs
  attached to scheduled frames.
- This is deliberately a scheduling core, not yet a drop-in serialqueue:
  it does not own a background I/O thread, clock-estimate conversion, UART
  bit-time calculation, deadline wakeups, retransmission/RTO, NAK fast
  retransmit, or the complete receiver notification queue.
