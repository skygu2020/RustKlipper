# klipper-transport

Initial cross-platform USB serial transport for Linux and Windows.

Implemented:
- Open a configured serial device at a selected baud rate.
- Bounded serial read timeout.
- Incremental frame decoding through `klipper-protocol::FrameDecoder`.
- Write a framed MCU payload.
- Clear input/output buffers and inspect pending byte counts.

Not implemented yet:
- Klipper serialqueue scheduling semantics (`minclock` / `reqclock`).
- ACK completion notifications and pending notification IDs.
- Message response callback registry and retry commands.
- Reactor/background task integration.
- Automatic reconnect and MCU identify handshake orchestration.
- Board-specific reset sequences.

This crate intentionally does not implement CAN.
