# Moonraker and Klipper Host process boundary

Moonraker and Klipper Host are separate applications. They must not share
Rust objects, dispatcher instances, locks, Tokio tasks, or process memory.

## Responsibilities

- **Klipper Host** owns printer lifecycle, G-code execution, MCU transport,
  configuration, and the Klippy API dispatcher.
- **Moonraker** owns the HTTP API, JSON-RPC router, WebSocket clients,
  authentication, and external client-facing behavior.
- **IPC** is the versioned-by-contract Klippy JSON protocol: one JSON value
  followed by ETX (byte `0x03`). A WebSocket client at `/klippysocket`
  maps to one independent Host TCP connection.

## Cross-platform transport

The initial IPC transport is Tokio TCP, supported on Windows, Linux, and
macOS. For a same-machine deployment, bind the Host listener to loopback
(for example, `127.0.0.1`) and configure Moonraker with that exact socket
address. Do not expose the Host IPC listener to an untrusted network.

The Host exposes `klipper_host::klippy_api::serve_tcp_listener`; the Host
application owns the listener and its lifecycle. Moonraker is configured with
`MoonrakerHttpServer::bind_with_klippy_endpoint` and connects only when a
client opens `/klippysocket`.

Unix Domain Sockets and Windows Named Pipes can be added later as transport
adapters. They must preserve the same byte-stream JSON + ETX contract and
must not introduce a compile-time dependency from Moonraker to Klipper Host.

## Failure isolation

- If the Host IPC endpoint is unavailable, Moonraker rejects the
  `/klippysocket` upgrade with HTTP 503; its HTTP/JSON-RPC service remains
  available.
- If a Host connection closes, only the corresponding Klippy WebSocket ends.
- Restarting Moonraker does not stop the Host process or printer runtime.
- Restarting the Host closes IPC streams; Moonraker remains alive and can
  accept new connections after the Host returns.

## Configuration and security

The IPC address is explicit configuration, not a hidden hard-coded port.
The current API accepts a `SocketAddr` so deployments choose a port that
does not conflict with other services. Loopback binding is the recommended
default for local deployments. Authentication and authorization for IPC are
future work; until then, keep the listener private to the machine or protect
it with an OS/network boundary.
