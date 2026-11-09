# klipper-gcode

Host-side G-code parsing primitives ported from `klippy/gcode.py`.

Implemented:
- Klipper-style letter-token splitting for traditional commands and parameters.
- Leading `N...` line number extraction.
- Original command-line preservation and raw parameter extraction.
- Extended `KEY=value` parameter parsing with quoted values.
- Typed integer/finite-float getters and range checks.
- Readiness-aware command registration and dispatch, mux commands, multi-line scripts, extended parameter decoding, handler responses, and automatic/explicit ACK handling.

Not yet implemented: mux commands, G-code macro/Jinja execution, printer
object integration, reactor-driven input/output, and motion command semantics.
