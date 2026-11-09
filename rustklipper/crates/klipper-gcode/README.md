# klipper-gcode

Host-side G-code parsing primitives ported from `klippy/gcode.py`.

Implemented:
- Klipper-style letter-token splitting for traditional commands and parameters.
- Leading `N...` line number extraction.
- Original command-line preservation and raw parameter extraction.
- Extended `KEY=value` parameter parsing with quoted values.
- Typed integer/finite-float getters and range checks.

Not yet implemented: command registration and dispatch, printer-ready/shutdown
gating, output/ack handling, mux commands, G-code macro/Jinja execution, and
motion command semantics.
