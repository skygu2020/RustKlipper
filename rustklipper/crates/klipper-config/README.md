# klipper-config

Rust configuration primitives for the Klipper host runtime.

The initial parser mirrors the common behavior of
`klippy/configfile.py` / Python's `RawConfigParser`:

- case-insensitive option names and case-sensitive section names;
- source-order section and option iteration;
- duplicate options overwrite their prior value without moving their position;
- repeated sections merge into the first section;
- `=` and `:` option delimiters;
- inline comments, indented multiline values, and typed scalar/list getters.

This crate is deliberately separate from the upstream Python tree. Include
expansion, autosave blocks, deprecation tracking, and printer-object-specific
validation remain follow-up work; the parser does not claim to implement those
features yet.

Run its tests from the Rust workspace:

```sh
cargo test -p klipper-config
```
