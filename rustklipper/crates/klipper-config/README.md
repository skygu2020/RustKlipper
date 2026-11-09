# klipper-config

Host-side Klipper configuration support.

Implemented:
- Ordered sections and options with case-insensitive option lookup.
- Typed scalar and list getters.
- Multiline option values and inline comments.
- Recursive `[include ...]` expansion relative to the including file.
- Sorted glob expansion, optional unmatched globs, missing direct-include errors,
  and recursive-include detection.

Not yet implemented: autosave blocks, option access/deprecation tracking,
printer-object-specific validation, and full parity with Python's
`configparser.RawConfigParser`.
