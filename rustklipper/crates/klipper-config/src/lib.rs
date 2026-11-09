//! Klipper-style configuration parsing primitives.
//!
//! This crate is the Rust counterpart of the parsing layer in
//! `klippy/configfile.py`. It intentionally preserves section order and
//! option insertion order so later config loading can reproduce Klipper's
//! deterministic traversal. File includes are handled by the loader layer.
use std::collections::HashMap;
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigOption {
    pub name: String,
    pub value: String,
    pub line: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigSection {
    pub name: String,
    pub line: usize,
    options: Vec<ConfigOption>,
    index: HashMap<String, usize>,
}

impl ConfigSection {
    fn new(name: String, line: usize) -> Self {
        Self { name, line, options: Vec::new(), index: HashMap::new() }
    }

    fn set(&mut self, name: String, value: String, line: usize) {
        let key = name.to_ascii_lowercase();
        if let Some(&index) = self.index.get(&key) {
            self.options[index].value = value;
            self.options[index].line = line;
        } else {
            let index = self.options.len();
            self.options.push(ConfigOption { name: key.clone(), value, line });
            self.index.insert(key, index);
        }
    }

    pub fn get(&self, name: &str) -> Option<&str> {
        self.index.get(&name.to_ascii_lowercase())
            .map(|&index| self.options[index].value.as_str())
    }

    pub fn has_option(&self, name: &str) -> bool {
        self.index.contains_key(&name.to_ascii_lowercase())
    }

    pub fn options(&self) -> &[ConfigOption] { &self.options }

    pub fn prefixed_options(&self, prefix: &str) -> impl Iterator<Item = &ConfigOption> {
        let prefix = prefix.to_ascii_lowercase();
        self.options.iter().filter(move |option| option.name.starts_with(&prefix))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ConfigDocument {
    sections: Vec<ConfigSection>,
    index: HashMap<String, usize>,
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum ConfigParseError {
    #[error("{filename}:{line}: option appears before the first section")]
    MissingSection { filename: String, line: usize },
    #[error("{filename}:{line}: malformed section header")]
    InvalidSection { filename: String, line: usize },
    #[error("{filename}:{line}: expected key=value option")]
    InvalidOption { filename: String, line: usize },
    #[error("{filename}:{line}: empty option name")]
    EmptyOption { filename: String, line: usize },
    #[error("{filename}:{line}: continuation without an option")]
    OrphanContinuation { filename: String, line: usize },
}

impl ConfigDocument {
    pub fn parse(input: &str) -> Result<Self, ConfigParseError> {
        Self::parse_named(input, "<memory>")
    }

    pub fn parse_named(input: &str, source: impl Into<String>) -> Result<Self, ConfigParseError> {
        Self::parse_named_at(input, source.into(), 0)
    }

    fn parse_named_at(input: &str, source: String, line_offset: usize) -> Result<Self, ConfigParseError> {
        let mut document = Self::default();
        let mut current: Option<usize> = None;
        let mut last_option: Option<(usize, usize)> = None;
        let mut last_option_indent: Option<usize> = None;

        for (zero_line, raw) in input.lines().enumerate() {
            let line_no = zero_line + 1 + line_offset;
            let line = raw.trim_end_matches('\r');
            let uncommented = strip_hash_comment(line);
            let trimmed = uncommented.trim();

            if trimmed.is_empty() || trimmed.starts_with(';') {
                continue;
            }

            if trimmed.starts_with('[') {
                let Some(name) = trimmed.strip_prefix('[').and_then(|s| s.strip_suffix(']')) else {
                    return Err(ConfigParseError::InvalidSection { filename: source, line: line_no });
                };
                let name = name.trim();
                if name.is_empty() || name.contains('[') || name.contains(']') {
                    return Err(ConfigParseError::InvalidSection { filename: source, line: line_no });
                }
                let key = name.to_owned();
                let index = if let Some(&index) = document.index.get(&key) {
                    index
                } else {
                    let index = document.sections.len();
                    document.sections.push(ConfigSection::new(key.clone(), line_no));
                    document.index.insert(key, index);
                    index
                };
                current = Some(index);
                last_option = None;
                last_option_indent = None;
                continue;
            }

            let section_index = current.ok_or_else(|| ConfigParseError::MissingSection {
                filename: source.clone(), line: line_no,
            })?;

            let indent = line.len() - line.trim_start().len();
            if last_option_indent.is_some_and(|previous| indent > previous) {
                let Some((option_section, option_index)) = last_option else {
                    return Err(ConfigParseError::OrphanContinuation {
                        filename: source.clone(), line: line_no,
                    });
                };
                if option_section == section_index {
                    let continuation = strip_inline_comment(trimmed).trim_end();
                    if !continuation.is_empty() {
                        let value = &mut document.sections[section_index].options[option_index].value;
                        if !value.is_empty() {
                            value.push('\n');
                        }
                        value.push_str(continuation);
                    }
                    continue;
                }
            }

            let option_text = strip_inline_comment(trimmed);
            let Some(delimiter) = option_text.find(|ch| ch == '=' || ch == ':') else {
                return Err(ConfigParseError::InvalidOption {
                    filename: source.clone(), line: line_no,
                });
            };
            let name = &option_text[..delimiter];
            let value = &option_text[delimiter + 1..];
            let name = name.trim();
            if name.is_empty() {
                return Err(ConfigParseError::EmptyOption {
                    filename: source.clone(), line: line_no,
                });
            }
            let value = value.trim().to_owned();
            let section = &mut document.sections[section_index];
            section.set(name.to_owned(), value, line_no);
            let option_index = *section.index.get(&name.to_ascii_lowercase()).unwrap();
            last_option = Some((section_index, option_index));
            last_option_indent = Some(indent);
        }
        Ok(document)
    }

    fn merge(&mut self, other: ConfigDocument) {
        for incoming in other.sections {
            let section_index = if let Some(&index) = self.index.get(&incoming.name) {
                index
            } else {
                let index = self.sections.len();
                self.index.insert(incoming.name.clone(), index);
                self.sections.push(ConfigSection::new(incoming.name.clone(), incoming.line));
                index
            };
            for option in incoming.options {
                self.sections[section_index].set(option.name, option.value, option.line);
            }
        }
    }

    pub fn section(&self, name: &str) -> Option<&ConfigSection> {
        self.index.get(name).map(|&index| &self.sections[index])
    }

    pub fn has_section(&self, name: &str) -> bool { self.index.contains_key(name) }

    pub fn sections(&self) -> &[ConfigSection] { &self.sections }

    pub fn sections_with_prefix(&self, prefix: &str) -> impl Iterator<Item = &ConfigSection> {
        self.sections.iter().filter(move |section| section.name.starts_with(prefix))
    }

    pub fn get(&self, section: &str, option: &str) -> Option<&str> {
        self.section(section)?.get(option)
    }

    pub fn get_required(&self, section: &str, option: &str) -> Result<&str, ConfigValueError> {
        self.get(section, option).ok_or_else(|| ConfigValueError::Missing {
            section: section.to_owned(), option: option.to_owned(),
        })
    }

    pub fn get_i64(&self, section: &str, option: &str) -> Result<i64, ConfigValueError> {
        let value = self.get_required(section, option)?;
        value.parse().map_err(|_| ConfigValueError::Invalid {
            section: section.to_owned(), option: option.to_owned(),
            value: value.to_owned(), expected: "integer",
        })
    }

    pub fn get_f64(&self, section: &str, option: &str) -> Result<f64, ConfigValueError> {
        let value = self.get_required(section, option)?;
        let parsed: f64 = value.parse().map_err(|_| ConfigValueError::Invalid {
            section: section.to_owned(), option: option.to_owned(),
            value: value.to_owned(), expected: "number",
        })?;
        if !parsed.is_finite() {
            return Err(ConfigValueError::Invalid {
                section: section.to_owned(), option: option.to_owned(),
                value: value.to_owned(), expected: "finite number",
            });
        }
        Ok(parsed)
    }

    /// Parse a comma-separated option, trimming whitespace around each item.
    pub fn get_list(&self, section: &str, option: &str, separator: char, count: Option<usize>)
        -> Result<Vec<String>, ConfigValueError>
    {
        let value = self.get_required(section, option)?;
        let values: Vec<String> = if value.trim().is_empty() {
            Vec::new()
        } else {
            value.split(separator).map(str::trim).map(str::to_owned).collect()
        };
        if let Some(expected) = count {
            if values.len() != expected {
                return Err(ConfigValueError::ElementCount {
                    section: section.to_owned(), option: option.to_owned(),
                    expected, actual: values.len(),
                });
            }
        }
        Ok(values)
    }

    pub fn get_i64_list(&self, section: &str, option: &str, separator: char, count: Option<usize>)
        -> Result<Vec<i64>, ConfigValueError>
    {
        self.get_list(section, option, separator, count)?
            .into_iter()
            .map(|value| value.parse().map_err(|_| ConfigValueError::Invalid {
                section: section.to_owned(), option: option.to_owned(),
                value, expected: "integer list",
            }))
            .collect()
    }

    pub fn get_f64_list(&self, section: &str, option: &str, separator: char, count: Option<usize>)
        -> Result<Vec<f64>, ConfigValueError>
    {
        self.get_list(section, option, separator, count)?
            .into_iter()
            .map(|value| {
                let parsed: f64 = value.parse().map_err(|_| ConfigValueError::Invalid {
                    section: section.to_owned(), option: option.to_owned(),
                    value: value.clone(), expected: "number list",
                })?;
                if !parsed.is_finite() {
                    return Err(ConfigValueError::Invalid {
                        section: section.to_owned(), option: option.to_owned(),
                        value, expected: "finite number list",
                    });
                }
                Ok(parsed)
            })
            .collect()
    }

    pub fn get_bool(&self, section: &str, option: &str) -> Result<bool, ConfigValueError> {
        let value = self.get_required(section, option)?;
        match value.to_ascii_lowercase().as_str() {
            "1" | "yes" | "true" | "on" => Ok(true),
            "0" | "no" | "false" | "off" => Ok(false),
            _ => Err(ConfigValueError::Invalid {
                section: section.to_owned(), option: option.to_owned(),
                value: value.to_owned(), expected: "boolean",
            }),
        }
    }
}


/// Filesystem-backed config loader with Klipper-compatible `[include ...]`
/// expansion. Includes are expanded in-place, sorted lexicographically, and
/// resolved relative to the file containing the include directive.
#[derive(Debug, Error)]
pub enum ConfigLoadError {
    #[error("unable to read config file '{path}': {source}")]
    Io { path: String, #[source] source: std::io::Error },
    #[error(transparent)]
    Parse(#[from] ConfigParseError),
    #[error("include file '{pattern}' does not exist")]
    IncludeNotFound { pattern: String },
    #[error("recursive include of config file '{path}'")]
    RecursiveInclude { path: String },
    #[error("invalid include pattern '{pattern}': {message}")]
    InvalidPattern { pattern: String, message: String },
}

/// Loads a main config file and recursively expands its include directives.
#[derive(Debug, Default, Clone, Copy)]
pub struct ConfigFileLoader;

impl ConfigFileLoader {
    pub fn load(path: impl AsRef<std::path::Path>) -> Result<ConfigDocument, ConfigLoadError> {
        let path = path.as_ref();
        let mut visited = std::collections::HashSet::new();
        Self::load_inner(path, &mut visited)
    }

    fn load_inner(
        path: &std::path::Path,
        visited: &mut std::collections::HashSet<std::path::PathBuf>,
    ) -> Result<ConfigDocument, ConfigLoadError> {
        use std::io::Read;
        let canonical = std::fs::canonicalize(path).map_err(|source| ConfigLoadError::Io {
            path: path.display().to_string(), source,
        })?;
        if !visited.insert(canonical.clone()) {
            return Err(ConfigLoadError::RecursiveInclude { path: path.display().to_string() });
        }
        let result = (|| {
            let mut file = std::fs::File::open(&canonical).map_err(|source| ConfigLoadError::Io {
                path: canonical.display().to_string(), source,
            })?;
            let mut data = String::new();
            file.read_to_string(&mut data).map_err(|source| ConfigLoadError::Io {
                path: canonical.display().to_string(), source,
            })?;
            let data = data.replace("\r\n", "\n");
            let mut merged = ConfigDocument::default();
            let mut buffered = Vec::new();
            let mut buffered_start_line = 1usize;
            let parent = canonical.parent().unwrap_or_else(|| std::path::Path::new("."));

            let flush = |lines: &mut Vec<String>, start_line: usize, merged: &mut ConfigDocument| -> Result<(), ConfigLoadError> {
                if lines.iter().all(|line| line.trim().is_empty()) {
                    lines.clear();
                    return Ok(());
                }
                let chunk = lines.join("\n");
                let parsed = ConfigDocument::parse_named_at(
                    &chunk, canonical.display().to_string(), start_line.saturating_sub(1),
                )?;
                merged.merge(parsed);
                lines.clear();
                Ok(())
            };

            for (line_index, line) in data.lines().enumerate() {
                let line_number = line_index + 1;
                let without_comment = strip_hash_comment(line);
                let trimmed = without_comment.trim();
                let include_spec = trimmed.strip_prefix('[')
                    .and_then(|value| value.strip_suffix(']'))
                    .and_then(|header| header.strip_prefix("include "))
                    .map(str::trim);
                if let Some(spec) = include_spec {
                    flush(&mut buffered, buffered_start_line, &mut merged)?;
                    buffered_start_line = line_number + 1;
                    let pattern = parent.join(spec);
                    let pattern_text = pattern.to_string_lossy().into_owned();
                    let mut matches = Vec::new();
                    for entry in glob::glob(&pattern_text).map_err(|error| ConfigLoadError::InvalidPattern {
                        pattern: pattern_text.clone(), message: error.to_string(),
                    })? {
                        matches.push(entry.map_err(|error| ConfigLoadError::InvalidPattern {
                            pattern: pattern_text.clone(), message: error.to_string(),
                        })?);
                    }
                    matches.sort();
                    let has_wildcards = spec.chars().any(|ch| matches!(ch, '*' | '?' | '['));
                    if matches.is_empty() && !has_wildcards {
                        return Err(ConfigLoadError::IncludeNotFound { pattern: pattern_text });
                    }
                    for included in matches {
                        let included_doc = Self::load_inner(&included, visited)?;
                        merged.merge(included_doc);
                    }
                } else {
                    if buffered.is_empty() {
                        buffered_start_line = line_number;
                    }
                    buffered.push(line.to_owned());
                }
            }
            flush(&mut buffered, buffered_start_line, &mut merged)?;
            Ok(merged)
        })();
        visited.remove(&canonical);
        result
    }
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum ConfigValueError {
    #[error("required option '{option}' is missing from section '{section}'")]
    Missing { section: String, option: String },
    #[error("option '{option}' in section '{section}' has invalid value '{value}' (expected {expected})")]
    Invalid { section: String, option: String, value: String, expected: &'static str },
    #[error("option '{option}' in section '{section}' must contain {expected} elements, got {actual}")]
    ElementCount { section: String, option: String, expected: usize, actual: usize },
}

/// Tracks which configuration options were consumed by host components.
#[derive(Debug, Default)]
pub struct ConfigUsageTracker {
    used: std::sync::Mutex<std::collections::HashSet<(String, String)>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnusedConfigOption {
    pub section: String,
    pub option: String,
    pub line: usize,
}

impl ConfigUsageTracker {
    pub fn new() -> Self {
        Self::default()
    }

    /// Reset access history when a new configuration document is installed.
    pub fn clear(&self) {
        self.used
            .lock()
            .expect("config usage lock poisoned")
            .clear();
    }

    /// Read an option and mark it as used when it exists.
    pub fn get<'a>(
        &self,
        config: &'a ConfigDocument,
        section: &str,
        option: &str,
    ) -> Option<&'a str> {
        let value = config.get(section, option)?;
        self.used
            .lock()
            .expect("config usage lock poisoned")
            .insert((section.to_owned(), option.to_ascii_lowercase()));
        Some(value)
    }

    pub fn get_required<'a>(
        &self,
        config: &'a ConfigDocument,
        section: &str,
        option: &str,
    ) -> Result<&'a str, ConfigValueError> {
        self.get(config, section, option)
            .ok_or_else(|| ConfigValueError::Missing {
                section: section.to_owned(),
                option: option.to_owned(),
            })
    }

    pub fn get_i64(
        &self,
        config: &ConfigDocument,
        section: &str,
        option: &str,
    ) -> Result<i64, ConfigValueError> {
        let value = self.get_required(config, section, option)?;
        value.parse().map_err(|_| ConfigValueError::Invalid {
            section: section.to_owned(),
            option: option.to_owned(),
            value: value.to_owned(),
            expected: "integer",
        })
    }

    pub fn get_bool(
        &self,
        config: &ConfigDocument,
        section: &str,
        option: &str,
    ) -> Result<bool, ConfigValueError> {
        let value = self.get_required(config, section, option)?;
        match value.to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => Ok(true),
            "0" | "false" | "no" | "off" => Ok(false),
            _ => Err(ConfigValueError::Invalid {
                section: section.to_owned(),
                option: option.to_owned(),
                value: value.to_owned(),
                expected: "boolean",
            }),
        }
    }

    /// Return every option that no component has read, preserving config order.
    pub fn unused_options(&self, config: &ConfigDocument) -> Vec<UnusedConfigOption> {
        let used = self.used.lock().expect("config usage lock poisoned");
        config
            .sections()
            .iter()
            .flat_map(|section| {
                section.options().iter().filter_map(|option| {
                    let key = (section.name.clone(), option.name.clone());
                    (!used.contains(&key)).then(|| UnusedConfigOption {
                        section: section.name.clone(),
                        option: option.name.clone(),
                        line: option.line,
                    })
                })
            })
            .collect()
    }
}

fn strip_hash_comment(line: &str) -> &str {
    line.split_once('#').map_or(line, |(head, _)| head)
}

fn strip_inline_comment(line: &str) -> &str {
    let bytes = line.as_bytes();
    for index in 0..bytes.len() {
        if (bytes[index] == b';' || bytes[index] == b'#')
            && (index == 0 || bytes[index - 1].is_ascii_whitespace())
        {
            return &line[..index];
        }
    }
    line
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_sections_options_comments_and_case_insensitive_keys() {
        let config = ConfigDocument::parse(
            "# printer config\n[printer]\nMax_velocity = 300 ; mm/s\nmax_accel=5000 # limit\n",
        ).unwrap();
        assert_eq!(config.get("printer", "MAX_VELOCITY"), Some("300"));
        assert_eq!(config.get("printer", "max_accel"), Some("5000"));
        assert_eq!(config.sections()[0].options().len(), 2);
    }

    #[test]
    fn repeated_options_override_without_changing_order() {
        let config = ConfigDocument::parse("[stepper_x]\npin=PA0\nposition_endstop=0\npin=PA1\n").unwrap();
        let options = config.section("stepper_x").unwrap().options();
        assert_eq!(options.len(), 2);
        assert_eq!(options[0].name, "pin");
        assert_eq!(options[0].value, "PA1");
        assert_eq!(options[1].name, "position_endstop");
    }

    #[test]
    fn supports_multiline_values_and_ignores_blank_lines() {
        let config = ConfigDocument::parse("[gcode_macro TEST]\n  gcode:\n    G28\n\n    M400\n").unwrap();
        assert_eq!(config.get("gcode_macro TEST", "gcode"), Some("G28\nM400"));
    }

    #[test]
    fn validates_section_and_option_syntax() {
        assert!(matches!(ConfigDocument::parse("key=value"), Err(ConfigParseError::MissingSection { .. })));
        assert!(matches!(ConfigDocument::parse("[printer\n"), Err(ConfigParseError::InvalidSection { .. })));
        assert!(matches!(ConfigDocument::parse("[printer]\ninvalid"), Err(ConfigParseError::InvalidOption { .. })));
    }

    #[test]
    fn typed_getters_validate_values_and_ranges_are_left_to_callers() {
        let config = ConfigDocument::parse("[printer]\nmax_velocity=300\nmax_accel=5000\nuse_pin=true\n").unwrap();
        assert_eq!(config.get_i64("printer", "max_velocity").unwrap(), 300);
        assert_eq!(config.get_f64("printer", "max_accel").unwrap(), 5000.0);
        assert!(config.get_bool("printer", "use_pin").unwrap());
        assert!(config.get_bool("printer", "missing").is_err());
    }

    #[test]
    fn usage_tracker_marks_reads_and_reports_unused_options_in_order() {
        let config = ConfigDocument::parse(
            "[printer]\nmax_velocity=300\nmax_accel=5000\nuse_pin=yes\n",
        )
        .unwrap();
        let tracker = ConfigUsageTracker::new();
        assert_eq!(tracker.get_i64(&config, "printer", "MAX_VELOCITY").unwrap(), 300);
        assert!(tracker.get_bool(&config, "printer", "use_pin").unwrap());
        let unused = tracker.unused_options(&config);
        assert_eq!(unused.len(), 1);
        assert_eq!(unused[0].section, "printer");
        assert_eq!(unused[0].option, "max_accel");
    }

    #[test]
    fn parses_string_and_integer_lists_with_optional_element_count() {
        let config = ConfigDocument::parse("[stepper_x]\nposition=10, 20,30\n").unwrap();
        assert_eq!(
            config.get_list("stepper_x", "position", ',', Some(3)).unwrap(),
            vec!["10", "20", "30"],
        );
        assert_eq!(
            config.get_i64_list("stepper_x", "position", ',', Some(3)).unwrap(),
            vec![10, 20, 30],
        );
        assert!(matches!(
            config.get_list("stepper_x", "position", ',', Some(2)),
            Err(ConfigValueError::ElementCount { expected: 2, actual: 3, .. })
        ));
    }

    #[test]
    fn parses_finite_float_lists() {
        let config = ConfigDocument::parse("[bed_mesh]\nmesh_min=0.5, 1.25\n").unwrap();
        assert_eq!(
            config.get_f64_list("bed_mesh", "mesh_min", ',', Some(2)).unwrap(),
            vec![0.5, 1.25],
        );
    }

    #[test]
    fn loads_relative_includes_in_source_order() {
        use std::fs;
        let dir = std::env::temp_dir().join(format!("rustklipper-config-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join("base.cfg"), "[printer]\nmax_velocity=200\n").unwrap();
        fs::write(dir.join("main.cfg"),
            "[printer]\nmax_accel=3000\n[include base.cfg]\n[stepper_x]\npin=PA0\n"
        ).unwrap();
        let config = ConfigFileLoader::load(dir.join("main.cfg")).unwrap();
        assert_eq!(config.get("printer", "max_velocity"), Some("200"));
        assert_eq!(config.get("printer", "max_accel"), Some("3000"));
        assert_eq!(config.get("stepper_x", "pin"), Some("PA0"));
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn rejects_missing_direct_include_but_allows_empty_glob() {
        use std::fs;
        let dir = std::env::temp_dir().join(format!("rustklipper-config-missing-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join("direct.cfg"), "[include absent.cfg]\n").unwrap();
        assert!(matches!(
            ConfigFileLoader::load(dir.join("direct.cfg")),
            Err(ConfigLoadError::IncludeNotFound { .. })
        ));
        fs::write(dir.join("wildcard.cfg"), "[include optional-*.cfg]\n").unwrap();
        assert!(ConfigFileLoader::load(dir.join("wildcard.cfg")).is_ok());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn expands_globs_in_sorted_order() {
        use std::fs;
        let dir = std::env::temp_dir().join(format!("rustklipper-config-glob-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(dir.join("parts")).unwrap();
        fs::write(dir.join("parts/20.cfg"), "[printer]\nmax_velocity=200\n").unwrap();
        fs::write(dir.join("parts/10.cfg"), "[printer]\nmax_velocity=100\n").unwrap();
        fs::write(dir.join("main.cfg"), "[include parts/*.cfg]\n").unwrap();
        let config = ConfigFileLoader::load(dir.join("main.cfg")).unwrap();
        assert_eq!(config.get("printer", "max_velocity"), Some("200"));
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn rejects_recursive_includes() {
        use std::fs;
        let dir = std::env::temp_dir().join(format!("rustklipper-config-cycle-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join("a.cfg"), "[include b.cfg]\n").unwrap();
        fs::write(dir.join("b.cfg"), "[include a.cfg]\n").unwrap();
        assert!(matches!(
            ConfigFileLoader::load(dir.join("a.cfg")),
            Err(ConfigLoadError::RecursiveInclude { .. })
        ));
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn duplicate_sections_merge_in_source_order() {
        let config = ConfigDocument::parse("[printer]\na=1\n[stepper_x]\npin=PA0\n[printer]\nb=2\n").unwrap();
        let printer = config.section("printer").unwrap();
        assert_eq!(printer.get("a"), Some("1"));
        assert_eq!(printer.get("b"), Some("2"));
        assert_eq!(config.sections().len(), 2);
    }
}
